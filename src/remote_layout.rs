use anyhow::{Context, Result, bail};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::config::RemoteConfig;
use crate::remote::{DownloadResult, UploadResult};
use crate::store::EntryMeta;

const V3_ROOT: &str = "v3";
const V3_MANIFESTS: &str = "manifests";
const V3_PACKS: &str = "packs";
const V3_MANIFEST_VERSION: u32 = 3;

/// Cap on total bytes written while extracting one downloaded entry pack.
/// Generous for real artifact packs (a large crate's rlib + rmeta + debug
/// info), but lethal to a decompression bomb that expands a few KB into
/// terabytes. Paired with [`MAX_ZSTD_WINDOW_LOG`] on the decoder (#212).
const MAX_EXTRACTED_BYTES: u64 = 8 * 1024 * 1024 * 1024; // 8 GiB

/// Max zstd window-log accepted on decode (2^27 = 128 MiB). Bounds the
/// decoder's allocation regardless of what the frame header claims (#212).
const MAX_ZSTD_WINDOW_LOG: u32 = 27;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct V3Manifest {
    version: u32,
    cache_key: String,
    crate_name: String,
    pack_key: String,
    pack_bytes: u64,
    original_bytes: u64,
    file_count: usize,
}

/// Remote cache layout for the `v3` pack-first format.
///
/// `v3` stores:
/// - a small manifest object for listing/existence checks
/// - one packed tar.zst object per entry for restore/upload
///
/// The local store stays content-addressed; remote transport is optimized for
/// cold object-store restores with low request fan-out.
pub struct RemoteLayout<'a> {
    client: &'a aws_sdk_s3::Client,
    remote: &'a RemoteConfig,
}

pub struct RemoteUploadResult {
    pub format: &'static str,
    pub transfer: UploadResult,
}

impl<'a> RemoteLayout<'a> {
    pub fn new(client: &'a aws_sdk_s3::Client, remote: &'a RemoteConfig) -> Self {
        Self { client, remote }
    }

    pub async fn exists_entry(&self, cache_key: &str, crate_name: &str) -> Result<bool> {
        let object_key = v3_manifest_key(&self.remote.prefix, cache_key, crate_name);
        match self
            .client
            .head_object()
            .bucket(&self.remote.bucket)
            .key(&object_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => {
                let http_status = e.raw_response().map(|r| r.status().as_u16());
                let err = e.into_service_error();
                if is_missing_head_object(&err) {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!(
                        "S3 head_object error for s3://{}/{}: {}",
                        self.remote.bucket,
                        object_key,
                        describe_head_object_error(&err, http_status)
                    ))
                }
            }
        }
    }

    pub async fn download_entry(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        _blobs_dir: &Path,
    ) -> Result<DownloadResult> {
        let object_key = v3_pack_key(&self.remote.prefix, cache_key, crate_name);

        tracing::debug!(
            "downloading v3 pack s3://{}/{}",
            self.remote.bucket,
            object_key
        );

        let request_start = std::time::Instant::now();
        let resp = self
            .client
            .get_object()
            .bucket(&self.remote.bucket)
            .key(&object_key)
            .send()
            .await
            .context("downloading v3 pack from S3")?;
        let request_ms = request_start.elapsed().as_millis() as u64;

        let body_start = std::time::Instant::now();
        let body = resp
            .body
            .collect()
            .await
            .context("reading v3 pack response body")?;
        let body_ms = body_start.elapsed().as_millis() as u64;
        let compressed = body.into_bytes();
        let compressed_len = compressed.len() as u64;

        let extract_start = std::time::Instant::now();
        let mut decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&compressed))
            .context("creating v3 zstd decoder")?;
        // Bound the decompression window so a hostile/buggy frame can't force a
        // huge allocation. 27 = 128 MiB, well above what level-3 packs use and
        // independent of the bomb guard on extracted bytes below (#212).
        decoder
            .window_log_max(MAX_ZSTD_WINDOW_LOG)
            .context("setting v3 zstd window-log cap")?;
        let original_bytes = extract_entry_pack(decoder, entry_dir)?;
        let extract_ms = extract_start.elapsed().as_millis() as u64;

        Ok(DownloadResult {
            format: "v3",
            object_key,
            compressed_bytes: compressed_len,
            original_bytes,
            network_ms: request_ms + body_ms,
            request_ms,
            body_ms,
            request_count: 1,
            decompress_ms: 0,
            extract_ms,
            disk_io_ms: 0,
            blobs_skipped: 0,
            blobs_total: 0,
        })
    }

    pub async fn upload_entry(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        compression_level: i32,
    ) -> Result<RemoteUploadResult> {
        let meta_path = entry_dir.join("meta.json");
        let meta_content = std::fs::read_to_string(&meta_path).context("reading meta.json")?;
        let meta: EntryMeta = serde_json::from_str(&meta_content).context("parsing meta.json")?;

        let compression_start = std::time::Instant::now();
        let packed = create_entry_pack_zstd(entry_dir, blobs_dir, &meta, compression_level)?;
        let compression_ms = compression_start.elapsed().as_millis() as u64;
        let pack_bytes = packed.len() as u64;
        let original_bytes =
            meta.files.iter().map(|f| f.size).sum::<u64>() + meta_content.len() as u64;

        let pack_key = v3_pack_key(&self.remote.prefix, cache_key, crate_name);
        let put_pack_start = std::time::Instant::now();
        self.client
            .put_object()
            .bucket(&self.remote.bucket)
            .key(&pack_key)
            .body(packed.into())
            .send()
            .await
            .context("uploading v3 pack to S3")?;
        let mut network_ms = put_pack_start.elapsed().as_millis() as u64;

        let manifest = V3Manifest {
            version: V3_MANIFEST_VERSION,
            cache_key: cache_key.to_string(),
            crate_name: crate_name.to_string(),
            pack_key: pack_key.clone(),
            pack_bytes,
            original_bytes,
            file_count: meta.files.len(),
        };
        let manifest_body = serde_json::to_vec(&manifest).context("serializing v3 manifest")?;
        let manifest_len = manifest_body.len() as u64;
        let manifest_key = v3_manifest_key(&self.remote.prefix, cache_key, crate_name);

        let put_manifest_start = std::time::Instant::now();
        self.client
            .put_object()
            .bucket(&self.remote.bucket)
            .key(&manifest_key)
            .body(manifest_body.into())
            .content_type("application/json")
            .send()
            .await
            .context("uploading v3 manifest to S3")?;
        network_ms += put_manifest_start.elapsed().as_millis() as u64;

        Ok(RemoteUploadResult {
            format: "v3",
            transfer: UploadResult {
                compressed_bytes: pack_bytes + manifest_len,
                compression_ms,
                head_checks_ms: 0,
                network_ms,
            },
        })
    }

    pub async fn list_keys(&self) -> Result<HashMap<String, String>> {
        let mut keys = HashMap::new();
        let mut continuation_token: Option<String> = None;
        let manifest_prefix = format!("{}/{V3_ROOT}/{V3_MANIFESTS}/", self.remote.prefix);

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.remote.bucket)
                .prefix(&manifest_prefix);

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.context("listing v3 manifests")?;

            for obj in resp.contents() {
                if let Some(key) = obj.key()
                    && let Some(stripped) = key.strip_prefix(&manifest_prefix)
                    && let Some(without_ext) = stripped.strip_suffix(".json")
                    && let Some((crate_name, cache_key)) = without_ext.rsplit_once('/')
                {
                    keys.insert(cache_key.to_string(), crate_name.to_string());
                }
            }

            // A truncated response must carry a continuation token. Some
            // non-AWS S3 implementations (MinIO/Ceph RGW/R2/proxies) have
            // emitted is_truncated=true with a missing/empty token; treat that
            // as end-of-pagination rather than looping forever re-listing page 1.
            match resp.next_continuation_token() {
                Some(token) if resp.is_truncated == Some(true) && !token.is_empty() => {
                    continuation_token = Some(token.to_string());
                }
                _ => break,
            }
        }

        Ok(keys)
    }

    pub async fn list_keys_for_crates(
        &self,
        crate_names: &HashSet<String>,
    ) -> Result<HashMap<String, String>> {
        let mut keys = HashMap::new();

        for crate_name in crate_names {
            let mut continuation_token: Option<String> = None;
            let manifest_prefix = format!(
                "{}/{V3_ROOT}/{V3_MANIFESTS}/{crate_name}/",
                self.remote.prefix
            );

            loop {
                let mut req = self
                    .client
                    .list_objects_v2()
                    .bucket(&self.remote.bucket)
                    .prefix(&manifest_prefix);

                if let Some(token) = &continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = req
                    .send()
                    .await
                    .with_context(|| format!("listing v3 manifests for crate {crate_name}"))?;

                for obj in resp.contents() {
                    if let Some(key) = obj.key()
                        && let Some(stripped) = key.strip_prefix(&manifest_prefix)
                        && let Some(cache_key) = stripped.strip_suffix(".json")
                    {
                        keys.insert(cache_key.to_string(), crate_name.clone());
                    }
                }

                // See list_keys: guard against a truncated response with no
                // usable continuation token (non-AWS S3) to avoid an infinite
                // re-list of page 1.
                match resp.next_continuation_token() {
                    Some(token) if resp.is_truncated == Some(true) && !token.is_empty() => {
                        continuation_token = Some(token.to_string());
                    }
                    _ => break,
                }
            }
        }

        Ok(keys)
    }
}

fn v3_manifest_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    format!("{prefix}/{V3_ROOT}/{V3_MANIFESTS}/{crate_name}/{cache_key}.json")
}

fn v3_pack_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    format!("{prefix}/{V3_ROOT}/{V3_PACKS}/{crate_name}/{cache_key}.tar.zst")
}

fn is_missing_head_object(err: &HeadObjectError) -> bool {
    err.is_not_found()
        || matches!(
            err.code(),
            Some("NotFound" | "NoSuchKey" | "404" | "NoSuchObject")
        )
}

fn describe_head_object_error(err: &HeadObjectError, http_status: Option<u16>) -> String {
    let mut details = Vec::new();

    if let Some(status) = http_status {
        details.push(format!("HTTP {status}"));
    }
    if let Some(code) = err.code() {
        details.push(format!("code={code}"));
    }
    if let Some(message) = err.message() {
        details.push(format!("message={message}"));
    }

    // Fallback: if no structured info, include Display output
    if details.is_empty() || (http_status.is_none() && err.code().is_none()) {
        details.push(err.to_string());
    }

    details.join(", ")
}

// pub(crate) so other modules' tests can build a valid entry pack fixture to
// drive the S3 download-success paths against the mock (sync pull, daemon
// remote-check HIT, prefetch). Production callers are all within this module.
pub(crate) fn create_entry_pack_zstd(
    entry_dir: &Path,
    blobs_dir: &Path,
    meta: &EntryMeta,
    compression_level: i32,
) -> Result<Vec<u8>> {
    let encoder = zstd::stream::Encoder::new(Vec::new(), compression_level)
        .context("creating zstd encoder")?;
    let mut archive = tar::Builder::new(encoder);

    let meta_path = entry_dir.join("meta.json");
    archive
        .append_path_with_name(&meta_path, "meta.json")
        .with_context(|| format!("adding {} to v3 pack", meta_path.display()))?;

    for cached_file in &meta.files {
        let path = blob_path(blobs_dir, &cached_file.hash);
        archive
            .append_path_with_name(&path, &cached_file.name)
            .with_context(|| format!("adding {} to v3 pack", path.display()))?;
    }

    let encoder = archive.into_inner().context("finishing v3 tar archive")?;
    encoder.finish().context("finishing v3 zstd compression")
}

fn blob_path(blobs_dir: &Path, hash: &str) -> PathBuf {
    // Panic-safe slice; hash shape is validated at the trust boundary in
    // `extract_entry_pack`, but never let a malformed hash crash here (#211).
    let prefix = hash.get(..2).unwrap_or(hash);
    blobs_dir.join(prefix).join(hash)
}

/// A blob hash is a 64-char blake3 hex digest. Validated where untrusted
/// `meta.json` enters (download/import) so a malformed hash can never reach
/// path construction or the integrity gate (#211).
pub(crate) fn is_blob_hash(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A cached artifact's `name` must be a single, normal path component — no
/// absolute/rooted path, no `..`, no separators. `meta.json` names are
/// attacker-influenced for a shared/MITM'd bucket, and `Path::join` with an
/// absolute or `..`-bearing component escapes the entry/target dir (e.g.
/// `dir.join("/etc/x") == "/etc/x"`), giving an arbitrary read/overwrite
/// primitive. Enforced at the import and restore trust boundaries (#211).
pub(crate) fn is_safe_artifact_name(name: &str) -> bool {
    use std::path::Component;
    let mut components = Path::new(name).components();
    matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    )
}

/// A writer that tees everything written through it into a blake3 hasher, so a
/// file's content hash can be computed in the same pass that writes it to disk.
struct HashingWriter<W: std::io::Write> {
    inner: W,
    hasher: blake3::Hasher,
}

impl<W: std::io::Write> std::io::Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// True if `path` is absolute or filesystem-rooted on *either* platform.
///
/// `Path::is_absolute()` is host-specific: on Windows it is false for a
/// Unix-style `/etc/passwd` (no drive letter), so a tar produced on Unix could
/// otherwise smuggle a rooted entry past the guard when extracted on Windows
/// (and vice-versa for a `C:\...` entry on Unix). Reject any entry whose first
/// component is a root or a drive/UNC prefix, regardless of host.
fn is_rooted_path(path: &Path) -> bool {
    use std::path::Component;
    path.is_absolute()
        || matches!(
            path.components().next(),
            Some(Component::RootDir | Component::Prefix(_))
        )
}

fn extract_entry_pack<R: std::io::Read>(reader: R, dest_dir: &Path) -> Result<u64> {
    let parent = dest_dir.parent().unwrap_or(Path::new("/tmp"));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent).context("creating temp dir for v3 extraction")?;

    let mut archive = tar::Archive::new(reader);
    let mut total_bytes = 0u64;
    // blake3 of each extracted file, computed in the same pass that writes it to
    // disk (no second read) so we can verify against meta.json below.
    let mut computed_hashes: HashMap<PathBuf, String> = HashMap::new();

    for entry in archive.entries()? {
        let mut entry = entry?;
        // Bomb guard: a tar can declare an enormous entry that a tiny zstd
        // frame expands to. tar framing means the reader below yields at most
        // `entry.size()` bytes, so the running declared total upper-bounds what
        // we will ever write to disk — reject before writing anything (#212).
        total_bytes = total_bytes.saturating_add(entry.size());
        if total_bytes > MAX_EXTRACTED_BYTES {
            bail!(
                "entry pack exceeds the {MAX_EXTRACTED_BYTES}-byte extraction cap \
                 (possible decompression bomb)"
            );
        }
        let path = entry.path()?.to_path_buf();

        if is_rooted_path(&path) {
            bail!("tar entry contains absolute path: {}", path.display());
        }
        if path
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        {
            bail!("tar entry contains path traversal: {}", path.display());
        }
        if entry.header().entry_type().is_symlink() || entry.header().entry_type().is_hard_link() {
            bail!(
                "tar entry contains link (rejected for security): {}",
                path.display()
            );
        }

        let dest = tmp_dir.path().join(&path);
        if entry.header().entry_type().is_dir() {
            entry.unpack(&dest)?;
            continue;
        }
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Tee the bytes through blake3 as we write them — one pass, no re-read.
        let file =
            std::fs::File::create(&dest).with_context(|| format!("creating {}", dest.display()))?;
        let mut writer = HashingWriter {
            inner: file,
            hasher: blake3::Hasher::new(),
        };
        std::io::copy(&mut entry, &mut writer)
            .with_context(|| format!("extracting {}", path.display()))?;
        computed_hashes.insert(path, writer.hasher.finalize().to_hex().to_string());
    }

    // Integrity gate (#178): every artifact the entry declares must hash to the
    // content address meta.json advertises. A remote that serves corrupt,
    // truncated, or swapped bytes is rejected here, before the entry is
    // imported or any build can hardlink it. (Authenticating the meta.json ↔
    // key binding itself is the separate signing work in #179.)
    let meta_path = tmp_dir.path().join("meta.json");
    let meta_content =
        std::fs::read_to_string(&meta_path).context("reading downloaded meta.json")?;
    let meta: EntryMeta =
        serde_json::from_str(&meta_content).context("parsing downloaded meta.json")?;
    // Validate declared fields from the untrusted meta.json before they flow
    // into hash/path logic (#211). A malformed hash or an unsafe file name is a
    // hostile/corrupt remote — reject loudly rather than build a bad path.
    for cached_file in &meta.files {
        if !is_blob_hash(&cached_file.hash) {
            bail!(
                "downloaded entry declares a malformed blob hash for {}: {:?}",
                cached_file.name,
                cached_file.hash
            );
        }
        let name = Path::new(&cached_file.name);
        if is_rooted_path(name)
            || name
                .components()
                .any(|c| c == std::path::Component::ParentDir)
        {
            bail!(
                "downloaded entry declares an unsafe file name: {:?}",
                cached_file.name
            );
        }
    }
    for cached_file in &meta.files {
        match computed_hashes.get(Path::new(&cached_file.name)) {
            Some(actual) if actual == &cached_file.hash => {}
            Some(actual) => bail!(
                "content hash mismatch for {} (expected {}, got {})",
                cached_file.name,
                cached_file.hash,
                actual
            ),
            None => bail!(
                "downloaded entry pack is missing declared file {}",
                cached_file.name
            ),
        }
    }

    if dest_dir.exists() {
        std::fs::remove_dir_all(dest_dir).context("removing existing extracted v3 entry dir")?;
    }

    let tmp_path = tmp_dir.keep();
    std::fs::rename(&tmp_path, dest_dir).or_else(|_| {
        copy_dir_all(&tmp_path, dest_dir).and_then(|()| {
            std::fs::remove_dir_all(&tmp_path).context("removing temp dir after v3 copy")
        })
    })?;

    Ok(total_bytes)
}

fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let dest = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir_all(&entry.path(), &dest)?;
        } else {
            std::fs::copy(entry.path(), &dest)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        blob_path, copy_dir_all, create_entry_pack_zstd, extract_entry_pack, is_blob_hash,
        is_rooted_path, is_safe_artifact_name, v3_manifest_key, v3_pack_key,
    };
    use crate::config::{Config, DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS};
    use crate::store::{EntryMeta, Store};
    use aws_sdk_s3::error::ErrorMetadata;
    use aws_sdk_s3::operation::head_object::HeadObjectError;
    use std::path::Path;

    #[test]
    fn v3_keys_follow_the_documented_layout() {
        // {prefix}/v3/manifests/{crate}/{key}.json and .../packs/{crate}/{key}.tar.zst
        assert_eq!(
            v3_manifest_key("myprefix", "abc123", "serde"),
            "myprefix/v3/manifests/serde/abc123.json"
        );
        assert_eq!(
            v3_pack_key("myprefix", "abc123", "serde"),
            "myprefix/v3/packs/serde/abc123.tar.zst"
        );
    }

    #[test]
    fn is_rooted_path_flags_absolute_and_prefixed_paths() {
        assert!(is_rooted_path(Path::new("/etc/passwd")));
        assert!(!is_rooted_path(Path::new("deps/libfoo.rlib")));
        assert!(!is_rooted_path(Path::new("foo")));
        assert!(!is_rooted_path(Path::new("../escape")));
        #[cfg(windows)]
        {
            assert!(is_rooted_path(Path::new(r"C:\Windows")));
            assert!(is_rooted_path(Path::new(r"\\server\share")));
        }
    }

    #[test]
    fn copy_dir_all_replicates_nested_tree() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(src.path().join("a/b")).unwrap();
        std::fs::write(src.path().join("top.txt"), b"top").unwrap();
        std::fs::write(src.path().join("a/mid.txt"), b"mid").unwrap();
        std::fs::write(src.path().join("a/b/leaf.txt"), b"leaf").unwrap();

        let target = dst.path().join("copy");
        copy_dir_all(src.path(), &target).unwrap();

        assert_eq!(std::fs::read(target.join("top.txt")).unwrap(), b"top");
        assert_eq!(std::fs::read(target.join("a/mid.txt")).unwrap(), b"mid");
        assert_eq!(std::fs::read(target.join("a/b/leaf.txt")).unwrap(), b"leaf");
    }

    /// #211: the trust-boundary hash validator accepts only a 64-char blake3
    /// hex digest and rejects everything a hostile/corrupt meta.json might
    /// carry (empty, short, wrong length, non-hex, traversal-shaped).
    #[test]
    fn is_blob_hash_accepts_only_blake3_hex() {
        assert!(is_blob_hash(&"a".repeat(64)));
        assert!(is_blob_hash(&"0123456789abcdef".repeat(4)));
        assert!(!is_blob_hash(""));
        assert!(!is_blob_hash("ab"));
        assert!(!is_blob_hash(&"a".repeat(63)));
        assert!(!is_blob_hash(&"a".repeat(65)));
        assert!(!is_blob_hash(&"g".repeat(64))); // non-hex
        assert!(!is_blob_hash("../../etc/passwd"));
    }

    /// #211: a cached artifact name must be a single normal component — reject
    /// absolute, rooted, parent-dir, separator-bearing, and empty names.
    #[test]
    fn is_safe_artifact_name_requires_single_normal_component() {
        assert!(is_safe_artifact_name("libfoo-abc123.rlib"));
        assert!(is_safe_artifact_name("foo.d"));
        assert!(!is_safe_artifact_name(""));
        assert!(!is_safe_artifact_name("/etc/passwd"));
        assert!(!is_safe_artifact_name("../escape"));
        assert!(!is_safe_artifact_name("a/b"));
        assert!(!is_safe_artifact_name("./a"));
        assert!(!is_safe_artifact_name(".."));
    }

    /// #211: building a blob path from a malformed (short) hash must not panic
    /// on the `[..2]` slice, even though such a hash is rejected upstream.
    #[test]
    fn blob_path_is_panic_safe_for_short_hash() {
        let dir = std::path::Path::new("/store/blobs");
        let _ = blob_path(dir, "a");
        let _ = blob_path(dir, "");
    }

    #[test]
    fn v3_pack_roundtrip_restores_meta_and_files() {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: tmp.path().join("cache"),
            max_size: 1024 * 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        };
        let store = Store::open(&config).unwrap();

        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let source_file = source_dir.join("libfoo.rlib");
        std::fs::write(&source_file, b"hello world").unwrap();

        store
            .put(
                "key123",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(source_file, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let entry_dir = store.entry_dir("key123");
        let meta: crate::store::EntryMeta =
            serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).unwrap()).unwrap();

        let packed = create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3).unwrap();

        let restored = tmp.path().join("restored");
        let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&packed)).unwrap();
        let original_bytes = extract_entry_pack(decoder, &restored).unwrap();

        assert!(original_bytes >= 11);
        assert_eq!(
            std::fs::read(restored.join("libfoo.rlib")).unwrap(),
            b"hello world"
        );
        let restored_meta: EntryMeta =
            serde_json::from_slice(&std::fs::read(restored.join("meta.json")).unwrap()).unwrap();
        assert_eq!(restored_meta.cache_key, "key123");
        assert_eq!(restored_meta.files.len(), 1);

        let restore_cache_dir = tmp.path().join("restore-cache");
        let restore_config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: restore_cache_dir,
            max_size: 1024 * 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        };
        let restore_store = Store::open(&restore_config).unwrap();
        let restore_entry_dir = restore_store.entry_dir("key123");
        if let Some(parent) = restore_entry_dir.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::rename(&restored, &restore_entry_dir).unwrap();

        restore_store.import_restored_entry("key123").unwrap();
        let restored_entry = restore_store.get("key123").unwrap().unwrap();
        assert_eq!(restored_entry.cache_key, "key123");
        assert_eq!(restored_entry.files.len(), 1);
        assert_eq!(restored_entry.files[0].name, "libfoo.rlib");

        let blob = blob_path(&restore_store.blobs_dir(), &restored_entry.files[0].hash);
        assert_eq!(std::fs::read(blob).unwrap(), b"hello world");
        assert!(restore_entry_dir.join("meta.json").exists());
        assert!(!restore_entry_dir.join("libfoo.rlib").exists());
    }

    #[test]
    fn v3_extract_rejects_content_hash_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: tmp.path().join("cache"),
            max_size: 1024 * 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        };
        let store = Store::open(&config).unwrap();

        // Produce a genuine entry so meta.json carries the real hash of
        // "hello world".
        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let source_file = source_dir.join("libfoo.rlib");
        std::fs::write(&source_file, b"hello world").unwrap();
        store
            .put(
                "key123",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(source_file, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let entry_dir = store.entry_dir("key123");

        // Build a pack with the genuine meta.json but tampered artifact bytes,
        // simulating a corrupt/poisoned remote.
        let tampered = source_dir.join("tampered.rlib");
        std::fs::write(&tampered, b"TAMPERED bytes that do not match the hash").unwrap();
        let encoder = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
        let mut archive = tar::Builder::new(encoder);
        archive
            .append_path_with_name(entry_dir.join("meta.json"), "meta.json")
            .unwrap();
        archive
            .append_path_with_name(&tampered, "libfoo.rlib")
            .unwrap();
        let encoder = archive.into_inner().unwrap();
        let packed = encoder.finish().unwrap();

        let restored = tmp.path().join("restored");
        let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&packed)).unwrap();
        let err = extract_entry_pack(decoder, &restored).unwrap_err();
        assert!(
            err.to_string().contains("content hash mismatch"),
            "expected a hash-mismatch rejection, got: {err}"
        );
        // The poisoned entry must not be published to its destination.
        assert!(!restored.exists());
    }

    fn build_raw_tar(filename: &[u8], body: &[u8]) -> Vec<u8> {
        let mut header = [0u8; 512];
        let name_len = filename.len().min(100);
        header[..name_len].copy_from_slice(&filename[..name_len]);
        header[100..107].copy_from_slice(b"0000644");
        header[107] = 0;
        let size_str = format!("{:011o}", body.len());
        header[124..135].copy_from_slice(size_str.as_bytes());
        header[135] = 0;
        header[156] = b'0';
        header[148..156].fill(b' ');
        let checksum: u32 = header.iter().map(|&b| b as u32).sum();
        let checksum_str = format!("{:06o}\0 ", checksum);
        header[148..156].copy_from_slice(checksum_str.as_bytes());

        let mut tar_data = Vec::new();
        tar_data.extend_from_slice(&header);
        tar_data.extend_from_slice(body);
        let padding = (512 - (body.len() % 512)) % 512;
        tar_data.extend(std::iter::repeat_n(0u8, padding));
        tar_data.extend(std::iter::repeat_n(0u8, 1024));
        tar_data
    }

    #[test]
    fn v3_extract_rejects_absolute_path() {
        let tar_data = build_raw_tar(b"/etc/passwd", b"evil");
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out");
        let result = extract_entry_pack(std::io::Cursor::new(&tar_data), &dest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("absolute path"));
    }

    #[test]
    fn v3_extract_rejects_path_traversal() {
        let tar_data = build_raw_tar(b"../escape.txt", b"evil");
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out");
        let result = extract_entry_pack(std::io::Cursor::new(&tar_data), &dest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path traversal"));
    }

    #[test]
    fn generic_head_object_not_found_codes_are_treated_as_misses() {
        let err = HeadObjectError::generic(ErrorMetadata::builder().code("NotFound").build());
        assert!(super::is_missing_head_object(&err));

        let err = HeadObjectError::generic(ErrorMetadata::builder().code("NoSuchKey").build());
        assert!(super::is_missing_head_object(&err));
    }

    #[test]
    fn generic_head_object_non_miss_code_is_not_treated_as_missing() {
        let err = HeadObjectError::generic(
            ErrorMetadata::builder()
                .code("SignatureDoesNotMatch")
                .build(),
        );
        assert!(!super::is_missing_head_object(&err));
    }

    // ── Mock-S3 round-trips for the v3 RemoteLayout ─────────────────────────
    //
    // Drive RemoteLayout against an in-process wire mock (no network) to cover
    // the head/get/put object paths and pack (de)serialization end to end.
    use super::RemoteLayout;
    use crate::config::RemoteConfig;
    use aws_smithy_http_client::test_util::wire::{ReplayedEvent, WireMockServer};

    fn min_config(cache_dir: std::path::PathBuf) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir,
            max_size: 1024 * 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        }
    }

    fn test_remote() -> RemoteConfig {
        RemoteConfig {
            bucket: "bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            prefix: "artifacts".to_string(),
            profile: None,
        }
    }

    async fn mock_client(events: Vec<ReplayedEvent>) -> (WireMockServer, aws_sdk_s3::Client) {
        let server = WireMockServer::start(events).await;
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "AK", "SK", None, None, "test",
            ))
            .endpoint_url(server.endpoint_url())
            .http_client(server.http_client())
            .force_path_style(true)
            .build();
        (server, aws_sdk_s3::Client::from_conf(conf))
    }

    /// Build a one-file store entry and return (tmpdir, store, entry_dir).
    fn populated_entry() -> (tempfile::TempDir, Store, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&min_config(tmp.path().join("cache"))).unwrap();
        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let source_file = source_dir.join("libfoo.rlib");
        std::fs::write(&source_file, b"hello world").unwrap();
        store
            .put(
                "key123",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(source_file, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let entry_dir = store.entry_dir("key123");
        (tmp, store, entry_dir)
    }

    #[tokio::test]
    async fn exists_entry_true_on_200_false_on_404() {
        let remote = test_remote();

        let (server, client) = mock_client(vec![ReplayedEvent::ok()]).await;
        let layout = RemoteLayout::new(&client, &remote);
        assert!(layout.exists_entry("key123", "foo").await.unwrap());
        server.shutdown();

        let (server, client) = mock_client(vec![ReplayedEvent::status(404)]).await;
        let layout = RemoteLayout::new(&client, &remote);
        assert!(!layout.exists_entry("key123", "foo").await.unwrap());
        server.shutdown();
    }

    #[tokio::test]
    async fn exists_entry_propagates_unexpected_errors() {
        // A 403 is not a "missing" code, so it must surface as an error rather
        // than a silent false (exercises describe_head_object_error).
        let remote = test_remote();
        let (server, client) = mock_client(vec![ReplayedEvent::status(403)]).await;
        let layout = RemoteLayout::new(&client, &remote);
        assert!(layout.exists_entry("key123", "foo").await.is_err());
        server.shutdown();
    }

    #[tokio::test]
    async fn download_entry_extracts_a_served_pack() {
        let (_tmp, store, entry_dir) = populated_entry();
        let meta: EntryMeta =
            serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).unwrap()).unwrap();
        let packed = create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3).unwrap();

        let remote = test_remote();
        let (server, client) = mock_client(vec![ReplayedEvent::with_body(&packed)]).await;
        let layout = RemoteLayout::new(&client, &remote);

        let dest = _tmp.path().join("restored");
        let result = layout
            .download_entry("key123", "foo", &dest, &store.blobs_dir())
            .await
            .expect("download_entry should succeed");
        assert_eq!(result.format, "v3");
        assert_eq!(
            std::fs::read(dest.join("libfoo.rlib")).unwrap(),
            b"hello world"
        );
        server.shutdown();
    }

    #[tokio::test]
    async fn upload_entry_puts_pack_and_manifest() {
        let (_tmp, store, entry_dir) = populated_entry();
        let remote = test_remote();
        // upload_entry issues two PutObjects: the pack, then the manifest.
        let (server, client) = mock_client(vec![ReplayedEvent::ok(), ReplayedEvent::ok()]).await;
        let layout = RemoteLayout::new(&client, &remote);

        let result = layout
            .upload_entry("key123", "foo", &entry_dir, &store.blobs_dir(), 3)
            .await
            .expect("upload_entry should succeed");
        assert_eq!(result.format, "v3");
        server.shutdown();
    }
}
