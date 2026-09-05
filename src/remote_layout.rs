use anyhow::{Context, Result, bail};
pub(crate) use kache_format::{is_blob_hash, is_safe_artifact_name};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::config::RemoteConfig;
use crate::remote::{DownloadResult, UploadResult};
use crate::remote_backend::RemoteBackend;
use crate::store::{EntryMeta, VerifiedRestoredEntry};

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
    backend: &'a dyn RemoteBackend,
    remote: &'a RemoteConfig,
}

pub struct RemoteUploadResult {
    pub format: &'static str,
    pub transfer: UploadResult,
}

/// A packed-prefetch entry validated while its artifact bytes were streamed
/// to disk. The store can batch-register this without hashing the artifacts a
/// second time.
pub(crate) struct VerifiedPackEntry {
    pub restored: VerifiedRestoredEntry,
    pub original_bytes: u64,
    pub extract_ms: u64,
}

struct ExpectedEntryBinding<'a> {
    cache_key: &'a str,
    crate_name: &'a str,
    meta_digest: &'a str,
}

impl<'a> RemoteLayout<'a> {
    pub fn new(backend: &'a dyn RemoteBackend, remote: &'a RemoteConfig) -> Self {
        Self { backend, remote }
    }

    pub async fn exists_entry(&self, cache_key: &str, crate_name: &str) -> Result<bool> {
        let object_key = v3_manifest_key(&self.remote.prefix, cache_key, crate_name);
        self.backend.head(&object_key).await
    }

    pub async fn download_entry(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        _blobs_dir: &Path,
    ) -> Result<DownloadResult> {
        self.download_entry_until(cache_key, crate_name, entry_dir, _blobs_dir, None)
            .await
    }

    /// Deadline-aware restore used by every daemon download path. The same
    /// monotonic instant covers GET and synchronous decompression/extraction.
    pub async fn download_entry_until(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        _blobs_dir: &Path,
        deadline: Option<Instant>,
    ) -> Result<DownloadResult> {
        let object_key = v3_pack_key(&self.remote.prefix, cache_key, crate_name);

        tracing::debug!("downloading v3 pack {}", self.backend.describe(&object_key));

        // A missing object is a clean miss, not a transfer failure. Callers
        // downcast to `EntryNotFound` to take the miss path (#485 Phase 0):
        // likely-present keys go straight to GET, and a stale key-cache
        // positive degrades to a miss, not an error.
        let fetched = crate::remote_resilience::RemoteDeadline::from_instant(deadline)
            .run("remote object GET", self.backend.get(&object_key, None))
            .await
            .context("downloading v3 pack")?
            .ok_or_else(|| {
                anyhow::Error::new(EntryNotFound).context(format!(
                    "v3 pack not found: {}",
                    self.backend.describe(&object_key)
                ))
            })?;
        let request_ms = fetched.request_ms;
        let body_ms = fetched.body_ms;
        let compressed = fetched.body;
        let compressed_len = compressed.len() as u64;

        let extract_start = std::time::Instant::now();
        let guarded = DeadlineReader {
            inner: std::io::Cursor::new(&compressed),
            deadline,
        };
        let mut decoder =
            zstd::stream::Decoder::new(guarded).context("creating v3 zstd decoder")?;
        // Bound the decompression window so a hostile/buggy frame can't force a
        // huge allocation. 27 = 128 MiB, well above what level-3 packs use and
        // independent of the bomb guard on extracted bytes below (#212).
        decoder
            .window_log_max(MAX_ZSTD_WINDOW_LOG)
            .context("setting v3 zstd window-log cap")?;
        let extracted = extract_entry_pack_until(decoder, entry_dir, deadline, None)?;
        let extract_ms = extract_start.elapsed().as_millis() as u64;

        Ok(DownloadResult {
            format: "v3",
            object_key,
            compressed_bytes: compressed_len,
            original_bytes: extracted.original_bytes,
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
        self.upload_entry_until(
            cache_key,
            crate_name,
            entry_dir,
            blobs_dir,
            compression_level,
            None,
        )
        .await
    }

    /// Deadline-aware upload. The same monotonic instant covers local pack
    /// construction and both remote PUTs, so synchronous compression cannot
    /// silently overrun the daemon's operation budget.
    pub async fn upload_entry_until(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        compression_level: i32,
        deadline: Option<Instant>,
    ) -> Result<RemoteUploadResult> {
        deadline_io_check(deadline, "upload preparation")?;
        let meta_path = entry_dir.join("meta.json");
        let meta_content = std::fs::read_to_string(&meta_path).context("reading meta.json")?;
        let meta: EntryMeta = serde_json::from_str(&meta_content).context("parsing meta.json")?;

        let compression_start = std::time::Instant::now();
        let packed =
            create_entry_pack_zstd_until(entry_dir, blobs_dir, &meta, compression_level, deadline)?;
        let compression_ms = compression_start.elapsed().as_millis() as u64;
        let pack_bytes = packed.len() as u64;
        let original_bytes =
            meta.files.iter().map(|f| f.size).sum::<u64>() + meta_content.len() as u64;

        let pack_key = v3_pack_key(&self.remote.prefix, cache_key, crate_name);
        let put_pack_start = std::time::Instant::now();
        self.backend
            .put(&pack_key, packed, None)
            .await
            .context("uploading v3 pack")?;
        deadline_io_check(deadline, "pack PUT")?;
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
        self.backend
            .put(&manifest_key, manifest_body, Some("application/json"))
            .await
            .context("uploading v3 manifest")?;
        deadline_io_check(deadline, "manifest PUT")?;
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
        let manifest_prefix = crate::config::join_remote_key(
            &self.remote.prefix,
            &format!("{V3_ROOT}/{V3_MANIFESTS}/"),
        );
        let objects = self
            .backend
            .list(&manifest_prefix)
            .await
            .context("listing v3 manifests")?;

        let keys = objects
            .iter()
            .filter_map(|key| {
                let stripped = key.strip_prefix(&manifest_prefix)?;
                let without_ext = stripped.strip_suffix(".json")?;
                let (crate_name, cache_key) = without_ext.rsplit_once('/')?;
                (crate::cache_key::is_valid_cache_key(cache_key)
                    && crate::cache_key::is_valid_crate_name(crate_name))
                .then(|| (cache_key.to_string(), crate_name.to_string()))
            })
            .collect();

        Ok(keys)
    }

    pub async fn list_keys_for_crates(
        &self,
        crate_names: &HashSet<String>,
    ) -> Result<HashMap<String, String>> {
        let mut keys = HashMap::new();

        for crate_name in crate_names {
            let manifest_prefix = crate::config::join_remote_key(
                &self.remote.prefix,
                &format!("{V3_ROOT}/{V3_MANIFESTS}/{crate_name}/"),
            );
            let objects = self
                .backend
                .list(&manifest_prefix)
                .await
                .with_context(|| format!("listing v3 manifests for crate {crate_name}"))?;

            keys.extend(objects.iter().filter_map(|key| {
                let stripped = key.strip_prefix(&manifest_prefix)?;
                let cache_key = stripped.strip_suffix(".json")?;
                crate::cache_key::is_valid_cache_key(cache_key)
                    .then(|| (cache_key.to_string(), crate_name.clone()))
            }));
        }

        Ok(keys)
    }
}

/// Extract one existing v3 payload carried inside an immutable prefetch pack.
/// The outer pack binds cache key, crate and exact `meta.json` digest; this
/// function verifies those bindings plus every artifact hash before publishing
/// the entry directory.
pub(crate) fn extract_verified_prefetch_entry(
    cache_key: &str,
    crate_name: &str,
    meta_digest: &str,
    payload: &[u8],
    entry_dir: &Path,
    deadline: Option<Instant>,
) -> Result<VerifiedPackEntry> {
    if !crate::cache_key::is_valid_cache_key(cache_key)
        || !crate::cache_key::is_valid_crate_name(crate_name)
        || !crate::cache_key::is_valid_cache_key(meta_digest)
    {
        bail!("invalid packed-prefetch entry binding");
    }
    let extract_start = Instant::now();
    let guarded = DeadlineReader {
        inner: std::io::Cursor::new(payload),
        deadline,
    };
    let mut decoder = zstd::stream::Decoder::new(guarded)
        .context("creating packed-prefetch entry zstd decoder")?;
    decoder
        .window_log_max(MAX_ZSTD_WINDOW_LOG)
        .context("setting packed-prefetch zstd window-log cap")?;
    let extracted = extract_entry_pack_until(
        decoder,
        entry_dir,
        deadline,
        Some(ExpectedEntryBinding {
            cache_key,
            crate_name,
            meta_digest,
        }),
    )?;
    Ok(VerifiedPackEntry {
        restored: VerifiedRestoredEntry {
            cache_key: cache_key.to_string(),
            meta: extracted.meta,
        },
        original_bytes: extracted.original_bytes,
        extract_ms: extract_start.elapsed().as_millis() as u64,
    })
}

fn v3_manifest_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    crate::config::join_remote_key(
        prefix,
        &format!("{V3_ROOT}/{V3_MANIFESTS}/{crate_name}/{cache_key}.json"),
    )
}

fn v3_pack_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    crate::config::join_remote_key(
        prefix,
        &format!("{V3_ROOT}/{V3_PACKS}/{crate_name}/{cache_key}.tar.zst"),
    )
}

/// Marker error: the requested entry does not exist in the remote (GET 404).
/// Downcast target for callers that treat absence as a clean miss rather than
/// a transfer failure (#485 Phase 0).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryNotFound;

impl std::fmt::Display for EntryNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("remote entry not found")
    }
}

impl std::error::Error for EntryNotFound {}

// pub(crate) so other modules' tests can build a valid entry pack fixture to
// drive the remote download-success paths against the mock (sync pull, daemon
// remote-check HIT, prefetch). Production callers are all within this module.
#[cfg(test)]
pub(crate) fn create_entry_pack_zstd(
    entry_dir: &Path,
    blobs_dir: &Path,
    meta: &EntryMeta,
    compression_level: i32,
) -> Result<Vec<u8>> {
    create_entry_pack_zstd_until(entry_dir, blobs_dir, meta, compression_level, None)
}

fn create_entry_pack_zstd_until(
    entry_dir: &Path,
    blobs_dir: &Path,
    meta: &EntryMeta,
    compression_level: i32,
    deadline: Option<Instant>,
) -> Result<Vec<u8>> {
    deadline_io_check(deadline, "upload compression")?;
    let output = DeadlineWriter {
        inner: Vec::new(),
        deadline,
        stage: "upload compression",
    };
    let encoder =
        zstd::stream::Encoder::new(output, compression_level).context("creating zstd encoder")?;
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
    let output = encoder.finish().context("finishing v3 zstd compression")?;
    deadline_io_check(deadline, "upload compression")?;
    Ok(output.inner)
}

fn blob_path(blobs_dir: &Path, hash: &str) -> PathBuf {
    // Panic-safe slice; hash shape is validated at the trust boundary in
    // `extract_entry_pack`, but never let a malformed hash crash here (#211).
    let prefix = hash.get(..2).unwrap_or(hash);
    blobs_dir.join(prefix).join(hash)
}

/// A writer that tees everything written through it into a blake3 hasher, so a
/// file's content hash can be computed in the same pass that writes it to disk.
struct DeadlineReader<R> {
    inner: R,
    deadline: Option<Instant>,
}

struct DeadlineWriter<W> {
    inner: W,
    deadline: Option<Instant>,
    stage: &'static str,
}

impl<W: std::io::Write> std::io::Write for DeadlineWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        deadline_io_check(self.deadline, self.stage)?;
        let written = self.inner.write(buf)?;
        deadline_io_check(self.deadline, self.stage)?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        deadline_io_check(self.deadline, self.stage)?;
        self.inner.flush()?;
        deadline_io_check(self.deadline, self.stage)
    }
}

fn deadline_io_check(deadline: Option<Instant>, stage: &'static str) -> std::io::Result<()> {
    if deadline.is_some_and(|at| Instant::now() >= at) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!("remote deadline elapsed during {stage}"),
        ));
    }
    Ok(())
}

impl<R: std::io::Read> std::io::Read for DeadlineReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        deadline_io_check(self.deadline, "decompression")?;
        let read = self.inner.read(buf)?;
        deadline_io_check(self.deadline, "decompression")?;
        Ok(read)
    }
}

struct HashingWriter<W: std::io::Write> {
    inner: W,
    hasher: blake3::Hasher,
    deadline: Option<Instant>,
}

impl<W: std::io::Write> std::io::Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        deadline_io_check(self.deadline, "extraction write")?;
        let n = self.inner.write(buf)?;
        deadline_io_check(self.deadline, "extraction write")?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        deadline_io_check(self.deadline, "extraction flush")?;
        self.inner.flush()?;
        deadline_io_check(self.deadline, "extraction flush")
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

#[cfg(test)]
fn extract_entry_pack<R: std::io::Read>(reader: R, dest_dir: &Path) -> Result<u64> {
    Ok(extract_entry_pack_until(reader, dest_dir, None, None)?.original_bytes)
}

struct ExtractedEntry {
    original_bytes: u64,
    meta: EntryMeta,
}

fn extract_entry_pack_until<R: std::io::Read>(
    reader: R,
    dest_dir: &Path,
    deadline: Option<Instant>,
    expected: Option<ExpectedEntryBinding<'_>>,
) -> Result<ExtractedEntry> {
    deadline_io_check(deadline, "extraction setup")?;
    let parent = dest_dir.parent().unwrap_or(Path::new("/tmp"));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent).context("creating temp dir for v3 extraction")?;

    let mut archive = tar::Archive::new(reader);
    let mut total_bytes = 0u64;
    // blake3 of each extracted file, computed in the same pass that writes it to
    // disk (no second read) so we can verify against meta.json below.
    let mut computed_hashes: HashMap<PathBuf, String> = HashMap::new();

    for entry in archive.entries()? {
        deadline_io_check(deadline, "tar extraction")?;
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
            deadline,
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
    deadline_io_check(deadline, "integrity validation")?;
    let meta_content =
        std::fs::read_to_string(&meta_path).context("reading downloaded meta.json")?;
    let meta: EntryMeta =
        serde_json::from_str(&meta_content).context("parsing downloaded meta.json")?;
    if meta.key_schema != crate::cache_key::CACHE_KEY_VERSION {
        bail!(
            "downloaded entry uses incompatible key schema {}",
            meta.key_schema
        );
    }
    if let Some(expected) = expected {
        if meta.cache_key != expected.cache_key || meta.crate_name != expected.crate_name {
            bail!("packed-prefetch cache-key or crate binding mismatch");
        }
        let actual_meta_digest = computed_hashes
            .get(Path::new("meta.json"))
            .context("missing streamed meta.json digest")?;
        if actual_meta_digest != expected.meta_digest {
            bail!(
                "packed-prefetch meta.json digest mismatch (expected {}, got {})",
                expected.meta_digest,
                actual_meta_digest
            );
        }
    }
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
        if !is_safe_artifact_name(&cached_file.name) {
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
        deadline_io_check(deadline, "entry publication")?;
        std::fs::remove_dir_all(dest_dir).context("removing existing extracted v3 entry dir")?;
    }

    let tmp_path = tmp_dir.keep();
    std::fs::rename(&tmp_path, dest_dir).or_else(|_| {
        copy_dir_all_until(&tmp_path, dest_dir, deadline).and_then(|()| {
            std::fs::remove_dir_all(&tmp_path).context("removing temp dir after v3 copy")
        })
    })?;

    deadline_io_check(deadline, "entry publication")?;

    Ok(ExtractedEntry {
        original_bytes: total_bytes,
        meta,
    })
}

#[cfg(test)]
fn copy_dir_all(src: &Path, dst: &Path) -> Result<()> {
    copy_dir_all_until(src, dst, None)
}

fn copy_dir_all_until(src: &Path, dst: &Path, deadline: Option<Instant>) -> Result<()> {
    deadline_io_check(deadline, "entry copy")?;
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        deadline_io_check(deadline, "entry copy")?;
        let entry = entry?;
        let dest = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir_all_until(&entry.path(), &dest, deadline)?;
        } else {
            std::fs::copy(entry.path(), &dest)?;
            deadline_io_check(deadline, "entry copy")?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        DeadlineReader, DeadlineWriter, HashingWriter, RemoteLayout, V3Manifest, blob_path,
        copy_dir_all, create_entry_pack_zstd, extract_entry_pack, extract_verified_prefetch_entry,
        is_rooted_path, v3_manifest_key, v3_pack_key,
    };
    use crate::config::{
        Config, DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS, RemoteConfig,
    };
    use crate::remote_backend::{GetObject, RemoteBackend, memory_backend};
    use crate::store::{CachedFile, EntryMeta, Store};
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};
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

    #[test]
    fn expired_deadline_interrupts_synchronous_read_and_write_work() {
        let expired = Some(std::time::Instant::now() - std::time::Duration::from_millis(1));
        let mut reader = DeadlineReader {
            inner: std::io::Cursor::new(b"compressed"),
            deadline: expired,
        };
        let read_error = std::io::Read::read(&mut reader, &mut [0_u8; 4]).unwrap_err();
        assert_eq!(read_error.kind(), std::io::ErrorKind::TimedOut);

        let mut writer = DeadlineWriter {
            inner: Vec::new(),
            deadline: expired,
            stage: "upload compression",
        };
        let write_error = std::io::Write::write_all(&mut writer, b"pack").unwrap_err();
        assert_eq!(write_error.kind(), std::io::ErrorKind::TimedOut);
        let flush_error = std::io::Write::flush(&mut writer).unwrap_err();
        assert_eq!(flush_error.kind(), std::io::ErrorKind::TimedOut);

        let mut hashing_writer = HashingWriter {
            inner: Vec::new(),
            hasher: blake3::Hasher::new(),
            deadline: expired,
        };
        let flush_error = std::io::Write::flush(&mut hashing_writer).unwrap_err();
        assert_eq!(flush_error.kind(), std::io::ErrorKind::TimedOut);
    }

    /// #211: building a blob path from a malformed (short) hash must not panic
    /// on the `[..2]` slice, even though such a hash is rejected upstream.
    #[test]
    fn blob_path_is_panic_safe_for_short_hash() {
        let dir = std::path::Path::new("/store/blobs");
        let _ = blob_path(dir, "a");
        let _ = blob_path(dir, "");
    }

    fn arbitrary_artifacts() -> impl Strategy<Value = BTreeMap<u16, Vec<u8>>> {
        proptest::collection::btree_map(
            any::<u16>(),
            proptest::collection::vec(any::<u8>(), 0..8193),
            2..7,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 12,
            max_shrink_iters: 64,
            ..ProptestConfig::default()
        })]

        #[test]
        fn property_v3_pack_round_trips_multiple_binary_artifacts(
            artifacts in arbitrary_artifacts(),
        ) {
            let tmp = tempfile::tempdir().unwrap();
            let entry_dir = tmp.path().join("entry");
            let blobs_dir = tmp.path().join("blobs");
            let restored_dir = tmp.path().join("restored");
            std::fs::create_dir_all(&entry_dir).unwrap();

            let mut files = Vec::with_capacity(artifacts.len());
            for (id, contents) in &artifacts {
                let name = format!("artifact_{id}.bin");
                let hash = blake3::hash(contents).to_hex().to_string();
                let path = blob_path(&blobs_dir, &hash);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, contents).unwrap();
                files.push(CachedFile {
                    name,
                    size: contents.len() as u64,
                    hash,
                    executable: false,
                });
            }

            let meta = EntryMeta {
                cache_key: blake3::hash(b"property-v3-entry").to_hex().to_string(),
                key_schema: crate::cache_key::CACHE_KEY_VERSION,
                crate_name: "property_crate".to_string(),
                crate_types: vec!["lib".to_string()],
                files,
                stdout: String::new(),
                stderr: String::new(),
                features: Vec::new(),
                target: "x86_64-unknown-linux-gnu".to_string(),
                profile: "debug".to_string(),
                compile_time_ms: 1,
                emit_kinds: vec!["link".to_string()],
            };
            let meta_bytes = serde_json::to_vec(&meta).unwrap();
            std::fs::write(entry_dir.join("meta.json"), &meta_bytes).unwrap();

            let packed = create_entry_pack_zstd(&entry_dir, &blobs_dir, &meta, 1).unwrap();
            let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&packed)).unwrap();
            let original_bytes = extract_entry_pack(decoder, &restored_dir).unwrap();
            let expected_original_bytes = meta_bytes.len() as u64
                + artifacts
                    .values()
                    .map(|contents| contents.len() as u64)
                    .sum::<u64>();
            prop_assert_eq!(original_bytes, expected_original_bytes);

            for (id, contents) in &artifacts {
                let name = format!("artifact_{id}.bin");
                let restored = std::fs::read(restored_dir.join(name)).unwrap();
                prop_assert_eq!(restored.as_slice(), contents.as_slice());
            }

            let restored_meta: EntryMeta = serde_json::from_slice(
                &std::fs::read(restored_dir.join("meta.json")).unwrap(),
            )
            .unwrap();
            prop_assert_eq!(restored_meta, meta);

            let actual_names = std::fs::read_dir(&restored_dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                .collect::<BTreeSet<_>>();
            let mut expected_names = artifacts
                .keys()
                .map(|id| format!("artifact_{id}.bin"))
                .collect::<BTreeSet<_>>();
            expected_names.insert("meta.json".to_string());
            prop_assert_eq!(actual_names, expected_names);
        }
    }

    #[test]
    fn v3_pack_roundtrip_restores_meta_and_files() {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            input_predictions: false,
            volume_stores: Vec::new(),
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: tmp.path().join("cache"),
            runtime_dir: tmp.path().join("cache"),
            max_size: 1024 * 1024,
            remote: None,
            remote_error: None,
            socket_path_override: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
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
            remote_readonly: false,
            modified_input_guard: false,
            input_predictions: false,
            volume_stores: Vec::new(),
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            runtime_dir: restore_cache_dir.clone(),
            cache_dir: restore_cache_dir,
            max_size: 1024 * 1024,
            remote: None,
            remote_error: None,
            socket_path_override: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
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
    fn packed_prefetch_checks_each_outer_and_inner_binding_independently() {
        let tmp = tempfile::tempdir().unwrap();
        let config = min_config(tmp.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"packed-binding-key").to_hex().to_string();
        let other_key = blake3::hash(b"other-packed-binding-key")
            .to_hex()
            .to_string();
        let source = tmp.path().join("source.rlib");
        std::fs::write(&source, b"packed binding artifact").unwrap();
        store
            .put(
                &key,
                "serde",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(source, "libserde.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let entry_dir = store.entry_dir(&key);
        let meta: EntryMeta =
            serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).unwrap()).unwrap();
        let meta_digest = blake3::hash(&std::fs::read(entry_dir.join("meta.json")).unwrap())
            .to_hex()
            .to_string();
        let packed = create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3).unwrap();

        for (cache_key, crate_name, expected_error) in [
            ("invalid", "serde", "invalid packed-prefetch entry binding"),
            (
                key.as_str(),
                "../unsafe",
                "invalid packed-prefetch entry binding",
            ),
            (
                other_key.as_str(),
                "serde",
                "packed-prefetch cache-key or crate binding mismatch",
            ),
            (
                key.as_str(),
                "tokio",
                "packed-prefetch cache-key or crate binding mismatch",
            ),
        ] {
            let restored = tmp
                .path()
                .join(format!("restored-{}", blake3::hash(cache_key.as_bytes())));
            let err = extract_verified_prefetch_entry(
                cache_key,
                crate_name,
                &meta_digest,
                &packed,
                &restored,
                None,
            )
            .err()
            .expect("a mismatched binding must be rejected");
            assert!(
                err.to_string().contains(expected_error),
                "expected {expected_error:?}, got {err}"
            );
            assert!(!restored.exists());
        }

        let restored = tmp.path().join("restored-invalid-meta-digest");
        let err =
            extract_verified_prefetch_entry(&key, "serde", "invalid", &packed, &restored, None)
                .err()
                .expect("a malformed metadata digest must be rejected at the outer binding gate");
        assert!(
            err.to_string()
                .contains("invalid packed-prefetch entry binding")
        );
        assert!(!restored.exists());
    }

    #[test]
    fn v3_extract_rejects_content_hash_mismatch() {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            input_predictions: false,
            volume_stores: Vec::new(),
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir: tmp.path().join("cache"),
            runtime_dir: tmp.path().join("cache"),
            max_size: 1024 * 1024,
            remote: None,
            remote_error: None,
            socket_path_override: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
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

    // ── Backend-neutral round-trips for the v3 RemoteLayout ─────────────────
    //
    // Drive RemoteLayout through its byte-object seam so the layout behavior
    // is exercised identically for S3 and filesystem transports.

    fn min_config(cache_dir: std::path::PathBuf) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            input_predictions: false,
            volume_stores: Vec::new(),
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            runtime_dir: cache_dir.clone(),
            cache_dir,
            max_size: 1024 * 1024,
            remote: None,
            remote_error: None,
            socket_path_override: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        }
    }

    fn test_remote() -> RemoteConfig {
        RemoteConfig::test_s3("bucket", "artifacts")
    }

    struct FailingBackend;

    #[async_trait::async_trait]
    impl RemoteBackend for FailingBackend {
        async fn head(&self, _key: &str) -> anyhow::Result<bool> {
            anyhow::bail!("permission denied")
        }

        async fn get(
            &self,
            _key: &str,
            _max_bytes: Option<u64>,
        ) -> anyhow::Result<Option<GetObject>> {
            unreachable!("unexpected get")
        }

        async fn put(
            &self,
            _key: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> anyhow::Result<()> {
            unreachable!("unexpected put")
        }

        async fn list(&self, _prefix: &str) -> anyhow::Result<Vec<String>> {
            unreachable!("unexpected list")
        }

        fn describe(&self, key: &str) -> String {
            format!("failing://test/{key}")
        }
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

    fn populated_cc_entry(
        cache_key: &str,
        crate_name: &str,
    ) -> (tempfile::TempDir, Store, std::path::PathBuf) {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(&min_config(tmp.path().join("cache"))).unwrap();
        let source_dir = tmp.path().join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let source_file = source_dir.join("foo.o");
        std::fs::write(&source_file, b"cc object bytes").unwrap();
        store
            .put_with_compile_time_independent(
                cache_key,
                crate_name,
                &[],
                &[],
                "x86_64-unknown-linux-gnu",
                "",
                &[(source_file, "foo.o".to_string())],
                "",
                "",
                0,
            )
            .unwrap();
        let entry_dir = store.entry_dir(cache_key);
        (tmp, store, entry_dir)
    }

    #[tokio::test]
    async fn exists_entry_reports_present_and_missing_objects() {
        let remote = test_remote();
        let backend = memory_backend();
        let layout = RemoteLayout::new(&backend, &remote);

        backend
            .put(
                &v3_manifest_key(&remote.prefix, "key123", "foo"),
                b"{}".to_vec(),
                Some("application/json"),
            )
            .await
            .unwrap();
        assert!(layout.exists_entry("key123", "foo").await.unwrap());
        assert!(!layout.exists_entry("missing", "foo").await.unwrap());
    }

    #[tokio::test]
    async fn exists_entry_propagates_unexpected_errors() {
        let remote = test_remote();
        let layout = RemoteLayout::new(&FailingBackend, &remote);
        let error = layout
            .exists_entry("key123", "foo")
            .await
            .expect_err("backend errors must not become cache misses");
        assert!(error.to_string().contains("permission denied"), "{error:#}");
    }

    #[tokio::test]
    async fn download_entry_extracts_a_stored_pack() {
        let (_tmp, store, entry_dir) = populated_entry();
        let meta: EntryMeta =
            serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).unwrap()).unwrap();
        let packed = create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3).unwrap();

        let remote = test_remote();
        let backend = memory_backend();
        backend
            .put(&v3_pack_key(&remote.prefix, "key123", "foo"), packed, None)
            .await
            .unwrap();
        let layout = RemoteLayout::new(&backend, &remote);

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
    }

    #[tokio::test]
    async fn upload_entry_puts_pack_and_manifest() {
        let (_tmp, store, entry_dir) = populated_entry();
        let remote = test_remote();
        let backend = memory_backend();
        let layout = RemoteLayout::new(&backend, &remote);

        let result = layout
            .upload_entry("key123", "foo", &entry_dir, &store.blobs_dir(), 3)
            .await
            .expect("upload_entry should succeed");
        assert_eq!(result.format, "v3");

        let pack_key = v3_pack_key(&remote.prefix, "key123", "foo");
        let manifest_key = v3_manifest_key(&remote.prefix, "key123", "foo");
        assert!(backend.head(&pack_key).await.unwrap());
        let manifest_body = backend
            .get(&manifest_key, None)
            .await
            .unwrap()
            .expect("manifest stored")
            .body;
        let manifest: V3Manifest = serde_json::from_slice(&manifest_body).unwrap();
        assert_eq!(manifest.pack_key, pack_key);
        assert_eq!(manifest.cache_key, "key123");
        assert_eq!(manifest.crate_name, "foo");
        assert_eq!(manifest.file_count, 1);
    }

    #[tokio::test]
    async fn cc_object_pack_round_trips_through_v3() {
        let cache_key = "c".repeat(64);
        let (_tmp, store, entry_dir) = populated_cc_entry(&cache_key, "foo.c");
        let remote = test_remote();
        let backend = memory_backend();
        let layout = RemoteLayout::new(&backend, &remote);

        layout
            .upload_entry(&cache_key, "foo.c", &entry_dir, &store.blobs_dir(), 3)
            .await
            .expect("cc object upload should reuse the v3 pack layout");

        let dest = _tmp.path().join("restored");
        layout
            .download_entry(&cache_key, "foo.c", &dest, &store.blobs_dir())
            .await
            .expect("cc object download should extract the same pack");
        assert_eq!(
            std::fs::read(dest.join("foo.o")).unwrap(),
            b"cc object bytes"
        );
        assert!(
            backend
                .head(&v3_manifest_key(&remote.prefix, &cache_key, "foo.c"))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn list_keys_maps_manifest_objects_to_crate_and_key() {
        let remote = test_remote();
        let backend = memory_backend();
        let key_a = "a".repeat(64);
        let key_b = "b".repeat(64);
        for key in [
            format!("artifacts/v3/manifests/serde/{key_a}.json"),
            format!("artifacts/v3/manifests/tokio/{key_b}.json"),
            // Non-manifest junk under the prefix is ignored (no .json suffix).
            "artifacts/v3/manifests/serde/notes.txt".to_string(),
            // Shape-valid object path, but an invalid cache key is untrusted
            // listing data and must not enter daemon knowledge caches.
            "artifacts/v3/manifests/serde/not-a-cache-key.json".to_string(),
        ] {
            backend.put(&key, vec![], None).await.unwrap();
        }
        let layout = RemoteLayout::new(&backend, &remote);

        let keys = layout.list_keys().await.expect("list_keys should succeed");
        assert_eq!(keys.get(&key_a).map(String::as_str), Some("serde"));
        assert_eq!(keys.get(&key_b).map(String::as_str), Some("tokio"));
        assert_eq!(
            keys.len(),
            2,
            "invalid listing objects must be ignored: {keys:?}"
        );
    }

    /// An empty prefix means "store at the bucket root". Naive `{prefix}/{rest}`
    /// formatting would emit a leading `/`, which the transport rejects as a
    /// non-canonical key — so this must round-trip through a real backend, not just
    /// produce a plausible-looking string.
    #[tokio::test]
    async fn empty_prefix_addresses_the_root_and_round_trips() {
        assert_eq!(
            v3_manifest_key("", "key123", "foo"),
            "v3/manifests/foo/key123.json"
        );
        assert_eq!(
            v3_pack_key("", "key123", "foo"),
            "v3/packs/foo/key123.tar.zst"
        );

        let remote = RemoteConfig {
            prefix: String::new(),
            backend: crate::config::RemoteBackendConfig::S3(crate::config::S3RemoteConfig {
                bucket: "bucket".to_string(),
                endpoint: None,
                region: "us-east-1".to_string(),
                profile: None,
                user_agent: None,
            }),
        };
        let backend = memory_backend();
        let cache_key = "a".repeat(64);
        let key = v3_manifest_key(&remote.prefix, &cache_key, "foo");
        backend
            .put(&key, b"{}".to_vec(), Some("application/json"))
            .await
            .expect("root-relative key must be accepted by the transport");
        let layout = RemoteLayout::new(&backend, &remote);
        let keys = layout
            .list_keys()
            .await
            .expect("listing the root must work");
        assert_eq!(keys.get(&cache_key).map(String::as_str), Some("foo"));
    }

    #[tokio::test]
    async fn list_keys_for_crates_queries_each_crate_prefix() {
        let remote = test_remote();
        let backend = memory_backend();
        let key_a = "a".repeat(64);
        let key_b = "b".repeat(64);
        let key_c = "c".repeat(64);
        for key in [
            format!("artifacts/v3/manifests/serde/{key_a}.json"),
            format!("artifacts/v3/manifests/tokio/{key_b}.json"),
            // Listing only the requested crate prefixes must exclude this.
            format!("artifacts/v3/manifests/other/{key_c}.json"),
        ] {
            backend.put(&key, vec![], None).await.unwrap();
        }
        let layout = RemoteLayout::new(&backend, &remote);

        let mut crates = std::collections::HashSet::new();
        crates.insert("serde".to_string());
        crates.insert("tokio".to_string());
        let keys = layout
            .list_keys_for_crates(&crates)
            .await
            .expect("list_keys_for_crates should succeed");
        assert_eq!(keys.get(&key_a).map(String::as_str), Some("serde"));
        assert_eq!(keys.get(&key_b).map(String::as_str), Some("tokio"));
        assert_eq!(keys.len(), 2);
    }
}
