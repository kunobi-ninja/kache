use anyhow::{Context, Result, bail};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::{RequestId, RequestIdExt};
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
                let err = e.into_service_error();
                if is_missing_head_object(&err) {
                    Ok(false)
                } else {
                    Err(anyhow::anyhow!(
                        "S3 head_object error for s3://{}/{}: {}",
                        self.remote.bucket,
                        object_key,
                        describe_head_object_error(&err)
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

        let decompress_start = std::time::Instant::now();
        let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&compressed))
            .context("creating v3 zstd decoder")?;
        let original_bytes = extract_entry_pack(decoder, entry_dir)?;
        let decompress_ms = decompress_start.elapsed().as_millis() as u64;

        Ok(DownloadResult {
            format: "v3",
            compressed_bytes: compressed_len,
            original_bytes,
            network_ms: request_ms + body_ms,
            request_ms,
            body_ms,
            request_count: 1,
            decompress_ms,
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

            if resp.is_truncated == Some(true) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
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

                if resp.is_truncated == Some(true) {
                    continuation_token = resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
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

fn describe_head_object_error(err: &HeadObjectError) -> String {
    let mut details = Vec::new();
    details.push(err.to_string());

    if let Some(code) = err.code() {
        details.push(format!("code={code}"));
    }
    if let Some(message) = err.message() {
        details.push(format!("message={message}"));
    }
    if let Some(request_id) = err.meta().request_id() {
        details.push(format!("request_id={request_id}"));
    }
    if let Some(extended_request_id) = err.meta().extended_request_id() {
        details.push(format!("extended_request_id={extended_request_id}"));
    }

    details.join(", ")
}

fn create_entry_pack_zstd(
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
    blobs_dir.join(&hash[..2]).join(hash)
}

fn extract_entry_pack<R: std::io::Read>(reader: R, dest_dir: &Path) -> Result<u64> {
    let parent = dest_dir.parent().unwrap_or(Path::new("/tmp"));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent).context("creating temp dir for v3 extraction")?;

    let mut archive = tar::Archive::new(reader);
    let mut total_bytes = 0u64;

    for entry in archive.entries()? {
        let mut entry = entry?;
        total_bytes += entry.size();
        let path = entry.path()?.to_path_buf();

        if path.is_absolute() {
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
        entry.unpack(&dest)?;
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
    use super::{blob_path, create_entry_pack_zstd, extract_entry_pack};
    use crate::config::{Config, DEFAULT_DAEMON_IDLE_TIMEOUT_SECS};
    use crate::store::{EntryMeta, Store};
    use aws_sdk_s3::error::ErrorMetadata;
    use aws_sdk_s3::operation::head_object::HeadObjectError;

    #[test]
    fn v3_pack_roundtrip_restores_meta_and_files() {
        let tmp = tempfile::tempdir().unwrap();
        let config = Config {
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
}
