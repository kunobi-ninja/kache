use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::config::RemoteConfig;

// ── S3 operations (take a pre-created client) ────────────────────

/// Download a cached entry from S3 using an existing client.
/// Uses streaming decompression to avoid buffering the full decompressed data in memory.
/// Key format: `{prefix}/{crate_name}/{cache_key}.tar.zst`
pub async fn download_with_client(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    cache_key: &str,
    dest_dir: &Path,
    crate_name: &str,
) -> Result<u64> {
    let object_key = s3_object_key(prefix, cache_key, crate_name);

    tracing::debug!("downloading s3://{}/{}", bucket, object_key);

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(&object_key)
        .send()
        .await
        .context("downloading from S3")?;

    let body = resp
        .body
        .collect()
        .await
        .context("reading S3 response body")?;
    let compressed = body.into_bytes();
    let compressed_len = compressed.len();

    // Streaming: zstd decoder wraps compressed bytes, tar reads from decoder
    let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&compressed))
        .context("creating zstd decoder")?;
    extract_tar_streaming(decoder, dest_dir)?;

    tracing::info!(
        "downloaded {} ({} bytes compressed)",
        cache_key,
        compressed_len
    );
    Ok(compressed_len as u64)
}

/// Check if a cached entry exists in S3 using an existing client.
/// Key format: `{prefix}/{crate_name}/{cache_key}.tar.zst`
pub async fn exists_with_client(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    cache_key: &str,
    crate_name: &str,
) -> Result<bool> {
    let object_key = s3_object_key(prefix, cache_key, crate_name);

    match client
        .head_object()
        .bucket(bucket)
        .key(&object_key)
        .send()
        .await
    {
        Ok(_) => Ok(true),
        Err(e) => {
            let err = e.into_service_error();
            if err.is_not_found() {
                Ok(false)
            } else {
                Err(anyhow::anyhow!("S3 head_object error: {err}"))
            }
        }
    }
}

/// Upload a cached entry to S3 using an existing client.
/// Uses streaming compression: tar data is piped through zstd encoder.
/// Key format: `{prefix}/{crate_name}/{cache_key}.tar.zst`
pub async fn upload_with_client(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    cache_key: &str,
    entry_dir: &Path,
    compression_level: i32,
    crate_name: &str,
) -> Result<u64> {
    let object_key = s3_object_key(prefix, cache_key, crate_name);

    // Stream tar directly through zstd (single buffer, no intermediate Vec)
    let compressed = create_tar_zstd(entry_dir, compression_level)?;

    let compressed_len = compressed.len() as u64;

    tracing::debug!(
        "uploading s3://{}/{} ({} bytes compressed)",
        bucket,
        object_key,
        compressed_len
    );

    client
        .put_object()
        .bucket(bucket)
        .key(&object_key)
        .body(compressed.into())
        .send()
        .await
        .context("uploading to S3")?;

    tracing::info!("uploaded {} to S3", cache_key);
    Ok(compressed_len)
}

/// List all cache keys from S3 (paginated).
/// Returns a map of cache_key → crate_name.
/// Key format: `{prefix}/{crate_name}/{cache_key}.tar.zst`
pub async fn list_keys(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, String>> {
    let mut keys = HashMap::new();
    let mut continuation_token: Option<String> = None;
    let s3_prefix = format!("{prefix}/");

    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(&s3_prefix);

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await.context("listing S3 objects")?;

        for obj in resp.contents() {
            if let Some(key) = obj.key()
                && let Some(stripped) = key.strip_prefix(&s3_prefix)
                && let Some(without_ext) = stripped.strip_suffix(".tar.zst")
            {
                // Format: "{crate_name}/{cache_key}"
                if let Some((crate_name, hash)) = without_ext.rsplit_once('/') {
                    keys.insert(hash.to_string(), crate_name.to_string());
                }
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

/// List S3 cache keys filtered to specific crate names.
/// Uses per-crate prefix listing for efficiency — only queries S3 for matching crates.
/// Returns a map of cache_key → crate_name.
pub async fn list_keys_for_crates(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    crate_names: &HashSet<String>,
) -> Result<HashMap<String, String>> {
    let mut keys = HashMap::new();

    for crate_name in crate_names {
        let crate_prefix = format!("{prefix}/{crate_name}/");
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(&crate_prefix);

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .with_context(|| format!("listing S3 objects for crate {crate_name}"))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key()
                    && let Some(stripped) = key.strip_prefix(&crate_prefix)
                    && let Some(cache_key) = stripped.strip_suffix(".tar.zst")
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

/// Build the S3 object key for a cache entry.
/// Format: `{prefix}/{crate_name}/{cache_key}.tar.zst`
fn s3_object_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    format!("{prefix}/{crate_name}/{cache_key}.tar.zst")
}

// ── Client construction ──────────────────────────────────────────

pub async fn create_s3_client(remote: &RemoteConfig) -> Result<aws_sdk_s3::Client> {
    let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(remote.region.clone()));

    // Apply AWS profile if configured (credentials from ~/.aws/credentials [profile])
    if let Some(profile) = &remote.profile {
        config_builder = config_builder.profile_name(profile);
    }

    // Check for kache-specific credentials (overrides profile)
    let has_access = std::env::var("KACHE_S3_ACCESS_KEY").ok();
    let has_secret = std::env::var("KACHE_S3_SECRET_KEY").ok();
    match (&has_access, &has_secret) {
        (Some(access_key), Some(secret_key)) => {
            config_builder =
                config_builder.credentials_provider(aws_sdk_s3::config::Credentials::new(
                    access_key,
                    secret_key,
                    None,
                    None,
                    "kache-env",
                ));
        }
        (Some(_), None) => {
            tracing::warn!(
                "KACHE_S3_ACCESS_KEY is set but KACHE_S3_SECRET_KEY is missing — ignoring partial credentials"
            );
        }
        (None, Some(_)) => {
            tracing::warn!(
                "KACHE_S3_SECRET_KEY is set but KACHE_S3_ACCESS_KEY is missing — ignoring partial credentials"
            );
        }
        (None, None) => {}
    }

    let sdk_config = config_builder.load().await;

    let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config).force_path_style(true); // Required for Ceph/MinIO

    if let Some(endpoint) = &remote.endpoint {
        s3_config = s3_config.endpoint_url(endpoint);
    }

    Ok(aws_sdk_s3::Client::from_conf(s3_config.build()))
}

// ── Internal helpers ─────────────────────────────────────────────

/// Guard that restores read-only permission on drop (exception safety).
struct ReadonlyGuard {
    path: std::path::PathBuf,
    restore: bool,
}

impl Drop for ReadonlyGuard {
    fn drop(&mut self) {
        if self.restore
            && let Ok(meta) = std::fs::metadata(&self.path)
        {
            let mut perms = meta.permissions();
            perms.set_readonly(true);
            let _ = std::fs::set_permissions(&self.path, perms);
        }
    }
}

/// Create a tar+zstd archive from a directory in a single pass (no intermediate tar buffer).
fn create_tar_zstd(dir: &Path, compression_level: i32) -> Result<Vec<u8>> {
    let encoder = zstd::stream::Encoder::new(Vec::new(), compression_level)
        .context("creating zstd encoder")?;
    let mut archive = tar::Builder::new(encoder);

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();

        if path.is_file() {
            // Make files readable for tar — guard restores on drop (panic-safe)
            let meta = std::fs::metadata(&path)?;
            let was_readonly = meta.permissions().readonly();
            let _guard = if was_readonly {
                let mut perms = meta.permissions();
                perms.set_readonly(false);
                std::fs::set_permissions(&path, perms)?;
                Some(ReadonlyGuard {
                    path: path.clone(),
                    restore: true,
                })
            } else {
                None
            };

            archive
                .append_path_with_name(&path, &name)
                .with_context(|| format!("adding {} to tar", path.display()))?;
        }
    }

    let encoder = archive.into_inner().context("finishing tar archive")?;
    encoder.finish().context("finishing zstd compression")
}

/// Extract a tar archive from a reader into a directory, with security checks.
/// Works with any `Read` source — enables streaming decompression.
/// Uses atomic extraction: writes to a temp dir, then renames on success.
fn extract_tar_streaming<R: std::io::Read>(reader: R, dest_dir: &Path) -> Result<()> {
    // Atomic extraction: extract into a sibling temp directory, rename on success
    let parent = dest_dir.parent().unwrap_or(Path::new("/tmp"));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent).context("creating temp dir for extraction")?;

    let mut archive = tar::Archive::new(reader);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();

        // Security: reject absolute paths, path traversal, and symlinks
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

    // Atomic swap: if dest already exists (race), remove it first
    if dest_dir.exists() {
        std::fs::remove_dir_all(dest_dir).context("removing existing dest dir")?;
    }

    // Persist the temp dir (prevent cleanup on drop) and rename
    let tmp_path = tmp_dir.keep();
    std::fs::rename(&tmp_path, dest_dir).or_else(|_| {
        // rename fails across filesystems — fall back to copy + remove
        copy_dir_all(&tmp_path, dest_dir).and_then(|()| {
            std::fs::remove_dir_all(&tmp_path).context("removing temp dir after copy")
        })
    })?;

    Ok(())
}

/// Recursively copy a directory (fallback when rename crosses filesystems).
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

// ── Build Manifest ───────────────────────────────────────────────

const MANIFEST_PREFIX: &str = "_manifests";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub compile_time_ms: u64,
    pub artifact_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildManifest {
    pub created: String,
    pub manifest_key: String,
    pub entries: Vec<ManifestEntry>,
}

/// Download a build manifest from S3.
pub async fn download_manifest(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    manifest_key: &str,
) -> Result<BuildManifest> {
    let object_key = format!("{prefix}/{MANIFEST_PREFIX}/{manifest_key}.json");

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(&object_key)
        .send()
        .await
        .context("downloading manifest from S3")?;

    let body = resp
        .body
        .collect()
        .await
        .context("reading manifest response body")?;
    let bytes = body.into_bytes();

    serde_json::from_slice(&bytes).context("parsing manifest JSON")
}

/// Upload a build manifest to S3.
pub async fn upload_manifest(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    manifest_key: &str,
    manifest: &BuildManifest,
) -> Result<()> {
    let object_key = format!("{prefix}/{MANIFEST_PREFIX}/{manifest_key}.json");
    let body = serde_json::to_vec_pretty(manifest).context("serializing manifest")?;

    client
        .put_object()
        .bucket(bucket)
        .key(&object_key)
        .body(body.into())
        .content_type("application/json")
        .send()
        .await
        .context("uploading manifest to S3")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tar_zstd_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("source");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("file1.rlib"), b"rlib data").unwrap();
        std::fs::write(src_dir.join("meta.json"), b"{}").unwrap();

        let compressed = create_tar_zstd(&src_dir, 3).unwrap();
        assert!(!compressed.is_empty());

        let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&compressed)).unwrap();
        let dest_dir = dir.path().join("dest");
        extract_tar_streaming(decoder, &dest_dir).unwrap();

        assert_eq!(
            std::fs::read_to_string(dest_dir.join("file1.rlib")).unwrap(),
            "rlib data"
        );
        assert_eq!(
            std::fs::read_to_string(dest_dir.join("meta.json")).unwrap(),
            "{}"
        );
    }

    #[test]
    fn test_streaming_zstd_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("source");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::write(src_dir.join("data.rlib"), b"streaming test data").unwrap();

        let compressed = create_tar_zstd(&src_dir, 3).unwrap();

        // Decompress: zstd decoder → tar extract (streaming)
        let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&compressed)).unwrap();
        let dest_dir = dir.path().join("dest");
        extract_tar_streaming(decoder, &dest_dir).unwrap();

        assert_eq!(
            std::fs::read_to_string(dest_dir.join("data.rlib")).unwrap(),
            "streaming test data"
        );
    }

    /// Build a raw tar archive with a crafted filename (bypasses tar crate validation).
    fn build_raw_tar(filename: &[u8], body: &[u8]) -> Vec<u8> {
        // Tar header is 512 bytes: name[0..100], mode[100..108], ..., size[124..136], checksum[148..156]
        let mut header = [0u8; 512];

        // Name field (first 100 bytes)
        let name_len = filename.len().min(100);
        header[..name_len].copy_from_slice(&filename[..name_len]);

        // Mode: 0000644
        header[100..107].copy_from_slice(b"0000644");
        header[107] = 0;

        // Size in octal (11 chars + null)
        let size_str = format!("{:011o}", body.len());
        header[124..135].copy_from_slice(size_str.as_bytes());
        header[135] = 0;

        // Type flag: '0' = regular file
        header[156] = b'0';

        // Compute checksum (sum of all bytes, treating checksum field as spaces)
        header[148..156].fill(b' ');
        let cksum: u32 = header.iter().map(|&b| b as u32).sum();
        let cksum_str = format!("{:06o}\0 ", cksum);
        header[148..156].copy_from_slice(cksum_str.as_bytes());

        let mut tar_data = Vec::new();
        tar_data.extend_from_slice(&header);
        tar_data.extend_from_slice(body);
        // Pad to 512-byte boundary
        let padding = (512 - (body.len() % 512)) % 512;
        tar_data.extend(std::iter::repeat_n(0u8, padding));
        // End-of-archive: two 512-byte zero blocks
        tar_data.extend(std::iter::repeat_n(0u8, 1024));
        tar_data
    }

    #[test]
    fn test_extract_rejects_absolute_path() {
        let tar_data = build_raw_tar(b"/etc/passwd", b"evil");
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out");
        let result = extract_tar_streaming(std::io::Cursor::new(&tar_data), &dest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("absolute path"));
    }

    #[test]
    fn test_extract_rejects_path_traversal() {
        let tar_data = build_raw_tar(b"../escape.txt", b"evil");
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out");
        let result = extract_tar_streaming(std::io::Cursor::new(&tar_data), &dest);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path traversal"));
    }

    #[test]
    fn test_s3_object_key() {
        let key = s3_object_key("artifacts", "abc123", "serde");
        assert_eq!(key, "artifacts/serde/abc123.tar.zst");
    }

    #[test]
    fn test_manifest_serde_roundtrip() {
        let manifest = BuildManifest {
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: "x86_64-unknown-linux-gnu".to_string(),
            entries: vec![
                ManifestEntry {
                    cache_key: "abc123".to_string(),
                    crate_name: "serde".to_string(),
                    compile_time_ms: 5000,
                    artifact_size: 1024 * 1024,
                },
                ManifestEntry {
                    cache_key: "def456".to_string(),
                    crate_name: "tokio".to_string(),
                    compile_time_ms: 200,
                    artifact_size: 512,
                },
            ],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: BuildManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].crate_name, "serde");
        assert_eq!(parsed.entries[0].compile_time_ms, 5000);
        assert_eq!(parsed.manifest_key, "x86_64-unknown-linux-gnu");
    }

    #[test]
    fn test_manifest_empty_entries() {
        let manifest = BuildManifest {
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: "test".to_string(),
            entries: vec![],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: BuildManifest = serde_json::from_str(&json).unwrap();
        assert!(parsed.entries.is_empty());
    }
}
