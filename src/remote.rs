use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::config::RemoteConfig;
use crate::store::EntryMeta;

/// Result of a download operation with timing breakdown.
pub struct DownloadResult {
    pub compressed_bytes: u64,
    /// Uncompressed size in bytes.
    pub original_bytes: u64,
    /// Time spent on S3 GET + body collection only (excludes decompression/disk I/O).
    pub network_ms: u64,
    /// Time spent in zstd decompression (ms).
    pub decompress_ms: u64,
    /// Time spent on disk I/O (fs::write + permissions + atomic rename), ms.
    pub disk_io_ms: u64,
    /// Number of v2 blobs that were already local (skipped download).
    pub blobs_skipped: u32,
    /// Total number of v2 blobs for this entry.
    pub blobs_total: u32,
}

/// Result of an upload operation with timing breakdown.
pub struct UploadResult {
    /// Total compressed bytes uploaded.
    pub compressed_bytes: u64,
    /// Time spent in zstd compression (ms).
    pub compression_ms: u64,
    /// Total time for HEAD requests (existence checks), ms.
    pub head_checks_ms: u64,
    /// Actual PUT time only (ms).
    pub network_ms: u64,
}

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
) -> Result<DownloadResult> {
    let object_key = s3_object_key(prefix, cache_key, crate_name);

    tracing::debug!("downloading s3://{}/{}", bucket, object_key);

    // Time only the network portion (S3 GET + body collection)
    let net_start = std::time::Instant::now();
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
    let network_ms = net_start.elapsed().as_millis() as u64;
    let compressed_len = compressed.len();

    // Streaming: zstd decoder wraps compressed bytes, tar reads from decoder
    let decompress_start = std::time::Instant::now();
    let decoder = zstd::stream::Decoder::new(std::io::Cursor::new(&compressed))
        .context("creating zstd decoder")?;
    let original_bytes = extract_tar_streaming(decoder, dest_dir)?;
    let decompress_ms = decompress_start.elapsed().as_millis() as u64;

    tracing::info!(
        "downloaded {} ({} bytes compressed)",
        cache_key,
        compressed_len
    );
    Ok(DownloadResult {
        compressed_bytes: compressed_len as u64,
        original_bytes,
        network_ms,
        decompress_ms,
        disk_io_ms: 0,
        blobs_skipped: 0,
        blobs_total: 0,
    })
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

/// Upload a cached entry to S3 using an existing client (v1 tar format).
/// Uses streaming compression: tar data is piped through zstd encoder.
/// Key format: `{prefix}/{crate_name}/{cache_key}.tar.zst`
///
/// Kept for v1 compatibility; new uploads use `upload_entry_v2`.
#[allow(dead_code)]
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
/// Checks both v1 (`{prefix}/{crate_name}/{cache_key}.tar.zst`) and
/// v2 (`{prefix}/manifests/{crate_name}/{cache_key}.json`) formats.
pub async fn list_keys(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, String>> {
    let mut keys = HashMap::new();

    // ── v1 keys: {prefix}/{crate_name}/{cache_key}.tar.zst ──
    let mut continuation_token: Option<String> = None;
    let s3_prefix = format!("{prefix}/");

    loop {
        let mut req = client.list_objects_v2().bucket(bucket).prefix(&s3_prefix);

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await.context("listing S3 objects (v1)")?;

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

    // ── v2 keys: {prefix}/manifests/{crate_name}/{cache_key}.json ──
    let manifests_prefix = format!("{prefix}/manifests/");
    continuation_token = None;

    loop {
        let mut req = client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&manifests_prefix);

        if let Some(token) = &continuation_token {
            req = req.continuation_token(token);
        }

        let resp = req.send().await.context("listing S3 objects (v2)")?;

        for obj in resp.contents() {
            if let Some(key) = obj.key()
                && let Some(stripped) = key.strip_prefix(&manifests_prefix)
                && let Some(without_ext) = stripped.strip_suffix(".json")
            {
                // Format: "{crate_name}/{cache_key}"
                if let Some((crate_name, cache_key)) = without_ext.rsplit_once('/') {
                    keys.entry(cache_key.to_string())
                        .or_insert_with(|| crate_name.to_string());
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
/// Checks both v1 and v2 formats. Returns a map of cache_key → crate_name.
pub async fn list_keys_for_crates(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    crate_names: &HashSet<String>,
) -> Result<HashMap<String, String>> {
    let mut keys = HashMap::new();

    for crate_name in crate_names {
        // ── v1: {prefix}/{crate_name}/{cache_key}.tar.zst ──
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
                .with_context(|| format!("listing S3 objects for crate {crate_name} (v1)"))?;

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

        // ── v2: {prefix}/manifests/{crate_name}/{cache_key}.json ──
        let manifest_prefix = format!("{prefix}/manifests/{crate_name}/");
        continuation_token = None;

        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(&manifest_prefix);

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req
                .send()
                .await
                .with_context(|| format!("listing S3 objects for crate {crate_name} (v2)"))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key()
                    && let Some(stripped) = key.strip_prefix(&manifest_prefix)
                    && let Some(cache_key) = stripped.strip_suffix(".json")
                {
                    keys.entry(cache_key.to_string())
                        .or_insert_with(|| crate_name.clone());
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

/// Build the S3 object key for a cache entry (v1 format).
/// Format: `{prefix}/{crate_name}/{cache_key}.tar.zst`
fn s3_object_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    format!("{prefix}/{crate_name}/{cache_key}.tar.zst")
}

/// S3 key for a content-addressed blob (v2 format).
/// Format: `{prefix}/blobs/{hash[0..2]}/{hash}.zst`
pub fn s3_blob_key(prefix: &str, hash: &str) -> String {
    format!("{prefix}/blobs/{}/{hash}.zst", &hash[..2])
}

/// S3 key for a cache entry manifest (v2 format).
/// Format: `{prefix}/manifests/{crate_name}/{cache_key}.json`
pub fn s3_manifest_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    format!("{prefix}/manifests/{crate_name}/{cache_key}.json")
}

// ── V2 blob-based S3 operations ─────────────────────────────────

/// Resolve the local blob path from the blobs directory.
fn local_blob_path(blobs_dir: &Path, hash: &str) -> PathBuf {
    blobs_dir.join(&hash[..2]).join(hash)
}

/// Upload a cache entry using the v2 blob-based format.
///
/// For each file in the entry's manifest:
///   - HEAD-check if the blob already exists on S3
///   - If not, read from local blob store, zstd-compress, and upload
///
/// Finally, upload the manifest (meta.json) to the manifests prefix.
/// Returns the total compressed bytes uploaded.
pub async fn upload_entry_v2(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    cache_key: &str,
    crate_name: &str,
    entry_dir: &Path,
    blobs_dir: &Path,
    compression_level: i32,
) -> Result<UploadResult> {
    // Read meta.json to get the list of files/blobs
    let meta_path = entry_dir.join("meta.json");
    let meta_content =
        std::fs::read_to_string(&meta_path).context("reading meta.json for v2 upload")?;
    let meta: EntryMeta =
        serde_json::from_str(&meta_content).context("parsing meta.json for v2 upload")?;

    let mut total_bytes: u64 = 0;
    let mut total_compression_ms: u64 = 0;
    let mut total_head_checks_ms: u64 = 0;
    let mut total_network_ms: u64 = 0;

    // Upload each blob that doesn't already exist on S3
    for cached_file in &meta.files {
        let blob_key = s3_blob_key(prefix, &cached_file.hash);

        // HEAD check — skip if already uploaded
        let head_start = std::time::Instant::now();
        let exists = match client
            .head_object()
            .bucket(bucket)
            .key(&blob_key)
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                let err = e.into_service_error();
                if err.is_not_found() {
                    false
                } else {
                    return Err(anyhow::anyhow!("S3 HEAD blob error: {err}"));
                }
            }
        };
        total_head_checks_ms += head_start.elapsed().as_millis() as u64;

        if exists {
            tracing::debug!("blob {} already on S3, skipping", &cached_file.hash[..16]);
            continue;
        }

        // Read the blob from local store
        let blob_path = local_blob_path(blobs_dir, &cached_file.hash);
        let blob_data = std::fs::read(&blob_path)
            .with_context(|| format!("reading blob {} for upload", blob_path.display()))?;

        // Compress with zstd
        let compress_start = std::time::Instant::now();
        let compressed = zstd::encode_all(std::io::Cursor::new(&blob_data), compression_level)
            .context("zstd-compressing blob for upload")?;
        total_compression_ms += compress_start.elapsed().as_millis() as u64;
        let compressed_len = compressed.len() as u64;

        tracing::debug!(
            "uploading blob s3://{}/{} ({} bytes compressed)",
            bucket,
            blob_key,
            compressed_len
        );

        let put_start = std::time::Instant::now();
        client
            .put_object()
            .bucket(bucket)
            .key(&blob_key)
            .body(compressed.into())
            .send()
            .await
            .with_context(|| format!("uploading blob {} to S3", &cached_file.hash[..16]))?;
        total_network_ms += put_start.elapsed().as_millis() as u64;

        total_bytes += compressed_len;
    }

    // Upload manifest (meta.json as JSON)
    let manifest_key = s3_manifest_key(prefix, cache_key, crate_name);
    let manifest_body = meta_content.into_bytes();
    let manifest_len = manifest_body.len() as u64;

    tracing::debug!(
        "uploading manifest s3://{}/{} ({} bytes)",
        bucket,
        manifest_key,
        manifest_len
    );

    let put_start = std::time::Instant::now();
    client
        .put_object()
        .bucket(bucket)
        .key(&manifest_key)
        .body(manifest_body.into())
        .content_type("application/json")
        .send()
        .await
        .context("uploading v2 manifest to S3")?;
    total_network_ms += put_start.elapsed().as_millis() as u64;

    total_bytes += manifest_len;

    tracing::info!(
        "uploaded {} (v2, {} blobs, {} bytes compressed)",
        cache_key.get(..16).unwrap_or(cache_key),
        meta.files.len(),
        total_bytes
    );

    Ok(UploadResult {
        compressed_bytes: total_bytes,
        compression_ms: total_compression_ms,
        head_checks_ms: total_head_checks_ms,
        network_ms: total_network_ms,
    })
}

/// Download a cache entry using the v2 blob-based format.
///
/// 1. Fetch manifest from S3. If 404, return `Ok(None)` (caller should try v1).
/// 2. For each file in manifest: download blob if not in local store, decompress, write.
/// 3. Write meta.json to entry_dir.
///
/// Returns `Ok(Some(DownloadResult))` on success, `Ok(None)` if no v2 manifest found.
pub async fn download_entry_v2(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    cache_key: &str,
    crate_name: &str,
    entry_dir: &Path,
    blobs_dir: &Path,
) -> Result<Option<DownloadResult>> {
    let manifest_key = s3_manifest_key(prefix, cache_key, crate_name);

    // Time only network I/O (S3 GET + body collection), not decompression/disk
    let net_start = std::time::Instant::now();

    // Try to fetch the v2 manifest
    let manifest_bytes = match client
        .get_object()
        .bucket(bucket)
        .key(&manifest_key)
        .send()
        .await
    {
        Ok(resp) => {
            let body = resp
                .body
                .collect()
                .await
                .context("reading v2 manifest response body")?;
            body.into_bytes()
        }
        Err(e) => {
            let debug = format!("{e:?}");
            let err = e.into_service_error();
            if err.is_no_such_key() {
                return Ok(None); // No v2 manifest — caller should try v1
            }
            return Err(anyhow::anyhow!(
                "S3 get manifest s3://{bucket}/{manifest_key} failed: {err} [{debug}]"
            ));
        }
    };

    let manifest_network_ms = net_start.elapsed().as_millis() as u64;
    let mut network_ms = manifest_network_ms;

    let meta: EntryMeta =
        serde_json::from_slice(&manifest_bytes).context("parsing v2 manifest JSON")?;

    let blobs_total = meta.files.len() as u32;
    let mut blobs_skipped = 0u32;
    let mut total_bytes: u64 = manifest_bytes.len() as u64;
    let mut total_original: u64 = 0;
    let mut total_decompress_ms: u64 = 0;
    let mut total_disk_io_ms: u64 = 0;

    // Download each blob that isn't already in the local store
    for cached_file in &meta.files {
        total_original += cached_file.size;
        let blob_path = local_blob_path(blobs_dir, &cached_file.hash);

        if blob_path.is_file() {
            blobs_skipped += 1;
            tracing::debug!(
                "blob {} already local, skipping download",
                &cached_file.hash[..16]
            );
            continue;
        }

        let blob_key = s3_blob_key(prefix, &cached_file.hash);

        // Time only the network portion of each blob download
        let blob_net_start = std::time::Instant::now();
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(&blob_key)
            .send()
            .await
            .with_context(|| format!("downloading blob {}", &cached_file.hash[..16]))?;

        let body = resp
            .body
            .collect()
            .await
            .context("reading blob response body")?;
        let compressed = body.into_bytes();
        network_ms += blob_net_start.elapsed().as_millis() as u64;
        total_bytes += compressed.len() as u64;

        // Decompress — timed separately from network
        let decompress_start = std::time::Instant::now();
        let decompressed = zstd::decode_all(std::io::Cursor::new(&compressed))
            .with_context(|| format!("decompressing blob {}", &cached_file.hash[..16]))?;
        total_decompress_ms += decompress_start.elapsed().as_millis() as u64;

        // Write to local blob store (atomic: write temp then rename)
        let disk_start = std::time::Instant::now();
        let shard_dir = blob_path.parent().unwrap();
        std::fs::create_dir_all(shard_dir).context("creating blob shard dir")?;

        let tmp_path = shard_dir.join(format!(".tmp_{}", &cached_file.hash));
        std::fs::write(&tmp_path, &decompressed).context("writing blob temp file")?;

        // Make read-only
        {
            let mut perms = std::fs::metadata(&tmp_path)?.permissions();
            perms.set_readonly(true);
            std::fs::set_permissions(&tmp_path, perms)?;
        }

        // Atomic rename (ignore AlreadyExists race — another download beat us)
        if let Err(e) = std::fs::rename(&tmp_path, &blob_path) {
            // If the blob appeared between our check and rename, that's fine
            if blob_path.is_file() {
                let _ = std::fs::remove_file(&tmp_path);
            } else {
                return Err(e).context("renaming blob into store");
            }
        }
        total_disk_io_ms += disk_start.elapsed().as_millis() as u64;

        tracing::debug!(
            "downloaded blob {} ({} bytes compressed)",
            &cached_file.hash[..16],
            compressed.len()
        );
    }

    // Write meta.json to entry dir
    std::fs::create_dir_all(entry_dir).context("creating entry dir for v2 download")?;
    std::fs::write(entry_dir.join("meta.json"), &manifest_bytes)
        .context("writing meta.json from v2 manifest")?;

    // Also write the artifact files as symlinks/copies from blob store into entry dir
    // so that import_downloaded_entry can find them (it expects files in entry_dir)
    for cached_file in &meta.files {
        let blob_path = local_blob_path(blobs_dir, &cached_file.hash);
        let dest_path = entry_dir.join(&cached_file.name);
        // Hard-link or copy the blob so import_downloaded_entry can move it
        if !dest_path.exists() {
            // Copy rather than hardlink — import_downloaded_entry will move/delete these
            std::fs::copy(&blob_path, &dest_path)
                .with_context(|| format!("copying blob to entry dir for {}", cached_file.name))?;
            // Make writable so import_downloaded_entry can process it
            let mut perms = std::fs::metadata(&dest_path)?.permissions();
            perms.set_readonly(false);
            std::fs::set_permissions(&dest_path, perms)?;
        }
    }

    tracing::info!(
        "downloaded {} (v2, {} blobs, {} bytes compressed)",
        cache_key.get(..16).unwrap_or(cache_key),
        meta.files.len(),
        total_bytes
    );

    Ok(Some(DownloadResult {
        compressed_bytes: total_bytes,
        original_bytes: total_original,
        network_ms,
        decompress_ms: total_decompress_ms,
        disk_io_ms: total_disk_io_ms,
        blobs_skipped,
        blobs_total,
    }))
}

/// Check if a v2 manifest exists for a cache key on S3.
pub async fn exists_v2(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    cache_key: &str,
    crate_name: &str,
) -> Result<bool> {
    let key = s3_manifest_key(prefix, cache_key, crate_name);

    match client.head_object().bucket(bucket).key(&key).send().await {
        Ok(_) => Ok(true),
        Err(e) => {
            let err = e.into_service_error();
            if err.is_not_found() {
                Ok(false)
            } else {
                Err(anyhow::anyhow!("S3 head_object error (v2 manifest): {err}"))
            }
        }
    }
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

/// Create a tar+zstd archive from a directory in a single pass (no intermediate tar buffer).
#[allow(dead_code)]
fn create_tar_zstd(dir: &Path, compression_level: i32) -> Result<Vec<u8>> {
    let encoder = zstd::stream::Encoder::new(Vec::new(), compression_level)
        .context("creating zstd encoder")?;
    let mut archive = tar::Builder::new(encoder);

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();

        if path.is_file() {
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
/// Extract a tar archive to a directory. Returns total bytes extracted.
fn extract_tar_streaming<R: std::io::Read>(reader: R, dest_dir: &Path) -> Result<u64> {
    // Atomic extraction: extract into a sibling temp directory, rename on success
    let parent = dest_dir.parent().unwrap_or(Path::new("/tmp"));
    std::fs::create_dir_all(parent)?;
    let tmp_dir = tempfile::tempdir_in(parent).context("creating temp dir for extraction")?;

    let mut archive = tar::Archive::new(reader);
    let mut total_bytes = 0u64;

    for entry in archive.entries()? {
        let mut entry = entry?;
        total_bytes += entry.size();
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

    Ok(total_bytes)
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
pub const MANIFEST_VERSION: &str = "v2";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub compile_time_ms: u64,
    pub artifact_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildManifest {
    /// 0 = legacy (v1), 2 = sharded (v2)
    #[serde(default)]
    pub version: u32,
    pub created: String,
    pub manifest_key: String,
    pub entries: Vec<ManifestEntry>,
}

// ── Sharded Manifest (v2) ────────────────────────────────────────

/// A single entry in a manifest shard — just the cache key and crate name.
/// No compile times (those are noisy and would destabilize shard hashes).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShardEntry {
    pub cache_key: String,
    pub crate_name: String,
}

/// A content-addressed shard of the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub version: u32,
    pub entries: Vec<ShardEntry>,
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

// ── Shard S3 operations ──────────────────────────────────────────

/// Build the S3 object key for a shard file.
/// Format: `{prefix}/_manifests/v2/{namespace}/shards/{shard_hash}.json`
pub fn shard_object_key(prefix: &str, namespace: &str, shard_hash: &str) -> String {
    format!("{prefix}/{MANIFEST_PREFIX}/{MANIFEST_VERSION}/{namespace}/shards/{shard_hash}.json")
}

/// Download a single shard from S3. Returns `Ok(None)` on 404 (shard not found).
pub async fn download_shard(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    namespace: &str,
    shard_hash: &str,
) -> Result<Option<Shard>> {
    let object_key = shard_object_key(prefix, namespace, shard_hash);

    match client
        .get_object()
        .bucket(bucket)
        .key(&object_key)
        .send()
        .await
    {
        Ok(resp) => {
            let body = resp
                .body
                .collect()
                .await
                .context("reading shard response body")?;
            let bytes = body.into_bytes();
            let shard: Shard = serde_json::from_slice(&bytes).context("parsing shard JSON")?;
            Ok(Some(shard))
        }
        Err(e) => {
            let err = e.into_service_error();
            if err.is_no_such_key() {
                Ok(None)
            } else {
                Err(anyhow::anyhow!("S3 get shard error: {err}"))
            }
        }
    }
}

/// Upload a single shard to S3.
pub async fn upload_shard(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    namespace: &str,
    shard_hash: &str,
    shard: &Shard,
) -> Result<()> {
    let object_key = shard_object_key(prefix, namespace, shard_hash);
    let body = serde_json::to_vec_pretty(shard).context("serializing shard")?;

    client
        .put_object()
        .bucket(bucket)
        .key(&object_key)
        .body(body.into())
        .content_type("application/json")
        .send()
        .await
        .context("uploading shard to S3")?;

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

    #[cfg(unix)]
    #[test]
    fn test_tar_zstd_keeps_readonly_source_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("source");
        std::fs::create_dir_all(&src_dir).unwrap();
        let path = src_dir.join("readonly.rlib");
        std::fs::write(&path, b"readonly data").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();

        let compressed = create_tar_zstd(&src_dir, 3).unwrap();

        assert!(!compressed.is_empty());
        assert!(
            std::fs::metadata(&path).unwrap().permissions().readonly(),
            "archiving must not flip source permissions on shared cache files"
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
            version: 2,
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
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].crate_name, "serde");
        assert_eq!(parsed.entries[0].compile_time_ms, 5000);
        assert_eq!(parsed.manifest_key, "x86_64-unknown-linux-gnu");
    }

    #[test]
    fn test_manifest_legacy_no_version_field() {
        // Simulate a v1 manifest that has no "version" field
        let json = r#"{"created":"2025-01-01T00:00:00Z","manifest_key":"test","entries":[]}"#;
        let parsed: BuildManifest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.version, 0); // default = legacy
    }

    #[test]
    fn test_manifest_empty_entries() {
        let manifest = BuildManifest {
            version: 0,
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: "test".to_string(),
            entries: vec![],
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: BuildManifest = serde_json::from_str(&json).unwrap();
        assert!(parsed.entries.is_empty());
    }

    #[test]
    fn test_shard_serde_roundtrip() {
        let shard = Shard {
            version: 2,
            entries: vec![
                ShardEntry {
                    cache_key: "abc123".to_string(),
                    crate_name: "serde".to_string(),
                },
                ShardEntry {
                    cache_key: "def456".to_string(),
                    crate_name: "syn".to_string(),
                },
            ],
        };
        let json = serde_json::to_string(&shard).unwrap();
        let parsed: Shard = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].crate_name, "serde");
    }

    #[test]
    fn test_shard_object_key() {
        let key = shard_object_key("artifacts", "x86_64-linux/abc123/release", "deadbeef");
        assert_eq!(
            key,
            "artifacts/_manifests/v2/x86_64-linux/abc123/release/shards/deadbeef.json"
        );
    }

    #[test]
    fn test_s3_blob_key() {
        let key = s3_blob_key(
            "cache",
            "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        );
        assert_eq!(
            key,
            "cache/blobs/ab/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890.zst"
        );

        // Different prefix
        let key2 = s3_blob_key("my/prefix", "ff0011223344");
        assert_eq!(key2, "my/prefix/blobs/ff/ff0011223344.zst");
    }

    #[test]
    fn test_s3_manifest_key() {
        let key = s3_manifest_key("cache", "abc123hash", "serde");
        assert_eq!(key, "cache/manifests/serde/abc123hash.json");

        let key2 = s3_manifest_key("my/prefix", "deadbeef", "tokio-runtime");
        assert_eq!(key2, "my/prefix/manifests/tokio-runtime/deadbeef.json");
    }

    #[test]
    fn test_local_blob_path() {
        let blobs = Path::new("/store/blobs");
        let path = local_blob_path(blobs, "abcdef1234");
        assert_eq!(path, PathBuf::from("/store/blobs/ab/abcdef1234"));
    }
}
