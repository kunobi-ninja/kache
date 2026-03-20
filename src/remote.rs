use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::RemoteConfig;

/// Result of a download operation with timing breakdown.
pub struct DownloadResult {
    pub format: &'static str,
    pub compressed_bytes: u64,
    /// Uncompressed size in bytes.
    pub original_bytes: u64,
    /// Time spent on S3 GET + body collection only (excludes decompression/disk I/O).
    pub network_ms: u64,
    /// Time spent waiting for response headers across all GET requests (ms).
    pub request_ms: u64,
    /// Time spent reading response bodies across all GET requests (ms).
    pub body_ms: u64,
    /// Number of GET requests issued for this download.
    pub request_count: u32,
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

pub async fn create_s3_client(remote: &RemoteConfig) -> Result<aws_sdk_s3::Client> {
    let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(remote.region.clone()));

    if let Some(profile) = &remote.profile {
        config_builder = config_builder.profile_name(profile);
    }

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
    let mut s3_config = aws_sdk_s3::config::Builder::from(&sdk_config).force_path_style(true);

    if let Some(endpoint) = &remote.endpoint {
        s3_config = s3_config.endpoint_url(endpoint);
    }

    Ok(aws_sdk_s3::Client::from_conf(s3_config.build()))
}

const MANIFEST_PREFIX: &str = "_manifests";
pub const MANIFEST_VERSION: &str = "v3";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub cache_key: String,
    pub crate_name: String,
    pub compile_time_ms: u64,
    pub artifact_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildManifest {
    /// 3 = current build-manifest schema used alongside the v3 remote entry layout.
    #[serde(default)]
    pub version: u32,
    pub created: String,
    pub manifest_key: String,
    pub entries: Vec<ManifestEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ShardEntry {
    pub cache_key: String,
    pub crate_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub version: u32,
    pub entries: Vec<ShardEntry>,
}

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

/// Format: `{prefix}/_manifests/v3/{namespace}/shards/{shard_hash}.json`
pub fn shard_object_key(prefix: &str, namespace: &str, shard_hash: &str) -> String {
    format!("{prefix}/{MANIFEST_PREFIX}/{MANIFEST_VERSION}/{namespace}/shards/{shard_hash}.json")
}

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
    fn test_manifest_serde_roundtrip() {
        let manifest = BuildManifest {
            version: 3,
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
        assert_eq!(parsed.version, 3);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].crate_name, "serde");
        assert_eq!(parsed.entries[0].compile_time_ms, 5000);
        assert_eq!(parsed.manifest_key, "x86_64-unknown-linux-gnu");
    }

    #[test]
    fn test_manifest_legacy_no_version_field() {
        let json = r#"{"created":"2025-01-01T00:00:00Z","manifest_key":"test","entries":[]}"#;
        let parsed: BuildManifest = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.version, 0);
    }

    #[test]
    fn test_manifest_empty_entries() {
        let manifest = BuildManifest {
            version: 3,
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
            version: 3,
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
        assert_eq!(parsed.version, 3);
        assert_eq!(parsed.entries.len(), 2);
        assert_eq!(parsed.entries[0].crate_name, "serde");
    }

    #[test]
    fn test_shard_object_key() {
        let key = shard_object_key("artifacts", "x86_64-linux/abc123/release", "deadbeef");
        assert_eq!(
            key,
            "artifacts/_manifests/v3/x86_64-linux/abc123/release/shards/deadbeef.json"
        );
    }
}
