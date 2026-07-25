use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::remote_backend::RemoteBackend;

/// Result of a download operation with timing breakdown.
pub struct DownloadResult {
    pub format: &'static str,
    /// Remote object key fetched for this download.
    pub object_key: String,
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
    /// Time spent extracting the downloaded archive to the local store (ms).
    ///
    /// For streaming pack formats this includes zstd decode, tar unpacking,
    /// and filesystem writes.
    pub extract_ms: u64,
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

/// Manifests and shards are small JSON documents (a few MB at most). Reject
/// anything larger up front so a compromised or hostile remote can't exhaust
/// memory by serving a giant object on the prefetch/plan path, where many of
/// these are fetched concurrently. Pre-checks advertised `Content-Length`
/// up front and enforces `max_bytes` on collected body bytes if `Content-Length`
/// is missing or under-reported.
const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

pub async fn download_manifest(
    backend: &dyn RemoteBackend,
    prefix: &str,
    manifest_key: &str,
) -> Result<BuildManifest> {
    let object_key =
        crate::config::join_remote_key(prefix, &format!("{MANIFEST_PREFIX}/{manifest_key}.json"));

    let fetched = backend
        .get(&object_key, Some(MAX_METADATA_BYTES))
        .await
        .context("downloading manifest")?
        .with_context(|| format!("manifest not found: {}", backend.describe(&object_key)))?;

    serde_json::from_slice(&fetched.body).context("parsing manifest JSON")
}

pub async fn upload_manifest(
    backend: &dyn RemoteBackend,
    prefix: &str,
    manifest_key: &str,
    manifest: &BuildManifest,
) -> Result<()> {
    let object_key =
        crate::config::join_remote_key(prefix, &format!("{MANIFEST_PREFIX}/{manifest_key}.json"));
    let body = serde_json::to_vec_pretty(manifest).context("serializing manifest")?;

    backend
        .put(&object_key, body, Some("application/json"))
        .await
        .context("uploading manifest")?;

    Ok(())
}

/// Format: `{prefix}/_manifests/v3/{namespace}/shards/{shard_hash}.json`
pub fn shard_object_key(prefix: &str, namespace: &str, shard_hash: &str) -> String {
    crate::config::join_remote_key(
        prefix,
        &format!("{MANIFEST_PREFIX}/{MANIFEST_VERSION}/{namespace}/shards/{shard_hash}.json"),
    )
}

pub async fn download_shard(
    backend: &dyn RemoteBackend,
    prefix: &str,
    namespace: &str,
    shard_hash: &str,
) -> Result<Option<Shard>> {
    let object_key = shard_object_key(prefix, namespace, shard_hash);

    let Some(fetched) = backend
        .get(&object_key, Some(MAX_METADATA_BYTES))
        .await
        .context("downloading shard")?
    else {
        return Ok(None);
    };

    let shard: Shard = serde_json::from_slice(&fetched.body).context("parsing shard JSON")?;
    Ok(Some(shard))
}

pub async fn upload_shard(
    backend: &dyn RemoteBackend,
    prefix: &str,
    namespace: &str,
    shard_hash: &str,
    shard: &Shard,
) -> Result<()> {
    let object_key = shard_object_key(prefix, namespace, shard_hash);
    let body = serde_json::to_vec_pretty(shard).context("serializing shard")?;

    backend
        .put(&object_key, body, Some("application/json"))
        .await
        .context("uploading shard")?;

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

    fn sample_manifest() -> BuildManifest {
        BuildManifest {
            version: 3,
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: "x86_64-unknown-linux-gnu".to_string(),
            entries: vec![ManifestEntry {
                cache_key: "abc".to_string(),
                crate_name: "serde".to_string(),
                compile_time_ms: 10,
                artifact_size: 100,
            }],
        }
    }

    #[tokio::test]
    async fn download_manifest_parses_a_stored_json_object() {
        let backend = crate::remote_backend::memory_backend();
        let body = serde_json::to_vec(&sample_manifest()).unwrap();
        backend
            .put("prefix/_manifests/key.json", body, Some("application/json"))
            .await
            .unwrap();

        let got = download_manifest(&backend, "prefix", "key")
            .await
            .expect("download should succeed");
        assert_eq!(got.entries.len(), 1);
        assert_eq!(got.entries[0].crate_name, "serde");
    }

    #[tokio::test]
    async fn upload_manifest_writes_the_expected_object() {
        let backend = crate::remote_backend::memory_backend();
        upload_manifest(&backend, "prefix", "mykey", &sample_manifest())
            .await
            .expect("upload should succeed");
        let stored = backend
            .get("prefix/_manifests/mykey.json", None)
            .await
            .unwrap()
            .expect("manifest object");
        let parsed: BuildManifest = serde_json::from_slice(&stored.body).unwrap();
        assert_eq!(parsed.manifest_key, "x86_64-unknown-linux-gnu");
    }

    #[tokio::test]
    async fn download_shard_found_parses_json() {
        let shard = Shard {
            version: 3,
            entries: vec![ShardEntry {
                cache_key: "k1".to_string(),
                crate_name: "tokio".to_string(),
            }],
        };
        let backend = crate::remote_backend::memory_backend();
        backend
            .put(
                &shard_object_key("prefix", "ns", "hash"),
                serde_json::to_vec(&shard).unwrap(),
                Some("application/json"),
            )
            .await
            .unwrap();

        let got = download_shard(&backend, "prefix", "ns", "hash")
            .await
            .expect("download should succeed")
            .expect("shard should be present");
        assert_eq!(got.entries, shard.entries);
    }

    #[tokio::test]
    async fn download_shard_missing_returns_none() {
        let backend = crate::remote_backend::memory_backend();
        let got = download_shard(&backend, "prefix", "ns", "missing")
            .await
            .expect("a missing object must not be an error");
        assert!(got.is_none());
    }
}
