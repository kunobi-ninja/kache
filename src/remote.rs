use anyhow::{Context, Result};
use aws_smithy_http_client::{
    Builder as SmithyHttpClientBuilder,
    tls::{self, rustls_provider::CryptoMode},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::config::RemoteConfig;
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

pub async fn create_s3_client(
    remote: &RemoteConfig,
    pool_idle_secs: u64,
) -> Result<aws_sdk_s3::Client> {
    // Build one ring-backed HTTPS client and share it across both the
    // aws-config credential-resolution path and the S3 client. Injecting it
    // is what lets us drop `default-https-client` (which would otherwise
    // force the aws-lc-rs crypto provider, pulling `aws-lc-sys`). See the TLS
    // note in Cargo.toml.
    let http_client = SmithyHttpClientBuilder::new()
        .tls_provider(tls::Provider::Rustls(CryptoMode::Ring))
        .pool_idle_timeout(Duration::from_secs(pool_idle_secs))
        .build_https();

    let mut config_builder = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .http_client(http_client.clone())
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

    s3_config = s3_config.http_client(http_client);
    tracing::debug!(pool_idle_secs, "S3 HTTP client configured");

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

/// Manifests and shards are small JSON documents (a few MB at most). Reject
/// anything larger up front so a compromised or hostile remote can't exhaust
/// memory by serving a giant object on the prefetch/plan path, where many of
/// these are fetched concurrently. Guards the honest-`Content-Length` case;
/// a remote that lies about its length is already an integrity problem.
const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024; // 64 MiB

pub async fn download_manifest(
    backend: &dyn RemoteBackend,
    prefix: &str,
    manifest_key: &str,
) -> Result<BuildManifest> {
    let object_key = format!("{prefix}/{MANIFEST_PREFIX}/{manifest_key}.json");

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
    let object_key = format!("{prefix}/{MANIFEST_PREFIX}/{manifest_key}.json");
    let body = serde_json::to_vec_pretty(manifest).context("serializing manifest")?;

    backend
        .put(&object_key, body, Some("application/json"))
        .await
        .context("uploading manifest")?;

    Ok(())
}

/// Format: `{prefix}/_manifests/v3/{namespace}/shards/{shard_hash}.json`
pub fn shard_object_key(prefix: &str, namespace: &str, shard_hash: &str) -> String {
    format!("{prefix}/{MANIFEST_PREFIX}/{MANIFEST_VERSION}/{namespace}/shards/{shard_hash}.json")
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

    #[tokio::test]
    async fn create_s3_client_builds_with_profile_and_endpoint() {
        // Config-only (no network): a RemoteConfig with both a named profile and
        // a custom endpoint exercises the profile_name and endpoint_url branches
        // of create_s3_client. Building the client must succeed offline.
        let remote = RemoteConfig {
            bucket: "b".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            region: "us-east-1".to_string(),
            prefix: "artifacts".to_string(),
            profile: Some("my-profile".to_string()),
        };
        let _client = create_s3_client(&remote, 30)
            .await
            .expect("client builds offline with profile + endpoint");
    }

    #[tokio::test]
    async fn create_s3_client_builds_without_profile_or_endpoint() {
        // The plain path (no profile, no custom endpoint) also builds.
        let remote = RemoteConfig {
            bucket: "b".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            prefix: "artifacts".to_string(),
            profile: None,
        };
        let _client = create_s3_client(&remote, 30)
            .await
            .expect("client builds offline with defaults");
    }

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

    // ── Mock-S3 round-trips ─────────────────────────────────────────────────
    //
    // These drive the real aws-sdk-s3 client against an in-process wire mock
    // (no network), exercising our request construction + response parsing for
    // the manifest/shard object paths.
    use aws_smithy_http_client::test_util::wire::{ReplayedEvent, WireMockServer};

    /// Build an S3 client whose HTTP traffic is served by `server`, replaying
    /// the canned `events` in request order.
    async fn mock_client(
        events: Vec<ReplayedEvent>,
    ) -> (WireMockServer, crate::remote_backend::S3Backend) {
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
        let backend = crate::remote_backend::S3Backend::new(
            aws_sdk_s3::Client::from_conf(conf),
            "bucket".to_string(),
        );
        (server, backend)
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
    async fn download_manifest_parses_a_served_json_object() {
        let body = serde_json::to_vec(&sample_manifest()).unwrap();
        let (server, client) = mock_client(vec![ReplayedEvent::with_body(&body)]).await;

        let got = download_manifest(&client, "prefix", "key")
            .await
            .expect("download should succeed");
        assert_eq!(got.entries.len(), 1);
        assert_eq!(got.entries[0].crate_name, "serde");
        server.shutdown();
    }

    #[tokio::test]
    async fn upload_manifest_puts_without_error() {
        // Exercises PutObject request construction + signing against the mock;
        // a 200 OK is all the operation needs. (The wire mock records only
        // connection/response events, not the request URI, so we assert on the
        // operation result rather than the path.)
        let (server, client) = mock_client(vec![ReplayedEvent::ok()]).await;
        upload_manifest(&client, "prefix", "mykey", &sample_manifest())
            .await
            .expect("upload should succeed");
        assert_eq!(server.events().len(), 3, "one request round-trip expected");
        server.shutdown();
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
        let body = serde_json::to_vec(&shard).unwrap();
        let (server, client) = mock_client(vec![ReplayedEvent::with_body(&body)]).await;

        let got = download_shard(&client, "prefix", "ns", "hash")
            .await
            .expect("download should succeed")
            .expect("shard should be present");
        assert_eq!(got.entries, shard.entries);
        server.shutdown();
    }

    #[tokio::test]
    async fn download_shard_missing_returns_none() {
        // A 404 with the S3 NoSuchKey error code maps to Ok(None), not an error.
        let not_found = ReplayedEvent::HttpResponse {
            status: 404,
            body: bytes::Bytes::from_static(
                b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code>\
                  <Message>The specified key does not exist.</Message></Error>",
            ),
        };
        let (server, client) = mock_client(vec![not_found]).await;
        let got = download_shard(&client, "prefix", "ns", "missing")
            .await
            .expect("a 404 NoSuchKey must not be an error");
        assert!(got.is_none());
        server.shutdown();
    }
}
