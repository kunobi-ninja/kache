use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::remote_backend::{ConditionalPut, RemoteBackend};

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
    /// What a miss costs to rebuild, and how big the artifact is
    /// (kunobi-ninja/kache#617). Both `Option`: shards written before
    /// cost-aware planning carry neither, and a missing value must read as
    /// "unknown" rather than "free and worthless".
    ///
    /// `save-manifest` already has both from `ManifestEntry`; it just was not
    /// persisting them into the shard.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compile_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_size: Option<u64>,
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

pub async fn try_download_manifest(
    backend: &dyn RemoteBackend,
    prefix: &str,
    manifest_key: &str,
) -> Result<Option<BuildManifest>> {
    let object_key =
        crate::config::join_remote_key(prefix, &format!("{MANIFEST_PREFIX}/{manifest_key}.json"));

    let Some(fetched) = backend
        .get(&object_key, Some(MAX_METADATA_BYTES))
        .await
        .context("downloading manifest")?
    else {
        return Ok(None);
    };

    let manifest = serde_json::from_slice(&fetched.body).context("parsing manifest JSON")?;
    Ok(Some(manifest))
}

/// A manifest as stored, with what merging needs. Readers parse the same
/// object as a [`BuildManifest`] and ignore the extra field.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredManifest {
    #[serde(flatten)]
    manifest: BuildManifest,
    /// When, and for which commit, each entry was last published, by cache
    /// key. Manifests written before merging have none.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    published: BTreeMap<String, Published>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Published {
    /// Unix seconds.
    at: i64,
    /// The commit CI was building, when it said.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
}

/// How long an entry stays in a manifest that builds of other or unknown
/// commits keep adding to.
const MANIFEST_MERGE_WINDOW_SECS: i64 = 60 * 60;

/// Conditional-write attempts before giving up and leaving the manifest as
/// the other publishers wrote it.
const MANIFEST_PUBLISH_ATTEMPTS: usize = 5;

/// Most entries a merged manifest holds. The publishing build's own entries
/// always stay; stored entries fill what is left, newest publishers first.
/// About 300 bytes each, so a full manifest stays far below the read limit.
const MANIFEST_MAX_ENTRIES: usize = 50_000;

/// Whether an entry already stored survives a merge.
///
/// Both commits known: only the same commit's entries stay, whatever their
/// age, so keys of earlier commits do not accumulate. Otherwise an entry stays
/// for [`MANIFEST_MERGE_WINDOW_SECS`] after it was last published.
fn keep_stored_entry(published: &Published, commit: Option<&str>, now: i64) -> bool {
    match (commit, published.commit.as_deref()) {
        (Some(new), Some(old)) => new == old,
        _ => now.saturating_sub(published.at) <= MANIFEST_MERGE_WINDOW_SECS,
    }
}

/// Combine a stored manifest with the entries one build just published.
///
/// Several builds share a manifest key: CI jobs for tests, lints and docs on
/// one lockfile, target and profile. Each publishes what it used, so an
/// overwrite keeps only the last job's entries. The merge keeps the new
/// entries first, in the order the build used them, then every stored entry
/// that [`keep_stored_entry`] retains.
fn merge_manifest(
    existing: Option<StoredManifest>,
    manifest: &BuildManifest,
    commit: Option<&str>,
    now: i64,
) -> StoredManifest {
    let stamp = Published {
        at: now,
        commit: commit.map(str::to_string),
    };
    let mut merged = StoredManifest {
        manifest: manifest.clone(),
        published: manifest
            .entries
            .iter()
            .map(|entry| (entry.cache_key.clone(), stamp.clone()))
            .collect(),
    };
    let Some(existing) = existing else {
        return merged;
    };
    for entry in existing.manifest.entries {
        if merged.manifest.entries.len() >= MANIFEST_MAX_ENTRIES {
            break;
        }
        if merged.published.contains_key(&entry.cache_key) {
            continue;
        }
        // An entry from before merging is treated as just published, once.
        // A stamp from the future counts as now, so it still expires.
        let published = match existing.published.get(&entry.cache_key) {
            Some(published) => Published {
                at: published.at.min(now),
                commit: published.commit.clone(),
            },
            None => Published {
                at: now,
                commit: None,
            },
        };
        if keep_stored_entry(&published, commit, now) {
            merged.published.insert(entry.cache_key.clone(), published);
            merged.manifest.entries.push(entry);
        }
    }
    merged
}

/// Refuse to store a manifest no reader would accept.
fn within_read_limit(body: Vec<u8>, limit: u64) -> Result<Vec<u8>> {
    anyhow::ensure!(
        body.len() as u64 <= limit,
        "merged manifest is {} bytes, over the {limit}-byte read limit",
        body.len()
    );
    Ok(body)
}

/// A strong entity tag, the only kind `If-Match` can compare.
fn strong_etag(etag: Option<String>) -> Option<String> {
    etag.filter(|tag| !tag.trim_start().starts_with("W/"))
}

/// Publish `manifest`, merged with what the remote already holds for its key
/// (see [`merge_manifest`]).
///
/// Where the store supports conditional writes, the read and the write are
/// tied by an entity tag: a publisher that loses a race reads again and
/// retries, and after [`MANIFEST_PUBLISH_ATTEMPTS`] it fails without writing,
/// leaving the winners' entries in place. A store that cannot make the write
/// conditional gets the merge written plainly, so a publisher racing between
/// the read and the write can still lose there.
pub async fn upload_manifest(
    backend: &dyn RemoteBackend,
    prefix: &str,
    manifest_key: &str,
    manifest: &BuildManifest,
    commit: Option<&str>,
) -> Result<()> {
    let object_key =
        crate::config::join_remote_key(prefix, &format!("{MANIFEST_PREFIX}/{manifest_key}.json"));
    let mut attempt = 0;
    loop {
        attempt += 1;
        let now = chrono::Utc::now().timestamp();
        let fetched = backend
            .get_versioned(&object_key, Some(MAX_METADATA_BYTES))
            .await
            .context("downloading manifest to merge")?;
        let present = fetched.is_some();
        let (existing, etag) = match fetched {
            // A manifest this build cannot parse is replaced, not merged.
            Some((object, etag)) => (serde_json::from_slice(&object.body).ok(), strong_etag(etag)),
            None => (None, None),
        };
        let merged = merge_manifest(existing, manifest, commit, now);
        let body = serde_json::to_vec(&merged).context("serializing manifest")?;
        let body = within_read_limit(body, MAX_METADATA_BYTES)?;
        let outcome = if present && etag.is_none() {
            // Nothing to make the replacement conditional on.
            ConditionalPut::Unsupported
        } else {
            backend
                .put_if_match(
                    &object_key,
                    body.clone(),
                    Some("application/json"),
                    etag.as_deref(),
                )
                .await
                .context("publishing merged manifest")?
        };
        match outcome {
            ConditionalPut::Stored => return Ok(()),
            ConditionalPut::Conflict if attempt < MANIFEST_PUBLISH_ATTEMPTS => continue,
            ConditionalPut::Conflict => anyhow::bail!(
                "{} kept changing during {MANIFEST_PUBLISH_ATTEMPTS} attempts to merge into it",
                backend.describe(&object_key)
            ),
            ConditionalPut::Unsupported => {
                return backend
                    .put(&object_key, body, Some("application/json"))
                    .await
                    .context("uploading manifest");
            }
        }
    }
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
                    compile_time_ms: Some(4200),
                    artifact_size: Some(9000),
                },
                // A pre-#617 shard entry: no cost, no size.
                ShardEntry {
                    cache_key: "def456".to_string(),
                    crate_name: "syn".to_string(),
                    compile_time_ms: None,
                    artifact_size: None,
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

        let got = try_download_manifest(&backend, "prefix", "key")
            .await
            .expect("download should succeed")
            .expect("manifest present");
        assert_eq!(got.entries.len(), 1);
        assert_eq!(got.entries[0].crate_name, "serde");
    }

    #[tokio::test]
    async fn upload_manifest_writes_the_expected_object() {
        let backend = crate::remote_backend::memory_backend();
        upload_manifest(&backend, "prefix", "mykey", &sample_manifest(), None)
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
                compile_time_ms: None,
                artifact_size: None,
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

    fn entry(cache_key: &str, compile_time_ms: u64) -> ManifestEntry {
        ManifestEntry {
            cache_key: cache_key.to_string(),
            crate_name: format!("crate-{cache_key}"),
            compile_time_ms,
            artifact_size: 1,
        }
    }

    fn manifest_with(entries: Vec<ManifestEntry>) -> BuildManifest {
        BuildManifest {
            version: 3,
            created: "2026-09-26T00:00:00Z".to_string(),
            manifest_key: "id/test".to_string(),
            entries,
        }
    }

    fn at(at: i64, commit: Option<&str>) -> Published {
        Published {
            at,
            commit: commit.map(str::to_string),
        }
    }

    fn stored(entries: Vec<(&str, Option<Published>)>) -> StoredManifest {
        StoredManifest {
            published: entries
                .iter()
                .filter_map(|(key, published)| {
                    published
                        .clone()
                        .map(|published| (key.to_string(), published))
                })
                .collect(),
            manifest: manifest_with(entries.iter().map(|(key, _)| entry(key, 1)).collect()),
        }
    }

    fn keys(manifest: &StoredManifest) -> Vec<&str> {
        manifest
            .manifest
            .entries
            .iter()
            .map(|entry| entry.cache_key.as_str())
            .collect()
    }

    const NOW: i64 = 1_000_000;

    #[test]
    fn merge_limits_are_an_hour_and_five_attempts() {
        assert_eq!(MANIFEST_MERGE_WINDOW_SECS, 3600);
        assert_eq!(MANIFEST_PUBLISH_ATTEMPTS, 5);
    }

    #[test]
    fn stored_entries_of_a_known_commit_stay_only_for_that_commit() {
        let ancient = at(NOW - 100_000, Some("c1"));
        assert!(keep_stored_entry(&ancient, Some("c1"), NOW));
        let fresh = at(NOW, Some("c1"));
        assert!(!keep_stored_entry(&fresh, Some("c2"), NOW));
    }

    #[test]
    fn stored_entries_with_an_unknown_commit_stay_for_an_hour() {
        for (stored_commit, new_commit) in [(None, None), (None, Some("c1")), (Some("c1"), None)] {
            assert!(keep_stored_entry(
                &at(NOW - 3600, stored_commit),
                new_commit,
                NOW
            ));
            assert!(!keep_stored_entry(
                &at(NOW - 3601, stored_commit),
                new_commit,
                NOW
            ));
        }
        assert!(!keep_stored_entry(&at(i64::MIN, None), None, NOW));
    }

    #[test]
    fn merge_without_a_stored_manifest_stamps_every_entry() {
        let merged = merge_manifest(None, &manifest_with(vec![entry("a", 1)]), Some("c1"), NOW);
        assert_eq!(keys(&merged), ["a"]);
        assert_eq!(merged.published.get("a"), Some(&at(NOW, Some("c1"))));
    }

    #[test]
    fn merge_puts_new_entries_first_and_restamps_duplicates() {
        let existing = stored(vec![
            ("shared", Some(at(NOW - 10, Some("c1")))),
            ("clippy", Some(at(NOW - 10, Some("c1")))),
        ]);
        let merged = merge_manifest(
            Some(existing),
            &manifest_with(vec![entry("test", 1), entry("shared", 9)]),
            Some("c1"),
            NOW,
        );
        assert_eq!(keys(&merged), ["test", "shared", "clippy"]);
        assert_eq!(merged.manifest.entries[1].compile_time_ms, 9);
        assert_eq!(merged.published.get("shared"), Some(&at(NOW, Some("c1"))));
        assert_eq!(
            merged.published.get("clippy"),
            Some(&at(NOW - 10, Some("c1")))
        );
    }

    #[test]
    fn merge_drops_other_commits_and_keeps_recent_unknown_entries() {
        let existing = stored(vec![
            ("older-commit", Some(at(NOW, Some("c1")))),
            ("same-commit", Some(at(NOW - 100_000, Some("c2")))),
            ("daemon-recent", Some(at(NOW - 3600, None))),
            ("daemon-stale", Some(at(NOW - 3601, None))),
        ]);
        let merged = merge_manifest(
            Some(existing),
            &manifest_with(vec![entry("new", 1)]),
            Some("c2"),
            NOW,
        );
        assert_eq!(keys(&merged), ["new", "same-commit", "daemon-recent"]);
        // A kept entry keeps its own provenance: the daemon's stays unknown,
        // so it still expires for later publishers of this commit.
        assert_eq!(
            merged.published.get("daemon-recent"),
            Some(&at(NOW - 3600, None))
        );
    }

    #[test]
    fn merge_keeps_entries_from_before_merging_once() {
        let merged = merge_manifest(
            Some(stored(vec![("legacy", None)])),
            &manifest_with(vec![entry("new", 1)]),
            Some("c1"),
            NOW,
        );
        assert_eq!(keys(&merged), ["new", "legacy"]);
        assert_eq!(merged.published.get("legacy"), Some(&at(NOW, None)));
    }

    #[test]
    fn merge_treats_a_future_stamp_as_now() {
        let merged = merge_manifest(
            Some(stored(vec![("future", Some(at(i64::MAX, None)))])),
            &manifest_with(vec![entry("new", 1)]),
            None,
            NOW,
        );
        assert_eq!(keys(&merged), ["new", "future"]);
        assert_eq!(merged.published.get("future"), Some(&at(NOW, None)));
    }

    #[test]
    fn stored_manifest_still_reads_as_a_build_manifest() {
        let merged = merge_manifest(None, &manifest_with(vec![entry("a", 1)]), Some("c1"), NOW);
        let json = serde_json::to_vec(&merged).unwrap();
        let plain: BuildManifest = serde_json::from_slice(&json).unwrap();
        assert_eq!(plain.entries.len(), 1);
        assert_eq!(plain.manifest_key, "id/test");
        let text = String::from_utf8(json).unwrap();
        assert!(
            text.contains(r#""published":{"a":{"at":1000000,"commit":"c1"}}"#),
            "{text}"
        );
    }

    #[test]
    fn merge_fills_up_to_the_entry_cap_with_stored_entries() {
        assert_eq!(MANIFEST_MAX_ENTRIES, 50_000);
        let stored_keys: Vec<String> = (0..MANIFEST_MAX_ENTRIES).map(|i| format!("s{i}")).collect();
        let existing = stored(
            stored_keys
                .iter()
                .map(|key| (key.as_str(), Some(at(NOW, None))))
                .collect(),
        );
        let merged = merge_manifest(
            Some(existing),
            &manifest_with(vec![entry("new", 1)]),
            None,
            NOW,
        );
        assert_eq!(merged.manifest.entries.len(), MANIFEST_MAX_ENTRIES);
        assert_eq!(merged.published.len(), MANIFEST_MAX_ENTRIES);
        let last = &merged.manifest.entries[MANIFEST_MAX_ENTRIES - 1].cache_key;
        assert_eq!(last, &format!("s{}", MANIFEST_MAX_ENTRIES - 2));

        // A build larger than the cap keeps every entry it published.
        let big: Vec<ManifestEntry> = (0..MANIFEST_MAX_ENTRIES + 1)
            .map(|i| entry(&format!("n{i}"), 1))
            .collect();
        let merged = merge_manifest(
            Some(stored(vec![("old", Some(at(NOW, None)))])),
            &manifest_with(big),
            None,
            NOW,
        );
        assert_eq!(merged.manifest.entries.len(), MANIFEST_MAX_ENTRIES + 1);
        assert!(!merged.published.contains_key("old"));
    }

    #[test]
    fn a_manifest_over_the_read_limit_is_refused() {
        assert_eq!(within_read_limit(vec![0; 4], 4).unwrap().len(), 4);
        assert!(within_read_limit(vec![0; 5], 4).is_err());
    }

    #[test]
    fn only_strong_entity_tags_are_used() {
        assert_eq!(
            strong_etag(Some("\"v1\"".into())).as_deref(),
            Some("\"v1\"")
        );
        assert_eq!(strong_etag(Some("W/\"v1\"".into())), None);
        assert_eq!(strong_etag(None), None);
    }

    #[derive(Debug, Clone, PartialEq)]
    enum Write {
        Conditional(Option<String>),
        Plain,
    }

    /// One scripted read: the stored bytes and their entity tag, or absence.
    type ScriptedRead = Option<(Vec<u8>, Option<String>)>;

    /// A manifest object whose reads and conditional-write answers are
    /// scripted per attempt. Records every write in order.
    #[derive(Default)]
    struct ScriptedStore {
        reads: std::sync::Mutex<std::collections::VecDeque<ScriptedRead>>,
        answers: std::sync::Mutex<std::collections::VecDeque<Result<ConditionalPut>>>,
        writes: std::sync::Mutex<Vec<(Write, Vec<u8>)>>,
    }

    impl ScriptedStore {
        fn new(reads: Vec<ScriptedRead>, answers: Vec<Result<ConditionalPut>>) -> Self {
            Self {
                reads: std::sync::Mutex::new(reads.into()),
                answers: std::sync::Mutex::new(answers.into()),
                writes: std::sync::Mutex::default(),
            }
        }

        fn writes(&self) -> Vec<(Write, StoredManifest)> {
            self.writes
                .lock()
                .unwrap()
                .iter()
                .map(|(write, body)| (write.clone(), serde_json::from_slice(body).unwrap()))
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl RemoteBackend for ScriptedStore {
        async fn head(&self, _key: &str) -> Result<bool> {
            Ok(false)
        }

        async fn get(
            &self,
            _key: &str,
            _max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            unreachable!("publishing reads through get_versioned")
        }

        async fn get_versioned(
            &self,
            _key: &str,
            _max_bytes: Option<u64>,
        ) -> Result<Option<(crate::remote_backend::GetObject, Option<String>)>> {
            let read = self
                .reads
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted read");
            Ok(read.map(|(body, etag)| {
                let object = crate::remote_backend::GetObject {
                    body: body.into(),
                    request_ms: 0,
                    body_ms: 0,
                };
                (object, etag)
            }))
        }

        async fn put_if_match(
            &self,
            _key: &str,
            body: Vec<u8>,
            _content_type: Option<&str>,
            expected: Option<&str>,
        ) -> Result<ConditionalPut> {
            self.writes
                .lock()
                .unwrap()
                .push((Write::Conditional(expected.map(str::to_string)), body));
            self.answers
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted answer")
        }

        async fn put(&self, _key: &str, body: Vec<u8>, _content_type: Option<&str>) -> Result<()> {
            self.writes.lock().unwrap().push((Write::Plain, body));
            Ok(())
        }

        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }

        fn describe(&self, key: &str) -> String {
            key.to_string()
        }
    }

    fn stored_json(keys: &[&str], commit: Option<&str>) -> Vec<u8> {
        let now = chrono::Utc::now().timestamp();
        let manifest = stored(
            keys.iter()
                .map(|key| (*key, Some(at(now, commit))))
                .collect(),
        );
        serde_json::to_vec(&manifest).unwrap()
    }

    fn written_keys(write: &(Write, StoredManifest)) -> Vec<&str> {
        keys(&write.1)
    }

    async fn publish(store: &ScriptedStore, commit: Option<&str>) -> Result<()> {
        upload_manifest(
            store,
            "p",
            "id/test",
            &manifest_with(vec![entry("a", 1)]),
            commit,
        )
        .await
    }

    #[tokio::test]
    async fn upload_creates_an_absent_manifest_only_if_still_absent() {
        let store = ScriptedStore::new(vec![None], vec![Ok(ConditionalPut::Stored)]);
        publish(&store, None).await.unwrap();
        let writes = store.writes();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, Write::Conditional(None));
        assert_eq!(written_keys(&writes[0]), ["a"]);
    }

    #[tokio::test]
    async fn upload_rereads_and_merges_after_a_conflict() {
        let store = ScriptedStore::new(
            vec![
                Some((stored_json(&["clippy"], Some("c1")), Some("\"e1\"".into()))),
                Some((
                    stored_json(&["clippy", "doc"], Some("c1")),
                    Some("\"e2\"".into()),
                )),
            ],
            vec![Ok(ConditionalPut::Conflict), Ok(ConditionalPut::Stored)],
        );
        publish(&store, Some("c1")).await.unwrap();
        let writes = store.writes();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].0, Write::Conditional(Some("\"e1\"".into())));
        assert_eq!(written_keys(&writes[0]), ["a", "clippy"]);
        assert_eq!(writes[1].0, Write::Conditional(Some("\"e2\"".into())));
        assert_eq!(written_keys(&writes[1]), ["a", "clippy", "doc"]);
    }

    #[tokio::test]
    async fn upload_fails_after_five_conflicts_without_overwriting() {
        let reads = (0..5)
            .map(|_| Some((stored_json(&["other"], None), Some("\"e\"".into()))))
            .collect();
        let answers = (0..5).map(|_| Ok(ConditionalPut::Conflict)).collect();
        let store = ScriptedStore::new(reads, answers);
        let error = publish(&store, None).await.unwrap_err();
        assert!(error.to_string().contains("5 attempts"), "{error:#}");
        let writes = store.writes();
        assert_eq!(writes.len(), 5);
        assert!(
            writes
                .iter()
                .all(|write| matches!(write.0, Write::Conditional(_)))
        );
    }

    #[tokio::test]
    async fn upload_writes_the_merge_plainly_where_conditions_are_unsupported() {
        let store = ScriptedStore::new(
            vec![Some((stored_json(&["other"], None), Some("\"e\"".into())))],
            vec![Ok(ConditionalPut::Unsupported)],
        );
        publish(&store, None).await.unwrap();
        let writes = store.writes();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[1].0, Write::Plain);
        assert_eq!(written_keys(&writes[1]), ["a", "other"]);
    }

    #[tokio::test]
    async fn upload_propagates_a_failed_conditional_write() {
        let store = ScriptedStore::new(
            vec![Some((stored_json(&["other"], None), Some("\"e\"".into())))],
            vec![Err(anyhow::anyhow!("timed out"))],
        );
        let error = publish(&store, None).await.unwrap_err();
        assert!(format!("{error:#}").contains("timed out"), "{error:#}");
        // A timed-out write may have landed; it is never repeated unconditionally.
        assert_eq!(store.writes().len(), 1);
    }

    #[tokio::test]
    async fn upload_without_a_strong_entity_tag_writes_the_merge_plainly() {
        for etag in [None, Some("W/\"e\"".to_string())] {
            let store =
                ScriptedStore::new(vec![Some((stored_json(&["other"], None), etag))], vec![]);
            publish(&store, None).await.unwrap();
            let writes = store.writes();
            assert_eq!(writes.len(), 1);
            assert_eq!(writes[0].0, Write::Plain);
            assert_eq!(written_keys(&writes[0]), ["a", "other"]);
        }
    }

    #[tokio::test]
    async fn upload_replaces_an_unreadable_manifest() {
        let store = ScriptedStore::new(
            vec![Some((b"not json".to_vec(), Some("\"e\"".into())))],
            vec![Ok(ConditionalPut::Stored)],
        );
        publish(&store, None).await.unwrap();
        let writes = store.writes();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, Write::Conditional(Some("\"e\"".into())));
        assert_eq!(written_keys(&writes[0]), ["a"]);

        // Present but unreadable and untagged: an existing object, not an
        // absent one, so no create-only write that could never succeed.
        let store = ScriptedStore::new(vec![Some((b"not json".to_vec(), None))], vec![]);
        publish(&store, None).await.unwrap();
        let writes = store.writes();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, Write::Plain);
    }

    #[tokio::test]
    async fn two_publishes_to_the_memory_store_keep_both_builds() {
        let backend = crate::remote_backend::memory_backend();
        let manifest = |key| manifest_with(vec![entry(key, 1)]);
        upload_manifest(&backend, "p", "id/test", &manifest("test"), Some("c1"))
            .await
            .unwrap();
        upload_manifest(&backend, "p", "id/test", &manifest("clippy"), Some("c1"))
            .await
            .unwrap();
        let stored = try_download_manifest(&backend, "p", "id/test")
            .await
            .unwrap()
            .expect("manifest");
        let keys: Vec<_> = stored
            .entries
            .iter()
            .map(|e| e.cache_key.as_str())
            .collect();
        assert_eq!(keys, ["clippy", "test"]);
    }
}
