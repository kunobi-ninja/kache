//! Entry-level access to the remote cache.
//!
//! Callers ask for cache entries, build manifests, shards and packed-prefetch
//! objects. `V3Remote` answers with the existing v3 object layout over a
//! `RemoteBackend`, delegating to `RemoteLayout` and `crate::remote`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;

use crate::config::RemoteConfig;
use crate::remote::{self, BuildManifest, DownloadResult, Shard};
use crate::remote_backend::{GetObject, RemoteBackend};
use crate::remote_layout::{DownloadObserver, ListObserver, RemoteLayout, RemoteUploadResult};

#[async_trait]
pub trait CacheRemote: Send + Sync {
    async fn exists_entry(&self, cache_key: &str, crate_name: &str) -> Result<bool>;

    async fn download_entry(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        deadline: Option<Instant>,
    ) -> Result<DownloadResult>;

    async fn download_entry_observed(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        deadline: Option<Instant>,
        observer: &mut dyn DownloadObserver,
    ) -> Result<DownloadResult>;

    async fn upload_entry(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        compression_level: i32,
        deadline: Option<Instant>,
    ) -> Result<RemoteUploadResult>;

    async fn list_keys(&self) -> Result<HashMap<String, String>>;

    async fn list_keys_observed(
        &self,
        observer: &mut dyn ListObserver,
    ) -> Result<HashMap<String, String>>;

    async fn list_keys_for_crates(
        &self,
        crate_names: &HashSet<String>,
    ) -> Result<HashMap<String, String>>;
}

/// v3 client-planner objects: build manifests, planner shards and
/// packed-prefetch catalogs and packs. Only the v3 layout has them.
#[async_trait]
pub trait V3Prefetch: Send + Sync {
    async fn get_build_manifest(&self, manifest_key: &str) -> Result<Option<BuildManifest>>;

    /// Publish `manifest` merged with what the remote holds for `manifest_key`;
    /// `commit` is the commit this build ran on, when CI says.
    async fn put_build_manifest(
        &self,
        manifest_key: &str,
        manifest: &BuildManifest,
        commit: Option<&str>,
    ) -> Result<()>;

    async fn get_shard(&self, namespace: &str, shard_hash: &str) -> Result<Option<Shard>>;

    /// Publish `shard` merged with what the remote holds for it; `commit` as
    /// for [`V3Prefetch::put_build_manifest`].
    async fn put_shard(
        &self,
        namespace: &str,
        shard_hash: &str,
        shard: &Shard,
        commit: Option<&str>,
    ) -> Result<()>;

    /// Object keys under `prefix` for packed prefetch discovery.
    async fn list_prefetch_objects(&self, prefix: &str) -> Result<Vec<String>>;

    /// One packed-prefetch object (catalog or pack), capped at `max_bytes`.
    async fn get_prefetch_object(&self, key: &str, max_bytes: u64) -> Result<Option<GetObject>>;
}

/// The v3 object layout over a byte-object backend.
pub struct V3Remote {
    backend: Arc<dyn RemoteBackend>,
    remote: RemoteConfig,
}

impl V3Remote {
    pub fn new(backend: Arc<dyn RemoteBackend>, remote: RemoteConfig) -> Self {
        Self { backend, remote }
    }

    fn layout(&self) -> RemoteLayout<'_> {
        RemoteLayout::new(self.backend.as_ref(), &self.remote)
    }
}

#[async_trait]
impl CacheRemote for V3Remote {
    async fn exists_entry(&self, cache_key: &str, crate_name: &str) -> Result<bool> {
        self.layout().exists_entry(cache_key, crate_name).await
    }

    async fn download_entry(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        deadline: Option<Instant>,
    ) -> Result<DownloadResult> {
        self.layout()
            .download_entry_until(cache_key, crate_name, entry_dir, blobs_dir, deadline)
            .await
    }

    async fn download_entry_observed(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        deadline: Option<Instant>,
        observer: &mut dyn DownloadObserver,
    ) -> Result<DownloadResult> {
        self.layout()
            .download_entry_observed(
                cache_key,
                crate_name,
                entry_dir,
                blobs_dir,
                deadline,
                Some(observer),
            )
            .await
    }

    async fn upload_entry(
        &self,
        cache_key: &str,
        crate_name: &str,
        entry_dir: &Path,
        blobs_dir: &Path,
        compression_level: i32,
        deadline: Option<Instant>,
    ) -> Result<RemoteUploadResult> {
        self.layout()
            .upload_entry_until(
                cache_key,
                crate_name,
                entry_dir,
                blobs_dir,
                compression_level,
                deadline,
            )
            .await
    }

    async fn list_keys(&self) -> Result<HashMap<String, String>> {
        self.layout().list_keys().await
    }

    async fn list_keys_observed(
        &self,
        observer: &mut dyn ListObserver,
    ) -> Result<HashMap<String, String>> {
        self.layout().list_keys_observed(Some(observer)).await
    }

    async fn list_keys_for_crates(
        &self,
        crate_names: &HashSet<String>,
    ) -> Result<HashMap<String, String>> {
        self.layout().list_keys_for_crates(crate_names).await
    }
}

#[async_trait]
impl V3Prefetch for V3Remote {
    async fn get_build_manifest(&self, manifest_key: &str) -> Result<Option<BuildManifest>> {
        remote::try_download_manifest(self.backend.as_ref(), &self.remote.prefix, manifest_key)
            .await
    }

    async fn put_build_manifest(
        &self,
        manifest_key: &str,
        manifest: &BuildManifest,
        commit: Option<&str>,
    ) -> Result<()> {
        remote::upload_manifest(
            self.backend.as_ref(),
            &self.remote.prefix,
            manifest_key,
            manifest,
            commit,
        )
        .await
    }

    async fn get_shard(&self, namespace: &str, shard_hash: &str) -> Result<Option<Shard>> {
        remote::download_shard(
            self.backend.as_ref(),
            &self.remote.prefix,
            namespace,
            shard_hash,
        )
        .await
    }

    async fn put_shard(
        &self,
        namespace: &str,
        shard_hash: &str,
        shard: &Shard,
        commit: Option<&str>,
    ) -> Result<()> {
        remote::upload_shard(
            self.backend.as_ref(),
            &self.remote.prefix,
            namespace,
            shard_hash,
            shard,
            commit,
        )
        .await
    }

    async fn list_prefetch_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.backend.list(prefix).await
    }

    async fn get_prefetch_object(&self, key: &str, max_bytes: u64) -> Result<Option<GetObject>> {
        self.backend.get(key, Some(max_bytes)).await
    }
}

#[cfg(test)]
impl V3Remote {
    /// Prefix for one packed-prefetch catalog's objects. `selector` must be a
    /// lowercase BLAKE3 digest, matching what callers derive it from.
    fn catalog_prefix(&self, selector: &str) -> Result<String> {
        crate::remote_pack::catalog_prefix(&self.remote.prefix, selector)
    }

    pub fn backend(&self) -> &Arc<dyn RemoteBackend> {
        &self.backend
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote_backend::{RemoteBackend, memory_backend};

    fn v3() -> (Arc<dyn RemoteBackend>, V3Remote) {
        let backend: Arc<dyn RemoteBackend> = Arc::new(memory_backend());
        let remote = V3Remote::new(
            Arc::clone(&backend),
            RemoteConfig::test_s3("bucket", "artifacts"),
        );
        (backend, remote)
    }

    /// A 64-char lowercase hex digest, the shape `is_valid_cache_key` and the
    /// packed-prefetch selector/digest validation both require.
    fn digest(label: &str) -> String {
        blake3::hash(label.as_bytes()).to_hex().to_string()
    }

    #[tokio::test]
    async fn exists_entry_reads_the_v3_manifest_key() {
        let (backend, remote) = v3();
        backend
            .put(
                "artifacts/v3/manifests/foo/key123.json",
                b"{}".to_vec(),
                Some("application/json"),
            )
            .await
            .unwrap();
        assert!(remote.exists_entry("key123", "foo").await.unwrap());
        assert!(!remote.exists_entry("missing", "foo").await.unwrap());
    }

    #[tokio::test]
    async fn list_keys_and_list_keys_for_crates_match_the_layout() {
        let (backend, remote) = v3();
        // `list_keys`/`list_keys_for_crates` filter out cache keys that are
        // not 64-char lowercase-hex BLAKE3 digests, so the fixture keys must
        // be real digests rather than short literals.
        let key1 = digest("k1");
        let key2 = digest("k2");
        for (crate_name, key) in [("foo", &key1), ("bar", &key2)] {
            backend
                .put(
                    &format!("artifacts/v3/manifests/{crate_name}/{key}.json"),
                    b"{}".to_vec(),
                    Some("application/json"),
                )
                .await
                .unwrap();
        }
        let all = remote.list_keys().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get(&key1).map(String::as_str), Some("foo"));

        let only_foo: HashSet<String> = ["foo".to_string()].into();
        let some = remote.list_keys_for_crates(&only_foo).await.unwrap();
        assert_eq!(some.len(), 1);
        assert!(some.contains_key(&key1));
    }

    #[tokio::test]
    async fn build_manifest_round_trips() {
        let (_backend, remote) = v3();
        assert!(remote.get_build_manifest("m1").await.unwrap().is_none());
        let manifest = remote::BuildManifest {
            version: 3,
            created: "2025-01-01T00:00:00Z".to_string(),
            manifest_key: "m1".to_string(),
            entries: vec![remote::ManifestEntry {
                cache_key: digest("k1"),
                crate_name: "foo".to_string(),
                compile_time_ms: 1234,
                artifact_size: 5678,
            }],
        };
        remote
            .put_build_manifest("m1", &manifest, None)
            .await
            .unwrap();
        let fetched = remote.get_build_manifest("m1").await.unwrap().unwrap();
        // `BuildManifest` has no `PartialEq`; compare the serialized shape instead.
        assert_eq!(
            serde_json::to_value(&fetched).unwrap(),
            serde_json::to_value(&manifest).unwrap()
        );
    }

    #[tokio::test]
    async fn shard_round_trips() {
        let (_backend, remote) = v3();
        assert!(remote.get_shard("ns", "h1").await.unwrap().is_none());
        let shard = remote::Shard {
            version: 3,
            entries: vec![remote::ShardEntry {
                cache_key: digest("k1"),
                crate_name: "foo".to_string(),
                compile_time_ms: Some(1234),
                artifact_size: Some(5678),
            }],
        };
        remote.put_shard("ns", "h1", &shard, None).await.unwrap();
        let fetched = remote.get_shard("ns", "h1").await.unwrap().unwrap();
        // `Shard` has no `PartialEq`; compare the serialized shape instead.
        assert_eq!(
            serde_json::to_value(&fetched).unwrap(),
            serde_json::to_value(&shard).unwrap()
        );
    }

    #[tokio::test]
    async fn prefetch_objects_are_listed_and_fetched_under_the_prefix() {
        let (backend, remote) = v3();
        let selector = digest("selector");
        let prefix = remote.catalog_prefix(&selector).unwrap();
        let key = format!("{prefix}{:020}-{}.json", 1, digest("catalog"));
        backend
            .put(&key, b"catalog".to_vec(), Some("application/json"))
            .await
            .unwrap();
        let listed = remote.list_prefetch_objects(&prefix).await.unwrap();
        assert_eq!(listed, vec![key.clone()]);
        let object = remote
            .get_prefetch_object(&key, 1024)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(object.body, b"catalog".to_vec());
        assert!(
            remote
                .get_prefetch_object("artifacts/none", 1024)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn upload_then_download_entry_round_trips_through_v3() {
        let (_tmp, store, entry_dir) = crate::remote_layout::tests::populated_entry();
        let (backend, remote) = v3();

        let uploaded = remote
            .upload_entry("key123", "foo", &entry_dir, &store.blobs_dir(), 3, None)
            .await
            .unwrap();
        assert!(uploaded.transfer.compressed_bytes > 0);
        assert!(
            backend
                .head("artifacts/v3/packs/foo/key123.tar.zst")
                .await
                .unwrap()
        );
        assert!(remote.exists_entry("key123", "foo").await.unwrap());

        let restore_dir = tempfile::tempdir().unwrap();
        let downloaded = remote
            .download_entry(
                "key123",
                "foo",
                restore_dir.path(),
                &store.blobs_dir(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(downloaded.format, "v3");
        assert!(restore_dir.path().join("meta.json").exists());
    }

    #[tokio::test]
    async fn download_entry_keeps_the_entry_not_found_chain() {
        let (_backend, remote) = v3();
        let tmp = tempfile::tempdir().unwrap();
        let error = match remote
            .download_entry("missing", "foo", tmp.path(), tmp.path(), None)
            .await
        {
            Ok(_) => panic!("expected a miss for an absent entry"),
            Err(error) => error,
        };
        assert_eq!(
            crate::remote_resilience::classify_remote_error(&error),
            crate::remote_resilience::RemoteErrorClass::Miss
        );
    }
}
