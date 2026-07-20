use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::future::join_all;
use kache_core::{
    BuildIntent, PlannerDataSource, PrefetchCandidate, PrefetchPlan,
    build_prefetch_plan as build_core_prefetch_plan,
};

use crate::daemon::Daemon;

pub async fn build_prefetch_plan(
    daemon: &Arc<Daemon>,
    intent: &BuildIntent,
) -> Result<PrefetchPlan> {
    build_core_prefetch_plan(&LocalPlannerSource { daemon }, intent, "fallback").await
}

struct LocalPlannerSource<'a> {
    daemon: &'a Arc<Daemon>,
}

#[async_trait]
impl PlannerDataSource for LocalPlannerSource<'_> {
    async fn shard_candidates(
        &self,
        namespace: &str,
        deps: &[(String, String)],
    ) -> Result<Vec<PrefetchCandidate>> {
        let remote = self
            .daemon
            .remote_config()
            .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
        let backend = self.daemon.get_remote_backend().await?;
        let shard_set = crate::shards::compute_shards(namespace, deps);

        tracing::info!(
            "fallback planner: {} deps -> {} shards for namespace '{namespace}'",
            deps.len(),
            shard_set.shards.len()
        );

        let shard_fetches = shard_set.shards.iter().map(|(hash, _entries)| {
            crate::remote::download_shard(backend.as_ref(), &remote.prefix, namespace, hash)
        });
        let shard_results = join_all(shard_fetches).await;

        let mut shards_matched = 0usize;
        let mut candidates = Vec::new();
        let mut seen = HashSet::new();

        for result in shard_results {
            match result {
                Ok(Some(shard)) => {
                    shards_matched += 1;
                    for entry in shard.entries {
                        if seen.insert(entry.cache_key.clone()) {
                            candidates.push(PrefetchCandidate {
                                cache_key: entry.cache_key,
                                crate_name: entry.crate_name,
                            });
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => tracing::warn!("fallback planner: shard download error: {e}"),
            }
        }

        tracing::info!(
            "fallback planner: {shards_matched}/{} shards matched, {} candidates resolved",
            shard_set.shards.len(),
            candidates.len()
        );

        Ok(candidates)
    }

    async fn history_candidates(&self, crate_names: &[String]) -> Result<Vec<PrefetchCandidate>> {
        let entries = self
            .daemon
            .with_store(|store| store.keys_for_crates(crate_names))?;
        Ok(entries
            .into_iter()
            .map(|(cache_key, crate_name, _entry_dir)| PrefetchCandidate {
                cache_key,
                crate_name,
            })
            .collect())
    }

    async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>> {
        let keys = self.daemon.key_cache_keys_for_crate(crate_name).await;
        if !keys.is_empty() {
            tracing::info!(
                "fallback planner: resolved {} extra candidates from S3 key cache for crate '{}'",
                keys.len(),
                crate_name
            );
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS};
    use crate::store::Store;

    fn test_config(
        cache_dir: std::path::PathBuf,
        remote: Option<crate::config::RemoteConfig>,
    ) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            windows_hardlink: false,
            auto_gc: true,
            path_only_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir,
            max_size: 1024 * 1024,
            remote,
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

    /// Seed one committed store entry for `crate_name` in `config`'s cache dir.
    fn seed_entry(config: &Config, cache_key: &str, crate_name: &str, dir: &std::path::Path) {
        let store = Store::open(config).unwrap();
        let src = dir.join(format!("{crate_name}.rlib"));
        std::fs::write(&src, b"artifact").unwrap();
        store
            .put(
                cache_key,
                crate_name,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(src, format!("{crate_name}.rlib"))],
                "",
                "",
            )
            .unwrap();
    }

    #[tokio::test]
    async fn history_candidates_returns_local_store_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"), None);
        seed_entry(&config, "key_serde_1", "serde", dir.path());

        let daemon = Arc::new(Daemon::new(config));
        let source = LocalPlannerSource { daemon: &daemon };

        let candidates = source
            .history_candidates(&["serde".to_string()])
            .await
            .expect("history_candidates should succeed");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].cache_key, "key_serde_1");
        assert_eq!(candidates[0].crate_name, "serde");

        // A crate with no entries yields nothing.
        let none = source
            .history_candidates(&["nonexistent".to_string()])
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn shard_candidates_errors_when_no_remote_configured() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"), None); // remote = None
        let daemon = Arc::new(Daemon::new(config));
        let source = LocalPlannerSource { daemon: &daemon };

        let err = source
            .shard_candidates("ns", &[("serde".to_string(), "1.0.0".to_string())])
            .await
            .expect_err("no remote -> error");
        assert!(
            err.to_string().contains("no remote configured"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn key_cache_keys_for_crate_is_empty_until_populated() {
        // A fresh daemon's S3 key cache is unpopulated, so the planner source
        // resolves no extra candidates (the `if !keys.is_empty()` false arm).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"), None);
        let daemon = Arc::new(Daemon::new(config));
        let source = LocalPlannerSource { daemon: &daemon };

        let keys = source.key_cache_keys_for_crate("serde").await.unwrap();
        assert!(keys.is_empty());
    }
}
