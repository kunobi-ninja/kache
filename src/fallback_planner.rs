use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::future::join_all;
use kache_core::{BuildIntent, PlannerDataSource, PrefetchCandidate, PrefetchPlan};

use crate::daemon::Daemon;

pub async fn build_prefetch_plan(
    daemon: &Arc<Daemon>,
    intent: &BuildIntent,
) -> Result<PrefetchPlan> {
    let (plan, composition) = kache_core::build_prefetch_plan_with_limits(
        &LocalPlannerSource { daemon },
        intent,
        "fallback",
        kache_core::PlanLimits::default(),
    )
    .await?;

    // Never silently truncate: a plan trimmed by a composition cap must be
    // distinguishable from one that had nothing more to offer (#616).
    if should_report_composition(&composition) {
        tracing::info!(
            candidates = plan.candidates.len(),
            from_shards = composition.from_shards,
            from_history = composition.from_history,
            from_key_cache = composition.from_key_cache,
            dropped_history_per_crate = composition.dropped_history_per_crate,
            dropped_key_cache_per_crate = composition.dropped_key_cache_per_crate,
            dropped_key_cache_total = composition.dropped_key_cache_total,
            "fallback planner: composition caps trimmed the plan"
        );
    }

    Ok(plan)
}

/// Report a plan's composition only when a cap actually dropped something
/// (kunobi-ninja/kache#616).
///
/// A separate predicate rather than an inline condition so the "only when
/// something was dropped" rule is testable: every plan logging its full
/// composition would drown the interesting case, and never logging would make
/// a truncated plan indistinguishable from an exhausted one.
fn should_report_composition(composition: &kache_core::PlanComposition) -> bool {
    composition.dropped_total() > 0
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
        self.daemon
            .remote_config()
            .ok_or_else(|| anyhow::anyhow!("no remote configured"))?;
        let shard_set = crate::shards::compute_shards(namespace, deps);

        tracing::info!(
            "fallback planner: {} deps -> {} shards for namespace '{namespace}'",
            deps.len(),
            shard_set.shards.len()
        );

        let shard_fetches = shard_set
            .shards
            .iter()
            .map(|(hash, _entries)| self.daemon.download_planner_shard(namespace, hash));
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
                                compile_time_ms: entry.compile_time_ms,
                                size_bytes: entry.artifact_size,
                                source: kache_core::CandidateSource::Shard,
                                demand_index: None,
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
            .map(|entry| PrefetchCandidate {
                cache_key: entry.cache_key,
                crate_name: entry.crate_name,
                compile_time_ms: entry.compile_time_ms,
                size_bytes: entry.size_bytes,
                source: kache_core::CandidateSource::History,
                demand_index: None,
            })
            .collect())
    }

    async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>> {
        let keys = self.daemon.key_cache_keys_for_crate(crate_name).await;
        if !keys.is_empty() {
            tracing::info!(
                "fallback planner: resolved {} extra candidates from remote key cache for crate '{}'",
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

    /// Composition is reported only when a cap actually dropped something
    /// (#616): always logging would drown the interesting case, never logging
    /// would hide a truncated plan.
    #[test]
    fn test_should_report_composition_only_when_something_dropped() {
        let mut composition = kache_core::PlanComposition::default();
        assert!(
            !should_report_composition(&composition),
            "a plan that dropped nothing is not worth reporting"
        );

        composition.from_shards = 40;
        composition.from_history = 5;
        assert!(
            !should_report_composition(&composition),
            "candidates admitted is not a reason to report"
        );

        composition.dropped_key_cache_per_crate = 1;
        assert!(
            should_report_composition(&composition),
            "a single drop is enough to report"
        );
    }
    use crate::config::{
        Config, DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS,
    };
    use crate::store::Store;

    fn test_config(
        cache_dir: std::path::PathBuf,
        remote: Option<crate::config::RemoteConfig>,
    ) -> Config {
        Config {
            remote_error: None,
            socket_path_override: None,
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            runtime_dir: cache_dir.clone(),
            cache_dir,
            max_size: 1024 * 1024,
            remote,
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
        // A fresh daemon's remote key cache is unpopulated, so the planner source
        // resolves no extra candidates (the `if !keys.is_empty()` false arm).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"), None);
        let daemon = Arc::new(Daemon::new(config));
        let source = LocalPlannerSource { daemon: &daemon };

        let keys = source.key_cache_keys_for_crate("serde").await.unwrap();
        assert!(keys.is_empty());
    }
}
