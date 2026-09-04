use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::future::join_all;
use kache_core::{BuildIntent, PlannerDataSource, PrefetchCandidate, PrefetchPlan};

use crate::daemon::{Daemon, SpeculativeManifestOutcome};
use crate::remote_resilience::{RemoteErrorClass, classify_remote_error};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SpeculativeIdentityOutcome {
    Resolved(Vec<PrefetchCandidate>),
    NotAdmitted(Vec<String>),
    RetryableFailures(Vec<(String, RemoteErrorClass)>),
    NonRetryableFailures(Vec<(String, RemoteErrorClass)>),
}

/// Convert an unresolved speculative lookup into exact keys for the ordinary
/// fallback path. Once fallback is selected, it must use demand admission
/// rather than remain queued behind the speculative prefetch gate.
pub(crate) fn retry_identity_with_ordinary_admission(
    intent: &BuildIntent,
) -> SpeculativeIdentityOutcome {
    let Some(identity_key) = intent
        .identity_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
    else {
        return SpeculativeIdentityOutcome::Resolved(Vec::new());
    };
    SpeculativeIdentityOutcome::NotAdmitted(crate::identity::manifest_lookup_keys(Some(
        identity_key,
    )))
}

/// Resolve identity metadata as lookahead without consuming a half-open
/// remote-breaker probe. Admission denial and retryable transport failure stay
/// distinct from an authoritative hit or miss.
pub async fn resolve_identity_candidates_speculative(
    daemon: &Arc<Daemon>,
    intent: &BuildIntent,
) -> SpeculativeIdentityOutcome {
    let Some(identity_key) = intent
        .identity_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
    else {
        return SpeculativeIdentityOutcome::Resolved(Vec::new());
    };

    let lookup_keys = crate::identity::manifest_lookup_keys(Some(identity_key));
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    let mut retryable_failures: Vec<(String, RemoteErrorClass)> = Vec::new();
    let mut non_retryable_failures: Vec<(String, RemoteErrorClass)> = Vec::new();
    for (key_index, key) in lookup_keys.iter().enumerate() {
        match daemon.download_planner_manifest_speculative(key).await {
            Ok(SpeculativeManifestOutcome::Completed(Some(manifest))) => {
                tracing::info!(
                    "fallback planner: identity manifest '{key}' has {} entries",
                    manifest.entries.len()
                );
                for (index, entry) in manifest.entries.into_iter().enumerate() {
                    if seen.insert(entry.cache_key.clone()) {
                        candidates.push(manifest_entry_candidate(entry, index));
                    }
                }
                return SpeculativeIdentityOutcome::Resolved(candidates);
            }
            Ok(SpeculativeManifestOutcome::Completed(None)) => {}
            Ok(SpeculativeManifestOutcome::NotAdmitted) => {
                let mut retry_keys = retryable_failures
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                retry_keys.extend(lookup_keys[key_index..].iter().cloned());
                return SpeculativeIdentityOutcome::NotAdmitted(retry_keys);
            }
            Err(e) => {
                let class = classify_remote_error(&e);
                match class {
                    RemoteErrorClass::Transient => {
                        retryable_failures.push((key.clone(), class));
                    }
                    RemoteErrorClass::Timeout => {
                        return SpeculativeIdentityOutcome::NonRetryableFailures(vec![(
                            key.clone(),
                            class,
                        )]);
                    }
                    _ => non_retryable_failures.push((key.clone(), class)),
                }
                tracing::debug!(
                    ?class,
                    "fallback planner: speculative identity manifest '{key}': {e}"
                );
            }
        }
    }
    if !retryable_failures.is_empty() {
        SpeculativeIdentityOutcome::RetryableFailures(retryable_failures)
    } else if !non_retryable_failures.is_empty() {
        SpeculativeIdentityOutcome::NonRetryableFailures(non_retryable_failures)
    } else {
        SpeculativeIdentityOutcome::Resolved(candidates)
    }
}

/// Build the fallback plan, reusing identity candidates resolved while the
/// advisory planner was in flight. Only denied or retryable lookahead performs
/// the ordinary lookup; an empty resolved set is authoritative for this plan.
pub async fn build_prefetch_plan_with_identity(
    daemon: &Arc<Daemon>,
    intent: &BuildIntent,
    identity_outcome: SpeculativeIdentityOutcome,
) -> Result<PrefetchPlan> {
    let (identity_candidates, identity_lookup_keys) = match identity_outcome {
        SpeculativeIdentityOutcome::Resolved(candidates) => (Some(candidates), None),
        SpeculativeIdentityOutcome::NotAdmitted(keys) => (None, Some(keys)),
        SpeculativeIdentityOutcome::RetryableFailures(failures) => (
            None,
            Some(failures.into_iter().map(|(key, _)| key).collect()),
        ),
        SpeculativeIdentityOutcome::NonRetryableFailures(_) => (Some(Vec::new()), None),
    };
    let (plan, composition) = kache_core::build_prefetch_plan_with_limits(
        &LocalPlannerSource {
            daemon,
            identity_candidates,
            identity_lookup_keys,
        },
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
            from_identity = composition.from_identity,
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
    identity_candidates: Option<Vec<PrefetchCandidate>>,
    identity_lookup_keys: Option<Vec<String>>,
}

fn manifest_entry_candidate(
    entry: crate::remote::ManifestEntry,
    index: usize,
) -> PrefetchCandidate {
    PrefetchCandidate {
        cache_key: entry.cache_key,
        crate_name: entry.crate_name,
        compile_time_ms: (entry.compile_time_ms > 0).then_some(entry.compile_time_ms),
        size_bytes: (entry.artifact_size > 0).then_some(entry.artifact_size),
        source: kache_core::CandidateSource::Manifest,
        demand_index: u32::try_from(index).ok(),
    }
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

    async fn identity_candidates(&self, identity_key: &str) -> Result<Vec<PrefetchCandidate>> {
        if let Some(candidates) = &self.identity_candidates {
            return Ok(candidates.clone());
        }

        let mut candidates = Vec::new();
        let mut seen = HashSet::new();
        let lookup_keys = self
            .identity_lookup_keys
            .clone()
            .unwrap_or_else(|| crate::identity::manifest_lookup_keys(Some(identity_key)));
        for key in lookup_keys {
            match self.daemon.download_planner_manifest(&key).await {
                Ok(Some(manifest)) => {
                    tracing::info!(
                        "fallback planner: identity manifest '{key}' has {} entries",
                        manifest.entries.len()
                    );
                    for (index, entry) in manifest.entries.into_iter().enumerate() {
                        if seen.insert(entry.cache_key.clone()) {
                            candidates.push(manifest_entry_candidate(entry, index));
                        }
                    }
                    break;
                }
                Ok(None) => {}
                Err(e) => tracing::debug!("fallback planner: identity manifest '{key}': {e}"),
            }
        }
        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_entry_zero_measurements_remain_unknown() {
        let candidate = manifest_entry_candidate(
            crate::remote::ManifestEntry {
                cache_key: "key".into(),
                crate_name: "crate".into(),
                compile_time_ms: 0,
                artifact_size: 0,
            },
            7,
        );
        assert_eq!(candidate.compile_time_ms, None);
        assert_eq!(candidate.size_bytes, None);
        assert_eq!(candidate.demand_index, Some(7));
    }

    #[test]
    fn manifest_entry_positive_measurements_are_preserved() {
        let candidate = manifest_entry_candidate(
            crate::remote::ManifestEntry {
                cache_key: "key".into(),
                crate_name: "crate".into(),
                compile_time_ms: 12,
                artifact_size: 34,
            },
            0,
        );
        assert_eq!(candidate.compile_time_ms, Some(12));
        assert_eq!(candidate.size_bytes, Some(34));
        assert_eq!(candidate.demand_index, Some(0));
    }

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
    use std::sync::atomic::{AtomicU64, Ordering};

    struct FailFirstManifestGetBackend {
        inner: Arc<dyn crate::remote_backend::RemoteBackend>,
        manifest_gets: AtomicU64,
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for FailFirstManifestGetBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.head(key).await
        }

        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            if key.contains("/_manifests/")
                && self.manifest_gets.fetch_add(1, Ordering::Relaxed) == 0
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "transient speculative manifest failure",
                )
                .into());
            }
            self.inner.get(key, max_bytes).await
        }

        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            self.inner.put(key, body, content_type).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }

        fn describe(&self, key: &str) -> String {
            self.inner.describe(key)
        }
    }

    struct EnvRestore {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn set_env(key: &'static str, value: Option<&str>) -> EnvRestore {
        let previous = std::env::var_os(key);
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        EnvRestore { key, previous }
    }

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
            input_predictions: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
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
        let source = LocalPlannerSource {
            daemon: &daemon,
            identity_candidates: None,
            identity_lookup_keys: None,
        };

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
        let source = LocalPlannerSource {
            daemon: &daemon,
            identity_candidates: None,
            identity_lookup_keys: None,
        };

        let err = source
            .shard_candidates("ns", &[("serde".to_string(), "1.0.0".to_string())])
            .await
            .expect_err("no remote -> error");
        assert!(
            err.to_string().contains("no remote configured"),
            "got: {err}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn identity_candidates_return_entries_from_the_first_manifest() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key = set_env("KACHE_MANIFEST_KEY", None);
        let dir = tempfile::tempdir().unwrap();
        let remote = crate::config::RemoteConfig::test_s3("bucket", "prefix");
        let config = test_config(dir.path().join("cache"), Some(remote));
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> =
            Arc::new(crate::remote_backend::memory_backend());
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2026-08-30T00:00:00Z".into(),
            manifest_key: "id/test".into(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: "cache-key".into(),
                crate_name: "serde".into(),
                compile_time_ms: 1200,
                artifact_size: 4096,
            }],
        };
        crate::remote::upload_manifest(backend.as_ref(), "prefix", "id/test", &manifest)
            .await
            .unwrap();
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend);
        let source = LocalPlannerSource {
            daemon: &daemon,
            identity_candidates: None,
            identity_lookup_keys: None,
        };

        let candidates = source.identity_candidates("id/test").await.unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].cache_key, "cache-key");
        assert_eq!(candidates[0].crate_name, "serde");
        assert_eq!(candidates[0].compile_time_ms, Some(1200));
        assert_eq!(candidates[0].size_bytes, Some(4096));
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn speculative_identity_falls_back_to_the_legacy_alias_after_a_miss() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key = set_env("KACHE_MANIFEST_KEY", None);
        let dir = tempfile::tempdir().unwrap();
        let remote = crate::config::RemoteConfig::test_s3("bucket", "prefix");
        let config = test_config(dir.path().join("cache"), Some(remote));
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> =
            Arc::new(crate::remote_backend::memory_backend());
        let cache_key = "f".repeat(64);
        let legacy = crate::identity::host_target_triple();
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2026-08-31T00:00:00Z".into(),
            manifest_key: legacy.clone(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: cache_key.clone(),
                crate_name: "serde".into(),
                compile_time_ms: 1200,
                artifact_size: 4096,
            }],
        };
        crate::remote::upload_manifest(backend.as_ref(), "prefix", &legacy, &manifest)
            .await
            .unwrap();
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend);
        let intent = BuildIntent {
            identity_key: Some("id/test".into()),
            ..Default::default()
        };

        let outcome = resolve_identity_candidates_speculative(&daemon, &intent).await;

        assert!(matches!(
            outcome,
            SpeculativeIdentityOutcome::Resolved(candidates)
                if candidates.len() == 1 && candidates[0].cache_key == cache_key
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn speculative_manifest_error_is_retried_by_ordinary_fallback_lookup() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key = set_env("KACHE_MANIFEST_KEY", Some("id/test"));
        let dir = tempfile::tempdir().unwrap();
        let remote = crate::config::RemoteConfig::test_s3("bucket", "prefix");
        let config = test_config(dir.path().join("cache"), Some(remote));
        let inner: Arc<dyn crate::remote_backend::RemoteBackend> =
            Arc::new(crate::remote_backend::memory_backend());
        let cache_key = "e".repeat(64);
        let manifest = crate::remote::BuildManifest {
            version: 3,
            created: "2026-08-31T00:00:00Z".into(),
            manifest_key: "id/test".into(),
            entries: vec![crate::remote::ManifestEntry {
                cache_key: cache_key.clone(),
                crate_name: "serde".into(),
                compile_time_ms: 1200,
                artifact_size: 4096,
            }],
        };
        crate::remote::upload_manifest(inner.as_ref(), "prefix", "id/test", &manifest)
            .await
            .unwrap();
        let backend = Arc::new(FailFirstManifestGetBackend {
            inner,
            manifest_gets: AtomicU64::new(0),
        });
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend.clone());
        let intent = BuildIntent {
            identity_key: Some("id/test".into()),
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let speculative = resolve_identity_candidates_speculative(&daemon, &intent).await;
        assert_eq!(
            speculative,
            SpeculativeIdentityOutcome::RetryableFailures(vec![(
                "id/test".into(),
                RemoteErrorClass::Transient,
            )])
        );
        assert_eq!(backend.manifest_gets.load(Ordering::Relaxed), 1);

        let plan = build_prefetch_plan_with_identity(&daemon, &intent, speculative)
            .await
            .expect("ordinary fallback lookup must retry the inconclusive lookahead");

        assert_eq!(backend.manifest_gets.load(Ordering::Relaxed), 2);
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, cache_key);
    }

    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn successful_speculative_miss_is_authoritative() {
        let _lock = crate::config::config_path_lock();
        let _manifest_key = set_env("KACHE_MANIFEST_KEY", Some("id/test"));
        let dir = tempfile::tempdir().unwrap();
        let remote = crate::config::RemoteConfig::test_s3("bucket", "prefix");
        let config = test_config(dir.path().join("cache"), Some(remote));
        let backend: Arc<dyn crate::remote_backend::RemoteBackend> =
            Arc::new(crate::remote_backend::memory_backend());
        let daemon = Arc::new(Daemon::new(config));
        daemon.set_remote_backend_for_test(backend);
        let intent = BuildIntent {
            identity_key: Some("id/test".into()),
            ..Default::default()
        };

        assert_eq!(
            resolve_identity_candidates_speculative(&daemon, &intent).await,
            SpeculativeIdentityOutcome::Resolved(Vec::new())
        );
    }

    #[tokio::test]
    async fn fallback_plan_reuses_preloaded_identity_candidates_without_a_remote() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"), None);
        let daemon = Arc::new(Daemon::new(config));
        let candidate = PrefetchCandidate {
            cache_key: "a".repeat(64),
            crate_name: "serde".into(),
            compile_time_ms: Some(1_200),
            size_bytes: Some(4_096),
            source: kache_core::CandidateSource::Manifest,
            demand_index: Some(0),
        };
        let intent = BuildIntent {
            identity_key: Some("id/test".into()),
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let plan = build_prefetch_plan_with_identity(
            &daemon,
            &intent,
            SpeculativeIdentityOutcome::Resolved(vec![candidate.clone(), candidate]),
        )
        .await
        .expect("preloaded identity metadata must not need a configured remote");

        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, "a".repeat(64));
        assert_eq!(
            plan.candidates[0].source,
            kache_core::CandidateSource::Manifest
        );
    }

    #[tokio::test]
    async fn key_cache_keys_for_crate_is_empty_until_populated() {
        // A fresh daemon's remote key cache is unpopulated, so the planner source
        // resolves no extra candidates (the `if !keys.is_empty()` false arm).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"), None);
        let daemon = Arc::new(Daemon::new(config));
        let source = LocalPlannerSource {
            daemon: &daemon,
            identity_candidates: None,
            identity_lookup_keys: None,
        };

        let keys = source.key_cache_keys_for_crate("serde").await.unwrap();
        assert!(keys.is_empty());
    }
}
