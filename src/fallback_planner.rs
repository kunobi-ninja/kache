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
        let client = self.daemon.get_s3_client().await?;
        let shard_set = crate::shards::compute_shards(namespace, deps);

        tracing::info!(
            "fallback planner: {} deps -> {} shards for namespace '{namespace}'",
            deps.len(),
            shard_set.shards.len()
        );

        let shard_fetches = shard_set.shards.iter().map(|(hash, _entries)| {
            crate::remote::download_shard(client, &remote.bucket, &remote.prefix, namespace, hash)
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
