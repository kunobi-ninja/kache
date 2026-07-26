#[cfg(feature = "planning")]
use std::collections::{HashMap, HashSet};

#[cfg(feature = "planning")]
use anyhow::Result;
#[cfg(feature = "planning")]
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BuildIntent {
    #[serde(default)]
    pub crate_names: Vec<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub cargo_lock_deps: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrefetchCandidate {
    pub cache_key: String,
    pub crate_name: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrefetchDisposition {
    Execute,
    UseFallback,
    DoNothing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrefetchPlan {
    #[serde(default)]
    pub plan_id: Option<String>,
    #[serde(default)]
    pub planner: Option<String>,
    pub disposition: PrefetchDisposition,
    #[serde(default)]
    pub candidates: Vec<PrefetchCandidate>,
}

#[cfg(feature = "planning")]
#[async_trait]
pub trait PlannerDataSource {
    async fn shard_candidates(
        &self,
        namespace: &str,
        deps: &[(String, String)],
    ) -> Result<Vec<PrefetchCandidate>>;

    async fn history_candidates(&self, crate_names: &[String]) -> Result<Vec<PrefetchCandidate>>;

    async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>>;
}

#[cfg(feature = "planning")]
pub async fn build_prefetch_plan<T>(
    source: &T,
    intent: &BuildIntent,
    planner_name: &str,
) -> Result<PrefetchPlan>
where
    T: PlannerDataSource + Sync + ?Sized,
{
    let crate_order = crate_query_order(intent);
    let mut seen = HashSet::new();
    let mut resolved_crates = HashSet::new();
    let mut candidates = Vec::new();

    // Sources are merged in descending order of confidence, each one filling
    // only the crates the ones before it left unresolved. Shard lookups used
    // to RETURN as soon as they produced anything (kunobi-ninja/kache#614),
    // but a shard hit is exact per bucket: one dependency bump invalidates one
    // of `NUM_SHARDS` buckets while the rest still match, so a single matching
    // shard was enough to short-circuit history and key-cache recovery for
    // every crate in every bucket that missed.
    if let Some(namespace) = intent.namespace.as_deref()
        && !intent.cargo_lock_deps.is_empty()
    {
        // A failed shard lookup is not fatal: the sources below still run.
        if let Ok(shard_candidates) = source
            .shard_candidates(namespace, &intent.cargo_lock_deps)
            .await
        {
            for candidate in order_candidates_by_crate_order(shard_candidates, intent) {
                resolved_crates.insert(candidate.crate_name.clone());
                if seen.insert(candidate.cache_key.clone()) {
                    candidates.push(candidate);
                }
            }
        }
    }

    let unresolved = |resolved: &HashSet<String>| -> Vec<String> {
        crate_order
            .iter()
            .filter(|name| !resolved.contains(*name))
            .cloned()
            .collect()
    };

    let history_query = unresolved(&resolved_crates);
    if !history_query.is_empty() {
        for candidate in order_candidates_by_crate_order(
            source.history_candidates(&history_query).await?,
            intent,
        ) {
            resolved_crates.insert(candidate.crate_name.clone());
            if seen.insert(candidate.cache_key.clone()) {
                candidates.push(candidate);
            }
        }
    }

    for crate_name in unresolved(&resolved_crates) {
        for cache_key in source.key_cache_keys_for_crate(&crate_name).await? {
            if seen.insert(cache_key.clone()) {
                candidates.push(PrefetchCandidate {
                    cache_key,
                    crate_name: crate_name.clone(),
                });
            }
        }
    }

    Ok(execute_plan(planner_name, candidates))
}

#[cfg(feature = "planning")]
fn execute_plan(planner_name: &str, candidates: Vec<PrefetchCandidate>) -> PrefetchPlan {
    let planner = planner_name.trim();
    PrefetchPlan {
        plan_id: None,
        planner: Some(if planner.is_empty() {
            "planner".to_string()
        } else {
            planner.to_string()
        }),
        disposition: PrefetchDisposition::Execute,
        candidates,
    }
}

#[cfg(feature = "planning")]
fn crate_query_order(intent: &BuildIntent) -> Vec<String> {
    let mut seen = HashSet::new();
    intent
        .crate_names
        .iter()
        .filter(|crate_name| seen.insert((*crate_name).clone()))
        .cloned()
        .collect()
}

#[cfg(feature = "planning")]
fn order_candidates_by_crate_order(
    mut candidates: Vec<PrefetchCandidate>,
    intent: &BuildIntent,
) -> Vec<PrefetchCandidate> {
    if intent.crate_names.is_empty() {
        return candidates;
    }

    let mut priority = HashMap::new();
    for (index, crate_name) in crate_query_order(intent).iter().enumerate() {
        priority.entry(crate_name.clone()).or_insert(index);
    }

    let mut indexed_candidates = candidates.drain(..).enumerate().collect::<Vec<_>>();
    indexed_candidates.sort_by_key(|(index, candidate)| {
        (
            priority
                .get(&candidate.crate_name)
                .copied()
                .unwrap_or(usize::MAX),
            *index,
        )
    });
    indexed_candidates
        .into_iter()
        .map(|(_, candidate)| candidate)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "planning")]
    use std::collections::HashMap;

    #[cfg(feature = "planning")]
    use anyhow::anyhow;

    #[cfg(feature = "planning")]
    #[derive(Default)]
    struct FakePlannerDataSource {
        shard_candidates: Vec<PrefetchCandidate>,
        shard_error: bool,
        history_candidates: Vec<PrefetchCandidate>,
        history_by_crate: HashMap<String, String>,
        key_cache: HashMap<String, Vec<String>>,
    }

    #[cfg(feature = "planning")]
    #[async_trait]
    impl PlannerDataSource for FakePlannerDataSource {
        async fn shard_candidates(
            &self,
            _namespace: &str,
            _deps: &[(String, String)],
        ) -> Result<Vec<PrefetchCandidate>> {
            if self.shard_error {
                Err(anyhow!("shard lookup failed"))
            } else {
                Ok(self.shard_candidates.clone())
            }
        }

        async fn history_candidates(
            &self,
            crate_names: &[String],
        ) -> Result<Vec<PrefetchCandidate>> {
            if !self.history_candidates.is_empty() {
                return Ok(self.history_candidates.clone());
            }

            Ok(crate_names
                .iter()
                .filter_map(|crate_name| {
                    self.history_by_crate
                        .get(crate_name)
                        .map(|cache_key| PrefetchCandidate {
                            cache_key: cache_key.clone(),
                            crate_name: crate_name.clone(),
                        })
                })
                .collect())
        }

        async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>> {
            Ok(self.key_cache.get(crate_name).cloned().unwrap_or_default())
        }
    }

    #[test]
    fn test_build_intent_serde_roundtrip() {
        let intent = BuildIntent {
            crate_names: vec!["serde".into(), "tokio".into()],
            namespace: Some("x86_64/hash/release".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let json = serde_json::to_string(&intent).unwrap();
        let parsed: BuildIntent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, intent);
    }

    #[test]
    fn test_build_intent_defaults_missing_fields() {
        let parsed: BuildIntent = serde_json::from_str(r#"{"crate_names":["serde"]}"#).unwrap();
        assert_eq!(parsed.crate_names, vec!["serde"]);
        assert!(parsed.namespace.is_none());
        assert!(parsed.cargo_lock_deps.is_empty());
    }

    #[test]
    fn test_prefetch_plan_serde_roundtrip() {
        let plan = PrefetchPlan {
            plan_id: Some("plan-1".into()),
            planner: Some("local".into()),
            disposition: PrefetchDisposition::Execute,
            candidates: vec![PrefetchCandidate {
                cache_key: "abc".into(),
                crate_name: "serde".into(),
            }],
        };

        let json = serde_json::to_string(&plan).unwrap();
        let parsed: PrefetchPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, plan);
    }

    #[test]
    fn test_prefetch_plan_missing_disposition_is_rejected() {
        let err = serde_json::from_str::<PrefetchPlan>(
            r#"{"planner":"legacy","candidates":[{"cache_key":"abc","crate_name":"serde"}]}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn test_prefetch_plan_do_nothing_roundtrip() {
        let plan = PrefetchPlan {
            plan_id: Some("plan-2".into()),
            planner: Some("remote".into()),
            disposition: PrefetchDisposition::DoNothing,
            candidates: vec![],
        };

        let json = serde_json::to_string(&plan).unwrap();
        let parsed: PrefetchPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, plan);
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_prefers_shard_candidates() {
        let source = FakePlannerDataSource {
            shard_candidates: vec![PrefetchCandidate {
                cache_key: "from-shard".into(),
                crate_name: "serde".into(),
            }],
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: Some("linux/hash/release".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        assert_eq!(plan.disposition, PrefetchDisposition::Execute);
        assert_eq!(plan.planner.as_deref(), Some("fallback"));
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, "from-shard");
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_falls_back_to_history_and_key_cache() {
        let mut source = FakePlannerDataSource {
            shard_error: true,
            history_candidates: vec![PrefetchCandidate {
                cache_key: "history-key".into(),
                crate_name: "serde".into(),
            }],
            ..Default::default()
        };
        source.key_cache.insert(
            "tokio".into(),
            vec!["tokio-key".into(), "history-key".into()],
        );

        let intent = BuildIntent {
            crate_names: vec!["serde".into(), "tokio".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        assert_eq!(plan.disposition, PrefetchDisposition::Execute);
        assert_eq!(plan.candidates.len(), 2);
        assert_eq!(plan.candidates[0].cache_key, "history-key");
        assert_eq!(plan.candidates[1].cache_key, "tokio-key");
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_orders_shard_candidates_by_crate_order() {
        let source = FakePlannerDataSource {
            shard_candidates: vec![
                PrefetchCandidate {
                    cache_key: "app-key".into(),
                    crate_name: "app".into(),
                },
                PrefetchCandidate {
                    cache_key: "dep-key".into(),
                    crate_name: "dep".into(),
                },
                PrefetchCandidate {
                    cache_key: "middle-key".into(),
                    crate_name: "middle".into(),
                },
            ],
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["dep".into(), "middle".into(), "app".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("dep".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["dep-key", "middle-key", "app-key"]);
    }

    /// A PARTIAL shard hit must not suppress the lower-confidence sources
    /// (kunobi-ninja/kache#614).
    ///
    /// Shard matching is exact per bucket, so a dependency bump invalidates
    /// one bucket while the rest still match. The planner used to return as
    /// soon as shards produced anything, so the crates in the missed buckets
    /// were dropped from the plan even though history and the key cache could
    /// resolve them.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_fills_crates_a_partial_shard_hit_missed() {
        let mut source = FakePlannerDataSource {
            // Only `dep` is in a bucket that still matches.
            shard_candidates: vec![PrefetchCandidate {
                cache_key: "dep-shard-key".into(),
                crate_name: "dep".into(),
            }],
            history_by_crate: HashMap::from([("middle".into(), "middle-history-key".into())]),
            ..Default::default()
        };
        source
            .key_cache
            .insert("app".into(), vec!["app-key-cache-key".into()]);

        let intent = BuildIntent {
            crate_names: vec!["dep".into(), "middle".into(), "app".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("dep".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec!["dep-shard-key", "middle-history-key", "app-key-cache-key"],
            "each source should fill the crates the higher-confidence ones left unresolved"
        );
    }

    /// A crate the shards already resolved is not re-queried from the
    /// lower-confidence sources (#614): shard keys are exact, history and the
    /// key cache are not, so they only fill gaps.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_does_not_requery_shard_resolved_crates() {
        let mut source = FakePlannerDataSource {
            shard_candidates: vec![PrefetchCandidate {
                cache_key: "serde-shard-key".into(),
                crate_name: "serde".into(),
            }],
            history_by_crate: HashMap::from([("serde".into(), "serde-stale-history-key".into())]),
            ..Default::default()
        };
        source
            .key_cache
            .insert("serde".into(), vec!["serde-stale-key-cache-key".into()]);

        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["serde-shard-key"]);
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_queries_history_by_crate_order() {
        let source = FakePlannerDataSource {
            history_by_crate: HashMap::from([
                ("app".into(), "app-key".into()),
                ("dep".into(), "dep-key".into()),
                ("middle".into(), "middle-key".into()),
            ]),
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["dep".into(), "middle".into(), "app".into()],
            namespace: None,
            cargo_lock_deps: vec![],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["dep-key", "middle-key", "app-key"]);
    }
}
