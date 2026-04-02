#[cfg(feature = "planning")]
use std::collections::HashSet;

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
    T: PlannerDataSource + Sync,
{
    if let Some(namespace) = intent.namespace.as_deref()
        && !intent.cargo_lock_deps.is_empty()
    {
        match source
            .shard_candidates(namespace, &intent.cargo_lock_deps)
            .await
        {
            Ok(candidates) if !candidates.is_empty() => {
                return Ok(execute_plan(planner_name, candidates));
            }
            Ok(_) | Err(_) => {}
        }
    }

    let mut seen = HashSet::new();
    let mut resolved_crates = HashSet::new();
    let mut candidates = Vec::new();

    for candidate in source.history_candidates(&intent.crate_names).await? {
        resolved_crates.insert(candidate.crate_name.clone());
        if seen.insert(candidate.cache_key.clone()) {
            candidates.push(candidate);
        }
    }

    for crate_name in intent
        .crate_names
        .iter()
        .filter(|name| !resolved_crates.contains(*name))
    {
        for cache_key in source.key_cache_keys_for_crate(crate_name).await? {
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
            _crate_names: &[String],
        ) -> Result<Vec<PrefetchCandidate>> {
            Ok(self.history_candidates.clone())
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
}
