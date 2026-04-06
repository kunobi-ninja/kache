use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use kache_core::{PlannerDataSource, PrefetchCandidate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannerStateFile {
    #[serde(default)]
    pub namespaces: HashMap<String, NamespaceState>,
    #[serde(default)]
    pub history: HashMap<String, Vec<PrefetchCandidate>>,
    #[serde(default)]
    pub key_cache: HashMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceState {
    #[serde(default)]
    pub deps: HashMap<String, Vec<PrefetchCandidate>>,
}

#[derive(Debug, Clone, Default)]
pub struct FilePlannerRepository {
    namespaces: HashMap<String, NamespaceState>,
    history: HashMap<String, Vec<PrefetchCandidate>>,
    key_cache: HashMap<String, Vec<String>>,
}

impl FilePlannerRepository {
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading planner state from {}", path.display()))?;
        let state: PlannerStateFile = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing planner state from {}", path.display()))?;
        Ok(Self::from_state(state))
    }

    pub fn from_state(state: PlannerStateFile) -> Self {
        Self {
            namespaces: state.namespaces,
            history: state.history,
            key_cache: state.key_cache,
        }
    }
}

#[async_trait]
impl PlannerDataSource for FilePlannerRepository {
    async fn shard_candidates(
        &self,
        namespace: &str,
        deps: &[(String, String)],
    ) -> Result<Vec<PrefetchCandidate>> {
        let Some(namespace_state) = self.namespaces.get(namespace) else {
            return Ok(Vec::new());
        };

        let mut seen = HashSet::new();
        let mut candidates = Vec::new();

        for (name, version) in deps {
            if let Some(entries) = namespace_state.deps.get(&dep_key(name, version)) {
                for candidate in entries {
                    if seen.insert(candidate.cache_key.clone()) {
                        candidates.push(candidate.clone());
                    }
                }
            }
        }

        Ok(candidates)
    }

    async fn history_candidates(&self, crate_names: &[String]) -> Result<Vec<PrefetchCandidate>> {
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();

        for crate_name in crate_names {
            if let Some(entries) = self.history.get(crate_name) {
                for candidate in entries {
                    if seen.insert(candidate.cache_key.clone()) {
                        candidates.push(candidate.clone());
                    }
                }
            }
        }

        Ok(candidates)
    }

    async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>> {
        Ok(self.key_cache.get(crate_name).cloned().unwrap_or_default())
    }
}

fn dep_key(name: &str, version: &str) -> String {
    format!("{name}@{version}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn repository_resolves_namespace_candidates_from_dep_index() {
        let repo = FilePlannerRepository::from_state(PlannerStateFile {
            namespaces: HashMap::from([(
                "linux/hash/debug".to_string(),
                NamespaceState {
                    deps: HashMap::from([
                        (
                            "serde@1.0.0".to_string(),
                            vec![PrefetchCandidate {
                                cache_key: "serde-key".to_string(),
                                crate_name: "serde".to_string(),
                            }],
                        ),
                        (
                            "tokio@1.0.0".to_string(),
                            vec![
                                PrefetchCandidate {
                                    cache_key: "tokio-key".to_string(),
                                    crate_name: "tokio".to_string(),
                                },
                                PrefetchCandidate {
                                    cache_key: "serde-key".to_string(),
                                    crate_name: "serde".to_string(),
                                },
                            ],
                        ),
                    ]),
                },
            )]),
            history: HashMap::new(),
            key_cache: HashMap::new(),
        });

        let candidates = repo
            .shard_candidates(
                "linux/hash/debug",
                &[
                    ("serde".to_string(), "1.0.0".to_string()),
                    ("tokio".to_string(), "1.0.0".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].cache_key, "serde-key");
        assert_eq!(candidates[1].cache_key, "tokio-key");
    }

    #[test]
    fn repository_loads_json_state_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("planner-state.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&PlannerStateFile {
                namespaces: HashMap::new(),
                history: HashMap::from([(
                    "serde".to_string(),
                    vec![PrefetchCandidate {
                        cache_key: "serde-key".to_string(),
                        crate_name: "serde".to_string(),
                    }],
                )]),
                key_cache: HashMap::from([("tokio".to_string(), vec!["tokio-key".to_string()])]),
            })
            .unwrap(),
        )
        .unwrap();

        let repo = FilePlannerRepository::load(&path).unwrap();
        assert_eq!(repo.history.get("serde").unwrap().len(), 1);
        assert_eq!(repo.key_cache.get("tokio").unwrap(), &["tokio-key"]);
    }
}
