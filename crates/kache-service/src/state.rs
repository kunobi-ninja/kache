use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use kache_core::{PlannerDataSource, PrefetchCandidate};
use serde::{Deserialize, Serialize};
use surrealdb::{
    Surreal,
    engine::local::{Db, RocksDb},
};

pub const DEFAULT_DB_PATH: &str = "/var/lib/kache/planner.db";

const PLANNER_NAMESPACE: &str = "kache";
const PLANNER_DATABASE: &str = "planner";

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

#[derive(Debug, Clone)]
pub struct SurrealPlannerRepository {
    db: Surreal<Db>,
}

impl SurrealPlannerRepository {
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating planner db directory {}", parent.display()))?;
        }

        let db = Surreal::new::<RocksDb>(path.to_path_buf())
            .await
            .with_context(|| format!("opening embedded planner db at {}", path.display()))?;
        db.use_ns(PLANNER_NAMESPACE)
            .use_db(PLANNER_DATABASE)
            .await
            .context("selecting planner namespace/database")?;

        let repo = Self { db };
        repo.init_schema().await?;
        Ok(repo)
    }

    pub async fn seed_from_state_file(&self, path: &Path) -> Result<()> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading planner seed state from {}", path.display()))?;
        let state: PlannerStateFile = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing planner seed state from {}", path.display()))?;
        self.seed_from_state(state).await
    }

    pub async fn seed_from_state(&self, state: PlannerStateFile) -> Result<()> {
        for (namespace, namespace_state) in state.namespaces {
            for (dep_key, candidates) in namespace_state.deps {
                for candidate in candidates {
                    self.upsert_namespace_artifact(&namespace, &dep_key, &candidate)
                        .await?;
                    self.upsert_crate_artifact(&candidate.crate_name, &candidate)
                        .await?;
                }
            }
        }

        for (crate_name, candidates) in state.history {
            for candidate in candidates {
                self.upsert_crate_artifact(&crate_name, &candidate).await?;
            }
        }

        for (crate_name, cache_keys) in state.key_cache {
            for cache_key in cache_keys {
                self.upsert_crate_artifact(
                    &crate_name,
                    &PrefetchCandidate {
                        cache_key,
                        crate_name: crate_name.clone(),
                    },
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn init_schema(&self) -> Result<()> {
        self.db
            .query(
                r#"
DEFINE TABLE namespace_artifact SCHEMAFULL;
DEFINE FIELD namespace ON namespace_artifact TYPE string;
DEFINE FIELD dep_key ON namespace_artifact TYPE string;
DEFINE FIELD cache_key ON namespace_artifact TYPE string;
DEFINE FIELD crate_name ON namespace_artifact TYPE string;
DEFINE FIELD last_seen_at ON namespace_artifact TYPE datetime;
DEFINE INDEX namespace_dep_cache ON namespace_artifact FIELDS namespace, dep_key, cache_key UNIQUE;

DEFINE TABLE crate_artifact SCHEMAFULL;
DEFINE FIELD crate_name ON crate_artifact TYPE string;
DEFINE FIELD cache_key ON crate_artifact TYPE string;
DEFINE FIELD last_seen_at ON crate_artifact TYPE datetime;
DEFINE INDEX crate_cache ON crate_artifact FIELDS crate_name, cache_key UNIQUE;
"#,
            )
            .await
            .context("initializing planner db schema")?
            .check()
            .context("validating planner db schema")?;

        Ok(())
    }

    async fn upsert_namespace_artifact(
        &self,
        namespace: &str,
        dep_key: &str,
        candidate: &PrefetchCandidate,
    ) -> Result<()> {
        self.db
            .query(
                r#"
UPSERT type::record("namespace_artifact", $id) CONTENT {
    namespace: $namespace,
    dep_key: $dep_key,
    cache_key: $cache_key,
    crate_name: $crate_name,
    last_seen_at: time::now()
};
"#,
            )
            .bind((
                "id",
                composite_id(&[namespace, dep_key, &candidate.cache_key]),
            ))
            .bind(("namespace", namespace.to_string()))
            .bind(("dep_key", dep_key.to_string()))
            .bind(("cache_key", candidate.cache_key.clone()))
            .bind(("crate_name", candidate.crate_name.clone()))
            .await
            .context("upserting namespace artifact projection")?
            .check()
            .context("validating namespace artifact upsert")?;

        Ok(())
    }

    async fn upsert_crate_artifact(
        &self,
        crate_name: &str,
        candidate: &PrefetchCandidate,
    ) -> Result<()> {
        self.db
            .query(
                r#"
UPSERT type::record("crate_artifact", $id) CONTENT {
    crate_name: $crate_name,
    cache_key: $cache_key,
    last_seen_at: time::now()
};
"#,
            )
            .bind(("id", composite_id(&[crate_name, &candidate.cache_key])))
            .bind(("crate_name", crate_name.to_string()))
            .bind(("cache_key", candidate.cache_key.clone()))
            .await
            .context("upserting crate artifact projection")?
            .check()
            .context("validating crate artifact upsert")?;

        Ok(())
    }
}

#[async_trait]
impl PlannerDataSource for SurrealPlannerRepository {
    async fn shard_candidates(
        &self,
        namespace: &str,
        deps: &[(String, String)],
    ) -> Result<Vec<PrefetchCandidate>> {
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();

        for (name, version) in deps {
            let dep_key = dep_key(name, version);
            let mut response = self
                .db
                .query(
                    r#"
SELECT cache_key, crate_name
     , last_seen_at
FROM namespace_artifact
WHERE namespace = $namespace AND dep_key = $dep_key
ORDER BY last_seen_at DESC;
"#,
                )
                .bind(("namespace", namespace.to_string()))
                .bind(("dep_key", dep_key.clone()))
                .await
                .context("querying namespace artifact projections")?
                .check()
                .context("validating namespace artifact query")?;

            let cache_keys: Vec<String> =
                response.take("cache_key").context("decoding cache keys")?;
            let crate_names: Vec<String> = response
                .take("crate_name")
                .context("decoding crate names")?;

            for (cache_key, crate_name) in cache_keys.into_iter().zip(crate_names) {
                if seen.insert(cache_key.clone()) {
                    candidates.push(PrefetchCandidate {
                        cache_key,
                        crate_name,
                    });
                }
            }
        }

        Ok(candidates)
    }

    async fn history_candidates(&self, crate_names: &[String]) -> Result<Vec<PrefetchCandidate>> {
        let mut seen = HashSet::new();
        let mut candidates = Vec::new();

        for crate_name in crate_names {
            let mut response = self
                .db
                .query(
                    r#"
SELECT cache_key, crate_name
     , last_seen_at
FROM crate_artifact
WHERE crate_name = $crate_name
ORDER BY last_seen_at DESC;
"#,
                )
                .bind(("crate_name", crate_name.clone()))
                .await
                .context("querying crate artifact history")?
                .check()
                .context("validating crate artifact history query")?;

            let cache_keys: Vec<String> =
                response.take("cache_key").context("decoding cache keys")?;
            let crate_names: Vec<String> = response
                .take("crate_name")
                .context("decoding crate names")?;

            for (cache_key, row_crate_name) in cache_keys.into_iter().zip(crate_names) {
                if seen.insert(cache_key.clone()) {
                    candidates.push(PrefetchCandidate {
                        cache_key,
                        crate_name: row_crate_name,
                    });
                }
            }
        }

        Ok(candidates)
    }

    async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>> {
        let mut response = self
            .db
            .query(
                r#"
SELECT cache_key, last_seen_at
FROM crate_artifact
WHERE crate_name = $crate_name
ORDER BY last_seen_at DESC;
"#,
            )
            .bind(("crate_name", crate_name.to_string()))
            .await
            .context("querying crate cache keys")?
            .check()
            .context("validating crate cache key query")?;

        response
            .take("cache_key")
            .context("decoding crate cache keys")
    }
}

fn dep_key(name: &str, version: &str) -> String {
    format!("{name}@{version}")
}

fn composite_id(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|part| {
            part.chars()
                .map(|ch| match ch {
                    'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
                    _ => '_',
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("__")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn repository_resolves_namespace_candidates_from_seed_state() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();
        repo.seed_from_state(PlannerStateFile {
            namespaces: HashMap::from([(
                "linux/hash/debug".to_string(),
                NamespaceState {
                    deps: HashMap::from([(
                        "serde@1.0.0".to_string(),
                        vec![PrefetchCandidate {
                            cache_key: "serde-key".to_string(),
                            crate_name: "serde".to_string(),
                        }],
                    )]),
                },
            )]),
            history: HashMap::new(),
            key_cache: HashMap::new(),
        })
        .await
        .unwrap();

        let candidates = repo
            .shard_candidates(
                "linux/hash/debug",
                &[("serde".to_string(), "1.0.0".to_string())],
            )
            .await
            .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].cache_key, "serde-key");
    }

    #[tokio::test]
    async fn repository_loads_seed_state_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("planner.db");
        let seed_path = dir.path().join("planner-state.json");

        std::fs::write(
            &seed_path,
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

        let repo = SurrealPlannerRepository::open(&db_path).await.unwrap();
        repo.seed_from_state_file(&seed_path).await.unwrap();

        let history = repo
            .history_candidates(&["serde".to_string()])
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].cache_key, "serde-key");

        let keys = repo.key_cache_keys_for_crate("tokio").await.unwrap();
        assert_eq!(keys, vec!["tokio-key".to_string()]);
    }
}
