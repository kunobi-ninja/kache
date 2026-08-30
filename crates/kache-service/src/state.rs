use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use async_trait::async_trait;
use kache_core::{CandidateSource, PlannerDataSource, PrefetchCandidate};
use serde::{Deserialize, Serialize};
use surrealdb::{
    Surreal,
    engine::local::{Db, SurrealKv},
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

        let db = Surreal::new::<SurrealKv>(path.to_path_buf())
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
                    &PrefetchCandidate::new(cache_key, crate_name.clone()),
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Define the planner tables, tolerating a database that already has them.
    ///
    /// This runs on every `open`, so it runs on every process start — and the
    /// planner db lives on a persistent volume in production. Plain
    /// `DEFINE TABLE` errors with "The table 'x' already exists" the second
    /// time, which is not a startup the service can recover from: it exits 1
    /// and CrashLoopBackOffs forever while the volume keeps the tables that
    /// caused it. `IF NOT EXISTS` makes each definition a no-op when present.
    ///
    /// Deliberately not `OVERWRITE`: on a SCHEMAFULL table that redefines
    /// rather than skips, which would churn the schema on every start.
    async fn init_schema(&self) -> Result<()> {
        self.db
            .query(
                r#"
DEFINE TABLE IF NOT EXISTS namespace_artifact SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS namespace ON namespace_artifact TYPE string;
DEFINE FIELD IF NOT EXISTS dep_key ON namespace_artifact TYPE string;
DEFINE FIELD IF NOT EXISTS cache_key ON namespace_artifact TYPE string;
DEFINE FIELD IF NOT EXISTS crate_name ON namespace_artifact TYPE string;
DEFINE FIELD IF NOT EXISTS last_seen_at ON namespace_artifact TYPE datetime;
DEFINE INDEX IF NOT EXISTS namespace_dep_cache ON namespace_artifact FIELDS namespace, dep_key, cache_key UNIQUE;

DEFINE TABLE IF NOT EXISTS crate_artifact SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS crate_name ON crate_artifact TYPE string;
DEFINE FIELD IF NOT EXISTS cache_key ON crate_artifact TYPE string;
DEFINE FIELD IF NOT EXISTS last_seen_at ON crate_artifact TYPE datetime;
DEFINE INDEX IF NOT EXISTS crate_cache ON crate_artifact FIELDS crate_name, cache_key UNIQUE;
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
        // Let the unique index own logical identity. Derived record ids can
        // collide, while index-based upserts also update legacy-id rows.
        self.db
            .query(
                r#"
UPSERT namespace_artifact CONTENT {
    namespace: $namespace,
    dep_key: $dep_key,
    cache_key: $cache_key,
    crate_name: $crate_name,
    last_seen_at: time::now()
};
"#,
            )
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
        // See `upsert_namespace_artifact`: record ids are deliberately opaque.
        self.db
            .query(
                r#"
UPSERT crate_artifact CONTENT {
    crate_name: $crate_name,
    cache_key: $cache_key,
    last_seen_at: time::now()
};
"#,
            )
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
                    candidates.push(
                        PrefetchCandidate::new(cache_key, crate_name)
                            .with_source(CandidateSource::Shard),
                    );
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
                    candidates.push(
                        PrefetchCandidate::new(cache_key, row_crate_name)
                            .with_source(CandidateSource::History),
                    );
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

    async fn identity_candidates(&self, _identity_key: &str) -> Result<Vec<PrefetchCandidate>> {
        // Identity manifests live on the object store the daemon reads, not in
        // the planner's Surreal projections. An empty list lets shards/history
        // fill the plan; the daemon fallback still fetches the object.
        Ok(Vec::new())
    }
}

fn dep_key(name: &str, version: &str) -> String {
    format!("{name}@{version}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Defining the schema against a db that already has it must succeed.
    ///
    /// Every other test here starts from an empty tempdir, so the suite only
    /// ever exercised the first-ever start. Production does not: the planner db
    /// sits on a persistent volume, so the *second* start is the normal case
    /// and the only one that can hit "table already exists". That gap let a
    /// non-idempotent `DEFINE TABLE` reach production, where the planner exited
    /// 1 on boot and CrashLoopBackOffed ~3700 times over 13 days without ever
    /// becoming ready.
    ///
    /// Re-runs `init_schema` rather than reopening the file: surrealkv holds an
    /// exclusive LOCK that a dropped handle does not release synchronously, so
    /// a genuine reopen would need a sleep-and-retry and would be flaky in CI.
    /// A restarting pod runs exactly this statement against exactly this state.
    #[tokio::test]
    async fn init_schema_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();

        // `open` defines the schema once.
        let repo = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();

        repo.init_schema()
            .await
            .expect("defining the schema against an existing schema must succeed");
    }

    #[test]
    fn dep_key_joins_name_and_version() {
        assert_eq!(dep_key("serde", "1.0.0"), "serde@1.0.0");
        assert_eq!(dep_key("", ""), "@");
    }

    #[tokio::test]
    async fn namespace_upsert_preserves_legacy_row_and_colliding_tuple() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();

        // v0.16 generated this lossy id for both `linux/hash/debug` and
        // `linux_hash_debug`. Keep it literal so this test guards compatibility
        // with databases written by that release, independent of new code.
        repo.db
            .query(
                r#"
UPSERT type::record("namespace_artifact", $id) CONTENT {
    namespace: "linux/hash/debug",
    dep_key: "serde@1.0.0",
    cache_key: "shared-key",
    crate_name: "legacy-value",
    last_seen_at: time::now()
};
"#,
            )
            .bind((
                "id",
                "linux_hash_debug__serde_1_0_0__shared-key".to_string(),
            ))
            .await
            .unwrap()
            .check()
            .unwrap();

        repo.upsert_namespace_artifact(
            "linux/hash/debug",
            "serde@1.0.0",
            &PrefetchCandidate::new("shared-key".to_string(), "slash-value".to_string()),
        )
        .await
        .unwrap();
        repo.upsert_namespace_artifact(
            "linux_hash_debug",
            "serde@1.0.0",
            &PrefetchCandidate::new("shared-key".to_string(), "underscore-value".to_string()),
        )
        .await
        .unwrap();

        let deps = [("serde".to_string(), "1.0.0".to_string())];
        let slash = repo
            .shard_candidates("linux/hash/debug", &deps)
            .await
            .unwrap();
        let underscore = repo
            .shard_candidates("linux_hash_debug", &deps)
            .await
            .unwrap();

        assert_eq!(slash.len(), 1);
        assert_eq!(slash[0].cache_key, "shared-key");
        assert_eq!(slash[0].crate_name, "slash-value");
        assert_eq!(underscore.len(), 1);
        assert_eq!(underscore[0].cache_key, "shared-key");
        assert_eq!(underscore[0].crate_name, "underscore-value");
    }

    #[tokio::test]
    async fn crate_upsert_preserves_legacy_row_and_colliding_tuple() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();

        // v0.16 generated this same lossy id for `serde/json` and `serde_json`.
        repo.db
            .query(
                r#"
UPSERT type::record("crate_artifact", $id) CONTENT {
    crate_name: "serde/json",
    cache_key: "shared-key",
    last_seen_at: time::now()
};
"#,
            )
            .bind(("id", "serde_json__shared-key".to_string()))
            .await
            .unwrap()
            .check()
            .unwrap();

        repo.upsert_crate_artifact(
            "serde/json",
            &PrefetchCandidate::new("shared-key".to_string(), "serde/json".to_string()),
        )
        .await
        .unwrap();
        repo.upsert_crate_artifact(
            "serde_json",
            &PrefetchCandidate::new("shared-key".to_string(), "serde_json".to_string()),
        )
        .await
        .unwrap();

        assert_eq!(
            repo.key_cache_keys_for_crate("serde/json").await.unwrap(),
            ["shared-key"]
        );
        assert_eq!(
            repo.key_cache_keys_for_crate("serde_json").await.unwrap(),
            ["shared-key"]
        );
    }

    #[tokio::test]
    async fn shard_candidates_dedupes_repeated_cache_keys_across_deps() {
        // The same cache_key seen under two different deps must be returned
        // once — the planner's `seen` set guards against duplicate prefetch.
        let dir = tempfile::tempdir().unwrap();
        let repo = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();
        repo.seed_from_state(PlannerStateFile {
            namespaces: HashMap::from([(
                "ns".to_string(),
                NamespaceState {
                    deps: HashMap::from([
                        (
                            "a@1".to_string(),
                            vec![PrefetchCandidate::new(
                                "shared-key".to_string(),
                                "shared".to_string(),
                            )],
                        ),
                        (
                            "b@1".to_string(),
                            vec![PrefetchCandidate::new(
                                "shared-key".to_string(),
                                "shared".to_string(),
                            )],
                        ),
                    ]),
                },
            )]),
            history: HashMap::new(),
            key_cache: HashMap::new(),
        })
        .await
        .unwrap();

        let candidates = repo
            .shard_candidates(
                "ns",
                &[
                    ("a".to_string(), "1".to_string()),
                    ("b".to_string(), "1".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].cache_key, "shared-key");
    }

    #[tokio::test]
    async fn queries_return_empty_for_unknown_keys() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();

        assert!(
            repo.shard_candidates("missing", &[("x".to_string(), "1".to_string())])
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.history_candidates(&["nope".to_string()])
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            repo.key_cache_keys_for_crate("nope")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn seed_from_state_file_rejects_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SurrealPlannerRepository::open(&dir.path().join("planner.db"))
            .await
            .unwrap();
        let err = repo
            .seed_from_state_file(&dir.path().join("does-not-exist.json"))
            .await;
        assert!(err.is_err());
    }

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
                        vec![PrefetchCandidate::new(
                            "serde-key".to_string(),
                            "serde".to_string(),
                        )],
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
                    vec![PrefetchCandidate::new(
                        "serde-key".to_string(),
                        "serde".to_string(),
                    )],
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
