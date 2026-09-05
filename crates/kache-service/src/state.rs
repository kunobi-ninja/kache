use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use kache_core::{CandidateSource, PlannerDataSource, PrefetchCandidate};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};

pub const DEFAULT_DB_PATH: &str = "/var/lib/kache/planner.db";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannerStateFile {
    #[serde(default)]
    pub namespaces: HashMap<String, NamespaceState>,
    #[serde(default)]
    pub history: HashMap<String, Vec<PrefetchCandidate>>,
    #[serde(default)]
    pub key_cache: HashMap<String, Vec<String>>,
}

impl PlannerStateFile {
    pub fn read_from(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading planner seed state from {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing planner seed state from {}", path.display()))
    }

    /// Whether seeding this would write no rows. Every field carries
    /// `#[serde(default)]`, so `{}` parses successfully and means exactly this.
    pub fn is_empty(&self) -> bool {
        self.namespaces
            .values()
            .all(|namespace| namespace.deps.values().all(|c| c.is_empty()))
            && self.history.values().all(|c| c.is_empty())
            && self.key_cache.values().all(|k| k.is_empty())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceState {
    #[serde(default)]
    pub deps: HashMap<String, Vec<PrefetchCandidate>>,
}

/// The planner's artifact projections, held in a SQLite file.
///
/// The workload is two tables of `(key tuple) -> cache key`, read by equality
/// and ordered by recency, written only while the leader seeds at startup.
/// That is what the same SQLite the local cache index already uses does well,
/// so the planner uses it too rather than a second storage engine.
#[derive(Clone)]
pub struct SqlitePlannerRepository {
    db: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for SqlitePlannerRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePlannerRepository")
            .finish_non_exhaustive()
    }
}

/// Whether the caller will repopulate the projections after `open`.
///
/// Only matters when the db path still holds a pre-SQLite planner database.
/// Those rows can only ever have come from a seed file — nothing else in the
/// service writes them — so whether the old store is disposable depends
/// entirely on whether rows are about to be put back.
///
/// `Reseeded` is a claim about a seed the caller has ALREADY read, parsed and
/// found non-empty; deriving it from "a `--seed-state-file` was configured"
/// would promise a rebuild that a missing, malformed or `{}` seed never
/// delivers. It still does not promise the new rows match the old ones: seeding
/// upserts and never clears, so a seed narrower than the accumulated store
/// leaves the difference behind in the quarantined copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedPlan {
    /// Non-empty seed state in hand, so a pre-SQLite database can be moved aside.
    Reseeded,
    /// Nothing to write. A pre-SQLite database is the only copy of that state.
    None,
}

impl SqlitePlannerRepository {
    pub async fn open(path: &Path, seed_plan: SeedPlan) -> Result<Self> {
        let path = path.to_path_buf();
        tokio::task::spawn_blocking(move || Self::open_blocking(&path, seed_plan))
            .await
            .context("joining planner db open task")?
    }

    fn open_blocking(path: &Path, seed_plan: SeedPlan) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating planner db directory {}", parent.display()))?;
        }

        // A pre-SQLite planner left a surrealkv *directory* at this path, and
        // under `planner.persistence.type=pvc` that path survives the upgrade.
        // Opening a directory as a SQLite file fails on every start, which is
        // the CrashLoopBackOff `init_schema` was hardened against, so the old
        // store cannot simply stay where it is.
        //
        // What it may be replaced with depends on the seed. With one
        // configured, the leader refills the projections seconds later and
        // moving the old store aside costs nothing. Without one, those rows are
        // the only copy: a service that quietly replaced them with empty tables
        // would come up ready and answer every request with a fallback plan,
        // which looks exactly like a cache with nothing worth prefetching.
        // Refuse instead, and say what to do about it.
        if path.is_dir() {
            if seed_plan == SeedPlan::None {
                anyhow::bail!(
                    "planner database {} is a pre-SQLite store and there is no non-empty seed \
                     state to rebuild it from, so replacing it would drop whatever it holds. \
                     Point --seed-state-file (planner.seedStateFile) at seed state that covers \
                     what this planner should serve, or point --db-path (planner.dbPath) at a \
                     new path — that starts from empty projections and every plan falls back to \
                     the client until something seeds it, but it leaves this store untouched.",
                    path.display()
                );
            }

            let quarantine = quarantine_legacy_planner_db(path)?;
            tracing::warn!(
                path = %path.display(),
                quarantine = %quarantine.display(),
                "moved a pre-SQLite planner database aside; the configured seed state file will repopulate the projections. The old store is kept, not deleted, and can be removed once the new database is seeded"
            );
        }

        let db = Connection::open(path)
            .with_context(|| format!("opening planner db at {}", path.display()))?;

        init_schema(&db)?;

        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }

    /// Run a closure against the connection, off the async executor.
    ///
    /// The point lookups here are microseconds against a local file, but
    /// seeding walks a whole state file while the server is already answering
    /// `/healthz`, so no query holds the runtime.
    async fn run<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            // A poisoned mutex is recovered rather than propagated: a panic in
            // one of these closures leaves no half-applied SQLite state (an
            // interrupted transaction rolls back when its guard drops), so
            // refusing every later query would turn one panic into a
            // permanently dead planner.
            let conn = db.lock().unwrap_or_else(|err| err.into_inner());
            f(&conn)
        })
        .await
        .context("joining planner db task")?
    }

    pub async fn seed_from_state_file(&self, path: &Path) -> Result<()> {
        self.seed_from_state(PlannerStateFile::read_from(path)?)
            .await
    }

    pub async fn seed_from_state(&self, state: PlannerStateFile) -> Result<()> {
        self.run(move |conn| {
            // One transaction for the whole seed: a partially applied state
            // file would serve plans from projections that never existed
            // together.
            let tx = conn.unchecked_transaction()?;

            for (namespace, namespace_state) in state.namespaces {
                for (dep_key, candidates) in namespace_state.deps {
                    for candidate in candidates {
                        upsert_namespace_artifact(&tx, &namespace, &dep_key, &candidate)?;
                        upsert_crate_artifact(&tx, &candidate.crate_name, &candidate)?;
                    }
                }
            }

            for (crate_name, candidates) in state.history {
                for candidate in candidates {
                    upsert_crate_artifact(&tx, &crate_name, &candidate)?;
                }
            }

            for (crate_name, cache_keys) in state.key_cache {
                for cache_key in cache_keys {
                    upsert_crate_artifact(
                        &tx,
                        &crate_name,
                        &PrefetchCandidate::new(cache_key, crate_name.clone()),
                    )?;
                }
            }

            tx.commit().context("committing planner seed")
        })
        .await
    }
}

/// Define the planner tables, tolerating a database that already has them.
///
/// This runs on every `open`, so it runs on every process start — and the
/// planner db lives on a persistent volume in production. A schema statement
/// that errors on the second start is not something the service recovers from:
/// it exits 1 and CrashLoopBackOffs forever while the volume keeps the state
/// that caused it. `IF NOT EXISTS` makes each statement a no-op when present.
fn init_schema(db: &Connection) -> Result<()> {
    db.pragma_update(None, "journal_mode", "WAL")
        .context("enabling WAL on the planner db")?;
    db.pragma_update(None, "synchronous", "NORMAL")
        .context("setting synchronous on the planner db")?;
    db.pragma_update(None, "busy_timeout", "5000")
        .context("setting busy_timeout on the planner db")?;

    // The key tuple *is* the primary key, so a namespace differing only by
    // `/` vs `_` cannot collapse into one row the way a derived record id can.
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS namespace_artifact (
            namespace    TEXT NOT NULL,
            dep_key      TEXT NOT NULL,
            cache_key    TEXT NOT NULL,
            crate_name   TEXT NOT NULL,
            last_seen_at INTEGER NOT NULL,
            PRIMARY KEY (namespace, dep_key, cache_key)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS crate_artifact (
            crate_name   TEXT NOT NULL,
            cache_key    TEXT NOT NULL,
            last_seen_at INTEGER NOT NULL,
            PRIMARY KEY (crate_name, cache_key)
        ) WITHOUT ROWID;",
    )
    .context("initializing planner db schema")?;

    Ok(())
}

fn upsert_namespace_artifact(
    db: &Connection,
    namespace: &str,
    dep_key: &str,
    candidate: &PrefetchCandidate,
) -> Result<()> {
    db.execute(
        "INSERT INTO namespace_artifact
             (namespace, dep_key, cache_key, crate_name, last_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (namespace, dep_key, cache_key) DO UPDATE SET
             crate_name   = excluded.crate_name,
             last_seen_at = excluded.last_seen_at",
        params![
            namespace,
            dep_key,
            candidate.cache_key,
            candidate.crate_name,
            now_nanos(),
        ],
    )
    .context("upserting namespace artifact projection")?;

    Ok(())
}

fn upsert_crate_artifact(
    db: &Connection,
    crate_name: &str,
    candidate: &PrefetchCandidate,
) -> Result<()> {
    db.execute(
        "INSERT INTO crate_artifact (crate_name, cache_key, last_seen_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT (crate_name, cache_key) DO UPDATE SET
             last_seen_at = excluded.last_seen_at",
        params![crate_name, candidate.cache_key, now_nanos()],
    )
    .context("upserting crate artifact projection")?;

    Ok(())
}

#[async_trait]
impl PlannerDataSource for SqlitePlannerRepository {
    async fn shard_candidates(
        &self,
        namespace: &str,
        deps: &[(String, String)],
    ) -> Result<Vec<PrefetchCandidate>> {
        let namespace = namespace.to_string();
        let dep_keys: Vec<String> = deps.iter().map(|(n, v)| dep_key(n, v)).collect();

        self.run(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT cache_key, crate_name
                     FROM namespace_artifact
                     WHERE namespace = ?1 AND dep_key = ?2
                     ORDER BY last_seen_at DESC, cache_key ASC",
                )
                .context("preparing namespace artifact query")?;

            let mut seen = HashSet::new();
            let mut candidates = Vec::new();

            for dep_key in &dep_keys {
                let rows = stmt
                    .query_map(params![&namespace, dep_key], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .context("querying namespace artifact projections")?;

                for row in rows {
                    let (cache_key, crate_name) =
                        row.context("decoding namespace artifact projection")?;
                    if seen.insert(cache_key.clone()) {
                        candidates.push(
                            PrefetchCandidate::new(cache_key, crate_name)
                                .with_source(CandidateSource::Shard),
                        );
                    }
                }
            }

            Ok(candidates)
        })
        .await
    }

    async fn history_candidates(&self, crate_names: &[String]) -> Result<Vec<PrefetchCandidate>> {
        let crate_names = crate_names.to_vec();

        self.run(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT cache_key, crate_name
                     FROM crate_artifact
                     WHERE crate_name = ?1
                     ORDER BY last_seen_at DESC, cache_key ASC",
                )
                .context("preparing crate artifact history query")?;

            let mut seen = HashSet::new();
            let mut candidates = Vec::new();

            for crate_name in &crate_names {
                let rows = stmt
                    .query_map(params![crate_name], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    })
                    .context("querying crate artifact history")?;

                for row in rows {
                    let (cache_key, row_crate_name) =
                        row.context("decoding crate artifact history")?;
                    if seen.insert(cache_key.clone()) {
                        candidates.push(
                            PrefetchCandidate::new(cache_key, row_crate_name)
                                .with_source(CandidateSource::History),
                        );
                    }
                }
            }

            Ok(candidates)
        })
        .await
    }

    async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>> {
        let crate_name = crate_name.to_string();

        self.run(move |conn| {
            let mut stmt = conn
                .prepare(
                    "SELECT cache_key
                     FROM crate_artifact
                     WHERE crate_name = ?1
                     ORDER BY last_seen_at DESC, cache_key ASC",
                )
                .context("preparing crate cache key query")?;

            let keys = stmt
                .query_map(params![crate_name], |row| row.get::<_, String>(0))
                .context("querying crate cache keys")?
                .collect::<rusqlite::Result<Vec<String>>>()
                .context("decoding crate cache keys")?;

            Ok(keys)
        })
        .await
    }

    async fn identity_candidates(&self, _identity_key: &str) -> Result<Vec<PrefetchCandidate>> {
        // Identity manifests live on the object store the daemon reads, not in
        // the planner's projections. An empty list lets shards/history fill the
        // plan; the daemon fallback still fetches the object.
        Ok(Vec::new())
    }
}

fn dep_key(name: &str, version: &str) -> String {
    format!("{name}@{version}")
}

/// Recency stamp for the projections, in nanoseconds since the epoch.
///
/// Nanoseconds rather than millis because seeding writes a whole state file in
/// a tight loop: at coarser resolution most rows would share a stamp and the
/// recency ordering would collapse. Queries still tie-break on `cache_key` so
/// the order is total even when two rows do land on the same instant.
fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Move a pre-SQLite planner database aside (to `<name>.surrealkv-<millis>`)
/// so a fresh one can be created in place. The old state is kept, not deleted:
/// it is an operator's data on a persistent volume, and the planner has no
/// business destroying it.
fn quarantine_legacy_planner_db(path: &Path) -> Result<PathBuf> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("planner.db");
    let quarantine = path.with_file_name(format!("{file_name}.surrealkv-{millis}"));

    std::fs::rename(path, &quarantine).with_context(|| {
        format!(
            "moving the pre-SQLite planner database {} aside to {}",
            path.display(),
            quarantine.display()
        )
    })?;

    Ok(quarantine)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Defining the schema against a db that already has it must succeed.
    ///
    /// Every other test here starts from an empty tempdir, so without this the
    /// suite would only ever exercise the first-ever start. Production does
    /// not: the planner db sits on a persistent volume, so the *second* start
    /// is the normal case. That gap once let a non-idempotent schema statement
    /// reach production, where the planner exited 1 on boot and
    /// CrashLoopBackOffed ~3700 times over 13 days without becoming ready.
    #[tokio::test]
    async fn init_schema_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("planner.db");

        // `open` defines the schema once.
        let repo = SqlitePlannerRepository::open(&db_path, SeedPlan::None)
            .await
            .unwrap();
        drop(repo);

        // A restarting pod runs `open` again against exactly this state.
        SqlitePlannerRepository::open(&db_path, SeedPlan::None)
            .await
            .expect("reopening a database that already has the schema must succeed");
    }

    /// surrealkv stored the planner as a directory, not a file.
    fn write_legacy_planner_dir(db_path: &Path) {
        std::fs::create_dir(db_path).unwrap();
        std::fs::write(db_path.join("LOCK"), b"legacy sentinel").unwrap();
    }

    /// With a seed configured, a surrealkv directory is moved aside, not deleted.
    #[tokio::test]
    async fn open_quarantines_a_legacy_planner_directory_when_a_seed_follows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("planner.db");
        write_legacy_planner_dir(&db_path);

        let repo = SqlitePlannerRepository::open(&db_path, SeedPlan::Reseeded)
            .await
            .unwrap();

        assert!(db_path.is_file(), "the new planner db must be a file");
        assert!(
            repo.key_cache_keys_for_crate("anything")
                .await
                .unwrap()
                .is_empty()
        );

        let quarantined: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".surrealkv-"))
            .collect();
        assert_eq!(
            quarantined.len(),
            1,
            "the old directory must be kept, not deleted: {quarantined:?}"
        );
    }

    /// With no seed configured, that same directory is the only copy of the
    /// projections. Replacing it with empty tables would leave the service
    /// ready and answering every request with a fallback plan, which is
    /// indistinguishable from a planner that simply has nothing to offer. Refuse
    /// instead, and leave the old store untouched for every retry.
    #[tokio::test]
    async fn open_refuses_a_legacy_planner_directory_when_nothing_will_reseed_it() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("planner.db");
        write_legacy_planner_dir(&db_path);

        // A CrashLoopBackOff retries; every attempt must be equally harmless.
        for _ in 0..3 {
            let err = SqlitePlannerRepository::open(&db_path, SeedPlan::None)
                .await
                .expect_err("a legacy store with no seed to rebuild it must not be replaced");
            assert!(
                err.to_string().contains("--seed-state-file"),
                "the error must say how to resolve it: {err}"
            );

            assert!(db_path.is_dir(), "the legacy store must stay in place");
            assert_eq!(
                std::fs::read(db_path.join("LOCK")).unwrap(),
                b"legacy sentinel"
            );
            assert_eq!(
                std::fs::read_dir(dir.path()).unwrap().count(),
                1,
                "refusing must not leave quarantine copies behind"
            );
        }
    }

    #[test]
    fn dep_key_joins_name_and_version() {
        assert_eq!(dep_key("serde", "1.0.0"), "serde@1.0.0");
        assert_eq!(dep_key("", ""), "@");
    }

    /// Namespaces differing only by `/` vs `_` must stay separate rows.
    ///
    /// v0.16 derived a record id by replacing punctuation, so `linux/hash/debug`
    /// and `linux_hash_debug` collapsed onto one row and each upsert clobbered
    /// the other's `crate_name`. The key tuple is now the primary key, so the
    /// collision is unrepresentable — this test holds the guarantee in place.
    #[tokio::test]
    async fn namespace_upsert_keeps_punctuation_variant_namespaces_apart() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
            .await
            .unwrap();

        repo.seed_from_state(PlannerStateFile {
            namespaces: HashMap::from([
                (
                    "linux/hash/debug".to_string(),
                    NamespaceState {
                        deps: HashMap::from([(
                            "serde@1.0.0".to_string(),
                            vec![PrefetchCandidate::new(
                                "shared-key".to_string(),
                                "slash-value".to_string(),
                            )],
                        )]),
                    },
                ),
                (
                    "linux_hash_debug".to_string(),
                    NamespaceState {
                        deps: HashMap::from([(
                            "serde@1.0.0".to_string(),
                            vec![PrefetchCandidate::new(
                                "shared-key".to_string(),
                                "underscore-value".to_string(),
                            )],
                        )]),
                    },
                ),
            ]),
            history: HashMap::new(),
            key_cache: HashMap::new(),
        })
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

    /// The crate projection has the same punctuation guarantee.
    #[tokio::test]
    async fn crate_upsert_keeps_punctuation_variant_crates_apart() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
            .await
            .unwrap();

        repo.seed_from_state(PlannerStateFile {
            namespaces: HashMap::new(),
            history: HashMap::from([
                (
                    "serde/json".to_string(),
                    vec![PrefetchCandidate::new(
                        "shared-key".to_string(),
                        "serde/json".to_string(),
                    )],
                ),
                (
                    "serde_json".to_string(),
                    vec![PrefetchCandidate::new(
                        "shared-key".to_string(),
                        "serde_json".to_string(),
                    )],
                ),
            ]),
            key_cache: HashMap::new(),
        })
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

    /// Re-seeding the same tuple updates it instead of erroring or duplicating.
    #[tokio::test]
    async fn seeding_twice_updates_in_place() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
            .await
            .unwrap();

        let state = |crate_name: &str| PlannerStateFile {
            namespaces: HashMap::from([(
                "ns".to_string(),
                NamespaceState {
                    deps: HashMap::from([(
                        "serde@1.0.0".to_string(),
                        vec![PrefetchCandidate::new(
                            "shared-key".to_string(),
                            crate_name.to_string(),
                        )],
                    )]),
                },
            )]),
            history: HashMap::new(),
            key_cache: HashMap::new(),
        };

        repo.seed_from_state(state("first")).await.unwrap();
        repo.seed_from_state(state("second")).await.unwrap();

        let candidates = repo
            .shard_candidates("ns", &[("serde".to_string(), "1.0.0".to_string())])
            .await
            .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].crate_name, "second");
    }

    #[tokio::test]
    async fn shard_candidates_dedupes_repeated_cache_keys_across_deps() {
        // The same cache_key seen under two different deps must be returned
        // once — the planner's `seen` set guards against duplicate prefetch.
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
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
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
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
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
            .await
            .unwrap();
        let err = repo
            .seed_from_state_file(&dir.path().join("does-not-exist.json"))
            .await;
        assert!(err.is_err());
    }

    /// A seed file that does not parse is rejected before any SQL runs.
    ///
    /// This does NOT exercise the seed transaction — parsing fails in
    /// `seed_from_state_file` before `seed_from_state` opens one.
    /// `seed_rolls_back_when_a_write_fails_partway` covers that.
    #[tokio::test]
    async fn seed_from_state_file_rejects_malformed_json_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
            .await
            .unwrap();

        let seed_path = dir.path().join("planner-state.json");
        std::fs::write(&seed_path, b"{ not json").unwrap();

        assert!(repo.seed_from_state_file(&seed_path).await.is_err());
        assert!(
            repo.history_candidates(&["serde".to_string()])
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A seed that fails after an earlier write must undo that write, and must
    /// not touch rows an earlier successful seed committed.
    ///
    /// Seeding a namespace writes both projections per candidate, so dropping
    /// `crate_artifact` makes the namespace insert succeed and the very next
    /// statement fail — a genuine mid-transaction failure. Without the enclosing
    /// transaction the new namespace row would survive and the planner would
    /// serve half a state file. The pre-existing row makes the difference
    /// between rolling back and simply wiping the table visible; an
    /// implementation that "cleaned up" by deleting everything on error would
    /// pass against an empty fixture.
    ///
    /// This proves rollback of the failing seed, not that every partial-seed
    /// shape rolls back: one transaction per candidate would also pass here.
    #[tokio::test]
    async fn seed_rolls_back_when_a_write_fails_partway() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
            .await
            .unwrap();

        let namespace_seed = |dep: &str, cache_key: &str| PlannerStateFile {
            namespaces: HashMap::from([(
                "ns".to_string(),
                NamespaceState {
                    deps: HashMap::from([(
                        dep.to_string(),
                        vec![PrefetchCandidate::new(
                            cache_key.to_string(),
                            "serde".to_string(),
                        )],
                    )]),
                },
            )]),
            history: HashMap::new(),
            key_cache: HashMap::new(),
        };

        // Committed state the failing seed must leave alone.
        repo.seed_from_state(namespace_seed("committed@1.0.0", "committed-key"))
            .await
            .unwrap();

        repo.run(|conn| {
            conn.execute_batch("DROP TABLE crate_artifact;")?;
            Ok(())
        })
        .await
        .unwrap();

        let err = repo
            .seed_from_state(namespace_seed("serde@1.0.0", "serde-key"))
            .await;
        assert!(err.is_err(), "the seed must fail once a write fails");

        let survivors = repo
            .shard_candidates("ns", &[("committed".to_string(), "1.0.0".to_string())])
            .await
            .unwrap();
        assert_eq!(
            survivors.iter().map(|c| &c.cache_key).collect::<Vec<_>>(),
            ["committed-key"],
            "the failed seed must not disturb already committed rows"
        );

        let rows: i64 = repo
            .run(|conn| {
                Ok(conn.query_row(
                    "SELECT COUNT(*) FROM namespace_artifact WHERE cache_key = ?1",
                    ["serde-key"],
                    |row| row.get(0),
                )?)
            })
            .await
            .unwrap();
        assert_eq!(rows, 0, "the successful write must have rolled back");
    }

    /// Reads return the most recently seen cache key first.
    ///
    /// The planner's caller truncates to the FIRST entries per crate, so a
    /// reversed order would silently hand it the stalest keys.
    #[tokio::test]
    async fn reads_return_the_most_recently_seeded_key_first() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
            .await
            .unwrap();

        let seed = |cache_key: &str| PlannerStateFile {
            namespaces: HashMap::from([(
                "ns".to_string(),
                NamespaceState {
                    deps: HashMap::from([(
                        "serde@1.0.0".to_string(),
                        vec![PrefetchCandidate::new(
                            cache_key.to_string(),
                            "serde".to_string(),
                        )],
                    )]),
                },
            )]),
            history: HashMap::new(),
            key_cache: HashMap::new(),
        };

        let order = async |repo: &SqlitePlannerRepository| {
            let shard: Vec<String> = repo
                .shard_candidates("ns", &[("serde".to_string(), "1.0.0".to_string())])
                .await
                .unwrap()
                .into_iter()
                .map(|c| c.cache_key)
                .collect();
            let history: Vec<String> = repo
                .history_candidates(&["serde".to_string()])
                .await
                .unwrap()
                .into_iter()
                .map(|c| c.cache_key)
                .collect();
            let keys = repo.key_cache_keys_for_crate("serde").await.unwrap();
            (shard, history, keys)
        };

        // Seeded oldest-first, and named so the NEWER key sorts LAST
        // alphabetically: ordering by `cache_key` alone, or stamping every row
        // identically and falling through to the tie-break, gives the reverse
        // of what these assertions demand.
        repo.seed_from_state(seed("aaa-older")).await.unwrap();
        repo.seed_from_state(seed("zzz-newer")).await.unwrap();

        let expected = ["zzz-newer".to_string(), "aaa-older".to_string()];
        let (shard, history, keys) = order(&repo).await;
        assert_eq!(shard, expected);
        assert_eq!(history, expected);
        assert_eq!(keys, expected);

        // Re-seeding an existing tuple must refresh its recency, not just leave
        // the row alone. Rewriting the stamps to known values first makes that
        // observable without depending on clock resolution — and keeps the
        // ordering they already imply, so an upsert that never refreshes
        // `last_seen_at` leaves the assertions below reading the old order
        // rather than falling into the `cache_key` tie-break that would happen
        // to match.
        repo.run(|conn| {
            conn.execute_batch(
                "UPDATE namespace_artifact SET last_seen_at = 1 WHERE cache_key = 'aaa-older';
                 UPDATE namespace_artifact SET last_seen_at = 2 WHERE cache_key = 'zzz-newer';
                 UPDATE crate_artifact SET last_seen_at = 1 WHERE cache_key = 'aaa-older';
                 UPDATE crate_artifact SET last_seen_at = 2 WHERE cache_key = 'zzz-newer';",
            )?;
            Ok(())
        })
        .await
        .unwrap();
        repo.seed_from_state(seed("aaa-older")).await.unwrap();

        let refreshed = ["aaa-older".to_string(), "zzz-newer".to_string()];
        let (shard, history, keys) = order(&repo).await;
        assert_eq!(shard, refreshed, "the re-seeded key must sort first again");
        assert_eq!(history, refreshed);
        assert_eq!(keys, refreshed);
    }

    #[tokio::test]
    async fn repository_resolves_namespace_candidates_from_seed_state() {
        let dir = tempfile::tempdir().unwrap();
        let repo = SqlitePlannerRepository::open(&dir.path().join("planner.db"), SeedPlan::None)
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

        let repo = SqlitePlannerRepository::open(&db_path, SeedPlan::None)
            .await
            .unwrap();
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

    /// State survives a process restart against the same file.
    #[tokio::test]
    async fn seeded_state_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("planner.db");

        let repo = SqlitePlannerRepository::open(&db_path, SeedPlan::None)
            .await
            .unwrap();
        repo.seed_from_state(PlannerStateFile {
            namespaces: HashMap::new(),
            history: HashMap::from([(
                "serde".to_string(),
                vec![PrefetchCandidate::new(
                    "serde-key".to_string(),
                    "serde".to_string(),
                )],
            )]),
            key_cache: HashMap::new(),
        })
        .await
        .unwrap();
        drop(repo);

        let reopened = SqlitePlannerRepository::open(&db_path, SeedPlan::None)
            .await
            .unwrap();
        let history = reopened
            .history_candidates(&["serde".to_string()])
            .await
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].cache_key, "serde-key");
    }
}
