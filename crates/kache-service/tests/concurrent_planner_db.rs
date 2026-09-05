//! Two OS processes seeding one planner database.
//!
//! Nothing in the service arbitrates between them. Until 0.16 the store was
//! surrealkv, which took an exclusive LOCK, so a second process crashed with
//! "planner.db/LOCK is already locked by another process" — loud, and the
//! reason the chart's `replicaCount` guard and `Recreate` strategy cite. SQLite
//! in WAL mode does not refuse a second opener, so that noise is gone and only
//! the chart keeps a surging rollout from producing two writers.
//!
//! That makes what SQLite does under two writers worth pinning down: contending
//! writes must serialize on `busy_timeout` and every one of them must land,
//! rather than one process losing its rows or leaving a half-written database
//! the next start cannot read. Separate processes, not two connections in one:
//! POSIX advisory locks are owned per process, so a single-process stand-in
//! would exercise different locking than two Pods on a node do.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;

use kache_core::{PlannerDataSource, PrefetchCandidate};
use kache_service::{NamespaceState, PlannerStateFile, SeedPlan, SqlitePlannerRepository};

/// Set on a re-executed copy of this test binary to make it a writer.
const WORKER_DB_PATH: &str = "KACHE_TEST_PLANNER_DB_PATH";
const WORKER_ID: &str = "KACHE_TEST_PLANNER_WORKER";

const WRITERS: usize = 4;
const KEYS_PER_WRITER: usize = 25;

fn crate_name(worker: usize, index: usize) -> String {
    format!("crate-{worker}-{index}")
}

fn cache_key(worker: usize, index: usize) -> String {
    format!("key-{worker}-{index}")
}

fn seed_state(worker: usize) -> PlannerStateFile {
    let mut namespaces = std::collections::HashMap::new();
    let mut deps = std::collections::HashMap::new();
    let mut history = std::collections::HashMap::new();

    for index in 0..KEYS_PER_WRITER {
        let candidate = PrefetchCandidate::new(cache_key(worker, index), crate_name(worker, index));
        deps.insert(
            format!("dep-{worker}-{index}@1.0.0"),
            vec![candidate.clone()],
        );
        history.insert(crate_name(worker, index), vec![candidate]);
    }

    namespaces.insert("shared".to_string(), NamespaceState { deps });

    PlannerStateFile {
        namespaces,
        history,
        key_cache: std::collections::HashMap::new(),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build worker runtime")
}

/// The writer half. Inert unless this process was re-executed as a worker, so
/// a normal `cargo test` run just skips it.
#[test]
fn planner_db_writer_worker() {
    let (Ok(db_path), Ok(worker)) = (std::env::var(WORKER_DB_PATH), std::env::var(WORKER_ID))
    else {
        return;
    };
    let worker: usize = worker.parse().expect("worker id");

    runtime().block_on(async move {
        let repo = SqlitePlannerRepository::open(Path::new(&db_path), SeedPlan::None)
            .await
            .expect("a second process must be able to open the planner db");
        repo.seed_from_state(seed_state(worker))
            .await
            .expect("seeding must survive contention with the other writers");
    });
}

fn spawn_worker(exe: &Path, db_path: &Path, worker: usize) -> std::process::Child {
    Command::new(exe)
        .args(["--exact", "planner_db_writer_worker", "--nocapture"])
        .env(WORKER_DB_PATH, db_path)
        .env(WORKER_ID, worker.to_string())
        .spawn()
        .expect("spawn planner db writer")
}

#[test]
fn concurrent_processes_all_land_their_rows_in_one_planner_db() {
    let exe = std::env::current_exe().expect("locate this test binary");
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path: PathBuf = dir.path().join("planner.db");

    // Create the schema up front so the writers contend on writes rather than
    // racing to define the same tables.
    runtime().block_on(async {
        SqlitePlannerRepository::open(&db_path, SeedPlan::None)
            .await
            .expect("create the planner db");
    });

    let children: Vec<_> = (0..WRITERS)
        .map(|worker| spawn_worker(&exe, &db_path, worker))
        .collect();

    for (worker, mut child) in children.into_iter().enumerate() {
        let status = child.wait().expect("await planner db writer");
        assert!(
            status.success(),
            "writer {worker} failed against a shared planner db: {status}"
        );
    }

    // Every writer's rows must be readable afterwards, through a fresh open —
    // a database left inconsistent by the contention would surface here.
    runtime().block_on(async {
        let repo = SqlitePlannerRepository::open(&db_path, SeedPlan::None)
            .await
            .expect("reopen the planner db the writers shared");

        let deps: Vec<(String, String)> = (0..WRITERS)
            .flat_map(|worker| {
                (0..KEYS_PER_WRITER)
                    .map(move |index| (format!("dep-{worker}-{index}"), "1.0.0".to_string()))
            })
            .collect();

        let found: HashSet<String> = repo
            .shard_candidates("shared", &deps)
            .await
            .expect("read back the shard projection")
            .into_iter()
            .map(|candidate| candidate.cache_key)
            .collect();

        let expected: HashSet<String> = (0..WRITERS)
            .flat_map(|worker| (0..KEYS_PER_WRITER).map(move |index| cache_key(worker, index)))
            .collect();

        let missing: Vec<_> = expected.difference(&found).collect();
        assert!(
            missing.is_empty(),
            "{} of {} rows were lost to write contention: {missing:?}",
            missing.len(),
            expected.len()
        );

        // The crate projection is written by the same seeds, so it must agree.
        for worker in 0..WRITERS {
            for index in 0..KEYS_PER_WRITER {
                assert_eq!(
                    repo.key_cache_keys_for_crate(&crate_name(worker, index))
                        .await
                        .expect("read back the crate projection"),
                    [cache_key(worker, index)],
                );
            }
        }
    });
}
