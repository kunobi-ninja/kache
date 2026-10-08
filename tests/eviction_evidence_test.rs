//! The report must read recorded evidence after the last WAL owner closes.
mod common;

fn report(cache: &std::path::Path) -> serde_json::Value {
    let output = common::hermetic_command(
        common::kache_binary(),
        cache,
        Some(&common::isolated_config_path(cache)),
    )
    .args(["report", "--format", "json", "--since", "1h"])
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn daemon_free_report_reads_mature_evidence_after_last_wal_owner_closes() {
    common::build_kache();
    let cache = tempfile::tempdir().unwrap();
    report(cache.path());
    let index = cache.path().join("index.db");
    let db = rusqlite::Connection::open(&index).unwrap();
    db.execute("INSERT INTO eviction_tombstones(cache_key,evicted_at,policy,size,hit_count,idle_hours,compile_time_ms,demanded_at,shadow_policy,shadow_would_evict) VALUES ('old',datetime('now','-8 days'),'lru',1073741824,0,0,900,datetime('now','-7 days'),'value-density',1)",[]).unwrap();
    drop(db);
    assert!(
        !cache.path().join("index.db-wal").exists(),
        "fixture must close the last WAL owner"
    );
    let json = report(cache.path());
    let evidence = &json["eviction_evidence"];
    assert_eq!(evidence["horizon_secs"], 604800);
    assert_eq!(json["meta"]["since_secs"], 3600);
    let store = &evidence["stores"][0];
    assert!(store["error"].is_null(), "{store}");
    let cohort = &store["evidence"]["live_shadow_agreed"];
    assert_eq!(cohort["mature"], 1);
    assert_eq!(cohort["demand_within_horizon_lower"], 1);
    assert_eq!(cohort["known_cost_logical_bytes"], 1073741824);
    assert_eq!(cohort["demanded_gross_compile_cost_ms_lower"], 900);
    let db = rusqlite::Connection::open(index).unwrap();
    assert_eq!(
        db.query_row("SELECT count(*) FROM eviction_tombstones", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}
