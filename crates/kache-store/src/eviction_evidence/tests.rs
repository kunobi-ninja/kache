use super::*;
use rusqlite::params;

fn database() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE eviction_tombstones(cache_key TEXT PRIMARY KEY, evicted_at TEXT, demanded_at TEXT, shadow_policy TEXT, shadow_would_evict INTEGER, size INTEGER, compile_time_ms INTEGER, hit_count INTEGER); CREATE TABLE shadow_victims(cache_key TEXT PRIMARY KEY, swept_at TEXT, shadow_policy TEXT, size INTEGER, compile_time_ms INTEGER, hit_count INTEGER); CREATE TABLE entries(cache_key TEXT PRIMARY KEY, last_accessed TEXT, hit_count INTEGER);").unwrap();
    db
}
fn live(
    db: &Connection,
    key: &str,
    start: i64,
    demand: Option<i64>,
    agreed: bool,
    size: i64,
    cost: i64,
) {
    db.execute("INSERT OR REPLACE INTO eviction_tombstones VALUES (?1, datetime(?2, 'unixepoch'), datetime(?3, 'unixepoch'), 'value-density', ?4, ?5, ?6, 0)", params![key, start, demand, agreed, size, cost]).unwrap();
}
fn shadow(db: &Connection, key: &str, start: i64, hits: i64) {
    db.execute("INSERT OR IGNORE INTO shadow_victims VALUES (?1, datetime(?2, 'unixepoch'), 'value-density', 100, 500, ?3)", params![key,start,hits]).unwrap();
}
fn hit(db: &Connection, key: &str, stamp: i64, hits: i64) {
    db.execute(
        "INSERT INTO entries VALUES (?1, datetime(?2, 'unixepoch'), ?3)",
        params![key, stamp, hits],
    )
    .unwrap();
}

#[test]
fn maturity_and_first_demand_boundaries_are_cost_weighted() {
    let db = database();
    live(&db, "expensive", 100, Some(200), true, 1 << 29, 900);
    live(&db, "cheap", 100, Some(201), true, 1 << 29, 100);
    live(&db, "young", 101, Some(150), true, 123, 8000);
    live(&db, "unknown", 100, Some(150), true, 9000, 0);
    live(&db, "kept", 100, None, false, 100, 700);
    let report = read(&db, 200, 100).unwrap();
    assert_eq!(report.as_of_unix_secs, 200);
    assert_eq!(report.horizon_secs, 100);
    // The later demand is beyond as_of, so move as_of forward and make
    // the immature observation young relative to that same snapshot.
    assert_eq!(report.live_shadow_agreed.invalid, 1);
    live(&db, "young", 102, Some(150), true, 123, 8000);
    let report = read(&db, 201, 100).unwrap();
    let c = report.live_shadow_agreed;
    assert_eq!(
        (c.observations, c.mature, c.immature, c.invalid),
        (4, 3, 1, 0)
    );
    assert_eq!(
        (c.demand_within_horizon_lower, c.demand_within_horizon_upper),
        (2, 2)
    );
    assert_eq!(c.unknown_compile_cost, 1);
    assert_eq!(c.known_cost_logical_bytes, 1_073_741_824);
    assert_eq!(c.gross_compile_cost_ms, 1000);
    assert_eq!(c.demanded_gross_compile_cost_ms_lower, 900);
    assert_eq!(c.demanded_gross_compile_cost_ms_upper, 900);
    assert_eq!(c.demanded_cost_ms_per_logical_gib(), Some((900.0, 900.0)));
    assert_eq!(report.live_shadow_kept.mature, 1);
    assert_eq!(report.live_shadow_kept.demand_within_horizon_upper, 0);
    assert_eq!(report.live_shadow_kept.gross_compile_cost_ms, 700);
}

#[test]
fn shadow_demand_bounds_do_not_infer_absence_from_throttled_or_late_stamps() {
    let db = database();
    for key in [
        "within",
        "late",
        "unchanged",
        "reset",
        "evicted",
        "evicted-late",
        "demanded",
    ] {
        shadow(&db, key, 100, 5);
    }
    hit(&db, "within", 200, 6);
    hit(&db, "late", 201, 6);
    hit(&db, "unchanged", 150, 5);
    hit(&db, "reset", 150, 1);
    live(&db, "evicted", 200, None, true, 100, 500);
    live(&db, "evicted-late", 201, None, true, 100, 500);
    live(&db, "demanded", 180, Some(200), false, 100, 500);
    db.execute(
        "UPDATE eviction_tombstones SET hit_count=6 WHERE cache_key LIKE 'evicted%'",
        [],
    )
    .unwrap();
    let c = read(&db, 300, 100).unwrap().shadow_only;
    assert_eq!(c.mature, 7);
    assert_eq!(
        (c.demand_within_horizon_lower, c.demand_within_horizon_upper),
        (3, 7)
    );
    assert_eq!(
        (
            c.demanded_gross_compile_cost_ms_lower,
            c.demanded_gross_compile_cost_ms_upper
        ),
        (1500, 3500)
    );
}

#[test]
fn repeated_evictions_use_latest_live_and_first_shadow_observations() {
    let db = database();
    live(&db, "same", 100, Some(150), true, 100, 700);
    shadow(&db, "same", 100, 5);
    shadow(&db, "same", 250, 0);
    live(&db, "same", 250, None, false, 100, 900);
    let r = read(&db, 300, 100).unwrap();
    assert_eq!(r.live_shadow_agreed.observations, 0);
    assert_eq!(
        (r.live_shadow_kept.observations, r.live_shadow_kept.immature),
        (1, 1)
    );
    assert_eq!(r.live_shadow_kept.gross_compile_cost_ms, 0);
    assert_eq!((r.shadow_only.observations, r.shadow_only.mature), (1, 1));
    assert_eq!(r.shadow_only.gross_compile_cost_ms, 500);
    assert_eq!(r.shadow_only.demand_within_horizon_lower, 0);
}

#[test]
fn malformed_future_and_pre_observation_demands_are_separate_from_censoring() {
    let db = database();
    live(&db, "future", 301, None, true, 1, 1);
    live(&db, "before", 100, Some(99), true, 1, 1);
    live(&db, "bad-demand", 100, None, true, 1, 1);
    live(&db, "bad-start", 100, None, true, 1, 1);
    db.execute(
        "UPDATE eviction_tombstones SET demanded_at='broken' WHERE cache_key='bad-demand'",
        [],
    )
    .unwrap();
    db.execute(
        "UPDATE eviction_tombstones SET evicted_at='broken' WHERE cache_key='bad-start'",
        [],
    )
    .unwrap();
    live(&db, "not-shadowed", 100, None, true, 1, 1);
    db.execute(
        "UPDATE eviction_tombstones SET shadow_policy=NULL WHERE cache_key='not-shadowed'",
        [],
    )
    .unwrap();
    let c = read(&db, 300, 100).unwrap().live_shadow_agreed;
    assert_eq!(
        (c.observations, c.invalid, c.mature, c.immature),
        (4, 4, 0, 0)
    );
}

#[test]
fn unknown_cost_and_shared_logical_bytes_are_not_reclaimed_byte_estimates() {
    let db = database();
    // Two entries may reference the same blob: each logical size stays in
    // the denominator; this deliberately makes no physical-reclaim estimate.
    live(&db, "shared-a", 100, Some(150), true, 1 << 29, 1000);
    live(&db, "shared-b", 100, None, true, 1 << 29, 1000);
    live(&db, "unknown", 100, Some(150), true, 1 << 30, -1);
    let c = read(&db, 300, 100).unwrap().live_shadow_agreed;
    assert_eq!(c.known_cost_logical_bytes, 1_073_741_824);
    assert_eq!(c.unknown_compile_cost, 1);
    assert_eq!(c.demand_within_horizon_lower, 2);
    assert_eq!(c.demanded_cost_ms_per_logical_gib(), Some((1000.0, 1000.0)));
    let empty = EvictionEvidenceCohort::default();
    assert_eq!(empty.demanded_cost_ms_per_logical_gib(), None);
    live(&db, "negative-size", 100, Some(150), false, -1, 100);
    assert_eq!(
        read(&db, 300, 100)
            .unwrap()
            .live_shadow_kept
            .known_cost_logical_bytes,
        0
    );
}

#[test]
fn evidence_query_is_read_only_and_rejects_invalid_horizons() {
    let db = database();
    live(&db, "same", 100, None, true, 100, 100);
    let changes = db.total_changes();
    db.execute_batch("PRAGMA query_only=ON").unwrap();
    assert_eq!(read(&db, 300, 100).unwrap().live_shadow_agreed.mature, 1);
    assert_eq!(db.total_changes(), changes);
    assert!(
        read(&db, 300, 0)
            .unwrap_err()
            .to_string()
            .contains("positive")
    );
    assert!(
        read(&db, 300, u64::MAX)
            .unwrap_err()
            .to_string()
            .contains("large")
    );
    assert_eq!(
        read(&db, i64::MAX, i64::MAX as u64)
            .unwrap()
            .live_shadow_agreed
            .immature,
        1
    );
}
