use super::*;

#[test]
fn horizon_is_independent_of_event_window_and_reads_production_schema() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    let db = rusqlite::Connection::open(config.index_db_path()).unwrap();
    db.execute("INSERT INTO eviction_tombstones(cache_key,evicted_at,policy,size,hit_count,idle_hours,compile_time_ms,demanded_at,shadow_policy,shadow_would_evict) VALUES ('old',datetime(100,'unixepoch'),'lru',1073741824,0,0,900,datetime(604900,'unixepoch'),'value-density',1)",[]).unwrap();
    let inventory = vec![StoreSummary {
        path: dir.path().to_path_buf(),
        ..Default::default()
    }];
    let evidence = collect(&config, &inventory, 604_900);
    assert_eq!(evidence.horizon_secs, 604_800);
    let row = evidence.stores[0].evidence.as_ref().unwrap();
    assert_eq!(row.live_shadow_agreed.mature, 1);
    assert_eq!(
        row.live_shadow_agreed.demanded_gross_compile_cost_ms_lower,
        900
    );
    assert!(evidence.stores[0].error.is_none());
    let text = lines(&evidence).join("\n");
    assert!(text.contains("900.0–900.0ms/logical GiB"));
    assert!(text.contains("independent of the build-event window"));
    assert!(text.contains("unknown cost 0"));
    drop(db);
    drop(store);
}

#[test]
fn missing_index_is_reported_without_creation_or_path_leaks() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("private-missing");
    let config = crate::test_support::test_config(missing.clone());
    let inventory = vec![StoreSummary {
        path: missing.clone(),
        ..Default::default()
    }];
    let evidence = collect(&config, &inventory, 900_000);
    assert!(!missing.exists());
    assert!(evidence.stores[0].evidence.is_none());
    assert_eq!(
        evidence.stores[0].error.as_deref(),
        Some("read-only index unavailable")
    );
    let json = serde_json::to_string(&evidence).unwrap();
    assert!(!json.contains("private-missing"));
    assert!(
        lines(&evidence)
            .join("\n")
            .contains("Store 0: read-only index unavailable")
    );
}

#[test]
fn json_and_full_formats_include_limits_and_old_reports_deserialize() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    let report =
        super::super::generate_report(&config, crate::since::SinceWindow::DEFAULT, 10).unwrap();
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["eviction_evidence"]["horizon_secs"], 604800);
    let limits = json["eviction_evidence"]["limitations"].as_array().unwrap();
    assert!(
        limits
            .iter()
            .any(|s| s.as_str().unwrap().contains("gross measured compile time"))
    );
    assert!(
        limits
            .iter()
            .any(|s| s.as_str().unwrap().contains("shared blobs"))
    );
    assert!(super::super::format_text(&report).contains("Eviction observations"));
    assert!(super::super::format_markdown(&report).contains("Immature and invalid"));
    let mut old = json;
    old.as_object_mut().unwrap().remove("eviction_evidence");
    let old: super::super::BuildReport = serde_json::from_value(old).unwrap();
    assert!(old.eviction_evidence.is_none());
}
