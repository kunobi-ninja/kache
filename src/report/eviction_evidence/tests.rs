use super::*;
use crate::store::Store;

#[test]
fn horizon_is_independent_of_event_window_and_reads_production_schema() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    let store = Store::open(&config).unwrap();
    let db = rusqlite::Connection::open(config.index_db_path()).unwrap();
    db.execute("INSERT INTO eviction_tombstones(cache_key,evicted_at,policy,size,hit_count,idle_hours,compile_time_ms,demanded_at,shadow_policy,shadow_would_evict) VALUES ('old',datetime(100,'unixepoch'),'lru',1073741824,0,0,900,datetime(604900,'unixepoch'),'value-density',1)",[]).unwrap();
    drop(db);
    drop(store);
    // Reopen for report after the last WAL owner has closed.
    let inventory = crate::store_view::read_report(&config, 604_900, HORIZON_SECS).unwrap();
    let evidence = collect(inventory.eviction_evidence, 604_900);
    assert_eq!(evidence.horizon_secs, 604_800);
    assert!(
        crate::store_view::read(&config, false, "name")
            .unwrap()
            .eviction_evidence
            .is_empty()
    );
    let row = evidence.stores[0].evidence.as_ref().unwrap();
    assert_eq!(row.live_shadow_agreed.mature, 1);
    assert_eq!(
        row.live_shadow_agreed.demanded_gross_compile_cost_ms_lower,
        900
    );
    assert!(evidence.stores[0].error.is_none());
    let text = lines(&evidence).join("\n");
    assert!(
        text.contains("Eviction observations · 7d horizon"),
        "{text}"
    );
    assert!(text.contains("both rankings would delete: 1 judged, 0 recent"));
    assert!(text.contains("Compiled again: 1, 900 ms (900.0 ms/logical GiB)"));
    assert!(text.contains("independent of the build-event window"));
    assert!(
        !text.contains("unknown cost 0"),
        "a zero unknown cost is not a row: {text}"
    );
}

#[test]
fn missing_index_is_reported_without_creation_or_path_leaks() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("private-missing");
    let mut config = crate::test_support::test_config(dir.path().join("main"));
    config.volume_stores.push(crate::config::VolumeStore {
        volume: dir.path().to_string_lossy().into_owned(),
        store: missing.clone(),
        max_size: Some(1 << 30),
    });
    let inventory = crate::store_view::read_report(&config, 900_000, HORIZON_SECS).unwrap();
    let evidence = collect(inventory.eviction_evidence, 900_000);
    assert!(!missing.exists());
    assert!(evidence.stores[1].evidence.is_none());
    assert_eq!(
        evidence.stores[1].error.as_deref(),
        Some("index unavailable")
    );
    let json = serde_json::to_string(&evidence).unwrap();
    assert!(!json.contains("private-missing"));
    assert!(json.contains("index unavailable"));
    // No store observed an eviction, so the text block stays out.
    assert!(lines(&evidence).is_empty());
    let mut observed = EvictionHorizonEvidence::default();
    observed.shadow_only.observations = 1;
    let mut evidence = evidence;
    evidence.stores[0].evidence = Some(observed);
    assert!(
        lines(&evidence)
            .join("\n")
            .contains("Store 1: index unavailable")
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
    // An empty store has nothing to show in the human formats.
    assert!(!super::super::format_text(&report).contains("Eviction observations"));
    assert!(!super::super::format_markdown(&report).contains("Immature and invalid"));
    let mut old = json;
    old.as_object_mut().unwrap().remove("eviction_evidence");
    let old: super::super::BuildReport = serde_json::from_value(old).unwrap();
    assert!(old.eviction_evidence.is_none());
}

#[test]
fn distinct_cohorts_and_missing_cost_remain_visible() {
    let mut evidence = EvictionHorizonEvidence::default();
    evidence.live_shadow_agreed.observations = 3;
    evidence.live_shadow_agreed.mature = 2;
    evidence.live_shadow_agreed.unknown_compile_cost = 1;
    evidence.live_shadow_kept.immature = 3;
    evidence.shadow_only.invalid = 4;
    let report = EvictionEvidenceReport {
        horizon_secs: 604800,
        as_of_unix_secs: 900000,
        limitations: vec![],
        stores: vec![StoreEvidence {
            store_index: 5,
            evidence: Some(evidence),
            error: None,
        }],
    };
    let text = lines(&report).join("\n");
    assert!(text.contains("Store 5, both rankings would delete: 2 judged, 0 recent\n"));
    assert!(text.contains("Store 5, live deleted, other ranking would keep: 0 judged, 3 recent"));
    assert!(text.contains(
        "Store 5, other ranking would delete, live kept: 0 judged, 0 recent, 4 unusable"
    ));
    assert!(text.contains("Compiled again: 0, 0 ms (unavailable), unknown cost 1"));
    assert!(
        !text.contains("live deleted, other ranking would keep: 0 judged, 3 recent\n    Compiled"),
        "a cohort with nothing demanded has no compiled-again line: {text}"
    );
}

#[test]
fn the_block_shows_only_with_observations() {
    let report = |cohort: Option<usize>| {
        let mut evidence = EvictionHorizonEvidence::default();
        match cohort {
            Some(0) => evidence.live_shadow_agreed.observations = 1,
            Some(1) => evidence.live_shadow_kept.observations = 1,
            Some(_) => evidence.shadow_only.observations = 1,
            None => {}
        }
        EvictionEvidenceReport {
            horizon_secs: HORIZON_SECS,
            as_of_unix_secs: 0,
            limitations: vec!["limit".into()],
            stores: vec![StoreEvidence {
                store_index: 0,
                evidence: Some(evidence),
                error: None,
            }],
        }
    };
    assert!(lines(&report(None)).is_empty());
    for cohort in 0..3 {
        let text = lines(&report(Some(cohort))).join("\n");
        assert!(text.contains("7d horizon"), "{text}");
        assert!(text.contains("do not change what gets deleted"), "{text}");
        assert!(
            text.contains("Recent rows are not in the compiled-again count."),
            "{text}"
        );
        assert!(
            text.contains("Logical bytes include shared blobs and are not disk space freed."),
            "{text}"
        );
        assert!(!text.contains("limit"), "{text}");
    }
    let mut hours = report(Some(0));
    hours.horizon_secs = 7_200;
    let hours = lines(&hours).join("\n");
    assert!(
        hours.contains("Judged rows have finished the 2h horizon."),
        "{hours}"
    );
    let mut ranged = EvictionHorizonEvidence::default();
    ranged.live_shadow_kept.observations = 2;
    ranged.live_shadow_kept.mature = 2;
    ranged.live_shadow_kept.demand_within_horizon_lower = 1;
    ranged.live_shadow_kept.demand_within_horizon_upper = 3;
    ranged.live_shadow_kept.demanded_gross_compile_cost_ms_lower = 10;
    ranged.live_shadow_kept.demanded_gross_compile_cost_ms_upper = 40;
    ranged.live_shadow_kept.known_cost_logical_bytes = 1 << 30;
    let ranged = EvictionEvidenceReport {
        horizon_secs: HORIZON_SECS,
        as_of_unix_secs: 0,
        limitations: vec![],
        stores: vec![StoreEvidence {
            store_index: 0,
            evidence: Some(ranged),
            error: None,
        }],
    };
    let ranged = lines(&ranged).join("\n");
    assert!(
        ranged.contains("Compiled again: 1–3, 10 ms–40 ms (10.0–40.0 ms/logical GiB)"),
        "{ranged}"
    );

    let mut errored = report(None);
    errored.stores[0].evidence = None;
    errored.stores[0].error = Some("index unavailable".into());
    assert!(lines(&errored).is_empty());
}

#[test]
fn any_one_demand_or_cost_prints_compiled_again() {
    let show = |edit: fn(&mut kache_store::EvictionEvidenceCohort)| {
        let mut evidence = EvictionHorizonEvidence::default();
        evidence.live_shadow_agreed.observations = 1;
        edit(&mut evidence.live_shadow_agreed);
        let report = EvictionEvidenceReport {
            horizon_secs: HORIZON_SECS,
            as_of_unix_secs: 0,
            limitations: vec![],
            stores: vec![StoreEvidence {
                store_index: 0,
                evidence: Some(evidence),
                error: None,
            }],
        };
        let text = lines(&report).join("\n");
        assert!(text.contains("Compiled again"), "{text}");
    };
    show(|cohort| cohort.demand_within_horizon_lower = 1);
    show(|cohort| cohort.demand_within_horizon_upper = 1);
    show(|cohort| cohort.demanded_gross_compile_cost_ms_lower = 1);
    show(|cohort| cohort.demanded_gross_compile_cost_ms_upper = 1);
    show(|cohort| cohort.unknown_compile_cost = 1);
}

#[test]
fn the_horizon_reads_in_its_largest_whole_unit() {
    assert_eq!(horizon_label(604_800), "7d");
    assert_eq!(horizon_label(86_400), "1d");
    assert_eq!(horizon_label(7_200), "2h");
    assert_eq!(horizon_label(90_000), "25h");
    assert_eq!(horizon_label(90), "90s");
    assert_eq!(horizon_label(0), "0s");
}
