use super::*;
use crate::events::{EventResult, log_event, read_events, rotate_events_if_needed};
use chrono::TimeZone;

fn at(day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, day, 12, 0, 0).unwrap()
}

/// A hit whose figures identify it: `n` ms of compile work avoided, `n`
/// reflinked, `2n` hardlinked and `3n` copied bytes.
fn hit(n: u64, session: &str) -> BuildEvent {
    let mut event = BuildEvent::new_for_test(&format!("crate_{n}"), EventResult::LocalHit);
    event.ts = at(1) + chrono::Duration::seconds(n as i64);
    event.session_id = session.to_string();
    event.compile_time_ms = n;
    event.reflinked_bytes = n;
    event.hardlinked_bytes = 2 * n;
    event.copied_bytes = 3 * n;
    event
}

fn totals(since: DateTime<Utc>) -> Totals {
    Totals::empty(since)
}

/// What every event ever logged adds up to.
fn expected(events: &[BuildEvent]) -> Totals {
    Totals::from_events(events).unwrap()
}

/// Log `events`, rotating after each like the wrapper does.
fn log_and_rotate(config: &Config, events: &[BuildEvent], max_size: u64, keep_lines: usize) {
    for event in events {
        log_event(&config.event_log_path(), event).unwrap();
        rotate_events_if_needed(
            &config.event_log_path(),
            max_size,
            keep_lines,
            &config.cache_dir,
        )
        .unwrap();
    }
}

#[test]
fn from_events_uses_the_stats_definitions() {
    let mut prefetched = hit(10, "");
    prefetched.result = EventResult::PrefetchHit;
    let mut remote = hit(100, "");
    remote.result = EventResult::RemoteHit;
    let mut miss = hit(1000, "");
    miss.result = EventResult::Miss;
    let events = [hit(1, ""), prefetched, remote, miss];
    let t = Totals::from_events(&events).unwrap();
    assert_eq!(t.since, at(1) + chrono::Duration::seconds(1));
    assert_eq!(t.hits, 3, "a miss is not a hit");
    assert_eq!(t.hit_compile_time_ms, 111, "a miss avoids nothing");
    // Restores are summed over every outcome, as in the report.
    assert_eq!(t.zero_copy_bytes, 1111 + 2 * 1111);
    assert_eq!(t.copied_bytes, 3 * 1111);
    assert_eq!(t.pruned_bytes(), 0);
    assert_eq!(Totals::from_events(&[]), None);
}

#[test]
fn absorb_adds_every_field_and_keeps_the_earlier_start() {
    let mut a = Totals {
        since: at(5),
        hits: 1,
        hit_compile_time_ms: 2,
        zero_copy_bytes: 3,
        copied_bytes: 4,
        pruned_automatic_bytes: 5,
        pruned_requested_bytes: 6,
    };
    let b = Totals {
        since: at(3),
        hits: 10,
        hit_compile_time_ms: 20,
        zero_copy_bytes: 30,
        copied_bytes: 40,
        pruned_automatic_bytes: 50,
        pruned_requested_bytes: 60,
    };
    a.absorb(&b);
    assert_eq!(
        a,
        Totals {
            since: at(3),
            hits: 11,
            hit_compile_time_ms: 22,
            zero_copy_bytes: 33,
            copied_bytes: 44,
            pruned_automatic_bytes: 55,
            pruned_requested_bytes: 66,
        }
    );
    assert_eq!(a.pruned_bytes(), 121);
    let mut later = totals(at(9));
    later.absorb(&totals(at(7)));
    assert_eq!(later.since, at(7));
}

#[test]
fn merge_keeps_whichever_side_exists() {
    let mut one = totals(at(2));
    one.hits = 1;
    let mut two = totals(at(4));
    two.hits = 2;
    assert_eq!(merge(None, None), None);
    assert_eq!(merge(Some(one.clone()), None), Some(one.clone()));
    assert_eq!(merge(None, Some(two.clone())), Some(two.clone()));
    let both = merge(Some(two), Some(one)).unwrap();
    assert_eq!((both.since, both.hits), (at(2), 3));
}

/// The case the ledger exists for: rotation drops the head of the log and
/// keeps a tail. The ledger must hold exactly the dropped part, so ledger
/// plus log equals everything ever logged, across several rotations.
#[test]
fn rotation_folds_what_it_drops_and_counts_nothing_twice() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    let events: Vec<BuildEvent> = (1..=60).map(|n| hit(n, "")).collect();
    let line_len = serde_json::to_string(&events[59]).unwrap().len() as u64 + 1;

    log_and_rotate(&config, &events[..30], line_len * 20, 5);
    let ledger = read(&config.cache_dir).expect("a rotation happened");
    let kept = read_events(&config.event_log_path()).unwrap();
    assert!(!kept.is_empty(), "rotation keeps a tail");
    assert!(ledger.hits > 0, "and drops a head");
    assert_eq!(ledger.hits as usize + kept.len(), 30);
    assert_eq!(
        ledger.since, events[0].ts,
        "counting began at the first event"
    );
    assert_eq!(lifetime(&config), Some(expected(&events[..30])));

    log_and_rotate(&config, &events[30..], line_len * 20, 5);
    assert_eq!(lifetime(&config), Some(expected(&events)));
}

/// A rotation can keep far more than `keep_lines` to hold the build in
/// progress. Only lines that leave the log are folded.
#[test]
fn a_session_tail_kept_by_rotation_is_not_folded() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    let mut events: Vec<BuildEvent> = (1..=40).map(|n| hit(n, "old")).collect();
    events.extend((41..=70).map(|n| hit(n, "current")));
    let line_len = serde_json::to_string(&events[69]).unwrap().len() as u64 + 1;
    for event in &events {
        log_event(&config.event_log_path(), event).unwrap();
    }
    rotate_events_if_needed(
        &config.event_log_path(),
        line_len * 64,
        5,
        &config.cache_dir,
    )
    .unwrap();

    let kept = read_events(&config.event_log_path()).unwrap();
    assert_eq!(kept.len(), 30, "the whole current build stays");
    assert_eq!(read(&config.cache_dir), Some(expected(&events[..40])));
    assert_eq!(lifetime(&config), Some(expected(&events)));
}

/// The other logs rotate through the same code without touching the ledger.
#[test]
fn plain_rotation_leaves_the_ledger_alone() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    for n in 1..=30 {
        log_event(&config.event_log_path(), &hit(n, "")).unwrap();
    }
    crate::events::rotate_if_needed(&config.event_log_path(), 1, 5).unwrap();
    assert_eq!(read_events(&config.event_log_path()).unwrap().len(), 1);
    assert!(!ledger_path(&config.cache_dir).exists());
}

#[test]
fn lines_without_build_events_fold_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let heartbeat = r#"{"event":"heartbeat","ts":"2026-09-01T12:00:00Z","crate_name":"a","pid":1,"elapsed_s":30}"#;
    fold_dropped_lines(dir.path(), &[heartbeat, "{\"torn\n", "\n"]).unwrap();
    assert!(!ledger_path(dir.path()).exists());

    let line = format!("{}\n", serde_json::to_string(&hit(7, "")).unwrap());
    fold_dropped_lines(dir.path(), &[heartbeat, &line]).unwrap();
    assert_eq!(read(dir.path()).unwrap().hits, 1);
}

#[test]
fn gc_runs_split_by_who_started_them() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    let run = |bytes_freed| crate::store::GcStats {
        bytes_freed,
        ..Default::default()
    };
    let started = Utc::now();
    crate::report::record_gc_run(&config, "daemon", SweepOrigin::Automatic, &run(100)).unwrap();
    // The daemon also runs the `kache gc` a user asks for.
    crate::report::record_gc_run(&config, "daemon", SweepOrigin::Requested, &run(30)).unwrap();
    crate::report::record_gc_run(&config, "auto", SweepOrigin::Automatic, &run(5)).unwrap();
    crate::report::record_gc_run(&config, "manual", SweepOrigin::Requested, &run(1)).unwrap();

    let ledger = read(&config.cache_dir).unwrap();
    assert_eq!(ledger.pruned_automatic_bytes, 105);
    assert_eq!(ledger.pruned_requested_bytes, 31);
    assert_eq!(ledger.hits, 0);
    assert!(ledger.since >= started, "counting began with the first run");
    assert_eq!(lifetime(&config), Some(ledger));
}

#[test]
fn nothing_recorded_reads_as_none() {
    let dir = tempfile::tempdir().unwrap();
    let config = crate::test_support::test_config(dir.path().to_path_buf());
    assert_eq!(read(&config.cache_dir), None);
    assert_eq!(lifetime(&config), None);

    // A log that never rotated is the whole history.
    log_event(&config.event_log_path(), &hit(4, "")).unwrap();
    assert_eq!(lifetime(&config), Some(expected(&[hit(4, "")])));
}

#[test]
fn a_gc_run_that_freed_nothing_starts_no_ledger() {
    let dir = tempfile::tempdir().unwrap();
    record_pruned(dir.path(), SweepOrigin::Automatic, 0, at(2)).unwrap();
    assert!(!ledger_path(dir.path()).exists());
    record_pruned(dir.path(), SweepOrigin::Automatic, 1, at(3)).unwrap();
    record_pruned(dir.path(), SweepOrigin::Automatic, 0, at(1)).unwrap();
    assert_eq!(
        read(dir.path()).unwrap().since,
        at(3),
        "a no-op run is not counted"
    );
}

#[test]
fn a_corrupt_ledger_reads_as_none_and_the_next_write_starts_over() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(ledger_path(dir.path()), "{ not json").unwrap();
    assert_eq!(read(dir.path()), None);

    record_pruned(dir.path(), SweepOrigin::Requested, 9, at(2)).unwrap();
    let mut only_this_run = totals(at(2));
    only_this_run.pruned_requested_bytes = 9;
    assert_eq!(read(dir.path()), Some(only_this_run));
    assert_eq!(
        std::fs::read_to_string(corrupt_path(dir.path())).unwrap(),
        "{ not json",
        "the corrupt ledger is kept aside"
    );

    // Valid JSON in the current schema with the wrong shape is corrupt too.
    std::fs::write(ledger_path(dir.path()), r#"{"schema":1,"hits":"many"}"#).unwrap();
    assert_eq!(read(dir.path()), None);
}

#[test]
fn a_newer_ledger_is_neither_read_nor_overwritten() {
    let dir = tempfile::tempdir().unwrap();
    let path = ledger_path(dir.path());
    let newer = r#"{"schema":2,"since":"2026-09-01T12:00:00Z","hits":5}"#;
    std::fs::write(&path, newer).unwrap();
    assert_eq!(read(dir.path()), None);
    record_pruned(dir.path(), SweepOrigin::Automatic, 9, at(2)).unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), newer);

    // A newer schema in a shape this version cannot parse is left alone
    // too: the schema number is read first.
    let reshaped = r#"{"schema":2,"totals":{"hits":5}}"#;
    std::fs::write(&path, reshaped).unwrap();
    record_pruned(dir.path(), SweepOrigin::Automatic, 9, at(2)).unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), reshaped);
    assert!(!corrupt_path(dir.path()).exists());

    // The current schema is read and added to.
    std::fs::write(&path, newer.replace("\"schema\":2", "\"schema\":1")).unwrap();
    record_pruned(dir.path(), SweepOrigin::Automatic, 9, at(2)).unwrap();
    let ledger = read(dir.path()).unwrap();
    assert_eq!((ledger.since, ledger.hits), (at(1), 5));
    assert_eq!(ledger.pruned_automatic_bytes, 9);
}

/// The file format, and so the `lifetime` object of `kache stats --json`.
#[test]
fn ledger_file_shape() {
    let dir = tempfile::tempdir().unwrap();
    record_pruned(dir.path(), SweepOrigin::Requested, 7, at(1)).unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(ledger_path(dir.path())).unwrap()).unwrap();
    assert_eq!(
        value,
        serde_json::json!({
            "schema": 1,
            "since": "2026-09-01T12:00:00Z",
            "hits": 0,
            "hit_compile_time_ms": 0,
            "zero_copy_bytes": 0,
            "copied_bytes": 0,
            "pruned_automatic_bytes": 0,
            "pruned_requested_bytes": 7,
        })
    );
}

/// Rotation and GC update the ledger from different processes; the lock
/// keeps every update.
#[test]
fn concurrent_updates_are_all_kept() {
    let dir = tempfile::tempdir().unwrap();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let cache_dir = dir.path().to_path_buf();
            std::thread::spawn(move || {
                for _ in 0..20 {
                    record_pruned(&cache_dir, SweepOrigin::Automatic, 1, Utc::now()).unwrap();
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(read(dir.path()).unwrap().pruned_automatic_bytes, 160);
}

/// A volume shard keeps its own `gc_stats.json` and so its own ledger;
/// lifetime totals include it.
#[test]
fn lifetime_includes_volume_shard_ledgers() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::test_support::test_config(dir.path().join("main"));
    let shard = dir.path().join("shard");
    std::fs::create_dir_all(&shard).unwrap();
    std::fs::write(shard.join("index.db"), b"").unwrap();
    config.volume_stores = vec![crate::config::VolumeStore {
        volume: "/mnt/vol/".into(),
        store: shard.clone(),
        max_size: None,
    }];
    record_pruned(&config.cache_dir, SweepOrigin::Automatic, 4, at(6)).unwrap();
    record_pruned(&shard, SweepOrigin::Requested, 3, at(2)).unwrap();
    let total = lifetime(&config).unwrap();
    assert_eq!(total.since, at(2));
    assert_eq!(total.pruned_automatic_bytes, 4);
    assert_eq!(total.pruned_requested_bytes, 3);
}
