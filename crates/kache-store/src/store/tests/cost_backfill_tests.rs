use super::*;

fn legacy(store: &Store, key: &str, cost: u64) -> EntryMeta {
    let mut entry = write_verified_fixture(store, key, key, "lib.rlib", None);
    entry.meta.compile_time_ms = cost;
    fs::write(
        store.entry_dir(key).join("meta.json"),
        serde_json::to_vec(&entry.meta).unwrap(),
    )
    .unwrap();
    store
        .db
        .execute(
            "INSERT INTO entries(cache_key, crate_name, size, committed)
             VALUES (?1, 'fixture', 25, 1)",
            params![key],
        )
        .unwrap();
    entry.meta
}

fn state(store: &Store, key: &str) -> (i64, i64, i64, i64) {
    store
        .db
        .query_row(
            "SELECT compile_time_ms, compile_time_backfilled,
                    compile_time_retry_at, compile_time_retry_count
             FROM entries WHERE cache_key = ?1",
            params![key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
}

#[test]
fn processed_zero_is_not_read_again_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    legacy(&store, "zero", 0);
    assert_eq!(store.backfill_compile_times_limited(3).unwrap(), 0);
    assert_eq!(state(&store, "zero"), (0, 1, 0, 0));
    fs::remove_file(store.entry_dir("zero").join("meta.json")).unwrap();
    drop(store);

    let store = Store::open(&config).unwrap();
    assert!(store.compile_time_candidates(3, 10_000).unwrap().is_empty());
    assert_eq!(store.backfill_compile_times().unwrap(), 0);
    assert_eq!(state(&store, "zero"), (0, 1, 0, 0));
}

#[test]
fn broken_prefix_defers_without_displacing_a_recoverable_entry() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let meta = legacy(&store, "a-broken", 91);
    legacy(&store, "z-valid", 7777);
    fs::remove_file(store.entry_dir("a-broken").join("meta.json")).unwrap();

    assert_eq!(
        store
            .backfill_compile_times_batch(1, 1000, || false)
            .unwrap(),
        0
    );
    assert_eq!(state(&store, "a-broken"), (0, 0, 1060, 1));
    assert_eq!(
        store
            .backfill_compile_times_batch(1, 1000, || false)
            .unwrap(),
        1
    );
    assert_eq!(state(&store, "z-valid"), (7777, 1, 0, 0));
    fs::write(
        store.entry_dir("a-broken").join("meta.json"),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
    assert_eq!(
        store
            .backfill_compile_times_batch(1, 1059, || false)
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .backfill_compile_times_batch(1, 1060, || false)
            .unwrap(),
        1
    );
    assert_eq!(state(&store, "a-broken"), (91, 1, 0, 0));
}

#[test]
fn invalid_metadata_is_deferred_and_an_absent_cost_is_processed_as_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    for key in [
        "bad-json",
        "wrong-key",
        "overflow",
        "negative",
        "absent-cost",
    ] {
        let meta = legacy(&store, key, 100);
        let mut value = serde_json::to_value(meta).unwrap();
        match key {
            "bad-json" => {
                fs::write(store.entry_dir(key).join("meta.json"), b"{").unwrap();
                continue;
            }
            "wrong-key" => value["cache_key"] = "another".into(),
            "overflow" => value["compile_time_ms"] = u64::MAX.into(),
            "negative" => value["compile_time_ms"] = (-1).into(),
            "absent-cost" => {
                value.as_object_mut().unwrap().remove("compile_time_ms");
            }
            _ => unreachable!(),
        }
        fs::write(
            store.entry_dir(key).join("meta.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(
        store
            .backfill_compile_times_batch(10, 1000, || false)
            .unwrap(),
        0
    );
    for key in ["bad-json", "wrong-key", "overflow", "negative"] {
        assert_eq!(state(&store, key), (0, 0, 1060, 1), "{key}");
    }
    assert_eq!(state(&store, "absent-cost"), (0, 1, 0, 0));
    assert!(store.compile_time_candidates(10, 1059).unwrap().is_empty());
    assert_eq!(
        store
            .backfill_compile_times_batch(10, 1060, || false)
            .unwrap(),
        0
    );
    assert_eq!(state(&store, "bad-json"), (0, 0, 1180, 2));
}

#[test]
fn existing_positive_costs_and_uncommitted_rows_are_not_backfilled() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    legacy(&store, "positive", 7777);
    legacy(&store, "uncommitted", 7777);
    store
        .db
        .execute(
            "UPDATE entries SET compile_time_ms = 50 WHERE cache_key = 'positive'",
            [],
        )
        .unwrap();
    store
        .db
        .execute(
            "UPDATE entries SET committed = 0 WHERE cache_key = 'uncommitted'",
            [],
        )
        .unwrap();
    assert_eq!(store.backfill_compile_times_limited(10).unwrap(), 0);
    assert_eq!(state(&store, "positive"), (50, 0, 0, 0));
    assert_eq!(state(&store, "uncommitted"), (0, 0, 0, 0));
}

#[test]
fn count_and_soft_time_bounds_leave_deterministic_progress() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    for index in 0..5 {
        legacy(&store, &format!("key-{index}"), 100);
    }
    assert_eq!(
        store
            .backfill_compile_times_batch(0, 1000, || false)
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .backfill_compile_times_batch(-1, 1000, || false)
            .unwrap(),
        0
    );
    assert_eq!(store.compile_time_candidates(10, 1000).unwrap().len(), 5);
    let mut checks = 0;
    assert_eq!(
        store
            .backfill_compile_times_batch(5, 1000, || {
                checks += 1;
                checks == 2
            })
            .unwrap(),
        2
    );
    assert_eq!(store.compile_time_candidates(10, 1000).unwrap().len(), 3);
    assert_eq!(
        store
            .backfill_compile_times_batch(3, 1000, || true)
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .backfill_compile_times_batch(1, 1000, || false)
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .backfill_compile_times_batch(3, 1000, || false)
            .unwrap(),
        1
    );
    assert_eq!(store.backfill_compile_times_limited(3).unwrap(), 0);
}

#[test]
fn count_limit_caps_the_last_page_and_preserves_the_remainder() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    for index in 0..200 {
        legacy(&store, &format!("key-{index:03}"), 100);
    }

    assert_eq!(store.backfill_compile_times_limited(130).unwrap(), 130);
    assert_eq!(store.compile_time_candidates(200, 1000).unwrap().len(), 70);
    assert_eq!(state(&store, "key-129"), (100, 1, 0, 0));
    assert_eq!(state(&store, "key-130"), (0, 0, 0, 0));

    assert_eq!(store.backfill_compile_times_limited(130).unwrap(), 70);
    assert!(store.compile_time_candidates(200, 1000).unwrap().is_empty());
}

#[test]
fn writes_commit_in_small_groups_between_metadata_reads() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    for index in 0..129 {
        legacy(&store, &format!("key-{index:03}"), 0);
    }
    let mut largest_visible_group = 0_i64;
    assert_eq!(
        store
            .backfill_compile_times_batch(129, 1000, || {
                let processed: i64 = store
                    .db
                    .query_row(
                        "SELECT count(*) FROM entries WHERE compile_time_backfilled = 1",
                        [],
                        |row| row.get(0),
                    )
                    .unwrap();
                largest_visible_group = largest_visible_group.max(processed);
                false
            })
            .unwrap(),
        0
    );
    assert_eq!(largest_visible_group, 128);
    assert!(store.compile_time_candidates(200, 1000).unwrap().is_empty());
}

#[test]
fn stale_results_cannot_update_deleted_replaced_or_revised_rows() {
    for change in [
        "DELETE FROM entries",
        "UPDATE entries SET rowid = rowid + 1",
        "UPDATE entries SET cache_key = 'another'",
        "UPDATE entries SET committed = 0",
        "UPDATE entries SET compile_time_ms = 99",
        "UPDATE entries SET compile_time_backfilled = 1",
        "UPDATE entries SET compile_time_retry_at = 1",
        "UPDATE entries SET compile_time_retry_count = 1",
        "UPDATE entries SET content_hash = 'replaced'",
    ] {
        for broken in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(test_config(dir.path())).unwrap();
            legacy(&store, "stale", 7777);
            if broken {
                fs::remove_file(store.entry_dir("stale").join("meta.json")).unwrap();
            }
            let candidate = store
                .compile_time_candidates(1, 1000)
                .unwrap()
                .pop()
                .unwrap();
            let outcome = store.compile_time_outcome(candidate);
            store.db.execute_batch(change).unwrap();
            let snapshot = || -> Vec<String> {
                let mut stmt = store.db.prepare(
                    "SELECT printf('%d|%s|%d|%d|%d|%d|%d|%s', rowid, cache_key,
                     committed, compile_time_ms, compile_time_backfilled,
                     compile_time_retry_at, compile_time_retry_count, coalesce(content_hash, 'NULL'))
                     FROM entries ORDER BY cache_key",
                ).unwrap();
                stmt.query_map([], |row| row.get(0))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap()
            };
            let before = snapshot();
            assert_eq!(
                store.apply_compile_time_outcomes(&[outcome], 1000).unwrap(),
                0,
                "{change}"
            );
            assert_eq!(snapshot(), before, "{change}, broken={broken}");
        }
    }
}

#[test]
fn a_current_zero_cost_publication_wins_over_a_stale_result() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    legacy(&store, "same-key", 7777);
    let candidate = store
        .compile_time_candidates(1, 1000)
        .unwrap()
        .pop()
        .unwrap();
    let outcome = store.compile_time_outcome(candidate);
    let source = dir.path().join("new.rlib");
    fs::write(&source, b"new publication").unwrap();
    store
        .put_with_compile_time(
            "same-key",
            "fixture",
            &["lib".into()],
            &[],
            "host",
            "dev",
            &[(source, "new.rlib".into())],
            "",
            "",
            0,
        )
        .unwrap();
    assert_eq!(
        store.apply_compile_time_outcomes(&[outcome], 1000).unwrap(),
        0
    );
    assert_eq!(state(&store, "same-key"), (0, 1, 0, 0));
}

#[test]
fn an_update_failure_rolls_back_cost_and_state_together() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    legacy(&store, "a", 91);
    legacy(&store, "b", 92);
    let outcomes: Vec<_> = store
        .compile_time_candidates(2, 1000)
        .unwrap()
        .into_iter()
        .map(|candidate| store.compile_time_outcome(candidate))
        .collect();
    store
        .db
        .execute_batch(
            "CREATE TRIGGER reject_backfill BEFORE UPDATE ON entries
         WHEN NEW.cache_key = 'b' BEGIN SELECT RAISE(ABORT, 'injected write failure'); END;",
        )
        .unwrap();
    assert!(store.apply_compile_time_outcomes(&outcomes, 1000).is_err());
    assert_eq!(state(&store, "a"), (0, 0, 0, 0));
    assert_eq!(state(&store, "b"), (0, 0, 0, 0));
}

#[test]
fn legacy_schema_upgrades_and_reopens_without_resetting_progress() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    legacy(&store, "legacy", 0);
    store
        .db
        .execute_batch(
            "DROP INDEX idx_entries_compile_time_pending;
         ALTER TABLE entries DROP COLUMN compile_time_backfilled;
         ALTER TABLE entries DROP COLUMN compile_time_retry_at;
         ALTER TABLE entries DROP COLUMN compile_time_retry_count;
         PRAGMA user_version = 9;",
        )
        .unwrap();
    drop(store);
    let store = Store::open(&config).unwrap();
    assert_eq!(state(&store, "legacy"), (0, 0, 0, 0));
    assert_eq!(store.backfill_compile_times_limited(1).unwrap(), 0);
    assert_eq!(state(&store, "legacy"), (0, 1, 0, 0));
    super::super::compile_time_backfill::ensure_schema(&store.db).unwrap();
    assert_eq!(state(&store, "legacy"), (0, 1, 0, 0));
    drop(store);
    let store = Store::open(&config).unwrap();
    assert!(store.compile_time_candidates(1, 1000).unwrap().is_empty());
}

#[test]
fn concurrent_schema_upgrades_serialize_column_creation() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();
    legacy(&store, "legacy", 0);
    store
        .db
        .execute_batch(
            "DROP INDEX idx_entries_compile_time_pending;
         ALTER TABLE entries DROP COLUMN compile_time_backfilled;
         ALTER TABLE entries DROP COLUMN compile_time_retry_at;
         ALTER TABLE entries DROP COLUMN compile_time_retry_count;
         PRAGMA user_version = 9;",
        )
        .unwrap();
    drop(store);

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let threads: Vec<_> = (0..3)
        .map(|_| {
            let cache = dir.path().to_owned();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let store = Store::open(test_config(&cache)).unwrap();
                assert_eq!(state(&store, "legacy"), (0, 0, 0, 0));
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}

#[test]
fn due_work_uses_the_partial_index_without_a_temporary_sort() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let sql = format!(
        "EXPLAIN QUERY PLAN {}",
        super::super::compile_time_backfill::SELECT_DUE
    );
    let mut stmt = store.db.prepare(&sql).unwrap();
    let details = stmt
        .query_map(params![1000, 3], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n");
    assert!(
        details.contains("idx_entries_compile_time_pending"),
        "{details}"
    );
    assert!(!details.contains("TEMP B-TREE"), "{details}");
}

#[test]
fn every_current_writer_indexes_zero_cost_metadata_during_publication() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(test_config(dir.path())).unwrap();
    let source = dir.path().join("put.rlib");
    fs::write(&source, b"put fixture").unwrap();
    store
        .put_with_compile_time(
            "put",
            "fixture",
            &["lib".into()],
            &[],
            "host",
            "dev",
            &[(source, "put.rlib".into())],
            "",
            "",
            0,
        )
        .unwrap();
    assert_eq!(state(&store, "put"), (0, 1, 0, 0));

    for (index, route) in ["download", "restored", "verified", "rebuild"]
        .iter()
        .enumerate()
    {
        let key = format!("{:064x}", index + 1);
        let mut entry = write_verified_fixture(&store, &key, &key, "lib.rlib", None);
        entry.meta.compile_time_ms = 0;
        fs::write(
            store.entry_dir(&key).join("meta.json"),
            serde_json::to_vec(&entry.meta).unwrap(),
        )
        .unwrap();
        match *route {
            "download" => store.import_downloaded_entry(&key).unwrap(),
            "restored" => store.import_restored_entry(&key).unwrap(),
            "verified" => {
                assert_eq!(store.import_verified_restored_entries(&[entry]).unwrap(), 1);
            }
            "rebuild" => {
                let blob = store.blob_path(&entry.meta.files[0].hash);
                fs::create_dir_all(blob.parent().unwrap()).unwrap();
                if !blob.exists() {
                    fs::copy(store.entry_dir(&key).join("lib.rlib"), &blob).unwrap();
                }
                assert!(
                    store
                        .rebuild_one_entry(&key, &store.entry_dir(&key))
                        .unwrap()
                        .is_some()
                );
            }
            _ => unreachable!(),
        }
        assert_eq!(state(&store, &key), (0, 1, 0, 0), "{route}");
    }
    assert!(store.compile_time_candidates(10, 1000).unwrap().is_empty());
}
