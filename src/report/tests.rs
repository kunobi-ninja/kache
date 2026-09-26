use super::*;
use chrono::Utc;

fn write_gc_stats(dir: &std::path::Path, last_run: chrono::DateTime<Utc>) {
    let persisted = format!(
        r#"{{"last_run":"{}","entries_evicted":3,"bytes_freed":4096,"blobs_removed":5,"duration_ms":12}}"#,
        last_run.to_rfc3339()
    );
    std::fs::write(dir.join("gc_stats.json"), persisted).unwrap();
}

/// A run at exactly the cutoff is inside the window: the comparison is
/// "older than the cutoff is excluded", not "at or older". One second
/// either side of that instant decides it.
#[test]
fn load_gc_summary_includes_a_run_exactly_at_the_cutoff() {
    let dir = tempfile::tempdir().unwrap();
    let last_run = Utc::now() - chrono::Duration::hours(3);
    write_gc_stats(dir.path(), last_run);
    // The fixture writes an RFC 3339 string, so compare against the value
    // that string parses back to rather than the original instant.
    let persisted: GcStatsPersisted =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("gc_stats.json")).unwrap())
            .unwrap();
    let written = chrono::DateTime::parse_from_rfc3339(&persisted.last_run)
        .unwrap()
        .with_timezone(&Utc);

    assert!(
        load_gc_summary(dir.path(), written).is_some(),
        "a run at the cutoff is within the window"
    );
    assert!(
        load_gc_summary(dir.path(), written - chrono::Duration::seconds(1)).is_some(),
        "a run after the cutoff is within the window"
    );
    assert!(
        load_gc_summary(dir.path(), written + chrono::Duration::seconds(1)).is_none(),
        "a run before the cutoff is outside it"
    );
}

#[test]
fn load_gc_summary_returns_recent_run_within_window() {
    let dir = tempfile::tempdir().unwrap();
    write_gc_stats(dir.path(), Utc::now() - chrono::Duration::hours(1));
    let gc = load_gc_summary(dir.path(), SinceWindow::DEFAULT.cutoff(Utc::now()))
        .expect("recent gc run is within the 24h window");
    assert_eq!(gc.entries_evicted, 3);
    assert_eq!(gc.bytes_freed, 4096);
    assert_eq!(gc.blobs_removed, 5);
}

#[test]
fn load_gc_summary_drops_run_older_than_window() {
    let dir = tempfile::tempdir().unwrap();
    write_gc_stats(dir.path(), Utc::now() - chrono::Duration::hours(48));
    assert!(
        load_gc_summary(dir.path(), SinceWindow::DEFAULT.cutoff(Utc::now())).is_none(),
        "a run 48h ago must fall outside the 24h window"
    );
}

#[test]
fn load_gc_summary_absent_when_no_stats_file() {
    let dir = tempfile::tempdir().unwrap();
    assert!(load_gc_summary(dir.path(), SinceWindow::DEFAULT.cutoff(Utc::now())).is_none());
}

#[test]
fn load_gc_summary_absent_on_malformed_stats_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("gc_stats.json"), b"not json").unwrap();
    assert!(load_gc_summary(dir.path(), SinceWindow::DEFAULT.cutoff(Utc::now())).is_none());
}

/// The GC history is opt-in with session recording. Off, a run writes
/// gc_stats.json and nothing under `telemetry/`; on, each run appends one
/// line carrying every figure of that run.
#[test]
fn gc_runs_history_is_appended_only_with_record_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = write_test_events(dir.path());
    let run = crate::store::GcStats {
        entries_evicted: 3,
        bytes_freed: 100,
        disk_bytes_reclaimed: 60,
        blobs_removed: 2,
        duration_ms: 9,
        entries_pinned: 4,
        entries_unreclaimable: 1,
        entries_failed: 5,
        entries_locked: 3,
        entries_busy_snapshot: 2,
        entries_recent_prefiltered: 4,
        entries_import_pinned: 7,
        evict_write_ms: 11,
        bytes_held: 12,
        housekeeping: Some(crate::store::HousekeepingStats {
            key_locks_removed: 6,
            key_locks_remaining: 7,
            predictions_pruned: 8,
            file_hashes_pruned: 9,
        }),
        ..Default::default()
    };

    record_gc_run(&config, "daemon", &run).unwrap();
    let persisted = read_gc_stats(&config.cache_dir).unwrap();
    assert_eq!(persisted.source, "daemon");
    assert_eq!(persisted.bytes_held, 12);
    assert!(
        !config.cache_dir.join("telemetry").exists(),
        "recording off: no telemetry dir"
    );

    config.record_sessions = true;
    record_gc_run(&config, "auto", &run).unwrap();
    record_gc_run(&config, "manual", &crate::store::GcStats::default()).unwrap();

    let log = std::fs::read_to_string(gc_runs_log_path(&config.cache_dir)).unwrap();
    let records: Vec<GcRunRecord> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2, "{log}");
    assert!(chrono::DateTime::parse_from_rfc3339(&records[0].ts).is_ok());
    assert_eq!(
        records[0],
        GcRunRecord {
            ts: records[0].ts.clone(),
            schema: GC_RUN_RECORD_SCHEMA,
            source: "auto".to_string(),
            entries_evicted: 3,
            bytes_freed: 100,
            disk_bytes_reclaimed: 60,
            blobs_removed: 2,
            entries_failed: 5,
            entries_locked: 3,
            entries_busy_snapshot: 2,
            entries_recent_prefiltered: 4,
            entries_import_pinned: 7,
            entries_pinned: 4,
            entries_unreclaimable: 1,
            bytes_held: 12,
            duration_ms: 9,
            evict_write_ms: 11,
            key_locks_removed: Some(6),
            key_locks_remaining: Some(7),
            predictions_pruned: Some(8),
            file_hashes_pruned: Some(9),
        }
    );
    assert_eq!(records[1].source, "manual");
    assert_eq!(records[1].entries_evicted, 0);
    // A run with no housekeeping writes no counts, rather than zeros.
    assert_eq!(records[1].key_locks_removed, None);
    assert_eq!(records[1].key_locks_remaining, None);
    assert_eq!(records[1].predictions_pruned, None);
    assert!(!log.lines().nth(1).unwrap().contains("key_locks"), "{log}");
}

#[test]
fn gc_runs_history_rotates_like_the_other_logs() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = write_test_events(dir.path());
    config.record_sessions = true;
    config.event_log_max_size = 1;
    config.event_log_keep_lines = 2;
    for entries_evicted in 1..=3 {
        let run = crate::store::GcStats {
            entries_evicted,
            ..Default::default()
        };
        record_gc_run(&config, "manual", &run).unwrap();
    }
    let log = std::fs::read_to_string(gc_runs_log_path(&config.cache_dir)).unwrap();
    let evicted: Vec<usize> = log
        .lines()
        .map(|line| {
            serde_json::from_str::<GcRunRecord>(line)
                .unwrap()
                .entries_evicted
        })
        .collect();
    // Past the size cap, rotation keeps at most keep_lines, then drops the
    // oldest until the file fits; a 1-byte cap leaves only the newest.
    assert_eq!(evicted, vec![3]);
}

/// gc_stats.json is the last run only: a second run replaces every field,
/// and the file keeps the keys that older readers require.
#[test]
fn record_gc_run_keeps_only_the_last_run() {
    let dir = tempfile::tempdir().unwrap();
    let first = crate::store::GcStats {
        entries_evicted: 3,
        bytes_freed: 100,
        entries_failed: 2,
        entries_locked: 2,
        ..Default::default()
    };
    let second = crate::store::GcStats {
        entries_evicted: 1,
        bytes_freed: 50,
        disk_bytes_reclaimed: 40,
        blobs_removed: 4,
        duration_ms: 7,
        entries_pinned: 6,
        entries_failed: 5,
        entries_locked: 4,
        evict_write_ms: 12,
        housekeeping: Some(crate::store::HousekeepingStats {
            key_locks_removed: 20,
            key_locks_remaining: 30,
            predictions_pruned: 40,
            file_hashes_pruned: 50,
        }),
        ..Default::default()
    };
    write_last_gc_run(dir.path(), "daemon", &first).unwrap();
    let without = read_gc_stats(dir.path()).unwrap();
    assert_eq!(
        (
            without.key_locks_removed,
            without.key_locks_remaining,
            without.predictions_pruned,
            without.file_hashes_pruned
        ),
        (None, None, None, None)
    );
    write_last_gc_run(dir.path(), "auto", &second).unwrap();

    let stats = read_gc_stats(dir.path()).unwrap();
    assert_eq!(
        (
            stats.source.as_str(),
            stats.entries_evicted,
            stats.bytes_freed,
            stats.disk_bytes_reclaimed,
            stats.blobs_removed,
            stats.duration_ms,
            stats.entries_pinned,
            stats.entries_failed,
            stats.entries_locked,
        ),
        ("auto", 1, 50, 40, 4, 7, 6, 5, 4)
    );
    assert_eq!(stats.evict_write_ms, 12);
    assert_eq!(
        (
            stats.key_locks_removed,
            stats.key_locks_remaining,
            stats.predictions_pruned,
            stats.file_hashes_pruned
        ),
        (Some(20), Some(30), Some(40), Some(50))
    );
    let raw: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join(GC_STATS_FILE)).unwrap())
            .unwrap();
    assert!(raw.get("totals").is_none(), "{raw}");
    for key in [
        "last_run",
        "entries_evicted",
        "bytes_freed",
        "blobs_removed",
        "duration_ms",
    ] {
        assert!(raw.get(key).is_some(), "older readers require {key}: {raw}");
    }
}

/// A gc_stats.json that no longer parses is replaced by the next GC run.
/// That has to show in the log rather than pass silently.
#[test]
fn unparseable_gc_stats_warns_before_it_is_replaced() {
    struct Capture(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(GC_STATS_FILE), b"{\"totals\": ").unwrap();
    let output = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let writer = std::sync::Arc::clone(&output);
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_writer(move || Capture(std::sync::Arc::clone(&writer)))
        .finish();

    let read = tracing::subscriber::with_default(subscriber, || read_gc_stats(dir.path()));

    assert!(read.is_none());
    let log = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(log.contains("WARN"), "{log}");
    assert!(log.contains("the next GC run replaces it"), "{log}");
}

/// Files written while gc_stats.json carried running totals still load,
/// and the next run writes the last-run record without them.
#[test]
fn gc_stats_with_totals_from_an_earlier_version_still_parses() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
            dir.path().join(GC_STATS_FILE),
            r#"{"last_run":"2026-09-12T12:11:05+00:00","entries_evicted":2,"bytes_freed":9,"blobs_removed":1,"duration_ms":3,"source":"auto","totals":{"since":"2026-09-01T00:00:00+00:00","runs":4}}"#,
        )
        .unwrap();
    let old = read_gc_stats(dir.path()).expect("the earlier format parses");
    assert_eq!(
        (old.entries_evicted, old.bytes_freed, old.source.as_str()),
        (2, 9, "auto")
    );

    let run = crate::store::GcStats {
        entries_evicted: 5,
        ..Default::default()
    };
    write_last_gc_run(dir.path(), "manual", &run).unwrap();

    let raw = std::fs::read_to_string(dir.path().join(GC_STATS_FILE)).unwrap();
    assert!(!raw.contains("totals"), "{raw}");
    let gc = load_gc_summary(dir.path(), SinceWindow::DEFAULT.cutoff(Utc::now()))
        .expect("the recorded run is inside the window");
    assert_eq!(gc.entries_evicted, 5);
}

fn test_event(
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
) -> BuildEvent {
    BuildEvent {
        ts: Utc::now(),
        session_id: String::new(),
        demands: Vec::new(),
        crate_name: crate_name.to_string(),
        root: String::new(),
        version: "0.1.0".to_string(),
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key: cache_key.to_string(),
        schema: 8,
        key_ms: 0,
        key_hash_hits: 0,
        key_hash_misses: 0,
        key_hash_bytes: 0,
        lookup_ms: 0,
        restore_ms: 0,
        store_ms: 0,
        startup_ms: 0,
        dep_info_ms: 0,
        dep_info_runs: 0,
        prediction_mismatches: 0,
        flight_wait_ms: 0,
        permit_wait_ms: 0,
        store_output_blobs: 0,
        store_duplicate_blobs: 0,
        store_new_blobs: 0,
        compiler_runs: 0,
        preprocessor_runs: 0,
        probe_runs: 0,
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
        passthrough_reason: String::new(),
        store_error: String::new(),
        store_handed_off: false,
        daemon_store_ms: 0,
        lookup_rejection: String::new(),
        verify_compare: String::new(),
        fallback: false,
        fallback_attempt: None,
        exit_code: None,
        key_fields: Default::default(),
        key_diff: Vec::new(),
        key_externs: Default::default(),
        key_externs_recorded: false,
        unit_id: String::new(),
        extern_units: Default::default(),
    }
}

fn test_transfer(
    crate_name: &str,
    direction: TransferDirection,
    format: &str,
    compressed_bytes: u64,
    elapsed_ms: u64,
    ok: bool,
) -> TransferEvent {
    TransferEvent {
        accounting: None,
        prefetch: None,
        outcome: String::new(),
        schema: 3,
        crate_name: crate_name.to_string(),
        direction,
        format: format.to_string(),
        cache_key: format!("{crate_name}-key"),
        object_key: format!("prefix/v3/packs/{crate_name}/{crate_name}-key.tar.zst"),
        compressed_bytes,
        started_at_unix_ms: 0,
        finished_at_unix_ms: 0,
        elapsed_ms,
        network_ms: elapsed_ms / 2, // simulate network = half of total
        semaphore_wait_ms: 0,
        head_ms: 0,
        request_ms: elapsed_ms / 5,
        body_ms: elapsed_ms / 3,
        request_count: 4,
        original_bytes: compressed_bytes * 3, // simulate ~3x compression ratio
        decompress_ms: elapsed_ms / 4,        // simulate decompress = quarter of total
        extract_ms: 0,
        disk_io_ms: 0,
        import_lock_wait_ms: 0,
        import_ms: 0,
        compression_ms: 0,
        head_checks_ms: 0,
        blobs_skipped: 0,
        blobs_total: 2,
        ok,
        timestamp: Utc::now().timestamp() as u64,
    }
}

#[test]
fn report_preserves_daemon_publication_without_counting_it_as_wrapper_time() {
    let mut event = test_event("foo.c", EventResult::Miss, 100, 90, 42, "key");
    event.store_ms = 2;
    event.store_handed_off = true;
    event.daemon_store_ms = 50;
    let detail = to_crate_detail(&event);
    assert!(detail.store_handed_off);
    assert_eq!(detail.daemon_store_ms, 50);
    assert_eq!(detail.overhead_ms, 10);
    let value = serde_json::to_value(detail).unwrap();
    assert_eq!(value["store_handed_off"], true);
    assert_eq!(value["daemon_store_ms"], 50);
    assert_eq!(event.store_ms, 2);
}

fn write_test_events(dir: &std::path::Path) -> Config {
    let root_a = dir.join("checkout-a");
    let root_b = dir.join("checkout-b");
    std::fs::create_dir_all(&root_a).unwrap();
    std::fs::create_dir_all(&root_b).unwrap();
    let root_a = root_a
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let root_b = root_b
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.to_path_buf(),
        runtime_dir: dir.to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };

    // Write build events
    let mut passthrough = test_event("build.rs", EventResult::Passthrough, 250, 0, 0, "");
    passthrough.passthrough_reason = "refused: unsupported rustc invocation".to_string();
    passthrough.fallback = true;
    passthrough.exit_code = Some(0);

    let mut skipped = test_event("doc-test", EventResult::Skipped, 0, 0, 0, "");
    skipped.passthrough_reason = "explicitly skipped".to_string();

    let mut dup = test_event(
        "my_lib",
        EventResult::Dup,
        5000,
        4800,
        3 * 1024 * 1024,
        "def456789012",
    );
    dup.store_output_blobs = 1;
    dup.store_duplicate_blobs = 1;

    let mut events = vec![
        test_event(
            "serde",
            EventResult::LocalHit,
            5,
            300,
            1024 * 1024,
            "abc123def456",
        ),
        test_event(
            "tokio",
            EventResult::PrefetchHit,
            8,
            500,
            2 * 1024 * 1024,
            "bcd234",
        ),
        test_event(
            "regex",
            EventResult::RemoteHit,
            120,
            400,
            512 * 1024,
            "cde345",
        ),
        dup,
        test_event(
            "my_app",
            EventResult::Miss,
            8000,
            7500,
            5 * 1024 * 1024,
            "efg567",
        ),
        test_event("broken", EventResult::Error, 10, 0, 0, "err001"),
        passthrough,
        skipped,
    ];
    for e in &mut events {
        e.root = root_a.clone();
    }
    events[5].root = root_b;
    for e in &events {
        events::log_event(&config.event_log_path(), e).unwrap();
    }

    // Write transfer events
    let transfers = vec![
        test_transfer(
            "serde",
            TransferDirection::Download,
            "v3",
            500_000,
            150,
            true,
        ),
        test_transfer(
            "tokio",
            TransferDirection::Download,
            "v3",
            1_000_000,
            300,
            true,
        ),
        test_transfer(
            "regex",
            TransferDirection::Download,
            "v3",
            200_000,
            80,
            true,
        ),
        test_transfer(
            "my_lib",
            TransferDirection::Upload,
            "v3",
            2_000_000,
            500,
            true,
        ),
        test_transfer(
            "my_app",
            TransferDirection::Upload,
            "v3",
            3_000_000,
            700,
            true,
        ),
        test_transfer("fail_dl", TransferDirection::Download, "v3", 0, 50, false),
    ];
    for t in &transfers {
        events::log_transfer(&config.transfer_log_path(), t).unwrap();
    }

    config
}

/// A compile kache failed to store stays a miss (so the hit rate keeps
/// counting it and the miss table keeps showing it) and is additionally
/// broken out as a store failure, named in the suggestions and flagged in
/// the miss row (kunobi-ninja/kache#629).
#[test]
fn store_failures_are_visible_without_leaving_the_miss_accounting() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());

    let mut failed = test_event(
        "lint_crate",
        EventResult::Miss,
        60_000,
        59_000,
        4 * 1024 * 1024,
        "fff999",
    );
    failed.store_error = "refusing to cache zero-byte artifact: liblint.rmeta".to_string();
    events::log_event(&config.event_log_path(), &failed).unwrap();

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    assert_eq!(report.summary.store_failures, 1);
    // Still a miss: the compiler ran, and demoting it would drop it out of
    // both the denominator and the miss table.
    assert_eq!(report.summary.misses, 2);
    assert_eq!(report.summary.total_crates, 6);

    let row = report
        .top_misses
        .iter()
        .find(|c| c.crate_name == "lint_crate")
        .expect("a failed store is still listed as a compiled miss");
    assert_eq!(
        row.store_error,
        "refusing to cache zero-byte artifact: liblint.rmeta"
    );

    assert!(
        report
            .suggestions
            .iter()
            .any(|s| s.contains("failed to store") && s.contains("lint_crate")),
        "suggestions should name the crate: {:?}",
        report.suggestions
    );

    // A hit that somehow carries the field must not inflate the counter:
    // only a compiled outcome can be a compile that failed to store.
    let mut bogus = test_event("serde", EventResult::LocalHit, 5, 300, 1024, "abc123def456");
    bogus.store_error = "should not be counted".to_string();
    events::log_event(&config.event_log_path(), &bogus).unwrap();
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    assert_eq!(report.summary.store_failures, 1);

    let text = format_text(&report);
    assert!(text.contains("Compiled but not cached: 1"), "text: {text}");
    assert!(
        text.contains("[not cached: refusing to cache zero-byte artifact: liblint.rmeta]"),
        "the miss row should carry the reason: {text}"
    );
}

#[test]
fn test_generate_report_with_all_result_types() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    assert_eq!(report.summary.total_crates, 5); // excludes errors from cacheable count
    assert_eq!(report.summary.local_hits, 1);
    assert_eq!(report.summary.prefetch_hits, 1);
    assert_eq!(report.summary.remote_hits, 1);
    assert_eq!(report.summary.dups, 1);
    assert_eq!(report.summary.misses, 1);
    assert_eq!(report.summary.errors, 1);
    assert_eq!(report.summary.passthroughs, 1);
    assert_eq!(report.summary.skipped, 1);
    assert_eq!(report.summary.fallbacks, 1);
    assert_eq!(report.bypass.reasons.len(), 2);
    assert_eq!(report.timeline.event_count, 8);
    assert_eq!(report.timeline.cacheable_count, 5);
    assert_eq!(report.timeline.hit_count, 3);
    assert_eq!(report.timeline.compiled_count, 2);
    assert_eq!(report.timeline.passthrough_count, 1);
    assert_eq!(report.timeline.skipped_count, 1);
    assert_eq!(report.timeline.error_count, 1);
    assert!(report.timeline.duration_ms > 0);
    assert!(report.timeline.start_unix_ms.unwrap() <= report.timeline.end_unix_ms.unwrap());
    assert_eq!(report.trace_events.len(), 8);
    let serde_trace = report
        .trace_events
        .iter()
        .find(|event| event.args.crate_name == "serde")
        .unwrap();
    assert_eq!(serde_trace.cat, "kache");
    assert_eq!(serde_trace.ph, "X");
    // The display name carries the result label; the bare crate name stays
    // in args (#456).
    assert_eq!(serde_trace.name, "hit: serde");
    assert_eq!(serde_trace.cname.as_deref(), Some("good"));
    assert_eq!(serde_trace.dur, 5_000);
    assert_eq!(serde_trace.args.result, "local_hit");
    assert_eq!(serde_trace.args.cache_key, "abc123def456");
    assert_eq!(serde_trace.args.overhead_ms, 5);
    assert!(report.summary.hit_rate_pct > 0.0);
    assert!(report.summary.time_saved_ms > 0);
    let serde_event = report
        .all_events
        .iter()
        .find(|event| event.crate_name == "serde")
        .unwrap();
    assert!(!serde_event.start_time.is_empty());
    assert!(!serde_event.end_time.is_empty());
    assert_eq!(
        serde_event.end_unix_ms - serde_event.start_unix_ms,
        serde_event.elapsed_ms as i64
    );
    let passthrough_detail = report
        .bypass
        .slowest
        .iter()
        .find(|detail| detail.crate_name == "build.rs")
        .unwrap();
    assert_eq!(
        passthrough_detail.end_unix_ms - passthrough_detail.start_unix_ms,
        passthrough_detail.elapsed_ms as i64
    );
    let network = report.network.as_ref().unwrap();
    assert_eq!(network.v3_downloads, 3);
    assert_eq!(network.v2_downloads, 0);
    assert_eq!(network.total_get_requests, 12);
}

/// A probe / query (`category` == `not-a-compile`) must be counted as a
/// probe, NOT a passthrough — `passthroughs` is the actionable "compiles
/// we couldn't cache" signal, and a probe is not a compile. A real
/// refusal (`unsupported|…`) stays a passthrough.
#[test]
fn probe_events_split_out_of_passthroughs() {
    let mut probe = test_event("rustc", EventResult::Passthrough, 5, 0, 0, "");
    probe.passthrough_reason = "not-a-compile|query / probe (--print, -vV)".to_string();

    let mut refusal = test_event("a.c", EventResult::Passthrough, 90, 0, 0, "");
    refusal.passthrough_reason = "unsupported|cc link mode — not yet".to_string();

    let events = vec![probe, refusal];

    let bypass = build_bypass_analysis(&events, 10);
    assert_eq!(bypass.probes, 1, "the query/probe must count as a probe");
    assert_eq!(
        bypass.passthroughs, 1,
        "only the real refusal stays a passthrough"
    );

    let timeline = build_report_timeline(&events);
    assert_eq!(timeline.probe_count, 1);
    assert_eq!(timeline.passthrough_count, 1);

    // The summary line labels probes distinctly so a clean build's probe
    // traffic doesn't read as a caching problem.
    let summary = format_bypass_summary(&bypass);
    assert!(summary.contains("1 probe"), "got: {summary}");
    assert!(summary.contains("1 passthrough"), "got: {summary}");
}

const REPORT_SCHEMA_JSON: &str = include_str!("../report.schema.json");

fn check_numeric_bounds(val: &serde_json::Value, schema: &serde_json::Value, path: &str) {
    if let Some(num) = val.as_f64() {
        if let Some(min) = schema.get("minimum").and_then(|m| m.as_f64()) {
            assert!(num >= min, "expected >= {min} at {path}, got {num}");
        }
        if let Some(max) = schema.get("maximum").and_then(|m| m.as_f64()) {
            assert!(num <= max, "expected <= {max} at {path}, got {num}");
        }
        if let Some(ex_min) = schema.get("exclusiveMinimum").and_then(|m| m.as_f64()) {
            assert!(num > ex_min, "expected > {ex_min} at {path}, got {num}");
        }
        if let Some(ex_max) = schema.get("exclusiveMaximum").and_then(|m| m.as_f64()) {
            assert!(num < ex_max, "expected < {ex_max} at {path}, got {num}");
        }
    }
}

fn validate_json_value(
    val: &serde_json::Value,
    schema: &serde_json::Value,
    root_schema: &serde_json::Value,
    path: &str,
) {
    if let Some(ref_path) = schema.get("$ref").and_then(|r| r.as_str())
        && let Some(def_name) = ref_path.strip_prefix("#/definitions/")
    {
        let target_schema = &root_schema["definitions"][def_name];
        assert!(
            !target_schema.is_null(),
            "schema definition not found for {ref_path} at {path}"
        );
        return validate_json_value(val, target_schema, root_schema, path);
    }

    if let Some(expected_type) = schema.get("type") {
        if let Some(type_str) = expected_type.as_str() {
            match type_str {
                "object" => {
                    assert!(val.is_object(), "expected object at {path}, got {val:?}");
                    let obj = val.as_object().unwrap();
                    if let Some(required) = schema.get("required").and_then(|r| r.as_array()) {
                        for req_key in required {
                            let key_str = req_key.as_str().unwrap();
                            assert!(
                                obj.contains_key(key_str),
                                "missing required key '{key_str}' at {path}"
                            );
                        }
                    }
                    if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
                        for (prop_name, prop_val) in obj {
                            if let Some(prop_schema) = props.get(prop_name) {
                                validate_json_value(
                                    prop_val,
                                    prop_schema,
                                    root_schema,
                                    &format!("{path}.{prop_name}"),
                                );
                            }
                        }
                    }
                }
                "array" => {
                    assert!(val.is_array(), "expected array at {path}, got {val:?}");
                    if let Some(item_schema) = schema.get("items") {
                        for (i, elem) in val.as_array().unwrap().iter().enumerate() {
                            validate_json_value(
                                elem,
                                item_schema,
                                root_schema,
                                &format!("{path}[{i}]"),
                            );
                        }
                    }
                }
                "string" => {
                    assert!(val.is_string(), "expected string at {path}, got {val:?}");
                }
                "integer" => {
                    assert!(
                        val.is_i64() || val.is_u64(),
                        "expected integer at {path}, got {val:?}"
                    );
                    check_numeric_bounds(val, schema, path);
                }
                "number" => {
                    assert!(val.is_number(), "expected number at {path}, got {val:?}");
                    check_numeric_bounds(val, schema, path);
                }
                "boolean" => {
                    assert!(val.is_boolean(), "expected boolean at {path}, got {val:?}");
                }
                "null" => {
                    assert!(val.is_null(), "expected null at {path}, got {val:?}");
                }
                other => panic!("unhandled schema type '{other}' at {path}"),
            }
        } else if let Some(type_arr) = expected_type.as_array() {
            let allowed_types: Vec<&str> = type_arr.iter().filter_map(|t| t.as_str()).collect();
            let matches_any = allowed_types.iter().any(|&t| match t {
                "object" => val.is_object(),
                "array" => val.is_array(),
                "string" => val.is_string(),
                "integer" => val.is_i64() || val.is_u64(),
                "number" => val.is_number(),
                "boolean" => val.is_boolean(),
                "null" => val.is_null(),
                _ => false,
            });
            assert!(
                matches_any,
                "value at {path} ({val:?}) does not match any allowed type in {allowed_types:?}"
            );
            if val.is_number() {
                check_numeric_bounds(val, schema, path);
            }
            if val.is_object() && allowed_types.contains(&"object") {
                if let Some(required) = schema.get("required").and_then(|r| r.as_array()) {
                    let obj = val.as_object().unwrap();
                    for req_key in required {
                        let key_str = req_key.as_str().unwrap();
                        assert!(
                            obj.contains_key(key_str),
                            "missing required key '{key_str}' at {path}"
                        );
                    }
                }
                if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
                    let obj = val.as_object().unwrap();
                    for (prop_name, prop_val) in obj {
                        if let Some(prop_schema) = props.get(prop_name) {
                            validate_json_value(
                                prop_val,
                                prop_schema,
                                root_schema,
                                &format!("{path}.{prop_name}"),
                            );
                        }
                    }
                }
            }
        }
    }

    if let Some(enums) = schema.get("enum").and_then(|e| e.as_array()) {
        let is_allowed = enums.contains(val);
        assert!(
            is_allowed,
            "value at {path} ({val:?}) not in enum set {enums:?}"
        );
    }
}

#[test]
fn test_format_json_conforms_to_schema_contract() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let json = format_json(&report).unwrap();
    let val: serde_json::Value = serde_json::from_str(&json).unwrap();
    let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA_JSON).unwrap();

    validate_json_value(&val, &schema, &schema, "report");
}

#[test]
#[should_panic(expected = "expected <= 100 at report.summary.hit_rate_pct, got 500")]
fn test_validator_rejects_out_of_bounds_number_maximum() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let json = format_json(&report).unwrap();
    let mut val: serde_json::Value = serde_json::from_str(&json).unwrap();
    val["summary"]["hit_rate_pct"] = serde_json::json!(500.0);
    let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA_JSON).unwrap();

    validate_json_value(&val, &schema, &schema, "report");
}

#[test]
#[should_panic(expected = "expected >= 0 at report.summary.time_saved_ms, got -10")]
fn test_validator_rejects_out_of_bounds_integer_minimum() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let json = format_json(&report).unwrap();
    let mut val: serde_json::Value = serde_json::from_str(&json).unwrap();
    val["summary"]["time_saved_ms"] = serde_json::json!(-10);
    let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA_JSON).unwrap();

    validate_json_value(&val, &schema, &schema, "report");
}

#[test]
#[should_panic(expected = "not in enum set")]
fn test_validator_rejects_invalid_schema_version() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let json = format_json(&report).unwrap();
    let mut val: serde_json::Value = serde_json::from_str(&json).unwrap();
    val["schema_version"] = serde_json::json!(2);
    let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA_JSON).unwrap();

    validate_json_value(&val, &schema, &schema, "report");
}

#[test]
#[should_panic(expected = "missing required key 'schema_version' at report")]
fn test_validator_rejects_missing_required_key() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let json = format_json(&report).unwrap();
    let mut val: serde_json::Value = serde_json::from_str(&json).unwrap();
    val.as_object_mut().unwrap().remove("schema_version");
    let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA_JSON).unwrap();

    validate_json_value(&val, &schema, &schema, "report");
}

#[test]
fn test_json_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let json = format_json(&report).unwrap();
    let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(raw["schema_version"], 1);
    assert!(raw.get("traceEvents").is_some());
    assert!(raw.get("trace_events").is_none());
    assert_eq!(raw["displayTimeUnit"], "ms");

    let parsed: BuildReport = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.schema_version, REPORT_SCHEMA_VERSION);

    assert_eq!(parsed.summary.total_crates, report.summary.total_crates);
    assert_eq!(parsed.summary.misses, report.summary.misses);
    assert_eq!(parsed.top_misses.len(), report.top_misses.len());
    assert_eq!(parsed.timeline.event_count, report.timeline.event_count);
    assert_eq!(parsed.trace_events.len(), report.trace_events.len());
    assert_eq!(
        parsed.trace_events[0].args.result,
        report.trace_events[0].args.result
    );
    assert_eq!(
        parsed.all_events[0].start_unix_ms,
        report.all_events[0].start_unix_ms
    );
    assert_eq!(parsed.all_events[0].end_time, report.all_events[0].end_time);
}

/// #897: the report's window is the one requested, in counters, in
/// `meta`, and in every heading.
#[test]
fn report_window_narrows_events_and_labels_itself() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let mut stale = test_event("ancient", EventResult::Miss, 5000, 4800, 1024, "old");
    stale.ts = Utc::now() - chrono::Duration::hours(3);
    events::log_event(&config.event_log_path(), &stale).unwrap();

    let wide = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    let narrow = generate_report(&config, SinceWindow::parse("15m").unwrap(), 10).unwrap();
    assert_eq!(wide.summary.misses, narrow.summary.misses + 1);
    assert!(
        !narrow.all_events.iter().any(|e| e.crate_name == "ancient"),
        "a 3h-old event is outside a 15m window"
    );

    assert_eq!(narrow.meta.since, "15m");
    assert_eq!(narrow.meta.since_secs, 900);
    assert_eq!(narrow.meta.since_hours, 0, "whole hours, rounded down");
    assert_eq!(wide.meta.since_hours, 24);
    assert_eq!(wide.meta.since_secs, 86_400);

    assert!(format_text(&narrow).contains("kache build report (last 15m)"));
    assert!(format_markdown(&narrow).contains("| Window | last 15m |"));
    assert!(format_github(&narrow).contains("| **Window** | last 15m |"));
    assert!(format_github(&narrow).contains("· last 15m*"));
    let json: serde_json::Value = serde_json::from_str(&format_json(&narrow).unwrap()).unwrap();
    assert_eq!(json["meta"]["since"], "15m");
    assert_eq!(json["meta"]["since_secs"], 900);
}

/// A report written before `meta.since` existed still gets a heading.
#[test]
fn window_label_falls_back_to_whole_hours() {
    let meta: ReportMeta = serde_json::from_str(
        r#"{"kache_version":"0.1.0","generated_at":"2026-01-01T00:00:00Z","since_hours":6}"#,
    )
    .unwrap();
    assert_eq!(meta.window_label(), "6h");
    assert_eq!(meta.since_secs, 0);
    let current = ReportMeta {
        since: "15m".to_string(),
        ..meta
    };
    assert_eq!(current.window_label(), "15m");
}

fn session_event(root: &str, session: &str, second: i64, elapsed_ms: u64) -> BuildEvent {
    let mut event = test_event("fixture", EventResult::Miss, elapsed_ms, 10, 100, "key");
    event.root = root.to_string();
    event.session_id = session.to_string();
    event.ts = DateTime::from_timestamp(1_700_000_000 + second, 0).unwrap();
    event
}

#[test]
fn last_build_selects_recorded_root_and_id_by_timestamp() {
    let mut events = vec![
        session_event("/repo", "new", 900, 100),
        session_event("/repo/nested", "new", 899, 100),
        session_event("/other", "new", 898, 100),
        session_event("/repo", "old", 897, 100),
        session_event("/repo", "", 896, 100),
        session_event("/repo", "new", 0, 100),
    ];
    let session = select_last_build(&mut events).unwrap();
    assert_eq!(session.root, "/repo");
    assert_eq!(session.session_id, "new");
    assert!(!session.inferred);
    assert_eq!(session.inactivity_secs, 300);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].ts.timestamp(), 1_700_000_900);
    assert_eq!(events[1].ts.timestamp(), 1_700_000_000);
    assert!(session.description().contains("recorded new; root: /repo"));
}

#[test]
fn last_build_infers_local_activity_with_idle_and_recorded_boundaries() {
    for (old_id, old_second) in [("", 0), ("recorded", 299)] {
        let mut events = vec![
            session_event("/repo", "", 1_199, 0),
            session_event("/repo", old_id, old_second, 0),
            // A ten-minute compile overlaps the preceding activity: do not
            // split merely because its completion is more than 5m later.
            session_event("/repo", "", 900, 600_000),
            session_event("/repo", "", 300, 0),
            session_event("/other", "", 1_198, 0),
        ];
        let session = select_last_build(&mut events).unwrap();
        assert!(session.inferred);
        assert!(session.session_id.is_empty());
        assert_eq!(session.inactivity_secs, 300);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].ts.timestamp(), 1_700_000_300);
        assert_eq!(events[2].ts.timestamp(), 1_700_001_199);
        assert!(
            session
                .description()
                .contains("inferred from activity (300s idle gap)")
        );
    }
}

#[test]
fn last_build_does_not_fall_back_to_an_older_known_root() {
    assert!(
        select_last_build(&mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("No recorded")
    );
    let mut events = vec![
        session_event("/repo", "known", 0, 0),
        session_event("", "", 1, 0),
    ];
    assert!(
        select_last_build(&mut events)
            .unwrap_err()
            .to_string()
            .contains("--root")
    );
}

#[test]
fn last_build_report_uses_retained_history_and_omits_unscoped_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let root = dir.path().join("checkout-a").canonicalize().unwrap();
    let root_str = root.to_str().unwrap();
    let mut hit = session_event(root_str, "selected", 10, 100);
    hit.result = EventResult::LocalHit;
    hit.compile_time_ms = 4_000;
    hit.copied_bytes = 512;
    let mut miss = session_event(root_str, "selected", 0, 200);
    miss.crate_name = "compiled".to_string();
    let mut bypass = session_event(root_str, "selected", 5, 50);
    bypass.result = EventResult::Passthrough;
    bypass.passthrough_reason = "fixture bypass".to_string();
    let old = session_event(root_str, "old", -1, 0);
    let other = session_event("/other", "selected", 9, 0);
    let lines =
        [&hit, &miss, &bypass, &old, &other].map(|event| serde_json::to_string(event).unwrap());
    std::fs::write(config.event_log_path(), lines.join("\n") + "\n").unwrap();
    write_gc_stats(dir.path(), Utc::now());

    let report = generate_report_with_filter(
        &config,
        SinceWindow::DEFAULT,
        10,
        &ReportFilter {
            root: None,
            last_build: true,
        },
    )
    .unwrap();
    assert_eq!(report.meta.root_filter.as_deref(), Some(root_str));
    assert_eq!(report.meta.session.as_ref().unwrap().session_id, "selected");
    assert!(report.meta.since_secs > 86_400);
    assert_eq!(report.summary.local_hits, 1);
    assert_eq!(report.summary.misses, 1);
    assert_eq!(report.summary.passthroughs, 1);
    assert_eq!(report.summary.time_saved_ms, 4_000);
    assert_eq!(report.storage.restored_bytes, 512);
    assert_eq!(report.timeline.event_count, 3);
    assert_eq!(report.timeline.duration_ms, 10_200);
    assert!(report.network.is_none());
    assert!(report.gc.is_none());
    for text in [
        format_text(&report),
        format_markdown(&report),
        format_github(&report),
    ] {
        assert!(text.contains("last build session"));
        assert!(text.contains("recorded selected"));
        assert!(text.contains("May include multiple Cargo commands"));
        assert!(text.contains("fixture bypass"));
    }
    let json: serde_json::Value = serde_json::from_str(&format_json(&report).unwrap()).unwrap();
    assert_eq!(json["meta"]["session"]["inferred"], false);
    let trace: serde_json::Value =
        serde_json::from_str(&format_trace_json(&report).unwrap()).unwrap();
    assert_eq!(trace["session"]["session_id"], "selected");

    let absent = generate_report_with_filter(
        &config,
        SinceWindow::DEFAULT,
        10,
        &ReportFilter {
            root: Some(dir.path().join("missing")),
            last_build: true,
        },
    );
    assert!(absent.unwrap_err().to_string().contains("No recorded"));
}

#[test]
fn test_report_filters_by_root() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let root = dir.path().join("checkout-a");
    let root = root.canonicalize().unwrap();

    let report = generate_report_with_filter(
        &config,
        SinceWindow::DEFAULT,
        10,
        &ReportFilter {
            root: Some(root.clone()),
            ..ReportFilter::default()
        },
    )
    .unwrap();

    let root = root.to_string_lossy().into_owned();
    assert_eq!(report.meta.root_filter.as_deref(), Some(root.as_str()));
    assert_eq!(report.timeline.event_count, 7);
    assert_eq!(report.timeline.error_count, 0);
    assert!(report.network.is_none());
    assert!(
        report
            .suggestions
            .iter()
            .any(|s| s.contains("Remote transfer data omitted"))
    );
    assert!(report.all_events.iter().all(|event| event.root == root));
    assert!(
        report
            .trace_events
            .iter()
            .all(|event| event.args.root == root)
    );
}

#[test]
fn test_trace_json_format_is_minimal_chrome_trace_container() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let json = format_trace_json(&report).unwrap();
    let raw: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(raw.as_object().unwrap().len(), 2);
    assert_eq!(raw["displayTimeUnit"], "ms");
    assert!(raw.get("summary").is_none());
    assert!(raw.get("all_events").is_none());

    let arr = raw["traceEvents"].as_array().unwrap();
    // Metadata events (process_name + one thread_name per lane) are prepended
    // ahead of the rich `X` slices.
    let metadata: Vec<_> = arr.iter().filter(|e| e["ph"] == "M").collect();
    // Crate slices carry `cat: kache`; their nested phases `kache.phase`.
    let slices: Vec<_> = arr
        .iter()
        .filter(|e| e["ph"] == "X" && e["cat"] == "kache")
        .collect();
    assert_eq!(slices.len(), report.trace_events.len());
    assert!(
        arr.iter()
            .filter(|e| e["ph"] == "X")
            .all(|e| e["cat"] == "kache" || e["cat"] == "kache.phase"),
        "every X slice is a crate or one of its phases"
    );
    assert_eq!(arr[0]["name"], "process_name");
    assert_eq!(arr[0]["args"]["name"], "kache");
    assert!(
        metadata.iter().any(|e| e["name"] == "thread_name"),
        "lanes must be named via thread_name metadata"
    );
    // Result is surfaced in the slice name + color, not only in args.
    assert!(
        slices.iter().all(|e| {
            let n = e["name"].as_str().unwrap();
            n.starts_with("hit: ")
                || n.starts_with("miss: ")
                || n.starts_with("dup: ")
                || n.starts_with("passthrough: ")
                || n.starts_with("error: ")
                || n.starts_with("skipped: ")
        }),
        "every slice name must carry its result label"
    );
    assert!(slices.iter().all(|e| e.get("cname").is_some()));
}

#[test]
fn assign_trace_lanes_packs_concurrent_events() {
    // `event_start` = ts - elapsed_ms, so set ts (the end) to place each
    // slice on the timeline. Two overlapping events must get distinct lanes;
    // a later non-overlapping event reuses lane 0 instead of a fresh one.
    let base = Utc::now();
    let mk = |name: &str, result, key: &str, start_off_ms: i64, dur_ms: u64| {
        let mut e = test_event(name, result, dur_ms, 0, 0, key);
        e.ts = base + chrono::Duration::milliseconds(start_off_ms + dur_ms as i64);
        e
    };
    let a = mk("a", EventResult::LocalHit, "k1", 0, 100); // [0, 100)
    let b = mk("b", EventResult::Miss, "k2", 10, 100); // [10, 110), overlaps a
    let c = mk("c", EventResult::LocalHit, "k3", 200, 50); // [200, 250), after both

    let lanes = assign_trace_lanes(&[a, b, c]);
    assert_eq!(lanes[0], 0, "first event takes lane 0");
    assert_eq!(lanes[1], 1, "an overlapping event takes a fresh lane");
    assert_eq!(lanes[2], 0, "a non-overlapping later event reuses lane 0");
}

/// A miss with every phase set, values chosen so no two offsets coincide.
fn phase_heavy_miss(lane: u32) -> TraceEvent {
    let mut e = test_event("m", EventResult::Miss, 140, 100, 0, "k-miss");
    e.ts = Utc::now();
    e.startup_ms = 1;
    e.key_ms = 10;
    e.dep_info_ms = 4;
    e.dep_info_runs = 1;
    e.lookup_ms = 2;
    e.flight_wait_ms = 3;
    e.permit_wait_ms = 5;
    e.store_ms = 6;
    to_trace_event(&e, lane)
}

#[test]
fn to_trace_event_carries_the_phase_numbers_in_args() {
    let parent = phase_heavy_miss(3);
    let a = &parent.args;
    assert_eq!(a.startup_ms, 1);
    assert_eq!(a.key_ms, 10);
    assert_eq!(a.dep_info_ms, 4);
    assert_eq!(a.dep_info_runs, 1);
    assert_eq!(a.lookup_ms, 2);
    assert_eq!(a.flight_wait_ms, 3);
    assert_eq!(a.permit_wait_ms, 5);
    assert_eq!(a.wait_ms, 8);
    assert_eq!(a.restore_ms, 0);
    assert_eq!(a.store_ms, 6);
    assert_eq!(a.overhead_ms, 40);
    // 40 - (1 + 10 + 2 + 8 + 0 + 6)
    assert_eq!(a.unattributed_ms, 13);
}

#[test]
fn trace_phase_events_nest_in_wrapper_order_on_the_parent_lane() {
    let parent = phase_heavy_miss(3);
    let phases = trace_phase_events(&parent);
    let parent_end = parent.ts + parent.dur as i64;

    let shape: Vec<(&str, i64, u64, &str)> = phases
        .iter()
        .map(|p| {
            (
                p.name.as_str(),
                p.ts - parent.ts,
                p.dur,
                p.args.parent.as_str(),
            )
        })
        .collect();
    assert_eq!(
        shape,
        vec![
            ("startup", 0, 1_000, "miss: m"),
            ("key", 1_000, 10_000, "miss: m"),
            ("dep-info", 1_000, 4_000, "key"),
            ("lookup", 11_000, 2_000, "miss: m"),
            ("wait", 13_000, 8_000, "miss: m"),
            ("compile", 21_000, 100_000, "miss: m"),
            ("store", 121_000, 6_000, "miss: m"),
        ]
    );
    for phase in &phases {
        assert_eq!(phase.ph, "X");
        assert_eq!(phase.cat, "kache.phase");
        assert_eq!(phase.pid, parent.pid);
        assert_eq!(phase.tid, 3, "phases share the parent's lane");
        assert_eq!(phase.args.crate_name, "m");
        assert_eq!(phase.args.result, "miss");
        assert_eq!(phase.args.phase, phase.name);
        assert!(phase.ts >= parent.ts);
        assert!(
            phase.ts + phase.dur as i64 <= parent_end,
            "{} must not extend past its parent",
            phase.name
        );
    }
    let key = &phases[1];
    let dep_info = &phases[2];
    assert_eq!(
        dep_info.ts, key.ts,
        "dep-info is drawn at the key slice's start"
    );
    assert!(dep_info.ts + dep_info.dur as i64 <= key.ts + key.dur as i64);
    // The gap after the last phase is the unattributed remainder.
    let last = phases.last().unwrap();
    assert_eq!(
        parent_end - (last.ts + last.dur as i64),
        parent.args.unattributed_ms as i64 * 1000
    );
}

#[test]
fn trace_phase_events_skip_empty_phases_and_never_draw_a_hit_compile() {
    let mut e = test_event("h", EventResult::LocalHit, 20, 250, 0, "k-hit");
    e.ts = Utc::now();
    e.key_ms = 10;
    e.lookup_ms = 2;
    e.restore_ms = 5;
    let parent = to_trace_event(&e, 0);
    let names: Vec<String> = trace_phase_events(&parent)
        .into_iter()
        .map(|p| p.name)
        .collect();
    // No startup (0 ms), no wait, no compile: the 250 ms compile cost is
    // the stored one, not time this process spent.
    assert_eq!(names, vec!["key", "lookup", "restore"]);
    assert_eq!(parent.args.unattributed_ms, 3);
}

#[test]
fn trace_phase_events_clamp_to_the_parent_and_drop_what_follows() {
    // Phases sum to 24 ms inside a 10 ms parent: key fills [0, 8), lookup
    // is cut to [8, 10), restore has no room and is dropped.
    let mut e = test_event("c", EventResult::LocalHit, 10, 0, 0, "k-clamp");
    e.ts = Utc::now();
    e.key_ms = 8;
    e.lookup_ms = 8;
    e.restore_ms = 8;
    let parent = to_trace_event(&e, 1);
    let phases = trace_phase_events(&parent);
    let parent_end = parent.ts + parent.dur as i64;
    assert_eq!(phases.len(), 2, "{phases:#?}");
    assert_eq!(phases[0].name, "key");
    assert_eq!(phases[0].dur, 8_000);
    assert_eq!(phases[1].name, "lookup");
    assert_eq!(phases[1].ts, parent.ts + 8_000);
    assert_eq!(phases[1].dur, 2_000, "cut at the parent's end");
    assert_eq!(phases[1].args.phase_ms, 8, "args keep the recorded value");
    assert_eq!(phases[1].ts + phases[1].dur as i64, parent_end);
    assert_eq!(parent.args.unattributed_ms, 0);
}

#[test]
fn trace_phase_events_keep_dep_info_inside_key() {
    // Inconsistent event: dep-info time without key time. The nested
    // slice needs a key slice to live in, so it is clamped away.
    let mut e = test_event("d", EventResult::LocalHit, 10, 0, 0, "k-dep");
    e.ts = Utc::now();
    e.dep_info_ms = 5;
    e.dep_info_runs = 1;
    e.lookup_ms = 1;
    let parent = to_trace_event(&e, 0);
    let names: Vec<String> = trace_phase_events(&parent)
        .into_iter()
        .map(|p| p.name)
        .collect();
    assert_eq!(names, vec!["lookup".to_string()]);

    // With a shorter key than dep-info, the child is cut to the key.
    e.key_ms = 3;
    let parent = to_trace_event(&e, 0);
    let phases = trace_phase_events(&parent);
    assert_eq!(phases[0].name, "key");
    assert_eq!(phases[1].name, "dep-info");
    assert_eq!(phases[1].dur, 3_000);
    assert_eq!(phases[1].ts, phases[0].ts);
}

#[test]
fn trace_phase_events_are_empty_for_a_zero_length_parent() {
    let mut e = test_event("z", EventResult::Miss, 0, 0, 0, "k-zero");
    e.ts = Utc::now();
    e.key_ms = 3;
    let parent = to_trace_event(&e, 0);
    assert!(trace_phase_events(&parent).is_empty());
}

/// Two crates with every phase set, replacing the shared fixture so the
/// totals are exact: a hit (40 ms, 20 unattributed) and a miss (500 ms
/// with a 400 ms compile, 27 unattributed).
fn write_phase_events(dir: &std::path::Path) -> Config {
    let config = write_test_events(dir);
    events::clear_events(&config.event_log_path()).unwrap();
    let mut hit = test_event("h", EventResult::LocalHit, 40, 250, 10, "k-h");
    hit.startup_ms = 3;
    hit.key_ms = 10;
    hit.dep_info_ms = 6;
    hit.dep_info_runs = 1;
    hit.lookup_ms = 2;
    hit.restore_ms = 5;
    let mut miss = test_event("m", EventResult::Miss, 500, 400, 10, "k-m");
    miss.startup_ms = 4;
    miss.key_ms = 20;
    miss.dep_info_ms = 9;
    miss.dep_info_runs = 1;
    miss.lookup_ms = 1;
    miss.flight_wait_ms = 7;
    miss.permit_wait_ms = 11;
    miss.store_ms = 30;
    for e in [hit, miss] {
        events::log_event(&config.event_log_path(), &e).unwrap();
    }
    config
}

#[test]
fn a_report_says_when_rotation_cut_its_build_short() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = write_test_events(dir.path());
    let log = config.event_log_path();
    events::clear_events(&log).unwrap();
    let mut event = test_event("c", EventResult::LocalHit, 40, 250, 10, "k-c");
    event.session_id = "big-build".to_string();
    for _ in 0..40 {
        events::log_event(&log, &event).unwrap();
    }
    let line = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .next()
        .unwrap()
        .len() as u64
        + 1;
    config.event_log_max_size = line * 20;
    let notice = |config: &Config| {
        generate_report(config, SinceWindow::DEFAULT, 10)
            .unwrap()
            .suggestions
            .iter()
            .any(|s| s.contains("outgrew the event log"))
    };
    assert!(!notice(&config), "nothing rotated yet");

    events::rotate_if_needed(&log, config.event_log_max_size, 5).unwrap();
    assert!(notice(&config), "the build lost its earliest events");

    // A later build that fits leaves no notice for itself.
    events::clear_events(&log).unwrap();
    event.session_id = "small-build".to_string();
    events::log_event(&log, &event).unwrap();
    assert!(!notice(&config), "the cut build is no longer in the report");
}

#[test]
fn report_totals_the_wrapper_phases() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_phase_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    let t = &report.timing;
    assert_eq!(t.total_startup_ms, 7);
    assert_eq!(t.avg_startup_ms, 3.5);
    assert_eq!(t.total_dep_info_ms, 15);
    assert_eq!(t.dep_info_runs, 2);
    assert_eq!(t.avg_dep_info_ms, 7.5);
    assert_eq!(t.total_wait_ms, 18);
    assert_eq!(t.total_flight_wait_ms, 7);
    assert_eq!(t.total_permit_wait_ms, 11);
    assert_eq!(t.avg_wait_ms, 9.0);
    assert_eq!(t.total_unattributed_ms, 47);
    assert_eq!(t.avg_unattributed_ms, 23.5);
    // Unchanged neighbours, as a cross-check of the fixture.
    assert_eq!(t.total_key_ms, 30);
    assert_eq!(t.total_lookup_ms, 3);
    assert_eq!(t.total_restore_ms, 5);
    assert_eq!(t.total_store_ms, 30);

    let json: serde_json::Value = serde_json::from_str(&format_json(&report).unwrap()).unwrap();
    assert_eq!(json["timing"]["total_dep_info_ms"], 15);
    assert_eq!(json["timing"]["dep_info_runs"], 2);
    assert_eq!(json["timing"]["total_wait_ms"], 18);
    assert_eq!(json["timing"]["total_startup_ms"], 7);
    assert_eq!(json["timing"]["total_unattributed_ms"], 47);
    let miss_slice = json["traceEvents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "miss: m")
        .unwrap();
    assert_eq!(miss_slice["args"]["dep_info_ms"], 9);
    assert_eq!(miss_slice["args"]["wait_ms"], 18);
    assert_eq!(miss_slice["args"]["unattributed_ms"], 27);
    let schema: serde_json::Value = serde_json::from_str(REPORT_SCHEMA_JSON).unwrap();
    validate_json_value(&json, &schema, &schema, "report");
}

#[test]
fn text_markdown_and_github_reports_show_the_wrapper_phases() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_phase_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let text = format_text(&report);
    for line in [
        "  Startup: ~7ms aggregate (avg 3.5ms/crate)",
        "  Dep-info pre-pass: 2 runs, ~15ms aggregate (avg 7.5ms/run)",
        "  Scheduler wait: ~18ms aggregate (flight ~7ms, permit ~11ms)",
        "  Unattributed: ~47ms aggregate (avg 23.5ms/crate)",
    ] {
        assert!(text.contains(line), "text is missing {line:?}:\n{text}");
    }

    // Percentages are of tracked wrapper time: 40 + 500 = 540 ms.
    let md = format_markdown(&report);
    for row in [
        "| Startup | ~7ms | 1.3% |",
        "| Key computation | ~30ms | 5.6% |",
        "| &nbsp;&nbsp;of which dep-info pre-pass (2 runs) | ~15ms | 2.8% |",
        "| Lookup | ~3ms | 0.6% |",
        "| Scheduler wait (flight ~7ms, permit ~11ms) | ~18ms | 3.3% |",
        "| Restore | ~5ms | 0.9% |",
        "| Store | ~30ms | 5.6% |",
        "| Unattributed | ~47ms | 8.7% |",
    ] {
        assert!(md.contains(row), "markdown is missing {row:?}:\n{md}");
    }

    let gh = format_github(&report);
    for row in [
        "| Startup | ~7ms aggregate (avg 3.5ms/crate) |",
        "| Dep-info pre-pass | 2 runs, ~15ms aggregate (avg 7.5ms/run) |",
        "| Scheduler wait | ~18ms aggregate (flight ~7ms, permit ~11ms) |",
        "| Unattributed | ~47ms aggregate (avg 23.5ms/crate) |",
    ] {
        assert!(gh.contains(row), "github is missing {row:?}:\n{gh}");
    }
}

#[test]
fn reports_without_crates_show_no_phase_lines() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    events::clear_events(&config.event_log_path()).unwrap();
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    assert_eq!(report.summary.total_crates, 0);
    assert!(!format_text(&report).contains("Startup:"));
    assert!(!format_markdown(&report).contains("| Startup |"));
    assert!(!format_github(&report).contains("| Startup |"));
}

#[test]
fn trace_json_emits_each_crate_slice_followed_by_its_phases() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_phase_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    let json = format_trace_json(&report).unwrap();
    let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
    let arr = raw["traceEvents"].as_array().unwrap();

    let expected_phases: usize = report
        .trace_events
        .iter()
        .map(|e| trace_phase_events(e).len())
        .sum();
    assert_eq!(
        expected_phases,
        5 + 7,
        "hit: startup, key, dep-info, lookup, restore; miss: all seven"
    );
    let phases: Vec<_> = arr.iter().filter(|e| e["cat"] == "kache.phase").collect();
    assert_eq!(phases.len(), expected_phases);

    let miss_at = arr.iter().position(|e| e["name"] == "miss: m").unwrap();
    let miss = &arr[miss_at];
    let names: Vec<&str> = arr[miss_at + 1..miss_at + 8]
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "startup", "key", "dep-info", "lookup", "wait", "compile", "store"
        ]
    );
    for phase in &arr[miss_at + 1..miss_at + 8] {
        assert_eq!(phase["ph"], "X");
        assert_eq!(phase["tid"], miss["tid"]);
        assert_eq!(phase["pid"], miss["pid"]);
        assert_eq!(phase["args"]["crate_name"], "m");
        let end = phase["ts"].as_i64().unwrap() + phase["dur"].as_i64().unwrap();
        assert!(end <= miss["ts"].as_i64().unwrap() + miss["dur"].as_i64().unwrap());
    }
}

#[test]
fn avg_ms_rounds_to_one_decimal_and_survives_an_empty_count() {
    assert_eq!(avg_ms(7, 2), 3.5);
    assert_eq!(avg_ms(15, 2), 7.5);
    assert_eq!(avg_ms(7, 3), 2.3);
    assert_eq!(avg_ms(0, 3), 0.0);
    assert_eq!(avg_ms(7, 0), 0.0);
}

#[test]
fn pct_of_rounds_to_one_decimal_and_survives_an_empty_total() {
    assert_eq!(pct_of(25, 200), 12.5);
    assert_eq!(pct_of(7, 540), 1.3);
    assert_eq!(pct_of(540, 540), 100.0);
    assert_eq!(pct_of(0, 540), 0.0);
    assert_eq!(pct_of(7, 0), 0.0);
}

#[test]
fn trace_result_style_distinguishes_hit_and_miss() {
    assert_eq!(trace_result_style(EventResult::LocalHit), ("hit", "good"));
    assert_eq!(trace_result_style(EventResult::RemoteHit), ("hit", "good"));
    assert_eq!(trace_result_style(EventResult::Miss), ("miss", "bad"));
    assert_eq!(
        trace_result_style(EventResult::Passthrough),
        ("passthrough", "grey")
    );
    // Hit and miss must not share a color.
    assert_ne!(
        trace_result_style(EventResult::LocalHit).1,
        trace_result_style(EventResult::Miss).1
    );
}

#[test]
fn test_markdown_contains_sections() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let md = format_markdown(&report);
    assert!(md.contains("### kache build report"));
    assert!(md.contains("#### Summary"));
    assert!(md.contains("#### Timing"));
    assert!(md.contains("#### Remote transfer"));
    assert!(md.contains("#### Prefetch"));
    assert!(md.contains("#### Passthroughs & Skips"));
    assert!(md.contains("#### Top Compiled Cache-Key Misses"));
    assert!(md.contains("#### Suggestions"));
}

#[test]
fn test_missing_transfer_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.path().to_path_buf(),
        runtime_dir: dir.path().to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };

    // Only write build events, no transfers
    let event = test_event("serde", EventResult::LocalHit, 5, 300, 1024, "abc");
    events::log_event(&config.event_log_path(), &event).unwrap();

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    assert!(report.network.is_none());
    assert!(
        report
            .suggestions
            .iter()
            .any(|s| s.contains("No remote transfer"))
    );
}

#[test]
fn test_suggestion_high_miss_share() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.path().to_path_buf(),
        runtime_dir: dir.path().to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };

    // Mostly misses — should trigger high miss share suggestion
    for i in 0..10 {
        let e = test_event(
            &format!("miss_{i}"),
            EventResult::Miss,
            5000,
            4500,
            1024 * 1024,
            &format!("key_{i}"),
        );
        events::log_event(&config.event_log_path(), &e).unwrap();
    }
    let hit = test_event("hit", EventResult::LocalHit, 5, 100, 1024, "hk");
    events::log_event(&config.event_log_path(), &hit).unwrap();

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    assert!(
        report
            .suggestions
            .iter()
            .any(|s| s.contains("compile time spent on compiled cache-key misses"))
    );
}

#[test]
fn test_suggestion_high_hit_overhead() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.path().to_path_buf(),
        runtime_dir: dir.path().to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };

    // Several hits, each with a high elapsed time -> avg overhead > 50ms
    // triggers the "cache hit overhead" suggestion.
    for i in 0..5 {
        let e = test_event(
            &format!("hit_{i}"),
            EventResult::LocalHit,
            300, // elapsed_ms (overhead)
            100,
            1024 * 1024,
            &format!("hk_{i}"),
        );
        events::log_event(&config.event_log_path(), &e).unwrap();
    }

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    assert!(
        report
            .suggestions
            .iter()
            .any(|s| s.contains("cache hit overhead")),
        "expected hit-overhead suggestion: {:?}",
        report.suggestions
    );
}

#[test]
fn test_suggestion_network_download_failures_and_fanout() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.path().to_path_buf(),
        runtime_dir: dir.path().to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };

    // One OK download (test_transfer sets request_count=4 -> 4 GETs for 1 hit,
    // > 3x, triggers the fan-out suggestion) plus two failed downloads
    // (>10% failure rate triggers the network-failure suggestion).
    let transfers = [
        test_transfer("ok_dl", TransferDirection::Download, "v3", 1000, 80, true),
        test_transfer("bad1", TransferDirection::Download, "v3", 0, 50, false),
        test_transfer("bad2", TransferDirection::Download, "v3", 0, 50, false),
    ];
    for t in &transfers {
        events::log_transfer(&config.transfer_log_path(), t).unwrap();
    }

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    let joined = report.suggestions.join("\n");
    assert!(
        joined.contains("downloads failed"),
        "expected download-failure suggestion: {:?}",
        report.suggestions
    );
    assert!(
        joined.contains("remote reads per cache hit"),
        "expected remote-read fan-out suggestion: {:?}",
        report.suggestions
    );
}

#[test]
fn test_suggestion_network_latency_thresholds() {
    // A download with high semaphore wait, open/setup latency exceeding
    // read/transfer time, and extract time exceeding read/transfer time triggers the
    // three latency-threshold suggestions (report.rs 999-1018) that the
    // fixed-ratio test_transfer helper can't reach.
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.path().to_path_buf(),
        runtime_dir: dir.path().to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };

    let slow = TransferEvent {
        accounting: None,
        prefetch: None,
        outcome: String::new(),
        schema: 3,
        crate_name: "slow".to_string(),
        direction: TransferDirection::Download,
        format: "v3".to_string(),
        cache_key: "slow-key".to_string(),
        object_key: "prefix/v3/packs/slow/slow-key.tar.zst".to_string(),
        compressed_bytes: 1000,
        started_at_unix_ms: 0,
        finished_at_unix_ms: 0,
        elapsed_ms: 80_000,
        network_ms: 40_000,
        semaphore_wait_ms: 11_000, // > 10s -> semaphore-wait suggestion
        head_ms: 0,
        request_ms: 31_000, // > 30s AND > body_ms -> request-latency suggestion
        body_ms: 1_000,
        request_count: 1,
        original_bytes: 3000,
        decompress_ms: 0,
        extract_ms: 31_000, // > 30s AND > body_ms -> extract-time suggestion
        disk_io_ms: 0,
        import_lock_wait_ms: 0,
        import_ms: 0,
        compression_ms: 0,
        head_checks_ms: 0,
        blobs_skipped: 0,
        blobs_total: 2,
        ok: true,
        timestamp: Utc::now().timestamp() as u64,
    };
    events::log_transfer(&config.transfer_log_path(), &slow).unwrap();

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    let joined = report.suggestions.join("\n");
    assert!(
        joined.contains("semaphore wait"),
        "expected semaphore-wait suggestion: {:?}",
        report.suggestions
    );
    assert!(
        joined.contains("remote open/setup latency"),
        "expected open-latency suggestion: {:?}",
        report.suggestions
    );
    assert!(
        joined.contains("archive extract time"),
        "expected extract-time suggestion: {:?}",
        report.suggestions
    );
}

#[test]
fn test_github_format_has_collapsible_sections() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let gh = format_github(&report);
    assert!(gh.contains("### kache build cache"));
    assert!(gh.contains("kache-action"));
    // Key metrics always visible
    assert!(gh.contains("**Crates**"));
    assert!(gh.contains("**Hit rate**"));
    assert!(gh.contains("**Compile work avoided**"));
    assert!(gh.contains("**Cache hit overhead**"));
    assert!(gh.contains("**Cache ROI**"));
    assert!(gh.contains("**Passthroughs / skipped**"));
    // Details in collapsible sections
    assert!(gh.contains("<details>"));
    assert!(gh.contains("<summary><strong>Top compiled cache-key misses</strong>"));
    assert!(gh.contains("<summary><strong>Passthroughs & skips</strong>"));
    assert!(gh.contains("via fallback"));
    assert!(gh.contains("refused: unsupported rustc invocation"));
    assert!(gh.contains("<summary><strong>Remote transfer</strong>"));
    assert!(gh.contains("<summary><strong>Timing & Prefetch</strong>"));
    assert!(gh.contains("Download format"));
    assert!(gh.contains("Read fan-out"));
    assert!(gh.contains("v3 3"));
    assert!(gh.contains("open"));
    assert!(gh.contains("read"));
}

#[test]
fn test_text_output() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    let text = format_text(&report);
    assert!(text.contains("kache build report"));
    assert!(text.contains("hit rate"));
    assert!(text.contains("Timing:"));
    assert!(text.contains("Remote transfer:"));
    assert!(text.contains("Passthroughs/skips:"));
}

#[test]
fn render_includes_storage_and_gc_sections_when_present() {
    // generate_report from synthetic events yields no storage/gc data, so
    // the has_storage_data and gc=Some render branches stay cold. Populate
    // them on a generated report and confirm all three formats render the
    // Storage and GC sections (markdown:1415/text:2111/github:1853 + GC).
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let mut report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    report.storage = StorageBreakdown {
        reflinked_bytes: 1024,
        hardlinked_bytes: 512,
        copied_bytes: 256,
        restored_bytes: 1792,
        zero_copy_pct: 85.7,
        store_blobs: 4,
        logical_bytes: 4096,
        blob_bytes: 2048,
        dedup_saved_bytes: 2048,
        accounting_consistent: true,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
    };
    report.gc = Some(GcSummary {
        last_run: "2026-06-19T12:00:00+00:00".to_string(),
        entries_evicted: 7,
        bytes_freed: 9000,
        disk_bytes_reclaimed: 4000,
        shared_bytes_retained: 5000,
        blobs_removed: 3,
    });

    for rendered in [format_markdown(&report), format_github(&report)] {
        assert!(rendered.contains("Storage"), "missing Storage section");
    }
    let text = format_text(&report);
    assert!(text.contains("Storage:"), "text missing Storage section");
    // GC summary surfaces its evicted-entry count in every format.
    for rendered in [
        format_markdown(&report),
        format_github(&report),
        format_text(&report),
    ] {
        let lower = rendered.to_lowercase();
        assert!(
            lower.contains("evicted") && lower.contains('7'),
            "GC section with evicted count should appear"
        );
    }
}

#[test]
fn render_network_and_error_sections_with_all_optional_fields() {
    // Synthetic events from write_test_events yield a network section
    // without uploads, compression, blob-dedup, failures, the dominant
    // cumulative phase, or an error table — so those optional rows stay
    // cold in all three renderers. Populate a fully-loaded NetworkAnalysis
    // plus an errors_detail list on a generated report and confirm every
    // format surfaces the upload, compression, dedup, failure, dominant-
    // phase, cumulative-phase, GET-fan-out, and error-table branches.
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let mut report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();

    report.network = Some(NetworkAnalysis {
        configured_backend: "s3".to_string(),
        bytes_up: 5 * 1024 * 1024,
        bytes_down: 20 * 1024 * 1024,
        uploads_ok: 4,
        uploads_failed: 2,
        downloads_ok: 10,
        downloads_failed: 3,
        avg_download_ms: 42.0,
        p95_download_ms: 90,
        max_download_ms: 120,
        throughput_mbps: 50.0,
        observed_bytes_down: 20 * 1024 * 1024,
        observed_downloads: 10,
        observed_span_ms: 300,
        observed_throughput_mbps: 66.7,
        max_concurrent_downloads: 4,
        network_throughput_mbps: 60.0,
        body_throughput_mbps: 70.0,
        dominant_download_phase: "body".to_string(),
        dominant_download_phase_ms: 800,
        dominant_download_phase_pct: 55.5,
        total_request_ms: 100,
        total_body_ms: 800,
        total_semaphore_wait_ms: 30,
        total_head_ms: 40,
        total_get_requests: 25,
        compression_ratio: 3.2,
        original_bytes_down: 64 * 1024 * 1024,
        total_decompress_ms: 50,
        total_extract_ms: 60,
        total_disk_io_ms: 70,
        total_import_ms: 80,
        total_import_lock_wait_ms: 35,
        total_compression_ms: 15,
        total_head_checks_ms: 25,
        blobs_skipped: 6,
        blobs_total: 10,
        v1_downloads: 1,
        v2_downloads: 2,
        v3_downloads: 7,
        unknown_format_downloads: 1,
        slowest_downloads: vec![TransferDetail {
            crate_name: "serde".to_string(),
            direction: "download".to_string(),
            format: "v3".to_string(),
            cache_key: "abcdef0123456789deadbeef".to_string(),
            object_key: "rust/abc/serde".to_string(),
            compressed_bytes: 2 * 1024 * 1024,
            started_at_unix_ms: 1_000,
            finished_at_unix_ms: 1_120,
            elapsed_ms: 120,
            network_ms: 100,
            semaphore_wait_ms: 5,
            head_ms: 6,
            request_ms: 7,
            body_ms: 80,
            decompress_ms: 9,
            extract_ms: 10,
            disk_io_ms: 11,
            import_lock_wait_ms: 4,
            import_ms: 12,
            request_count: 4,
            blobs_skipped: 2,
            blobs_total: 5,
            throughput_mbps: 40.0,
            ok: true,
        }],
    });
    report.errors_detail = vec![ErrorDetail {
        crate_name: "boom".to_string(),
        cache_key: "f00dcafef00dcafe".to_string(),
        timestamp: "2026-06-19T12:00:00+00:00".to_string(),
    }];

    let positive_markdown = format_markdown(&report);
    let positive_github = format_github(&report);
    let positive_text = format_text(&report);
    assert!(
        positive_github.contains("67 MB/s observed wall span"),
        "GitHub summary must prefer the observed wall-span rate: {positive_github}"
    );
    for rendered in [positive_markdown, positive_github, positive_text] {
        let lower = rendered.to_lowercase();
        // Upload row (uploads_ok > 0) and its compression/existence split.
        assert!(
            lower.contains("upload"),
            "missing upload section: {rendered}"
        );
        // Compression ratio row (compression_ratio > 0).
        assert!(
            lower.contains("compress"),
            "missing compression row: {rendered}"
        );
        // Blob dedup row (blobs_total > 0).
        assert!(
            lower.contains("dedup"),
            "missing blob dedup row: {rendered}"
        );
        // The slowest download crate is listed.
        assert!(
            lower.contains("serde"),
            "missing slowest download: {rendered}"
        );
        // The error table lists the failing crate.
        assert!(lower.contains("boom"), "missing error entry: {rendered}");
        assert!(
            lower.contains("observed wall-span"),
            "missing observed throughput label: {rendered}"
        );
        assert!(
            lower.contains("66.7 mb/s") && !lower.contains("unavailable"),
            "nonzero observed span must render the measured rate: {rendered}"
        );
        assert!(
            lower.contains("cumulative service-time"),
            "missing cumulative-rate label: {rendered}"
        );
        assert!(
            lower.contains("import lock"),
            "missing separate import lock timing: {rendered}"
        );
        assert!(
            !lower.contains("dominant aggregate") && !lower.contains("aggregate download phase"),
            "remote service-time totals must not be labeled aggregate: {rendered}"
        );
    }

    let network = report.network.as_mut().unwrap();
    network.observed_bytes_down = 0;
    network.observed_downloads = 0;
    network.observed_span_ms = 0;
    network.observed_throughput_mbps = 0.0;
    network.max_concurrent_downloads = 0;
    network.dominant_download_phase.clear();
    network.dominant_download_phase_ms = 0;
    network.dominant_download_phase_pct = 0.0;
    network.total_request_ms = 0;
    network.total_body_ms = 0;
    network.total_semaphore_wait_ms = 0;
    network.total_head_ms = 0;
    network.total_decompress_ms = 0;
    network.total_extract_ms = 0;
    network.total_disk_io_ms = 0;
    network.total_import_ms = 0;
    network.total_import_lock_wait_ms = 35;

    let legacy_markdown = format_markdown(&report);
    let legacy_github = format_github(&report);
    let legacy_text = format_text(&report);
    assert!(
        legacy_github.contains("70 MB/s cumulative read service"),
        "GitHub summary must label the legacy fallback as cumulative: {legacy_github}"
    );
    for rendered in [legacy_markdown, legacy_github, legacy_text] {
        let lower = rendered.to_lowercase();
        assert!(
            lower.contains("observed wall-span throughput") && lower.contains("unavailable"),
            "zero observed span must render the legacy-event fallback: {rendered}"
        );
        assert!(
            lower.contains("import lock wait 35ms"),
            "isolated import-lock timing must render the phase row: {rendered}"
        );
    }

    let network = report.network.as_mut().unwrap();
    network.downloads_ok = 0;
    network.total_import_lock_wait_ms = 0;
    for rendered in [
        format_markdown(&report),
        format_github(&report),
        format_text(&report),
    ] {
        let lower = rendered.to_lowercase();
        assert!(
            !lower.contains("unavailable (legacy transfer events)"),
            "no downloads must not claim a legacy throughput fallback: {rendered}"
        );
        assert!(
            !lower.contains("cumulative download phase time")
                && !lower.contains("cumulative phase time:"),
            "all-zero phase totals must omit the phase row: {rendered}"
        );
    }
}

#[test]
fn test_empty_report() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.path().to_path_buf(),
        runtime_dir: dir.path().to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    assert_eq!(report.summary.total_crates, 0);
    assert_eq!(report.summary.hit_rate_pct, 0.0);
    assert!(report.network.is_none());
    assert!(report.top_misses.is_empty());
}

#[test]
fn test_format_bytes() {
    assert_eq!(format_bytes(0), "0 B");
    assert_eq!(format_bytes(512), "512 B");
    assert_eq!(format_bytes(1024), "1.0 KB");
    assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
    assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
}

#[test]
fn test_markdown_cell_escapes_pipes_and_newlines() {
    assert_eq!(markdown_cell("a|b"), "a\\|b");
    assert_eq!(markdown_cell("line1\nline2"), "line1 line2");
    assert_eq!(markdown_cell("plain"), "plain");
}

#[test]
fn test_format_exit_code() {
    assert_eq!(format_exit_code(Some(0)), "0");
    assert_eq!(format_exit_code(Some(101)), "101");
    assert_eq!(format_exit_code(None), "-");
}

#[test]
fn test_bypass_total_sums_passthroughs_probes_and_skipped() {
    let bypass = BypassAnalysis {
        passthroughs: 3,
        probes: 1,
        skipped: 2,
        ..Default::default()
    };
    assert_eq!(bypass_total(&bypass), 6);
    assert_eq!(bypass_total(&BypassAnalysis::default()), 0);
}

#[test]
fn test_format_bypass_summary_variants() {
    assert_eq!(format_bypass_summary(&BypassAnalysis::default()), "none");

    // Singular vs plural, with fallback breakdown.
    let one = BypassAnalysis {
        passthroughs: 1,
        ..Default::default()
    };
    assert_eq!(format_bypass_summary(&one), "1 passthrough");

    let many = BypassAnalysis {
        passthroughs: 3,
        fallbacks: 2,
        skipped: 1,
        ..Default::default()
    };
    assert_eq!(
        format_bypass_summary(&many),
        "3 passthroughs (2 via fallback) / 1 skipped"
    );
}

#[test]
fn fallback_recovery_survives_report_conversion_and_formatting() {
    let mut event = test_event("fixture", EventResult::Passthrough, 5, 0, 0, "");
    event.passthrough_reason = "unsupported|assembly".into();
    event.exit_code = Some(0);
    let plain = to_bypass_detail(&event);
    assert_eq!(bypass_detail_reason(&plain), "unsupported|assembly");
    event.fallback_attempt = Some(crate::fallback::Attempt {
        wrapper: "sccache".into(),
        outcome: crate::fallback::Outcome::Failed,
        exit_code: Some(42),
        detail: "exit status: 42".into(),
    });
    let detail = to_bypass_detail(&event);
    assert_eq!(detail.fallback_attempt, event.fallback_attempt);
    assert_eq!(detail.route, "direct");
    assert_eq!(detail.exit_code, Some(0));
    assert_eq!(
        bypass_detail_reason(&detail),
        "unsupported|assembly; fallback `sccache`: exit status: 42"
    );
    let json = serde_json::to_value(&detail).unwrap();
    assert_eq!(json["fallback_attempt"]["exit_code"], 42);
    let mut lines = Vec::new();
    push_bypass_tables(
        &mut lines,
        &BypassAnalysis {
            slowest: vec![detail],
            ..Default::default()
        },
    );
    assert!(
        lines
            .join("\n")
            .contains("fallback `sccache`: exit status: 42")
    );
}

#[test]
fn test_bypass_route_reflects_result_and_fallback() {
    let mut e = test_event("c", EventResult::Passthrough, 1, 0, 0, "k");
    assert_eq!(bypass_route(&e), "direct");
    e.fallback = true;
    assert_eq!(bypass_route(&e), "fallback");
    e.result = EventResult::Skipped;
    assert_eq!(bypass_route(&e), "skipped");
    e.result = EventResult::Miss;
    assert_eq!(bypass_route(&e), "n/a");
}

#[test]
fn test_bypass_reason_defaults_to_unknown() {
    let mut e = test_event("c", EventResult::Passthrough, 1, 0, 0, "k");
    assert_eq!(bypass_reason(&e), "unknown");
    e.passthrough_reason = "  linker invocation  ".to_string();
    assert_eq!(bypass_reason(&e), "linker invocation");
}

#[test]
fn test_build_bypass_analysis_counts_groups_and_sorts() {
    let mut direct = test_event("a", EventResult::Passthrough, 30, 0, 0, "k");
    direct.passthrough_reason = "linker".to_string();

    let mut fallback = test_event("b", EventResult::Passthrough, 50, 0, 0, "k");
    fallback.passthrough_reason = "linker".to_string();
    fallback.fallback = true;

    let mut skipped = test_event("c", EventResult::Skipped, 5, 0, 0, "k");
    skipped.passthrough_reason = "disabled".to_string();

    // A non-bypass event must be ignored entirely.
    let miss = test_event("d", EventResult::Miss, 999, 0, 0, "k");

    let analysis = build_bypass_analysis(&[direct, fallback, skipped, miss], 10);

    assert_eq!(analysis.passthroughs, 2);
    assert_eq!(analysis.skipped, 1);
    assert_eq!(analysis.fallbacks, 1);
    assert_eq!(analysis.direct_passthroughs, 1);
    // "linker" appears under two different routes (direct + fallback), so it
    // groups into two reason rows; "disabled" is a third.
    assert_eq!(analysis.reasons.len(), 3);
    // Slowest-first ordering: the 50ms fallback leads.
    assert_eq!(analysis.slowest.first().unwrap().elapsed_ms, 50);
    assert!(analysis.slowest.iter().all(|d| d.crate_name != "d"));
}

#[test]
fn not_found_transfers_are_not_download_failures() {
    let mut missing = test_transfer("gone", TransferDirection::Download, "v3", 0, 5, false);
    missing.outcome = "not_found".to_string();
    let network = build_network_analysis(&[missing.clone()], 10);
    assert_eq!(network.downloads_failed, 0);
    assert_eq!(network.downloads_ok, 0);
    assert_eq!(network.bytes_down, 0);
    missing.outcome = "error".to_string();
    assert_eq!(
        build_network_analysis(&[missing.clone()], 10).downloads_failed,
        1
    );
    missing.outcome = "skipped".to_string();
    missing.request_count = 0;
    let skipped = build_network_analysis(&[missing], 10);
    assert_eq!(skipped.downloads_failed, 0);
    assert_eq!(skipped.downloads_ok, 0);
    assert_eq!(skipped.bytes_down, 0);
}

#[test]
fn packed_list_and_cancellation_do_not_diagnose_download_failures() {
    let mut list = test_transfer(
        "catalog",
        TransferDirection::Download,
        "pack_catalog",
        0,
        5,
        false,
    );
    list.accounting = Some(kache_core::timeline::PrefetchAccounting {
        operation: kache_core::timeline::PrefetchOperation::List,
        ..Default::default()
    });
    let mut cancelled = test_transfer("pack", TransferDirection::Download, "pack", 100, 5, false);
    cancelled.outcome = "cancelled".to_owned();
    assert_eq!(
        build_network_analysis(&[list.clone(), cancelled], 10).downloads_failed,
        0
    );
    list.ok = true;
    assert_eq!(build_network_analysis(&[list], 10).downloads_ok, 0);
}

#[test]
fn test_build_network_analysis_aggregates_transfers() {
    let transfers = vec![
        test_transfer("serde", TransferDirection::Upload, "v3", 1_000, 40, true),
        test_transfer("tokio", TransferDirection::Upload, "v3", 0, 10, false), // failed
        test_transfer("regex", TransferDirection::Download, "v3", 2_000, 80, true),
        test_transfer("syn", TransferDirection::Download, "v3", 0, 5, false), // failed
    ];
    let na = build_network_analysis(&transfers, 10);
    assert_eq!(na.uploads_ok, 1);
    assert_eq!(na.uploads_failed, 1);
    assert_eq!(na.downloads_ok, 1);
    assert_eq!(na.downloads_failed, 1);
    assert_eq!(na.bytes_up, 1_000);
    assert_eq!(na.bytes_down, 2_000);
    assert!(na.max_download_ms >= 80);
}

#[test]
fn network_analysis_distinguishes_observed_span_from_cumulative_service_time() {
    let mib = 1024 * 1024;
    let mut first = test_transfer(
        "first",
        TransferDirection::Download,
        "v3",
        10 * mib,
        2_000,
        true,
    );
    first.started_at_unix_ms = 1_000;
    first.finished_at_unix_ms = 3_000;
    first.import_lock_wait_ms = 125;
    first.import_ms = 250;

    let mut second = test_transfer(
        "second",
        TransferDirection::Download,
        "v3",
        20 * mib,
        2_000,
        true,
    );
    second.started_at_unix_ms = 2_000;
    second.finished_at_unix_ms = 4_000;
    second.import_lock_wait_ms = 375;
    second.import_ms = 500;

    let analysis = build_network_analysis(&[first, second], 10);

    assert_eq!(analysis.observed_span_ms, 3_000);
    assert_eq!(analysis.observed_throughput_mbps, 10.0);
    assert_eq!(analysis.max_concurrent_downloads, 2);
    assert_eq!(analysis.throughput_mbps, 7.5);
    assert_eq!(analysis.total_import_lock_wait_ms, 500);
    assert_eq!(analysis.total_import_ms, 750);
}

#[test]
fn network_analysis_rejects_invalid_wall_intervals() {
    let mib = 1024 * 1024;
    let mut valid = test_transfer(
        "valid",
        TransferDirection::Download,
        "v3",
        4 * mib,
        1_000,
        true,
    );
    valid.started_at_unix_ms = 2_000;
    valid.finished_at_unix_ms = 3_000;

    let mut zero_start = valid.clone();
    zero_start.crate_name = "zero-start".to_string();
    zero_start.started_at_unix_ms = 0;

    let mut zero_length = valid.clone();
    zero_length.crate_name = "zero-length".to_string();
    zero_length.finished_at_unix_ms = zero_length.started_at_unix_ms;

    let mut reversed = valid.clone();
    reversed.crate_name = "reversed".to_string();
    reversed.finished_at_unix_ms = reversed.started_at_unix_ms - 1;

    let analysis = build_network_analysis(&[valid, zero_start, zero_length, reversed], 10);

    assert_eq!(analysis.observed_downloads, 1);
    assert_eq!(analysis.observed_bytes_down, 4 * mib);
    assert_eq!(analysis.observed_span_ms, 1_000);
    assert_eq!(analysis.observed_throughput_mbps, 4.0);
    assert_eq!(analysis.max_concurrent_downloads, 1);
}

#[test]
fn network_analysis_disk_fallback_subtracts_only_v3_import_timing() {
    let mut timed = test_transfer(
        "timed",
        TransferDirection::Download,
        "v3",
        1024,
        1_000,
        true,
    );
    timed.network_ms = 400;
    timed.decompress_ms = 100;
    timed.extract_ms = 75;
    timed.import_lock_wait_ms = 50;
    timed.import_ms = 25;
    timed.disk_io_ms = 0;

    let timed_analysis = build_network_analysis(&[timed.clone()], 10);
    assert_eq!(timed_analysis.total_disk_io_ms, 350);
    assert_eq!(timed_analysis.total_import_lock_wait_ms, 50);
    assert_eq!(timed_analysis.total_import_ms, 25);

    timed.schema = 2;
    let legacy_analysis = build_network_analysis(&[timed], 10);
    assert_eq!(legacy_analysis.total_disk_io_ms, 425);
    assert_eq!(legacy_analysis.total_import_lock_wait_ms, 50);
    assert_eq!(legacy_analysis.total_import_ms, 25);
}

#[test]
fn transfer_event_v2_deserializes_without_v3_timing_fields() {
    let event: TransferEvent = serde_json::from_value(serde_json::json!({
        "schema": 2,
        "crate_name": "legacy",
        "direction": "download",
        "compressed_bytes": 1024,
        "elapsed_ms": 10,
        "ok": true,
        "timestamp": 123
    }))
    .unwrap();

    assert_eq!(event.started_at_unix_ms, 0);
    assert_eq!(event.finished_at_unix_ms, 0);
    assert_eq!(event.import_lock_wait_ms, 0);
}

#[test]
fn test_push_storage_table_renders_rows() {
    let storage = StorageBreakdown {
        reflinked_bytes: 800,
        hardlinked_bytes: 150,
        copied_bytes: 50,
        restored_bytes: 1000,
        zero_copy_pct: 95.0,
        store_blobs: 12,
        logical_bytes: 5000,
        blob_bytes: 3000,
        dedup_saved_bytes: 2000,
        accounting_consistent: true,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
    };
    let mut lines = Vec::new();
    push_storage_table(&mut lines, &storage);
    let joined = lines.join("\n");
    assert!(joined.contains("Restored bytes"));
    assert!(joined.contains("Zero-copy restores"));
    assert!(joined.contains("Store footprint"));
    assert!(joined.contains("Store blobs"));
}

#[test]
fn report_counts_each_download_transfer_format() {
    // Downloads logged with v1 / v2 / unknown formats exercise the
    // format-match arms in the transfer aggregation (not just the v3 arm
    // the other tests use).
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: false,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_clean_idle_targets_days: 0,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        cache_dir: dir.path().to_path_buf(),
        runtime_dir: dir.path().to_path_buf(),
        max_size: 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        trust_codegen_backends: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    };
    for fmt in ["v1", "v2", "weird-future-format"] {
        let t = test_transfer(fmt, TransferDirection::Download, fmt, 500, 40, true);
        events::log_transfer(&config.transfer_log_path(), &t).unwrap();
    }

    // Must aggregate without panicking across all format arms.
    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    assert!(
        report.network.is_some(),
        "downloads should yield network analysis"
    );
}

/// Copy-fallback reason rows render only when their guard sums are
/// positive, with each reason's own bytes. Any arithmetic change to the
/// sums hides a row or panics on underflow in debug builds.
#[test]
fn push_storage_table_renders_copy_reason_sums() {
    let storage = StorageBreakdown {
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        restored_bytes: 0,
        zero_copy_pct: 0.0,
        store_blobs: 0,
        logical_bytes: 0,
        blob_bytes: 0,
        dedup_saved_bytes: 0,
        accounting_consistent: true,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 100,
        store_copy_permission_bytes: 200,
        store_copy_ineligible_bytes: 300,
        store_copy_other_bytes: 400,
        restore_copy_cross_device_bytes: 100,
        restore_copy_permission_bytes: 200,
        restore_copy_exclusive_bytes: 400,
        restore_copy_other_bytes: 800,
    };
    let mut lines = Vec::new();
    push_storage_table(&mut lines, &storage);
    let joined = lines.join("\n");
    assert!(
        joined.contains("Store copy reasons"),
        "nonzero store reasons must render: {joined}"
    );
    assert!(
        joined.contains("Restore copy reasons"),
        "nonzero restore reasons must render: {joined}"
    );
    assert!(
        joined.contains(&format_bytes(100))
            && joined.contains(&format_bytes(200))
            && joined.contains(&format_bytes(300))
            && joined.contains(&format_bytes(400))
            && joined.contains(&format_bytes(800)),
        "each reason's own bytes must appear: {joined}"
    );
}

#[test]
fn copy_reason_bytes_total_adds_all_four_terms() {
    assert_eq!(copy_reason_bytes_total(0, 0, 0, 0), 0);
    assert_eq!(copy_reason_bytes_total(1, 0, 0, 0), 1);
    assert_eq!(copy_reason_bytes_total(1, 2, 4, 8), 15);
    assert_eq!(copy_reason_bytes_total(100, 200, 300, 400), 1000);
}

#[test]
fn push_storage_table_renders_store_ingest_line() {
    // Non-zero store ingest (reflinked + hardlinked + copied bytes)
    // renders the "Store ingest" line with a zero-copy-share % that
    // counts both reflinked and hardlinked bytes as shared.
    // Covers push_storage_table's ingest>0 branch.
    let storage = StorageBreakdown {
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        restored_bytes: 0,
        zero_copy_pct: 0.0,
        store_blobs: 4,
        logical_bytes: 4096,
        blob_bytes: 2048,
        dedup_saved_bytes: 0,
        accounting_consistent: true,
        store_reflinked_bytes: 2000,
        store_hardlinked_bytes: 1000,
        store_copied_bytes: 1000,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
    };
    let mut lines = Vec::new();
    push_storage_table(&mut lines, &storage);
    let joined = lines.join("\n");
    assert!(joined.contains("Store ingest"), "got: {joined}");
    assert!(joined.contains("reflinked (CoW)"));
    assert!(joined.contains("hardlinked"), "got: {joined}");
    assert!(
        joined.contains("75.0% shared with build output"),
        "hardlinked ingest must count toward the shared %: {joined}"
    );
}

#[test]
fn github_storage_summary_without_restores_shows_logical_and_blobs() {
    // When nothing was restored this run, the github Storage summary falls
    // back to the "{logical} logical, {blobs} blobs" form (the else arm).
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let mut report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    report.storage = StorageBreakdown {
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        restored_bytes: 0, // -> else branch
        zero_copy_pct: 0.0,
        store_blobs: 7,
        logical_bytes: 9000,
        blob_bytes: 5000,
        dedup_saved_bytes: 4000,
        accounting_consistent: true,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
    };
    let gh = format_github(&report);
    assert!(
        gh.contains("logical") && gh.contains("blobs"),
        "summary: {gh}"
    );
}

#[test]
fn storage_render_flags_impossible_dedup_accounting() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let mut report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    report.storage.logical_bytes = 29;
    report.storage.blob_bytes = 56;
    report.storage.dedup_saved_bytes = 0;
    report.storage.accounting_consistent = false;

    let github = format_github(&report);
    assert!(github.contains("accounting inconsistent"), "{github}");
    assert!(github.contains("store index needs repair"), "{github}");
    assert!(!github.contains("0 B dedup saved"), "{github}");

    let text = format_text(&report);
    assert!(text.contains("Store accounting inconsistent"), "{text}");
    assert!(text.contains("store index needs repair"), "{text}");
}

#[test]
fn storage_render_preserves_zero_boundaries_and_summary_choice() {
    let dir = tempfile::tempdir().unwrap();
    let config = write_test_events(dir.path());
    let render = |logical_bytes, blob_bytes, accounting_consistent| {
        let mut report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
        report.storage.restored_bytes = 0;
        report.storage.logical_bytes = logical_bytes;
        report.storage.blob_bytes = blob_bytes;
        report.storage.accounting_consistent = accounting_consistent;
        (format_github(&report), format_text(&report))
    };

    for (logical, blobs) in [(1, 0), (0, 1)] {
        let (github, text) = render(logical, blobs, true);
        assert!(github.contains("Store footprint"), "{github}");
        assert!(text.contains("  Store:"), "{text}");
    }
    for (logical, blobs) in [(1, 0), (0, 1)] {
        let (github, text) = render(logical, blobs, false);
        assert!(github.contains("accounting inconsistent"), "{github}");
        assert!(text.contains("Store accounting inconsistent"), "{text}");
    }

    let (github, _) = render(1, 1, true);
    assert!(github.contains("1 B logical, 1 B blobs"), "{github}");
    assert!(
        !github.contains("zero-copy restores, 0 B restored"),
        "{github}"
    );

    let mut restored = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    restored.storage.restored_bytes = 1024;
    restored.storage.logical_bytes = 1;
    restored.storage.blob_bytes = 1;
    restored.storage.accounting_consistent = true;
    let github = format_github(&restored);
    assert!(github.contains("zero-copy restores"), "{github}");
    assert!(github.contains("KB restored"), "{github}");

    assert!(blob_accounting_consistent(0, 0));
    assert!(blob_accounting_consistent(5, 5));
    assert!(blob_accounting_consistent(5, 4));
    assert!(!blob_accounting_consistent(4, 5));

    let state = |logical_bytes, blob_bytes, accounting_consistent| {
        let mut report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
        report.storage.logical_bytes = logical_bytes;
        report.storage.blob_bytes = blob_bytes;
        report.storage.accounting_consistent = accounting_consistent;
        storage_accounting_state(&report.storage)
    };
    assert_eq!(state(0, 0, false), StorageAccountingState::Absent);
    assert_eq!(state(1, 0, true), StorageAccountingState::Consistent);
    assert_eq!(state(0, 1, true), StorageAccountingState::Consistent);
    assert_eq!(state(1, 0, false), StorageAccountingState::Inconsistent);
    assert_eq!(state(0, 1, false), StorageAccountingState::Inconsistent);

    let report = generate_report(&config, SinceWindow::DEFAULT, 10).unwrap();
    let mut old_json = serde_json::to_value(&report.storage).unwrap();
    old_json
        .as_object_mut()
        .unwrap()
        .remove("accounting_consistent");
    let decoded: StorageBreakdown = serde_json::from_value(old_json).unwrap();
    assert!(
        decoded.accounting_consistent,
        "old JSON must stay consistent"
    );
}

fn empty_storage() -> StorageBreakdown {
    StorageBreakdown {
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        restored_bytes: 0,
        zero_copy_pct: 0.0,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
        store_copy_cross_device_bytes: 0,
        store_copy_permission_bytes: 0,
        store_copy_ineligible_bytes: 0,
        store_copy_other_bytes: 0,
        restore_copy_cross_device_bytes: 0,
        restore_copy_permission_bytes: 0,
        restore_copy_exclusive_bytes: 0,
        restore_copy_other_bytes: 0,
        store_blobs: 0,
        logical_bytes: 0,
        blob_bytes: 0,
        dedup_saved_bytes: 0,
        accounting_consistent: true,
    }
}

#[test]
fn has_storage_data_is_true_for_each_copy_reason() {
    // One test per `||` arm: deleting any arm must fail its case, where
    // only that reason is non-zero.
    let mut base = empty_storage();
    assert!(!has_storage_data(&base));

    base.store_copy_cross_device_bytes = 1;
    assert!(has_storage_data(&base));
    base = empty_storage();

    base.store_copy_permission_bytes = 1;
    assert!(has_storage_data(&base));
    base = empty_storage();

    base.store_copy_ineligible_bytes = 1;
    assert!(has_storage_data(&base));
    base = empty_storage();

    base.store_copy_other_bytes = 1;
    assert!(has_storage_data(&base));
    base = empty_storage();

    base.restore_copy_cross_device_bytes = 1;
    assert!(has_storage_data(&base));
    base = empty_storage();

    base.restore_copy_permission_bytes = 1;
    assert!(has_storage_data(&base));
    base = empty_storage();

    base.restore_copy_exclusive_bytes = 1;
    assert!(has_storage_data(&base));
    base = empty_storage();

    base.restore_copy_other_bytes = 1;
    assert!(has_storage_data(&base));
}

#[test]
fn push_storage_table_renders_store_copy_reasons() {
    let mut storage = empty_storage();
    storage.store_copy_cross_device_bytes = 100;
    storage.store_copy_permission_bytes = 200;
    storage.store_copy_ineligible_bytes = 300;
    storage.store_copy_other_bytes = 400;
    let mut lines = Vec::new();
    push_storage_table(&mut lines, &storage);
    let joined = lines.join("\n");
    assert!(joined.contains("Store copy reasons"), "got: {joined}");
    assert!(joined.contains("cross-device (EXDEV)"), "got: {joined}");
    assert!(joined.contains("permission (EPERM)"), "got: {joined}");
    assert!(joined.contains("kind-ineligible"), "got: {joined}");
}

#[test]
fn push_storage_table_renders_restore_copy_reasons() {
    let mut storage = empty_storage();
    storage.restore_copy_cross_device_bytes = 100;
    storage.restore_copy_permission_bytes = 200;
    storage.restore_copy_exclusive_bytes = 300;
    storage.restore_copy_other_bytes = 400;
    let mut lines = Vec::new();
    push_storage_table(&mut lines, &storage);
    let joined = lines.join("\n");
    assert!(joined.contains("Restore copy reasons"), "got: {joined}");
    assert!(joined.contains("cross-device (EXDEV)"), "got: {joined}");
    assert!(joined.contains("exclusive-carrier"), "got: {joined}");
}

#[test]
fn push_storage_table_omits_copy_reasons_when_zero() {
    let storage = empty_storage();
    let mut lines = Vec::new();
    push_storage_table(&mut lines, &storage);
    let joined = lines.join("\n");
    assert!(!joined.contains("Store copy reasons"), "got: {joined}");
    assert!(!joined.contains("Restore copy reasons"), "got: {joined}");
}

#[test]
fn test_push_error_table_truncates_at_ten() {
    let errors: Vec<ErrorDetail> = (0..12)
        .map(|i| ErrorDetail {
            crate_name: format!("crate{i}"),
            cache_key: "0123456789abcdef".to_string(),
            timestamp: "2025-01-01T00:00:00".to_string(),
        })
        .collect();
    let mut lines = Vec::new();
    push_error_table(&mut lines, &errors);
    let joined = lines.join("\n");
    assert!(joined.contains("| Crate | Time | Key |"));
    assert!(
        joined.contains("2 more"),
        "should note the overflow beyond 10"
    );
}

#[test]
fn test_push_bypass_tables_renders_reasons_and_slowest() {
    let bypass = BypassAnalysis {
        passthroughs: 1,
        reasons: vec![BypassReason {
            result: "passthrough".to_string(),
            route: "direct".to_string(),
            reason: "linker".to_string(),
            count: 3,
            failures: 1,
            max_elapsed_ms: 1500,
        }],
        slowest: vec![BypassDetail {
            fallback_attempt: None,
            crate_name: "foo".to_string(),
            root: String::new(),
            result: "passthrough".to_string(),
            route: "direct".to_string(),
            reason: "linker".to_string(),
            start_time: String::new(),
            end_time: String::new(),
            start_unix_ms: 0,
            end_unix_ms: 0,
            elapsed_ms: 1500,
            exit_code: Some(0),
            timestamp: "2025-01-01T00:00:00".to_string(),
        }],
        ..Default::default()
    };
    let mut lines = Vec::new();
    push_bypass_tables(&mut lines, &bypass);
    let joined = lines.join("\n");
    assert!(joined.contains("| Result | Route | Reason"));
    assert!(joined.contains("Slowest bypassed invocations"));
    assert!(joined.contains("foo"));
}

#[test]
fn test_build_network_analysis_empty_is_zeroed() {
    let na = build_network_analysis(&[], 10);
    assert_eq!(na.uploads_ok, 0);
    assert_eq!(na.downloads_ok, 0);
    assert_eq!(na.bytes_up, 0);
    assert_eq!(na.bytes_down, 0);
}

#[test]
fn test_build_bypass_analysis_respects_top_limit() {
    let events: Vec<BuildEvent> = (0..5)
        .map(|i| {
            let mut e = test_event(&format!("c{i}"), EventResult::Passthrough, i, 0, 0, "k");
            e.passthrough_reason = format!("reason{i}");
            e
        })
        .collect();
    let analysis = build_bypass_analysis(&events, 2);
    assert_eq!(analysis.passthroughs, 5, "totals count all events");
    assert!(analysis.reasons.len() <= 2, "reasons truncated to top");
    assert!(analysis.slowest.len() <= 2, "slowest truncated to top");
}
