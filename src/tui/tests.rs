use super::*;

fn terminal_restore_was_observed(action: impl FnOnce()) -> bool {
    TERMINAL_RESTORE_OBSERVED.with(|observed| observed.set(false));
    action();
    TERMINAL_RESTORE_OBSERVED.with(std::cell::Cell::get)
}

#[test]
fn terminal_restore_function_runs_the_cleanup_path() {
    assert!(terminal_restore_was_observed(restore_terminal));
}

#[test]
fn terminal_restore_guard_runs_on_drop() {
    assert!(terminal_restore_was_observed(|| {
        let _guard = TerminalModeGuard;
    }));
}

#[test]
fn test_tab_needs_entries_only_for_store() {
    assert!(!tab_needs_entries(Tab::Build));
    assert!(!tab_needs_entries(Tab::Projects));
    assert!(tab_needs_entries(Tab::Store));
    assert!(!tab_needs_entries(Tab::Transfer));
    assert!(!tab_needs_entries(Tab::Why));
}

#[test]
fn event_presentation_surfaces_the_action_not_just_disposition() {
    // A miss actually built and cached the output — and is a normal,
    // neutral outcome, not a failure (only Error is red).
    let (_, status, action, color) = event_presentation(EventResult::Miss);
    assert_eq!(status, "miss");
    assert_eq!(action, "built + cached");
    assert_ne!(color, Color::Red);

    assert_eq!(event_presentation(EventResult::Dup).2, "built + deduped");
    assert_eq!(event_presentation(EventResult::LocalHit).2, "restored");
    assert_eq!(event_presentation(EventResult::RemoteHit).2, "downloaded");
    assert_eq!(
        event_presentation(EventResult::Passthrough).2,
        "built (not cached)"
    );

    // Error is the only red outcome.
    assert_eq!(event_presentation(EventResult::Error).3, Color::Red);
}

#[test]
fn fmt_duration_ms_blanks_zero_and_scales() {
    assert_eq!(fmt_duration_ms(0), "");
    assert_eq!(fmt_duration_ms(250), "250ms");
    assert_eq!(fmt_duration_ms(1500), "1.5s");
}

#[test]
fn format_speed_scales_units() {
    assert_eq!(format_speed(0.0), "0 B/s");
    assert_eq!(format_speed(500.0), "500 B/s");
    assert_eq!(format_speed(2_000.0), "2 KB/s");
    assert_eq!(format_speed(5_000_000.0), "5.0 MB/s");
}

#[test]
fn sort_mode_next_cycles_through_all_modes() {
    let mut m = SortMode::Size;
    let mut labels = vec![m.label().to_string()];
    for _ in 0..4 {
        m = m.next();
        labels.push(m.label().to_string());
    }
    // Size -> Hits -> Age -> Name -> Size (wraps)
    assert_eq!(labels, ["size", "hits", "age", "name", "size"]);
}

#[test]
fn shorten_home_replaces_home_prefix() {
    if let Some(home) = dirs::home_dir() {
        let p = home.join("projects/x");
        assert_eq!(shorten_home(&p), "~/projects/x");
    }
    // A path outside home is returned unchanged.
    let outside = std::path::Path::new("/opt/elsewhere");
    assert_eq!(shorten_home(outside), "/opt/elsewhere");
}

fn test_config() -> Config {
    use crate::config::{
        DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS,
    };
    Config {
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
        cache_dir: std::env::temp_dir().join("kache-tui-test"),
        runtime_dir: std::env::temp_dir().join("kache-tui-test"),
        max_size: 1024 * 1024,
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
        event_log_max_size: 1024 * 1024,
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
        daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
    }
}

fn test_state() -> AppState {
    let config = test_config();
    AppState {
        tailer: EventTailer::new(config.event_log_path()),
        config,
        active_tab: Tab::Build,
        events: Vec::new(),
        live_heartbeats: std::collections::HashMap::new(),
        build_scroll: Viewport::new(ScrollAnchor::Bottom),
        build_filter: String::new(),
        store_filter: String::new(),
        why_filter: String::new(),
        filter_active: false,
        sort_mode: SortMode::Size,
        store_scroll: Viewport::new(ScrollAnchor::Top),
        stats_snapshot: StatsSnapshot::default(),
        stats_loaded: false,
        last_stats_fetch: Instant::now(),
        project_scan: Arc::new(Mutex::new(ProjectScanData::default())),
        last_project_refresh: Instant::now(),
        project_scroll: Viewport::new(ScrollAnchor::Top),
        sessions: Vec::new(),
        grouped_len: 0,
        selected_session: None,
        shown: None,
        why_cache: None,
        transfer_scroll: Viewport::new(ScrollAnchor::Top),
        why_scroll: Viewport::new(ScrollAnchor::Top),
        prev_bytes_uploaded: 0,
        prev_bytes_downloaded: 0,
        upload_speed_bps: 0.0,
        download_speed_bps: 0.0,
        rustc_version_slot: Arc::new(Mutex::new(None)),
        stats_result_slot: Arc::new(Mutex::new(None)),
        stats_fetch_in_flight: false,
        stats_fetch_requested_entries: false,
        should_quit: false,
        paused: false,
        spark_window: SPARK_WINDOW,
        rustc_version: "test".to_string(),
        wrapper_status: "test".to_string(),
        service_installed: false,
    }
}

#[test]
fn project_scan_status_distinguishes_loading_idle_and_unscanned() {
    assert_eq!(project_scan_status(false, false, false), "calculating");
    assert_eq!(project_scan_status(true, true, true), "calculating");
    assert_eq!(project_scan_status(true, false, true), "idle");
    assert_eq!(project_scan_status(true, false, false), "not scanned");
    assert!(project_scan_can_start(false));
    assert!(!project_scan_can_start(true));
}

#[test]
fn tui_uses_the_daemon_remote_status() {
    let config = test_config();
    let daemon_cache_dir =
        std::path::absolute(std::env::temp_dir().join("kache-tui-daemon-a")).unwrap();
    let snapshot = StatsSnapshot {
        daemon_connected: true,
        daemon_effective_config: Some(crate::daemon::EffectiveConfig {
            max_size: config.max_size,
            cache_dir: daemon_cache_dir.to_string_lossy().into_owned(),
            runtime_dir: "/shared/runtime".to_string(),
            config_path: "/daemon-a/config.toml".to_string(),
            config_fingerprint: Some("daemon-a-fingerprint".to_string()),
            prefetch_enabled: true,
            remote_description: Some("s3://daemon-a/cache".to_string()),
            local_only: false,
            remote_error: None,
            remote_key_cache_refresh_secs: 60,
            socket_path: "/shared/daemon.sock".to_string(),
            started_at_ms: 1,
        }),
        ..StatsSnapshot::default()
    };
    assert_eq!(
        effective_remote_status(&config, &snapshot),
        "s3://daemon-a/cache"
    );
}

#[test]
fn project_scan_removes_stale_entries() {
    let dir = tempfile::tempdir().unwrap();
    let stats = Arc::new(Mutex::new(ProjectScanData {
        project_targets: vec![cli::TargetEntry {
            path: dir.path().join("removed-target"),
            size: 1,
            cached_bytes: 0,
            estimated_reclaimable_bytes: 1,
            scan_identity: None,
            profiles: Vec::new(),
            breakdown: cli::CategoryBreakdown::default(),
            stale: false,
        }],
        ..ProjectScanData::default()
    }));

    spawn_project_scan(Arc::clone(&stats), dir.path().to_path_buf())
        .join()
        .unwrap();

    let scan = stats.lock().unwrap();
    assert!(scan.scanned);
    assert!(!scan.scanning);
    assert!(scan.project_targets.is_empty());
}

#[test]
fn viewport_scroll_to_max_then_stop() {
    let mut top = Viewport::new(ScrollAnchor::Top);
    top.visible_range(10, 5);
    assert_eq!(top.max_offset, 5);
    for _ in 0..20 {
        top.scroll_down();
    }
    assert_eq!(top.offset, 5);
    top.scroll_up();
    assert_eq!(top.offset, 4);

    let mut bottom = Viewport::new(ScrollAnchor::Bottom);
    bottom.visible_range(10, 5);
    assert_eq!(bottom.max_offset, 5);
    for _ in 0..20 {
        bottom.scroll_up();
    }
    assert_eq!(bottom.offset, 5);
    bottom.scroll_down();
    assert_eq!(bottom.offset, 4);
}

#[test]
fn viewport_visible_range_shrink_and_expand() {
    let mut v = Viewport::new(ScrollAnchor::Top);
    v.visible_range(100, 10);
    for _ in 0..50 {
        v.scroll_down();
    }
    assert_eq!(v.offset, 50);
    assert_eq!(v.max_offset, 90);

    v.visible_range(20, 10);
    assert_eq!(v.offset, 10);
    assert_eq!(v.max_offset, 10);

    v.visible_range(100, 10);
    assert_eq!(v.offset, 10);
    assert_eq!(v.max_offset, 90);
}

#[test]
fn handle_key_number_keys_switch_tabs() {
    let mut s = test_state();
    handle_key(&mut s, KeyCode::Char('2'));
    assert_eq!(s.active_tab, Tab::Why);
    handle_key(&mut s, KeyCode::Char('3'));
    assert_eq!(s.active_tab, Tab::Projects);
    handle_key(&mut s, KeyCode::Char('4'));
    assert_eq!(s.active_tab, Tab::Store);
    handle_key(&mut s, KeyCode::Char('5'));
    assert_eq!(s.active_tab, Tab::Transfer);
    handle_key(&mut s, KeyCode::Char('1'));
    assert_eq!(s.active_tab, Tab::Build);
}

/// Landing on Projects or Store backdates that tab's refresh clock so its
/// data is fetched immediately rather than at the next interval. Getting
/// the sign wrong would postpone the fetch instead, which reads as a tab
/// that renders stale numbers on arrival.
#[test]
fn switching_to_a_fetching_tab_forces_an_immediate_refresh() {
    let mut s = test_state();
    let fresh = Instant::now();
    s.last_project_refresh = fresh;
    s.last_stats_fetch = fresh;

    switch_tab(&mut s, Tab::Projects);
    assert!(
        s.last_project_refresh.elapsed() >= PROJECT_REFRESH_INTERVAL,
        "Projects must be due for refresh on arrival"
    );

    switch_tab(&mut s, Tab::Store);
    assert!(
        s.last_stats_fetch.elapsed() >= SNAPSHOT_REFRESH_INTERVAL,
        "Store must be due for a stats fetch on arrival"
    );

    // Tabs without their own fetch leave both clocks alone.
    let before_project = s.last_project_refresh;
    let before_stats = s.last_stats_fetch;
    switch_tab(&mut s, Tab::Transfer);
    assert_eq!(s.last_project_refresh, before_project);
    assert_eq!(s.last_stats_fetch, before_stats);
}

#[test]
fn handle_key_tab_cycles_forward_and_wraps() {
    let mut s = test_state();
    let order = [
        Tab::Why,
        Tab::Projects,
        Tab::Store,
        Tab::Transfer,
        Tab::Build,
    ];
    for expected in order {
        handle_key(&mut s, KeyCode::Tab);
        assert_eq!(s.active_tab, expected);
    }
}

#[test]
fn handle_key_q_sets_should_quit() {
    let mut s = test_state();
    handle_key(&mut s, KeyCode::Char('q'));
    assert!(s.should_quit);
}

/// Raw mode delivers Ctrl+C as a key event. Dropping the modifier made
/// it a bare `c`, which on the Build tab cleared the event list instead
/// of quitting.
#[test]
fn ctrl_c_quits_and_keeps_the_events() {
    let mut s = test_state();
    s.active_tab = Tab::Build;
    s.events
        .push(sample_build_event("serde", EventResult::Miss, 10, 1));
    handle_key_event(
        &mut s,
        KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
    );
    assert!(s.should_quit, "Ctrl+C must quit");
    assert_eq!(s.events.len(), 1, "and must not clear the Build tab");

    // Plain `c` still clears, and the modifier-free path is unchanged.
    let mut s = test_state();
    s.active_tab = Tab::Build;
    s.events
        .push(sample_build_event("serde", EventResult::Miss, 10, 1));
    handle_key_event(
        &mut s,
        KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE),
    );
    assert!(!s.should_quit);
    assert!(s.events.is_empty());
}

#[test]
fn page_and_edge_keys_move_the_active_viewport() {
    let mut s = test_state();
    s.active_tab = Tab::Store;
    s.store_scroll.visible_range(100, 10);
    handle_key(&mut s, KeyCode::PageDown);
    assert_eq!(s.store_scroll.offset, 10, "a page is what the panel showed");
    handle_key(&mut s, KeyCode::End);
    assert_eq!(s.store_scroll.offset, 90);
    handle_key(&mut s, KeyCode::PageUp);
    assert_eq!(s.store_scroll.offset, 80);
    handle_key(&mut s, KeyCode::Home);
    assert_eq!(s.store_scroll.offset, 0);
    handle_key(&mut s, KeyCode::Char('j'));
    assert_eq!(s.store_scroll.offset, 1);
    handle_key(&mut s, KeyCode::Char('k'));
    assert_eq!(s.store_scroll.offset, 0);

    // Bottom-anchored: End is "follow the newest", Home is the oldest row.
    s.active_tab = Tab::Build;
    s.build_scroll.visible_range(100, 10);
    handle_key(&mut s, KeyCode::PageUp);
    assert_eq!(s.build_scroll.offset, 10);
    assert!(!s.build_scroll.at_anchor());
    handle_key(&mut s, KeyCode::Home);
    assert_eq!(s.build_scroll.offset, 90);
    handle_key(&mut s, KeyCode::End);
    assert_eq!(s.build_scroll.offset, 0);
    assert!(s.build_scroll.at_anchor());
}

/// A reader who scrolled back into history keeps looking at the same rows
/// while a build appends new ones; a reader at the live edge follows.
#[test]
fn arriving_rows_do_not_move_a_scrolled_reader() {
    let mut s = test_state();
    for i in 0..20 {
        s.push_event(session_event(
            &format!("c{i}"),
            EventResult::Miss,
            "/w",
            "s1",
            100,
        ));
    }
    s.refresh_sessions(chrono::Utc::now());
    let range = s.build_scroll.visible_range(20, 5);
    assert_eq!(range, 15..20, "following the newest");

    s.build_scroll.scroll_up_by(10);
    let range = s.build_scroll.visible_range(20, 5);
    assert_eq!(range, 5..10);

    for i in 20..23 {
        s.push_event(session_event(
            &format!("c{i}"),
            EventResult::Miss,
            "/w",
            "s1",
            50,
        ));
    }
    s.refresh_sessions(chrono::Utc::now());
    let range = s.build_scroll.visible_range(23, 5);
    assert_eq!(range, 5..10, "same rows after three arrivals");

    // Rows for another, older build move nothing here.
    s.push_event(session_event("x", EventResult::Miss, "/v", "s2", 400));
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.selected_session().unwrap().key, "id:s1");
    let range = s.build_scroll.visible_range(23, 5);
    assert_eq!(range, 5..10);

    // At the live edge the newest rows are the view.
    s.build_scroll.end();
    s.push_event(session_event("c23", EventResult::Miss, "/w", "s1", 30));
    s.refresh_sessions(chrono::Utc::now());
    let range = s.build_scroll.visible_range(24, 5);
    assert_eq!(range, 19..24);

    // An event the filter hides does not count as an arrival; one the
    // filter shows does.
    s.build_scroll.scroll_up_by(10);
    s.build_filter = "zzz".to_string();
    s.push_event(session_event("c24", EventResult::Miss, "/w", "s1", 20));
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.build_scroll.offset, 10);
    s.build_filter = "c2".to_string();
    s.push_event(session_event("c25", EventResult::Miss, "/w", "s1", 10));
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.build_scroll.offset, 11, "a matching filter still counts");
}

#[test]
fn pause_toggles_and_is_announced() {
    let mut s = test_state();
    assert!(!s.paused);
    handle_key(&mut s, KeyCode::Char('p'));
    assert!(s.paused);
    assert!(rendered_tab(&mut s, Tab::Build).contains("PAUSED"));
    handle_key(&mut s, KeyCode::Char('p'));
    assert!(!s.paused);
    assert!(!rendered_tab(&mut s, Tab::Build).contains("PAUSED"));
}

#[test]
fn lookup_series_buckets_by_time_and_keeps_idle_time_flat() {
    use chrono::Duration as ChronoDuration;
    let now = chrono::Utc::now();
    let window = Duration::from_secs(300);
    let at = |secs_ago: i64, result: EventResult| {
        let mut event = sample_build_event("x", result, 1, 1);
        event.ts = now - ChronoDuration::seconds(secs_ago);
        event
    };
    let events = vec![
        at(10, EventResult::LocalHit),
        at(10, EventResult::RemoteHit),
        at(10, EventResult::Miss),
        at(150, EventResult::Dup),
        at(290, EventResult::PrefetchHit),
        at(290, EventResult::Passthrough),
        at(400, EventResult::LocalHit),
        at(-5, EventResult::LocalHit),
    ];
    let (hits, misses) = lookup_series(&events, now, window, 3);
    assert_eq!(
        hits,
        vec![1, 0, 2],
        "oldest bucket first; 400s ago and the future are out"
    );
    assert_eq!(
        misses,
        vec![0, 1, 1],
        "dup counts as a compile, passthrough as neither"
    );
    assert_eq!(lookup_series(&events, now, window, 0), (vec![], vec![]));
}

#[test]
fn fmt_window_picks_the_largest_exact_unit() {
    assert_eq!(fmt_window(Duration::from_secs(300)), "5m");
    assert_eq!(fmt_window(Duration::from_secs(7200)), "2h");
    assert_eq!(fmt_window(Duration::from_secs(86_400 * 7)), "7d");
    assert_eq!(fmt_window(Duration::from_secs(90)), "90s");
}

#[test]
fn tab_bar_click_targets_match_the_drawn_labels() {
    let titles = tab_titles();
    for (tab, label, start) in titles {
        assert_eq!(tab_at_column(start), Some(tab), "first column of {label:?}");
        assert_eq!(
            tab_at_column(start + label.len() as u16 - 1),
            Some(tab),
            "last column of {label:?}"
        );
    }
    // The two-space gutter between labels selects nothing.
    let (_, first, start) = titles[0];
    assert_eq!(tab_at_column(start + first.len() as u16), None);
    assert_eq!(tab_at_column(999), None);

    let mut s = test_state();
    let area = Rect::new(0, 0, 120, 40);
    let (_, _, store_x) = titles[3];
    handle_mouse(
        &mut s,
        MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: store_x + 1,
            row: 0,
            modifiers: KeyModifiers::NONE,
        },
        area,
    );
    assert_eq!(s.active_tab, Tab::Store);

    // The wheel scrolls the active tab, three rows a notch.
    s.store_scroll.visible_range(100, 10);
    handle_mouse(
        &mut s,
        MouseEvent {
            kind: MouseEventKind::ScrollDown,
            column: 40,
            row: 20,
            modifiers: KeyModifiers::NONE,
        },
        area,
    );
    assert_eq!(s.store_scroll.offset, 3);
}

#[test]
fn too_small_terminal_says_so_instead_of_drawing_garbage() {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    let mut state = test_state();
    let mut terminal = Terminal::new(TestBackend::new(40, 10)).unwrap();
    terminal.draw(|frame| draw_ui(frame, &mut state)).unwrap();
    let rendered: String = terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect();
    // Wrapped text: check the tokens, not a phrase that may span rows.
    assert!(rendered.contains("40×10"), "{rendered}");
    assert!(rendered.contains("60×16"), "{rendered}");
    assert!(rendered.contains("kache stats"), "{rendered}");
    assert!(
        !rendered.contains("[1] Build"),
        "no tab bar in the guard screen"
    );
}

/// Render `tab` at `width`×`height` and return the screen as lines.
fn rendered_lines(state: &mut AppState, tab: Tab, width: u16, height: u16) -> Vec<String> {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    state.active_tab = tab;
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| draw_ui(frame, state))
        .expect("draw should succeed");
    let buffer = terminal.backend().buffer();
    (0..height)
        .map(|y| (0..width).map(|x| buffer[(x, y)].symbol()).collect())
        .collect()
}

#[test]
fn packed_list_and_cancellation_have_distinct_transfer_labels() {
    let mut state = populated_state();
    let transfer = &mut state.stats_snapshot.recent_transfers[0];
    transfer.direction = daemon::TransferDirection::Download;
    transfer.ok = false;
    transfer.outcome = "cancelled".to_owned();
    transfer.accounting = Some(kache_core::timeline::PrefetchAccounting {
        operation: kache_core::timeline::PrefetchOperation::List,
        ..Default::default()
    });
    let screen = rendered_lines(&mut state, Tab::Transfer, 120, 40).join("\n");
    assert!(screen.contains("STOP"), "{screen}");
    assert!(!screen.contains("FAIL"), "{screen}");
    let row = screen.lines().find(|line| line.contains("serde")).unwrap();
    assert!(
        row.trim_start_matches('│').trim_start().starts_with("L "),
        "LIST row: {row}"
    );
}

#[test]
fn not_found_transfers_render_as_misses() {
    let mut state = populated_state();
    let transfer = &mut state.stats_snapshot.recent_transfers[0];
    transfer.direction = daemon::TransferDirection::Download;
    transfer.ok = false;
    transfer.outcome = "not_found".to_string();
    let screen = rendered_lines(&mut state, Tab::Transfer, 120, 40).join("\n");
    assert!(screen.contains("MISS"), "{screen}");
    assert!(!screen.contains("FAIL"), "{screen}");
    state.stats_snapshot.recent_transfers[0].direction = daemon::TransferDirection::Upload;
    let screen = rendered_lines(&mut state, Tab::Transfer, 120, 40).join("\n");
    assert!(screen.contains("FAIL"), "{screen}");
    state.stats_snapshot.recent_transfers[0].direction = daemon::TransferDirection::Download;
    state.stats_snapshot.recent_transfers[0].outcome = "error".to_string();
    let screen = rendered_lines(&mut state, Tab::Transfer, 120, 40).join("\n");
    assert!(screen.contains("FAIL"), "{screen}");
    state.stats_snapshot.recent_transfers[0].outcome = "skipped".to_string();
    state.stats_snapshot.recent_transfers[0].request_count = 0;
    let screen = rendered_lines(&mut state, Tab::Transfer, 120, 40).join("\n");
    assert!(screen.contains("SKIP"), "{screen}");
    assert!(!screen.contains("FAIL"), "{screen}");
}

fn populated_state() -> AppState {
    let mut state = test_state();
    state.events = vec![
        sample_build_event("serde", EventResult::Miss, 4200, 2_000_000),
        sample_build_event("tokio", EventResult::LocalHit, 30, 1_500_000),
        sample_build_event("build.rs", EventResult::Passthrough, 80, 0),
    ];
    state.stats_snapshot.entries = vec![
        sample_stats_entry("serde", 2_000_000, 5),
        sample_stats_entry("tokio", 1_500_000, 2),
    ];
    state.stats_snapshot.entry_count = 2;
    state.stats_snapshot.total_size = 3_500_000;
    state.stats_loaded = true;
    state.stats_snapshot.recent_transfers = vec![daemon::TransferEvent {
        accounting: None,
        prefetch: None,
        outcome: String::new(),
        schema: 3,
        crate_name: "serde".to_string(),
        direction: daemon::TransferDirection::Upload,
        format: "tar.zst".to_string(),
        cache_key: "serde-key".to_string(),
        object_key: "prefix/serde".to_string(),
        compressed_bytes: 1000,
        started_at_unix_ms: 0,
        finished_at_unix_ms: 0,
        elapsed_ms: 12,
        network_ms: 6,
        semaphore_wait_ms: 0,
        head_ms: 0,
        request_ms: 2,
        body_ms: 4,
        request_count: 1,
        original_bytes: 3000,
        decompress_ms: 0,
        extract_ms: 0,
        disk_io_ms: 0,
        import_lock_wait_ms: 0,
        import_ms: 0,
        compression_ms: 0,
        head_checks_ms: 0,
        blobs_skipped: 0,
        blobs_total: 1,
        ok: true,
        timestamp: 0,
    }];
    {
        let mut scan = state.project_scan.lock().unwrap();
        scan.project_targets = vec![cli::TargetEntry {
            path: std::path::PathBuf::from("/work/myproj/target"),
            size: 5_000_000,
            cached_bytes: 3_000_000,
            estimated_reclaimable_bytes: 2_000_000,
            scan_identity: None,
            profiles: vec!["debug".to_string()],
            breakdown: cli::CategoryBreakdown::default(),
            stale: false,
        }];
        scan.scanned = true;
    }
    state.refresh_sessions(chrono::Utc::now());
    state
}

/// An event stamped with a build root and session id, as the wrapper
/// writes them.
fn session_event(
    crate_name: &str,
    result: EventResult,
    root: &str,
    session: &str,
    secs_ago: i64,
) -> BuildEvent {
    let mut event = sample_build_event(crate_name, result, 100, 1);
    event.root = root.to_string();
    event.session_id = session.to_string();
    event.ts = chrono::Utc::now() - chrono::Duration::seconds(secs_ago);
    event.compile_time_ms = 2_000;
    event
}

/// Two builds: an old finished one and a fresh one. Up/Down pick between
/// them, the top row is followed until a pick is made, and Enter opens
/// Why for the pick.
#[test]
fn arrows_pick_a_build_and_enter_opens_why() {
    let mut s = test_state();
    s.events = vec![
        session_event("old_a", EventResult::Miss, "/w/old", "s-old", 900),
        session_event("old_b", EventResult::LocalHit, "/w/old", "s-old", 899),
        session_event("new_a", EventResult::Miss, "/w/new", "s-new", 5),
    ];
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.sessions.len(), 2);
    assert_eq!(s.sessions[0].key, "id:s-new", "newest on top");
    assert!(s.selected_session.is_none(), "following by default");
    assert_eq!(s.selected_session().unwrap().key, "id:s-new");

    let screen = rendered_tab(&mut s, Tab::Build);
    assert!(screen.contains("following the top build"), "{screen}");
    assert!(screen.contains("new_a"), "the event panel is the top build");
    assert!(!screen.contains("old_a"), "other builds' events stay out");

    handle_key(&mut s, KeyCode::Down);
    assert_eq!(s.selected_session.as_deref(), Some("id:s-old"));
    let screen = rendered_tab(&mut s, Tab::Build);
    assert!(
        screen.contains("old_a") && !screen.contains("new_a"),
        "{screen}"
    );
    assert!(!screen.contains("following the top build"));

    // Past the top row is back to following.
    handle_key(&mut s, KeyCode::Up);
    assert_eq!(s.selected_session.as_deref(), Some("id:s-new"));
    handle_key(&mut s, KeyCode::Up);
    assert!(s.selected_session.is_none());

    // Enter lands on Why for the selected build.
    handle_key(&mut s, KeyCode::Down);
    handle_key(&mut s, KeyCode::Enter);
    assert_eq!(s.active_tab, Tab::Why);
    let screen = rendered_tab(&mut s, Tab::Why);
    assert!(screen.contains("Why · old"), "{screen}");

    // Clearing forgets the pick along with the events.
    s.active_tab = Tab::Build;
    handle_key(&mut s, KeyCode::Char('c'));
    assert!(s.sessions.is_empty() && s.selected_session.is_none());
}

/// A pick survives the session list reordering under it: the key, not
/// the row index, is what is remembered.
#[test]
fn a_picked_build_stays_picked_when_rows_reorder() {
    let mut s = test_state();
    s.events = vec![
        session_event("a", EventResult::Miss, "/w/one", "s1", 500),
        session_event("b", EventResult::Miss, "/w/two", "s2", 400),
    ];
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.sessions[0].key, "id:s2");
    handle_key(&mut s, KeyCode::Down);
    assert_eq!(s.selected_session.as_deref(), Some("id:s1"));
    // s1 wakes up and moves to the top.
    s.push_event(session_event("c", EventResult::Miss, "/w/one", "s1", 1));
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.sessions[0].key, "id:s1");
    assert_eq!(s.selected_session().unwrap().key, "id:s1");
    assert_eq!(s.selected_index(), Some(0));
}

/// Forty misses downstream of one changed leaf read as one cause naming
/// the leaf, with the crates it took down and what they cost.
#[test]
fn why_tab_collapses_a_cascade_to_its_root() {
    let externs = |mut e: BuildEvent, digest: &str| {
        e.key_externs = [("leaf".to_string(), digest.to_string())]
            .into_iter()
            .collect();
        e.key_externs_recorded = true;
        e
    };
    let leaf = |mut e: BuildEvent, sources: &str| {
        e.key_externs_recorded = true;
        e.key_fields = [("sources".to_string(), sources.to_string())]
            .into_iter()
            .collect();
        e
    };
    let mut events = vec![leaf(
        session_event("leaf", EventResult::LocalHit, "/w", "before", 1000),
        "1111",
    )];
    for i in 0..40 {
        events.push(externs(
            session_event(
                &format!("app{i}"),
                EventResult::LocalHit,
                "/w",
                "before",
                999,
            ),
            "aaaa",
        ));
    }
    let mut leaf_miss = leaf(
        session_event("leaf", EventResult::Miss, "/w", "now", 10),
        "2222",
    );
    leaf_miss.key_diff = vec!["sources".to_string()];
    events.push(leaf_miss);
    for i in 0..40 {
        events.push(externs(
            session_event(&format!("app{i}"), EventResult::Miss, "/w", "now", 9),
            "bbbb",
        ));
    }
    let mut s = test_state();
    s.events = events;
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.selected_session().unwrap().key, "id:now");

    let lines: Vec<String> = why_lines(&mut s, 118)
        .iter()
        .map(|line| line.to_string())
        .collect();
    let text = lines.join("\n");
    assert!(text.contains("41 misses"), "{text}");
    let downstream = lines
        .iter()
        .find(|l| l.contains("downstream of leaf"))
        .unwrap_or_else(|| panic!("{text}"));
    assert!(downstream.contains("40"), "{downstream}");
    assert!(downstream.contains("app0, app1, app2 +37"), "{downstream}");
    assert!(
        downstream.contains("1m20s"),
        "40 x 2s of compile: {downstream}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("own inputs changed: sources")),
        "{text}"
    );
    assert!(
        !text.contains("explain_miss"),
        "digests were recorded, so no hint to enable them: {text}"
    );
    assert!(text.contains("saved "), "cost strip: {text}");

    let screen = rendered_tab(&mut s, Tab::Why);
    assert!(screen.contains("downstream of leaf"), "{screen}");

    // Narrow: the examples still fit at 60 columns (bar, count, and the
    // cost take 28), and are the first thing to go below that.
    let narrow = why_lines(&mut s, 60)
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(narrow.contains("downstream of leaf  app0"), "{narrow}");
    // At 58 the room is exactly the description plus twelve: examples
    // need more than that, so none; at 60 they fit but are clipped to the
    // twelve cells left.
    let edge = why_text(&mut s, 58).join("\n");
    assert!(
        edge.contains("downstream of leaf  1m20s"),
        "no ellipsis: {edge}"
    );
    assert!(!edge.contains("app0"), "{edge}");
    assert!(narrow.contains("app0, app1,…"), "{narrow}");
    assert!(!narrow.contains("app2"), "{narrow}");
    let narrower = why_lines(&mut s, 55)
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(narrower.contains("downstream of leaf"), "{narrower}");
    assert!(!narrower.contains("app0"), "{narrower}");
}

/// Misses with nothing recorded are said to be unexplained, and the
/// reader is told what to switch on.
#[test]
fn why_tab_says_when_it_cannot_explain_and_how_to_fix_that() {
    let mut s = test_state();
    s.events = vec![
        session_event("x", EventResult::LocalHit, "/w", "before", 500),
        session_event("x", EventResult::Miss, "/w", "now", 5),
        session_event("y", EventResult::Miss, "/w", "now", 4),
    ];
    s.refresh_sessions(chrono::Utc::now());
    let text: Vec<String> = why_lines(&mut s, 100)
        .iter()
        .map(|line| line.to_string())
        .collect();
    let text = text.join("\n");
    assert!(text.contains("unexplained"), "{text}");
    assert!(
        text.contains("no earlier compile in the loaded history"),
        "{text}"
    );
    assert!(text.contains("explain_miss = true"), "{text}");

    s.config.explain_miss = true;
    s.why_cache = None;
    let text: Vec<String> = why_lines(&mut s, 100)
        .iter()
        .map(|line| line.to_string())
        .collect();
    assert!(
        !text.join("\n").contains("explain_miss = true"),
        "already on, so no hint"
    );
}

#[test]
fn why_tab_without_builds_says_so() {
    let mut s = test_state();
    let screen = rendered_tab(&mut s, Tab::Why);
    assert!(screen.contains("No builds recorded yet"), "{screen}");
}

#[test]
fn share_bar_never_hides_a_present_cause() {
    assert_eq!(share_bar(0, 10, 4), "░░░░");
    assert_eq!(share_bar(1, 1000, 4), "█░░░");
    assert_eq!(share_bar(10, 10, 4), "████");
    assert_eq!(share_bar(3, 0, 2), "  ");
    assert_eq!(clip("abcdef", 4), "abc…");
    assert_eq!(clip("abc", 4), "abc");
    assert_eq!(fmt_saved_ms(0), "0s");
    assert_eq!(fmt_saved_ms(750), "750ms");
    assert_eq!(fmt_saved_ms(80_000), "1m20s");
}

/// Every tab at a laptop-sized and a wide terminal: the seeded row is on
/// screen, the columns a narrow terminal cannot afford are gone rather
/// than clipped, and nothing spills past the right edge.
#[test]
fn every_tab_fits_narrow_and_wide_terminals() {
    for (tab, seeded, wide_only) in [
        (Tab::Build, "serde", "Size"),
        (Tab::Store, "serde", "Created"),
        (Tab::Projects, "myproj", "Fprint"),
        (Tab::Transfer, "serde", ""),
        (Tab::Why, "build.rs", "direct"),
    ] {
        for (width, height) in [(80u16, 24u16), (120, 40)] {
            let mut state = populated_state();
            let lines = rendered_lines(&mut state, tab, width, height);
            let screen = lines.join("\n");
            assert!(
                screen.contains(seeded),
                "{tab:?} at {width}x{height} must show {seeded:?}:\n{screen}"
            );
            assert!(
                screen.contains("q: quit"),
                "{tab:?} at {width}x{height} must keep its help bar:\n{screen}"
            );
            if !wide_only.is_empty() {
                assert_eq!(
                    screen.contains(wide_only),
                    width >= 120,
                    "{tab:?} at {width}: column {wide_only:?} is wide-only:\n{screen}"
                );
            }
        }
    }
}

#[test]
fn handle_key_filter_mode_captures_text() {
    let mut s = test_state();
    s.active_tab = Tab::Build;
    handle_key(&mut s, KeyCode::Char('f'));
    assert!(s.filter_active);
    for c in "abc".chars() {
        handle_key(&mut s, KeyCode::Char(c));
    }
    assert_eq!(s.filter(), "abc");
    handle_key(&mut s, KeyCode::Backspace);
    assert_eq!(s.filter(), "ab");
    // Enter commits and leaves input mode; the filter stays applied.
    handle_key(&mut s, KeyCode::Enter);
    assert!(!s.filter_active);
    assert_eq!(s.filter(), "ab");
    // 'q' no longer types into the filter; it quits.
    handle_key(&mut s, KeyCode::Char('q'));
    assert!(s.should_quit);
}

/// Esc used to be bound to quit, so dismissing a filter and pressing it
/// once more out of reflex tore down the session.
#[test]
fn esc_clears_the_filter_instead_of_quitting() {
    let mut s = test_state();
    s.active_tab = Tab::Build;

    // Esc while typing cancels the filter outright.
    handle_key(&mut s, KeyCode::Char('f'));
    for c in "serde".chars() {
        handle_key(&mut s, KeyCode::Char(c));
    }
    handle_key(&mut s, KeyCode::Esc);
    assert!(!s.filter_active);
    assert_eq!(s.filter(), "", "Esc cancels rather than committing");
    assert!(!s.should_quit, "Esc must never quit");

    // Esc on a committed filter clears it, still without quitting.
    handle_key(&mut s, KeyCode::Char('f'));
    for c in "tokio".chars() {
        handle_key(&mut s, KeyCode::Char(c));
    }
    handle_key(&mut s, KeyCode::Enter);
    assert_eq!(s.filter(), "tokio");
    handle_key(&mut s, KeyCode::Esc);
    assert_eq!(s.filter(), "");
    assert!(!s.should_quit);

    // Esc with nothing to clear is inert, not fatal.
    handle_key(&mut s, KeyCode::Esc);
    assert!(!s.should_quit);
}

/// One shared filter meant Build's text silently narrowed Store and
/// Passthrough, with no title or help text admitting it.
#[test]
fn filters_do_not_leak_between_tabs() {
    let mut s = test_state();

    s.active_tab = Tab::Build;
    handle_key(&mut s, KeyCode::Char('f'));
    for c in "serde".chars() {
        handle_key(&mut s, KeyCode::Char(c));
    }
    handle_key(&mut s, KeyCode::Enter);

    s.active_tab = Tab::Store;
    assert_eq!(s.filter(), "", "Store keeps its own filter");
    s.active_tab = Tab::Why;
    assert_eq!(s.filter(), "", "Passthrough keeps its own filter");

    // Tabs that cannot filter report no filter and ignore `f`.
    s.active_tab = Tab::Projects;
    assert_eq!(s.filter(), "");
    handle_key(&mut s, KeyCode::Char('f'));
    assert!(!s.filter_active, "`f` is inert where nothing is filterable");

    s.active_tab = Tab::Build;
    assert_eq!(
        s.filter(),
        "serde",
        "Build's filter survived the round trip"
    );
}

/// Shift+Tab shared an arm with Tab, so both cycled forward and there was
/// no way back except by number.
#[test]
fn shift_tab_walks_backwards() {
    let mut s = test_state();
    s.active_tab = Tab::Build;

    handle_key(&mut s, KeyCode::Tab);
    assert_eq!(s.active_tab, Tab::Why);
    handle_key(&mut s, KeyCode::BackTab);
    assert_eq!(s.active_tab, Tab::Build);
    // And it wraps to the last tab rather than sticking.
    handle_key(&mut s, KeyCode::BackTab);
    assert_eq!(s.active_tab, Tab::Transfer);
    handle_key(&mut s, KeyCode::Tab);
    assert_eq!(s.active_tab, Tab::Build);
}

/// Render `tab` and return everything on screen as one string.
fn rendered_tab(state: &mut AppState, tab: Tab) -> String {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    state.active_tab = tab;
    let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
    terminal
        .draw(|frame| draw_ui(frame, state))
        .expect("draw should succeed");
    terminal
        .backend()
        .buffer()
        .content()
        .iter()
        .map(|c| c.symbol())
        .collect()
}

/// The help bar is the only place the per-tab keys are documented, so it
/// has to actually reach the screen. Rendering "without panicking" did not
/// prove that: dropping the help bar entirely still drew a full tab.
#[test]
fn every_tab_renders_its_help_keys() {
    for (tab, expected) in [
        (Tab::Build, "c: clear"),
        (Tab::Store, "s: sort"),
        (Tab::Why, "f: filter"),
    ] {
        let mut state = test_state();
        let rendered = rendered_tab(&mut state, tab);
        assert!(
            rendered.contains("q: quit"),
            "tab {tab:?} must show how to quit"
        );
        assert!(
            rendered.contains(expected),
            "tab {tab:?} must show its own key {expected:?}"
        );
    }
}

/// The clear hint only appears once there is a filter to clear, and it
/// appears on screen rather than only in the returned string.
#[test]
fn help_bar_offers_the_way_out_of_a_filter() {
    let mut state = test_state();
    assert!(
        !rendered_tab(&mut state, Tab::Build).contains("Esc: clear filter"),
        "no filter, no clear hint"
    );

    state.active_tab = Tab::Build;
    handle_key(&mut state, KeyCode::Char('f'));
    for c in "serde".chars() {
        handle_key(&mut state, KeyCode::Char(c));
    }
    handle_key(&mut state, KeyCode::Enter);

    let rendered = rendered_tab(&mut state, Tab::Build);
    assert!(
        rendered.contains("Esc: clear filter"),
        "a committed filter must advertise how to clear it"
    );
    assert!(
        rendered.contains("[filter: serde]"),
        "and the title must name it"
    );
}

/// A committed filter has to be visible somewhere, or rows just go missing.
#[test]
fn committed_filter_is_announced_in_title_and_help() {
    let mut s = test_state();
    s.active_tab = Tab::Build;
    assert_eq!(filtered_title(" Live Build ", s.filter()), " Live Build ");
    assert!(
        !help_line(&s, "q: quit").contains("Esc"),
        "nothing to clear, so no clear hint"
    );

    handle_key(&mut s, KeyCode::Char('f'));
    for c in "serde".chars() {
        handle_key(&mut s, KeyCode::Char(c));
    }
    handle_key(&mut s, KeyCode::Enter);

    assert_eq!(
        filtered_title(" Live Build ", s.filter()),
        " Live Build [filter: serde] "
    );
    // The title names the filter; the help bar keeps the tab's own keys
    // and only adds the way out.
    let help = help_line(&s, "q: quit  s: sort");
    assert!(
        help.contains("s: sort"),
        "tab keys survive a filter: {help}"
    );
    assert!(
        help.contains("Esc: clear"),
        "and it says how to clear: {help}"
    );
}

#[test]
fn handle_key_scroll_is_per_tab() {
    let mut s = test_state();
    s.active_tab = Tab::Store;
    s.store_scroll.max_offset = 10;
    handle_key(&mut s, KeyCode::Down);
    handle_key(&mut s, KeyCode::Down);
    assert_eq!(s.store_scroll.offset, 2);
    handle_key(&mut s, KeyCode::Up);
    assert_eq!(s.store_scroll.offset, 1);
    // A different tab tracks its own offset.
    s.active_tab = Tab::Transfer;
    s.transfer_scroll.max_offset = 10;
    handle_key(&mut s, KeyCode::Down);
    assert_eq!(s.transfer_scroll.offset, 1);
    assert_eq!(s.store_scroll.offset, 1, "store offset is untouched");
}

#[test]
fn handle_key_clear_resets_build_offset() {
    let mut s = test_state();
    s.active_tab = Tab::Build;
    s.build_scroll.max_offset = 10;
    handle_key(&mut s, KeyCode::PageUp);
    assert_eq!(s.build_scroll.offset, 1);
    handle_key(&mut s, KeyCode::Char('c'));
    assert!(s.events.is_empty());
    assert_eq!(s.build_scroll.offset, 0);
}

#[test]
fn handle_key_store_sort_cycles_and_resets_offset() {
    let mut s = test_state();
    s.active_tab = Tab::Store;
    s.store_scroll.max_offset = 5;
    assert_eq!(s.sort_mode.label(), "size");
    for _ in 0..3 {
        handle_key(&mut s, KeyCode::Down);
    }
    assert_eq!(s.store_scroll.offset, 3);
    handle_key(&mut s, KeyCode::Char('s'));
    assert_eq!(s.sort_mode.label(), "hits");
    assert_eq!(s.store_scroll.offset, 0);
}

#[test]
fn handle_key_filter_input_resets_affected_viewports() {
    let mut s = test_state();
    s.active_tab = Tab::Store;
    s.store_scroll.max_offset = 5;
    s.store_scroll.offset = 3;
    handle_key(&mut s, KeyCode::Char('f'));
    handle_key(&mut s, KeyCode::Char('s'));
    assert_eq!(s.store_scroll.offset, 0);
    s.store_scroll.offset = 3;
    handle_key(&mut s, KeyCode::Backspace);
    assert_eq!(s.store_scroll.offset, 0);
}

#[test]
fn draw_ui_renders_every_tab_without_panicking() {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    for tab in [
        Tab::Build,
        Tab::Projects,
        Tab::Store,
        Tab::Transfer,
        Tab::Why,
    ] {
        let mut state = test_state();
        state.active_tab = tab;
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal
            .draw(|frame| draw_ui(frame, &mut state))
            .expect("draw should succeed");
        // The draw must produce visible content (the tab bar + body), not
        // a blank screen — proves the per-tab draw paths actually ran.
        let buffer = terminal.backend().buffer().clone();
        let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(
            rendered.trim().chars().any(|c| !c.is_whitespace()),
            "tab {tab:?} should render visible content"
        );
    }
}

#[test]
fn load_history_keeps_recent_builds_once_and_tails_from_there() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("events.jsonl");
    let cutoff = chrono::Utc::now() - chrono::Duration::hours(1);

    let mut old = sample_build_event("old", EventResult::Miss, 1, 1);
    old.ts = cutoff - chrono::Duration::seconds(1);
    let mut edge = sample_build_event("edge", EventResult::LocalHit, 1, 1);
    edge.ts = cutoff;
    let recent = sample_build_event("recent", EventResult::LocalHit, 1, 1);
    for event in [&old, &edge, &recent] {
        events::log_event(&log, event).unwrap();
    }
    let beat = HeartbeatEvent {
        schema: 1,
        event: "heartbeat".to_string(),
        ts: chrono::Utc::now(),
        eta_s: None,
        crate_name: "gone".to_string(),
        root: "/w".to_string(),
        pid: 1,
        elapsed_s: 1,
        typical_s: None,
    };
    events::log_heartbeat(&log, &beat).unwrap();

    // Builds that finished before the monitor opened are listed (#1081).
    let (mut tailer, history) = load_history(log.clone(), cutoff);
    let names: Vec<&str> = history.iter().map(|e| e.crate_name.as_str()).collect();
    assert_eq!(names, ["edge", "recent"]);

    // The tail starts after the history: nothing is delivered twice.
    assert!(tailer.poll().unwrap().is_empty());
    let later = sample_build_event("later", EventResult::Miss, 1, 1);
    events::log_event(&log, &later).unwrap();
    let tailed = tailer.poll().unwrap();
    assert_eq!(tailed.len(), 1);
    assert_eq!(tailed[0].crate_name, "later");
}

#[test]
fn monitor_history_defaults_to_one_hour() {
    assert_eq!(MONITOR_HISTORY.secs(), 3600);
    assert_eq!(SinceWindow::whole_hours(3).secs(), 3 * 3600);
}

fn sample_build_event(
    crate_name: &str,
    result: events::EventResult,
    elapsed_ms: u64,
    size: u64,
) -> events::BuildEvent {
    events::BuildEvent {
        ts: chrono::Utc::now(),
        session_id: String::new(),
        demands: Vec::new(),
        crate_name: crate_name.to_string(),
        version: "0.1.0".to_string(),
        result,
        elapsed_ms,
        compile_time_ms: elapsed_ms,
        size,
        cache_key: "0123456789abcdef".to_string(),
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
        root: String::new(),
        passthrough_reason: "linker invocation".to_string(),
        store_error: String::new(),
        store_handed_off: false,
        daemon_store_ms: 0,
        lookup_rejection: String::new(),
        verify_compare: String::new(),
        fallback: false,
        fallback_attempt: None,
        exit_code: Some(0),
        key_fields: Default::default(),
        key_diff: Vec::new(),
        key_externs: Default::default(),
        key_externs_recorded: false,
        unit_id: String::new(),
        extern_units: Default::default(),
    }
}

fn sample_stats_entry(crate_name: &str, size: u64, hits: u64) -> daemon::StatsEntry {
    daemon::StatsEntry {
        cache_key: "0123456789abcdef".to_string(),
        crate_name: crate_name.to_string(),
        crate_type: "lib".to_string(),
        profile: "debug".to_string(),
        size,
        hit_count: hits,
        created_at: "2025-01-01 00:00:00".to_string(),
        last_accessed: "2025-01-01 00:00:00".to_string(),
        content_hash: None,
    }
}

#[test]
fn draw_stats_bar_renders_healthy_connected_daemon() {
    // The existing populated render exercises the offline/empty arms of
    // draw_stats_bar. This drives the "healthy" combinations: daemon
    // connected + service installed (daemon_tag ""), non-zero event totals
    // (hit-rate %), max_size > 0 (store %), a known daemon version, and a
    // configured remote. Covers draw_stats_bar's connected branches.
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    let mut state = test_state();
    state.active_tab = Tab::Build;
    state.service_installed = true;
    state.config.remote = Some(crate::config::RemoteConfig::test_s3("b", "p"));
    state.stats_loaded = true;

    let snap = &mut state.stats_snapshot;
    snap.daemon_connected = true;
    snap.daemon_version = "9.9.9".to_string();
    snap.daemon_build_epoch = 4242;
    snap.max_size = 10_000_000;
    snap.total_size = 4_000_000;
    snap.event_stats.local_hits = 7;
    snap.event_stats.prefetch_hits = 1;
    snap.event_stats.remote_hits = 2;
    snap.event_stats.dups = 1;
    snap.event_stats.misses = 3;
    snap.event_stats.total_elapsed_ms = 5000;
    snap.event_stats.miss_elapsed_ms = 3000;

    let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
    terminal
        .draw(|frame| draw_ui(frame, &mut state))
        .expect("healthy-daemon draw should succeed");
    let buffer = terminal.backend().buffer().clone();
    let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
    // The connected daemon's version surfaces in the stats bar.
    assert!(
        rendered.contains("9.9.9"),
        "connected daemon version should render in the stats bar"
    );
}

#[test]
fn draw_ui_renders_populated_tabs_without_panicking() {
    use events::EventResult;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    for tab in [Tab::Build, Tab::Store, Tab::Why, Tab::Transfer] {
        let mut state = test_state();
        state.active_tab = tab;
        // Build events (varied results incl. a passthrough) drive the build +
        // passthrough tab row rendering and the sparkline.
        state.events = vec![
            sample_build_event("serde", EventResult::Miss, 4200, 2_000_000),
            sample_build_event("tokio", EventResult::LocalHit, 30, 1_500_000),
            sample_build_event("build.rs", EventResult::Passthrough, 80, 0),
        ];
        // Cached-entry rows drive the store table.
        state.stats_snapshot.entries = vec![
            sample_stats_entry("serde", 2_000_000, 5),
            sample_stats_entry("tokio", 1_500_000, 2),
        ];
        state.stats_snapshot.entry_count = 2;
        state.stats_snapshot.total_size = 3_500_000;
        state.stats_loaded = true;
        state.refresh_sessions(chrono::Utc::now());
        // Transfer-tab counters/speeds.
        state.stats_snapshot.uploads_completed = 3;
        state.upload_speed_bps = 2_500_000.0;
        state.download_speed_bps = 800_000.0;

        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal
            .draw(|frame| draw_ui(frame, &mut state))
            .expect("populated draw should succeed");
        let buffer = terminal.backend().buffer().clone();
        let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
        // Populated tabs surface a crate name we seeded.
        if matches!(tab, Tab::Build | Tab::Store | Tab::Why) {
            assert!(
                rendered.contains("serde")
                    || rendered.contains("tokio")
                    || rendered.contains("build.rs"),
                "tab {tab:?} should render seeded data"
            );
        }
    }
}

#[test]
fn draw_projects_tab_renders_populated_scan() {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    let mut state = test_state();
    state.active_tab = Tab::Projects;
    {
        let mut scan = state.project_scan.lock().unwrap();
        scan.project_targets = vec![cli::TargetEntry {
            path: std::path::PathBuf::from("/work/myproj/target"),
            size: 5_000_000,
            cached_bytes: 3_000_000,
            estimated_reclaimable_bytes: 2_000_000,
            scan_identity: None,
            profiles: vec!["debug".to_string(), "release".to_string()],
            breakdown: cli::CategoryBreakdown::default(),
            stale: false,
        }];
        scan.scanning = false;
        scan.scanned = true;
    }

    let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
    terminal
        .draw(|frame| draw_ui(frame, &mut state))
        .expect("projects draw should succeed");
    let buffer = terminal.backend().buffer().clone();
    let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
    assert!(
        rendered.contains("myproj") || rendered.contains("target"),
        "projects tab should render the scanned target path"
    );
    assert!(
        rendered.contains("kache projects"),
        "projects overview title should render: {rendered}"
    );
}
#[test]
fn tab_titles_advance_by_label_width_plus_gutter() {
    let titles = tab_titles();
    assert_eq!(titles[0].2, 0);
    for pair in titles.windows(2) {
        let (_, label, start) = pair[0];
        assert_eq!(pair[1].2, start + label.len() as u16 + 2, "{label:?}");
    }
}

/// The tailer is read only while not paused, a heartbeat shows up as an
/// in-flight compile, and the crate's completing event clears it.
#[test]
fn ingest_respects_pause_and_settles_heartbeats() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("events.jsonl");
    let mut s = test_state();
    s.tailer = EventTailer::from_start(log.clone());

    let beat = HeartbeatEvent {
        schema: 1,
        event: "heartbeat".to_string(),
        ts: chrono::Utc::now(),
        eta_s: Some(60),
        crate_name: "gkrust".to_string(),
        root: "/w".to_string(),
        pid: 4242,
        elapsed_s: 30,
        typical_s: Some(90),
    };
    events::log_heartbeat(&log, &beat).unwrap();

    s.paused = true;
    s.ingest_tailed_records();
    assert!(s.live_heartbeats.is_empty(), "paused: nothing is read");
    assert!(s.in_flight_view().is_empty());

    s.paused = false;
    s.ingest_tailed_records();
    assert_eq!(s.live_heartbeats.len(), 1, "resumed: the beat was replayed");
    assert_eq!(s.in_flight_view()[0].crate_name, "gkrust");

    // The same crate compiling in another tree is a different unit: its
    // beat must survive this tree's completion (crate AND root match).
    let elsewhere = HeartbeatEvent {
        pid: 4343,
        root: "/other".to_string(),
        ..beat.clone()
    };
    events::log_heartbeat(&log, &elsewhere).unwrap();
    s.ingest_tailed_records();
    assert_eq!(s.live_heartbeats.len(), 2);

    let mut done = sample_build_event("gkrust", EventResult::Miss, 100, 1);
    done.root = "/w".to_string();
    events::log_event(&log, &done).unwrap();
    s.ingest_tailed_records();
    assert_eq!(s.events.len(), 1);
    assert_eq!(
        s.live_heartbeats.len(),
        1,
        "completion ends this tree's in-flight row only"
    );
    assert_eq!(s.live_heartbeats[&4343].1.root, "/other");
}

#[test]
fn background_work_is_due_only_when_unpaused_idle_and_on_schedule() {
    let mut s = test_state();
    s.last_stats_fetch = Instant::now() - SNAPSHOT_REFRESH_INTERVAL;
    s.last_project_refresh = Instant::now() - PROJECT_REFRESH_INTERVAL;
    s.active_tab = Tab::Projects;
    assert!(s.stats_fetch_due());
    assert!(s.project_scan_due());

    s.paused = true;
    assert!(!s.stats_fetch_due(), "paused starts nothing");
    assert!(!s.project_scan_due());
    s.paused = false;

    s.stats_fetch_in_flight = true;
    assert!(!s.stats_fetch_due(), "one fetch at a time");
    s.stats_fetch_in_flight = false;

    s.last_stats_fetch = Instant::now();
    assert!(!s.stats_fetch_due(), "not before the interval");

    s.active_tab = Tab::Build;
    assert!(
        !s.project_scan_due(),
        "scans only while Projects is showing"
    );
    s.active_tab = Tab::Projects;
    s.last_project_refresh = Instant::now();
    assert!(!s.project_scan_due());
}

#[test]
fn terminal_events_dispatch_presses_and_mouse_only() {
    let area = Rect::new(0, 0, 120, 40);
    let press = |kind| KeyEvent::new_with_kind(KeyCode::Char('q'), KeyModifiers::NONE, kind);

    let mut s = test_state();
    handle_terminal_event(&mut s, Event::Key(press(KeyEventKind::Release)), area);
    assert!(!s.should_quit, "a release is not a keystroke");
    handle_terminal_event(&mut s, Event::Key(press(KeyEventKind::Repeat)), area);
    assert!(!s.should_quit);
    handle_terminal_event(&mut s, Event::Resize(80, 24), area);
    assert!(!s.should_quit);
    handle_terminal_event(&mut s, Event::Key(press(KeyEventKind::Press)), area);
    assert!(s.should_quit);

    let mut s = test_state();
    let (_, _, store_x) = tab_titles()[3];
    handle_terminal_event(
        &mut s,
        Event::Mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: store_x,
            row: 0,
            modifiers: KeyModifiers::NONE,
        }),
        area,
    );
    assert_eq!(s.active_tab, Tab::Store, "mouse events reach the handler");
}

#[test]
fn mouse_clicks_off_the_tab_row_do_nothing_and_the_wheel_goes_both_ways() {
    let area = Rect::new(0, 0, 120, 40);
    let mut s = test_state();
    let (_, _, store_x) = tab_titles()[2];
    handle_mouse(
        &mut s,
        MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: store_x,
            row: 5,
            modifiers: KeyModifiers::NONE,
        },
        area,
    );
    assert_eq!(
        s.active_tab,
        Tab::Build,
        "a click in the body is not a tab click"
    );

    s.active_tab = Tab::Store;
    s.store_scroll.visible_range(100, 10);
    let wheel = |kind| MouseEvent {
        kind,
        column: 40,
        row: 20,
        modifiers: KeyModifiers::NONE,
    };
    handle_mouse(&mut s, wheel(MouseEventKind::ScrollDown), area);
    handle_mouse(&mut s, wheel(MouseEventKind::ScrollDown), area);
    assert_eq!(s.store_scroll.offset, 6);
    handle_mouse(&mut s, wheel(MouseEventKind::ScrollUp), area);
    assert_eq!(s.store_scroll.offset, 3);

    // A tiny terminal ignores the mouse along with everything else.
    s.active_tab = Tab::Build;
    handle_mouse(
        &mut s,
        MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: store_x,
            row: 0,
            modifiers: KeyModifiers::NONE,
        },
        Rect::new(0, 0, 40, 10),
    );
    assert_eq!(s.active_tab, Tab::Build);
}

#[test]
fn terminal_size_guard_is_exact_on_both_axes() {
    assert!(!terminal_too_small(Rect::new(0, 0, MIN_WIDTH, MIN_HEIGHT)));
    assert!(terminal_too_small(Rect::new(
        0,
        0,
        MIN_WIDTH - 1,
        MIN_HEIGHT
    )));
    assert!(terminal_too_small(Rect::new(
        0,
        0,
        MIN_WIDTH,
        MIN_HEIGHT - 1
    )));
    assert!(terminal_too_small(Rect::new(
        0,
        0,
        MIN_WIDTH - 1,
        MIN_HEIGHT - 1
    )));
    assert!(!terminal_too_small(Rect::new(0, 0, 200, 60)));
}

#[test]
fn lookup_series_counts_an_event_from_this_instant_in_the_newest_column() {
    let now = chrono::Utc::now();
    let mut event = sample_build_event("x", EventResult::LocalHit, 1, 1);
    event.ts = now;
    let (hits, misses) = lookup_series(&[event], now, Duration::from_secs(300), 4);
    assert_eq!(hits, vec![0, 0, 0, 1]);
    assert_eq!(misses, vec![0; 4]);
}

#[test]
fn fmt_window_falls_back_when_the_unit_does_not_divide() {
    assert_eq!(fmt_window(Duration::from_secs(45_000)), "750m");
    assert_eq!(fmt_window(Duration::from_secs(129_600)), "36h");
    assert_eq!(fmt_window(SPARK_WINDOW), "5m");
}

/// Render the sparkline alone into a `width`×`height` area.
fn rendered_sparkline(state: &AppState, width: u16, height: u16) -> String {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| draw_sparkline(frame, state, frame.area()))
        .unwrap();
    let buffer = terminal.backend().buffer();
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn sparkline_labels_its_axis_and_says_when_nothing_happened() {
    let mut s = test_state();
    let screen = rendered_sparkline(&s, 80, 5);
    assert!(screen.contains("Lookups · last 5m"), "{screen}");
    assert!(screen.contains("5m ago"), "{screen}");
    assert!(screen.contains("now →"), "{screen}");
    assert!(
        screen.contains("hit    0") && screen.contains("miss   0"),
        "{screen}"
    );
    assert!(screen.contains("no lookups in the last 5m"), "{screen}");

    // Only misses: the idle note goes away (hits + misses, not a product).
    s.events = vec![sample_build_event("a", EventResult::Miss, 1, 1)];
    let screen = rendered_sparkline(&s, 80, 5);
    assert!(!screen.contains("no lookups"), "{screen}");
    assert!(screen.contains("miss   1"), "{screen}");

    // Equal counts: still not idle (a difference would say zero).
    s.events
        .push(sample_build_event("b", EventResult::LocalHit, 1, 1));
    let screen = rendered_sparkline(&s, 80, 5);
    assert!(!screen.contains("no lookups"), "{screen}");
    assert!(screen.contains("hit    1"), "{screen}");

    // The whole Build tab carries it too.
    let screen = rendered_tab(&mut s, Tab::Build);
    assert!(screen.contains("Lookups · last 5m"), "{screen}");
}

/// Below three inner rows or twelve inner columns there is no room for
/// two strips and an axis, so the panel draws its frame and nothing else.
#[test]
fn sparkline_needs_three_rows_and_twelve_columns_inside_the_frame() {
    let s = test_state();
    for (width, height, drawn) in [
        (14u16, 5u16, true),
        (13, 5, false),
        (14, 4, false),
        (13, 4, false),
        (80, 5, true),
    ] {
        let screen = rendered_sparkline(&s, width, height);
        assert_eq!(
            screen.contains("hit"),
            drawn,
            "{width}x{height} should draw={drawn}:\n{screen}"
        );
    }
}

/// The columns that survive a narrow terminal: Compile and Exit and the
/// Projects profile need 78/80 columns, Type/Profile on Store need 84.
#[test]
fn mid_priority_columns_have_their_own_thresholds() {
    let mut state = populated_state();
    for (tab, column, min_width) in [
        (Tab::Build, "Compile", 78u16),
        (Tab::Store, "Profile", 84),
        (Tab::Projects, "[debug]", 80),
    ] {
        for width in [min_width - 1, min_width, 120] {
            let screen = rendered_lines(&mut state, tab, width, 40).join("\n");
            assert_eq!(
                screen.contains(column),
                width >= min_width,
                "{tab:?} at {width}: {column:?}\n{screen}"
            );
        }
    }
    let screen = rendered_lines(&mut state, Tab::Projects, 120, 40).join("\n");
    assert!(screen.contains("Total (1 project)"), "{screen}");
    assert!(
        screen.contains("Fprint:"),
        "totals carry the breakdown when wide"
    );
    let screen = rendered_lines(&mut state, Tab::Projects, 80, 24).join("\n");
    assert!(screen.contains("Total (1 project)"), "{screen}");
    assert!(!screen.contains("Fprint:"), "and drop it when narrow");
}

#[test]
fn changing_build_resets_both_panels_and_a_lone_build_can_be_pinned() {
    let mut s = test_state();
    s.push_event(session_event("a", EventResult::Miss, "/w/one", "s1", 500));
    s.refresh_sessions(chrono::Utc::now());
    assert!(s.selected_session.is_none());
    // Down on the only row pins it rather than doing nothing.
    handle_key(&mut s, KeyCode::Down);
    assert_eq!(s.selected_session.as_deref(), Some("id:s1"));

    // Scroll both panels away from their edges, then let a newer build
    // arrive: the pin holds and nothing resets.
    s.build_scroll.visible_range(50, 10);
    s.build_scroll.scroll_up_by(5);
    s.why_scroll.visible_range(50, 10);
    s.why_scroll.scroll_down_by(7);
    s.push_event(session_event("b", EventResult::Miss, "/w/two", "s2", 1));
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.sessions[0].key, "id:s2");
    assert_eq!(s.selected_session().unwrap().key, "id:s1");
    assert_eq!((s.build_scroll.offset, s.why_scroll.offset), (5, 7));

    // Picking the other build starts both panels from their edge.
    handle_key(&mut s, KeyCode::Up);
    assert_eq!(s.selected_session().unwrap().key, "id:s2");
    assert_eq!((s.build_scroll.offset, s.why_scroll.offset), (0, 0));

    // Following, a new top build resets them too.
    handle_key(&mut s, KeyCode::Up);
    assert!(s.selected_session.is_none());
    s.build_scroll.visible_range(50, 10);
    s.build_scroll.scroll_up_by(3);
    s.push_event(session_event("c", EventResult::Miss, "/w/three", "s3", 0));
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.selected_session().unwrap().key, "id:s3");
    assert_eq!(s.build_scroll.offset, 0);
}
fn why_text(s: &mut AppState, width: u16) -> Vec<String> {
    why_lines(s, width)
        .iter()
        .map(|line| line.to_string())
        .collect()
}

#[test]
fn build_tab_layout_thresholds() {
    // No builds: no Builds panel at all, not an empty one.
    let mut s = test_state();
    let screen = rendered_tab(&mut s, Tab::Build);
    assert!(!screen.contains("Builds ·"), "{screen}");

    // The sparkline needs 30 content rows (31 with the tab bar).
    let mut s = populated_state();
    let tall = rendered_lines(&mut s, Tab::Build, 100, 31).join("\n");
    assert!(tall.contains("Lookups · last"), "{tall}");
    let short = rendered_lines(&mut s, Tab::Build, 100, 30).join("\n");
    assert!(!short.contains("Lookups · last"), "{short}");
    assert!(
        short.contains("Builds ·"),
        "the builds panel stays: {short}"
    );

    // The long help needs 100 columns.
    let wide = rendered_lines(&mut s, Tab::Build, 100, 40).join("\n");
    assert!(
        wide.contains("Enter: why") && wide.contains("PgUp PgDn End"),
        "{wide}"
    );
    let narrow = rendered_lines(&mut s, Tab::Build, 99, 40).join("\n");
    assert!(
        narrow.contains("⏎: why") && !narrow.contains("Enter: why"),
        "{narrow}"
    );
}

/// A body taller than the panel gets a position marker, and so does a
/// body scrolled away from the top even when its end is in view.
#[test]
fn why_tab_marks_its_scroll_position_only_when_there_is_more() {
    let mut s = test_state();
    s.events = vec![session_event("a", EventResult::Miss, "/w", "s1", 5)];
    s.refresh_sessions(chrono::Utc::now());
    let screen = rendered_tab(&mut s, Tab::Why);
    assert!(!screen.contains('–'), "everything fits: {screen}");

    for i in 0..30 {
        let mut e = session_event(&format!("cc{i}"), EventResult::Passthrough, "/w", "s1", 4);
        e.passthrough_reason = "unsupported|flag".to_string();
        s.push_event(e);
    }
    s.refresh_sessions(chrono::Utc::now());
    let screen = rendered_lines(&mut s, Tab::Why, 100, 20).join("\n");
    assert!(screen.contains("1–16 of"), "{screen}");
    assert!(screen.contains("PgUp PgDn"), "{screen}");

    s.active_tab = Tab::Why;
    handle_key(&mut s, KeyCode::End);
    let screen = rendered_lines(&mut s, Tab::Why, 100, 20).join("\n");
    let total = why_lines(&mut s, 98).len();
    assert!(
        screen.contains(&format!("{total} of {total}")),
        "scrolled to the end: {screen}"
    );
}

#[test]
fn why_cost_strip_mentions_copies_only_when_there_are_any() {
    let mut s = test_state();
    let mut hit = session_event("a", EventResult::LocalHit, "/w", "s1", 5);
    hit.reflinked_bytes = 1000;
    s.events = vec![hit];
    s.refresh_sessions(chrono::Utc::now());
    let text = why_text(&mut s, 120).join("\n");
    assert!(text.contains("restored 1000 B"), "{text}");
    assert!(!text.contains("by copy"), "{text}");
    assert!(text.contains("none: every lookup hit"), "{text}");
    assert!(!text.contains("explain_miss"), "no misses, no hint: {text}");

    let mut hit = session_event("b", EventResult::LocalHit, "/w", "s1", 4);
    hit.copied_bytes = 1000;
    s.push_event(hit);
    s.refresh_sessions(chrono::Utc::now());
    let text = why_text(&mut s, 120).join("\n");
    assert!(text.contains("50% by copy"), "{text}");

    // A build with only passthroughs looked nothing up.
    let mut s = test_state();
    let mut pt = session_event("cc", EventResult::Passthrough, "/w", "s2", 3);
    pt.passthrough_reason = "unsupported|flag".to_string();
    s.events = vec![pt];
    s.refresh_sessions(chrono::Utc::now());
    let text = why_text(&mut s, 120).join("\n");
    assert!(text.contains("none: nothing was looked up"), "{text}");
}

#[test]
fn why_counts_misses_in_english_and_says_when_capped() {
    let mut s = test_state();
    s.events = vec![session_event("a", EventResult::Miss, "/w", "s1", 5)];
    s.refresh_sessions(chrono::Utc::now());
    let text = why_text(&mut s, 120).join("\n");
    assert!(text.contains("Misses by cause · 1 miss\n"), "{text}");
    assert!(!text.contains("Misses by cause · 1 misses"), "{text}");
    assert!(!text.contains("analyzed)"), "{text}");
    assert!(!text.contains("none:"), "there is a miss: {text}");
    assert!(
        !text.contains("Passthroughs by reason"),
        "none to group: {text}"
    );
    assert!(!text.contains("Chronic misses"), "none: {text}");

    for i in 0..(tui_sessions::MAX_ANALYZED_MISSES + 9) {
        s.push_event(session_event(
            &format!("m{i}"),
            EventResult::Miss,
            "/w",
            "s1",
            4,
        ));
    }
    s.refresh_sessions(chrono::Utc::now());
    let text = why_text(&mut s, 120).join("\n");
    assert!(
        text.contains(&format!(
            "{} misses (newest {} of {} analyzed)",
            tui_sessions::MAX_ANALYZED_MISSES + 10,
            tui_sessions::MAX_ANALYZED_MISSES,
            tui_sessions::MAX_ANALYZED_MISSES + 10
        )),
        "{text}"
    );
}

/// Every bar line and every passthrough row is laid out inside the
/// width it was asked for, including long reasons and the probe note.
fn fit_state() -> AppState {
    let mut s = test_state();
    let long =
        "unsupported|cc flag -march=native -mtune=native -fno-omit-frame-pointer -Wl,--as-needed";
    for i in 0..3 {
        let mut e = session_event(
            &format!("a_rather_long_crate_name_{i}"),
            EventResult::Passthrough,
            "/w",
            "s1",
            5,
        );
        e.passthrough_reason = long.to_string();
        s.push_event(e);
    }
    let mut probe = session_event("rustc", EventResult::Passthrough, "/w", "s1", 4);
    probe.passthrough_reason =
        "not-a-compile|--print cfg with a long tail of arguments, and then some more".to_string();
    s.push_event(probe);
    s.push_event(session_event("m", EventResult::Miss, "/w", "s1", 3));
    s.refresh_sessions(chrono::Utc::now());
    s
}

#[test]
fn why_lines_fit_the_width_they_are_given() {
    let mut s = fit_state();
    for width in [50u16, 60, 78, 100] {
        for line in why_lines(&mut s, width) {
            let text = line.to_string();
            let laid_out = text.contains('░')
                || text.contains('█')
                || text.trim_start().starts_with(|c: char| c.is_ascii_digit());
            if laid_out {
                assert!(
                    line.width() <= width as usize,
                    "{width}: {} cells: {text:?}",
                    line.width()
                );
            }
        }
    }
    // Exact widths: the crate column is width/4 (8..22) and the kind
    // column width/6 (6..14), so at 60 a passthrough row shows 15 cells
    // of crate and 10 of kind, and at 88 the full 22 and 14.
    let at_60 = why_text(&mut s, 60).join("\n");
    assert!(
        at_60.contains("  a_rather_long_…  unsupport…  cc flag"),
        "{at_60}"
    );
    let at_88 = why_text(&mut s, 88).join("\n");
    assert!(
        at_88.contains("  a_rather_long_crate_n…  unsupported     cc flag"),
        "{at_88}"
    );
    // The probe group line at 100: label, then the note, and the label
    // is cut so that exactly the note and one cell of slack remain.
    let group_line = why_lines(&mut s, 100)
        .iter()
        .map(|line| line.to_string())
        .find(|line| line.contains("(queries, not compiles)"))
        .unwrap();
    assert_eq!(
        Line::from(group_line.as_str()).width(),
        99,
        "{group_line:?}"
    );
    let text = why_text(&mut s, 100).join("\n");
    assert!(text.contains("(queries, not compiles)"), "{text}");
    assert!(text.contains("not-a-compile: --print cfg with"), "{text}");

    // The filter narrows the passthrough list and says so.
    s.why_filter = "rustc".to_string();
    s.active_tab = Tab::Why;
    let text = why_text(&mut s, 100).join("\n");
    assert!(
        text.contains("Passthroughs in this build · 1 matching \"rustc\""),
        "{text}"
    );
    assert!(
        text.contains("  rustc") && !text.contains("a_rather_long"),
        "{text}"
    );
    s.why_filter = "-march".to_string();
    let text = why_text(&mut s, 100).join("\n");
    assert!(text.contains("· 3 matching"), "reason matches too: {text}");
    s.why_filter = "zzz".to_string();
    let text = why_text(&mut s, 100).join("\n");
    assert!(text.contains("none match the filter"), "{text}");
}
#[test]
fn enter_opens_why_only_from_build() {
    let mut s = test_state();
    s.active_tab = Tab::Store;
    handle_key(&mut s, KeyCode::Enter);
    assert_eq!(s.active_tab, Tab::Store);
    s.active_tab = Tab::Build;
    handle_key(&mut s, KeyCode::Enter);
    assert_eq!(s.active_tab, Tab::Why);
}

#[test]
fn fmt_saved_ms_switches_units_at_one_second() {
    assert_eq!(fmt_saved_ms(999), "999ms");
    assert_eq!(fmt_saved_ms(1000), "1s");
}

/// A heartbeat for a tree keeps that tree's newest build running past
/// the grace period.
#[test]
fn in_flight_compile_keeps_its_build_running() {
    let mut s = test_state();
    s.events = vec![session_event("a", EventResult::Miss, "/w", "s1", 300)];
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.sessions[0].state, SessionState::Finished);
    let beat = HeartbeatEvent {
        schema: 1,
        event: "heartbeat".to_string(),
        ts: chrono::Utc::now(),
        crate_name: "b".to_string(),
        root: "/w".to_string(),
        pid: 7,
        elapsed_s: 5,
        typical_s: None,
        eta_s: None,
    };
    s.live_heartbeats.insert(7, (Instant::now(), beat));
    s.refresh_sessions(chrono::Utc::now());
    assert_eq!(s.sessions[0].state, SessionState::Live);
    let screen = rendered_tab(&mut s, Tab::Build);
    assert!(screen.contains("running"), "{screen}");
    assert!(screen.contains("5s elapsed"), "{screen}");
}

/// Render `tab` and return the raw buffer, for assertions on style.
fn rendered_buffer(state: &mut AppState, tab: Tab, width: u16, height: u16) -> Buffer {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    state.active_tab = tab;
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal
        .draw(|frame| draw_ui(frame, state))
        .expect("draw should succeed");
    terminal.backend().buffer().clone()
}

fn row_text(buffer: &Buffer, y: u16) -> String {
    (0..buffer.area.width)
        .map(|x| buffer[(x, y)].symbol())
        .collect()
}

/// The Builds table: a lone build gets its row, the selected row is
/// highlighted, zero counts are muted, headers sit left and numbers
/// right, and the pass and saved columns need 100 columns.
#[test]
fn builds_table_layout_and_styling() {
    let mut s = test_state();
    s.events = vec![
        session_event("a", EventResult::Miss, "/w/one", "s1", 5),
        session_event("b", EventResult::LocalHit, "/w/two", "s2", 400),
    ];
    s.refresh_sessions(chrono::Utc::now());

    let buffer = rendered_buffer(&mut s, Tab::Build, 100, 40);
    let rows: Vec<String> = (0..40).map(|y| row_text(&buffer, y)).collect();
    let header_y = rows
        .iter()
        .position(|row| row.contains("Started") && row.contains("State"))
        .unwrap_or_else(|| panic!("{}", rows.join("\n")));
    let header = &rows[header_y];
    assert!(header.starts_with("│  Build"), "labels left: {header:?}");
    assert!(
        header.contains("pass") && header.contains("saved"),
        "{header:?}"
    );
    assert!(
        header.trim_end_matches('│').trim_end().ends_with("saved"),
        "numbers right: {header:?}"
    );

    let selected_y = header_y + 1;
    assert!(rows[selected_y].contains("▸ one"), "{}", rows[selected_y]);
    let mark = rows[selected_y].find('▸').unwrap() as u16;
    let style = &buffer[(mark + 2, selected_y as u16)];
    assert!(style.modifier.contains(Modifier::REVERSED), "{style:?}");
    assert!(style.modifier.contains(Modifier::BOLD), "{style:?}");

    // "two" has 0 misses: that cell is muted; its 1 hit is not.
    let other_y = selected_y + 1;
    assert!(rows[other_y].contains("two"), "{}", rows[other_y]);
    // Column, not byte offset: the border glyphs are multi-byte.
    let col_of = |row: &str, needle: &str| -> usize {
        let chars: Vec<char> = row.chars().collect();
        let needle: Vec<char> = needle.chars().collect();
        chars
            .windows(needle.len())
            .position(|window| window == needle.as_slice())
            .unwrap()
    };
    let miss_x = col_of(header, "miss") + 3;
    let hit_x = col_of(header, "hit") + 2;
    assert_eq!(buffer[(miss_x as u16, other_y as u16)].symbol(), "0");
    assert_eq!(buffer[(miss_x as u16, other_y as u16)].fg, Color::DarkGray);
    assert_eq!(buffer[(hit_x as u16, other_y as u16)].symbol(), "1");
    assert_eq!(buffer[(hit_x as u16, other_y as u16)].fg, Color::Green);

    let narrow = rendered_lines(&mut s, Tab::Build, 99, 40);
    let header = narrow
        .iter()
        .find(|row| row.contains("Started") && row.contains("State"))
        .unwrap();
    assert!(
        !header.contains("pass") && !header.contains("saved"),
        "{header:?}"
    );

    // One build alone still gets its row (border, header, one row).
    let mut s = test_state();
    s.events = vec![session_event("a", EventResult::Miss, "/w/one", "s1", 5)];
    s.refresh_sessions(chrono::Utc::now());
    let screen = rendered_tab(&mut s, Tab::Build);
    assert!(screen.contains("▸ one"), "{screen}");
}
