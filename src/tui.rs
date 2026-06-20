use anyhow::Result;
use bytesize::ByteSize;
use crossterm::ExecutableCommand;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use ratatui::widgets::*;
use std::io::stdout;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::cli;
use crate::config::Config;
use crate::daemon;
use crate::events::{self, BuildEvent, EventResult, EventTailer};

// ── Tabs & panels ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tab {
    Build,
    Projects,
    Store,
    Transfer,
    Passthrough,
}

fn tab_needs_entries(tab: Tab) -> bool {
    matches!(tab, Tab::Store)
}

// ── Sort mode (shared between tabs) ────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum SortMode {
    Size,
    Hits,
    Age,
    Name,
}

impl SortMode {
    fn label(&self) -> &str {
        match self {
            SortMode::Size => "size",
            SortMode::Hits => "hits",
            SortMode::Age => "age",
            SortMode::Name => "name",
        }
    }

    fn next(&self) -> Self {
        match self {
            SortMode::Size => SortMode::Hits,
            SortMode::Hits => SortMode::Age,
            SortMode::Age => SortMode::Name,
            SortMode::Name => SortMode::Size,
        }
    }
}

// ── Stats snapshot — delegates to cli::fetch_stats_snapshot ─────────────────

/// Type alias for the shared snapshot used by TUI and CLI.
type StatsSnapshot = cli::StatsSnapshot;

/// Replace the user's home directory prefix with `~` for shorter, more private display.
fn shorten_home(path: &std::path::Path) -> String {
    if let Some(home) = dirs::home_dir()
        && let Ok(rest) = path.strip_prefix(&home)
    {
        return format!("~/{}", rest.display());
    }
    path.display().to_string()
}

/// Try daemon first, fall back to direct reads. Wrapper for the TUI's 24h default.
fn fetch_stats(config: &Config, include_entries: bool, sort_by: &str) -> StatsSnapshot {
    cli::fetch_stats_snapshot(config, include_entries, sort_by, Some(24))
}

// ── App state ──────────────────────────────────────────────────────────────

/// Project scan data computed in a background thread.
struct ProjectScanData {
    project_targets: Vec<cli::TargetEntry>,
    link_stats: cli::LinkStats,
    scanning: bool,
    scanned: bool,
}

impl Default for ProjectScanData {
    fn default() -> Self {
        Self {
            project_targets: Vec::new(),
            link_stats: cli::LinkStats {
                store_bytes: 0,
                linked_refs: 0,
                saved_bytes: 0,
            },
            scanning: false,
            scanned: false,
        }
    }
}

struct AppState {
    config: Config,
    active_tab: Tab,

    // Build tab
    tailer: EventTailer,
    events: Vec<BuildEvent>,
    scroll_offset: usize,
    filter: String,
    filter_active: bool,

    // Store tab
    sort_mode: SortMode,
    store_scroll: usize,

    // Store + event stats (daemon-first snapshot)
    stats_snapshot: StatsSnapshot,
    stats_loaded: bool,
    last_stats_fetch: Instant,

    // Projects tab (shared with background scanner thread for target/ scanning)
    project_scan: Arc<Mutex<ProjectScanData>>,
    last_project_refresh: Instant,
    project_scroll: usize,

    // Transfer tab
    transfer_scroll: usize,
    passthrough_scroll: usize,
    prev_bytes_uploaded: u64,
    prev_bytes_downloaded: u64,
    upload_speed_bps: f64,
    download_speed_bps: f64,

    // Background result slots
    rustc_version_slot: Arc<Mutex<Option<String>>>,
    stats_result_slot: Arc<Mutex<Option<StatsSnapshot>>>,
    stats_fetch_in_flight: bool,
    stats_fetch_requested_entries: bool,

    should_quit: bool,
    rustc_version: String,
    wrapper_status: String,
    service_installed: bool,
}

const PROJECT_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const SNAPSHOT_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

// ── Entry point ────────────────────────────────────────────────────────────

/// Run the TUI monitor dashboard.
pub fn run_monitor(config: &Config, since_hours: Option<u64>) -> Result<()> {
    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    let tailer = if since_hours.is_some() {
        EventTailer::from_start(config.event_log_path())
    } else {
        EventTailer::new(config.event_log_path())
    };

    let initial_events = if let Some(hours) = since_hours {
        let since = chrono::Utc::now() - chrono::Duration::hours(hours as i64);
        events::read_events_since(&config.event_log_path(), since).unwrap_or_default()
    } else {
        Vec::new()
    };

    let rustc_version_slot: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    {
        let slot = Arc::clone(&rustc_version_slot);
        std::thread::spawn(move || {
            let ver = std::process::Command::new("rustc")
                .arg("--version")
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            if let Ok(mut s) = slot.lock() {
                *s = Some(ver);
            }
        });
    }

    let project_scan = Arc::new(Mutex::new(ProjectScanData::default()));

    // Project scans are expensive on large caches/workspaces, so defer them until the
    // Projects tab is shown.

    // Stats start empty; the first periodic refresh fires immediately (see last_stats_fetch below).
    let stats_snapshot = StatsSnapshot::default();
    let stats_result_slot: Arc<Mutex<Option<StatsSnapshot>>> = Arc::new(Mutex::new(None));

    let service_installed = crate::service::service_file_path()
        .map(|p| p.exists())
        .unwrap_or(false);

    let mut state = AppState {
        config: config.clone(),
        active_tab: Tab::Build,
        tailer,
        events: initial_events,
        scroll_offset: 0,
        filter: String::new(),
        filter_active: false,
        sort_mode: SortMode::Size,
        store_scroll: 0,
        stats_snapshot,
        stats_loaded: false,
        last_stats_fetch: Instant::now() - SNAPSHOT_REFRESH_INTERVAL, // trigger immediate first fetch
        project_scan,
        last_project_refresh: Instant::now(),
        project_scroll: 0,
        transfer_scroll: 0,
        passthrough_scroll: 0,
        prev_bytes_uploaded: 0,
        prev_bytes_downloaded: 0,
        upload_speed_bps: 0.0,
        download_speed_bps: 0.0,
        rustc_version_slot: Arc::clone(&rustc_version_slot),
        stats_result_slot: Arc::clone(&stats_result_slot),
        stats_fetch_in_flight: false,
        stats_fetch_requested_entries: false,
        should_quit: false,
        rustc_version: "\u{2026}".to_string(), // placeholder until background thread completes
        wrapper_status: crate::wrapper_config::wrapper_status_line(),
        service_installed,
    };

    loop {
        // Poll for new build events
        if let Ok(new_events) = state.tailer.poll() {
            state.events.extend(new_events);
        }

        // Check for completed background rustc_version
        if let Ok(mut slot) = state.rustc_version_slot.lock()
            && let Some(ver) = slot.take()
        {
            state.rustc_version = ver;
        }

        // Check for completed background stats fetch
        if let Ok(mut slot) = state.stats_result_slot.lock()
            && let Some(new_snap) = slot.take()
        {
            let previous_entries = if state.stats_fetch_requested_entries {
                Vec::new()
            } else {
                std::mem::take(&mut state.stats_snapshot.entries)
            };
            let mut new_snap = new_snap;
            if !state.stats_fetch_requested_entries {
                new_snap.entries = previous_entries;
            }
            let old_up = state.stats_snapshot.bytes_uploaded;
            let old_down = state.stats_snapshot.bytes_downloaded;
            let interval = SNAPSHOT_REFRESH_INTERVAL.as_secs_f64();
            state.upload_speed_bps =
                (new_snap.bytes_uploaded.saturating_sub(old_up)) as f64 / interval;
            state.download_speed_bps =
                (new_snap.bytes_downloaded.saturating_sub(old_down)) as f64 / interval;
            state.prev_bytes_uploaded = new_snap.bytes_uploaded;
            state.prev_bytes_downloaded = new_snap.bytes_downloaded;
            state.stats_snapshot = new_snap;
            state.stats_loaded = true;
            state.stats_fetch_in_flight = false;
        }

        // Spawn a background stats refresh when due (non-blocking)
        if !state.stats_fetch_in_flight
            && state.last_stats_fetch.elapsed() >= SNAPSHOT_REFRESH_INTERVAL
        {
            state.stats_fetch_in_flight = true;
            state.last_stats_fetch = Instant::now();
            let cfg = state.config.clone();
            let sort = state.sort_mode.label().to_string();
            let include_entries = tab_needs_entries(state.active_tab);
            state.stats_fetch_requested_entries = include_entries;
            let slot = Arc::clone(&state.stats_result_slot);
            std::thread::spawn(move || {
                let snap = fetch_stats(&cfg, include_entries, &sort);
                if let Ok(mut s) = slot.lock() {
                    *s = Some(snap);
                }
            });
        }

        // Refresh target/ scan periodically when on stats tab
        if state.active_tab == Tab::Projects
            && state.last_project_refresh.elapsed() >= PROJECT_REFRESH_INTERVAL
        {
            let is_scanning = state
                .project_scan
                .lock()
                .map(|s| s.scanning)
                .unwrap_or(false);
            if !is_scanning {
                spawn_project_scan(Arc::clone(&state.project_scan), state.config.store_dir());
                state.last_project_refresh = Instant::now();
            }
        }

        terminal.draw(|frame| draw_ui(frame, &state))?;

        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            handle_key(&mut state, key.code);
        }

        if state.should_quit {
            break;
        }
    }

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

/// Spawn a background thread to scan target dirs and compute link stats.
/// Results stream in progressively — each discovered project updates the UI immediately.
fn spawn_project_scan(stats: Arc<Mutex<ProjectScanData>>, store_dir: std::path::PathBuf) {
    if let Ok(mut s) = stats.lock() {
        s.scanning = true;
        // Mark existing entries stale instead of clearing — keeps the UI populated
        for t in s.project_targets.iter_mut() {
            t.stale = true;
        }
    }
    std::thread::spawn(move || {
        // First: compute link stats (fast — just walks the store)
        let link = cli::compute_link_stats(&store_dir);
        if let Ok(mut s) = stats.lock() {
            s.link_stats = link;
        }

        // Then: discover target dirs and scan each one progressively
        let root = std::env::current_dir().unwrap_or_default();
        let mut all_targets = Vec::new();
        cli::find_target_dirs(&root, &mut all_targets);

        // Push each scanned target immediately so the UI updates incrementally.
        // If a path already exists (stale), replace in-place; otherwise append.
        for target in all_targets {
            if let Ok(mut s) = stats.lock() {
                if let Some(existing) = s.project_targets.iter_mut().find(|e| e.path == target.path)
                {
                    *existing = target;
                } else {
                    s.project_targets.push(target);
                }
                // Keep sorted by size descending
                s.project_targets
                    .sort_by_key(|entry| std::cmp::Reverse(entry.size));
            }
        }

        if let Ok(mut s) = stats.lock() {
            // Remove entries that are still stale (no longer exist on disk)
            s.project_targets.retain(|t| !t.stale);
            s.scanning = false;
            s.scanned = true;
        }
    });
}

// ── Key handling ───────────────────────────────────────────────────────────

fn handle_key(state: &mut AppState, key: KeyCode) {
    // Filter input mode
    if state.filter_active {
        match key {
            KeyCode::Esc | KeyCode::Enter => state.filter_active = false,
            KeyCode::Backspace => {
                state.filter.pop();
            }
            KeyCode::Char(c) => state.filter.push(c),
            _ => {}
        }
        return;
    }

    match key {
        KeyCode::Char('q') | KeyCode::Esc => state.should_quit = true,
        // Tab switching
        KeyCode::Char('1') => state.active_tab = Tab::Build,
        KeyCode::Char('2') => {
            state.active_tab = Tab::Projects;
            state.last_project_refresh = Instant::now() - PROJECT_REFRESH_INTERVAL;
        }
        KeyCode::Char('3') => {
            state.active_tab = Tab::Store;
            state.last_stats_fetch = Instant::now() - SNAPSHOT_REFRESH_INTERVAL;
        }
        KeyCode::Char('4') => state.active_tab = Tab::Transfer,
        KeyCode::Char('5') => state.active_tab = Tab::Passthrough,
        KeyCode::BackTab | KeyCode::Tab => match state.active_tab {
            Tab::Build => {
                state.active_tab = Tab::Projects;
                state.last_project_refresh = Instant::now() - PROJECT_REFRESH_INTERVAL;
            }
            Tab::Projects => {
                state.active_tab = Tab::Store;
                state.last_stats_fetch = Instant::now() - SNAPSHOT_REFRESH_INTERVAL;
            }
            Tab::Store => state.active_tab = Tab::Transfer,
            Tab::Transfer => state.active_tab = Tab::Passthrough,
            Tab::Passthrough => state.active_tab = Tab::Build,
        },
        // Scrolling
        KeyCode::Up => match state.active_tab {
            Tab::Build => state.scroll_offset = state.scroll_offset.saturating_sub(1),
            Tab::Projects => state.project_scroll = state.project_scroll.saturating_sub(1),
            Tab::Store => state.store_scroll = state.store_scroll.saturating_sub(1),
            Tab::Transfer => state.transfer_scroll = state.transfer_scroll.saturating_sub(1),
            Tab::Passthrough => {
                state.passthrough_scroll = state.passthrough_scroll.saturating_sub(1)
            }
        },
        KeyCode::Down => match state.active_tab {
            Tab::Build => state.scroll_offset += 1,
            Tab::Projects => state.project_scroll += 1,
            Tab::Store => state.store_scroll += 1,
            Tab::Transfer => state.transfer_scroll += 1,
            Tab::Passthrough => state.passthrough_scroll += 1,
        },
        // Build tab
        KeyCode::Char('f') if state.active_tab == Tab::Build => {
            state.filter_active = true;
        }
        KeyCode::Char('c') if state.active_tab == Tab::Build => {
            state.events.clear();
        }
        // Store tab
        KeyCode::Char('s') if state.active_tab == Tab::Store => {
            state.sort_mode = state.sort_mode.next();
            state.last_stats_fetch = Instant::now() - SNAPSHOT_REFRESH_INTERVAL;
        }
        KeyCode::Char('f') if state.active_tab == Tab::Store => {
            state.filter_active = true;
        }
        KeyCode::Char('f') if state.active_tab == Tab::Passthrough => {
            state.filter_active = true;
        }
        // Projects tab: force refresh
        KeyCode::Char('r') if state.active_tab == Tab::Projects => {
            state.last_project_refresh = Instant::now() - PROJECT_REFRESH_INTERVAL;
        }
        _ => {}
    }
}

// ── Drawing ────────────────────────────────────────────────────────────────

fn draw_ui(frame: &mut Frame, state: &AppState) {
    let area = frame.area();

    // Tab bar at the top
    let chunks = Layout::vertical([
        Constraint::Length(1), // Tab bar
        Constraint::Min(1),    // Content
    ])
    .split(area);

    draw_tab_bar(frame, state, chunks[0]);

    match state.active_tab {
        Tab::Build => draw_build_tab(frame, state, chunks[1]),
        Tab::Projects => draw_projects_tab(frame, state, chunks[1]),
        Tab::Store => draw_store_tab(frame, state, chunks[1]),
        Tab::Transfer => draw_transfer_tab(frame, state, chunks[1]),
        Tab::Passthrough => draw_passthrough_tab(frame, state, chunks[1]),
    }
}

fn draw_tab_bar(frame: &mut Frame, state: &AppState, area: Rect) {
    let style_for = |tab: Tab| -> Style {
        if state.active_tab == tab {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        }
    };

    let tabs = Line::from(vec![
        Span::styled(" [1] Build ", style_for(Tab::Build)),
        Span::raw("  "),
        Span::styled("[2] Projects", style_for(Tab::Projects)),
        Span::raw("  "),
        Span::styled("[3] Store ", style_for(Tab::Store)),
        Span::raw("  "),
        Span::styled("[4] Transfer ", style_for(Tab::Transfer)),
        Span::raw("  "),
        Span::styled("[5] Passthrough ", style_for(Tab::Passthrough)),
    ]);
    frame.render_widget(Paragraph::new(tabs), area);
}

// ── Build tab (existing monitor) ───────────────────────────────────────────

fn draw_build_tab(frame: &mut Frame, state: &AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(9), // Stats bar
        Constraint::Min(8),    // Live build events
        Constraint::Length(5), // Sparkline
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    draw_stats_bar(frame, state, chunks[0]);
    draw_live_build(frame, state, chunks[1]);
    draw_sparkline(frame, state, chunks[2]);
    draw_build_help(frame, state, chunks[3]);
}

fn draw_stats_bar(frame: &mut Frame, state: &AppState, area: Rect) {
    let snap = &state.stats_snapshot;
    let daemon_tag = if !state.stats_loaded {
        " (loading)"
    } else {
        match (snap.daemon_connected, state.service_installed) {
            (true, true) => "",
            (true, false) => " (no service)",
            (false, true) => " (daemon offline)",
            (false, false) => " (daemon offline, no service)",
        }
    };
    let block = Block::bordered().title(format!(" kache monitor{daemon_tag} "));

    let total = snap.event_stats.local_hits
        + snap.event_stats.prefetch_hits
        + snap.event_stats.remote_hits
        + snap.event_stats.dups
        + snap.event_stats.misses;
    let (local_pct, remote_pct, miss_pct) = if total > 0 {
        (
            ((snap.event_stats.local_hits + snap.event_stats.prefetch_hits) as f64 / total as f64)
                * 100.0,
            (snap.event_stats.remote_hits as f64 / total as f64) * 100.0,
            ((snap.event_stats.dups + snap.event_stats.misses) as f64 / total as f64) * 100.0,
        )
    } else {
        (0.0, 0.0, 0.0)
    };

    let store_pct = if snap.max_size > 0 {
        (snap.total_size as f64 / snap.max_size as f64) * 100.0
    } else {
        0.0
    };

    let remote_status = if state.config.remote.is_some() {
        "configured"
    } else {
        "not configured"
    };

    let wrapper_status = &state.wrapper_status;

    let kache_version = crate::VERSION;

    let daemon_info = if !state.stats_loaded {
        "daemon: checking".to_string()
    } else if snap.daemon_connected && !snap.daemon_version.is_empty() {
        let epoch = snap.daemon_build_epoch;
        let my_epoch = crate::daemon::build_epoch();
        if epoch == my_epoch {
            format!("daemon: v{} (epoch {epoch})", snap.daemon_version)
        } else {
            format!(
                "daemon: v{} (epoch {epoch}) \u{2190} MISMATCH, auto-restart pending",
                snap.daemon_version
            )
        }
    } else {
        "daemon: offline".to_string()
    };

    let my_epoch = crate::daemon::build_epoch();

    let dedup_line = {
        // Blob-level savings from the latest periodic stats refresh.
        let blob_savings = state.stats_snapshot.blob_stats.as_ref();

        let scan_part = if let Ok(scan_stats) = state.project_scan.lock() {
            let ls = &scan_stats.link_stats;
            let dedup_status = if !state.stats_loaded || scan_stats.scanning {
                "calculating"
            } else if scan_stats.scanned {
                "idle"
            } else {
                "not scanned"
            };
            if ls.saved_bytes > 0 {
                format!(
                    "{} via {} hardlinks    Scan: {dedup_status}",
                    ByteSize(ls.saved_bytes),
                    ls.linked_refs,
                )
            } else {
                // Zero hardlinks is the expected, healthy state on
                // copy-on-write filesystems (APFS, btrfs, XFS-with-reflink):
                // restores are reflinks with independent inodes, so store blobs
                // keep nlink == 1. The real savings are the blob-level `Dedup:`
                // figure to the left, not this Unix-only hardlink scan.
                format!("none — restores prefer reflink/CoW    Scan: {dedup_status}")
            }
        } else {
            "n/a".to_string()
        };

        if let Some(bs) = blob_savings {
            let pct = if bs.total_logical_size > 0 {
                bs.savings as f64 / bs.total_logical_size as f64 * 100.0
            } else {
                0.0
            };
            format!(
                "  Dedup: {} saved ({:.1}%)    Blobs: {} physical    Hardlinks: {scan_part}",
                ByteSize(bs.savings),
                pct,
                ByteSize(bs.total_blob_size),
            )
        } else if state.stats_loaded {
            format!("  Dedup: {scan_part}")
        } else {
            "  Dedup: calculating...".to_string()
        }
    };

    let transfer_line = if !state.stats_loaded {
        "  Transfer: calculating...".to_string()
    } else if snap.daemon_connected {
        format!(
            "  Transfer: ↑ {} uploading  ↓ {} downloading",
            snap.pending_uploads, snap.active_downloads,
        )
    } else {
        "  Transfer: n/a (daemon offline)".to_string()
    };

    let hit_line = if !state.stats_loaded {
        format!("  Hit rate: calculating...    Remote: {remote_status}")
    } else {
        let count_hit_rate = crate::cli::count_hit_rate(&snap.event_stats);
        let weighted_hit_rate = crate::cli::compile_weighted_hit_rate(&snap.event_stats);
        let miss_time_share = if snap.event_stats.total_elapsed_ms > 0 {
            Some(
                (snap.event_stats.miss_elapsed_ms as f64
                    / snap.event_stats.total_elapsed_ms as f64)
                    * 100.0,
            )
        } else {
            None
        };

        match (weighted_hit_rate, miss_time_share) {
            (Some(weighted), Some(miss_share)) => format!(
                "  Hit rate: {count_hit_rate:.0}% count | {weighted:.0}% weighted | {miss_share:.0}% miss-time    Remote: {remote_status}",
            ),
            (Some(weighted), None) => format!(
                "  Hit rate: {count_hit_rate:.0}% count | {weighted:.0}% weighted    Remote: {remote_status}",
            ),
            _ => format!(
                "  Hit rate: {local_pct:.0}% local | {remote_pct:.0}% remote | {miss_pct:.0}% miss    Remote: {remote_status}",
            ),
        }
    };

    let store_line = if state.stats_loaded {
        Line::from(format!(
            "  Store: {} / {} [{:>5.1}%]    {} entries",
            ByteSize(snap.total_size),
            ByteSize(snap.max_size),
            store_pct,
            snap.entry_count,
        ))
    } else {
        Line::from("  Store: calculating...")
    };

    let text = vec![
        store_line,
        Line::from(hit_line),
        Line::from(
            "  count = % builds served from cache · weighted = % compile-time saved · miss-time = % wall-time in misses",
        )
        .style(Style::default().fg(Color::DarkGray)),
        Line::from(dedup_line),
        Line::from(transfer_line),
        Line::from(format!("  {wrapper_status}    {}", state.rustc_version)),
        Line::from(format!(
            "  kache v{kache_version} (epoch {my_epoch})    {daemon_info}    Cache: {}",
            shorten_home(&state.config.cache_dir)
        )),
    ];

    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}

/// Per-disposition presentation: a status glyph + short label, the *action*
/// kache actually took (so a wall of "miss" reads as "built + cached" rather
/// than a failure), and a colour where a normal build is neutral and only a
/// real `Error` is red.
fn event_presentation(result: EventResult) -> (&'static str, &'static str, &'static str, Color) {
    match result {
        EventResult::LocalHit => ("✓", "hit", "restored", Color::Green),
        EventResult::PrefetchHit => ("⇣", "prefetch", "restored (prefetch)", Color::Cyan),
        EventResult::RemoteHit => ("↓", "remote", "downloaded", Color::Blue),
        EventResult::Dup => ("=", "dup", "built + deduped", Color::Cyan),
        EventResult::Miss => ("•", "miss", "built + cached", Color::White),
        EventResult::Passthrough => ("→", "pass", "built (not cached)", Color::Magenta),
        EventResult::Skipped => ("·", "skip", "skipped", Color::DarkGray),
        EventResult::Error => ("!", "error", "error", Color::Red),
    }
}

fn fmt_duration_ms(ms: u64) -> String {
    if ms == 0 {
        String::new()
    } else if ms >= 1000 {
        format!("{:.1}s", ms as f64 / 1000.0)
    } else {
        format!("{ms}ms")
    }
}

fn draw_live_build(frame: &mut Frame, state: &AppState, area: Rect) {
    let block = Block::bordered()
        .title(" Live Build ")
        .border_style(Style::default().fg(Color::Cyan));

    let filtered_events: Vec<&BuildEvent> = state
        .events
        .iter()
        .filter(|e| state.filter.is_empty() || e.crate_name.contains(&state.filter))
        .collect();

    // -2 for the borders, -1 for the header row.
    let max_visible = (area.height as usize).saturating_sub(3);
    let start = filtered_events
        .len()
        .saturating_sub(max_visible + state.scroll_offset);

    let header = Row::new(vec![
        Cell::from("Status"),
        Cell::from("Crate"),
        Cell::from("Action"),
        Cell::from(Line::from("Compile").right_aligned()),
        Cell::from(Line::from("Total").right_aligned()),
        Cell::from(Line::from("Size").right_aligned()),
    ])
    .style(Style::default().fg(Color::DarkGray));

    let rows: Vec<Row> = filtered_events
        .iter()
        .skip(start)
        .take(max_visible)
        .map(|event| {
            let (icon, status, action, color) = event_presentation(event.result);
            let cstyle = Style::default().fg(color);
            // Compile time is only meaningful where the compiler ran for this
            // invocation (a fresh build); blank it for hits/skips.
            let compile = if matches!(event.result, EventResult::Miss | EventResult::Dup) {
                fmt_duration_ms(event.compile_time_ms)
            } else {
                String::new()
            };
            let total = fmt_duration_ms(event.elapsed_ms);
            let size = if event.size > 0 {
                ByteSize(event.size).to_string()
            } else {
                String::new()
            };
            Row::new(vec![
                Cell::from(format!("{icon} {status}")).style(cstyle),
                Cell::from(event.crate_name.clone()),
                Cell::from(action).style(cstyle),
                Cell::from(Line::from(compile).right_aligned())
                    .style(Style::default().fg(Color::DarkGray)),
                Cell::from(Line::from(total).right_aligned()),
                Cell::from(Line::from(size).right_aligned()),
            ])
        })
        .collect();

    // ratatui clips each cell to its column width, so a long crate name
    // truncates inside the Crate column instead of shoving every later column
    // out of its grid (the old hand-padded `format!` rows misaligned on long
    // names and ambiguous-width glyphs).
    let widths = [
        Constraint::Length(11), // Status (icon + word)
        Constraint::Min(14),    // Crate
        Constraint::Length(19), // Action
        Constraint::Length(9),  // Compile
        Constraint::Length(8),  // Total
        Constraint::Length(11), // Size
    ];

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

fn draw_sparkline(frame: &mut Frame, state: &AppState, area: Rect) {
    let block = Block::bordered().title(" Hit Rate (recent) ");

    let width = area.width as usize;
    let bucket_count = width.saturating_sub(4);

    if state.events.is_empty() || bucket_count == 0 {
        frame.render_widget(Paragraph::new("  No data yet").block(block), area);
        return;
    }

    let events_per_bucket = (state.events.len() / bucket_count).max(1);
    let mut data: Vec<u64> = Vec::new();

    for chunk in state.events.chunks(events_per_bucket) {
        let hits = chunk
            .iter()
            .filter(|e| {
                matches!(
                    e.result,
                    EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit
                )
            })
            .count();
        let total = chunk.len();
        let rate = if total > 0 {
            (hits as f64 / total as f64 * 8.0) as u64
        } else {
            0
        };
        data.push(rate);
    }

    while data.len() < bucket_count {
        data.push(0);
    }

    let sparkline = Sparkline::default()
        .block(block)
        .data(&data[..bucket_count.min(data.len())])
        .max(8)
        .style(Style::default().fg(Color::Green));

    frame.render_widget(sparkline, area);
}

fn draw_build_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let help = if state.filter_active {
        format!("  filter: {}_ (Esc to close)", state.filter)
    } else {
        "  q: quit  f: filter  ↑↓: scroll  Tab: next  c: clear  1-5: tabs".to_string()
    };

    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Store tab ─────────────────────────────────────────────────────────────

fn draw_store_tab(frame: &mut Frame, state: &AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Min(5),    // Crates table (full height)
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    draw_store_table(frame, state, chunks[0]);
    draw_store_help(frame, state, chunks[1]);
}

fn draw_store_table(frame: &mut Frame, state: &AppState, area: Rect) {
    let dedup_info = if let Some(bs) = state.stats_snapshot.blob_stats.as_ref() {
        if bs.total_blobs > 0 {
            let pct = if bs.total_logical_size > 0 {
                bs.savings as f64 / bs.total_logical_size as f64 * 100.0
            } else {
                0.0
            };
            format!(
                " | dedup: {} physical, {:.1}% saved",
                ByteSize(bs.total_blob_size),
                pct,
            )
        } else {
            String::new()
        }
    } else {
        String::new()
    };
    let title = format!(
        " Cached Crates — {} entries, {} (sort: {}){dedup_info} ",
        state.stats_snapshot.entry_count,
        ByteSize(state.stats_snapshot.total_size),
        state.sort_mode.label()
    );
    let block = Block::bordered()
        .title(title)
        .border_style(Style::default().fg(Color::Cyan));

    let entries = &state.stats_snapshot.entries;

    let mut content_hash_counts: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for entry in entries {
        if let Some(ch) = &entry.content_hash {
            *content_hash_counts.entry(ch.as_str()).or_insert(0) += 1;
        }
    }

    let header = Row::new(vec![
        "Key", "Crate", "Type", "Profile", "Size", "Hits", "Dup", "Created", "Accessed",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD))
    .bottom_margin(0);

    let filtered: Vec<&daemon::StatsEntry> = entries
        .iter()
        .filter(|e| {
            state.filter.is_empty()
                || e.crate_name.contains(&state.filter)
                || e.cache_key.contains(&state.filter)
        })
        .collect();

    let visible_rows = (area.height as usize).saturating_sub(3); // borders + header
    let skip = state
        .store_scroll
        .min(filtered.len().saturating_sub(visible_rows));

    let rows: Vec<Row> = filtered
        .iter()
        .skip(skip)
        .take(visible_rows)
        .map(|entry| {
            let key_short = if entry.cache_key.len() > 12 {
                &entry.cache_key[..12]
            } else {
                &entry.cache_key
            };
            let crate_type = if entry.crate_type.is_empty() {
                "-"
            } else {
                &entry.crate_type
            };
            let profile = if entry.profile.is_empty() {
                "-"
            } else {
                &entry.profile
            };
            let dup = if let Some(ch) = &entry.content_hash {
                let count = content_hash_counts.get(ch.as_str()).copied().unwrap_or(1);
                if count > 1 {
                    format!("{count}x")
                } else {
                    String::new()
                }
            } else {
                String::new()
            };
            Row::new(vec![
                Cell::from(key_short.to_string()),
                Cell::from(entry.crate_name.clone()),
                Cell::from(crate_type.to_string()),
                Cell::from(profile.to_string()),
                Cell::from(ByteSize(entry.size).to_string()),
                Cell::from(entry.hit_count.to_string()),
                Cell::from(dup).style(Style::default().fg(Color::Yellow)),
                Cell::from(
                    entry
                        .created_at
                        .get(..10)
                        .unwrap_or(&entry.created_at)
                        .to_string(),
                ),
                Cell::from(
                    entry
                        .last_accessed
                        .get(..10)
                        .unwrap_or(&entry.last_accessed)
                        .to_string(),
                ),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(13), // Key
        Constraint::Min(18),    // Crate
        Constraint::Length(10), // Type
        Constraint::Length(10), // Profile
        Constraint::Length(10), // Size
        Constraint::Length(6),  // Hits
        Constraint::Length(5),  // Dup
        Constraint::Length(12), // Created
        Constraint::Length(12), // Accessed
    ];

    let table = Table::new(rows, widths).header(header).block(block);

    frame.render_widget(table, area);
}

fn draw_store_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let help = if state.filter_active {
        format!("  filter: {}_ (Esc to close)", state.filter)
    } else {
        "  q: quit  s: sort  f: filter  ↑↓: scroll  Tab: next  1-5: tabs".to_string()
    };

    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Projects tab ───────────────────────────────────────────────────────────

fn draw_projects_tab(frame: &mut Frame, state: &AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(9), // Overview panel
        Constraint::Min(5),    // Projects table
        Constraint::Length(3), // Totals bar
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    draw_projects_overview(frame, state, chunks[0]);
    draw_projects_table(frame, state, chunks[1]);
    draw_projects_totals(frame, state, chunks[2]);
    draw_projects_help(frame, chunks[3]);
}

fn draw_projects_overview(frame: &mut Frame, state: &AppState, area: Rect) {
    let scan_stats = state.project_scan.lock().unwrap();
    let scanning = scan_stats.scanning;
    let snap = &state.stats_snapshot;

    let daemon_tag = match (snap.daemon_connected, state.service_installed) {
        (true, true) => "",
        (true, false) => " (no service)",
        (false, true) => " (daemon offline)",
        (false, false) => " (daemon offline, no service)",
    };
    let scan_tag = if scanning { " (scanning...)" } else { "" };
    let title = format!(" kache projects{daemon_tag}{scan_tag}");
    let block = Block::bordered().title(title);

    let store_pct = if snap.max_size > 0 {
        (snap.total_size as f64 / snap.max_size as f64) * 100.0
    } else {
        0.0
    };

    let es = &snap.event_stats;
    let hit_rate = crate::cli::count_hit_rate(es);
    let weighted_hit_rate = crate::cli::compile_weighted_hit_rate(es);
    let time_saved = if es.hit_compile_time_ms > 0 {
        crate::cli::format_duration_ms(es.hit_compile_time_ms)
    } else {
        "n/a".to_string()
    };

    let ls = &scan_stats.link_stats;
    // Dedup summary: lead with the cross-platform blob-level savings (storing
    // each unique artifact once). The hardlink sub-figure is a Unix-only
    // restore detail that reads zero on copy-on-write filesystems, where
    // restores are reflinks (independent inodes) rather than hardlinks — so
    // never present it as the headline dedup number.
    let hardlink_part = if ls.saved_bytes > 0 {
        format!(
            "Hardlinks: {} via {} hardlinks",
            ByteSize(ls.saved_bytes),
            ls.linked_refs,
        )
    } else {
        "Hardlinks: none — restores prefer reflink/CoW".to_string()
    };
    let dedup_summary = if let Some(bs) = state.stats_snapshot.blob_stats.as_ref() {
        let pct = if bs.total_logical_size > 0 {
            bs.savings as f64 / bs.total_logical_size as f64 * 100.0
        } else {
            0.0
        };
        format!(
            "{} saved ({:.1}%)    {hardlink_part}",
            ByteSize(bs.savings),
            pct,
        )
    } else {
        format!("calculating...    {hardlink_part}")
    };

    let wrapper_status = crate::wrapper_config::wrapper_status_line();

    let remote_status = if let Some(remote) = &state.config.remote {
        format!("S3: {}", remote.bucket)
    } else {
        "not configured".to_string()
    };

    let kache_version = crate::VERSION;
    let my_epoch = crate::daemon::build_epoch();

    let daemon_info = if snap.daemon_connected && !snap.daemon_version.is_empty() {
        let epoch = snap.daemon_build_epoch;
        if epoch == my_epoch {
            format!("daemon: v{} (epoch {epoch})", snap.daemon_version)
        } else {
            format!(
                "daemon: v{} (epoch {epoch}) \u{2190} MISMATCH, auto-restart pending",
                snap.daemon_version
            )
        }
    } else {
        "daemon: offline".to_string()
    };

    let transfer_spans = if snap.daemon_connected {
        vec![
            Span::styled("  Transfer: ", Style::default().fg(Color::Cyan)),
            Span::styled(
                format!("↑ {}", snap.pending_uploads),
                if snap.pending_uploads > 0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                },
            ),
            Span::raw(" uploading  "),
            Span::styled(
                format!("↓ {}", snap.active_downloads),
                if snap.active_downloads > 0 {
                    Style::default().fg(Color::Blue)
                } else {
                    Style::default()
                },
            ),
            Span::raw(" downloading"),
        ]
    } else {
        vec![
            Span::styled("  Transfer: ", Style::default().fg(Color::Cyan)),
            Span::styled("n/a", Style::default().fg(Color::DarkGray)),
        ]
    };

    let text = vec![
        Line::from(vec![
            Span::styled("  Store: ", Style::default().fg(Color::Cyan)),
            Span::raw(format!(
                "{} / {} [{:.1}%]",
                ByteSize(snap.total_size),
                ByteSize(snap.max_size),
                store_pct
            )),
            Span::raw(format!("    {} entries", snap.entry_count)),
        ]),
        Line::from(vec![
            Span::styled("  Hit rate: ", Style::default().fg(Color::Cyan)),
            Span::raw(format!(
                "{hit_rate:.0}% count{} (24h: {} hits, {} dups, {} misses)",
                weighted_hit_rate
                    .map(|v| format!(" | {v:.0}% weighted"))
                    .unwrap_or_default(),
                es.local_hits + es.prefetch_hits + es.remote_hits,
                es.dups,
                es.misses
            )),
            Span::raw(format!("    Time saved: {time_saved}")),
        ]),
        Line::from(vec![
            Span::styled("  Dedup: ", Style::default().fg(Color::Cyan)),
            Span::raw(dedup_summary),
        ]),
        Line::from(transfer_spans),
        Line::from(vec![
            Span::styled("  Remote: ", Style::default().fg(Color::Cyan)),
            Span::raw(format!("{remote_status}    {wrapper_status}")),
        ]),
        Line::from(format!(
            "  kache v{kache_version} (epoch {my_epoch})    {daemon_info}    {}",
            state.rustc_version
        )),
    ];

    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}

fn draw_projects_table(frame: &mut Frame, state: &AppState, area: Rect) {
    let stats = state.project_scan.lock().unwrap();

    let block = Block::bordered()
        .title(" Projects ")
        .border_style(Style::default().fg(Color::Cyan));

    if stats.project_targets.is_empty() {
        let msg = if stats.scanning {
            "  Scanning..."
        } else {
            "  No target/ directories found."
        };
        frame.render_widget(Paragraph::new(msg).block(block), area);
        return;
    }

    let root = std::env::current_dir().unwrap_or_default();

    let header = Row::new(vec![
        "Path", "Size", "Cached", "Incr", "Build", "Deps", "Bin", "Fprint", "Profile",
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    let fmt = |v: u64| -> String {
        if v > 0 {
            format!("{:>8}", ByteSize(v))
        } else {
            String::new()
        }
    };

    let rows: Vec<Row> = stats
        .project_targets
        .iter()
        .map(|t| {
            let rel = t.path.strip_prefix(&root).unwrap_or(&t.path);
            let path_label = if t.stale {
                format!("~ {}", rel.display())
            } else {
                format!("{}", rel.display())
            };

            let profile_str = if t.profiles.is_empty() {
                String::new()
            } else {
                format!("[{}]", t.profiles.join(", "))
            };

            let b = &t.breakdown;

            Row::new(vec![
                Cell::from(path_label),
                Cell::from(format!("{:>8}", ByteSize(t.size))),
                Cell::from(format!("{:>8}", ByteSize(t.cached_bytes))),
                Cell::from(fmt(b.incremental)),
                Cell::from(fmt(b.build_scripts)),
                Cell::from(fmt(b.deps_local)),
                Cell::from(fmt(b.binaries)),
                Cell::from(fmt(b.fingerprints)),
                Cell::from(profile_str),
            ])
        })
        .collect();

    let widths = [
        Constraint::Min(20),    // Path
        Constraint::Length(9),  // Size
        Constraint::Length(9),  // Cached
        Constraint::Length(9),  // Incr
        Constraint::Length(9),  // Build
        Constraint::Length(9),  // Deps
        Constraint::Length(9),  // Bin
        Constraint::Length(9),  // Fprint
        Constraint::Length(14), // Profile
    ];

    let visible_rows = (area.height as usize).saturating_sub(3); // borders + header
    let skip = state
        .project_scroll
        .min(rows.len().saturating_sub(visible_rows));

    let table = Table::new(rows.into_iter().skip(skip).collect::<Vec<_>>(), widths)
        .header(header)
        .block(block);

    frame.render_widget(table, area);
}

fn draw_projects_totals(frame: &mut Frame, state: &AppState, area: Rect) {
    let stats = state.project_scan.lock().unwrap();

    if stats.project_targets.is_empty() {
        frame.render_widget(Block::bordered().title(" Total "), area);
        return;
    }

    let mut total_size = 0u64;
    let mut total_cached = 0u64;
    let mut total_incr = 0u64;
    let mut total_build = 0u64;
    let mut total_deps = 0u64;
    let mut total_bin = 0u64;
    let mut total_fprint = 0u64;

    for t in &stats.project_targets {
        total_size += t.size;
        total_cached += t.cached_bytes;
        total_incr += t.breakdown.incremental;
        total_build += t.breakdown.build_scripts;
        total_deps += t.breakdown.deps_local;
        total_bin += t.breakdown.binaries;
        total_fprint += t.breakdown.fingerprints;
    }

    let n = stats.project_targets.len();
    let title = format!(" Total ({n} project{}) ", if n == 1 { "" } else { "s" });

    let fmt = |v: u64| -> Span {
        if v > 0 {
            Span::raw(format!("{} ", ByteSize(v)))
        } else {
            Span::styled("- ", Style::default().fg(Color::DarkGray))
        }
    };

    let line = Line::from(vec![
        Span::styled("  Size: ", Style::default().fg(Color::Cyan)),
        Span::styled(
            format!("{}", ByteSize(total_size)),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("Cached: ", Style::default().fg(Color::Cyan)),
        Span::styled(
            format!("{}", ByteSize(total_cached)),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("Incr: ", Style::default().fg(Color::DarkGray)),
        fmt(total_incr),
        Span::styled("Build: ", Style::default().fg(Color::DarkGray)),
        fmt(total_build),
        Span::styled("Deps: ", Style::default().fg(Color::DarkGray)),
        fmt(total_deps),
        Span::styled("Bin: ", Style::default().fg(Color::DarkGray)),
        fmt(total_bin),
        Span::styled("Fprint: ", Style::default().fg(Color::DarkGray)),
        fmt(total_fprint),
    ]);

    let block = Block::bordered().title(title);
    let paragraph = Paragraph::new(line).block(block);
    frame.render_widget(paragraph, area);
}

fn draw_projects_help(frame: &mut Frame, area: Rect) {
    let help = "  q: quit  r: refresh  ↑↓: scroll  Tab: next  1-5: tabs";

    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Transfer tab ──────────────────────────────────────────────────────────

fn draw_transfer_tab(frame: &mut Frame, state: &AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(3), // Upload queue gauge
        Constraint::Length(9), // Transfer activity summary
        Constraint::Min(5),    // Recent transfers table
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    draw_transfer_pending(frame, state, chunks[0]);
    draw_transfer_activity(frame, state, chunks[1]);
    draw_recent_transfers(frame, state, chunks[2]);
    draw_transfer_help(frame, chunks[3]);
}

fn draw_transfer_pending(frame: &mut Frame, state: &AppState, area: Rect) {
    let snap = &state.stats_snapshot;
    let pending = snap.pending_uploads;

    let color = if pending == 0 {
        Color::Green
    } else if pending < 100 {
        Color::Yellow
    } else {
        Color::Red
    };

    let label = format!("  {} pending uploads", pending);
    let paragraph = Paragraph::new(Span::styled(label, Style::default().fg(color)))
        .block(Block::bordered().title(" Upload Queue "));

    frame.render_widget(paragraph, area);
}

fn format_speed(bps: f64) -> String {
    if bps >= 1_000_000.0 {
        format!("{:.1} MB/s", bps / 1_000_000.0)
    } else if bps >= 1_000.0 {
        format!("{:.0} KB/s", bps / 1_000.0)
    } else if bps > 0.0 {
        format!("{:.0} B/s", bps)
    } else {
        "0 B/s".to_string()
    }
}

fn draw_transfer_activity(frame: &mut Frame, state: &AppState, area: Rect) {
    let snap = &state.stats_snapshot;
    let block = Block::bordered()
        .title(" Transfer Activity ")
        .border_style(Style::default().fg(Color::Cyan));

    let s3_slots = format!(
        "{} / {}",
        snap.s3_concurrency_used, snap.s3_concurrency_total
    );
    let up_speed = format_speed(state.upload_speed_bps);
    let down_speed = format_speed(state.download_speed_bps);

    let text = vec![
        Line::from(vec![
            Span::styled("  Active: ", Style::default().fg(Color::Cyan)),
            Span::styled(
                format!("↑ {} uploading", snap.pending_uploads),
                if snap.pending_uploads > 0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                },
            ),
            Span::raw("    "),
            Span::styled(
                format!("↓ {} downloading", snap.active_downloads),
                if snap.active_downloads > 0 {
                    Style::default().fg(Color::Blue)
                } else {
                    Style::default()
                },
            ),
            Span::raw(format!("    S3 slots: {s3_slots}")),
        ]),
        Line::from(vec![
            Span::styled("  Speed:  ", Style::default().fg(Color::Cyan)),
            Span::styled(format!("↑ {up_speed}"), Style::default().fg(Color::Yellow)),
            Span::raw("    "),
            Span::styled(format!("↓ {down_speed}"), Style::default().fg(Color::Blue)),
        ]),
        Line::from(vec![
            Span::styled("  Uploads: ", Style::default().fg(Color::Cyan)),
            Span::styled(
                format!("{} ok", snap.uploads_completed),
                Style::default().fg(Color::Green),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} failed", snap.uploads_failed),
                if snap.uploads_failed > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} skipped", snap.uploads_skipped),
                Style::default().fg(Color::DarkGray),
            ),
            Span::raw(format!("    total: {}", ByteSize(snap.bytes_uploaded))),
        ]),
        Line::from(vec![
            Span::styled("  Downloads: ", Style::default().fg(Color::Cyan)),
            Span::styled(
                format!("{} ok", snap.downloads_completed),
                Style::default().fg(Color::Green),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} failed", snap.downloads_failed),
                if snap.downloads_failed > 0 {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default().fg(Color::DarkGray)
                },
            ),
            Span::raw(format!("    total: {}", ByteSize(snap.bytes_downloaded))),
        ]),
        Line::from(""),
        Line::from(vec![
            Span::styled("  Daemon: ", Style::default().fg(Color::Cyan)),
            if snap.daemon_connected {
                Span::styled("connected", Style::default().fg(Color::Green))
            } else {
                Span::styled("offline", Style::default().fg(Color::Red))
            },
        ]),
    ];

    let paragraph = Paragraph::new(text).block(block);
    frame.render_widget(paragraph, area);
}

fn draw_recent_transfers(frame: &mut Frame, state: &AppState, area: Rect) {
    let transfers = &state.stats_snapshot.recent_transfers;

    let block = Block::bordered()
        .title(format!(" Recent Transfers ({}) ", transfers.len()))
        .border_style(Style::default().fg(Color::Cyan));

    if transfers.is_empty() {
        let msg = if !state.stats_snapshot.daemon_connected {
            "  Daemon offline — no transfer data available"
        } else {
            "  No transfers yet"
        };
        frame.render_widget(Paragraph::new(msg).block(block), area);
        return;
    }

    let header = Row::new(vec!["Dir", "Crate", "Size", "Time", "Status"])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let visible_rows = (area.height as usize).saturating_sub(3);
    let skip = state
        .transfer_scroll
        .min(transfers.len().saturating_sub(visible_rows));

    // Show in reverse chronological order
    let rows: Vec<Row> = transfers
        .iter()
        .rev()
        .skip(skip)
        .take(visible_rows)
        .map(|evt| {
            let (arrow, dir_style) = match evt.direction {
                daemon::TransferDirection::Upload => ("↑", Style::default().fg(Color::Yellow)),
                daemon::TransferDirection::Download => ("↓", Style::default().fg(Color::Blue)),
            };

            let elapsed = if evt.elapsed_ms > 1000 {
                format!("{:.1}s", evt.elapsed_ms as f64 / 1000.0)
            } else {
                format!("{}ms", evt.elapsed_ms)
            };

            let (status, status_style) = if evt.ok {
                ("ok", Style::default().fg(Color::Green))
            } else {
                ("FAIL", Style::default().fg(Color::Red))
            };

            Row::new(vec![
                Cell::from(arrow).style(dir_style),
                Cell::from(evt.crate_name.clone()),
                Cell::from(ByteSize(evt.compressed_bytes).to_string()),
                Cell::from(elapsed),
                Cell::from(status).style(status_style),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(3),  // Dir arrow
        Constraint::Min(20),    // Crate name
        Constraint::Length(10), // Size
        Constraint::Length(8),  // Time
        Constraint::Length(6),  // Status
    ];

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

fn draw_transfer_help(frame: &mut Frame, area: Rect) {
    let help = "  q: quit  ↑↓: scroll  Tab: next  1-5: tabs";
    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Passthrough tab ───────────────────────────────────────────────────────

fn draw_passthrough_tab(frame: &mut Frame, state: &AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Min(5),    // Passthrough table
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    draw_passthrough_table(frame, state, chunks[0]);
    draw_passthrough_help(frame, state, chunks[1]);
}

fn draw_passthrough_table(frame: &mut Frame, state: &AppState, area: Rect) {
    let events: Vec<&BuildEvent> = state
        .events
        .iter()
        .filter(|event| matches!(event.result, EventResult::Passthrough))
        .filter(|event| {
            state.filter.is_empty()
                || event.crate_name.contains(&state.filter)
                || event.passthrough_reason.contains(&state.filter)
        })
        .collect();

    let block = Block::bordered()
        .title(format!(" Passthroughs ({}) ", events.len()))
        .border_style(Style::default().fg(Color::Cyan));

    if events.is_empty() {
        let msg = if state.filter.is_empty() {
            "  No passthroughs yet"
        } else {
            "  No passthroughs match the filter"
        };
        frame.render_widget(Paragraph::new(msg).block(block), area);
        return;
    }

    let header = Row::new(vec!["Time", "Crate", "Route", "Exit", "Kind", "Reason"])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let visible_rows = (area.height as usize).saturating_sub(3);
    let skip = state
        .passthrough_scroll
        .min(events.len().saturating_sub(visible_rows));

    let rows: Vec<Row> = events
        .iter()
        .rev()
        .skip(skip)
        .take(visible_rows)
        .map(|event| {
            let exit = event
                .exit_code
                .map(|code| code.to_string())
                .unwrap_or_default();
            let route = if event.fallback { "fallback" } else { "direct" };
            let (kind, reason) = passthrough_reason_parts(&event.passthrough_reason);

            let exit_style = match event.exit_code {
                Some(0) => Style::default().fg(Color::Green),
                Some(_) => Style::default().fg(Color::Red),
                None => Style::default().fg(Color::DarkGray),
            };

            Row::new(vec![
                Cell::from(event.ts.format("%H:%M:%S").to_string()),
                Cell::from(event.crate_name.clone()),
                Cell::from(route).style(Style::default().fg(Color::Magenta)),
                Cell::from(exit).style(exit_style),
                Cell::from(kind.to_string()).style(Style::default().fg(Color::Cyan)),
                Cell::from(reason.to_string()),
            ])
        })
        .collect();

    let widths = [
        Constraint::Length(9),
        Constraint::Min(18),
        Constraint::Length(10),
        Constraint::Length(6),
        Constraint::Length(14),
        Constraint::Percentage(45),
    ];

    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

fn passthrough_reason_parts(reason: &str) -> (&str, &str) {
    let reason = reason.trim();
    if reason.is_empty() {
        return ("unknown", "unknown");
    }

    if let Some((kind, detail)) = reason.split_once('|') {
        let kind = kind.trim();
        let detail = detail.trim();
        return (
            if kind.is_empty() { "unknown" } else { kind },
            if detail.is_empty() { "unknown" } else { detail },
        );
    }

    ("N/A", reason.strip_prefix("refused: ").unwrap_or(reason))
}

fn draw_passthrough_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let help = if state.filter_active {
        format!("  filter: {}_ (Esc to close)", state.filter)
    } else {
        "  q: quit  f: filter  ↑↓: scroll  Tab: next  1-5: tabs".to_string()
    };
    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tab_needs_entries_only_for_store() {
        assert!(!tab_needs_entries(Tab::Build));
        assert!(!tab_needs_entries(Tab::Projects));
        assert!(tab_needs_entries(Tab::Store));
        assert!(!tab_needs_entries(Tab::Transfer));
        assert!(!tab_needs_entries(Tab::Passthrough));
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
    fn passthrough_reason_parts_split_structured_reasons() {
        assert_eq!(
            passthrough_reason_parts("unsupported|cc unsupported flag(s): -foo — not yet"),
            ("unsupported", "cc unsupported flag(s): -foo — not yet")
        );
        assert_eq!(
            passthrough_reason_parts("refused: cc: unsupported flag(s): -m64"),
            ("N/A", "cc: unsupported flag(s): -m64")
        );
        assert_eq!(passthrough_reason_parts(""), ("unknown", "unknown"));
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
        use crate::config::{DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS};
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: std::env::temp_dir().join("kache-tui-test"),
            max_size: 1024 * 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        }
    }

    fn test_state() -> AppState {
        let config = test_config();
        AppState {
            tailer: EventTailer::new(config.event_log_path()),
            config,
            active_tab: Tab::Build,
            events: Vec::new(),
            scroll_offset: 0,
            filter: String::new(),
            filter_active: false,
            sort_mode: SortMode::Size,
            store_scroll: 0,
            stats_snapshot: StatsSnapshot::default(),
            stats_loaded: false,
            last_stats_fetch: Instant::now(),
            project_scan: Arc::new(Mutex::new(ProjectScanData::default())),
            last_project_refresh: Instant::now(),
            project_scroll: 0,
            transfer_scroll: 0,
            passthrough_scroll: 0,
            prev_bytes_uploaded: 0,
            prev_bytes_downloaded: 0,
            upload_speed_bps: 0.0,
            download_speed_bps: 0.0,
            rustc_version_slot: Arc::new(Mutex::new(None)),
            stats_result_slot: Arc::new(Mutex::new(None)),
            stats_fetch_in_flight: false,
            stats_fetch_requested_entries: false,
            should_quit: false,
            rustc_version: "test".to_string(),
            wrapper_status: "test".to_string(),
            service_installed: false,
        }
    }

    #[test]
    fn handle_key_number_keys_switch_tabs() {
        let mut s = test_state();
        handle_key(&mut s, KeyCode::Char('2'));
        assert_eq!(s.active_tab, Tab::Projects);
        handle_key(&mut s, KeyCode::Char('3'));
        assert_eq!(s.active_tab, Tab::Store);
        handle_key(&mut s, KeyCode::Char('5'));
        assert_eq!(s.active_tab, Tab::Passthrough);
        handle_key(&mut s, KeyCode::Char('1'));
        assert_eq!(s.active_tab, Tab::Build);
    }

    #[test]
    fn handle_key_tab_cycles_forward_and_wraps() {
        let mut s = test_state();
        let order = [
            Tab::Projects,
            Tab::Store,
            Tab::Transfer,
            Tab::Passthrough,
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

    #[test]
    fn handle_key_filter_mode_captures_text() {
        let mut s = test_state();
        s.active_tab = Tab::Build;
        handle_key(&mut s, KeyCode::Char('f'));
        assert!(s.filter_active);
        for c in "abc".chars() {
            handle_key(&mut s, KeyCode::Char(c));
        }
        assert_eq!(s.filter, "abc");
        handle_key(&mut s, KeyCode::Backspace);
        assert_eq!(s.filter, "ab");
        handle_key(&mut s, KeyCode::Esc);
        assert!(!s.filter_active);
        // 'q' no longer types into the filter; it quits.
        handle_key(&mut s, KeyCode::Char('q'));
        assert!(s.should_quit);
    }

    #[test]
    fn handle_key_scroll_is_per_tab() {
        let mut s = test_state();
        s.active_tab = Tab::Store;
        handle_key(&mut s, KeyCode::Down);
        handle_key(&mut s, KeyCode::Down);
        assert_eq!(s.store_scroll, 2);
        handle_key(&mut s, KeyCode::Up);
        assert_eq!(s.store_scroll, 1);
        // A different tab tracks its own offset.
        s.active_tab = Tab::Passthrough;
        handle_key(&mut s, KeyCode::Down);
        assert_eq!(s.passthrough_scroll, 1);
        assert_eq!(s.store_scroll, 1, "store offset is untouched");
    }

    #[test]
    fn handle_key_store_s_cycles_sort_mode() {
        let mut s = test_state();
        s.active_tab = Tab::Store;
        assert_eq!(s.sort_mode.label(), "size");
        handle_key(&mut s, KeyCode::Char('s'));
        assert_eq!(s.sort_mode.label(), "hits");
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
            Tab::Passthrough,
        ] {
            let mut state = test_state();
            state.active_tab = tab;
            let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
            terminal
                .draw(|frame| draw_ui(frame, &state))
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

    fn sample_build_event(
        crate_name: &str,
        result: events::EventResult,
        elapsed_ms: u64,
        size: u64,
    ) -> events::BuildEvent {
        events::BuildEvent {
            ts: chrono::Utc::now(),
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
            store_copied_bytes: 0,
            root: String::new(),
            passthrough_reason: "linker invocation".to_string(),
            fallback: false,
            exit_code: Some(0),
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
        state.config.remote = Some(crate::config::RemoteConfig {
            bucket: "b".into(),
            endpoint: None,
            region: "us-east-1".into(),
            prefix: "p".into(),
            profile: None,
        });
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
            .draw(|frame| draw_ui(frame, &state))
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

        for tab in [Tab::Build, Tab::Store, Tab::Passthrough, Tab::Transfer] {
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
            // Transfer-tab counters/speeds.
            state.stats_snapshot.uploads_completed = 3;
            state.upload_speed_bps = 2_500_000.0;
            state.download_speed_bps = 800_000.0;

            let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
            terminal
                .draw(|frame| draw_ui(frame, &state))
                .expect("populated draw should succeed");
            let buffer = terminal.backend().buffer().clone();
            let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
            // Populated tabs surface a crate name we seeded.
            if matches!(tab, Tab::Build | Tab::Store | Tab::Passthrough) {
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
                profiles: vec!["debug".to_string(), "release".to_string()],
                breakdown: cli::CategoryBreakdown::default(),
                stale: false,
            }];
            scan.link_stats = cli::LinkStats {
                store_bytes: 10_000_000,
                linked_refs: 42,
                saved_bytes: 7_000_000,
            };
            scan.scanning = false;
            scan.scanned = true;
        }

        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal
            .draw(|frame| draw_ui(frame, &state))
            .expect("projects draw should succeed");
        let buffer = terminal.backend().buffer().clone();
        let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(
            rendered.contains("myproj") || rendered.contains("target"),
            "projects tab should render the scanned target path"
        );
    }
}
