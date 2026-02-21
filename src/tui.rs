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
    Stats,
    Store,
    Transfer,
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

/// Stats data computed in a background thread.
struct StatsData {
    project_targets: Vec<cli::TargetEntry>,
    link_stats: cli::LinkStats,
    scanning: bool,
}

impl Default for StatsData {
    fn default() -> Self {
        Self {
            project_targets: Vec::new(),
            link_stats: cli::LinkStats {
                store_bytes: 0,
                linked_refs: 0,
                saved_bytes: 0,
            },
            scanning: false,
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
    last_stats_fetch: Instant,

    // Stats tab (shared with background scanner thread for target/ scanning)
    stats: Arc<Mutex<StatsData>>,
    last_stats_refresh: Instant,
    stats_scroll: usize,

    // Transfer tab
    transfer_scroll: usize,
    prev_bytes_uploaded: u64,
    prev_bytes_downloaded: u64,
    upload_speed_bps: f64,
    download_speed_bps: f64,

    should_quit: bool,
    rustc_version: String,
    service_installed: bool,
}

const STATS_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
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

    let rustc_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let stats = Arc::new(Mutex::new(StatsData::default()));

    // Kick off initial background scan
    spawn_stats_scan(Arc::clone(&stats), config.store_dir());

    // Initial stats snapshot (daemon-first)
    let stats_snapshot = fetch_stats(config, true, "size");
    let initial_bytes_uploaded = stats_snapshot.bytes_uploaded;
    let initial_bytes_downloaded = stats_snapshot.bytes_downloaded;

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
        last_stats_fetch: Instant::now(),
        stats,
        last_stats_refresh: Instant::now(),
        stats_scroll: 0,
        transfer_scroll: 0,
        prev_bytes_uploaded: initial_bytes_uploaded,
        prev_bytes_downloaded: initial_bytes_downloaded,
        upload_speed_bps: 0.0,
        download_speed_bps: 0.0,
        should_quit: false,
        rustc_version,
        service_installed,
    };

    loop {
        // Poll for new build events
        if let Ok(new_events) = state.tailer.poll() {
            state.events.extend(new_events);
        }

        // Refresh store/event snapshot periodically
        if state.last_stats_fetch.elapsed() >= SNAPSHOT_REFRESH_INTERVAL {
            let old_up = state.stats_snapshot.bytes_uploaded;
            let old_down = state.stats_snapshot.bytes_downloaded;
            state.stats_snapshot = fetch_stats(&state.config, true, state.sort_mode.label());
            let interval = SNAPSHOT_REFRESH_INTERVAL.as_secs_f64();
            state.upload_speed_bps =
                (state.stats_snapshot.bytes_uploaded.saturating_sub(old_up)) as f64 / interval;
            state.download_speed_bps =
                (state.stats_snapshot.bytes_downloaded.saturating_sub(old_down)) as f64 / interval;
            state.prev_bytes_uploaded = state.stats_snapshot.bytes_uploaded;
            state.prev_bytes_downloaded = state.stats_snapshot.bytes_downloaded;
            state.last_stats_fetch = Instant::now();
        }

        // Refresh target/ scan periodically when on stats tab
        if state.active_tab == Tab::Stats
            && state.last_stats_refresh.elapsed() >= STATS_REFRESH_INTERVAL
        {
            let is_scanning = state.stats.lock().map(|s| s.scanning).unwrap_or(false);
            if !is_scanning {
                spawn_stats_scan(Arc::clone(&state.stats), state.config.store_dir());
                state.last_stats_refresh = Instant::now();
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
fn spawn_stats_scan(stats: Arc<Mutex<StatsData>>, store_dir: std::path::PathBuf) {
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
                s.project_targets.sort_by(|a, b| b.size.cmp(&a.size));
            }
        }

        if let Ok(mut s) = stats.lock() {
            // Remove entries that are still stale (no longer exist on disk)
            s.project_targets.retain(|t| !t.stale);
            s.scanning = false;
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
            state.active_tab = Tab::Stats;
            state.last_stats_refresh = Instant::now() - STATS_REFRESH_INTERVAL;
        }
        KeyCode::Char('3') => state.active_tab = Tab::Store,
        KeyCode::Char('4') => state.active_tab = Tab::Transfer,
        KeyCode::BackTab | KeyCode::Tab => match state.active_tab {
            Tab::Build => {
                state.active_tab = Tab::Stats;
                state.last_stats_refresh = Instant::now() - STATS_REFRESH_INTERVAL;
            }
            Tab::Stats => state.active_tab = Tab::Store,
            Tab::Store => state.active_tab = Tab::Transfer,
            Tab::Transfer => state.active_tab = Tab::Build,
        },
        // Scrolling
        KeyCode::Up => match state.active_tab {
            Tab::Build => state.scroll_offset = state.scroll_offset.saturating_sub(1),
            Tab::Stats => state.stats_scroll = state.stats_scroll.saturating_sub(1),
            Tab::Store => state.store_scroll = state.store_scroll.saturating_sub(1),
            Tab::Transfer => state.transfer_scroll = state.transfer_scroll.saturating_sub(1),
        },
        KeyCode::Down => match state.active_tab {
            Tab::Build => state.scroll_offset += 1,
            Tab::Stats => state.stats_scroll += 1,
            Tab::Store => state.store_scroll += 1,
            Tab::Transfer => state.transfer_scroll += 1,
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
        }
        KeyCode::Char('f') if state.active_tab == Tab::Store => {
            state.filter_active = true;
        }
        // Stats tab: force refresh
        KeyCode::Char('r') if state.active_tab == Tab::Stats => {
            state.last_stats_refresh = Instant::now() - STATS_REFRESH_INTERVAL;
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
        Tab::Stats => draw_stats_tab(frame, state, chunks[1]),
        Tab::Store => draw_store_tab(frame, state, chunks[1]),
        Tab::Transfer => draw_transfer_tab(frame, state, chunks[1]),
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
        Span::styled("[2] Stats ", style_for(Tab::Stats)),
        Span::raw("  "),
        Span::styled("[3] Store ", style_for(Tab::Store)),
        Span::raw("  "),
        Span::styled("[4] Transfer ", style_for(Tab::Transfer)),
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
    let daemon_tag = match (snap.daemon_connected, state.service_installed) {
        (true, true) => "",
        (true, false) => " (no service)",
        (false, true) => " (daemon offline)",
        (false, false) => " (daemon offline, no service)",
    };
    let block = Block::bordered().title(format!(" kache monitor{daemon_tag} "));

    let total =
        snap.event_stats.local_hits + snap.event_stats.remote_hits + snap.event_stats.misses;
    let (local_pct, remote_pct, miss_pct) = if total > 0 {
        (
            (snap.event_stats.local_hits as f64 / total as f64) * 100.0,
            (snap.event_stats.remote_hits as f64 / total as f64) * 100.0,
            (snap.event_stats.misses as f64 / total as f64) * 100.0,
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

    let wrapper_status = match std::env::var("RUSTC_WRAPPER") {
        Ok(v) if v.contains("kache") => "RUSTC_WRAPPER=kache ✓".to_string(),
        Ok(v) => format!("RUSTC_WRAPPER={v} (not kache!)"),
        Err(_) => "RUSTC_WRAPPER not set ✗".to_string(),
    };

    let kache_version = crate::VERSION;

    let daemon_info = if snap.daemon_connected && !snap.daemon_version.is_empty() {
        let epoch = snap.daemon_build_epoch;
        let my_epoch = crate::daemon::build_epoch();
        if epoch == my_epoch {
            format!("daemon: v{} (epoch {epoch})", snap.daemon_version)
        } else {
            format!(
                "daemon: v{} (epoch {epoch}) \u{2190} MISMATCH, restart daemon",
                snap.daemon_version
            )
        }
    } else {
        "daemon: offline".to_string()
    };

    let my_epoch = crate::daemon::build_epoch();

    let dedup_line = if let Ok(scan_stats) = state.stats.lock() {
        let ls = &scan_stats.link_stats;
        if ls.saved_bytes > 0 {
            format!(
                "  Deduplicated: {}    ({} hardlinks)",
                ByteSize(ls.saved_bytes),
                ls.linked_refs,
            )
        } else {
            "  Deduplicated: 0 B".to_string()
        }
    } else {
        "  Deduplicated: n/a".to_string()
    };

    let transfer_line = if snap.daemon_connected {
        format!(
            "  Transfer: ↑ {} uploading  ↓ {} downloading",
            snap.pending_uploads, snap.active_downloads,
        )
    } else {
        "  Transfer: n/a (daemon offline)".to_string()
    };

    let text = vec![
        Line::from(format!(
            "  Store: {} / {} [{:>5.1}%]    {} entries",
            ByteSize(snap.total_size),
            ByteSize(snap.max_size),
            store_pct,
            snap.entry_count,
        )),
        Line::from(format!(
            "  Hit rate: {local_pct:.0}% local | {remote_pct:.0}% remote | {miss_pct:.0}% miss    Remote: {remote_status}",
        )),
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

fn draw_live_build(frame: &mut Frame, state: &AppState, area: Rect) {
    let border_style = Style::default().fg(Color::Cyan);

    let block = Block::bordered()
        .title(" Live Build ")
        .border_style(border_style);

    let filtered_events: Vec<&BuildEvent> = state
        .events
        .iter()
        .filter(|e| {
            if state.filter.is_empty() {
                true
            } else {
                e.crate_name.contains(&state.filter)
            }
        })
        .collect();

    let max_visible = (area.height as usize).saturating_sub(2);
    let start = if filtered_events.len() > max_visible + state.scroll_offset {
        filtered_events.len() - max_visible - state.scroll_offset
    } else {
        0
    };

    let visible: Vec<Line> = filtered_events
        .iter()
        .skip(start)
        .take(max_visible)
        .map(|event| {
            let (icon, style) = match event.result {
                EventResult::LocalHit => ("✓", Style::default().fg(Color::Green)),
                EventResult::RemoteHit => ("↓", Style::default().fg(Color::Blue)),
                EventResult::Miss => ("✗", Style::default().fg(Color::Yellow)),
                EventResult::Error => ("!", Style::default().fg(Color::Red)),
                EventResult::Skipped => ("→", Style::default().fg(Color::DarkGray)),
            };

            let elapsed = if event.elapsed_ms > 1000 {
                format!("{:.1}s", event.elapsed_ms as f64 / 1000.0)
            } else {
                format!("{}ms", event.elapsed_ms)
            };

            Line::from(vec![
                Span::styled(format!("  {icon} "), style),
                Span::raw(format!("{:<24}", event.crate_name)),
                Span::styled(format!("{:<14}", event.result), style),
                Span::raw(format!("{:>8}  ", elapsed)),
                Span::raw(format!("{:>10}", ByteSize(event.size))),
            ])
        })
        .collect();

    let paragraph = Paragraph::new(visible).block(block);
    frame.render_widget(paragraph, area);
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
            .filter(|e| matches!(e.result, EventResult::LocalHit | EventResult::RemoteHit))
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
        "  q: quit  f: filter  ↑↓: scroll  Tab: next  c: clear  1/2/3/4: tabs".to_string()
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
    let title = format!(
        " Cached Crates — {} entries, {} (sort: {}) ",
        state.stats_snapshot.entry_count,
        ByteSize(state.stats_snapshot.total_size),
        state.sort_mode.label()
    );
    let block = Block::bordered()
        .title(title)
        .border_style(Style::default().fg(Color::Cyan));

    let entries = &state.stats_snapshot.entries;

    let header = Row::new(vec![
        "Key", "Crate", "Type", "Profile", "Size", "Hits", "Created", "Accessed",
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
            Row::new(vec![
                Cell::from(key_short.to_string()),
                Cell::from(entry.crate_name.clone()),
                Cell::from(crate_type.to_string()),
                Cell::from(profile.to_string()),
                Cell::from(ByteSize(entry.size).to_string()),
                Cell::from(entry.hit_count.to_string()),
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
        "  q: quit  s: sort  f: filter  ↑↓: scroll  Tab: next  1/2/3/4: tabs".to_string()
    };

    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Stats tab ──────────────────────────────────────────────────────────────

fn draw_stats_tab(frame: &mut Frame, state: &AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Length(9), // Overview panel
        Constraint::Min(5),    // Projects table
        Constraint::Length(3), // Totals bar
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    draw_stats_overview(frame, state, chunks[0]);
    draw_stats_projects(frame, state, chunks[1]);
    draw_stats_totals(frame, state, chunks[2]);
    draw_stats_help(frame, chunks[3]);
}

fn draw_stats_overview(frame: &mut Frame, state: &AppState, area: Rect) {
    let scan_stats = state.stats.lock().unwrap();
    let scanning = scan_stats.scanning;
    let snap = &state.stats_snapshot;

    let daemon_tag = match (snap.daemon_connected, state.service_installed) {
        (true, true) => "",
        (true, false) => " (no service)",
        (false, true) => " (daemon offline)",
        (false, false) => " (daemon offline, no service)",
    };
    let scan_tag = if scanning { " (scanning...)" } else { "" };
    let title = format!(" kache stats{daemon_tag}{scan_tag} ");
    let block = Block::bordered().title(title);

    let store_pct = if snap.max_size > 0 {
        (snap.total_size as f64 / snap.max_size as f64) * 100.0
    } else {
        0.0
    };

    let es = &snap.event_stats;
    let total_events = es.local_hits + es.remote_hits + es.misses;
    let hit_rate = if total_events > 0 {
        ((es.local_hits + es.remote_hits) as f64 / total_events as f64) * 100.0
    } else {
        0.0
    };

    // Time saved
    let time_saved = if es.total_elapsed_ms > 0 && total_events > 0 {
        let avg_ms = es.total_elapsed_ms / total_events as u64;
        let hits = (es.local_hits + es.remote_hits) as u64;
        let saved_s = (hits * avg_ms) / 1000;
        if saved_s >= 3600 {
            format!("~{:.1}h", saved_s as f64 / 3600.0)
        } else if saved_s >= 60 {
            format!("~{:.0}min", saved_s as f64 / 60.0)
        } else {
            format!("~{saved_s}s")
        }
    } else {
        "n/a".to_string()
    };

    let ls = &scan_stats.link_stats;
    let dedup_ratio = if ls.store_bytes > 0 {
        format!(
            "{:.1}x",
            (ls.store_bytes + ls.saved_bytes) as f64 / ls.store_bytes as f64
        )
    } else {
        "n/a".to_string()
    };

    let wrapper_status = match std::env::var("RUSTC_WRAPPER") {
        Ok(v) if v.contains("kache") => "RUSTC_WRAPPER=kache ✓".to_string(),
        Ok(v) => format!("RUSTC_WRAPPER={v} (not kache!)"),
        Err(_) => "RUSTC_WRAPPER not set ✗".to_string(),
    };

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
                "daemon: v{} (epoch {epoch}) \u{2190} MISMATCH, restart daemon",
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
                "{hit_rate:.0}% (24h: {} hits, {} misses)",
                es.local_hits + es.remote_hits,
                es.misses
            )),
            Span::raw(format!("    Time saved: {time_saved}")),
        ]),
        Line::from(vec![
            Span::styled("  Dedup: ", Style::default().fg(Color::Cyan)),
            Span::raw(format!(
                "{dedup_ratio}    Hardlinks: {} files saving {}",
                ls.linked_refs,
                ByteSize(ls.saved_bytes)
            )),
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

fn draw_stats_projects(frame: &mut Frame, state: &AppState, area: Rect) {
    let stats = state.stats.lock().unwrap();

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
        .stats_scroll
        .min(rows.len().saturating_sub(visible_rows));

    let table = Table::new(rows.into_iter().skip(skip).collect::<Vec<_>>(), widths)
        .header(header)
        .block(block);

    frame.render_widget(table, area);
}

fn draw_stats_totals(frame: &mut Frame, state: &AppState, area: Rect) {
    let stats = state.stats.lock().unwrap();

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

fn draw_stats_help(frame: &mut Frame, area: Rect) {
    let help = "  q: quit  r: refresh  ↑↓: scroll  Tab: next  1/2/3/4: tabs";

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

    draw_transfer_gauge(frame, state, chunks[0]);
    draw_transfer_activity(frame, state, chunks[1]);
    draw_recent_transfers(frame, state, chunks[2]);
    draw_transfer_help(frame, chunks[3]);
}

fn draw_transfer_gauge(frame: &mut Frame, state: &AppState, area: Rect) {
    let snap = &state.stats_snapshot;
    let capacity = snap.upload_queue_capacity.max(1);
    let used = snap.pending_uploads;
    let ratio = (used as f64 / capacity as f64).min(1.0);

    let color = if ratio < 0.5 {
        Color::Green
    } else if ratio < 0.8 {
        Color::Yellow
    } else {
        Color::Red
    };

    let label = format!("{used} / {capacity} queued");
    let gauge = Gauge::default()
        .block(Block::bordered().title(" Upload Queue "))
        .gauge_style(Style::default().fg(color))
        .ratio(ratio)
        .label(label);

    frame.render_widget(gauge, area);
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

    let s3_slots = format!("{} / {}", snap.s3_concurrency_used, snap.s3_concurrency_total);
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
                daemon::TransferDirection::Upload => {
                    ("↑", Style::default().fg(Color::Yellow))
                }
                daemon::TransferDirection::Download => {
                    ("↓", Style::default().fg(Color::Blue))
                }
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
    let help = "  q: quit  ↑↓: scroll  Tab: next  1/2/3/4: tabs";
    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}
