use anyhow::Result;
use bytesize::ByteSize;
use crossterm::ExecutableCommand;
use crossterm::event::{
    self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind,
    KeyModifiers, MouseButton, MouseEvent, MouseEventKind,
};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::prelude::*;
use ratatui::widgets::*;
use std::io::{IsTerminal, stdout};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::cli;
use crate::config::Config;
use crate::daemon;
use crate::events::{self, BuildEvent, EventRecord, EventResult, EventTailer, HeartbeatEvent};
use crate::since::SinceWindow;
use crate::tui_sessions::{self, Analysis, Cause, Session, SessionState};

// ── Terminal mode guard ────────────────────────────────────────────────────

/// RAII owner of the crossterm raw-mode + alternate-screen pair, shared by
/// every TUI entry point (monitor, config editor, interactive clean).
///
/// The straight-line "enable, run, disable" shape leaks a broken terminal
/// on every early exit: a `?` between enable and disable returns with raw
/// mode still on, and a panic unwinds past the restore entirely. Restoring
/// in `Drop` covers both. A process-wide panic hook additionally restores
/// the terminal *before* the panic message prints, so it lands on the
/// user's real screen instead of vanishing with the alternate one.
pub(crate) struct TerminalModeGuard;

#[cfg(test)]
std::thread_local! {
    static TERMINAL_RESTORE_OBSERVED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl TerminalModeGuard {
    pub(crate) fn enter() -> Result<Self> {
        Self::enter_with_mouse(false)
    }

    /// Like [`enter`](Self::enter), optionally asking the terminal to report
    /// mouse events too. Only the monitor wants those: capture disables the
    /// terminal's native text selection, which the config editor and the
    /// interactive clean have no reason to take away.
    pub(crate) fn enter_with_mouse(mouse: bool) -> Result<Self> {
        static PANIC_HOOK: std::sync::Once = std::sync::Once::new();
        PANIC_HOOK.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                restore_terminal();
                previous(info);
            }));
        });
        enable_raw_mode()?;
        if let Err(e) = stdout().execute(EnterAlternateScreen) {
            // No guard exists yet to undo the half-entered state — and
            // the escape sequence may have been written before the error
            // surfaced, so leave the alternate screen too.
            restore_terminal();
            return Err(e.into());
        }
        if mouse && let Err(e) = stdout().execute(EnableMouseCapture) {
            restore_terminal();
            return Err(e.into());
        }
        Ok(Self)
    }
}

impl Drop for TerminalModeGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

/// Idempotent: leaving the main screen buffer and disabling an already
/// disabled raw mode are no-ops, so the hook and the guard can both run.
fn restore_terminal() {
    #[cfg(test)]
    TERMINAL_RESTORE_OBSERVED.with(|observed| observed.set(true));
    // Harmless when capture was never enabled, and it must come before the
    // screen switch: a terminal left reporting mouse motion sprays escape
    // sequences into the shell that follows.
    let _ = stdout().execute(DisableMouseCapture);
    let _ = stdout().execute(LeaveAlternateScreen);
    let _ = disable_raw_mode();
}

// ── Tabs & panels ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tab {
    Build,
    /// Why the selected build missed: causes, passthrough reasons, chronic
    /// misses. Sits next to Build because Enter on a build lands here.
    Why,
    Projects,
    Store,
    Transfer,
}

impl Tab {
    const ORDER: [Tab; 5] = [
        Tab::Build,
        Tab::Why,
        Tab::Projects,
        Tab::Store,
        Tab::Transfer,
    ];

    fn index(self) -> usize {
        Self::ORDER
            .iter()
            .position(|tab| *tab == self)
            .unwrap_or_default()
    }

    fn next(self) -> Tab {
        Self::ORDER[(self.index() + 1) % Self::ORDER.len()]
    }

    fn previous(self) -> Tab {
        Self::ORDER[(self.index() + Self::ORDER.len() - 1) % Self::ORDER.len()]
    }
}

fn tab_needs_entries(tab: Tab) -> bool {
    matches!(tab, Tab::Store)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ScrollAnchor {
    Top,
    Bottom,
}

/// Per-panel scroll state. `offset` is the distance from the panel's default
/// anchor, and is always kept within `0..=max_offset`.
#[derive(Debug, Clone, Copy)]
struct Viewport {
    offset: usize,
    max_offset: usize,
    anchor: ScrollAnchor,
    /// Rows the panel showed on its last draw; the unit for PgUp/PgDn.
    page: usize,
}

impl Viewport {
    fn new(anchor: ScrollAnchor) -> Self {
        Self {
            offset: 0,
            max_offset: 0,
            anchor,
            page: 1,
        }
    }

    fn scroll_up(&mut self) {
        self.scroll_up_by(1);
    }

    fn scroll_down(&mut self) {
        self.scroll_down_by(1);
    }

    fn scroll_up_by(&mut self, rows: usize) {
        match self.anchor {
            ScrollAnchor::Top => self.offset = self.offset.saturating_sub(rows),
            ScrollAnchor::Bottom => {
                self.offset = self.offset.saturating_add(rows).min(self.max_offset)
            }
        }
    }

    fn scroll_down_by(&mut self, rows: usize) {
        match self.anchor {
            ScrollAnchor::Top => {
                self.offset = self.offset.saturating_add(rows).min(self.max_offset)
            }
            ScrollAnchor::Bottom => self.offset = self.offset.saturating_sub(rows),
        }
    }

    fn page_up(&mut self) {
        self.scroll_up_by(self.page.max(1));
    }

    fn page_down(&mut self) {
        self.scroll_down_by(self.page.max(1));
    }

    /// Jump to the first row in logical order.
    fn home(&mut self) {
        self.offset = match self.anchor {
            ScrollAnchor::Top => 0,
            ScrollAnchor::Bottom => self.max_offset,
        };
    }

    /// Jump to the last row in logical order. For a bottom-anchored panel this
    /// is "follow the newest".
    fn end(&mut self) {
        self.offset = match self.anchor {
            ScrollAnchor::Top => self.max_offset,
            ScrollAnchor::Bottom => 0,
        };
    }

    /// Whether the panel is showing its default edge, which for a
    /// bottom-anchored panel means it is following new rows.
    fn at_anchor(&self) -> bool {
        self.offset == 0
    }

    /// `count` rows were added at the panel's live edge. A reader who has
    /// scrolled away keeps looking at the same rows; the live edge is where
    /// new rows land, and only a viewport sitting on it moves with them. The
    /// offset is clamped on the next draw, so overshooting here is harmless.
    fn rows_arrived(&mut self, count: usize) {
        if self.offset > 0 {
            self.offset = self.offset.saturating_add(count);
        }
    }

    fn reset(&mut self) {
        self.offset = 0;
    }

    /// Update this viewport from the rows and height that will actually render,
    /// clamp stale state, and return the corresponding range in logical order.
    fn visible_range(&mut self, item_count: usize, visible_rows: usize) -> std::ops::Range<usize> {
        self.page = visible_rows.max(1);
        let visible_rows = visible_rows.min(item_count);
        self.max_offset = item_count.saturating_sub(visible_rows);
        self.offset = self.offset.min(self.max_offset);

        let start = match self.anchor {
            ScrollAnchor::Top => self.offset,
            ScrollAnchor::Bottom => {
                item_count.saturating_sub(visible_rows.saturating_add(self.offset))
            }
        };
        let end = start.saturating_add(visible_rows).min(item_count);
        start..end
    }
}

/// The help bar for a tab, given its own key list.
///
/// Typing a filter is modal, so the input line replaces the keys. Once a
/// filter is committed the tab's keys come back and only gain `Esc: clear` —
/// the panel title already carries the filter, so repeating it here would cost
/// the row's remaining width for nothing, and dropping the tab's own keys
/// (`s: sort` while filtering the store) would trade one confusion for
/// another.
fn help_line(state: &AppState, keys: &str) -> String {
    if state.filter_active {
        return format!("  filter: {}_   Enter: apply   Esc: cancel", state.filter());
    }
    if state.filter().is_empty() {
        format!("  {keys}")
    } else {
        format!("  {keys}  Esc: clear filter")
    }
}

/// Panel title carrying the active filter, so a narrowed view always says so.
/// Without this a committed filter left no trace on screen: rows were simply
/// missing and nothing explained why.
fn filtered_title(base: &str, filter: &str) -> String {
    if filter.is_empty() {
        base.to_string()
    } else {
        format!("{}[filter: {filter}] ", base)
    }
}

/// Filters are per-tab, so editing one only invalidates its own viewport.
fn reset_filtered_viewports(state: &mut AppState) {
    match state.active_tab {
        Tab::Build => state.build_scroll.reset(),
        Tab::Store => state.store_scroll.reset(),
        Tab::Why => state.why_scroll.reset(),
        Tab::Projects | Tab::Transfer => {}
    }
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

fn effective_remote_status(config: &Config, snap: &StatsSnapshot) -> String {
    if let Some(effective) = snap.daemon_effective_config.as_ref() {
        if let Some(remote) = effective.remote_description.as_ref() {
            return remote.clone();
        }
        if effective.local_only {
            return "local-only".to_string();
        }
        if effective.remote_error.is_some() {
            return "misconfigured".to_string();
        }
        return "not configured".to_string();
    }

    let client = config
        .remote
        .as_ref()
        .map_or_else(|| "not configured".to_string(), |remote| remote.describe());
    if snap.daemon_connected {
        format!("{client} (client config; daemon did not report)")
    } else {
        format!("{client} (client config)")
    }
}

// ── App state ──────────────────────────────────────────────────────────────

/// Project scan data computed in a background thread.
#[derive(Default)]
struct ProjectScanData {
    project_targets: Vec<cli::TargetEntry>,
    scanning: bool,
    scanned: bool,
}

fn project_scan_status(stats_loaded: bool, scanning: bool, scanned: bool) -> &'static str {
    if !stats_loaded || scanning {
        "calculating"
    } else if scanned {
        "idle"
    } else {
        "not scanned"
    }
}

fn project_scan_can_start(is_scanning: bool) -> bool {
    !is_scanning
}

struct AppState {
    config: Config,
    active_tab: Tab,

    // Build tab
    tailer: EventTailer,
    events: Vec<BuildEvent>,
    /// Daemon-offline fallback for the In-flight panel
    /// (kunobi-ninja/kache#131): last heartbeat per child PID from the tailed
    /// event log, expired by age and cleared when the crate's completing
    /// BuildEvent arrives.
    live_heartbeats: std::collections::HashMap<u32, (Instant, HeartbeatEvent)>,
    build_scroll: Viewport,
    /// Crate-name filters, one per filterable tab. A single shared string made
    /// the three tabs filter each other: typing `serde` on Build and pressing
    /// `3` left the Store table silently narrowed to `serde`, with the title
    /// and help bar showing nothing about it.
    build_filter: String,
    store_filter: String,
    why_filter: String,
    filter_active: bool,

    // Store tab
    sort_mode: SortMode,
    store_scroll: Viewport,

    // Store + event stats (daemon-first snapshot)
    stats_snapshot: StatsSnapshot,
    stats_loaded: bool,
    last_stats_fetch: Instant,

    // Projects tab (shared with background scanner thread for target/ scanning)
    project_scan: Arc<Mutex<ProjectScanData>>,
    last_project_refresh: Instant,
    project_scroll: Viewport,

    // Build sessions (kunobi-ninja/kache#583). Regrouped when the log grew,
    // re-sorted every tick. The selected build drives the Build event panel
    // and the Why tab.
    sessions: Vec<Session>,
    /// `events.len()` the sessions were grouped from.
    grouped_len: usize,
    /// The session the reader picked, by key; `None` follows the top row,
    /// which is the newest running build.
    selected_session: Option<String>,
    /// The build the panels showed last tick, and how many events it had,
    /// so a change of build resets the viewports and new rows in the same
    /// build keep a scrolled reader in place.
    shown: Option<(String, usize)>,
    /// The Why analysis for `(session key, event count)` it was computed
    /// for. Recomputed only when either changes: the cascade walk is per
    /// miss and not free.
    why_cache: Option<(String, usize, Analysis)>,

    // Transfer tab
    transfer_scroll: Viewport,
    /// Why tab: the whole explanation is one scrollable body.
    why_scroll: Viewport,
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
    /// `p` freezes the screen: no new events are read, no snapshot or scan is
    /// started. The tailer keeps its offset, so resuming replays everything
    /// that happened meanwhile rather than dropping it.
    paused: bool,
    /// The time span the lookup sparklines cover. Five minutes by default;
    /// the `--since` window when one was given, so a `--since 24h` session
    /// sees its history instead of an empty strip.
    spark_window: Duration,
    rustc_version: String,
    wrapper_status: String,
    service_installed: bool,
}

impl AppState {
    /// The viewport of the panel the active tab scrolls.
    fn active_viewport(&mut self) -> &mut Viewport {
        match self.active_tab {
            Tab::Build => &mut self.build_scroll,
            Tab::Projects => &mut self.project_scroll,
            Tab::Store => &mut self.store_scroll,
            Tab::Transfer => &mut self.transfer_scroll,
            Tab::Why => &mut self.why_scroll,
        }
    }

    /// Read what the event log appended since the last tick: build events
    /// and heartbeats (kunobi-ninja/kache#131). Paused, nothing is read; the
    /// tailer keeps its offset, so resuming replays it all.
    fn ingest_tailed_records(&mut self) {
        if self.paused {
            return;
        }
        let Ok(records) = self.tailer.poll_records() else {
            return;
        };
        for record in records {
            match record {
                EventRecord::Build(event) => {
                    // A completing BuildEvent ends that crate's in-flight
                    // status (heartbeats carry a pid, BuildEvents don't —
                    // match on crate+root). Known cosmetic limit: two
                    // concurrent units of the SAME crate+root (host vs
                    // target) both clear on the first completion; the
                    // survivor reappears on its next beat within one
                    // cadence.
                    self.live_heartbeats.retain(|_, (_, hb)| {
                        hb.crate_name != event.crate_name || hb.root != event.root
                    });
                    self.push_event(*event);
                }
                EventRecord::Heartbeat(hb) => {
                    self.live_heartbeats.insert(hb.pid, (Instant::now(), hb));
                }
            }
        }
    }

    /// Whether to start a snapshot fetch this tick: not while paused, never
    /// two at once, and only at the refresh cadence.
    fn stats_fetch_due(&self) -> bool {
        !self.paused
            && !self.stats_fetch_in_flight
            && self.last_stats_fetch.elapsed() >= SNAPSHOT_REFRESH_INTERVAL
    }

    /// Whether to start a target-dir scan this tick: only while the Projects
    /// tab is showing (the scan is expensive on big workspaces), not while
    /// paused, and only at its own cadence.
    fn project_scan_due(&self) -> bool {
        !self.paused
            && self.active_tab == Tab::Projects
            && self.last_project_refresh.elapsed() >= PROJECT_REFRESH_INTERVAL
    }

    /// Append a tailed build event. Scroll bookkeeping happens when the
    /// sessions are refreshed, once the event is known to belong to the
    /// build on screen.
    fn push_event(&mut self, event: BuildEvent) {
        self.events.push(event);
    }

    /// Bring the sessions up to date: regroup when the log grew, decide
    /// which are live and sort them every tick (a build goes from running to
    /// done by sitting idle), then reconcile the panels with the build that
    /// ends up selected.
    fn refresh_sessions(&mut self, now: chrono::DateTime<chrono::Utc>) {
        let live_roots: std::collections::HashSet<String> = self
            .in_flight_view()
            .into_iter()
            .map(|entry| entry.root)
            .filter(|root| !root.is_empty())
            .collect();
        if self.grouped_len != self.events.len() {
            self.sessions = tui_sessions::group(
                &self.events,
                Duration::from_secs(crate::wrapper::BUILD_SESSION_SECS),
            );
            self.grouped_len = self.events.len();
        }
        tui_sessions::refresh_state(&mut self.sessions, now, &live_roots);
        self.reconcile_shown_build();
    }

    /// Keep the Build and Why panels honest about which build they show.
    /// A different build (a pick, or the top row changing while following)
    /// starts both panels from their default edge. The same build with new
    /// rows keeps a scrolled-back reader on the rows they were reading;
    /// rows from other builds never move anything.
    fn reconcile_shown_build(&mut self) {
        let now_shown = self
            .selected_session()
            .map(|session| (session.key.clone(), session.events.len()));
        match (&self.shown, &now_shown) {
            (Some((was, had)), Some((is, has))) if was == is => {
                let arrived = self
                    .selected_session()
                    .map(|session| {
                        session.events[*had..*has]
                            .iter()
                            .filter(|&&index| {
                                let event = &self.events[index];
                                self.build_filter.is_empty()
                                    || event.crate_name.contains(&self.build_filter)
                            })
                            .count()
                    })
                    .unwrap_or(0);
                self.build_scroll.rows_arrived(arrived);
            }
            (Some(_), Some(_)) | (None, Some(_)) => {
                self.build_scroll.reset();
                self.why_scroll.reset();
            }
            (_, None) => {}
        }
        self.shown = now_shown;
    }

    /// The build the Build and Why tabs are about: the reader's pick when it
    /// still exists, else the top row.
    fn selected_session(&self) -> Option<&Session> {
        self.selected_session
            .as_deref()
            .and_then(|key| self.sessions.iter().find(|s| s.key == key))
            .or_else(|| self.sessions.first())
    }

    fn selected_index(&self) -> Option<usize> {
        let key = self.selected_session()?.key.as_str();
        self.sessions.iter().position(|s| s.key == key)
    }

    /// Move the selection one row down. The pick becomes explicit even on
    /// the last row, so a new build arriving on top no longer changes what
    /// the reader is looking at.
    fn select_next_session(&mut self) {
        let Some(index) = self.selected_index() else {
            return;
        };
        let next = (index + 1).min(self.sessions.len().saturating_sub(1));
        self.selected_session = Some(self.sessions[next].key.clone());
        self.reconcile_shown_build();
    }

    /// Move the selection one row up. Above the top row is "follow the
    /// newest build", the state the monitor starts in.
    fn select_previous_session(&mut self) {
        let Some(index) = self.selected_index() else {
            return;
        };
        if index == 0 {
            self.selected_session = None;
        } else {
            self.selected_session = Some(self.sessions[index - 1].key.clone());
        }
        self.reconcile_shown_build();
    }

    /// The Why analysis for the selected build, computed on first use and
    /// whenever the event count moved.
    fn why_analysis(&mut self) -> Option<&Analysis> {
        let session = self.selected_session()?;
        let key = session.key.clone();
        let stale =
            !matches!(&self.why_cache, Some((k, n, _)) if *k == key && *n == self.events.len());
        if stale {
            let analysis = tui_sessions::analyze_session(&self.events, session, &self.sessions);
            self.why_cache = Some((key, self.events.len(), analysis));
        }
        self.why_cache.as_ref().map(|(_, _, analysis)| analysis)
    }

    /// The active tab's filter, empty for tabs that do not filter.
    fn filter(&self) -> &str {
        match self.active_tab {
            Tab::Build => &self.build_filter,
            Tab::Store => &self.store_filter,
            Tab::Why => &self.why_filter,
            Tab::Projects | Tab::Transfer => "",
        }
    }

    /// Mutable handle to the active tab's filter; `None` on tabs that do not
    /// filter, which is what makes `f` and `Esc` no-ops there.
    fn filter_mut(&mut self) -> Option<&mut String> {
        match self.active_tab {
            Tab::Build => Some(&mut self.build_filter),
            Tab::Store => Some(&mut self.store_filter),
            Tab::Why => Some(&mut self.why_filter),
            Tab::Projects | Tab::Transfer => None,
        }
    }

    /// In-flight compiles for the panel: the daemon registry when available,
    /// else derived from tailed heartbeat lines (daemonless builds still get
    /// the panel; kunobi-ninja/kache#131). Entries sorted oldest-first.
    fn in_flight_view(&self) -> Vec<crate::daemon::InFlightEntry> {
        if !self.stats_snapshot.in_flight.is_empty() {
            return self.stats_snapshot.in_flight.clone();
        }
        let mut entries: Vec<crate::daemon::InFlightEntry> = self
            .live_heartbeats
            .values()
            .map(|(seen, hb)| crate::daemon::InFlightEntry {
                crate_name: hb.crate_name.clone(),
                root: hb.root.clone(),
                pid: hb.pid,
                // The heartbeat's elapsed plus time since we read it, so the
                // panel keeps counting between beats.
                elapsed_s: hb.elapsed_s + seen.elapsed().as_secs(),
                typical_s: hb.typical_s,
                eta_s: hb
                    .typical_s
                    .map(|t| t.saturating_sub(hb.elapsed_s + seen.elapsed().as_secs())),
            })
            .collect();
        entries.sort_by_key(|e| std::cmp::Reverse(e.elapsed_s));
        entries
    }
}

const PROJECT_REFRESH_INTERVAL: Duration = Duration::from_secs(10);
const SNAPSHOT_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

// ── Entry point ────────────────────────────────────────────────────────────

/// Run the TUI monitor dashboard.
pub fn run_monitor(config: &Config, since: Option<SinceWindow>) -> Result<()> {
    if !stdout().is_terminal() {
        anyhow::bail!(
            "kache monitor needs a terminal; use `kache stats` for a plain-text summary that can be redirected"
        );
    }
    let _terminal_mode = TerminalModeGuard::enter_with_mouse(true)?;

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    let tailer = if since.is_some() {
        EventTailer::from_start(config.event_log_path())
    } else {
        EventTailer::new(config.event_log_path())
    };

    let initial_events = if let Some(window) = since {
        let cutoff = window.cutoff(chrono::Utc::now());
        events::read_events_since(&config.event_log_path(), cutoff).unwrap_or_default()
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
        live_heartbeats: std::collections::HashMap::new(),
        build_scroll: Viewport::new(ScrollAnchor::Bottom),
        build_filter: String::new(),
        store_filter: String::new(),
        why_filter: String::new(),
        filter_active: false,
        sort_mode: SortMode::Size,
        store_scroll: Viewport::new(ScrollAnchor::Top),
        stats_snapshot,
        stats_loaded: false,
        last_stats_fetch: Instant::now() - SNAPSHOT_REFRESH_INTERVAL, // trigger immediate first fetch
        project_scan,
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
        rustc_version_slot: Arc::clone(&rustc_version_slot),
        stats_result_slot: Arc::clone(&stats_result_slot),
        stats_fetch_in_flight: false,
        stats_fetch_requested_entries: false,
        should_quit: false,
        paused: false,
        spark_window: since.map_or(SPARK_WINDOW, |window| {
            Duration::from_secs(window.secs().max(60))
        }),
        rustc_version: "\u{2026}".to_string(), // placeholder until background thread completes
        wrapper_status: crate::wrapper_config::wrapper_status_line(),
        service_installed,
    };

    loop {
        state.ingest_tailed_records();
        // Expire heartbeats whose wrapper stopped beating (killed build).
        let stale_after = Duration::from_secs(state.config.heartbeat_secs.max(30) * 3);
        state
            .live_heartbeats
            .retain(|_, (seen, _)| seen.elapsed() < stale_after);

        // Sessions depend on the clock (a build goes from running to done by
        // sitting idle), so they are regrouped every tick, paused or not.
        state.refresh_sessions(chrono::Utc::now());

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
        if state.stats_fetch_due() {
            state.stats_fetch_in_flight = true;
            state.last_stats_fetch = Instant::now();
            let cfg = state.config.clone();
            let sort = state.sort_mode.label().to_string();
            let include_entries = tab_needs_entries(state.active_tab);
            state.stats_fetch_requested_entries = include_entries;
            let slot = Arc::clone(&state.stats_result_slot);
            std::thread::spawn(move || {
                // Auto-start stays silent here: the raw-mode alternate screen
                // owns the terminal, so a stderr notice would corrupt it.
                let snap = cli::fetch_stats_snapshot(
                    &cfg,
                    include_entries,
                    &sort,
                    SinceWindow::DEFAULT,
                    false,
                    false,
                );
                if let Ok(mut s) = slot.lock() {
                    *s = Some(snap);
                }
            });
        }

        // Refresh target/ scan periodically when on stats tab
        if state.project_scan_due() {
            let is_scanning = state
                .project_scan
                .lock()
                .map(|s| s.scanning)
                .unwrap_or(false);
            if project_scan_can_start(is_scanning) {
                let root = std::env::current_dir().unwrap_or_default();
                drop(spawn_project_scan(Arc::clone(&state.project_scan), root));
                state.last_project_refresh = Instant::now();
            }
        }

        terminal.draw(|frame| draw_ui(frame, &mut state))?;

        if event::poll(Duration::from_millis(100))? {
            let area = terminal.size()?.into();
            handle_terminal_event(&mut state, event::read()?, area);
        }

        if state.should_quit {
            break;
        }
    }

    Ok(())
}

/// Spawn a background thread to scan target dirs.
/// Results stream in progressively — each discovered project updates the UI immediately.
fn spawn_project_scan(
    stats: Arc<Mutex<ProjectScanData>>,
    root: std::path::PathBuf,
) -> std::thread::JoinHandle<()> {
    if let Ok(mut s) = stats.lock() {
        s.scanning = true;
        // Mark existing entries stale instead of clearing — keeps the UI populated
        for t in s.project_targets.iter_mut() {
            t.stale = true;
        }
    }
    std::thread::spawn(move || {
        // Discover target dirs and scan each one progressively.
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
    })
}

// ── Key handling ───────────────────────────────────────────────────────────

/// Move to `tab`, forcing the refresh that tab's data needs. One place for it,
/// so the number keys and both Tab directions cannot drift apart.
fn switch_tab(state: &mut AppState, tab: Tab) {
    state.active_tab = tab;
    match tab {
        Tab::Projects => state.last_project_refresh = Instant::now() - PROJECT_REFRESH_INTERVAL,
        Tab::Store => state.last_stats_fetch = Instant::now() - SNAPSHOT_REFRESH_INTERVAL,
        Tab::Build | Tab::Transfer | Tab::Why => {}
    }
}

/// One terminal event: key presses and mouse events are dispatched, key
/// releases and repeats (some terminals report them) and resizes are not.
/// Kept out of the loop so it can be exercised without a terminal.
fn handle_terminal_event(state: &mut AppState, event: Event, area: Rect) {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => handle_key_event(state, key),
        Event::Mouse(mouse) => handle_mouse(state, mouse, area),
        _ => {}
    }
}

/// Keys that carry a modifier are decided here; everything else is the plain
/// code. Raw mode turns Ctrl+C into a key event instead of a signal, and
/// dropping the modifier made it a bare `c`: on the Build tab that cleared
/// the event list, which is the opposite of what the finger meant.
fn handle_key_event(state: &mut AppState, key: KeyEvent) {
    if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
        state.should_quit = true;
        return;
    }
    handle_key(state, key.code);
}

/// Mouse support: clicking a tab title selects it; the wheel scrolls the
/// active tab's panel three rows at a time. Anything else is ignored.
fn handle_mouse(state: &mut AppState, mouse: MouseEvent, area: Rect) {
    if terminal_too_small(area) {
        return;
    }
    match mouse.kind {
        MouseEventKind::Down(MouseButton::Left) if mouse.row == area.y => {
            if let Some(tab) = tab_at_column(mouse.column.saturating_sub(area.x)) {
                switch_tab(state, tab);
            }
        }
        MouseEventKind::ScrollUp => state.active_viewport().scroll_up_by(3),
        MouseEventKind::ScrollDown => state.active_viewport().scroll_down_by(3),
        _ => {}
    }
}

fn handle_key(state: &mut AppState, key: KeyCode) {
    // Filter input mode
    if state.filter_active {
        match key {
            KeyCode::Enter => state.filter_active = false,
            // Cancel: drop the filter being typed rather than committing it.
            // Leaving it applied is how an invisible filter used to survive.
            KeyCode::Esc => {
                state.filter_active = false;
                if let Some(filter) = state.filter_mut()
                    && !filter.is_empty()
                {
                    filter.clear();
                    reset_filtered_viewports(state);
                }
            }
            KeyCode::Backspace => {
                if state.filter_mut().and_then(|f| f.pop()).is_some() {
                    reset_filtered_viewports(state);
                }
            }
            KeyCode::Char(c) => {
                if let Some(filter) = state.filter_mut() {
                    filter.push(c);
                    reset_filtered_viewports(state);
                }
            }
            _ => {}
        }
        return;
    }

    match key {
        // `q` alone quits. Esc is the universal back-out key, so binding it to
        // quit meant dismissing a filter and then pressing it once more out of
        // reflex tore down the session.
        KeyCode::Char('q') => state.should_quit = true,
        // Esc outside input mode clears the current tab's filter, which is
        // otherwise only removable by re-entering the filter and holding
        // backspace.
        KeyCode::Esc => {
            if let Some(filter) = state.filter_mut()
                && !filter.is_empty()
            {
                filter.clear();
                reset_filtered_viewports(state);
            }
        }
        // Tab switching
        KeyCode::Char('1') => switch_tab(state, Tab::Build),
        KeyCode::Char('2') => switch_tab(state, Tab::Why),
        KeyCode::Char('3') => switch_tab(state, Tab::Projects),
        KeyCode::Char('4') => switch_tab(state, Tab::Store),
        KeyCode::Char('5') => switch_tab(state, Tab::Transfer),
        KeyCode::Tab => switch_tab(state, state.active_tab.next()),
        // Shift+Tab used to share an arm with Tab and cycle forward too, so
        // there was no way back except by number.
        KeyCode::BackTab => switch_tab(state, state.active_tab.previous()),
        // On Build and Why, Up/Down pick the build; the event panel scrolls by
        // page and by End. A flat list needed one axis, a list of builds with
        // a panel each needs two.
        KeyCode::Up | KeyCode::Char('k') if matches!(state.active_tab, Tab::Build | Tab::Why) => {
            state.select_previous_session();
        }
        KeyCode::Down | KeyCode::Char('j') if matches!(state.active_tab, Tab::Build | Tab::Why) => {
            state.select_next_session();
        }
        KeyCode::Enter if state.active_tab == Tab::Build => switch_tab(state, Tab::Why),
        // Scrolling: one row, one page, or straight to either end. `j`/`k`
        // for hands that live on the home row.
        KeyCode::Up | KeyCode::Char('k') => state.active_viewport().scroll_up(),
        KeyCode::Down | KeyCode::Char('j') => state.active_viewport().scroll_down(),
        KeyCode::PageUp => state.active_viewport().page_up(),
        KeyCode::PageDown => state.active_viewport().page_down(),
        KeyCode::Home => state.active_viewport().home(),
        KeyCode::End => state.active_viewport().end(),
        // Pause is global: it freezes every tab, not the one in front.
        KeyCode::Char('p') => state.paused = !state.paused,
        // Build tab
        // One arm for every filterable tab: `filter_mut` already encodes which
        // those are, so `f` is inert on Projects and Transfer instead of
        // opening an input that edits nothing.
        KeyCode::Char('f') if state.filter_mut().is_some() => {
            state.filter_active = true;
        }
        KeyCode::Char('c') if state.active_tab == Tab::Build => {
            state.events.clear();
            state.sessions.clear();
            state.selected_session = None;
            state.why_cache = None;
            state.build_scroll.reset();
        }
        // Store tab
        KeyCode::Char('s') if state.active_tab == Tab::Store => {
            state.sort_mode = state.sort_mode.next();
            state.store_scroll.reset();
            state.last_stats_fetch = Instant::now() - SNAPSHOT_REFRESH_INTERVAL;
        }
        // Projects tab: force refresh
        KeyCode::Char('r') if state.active_tab == Tab::Projects => {
            state.last_project_refresh = Instant::now() - PROJECT_REFRESH_INTERVAL;
        }
        _ => {}
    }
}

// ── Drawing ────────────────────────────────────────────────────────────────

/// The smallest terminal the layout still reads on. Below this the fixed-height
/// panels overlap and the tables lose their headers, so say so instead.
const MIN_WIDTH: u16 = 60;
const MIN_HEIGHT: u16 = 16;

fn terminal_too_small(area: Rect) -> bool {
    area.width < MIN_WIDTH || area.height < MIN_HEIGHT
}

fn draw_ui(frame: &mut Frame, state: &mut AppState) {
    let area = frame.area();

    if terminal_too_small(area) {
        frame.render_widget(
            Paragraph::new(format!(
                "Terminal is {}×{}; kache monitor needs at least {MIN_WIDTH}×{MIN_HEIGHT}.\n\n`kache stats` prints the same numbers as text.\nq quits.",
                area.width, area.height
            ))
            .wrap(Wrap { trim: false }),
            area,
        );
        return;
    }

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
        Tab::Why => draw_why_tab(frame, state, chunks[1]),
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

    let mut spans = Vec::with_capacity(Tab::ORDER.len() * 2);
    for (tab, label, _) in tab_titles() {
        spans.push(Span::styled(label, style_for(tab)));
        spans.push(Span::raw("  "));
    }
    if state.paused {
        spans.push(Span::styled(
            " PAUSED (p resumes) ",
            Style::default()
                .fg(Color::Black)
                .bg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
}

/// Every tab's label and the column it starts at in the tab bar. One place
/// for the geometry so the drawing and the mouse hit-test cannot disagree.
fn tab_titles() -> [(Tab, &'static str, u16); 5] {
    const LABELS: [(Tab, &str); 5] = [
        (Tab::Build, " [1] Build "),
        (Tab::Why, "[2] Why "),
        (Tab::Projects, "[3] Projects"),
        (Tab::Store, "[4] Store "),
        (Tab::Transfer, "[5] Transfer "),
    ];
    let mut x = 0u16;
    LABELS.map(|(tab, label)| {
        let start = x;
        x += label.len() as u16 + 2;
        (tab, label, start)
    })
}

/// The tab whose label covers `column` of the tab bar, if any.
fn tab_at_column(column: u16) -> Option<Tab> {
    tab_titles()
        .into_iter()
        .find(|(_, label, start)| column >= *start && column < start + label.len() as u16)
        .map(|(tab, _, _)| tab)
}

// ── Build tab (existing monitor) ───────────────────────────────────────────

fn draw_build_tab(frame: &mut Frame, state: &mut AppState, area: Rect) {
    // The In-flight panel (kunobi-ninja/kache#131) only takes rows while
    // something is actually compiling; idle sessions keep the classic layout.
    let in_flight = state.in_flight_view();
    let in_flight_rows = if in_flight.is_empty() {
        0
    } else {
        // Panel = border (2) + one row per compile, capped so a -j16 build
        // can't crowd out the event stream.
        (in_flight.len().min(6) + 2) as u16
    };
    // Builds panel: border (2) + header (1) + up to five rows. Absent until
    // there is a build to list, so an idle monitor keeps the classic layout.
    let has_builds = !state.sessions.is_empty();
    let builds_rows = if has_builds {
        (state.sessions.len().min(5) + 3) as u16
    } else {
        0
    };
    // On a short terminal the sparkline goes before the event rows do.
    let show_spark = area.height >= 30;
    let spark_rows = if show_spark { 5 } else { 0 };
    let chunks = Layout::vertical([
        Constraint::Length(9),              // Stats bar
        Constraint::Length(in_flight_rows), // In-flight compiles (if any)
        Constraint::Length(builds_rows),    // Builds (if any)
        Constraint::Min(6),                 // Selected build's events
        Constraint::Length(spark_rows),     // Sparkline
        Constraint::Length(1),              // Help bar
    ])
    .split(area);

    draw_stats_bar(frame, state, chunks[0]);
    if in_flight_rows > 0 {
        draw_in_flight(frame, &in_flight, chunks[1]);
    }
    if has_builds {
        draw_builds(frame, state, chunks[2]);
    }
    draw_live_build(frame, state, chunks[3]);
    if show_spark {
        draw_sparkline(frame, state, chunks[4]);
    }
    draw_build_help(frame, state, chunks[5]);
}

/// `4m12s`-style compact milliseconds for cost strips; blank for zero.
fn fmt_saved_ms(ms: u64) -> String {
    if ms == 0 {
        return "0s".to_string();
    }
    if ms < 1000 {
        return format!("{ms}ms");
    }
    fmt_secs(ms / 1000)
}

/// The Builds table: one row per session, running builds first, the selected
/// one marked. Selecting a build scopes the event panel and the Why tab.
fn draw_builds(frame: &mut Frame, state: &mut AppState, area: Rect) {
    let selected = state.selected_index().unwrap_or(0);
    let count = state.sessions.len();
    let following = state.selected_session.is_none();
    let title = format!(
        " Builds · {}/{}{} ",
        selected + 1,
        count,
        if following {
            " · following the top build"
        } else {
            ""
        }
    );
    let block = Block::bordered()
        .title(title)
        .border_style(Style::default().fg(Color::Cyan));

    let wide = area.width >= 100;
    let mut labels = vec!["Build", "Started", "State"];
    let mut widths = vec![
        Constraint::Min(16),
        Constraint::Length(8),
        Constraint::Length(8),
    ];
    let numeric_from = labels.len();
    labels.extend(["hit", "miss"]);
    widths.extend([Constraint::Length(6), Constraint::Length(6)]);
    if wide {
        labels.push("pass");
        widths.push(Constraint::Length(6));
    }
    labels.push("rate");
    widths.push(Constraint::Length(5));
    if wide {
        labels.push("saved");
        widths.push(Constraint::Length(8));
    }
    let header = Row::new(
        labels
            .iter()
            .enumerate()
            .map(|(i, label)| {
                let line = Line::from(*label);
                Cell::from(if i >= numeric_from {
                    line.right_aligned()
                } else {
                    line
                })
            })
            .collect::<Vec<_>>(),
    )
    .style(Style::default().fg(Color::DarkGray));

    let right = |text: String, style: Style| Cell::from(Line::styled(text, style).right_aligned());
    let count_cell = |value: u64, color: Color| {
        right(
            value.to_string(),
            Style::default().fg(if value == 0 { Color::DarkGray } else { color }),
        )
    };
    let rows: Vec<Row> = state
        .sessions
        .iter()
        .map(|session| {
            let state_style = match session.state {
                SessionState::Live => Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
                SessionState::Finished => Style::default().fg(Color::DarkGray),
            };
            let name = if session.inferred {
                format!("{} ~", session.workspace_name())
            } else {
                session.workspace_name().to_string()
            };
            let mut cells = vec![
                Cell::from(name),
                Cell::from(
                    session
                        .started
                        .with_timezone(&chrono::Local)
                        .format("%H:%M:%S")
                        .to_string(),
                ),
                Cell::from(Span::styled(session.state.label(), state_style)),
                count_cell(session.tally.hits, Color::Green),
                count_cell(session.tally.compiled(), Color::White),
            ];
            if wide {
                cells.push(count_cell(session.tally.passthroughs, Color::Magenta));
            }
            cells.push(right(
                session
                    .tally
                    .hit_rate()
                    .map_or_else(|| "-".to_string(), |rate| format!("{rate:.0}%")),
                Style::default(),
            ));
            if wide {
                cells.push(right(
                    fmt_saved_ms(session.tally.saved_ms),
                    Style::default().fg(Color::Green),
                ));
            }
            Row::new(cells)
        })
        .collect();

    let visible = area.height.saturating_sub(3) as usize;
    let table = Table::new(rows, widths)
        .header(header)
        .highlight_symbol("▸ ")
        .row_highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::REVERSED),
        )
        .block(block);
    // A stateful table keeps the selected row visible past the first page.
    let mut table_state = TableState::default()
        .with_selected(selected)
        .with_offset(selected.saturating_add(1).saturating_sub(visible.max(1)));
    frame.render_stateful_widget(table, area, &mut table_state);
}

/// Render the in-flight compiles panel: oldest first, one line each.
fn draw_in_flight(frame: &mut Frame, entries: &[crate::daemon::InFlightEntry], area: Rect) {
    let lines: Vec<Line> = entries
        .iter()
        .take(6)
        .map(|e| {
            let mut spans = vec![
                Span::styled(
                    format!("{:<24}", e.crate_name),
                    Style::default().fg(Color::Yellow),
                ),
                Span::raw(format!(" {} elapsed", fmt_secs(e.elapsed_s))),
            ];
            if let (Some(t), Some(eta)) = (e.typical_s, e.eta_s) {
                spans.push(Span::styled(
                    format!("  (typical {}, ETA {})", fmt_secs(t), fmt_secs(eta.max(1))),
                    Style::default().fg(Color::DarkGray),
                ));
            }
            spans.push(Span::styled(
                format!("  pid {}", e.pid),
                Style::default().fg(Color::DarkGray),
            ));
            Line::from(spans)
        })
        .collect();
    let block = Block::default().borders(Borders::ALL).title(" In flight ");
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

/// `4m20s`-style compact seconds for the in-flight panel.
fn fmt_secs(total: u64) -> String {
    let (h, m, s) = (total / 3600, (total % 3600) / 60, total % 60);
    if h > 0 {
        format!("{h}h{m:02}m")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
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

    let remote_status = effective_remote_status(&state.config, snap);
    let effective_cache_dir = snap
        .daemon_effective_config
        .as_ref()
        .map(|eff| std::path::Path::new(eff.cache_dir.as_str()))
        .unwrap_or(&state.config.cache_dir);

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
            let dedup_status =
                project_scan_status(state.stats_loaded, scan_stats.scanning, scan_stats.scanned);
            format!("Scan: {dedup_status}")
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
                "  Dedup: {} saved ({:.1}%)    Blobs: {} physical    {scan_part}",
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
            shorten_home(effective_cache_dir)
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

fn draw_live_build(frame: &mut Frame, state: &mut AppState, area: Rect) {
    // A reader who scrolled back is told so, and told the way back: rows that
    // stop moving otherwise look like a build that stopped.
    let scrolled = if state.build_scroll.at_anchor() {
        ""
    } else {
        " · scrolled back, End follows"
    };
    // The selected build's name and its cost strip: what the cache saved it,
    // what its misses cost, and what kache itself cost. The last figure is
    // the one nobody else reports.
    let title = match state.selected_session() {
        Some(session) => {
            let t = &session.tally;
            let overhead = t.overhead_share().map_or_else(
                || fmt_saved_ms(t.overhead_ms),
                |share| format!("{} ({share:.0}%)", fmt_saved_ms(t.overhead_ms)),
            );
            format!(
                " {} · saved {} · in misses {} · kache {overhead}{scrolled} ",
                session.workspace_name(),
                fmt_saved_ms(t.saved_ms),
                fmt_saved_ms(t.miss_ms),
            )
        }
        None => format!(" Live Build{scrolled} "),
    };
    let block = Block::bordered()
        .title(filtered_title(&title, state.filter()))
        .border_style(Style::default().fg(Color::Cyan));

    if state.sessions.is_empty() {
        frame.render_widget(
            Paragraph::new(
                "  Waiting for builds…\n\n  Run `cargo build` in any workspace; builds from every\n  workspace on this machine appear here, newest on top.",
            )
            .block(block),
            area,
        );
        return;
    }

    let filter_empty = state.filter().is_empty();
    let filter_label = state.filter().to_string();
    let selected_events: Vec<usize> = state
        .selected_session()
        .map(|session| session.events.clone())
        .unwrap_or_default();
    let filtered_events: Vec<&BuildEvent> = selected_events
        .iter()
        .map(|&index| &state.events[index])
        .filter(|e| filter_empty || e.crate_name.contains(&filter_label))
        .collect();

    // -2 for the borders, -1 for the header row.
    let visible_rows = (area.height as usize).saturating_sub(3);
    let range = state
        .build_scroll
        .visible_range(filtered_events.len(), visible_rows);

    // Narrow terminals lose the columns a reader can live without, right to
    // left, instead of clipping every column into an unreadable grid.
    let show_size = area.width >= 90;
    let show_compile = area.width >= 78;

    let mut header = vec![
        Cell::from("Status"),
        Cell::from("Crate"),
        Cell::from("Action"),
    ];
    let mut widths = vec![
        Constraint::Length(11), // Status (icon + word)
        Constraint::Min(14),    // Crate
        Constraint::Length(19), // Action
    ];
    if show_compile {
        header.push(Cell::from(Line::from("Compile").right_aligned()));
        widths.push(Constraint::Length(9));
    }
    header.push(Cell::from(Line::from("Total").right_aligned()));
    widths.push(Constraint::Length(8));
    if show_size {
        header.push(Cell::from(Line::from("Size").right_aligned()));
        widths.push(Constraint::Length(11));
    }
    let header = Row::new(header).style(Style::default().fg(Color::DarkGray));

    let rows: Vec<Row> = filtered_events
        .iter()
        .skip(range.start)
        .take(range.len())
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
            let mut cells = vec![
                Cell::from(format!("{icon} {status}")).style(cstyle),
                Cell::from(event.crate_name.clone()),
                Cell::from(action).style(cstyle),
            ];
            if show_compile {
                cells.push(
                    Cell::from(Line::from(compile).right_aligned())
                        .style(Style::default().fg(Color::DarkGray)),
                );
            }
            cells.push(Cell::from(Line::from(total).right_aligned()));
            if show_size {
                cells.push(Cell::from(Line::from(size).right_aligned()));
            }
            Row::new(cells)
        })
        .collect();

    // ratatui clips each cell to its column width, so a long crate name
    // truncates inside the Crate column instead of shoving every later column
    // out of its grid (the old hand-padded `format!` rows misaligned on long
    // names and ambiguous-width glyphs).
    let table = Table::new(rows, widths).header(header).block(block);
    frame.render_widget(table, area);
}

/// How far back the lookup sparklines look when no `--since` was given.
const SPARK_WINDOW: Duration = Duration::from_secs(5 * 60);

/// Hits and misses per time bucket over the last `window`, oldest first, one
/// bucket per column. Time-based rather than event-based so the strip is a
/// clock: a quiet minute is a flat minute, and a burst is a burst, instead of
/// every event being one step regardless of when it happened.
fn lookup_series(
    events: &[BuildEvent],
    now: chrono::DateTime<chrono::Utc>,
    window: Duration,
    columns: usize,
) -> (Vec<u64>, Vec<u64>) {
    let mut hits = vec![0u64; columns];
    let mut misses = vec![0u64; columns];
    if columns == 0 {
        return (hits, misses);
    }
    let window_ms = window.as_millis().max(1);
    for event in events {
        let age_ms = now.signed_duration_since(event.ts).num_milliseconds();
        if age_ms < 0 {
            continue;
        }
        let age_ms = age_ms as u128;
        if age_ms >= window_ms {
            continue;
        }
        let index = columns - 1 - (age_ms * columns as u128 / window_ms) as usize;
        match event.result {
            EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit => {
                hits[index] += 1;
            }
            EventResult::Miss | EventResult::Dup => misses[index] += 1,
            EventResult::Passthrough | EventResult::Skipped | EventResult::Error => {}
        }
    }
    (hits, misses)
}

/// A compact window label: `5m`, `2h`, `7d`.
fn fmt_window(window: Duration) -> String {
    let secs = window.as_secs();
    if secs >= 86_400 && secs.is_multiple_of(86_400) {
        format!("{}d", secs / 86_400)
    } else if secs >= 3600 && secs.is_multiple_of(3600) {
        format!("{}h", secs / 3600)
    } else if secs >= 60 && secs.is_multiple_of(60) {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

fn draw_sparkline(frame: &mut Frame, state: &AppState, area: Rect) {
    let window = fmt_window(state.spark_window);
    let block = Block::bordered().title(format!(" Lookups · last {window} "));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height < 3 || inner.width < 12 {
        return;
    }

    let label_width = 8u16;
    let columns = inner.width.saturating_sub(label_width) as usize;
    let (hits, misses) = lookup_series(
        &state.events,
        chrono::Utc::now(),
        state.spark_window,
        columns,
    );
    let total_hits: u64 = hits.iter().sum();
    let total_misses: u64 = misses.iter().sum();
    // One scale for both strips, so a hit column and a miss column of the
    // same height mean the same number of compiles.
    let max = hits
        .iter()
        .chain(&misses)
        .copied()
        .max()
        .unwrap_or(0)
        .max(1);

    let rows = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(inner);
    for (row, label, data, color) in [
        (rows[0], format!("hit {total_hits:>4}"), &hits, Color::Green),
        (
            rows[1],
            format!("miss{total_misses:>4}"),
            &misses,
            Color::Red,
        ),
    ] {
        let parts =
            Layout::horizontal([Constraint::Length(label_width), Constraint::Min(0)]).split(row);
        frame.render_widget(
            Paragraph::new(label).style(Style::default().fg(color)),
            parts[0],
        );
        frame.render_widget(
            Sparkline::default()
                .data(data)
                .max(max)
                .style(Style::default().fg(color)),
            parts[1],
        );
    }
    let axis = Layout::horizontal([
        Constraint::Length(label_width),
        Constraint::Min(0),
        Constraint::Length(6),
    ])
    .split(rows[2]);
    let idle = if total_hits + total_misses == 0 {
        format!("  ·  no lookups in the last {window}")
    } else {
        String::new()
    };
    let muted = Style::default().fg(Color::DarkGray);
    frame.render_widget(
        Paragraph::new(format!("{window} ago{idle}")).style(muted),
        axis[1],
    );
    frame.render_widget(
        Paragraph::new("now →").right_aligned().style(muted),
        axis[2],
    );
}

fn draw_build_help(frame: &mut Frame, state: &AppState, area: Rect) {
    // The full key list needs ~100 columns; narrower terminals get the
    // short form rather than a clipped one.
    let keys = if area.width >= 100 {
        "q: quit  p: pause  ↑↓: build  Enter: why  f: filter  PgUp PgDn End: events  ⇥/⇧⇥: tabs  c: clear"
    } else {
        "q: quit  p: pause  ↑↓: build  ⏎: why  f: filter  PgDn/End: events  ⇥: tabs  c: clear"
    };
    let help = help_line(state, keys);

    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Store tab ─────────────────────────────────────────────────────────────

fn draw_store_tab(frame: &mut Frame, state: &mut AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Min(5),    // Crates table (full height)
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    draw_store_table(frame, state, chunks[0]);
    draw_store_help(frame, state, chunks[1]);
}

fn draw_store_table(frame: &mut Frame, state: &mut AppState, area: Rect) {
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
        .title(filtered_title(&title, state.filter()))
        .border_style(Style::default().fg(Color::Cyan));

    let entries = &state.stats_snapshot.entries;

    let mut content_hash_counts: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for entry in entries {
        if let Some(ch) = &entry.content_hash {
            *content_hash_counts.entry(ch.as_str()).or_insert(0) += 1;
        }
    }

    // Dates go first when the terminal narrows, then type and profile; the
    // key, crate, size, hits, and dup columns are what the tab is for.
    let show_dates = area.width >= 110;
    let show_kind = area.width >= 84;

    let mut labels = vec!["Key", "Crate"];
    let mut widths = vec![Constraint::Length(13), Constraint::Min(18)];
    if show_kind {
        labels.extend(["Type", "Profile"]);
        widths.extend([Constraint::Length(10), Constraint::Length(10)]);
    }
    labels.extend(["Size", "Hits", "Dup"]);
    widths.extend([
        Constraint::Length(10),
        Constraint::Length(6),
        Constraint::Length(5),
    ]);
    if show_dates {
        labels.extend(["Created", "Accessed"]);
        widths.extend([Constraint::Length(12), Constraint::Length(12)]);
    }
    let header = Row::new(labels)
        .style(Style::default().add_modifier(Modifier::BOLD))
        .bottom_margin(0);

    let filter_empty = state.filter().is_empty();
    let filter_label = state.filter().to_string();
    let filtered: Vec<&daemon::StatsEntry> = entries
        .iter()
        .filter(|e| {
            filter_empty
                || e.crate_name.contains(&filter_label)
                || e.cache_key.contains(&filter_label)
        })
        .collect();

    let visible_rows = (area.height as usize).saturating_sub(3); // borders + header
    let range = state
        .store_scroll
        .visible_range(filtered.len(), visible_rows);

    let rows: Vec<Row> = filtered
        .iter()
        .skip(range.start)
        .take(range.len())
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
            let mut cells = vec![
                Cell::from(key_short.to_string()),
                Cell::from(entry.crate_name.clone()),
            ];
            if show_kind {
                cells.push(Cell::from(crate_type.to_string()));
                cells.push(Cell::from(profile.to_string()));
            }
            cells.push(Cell::from(ByteSize(entry.size).to_string()));
            cells.push(Cell::from(entry.hit_count.to_string()));
            cells.push(Cell::from(dup).style(Style::default().fg(Color::Yellow)));
            if show_dates {
                cells.push(Cell::from(
                    entry
                        .created_at
                        .get(..10)
                        .unwrap_or(&entry.created_at)
                        .to_string(),
                ));
                cells.push(Cell::from(
                    entry
                        .last_accessed
                        .get(..10)
                        .unwrap_or(&entry.last_accessed)
                        .to_string(),
                ));
            }
            Row::new(cells)
        })
        .collect();

    let table = Table::new(rows, widths).header(header).block(block);

    frame.render_widget(table, area);
}

fn draw_store_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let help = help_line(
        state,
        "q: quit  p: pause  s: sort  f: filter  ↑↓ PgUp PgDn: scroll  ⇥/⇧⇥: tabs",
    );

    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Projects tab ───────────────────────────────────────────────────────────

fn draw_projects_tab(frame: &mut Frame, state: &mut AppState, area: Rect) {
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
    draw_projects_help(frame, state, chunks[3]);
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

    // Blob-level content dedup is the only storage figure in the live header.
    // Clone reclamation belongs to `kache gc` and `kache clean`.
    let dedup_summary = if let Some(bs) = state.stats_snapshot.blob_stats.as_ref() {
        let pct = if bs.total_logical_size > 0 {
            bs.savings as f64 / bs.total_logical_size as f64 * 100.0
        } else {
            0.0
        };
        format!("{} saved ({:.1}%)", ByteSize(bs.savings), pct)
    } else {
        "calculating...".to_string()
    };

    let wrapper_status = crate::wrapper_config::wrapper_status_line();

    let remote_status = effective_remote_status(&state.config, snap);

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

fn draw_projects_table(frame: &mut Frame, state: &mut AppState, area: Rect) {
    let block = Block::bordered()
        .title(" Projects ")
        .border_style(Style::default().fg(Color::Cyan));

    let (item_count, scanning) = {
        let stats = state.project_scan.lock().unwrap();
        (stats.project_targets.len(), stats.scanning)
    };

    if item_count == 0 {
        let msg = if scanning {
            "  Scanning..."
        } else {
            "  No target/ directories found."
        };
        frame.render_widget(Paragraph::new(msg).block(block), area);
        return;
    }

    let visible_rows = (area.height as usize).saturating_sub(3); // borders + header
    let range = state.project_scroll.visible_range(item_count, visible_rows);

    // The per-category breakdown is detail; path, size, and cached bytes are
    // the answer. Narrow terminals keep the answer.
    let show_breakdown = area.width >= 100;
    let show_profile = area.width >= 80;

    let mut labels = vec!["Path", "Size", "Cached"];
    let mut widths = vec![
        Constraint::Min(20),
        Constraint::Length(9),
        Constraint::Length(9),
    ];
    if show_breakdown {
        labels.extend(["Incr", "Build", "Deps", "Bin", "Fprint"]);
        widths.extend([Constraint::Length(9); 5]);
    }
    if show_profile {
        labels.push("Profile");
        widths.push(Constraint::Length(14));
    }
    let header = Row::new(labels).style(Style::default().add_modifier(Modifier::BOLD));

    let root = std::env::current_dir().unwrap_or_default();

    let fmt = |v: u64| -> String {
        if v > 0 {
            format!("{:>8}", ByteSize(v))
        } else {
            String::new()
        }
    };

    let rows: Vec<Row> = {
        let stats = state.project_scan.lock().unwrap();
        stats
            .project_targets
            .iter()
            .skip(range.start)
            .take(range.len())
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
                let mut cells = vec![
                    Cell::from(path_label),
                    Cell::from(format!("{:>8}", ByteSize(t.size))),
                    Cell::from(format!("{:>8}", ByteSize(t.cached_bytes))),
                ];
                if show_breakdown {
                    cells.extend([
                        Cell::from(fmt(b.incremental)),
                        Cell::from(fmt(b.build_scripts)),
                        Cell::from(fmt(b.deps_local)),
                        Cell::from(fmt(b.binaries)),
                        Cell::from(fmt(b.fingerprints)),
                    ]);
                }
                if show_profile {
                    cells.push(Cell::from(profile_str));
                }
                Row::new(cells)
            })
            .collect()
    };

    let table = Table::new(rows, widths).header(header).block(block);
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

    let mut spans = vec![
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
    ];
    // Same threshold as the table above: the breakdown appears in both places
    // or neither.
    if area.width >= 100 {
        spans.extend([
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
    }
    let line = Line::from(spans);

    let block = Block::bordered().title(title);
    let paragraph = Paragraph::new(line).block(block);
    frame.render_widget(paragraph, area);
}

fn draw_projects_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let help = help_line(
        state,
        "q: quit  p: pause  r: refresh  ↑↓ PgUp PgDn: scroll  ⇥/⇧⇥: tabs",
    );

    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Transfer tab ──────────────────────────────────────────────────────────

fn draw_transfer_tab(frame: &mut Frame, state: &mut AppState, area: Rect) {
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
    draw_transfer_help(frame, state, chunks[3]);
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
            Span::raw(format!("    Remote slots: {s3_slots}")),
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

fn draw_recent_transfers(frame: &mut Frame, state: &mut AppState, area: Rect) {
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
    let range = state
        .transfer_scroll
        .visible_range(transfers.len(), visible_rows);

    // Show in reverse chronological order
    let rows: Vec<Row> = transfers
        .iter()
        .rev()
        .skip(range.start)
        .take(range.len())
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

fn draw_transfer_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let help = help_line(state, "q: quit  p: pause  ↑↓ PgUp PgDn: scroll  ⇥/⇧⇥: tabs");
    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

// ── Why tab ───────────────────────────────────────────────────────────────

/// Why the selected build missed: the misses collapsed to the few causes
/// behind them, the passthroughs by reason, the crates that keep missing,
/// and every passthrough in the build. One body, scrolled as a whole, so
/// nothing is ever clipped out of reach.
fn draw_why_tab(frame: &mut Frame, state: &mut AppState, area: Rect) {
    let chunks = Layout::vertical([
        Constraint::Min(3),    // Body
        Constraint::Length(1), // Help bar
    ])
    .split(area);

    let title = match state.selected_session() {
        Some(session) => format!(
            " Why · {} · {} ",
            session.workspace_name(),
            session.state.label()
        ),
        None => " Why ".to_string(),
    };
    let block = Block::bordered()
        .title(filtered_title(&title, state.filter()))
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(chunks[0]);
    let lines = why_lines(state, inner.width);
    let range = state
        .why_scroll
        .visible_range(lines.len(), inner.height as usize);
    let block = if range.end < lines.len() || range.start > 0 {
        block.title_bottom(
            Line::from(format!(
                " {}–{} of {} · PgUp PgDn ",
                range.start + 1,
                range.end,
                lines.len()
            ))
            .right_aligned(),
        )
    } else {
        block
    };
    frame.render_widget(
        Paragraph::new(lines)
            .scroll((range.start as u16, 0))
            .block(block),
        chunks[0],
    );
    draw_why_help(frame, state, chunks[1]);
}

/// A proportional bar of `width` cells, never empty for a non-zero share so
/// a rare cause still shows as present.
fn share_bar(count: usize, total: usize, width: usize) -> String {
    if total == 0 || width == 0 {
        return " ".repeat(width);
    }
    let filled = (count.saturating_mul(width) / total)
        .max(usize::from(count > 0))
        .min(width);
    format!("{}{}", "█".repeat(filled), "░".repeat(width - filled))
}

/// Terminal cells `text` occupies. Bytes and chars both lie for CJK and
/// combining marks; the same measure ratatui lays out with.
fn cells(text: &str) -> usize {
    Line::from(text).width()
}

/// Clip `text` to `width` cells with an ellipsis, on a character boundary.
fn clip(text: &str, width: usize) -> String {
    if cells(text) <= width {
        return text.to_string();
    }
    let mut kept = String::new();
    for ch in text.chars() {
        if cells(&format!("{kept}{ch}…")) > width {
            break;
        }
        kept.push(ch);
    }
    format!("{kept}…")
}

/// Pad `text` to `width` cells, clipping first if it is longer.
fn pad(text: &str, width: usize) -> String {
    let text = clip(text, width);
    let padding = width.saturating_sub(cells(&text));
    format!("{text}{}", " ".repeat(padding))
}

/// The Why body, as lines. Pure so it can be asserted on in tests without
/// a terminal. `width` is the inner width the lines are laid out for.
fn why_lines(state: &mut AppState, width: u16) -> Vec<Line<'static>> {
    let muted = Style::default().fg(Color::DarkGray);
    let heading = Style::default()
        .fg(Color::Cyan)
        .add_modifier(Modifier::BOLD);
    let explain_miss = state.config.explain_miss;
    let filter = state.filter().to_string();
    let Some(session) = state.selected_session().cloned() else {
        return vec![
            Line::from("  No builds recorded yet."),
            Line::styled(
                "  Run `cargo build` in any workspace and its misses are explained here.",
                muted,
            ),
        ];
    };
    // Fill the cache first, then borrow: the analysis is owned by the state.
    state.why_analysis();
    let Some((_, _, analysis)) = &state.why_cache else {
        return Vec::new();
    };
    let t = &session.tally;
    let width = width as usize;

    let mut lines = Vec::new();
    lines.push(Line::from(vec![
        Span::styled(
            format!("  {} ", session.workspace_name()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(
                "· {} · started {} · {} hits, {} misses, {} passthroughs",
                session.state.label(),
                session
                    .started
                    .with_timezone(&chrono::Local)
                    .format("%H:%M:%S"),
                t.hits,
                t.compiled(),
                t.passthroughs,
            ),
            muted,
        ),
    ]));
    let overhead = t.overhead_share().map_or_else(
        || fmt_saved_ms(t.overhead_ms),
        |share| {
            format!(
                "{} ({share:.0}% of lookup time)",
                fmt_saved_ms(t.overhead_ms)
            )
        },
    );
    let restored = match t.copy_share() {
        Some(share) if share >= 1.0 => format!(
            " · restored {}, {share:.0}% by copy",
            ByteSize(t.restored_bytes())
        ),
        Some(_) => format!(" · restored {}", ByteSize(t.restored_bytes())),
        None => String::new(),
    };
    lines.push(Line::from(vec![
        Span::raw("  saved "),
        Span::styled(fmt_saved_ms(t.saved_ms), Style::default().fg(Color::Green)),
        Span::raw(" · in misses "),
        Span::styled(fmt_saved_ms(t.miss_ms), Style::default().fg(Color::White)),
        Span::raw(format!(" · kache overhead {overhead}{restored}")),
    ]));

    // ── Misses by cause ──
    lines.push(Line::default());
    let capped = if analysis.misses_analyzed < analysis.misses_total {
        format!(
            " (newest {} of {} analyzed)",
            analysis.misses_analyzed, analysis.misses_total
        )
    } else {
        String::new()
    };
    lines.push(Line::styled(
        format!(
            "  Misses by cause · {} miss{}{capped}",
            analysis.misses_total,
            if analysis.misses_total == 1 { "" } else { "es" }
        ),
        heading,
    ));
    if analysis.misses_total == 0 {
        lines.push(Line::styled(
            if t.hits > 0 {
                "  none: every lookup hit"
            } else {
                "  none: nothing was looked up"
            },
            muted,
        ));
    }
    let bar_width = 12usize;
    let count_width = analysis
        .causes
        .iter()
        .chain(std::iter::empty())
        .map(|g| g.count.to_string().len())
        .chain(
            analysis
                .passthroughs
                .iter()
                .map(|g| g.count.to_string().len()),
        )
        .max()
        .unwrap_or(1);
    for group in &analysis.causes {
        let color = if group.cause.is_failure() {
            Color::Red
        } else if matches!(group.cause, Cause::Unexplained) {
            Color::DarkGray
        } else {
            Color::Yellow
        };
        let examples = if group.examples.is_empty() {
            String::new()
        } else {
            let more = group.count.saturating_sub(group.examples.len());
            format!(
                "{}{}",
                group.examples.join(", "),
                if more > 0 {
                    format!(" +{more}")
                } else {
                    String::new()
                }
            )
        };
        let prefix = format!(
            "  {} {:>count_width$}  ",
            share_bar(group.count, analysis.misses_analyzed, bar_width),
            group.count
        );
        let cost = format!("  {}", fmt_saved_ms(group.compile_ms));
        let room = width.saturating_sub(cells(&prefix) + cells(&cost) + 2);
        let describe = group.cause.describe();
        let describe_cells = cells(&describe);
        let (describe_width, example_width) = if room > describe_cells + 12 {
            (describe_cells, room - describe_cells - 2)
        } else {
            (room, 0)
        };
        let mut spans = vec![
            Span::styled(prefix, Style::default().fg(color)),
            Span::raw(clip(&describe, describe_width)),
        ];
        if example_width > 0 && !examples.is_empty() {
            spans.push(Span::styled(
                format!("  {}", clip(&examples, example_width)),
                muted,
            ));
        }
        spans.push(Span::styled(cost, muted));
        lines.push(Line::from(spans));
    }
    if analysis.misses_total > 0 && !analysis.cascade_recorded {
        lines.push(Line::styled(
            if explain_miss {
                "  no dependency digests on this build's misses; the next build records them (explain_miss is on)"
            } else {
                "  dependency cascades are not recorded: set [cache] explain_miss = true to name the crate that changed"
            },
            muted,
        ));
    }

    // ── Passthroughs by reason ──
    if !analysis.passthroughs.is_empty() {
        lines.push(Line::default());
        let total: usize = analysis.passthroughs.iter().map(|g| g.count).sum();
        lines.push(Line::styled(
            format!("  Passthroughs by reason · {total}"),
            heading,
        ));
        for group in &analysis.passthroughs {
            let label = if group.kind.is_empty() {
                group.reason.clone()
            } else {
                format!("{}: {}", group.kind, group.reason)
            };
            let note = if group.probe {
                "  (queries, not compiles)"
            } else {
                ""
            };
            let prefix = format!(
                "  {} {:>count_width$}  ",
                share_bar(group.count, total, bar_width),
                group.count
            );
            let room = width.saturating_sub(cells(&prefix) + cells(note) + 1);
            lines.push(Line::from(vec![
                Span::styled(
                    prefix,
                    Style::default().fg(if group.probe {
                        Color::DarkGray
                    } else {
                        Color::Magenta
                    }),
                ),
                Span::raw(clip(&label, room)),
                Span::styled(note, muted),
            ]));
        }
    }

    // ── Chronic ──
    if !analysis.chronic.is_empty() {
        lines.push(Line::default());
        lines.push(Line::styled(
            "  Chronic misses · missed in N of the M builds of this tree that looked it up",
            heading,
        ));
        for chronic in &analysis.chronic {
            lines.push(Line::from(vec![
                Span::raw(format!(
                    "  {} {}/{}",
                    pad(&chronic.crate_name, 28),
                    chronic.missed,
                    chronic.seen
                )),
                Span::styled(
                    if chronic.last_store_failed {
                        "  its latest lookup ended in a store failure"
                    } else {
                        ""
                    },
                    Style::default().fg(Color::Red),
                ),
            ]));
        }
    }

    // ── Every passthrough in the build ──
    let passthroughs: Vec<&BuildEvent> = session
        .events
        .iter()
        .map(|&index| &state.events[index])
        .filter(|event| matches!(event.result, EventResult::Passthrough))
        .filter(|event| {
            filter.is_empty()
                || event.crate_name.contains(&filter)
                || event.passthrough_reason.contains(&filter)
        })
        .collect();
    lines.push(Line::default());
    lines.push(Line::styled(
        format!(
            "  Passthroughs in this build · {}{}",
            passthroughs.len(),
            if filter.is_empty() {
                String::new()
            } else {
                format!(" matching {filter:?}")
            }
        ),
        heading,
    ));
    if passthroughs.is_empty() {
        lines.push(Line::styled(
            if filter.is_empty() {
                "  none"
            } else {
                "  none match the filter"
            },
            muted,
        ));
    }
    // Route and exit code are detail; on a narrow terminal the reason wins.
    let wide = width >= 96;
    // The crate and kind columns give up width before the reason does.
    let crate_width = (width / 4).clamp(8, 22);
    let kind_width = (width / 6).clamp(6, 14);
    for event in passthroughs.iter().rev() {
        let (kind, reason) = tui_sessions::passthrough_parts(&event.passthrough_reason);
        let mut spans = vec![
            Span::styled(
                format!(
                    "  {}  ",
                    event.ts.with_timezone(&chrono::Local).format("%H:%M:%S")
                ),
                muted,
            ),
            Span::raw(pad(&event.crate_name, crate_width)),
        ];
        if wide {
            let route = if event.fallback { "fallback" } else { "direct" };
            let (exit, exit_style) = match event.exit_code {
                Some(0) => ("0".to_string(), Style::default().fg(Color::Green)),
                Some(code) => (code.to_string(), Style::default().fg(Color::Red)),
                None => ("-".to_string(), muted),
            };
            spans.push(Span::styled(
                format!("  {route:<9}"),
                Style::default().fg(Color::Magenta),
            ));
            spans.push(Span::styled(format!("{exit:>4}  "), exit_style));
        } else {
            spans.push(Span::raw("  "));
        }
        let kind = if kind.is_empty() { "-" } else { kind };
        spans.push(Span::styled(
            pad(kind, kind_width),
            Style::default().fg(Color::Cyan),
        ));
        let used: usize = spans.iter().map(|span| cells(&span.content)).sum();
        spans.push(Span::raw(format!(
            "  {}",
            clip(reason, width.saturating_sub(used + 2))
        )));
        lines.push(Line::from(spans));
    }
    lines
}

fn draw_why_help(frame: &mut Frame, state: &AppState, area: Rect) {
    let help = help_line(
        state,
        "q: quit  p: pause  ↑↓: build  f: filter  PgUp PgDn: scroll  ⇥/⇧⇥: tabs",
    );
    let paragraph = Paragraph::new(help).style(Style::default().fg(Color::DarkGray));
    frame.render_widget(paragraph, area);
}

#[cfg(test)]
mod tests {
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
            volume_stores: Vec::new(),
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
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

    fn sample_build_event(
        crate_name: &str,
        result: events::EventResult,
        elapsed_ms: u64,
        size: u64,
    ) -> events::BuildEvent {
        events::BuildEvent {
            ts: chrono::Utc::now(),
            session_id: String::new(),
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
            lookup_rejection: String::new(),
            verify_compare: String::new(),
            fallback: false,
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
        let long = "unsupported|cc flag -march=native -mtune=native -fno-omit-frame-pointer -Wl,--as-needed";
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
            "not-a-compile|--print cfg with a long tail of arguments, and then some more"
                .to_string();
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
}
