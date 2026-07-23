use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// A single build event logged by the wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildEvent {
    pub ts: DateTime<Utc>,
    pub crate_name: String,
    /// Build tree/root this compiler invocation belongs to.
    ///
    /// For rustc this is the workspace root derived from Cargo's output
    /// layout. For cc-family compiles this is the best common source/build
    /// root kache can derive, falling back to the current directory.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root: String,
    #[serde(default)]
    pub version: String,
    pub result: EventResult,
    pub elapsed_ms: u64,
    /// Estimated compile cost for this invocation.
    ///
    /// Misses record the compile phase duration before cache-store work.
    /// Hits reuse the cached entry's stored compile cost when known.
    #[serde(default)]
    pub compile_time_ms: u64,
    pub size: u64,
    #[serde(default)]
    pub cache_key: String,
    /// Event schema version: 0 = legacy, 1 = prefetch-aware,
    /// 2 = compile-cost-aware, 3 = op-count-aware, 4 = probe-count-aware,
    /// 5 = passthrough details, 6 = file-hash cache metrics,
    /// 7 = restore-method bytes, 8 = dup outcome + store blob counters,
    /// 9 = event root, 10 = build session id (#583),
    /// 11 = key field hashes + miss key diff (#131).
    #[serde(default)]
    pub schema: u32,
    /// Build session this event belongs to (kunobi-ninja/kache#583 P0.5).
    ///
    /// Minted once per build by the wrapper that wins the session-marker
    /// lock (see `wrapper::build_session_id`) and read back by every other
    /// wrapper in the same build, so per-crate events can be joined with the
    /// daemon's per-plan prefetch summary. Empty = legacy wrapper or no
    /// session marker (never fails a build).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
    /// Cache key computation time (ms).
    #[serde(default)]
    pub key_ms: u64,
    /// File hashes served from the key-computation hash cache.
    #[serde(default)]
    pub key_hash_hits: u64,
    /// File hashes computed by reading file contents during key computation.
    #[serde(default)]
    pub key_hash_misses: u64,
    /// File bytes read and hashed during key computation.
    #[serde(default)]
    pub key_hash_bytes: u64,
    /// Store lookup time — SQLite query + meta read (ms).
    #[serde(default)]
    pub lookup_ms: u64,
    /// Restore from cache time — blob link/copy + mtime + depinfo + codesign (hits only, ms).
    #[serde(default)]
    pub restore_ms: u64,
    /// Store put time — tar + compress + dedup + SQLite (dup/miss only, ms).
    #[serde(default)]
    pub store_ms: u64,
    /// Unique output blobs handled by store put (compiled outcomes only).
    #[serde(default)]
    pub store_output_blobs: u32,
    /// Output blobs whose content hash already existed before store put.
    #[serde(default)]
    pub store_duplicate_blobs: u32,
    /// Output blobs whose content hash was new before store put.
    #[serde(default)]
    pub store_new_blobs: u32,
    /// Times kache spawned the underlying compiler for this build.
    /// 0 on a cache hit, 1 on a dup/miss. Deterministic — independent of
    /// machine speed — so the e2e harness can assert on it.
    #[serde(default)]
    pub compiler_runs: u32,
    /// Times kache spawned the preprocessor (`cc -E`) for this build —
    /// once per C/C++ compile to derive the cache key, 0 for rustc.
    #[serde(default)]
    pub preprocessor_runs: u32,
    /// Times kache spawned a compiler probe (`cc --version` / `cc -###`)
    /// for this build. Memoized on disk, so the first compile of a
    /// build records 1 and the rest record 0; a warm probe cache
    /// records 0.
    #[serde(default)]
    pub probe_runs: u32,
    /// Bytes restored from cache by a CoW reflink on a hit — physically
    /// zero-copy and write-isolated, kache's preferred restore path.
    #[serde(default)]
    pub reflinked_bytes: u64,
    /// Bytes restored by a hardlink — zero-copy via a shared inode, the
    /// fallback when the filesystem has no CoW (reflink) support.
    #[serde(default)]
    pub hardlinked_bytes: u64,
    /// Bytes restored by a full physical copy — the last-resort fallback
    /// when neither reflink nor hardlink is available.
    #[serde(default)]
    pub copied_bytes: u64,
    /// Bytes ingested into the store by a CoW reflink on a miss — the blob
    /// shares blocks with the build's own output file, so it costs ~no extra
    /// disk (APFS / btrfs / XFS-with-reflink).
    #[serde(default)]
    pub store_reflinked_bytes: u64,
    /// Bytes ingested into the store by a hardlink on a miss — the blob
    /// shares an inode with the build's own output, zero-copy on filesystems
    /// without CoW (immutable artifact kinds only).
    #[serde(default)]
    pub store_hardlinked_bytes: u64,
    /// Bytes ingested into the store by a full physical copy on a miss — a
    /// genuine second copy, the fallback when neither reflink nor hardlink
    /// is available.
    #[serde(default)]
    pub store_copied_bytes: u64,
    /// Why kache passed the invocation through instead of caching it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub passthrough_reason: String,
    /// Whether a configured fallback wrapper handled the passthrough.
    #[serde(default, skip_serializing_if = "is_false")]
    pub fallback: bool,
    /// Exit code from the passthrough command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    /// Per-group cache-key digests (kunobi-ninja/kache#131): 16-hex prefixes
    /// of each key input group (compiler, args, sources, env_deps, externs,
    /// link, env_cfg, remap, crate), computed as a tee of the exact bytes the
    /// final key hashes. Powers `explain_miss` — diffing two events' maps
    /// names which input group changed. Empty for cc compiles and
    /// passthroughs.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub key_fields: std::collections::BTreeMap<String, String>,
    /// On a miss with `[cache] explain_miss` on: the key groups whose digests
    /// changed vs this crate's last hit in the same build tree.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key_diff: Vec<String>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventResult {
    LocalHit,
    /// Local hit on an artifact that was downloaded by the manifest prefetch.
    PrefetchHit,
    RemoteHit,
    /// Entry miss; compiler ran; all output blobs already existed.
    Dup,
    Miss,
    Error,
    Passthrough,
    Skipped,
}

impl std::fmt::Display for EventResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventResult::LocalHit => write!(f, "local_hit"),
            EventResult::PrefetchHit => write!(f, "prefetch_hit"),
            EventResult::RemoteHit => write!(f, "remote_hit"),
            EventResult::Dup => write!(f, "dup"),
            EventResult::Miss => write!(f, "miss"),
            EventResult::Error => write!(f, "error"),
            EventResult::Passthrough => write!(f, "passthrough"),
            EventResult::Skipped => write!(f, "skipped"),
        }
    }
}

/// Per-session prefetch summary, appended to `summaries.jsonl` by the daemon
/// when a build session is finalized (kunobi-ninja/kache#583 P0.5).
///
/// Finalization happens on session inactivity, supersession by a newer
/// session, or daemon shutdown — cargo gives no positive end-of-build signal,
/// so closure is always inferred (`closure_reason` says how). Counts are kept
/// as raw numerators/denominators; consumers derive ratios (key precision =
/// `used_keys / downloaded_keys`, byte precision = `used_bytes /
/// downloaded_bytes`) so mixed-version or partial sessions stay auditable.
///
/// `used_keys`/`used_bytes` are a LOWER BOUND: a completed prefetch is
/// imported into the local store and consumed as a `LocalHit` without
/// contacting the daemon, so daemon-side demand misses it. Join per-crate
/// events by `session_id` + `cache_key` for full attribution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildSummaryEvent {
    pub ts: DateTime<Utc>,
    pub schema: u32,
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub root: String,
    /// `advisory` | `fallback` | `none` — which planner produced the plan.
    #[serde(default)]
    pub plan_source: String,
    #[serde(default)]
    pub plan_id: String,
    /// `inactivity` | `superseded` | `shutdown`.
    #[serde(default)]
    pub closure_reason: String,
    #[serde(default)]
    pub started_at_ms: u64,
    #[serde(default)]
    pub last_activity_ms: u64,
    /// Plan candidates offered for prefetch.
    #[serde(default)]
    pub candidate_keys: u64,
    /// Prefetch downloads completed (distinct keys / compressed wire bytes).
    #[serde(default)]
    pub downloaded_keys: u64,
    #[serde(default)]
    pub downloaded_bytes: u64,
    /// Daemon-visible consumption of downloaded keys (lower bound, see above).
    #[serde(default)]
    pub used_keys: u64,
    #[serde(default)]
    pub used_bytes: u64,
    /// Distinct keys demanded via RemoteCheck while the plan was active.
    #[serde(default)]
    pub demanded_keys: u64,
    /// Distinct demanded keys that were plan candidates.
    #[serde(default)]
    pub demanded_candidate_keys: u64,
    /// Whether adaptive cancellation fired for this plan.
    #[serde(default)]
    pub cancelled: bool,
    /// Key-cache LIST refreshes attributed to this session (delta of the
    /// daemon-lifetime counters between plan install and finalization).
    #[serde(default)]
    pub list_requests: u64,
    #[serde(default)]
    pub list_duration_ms: u64,
}

/// A liveness ping for an in-flight compile (kunobi-ninja/kache#131),
/// appended to the same `events.jsonl` stream by the wrapper's monitor thread
/// while a cache-miss compile runs. Gives non-TTY consumers (bench harnesses,
/// dashboards, CI parsers) the same "still compiling X" signal the stderr
/// heartbeat gives humans — TTY throttling (mach) can eat stderr, this can't.
///
/// Wire compatibility is load-bearing: pre-heartbeat readers per-line
/// try-parse `BuildEvent` and silently skip lines that fail, and `BuildEvent`
/// tolerates unknown fields — so an `event: "heartbeat"` tag alone would NOT
/// stop an old reader from mis-parsing this line. What does is omission: this
/// struct deliberately carries none of `result`/`elapsed_ms`/`size`, the
/// `BuildEvent` fields with no serde default, so old readers fail the parse
/// and skip the line. Conversely `event` has no default here, so a
/// `BuildEvent` line can never mis-parse as a heartbeat.
///
/// `schema` is the heartbeat record's OWN version lineage (starting at
/// [`HEARTBEAT_SCHEMA`] = 1), independent of `BuildEvent::schema`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatEvent {
    /// Always [`HEARTBEAT_EVENT_TAG`] — the discriminator for jsonl consumers.
    pub event: String,
    pub ts: DateTime<Utc>,
    pub crate_name: String,
    /// Build tree/root, same derivation as [`BuildEvent::root`].
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root: String,
    /// PID of the compiler child being waited on.
    pub pid: u32,
    /// Seconds since the compiler child was spawned.
    pub elapsed_s: u64,
    /// Median historical compile time for this crate (see
    /// [`typical_compile_ms`]), when history exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typical_s: Option<u64>,
    /// `typical_s - elapsed_s`, floored at zero — omitted with no history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_s: Option<u64>,
    #[serde(default)]
    pub schema: u32,
}

/// Discriminator value of [`HeartbeatEvent::event`].
pub const HEARTBEAT_EVENT_TAG: &str = "heartbeat";

/// Current [`HeartbeatEvent::schema`] version.
pub const HEARTBEAT_SCHEMA: u32 = 1;

/// One parsed line of the event log, for consumers that want the full mixed
/// stream (`kache monitor`). Existing [`BuildEvent`]-only readers keep their
/// narrow view and skip heartbeat lines by parse failure.
#[derive(Debug, Clone)]
pub enum EventRecord {
    // Boxed: BuildEvent is ~350 bytes vs the heartbeat's ~140 (clippy
    // large_enum_variant), and records are consumed one at a time.
    Build(Box<BuildEvent>),
    Heartbeat(HeartbeatEvent),
}

/// Parse one event-log line into whichever record type it is. Heartbeats are
/// tried first: a heartbeat line can never parse as a `BuildEvent` (missing
/// required fields), but the reverse must also never happen, which the
/// required `event` tag guarantees.
fn parse_event_line(line: &str) -> Option<EventRecord> {
    if let Ok(hb) = serde_json::from_str::<HeartbeatEvent>(line) {
        if hb.event == HEARTBEAT_EVENT_TAG {
            return Some(EventRecord::Heartbeat(hb));
        }
        return None;
    }
    serde_json::from_str::<BuildEvent>(line)
        .ok()
        .map(|e| EventRecord::Build(Box::new(e)))
}

/// Append one serialized JSON line under the exclusive sidecar lock — the
/// shared tail of [`log_event`] and [`log_heartbeat`], so every writer has the
/// same interleaving guarantee across concurrent wrapper processes.
fn append_log_line(event_log_path: &Path, line: String) -> Result<()> {
    if let Some(parent) = event_log_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let lock = open_log_lock(event_log_path).context("opening event log lock")?;
    lock.lock().context("locking event log")?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(event_log_path)
        .context("opening event log")?;

    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    let write_result = file.write_all(&bytes).context("writing event to log");
    lock.unlock().context("unlocking event log")?;
    write_result
}

/// Append a build event to the event log file.
/// Uses an exclusive sidecar file lock so concurrent wrapper processes cannot
/// interleave JSON lines.
pub fn log_event(event_log_path: &Path, event: &BuildEvent) -> Result<()> {
    append_log_line(
        event_log_path,
        serde_json::to_string(event).context("serializing event")?,
    )
}

/// Append a heartbeat line to the event log (kunobi-ninja/kache#131).
pub fn log_heartbeat(event_log_path: &Path, event: &HeartbeatEvent) -> Result<()> {
    append_log_line(
        event_log_path,
        serde_json::to_string(event).context("serializing heartbeat")?,
    )
}

/// Number of most-recent samples the typical-time median is computed over.
const TYPICAL_WINDOW: usize = 20;

/// Median compile cost (ms) for `crate_name` from the recent event log — the
/// "typical: 7m51s" / ETA input for heartbeats (kunobi-ninja/kache#131).
///
/// One full log read under a shared lock. Callers invoke this lazily on the
/// FIRST heartbeat tick, never at spawn: only compiles already running longer
/// than one cadence (default 30 s) pay the read, so a cold build with hundreds
/// of fast misses performs no scans at all. Uses the last [`TYPICAL_WINDOW`]
/// events with a recorded compile cost for the crate, dropping samples more
/// than 3σ from the window median (a one-off `-j1` or thermally-throttled
/// build must not wreck the estimate).
pub fn typical_compile_ms(event_log_path: &Path, crate_name: &str) -> Option<u64> {
    let events = read_events(event_log_path).ok()?;
    let samples: Vec<u64> = events
        .iter()
        .filter(|e| e.crate_name == crate_name && e.compile_time_ms > 0)
        .map(|e| e.compile_time_ms)
        .collect();
    let window = &samples[samples.len().saturating_sub(TYPICAL_WINDOW)..];
    let center = median(window)?;
    let n = window.len() as f64;
    let mean = window.iter().sum::<u64>() as f64 / n;
    let sigma = (window
        .iter()
        .map(|&s| {
            let d = s as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / n)
        .sqrt();
    let kept: Vec<u64> = window
        .iter()
        .copied()
        .filter(|&s| sigma == 0.0 || (s as f64 - center as f64).abs() <= 3.0 * sigma)
        .collect();
    median(&kept)
}

/// Median of a non-empty slice (`None` when empty). Even-length slices take
/// the lower-middle element — stability over precision for ETA display.
fn median(samples: &[u64]) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Some(sorted[(sorted.len() - 1) / 2])
}

/// Read all events from the event log.
pub fn read_events(event_log_path: &Path) -> Result<Vec<BuildEvent>> {
    if !event_log_path.exists() {
        return Ok(Vec::new());
    }

    let lock = open_log_lock(event_log_path).context("opening event log lock")?;
    lock.lock_shared().context("locking event log for read")?;

    let file = File::open(event_log_path).context("opening event log")?;
    let reader = BufReader::new(&file);
    let mut events = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<BuildEvent>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::debug!("skipping invalid event line: {}", e);
            }
        }
    }

    lock.unlock().context("unlocking event log")?;
    Ok(events)
}

/// Read events since a given timestamp.
pub fn read_events_since(event_log_path: &Path, since: DateTime<Utc>) -> Result<Vec<BuildEvent>> {
    let all = read_events(event_log_path)?;
    Ok(all.into_iter().filter(|e| e.ts >= since).collect())
}

#[cfg(windows)]
fn get_file_identity(file: &std::fs::File) -> Option<(u32, u32, u32)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };
    let handle = file.as_raw_handle();
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let ok = unsafe { GetFileInformationByHandle(handle as _, &mut info) };
    if ok != 0 {
        Some((
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ))
    } else {
        None
    }
}

/// Tail the event log, returning new events since the last known position.
pub struct EventTailer {
    path: PathBuf,
    position: u64,
    file: Option<File>,
}

impl EventTailer {
    pub fn new(path: PathBuf) -> Self {
        let file = File::open(&path).ok();
        let position = file
            .as_ref()
            .and_then(|file| file.metadata().ok())
            .map(|m| m.len())
            .unwrap_or(0);
        EventTailer {
            path,
            position,
            file,
        }
    }

    /// Start from the beginning.
    pub fn from_start(path: PathBuf) -> Self {
        let file = File::open(&path).ok();
        EventTailer {
            path,
            position: 0,
            file,
        }
    }

    /// Read new build events since last poll. Note that log rotation is lossy
    /// and may discard events that were never polled. Heartbeat lines are
    /// skipped — use [`EventTailer::poll_records`] for the mixed stream (all
    /// live consumers do; this narrow view is kept for the rotation/truncation
    /// tests and future builds-only consumers).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn poll(&mut self) -> Result<Vec<BuildEvent>> {
        Ok(self
            .poll_records()?
            .into_iter()
            .filter_map(|r| match r {
                EventRecord::Build(e) => Some(*e),
                EventRecord::Heartbeat(_) => None,
            })
            .collect())
    }

    /// Read all new records since last poll — build events AND heartbeats
    /// (kunobi-ninja/kache#131), for consumers like `kache monitor` that
    /// render in-flight compiles.
    pub fn poll_records(&mut self) -> Result<Vec<EventRecord>> {
        if self.file.is_none() {
            match File::open(&self.path) {
                Ok(file) => self.file = Some(file),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(e) => return Err(e.into()),
            }
        }

        let mut rotated = false;
        if let Some(file) = &self.file {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if let Ok(m1) = file.metadata()
                    && let Ok(m2) = std::fs::metadata(&self.path)
                    && (m1.dev() != m2.dev() || m1.ino() != m2.ino())
                {
                    rotated = true;
                }
            }
            #[cfg(windows)]
            {
                if let Some(id1) = get_file_identity(file)
                    && let Ok(f2) = std::fs::File::open(&self.path)
                    && let Some(id2) = get_file_identity(&f2)
                    && id1 != id2
                {
                    rotated = true;
                }
            }
        }

        if rotated {
            match File::open(&self.path) {
                Ok(file) => {
                    self.file = Some(file);
                    self.position = 0;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.file = None;
                    self.position = 0;
                    return Ok(Vec::new());
                }
                Err(e) => return Err(e.into()),
            }
        }

        let file = self.file.as_mut().unwrap();
        let file_len = file.metadata()?.len();

        if file_len < self.position {
            // File was truncated (log rotation), start from beginning
            self.position = 0;
        }

        if file_len <= self.position {
            return Ok(Vec::new());
        }

        file.seek(SeekFrom::Start(self.position))?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();
        let mut bytes_read = 0u64;

        for line in reader.lines() {
            let line = line?;
            bytes_read += line.len() as u64 + 1; // +1 for newline
            if line.trim().is_empty() {
                continue;
            }
            if let Some(record) = parse_event_line(&line) {
                records.push(record);
            }
        }

        self.position += bytes_read;
        Ok(records)
    }
}

/// Rotate the event log if it exceeds the max size.
/// Keeps the last `keep_lines` lines.
fn rotate_log_impl(
    log_path: &Path,
    max_size: u64,
    keep_lines: usize,
    log_label: &str,
) -> Result<()> {
    if !log_path.exists() {
        return Ok(());
    }

    // Lock-free stat gate to keep the hot path cheap when rotation is not needed.
    if let Ok(meta) = fs::metadata(log_path)
        && meta.len() <= max_size
    {
        return Ok(());
    }

    // 1. Acquire the lock before querying metadata or reading
    let lock = open_log_lock(log_path).context("opening log lock")?;
    lock.lock().context("locking log for rotation")?;

    let res = (|| -> Result<()> {
        let meta = fs::metadata(log_path)?;
        if meta.len() <= max_size {
            return Ok(());
        }

        // Clean up stale temp files in the same directory (older than 5 minutes)
        if let Some(parent) = log_path.parent()
            && let Some(file_prefix) = log_path.file_name().and_then(|n| n.to_str())
        {
            let _ = crate::atomic::cleanup_temp_files(
                parent,
                file_prefix,
                std::time::Duration::from_secs(300),
            );
        }

        let content = fs::read_to_string(log_path)?;
        let lines: Vec<&str> = content.lines().collect();
        let keep_from = lines.len().saturating_sub(keep_lines);
        let mut kept: Vec<&str> = lines[keep_from..].to_vec();

        // Size-cap re-check: trim additional lines from the beginning if total size still exceeds max_size
        let mut total_bytes: u64 = kept.iter().map(|line| line.len() as u64 + 1).sum();
        while total_bytes > max_size && kept.len() > 1 {
            let removed = kept.remove(0);
            total_bytes -= (removed.len() + 1) as u64;
        }

        let output = kept.join("\n") + "\n";
        crate::atomic::atomic_replace(log_path, output.as_bytes())
            .context("writing and replacing log file atomically")?;

        tracing::info!(
            "rotated {}: kept {} of {} lines",
            log_label,
            kept.len(),
            lines.len()
        );
        Ok(())
    })();

    let _ = lock.unlock();
    res
}

/// Rotate the event log if it exceeds the max size.
/// Keeps the last `keep_lines` lines.
pub fn rotate_if_needed(event_log_path: &Path, max_size: u64, keep_lines: usize) -> Result<()> {
    rotate_log_impl(event_log_path, max_size, keep_lines, "event log")
}

// ── Summary log ─────────────────────────────────────────────────────────────

/// Append a per-session build summary to the summary log (`summaries.jsonl`).
/// Own file rather than `events.jsonl` so `read_events` never has to skip
/// foreign lines; same locking discipline as the other logs.
pub fn log_summary(summary_log_path: &Path, event: &BuildSummaryEvent) -> Result<()> {
    if let Some(parent) = summary_log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let lock = open_log_lock(summary_log_path).context("opening summary log lock")?;
    lock.lock().context("locking summary log")?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(summary_log_path)
        .context("opening summary log")?;
    let line = serde_json::to_string(event).context("serializing summary event")?;
    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    file.write_all(&bytes)
        .context("writing summary event to log")?;
    lock.unlock().context("unlocking summary log")?;
    Ok(())
}

/// Read all build summaries from the summary log. Invalid lines are skipped
/// (schema evolution / partial writes must not poison reporting).
pub fn read_summaries(summary_log_path: &Path) -> Result<Vec<BuildSummaryEvent>> {
    if !summary_log_path.exists() {
        return Ok(Vec::new());
    }
    let lock = open_log_lock(summary_log_path).context("opening summary log lock")?;
    lock.lock_shared().context("locking summary log for read")?;

    let file = File::open(summary_log_path).context("opening summary log")?;
    let reader = BufReader::new(&file);
    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<BuildSummaryEvent>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::debug!("skipping invalid summary line: {}", e);
            }
        }
    }

    lock.unlock().context("unlocking summary log")?;
    Ok(events)
}

// ── Transfer log ────────────────────────────────────────────────────────────

use crate::daemon::TransferEvent;

/// Append a transfer event to the transfer log file.
pub fn log_transfer(transfer_log_path: &Path, event: &TransferEvent) -> Result<()> {
    if let Some(parent) = transfer_log_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let lock = open_log_lock(transfer_log_path).context("opening transfer log lock")?;
    lock.lock().context("locking transfer log")?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(transfer_log_path)
        .context("opening transfer log")?;
    let line = serde_json::to_string(event).context("serializing transfer event")?;
    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    file.write_all(&bytes)
        .context("writing transfer event to log")?;
    lock.unlock().context("unlocking transfer log")?;
    Ok(())
}

/// Read all transfer events from the transfer log.
pub fn read_transfers(transfer_log_path: &Path) -> Result<Vec<TransferEvent>> {
    if !transfer_log_path.exists() {
        return Ok(Vec::new());
    }
    let lock = open_log_lock(transfer_log_path).context("opening transfer log lock")?;
    lock.lock_shared()
        .context("locking transfer log for read")?;

    let file = File::open(transfer_log_path).context("opening transfer log")?;
    let reader = BufReader::new(&file);
    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<TransferEvent>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                tracing::debug!("skipping invalid transfer line: {}", e);
            }
        }
    }
    lock.unlock().context("unlocking transfer log")?;
    Ok(events)
}

fn open_log_lock(log_path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(sidecar_lock_path(log_path))
        .context("opening log lock")
}

fn sidecar_lock_path(log_path: &Path) -> PathBuf {
    let mut path = log_path.as_os_str().to_owned();
    path.push(".lock");
    PathBuf::from(path)
}

/// Read transfer events since a given unix timestamp (seconds).
pub fn read_transfers_since(transfer_log_path: &Path, since_ts: u64) -> Result<Vec<TransferEvent>> {
    let all = read_transfers(transfer_log_path)?;
    Ok(all
        .into_iter()
        .filter(|e| e.timestamp >= since_ts)
        .collect())
}

/// Rotate the transfer log if it exceeds the max size.
/// Keeps the last `keep_lines` lines.
pub fn rotate_transfers_if_needed(
    transfer_log_path: &Path,
    max_size: u64,
    keep_lines: usize,
) -> Result<()> {
    rotate_log_impl(transfer_log_path, max_size, keep_lines, "transfer log")
}

/// Clear the event log.
#[allow(dead_code)]
pub fn clear_events(event_log_path: &Path) -> Result<()> {
    if !event_log_path.exists() {
        return Ok(());
    }
    let lock = open_log_lock(event_log_path).context("opening event log lock")?;
    lock.lock().context("locking event log for clearing")?;
    let res = fs::write(event_log_path, "");
    let _ = lock.unlock();
    res.context("clearing event log")
}

/// Get event statistics.
pub struct EventStats {
    #[allow(dead_code)]
    pub total: usize,
    pub local_hits: usize,
    pub prefetch_hits: usize,
    pub remote_hits: usize,
    pub dups: usize,
    pub misses: usize,
    pub errors: usize,
    pub total_size: u64,
    pub total_elapsed_ms: u64,
    pub hit_elapsed_ms: u64,
    pub miss_elapsed_ms: u64,
    pub hit_compile_time_ms: u64,
    pub miss_compile_time_ms: u64,
    pub total_key_ms: u64,
    pub total_lookup_ms: u64,
    pub total_restore_ms: u64,
    pub total_store_ms: u64,
    pub store_output_blobs: u32,
    pub store_duplicate_blobs: u32,
    pub store_new_blobs: u32,
    /// Bytes restored from cache by CoW reflink (physically zero-copy).
    pub reflinked_bytes: u64,
    /// Bytes restored by hardlink (zero-copy via a shared inode).
    pub hardlinked_bytes: u64,
    /// Bytes restored by a full physical copy.
    pub copied_bytes: u64,
    /// Bytes ingested into the store by a CoW reflink (shares blocks with the
    /// build's output — not a second physical copy).
    pub store_reflinked_bytes: u64,
    /// Bytes ingested into the store by a hardlink (shares an inode with the
    /// build's output — not a second physical copy).
    pub store_hardlinked_bytes: u64,
    /// Bytes ingested into the store by a full physical copy (a real second copy).
    pub store_copied_bytes: u64,
}

pub fn compute_stats(events: &[BuildEvent]) -> EventStats {
    let mut stats = EventStats {
        total: events.len(),
        local_hits: 0,
        prefetch_hits: 0,
        remote_hits: 0,
        dups: 0,
        misses: 0,
        errors: 0,
        total_size: 0,
        total_elapsed_ms: 0,
        hit_elapsed_ms: 0,
        miss_elapsed_ms: 0,
        hit_compile_time_ms: 0,
        miss_compile_time_ms: 0,
        total_key_ms: 0,
        total_lookup_ms: 0,
        total_restore_ms: 0,
        total_store_ms: 0,
        store_output_blobs: 0,
        store_duplicate_blobs: 0,
        store_new_blobs: 0,
        reflinked_bytes: 0,
        hardlinked_bytes: 0,
        copied_bytes: 0,
        store_reflinked_bytes: 0,
        store_hardlinked_bytes: 0,
        store_copied_bytes: 0,
    };

    for event in events {
        match event.result {
            EventResult::LocalHit => {
                stats.local_hits += 1;
                stats.hit_elapsed_ms += event.elapsed_ms;
                stats.hit_compile_time_ms += event.compile_time_ms;
            }
            EventResult::PrefetchHit => {
                stats.prefetch_hits += 1;
                stats.hit_elapsed_ms += event.elapsed_ms;
                stats.hit_compile_time_ms += event.compile_time_ms;
            }
            EventResult::RemoteHit => {
                stats.remote_hits += 1;
                stats.hit_elapsed_ms += event.elapsed_ms;
                stats.hit_compile_time_ms += event.compile_time_ms;
            }
            EventResult::Dup => {
                stats.dups += 1;
                stats.miss_elapsed_ms += event.elapsed_ms;
                stats.miss_compile_time_ms += if event.compile_time_ms > 0 {
                    event.compile_time_ms
                } else {
                    event.elapsed_ms
                };
            }
            EventResult::Miss => {
                stats.misses += 1;
                stats.miss_elapsed_ms += event.elapsed_ms;
                stats.miss_compile_time_ms += if event.compile_time_ms > 0 {
                    event.compile_time_ms
                } else {
                    event.elapsed_ms
                };
            }
            EventResult::Error => stats.errors += 1,
            EventResult::Passthrough | EventResult::Skipped => continue,
        }
        stats.total_size += event.size;
        stats.total_elapsed_ms += event.elapsed_ms;
        stats.total_key_ms += event.key_ms;
        stats.total_lookup_ms += event.lookup_ms;
        stats.total_restore_ms += event.restore_ms;
        stats.total_store_ms += event.store_ms;
        stats.store_output_blobs += event.store_output_blobs;
        stats.store_duplicate_blobs += event.store_duplicate_blobs;
        stats.store_new_blobs += event.store_new_blobs;
        stats.reflinked_bytes += event.reflinked_bytes;
        stats.hardlinked_bytes += event.hardlinked_bytes;
        stats.copied_bytes += event.copied_bytes;
        stats.store_reflinked_bytes += event.store_reflinked_bytes;
        stats.store_hardlinked_bytes += event.store_hardlinked_bytes;
        stats.store_copied_bytes += event.store_copied_bytes;
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::*;

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
            crate_name: crate_name.to_string(),
            root: String::new(),
            version: "0.0.0".to_string(),
            session_id: String::new(),
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
            passthrough_reason: String::new(),
            fallback: false,
            exit_code: None,
            key_fields: Default::default(),
            key_diff: Vec::new(),
        }
    }

    #[test]
    fn test_log_and_read_events() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = BuildEvent {
            ts: Utc::now(),
            crate_name: "serde".to_string(),
            root: "/work/tree".to_string(),
            version: "1.0.210".to_string(),
            session_id: String::new(),
            result: EventResult::LocalHit,
            elapsed_ms: 2,
            compile_time_ms: 250,
            size: 3145728,
            cache_key: "abc123".to_string(),
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
            store_hardlinked_bytes: 0,
            store_copied_bytes: 0,
            passthrough_reason: String::new(),
            fallback: false,
            exit_code: None,
            key_fields: Default::default(),
            key_diff: Vec::new(),
        };

        log_event(&log_path, &event).unwrap();
        log_event(&log_path, &event).unwrap();

        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].crate_name, "serde");
        assert_eq!(events[0].result, EventResult::LocalHit);
    }

    fn test_heartbeat(crate_name: &str, elapsed_s: u64) -> HeartbeatEvent {
        HeartbeatEvent {
            event: HEARTBEAT_EVENT_TAG.to_string(),
            ts: Utc::now(),
            crate_name: crate_name.to_string(),
            root: "/work/tree".to_string(),
            pid: 4242,
            elapsed_s,
            typical_s: Some(471),
            eta_s: Some(211),
            schema: HEARTBEAT_SCHEMA,
        }
    }

    /// #131 wire-compat invariant: a heartbeat line must be INVISIBLE to
    /// BuildEvent-only readers (skipped by parse failure, never mis-parsed as
    /// a degenerate BuildEvent) — and a BuildEvent line must never parse as a
    /// heartbeat.
    #[test]
    fn heartbeat_lines_are_invisible_to_build_event_readers() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        log_event(
            &log_path,
            &test_event("gkrust", EventResult::Miss, 471_000, 471_000, 1024, "k1"),
        )
        .unwrap();
        log_heartbeat(&log_path, &test_heartbeat("gkrust", 260)).unwrap();
        log_event(
            &log_path,
            &test_event("serde", EventResult::LocalHit, 2, 250, 64, "k2"),
        )
        .unwrap();

        let builds = read_events(&log_path).unwrap();
        assert_eq!(
            builds.len(),
            2,
            "BuildEvent readers must skip the heartbeat line"
        );

        let hb_line = serde_json::to_string(&test_heartbeat("gkrust", 260)).unwrap();
        assert!(
            serde_json::from_str::<BuildEvent>(&hb_line).is_err(),
            "heartbeat must not deserialize as BuildEvent"
        );
        let build_line =
            serde_json::to_string(&test_event("gkrust", EventResult::Miss, 1, 1, 1, "k")).unwrap();
        assert!(
            serde_json::from_str::<HeartbeatEvent>(&build_line).is_err(),
            "BuildEvent must not deserialize as heartbeat"
        );
    }

    /// #131: the mixed-stream tailer yields both record kinds in order, while
    /// the legacy `poll` keeps its builds-only view.
    #[test]
    fn tailer_poll_records_yields_heartbeats_and_builds() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");
        let mut tailer = EventTailer::from_start(log_path.clone());

        log_heartbeat(&log_path, &test_heartbeat("gkrust", 30)).unwrap();
        log_event(
            &log_path,
            &test_event("gkrust", EventResult::Miss, 60_000, 60_000, 1024, "k1"),
        )
        .unwrap();

        let records = tailer.poll_records().unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(&records[0], EventRecord::Heartbeat(h) if h.elapsed_s == 30));
        assert!(matches!(&records[1], EventRecord::Build(e) if e.result == EventResult::Miss));

        log_heartbeat(&log_path, &test_heartbeat("gkrust", 60)).unwrap();
        assert!(
            tailer.poll().unwrap().is_empty(),
            "builds-only poll must skip a heartbeat-only append"
        );
    }

    /// #131 ETA source: median over the recent window for the crate, ignoring
    /// other crates, zero compile costs, and extreme outliers.
    #[test]
    fn typical_compile_ms_is_a_robust_per_crate_median() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        assert_eq!(
            typical_compile_ms(&log_path, "gkrust"),
            None,
            "no history → no estimate"
        );

        // Tight cluster + one extreme outlier + noise from other crates and
        // hit events with no compile cost recorded.
        for ms in [100_000u64, 101_000, 99_000, 100_500, 100_200] {
            log_event(
                &log_path,
                &test_event("gkrust", EventResult::Miss, ms, ms, 1024, "k"),
            )
            .unwrap();
        }
        log_event(
            &log_path,
            &test_event("gkrust", EventResult::Miss, 900_000, 900_000, 1024, "k"),
        )
        .unwrap();
        log_event(
            &log_path,
            &test_event("serde", EventResult::Miss, 2_000, 2_000, 64, "k"),
        )
        .unwrap();
        log_event(
            &log_path,
            &test_event("gkrust", EventResult::LocalHit, 5, 0, 1024, "k"),
        )
        .unwrap();

        let typical = typical_compile_ms(&log_path, "gkrust").unwrap();
        assert!(
            (99_000..=101_000).contains(&typical),
            "median must sit in the cluster and shed the 900s outlier, got {typical}"
        );

        assert_eq!(typical_compile_ms(&log_path, "serde"), Some(2_000));
    }

    #[test]
    fn test_event_tailer() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let mut tailer = EventTailer::from_start(log_path.clone());

        // No file yet
        assert_eq!(tailer.poll().unwrap().len(), 0);

        // Write an event
        let event = test_event("tokio", EventResult::Miss, 5000, 4800, 8388608, "def456");
        log_event(&log_path, &event).unwrap();

        // Should read the new event
        let new_events = tailer.poll().unwrap();
        assert_eq!(new_events.len(), 1);

        // No new events
        assert_eq!(tailer.poll().unwrap().len(), 0);

        // Write another
        log_event(&log_path, &event).unwrap();
        let new_events = tailer.poll().unwrap();
        assert_eq!(new_events.len(), 1);
    }

    #[test]
    fn test_event_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        // Write many events
        for i in 0..100 {
            let event = test_event(
                &format!("crate_{i}"),
                EventResult::LocalHit,
                1,
                25,
                1024,
                &format!("key_{i}"),
            );
            log_event(&log_path, &event).unwrap();
        }

        // Rotate with small max size (10000 bytes), keep 10 lines
        rotate_if_needed(&log_path, 10000, 10).unwrap();

        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 10);
        // Should keep the last 10
        assert_eq!(events[0].crate_name, "crate_90");

        // Now, if we rotate with a very small max size (e.g. 200 bytes), it should trim down to 1 line
        // because 2 lines would exceed 200 bytes.
        rotate_if_needed(&log_path, 200, 10).unwrap();
        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].crate_name, "crate_99");
    }

    #[test]
    fn test_event_result_display() {
        assert_eq!(EventResult::LocalHit.to_string(), "local_hit");
        assert_eq!(EventResult::PrefetchHit.to_string(), "prefetch_hit");
        assert_eq!(EventResult::RemoteHit.to_string(), "remote_hit");
        assert_eq!(EventResult::Dup.to_string(), "dup");
        assert_eq!(EventResult::Miss.to_string(), "miss");
        assert_eq!(EventResult::Error.to_string(), "error");
        assert_eq!(EventResult::Passthrough.to_string(), "passthrough");
        assert_eq!(EventResult::Skipped.to_string(), "skipped");
    }

    #[test]
    fn test_read_events_nonexistent_file() {
        let events = read_events(Path::new("/nonexistent/events.jsonl")).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn test_read_events_with_invalid_lines() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("valid", EventResult::Miss, 100, 90, 1024, "key");
        log_event(&log_path, &event).unwrap();

        // Append invalid JSON
        use std::io::Write;
        let mut f = OpenOptions::new().append(true).open(&log_path).unwrap();
        writeln!(f, "this is not json").unwrap();
        writeln!(f, "{{}}").unwrap(); // valid JSON but missing fields

        let events = read_events(&log_path).unwrap();
        // Only the first valid event should be parsed
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].crate_name, "valid");
    }

    #[test]
    fn test_read_events_since() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let mut old_event = test_event("old", EventResult::Miss, 100, 80, 1024, "key1");
        old_event.ts = Utc::now() - chrono::Duration::hours(2);
        let new_event = test_event("new", EventResult::LocalHit, 10, 250, 512, "key2");

        log_event(&log_path, &old_event).unwrap();
        log_event(&log_path, &new_event).unwrap();

        let since = Utc::now() - chrono::Duration::hours(1);
        let events = read_events_since(&log_path, since).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].crate_name, "new");
    }

    #[test]
    fn test_compute_stats() {
        let events = vec![
            test_event("a", EventResult::LocalHit, 10, 300, 100, "k1"),
            test_event("b", EventResult::PrefetchHit, 5, 250, 150, "k1b"),
            test_event("c", EventResult::RemoteHit, 50, 900, 200, "k2"),
            test_event("dup", EventResult::Dup, 700, 650, 400, "kdup"),
            test_event("d", EventResult::Miss, 1000, 950, 500, "k3"),
            test_event("e", EventResult::Error, 5, 0, 0, "k4"),
            test_event("f", EventResult::Skipped, 0, 0, 0, "k5"),
            test_event("g", EventResult::Passthrough, 25, 0, 0, ""),
        ];

        let stats = compute_stats(&events);
        assert_eq!(stats.total, 8);
        assert_eq!(stats.local_hits, 1);
        assert_eq!(stats.prefetch_hits, 1);
        assert_eq!(stats.remote_hits, 1);
        assert_eq!(stats.dups, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.total_size, 1350);
        assert_eq!(stats.total_elapsed_ms, 1770);
        assert_eq!(stats.hit_elapsed_ms, 65);
        assert_eq!(stats.miss_elapsed_ms, 1700);
        assert_eq!(stats.hit_compile_time_ms, 1450);
        assert_eq!(stats.miss_compile_time_ms, 1600);
    }

    #[test]
    fn test_compute_stats_empty() {
        let stats = compute_stats(&[]);
        assert_eq!(stats.total, 0);
        assert_eq!(stats.local_hits, 0);
    }

    #[test]
    fn test_clear_events() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");
        log_event(&log_path, &event).unwrap();

        assert!(!read_events(&log_path).unwrap().is_empty());
        clear_events(&log_path).unwrap();
        assert!(read_events(&log_path).unwrap().is_empty());
    }

    #[test]
    fn test_clear_events_nonexistent() {
        clear_events(Path::new("/nonexistent/events.jsonl")).unwrap();
    }

    #[test]
    fn test_rotate_skips_small_file() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");
        log_event(&log_path, &event).unwrap();

        let size_before = fs::metadata(&log_path).unwrap().len();
        // max_size is larger than the file — should not rotate
        rotate_if_needed(&log_path, 1_000_000, 10).unwrap();
        let size_after = fs::metadata(&log_path).unwrap().len();
        assert_eq!(size_before, size_after);
    }

    #[test]
    fn test_rotate_nonexistent() {
        rotate_if_needed(Path::new("/nonexistent/events.jsonl"), 100, 10).unwrap();
    }

    #[test]
    fn test_rotate_transfers_trims_to_keep_lines_when_oversized() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("transfers.jsonl");
        // 100 lines, well over a 100-byte cap.
        let body: String = (0..100).map(|i| format!("line {i}\n")).collect();
        fs::write(&log_path, body).unwrap();

        rotate_transfers_if_needed(&log_path, 100, 10).unwrap();

        let kept = fs::read_to_string(&log_path).unwrap();
        let lines: Vec<&str> = kept.lines().collect();
        assert_eq!(lines.len(), 10, "should keep the last 10 lines");
        assert_eq!(lines[0], "line 90", "keeps the tail");
        assert_eq!(lines[9], "line 99");
    }

    #[test]
    fn test_rotate_transfers_skips_small_and_nonexistent() {
        // Nonexistent file: no-op.
        rotate_transfers_if_needed(Path::new("/nonexistent/transfers.jsonl"), 100, 10).unwrap();
        // Under the cap: left untouched.
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("transfers.jsonl");
        fs::write(&log_path, "a\nb\n").unwrap();
        rotate_transfers_if_needed(&log_path, 1_000_000, 1).unwrap();
        assert_eq!(fs::read_to_string(&log_path).unwrap(), "a\nb\n");
    }

    #[test]
    fn test_event_tailer_handles_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");

        // Write several events and advance tailer position
        for _ in 0..10 {
            log_event(&log_path, &event).unwrap();
        }
        let mut tailer = EventTailer::from_start(log_path.clone());
        assert_eq!(tailer.poll().unwrap().len(), 10);

        // Truncate (simulate rotation)
        fs::write(&log_path, "").unwrap();
        log_event(&log_path, &event).unwrap();

        // Tailer should detect truncation and reset
        let events = tailer.poll().unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_read_transfers_missing_file_is_empty() {
        let got = read_transfers(Path::new("/nonexistent/transfers.jsonl")).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn test_read_transfers_skips_blank_and_invalid_lines() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("transfers.jsonl");
        // A blank line and a non-JSON line are both skipped, not fatal.
        fs::write(&log, "\n   \nnot json at all\n{ partial: \n").unwrap();
        let got = read_transfers(&log).unwrap();
        assert!(got.is_empty(), "invalid transfer lines are skipped");
    }

    #[test]
    fn test_event_tailer_handles_rename_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 80, 1024, "key");

        // Write several events
        for _ in 0..5 {
            log_event(&log_path, &event).unwrap();
        }

        // Start tailing from start
        let mut tailer = EventTailer::from_start(log_path.clone());
        assert_eq!(tailer.poll().unwrap().len(), 5);

        rotate_if_needed(&log_path, 1500, 2).unwrap();

        // Write 10 more events to the newly rotated log (new file will have 2 kept + 10 new = 12 events).
        // This ensures the replacement file is larger than the tailer's previous byte offset (5 events),
        // exercising the rename-detection reset path instead of passing via the file_len < position fallback.
        for _ in 0..10 {
            log_event(&log_path, &event).unwrap();
        }

        // Tailer should detect the inode/file index change and reopen/reset position to 0
        let events = tailer.poll().unwrap();
        assert_eq!(events.len(), 2 + 10);
    }

    #[test]
    fn test_concurrent_log_append_and_rotate() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        // Seed with a first event to ensure the file exists
        let first_event = test_event("0", EventResult::LocalHit, 10, 10, 100, "key0");
        log_event(&log_path, &first_event).unwrap();

        let running = Arc::new(AtomicBool::new(true));
        let appender_done = Arc::new(AtomicBool::new(false));

        // Spawn a background rotator thread
        let log_path_clone = log_path.clone();
        let running_clone = running.clone();
        let rotator = std::thread::spawn(move || {
            // Keep 100 lines, max_size 65536 to trigger rotation.
            // A larger cap reduces scheduler-dependent flakiness while keeping rotation behavior.
            while running_clone.load(Ordering::Relaxed) {
                let _ = rotate_if_needed(&log_path_clone, 65536, 100);
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        });

        // Spawn appender thread
        let log_path_clone2 = log_path.clone();
        let appender_done_clone = appender_done.clone();
        let appender = std::thread::spawn(move || {
            for i in 1..=200 {
                let event = test_event(&i.to_string(), EventResult::LocalHit, 10, 10, 100, "key");
                log_event(&log_path_clone2, &event).expect("log_event failed");
                std::thread::sleep(std::time::Duration::from_millis(3));
            }
            appender_done_clone.store(true, Ordering::Relaxed);
        });

        // Spawn tailer/monitor thread
        let log_path_clone3 = log_path.clone();
        let running_clone3 = running.clone();
        let appender_done_clone2 = appender_done.clone();
        let tailer_thread = std::thread::spawn(move || {
            let mut tailer = EventTailer::from_start(log_path_clone3);
            let mut polled_ids = std::collections::HashSet::new();

            while running_clone3.load(Ordering::Relaxed)
                || !appender_done_clone2.load(Ordering::Relaxed)
            {
                if let Ok(events) = tailer.poll() {
                    for event in events {
                        if let Ok(id) = event.crate_name.parse::<usize>() {
                            polled_ids.insert(id);
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }

            // Final drain
            if let Ok(events) = tailer.poll() {
                for event in events {
                    if let Ok(id) = event.crate_name.parse::<usize>() {
                        polled_ids.insert(id);
                    }
                }
            }
            polled_ids
        });

        appender.join().unwrap();
        running.store(false, Ordering::Relaxed);
        rotator.join().unwrap();
        let polled_ids = tailer_thread.join().unwrap();

        // Verify the final log file contents
        let content = fs::read_to_string(&log_path).unwrap();

        // 1. Assert trailing newline is preserved exactly
        assert!(content.ends_with("\n"), "log must end with a newline");
        assert!(
            !content.ends_with("\n\n"),
            "log must not end with double newlines"
        );

        // 2. Parse surviving events
        let mut ids = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let event: BuildEvent =
                serde_json::from_str(line).expect("each line must be valid JSON");
            let id: usize = event
                .crate_name
                .parse()
                .expect("crate name must be parsed as id");
            ids.push(id);
        }

        // 3. Verify no gaps in the surviving sequence in the log file
        assert!(!ids.is_empty(), "at least some events must survive");
        assert!(ids.len() > 1, "multiple events must survive");
        let start = ids[0];
        let end = ids[ids.len() - 1];
        let expected: Vec<usize> = (start..=end).collect();
        assert_eq!(
            ids, expected,
            "there must be no missing events or gaps in the surviving log: got {:?}",
            ids
        );

        // 4. The tailer must still be following the log after rotations — i.e. it
        // observed the final event.
        //
        // It may legitimately miss earlier events: the rotator trims to `keep_lines`
        // concurrently, so an event appended and then rotated away before the tailer
        // next polled was never observable. That is rotation working, not event loss —
        // asserting the tailer sees *every* id makes this test a race it can only win
        // when the tailer happens to outrun the rotator (it flaked on Windows CI).
        //
        // What must hold is that rotation never leaves the tailer stranded on the old
        // inode (kunobi-ninja/kache#518): if it did, it would stop seeing new events
        // entirely and never reach the last one. The deterministic inode-swap guard is
        // `test_event_tailer_handles_rename_rotation`; this is its concurrent analogue.
        //
        // In addition, we verify that the events observed by the tailer form a gap-free
        // sequence (excluding any missed events rotated away before the tailer could poll).
        let mut actual_polled: Vec<usize> = polled_ids.into_iter().collect();
        actual_polled.sort();
        assert!(
            !actual_polled.is_empty(),
            "EventTailer must have observed some events"
        );

        let start_id = actual_polled[0];
        let end_id = *actual_polled.last().unwrap();
        let expected_sequence: Vec<usize> = (start_id..=end_id).collect();
        assert_eq!(
            actual_polled, expected_sequence,
            "EventTailer must not miss any events in its observed sequence: got {:?}",
            actual_polled
        );

        assert_eq!(
            end_id,
            200,
            "EventTailer stopped following the log across rotation: never observed the \
             final event (saw {} ids, max {:?})",
            actual_polled.len(),
            actual_polled.last()
        );
    }
}
