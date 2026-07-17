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
    /// 9 = event root.
    #[serde(default)]
    pub schema: u32,
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
    /// Bytes ingested into the store by a full physical copy on a miss — a
    /// genuine second copy, the fallback on a filesystem without CoW.
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

/// Summary event logged once per build session with prefetch metrics.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildSummaryEvent {
    pub ts: DateTime<Utc>,
    pub schema: u32,
    pub warming_wait_ms: u64,
    pub prefetch_duration_ms: u64,
    pub prefetch_requested: usize,
    pub prefetch_downloaded: usize,
    pub shards_matched: usize,
    pub shards_total: usize,
}

/// Append a build event to the event log file.
/// Uses an exclusive sidecar file lock so concurrent wrapper processes cannot
/// interleave JSON lines.
pub fn log_event(event_log_path: &Path, event: &BuildEvent) -> Result<()> {
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

    let line = serde_json::to_string(event).context("serializing event")?;
    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    file.write_all(&bytes).context("writing event to log")?;
    lock.unlock().context("unlocking event log")?;

    Ok(())
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

    /// Read new events since last poll.
    pub fn poll(&mut self) -> Result<Vec<BuildEvent>> {
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
        let mut events = Vec::new();
        let mut bytes_read = 0u64;

        for line in reader.lines() {
            let line = line?;
            bytes_read += line.len() as u64 + 1; // +1 for newline
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(event) = serde_json::from_str::<BuildEvent>(&line) {
                events.push(event);
            }
        }

        self.position += bytes_read;
        Ok(events)
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
            && let Ok(entries) = fs::read_dir(parent)
        {
            let file_prefix = log_path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let temp_marker = format!("{}.tmp.", file_prefix);
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str())
                    && name.starts_with(&temp_marker)
                    && let Ok(meta) = fs::metadata(&path)
                    && let Ok(modified) = meta.modified()
                    && let Ok(age) = modified.elapsed()
                    && age > std::time::Duration::from_secs(300)
                {
                    let _ = fs::remove_file(&path);
                }
            }
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

        // Generate unique temp file path using PID
        let pid = std::process::id();
        let mut temp_name = log_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("invalid log path"))?
            .to_owned();
        temp_name.push(format!(".tmp.{pid}"));
        let temp_path = log_path.with_file_name(temp_name);

        // Write to temp file, fsync, and close
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .context("opening temp log file")?;
        let output = kept.join("\n") + "\n";
        file.write_all(output.as_bytes())
            .context("writing kept lines to temp file")?;
        file.sync_all().context("flushing log temp file")?;
        drop(file); // Close temp file

        // Platform-robust atomic rename with retry on Windows transient sharing/access errors
        const MAX_RETRY_ATTEMPTS: u32 = 10;
        let mut rename_ok = false;
        for attempt in 0..MAX_RETRY_ATTEMPTS {
            match fs::rename(&temp_path, log_path) {
                Ok(()) => {
                    rename_ok = true;
                    break;
                }
                Err(e) => {
                    // Check if it's a transient rename error (Access Denied / Sharing Violation on Windows)
                    #[cfg(windows)]
                    let is_transient = matches!(e.raw_os_error(), Some(5) | Some(32));
                    #[cfg(not(windows))]
                    let is_transient = false;

                    if !is_transient {
                        let _ = fs::remove_file(&temp_path);
                        return Err(e).context("renaming log file");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(
                        (1u64 << attempt.min(6)).min(64),
                    ));
                }
            }
        }

        if !rename_ok {
            let _ = fs::remove_file(&temp_path);
            anyhow::bail!("failed to rename temp log file after retries");
        }

        // On Unix, fsync the parent directory to guarantee rename durability
        #[cfg(unix)]
        {
            if let Some(parent) = log_path.parent()
                && let Ok(dir) = File::open(parent)
            {
                let _ = dir.sync_all();
            }
        }

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
            store_copied_bytes: 0,
            passthrough_reason: String::new(),
            fallback: false,
            exit_code: None,
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
            store_copied_bytes: 0,
            passthrough_reason: String::new(),
            fallback: false,
            exit_code: None,
        };

        log_event(&log_path, &event).unwrap();
        log_event(&log_path, &event).unwrap();

        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].crate_name, "serde");
        assert_eq!(events[0].result, EventResult::LocalHit);
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

        // Write one more event to the newly rotated log
        log_event(&log_path, &event).unwrap();

        // Tailer should detect the inode/file index change and reopen/reset position
        let events = tailer.poll().unwrap();
        assert_eq!(events.len(), 3);
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
            // Keep 20 lines, max_size 4000 to trigger rotation
            while running_clone.load(Ordering::Relaxed) {
                let _ = rotate_if_needed(&log_path_clone, 4000, 20);
                std::thread::sleep(std::time::Duration::from_millis(5));
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
            polled_ids.insert(0);

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

        // 4. Verify that the EventTailer observed EVERY single event (0..=200) without loss
        for expected_id in 0..=200 {
            assert!(
                polled_ids.contains(&expected_id),
                "EventTailer missed event ID {}",
                expected_id
            );
        }
    }
}
