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
    #[serde(default)]
    pub version: String,
    pub result: EventResult,
    pub elapsed_ms: u64,
    pub size: u64,
    #[serde(default)]
    pub cache_key: String,
    /// Event schema version: 0 = legacy, 1 = with prefetch metrics.
    #[serde(default)]
    pub schema: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventResult {
    LocalHit,
    /// Local hit on an artifact that was downloaded by the manifest prefetch.
    PrefetchHit,
    RemoteHit,
    Miss,
    Error,
    Skipped,
}

impl std::fmt::Display for EventResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventResult::LocalHit => write!(f, "local_hit"),
            EventResult::PrefetchHit => write!(f, "prefetch_hit"),
            EventResult::RemoteHit => write!(f, "remote_hit"),
            EventResult::Miss => write!(f, "miss"),
            EventResult::Error => write!(f, "error"),
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
/// Uses O_APPEND for atomic writes on POSIX (safe for concurrent writers).
pub fn log_event(event_log_path: &Path, event: &BuildEvent) -> Result<()> {
    if let Some(parent) = event_log_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(event_log_path)
        .context("opening event log")?;

    let line = serde_json::to_string(event).context("serializing event")?;
    writeln!(file, "{line}").context("writing event to log")?;

    Ok(())
}

/// Read all events from the event log.
pub fn read_events(event_log_path: &Path) -> Result<Vec<BuildEvent>> {
    if !event_log_path.exists() {
        return Ok(Vec::new());
    }

    let file = File::open(event_log_path).context("opening event log")?;
    let reader = BufReader::new(file);
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

    Ok(events)
}

/// Read events since a given timestamp.
pub fn read_events_since(event_log_path: &Path, since: DateTime<Utc>) -> Result<Vec<BuildEvent>> {
    let all = read_events(event_log_path)?;
    Ok(all.into_iter().filter(|e| e.ts >= since).collect())
}

/// Tail the event log, returning new events since the last known position.
pub struct EventTailer {
    path: PathBuf,
    position: u64,
}

impl EventTailer {
    pub fn new(path: PathBuf) -> Self {
        let position = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        EventTailer { path, position }
    }

    /// Start from the beginning.
    pub fn from_start(path: PathBuf) -> Self {
        EventTailer { path, position: 0 }
    }

    /// Read new events since last poll.
    pub fn poll(&mut self) -> Result<Vec<BuildEvent>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();

        if file_len < self.position {
            // File was truncated (log rotation), start from beginning
            self.position = 0;
        }

        if file_len <= self.position {
            return Ok(Vec::new());
        }

        file.seek(SeekFrom::Start(self.position))?;
        let reader = BufReader::new(&file);
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
pub fn rotate_if_needed(event_log_path: &Path, max_size: u64, keep_lines: usize) -> Result<()> {
    if !event_log_path.exists() {
        return Ok(());
    }

    let meta = fs::metadata(event_log_path)?;
    if meta.len() <= max_size {
        return Ok(());
    }

    let content = fs::read_to_string(event_log_path)?;
    let lines: Vec<&str> = content.lines().collect();
    let keep_from = lines.len().saturating_sub(keep_lines);
    let kept: Vec<&str> = lines[keep_from..].to_vec();
    fs::write(event_log_path, kept.join("\n") + "\n")?;

    tracing::info!(
        "rotated event log: kept {} of {} lines",
        kept.len(),
        lines.len()
    );
    Ok(())
}

/// Clear the event log.
#[allow(dead_code)]
pub fn clear_events(event_log_path: &Path) -> Result<()> {
    if event_log_path.exists() {
        fs::write(event_log_path, "")?;
    }
    Ok(())
}

/// Get event statistics.
pub struct EventStats {
    #[allow(dead_code)]
    pub total: usize,
    pub local_hits: usize,
    pub prefetch_hits: usize,
    pub remote_hits: usize,
    pub misses: usize,
    pub errors: usize,
    pub total_size: u64,
    pub total_elapsed_ms: u64,
}

pub fn compute_stats(events: &[BuildEvent]) -> EventStats {
    let mut stats = EventStats {
        total: events.len(),
        local_hits: 0,
        prefetch_hits: 0,
        remote_hits: 0,
        misses: 0,
        errors: 0,
        total_size: 0,
        total_elapsed_ms: 0,
    };

    for event in events {
        match event.result {
            EventResult::LocalHit => stats.local_hits += 1,
            EventResult::PrefetchHit => stats.prefetch_hits += 1,
            EventResult::RemoteHit => stats.remote_hits += 1,
            EventResult::Miss => stats.misses += 1,
            EventResult::Error => stats.errors += 1,
            EventResult::Skipped => {}
        }
        stats.total_size += event.size;
        stats.total_elapsed_ms += event.elapsed_ms;
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
        size: u64,
        cache_key: &str,
    ) -> BuildEvent {
        BuildEvent {
            ts: Utc::now(),
            crate_name: crate_name.to_string(),
            version: "0.0.0".to_string(),
            result,
            elapsed_ms,
            size,
            cache_key: cache_key.to_string(),
            schema: 0,
        }
    }

    #[test]
    fn test_log_and_read_events() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = BuildEvent {
            ts: Utc::now(),
            crate_name: "serde".to_string(),
            version: "1.0.210".to_string(),
            result: EventResult::LocalHit,
            elapsed_ms: 2,
            size: 3145728,
            cache_key: "abc123".to_string(),
            schema: 0,
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
        let event = test_event("tokio", EventResult::Miss, 5000, 8388608, "def456");
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
                1024,
                &format!("key_{i}"),
            );
            log_event(&log_path, &event).unwrap();
        }

        // Rotate with small max size, keep 10 lines
        rotate_if_needed(&log_path, 100, 10).unwrap();

        let events = read_events(&log_path).unwrap();
        assert_eq!(events.len(), 10);
        // Should keep the last 10
        assert_eq!(events[0].crate_name, "crate_90");
    }

    #[test]
    fn test_event_result_display() {
        assert_eq!(EventResult::LocalHit.to_string(), "local_hit");
        assert_eq!(EventResult::PrefetchHit.to_string(), "prefetch_hit");
        assert_eq!(EventResult::RemoteHit.to_string(), "remote_hit");
        assert_eq!(EventResult::Miss.to_string(), "miss");
        assert_eq!(EventResult::Error.to_string(), "error");
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

        let event = test_event("valid", EventResult::Miss, 100, 1024, "key");
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

        let mut old_event = test_event("old", EventResult::Miss, 100, 1024, "key1");
        old_event.ts = Utc::now() - chrono::Duration::hours(2);
        let new_event = test_event("new", EventResult::LocalHit, 10, 512, "key2");

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
            test_event("a", EventResult::LocalHit, 10, 100, "k1"),
            test_event("b", EventResult::PrefetchHit, 5, 150, "k1b"),
            test_event("c", EventResult::RemoteHit, 50, 200, "k2"),
            test_event("d", EventResult::Miss, 1000, 500, "k3"),
            test_event("e", EventResult::Error, 5, 0, "k4"),
            test_event("f", EventResult::Skipped, 0, 0, "k5"),
        ];

        let stats = compute_stats(&events);
        assert_eq!(stats.total, 6);
        assert_eq!(stats.local_hits, 1);
        assert_eq!(stats.prefetch_hits, 1);
        assert_eq!(stats.remote_hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.total_size, 950);
        assert_eq!(stats.total_elapsed_ms, 1070);
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

        let event = test_event("test", EventResult::Miss, 100, 1024, "key");
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

        let event = test_event("test", EventResult::Miss, 100, 1024, "key");
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
    fn test_event_tailer_handles_truncation() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("events.jsonl");

        let event = test_event("test", EventResult::Miss, 100, 1024, "key");

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
}
