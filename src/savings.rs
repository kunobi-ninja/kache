//! The lifetime savings ledger: `savings.json` in the cache dir.
//!
//! `kache stats` reads its window from the event log, which rotates by size,
//! so older figures used to disappear. The ledger keeps them. It is written
//! only where kache already does bookkeeping, never per compile: a rotation
//! folds in the events it drops, and every recorded GC run adds the bytes it
//! pruned. Lifetime figures are the ledger plus what the log still holds.
//!
//! A missing, corrupt or newer-schema ledger reads as not recorded. A
//! corrupt one is moved to `savings.json.corrupt` before the next write; a
//! newer one is never rewritten.

use crate::config::Config;
use crate::events::BuildEvent;
use crate::store::SweepOrigin;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

pub const SAVINGS_FILE: &str = "savings.json";

/// Current `savings.json` schema. A newer file is left alone.
pub const SAVINGS_SCHEMA: u32 = 1;

/// What kache saved since `since`. The same shape is the `lifetime` object
/// of `kache stats --json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    /// The oldest event or GC run counted.
    pub since: DateTime<Utc>,
    /// Local, prefetched and remote hits.
    #[serde(default)]
    pub hits: u64,
    /// Compile time those hits avoided, as `kache stats` counts it.
    #[serde(default)]
    pub hit_compile_time_ms: u64,
    /// Bytes restored by reflink or hardlink.
    #[serde(default)]
    pub zero_copy_bytes: u64,
    /// Bytes restored by a full copy.
    #[serde(default)]
    pub copied_bytes: u64,
    /// Store bytes removed by the daemon's sweeps and the auto-GC worker.
    #[serde(default)]
    pub pruned_automatic_bytes: u64,
    /// Store bytes removed by a `kache gc` the user ran.
    #[serde(default)]
    pub pruned_requested_bytes: u64,
}

impl Totals {
    fn empty(since: DateTime<Utc>) -> Self {
        Self {
            since,
            hits: 0,
            hit_compile_time_ms: 0,
            zero_copy_bytes: 0,
            copied_bytes: 0,
            pruned_automatic_bytes: 0,
            pruned_requested_bytes: 0,
        }
    }

    /// Add `other` to these totals; the earlier start wins.
    fn absorb(&mut self, other: &Totals) {
        self.since = self.since.min(other.since);
        self.hits += other.hits;
        self.hit_compile_time_ms += other.hit_compile_time_ms;
        self.zero_copy_bytes += other.zero_copy_bytes;
        self.copied_bytes += other.copied_bytes;
        self.pruned_automatic_bytes += other.pruned_automatic_bytes;
        self.pruned_requested_bytes += other.pruned_requested_bytes;
    }

    pub fn pruned_bytes(&self) -> u64 {
        self.pruned_automatic_bytes + self.pruned_requested_bytes
    }

    /// The hit and restore totals of `events`, using the definitions of the
    /// windowed stats and the report. `None` when there are no events.
    fn from_events(events: &[BuildEvent]) -> Option<Self> {
        let since = events.iter().map(|event| event.ts).min()?;
        let stats = crate::events::compute_stats(events);
        Some(Self {
            hits: (stats.local_hits + stats.prefetch_hits + stats.remote_hits) as u64,
            hit_compile_time_ms: stats.hit_compile_time_ms,
            zero_copy_bytes: stats.reflinked_bytes + stats.hardlinked_bytes,
            copied_bytes: stats.copied_bytes,
            ..Self::empty(since)
        })
    }
}

/// Both sides added together, or whichever one exists.
fn merge(a: Option<Totals>, b: Option<Totals>) -> Option<Totals> {
    match (a, b) {
        (Some(mut a), Some(b)) => {
            a.absorb(&b);
            Some(a)
        }
        (a, b) => a.or(b),
    }
}

#[derive(Serialize, Deserialize)]
struct LedgerFile {
    schema: u32,
    #[serde(flatten)]
    totals: Totals,
}

pub fn ledger_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(SAVINGS_FILE)
}

fn lock_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(format!("{SAVINGS_FILE}.lock"))
}

/// What a `savings.json` holds.
enum Ledger {
    Missing,
    /// Unreadable as a ledger of any schema.
    Corrupt,
    /// Written by a newer kache. Its shape may differ, so the schema number
    /// is read before anything else and the file is never rewritten.
    Newer,
    Current(LedgerFile),
}

fn load(path: &Path) -> Ledger {
    let Ok(content) = fs::read_to_string(path) else {
        return if path.exists() {
            Ledger::Corrupt
        } else {
            Ledger::Missing
        };
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&content) else {
        return Ledger::Corrupt;
    };
    let schema = value.get("schema").and_then(serde_json::Value::as_u64);
    if schema.is_some_and(|schema| schema > u64::from(SAVINGS_SCHEMA)) {
        return Ledger::Newer;
    }
    match serde_json::from_value::<LedgerFile>(value) {
        Ok(file) => Ledger::Current(file),
        Err(_) => Ledger::Corrupt,
    }
}

/// Where a corrupt ledger is moved before a fresh one replaces it.
fn corrupt_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join(format!("{SAVINGS_FILE}.corrupt"))
}

/// The ledger in `cache_dir`, or `None` when it is not recorded.
pub fn read(cache_dir: &Path) -> Option<Totals> {
    match load(&ledger_path(cache_dir)) {
        Ledger::Current(file) => Some(file.totals),
        Ledger::Missing | Ledger::Corrupt | Ledger::Newer => None,
    }
}

/// Add `delta` to the ledger in `cache_dir` under its lock, so a rotation and
/// a GC in different processes cannot lose each other's update. A missing
/// ledger starts from `delta`; a corrupt one is moved to `savings.json.corrupt`
/// first; a newer one is left alone, and `delta` is not recorded.
fn add(cache_dir: &Path, delta: &Totals) -> Result<()> {
    fs::create_dir_all(cache_dir).context("creating cache dir for the savings ledger")?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path(cache_dir))
        .context("opening savings ledger lock")?;
    lock.lock().context("locking savings ledger")?;
    let path = ledger_path(cache_dir);
    let res = (|| -> Result<()> {
        let mut totals = delta.clone();
        match load(&path) {
            Ledger::Newer => return Ok(()),
            Ledger::Missing => {}
            Ledger::Corrupt => {
                fs::rename(&path, corrupt_path(cache_dir))
                    .context("moving the corrupt savings ledger aside")?;
            }
            Ledger::Current(current) => totals.absorb(&current.totals),
        }
        let json = serde_json::to_string_pretty(&LedgerFile {
            schema: SAVINGS_SCHEMA,
            totals,
        })?;
        crate::atomic::atomic_replace(&path, json.as_bytes())
    })();
    let _ = lock.unlock();
    res
}

/// Fold the event-log lines a rotation drops into the ledger. Called after
/// the log has been replaced and its lock released, so a reader sees each
/// event in the log or in the ledger, never both; between the replace and
/// this fold it briefly sees it in neither. Lines that are not build events
/// (heartbeats, torn writes) count for nothing.
pub(crate) fn fold_dropped_lines(cache_dir: &Path, lines: &[&str]) -> Result<()> {
    let events: Vec<BuildEvent> = lines
        .iter()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    match Totals::from_events(&events) {
        Some(delta) => add(cache_dir, &delta),
        None => Ok(()),
    }
}

/// Add a GC run's freed store bytes, split by who started it. A run that
/// freed nothing leaves the ledger as it is, so it does not start one.
pub(crate) fn record_pruned(
    cache_dir: &Path,
    origin: SweepOrigin,
    bytes: u64,
    at: DateTime<Utc>,
) -> Result<()> {
    if bytes == 0 {
        return Ok(());
    }
    let mut delta = Totals::empty(at);
    match origin {
        SweepOrigin::Automatic => delta.pruned_automatic_bytes = bytes,
        SweepOrigin::Requested => delta.pruned_requested_bytes = bytes,
    }
    add(cache_dir, &delta)
}

/// Lifetime totals for `kache stats`: the ledger, each volume shard's ledger
/// (shards record only their GC runs), and the events the log still holds.
/// The log and the main ledger are read under the log's shared lock, so a
/// rotation cannot move events between them mid-read. `None` when nothing
/// is recorded anywhere.
pub(crate) fn lifetime(config: &Config) -> Option<Totals> {
    let (events, ledger) =
        crate::events::read_events_and(&config.event_log_path(), || read(&config.cache_dir))
            .unwrap_or_else(|_| (Vec::new(), read(&config.cache_dir)));
    crate::volume_gc::run_on_volume_shards(config, |shard| Ok(read(&shard.cache_dir)))
        .into_iter()
        .map(|(_, shard)| shard)
        .fold(merge(ledger, Totals::from_events(&events)), merge)
}

#[cfg(test)]
mod tests;
