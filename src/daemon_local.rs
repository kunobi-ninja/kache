//! Daemon-assisted local hits (kunobi-ninja/kache#565).
//!
//! The daemon answers `LocalLookup` without ever touching the `with_store`
//! mutex: probes run on dedicated threads that each own a read-only SQLite
//! connection, and the pin/accounting write goes through a single batched
//! writer thread that commits BEFORE the hit reply is sent. That ordering is
//! the safety invariant (see `notes/design/daemon-local-hit.md`): the
//! `last_accessed` touch is what `remove_entry_guarded`'s idle-grace check
//! reads — cross-process, so a separate `kache gc` respects it too — and a
//! deferred touch would let GC evict blobs between our reply and the
//! wrapper's reflink.
//!
//! Blobs are content-addressed, which makes the remaining probe→pin race
//! benign: if the entry is replaced in between, the pin's re-check either
//! fails (reply `fallback`) or pins the successor row while the wrapper
//! restores the still-present old blobs — which are, by content address, a
//! correct result for the same cache key.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rusqlite::TransactionBehavior;

use crate::config::Config;
use crate::daemon::LocalLookupReply;
use crate::store::{self, ProbeOutcome};

/// Server-side budget for one lookup (probe + pin). Past this the daemon
/// replies `fallback` — overload sheds to the wrapper's local path.
pub(crate) const LOCAL_LOOKUP_DEADLINE: Duration = Duration::from_millis(50);

/// Microbatch window for the pin writer: long enough to group-commit a burst
/// of parallel wrappers, short enough to be invisible next to a compile.
const PIN_BATCH_WINDOW: Duration = Duration::from_millis(2);
const PIN_BATCH_MAX: usize = 128;

/// Queue bounds: overload must shed (an instant `fallback` reply), never
/// accumulate. A stale job (deadline passed or requester gone) is skipped by
/// the workers so a burst can't keep threads busy on answers nobody reads —
/// and, for pins, can't keep touching `last_accessed` after the wrapper
/// already fell back (that would double-count hits and extend GC pinning
/// with no restore in flight).
const PROBE_QUEUE_CAP: usize = 64;
const PIN_QUEUE_CAP: usize = 1024;

struct ProbeJob {
    key: String,
    deadline: Instant,
    reply: tokio::sync::oneshot::Sender<ProbeOutcome>,
}

struct PinJob {
    key: String,
    deadline: Instant,
    reply: tokio::sync::oneshot::Sender<bool>,
}

impl PinJob {
    fn is_stale(&self) -> bool {
        self.reply.is_closed() || Instant::now() > self.deadline
    }
}

/// Read-only probe pool + batched pin writer. Threads live for the daemon's
/// lifetime; senders dropping (daemon shutdown) ends the worker loops.
pub(crate) struct LocalHitService {
    probe_txs: Vec<mpsc::SyncSender<ProbeJob>>,
    next_probe: AtomicUsize,
    pin_tx: mpsc::SyncSender<PinJob>,
}

impl LocalHitService {
    pub(crate) fn new(config: &Config) -> Result<Self> {
        let db_path = config.index_db_path();
        let store_dir = config.store_dir();

        // Writer first: a full read-write open (schema init included) so a
        // cold store exists before any read-only connection tries to attach
        // to its WAL.
        std::fs::create_dir_all(&store_dir)
            .with_context(|| format!("creating store directory {}", store_dir.display()))?;
        let pin_db = store::open_index_db(&db_path)
            .with_context(|| format!("opening index for pin writer {}", db_path.display()))?;
        let (pin_tx, pin_rx) = mpsc::sync_channel::<PinJob>(PIN_QUEUE_CAP);
        std::thread::Builder::new()
            .name("kache-pin-writer".to_string())
            .spawn(move || pin_worker(pin_db, pin_rx))
            .context("spawning pin writer thread")?;

        // Probe pool: dedicated threads, one read-only connection each. Not
        // spawn_blocking — tokio's blocking pool has no affinity and would
        // balloon the connection count under load.
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(8);
        let mut probe_txs = Vec::with_capacity(workers);
        for i in 0..workers {
            let db = store::open_index_db_readonly(&db_path)?;
            let store_dir = store_dir.clone();
            let (tx, rx) = mpsc::sync_channel::<ProbeJob>(PROBE_QUEUE_CAP);
            std::thread::Builder::new()
                .name(format!("kache-probe-{i}"))
                .spawn(move || probe_worker(db, store_dir, rx))
                .context("spawning probe thread")?;
            probe_txs.push(tx);
        }

        Ok(Self {
            probe_txs,
            next_probe: AtomicUsize::new(0),
            pin_tx,
        })
    }

    /// Probe, then pin, then answer. Returns `fallback` for every internal
    /// failure — the caller enforces the wall-clock deadline; the per-job
    /// deadline lets workers drop work that already missed it.
    pub(crate) async fn lookup(&self, key: &str) -> LocalLookupReply {
        let deadline = Instant::now() + LOCAL_LOOKUP_DEADLINE;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let idx = self.next_probe.fetch_add(1, Ordering::Relaxed) % self.probe_txs.len();
        if self.probe_txs[idx]
            .try_send(ProbeJob {
                key: key.to_string(),
                deadline,
                reply: tx,
            })
            .is_err()
        {
            // Full queue or worker gone — shed immediately, don't queue.
            return LocalLookupReply::fallback("probe queue full");
        }
        let outcome = match rx.await {
            Ok(outcome) => outcome,
            Err(_) => return LocalLookupReply::fallback("probe dropped"),
        };

        let meta = match outcome {
            ProbeOutcome::Hit(meta) => meta,
            ProbeOutcome::Miss => return LocalLookupReply::miss(),
            ProbeOutcome::Fallback(reason) => return LocalLookupReply::fallback(reason),
        };

        // Commit the pin BEFORE replying: the last_accessed touch is the
        // cross-process guard that keeps GC's idle-grace check from evicting
        // these blobs while the wrapper is still reflinking them.
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .pin_tx
            .try_send(PinJob {
                key: key.to_string(),
                deadline,
                reply: tx,
            })
            .is_err()
        {
            return LocalLookupReply::fallback("pin queue full");
        }
        match rx.await {
            Ok(true) => LocalLookupReply::hit(*meta),
            Ok(false) => LocalLookupReply::fallback("entry vanished before pin"),
            Err(_) => LocalLookupReply::fallback("pin dropped"),
        }
    }
}

fn probe_worker(db: rusqlite::Connection, store_dir: PathBuf, rx: mpsc::Receiver<ProbeJob>) {
    while let Ok(job) = rx.recv() {
        // Stale (deadline passed / requester gone) probes answer nobody —
        // skip them so a backlog can't occupy the pool.
        if job.reply.is_closed() || Instant::now() > job.deadline {
            continue;
        }
        let outcome = store::probe_entry_readonly(&db, &store_dir, &job.key);
        let _ = job.reply.send(outcome);
    }
}

fn pin_worker(mut db: rusqlite::Connection, rx: mpsc::Receiver<PinJob>) {
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        let window = Instant::now() + PIN_BATCH_WINDOW;
        while batch.len() < PIN_BATCH_MAX {
            let remaining = window.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match rx.recv_timeout(remaining) {
                Ok(job) => batch.push(job),
                Err(_) => break,
            }
        }
        // A pin whose requester timed out must not commit: the wrapper is on
        // the local path (which does its own accounting), so a late touch
        // would double-count the hit and extend GC pinning for a restore
        // that is not happening.
        batch.retain(|job| !job.is_stale());
        if batch.is_empty() {
            continue;
        }

        match run_pin_batch(&mut db, &batch) {
            Ok(pinned) => {
                for (job, ok) in batch.into_iter().zip(pinned) {
                    let _ = job.reply.send(ok);
                }
            }
            Err(e) => {
                tracing::warn!("local-hit pin batch failed: {e:#}");
                for job in batch {
                    let _ = job.reply.send(false);
                }
            }
        }
    }
}

/// One IMMEDIATE transaction per batch: writer reservation is taken up front,
/// so the pin-vs-GC race is decided by SQLite's write lock order instead of
/// deferred-transaction upgrade errors. `committed = 1` re-checks the row so
/// an entry removed since the probe answers `false` (→ fallback), not a
/// phantom pin.
fn run_pin_batch(db: &mut rusqlite::Connection, batch: &[PinJob]) -> Result<Vec<bool>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut pinned = Vec::with_capacity(batch.len());
    {
        let mut stmt = tx.prepare_cached(
            "UPDATE entries
             SET last_accessed = datetime('now'), hit_count = hit_count + 1
             WHERE cache_key = ?1 AND committed = 1",
        )?;
        for job in batch {
            pinned.push(stmt.execute([&job.key])? > 0);
        }
    }
    tx.commit()?;
    Ok(pinned)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pin batch must commit a fresh `last_accessed` (the GC idle-grace
    /// pin) and increment `hit_count`, and must report `false` for keys that
    /// vanished or were never committed — the fallback signal.
    #[test]
    fn pin_batch_touches_committed_rows_and_rejects_missing() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let mut db = store::open_index_db(&db_path).unwrap();
        db.execute_batch(
            "INSERT INTO entries (cache_key, crate_name, committed, last_accessed, hit_count)
             VALUES ('live', 'a', 1, datetime('now', '-1 hour'), 0),
                    ('uncommitted', 'b', 0, datetime('now', '-1 hour'), 0);",
        )
        .unwrap();

        let mut rxs = Vec::new();
        let jobs: Vec<PinJob> = ["live", "uncommitted", "gone"]
            .iter()
            .map(|k| {
                let (reply, rx) = tokio::sync::oneshot::channel();
                rxs.push(rx);
                PinJob {
                    key: k.to_string(),
                    deadline: Instant::now() + Duration::from_secs(5),
                    reply,
                }
            })
            .collect();
        assert!(!jobs[0].is_stale(), "live receiver + future deadline");
        let pinned = run_pin_batch(&mut db, &jobs).unwrap();
        assert_eq!(pinned, vec![true, false, false]);

        // Stale detection: a dropped requester or an expired deadline must
        // exclude the job from any future batch (pin_worker retains on this).
        let (reply, rx) = tokio::sync::oneshot::channel::<bool>();
        drop(rx);
        let closed = PinJob {
            key: "live".to_string(),
            deadline: Instant::now() + Duration::from_secs(5),
            reply,
        };
        assert!(closed.is_stale(), "closed reply is stale");
        let (reply, _rx2) = tokio::sync::oneshot::channel::<bool>();
        let expired = PinJob {
            key: "live".to_string(),
            deadline: Instant::now() - Duration::from_millis(1),
            reply,
        };
        assert!(expired.is_stale(), "expired deadline is stale");

        let (hits, recent): (i64, i64) = db
            .query_row(
                "SELECT hit_count,
                        last_accessed >= datetime('now', '-60 seconds')
                 FROM entries WHERE cache_key = 'live'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(hits, 1);
        assert_eq!(recent, 1, "pin must refresh last_accessed (GC grace pin)");
    }
}
