//! One writer thread per index for file-hash memo rows in a long-lived
//! process (the daemon).
//!
//! A compile writes its memo rows with a short busy wait so a hit never
//! stalls behind another connection's write lock. Under sustained load (many
//! concurrent compiles, GC eviction, publishes) that lock is busy most of the
//! time, so most batches were dropped and every later unit hashed the same
//! files again: more reads, longer compiles, more lock pressure. In the daemon
//! the batches go to this thread instead, which waits as long as the index
//! needs and coalesces batches into one transaction. A short-lived wrapper
//! process never enables it: queued rows would die with the process.

use kache_store::file_hash::{FileFingerprint, FileHashCache};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

pub(crate) type MemoRows = Vec<(FileFingerprint, String)>;

/// Batches waiting for the writer. A full queue hands the batch back to the
/// caller's own short write, so memory stays bounded when the index stalls.
const QUEUE_BATCHES: usize = 1024;
/// Rows folded into one transaction, so one commit never holds the write
/// lock for long.
const MAX_ROWS_PER_COMMIT: usize = 20_000;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_ATTEMPTS: u32 = 4;

static ENABLED: AtomicBool = AtomicBool::new(false);
static WRITERS: Mutex<Option<HashMap<PathBuf, SyncSender<MemoRows>>>> = Mutex::new(None);

/// Route memo rows through a writer thread for the rest of this process.
pub(crate) fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// Queue `rows` for the index at `db_path`. `Err` hands them back when no
/// writer runs in this process, its queue is full or it has stopped.
pub(crate) fn submit(db_path: &Path, rows: MemoRows) -> Result<(), MemoRows> {
    if !ENABLED.load(Ordering::Relaxed) || db_path.as_os_str().is_empty() {
        return Err(rows);
    }
    let Some(sender) = writer_for(db_path) else {
        return Err(rows);
    };
    match sender.try_send(rows) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(rows)) => Err(rows),
        Err(TrySendError::Disconnected(rows)) => {
            forget(db_path);
            Err(rows)
        }
    }
}

fn writer_for(db_path: &Path) -> Option<SyncSender<MemoRows>> {
    let mut writers = WRITERS.lock().unwrap_or_else(PoisonError::into_inner);
    let writers = writers.get_or_insert_with(HashMap::new);
    if let Some(sender) = writers.get(db_path) {
        return Some(sender.clone());
    }
    let (sender, receiver) = sync_channel(QUEUE_BATCHES);
    let path = db_path.to_path_buf();
    std::thread::Builder::new()
        .name("kache-memo-writer".into())
        .spawn(move || run(&path, &receiver))
        .ok()?;
    writers.insert(db_path.to_path_buf(), sender.clone());
    Some(sender)
}

fn forget(db_path: &Path) {
    if let Some(writers) = WRITERS
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .as_mut()
    {
        writers.remove(db_path);
    }
}

fn open(db_path: &Path) -> rusqlite::Result<rusqlite::Connection> {
    let db = rusqlite::Connection::open(db_path)?;
    db.busy_timeout(BUSY_TIMEOUT)?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(db)
}

fn run(db_path: &Path, receiver: &Receiver<MemoRows>) {
    let db = match open(db_path) {
        Ok(db) => db,
        Err(error) => {
            // Dropping the receiver disconnects every sender, which then
            // falls back to its own write.
            tracing::warn!(path = %db_path.display(), "memo writer cannot open the index: {error}");
            return;
        }
    };
    while let Ok(mut rows) = receiver.recv() {
        while rows.len() < MAX_ROWS_PER_COMMIT {
            match receiver.try_recv() {
                Ok(more) => rows.extend(more),
                Err(_) => break,
            }
        }
        write_with_retry(&db, &rows);
    }
}

fn write_with_retry(db: &rusqlite::Connection, rows: &[(FileFingerprint, String)]) {
    let mut last_error = None;
    for attempt in 0..WRITE_ATTEMPTS {
        match write(db, rows) {
            Ok(()) => return,
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(50 << attempt));
            }
        }
    }
    if let Some(error) = last_error {
        tracing::warn!(rows = rows.len(), "file hash memo not written: {error}");
    }
}

pub(crate) fn write(
    db: &rusqlite::Connection,
    rows: &[(FileFingerprint, String)],
) -> rusqlite::Result<()> {
    let cache = FileHashCache::Borrowed(db);
    db.execute_batch("BEGIN IMMEDIATE")?;
    for (fingerprint, hash) in rows {
        if let Err(error) = cache.put(fingerprint, hash) {
            let _ = db.execute_batch("ROLLBACK");
            return Err(error);
        }
    }
    db.execute_batch("COMMIT")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(dir: &Path, name: &str) -> (FileFingerprint, String) {
        (
            FileFingerprint {
                path: dir.join(name).to_string_lossy().into_owned(),
                size: 100_000,
                mtime_ns: 1,
                ctime_ns: 1,
                inode: 7,
            },
            "ab".repeat(32),
        )
    }

    fn rows_in(db_path: &Path) -> i64 {
        let db = rusqlite::Connection::open(db_path).unwrap();
        db.query_row("SELECT count(*) FROM file_hashes", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn a_queued_batch_waits_out_a_held_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        drop(FileHashCache::open(&db_path).unwrap());
        let holder = rusqlite::Connection::open(&db_path).unwrap();
        holder.execute_batch("BEGIN IMMEDIATE").unwrap();

        // Not through `submit`: enabling is process-wide and would move
        // every other test's flush onto a writer thread.
        writer_for(&db_path)
            .unwrap()
            .send(vec![row(dir.path(), "a"), row(dir.path(), "b")])
            .unwrap();
        std::thread::sleep(Duration::from_millis(300));
        holder.execute_batch("COMMIT").unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while rows_in(&db_path) < 2 {
            assert!(std::time::Instant::now() < deadline, "rows never written");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
