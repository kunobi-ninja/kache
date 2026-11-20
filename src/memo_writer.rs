//! One writer thread per index for file-hash memo rows in a long-lived
//! process (the daemon).
//!
//! A compile writes its memo rows with a short busy wait so a hit never
//! stalls behind another connection's write lock. Under sustained load (many
//! concurrent compiles, GC eviction, publishes) that lock is busy most of the
//! time, so most batches were dropped and every later unit hashed the same
//! files again: more reads, longer compiles, more lock pressure. In the daemon
//! the batches go to this thread instead, which waits out a busy index for
//! longer than a compile could and writes the batches that queued meanwhile
//! together. A short-lived wrapper process never enables it: queued rows
//! would die with the process.
//!
//! The thread opens the index for each attempt at a write and closes it
//! after, as the rest of the daemon does. An index that was quarantined and
//! created again is then written at its new file, and nothing keeps the old
//! one open between writes. Rows still queued when the daemon exits are lost, as the
//! rows of a busy index were before: the next build hashes those files again.

use kache_store::file_hash::{FileFingerprint, FileHashCache};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

pub(crate) type MemoRows = Vec<(FileFingerprint, String)>;

/// Rows one writer holds, queued or being written. Past this a batch goes
/// back to the caller's own short write, so memory stays bounded while the
/// index stalls: a row is a path and a hash, a few hundred bytes.
const MAX_HELD_ROWS: usize = 200_000;
/// Rows written in one transaction, so no commit holds the write lock for
/// long.
const MAX_ROWS_PER_COMMIT: usize = 20_000;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
/// Times a failed write is tried again, each after a longer pause.
const WRITE_RETRIES: u32 = 3;

static ENABLED: AtomicBool = AtomicBool::new(false);
static WRITERS: Mutex<Option<HashMap<PathBuf, Queue>>> = Mutex::new(None);

/// Route memo rows through a writer thread for the rest of this process.
// Process-wide, so no test can turn it on: every other test's flush would
// move to a writer thread. `submit` is tested with it off, and `enqueue`
// covers what it does once on.
#[mutants::skip]
pub(crate) fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// Queue `rows` for the index at `db_path`. `Err` hands them back when no
/// writer runs in this process, it holds too many rows or it has stopped.
pub(crate) fn submit(db_path: &Path, rows: MemoRows) -> Result<(), MemoRows> {
    if !ENABLED.load(Ordering::Relaxed) {
        return Err(rows);
    }
    enqueue(db_path, rows)
}

/// Queue `rows` with the writer of the index at `db_path`, started on first
/// use.
fn enqueue(db_path: &Path, rows: MemoRows) -> Result<(), MemoRows> {
    // An index in memory has no file another connection could open.
    if db_path.as_os_str().is_empty() {
        return Err(rows);
    }
    let Some(writer) = writer_for(db_path) else {
        return Err(rows);
    };
    writer.push(rows).map_err(|(refused, rows)| {
        if matches!(refused, Refused::Stopped) {
            forget(db_path, &writer);
        }
        rows
    })
}

fn writer_for(db_path: &Path) -> Option<Queue> {
    let mut writers = WRITERS.lock().unwrap_or_else(PoisonError::into_inner);
    let writers = writers.get_or_insert_with(HashMap::new);
    if let Some(writer) = writers.get(db_path) {
        return Some(writer.clone());
    }
    let writer = Queue::start(db_path, MAX_HELD_ROWS)?;
    writers.insert(db_path.to_path_buf(), writer.clone());
    Some(writer)
}

/// Drop `stopped` as the writer for `db_path`, so the next batch starts
/// another. A writer that already took its place stays.
fn forget(db_path: &Path, stopped: &Queue) {
    let mut writers = WRITERS.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(writers) = writers.as_mut()
        && writers
            .get(db_path)
            .is_some_and(|writer| Arc::ptr_eq(&writer.held, &stopped.held))
    {
        writers.remove(db_path);
    }
}

/// Why a writer handed a batch back.
#[derive(Debug, PartialEq, Eq)]
enum Refused {
    /// It holds as many rows as it may.
    Full,
    /// Its thread has ended.
    Stopped,
}

/// The way in to one index's writer thread.
#[derive(Clone)]
struct Queue {
    sender: Sender<MemoRows>,
    /// Rows queued or being written.
    held: Arc<AtomicUsize>,
    capacity: usize,
}

impl Queue {
    /// Start a writer for the index at `db_path` that holds at most
    /// `capacity` rows.
    fn start(db_path: &Path, capacity: usize) -> Option<Self> {
        let (sender, receiver) = channel();
        let held = Arc::new(AtomicUsize::new(0));
        let (path, writing) = (db_path.to_path_buf(), Arc::clone(&held));
        std::thread::Builder::new()
            .name("kache-memo-writer".into())
            .spawn(move || run(&path, &receiver, &writing))
            .ok()?;
        Some(Self {
            sender,
            held,
            capacity,
        })
    }

    fn push(&self, rows: MemoRows) -> Result<(), (Refused, MemoRows)> {
        let incoming = rows.len();
        if self.held.fetch_add(incoming, Ordering::Relaxed) + incoming > self.capacity {
            self.held.fetch_sub(incoming, Ordering::Relaxed);
            return Err((Refused::Full, rows));
        }
        self.sender.send(rows).map_err(|unsent| {
            self.held.fetch_sub(incoming, Ordering::Relaxed);
            (Refused::Stopped, unsent.0)
        })
    }
}

fn run(db_path: &Path, receiver: &Receiver<MemoRows>, held: &AtomicUsize) {
    while let Ok(mut rows) = receiver.recv() {
        // One write for everything that queued while the last one ran.
        rows.extend(receiver.try_iter().flatten());
        write_rows(db_path, &rows, MAX_ROWS_PER_COMMIT);
        held.fetch_sub(rows.len(), Ordering::Relaxed);
    }
}

/// Write `rows` to the index at `db_path`, `per_commit` rows to a
/// transaction. Each attempt opens the index itself and closes it after, so
/// nothing holds the file while a retry waits, and an index created again
/// meanwhile is the one written. A commit that fails after its retries ends
/// the write: an index that takes no rows is not asked again for the rest.
fn write_rows(db_path: &Path, rows: &[(FileFingerprint, String)], per_commit: usize) {
    for commit in rows.chunks(per_commit) {
        let written = retried(WRITE_RETRIES, std::thread::sleep, || {
            open(db_path).and_then(|db| write(&db, commit))
        });
        if let Err(error) = written {
            tracing::debug!(
                rows = rows.len(),
                path = %db_path.display(),
                "file hash memo not written: {error}"
            );
            break;
        }
    }
}

/// The index at `db_path`. Never creates it: the store does, with its
/// schema, and an empty file made while the index was being replaced would
/// stand in the store's way.
fn open(db_path: &Path) -> rusqlite::Result<rusqlite::Connection> {
    let db = rusqlite::Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    db.busy_timeout(BUSY_TIMEOUT)?;
    // A memo row is not served after a hard stop
    // ([`kache_store::file_hash::memo_rows_valid_since`]), so none needs a
    // flush of its own.
    db.pragma_update(None, "synchronous", "NORMAL")?;
    Ok(db)
}

/// `write()`, tried again up to `retries` times while it fails, after a
/// pause that doubles from 50 ms.
fn retried<E>(
    retries: u32,
    mut pause: impl FnMut(Duration),
    mut write: impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    let mut outcome = write();
    for retry in 0..retries {
        if outcome.is_ok() {
            break;
        }
        pause(Duration::from_millis(50 << retry));
        outcome = write();
    }
    outcome
}

/// Write `rows` in one transaction, and leave none open on `db` however it
/// ends: a transaction left open holds the index write lock for as long as
/// the connection lives.
pub(crate) fn write(
    db: &rusqlite::Connection,
    rows: &[(FileFingerprint, String)],
) -> rusqlite::Result<()> {
    let cache = FileHashCache::Borrowed(db);
    db.execute_batch("BEGIN IMMEDIATE")?;
    let written = rows
        .iter()
        .try_for_each(|(fingerprint, hash)| cache.put(fingerprint, hash))
        .and_then(|()| db.execute_batch("COMMIT"));
    if written.is_err() {
        // A COMMIT that fails can leave its transaction open too.
        let _ = db.execute_batch("ROLLBACK");
    }
    written
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

    /// An index with the memo's tables at `dir/index.db`.
    fn index_in(dir: &Path) -> PathBuf {
        let db_path = dir.join("index.db");
        drop(FileHashCache::open(&db_path).unwrap());
        db_path
    }

    fn rows_in(db_path: &Path) -> i64 {
        let db = rusqlite::Connection::open(db_path).unwrap();
        db.query_row("SELECT count(*) FROM file_hashes", [], |row| row.get(0))
            .unwrap()
    }

    fn wait_until(what: &str, mut met: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !met() {
            assert!(std::time::Instant::now() < deadline, "{what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Holds the write lock of the index at `db_path` until it commits.
    fn write_lock(db_path: &Path) -> rusqlite::Connection {
        let holder = rusqlite::Connection::open(db_path).unwrap();
        holder.execute_batch("BEGIN IMMEDIATE").unwrap();
        holder
    }

    #[test]
    fn a_process_that_never_enabled_the_writer_keeps_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = index_in(dir.path());
        let rows = vec![row(dir.path(), "a")];
        assert_eq!(submit(&db_path, rows.clone()), Err(rows));
    }

    #[test]
    fn an_index_in_memory_has_no_writer() {
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![row(dir.path(), "a")];
        assert_eq!(enqueue(Path::new(""), rows.clone()), Err(rows));
    }

    /// The caller is not held up by a busy index, and its rows are written
    /// once the lock is free.
    #[test]
    fn queued_rows_wait_out_a_held_write_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = index_in(dir.path());
        let holder = write_lock(&db_path);

        // Not through `submit`: enabling is process-wide and would move
        // every other test's flush onto a writer thread.
        enqueue(&db_path, vec![row(dir.path(), "a"), row(dir.path(), "b")]).unwrap();
        // Long enough for the writer to meet the lock.
        std::thread::sleep(Duration::from_millis(300));
        holder.execute_batch("COMMIT").unwrap();

        wait_until("rows never written", || rows_in(&db_path) == 2);
    }

    #[test]
    fn a_writer_hands_back_rows_past_its_capacity_until_it_has_written() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = index_in(dir.path());
        let holder = write_lock(&db_path);
        let writer = Queue::start(&db_path, 3).unwrap();
        let held = || writer.held.load(Ordering::Relaxed);

        let three = ["a", "b", "c"].map(|name| row(dir.path(), name)).to_vec();
        assert_eq!(writer.push(three), Ok(()), "as many as it may hold");
        let fourth = vec![row(dir.path(), "d")];
        assert_eq!(
            writer.push(fourth.clone()),
            Err((Refused::Full, fourth.clone()))
        );
        assert_eq!(held(), 3, "the rows handed back are not held");

        holder.execute_batch("COMMIT").unwrap();
        wait_until("rows never written", || held() == 0);
        assert_eq!(rows_in(&db_path), 3);
        assert_eq!(writer.push(fourth), Ok(()), "room again");
        wait_until("the later row never written", || rows_in(&db_path) == 4);
    }

    /// A writer whose thread has ended hands its rows back, and the next
    /// batch for that index starts another.
    #[test]
    fn a_stopped_writer_hands_rows_back_and_is_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = index_in(dir.path());
        let (sender, receiver) = channel();
        drop(receiver);
        let stopped = Queue {
            sender,
            held: Arc::new(AtomicUsize::new(0)),
            capacity: 10,
        };
        WRITERS
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert_with(HashMap::new)
            .insert(db_path.clone(), stopped.clone());

        let rows = vec![row(dir.path(), "a")];
        assert_eq!(enqueue(&db_path, rows.clone()), Err(rows.clone()));
        assert_eq!(stopped.held.load(Ordering::Relaxed), 0);
        assert_eq!(enqueue(&db_path, rows), Ok(()), "a new writer");
        wait_until("rows never written", || rows_in(&db_path) == 1);
    }

    /// An index that is quarantined and created again while the daemon runs
    /// gets the later rows at its new file.
    #[test]
    fn rows_reach_an_index_that_was_replaced_while_the_writer_ran() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = index_in(dir.path());
        let quarantined = dir.path().join("index.db.corrupt");
        let writer = Queue::start(&db_path, 10).unwrap();
        // Nothing held: the write is done and its connection closed.
        let written = || writer.held.load(Ordering::Relaxed) == 0;

        writer.push(vec![row(dir.path(), "a")]).unwrap();
        wait_until("the first row never written", written);
        assert_eq!(rows_in(&db_path), 1);

        std::fs::rename(&db_path, &quarantined).unwrap();
        index_in(dir.path());
        writer
            .push(vec![row(dir.path(), "b"), row(dir.path(), "c")])
            .unwrap();
        wait_until("the later rows never written", written);
        assert_eq!(rows_in(&db_path), 2, "at the index now there");
        assert_eq!(rows_in(&quarantined), 1);
    }

    #[test]
    fn the_writer_never_creates_an_index() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("index.db");
        assert!(open(&missing).is_err());
        write_rows(&missing, &[row(dir.path(), "a")], 1);
        assert!(!missing.exists());
    }

    /// A write is committed a few rows at a time, and ends at the first
    /// commit the index refuses.
    #[test]
    fn rows_are_committed_a_few_at_a_time_until_a_commit_fails() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = index_in(dir.path());
        rusqlite::Connection::open(&db_path)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse BEFORE INSERT ON file_hashes
                 WHEN NEW.path LIKE '%refused'
                 BEGIN SELECT RAISE(ABORT, 'refused'); END;",
            )
            .unwrap();

        let rows = ["a", "b", "c", "refused", "e", "f"].map(|name| row(dir.path(), name));
        write_rows(&db_path, &rows, 2);
        assert_eq!(
            rows_in(&db_path),
            2,
            "the first commit: not `c`, refused with its neighbour, nor the commit after"
        );
    }

    #[test]
    fn a_failed_write_is_tried_again_after_doubling_pauses() {
        // A write that fails `failing` times, then succeeds.
        let attempt = |failing: u32| {
            let (mut writes, mut pauses) = (0, Vec::new());
            let outcome = retried(
                3,
                |pause| pauses.push(pause.as_millis()),
                || {
                    writes += 1;
                    if writes <= failing {
                        Err(writes)
                    } else {
                        Ok(())
                    }
                },
            );
            (outcome, writes, pauses)
        };
        assert_eq!(attempt(0), (Ok(()), 1, vec![]));
        assert_eq!(attempt(2), (Ok(()), 3, vec![50, 100]));
        assert_eq!(
            attempt(9),
            (Err(4), 4, vec![50, 100, 200]),
            "the last failure, after every retry"
        );
    }

    #[test]
    fn a_failed_write_leaves_no_transaction_open() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        // No memo tables yet, so the first row fails inside the transaction.
        let db = rusqlite::Connection::open(&db_path).unwrap();
        let rows = [row(dir.path(), "a")];
        assert!(write(&db, &rows).is_err());
        assert!(db.is_autocommit(), "rolled back");

        kache_store::file_hash::ensure_file_hash_cache_schema(&db).unwrap();
        write(&db, &rows).unwrap();
        assert!(db.is_autocommit());
        assert_eq!(rows_in(&db_path), 1);
    }
}
