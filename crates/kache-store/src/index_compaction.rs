//! Reclaiming free SQLite pages from the index file.

use crate::file_hash::FileHashCache;
use rusqlite::{Connection, ErrorCode};

/// Free pages below this many bytes are left for SQLite to reuse.
const MIN_RECLAIM_BYTES: u64 = 64 << 20;
/// Room kept on top of the rewritten pages for journal and filesystem slack.
const VACUUM_HEADROOM_BYTES: u64 = 64 << 20;

/// Page accounting of the index file, read without modifying it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexPageStats {
    pub pages: u64,
    pub free_pages: u64,
    pub page_size: u64,
}

impl IndexPageStats {
    pub fn file_bytes(&self) -> u64 {
        self.pages.saturating_mul(self.page_size)
    }

    pub fn free_bytes(&self) -> u64 {
        self.free_pages.saturating_mul(self.page_size)
    }

    pub fn live_bytes(&self) -> u64 {
        self.pages
            .saturating_sub(self.free_pages)
            .saturating_mul(self.page_size)
    }

    /// Worth a VACUUM only when the freelist is both large and at least a
    /// quarter of the file.
    pub fn should_compact(&self) -> bool {
        self.free_bytes() >= MIN_RECLAIM_BYTES && self.free_pages >= self.pages.div_ceil(4)
    }

    /// Free disk bytes a VACUUM needs. VACUUM copies only live pages: once
    /// into a temporary database and once into the WAL. The size of the file
    /// itself does not matter, so a 45 GB index with 1 GB live needs about
    /// 2 GB, not 90 GB.
    pub fn vacuum_space_needed(&self) -> u64 {
        self.live_bytes()
            .saturating_mul(2)
            .saturating_add(VACUUM_HEADROOM_BYTES)
    }

    pub fn fits_in(&self, free_disk_bytes: u64) -> bool {
        free_disk_bytes >= self.vacuum_space_needed()
    }
}

/// Result of one compaction attempt. Only I/O failures are errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexCompaction {
    /// The freelist is below the thresholds.
    NotNeeded,
    /// Another connection holds the index. Nothing was lost; retry later.
    Busy,
    /// The volume cannot hold the rewritten copy. Free pages stay reusable.
    InsufficientSpace { needed: u64, free: u64 },
    /// File size in bytes before and after.
    Compacted { before: u64, after: u64 },
}

/// Page accounting for any connection to the index, including read-only ones.
pub fn index_page_stats(db: &Connection) -> rusqlite::Result<IndexPageStats> {
    let pragma = |name: &str| -> rusqlite::Result<u64> {
        db.pragma_query_value(None, name, |row| row.get::<_, u32>(0).map(u64::from))
    };
    Ok(IndexPageStats {
        pages: pragma("page_count")?,
        free_pages: pragma("freelist_count")?,
        page_size: pragma("page_size")?,
    })
}

fn is_busy(err: &rusqlite::Error) -> bool {
    matches!(
        err.sqlite_error_code(),
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    )
}

/// Where a compaction attempt stopped. The two busy stages differ only in
/// the message `compact_sparse_index` shows.
enum Attempt {
    Done(IndexCompaction),
    BusyBeforeVacuum,
    BusyAfterVacuum,
}

const BUSY_BEFORE_VACUUM: &str = "index is busy; retry compaction after builds finish";
const BUSY_AFTER_VACUUM: &str = "index compacted but WAL is busy; retry repair after builds finish";
const NO_SPACE: &str = "not enough free space to compact the index; its free pages remain reusable";

/// Turn index contention into `when_busy`; every other error stays an error.
fn busy_as<T>(result: rusqlite::Result<T>, when_busy: T) -> rusqlite::Result<T> {
    match result {
        Err(err) if is_busy(&err) => Ok(when_busy),
        other => other,
    }
}

/// TRUNCATE checkpoint; `Ok(false)` when another connection blocked it.
fn checkpoint(db: &Connection) -> rusqlite::Result<bool> {
    let ran = db.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        row.get::<_, i64>(0).map(|busy| busy == 0)
    });
    busy_as(ran, false)
}

fn attempt(db: &Connection) -> anyhow::Result<Attempt> {
    use anyhow::Context;
    let stats = index_page_stats(db)?;
    if !stats.should_compact() {
        return Ok(Attempt::Done(IndexCompaction::NotNeeded));
    }
    let path = std::path::Path::new(db.path().context("index has no filesystem path")?);
    let free = kache_fs::volume_usage(path)
        .context("could not check free space for index compaction")?
        .free;
    if !stats.fits_in(free) {
        return Ok(Attempt::Done(IndexCompaction::InsufficientSpace {
            needed: stats.vacuum_space_needed(),
            free,
        }));
    }
    if !checkpoint(db)? {
        return Ok(Attempt::BusyBeforeVacuum);
    }
    let before = std::fs::metadata(path)?.len();
    if !busy_as(db.execute_batch("VACUUM").map(|()| true), false)? {
        return Ok(Attempt::BusyBeforeVacuum);
    }
    // The file shrinks only once the vacuumed pages leave the WAL.
    if !checkpoint(db)? {
        return Ok(Attempt::BusyAfterVacuum);
    }
    let after = std::fs::metadata(path)?.len();
    Ok(Attempt::Done(IndexCompaction::Compacted { before, after }))
}

impl FileHashCache<'_> {
    pub fn index_page_stats(&self) -> rusqlite::Result<IndexPageStats> {
        index_page_stats(self.db())
    }

    /// VACUUM the index when its freelist is large. Index reads continue
    /// while it runs but writes wait, so callers pick a quiet moment; a
    /// contended index reports `Busy` instead of failing.
    pub fn compact_index(&self) -> anyhow::Result<IndexCompaction> {
        Ok(match attempt(self.db())? {
            Attempt::Done(outcome) => outcome,
            Attempt::BusyBeforeVacuum | Attempt::BusyAfterVacuum => IndexCompaction::Busy,
        })
    }

    /// `compact_index` for explicit repair: anything short of a compaction
    /// that was needed is an error the user can read.
    pub fn compact_sparse_index(&self) -> anyhow::Result<Option<(u64, u64)>> {
        match attempt(self.db())? {
            Attempt::Done(IndexCompaction::NotNeeded) => Ok(None),
            Attempt::Done(IndexCompaction::Compacted { before, after }) => {
                Ok(Some((before, after)))
            }
            Attempt::Done(IndexCompaction::InsufficientSpace { .. }) => anyhow::bail!(NO_SPACE),
            Attempt::Done(IndexCompaction::Busy) | Attempt::BusyBeforeVacuum => {
                anyhow::bail!(BUSY_BEFORE_VACUUM)
            }
            Attempt::BusyAfterVacuum => anyhow::bail!(BUSY_AFTER_VACUUM),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(pages: u64, free_pages: u64, page_size: u64) -> IndexPageStats {
        IndexPageStats {
            pages,
            free_pages,
            page_size,
        }
    }

    /// A 45 GB index whose migration left 44.2 GB on the freelist.
    const SPARSE_HOST: IndexPageStats = IndexPageStats {
        pages: 11_019_675,
        free_pages: 10_803_505,
        page_size: 4096,
    };

    /// An index with 80 MiB of dropped legacy rows and one live table.
    #[cfg(unix)]
    fn sparse_index(path: &std::path::Path) -> FileHashCache<'static> {
        let db = Connection::open(path).unwrap();
        db.execute_batch(
            "CREATE TABLE legacy(payload BLOB);
             INSERT INTO legacy VALUES (zeroblob(83886080));
             CREATE TABLE kept(value TEXT);
             INSERT INTO kept VALUES ('live');",
        )
        .unwrap();
        drop(db);
        let cache = FileHashCache::open(path).unwrap();
        cache.db().execute_batch("DROP TABLE legacy").unwrap();
        cache
    }

    #[test]
    fn index_compaction_requires_both_size_and_fraction() {
        assert!(!stats(100_000, 25_000, 1).should_compact());
        assert!(!stats(1_000_000, 20_000, 4096).should_compact());
        assert!(stats(65_536, 16_384, 4096).should_compact());
        assert!(!stats(65_537, 16_384, 4096).should_compact());
        assert!(!stats(65_532, 16_383, 4096).should_compact());
    }

    #[test]
    fn page_stats_split_the_file_into_free_and_live_bytes() {
        let s = stats(10, 3, 4096);
        assert_eq!(s.file_bytes(), 40_960);
        assert_eq!(s.free_bytes(), 12_288);
        assert_eq!(s.live_bytes(), 28_672);
        assert_eq!(s.vacuum_space_needed(), 2 * 28_672 + (64 << 20));
        assert_eq!(stats(3, 10, 4096).live_bytes(), 0);
        assert_eq!(stats(u64::MAX, 0, 4096).vacuum_space_needed(), u64::MAX);
    }

    #[test]
    fn space_guard_follows_live_bytes_not_file_size() {
        assert_eq!(SPARSE_HOST.file_bytes(), 45_136_588_800);
        assert_eq!(SPARSE_HOST.live_bytes(), 885_432_320);
        assert!(SPARSE_HOST.should_compact());
        let needed = SPARSE_HOST.vacuum_space_needed();
        assert_eq!(needed, 1_837_973_504);
        assert!(SPARSE_HOST.fits_in(3 << 30));
        assert!(!SPARSE_HOST.fits_in(1 << 30));
        assert!(SPARSE_HOST.fits_in(needed));
        assert!(!SPARSE_HOST.fits_in(needed - 1));
    }

    #[test]
    fn only_contention_counts_as_busy() {
        let failure =
            |code: i32| rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None);
        assert!(is_busy(&failure(rusqlite::ffi::SQLITE_BUSY)));
        assert!(is_busy(&failure(rusqlite::ffi::SQLITE_LOCKED)));
        assert!(!is_busy(&failure(rusqlite::ffi::SQLITE_IOERR)));
        assert!(!is_busy(&failure(rusqlite::ffi::SQLITE_FULL)));
        assert!(!is_busy(&rusqlite::Error::QueryReturnedNoRows));

        assert_eq!(busy_as(Err(failure(rusqlite::ffi::SQLITE_BUSY)), 7), Ok(7));
        assert_eq!(
            busy_as(Err(failure(rusqlite::ffi::SQLITE_LOCKED)), 7),
            Ok(7)
        );
        assert_eq!(busy_as(Ok(3), 7), Ok(3));
        let io = busy_as(Err(failure(rusqlite::ffi::SQLITE_IOERR)), 7).unwrap_err();
        assert_eq!(io.sqlite_error_code(), Some(ErrorCode::SystemIoFailure));
    }

    #[test]
    fn fresh_index_needs_no_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        let stats = cache.index_page_stats().unwrap();
        assert!(stats.pages > 0);
        assert_eq!(stats.free_pages, 0);
        assert!(!stats.should_compact());
        assert_eq!(cache.compact_index().unwrap(), IndexCompaction::NotNeeded);
        assert_eq!(cache.compact_sparse_index().unwrap(), None);
    }

    #[test]
    #[cfg(unix)]
    fn dropped_table_is_reported_then_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let cache = sparse_index(&path);
        let sparse = cache.index_page_stats().unwrap();
        assert!(sparse.free_bytes() >= 80 << 20);
        assert!(sparse.live_bytes() < 1 << 20);
        assert!(sparse.should_compact());
        assert_eq!(index_page_stats(cache.db()).unwrap(), sparse);

        let IndexCompaction::Compacted { before, after } = cache.compact_index().unwrap() else {
            panic!("large freelist must compact");
        };
        assert!(before >= 80 << 20);
        assert!(after < 1 << 20);
        assert_eq!(after, std::fs::metadata(&path).unwrap().len());
        let compacted = cache.index_page_stats().unwrap();
        assert_eq!(compacted.free_pages, 0);
        assert_eq!(compacted.file_bytes(), after);
        let kept: String = cache
            .db()
            .query_row("SELECT value FROM kept", [], |row| row.get(0))
            .unwrap();
        assert_eq!(kept, "live");
        assert_eq!(cache.compact_index().unwrap(), IndexCompaction::NotNeeded);
    }

    #[test]
    #[cfg(unix)]
    fn open_reader_makes_compaction_busy_and_leaves_the_file_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        let cache = sparse_index(&path);
        cache.db().pragma_update(None, "busy_timeout", "0").unwrap();
        let reader = Connection::open(&path).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        let _: String = reader
            .query_row("SELECT value FROM kept", [], |row| row.get(0))
            .unwrap();

        let before = cache.index_page_stats().unwrap();
        assert_eq!(cache.compact_index().unwrap(), IndexCompaction::Busy);
        assert_eq!(cache.index_page_stats().unwrap(), before);
        assert!(std::fs::metadata(&path).unwrap().len() >= 80 << 20);
        let err = cache.compact_sparse_index().unwrap_err();
        assert_eq!(err.to_string(), BUSY_BEFORE_VACUUM);

        reader.execute_batch("COMMIT").unwrap();
        assert!(matches!(
            cache.compact_index().unwrap(),
            IndexCompaction::Compacted { .. }
        ));
    }
}
