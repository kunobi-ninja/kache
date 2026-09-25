//! Persistent file fingerprints and opaque compiler memo records.

pub use crate::cc_memo::{CcPreprocessMemo, CcPreprocessMemoInput};
pub use crate::index_compaction::{IndexCompaction, IndexPageStats, index_page_stats};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::path::{Path, PathBuf};

pub const MIN_PERSISTED_HASH_BYTES: i64 = 64 * 1024;

/// How long a file must have been left alone before its hash may be memoised.
///
/// A memo row is trusted whenever the stamp matches. A file written, hashed and
/// then written again inside one timestamp tick keeps the same stamp with
/// different bytes, and a row recorded between the two writes would hand the
/// old hash to every later lookup. Filesystems tick coarsely: HFS+ in whole
/// seconds, FAT in two, ext4 at the kernel's coarse clock. Two seconds after
/// the last change, a further write lands on a later tick and changes the
/// stamp, so a row recorded then cannot be stale this way.
pub const HASH_SETTLE_NS: i64 = 2_000_000_000;

/// Whether `fingerprint`'s file last changed at least [`HASH_SETTLE_NS`]
/// before `now_ns`. The ctime counts as well as the mtime: tools that restore
/// an old mtime after writing cannot hold the ctime back.
pub fn stamp_is_settled(fingerprint: &FileFingerprint, now_ns: i64) -> bool {
    let changed = fingerprint.mtime_ns.max(fingerprint.ctime_ns);
    now_ns.saturating_sub(changed) >= HASH_SETTLE_NS
}

/// Paths per lookup statement, well under SQLite's bound-parameter limit.
const FILE_HASH_LOOKUP_CHUNK: usize = 256;

pub enum FileHashCache<'db> {
    Borrowed(&'db Connection),
    #[cfg(any(test, feature = "test-support"))]
    Owned(Connection),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FileFingerprint {
    pub path: String,
    pub size: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    /// Filesystem inode (0 on non-Unix / unavailable). Folded into the memo key
    /// so an in-place swap that preserves path+size+mtime+ctime but changes the
    /// inode (and content) can't return a stale memoized hash (kunobi-ninja/kache#324).
    pub inode: i64,
}

/// Result of a content-hash cache lookup that does NOT compute a blake3 — the
/// lock-narrowing seam for the daemon's `HashFiles` path (#281). The caller
/// hashes (`hash_file`) outside any store lock on a miss, then records via
/// [`FileHashCache::record_cached`].
pub enum FileHashLookup {
    /// Cached hash found — no hashing needed.
    Hit(String),
    /// Cache miss; hash the file then record under this fingerprint.
    NeedsHash(FileFingerprint),
    /// Too small to persist, or metadata unreadable — hash but don't cache.
    Uncacheable,
}

impl<'db> FileHashCache<'db> {
    #[cfg(any(test, feature = "test-support"))]
    pub fn open(index_db_path: &Path) -> Result<Self> {
        let db = Connection::open(index_db_path)
            .with_context(|| format!("opening file hash cache {}", index_db_path.display()))?;
        db.pragma_update(None, "busy_timeout", "5000")?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "NORMAL")?;
        ensure_file_hash_cache_schema(&db)?;
        Ok(Self::Owned(db))
    }

    pub fn db(&self) -> &Connection {
        match self {
            Self::Borrowed(db) => db,
            #[cfg(any(test, feature = "test-support"))]
            Self::Owned(db) => db,
        }
    }

    pub fn get(&self, fingerprint: &FileFingerprint) -> rusqlite::Result<Option<String>> {
        self.db()
            .query_row(
                "SELECT hash FROM file_hashes
                 WHERE path = ?1 AND size = ?2 AND mtime_ns = ?3 AND ctime_ns = ?4 AND inode = ?5",
                params![
                    fingerprint.path,
                    fingerprint.size,
                    fingerprint.mtime_ns,
                    fingerprint.ctime_ns,
                    fingerprint.inode
                ],
                |row| row.get(0),
            )
            .optional()
    }

    /// Memoised hashes for many files in one statement per chunk, keyed by
    /// path. A row counts only when its whole stamp matches, so a file that
    /// changed since it was recorded reads as absent, exactly as with [`get`].
    ///
    /// [`get`]: Self::get
    pub fn get_many(
        &self,
        fingerprints: &[&FileFingerprint],
    ) -> rusqlite::Result<std::collections::HashMap<String, String>> {
        let mut found = std::collections::HashMap::new();
        for chunk in fingerprints.chunks(FILE_HASH_LOOKUP_CHUNK) {
            let wanted: std::collections::HashSet<&FileFingerprint> =
                chunk.iter().copied().collect();
            let placeholders = vec!["?"; chunk.len()].join(",");
            let mut stmt = self.db().prepare_cached(&format!(
                "SELECT path, size, mtime_ns, ctime_ns, inode, hash FROM file_hashes
                 WHERE path IN ({placeholders})"
            ))?;
            let rows = stmt.query_map(
                rusqlite::params_from_iter(chunk.iter().map(|f| f.path.as_str())),
                |row| {
                    Ok((
                        FileFingerprint {
                            path: row.get(0)?,
                            size: row.get(1)?,
                            mtime_ns: row.get(2)?,
                            ctime_ns: row.get(3)?,
                            inode: row.get(4)?,
                        },
                        row.get::<_, String>(5)?,
                    ))
                },
            )?;
            for row in rows {
                let (stamp, hash) = row?;
                if wanted.contains(&stamp) {
                    found.insert(stamp.path, hash);
                }
            }
        }
        Ok(found)
    }

    pub fn put(&self, fingerprint: &FileFingerprint, hash: &str) -> rusqlite::Result<()> {
        self.db().execute(
            "INSERT OR REPLACE INTO file_hashes
             (path, size, mtime_ns, ctime_ns, inode, hash, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))",
            params![
                fingerprint.path,
                fingerprint.size,
                fingerprint.mtime_ns,
                fingerprint.ctime_ns,
                fingerprint.inode,
                hash
            ],
        )?;
        Ok(())
    }

    /// The stored classification of how the source with `content_hash` uses
    /// `var`. `scanner` is the caller's scanner version: a row recorded by any
    /// other version reads as absent, so a scanner fix never reuses an answer
    /// the old scanner gave for unchanged source. `use_kind` is opaque here.
    pub fn get_source_env_dep_use(
        &self,
        content_hash: &str,
        var: &str,
        scanner: u32,
    ) -> rusqlite::Result<Option<i64>> {
        self.db()
            .query_row(
                "SELECT use_kind FROM source_env_dep_uses
                 WHERE content_hash = ?1 AND env_var = ?2 AND scanner = ?3",
                params![content_hash, var, scanner],
                |row| row.get(0),
            )
            .optional()
    }

    pub fn put_source_env_dep_use(
        &self,
        content_hash: &str,
        var: &str,
        scanner: u32,
        use_kind: i64,
    ) -> rusqlite::Result<()> {
        self.db().execute(
            "INSERT OR REPLACE INTO source_env_dep_uses
             (content_hash, env_var, scanner, use_kind, updated_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            params![content_hash, var, scanner, use_kind],
        )?;
        Ok(())
    }

    /// Whether any entry, committed or not, was stored under `crate_name`
    /// for `unit`, Cargo's `-C metadata` hash, or with no unit recorded.
    ///
    /// Every cache key folds the crate name and that hash in, so `false`
    /// means no key this unit can produce is in the local store. A row with
    /// no unit (stored by an older kache, or by a path that never learns
    /// the unit) counts for every unit of its crate name. An empty `unit`
    /// asks about the crate name alone.
    pub fn has_entry_for_unit(&self, crate_name: &str, unit: &str) -> rusqlite::Result<bool> {
        if unit.is_empty() {
            return self.db().query_row(
                "SELECT EXISTS(SELECT 1 FROM entries WHERE crate_name = ?1)",
                params![crate_name],
                |row| row.get(0),
            );
        }
        self.db().query_row(
            "SELECT EXISTS(SELECT 1 FROM entries
                           WHERE crate_name = ?1 AND unit_id IN (?2, ''))",
            params![crate_name, unit],
            |row| row.get(0),
        )
    }

    /// Return the stored schema and payload for `identity`, or `None` when absent.
    /// The caller validates the schema before interpreting the payload.
    ///
    /// A read is a use: it refreshes the row's `last_used` stamp, at most once
    /// per [`INPUT_PREDICTION_TOUCH_INTERVAL_SECS`] so a build does not issue
    /// one write per unit.
    pub fn get_input_prediction(&self, identity: &str) -> rusqlite::Result<Option<(u32, String)>> {
        self.get_input_prediction_at(identity, unix_now())
    }

    fn get_input_prediction_at(
        &self,
        identity: &str,
        now: i64,
    ) -> rusqlite::Result<Option<(u32, String)>> {
        let row: Option<(u32, String, i64)> = self
            .db()
            .query_row(
                "SELECT schema, prediction_json, last_used FROM input_predictions
                 WHERE identity = ?1",
                params![identity],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let Some((schema, json, last_used)) = row else {
            return Ok(None);
        };
        // Best-effort: a stamp that cannot be written costs an early prune
        // and a repeated pre-pass, never the hit.
        if prediction_touch_due(last_used, now)
            && let Err(error) = self.db().execute(
                "UPDATE input_predictions SET last_used = ?2 WHERE identity = ?1",
                params![identity, now],
            )
        {
            tracing::debug!("input prediction touch failed: {error}");
        }
        Ok(Some((schema, json)))
    }

    pub fn put_input_prediction(
        &self,
        identity: &str,
        schema: u32,
        crate_name: Option<&str>,
        prediction_json: &str,
    ) -> rusqlite::Result<()> {
        self.db().execute(
            "INSERT OR REPLACE INTO input_predictions
             (identity, schema, crate_name, prediction_json, updated_at, last_used)
             VALUES (?1, ?2, ?3, ?4, datetime('now'), unixepoch())",
            params![identity, schema, crate_name, prediction_json],
        )?;
        Ok(())
    }

    /// Delete predictions no build has read or recorded for
    /// [`INPUT_PREDICTION_RETENTION_SECS`]. Run from the GC sweep and
    /// `doctor --repair`. Freed pages go to the freelist; index compaction
    /// returns them to disk.
    pub fn prune_input_predictions(&self) -> rusqlite::Result<usize> {
        self.prune_input_predictions_at(unix_now())
    }

    /// Delete file hash rows not written for [`FILE_HASH_RETENTION_SECS`].
    /// Run from the GC sweep and `doctor --repair`, like
    /// [`Self::prune_input_predictions`]. A lookup never refreshes a row, so
    /// that the hit path stays read-only; the price is one re-hash a month
    /// for a file read that whole time without changing.
    pub fn prune_file_hashes(&self) -> rusqlite::Result<usize> {
        self.prune_file_hashes_at(unix_now())
    }

    /// Rebuild an older store's `file_hashes` as a `WITHOUT ROWID` table,
    /// which keeps each path once instead of in the table and again in its
    /// primary key index: 27% smaller on a real 1.3-million-row table
    /// (kunobi-ninja/kache#1206). Copying takes the write lock for about a
    /// second per million rows, so it runs from the GC sweep after the prune,
    /// never when a build opens the index. Returns whether it rebuilt.
    pub fn rebuild_file_hashes_without_rowid(&self) -> rusqlite::Result<bool> {
        let tx = Transaction::new_unchecked(self.db(), TransactionBehavior::Immediate)?;
        if !file_hashes_has_rowid(&tx)? {
            return Ok(false);
        }
        tx.execute_batch(&format!(
            "CREATE TABLE file_hashes_rebuilt ({FILE_HASHES_COLUMNS}) WITHOUT ROWID;
             INSERT INTO file_hashes_rebuilt
                 (path, size, mtime_ns, ctime_ns, inode, hash, updated_at)
                 SELECT path, size, mtime_ns, ctime_ns, inode, hash, updated_at
                 FROM file_hashes;
             DROP TABLE file_hashes;
             ALTER TABLE file_hashes_rebuilt RENAME TO file_hashes;
             CREATE INDEX IF NOT EXISTS file_hashes_updated_at ON file_hashes(updated_at);"
        ))?;
        tx.commit()?;
        Ok(true)
    }

    fn prune_file_hashes_at(&self, now: i64) -> rusqlite::Result<usize> {
        // Built here, not when a wrapper opens the index, for the reason
        // given in `prune_input_predictions_at`.
        self.db().execute_batch(
            "CREATE INDEX IF NOT EXISTS file_hashes_updated_at
             ON file_hashes(updated_at)",
        )?;
        self.db().execute(
            "DELETE FROM file_hashes WHERE updated_at < datetime(?1, 'unixepoch')",
            params![now.saturating_sub(FILE_HASH_RETENTION_SECS)],
        )
    }

    fn prune_input_predictions_at(&self, now: i64) -> rusqlite::Result<usize> {
        // Built here, not when a wrapper upgrades the table: indexing a large
        // table takes the write lock for seconds, and this runs off the build.
        self.db().execute_batch(
            "CREATE INDEX IF NOT EXISTS input_predictions_last_used
             ON input_predictions(last_used)",
        )?;
        self.db().execute(
            "DELETE FROM input_predictions WHERE last_used < ?1",
            params![prediction_prune_cutoff(now)],
        )
    }
}

/// How long a file hash row is kept after it was last written. The memo is
/// only a saving: a row pruned while its file is still in use costs one
/// re-read of that file, at most once per retention window. Without a bound
/// the table kept every path any build ever hashed, so worktrees and
/// checkouts deleted long ago stayed in it for good (kunobi-ninja/kache#1206).
pub const FILE_HASH_RETENTION_SECS: i64 = 30 * 86_400;

/// A prediction hit refreshes `last_used` only when the stamp is this old.
pub const INPUT_PREDICTION_TOUCH_INTERVAL_SECS: i64 = 86_400;

/// How long an unused prediction is kept. The same window as the C/C++
/// preprocess memos: long enough for a branch left alone for a few weeks,
/// short enough that identities no build produces any more stop piling up.
pub const INPUT_PREDICTION_RETENTION_SECS: i64 = 30 * 86_400;

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

fn prediction_touch_due(last_used: i64, now: i64) -> bool {
    now.saturating_sub(last_used) >= INPUT_PREDICTION_TOUCH_INTERVAL_SECS
}

fn prediction_prune_cutoff(now: i64) -> i64 {
    now.saturating_sub(INPUT_PREDICTION_RETENTION_SECS)
}

fn input_predictions_have_last_used(db: &Connection) -> rusqlite::Result<bool> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('input_predictions') WHERE name = 'last_used')",
        [],
        |row| row.get(0),
    )
}

/// Add `last_used` to a table created before the column existed.
///
/// The default is the migration time as a literal: SQLite refuses an
/// expression default in `ADD COLUMN`, and a constant one is recorded in the
/// schema without rewriting a row, so the upgrade costs the same on an empty
/// table and a 200 MB one. Existing rows therefore read as used now, and the
/// first prune of them comes a full retention window later. `updated_at` is
/// not used as the starting stamp: it records the last write, and a
/// prediction for a dependency that never changes is read daily and written
/// once.
fn ensure_input_predictions_last_used(db: &Connection, now: i64) -> rusqlite::Result<()> {
    if input_predictions_have_last_used(db)? {
        return Ok(());
    }
    // Recheck under the writer lock: many wrappers open the index at once,
    // and a second ALTER fails with a duplicate column.
    let tx = Transaction::new_unchecked(db, TransactionBehavior::Immediate)?;
    if !input_predictions_have_last_used(&tx)? {
        tx.execute_batch(&format!(
            "ALTER TABLE input_predictions ADD COLUMN last_used INTEGER NOT NULL DEFAULT {now}"
        ))?;
    }
    tx.commit()
}

/// The columns of `file_hashes`, for the table a new store creates and for
/// the rebuild of an older one.
const FILE_HASHES_COLUMNS: &str = "
    path       TEXT PRIMARY KEY,
    size       INTEGER NOT NULL,
    mtime_ns   INTEGER NOT NULL,
    ctime_ns   INTEGER NOT NULL DEFAULT 0,
    inode      INTEGER NOT NULL DEFAULT 0,
    hash       TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))";

/// Whether `file_hashes` is still the rowid table older stores created. A
/// rowid table keys its rows by a hidden integer, so `path` sits in the table
/// and again in the automatic index that enforces its primary key.
fn file_hashes_has_rowid(db: &Connection) -> rusqlite::Result<bool> {
    let sql: Option<String> = db
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'file_hashes'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(sql.is_some_and(|sql| !sql.to_ascii_uppercase().contains("WITHOUT ROWID")))
}

pub fn ensure_file_hash_cache_schema(db: &Connection) -> rusqlite::Result<()> {
    crate::cc_memo::ensure_schema(db)?;
    crate::cc_memo::ensure_mapped_hash_schema(db)?;
    // An existing rowid table stays until GC housekeeping rebuilds it, off
    // the build path (`FileHashCache::rebuild_file_hashes_without_rowid`).
    db.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS file_hashes ({FILE_HASHES_COLUMNS}) WITHOUT ROWID;"
    ))?;
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS input_predictions (
            identity        TEXT PRIMARY KEY,
            schema          INTEGER NOT NULL,
            crate_name      TEXT,
            prediction_json TEXT NOT NULL,
            updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
            last_used       INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS source_env_dep_uses (
            content_hash TEXT NOT NULL,
            env_var      TEXT NOT NULL,
            scanner      INTEGER NOT NULL,
            use_kind     INTEGER NOT NULL,
            updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (content_hash, env_var)
        );
        -- Boolean answers from the first env-use scanner, which missed
        -- computed names and other spellings. Nothing reads them any more.
        DROP TABLE IF EXISTS source_env_runtime_uses;",
    )?;
    for column in [
        "ALTER TABLE file_hashes ADD COLUMN ctime_ns INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE file_hashes ADD COLUMN inode INTEGER NOT NULL DEFAULT 0",
    ] {
        if let Err(e) = db.execute_batch(column)
            && !e.to_string().contains("duplicate column name")
        {
            return Err(e);
        }
    }
    ensure_input_predictions_last_used(db, unix_now())
}

impl FileFingerprint {
    /// Identity of a file as the memo sees it. Also the cheapest available
    /// proof that an external tool did NOT rewrite a file across some
    /// operation: any in-place write bumps `mtime_ns`/`ctime_ns` (and a
    /// replace-by-rename changes `inode`), so an unchanged fingerprint means
    /// unchanged bytes. `restore_from_cache` reads it that way (#540).
    pub fn from_path(path: &Path) -> Result<Self> {
        let metadata = std::fs::metadata(path)
            .with_context(|| format!("reading metadata for {}", path.display()))?;
        let absolute_path = absolute_path(path);

        Ok(Self {
            path: absolute_path.to_string_lossy().into_owned(),
            size: i64::try_from(metadata.len()).unwrap_or(i64::MAX),
            mtime_ns: metadata_mtime_ns(&metadata),
            ctime_ns: metadata_ctime_ns(&metadata),
            inode: metadata_inode(&metadata),
        })
    }
}

pub fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

/// Filesystem inode number (0 where unavailable, e.g. non-Unix).
pub fn metadata_inode(metadata: &std::fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        i64::try_from(metadata.ino()).unwrap_or(i64::MAX)
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0
    }
}

pub fn metadata_mtime_ns(metadata: &std::fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        metadata_parts_ns(metadata.mtime(), metadata.mtime_nsec())
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        windows_filetime_ns(metadata.last_write_time())
    }

    #[cfg(not(any(unix, windows)))]
    {
        system_time_ns(metadata.modified().ok()).unwrap_or_default()
    }
}

pub fn metadata_ctime_ns(metadata: &std::fs::Metadata) -> i64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        metadata_parts_ns(metadata.ctime(), metadata.ctime_nsec())
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;

        windows_filetime_ns(metadata.creation_time())
    }

    #[cfg(not(any(unix, windows)))]
    {
        system_time_ns(metadata.created().ok()).unwrap_or_else(|| metadata_mtime_ns(metadata))
    }
}

#[cfg(unix)]
fn metadata_parts_ns(seconds: i64, nanoseconds: i64) -> i64 {
    seconds
        .saturating_mul(1_000_000_000)
        .saturating_add(nanoseconds)
}

#[cfg(any(windows, test))]
fn windows_filetime_ns(filetime_100ns: u64) -> i64 {
    const UNIX_EPOCH_FILETIME_100NS: u64 = 116_444_736_000_000_000;

    filetime_100ns
        .saturating_sub(UNIX_EPOCH_FILETIME_100NS)
        .saturating_mul(100)
        .min(i64::MAX as u64) as i64
}

#[cfg(any(test, not(any(unix, windows))))]
fn system_time_ns(time: Option<std::time::SystemTime>) -> Option<i64> {
    let duration = time?.duration_since(std::time::UNIX_EPOCH).ok()?;
    Some(i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
}

/// Hash a file using blake3.
pub fn hash_file(path: &Path) -> Result<String> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening {} for hashing", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    hasher
        .update_reader(file)
        .with_context(|| format!("reading {} for hashing", path.display()))?;
    Ok(hasher.finalize().to_hex().to_string())
}

impl FileHashCache<'_> {
    /// Cache lookup ONLY — reads the persistent hash cache, never computes a
    /// blake3. Lets a caller holding a coarse lock (the daemon's `Mutex<Store>`)
    /// release it before the expensive file read and re-take it only for the
    /// short record (#281). Mirrors `FileHasher::hash`'s fingerprint + min-size +
    /// cache-get logic exactly, so the cache key is identical.
    pub fn lookup_cached(&self, path: &Path) -> FileHashLookup {
        let cache = self;
        let fingerprint = match FileFingerprint::from_path(path) {
            Ok(fp) => fp,
            Err(e) => {
                tracing::debug!(
                    "file hash cache metadata lookup failed for {}: {e}",
                    path.display()
                );
                return FileHashLookup::Uncacheable;
            }
        };
        if fingerprint.size < MIN_PERSISTED_HASH_BYTES {
            return FileHashLookup::Uncacheable;
        }
        match cache.get(&fingerprint) {
            Ok(Some(hash)) => FileHashLookup::Hit(hash),
            Ok(None) => FileHashLookup::NeedsHash(fingerprint),
            Err(e) => {
                // Treat a lookup error as a miss — recompute rather than fail.
                tracing::debug!("file hash cache lookup failed for {}: {e}", path.display());
                FileHashLookup::NeedsHash(fingerprint)
            }
        }
    }
    /// Record a freshly-computed hash for `fingerprint` (the miss arm of
    /// [`Self::lookup_cached`]). Best-effort — a cache write failure is logged,
    /// not propagated.
    pub fn record_cached(&self, fingerprint: &FileFingerprint, hash: &str) {
        if let Err(e) = self.put(fingerprint, hash) {
            tracing::debug!("file hash cache update failed: {e}");
        }
    }
    /// Record a hash the caller already knows for `fingerprint` — without
    /// reading the file (kunobi-ninja/kache#540).
    ///
    /// The caller must have established that hash for THAT fingerprint, not
    /// merely for that path: the row is only ever served back on an exact
    /// fingerprint match, so a fingerprint captured at the moment the content
    /// was known stays a true statement even if the file changes a moment
    /// later — the changed file simply misses and gets hashed. Re-stating the
    /// path here instead would pair the new file's fingerprint with the old
    /// file's hash.
    ///
    /// Honors the same size floor as `FileHasher::hash`, which would not consult
    /// the memo for a smaller file anyway.
    pub fn record_verified(&self, fingerprint: &FileFingerprint, hash: &str) {
        if fingerprint.size < MIN_PERSISTED_HASH_BYTES {
            return;
        }
        self.record_cached(fingerprint, hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The table as stores before #1206 created it.
    const ROWID_FILE_HASHES: &str = "CREATE TABLE file_hashes (
        path       TEXT PRIMARY KEY,
        size       INTEGER NOT NULL,
        mtime_ns   INTEGER NOT NULL,
        ctime_ns   INTEGER NOT NULL DEFAULT 0,
        inode      INTEGER NOT NULL DEFAULT 0,
        hash       TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )";

    #[test]
    fn a_new_store_keeps_file_hashes_without_rowid() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        assert!(!file_hashes_has_rowid(&db).unwrap());
        let cache = FileHashCache::Borrowed(&db);
        assert!(!cache.rebuild_file_hashes_without_rowid().unwrap());
    }

    #[test]
    fn an_older_file_hashes_table_is_rebuilt_once_with_its_rows() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch(ROWID_FILE_HASHES).unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        assert!(
            file_hashes_has_rowid(&db).unwrap(),
            "opening does not rebuild"
        );
        db.execute(
            "INSERT INTO file_hashes (path, size, mtime_ns, ctime_ns, inode, hash, updated_at)
             VALUES ('/a', 1, 2, 3, 4, 'h', '2026-09-01 00:00:00')",
            [],
        )
        .unwrap();

        let cache = FileHashCache::Borrowed(&db);
        assert!(cache.rebuild_file_hashes_without_rowid().unwrap());
        assert!(!file_hashes_has_rowid(&db).unwrap());
        let row: (String, i64, i64, i64, i64, String, String) = db
            .query_row(
                "SELECT path, size, mtime_ns, ctime_ns, inode, hash, updated_at FROM file_hashes",
                [],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            row,
            (
                "/a".into(),
                1,
                2,
                3,
                4,
                "h".into(),
                "2026-09-01 00:00:00".into()
            )
        );
        let indexed: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master
                 WHERE type = 'index' AND name = 'file_hashes_updated_at')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(indexed, "the prune's index comes back");
        assert!(!cache.rebuild_file_hashes_without_rowid().unwrap(), "once");
    }

    #[test]
    fn the_file_hash_window_is_thirty_days() {
        assert_eq!(FILE_HASH_RETENTION_SECS, 2_592_000);
    }

    #[test]
    fn file_hash_rows_are_pruned_a_month_after_they_were_written() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        let cache = FileHashCache::Borrowed(&db);
        // November 2023: the rows are years old by the real clock.
        let now = 1_700_000_000_i64;
        for (path, age) in [
            ("/kept", FILE_HASH_RETENTION_SECS - 60),
            ("/boundary", FILE_HASH_RETENTION_SECS),
            ("/old", FILE_HASH_RETENTION_SECS + 60),
        ] {
            db.execute(
                "INSERT INTO file_hashes (path, size, mtime_ns, hash, updated_at)
                 VALUES (?1, 1, 1, 'h', datetime(?2, 'unixepoch'))",
                rusqlite::params![path, now - age],
            )
            .unwrap();
        }
        assert_eq!(cache.prune_file_hashes_at(now).unwrap(), 1);
        let mut left: Vec<String> = db
            .prepare("SELECT path FROM file_hashes ORDER BY path")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        left.sort();
        assert_eq!(
            left,
            ["/boundary", "/kept"],
            "a row exactly a month old stays"
        );
        // A write refreshes the row: `put` stamps it now.
        assert_eq!(
            cache.prune_file_hashes().unwrap(),
            2,
            "real clock: both are years old"
        );
    }

    fn stamp(path: &str, mtime_ns: i64, ctime_ns: i64) -> FileFingerprint {
        FileFingerprint {
            path: path.to_string(),
            size: 10,
            mtime_ns,
            ctime_ns,
            inode: 7,
        }
    }

    #[test]
    fn a_stamp_settles_exactly_one_window_after_its_last_change() {
        let changed = 1_000_000_000_000;
        let file = stamp("/h.h", changed, changed);
        assert!(!stamp_is_settled(&file, changed));
        assert!(!stamp_is_settled(&file, changed + HASH_SETTLE_NS - 1));
        assert!(stamp_is_settled(&file, changed + HASH_SETTLE_NS));
        // A clock behind the file (network filesystems) is not settled.
        assert!(!stamp_is_settled(&file, changed - 1));
    }

    #[test]
    fn a_recent_ctime_holds_back_a_file_whose_mtime_was_restored() {
        // `touch -r`, `cp -p` and rsync write new bytes, then put an old mtime
        // back. Only the ctime shows the write.
        let now = 1_000_000_000_000;
        let old = now - 10 * HASH_SETTLE_NS;
        assert!(!stamp_is_settled(&stamp("/h.h", old, now - 1), now));
        assert!(stamp_is_settled(&stamp("/h.h", old, old), now));
    }

    #[test]
    fn many_lookups_return_only_rows_whose_whole_stamp_matches() {
        let db = Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        let cache = FileHashCache::Borrowed(&db);
        let same = stamp("/same.h", 1, 1);
        let moved = stamp("/moved.h", 1, 1);
        cache.put(&same, "h-same").unwrap();
        cache.put(&moved, "h-moved").unwrap();
        let mut rewritten = moved.clone();
        rewritten.mtime_ns = 2;
        let absent = stamp("/absent.h", 1, 1);

        let found = cache.get_many(&[&same, &rewritten, &absent]).unwrap();
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found["/same.h"], "h-same");
        assert!(
            !found.contains_key("/moved.h"),
            "a changed stamp reads as absent"
        );
    }

    #[test]
    fn many_lookups_span_more_paths_than_one_statement_binds() {
        let db = Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        let cache = FileHashCache::Borrowed(&db);
        let stamps: Vec<FileFingerprint> = (0..FILE_HASH_LOOKUP_CHUNK * 2 + 3)
            .map(|i| stamp(&format!("/h{i}.h"), 1, 1))
            .collect();
        for (i, s) in stamps.iter().enumerate() {
            cache.put(s, &format!("hash{i}")).unwrap();
        }
        let refs: Vec<&FileFingerprint> = stamps.iter().collect();
        let found = cache.get_many(&refs).unwrap();
        assert_eq!(found.len(), stamps.len());
        assert_eq!(found["/h0.h"], "hash0");
        let last = stamps.len() - 1;
        assert_eq!(found[&format!("/h{last}.h")], format!("hash{last}"));
    }

    #[test]
    fn has_entry_for_unit_sees_only_that_crate_and_unit() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE entries (cache_key TEXT PRIMARY KEY, crate_name TEXT NOT NULL,
                                   unit_id TEXT NOT NULL DEFAULT '');",
        )
        .unwrap();
        let cache = FileHashCache::Borrowed(&db);
        assert!(!cache.has_entry_for_unit("gpui", "").unwrap());
        assert!(!cache.has_entry_for_unit("gpui", "u1").unwrap());
        db.execute(
            "INSERT INTO entries (cache_key, crate_name, unit_id) VALUES ('k', 'gpui_base', 'u1')",
            [],
        )
        .unwrap();
        assert!(!cache.has_entry_for_unit("gpui", "u1").unwrap());
        assert!(cache.has_entry_for_unit("gpui_base", "u1").unwrap());
        assert!(
            cache.has_entry_for_unit("gpui_base", "").unwrap(),
            "any unit"
        );
        assert!(
            !cache.has_entry_for_unit("gpui_base", "u2").unwrap(),
            "another unit of the same crate name is absent"
        );
        db.execute(
            "INSERT INTO entries (cache_key, crate_name) VALUES ('k2', 'build_script_build')",
            [],
        )
        .unwrap();
        assert!(
            cache
                .has_entry_for_unit("build_script_build", "u9")
                .unwrap(),
            "a row with no unit stands for every unit of its name"
        );
    }

    #[test]
    fn fingerprint_preserves_the_filesystem_identity() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("artifact.rlib");
        std::fs::write(&file, vec![7; 65_537]).unwrap();
        #[cfg(any(unix, windows))]
        let metadata = std::fs::metadata(&file).unwrap();
        let fingerprint = FileFingerprint::from_path(&file).unwrap();
        assert_eq!(fingerprint.path, file.to_string_lossy());
        assert_eq!(fingerprint.size, 65_537);

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(fingerprint.inode, metadata.ino() as i64);
            assert_eq!(
                fingerprint.mtime_ns,
                metadata.mtime() * 1_000_000_000 + metadata.mtime_nsec()
            );
            assert_eq!(
                fingerprint.ctime_ns,
                metadata.ctime() * 1_000_000_000 + metadata.ctime_nsec()
            );
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            assert_eq!(fingerprint.inode, 0);
            assert_eq!(
                fingerprint.mtime_ns,
                ((metadata.last_write_time() - 116_444_736_000_000_000) * 100) as i64
            );
            assert_eq!(
                fingerprint.ctime_ns,
                ((metadata.creation_time() - 116_444_736_000_000_000) * 100) as i64
            );
        }
        assert_eq!(
            absolute_path(Path::new("relative.rlib")),
            std::env::current_dir().unwrap().join("relative.rlib")
        );
    }

    #[test]
    fn windows_filetime_converts_epoch_units_and_saturates() {
        assert_eq!(windows_filetime_ns(0), 0);
        assert_eq!(windows_filetime_ns(116_444_735_999_999_999), 0);
        assert_eq!(windows_filetime_ns(116_444_736_000_000_000), 0);
        assert_eq!(windows_filetime_ns(116_444_736_000_000_001), 100);
        assert_eq!(windows_filetime_ns(116_444_736_012_345_678), 1_234_567_800);
        assert_eq!(windows_filetime_ns(u64::MAX), i64::MAX);
    }

    #[test]
    fn system_time_conversion_rejects_missing_or_pre_epoch_values() {
        use std::time::{Duration, UNIX_EPOCH};

        // Windows represents SystemTime in 100 ns ticks.
        assert_eq!(system_time_ns(None), None);
        assert_eq!(
            system_time_ns(Some(UNIX_EPOCH - Duration::from_nanos(100))),
            None
        );
        assert_eq!(system_time_ns(Some(UNIX_EPOCH)), Some(0));
        assert_eq!(
            system_time_ns(Some(UNIX_EPOCH + Duration::new(1, 234_567_800))),
            Some(1_234_567_800)
        );
        assert_eq!(
            system_time_ns(Some(UNIX_EPOCH + Duration::from_secs(9_223_372_037))),
            Some(i64::MAX)
        );
    }

    #[test]
    fn env_dep_use_memo_persists_each_answer_per_content_variable_and_scanner() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(
            cache
                .get_source_env_dep_use("source-a", "OUT_DIR", 2)
                .unwrap(),
            None
        );
        cache
            .put_source_env_dep_use("source-a", "OUT_DIR", 2, 1)
            .unwrap();
        cache
            .put_source_env_dep_use("source-a", "OTHER", 2, 0)
            .unwrap();
        cache
            .put_source_env_dep_use("source-b", "OUT_DIR", 2, 2)
            .unwrap();
        drop(cache);

        let cache = FileHashCache::open(&db_path).unwrap();
        let get = |content: &str, var: &str, scanner: u32| {
            cache.get_source_env_dep_use(content, var, scanner).unwrap()
        };
        assert_eq!(get("source-a", "OUT_DIR", 2), Some(1));
        assert_eq!(get("source-a", "OTHER", 2), Some(0));
        assert_eq!(get("source-b", "OUT_DIR", 2), Some(2));
        assert_eq!(get("source-b", "OTHER", 2), None);
        // Another scanner version never sees these answers.
        assert_eq!(get("source-a", "OUT_DIR", 1), None);
        assert_eq!(get("source-a", "OUT_DIR", 3), None);

        // A newer scanner's answer replaces the row for the same content.
        cache
            .put_source_env_dep_use("source-a", "OUT_DIR", 3, 2)
            .unwrap();
        assert_eq!(get("source-a", "OUT_DIR", 3), Some(2));
        assert_eq!(get("source-a", "OUT_DIR", 2), None);
    }

    #[test]
    fn schema_drops_the_boolean_env_use_memo() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE source_env_runtime_uses (
                content_hash    TEXT NOT NULL,
                env_var         TEXT NOT NULL,
                has_runtime_use INTEGER NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (content_hash, env_var)
            );
            INSERT INTO source_env_runtime_uses (content_hash, env_var, has_runtime_use)
            VALUES ('source-a', 'OUT_DIR', 0);",
        )
        .unwrap();
        ensure_file_hash_cache_schema(&conn).unwrap();
        let tables = |name: &str| -> i64 {
            conn.query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                params![name],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(tables("source_env_runtime_uses"), 0);
        assert_eq!(tables("source_env_dep_uses"), 1);
    }

    #[test]
    fn input_prediction_preserves_the_callers_schema_and_opaque_payload() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(cache.get_input_prediction("first").unwrap(), None);
        cache
            .put_input_prediction("first", 17, Some("example"), "payload-a")
            .unwrap();
        cache
            .put_input_prediction("second", 42, None, "payload-b")
            .unwrap();
        drop(cache);

        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(
            cache.get_input_prediction("first").unwrap(),
            Some((17, "payload-a".into()))
        );
        assert_eq!(cache.get_input_prediction("missing").unwrap(), None);
        cache
            .put_input_prediction("first", 18, None, "payload-c")
            .unwrap();
        assert_eq!(
            cache.get_input_prediction("first").unwrap(),
            Some((18, "payload-c".into()))
        );
        assert_eq!(
            cache.get_input_prediction("second").unwrap(),
            Some((42, "payload-b".into()))
        );
    }

    fn prediction_last_used(cache: &FileHashCache<'_>, identity: &str) -> i64 {
        cache
            .db()
            .query_row(
                "SELECT last_used FROM input_predictions WHERE identity = ?1",
                params![identity],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn set_prediction_last_used(cache: &FileHashCache<'_>, identity: &str, last_used: i64) {
        cache
            .db()
            .execute(
                "UPDATE input_predictions SET last_used = ?2 WHERE identity = ?1",
                params![identity, last_used],
            )
            .unwrap();
    }

    /// The table as every release before `last_used` created it.
    fn create_legacy_input_predictions(db: &Connection) {
        db.execute_batch(
            "CREATE TABLE input_predictions (
                identity        TEXT PRIMARY KEY,
                schema          INTEGER NOT NULL,
                crate_name      TEXT,
                prediction_json TEXT NOT NULL,
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
            INSERT INTO input_predictions (identity, schema, crate_name, prediction_json, updated_at)
            VALUES ('old-a', 3, 'alpha', 'payload-a', '2020-01-01 00:00:00'),
                   ('old-b', 4, NULL, 'payload-b', '2020-01-02 00:00:00');",
        )
        .unwrap();
    }

    #[test]
    fn input_prediction_windows_are_pinned() {
        assert_eq!(INPUT_PREDICTION_TOUCH_INTERVAL_SECS, 86_400);
        assert_eq!(INPUT_PREDICTION_RETENTION_SECS, 2_592_000);
        assert!(!prediction_touch_due(1_000_000, 1_000_000 + 86_399));
        assert!(prediction_touch_due(1_000_000, 1_000_000 + 86_400));
        // A stamp ahead of the clock is never due, and never overflows.
        assert!(!prediction_touch_due(2_000_000, 1_000_000));
        assert!(!prediction_touch_due(i64::MAX, i64::MIN));
        assert_eq!(prediction_prune_cutoff(3_000_000), 408_000);
        assert_eq!(prediction_prune_cutoff(i64::MIN), i64::MIN);
    }

    #[test]
    fn input_prediction_hit_refreshes_last_used_at_most_once_per_interval() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        cache
            .put_input_prediction("unit", 1, None, "payload")
            .unwrap();
        let now = 1_900_000_000;
        set_prediction_last_used(&cache, "unit", now - 86_399);
        assert_eq!(
            cache.get_input_prediction_at("unit", now).unwrap(),
            Some((1, "payload".into()))
        );
        assert_eq!(prediction_last_used(&cache, "unit"), now - 86_399);

        set_prediction_last_used(&cache, "unit", now - 86_400);
        assert_eq!(
            cache.get_input_prediction_at("unit", now).unwrap(),
            Some((1, "payload".into()))
        );
        assert_eq!(prediction_last_used(&cache, "unit"), now);

        // A second hit inside the interval writes nothing.
        cache.get_input_prediction_at("unit", now + 5).unwrap();
        assert_eq!(prediction_last_used(&cache, "unit"), now);
        // A miss touches nothing and other rows are left alone.
        cache
            .put_input_prediction("other", 1, None, "payload")
            .unwrap();
        set_prediction_last_used(&cache, "other", 7);
        assert_eq!(cache.get_input_prediction_at("absent", now).unwrap(), None);
        cache.get_input_prediction_at("unit", now + 86_400).unwrap();
        assert_eq!(prediction_last_used(&cache, "other"), 7);
    }

    #[test]
    fn input_prediction_read_and_record_stamp_the_wall_clock() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        let before = unix_now();
        assert!(before > 1_700_000_000);
        cache
            .put_input_prediction("unit", 1, None, "payload")
            .unwrap();
        assert!(prediction_last_used(&cache, "unit") >= before);

        set_prediction_last_used(&cache, "unit", 1);
        cache.get_input_prediction("unit").unwrap();
        assert!(prediction_last_used(&cache, "unit") >= before);

        // Recording again replaces the row and restamps it.
        set_prediction_last_used(&cache, "unit", 1);
        cache
            .put_input_prediction("unit", 2, None, "payload-2")
            .unwrap();
        assert!(prediction_last_used(&cache, "unit") >= before);
    }

    #[test]
    fn prune_input_predictions_removes_only_rows_unused_past_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        // Empty table, as on every store with predictions off.
        assert_eq!(cache.prune_input_predictions().unwrap(), 0);

        let now = 1_900_000_000;
        for identity in ["expired", "boundary", "fresh"] {
            cache
                .put_input_prediction(identity, 1, None, "payload")
                .unwrap();
        }
        set_prediction_last_used(&cache, "expired", now - 2_592_001);
        set_prediction_last_used(&cache, "boundary", now - 2_592_000);
        set_prediction_last_used(&cache, "fresh", now);
        assert_eq!(cache.prune_input_predictions_at(now).unwrap(), 1);
        assert_eq!(cache.get_input_prediction_at("expired", now).unwrap(), None);
        assert!(
            cache
                .get_input_prediction_at("boundary", now - 86_400)
                .unwrap()
                .is_some()
        );
        assert!(
            cache
                .get_input_prediction_at("fresh", now)
                .unwrap()
                .is_some()
        );
        assert_eq!(cache.prune_input_predictions_at(now).unwrap(), 0);

        let indexed: bool = cache
            .db()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index'
                 AND name = 'input_predictions_last_used')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(indexed);
    }

    #[test]
    fn prune_input_predictions_uses_the_wall_clock() {
        let dir = tempfile::tempdir().unwrap();
        let cache = FileHashCache::open(&dir.path().join("index.db")).unwrap();
        cache
            .put_input_prediction("ancient", 1, None, "payload")
            .unwrap();
        cache
            .put_input_prediction("live", 1, None, "payload")
            .unwrap();
        set_prediction_last_used(&cache, "ancient", 1);
        assert_eq!(cache.prune_input_predictions().unwrap(), 1);
        assert!(cache.get_input_prediction("live").unwrap().is_some());
        assert_eq!(cache.get_input_prediction("ancient").unwrap(), None);
    }

    #[test]
    fn schema_upgrade_adds_last_used_and_keeps_existing_predictions() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let db = Connection::open(&db_path).unwrap();
        create_legacy_input_predictions(&db);
        assert!(!input_predictions_have_last_used(&db).unwrap());

        ensure_input_predictions_last_used(&db, 1_700_000_000).unwrap();
        assert!(input_predictions_have_last_used(&db).unwrap());
        // A second upgrade changes nothing, including the recorded default.
        ensure_input_predictions_last_used(&db, 1_700_000_999).unwrap();
        ensure_file_hash_cache_schema(&db).unwrap();
        drop(db);

        let cache = FileHashCache::open(&db_path).unwrap();
        // Existing rows read as used at the upgrade, not at their last write.
        assert_eq!(prediction_last_used(&cache, "old-a"), 1_700_000_000);
        assert_eq!(prediction_last_used(&cache, "old-b"), 1_700_000_000);
        assert_eq!(
            cache
                .get_input_prediction_at("old-a", 1_700_000_000)
                .unwrap(),
            Some((3, "payload-a".into()))
        );
        assert_eq!(
            cache
                .get_input_prediction_at("old-b", 1_700_000_000)
                .unwrap(),
            Some((4, "payload-b".into()))
        );
        // They outlive a prune until a full window after the upgrade.
        assert_eq!(
            cache
                .prune_input_predictions_at(1_700_000_000 + 2_592_000)
                .unwrap(),
            0
        );
        assert_eq!(
            cache
                .prune_input_predictions_at(1_700_000_000 + 2_592_001)
                .unwrap(),
            2
        );
        // A row recorded after the upgrade carries its own stamp.
        cache
            .put_input_prediction("new", 5, None, "payload-n")
            .unwrap();
        assert!(prediction_last_used(&cache, "new") > 1_700_000_999);
    }

    #[test]
    fn schema_upgrade_survives_many_connections_opening_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        {
            let db = Connection::open(&db_path).unwrap();
            db.pragma_update(None, "journal_mode", "WAL").unwrap();
            create_legacy_input_predictions(&db);
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let db_path = db_path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let db = Connection::open(&db_path).unwrap();
                    db.pragma_update(None, "busy_timeout", "5000").unwrap();
                    barrier.wait();
                    ensure_input_predictions_last_used(&db, 1_700_000_000)
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }
        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(prediction_last_used(&cache, "old-a"), 1_700_000_000);
        assert_eq!(prediction_last_used(&cache, "old-b"), 1_700_000_000);
    }

    #[test]
    fn file_hash_memo_key_includes_inode() {
        // An in-place swap that preserves path+size+mtime+ctime but changes the
        // inode (and content) must NOT return a stale memoized hash
        // (kunobi-ninja/kache#324).
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        ensure_file_hash_cache_schema(&conn).unwrap();
        let cache = FileHashCache::Borrowed(&conn);

        let fp = |inode: i64| FileFingerprint {
            path: "/x/lib.rlib".to_string(),
            size: 100,
            mtime_ns: 1,
            ctime_ns: 2,
            inode,
        };

        cache.put(&fp(10), "hash_for_inode_10").unwrap();
        assert_eq!(
            cache.get(&fp(10)).unwrap().as_deref(),
            Some("hash_for_inode_10")
        );
        assert_eq!(
            cache.get(&fp(20)).unwrap(),
            None,
            "a different inode (same path/size/mtime/ctime) must miss the memo"
        );
    }

    #[test]
    fn test_hash_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rs");
        std::fs::write(&file, b"fn main() {}").unwrap();

        let hash = hash_file(&file).unwrap();
        assert_eq!(hash.len(), 64); // blake3 hex is 64 chars

        // Same content = same hash
        let file2 = dir.path().join("test2.rs");
        std::fs::write(&file2, b"fn main() {}").unwrap();
        let hash2 = hash_file(&file2).unwrap();
        assert_eq!(hash, hash2);

        // Different content = different hash
        let file3 = dir.path().join("test3.rs");
        std::fs::write(&file3, b"fn main() { println!(\"hello\"); }").unwrap();
        let hash3 = hash_file(&file3).unwrap();
        assert_ne!(hash, hash3);

        // Larger than blake3's streaming read buffer so the digest spans
        // multiple reads instead of relying on a single in-memory buffer.
        let large = dir.path().join("large.rlib");
        let large_bytes: Vec<u8> = (0..256 * 1024 + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        std::fs::write(&large, &large_bytes).unwrap();
        assert_eq!(
            hash_file(&large).unwrap(),
            blake3::hash(&large_bytes).to_hex().to_string()
        );
    }

    #[test]
    fn lookup_cached_too_small_or_unreadable_is_uncacheable() {
        // A sub-threshold file and an unreadable path both yield Uncacheable
        // from the persistent cache: the first via the min-size guard, the
        // second via the metadata-read failure arm.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let small = dir.path().join("small.rs");
        std::fs::write(&small, b"fn main() {}").unwrap();

        let fh = FileHashCache::open(&db_path).unwrap();
        assert!(matches!(
            fh.lookup_cached(&small),
            FileHashLookup::Uncacheable
        ));
        // Nonexistent path -> FileFingerprint::from_path errors -> Uncacheable.
        assert!(matches!(
            fh.lookup_cached(&dir.path().join("nope.rlib")),
            FileHashLookup::Uncacheable
        ));
    }

    #[test]
    fn lookup_cached_miss_then_record_then_hit_roundtrips() {
        // The daemon's lock-narrowing seam: a large file first reports
        // NeedsHash (miss), then after record_cached() a subsequent
        // lookup_cached() returns Hit with the recorded digest — without ever
        // computing a blake3 in lookup_cached itself. Covers NeedsHash, the
        // record_cached put arm, and the Hit arm.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let file = dir.path().join("large.rlib");
        std::fs::write(&file, vec![7u8; 70 * 1024]).unwrap();

        let fh = FileHashCache::open(&db_path).unwrap();
        let fp = match fh.lookup_cached(&file) {
            FileHashLookup::NeedsHash(fp) => fp,
            _ => panic!("expected NeedsHash on first lookup"),
        };
        fh.record_cached(&fp, "cafef00d");

        match fh.lookup_cached(&file) {
            FileHashLookup::Hit(h) => assert_eq!(h, "cafef00d"),
            _ => panic!("expected Hit after record_cached"),
        }
    }

    #[test]
    fn record_verified_honors_the_persistence_floor() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let below = dir.path().join("below.rlib");
        let at_floor = dir.path().join("at-floor.rlib");
        std::fs::write(&below, vec![1u8; 65_535]).unwrap();
        std::fs::write(&at_floor, vec![2u8; 65_536]).unwrap();

        let hasher = FileHashCache::open(&db_path).unwrap();
        hasher.record_verified(&FileFingerprint::from_path(&below).unwrap(), "below");
        hasher.record_verified(&FileFingerprint::from_path(&at_floor).unwrap(), "at-floor");

        assert!(matches!(
            hasher.lookup_cached(&below),
            FileHashLookup::Uncacheable
        ));
        assert!(matches!(
            hasher.lookup_cached(&at_floor),
            FileHashLookup::Hit(hash) if hash == "at-floor"
        ));

        let conn = rusqlite::Connection::open(&db_path).unwrap();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM file_hashes", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1, "sub-threshold fingerprints must not be stored");
    }
}
