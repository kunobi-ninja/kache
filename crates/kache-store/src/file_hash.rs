//! Persistent file fingerprints and opaque compiler memo records.

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::{Path, PathBuf};

pub const MIN_PERSISTED_HASH_BYTES: i64 = 64 * 1024;

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

    pub fn get_runtime_env_use(
        &self,
        content_hash: &str,
        var: &str,
    ) -> rusqlite::Result<Option<bool>> {
        self.db()
            .query_row(
                "SELECT has_runtime_use FROM source_env_runtime_uses
                 WHERE content_hash = ?1 AND env_var = ?2",
                params![content_hash, var],
                |row| row.get::<_, i64>(0).map(|value| value != 0),
            )
            .optional()
    }

    pub fn put_runtime_env_use(
        &self,
        content_hash: &str,
        var: &str,
        has_runtime_use: bool,
    ) -> rusqlite::Result<()> {
        self.db().execute(
            "INSERT OR REPLACE INTO source_env_runtime_uses
             (content_hash, env_var, has_runtime_use, updated_at)
             VALUES (?1, ?2, ?3, datetime('now'))",
            params![content_hash, var, i64::from(has_runtime_use)],
        )?;
        Ok(())
    }

    pub fn get_cc_preprocess_memo(
        &self,
        memo_key: &str,
    ) -> rusqlite::Result<Option<(String, String)>> {
        self.db()
            .query_row(
                "SELECT preprocessed_hash, inputs_json FROM cc_preprocess_memos
                 WHERE memo_key = ?1",
                params![memo_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
    }

    /// Return the stored schema and payload for `identity`, or `None` when absent.
    /// The caller validates the schema before interpreting the payload.
    pub fn get_input_prediction(&self, identity: &str) -> rusqlite::Result<Option<(u32, String)>> {
        self.db()
            .query_row(
                "SELECT schema, prediction_json FROM input_predictions WHERE identity = ?1",
                params![identity],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
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
             (identity, schema, crate_name, prediction_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, datetime('now'))",
            params![identity, schema, crate_name, prediction_json],
        )?;
        Ok(())
    }

    pub fn put_cc_preprocess_memo(
        &self,
        memo_key: &str,
        preprocessed_hash: &str,
        inputs_json: &str,
    ) -> rusqlite::Result<()> {
        self.db().execute(
            "INSERT OR REPLACE INTO cc_preprocess_memos
             (memo_key, preprocessed_hash, inputs_json, updated_at)
             VALUES (?1, ?2, ?3, datetime('now'))",
            params![memo_key, preprocessed_hash, inputs_json],
        )?;
        Ok(())
    }
}

pub fn ensure_file_hash_cache_schema(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS file_hashes (
            path       TEXT PRIMARY KEY,
            size       INTEGER NOT NULL,
            mtime_ns   INTEGER NOT NULL,
            ctime_ns   INTEGER NOT NULL DEFAULT 0,
            inode      INTEGER NOT NULL DEFAULT 0,
            hash       TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS cc_preprocess_memos (
            memo_key          TEXT PRIMARY KEY,
            preprocessed_hash TEXT NOT NULL,
            inputs_json       TEXT NOT NULL,
            updated_at        TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS input_predictions (
            identity        TEXT PRIMARY KEY,
            schema          INTEGER NOT NULL,
            crate_name      TEXT,
            prediction_json TEXT NOT NULL,
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS source_env_runtime_uses (
            content_hash    TEXT NOT NULL,
            env_var         TEXT NOT NULL,
            has_runtime_use INTEGER NOT NULL,
            updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (content_hash, env_var)
        );",
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
    Ok(())
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
    fn runtime_env_memo_persists_both_answers_for_each_content_and_variable() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(
            cache.get_runtime_env_use("source-a", "OUT_DIR").unwrap(),
            None
        );
        cache
            .put_runtime_env_use("source-a", "OUT_DIR", true)
            .unwrap();
        cache
            .put_runtime_env_use("source-a", "OTHER", false)
            .unwrap();
        cache
            .put_runtime_env_use("source-b", "OUT_DIR", false)
            .unwrap();
        drop(cache);

        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(
            cache.get_runtime_env_use("source-a", "OUT_DIR").unwrap(),
            Some(true)
        );
        assert_eq!(
            cache.get_runtime_env_use("source-a", "OTHER").unwrap(),
            Some(false)
        );
        assert_eq!(
            cache.get_runtime_env_use("source-b", "OUT_DIR").unwrap(),
            Some(false)
        );
        assert_eq!(
            cache.get_runtime_env_use("source-b", "OTHER").unwrap(),
            None
        );

        cache
            .put_runtime_env_use("source-a", "OUT_DIR", false)
            .unwrap();
        assert_eq!(
            cache.get_runtime_env_use("source-a", "OUT_DIR").unwrap(),
            Some(false)
        );
    }

    #[test]
    fn cc_memo_persists_and_replaces_only_the_requested_key() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("index.db");
        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(cache.get_cc_preprocess_memo("first").unwrap(), None);
        cache
            .put_cc_preprocess_memo("first", "digest-a", "inputs-a")
            .unwrap();
        cache
            .put_cc_preprocess_memo("second", "digest-b", "inputs-b")
            .unwrap();
        drop(cache);

        let cache = FileHashCache::open(&db_path).unwrap();
        assert_eq!(
            cache.get_cc_preprocess_memo("first").unwrap(),
            Some(("digest-a".into(), "inputs-a".into()))
        );
        assert_eq!(cache.get_cc_preprocess_memo("missing").unwrap(), None);
        cache
            .put_cc_preprocess_memo("first", "digest-c", "inputs-c")
            .unwrap();
        assert_eq!(
            cache.get_cc_preprocess_memo("first").unwrap(),
            Some(("digest-c".into(), "inputs-c".into()))
        );
        assert_eq!(
            cache.get_cc_preprocess_memo("second").unwrap(),
            Some(("digest-b".into(), "inputs-b".into()))
        );
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
