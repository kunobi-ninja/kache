use anyhow::{Context, Result};
use rusqlite::{Connection, Error as SqlError, ErrorCode, params};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::config::Config;

/// Exclude a directory from Time Machine backups and Spotlight indexing.
#[cfg(target_os = "macos")]
pub(crate) fn exclude_from_indexing(dir: &Path) {
    // Time Machine: sets com.apple.metadata:com_apple_backup_excludeItem xattr
    let _ = std::process::Command::new("tmutil")
        .args(["addexclusion", &dir.display().to_string()])
        .output();

    // Spotlight: .metadata_never_index sentinel
    let sentinel = dir.join(".metadata_never_index");
    if !sentinel.exists() {
        let _ = fs::File::create(&sentinel);
    }
}

/// Metadata stored alongside cached artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMeta {
    pub cache_key: String,
    pub crate_name: String,
    pub crate_types: Vec<String>,
    pub files: Vec<CachedFile>,
    pub stdout: String,
    pub stderr: String,
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub profile: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedFile {
    /// Filename relative to the cache entry directory
    pub name: String,
    /// Size in bytes
    pub size: u64,
    /// blake3 hash of file content
    pub hash: String,
}

/// The local content-addressed store.
pub struct Store {
    config: Config,
    db: Connection,
}

/// Lock guard for a cache key. Dropping it releases the lock.
pub struct KeyLock {
    path: PathBuf,
}

impl Drop for KeyLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Compute a content hash from a set of blob hashes.
/// Sorts hashes for determinism, then blake3-hashes the concatenation.
/// Returns a 16-char hex prefix.
fn compute_content_hash(file_hashes: &[&str]) -> String {
    let mut sorted: Vec<&str> = file_hashes.to_vec();
    sorted.sort();
    let mut h = blake3::Hasher::new();
    for hash in &sorted {
        h.update(hash.as_bytes());
    }
    h.finalize().to_hex()[..16].to_string()
}

const STORE_OPEN_MAX_ATTEMPTS: u32 = 6;
const STORE_OPEN_RETRY_DELAYS_MS: [u64; 5] = [25, 50, 100, 200, 250];

fn sqlite_open_retry_delay(attempt: u32) -> Duration {
    let idx = attempt.saturating_sub(1) as usize;
    Duration::from_millis(*STORE_OPEN_RETRY_DELAYS_MS.get(idx).unwrap_or(&250))
}

fn is_retryable_sqlite_open_error(err: &SqlError) -> bool {
    match err {
        SqlError::SqliteFailure(code, _) => matches!(
            code.code,
            ErrorCode::CannotOpen
                | ErrorCode::DatabaseBusy
                | ErrorCode::DatabaseLocked
                | ErrorCode::SystemIoFailure
        ),
        _ => false,
    }
}

fn initialize_db(db: &Connection) -> rusqlite::Result<()> {
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    // Let concurrent writers retry for up to 5 s instead of failing immediately
    // with SQLITE_BUSY -- critical when 300+ wrapper processes hit the DB in parallel.
    db.pragma_update(None, "busy_timeout", "5000")?;

    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS entries (
            cache_key TEXT PRIMARY KEY,
            crate_name TEXT NOT NULL,
            size INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_accessed TEXT NOT NULL DEFAULT (datetime('now')),
            hit_count INTEGER NOT NULL DEFAULT 0,
            committed INTEGER NOT NULL DEFAULT 0
        );",
    )?;

    // Migrations (idempotent -- ignore "duplicate column" errors)
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN crate_type TEXT NOT NULL DEFAULT ''");
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN profile TEXT NOT NULL DEFAULT ''");
    let _ =
        db.execute_batch("ALTER TABLE entries ADD COLUMN num_features INTEGER NOT NULL DEFAULT 0");
    let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN content_hash TEXT");

    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS blobs (
            hash     TEXT PRIMARY KEY,
            size     INTEGER NOT NULL,
            refcount INTEGER NOT NULL DEFAULT 1
        );",
    )?;

    Ok(())
}

fn open_index_db(db_path: &Path) -> Result<Connection> {
    let mut last_error: Option<SqlError> = None;

    for attempt in 1..=STORE_OPEN_MAX_ATTEMPTS {
        match Connection::open(db_path).and_then(|db| {
            initialize_db(&db)?;
            Ok(db)
        }) {
            Ok(db) => return Ok(db),
            Err(err)
                if attempt < STORE_OPEN_MAX_ATTEMPTS && is_retryable_sqlite_open_error(&err) =>
            {
                let delay = sqlite_open_retry_delay(attempt);
                tracing::debug!(
                    path = %db_path.display(),
                    attempt,
                    ?delay,
                    "retrying transient SQLite open failure: {err}"
                );
                last_error = Some(err);
                std::thread::sleep(delay);
            }
            Err(err) => {
                last_error = Some(err);
                break;
            }
        }
    }

    Err(last_error
        .expect("open_index_db must record an error before returning")
        .into())
}

impl Store {
    pub fn open(config: &Config) -> Result<Self> {
        fs::create_dir_all(&config.cache_dir)
            .with_context(|| format!("creating cache directory {}", config.cache_dir.display()))?;
        let store_dir = config.store_dir();
        fs::create_dir_all(&store_dir)
            .with_context(|| format!("creating store directory {}", store_dir.display()))?;

        let db_path = config.index_db_path();
        let db = open_index_db(&db_path)
            .with_context(|| format!("opening index database {}", db_path.display()))?;

        Ok(Store {
            config: config.clone(),
            db,
        })
    }

    /// Check if a committed entry exists for this cache key.
    pub fn contains(&self, cache_key: &str) -> bool {
        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");

        if !meta_path.exists() {
            return false;
        }

        // Check if it's committed in the database
        self.db
            .query_row(
                "SELECT committed FROM entries WHERE cache_key = ?1",
                params![cache_key],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
    }

    /// Load metadata for a cached entry and record a hit.
    pub fn get(&self, cache_key: &str) -> Result<Option<EntryMeta>> {
        if !self.contains(cache_key) {
            return Ok(None);
        }

        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");
        let content = fs::read_to_string(&meta_path).context("reading entry meta.json")?;
        let meta: EntryMeta = serde_json::from_str(&content).context("parsing entry meta.json")?;

        // Lazy migration: if legacy artifacts still live in the entry dir, migrate them
        let needs_migration = meta.files.iter().any(|f| entry_dir.join(&f.name).exists());
        if needs_migration && let Err(e) = self.migrate_entry_to_blobs(&meta) {
            tracing::warn!(
                "lazy migration failed for {}: {e}",
                &cache_key[..16.min(cache_key.len())]
            );
        }

        // Verify all cached blobs still exist on disk and match expected size
        for cached_file in &meta.files {
            let blob = self.blob_path(&cached_file.hash);
            if !blob.is_file() {
                tracing::warn!(
                    "cache entry {} missing blob {} for file {}, evicting",
                    cache_key.get(..16).unwrap_or(cache_key),
                    &cached_file.hash[..16],
                    cached_file.name
                );
                let _ = self.remove_entry(cache_key);
                return Ok(None);
            }

            // Size validation: catches truncated/corrupt artifacts (e.g. LLVM
            // "truncated or malformed object") without the cost of re-hashing.
            if let Ok(file_meta) = fs::metadata(&blob)
                && file_meta.len() != cached_file.size
            {
                tracing::warn!(
                    "cache entry {} file {} size mismatch (expected {}, got {}), evicting",
                    cache_key.get(..16).unwrap_or(cache_key),
                    cached_file.name,
                    cached_file.size,
                    file_meta.len(),
                );
                let _ = self.remove_entry(cache_key);
                return Ok(None);
            }
        }

        // Update access time and hit count
        self.db.execute(
            "UPDATE entries SET last_accessed = datetime('now'), hit_count = hit_count + 1 WHERE cache_key = ?1",
            params![cache_key],
        )?;

        Ok(Some(meta))
    }

    /// Acquire a build lock for a cache key. Returns None if another process holds it.
    pub fn try_lock(&self, cache_key: &str) -> Result<Option<KeyLock>> {
        let lock_path = self.entry_dir(cache_key).with_extension("lock");
        fs::create_dir_all(lock_path.parent().unwrap())?;

        // Try to create the lock file exclusively
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(mut f) => {
                use std::io::Write;
                // Write PID for debugging
                let _ = write!(f, "{}", std::process::id());
                Ok(Some(KeyLock { path: lock_path }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Check if the lock is stale (process died)
                if self.is_lock_stale(&lock_path)? {
                    fs::remove_file(&lock_path)?;
                    // Retry once
                    match fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&lock_path)
                    {
                        Ok(mut f) => {
                            use std::io::Write;
                            let _ = write!(f, "{}", std::process::id());
                            Ok(Some(KeyLock { path: lock_path }))
                        }
                        Err(_) => Ok(None),
                    }
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Wait for a cache key to become committed (another process is building it).
    pub fn wait_for_committed(&self, cache_key: &str) -> Result<bool> {
        let lock_path = self.entry_dir(cache_key).with_extension("lock");
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(600); // 10 min max

        while lock_path.exists() && start.elapsed() < timeout {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // After the lock is gone, check if it was committed
        Ok(self.contains(cache_key))
    }

    /// Store compilation outputs under the cache key.
    ///
    /// Artifact files are stored in the content-addressed blob store
    /// (`store/blobs/{hash[0..2]}/{hash}`). The entry directory only
    /// contains `meta.json`. Identical content is deduplicated via
    /// reference counting in the `blobs` table.
    pub fn put(
        &self,
        cache_key: &str,
        crate_name: &str,
        crate_types: &[String],
        features: &[String],
        target: &str,
        profile: &str,
        output_files: &[(PathBuf, String)], // (source_path, filename_in_store)
        stdout: &str,
        stderr: &str,
    ) -> Result<()> {
        let entry_dir = self.entry_dir(cache_key);
        fs::create_dir_all(&entry_dir).context("creating entry directory")?;

        let mut cached_files = Vec::new();
        let mut total_size = 0u64;

        for (source_path, store_name) in output_files {
            // 1. Hash the source file BEFORE copying
            let hash = crate::cache_key::hash_file(source_path)?;
            let size = fs::metadata(source_path)?.len();
            if size == 0 {
                anyhow::bail!("refusing to cache zero-byte artifact: {}", store_name);
            }
            total_size += size;

            // 2. Compute blob path
            let blob = self.blob_path(&hash);

            // 3. Try to insert into blobs table (INSERT OR IGNORE for race safety)
            self.db.execute(
                "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                params![hash, size as i64],
            )?;

            if self.db.changes() == 0 {
                // 4. Blob already exists — bump refcount
                self.db.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![hash],
                )?;
            } else {
                // 5. New blob — copy source to blob path atomically
                fs::create_dir_all(blob.parent().unwrap())
                    .context("creating blob shard directory")?;
                let tmp = blob.with_extension("tmp");
                fs::copy(source_path, &tmp)
                    .with_context(|| format!("copying {} to blob store", source_path.display()))?;
                fs::rename(&tmp, &blob).context("atomic rename of blob")?;

                // Make blob read-only to prevent accidental modification
                let mut perms = fs::metadata(&blob)?.permissions();
                perms.set_readonly(true);
                fs::set_permissions(&blob, perms)?;
            }

            cached_files.push(CachedFile {
                name: store_name.clone(),
                size,
                hash,
            });
        }

        let content_hash = compute_content_hash(
            &cached_files
                .iter()
                .map(|f| f.hash.as_str())
                .collect::<Vec<_>>(),
        );

        // Write metadata (only meta.json in the entry directory)
        let meta = EntryMeta {
            cache_key: cache_key.to_string(),
            crate_name: crate_name.to_string(),
            crate_types: crate_types.to_vec(),
            files: cached_files,
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            features: features.to_vec(),
            target: target.to_string(),
            profile: profile.to_string(),
        };
        let meta_json =
            serde_json::to_string_pretty(&meta).context("serializing entry metadata")?;
        fs::write(entry_dir.join("meta.json"), meta_json)?;

        // Record in the database and mark committed
        let crate_type_str = crate_types.join(",");
        let num_features = features.len() as i64;
        self.db.execute(
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
            params![cache_key, crate_name, crate_type_str, profile, num_features, total_size as i64, content_hash],
        )?;

        Ok(())
    }

    /// Import a remotely downloaded entry into the database.
    ///
    /// Downloaded entries arrive as tar archives extracted into the entry
    /// directory (old format: artifact files alongside meta.json). This
    /// method moves the artifact files into the content-addressed blob
    /// store and records them in the `blobs` table, leaving only
    /// `meta.json` in the entry directory.
    pub fn import_downloaded_entry(&self, cache_key: &str) -> Result<()> {
        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");
        let content = fs::read_to_string(&meta_path).context("reading downloaded meta.json")?;
        let meta: EntryMeta =
            serde_json::from_str(&content).context("parsing downloaded meta.json")?;

        // Verify all files listed in meta.json exist on disk and match expected size
        for cached_file in &meta.files {
            let file_path = entry_dir.join(&cached_file.name);
            if !file_path.is_file() {
                anyhow::bail!(
                    "downloaded entry {} missing file: {}",
                    cache_key.get(..16).unwrap_or(cache_key),
                    cached_file.name
                );
            }
            if let Ok(file_meta) = fs::metadata(&file_path)
                && file_meta.len() != cached_file.size
            {
                anyhow::bail!(
                    "downloaded entry {} file {} size mismatch (expected {}, got {})",
                    cache_key.get(..16).unwrap_or(cache_key),
                    cached_file.name,
                    cached_file.size,
                    file_meta.len(),
                );
            }
        }

        // Move artifact files into the content-addressed blob store
        for cached_file in &meta.files {
            let file_path = entry_dir.join(&cached_file.name);
            let hash = &cached_file.hash;
            let blob = self.blob_path(hash);

            // INSERT OR IGNORE for race safety (same pattern as put())
            self.db.execute(
                "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                params![hash, cached_file.size as i64],
            )?;

            if self.db.changes() == 0 {
                // Blob already exists — bump refcount, delete the downloaded copy
                self.db.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![hash],
                )?;
                fs::remove_file(&file_path).with_context(|| {
                    format!(
                        "removing deduplicated downloaded artifact {}",
                        file_path.display()
                    )
                })?;
            } else {
                // New blob — move the downloaded file into the blob store
                fs::create_dir_all(blob.parent().unwrap())
                    .context("creating blob shard directory")?;
                fs::rename(&file_path, &blob).with_context(|| {
                    format!(
                        "moving downloaded artifact {} to blob store",
                        file_path.display()
                    )
                })?;

                // Make blob read-only to prevent accidental modification
                let mut perms = fs::metadata(&blob)?.permissions();
                perms.set_readonly(true);
                fs::set_permissions(&blob, perms)?;
            }
        }

        let total_size: u64 = meta.files.iter().map(|f| f.size).sum();

        let content_hash = compute_content_hash(
            &meta
                .files
                .iter()
                .map(|f| f.hash.as_str())
                .collect::<Vec<_>>(),
        );

        let crate_type_str = meta.crate_types.join(",");
        let num_features = meta.features.len() as i64;
        self.db.execute(
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, content_hash, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)",
            params![cache_key, meta.crate_name, crate_type_str, meta.profile, num_features, total_size as i64, content_hash],
        )?;

        Ok(())
    }

    /// Look up cache keys for the given crate names (most recent per crate).
    pub fn keys_for_crates(
        &self,
        crate_names: &[String],
    ) -> Result<Vec<(String, String, PathBuf)>> {
        if crate_names.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders: Vec<&str> = crate_names.iter().map(|_| "?").collect();
        let sql = format!(
            "SELECT cache_key, crate_name FROM entries WHERE committed = 1 AND crate_name IN ({}) ORDER BY last_accessed DESC",
            placeholders.join(",")
        );
        let mut stmt = self.db.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::ToSql> = crate_names
            .iter()
            .map(|n| n as &dyn rusqlite::ToSql)
            .collect();
        let rows = stmt.query_map(params.as_slice(), |row| {
            let key: String = row.get(0)?;
            let cn: String = row.get(1)?;
            Ok((key, cn))
        })?;
        let mut results = Vec::new();
        for row in rows {
            let (key, cn) = row?;
            let entry_dir = self.entry_dir(&key);
            results.push((key, cn, entry_dir));
        }
        Ok(results)
    }

    /// Resolve the filesystem path for a content-addressed blob.
    /// Layout: store/blobs/{first 2 hex chars}/{full hash}
    pub fn blob_path(&self, hash: &str) -> PathBuf {
        let prefix = &hash[..2];
        self.config
            .store_dir()
            .join("blobs")
            .join(prefix)
            .join(hash)
    }

    /// Directory containing all blobs.
    #[allow(dead_code)] // used in tests
    pub fn blobs_dir(&self) -> PathBuf {
        self.config.store_dir().join("blobs")
    }

    /// Get the directory for a cache entry.
    pub fn entry_dir(&self, cache_key: &str) -> PathBuf {
        self.config.store_dir().join(cache_key)
    }

    /// Get the full path to a cached file (legacy entry-based layout).
    #[allow(dead_code)]
    pub fn cached_file_path(&self, cache_key: &str, filename: &str) -> PathBuf {
        self.entry_dir(cache_key).join(filename)
    }

    /// Calculate the total size of the store.
    pub fn total_size(&self) -> Result<u64> {
        let size: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size), 0) FROM entries", [], |row| {
                    row.get(0)
                })?;
        Ok(size as u64)
    }

    /// Get the number of entries in the store.
    pub fn entry_count(&self) -> Result<usize> {
        let count: i64 = self
            .db
            .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// LRU eviction: remove least-recently-accessed entries until under the size limit.
    /// Evicts down to 90% of max_size to create headroom and avoid boundary thrashing.
    pub fn evict(&self) -> Result<usize> {
        let max_size = self.config.max_size;
        let target = max_size * 9 / 10; // evict to 90% — avoids boundary thrashing
        let mut evicted = 0;

        loop {
            let current_size = self.total_size()?;
            if current_size <= target {
                break;
            }

            // Find the least recently accessed entry
            let entry: Option<(String, String)> = self
                .db
                .query_row(
                    "SELECT cache_key, crate_name FROM entries ORDER BY last_accessed ASC LIMIT 1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            if let Some((key, _name)) = entry {
                self.remove_entry(&key)?;
                evicted += 1;
            } else {
                break;
            }
        }

        Ok(evicted)
    }

    /// Evict entries older than the given duration.
    pub fn evict_older_than(&self, hours: u64) -> Result<usize> {
        let keys: Vec<String> = {
            let mut stmt = self.db.prepare(
                "SELECT cache_key FROM entries WHERE last_accessed < datetime('now', ?1)",
            )?;

            stmt.query_map(params![format!("-{hours} hours")], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut evicted = 0;
        for key in &keys {
            self.remove_entry(key)?;
            evicted += 1;
        }
        Ok(evicted)
    }

    /// Evict duplicate entries that share the same content_hash.
    /// Keeps the most recently accessed entry for each content_hash group
    /// (consistent with LRU eviction policy).
    /// Returns the number of entries evicted.
    pub fn evict_duplicate_entries(&self) -> Result<usize> {
        let mut stmt = self.db.prepare(
            "SELECT e.cache_key
             FROM entries e
             JOIN (
                 SELECT content_hash, MAX(last_accessed) as newest_access
                 FROM entries
                 WHERE content_hash IS NOT NULL AND committed = 1
                 GROUP BY content_hash
                 HAVING COUNT(*) > 1
             ) dups ON e.content_hash = dups.content_hash
             WHERE e.last_accessed < dups.newest_access AND e.committed = 1",
        )?;

        let keys: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;

        let mut evicted = 0;
        for key in &keys {
            self.remove_entry(key)?;
            evicted += 1;
        }
        Ok(evicted)
    }

    /// Backfill content_hash for entries that don't have one.
    /// Reads meta.json from each entry to get file hashes.
    /// Returns the number of entries updated.
    pub fn backfill_content_hashes(&self) -> Result<usize> {
        let keys: Vec<String> = {
            let mut stmt = self.db.prepare(
                "SELECT cache_key FROM entries WHERE content_hash IS NULL AND committed = 1",
            )?;
            stmt.query_map([], |row| row.get(0))?
                .collect::<Result<Vec<_>, _>>()?
        };

        let mut updated = 0;
        for key in &keys {
            let meta_path = self.entry_dir(key).join("meta.json");
            if let Ok(content) = fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<EntryMeta>(&content)
            {
                let hashes: Vec<&str> = meta.files.iter().map(|f| f.hash.as_str()).collect();
                let content_hash = compute_content_hash(&hashes);
                self.db.execute(
                    "UPDATE entries SET content_hash = ?1 WHERE cache_key = ?2",
                    params![content_hash, key],
                )?;
                updated += 1;
            }
        }
        Ok(updated)
    }

    /// Remove a single cache entry (files + DB record).
    pub fn remove_entry(&self, cache_key: &str) -> Result<()> {
        let entry_dir = self.entry_dir(cache_key);

        // Decrement blob refcounts based on meta.json
        let meta_path = entry_dir.join("meta.json");
        if meta_path.exists()
            && let Ok(content) = fs::read_to_string(&meta_path)
            && let Ok(meta) = serde_json::from_str::<EntryMeta>(&content)
        {
            for cached_file in &meta.files {
                self.decrement_blob_refcount(&cached_file.hash)?;
            }
        }

        // Remove entry directory (just meta.json in new format, may have artifacts in legacy)
        if entry_dir.exists() {
            if let Ok(entries) = fs::read_dir(&entry_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Ok(meta) = fs::metadata(&path) {
                        let mut perms = meta.permissions();
                        perms.set_readonly(false);
                        let _ = fs::set_permissions(&path, perms);
                    }
                }
            }
            fs::remove_dir_all(&entry_dir)?;
        }

        self.db.execute(
            "DELETE FROM entries WHERE cache_key = ?1",
            params![cache_key],
        )?;

        Ok(())
    }

    /// Decrement a blob's refcount. Deletes blob file when refcount reaches 0.
    fn decrement_blob_refcount(&self, hash: &str) -> Result<()> {
        self.db.execute(
            "UPDATE blobs SET refcount = refcount - 1 WHERE hash = ?1",
            params![hash],
        )?;

        let refcount: Option<i64> = self
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .ok();

        if refcount == Some(0) {
            let blob = self.blob_path(hash);
            if blob.exists() {
                let mut perms = fs::metadata(&blob)?.permissions();
                perms.set_readonly(false);
                fs::set_permissions(&blob, perms)?;
                fs::remove_file(&blob)?;
            }
            self.db
                .execute("DELETE FROM blobs WHERE hash = ?1", params![hash])?;
        }

        Ok(())
    }

    /// Clear the entire store.
    pub fn clear(&self) -> Result<()> {
        let store_dir = self.config.store_dir();
        if store_dir.exists() {
            // Make everything writable recursively, then remove all subdirs
            for entry in fs::read_dir(&store_dir)?.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::make_writable_recursive(&path);
                    let _ = fs::remove_dir_all(&path);
                }
            }
        }
        self.db.execute("DELETE FROM entries", [])?;
        self.db.execute("DELETE FROM blobs", [])?;
        Ok(())
    }

    /// Recursively make all files in a directory writable so they can be deleted.
    fn make_writable_recursive(dir: &Path) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    Self::make_writable_recursive(&path);
                } else if let Ok(meta) = fs::metadata(&path) {
                    let mut perms = meta.permissions();
                    perms.set_readonly(false);
                    let _ = fs::set_permissions(&path, perms);
                }
            }
        }
    }

    /// List all entries for display.
    pub fn list_entries(&self, sort_by: &str) -> Result<Vec<EntryInfo>> {
        let order_clause = match sort_by {
            "size" => "size DESC",
            "hits" => "hit_count DESC",
            "age" => "created_at ASC",
            _ => "crate_name ASC",
        };

        let mut stmt = self.db.prepare(&format!(
            "SELECT cache_key, crate_name, crate_type, profile, size, created_at, last_accessed, hit_count, content_hash FROM entries WHERE committed = 1 ORDER BY {order_clause}"
        ))?;

        let entries = stmt
            .query_map([], |row| {
                Ok(EntryInfo {
                    cache_key: row.get(0)?,
                    crate_name: row.get(1)?,
                    crate_type: row.get(2)?,
                    profile: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    created_at: row.get(5)?,
                    last_accessed: row.get(6)?,
                    hit_count: row.get::<_, i64>(7)? as u64,
                    content_hash: row.get(8)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(entries)
    }

    /// Migrate a single legacy entry's artifacts into the blob store.
    fn migrate_entry_to_blobs(&self, meta: &EntryMeta) -> Result<()> {
        let entry_dir = self.entry_dir(&meta.cache_key);
        for cached_file in &meta.files {
            let artifact_path = entry_dir.join(&cached_file.name);
            if !artifact_path.exists() {
                continue; // Already migrated
            }
            let blob = self.blob_path(&cached_file.hash);
            let blob_dir = blob.parent().unwrap();
            fs::create_dir_all(blob_dir)?;

            // Check if blob already exists
            let existing: Option<i64> = self
                .db
                .query_row(
                    "SELECT refcount FROM blobs WHERE hash = ?1",
                    params![cached_file.hash],
                    |row| row.get(0),
                )
                .ok();

            if existing.is_some() {
                // Blob exists — delete artifact, bump refcount
                if let Ok(m) = fs::metadata(&artifact_path) {
                    let mut perms = m.permissions();
                    perms.set_readonly(false);
                    let _ = fs::set_permissions(&artifact_path, perms);
                }
                fs::remove_file(&artifact_path)?;
                self.db.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![cached_file.hash],
                )?;
            } else {
                // New blob — rename artifact into blob store
                if let Ok(m) = fs::metadata(&artifact_path) {
                    let mut perms = m.permissions();
                    if !perms.readonly() {
                        perms.set_readonly(true);
                        fs::set_permissions(&artifact_path, perms)?;
                    }
                }
                fs::rename(&artifact_path, &blob)?;
                self.db.execute(
                    "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                    params![cached_file.hash, cached_file.size as i64],
                )?;
                if self.db.changes() == 0 {
                    self.db.execute(
                        "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                        params![cached_file.hash],
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Bulk-migrate all legacy entries' artifacts into the blob store.
    pub fn migrate_to_blobs(&self, progress: impl Fn(usize, usize)) -> Result<MigrationStats> {
        let store_dir = self.config.store_dir();
        let mut stats = MigrationStats::default();

        let mut entry_dirs = Vec::new();
        if let Ok(entries) = fs::read_dir(&store_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() && path.file_name().is_some_and(|n| n != "blobs") {
                    let meta_path = path.join("meta.json");
                    if meta_path.exists() {
                        let has_artifacts = fs::read_dir(&path)
                            .into_iter()
                            .flatten()
                            .flatten()
                            .any(|e| e.file_name() != "meta.json");
                        if has_artifacts {
                            entry_dirs.push(path);
                        }
                    }
                }
            }
        }

        let total = entry_dirs.len();
        for (i, entry_dir) in entry_dirs.iter().enumerate() {
            progress(i, total);
            stats.entries_scanned += 1;

            let meta_path = entry_dir.join("meta.json");
            let content = match fs::read_to_string(&meta_path) {
                Ok(c) => c,
                Err(_) => {
                    stats.entries_skipped += 1;
                    continue;
                }
            };
            let meta: EntryMeta = match serde_json::from_str(&content) {
                Ok(m) => m,
                Err(_) => {
                    stats.entries_skipped += 1;
                    continue;
                }
            };

            match self.migrate_entry_to_blobs(&meta) {
                Ok(()) => stats.entries_migrated += 1,
                Err(_) => stats.entries_skipped += 1,
            }
        }

        progress(total, total);
        Ok(stats)
    }

    fn is_lock_stale(&self, lock_path: &Path) -> Result<bool> {
        let content = fs::read_to_string(lock_path).unwrap_or_default();
        if let Ok(pid) = content.trim().parse::<u32>() {
            // Check if the process is still alive
            unsafe {
                if libc::kill(pid as i32, 0) != 0 {
                    return Ok(true); // Process doesn't exist
                }
            }
            // Check if lock file is older than 1 hour (safety net)
            if let Ok(meta) = fs::metadata(lock_path)
                && let Ok(age) = meta.modified()?.elapsed()
                && age > std::time::Duration::from_secs(3600)
            {
                return Ok(true);
            }
            Ok(false)
        } else {
            Ok(true) // Can't parse PID, consider stale
        }
    }

    /// Return content-dedup statistics: unique blobs, physical vs logical size.
    pub fn blob_stats(&self) -> Result<BlobStats> {
        let total_blobs: i64 = self
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))?;
        let total_blob_size: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size), 0) FROM blobs", [], |row| {
                    row.get(0)
                })?;
        let total_logical_size: i64 =
            self.db
                .query_row("SELECT COALESCE(SUM(size), 0) FROM entries", [], |row| {
                    row.get(0)
                })?;
        Ok(BlobStats {
            total_blobs: total_blobs as usize,
            total_blob_size: total_blob_size as u64,
            total_logical_size: total_logical_size as u64,
            savings: (total_logical_size as u64).saturating_sub(total_blob_size as u64),
        })
    }
}

/// Content-dedup statistics.
#[derive(Debug, Default)]
pub struct BlobStats {
    pub total_blobs: usize,
    pub total_blob_size: u64,
    pub total_logical_size: u64,
    pub savings: u64,
}

/// Statistics from a blob migration run.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct MigrationStats {
    pub entries_scanned: usize,
    pub entries_migrated: usize,
    pub entries_skipped: usize,
    pub blobs_created: usize,
    pub blobs_reused: usize,
    pub bytes_saved: u64,
}

#[derive(Debug, Clone)]
pub struct EntryInfo {
    pub cache_key: String,
    pub crate_name: String,
    pub crate_type: String,
    pub profile: String,
    pub size: u64,
    pub created_at: String,
    pub last_accessed: String,
    pub hit_count: u64,
    pub content_hash: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(dir: &Path) -> Config {
        Config {
            cache_dir: dir.to_path_buf(),
            max_size: 1024 * 1024, // 1 MiB
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
        }
    }

    #[test]
    fn test_store_put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a fake output file
        let output_file = dir.path().join("output.rlib");
        std::fs::write(&output_file, b"fake rlib content").unwrap();

        store
            .put(
                "abc123",
                "mylib",
                &["lib".to_string()],
                &["std".to_string()],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libmylib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        assert!(store.contains("abc123"));
        let meta = store.get("abc123").unwrap().unwrap();
        assert_eq!(meta.crate_name, "mylib");
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.files[0].name, "libmylib.rlib");
    }

    #[test]
    fn test_retryable_sqlite_open_error_for_missing_parent() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("missing").join("index.db");

        let err = open_index_db(&db_path).unwrap_err();
        let sql_err = err.downcast_ref::<SqlError>().unwrap();

        assert!(is_retryable_sqlite_open_error(sql_err));
    }

    #[test]
    fn test_store_open_creates_cache_root() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("nested").join("cache");
        let config = test_config(&cache_dir);

        let _store = Store::open(&config).unwrap();

        assert!(cache_dir.is_dir());
        assert!(config.store_dir().is_dir());
        assert!(config.index_db_path().is_file());
    }

    #[test]
    fn test_store_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path());
        config.max_size = 100; // Very small limit to trigger eviction

        let store = Store::open(&config).unwrap();

        // Put a large-ish entry
        let output_file = dir.path().join("big.rlib");
        std::fs::write(&output_file, vec![0u8; 200]).unwrap();

        store
            .put(
                "key1",
                "big_crate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(output_file, "libbig.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let evicted = store.evict().unwrap();
        assert!(evicted > 0);
        assert!(!store.contains("key1"));
    }

    #[test]
    fn test_store_locking() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let lock1 = store.try_lock("testkey").unwrap();
        assert!(lock1.is_some());

        // Second lock attempt should fail
        let lock2 = store.try_lock("testkey").unwrap();
        assert!(lock2.is_none());

        // Drop first lock
        drop(lock1);

        // Now should succeed
        let lock3 = store.try_lock("testkey").unwrap();
        assert!(lock3.is_some());
    }

    #[test]
    fn test_store_clear() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output_file = dir.path().join("out.rlib");
        std::fs::write(&output_file, b"content").unwrap();

        store
            .put(
                "k1",
                "c1",
                &["lib".to_string()],
                &[],
                "",
                "dev",
                &[(output_file.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        assert!(store.contains("k1"));
        store.clear().unwrap();
        assert!(!store.contains("k1"));
    }

    #[test]
    fn test_store_entry_dir() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = store.entry_dir("abc123");
        assert!(entry_dir.to_string_lossy().contains("store"));
        assert!(entry_dir.to_string_lossy().contains("abc123"));
    }

    #[test]
    fn test_store_cached_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let path = store.cached_file_path("key1", "libfoo.rlib");
        assert!(path.to_string_lossy().contains("key1"));
        assert!(path.to_string_lossy().ends_with("libfoo.rlib"));
    }

    #[test]
    fn test_store_total_size_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert_eq!(store.total_size().unwrap(), 0);
    }

    #[test]
    fn test_store_entry_count_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert_eq!(store.entry_count().unwrap(), 0);
    }

    #[test]
    fn test_store_entry_count_after_put() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("a.rlib");
        std::fs::write(&output, b"data").unwrap();
        store
            .put(
                "k1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "a.rlib".into())],
                "",
                "",
            )
            .unwrap();

        std::fs::write(&output, b"data2").unwrap();
        store
            .put(
                "k2",
                "c2",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "b.rlib".into())],
                "",
                "",
            )
            .unwrap();

        assert_eq!(store.entry_count().unwrap(), 2);
    }

    #[test]
    fn test_store_contains_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert!(!store.contains("nonexistent_key"));
    }

    #[test]
    fn test_store_get_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        assert!(store.get("nonexistent_key").unwrap().is_none());
    }

    #[test]
    fn test_store_remove_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "rem1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        assert!(store.contains("rem1"));

        store.remove_entry("rem1").unwrap();
        assert!(!store.contains("rem1"));
        assert_eq!(store.entry_count().unwrap(), 0);
    }

    #[test]
    fn test_store_remove_entry_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Should not error
        store.remove_entry("nonexistent").unwrap();
    }

    #[test]
    fn test_store_list_entries_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entries = store.list_entries("name").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_store_list_entries_sort_by() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let out1 = dir.path().join("a.rlib");
        std::fs::write(&out1, vec![0u8; 100]).unwrap();
        store
            .put(
                "k1",
                "alpha",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(out1, "a.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let out2 = dir.path().join("b.rlib");
        std::fs::write(&out2, vec![0u8; 200]).unwrap();
        store
            .put(
                "k2",
                "beta",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(out2, "b.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Sort by name
        let entries = store.list_entries("name").unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].crate_name, "alpha");

        // Sort by size
        let entries = store.list_entries("size").unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].size >= entries[1].size);

        // Sort by hits
        let entries = store.list_entries("hits").unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_store_evict_older_than() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Backdate the entry so eviction is deterministic (not timing-dependent)
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-48 hours') WHERE cache_key = 'k1'",
                [],
            )
            .unwrap();

        // Evict entries older than 24 hours — our backdated entry qualifies
        let evicted = store.evict_older_than(24).unwrap();
        assert_eq!(evicted, 1);
        assert!(!store.contains("k1"));
    }

    #[test]
    fn test_store_evict_older_than_keeps_recent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Evict entries older than 9999 hours — nothing should be evicted
        let evicted = store.evict_older_than(9999).unwrap();
        assert_eq!(evicted, 0);
        assert!(store.contains("k1"));
    }

    #[test]
    fn test_store_import_downloaded_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a fake downloaded entry directory
        let entry_dir = config.store_dir().join("downloaded_key");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let artifact_content = b"fake artifact";
        std::fs::write(entry_dir.join("lib.rlib"), artifact_content).unwrap();
        let meta = EntryMeta {
            cache_key: "downloaded_key".to_string(),
            crate_name: "downloaded_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: artifact_content.len() as u64,
                hash: "abc".to_string(),
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec!["std".to_string()],
            target: "x86_64-unknown-linux-gnu".to_string(),
            profile: "dev".to_string(),
        };
        let meta_json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();

        store.import_downloaded_entry("downloaded_key").unwrap();
        assert!(store.contains("downloaded_key"));
        assert_eq!(store.entry_count().unwrap(), 1);
    }

    #[test]
    fn test_store_import_downloaded_entry_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create entry directory with meta.json but NO artifact file
        let entry_dir = config.store_dir().join("incomplete_key");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let meta = EntryMeta {
            cache_key: "incomplete_key".to_string(),
            crate_name: "incomplete_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: 42,
                hash: "abc".to_string(),
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
        };
        let meta_json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();
        // Deliberately NOT creating lib.rlib

        let err = store.import_downloaded_entry("incomplete_key").unwrap_err();
        assert!(
            err.to_string().contains("missing file"),
            "expected 'missing file' error, got: {err}"
        );
        assert!(!store.contains("incomplete_key"));
    }

    #[test]
    fn test_import_downloaded_entry_creates_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Simulate a downloaded entry (old tar format: files in entry dir)
        let entry_dir = config.store_dir().join("dl_key");
        fs::create_dir_all(&entry_dir).unwrap();
        fs::write(entry_dir.join("lib.rlib"), b"artifact data").unwrap();

        let hash = crate::cache_key::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let meta = EntryMeta {
            cache_key: "dl_key".to_string(),
            crate_name: "dl_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: 13,
                hash: hash.clone(),
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();

        store.import_downloaded_entry("dl_key").unwrap();

        // Blob should exist
        let blob = store.blob_path(&hash);
        assert!(
            blob.exists(),
            "blob should be created from downloaded artifact"
        );

        // Entry dir artifact should be gone (only meta.json remains)
        assert!(
            !entry_dir.join("lib.rlib").exists(),
            "artifact should have been moved to blob store"
        );
        assert!(
            entry_dir.join("meta.json").exists(),
            "meta.json should remain"
        );

        // Blob should be read-only
        let perms = fs::metadata(&blob).unwrap().permissions();
        assert!(perms.readonly(), "imported blob should be read-only");

        // Refcount should be 1 in the blobs table
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 1);

        // Entry should be committed
        assert!(store.contains("dl_key"));
    }

    #[test]
    fn test_store_get_evicts_entry_with_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Put a valid entry
        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "damaged_key",
                "damaged_crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        assert!(store.contains("damaged_key"));

        // Simulate corruption: delete the blob file from the store
        let meta_content =
            std::fs::read_to_string(store.entry_dir("damaged_key").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        // Make writable so we can delete
        let mut perms = std::fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&blob, perms).unwrap();
        std::fs::remove_file(&blob).unwrap();

        // get() should detect the missing file, evict, and return None
        let result = store.get("damaged_key").unwrap();
        assert!(
            result.is_none(),
            "expected None for entry with missing file"
        );
        assert!(
            !store.contains("damaged_key"),
            "entry should have been evicted"
        );
    }

    #[test]
    fn test_store_get_evicts_entry_with_corrupted_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Put a valid entry
        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"valid rlib content here").unwrap();
        store
            .put(
                "corrupt_key",
                "corrupt_crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        assert!(store.contains("corrupt_key"));

        // Simulate corruption: truncate the blob to a different size
        let meta_content =
            std::fs::read_to_string(store.entry_dir("corrupt_key").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let mut perms = std::fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&blob, perms).unwrap();
        std::fs::write(&blob, b"short").unwrap();

        // get() should detect the size mismatch, evict, and return None
        let result = store.get("corrupt_key").unwrap();
        assert!(
            result.is_none(),
            "expected None for entry with size-corrupted file"
        );
        assert!(
            !store.contains("corrupt_key"),
            "entry should have been evicted"
        );
    }

    #[test]
    fn test_store_put_rejects_zero_byte_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a zero-byte file
        let output = dir.path().join("empty.rlib");
        std::fs::write(&output, b"").unwrap();

        let err = store
            .put(
                "zero_key",
                "zero_crate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "empty.rlib".into())],
                "",
                "",
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("zero-byte"),
            "expected 'zero-byte' error, got: {err}"
        );
        assert!(!store.contains("zero_key"));
    }

    #[test]
    fn test_store_import_rejects_size_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create a fake downloaded entry with mismatched size in metadata
        let entry_dir = config.store_dir().join("mismatch_key");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let meta = EntryMeta {
            cache_key: "mismatch_key".to_string(),
            crate_name: "mismatch_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: 9999, // Wrong size
                hash: "abc".to_string(),
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
        };
        let meta_json = serde_json::to_string_pretty(&meta).unwrap();
        std::fs::write(entry_dir.join("meta.json"), meta_json).unwrap();
        std::fs::write(entry_dir.join("lib.rlib"), b"small content").unwrap();

        let err = store.import_downloaded_entry("mismatch_key").unwrap_err();
        assert!(
            err.to_string().contains("size mismatch"),
            "expected 'size mismatch' error, got: {err}"
        );
    }

    #[test]
    fn test_store_keys_for_crates_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let result = store.keys_for_crates(&[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_store_keys_for_crates_with_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "serde",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        std::fs::write(&output, b"content2").unwrap();
        store
            .put(
                "k2",
                "tokio",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let result = store.keys_for_crates(&["serde".to_string()]).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1, "serde");

        let result = store
            .keys_for_crates(&["serde".to_string(), "tokio".to_string()])
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_store_keys_for_crates_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let result = store.keys_for_crates(&["nonexistent".to_string()]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_store_put_records_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        std::fs::write(&output, b"my rlib content").unwrap();
        store
            .put(
                "meta_key",
                "mycrate",
                &["lib".into(), "rlib".into()],
                &["std".into(), "derive".into()],
                "x86_64-unknown-linux-gnu",
                "release",
                &[(output, "lib.rlib".into())],
                "stdout text",
                "stderr text",
            )
            .unwrap();

        let meta = store.get("meta_key").unwrap().unwrap();
        assert_eq!(meta.crate_name, "mycrate");
        assert_eq!(meta.crate_types, vec!["lib", "rlib"]);
        assert_eq!(meta.features, vec!["std", "derive"]);
        assert_eq!(meta.target, "x86_64-unknown-linux-gnu");
        assert_eq!(meta.profile, "release");
        assert_eq!(meta.stdout, "stdout text");
        assert_eq!(meta.stderr, "stderr text");
        assert_eq!(meta.files.len(), 1);
        assert!(!meta.files[0].hash.is_empty());
    }

    #[test]
    fn test_store_wait_for_committed_returns_false_when_not_committed() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // No entry committed — should return false immediately (no lock file)
        let result = store.wait_for_committed("nope").unwrap();
        assert!(!result);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_creates_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        exclude_from_indexing(dir.path());
        let sentinel = dir.path().join(".metadata_never_index");
        assert!(sentinel.exists());
        assert!(
            sentinel.metadata().unwrap().len() == 0,
            "sentinel should be empty"
        );
        // Idempotent — second call doesn't fail or modify
        exclude_from_indexing(dir.path());
        assert!(sentinel.exists());
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_sets_tmutil_xattr() {
        let dir = tempfile::tempdir().unwrap();
        exclude_from_indexing(dir.path());
        let output = std::process::Command::new("tmutil")
            .args(["isexcluded", &dir.path().display().to_string()])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains("[Excluded]"),
            "expected [Excluded] in tmutil output, got: {stdout}"
        );
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_skips_existing_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        let sentinel = dir.path().join(".metadata_never_index");
        // Pre-create sentinel with known content
        fs::write(&sentinel, b"existing").unwrap();
        exclude_from_indexing(dir.path());
        // Should not overwrite — guard checks exists()
        assert_eq!(fs::read(&sentinel).unwrap(), b"existing");
    }

    #[test]
    fn test_blob_path_sharding() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let path = store.blob_path(hash);
        assert!(path.to_string_lossy().contains("blobs/ab/"));
        assert!(path.to_string_lossy().ends_with(hash));
    }

    #[test]
    fn test_blobs_table_created() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Table should exist — query it
        let count: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_nonexistent_dir_silent() {
        let dir = PathBuf::from("/tmp/kache_test_nonexistent_874291");
        assert!(!dir.exists());
        // Should not panic — both operations fail silently
        exclude_from_indexing(&dir);
    }

    #[test]
    fn test_put_creates_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"rlib content").unwrap();
        store
            .put(
                "k1",
                "mycrate",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Blob should exist
        let meta_path = store.entry_dir("k1").join("meta.json");
        let content = fs::read_to_string(&meta_path).unwrap();
        let meta: EntryMeta = serde_json::from_str(&content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        assert!(
            blob.exists(),
            "blob file should exist at {}",
            blob.display()
        );

        // Entry dir should only have meta.json (no artifact files)
        let entry_dir = store.entry_dir("k1");
        let mut files: Vec<_> = fs::read_dir(&entry_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        files.sort();
        assert_eq!(
            files,
            vec!["meta.json"],
            "entry dir should only contain meta.json"
        );
    }

    #[test]
    fn test_put_deduplicates_identical_content() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"same content").unwrap();
        store
            .put(
                "k1",
                "crate_a",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Put again with same content but different cache key
        fs::write(&output, b"same content").unwrap();
        store
            .put(
                "k2",
                "crate_a",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Both entries should reference the same blob hash
        let m1: EntryMeta = serde_json::from_str(
            &fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap(),
        )
        .unwrap();
        let m2: EntryMeta = serde_json::from_str(
            &fs::read_to_string(store.entry_dir("k2").join("meta.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(m1.files[0].hash, m2.files[0].hash);

        // Refcount should be 2
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![m1.files[0].hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 2);
    }

    #[test]
    fn test_get_verifies_blobs_not_entry_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Entry dir should NOT have lib.rlib — only meta.json
        assert!(!store.entry_dir("k1").join("lib.rlib").exists());

        // get() should still succeed (resolving via blob store)
        let meta = store.get("k1").unwrap();
        assert!(meta.is_some());
    }

    #[test]
    fn test_get_evicts_when_blob_missing() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Read meta to get the hash
        let meta_content = fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let blob = store.blob_path(&meta.files[0].hash);

        // Delete the blob to simulate corruption
        let mut perms = fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(false);
        fs::set_permissions(&blob, perms).unwrap();
        fs::remove_file(&blob).unwrap();

        // get() should detect missing blob and evict
        let result = store.get("k1").unwrap();
        assert!(result.is_none());
        assert!(!store.contains("k1"));
    }

    #[test]
    fn test_put_blob_is_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let meta: EntryMeta = serde_json::from_str(
            &fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap(),
        )
        .unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        let perms = fs::metadata(&blob).unwrap().permissions();
        assert!(perms.readonly(), "blob should be read-only");
    }

    #[test]
    fn test_remove_entry_decrements_refcount() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"shared content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        fs::write(&output, b"shared content").unwrap();
        store
            .put(
                "k2",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Get the hash from meta.json
        let meta_content = fs::read_to_string(store.entry_dir("k1").join("meta.json")).unwrap();
        let meta: EntryMeta = serde_json::from_str(&meta_content).unwrap();
        let hash = meta.files[0].hash.clone();
        let blob = store.blob_path(&hash);

        // Remove first entry — blob should still exist (refcount 1)
        store.remove_entry("k1").unwrap();
        assert!(blob.exists(), "blob should survive when refcount > 0");

        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 1);

        // Remove second entry — blob should be deleted (refcount 0)
        store.remove_entry("k2").unwrap();
        assert!(!blob.exists(), "blob should be deleted when refcount = 0");

        let count: i64 = store
            .db
            .query_row(
                "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
                params![&hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_clear_removes_blobs_too() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"content").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        store.clear().unwrap();

        // Blobs dir should be empty or gone
        let blobs_dir = store.blobs_dir();
        if blobs_dir.exists() {
            let has_files = fs::read_dir(&blobs_dir)
                .unwrap()
                .flatten()
                .any(|e| e.path().is_dir());
            assert!(
                !has_files,
                "blobs dir should have no shard subdirs after clear"
            );
        }

        // Blobs table should be empty
        let count: i64 = store
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_get_lazily_migrates_legacy_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Simulate a legacy entry: artifacts in entry dir, no blobs
        let entry_dir = config.store_dir().join("old_key");
        fs::create_dir_all(&entry_dir).unwrap();
        let content = b"old format artifact";
        fs::write(entry_dir.join("lib.rlib"), content).unwrap();

        let hash = crate::cache_key::hash_file(&entry_dir.join("lib.rlib")).unwrap();
        let meta = EntryMeta {
            cache_key: "old_key".to_string(),
            crate_name: "old_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: content.len() as u64,
                hash: hash.clone(),
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('old_key', 'old_crate', ?1, 1)",
                params![content.len() as i64],
            )
            .unwrap();

        // get() should transparently migrate the entry
        let result = store.get("old_key").unwrap();
        assert!(result.is_some());

        // Blob should now exist
        let blob = store.blob_path(&hash);
        assert!(
            blob.exists(),
            "get() should have migrated artifact to blob store"
        );

        // Artifact should be gone from entry dir
        assert!(!entry_dir.join("lib.rlib").exists());
    }

    #[test]
    fn test_migrate_to_blobs_bulk() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let content = b"shared artifact bytes";
        let hash = {
            let tmp = dir.path().join("tmp");
            fs::write(&tmp, content).unwrap();
            crate::cache_key::hash_file(&tmp).unwrap()
        };

        // Create two legacy entries with identical content
        for key in &["old1", "old2"] {
            let entry_dir = config.store_dir().join(key);
            fs::create_dir_all(&entry_dir).unwrap();
            fs::write(entry_dir.join("lib.rlib"), content).unwrap();

            let meta = EntryMeta {
                cache_key: key.to_string(),
                crate_name: "shared_crate".to_string(),
                crate_types: vec!["lib".to_string()],
                files: vec![CachedFile {
                    name: "lib.rlib".to_string(),
                    size: content.len() as u64,
                    hash: hash.clone(),
                }],
                stdout: String::new(),
                stderr: String::new(),
                features: vec![],
                target: String::new(),
                profile: "dev".to_string(),
            };
            fs::write(
                entry_dir.join("meta.json"),
                serde_json::to_string_pretty(&meta).unwrap(),
            )
            .unwrap();
            store
                .db
                .execute(
                    &format!(
                        "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('{key}', 'shared_crate', {}, 1)",
                        content.len()
                    ),
                    [],
                )
                .unwrap();
        }

        let stats = store.migrate_to_blobs(|_, _| {}).unwrap();
        assert_eq!(stats.entries_migrated, 2);

        // Refcount should be 2
        let refcount: i64 = store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(refcount, 2);
    }

    #[test]
    fn test_blob_stats() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Empty store
        let stats = store.blob_stats().unwrap();
        assert_eq!(stats.total_blobs, 0);
        assert_eq!(stats.savings, 0);

        // Add two entries with same content
        let output = dir.path().join("lib.rlib");
        fs::write(&output, b"shared content!").unwrap();
        store
            .put(
                "k1",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output.clone(), "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();
        fs::write(&output, b"shared content!").unwrap();
        store
            .put(
                "k2",
                "c",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(output, "lib.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let stats = store.blob_stats().unwrap();
        assert_eq!(stats.total_blobs, 1); // one unique blob
        assert!(stats.total_logical_size > stats.total_blob_size); // dedup savings
        assert!(stats.savings > 0);
    }

    // =========================================================================
    // Comprehensive dedup integration tests
    // =========================================================================

    /// Helper: create a temp file with given content and return its path.
    fn write_temp_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.join(name);
        fs::write(&path, content).unwrap();
        path
    }

    /// Helper: read meta.json for a cache key and return the EntryMeta.
    fn read_meta(store: &Store, cache_key: &str) -> EntryMeta {
        let meta_path = store.entry_dir(cache_key).join("meta.json");
        let content = fs::read_to_string(&meta_path).unwrap();
        serde_json::from_str(&content).unwrap()
    }

    /// Helper: query refcount for a blob hash, returns None if blob doesn't exist in DB.
    fn blob_refcount(store: &Store, hash: &str) -> Option<i64> {
        store
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .ok()
    }

    /// Helper: count rows in blobs table.
    fn blob_table_count(store: &Store) -> i64 {
        store
            .db
            .query_row("SELECT COUNT(*) FROM blobs", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn test_full_dedup_lifecycle() {
        // Put two entries with some shared and some unique files.
        // Verify blobs exist and refcounts are correct.
        // Remove one entry — shared blobs still exist (refcount decremented).
        // Remove second entry — all blobs are deleted (refcount 0).
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Shared content between entries 1 and 2
        let shared = write_temp_file(dir.path(), "shared.rlib", b"shared artifact data");
        // Unique content for entry 1
        let unique1 = write_temp_file(dir.path(), "unique1.rlib", b"unique to entry 1");
        // Unique content for entry 2
        let unique2 = write_temp_file(dir.path(), "unique2.rlib", b"unique to entry 2");

        // Put entry 1: shared + unique1
        store
            .put(
                "entry1",
                "crate_a",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[
                    (shared.clone(), "shared.rlib".into()),
                    (unique1, "unique1.rlib".into()),
                ],
                "",
                "",
            )
            .unwrap();

        // Re-create shared file (put() reads from source path, content must exist)
        fs::write(&shared, b"shared artifact data").unwrap();

        // Put entry 2: shared + unique2
        store
            .put(
                "entry2",
                "crate_b",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[
                    (shared, "shared.rlib".into()),
                    (unique2, "unique2.rlib".into()),
                ],
                "",
                "",
            )
            .unwrap();

        // Read metadata to get hashes
        let meta1 = read_meta(&store, "entry1");
        let meta2 = read_meta(&store, "entry2");
        let shared_hash = &meta1
            .files
            .iter()
            .find(|f| f.name == "shared.rlib")
            .unwrap()
            .hash;
        let unique1_hash = &meta1
            .files
            .iter()
            .find(|f| f.name == "unique1.rlib")
            .unwrap()
            .hash;
        let unique2_hash = &meta2
            .files
            .iter()
            .find(|f| f.name == "unique2.rlib")
            .unwrap()
            .hash;

        // Shared blob should have the same hash in both entries
        let shared_hash2 = &meta2
            .files
            .iter()
            .find(|f| f.name == "shared.rlib")
            .unwrap()
            .hash;
        assert_eq!(shared_hash, shared_hash2);

        // Verify refcounts: shared=2, unique1=1, unique2=1
        assert_eq!(blob_refcount(&store, shared_hash), Some(2));
        assert_eq!(blob_refcount(&store, unique1_hash), Some(1));
        assert_eq!(blob_refcount(&store, unique2_hash), Some(1));

        // All blob files should exist on disk
        assert!(store.blob_path(shared_hash).exists());
        assert!(store.blob_path(unique1_hash).exists());
        assert!(store.blob_path(unique2_hash).exists());

        // Remove entry 1 — shared blob should still exist, unique1 blob should be gone
        store.remove_entry("entry1").unwrap();
        assert_eq!(blob_refcount(&store, shared_hash), Some(1));
        assert!(store.blob_path(shared_hash).exists());
        assert!(!store.blob_path(unique1_hash).exists());
        assert_eq!(blob_refcount(&store, unique1_hash), None);

        // Remove entry 2 — everything should be gone
        store.remove_entry("entry2").unwrap();
        assert!(!store.blob_path(shared_hash).exists());
        assert!(!store.blob_path(unique2_hash).exists());
        assert_eq!(blob_refcount(&store, shared_hash), None);
        assert_eq!(blob_refcount(&store, unique2_hash), None);
        assert_eq!(blob_table_count(&store), 0);
    }

    #[test]
    fn test_put_get_restore_cycle() {
        // Put an entry with multiple files, get it, verify metadata,
        // verify blob files exist and are read-only,
        // verify entry dir only contains meta.json.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let file_a = write_temp_file(dir.path(), "a.rlib", b"rlib artifact content");
        let file_b = write_temp_file(dir.path(), "b.dylib", b"dylib artifact content");
        let file_c = write_temp_file(dir.path(), "c.rmeta", b"rmeta artifact content");

        store
            .put(
                "multi_key",
                "multi_crate",
                &["lib".into(), "dylib".into()],
                &["serde".into(), "tokio".into()],
                "aarch64-apple-darwin",
                "release",
                &[
                    (file_a, "a.rlib".into()),
                    (file_b, "b.dylib".into()),
                    (file_c, "c.rmeta".into()),
                ],
                "some stdout",
                "some stderr",
            )
            .unwrap();

        // Get the entry and verify metadata
        let meta = store.get("multi_key").unwrap().unwrap();
        assert_eq!(meta.crate_name, "multi_crate");
        assert_eq!(meta.crate_types, vec!["lib", "dylib"]);
        assert_eq!(meta.features, vec!["serde", "tokio"]);
        assert_eq!(meta.target, "aarch64-apple-darwin");
        assert_eq!(meta.profile, "release");
        assert_eq!(meta.stdout, "some stdout");
        assert_eq!(meta.stderr, "some stderr");
        assert_eq!(meta.files.len(), 3);

        // Verify blob files exist and are read-only
        for cached_file in &meta.files {
            let blob = store.blob_path(&cached_file.hash);
            assert!(blob.exists(), "blob for {} should exist", cached_file.name);
            let perms = fs::metadata(&blob).unwrap().permissions();
            assert!(
                perms.readonly(),
                "blob for {} should be read-only",
                cached_file.name
            );
        }

        // Verify entry dir only contains meta.json
        let entry_dir = store.entry_dir("multi_key");
        let mut files: Vec<String> = fs::read_dir(&entry_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        files.sort();
        assert_eq!(files, vec!["meta.json"]);
    }

    #[test]
    fn test_clear_removes_all_blobs_and_tables() {
        // Put a few entries, call clear(), verify blobs directory and tables are empty.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        // Create 3 entries with different content
        for i in 0..3 {
            let file = write_temp_file(
                dir.path(),
                &format!("f{i}.rlib"),
                format!("content {i}").as_bytes(),
            );
            store
                .put(
                    &format!("key{i}"),
                    &format!("crate{i}"),
                    &["lib".into()],
                    &[],
                    "",
                    "dev",
                    &[(file, format!("lib{i}.rlib"))],
                    "",
                    "",
                )
                .unwrap();
        }

        assert_eq!(store.entry_count().unwrap(), 3);
        assert!(blob_table_count(&store) >= 3);

        store.clear().unwrap();

        // Entries table should be empty
        assert_eq!(store.entry_count().unwrap(), 0);

        // Blobs table should be empty
        assert_eq!(blob_table_count(&store), 0);

        // Blobs directory should be empty or removed
        let blobs_dir = store.blobs_dir();
        if blobs_dir.exists() {
            let any_content = fs::read_dir(&blobs_dir).unwrap().flatten().any(|_| true);
            assert!(!any_content, "blobs dir should be empty after clear");
        }
    }

    #[test]
    fn test_migration_of_legacy_entry() {
        // Create a "legacy" entry by manually writing files to an entry dir
        // (meta.json + artifact files, without blob store).
        // Call migrate_entry_to_blobs() directly.
        // Verify artifacts moved to blob store, entry dir only has meta.json.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = config.store_dir().join("legacy_key");
        fs::create_dir_all(&entry_dir).unwrap();

        // Create two legacy artifact files
        let content_a = b"legacy artifact A";
        let content_b = b"legacy artifact B";
        fs::write(entry_dir.join("a.rlib"), content_a).unwrap();
        fs::write(entry_dir.join("b.dylib"), content_b).unwrap();

        let hash_a = crate::cache_key::hash_file(&entry_dir.join("a.rlib")).unwrap();
        let hash_b = crate::cache_key::hash_file(&entry_dir.join("b.dylib")).unwrap();

        let meta = EntryMeta {
            cache_key: "legacy_key".to_string(),
            crate_name: "legacy_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![
                CachedFile {
                    name: "a.rlib".to_string(),
                    size: content_a.len() as u64,
                    hash: hash_a.clone(),
                },
                CachedFile {
                    name: "b.dylib".to_string(),
                    size: content_b.len() as u64,
                    hash: hash_b.clone(),
                },
            ],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: String::new(),
            profile: "dev".to_string(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();

        // Register in DB as committed
        store
            .db
            .execute(
                "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('legacy_key', 'legacy_crate', ?1, 1)",
                params![(content_a.len() + content_b.len()) as i64],
            )
            .unwrap();

        // Call migrate_entry_to_blobs directly
        store.migrate_entry_to_blobs(&meta).unwrap();

        // Artifacts should be gone from entry dir
        assert!(
            !entry_dir.join("a.rlib").exists(),
            "a.rlib should be moved to blob store"
        );
        assert!(
            !entry_dir.join("b.dylib").exists(),
            "b.dylib should be moved to blob store"
        );

        // meta.json should remain
        assert!(entry_dir.join("meta.json").exists());

        // Blobs should exist and be read-only
        let blob_a = store.blob_path(&hash_a);
        let blob_b = store.blob_path(&hash_b);
        assert!(blob_a.exists(), "blob for a.rlib should exist");
        assert!(blob_b.exists(), "blob for b.dylib should exist");
        assert!(fs::metadata(&blob_a).unwrap().permissions().readonly());
        assert!(fs::metadata(&blob_b).unwrap().permissions().readonly());

        // Refcounts should be 1
        assert_eq!(blob_refcount(&store, &hash_a), Some(1));
        assert_eq!(blob_refcount(&store, &hash_b), Some(1));

        // Entry dir should only have meta.json
        let files: Vec<String> = fs::read_dir(&entry_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(files, vec!["meta.json"]);
    }

    #[test]
    fn test_eviction_with_shared_blobs() {
        // Put 3 entries where entries 1 and 2 share blobs, entry 3 is unique.
        // Remove entry 1 → shared blobs persist with refcount decremented.
        // Remove entry 2 → shared blobs deleted.
        // Entry 3's blobs should be unaffected throughout.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let shared_content = b"shared between 1 and 2";
        let unique3_content = b"unique to entry 3 only";

        // Entry 1: shared blob
        let f = write_temp_file(dir.path(), "shared.rlib", shared_content);
        store
            .put(
                "e1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f, "shared.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Entry 2: same shared blob
        let f = write_temp_file(dir.path(), "shared.rlib", shared_content);
        store
            .put(
                "e2",
                "c2",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f, "shared.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Entry 3: unique blob
        let f = write_temp_file(dir.path(), "unique3.rlib", unique3_content);
        store
            .put(
                "e3",
                "c3",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f, "unique3.rlib".into())],
                "",
                "",
            )
            .unwrap();

        let meta1 = read_meta(&store, "e1");
        let meta3 = read_meta(&store, "e3");
        let shared_hash = &meta1.files[0].hash;
        let unique3_hash = &meta3.files[0].hash;

        assert_eq!(blob_refcount(&store, shared_hash), Some(2));
        assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

        // Remove entry 1 — shared blob persists
        store.remove_entry("e1").unwrap();
        assert_eq!(blob_refcount(&store, shared_hash), Some(1));
        assert!(store.blob_path(shared_hash).exists());
        // Entry 3 unaffected
        assert!(store.blob_path(unique3_hash).exists());
        assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

        // Remove entry 2 — shared blob now deleted
        store.remove_entry("e2").unwrap();
        assert!(!store.blob_path(shared_hash).exists());
        assert_eq!(blob_refcount(&store, shared_hash), None);
        // Entry 3 still unaffected
        assert!(store.blob_path(unique3_hash).exists());
        assert_eq!(blob_refcount(&store, unique3_hash), Some(1));

        // Verify entry 3 can still be retrieved
        let meta = store.get("e3").unwrap();
        assert!(meta.is_some());
    }

    #[test]
    fn test_blob_stats_with_known_overlap() {
        // Put entries with known content overlap.
        // Verify logical vs physical size, savings percentage.
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path());
        let store = Store::open(&config).unwrap();

        let shared_content = b"AAAA"; // 4 bytes, shared by entries 1 and 2
        let unique_content = b"BBBBBBBB"; // 8 bytes, only in entry 1

        // Entry 1: shared (4 bytes) + unique (8 bytes) = 12 bytes logical
        let f_shared = write_temp_file(dir.path(), "shared.rlib", shared_content);
        let f_unique = write_temp_file(dir.path(), "unique.rlib", unique_content);
        store
            .put(
                "stats1",
                "c1",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[
                    (f_shared, "shared.rlib".into()),
                    (f_unique, "unique.rlib".into()),
                ],
                "",
                "",
            )
            .unwrap();

        // Entry 2: shared (4 bytes) = 4 bytes logical
        let f_shared = write_temp_file(dir.path(), "shared.rlib", shared_content);
        store
            .put(
                "stats2",
                "c2",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(f_shared, "shared.rlib".into())],
                "",
                "",
            )
            .unwrap();

        // Total logical size from entries table = 12 + 4 = 16 bytes
        // Total physical blob size = 4 (shared) + 8 (unique) = 12 bytes
        // Savings = 16 - 12 = 4 bytes
        let stats = store.blob_stats().unwrap();
        assert_eq!(stats.total_blobs, 2, "should have 2 unique blobs");
        assert_eq!(
            stats.total_blob_size, 12,
            "physical size should be 12 bytes"
        );
        assert_eq!(
            stats.total_logical_size, 16,
            "logical size should be 16 bytes"
        );
        assert_eq!(stats.savings, 4, "savings should be 4 bytes");
    }

    #[test]
    fn test_put_stores_content_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"artifact-content-1234").unwrap();

        store
            .put(
                "key_ch_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let ch: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = 'key_ch_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ch.len(), 16, "content_hash should be 16 hex chars");
    }

    #[test]
    fn test_import_downloaded_entry_stores_content_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let entry_dir = store.entry_dir("dl_ch_test");
        std::fs::create_dir_all(&entry_dir).unwrap();

        let artifact = entry_dir.join("lib.rlib");
        std::fs::write(&artifact, b"downloaded-artifact-data").unwrap();
        let hash = crate::cache_key::hash_file(&artifact).unwrap();
        let size = std::fs::metadata(&artifact).unwrap().len();

        let meta = EntryMeta {
            cache_key: "dl_ch_test".to_string(),
            crate_name: "dlcrate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size,
                hash,
            }],
            stdout: String::new(),
            stderr: String::new(),
            features: vec![],
            target: "x86_64-unknown-linux-gnu".to_string(),
            profile: "dev".to_string(),
        };
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        )
        .unwrap();

        store.import_downloaded_entry("dl_ch_test").unwrap();

        let ch: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = 'dl_ch_test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ch.len(), 16);
    }

    #[test]
    fn test_list_entries_includes_content_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"list-test-content").unwrap();

        store
            .put(
                "list_ch_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let entries = store.list_entries("name").unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].content_hash.is_some());
        assert_eq!(entries[0].content_hash.as_ref().unwrap().len(), 16);
    }

    #[test]
    fn test_evict_duplicate_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();

        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"same-content-bytes").unwrap();

        store
            .put(
                "dup_key_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1.clone(), "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Artificially age the first entry's access time (LRU policy)
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'dup_key_1'",
                [],
            )
            .unwrap();

        store
            .put(
                "dup_key_2",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        assert_eq!(store.entry_count().unwrap(), 2);

        let evicted = store.evict_duplicate_entries().unwrap();
        assert_eq!(evicted, 1);
        assert_eq!(store.entry_count().unwrap(), 1);

        assert!(store.contains("dup_key_2"));
        assert!(!store.contains("dup_key_1"));
    }

    #[test]
    fn test_backfill_content_hashes() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();
        let file1 = dir.join("lib.rlib");
        std::fs::write(&file1, b"backfill-content").unwrap();

        store
            .put(
                "bf_key_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file1, "lib.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Simulate a legacy entry by clearing the content_hash
        store
            .db
            .execute(
                "UPDATE entries SET content_hash = NULL WHERE cache_key = 'bf_key_1'",
                [],
            )
            .unwrap();

        let backfilled = store.backfill_content_hashes().unwrap();
        assert_eq!(backfilled, 1);

        let ch: String = store
            .db
            .query_row(
                "SELECT content_hash FROM entries WHERE cache_key = 'bf_key_1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ch.len(), 16);
    }

    #[test]
    fn test_content_hash_column_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();
        let result: Result<Option<String>, _> =
            store
                .db
                .query_row("SELECT content_hash FROM entries LIMIT 1", [], |row| {
                    row.get(0)
                });
        // Query should succeed (column exists), just no rows
        assert!(result.is_ok() || result.unwrap_err().to_string().contains("no rows"));
    }

    #[test]
    fn test_content_hash_full_dedup_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        let config = test_config(tmp.path());
        let store = Store::open(&config).unwrap();

        let dir = tmp.path().join("src");
        std::fs::create_dir_all(&dir).unwrap();

        // Create 3 entries: 2 with identical content, 1 different
        let file_a = dir.join("a.rlib");
        std::fs::write(&file_a, b"shared-content").unwrap();
        let file_b = dir.join("b.rlib");
        std::fs::write(&file_b, b"different-content").unwrap();

        store
            .put(
                "ch_lc_1",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file_a.clone(), "a.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Age the first entry's access time (LRU policy)
        store
            .db
            .execute(
                "UPDATE entries SET last_accessed = datetime('now', '-1 hour') WHERE cache_key = 'ch_lc_1'",
                [],
            )
            .unwrap();

        store
            .put(
                "ch_lc_2",
                "mycrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file_a, "a.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        store
            .put(
                "ch_lc_3",
                "othercrate",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(file_b, "b.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        // Verify content hashes
        let entries = store.list_entries("name").unwrap();
        assert_eq!(entries.len(), 3);

        let ch1 = entries
            .iter()
            .find(|e| e.cache_key == "ch_lc_1")
            .unwrap()
            .content_hash
            .as_ref()
            .unwrap();
        let ch2 = entries
            .iter()
            .find(|e| e.cache_key == "ch_lc_2")
            .unwrap()
            .content_hash
            .as_ref()
            .unwrap();
        let ch3 = entries
            .iter()
            .find(|e| e.cache_key == "ch_lc_3")
            .unwrap()
            .content_hash
            .as_ref()
            .unwrap();
        assert_eq!(ch1, ch2, "identical content should have same hash");
        assert_ne!(ch1, ch3, "different content should have different hash");

        // Evict duplicates
        let evicted = store.evict_duplicate_entries().unwrap();
        assert_eq!(evicted, 1);
        assert_eq!(store.entry_count().unwrap(), 2);
        assert!(store.contains("ch_lc_2")); // newer survives
        assert!(store.contains("ch_lc_3")); // unique survives
        assert!(!store.contains("ch_lc_1")); // older dup removed
    }
}
