use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

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

impl Store {
    pub fn open(config: &Config) -> Result<Self> {
        fs::create_dir_all(config.store_dir()).context("creating store directory")?;

        let db = Connection::open(config.index_db_path()).context("opening index database")?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "NORMAL")?;
        // Let concurrent writers retry for up to 5 s instead of failing immediately
        // with SQLITE_BUSY — critical when 300+ wrapper processes hit the DB in parallel.
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
        )
        .context("creating entries table")?;

        // Migrations (idempotent — ignore "duplicate column" errors)
        let _ =
            db.execute_batch("ALTER TABLE entries ADD COLUMN crate_type TEXT NOT NULL DEFAULT ''");
        let _ = db.execute_batch("ALTER TABLE entries ADD COLUMN profile TEXT NOT NULL DEFAULT ''");
        let _ = db.execute_batch(
            "ALTER TABLE entries ADD COLUMN num_features INTEGER NOT NULL DEFAULT 0",
        );

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

        // Verify all cached files still exist on disk
        for cached_file in &meta.files {
            let file_path = entry_dir.join(&cached_file.name);
            if !file_path.is_file() {
                tracing::warn!(
                    "cache entry {} missing file {}, evicting",
                    cache_key.get(..16).unwrap_or(cache_key),
                    cached_file.name
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
            let dest = entry_dir.join(store_name);

            // Copy the file into the store
            fs::copy(source_path, &dest)
                .with_context(|| format!("copying {} to store", source_path.display()))?;

            let metadata = fs::metadata(&dest)?;
            let size = metadata.len();
            total_size += size;

            // Make it read-only to prevent accidental modification through hardlinks
            let mut perms = metadata.permissions();
            perms.set_readonly(true);
            fs::set_permissions(&dest, perms)?;

            let hash = crate::cache_key::hash_file(&dest)?;
            cached_files.push(CachedFile {
                name: store_name.clone(),
                size,
                hash,
            });
        }

        // Write metadata
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
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
            params![cache_key, crate_name, crate_type_str, profile, num_features, total_size as i64],
        )?;

        Ok(())
    }

    /// Import a remotely downloaded entry into the database.
    /// Reads meta.json from the entry directory and marks it committed.
    pub fn import_downloaded_entry(&self, cache_key: &str) -> Result<()> {
        let entry_dir = self.entry_dir(cache_key);
        let meta_path = entry_dir.join("meta.json");
        let content = fs::read_to_string(&meta_path).context("reading downloaded meta.json")?;
        let meta: EntryMeta =
            serde_json::from_str(&content).context("parsing downloaded meta.json")?;

        // Verify all files listed in meta.json exist on disk
        for cached_file in &meta.files {
            let file_path = entry_dir.join(&cached_file.name);
            if !file_path.is_file() {
                anyhow::bail!(
                    "downloaded entry {} missing file: {}",
                    cache_key.get(..16).unwrap_or(cache_key),
                    cached_file.name
                );
            }
        }

        let total_size: u64 = meta.files.iter().map(|f| f.size).sum();

        let crate_type_str = meta.crate_types.join(",");
        let num_features = meta.features.len() as i64;
        self.db.execute(
            "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
            params![cache_key, meta.crate_name, crate_type_str, meta.profile, num_features, total_size as i64],
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

    /// Get the directory for a cache entry.
    pub fn entry_dir(&self, cache_key: &str) -> PathBuf {
        self.config.store_dir().join(cache_key)
    }

    /// Get the full path to a cached file.
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
    pub fn evict(&self) -> Result<usize> {
        let max_size = self.config.max_size;
        let mut evicted = 0;

        loop {
            let current_size = self.total_size()?;
            if current_size <= max_size {
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

    /// Remove a single cache entry (files + DB record).
    pub fn remove_entry(&self, cache_key: &str) -> Result<()> {
        let entry_dir = self.entry_dir(cache_key);
        if entry_dir.exists() {
            // Need to make files writable before removing (they're read-only)
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

    /// Clear the entire store.
    pub fn clear(&self) -> Result<()> {
        let store_dir = self.config.store_dir();
        if store_dir.exists() {
            // Make everything writable first
            for entry in fs::read_dir(&store_dir)?.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    for file in fs::read_dir(&path).into_iter().flatten().flatten() {
                        let fp = file.path();
                        if let Ok(meta) = fs::metadata(&fp) {
                            let mut perms = meta.permissions();
                            perms.set_readonly(false);
                            let _ = fs::set_permissions(&fp, perms);
                        }
                    }
                    let _ = fs::remove_dir_all(&path);
                }
            }
        }
        self.db.execute("DELETE FROM entries", [])?;
        Ok(())
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
            "SELECT cache_key, crate_name, crate_type, profile, size, created_at, last_accessed, hit_count FROM entries WHERE committed = 1 ORDER BY {order_clause}"
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
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        Ok(entries)
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

        let meta = EntryMeta {
            cache_key: "downloaded_key".to_string(),
            crate_name: "downloaded_crate".to_string(),
            crate_types: vec!["lib".to_string()],
            files: vec![CachedFile {
                name: "lib.rlib".to_string(),
                size: 42,
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
        // Create the artifact file too
        std::fs::write(entry_dir.join("lib.rlib"), b"fake artifact").unwrap();

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

        // Simulate corruption: delete the artifact file from the store
        let artifact = store.cached_file_path("damaged_key", "lib.rlib");
        // Make writable so we can delete
        let mut perms = std::fs::metadata(&artifact).unwrap().permissions();
        perms.set_readonly(false);
        std::fs::set_permissions(&artifact, perms).unwrap();
        std::fs::remove_file(&artifact).unwrap();

        // get() should detect the missing file, evict, and return None
        let result = store.get("damaged_key").unwrap();
        assert!(result.is_none(), "expected None for entry with missing file");
        assert!(
            !store.contains("damaged_key"),
            "entry should have been evicted"
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
    #[cfg(target_os = "macos")]
    fn test_exclude_from_indexing_nonexistent_dir_silent() {
        let dir = PathBuf::from("/tmp/kache_test_nonexistent_874291");
        assert!(!dir.exists());
        // Should not panic — both operations fail silently
        exclude_from_indexing(&dir);
    }
}
