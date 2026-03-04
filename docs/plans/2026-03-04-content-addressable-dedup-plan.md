# Content-Addressable Dedup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace per-entry artifact storage with a content-addressed blob store, deduplicating identical files across cache entries both locally and on S3.

**Architecture:** A `store/blobs/{hash[0..2]}/{hash}` directory tree holds one canonical copy per unique file. Entry directories (`store/<cache_key>/`) shrink to only `meta.json`. A new `blobs` SQLite table tracks refcounts. `restore_from_cache()` links directly from blobs to cargo target dirs. S3 uploads individual blobs + manifests instead of per-entry tars.

**Tech Stack:** Rust, rusqlite, blake3 (already in use), aws-sdk-s3, zstd, clap

**Design doc:** `docs/plans/2026-03-04-content-addressable-dedup-design.md`

---

### Task 1: Add blob store helpers to Store

**Files:**
- Modify: `src/store.rs:52-55` (Store struct)
- Modify: `src/store.rs:69-104` (Store::open)
- Test: `src/store.rs` (mod tests)

**Step 1: Write failing tests for blob path resolution and blob table creation**

```rust
// Add to existing mod tests in src/store.rs

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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --lib store::tests::test_blob_path_sharding -- --nocapture`
Expected: FAIL — `blob_path` method doesn't exist

**Step 3: Implement blob_path() and blobs table creation**

Add to `Store`:

```rust
/// Resolve the filesystem path for a content-addressed blob.
/// Layout: store/blobs/{first 2 hex chars}/{full hash}
pub fn blob_path(&self, hash: &str) -> PathBuf {
    let prefix = &hash[..2];
    self.config.store_dir().join("blobs").join(prefix).join(hash)
}

/// Directory containing all blobs.
pub fn blobs_dir(&self) -> PathBuf {
    self.config.store_dir().join("blobs")
}
```

In `Store::open()`, after the existing `CREATE TABLE entries` and migrations, add:

```rust
db.execute_batch(
    "CREATE TABLE IF NOT EXISTS blobs (
        hash     TEXT PRIMARY KEY,
        size     INTEGER NOT NULL,
        refcount INTEGER NOT NULL DEFAULT 1
    );",
)
.context("creating blobs table")?;
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --lib store::tests::test_blob_path_sharding store::tests::test_blobs_table_created -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/store.rs
git commit -m "feat(store): add blob store helpers and blobs table"
```

---

### Task 2: Implement blob-aware `put()`

**Files:**
- Modify: `src/store.rs:216-283` (Store::put)
- Test: `src/store.rs` (mod tests)

**Step 1: Write failing tests for blob-aware put**

```rust
#[test]
fn test_put_creates_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"rlib content").unwrap();
    store
        .put("k1", "mycrate", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "")
        .unwrap();

    // Blob should exist
    let meta = store.get("k1").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    assert!(blob.exists(), "blob file should exist at {}", blob.display());

    // Entry dir should only have meta.json (no artifact files)
    let entry_dir = store.entry_dir("k1");
    let files: Vec<_> = fs::read_dir(&entry_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(files, vec!["meta.json"], "entry dir should only contain meta.json");
}

#[test]
fn test_put_deduplicates_identical_content() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"same content").unwrap();
    store
        .put("k1", "crate_a", &["lib".into()], &[], "", "dev",
             &[(output.clone(), "lib.rlib".into())], "", "")
        .unwrap();

    // Put again with same content but different cache key
    fs::write(&output, b"same content").unwrap();
    store
        .put("k2", "crate_a", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "")
        .unwrap();

    // Both entries should reference the same blob
    let meta1 = store.get("k1").unwrap().unwrap();
    let meta2 = store.get("k2").unwrap().unwrap();
    assert_eq!(meta1.files[0].hash, meta2.files[0].hash);

    // Refcount should be 2
    let refcount: i64 = store.db.query_row(
        "SELECT refcount FROM blobs WHERE hash = ?1",
        params![meta1.files[0].hash],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(refcount, 2);
}

#[test]
fn test_put_blob_is_readonly() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"content").unwrap();
    store
        .put("k1", "c", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "")
        .unwrap();

    let meta = store.get("k1").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let perms = fs::metadata(&blob).unwrap().permissions();
    assert!(perms.readonly(), "blob should be read-only");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --lib store::tests::test_put_creates_blob -- --nocapture`
Expected: FAIL — entry dir still contains artifact files (old put behavior)

**Step 3: Rewrite `put()` to use blob store**

Replace the current `put()` body. Key changes:
- Hash the source file first (before copying)
- Check if blob exists; if yes, increment refcount; if no, copy to blob path
- Use `O_CREAT|O_EXCL` via `OpenOptions::new().create_new(true)` for atomic blob creation
- Do NOT write artifact files into entry dir — only `meta.json`
- Use `fs::create_dir_all` for the blob shard dir (`blobs/ab/`)

```rust
pub fn put(
    &self,
    cache_key: &str,
    crate_name: &str,
    crate_types: &[String],
    features: &[String],
    target: &str,
    profile: &str,
    output_files: &[(PathBuf, String)],
    stdout: &str,
    stderr: &str,
) -> Result<()> {
    let entry_dir = self.entry_dir(cache_key);
    fs::create_dir_all(&entry_dir).context("creating entry directory")?;

    let mut cached_files = Vec::new();
    let mut total_size = 0u64;

    for (source_path, store_name) in output_files {
        let hash = crate::cache_key::hash_file(source_path)?;
        let size = fs::metadata(source_path)?.len();
        total_size += size;

        let blob = self.blob_path(&hash);

        // Check if blob already exists in DB
        let existing: Option<i64> = self
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![hash],
                |row| row.get(0),
            )
            .ok();

        if existing.is_some() {
            // Blob exists — increment refcount
            self.db.execute(
                "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                params![hash],
            )?;
        } else {
            // New blob — copy to blob store
            let blob_dir = blob.parent().unwrap();
            fs::create_dir_all(blob_dir)?;

            // Atomic create: copy to temp then rename, or use create_new
            let temp_blob = blob.with_extension("tmp");
            fs::copy(source_path, &temp_blob)
                .with_context(|| format!("copying {} to blob store", source_path.display()))?;

            // Make read-only
            let meta = fs::metadata(&temp_blob)?;
            let mut perms = meta.permissions();
            perms.set_readonly(true);
            fs::set_permissions(&temp_blob, perms)?;

            // Atomic move into place (if another process raced us, rename fails on some platforms;
            // on POSIX rename replaces atomically which is fine — same content)
            match fs::rename(&temp_blob, &blob) {
                Ok(()) => {}
                Err(_) if blob.exists() => {
                    // Another process created it first — that's fine, remove our temp
                    let _ = fs::remove_file(&temp_blob);
                }
                Err(e) => return Err(e.into()),
            }

            self.db.execute(
                "INSERT OR IGNORE INTO blobs (hash, size, refcount) VALUES (?1, ?2, 1)",
                params![hash, size as i64],
            )?;
            // If INSERT OR IGNORE didn't insert (race), increment instead
            if self.db.changes() == 0 {
                self.db.execute(
                    "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                    params![hash],
                )?;
            }
        }

        cached_files.push(CachedFile {
            name: store_name.clone(),
            size,
            hash,
        });
    }

    // Write metadata only — no artifact files in entry dir
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

    let crate_type_str = crate_types.join(",");
    let num_features = features.len() as i64;
    self.db.execute(
        "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
        params![cache_key, crate_name, crate_type_str, profile, num_features, total_size as i64],
    )?;

    Ok(())
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test --lib store::tests -- --nocapture`
Expected: PASS (all new tests + existing tests — existing `test_store_put_and_get` may need updating since `cached_file_path` now returns entry_dir paths that don't have artifacts)

**Step 5: Fix existing tests that assume artifacts in entry dir**

The existing `test_store_put_and_get` calls `store.get()` which internally checks files exist at `entry_dir.join(name)`. This must be updated — see Task 3.

**Step 6: Commit**

```bash
git add src/store.rs
git commit -m "feat(store): blob-aware put() with content dedup and refcounting"
```

---

### Task 3: Update `get()` and `contains()` to resolve blobs

**Files:**
- Modify: `src/store.rs:107-157` (contains, get)
- Test: `src/store.rs` (mod tests)

**Step 1: Write failing test for blob-aware get**

```rust
#[test]
fn test_get_verifies_blobs_not_entry_files() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"content").unwrap();
    store
        .put("k1", "c", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "")
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
        .put("k1", "c", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "")
        .unwrap();

    // Delete the blob to simulate corruption
    let meta = store.get("k1").unwrap().unwrap();
    let blob = store.blob_path(&meta.files[0].hash);
    let mut perms = fs::metadata(&blob).unwrap().permissions();
    perms.set_readonly(false);
    fs::set_permissions(&blob, perms).unwrap();
    fs::remove_file(&blob).unwrap();

    // get() should detect missing blob and evict
    let result = store.get("k1").unwrap();
    assert!(result.is_none());
    assert!(!store.contains("k1"));
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --lib store::tests::test_get_verifies_blobs_not_entry_files -- --nocapture`
Expected: FAIL — current `get()` checks `entry_dir.join(name)` which no longer has artifacts

**Step 3: Update `get()` to verify blobs instead of entry dir files**

```rust
pub fn get(&self, cache_key: &str) -> Result<Option<EntryMeta>> {
    if !self.contains(cache_key) {
        return Ok(None);
    }

    let entry_dir = self.entry_dir(cache_key);
    let meta_path = entry_dir.join("meta.json");
    let content = fs::read_to_string(&meta_path).context("reading entry meta.json")?;
    let meta: EntryMeta = serde_json::from_str(&content).context("parsing entry meta.json")?;

    // Verify all blobs exist (not entry dir files)
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
    }

    // Update access time and hit count
    self.db.execute(
        "UPDATE entries SET last_accessed = datetime('now'), hit_count = hit_count + 1 WHERE cache_key = ?1",
        params![cache_key],
    )?;

    Ok(Some(meta))
}
```

**Step 4: Run all store tests**

Run: `cargo test --lib store::tests -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/store.rs
git commit -m "feat(store): get() verifies blobs instead of entry dir files"
```

---

### Task 4: Update `remove_entry()` with refcount decrement

**Files:**
- Modify: `src/store.rs:432-453` (remove_entry)
- Test: `src/store.rs` (mod tests)

**Step 1: Write failing tests for refcount-aware removal**

```rust
#[test]
fn test_remove_entry_decrements_refcount() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"shared content").unwrap();
    store.put("k1", "c", &["lib".into()], &[], "", "dev",
             &[(output.clone(), "lib.rlib".into())], "", "").unwrap();
    fs::write(&output, b"shared content").unwrap();
    store.put("k2", "c", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "").unwrap();

    let meta = store.get("k1").unwrap().unwrap();
    let hash = &meta.files[0].hash;
    let blob = store.blob_path(hash);

    // Remove first entry — blob should still exist (refcount 1)
    store.remove_entry("k1").unwrap();
    assert!(blob.exists(), "blob should survive when refcount > 0");

    let refcount: i64 = store.db.query_row(
        "SELECT refcount FROM blobs WHERE hash = ?1",
        params![hash],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(refcount, 1);

    // Remove second entry — blob should be deleted (refcount 0)
    store.remove_entry("k2").unwrap();
    assert!(!blob.exists(), "blob should be deleted when refcount = 0");

    let count: i64 = store.db.query_row(
        "SELECT COUNT(*) FROM blobs WHERE hash = ?1",
        params![hash],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_remove_entry_nonexistent_still_safe() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    // Should not error
    store.remove_entry("nonexistent").unwrap();
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --lib store::tests::test_remove_entry_decrements_refcount -- --nocapture`
Expected: FAIL — current `remove_entry` doesn't touch blobs table

**Step 3: Rewrite `remove_entry()`**

```rust
pub fn remove_entry(&self, cache_key: &str) -> Result<()> {
    let entry_dir = self.entry_dir(cache_key);

    // Decrement blob refcounts based on meta.json
    let meta_path = entry_dir.join("meta.json");
    if meta_path.exists() {
        if let Ok(content) = fs::read_to_string(&meta_path) {
            if let Ok(meta) = serde_json::from_str::<EntryMeta>(&content) {
                for cached_file in &meta.files {
                    self.decrement_blob_refcount(&cached_file.hash)?;
                }
            }
        }
    }

    // Remove entry directory (just meta.json in new format, may have artifacts in legacy format)
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
```

**Step 4: Run all store tests**

Run: `cargo test --lib store::tests -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/store.rs
git commit -m "feat(store): refcount-aware remove_entry with blob cleanup"
```

---

### Task 5: Update `clear()` to also clear blobs

**Files:**
- Modify: `src/store.rs:456-477` (clear)
- Test: `src/store.rs` (mod tests)

**Step 1: Write failing test**

```rust
#[test]
fn test_clear_removes_blobs_too() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"content").unwrap();
    store.put("k1", "c", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "").unwrap();

    store.clear().unwrap();

    // Blobs dir should be empty or gone
    let blobs_dir = store.blobs_dir();
    if blobs_dir.exists() {
        let count = fs::read_dir(&blobs_dir).unwrap().count();
        assert_eq!(count, 0, "blobs dir should be empty after clear");
    }

    // Blobs table should be empty
    let count: i64 = store.db.query_row(
        "SELECT COUNT(*) FROM blobs", [], |row| row.get(0),
    ).unwrap();
    assert_eq!(count, 0);
}
```

**Step 2: Run test to verify failure, then implement**

Update `clear()` to also remove `blobs/` directory and truncate `blobs` table:

```rust
pub fn clear(&self) -> Result<()> {
    let store_dir = self.config.store_dir();
    if store_dir.exists() {
        // Make everything writable first, then remove all dirs
        for entry in fs::read_dir(&store_dir)?.flatten() {
            let path = entry.path();
            if path.is_dir() {
                // Recursively make writable
                fn make_writable_recursive(dir: &Path) {
                    if let Ok(entries) = fs::read_dir(dir) {
                        for file in entries.flatten() {
                            let fp = file.path();
                            if fp.is_dir() {
                                make_writable_recursive(&fp);
                            } else if let Ok(meta) = fs::metadata(&fp) {
                                let mut perms = meta.permissions();
                                perms.set_readonly(false);
                                let _ = fs::set_permissions(&fp, perms);
                            }
                        }
                    }
                }
                make_writable_recursive(&path);
                let _ = fs::remove_dir_all(&path);
            }
        }
    }
    self.db.execute("DELETE FROM entries", [])?;
    self.db.execute("DELETE FROM blobs", [])?;
    Ok(())
}
```

**Step 3: Run tests, commit**

Run: `cargo test --lib store::tests -- --nocapture`

```bash
git add src/store.rs
git commit -m "feat(store): clear() also removes blobs dir and table"
```

---

### Task 6: Update `restore_from_cache()` to link from blobs

**Files:**
- Modify: `src/wrapper.rs:318-387` (restore_from_cache)
- Modify: `src/store.rs` (add `pub fn blob_path` if not already pub)
- Test: `src/store.rs` or integration test

**Step 1: Write failing test**

Add an integration-style test that exercises the full put → get → restore flow using blobs. This test should be in `src/store.rs` since we need to verify blob resolution works:

```rust
#[test]
fn test_cached_file_path_resolves_to_blob() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path());
    let store = Store::open(&config).unwrap();

    let output = dir.path().join("lib.rlib");
    fs::write(&output, b"rlib bytes").unwrap();
    store.put("k1", "c", &["lib".into()], &[], "", "dev",
             &[(output, "lib.rlib".into())], "", "").unwrap();

    let meta = store.get("k1").unwrap().unwrap();
    // cached_file_path should now resolve to blob, not entry dir
    let path = store.cached_file_path("k1", &meta.files[0].name);
    // For backward compat, the actual restore should use blob_path
    let blob = store.blob_path(&meta.files[0].hash);
    assert!(blob.exists());
    assert_eq!(fs::read(&blob).unwrap(), b"rlib bytes");
}
```

**Step 2: Update `restore_from_cache()` in wrapper.rs**

Change the store_path resolution from `store.cached_file_path(cache_key, name)` to `store.blob_path(hash)`:

```rust
fn restore_from_cache(
    _config: &Config,
    store: &Store,
    args: &RustcArgs,
    meta: &crate::store::EntryMeta,
) -> Result<()> {
    let output_dir = if let Some(output) = &args.output {
        output.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else if let Some(dir) = &args.out_dir {
        dir.clone()
    } else {
        anyhow::bail!("no output path (-o) or output directory (--out-dir) in args");
    };

    let strategy = if args.is_executable_output() {
        LinkStrategy::Copy
    } else {
        LinkStrategy::Hardlink
    };

    for cached_file in &meta.files {
        // Resolve from blob store (content-addressed)
        let store_path = store.blob_path(&cached_file.hash);

        if !store_path.exists() {
            anyhow::bail!(
                "blob missing for {} (hash {}): {}",
                meta.cache_key,
                &cached_file.hash[..16],
                cached_file.name
            );
        }

        let target_path = if let Some(output) = &args.output {
            if cached_file.name == output.file_name().unwrap_or_default().to_string_lossy() {
                output.clone()
            } else {
                output_dir.join(&cached_file.name)
            }
        } else {
            output_dir.join(&cached_file.name)
        };

        link::link_to_target(&store_path, &target_path, strategy).with_context(|| {
            format!("linking {} -> {}", store_path.display(), target_path.display())
        })?;

        link::touch_mtime(&target_path)?;

        if cached_file.name.ends_with(".d")
            && let Ok(pwd) = std::env::current_dir()
        {
            let _ = link::rewrite_depinfo(&target_path, &pwd, DepInfoMode::Expand);
        }

        if args.is_executable_output() && !cached_file.name.ends_with(".d") {
            compile::codesign_adhoc(&target_path)?;
        }
    }

    Ok(())
}
```

**Step 3: Run full test suite**

Run: `cargo test -- --nocapture`
Expected: PASS

**Step 4: Commit**

```bash
git add src/wrapper.rs src/store.rs
git commit -m "feat(wrapper): restore_from_cache links from blob store"
```

---

### Task 7: Update `import_downloaded_entry()` to populate blob store

**Files:**
- Modify: `src/store.rs:285-316` (import_downloaded_entry)
- Test: `src/store.rs` (mod tests)

**Step 1: Write failing test**

```rust
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
        stdout: String::new(), stderr: String::new(),
        features: vec![], target: String::new(), profile: "dev".to_string(),
    };
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    ).unwrap();

    store.import_downloaded_entry("dl_key").unwrap();

    // Blob should exist
    let blob = store.blob_path(&hash);
    assert!(blob.exists(), "blob should be created from downloaded artifact");

    // Entry dir should only have meta.json
    let entry_files: Vec<_> = fs::read_dir(&entry_dir).unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();
    assert_eq!(entry_files, vec!["meta.json"]);
}
```

**Step 2: Run test to verify failure**

**Step 3: Update `import_downloaded_entry()` to move artifacts into blob store**

```rust
pub fn import_downloaded_entry(&self, cache_key: &str) -> Result<()> {
    let entry_dir = self.entry_dir(cache_key);
    let meta_path = entry_dir.join("meta.json");
    let content = fs::read_to_string(&meta_path).context("reading downloaded meta.json")?;
    let meta: EntryMeta =
        serde_json::from_str(&content).context("parsing downloaded meta.json")?;

    for cached_file in &meta.files {
        let file_path = entry_dir.join(&cached_file.name);
        if !file_path.is_file() {
            anyhow::bail!(
                "downloaded entry {} missing file: {}",
                cache_key.get(..16).unwrap_or(cache_key),
                cached_file.name
            );
        }

        // Move artifact into blob store
        let blob = self.blob_path(&cached_file.hash);
        let existing: Option<i64> = self
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![cached_file.hash],
                |row| row.get(0),
            )
            .ok();

        if existing.is_some() {
            // Blob already exists — just delete the downloaded file
            let _ = fs::remove_file(&file_path);
            self.db.execute(
                "UPDATE blobs SET refcount = refcount + 1 WHERE hash = ?1",
                params![cached_file.hash],
            )?;
        } else {
            // Move into blob store
            let blob_dir = blob.parent().unwrap();
            fs::create_dir_all(blob_dir)?;

            // Make read-only before moving
            let meta = fs::metadata(&file_path)?;
            let mut perms = meta.permissions();
            perms.set_readonly(true);
            fs::set_permissions(&file_path, perms)?;

            fs::rename(&file_path, &blob)?;

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

    let total_size: u64 = meta.files.iter().map(|f| f.size).sum();
    let crate_type_str = meta.crate_types.join(",");
    let num_features = meta.features.len() as i64;
    self.db.execute(
        "INSERT OR REPLACE INTO entries (cache_key, crate_name, crate_type, profile, num_features, size, committed) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)",
        params![cache_key, meta.crate_name, crate_type_str, meta.profile, num_features, total_size as i64],
    )?;

    Ok(())
}
```

**Step 4: Run tests, commit**

Run: `cargo test --lib store::tests -- --nocapture`

```bash
git add src/store.rs
git commit -m "feat(store): import_downloaded_entry moves artifacts into blob store"
```

---

### Task 8: Lazy migration on access + background bulk migration

**Files:**
- Modify: `src/store.rs` (add `migrate_entry_to_blobs`, `migrate_to_blobs`, update `get()`)
- Modify: `src/daemon.rs` (spawn background migration task on startup)
- Test: `src/store.rs` (mod tests)

**Step 1: Write failing tests for lazy migration**

```rust
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
        stdout: String::new(), stderr: String::new(),
        features: vec![], target: String::new(), profile: "dev".to_string(),
    };
    fs::write(
        entry_dir.join("meta.json"),
        serde_json::to_string_pretty(&meta).unwrap(),
    ).unwrap();
    store.db.execute(
        "INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('old_key', 'old_crate', ?1, 1)",
        params![content.len() as i64],
    ).unwrap();

    // get() should transparently migrate the entry
    let result = store.get("old_key").unwrap();
    assert!(result.is_some());

    // Blob should now exist
    let blob = store.blob_path(&hash);
    assert!(blob.exists(), "get() should have migrated artifact to blob store");

    // Entry dir should only have meta.json
    let files: Vec<_> = fs::read_dir(&entry_dir).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name() != "meta.json")
        .count();
    assert_eq!(files, 0, "artifact should have been moved out of entry dir");
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
            stdout: String::new(), stderr: String::new(),
            features: vec![], target: String::new(), profile: "dev".to_string(),
        };
        fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string_pretty(&meta).unwrap(),
        ).unwrap();
        store.db.execute(
            &format!("INSERT INTO entries (cache_key, crate_name, size, committed) VALUES ('{key}', 'shared_crate', {}, 1)", content.len()),
            [],
        ).unwrap();
    }

    let stats = store.migrate_to_blobs(|_, _| {}).unwrap();
    assert_eq!(stats.entries_migrated, 2);
    assert_eq!(stats.blobs_created, 1); // only one unique blob

    // Refcount should be 2
    let refcount: i64 = store.db.query_row(
        "SELECT refcount FROM blobs WHERE hash = ?1",
        params![hash],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(refcount, 2);
}
```

**Step 2: Run tests to verify failure**

Run: `cargo test --lib store::tests::test_get_lazily_migrates_legacy_entry -- --nocapture`
Expected: FAIL — get() doesn't know about lazy migration yet

**Step 3: Implement `migrate_entry_to_blobs()` (single entry) and update `get()`**

Add a private helper that migrates one entry's artifacts into the blob store:

```rust
/// Migrate a single legacy entry's artifacts into the blob store.
/// Called lazily on get() when artifacts are found in the entry dir instead of blobs.
fn migrate_entry_to_blobs(&self, meta: &EntryMeta) -> Result<()> {
    let entry_dir = self.entry_dir(&meta.cache_key);

    for cached_file in &meta.files {
        let artifact_path = entry_dir.join(&cached_file.name);
        if !artifact_path.exists() {
            continue; // Already migrated or missing
        }

        let blob = self.blob_path(&cached_file.hash);

        let existing: Option<i64> = self
            .db
            .query_row(
                "SELECT refcount FROM blobs WHERE hash = ?1",
                params![cached_file.hash],
                |row| row.get(0),
            )
            .ok();

        if existing.is_some() {
            // Blob already exists — delete the artifact, increment refcount
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
            let blob_dir = blob.parent().unwrap();
            fs::create_dir_all(blob_dir)?;

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
```

Update `get()` — after parsing meta, before verifying blobs, check for legacy artifacts:

```rust
pub fn get(&self, cache_key: &str) -> Result<Option<EntryMeta>> {
    if !self.contains(cache_key) {
        return Ok(None);
    }

    let entry_dir = self.entry_dir(cache_key);
    let meta_path = entry_dir.join("meta.json");
    let content = fs::read_to_string(&meta_path).context("reading entry meta.json")?;
    let meta: EntryMeta = serde_json::from_str(&content).context("parsing entry meta.json")?;

    // Lazy migration: if artifacts are in entry dir (legacy format), migrate them
    let needs_migration = meta.files.iter().any(|f| entry_dir.join(&f.name).exists());
    if needs_migration {
        if let Err(e) = self.migrate_entry_to_blobs(&meta) {
            tracing::warn!("lazy migration failed for {}: {e}", &cache_key[..16]);
        }
    }

    // Verify all blobs exist
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
    }

    self.db.execute(
        "UPDATE entries SET last_accessed = datetime('now'), hit_count = hit_count + 1 WHERE cache_key = ?1",
        params![cache_key],
    )?;

    Ok(Some(meta))
}
```

**Step 4: Implement `migrate_to_blobs()` (bulk, for background daemon use)**

Same `migrate_to_blobs()` as before — walks all entry dirs and calls `migrate_entry_to_blobs()`:

```rust
#[derive(Debug, Default)]
pub struct MigrationStats {
    pub entries_scanned: usize,
    pub entries_migrated: usize,
    pub entries_skipped: usize,
    pub blobs_created: usize,
    pub blobs_reused: usize,
    pub bytes_saved: u64,
}

pub fn migrate_to_blobs(
    &self,
    progress: impl Fn(usize, usize),
) -> Result<MigrationStats> {
    // ... (same implementation as before, walks store dir, calls migrate_entry_to_blobs)
}
```

**Step 5: Add background migration to daemon startup**

In `src/daemon.rs`, after the daemon server starts, spawn a non-blocking tokio task:

```rust
// In daemon server setup, after listener is bound:
tokio::spawn(async move {
    let config = config.clone();
    tokio::task::spawn_blocking(move || {
        if let Ok(store) = Store::open(&config) {
            let stats = store.migrate_to_blobs(|_, _| {});
            if let Ok(stats) = stats {
                if stats.entries_migrated > 0 {
                    tracing::info!(
                        "background migration: migrated {} entries, saved {:.1} MiB",
                        stats.entries_migrated,
                        stats.bytes_saved as f64 / 1_048_576.0
                    );
                }
            }
        }
    }).await
});
```

**Step 6: Run tests, commit**

Run: `cargo test -- --nocapture`

```bash
git add src/store.rs src/daemon.rs
git commit -m "feat(store): lazy migration on get() + background bulk migration on daemon startup"
```

---

### Task 9: Add dedup stats to `kache stats` and TUI

**Files:**
- Modify: `src/store.rs` (add `blob_stats()` method)
- Modify: `src/cli.rs` or wherever stats are rendered
- Modify: `src/tui.rs` (Store tab)

**Step 1: Add `blob_stats()` to Store**

```rust
#[derive(Debug, Default)]
pub struct BlobStats {
    pub total_blobs: usize,
    pub total_blob_size: u64,
    pub total_logical_size: u64,
    pub savings: u64,
}

pub fn blob_stats(&self) -> Result<BlobStats> {
    let total_blobs: i64 = self.db.query_row(
        "SELECT COUNT(*) FROM blobs", [], |row| row.get(0),
    )?;
    let total_blob_size: i64 = self.db.query_row(
        "SELECT COALESCE(SUM(size), 0) FROM blobs", [], |row| row.get(0),
    )?;
    let total_logical_size: i64 = self.db.query_row(
        "SELECT COALESCE(SUM(size), 0) FROM entries", [], |row| row.get(0),
    )?;
    Ok(BlobStats {
        total_blobs: total_blobs as usize,
        total_blob_size: total_blob_size as u64,
        total_logical_size: total_logical_size as u64,
        savings: (total_logical_size as u64).saturating_sub(total_blob_size as u64),
    })
}
```

**Step 2: Integrate into stats display and TUI**

Find where stats are displayed (likely in CLI handler for `Commands::Stats` and in `tui.rs` Store tab) and add dedup lines:

```
  Logical size:  46.9 GiB
  Physical size: 36.2 GiB (blobs)
  Dedup savings: 10.7 GiB (22.8%)
```

**Step 3: Run tests, commit**

```bash
git add src/store.rs src/cli.rs src/tui.rs
git commit -m "feat: show dedup savings in kache stats and TUI"
```

---

### Task 10: Update S3 upload to use blob-based format

**Files:**
- Modify: `src/remote.rs` (add blob upload/download, keep old format for backward compat)
- Modify: `src/daemon.rs` (update upload/download handlers)
- Test: `src/remote.rs` (mod tests)

**Step 1: Add S3 blob key helpers**

```rust
/// S3 key for a content-addressed blob.
fn s3_blob_key(prefix: &str, hash: &str) -> String {
    format!("{prefix}/blobs/{}/{hash}.zst", &hash[..2])
}

/// S3 key for a cache entry manifest.
fn s3_manifest_key(prefix: &str, cache_key: &str, crate_name: &str) -> String {
    format!("{prefix}/manifests/{crate_name}/{cache_key}.json")
}
```

**Step 2: Implement `upload_entry_v2()`**

New upload function that:
1. For each `CachedFile` in meta: HEAD check `s3_blob_key`, upload blob if missing (zstd compressed individually)
2. Upload manifest (meta.json content without artifact bytes)

**Step 3: Implement `download_entry_v2()`**

New download function that:
1. Try fetching manifest first (`s3_manifest_key`)
2. If found: download missing blobs, populate local blob store
3. If not found: fall back to old `download_with_client()` (tar format)

**Step 4: Update daemon to use v2 for uploads, v2-with-fallback for downloads**

**Step 5: Run tests, commit**

```bash
git add src/remote.rs src/daemon.rs
git commit -m "feat(remote): S3 blob-based upload/download with tar fallback"
```

---

### Task 11: Update S3 sync command for blob format

**Files:**
- Modify: `src/main.rs` or `src/cli.rs` (sync command handler)
- Modify: `src/remote.rs` (sync logic)

**Step 1: Update sync to use manifest-based listing**

The `kache sync` command currently lists S3 objects matching `{prefix}/{crate_name}/`. Update to also check `{prefix}/manifests/{crate_name}/` for v2 entries.

**Step 2: Update sync download to use v2 format with fallback**

**Step 3: Run tests, commit**

```bash
git add src/remote.rs src/main.rs
git commit -m "feat(sync): support blob-based S3 format with tar fallback"
```

---

### Task 12: Full integration test suite

**Files:**
- Create: `tests/dedup_integration.rs` (or add to existing integration tests)

**Step 1: Write comprehensive integration tests**

```rust
// Test: full lifecycle — put, get, restore, evict, migrate
// Test: concurrent put with same content (refcount correctness)
// Test: eviction with shared blobs (last entry eviction deletes blob)
// Test: migration of mixed old/new format store
// Test: import_downloaded_entry into blob store
// Test: clear() removes everything including blobs
```

**Step 2: Run full test suite**

Run: `make test`

**Step 3: Commit**

```bash
git add tests/dedup_integration.rs
git commit -m "test: comprehensive dedup integration tests"
```

---

### Task 13: Final cleanup and make check/lint

**Files:**
- All modified files

**Step 1: Run full CI checks**

```bash
make check
make lint
make test
```

**Step 2: Fix any warnings or clippy issues**

**Step 3: Commit**

```bash
git commit -m "chore: fix clippy warnings from dedup changes"
```

---

## Implementation Order & Dependencies

```
Task 1 (blob helpers + table)
  └─► Task 2 (blob-aware put)
       └─► Task 3 (blob-aware get)
            └─► Task 4 (refcount-aware remove)
                 └─► Task 5 (clear with blobs)
                      └─► Task 6 (restore from blobs)
                           └─► Task 7 (import downloaded → blobs)
                                └─► Task 8 (lazy + background migration)
                                     └─► Task 9 (stats/TUI)
Task 10 (S3 blob upload) — can start after Task 7
Task 11 (S3 sync) — after Task 10
Task 12 (integration tests) — after Task 8
Task 13 (cleanup) — final
```

Tasks 10-11 (S3) can be developed in parallel with Tasks 8-9 (local migration/stats) since they touch different files.

**Migration strategy (no CLI command needed):**
- **Lazy**: `get()` detects legacy artifacts in entry dir and migrates them on the fly (near-instant per entry)
- **Background**: Daemon spawns a non-blocking migration task on startup to catch remaining entries
- Together these ensure all entries are migrated without any user action or startup delay
