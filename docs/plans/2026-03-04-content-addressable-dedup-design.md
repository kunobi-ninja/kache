# Content-Addressable Dedup for Kache

## Problem

The kache local store accumulates many entries for the same crate with different cache keys (due to dependency artifact hash churn, feature permutations, `-C metadata` changes, etc.). Analysis of a real store shows:

- **8,337 entries, 50.4 GB** total
- **24,733 unique file hashes** across 40,332 artifact files
- **11.5 GB (22.7%)** is pure content duplication — identical bytes stored under different cache keys
- Worst offenders: `kunobi_lib` (1.55 GB wasted), `aws_sdk_s3` (902 MB), `futures_util` (2.5x duplication ratio)

## Design

### Core idea

Replace per-entry artifact storage with a **content-addressed blob store**. Entry directories contain only `meta.json`; artifact bytes live in a shared blob store keyed by blake3 content hash (already computed and stored in `CachedFile.hash`).

### Local blob store layout

```
~/Library/Caches/kache/
├── index.db                  # gains `blobs` table
├── store/
│   ├── blobs/                # content-addressed blob store
│   │   └── ab/               # 2-char hex prefix sharding
│   │       └── abcdef1234…   # canonical artifact, read-only (0444)
│   └── <cache_key>/          # entry directories (metadata only)
│       └── meta.json         # unchanged format
```

- **2-char prefix sharding** prevents any single directory from having 24k+ entries.
- Blob files are read-only (0444), consistent with current artifact protection.
- `meta.json` format is **unchanged** — `CachedFile { name, size, hash }` already contains everything needed to resolve blobs.

### SQLite schema addition

```sql
CREATE TABLE blobs (
    hash     TEXT PRIMARY KEY,   -- blake3 hex (64 chars)
    size     INTEGER NOT NULL,   -- file size in bytes
    refcount INTEGER NOT NULL DEFAULT 1
);
```

Refcount tracks how many cache entries reference each blob. Blob file is deleted only when refcount reaches 0.

### Modified operations

#### `put()` — writing a new cache entry

```
For each artifact file:
  1. hash = blake3(source_file)   # already computed for CachedFile
  2. blob_path = store/blobs/{hash[0..2]}/{hash}
  3. IF blob exists (refcount > 0):
       UPDATE blobs SET refcount = refcount + 1
     ELSE:
       copy(source, blob_path), chmod 0444
       INSERT INTO blobs (hash, size, refcount) VALUES (?, ?, 1)
  4. Build CachedFile { name, size, hash } as before
Write meta.json to entry dir (no artifact files in entry dir).
INSERT INTO entries as before.
```

Blob creation uses `O_CREAT|O_EXCL` for atomicity when two processes race to create the same blob.

#### `get()` / `restore_from_cache()` — cache hit

```
1. Read meta.json from entry dir
2. For each CachedFile:
     blob_path = store/blobs/{hash[0..2]}/{hash}
     link_to_target(blob_path, cargo_target/filename, strategy)
       # hardlink → reflink → copy (existing fallback chain in link.rs)
3. Update last_accessed / hit_count in SQLite
```

If any blob is missing, treat as corrupted entry → `remove_entry()`.

#### `remove_entry()` — eviction / deletion

```
For each CachedFile in meta.json:
  UPDATE blobs SET refcount = refcount - 1 WHERE hash = ?
  IF refcount = 0:
    DELETE FROM blobs WHERE hash = ?
    unlink(blob_path)
remove_dir_all(entry_dir)   # only meta.json remains
DELETE FROM entries WHERE cache_key = ?
```

#### `evict()` — size tracking

`entries.size` remains **logical size** (sum of all referenced file sizes, regardless of sharing). This is what the user sees and what LRU decisions should be based on. Actual disk savings happen transparently.

A new `kache stats` display shows: `Logical: 46.9 GB | Physical: ~36 GB | Dedup savings: 23%`

### Remote (S3) dedup

#### New S3 layout

```
<prefix>/
├── blobs/
│   └── ab/
│       └── abcdef1234…5678.zst    # individually compressed blob
├── manifests/
│   └── <crate_name>/
│       └── <cache_key>.json       # EntryMeta (metadata only, no artifact bytes)
```

#### Upload flow

1. For each `CachedFile`: HEAD check if `blobs/{hash[0..2]}/{hash}.zst` exists on S3 (use in-memory S3KeyCache)
2. Upload only missing blobs (zstd-compressed individually)
3. Upload manifest (meta.json content)

#### Download flow

1. Fetch manifest → parse `EntryMeta`
2. For each `CachedFile`: check if blob exists in local blob store
3. Download only missing blobs from S3
4. Insert into local blob store, update refcounts
5. Write meta.json to entry dir, commit in SQLite

#### Backward compatibility

- Download: check for old `{prefix}/{crate_name}/{cache_key}.tar.zst` format; if found, download and extract as before, then optionally dedup locally
- Upload: always use new format (blobs + manifests)
- Old-format S3 entries remain readable until evicted; can be batch-upgraded via `kache dedup --remote`

### Migration of existing local stores

`kache dedup` CLI command (also runnable as daemon background task on startup):

```
For each store/<cache_key>/meta.json:
  For each CachedFile:
    blob_path = blobs/{hash[0..2]}/{hash}
    entry_file = <cache_key>/{filename}
    IF blob doesn't exist:
      rename(entry_file, blob_path)    # instant (same filesystem)
      INSERT INTO blobs (hash, size, refcount) VALUES (?, ?, 1)
    ELSE:
      unlink(entry_file)              # blob already has the content
      UPDATE blobs SET refcount = refcount + 1
  # entry dir now contains only meta.json
Report: "Migrated N entries, saved X GB"
```

- **Non-destructive**: if interrupted, entries with remaining artifact files still work in the new code (get() can fall back to reading from entry dir if blob is missing)
- **Resumable**: skips entries that are already migrated (no artifact files in entry dir)
- No cache key version bump needed — keys and meta.json format are unchanged

### Concurrency safety

- Blob file creation: `O_CREAT|O_EXCL` (atomic, same pattern as existing `KeyLock`)
- Refcount updates: SQLite transactions (serialized by WAL + busy_timeout)
- Per-key `KeyLock`: prevents two processes from writing the same cache entry simultaneously (unchanged)
- Migration: safe to run concurrently with normal cache operations — worst case, a blob gets created by both migration and a put(), one wins via O_EXCL

### Version detection

Add `store/.version` file containing `2` (current: no file = v1). Future format breaks can detect and handle version mismatches.

## Non-goals

- Compression of local blobs (filesystem-level compression like APFS handles this better)
- Deduplication across different rustc versions (keys already differentiate)
- Block-level deduplication (filesystem concern, not application concern)

## Testing strategy

- Unit tests: blob store operations (put, get, remove, refcount lifecycle)
- Integration tests: migration of a synthetic store, concurrent put/get with shared blobs
- Property tests: refcount invariant (sum of entry references = blob refcount)
- Edge cases: interrupted migration, corrupted blob, eviction with shared blobs, cross-process races
