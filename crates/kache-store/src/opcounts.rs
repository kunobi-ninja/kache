//! Bytes materialized by the store and its consumers.

use std::sync::atomic::{AtomicU64, Ordering};

static REFLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static HARDLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static COPIED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Record `bytes` restored from cache by a CoW reflink.
pub fn record_reflinked(bytes: u64) {
    REFLINKED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` restored by a hardlink (reflink unavailable).
pub fn record_hardlinked(bytes: u64) {
    HARDLINKED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` restored by a full physical copy (no reflink, no hardlink).
pub fn record_copied(bytes: u64) {
    COPIED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Bytes restored by CoW reflink so far in this process.
pub fn reflinked_bytes() -> u64 {
    REFLINKED_BYTES.load(Ordering::Relaxed)
}

/// Bytes restored by hardlink so far in this process.
pub fn hardlinked_bytes() -> u64 {
    HARDLINKED_BYTES.load(Ordering::Relaxed)
}

/// Bytes restored by a full copy so far in this process.
pub fn copied_bytes() -> u64 {
    COPIED_BYTES.load(Ordering::Relaxed)
}

// ── Store-method byte counters ──────────────────────────────────────────────
//
// The mirror image of the restore counters above: how a freshly-compiled
// artifact entered the content-addressed store on a miss. The store tries a
// CoW reflink (clonefile / FICLONE) first, so on APFS / btrfs / XFS-with-reflink
// the blob shares blocks with the build's own output file — storing costs
// ~no physical bytes. Without CoW (ext4 without reflink, tmpfs) it hardlinks
// immutable artifact kinds (shared inode, still zero-copy), and only falls
// back to a full copy where neither is possible (mutable kinds, a
// cross-volume store).
//
// Splitting store bytes by mechanism is what lets `kache report` (and the
// clone benchmark) account for disk honestly: a blob reflinked or hardlinked
// from the objdir is NOT a second physical copy, so a naive "objdir + store"
// sum double-counts it. Deterministic given the same source + filesystem.

static STORE_REFLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_HARDLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_COPIED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Record `bytes` ingested into the store by a CoW reflink (shares blocks
/// with the build's output file — physically zero-copy).
pub fn record_store_reflinked(bytes: u64) {
    STORE_REFLINKED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` ingested into the store by a hardlink (shares an inode
/// with the build's output file — zero-copy on filesystems without CoW).
pub fn record_store_hardlinked(bytes: u64) {
    STORE_HARDLINKED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` ingested into the store by a full physical copy (no
/// reflink, no hardlink — the blob is a genuine second copy).
pub fn record_store_copied(bytes: u64) {
    STORE_COPIED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Bytes ingested into the store by CoW reflink so far in this process.
pub fn store_reflinked_bytes() -> u64 {
    STORE_REFLINKED_BYTES.load(Ordering::Relaxed)
}

/// Bytes ingested into the store by hardlink so far in this process.
pub fn store_hardlinked_bytes() -> u64 {
    STORE_HARDLINKED_BYTES.load(Ordering::Relaxed)
}

/// Bytes ingested into the store by a full copy so far in this process.
pub fn store_copied_bytes() -> u64 {
    STORE_COPIED_BYTES.load(Ordering::Relaxed)
}
