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

// ── Hardlink-fallback reason byte counters (#835) ─────────────────────────────
//
// A copy fallback is not one condition: `link(2)` can fail with EXDEV across
// two bind mounts of one filesystem, EPERM under `protected_hardlinks`, or any
// other errno; the ingest can also refuse a hardlink by policy
// (kind-ineligible: executable, dylib, depinfo, extensionless, or a cc put
// that never shares inodes); the restore can refuse it for the
// exclusive-carrier rule (#794: the blob already has a consumer). Recording
// only `copied_bytes` leaves those indistinguishable in events, which is how
// ext4 CI showed 0% multi-link blobs with no signal. These break the copy
// side down by reason, still as deterministic bytes, so `kache report` can
// show *why* zero-copy did not happen. Observability only: recording a reason
// never changes what gets linked.

static STORE_COPY_CROSS_DEVICE_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_COPY_PERMISSION_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_COPY_INELIGIBLE_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_COPY_OTHER_BYTES: AtomicU64 = AtomicU64::new(0);
static RESTORE_COPY_CROSS_DEVICE_BYTES: AtomicU64 = AtomicU64::new(0);
static RESTORE_COPY_PERMISSION_BYTES: AtomicU64 = AtomicU64::new(0);
static RESTORE_COPY_EXCLUSIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static RESTORE_COPY_OTHER_BYTES: AtomicU64 = AtomicU64::new(0);

/// Record `bytes` copied into the store because `link(2)` failed with
/// `CrossesDevices` (EXDEV across mounts, including two bind mounts of one
/// filesystem).
pub fn record_store_copy_cross_device(bytes: u64) {
    STORE_COPY_CROSS_DEVICE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` copied into the store because `link(2)` failed with
/// `PermissionDenied` (EPERM/EACCES, e.g. `protected_hardlinks`).
pub fn record_store_copy_permission(bytes: u64) {
    STORE_COPY_PERMISSION_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` copied into the store because a hardlink was never
/// attempted: the artifact kind is ineligible (executable, dylib, depinfo,
/// extensionless) or the put forbids source hardlinks (cc objects never share
/// inodes).
pub fn record_store_copy_ineligible(bytes: u64) {
    STORE_COPY_INELIGIBLE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` copied into the store because `link(2)` failed with any
/// other errno (EMLINK, EEXIST, …).
pub fn record_store_copy_other(bytes: u64) {
    STORE_COPY_OTHER_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` restored by copy because `link(2)` failed with
/// `CrossesDevices`.
pub fn record_restore_copy_cross_device(bytes: u64) {
    RESTORE_COPY_CROSS_DEVICE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` restored by copy because `link(2)` failed with
/// `PermissionDenied`.
pub fn record_restore_copy_permission(bytes: u64) {
    RESTORE_COPY_PERMISSION_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` restored by copy because of the exclusive-carrier rule
/// (#794): the blob already had a hardlink consumer (`nlink != 1` before the
/// link, or `nlink != 2` after), so a second share would let one consumer's
/// mtime stamp reach another. Unix-only: link counts do not exist on Windows.
#[cfg(unix)]
pub fn record_restore_copy_exclusive(bytes: u64) {
    RESTORE_COPY_EXCLUSIVE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` restored by copy because `link(2)` failed with any other
/// errno, or the blob's link count could not be verified.
pub fn record_restore_copy_other(bytes: u64) {
    RESTORE_COPY_OTHER_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Bytes copied into the store after an EXDEV hardlink failure.
pub fn store_copy_cross_device_bytes() -> u64 {
    STORE_COPY_CROSS_DEVICE_BYTES.load(Ordering::Relaxed)
}

/// Bytes copied into the store after an EPERM/EACCES hardlink failure.
pub fn store_copy_permission_bytes() -> u64 {
    STORE_COPY_PERMISSION_BYTES.load(Ordering::Relaxed)
}

/// Bytes copied into the store without attempting a hardlink (kind-ineligible).
pub fn store_copy_ineligible_bytes() -> u64 {
    STORE_COPY_INELIGIBLE_BYTES.load(Ordering::Relaxed)
}

/// Bytes copied into the store after any other hardlink errno.
pub fn store_copy_other_bytes() -> u64 {
    STORE_COPY_OTHER_BYTES.load(Ordering::Relaxed)
}

/// Bytes restored by copy after an EXDEV hardlink failure.
pub fn restore_copy_cross_device_bytes() -> u64 {
    RESTORE_COPY_CROSS_DEVICE_BYTES.load(Ordering::Relaxed)
}

/// Bytes restored by copy after an EPERM/EACCES hardlink failure.
pub fn restore_copy_permission_bytes() -> u64 {
    RESTORE_COPY_PERMISSION_BYTES.load(Ordering::Relaxed)
}

/// Bytes restored by copy for the exclusive-carrier rule (#794).
pub fn restore_copy_exclusive_bytes() -> u64 {
    RESTORE_COPY_EXCLUSIVE_BYTES.load(Ordering::Relaxed)
}

/// Bytes restored by copy after any other hardlink failure.
pub fn restore_copy_other_bytes() -> u64 {
    RESTORE_COPY_OTHER_BYTES.load(Ordering::Relaxed)
}
