//! Bytes materialized by the store and its consumers.
//!
//! Byte totals use the current invocation scope when present and process totals
//! otherwise. Thread snapshots always accumulate for before/after deltas.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct InvocationBytes {
    reflinked: Cell<u64>,
    hardlinked: Cell<u64>,
    copied: Cell<u64>,
    store_reflinked: Cell<u64>,
    store_hardlinked: Cell<u64>,
    store_copied: Cell<u64>,
    store_copy_cross_device: Cell<u64>,
    store_copy_permission: Cell<u64>,
    store_copy_ineligible: Cell<u64>,
    store_copy_other: Cell<u64>,
    restore_copy_cross_device: Cell<u64>,
    restore_copy_permission: Cell<u64>,
    restore_copy_exclusive: Cell<u64>,
    restore_copy_other: Cell<u64>,
}

thread_local! {
    static INVOCATION_BYTES: RefCell<Option<Rc<InvocationBytes>>> = const { RefCell::new(None) };
}

/// Own byte accounting for one synchronous invocation on the current thread.
///
/// Nested scopes restore their parent on drop. The guard cannot move between
/// threads and must not span an async suspension or task switch. Thread byte
/// snapshots remain cumulative so before/after deltas retain their meaning.
pub struct InvocationByteScope {
    previous: Option<Rc<InvocationBytes>>,
    _current: Rc<InvocationBytes>,
}

impl InvocationByteScope {
    pub fn enter() -> Self {
        let current = Rc::new(InvocationBytes::default());
        let previous = INVOCATION_BYTES.with(|slot| slot.replace(Some(Rc::clone(&current))));
        Self {
            previous,
            _current: current,
        }
    }
}

impl Drop for InvocationByteScope {
    fn drop(&mut self) {
        INVOCATION_BYTES.with(|slot| slot.replace(self.previous.take()));
    }
}

fn record_bytes(global: &AtomicU64, field: fn(&InvocationBytes) -> &Cell<u64>, bytes: u64) {
    INVOCATION_BYTES.with(|slot| {
        if let Some(current) = slot.borrow().as_ref() {
            let counter = field(current);
            counter.set(counter.get() + bytes);
        } else {
            global.fetch_add(bytes, Ordering::Relaxed);
        }
    });
}

fn bytes(global: &AtomicU64, field: fn(&InvocationBytes) -> &Cell<u64>) -> u64 {
    INVOCATION_BYTES.with(|slot| {
        slot.borrow().as_ref().map_or_else(
            || global.load(Ordering::Relaxed),
            |current| field(current).get(),
        )
    })
}

static REFLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static HARDLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static COPIED_BYTES: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_REFLINKED: Cell<u64> = const { Cell::new(0) };
    static THREAD_COPIED: Cell<u64> = const { Cell::new(0) };
}

/// Record `bytes` restored from cache by a CoW reflink.
pub fn record_reflinked(bytes: u64) {
    record_bytes(&REFLINKED_BYTES, |counts| &counts.reflinked, bytes);
    THREAD_REFLINKED.with(|slot| slot.set(slot.get() + bytes));
}

/// Record `bytes` restored by a hardlink (reflink unavailable).
pub fn record_hardlinked(bytes: u64) {
    record_bytes(&HARDLINKED_BYTES, |counts| &counts.hardlinked, bytes);
}

/// Record `bytes` restored by a full physical copy (no reflink, no hardlink).
pub fn record_copied(bytes: u64) {
    record_bytes(&COPIED_BYTES, |counts| &counts.copied, bytes);
    THREAD_COPIED.with(|slot| slot.set(slot.get() + bytes));
}

/// Bytes restored by CoW reflink so far in this process.
pub fn reflinked_bytes() -> u64 {
    bytes(&REFLINKED_BYTES, |counts| &counts.reflinked)
}

/// Bytes restored by hardlink so far in this process.
pub fn hardlinked_bytes() -> u64 {
    bytes(&HARDLINKED_BYTES, |counts| &counts.hardlinked)
}

/// Bytes restored by a full copy so far in this process.
pub fn copied_bytes() -> u64 {
    bytes(&COPIED_BYTES, |counts| &counts.copied)
}

/// Reflink bytes recorded on this thread. Other tests increment the process
/// total concurrently; a restore test reads this instead.
pub fn thread_reflinked_bytes() -> u64 {
    THREAD_REFLINKED.with(Cell::get)
}

/// Copied bytes recorded on this thread.
pub fn thread_copied_bytes() -> u64 {
    THREAD_COPIED.with(Cell::get)
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

/// Store bytes recorded by the current thread. A daemon can snapshot these
/// around a synchronous put without counting simultaneous remote imports.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StoreThreadBytes {
    pub reflinked: u64,
    pub hardlinked: u64,
    pub copied: u64,
    pub copy_cross_device: u64,
    pub copy_permission: u64,
    pub copy_ineligible: u64,
    pub copy_other: u64,
}

thread_local! {
    static STORE_THREAD_BYTES: std::cell::Cell<StoreThreadBytes> =
        std::cell::Cell::new(StoreThreadBytes::default());
}

pub fn store_thread_bytes() -> StoreThreadBytes {
    STORE_THREAD_BYTES.with(std::cell::Cell::get)
}

static STORE_REFLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_HARDLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_COPIED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Record `bytes` ingested into the store by a CoW reflink (shares blocks
/// with the build's output file — physically zero-copy).
pub fn record_store_reflinked(bytes: u64) {
    STORE_THREAD_BYTES.with(|counter| {
        let mut value = counter.get();
        value.reflinked += bytes;
        counter.set(value);
    });
    record_bytes(
        &STORE_REFLINKED_BYTES,
        |counts| &counts.store_reflinked,
        bytes,
    );
}

/// Record `bytes` ingested into the store by a hardlink (shares an inode
/// with the build's output file — zero-copy on filesystems without CoW).
pub fn record_store_hardlinked(bytes: u64) {
    STORE_THREAD_BYTES.with(|counter| {
        let mut value = counter.get();
        value.hardlinked += bytes;
        counter.set(value);
    });
    record_bytes(
        &STORE_HARDLINKED_BYTES,
        |counts| &counts.store_hardlinked,
        bytes,
    );
}

/// Record `bytes` ingested into the store by a full physical copy (no
/// reflink, no hardlink — the blob is a genuine second copy).
pub fn record_store_copied(bytes: u64) {
    STORE_THREAD_BYTES.with(|counter| {
        let mut value = counter.get();
        value.copied += bytes;
        counter.set(value);
    });
    record_bytes(&STORE_COPIED_BYTES, |counts| &counts.store_copied, bytes);
}

/// Bytes ingested into the store by CoW reflink so far in this process.
pub fn store_reflinked_bytes() -> u64 {
    bytes(&STORE_REFLINKED_BYTES, |counts| &counts.store_reflinked)
}

/// Bytes ingested into the store by hardlink so far in this process.
pub fn store_hardlinked_bytes() -> u64 {
    bytes(&STORE_HARDLINKED_BYTES, |counts| &counts.store_hardlinked)
}

/// Bytes ingested into the store by a full copy so far in this process.
pub fn store_copied_bytes() -> u64 {
    bytes(&STORE_COPIED_BYTES, |counts| &counts.store_copied)
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
    STORE_THREAD_BYTES.with(|counter| {
        let mut value = counter.get();
        value.copy_cross_device += bytes;
        counter.set(value);
    });
    record_bytes(
        &STORE_COPY_CROSS_DEVICE_BYTES,
        |counts| &counts.store_copy_cross_device,
        bytes,
    );
}

/// Record `bytes` copied into the store because `link(2)` failed with
/// `PermissionDenied` (EPERM/EACCES, e.g. `protected_hardlinks`).
pub fn record_store_copy_permission(bytes: u64) {
    STORE_THREAD_BYTES.with(|counter| {
        let mut value = counter.get();
        value.copy_permission += bytes;
        counter.set(value);
    });
    record_bytes(
        &STORE_COPY_PERMISSION_BYTES,
        |counts| &counts.store_copy_permission,
        bytes,
    );
}

/// Record `bytes` copied into the store because a hardlink was never
/// attempted: the artifact kind is ineligible (executable, dylib, depinfo,
/// extensionless) or the put forbids source hardlinks (cc objects never share
/// inodes).
pub fn record_store_copy_ineligible(bytes: u64) {
    STORE_THREAD_BYTES.with(|counter| {
        let mut value = counter.get();
        value.copy_ineligible += bytes;
        counter.set(value);
    });
    record_bytes(
        &STORE_COPY_INELIGIBLE_BYTES,
        |counts| &counts.store_copy_ineligible,
        bytes,
    );
}

/// Record `bytes` copied into the store because `link(2)` failed with any
/// other errno (EMLINK, EEXIST, …).
pub fn record_store_copy_other(bytes: u64) {
    STORE_THREAD_BYTES.with(|counter| {
        let mut value = counter.get();
        value.copy_other += bytes;
        counter.set(value);
    });
    record_bytes(
        &STORE_COPY_OTHER_BYTES,
        |counts| &counts.store_copy_other,
        bytes,
    );
}

/// Record `bytes` restored by copy because `link(2)` failed with
/// `CrossesDevices`.
pub fn record_restore_copy_cross_device(bytes: u64) {
    record_bytes(
        &RESTORE_COPY_CROSS_DEVICE_BYTES,
        |counts| &counts.restore_copy_cross_device,
        bytes,
    );
}

/// Record `bytes` restored by copy because `link(2)` failed with
/// `PermissionDenied`.
pub fn record_restore_copy_permission(bytes: u64) {
    record_bytes(
        &RESTORE_COPY_PERMISSION_BYTES,
        |counts| &counts.restore_copy_permission,
        bytes,
    );
}

/// Record `bytes` restored by copy because of the exclusive-carrier rule
/// (#794): the blob already had a hardlink consumer (`nlink != 1` before the
/// link, or `nlink != 2` after), so a second share would let one consumer's
/// mtime stamp reach another. Unix-only: link counts do not exist on Windows.
#[cfg(unix)]
pub fn record_restore_copy_exclusive(bytes: u64) {
    record_bytes(
        &RESTORE_COPY_EXCLUSIVE_BYTES,
        |counts| &counts.restore_copy_exclusive,
        bytes,
    );
}

/// Record `bytes` restored by copy because `link(2)` failed with any other
/// errno, or the blob's link count could not be verified.
pub fn record_restore_copy_other(bytes: u64) {
    record_bytes(
        &RESTORE_COPY_OTHER_BYTES,
        |counts| &counts.restore_copy_other,
        bytes,
    );
}

/// Bytes copied into the store after an EXDEV hardlink failure.
pub fn store_copy_cross_device_bytes() -> u64 {
    bytes(&STORE_COPY_CROSS_DEVICE_BYTES, |counts| {
        &counts.store_copy_cross_device
    })
}

/// Bytes copied into the store after an EPERM/EACCES hardlink failure.
pub fn store_copy_permission_bytes() -> u64 {
    bytes(&STORE_COPY_PERMISSION_BYTES, |counts| {
        &counts.store_copy_permission
    })
}

/// Bytes copied into the store without attempting a hardlink (kind-ineligible).
pub fn store_copy_ineligible_bytes() -> u64 {
    bytes(&STORE_COPY_INELIGIBLE_BYTES, |counts| {
        &counts.store_copy_ineligible
    })
}

/// Bytes copied into the store after any other hardlink errno.
pub fn store_copy_other_bytes() -> u64 {
    bytes(&STORE_COPY_OTHER_BYTES, |counts| &counts.store_copy_other)
}

/// Bytes restored by copy after an EXDEV hardlink failure.
pub fn restore_copy_cross_device_bytes() -> u64 {
    bytes(&RESTORE_COPY_CROSS_DEVICE_BYTES, |counts| {
        &counts.restore_copy_cross_device
    })
}

/// Bytes restored by copy after an EPERM/EACCES hardlink failure.
pub fn restore_copy_permission_bytes() -> u64 {
    bytes(&RESTORE_COPY_PERMISSION_BYTES, |counts| {
        &counts.restore_copy_permission
    })
}

/// Bytes restored by copy for the exclusive-carrier rule (#794).
pub fn restore_copy_exclusive_bytes() -> u64 {
    bytes(&RESTORE_COPY_EXCLUSIVE_BYTES, |counts| {
        &counts.restore_copy_exclusive
    })
}

/// Bytes restored by copy after any other hardlink failure.
pub fn restore_copy_other_bytes() -> u64 {
    bytes(&RESTORE_COPY_OTHER_BYTES, |counts| {
        &counts.restore_copy_other
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_invocation_bytes(bytes: u64) {
        record_reflinked(bytes);
        record_hardlinked(bytes + 1);
        record_copied(bytes + 2);
        record_store_reflinked(bytes + 3);
        record_store_hardlinked(bytes + 4);
        record_store_copied(bytes + 5);
        record_store_copy_cross_device(bytes + 6);
        record_store_copy_permission(bytes + 7);
        record_store_copy_ineligible(bytes + 8);
        record_store_copy_other(bytes + 9);
        record_restore_copy_cross_device(bytes + 10);
        record_restore_copy_permission(bytes + 11);
        #[cfg(unix)]
        record_restore_copy_exclusive(bytes + 12);
        record_restore_copy_other(bytes + 13);
    }

    fn assert_invocation_bytes(bytes: u64) {
        assert_eq!(reflinked_bytes(), bytes);
        assert_eq!(hardlinked_bytes(), bytes + 1);
        assert_eq!(copied_bytes(), bytes + 2);
        assert_eq!(store_reflinked_bytes(), bytes + 3);
        assert_eq!(store_hardlinked_bytes(), bytes + 4);
        assert_eq!(store_copied_bytes(), bytes + 5);
        assert_eq!(store_copy_cross_device_bytes(), bytes + 6);
        assert_eq!(store_copy_permission_bytes(), bytes + 7);
        assert_eq!(store_copy_ineligible_bytes(), bytes + 8);
        assert_eq!(store_copy_other_bytes(), bytes + 9);
        assert_eq!(restore_copy_cross_device_bytes(), bytes + 10);
        assert_eq!(restore_copy_permission_bytes(), bytes + 11);
        #[cfg(unix)]
        assert_eq!(restore_copy_exclusive_bytes(), bytes + 12);
        #[cfg(not(unix))]
        assert_eq!(restore_copy_exclusive_bytes(), 0);
        assert_eq!(restore_copy_other_bytes(), bytes + 13);
    }

    fn assert_empty_invocation() {
        assert_eq!(reflinked_bytes(), 0);
        assert_eq!(hardlinked_bytes(), 0);
        assert_eq!(copied_bytes(), 0);
        assert_eq!(store_reflinked_bytes(), 0);
        assert_eq!(store_hardlinked_bytes(), 0);
        assert_eq!(store_copied_bytes(), 0);
        assert_eq!(store_copy_cross_device_bytes(), 0);
        assert_eq!(store_copy_permission_bytes(), 0);
        assert_eq!(store_copy_ineligible_bytes(), 0);
        assert_eq!(store_copy_other_bytes(), 0);
        assert_eq!(restore_copy_cross_device_bytes(), 0);
        assert_eq!(restore_copy_permission_bytes(), 0);
        assert_eq!(restore_copy_exclusive_bytes(), 0);
        assert_eq!(restore_copy_other_bytes(), 0);
    }

    #[test]
    fn concurrent_invocations_separate_every_restore_and_store_counter() {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for bytes in [11, 37] {
                let barrier = &barrier;
                threads.spawn(move || {
                    let _scope = InvocationByteScope::enter();
                    barrier.wait();
                    record_invocation_bytes(bytes);
                    assert_invocation_bytes(bytes);
                });
            }
        });
    }

    #[test]
    fn nested_scopes_and_worker_reuse_keep_byte_accounting_separate() {
        std::thread::spawn(|| {
            for bytes in [13, 41] {
                let _scope = InvocationByteScope::enter();
                assert_empty_invocation();
                record_invocation_bytes(bytes);
                {
                    let _nested = InvocationByteScope::enter();
                    assert_empty_invocation();
                    record_invocation_bytes(71);
                    assert_invocation_bytes(71);
                }
                assert_invocation_bytes(bytes);
            }
        })
        .join()
        .unwrap();
    }

    #[test]
    fn thread_snapshots_include_nested_work_for_publication_deltas() {
        let before = store_thread_bytes();
        let reflink_before = thread_reflinked_bytes();
        let copied_before = thread_copied_bytes();
        {
            let _scope = InvocationByteScope::enter();
            record_invocation_bytes(10);
            {
                let _nested = InvocationByteScope::enter();
                record_invocation_bytes(20);
            }
            assert_invocation_bytes(10);
        }
        let after = store_thread_bytes();
        assert_eq!(after.reflinked - before.reflinked, 36);
        assert_eq!(after.hardlinked - before.hardlinked, 38);
        assert_eq!(after.copied - before.copied, 40);
        assert_eq!(after.copy_cross_device - before.copy_cross_device, 42);
        assert_eq!(after.copy_permission - before.copy_permission, 44);
        assert_eq!(after.copy_ineligible - before.copy_ineligible, 46);
        assert_eq!(after.copy_other - before.copy_other, 48);
        assert_eq!(thread_reflinked_bytes() - reflink_before, 30);
        assert_eq!(thread_copied_bytes() - copied_before, 34);
    }
}
