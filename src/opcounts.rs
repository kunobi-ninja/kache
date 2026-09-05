//! Process-global counters for the work kache does while handling one
//! compile — external programs spawned, and bytes restored from cache.
//!
//! Each `kache` wrapper invocation is its own process and handles
//! exactly one compile, so a process-global counter read when the build
//! event is logged reflects that compile's work — no per-call plumbing
//! through the `Compiler` trait is needed.
//!
//! Unlike timings, these counts are **deterministic**: they do not
//! depend on machine speed, runner load, or filesystem-cache warmth. So
//! the e2e harness can assert on them as a perf-regression guard — e.g.
//! "a warm cache hit must not spawn the compiler" — with the same
//! reliability as a correctness assertion. Wall-clock budgets cannot do
//! that across the self-hosted / GitHub-hosted runner mix.

use std::cell::Cell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

static COMPILER_RUNS: AtomicU32 = AtomicU32::new(0);
static PREPROCESSOR_RUNS: AtomicU32 = AtomicU32::new(0);
static PROBE_RUNS: AtomicU32 = AtomicU32::new(0);

thread_local! {
    static SUSPEND_SPAWN_COUNTS: Cell<u32> = const { Cell::new(0) };
}

#[cfg(test)]
thread_local! {
    static SKIPPED_COMPILER_RUNS: Cell<u32> = const { Cell::new(0) };
}

/// Run `f` without counting compiler/preprocessor/probe spawns toward this
/// process's event. Compile-and-compare qualification still spawns rustc on
/// a hit; the hit event must keep `compiler_runs = 0` so warm-cache
/// assertions stay meaningful.
pub fn suspend_spawn_counts<T>(f: impl FnOnce() -> T) -> T {
    SUSPEND_SPAWN_COUNTS.with(|depth| depth.set(depth.get() + 1));
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            SUSPEND_SPAWN_COUNTS.with(|depth| depth.set(depth.get().saturating_sub(1)));
        }
    }
    let _restore = Restore;
    f()
}

fn spawn_counts_suspended() -> bool {
    SUSPEND_SPAWN_COUNTS.with(|depth| depth.get() > 0)
}

/// Record that kache spawned the underlying compiler — `rustc`, or a
/// C-family `cc -c` compile. A cache hit must record zero of these; a
/// miss records one.
pub fn record_compiler_run() {
    if spawn_counts_suspended() {
        #[cfg(test)]
        SKIPPED_COMPILER_RUNS.with(|count| count.set(count.get() + 1));
        return;
    }
    COMPILER_RUNS.fetch_add(1, Ordering::Relaxed);
}

/// Record that kache spawned the preprocessor (`cc -E`) — currently
/// done once per C/C++ compile to derive the cache key. Always zero for
/// rustc, which has no separate preprocess step.
pub fn record_preprocessor_run() {
    if spawn_counts_suspended() {
        return;
    }
    PREPROCESSOR_RUNS.fetch_add(1, Ordering::Relaxed);
}

/// Compiler spawns recorded so far in this process.
pub fn compiler_runs() -> u32 {
    COMPILER_RUNS.load(Ordering::Relaxed)
}

/// Preprocessor spawns recorded so far in this process.
pub fn preprocessor_runs() -> u32 {
    PREPROCESSOR_RUNS.load(Ordering::Relaxed)
}

/// Record that kache ran a compiler probe — `<cc> --version` (and, in
/// future, `cc -###`). Probes are memoized through an on-disk cache, so
/// a build records one of these the first time it sees a compiler and
/// zero thereafter; a fully warm probe cache records zero.
pub fn record_probe_run() {
    if spawn_counts_suspended() {
        return;
    }
    PROBE_RUNS.fetch_add(1, Ordering::Relaxed);
}

/// Compiler probes recorded so far in this process.
///
/// `#[allow(dead_code)]`: the probe op-count assertion in the e2e
/// harness is the production consumer and lands with the harness
/// change; today only the unit test below reads it.
#[allow(dead_code)]
pub fn probe_runs() -> u32 {
    PROBE_RUNS.load(Ordering::Relaxed)
}

// ── Restore-method byte counters ───────────────────────────────────────────
//
// A cache hit is restored by reflink (CoW — physically zero-copy *and*
// write-isolated), falling back to a hardlink, then to a full copy.
// Splitting restored bytes by mechanism lets `kache report` show how much
// disk the cache genuinely saved versus had to duplicate. Like the spawn
// counts above, these are deterministic given the same source + filesystem.

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

// ── Wrapper phase timings ───────────────────────────────────────────────────
//
// `key_ms`, `lookup_ms`, `restore_ms` and `store_ms` are measured inline in
// the wrapper and handed to the event by hand. The phases below happen in
// code the wrapper does not call directly (the dep-info pre-pass inside key
// computation, the scheduler's flight and permit waits) or before the wrapper
// exists at all (process startup), so they accumulate here and are read once
// at the single event write site, like the spawn counts above. Microseconds
// are kept internally; the event rounds down to whole milliseconds.

static PROCESS_START: OnceLock<Instant> = OnceLock::new();
static STARTUP_US: AtomicU64 = AtomicU64::new(0);
static DEP_INFO_US: AtomicU64 = AtomicU64::new(0);
static DEP_INFO_RUNS: AtomicU32 = AtomicU32::new(0);
static FLIGHT_WAIT_US: AtomicU64 = AtomicU64::new(0);
static PERMIT_WAIT_US: AtomicU64 = AtomicU64::new(0);

/// Pin the process start. Called first thing in `main`, before argv handling
/// or config load, so `startup_ms` covers everything that runs before the
/// wrapper's first cache decision. A second call changes nothing.
///
/// Process-wide and permanent: in a test binary, once any test pins it,
/// every later in-process wrapper run measures `elapsed_ms` from that pin.
/// Tests must not assert an upper bound on `elapsed_ms` or `startup_ms`.
pub fn mark_process_start() {
    let _ = PROCESS_START.set(Instant::now());
}

/// The instant `main` pinned, or `None` when this process never went through
/// `main` (unit tests, library callers). The wrapper then anchors elapsed
/// time at its own entry and records no startup.
pub fn process_start() -> Option<Instant> {
    PROCESS_START.get().copied()
}

/// Record the time from process start to the wrapper's entry.
pub fn record_startup(spent: Duration) {
    STARTUP_US.fetch_add(micros(spent), Ordering::Relaxed);
}

/// Process start to wrapper entry so far in this process, whole milliseconds.
pub fn startup_ms() -> u64 {
    ms_from_micros(STARTUP_US.load(Ordering::Relaxed))
}

/// Record one `rustc --emit=dep-info` pre-pass spawn and the time spent
/// waiting for it. Counted whether or not rustc succeeded: the spawn happened.
pub fn record_dep_info_run(spent: Duration) {
    DEP_INFO_RUNS.fetch_add(1, Ordering::Relaxed);
    DEP_INFO_US.fetch_add(micros(spent), Ordering::Relaxed);
}

/// Dep-info pre-pass spawns so far in this process.
pub fn dep_info_runs() -> u32 {
    DEP_INFO_RUNS.load(Ordering::Relaxed)
}

/// Time spent in dep-info pre-pass spawns so far, whole milliseconds.
pub fn dep_info_ms() -> u64 {
    ms_from_micros(DEP_INFO_US.load(Ordering::Relaxed))
}

static PREDICTION_MISMATCHES: AtomicU64 = AtomicU64::new(0);

/// Record a sampled verification where the predicted closure differed from
/// the pre-pass closure. The pre-pass result won; the record was refreshed
/// from it downstream.
pub fn record_prediction_mismatch() {
    PREDICTION_MISMATCHES.fetch_add(1, Ordering::Relaxed);
}

/// Sampled-verification mismatches so far in this process.
pub fn prediction_mismatches() -> u64 {
    PREDICTION_MISMATCHES.load(Ordering::Relaxed)
}

/// Record time blocked joining a scheduler flight another process owned.
pub fn record_flight_wait(spent: Duration) {
    FLIGHT_WAIT_US.fetch_add(micros(spent), Ordering::Relaxed);
}

/// Record time blocked acquiring scheduler permit slots.
pub fn record_permit_wait(spent: Duration) {
    PERMIT_WAIT_US.fetch_add(micros(spent), Ordering::Relaxed);
}

/// Flight-join wait so far in this process, whole milliseconds.
pub fn flight_wait_ms() -> u64 {
    ms_from_micros(FLIGHT_WAIT_US.load(Ordering::Relaxed))
}

/// Permit wait so far in this process, whole milliseconds.
pub fn permit_wait_ms() -> u64 {
    ms_from_micros(PERMIT_WAIT_US.load(Ordering::Relaxed))
}

fn micros(spent: Duration) -> u64 {
    u64::try_from(spent.as_micros()).unwrap_or(u64::MAX)
}

fn ms_from_micros(micros: u64) -> u64 {
    micros / 1000
}

#[cfg(test)]
mod tests {
    use super::*;

    // The counters are process-global and only ever increment (no
    // reset), so these assertions are safe under parallel test
    // execution: `after > before` holds regardless of what other
    // tests increment concurrently.

    #[test]
    fn record_compiler_run_increments_monotonically() {
        let before = compiler_runs();
        record_compiler_run();
        assert!(compiler_runs() > before);
    }

    #[test]
    fn record_preprocessor_run_increments_monotonically() {
        let before = preprocessor_runs();
        record_preprocessor_run();
        assert!(preprocessor_runs() > before);
    }

    #[test]
    fn record_probe_run_increments_monotonically() {
        let before = probe_runs();
        record_probe_run();
        assert!(probe_runs() > before);
    }

    #[test]
    fn record_prediction_mismatch_increments_monotonically() {
        // Same process-global lock the verification tests hold: exact
        // counting needs isolation from their increments.
        let _lock = crate::test_support::process_state_test_lock();
        let before = prediction_mismatches();
        record_prediction_mismatch();
        record_prediction_mismatch();
        assert_eq!(prediction_mismatches(), before + 2);
    }

    #[test]
    fn suspend_spawn_counts_skips_compiler_run_on_this_thread() {
        let skipped_before = SKIPPED_COMPILER_RUNS.with(|count| count.get());
        assert!(
            !SUSPEND_SPAWN_COUNTS.with(|depth| depth.get() > 0),
            "this test thread must start unsuspended"
        );
        suspend_spawn_counts(|| {
            assert!(SUSPEND_SPAWN_COUNTS.with(|depth| depth.get() > 0));
            record_compiler_run();
            suspend_spawn_counts(|| {
                assert!(SUSPEND_SPAWN_COUNTS.with(|depth| depth.get() >= 2));
            });
        });
        assert!(SUSPEND_SPAWN_COUNTS.with(|depth| depth.get() == 0));
        let skipped = SKIPPED_COMPILER_RUNS.with(|count| count.get());
        assert_eq!(skipped, skipped_before + 1);
        record_compiler_run();
        assert_eq!(
            SKIPPED_COMPILER_RUNS.with(|count| count.get()),
            skipped,
            "an unsuspended compiler run must not count as skipped"
        );
    }

    #[test]
    fn mark_process_start_pins_one_instant() {
        mark_process_start();
        let pinned = process_start().expect("main's mark must be readable");
        assert!(pinned <= Instant::now());
        // A second mark must not move the anchor: `startup_ms` is measured
        // from the first one.
        mark_process_start();
        assert_eq!(process_start(), Some(pinned));
    }

    #[test]
    fn startup_accumulates_whole_milliseconds() {
        let before = startup_ms();
        record_startup(Duration::from_micros(7_400));
        assert!(startup_ms() >= before + 7);
    }

    #[test]
    fn dep_info_run_counts_and_times_each_spawn() {
        let runs_before = dep_info_runs();
        let ms_before = dep_info_ms();
        record_dep_info_run(Duration::from_millis(12));
        record_dep_info_run(Duration::from_millis(3));
        assert!(dep_info_runs() >= runs_before + 2);
        assert!(dep_info_ms() >= ms_before + 15);
    }

    #[test]
    fn scheduler_waits_accumulate_in_their_own_counters() {
        let flight_before = flight_wait_ms();
        let permit_before = permit_wait_ms();
        record_flight_wait(Duration::from_millis(5));
        record_permit_wait(Duration::from_millis(9));
        assert!(flight_wait_ms() >= flight_before + 5);
        assert!(permit_wait_ms() >= permit_before + 9);
    }

    #[test]
    fn ms_from_micros_rounds_down() {
        assert_eq!(ms_from_micros(0), 0);
        assert_eq!(ms_from_micros(999), 0);
        assert_eq!(ms_from_micros(1_000), 1);
        assert_eq!(ms_from_micros(7_500), 7);
    }

    #[test]
    fn micros_saturates_instead_of_wrapping() {
        assert_eq!(micros(Duration::from_micros(1_234)), 1_234);
        assert_eq!(micros(Duration::MAX), u64::MAX);
    }

    #[test]
    fn suspend_spawn_counts_restores_after_panic() {
        let _ = std::panic::catch_unwind(|| {
            suspend_spawn_counts(|| panic!("qualification compile failed"));
        });
        assert!(
            SUSPEND_SPAWN_COUNTS.with(|depth| depth.get() == 0),
            "panic inside suspend must not leave spawn counts suppressed"
        );
    }

    #[test]
    fn restore_byte_counters_increment_monotonically() {
        let before = reflinked_bytes() + hardlinked_bytes() + copied_bytes();
        record_reflinked(64);
        record_hardlinked(32);
        record_copied(16);
        assert!(reflinked_bytes() + hardlinked_bytes() + copied_bytes() >= before + 112);
    }

    #[test]
    fn store_byte_counters_increment_monotonically() {
        let before = store_reflinked_bytes() + store_hardlinked_bytes() + store_copied_bytes();
        record_store_reflinked(128);
        record_store_hardlinked(32);
        record_store_copied(64);
        assert!(
            store_reflinked_bytes() + store_hardlinked_bytes() + store_copied_bytes()
                >= before + 224
        );
    }

    #[test]
    fn store_copy_cross_device_counter_increments() {
        let before = store_copy_cross_device_bytes();
        record_store_copy_cross_device(11);
        assert!(store_copy_cross_device_bytes() >= before + 11);
    }

    #[test]
    fn store_copy_permission_counter_increments() {
        let before = store_copy_permission_bytes();
        record_store_copy_permission(13);
        assert!(store_copy_permission_bytes() >= before + 13);
    }

    #[test]
    fn store_copy_ineligible_counter_increments() {
        let before = store_copy_ineligible_bytes();
        record_store_copy_ineligible(17);
        assert!(store_copy_ineligible_bytes() >= before + 17);
    }

    #[test]
    fn store_copy_other_counter_increments() {
        let before = store_copy_other_bytes();
        record_store_copy_other(19);
        assert!(store_copy_other_bytes() >= before + 19);
    }

    #[test]
    fn restore_copy_cross_device_counter_increments() {
        let before = restore_copy_cross_device_bytes();
        record_restore_copy_cross_device(23);
        assert!(restore_copy_cross_device_bytes() >= before + 23);
    }

    #[test]
    fn restore_copy_permission_counter_increments() {
        let before = restore_copy_permission_bytes();
        record_restore_copy_permission(29);
        assert!(restore_copy_permission_bytes() >= before + 29);
    }

    #[test]
    #[cfg(unix)]
    fn restore_copy_exclusive_counter_increments() {
        let before = restore_copy_exclusive_bytes();
        record_restore_copy_exclusive(31);
        assert!(restore_copy_exclusive_bytes() >= before + 31);
    }

    #[test]
    fn restore_copy_other_counter_increments() {
        let before = restore_copy_other_bytes();
        record_restore_copy_other(37);
        assert!(restore_copy_other_bytes() >= before + 37);
    }
}
