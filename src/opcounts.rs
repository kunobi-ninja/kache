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

pub use kache_store::opcounts::*;

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
}
