//! Counters for one synchronous compiler invocation.
//!
//! An explicit scope owns accounting while a daemon worker handles a compile.
//! Legacy callers without a scope retain process totals. Counts are deterministic
//! given the same compiler work and filesystem; timings measure elapsed work.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

struct InvocationCounts {
    start: Instant,
    child_rss: ChildRss,
    suspend_spawn_counts: Cell<u32>,
    compiler_runs: Cell<u32>,
    preprocessor_runs: Cell<u32>,
    probe_runs: Cell<u32>,
    dep_info_runs: Cell<u32>,
    startup_us: Cell<u64>,
    dep_info_us: Cell<u64>,
    flight_wait_us: Cell<u64>,
    permit_wait_us: Cell<u64>,
    prediction_mismatches: Cell<u64>,
}

thread_local! {
    static INVOCATION_COUNTS: RefCell<Option<Rc<InvocationCounts>>> = const { RefCell::new(None) };
}

/// Account for one synchronous compilation on this thread, starting at `start`.
///
/// Nested scopes restore their parent on drop. The guard cannot move between
/// threads and must not span an async suspension or task switch.
pub struct InvocationScope {
    previous: Option<Rc<InvocationCounts>>,
    _current: Rc<InvocationCounts>,
    _bytes: kache_store::opcounts::InvocationByteScope,
}

impl InvocationScope {
    pub fn enter(start: Instant) -> Self {
        let current = Rc::new(InvocationCounts {
            start,
            child_rss: ChildRss::default(),
            suspend_spawn_counts: Cell::new(0),
            compiler_runs: Cell::new(0),
            preprocessor_runs: Cell::new(0),
            probe_runs: Cell::new(0),
            dep_info_runs: Cell::new(0),
            startup_us: Cell::new(0),
            dep_info_us: Cell::new(0),
            flight_wait_us: Cell::new(0),
            permit_wait_us: Cell::new(0),
            prediction_mismatches: Cell::new(0),
        });
        let previous = INVOCATION_COUNTS.with(|slot| slot.replace(Some(Rc::clone(&current))));
        Self {
            previous,
            _current: current,
            _bytes: kache_store::opcounts::InvocationByteScope::enter(),
        }
    }
}

impl Drop for InvocationScope {
    fn drop(&mut self) {
        INVOCATION_COUNTS.with(|slot| slot.replace(self.previous.take()));
    }
}

/// Captured at spawn so reaping on another thread or inside a nested scope
/// still attributes the sample to the invocation that created the child.
#[derive(Clone, Default)]
pub(crate) struct ChildRss(std::sync::Arc<AtomicU64>);

impl ChildRss {
    #[cfg(any(unix, test))]
    pub(crate) fn record(&self, bytes: u64) {
        self.0.fetch_max(bytes, Ordering::Relaxed);
    }

    fn peak(&self) -> Option<u64> {
        match self.0.load(Ordering::Relaxed) {
            0 => None,
            bytes => Some(bytes),
        }
    }
}

#[cfg(any(unix, test))]
pub(crate) fn child_rss_recorder() -> Option<ChildRss> {
    INVOCATION_COUNTS.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|counts| counts.child_rss.clone())
    })
}

/// Outer None means legacy process accounting; Some(None) means this request
/// has not reaped a child with a usable memory sample.
pub(crate) fn invocation_child_rss() -> Option<Option<u64>> {
    INVOCATION_COUNTS.with(|slot| slot.borrow().as_ref().map(|counts| counts.child_rss.peak()))
}

fn add_u32(global: &AtomicU32, field: fn(&InvocationCounts) -> &Cell<u32>, value: u32) {
    INVOCATION_COUNTS.with(|slot| {
        if let Some(current) = slot.borrow().as_ref() {
            let counter = field(current);
            counter.set(counter.get() + value);
        } else {
            global.fetch_add(value, Ordering::Relaxed);
        }
    });
}

fn get_u32(global: &AtomicU32, field: fn(&InvocationCounts) -> &Cell<u32>) -> u32 {
    INVOCATION_COUNTS.with(|slot| {
        slot.borrow().as_ref().map_or_else(
            || global.load(Ordering::Relaxed),
            |current| field(current).get(),
        )
    })
}

fn add_u64(global: &AtomicU64, field: fn(&InvocationCounts) -> &Cell<u64>, value: u64) {
    INVOCATION_COUNTS.with(|slot| {
        if let Some(current) = slot.borrow().as_ref() {
            let counter = field(current);
            counter.set(counter.get() + value);
        } else {
            global.fetch_add(value, Ordering::Relaxed);
        }
    });
}

fn get_u64(global: &AtomicU64, field: fn(&InvocationCounts) -> &Cell<u64>) -> u64 {
    INVOCATION_COUNTS.with(|slot| {
        slot.borrow().as_ref().map_or_else(
            || global.load(Ordering::Relaxed),
            |current| field(current).get(),
        )
    })
}

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

/// Run `f` without counting compiler/preprocessor/probe spawns toward the
/// active invocation's event (or the legacy process total). Compile-and-compare
/// qualification still spawns rustc on a hit; the hit event must keep
/// `compiler_runs = 0` so warm-cache
/// assertions stay meaningful.
pub fn suspend_spawn_counts<T>(f: impl FnOnce() -> T) -> T {
    let invocation = INVOCATION_COUNTS.with(|slot| slot.borrow().clone());
    if let Some(current) = invocation.as_ref() {
        let depth = &current.suspend_spawn_counts;
        depth.set(depth.get() + 1);
    } else {
        SUSPEND_SPAWN_COUNTS.with(|depth| depth.set(depth.get() + 1));
    }
    struct Restore(Option<Rc<InvocationCounts>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            if let Some(current) = self.0.as_ref() {
                let depth = &current.suspend_spawn_counts;
                depth.set(depth.get().saturating_sub(1));
            } else {
                SUSPEND_SPAWN_COUNTS.with(|depth| depth.set(depth.get().saturating_sub(1)));
            }
        }
    }
    let _restore = Restore(invocation);
    f()
}

fn spawn_counts_suspended() -> bool {
    INVOCATION_COUNTS.with(|slot| {
        slot.borrow().as_ref().map_or_else(
            || SUSPEND_SPAWN_COUNTS.with(|depth| depth.get() > 0),
            |current| current.suspend_spawn_counts.get() > 0,
        )
    })
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
    add_u32(&COMPILER_RUNS, |counts| &counts.compiler_runs, 1);
}

/// Record that kache spawned the preprocessor (`cc -E`) — currently
/// done once per C/C++ compile to derive the cache key. Always zero for
/// rustc, which has no separate preprocess step.
pub fn record_preprocessor_run() {
    if spawn_counts_suspended() {
        return;
    }
    add_u32(&PREPROCESSOR_RUNS, |counts| &counts.preprocessor_runs, 1);
}

/// Compiler spawns for the active invocation, or the legacy process total.
pub fn compiler_runs() -> u32 {
    get_u32(&COMPILER_RUNS, |counts| &counts.compiler_runs)
}

/// Preprocessor spawns for the active invocation, or the legacy process total.
pub fn preprocessor_runs() -> u32 {
    get_u32(&PREPROCESSOR_RUNS, |counts| &counts.preprocessor_runs)
}

/// Record that kache ran a compiler probe — `<cc> --version` (and, in
/// future, `cc -###`). Probes are memoized through an on-disk cache, so
/// a build records one of these the first time it sees a compiler and
/// zero thereafter; a fully warm probe cache records zero.
pub fn record_probe_run() {
    if spawn_counts_suspended() {
        return;
    }
    add_u32(&PROBE_RUNS, |counts| &counts.probe_runs, 1);
}

/// Compiler probes for the active invocation, or the legacy process total.
pub fn probe_runs() -> u32 {
    get_u32(&PROBE_RUNS, |counts| &counts.probe_runs)
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

/// Pin the legacy process start. Called first thing in `main`, before argv handling
/// or config load, so `startup_ms` covers everything that runs before the
/// wrapper's first cache decision. A second call changes nothing.
///
/// Process-wide and permanent: in a test binary, once any test pins it,
/// every later in-process wrapper run measures `elapsed_ms` from that pin.
/// Tests must not assert an upper bound on legacy `elapsed_ms` or `startup_ms`.
/// An invocation scope keeps its injected start instead.
#[cfg(test)]
pub fn mark_process_start() {
    mark_process_start_at(Instant::now());
}

pub fn mark_process_start_at(started_at: Instant) {
    let _ = PROCESS_START.set(started_at);
}

/// The active invocation's injected start, otherwise the instant `main` pinned.
/// Returns `None` when neither exists (unit tests, library callers); the wrapper
/// then anchors elapsed time at its own entry and records no startup.
pub fn process_start() -> Option<Instant> {
    INVOCATION_COUNTS.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|current| current.start)
            .or_else(|| PROCESS_START.get().copied())
    })
}

/// Record the time from process start to the wrapper's entry.
pub fn record_startup(spent: Duration) {
    add_u64(&STARTUP_US, |counts| &counts.startup_us, micros(spent));
}

/// Startup time for the active invocation, or the process total, in milliseconds.
pub fn startup_ms() -> u64 {
    ms_from_micros(get_u64(&STARTUP_US, |counts| &counts.startup_us))
}

/// Record one `rustc --emit=dep-info` pre-pass spawn and the time spent
/// waiting for it. Counted whether or not rustc succeeded: the spawn happened.
pub fn record_dep_info_run(spent: Duration) {
    add_u32(&DEP_INFO_RUNS, |counts| &counts.dep_info_runs, 1);
    add_u64(&DEP_INFO_US, |counts| &counts.dep_info_us, micros(spent));
}

/// Dep-info pre-pass spawns for the active invocation, or the process total.
pub fn dep_info_runs() -> u32 {
    get_u32(&DEP_INFO_RUNS, |counts| &counts.dep_info_runs)
}

/// Dep-info time for the active invocation, or the process total, in milliseconds.
pub fn dep_info_ms() -> u64 {
    ms_from_micros(get_u64(&DEP_INFO_US, |counts| &counts.dep_info_us))
}

static PREDICTION_MISMATCHES: AtomicU64 = AtomicU64::new(0);

/// Record a sampled verification where the predicted closure differed from
/// the pre-pass closure. The pre-pass result won; the record was refreshed
/// from it downstream.
pub fn record_prediction_mismatch() {
    add_u64(
        &PREDICTION_MISMATCHES,
        |counts| &counts.prediction_mismatches,
        1,
    );
}

/// Verification mismatches for the active invocation, or the process total.
pub fn prediction_mismatches() -> u64 {
    get_u64(&PREDICTION_MISMATCHES, |counts| {
        &counts.prediction_mismatches
    })
}

/// Record time blocked joining a scheduler flight another process owned.
pub fn record_flight_wait(spent: Duration) {
    add_u64(
        &FLIGHT_WAIT_US,
        |counts| &counts.flight_wait_us,
        micros(spent),
    );
}

/// Record time blocked acquiring scheduler permit slots.
pub fn record_permit_wait(spent: Duration) {
    add_u64(
        &PERMIT_WAIT_US,
        |counts| &counts.permit_wait_us,
        micros(spent),
    );
}

/// Flight wait for the active invocation, or the process total, in milliseconds.
pub fn flight_wait_ms() -> u64 {
    ms_from_micros(get_u64(&FLIGHT_WAIT_US, |counts| &counts.flight_wait_us))
}

/// Permit wait for the active invocation, or the process total, in milliseconds.
pub fn permit_wait_ms() -> u64 {
    ms_from_micros(get_u64(&PERMIT_WAIT_US, |counts| &counts.permit_wait_us))
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

    fn record_invocation_work(repetitions: u32) {
        for _ in 0..repetitions {
            record_compiler_run();
            record_preprocessor_run();
            record_probe_run();
            record_startup(Duration::from_millis(2));
            record_dep_info_run(Duration::from_millis(3));
            record_flight_wait(Duration::from_millis(5));
            record_permit_wait(Duration::from_millis(7));
            record_prediction_mismatch();
            record_reflinked(11);
            record_hardlinked(13);
            record_copied(17);
        }
    }

    fn assert_invocation_work(repetitions: u32) {
        assert_eq!(compiler_runs(), repetitions);
        assert_eq!(preprocessor_runs(), repetitions);
        assert_eq!(probe_runs(), repetitions);
        assert_eq!(dep_info_runs(), repetitions);
        let repetitions = u64::from(repetitions);
        assert_eq!(startup_ms(), repetitions * 2);
        assert_eq!(dep_info_ms(), repetitions * 3);
        assert_eq!(flight_wait_ms(), repetitions * 5);
        assert_eq!(permit_wait_ms(), repetitions * 7);
        assert_eq!(prediction_mismatches(), repetitions);
        assert_eq!(reflinked_bytes(), repetitions * 11);
        assert_eq!(hardlinked_bytes(), repetitions * 13);
        assert_eq!(copied_bytes(), repetitions * 17);
    }

    #[test]
    fn child_memory_owners_survive_nested_scopes_and_cross_thread_reaping() {
        let worker = std::thread::spawn(|| {
            assert_eq!(invocation_child_rss(), None);
            assert!(child_rss_recorder().is_none());
            for _ in 0..2 {
                let _parent = InvocationScope::enter(Instant::now());
                assert_eq!(invocation_child_rss(), Some(None));
                let parent = child_rss_recorder().unwrap();
                parent.record(8192);
                parent.record(0);
                parent.record(4096);
                assert_eq!(invocation_child_rss(), Some(Some(8192)));
                {
                    let _nested = InvocationScope::enter(Instant::now());
                    let nested = child_rss_recorder().unwrap();
                    std::thread::scope(|threads| {
                        threads.spawn(|| parent.record(16384));
                        threads.spawn(|| nested.record(2048));
                    });
                    assert_eq!(invocation_child_rss(), Some(Some(2048)));
                }
                assert_eq!(invocation_child_rss(), Some(Some(16384)));
            }
            assert_eq!(invocation_child_rss(), None);
        });
        worker.join().unwrap();
    }

    #[test]
    fn concurrent_invocations_keep_counts_timings_and_bytes_separate() {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for repetitions in [2, 5] {
                let barrier = &barrier;
                threads.spawn(move || {
                    let start = Instant::now() - Duration::from_secs(60 + u64::from(repetitions));
                    let _scope = InvocationScope::enter(start);
                    barrier.wait();
                    record_invocation_work(repetitions);
                    assert_invocation_work(repetitions);
                    assert_eq!(process_start(), Some(start));
                    mark_process_start();
                    assert_eq!(process_start(), Some(start));
                });
            }
        });
    }

    #[test]
    fn reused_worker_and_nested_scopes_restore_their_own_metrics() {
        let worker = std::thread::spawn(|| {
            let parent_start = Instant::now() - Duration::from_secs(30);
            for repetitions in [2, 4] {
                let _parent = InvocationScope::enter(parent_start);
                assert_invocation_work(0);
                record_invocation_work(repetitions);
                {
                    let child_start = Instant::now();
                    let _child = InvocationScope::enter(child_start);
                    assert_invocation_work(0);
                    assert_eq!(process_start(), Some(child_start));
                    record_invocation_work(3);
                    assert_invocation_work(3);
                }
                assert_invocation_work(repetitions);
                assert_eq!(process_start(), Some(parent_start));
            }
        });
        worker.join().unwrap();
    }

    #[test]
    fn invocation_suspension_is_nested_and_restored_after_panic() {
        let _parent = InvocationScope::enter(Instant::now());
        suspend_spawn_counts(|| {
            record_compiler_run();
            record_preprocessor_run();
            record_probe_run();
            {
                let _child = InvocationScope::enter(Instant::now());
                record_compiler_run();
                assert_eq!(compiler_runs(), 1);
            }
            assert!(spawn_counts_suspended());
            suspend_spawn_counts(|| {
                record_compiler_run();
                assert!(spawn_counts_suspended());
            });
            assert!(spawn_counts_suspended());
        });
        assert_eq!(compiler_runs(), 0);
        assert_eq!(preprocessor_runs(), 0);
        assert_eq!(probe_runs(), 0);
        let _ = std::panic::catch_unwind(|| {
            suspend_spawn_counts(|| panic!("test panic"));
        });
        assert!(!spawn_counts_suspended());
        record_compiler_run();
        record_preprocessor_run();
        record_probe_run();
        assert_eq!(compiler_runs(), 1);
        assert_eq!(preprocessor_runs(), 1);
        assert_eq!(probe_runs(), 1);
    }

    #[test]
    fn invocation_scope_restores_parent_after_panic() {
        let start = Instant::now() - Duration::from_secs(30);
        let _scope = InvocationScope::enter(start);
        record_invocation_work(2);
        let _ = std::panic::catch_unwind(|| {
            let _nested = InvocationScope::enter(Instant::now());
            record_invocation_work(3);
            panic!("test panic");
        });
        assert_invocation_work(2);
        assert_eq!(process_start(), Some(start));
    }

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
