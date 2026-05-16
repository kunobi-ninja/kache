//! Process-global counters for the external programs kache spawns
//! while handling one compile.
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

use std::sync::atomic::{AtomicU32, Ordering};

static COMPILER_RUNS: AtomicU32 = AtomicU32::new(0);
static PREPROCESSOR_RUNS: AtomicU32 = AtomicU32::new(0);

/// Record that kache spawned the underlying compiler — `rustc`, or a
/// C-family `cc -c` compile. A cache hit must record zero of these; a
/// miss records one.
pub fn record_compiler_run() {
    COMPILER_RUNS.fetch_add(1, Ordering::Relaxed);
}

/// Record that kache spawned the preprocessor (`cc -E`) — currently
/// done once per C/C++ compile to derive the cache key. Always zero for
/// rustc, which has no separate preprocess step.
pub fn record_preprocessor_run() {
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
}
