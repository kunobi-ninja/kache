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

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

static COMPILER_RUNS: AtomicU32 = AtomicU32::new(0);
static PREPROCESSOR_RUNS: AtomicU32 = AtomicU32::new(0);
static PROBE_RUNS: AtomicU32 = AtomicU32::new(0);

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

/// Record that kache ran a compiler probe — `<cc> --version` (and, in
/// future, `cc -###`). Probes are memoized through an on-disk cache, so
/// a build records one of these the first time it sees a compiler and
/// zero thereafter; a fully warm probe cache records zero.
pub fn record_probe_run() {
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
// ~no physical bytes. It falls back to a full copy on a filesystem without CoW
// (ext4 without reflink, tmpfs, a cross-volume store).
//
// Splitting store bytes by mechanism is what lets `kache report` (and the
// clone benchmark) account for disk honestly: a blob reflinked from the
// objdir is NOT a second physical copy, so a naive "objdir + store" sum
// double-counts it. Deterministic given the same source + filesystem.

static STORE_REFLINKED_BYTES: AtomicU64 = AtomicU64::new(0);
static STORE_COPIED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Record `bytes` ingested into the store by a CoW reflink (shares blocks
/// with the build's output file — physically zero-copy).
pub fn record_store_reflinked(bytes: u64) {
    STORE_REFLINKED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Record `bytes` ingested into the store by a full physical copy (the
/// filesystem has no CoW, so the blob is a genuine second copy).
pub fn record_store_copied(bytes: u64) {
    STORE_COPIED_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// Bytes ingested into the store by CoW reflink so far in this process.
pub fn store_reflinked_bytes() -> u64 {
    STORE_REFLINKED_BYTES.load(Ordering::Relaxed)
}

/// Bytes ingested into the store by a full copy so far in this process.
pub fn store_copied_bytes() -> u64 {
    STORE_COPIED_BYTES.load(Ordering::Relaxed)
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
    fn restore_byte_counters_increment_monotonically() {
        let before = reflinked_bytes() + hardlinked_bytes() + copied_bytes();
        record_reflinked(64);
        record_hardlinked(32);
        record_copied(16);
        assert!(reflinked_bytes() + hardlinked_bytes() + copied_bytes() >= before + 112);
    }

    #[test]
    fn store_byte_counters_increment_monotonically() {
        let before = store_reflinked_bytes() + store_copied_bytes();
        record_store_reflinked(128);
        record_store_copied(64);
        assert!(store_reflinked_bytes() + store_copied_bytes() >= before + 192);
    }
}
