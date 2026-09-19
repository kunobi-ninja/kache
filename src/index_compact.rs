//! Daemon-side compaction of `index.db`.
//!
//! The index keeps `auto_vacuum` off, so pages freed by a migration or a
//! prune stay in the file until a VACUUM. The daemon runs that VACUUM when
//! the machine looks quiet, with a bounded fallback for hosts that never are.

use crate::cache_key::{IndexCompaction, IndexPageStats};
use crate::config::Config;
use crate::store::Store;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// No wrapper request for this long counts as quiet. Permits cover only the
/// miss path; a build made of hits holds none but still sends requests.
pub(crate) const QUIET_AFTER: Duration = Duration::from_secs(60);
/// How long the index may stay over the threshold before a busy host
/// compacts anyway.
pub(crate) const FORCE_AFTER: Duration = Duration::from_secs(6 * 3600);
/// Largest live index a forced or shutdown compaction rewrites. Readers keep
/// working during a WAL-mode VACUUM; writers wait. A local SSD rewrote about
/// 180 MiB/s, so 1 GiB keeps that wait near the wrappers' 5 s busy timeout.
pub(crate) const FORCE_MAX_LIVE_BYTES: u64 = 1 << 30;
/// Largest live index a quiet compaction rewrites. VACUUM holds the write
/// lock for the whole rewrite, and a build that starts meanwhile waits on it.
/// Above this the file is left to `kache doctor --repair`.
pub(crate) const QUIET_MAX_LIVE_BYTES: u64 = 8 << 30;
/// Delay before the first check, short enough that a daemon living for one
/// CI job still gets one.
const FIRST_CHECK_AFTER: Duration = Duration::from_secs(60);
const CHECK_INTERVAL: Duration = Duration::from_secs(300);

/// Monotonic record of the last wrapper request the daemon accepted.
#[derive(Debug)]
pub(crate) struct RequestClock {
    origin: Instant,
    last_ms: AtomicU64,
}

impl RequestClock {
    /// Daemon start counts as a request.
    pub(crate) fn new() -> Self {
        Self {
            origin: Instant::now(),
            last_ms: AtomicU64::new(0),
        }
    }

    fn ms_at(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.origin).as_millis() as u64
    }

    pub(crate) fn touch(&self, at: Instant) {
        self.last_ms.fetch_max(self.ms_at(at), Ordering::Relaxed);
    }

    fn idle_at(&self, now: Instant) -> Duration {
        let last = self.last_ms.load(Ordering::Relaxed);
        Duration::from_millis(self.ms_at(now).saturating_sub(last))
    }

    pub(crate) fn idle_for(&self) -> Duration {
        self.idle_at(Instant::now())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// No compile permits held and no recent request.
    Quiet,
    /// Over the threshold for [`FORCE_AFTER`] with a small live index.
    Forced,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Mode::Quiet => "quiet",
            Mode::Forced => "forced",
        }
    }

    /// A quiet compaction yields to the first contender. A forced one waits
    /// like any other writer, or a busy host would never get through.
    fn busy_timeout_ms(self) -> u32 {
        match self {
            Mode::Quiet => 0,
            Mode::Forced => 5000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// Compiles hold permits, or a wrapper request arrived recently.
    Busy,
    /// The permit slots could not be read.
    UnknownLoad,
    /// The live index is above the cap for the mode that would run:
    /// [`QUIET_MAX_LIVE_BYTES`] or [`FORCE_MAX_LIVE_BYTES`].
    TooLarge,
    /// A GC holds `gc.lock`; the two must not overlap.
    GcRunning,
}

impl SkipReason {
    fn label(self) -> &'static str {
        match self {
            SkipReason::Busy => "builds are active",
            SkipReason::UnknownLoad => "build load is unknown",
            SkipReason::TooLarge => {
                "live index is too large to rewrite unattended; run `kache doctor --repair`"
            }
            SkipReason::GcRunning => "a GC holds gc.lock",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    NotNeeded,
    Run(Mode),
    Skip(SkipReason),
}

/// Whether to compact now.
///
/// A quiet machine may while the live data is at most
/// [`QUIET_MAX_LIVE_BYTES`]. A machine that is never quiet compacts once
/// the index has been over the threshold for [`FORCE_AFTER`], but only while
/// the live data is at most [`FORCE_MAX_LIVE_BYTES`]: VACUUM rewrites live
/// pages only, so with little live data the exclusive lock lasts seconds and
/// wrappers wait it out inside their busy timeout.
pub(crate) fn decide(
    over_threshold: bool,
    permits: Option<u32>,
    since_last_request: Duration,
    over_threshold_for: Duration,
    live_bytes: u64,
) -> Decision {
    if !over_threshold {
        return Decision::NotNeeded;
    }
    if permits == Some(0) && since_last_request >= QUIET_AFTER {
        if live_bytes <= QUIET_MAX_LIVE_BYTES {
            return Decision::Run(Mode::Quiet);
        }
        return Decision::Skip(SkipReason::TooLarge);
    }
    if over_threshold_for >= FORCE_AFTER {
        if live_bytes <= FORCE_MAX_LIVE_BYTES {
            return Decision::Run(Mode::Forced);
        }
        return Decision::Skip(SkipReason::TooLarge);
    }
    Decision::Skip(match permits {
        None => SkipReason::UnknownLoad,
        Some(_) => SkipReason::Busy,
    })
}

/// The decision at daemon shutdown. The shutdown request itself just
/// arrived, so request age is waived. The size cap keeps shutdown short.
pub(crate) fn decide_at_shutdown(
    over_threshold: bool,
    permits: Option<u32>,
    live_bytes: u64,
) -> Decision {
    if over_threshold && live_bytes > FORCE_MAX_LIVE_BYTES {
        return Decision::Skip(SkipReason::TooLarge);
    }
    decide(
        over_threshold,
        permits,
        Duration::MAX,
        Duration::ZERO,
        live_bytes,
    )
}

/// What asked for the compaction.
#[derive(Clone, Copy)]
pub(crate) enum Trigger<'a> {
    Periodic(&'a RequestClock),
    Shutdown,
}

impl Trigger<'_> {
    fn decide(
        self,
        stats: &IndexPageStats,
        permits: Option<u32>,
        over_threshold_for: Duration,
    ) -> Decision {
        match self {
            Trigger::Periodic(clock) => decide(
                stats.should_compact(),
                permits,
                clock.idle_for(),
                over_threshold_for,
                stats.live_bytes(),
            ),
            Trigger::Shutdown => {
                decide_at_shutdown(stats.should_compact(), permits, stats.live_bytes())
            }
        }
    }
}

/// When the index was first seen over the threshold. Kept on disk because a
/// CI daemon restarts per job and an in-memory timer would never reach
/// [`FORCE_AFTER`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct OverThreshold {
    /// Unix seconds.
    since: u64,
}

fn state_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("index-compact.json")
}

fn read_since(cache_dir: &Path) -> Option<u64> {
    let json = std::fs::read(state_path(cache_dir)).ok()?;
    serde_json::from_slice::<OverThreshold>(&json)
        .ok()
        .map(|state| state.since)
}

/// A missing or corrupt record, or one from the future after the clock went
/// backwards, restarts the timer at `now`.
fn resolve_since(stored: Option<u64>, now: u64) -> u64 {
    stored.filter(|since| *since <= now).unwrap_or(now)
}

/// How long the index has been over the threshold, recording `now` as the
/// start when nothing usable is on disk.
fn observe_over_threshold(cache_dir: &Path, now: u64) -> Duration {
    let stored = read_since(cache_dir);
    let since = resolve_since(stored, now);
    if stored != Some(since)
        && let Ok(json) = serde_json::to_vec(&OverThreshold { since })
        && let Err(error) = crate::atomic::atomic_replace(&state_path(cache_dir), &json)
    {
        tracing::debug!("index compaction: could not record the threshold time: {error:#}");
    }
    Duration::from_secs(now.saturating_sub(since))
}

fn clear_state(cache_dir: &Path) {
    let _ = std::fs::remove_file(state_path(cache_dir));
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    NotNeeded,
    Skipped(SkipReason),
    Attempted {
        mode: Mode,
        result: IndexCompaction,
        elapsed: Duration,
    },
}

impl Outcome {
    /// An attempt, or a skip that waiting will not cure, is worth an info
    /// line. "Nothing to do" and "builds are active" repeat every tick.
    fn is_noteworthy(&self) -> bool {
        match self {
            Outcome::NotNeeded => false,
            Outcome::Skipped(reason) => matches!(reason, SkipReason::TooLarge),
            Outcome::Attempted { .. } => true,
        }
    }

    fn describe(&self) -> String {
        match *self {
            Outcome::NotNeeded => "index compaction not needed".to_string(),
            Outcome::Skipped(reason) => {
                format!("index compaction deferred: {}", reason.label())
            }
            Outcome::Attempted {
                mode,
                result,
                elapsed,
            } => {
                let mode = mode.label();
                let ms = elapsed.as_millis();
                match result {
                    IndexCompaction::Compacted { before, after } => {
                        format!("index compacted ({mode}): {before} -> {after} bytes in {ms} ms")
                    }
                    IndexCompaction::Busy => {
                        format!(
                            "index compaction ({mode}) found the index busy after {ms} ms; will retry"
                        )
                    }
                    IndexCompaction::InsufficientSpace { needed, free } => format!(
                        "index compaction ({mode}) needs {needed} bytes of free disk, found {free}; will retry"
                    ),
                    IndexCompaction::NotNeeded => {
                        format!("index compaction ({mode}) found nothing to reclaim")
                    }
                }
            }
        }
    }
}

/// Turn a decision that does not run into its outcome, clearing the
/// threshold record once the index is back under it.
fn settle(cache_dir: &Path, decision: Decision) -> Result<Mode, Outcome> {
    match decision {
        Decision::Run(mode) => Ok(mode),
        Decision::Skip(reason) => Err(Outcome::Skipped(reason)),
        Decision::NotNeeded => {
            clear_state(cache_dir);
            Err(Outcome::NotNeeded)
        }
    }
}

fn attempt(config: &Config, trigger: Trigger<'_>, now: u64) -> anyhow::Result<Outcome> {
    let started = Instant::now();
    let cache_dir = &config.cache_dir;
    // Own connection, like GC: a VACUUM must not sit on the daemon's Store
    // mutex.
    let store = Store::open(config)?;
    let index = store.file_hash_cache();
    let stats = index.index_page_stats()?;
    let over_for = if stats.should_compact() {
        observe_over_threshold(cache_dir, now)
    } else {
        Duration::ZERO
    };
    let decide = || {
        trigger.decide(
            &stats,
            crate::scheduler::permits_in_use(cache_dir),
            over_for,
        )
    };
    if let Err(outcome) = settle(cache_dir, decide()) {
        return Ok(outcome);
    }
    let Some(_gc_lock) = store.try_gc_lock()? else {
        return Ok(Outcome::Skipped(SkipReason::GcRunning));
    };
    // A build can start between the first decision and the lock.
    let mode = match settle(cache_dir, decide()) {
        Ok(mode) => mode,
        Err(outcome) => return Ok(outcome),
    };
    index
        .db()
        .pragma_update(None, "busy_timeout", mode.busy_timeout_ms())?;
    let result = index.compact_index()?;
    if matches!(
        result,
        IndexCompaction::Compacted { .. } | IndexCompaction::NotNeeded
    ) {
        clear_state(cache_dir);
    }
    Ok(Outcome::Attempted {
        mode,
        result,
        elapsed: started.elapsed(),
    })
}

/// One compaction attempt. Blocking: call from `spawn_blocking`. Errors are
/// logged and dropped; a later tick retries.
pub(crate) fn run(config: &Config, trigger: Trigger<'_>) -> Option<Outcome> {
    match attempt(config, trigger, unix_now_secs()) {
        Ok(outcome) => {
            if outcome.is_noteworthy() {
                tracing::info!("{}", outcome.describe());
            } else {
                tracing::debug!("{}", outcome.describe());
            }
            Some(outcome)
        }
        Err(error) => {
            tracing::warn!("index compaction failed: {error:#}");
            None
        }
    }
}

/// Check shortly after daemon start, then every few minutes. A check that
/// finds nothing to do costs one index open and three PRAGMAs.
pub(crate) fn spawn_periodic(
    config: Config,
    clock: Arc<RequestClock>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::time::sleep(FIRST_CHECK_AFTER).await;
        let mut interval = tokio::time::interval(CHECK_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let config = config.clone();
            let clock = clock.clone();
            let task = tokio::task::spawn_blocking(move || run(&config, Trigger::Periodic(&clock)));
            if let Err(error) = task.await {
                tracing::warn!("index compaction task panicked: {error}");
            }
        }
    })
}

/// The shutdown attempt: quiet mode only, so a contended index yields at once.
pub(crate) async fn run_at_shutdown(config: Config) {
    let task = tokio::task::spawn_blocking(move || run(&config, Trigger::Shutdown));
    if let Err(error) = task.await {
        tracing::warn!("index compaction task panicked: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use crate::store::StoreLock;

    const FORCE_CAP: u64 = 1024 * 1024 * 1024;
    const SEC: Duration = Duration::from_secs(1);

    #[test]
    fn thresholds_are_the_documented_ones() {
        assert_eq!(QUIET_AFTER, Duration::from_secs(60));
        assert_eq!(FORCE_AFTER, Duration::from_secs(21_600));
        assert_eq!(FORCE_MAX_LIVE_BYTES, 1_073_741_824);
        assert_eq!(QUIET_MAX_LIVE_BYTES, 8_589_934_592);
        assert_eq!(FIRST_CHECK_AFTER, Duration::from_secs(60));
        assert_eq!(CHECK_INTERVAL, Duration::from_secs(300));
        // 2026-01-01: a clock stuck at 0 would make every record look fresh.
        assert!(unix_now_secs() > 1_767_225_600);
    }

    #[test]
    fn an_index_under_the_threshold_is_never_compacted() {
        for permits in [None, Some(0), Some(1)] {
            assert_eq!(
                decide(false, permits, Duration::MAX, Duration::MAX, 0),
                Decision::NotNeeded
            );
        }
        assert_eq!(decide_at_shutdown(false, Some(0), 0), Decision::NotNeeded);
        assert_eq!(
            decide_at_shutdown(false, Some(0), u64::MAX),
            Decision::NotNeeded
        );
    }

    #[test]
    fn quiet_needs_zero_permits_and_a_minute_without_requests() {
        let quiet = |permits, idle| decide(true, permits, idle, Duration::ZERO, 0);
        assert_eq!(quiet(Some(0), QUIET_AFTER), Decision::Run(Mode::Quiet));
        assert_eq!(quiet(Some(0), Duration::MAX), Decision::Run(Mode::Quiet));
        assert_eq!(
            quiet(Some(0), QUIET_AFTER - SEC),
            Decision::Skip(SkipReason::Busy)
        );
        assert_eq!(
            quiet(Some(1), QUIET_AFTER),
            Decision::Skip(SkipReason::Busy)
        );
        assert_eq!(
            quiet(None, QUIET_AFTER),
            Decision::Skip(SkipReason::UnknownLoad)
        );
        assert_eq!(
            quiet(None, Duration::ZERO),
            Decision::Skip(SkipReason::UnknownLoad)
        );
    }

    #[test]
    fn quiet_leaves_a_large_live_index_alone() {
        let quiet = |live, over_for| decide(true, Some(0), QUIET_AFTER, over_for, live);
        assert_eq!(
            quiet(QUIET_MAX_LIVE_BYTES, Duration::ZERO),
            Decision::Run(Mode::Quiet)
        );
        assert_eq!(
            quiet(QUIET_MAX_LIVE_BYTES + 1, Duration::ZERO),
            Decision::Skip(SkipReason::TooLarge)
        );
        // The quiet cap is above the forced one, so waiting changes nothing.
        assert_eq!(
            quiet(QUIET_MAX_LIVE_BYTES + 1, Duration::MAX),
            Decision::Skip(SkipReason::TooLarge)
        );
        assert_eq!(QUIET_MAX_LIVE_BYTES, 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn a_busy_host_forces_after_six_hours_with_a_small_live_index() {
        let busy = |permits, over_for, live| decide(true, permits, Duration::ZERO, over_for, live);
        for permits in [None, Some(0), Some(1)] {
            assert_eq!(
                busy(permits, FORCE_AFTER, FORCE_CAP),
                Decision::Run(Mode::Forced)
            );
            assert_eq!(busy(permits, Duration::MAX, 0), Decision::Run(Mode::Forced));
            assert_eq!(
                busy(permits, FORCE_AFTER, FORCE_CAP + 1),
                Decision::Skip(SkipReason::TooLarge)
            );
        }
        assert_eq!(
            busy(Some(1), FORCE_AFTER - SEC, 0),
            Decision::Skip(SkipReason::Busy)
        );
        assert_eq!(
            busy(None, FORCE_AFTER - SEC, 0),
            Decision::Skip(SkipReason::UnknownLoad)
        );
    }

    #[test]
    fn quiet_wins_over_forced_and_ignores_the_size_cap() {
        assert_eq!(
            decide(true, Some(0), QUIET_AFTER, FORCE_AFTER, FORCE_CAP + 1),
            Decision::Run(Mode::Quiet)
        );
    }

    #[test]
    fn shutdown_waives_request_age_but_not_permits_or_size() {
        assert_eq!(
            decide_at_shutdown(true, Some(0), FORCE_CAP),
            Decision::Run(Mode::Quiet)
        );
        assert_eq!(
            decide_at_shutdown(true, Some(0), FORCE_CAP + 1),
            Decision::Skip(SkipReason::TooLarge)
        );
        assert_eq!(
            decide_at_shutdown(true, Some(1), 0),
            Decision::Skip(SkipReason::Busy)
        );
        assert_eq!(
            decide_at_shutdown(true, None, 0),
            Decision::Skip(SkipReason::UnknownLoad)
        );
    }

    #[test]
    fn trigger_feeds_the_matching_decision() {
        let sparse = IndexPageStats {
            pages: 100_000,
            free_pages: 90_000,
            page_size: 4096,
        };
        let dense = IndexPageStats {
            free_pages: 0,
            ..sparse
        };
        let clock = RequestClock::new();
        let periodic = Trigger::Periodic(&clock);
        // The clock was touched at creation, so the machine is not quiet.
        assert_eq!(
            periodic.decide(&sparse, Some(0), Duration::ZERO),
            Decision::Skip(SkipReason::Busy)
        );
        assert_eq!(
            periodic.decide(&sparse, Some(0), FORCE_AFTER),
            Decision::Run(Mode::Forced)
        );
        assert_eq!(
            periodic.decide(&dense, Some(0), FORCE_AFTER),
            Decision::NotNeeded
        );
        assert_eq!(
            Trigger::Shutdown.decide(&sparse, Some(0), Duration::ZERO),
            Decision::Run(Mode::Quiet)
        );
        assert_eq!(
            Trigger::Shutdown.decide(&sparse, Some(2), FORCE_AFTER),
            Decision::Skip(SkipReason::Busy)
        );
        assert_eq!(
            Trigger::Shutdown.decide(&dense, Some(0), Duration::ZERO),
            Decision::NotNeeded
        );
        let huge = IndexPageStats {
            pages: 2_000_000,
            free_pages: 1_000_000,
            page_size: 4096,
        };
        assert!(huge.should_compact() && huge.live_bytes() > FORCE_CAP);
        assert_eq!(
            Trigger::Shutdown.decide(&huge, Some(0), Duration::ZERO),
            Decision::Skip(SkipReason::TooLarge)
        );
    }

    #[test]
    fn request_clock_measures_time_since_the_latest_request() {
        let clock = RequestClock::new();
        let t0 = clock.origin;
        assert_eq!(clock.idle_at(t0), Duration::ZERO);
        assert_eq!(clock.idle_at(t0 + 90 * SEC), 90 * SEC);

        clock.touch(t0 + 30 * SEC);
        assert_eq!(clock.idle_at(t0 + 90 * SEC), 60 * SEC);
        // An older request must not move the mark back.
        clock.touch(t0 + 10 * SEC);
        assert_eq!(clock.idle_at(t0 + 90 * SEC), 60 * SEC);
        // A reading from before the last request is zero, not a wraparound.
        assert_eq!(clock.idle_at(t0 + 20 * SEC), Duration::ZERO);
        assert!(clock.idle_for() < 30 * SEC);
    }

    #[test]
    fn modes_and_reasons_have_distinct_labels() {
        assert_eq!(Mode::Quiet.label(), "quiet");
        assert_eq!(Mode::Forced.label(), "forced");
        assert_eq!(Mode::Quiet.busy_timeout_ms(), 0);
        assert_eq!(Mode::Forced.busy_timeout_ms(), 5000);
        let labels: std::collections::HashSet<_> = [
            SkipReason::Busy,
            SkipReason::UnknownLoad,
            SkipReason::TooLarge,
            SkipReason::GcRunning,
        ]
        .map(SkipReason::label)
        .into_iter()
        .collect();
        assert_eq!(labels.len(), 4);
        assert!(labels.iter().all(|label| !label.is_empty()));
    }

    #[test]
    fn outcomes_describe_mode_sizes_and_duration() {
        assert!(!Outcome::NotNeeded.is_noteworthy());
        assert!(!Outcome::Skipped(SkipReason::Busy).is_noteworthy());
        assert!(!Outcome::Skipped(SkipReason::UnknownLoad).is_noteworthy());
        assert!(!Outcome::Skipped(SkipReason::GcRunning).is_noteworthy());
        assert!(Outcome::Skipped(SkipReason::TooLarge).is_noteworthy());
        assert_eq!(Outcome::NotNeeded.describe(), "index compaction not needed");
        assert_eq!(
            Outcome::Skipped(SkipReason::GcRunning).describe(),
            "index compaction deferred: a GC holds gc.lock"
        );
        let attempted = |mode, result| Outcome::Attempted {
            mode,
            result,
            elapsed: Duration::from_millis(1500),
        };
        assert!(attempted(Mode::Quiet, IndexCompaction::Busy).is_noteworthy());
        assert_eq!(
            attempted(
                Mode::Forced,
                IndexCompaction::Compacted {
                    before: 45_000,
                    after: 900
                }
            )
            .describe(),
            "index compacted (forced): 45000 -> 900 bytes in 1500 ms"
        );
        assert_eq!(
            attempted(Mode::Quiet, IndexCompaction::Busy).describe(),
            "index compaction (quiet) found the index busy after 1500 ms; will retry"
        );
        assert_eq!(
            attempted(
                Mode::Quiet,
                IndexCompaction::InsufficientSpace { needed: 7, free: 3 }
            )
            .describe(),
            "index compaction (quiet) needs 7 bytes of free disk, found 3; will retry"
        );
        assert_eq!(
            attempted(Mode::Quiet, IndexCompaction::NotNeeded).describe(),
            "index compaction (quiet) found nothing to reclaim"
        );
    }

    #[test]
    fn threshold_time_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_since(dir.path()), None);
        assert_eq!(observe_over_threshold(dir.path(), 1_000), Duration::ZERO);
        assert_eq!(read_since(dir.path()), Some(1_000));
        assert_eq!(
            std::fs::read_to_string(state_path(dir.path())).unwrap(),
            r#"{"since":1000}"#
        );
        assert_eq!(
            observe_over_threshold(dir.path(), 1_000 + 21_600),
            FORCE_AFTER
        );
        assert_eq!(
            read_since(dir.path()),
            Some(1_000),
            "the start must not move"
        );

        clear_state(dir.path());
        assert!(!state_path(dir.path()).exists());
        clear_state(dir.path());
    }

    #[test]
    fn corrupt_or_future_threshold_time_restarts_the_timer() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(state_path(dir.path()), b"{not json").unwrap();
        assert_eq!(read_since(dir.path()), None);
        assert_eq!(observe_over_threshold(dir.path(), 500), Duration::ZERO);
        assert_eq!(read_since(dir.path()), Some(500));

        // The clock went backwards: no huge duration, and the timer restarts.
        assert_eq!(observe_over_threshold(dir.path(), 400), Duration::ZERO);
        assert_eq!(read_since(dir.path()), Some(400));
        assert_eq!(observe_over_threshold(dir.path(), 460), 60 * SEC);

        assert_eq!(resolve_since(None, 9), 9);
        assert_eq!(resolve_since(Some(9), 9), 9);
        assert_eq!(resolve_since(Some(8), 9), 8);
        assert_eq!(resolve_since(Some(10), 9), 9);
    }

    const NOW: u64 = 1_800_000_000;

    /// A store whose index holds 80 MiB of dropped rows and little else.
    #[cfg(unix)]
    fn sparse_store(dir: &Path) -> Config {
        let config = crate::test_support::test_config(dir.to_path_buf());
        let index_path = {
            let store = Store::open(&config).unwrap();
            let path = store.file_hash_cache().db().path().unwrap().to_string();
            PathBuf::from(path)
        };
        let db = rusqlite::Connection::open(&index_path).unwrap();
        db.execute_batch(
            "CREATE TABLE legacy(payload BLOB);
             INSERT INTO legacy VALUES (zeroblob(83886080));
             DROP TABLE legacy;",
        )
        .unwrap();
        config
    }

    #[cfg(unix)]
    fn index_len(config: &Config) -> u64 {
        std::fs::metadata(config.cache_dir.join("index.db"))
            .unwrap()
            .len()
    }

    /// A clock whose last request is long past.
    fn idle_clock() -> RequestClock {
        RequestClock {
            origin: Instant::now() - 2 * QUIET_AFTER,
            last_ms: AtomicU64::new(0),
        }
    }

    #[cfg(unix)]
    fn hold_permit(config: &Config) -> StoreLock {
        let permits = config.cache_dir.join("scheduler").join("permits");
        std::fs::create_dir_all(&permits).unwrap();
        StoreLock::try_acquire(&permits.join("0")).unwrap().unwrap()
    }

    #[test]
    fn dense_index_is_left_alone_and_a_stale_record_is_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().to_path_buf());
        drop(Store::open(&config).unwrap());
        std::fs::write(state_path(dir.path()), br#"{"since":1}"#).unwrap();

        let clock = idle_clock();
        assert_eq!(
            attempt(&config, Trigger::Periodic(&clock), NOW).unwrap(),
            Outcome::NotNeeded
        );
        assert!(!state_path(dir.path()).exists());
        assert_eq!(run(&config, Trigger::Shutdown), Some(Outcome::NotNeeded));
        assert!(!state_path(dir.path()).exists());
    }

    #[test]
    #[cfg(unix)]
    fn quiet_machine_compacts_and_clears_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparse_store(dir.path());
        assert!(index_len(&config) >= 80 << 20);

        // A recent request defers, and starts the threshold timer.
        let fresh = RequestClock::new();
        assert_eq!(
            attempt(&config, Trigger::Periodic(&fresh), NOW).unwrap(),
            Outcome::Skipped(SkipReason::Busy)
        );
        assert_eq!(read_since(dir.path()), Some(NOW));
        assert!(index_len(&config) >= 80 << 20);

        let clock = idle_clock();
        let Some(Outcome::Attempted {
            mode: Mode::Quiet,
            result: IndexCompaction::Compacted { before, after },
            ..
        }) = run(&config, Trigger::Periodic(&clock))
        else {
            panic!("a quiet machine must compact");
        };
        assert!(before >= 80 << 20);
        assert_eq!(after, index_len(&config));
        assert!(after < 8 << 20);
        assert!(!state_path(dir.path()).exists());
        // The store still opens and the next check has nothing to do.
        assert_eq!(
            run(&config, Trigger::Periodic(&clock)),
            Some(Outcome::NotNeeded)
        );
    }

    #[test]
    #[cfg(unix)]
    fn held_permit_defers_until_the_force_window() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparse_store(dir.path());
        let permit = hold_permit(&config);
        let clock = idle_clock();

        assert_eq!(
            attempt(&config, Trigger::Periodic(&clock), NOW).unwrap(),
            Outcome::Skipped(SkipReason::Busy)
        );
        assert_eq!(
            attempt(&config, Trigger::Shutdown, NOW).unwrap(),
            Outcome::Skipped(SkipReason::Busy)
        );
        assert_eq!(
            attempt(&config, Trigger::Periodic(&clock), NOW + 21_599).unwrap(),
            Outcome::Skipped(SkipReason::Busy)
        );
        assert!(index_len(&config) >= 80 << 20);
        assert_eq!(read_since(dir.path()), Some(NOW));

        let outcome = attempt(&config, Trigger::Periodic(&clock), NOW + 21_600).unwrap();
        assert!(
            matches!(
                outcome,
                Outcome::Attempted {
                    mode: Mode::Forced,
                    result: IndexCompaction::Compacted { .. },
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert!(index_len(&config) < 8 << 20);
        assert!(!state_path(dir.path()).exists());
        drop(permit);
    }

    #[test]
    #[cfg(unix)]
    fn shutdown_compacts_despite_a_recent_request() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparse_store(dir.path());
        let outcome = attempt(&config, Trigger::Shutdown, NOW).unwrap();
        assert!(
            matches!(
                outcome,
                Outcome::Attempted {
                    mode: Mode::Quiet,
                    result: IndexCompaction::Compacted { .. },
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert!(index_len(&config) < 8 << 20);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn shutdown_attempt_runs_off_the_async_workers_and_compacts() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparse_store(dir.path());
        run_at_shutdown(config.clone()).await;
        assert!(index_len(&config) < 8 << 20);
    }

    #[test]
    #[cfg(unix)]
    fn running_gc_defers_compaction() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparse_store(dir.path());
        let gc_store = Store::open(&config).unwrap();
        let gc_lock = gc_store.try_gc_lock().unwrap().unwrap();
        let clock = idle_clock();

        assert_eq!(
            attempt(&config, Trigger::Periodic(&clock), NOW).unwrap(),
            Outcome::Skipped(SkipReason::GcRunning)
        );
        assert!(index_len(&config) >= 80 << 20);
        assert_eq!(read_since(dir.path()), Some(NOW));

        drop(gc_lock);
        assert!(matches!(
            attempt(&config, Trigger::Periodic(&clock), NOW).unwrap(),
            Outcome::Attempted {
                result: IndexCompaction::Compacted { .. },
                ..
            }
        ));
    }

    #[test]
    #[cfg(unix)]
    fn contended_index_yields_and_keeps_the_record() {
        let dir = tempfile::tempdir().unwrap();
        let config = sparse_store(dir.path());
        let reader = rusqlite::Connection::open(config.cache_dir.join("index.db")).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        let _: i64 = reader
            .query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))
            .unwrap();

        let clock = idle_clock();
        let started = Instant::now();
        let outcome = attempt(&config, Trigger::Periodic(&clock), NOW).unwrap();
        assert!(
            matches!(
                outcome,
                Outcome::Attempted {
                    mode: Mode::Quiet,
                    result: IndexCompaction::Busy,
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "quiet mode must not wait out the busy timeout"
        );
        assert!(index_len(&config) >= 80 << 20);
        assert_eq!(read_since(dir.path()), Some(NOW));
        // The lock was released: a GC can run.
        let store = Store::open(&config).unwrap();
        assert!(store.try_gc_lock().unwrap().is_some());
    }
}
