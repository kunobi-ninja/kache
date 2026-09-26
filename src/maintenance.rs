//! Store maintenance the daemon runs when the machine looks quiet.
//!
//! Each check first heals drift in the blob index ([`crate::blob_heal`]) and
//! then compacts `index.db` ([`crate::index_compact`]): a heal frees rows, and
//! the compaction after it returns their pages to the disk. Both hold the
//! index write lock while they work, so both wait for a moment with no build.
//! Last, [`crate::target_cleanup`] removes target directories the
//! configuration no longer wants kept.

use crate::config::Config;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// No wrapper request for this long counts as quiet. Permits cover only the
/// miss path; a build made of hits holds none but still sends requests.
pub(crate) const QUIET_AFTER: Duration = Duration::from_secs(60);
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

    /// A clock whose last request is long past.
    #[cfg(test)]
    pub(crate) fn idle() -> Self {
        Self {
            origin: Instant::now() - 2 * QUIET_AFTER,
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

/// Quiet means no compile or leased test binary holds a permit and no
/// wrapper request arrived for [`QUIET_AFTER`]. Unreadable permit slots are
/// not quiet.
pub(crate) fn is_quiet(permits: Option<u32>, since_last_request: Duration) -> bool {
    permits == Some(0) && since_last_request >= QUIET_AFTER
}

/// What asked for the maintenance.
#[derive(Clone, Copy)]
pub(crate) enum Trigger<'a> {
    Periodic(&'a RequestClock),
    Shutdown,
}

impl Trigger<'_> {
    /// Time since the last request. The shutdown request itself just arrived,
    /// so its age is waived.
    pub(crate) fn idle_for(self) -> Duration {
        match self {
            Trigger::Periodic(clock) => clock.idle_for(),
            Trigger::Shutdown => Duration::MAX,
        }
    }
}

pub(crate) fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// One maintenance pass. Blocking: call from `spawn_blocking`. The steps log
/// their own failures; one failing does not stop the other.
pub(crate) fn run(config: &Config, trigger: Trigger<'_>) {
    crate::blob_heal::run(config, trigger);
    crate::index_compact::run(config, trigger);
    crate::target_cleanup::run(config, trigger);
}

/// Check shortly after daemon start, then every few minutes. A check that
/// finds nothing to do costs two index opens, one aggregate query over the
/// blob tables and three PRAGMAs.
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
                tracing::warn!("store maintenance task panicked: {error}");
            }
        }
    })
}

/// The shutdown pass: quiet rules only, so a contended index yields at once.
pub(crate) async fn run_at_shutdown(config: Config) {
    let task = tokio::task::spawn_blocking(move || run(&config, Trigger::Shutdown));
    if let Err(error) = task.await {
        tracing::warn!("store maintenance task panicked: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEC: Duration = Duration::from_secs(1);

    #[test]
    fn schedule_is_the_documented_one() {
        assert_eq!(QUIET_AFTER, Duration::from_secs(60));
        assert_eq!(FIRST_CHECK_AFTER, Duration::from_secs(60));
        assert_eq!(CHECK_INTERVAL, Duration::from_secs(300));
        // 2026-01-01: a clock stuck at 0 would make every record look fresh.
        assert!(unix_now_secs() > 1_767_225_600);
    }

    #[test]
    fn quiet_needs_zero_permits_and_a_minute_without_requests() {
        assert!(is_quiet(Some(0), QUIET_AFTER));
        assert!(is_quiet(Some(0), Duration::MAX));
        assert!(!is_quiet(Some(0), QUIET_AFTER - SEC));
        assert!(!is_quiet(Some(1), QUIET_AFTER));
        assert!(!is_quiet(None, Duration::MAX));
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

    /// Both steps run in one pass, the heal first.
    #[test]
    #[cfg(unix)]
    fn one_pass_heals_the_blob_index_and_compacts_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = crate::test_support::test_config(dir.path().to_path_buf());
        let store = crate::store::Store::open(&config).unwrap();
        store
            .file_hash_cache()
            .db()
            .execute_batch(
                "INSERT INTO blobs (hash, size, refcount) VALUES ('unowned', 4096, 1);
                 CREATE TABLE legacy(payload BLOB);
                 INSERT INTO legacy VALUES (zeroblob(83886080));
                 DROP TABLE legacy;",
            )
            .unwrap();
        assert_eq!(store.blob_refcount_drift().unwrap().unowned, 1);
        let index = config.cache_dir.join("index.db");
        assert!(std::fs::metadata(&index).unwrap().len() >= 80 << 20);

        run(&config, Trigger::Shutdown);
        assert!(store.blob_refcount_drift().unwrap().is_clean());
        assert!(std::fs::metadata(&index).unwrap().len() < 8 << 20);
    }

    #[test]
    fn trigger_reports_request_age() {
        let fresh = RequestClock::new();
        assert!(Trigger::Periodic(&fresh).idle_for() < QUIET_AFTER);
        let idle = RequestClock::idle();
        let age = Trigger::Periodic(&idle).idle_for();
        assert!(age >= 2 * QUIET_AFTER && age < 3 * QUIET_AFTER, "{age:?}");
        assert_eq!(Trigger::Shutdown.idle_for(), Duration::MAX);
    }
}
