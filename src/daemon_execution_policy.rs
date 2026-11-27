//! Shared decisions of the Unix and Windows compiler execution handlers once
//! a client has committed an invocation.
//!
//! A committed invocation that the daemon cannot finish goes back to the
//! client while nothing reached its output streams: a cache or daemon failure
//! is never a build failure when a rerun cannot show Cargo anything twice.

use kache_protocol::execution::ExecutionReply;
use std::time::{Duration, Instant};

/// How long a compiler worker that should stop (the daemon drains) gets to
/// observe cancellation, kill and reap its children and unwind. Every wait in
/// the pipeline polls cancellation, so a healthy worker stops well inside
/// this. A worker that does not is stranded in uncancellable work; the
/// handler answers without it rather than holding the drain and the client.
pub(super) const WORKER_STOP_GRACE: Duration = Duration::from_secs(5);

/// Interval of [`ExecutionReply::Heartbeat`] frames to a delegated client,
/// written by [`super::frames::FrameWriter`]'s thread. The client gives up
/// after `kache_ipc::execution::EXECUTION_SILENCE_LIMIT` without a byte.
pub(super) const EXECUTION_HEARTBEAT: Duration = Duration::from_secs(5);

/// Longest a delegated worker may spend without a compiler child alive: key
/// computation, store lookup and restore, remote checks, flight and key-lock
/// waits, and the store hand-off. The compiler run itself never counts. The
/// legitimate maximum is the coordination allowance
/// ([`crate::scheduler::COORDINATION_ALLOWANCE`], 2 min) plus the 3 s remote
/// budget and the 30 s busy-index lookup budget; three minutes leaves slack
/// above that and ends an unknown wedge far sooner than the 30 min waits.
pub(super) const CACHE_STAGE_BUDGET: Duration = Duration::from_secs(180);

/// Bounds the cache-only part of a delegated compile; see
/// [`CACHE_STAGE_BUDGET`]. Driven by explicit instants, so tests need no clock.
pub(super) struct CacheStageWatch {
    budget: Duration,
    idle_since: Instant,
    spawns: u64,
}

impl CacheStageWatch {
    pub(super) fn new(budget: Duration, now: Instant) -> Self {
        Self {
            budget,
            idle_since: now,
            spawns: 0,
        }
    }

    /// Record a sample of the worker's children; true once the worker has
    /// had no compiler child alive, and started none, for the whole budget.
    pub(super) fn expired(&mut self, now: Instant, (child_running, spawns): (bool, u64)) -> bool {
        if child_running || spawns != self.spawns {
            self.spawns = spawns;
            self.idle_since = now;
            return false;
        }
        now.saturating_duration_since(self.idle_since) >= self.budget
    }
}

/// The watchdog's sampling clock: first tick one heartbeat interval after
/// acceptance, and missed ticks are skipped rather than replayed in a burst.
pub(super) fn execution_ticker() -> tokio::time::Interval {
    ticker_every(EXECUTION_HEARTBEAT)
}

fn ticker_every(period: Duration) -> tokio::time::Interval {
    let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker
}

/// Write the terminal reply of a delegated invocation after every frame
/// queued before it, then release the writer's handle on the connection.
pub(super) async fn deliver(
    frames: super::frames::FrameWriter,
    reply: &ExecutionReply,
) -> anyhow::Result<()> {
    frames.send(reply)?;
    frames.finish().await
}

/// Cancel a delegated compiler worker and wait up to `grace` for it to
/// finish. True when it finished, so its children are reaped and nothing it
/// started can still write into the client's target directory.
pub(super) async fn stop_worker<T>(
    cancellation: &crate::invocation::process::Cancellation,
    worker: &mut tokio::task::JoinHandle<T>,
    grace: Duration,
) -> bool {
    cancellation.cancel();
    tokio::time::timeout(grace, worker).await.is_ok()
}

/// The terminal reply for a committed invocation that did not complete: a
/// pipeline error, a worker panic, or a drain.
///
/// The client may compile locally ([`ExecutionReply::Fallback`]) when nothing
/// reached its output streams and nothing the daemon started can still act:
/// the worker finished, or `client_owns_effects` (a Windows client owns every
/// child and every target-directory write of an untrusted pipeline, and stops
/// them when it takes the invocation back). Otherwise the failure stands:
/// rerunning could show Cargo the same output twice or race a late writer.
pub(super) fn unfinished_reply(
    output_written: bool,
    worker_finished: bool,
    client_owns_effects: bool,
    reason: String,
) -> ExecutionReply {
    if !output_written && (worker_finished || client_owns_effects) {
        ExecutionReply::Fallback { reason }
    } else {
        ExecutionReply::Failed { message: reason }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A wedged cache stage (no compiler child alive, none started) ends at
    /// the budget; a compiler run, however long, never counts against it.
    #[test]
    fn the_cache_stage_watch_fires_only_after_a_childless_budget() {
        let budget = Duration::from_secs(180);
        let start = Instant::now();
        let at = |secs: u64| start + Duration::from_secs(secs);

        // Wedged before any compiler: fires at the budget, not before.
        let mut wedged = CacheStageWatch::new(budget, start);
        assert!(!wedged.expired(at(5), (false, 0)));
        assert!(!wedged.expired(at(179), (false, 0)));
        assert!(wedged.expired(at(180), (false, 0)));

        // A ten-minute rustc keeps it quiet; the clock restarts when it ends.
        let mut compiling = CacheStageWatch::new(budget, start);
        assert!(!compiling.expired(at(60), (true, 1)));
        assert!(!compiling.expired(at(660), (true, 1)));
        // Idle from the last sample that saw it running (660 s).
        assert!(!compiling.expired(at(665), (false, 1)));
        assert!(!compiling.expired(at(839), (false, 1)));
        assert!(compiling.expired(at(840), (false, 1)));

        // A compiler that started and exited between two samples still counts.
        let mut quick = CacheStageWatch::new(budget, start);
        assert!(!quick.expired(at(170), (false, 1)));
        assert!(!quick.expired(at(349), (false, 1)));
        assert!(quick.expired(at(350), (false, 1)));
    }

    #[test]
    fn the_cache_budget_sits_above_every_legitimate_cache_wait() {
        assert_eq!(CACHE_STAGE_BUDGET, Duration::from_secs(180));
        // Coordination allowance + 3 s demand + 30 s busy-index lookup.
        assert!(
            CACHE_STAGE_BUDGET
                > crate::scheduler::COORDINATION_ALLOWANCE
                    + Duration::from_secs(3)
                    + Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn the_watchdog_samples_once_per_period_after_acceptance() {
        let period = Duration::from_millis(100);
        let started = tokio::time::Instant::now();
        let mut ticker = ticker_every(period);
        ticker.tick().await;
        assert!(started.elapsed() >= period, "{:?}", started.elapsed());
        // A stalled handler skips missed samples instead of firing a burst.
        tokio::time::sleep(period * 3 + period / 2).await;
        ticker.tick().await;
        let resumed = tokio::time::Instant::now();
        ticker.tick().await;
        assert!(
            resumed.elapsed() >= period / 4,
            "missed samples fired in a burst: {:?}",
            resumed.elapsed()
        );
    }

    #[test]
    fn many_heartbeats_fit_in_the_clients_silence_limit() {
        assert_eq!(EXECUTION_HEARTBEAT, Duration::from_secs(5));
        assert!(kache_ipc::execution::EXECUTION_SILENCE_LIMIT >= EXECUTION_HEARTBEAT * 10);
    }

    #[test]
    fn an_unfinished_invocation_returns_to_the_client_only_when_a_rerun_is_safe() {
        let fallback = |output, finished, client_owns| {
            matches!(
                unfinished_reply(output, finished, client_owns, "drain".into()),
                ExecutionReply::Fallback { .. }
            )
        };
        // Nothing visible and nothing left running: compile locally.
        assert!(fallback(false, true, false));
        assert!(fallback(false, true, true));
        // A stranded worker is harmless only where the client owns its effects.
        assert!(fallback(false, false, true));
        assert!(!fallback(false, false, false));
        // Cargo may already have read output: never rerun.
        assert!(!fallback(true, true, true));
        assert!(!fallback(true, false, false));
        match unfinished_reply(true, true, false, "worker panicked".into()) {
            ExecutionReply::Failed { message } => assert_eq!(message, "worker panicked"),
            reply => panic!("{reply:?}"),
        }
        match unfinished_reply(false, true, false, "daemon is shutting down".into()) {
            ExecutionReply::Fallback { reason } => assert_eq!(reason, "daemon is shutting down"),
            reply => panic!("{reply:?}"),
        }
    }

    #[tokio::test]
    async fn stopping_a_worker_cancels_it_and_reports_whether_it_finished() {
        use crate::invocation::process::Cancellation;
        let cancellation = Cancellation::default();
        let observed = cancellation.clone();
        let mut cooperative = tokio::task::spawn_blocking(move || {
            while observed.check().is_ok() {
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        assert!(stop_worker(&cancellation, &mut cooperative, Duration::from_secs(10)).await);

        let (release, stranded_until) = std::sync::mpsc::channel::<()>();
        let mut stranded = tokio::task::spawn_blocking(move || {
            let _ = stranded_until.recv_timeout(Duration::from_secs(30));
        });
        let unheeded = Cancellation::default();
        assert!(!stop_worker(&unheeded, &mut stranded, Duration::from_millis(50)).await);
        assert!(unheeded.check().is_err());
        drop(release);
        stranded.await.unwrap();
    }
}
