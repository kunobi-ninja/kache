//! Shared decisions of the Unix and Windows compiler execution handlers once
//! a client has committed an invocation.
//!
//! A committed invocation that the daemon cannot finish goes back to the
//! client while nothing reached its output streams: a cache or daemon failure
//! is never a build failure when a rerun cannot show Cargo anything twice.

use kache_protocol::execution::ExecutionReply;
use std::time::Duration;

/// How long a compiler worker that should stop (the daemon drains) gets to
/// observe cancellation, kill and reap its children and unwind. Every wait in
/// the pipeline polls cancellation, so a healthy worker stops well inside
/// this. A worker that does not is stranded in uncancellable work; the
/// handler answers without it rather than holding the drain and the client.
pub(super) const WORKER_STOP_GRACE: Duration = Duration::from_secs(5);

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
