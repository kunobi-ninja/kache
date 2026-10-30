#[cfg(any(unix, windows))]
pub(crate) mod execution;
pub(crate) mod io;
pub(crate) mod process;
#[cfg(unix)]
pub(crate) mod unix_broker;
#[cfg(windows)]
pub(crate) mod windows_broker;
mod workspace;

/// Inputs and timing transferred to one synchronous compiler request.
pub(crate) struct InvocationContext {
    /// The existing daemon owns background work started by this invocation.
    pub(crate) daemon: Option<std::sync::Arc<crate::daemon::Daemon>>,
    pub(crate) env: InvocationEnv,
    pub(crate) started_at: std::time::Instant,
    pub(crate) shim_stem: String,
    pub(crate) io: io::InvocationIo,
    pub(crate) cancellation: Option<process::Cancellation>,
    #[cfg(unix)]
    pub(crate) jobserver: kache_ipc::jobserver::Descriptors,
}

/// Keep a store-key waiter inside the caller's cancellation lifetime.
pub(crate) fn wait_for_committed(
    store: &crate::store::Store,
    key: &str,
    timeout: Option<std::time::Duration>,
) -> anyhow::Result<bool> {
    let (wait, bounded_timeout) = store_wait_budget(timeout);
    // Supplying a bound replaces the store's None timeout. Retain that
    // default's shortcut when the peer has already published its entry.
    if timeout.is_none() && wait.scoped() {
        process::check()?;
        if store.contains(key) {
            return Ok(true);
        }
    }
    store.wait_for_committed_with(key, bounded_timeout, || Ok(process::check()?))
}

fn store_wait_budget(
    timeout: Option<std::time::Duration>,
) -> (crate::scheduler::BudgetWait, Option<std::time::Duration>) {
    let wait =
        crate::scheduler::BudgetWait::start(timeout.unwrap_or(crate::store::BUILD_LOCK_TIMEOUT));
    let timeout = if wait.scoped() {
        Some(wait.timeout)
    } else {
        timeout
    };
    (wait, timeout)
}

pub(crate) use kache_config::InvocationEnv;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn shared_budget_preserves_store_defaults_and_explicit_wait_limits() {
        assert_eq!(store_wait_budget(None).1, None);
        let _scope = crate::scheduler::InvocationWaitScope::enter();
        assert_eq!(store_wait_budget(None).1, Some(Duration::from_secs(600)));
        assert_eq!(
            store_wait_budget(Some(Duration::from_secs(7))).1,
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            store_wait_budget(Some(Duration::from_secs(900))).1,
            Some(Duration::from_secs(900))
        );
        assert!(
            store_wait_budget(Some(Duration::from_secs(3600)))
                .1
                .unwrap()
                <= Duration::from_secs(1800)
        );
    }
}
