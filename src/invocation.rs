#[cfg(any(unix, windows))]
pub(crate) mod execution;
pub(crate) mod io;
pub(crate) mod process;
#[cfg(windows)]
pub(crate) mod windows_broker;

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
    store.wait_for_committed_with(key, timeout, || Ok(process::check()?))
}

pub(crate) use kache_config::InvocationEnv;
