//! Kache policy adapter for the shared exclusive replacement transaction.
use super::*;
use kunobi_daemon::{
    ProcessLock,
    replacement::{self, Budgets, Driver, Mode, Progress, Step},
};

pub(super) fn ensure(config: &Config, force: bool) -> Result<bool> {
    let deadline = Instant::now() + DAEMON_START_TIMEOUT;
    if !force && current(config, deadline)?.is_some() {
        return Ok(true);
    }
    let socket = config.socket_path();
    std::fs::create_dir_all(socket.parent().context("socket has no parent")?)?;
    let Some(lock) = kunobi_daemon::readiness::wait_until(deadline, |_| {
        ProcessLock::try_acquire(socket.with_extension("lock"))
    })?
    else {
        return Ok(false);
    };
    let mut driver = KacheReplacement {
        config,
        force,
        child: None,
        executable: None,
        stopping: false,
    };
    match replacement::run(
        &lock,
        Mode::Exclusive,
        Budgets {
            setup: DAEMON_START_TIMEOUT,
            drain: Some(Duration::from_secs(35)),
        },
        &mut driver,
    ) {
        Ok(_) => Ok(true),
        Err(error) if matches!(error.reason, replacement::Reason::Deadline) => Ok(false),
        Err(error) => Err(anyhow::anyhow!(error.to_string())),
    }
}

/// Legacy readiness adapter. New lifecycle control uses its own identity proof;
/// this response remains necessary while older Kache daemons are supported.
enum ObservedOwner {
    Ready(DaemonHealth),
    Pending,
    AbsentOrOutdated,
}

pub(super) fn current(config: &Config, deadline: Instant) -> Result<Option<DaemonHealth>> {
    Ok(match observe(config, deadline)? {
        ObservedOwner::Ready(health) => Some(health),
        ObservedOwner::Pending | ObservedOwner::AbsentOrOutdated => None,
    })
}

fn observe(config: &Config, deadline: Instant) -> Result<ObservedOwner> {
    match lifecycle_control::health(config, deadline) {
        Ok(Some(health)) => {
            if client_epoch_is_newer(build_epoch(), health.revision) {
                return Ok(ObservedOwner::AbsentOrOutdated);
            }
            return Ok(if health.ready && !health.draining {
                ObservedOwner::Ready(DaemonHealth {
                    version: health.build,
                    build_epoch: health.revision,
                })
            } else {
                ObservedOwner::Pending
            });
        }
        Ok(None) => {
            if let Some(health) = current_socket(&config.socket_path(), deadline)? {
                return Ok(ObservedOwner::Ready(health));
            }
        }
        Err(error) if transient(&error) => {}
        Err(error) => return Err(error),
    }
    // This legacy record plus a held lock permits waiting, never claiming ready.
    Ok(
        if starting_daemon_epoch(config)
            .is_some_and(|epoch| !client_epoch_is_newer(build_epoch(), epoch))
        {
            ObservedOwner::Pending
        } else {
            ObservedOwner::AbsentOrOutdated
        },
    )
}

pub(super) fn current_socket(socket: &Path, deadline: Instant) -> Result<Option<DaemonHealth>> {
    let timeout = deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(2));
    if timeout.is_zero() {
        return Ok(None);
    }
    let response =
        match lifecycle_control::legacy_request(socket, &Request::Health, Instant::now() + timeout)
        {
            Ok(response) => response,
            Err(error) if !transient(&error) => return Err(error),
            Err(_) => return Ok(None),
        };
    let response: Response = serde_json::from_str(&response)?;
    let Some(health) = response.health.filter(|_| response.ok) else {
        return Ok(None);
    };
    Ok((!client_epoch_is_newer(build_epoch(), health.build_epoch)).then_some(health))
}

struct KacheReplacement<'a> {
    config: &'a Config,
    force: bool,
    child: Option<std::process::Child>,
    executable: Option<PathBuf>,
    stopping: bool,
}
impl Driver for KacheReplacement<'_> {
    type Error = anyhow::Error;
    fn perform(&mut self, step: Step, deadline: Option<Instant>) -> Result<Progress> {
        let config = self.config;
        let socket = config.socket_path();
        let deadline = deadline.context("Kache replacement requires an explicit phase budget")?;
        match step {
            Step::Recheck => {
                if !self.force {
                    match observe(config, deadline)? {
                        ObservedOwner::Ready(_) => return Ok(Progress::Unchanged),
                        ObservedOwner::Pending => return Ok(Progress::Pending),
                        ObservedOwner::AbsentOrOutdated => {}
                    }
                }
            }
            Step::Prepare => {
                self.executable =
                    Some(std::env::current_exe().context("locating replacement executable")?);
                std::fs::metadata(self.executable.as_ref().unwrap())
                    .context("reading replacement executable")?;
            }
            Step::Drain => {
                if !self.stopping {
                    if daemon_run_lock_is_held(&socket)? {
                        // The daemon owns persistence and its allowed cancellation
                        // policy. Never kill it merely because readiness is slow.
                        match lifecycle_control::request(
                            config,
                            kunobi_daemon::wire::operation::DRAIN,
                            deadline,
                        ) {
                            Ok(Some(_)) => {}
                            Ok(None) => {
                                let _ = lifecycle_control::legacy_request(
                                    &socket,
                                    &Request::Shutdown,
                                    deadline,
                                );
                            }
                            Err(error) if transient(&error) => {}
                            Err(error) => return Err(error),
                        }
                    }
                    self.stopping = true;
                }
                if daemon_run_lock_is_held(&socket)? {
                    return Ok(Progress::Pending);
                }
            }
            Step::Start => {
                // A service manager may already have restarted after the drain.
                match observe(config, deadline)? {
                    ObservedOwner::Ready(_) | ObservedOwner::Pending => return Ok(Progress::Done),
                    ObservedOwner::AbsentOrOutdated => {}
                }
                if crate::service::manages_instance(config)? {
                    anyhow::ensure!(
                        crate::service::kickstart(deadline)?,
                        "installed service disappeared during replacement"
                    );
                } else {
                    let log = socket.with_extension("log");
                    rotate_daemon_log_if_large(&log);
                    let stderr = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(log)
                        .map(std::process::Stdio::from)
                        .unwrap_or_else(|_| std::process::Stdio::null());
                    warn_if_remote_is_env_only(config);
                    self.child = Some(spawn_detached_daemon(
                        self.executable.as_ref().unwrap(),
                        stderr,
                    )?);
                }
            }
            Step::Verify | Step::Validate => {
                if let Some(child) = &mut self.child
                    && let Some(exit) = child.try_wait()?
                {
                    anyhow::ensure!(
                        exit.success(),
                        "daemon candidate exited before readiness: {exit}"
                    );
                    self.child = None; // A concurrent service owner may have won.
                }
                if current(config, deadline)?.is_none() {
                    if step == Step::Verify {
                        return Ok(Progress::Pending);
                    }
                    anyhow::bail!("daemon lost readiness before activation");
                }
            }
            Step::Commit => {}
            Step::Retire => unreachable!("Kache uses exclusive replacement"),
        }
        Ok(Progress::Done)
    }
}
impl Drop for KacheReplacement<'_> {
    fn drop(&mut self) {
        // A timeout does not prove that the child stopped. Its process lock and
        // next live probe remain authoritative. Never kill a managed process.
        if let Some(mut child) = self.child.take() {
            let _ = std::thread::Builder::new()
                .name("daemon-reaper".into())
                .spawn(move || {
                    let _ = child.wait();
                });
        }
    }
}

fn transient(error: &anyhow::Error) -> bool {
    error.chain().any(|error| {
        matches!(
            error.downcast_ref::<kunobi_daemon::local::ConnectError>(),
            Some(kunobi_daemon::local::ConnectError::ConnectTimeout)
        ) || error.downcast_ref::<std::io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::WouldBlock
            )
        })
    })
}
