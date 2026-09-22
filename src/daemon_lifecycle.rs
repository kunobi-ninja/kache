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
        retiring_pid: None,
        log_start: None,
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
    retiring_pid: Option<u32>,
    /// The daemon log and its length when the candidate was started, so a
    /// candidate that dies early can be reported with what it wrote.
    log_start: Option<(PathBuf, u64)>,
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
                            Ok(Some(proof)) => self.retiring_pid = Some(proof.process_id),
                            Ok(None) => {
                                // A legacy record is only a waiting hint. It never
                                // authorizes signalling that PID.
                                self.retiring_pid =
                                    read_daemon_state(&socket).map(|state| state.pid);
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
                if daemon_run_lock_is_held(&socket)?
                    || self.retiring_pid.is_some_and(process_is_alive)
                {
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
                    let written = std::fs::metadata(&log).map_or(0, |meta| meta.len());
                    self.log_start = Some((log.clone(), written));
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
                        "{}",
                        candidate_exit_message(exit, self.log_start.as_ref())
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

/// Enough of the daemon log to hold its fatal error, without flooding the terminal.
const EXIT_LOG_TAIL_BYTES: u64 = 2048;

/// What `kache daemon start` reports when the new daemon exits before it is
/// ready. The exit status alone does not say why; the daemon writes its fatal
/// error to its log, so the lines it wrote since it was started are included.
fn candidate_exit_message(exit: impl std::fmt::Display, log: Option<&(PathBuf, u64)>) -> String {
    let mut message = format!("daemon candidate exited before readiness: {exit}");
    if let Some((path, start)) = log
        && let Some(written) = written_since(path, *start)
    {
        message.push_str(&format!("\n{}:", path.display()));
        for line in written.lines() {
            message.push_str(&format!("\n  {line}"));
        }
    }
    message
}

/// The text appended to `path` after `start` bytes, at most
/// [`EXIT_LOG_TAIL_BYTES`] of it. A cut that lands mid-line drops that partial
/// line. `None` when nothing was written or the log cannot be read.
fn written_since(path: &Path, start: u64) -> Option<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    if len <= start {
        return None;
    }
    let from = start.max(len - EXIT_LOG_TAIL_BYTES.min(len));
    file.seek(SeekFrom::Start(from)).ok()?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).ok()?;
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if from > start
        && let Some(newline) = text.find('\n')
    {
        text.drain(..=newline);
    }
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn driver(config: &Config) -> KacheReplacement<'_> {
        KacheReplacement {
            config,
            force: true,
            child: None,
            executable: None,
            stopping: false,
            retiring_pid: None,
            log_start: None,
        }
    }

    #[test]
    fn only_what_the_candidate_wrote_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("daemon.log");
        std::fs::write(&log, "old run: fine\n").unwrap();
        let start = std::fs::metadata(&log).unwrap().len();
        assert_eq!(written_since(&log, start), None, "nothing written yet");

        let mut file = std::fs::OpenOptions::new().append(true).open(&log).unwrap();
        std::io::Write::write_all(&mut file, b"Error: acquiring daemon socket\n\n").unwrap();
        assert_eq!(
            written_since(&log, start).as_deref(),
            Some("Error: acquiring daemon socket")
        );
        assert_eq!(written_since(&dir.path().join("absent.log"), 0), None);
    }

    #[test]
    fn whitespace_alone_is_not_worth_reporting() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("daemon.log");
        std::fs::write(&log, "\n  \n").unwrap();
        assert_eq!(written_since(&log, 0), None);
    }

    #[test]
    fn a_long_log_keeps_its_last_whole_lines() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("daemon.log");
        let line = "x".repeat(99);
        let mut text = String::new();
        for _ in 0..100 {
            text.push_str(&line);
            text.push('\n');
        }
        text.push_str("Error: the cause\n");
        std::fs::write(&log, &text).unwrap();

        let tail = written_since(&log, 0).unwrap();
        assert!(tail.ends_with("Error: the cause"), "{tail}");
        assert!(tail.len() as u64 <= EXIT_LOG_TAIL_BYTES);
        // The cut landed mid-line; only whole lines remain.
        assert!(tail.lines().all(|l| l == line || l == "Error: the cause"));
        // Exactly the window, less the partial first line.
        let window = &text[text.len() - EXIT_LOG_TAIL_BYTES as usize..];
        let expected = window[window.find('\n').unwrap() + 1..].trim();
        assert_eq!(tail, expected);
    }

    #[test]
    fn a_log_shorter_than_the_window_is_read_from_the_start() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("daemon.log");
        std::fs::write(&log, "first line\nsecond line\n").unwrap();
        assert_eq!(
            written_since(&log, 0).as_deref(),
            Some("first line\nsecond line")
        );
    }

    #[test]
    fn the_exit_message_carries_the_log_lines_when_there_are_any() {
        assert_eq!(
            candidate_exit_message("exit status: 1", None),
            "daemon candidate exited before readiness: exit status: 1"
        );
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("daemon.log");
        std::fs::write(&log, "Error: one\ncaused by: two\n").unwrap();
        assert_eq!(
            candidate_exit_message("exit status: 1", Some(&(log.clone(), 0))),
            format!(
                "daemon candidate exited before readiness: exit status: 1\n{}:\n  Error: one\n  caused by: two",
                log.display()
            )
        );
        let len = std::fs::metadata(&log).unwrap().len();
        assert_eq!(
            candidate_exit_message("exit status: 1", Some(&(log, len))),
            "daemon candidate exited before readiness: exit status: 1"
        );
    }

    #[cfg(unix)]
    #[test]
    fn abandoned_startup_reaps_the_child_without_terminating_it() {
        use std::io::Write;
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let mut child = std::process::Command::new("sh")
            .args(["-c", "read release"])
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let pid = child.id() as libc::pid_t;
        let mut input = child.stdin.take().unwrap();
        let mut replacement = driver(&config);
        replacement.child = Some(child);
        drop(replacement);
        // Signal zero observes this owned child without signalling it.
        assert_eq!(unsafe { libc::kill(pid, 0) }, 0);
        input.write_all(b"release\n").unwrap();
        drop(input);
        let reaped =
            kunobi_daemon::readiness::wait_until(Instant::now() + Duration::from_secs(3), |_| {
                Ok::<_, std::io::Error>((unsafe { libc::kill(pid, 0) } != 0).then_some(()))
            })
            .unwrap()
            .is_some();
        if !reaped {
            // Clean the fixture even if the reaper regression is reintroduced.
            unsafe {
                libc::waitpid(pid, std::ptr::null_mut(), 0);
            }
        }
        assert!(reaped, "abandoned startup left a zombie child");
    }

    #[test]
    fn failed_ownership_probe_remains_an_error() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        std::fs::create_dir(daemon_run_lock_path(&config.socket_path())).unwrap();
        assert!(
            ensure(&config, true).is_err(),
            "unreadable ownership is not a startup timeout"
        );
    }

    #[test]
    fn legacy_initializer_is_waited_for_without_claiming_readiness() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let _lock = ProcessLock::try_acquire(daemon_run_lock_path(&config.socket_path()))
            .unwrap()
            .unwrap();
        let coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.write_phase(DaemonPhase::Starting).unwrap();
        assert!(matches!(
            observe(&config, Instant::now() + Duration::from_secs(1)).unwrap(),
            ObservedOwner::Pending
        ));
    }

    #[test]
    fn drain_waits_for_both_exclusive_lock_and_retiring_process() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let mut replacement = driver(&config);
        replacement.stopping = true;
        replacement.retiring_pid = Some(std::process::id());
        let deadline = Some(Instant::now() + Duration::from_secs(1));
        assert_eq!(
            replacement.perform(Step::Drain, deadline).unwrap(),
            Progress::Pending
        );
        replacement.retiring_pid = None;
        let lock = ProcessLock::try_acquire(daemon_run_lock_path(&config.socket_path()))
            .unwrap()
            .unwrap();
        assert_eq!(
            replacement.perform(Step::Drain, deadline).unwrap(),
            Progress::Pending
        );
        drop(lock);
        assert_eq!(
            replacement.perform(Step::Drain, deadline).unwrap(),
            Progress::Done
        );
    }

    #[test]
    fn drain_keeps_waiting_when_the_held_owner_has_not_bound_control_yet() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let _lock = ProcessLock::try_acquire(daemon_run_lock_path(&config.socket_path()))
            .unwrap()
            .unwrap();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(kunobi_daemon::wire::VERSION);
        coord.write_phase(DaemonPhase::Starting).unwrap();
        assert_eq!(
            driver(&config)
                .perform(Step::Drain, Some(Instant::now() + Duration::from_secs(1)))
                .unwrap(),
            Progress::Pending
        );
    }

    #[test]
    fn drain_does_not_hide_an_unsupported_control_protocol() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let _lock = ProcessLock::try_acquire(daemon_run_lock_path(&config.socket_path()))
            .unwrap()
            .unwrap();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(u32::MAX);
        coord.write_phase(DaemonPhase::Ready).unwrap();
        let error = driver(&config)
            .perform(Step::Drain, Some(Instant::now() + Duration::from_secs(1)))
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported lifecycle control version")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replacement_requests_drain_before_waiting_for_cache_ownership() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let lock = ProcessLock::try_acquire(daemon_run_lock_path(&config.socket_path()))
            .unwrap()
            .unwrap();
        let lifecycle = Arc::new(Lifecycle::default());
        let mut server = lifecycle_control::serve(&config, Arc::clone(&lifecycle))
            .await
            .unwrap();
        server.service.mark_ready();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(kunobi_daemon::wire::VERSION);
        coord.write_phase(DaemonPhase::Ready).unwrap();
        let pending = lifecycle.begin().unwrap();
        let progress = tokio::task::spawn_blocking(move || {
            let mut driver = KacheReplacement {
                config: &config,
                force: true,
                child: None,
                executable: None,
                stopping: false,
                retiring_pid: None,
                log_start: None,
            };
            driver.perform(Step::Drain, Some(Instant::now() + Duration::from_secs(2)))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(progress, Progress::Pending);
        assert!(
            !lifecycle.accepting_calls(),
            "replacement must request drain before waiting"
        );
        assert_eq!(lifecycle.snapshot().active, 1);
        drop(pending);
        drop(lock);
        server.finish().await;
    }
}
