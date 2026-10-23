//! Kache identity and endpoint adapter for shared lifecycle control.
use super::*;
#[cfg(not(windows))]
use kunobi_daemon::local::Duplex;
use kunobi_daemon::{
    ServiceIdentity,
    admission::{Admission, Limits, Pool},
    control::ControlService,
    local::peer::PeerCredentials,
    peer::{Policy, SameUser},
    wire::{self, operation},
};

// Protocol masks: HEALTH | HEALTH_DETAILS | DRAIN, and required health details.
const CONTROL_SUPPORTED: u64 = 0x13;
const CONTROL_REQUIRED: u64 = 0x11;

// Stable service-family UUID. Cache instance separation is independent of builds.
const SERVICE_ID: [u8; 16] = [
    0xa1, 0x94, 0xcb, 0xee, 0x70, 0x11, 0x43, 0x9c, 0xb6, 0x7e, 0x62, 0x96, 0x2a, 0x3a, 0xd8, 0x51,
];

/// The control endpoint, a sibling of the daemon socket.
///
/// Its name is part of the daemon's path budget: a Unix socket address holds
/// 103 bytes of path on macOS and 107 on Linux, and both sockets have to fit.
/// `daemon.ctl` is shorter than the `daemon.sock` it is derived from, so any
/// runtime directory that could hold a daemon socket before this endpoint
/// existed still can. The first name for it, `daemon.control.v2.sock`, cost
/// 11 bytes more and took CI runtime directories that had always worked over
/// the macOS limit (kunobi-ninja/kache#1216).
///
/// Clients and the daemon must derive the same name, so this is the only
/// place that spells it.
pub(super) fn endpoint(config: &Config) -> PathBuf {
    config.socket_path().with_extension("ctl")
}

fn offer(config: &Config) -> Result<wire::Hello> {
    let socket = config.socket_path();
    let parent = socket.parent().context("socket has no parent")?;
    let path = parent
        .canonicalize()
        .unwrap_or_else(|_| parent.to_path_buf())
        .join(socket.file_name().context("socket has no filename")?);
    let instance = blake3::hash(path.as_os_str().as_encoded_bytes())
        .to_hex()
        .to_string();
    let identity = ServiceIdentity::new(SERVICE_ID, "kache", "shared", instance)?;
    Ok(wire::Hello::new(
        &identity,
        CONTROL_SUPPORTED,
        CONTROL_REQUIRED,
    ))
}

pub(super) struct Server {
    pub(super) service: Arc<ControlService>,
    task: tokio::task::JoinHandle<()>,
    stop: Arc<Notify>,
    _socket: SocketCleanupGuard,
}
impl Server {
    pub(super) async fn finish(&mut self) {
        self.stop.notify_one();
        let _ = (&mut self.task).await;
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub(super) async fn serve(config: &Config, lifecycle: Arc<Lifecycle>) -> Result<Server> {
    let path = endpoint(config);
    let listener = crate::transport::bind_daemon_listener(&path)
        .await?
        .context("control endpoint already owned")?;
    let socket = SocketCleanupGuard::new(&path)?;
    // 1.0.x clients replace a daemon whose revision is older than their own
    // executable mtime; later clients compare the release in `build`.
    let service = Arc::new(ControlService::new(
        lifecycle,
        0,
        VERSION.into(),
        build_epoch(),
    ));
    let offer = offer(config)?;
    let handler = Arc::clone(&service);
    let stop = Arc::new(Notify::new());
    let stopping = Arc::clone(&stop);
    let task = tokio::spawn(async move {
        let admission = Arc::new(Admission::new(Limits::default()));
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = stopping.notified() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
                accepted = listener.accept() => {
                    let Ok(stream) = accepted else { break };
                    let Some(permit) = admission.try_acquire(Pool::Control) else { continue };
                    let handler = Arc::clone(&handler);
                    let offer = offer.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        #[cfg(unix)]
                        if crate::transport::require_self_peer(crate::transport::peer_euid(&stream)).is_err() { return; }
                        #[cfg(windows)]
                        {
                            let Ok(credentials) = stream.credentials() else { return; };
                            if SameUser.grant(&credentials).is_err() { return; }
                        }
                        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                        let _ = handler.serve(&stream, &offer, deadline).await;
                    });
                }
            }
        }
        // Preserve pending drain acknowledgements before the owner exits.
        // Each control exchange already has its own absolute deadline.
        while connections.join_next().await.is_some() {}
    });
    Ok(Server {
        service,
        task,
        stop,
        _socket: socket,
    })
}

/// None means no binary endpoint was advertised. Any binary failure is final;
/// callers may retry discovery, but may not downgrade this request to legacy.
pub(super) fn request(
    config: &Config,
    operation: u32,
    deadline: Instant,
) -> Result<Option<wire::Health>> {
    let Some(state) = read_daemon_state(&config.socket_path()) else {
        return Ok(None);
    };
    let Some(version) = state.control_version else {
        return Ok(None);
    };
    anyhow::ensure!(
        version == wire::VERSION,
        "unsupported lifecycle control version {version}"
    );
    let expected = kunobi_daemon::ProcessId::new(state.pid)
        .context("lifecycle advertisement has an invalid PID")?;
    let path = endpoint(config);
    let offer = offer(config)?;
    anyhow::ensure!(
        matches!(operation, operation::HEALTH | operation::DRAIN),
        "unsupported lifecycle operation {operation}"
    );
    #[cfg(windows)]
    let health = request_windows(path, offer, expected, operation, deadline)?;
    #[cfg(not(windows))]
    let health = {
        let endpoint = crate::transport::daemon_endpoint(&path);
        match operation {
            operation::HEALTH => {
                kunobi_daemon::client::health(&endpoint, &offer, Some(expected), deadline)
            }
            _ => kunobi_daemon::client::drain(&endpoint, &offer, Some(expected), deadline),
        }?
    };
    Ok(Some(health))
}

#[cfg(any(windows, test))]
fn lifecycle_message(operation: u32) -> wire::Control {
    wire::Control {
        operation,
        ..Default::default()
    }
}

/// Use the shared codec, reply validation and peer policies with cancelable
/// Windows I/O. The synchronous shared adapter cannot accept a bounded stream.
#[cfg(windows)]
fn request_windows(
    path: PathBuf,
    offer: wire::Hello,
    expected: kunobi_daemon::ProcessId,
    operation: u32,
    deadline: Instant,
) -> Result<wire::Health> {
    use kunobi_daemon::client::RequestError as Error;
    use kunobi_daemon::peer::{ExpectedProcess, Rejected};
    super::with_windows_client(move || async move {
        if Instant::now() >= deadline {
            return Err(
                Error::Unavailable(kunobi_daemon::local::ConnectError::ConnectTimeout).into(),
            );
        }
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
            // One attempt, as PlatformDuplex::connect_once_until specifies.
            let stream = crate::transport::connect_tokio(&path, Duration::ZERO)
                .await
                .map_err(|error| Error::Unavailable(classify_connect_error(&error)))?;
            let _discard = crate::transport::discard_pipe_writes_on_drop(&stream);
            let evidence = stream.credentials().map_err(Error::Credentials)?;
            ExpectedProcess::new(move || Some(expected))
                .grant(&evidence)
                .map_err(Error::Peer)?;
            let peer = evidence.pid.ok_or(Error::Peer(Rejected::Unreported))?;
            if Instant::now() >= deadline {
                return Err(Error::Late);
            }
            let mut session = wire::AsyncSession::connect(&stream, &offer)
                .await
                .map_err(Error::Protocol)?;
            if Instant::now() >= deadline {
                return Err(Error::Late);
            }
            let message = lifecycle_message(operation);
            session.send(&message).await.map_err(Error::Protocol)?;
            let reply = session.receive().await.map_err(Error::Protocol)?;
            let health = wire::Health::from_response(&reply, &message).map_err(Error::Protocol)?;
            if health.process_id != peer.get() {
                return Err(Error::ProcessMismatch {
                    peer,
                    reported: health.process_id,
                });
            }
            if Instant::now() >= deadline {
                return Err(Error::Late);
            }
            Ok(health)
        })
        .await
        .map_err(|_| Error::Protocol(std::io::ErrorKind::TimedOut.into()))?
        .map_err(anyhow::Error::from)
    })
}

#[cfg(windows)]
fn classify_connect_error(error: &std::io::Error) -> kunobi_daemon::local::ConnectError {
    use kunobi_daemon::local::ConnectError;
    if error.kind() == std::io::ErrorKind::PermissionDenied {
        ConnectError::PermissionDenied
    } else if kunobi_daemon::local::is_transient(error) {
        ConnectError::ConnectTimeout
    } else {
        ConnectError::Failed(error.kind())
    }
}

pub(super) fn health(config: &Config, deadline: Instant) -> Result<Option<wire::Health>> {
    request(config, operation::HEALTH, deadline)
}

#[cfg(not(windows))]
type Local = kunobi_daemon::local::PlatformDuplex;

#[cfg(not(windows))]
fn connect(path: &Path, deadline: Instant) -> Result<Local> {
    let stream = Local::connect_once_until(&crate::transport::daemon_endpoint(path), deadline)?;
    let credentials = stream
        .credentials()
        .context("reading lifecycle peer credentials")?;
    SameUser
        .grant(&credentials)
        .context("authenticating lifecycle peer")?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    anyhow::ensure!(!remaining.is_zero(), "lifecycle deadline expired");
    stream
        .set_read_deadline(Some(remaining))
        .context("setting lifecycle setup deadline")?;
    Ok(stream)
}

/// Bounded compatibility exchange on the existing JSON endpoint. Only callers
/// that found no binary advertisement may use this adapter.
pub(super) fn legacy_request(path: &Path, request: &Request, deadline: Instant) -> Result<String> {
    #[cfg(not(windows))]
    {
        legacy_exchange(path, request, deadline)
    }
    #[cfg(windows)]
    {
        super::with_windows_client(|| legacy_exchange_windows(path, request, deadline))
    }
}

#[cfg(not(windows))]
fn legacy_exchange(path: &Path, request: &Request, deadline: Instant) -> Result<String> {
    use std::io::{BufRead, Read, Write};
    let stream = connect(path, deadline)?;
    let (read, mut write) = stream.split()?;
    let mut bytes = serde_json::to_vec(request)?;
    bytes.push(b'\n');
    write.write_all(&bytes)?;
    write.flush()?;
    let mut response = String::new();
    std::io::BufReader::new(read.take(65_536)).read_line(&mut response)?;
    if !response.ends_with('\n') {
        return Err(std::io::Error::from(if response.len() == 65_536 {
            std::io::ErrorKind::InvalidData
        } else {
            std::io::ErrorKind::UnexpectedEof
        })
        .into());
    }
    anyhow::ensure!(Instant::now() < deadline, "late legacy lifecycle response");
    Ok(response)
}

#[cfg(windows)]
async fn legacy_exchange_windows(
    path: &Path,
    request: &Request,
    deadline: Instant,
) -> Result<String> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    if Instant::now() >= deadline {
        return Err(kunobi_daemon::local::ConnectError::ConnectTimeout.into());
    }
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        let stream = crate::transport::connect_tokio(path, Duration::ZERO)
            .await
            .map_err(|error| classify_connect_error(&error))?;
        let _discard = crate::transport::discard_pipe_writes_on_drop(&stream);
        SameUser
            .grant(&stream.credentials()?)
            .context("authenticating lifecycle peer")?;
        if Instant::now() >= deadline {
            return Err(kunobi_daemon::local::ConnectError::ConnectTimeout.into());
        }
        let mut bytes = serde_json::to_vec(request)?;
        bytes.push(b'\n');
        (&stream).write_all(&bytes).await?;
        let mut response = String::new();
        tokio::io::BufReader::new((&stream).take(65_536))
            .read_line(&mut response)
            .await?;
        if !response.ends_with('\n') {
            return Err(std::io::Error::from(if response.len() == 65_536 {
                std::io::ErrorKind::InvalidData
            } else {
                std::io::ErrorKind::UnexpectedEof
            })
            .into());
        }
        anyhow::ensure!(Instant::now() < deadline, "late legacy lifecycle response");
        Ok(response)
    })
    .await
    .map_err(|_| std::io::Error::from(std::io::ErrorKind::TimedOut))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lifecycle_messages_preserve_health_and_drain_through_the_binary_codec() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let local = offer(&config).unwrap();
        for operation in [operation::HEALTH, operation::DRAIN] {
            let (client, server) = tokio::io::duplex(4096);
            tokio::time::timeout(Duration::from_secs(5), async {
                let client = async {
                    let mut session = wire::AsyncSession::connect(client, &local).await.unwrap();
                    let message = lifecycle_message(operation);
                    session.send(&message).await.unwrap();
                    let reply = session.receive().await.unwrap();
                    let health = wire::Health::from_response(&reply, &message).unwrap();
                    assert_eq!(health.ready, operation == operation::HEALTH);
                    assert_eq!(health.draining, operation == operation::DRAIN);
                };
                let server = async {
                    let mut session = wire::AsyncSession::accept(server, &local).await.unwrap();
                    let message = session.receive().await.unwrap();
                    assert_eq!(message.operation, operation);
                    let health = wire::Health {
                        process_id: std::process::id(),
                        ready: message.operation == operation::HEALTH,
                        draining: message.operation == operation::DRAIN,
                        ..Default::default()
                    };
                    session
                        .send(&health.response(&message).unwrap())
                        .await
                        .unwrap();
                };
                tokio::join!(client, server);
            })
            .await
            .expect("binary lifecycle roundtrip stalled");
        }
    }

    /// A daemon that stopped accepting: its only pipe instance is taken. The
    /// legacy exchange ends by its deadline with an error callers treat as a
    /// daemon not there yet, instead of waiting for the pipe to free.
    #[cfg(windows)]
    #[test]
    fn a_legacy_request_to_a_busy_pipe_ends_by_its_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("daemon.sock");
        let _listener = crate::transport::ListenerOptions::new()
            .name(crate::transport::socket_name(&socket).unwrap())
            .create_sync()
            .expect("bind listener");
        let _first = crate::transport::connect_sync(&socket, Duration::from_millis(300))
            .expect("first client");

        let (done, finished) = std::sync::mpsc::channel();
        let started = Instant::now();
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_millis(300);
            let _ = done.send(legacy_request(&socket, &Request::Health, deadline));
        });
        let result = finished
            .recv_timeout(Duration::from_secs(20))
            .expect("the lifecycle exchange hung on a busy pipe");
        let waited = started.elapsed();
        let error = result.expect_err("a busy pipe cannot answer");
        assert!(waited < Duration::from_secs(5), "{waited:?}");
        assert!(
            super::super::lifecycle_client::transient(&error),
            "a stalled pipe reads as a daemon not there yet: {error:#}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn advertised_binary_health_and_drain_fail_once_when_the_pipe_is_busy() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let control = endpoint(&config);
        let _listener = crate::transport::ListenerOptions::new()
            .name(crate::transport::socket_name(&control).unwrap())
            .create_sync()
            .unwrap();
        let _first = crate::transport::connect_sync(&control, Duration::ZERO).unwrap();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(wire::VERSION);
        coord.write_phase(DaemonPhase::Ready).unwrap();
        for operation in [operation::HEALTH, operation::DRAIN] {
            let probe = config.clone();
            let (done, finished) = std::sync::mpsc::channel();
            let started = Instant::now();
            std::thread::spawn(move || {
                let _ = done.send(request(
                    &probe,
                    operation,
                    Instant::now() + Duration::from_secs(2),
                ));
            });
            let failure = finished
                .recv_timeout(Duration::from_secs(20))
                .expect("binary lifecycle connect hung")
                .unwrap_err();
            assert!(matches!(
                failure.downcast_ref::<kunobi_daemon::client::RequestError>(),
                Some(kunobi_daemon::client::RequestError::Unavailable(
                    kunobi_daemon::local::ConnectError::ConnectTimeout
                ))
            ));
            assert!(super::super::lifecycle_client::transient(&failure));
            assert!(
                started.elapsed() < Duration::from_millis(750),
                "one attempt must not spend the two-second retry budget"
            );
        }
    }

    /// Both adapters must bound a connected peer that leaves its hello or
    /// request unread, not only the ERROR_PIPE_BUSY connection path.
    #[cfg(windows)]
    #[test]
    fn connected_lifecycle_exchanges_honor_short_deadlines() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let control = endpoint(&config);
        let _binary = crate::transport::ListenerOptions::new()
            .name(crate::transport::socket_name(&control).unwrap())
            .create_sync()
            .unwrap();
        let socket = config.socket_path();
        let _legacy = crate::transport::ListenerOptions::new()
            .name(crate::transport::socket_name(&socket).unwrap())
            .create_sync()
            .unwrap();
        let mut coord = DaemonCoordFile::for_socket(&socket);
        coord.control_version = Some(wire::VERSION);
        coord.write_phase(DaemonPhase::Ready).unwrap();
        for binary in [true, false] {
            let config = config.clone();
            let socket = socket.clone();
            let (done, finished) = std::sync::mpsc::channel();
            let started = Instant::now();
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_millis(75);
                let result = if binary {
                    request(&config, operation::HEALTH, deadline).map(|_| ())
                } else {
                    legacy_request(&socket, &Request::Health, deadline).map(|_| ())
                };
                let _ = done.send(result);
            });
            let failure = finished
                .recv_timeout(Duration::from_secs(20))
                .expect("connected lifecycle exchange hung")
                .unwrap_err();
            assert!(started.elapsed() < Duration::from_millis(750));
            if binary {
                assert!(matches!(
                    failure.downcast_ref::<kunobi_daemon::client::RequestError>(),
                    Some(kunobi_daemon::client::RequestError::Protocol(error))
                        if error.kind() == std::io::ErrorKind::TimedOut
                ));
                assert!(!super::super::lifecycle_client::transient(&failure));
            } else {
                assert_eq!(
                    failure.downcast_ref::<std::io::Error>().unwrap().kind(),
                    std::io::ErrorKind::TimedOut
                );
                assert!(super::super::lifecycle_client::transient(&failure));
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn expired_legacy_deadline_is_transient_and_permission_errors_are_terminal() {
        let root = tempfile::tempdir().unwrap();
        let error = legacy_request(
            &root.path().join("absent.sock"),
            &Request::Health,
            Instant::now(),
        )
        .unwrap_err();
        assert!(super::super::lifecycle_client::transient(&error));
        assert_eq!(
            classify_connect_error(&std::io::ErrorKind::PermissionDenied.into()),
            kunobi_daemon::local::ConnectError::PermissionDenied
        );
        assert_eq!(
            classify_connect_error(&std::io::ErrorKind::InvalidData.into()),
            kunobi_daemon::local::ConnectError::Failed(std::io::ErrorKind::InvalidData)
        );
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_legacy_replies_preserve_framing_and_the_size_limit() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

        let mut limit_with_newline = vec![b'x'; 65_535];
        limit_with_newline.push(b'\n');
        for (payload, expected) in [
            (b"ready\n".to_vec(), None),
            (limit_with_newline, None),
            (b"partial".to_vec(), Some(std::io::ErrorKind::UnexpectedEof)),
            (vec![b'x'; 65_536], Some(std::io::ErrorKind::InvalidData)),
        ] {
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("legacy.sock");
            let listener = crate::transport::bind_daemon_listener(&socket)
                .await
                .unwrap()
                .unwrap();
            let answer = payload.clone();
            let server = tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    let stream = listener.accept().await.unwrap();
                    let _discard = crate::transport::discard_pipe_writes_on_drop(&stream);
                    let mut line = String::new();
                    tokio::io::BufReader::new(&stream)
                        .read_line(&mut line)
                        .await
                        .unwrap();
                    assert!(matches!(
                        serde_json::from_str::<Request>(&line).unwrap(),
                        Request::Health
                    ));
                    (&stream).write_all(&answer).await.unwrap();
                    // The reader may close immediately after rejecting the frame.
                    let _ = (&stream).flush().await;
                })
                .await
                .expect("legacy reply fixture stalled");
            });
            let result = tokio::task::spawn_blocking(move || {
                legacy_request(
                    &socket,
                    &Request::Health,
                    Instant::now() + Duration::from_secs(5),
                )
            })
            .await
            .unwrap();
            server.await.unwrap();
            match expected {
                None => assert_eq!(result.unwrap().as_bytes(), payload),
                Some(kind) => assert_eq!(
                    result
                        .unwrap_err()
                        .downcast_ref::<std::io::Error>()
                        .unwrap()
                        .kind(),
                    kind
                ),
            }
        }
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windows_binary_replies_validate_identity_operation_and_refusal() {
        use kunobi_daemon::client::RequestError;

        #[derive(Clone, Copy, Debug)]
        enum Reply {
            Valid,
            OtherProcess,
            OtherOperation,
            OtherRequest,
            Refused,
        }

        for case in [
            Reply::Valid,
            Reply::OtherProcess,
            Reply::OtherOperation,
            Reply::OtherRequest,
            Reply::Refused,
        ] {
            let root = tempfile::tempdir().unwrap();
            let config = super::super::tests::test_config(root.path());
            let listener = crate::transport::bind_daemon_listener(&endpoint(&config))
                .await
                .unwrap()
                .unwrap();
            let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
            coord.control_version = Some(wire::VERSION);
            coord.write_phase(DaemonPhase::Ready).unwrap();
            let local = offer(&config).unwrap();
            let invalid = request(
                &config,
                operation::PREPARE,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap_err();
            assert!(
                invalid
                    .to_string()
                    .contains("unsupported lifecycle operation")
            );

            let server = tokio::spawn(async move {
                tokio::time::timeout(Duration::from_secs(10), async {
                    let stream = listener.accept().await.unwrap();
                    let _discard = crate::transport::discard_pipe_writes_on_drop(&stream);
                    if matches!(case, Reply::Refused) {
                        wire::AsyncSession::refuse(&stream, &local, wire::Refused::PeerRejected)
                            .await
                            .unwrap();
                        return;
                    }
                    let mut session = wire::AsyncSession::accept(&stream, &local).await.unwrap();
                    let message = session.receive().await.unwrap();
                    assert_eq!(message.operation, operation::HEALTH);
                    let health = wire::Health {
                        process_id: if matches!(case, Reply::OtherProcess) {
                            std::process::id().checked_add(1).unwrap()
                        } else {
                            std::process::id()
                        },
                        ready: true,
                        ..Default::default()
                    };
                    let mut reply = health.response(&message).unwrap();
                    match case {
                        Reply::OtherOperation => reply.operation = operation::DRAIN,
                        Reply::OtherRequest => reply.request_id = message.request_id + 1,
                        _ => {}
                    }
                    session.send(&reply).await.unwrap();
                })
                .await
                .expect("binary reply fixture stalled");
            });
            let result = query(&config, operation::HEALTH).await;
            server.await.unwrap();
            if matches!(case, Reply::Valid) {
                let health = result.unwrap().unwrap();
                assert_eq!(health.process_id, std::process::id());
                assert!(health.ready);
                continue;
            }
            let error = result.unwrap_err();
            match case {
                Reply::OtherProcess => assert!(matches!(
                    error.downcast_ref::<RequestError>(),
                    Some(RequestError::ProcessMismatch { .. })
                )),
                Reply::OtherOperation | Reply::OtherRequest => assert!(matches!(
                    error.downcast_ref::<RequestError>(),
                    Some(RequestError::Protocol(error))
                        if error.kind() == std::io::ErrorKind::InvalidData
                )),
                Reply::Refused => {
                    assert!(matches!(
                        error.downcast_ref::<RequestError>(),
                        Some(RequestError::Protocol(error))
                            if wire::Refused::of(error) == Some(wire::Refused::PeerRejected)
                    ));
                    assert!(!super::super::lifecycle_client::transient(&error));
                }
                Reply::Valid => unreachable!(),
            }
        }
    }

    /// Both endpoints are Unix sockets in the same directory, so they share one
    /// path budget. A control name that cost more than `daemon.sock` refuses a
    /// runtime directory that has always held a daemon (kunobi-ninja/kache#1216).
    #[test]
    fn the_control_endpoint_is_no_longer_than_the_socket_it_belongs_to() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let socket = config.socket_path();
        let control = endpoint(&config);
        assert_eq!(control.parent(), socket.parent());
        assert_ne!(control, socket);
        assert!(
            control.as_os_str().len() <= socket.as_os_str().len(),
            "{} needs more path than {}",
            control.display(),
            socket.display()
        );
    }

    #[test]
    fn control_negotiation_requires_typed_health_and_advertises_drain() {
        use wire::capability;
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let local = offer(&config).unwrap();
        assert_eq!(
            local.supported,
            capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN
        );
        assert_eq!(
            local.required,
            capability::HEALTH | capability::HEALTH_DETAILS
        );
        let mut peer = local.clone();
        peer.supported = capability::HEALTH;
        peer.required = capability::HEALTH;
        assert!(wire::negotiate(&local, &peer).is_err());
    }

    async fn query(config: &Config, operation: u32) -> Result<Option<wire::Health>> {
        let config = config.clone();
        tokio::task::spawn_blocking(move || {
            request(&config, operation, Instant::now() + Duration::from_secs(2))
        })
        .await
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_control_proves_startup_readiness_and_drain_without_waiting_for_admitted_work() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let lifecycle = Arc::new(Lifecycle::default());
        let server = serve(&config, Arc::clone(&lifecycle)).await.unwrap();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(wire::VERSION);
        coord.write_phase(DaemonPhase::Starting).unwrap();
        let initial = query(&config, operation::HEALTH).await.unwrap().unwrap();
        assert!(!initial.ready);
        assert!(!initial.draining);
        assert_eq!(initial.process_id, std::process::id());
        let starter_config = config.clone();
        let starter =
            tokio::task::spawn_blocking(move || lifecycle_client::ensure(&starter_config, false));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!starter.is_finished(), "an initializing owner is not ready");
        assert!(
            lifecycle.accepting_calls(),
            "a compatible initializer must not be drained"
        );
        server.service.mark_ready();
        assert!(starter.await.unwrap().unwrap());
        let health_config = config.clone();
        let health = tokio::task::spawn_blocking(move || send_health_request(&health_config))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(health.version, VERSION);
        assert_ne!(build_epoch(), 0, "the test binary has a readable mtime");
        assert_eq!(health.build_epoch, build_epoch());
        let pending = lifecycle.begin().unwrap();
        assert!(
            query(&config, operation::HEALTH)
                .await
                .unwrap()
                .unwrap()
                .ready
        );
        let drain = query(&config, operation::DRAIN).await.unwrap().unwrap();
        assert!(drain.draining);
        assert!(!drain.ready);
        assert_eq!(drain.active, 1);
        assert!(lifecycle.begin().is_none());
        drop(pending);
        assert_eq!(
            query(&config, operation::HEALTH)
                .await
                .unwrap()
                .unwrap()
                .active,
            0
        );
    }

    #[tokio::test]
    async fn dropping_control_server_releases_the_listener_and_handler() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let server = serve(&config, Arc::new(Lifecycle::default()))
            .await
            .unwrap();
        let handler = Arc::downgrade(&server.service);
        tokio::task::yield_now().await;
        drop(server);
        tokio::time::timeout(Duration::from_secs(2), async {
            while handler.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped control server leaked its listener task");
        let mut replacement = serve(&config, Arc::new(Lifecycle::default()))
            .await
            .unwrap();
        replacement.finish().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_exit_preserves_the_drain_acknowledgement() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let lifecycle = Arc::new(Lifecycle::default());
        let mut server = serve(&config, Arc::clone(&lifecycle)).await.unwrap();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(wire::VERSION);
        coord.write_phase(DaemonPhase::Ready).unwrap();
        server.service.mark_ready();
        let probe_config = config.clone();
        let client = tokio::spawn(async move { query(&config, operation::DRAIN).await });
        tokio::time::timeout(Duration::from_secs(3), lifecycle.draining())
            .await
            .unwrap();
        server.finish().await;
        assert!(
            query(&probe_config, operation::HEALTH).await.is_err(),
            "finished server still accepts control requests"
        );
        drop(server);
        let acknowledged = client.await.unwrap().unwrap().unwrap();
        assert!(acknowledged.draining);
        assert!(!acknowledged.ready);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_record_cannot_substitute_another_pid_or_cache_instance() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let lifecycle = Arc::new(Lifecycle::default());
        let _server = serve(&config, Arc::clone(&lifecycle)).await.unwrap();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(wire::VERSION);
        coord.pid = std::process::id().saturating_add(1);
        coord.write_phase(DaemonPhase::Starting).unwrap();
        assert!(query(&config, operation::DRAIN).await.is_err());
        let probe_config = config.clone();
        let failure = tokio::task::spawn_blocking(move || {
            lifecycle_client::current(&probe_config, Instant::now() + Duration::from_secs(2))
        })
        .await
        .unwrap()
        .expect_err("identity mismatch must not become a retryable startup observation");
        assert!(matches!(
            failure.downcast_ref::<kunobi_daemon::client::RequestError>(),
            Some(kunobi_daemon::client::RequestError::Peer(
                kunobi_daemon::peer::Rejected::OtherProcess { .. }
            ))
        ));
        assert!(lifecycle.accepting_calls());
        let other = super::super::tests::test_config(&root.path().join("another-cache"));
        assert!(wire::negotiate(&offer(&config).unwrap(), &offer(&other).unwrap()).is_err());
        let mut foreign = offer(&config).unwrap();
        foreign.service_id[0] ^= 1;
        assert!(wire::negotiate(&offer(&config).unwrap(), &foreign).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn legacy_control_distinguishes_truncation_from_oversized_responses() {
        use std::io::{BufRead, Write};
        for (payload, expected) in [
            (b"partial".to_vec(), std::io::ErrorKind::UnexpectedEof),
            (vec![b'x'; 1 << 16], std::io::ErrorKind::InvalidData),
        ] {
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("legacy.sock");
            let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
            listener.set_nonblocking(true).unwrap();
            let server = std::thread::spawn(move || {
                for _ in 0..2 {
                    let (mut stream, _) = kunobi_daemon::readiness::wait_until(
                        Instant::now() + Duration::from_secs(5),
                        |_| match listener.accept() {
                            Ok(peer) => Ok(Some(peer)),
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                Ok(None)
                            }
                            Err(error) => Err(error),
                        },
                    )
                    .unwrap()
                    .expect("legacy client never connected");
                    // macOS can inherit the listener's nonblocking mode.
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut request = String::new();
                    std::io::BufReader::new(&stream)
                        .read_line(&mut request)
                        .unwrap();
                    assert!(request.ends_with('\n'));
                    stream.write_all(&payload).unwrap();
                }
            });
            let error = legacy_request(
                &socket,
                &Request::Health,
                Instant::now() + Duration::from_secs(2),
            )
            .unwrap_err();
            assert_eq!(
                error.downcast_ref::<std::io::Error>().unwrap().kind(),
                expected
            );
            let health =
                lifecycle_client::current_socket(&socket, Instant::now() + Duration::from_secs(2));
            match expected {
                std::io::ErrorKind::InvalidData => {
                    assert!(health.is_err(), "invalid frame is not retryable")
                }
                _ => assert!(
                    health.unwrap().is_none(),
                    "closed legacy peer may still be starting"
                ),
            }
            server.join().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn an_unavailable_advertised_binary_endpoint_never_downgrades_to_legacy() {
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let legacy = std::os::unix::net::UnixListener::bind(config.socket_path()).unwrap();
        legacy.set_nonblocking(true).unwrap();
        let mut coord = DaemonCoordFile::for_socket(&config.socket_path());
        coord.control_version = Some(wire::VERSION);
        coord.write_phase(DaemonPhase::Ready).unwrap();
        assert!(
            lifecycle_client::current(&config, Instant::now() + Duration::from_millis(200))
                .unwrap()
                .is_none()
        );
        assert_eq!(
            legacy.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        assert!(send_health_request(&config).is_err());
        assert_eq!(
            legacy.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
