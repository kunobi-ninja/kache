//! Kache identity and endpoint adapter for shared lifecycle control.
use super::*;
use kunobi_daemon::{
    ServiceIdentity,
    admission::{Admission, Limits, Pool},
    control::ControlService,
    local::Duplex,
    transport::SplitIo,
    wire::{self, operation},
};

// Protocol masks: HEALTH | HEALTH_DETAILS | DRAIN, and required health details.
const CONTROL_SUPPORTED: u64 = 0x13;
const CONTROL_REQUIRED: u64 = 0x11;

// Stable service-family UUID. Cache instance separation is independent of builds.
const SERVICE_ID: [u8; 16] = [
    0xa1, 0x94, 0xcb, 0xee, 0x70, 0x11, 0x43, 0x9c, 0xb6, 0x7e, 0x62, 0x96, 0x2a, 0x3a, 0xd8, 0x51,
];

pub(super) fn endpoint(config: &Config) -> PathBuf {
    config.socket_path().with_extension("control.v2.sock")
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
    let listener =
        crate::transport::bind_daemon_listener(&path)?.context("control endpoint already owned")?;
    let socket = SocketCleanupGuard::new(&path)?;
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
                            use interprocess::local_socket::traits::StreamCommon as _;
                            let Some(pid) = stream.peer_creds().ok().and_then(|creds| creds.pid()) else { return; };
                            if kunobi_daemon::local::windows::verify_process_user(pid).is_err() { return; }
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
    let stream = connect(&endpoint(config), deadline)?;
    let pid = stream.peer_pid().context("reading lifecycle peer PID")?;
    anyhow::ensure!(
        pid == state.pid,
        "lifecycle peer differs from advertised process"
    );
    let (read, write) = stream.split().context("splitting lifecycle socket")?;
    let mut session = wire::Session::connect(SplitIo { read, write }, &offer(config)?)
        .context("negotiating lifecycle control")?;
    let request = wire::Control {
        operation,
        ..Default::default()
    };
    session
        .send(&request)
        .context("sending lifecycle control")?;
    let health = wire::Health::from_response(
        &session.receive().context("receiving lifecycle control")?,
        &request,
    )?;
    anyhow::ensure!(
        health.process_id == pid,
        "health response differs from OS peer"
    );
    anyhow::ensure!(Instant::now() < deadline, "late lifecycle response");
    Ok(Some(health))
}

pub(super) fn health(config: &Config, deadline: Instant) -> Result<Option<wire::Health>> {
    request(config, operation::HEALTH, deadline)
}

type Local = kunobi_daemon::local::PlatformDuplex;

fn connect(path: &Path, deadline: Instant) -> Result<Local> {
    let stream = Local::connect_once_until(&crate::transport::daemon_endpoint(path), deadline)?;
    stream
        .verify_peer_user()
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

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(
            failure
                .to_string()
                .contains("differs from advertised process")
        );
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
