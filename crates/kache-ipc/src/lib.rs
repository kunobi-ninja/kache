//! Cross-platform local IPC transport for the daemon.
//!
//! The daemon's local socket and the wrapper / CLI clients use the
//! `interprocess` crate so the same API works on Unix domain sockets and
//! Windows named pipes. This module hides the `Name` resolution machinery
//! and provides a `prelude` with the trait imports callers need.
//!
//! # Naming
//!
//! kache addresses the daemon by a filesystem path (by default
//! `<runtime_dir>/daemon.sock`). On Unix that becomes the literal
//! UDS path. On Windows the path is translated to a named pipe under
//! `\\.\pipe\` by `interprocess`.

use anyhow::{Context, Result};
use std::path::Path;

mod deadline_io;
pub use deadline_io::finish_by;

pub use interprocess::local_socket::ListenerOptions;
#[cfg(feature = "async")]
pub use interprocess::local_socket::tokio::Stream as TokioStream;
pub use interprocess::local_socket::{Name, Stream as SyncStream};
// TokioListener is the type produced by `ListenerOptions::new().create_tokio()`.
// Re-exported so the daemon's `accept_loop` and its tests can name it explicitly.
#[cfg(feature = "async")]
pub use interprocess::local_socket::tokio::Listener as TokioListener;

/// Trait imports needed to call methods on the listener / stream types.
/// Most users want `use crate::prelude::*;`.
pub mod prelude {
    #[cfg(feature = "async")]
    pub use interprocess::local_socket::traits::tokio::{Listener as _, Stream as _};
    pub use interprocess::local_socket::traits::{Listener as _, Stream as _};
}

/// Why the daemon cannot bind a local socket at `path`, or `None` when it can.
///
/// A Unix socket address has a fixed-size path field, so a deep runtime
/// directory makes the daemon fail to start. kunobi-daemon reports the length
/// and the limit; this adds the two kache settings that move the socket.
/// Windows names a pipe after the path, which has no such limit.
pub fn socket_path_problem(path: &Path) -> Option<String> {
    #[cfg(unix)]
    if let Err(error) = kunobi_daemon::local::unix_socket::check_path(path) {
        return Some(format!(
            "{error}. Point KACHE_SOCKET_PATH at a shorter absolute path, or set a shorter \
             [cache] runtime_dir."
        ));
    }
    #[cfg(not(unix))]
    let _ = path;
    None
}

/// Bind through shared ownership and disable the transport's unchecked unlink.
#[cfg(feature = "async")]
pub async fn bind_daemon_listener(path: &Path) -> Result<Option<TokioListener>> {
    use interprocess::local_socket::traits::tokio::Listener as _;
    if let Some(problem) = socket_path_problem(path) {
        anyhow::bail!(problem);
    }
    #[cfg(unix)]
    let mut listener: TokioListener = {
        use kunobi_daemon::local::unix_socket::{self, Bound};
        let bound = unix_socket::acquire(path)
            .map_err(|error| anyhow::anyhow!("acquiring daemon socket: {error:?}"))?;
        let Bound::Won(listener) = bound else {
            return Ok(None);
        };
        let fd: std::os::fd::OwnedFd = listener.into();
        interprocess::os::unix::uds_local_socket::tokio::Listener::try_from(fd)?.into()
    };
    #[cfg(windows)]
    let mut listener = {
        use kunobi_daemon::local::windows_socket::{self, Bound};
        let bound = windows_socket::acquire_tokio(Path::new(&*daemon_endpoint(path)))
            .await
            .map_err(|error| anyhow::anyhow!("acquiring daemon pipe: {error:?}"))?;
        let Bound::Won(listener) = bound else {
            return Ok(None);
        };
        listener
    };
    listener.do_not_reclaim_name_on_drop();
    Ok(Some(listener))
}

/// The kunobi-daemon endpoint for a daemon socket path.
///
/// This name is where kache processes meet. A daemon and the clients looking
/// for it must derive the same value, so it is computed in exactly one place:
/// a second copy that drifted would leave a running daemon unreachable after
/// an upgrade. On Windows it is a hash of the path, because a named pipe lives
/// in its own namespace rather than on the filesystem.
pub fn daemon_endpoint(path: &Path) -> Box<kunobi_daemon::local::Endpoint> {
    #[cfg(unix)]
    {
        path.into()
    }
    #[cfg(windows)]
    {
        let hash = blake3::hash(path.as_os_str().as_encoded_bytes());
        format!("kache-daemon-{}", &hash.to_hex()[..16]).into_boxed_str()
    }
}

/// Build the platform-appropriate IPC name for a path.
///
/// On Unix, the path is used directly as a filesystem UDS path via
/// `GenericFilePath`. On Windows, the path cannot be used as a named pipe
/// path directly (named pipes must live under `\\.\pipe\`), so we derive a
/// namespaced name from the path via `GenericNamespaced`.
pub fn socket_name(path: &Path) -> Result<Name<'static>> {
    #[cfg(unix)]
    {
        use interprocess::local_socket::{GenericFilePath, ToFsName};
        path.to_fs_name::<GenericFilePath>()
            .with_context(|| format!("converting {} to local-socket name", path.display()))
            .map(|n| n.into_owned())
    }
    #[cfg(windows)]
    {
        use interprocess::local_socket::{GenericNamespaced, ToNsName};
        daemon_endpoint(path)
            .to_ns_name::<GenericNamespaced>()
            .with_context(|| format!("converting {} to named pipe name", path.display()))
            .map(|n| n.into_owned())
    }
}

/// Reject a connection whose peer is not the daemon's own user.
///
/// Defense-in-depth behind the `0600` mode kunobi-daemon gives the socket: even
/// if the socket file ends up reachable by others (nonstandard umask, relinked
/// path, shared cache dir), every accepted connection is checked against the peer's
/// effective UID via `SO_PEERCRED` (Linux) / `getpeereid` (macOS, BSDs).
/// Fails closed when credentials cannot be determined — a peer we cannot
/// authenticate is a peer we refuse. Windows named pipes carry peer identity
/// in the kernel object ACL rather than creds messages, so the check is
/// Unix-only.
///
/// The decision is taken over an `Option<u32>` rather than a live stream so
/// every branch is assertable: CI cannot open a connection from a foreign
/// UID, and cannot make the platform withhold credentials.
#[cfg(unix)]
pub fn require_self_peer(peer_euid: Option<u32>) -> std::io::Result<()> {
    match peer_euid {
        Some(peer_uid) if peer_uid_is_self(peer_uid) => Ok(()),
        Some(peer_uid) => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "peer uid {peer_uid} does not match daemon uid {}",
                self_uid()
            ),
        )),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("peer credentials unavailable (daemon uid {})", self_uid()),
        )),
    }
}

/// The peer's effective UID, where the platform exposes it: `SO_PEERCRED` on
/// Linux, `getpeereid` on macOS and the BSDs. `None` means we could not
/// authenticate the peer, which [`require_self_peer`] treats as a refusal.
#[cfg(all(unix, feature = "async"))]
pub fn peer_euid(stream: &TokioStream) -> Option<u32> {
    use interprocess::local_socket::traits::StreamCommon as _;
    stream.peer_creds().ok().and_then(|creds| creds.euid())
}

#[cfg(all(unix, feature = "async"))]
pub fn peer_pid(stream: &TokioStream) -> Option<u32> {
    use interprocess::local_socket::traits::StreamCommon as _;
    stream
        .peer_creds()
        .ok()?
        .pid()
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid != 0)
}

/// Kernel audit identity. Keep its representation opaque and use libbsm to
/// interpret it; sandbox checks consume the complete token, including PID version.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PeerAuditToken([u32; 8]);

#[cfg(target_os = "macos")]
impl PeerAuditToken {
    pub fn pid(self) -> libc::pid_t {
        #[link(name = "bsm")]
        unsafe extern "C" {
            fn audit_token_to_pid(token: PeerAuditToken) -> libc::pid_t;
        }
        unsafe { audit_token_to_pid(self) }
    }
}

#[cfg(all(target_os = "macos", feature = "async"))]
pub fn peer_audit_token(stream: &TokioStream) -> std::io::Result<PeerAuditToken> {
    use std::os::fd::AsRawFd;
    let TokioStream::UdSocket(socket) = stream;
    let mut token = PeerAuditToken([0; 8]);
    let mut length = std::mem::size_of::<PeerAuditToken>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            socket.inner().as_raw_fd(),
            libc::SOL_LOCAL,
            libc::LOCAL_PEERTOKEN,
            (&mut token as *mut PeerAuditToken).cast(),
            &mut length,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if length as usize != std::mem::size_of::<PeerAuditToken>() || token.pid() <= 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid peer audit identity",
        ));
    }
    Ok(token)
}

/// The daemon's effective UID (test seam: the comparison lives here so the
/// accept/reject decision is assertable without a foreign-UID connection).
#[cfg(unix)]
fn self_uid() -> u32 {
    unsafe { libc::geteuid() }
}

/// Does a peer's effective UID belong to the daemon's own user?
#[cfg(unix)]
fn peer_uid_is_self(peer_uid: u32) -> bool {
    peer_uid == self_uid()
}

/// How long a client waits for a busy daemon pipe on Windows before it gives
/// up as if no daemon ran.
///
/// When every instance of the daemon's pipe is taken (`ERROR_PIPE_BUSY`), a
/// plain connect blocks in `WaitNamedPipeW` until one frees, so a stalled
/// daemon would hang every wrapper that probes it or hands it work. A missing
/// pipe still fails at once.
pub const BUSY_PIPE_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

/// The named pipe a daemon socket path stands for on Windows: the pipe
/// `socket_name` names, spelled as a path.
#[cfg(windows)]
pub fn pipe_path(socket: &Path) -> String {
    format!(r"\\.\pipe\{}", daemon_endpoint(socket))
}

/// Connect a synchronous client to the daemon socket at `socket`.
///
/// On Windows a pipe still busy after `busy_wait` fails with `TimedOut`,
/// which callers treat as the failed connection a missing daemon gives. This
/// goes through interprocess's named-pipe layer: its local-socket layer drops
/// the wait mode of `ConnectOptions` on Windows (interprocess 2.4.4 to 2.4.6
/// call `connect_by_path`, an unbounded wait, whatever the options say), so a
/// `ConnectWaitMode::Timeout` set there never takes effect. On Unix this is
/// `SyncStream::connect`, unchanged.
pub fn connect_sync(socket: &Path, busy_wait: std::time::Duration) -> std::io::Result<SyncStream> {
    #[cfg(windows)]
    {
        use interprocess::os::windows::named_pipe::{DuplexPipeStream, local_socket, pipe_mode};
        let pipe = DuplexPipeStream::<pipe_mode::Bytes>::connect_by_path_with_wait_mode(
            pipe_path(socket),
            interprocess::ConnectWaitMode::Timeout(busy_wait),
        )?;
        Ok(local_socket::Stream::from(pipe).into())
    }
    #[cfg(not(windows))]
    {
        use interprocess::local_socket::traits::Stream as _;
        let _ = busy_wait;
        SyncStream::connect(socket_name(socket).map_err(std::io::Error::other)?)
    }
}

/// Connect a Tokio client to the daemon socket at `socket`, with
/// [`connect_sync`]'s bound on a busy Windows pipe.
///
/// Without it the connect waits for a busy pipe in a blocking task with no
/// limit: a timeout around the request does not end that task, and dropping
/// the runtime that owns it waits for it, so a caller's own timeout can still
/// hang. On Unix this is `TokioStream::connect`, unchanged.
#[cfg(feature = "async")]
pub async fn connect_tokio(
    socket: &Path,
    busy_wait: std::time::Duration,
) -> std::io::Result<TokioStream> {
    #[cfg(windows)]
    {
        use interprocess::os::windows::named_pipe::{local_socket, pipe_mode, tokio};
        // Zero-wait attempts never enter interprocess's spawn_blocking path.
        // Cancellation and runtime drop therefore leave no busy-pipe waiter.
        let path = pipe_path(socket);
        let deadline = ::tokio::time::Instant::now() + busy_wait;
        loop {
            match tokio::DuplexPipeStream::<pipe_mode::Bytes>::connect_by_path_with_wait_mode(
                path.as_str(),
                interprocess::ConnectWaitMode::Timeout(std::time::Duration::ZERO),
            )
            .await
            {
                Ok(pipe) => return Ok(local_socket::tokio::Stream::from(pipe).into()),
                Err(error)
                    if error.kind() == std::io::ErrorKind::TimedOut
                        && ::tokio::time::Instant::now() < deadline =>
                {
                    let next = (::tokio::time::Instant::now()
                        + std::time::Duration::from_millis(10))
                    .min(deadline);
                    ::tokio::time::sleep_until(next).await;
                    if ::tokio::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
    #[cfg(not(windows))]
    {
        use interprocess::local_socket::traits::tokio::Stream as _;
        let _ = busy_wait;
        TokioStream::connect(socket_name(socket).map_err(std::io::Error::other)?).await
    }
}

/// Close a failed or completed client exchange without interprocess starting
/// an unbounded background pipe flush. A reply proves the request was read;
/// failed exchanges may discard their remaining bytes.
#[cfg(all(windows, feature = "async"))]
#[must_use = "keep this guard alive through the exchange"]
pub fn discard_pipe_writes_on_drop(stream: &TokioStream) -> impl Drop + '_ {
    struct Discard<'a>(&'a TokioStream);
    impl Drop for Discard<'_> {
        fn drop(&mut self) {
            let TokioStream::NamedPipe(pipe) = self.0;
            pipe.inner().assume_flushed();
        }
    }
    Discard(stream)
}

/// Keep synchronous pipe cleanup inside the bounded I/O worker. Its caller
/// explicitly flushes a successful no-response request before releasing it.
#[cfg(windows)]
#[must_use = "keep this guard alive through the exchange"]
pub fn discard_sync_pipe_writes_on_drop(stream: &SyncStream) -> impl Drop + '_ {
    struct Discard<'a>(&'a SyncStream);
    impl Drop for Discard<'_> {
        fn drop(&mut self) {
            let SyncStream::NamedPipe(pipe) = self.0;
            pipe.inner().assume_flushed();
        }
    }
    Discard(stream)
}

/// True if a daemon socket / named pipe at `path` accepts a connection
/// right now. Used by liveness probes — does not check whether the daemon
/// is responsive, only whether *something* is listening. A Windows pipe
/// still busy after [`BUSY_PIPE_WAIT`] counts as unreachable.
pub fn is_reachable(path: &Path) -> bool {
    connect_sync(path, BUSY_PIPE_WAIT).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn a_socket_path_fits_up_to_the_platform_limit_and_not_one_byte_more() {
        let max = kunobi_daemon::local::unix_socket::MAX_PATH_BYTES;
        let fits = format!("/{}", "a".repeat(max - 1));
        assert_eq!(fits.len(), max);
        assert_eq!(socket_path_problem(Path::new(&fits)), None);

        let too_long = format!("{fits}b");
        let problem = socket_path_problem(Path::new(&too_long)).expect("one byte over the limit");
        assert!(problem.contains(&format!("{} bytes", max + 1)), "{problem}");
        assert!(problem.contains("KACHE_SOCKET_PATH"), "{problem}");
        assert!(problem.contains("runtime_dir"), "{problem}");
    }

    #[cfg(all(unix, feature = "async"))]
    #[test]
    fn binding_an_overlong_path_names_the_limit() {
        let path = format!(
            "/{}",
            "a".repeat(kunobi_daemon::local::unix_socket::MAX_PATH_BYTES)
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let Err(error) = runtime.block_on(bind_daemon_listener(Path::new(&path))) else {
            panic!("an overlong path cannot bind");
        };
        assert!(error.to_string().contains("KACHE_SOCKET_PATH"), "{error:#}");
    }

    #[test]
    fn socket_name_builds_for_a_plausible_path() {
        // On Unix the path becomes a UDS filesystem name; on Windows it is
        // hashed into a `\\.\pipe\` namespaced name. Either way a normal path
        // must resolve without error.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("kache-daemon.sock");
        assert!(socket_name(&path).is_ok());
    }

    #[test]
    fn is_reachable_is_false_when_nothing_listens() {
        // A path under a fresh temp dir has no listener bound, so a connect
        // attempt must fail and `is_reachable` must report false.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("definitely-not-listening.sock");
        assert!(!is_reachable(&path));
    }

    #[cfg(unix)]
    #[test]
    fn is_reachable_is_true_for_a_live_listener() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.sock");
        let name = socket_name(&path).unwrap();
        // Hold the listener open for the duration of the probe.
        let _listener = ListenerOptions::new()
            .name(name)
            .create_sync()
            .expect("bind listener");
        assert!(is_reachable(&path));
    }

    /// Every pipe instance taken: the second client gets `ERROR_PIPE_BUSY`.
    /// An unbounded connect would wait forever; this one must give up.
    #[cfg(windows)]
    #[test]
    fn a_busy_pipe_fails_within_the_wait_instead_of_hanging() {
        use interprocess::local_socket::traits::Stream as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("busy.sock");
        // One instance, never accepted: the first client takes it.
        let _listener = ListenerOptions::new()
            .name(socket_name(&path).unwrap())
            .create_sync()
            .expect("bind listener");
        let _first = SyncStream::connect(socket_name(&path).unwrap()).expect("first client");

        let (done, finished) = std::sync::mpsc::channel();
        let probe = path.clone();
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let result = connect_sync(&probe, std::time::Duration::from_millis(300));
            let _ = done.send((result.map(|_| ()), started.elapsed()));
        });
        let (result, elapsed) = finished
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("connect to a busy pipe hung");
        assert!(result.is_err(), "a busy pipe cannot take a second client");
        assert!(elapsed < std::time::Duration::from_secs(10), "{elapsed:?}");
    }

    /// The Tokio client gives up on a busy pipe too, so dropping its runtime
    /// afterwards has no blocking task left to wait for.
    #[cfg(all(windows, feature = "async"))]
    #[test]
    fn a_busy_pipe_fails_within_the_wait_for_a_tokio_client() {
        use interprocess::local_socket::traits::Stream as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("busy-tokio.sock");
        let _listener = ListenerOptions::new()
            .name(socket_name(&path).unwrap())
            .create_sync()
            .expect("bind listener");
        let _first = SyncStream::connect(socket_name(&path).unwrap()).expect("first client");

        let (done, finished) = std::sync::mpsc::channel();
        let probe = path.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result =
                runtime.block_on(connect_tokio(&probe, std::time::Duration::from_millis(300)));
            // Dropping the runtime must not wait on a connect still blocked.
            drop(runtime);
            let _ = done.send(result.map(|_| ()));
        });
        let result = finished
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("Tokio connect to a busy pipe hung");
        assert!(result.is_err(), "a busy pipe cannot take a second client");
    }

    /// Cancel an outer request before its longer connect wait. Dropping the
    /// runtime must not join an abandoned blocking named-pipe connect.
    #[cfg(all(windows, feature = "async"))]
    #[test]
    fn cancelling_a_busy_connect_does_not_delay_runtime_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cancel-busy.sock");
        let _listener = ListenerOptions::new()
            .name(socket_name(&path).unwrap())
            .create_sync()
            .unwrap();
        let _first = connect_sync(&path, std::time::Duration::ZERO).unwrap();
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let started = std::time::Instant::now();
            assert!(
                runtime
                    .block_on(async {
                        tokio::time::timeout(
                            std::time::Duration::from_millis(75),
                            connect_tokio(&path, std::time::Duration::from_secs(2)),
                        )
                        .await
                    })
                    .is_err()
            );
            drop(runtime);
            done.send(started.elapsed()).unwrap();
        });
        let elapsed = finished
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("runtime drop waited for the cancelled connect");
        assert!(
            elapsed < std::time::Duration::from_millis(750),
            "{elapsed:?}"
        );
    }

    /// A free instance: the bounded connect takes it at once, and the stream
    /// it returns carries bytes both ways like `SyncStream::connect`'s.
    #[cfg(windows)]
    #[test]
    fn a_free_pipe_connects_and_carries_bytes_both_ways() {
        use interprocess::local_socket::traits::Listener as _;
        use std::io::{Read, Write};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("free.sock");
        let listener = ListenerOptions::new()
            .name(socket_name(&path).unwrap())
            .create_sync()
            .expect("bind listener");
        let server = std::thread::spawn(move || {
            let mut stream = listener.accept().expect("accept");
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).unwrap();
            stream.write_all(&[byte[0] + 1]).unwrap();
        });
        let started = std::time::Instant::now();
        let mut client =
            connect_sync(&path, std::time::Duration::from_millis(300)).expect("connect");
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        client.write_all(&[41]).unwrap();
        let mut reply = [0u8; 1];
        client.read_exact(&mut reply).unwrap();
        assert_eq!(reply, [42]);
        server.join().unwrap();
        assert!(
            !is_reachable_within_test(&path),
            "the pipe is gone with its listener"
        );
    }

    /// The same for the Tokio client.
    #[cfg(all(windows, feature = "async"))]
    #[test]
    fn a_free_pipe_connects_a_tokio_client_and_carries_bytes() {
        use interprocess::local_socket::traits::Listener as _;
        use std::io::{Read, Write};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("free-tokio.sock");
        let listener = ListenerOptions::new()
            .name(socket_name(&path).unwrap())
            .create_sync()
            .expect("bind listener");
        let server = std::thread::spawn(move || {
            let mut stream = listener.accept().expect("accept");
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).unwrap();
            stream.write_all(&[byte[0] + 1]).unwrap();
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let reply = runtime.block_on(async {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut client = connect_tokio(&path, std::time::Duration::from_millis(300))
                .await
                .expect("connect");
            client.write_all(&[41]).await.unwrap();
            let mut reply = [0u8; 1];
            client.read_exact(&mut reply).await.unwrap();
            reply
        });
        assert_eq!(reply, [42]);
        server.join().unwrap();
    }

    /// The pipe disappears once its listener is gone; probing it again must
    /// not wait for anything.
    #[cfg(windows)]
    fn is_reachable_within_test(path: &Path) -> bool {
        let started = std::time::Instant::now();
        let reachable = is_reachable(path);
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
        reachable
    }

    #[test]
    fn finish_by_returns_what_finishes_in_time() {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        assert_eq!(finish_by(deadline, || 7), Some(7));
    }

    #[test]
    fn finish_by_stops_waiting_at_the_deadline() {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let started = std::time::Instant::now();
        let deadline = started + std::time::Duration::from_millis(200);
        let result = finish_by(deadline, move || {
            // Blocks until the test lets go: work that never ends on its own.
            let _ = blocked.recv();
            1
        });
        assert_eq!(result, None);
        let waited = started.elapsed();
        assert!(
            waited >= std::time::Duration::from_millis(200),
            "{waited:?}"
        );
        assert!(waited < std::time::Duration::from_secs(5), "{waited:?}");
        drop(release);
    }

    #[cfg(unix)]
    #[test]
    fn require_self_peer_rejects_foreign_and_unauthenticated_peers() {
        // The accept direction is also covered end-to-end by
        // `same_user_peer_check_accepts_own_connections`; this pins every
        // branch, including the two CI cannot produce with a real
        // connection: a foreign UID, and a platform that withholds creds.
        let mine = self_uid();
        assert!(require_self_peer(Some(mine)).is_ok());

        let foreign = require_self_peer(Some(mine.wrapping_add(1))).unwrap_err();
        assert_eq!(foreign.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(foreign.to_string().contains("does not match daemon uid"));

        // Fail closed: a peer we cannot authenticate is a peer we refuse.
        let unknown = require_self_peer(None).unwrap_err();
        assert_eq!(unknown.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(unknown.to_string().contains("credentials unavailable"));

        assert!(peer_uid_is_self(mine));
        assert!(!peer_uid_is_self(mine.wrapping_add(0xdead_beef)));
    }

    #[cfg(all(unix, feature = "async"))]
    #[tokio::test]
    async fn same_user_peer_check_accepts_own_connections() {
        use crate::prelude::*;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.sock");
        let name = socket_name(&path).unwrap();
        let listener = ListenerOptions::new()
            .name(name)
            .create_tokio()
            .expect("bind listener");

        let client = tokio::spawn(async move {
            TokioStream::connect(socket_name(&path).unwrap())
                .await
                .expect("connect")
        });
        let (stream, client) = tokio::join!(listener.accept(), client);

        // The test client runs under the same effective UID as the "daemon",
        // so the credential gate must accept it. Rejection of foreign UIDs is
        // not unit-testable without privilege switching.
        let stream = stream.expect("accept");
        require_self_peer(peer_euid(&stream)).expect("same-user peer accepted");
        let client = client.expect("client task");
        require_self_peer(peer_euid(&client)).expect("same-user server accepted");
        #[cfg(target_os = "linux")]
        for connection in [&stream, &client] {
            assert_eq!(peer_pid(connection), Some(std::process::id()));
        }
    }
}

/// Unix descriptor transfer and its close-on-exec admission boundary.
#[cfg(unix)]
pub mod descriptors;

/// Owned Windows streams duplicated from a connected same-user pipe peer.
#[cfg(windows)]
pub mod windows_handles;

/// Request-owned Unix jobserver inheritance.
#[cfg(unix)]
pub mod jobserver;

#[cfg(any(unix, windows))]
pub mod execution;
#[cfg(windows)]
pub mod windows_msvc;
#[cfg(windows)]
pub mod windows_process;
#[cfg(windows)]
mod windows_security;
