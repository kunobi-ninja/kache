//! Synchronous compiler handoff with fallback rights ending at the first Commit write.

use crate::prelude::*;
use anyhow::{Result, bail, ensure};
use kache_protocol::execution::{
    EXECUTION_VERSION, ExecutionCommand, ExecutionReply, ExecutionRequest,
};
use std::io::{BufRead, Read, Write};
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
use std::time::Duration;

const PREPARE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_REPLY_BYTES: u64 = 64 << 10;

/// `None` permits local execution. An error means Commit may have reached the
/// daemon and the caller must not run the compiler again.
pub fn try_execute(
    socket: &Path,
    request: &ExecutionRequest,
    #[cfg(unix)] descriptors: &[BorrowedFd<'_>],
) -> Result<Option<i32>> {
    let mut committed = false;
    match exchange(
        socket,
        request,
        #[cfg(unix)]
        descriptors,
        &mut committed,
    ) {
        Ok(exit) => Ok(Some(exit)),
        Err(error) if !committed => {
            tracing::debug!(%error, "daemon did not prepare compiler execution");
            Ok(None)
        }
        Err(error) => Err(error.context(
            "daemon compilation did not complete; the compiler will not be run again locally",
        )),
    }
}

fn exchange(
    socket: &Path,
    request: &ExecutionRequest,
    #[cfg(unix)] descriptors: &[BorrowedFd<'_>],
    committed: &mut bool,
) -> Result<i32> {
    let stream = Connection::connect(socket)?;
    write_message(
        &mut &stream,
        &kache_protocol::Request::ExecuteV1(Box::new(request.clone())),
    )?;
    let mut reader = std::io::BufReader::new(&stream);
    let ready = read_reply(&mut reader)?;
    #[cfg(unix)]
    {
        let ticket = match ready {
            ExecutionReply::Ready {
                version: EXECUTION_VERSION,
                descriptors: Some(ticket),
            } => ticket,
            reply => bail!("compiler execution was not ready: {reply:?}"),
        };
        let endpoint = PathBuf::from(ticket.endpoint.decode()?);
        ensure!(endpoint.is_absolute(), "invalid descriptor endpoint");
        let datagram = std::os::unix::net::UnixDatagram::unbound()?;
        datagram.set_write_timeout(Some(PREPARE_TIMEOUT))?;
        datagram.connect(endpoint)?;
        crate::descriptors::send(datagram.as_fd(), &ticket.key, descriptors)?;
    }
    #[cfg(windows)]
    ensure!(
        matches!(
            ready,
            ExecutionReply::Ready {
                version: EXECUTION_VERSION,
                descriptors: None
            }
        ),
        "compiler execution was not ready: {ready:?}"
    );
    ensure!(
        matches!(read_reply(&mut reader)?, ExecutionReply::Prepared),
        "compiler execution was not prepared"
    );

    stream.prepare_commit()?;
    send_commit(&mut &stream, committed)?;
    stream.commit_written()?;
    ensure!(
        matches!(read_reply(&mut reader)?, ExecutionReply::Accepted),
        "daemon did not acknowledge compiler execution"
    );
    match read_reply(&mut reader)? {
        ExecutionReply::Complete { exit_code } => Ok(exit_code),
        ExecutionReply::Failed { message } => bail!("{message}"),
        reply => bail!("invalid compiler completion: {reply:?}"),
    }
}

/// Windows pipes have no socket timeout API. Preparation uses nonblocking
/// reads/writes under one deadline, then completion waits in the kernel.
struct Connection {
    inner: crate::SyncStream,
    #[cfg(windows)]
    deadline: std::cell::Cell<Option<std::time::Instant>>,
}

impl Connection {
    fn connect(socket: &Path) -> Result<Self> {
        #[cfg(unix)]
        let inner = crate::SyncStream::connect(crate::socket_name(socket)?)?;
        #[cfg(windows)]
        let (inner, deadline): (crate::SyncStream, std::time::Instant) = {
            use interprocess::os::windows::named_pipe::{DuplexPipeStream, pipe_mode::Bytes};
            let deadline = std::time::Instant::now() + PREPARE_TIMEOUT;
            // The generic local-socket adapter in interprocess 2.4.4 ignores
            // ConnectOptions::wait_mode on Windows. Keep its native pipe's
            // bounded connect, then wrap the same owned handle.
            let path = format!(r"\\.\pipe\{}", crate::daemon_endpoint(socket));
            let pipe = DuplexPipeStream::<Bytes>::connect_by_path_with_wait_mode(
                path,
                interprocess::ConnectWaitMode::Timeout(PREPARE_TIMEOUT),
            )?;
            let handle: std::os::windows::io::OwnedHandle = pipe
                .try_into()
                .map_err(|_| anyhow::anyhow!("new named pipe unexpectedly has shared ownership"))?;
            let inner =
                interprocess::os::windows::named_pipe::local_socket::Stream::try_from(handle)
                    .map_err(|error| anyhow::anyhow!("wrapping compiler named pipe: {error:?}"))?;
            (inner.into(), deadline)
        };
        #[cfg(unix)]
        {
            use interprocess::local_socket::traits::StreamCommon as _;
            crate::require_self_peer(inner.peer_creds()?.euid())?;
            inner.set_recv_timeout(Some(PREPARE_TIMEOUT))?;
            inner.set_send_timeout(Some(PREPARE_TIMEOUT))?;
        }
        #[cfg(windows)]
        {
            use kunobi_daemon::local::peer::PeerCredentials as _;
            let peer = inner.credentials()?;
            ensure!(
                peer.same_user && peer.pid.is_some(),
                "compiler endpoint belongs to another user or has no peer identity"
            );
            inner.set_nonblocking(true)?;
        }
        Ok(Self {
            inner,
            #[cfg(windows)]
            deadline: std::cell::Cell::new(Some(deadline)),
        })
    }

    fn prepare_commit(&self) -> Result<()> {
        #[cfg(unix)]
        self.inner.set_recv_timeout(None)?;
        Ok(())
    }

    fn commit_written(&self) -> Result<()> {
        #[cfg(windows)]
        {
            // This fallible transition is after Commit: failure is final,
            // never permission to run the compiler a second time.
            self.inner.set_nonblocking(false)?;
            self.deadline.set(None);
        }
        Ok(())
    }
}

impl Read for &Connection {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            (&self.inner).read(bytes)
        }
        #[cfg(windows)]
        {
            let deadline = self.deadline.get();
            if deadline.is_none() || bytes.is_empty() {
                return (&self.inner).read(bytes);
            }
            use std::os::windows::io::{AsHandle, AsRawHandle};
            use windows_sys::Win32::System::Pipes::PeekNamedPipe;
            retry_until(deadline, || {
                let crate::SyncStream::NamedPipe(pipe) = &self.inner;
                let mut available = 0;
                if unsafe {
                    PeekNamedPipe(
                        pipe.as_handle().as_raw_handle(),
                        std::ptr::null_mut(),
                        0,
                        std::ptr::null_mut(),
                        &mut available,
                        std::ptr::null_mut(),
                    )
                } == 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                if available == 0 {
                    return Err(std::io::ErrorKind::WouldBlock.into());
                }
                // One reader owns this byte stream. Request no more than the
                // bytes already queued, so the setup read cannot block.
                let count = bytes.len().min(available as usize);
                (&self.inner).read(&mut bytes[..count])
            })
        }
    }
}

impl Write for &Connection {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        #[cfg(unix)]
        {
            (&self.inner).write(bytes)
        }
        #[cfg(windows)]
        {
            retry_until(self.deadline.get(), || match (&self.inner).write(bytes) {
                // PIPE_NOWAIT may report a successful zero-byte write when
                // its byte buffer is full; retry it under the same deadline.
                Ok(0) if !bytes.is_empty() => Err(std::io::ErrorKind::WouldBlock.into()),
                result => result,
            })
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (&self.inner).flush()
    }
}

#[cfg(any(test, windows))]
fn retry_until<T>(
    deadline: Option<std::time::Instant>,
    mut operation: impl FnMut() -> std::io::Result<T>,
) -> std::io::Result<T> {
    loop {
        match operation() {
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                if deadline.is_some_and(|until| std::time::Instant::now() >= until) {
                    return Err(std::io::ErrorKind::TimedOut.into());
                }
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            result => return result,
        }
    }
}

fn send_commit(writer: &mut impl Write, committed: &mut bool) -> Result<()> {
    // Do this BEFORE the first Commit write, including partial-write errors.
    *committed = true;
    write_message(writer, &ExecutionCommand::Commit)
}

fn write_message(writer: &mut impl Write, message: &impl serde::Serialize) -> Result<()> {
    let mut frame = serde_json::to_vec(message)?;
    frame.push(b'\n');
    writer.write_all(&frame)?;
    writer.flush()?;
    Ok(())
}

fn read_reply(reader: &mut impl BufRead) -> Result<ExecutionReply> {
    let mut frame = Vec::new();
    reader.take(MAX_REPLY_BYTES).read_until(b'\n', &mut frame)?;
    ensure!(
        frame.last() == Some(&b'\n'),
        "incomplete compiler execution response"
    );
    Ok(serde_json::from_slice(&frame)?)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use kache_protocol::execution::{ExecutionChannels, ExecutionEntry, NativeString};
    use std::fs::File;
    use std::os::unix::net::{UnixDatagram, UnixListener};

    #[derive(Clone, Copy, Debug)]
    enum Disconnect {
        BeforeReady,
        LegacyDaemon,
        BeforePrepared,
        AfterPrepared,
        AfterCommit,
        AfterAccepted,
    }

    struct Fixture {
        request: ExecutionRequest,
        files: Vec<File>,
    }

    fn fixture(root: &Path) -> Fixture {
        let input = root.join("input");
        std::fs::write(&input, b"native-input").unwrap();
        let native = |value: &std::ffi::OsStr| NativeString::capture(value);
        Fixture {
            request: ExecutionRequest {
                version: EXECUTION_VERSION,
                entry: ExecutionEntry::Compiler,
                client_version: env!("CARGO_PKG_VERSION").into(),
                client_pid: std::process::id(),
                creation_mask: Some(0o022),
                argv: vec![native("/bin/true".as_ref())],
                env: vec![(
                    native("CLIENT".as_ref()),
                    native("execution-fixture".as_ref()),
                )],
                cwd: native(root.as_os_str()),
                home: None,
                temp: native(root.as_os_str()),
                cache: None,
                executable: native(std::env::current_exe().unwrap().as_os_str()),
                startup_micros: 0,
                channels: ExecutionChannels::Unix {
                    terminal: false,
                    jobserver: vec![],
                },
            },
            files: vec![
                File::open(input).unwrap(),
                File::create(root.join("out")).unwrap(),
                File::create(root.join("err")).unwrap(),
            ],
        }
    }

    fn disconnected_exchange(stage: Disconnect) -> Result<Option<i32>> {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let path = root.path().join("control");
        let listener = UnixListener::bind(&path).unwrap();
        let context = fixture(root.path());
        std::thread::scope(|threads| {
            let server = threads.spawn(|| {
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = std::io::BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let kache_protocol::Request::ExecuteV1(request) =
                    serde_json::from_str(&line).unwrap()
                else {
                    panic!("wrong request: {line}");
                };
                assert_eq!(request.entry, ExecutionEntry::Compiler);
                assert_eq!(request.argv[0].decode().unwrap(), "/bin/true");
                if matches!(stage, Disconnect::BeforeReady) {
                    return;
                }
                if matches!(stage, Disconnect::LegacyDaemon) {
                    write_message(
                        &mut stream,
                        &kache_protocol::Response::err("unknown request"),
                    )
                    .unwrap();
                    return;
                }
                let endpoint = root.path().join("fds");
                let datagram = UnixDatagram::bind(&endpoint).unwrap();
                datagram.set_nonblocking(true).unwrap();
                let ticket = kache_protocol::execution::DescriptorTicket {
                    endpoint: NativeString::capture(endpoint.as_os_str()),
                    key: [29; 16],
                };
                write_message(
                    &mut stream,
                    &ExecutionReply::Ready {
                        version: EXECUTION_VERSION,
                        descriptors: Some(ticket.clone()),
                    },
                )
                .unwrap();
                let started = std::time::Instant::now();
                let files = loop {
                    match crate::descriptors::receive(datagram.as_fd(), &ticket.key, 3) {
                        Ok(files) => break files,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(started.elapsed() < PREPARE_TIMEOUT);
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(error) => panic!("receive failed: {error}"),
                    }
                };
                assert_eq!(files.len(), 3);
                if matches!(stage, Disconnect::BeforePrepared) {
                    return;
                }
                write_message(&mut stream, &ExecutionReply::Prepared).unwrap();
                if matches!(stage, Disconnect::AfterPrepared) {
                    return;
                }
                line.clear();
                reader.read_line(&mut line).unwrap();
                assert_eq!(
                    serde_json::from_str::<ExecutionCommand>(&line).unwrap(),
                    ExecutionCommand::Commit
                );
                if matches!(stage, Disconnect::AfterCommit) {
                    return;
                }
                write_message(&mut stream, &ExecutionReply::Accepted).unwrap();
            });
            let result = try_execute(
                &path,
                &context.request,
                &context.files.iter().map(AsFd::as_fd).collect::<Vec<_>>(),
            );
            server.join().unwrap();
            result
        })
    }

    #[test]
    fn execution_disconnects_before_commit_allow_local_fallback() {
        for stage in [
            Disconnect::BeforeReady,
            Disconnect::LegacyDaemon,
            Disconnect::BeforePrepared,
        ] {
            assert_eq!(disconnected_exchange(stage).unwrap(), None, "{stage:?}");
        }
    }

    #[test]
    fn execution_commit_and_completion_failures_never_allow_local_fallback() {
        for stage in [Disconnect::AfterCommit, Disconnect::AfterAccepted] {
            let error = disconnected_exchange(stage).unwrap_err();
            assert!(
                error.to_string().contains("will not be run again locally"),
                "{stage:?}: {error:#}"
            );
        }
    }

    #[test]
    fn execution_disconnect_after_prepared_cannot_report_completion() {
        // The peer can close before the timeout is cleared (safe fallback),
        // or after the first Commit write is attempted (final failure).
        match disconnected_exchange(Disconnect::AfterPrepared) {
            Ok(None) => {}
            Err(error) => assert!(error.to_string().contains("will not be run again locally")),
            Ok(Some(exit)) => panic!("disconnected peer reported completion: {exit}"),
        }
    }
}

#[cfg(test)]
mod protocol_tests {
    use super::*;

    #[test]
    fn preparation_retries_only_transient_io_and_stops_at_the_deadline() {
        let mut attempts = 0;
        let value = retry_until(
            Some(std::time::Instant::now() + Duration::from_secs(1)),
            || {
                attempts += 1;
                match attempts {
                    1 => Err(std::io::ErrorKind::Interrupted.into()),
                    2 => Err(std::io::ErrorKind::WouldBlock.into()),
                    _ => Ok(42),
                }
            },
        )
        .unwrap();
        assert_eq!(value, 42);
        assert_eq!(attempts, 3);
        for kind in [
            std::io::ErrorKind::WouldBlock,
            std::io::ErrorKind::Interrupted,
        ] {
            let error = retry_until::<()>(Some(std::time::Instant::now()), || Err(kind.into()))
                .unwrap_err();
            assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        }
        let error =
            retry_until::<()>(None, || Err(std::io::ErrorKind::BrokenPipe.into())).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn execution_commit_write_and_flush_failures_are_always_irrevocable() {
        struct BrokenWriter {
            remaining: usize,
        }
        impl Write for BrokenWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if self.remaining == 0 {
                    return Err(std::io::ErrorKind::BrokenPipe.into());
                }
                let accepted = bytes.len().min(self.remaining);
                self.remaining -= accepted;
                Ok(accepted)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
        }
        for remaining in 0..=b"\"commit\"\n".len() {
            let mut committed = false;
            assert!(send_commit(&mut BrokenWriter { remaining }, &mut committed).is_err());
            assert!(
                committed,
                "write failed after {remaining} bytes without ending fallback rights"
            );
        }
    }

    #[test]
    fn execution_reply_frames_are_bounded_and_must_be_complete() {
        assert!(read_reply(&mut &b"{\"execution\":\"prepared\"}\n"[..]).is_ok());
        assert!(read_reply(&mut &b"{\"execution\":\"prepared\"}"[..]).is_err());
        assert!(read_reply(&mut &b""[..]).is_err());
        let large = vec![b'x'; usize::try_from(MAX_REPLY_BYTES).unwrap() + 10];
        let mut source = &large[..];
        assert!(read_reply(&mut source).is_err());
        assert_eq!(source.len(), 10);
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use interprocess::local_socket::ListenerNonblockingMode;
    use kache_protocol::execution::{ExecutionChannels, ExecutionEntry, NativeString};

    #[derive(Clone, Copy)]
    enum Peer {
        Complete,
        BeforePrepared,
        AfterCommit,
        Legacy,
    }

    fn exchange_with(peer: Peer) -> Result<Option<i32>> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("control");
        let listener = crate::ListenerOptions::new()
            .name(crate::socket_name(&path)?)
            .nonblocking(ListenerNonblockingMode::Both)
            .create_sync()?;
        let native = |value: &std::ffi::OsStr| NativeString::capture(value);
        let request = ExecutionRequest {
            version: EXECUTION_VERSION,
            entry: ExecutionEntry::Compiler,
            client_version: "fixture".into(),
            client_pid: std::process::id(),
            creation_mask: None,
            argv: vec![native("compiler".as_ref())],
            env: vec![],
            cwd: native(root.path().as_os_str()),
            home: None,
            temp: native(root.path().as_os_str()),
            cache: None,
            executable: native(std::env::current_exe()?.as_os_str()),
            startup_micros: 0,
            channels: ExecutionChannels::Windows {
                stdin: 0,
                stdout: 0,
                stderr: 0,
            },
        };
        let server = std::thread::spawn(move || -> Result<()> {
            let deadline = std::time::Instant::now() + PREPARE_TIMEOUT;
            let inner = retry_until(Some(deadline), || listener.accept())?;
            let connection = Connection {
                inner,
                deadline: std::cell::Cell::new(Some(deadline)),
            };
            let mut reader = std::io::BufReader::new(&connection);
            let mut line = String::new();
            reader.read_line(&mut line)?;
            ensure!(
                matches!(
                    serde_json::from_str::<kache_protocol::Request>(&line)?,
                    kache_protocol::Request::ExecuteV1(_)
                ),
                "expected execution request"
            );
            if matches!(peer, Peer::Legacy) {
                write_message(&mut &connection, &kache_protocol::Response::ok())?;
                return Ok(());
            }
            write_message(
                &mut &connection,
                &ExecutionReply::Ready {
                    version: EXECUTION_VERSION,
                    descriptors: None,
                },
            )?;
            if matches!(peer, Peer::BeforePrepared) {
                return Ok(());
            }
            // Exercise the client's empty-but-connected named-pipe read.
            std::thread::sleep(Duration::from_millis(4));
            write_message(&mut &connection, &ExecutionReply::Prepared)?;
            line.clear();
            reader.read_line(&mut line)?;
            ensure!(
                serde_json::from_str::<ExecutionCommand>(&line)? == ExecutionCommand::Commit,
                "expected commit"
            );
            if matches!(peer, Peer::AfterCommit) {
                return Ok(());
            }
            write_message(&mut &connection, &ExecutionReply::Accepted)?;
            write_message(
                &mut &connection,
                &ExecutionReply::Complete { exit_code: 19 },
            )?;
            Ok(())
        });
        let result = try_execute(&path, &request);
        server.join().expect("named-pipe peer panicked")?;
        result
    }

    #[test]
    fn named_pipe_preparation_and_completion_preserve_commit_boundary() {
        assert_eq!(exchange_with(Peer::Complete).unwrap(), Some(19));
        for peer in [Peer::BeforePrepared, Peer::Legacy] {
            assert_eq!(exchange_with(peer).unwrap(), None);
        }
        let error = exchange_with(Peer::AfterCommit).unwrap_err();
        assert!(error.to_string().contains("will not be run again locally"));
    }
}
