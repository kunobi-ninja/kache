//! Synchronous compiler handoff with fallback rights ending at the first Commit write.

#[cfg(all(test, windows))]
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
#[cfg(not(windows))]
const MAX_REPLY_BYTES: u64 = 64 << 10;
#[cfg(windows)]
use windows::MAX_REPLY_BYTES;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
mod windows_client;
#[cfg(windows)]
use windows::Connection;
#[cfg(unix)]
mod unix;
#[cfg(unix)]
use unix::Connection;

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
    #[cfg(windows)]
    let mut client = windows_client::Client::default();
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
    #[cfg(windows)]
    stream.commit_written()?;
    ensure!(
        matches!(read_reply(&mut reader)?, ExecutionReply::Accepted),
        "daemon did not acknowledge compiler execution"
    );
    #[cfg(windows)]
    loop {
        match read_reply(&mut reader)? {
            ExecutionReply::Complete { exit_code } => return Ok(exit_code),
            ExecutionReply::Failed { message } => bail!("{message}"),
            ExecutionReply::WindowsControl { command } => {
                write_message(&mut &stream, &client.execute(command))?;
            }
            reply => bail!("invalid compiler completion: {reply:?}"),
        }
    }
    #[cfg(not(windows))]
    match read_reply(&mut reader)? {
        ExecutionReply::Complete { exit_code } => Ok(exit_code),
        ExecutionReply::Failed { message } => bail!("{message}"),
        reply => bail!("invalid compiler completion: {reply:?}"),
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
                std::thread::sleep(Duration::from_millis(1));
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
        listener.set_nonblocking(true).unwrap();
        let context = fixture(root.path());
        std::thread::scope(|threads| {
            let server = threads.spawn(|| {
                let (mut stream, _) =
                    retry_until(Some(std::time::Instant::now() + PREPARE_TIMEOUT), || {
                        listener.accept()
                    })
                    .unwrap();
                stream.set_nonblocking(false).unwrap();
                stream.set_read_timeout(Some(PREPARE_TIMEOUT)).unwrap();
                stream.set_write_timeout(Some(PREPARE_TIMEOUT)).unwrap();
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
                        Err(error) => {
                            assert_eq!(
                                error.kind(),
                                std::io::ErrorKind::WouldBlock,
                                "receive failed: {error}"
                            );
                            assert!(started.elapsed() < PREPARE_TIMEOUT);
                            std::thread::sleep(Duration::from_millis(1));
                        }
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
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _ = send.send(retry_until::<()>(None, || {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }));
        });
        let error = receive
            .recv_timeout(PREPARE_TIMEOUT)
            .expect("non-transient errors must stop retries without a deadline")
            .unwrap_err();
        worker.join().unwrap();
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
    use anyhow::Context as _;
    use interprocess::local_socket::ListenerNonblockingMode;
    use kache_protocol::execution::{ExecutionChannels, ExecutionEntry, NativeString};

    #[derive(Clone, Copy, Debug)]
    enum Peer {
        Complete,
        BeforePrepared,
        AfterCommit,
        Legacy,
    }

    fn request(root: &Path) -> ExecutionRequest {
        let native = |value: &std::ffi::OsStr| NativeString::capture(value);
        ExecutionRequest {
            version: EXECUTION_VERSION,
            entry: ExecutionEntry::Compiler,
            client_version: "fixture".into(),
            client_pid: std::process::id(),
            creation_mask: None,
            argv: vec![native("compiler".as_ref())],
            env: vec![],
            cwd: native(root.as_os_str()),
            home: None,
            temp: native(root.as_os_str()),
            cache: None,
            executable: native(std::env::current_exe().unwrap().as_os_str()),
            startup_micros: 0,
            channels: ExecutionChannels::Windows {
                stdin: 0,
                stdout: 0,
                stderr: 0,
                process_broker: true,
            },
        }
    }

    fn exchange_with(peer: Peer) -> Result<Option<i32>> {
        let root = tempfile::tempdir()?;
        let path = root.path().join("control");
        let listener = crate::ListenerOptions::new()
            .name(crate::socket_name(&path)?)
            .nonblocking(ListenerNonblockingMode::Both)
            .create_sync()?;
        let request = request(root.path());
        let server = std::thread::spawn(move || -> Result<()> {
            let deadline = std::time::Instant::now() + PREPARE_TIMEOUT;
            let inner = retry_until(Some(deadline), || listener.accept())
                .context("accepting the execution client")?;
            let connection = Connection::from_stream(inner, deadline)?;
            let mut reader = std::io::BufReader::new(&connection);
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .context("reading the execution request")?;
            ensure!(
                matches!(
                    serde_json::from_str::<kache_protocol::Request>(&line)?,
                    kache_protocol::Request::ExecuteV1(_)
                ),
                "expected execution request"
            );
            assert_identification_only(&connection)?;
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
            reader
                .read_line(&mut line)
                .context("reading the execution commit")?;
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
        // A complete peer must expose preparation errors, rather than letting
        // the availability fallback hide them behind the server's timeout.
        let result = if matches!(peer, Peer::Complete) {
            exchange(&path, &request, &mut false).map(Some)
        } else {
            try_execute(&path, &request)
        };
        server
            .join()
            .expect("named-pipe peer panicked")
            .with_context(|| format!("peer {peer:?}; client returned {result:?}"))?;
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

    fn assert_identification_only(connection: &Connection) -> Result<()> {
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
        use windows_sys::Win32::Security::{
            GetTokenInformation, SecurityIdentification, TOKEN_QUERY, TokenImpersonationLevel,
        };
        use windows_sys::Win32::System::Threading::{GetCurrentThread, OpenThreadToken};
        let crate::SyncStream::NamedPipe(pipe) = &connection.inner;
        let _impersonation = pipe.inner().impersonate_client()?;
        let mut token = std::ptr::null_mut();
        ensure!(
            unsafe { OpenThreadToken(GetCurrentThread(), TOKEN_QUERY, 1, &mut token) } != 0,
            "{}",
            std::io::Error::last_os_error()
        );
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let mut level = 0i32;
        let mut size = 0u32;
        ensure!(
            unsafe {
                GetTokenInformation(
                    token.as_raw_handle(),
                    TokenImpersonationLevel,
                    (&mut level as *mut i32).cast(),
                    std::mem::size_of::<i32>() as u32,
                    &mut size,
                )
            } != 0,
            "{}",
            std::io::Error::last_os_error()
        );
        ensure!(
            level == SecurityIdentification,
            "the pipe server must not act with the compiler client's token"
        );
        Ok(())
    }

    struct PeerChild(std::process::Child);
    impl Drop for PeerChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn a_weaker_same_user_server_receives_no_request_or_commit() {
        use kunobi_daemon::local::peer::PeerCredentials as _;
        use std::os::windows::io::AsHandle;
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("control");
        let mut child = PeerChild(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "execution::windows_tests::weaker_server_fixture",
                    "--nocapture",
                ])
                .env("KACHE_WEAKER_PEER_ROOT", root.path())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !root.path().join("ready").exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "weaker pipe fixture exited before listening"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "weaker pipe fixture did not start"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!crate::windows_security::matches_daemon(child.0.as_handle()).unwrap());
        // Establish that a same-SID-only check would accept this real peer.
        use interprocess::os::windows::named_pipe::{DuplexPipeStream, pipe_mode::Bytes};
        let pipe = DuplexPipeStream::<Bytes>::connect_by_path_with_wait_mode(
            format!(r"\\.\pipe\{}", crate::daemon_endpoint(&socket)),
            interprocess::ConnectWaitMode::Timeout(PREPARE_TIMEOUT),
        )
        .unwrap();
        let probe: crate::SyncStream =
            interprocess::os::windows::named_pipe::local_socket::Stream::from(pipe).into();
        while !root.path().join("accepted").exists() {
            assert!(child.0.try_wait().unwrap().is_none());
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(probe.credentials().unwrap().same_user);
        assert_eq!(
            probe.credentials().unwrap().pid.unwrap().get(),
            child.0.id()
        );
        drop(probe);

        let mut committed = false;
        let error = exchange(&socket, &request(root.path()), &mut committed).unwrap_err();
        assert!(!committed);
        assert!(
            format!("{error:#}").contains("security context differs"),
            "{error:#}"
        );
        assert_eq!(try_execute(&socket, &request(root.path())).unwrap(), None);
        std::fs::write(root.path().join("done"), b"done").unwrap();
        while child.0.try_wait().unwrap().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "weaker pipe fixture did not finish"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let status = child.0.wait().unwrap();
        let mut diagnostics = String::new();
        if let Some(mut stderr) = child.0.stderr.take() {
            stderr.read_to_string(&mut diagnostics).unwrap();
        }
        assert!(status.success(), "{diagnostics}");
        let received = std::fs::read_to_string(root.path().join("received")).unwrap();
        assert!(
            !received.is_empty(),
            "the credential probe must reach the server"
        );
        assert!(received.split(',').all(|count| count == "0"), "{received}");
    }

    #[test]
    #[ignore = "isolated primary token for named-pipe server admission"]
    fn weaker_server_fixture() {
        // `--include-ignored` must not change the calling test process token.
        let Some(root) = std::env::var_os("KACHE_WEAKER_PEER_ROOT") else {
            return;
        };
        let root = std::path::PathBuf::from(root);
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
        use windows_sys::Win32::Foundation::{GetLastError, LUID, SetLastError};
        use windows_sys::Win32::Security::{
            AdjustTokenPrivileges, LUID_AND_ATTRIBUTES, LookupPrivilegeValueW,
            SE_PRIVILEGE_REMOVED, TOKEN_ADJUST_PRIVILEGES, TOKEN_PRIVILEGES, TOKEN_QUERY,
        };
        use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};
        let mut token = std::ptr::null_mut();
        assert_ne!(
            unsafe {
                OpenProcessToken(
                    GetCurrentProcess(),
                    TOKEN_QUERY | TOKEN_ADJUST_PRIVILEGES,
                    &mut token,
                )
            },
            0
        );
        let token = unsafe { OwnedHandle::from_raw_handle(token) };
        let name = "SeChangeNotifyPrivilege"
            .encode_utf16()
            .chain(Some(0))
            .collect::<Vec<_>>();
        let mut luid: LUID = unsafe { std::mem::zeroed() };
        assert_ne!(
            unsafe { LookupPrivilegeValueW(std::ptr::null(), name.as_ptr(), &mut luid) },
            0
        );
        let privileges = TOKEN_PRIVILEGES {
            PrivilegeCount: 1,
            Privileges: [LUID_AND_ATTRIBUTES {
                Luid: luid,
                Attributes: SE_PRIVILEGE_REMOVED,
            }],
        };
        unsafe {
            SetLastError(0);
        }
        assert_ne!(
            unsafe {
                AdjustTokenPrivileges(
                    token.as_raw_handle(),
                    0,
                    &privileges,
                    0,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
            },
            0
        );
        assert_eq!(
            unsafe { GetLastError() },
            0,
            "the fixture must remove its privilege"
        );
        drop(token);
        let socket = root.join("control");
        let listener = crate::ListenerOptions::new()
            .name(crate::socket_name(&socket).unwrap())
            .nonblocking(ListenerNonblockingMode::Both)
            .create_sync()
            .unwrap();
        std::fs::write(root.join("ready"), b"ready").unwrap();
        let mut received = Vec::new();
        let limit = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            // The listener can discard a client that closes before accept.
            // Count observed bytes, rather than requiring every rejected
            // connection to survive long enough to be accepted in user space.
            let stream = match listener.accept() {
                Ok(stream) => stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if root.join("done").exists() {
                        break;
                    }
                    assert!(std::time::Instant::now() < limit);
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                Err(error) => panic!("accepting the credential probe: {error}"),
            };
            std::fs::write(root.join("accepted"), b"accepted").unwrap();
            let deadline = std::time::Instant::now() + PREPARE_TIMEOUT;
            let mut byte = [0u8; 1];
            let count = retry_until(Some(deadline), || (&stream).read(&mut byte))
                .or_else(|error| {
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::UnexpectedEof
                    ) {
                        Ok(0)
                    } else {
                        Err(error)
                    }
                })
                .unwrap();
            received.push(count.to_string());
        }
        std::fs::write(root.join("received"), received.join(",")).unwrap();
    }
}
