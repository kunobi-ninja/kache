//! Compiler tasks reuse the daemon process. Preparation owns every resource
//! before Commit, and never starts compiler work.

use super::*;
use crate::invocation::{
    InvocationContext, InvocationEnv, io::InvocationIo, process::Cancellation,
};
use kache_protocol::execution::{
    DescriptorTicket, EXECUTION_VERSION, ExecutionChannels, ExecutionCommand, ExecutionEntry,
    ExecutionReply, ExecutionRequest, NativeString,
};
use std::ffi::OsString;
use std::fs::File;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixDatagram;
use tokio::io::AsyncReadExt;

const PREPARE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(any(test, target_os = "linux"))]
#[path = "daemon_execution_linux.rs"]
mod linux;

#[cfg(target_os = "macos")]
#[path = "daemon_execution_macos.rs"]
mod macos;

fn peer_may_delegate(stream: &TokioStream, reported_pid: u32) -> bool {
    #[cfg(target_os = "macos")]
    {
        macos::peer_may_delegate(stream, reported_pid)
    }
    #[cfg(target_os = "linux")]
    {
        linux::peer_may_delegate(
            Path::new("/proc"),
            crate::transport::peer_pid(stream),
            reported_pid,
        )
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = (stream, reported_pid);
        false
    }
}

struct Prepared {
    entry: ExecutionEntry,
    args: Vec<String>,
    env: InvocationEnv,
    executable: PathBuf,
    terminal: bool,
    originals: Vec<i32>,
    started_at: Instant,
}

impl Prepared {
    fn decode(request: ExecutionRequest, version: &str) -> Result<Self> {
        anyhow::ensure!(
            request.version == EXECUTION_VERSION,
            "unsupported execution protocol"
        );
        anyhow::ensure!(
            request.client_version == version,
            "compiler client and daemon releases differ"
        );
        anyhow::ensure!(
            request.creation_mask.is_some()
                && request.creation_mask == kache_client::creation_mask(),
            "compiler file creation policy requires local execution"
        );
        let ExecutionChannels::Unix {
            terminal,
            jobserver,
        } = request.channels
        else {
            anyhow::bail!("compiler channels belong to another platform");
        };
        let vars = request
            .env
            .iter()
            .map(|(key, value)| Ok((key.decode()?, value.decode()?)))
            .collect::<Result<Vec<(OsString, OsString)>>>()?;
        anyhow::ensure!(
            vars.iter()
                .all(|(key, _)| !key.is_empty() && !key.as_bytes().contains(&b'=')),
            "invalid compiler environment key"
        );
        anyhow::ensure!(
            jobserver == kache_ipc::jobserver::original_fds(&vars)?,
            "jobserver manifest does not match compiler environment"
        );
        let mut env = InvocationEnv::from_parts(
            vars,
            Some(PathBuf::from(request.cwd.decode()?)),
            request
                .home
                .as_ref()
                .map(NativeString::decode)
                .transpose()?
                .map(PathBuf::from),
            PathBuf::from(request.temp.decode()?),
        );
        env = env.with_cache_dir(
            request
                .cache
                .as_ref()
                .map(NativeString::decode)
                .transpose()?
                .map(PathBuf::from),
        );
        env.validate()?;
        anyhow::ensure!(
            env.home_dir().is_none_or(Path::is_absolute)
                && env.cache_dir().is_none_or(Path::is_absolute),
            "compiler user directories must be absolute"
        );
        anyhow::ensure!(
            env.cwd().is_some_and(Path::is_dir),
            "compiler working directory is unavailable"
        );
        let executable = PathBuf::from(request.executable.decode()?);
        anyhow::ensure!(
            executable.is_absolute(),
            "compiler client executable must be absolute"
        );
        let args = request
            .argv
            .iter()
            .map(|arg| {
                arg.decode()?
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("compiler arguments are not UTF-8"))
            })
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(!args.is_empty(), "compiler argv is empty");
        if request.entry == ExecutionEntry::Original {
            anyhow::ensure!(
                crate::original_compiler_request(&args, &env),
                "original entry is not a compiler invocation"
            );
        }
        let started_at = Instant::now()
            .checked_sub(Duration::from_micros(request.startup_micros))
            .context("invalid compiler startup duration")?;
        Ok(Self {
            entry: request.entry,
            args,
            env,
            executable,
            terminal,
            originals: jobserver,
            started_at,
        })
    }

    fn receive(
        self,
        mut descriptors: Vec<OwnedFd>,
        cancellation: Cancellation,
        daemon: Arc<Daemon>,
    ) -> Result<(ExecutionEntry, PathBuf, Vec<String>, InvocationContext)> {
        let jobserver = kache_ipc::jobserver::Descriptors::new(
            descriptors.split_off(3 + usize::from(self.terminal)),
        )?;
        let vars = kache_ipc::jobserver::rewrite(
            &self.env.vars_os().collect::<Vec<_>>(),
            &self.originals,
            &jobserver,
        )?;
        let mut env = self.env;
        for (key, value) in vars {
            if ["CARGO_MAKEFLAGS", "MAKEFLAGS", "MFLAGS"]
                .iter()
                .any(|name| key == std::ffi::OsStr::new(name))
            {
                env.set_os(&key, value);
            }
        }
        let mut files = descriptors.into_iter().map(File::from);
        let io = InvocationIo::from_files(
            files.next().context("missing compiler stdin")?,
            files.next().context("missing compiler stdout")?,
            files.next().context("missing compiler stderr")?,
            files.next(),
        );
        Ok((
            self.entry,
            self.executable.clone(),
            self.args,
            InvocationContext {
                daemon: Some(daemon),
                env,
                started_at: self.started_at,
                shim_stem: self
                    .executable
                    .file_stem()
                    .context("missing compiler client name")?
                    .to_string_lossy()
                    .into_owned(),
                io,
                cancellation: Some(cancellation),
                jobserver,
            },
        ))
    }
}

struct DescriptorChannel {
    socket: tokio::io::unix::AsyncFd<UnixDatagram>,
    ticket: DescriptorTicket,
    _directory: tempfile::TempDir,
}

impl DescriptorChannel {
    fn new() -> Result<Self> {
        // A private short directory also works when the main endpoint is near
        // the native sockaddr_un limit. The control peer authenticates this path.
        let directory = tempfile::Builder::new()
            .prefix("kache-exec-")
            .tempdir_in("/tmp")?;
        let path = directory.path().join("fds");
        let socket = UnixDatagram::bind(&path)?;
        socket.set_nonblocking(true)?;
        let mut key = [0; 16];
        getrandom::fill(&mut key).context("creating descriptor ticket")?;
        Ok(Self {
            socket: tokio::io::unix::AsyncFd::new(socket)?,
            ticket: DescriptorTicket {
                endpoint: NativeString::capture(path.as_os_str()),
                key,
            },
            _directory: directory,
        })
    }

    async fn receive(&self, expected: usize) -> Result<Vec<OwnedFd>> {
        loop {
            let mut ready = self.socket.readable().await?;
            match ready.try_io(|socket| {
                kache_ipc::descriptors::receive(
                    socket.get_ref().as_fd(),
                    &self.ticket.key,
                    expected,
                )
            }) {
                Ok(result) => return Ok(result?),
                Err(_) => continue,
            }
        }
    }
}

/// Cancels even when a forced drain aborts the async connection task. The
/// blocking worker retains the token until it has reaped its compiler children.
struct CancelOnDrop(Cancellation);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn write_reply(stream: &TokioStream, reply: &ExecutionReply) -> Result<()> {
    let mut frame = serde_json::to_vec(reply)?;
    frame.push(b'\n');
    let mut writer = stream;
    tokio::time::timeout(PREPARE_TIMEOUT, writer.write_all(&frame)).await??;
    Ok(())
}

pub(super) async fn handle(
    stream: &TokioStream,
    reader: &mut BufReader<&TokioStream>,
    request: ExecutionRequest,
    daemon: &Arc<Daemon>,
    lifecycle: &Arc<Lifecycle>,
) -> Result<()> {
    let client_pid = request.client_pid;
    if !peer_may_delegate(stream, client_pid) {
        return write_reply(
            stream,
            &ExecutionReply::Declined {
                reason: "compiler process isolation requires local execution".into(),
            },
        )
        .await;
    }
    let prepared = match Prepared::decode(request, &daemon.version) {
        Ok(prepared) => prepared,
        Err(error) => {
            return write_reply(
                stream,
                &ExecutionReply::Declined {
                    reason: error.to_string(),
                },
            )
            .await;
        }
    };
    let permit = match daemon.execution_workers.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return write_reply(
                stream,
                &ExecutionReply::Declined {
                    reason: "compiler workers are busy".into(),
                },
            )
            .await;
        }
    };
    let channel = match DescriptorChannel::new() {
        Ok(channel) => channel,
        Err(error) => {
            return write_reply(
                stream,
                &ExecutionReply::Declined {
                    reason: error.to_string(),
                },
            )
            .await;
        }
    };
    write_reply(
        stream,
        &ExecutionReply::Ready {
            version: EXECUTION_VERSION,
            descriptors: Some(channel.ticket.clone()),
        },
    )
    .await?;
    let descriptors = tokio::select! {
        _ = lifecycle.draining() => return Ok(()),
        incoming = reader.fill_buf() => {
            let _ = incoming?;
            anyhow::bail!("compiler client disconnected or wrote before preparation");
        }
        result = tokio::time::timeout(PREPARE_TIMEOUT, channel.receive(3 + usize::from(prepared.terminal) + prepared.originals.len())) => result??,
    };
    drop(channel);
    let cancellation = Cancellation::default();
    let _cancel = CancelOnDrop(cancellation.clone());
    let (entry, executable, args, context) =
        prepared.receive(descriptors, cancellation.clone(), Arc::clone(daemon))?;
    write_reply(stream, &ExecutionReply::Prepared).await?;
    let mut frame = Vec::new();
    let commit = tokio::select! {
        _ = lifecycle.draining() => return Ok(()),
        result = tokio::time::timeout(PREPARE_TIMEOUT, read_bounded_line(reader, &mut frame)) => result??,
    }.context("compiler client disconnected before Commit")?;
    let _: ExecutionCommand = serde_json::from_str(&commit)?;
    anyhow::ensure!(
        peer_may_delegate(stream, client_pid),
        "compiler process isolation changed before execution"
    );
    write_reply(stream, &ExecutionReply::Accepted).await?;
    let activity = daemon.request_clock.begin_activity();

    let mut worker = tokio::task::spawn_blocking(move || {
        let _activity = activity;
        let _permit = permit;
        match entry {
            ExecutionEntry::Compiler => crate::run_compiler_invocation(&args, context),
            ExecutionEntry::Original => {
                crate::run_original_compiler_invocation(&args, context, &executable)
            }
        }
    });
    let mut unexpected = [0; 1];
    let result = tokio::select! {
        result = &mut worker => result,
        disconnected = reader.read(&mut unexpected) => {
            let _ = disconnected;
            cancellation.cancel();
            let _ = worker.await;
            return Ok(());
        }
    };
    let reply = match result {
        Ok(Ok(exit_code)) => ExecutionReply::Complete { exit_code },
        Ok(Err(error)) => ExecutionReply::Failed {
            message: format!("{error:#}"),
        },
        Err(error) => ExecutionReply::Failed {
            message: format!("compiler worker failed: {error}"),
        },
    };
    write_reply(stream, &reply).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn native_execution_available() -> bool {
        #[cfg(target_os = "linux")]
        if !linux::permits_delegation(Path::new("/proc"), std::process::id()).unwrap_or(false) {
            eprintln!(
                "native daemon execution unavailable inside this test sandbox; local compilation remains enabled"
            );
            return false;
        }
        true
    }

    fn fixture(root: &Path, name: &str) -> (Vec<String>, InvocationContext) {
        let root = root.join(name);
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let program = root.join("rustc");
        std::fs::write(&program, b"#!/bin/sh\nprintf '%s' \"$$\" > \"$PIDFILE\"\nprintf '%s|%s\\n' \"$CLIENT\" \"$PWD\" >> \"$RECORD\"\nprintf 'metadata-%s\\n' \"$CLIENT\"\nprintf 'diagnostic-%s\\n' \"$CLIENT\" >&2\nprintf ready > \"$READY\"\ni=0\nwhile [ ! -e \"$RELEASE\" ] && [ $i -lt 400 ]; do /bin/sleep .01; i=$((i+1)); done\nexit 7\n").unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(root.join("config.toml"), "").unwrap();
        std::fs::write(root.join("stdin"), b"input\n").unwrap();
        let env = InvocationEnv::from_parts(
            [
                ("KACHE_CONFIG", root.join("config.toml").into_os_string()),
                ("KACHE_CACHE_DIR", root.join("cache").into_os_string()),
                ("KACHE_RUNTIME_DIR", root.join("runtime").into_os_string()),
                ("KACHE_DISABLED", "1".into()),
                ("PATH", "/usr/bin:/bin".into()),
                ("CLIENT", name.into()),
                ("RECORD", root.join("record").into_os_string()),
                ("PIDFILE", root.join("pid").into_os_string()),
                ("READY", root.join("ready").into_os_string()),
                ("RELEASE", root.join("release").into_os_string()),
            ],
            Some(root.clone()),
            Some(root.clone()),
            root.clone(),
        );
        let io = InvocationIo::from_files(
            File::open(root.join("stdin")).unwrap(),
            File::create(root.join("stdout")).unwrap(),
            File::create(root.join("stderr")).unwrap(),
            None,
        );
        (
            vec![program.to_str().unwrap().into()],
            InvocationContext {
                daemon: None,
                env,
                io,
                started_at: Instant::now(),
                shim_stem: "kache".into(),
                cancellation: None,
                jobserver: Default::default(),
            },
        )
    }

    async fn ready(path: &Path) {
        let start = Instant::now();
        while !path.exists() {
            assert!(
                start.elapsed() < Duration::from_secs(3),
                "compiler did not become ready: {}",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn server(
        root: &Path,
        connections: usize,
        workers: usize,
    ) -> (
        PathBuf,
        Arc<Daemon>,
        Arc<Lifecycle>,
        tokio::task::JoinHandle<Vec<Result<()>>>,
    ) {
        let config = super::super::tests::test_config(root);
        let socket = config.socket_path();
        let listener = crate::transport::bind_daemon_listener(&socket)
            .await
            .unwrap()
            .unwrap();
        let mut daemon = Daemon::new(config);
        daemon.execution_workers = Arc::new(tokio::sync::Semaphore::new(workers));
        daemon.request_clock = Arc::new(crate::maintenance::RequestClock::idle());
        let daemon = Arc::new(daemon);
        let lifecycle = Arc::new(Lifecycle::default());
        let d = daemon.clone();
        let l = lifecycle.clone();
        let task = tokio::spawn(async move {
            let mut handlers = tokio::task::JoinSet::new();
            for _ in 0..connections {
                let stream = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                    .await
                    .expect("execution client did not connect")
                    .unwrap();
                let d = d.clone();
                let l = l.clone();
                handlers.spawn(async move {
                    tokio::time::timeout(
                        Duration::from_secs(30),
                        super::super::handle_connection(stream, &d, &l),
                    )
                    .await
                    .expect("execution request did not finish")
                });
            }
            let mut results = Vec::new();
            while let Some(result) = handlers.join_next().await {
                results.push(result.unwrap());
            }
            results
        });
        (socket, daemon, lifecycle, task)
    }

    #[tokio::test]
    async fn execution_handoff_streams_two_compilers_without_serializing_or_mixing_clients() {
        if !native_execution_available() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (socket, daemon, _, server) = server(root.path(), 2, 2).await;
        let mut clients = Vec::new();
        let mut paths = Vec::new();
        for name in ["left", "right"] {
            let (args, context) = fixture(root.path(), name);
            paths.push(context.env.cwd().unwrap().to_owned());
            let socket = socket.clone();
            clients.push(tokio::task::spawn_blocking(move || {
                if name == "left" {
                    let (files, terminal) = context.io.transfer_files()?;
                    let args = std::iter::once(std::ffi::OsString::from("kache"))
                        .chain(args.iter().map(std::ffi::OsString::from))
                        .collect::<Vec<_>>();
                    kache_client::Execution {
                        entry: ExecutionEntry::Original,
                        args: &args,
                        env: &context.env,
                        started_at: context.started_at,
                        executable: &std::env::current_exe()?,
                        files: &files,
                        terminal,
                        client_version: crate::VERSION,
                    }
                    .try_execute(&socket)
                } else {
                    crate::invocation::execution::try_execute(&socket, &args, &context)
                }
            }));
        }
        for (name, path) in ["left", "right"].into_iter().zip(&paths) {
            ready(&path.join("ready")).await;
            assert_eq!(
                std::fs::read_to_string(path.join("stdout")).unwrap(),
                format!("metadata-{name}\n")
            );
            assert_eq!(
                std::fs::read_to_string(path.join("stderr")).unwrap(),
                format!("diagnostic-{name}\n")
            );
        }
        assert!(
            clients.iter().all(|client| !client.is_finished()),
            "metadata must stream before either compiler exits"
        );
        for path in &paths {
            std::fs::write(path.join("release"), b"go").unwrap();
        }
        for client in clients {
            assert_eq!(client.await.unwrap().unwrap(), Some(7));
        }
        for (name, path) in ["left", "right"].into_iter().zip(&paths) {
            assert_eq!(
                std::fs::read_to_string(path.join("record")).unwrap(),
                format!("{name}|{}\n", path.display())
            );
        }
        assert!(server.await.unwrap().iter().all(Result::is_ok));
        assert_eq!(daemon.execution_workers.available_permits(), 2);
    }

    #[tokio::test]
    async fn execution_keeps_a_silent_compiler_alive_past_the_daemon_idle_timeout() {
        if !native_execution_available() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let config = super::super::tests::test_config(root.path());
        let socket = config.socket_path();
        let listener = crate::transport::bind_daemon_listener(&socket)
            .await
            .unwrap()
            .unwrap();
        let daemon = Arc::new(Daemon::new(config));
        let initial_permits = daemon.execution_workers.available_permits();
        let lifecycle = Arc::new(Lifecycle::default());
        let server_daemon = daemon.clone();
        let server = tokio::spawn(async move {
            super::super::accept_loop(
                &listener,
                &server_daemon,
                &lifecycle,
                Some(Duration::from_millis(300)),
                Duration::from_millis(100),
                std::future::pending::<()>(),
            )
            .await
        });
        let (args, context) = fixture(root.path(), "silent");
        let path = context.env.cwd().unwrap().to_owned();
        let client = tokio::task::spawn_blocking(move || {
            crate::invocation::execution::try_execute(&socket, &args, &context)
        });
        ready(&path.join("ready")).await;
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(
            !client.is_finished(),
            "idle expiry interrupted an active compiler"
        );
        assert!(
            !server.is_finished(),
            "daemon exited while compiler was active"
        );
        std::fs::write(path.join("release"), b"go").unwrap();
        assert_eq!(client.await.unwrap().unwrap(), Some(7));
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            daemon.execution_workers.available_permits(),
            initial_permits
        );
    }

    fn request(args: &[String], context: &InvocationContext) -> ExecutionRequest {
        let native = |value: &std::ffi::OsStr| NativeString::capture(value);
        ExecutionRequest {
            version: EXECUTION_VERSION,
            entry: ExecutionEntry::Compiler,
            client_version: crate::VERSION.into(),
            client_pid: std::process::id(),
            creation_mask: kache_client::creation_mask(),
            argv: args.iter().map(|arg| native(arg.as_ref())).collect(),
            env: context
                .env
                .vars_os()
                .map(|(key, value)| (native(&key), native(&value)))
                .collect(),
            cwd: native(context.env.cwd().unwrap().as_os_str()),
            home: None,
            temp: native(context.env.temp_dir().as_os_str()),
            cache: None,
            executable: native(std::env::current_exe().unwrap().as_os_str()),
            startup_micros: 0,
            channels: ExecutionChannels::Unix {
                terminal: false,
                jobserver: vec![],
            },
        }
    }

    #[tokio::test]
    async fn preparation_keeps_terminal_jobserver_and_environment_in_their_own_slots() {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::MetadataExt;
        use std::os::unix::net::UnixStream;

        let root = tempfile::tempdir().unwrap();
        let daemon = Arc::new(Daemon::new_with_provenance(
            super::super::tests::test_config(root.path()),
            &crate::config::ConfigFileProvenance {
                path: root.path().join("config.toml"),
                fingerprint: "fixture".to_owned(),
            },
        ));
        for terminal in [false, true] {
            for with_jobserver in [false, true] {
                let name = format!("slots-{terminal}-{with_jobserver}");
                let (args, mut context) = fixture(root.path(), &name);
                let (read, write) = UnixStream::pair().unwrap();
                let mut jobs = Vec::new();
                if with_jobserver {
                    context.env.set(
                        "CARGO_MAKEFLAGS",
                        format!(
                            "-j --jobserver-auth={},{}",
                            read.as_raw_fd(),
                            write.as_raw_fd()
                        ),
                    );
                    jobs = vec![
                        (read.as_raw_fd(), OwnedFd::from(read.try_clone().unwrap())),
                        (write.as_raw_fd(), OwnedFd::from(write.try_clone().unwrap())),
                    ];
                    jobs.sort_by_key(|(original, _)| *original);
                }
                let mut request = request(&args, &context);
                request.env.extend([
                    (
                        NativeString::Unix(b"DUPLICATE".to_vec()),
                        NativeString::Unix(b"first".to_vec()),
                    ),
                    (
                        NativeString::Unix(b"DUPLICATE".to_vec()),
                        NativeString::Unix(b"second".to_vec()),
                    ),
                    (
                        NativeString::Unix(b"NATIVE_VALUE".to_vec()),
                        NativeString::Unix(vec![255]),
                    ),
                ]);
                request.channels = ExecutionChannels::Unix {
                    terminal,
                    jobserver: jobs.iter().map(|(original, _)| *original).collect(),
                };
                let prepared = Prepared::decode(request, crate::VERSION).unwrap();
                let directory = context.env.cwd().unwrap();
                let paths =
                    ["input", "output", "error", "terminal"].map(|name| directory.join(name));
                let count = if terminal { 4 } else { 3 };
                let mut descriptors = paths[..count]
                    .iter()
                    .map(|path| OwnedFd::from(File::create(path).unwrap()))
                    .collect::<Vec<_>>();
                let job_numbers = jobs
                    .iter()
                    .map(|(original, received)| (*original, received.as_raw_fd()))
                    .collect::<std::collections::BTreeMap<_, _>>();
                descriptors.extend(jobs.into_iter().map(|(_, file)| file));
                let (_, _, _, received) = prepared
                    .receive(descriptors, Cancellation::default(), daemon.clone())
                    .unwrap();
                let (files, has_terminal) = received.io.transfer_files().unwrap();
                assert_eq!(has_terminal, terminal);
                assert_eq!(files.len(), count);
                for (file, path) in files.iter().zip(&paths) {
                    let actual = file.metadata().unwrap();
                    let expected = std::fs::metadata(path).unwrap();
                    assert_eq!(
                        (actual.dev(), actual.ino()),
                        (expected.dev(), expected.ino())
                    );
                }
                assert_eq!(
                    received.jobserver.files().len(),
                    if with_jobserver { 2 } else { 0 }
                );
                if with_jobserver {
                    assert_eq!(
                        received.env.var("CARGO_MAKEFLAGS").unwrap(),
                        format!(
                            "-j --jobserver-auth={},{}",
                            job_numbers[&read.as_raw_fd()],
                            job_numbers[&write.as_raw_fd()]
                        )
                    );
                }
                assert_eq!(
                    received
                        .env
                        .vars_os()
                        .filter(|(key, _)| key == "DUPLICATE")
                        .map(|(_, value)| value)
                        .collect::<Vec<_>>(),
                    vec![OsString::from("first"), OsString::from("second")]
                );
                assert_eq!(
                    received.env.var_os("NATIVE_VALUE").unwrap().as_bytes(),
                    &[255]
                );
                {
                    let _io = crate::invocation::io::InvocationIoScope::enter(received.io.clone());
                    crate::invocation::io::stdout()
                        .write_all(b"stdout")
                        .unwrap();
                    crate::invocation::io::stderr()
                        .write_all(b"stderr")
                        .unwrap();
                    let writer = crate::invocation::io::terminal();
                    if terminal {
                        writer.unwrap().write_all(b"terminal").unwrap();
                    } else {
                        assert!(writer.is_none());
                    }
                }
                assert_eq!(std::fs::read(&paths[0]).unwrap(), b"");
                assert_eq!(std::fs::read(&paths[1]).unwrap(), b"stdout");
                assert_eq!(std::fs::read(&paths[2]).unwrap(), b"stderr");
                if terminal {
                    assert_eq!(std::fs::read(&paths[3]).unwrap(), b"terminal");
                }
            }
        }
    }

    #[tokio::test]
    async fn aborting_the_connection_task_cancels_only_its_compiler_request() {
        use crate::invocation::process::{CancellationScope, CommandExt};

        let cancellation = Cancellation::default();
        let worker_token = cancellation.clone();
        let (ready, waiting) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = CancelOnDrop(worker_token);
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        tokio::time::timeout(Duration::from_secs(3), waiting)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        {
            let _scope = CancellationScope::borrow(Some(cancellation));
            assert_eq!(
                std::process::Command::new("missing-cancelled-compiler")
                    .invocation_spawn()
                    .err()
                    .unwrap()
                    .kind(),
                std::io::ErrorKind::Interrupted
            );
        }
        let _other = CancellationScope::borrow(Some(Cancellation::default()));
        crate::invocation::process::check().unwrap();
    }

    #[tokio::test]
    async fn only_accepted_execution_marks_build_activity() {
        if !native_execution_available() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (socket, daemon, _, server) = server(root.path(), 2, 1).await;
        let (_, context) = fixture(root.path(), "activity");
        let rejected_socket = socket.clone();
        let idle = tokio::task::spawn_blocking(move || {
            let (files, terminal) = context.io.transfer_files()?;
            kache_client::Execution {
                entry: ExecutionEntry::Original,
                args: &["kache".into(), "stats".into()],
                env: &context.env,
                started_at: context.started_at,
                executable: &std::env::current_exe()?,
                files: &files,
                terminal,
                client_version: crate::VERSION,
            }
            .try_execute(&rejected_socket)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(idle, None);
        assert!(daemon.request_clock.idle_for() >= crate::maintenance::QUIET_AFTER);
        let (args2, context2) = fixture(root.path(), "accepted-activity");
        std::fs::write(context2.env.cwd().unwrap().join("release"), "go").unwrap();
        let exit = tokio::task::spawn_blocking(move || {
            crate::invocation::execution::try_execute(&socket, &args2, &context2)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(exit, Some(7));
        assert!(daemon.request_clock.idle_for() < crate::maintenance::QUIET_AFTER);
        for result in server.await.unwrap() {
            result.unwrap();
        }
    }

    #[test]
    fn original_entry_declines_cli_and_unknown_programs_without_probing_them() {
        let root = tempfile::tempdir().unwrap();
        let (args, context) = fixture(root.path(), "raw-validation");
        let mut valid = request(&args, &context);
        valid.entry = ExecutionEntry::Original;
        valid
            .argv
            .insert(0, NativeString::capture("kache".as_ref()));
        assert!(Prepared::decode(valid.clone(), crate::VERSION).is_ok());
        let unknown = root.path().join("unknown-compiler");
        let marker = root.path().join("probe-ran");
        std::fs::write(
            &unknown,
            format!("#!/bin/sh\ntouch '{}'\n", marker.display()),
        )
        .unwrap();
        std::fs::set_permissions(&unknown, std::fs::Permissions::from_mode(0o755)).unwrap();
        for args in [
            vec!["kache".to_owned(), "stats".to_owned()],
            vec!["kache".to_owned(), unknown.to_str().unwrap().to_owned()],
        ] {
            let mut invalid = valid.clone();
            invalid.argv = args
                .iter()
                .map(|arg| NativeString::capture(arg.as_ref()))
                .collect();
            assert!(Prepared::decode(invalid, crate::VERSION).is_err());
        }
        assert!(!marker.exists(), "preparation executed an unknown program");
        for name in [
            "KACHE_FAMILY_PROBE_ACTIVE",
            crate::build_script::SHIM_PATH_ENV,
        ] {
            let mut invalid = valid.clone();
            invalid.env.push((
                NativeString::capture(name.as_ref()),
                NativeString::capture("1".as_ref()),
            ));
            assert!(Prepared::decode(invalid, crate::VERSION).is_err(), "{name}");
        }
    }

    #[tokio::test]
    async fn original_compiler_shim_uses_the_clients_relative_path() {
        if !native_execution_available() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (socket, _, _, server) = server(root.path(), 1, 1).await;
        let (args, mut context) = fixture(root.path(), "shim");
        let cwd = context.env.cwd().unwrap().to_owned();
        for dir in ["shims", "tools"] {
            std::fs::create_dir(cwd.join(dir)).unwrap();
        }
        std::fs::write(cwd.join("shims/.kache-shims"), "").unwrap();
        std::os::unix::fs::symlink(std::env::current_exe().unwrap(), cwd.join("shims/cc")).unwrap();
        std::fs::copy(&args[0], cwd.join("tools/cc")).unwrap();
        context.env.set("PATH", "shims:tools:/usr/bin:/bin");
        std::fs::write(cwd.join("release"), "go").unwrap();
        let exit = tokio::task::spawn_blocking(move || {
            let (files, terminal) = context.io.transfer_files()?;
            kache_client::Execution {
                entry: ExecutionEntry::Original,
                args: &["cc".into(), "--version".into()],
                env: &context.env,
                started_at: context.started_at,
                executable: &std::env::current_exe()?,
                files: &files,
                terminal,
                client_version: crate::VERSION,
            }
            .try_execute(&socket)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(exit, Some(7));
        for result in server.await.unwrap() {
            result.unwrap();
        }
        assert_eq!(
            std::fs::read_to_string(cwd.join("record")).unwrap(),
            format!("shim|{}\n", cwd.display())
        );
        assert_eq!(
            std::fs::read_to_string(cwd.join("stdout")).unwrap(),
            "metadata-shim\n"
        );
        assert_eq!(
            std::fs::read_to_string(cwd.join("stderr")).unwrap(),
            "diagnostic-shim\n"
        );
    }

    #[test]
    fn execution_preparation_rejects_incompatible_and_malformed_native_inputs() {
        let root = tempfile::tempdir().unwrap();
        let (args, context) = fixture(root.path(), "validation");
        let valid = request(&args, &context);
        assert!(Prepared::decode(valid.clone(), crate::VERSION).is_ok());
        type InvalidCase = (&'static str, fn(&mut ExecutionRequest));
        let cases: [InvalidCase; 13] = [
            ("protocol", |r| r.version += 1),
            ("missing creation policy", |r| r.creation_mask = None),
            ("different creation policy", |r| {
                r.creation_mask = r.creation_mask.map(|mask| mask ^ 0o077)
            }),
            ("release", |r| r.client_version = "different-release".into()),
            ("entry", |r| {
                r.entry = ExecutionEntry::Original;
                r.argv = ["kache", "stats"]
                    .into_iter()
                    .map(|arg| NativeString::capture(arg.as_ref()))
                    .collect();
            }),
            ("channels", |r| {
                r.channels = ExecutionChannels::Windows {
                    stdin: 1,
                    stdout: 2,
                    stderr: 3,
                    process_broker: false,
                }
            }),
            ("argv", |r| r.argv.clear()),
            ("argument encoding", |r| {
                r.argv[0] = NativeString::Unix(vec![255])
            }),
            ("environment key", |r| {
                r.env[0].0 = NativeString::Unix(b"invalid=name".to_vec())
            }),
            ("environment NUL", |r| {
                r.env[0].1 = NativeString::Unix(vec![0])
            }),
            ("working directory", |r| {
                r.cwd = NativeString::Unix(b"relative".to_vec())
            }),
            ("cache directory", |r| {
                r.cache = Some(NativeString::Unix(b"relative".to_vec()))
            }),
            ("executable", |r| {
                r.executable = NativeString::Unix(b"relative".to_vec())
            }),
        ];
        for (name, mutate) in cases {
            let mut request = valid.clone();
            mutate(&mut request);
            assert!(Prepared::decode(request, crate::VERSION).is_err(), "{name}");
        }
        let mut invalid_jobs = valid.clone();
        invalid_jobs.channels = ExecutionChannels::Unix {
            terminal: false,
            jobserver: vec![999],
        };
        assert!(Prepared::decode(invalid_jobs, crate::VERSION).is_err());
        let mut native_value = valid;
        native_value.env.push((
            NativeString::Unix(b"NATIVE_VALUE".to_vec()),
            NativeString::Unix(vec![255]),
        ));
        assert!(Prepared::decode(native_value, crate::VERSION).is_ok());
    }

    async fn prepare_client(
        socket: &Path,
        request: ExecutionRequest,
        context: &InvocationContext,
    ) -> (TokioStream, DescriptorTicket) {
        let stream = TokioStream::connect(crate::transport::socket_name(socket).unwrap())
            .await
            .unwrap();
        let mut frame = serde_json::to_vec(&Request::ExecuteV1(Box::new(request))).unwrap();
        frame.push(b'\n');
        (&stream).write_all(&frame).await.unwrap();
        let mut reader = BufReader::new(&stream);
        let line = read_bounded_line(&mut reader, &mut Vec::new())
            .await
            .unwrap()
            .unwrap();
        let ExecutionReply::Ready {
            descriptors: Some(ticket),
            ..
        } = serde_json::from_str(&line).unwrap()
        else {
            panic!("server not ready: {line}");
        };
        let files = context.io.transfer_files().unwrap().0;
        let socket = UnixDatagram::unbound().unwrap();
        socket
            .connect(PathBuf::from(ticket.endpoint.decode().unwrap()))
            .unwrap();
        kache_ipc::descriptors::send(
            socket.as_fd(),
            &ticket.key,
            &files.iter().map(AsFd::as_fd).collect::<Vec<_>>(),
        )
        .unwrap();
        let line = read_bounded_line(&mut reader, &mut Vec::new())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            serde_json::from_str(&line).unwrap(),
            ExecutionReply::Prepared
        ));
        drop(reader);
        (stream, ticket)
    }

    #[tokio::test]
    async fn prepared_compiler_never_runs_without_commit_and_releases_its_resources() {
        if !native_execution_available() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (socket, daemon, _, server) = server(root.path(), 1, 1).await;
        let (args, context) = fixture(root.path(), "uncommitted");
        let path = context.env.cwd().unwrap().to_owned();
        let (stream, ticket) = prepare_client(&socket, request(&args, &context), &context).await;
        assert!(!path.join("record").exists());
        assert_eq!(daemon.execution_workers.available_permits(), 0);
        assert!(
            !PathBuf::from(ticket.endpoint.decode().unwrap()).exists(),
            "one-use endpoint must already be removed"
        );
        drop(stream);
        assert!(server.await.unwrap()[0].is_err());
        assert!(!path.join("record").exists());
        assert_eq!(daemon.execution_workers.available_permits(), 1);
    }

    #[tokio::test]
    async fn draining_before_commit_releases_worker_without_starting_a_compiler() {
        if !native_execution_available() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (socket, daemon, lifecycle, server) = server(root.path(), 1, 1).await;
        let (args, context) = fixture(root.path(), "draining");
        let path = context.env.cwd().unwrap().to_owned();
        let (stream, _) = prepare_client(&socket, request(&args, &context), &context).await;
        lifecycle.start_drain();
        assert!(server.await.unwrap()[0].is_ok());
        drop(stream);
        assert!(!path.join("record").exists());
        assert_eq!(daemon.execution_workers.available_permits(), 1);
    }

    #[tokio::test]
    async fn disconnected_accepted_compiler_is_cancelled_and_reaped() {
        if !native_execution_available() {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let (socket, daemon, _, server) = server(root.path(), 1, 1).await;
        let (args, context) = fixture(root.path(), "disconnected");
        let path = context.env.cwd().unwrap().to_owned();
        let (stream, _) = prepare_client(&socket, request(&args, &context), &context).await;
        (&stream).write_all(b"\"commit\"\n").await.unwrap();
        let line = read_bounded_line(&mut BufReader::new(&stream), &mut Vec::new())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            serde_json::from_str(&line).unwrap(),
            ExecutionReply::Accepted
        ));
        ready(&path.join("ready")).await;
        let start = Instant::now();
        drop(stream);
        let results = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        assert!(results[0].is_ok());
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(
            std::fs::read_to_string(path.join("record"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        assert_eq!(daemon.execution_workers.available_permits(), 1);
        let pid: libc::pid_t = std::fs::read_to_string(path.join("pid"))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            unsafe { libc::waitpid(pid, std::ptr::null_mut(), libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    #[test]
    #[ignore = "native child fixture for execution_jobserver_descriptors_reach_only_the_compiler"]
    fn execution_jobserver_child_fixture() {
        use std::io::{Read, Write};
        use std::os::fd::{AsRawFd, FromRawFd};
        let env = std::env::vars_os().collect::<Vec<_>>();
        assert_eq!(kache_ipc::jobserver::original_fds(&env).unwrap().len(), 2);
        let flags = std::env::var("CARGO_MAKEFLAGS").unwrap();
        let (read, write) = flags.rsplit_once('=').unwrap().1.split_once(',').unwrap();
        let mut read = unsafe { File::from_raw_fd(read.parse().unwrap()) };
        let mut write = unsafe { File::from_raw_fd(write.parse().unwrap()) };
        assert_eq!(
            unsafe { libc::fcntl(read.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        assert_eq!(
            unsafe { libc::fcntl(write.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let mut byte = [0; 1];
        read.read_exact(&mut byte).unwrap();
        assert_eq!(&byte, b"t");
        write.write_all(b"r").unwrap();
    }

    #[tokio::test]
    async fn execution_jobserver_descriptors_reach_only_the_compiler() {
        if !native_execution_available() {
            return;
        }
        use std::io::{Read, Write};
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        let root = tempfile::tempdir().unwrap();
        let (socket, _, _, server) = server(root.path(), 1, 1).await;
        let (mut args, mut context) = fixture(root.path(), "jobserver");
        let program = PathBuf::from(&args[0]);
        std::fs::remove_file(&program).unwrap();
        std::os::unix::fs::symlink(std::env::current_exe().unwrap(), &program).unwrap();
        args.extend(
            [
                "--exact",
                "daemon::execution::tests::execution_jobserver_child_fixture",
                "--ignored",
                "--nocapture",
            ]
            .map(str::to_owned),
        );
        let (mut read, mut write) = UnixStream::pair().unwrap();
        context.env.set(
            "CARGO_MAKEFLAGS",
            format!(
                "-j --jobserver-auth={},{}",
                read.as_raw_fd(),
                write.as_raw_fd()
            ),
        );
        write.write_all(b"t").unwrap();
        let result = tokio::task::spawn_blocking(move || {
            crate::invocation::execution::try_execute(&socket, &args, &context)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result, Some(0));
        assert!(server.await.unwrap()[0].is_ok());
        read.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut byte = [0; 1];
        read.read_exact(&mut byte).unwrap();
        assert_eq!(&byte, b"r");
        assert_ne!(
            unsafe { libc::fcntl(read.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        assert_ne!(
            unsafe { libc::fcntl(write.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
    }

    #[tokio::test]
    async fn execution_declines_a_forged_peer_pid_before_preparing_a_worker() {
        let root = tempfile::tempdir().unwrap();
        let (socket, daemon, _, server) = server(root.path(), 1, 1).await;
        let (args, context) = fixture(root.path(), "forged");
        let path = context.env.cwd().unwrap().to_owned();
        let mut request = request(&args, &context);
        request.client_pid = 0;
        let stream = TokioStream::connect(crate::transport::socket_name(&socket).unwrap())
            .await
            .unwrap();
        let mut frame = serde_json::to_vec(&Request::ExecuteV1(Box::new(request))).unwrap();
        frame.push(b'\n');
        (&stream).write_all(&frame).await.unwrap();
        let line = read_bounded_line(&mut BufReader::new(&stream), &mut Vec::new())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            serde_json::from_str(&line).unwrap(),
            ExecutionReply::Declined { .. }
        ));
        assert!(server.await.unwrap()[0].is_ok());
        assert!(!path.join("record").exists());
        assert_eq!(daemon.execution_workers.available_permits(), 1);
    }

    #[cfg(target_os = "macos")]
    #[test]
    #[ignore = "native sandbox client for execution_rejects_sandboxed_client_using_kernel_identity"]
    fn execution_sandboxed_client_fixture() {
        let root = PathBuf::from(std::env::var_os("KACHE_TEST_EXECUTION_ROOT").unwrap());
        let socket = PathBuf::from(std::env::var_os("KACHE_TEST_EXECUTION_SOCKET").unwrap());
        let (args, context) = fixture(&root, "restricted");
        let path = context.env.cwd().unwrap().to_owned();
        std::fs::write(path.join("release"), b"go").unwrap();
        assert_eq!(
            crate::invocation::execution::try_execute(&socket, &args, &context).unwrap(),
            None
        );
        assert!(!path.join("record").exists());
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn execution_rejects_sandboxed_client_using_kernel_identity() {
        let root = tempfile::tempdir().unwrap();
        let root_path = root.path().canonicalize().unwrap();
        let (socket, daemon, _, server) = server(&root_path, 1, 1).await;
        let denied = root_path.join("denied");
        std::fs::create_dir(&denied).unwrap();
        let profile = format!(
            "(version 1)(allow default)(deny file-write* (subpath \"{}\"))",
            denied.display()
        );
        let output = crate::platform::host_output_async(
            tokio::process::Command::new("/usr/bin/sandbox-exec")
                .arg("-p")
                .arg(profile)
                .arg(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "daemon::execution::tests::execution_sandboxed_client_fixture",
                    "--ignored",
                    "--nocapture",
                ])
                .env_clear()
                .env("PATH", "/usr/bin:/bin")
                .env("KACHE_TEST_EXECUTION_ROOT", &root_path)
                .env("KACHE_TEST_EXECUTION_SOCKET", socket)
                .kill_on_drop(true),
        )
        .await
        .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(server.await.unwrap()[0].is_ok());
        assert!(!root_path.join("restricted/record").exists());
        assert_eq!(daemon.execution_workers.available_permits(), 1);
    }
}
