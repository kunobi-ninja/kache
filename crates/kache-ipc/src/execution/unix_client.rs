//! The original client owns native children; the daemon owns cache work.

use kache_protocol::execution::{
    ExecutionChannels, ExecutionRequest,
    process::{Spawn, Stdio as Plan, Stream},
    unix::{Command, MAX_FRAME_BYTES, Reply},
};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::net::{UnixDatagram, UnixStream};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Stdio};

mod workspace;

pub(super) struct Client {
    control: BufReader<UnixStream>,
    pipes: UnixDatagram,
    stdio: [File; 3],
    jobserver: crate::jobserver::Descriptors,
    children: HashMap<u64, Pending>,
    next_id: u64,
    workspace: workspace::Files,
}

struct Pending {
    child: Child,
    status: Option<i32>,
    max_rss: i64,
}

impl Pending {
    fn kill(&mut self) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        // This owner retains the child PID until wait4 reaps it.
        unsafe { libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL) };
        self.child.kill()
    }

    fn wait(&mut self, flags: i32) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        let mut status = 0;
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let result = retry_interrupted(|| {
            checked(unsafe {
                libc::wait4(
                    self.child.id() as libc::pid_t,
                    &mut status,
                    flags,
                    usage.as_mut_ptr(),
                )
            })
        })?;
        if result > 0 {
            self.status = Some(status);
            self.max_rss = unsafe { usage.assume_init() }.ru_maxrss;
        }
        Ok(())
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        let _ = self.kill();
        drop(self.child.stdin.take());
        drop(self.child.stdout.take());
        drop(self.child.stderr.take());
        let _ = self.wait(0);
    }
}

impl Client {
    pub(super) fn prepare(
        request: &ExecutionRequest,
        files: &[BorrowedFd<'_>],
    ) -> io::Result<(Self, [OwnedFd; 2])> {
        let ExecutionChannels::UnixClient {
            terminal,
            jobserver,
            ..
        } = &request.channels
        else {
            return Err(invalid("missing client process broker"));
        };
        let offset = 3 + usize::from(*terminal);
        if files.len() != offset + jobserver.len() {
            return Err(invalid("invalid compiler descriptor manifest"));
        }
        let copy = |index: usize| files[index].try_clone_to_owned().map(File::from);
        let jobserver = crate::jobserver::Descriptors::new(
            files[offset..]
                .iter()
                .map(|file| file.try_clone_to_owned())
                .collect::<io::Result<_>>()?,
        )?;
        let (control, peer) = UnixStream::pair()?;
        control.set_read_timeout(Some(super::PREPARE_TIMEOUT))?;
        control.set_write_timeout(Some(super::PREPARE_TIMEOUT))?;
        let (pipes, peer_pipes) = UnixDatagram::pair()?;
        pipes.set_write_timeout(Some(super::PREPARE_TIMEOUT))?;
        pipes.set_read_timeout(Some(super::PREPARE_TIMEOUT))?;
        Ok((
            Self {
                control: BufReader::new(control),
                pipes,
                stdio: [copy(0)?, copy(1)?, copy(2)?],
                jobserver,
                children: HashMap::new(),
                next_id: 0,
                workspace: workspace::Files::default(),
            },
            [peer.into(), peer_pipes.into()],
        ))
    }

    /// Wait on both connections. Completion or disconnect must interrupt an
    /// otherwise idle broker; compiler output travels over independent pipes.
    pub(super) fn ready(&self, completion: BorrowedFd<'_>) -> io::Result<bool> {
        let mut fds = [
            libc::pollfd {
                fd: completion.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.control.get_ref().as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        retry_interrupted(|| checked(unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) }))?;
        Ok(fds[0].revents == 0)
    }

    pub(super) fn service(&mut self) -> anyhow::Result<bool> {
        use std::io::Read;
        let mut frame = Vec::new();
        self.control
            .by_ref()
            .take(MAX_FRAME_BYTES)
            .read_until(b'\n', &mut frame)?;
        if frame.is_empty() {
            return Ok(false);
        }
        anyhow::ensure!(
            frame.last() == Some(&b'\n'),
            "incomplete native process request"
        );
        let command = serde_json::from_slice(&frame)?;
        let (reply, files) = match self.execute(command) {
            Ok(result) => result,
            Err(error) => (
                Reply::Failed {
                    os_error: error.raw_os_error(),
                    message: error.to_string(),
                },
                Vec::new(),
            ),
        };
        super::write_message(self.control.get_mut(), &reply)?;
        if !files.is_empty() {
            let files = files.iter().map(AsFd::as_fd).collect::<Vec<_>>();
            crate::descriptors::send(self.pipes.as_fd(), &[0; 16], &files)?;
        }
        Ok(true)
    }

    fn execute(&mut self, command: Command) -> io::Result<(Reply, Vec<OwnedFd>)> {
        let reply = match command {
            Command::Workspace(operation) => {
                return self
                    .workspace
                    .execute(operation, &self.pipes)
                    .map(|reply| (reply, Vec::new()));
            }
            Command::Spawn { command, jobserver } => return self.spawn(*command, jobserver),
            Command::TryWait { id } => {
                let pending = self.children.get_mut(&id).ok_or_else(unknown)?;
                pending.wait(libc::WNOHANG)?;
                Reply::Waited {
                    status: pending.status,
                    max_rss: pending.max_rss,
                }
            }
            Command::Kill { id } => {
                self.children.get_mut(&id).ok_or_else(unknown)?.kill()?;
                Reply::Done
            }
            Command::Release { id } => {
                drop(self.children.remove(&id).ok_or_else(unknown)?);
                Reply::Done
            }
        };
        Ok((reply, Vec::new()))
    }

    fn spawn(&mut self, spawn: Spawn, indices: Vec<usize>) -> io::Result<(Reply, Vec<OwnedFd>)> {
        if self.children.len() >= 64 {
            return Err(invalid("too many native compiler children"));
        }
        let program = PathBuf::from(spawn.program.decode().map_err(invalid)?);
        let cwd = PathBuf::from(spawn.cwd.decode().map_err(invalid)?);
        if !program.is_absolute() || !cwd.is_absolute() {
            return Err(invalid("native compiler paths must be absolute"));
        }
        let args = spawn
            .args
            .iter()
            .map(|arg| arg.decode().map_err(invalid))
            .collect::<io::Result<Vec<_>>>()?;
        let env = spawn
            .env
            .iter()
            .map(|(key, value)| {
                Ok((
                    key.decode().map_err(invalid)?,
                    value.decode().map_err(invalid)?,
                ))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let originals = crate::jobserver::original_fds(&env)?;
        if originals.len() != indices.len() {
            return Err(invalid("invalid compiler jobserver mapping"));
        }
        let mut used = std::collections::BTreeSet::new();
        let jobserver = crate::jobserver::Descriptors::new(
            indices
                .into_iter()
                .map(|index| {
                    if !used.insert(index) {
                        return Err(invalid("duplicate compiler jobserver endpoint"));
                    }
                    self.jobserver
                        .files()
                        .get(index)
                        .ok_or_else(|| invalid("unknown compiler jobserver endpoint"))?
                        .try_clone()
                })
                .collect::<io::Result<Vec<_>>>()?,
        )?;
        let env = crate::jobserver::rewrite(&env, &originals, &jobserver)?;
        let mut command = std::process::Command::new(program);
        command.args(args).env_clear().envs(env).current_dir(cwd);
        let Plan {
            stdin,
            stdout,
            stderr,
        } = spawn.stdio;
        command
            .stdin(self.stream(stdin, 0)?)
            .stdout(self.stream(stdout, 1)?)
            .stderr(self.stream(stderr, 2)?);
        command.process_group(0);
        #[cfg(target_os = "linux")]
        let parent = std::process::id();
        #[cfg(target_os = "linux")]
        unsafe {
            command.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() != parent as libc::pid_t {
                    return Err(io::Error::from_raw_os_error(libc::EINTR));
                }
                Ok(())
            });
        }
        let _scope = crate::jobserver::Scope::enter(jobserver);
        crate::jobserver::configure(&mut command);
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| invalid("compiler child ID exhausted"))?;
        let child = {
            let _admission = crate::descriptors::spawn_guard();
            command.spawn()?
        };
        let mut pending = Pending {
            child,
            status: None,
            max_rss: 0,
        };
        let reply = Reply::Spawned {
            id: self.next_id,
            pid: pending.child.id(),
        };
        let mut files = Vec::new();
        if let Some(pipe) = pending.child.stdin.take() {
            files.push(pipe.into());
        }
        if let Some(pipe) = pending.child.stdout.take() {
            files.push(pipe.into());
        }
        if let Some(pipe) = pending.child.stderr.take() {
            files.push(pipe.into());
        }
        self.children.insert(self.next_id, pending);
        Ok((reply, files))
    }

    fn stream(&self, mode: Stream, index: usize) -> io::Result<Stdio> {
        Ok(match mode {
            Stream::Inherit => self.stdio[index].try_clone()?.into(),
            Stream::Null => Stdio::null(),
            Stream::Pipe => Stdio::piped(),
        })
    }
}

fn checked(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

fn retry_interrupted<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

fn invalid(message: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_string())
}

fn unknown() -> io::Error {
    invalid("unknown native compiler child")
}

#[cfg(test)]
mod tests {
    use super::*;
    use kache_protocol::execution::{EXECUTION_VERSION, ExecutionEntry, NativeString};
    use std::io::{Read, Seek, Write};
    use std::os::unix::process::ExitStatusExt;

    fn isolated_test(name: &str) {
        let root = tempfile::tempdir().unwrap();
        let stdout = root.path().join("stdout");
        let stderr = root.path().join("stderr");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("execution::unix_client::tests::{name}"),
                "--ignored",
                "--nocapture",
            ])
            .stdout(File::create(&stdout).unwrap())
            .stderr(File::create(&stderr).unwrap())
            .process_group(0)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                unsafe {
                    libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
                }
                break child.wait().unwrap();
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        };
        assert!(
            status.success(),
            "{name}: {status}\n{}\n{}",
            std::fs::read_to_string(stdout).unwrap(),
            std::fs::read_to_string(stderr).unwrap()
        );
    }

    fn fixture(root: &std::path::Path) -> Client {
        fixture_with_manifest(root, false, 0).0
    }

    fn fixture_with_manifest(
        root: &std::path::Path,
        terminal: bool,
        jobservers: usize,
    ) -> (Client, [OwnedFd; 2]) {
        let mut files = vec![
            tempfile::tempfile().unwrap(),
            tempfile::tempfile().unwrap(),
            tempfile::tempfile().unwrap(),
        ];
        for _ in 0..usize::from(terminal) + jobservers {
            files.push(tempfile::tempfile().unwrap());
        }
        let request = ExecutionRequest {
            version: EXECUTION_VERSION,
            entry: ExecutionEntry::Compiler,
            client_version: "fixture".into(),
            client_pid: std::process::id(),
            creation_mask: Some(0o022),
            argv: Vec::new(),
            env: Vec::new(),
            cwd: NativeString::capture(root.as_os_str()),
            home: None,
            temp: NativeString::capture(root.as_os_str()),
            cache: None,
            executable: NativeString::capture("/bin/true".as_ref()),
            startup_micros: 0,
            channels: ExecutionChannels::UnixClient {
                terminal,
                jobserver: (0..jobservers as i32).collect(),
                trusted_pipeline: false,
            },
        };
        Client::prepare(&request, &files.iter().map(AsFd::as_fd).collect::<Vec<_>>()).unwrap()
    }

    fn spawn(root: &std::path::Path, script: &str, stdio: Plan) -> Command {
        let native = |value: &str| NativeString::capture(value.as_ref());
        Command::Spawn {
            command: Box::new(Spawn {
                program: native("/bin/sh"),
                args: vec![native("-c"), native(script)],
                env: vec![(native("CLIENT_VALUE"), native("request-only"))],
                cwd: NativeString::capture(root.as_os_str()),
                stdio,
            }),
            jobserver: Vec::new(),
        }
    }

    #[test]
    fn terminal_and_jobserver_descriptors_follow_the_manifest() {
        let root = tempfile::tempdir().unwrap();
        for terminal in [false, true] {
            let (client, _peer) = fixture_with_manifest(root.path(), terminal, 2);
            assert_eq!(client.jobserver.files().len(), 2);
        }
    }

    #[test]
    fn readiness_waits_for_an_event_and_prioritizes_completion() {
        let root = tempfile::tempdir().unwrap();
        let (client, endpoints) = fixture_with_manifest(root.path(), false, 0);
        let mut control = UnixStream::from(endpoints.into_iter().next().unwrap());
        let (completion, mut producer) = UnixStream::pair().unwrap();
        std::thread::scope(|threads| {
            threads.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(40));
                producer.write_all(b"complete").unwrap();
            });
            let start = std::time::Instant::now();
            assert!(!client.ready(completion.as_fd()).unwrap());
            assert!(start.elapsed() >= std::time::Duration::from_millis(10));
        });
        let (empty, _producer) = UnixStream::pair().unwrap();
        control.write_all(b"request").unwrap();
        assert!(client.ready(empty.as_fd()).unwrap());
        assert!(!client.ready(completion.as_fd()).unwrap());
    }

    #[test]
    fn interrupted_syscalls_retry_and_other_errors_return_immediately() {
        let mut calls = 0;
        assert_eq!(
            retry_interrupted(|| {
                calls += 1;
                if calls == 1 {
                    Err(io::ErrorKind::Interrupted.into())
                } else {
                    Ok(7)
                }
            })
            .unwrap(),
            7
        );
        assert_eq!(calls, 2);
        let mut calls = 0;
        let error = retry_interrupted::<()>(|| {
            calls += 1;
            assert_eq!(calls, 1, "non-interrupted errors must not retry");
            Err(io::ErrorKind::PermissionDenied.into())
        })
        .unwrap_err();
        assert_eq!(calls, 1);
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(checked(0).unwrap(), 0);
        assert_eq!(checked(17).unwrap(), 17);
        assert!(checked(-1).is_err());
    }

    #[test]
    fn native_child_uses_client_parent_streams_environment_and_directory() {
        let root = tempfile::tempdir().unwrap();
        let mut client = fixture(root.path());
        let command = spawn(
            root.path(),
            "IFS= read -r line; printf '%s:%s:%s:%s' \"$PPID\" \"$CLIENT_VALUE\" \"$PWD\" \"$line\"; printf error >&2; exit 23",
            Plan {
                stdin: Stream::Pipe,
                stdout: Stream::Pipe,
                stderr: Stream::Pipe,
            },
        );
        let (Reply::Spawned { id, pid }, files) = client.execute(command).unwrap() else {
            panic!("missing child");
        };
        assert_ne!(pid, std::process::id());
        assert_eq!(files.len(), 3);
        let mut files = files.into_iter().map(File::from);
        let mut input = files.next().unwrap();
        input.write_all(b"native-input\n").unwrap();
        drop(input);
        let mut output = String::new();
        files.next().unwrap().read_to_string(&mut output).unwrap();
        assert_eq!(
            output,
            format!(
                "{}:request-only:{}:native-input",
                std::process::id(),
                root.path().canonicalize().unwrap().display()
            )
        );
        let mut error = String::new();
        files.next().unwrap().read_to_string(&mut error).unwrap();
        assert_eq!(error, "error");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let status = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "native child was never reaped"
            );
            let (Reply::Waited { status, max_rss }, files) =
                client.execute(Command::TryWait { id }).unwrap()
            else {
                panic!("missing status");
            };
            assert!(files.is_empty());
            if let Some(status) = status {
                assert!(max_rss > 0);
                break status;
            }
            std::thread::yield_now();
        };
        assert_eq!(std::process::ExitStatus::from_raw(status).code(), Some(23));
        assert!(matches!(
            client.execute(Command::Kill { id }).unwrap().0,
            Reply::Done
        ));
        client.execute(Command::Release { id }).unwrap();
        assert!(client.execute(Command::TryWait { id }).is_err());
    }

    #[test]
    fn dropping_the_client_kills_and_reaps_its_live_child_group() {
        isolated_test("dropping_client_fixture");
    }

    #[test]
    #[ignore = "bounded subprocess fixture"]
    fn dropping_client_fixture() {
        let root = tempfile::tempdir().unwrap();
        let mut client = fixture(root.path());
        let (Reply::Spawned { id, pid }, files) = client
            .execute(spawn(
                root.path(),
                "read line",
                Plan {
                    stdin: Stream::Pipe,
                    stdout: Stream::Null,
                    stderr: Stream::Null,
                },
            ))
            .unwrap()
        else {
            panic!("missing child");
        };
        let (Reply::Waited { status, .. }, _) = client.execute(Command::TryWait { id }).unwrap()
        else {
            panic!("missing status");
        };
        assert_eq!(status, None);
        drop(client);
        assert_eq!(unsafe { libc::kill(pid as libc::pid_t, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(pid as libc::pid_t, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
        drop(files);
    }

    #[test]
    fn native_spawn_rejects_relative_paths_and_invalid_encoding() {
        let root = tempfile::tempdir().unwrap();
        let mut client = fixture(root.path());
        for program in [
            NativeString::capture("sh".as_ref()),
            NativeString::Windows(vec![65]),
            NativeString::Unix(vec![0]),
        ] {
            let Command::Spawn {
                mut command,
                jobserver,
            } = spawn(root.path(), "exit 0", Plan::QUIET)
            else {
                unreachable!()
            };
            command.program = program;
            assert!(
                client
                    .execute(Command::Spawn { command, jobserver })
                    .is_err()
            );
            assert!(client.children.is_empty());
        }
        assert!(client.execute(Command::Kill { id: 77 }).is_err());
        assert!(client.execute(Command::Release { id: 77 }).is_err());
    }

    #[test]
    fn jobserver_mapping_keeps_endpoint_order_when_daemon_fd_numbers_are_reversed() {
        let root = tempfile::tempdir().unwrap();
        let mut client = fixture(root.path());
        let mut first = tempfile::tempfile().unwrap();
        let mut second = tempfile::tempfile().unwrap();
        first.write_all(b"read-end").unwrap();
        first.rewind().unwrap();
        second.write_all(b"write-end").unwrap();
        second.rewind().unwrap();
        client.jobserver =
            crate::jobserver::Descriptors::new(vec![first.into(), second.into()]).unwrap();
        let Command::Spawn { mut command, .. } = spawn(
            root.path(),
            "pair=${CARGO_MAKEFLAGS#*=}; r=${pair%,*}; w=${pair#*,}; /bin/cat /dev/fd/$r; /bin/cat /dev/fd/$w",
            Plan::OUTPUT,
        ) else {
            unreachable!()
        };
        command.env.push((
            NativeString::capture("CARGO_MAKEFLAGS".as_ref()),
            NativeString::capture("--jobserver-auth=22,11".as_ref()),
        ));
        let (Reply::Spawned { id, .. }, files) = client
            .execute(Command::Spawn {
                command,
                jobserver: vec![1, 0],
            })
            .unwrap()
        else {
            panic!("missing child");
        };
        let mut outputs = files.into_iter().map(File::from);
        let mut actual = String::new();
        outputs.next().unwrap().read_to_string(&mut actual).unwrap();
        let mut error = String::new();
        outputs.next().unwrap().read_to_string(&mut error).unwrap();
        assert_eq!(actual, "read-endwrite-end", "{error}");
        client.execute(Command::Release { id }).unwrap();
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn native_children_keep_the_original_clients_seccomp_and_no_new_privs() {
        isolated_test("restricted_client_fixture");
    }

    #[test]
    #[ignore = "isolated process installs an additional syscall filter"]
    #[cfg(target_os = "linux")]
    fn restricted_client_fixture() {
        let root = tempfile::tempdir().unwrap();
        let mut client = fixture(root.path());
        let blob = root.path().join("blob");
        let denied = root.path().join("denied-output");
        std::fs::write(&blob, b"cached bytes").unwrap();
        // This subprocess denies a compiler query and write-capable openat.
        // The outer test runner does not install this additional filter.
        let instructions = [
            libc::sock_filter {
                code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
                jt: 0,
                jf: 0,
                k: 0,
            },
            libc::sock_filter {
                code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
                jt: 0,
                jf: 1,
                k: libc::SYS_getpriority as u32,
            },
            libc::sock_filter {
                code: (libc::BPF_RET | libc::BPF_K) as u16,
                jt: 0,
                jf: 0,
                k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
            },
            libc::sock_filter {
                code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
                jt: 0,
                jf: 3,
                k: libc::SYS_openat as u32,
            },
            // struct seccomp_data.args[2] is the openat flags argument.
            libc::sock_filter {
                code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
                jt: 0,
                jf: 0,
                k: 32,
            },
            libc::sock_filter {
                code: (libc::BPF_JMP | libc::BPF_JSET | libc::BPF_K) as u16,
                jt: 0,
                jf: 1,
                k: (libc::O_WRONLY | libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC) as u32,
            },
            libc::sock_filter {
                code: (libc::BPF_RET | libc::BPF_K) as u16,
                jt: 0,
                jf: 0,
                k: libc::SECCOMP_RET_ERRNO | libc::EPERM as u32,
            },
            libc::sock_filter {
                code: (libc::BPF_RET | libc::BPF_K) as u16,
                jt: 0,
                jf: 0,
                k: libc::SECCOMP_RET_ALLOW,
            },
        ];
        let program = libc::sock_fprog {
            len: instructions.len() as u16,
            filter: instructions.as_ptr().cast_mut(),
        };
        assert_eq!(
            unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
            0
        );
        assert_eq!(
            unsafe { libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &program) },
            0
        );
        let native = |value: &std::ffi::OsStr| NativeString::capture(value);
        let copy = Command::Workspace(kache_protocol::execution::unix::Workspace::Copy {
            source: native(blob.as_os_str()),
            target: native(denied.as_os_str()),
            executable: false,
        });
        assert_eq!(
            client.execute(copy).unwrap_err().raw_os_error(),
            Some(libc::EPERM)
        );
        assert!(!denied.exists());
        assert_eq!(std::fs::read(&blob).unwrap(), b"cached bytes");
        let command = Command::Spawn {
            command: Box::new(Spawn {
                program: native(std::env::current_exe().unwrap().as_os_str()),
                args: [
                    "--exact",
                    "execution::unix_client::tests::restricted_compiler_fixture",
                    "--ignored",
                    "--nocapture",
                ]
                .iter()
                .map(|value| native(value.as_ref()))
                .collect(),
                env: Vec::new(),
                cwd: native(root.path().as_os_str()),
                stdio: Plan {
                    stdin: Stream::Inherit,
                    stdout: Stream::Pipe,
                    stderr: Stream::Pipe,
                },
            }),
            jobserver: Vec::new(),
        };
        let (Reply::Spawned { id, .. }, files) = client.execute(command).unwrap() else {
            panic!("missing native child");
        };
        let outputs = files
            .into_iter()
            .map(File::from)
            .map(|mut file| {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes).unwrap();
                bytes
            })
            .collect::<Vec<_>>();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let status = loop {
            assert!(
                std::time::Instant::now() < deadline,
                "native child was never reaped"
            );
            let (Reply::Waited { status, .. }, _) =
                client.execute(Command::TryWait { id }).unwrap()
            else {
                panic!("missing status");
            };
            if let Some(status) = status {
                break std::process::ExitStatus::from_raw(status);
            }
        };
        assert!(status.success(), "{outputs:?}");
        assert!(String::from_utf8_lossy(&outputs[0]).contains("1 passed"));
        client.execute(Command::Release { id }).unwrap();
    }

    #[test]
    #[ignore = "native compiler verifies the original clients additional filter"]
    #[cfg(target_os = "linux")]
    fn restricted_compiler_fixture() {
        assert_eq!(
            unsafe { libc::prctl(libc::PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) },
            1
        );
        assert_eq!(
            unsafe { libc::syscall(libc::SYS_getpriority, libc::PRIO_PROCESS, 0) },
            -1
        );
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
    }
}
