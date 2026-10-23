//! Blocking compiler operations use private client-owned process channels.

use super::InvocationEnv;
use kache_protocol::execution::{
    NativeString,
    unix::{Command, MAX_FRAME_BYTES, Reply},
    windows::{Spawn, Stdio, Stream},
};
use std::io::{self, BufRead, BufReader, Read, Seek, Write};
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::{UnixDatagram, UnixStream};
use std::os::unix::process::ExitStatusExt;
use std::process::{ChildStderr, ChildStdin, ChildStdout, ExitStatus};
use std::sync::{Arc, Mutex};

pub(crate) struct Broker {
    channel: Mutex<Channel>,
    env: InvocationEnv,
    jobserver: kache_ipc::jobserver::Descriptors,
}

struct Channel {
    reader: BufReader<UnixStream>,
    pipes: UnixDatagram,
}

impl Broker {
    pub(crate) fn new(
        control: OwnedFd,
        pipes: OwnedFd,
        env: InvocationEnv,
        jobserver: kache_ipc::jobserver::Descriptors,
    ) -> io::Result<Arc<Self>> {
        let control = UnixStream::from(control);
        let timeout = Some(std::time::Duration::from_secs(5));
        control.set_read_timeout(timeout)?;
        control.set_write_timeout(timeout)?;
        let pipes = UnixDatagram::from(pipes);
        pipes.set_read_timeout(timeout)?;
        Ok(Arc::new(Self {
            channel: Mutex::new(Channel {
                reader: BufReader::new(control),
                pipes,
            }),
            env,
            jobserver,
        }))
    }

    fn call(&self, command: Command, pipes: usize) -> io::Result<(Reply, Vec<OwnedFd>)> {
        self.call_with_contents(command, pipes, None)
    }

    fn call_with_contents(
        &self,
        command: Command,
        pipes: usize,
        contents: Option<&std::fs::File>,
    ) -> io::Result<(Reply, Vec<OwnedFd>)> {
        let mut channel = self
            .channel
            .lock()
            .map_err(|_| io::Error::other("native process channel poisoned"))?;
        let mut frame = serde_json::to_vec(&command)?;
        frame.push(b'\n');
        if frame.len() as u64 > MAX_FRAME_BYTES {
            return Err(io::Error::other(
                "native process request exceeds frame limit",
            ));
        }
        let exchanged = (|| -> io::Result<(Reply, Vec<OwnedFd>)> {
            channel.reader.get_mut().write_all(&frame)?;
            if let Some(contents) = contents {
                kache_ipc::descriptors::send(channel.pipes.as_fd(), &[0; 16], &[contents.as_fd()])?;
            }
            frame.clear();
            channel
                .reader
                .by_ref()
                .take(MAX_FRAME_BYTES)
                .read_until(b'\n', &mut frame)?;
            if frame.last() != Some(&b'\n') {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native process client disconnected",
                ));
            }
            let reply = serde_json::from_slice(&frame)?;
            let files = if pipes == 0 || matches!(reply, Reply::Failed { .. }) {
                Vec::new()
            } else {
                kache_ipc::descriptors::receive(channel.pipes.as_fd(), &[0; 16], pipes)?
            };
            Ok((reply, files))
        })();
        // A timed-out or malformed exchange may leave descriptors or replies
        // queued. Close the connection so another operation cannot consume them.
        if exchanged.is_err() {
            let _ = channel.reader.get_ref().shutdown(std::net::Shutdown::Both);
        }
        let (reply, files) = exchanged?;
        if let Reply::Failed { os_error, message } = reply {
            return Err(os_error
                .map(io::Error::from_raw_os_error)
                .unwrap_or_else(|| io::Error::other(message)));
        }
        Ok((reply, files))
    }

    pub(crate) fn spawn(
        self: &Arc<Self>,
        command: &std::process::Command,
        stdio: Stdio,
    ) -> io::Result<Child> {
        let cwd = command
            .get_current_dir()
            .or(self.env.cwd())
            .ok_or_else(|| io::Error::other("native compiler has no working directory"))?;
        let vars = command
            .get_envs()
            .filter_map(|(key, value)| value.map(|value| (key.to_owned(), value.to_owned())))
            .collect::<Vec<_>>();
        let jobserver = kache_ipc::jobserver::original_fds(&vars)?
            .into_iter()
            .map(|fd| {
                self.jobserver
                    .files()
                    .iter()
                    .position(|file| file.as_raw_fd() == fd)
                    .ok_or_else(|| io::Error::other("compiler jobserver is outside the invocation"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let spawn = Spawn {
            program: NativeString::capture(command.get_program()),
            args: command.get_args().map(NativeString::capture).collect(),
            env: command
                .get_envs()
                .filter_map(|(key, value)| {
                    value.map(|value| (NativeString::capture(key), NativeString::capture(value)))
                })
                .collect(),
            cwd: NativeString::capture(cwd.as_os_str()),
            stdio,
        };
        let count = [stdio.stdin, stdio.stdout, stdio.stderr]
            .into_iter()
            .filter(|mode| *mode == Stream::Pipe)
            .count();
        let (reply, files) = self.call(
            Command::Spawn {
                command: Box::new(spawn),
                jobserver,
            },
            count,
        )?;
        let Reply::Spawned { id, pid } = reply else {
            return Err(io::Error::other("invalid native compiler spawn response"));
        };
        let mut files = files.into_iter();
        let mut next = |mode| {
            if mode == Stream::Pipe {
                files.next()
            } else {
                None
            }
        };
        Ok(Child {
            inner: Inner::Remote {
                id,
                pid,
                broker: Arc::clone(self),
            },
            stdin: next(stdio.stdin).map(ChildStdin::from),
            stdout: next(stdio.stdout).map(ChildStdout::from),
            stderr: next(stdio.stderr).map(ChildStderr::from),
        })
    }

    fn done(&self, command: Command) -> io::Result<()> {
        match self.call(command, 0)?.0 {
            Reply::Done => Ok(()),
            _ => Err(io::Error::other("invalid native compiler acknowledgement")),
        }
    }
}

impl kache_store::workspace::Workspace for Broker {
    fn execute(
        &self,
        operation: kache_store::workspace::Operation<'_>,
    ) -> io::Result<kache_store::workspace::Outcome> {
        use kache_protocol::execution::unix::Workspace;
        use kache_store::workspace::{Operation, Outcome};
        let path = |path: &std::path::Path| {
            self.env
                .resolve(path)
                .map(|path| NativeString::capture(path.as_os_str()))
                .ok_or_else(|| io::Error::other("workspace path cannot be resolved"))
        };
        let mut content = None;
        let command = match operation {
            Operation::Reflink(source, target) => Workspace::Reflink {
                source: path(source)?,
                target: path(target)?,
            },
            Operation::Copy(source, target, executable) => Workspace::Copy {
                source: path(source)?,
                target: path(target)?,
                executable,
            },
            Operation::Hardlink(source, target) => Workspace::Hardlink {
                source: path(source)?,
                target: path(target)?,
            },
            Operation::Remove(value) => Workspace::Remove { path: path(value)? },
            Operation::CreateDirectories(value) => {
                Workspace::CreateDirectories { path: path(value)? }
            }
            Operation::Permissions(value, mode) => Workspace::Permissions {
                path: path(value)?,
                mode: mode & 0o777,
            },
            Operation::Touch(value) => Workspace::Touch { path: path(value)? },
            Operation::Write(value, bytes, mode) => {
                content = Some(content_file(bytes)?);
                Workspace::Write {
                    path: path(value)?,
                    mode,
                }
            }
            Operation::PrepareFile(source, target) => Workspace::PrepareFile {
                source: path(source)?,
                target: path(target)?,
            },
            Operation::PrepareBytes(target, bytes) => {
                content = Some(content_file(bytes)?);
                Workspace::PrepareBytes {
                    target: path(target)?,
                }
            }
            Operation::Publish(id, replace) => Workspace::Publish { id, replace },
            Operation::Discard(id) => Workspace::Discard { id },
        };
        match self
            .call_with_contents(Command::Workspace(command), 0, content.as_ref())?
            .0
        {
            Reply::Done => Ok(Outcome::Done),
            Reply::PreparedFile {
                id,
                bytes,
                reflinked,
            } => Ok(Outcome::Prepared {
                id,
                bytes,
                reflinked,
            }),
            _ => Err(io::Error::other("invalid workspace operation response")),
        }
    }
}

fn content_file(bytes: &[u8]) -> io::Result<std::fs::File> {
    let fd = unsafe { libc::memfd_create(c"kache-restore".as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    file.write_all(bytes)?;
    file.rewind()?;
    Ok(file)
}

pub(crate) struct Child {
    inner: Inner,
    pub(crate) stdin: Option<ChildStdin>,
    pub(crate) stdout: Option<ChildStdout>,
    pub(crate) stderr: Option<ChildStderr>,
}

enum Inner {
    Local(std::process::Child),
    Remote {
        id: u64,
        pid: u32,
        broker: Arc<Broker>,
    },
}

impl Child {
    pub(crate) fn local(mut child: std::process::Child) -> Self {
        Self {
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
            inner: Inner::Local(child),
        }
    }

    pub(crate) fn id(&self) -> u32 {
        match &self.inner {
            Inner::Local(child) => child.id(),
            Inner::Remote { pid, .. } => *pid,
        }
    }

    pub(crate) fn remote(&self) -> bool {
        matches!(self.inner, Inner::Remote { .. })
    }

    #[cfg(test)]
    pub(crate) fn wait(&mut self) -> io::Result<ExitStatus> {
        match &mut self.inner {
            Inner::Local(child) => child.wait(),
            Inner::Remote { .. } => Err(io::Error::other(
                "native broker wait requires request ownership",
            )),
        }
    }

    pub(crate) fn kill(&mut self) -> io::Result<()> {
        match &mut self.inner {
            Inner::Local(child) => child.kill(),
            Inner::Remote { id, broker, .. } => broker.done(Command::Kill { id: *id }),
        }
    }

    pub(crate) fn try_wait_remote(&self) -> io::Result<(Option<ExitStatus>, i64)> {
        let Inner::Remote { id, broker, .. } = &self.inner else {
            return Err(io::Error::other("compiler child is local"));
        };
        match broker.call(Command::TryWait { id: *id }, 0)?.0 {
            Reply::Waited { status, max_rss } => Ok((status.map(ExitStatus::from_raw), max_rss)),
            _ => Err(io::Error::other("invalid native compiler wait response")),
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if let Inner::Remote { id, broker, .. } = &self.inner {
            let _ = broker.done(Command::Release { id: *id });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sized_frame(bytes: usize) -> Command {
        let command = |units| {
            Command::Workspace(kache_protocol::execution::unix::Workspace::Remove {
                path: NativeString::Unix(units),
            })
        };
        let overhead = serde_json::to_vec(&command(Vec::new())).unwrap().len() + 1;
        let count = (bytes - overhead).div_ceil(2);
        let mut units = vec![0; count];
        if overhead + count * 2 - 1 < bytes {
            units[0] = 99;
        }
        let command = command(units);
        assert_eq!(serde_json::to_vec(&command).unwrap().len() + 1, bytes);
        command
    }

    #[test]
    fn frame_limit_accepts_the_boundary_and_rejects_larger_requests_before_writing() {
        let (broker, client) = pair();
        let error = broker.call(sized_frame(8_388_609), 0).unwrap_err();
        assert!(error.to_string().contains("exceeds frame limit"));
        std::thread::scope(|threads| {
            let peer = threads.spawn(move || {
                let mut reader = BufReader::new(client);
                let mut frame = Vec::new();
                reader.read_until(b'\n', &mut frame).unwrap();
                assert_eq!(frame.len(), 8_388_608);
                assert_eq!(frame.last(), Some(&b'\n'));
                reader
                    .get_mut()
                    .write_all(b"{\"operation_result\":\"done\"}\n")
                    .unwrap();
            });
            assert_eq!(
                broker.call(sized_frame(8_388_608), 0).unwrap().0,
                Reply::Done
            );
            peer.join().unwrap();
        });
    }

    #[test]
    fn workspace_permissions_strip_file_type_and_special_bits() {
        use kache_store::workspace::{Operation, Workspace};
        let (broker, client) = pair();
        let root = tempfile::tempdir().unwrap();
        std::thread::scope(|threads| {
            let peer = threads.spawn(move || {
                let mut reader = BufReader::new(client);
                let mut frame = String::new();
                reader.read_line(&mut frame).unwrap();
                let Command::Workspace(kache_protocol::execution::unix::Workspace::Permissions {
                    mode,
                    ..
                }) = serde_json::from_str(&frame).unwrap()
                else {
                    panic!("missing permissions operation");
                };
                assert_eq!(mode, 0o644);
                reader
                    .get_mut()
                    .write_all(b"{\"operation_result\":\"done\"}\n")
                    .unwrap();
            });
            broker
                .execute(Operation::Permissions(root.path(), 0o104644))
                .unwrap();
            peer.join().unwrap();
        });
    }

    #[test]
    fn content_files_accept_descriptor_zero_and_propagate_creation_errors() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "invocation::linux_broker::tests::content_descriptor_fixture",
                "--ignored",
                "--nocapture",
            ])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }

    #[test]
    #[ignore = "isolated process closes stdin and changes its descriptor limit"]
    fn content_descriptor_fixture() {
        unsafe {
            libc::close(0);
        }
        let mut file = content_file(b"transformed bytes").unwrap();
        assert_eq!(file.as_raw_fd(), 0);
        let mut contents = Vec::new();
        file.read_to_end(&mut contents).unwrap();
        assert_eq!(contents, b"transformed bytes");
        drop(file);
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
            0
        );
        let exhausted = libc::rlimit {
            rlim_cur: 0,
            rlim_max: limit.rlim_max,
        };
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &exhausted) },
            0
        );
        let error = content_file(b"").unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
    }

    fn pair() -> (Arc<Broker>, UnixStream) {
        let (control, client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .unwrap();
        let (pipes, _client_pipes) = UnixDatagram::pair().unwrap();
        let broker = Broker::new(
            control.into(),
            pipes.into(),
            InvocationEnv::default(),
            kache_ipc::jobserver::Descriptors::default(),
        )
        .unwrap();
        (broker, client)
    }

    #[test]
    fn remote_children_report_status_and_release_their_client_owner() {
        let (broker, client) = pair();
        std::thread::scope(|threads| {
            let peer = threads.spawn(move || {
                let mut reader = BufReader::new(client);
                for (expected, reply) in [
                    (
                        Command::TryWait { id: 5 },
                        Reply::Waited {
                            status: Some(7 << 8),
                            max_rss: 99,
                        },
                    ),
                    (Command::Kill { id: 5 }, Reply::Done),
                    (Command::Release { id: 5 }, Reply::Done),
                ] {
                    let mut frame = String::new();
                    reader.read_line(&mut frame).unwrap();
                    assert_eq!(serde_json::from_str::<Command>(&frame).unwrap(), expected);
                    let mut bytes = serde_json::to_vec(&reply).unwrap();
                    bytes.push(b'\n');
                    reader.get_mut().write_all(&bytes).unwrap();
                }
            });
            let mut child = Child {
                inner: Inner::Remote {
                    id: 5,
                    pid: 7,
                    broker,
                },
                stdin: None,
                stdout: None,
                stderr: None,
            };
            let (status, max_rss) = child.try_wait_remote().unwrap();
            assert_eq!(status.unwrap().code(), Some(7));
            assert_eq!(max_rss, 99);
            child.kill().unwrap();
            drop(child);
            peer.join().unwrap();
        });
    }

    #[test]
    fn malformed_replies_close_the_channel_before_another_operation_can_reuse_it() {
        let (broker, client) = pair();
        std::thread::scope(|threads| {
            let peer = threads.spawn(move || {
                let mut reader = BufReader::new(client);
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                reader.get_mut().write_all(b"malformed\n").unwrap();
                let mut byte = [0];
                assert_eq!(
                    reader.read(&mut byte).unwrap(),
                    0,
                    "transport error must end the broker connection"
                );
            });
            assert!(broker.call(Command::TryWait { id: 1 }, 0).is_err());
            assert!(broker.call(Command::TryWait { id: 2 }, 0).is_err());
            peer.join().unwrap();
        });
    }

    #[test]
    fn permission_denials_keep_the_framed_channel_usable() {
        let (broker, client) = pair();
        std::thread::scope(|threads| {
            let peer = threads.spawn(move || {
                let mut reader = BufReader::new(client);
                for reply in [
                    Reply::Failed {
                        os_error: Some(libc::EACCES),
                        message: "denied".into(),
                    },
                    Reply::Done,
                ] {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    assert!(!line.is_empty());
                    let mut bytes = serde_json::to_vec(&reply).unwrap();
                    bytes.push(b'\n');
                    reader.get_mut().write_all(&bytes).unwrap();
                }
            });
            assert_eq!(
                broker
                    .done(Command::Kill { id: 1 })
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::EACCES)
            );
            broker.done(Command::Release { id: 1 }).unwrap();
            peer.join().unwrap();
        });
    }
}
