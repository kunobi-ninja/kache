//! Native process creation and console writes stay in the original frontend.

use crate::windows_process::{Job, spawn_with_lifetime};
use kache_protocol::execution::windows::{Child, Command, Output, Reply, Spawn, Stream};
use std::collections::HashMap;
use std::io::{self, Write};
use std::os::windows::io::{AsHandle, AsRawHandle};
use std::path::PathBuf;

#[derive(Default)]
pub(super) struct Client {
    next_id: u64,
    children: HashMap<u64, Pending>,
    stalled_discovery: bool,
}

struct Pending {
    child: std::process::Child,
    lifetime: Job,
    cancellation: Job,
}

impl Drop for Pending {
    fn drop(&mut self) {
        // This job has no daemon duplicate. It also covers descendants when
        // the control connection disappears while a compiler is still alive.
        let _ = self.lifetime.kill();
        drop(self.child.stdin.take());
        let _ = self.child.wait();
    }
}

impl Client {
    pub(super) fn execute(&mut self, command: Command) -> Reply {
        match self.execute_inner(command) {
            Ok(reply) => reply,
            Err(error) => Reply::Failed {
                os_error: error.raw_os_error(),
                message: error.to_string(),
            },
        }
    }

    fn execute_inner(&mut self, command: Command) -> io::Result<Reply> {
        match command {
            Command::DiscoverMsvc(request) => self.discover(request),
            Command::Spawn(spawn) => self.spawn(*spawn),
            Command::Release { id } => {
                let pending = self.children.get_mut(&id).ok_or_else(unknown_child)?;
                drop(pending.child.stdin.take());
                drop(pending.child.stdout.take());
                drop(pending.child.stderr.take());
                Ok(Reply::Done)
            }
            Command::Abort { id } => {
                let pending = self.children.get_mut(&id).ok_or_else(unknown_child)?;
                pending.lifetime.kill()?;
                pending.child.wait()?;
                self.children.remove(&id);
                Ok(Reply::Done)
            }
            Command::Finish { id } => {
                let pending = self.children.get_mut(&id).ok_or_else(unknown_child)?;
                if pending.child.try_wait()?.is_none() {
                    return Err(io::Error::other("native compiler has not exited"));
                }
                self.children.remove(&id);
                Ok(Reply::Done)
            }
            Command::Write { output, bytes } => {
                match output {
                    Output::Stdout => std::io::stdout().lock().write_all(&bytes)?,
                    Output::Stderr => std::io::stderr().lock().write_all(&bytes)?,
                }
                Ok(Reply::Done)
            }
            Command::Flush { output } => {
                match output {
                    Output::Stdout => std::io::stdout().lock().flush()?,
                    Output::Stderr => std::io::stderr().lock().flush()?,
                }
                Ok(Reply::Done)
            }
        }
    }

    fn discover(
        &mut self,
        request: kache_protocol::execution::windows::Discovery,
    ) -> io::Result<Reply> {
        if self.stalled_discovery {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "MSVC discovery already timed out",
            ));
        }
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("kache-msvc-discovery".into())
            .spawn(move || {
                let _ = sender.send(crate::windows_msvc::discover(&request));
            })?;
        match receiver.recv_timeout(std::time::Duration::from_secs(5)) {
            Ok(result) => {
                let _ = worker.join();
                result.map(|tools| Reply::Msvc { tools })
            }
            Err(error) => {
                // The frontend's process-lifetime job owns discovery's native
                // descendants. Process exit cancels this detached worker too.
                self.stalled_discovery = true;
                Err(io::Error::other(format!(
                    "MSVC discovery did not complete: {error}"
                )))
            }
        }
    }

    fn spawn(&mut self, spawn: Spawn) -> io::Result<Reply> {
        if self.children.len() >= 64 {
            return Err(io::Error::other("too many live compiler children"));
        }
        let program = PathBuf::from(spawn.program.decode().map_err(invalid)?);
        let cwd = PathBuf::from(spawn.cwd.decode().map_err(invalid)?);
        if !program.is_absolute() || !cwd.is_absolute() {
            return Err(invalid("native compiler paths must be absolute"));
        }
        let args = spawn
            .args
            .iter()
            .map(|value| value.decode().map_err(invalid))
            .collect::<io::Result<Vec<_>>>()?;
        let env = spawn
            .env
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.decode().map_err(invalid)?,
                    value.decode().map_err(invalid)?,
                ))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut command = std::process::Command::new(program);
        command
            .args(args)
            .env_clear()
            .envs(env)
            .current_dir(cwd)
            .stdin(stdio(spawn.stdio.stdin))
            .stdout(stdio(spawn.stdio.stdout))
            .stderr(stdio(spawn.stdio.stderr));
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| io::Error::other("compiler child ID exhausted"))?;
        let (child, lifetime, cancellation) = spawn_with_lifetime(&mut command)?;
        let pending = Pending {
            child,
            lifetime,
            cancellation,
        };
        let describe = |value: &dyn AsRawHandle| value.as_raw_handle() as usize as u64;
        let child = Child {
            id: self.next_id,
            pid: pending.child.id(),
            process: describe(&pending.child),
            job: pending.cancellation.as_handle().as_raw_handle() as usize as u64,
            stdin: pending.child.stdin.as_ref().map(|pipe| describe(pipe)),
            stdout: pending.child.stdout.as_ref().map(|pipe| describe(pipe)),
            stderr: pending.child.stderr.as_ref().map(|pipe| describe(pipe)),
        };
        self.children.insert(child.id, pending);
        Ok(Reply::Spawned { child })
    }
}

fn stdio(stream: Stream) -> std::process::Stdio {
    match stream {
        Stream::Inherit => std::process::Stdio::inherit(),
        Stream::Null => std::process::Stdio::null(),
        Stream::Pipe => std::process::Stdio::piped(),
    }
}

fn invalid(error: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

fn unknown_child() -> io::Error {
    invalid("native compiler child is not owned by this invocation")
}

#[cfg(test)]
mod tests {
    use super::*;
    use kache_protocol::execution::{NativeString, windows::Stdio};
    use std::os::windows::io::BorrowedHandle;
    use windows_sys::Win32::System::Threading::{GetExitCodeProcess, WaitForSingleObject};

    fn request(root: &std::path::Path, mode: &str) -> Spawn {
        let native = |value: &std::ffi::OsStr| NativeString::capture(value);
        let mut env = std::env::vars_os()
            .map(|(key, value)| (native(&key), native(&value)))
            .collect::<Vec<_>>();
        env.push((
            native("KACHE_BROKER_FIXTURE".as_ref()),
            native(mode.as_ref()),
        ));
        env.push((
            native("KACHE_BROKER_READY".as_ref()),
            native(root.join("ready").as_os_str()),
        ));
        Spawn {
            program: native(std::env::current_exe().unwrap().as_os_str()),
            args: [
                "--ignored",
                "--exact",
                "execution::windows_client::tests::native_child",
            ]
            .into_iter()
            .map(|arg| native(arg.as_ref()))
            .collect(),
            env,
            cwd: native(root.as_os_str()),
            stdio: Stdio::OUTPUT,
        }
    }

    #[test]
    #[ignore = "child process for the Windows native-launch ownership tests"]
    fn native_child() {
        std::fs::write(std::env::var_os("KACHE_BROKER_READY").unwrap(), b"ready").unwrap();
        if std::env::var("KACHE_BROKER_FIXTURE").unwrap() == "wait" {
            std::thread::sleep(std::time::Duration::from_secs(30));
        } else {
            std::io::stdout()
                .write_all(b"native-output-\xff\n")
                .unwrap();
            std::io::stderr().write_all(b"native-error-\xfe\n").unwrap();
        }
    }

    fn started(client: &mut Client, spawn: Spawn) -> Child {
        let Reply::Spawned { child } = client.execute(Command::Spawn(Box::new(spawn))) else {
            panic!("native child did not start");
        };
        child
    }

    #[test]
    fn daemon_duplicates_keep_captured_native_streams_after_client_release() {
        use std::io::Read;
        let root = tempfile::tempdir().unwrap();
        let mut client = Client::default();
        let child = started(&mut client, request(root.path(), "output"));
        let clone = |value: u64| {
            unsafe { BorrowedHandle::borrow_raw(value as usize as _) }
                .try_clone_to_owned()
                .unwrap()
        };
        let mut stdout = std::process::ChildStdout::from(clone(child.stdout.unwrap()));
        let mut stderr = std::process::ChildStderr::from(clone(child.stderr.unwrap()));
        assert_eq!(
            client.execute(Command::Release { id: child.id }),
            Reply::Done
        );
        let mut output = Vec::new();
        let mut error = Vec::new();
        stdout.read_to_end(&mut output).unwrap();
        stderr.read_to_end(&mut error).unwrap();
        client
            .children
            .get_mut(&child.id)
            .unwrap()
            .child
            .wait()
            .unwrap();
        assert!(
            output
                .windows(b"native-output-\xff".len())
                .any(|value| value == b"native-output-\xff")
        );
        assert!(
            error
                .windows(b"native-error-\xfe".len())
                .any(|value| value == b"native-error-\xfe")
        );
        assert_eq!(
            client.execute(Command::Finish { id: child.id }),
            Reply::Done
        );
        assert!(client.children.is_empty());
    }

    #[test]
    fn abort_reaps_a_started_child_before_acknowledging_transfer_failure() {
        let root = tempfile::tempdir().unwrap();
        let mut client = Client::default();
        let child = started(&mut client, request(root.path(), "wait"));
        let process = client
            .children
            .get(&child.id)
            .unwrap()
            .child
            .as_handle()
            .try_clone_to_owned()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !root.path().join("ready").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "native child did not start"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(
            unsafe { WaitForSingleObject(process.as_raw_handle(), 0) },
            windows_sys::Win32::Foundation::WAIT_TIMEOUT
        );
        assert_eq!(client.execute(Command::Abort { id: child.id }), Reply::Done);
        assert!(client.children.is_empty());
        assert_eq!(
            unsafe { WaitForSingleObject(process.as_raw_handle(), 0) },
            windows_sys::Win32::Foundation::WAIT_OBJECT_0
        );
        assert!(matches!(
            client.execute(Command::Release { id: child.id }),
            Reply::Failed { .. }
        ));
    }

    #[test]
    fn client_lifetime_kills_children_while_daemon_handles_remain_open() {
        let root = tempfile::tempdir().unwrap();
        let mut client = Client::default();
        let child = started(&mut client, request(root.path(), "wait"));
        let pending = client.children.get(&child.id).unwrap();
        let daemon_job = pending
            .cancellation
            .as_handle()
            .try_clone_to_owned()
            .unwrap();
        let daemon_process = pending.child.as_handle().try_clone_to_owned().unwrap();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !root.path().join("ready").exists() && std::time::Instant::now() < until {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(
            root.path().join("ready").exists(),
            "native child must actually be running"
        );
        drop(client);
        assert_eq!(
            unsafe { WaitForSingleObject(daemon_process.as_raw_handle(), 0) },
            0
        );
        let mut status = 0;
        assert_ne!(
            unsafe { GetExitCodeProcess(daemon_process.as_raw_handle(), &mut status) },
            0
        );
        assert_ne!(status, 259, "compiler must not remain active");
        drop(daemon_job);
    }
}
