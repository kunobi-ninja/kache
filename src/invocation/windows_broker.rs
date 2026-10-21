//! Blocking pipeline requests use the existing async execution connection.

use crate::invocation::InvocationEnv;
use kache_ipc::windows_handles::{CompilerHandles, PeerProcess};
use kache_protocol::execution::{
    NativeString,
    windows::{Command, Output, Reply, Spawn, Stdio, output_chunks},
};
use std::io;
use std::process::Command as ProcessCommand;
use std::sync::{Arc, mpsc};

pub(crate) struct Call {
    pub(crate) command: Command,
    pub(crate) result: Option<mpsc::SyncSender<io::Result<Reply>>>,
}

pub(crate) struct Broker {
    sender: tokio::sync::mpsc::Sender<Call>,
    finished: tokio::sync::mpsc::UnboundedSender<u64>,
    peer: Arc<PeerProcess>,
    env: InvocationEnv,
}

impl Broker {
    pub(crate) fn discover_msvc(
        &self,
        request: kache_protocol::execution::windows::Discovery,
    ) -> io::Result<kache_protocol::execution::windows::Msvc> {
        match self.call(Command::DiscoverMsvc(request))? {
            Reply::Msvc { tools } => Ok(tools),
            _ => Err(io::Error::other("invalid MSVC discovery response")),
        }
    }
    pub(crate) fn new(
        sender: tokio::sync::mpsc::Sender<Call>,
        finished: tokio::sync::mpsc::UnboundedSender<u64>,
        peer: Arc<PeerProcess>,
        env: InvocationEnv,
    ) -> Self {
        Self {
            sender,
            finished,
            peer,
            env,
        }
    }

    pub(crate) fn spawn(
        &self,
        command: &ProcessCommand,
        stdio: Stdio,
    ) -> io::Result<(u64, u32, CompilerHandles)> {
        let cwd = command
            .get_current_dir()
            .or(self.env.cwd())
            .ok_or_else(|| io::Error::other("compiler command has no invocation directory"))?;
        let spawn = Spawn {
            program: NativeString::capture(command.get_program()),
            args: command.get_args().map(NativeString::capture).collect(),
            env: command
                .get_envs()
                .filter_map(|(name, value)| {
                    value.map(|value| (NativeString::capture(name), NativeString::capture(value)))
                })
                .collect(),
            cwd: NativeString::capture(cwd.as_os_str()),
            stdio,
        };
        let Reply::Spawned { child } = self.call(Command::Spawn(Box::new(spawn)))? else {
            return Err(io::Error::other(
                "frontend did not return a native compiler",
            ));
        };
        let transfer = || -> io::Result<CompilerHandles> {
            let handles = self.peer.duplicate_compiler(&child)?;
            self.done(Command::Release { id: child.id })?;
            Ok(handles)
        };
        match transfer() {
            Ok(handles) => Ok((child.id, child.pid, handles)),
            Err(error) => {
                // The frontend resumed the child before describing its handles.
                // Complete the rollback before another operation can start.
                if let Err(cleanup) = self.done(Command::Abort { id: child.id }) {
                    return Err(io::Error::other(format!(
                        "native handle transfer failed: {error}; abort failed: {cleanup}"
                    )));
                }
                Err(error)
            }
        }
    }

    pub(crate) fn finish(&self, id: u64) {
        // The last process owner can drop on an async cancellation task.
        // Cleanup must not block that task or compete for a full RPC queue.
        let _ = self.finished.send(id);
    }

    pub(crate) fn write(&self, output: Output, bytes: &[u8]) -> io::Result<()> {
        for chunk in output_chunks(bytes) {
            self.done(Command::Write {
                output,
                bytes: chunk.to_owned(),
            })?;
        }
        Ok(())
    }

    pub(crate) fn flush(&self, output: Output) -> io::Result<()> {
        self.done(Command::Flush { output })
    }

    fn done(&self, command: Command) -> io::Result<()> {
        match self.call(command)? {
            Reply::Done => Ok(()),
            _ => Err(io::Error::other("invalid native compiler acknowledgement")),
        }
    }

    fn call(&self, command: Command) -> io::Result<Reply> {
        let (sender, receiver) = mpsc::sync_channel(1);
        self.sender
            .blocking_send(Call {
                command,
                result: Some(sender),
            })
            .map_err(|_| disconnected())?;
        let reply = receiver.recv().map_err(|_| disconnected())??;
        match reply {
            Reply::Failed {
                os_error: Some(code),
                ..
            } => Err(io::Error::from_raw_os_error(code)),
            Reply::Failed { message, .. } => Err(io::Error::other(message)),
            reply => Ok(reply),
        }
    }
}

fn disconnected() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "compiler frontend disconnected")
}

#[cfg(test)]
mod tests {
    use super::*;
    use kache_ipc::prelude::*;
    use std::io::{Read, Write};
    use std::os::windows::io::{AsHandle, AsRawHandle};
    use std::path::Path;
    use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::WaitForSingleObject;

    #[test]
    #[ignore = "native compiler for incomplete handle-transfer rollback"]
    fn transfer_child() {
        std::fs::write(std::env::var_os("KACHE_TRANSFER_READY").unwrap(), b"ready").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(30));
    }

    fn connection(root: &Path) -> (kache_ipc::SyncStream, kache_ipc::SyncStream) {
        let socket = root.join("transfer");
        let listener = kache_ipc::ListenerOptions::new()
            .name(kache_ipc::socket_name(&socket).unwrap())
            .create_sync()
            .unwrap();
        std::thread::scope(|scope| {
            let server = scope.spawn(|| listener.accept().unwrap());
            let mut client =
                kache_ipc::SyncStream::connect(kache_ipc::socket_name(&socket).unwrap()).unwrap();
            client.write_all(b"ready").unwrap();
            let mut server = server.join().unwrap();
            let mut bytes = [0u8; 5];
            server.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"ready");
            (client, server)
        })
    }

    #[tokio::test]
    async fn async_cleanup_survives_a_full_rpc_queue() {
        let root = tempfile::tempdir().unwrap();
        let (_client, server) = connection(root.path());
        let peer = Arc::new(PeerProcess::for_connection(&server, std::process::id()).unwrap());
        let (sender, _calls) = tokio::sync::mpsc::channel(1);
        assert!(
            sender
                .try_send(Call {
                    command: Command::Flush {
                        output: Output::Stderr
                    },
                    result: None
                })
                .is_ok()
        );
        assert_eq!(sender.capacity(), 0);
        let (finished, mut cleanup) = tokio::sync::mpsc::unbounded_channel();
        let env = InvocationEnv::from_parts(
            Vec::<(std::ffi::OsString, std::ffi::OsString)>::new(),
            Some(root.path().to_owned()),
            None,
            root.path().to_owned(),
        );
        let broker = Broker::new(sender, finished, peer, env);
        broker.finish(7);
        assert_eq!(cleanup.try_recv().unwrap(), 7);
        assert!(cleanup.try_recv().is_err());
    }

    #[test]
    fn a_failed_native_handle_transfer_is_aborted_before_spawn_returns() {
        failed_native_transfer(false);
    }

    #[test]
    fn an_abort_error_preserves_the_native_handle_transfer_error() {
        failed_native_transfer(true);
    }

    fn failed_native_transfer(abort_error: bool) {
        let root = tempfile::tempdir().unwrap();
        let (_client, server) = connection(root.path());
        let peer = Arc::new(PeerProcess::for_connection(&server, std::process::id()).unwrap());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let env = InvocationEnv::from_parts(
            Vec::<(std::ffi::OsString, std::ffi::OsString)>::new(),
            Some(root.path().to_owned()),
            None,
            root.path().to_owned(),
        );
        let (finished, _finished) = tokio::sync::mpsc::unbounded_channel();
        let broker = Broker::new(sender, finished, peer, env);
        let ready = root.path().join("ready");
        let child_ready = ready.clone();
        let frontend = std::thread::spawn(move || {
            let call = receiver.blocking_recv().expect("native spawn request");
            assert!(matches!(call.command, Command::Spawn(_)));
            let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "invocation::windows_broker::tests::transfer_child",
                ])
                .env("KACHE_TRANSFER_READY", &child_ready)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null());
            let (mut child, lifetime, cancellation) =
                kache_ipc::windows_process::spawn_with_lifetime(&mut command).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !child_ready.exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "native compiler did not start"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert_eq!(
                unsafe { WaitForSingleObject(child.as_raw_handle(), 0) },
                WAIT_TIMEOUT
            );
            let metadata = kache_protocol::execution::windows::Child {
                id: 7,
                pid: child.id(),
                process: child.as_raw_handle() as usize as u64,
                job: cancellation.as_handle().as_raw_handle() as usize as u64,
                stdin: None,
                // The running child exists, but this stream cannot be duplicated.
                stdout: Some(0),
                stderr: None,
            };
            call.result
                .unwrap()
                .send(Ok(Reply::Spawned { child: metadata }))
                .unwrap();
            let abort = receiver
                .blocking_recv()
                .expect("incomplete transfer must be aborted");
            assert_eq!(abort.command, Command::Abort { id: 7 });
            lifetime.kill().unwrap();
            child.wait().unwrap();
            assert_eq!(
                unsafe { WaitForSingleObject(child.as_raw_handle(), 0) },
                WAIT_OBJECT_0
            );
            let reply = if abort_error {
                Err(io::Error::other("fixture abort acknowledgement failed"))
            } else {
                Ok(Reply::Done)
            };
            abort.result.unwrap().send(reply).unwrap();
        });
        let result = broker.spawn(
            &ProcessCommand::new(std::env::current_exe().unwrap()),
            Stdio::OUTPUT,
        );
        drop(broker);
        let error = result
            .err()
            .expect("invalid stream handle must be rejected");
        assert!(error.to_string().contains("invalid compiler stream handle"));
        if abort_error {
            assert!(
                error
                    .to_string()
                    .contains("fixture abort acknowledgement failed")
            );
        } else {
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
        frontend.join().unwrap();
        assert!(ready.exists(), "the child ran before the transfer failed");
    }
}
