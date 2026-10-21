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
        peer: Arc<PeerProcess>,
        env: InvocationEnv,
    ) -> Self {
        Self { sender, peer, env }
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
                self.done(Command::Abort { id: child.id })?;
                Err(error)
            }
        }
    }

    pub(crate) fn finish(&self, id: u64) {
        let _ = self.sender.blocking_send(Call {
            command: Command::Finish { id },
            result: None,
        });
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

    #[test]
    fn a_failed_native_handle_transfer_is_aborted_before_spawn_returns() {
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
        let broker = Broker::new(sender, peer, env);
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
            abort.result.unwrap().send(Ok(Reply::Done)).unwrap();
        });
        let result = broker.spawn(
            &ProcessCommand::new(std::env::current_exe().unwrap()),
            Stdio::OUTPUT,
        );
        drop(broker);
        assert!(result.is_err());
        frontend.join().unwrap();
        assert!(ready.exists(), "the child ran before the transfer failed");
    }
}
