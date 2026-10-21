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
        let handles = self.peer.duplicate_compiler(&child)?;
        self.done(Command::Release { id: child.id })?;
        Ok((child.id, child.pid, handles))
    }

    pub(crate) fn finish(&self, id: u64) {
        let _ = self.sender.try_send(Call {
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
