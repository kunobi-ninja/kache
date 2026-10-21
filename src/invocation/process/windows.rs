//! Windows compiler process and cancellation-job ownership.

pub(super) use kache_ipc::windows_process::{Job, spawn};

use crate::invocation::windows_broker::Broker;
use kache_ipc::windows_handles::CompilerHandles;
use std::io;
use std::os::windows::io::{AsRawHandle, OwnedHandle, RawHandle};
use std::os::windows::process::ExitStatusExt;
use std::process::{ChildStderr, ChildStdin, ChildStdout, ExitStatus};
use std::sync::Arc;

pub(super) fn wait_for_exit(handle: RawHandle) -> io::Result<()> {
    if unsafe { windows_sys::Win32::System::Threading::WaitForSingleObject(handle, u32::MAX) }
        == windows_sys::Win32::Foundation::WAIT_FAILED
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

enum Process {
    Local(std::process::Child),
    Remote {
        handle: OwnedHandle,
        id: u64,
        pid: u32,
        broker: Arc<Broker>,
    },
}

pub(super) struct Child {
    process: Process,
    pub(super) stdin: Option<ChildStdin>,
    pub(super) stdout: Option<ChildStdout>,
    pub(super) stderr: Option<ChildStderr>,
}

impl Child {
    pub(super) fn local(mut child: std::process::Child) -> Self {
        Self {
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
            process: Process::Local(child),
        }
    }

    pub(super) fn remote(
        id: u64,
        pid: u32,
        handles: CompilerHandles,
        broker: Arc<Broker>,
    ) -> (Self, Job) {
        (
            Self {
                process: Process::Remote {
                    handle: handles.process,
                    id,
                    pid,
                    broker,
                },
                stdin: handles.stdin,
                stdout: handles.stdout,
                stderr: handles.stderr,
            },
            handles.job,
        )
    }

    pub(super) fn id(&self) -> u32 {
        match &self.process {
            Process::Local(child) => child.id(),
            Process::Remote { pid, .. } => *pid,
        }
    }

    pub(super) fn kill(&mut self) -> io::Result<()> {
        match &mut self.process {
            Process::Local(child) => child.kill(),
            Process::Remote { handle, .. } => {
                if unsafe {
                    windows_sys::Win32::System::Threading::TerminateProcess(
                        handle.as_raw_handle(),
                        1,
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            }
        }
    }

    pub(super) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match &mut self.process {
            Process::Local(child) => child.try_wait(),
            Process::Remote { handle, .. } => wait(handle, 0),
        }
    }

    pub(super) fn wait(&mut self) -> io::Result<ExitStatus> {
        match &mut self.process {
            Process::Local(child) => child.wait(),
            Process::Remote { handle, .. } => wait(handle, u32::MAX)?
                .ok_or_else(|| io::Error::other("compiler wait did not complete")),
        }
    }
}

impl AsRawHandle for Child {
    fn as_raw_handle(&self) -> RawHandle {
        match &self.process {
            Process::Local(child) => child.as_raw_handle(),
            Process::Remote { handle, .. } => handle.as_raw_handle(),
        }
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if let Process::Remote { id, broker, .. } = &self.process {
            broker.finish(*id);
        }
    }
}

fn wait(handle: &OwnedHandle, timeout: u32) -> io::Result<Option<ExitStatus>> {
    use windows_sys::Win32::Foundation::{WAIT_OBJECT_0, WAIT_TIMEOUT};
    use windows_sys::Win32::System::Threading::{GetExitCodeProcess, WaitForSingleObject};
    match unsafe { WaitForSingleObject(handle.as_raw_handle(), timeout) } {
        WAIT_TIMEOUT => Ok(None),
        WAIT_OBJECT_0 => {
            let mut status = 0;
            if unsafe { GetExitCodeProcess(handle.as_raw_handle(), &mut status) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Some(ExitStatus::from_raw(status)))
        }
        _ => Err(io::Error::last_os_error()),
    }
}
