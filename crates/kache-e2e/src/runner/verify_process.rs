//! Deadline-bound verification process ownership and stdout capture.

use std::io;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::{io::Read, process::ChildStdout};
#[cfg(windows)]
use windows::{read_available, spawn};

pub(super) struct Output {
    pub exit_code: i32,
    pub stdout: Vec<u8>,
    pub failure_reason: Option<String>,
}

struct Process {
    child: Child,
    #[cfg(windows)]
    job: Option<std::os::windows::io::OwnedHandle>,
}

impl Process {
    fn terminate(&mut self) -> io::Result<()> {
        #[cfg(unix)]
        {
            if unsafe { libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL) } != 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
            }
        }
        #[cfg(windows)]
        windows::terminate(self.job.as_ref().expect("owned verifier job"))?;
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.terminate();
        #[cfg(windows)]
        drop(self.job.take()); // Kill-on-close also covers termination failure.
        // Killing an unreaped child of ours cannot fail, so the wait is bounded.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub(super) fn run(mut command: Command, timeout_s: u64) -> io::Result<Output> {
    let started = Instant::now();
    let timeout = Duration::from_secs(timeout_s);
    command.stdout(Stdio::piped());
    let mut process = spawn(command)?;
    let mut pipe = process.child.stdout.take().expect("piped verifier stdout");
    #[cfg(unix)]
    make_nonblocking(std::os::fd::AsRawFd::as_raw_fd(&pipe))?;
    let mut stdout = Vec::new();
    let mut buffer = [0; 4096];
    let mut eof = false;
    loop {
        if started.elapsed() >= timeout {
            let mut reason = format!("timed out after {timeout_s} seconds");
            if let Err(error) = process.terminate() {
                reason.push_str(&format!("; cleanup failed: {error}"));
            }
            return Ok(Output {
                exit_code: -1,
                stdout,
                failure_reason: Some(reason),
            });
        }
        let read = if eof {
            None
        } else {
            read_available(&mut pipe, &mut buffer)?
        };
        match read {
            Some(0) => eof = true,
            Some(n) => {
                // More output may be ready: read again before any pause.
                stdout.extend_from_slice(&buffer[..n]);
                continue;
            }
            None => {}
        }
        if let Some(status) = process.child.try_wait()?
            && eof
        {
            return Ok(Output {
                exit_code: status.code().unwrap_or(-1),
                stdout,
                failure_reason: None,
            });
        }
        std::thread::sleep(
            Duration::from_millis(10).min(timeout.saturating_sub(started.elapsed())),
        );
    }
}

#[cfg(unix)]
fn spawn(mut command: Command) -> io::Result<Process> {
    use std::os::unix::process::CommandExt;
    command.process_group(0);
    Ok(Process {
        child: command.spawn()?,
    })
}

/// One `FIONBIO` call sets `O_NONBLOCK` and keeps the other status flags.
#[cfg(unix)]
pub(super) fn make_nonblocking(fd: std::os::fd::RawFd) -> io::Result<()> {
    let mut nonblocking: libc::c_int = 1;
    if unsafe { libc::ioctl(fd, libc::FIONBIO, &mut nonblocking) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn read_available(
    pipe: &mut ChildStdout,
    buffer: &mut [u8],
) -> io::Result<Option<usize>> {
    match pipe.read(buffer) {
        Ok(n) => Ok(Some(n)),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Windows job-object ownership and pipe polling. Kept in one module so the
/// Linux mutation lane, which never compiles it, can exclude it by path.
#[cfg(windows)]
mod windows {
    use super::Process;
    use std::io::{self, Read};
    use std::process::{ChildStdout, Command};

    pub(super) fn terminate(job: &std::os::windows::io::OwnedHandle) -> io::Result<()> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::JobObjects::TerminateJobObject;
        if unsafe { TerminateJobObject(job.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn read_available(
        pipe: &mut ChildStdout,
        buffer: &mut [u8],
    ) -> io::Result<Option<usize>> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::ERROR_BROKEN_PIPE;
        use windows_sys::Win32::System::Pipes::PeekNamedPipe;
        let mut available = 0;
        if unsafe {
            PeekNamedPipe(
                pipe.as_raw_handle(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                &mut available,
                std::ptr::null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) {
                Ok(Some(0))
            } else {
                Err(error)
            };
        }
        if available == 0 {
            return Ok(None);
        }
        let count = buffer.len().min(available as usize);
        pipe.read(&mut buffer[..count]).map(Some)
    }

    pub(super) fn spawn(mut command: Command) -> io::Result<Process> {
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };
        use windows_sys::Win32::System::Threading::CREATE_SUSPENDED;
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        // Enroll before any verifier code runs, including code that exits its
        // parent or starts descendants. Stable CommandExt has no job-list API.
        command.creation_flags(CREATE_SUSPENDED);
        let child = command.spawn()?;
        let process = Process {
            child,
            job: Some(job),
        };
        if unsafe {
            AssignProcessToJobObject(
                process.job.as_ref().unwrap().as_raw_handle(),
                process.child.as_raw_handle(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        resume_primary_thread(process.child.id())?;
        Ok(process)
    }

    fn resume_primary_thread(pid: u32) -> io::Result<()> {
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
        use windows_sys::Win32::Foundation::{ERROR_NO_MORE_FILES, INVALID_HANDLE_VALUE};
        use windows_sys::Win32::System::Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
        };
        use windows_sys::Win32::System::Threading::{
            OpenThread, ResumeThread, THREAD_SUSPEND_RESUME,
        };
        let raw = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if raw == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let snapshot = unsafe { OwnedHandle::from_raw_handle(raw) };
        // Every field is named: cargo-mutants deletes fields from a literal
        // with `..Default::default()`, and its exclude_re does not apply to them.
        let mut entry = THREADENTRY32 {
            dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
            cntUsage: 0,
            th32ThreadID: 0,
            th32OwnerProcessID: 0,
            tpBasePri: 0,
            tpDeltaPri: 0,
            dwFlags: 0,
        };
        if unsafe { Thread32First(snapshot.as_raw_handle(), &mut entry) } == 0 {
            return Err(io::Error::last_os_error());
        }
        loop {
            if entry.th32OwnerProcessID == pid {
                let raw = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if raw.is_null() {
                    return Err(io::Error::last_os_error());
                }
                let thread = unsafe { OwnedHandle::from_raw_handle(raw) };
                return if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                };
            }
            if unsafe { Thread32Next(snapshot.as_raw_handle(), &mut entry) } == 0 {
                let error = io::Error::last_os_error();
                return if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "suspended verifier has no primary thread",
                    ))
                } else {
                    Err(error)
                };
            }
        }
    }
}
