//! Own handles received from a Windows named-pipe client. Execution admission
//! must separately preserve that client's security and process restrictions.

use kunobi_daemon::local::peer::PeerCredentials;
use kunobi_daemon::peer::UserEvidence;
use std::fs::File;
use std::io;
use std::os::windows::io::{AsHandle, AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle};
use windows_sys::Win32::Foundation::{
    DUPLICATE_SAME_ACCESS, DuplicateHandle, WAIT_FAILED, WAIT_TIMEOUT,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_TYPE_CHAR, FILE_TYPE_DISK, FILE_TYPE_PIPE, GetFileType,
};
use windows_sys::Win32::System::Console::GetConsoleMode;
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetProcessId, OpenProcess, PROCESS_DUP_HANDLE,
    PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
};

/// Pins the process opened for the PID reported by a connected pipe. A server
/// must retain the pipe and check execution policy before starting any work.
pub struct PeerProcess {
    process: OwnedHandle,
    pid: u32,
}

pub struct CompilerHandles {
    pub process: OwnedHandle,
    pub job: crate::windows_process::Job,
    pub stdin: Option<std::process::ChildStdin>,
    pub stdout: Option<std::process::ChildStdout>,
    pub stderr: Option<std::process::ChildStderr>,
}

impl PeerProcess {
    pub fn permits_delegation(&self) -> io::Result<bool> {
        self.require_alive()?;
        crate::windows_security::matches_daemon(self.process.as_handle())
    }
    /// Duplicate only from the pinned frontend, before acknowledging Release.
    pub fn duplicate_compiler(
        &self,
        child: &kache_protocol::execution::windows::Child,
    ) -> io::Result<CompilerHandles> {
        self.require_alive()?;
        if child.id == 0 || child.pid == 0 || child.pid == self.pid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid native compiler identity",
            ));
        }
        let process = self.duplicate_handle(child.process)?;
        if unsafe { GetProcessId(process.as_raw_handle()) } != child.pid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native compiler handle does not match its PID",
            ));
        }
        let job = crate::windows_process::Job::from_owned(self.duplicate_handle(child.job)?)?;
        let mut in_job = 0;
        if unsafe {
            windows_sys::Win32::System::JobObjects::IsProcessInJob(
                process.as_raw_handle(),
                job.as_handle().as_raw_handle(),
                &mut in_job,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if in_job == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "native compiler does not belong to its cancellation job",
            ));
        }
        let pipe = |value| -> io::Result<OwnedHandle> {
            let handle = self.duplicate_handle(value)?;
            if unsafe { GetFileType(handle.as_raw_handle()) } != FILE_TYPE_PIPE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "native compiler stream is not a pipe",
                ));
            }
            Ok(handle)
        };
        let stdin = child
            .stdin
            .map(pipe)
            .transpose()?
            .map(std::process::ChildStdin::from);
        let stdout = child
            .stdout
            .map(pipe)
            .transpose()?
            .map(std::process::ChildStdout::from);
        let stderr = child
            .stderr
            .map(pipe)
            .transpose()?
            .map(std::process::ChildStderr::from);
        self.require_alive()?;
        Ok(CompilerHandles {
            process,
            job,
            stdin,
            stdout,
            stderr,
        })
    }
    pub fn for_connection(stream: &impl PeerCredentials, reported_pid: u32) -> io::Result<Self> {
        require_peer(stream, reported_pid)?;
        let process = unsafe {
            OpenProcess(
                PROCESS_DUP_HANDLE | PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                reported_pid,
            )
        };
        if process.is_null() {
            return Err(io::Error::last_os_error());
        }
        let peer = Self {
            process: unsafe { OwnedHandle::from_raw_handle(process) },
            pid: reported_pid,
        };
        peer.verify_connection(stream)?;
        Ok(peer)
    }

    pub fn verify_connection(&self, stream: &impl PeerCredentials) -> io::Result<()> {
        require_peer(stream, self.pid)?;
        self.require_alive()
    }

    /// The client keeps the originals alive until Complete or disconnect.
    /// Every duplicate is non-inheritable and is closed on partial failure.
    pub fn duplicate_stdio(&self, handles: [u64; 3]) -> io::Result<[File; 3]> {
        self.require_alive()?;
        let files = [
            self.duplicate_file(handles[0])?,
            self.duplicate_file(handles[1])?,
            self.duplicate_file(handles[2])?,
        ];
        self.require_alive()?;
        Ok(files)
    }

    fn require_alive(&self) -> io::Result<()> {
        match unsafe { WaitForSingleObject(self.process.as_raw_handle(), 0) } {
            WAIT_TIMEOUT => Ok(()),
            WAIT_FAILED => Err(io::Error::last_os_error()),
            _ => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "compiler client process exited",
            )),
        }
    }

    fn duplicate_file(&self, value: u64) -> io::Result<File> {
        let file = File::from(self.duplicate_handle(value)?);
        let kind = unsafe { GetFileType(file.as_raw_handle()) };
        if !matches!(kind, FILE_TYPE_CHAR | FILE_TYPE_DISK | FILE_TYPE_PIPE) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "compiler stream is not a file or pipe",
            ));
        }
        let mut mode = 0;
        if kind == FILE_TYPE_CHAR && unsafe { GetConsoleMode(file.as_raw_handle(), &mut mode) } != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "compiler console requires local execution",
            ));
        }
        Ok(file)
    }

    fn duplicate_handle(&self, value: u64) -> io::Result<OwnedHandle> {
        let value = usize::try_from(value)
            .ok()
            .filter(|value| *value != 0 && isize::try_from(*value).is_ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid compiler stream handle",
                )
            })?;
        let mut duplicate = std::ptr::null_mut();
        if unsafe {
            DuplicateHandle(
                self.process.as_raw_handle(),
                value as _,
                GetCurrentProcess(),
                &mut duplicate,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { OwnedHandle::from_raw_handle(duplicate) })
    }
}

impl AsHandle for PeerProcess {
    fn as_handle(&self) -> BorrowedHandle<'_> {
        self.process.as_handle()
    }
}

fn require_peer(stream: &impl PeerCredentials, reported_pid: u32) -> io::Result<()> {
    let peer = stream.credentials()?;
    if reported_pid == 0
        || !peer.same_user
        || peer.user_evidence != UserEvidence::Connection
        || peer.pid.map(|pid| pid.get()) != Some(reported_pid)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "compiler handles require the connected same-user client",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use std::io::{Read, Seek, Write};
    use windows_sys::Win32::Foundation::{GetHandleInformation, HANDLE_FLAG_INHERIT};

    fn connected() -> (crate::SyncStream, crate::SyncStream) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("handles.sock");
        let listener = crate::ListenerOptions::new()
            .name(crate::socket_name(&path).unwrap())
            .create_sync()
            .unwrap();
        std::thread::scope(|scope| {
            let client = scope.spawn(|| {
                let mut client =
                    crate::SyncStream::connect(crate::socket_name(&path).unwrap()).unwrap();
                client.write_all(b"x").unwrap();
                client
            });
            let mut server = listener.accept().unwrap();
            // Reading a client message gives the pipe connection credentials.
            server.read_exact(&mut [0]).unwrap();
            (server, client.join().unwrap())
        })
    }

    #[test]
    fn connected_peer_owns_non_inheritable_streams_after_originals_close() {
        let (server, _client) = connected();
        let peer = PeerProcess::for_connection(&server, std::process::id()).unwrap();
        let mut input = tempfile::tempfile().unwrap();
        input.write_all(b"compiler input").unwrap();
        input.rewind().unwrap();
        let output = tempfile::tempfile().unwrap();
        let error = tempfile::tempfile().unwrap();
        let handles = [&input, &output, &error].map(|file| file.as_raw_handle() as usize as u64);
        let [mut received_input, mut received_output, mut received_error] =
            peer.duplicate_stdio(handles).unwrap();
        drop((input, output, error));
        for file in [&received_input, &received_output, &received_error] {
            let mut flags = 0;
            assert_ne!(
                unsafe { GetHandleInformation(file.as_raw_handle(), &mut flags) },
                0
            );
            assert_eq!(flags & HANDLE_FLAG_INHERIT, 0);
        }
        let mut bytes = Vec::new();
        received_input.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"compiler input");
        for (file, expected) in [
            (&mut received_output, b"stdout"),
            (&mut received_error, b"stderr"),
        ] {
            file.write_all(expected).unwrap();
            file.rewind().unwrap();
            bytes.clear();
            file.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, expected);
        }
        peer.verify_connection(&server).unwrap();
        assert_eq!(
            PeerProcess::for_connection(&server, std::process::id().wrapping_add(1))
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn pseudo_handles_and_non_files_are_rejected_without_closing_originals() {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::System::Threading::CreateEventW;
        let (server, _client) = connected();
        let peer = PeerProcess::for_connection(&server, std::process::id()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("exclusive");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .share_mode(0)
            .open(&path)
            .unwrap();
        let handle = file.as_raw_handle() as usize as u64;
        for invalid in [0, u64::MAX, u64::MAX - 1, u64::MAX - 5] {
            assert!(peer.duplicate_stdio([handle, handle, invalid]).is_err());
            file.write_all(b"still open").unwrap();
        }
        let event = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
        assert!(!event.is_null());
        let event = unsafe { OwnedHandle::from_raw_handle(event) };
        assert!(
            peer.duplicate_stdio([handle, handle, event.as_raw_handle() as usize as u64])
                .is_err()
        );
        let mut flags = 0;
        assert_ne!(
            unsafe { GetHandleInformation(event.as_raw_handle(), &mut flags) },
            0
        );
        drop(file);
        // Any leaked duplicate from a partial transfer retains the exclusive
        // file object and prevents a second independent open.
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .share_mode(0)
            .open(&path)
            .unwrap();
    }

    #[test]
    #[ignore = "native child for the compiler-handle admission test"]
    fn compiler_handle_fixture() {
        std::thread::sleep(std::time::Duration::from_secs(30));
    }

    #[test]
    fn compiler_handle_transfer_requires_the_matching_pid_and_job() {
        use crate::windows_process::Job;
        struct Fixture {
            child: std::process::Child,
            job: Job,
        }
        impl Drop for Fixture {
            fn drop(&mut self) {
                let _ = self.job.kill();
                let _ = self.child.wait();
            }
        }
        let (server, _client) = connected();
        let peer = PeerProcess::for_connection(&server, std::process::id()).unwrap();
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--ignored",
                "--exact",
                "windows_handles::tests::compiler_handle_fixture",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let (child, job) = crate::windows_process::spawn(&mut command).unwrap();
        let fixture = Fixture { child, job };
        let mut packet = kache_protocol::execution::windows::Child {
            id: 1,
            pid: fixture.child.id(),
            process: fixture.child.as_raw_handle() as usize as u64,
            job: fixture.job.as_handle().as_raw_handle() as usize as u64,
            stdin: None,
            stdout: None,
            stderr: None,
        };
        let received = peer.duplicate_compiler(&packet).unwrap();
        assert_eq!(
            unsafe { GetProcessId(received.process.as_raw_handle()) },
            fixture.child.id()
        );
        let unrelated = Job::new().unwrap();
        packet.job = unrelated.as_handle().as_raw_handle() as usize as u64;
        assert_eq!(
            peer.duplicate_compiler(&packet).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        packet.job = fixture.job.as_handle().as_raw_handle() as usize as u64;
        packet.pid = std::process::id();
        assert_eq!(
            peer.duplicate_compiler(&packet).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
        packet.pid = fixture.child.id().wrapping_add(1);
        assert_eq!(
            peer.duplicate_compiler(&packet).err().unwrap().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
