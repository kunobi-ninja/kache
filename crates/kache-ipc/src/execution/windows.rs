//! Named-pipe setup has one deadline; committed compilation waits in the kernel.

use super::{PREPARE_TIMEOUT, retry_until};
use crate::prelude::*;
use anyhow::Result;
use std::io::{self, Read, Write};
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::Path;
use windows_sys::Win32::Foundation::{ERROR_PIPE_BUSY, WAIT_TIMEOUT};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_OVERLAPPED, FILE_SHARE_READ, FILE_SHARE_WRITE, SECURITY_IDENTIFICATION,
};
use windows_sys::Win32::System::Threading::{
    OpenProcess, PROCESS_DUP_HANDLE, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    WaitForSingleObject,
};

pub(super) const MAX_REPLY_BYTES: u64 = 8 << 20;

pub(super) struct Connection {
    pub(super) inner: crate::SyncStream,
    pub(super) deadline: std::cell::Cell<Option<std::time::Instant>>,
    peer: PeerSecurity,
    write_capacity: usize,
}

/// Pin the peer before trusting its process-creation requests. Credentials
/// for the server use the kernel pipe PID and a process lookup, rather than the
/// connection-bound client SID required when duplicating client handles.
struct PeerSecurity {
    process: OwnedHandle,
    pid: u32,
    trusted_pipeline: bool,
}

impl PeerSecurity {
    fn open(stream: &crate::SyncStream, trusted_pipeline: bool) -> io::Result<Self> {
        use kunobi_daemon::local::peer::PeerCredentials as _;
        let peer = stream.credentials()?;
        let pid = peer
            .pid
            .map(|pid| pid.get())
            .filter(|_| peer.same_user)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "compiler endpoint has no same-user process identity",
                )
            })?;
        let process = unsafe {
            OpenProcess(
                PROCESS_DUP_HANDLE | PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if process.is_null() {
            return Err(io::Error::last_os_error());
        }
        let peer = Self {
            process: unsafe { OwnedHandle::from_raw_handle(process) },
            pid,
            trusted_pipeline,
        };
        peer.verify(stream)?;
        Ok(peer)
    }

    fn verify(&self, stream: &crate::SyncStream) -> io::Result<()> {
        use kunobi_daemon::local::peer::PeerCredentials as _;
        let peer = stream.credentials()?;
        if !peer.same_user || peer.pid.map(|pid| pid.get()) != Some(self.pid) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "compiler endpoint identity changed",
            ));
        }
        if unsafe { WaitForSingleObject(self.process.as_raw_handle(), 0) } != WAIT_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "compiler endpoint process exited",
            ));
        }
        if !crate::windows_security::matches_daemon(
            self.process.as_handle(),
            self.trusted_pipeline,
        )? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "compiler endpoint security context differs",
            ));
        }
        Ok(())
    }
}

impl Connection {
    pub(super) fn connect(socket: &Path, trusted_pipeline: bool) -> Result<Self> {
        use interprocess::os::windows::named_pipe::{DuplexPipeStream, pipe_mode::Bytes};
        let deadline = std::time::Instant::now() + PREPARE_TIMEOUT;
        let path = format!(r"\\.\pipe\{}", crate::daemon_endpoint(socket));
        // Identification lets the server inspect our SID without acting with
        // our token, including while its identity is still being checked.
        let handle = retry_until(Some(deadline), || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_OVERLAPPED)
                .security_qos_flags(SECURITY_IDENTIFICATION)
                .open(&path)
                .map(OwnedHandle::from)
                .map_err(|error| {
                    if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) {
                        io::ErrorKind::WouldBlock.into()
                    } else {
                        error
                    }
                })
        })?;
        let pipe = DuplexPipeStream::<Bytes>::try_from(handle).map_err(io::Error::from)?;
        let inner = interprocess::os::windows::named_pipe::local_socket::Stream::from(pipe);
        Self::from_stream(inner.into(), deadline, trusted_pipeline)
    }

    pub(super) fn from_stream(
        inner: crate::SyncStream,
        deadline: std::time::Instant,
        trusted_pipeline: bool,
    ) -> Result<Self> {
        let peer = PeerSecurity::open(&inner, trusted_pipeline)?;
        use windows_sys::Win32::System::Pipes::{GetNamedPipeInfo, PIPE_SERVER_END};
        let crate::SyncStream::NamedPipe(pipe) = &inner;
        let (mut flags, mut outbound, mut inbound) = (0, 0, 0);
        if unsafe {
            GetNamedPipeInfo(
                pipe.as_handle().as_raw_handle(),
                &mut flags,
                &mut outbound,
                &mut inbound,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error().into());
        }
        let write_capacity = if flags & PIPE_SERVER_END != 0 {
            outbound
        } else {
            inbound
        };
        anyhow::ensure!(write_capacity > 0, "compiler pipe has no write buffer");
        inner.set_nonblocking(true)?;
        Ok(Self {
            inner,
            deadline: std::cell::Cell::new(Some(deadline)),
            peer,
            write_capacity: write_capacity as usize,
        })
    }

    pub(super) fn contents(&self, value: u64) -> io::Result<std::fs::File> {
        use windows_sys::Win32::Foundation::{DUPLICATE_SAME_ACCESS, DuplicateHandle};
        use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, GetFileType};
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        self.peer.verify(&self.inner)?;
        let value = usize::try_from(value)
            .ok()
            .filter(|value| *value != 0 && isize::try_from(*value).is_ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid restore contents handle",
                )
            })?;
        let mut handle = std::ptr::null_mut();
        if unsafe {
            DuplicateHandle(
                self.peer.process.as_raw_handle(),
                value as _,
                GetCurrentProcess(),
                &mut handle,
                0,
                0,
                DUPLICATE_SAME_ACCESS,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let file = std::fs::File::from(unsafe { OwnedHandle::from_raw_handle(handle) });
        if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK
            || !file.metadata()?.is_file()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "restore contents must be a regular file",
            ));
        }
        Ok(file)
    }

    pub(super) fn prepare_commit(&self) -> Result<()> {
        self.peer.verify(&self.inner)?;
        crate::windows_process::protect_current_process()?;
        Ok(())
    }

    pub(super) fn commit_written(&self) -> Result<()> {
        // This fallible transition is after the first Commit write attempt.
        // A failure never permits a second local compiler run.
        self.inner.set_nonblocking(false)?;
        self.deadline.set(None);
        Ok(())
    }
}

impl Read for &Connection {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        use std::os::windows::io::{AsHandle, AsRawHandle};
        use windows_sys::Win32::System::Pipes::PeekNamedPipe;
        let deadline = self.deadline.get();
        if deadline.is_none() || bytes.is_empty() {
            return (&self.inner).read(bytes);
        }
        retry_until(deadline, || {
            let crate::SyncStream::NamedPipe(pipe) = &self.inner;
            let mut available = 0;
            if unsafe {
                PeekNamedPipe(
                    pipe.as_handle().as_raw_handle(),
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    &mut available,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                return Err(std::io::Error::last_os_error());
            }
            if available == 0 {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            // One reader owns the stream; queued bytes cannot disappear
            // between the peek and this bounded read.
            let count = bytes.len().min(available as usize);
            (&self.inner).read(&mut bytes[..count])
        })
    }
}

impl Write for &Connection {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // A nonblocking pipe can return zero for a frame larger than its
        // buffer even while empty. Send bounded pieces so the peer can read.
        let bytes = &bytes[..bytes.len().min(self.write_capacity)];
        retry_until(self.deadline.get(), || match (&self.inner).write(bytes) {
            // PIPE_NOWAIT may successfully write zero bytes when full.
            Ok(0) if !bytes.is_empty() => Err(std::io::ErrorKind::WouldBlock.into()),
            result => result,
        })
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (&self.inner).flush()
    }
}
