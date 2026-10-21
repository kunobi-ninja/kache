//! Named-pipe setup has one deadline; committed compilation waits in the kernel.

use super::{PREPARE_TIMEOUT, retry_until};
use crate::prelude::*;
use anyhow::{Result, ensure};
use std::io::{Read, Write};
use std::path::Path;

pub(super) struct Connection {
    pub(super) inner: crate::SyncStream,
    pub(super) deadline: std::cell::Cell<Option<std::time::Instant>>,
}

impl Connection {
    pub(super) fn connect(socket: &Path) -> Result<Self> {
        use interprocess::os::windows::named_pipe::{DuplexPipeStream, pipe_mode::Bytes};
        use kunobi_daemon::local::peer::PeerCredentials as _;
        let deadline = std::time::Instant::now() + PREPARE_TIMEOUT;
        // interprocess 2.4.4's generic local-socket connector ignores
        // ConnectOptions::wait_mode. Wrap the same bounded native pipe.
        let path = format!(r"\\.\pipe\{}", crate::daemon_endpoint(socket));
        let pipe = DuplexPipeStream::<Bytes>::connect_by_path_with_wait_mode(
            path,
            interprocess::ConnectWaitMode::Timeout(PREPARE_TIMEOUT),
        )?;
        let inner = interprocess::os::windows::named_pipe::local_socket::Stream::from(pipe);
        let inner: crate::SyncStream = inner.into();
        let peer = inner.credentials()?;
        ensure!(
            peer.same_user && peer.pid.is_some(),
            "compiler endpoint belongs to another user or has no peer identity"
        );
        inner.set_nonblocking(true)?;
        Ok(Self {
            inner,
            deadline: std::cell::Cell::new(Some(deadline)),
        })
    }

    pub(super) fn prepare_commit(&self) -> Result<()> {
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
