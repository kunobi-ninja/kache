//! Unix socket setup and timeout transitions for compiler handoff.

use super::PREPARE_TIMEOUT;
use crate::prelude::*;
use anyhow::Result;
use interprocess::local_socket::traits::StreamCommon as _;
use std::io::{Read, Write};
use std::path::Path;

pub(super) struct Connection {
    inner: crate::SyncStream,
}

#[cfg(target_os = "linux")]
impl std::os::fd::AsFd for Connection {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        let crate::SyncStream::UdSocket(socket) = &self.inner;
        socket.inner().as_fd()
    }
}

impl Connection {
    pub(super) fn connect(socket: &Path) -> Result<Self> {
        let inner = crate::SyncStream::connect(crate::socket_name(socket)?)?;
        crate::require_self_peer(inner.peer_creds()?.euid())?;
        inner.set_recv_timeout(Some(PREPARE_TIMEOUT))?;
        inner.set_send_timeout(Some(PREPARE_TIMEOUT))?;
        Ok(Self { inner })
    }

    pub(super) fn prepare_commit(&self) -> Result<()> {
        self.inner.set_recv_timeout(None)?;
        Ok(())
    }
}

impl Read for &Connection {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        (&self.inner).read(bytes)
    }
}

impl Write for &Connection {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        (&self.inner).write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (&self.inner).flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;

    fn timeout(connection: &Connection, option: libc::c_int) -> std::time::Duration {
        let crate::SyncStream::UdSocket(socket) = &connection.inner;
        let mut value: libc::timeval = unsafe { std::mem::zeroed() };
        let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
        assert_eq!(
            unsafe {
                libc::getsockopt(
                    socket.inner().as_raw_fd(),
                    libc::SOL_SOCKET,
                    option,
                    (&mut value as *mut libc::timeval).cast(),
                    &mut length,
                )
            },
            0
        );
        assert!(value.tv_sec >= 0 && value.tv_usec >= 0);
        std::time::Duration::from_secs(value.tv_sec as u64)
            + std::time::Duration::from_micros(value.tv_usec as u64)
    }

    #[test]
    fn commit_removes_the_setup_read_deadline_for_long_compiler_runs() {
        let root = tempfile::tempdir_in("/tmp").unwrap();
        let path = root.path().join("control");
        let _listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let connection = Connection::connect(&path).unwrap();
        assert_eq!(
            timeout(&connection, libc::SO_RCVTIMEO),
            std::time::Duration::from_secs(5)
        );
        connection.prepare_commit().unwrap();
        assert_eq!(
            timeout(&connection, libc::SO_RCVTIMEO),
            std::time::Duration::ZERO
        );
        assert_eq!(
            timeout(&connection, libc::SO_SNDTIMEO),
            std::time::Duration::from_secs(5)
        );
    }
}
