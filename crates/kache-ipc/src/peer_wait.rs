//! Exchanges with a live peer outlast socket deadlines.
//!
//! Both ends of a committed exchange may be stalled by memory reclaim, a CPU
//! quota or SIGSTOP. A socket deadline only paces a liveness check: a stalled
//! peer delays the exchange and never fails it. `alive` ends the wait when the
//! peer has gone or the work was cancelled.

use std::io::{self, Write};

/// Retry `operation` at every socket deadline while `alive` succeeds.
///
/// A deadline can interrupt a read or receive with its data still queued, so
/// the operation must be resumable: `read_until` keeps the bytes it consumed
/// and a datagram receive takes nothing on timeout.
pub fn while_alive<T>(
    mut alive: impl FnMut() -> io::Result<()>,
    mut operation: impl FnMut() -> io::Result<T>,
) -> io::Result<T> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                alive()?;
            }
            result => return result,
        }
    }
}

/// `write_all` loses its progress at a deadline. Resume at the written offset
/// so a frame larger than the socket buffer survives a stalled reader.
pub fn write_all_while_alive(
    writer: &mut impl Write,
    mut bytes: &[u8],
    mut alive: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    while !bytes.is_empty() {
        match while_alive(&mut alive, || writer.write(bytes))? {
            0 => return Err(io::ErrorKind::WriteZero.into()),
            written => bytes = &bytes[written..],
        }
    }
    Ok(())
}

/// Succeed while the peer still holds its end of a stream socket.
#[cfg(unix)]
pub fn connected(socket: std::os::fd::BorrowedFd<'_>) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut poll = libc::pollfd {
        fd: socket.as_raw_fd(),
        // macOS reports a hangup only for a registered read interest.
        events: libc::POLLIN,
        revents: 0,
    };
    if unsafe { libc::poll(&mut poll, 1, 0) } < 0 {
        return Err(io::Error::last_os_error());
    }
    if poll.revents & (libc::POLLHUP | libc::POLLERR) != 0 {
        return Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "peer closed the channel",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadlines_and_interrupts_retry_while_alive_and_other_results_return() {
        let mut checks = 0;
        let mut calls = 0;
        let result = while_alive(
            || {
                checks += 1;
                Ok(())
            },
            || {
                calls += 1;
                match calls {
                    1 => Err(io::ErrorKind::WouldBlock.into()),
                    2 => Err(io::ErrorKind::TimedOut.into()),
                    3 => Err(io::ErrorKind::Interrupted.into()),
                    _ => Ok(7),
                }
            },
        );
        assert_eq!(result.unwrap(), 7);
        assert_eq!((calls, checks), (4, 2));
        let error = while_alive::<()>(
            || panic!("only deadlines check liveness"),
            || Err(io::ErrorKind::PermissionDenied.into()),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        let error = while_alive::<()>(
            || Err(io::ErrorKind::BrokenPipe.into()),
            || Err(io::ErrorKind::WouldBlock.into()),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    /// A writer that accepts a few bytes per call and stalls in between.
    struct Stalling {
        written: Vec<u8>,
        stalls: usize,
    }

    impl Write for Stalling {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.stalls += 1;
            if self.stalls % 2 == 1 {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            let count = bytes.len().min(3);
            self.written.extend_from_slice(&bytes[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn writes_resume_at_their_offset_after_each_deadline() {
        let mut writer = Stalling {
            written: Vec::new(),
            stalls: 0,
        };
        write_all_while_alive(&mut writer, b"a frame", || Ok(())).unwrap();
        assert_eq!(writer.written, b"a frame");
        let error = write_all_while_alive(&mut writer, b"more", || {
            Err(io::ErrorKind::BrokenPipe.into())
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        struct Closed;
        impl Write for Closed {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Ok(0)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let error = write_all_while_alive(&mut Closed, b"x", || Ok(())).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
        write_all_while_alive(&mut Closed, b"", || panic!("nothing to write")).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_stream_is_connected_until_its_peer_closes_even_with_unread_data() {
        use std::os::fd::AsFd;
        let (local, mut peer) = std::os::unix::net::UnixStream::pair().unwrap();
        connected(local.as_fd()).unwrap();
        peer.write_all(b"queued").unwrap();
        connected(local.as_fd()).unwrap();
        drop(peer);
        let error = connected(local.as_fd()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        let (local, peer) = std::os::unix::net::UnixStream::pair().unwrap();
        peer.shutdown(std::net::Shutdown::Both).unwrap();
        assert!(connected(local.as_fd()).is_err());
    }
}
