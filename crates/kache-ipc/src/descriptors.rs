//! Transfer request-owned descriptors in authenticated Unix datagrams.
//!
//! Linux receives close-on-exec descriptors atomically. Platforms without that
//! receive flag serialize admission against process creation. Daemon code that
//! starts a child must hold spawn_guard() through the spawn call.

use std::io;
use std::mem::{size_of, size_of_val, zeroed};
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::sync::{Mutex, MutexGuard};

pub const MAX_DESCRIPTORS: usize = 64;
const MARKER: u8 = b'F';
const PACKET_BYTES: usize = 17;
// XNU sockargs bounds a single control message to MCLBYTES (2048). A
// datagram preserves that boundary; a stream can coalesce control records.
// Receive the whole native control record, then reject surplus handles.
const RECEIVE_CONTROL_WORDS: usize = 2048usize.div_ceil(size_of::<usize>());
const CONTROL_WORDS: usize =
    (unsafe { libc::CMSG_SPACE((MAX_DESCRIPTORS * size_of::<libc::c_int>()) as u32) } as usize)
        .div_ceil(size_of::<usize>());

static ADMISSION: Mutex<()> = Mutex::new(());

/// Hold only through process creation, never while waiting for a child.
/// Linux's atomic receive flag makes this guard unnecessary there.
pub fn spawn_guard() -> Option<MutexGuard<'static, ()>> {
    if cfg!(any(target_os = "linux", target_os = "android")) {
        None
    } else {
        Some(
            ADMISSION
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()),
        )
    }
}

fn check_count(count: usize) -> io::Result<()> {
    if count == 0 || count > MAX_DESCRIPTORS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid descriptor count",
        ));
    }
    Ok(())
}

fn check_datagram(socket: BorrowedFd<'_>) -> io::Result<()> {
    let mut kind: libc::c_int = 0;
    let mut length = size_of::<libc::c_int>() as libc::socklen_t;
    if unsafe {
        libc::getsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut kind as *mut libc::c_int).cast(),
            &mut length,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if kind != libc::SOCK_DGRAM {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "descriptor transfer requires a datagram socket",
        ));
    }
    Ok(())
}

/// Send a single descriptor bundle with its one-use ticket. The sender
/// retains its own descriptors. Only datagram sockets are accepted.
pub fn send(socket: BorrowedFd<'_>, ticket: &[u8; 16], files: &[BorrowedFd<'_>]) -> io::Result<()> {
    check_count(files.len())?;
    check_datagram(socket)?;
    let mut storage = [0usize; CONTROL_WORDS];
    let mut packet = [0u8; PACKET_BYTES];
    packet[0] = MARKER;
    packet[1..].copy_from_slice(ticket);
    let mut vector = libc::iovec {
        iov_base: packet.as_mut_ptr().cast(),
        iov_len: packet.len(),
    };
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = storage.as_mut_ptr().cast();
    message.msg_controllen =
        unsafe { libc::CMSG_SPACE((files.len() * size_of::<libc::c_int>()) as u32) } as _;
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN((files.len() * size_of::<libc::c_int>()) as u32) as _;
        let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
        for (index, file) in files.iter().enumerate() {
            data.add(index).write_unaligned(file.as_raw_fd());
        }
    }
    loop {
        let sent = unsafe { libc::sendmsg(socket.as_raw_fd(), &message, libc::MSG_NOSIGNAL) };
        if sent == PACKET_BYTES as isize {
            return Ok(());
        }
        if sent >= 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

/// Receive one bundle, owning every delivered descriptor before validation.
/// Received files stay close-on-exec in the parent. A child may explicitly
/// inherit its request's jobserver descriptors after fork.
pub fn receive(
    socket: BorrowedFd<'_>,
    ticket: &[u8; 16],
    expected: usize,
) -> io::Result<Vec<OwnedFd>> {
    receive_with(socket, ticket, expected, || {})
}

fn receive_with(
    socket: BorrowedFd<'_>,
    ticket: &[u8; 16],
    expected: usize,
    admitted: impl FnOnce(),
) -> io::Result<Vec<OwnedFd>> {
    check_count(expected)?;
    check_datagram(socket)?;
    let _admission = spawn_guard();
    let mut storage = [0usize; RECEIVE_CONTROL_WORDS];
    let mut packet = [0u8; PACKET_BYTES];
    let mut vector = libc::iovec {
        iov_base: packet.as_mut_ptr().cast(),
        iov_len: packet.len(),
    };
    let mut message: libc::msghdr = unsafe { zeroed() };
    message.msg_iov = &mut vector;
    message.msg_iovlen = 1;
    message.msg_control = storage.as_mut_ptr().cast();
    message.msg_controllen = size_of_val(&storage) as _;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let flags = libc::MSG_CMSG_CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let flags = 0;
    let received = loop {
        message.msg_controllen = size_of_val(&storage) as _;
        message.msg_flags = 0;
        let result = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut message, flags) };
        if result >= 0 {
            break result;
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    };
    let mut files = Vec::with_capacity(expected);
    let filled = (message.msg_controllen as usize).min(size_of_val(&storage));
    let header_size = unsafe { libc::CMSG_LEN(0) } as usize;
    let mut offset = 0usize;
    let mut invalid = false;
    while filled.saturating_sub(offset) >= size_of::<libc::cmsghdr>() {
        let header = unsafe {
            storage
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<libc::cmsghdr>()
                .read_unaligned()
        };
        let length: usize = header.cmsg_len as _;
        if length < header_size {
            invalid = true;
            break;
        }
        let remaining = filled - offset;
        // Darwin leaves cmsg_len untruncated under MSG_CTRUNC. Only the bytes
        // actually copied into our control buffer can contain received fds.
        let bounded = length.min(remaining);
        invalid |= length > remaining;
        let payload = bounded.saturating_sub(header_size);
        if header.cmsg_level == libc::SOL_SOCKET && header.cmsg_type == libc::SCM_RIGHTS {
            invalid |= !payload.is_multiple_of(size_of::<libc::c_int>());
            let data = unsafe {
                storage
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset + header_size)
                    .cast::<libc::c_int>()
            };
            for index in 0..payload / size_of::<libc::c_int>() {
                let fd = unsafe { data.add(index).read_unaligned() };
                if fd < 0 {
                    invalid = true;
                    continue;
                }
                files.push(unsafe { OwnedFd::from_raw_fd(fd) });
            }
        } else {
            invalid = true;
        }
        if length > remaining {
            break;
        }
        let step = unsafe { libc::CMSG_SPACE((length - header_size) as u32) } as usize;
        if step == 0 || step > remaining {
            break;
        }
        offset += step;
    }
    // The guard also covers test-induced pauses in the non-atomic interval.
    admitted();
    for file in &files {
        let current = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
        if current < 0
            || unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, current | libc::FD_CLOEXEC) }
                < 0
        {
            return Err(io::Error::last_os_error());
        }
    }
    if received != PACKET_BYTES as isize
        || packet[0] != MARKER
        || &packet[1..] != ticket
        || invalid
        || message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0
        || files.len() != expected
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid descriptor bundle",
        ));
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{Read, Seek, Write};
    use std::os::fd::AsFd;
    use std::os::unix::net::{UnixDatagram, UnixStream};

    const TICKET: [u8; 16] = [7; 16];

    #[test]
    fn transferred_files_are_owned_close_on_exec_and_share_their_open_description() {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        let mut source = tempfile::tempfile().unwrap();
        source.write_all(b"client bytes").unwrap();
        source.rewind().unwrap();
        send(sender.as_fd(), &TICKET, &[source.as_fd()]).unwrap();
        let mut received = receive(receiver.as_fd(), &TICKET, 1).unwrap();
        let file = received.pop().unwrap();
        assert_ne!(file.as_raw_fd(), source.as_raw_fd());
        assert_ne!(
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let mut bytes = Vec::new();
        File::from(file).read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"client bytes");
        assert_eq!(source.stream_position().unwrap(), 12);
    }

    #[test]
    fn rejecting_a_bundle_closes_all_received_descriptors() {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        let (writer, mut reader) = UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        send(sender.as_fd(), &TICKET, &[writer.as_fd()]).unwrap();
        assert_eq!(
            receive(receiver.as_fd(), &TICKET, 2).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(
            send(sender.as_fd(), &TICKET, &[]).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            receive(receiver.as_fd(), &TICKET, MAX_DESCRIPTORS + 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn ordinary_bytes_empty_datagrams_and_streams_are_rejected() {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        sender.send(b"F").unwrap();
        assert_eq!(
            receive(receiver.as_fd(), &TICKET, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        sender.send(b"").unwrap();
        assert_eq!(
            receive(receiver.as_fd(), &TICKET, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let (_sender, stream) = UnixStream::pair().unwrap();
        assert_eq!(
            receive(stream.as_fd(), &TICKET, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn a_wrong_ticket_closes_its_descriptors_without_accepting_the_bundle() {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        let (writer, mut reader) = UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        send(sender.as_fd(), &TICKET, &[writer.as_fd()]).unwrap();
        assert_eq!(
            receive(receiver.as_fd(), &[8; 16], 1).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn the_largest_protocol_bundle_is_received_in_full() {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        let (writer, mut reader) = UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let files = vec![writer.as_fd(); MAX_DESCRIPTORS];
        send(sender.as_fd(), &TICKET, &files).unwrap();
        let received = receive(receiver.as_fd(), &TICKET, MAX_DESCRIPTORS).unwrap();
        assert_eq!(received.len(), MAX_DESCRIPTORS);
        assert_eq!(
            send(
                sender.as_fd(),
                &TICKET,
                &vec![writer.as_fd(); MAX_DESCRIPTORS + 1]
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
        drop(received);
        drop(files);
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn oversized_control_bundle_does_not_leave_received_pipes_open() {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        let (writer, mut reader) = UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let count = MAX_DESCRIPTORS + 4;
        let length =
            unsafe { libc::CMSG_SPACE((count * size_of::<libc::c_int>()) as u32) } as usize;
        let mut storage = vec![0usize; length.div_ceil(size_of::<usize>())];
        let mut packet = [0u8; PACKET_BYTES];
        packet[0] = MARKER;
        packet[1..].copy_from_slice(&TICKET);
        let mut vector = libc::iovec {
            iov_base: packet.as_mut_ptr().cast(),
            iov_len: packet.len(),
        };
        let mut message: libc::msghdr = unsafe { zeroed() };
        message.msg_iov = &mut vector;
        message.msg_iovlen = 1;
        message.msg_control = storage.as_mut_ptr().cast();
        message.msg_controllen = length as _;
        unsafe {
            let header = libc::CMSG_FIRSTHDR(&message);
            (*header).cmsg_level = libc::SOL_SOCKET;
            (*header).cmsg_type = libc::SCM_RIGHTS;
            (*header).cmsg_len = libc::CMSG_LEN((count * size_of::<libc::c_int>()) as u32) as _;
            let data = libc::CMSG_DATA(header).cast::<libc::c_int>();
            for index in 0..count {
                data.add(index).write_unaligned(writer.as_raw_fd());
            }
            assert_eq!(
                libc::sendmsg(sender.as_raw_fd(), &message, libc::MSG_NOSIGNAL),
                PACKET_BYTES as isize
            );
        }
        assert_eq!(
            receive(receiver.as_fd(), &TICKET, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert!(bytes.is_empty());
    }

    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    #[test]
    fn process_creation_waits_until_received_descriptors_are_close_on_exec() {
        let (sender, receiver) = UnixDatagram::pair().unwrap();
        let source = tempfile::tempfile().unwrap();
        send(sender.as_fd(), &TICKET, &[source.as_fd()]).unwrap();
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (spawned_tx, spawned_rx) = std::sync::mpsc::channel();
        std::thread::scope(|threads| {
            let receiving = threads.spawn(move || {
                receive_with(receiver.as_fd(), &TICKET, 1, || {
                    admitted_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
                .unwrap()
            });
            admitted_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap();
            let spawning = threads.spawn(|| {
                let _guard = spawn_guard();
                spawned_tx.send(()).unwrap();
            });
            assert!(
                spawned_rx
                    .recv_timeout(std::time::Duration::from_millis(30))
                    .is_err()
            );
            release_tx.send(()).unwrap();
            let received = receiving.join().unwrap();
            spawning.join().unwrap();
            spawned_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap();
            assert_ne!(
                unsafe { libc::fcntl(received[0].as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
                0
            );
        });
    }
}
