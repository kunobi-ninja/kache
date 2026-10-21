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
    let (descriptors, invalid) = parse_control(&storage, message.msg_controllen as usize);
    // recvmsg transfers a distinct owned descriptor for each SCM_RIGHTS item.
    // Own all of them before rejecting the packet or setting close-on-exec.
    let files = descriptors
        .into_iter()
        .map(|fd| unsafe { OwnedFd::from_raw_fd(fd) })
        .collect::<Vec<_>>();
    // The guard also covers test-induced pauses in the non-atomic interval.
    admitted();
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    for file in &files {
        set_close_on_exec(file)?;
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

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn set_close_on_exec(file: &OwnedFd) -> io::Result<()> {
    let current = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
    if current == -1
        || unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, current | libc::FD_CLOEXEC) } == -1
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Read the initialized control buffer without taking ownership of its fds.
/// Padding and truncated records never extend the accessible byte slice.
fn parse_control(storage: &[usize], filled: usize) -> (Vec<libc::c_int>, bool) {
    // usize has no invalid representations; every byte in storage is initialized.
    let bytes =
        unsafe { std::slice::from_raw_parts(storage.as_ptr().cast::<u8>(), size_of_val(storage)) };
    let mut control = &bytes[..filled.min(bytes.len())];
    let header_size = unsafe { libc::CMSG_LEN(0) } as usize;
    let mut descriptors = Vec::new();
    let mut invalid = false;
    while control.len() >= size_of::<libc::cmsghdr>() {
        let header = unsafe { control.as_ptr().cast::<libc::cmsghdr>().read_unaligned() };
        let length: usize = header.cmsg_len as _;
        if length < header_size {
            invalid = true;
            break;
        }
        // Darwin leaves cmsg_len untruncated under MSG_CTRUNC. Only the bytes
        // actually copied into our control buffer can contain received fds.
        let bounded = length.min(control.len());
        invalid |= length > control.len();
        let payload = bounded.saturating_sub(header_size);
        if header.cmsg_level == libc::SOL_SOCKET && header.cmsg_type == libc::SCM_RIGHTS {
            invalid |= !payload.is_multiple_of(size_of::<libc::c_int>());
            for bytes in control[header_size..bounded]
                .as_chunks::<{ size_of::<libc::c_int>() }>()
                .0
            {
                let fd = libc::c_int::from_ne_bytes(*bytes);
                if fd < 0 {
                    invalid = true;
                    continue;
                }
                descriptors.push(fd);
            }
        } else {
            invalid = true;
        }
        let step = unsafe { libc::CMSG_SPACE(payload as u32) } as usize;
        control = control.get(step..).unwrap_or_default();
    }
    (descriptors, invalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{Read, Seek, Write};
    use std::os::fd::AsFd;
    use std::os::unix::net::{UnixDatagram, UnixStream};

    const TICKET: [u8; 16] = [7; 16];

    fn datagram_pair() -> (UnixDatagram, UnixDatagram) {
        let sockets = UnixDatagram::pair().unwrap();
        for socket in [&sockets.0, &sockets.1] {
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(std::time::Duration::from_secs(5)))
                .unwrap();
        }
        sockets
    }

    fn control_record(level: libc::c_int, kind: libc::c_int, payload: &[u8]) -> Vec<u8> {
        let length = unsafe { libc::CMSG_SPACE(payload.len() as u32) } as usize;
        let mut bytes = vec![0; length];
        unsafe {
            let header = bytes.as_mut_ptr().cast::<libc::cmsghdr>();
            std::ptr::addr_of_mut!((*header).cmsg_len)
                .write_unaligned(libc::CMSG_LEN(payload.len() as u32) as _);
            std::ptr::addr_of_mut!((*header).cmsg_level).write_unaligned(level);
            std::ptr::addr_of_mut!((*header).cmsg_type).write_unaligned(kind);
        }
        let start = unsafe { libc::CMSG_LEN(0) } as usize;
        bytes[start..start + payload.len()].copy_from_slice(payload);
        bytes
    }

    fn control_words(bytes: &[u8]) -> Vec<usize> {
        let mut storage = vec![0; bytes.len().div_ceil(size_of::<usize>())];
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                storage.as_mut_ptr().cast::<u8>(),
                bytes.len(),
            );
        }
        storage
    }

    fn parse_records(bytes: &[u8]) -> (Vec<libc::c_int>, bool) {
        parse_control(&control_words(bytes), bytes.len())
    }

    fn rights(descriptors: &[libc::c_int]) -> Vec<u8> {
        let payload = descriptors
            .iter()
            .flat_map(|fd| fd.to_ne_bytes())
            .collect::<Vec<_>>();
        control_record(libc::SOL_SOCKET, libc::SCM_RIGHTS, &payload)
    }

    #[test]
    fn control_records_preserve_zero_and_positive_descriptors_in_order() {
        let record = rights(&[0, 5, 19]);
        assert_eq!(parse_records(&record), (vec![0, 5, 19], false));
        assert_eq!(parse_records(&rights(&[0, 5])), (vec![0, 5], false));
        assert_eq!(parse_control(&[], usize::MAX), (vec![], false));
        let storage = control_words(&record);
        assert_eq!(parse_control(&storage, 0), (vec![], false));
        assert_eq!(
            parse_control(&storage, usize::MAX),
            (vec![0, 5, 19], false),
            "the reported size cannot extend beyond initialized storage"
        );
    }

    #[test]
    fn control_records_distinguish_short_and_empty_headers() {
        let header_size = unsafe { libc::CMSG_LEN(0) } as usize;
        for length in [0, header_size - 1] {
            let mut record = rights(&[]);
            unsafe {
                std::ptr::addr_of_mut!((*record.as_mut_ptr().cast::<libc::cmsghdr>()).cmsg_len)
                    .write_unaligned(length as _);
            }
            assert_eq!(parse_records(&record), (vec![], true), "length={length}");
        }
        assert_eq!(parse_records(&rights(&[])), (vec![], false));
        assert!(
            parse_records(&rights(&[9])[..size_of::<libc::cmsghdr>() - 1])
                .0
                .is_empty()
        );
    }

    #[test]
    fn unrecognized_control_records_do_not_hide_later_descriptors() {
        for (level, kind) in [
            (libc::SOL_SOCKET + 1, libc::SCM_RIGHTS),
            (libc::SOL_SOCKET, libc::SCM_RIGHTS + 1),
            (libc::SOL_SOCKET + 1, libc::SCM_RIGHTS + 1),
        ] {
            let mut bytes = control_record(level, kind, &17i32.to_ne_bytes());
            bytes.extend(rights(&[0, 7]));
            assert_eq!(parse_records(&bytes), (vec![0, 7], true));
        }
    }

    #[test]
    fn partial_control_payloads_retain_every_complete_descriptor() {
        for payload in [
            vec![0xff],
            [0i32.to_ne_bytes().as_slice(), &[0xff]].concat(),
        ] {
            let mut bytes = control_record(libc::SOL_SOCKET, libc::SCM_RIGHTS, &payload);
            bytes.extend(rights(&[11]));
            let expected = if payload.len() == 1 {
                vec![11]
            } else {
                vec![0, 11]
            };
            assert_eq!(parse_records(&bytes), (expected, true));
        }
        let mut bytes = rights(&[-1, 0, -7, 13]);
        bytes.extend(rights(&[23]));
        assert_eq!(parse_records(&bytes), (vec![0, 13, 23], true));
    }

    #[test]
    fn untruncated_control_lengths_only_read_the_delivered_bytes() {
        let mut bytes = rights(&[0, 17]);
        let actual = unsafe { libc::CMSG_LEN(8) } as usize;
        bytes.truncate(actual);
        unsafe {
            std::ptr::addr_of_mut!((*bytes.as_mut_ptr().cast::<libc::cmsghdr>()).cmsg_len)
                .write_unaligned((actual + 64) as _);
        }
        assert_eq!(parse_records(&bytes), (vec![0, 17], true));
        bytes.truncate(actual - 1);
        assert_eq!(parse_records(&bytes), (vec![0], true));
    }

    #[test]
    fn aligned_control_records_are_visited_once_without_reading_padding() {
        let mut bytes = rights(&[0]);
        let first_length = unsafe { libc::CMSG_LEN(4) } as usize;
        bytes[first_length..].fill(0xff);
        bytes.extend(rights(&[5, 19]));
        bytes.extend(rights(&[23]));
        assert_eq!(parse_records(&bytes), (vec![0, 5, 19, 23], false));
    }

    #[test]
    fn a_full_nonblocking_send_queue_returns_its_error_without_retrying() {
        let (sender, receiver) = datagram_pair();
        sender.set_nonblocking(true).unwrap();
        receiver.set_nonblocking(true).unwrap();
        let packet = [0; PACKET_BYTES];
        let full_error = loop {
            match sender.send(&packet) {
                Ok(_) => {}
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock
                        || error.raw_os_error() == Some(libc::ENOBUFS) =>
                {
                    break error.raw_os_error();
                }
                Err(error) => panic!("filling datagram queue: {error}"),
            }
        };
        let source = tempfile::tempfile().unwrap();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let result = std::thread::scope(|threads| {
            threads.spawn(|| {
                result_tx
                    .send(send(sender.as_fd(), &TICKET, &[source.as_fd()]))
                    .unwrap();
            });
            match result_rx.recv_timeout(std::time::Duration::from_secs(3)) {
                Ok(result) => result,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Release an erroneous retry loop so it fails its assertion.
                    let mut packet = [0; PACKET_BYTES];
                    while receiver.recv(&mut packet).is_ok() {}
                    result_rx
                        .recv_timeout(std::time::Duration::from_secs(3))
                        .unwrap()
                }
                Err(error) => panic!("descriptor sender ended without a result: {error}"),
            }
        });
        assert_eq!(result.unwrap_err().raw_os_error(), full_error);
    }

    #[test]
    fn invalid_packet_headers_and_truncation_close_received_descriptors() {
        for variant in ["marker", "short", "truncated"] {
            let (sender, receiver) = datagram_pair();
            let (writer, mut reader) = UnixStream::pair().unwrap();
            reader
                .set_read_timeout(Some(std::time::Duration::from_secs(1)))
                .unwrap();
            let mut ticket = TICKET;
            ticket[15] = 0;
            let mut packet = vec![MARKER];
            packet.extend_from_slice(&ticket);
            match variant {
                "marker" => packet[0] = b'X',
                "short" => {
                    packet.pop();
                }
                "truncated" => packet.push(0xff),
                _ => unreachable!(),
            }
            let control = rights(&[writer.as_raw_fd()]);
            let mut storage = control_words(&control);
            let mut vector = libc::iovec {
                iov_base: packet.as_mut_ptr().cast(),
                iov_len: packet.len(),
            };
            let mut message: libc::msghdr = unsafe { zeroed() };
            message.msg_iov = &mut vector;
            message.msg_iovlen = 1;
            message.msg_control = storage.as_mut_ptr().cast();
            message.msg_controllen = control.len() as _;
            assert_eq!(
                unsafe { libc::sendmsg(sender.as_raw_fd(), &message, libc::MSG_NOSIGNAL) },
                packet.len() as isize
            );
            assert_eq!(
                receive(receiver.as_fd(), &ticket, 1).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "{variant}"
            );
            drop(writer);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).unwrap();
            assert!(bytes.is_empty(), "{variant}");
        }
    }

    #[test]
    fn transferred_files_are_owned_close_on_exec_and_share_their_open_description() {
        let (sender, receiver) = datagram_pair();
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
        let (sender, receiver) = datagram_pair();
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
        let (sender, receiver) = datagram_pair();
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
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        assert_eq!(
            receive(stream.as_fd(), &TICKET, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn an_empty_nonblocking_socket_returns_would_block_without_waiting_for_a_sender() {
        let (sender, receiver) = datagram_pair();
        receiver.set_nonblocking(true).unwrap();
        let (file, _other) = UnixStream::pair().unwrap();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let result = std::thread::scope(|threads| {
            threads.spawn(move || {
                result_tx
                    .send(receive(receiver.as_fd(), &TICKET, 1))
                    .unwrap();
            });
            match result_rx.recv_timeout(std::time::Duration::from_secs(3)) {
                Ok(result) => result,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    // Unblock an erroneous retry loop, so this regression
                    // fails with a result instead of leaving a busy worker.
                    send(sender.as_fd(), &TICKET, &[file.as_fd()]).unwrap();
                    result_rx
                        .recv_timeout(std::time::Duration::from_secs(3))
                        .unwrap()
                }
                Err(error) => panic!("descriptor receiver ended without a result: {error}"),
            }
        });
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
    }

    #[test]
    fn a_wrong_ticket_closes_its_descriptors_without_accepting_the_bundle() {
        let (sender, receiver) = datagram_pair();
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
        let (sender, receiver) = datagram_pair();
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
        let (sender, receiver) = datagram_pair();
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
        let (sender, receiver) = datagram_pair();
        let source = tempfile::tempfile().unwrap();
        send(sender.as_fd(), &TICKET, &[source.as_fd()]).unwrap();
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (spawned_tx, spawned_rx) = std::sync::mpsc::channel();
        std::thread::scope(|threads| {
            let receiving = threads.spawn(move || {
                receive_with(receiver.as_fd(), &TICKET, 1, || {
                    admitted_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(5))
                        .unwrap();
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
