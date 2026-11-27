//! Frames to a delegated client after Accepted, written by a dedicated OS
//! thread through its own handle on the connection.
//!
//! Heartbeats must not depend on the daemon's async runtime: on a starved
//! host (every runtime worker and blocking thread busy) the connection task
//! can go unpolled for a minute while the compile is healthy, and the client
//! would give up on it. This thread needs only the connection and a CPU
//! slice: it writes queued frames in order and a heartbeat whenever nothing
//! was queued for an interval.

use anyhow::{Context, Result};
use kache_ipc::TokioStream;
use kache_protocol::execution::ExecutionReply;
use std::io;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

/// Longest one frame may wait for a client that does not read before the
/// writer gives the connection up: the client's own silence limit, so a
/// starved client is never cut off sooner than it would cut off the daemon.
const FRAME_WRITE_LIMIT: Duration = kache_ipc::execution::EXECUTION_SILENCE_LIMIT;

pub(super) struct FrameWriter {
    frames: Option<mpsc::Sender<Vec<u8>>>,
    done: Option<tokio::sync::oneshot::Receiver<io::Result<()>>>,
}

impl FrameWriter {
    /// Start writing to `stream` through a duplicate of its handle.
    pub(super) fn start(stream: &TokioStream, heartbeat: Duration) -> io::Result<Self> {
        Self::with_sink(duplicate(stream)?, heartbeat)
    }

    fn with_sink(
        mut sink: impl FnMut(&[u8]) -> io::Result<()> + Send + 'static,
        heartbeat: Duration,
    ) -> io::Result<Self> {
        let (frames, queue) = mpsc::channel::<Vec<u8>>();
        let (finished, done) = tokio::sync::oneshot::channel();
        let beat = frame(&ExecutionReply::Heartbeat).map_err(io::Error::other)?;
        std::thread::Builder::new()
            .name("kache-execution-frames".into())
            .spawn(move || {
                let result = (|| loop {
                    match queue.recv_timeout(heartbeat) {
                        Ok(frame) => sink(&frame)?,
                        Err(RecvTimeoutError::Timeout) => sink(&beat)?,
                        // Every queued frame is written before this.
                        Err(RecvTimeoutError::Disconnected) => return Ok(()),
                    }
                })();
                let _ = finished.send(result);
            })?;
        Ok(Self {
            frames: Some(frames),
            done: Some(done),
        })
    }

    /// Queue `reply` after every frame queued before it.
    pub(super) fn send(&self, reply: &ExecutionReply) -> Result<()> {
        self.frames
            .as_ref()
            .context("execution frame writer finished")?
            .send(frame(reply)?)
            .map_err(|_| anyhow::anyhow!("execution frame writer stopped (client gone)"))
    }

    /// Stop heartbeats once every queued frame is written, and wait for the
    /// writer to release its handle. Needs no runtime worker beyond this one
    /// await: the thread reports through a oneshot channel.
    pub(super) async fn finish(mut self) -> Result<()> {
        drop(self.frames.take());
        match self.done.take() {
            Some(done) => done
                .await
                .context("execution frame writer vanished")?
                .context("writing execution frames"),
            None => Ok(()),
        }
    }
}

impl Drop for FrameWriter {
    /// Early exits stop heartbeats too. The thread drains what is queued
    /// (bounded by [`FRAME_WRITE_LIMIT`] per frame) and closes its handle.
    fn drop(&mut self) {
        drop(self.frames.take());
    }
}

fn frame(reply: &ExecutionReply) -> Result<Vec<u8>> {
    let mut frame = serde_json::to_vec(reply)?;
    frame.push(b'\n');
    Ok(frame)
}

/// A blocking writer on a duplicate of the connection's socket. The
/// duplicate shares the runtime's non-blocking file status, so a full send
/// buffer is waited out with `poll` instead of switching modes under it.
#[cfg(unix)]
fn duplicate(
    stream: &TokioStream,
) -> io::Result<impl FnMut(&[u8]) -> io::Result<()> + Send + use<>> {
    use std::io::Write;
    use std::os::fd::{AsFd, AsRawFd};
    let TokioStream::UdSocket(socket) = stream;
    let file = std::fs::File::from(socket.inner().as_fd().try_clone_to_owned()?);
    Ok(move |mut bytes: &[u8]| {
        let deadline = std::time::Instant::now() + FRAME_WRITE_LIMIT;
        while !bytes.is_empty() {
            match (&file).write(bytes) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(written) => bytes = &bytes[written..],
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        return Err(io::ErrorKind::TimedOut.into());
                    }
                    let mut ready = libc::pollfd {
                        fd: file.as_raw_fd(),
                        events: libc::POLLOUT,
                        revents: 0,
                    };
                    let timeout =
                        libc::c_int::try_from(left.as_millis()).unwrap_or(libc::c_int::MAX);
                    // SAFETY: one valid pollfd for the duration of the call.
                    if unsafe { libc::poll(&mut ready, 1, timeout) } < 0 {
                        let error = io::Error::last_os_error();
                        if error.kind() != io::ErrorKind::Interrupted {
                            return Err(error);
                        }
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    })
}

/// A writer on a duplicate of the connection's pipe handle. The pipe is
/// opened for overlapped I/O and bound to the runtime's completion port, so
/// each write is overlapped with its own event whose low bit keeps the
/// completion off that port, and is waited out here.
#[cfg(windows)]
fn duplicate(
    stream: &TokioStream,
) -> io::Result<impl FnMut(&[u8]) -> io::Result<()> + Send + use<>> {
    use std::os::windows::io::AsHandle;
    let TokioStream::NamedPipe(pipe) = stream;
    let handle = pipe.inner().as_handle().try_clone_to_owned()?;
    Ok(move |bytes: &[u8]| write_overlapped(&handle, bytes))
}

#[cfg(windows)]
fn write_overlapped(
    handle: &std::os::windows::io::OwnedHandle,
    mut bytes: &[u8],
) -> io::Result<()> {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, GetLastError, HANDLE, WAIT_OBJECT_0};
    use windows_sys::Win32::Storage::FileSystem::WriteFile;
    use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
    use windows_sys::Win32::System::Threading::{CreateEventW, WaitForSingleObject};

    // SAFETY: plain manual-reset event, no name or security attributes.
    let event = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
    if event.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateEventW returned a fresh handle this function owns.
    let event = unsafe { OwnedHandle::from_raw_handle(event) };
    let pipe = handle.as_raw_handle() as HANDLE;
    while !bytes.is_empty() {
        // SAFETY: an all-zero OVERLAPPED is its documented initial state.
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.hEvent = (event.as_raw_handle() as usize | 1) as HANDLE;
        let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
        let mut written = 0u32;
        // SAFETY: `bytes` and `overlapped` outlive the operation, which is
        // waited for (or cancelled and then waited for) below.
        let started =
            unsafe { WriteFile(pipe, bytes.as_ptr(), length, &mut written, &mut overlapped) };
        if started == 0 {
            // SAFETY: reads this thread's last error.
            let error = unsafe { GetLastError() };
            if error != ERROR_IO_PENDING {
                return Err(io::Error::from_raw_os_error(error as i32));
            }
            let limit = u32::try_from(FRAME_WRITE_LIMIT.as_millis()).unwrap_or(u32::MAX);
            // SAFETY: valid event handle.
            let waited = unsafe { WaitForSingleObject(event.as_raw_handle() as HANDLE, limit) };
            if waited != WAIT_OBJECT_0 {
                // SAFETY: cancels only this operation; it is reaped below.
                unsafe { CancelIoEx(pipe, &overlapped) };
            }
            // SAFETY: waits for the operation to finish or be cancelled.
            if unsafe { GetOverlappedResult(pipe, &overlapped, &mut written, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            if waited != WAIT_OBJECT_0 {
                return Err(io::ErrorKind::TimedOut.into());
            }
        }
        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        bytes = &bytes[written as usize..];
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    /// Every frame the writer emitted, with the instant it was written.
    type Written = Arc<Mutex<Vec<(Instant, Vec<u8>)>>>;

    fn recording() -> (
        Written,
        impl FnMut(&[u8]) -> io::Result<()> + Send + 'static,
    ) {
        let written = Arc::new(Mutex::new(Vec::new()));
        let sink = {
            let written = Arc::clone(&written);
            move |bytes: &[u8]| {
                written
                    .lock()
                    .unwrap()
                    .push((Instant::now(), bytes.to_vec()));
                Ok(())
            }
        };
        (written, sink)
    }

    /// A starved daemon: the connection task's runtime is blocked (here the
    /// whole current-thread runtime sleeps) for many heartbeat intervals,
    /// yet the writer thread keeps the client hearing from the daemon.
    #[tokio::test]
    async fn heartbeats_continue_while_the_runtime_is_blocked() {
        let (written, sink) = recording();
        let writer = FrameWriter::with_sink(sink, Duration::from_millis(20)).unwrap();
        let blocked = Instant::now();
        std::thread::sleep(Duration::from_millis(400));
        let beats = written
            .lock()
            .unwrap()
            .iter()
            .filter(|(at, bytes)| {
                *at > blocked && bytes.starts_with(br#"{"execution":"heartbeat"}"#)
            })
            .count();
        assert!(
            beats >= 5,
            "only {beats} heartbeats while the runtime was blocked"
        );
        writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn queued_frames_keep_their_order_and_finish_flushes_them() {
        let (written, sink) = recording();
        let writer = FrameWriter::with_sink(sink, Duration::from_secs(3600)).unwrap();
        writer.send(&ExecutionReply::Accepted).unwrap();
        writer
            .send(&ExecutionReply::Complete { exit_code: 3 })
            .unwrap();
        writer.finish().await.unwrap();
        let frames = written
            .lock()
            .unwrap()
            .iter()
            .map(|(_, bytes)| serde_json::from_slice::<ExecutionReply>(bytes).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            frames,
            vec![
                ExecutionReply::Accepted,
                ExecutionReply::Complete { exit_code: 3 }
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_write_stops_the_writer_and_is_reported() {
        let writer = FrameWriter::with_sink(
            |_: &[u8]| Err(io::ErrorKind::BrokenPipe.into()),
            Duration::from_millis(5),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(writer.send(&ExecutionReply::Heartbeat).is_err());
        assert!(writer.finish().await.is_err());
    }

    /// The same over a real daemon connection: a reader on the client end
    /// keeps receiving heartbeats while the daemon's runtime is blocked.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_blocked_runtime_still_heartbeats_over_the_socket() {
        use interprocess::local_socket::traits::tokio::Listener as _;
        use std::io::BufRead;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("frames.sock");
        let name = kache_ipc::socket_name(&path).unwrap();
        let listener = interprocess::local_socket::ListenerOptions::new()
            .name(name)
            .create_tokio()
            .unwrap();
        let client = std::thread::spawn({
            let path = path.clone();
            move || {
                let stream = std::os::unix::net::UnixStream::connect(path).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_millis(250)))
                    .unwrap();
                let mut lines = std::io::BufReader::new(stream).lines();
                (0..5)
                    .map(|_| lines.next().unwrap().unwrap())
                    .collect::<Vec<_>>()
            }
        });
        let stream = listener.accept().await.unwrap();
        let writer = FrameWriter::start(&stream, Duration::from_millis(50)).unwrap();
        // Starve the runtime: nothing async runs for ten heartbeat periods.
        std::thread::sleep(Duration::from_millis(500));
        let lines = client.join().unwrap();
        assert!(
            lines
                .iter()
                .all(|line| line == r#"{"execution":"heartbeat"}"#),
            "{lines:?}"
        );
        drop(writer);
    }
}
