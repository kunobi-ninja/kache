//! Standard streams owned by a synchronous compiler request.

use std::cell::RefCell;
use std::fs::File;
use std::io::{self, Write};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, Weak};

#[derive(Clone)]
pub(crate) struct InvocationIo(Source);

#[derive(Clone)]
enum Source {
    Process,
    Owned(Arc<Streams>),
    Borrowed(Weak<Streams>),
}

struct Streams {
    input: File,
    output: Arc<Output>,
    error: Arc<Output>,
    terminal: Option<Arc<Output>>,
}

struct Output {
    file: File,
    serial: Mutex<()>,
}

impl Output {
    fn new(file: File) -> Arc<Self> {
        Arc::new(Self {
            file,
            serial: Mutex::new(()),
        })
    }

    fn with_writer<T>(&self, write: impl FnOnce(&mut &File) -> io::Result<T>) -> io::Result<T> {
        let _serial = self
            .serial
            .lock()
            .map_err(|_| io::Error::other("invocation output lock poisoned"))?;
        write(&mut &self.file)
    }
}

impl InvocationIo {
    /// Preserve inherited streams for standalone calls without captured handles.
    pub(crate) fn process() -> Self {
        Self(Source::Process)
    }

    /// Capture at the standalone process boundary. A daemon must receive owned
    /// files from its client; it must never use this fallback for a request.
    pub(crate) fn capture() -> Self {
        Self::try_capture().unwrap_or_else(|_| Self::process())
    }

    pub(crate) fn from_files(
        input: File,
        output: File,
        error: File,
        terminal: Option<File>,
    ) -> Self {
        Self(Source::Owned(Arc::new(Streams {
            input,
            output: Output::new(output),
            error: Output::new(error),
            terminal: terminal.map(Output::new),
        })))
    }

    /// Background notices may outlive the compiler. They borrow its channels
    /// without extending the lifetime of Cargo's output pipes.
    pub(crate) fn borrow_for_monitor(&self) -> Self {
        match &self.0 {
            Source::Process => Self::process(),
            Source::Owned(streams) => Self(Source::Borrowed(Arc::downgrade(streams))),
            Source::Borrowed(_) => self.clone(),
        }
    }

    fn streams(&self) -> io::Result<Option<Arc<Streams>>> {
        match &self.0 {
            Source::Process => Ok(None),
            Source::Owned(streams) => Ok(Some(Arc::clone(streams))),
            Source::Borrowed(streams) => streams.upgrade().map(Some).ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "compiler invocation finished")
            }),
        }
    }

    #[cfg(unix)]
    pub(crate) fn try_capture() -> io::Result<Self> {
        use std::os::fd::FromRawFd;

        fn duplicate(fd: libc::c_int) -> io::Result<File> {
            // fcntl also handles closed standard descriptors without borrowing
            // an invalid fd. Captured parent descriptors stay close-on-exec.
            let owned = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
            if owned < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(unsafe { File::from_raw_fd(owned) })
        }
        Ok(Self::from_files(
            duplicate(0)?,
            duplicate(1)?,
            duplicate(2)?,
            process_terminal(),
        ))
    }

    #[cfg(windows)]
    pub(crate) fn try_capture() -> io::Result<Self> {
        use std::io::IsTerminal;
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::{
            DUPLICATE_SAME_ACCESS, DuplicateHandle, INVALID_HANDLE_VALUE,
        };
        use windows_sys::Win32::System::Console::{
            GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;

        fn duplicate(which: u32) -> io::Result<File> {
            let handle = unsafe { GetStdHandle(which) };
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                return Err(io::Error::other("standard handle unavailable"));
            }
            let process = unsafe { GetCurrentProcess() };
            let mut owned = std::ptr::null_mut();
            if unsafe {
                DuplicateHandle(
                    process,
                    handle,
                    process,
                    &mut owned,
                    0,
                    0,
                    DUPLICATE_SAME_ACCESS,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(unsafe { File::from_raw_handle(owned) })
        }
        let input = duplicate(STD_INPUT_HANDLE)?;
        let output = duplicate(STD_OUTPUT_HANDLE)?;
        let error = duplicate(STD_ERROR_HANDLE)?;
        // Rust's console writer performs Unicode conversion. Retain the local
        // path until console handles have an equivalent remote writer.
        if input.is_terminal() || output.is_terminal() || error.is_terminal() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "console handles require local execution",
            ));
        }
        Ok(Self::from_files(input, output, error, None))
    }

    #[cfg(not(any(unix, windows)))]
    pub(crate) fn try_capture() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "standard handle capture unavailable",
        ))
    }

    fn configure(&self, command: &mut Command) -> io::Result<()> {
        if let Some(streams) = self.streams()? {
            command.stdin(Stdio::from(streams.input.try_clone()?));
            command.stdout(Stdio::from(streams.output.file.try_clone()?));
            command.stderr(Stdio::from(streams.error.file.try_clone()?));
        }
        Ok(())
    }
}

thread_local! {
    static CURRENT: RefCell<Option<InvocationIo>> = const { RefCell::new(None) };
}

/// A blocking worker holds this guard through execution and result replay.
pub(crate) struct InvocationIoScope {
    previous: Option<InvocationIo>,
    _warnings: kache_store::markers::InvocationWarningScope,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl InvocationIoScope {
    pub(crate) fn enter(io: InvocationIo) -> Self {
        let previous = CURRENT.with(|slot| slot.replace(Some(io)));
        Self {
            previous,
            _warnings: kache_store::markers::InvocationWarningScope::enter(|message| {
                let _ = writeln!(stderr(), "{message}");
            }),
            _thread: std::marker::PhantomData,
        }
    }
}

impl Drop for InvocationIoScope {
    fn drop(&mut self) {
        CURRENT.with(|slot| slot.replace(self.previous.take()));
    }
}

pub(crate) fn current() -> InvocationIo {
    CURRENT
        .with(|slot| slot.borrow().clone())
        .unwrap_or_else(InvocationIo::process)
}

enum Sink {
    Output,
    Error,
    File(Arc<Output>),
    Closed,
}

pub(crate) struct InvocationWriter(Sink);

impl Write for InvocationWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match &self.0 {
            Sink::Output => io::stdout().write(bytes),
            Sink::Error => io::stderr().write(bytes),
            Sink::File(output) => output.with_writer(|file| file.write(bytes)),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match &self.0 {
            Sink::Output => io::stdout().write_all(bytes),
            Sink::Error => io::stderr().write_all(bytes),
            Sink::File(output) => output.with_writer(|file| file.write_all(bytes)),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    fn write_fmt(&mut self, args: std::fmt::Arguments<'_>) -> io::Result<()> {
        match &self.0 {
            Sink::Output => io::stdout().write_fmt(args),
            Sink::Error => io::stderr().write_fmt(args),
            Sink::File(output) => output.with_writer(|file| file.write_fmt(args)),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &self.0 {
            Sink::Output => io::stdout().flush(),
            Sink::Error => io::stderr().flush(),
            Sink::File(output) => output.with_writer(|file| file.flush()),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
        }
    }
}

pub(crate) fn stdout() -> InvocationWriter {
    InvocationWriter(match current().streams() {
        Ok(Some(streams)) => Sink::File(Arc::clone(&streams.output)),
        Ok(None) => Sink::Output,
        Err(_) => Sink::Closed,
    })
}

pub(crate) fn stderr() -> InvocationWriter {
    InvocationWriter(match current().streams() {
        Ok(Some(streams)) => Sink::File(Arc::clone(&streams.error)),
        Ok(None) => Sink::Error,
        Err(_) => Sink::Closed,
    })
}

pub(crate) fn terminal() -> Option<InvocationWriter> {
    let output = match current().streams() {
        Ok(Some(streams)) => streams.terminal.clone(),
        Ok(None) => process_terminal().map(Output::new),
        Err(_) => None,
    };
    output.map(|output| InvocationWriter(Sink::File(output)))
}

#[cfg(unix)]
fn process_terminal() -> Option<File> {
    std::fs::OpenOptions::new()
        .write(true)
        .open("/dev/tty")
        .ok()
}

#[cfg(not(unix))]
fn process_terminal() -> Option<File> {
    None
}

/// Only inherited-stream executions use this. Captured-output probes and
/// compiles keep their explicit pipe/null setup and replay through the writers.
pub(crate) fn status(command: &mut Command) -> io::Result<ExitStatus> {
    current().configure(command)?;
    super::process::status(command)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn files(root: &std::path::Path, name: &str, tty: bool) -> InvocationIo {
        let input = root.join(format!("{name}.input"));
        std::fs::write(&input, format!("{name}-input\n")).unwrap();
        InvocationIo::from_files(
            File::open(input).unwrap(),
            File::create(root.join(format!("{name}.out"))).unwrap(),
            File::create(root.join(format!("{name}.err"))).unwrap(),
            tty.then(|| File::create(root.join(format!("{name}.tty"))).unwrap()),
        )
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_requests_replay_and_inherit_only_their_own_streams() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for (root, name, tty) in [
                (first.path(), "first", true),
                (second.path(), "second", false),
            ] {
                let barrier = &barrier;
                threads.spawn(move || {
                    let _scope = InvocationIoScope::enter(files(root, name, tty));
                    barrier.wait();
                    writeln!(stdout(), "before-{name}").unwrap();
                    writeln!(stderr(), "before-error-{name}").unwrap();
                    kache_store::markers::WarnSink::Stderr.emit(&format!("store-fault-{name}"));
                    crate::notice::show_requested(&format!("notice-{name}"));
                    let nested = files(root, "nested", false);
                    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let _nested = InvocationIoScope::enter(nested);
                        writeln!(stdout(), "nested-output").unwrap();
                        writeln!(stderr(), "nested-error").unwrap();
                        panic!("nested request failed");
                    }));
                    assert!(failed.is_err());
                    let env = crate::invocation::InvocationEnv::from_parts(
                        [("CLIENT", name)], Some(root.to_owned()), None, root.to_owned(),
                    );
                    let child = status(env.command("/bin/sh").unwrap().args([
                        "-c", "IFS= read -r line; printf 'child:%s:%s\\n' \"$CLIENT\" \"$line\"; printf 'child-error:%s\\n' \"$CLIENT\" >&2",
                    ])).unwrap();
                    assert!(child.success());
                    writeln!(stdout(), "after-{name}").unwrap();
                    assert_eq!(std::fs::read_to_string(root.join(format!("{name}.out"))).unwrap(),
                        format!("before-{name}\nchild:{name}:{name}-input\nafter-{name}\n"));
                    let requested = if tty { String::new() } else { format!("notice-{name}\n") };
                    assert_eq!(std::fs::read_to_string(root.join(format!("{name}.err"))).unwrap(),
                        format!("before-error-{name}\nstore-fault-{name}\n{requested}child-error:{name}\n"));
                    if tty {
                        assert_eq!(std::fs::read_to_string(root.join(format!("{name}.tty"))).unwrap(), format!("notice-{name}\n"));
                    }
                    assert_eq!(std::fs::read_to_string(root.join("nested.out")).unwrap(), "nested-output\n");
                    assert_eq!(std::fs::read_to_string(root.join("nested.err")).unwrap(), "nested-error\n");
                });
            }
        });
    }

    #[cfg(unix)]
    #[test]
    fn a_late_monitor_does_not_keep_output_pipes_open_or_use_process_streams() {
        use std::io::Read;
        use std::os::fd::OwnedFd;
        use std::os::unix::net::UnixStream;

        let (output, mut reader) = UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let io = InvocationIo::from_files(
            tempfile::tempfile().unwrap(),
            File::from(OwnedFd::from(output)),
            tempfile::tempfile().unwrap(),
            None,
        );
        let borrowed = io.borrow_for_monitor();
        {
            let _scope = InvocationIoScope::enter(io);
            stdout().write_all(b"request output").unwrap();
        }
        let _late = InvocationIoScope::enter(borrowed);
        let mut received = Vec::new();
        reader.read_to_end(&mut received).unwrap();
        assert_eq!(received, b"request output");
        assert_eq!(
            stdout().write_all(b"late").unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            stderr().write_all(b"late").unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(terminal().is_none());
        assert_eq!(
            status(Command::new("/bin/sh").args(["-c", "exit 0"]))
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[cfg(unix)]
    #[test]
    fn captured_parent_descriptors_are_close_on_exec() {
        use std::os::fd::AsRawFd;

        let io = InvocationIo::try_capture().unwrap();
        let streams = io.streams().unwrap().unwrap();
        for file in [&streams.input, &streams.output.file, &streams.error.file] {
            assert_ne!(
                unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
                0
            );
        }
    }
}
