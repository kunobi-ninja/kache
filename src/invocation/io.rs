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
    #[cfg(target_os = "linux")]
    Linux {
        streams: Arc<Streams>,
        broker: Arc<super::linux_broker::Broker>,
    },
    #[cfg(windows)]
    Windows(Arc<super::windows_broker::Broker>),
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
    #[cfg(target_os = "linux")]
    pub(crate) fn with_linux_broker(
        self,
        broker: Arc<super::linux_broker::Broker>,
    ) -> io::Result<Self> {
        let streams = self
            .streams()?
            .ok_or_else(|| io::Error::other("native broker requires captured streams"))?;
        Ok(Self(Source::Linux { streams, broker }))
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn linux_broker(&self) -> Option<Arc<super::linux_broker::Broker>> {
        match &self.0 {
            Source::Linux { broker, .. } => Some(Arc::clone(broker)),
            _ => None,
        }
    }
    #[cfg(windows)]
    pub(crate) fn from_broker(broker: Arc<super::windows_broker::Broker>) -> Self {
        Self(Source::Windows(broker))
    }

    #[cfg(windows)]
    pub(crate) fn broker(&self) -> Option<Arc<super::windows_broker::Broker>> {
        match &self.0 {
            Source::Windows(broker) => Some(Arc::clone(broker)),
            _ => None,
        }
    }
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
            #[cfg(target_os = "linux")]
            Source::Linux { streams, .. } => Self(Source::Borrowed(Arc::downgrade(streams))),
            #[cfg(windows)]
            Source::Windows(_) => self.clone(),
        }
    }

    fn streams(&self) -> io::Result<Option<Arc<Streams>>> {
        match &self.0 {
            Source::Process => Ok(None),
            Source::Owned(streams) => Ok(Some(Arc::clone(streams))),
            #[cfg(target_os = "linux")]
            Source::Linux { streams, .. } => Ok(Some(Arc::clone(streams))),
            Source::Borrowed(streams) => streams.upgrade().map(Some).ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "compiler invocation finished")
            }),
            #[cfg(windows)]
            Source::Windows(_) => Err(io::Error::other(
                "frontend-owned Windows streams require a native IO plan",
            )),
        }
    }

    #[cfg(unix)]
    pub(crate) fn transfer_files(&self) -> io::Result<(Vec<File>, bool)> {
        let streams = self.streams()?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "compiler streams are not captured",
            )
        })?;
        let mut files = vec![
            streams.input.try_clone()?,
            streams.output.file.try_clone()?,
            streams.error.file.try_clone()?,
        ];
        if let Some(terminal) = &streams.terminal {
            files.push(terminal.file.try_clone()?);
        }
        Ok((files, streams.terminal.is_some()))
    }

    pub(crate) fn try_capture() -> io::Result<Self> {
        let stdio = kache_client::Stdio::capture()?;
        Ok(Self::from_files(
            stdio.input,
            stdio.output,
            stdio.error,
            stdio.terminal,
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
    #[cfg(target_os = "linux")]
    _workspace: kache_store::workspace::Scope,
}

impl InvocationIoScope {
    pub(crate) fn enter(io: InvocationIo) -> Self {
        #[cfg(target_os = "linux")]
        let workspace = kache_store::workspace::Scope::enter(
            io.linux_broker()
                .map(|broker| broker as Arc<dyn kache_store::workspace::Workspace>),
        );
        let previous = CURRENT.with(|slot| slot.replace(Some(io)));
        Self {
            previous,
            _warnings: kache_store::markers::InvocationWarningScope::enter(|message| {
                let _ = writeln!(stderr(), "{message}");
            }),
            _thread: std::marker::PhantomData,
            #[cfg(target_os = "linux")]
            _workspace: workspace,
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
    #[cfg(windows)]
    Windows(
        Arc<super::windows_broker::Broker>,
        kache_protocol::execution::windows::Output,
    ),
}

pub(crate) struct InvocationWriter(Sink);

impl Write for InvocationWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match &self.0 {
            Sink::Output => io::stdout().write(bytes),
            Sink::Error => io::stderr().write(bytes),
            Sink::File(output) => output.with_writer(|file| file.write(bytes)),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
            #[cfg(windows)]
            Sink::Windows(broker, output) => {
                broker.write(*output, bytes)?;
                Ok(bytes.len())
            }
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match &self.0 {
            Sink::Output => io::stdout().write_all(bytes),
            Sink::Error => io::stderr().write_all(bytes),
            Sink::File(output) => output.with_writer(|file| file.write_all(bytes)),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
            #[cfg(windows)]
            Sink::Windows(broker, output) => broker.write(*output, bytes),
        }
    }

    fn write_fmt(&mut self, args: std::fmt::Arguments<'_>) -> io::Result<()> {
        match &self.0 {
            Sink::Output => io::stdout().write_fmt(args),
            Sink::Error => io::stderr().write_fmt(args),
            Sink::File(output) => output.with_writer(|file| file.write_fmt(args)),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
            #[cfg(windows)]
            Sink::Windows(broker, output) => broker.write(*output, args.to_string().as_bytes()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &self.0 {
            Sink::Output => io::stdout().flush(),
            Sink::Error => io::stderr().flush(),
            Sink::File(output) => output.with_writer(|file| file.flush()),
            Sink::Closed => Err(io::ErrorKind::BrokenPipe.into()),
            #[cfg(windows)]
            Sink::Windows(broker, output) => broker.flush(*output),
        }
    }
}

pub(crate) fn stdout() -> InvocationWriter {
    #[cfg(windows)]
    if let Some(broker) = current().broker() {
        return InvocationWriter(Sink::Windows(
            broker,
            kache_protocol::execution::windows::Output::Stdout,
        ));
    }
    InvocationWriter(match current().streams() {
        Ok(Some(streams)) => Sink::File(Arc::clone(&streams.output)),
        Ok(None) => Sink::Output,
        Err(_) => Sink::Closed,
    })
}

pub(crate) fn stderr() -> InvocationWriter {
    #[cfg(windows)]
    if let Some(broker) = current().broker() {
        return InvocationWriter(Sink::Windows(
            broker,
            kache_protocol::execution::windows::Output::Stderr,
        ));
    }
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
    #[cfg(target_os = "linux")]
    if current().linux_broker().is_some() {
        use super::process::CommandExt as _;
        return command
            .invocation_spawn_with_io(kache_protocol::execution::windows::Stdio::INHERIT)?
            .wait();
    }
    #[cfg(windows)]
    if current().broker().is_some() {
        use super::process::CommandExt as _;
        return command
            .invocation_spawn_with_io(kache_protocol::execution::windows::Stdio::INHERIT)?
            .wait();
    }
    current().configure(command)?;
    super::process::status(command)
}

#[cfg(all(test, unix))]
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
            stdout().flush().unwrap();
            stderr().flush().unwrap();
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
        assert_eq!(
            stdout().flush().unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            stderr().flush().unwrap_err().kind(),
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
