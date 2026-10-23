//! Compiler children owned by a cancellable request.

use kache_protocol::execution::windows::{Stdio as ChildIo, Stream as ChildStream};
use std::cell::RefCell;
use std::io::{self, Read};
use std::process::{ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Output, Stdio};
use std::sync::{Arc, Mutex, Weak};

#[cfg(windows)]
mod windows;

#[derive(Clone, Default)]
pub(crate) struct Cancellation(Arc<Mutex<Request>>);

#[derive(Default)]
struct Request {
    cancelled: bool,
    children: Vec<Weak<Mutex<Running>>>,
}

impl Cancellation {
    pub(crate) fn cancel(&self) {
        let mut request = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        request.cancelled = true;
        request.children.retain(|child| {
            let Some(child) = child.upgrade() else {
                return false;
            };
            let _ = child
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .kill();
            true
        });
    }

    fn check(&self) -> io::Result<()> {
        if self
            .0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .cancelled
        {
            Err(cancelled())
        } else {
            Ok(())
        }
    }
}

fn cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "compiler invocation cancelled")
}

thread_local! {
    static CURRENT: RefCell<Option<Cancellation>> = const { RefCell::new(None) };
}

pub(crate) struct CancellationScope {
    previous: Option<Cancellation>,
    cancel_on_drop: bool,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl CancellationScope {
    pub(crate) fn enter(cancellation: Option<Cancellation>) -> Self {
        Self {
            previous: CURRENT.with(|slot| slot.replace(cancellation)),
            cancel_on_drop: true,
            _thread: std::marker::PhantomData,
        }
    }
    /// Route a request into a joined helper thread without ending it when
    /// that helper finishes. The calling invocation retains completion ownership.
    pub(crate) fn borrow(cancellation: Option<Cancellation>) -> Self {
        let mut scope = Self::enter(cancellation);
        scope.cancel_on_drop = false;
        scope
    }
}

impl Drop for CancellationScope {
    fn drop(&mut self) {
        let finished = CURRENT.with(|slot| slot.replace(self.previous.take()));
        if self.cancel_on_drop
            && let Some(cancellation) = finished
        {
            cancellation.cancel();
        }
    }
}

pub(crate) fn current() -> Option<Cancellation> {
    CURRENT.with(|slot| slot.borrow().clone())
}

pub(crate) fn check() -> io::Result<()> {
    match current() {
        Some(cancellation) => cancellation.check(),
        None => Ok(()),
    }
}

struct Running {
    #[cfg(not(any(windows, target_os = "linux")))]
    child: std::process::Child,
    #[cfg(target_os = "linux")]
    child: super::linux_broker::Child,
    #[cfg(windows)]
    child: windows::Child,
    controlled: bool,
    status: Option<ExitStatus>,
    #[cfg(unix)]
    rss: Option<crate::opcounts::ChildRss>,
    #[cfg(windows)]
    tree: Option<windows::Job>,
}

impl Running {
    fn kill(&mut self) -> io::Result<()> {
        if self.status.is_some() {
            return Ok(());
        }
        #[cfg(target_os = "linux")]
        if self.child.remote() {
            return self.child.kill();
        }
        if self.controlled {
            #[cfg(unix)]
            crate::platform::kill_process_group(self.child.id());
            #[cfg(windows)]
            if let Some(tree) = &self.tree {
                return tree.kill();
            }
        }
        // Also handle a child that changed its own process group.
        self.child.kill()
    }

    fn wait(&mut self, blocking: bool) -> io::Result<Option<ExitStatus>> {
        if self.status.is_some() {
            return Ok(self.status);
        }
        #[cfg(target_os = "linux")]
        if self.child.remote() {
            let (status, max_rss) = self.child.try_wait_remote()?;
            self.status = status;
            if status.is_some()
                && let Some(rss) = &self.rss
                && let Some(bytes) = crate::scheduler::rss_from_getrusage(0, max_rss)
            {
                rss.record(bytes);
            }
            return Ok(status);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            let mut status = 0;
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
            loop {
                // The child remains owned until this lock records its status.
                // wait4 samples this PID, unlike process-wide RUSAGE_CHILDREN.
                let result = unsafe {
                    libc::wait4(
                        self.child.id() as libc::pid_t,
                        &mut status,
                        if blocking { 0 } else { libc::WNOHANG },
                        usage.as_mut_ptr(),
                    )
                };
                if result > 0 {
                    self.status = Some(ExitStatus::from_raw(status));
                    let usage = unsafe { usage.assume_init() };
                    if let Some(rss) = &self.rss
                        && let Some(bytes) =
                            crate::scheduler::rss_from_getrusage(0, usage.ru_maxrss)
                    {
                        rss.record(bytes);
                    }
                    return Ok(self.status);
                }
                if result == 0 {
                    return Ok(None);
                }
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
        #[cfg(not(unix))]
        {
            self.status = if blocking {
                Some(self.child.wait()?)
            } else {
                self.child.try_wait()?
            };
            Ok(self.status)
        }
    }
}

/// Reaping and cancellation share the same lock. A cancelled request never
/// signals a PID after this owner has reaped it and allowed PID reuse.
pub(crate) struct Child {
    running: Arc<Mutex<Running>>,
    pub(crate) stdin: Option<ChildStdin>,
    pub(crate) stdout: Option<ChildStdout>,
    pub(crate) stderr: Option<ChildStderr>,
    pid: u32,
}

impl Child {
    pub(crate) fn id(&self) -> u32 {
        self.pid
    }

    pub(crate) fn kill(&mut self) -> io::Result<()> {
        self.running
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .kill()
    }

    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        running.wait(false)
    }

    pub(crate) fn wait(&mut self) -> io::Result<ExitStatus> {
        // Match Child::wait: a child waiting for stdin must see EOF.
        drop(self.stdin.take());
        let running = self
            .running
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        #[cfg(target_os = "linux")]
        if running.child.remote() {
            drop(running);
            loop {
                if let Some(status) = self.try_wait()? {
                    return Ok(status);
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        if !running.controlled || running.status.is_some() {
            drop(running);
            return self.reap();
        }
        // Observe exit without reaping or holding the cancellation lock. The
        // child retains its PID until reap() takes that lock below.
        #[cfg(unix)]
        {
            drop(running);
            let mut info = std::mem::MaybeUninit::<libc::siginfo_t>::uninit();
            loop {
                let result = unsafe {
                    libc::waitid(
                        libc::P_PID,
                        self.pid as libc::id_t,
                        info.as_mut_ptr(),
                        libc::WEXITED | libc::WNOWAIT,
                    )
                };
                if result == 0 {
                    break;
                }
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            let handle = running.child.as_raw_handle();
            drop(running);
            windows::wait_for_exit(handle)?;
        }
        #[cfg(not(any(unix, windows)))]
        drop(running);
        self.reap()
    }

    fn reap(&mut self) -> io::Result<ExitStatus> {
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        running.wait(true)?.ok_or_else(|| {
            io::Error::other("blocking compiler wait returned without an exit status")
        })
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if running.controlled && running.status.is_none() {
            let _ = running.kill();
            let _ = running.wait(true);
        }
    }
}

pub(crate) trait CommandExt {
    fn invocation_spawn(&mut self) -> io::Result<Child>;
    fn invocation_spawn_with_io(&mut self, io: ChildIo) -> io::Result<Child>;
    fn invocation_spawn_in_group_with_io(&mut self, io: ChildIo) -> io::Result<Child>;
    /// Captured-output compiler commands use null stdin and two output pipes.
    fn invocation_output(&mut self) -> io::Result<Output>;
    /// Collect only streams the caller explicitly configured as pipes.
    #[cfg(all(test, unix))]
    fn invocation_capture(&mut self) -> io::Result<Output>;
    fn invocation_capture_with_io(&mut self, io: ChildIo) -> io::Result<Output>;
}

impl CommandExt for Command {
    fn invocation_spawn(&mut self) -> io::Result<Child> {
        spawn(self, false, None)
    }

    fn invocation_spawn_with_io(&mut self, io: ChildIo) -> io::Result<Child> {
        spawn(self, false, Some(io))
    }

    fn invocation_spawn_in_group_with_io(&mut self, io: ChildIo) -> io::Result<Child> {
        spawn(self, true, Some(io))
    }

    fn invocation_output(&mut self) -> io::Result<Output> {
        capture(self, Some(ChildIo::OUTPUT))
    }

    #[cfg(all(test, unix))]
    fn invocation_capture(&mut self) -> io::Result<Output> {
        capture(self, None)
    }

    fn invocation_capture_with_io(&mut self, io: ChildIo) -> io::Result<Output> {
        capture(self, Some(io))
    }
}

fn capture(command: &mut Command, io: Option<ChildIo>) -> io::Result<Output> {
    let mut child = spawn(command, false, io)?;
    let mut stdout = child.stdout.take();
    let mut stderr = child.stderr.take();
    std::thread::scope(|scope| {
        let running = Arc::clone(&child.running);
        let out = scope.spawn(move || {
            let mut bytes = Vec::new();
            let result = match stdout.as_mut() {
                Some(pipe) => pipe.read_to_end(&mut bytes).map(|_| bytes),
                None => Ok(bytes),
            };
            if result.is_err() {
                let _ = running
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .kill();
            }
            result
        });
        let mut err = Vec::new();
        let err_result = match stderr.as_mut() {
            Some(pipe) => pipe.read_to_end(&mut err),
            None => Ok(0),
        };
        if err_result.is_err() {
            let _ = child.kill();
        }
        let out_result = out.join();
        let status = child.wait();
        let stdout = out_result.map_err(|_| io::Error::other("child stdout reader panicked"))??;
        err_result?;
        Ok(Output {
            status: status?,
            stdout,
            stderr: err,
        })
    })
}

pub(crate) fn status(command: &mut Command) -> io::Result<ExitStatus> {
    command.invocation_spawn()?.wait()
}

fn spawn(command: &mut Command, force_group: bool, io: Option<ChildIo>) -> io::Result<Child> {
    #[cfg(unix)]
    kache_ipc::jobserver::configure(command);
    let cancellation = CURRENT.with(|slot| slot.borrow().clone());
    let mut request = cancellation
        .as_ref()
        .map(|token| token.0.lock().unwrap_or_else(|poison| poison.into_inner()));
    if request.as_ref().is_some_and(|request| request.cancelled) {
        return Err(cancelled());
    }
    let controlled = force_group || request.is_some();
    if let Some(io) = io {
        let stream = |mode| match mode {
            ChildStream::Inherit => Stdio::inherit(),
            ChildStream::Null => Stdio::null(),
            ChildStream::Pipe => Stdio::piped(),
        };
        command
            .stdin(stream(io.stdin))
            .stdout(stream(io.stdout))
            .stderr(stream(io.stderr));
    }
    #[cfg(unix)]
    if controlled {
        crate::platform::configure_process_group(command);
    }
    // Cancellation may run a host utility while holding the request lock.
    // Keep that lock order: request, then descriptor admission.
    #[cfg(unix)]
    let descriptor_admission = kache_ipc::descriptors::spawn_guard();
    #[cfg(windows)]
    let (mut child, tree) = if let Some(broker) = crate::invocation::io::current().broker() {
        let (id, pid, handles) = broker.spawn(
            command,
            io.ok_or_else(|| {
                io::Error::other("delegated compiler command requires an explicit IO plan")
            })?,
        )?;
        let (child, job) = windows::Child::remote(id, pid, handles, broker);
        (child, Some(job))
    } else if controlled {
        let (child, job) = windows::spawn(command)?;
        (windows::Child::local(child), Some(job))
    } else {
        (windows::Child::local(command.spawn()?), None)
    };
    #[cfg(target_os = "linux")]
    let mut child = if let Some(broker) = crate::invocation::io::current().linux_broker() {
        broker.spawn(
            command,
            io.ok_or_else(|| {
                io::Error::other("delegated compiler command requires an explicit IO plan")
            })?,
        )?
    } else {
        super::linux_broker::Child::local(command.spawn()?)
    };
    #[cfg(not(any(windows, target_os = "linux")))]
    let mut child = command.spawn()?;
    #[cfg(unix)]
    drop(descriptor_admission);
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let pid = child.id();
    let running = Arc::new(Mutex::new(Running {
        child,
        controlled,
        status: None,
        #[cfg(unix)]
        rss: crate::opcounts::child_rss_recorder(),
        #[cfg(windows)]
        tree,
    }));
    if let Some(request) = request.as_mut() {
        request.children.retain(|child| child.strong_count() != 0);
        request.children.push(Arc::downgrade(&running));
    }
    Ok(Child {
        running,
        stdin,
        stdout,
        stderr,
        pid,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_scopes_restore_after_panic_and_worker_reuse() {
        for _ in 0..2 {
            let parent = Cancellation::default();
            let nested = Cancellation::default();
            {
                let _parent = CancellationScope::enter(Some(parent.clone()));
                let result = std::panic::catch_unwind(|| {
                    let _nested = CancellationScope::enter(Some(nested.clone()));
                    nested.cancel();
                    assert_eq!(check().unwrap_err().kind(), io::ErrorKind::Interrupted);
                    panic!("nested request failed");
                });
                assert!(result.is_err());
                {
                    let _borrowed = CancellationScope::borrow(current());
                    check().unwrap();
                }
                check().unwrap();
                assert!(nested.check().is_err());
                parent.check().unwrap();
            }
            assert!(parent.check().is_err());
            check().unwrap();
        }
    }

    #[cfg(unix)]
    fn shell(script: &str) -> Command {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", script]).env("PATH", env!("PATH"));
        command
    }

    #[cfg(unix)]
    #[test]
    fn spawning_another_child_discards_completed_registrations() {
        let request = Cancellation::default();
        let _scope = CancellationScope::enter(Some(request.clone()));
        let mut first = shell("exit 7").invocation_spawn().unwrap();
        assert_eq!(first.wait().unwrap().code(), Some(7));
        drop(first);
        let mut second = shell("exit 9").invocation_spawn().unwrap();
        {
            let registered = request.0.lock().unwrap();
            assert_eq!(registered.children.len(), 1);
            assert!(Arc::ptr_eq(
                &registered.children[0].upgrade().unwrap(),
                &second.running,
            ));
        }
        assert_eq!(second.wait().unwrap().code(), Some(9));
    }

    #[cfg(unix)]
    fn wait_for_file(path: &std::path::Path) {
        let started = std::time::Instant::now();
        while !path.exists() {
            assert!(
                started.elapsed() < std::time::Duration::from_secs(3),
                "child never became ready"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    #[cfg(unix)]
    #[test]
    fn cancelling_one_request_kills_its_descendants_and_keeps_another_running() {
        let root = tempfile::tempdir().unwrap();
        let ready_a = root.path().join("a.ready");
        let ready_b = root.path().join("b.ready");
        let release_b = root.path().join("b.release");
        let first = Cancellation::default();
        let second = Cancellation::default();
        std::thread::scope(|threads| {
            let first_worker = threads.spawn(|| {
                let _scope = CancellationScope::enter(Some(first.clone()));
                let output = shell("printf 'a-start\\n'; (sleep 4; printf 'a-descendant\\n') & printf ready > \"$READY\"; wait; printf 'a-end\\n'")
                    .env("READY", &ready_a).invocation_output().unwrap();
                assert!(!output.status.success());
                assert_eq!(output.stdout, b"a-start\n");
                assert!(output.stderr.is_empty());
                assert_eq!(shell("exit 0").invocation_spawn().err().unwrap().kind(), io::ErrorKind::Interrupted);
            });
            let second_worker = threads.spawn(|| {
                let _scope = CancellationScope::enter(Some(second.clone()));
                shell("printf ready > \"$READY\"; i=0; while [ ! -e \"$RELEASE\" ] && [ $i -lt 40 ]; do sleep .1; i=$((i+1)); done; printf 'b-output\\n'; printf 'b-error\\n' >&2; exit 7")
                    .env("READY", &ready_b).env("RELEASE", &release_b).invocation_output().unwrap()
            });
            wait_for_file(&ready_a);
            wait_for_file(&ready_b);
            let cancelled_at = std::time::Instant::now();
            first.cancel();
            first_worker.join().unwrap();
            assert!(cancelled_at.elapsed() < std::time::Duration::from_secs(2));
            second.check().unwrap();
            std::fs::write(&release_b, b"go").unwrap();
            let output = second_worker.join().unwrap();
            assert_eq!(output.status.code(), Some(7));
            assert_eq!(output.stdout, b"b-output\n");
            assert_eq!(output.stderr, b"b-error\n");
        });
    }

    #[cfg(unix)]
    #[test]
    fn child_memory_belongs_to_its_spawning_invocation_when_reaped_elsewhere() {
        let _parent = crate::opcounts::InvocationScope::enter(std::time::Instant::now());
        let mut child = shell("exit 7").invocation_spawn().unwrap();
        assert_eq!(crate::opcounts::invocation_child_rss(), Some(None));
        {
            let _nested = crate::opcounts::InvocationScope::enter(std::time::Instant::now());
            std::thread::scope(|threads| {
                threads.spawn(|| {
                    let _other = crate::opcounts::InvocationScope::enter(std::time::Instant::now());
                    assert_eq!(child.wait().unwrap().code(), Some(7));
                    assert_eq!(child.try_wait().unwrap().unwrap().code(), Some(7));
                    assert_eq!(child.wait().unwrap().code(), Some(7));
                    assert_eq!(crate::opcounts::invocation_child_rss(), Some(None));
                });
            });
            assert_eq!(crate::opcounts::invocation_child_rss(), Some(None));
        }
        assert!(crate::opcounts::invocation_child_rss().unwrap().unwrap() > 1024);
    }

    #[cfg(unix)]
    #[test]
    fn child_id_matches_the_native_process_before_and_after_reaping() {
        let _scope = CancellationScope::enter(Some(Cancellation::default()));
        let mut child = shell("printf '%s\\n' \"$$\"; exit 23")
            .stdout(Stdio::piped())
            .invocation_spawn()
            .unwrap();
        let reported_id = child.id();
        assert_eq!(child.wait().unwrap().code(), Some(23));
        let mut output = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();
        let native_id: u32 = output.trim().parse().unwrap();
        assert!(native_id > 1);
        assert_eq!(reported_id, native_id);
        assert_eq!(child.id(), native_id);
    }

    #[cfg(unix)]
    #[test]
    fn dropping_an_uncontrolled_child_leaves_it_running() {
        use std::io::Write;

        let _scope = CancellationScope::enter(None);
        let root = tempfile::tempdir().unwrap();
        let finished = root.path().join("finished");
        let mut child = shell(
            "IFS= read -r line; [ \"$line\" = release ] || exit 39; printf survived > \"$FINISHED\"; exit 17",
        )
        .env("FINISHED", &finished)
        .stdin(Stdio::piped())
        .invocation_spawn()
        .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        // Retain the native owner so the test can reap it after the wrapper drops.
        let running = Arc::clone(&child.running);
        drop(child);
        let release = writeln!(stdin, "release");
        drop(stdin);
        let status = running.lock().unwrap().child.wait();
        release.unwrap();
        assert_eq!(status.unwrap().code(), Some(17));
        assert_eq!(std::fs::read(&finished).unwrap(), b"survived");
    }

    #[cfg(unix)]
    #[test]
    fn polling_and_blocking_waits_return_unrecoverable_errors() {
        for mode in ["poll", "blocking"] {
            let mut fixture = crate::platform::host_spawn(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "invocation::process::tests::externally_reaped_child_fixture",
                        "--ignored",
                        "--nocapture",
                    ])
                    .env("KACHE_TEST_WAIT_ERROR", mode)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped()),
            )
            .unwrap();
            let started = std::time::Instant::now();
            loop {
                if fixture.try_wait().unwrap().is_some() {
                    let output = fixture.wait_with_output().unwrap();
                    assert!(output.status.success(), "{output:?}");
                    break;
                }
                if started.elapsed() > std::time::Duration::from_secs(5) {
                    fixture.kill().unwrap();
                    let output = fixture.wait_with_output().unwrap();
                    panic!("wait retried an unrecoverable error: {output:?}");
                }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "subprocess fixture for bounded native wait-error coverage"]
    fn externally_reaped_child_fixture() {
        let blocking = std::env::var("KACHE_TEST_WAIT_ERROR").unwrap() == "blocking";
        let _scope = CancellationScope::enter(blocking.then(Cancellation::default));
        let mut child = shell("exit 7").invocation_spawn().unwrap();
        if blocking {
            // Observe a PID which cannot be our child, while retaining the
            // native child's ownership for safe cancellation and reaping.
            child.pid = std::process::id();
            assert_eq!(child.wait().unwrap_err().raw_os_error(), Some(libc::ECHILD));
            return;
        }
        assert_eq!(
            child.running.lock().unwrap().child.wait().unwrap().code(),
            Some(7)
        );
        assert_eq!(
            child.try_wait().unwrap_err().raw_os_error(),
            Some(libc::ECHILD)
        );
    }

    #[cfg(unix)]
    #[test]
    fn polling_a_running_child_does_not_record_a_memory_sample() {
        let root = tempfile::tempdir().unwrap();
        let ready = root.path().join("ready");
        let release = root.path().join("release");
        let _counts = crate::opcounts::InvocationScope::enter(std::time::Instant::now());
        let _cancel = CancellationScope::enter(Some(Cancellation::default()));
        let mut child = shell(
            "printf ready > \"$READY\"; while [ ! -e \"$RELEASE\" ]; do sleep .01; done; exit 6",
        )
        .env("READY", &ready)
        .env("RELEASE", &release)
        .invocation_spawn()
        .unwrap();
        wait_for_file(&ready);
        assert_eq!(child.try_wait().unwrap(), None);
        assert_eq!(crate::opcounts::invocation_child_rss(), Some(None));
        std::fs::write(&release, b"go").unwrap();
        let started = std::time::Instant::now();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert_eq!(status.code(), Some(6));
                break;
            }
            assert!(started.elapsed() < std::time::Duration::from_secs(3));
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(crate::opcounts::invocation_child_rss().unwrap().unwrap() > 1024);
        assert_eq!(child.wait().unwrap().code(), Some(6));
    }

    #[cfg(unix)]
    #[test]
    fn dropping_a_child_kills_and_reaps_it_without_cancelling_the_request() {
        let _counts = crate::opcounts::InvocationScope::enter(std::time::Instant::now());
        let token = Cancellation::default();
        let _scope = CancellationScope::enter(Some(token.clone()));
        let started = std::time::Instant::now();
        let child = shell("exec sleep 4").invocation_spawn().unwrap();
        let pid = child.id();
        drop(child);
        assert!(crate::opcounts::invocation_child_rss().unwrap().unwrap() > 0);
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        let mut wait_status = 0;
        assert_eq!(
            unsafe { libc::waitpid(pid as libc::pid_t, &mut wait_status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
        token.check().unwrap();
        assert!(status(&mut shell("exit 0")).unwrap().success());
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_interrupts_a_blocking_wait() {
        let root = tempfile::tempdir().unwrap();
        let ready = root.path().join("ready");
        let token = Cancellation::default();
        std::thread::scope(|threads| {
            let worker = threads.spawn(|| {
                let _scope = CancellationScope::enter(Some(token.clone()));
                status(shell("printf ready > \"$READY\"; exec sleep 4").env("READY", &ready))
                    .unwrap()
            });
            wait_for_file(&ready);
            std::thread::sleep(std::time::Duration::from_millis(50));
            let started = std::time::Instant::now();
            token.cancel();
            assert!(!worker.join().unwrap().success());
            assert!(started.elapsed() < std::time::Duration::from_secs(2));
        });
    }

    #[cfg(unix)]
    #[test]
    fn controlled_output_preserves_bytes_and_wait_closes_stdin() {
        let _scope = CancellationScope::enter(Some(Cancellation::default()));
        let output = shell("printf '\\377out'; printf '\\376err' >&2; exit 9")
            .invocation_output()
            .unwrap();
        assert_eq!(output.status.code(), Some(9));
        assert_eq!(output.stdout, b"\xffout");
        assert_eq!(output.stderr, b"\xfeerr");
        let mut child = shell("exec cat >/dev/null")
            .stdin(Stdio::piped())
            .invocation_spawn()
            .unwrap();
        assert!(child.wait().unwrap().success());
        let output = shell("printf ignored; printf retained >&2")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .invocation_capture()
            .unwrap();
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
        assert_eq!(output.stderr, b"retained");
    }

    #[cfg(unix)]
    #[test]
    fn completed_children_keep_their_exit_status_after_late_cancellation() {
        let token = Cancellation::default();
        let _scope = CancellationScope::enter(Some(token.clone()));
        let mut child = shell("exit 7").invocation_spawn().unwrap();
        assert_eq!(child.wait().unwrap().code(), Some(7));
        token.cancel();
        assert_eq!(child.try_wait().unwrap().unwrap().code(), Some(7));
        assert_eq!(child.wait().unwrap().code(), Some(7));
        assert_eq!(
            shell("exit 0").invocation_output().unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
    }
}
