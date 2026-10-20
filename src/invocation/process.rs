//! Compiler children owned by a cancellable request.

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
    child: std::process::Child,
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
            if unsafe {
                windows_sys::Win32::System::Threading::WaitForSingleObject(handle, u32::MAX)
            } == windows_sys::Win32::Foundation::WAIT_FAILED
            {
                return Err(io::Error::last_os_error());
            }
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
    fn invocation_spawn_in_group(&mut self) -> io::Result<Child>;
    /// Captured-output compiler commands use null stdin and two output pipes.
    fn invocation_output(&mut self) -> io::Result<Output>;
    /// Collect only streams the caller explicitly configured as pipes.
    fn invocation_capture(&mut self) -> io::Result<Output>;
}

impl CommandExt for Command {
    fn invocation_spawn(&mut self) -> io::Result<Child> {
        spawn(self, false)
    }

    fn invocation_spawn_in_group(&mut self) -> io::Result<Child> {
        spawn(self, true)
    }

    fn invocation_output(&mut self) -> io::Result<Output> {
        self.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .invocation_capture()
    }

    fn invocation_capture(&mut self) -> io::Result<Output> {
        let mut child = self.invocation_spawn()?;
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
            let stdout =
                out_result.map_err(|_| io::Error::other("child stdout reader panicked"))??;
            err_result?;
            Ok(Output {
                status: status?,
                stdout,
                stderr: err,
            })
        })
    }
}

pub(crate) fn status(command: &mut Command) -> io::Result<ExitStatus> {
    command.invocation_spawn()?.wait()
}

fn spawn(command: &mut Command, force_group: bool) -> io::Result<Child> {
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
    #[cfg(unix)]
    if controlled {
        crate::platform::configure_process_group(command);
    }
    // Cancellation may run a host utility while holding the request lock.
    // Keep that lock order: request, then descriptor admission.
    #[cfg(unix)]
    let descriptor_admission = kache_ipc::descriptors::spawn_guard();
    #[cfg(windows)]
    let (mut child, tree) = if controlled {
        let (child, job) = windows::spawn(command)?;
        (child, Some(job))
    } else {
        (command.spawn()?, None)
    };
    #[cfg(not(windows))]
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
        command.args(["-c", script]);
        command
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
                let output = shell("printf 'a-start\\n'; (/bin/sleep 4; printf 'a-descendant\\n') & printf ready > \"$READY\"; wait; printf 'a-end\\n'")
                    .env("READY", &ready_a).invocation_output().unwrap();
                assert!(!output.status.success());
                assert_eq!(output.stdout, b"a-start\n");
                assert!(output.stderr.is_empty());
                assert_eq!(shell("exit 0").invocation_spawn().err().unwrap().kind(), io::ErrorKind::Interrupted);
            });
            let second_worker = threads.spawn(|| {
                let _scope = CancellationScope::enter(Some(second.clone()));
                shell("printf ready > \"$READY\"; i=0; while [ ! -e \"$RELEASE\" ] && [ $i -lt 40 ]; do /bin/sleep .1; i=$((i+1)); done; printf 'b-output\\n'; printf 'b-error\\n' >&2; exit 7")
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
    fn polling_a_running_child_does_not_record_a_memory_sample() {
        let root = tempfile::tempdir().unwrap();
        let ready = root.path().join("ready");
        let release = root.path().join("release");
        let _counts = crate::opcounts::InvocationScope::enter(std::time::Instant::now());
        let _cancel = CancellationScope::enter(Some(Cancellation::default()));
        let mut child = shell(
            "printf ready > \"$READY\"; while [ ! -e \"$RELEASE\" ]; do /bin/sleep .01; done; exit 6",
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
        let child = shell("exec /bin/sleep 4").invocation_spawn().unwrap();
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
                status(shell("printf ready > \"$READY\"; exec /bin/sleep 4").env("READY", &ready))
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
        let mut child = shell("exec /bin/cat >/dev/null")
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
