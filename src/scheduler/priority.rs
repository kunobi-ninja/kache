//! Whether the build that asks for a permit runs in the foreground.
//!
//! The answer describes the calling build, not the process that waits: a
//! request the daemon executes carries its client's priority in
//! [`InvocationPriorityScope`], and every other request reads its own
//! process. A child inherits its parent's priority, so a compiler wrapper
//! reports the priority of the Cargo, make or IDE that started it.
//!
//! - macOS: Darwin background (`taskpolicy -b`, `setpriority(PRIO_DARWIN_*)`)
//!   or a positive nice value.
//! - Linux: a positive nice value, or the `SCHED_BATCH` / `SCHED_IDLE`
//!   policy.
//! - Windows: the idle or below-normal priority class.
//! - Elsewhere, and whenever the process cannot be read: foreground.

/// Scheduling class of the build behind a permit request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Priority {
    #[default]
    Foreground,
    Background,
}

impl Priority {
    fn from_background(background: bool) -> Self {
        if background {
            Self::Background
        } else {
            Self::Foreground
        }
    }

    /// The priority of this process.
    pub(crate) fn of_this_process() -> Self {
        #[cfg(unix)]
        {
            Self::of_process(std::process::id())
        }
        #[cfg(windows)]
        {
            Self::from_background(this_process_is_background())
        }
        #[cfg(not(any(unix, windows)))]
        {
            Self::Foreground
        }
    }

    /// The priority of the process `pid`. The caller must know `pid` still
    /// names the process it means, such as the authenticated peer of an open
    /// connection.
    #[cfg(unix)]
    pub(crate) fn of_process(pid: u32) -> Self {
        Self::from_background(process_is_background(pid))
    }

    /// The priority a Windows priority class stands for.
    #[cfg(any(test, windows))]
    pub(crate) fn of_windows_class(class: u32) -> Self {
        Self::from_background(windows_background(class))
    }
}

thread_local! {
    static INVOCATION: std::cell::Cell<Option<Priority>> = const { std::cell::Cell::new(None) };
}

/// The priority of one synchronous request whose build is another process.
/// The daemon enters it with its client's priority; a request outside any
/// scope reads its own process.
pub(crate) struct InvocationPriorityScope {
    previous: Option<Priority>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl InvocationPriorityScope {
    pub(crate) fn enter(priority: Option<Priority>) -> Self {
        Self {
            previous: INVOCATION.with(|slot| slot.replace(priority)),
            _thread: std::marker::PhantomData,
        }
    }
}

impl Drop for InvocationPriorityScope {
    fn drop(&mut self) {
        INVOCATION.with(|slot| slot.set(self.previous));
    }
}

/// The priority of the build this thread is serving.
pub(crate) fn current() -> Priority {
    INVOCATION
        .with(std::cell::Cell::get)
        .unwrap_or_else(Priority::of_this_process)
}

/// Whether macOS reports a process as background: Darwin background set from
/// inside (`PROC_FLAG_DARWINBG`) or outside (`PROC_FLAG_EXT_DARWINBG`), or
/// lowered with nice.
#[cfg(any(test, target_os = "macos"))]
pub(crate) fn macos_background(flags: u32, nice: i32) -> bool {
    const DARWIN_BACKGROUND_FLAGS: u32 = 0x0001_8000;
    flags & DARWIN_BACKGROUND_FLAGS != 0 || nice > 0
}

/// Whether Linux runs a process as background: lowered with nice, or under
/// the batch or idle policy.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn linux_background(nice: i32, policy: i32) -> bool {
    const SCHED_BATCH: i32 = 3;
    const SCHED_IDLE: i32 = 5;
    nice > 0 || policy == SCHED_BATCH || policy == SCHED_IDLE
}

/// Whether a Windows priority class is below normal.
#[cfg(any(test, windows))]
pub(crate) fn windows_background(class: u32) -> bool {
    const IDLE_PRIORITY_CLASS: u32 = 0x40;
    const BELOW_NORMAL_PRIORITY_CLASS: u32 = 0x4000;
    class == IDLE_PRIORITY_CLASS || class == BELOW_NORMAL_PRIORITY_CLASS
}

#[cfg(target_os = "macos")]
// Linux mutation shards cannot execute this macOS syscall adapter. The
// classifier is exercised on every host; macOS CI also tests real processes.
#[mutants::skip]
fn process_is_background(pid: u32) -> bool {
    let Ok(pid) = libc::c_int::try_from(pid) else {
        return false;
    };
    // Niceness remains readable when the wider process-info query is denied.
    // SAFETY: getpriority only reads the scheduling state of this process.
    let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, pid as u32) };
    // SAFETY: `proc_bsdinfo` is a C struct of integers and arrays; zero is a
    // valid value, and the kernel writes at most `size` bytes into it.
    let mut info = unsafe { std::mem::zeroed::<libc::proc_bsdinfo>() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let written =
        unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDTBSDINFO, 0, (&raw mut info).cast(), size) };
    let flags = if written == size { info.pbi_flags } else { 0 };
    macos_background(flags, nice)
}

#[cfg(target_os = "linux")]
fn process_is_background(pid: u32) -> bool {
    let Ok(sched_pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // Both calls return -1 on failure: a raised nice value and no policy,
    // which reads as foreground.
    // SAFETY: both calls only read the scheduling state of `pid`.
    let nice = unsafe { libc::getpriority(libc::PRIO_PROCESS, pid) };
    // SAFETY: as above.
    let policy = unsafe { libc::sched_getscheduler(sched_pid) };
    linux_background(nice, policy)
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
#[mutants::skip] // Not compiled by the supported-platform CI matrix.
fn process_is_background(pid: u32) -> bool {
    let _ = pid;
    false
}

#[cfg(windows)]
#[mutants::skip] // Windows API adapter; its class decoder is tested on every host.
fn this_process_is_background() -> bool {
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetPriorityClass};
    // SAFETY: the pseudo handle of this process needs no closing.
    windows_background(unsafe { GetPriorityClass(GetCurrentProcess()) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macos_background_reads_both_darwin_flags_and_nice() {
        assert!(!macos_background(0, 0));
        assert!(macos_background(0x8000, 0), "set from inside");
        assert!(macos_background(0x10000, 0), "set by taskpolicy -b");
        assert!(macos_background(0, 1), "niced");
        assert!(!macos_background(0x21404010, 0), "a plain process");
        assert!(
            macos_background(0x21014010, 0),
            "the same after taskpolicy -b"
        );
        assert!(!macos_background(0, -5), "a raised priority is foreground");
    }

    #[test]
    fn linux_background_reads_nice_and_policy() {
        assert!(!linux_background(0, 0), "SCHED_OTHER at nice 0");
        assert!(linux_background(1, 0));
        assert!(!linux_background(-1, 0));
        assert!(linux_background(0, 3), "SCHED_BATCH");
        assert!(linux_background(0, 5), "SCHED_IDLE");
        assert!(!linux_background(0, 1), "SCHED_FIFO");
        assert!(!linux_background(0, 2), "SCHED_RR");
    }

    #[test]
    fn windows_background_is_idle_or_below_normal() {
        assert!(windows_background(0x40));
        assert!(windows_background(0x4000));
        assert!(!windows_background(0x20), "normal");
        assert!(!windows_background(0x8000), "above normal");
        assert!(!windows_background(0), "unreadable");
        assert_eq!(Priority::of_windows_class(0x40), Priority::Background);
        assert_eq!(Priority::of_windows_class(0x4000), Priority::Background);
        assert_eq!(Priority::of_windows_class(0x20), Priority::Foreground);
    }

    #[test]
    fn a_scope_replaces_the_process_until_dropped() {
        let own = Priority::of_this_process();
        assert_eq!(current(), own);
        {
            let _outer = InvocationPriorityScope::enter(Some(Priority::Background));
            assert_eq!(current(), Priority::Background);
            {
                let _inner = InvocationPriorityScope::enter(Some(Priority::Foreground));
                assert_eq!(current(), Priority::Foreground);
            }
            assert_eq!(current(), Priority::Background);
            let _none = InvocationPriorityScope::enter(None);
            assert_eq!(current(), own, "no client priority reads this process");
        }
        assert_eq!(current(), own);
    }

    #[cfg(unix)]
    #[test]
    fn a_niced_child_reads_as_background() {
        let mut calm = std::process::Command::new("sleep")
            .arg("5")
            .spawn()
            .unwrap();
        let mut niced = std::process::Command::new("nice")
            .args(["-n", "10", "sleep", "5"])
            .spawn()
            .unwrap();
        // `nice` execs `sleep` in place; wait until the new image runs niced.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while Priority::of_process(niced.id()) != Priority::Background
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let observed = (
            Priority::of_process(calm.id()),
            Priority::of_process(niced.id()),
        );
        let _ = calm.kill();
        let _ = niced.kill();
        let _ = calm.wait();
        let _ = niced.wait();
        // The suite itself may run niced, and children inherit it.
        if Priority::of_this_process() == Priority::Foreground {
            assert_eq!(observed.0, Priority::Foreground);
        }
        assert_eq!(observed.1, Priority::Background);
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "subprocess fixture for this_process_reads_its_own_nice"]
    fn niced_process_fixture() {
        let out = std::env::var_os("KACHE_TEST_PRIORITY_OUT").expect("fixture output");
        let observed = (Priority::of_this_process(), current());
        std::fs::write(out, format!("{observed:?}")).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn this_process_reads_its_own_nice() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("priority");
        let status = std::process::Command::new("nice")
            .args(["-n", "5"])
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "scheduler::priority::tests::niced_process_fixture",
                "--ignored",
                "--quiet",
            ])
            .env("KACHE_TEST_PRIORITY_OUT", &out)
            .status()
            .unwrap();
        assert!(status.success());
        assert_eq!(
            std::fs::read_to_string(out).unwrap(),
            "(Background, Background)"
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_process_is_foreground() {
        assert_eq!(Priority::of_process(u32::MAX), Priority::Foreground);
        assert_eq!(Priority::of_process(0x7fff_fff0), Priority::Foreground);
    }
}
