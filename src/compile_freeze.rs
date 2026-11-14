//! Suspending a compiler at its dep-info report, so a compile that links
//! can be stopped on a cache hit without a linker outliving it.

/// A compiler suspended at its dep-info report, if it could be shown safe
/// to stop: Linux reports every thread stopped and no child process, so it
/// has started no linker that would outlive it (a wrapper that spawned the
/// real compiler fails this as well). rustc runs its linker only after
/// codegen, which a stopped compiler cannot reach. A process detached
/// before dep-info, such as one a proc macro spawned, is out of reach here
/// as it is for a compile that does not link. Dropping it resumes the
/// compiler; a stop keeps it suspended until the caller kills it.
pub(crate) struct Suspended(Option<u32>);

impl Suspended {
    /// Suspend `pid`. `None` when there is nothing to stop, or when it
    /// cannot be shown safe to stop; the compiler then runs on.
    pub(crate) fn new(pid: Option<u32>) -> Option<Self> {
        let pid = pid?;
        #[cfg(target_os = "linux")]
        {
            let raw = libc::pid_t::try_from(pid).ok()?;
            if unsafe { libc::kill(raw, libc::SIGSTOP) } != 0 {
                return None;
            }
            let suspended = Self(Some(pid));
            let proc_dir = std::path::Path::new("/proc").join(pid.to_string());
            (linux_stopped(&proc_dir) && !linux_has_children(&proc_dir)).then_some(suspended)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = pid;
            None
        }
    }

    /// Leave the compiler stopped for the kill that follows.
    pub(crate) fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for Suspended {
    fn drop(&mut self) {
        resume(self.0);
    }
}

/// Continue `pid` if it is stopped. Harmless on a running or killed one;
/// the caller has not reaped it, so the pid is still its own.
pub(crate) fn resume(pid: Option<u32>) {
    #[cfg(unix)]
    if let Some(pid) = pid.and_then(|pid| libc::pid_t::try_from(pid).ok()) {
        unsafe { libc::kill(pid, libc::SIGCONT) };
    }
    #[cfg(not(unix))]
    let _ = pid;
}

/// Whether every thread of the process reports itself stopped, waiting
/// briefly for the group stop to complete. Threads stop one at a time; a
/// thread still running could finish a fork after the children are read.
/// An exited compiler (a zombie) never qualifies.
#[cfg(target_os = "linux")]
fn linux_stopped(proc_dir: &std::path::Path) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(50);
    loop {
        match linux_thread_states(proc_dir) {
            Some(states) if !states.is_empty() && states.iter().all(|state| *state == 'T') => {
                return true;
            }
            None => return false,
            Some(_) if std::time::Instant::now() >= deadline => return false,
            Some(_) => std::thread::sleep(std::time::Duration::from_micros(200)),
        }
    }
}

/// The state of each thread, or `None` when the process cannot be read.
#[cfg(target_os = "linux")]
fn linux_thread_states(proc_dir: &std::path::Path) -> Option<Vec<char>> {
    std::fs::read_dir(proc_dir.join("task"))
        .ok()?
        .map(|task| {
            let stat = std::fs::read_to_string(task.ok()?.path().join("stat")).ok()?;
            linux_process_state(&stat)
        })
        .collect()
}

/// The state letter of a `/proc/<pid>/stat` line. The command name can hold
/// spaces and parentheses, so the state follows the last `)`.
#[cfg(any(target_os = "linux", test))]
fn linux_process_state(stat: &str) -> Option<char> {
    stat.rsplit_once(')')?.1.trim_start().chars().next()
}

/// Whether any thread of the process has a child. Unreadable counts as yes.
#[cfg(target_os = "linux")]
fn linux_has_children(proc_dir: &std::path::Path) -> bool {
    let Ok(tasks) = std::fs::read_dir(proc_dir.join("task")) else {
        return true;
    };
    tasks.into_iter().any(|task| {
        task.ok()
            .and_then(|task| std::fs::read_to_string(task.path().join("children")).ok())
            .is_none_or(|children| !children.trim().is_empty())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_process_state_follows_the_last_parenthesis() {
        assert_eq!(linux_process_state("42 (rustc) T 1 2"), Some('T'));
        assert_eq!(linux_process_state("42 (a) (b) R 1"), Some('R'));
        assert_eq!(linux_process_state("42 (odd)name) S 1"), Some('S'));
        assert_eq!(linux_process_state("no state"), None);
    }
}
