//! Tells a running process apart from a later one that reuses its number.
//!
//! A process number outlives its process: the kernel hands it to the next
//! process, and on Linux a thread's ID answers `kill(id, 0)` and has a
//! `/proc/<id>` of its own. A record that keeps only the number therefore
//! reads as alive for as long as anything holds that number. Recording when
//! the process started, too, tells the two apart.

/// One run of a process: its number and, where the OS reports one, its start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProcessIdentity {
    pid: u32,
    start: Start,
}

/// When a process started, in the OS's own units: clock ticks since boot on
/// Linux (`/proc/<pid>/stat` field 22).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    At(u64),
    /// Nothing recorded: the record predates starts, the process was gone
    /// when it was looked up, or the platform reports no start.
    Unrecorded,
}

/// What the OS reports for a process number now.
enum Found {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Started(u64),
    /// No process has this number, it names a thread rather than a process,
    /// or the process has exited and only waits for its parent.
    Gone,
    /// This platform reports no start.
    #[cfg_attr(target_os = "linux", allow(dead_code))]
    Unsupported,
}

impl ProcessIdentity {
    pub(crate) fn current() -> Self {
        Self::of(std::process::id())
    }

    /// The process holding `pid` now.
    pub(crate) fn of(pid: u32) -> Self {
        let start = match found(pid) {
            Found::Started(at) => Start::At(at),
            Found::Gone | Found::Unsupported => Start::Unrecorded,
        };
        Self { pid, start }
    }

    /// A process read back from a record that names its start.
    pub(crate) fn started_at(pid: u32, start: u64) -> Self {
        Self {
            pid,
            start: Start::At(start),
        }
    }

    /// A process read back from a record that names no start.
    pub(crate) fn unrecorded(pid: u32) -> Self {
        Self {
            pid,
            start: Start::Unrecorded,
        }
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }

    /// The start to write into a record, where one is known.
    pub(crate) fn start(&self) -> Option<u64> {
        match self.start {
            Start::At(at) => Some(at),
            Start::Unrecorded => None,
        }
    }

    /// Whether this very process is still running. Where the platform
    /// reports starts, that takes a process at this number that leads its
    /// thread group and started when this one did; a record without a start
    /// cannot tell its process from a later one, so it is not running.
    /// Elsewhere this is [`crate::daemon::process_is_alive`].
    pub(crate) fn is_running(&self) -> bool {
        match (found(self.pid), self.start) {
            (Found::Started(now), Start::At(then)) => now == then,
            (Found::Started(_), Start::Unrecorded) | (Found::Gone, _) => false,
            (Found::Unsupported, _) => crate::daemon::process_is_alive(self.pid),
        }
    }
}

/// PIDs 0 and 1 and values past `i32::MAX` never name a process kache
/// records: the first is the kernel's, the second init's, and the last read
/// as broadcasts to `kill`.
fn found(pid: u32) -> Found {
    if pid <= 1 || i32::try_from(pid).is_err() {
        return Found::Gone;
    }
    found_on_this_platform(pid)
}

#[cfg(target_os = "linux")]
fn found_on_this_platform(pid: u32) -> Found {
    let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) else {
        return Found::Gone;
    };
    if thread_group(&status) != Some(pid) {
        return Found::Gone;
    }
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => started_from_stat(&stat).map_or(Found::Gone, Found::Started),
        Err(_) => Found::Gone,
    }
}

#[cfg(not(target_os = "linux"))]
fn found_on_this_platform(_pid: u32) -> Found {
    Found::Unsupported
}

/// The `Tgid` line of `/proc/<pid>/status`: the process a thread belongs to.
#[cfg(target_os = "linux")]
fn thread_group(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("Tgid:"))
        .and_then(|tgid| tgid.trim().parse().ok())
}

/// Field 22 of `/proc/<pid>/stat`, unless field 3 says the process has
/// exited (`Z` or `X`). The name in field 2 may hold spaces and parentheses,
/// so fields are counted from the last `)`.
#[cfg(target_os = "linux")]
fn started_from_stat(stat: &str) -> Option<u64> {
    let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_ascii_whitespace();
    if matches!(fields.next()?, "Z" | "X") {
        return None;
    }
    // Fields 4 to 21 lie between the state and the start.
    fields.nth(18)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_process_is_running_and_numbers_kache_never_records_are_not() {
        assert!(ProcessIdentity::current().is_running());
        for pid in [0, 1, u32::MAX] {
            assert!(!ProcessIdentity::of(pid).is_running(), "pid {pid}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_process_runs_until_it_exits_and_only_under_its_own_start() {
        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .spawn()
            .unwrap();
        let running = ProcessIdentity::of(child.id());
        let start = running.start().expect("a running child reports its start");
        assert!(running.is_running());
        assert!(!ProcessIdentity::started_at(child.id(), start + 1).is_running());
        assert!(!ProcessIdentity::unrecorded(child.id()).is_running());
        child.kill().unwrap();
        // Wait for the exit without reaping, so the number is still the child's.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                child.id(),
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        assert_eq!(waited, 0, "{}", std::io::Error::last_os_error());
        assert!(!running.is_running());
        child.wait().unwrap();
        assert!(!running.is_running());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_thread_id_never_reads_as_a_process() {
        let (sent, received) = std::sync::mpsc::channel();
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            sent.send(unsafe { libc::gettid() } as u32).unwrap();
            let _ = stopped.recv();
        });
        let tid = received.recv().unwrap();
        let stat = std::fs::read_to_string(format!("/proc/{tid}/stat")).unwrap();
        // Even a record holding the thread's own start names no process.
        let start = started_from_stat(&stat).unwrap();
        assert!(!ProcessIdentity::started_at(tid, start).is_running());
        assert_eq!(ProcessIdentity::of(tid).start(), None);
        drop(stop);
        thread.join().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_start_is_read_past_a_name_holding_spaces_and_parentheses() {
        let fields: String = (4..=21).map(|n| format!(" {n}")).collect();
        let stat = format!("42 (a) b (c) S{fields} 9876 23 24\n");
        assert_eq!(started_from_stat(&stat), Some(9876));
        assert_eq!(started_from_stat(&stat.replace(") S ", ") Z ")), None);
    }
}
