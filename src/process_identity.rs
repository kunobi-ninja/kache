//! Tells a running process apart from a later one that reuses its number.
//!
//! A process number outlives its process: the kernel hands it to the next
//! process, and on Linux a thread's ID answers `kill(id, 0)` and has a
//! `/proc/<id>` of its own. A record that keeps only the number therefore
//! reads as alive for as long as anything holds that number. Recording when
//! the process started, too, tells the two apart.

#[cfg(not(target_os = "linux"))]
use crate::daemon::process_is_alive as process_may_be_running;

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
    /// The first probe established that this number did not name a process.
    Exited,
    /// The record predates starts, or the OS could not establish one while
    /// the process might still be running.
    Unrecorded,
}

/// What the OS reports for a process number now.
#[derive(Debug, PartialEq, Eq)]
enum Found {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    Started(u64),
    /// A reserved PID or a thread ID rather than a process Kache records.
    Gone,
    /// This platform reports no start, or reading it was inconclusive.
    Unknown,
}

impl ProcessIdentity {
    pub(crate) fn current() -> Self {
        Self::of(std::process::id())
    }

    /// The process holding `pid` now.
    pub(crate) fn of(pid: u32) -> Self {
        Self::from_found(pid, found(pid), || process_may_be_running(pid))
    }

    fn from_found(pid: u32, found: Found, probe: impl FnOnce() -> bool) -> Self {
        let start = match found {
            Found::Started(at) => Start::At(at),
            Found::Gone => Start::Exited,
            Found::Unknown => {
                if probe() {
                    Start::Unrecorded
                } else {
                    Start::Exited
                }
            }
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
            Start::Exited | Start::Unrecorded => None,
        }
    }

    /// Whether this process may still be running. Where the platform
    /// reports starts, that takes a process at this number that leads its
    /// thread group and started when this one did. Legacy records and
    /// inconclusive probes retain the conservative liveness check: missing
    /// identity information alone does not establish that a process exited.
    pub(crate) fn is_running(&self) -> bool {
        self.is_running_from(found(self.pid), || process_may_be_running(self.pid))
    }

    fn is_running_from(&self, found: Found, probe: impl FnOnce() -> bool) -> bool {
        match (found, self.start) {
            (_, Start::Exited) => false,
            (Found::Started(now), Start::At(then)) => now == then,
            (Found::Started(_), Start::Unrecorded) => true,
            (Found::Gone, _) => false,
            (Found::Unknown, _) => probe(),
        }
    }
}

/// An inconclusive identity read must not mistake a dead group leader for
/// the whole process exiting. On Linux, kill(pid, 0) keeps live workers and
/// treats errors other than a confirmed missing process conservatively.
#[cfg(target_os = "linux")]
fn process_may_be_running(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 1 {
        return false;
    }
    // SAFETY: a positive process ID and signal 0 only probe existence and
    // permission. No signal is delivered and no caller memory is accessed.
    let result = unsafe { libc::kill(pid, 0) };
    liveness_from_signal_probe(result, || std::io::Error::last_os_error().raw_os_error())
}

#[cfg(target_os = "linux")]
fn liveness_from_signal_probe(result: libc::c_int, error: impl FnOnce() -> Option<i32>) -> bool {
    result == 0 || error() != Some(libc::ESRCH)
}

/// PID 1 names init, which Kache excludes from its records. PID 0 has no
/// procfs entry; the fallback also rejects it and values that would become
/// broadcasts to `kill`.
fn found(pid: u32) -> Found {
    if pid == 1 {
        return Found::Gone;
    }
    #[cfg(target_os = "linux")]
    {
        found_on_this_platform(pid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Found::Unknown
    }
}

#[cfg(target_os = "linux")]
fn found_on_this_platform(pid: u32) -> Found {
    found_in_proc(pid, |file| read_proc_file(format!("/proc/{pid}/{file}")))
}

#[cfg(target_os = "linux")]
fn read_proc_file(path: impl AsRef<std::path::Path>) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    // Only the process name may contain non-UTF-8 bytes. Its contents do not
    // affect the ASCII Tgid, state or start-time fields we need.
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

#[cfg(target_os = "linux")]
fn found_in_proc(pid: u32, read: impl Fn(&str) -> std::io::Result<String>) -> Found {
    let status = match read("status") {
        Ok(status) => status,
        Err(_) => return Found::Unknown,
    };
    match thread_group(&status) {
        Some(tgid) if tgid != pid => return Found::Gone,
        None => return Found::Unknown,
        Some(_) => {}
    }
    match read("stat") {
        Ok(stat) => process_from_stat(&stat),
        Err(_) => Found::Unknown,
    }
}

/// The `Tgid` line of `/proc/<pid>/status`: the process a thread belongs to.
#[cfg(target_os = "linux")]
fn thread_group(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("Tgid:"))
        .and_then(|tgid| tgid.trim().parse().ok())
}

/// Field 22 of `/proc/<pid>/stat`. An exited group leader can still have live
/// worker threads, so its state alone does not establish process exit. The
/// name in field 2 may hold spaces and parentheses; count from the last `)`.
#[cfg(target_os = "linux")]
fn process_from_stat(stat: &str) -> Found {
    let Some((_, tail)) = stat.rsplit_once(')') else {
        return Found::Unknown;
    };
    let mut fields = tail.split_ascii_whitespace();
    let state = match fields.next() {
        Some(state @ ("R" | "S" | "D" | "T" | "t" | "P" | "I" | "Z" | "X")) => state,
        _ => return Found::Unknown,
    };
    // Field 20 and state come from the same stat snapshot. A prior status
    // read could say one thread just before this process creates a worker.
    let threads = fields
        .nth(16)
        .and_then(|threads| threads.parse::<u32>().ok());
    if matches!(state, "Z" | "X") && threads == Some(1) {
        return Found::Gone;
    }
    // Field 21 lies between the thread count and the start (field 22).
    fields
        .nth(1)
        .and_then(|start| start.parse().ok())
        .map_or(Found::Unknown, Found::Started)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    struct RunningChild(std::process::Child);

    #[cfg(target_os = "linux")]
    impl Drop for RunningChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_signal_probe_requires_a_confirmed_missing_process_to_report_exit() {
        assert!(liveness_from_signal_probe(0, || panic!(
            "success ignores stale errno"
        )));
        assert!(!liveness_from_signal_probe(-1, || Some(libc::ESRCH)));
        for error in [Some(libc::EPERM), Some(libc::EIO), None] {
            assert!(liveness_from_signal_probe(-1, || error));
        }
    }

    #[test]
    fn unknown_identity_uses_liveness_and_legacy_records_keep_live_processes() {
        let recorded = ProcessIdentity::started_at(42, 17);
        assert!(recorded.is_running_from(Found::Started(17), || panic!("known start")));
        assert!(!recorded.is_running_from(Found::Started(18), || panic!("different start")));
        assert!(!recorded.is_running_from(Found::Gone, || panic!("confirmed exit")));
        for process in [recorded, ProcessIdentity::unrecorded(42)] {
            assert!(process.is_running_from(Found::Unknown, || true));
            assert!(!process.is_running_from(Found::Unknown, || false));
        }
        assert!(
            ProcessIdentity::unrecorded(42)
                .is_running_from(Found::Started(17), || panic!("known live process"))
        );
        let exited = ProcessIdentity {
            pid: 42,
            start: Start::Exited,
        };
        assert_eq!(exited.start(), None);
        for found in [Found::Started(17), Found::Gone, Found::Unknown] {
            assert!(!exited.is_running_from(found, || panic!("confirmed exit is final")));
        }
        for alive in [false, true] {
            let initially_unknown = ProcessIdentity::from_found(42, Found::Unknown, || alive);
            assert_eq!(
                initially_unknown.is_running_from(Found::Started(18), || panic!("known start")),
                alive,
                "a confirmed exit must not become a legacy record"
            );
        }
    }

    #[test]
    fn this_process_is_running_and_numbers_kache_never_records_are_not() {
        assert!(ProcessIdentity::current().is_running());
        for pid in [0, 1, u32::MAX] {
            assert!(!ProcessIdentity::of(pid).is_running(), "pid {pid}");
            assert!(!process_may_be_running(pid), "invalid probe pid {pid}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_process_runs_until_it_exits_and_only_under_its_own_start() {
        let mut child = RunningChild(
            std::process::Command::new("sleep")
                .arg("60")
                .spawn()
                .unwrap(),
        );
        let running = ProcessIdentity::of(child.0.id());
        let start = running.start().expect("a running child reports its start");
        assert!(running.is_running());
        assert!(!ProcessIdentity::started_at(child.0.id(), start + 1).is_running());
        assert!(ProcessIdentity::unrecorded(child.0.id()).is_running());
        child.0.kill().unwrap();
        // Wait for the exit without reaping, so the number is still the child's.
        // SAFETY: siginfo_t is a plain C output value; waitid receives its
        // valid pointer and waits only for this test's unreaped child.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // SAFETY: the child belongs to this test, info is writable, and
        // WNOWAIT leaves the exit available for Child::wait to reap.
        let waited = unsafe {
            libc::waitid(
                libc::P_PID,
                child.0.id(),
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        assert_eq!(waited, 0, "{}", std::io::Error::last_os_error());
        // This fallback deliberately cannot distinguish a dead leader from
        // a group with live workers. The stat thread count must decide.
        assert!(process_may_be_running(child.0.id()));
        assert!(!running.is_running());
        child.0.wait().unwrap();
        assert!(!running.is_running());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_thread_id_never_reads_as_a_process() {
        let (sent, received) = std::sync::mpsc::channel();
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            // SAFETY: gettid has no arguments and returns this thread's ID.
            sent.send(unsafe { libc::gettid() } as u32).unwrap();
            let _ = stopped.recv();
        });
        let tid = received.recv().unwrap();
        let stat = std::fs::read_to_string(format!("/proc/{tid}/stat")).unwrap();
        // Even a record holding the thread's own start names no process.
        let Found::Started(start) = process_from_stat(&stat) else {
            panic!("a live thread has a start time");
        };
        assert!(!ProcessIdentity::started_at(tid, start).is_running());
        let process = ProcessIdentity::of(tid);
        assert_eq!(process.start(), None);
        assert!(!process.is_running_from(Found::Started(start), || true));
        drop(stop);
        thread.join().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn the_start_is_read_past_a_name_holding_spaces_and_parentheses() {
        let fields: String = (4..=21).map(|n| format!(" {n}")).collect();
        let stat = format!("42 (a) b (c) S{fields} 9876 23 24\n");
        assert_eq!(process_from_stat(&stat), Found::Started(9876));
        for state in ["R", "S", "D", "T", "t", "P", "I"] {
            assert_eq!(
                process_from_stat(&stat.replace(") S ", &format!(") {state} "))),
                Found::Started(9876)
            );
        }
        for state in ["Z", "X"] {
            let exited_stat = stat.replace(") S ", &format!(") {state} "));
            let found = process_from_stat(&exited_stat);
            assert_eq!(found, Found::Started(9876));
            // A dead leader can still own live workers with this identity.
            assert!(ProcessIdentity::started_at(42, 9876).is_running_from(found, || true));
            assert!(
                !ProcessIdentity::started_at(42, 9877)
                    .is_running_from(process_from_stat(&exited_stat), || true)
            );
            assert_eq!(
                process_from_stat(&exited_stat.replace(" 20 ", " 1 ")),
                Found::Gone
            );
        }
        assert_eq!(
            process_from_stat(&stat.replace(") S ", ") invalid-state ")),
            Found::Unknown
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unreadable_or_malformed_proc_metadata_does_not_establish_exit() {
        let fields: String = (4..=21).map(|n| format!(" {n}")).collect();
        let stat = format!("42 (compile) S{fields} 9876 23\n");
        assert_eq!(
            found_in_proc(42, |file| Ok(if file == "status" {
                "Name:\tcompile\nTgid:\t42\n".to_owned()
            } else {
                stat.clone()
            })),
            Found::Started(9876)
        );
        for broken in ["", "Name:\tcompile\n", "Tgid:\tnot-a-number\n"] {
            assert_eq!(
                found_in_proc(42, |file| {
                    assert_eq!(file, "status", "invalid status must not read stat");
                    Ok(broken.to_owned())
                }),
                Found::Unknown
            );
        }
        assert_eq!(
            found_in_proc(42, |file| {
                assert_eq!(file, "status", "a thread must not read stat");
                Ok("Tgid:\t41\n".to_owned())
            }),
            Found::Gone
        );
        for broken in [
            "",
            "42 (compile)",
            "42 (compile) S",
            "42 (compile) S 4",
            "42 compile S 4",
        ] {
            assert_eq!(process_from_stat(broken), Found::Unknown, "{broken}");
        }
        assert_eq!(
            process_from_stat(&stat.replace("9876", "invalid-start")),
            Found::Unknown
        );
        for stage in ["status", "stat"] {
            for code in [libc::ENOENT, libc::ESRCH, libc::EACCES, libc::EIO] {
                let actual = found_in_proc(42, |file| {
                    if file == stage {
                        Err(std::io::Error::from_raw_os_error(code))
                    } else {
                        assert_eq!(file, "status");
                        Ok("Tgid:\t42\n".to_owned())
                    }
                });
                assert_eq!(actual, Found::Unknown, "{stage}, errno {code}");
            }
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_non_utf8_process_name_does_not_hide_the_numeric_identity() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("status"),
            b"Name:\tcompile\xff\nTgid:\t42\n",
        )
        .unwrap();
        let fields: String = (4..=21).map(|n| format!(" {n}")).collect();
        let mut stat = b"42 (compile\xff) S".to_vec();
        stat.extend_from_slice(format!("{fields} 9876 23\n").as_bytes());
        std::fs::write(dir.path().join("stat"), stat).unwrap();
        assert_eq!(
            found_in_proc(42, |file| read_proc_file(dir.path().join(file))),
            Found::Started(9876)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_exited_leader_keeps_live_workers_and_a_lone_zombie_is_gone() {
        let pid = std::process::id();
        let process = ProcessIdentity::current();
        let (ready, started) = std::sync::mpsc::channel();
        let (stop, stopped) = std::sync::mpsc::channel::<()>();
        let worker = std::thread::spawn(move || {
            ready.send(()).unwrap();
            let _ = stopped.recv();
        });
        started.recv().unwrap();
        let start = process.start().unwrap();
        let stat = |threads: &str| {
            format!(
                "{pid} (compile) Z 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 {threads} 21 {start} 23"
            )
        };
        for threads in ["2", "missing", "invalid", "0"] {
            let leader = found_in_proc(pid, |file| {
                Ok(if file == "status" {
                    // Deliberately stale: stat must supply the thread count.
                    format!("Tgid:\t{pid}\nThreads:\t1\n")
                } else {
                    stat(threads)
                })
            });
            assert_eq!(leader, Found::Started(start), "threads: {threads}");
            assert!(process.is_running_from(leader, || panic!("the birth is known")));
        }
        let unknown = process_from_stat(&format!("{pid} (compile) Z"));
        assert_eq!(unknown, Found::Unknown);
        assert!(process.is_running_from(unknown, || process_may_be_running(pid)));
        let lone_zombie = found_in_proc(pid, |file| {
            Ok(if file == "status" {
                format!("Tgid:\t{pid}\nThreads:\t2\n")
            } else {
                stat("1")
            })
        });
        assert_eq!(lone_zombie, Found::Gone);
        assert!(!process.is_running_from(lone_zombie, || panic!("confirmed lone zombie")));
        drop(stop);
        worker.join().unwrap();
    }
}
