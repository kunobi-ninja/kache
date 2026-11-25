//! How many cores a running test binary uses, so its lease can give back
//! the slots it does not.
//!
//! A test lease is sized when the binary starts: an elastic want takes every
//! free test slot. A binary whose last tests run serially, or one that hangs,
//! would keep all of them until it exits. The runner reads the CPU time of
//! the binary and its descendants every [`WINDOW`], and once [`WINDOWS`]
//! readings are in, shrinks the lease to the most cores any of them used,
//! never below the slots the binary's memory takes. A lease only shrinks:
//! the binary keeps the thread count it was started with.

use std::process::{Child, ExitStatus};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use crate::scheduler::{TestLease, TestWant};

/// How often the runner reads the binary's CPU time.
pub(crate) const WINDOW: Duration = Duration::from_secs(1);
/// How many readings the lease follows: the busiest of them sets its size,
/// so one quiet second (a child that exited, a test waiting on I/O) does not
/// shrink it.
pub(crate) const WINDOWS: usize = 5;
/// The most processes one reading visits.
const TREE_LIMIT: usize = 512;

/// The slots a lease keeps: the cores the busiest reading used, rounded up,
/// and never fewer than `floor` or one.
pub(crate) fn slots_in_use(readings: &[f64], floor: u32) -> u32 {
    let busiest = readings.iter().copied().fold(0.0_f64, f64::max);
    let cores = if busiest.is_finite() {
        busiest.ceil().min(f64::from(u32::MAX)) as u32
    } else {
        u32::MAX
    };
    cores.max(floor).max(1)
}

/// Cores used between two readings of CPU time `wall` apart. CPU time can go
/// down when a descendant exits before its parent reaps it; that reads as
/// none.
fn cores(before: Duration, after: Duration, wall: Duration) -> Option<f64> {
    if wall.is_zero() {
        return None;
    }
    Some(after.checked_sub(before)?.as_secs_f64() / wall.as_secs_f64())
}

/// Only spare slots from an elastic grant can be returned. Fixed grants
/// include explicit libtest thread counts and nextest's per-test reservations.
fn shrink_floor(want: TestWant, held: u32) -> Option<u32> {
    match want {
        TestWant::Elastic(floor) if floor < held => Some(floor),
        _ => None,
    }
}

/// Keep the grant alive until the child exits, even if a sampler cannot start.
pub(super) fn wait(
    child: &mut Child,
    mut lease: TestLease,
    want: TestWant,
) -> std::io::Result<ExitStatus> {
    let Some(floor) = shrink_floor(want, lease.held()) else {
        return child.wait();
    };
    if !cfg!(any(target_os = "macos", target_os = "linux")) {
        return child.wait();
    }
    let pid = child.id();
    std::thread::scope(|scope| {
        let (stop, stopped) = mpsc::channel();
        let watcher = std::thread::Builder::new()
            .spawn_scoped(scope, || sample(pid, floor, &mut lease, stopped));
        let result = child.wait();
        drop(stop);
        if let Ok(watcher) = watcher {
            let _ = watcher.join();
        }
        result
    })
}

fn sample(pid: u32, floor: u32, lease: &mut TestLease, stopped: mpsc::Receiver<()>) {
    let Some(mut last) = tree_cpu_time(pid) else {
        return;
    };
    let mut at = Instant::now();
    let mut readings = std::collections::VecDeque::with_capacity(WINDOWS);
    while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(WINDOW) {
        let Some(now) = tree_cpu_time(pid) else {
            return;
        };
        let instant = Instant::now();
        let reading = cores(last, now, instant.duration_since(at));
        at = instant;
        last = now;
        let Some(reading) = reading else {
            // An incomplete interval is no evidence that the binary was idle.
            readings.clear();
            continue;
        };
        if readings.len() == WINDOWS {
            readings.pop_front();
        }
        readings.push_back(reading);
        if readings.len() == WINDOWS {
            let keep = slots_in_use(readings.make_contiguous(), floor);
            lease.shrink_to(keep);
            if lease.held() == floor {
                break;
            }
        }
    }
}

/// CPU time of `pid` and its live descendants, each with the time of the
/// children it has reaped. `None` when `pid` cannot be read.
pub(crate) fn tree_cpu_time(pid: u32) -> Option<Duration> {
    let mut total = process_cpu_time(pid)?;
    let mut pending = children(pid);
    let mut visited = 1;
    while let Some(child) = pending.pop() {
        if visited >= TREE_LIMIT {
            return None;
        }
        visited += 1;
        if let Some(time) = process_cpu_time(child) {
            total += time;
        }
        pending.extend(children(child));
    }
    Some(total)
}

#[cfg(target_os = "macos")]
// libc deprecates its mach_* bindings in favor of the `mach2` crate.
#[allow(deprecated)]
#[mutants::skip] // macOS syscall adapter; Linux mutation shards cannot run it.
fn process_cpu_time(pid: u32) -> Option<Duration> {
    let pid = libc::c_int::try_from(pid).ok()?;
    // SAFETY: `rusage_info_v2` is a C struct of integers; zero is valid, and
    // the kernel writes one of that flavor into it.
    let mut info = unsafe { std::mem::zeroed::<libc::rusage_info_v2>() };
    let read = unsafe {
        libc::proc_pid_rusage(
            pid,
            libc::RUSAGE_INFO_V2,
            (&raw mut info).cast::<libc::rusage_info_t>(),
        )
    };
    if read != 0 {
        return None;
    }
    // Mach absolute time units, which are not nanoseconds on Apple Silicon.
    let mut timebase = libc::mach_timebase_info { numer: 0, denom: 0 };
    if unsafe { libc::mach_timebase_info(&mut timebase) } != libc::KERN_SUCCESS
        || timebase.denom == 0
    {
        return None;
    }
    let units = u128::from(info.ri_user_time)
        + u128::from(info.ri_system_time)
        + u128::from(info.ri_child_user_time)
        + u128::from(info.ri_child_system_time);
    let nanos = units * u128::from(timebase.numer) / u128::from(timebase.denom);
    Some(Duration::from_nanos(
        u64::try_from(nanos).unwrap_or(u64::MAX),
    ))
}

#[cfg(target_os = "macos")]
#[mutants::skip] // macOS syscall adapter; Linux mutation shards cannot run it.
fn children(pid: u32) -> Vec<u32> {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return Vec::new();
    };
    let mut buffer = vec![0 as libc::pid_t; 256];
    loop {
        let size = libc::c_int::try_from(buffer.len() * std::mem::size_of::<libc::pid_t>())
            .unwrap_or(libc::c_int::MAX);
        // SAFETY: the kernel writes at most `size` bytes of pids.
        let written = unsafe { libc::proc_listchildpids(pid, buffer.as_mut_ptr().cast(), size) };
        // A count of pids, or -1.
        let Ok(count) = usize::try_from(written) else {
            return Vec::new();
        };
        if count < buffer.len() || buffer.len() >= TREE_LIMIT {
            return buffer[..count.min(buffer.len())]
                .iter()
                .filter_map(|&child| u32::try_from(child).ok().filter(|&child| child > 0))
                .collect();
        }
        buffer.resize(buffer.len() * 2, 0);
    }
}

#[cfg(target_os = "linux")]
fn process_cpu_time(pid: u32) -> Option<Duration> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let ticks = linux_stat_ticks(&stat)?;
    // SAFETY: sysconf only reads a constant of the system.
    let per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let per_second = u64::try_from(per_second).ok().filter(|&tick| tick > 0)?;
    Some(Duration::from_secs_f64(ticks as f64 / per_second as f64))
}

/// Clock ticks of a `/proc/<pid>/stat` line: its own user and system time
/// and those of the children it reaped (fields 14 to 17). The command name
/// can hold spaces and parentheses, so the fields follow the last `)`.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn linux_stat_ticks(stat: &str) -> Option<u64> {
    let mut fields = stat.rsplit_once(')')?.1.split_whitespace().skip(11);
    let mut ticks = 0_u64;
    for _ in 0..4 {
        ticks = ticks.checked_add(fields.next()?.parse().ok()?)?;
    }
    Some(ticks)
}

#[cfg(target_os = "linux")]
fn children(pid: u32) -> Vec<u32> {
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return Vec::new();
    };
    tasks
        .flatten()
        .filter_map(|task| std::fs::read_to_string(task.path().join("children")).ok())
        .flat_map(|list| {
            list.split_whitespace()
                .filter_map(|child| child.parse().ok())
                .collect::<Vec<u32>>()
        })
        .collect()
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_cpu_time(_pid: u32) -> Option<Duration> {
    // No reading: the lease keeps the size it started with.
    None
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn children(_pid: u32) -> Vec<u32> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_elastic_grants_above_their_floor_can_shrink() {
        assert_eq!(shrink_floor(TestWant::Fixed(8), 8), None);
        assert_eq!(shrink_floor(TestWant::Fixed(8), 6), None);
        assert_eq!(shrink_floor(TestWant::Elastic(3), 8), Some(3));
        assert_eq!(shrink_floor(TestWant::Elastic(3), 3), None);
        assert_eq!(shrink_floor(TestWant::Elastic(8), 3), None);
    }

    #[test]
    fn a_lease_keeps_the_busiest_readings_cores() {
        assert_eq!(slots_in_use(&[0.0, 0.0, 0.0], 1), 1, "idle keeps the floor");
        assert_eq!(slots_in_use(&[0.0, 0.0], 0), 1, "never none");
        assert_eq!(slots_in_use(&[0.0, 0.2, 0.0], 3), 3, "memory floor");
        assert_eq!(slots_in_use(&[0.5, 2.0, 1.2], 1), 2);
        assert_eq!(slots_in_use(&[0.5, 2.01, 1.2], 1), 3, "rounds up");
        assert_eq!(slots_in_use(&[7.9, 0.1], 2), 8);
        assert_eq!(slots_in_use(&[f64::INFINITY], 1), u32::MAX);
        assert_eq!(slots_in_use(&[], 4), 4);
    }

    #[test]
    fn cores_are_cpu_time_over_wall_time() {
        let s = Duration::from_secs;
        assert_eq!(cores(s(10), s(14), s(2)), Some(2.0));
        assert_eq!(cores(s(10), s(10), s(2)), Some(0.0));
        assert_eq!(cores(s(10), s(4), s(2)), None, "an exited child");
        assert_eq!(cores(s(0), s(5), Duration::ZERO), None);
        assert_eq!(cores(s(1), s(2), s(4)), Some(0.25));
    }

    #[test]
    fn a_linux_stat_line_sums_own_and_reaped_ticks() {
        let line = "42 (my (test)) S 1 42 42 0 -1 4194304 100 0 0 0 7 3 20 10 20 0 1 0";
        assert_eq!(linux_stat_ticks(line), Some(40));
        assert_eq!(
            linux_stat_ticks("42 (t) S 1 2 3 4 5 6 7 8 9 10 11 12 13"),
            None
        );
        assert_eq!(
            linux_stat_ticks("42 (t) S 1 2 3 4 5 6 7 8 9 10 x 1 1 1"),
            None
        );
        assert_eq!(linux_stat_ticks("no parens"), None);
    }

    #[cfg(any(target_os = "macos", target_os = "linux"))]
    #[test]
    fn a_busy_child_counts_toward_its_parents_tree() {
        // A shell whose child spins: the shell itself does nothing.
        let mut shell = std::process::Command::new("/bin/sh")
            .args(["-c", "ulimit -t 12 || exit 1; while :; do :; done & wait"])
            .spawn()
            .unwrap();
        let pid = shell.id();
        let deadline = Instant::now() + Duration::from_secs(10);
        while children(pid).is_empty() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let spinner = children(pid);
        let before = tree_cpu_time(pid).unwrap();
        let started = Instant::now();
        std::thread::sleep(Duration::from_millis(500));
        let after = tree_cpu_time(pid).unwrap();
        let used = cores(before, after, started.elapsed()).unwrap();
        for child in &spinner {
            unsafe { libc::kill(*child as libc::pid_t, libc::SIGKILL) };
        }
        let _ = shell.kill();
        let _ = shell.wait();
        assert_eq!(spinner.len(), 1, "one spinning child");
        assert!(used > 0.5, "the spinner's core counts: {used}");
        assert!(used < 1.5, "one core, not more: {used}");
        assert!(tree_cpu_time(u32::MAX).is_none());
    }
}
