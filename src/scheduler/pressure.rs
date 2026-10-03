//! Whether the machine, or this process's cgroup, is short of memory now.
//!
//! The scheduler weighs a compile by the memory it used before, but that
//! cannot see what else is running: several agents' builds, an IDE, a
//! browser. When memory runs short, a compile it admits anyway can push the
//! machine into swap or the OOM killer. Under pressure, [`super::Scheduler`]
//! therefore asks for the whole pool for a compile: it waits for the other
//! compiles to finish, so they run one after another until pressure eases.
//! Slots running tests hold still count against that need as before, so a
//! test waiting on a compile cannot stall it.
//!
//! - macOS: the kernel's own verdict, `kern.memorystatus_vm_pressure_level`,
//!   at warning or critical.
//! - Linux: memory PSI, some task stalled on memory for at least
//!   [`PSI_SOME_AVG10`] percent of the last ten seconds. The cgroup's own
//!   `memory.pressure` is read when there is one, so a container sees its
//!   limit rather than the host's.
//! - Elsewhere, and when nothing can be read: no pressure.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// How long one reading of the pressure is reused. Admission polls every few
/// milliseconds; memory pressure changes over seconds.
const SAMPLE_FOR: Duration = Duration::from_millis(250);

/// Share of the last ten seconds some task spent stalled on memory, in
/// percent, from which Linux counts as under pressure.
#[cfg(any(test, target_os = "linux"))]
pub(crate) const PSI_SOME_AVG10: f64 = 10.0;

/// `[cache] scheduler_memory_pressure`, set once at wrapper startup.
static ENABLED: AtomicBool = AtomicBool::new(true);

pub(crate) fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
}

// A pressure answer a test sets for its own thread, so other tests running
// at the same time are not affected.
#[cfg(test)]
thread_local! {
    static FORCED: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn force(pressured: Option<bool>) {
    FORCED.with(|forced| forced.set(pressured));
}

/// Whether admission should hold back now.
pub(crate) fn now() -> bool {
    if !ENABLED.load(Ordering::Relaxed) {
        return false;
    }
    #[cfg(test)]
    if let Some(pressured) = FORCED.with(std::cell::Cell::get) {
        return pressured;
    }
    sampled(Instant::now(), under_pressure)
}

thread_local! {
    static LAST: std::cell::Cell<Option<(Instant, bool)>> = const { std::cell::Cell::new(None) };
}

/// The reading taken within [`SAMPLE_FOR`] of `now`, else a new one from
/// `read`.
pub(crate) fn sampled(now: Instant, read: impl FnOnce() -> bool) -> bool {
    LAST.with(|last| match last.get() {
        Some((at, pressured)) if now.saturating_duration_since(at) < SAMPLE_FOR => pressured,
        _ => {
            let pressured = read();
            last.set(Some((now, pressured)));
            pressured
        }
    })
}

/// The weight to admit a compile with: under pressure, the whole pool, so it
/// waits for every other compile to finish.
pub(crate) fn weight(weight: u32, pool: u32, pressured: bool) -> u32 {
    if pressured { pool } else { weight }
}

/// Whether macOS's pressure level (1 normal, 2 warning, 4 critical) is
/// pressure.
#[cfg(any(test, target_os = "macos"))]
pub(crate) fn macos_pressured(level: i32) -> bool {
    level >= 2
}

/// Whether a PSI file's `some avg10` is at or over [`PSI_SOME_AVG10`].
/// `None` when the text has no such line.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn psi_pressured(text: &str) -> Option<bool> {
    let some = text.lines().find(|line| line.starts_with("some "))?;
    let avg10 = some
        .split_whitespace()
        .find_map(|field| field.strip_prefix("avg10="))?
        .parse::<f64>()
        .ok()?;
    Some(avg10 >= PSI_SOME_AVG10)
}

#[cfg(target_os = "macos")]
fn under_pressure() -> bool {
    let mut level: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated, and `level`/`size` describe a
    // buffer of exactly the size the kernel writes for this integer.
    let rc = unsafe {
        libc::sysctlbyname(
            c"kern.memorystatus_vm_pressure_level".as_ptr(),
            (&raw mut level).cast(),
            &raw mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    rc == 0 && macos_pressured(level)
}

#[cfg(target_os = "linux")]
fn under_pressure() -> bool {
    use std::path::{Path, PathBuf};
    use std::sync::OnceLock;
    static CGROUP: OnceLock<Option<PathBuf>> = OnceLock::new();
    let cgroup = CGROUP.get_or_init(|| {
        super::ResourceDomain::from_files(
            Path::new("/proc/self/cgroup"),
            Path::new("/proc/self/mountinfo"),
        )
        .map(|domain| domain.current.join("memory.pressure"))
    });
    pressured_from(cgroup.as_deref(), Path::new("/proc/pressure/memory"))
}

/// The cgroup's own PSI reading when `cgroup` names a readable one, else the
/// host's; no pressure when neither can be read.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn pressured_from(cgroup: Option<&std::path::Path>, host: &std::path::Path) -> bool {
    let read = |path: &std::path::Path| {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|text| psi_pressured(&text))
    };
    cgroup
        .and_then(read)
        .or_else(|| read(host))
        .unwrap_or(false)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn under_pressure() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_takes_the_whole_pool() {
        assert_eq!(weight(2, 8, false), 2);
        assert_eq!(weight(2, 8, true), 8);
        // An ask that is already the whole pool stays there either way.
        assert_eq!(weight(8, 8, false), 8);
        assert_eq!(weight(8, 8, true), 8);
        assert_eq!(weight(1, 1, false), 1);
        assert_eq!(weight(1, 1, true), 1);
    }

    #[test]
    fn a_forced_answer_replaces_the_machine_until_cleared() {
        let _lock = crate::test_support::process_state_test_lock();
        set_enabled(true);
        let live = under_pressure();
        force(Some(!live));
        assert_eq!(now(), !live, "the pin replaces whatever the machine says");
        force(Some(live));
        assert_eq!(now(), live);
        force(None);
        assert_eq!(now(), live, "clearing the pin reads the machine again");
    }

    #[test]
    fn reads_the_macos_level() {
        assert!(!macos_pressured(1));
        assert!(macos_pressured(2));
        assert!(macos_pressured(4));
        assert!(!macos_pressured(0));
    }

    #[test]
    fn reads_psi() {
        let calm = "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";
        assert_eq!(psi_pressured(calm), Some(false));
        let under = "some avg10=10.00 avg60=3.10 avg300=1.00 total=123\nfull avg10=4.00 avg60=0 avg300=0 total=9\n";
        assert_eq!(psi_pressured(under), Some(true));
        let just_below = "some avg10=9.99 avg60=50.00 avg300=50.00 total=1\n";
        assert_eq!(psi_pressured(just_below), Some(false));
        assert_eq!(psi_pressured("full avg10=90.00\n"), None);
        assert_eq!(psi_pressured("some avg60=90.00\n"), None);
        assert_eq!(psi_pressured("some avg10=lots\n"), None);
        assert_eq!(psi_pressured(""), None);
        assert_eq!(PSI_SOME_AVG10, 10.0);
    }

    #[test]
    fn prefers_the_cgroup_reading_then_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let calm = dir.path().join("calm");
        let busy = dir.path().join("busy");
        let bad = dir.path().join("bad");
        let missing = dir.path().join("missing");
        std::fs::write(&calm, "some avg10=0.00 avg60=0 avg300=0 total=0\n").unwrap();
        std::fs::write(&busy, "some avg10=50.00 avg60=0 avg300=0 total=0\n").unwrap();
        std::fs::write(&bad, "nothing useful\n").unwrap();
        assert!(
            pressured_from(Some(&busy), &calm),
            "the cgroup's own reading wins"
        );
        assert!(!pressured_from(Some(&calm), &busy), "even when calm");
        assert!(
            pressured_from(Some(&missing), &busy),
            "unreadable cgroup: host"
        );
        assert!(pressured_from(Some(&bad), &busy), "unusable cgroup: host");
        assert!(pressured_from(None, &busy));
        assert!(!pressured_from(None, &calm));
        assert!(
            !pressured_from(Some(&missing), &missing),
            "nothing readable: none"
        );
    }

    #[test]
    fn the_switch_turns_pressure_off() {
        let _guard = crate::test_support::process_state_test_lock();
        force(Some(true));
        set_enabled(false);
        assert!(!now());
        set_enabled(true);
        assert!(now());
        force(None);
    }

    #[test]
    fn a_reading_is_reused_for_a_while() {
        // Far past any reading an earlier test left on this thread.
        let start = Instant::now() + Duration::from_secs(3600);
        assert!(sampled(start, || true));
        assert!(sampled(start + Duration::from_millis(249), || false));
        assert!(!sampled(start + SAMPLE_FOR, || false));
        assert!(!sampled(start + SAMPLE_FOR, || panic!("reused")));
        assert_eq!(SAMPLE_FOR, Duration::from_millis(250));
    }
}
