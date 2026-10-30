//! Whether the machine, or this process's cgroup, is short of memory now,
//! and how many slots compiles may fill while it is.
//!
//! The scheduler weighs a compile by the memory it used before, but that
//! cannot see what else is running: several agents' builds, an IDE, a
//! browser. When memory runs short, a compile it admits anyway can push the
//! machine into swap or the OOM killer. Every reading therefore moves a
//! [`Ceiling`] on the slots compiles hold, shared by the wrappers of one
//! cache in `scheduler/pressure`. Ordinary admission takes a compile only
//! while the slots held, its own included, stay within the ceiling (see
//! [`ask`]); compiles already running keep their slots when it falls below
//! what they hold. Bounded recovery admission uses the compile's own weight
//! instead. Slots running tests hold still count against that need as
//! before, so a test waiting on a compile cannot stall it.
//!
//! - macOS: the kernel's own verdict, `kern.memorystatus_vm_pressure_level`,
//!   at warning or critical.
//! - Linux: memory PSI. Some task stalled on memory for at least
//!   [`PSI_AVG10`] percent of the last ten seconds is a warning; every task
//!   stalled that long is critical. The cgroup's own `memory.pressure` is
//!   read when there is one, so a container sees its limit rather than the
//!   host's.
//! - Elsewhere, and when nothing can be read: unknown, which leaves the
//!   ceiling where it is.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use crate::store::StoreLock;

/// How long one reading of the pressure is reused. Admission polls every few
/// milliseconds; memory pressure changes over seconds.
const SAMPLE_FOR: Duration = Duration::from_millis(250);

/// How long a change in admissions takes to show in a reading: Linux
/// averages its PSI over ten seconds. The ceiling halves at most once per
/// period, and grows only after a whole period of calm readings.
pub(crate) const SETTLE: Duration = Duration::from_secs(10);

/// Share of the last ten seconds tasks spent stalled on memory, in percent,
/// from which Linux counts as under pressure: some task for a warning, every
/// task for critical.
#[cfg(any(test, target_os = "linux"))]
pub(crate) const PSI_AVG10: f64 = 10.0;

/// `[cache] scheduler_memory_pressure`, set once at wrapper startup.
static ENABLED: AtomicBool = AtomicBool::new(true);

#[cfg(test)]
pub(crate) fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
}

thread_local! {
    static INVOCATION_ENABLED: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// Scheduler policy for one synchronous request. Machine pressure samples may
/// be reused; a client's opt-out must remain local to its invocation.
pub(crate) struct InvocationPressureScope {
    previous: Option<bool>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl InvocationPressureScope {
    pub(crate) fn enter(enabled: bool) -> Self {
        Self {
            previous: INVOCATION_ENABLED.with(|slot| slot.replace(Some(enabled))),
            _thread: std::marker::PhantomData,
        }
    }
}

impl Drop for InvocationPressureScope {
    fn drop(&mut self) {
        INVOCATION_ENABLED.with(|slot| slot.set(self.previous));
    }
}

/// What one reading says about memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Level {
    Calm,
    Warning,
    Critical,
    /// Nothing could be read, or this system has no reading.
    Unknown,
}

// A pressure answer a test sets for its own thread, so other tests running
// at the same time are not affected.
#[cfg(test)]
thread_local! {
    static FORCED: std::cell::Cell<Option<Level>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) fn force(level: Option<Level>) {
    FORCED.with(|forced| forced.set(level));
}

/// The pressure now, or `None` when the check is off.
pub(crate) fn now() -> Option<Level> {
    if !INVOCATION_ENABLED.with(|slot| {
        slot.get()
            .unwrap_or_else(|| ENABLED.load(Ordering::Relaxed))
    }) {
        return None;
    }
    #[cfg(test)]
    if let Some(level) = FORCED.with(std::cell::Cell::get) {
        return Some(level);
    }
    Some(sampled(Instant::now(), read_level))
}

thread_local! {
    static LAST: std::cell::Cell<Option<(Instant, Level)>> = const { std::cell::Cell::new(None) };
}

/// The reading taken within [`SAMPLE_FOR`] of `now`, else a new one from
/// `read`.
pub(crate) fn sampled(now: Instant, read: impl FnOnce() -> Level) -> Level {
    LAST.with(|last| match last.get() {
        Some((at, level)) if now.saturating_duration_since(at) < SAMPLE_FOR => level,
        _ => {
            let level = read();
            last.set(Some((now, level)));
            level
        }
    })
}

/// The slots a compile of `weight` needs free: its own, and every slot above
/// the `ceiling`. It keeps only its weight, so taking it leaves the slots
/// held within the ceiling. A compile heavier than the ceiling needs the
/// whole pool free and runs alone.
pub(crate) fn ask(weight: u32, pool: u32, ceiling: u32) -> u32 {
    weight.saturating_add(pool.saturating_sub(ceiling))
}

/// The most slots compiles may hold, as the readings so far have moved it.
///
/// - Critical: one slot, at once.
/// - Warning: half the slots, rounded up, at most once per [`SETTLE`]. A
///   warning that persists keeps halving down to the compile reserve (see
///   [`super::reserve_for`]), so compiles keep a quarter of the pool while
///   pressure stays at warning. It never raises a lower ceiling.
/// - Calm: unchanged until the calm readings have lasted a [`SETTLE`], then a
///   quarter of the pool more per [`SETTLE`]. Any other reading ends the run,
///   so readings that alternate never grow it.
/// - Unknown: unchanged, and it ends a calm run.
///
/// The slot count is not tied to one pool size: wrappers with different
/// pools share it, each clamping it to its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Ceiling {
    /// Slots when the last reading that was not calm came in.
    slots: u32,
    /// When the ceiling last fell, in milliseconds since the epoch.
    cut: u64,
    /// When the run of calm readings since then began.
    calm: Option<u64>,
}

impl Ceiling {
    /// No reading has lowered it: the whole pool, whatever its size.
    pub(crate) const OPEN: Self = Self {
        slots: u32::MAX,
        cut: 0,
        calm: None,
    };

    /// The ceiling at `now` for a pool of `pool` slots.
    pub(crate) fn slots(self, pool: u32, now: u64) -> u32 {
        let periods = self.calm.map_or(0, |since| {
            now.saturating_sub(since) / SETTLE.as_millis() as u64
        });
        let grown = u64::from(self.slots)
            .saturating_add(periods.saturating_mul(u64::from(super::reserve_for(pool))));
        grown.min(u64::from(pool)) as u32
    }

    /// The ceiling after a reading of `level` at `now`.
    pub(crate) fn observe(self, level: Level, pool: u32, now: u64) -> Self {
        let slots = self.slots(pool, now);
        let held = Self {
            slots,
            calm: None,
            ..self
        };
        match level {
            Level::Calm if self.calm.is_some() || slots == pool => self,
            Level::Calm => Self {
                calm: Some(now),
                ..held
            },
            Level::Unknown if self.calm.is_none() => self,
            Level::Unknown => held,
            Level::Warning if now.saturating_sub(self.cut) < SETTLE.as_millis() as u64 => held,
            Level::Warning => Self {
                slots: slots.div_ceil(2).max(super::reserve_for(pool)).min(slots),
                cut: now,
                calm: None,
            },
            Level::Critical => Self {
                slots: 1,
                cut: now,
                calm: None,
            },
        }
    }

    fn parse(text: &str) -> Option<Self> {
        let mut fields = text.split_whitespace();
        let slots = fields.next()?.parse().ok().filter(|slots| *slots > 0)?;
        let cut = fields.next()?.parse().ok()?;
        let calm = match fields.next()? {
            "-" => None,
            since => Some(since.parse().ok()?),
        };
        Some(Self { slots, cut, calm })
    }

    fn render(self) -> String {
        let calm = self
            .calm
            .map_or_else(|| "-".to_string(), |since| since.to_string());
        format!("{} {} {calm}", self.slots, self.cut)
    }
}

/// The ceiling for this wrapper's pool of `pool` slots under the cache's
/// scheduler `root`, after taking a reading. The pool itself when the check
/// is off.
pub(crate) fn ceiling(root: &Path, pool: u32) -> u32 {
    match now() {
        Some(level) => shared(root, pool, level, super::queue::millis(SystemTime::now())),
        None => pool,
    }
}

/// Apply a reading of `level` at `now` to the ceiling stored under `root`,
/// and return it. A missing or unreadable record is [`Ceiling::OPEN`]. A
/// record that changes is rewritten under `pressure.lock`, from a read taken
/// under it, so a fall another wrapper stored is not lost. When that lock is
/// busy, another wrapper is storing a reading of its own, and this one is
/// used without being stored.
pub(crate) fn shared(root: &Path, pool: u32, level: Level, now: u64) -> u32 {
    let path = root.join("pressure");
    let before = stored(&path);
    let after = before.observe(level, pool, now);
    if after == before {
        return after.slots(pool, now);
    }
    match StoreLock::try_acquire(&root.join("pressure.lock")) {
        Ok(Some(_lock)) => {
            let after = stored(&path).observe(level, pool, now);
            if let Err(error) =
                crate::atomic::atomic_replace_deferrable(&path, after.render().as_bytes(), false)
            {
                tracing::debug!("memory pressure ceiling not stored: {error:#}");
            }
            after.slots(pool, now)
        }
        Ok(None) | Err(_) => after.slots(pool, now),
    }
}

fn stored(path: &Path) -> Ceiling {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| Ceiling::parse(&text))
        .unwrap_or(Ceiling::OPEN)
}

/// macOS's pressure level: 1 normal, 2 warning, 4 critical.
#[cfg(any(test, target_os = "macos"))]
pub(crate) fn macos_level(level: i32) -> Level {
    match level {
        1 => Level::Calm,
        2 | 3 => Level::Warning,
        4.. => Level::Critical,
        _ => Level::Unknown,
    }
}

/// The level a PSI file's `avg10` values give. `None` when the text has no
/// `some` line with one.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn psi_level(text: &str) -> Option<Level> {
    let avg10 = |kind: &str| {
        let line = text.lines().find(|line| line.starts_with(kind))?;
        line.split_whitespace()
            .find_map(|field| field.strip_prefix("avg10="))?
            .parse::<f64>()
            .ok()
    };
    let some = avg10("some ")?;
    Some(if avg10("full ").is_some_and(|full| full >= PSI_AVG10) {
        Level::Critical
    } else if some >= PSI_AVG10 {
        Level::Warning
    } else {
        Level::Calm
    })
}

#[cfg(target_os = "macos")]
fn read_level() -> Level {
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
    if rc == 0 {
        macos_level(level)
    } else {
        Level::Unknown
    }
}

#[cfg(target_os = "linux")]
fn read_level() -> Level {
    use std::path::PathBuf;
    use std::sync::OnceLock;
    static CGROUP: OnceLock<Option<PathBuf>> = OnceLock::new();
    let cgroup = CGROUP.get_or_init(|| {
        super::ResourceDomain::from_files(
            Path::new("/proc/self/cgroup"),
            Path::new("/proc/self/mountinfo"),
        )
        .map(|domain| domain.current.join("memory.pressure"))
    });
    level_from(cgroup.as_deref(), Path::new("/proc/pressure/memory"))
}

/// The cgroup's own PSI reading when `cgroup` names a readable one, else the
/// host's; unknown when neither can be read.
#[cfg(any(test, target_os = "linux"))]
pub(crate) fn level_from(cgroup: Option<&Path>, host: &Path) -> Level {
    let read = |path: &Path| {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|text| psi_level(&text))
    };
    cgroup
        .and_then(read)
        .or_else(|| read(host))
        .unwrap_or(Level::Unknown)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn read_level() -> Level {
    Level::Unknown
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_clients_keep_their_pressure_policy_on_reused_workers() {
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            for enabled in [false, true] {
                let barrier = &barrier;
                threads.spawn(move || {
                    force(Some(Level::Warning));
                    let mut observations = Vec::new();
                    for _ in 0..2 {
                        let _scope = InvocationPressureScope::enter(enabled);
                        barrier.wait();
                        let before = now().is_some();
                        let nested = std::panic::catch_unwind(|| {
                            let _child = InvocationPressureScope::enter(!enabled);
                            std::panic::panic_any(now().is_some());
                        })
                        .err()
                        .and_then(|payload| payload.downcast::<bool>().ok())
                        .map(|value| *value);
                        let after = now().is_some();
                        barrier.wait();
                        observations.push((before, nested, after));
                    }
                    for (before, nested, after) in observations {
                        assert_eq!(before, enabled);
                        assert_eq!(nested, Some(!enabled));
                        assert_eq!(after, enabled);
                    }
                    assert!(INVOCATION_ENABLED.with(|slot| slot.get().is_none()));
                    force(None);
                });
            }
        });
    }

    #[test]
    fn a_compile_needs_the_slots_above_the_ceiling_free() {
        assert_eq!(ask(2, 8, 8), 2, "an open ceiling asks for the weight");
        assert_eq!(ask(2, 8, 5), 5);
        assert_eq!(ask(1, 8, 1), 8, "a ceiling of one runs compiles alone");
        assert_eq!(ask(3, 8, 2), 9, "heavier than the ceiling: past the pool");
        assert_eq!(ask(1, 1, 1), 1);
        assert_eq!(
            ask(1, 4, 9),
            1,
            "a ceiling above this pool asks for nothing more"
        );
        assert_eq!(ask(u32::MAX, 4, 1), u32::MAX);
    }

    const SETTLE_MS: u64 = 10_000;

    fn ceiling(slots: u32, cut: u64, calm: Option<u64>) -> Ceiling {
        Ceiling { slots, cut, calm }
    }

    #[test]
    fn the_settling_period_is_ten_seconds() {
        assert_eq!(SETTLE, Duration::from_secs(10));
    }

    #[test]
    fn an_open_ceiling_is_the_whole_pool() {
        for pool in [1, 4, 18] {
            assert_eq!(Ceiling::OPEN.slots(pool, 0), pool);
            assert_eq!(Ceiling::OPEN.slots(pool, u64::MAX), pool);
            for level in [Level::Calm, Level::Unknown] {
                assert_eq!(Ceiling::OPEN.observe(level, pool, 50_000), Ceiling::OPEN);
            }
        }
    }

    #[test]
    fn a_warning_halves_the_ceiling_once_per_settling_period() {
        let start = 1_000_000;
        let once = Ceiling::OPEN.observe(Level::Warning, 18, start);
        assert_eq!(once, ceiling(9, start, None));
        let soon = once.observe(Level::Warning, 18, start + SETTLE_MS - 1);
        assert_eq!(
            soon, once,
            "a second warning inside the period changes nothing"
        );
        let twice = soon.observe(Level::Warning, 18, start + SETTLE_MS);
        assert_eq!(twice, ceiling(5, start + SETTLE_MS, None), "9 halves to 5");
        let thrice = twice.observe(Level::Warning, 18, start + 2 * SETTLE_MS);
        assert_eq!(
            thrice,
            ceiling(4, start + 2 * SETTLE_MS, None),
            "5 halves to 3, but the reserve of a pool of 18 is 4"
        );
        let sustained = thrice.observe(Level::Warning, 18, start + 9 * SETTLE_MS);
        assert_eq!(sustained.slots(18, 0), 4, "never below the reserve");
        for (pool, floor) in [(1, 1), (2, 1), (4, 1), (8, 2), (16, 4)] {
            let mut warned = Ceiling::OPEN;
            for step in 0..6 {
                warned = warned.observe(Level::Warning, pool, start + step * SETTLE_MS);
            }
            assert_eq!(warned.slots(pool, start), floor, "pool {pool}");
        }
    }

    #[test]
    fn critical_drops_to_one_slot_at_once() {
        let start = 1_000_000;
        let warned = Ceiling::OPEN.observe(Level::Warning, 16, start);
        assert_eq!(
            warned.observe(Level::Critical, 16, start + 1),
            ceiling(1, start + 1, None)
        );
        let calm = ceiling(4, start, Some(start + 5));
        let critical = calm.observe(Level::Critical, 16, start + 6);
        assert_eq!(critical, ceiling(1, start + 6, None));
        let later = start + 6 + SETTLE_MS;
        assert_eq!(
            critical.observe(Level::Warning, 16, later),
            ceiling(1, later, None),
            "a warning does not raise it to the reserve"
        );
    }

    #[test]
    fn calm_grows_the_ceiling_a_quarter_pool_per_settled_period() {
        let start = 1_000_000;
        let low = ceiling(1, start, None);
        let calm = low.observe(Level::Calm, 16, start + 3);
        assert_eq!(calm, ceiling(1, start, Some(start + 3)));
        assert_eq!(
            calm.observe(Level::Calm, 16, start + 4),
            calm,
            "later calm readings keep the run's start"
        );
        assert_eq!(
            calm.slots(16, start + 3 + SETTLE_MS - 1),
            1,
            "not before a whole period"
        );
        assert_eq!(calm.slots(16, start + 3 + SETTLE_MS), 5);
        assert_eq!(calm.slots(16, start + 3 + 2 * SETTLE_MS), 9);
        assert_eq!(calm.slots(16, start + 3 + 4 * SETTLE_MS), 16);
        assert_eq!(
            calm.slots(16, start + 3 + 40 * SETTLE_MS),
            16,
            "at most the pool"
        );
        assert_eq!(calm.slots(16, u64::MAX), 16);
        assert_eq!(
            calm.slots(16, start),
            1,
            "a clock that went back grows nothing"
        );
        assert_eq!(
            ceiling(1, 0, Some(0)).slots(2, SETTLE_MS),
            2,
            "one slot per period on small pools"
        );
        assert_eq!(ceiling(2, 0, Some(0)).slots(8, SETTLE_MS), 4);
    }

    #[test]
    fn a_calm_open_ceiling_needs_no_run() {
        let full = ceiling(8, 5, None);
        assert_eq!(full.observe(Level::Calm, 8, 100_000), full);
        assert_eq!(
            full.observe(Level::Calm, 16, 100_000),
            ceiling(8, 5, Some(100_000)),
            "a larger pool still grows"
        );
    }

    #[test]
    fn alternating_readings_never_grow_the_ceiling() {
        let start = 1_000_000;
        let mut ceiling = Ceiling::OPEN.observe(Level::Warning, 16, start);
        let mut seen = Vec::new();
        for second in 1..=120 {
            let level = if second % 2 == 0 {
                Level::Warning
            } else {
                Level::Calm
            };
            ceiling = ceiling.observe(level, 16, start + second * 1_000);
            seen.push(ceiling.slots(16, start + second * 1_000));
        }
        assert!(
            seen.windows(2).all(|pair| pair[1] <= pair[0]),
            "alternation only falls: {seen:?}"
        );
        assert_eq!(seen.last(), Some(&4));
    }

    #[test]
    fn a_warning_folds_the_growth_of_a_calm_run_before_halving() {
        let start = 1_000_000;
        let calm = ceiling(2, start, Some(start));
        let later = start + 3 * SETTLE_MS;
        assert_eq!(calm.slots(16, later), 14);
        assert_eq!(
            calm.observe(Level::Warning, 16, later),
            ceiling(7, later, None)
        );
        let early = start + SETTLE_MS;
        assert_eq!(
            ceiling(2, early - 1, Some(start)).observe(Level::Warning, 16, early),
            ceiling(6, early - 1, None),
            "too soon after a fall: the grown ceiling holds"
        );
    }

    #[test]
    fn unknown_readings_hold_the_ceiling_and_end_a_calm_run() {
        let start = 1_000_000;
        let low = ceiling(3, start, None);
        assert_eq!(low.observe(Level::Unknown, 16, start + 60 * SETTLE_MS), low);
        let calm = ceiling(3, start, Some(start));
        let held = calm.observe(Level::Unknown, 16, start + SETTLE_MS);
        assert_eq!(held, ceiling(7, start, None));
        assert_eq!(held.slots(16, start + 60 * SETTLE_MS), 7);
    }

    #[test]
    fn a_ceiling_round_trips_and_rejects_garbage() {
        for value in [
            ceiling(1, 0, None),
            ceiling(7, 1_700_000_000_000, Some(1_700_000_000_123)),
            Ceiling::OPEN,
        ] {
            assert_eq!(Ceiling::parse(&value.render()), Some(value));
        }
        assert_eq!(ceiling(3, 4, Some(5)).render(), "3 4 5");
        assert_eq!(ceiling(3, 4, None).render(), "3 4 -");
        for garbage in ["", "3", "3 4", "0 4 -", "x 4 -", "3 x -", "3 4 x", "-1 4 -"] {
            assert_eq!(Ceiling::parse(garbage), None, "{garbage:?}");
        }
    }

    /// A time well after the epoch, as a wrapper's clock reads.
    const T: u64 = 1_700_000_000_000;

    #[test]
    fn wrappers_share_the_ceiling_and_store_only_changes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let record = root.join("pressure");
        let read = || std::fs::read_to_string(&record).unwrap();
        assert_eq!(shared(root, 8, Level::Calm, T), 8);
        assert_eq!(shared(root, 8, Level::Unknown, T + 1), 8);
        assert!(!record.exists(), "an open ceiling stores nothing");
        assert_eq!(shared(root, 8, Level::Warning, T + 2), 4);
        assert_eq!(read(), format!("4 {} -", T + 2));
        assert_eq!(
            shared(root, 16, Level::Calm, T + 3),
            4,
            "another pool sees it"
        );
        assert_eq!(read(), format!("4 {} {}", T + 2, T + 3));
        assert_eq!(shared(root, 8, Level::Calm, T + 3 + SETTLE_MS), 6);
        assert_eq!(
            read(),
            format!("4 {} {}", T + 2, T + 3),
            "growth is computed, not stored"
        );
        std::fs::write(&record, "garbage").unwrap();
        assert_eq!(shared(root, 8, Level::Warning, T + 4), 4, "garbage is open");
        assert_eq!(read(), format!("4 {} -", T + 4));
    }

    #[test]
    fn a_busy_writer_lock_uses_the_reading_without_storing_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let lock = StoreLock::try_acquire(&root.join("pressure.lock"))
            .unwrap()
            .unwrap();
        assert_eq!(shared(root, 8, Level::Critical, T), 1);
        assert!(!root.join("pressure").exists());
        drop(lock);
        assert_eq!(shared(root, 8, Level::Critical, T), 1);
        assert_eq!(
            std::fs::read_to_string(root.join("pressure")).unwrap(),
            format!("1 {T} -")
        );
    }

    #[test]
    fn an_unwritable_record_still_lowers_this_wrappers_ceiling() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir(root.join("pressure")).unwrap();
        assert_eq!(shared(root, 8, Level::Warning, T), 4);
    }

    #[test]
    fn the_switch_turns_the_ceiling_off() {
        let dir = tempfile::tempdir().unwrap();
        force(Some(Level::Critical));
        {
            let _off = InvocationPressureScope::enter(false);
            assert_eq!(super::ceiling(dir.path(), 8), 8);
            assert!(
                !dir.path().join("pressure").exists(),
                "a switched-off wrapper stores nothing"
            );
        }
        assert_eq!(super::ceiling(dir.path(), 8), 1);
        {
            let _guard = crate::test_support::process_state_test_lock();
            set_enabled(false);
            assert_eq!(now(), None);
            set_enabled(true);
            assert_eq!(now(), Some(Level::Critical));
        }
        force(None);
    }

    #[test]
    fn a_forced_answer_replaces_the_machine_until_cleared() {
        let _lock = crate::test_support::process_state_test_lock();
        set_enabled(true);
        let live = read_level();
        let other = if live == Level::Critical {
            Level::Calm
        } else {
            Level::Critical
        };
        force(Some(other));
        assert_eq!(
            now(),
            Some(other),
            "the pin replaces whatever the machine says"
        );
        force(Some(live));
        assert_eq!(now(), Some(live));
        force(None);
        assert_eq!(
            now(),
            Some(live),
            "clearing the pin reads the machine again"
        );
    }

    #[test]
    fn reads_the_macos_level() {
        assert_eq!(macos_level(1), Level::Calm);
        assert_eq!(macos_level(2), Level::Warning);
        assert_eq!(macos_level(3), Level::Warning);
        assert_eq!(macos_level(4), Level::Critical);
        assert_eq!(macos_level(5), Level::Critical);
        assert_eq!(macos_level(0), Level::Unknown);
        assert_eq!(macos_level(-1), Level::Unknown);
    }

    #[test]
    fn reads_psi() {
        let calm = "some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";
        assert_eq!(psi_level(calm), Some(Level::Calm));
        let under = "some avg10=10.00 avg60=3.10 avg300=1.00 total=123\nfull avg10=4.00 avg60=0 avg300=0 total=9\n";
        assert_eq!(psi_level(under), Some(Level::Warning));
        let just_below = "some avg10=9.99 avg60=50.00 avg300=50.00 total=1\n";
        assert_eq!(psi_level(just_below), Some(Level::Calm));
        let stalled = "some avg10=30.00 avg60=0 avg300=0 total=1\nfull avg10=10.00 avg60=0 avg300=0 total=1\n";
        assert_eq!(psi_level(stalled), Some(Level::Critical));
        let nearly =
            "some avg10=30.00 avg60=0 avg300=0 total=1\nfull avg10=9.99 avg60=0 avg300=0 total=1\n";
        assert_eq!(psi_level(nearly), Some(Level::Warning));
        let bad_full = "some avg10=30.00 avg60=0 avg300=0 total=1\nfull avg10=lots\n";
        assert_eq!(
            psi_level(bad_full),
            Some(Level::Warning),
            "some still counts"
        );
        assert_eq!(psi_level("full avg10=90.00\n"), None);
        assert_eq!(psi_level("some avg60=90.00\n"), None);
        assert_eq!(psi_level("some avg10=lots\n"), None);
        assert_eq!(psi_level(""), None);
        assert_eq!(PSI_AVG10, 10.0);
    }

    #[test]
    fn prefers_the_cgroup_reading_then_the_host() {
        let dir = tempfile::tempdir().unwrap();
        let calm = dir.path().join("calm");
        let busy = dir.path().join("busy");
        let stalled = dir.path().join("stalled");
        let bad = dir.path().join("bad");
        let missing = dir.path().join("missing");
        std::fs::write(&calm, "some avg10=0.00 avg60=0 avg300=0 total=0\n").unwrap();
        std::fs::write(&busy, "some avg10=50.00 avg60=0 avg300=0 total=0\n").unwrap();
        std::fs::write(
            &stalled,
            "some avg10=60.00 avg60=0 avg300=0 total=0\nfull avg10=40.00 avg60=0 avg300=0 total=0\n",
        )
        .unwrap();
        std::fs::write(&bad, "nothing useful\n").unwrap();
        assert_eq!(
            level_from(Some(&busy), &calm),
            Level::Warning,
            "the cgroup's own reading wins"
        );
        assert_eq!(
            level_from(Some(&calm), &busy),
            Level::Calm,
            "even when calm"
        );
        assert_eq!(level_from(Some(&stalled), &calm), Level::Critical);
        assert_eq!(
            level_from(Some(&missing), &busy),
            Level::Warning,
            "unreadable cgroup: host"
        );
        assert_eq!(
            level_from(Some(&bad), &stalled),
            Level::Critical,
            "unusable cgroup: host"
        );
        assert_eq!(level_from(None, &busy), Level::Warning);
        assert_eq!(level_from(None, &calm), Level::Calm);
        assert_eq!(
            level_from(Some(&missing), &missing),
            Level::Unknown,
            "nothing readable: unknown"
        );
        assert_eq!(level_from(None, &bad), Level::Unknown);
    }

    #[test]
    fn a_reading_is_reused_for_a_while() {
        // Far past any reading an earlier test left on this thread.
        let start = Instant::now() + Duration::from_secs(3600);
        assert_eq!(sampled(start, || Level::Warning), Level::Warning);
        assert_eq!(
            sampled(start + Duration::from_millis(249), || Level::Calm),
            Level::Warning
        );
        assert_eq!(sampled(start + SAMPLE_FOR, || Level::Calm), Level::Calm);
        assert_eq!(
            sampled(start + SAMPLE_FOR, || panic!("reused")),
            Level::Calm
        );
        assert_eq!(SAMPLE_FOR, Duration::from_millis(250));
    }
}
