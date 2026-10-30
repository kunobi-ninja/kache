//! Order in which waiting requests take permit slots.
//!
//! The slot locks stay the authority for capacity: a request runs only once
//! it holds its slots, and the kernel frees them when its process dies. This
//! module only decides which waiter may take free slots first, so a request
//! that needs many slots is not overtaken forever by smaller ones that arrive
//! after it, and a foreground build does not queue behind background ones.
//!
//! A waiting request keeps a ticket: `queue/<rank>-<kind>-<need>-<id>`, whose
//! content says how often the ticket has been passed and when its owner last
//! looked, and `waiters/<id>`, which the owner holds an exclusive lock on.
//! Every admission decision runs under `queue.lock`, so two waiters never
//! plan with the same free slots.
//!
//! - Rank: the arrival time in milliseconds, less [`FOREGROUND_HEAD_START`]
//!   for a foreground build. Lower ranks go first. A ticket keeps its rank
//!   while later ones arrive, so its priority rises the longer it waits: a
//!   background request that has waited the head start outranks every
//!   foreground request that arrives after.
//! - Free slots go to tickets in rank order, each taking the lowest slots it
//!   can use. A ticket that does not fit is passed by later ones that do.
//! - The first ticket can be passed [`pass_budget`] times. After that it
//!   reserves the free slots it can use, so they add up until it fits. A test
//!   reserves only test slots, so a compile a test waits on still runs.
//! - A ticket is live while its owner holds the lock on `waiters/<id>`. A
//!   cancelled waiter removes its ticket; a killed one leaves files whose lock
//!   the kernel has released, and the next planner deletes them. The lock, not
//!   a process ID, says who is alive, so a reused process ID changes nothing.
//! - A ticket whose owner has not looked for [`STALE_AFTER`] is skipped: a
//!   waiter the OS has starved of CPU cannot hold back the queue. It gets its
//!   place back the next time it runs.
//!
//! Wrappers of a Kache release from before this queue take free slots
//! without a ticket. Their slots count like any other, but while releases are
//! mixed such a wrapper can take slots a ticket was saving up.

use anyhow::Result;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::priority::Priority;

/// How far ahead of a background request a foreground one arriving at the
/// same time is placed.
pub(super) const FOREGROUND_HEAD_START: Duration = Duration::from_secs(30);
/// How long a ticket counts after its owner last looked. A waiter looks at
/// least every [`super::permit_wait::SATURATED_SLICE`].
pub(super) const STALE_AFTER: Duration = Duration::from_secs(2);
/// The longest a waiter waits for `queue.lock` before taking slots out of
/// order. Planning takes microseconds; only a holder the OS stopped running
/// keeps the lock longer.
pub(super) const LOCK_WAIT: Duration = Duration::from_secs(1);
/// How soon a waiter tries `queue.lock` again. A release wakes every waiter
/// at once and each holds the lock for microseconds, so a millisecond
/// backoff would leave the lock idle while they sleep.
pub(super) const LOCK_POLL: Duration = Duration::from_micros(200);

pub(super) fn lock_path(root: &Path) -> PathBuf {
    root.join("queue.lock")
}

/// Hold `queue.lock`, trying again every [`LOCK_POLL`] for up to `wait`.
/// `None` when it stayed held. The file is never unlinked, so the lock on
/// the open handle is the lock on the path.
pub(super) fn lock(root: &Path, wait: Duration) -> Result<Option<fs::File>> {
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path(root))?;
    let start = std::time::Instant::now();
    loop {
        crate::invocation::process::check()?;
        match file.try_lock() {
            Ok(()) => return Ok(Some(file)),
            Err(fs::TryLockError::WouldBlock) => {}
            Err(fs::TryLockError::Error(error)) => return Err(error.into()),
        }
        let left = wait.saturating_sub(start.elapsed());
        if left.is_zero() {
            return Ok(None);
        }
        std::thread::sleep(LOCK_POLL.min(left));
    }
}

pub(super) fn queue_dir(root: &Path) -> PathBuf {
    root.join("queue")
}

pub(super) fn waiters_dir(root: &Path) -> PathBuf {
    root.join("waiters")
}

/// How often the first ticket can be passed before it reserves slots: once
/// per slot of the pool.
pub(super) fn pass_budget(pool: u32) -> u32 {
    pool
}

/// Which slots a request can use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// Any slot.
    Compile,
    /// Slots above the compile reserve.
    Test,
}

impl Kind {
    fn tag(self) -> &'static str {
        match self {
            Self::Compile => "c",
            Self::Test => "t",
        }
    }

    fn from_tag(tag: &str) -> Option<Self> {
        match tag {
            "c" => Some(Self::Compile),
            "t" => Some(Self::Test),
            _ => None,
        }
    }

    pub(super) fn uses(self, index: u32, reserve: u32) -> bool {
        match self {
            Self::Compile => true,
            Self::Test => index >= reserve,
        }
    }
}

/// A queued request, as its ticket name describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Entry {
    pub(super) rank: u64,
    pub(super) kind: Kind,
    pub(super) need: u32,
    pub(super) id: String,
}

impl Entry {
    pub(super) fn new(rank: u64, kind: Kind, need: u32) -> Self {
        Self {
            rank,
            kind,
            need,
            id: new_id(),
        }
    }

    pub(super) fn name(&self) -> String {
        format!(
            "{}-{}-{}-{}",
            self.rank,
            self.kind.tag(),
            self.need,
            self.id
        )
    }

    pub(super) fn parse(name: &str) -> Option<Self> {
        let mut fields = name.splitn(4, '-');
        let rank = fields.next()?.parse().ok()?;
        let kind = Kind::from_tag(fields.next()?)?;
        let need = fields.next()?.parse().ok().filter(|need| *need > 0)?;
        let id = fields.next().filter(|id| valid_id(id))?.to_string();
        Some(Self {
            rank,
            kind,
            need,
            id,
        })
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn new_id() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut bytes = [0u8; 8];
    if getrandom::fill(&mut bytes).is_err() {
        // Unique among this process's tickets; a live ticket of another
        // process with the same name makes creation fail and the request
        // takes slots out of order.
        let next = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        bytes = (u64::from(std::process::id()) << 32 | next).to_le_bytes();
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |since| {
        u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
    })
}

/// The rank of a request arriving at `now`.
pub(super) fn rank(now: SystemTime, priority: Priority) -> u64 {
    let arrived = millis(now);
    match priority {
        Priority::Foreground => arrived.saturating_sub(FOREGROUND_HEAD_START.as_millis() as u64),
        Priority::Background => arrived,
    }
}

/// What a queued request's owner and planners write in its ticket.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct State {
    /// Times a later ticket took slots while this one was first and did not
    /// fit.
    pub(super) passes: u32,
    /// When the owner last looked, in milliseconds since the epoch.
    pub(super) seen: u64,
}

impl State {
    fn parse(text: &str) -> Option<Self> {
        let mut fields = text.split_whitespace();
        let passes = fields.next()?.parse().ok()?;
        let seen = fields.next()?.parse().ok()?;
        Some(Self { passes, seen })
    }

    fn fresh(self, now: SystemTime) -> bool {
        millis(now).saturating_sub(self.seen) < STALE_AFTER.as_millis() as u64
    }
}

fn read_state(file: &mut fs::File) -> Option<State> {
    let mut text = String::new();
    file.seek(SeekFrom::Start(0)).ok()?;
    file.read_to_string(&mut text).ok()?;
    State::parse(&text)
}

fn write_state(file: &mut fs::File, state: State) -> Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    write!(file, "{} {}", state.passes, state.seen)?;
    Ok(())
}

/// Every ticket in the queue, in rank order. Names that are not tickets are
/// ignored.
pub(super) fn list(root: &Path) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    let items = match fs::read_dir(queue_dir(root)) {
        Ok(items) => items,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(entries),
        Err(error) => return Err(error.into()),
    };
    for item in items {
        if let Some(entry) = item?.file_name().to_str().and_then(Entry::parse) {
            entries.push(entry);
        }
    }
    sort(&mut entries);
    Ok(entries)
}

fn sort(entries: &mut [Entry]) {
    entries.sort_by(|left, right| (left.rank, &left.id).cmp(&(right.rank, &right.id)));
}

/// Place `entry` among `entries`, unless it is already there.
pub(super) fn with_entry(mut entries: Vec<Entry>, entry: &Entry) -> Vec<Entry> {
    if !entries.iter().any(|queued| queued.id == entry.id) {
        entries.push(entry.clone());
        sort(&mut entries);
    }
    entries
}

/// Whether another request's ticket counts now. A ticket whose owner is gone
/// is deleted. Call only under `queue.lock`.
pub(super) fn ready(root: &Path, entry: &Entry, now: SystemTime) -> bool {
    match owner_alive(&waiters_dir(root).join(&entry.id)) {
        Some(true) => {}
        Some(false) => {
            remove(root, entry);
            return false;
        }
        None => return false,
    }
    fs::File::open(queue_dir(root).join(entry.name()))
        .ok()
        .and_then(|mut file| read_state(&mut file))
        .is_some_and(|state| state.fresh(now))
}

/// `Some(false)` when nothing holds the lock at `path` or the file is gone;
/// `None` when it cannot be probed.
fn owner_alive(path: &Path) -> Option<bool> {
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Some(false),
        Err(_) => return None,
    };
    match file.try_lock_shared() {
        Ok(()) => Some(false),
        Err(fs::TryLockError::WouldBlock) => Some(true),
        Err(fs::TryLockError::Error(_)) => None,
    }
}

/// Create and lock the file that says a waiter is alive.
fn waiter_lock(path: &Path) -> Result<fs::File> {
    let owner = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    if let Err(error) = owner.try_lock() {
        let _ = fs::remove_file(path);
        anyhow::bail!("waiter lock unavailable: {error}");
    }
    Ok(owner)
}

fn remove(root: &Path, entry: &Entry) {
    let _ = fs::remove_file(queue_dir(root).join(entry.name()));
    let _ = fs::remove_file(waiters_dir(root).join(&entry.id));
}

/// How often `entry` has been passed. Call only under `queue.lock`.
pub(super) fn passes(root: &Path, entry: &Entry) -> u32 {
    fs::File::open(queue_dir(root).join(entry.name()))
        .ok()
        .and_then(|mut file| read_state(&mut file))
        .map_or(0, |state| state.passes)
}

/// Count one more pass of `entry`. Call only under `queue.lock`.
pub(super) fn record_pass(root: &Path, entry: &Entry) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(queue_dir(root).join(entry.name()))?;
    let mut state = read_state(&mut file).unwrap_or_default();
    state.passes = state.passes.saturating_add(1);
    write_state(&mut file, state)
}

/// What [`plan`] gives the request it was asked about.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Admitted {
    /// The slots to take now.
    pub(super) slots: Vec<u32>,
    /// The first ticket, when it was waiting and this request passes it.
    pub(super) passed: Option<Entry>,
    /// Free test slots no ticket wants, when asked for.
    pub(super) spare: Vec<u32>,
}

/// Hand `free` slots to the tickets in `queue`, which is in rank order and
/// holds `me`, and say what `me` gets. `None` means wait.
///
/// `ready` is asked only about other tickets that would take slots or would
/// be first, and `passes` only about the first ticket when it does not fit.
/// With `spare`, the tickets after `me` take their slots too, and the test
/// slots left over are spare.
pub(super) fn plan(
    queue: &[Entry],
    me: &str,
    free: &[u32],
    reserve: u32,
    budget: u32,
    spare: bool,
    mut ready: impl FnMut(&Entry) -> bool,
    mut passes: impl FnMut(&Entry) -> u32,
) -> Option<Admitted> {
    let mut available = free.to_vec();
    // The first ticket that counts, and whether it got its slots.
    let mut first: Option<(&Entry, bool)> = None;
    let mut mine = None;
    for entry in queue {
        if mine.is_some() && !spare {
            break;
        }
        let is_me = entry.id == me;
        let fit = take(&available, entry.kind, entry.need, reserve);
        if !is_me && (fit.is_some() || first.is_none()) && !ready(entry) {
            continue;
        }
        let is_first = first.is_none();
        match fit {
            Some(slots) => {
                available.retain(|index| !slots.contains(index));
                if is_first {
                    first = Some((entry, true));
                }
                if is_me {
                    mine = Some(slots);
                }
            }
            None if is_me => return None,
            None if is_first => {
                first = Some((entry, false));
                if passes(entry) >= budget {
                    available.retain(|&index| !entry.kind.uses(index, reserve));
                }
            }
            None => {}
        }
    }
    let slots = mine?;
    let passed = match first {
        Some((entry, false)) => Some(entry.clone()),
        _ => None,
    };
    let spare = if spare {
        available
            .into_iter()
            .filter(|&index| Kind::Test.uses(index, reserve))
            .collect()
    } else {
        Vec::new()
    };
    Some(Admitted {
        slots,
        passed,
        spare,
    })
}

/// The lowest `need` slots of `available` that `kind` can use, if there are
/// that many.
fn take(available: &[u32], kind: Kind, need: u32, reserve: u32) -> Option<Vec<u32>> {
    let slots: Vec<u32> = available
        .iter()
        .copied()
        .filter(|&index| kind.uses(index, reserve))
        .take(need as usize)
        .collect();
    (slots.len() == need as usize).then_some(slots)
}

/// A waiting request's place in the queue. Dropping it leaves the queue.
pub(super) struct Ticket {
    root: PathBuf,
    entry: Entry,
    state: fs::File,
    // Declared last: the lock is released after both files are unlinked.
    _owner: fs::File,
    wake: bool,
}

impl Ticket {
    /// Queue `entry`. Call only under `queue.lock`, so no planner sees the
    /// ticket before its owner holds it.
    pub(super) fn create(root: &Path, entry: Entry, now: SystemTime) -> Result<Self> {
        fs::create_dir_all(queue_dir(root))?;
        fs::create_dir_all(waiters_dir(root))?;
        let owner = waiter_lock(&waiters_dir(root).join(&entry.id))?;
        // From here on, dropping the ticket removes both files.
        let state = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(queue_dir(root).join(entry.name()));
        let state = match state {
            Ok(state) => state,
            Err(error) => {
                let _ = fs::remove_file(waiters_dir(root).join(&entry.id));
                return Err(error.into());
            }
        };
        let mut ticket = Self {
            root: root.to_path_buf(),
            entry,
            state,
            _owner: owner,
            wake: false,
        };
        write_state(
            &mut ticket.state,
            State {
                passes: 0,
                seen: millis(now),
            },
        )?;
        ticket.wake = true;
        Ok(ticket)
    }

    pub(super) fn entry(&self) -> &Entry {
        &self.entry
    }

    /// Note that the owner looked at `now`, asking for `need` slots. Call only
    /// under `queue.lock`.
    pub(super) fn refresh(&mut self, need: u32, now: SystemTime) -> Result<()> {
        if need != self.entry.need {
            let renamed = Entry {
                need,
                ..self.entry.clone()
            };
            fs::rename(
                queue_dir(&self.root).join(self.entry.name()),
                queue_dir(&self.root).join(renamed.name()),
            )?;
            self.entry = renamed;
        }
        let mut state = read_state(&mut self.state).unwrap_or_default();
        state.seen = millis(now);
        write_state(&mut self.state, state)
    }

    /// Leave the queue after taking slots. Nothing changed for the other
    /// waiters that the slot releases did not already tell them.
    pub(super) fn admitted(mut self) {
        self.wake = false;
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let _ = fs::remove_file(queue_dir(&self.root).join(self.entry.name()));
        let _ = fs::remove_file(waiters_dir(&self.root).join(&self.entry.id));
        if self.wake {
            // A waiter that gives up can free slots it was reserving.
            super::permit_wait::notify(&self.root);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn entry(rank: u64, kind: Kind, need: u32, id: &str) -> Entry {
        Entry {
            rank,
            kind,
            need,
            id: id.into(),
        }
    }

    fn at(ms: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(ms)
    }

    /// Plan with every other ticket ready and never passed.
    fn plan_all_ready(queue: &[Entry], me: &str, free: &[u32]) -> Option<Admitted> {
        plan(queue, me, free, 1, 4, false, |_| true, |_| 0)
    }

    #[test]
    fn a_ticket_name_round_trips() {
        let queued = entry(1_793_351_028_000, Kind::Test, 3, "9f3a00c1");
        assert_eq!(queued.name(), "1793351028000-t-3-9f3a00c1");
        assert_eq!(Entry::parse(&queued.name()), Some(queued));
        assert_eq!(
            Entry::parse("5-c-1-ab"),
            Some(entry(5, Kind::Compile, 1, "ab"))
        );
    }

    #[test]
    fn names_that_are_not_tickets_are_rejected() {
        for name in [
            "",
            "5",
            "5-c",
            "5-c-1",
            "5-c-1-",
            "x-c-1-ab",
            "5-x-1-ab",
            "5-c-0-ab",
            "5-c-x-ab",
            "5-c-1-a.b",
            "5-c-1-a-b",
            "-5-c-1-ab",
        ] {
            assert_eq!(Entry::parse(name), None, "{name:?}");
        }
    }

    #[test]
    fn new_ids_are_distinct_ticket_ids() {
        let first = Entry::new(1, Kind::Compile, 1);
        let second = Entry::new(1, Kind::Compile, 1);
        assert_ne!(first.id, second.id);
        assert_eq!(first.id.len(), 16);
        assert_eq!(Entry::parse(&first.name()), Some(first));
    }

    #[test]
    fn a_foreground_request_ranks_thirty_seconds_ahead() {
        assert_eq!(rank(at(100_000), Priority::Background), 100_000);
        assert_eq!(rank(at(100_000), Priority::Foreground), 70_000);
        assert_eq!(rank(at(20_000), Priority::Foreground), 0, "saturates");
        assert_eq!(
            rank(UNIX_EPOCH - Duration::from_secs(1), Priority::Background),
            0
        );
        // Waiting raises priority: a background request that has waited the
        // head start goes before a foreground one that arrives after.
        assert!(rank(at(100_000), Priority::Background) < rank(at(130_001), Priority::Foreground));
        assert!(rank(at(100_000), Priority::Background) > rank(at(129_999), Priority::Foreground));
    }

    #[test]
    fn the_pass_budget_is_one_pass_per_slot() {
        assert_eq!(pass_budget(1), 1);
        assert_eq!(pass_budget(18), 18);
    }

    #[test]
    fn tests_use_only_slots_above_the_reserve() {
        assert!(Kind::Compile.uses(0, 2));
        assert!(Kind::Compile.uses(5, 2));
        assert!(!Kind::Test.uses(1, 2));
        assert!(Kind::Test.uses(2, 2));
    }

    #[test]
    fn a_ticket_is_fresh_for_two_seconds_after_its_owner_looked() {
        let state = State {
            passes: 0,
            seen: 10_000,
        };
        assert!(state.fresh(at(10_000)));
        assert!(state.fresh(at(11_999)));
        assert!(!state.fresh(at(12_000)));
        assert!(state.fresh(at(9_000)), "a clock step back keeps it");
        assert_eq!(
            State::parse("3 10000"),
            Some(State {
                passes: 3,
                seen: 10_000
            })
        );
        assert_eq!(State::parse("3"), None);
        assert_eq!(State::parse("x 1"), None);
        assert_eq!(State::parse("1 x"), None);
    }

    #[test]
    fn with_entry_places_a_new_entry_by_rank_once() {
        let queue = vec![
            entry(1, Kind::Compile, 1, "a"),
            entry(3, Kind::Compile, 1, "c"),
        ];
        let me = entry(2, Kind::Compile, 1, "b");
        let placed = with_entry(queue, &me);
        let ids: Vec<_> = placed.iter().map(|entry| entry.id.as_str()).collect();
        assert_eq!(ids, ["a", "b", "c"]);
        assert_eq!(with_entry(placed.clone(), &me), placed);
        let tied = with_entry(vec![entry(2, Kind::Compile, 1, "z")], &me);
        assert_eq!(tied[0].id, "b", "ties break on the id");
    }

    #[test]
    fn the_only_request_takes_the_lowest_free_slots() {
        let me = entry(1, Kind::Compile, 2, "me");
        let admitted = plan_all_ready(std::slice::from_ref(&me), "me", &[1, 2, 3]).unwrap();
        assert_eq!(admitted.slots, [1, 2]);
        assert_eq!(admitted.passed, None);
        assert!(admitted.spare.is_empty());
        let test = entry(1, Kind::Test, 2, "me");
        let admitted = plan(&[test], "me", &[0, 1, 2], 1, 4, false, |_| true, |_| 0).unwrap();
        assert_eq!(admitted.slots, [1, 2], "a test skips the reserve");
        assert_eq!(plan_all_ready(&[me], "me", &[1]), None);
    }

    #[test]
    fn a_ticket_ahead_that_fits_goes_first() {
        let queue = [
            entry(1, Kind::Compile, 1, "ahead"),
            entry(2, Kind::Compile, 1, "me"),
        ];
        assert_eq!(plan_all_ready(&queue, "me", &[4]), None);
        let admitted = plan_all_ready(&queue, "me", &[4, 5]).unwrap();
        assert_eq!(admitted.slots, [5]);
        assert_eq!(admitted.passed, None, "the first ticket got its slots");
    }

    #[test]
    fn a_large_first_ticket_is_passed_until_its_budget_is_spent() {
        let queue = [
            entry(1, Kind::Compile, 3, "large"),
            entry(2, Kind::Compile, 1, "me"),
        ];
        let with_passes = |passes| plan(&queue, "me", &[0, 1], 0, 2, false, |_| true, |_| passes);
        let admitted = with_passes(0).unwrap();
        assert_eq!(admitted.slots, [0]);
        assert_eq!(admitted.passed, Some(queue[0].clone()));
        assert!(with_passes(1).is_some());
        assert_eq!(with_passes(2), None, "the budget is spent: it reserves");
        assert_eq!(with_passes(3), None);
    }

    #[test]
    fn a_reserving_test_leaves_compiles_the_reserve() {
        let queue = [
            entry(1, Kind::Test, 3, "test"),
            entry(2, Kind::Compile, 1, "me"),
        ];
        let admitted = plan(&queue, "me", &[0, 2], 1, 0, false, |_| true, |_| 0).unwrap();
        assert_eq!(admitted.slots, [0], "a test cannot use slot 0");
        let queue = [
            entry(1, Kind::Compile, 3, "compile"),
            entry(2, Kind::Test, 1, "me"),
        ];
        assert_eq!(
            plan(&queue, "me", &[0, 2], 1, 0, false, |_| true, |_| 0),
            None,
            "a reserving compile can use every slot"
        );
    }

    #[test]
    fn tickets_that_are_not_ready_are_skipped() {
        let queue = [
            entry(1, Kind::Compile, 9, "gone"),
            entry(2, Kind::Compile, 1, "stale"),
            entry(3, Kind::Compile, 2, "large"),
            entry(4, Kind::Compile, 1, "me"),
        ];
        let asked = RefCell::new(Vec::new());
        let admitted = plan(
            &queue,
            "me",
            &[0, 1, 2],
            0,
            0,
            false,
            |entry| {
                asked.borrow_mut().push(entry.id.clone());
                entry.id == "large"
            },
            |_| 0,
        )
        .unwrap();
        assert_eq!(admitted.slots, [2], "the large ticket is first and fits");
        assert_eq!(admitted.passed, None);
        assert_eq!(*asked.borrow(), ["gone", "stale", "large"]);
    }

    #[test]
    fn readiness_is_asked_only_of_tickets_that_matter() {
        let queue = [
            entry(1, Kind::Compile, 1, "first"),
            entry(2, Kind::Compile, 5, "large"),
            entry(3, Kind::Compile, 1, "me"),
            entry(4, Kind::Compile, 1, "after"),
        ];
        let asked = RefCell::new(Vec::new());
        let passes_asked = RefCell::new(0);
        let admitted = plan(
            &queue,
            "me",
            &[0, 1],
            0,
            0,
            false,
            |entry| {
                asked.borrow_mut().push(entry.id.clone());
                true
            },
            |_| {
                *passes_asked.borrow_mut() += 1;
                0
            },
        )
        .unwrap();
        assert_eq!(admitted.slots, [1]);
        assert_eq!(
            *asked.borrow(),
            ["first"],
            "a later large ticket takes nothing"
        );
        assert_eq!(*passes_asked.borrow(), 0, "the first ticket fit");
    }

    #[test]
    fn a_first_request_that_does_not_fit_waits_without_asking() {
        let queue = [
            entry(1, Kind::Compile, 3, "me"),
            entry(2, Kind::Compile, 1, "small"),
        ];
        let asked = RefCell::new(0);
        let plan = plan(
            &queue,
            "me",
            &[0],
            0,
            0,
            true,
            |_| {
                *asked.borrow_mut() += 1;
                true
            },
            |_| {
                *asked.borrow_mut() += 1;
                0
            },
        );
        assert_eq!(plan, None);
        assert_eq!(*asked.borrow(), 0);
    }

    #[test]
    fn spare_test_slots_are_what_every_ticket_leaves() {
        let queue = [
            entry(1, Kind::Test, 2, "ahead"),
            entry(2, Kind::Test, 1, "me"),
            entry(3, Kind::Test, 2, "after"),
            entry(4, Kind::Compile, 1, "compile"),
        ];
        let admitted = plan(
            &queue,
            "me",
            &[0, 1, 2, 3, 4, 5, 6, 7],
            2,
            4,
            true,
            |_| true,
            |_| 0,
        )
        .unwrap();
        assert_eq!(admitted.slots, [4]);
        assert_eq!(
            admitted.spare,
            [7],
            "0 and 1 are the reserve; 6 is the compile's"
        );
        let without = plan(
            &queue,
            "me",
            &[0, 1, 2, 3, 4, 5, 6, 7],
            2,
            4,
            false,
            |_| true,
            |_| 0,
        )
        .unwrap();
        assert!(without.spare.is_empty());
        let asked = RefCell::new(Vec::new());
        plan(
            &queue,
            "me",
            &[2, 3, 4],
            2,
            4,
            false,
            |entry| {
                asked.borrow_mut().push(entry.id.clone());
                true
            },
            |_| 0,
        )
        .unwrap();
        assert_eq!(
            *asked.borrow(),
            ["ahead"],
            "no spare: tickets after are not read"
        );
    }

    fn root() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    #[test]
    fn a_ticket_is_listed_while_held_and_removed_when_dropped() {
        let dir = root();
        assert!(list(dir.path()).unwrap().is_empty(), "no queue yet");
        let queued = entry(7, Kind::Compile, 2, "abc");
        let ticket = Ticket::create(dir.path(), queued.clone(), at(5_000)).unwrap();
        assert_eq!(ticket.entry(), &queued);
        fs::write(queue_dir(dir.path()).join("not-a-ticket"), b"").unwrap();
        assert_eq!(list(dir.path()).unwrap(), std::slice::from_ref(&queued));
        assert_eq!(
            owner_alive(&waiters_dir(dir.path()).join("abc")),
            Some(true)
        );
        assert!(ready(dir.path(), &queued, at(6_000)));
        assert!(!ready(dir.path(), &queued, at(7_000)), "stale");
        assert_eq!(
            list(dir.path()).unwrap(),
            std::slice::from_ref(&queued),
            "stale is kept"
        );
        drop(ticket);
        assert!(list(dir.path()).unwrap().is_empty());
        assert!(!waiters_dir(dir.path()).join("abc").exists());
    }

    #[test]
    fn list_sorts_by_rank_and_reports_an_unreadable_queue() {
        let dir = root();
        let _late = Ticket::create(dir.path(), entry(9, Kind::Compile, 1, "late"), at(0)).unwrap();
        let _early = Ticket::create(dir.path(), entry(3, Kind::Test, 1, "early"), at(0)).unwrap();
        let ids: Vec<_> = list(dir.path())
            .unwrap()
            .into_iter()
            .map(|entry| entry.id)
            .collect();
        assert_eq!(ids, ["early", "late"]);
        let file = root();
        fs::write(file.path().join("queue"), b"").unwrap();
        assert!(list(file.path()).is_err());
    }

    #[test]
    fn a_ticket_whose_owner_is_gone_is_deleted() {
        let dir = root();
        let queued = entry(7, Kind::Compile, 1, "dead");
        fs::create_dir_all(queue_dir(dir.path())).unwrap();
        fs::create_dir_all(waiters_dir(dir.path())).unwrap();
        // What a killed owner leaves: both files, and no lock.
        fs::write(queue_dir(dir.path()).join(queued.name()), b"0 5000").unwrap();
        fs::write(waiters_dir(dir.path()).join("dead"), b"").unwrap();
        assert!(!ready(dir.path(), &queued, at(5_000)));
        assert!(list(dir.path()).unwrap().is_empty());
        assert!(!waiters_dir(dir.path()).join("dead").exists());

        // A ticket whose lock file is already gone.
        fs::write(queue_dir(dir.path()).join(queued.name()), b"0 5000").unwrap();
        assert!(!ready(dir.path(), &queued, at(5_000)));
        assert!(list(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn a_live_ticket_with_no_state_is_not_ready() {
        let dir = root();
        let queued = entry(7, Kind::Compile, 1, "blank");
        let _ticket = Ticket::create(dir.path(), queued.clone(), at(5_000)).unwrap();
        fs::write(queue_dir(dir.path()).join(queued.name()), b"").unwrap();
        assert!(!ready(dir.path(), &queued, at(5_000)));
        assert_eq!(
            list(dir.path()).unwrap(),
            [queued],
            "its owner still holds it"
        );
    }

    #[test]
    fn refreshing_a_ticket_renames_it_and_keeps_its_passes() {
        let dir = root();
        let queued = entry(7, Kind::Compile, 4, "grow");
        let mut ticket = Ticket::create(dir.path(), queued.clone(), at(1_000)).unwrap();
        record_pass(dir.path(), &queued).unwrap();
        record_pass(dir.path(), &queued).unwrap();
        assert_eq!(passes(dir.path(), &queued), 2);

        ticket.refresh(4, at(3_000)).unwrap();
        assert_eq!(
            list(dir.path()).unwrap(),
            std::slice::from_ref(&queued),
            "same need, same name"
        );
        assert!(ready(dir.path(), &queued, at(4_000)), "seen again at 3 s");

        ticket.refresh(2, at(5_000)).unwrap();
        let shrunk = entry(7, Kind::Compile, 2, "grow");
        assert_eq!(ticket.entry(), &shrunk);
        assert_eq!(list(dir.path()).unwrap(), std::slice::from_ref(&shrunk));
        assert_eq!(passes(dir.path(), &shrunk), 2);
        assert_eq!(passes(dir.path(), &queued), 0, "the old name is gone");
        assert!(record_pass(dir.path(), &queued).is_err());
        drop(ticket);
        assert!(list(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn a_taken_name_leaves_no_waiter_lock_behind() {
        let dir = root();
        let queued = entry(7, Kind::Compile, 1, "twice");
        fs::create_dir_all(queue_dir(dir.path())).unwrap();
        fs::write(queue_dir(dir.path()).join(queued.name()), b"").unwrap();
        assert!(Ticket::create(dir.path(), queued, at(0)).is_err());
        assert!(!waiters_dir(dir.path()).join("twice").exists());
        let held = Ticket::create(dir.path(), entry(1, Kind::Compile, 1, "held"), at(0)).unwrap();
        assert!(
            Ticket::create(dir.path(), entry(2, Kind::Compile, 1, "held"), at(0)).is_err(),
            "an id another waiter holds"
        );
        assert!(waiters_dir(dir.path()).join("held").exists());
        drop(held);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn only_a_ticket_that_gives_up_wakes_the_waiters() {
        let dir = root();
        let seen = super::super::permit_wait::snapshot(dir.path());
        Ticket::create(dir.path(), entry(1, Kind::Compile, 1, "admitted"), at(0))
            .unwrap()
            .admitted();
        assert_eq!(super::super::permit_wait::snapshot(dir.path()), seen);
        drop(Ticket::create(dir.path(), entry(1, Kind::Compile, 1, "cancelled"), at(0)).unwrap());
        assert_eq!(
            super::super::permit_wait::snapshot(dir.path()),
            seen.wrapping_add(1)
        );
    }
}
