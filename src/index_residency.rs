//! Keeps the store's index in the page cache while the machine has room.
//!
//! Every hit reads the index several times before its compiler would have
//! started: the unit's input prediction, its entry, the file-hash memo. Each
//! read is a page or two of a file that grows to gigabytes, and on a machine
//! whose builds stream hundreds of gigabytes through the page cache those
//! pages are the first to go: they are read once per unit, long ago, while
//! the builds' own files are fresh. On a 196 GB build machine with 175 GB
//! available, three quarters of a 6.7 GB index had left the cache in a busy
//! hour. A read of an evicted page waits for the disk, and on a disk busy
//! with the builds' writes that wait is 100-300 ms, for every unit reading
//! at that moment: a warm build of 900 units spent 6.1 s in 1,967 prediction
//! reads (p99 121 ms), against 0.1 s (p99 0.3 ms) with the index cached.
//!
//! So the daemon looks at which of the index's pages are resident
//! (`mincore` on a read-only mapping, which reads nothing) and reads the
//! missing ones back, a bounded amount per pass, below normal priority. It
//! reads each twice: a page read once lands on the kernel's inactive list,
//! which the builds' streaming turns over in minutes (a 6.7 GB index read
//! once was a third resident two minutes later), while a second read moves
//! it to the active list, which ages far more slowly. It does so only while
//! the machine's available memory is many times the index's size: on a
//! machine with little room the builds need the memory more than the index
//! needs the speed, and the kernel's choice stands.
//!
//! The active list does not keep a page for good, though. The kernel moves
//! file pages off its tail whatever their use (only mapped executables are
//! spared), and a page that is not read twice during its minute or two on
//! the inactive list then goes. Pages read back in one pass reach that tail
//! together, one turnover of the active list later (15-25 minutes on a busy
//! evening of the same machine), so the keeper found 3-6 GB of the index
//! gone at once several times an hour and read 18-24 GB an hour back from
//! the disk. So each pass also reads one byte of every page that is still
//! resident, twice, from memory: a page the builds pushed to the inactive
//! list moves back to the active list before it is evicted, and the disk is
//! read only for what slipped out anyway. (Faulting the pages into a fresh mapping with
//! `MADV_POPULATE_READ` costs a third as much but did not keep them: on the
//! same machine a 1 GiB file marked that way lost 97 % in its wave, one read
//! a page 5 %.)
//!
//! The keeper holds each file open for the daemon's life. Closing any
//! descriptor of a file drops every POSIX lock the process holds on it,
//! SQLite's own included, so a descriptor of a file since replaced is kept
//! too, never closed.
//!
//! When what one pass read back is missing again at the next (a host taking
//! back an idle guest's memory), the keeper doubles its wait, up to
//! [`MAX_INTERVAL`], until a pass gains ground again.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// How often the daemon looks.
pub(crate) const CHECK_INTERVAL: Duration = Duration::from_secs(30);

/// The longest the keeper waits between passes while it backs off.
const MAX_INTERVAL: Duration = Duration::from_secs(8 * 60);

/// A read-back at least this large that the next pass finds missing again
/// makes the keeper back off.
const BACKOFF_FLOOR_BYTES: u64 = 64 << 20;

/// The most a pass asks the kernel to read back, over every file.
const PASS_BUDGET_BYTES: u64 = 1 << 30;

/// The index is kept resident only while the machine's available memory is
/// at least this many times its size...
const ROOM_FACTOR: u64 = 8;

/// ...and at least this much.
const ROOM_FLOOR_BYTES: u64 = 16 << 30;

/// Missing pages closer than this are asked for in one request.
const MERGE_GAP_BYTES: u64 = 256 << 10;

/// What one pass found and did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pass {
    /// Bytes of the files looked at.
    pub(crate) file_bytes: u64,
    /// Of those, bytes of the spans not in the page cache, with the cached
    /// gaps under [`MERGE_GAP_BYTES`] between them that a read-back reads.
    pub(crate) missing_bytes: u64,
    /// Bytes read back (at most [`PASS_BUDGET_BYTES`]).
    pub(crate) read_bytes: u64,
    /// Bytes of the resident pages touched (one byte read of each).
    pub(crate) touched_bytes: u64,
}

/// Why a pass did nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Skipped {
    /// `KACHE_KEEP_INDEX_RESIDENT=0`.
    Off,
    /// Available memory is under the room the index needs.
    NoRoom { available: u64, needed: u64 },
    /// No index yet, or memory could not be read.
    Unknown,
}

/// Whether `KACHE_KEEP_INDEX_RESIDENT` leaves the keeper on (anything but
/// `0`, `false`, `off` or `no`).
pub(crate) fn enabled(value: Option<&str>) -> bool {
    !matches!(
        value
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref(),
        Some("0" | "false" | "off" | "no")
    )
}

/// The room the index's `bytes` need before the keeper holds them.
pub(crate) fn room_needed(bytes: u64) -> u64 {
    bytes.saturating_mul(ROOM_FACTOR).max(ROOM_FLOOR_BYTES)
}

/// The index file and its write-ahead log.
pub(crate) fn index_files(db_path: &Path) -> [PathBuf; 2] {
    let mut wal = db_path.as_os_str().to_owned();
    wal.push("-wal");
    [db_path.to_path_buf(), PathBuf::from(wal)]
}

/// How long to wait after `pass`, given the pass before it and the wait that
/// led here. Twice as long, up to [`MAX_INTERVAL`], when the earlier pass
/// read back at least [`BACKOFF_FLOOR_BYTES`] and the missing bytes shrank
/// by less than half of that since; [`CHECK_INTERVAL`] otherwise.
pub(crate) fn next_interval(
    waited: Duration,
    previous: Option<&Pass>,
    pass: Option<&Pass>,
) -> Duration {
    let (Some(previous), Some(pass)) = (previous, pass) else {
        return CHECK_INTERVAL;
    };
    let gained = previous.missing_bytes.saturating_sub(pass.missing_bytes);
    if previous.read_bytes >= BACKOFF_FLOOR_BYTES && gained.saturating_mul(2) < previous.read_bytes
    {
        (waited * 2).min(MAX_INTERVAL)
    } else {
        CHECK_INTERVAL
    }
}

/// An index file the keeper holds open, and the inode it is.
struct Held {
    file: File,
    dev: u64,
    ino: u64,
}

/// The keeper's files and what its last pass found.
pub(crate) struct Keeper {
    paths: [PathBuf; 2],
    held: [Option<Held>; 2],
    /// Descriptors of index files since replaced, never closed (module
    /// docs).
    retired: Vec<File>,
    interval: Duration,
    last: Option<Pass>,
}

impl Keeper {
    pub(crate) fn new(db_path: &Path) -> Self {
        Self {
            paths: index_files(db_path),
            held: [None, None],
            retired: Vec::new(),
            interval: CHECK_INTERVAL,
            last: None,
        }
    }

    /// One pass, then how long to wait before the next.
    pub(crate) fn tick(&mut self, available: Option<u64>) -> Duration {
        let pass = self.keep_resident(available).ok();
        if let Some(pass) = &pass
            && worth_a_line(pass)
        {
            tracing::info!(
                index_mib = pass.file_bytes >> 20,
                missing_mib = pass.missing_bytes >> 20,
                read_mib = pass.read_bytes >> 20,
                touched_mib = pass.touched_bytes >> 20,
                "index pages read back into memory"
            );
        }
        self.interval = next_interval(self.interval, self.last.as_ref(), pass.as_ref());
        self.last = pass;
        self.interval
    }

    /// The held files that exist, by index, with their lengths. A path that
    /// names another inode than the one held is opened again, and the old
    /// descriptor goes to [`retire`].
    fn refresh(&mut self) -> Vec<(usize, u64)> {
        let mut found = Vec::new();
        for (index, path) in self.paths.iter().enumerate() {
            let Ok(meta) = std::fs::metadata(path) else {
                continue;
            };
            let same = self.held[index]
                .as_ref()
                .is_some_and(|held| held.dev == meta.dev() && held.ino == meta.ino());
            if !same {
                let Ok(file) = File::open(path) else {
                    continue;
                };
                let Ok(opened) = file.metadata() else {
                    retire(&mut self.retired, index, file);
                    continue;
                };
                let held = Held {
                    file,
                    dev: opened.dev(),
                    ino: opened.ino(),
                };
                if let Some(old) = self.held[index].replace(held) {
                    retire(&mut self.retired, index, old.file);
                }
            }
            if let Some(held) = &self.held[index]
                && let Ok(meta) = held.file.metadata()
            {
                found.push((index, meta.len()));
            }
        }
        found
    }

    /// One pass over the index's files, if `available` memory leaves room
    /// for them.
    fn keep_resident(&mut self, available: Option<u64>) -> Result<Pass, Skipped> {
        if !enabled(std::env::var("KACHE_KEEP_INDEX_RESIDENT").ok().as_deref()) {
            return Err(Skipped::Off);
        }
        let files = self.refresh();
        let total: u64 = files.iter().map(|(_, len)| len).sum();
        let Some(available) = available else {
            return Err(Skipped::Unknown);
        };
        if total == 0 {
            return Err(Skipped::Unknown);
        }
        let needed = room_needed(total);
        if available < needed {
            return Err(Skipped::NoRoom { available, needed });
        }
        let mut pass = Pass::default();
        let mut budget = PASS_BUDGET_BYTES;
        let mut reads: Vec<(&File, u64, u64)> = Vec::new();
        for (index, len) in files {
            let Some(held) = &self.held[index] else {
                continue;
            };
            let Ok(found) = residency(&held.file, len) else {
                continue;
            };
            pass.file_bytes += len;
            pass.touched_bytes += found.touched;
            let runs = spans(&found.pages, found.page);
            pass.missing_bytes += runs.iter().map(|(_, run)| run).sum::<u64>();
            for (offset, ask) in take(&mut budget, &runs) {
                reads.push((&held.file, offset, ask));
            }
        }
        pass.read_bytes = read_twice(&reads);
        Ok(pass)
    }
}

/// Put away the descriptor of a replaced file: the index's (`index` 0) is
/// kept open in `retired`, since closing it would drop SQLite's locks; the
/// WAL's is closed, since SQLite takes no lock on the WAL file itself and a
/// kept descriptor would hold a deleted log's blocks.
fn retire(retired: &mut Vec<File>, index: usize, file: File) {
    if index == 0 {
        retired.push(file);
    }
}

/// The machine's available memory from `/proc/meminfo`'s text, in bytes.
pub(crate) fn mem_available(meminfo: &str) -> Option<u64> {
    let kib: u64 = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    kib.checked_mul(1024)
}

/// Whether a pass read enough to say so in the log (64 MiB or more): the
/// small passes of a quiet machine would fill it.
pub(crate) fn worth_a_line(pass: &Pass) -> bool {
    pass.read_bytes >= 64 << 20
}

/// Nice of the keeper's thread: below the builds, which no pass should slow.
const KEEPER_NICE: libc::c_int = 10;

/// Start the keeper for the index at `db_path`: a thread of its own, at a
/// lower priority, for the daemon's life. Only the waiting is here; each
/// pass is [`Keeper::tick`], which the tests drive.
#[mutants::skip]
pub(crate) fn spawn(db_path: &Path) {
    // Never dropped, even if the thread unwinds: dropping it would close the
    // index's descriptors (module docs).
    let mut keeper = std::mem::ManuallyDrop::new(Keeper::new(db_path));
    let started = std::thread::Builder::new()
        .name("kache-index-keeper".into())
        .spawn(move || {
            // SAFETY: gettid and setpriority take no pointers and act on
            // this thread alone; a refusal leaves its inherited priority.
            unsafe {
                let tid = libc::syscall(libc::SYS_gettid) as libc::id_t;
                if libc::getpriority(libc::PRIO_PROCESS, tid) < KEEPER_NICE {
                    libc::setpriority(libc::PRIO_PROCESS, tid, KEEPER_NICE);
                }
            }
            let mut wait = CHECK_INTERVAL;
            loop {
                std::thread::sleep(wait);
                let available = std::fs::read_to_string("/proc/meminfo")
                    .ok()
                    .and_then(|text| mem_available(&text));
                wait = keeper.tick(available);
            }
        });
    if let Err(error) = started {
        tracing::warn!("could not start the index keeper: {error}");
    }
}

/// Runs of `file`'s pages not in the page cache, as (offset, length) in
/// bytes. Reads none of them from the disk (and marks the resident ones
/// used, as every look does).
#[cfg(test)]
fn missing_runs(file: &File, len: u64) -> io::Result<Vec<(u64, u64)>> {
    let found = residency(file, len)?;
    Ok(runs_of(&found.pages, found.page))
}

/// Whether `mincore`'s byte for a page says it is in the page cache. Only
/// the lowest bit counts; the kernel may set others.
fn resident(flags: u8) -> bool {
    flags & 1 == 1
}

/// The byte offset of page `index`, if `off_t` holds it.
fn page_offset(index: usize, page: u64) -> Option<libc::off_t> {
    libc::off_t::try_from((index as u64).checked_mul(page)?).ok()
}

/// What a one-byte `RWF_NOWAIT` read that returned `read` (with `errno`
/// after it) says: `Some(true)` it read the page, `Some(false)` the page
/// left the cache since `mincore` looked, `None` stop the sweep (the file
/// system cannot read without waiting, or the file shrank).
fn touch_result(read: isize, errno: Option<i32>) -> Option<bool> {
    match read {
        1 => Some(true),
        -1 if errno == Some(libc::EAGAIN) => Some(false),
        _ => None,
    }
}

/// Read one byte of each page of `file` that `pages` (one byte a page of
/// `page` bytes, `mincore`'s answer) says is in the page cache, in two
/// sweeps, and return how many bytes of pages the first touched. A read
/// marks its page used, and a page on the inactive list marked twice moves
/// back to the active list at once; the kernel marks a page only once for
/// reads of the same page in a row, hence two sweeps rather than two reads.
/// (One sweep a pass lost 40 % of a file in a wave where the inactive list
/// held a page for about two passes: marked once there, it was evicted
/// before the next.) A read, not a load through a mapping: a load past the
/// end of a file that shrank meanwhile (a `VACUUM`) would raise `SIGBUS`.
fn touch_resident(file: &File, pages: &[u8], page: u64) -> u64 {
    let Some(touched) = touch_sweep(file, pages, page) else {
        return 0;
    };
    touch_sweep(file, pages, page);
    touched
}

/// One sweep of [`touch_resident`]. `RWF_NOWAIT`: a page evicted since
/// `mincore` looked is left to the read-back. Where the file system cannot
/// read without waiting (tmpfs answers `EOPNOTSUPP`), it stops at the first
/// refusal: nothing is marked there, and a failing call a page would only
/// cost.
fn touch_sweep(file: &File, pages: &[u8], page: u64) -> Option<u64> {
    let mut byte = 0u8;
    let mut touched = 0;
    for (index, &flags) in pages.iter().enumerate() {
        if !resident(flags) {
            continue;
        }
        let offset = page_offset(index, page)?;
        let iov = libc::iovec {
            iov_base: (&raw mut byte).cast(),
            iov_len: 1,
        };
        // SAFETY: `iov` names the one writable byte `byte` for the call's
        // length, and the descriptor is the caller's open file.
        let read = unsafe { libc::preadv2(file.as_raw_fd(), &iov, 1, offset, libc::RWF_NOWAIT) };
        if touch_result(read, io::Error::last_os_error().raw_os_error())? {
            touched += page;
        }
    }
    Some(touched)
}

/// What [`residency`] found.
struct Residency {
    /// One byte per page of the file; the lowest bit says it is cached.
    pages: Vec<u8>,
    /// The page size.
    page: u64,
    /// Bytes of the resident pages marked used.
    touched: u64,
}

/// Which pages of `file` (its first `len` bytes) are in the page cache,
/// reading none from the disk, and each resident one read once from memory
/// ([`touch_resident`]).
fn residency(file: &File, len: u64) -> io::Result<Residency> {
    if len == 0 {
        return Ok(Residency {
            pages: Vec::new(),
            page: 4096,
            touched: 0,
        });
    }
    // SAFETY: sysconf has no preconditions.
    let page = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(4096);
    let map_len = usize::try_from(len).map_err(|_| io::Error::other("index too large to map"))?;
    // SAFETY: a fresh read-only shared mapping of an open file the caller
    // owns for the call; nothing loads through it (mincore only asks the
    // kernel about it), so a concurrent writer or truncation cannot fault
    // here, and it is unmapped below.
    let addr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            map_len,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    };
    if addr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    let pages = len.div_ceil(page);
    let mut vec = vec![0u8; usize::try_from(pages).unwrap_or(0)];
    // SAFETY: `addr` is the page-aligned start of the `map_len`-byte mapping
    // made above, and `vec` holds one byte per page of it, as mincore needs.
    let rc = unsafe { libc::mincore(addr, map_len, vec.as_mut_ptr().cast()) };
    if rc != 0 {
        let error = io::Error::last_os_error();
        // SAFETY: the mapping made above, unmapped once; nothing refers to it.
        unsafe {
            libc::munmap(addr, map_len);
        }
        return Err(error);
    }
    // SAFETY: the mapping made above, unmapped once; nothing refers to it.
    unsafe {
        libc::munmap(addr, map_len);
    }
    let touched = touch_resident(file, &vec, page);
    Ok(Residency {
        pages: vec,
        page,
        touched,
    })
}

/// The (offset, length) runs of pages whose resident bit is clear.
fn runs_of(vec: &[u8], page: u64) -> Vec<(u64, u64)> {
    let mut runs = Vec::new();
    let mut start = None;
    for (index, &flags) in vec.iter().enumerate() {
        let index = index as u64;
        match (resident(flags), start) {
            (false, None) => start = Some(index),
            (true, Some(first)) => {
                runs.push((first * page, (index - first) * page));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(first) = start {
        runs.push((first * page, (vec.len() as u64 - first) * page));
    }
    runs
}

/// The spans a pass reads back: the runs of missing pages, those less than
/// [`MERGE_GAP_BYTES`] apart joined.
fn spans(pages: &[u8], page: u64) -> Vec<(u64, u64)> {
    merge(&runs_of(pages, page), MERGE_GAP_BYTES)
}

/// `runs` with gaps under `gap` bytes between them joined.
fn merge(runs: &[(u64, u64)], gap: u64) -> Vec<(u64, u64)> {
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for &(offset, len) in runs {
        match merged.last_mut() {
            Some((first, total)) if offset <= *first + *total + gap => {
                *total = offset + len - *first;
            }
            _ => merged.push((offset, len)),
        }
    }
    merged
}

/// The reads of `runs` that `budget` still allows, taken from it.
fn take(budget: &mut u64, runs: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut reads = Vec::new();
    for &(offset, run) in runs {
        let ask = run.min(*budget);
        if ask == 0 {
            break;
        }
        *budget -= ask;
        reads.push((offset, ask));
    }
    reads
}

/// Read each of `reads` (file, offset, length) back twice, in two sweeps,
/// and return the bytes the first sweep read. Two sweeps for the reason
/// [`touch_resident`] gives: a run read twice in a row would be marked once
/// where it is a single page.
fn read_twice(reads: &[(&File, u64, u64)]) -> u64 {
    let read: u64 = reads
        .iter()
        .map(|&(file, offset, len)| read_back(file, offset, len))
        .sum();
    for &(file, offset, len) in reads {
        read_back(file, offset, len);
    }
    read
}

/// Read `len` bytes of `file` from `offset` and throw them away, returning
/// how many there were: the read puts the pages in the page cache and marks
/// them used. It stops at the end of the file or at an error.
fn read_back(file: &File, offset: u64, len: u64) -> u64 {
    use std::os::unix::fs::FileExt;
    let mut buffer = vec![0u8; 1 << 20];
    let mut done = 0u64;
    while done != len {
        let want = usize::try_from((len - done).min(buffer.len() as u64)).unwrap_or(buffer.len());
        let before = done;
        match file.read_at(&mut buffer[..want], offset + done) {
            Ok(0) | Err(_) => break,
            Ok(read) => done += read as u64,
        }
        debug_assert!(done > before, "a read-back that read nothing kept going");
    }
    done
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_of(bytes: usize) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("index.db");
        std::fs::write(&path, vec![7u8; bytes]).expect("an index");
        (dir, path)
    }

    #[test]
    fn available_memory_is_read_in_kib() {
        let text = "MemTotal:       16384 kB\nMemFree:  1 kB\nMemAvailable:    2048 kB\n";
        assert_eq!(mem_available(text), Some(2048 * 1024));
        assert_eq!(mem_available("MemTotal: 16384 kB\n"), None);
        assert_eq!(mem_available("MemAvailable: lots kB\n"), None);
    }

    #[test]
    fn only_a_pass_of_64_mib_or_more_is_logged() {
        let pass = |read_bytes| Pass {
            read_bytes,
            ..Pass::default()
        };
        assert!(!worth_a_line(&pass((64 << 20) - 1)));
        assert!(worth_a_line(&pass(64 << 20)));
    }

    #[test]
    fn only_the_lowest_bit_says_resident() {
        assert!(resident(1));
        assert!(resident(3));
        assert!(!resident(0));
        assert!(!resident(2));
    }

    #[test]
    fn page_offsets_are_index_times_page() {
        assert_eq!(page_offset(3, 4096), Some(3 * 4096));
        assert_eq!(page_offset(usize::MAX, 4096), None);
    }

    #[test]
    fn only_eagain_skips_a_page() {
        assert_eq!(touch_result(1, None), Some(true));
        assert_eq!(touch_result(-1, Some(libc::EAGAIN)), Some(false));
        assert_eq!(touch_result(-1, Some(libc::EOPNOTSUPP)), None);
        assert_eq!(touch_result(0, Some(libc::EAGAIN)), None);
    }

    #[test]
    fn missing_pages_come_out_as_runs() {
        let page = 4096;
        assert_eq!(runs_of(&[1, 1, 1], page), vec![]);
        assert_eq!(
            runs_of(&[0, 0, 1, 0, 1, 1, 0], page),
            vec![(0, 2 * page), (3 * page, page), (6 * page, page)]
        );
        assert_eq!(runs_of(&[2, 3], page), vec![(0, page)]);
    }

    #[test]
    fn close_runs_are_asked_for_together() {
        let runs = [(0, 4096), (8192, 4096), (1 << 20, 4096)];
        assert_eq!(merge(&runs, 8192), vec![(0, 12288), (1 << 20, 4096)]);
        assert_eq!(
            merge(&runs, 0),
            vec![(0, 4096), (8192, 4096), (1 << 20, 4096)]
        );
        assert_eq!(
            merge(&runs, 4096),
            vec![(0, 12288), (1 << 20, 4096)],
            "a gap of exactly `gap` joins"
        );
        assert_eq!(
            merge(&runs, 4095),
            vec![(0, 4096), (8192, 4096), (1 << 20, 4096)]
        );
        assert_eq!(
            merge(&[(4096, 4096), (8192, 8192)], 0),
            vec![(4096, 12288)],
            "a run joined to one past the start of the file"
        );
    }

    #[test]
    fn holes_up_to_256_kib_apart_are_read_as_one_span() {
        let page = 4096;
        let holes = |apart: usize| {
            let mut pages = vec![0u8];
            pages.extend(std::iter::repeat_n(1u8, apart));
            pages.push(0);
            pages
        };
        assert_eq!(spans(&holes(64), page), vec![(0, 66 * page)]);
        assert_eq!(spans(&holes(65), page), vec![(0, page), (66 * page, page)]);
    }

    #[test]
    fn a_pass_reads_no_more_than_its_budget() {
        let mut budget = 5 * 4096;
        let runs = [(0, 3 * 4096), (1 << 20, 3 * 4096), (2 << 20, 4096)];
        assert_eq!(
            take(&mut budget, &runs),
            vec![(0, 3 * 4096), (1 << 20, 2 * 4096)]
        );
        assert_eq!(budget, 0);
        assert_eq!(take(&mut budget, &runs), vec![], "the next file gets none");
    }

    #[test]
    fn a_read_back_counts_what_it_read() {
        let (_dir, path) = index_of(4 << 20);
        let file = File::open(&path).expect("the index opens");
        let half = 3 << 19;
        assert_eq!(read_back(&file, 4096, half), half, "across reads of 1 MiB");
        assert_eq!(
            read_back(&file, (4 << 20) - 100, 4096),
            100,
            "up to the end"
        );
        assert_eq!(read_back(&file, 8 << 20, 10), 0, "past the end");
        assert_eq!(
            read_twice(&[(&file, 0, 4096), (&file, 1 << 20, 8192)]),
            3 * 4096,
            "the first sweep's bytes"
        );
    }

    #[test]
    fn the_keeper_backs_off_while_its_reads_do_not_stay() {
        let gib = 1 << 30;
        let read = |missing_bytes, read_bytes| Pass {
            missing_bytes,
            read_bytes,
            ..Pass::default()
        };
        let lost = read(5 * gib, gib);
        assert_eq!(
            next_interval(CHECK_INTERVAL, Some(&lost), Some(&read(5 * gib, gib))),
            2 * CHECK_INTERVAL
        );
        assert_eq!(
            next_interval(2 * CHECK_INTERVAL, Some(&lost), Some(&read(5 * gib, gib))),
            4 * CHECK_INTERVAL
        );
        assert_eq!(
            next_interval(MAX_INTERVAL, Some(&lost), Some(&read(5 * gib, gib))),
            MAX_INTERVAL
        );
        assert_eq!(
            next_interval(4 * CHECK_INTERVAL, Some(&lost), Some(&read(4 * gib, gib))),
            CHECK_INTERVAL,
            "a pass that gains ground resets the wait"
        );
        assert_eq!(
            next_interval(
                CHECK_INTERVAL,
                Some(&lost),
                Some(&read(5 * gib - gib / 2, 0))
            ),
            CHECK_INTERVAL,
            "half of the read-back kept is ground gained"
        );
        assert_eq!(
            next_interval(
                CHECK_INTERVAL,
                Some(&lost),
                Some(&read(5 * gib - gib / 2 + 1, 0))
            ),
            2 * CHECK_INTERVAL
        );
        let small = read(5 * gib, BACKOFF_FLOOR_BYTES);
        assert_eq!(
            next_interval(CHECK_INTERVAL, Some(&small), Some(&small)),
            2 * CHECK_INTERVAL
        );
        let smaller = read(5 * gib, BACKOFF_FLOOR_BYTES - 1);
        assert_eq!(
            next_interval(CHECK_INTERVAL, Some(&smaller), Some(&smaller)),
            CHECK_INTERVAL,
            "a small read-back never backs off"
        );
        assert_eq!(
            next_interval(MAX_INTERVAL, None, Some(&lost)),
            CHECK_INTERVAL
        );
        assert_eq!(
            next_interval(MAX_INTERVAL, Some(&lost), None),
            CHECK_INTERVAL
        );
    }

    #[test]
    fn the_index_waits_for_room() {
        assert_eq!(
            room_needed(1 << 30),
            16 << 30,
            "the floor holds for a small index"
        );
        assert_eq!(room_needed(7 << 30), 56 << 30, "eight times a large one");
        let (_dir, path) = index_of(1 << 16);
        let mut keeper = Keeper::new(&path);
        assert_eq!(
            keeper.keep_resident(Some((16 << 30) - 1)),
            Err(Skipped::NoRoom {
                available: (16 << 30) - 1,
                needed: 16 << 30
            })
        );
        assert!(keeper.keep_resident(Some(16 << 30)).is_ok(), "exactly room");
        assert_eq!(keeper.keep_resident(None), Err(Skipped::Unknown));
        std::fs::write(&path, b"").expect("an empty index");
        assert_eq!(
            Keeper::new(&path).keep_resident(Some(64 << 30)),
            Err(Skipped::Unknown)
        );
    }

    #[test]
    fn the_keeper_holds_its_descriptors_and_retires_replaced_ones() {
        let (dir, path) = index_of(1 << 16);
        let mut keeper = Keeper::new(&path);
        assert_eq!(keeper.tick(Some(64 << 30)), CHECK_INTERVAL);
        let first = keeper.held[0].as_ref().expect("the index is held").ino;
        keeper.tick(Some(64 << 30));
        assert_eq!(keeper.held[0].as_ref().expect("still held").ino, first);
        assert!(
            keeper.retired.is_empty(),
            "an unchanged file keeps its descriptor"
        );
        let next = dir.path().join("next.db");
        std::fs::write(&next, vec![9u8; 1 << 16]).expect("a new index");
        std::fs::rename(&next, &path).expect("it replaces the old one");
        keeper.tick(Some(64 << 30));
        let now = keeper.held[0].as_ref().expect("the new index is held").ino;
        assert_ne!(now, first);
        assert_eq!(now, std::fs::metadata(&path).expect("the index").ino());
        assert_eq!(keeper.retired.len(), 1, "the old descriptor is kept open");
        let wal = index_files(&path)[1].clone();
        std::fs::write(&wal, vec![1u8; 1 << 14]).expect("a log");
        keeper.tick(Some(64 << 30));
        let log = keeper.held[1].as_ref().expect("the log is held").ino;
        std::fs::write(&next, vec![2u8; 1 << 14]).expect("a new log");
        std::fs::rename(&next, &wal).expect("it replaces the old one");
        keeper.tick(Some(64 << 30));
        assert_ne!(
            keeper.held[1].as_ref().expect("the new log is held").ino,
            log
        );
        assert_eq!(
            keeper.retired.len(),
            1,
            "a replaced log's descriptor is closed"
        );
    }

    #[test]
    fn a_pass_looks_at_the_index_and_its_log() {
        let (_dir, path) = index_of(1 << 16);
        std::fs::write(index_files(&path)[1].clone(), vec![1u8; 1 << 14]).expect("its log");
        let pass = Keeper::new(&path)
            .keep_resident(Some(64 << 30))
            .expect("room enough");
        assert_eq!(pass.file_bytes, (1 << 16) + (1 << 14));
        assert!(pass.read_bytes <= pass.missing_bytes);
        assert!(pass.missing_bytes <= pass.file_bytes);
        let resident = pass.file_bytes - pass.missing_bytes;
        if reads_without_waiting(&path) {
            assert_eq!(
                pass.touched_bytes, resident,
                "every resident page is touched"
            );
        } else {
            assert_eq!(
                pass.touched_bytes, 0,
                "a file system that refuses touches nothing"
            );
        }
    }

    /// Whether `path`'s file system reads a cached page with `RWF_NOWAIT`
    /// (tmpfs, where a test's temporary directory may be, does not).
    fn reads_without_waiting(path: &Path) -> bool {
        let file = File::open(path).expect("the file opens");
        let mut byte = 0u8;
        let iov = libc::iovec {
            iov_base: (&raw mut byte).cast(),
            iov_len: 1,
        };
        // SAFETY: `iov` names the one writable byte `byte` for the call's
        // length, and the descriptor is `file`'s, open for the call.
        let read = unsafe { libc::preadv2(file.as_raw_fd(), &iov, 1, 0, libc::RWF_NOWAIT) };
        read == 1 || (read < 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EAGAIN))
    }

    #[test]
    fn evicted_pages_are_read_back() {
        let (_dir, path) = index_of(1 << 20);
        let file = File::open(&path).expect("the index opens");
        file.sync_all().expect("its pages are clean");
        // SAFETY: an advisory call on an open descriptor.
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        assert_eq!(rc, 0, "clean pages can be dropped");
        let before = missing_runs(&file, 1 << 20).expect("mincore answers");
        assert!(!before.is_empty(), "the dropped pages are missing");
        let pass = Keeper::new(&path)
            .keep_resident(Some(64 << 30))
            .expect("room enough");
        assert_eq!(
            pass.read_bytes, pass.missing_bytes,
            "all of it within the budget"
        );
        assert!(pass.read_bytes > 0);
        assert_eq!(
            missing_runs(&file, 1 << 20).expect("mincore answers"),
            vec![],
            "back in memory"
        );
    }

    #[test]
    fn the_switch_turns_it_off() {
        assert!(enabled(None));
        assert!(enabled(Some("1")));
        for off in ["0", "false", "OFF", " no "] {
            assert!(!enabled(Some(off)), "{off}");
        }
    }
}
