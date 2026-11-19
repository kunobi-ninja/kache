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

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

/// How often the daemon looks.
pub(crate) const CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

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
    /// Of those, bytes not in the page cache.
    pub(crate) missing_bytes: u64,
    /// Bytes read back (at most [`PASS_BUDGET_BYTES`]).
    pub(crate) read_bytes: u64,
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

/// One pass over `files`, if `available` memory leaves room for them.
pub(crate) fn keep_resident(files: &[PathBuf], available: Option<u64>) -> Result<Pass, Skipped> {
    if !enabled(std::env::var("KACHE_KEEP_INDEX_RESIDENT").ok().as_deref()) {
        return Err(Skipped::Off);
    }
    let sizes: Vec<(File, u64)> = files
        .iter()
        .filter_map(|path| {
            let file = File::open(path).ok()?;
            let len = file.metadata().ok()?.len();
            Some((file, len))
        })
        .collect();
    let total: u64 = sizes.iter().map(|(_, len)| len).sum();
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
    for (file, len) in &sizes {
        pass.file_bytes += len;
        let Ok(runs) = missing_runs(file, *len) else {
            continue;
        };
        for (offset, run) in merge(&runs, MERGE_GAP_BYTES) {
            pass.missing_bytes += run;
            if budget == 0 {
                continue;
            }
            let ask = run.min(budget);
            // Twice: the second read moves the pages to the active list.
            if read_back(file, offset, ask).is_ok() && read_back(file, offset, ask).is_ok() {
                pass.read_bytes += ask;
            }
            budget -= ask;
        }
    }
    Ok(pass)
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
/// lower priority, one pass every [`CHECK_INTERVAL`] for the daemon's life.
pub(crate) fn spawn(db_path: &Path) {
    let files = index_files(db_path);
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
            loop {
                std::thread::sleep(CHECK_INTERVAL);
                let available = std::fs::read_to_string("/proc/meminfo")
                    .ok()
                    .and_then(|text| mem_available(&text));
                if let Ok(pass) = keep_resident(&files, available)
                    && worth_a_line(&pass)
                {
                    tracing::info!(
                        index_mib = pass.file_bytes >> 20,
                        missing_mib = pass.missing_bytes >> 20,
                        read_mib = pass.read_bytes >> 20,
                        "index pages read back into memory"
                    );
                }
            }
        });
    if let Err(error) = started {
        tracing::warn!("could not start the index keeper: {error}");
    }
}

/// Runs of `file`'s pages not in the page cache, as (offset, length) in
/// bytes. Reads none of them.
fn missing_runs(file: &File, len: u64) -> io::Result<Vec<(u64, u64)>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: sysconf has no preconditions.
    let page = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
        .ok()
        .filter(|page| *page > 0)
        .unwrap_or(4096);
    let map_len = usize::try_from(len).map_err(|_| io::Error::other("index too large to map"))?;
    // SAFETY: a fresh read-only shared mapping of an open file the caller
    // owns for the call; nothing reads through it, so a concurrent writer
    // or truncation cannot fault here, and it is unmapped below.
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
    let result = if rc == 0 {
        Ok(runs_of(&vec, page))
    } else {
        Err(io::Error::last_os_error())
    };
    // SAFETY: the mapping made above, unmapped once; nothing refers to it.
    unsafe {
        libc::munmap(addr, map_len);
    }
    result
}

/// The (offset, length) runs of pages whose resident bit is clear.
fn runs_of(vec: &[u8], page: u64) -> Vec<(u64, u64)> {
    let mut runs = Vec::new();
    let mut start = None;
    for (index, flags) in vec.iter().enumerate() {
        let index = index as u64;
        match (flags & 1 == 0, start) {
            (true, None) => start = Some(index),
            (false, Some(first)) => {
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

/// Read `len` bytes of `file` from `offset` and throw them away: the read
/// puts the pages in the page cache and marks them used.
fn read_back(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    let mut buffer = vec![0u8; 1 << 20];
    let mut at = offset;
    let end = offset.saturating_add(len);
    while at < end {
        let want = usize::try_from((end - at).min(buffer.len() as u64)).unwrap_or(buffer.len());
        match file.read_at(&mut buffer[..want], at) {
            Ok(0) => break,
            Ok(read) => at += read as u64,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn missing_pages_come_out_as_runs() {
        let page = 4096;
        assert_eq!(runs_of(&[1, 1, 1], page), vec![]);
        assert_eq!(
            runs_of(&[0, 0, 1, 0, 1, 1, 0], page),
            vec![(0, 2 * page), (3 * page, page), (6 * page, page)]
        );
        // Only the resident bit counts; the kernel may set others.
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
    }

    #[test]
    fn the_index_waits_for_room() {
        assert_eq!(
            room_needed(1 << 30),
            16 << 30,
            "the floor holds for a small index"
        );
        assert_eq!(room_needed(7 << 30), 56 << 30, "eight times a large one");
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("index.db");
        std::fs::write(&path, vec![7u8; 1 << 16]).expect("a small index");
        let files = index_files(&path);
        assert_eq!(
            keep_resident(&files, Some(1 << 30)),
            Err(Skipped::NoRoom {
                available: 1 << 30,
                needed: 16 << 30
            })
        );
        assert_eq!(keep_resident(&files, None), Err(Skipped::Unknown));
    }

    #[test]
    fn a_pass_looks_at_the_index_and_its_log() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("index.db");
        std::fs::write(&path, vec![7u8; 1 << 16]).expect("a small index");
        std::fs::write(index_files(&path)[1].clone(), vec![1u8; 1 << 14]).expect("its log");
        let pass = keep_resident(&index_files(&path), Some(64 << 30)).expect("room enough");
        assert_eq!(pass.file_bytes, (1 << 16) + (1 << 14));
        assert!(pass.read_bytes <= pass.missing_bytes);
        assert!(pass.missing_bytes <= pass.file_bytes);
    }

    #[test]
    fn evicted_pages_are_read_back() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("index.db");
        std::fs::write(&path, vec![7u8; 1 << 20]).expect("an index");
        let file = File::open(&path).expect("the index opens");
        file.sync_all().expect("its pages are clean");
        // SAFETY: an advisory call on an open descriptor.
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        assert_eq!(rc, 0, "clean pages can be dropped");
        let before = missing_runs(&file, 1 << 20).expect("mincore answers");
        assert!(!before.is_empty(), "the dropped pages are missing");
        let pass = keep_resident(&index_files(&path), Some(64 << 30)).expect("room enough");
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
