//! Targets used by programs launched without a Cargo or Kache lease.
//!
//! A cleanup pass takes one lazy snapshot. Linux reports executables, mapped
//! files and open descriptors; macOS reports executables. These observations
//! complement locks: they cannot prevent a new program starting after the scan.

use std::cell::OnceCell;
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const SCAN_LIMIT: Duration = Duration::from_secs(2);

#[derive(Default)]
pub(crate) struct ActivePrograms {
    paths: OnceCell<io::Result<BTreeSet<PathBuf>>>,
}

impl ActivePrograms {
    pub(crate) fn uses(&self, target: &Path) -> bool {
        self.uses_with(target, || scan(Instant::now() + SCAN_LIMIT))
    }

    fn uses_with(
        &self,
        target: &Path,
        scan: impl FnOnce() -> io::Result<BTreeSet<PathBuf>>,
    ) -> bool {
        let Ok(target) = std::fs::canonicalize(target) else {
            return false;
        };
        let paths = self.paths.get_or_init(|| {
            let result = scan();
            if let Err(error) = &result {
                tracing::debug!("keeping targets because the process scan did not finish: {error}");
            }
            result
        });
        // An unavailable process table or an expired scan keeps targets for
        // this pass. A partial snapshot must not authorize deletion.
        match paths {
            Ok(paths) => paths.iter().any(|path| path.starts_with(&target)),
            Err(_) => true,
        }
    }
}

fn check_deadline(now: Instant, deadline: Instant) -> io::Result<()> {
    if now >= deadline {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "process scan timed out",
        ))
    } else {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
fn scan(deadline: Instant) -> io::Result<BTreeSet<PathBuf>> {
    // SAFETY: geteuid has no arguments or failure case.
    scan_linux(Path::new("/proc"), unsafe { libc::geteuid() }, deadline)
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn scan_linux(root: &Path, uid: u32, deadline: Instant) -> io::Result<BTreeSet<PathBuf>> {
    use std::os::unix::fs::MetadataExt;
    let mut paths = BTreeSet::new();
    for entry in std::fs::read_dir(root)? {
        check_deadline(Instant::now(), deadline)?;
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
            .is_none()
        {
            continue;
        }
        let process = entry.path();
        if !std::fs::metadata(&process).is_ok_and(|meta| meta.uid() == uid) {
            continue;
        }
        // Processes and descriptors can disappear during the walk. A policy
        // that denies reading a particular process leaves it unobserved.
        if let Ok(path) = std::fs::read_link(process.join("exe")) {
            paths.insert(path);
        }
        if let Ok(file) = std::fs::File::open(process.join("maps")) {
            read_maps(std::io::BufReader::new(file), deadline, &mut paths)?;
        }
        if let Ok(entries) = std::fs::read_dir(process.join("fd")) {
            for descriptor in entries.flatten() {
                check_deadline(Instant::now(), deadline)?;
                if let Ok(path) = std::fs::read_link(descriptor.path()) {
                    paths.insert(path);
                }
            }
        }
    }
    check_deadline(Instant::now(), deadline)?;
    Ok(paths)
}

#[cfg(any(target_os = "linux", all(test, unix)))]
fn read_maps(
    mut reader: impl std::io::BufRead,
    deadline: Instant,
    paths: &mut BTreeSet<PathBuf>,
) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let mut line = Vec::new();
    loop {
        check_deadline(Instant::now(), deadline)?;
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        if let Some(start) = line.iter().position(|byte| *byte == b'/') {
            let raw = &line[start..];
            let raw = raw.strip_suffix(b"\n").unwrap_or(raw);
            paths.insert(PathBuf::from(std::ffi::OsStr::from_bytes(raw)));
            // proc maps escapes newlines as \012, ambiguously with a literal
            // backslash sequence. Keep both spellings, including non-UTF-8.
            let mut rest = raw;
            let mut decoded = Vec::new();
            while let Some(index) = rest.windows(4).position(|part| part == br"\012") {
                decoded.extend_from_slice(&rest[..index]);
                decoded.push(b'\n');
                rest = &rest[index + 4..];
            }
            decoded.extend_from_slice(rest);
            paths.insert(PathBuf::from(std::ffi::OsStr::from_bytes(&decoded)));
        }
    }
}

#[cfg(target_os = "macos")]
// The mutation lane runs on Linux. Native-process tests exercise this FFI
// collector on macOS; the snapshot policy and Linux walker stay in scope.
#[mutants::skip]
fn scan(deadline: Instant) -> io::Result<BTreeSet<PathBuf>> {
    use std::os::unix::ffi::OsStrExt;
    // PROC_UID_ONLY from <sys/proc_info.h> selects this effective user's
    // processes. proc_listpids returns the number of bytes, not the PID count.
    const PROC_UID_ONLY: u32 = 4;
    // SAFETY: geteuid has no arguments; a null buffer asks for the list size.
    let uid = unsafe { libc::geteuid() };
    let needed = unsafe { libc::proc_listpids(PROC_UID_ONLY, uid, std::ptr::null_mut(), 0) };
    if needed <= 0 {
        return Err(io::Error::last_os_error());
    }
    let mut pids = vec![0 as libc::pid_t; needed as usize / size_of::<libc::pid_t>() + 256];
    let capacity = i32::try_from(std::mem::size_of_val(pids.as_slice()))
        .map_err(|_| io::Error::other("process list too large"))?;
    // SAFETY: the vector is writable for capacity bytes.
    let written =
        unsafe { libc::proc_listpids(PROC_UID_ONLY, uid, pids.as_mut_ptr().cast(), capacity) };
    if written <= 0 || written >= capacity {
        return Err(io::Error::other("process list unavailable or truncated"));
    }
    pids.truncate(written as usize / size_of::<libc::pid_t>());
    let mut paths = BTreeSet::new();
    let mut buffer = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    for pid in pids {
        check_deadline(Instant::now(), deadline)?;
        // SAFETY: buffer has the size passed to proc_pidpath.
        let length =
            unsafe { libc::proc_pidpath(pid, buffer.as_mut_ptr().cast(), buffer.len() as u32) };
        if length > 0 {
            paths.insert(PathBuf::from(std::ffi::OsStr::from_bytes(
                &buffer[..length as usize],
            )));
        }
    }
    check_deadline(Instant::now(), deadline)?;
    Ok(paths)
}

// Windows prevents removal of running executables and open files. Other
// platforms retain their existing Cargo-lock protection.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
// This constant fallback is never compiled by the Linux mutation lane.
#[mutants::skip]
fn scan(deadline: Instant) -> io::Result<BTreeSet<PathBuf>> {
    check_deadline(Instant::now(), deadline)?;
    Ok(BTreeSet::new())
}

#[cfg(test)]
#[path = "target_processes_tests.rs"]
pub(crate) mod tests;
