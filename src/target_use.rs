//! Process-held lock that keeps target cleanup out of running Cargo commands.

use std::ffi::OsStr;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Unlock explicitly before closing: a concurrent fork can briefly inherit
/// the descriptor, even though it closes on exec. A shared lease also leaves
/// a record naming its command, so cleanup can say what it waits for.
pub(crate) struct Lease {
    file: File,
    record: Option<PathBuf>,
}

impl Lease {
    fn new(file: File) -> Self {
        Self { file, record: None }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(record) = &self.record {
            let _ = std::fs::remove_file(record);
        }
        let _ = self.file.unlock();
    }
}

/// A running command that holds a shared lease, from its record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Holder {
    pub(crate) pid: u32,
    /// What the command is, such as `cargo run` or a test binary's name.
    pub(crate) what: String,
    /// When it took the lease, in seconds since the epoch.
    pub(crate) since: u64,
}

/// The exclusive lease cleanup holds. While it is held, a file in the cache
/// directory names this process to the commands waiting for it. The file goes
/// before the lock does, so it never takes the next holder's with it.
pub(crate) struct Reservation {
    holder: PathBuf,
    _lease: Lease,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.holder);
    }
}

fn open(cache_dir: &Path) -> io::Result<File> {
    std::fs::create_dir_all(cache_dir)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(cache_dir.join("target-use.lock"))
}

fn holder_record(cache_dir: &Path) -> PathBuf {
    cache_dir.join("target-use.holder")
}

/// One file per shared lease, named by its process and the time it was taken.
fn holders_dir(cache_dir: &Path) -> PathBuf {
    cache_dir.join("target-use.holders")
}

fn record_holder(cache_dir: &Path, what: &str) -> Option<PathBuf> {
    let dir = holders_dir(cache_dir);
    std::fs::create_dir_all(&dir).ok()?;
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let pid = std::process::id();
    let record = dir.join(format!("{pid}-{}", since.as_nanos()));
    // A tab cannot appear in the time, so `what` keeps any characters.
    std::fs::write(&record, format!("{}\t{what}", since.as_secs())).ok()?;
    Some(record)
}

fn parse_holder(name: &str, text: &str) -> Option<Holder> {
    let pid = name.split('-').next()?.parse().ok()?;
    let (since, what) = text.split_once('\t')?;
    Some(Holder {
        pid,
        what: what.trim().to_owned(),
        since: since.parse().ok()?,
    })
}

/// The commands holding shared leases now, oldest first. A record left by a
/// process that is gone is removed.
pub(crate) fn holders(cache_dir: &Path) -> Vec<Holder> {
    let Ok(entries) = std::fs::read_dir(holders_dir(cache_dir)) else {
        return Vec::new();
    };
    let mut holders = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(holder) = std::fs::read_to_string(entry.path())
            .ok()
            .and_then(|text| parse_holder(&name, &text))
        else {
            continue;
        };
        if crate::daemon::process_is_alive(holder.pid) {
            holders.push(holder);
        } else {
            let _ = std::fs::remove_file(entry.path());
        }
    }
    holders.sort_by_key(|holder| (holder.since, holder.pid));
    holders
}

/// Who keeps cleanup from deleting now: `None` when nothing does, otherwise
/// the commands holding shared leases (empty when none left a record, such
/// as another cleanup or an older Kache). The probe takes the lock for an
/// instant and records nothing.
pub(crate) fn blockers(cache_dir: &Path) -> Option<Vec<Holder>> {
    let file = open(cache_dir).ok()?;
    match file.try_lock() {
        Ok(()) => {
            let _ = file.unlock();
            None
        }
        Err(_) => Some(holders(cache_dir)),
    }
}

/// Hold while Cargo or a target runner may still use build output. While
/// cleanup holds the lock, `notice` gets one line saying so before the wait.
/// `what` names the command to a cleanup that has to skip targets for it.
pub(crate) fn shared(cache_dir: &Path, what: &str, notice: &mut dyn Write) -> io::Result<Lease> {
    let file = open(cache_dir)?;
    match file.try_lock_shared() {
        Ok(()) => {}
        // One write, so the line stays whole beside other processes' output.
        Err(TryLockError::WouldBlock) => {
            let _ = notice.write_all(waiting(cache_dir).as_bytes());
            file.lock_shared()?;
        }
        Err(TryLockError::Error(error)) => return Err(error),
    }
    let mut lease = Lease::new(file);
    lease.record = record_holder(cache_dir, what);
    Ok(lease)
}

/// [`shared`] for a command started with `args`. The line goes to stderr
/// unless a `-q` or `--quiet` before any `--` asked Cargo to be quiet: Cargo
/// then hides its own lock waits, and passes a test binary `--quiet`.
pub(crate) fn shared_for<'a>(
    cache_dir: &Path,
    what: &str,
    args: impl IntoIterator<Item = &'a OsStr>,
) -> io::Result<Lease> {
    if quiet(args) {
        shared(cache_dir, what, &mut io::sink())
    } else {
        shared(cache_dir, what, &mut io::stderr())
    }
}

fn quiet<'a>(args: impl IntoIterator<Item = &'a OsStr>) -> bool {
    args.into_iter()
        .take_while(|arg| *arg != "--")
        .any(|arg| arg == "-q" || arg == "--quiet")
}

/// What a command waiting for cleanup says, with the cleanup's process when
/// it left a record.
fn waiting(cache_dir: &Path) -> String {
    let pid = std::fs::read_to_string(holder_record(cache_dir))
        .ok()
        .and_then(|text| text.trim().parse::<u32>().ok());
    match pid {
        Some(pid) => format!("kache: waiting for target cleanup (pid {pid}) to finish\n"),
        None => "kache: waiting for target cleanup to finish\n".to_owned(),
    }
}

/// Reserve deletion without waiting for a running command.
pub(crate) fn try_exclusive(cache_dir: &Path) -> io::Result<Option<Reservation>> {
    let file = open(cache_dir)?;
    match file.try_lock() {
        Ok(()) => {
            let lease = Lease::new(file);
            let holder = holder_record(cache_dir);
            let _ = std::fs::write(&holder, std::process::id().to_string());
            Ok(Some(Reservation {
                holder,
                _lease: lease,
            }))
        }
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Hands each write to the test as it happens.
    struct Said(std::sync::mpsc::Sender<Vec<u8>>);

    impl Write for Said {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let _ = self.0.send(bytes.to_vec());
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn naming(pid: u32) -> String {
        format!("kache: waiting for target cleanup (pid {pid}) to finish\n")
    }

    #[test]
    fn a_shared_lease_names_its_command_until_it_ends() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(blockers(dir.path()), None);
        let run = shared(dir.path(), "cargo run", &mut io::sink()).unwrap();
        let named = holders(dir.path());
        assert_eq!(named.len(), 1);
        assert_eq!(
            (named[0].pid, named[0].what.as_str()),
            (std::process::id(), "cargo run")
        );
        assert_eq!(blockers(dir.path()), Some(named));
        drop(run);
        assert!(holders(dir.path()).is_empty());
        assert_eq!(blockers(dir.path()), None);
        // The probe leaves no record of its own behind.
        assert!(try_exclusive(dir.path()).unwrap().is_some());
    }

    #[test]
    fn a_record_left_by_a_gone_process_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let records = holders_dir(dir.path());
        std::fs::create_dir_all(&records).unwrap();
        // PID 1 never names a lease holder, and garbage parses as nothing.
        std::fs::write(records.join("1-5"), "5\tcargo run").unwrap();
        std::fs::write(records.join("garbage"), "no tab").unwrap();
        assert!(holders(dir.path()).is_empty());
        assert!(!records.join("1-5").exists());
        assert_eq!(
            parse_holder("42-9", "17\tcargo test --all"),
            Some(Holder {
                pid: 42,
                what: "cargo test --all".to_owned(),
                since: 17
            })
        );
        assert_eq!(parse_holder("x-9", "17\tcargo"), None);
    }

    #[test]
    fn a_running_command_blocks_cleanup_until_it_exits() {
        let dir = tempfile::tempdir().unwrap();
        let first = shared(dir.path(), "cargo build", &mut io::sink()).unwrap();
        let second = shared(dir.path(), "cargo build", &mut io::sink()).unwrap();
        assert!(try_exclusive(dir.path()).unwrap().is_none());
        drop(first);
        assert!(try_exclusive(dir.path()).unwrap().is_none());
        drop(second);
        assert!(try_exclusive(dir.path()).unwrap().is_some());
    }

    #[test]
    fn a_command_says_it_waits_for_cleanup_then_runs_once_cleanup_ends() {
        let dir = tempfile::tempdir().unwrap();
        let cleanup = try_exclusive(dir.path()).unwrap().unwrap();
        let (said, heard) = std::sync::mpsc::channel();
        let cache = dir.path().to_path_buf();
        let command =
            std::thread::spawn(move || shared(&cache, "cargo build", &mut Said(said)).unwrap());
        let line = heard
            .recv_timeout(Duration::from_secs(10))
            .expect("the command never said it was waiting");
        assert_eq!(String::from_utf8(line).unwrap(), naming(std::process::id()));
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !command.is_finished(),
            "the command started while cleanup held the lock"
        );
        drop(cleanup);
        let lease = command.join().unwrap();
        assert!(
            try_exclusive(dir.path()).unwrap().is_none(),
            "the command runs without its lease"
        );
        drop(lease);
        assert!(heard.try_recv().is_err(), "more than one line");
    }

    #[test]
    fn a_command_takes_a_free_lock_without_a_word() {
        let dir = tempfile::tempdir().unwrap();
        let mut said = Vec::new();
        let first = shared(dir.path(), "cargo build", &mut said).unwrap();
        let second = shared(dir.path(), "cargo build", &mut said).unwrap();
        assert!(said.is_empty(), "{}", String::from_utf8_lossy(&said));
        drop((first, second));
    }

    #[test]
    fn the_wait_names_the_cleanup_process_only_while_it_holds_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let anonymous = "kache: waiting for target cleanup to finish\n";
        assert_eq!(waiting(dir.path()), anonymous);
        let cleanup = try_exclusive(dir.path()).unwrap().unwrap();
        assert_eq!(waiting(dir.path()), naming(std::process::id()));
        drop(cleanup);
        assert_eq!(waiting(dir.path()), anonymous);
        std::fs::write(holder_record(dir.path()), "not a pid").unwrap();
        assert_eq!(waiting(dir.path()), anonymous);
    }

    #[test]
    fn quiet_cargo_hides_the_wait() {
        let is_quiet = |args: &[&str]| quiet(args.iter().map(OsStr::new));
        assert!(!is_quiet(&["build"]));
        assert!(is_quiet(&["build", "-q"]));
        assert!(is_quiet(&["--quiet", "test"]));
        // What follows `--` belongs to the program Cargo runs.
        assert!(!is_quiet(&["run", "--", "-q"]));
    }

    #[cfg(unix)]
    #[test]
    fn closing_a_lease_releases_an_inherited_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let lease = shared(dir.path(), "cargo build", &mut io::sink()).unwrap();
        let inherited = lease.file.try_clone().unwrap();
        drop(lease);
        assert!(try_exclusive(dir.path()).unwrap().is_some());
        drop(inherited);
    }
}
