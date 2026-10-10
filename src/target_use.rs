//! Process-held lock that keeps target cleanup out of running Cargo commands.

use std::ffi::OsStr;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Unlock explicitly before closing: a concurrent fork can briefly inherit
/// the descriptor, even though it closes on exec.
pub(crate) struct Lease(File);

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
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

/// Hold while Cargo or a target runner may still use build output. While
/// cleanup holds the lock, `notice` gets one line saying so before the wait.
pub(crate) fn shared(cache_dir: &Path, notice: &mut dyn Write) -> io::Result<Lease> {
    let file = open(cache_dir)?;
    match file.try_lock_shared() {
        Ok(()) => return Ok(Lease(file)),
        // One write, so the line stays whole beside other processes' output.
        Err(TryLockError::WouldBlock) => {
            let _ = notice.write_all(waiting(cache_dir).as_bytes());
        }
        Err(TryLockError::Error(error)) => return Err(error),
    }
    file.lock_shared()?;
    Ok(Lease(file))
}

/// [`shared`] for a command started with `args`. The line goes to stderr
/// unless a `-q` or `--quiet` before any `--` asked Cargo to be quiet: Cargo
/// then hides its own lock waits, and passes a test binary `--quiet`.
pub(crate) fn shared_for<'a>(
    cache_dir: &Path,
    args: impl IntoIterator<Item = &'a OsStr>,
) -> io::Result<Lease> {
    if quiet(args) {
        shared(cache_dir, &mut io::sink())
    } else {
        shared(cache_dir, &mut io::stderr())
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
            let lease = Lease(file);
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
    fn a_running_command_blocks_cleanup_until_it_exits() {
        let dir = tempfile::tempdir().unwrap();
        let first = shared(dir.path(), &mut io::sink()).unwrap();
        let second = shared(dir.path(), &mut io::sink()).unwrap();
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
        let command = std::thread::spawn(move || shared(&cache, &mut Said(said)).unwrap());
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
        let first = shared(dir.path(), &mut said).unwrap();
        let second = shared(dir.path(), &mut said).unwrap();
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
        let lease = shared(dir.path(), &mut io::sink()).unwrap();
        let inherited = lease.0.try_clone().unwrap();
        drop(lease);
        assert!(try_exclusive(dir.path()).unwrap().is_some());
        drop(inherited);
    }
}
