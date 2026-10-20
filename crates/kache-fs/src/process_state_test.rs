use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

/// Serialize unit tests that observe or mutate process-global state such as
/// the current directory or environment variables.
static PROCESS_STATE_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Holds the process-state lock and puts the current directory back where it
/// was once the guard drops.
///
/// A test that moves into a directory and then panics never reaches its own
/// restore line. The guard restores the directory for it: otherwise the next
/// `std::env::current_dir()` in the binary goes through libc's `getcwd`
/// fallback, which climbs `..` and reads each parent directory to recover the
/// name; under a `$TMPDIR` full of test scratch that costs seconds per call,
/// and the rest of the run crawls. The mutation lane saw this as a mutant the
/// key tests do observe scoring TIMEOUT instead of caught, because the failing
/// test was one of the two that change directory.
pub struct ProcessStateTestGuard {
    original_dir: Option<PathBuf>,
    _lock: MutexGuard<'static, ()>,
}

impl ProcessStateTestGuard {
    /// Make a fresh [`cwd_dir`] the current directory until the guard drops.
    pub fn enter(&mut self) -> PathBuf {
        let path = cwd_dir();
        std::env::set_current_dir(&path).unwrap();
        path
    }
}

impl Drop for ProcessStateTestGuard {
    fn drop(&mut self) {
        // Runs before the lock field drops, so the directory is back before
        // the next test can take the lock. Best effort: a panic here while
        // already unwinding would abort the whole test binary.
        if let Some(dir) = self.original_dir.take() {
            let _ = std::env::set_current_dir(dir);
        }
    }
}

/// Directories tests made the current directory, kept until the binary exits.
///
/// A child process inherits the current directory when it starts. A test that
/// spawns rustc without the lock can therefore start it inside another test's
/// directory, and rustc can still be starting when that test finishes. rustc
/// panics when its working directory no longer exists ("expecting a current
/// working directory to exist"), so deleting the directory at the end of the
/// test failed unrelated dep-info tests at random.
static CWD_DIRS: Mutex<Vec<tempfile::TempDir>> = Mutex::new(Vec::new());

/// A new directory a test may make the current directory. It stays on disk
/// until the test binary exits; see [`CWD_DIRS`].
pub fn cwd_dir() -> PathBuf {
    static REMOVE_AT_EXIT: std::sync::Once = std::sync::Once::new();
    REMOVE_AT_EXIT.call_once(|| {
        // SAFETY: registers a plain function that captures nothing. Exiting
        // runs it after the harness has joined every test, so no child a test
        // spawned is still running.
        unsafe { libc::atexit(remove_cwd_dirs) };
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    CWD_DIRS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .push(dir);
    path
}

extern "C" fn remove_cwd_dirs() {
    // Unwinding out of an `extern "C"` function aborts, so skip a poisoned
    // lock rather than unwrap it.
    if let Ok(mut dirs) = CWD_DIRS.lock() {
        dirs.clear();
    }
}

/// Keep a poisoned lock usable so one failing test does not cascade into
/// unrelated failures.
pub fn process_state_test_lock() -> ProcessStateTestGuard {
    let lock = PROCESS_STATE_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    ProcessStateTestGuard {
        original_dir: std::env::current_dir().ok(),
        _lock: lock,
    }
}
