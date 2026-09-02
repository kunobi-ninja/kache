use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

/// Serialize unit tests that observe or mutate process-global state such as
/// the current directory or environment variables.
static PROCESS_STATE_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Holds the process-state lock and puts the current directory back where it
/// was once the guard drops.
///
/// A test that moves into a `TempDir` and then panics never reaches its own
/// restore line, and unwinding deletes the directory the process is standing
/// in. From then on every `std::env::current_dir()` in the binary goes through
/// libc's `getcwd` fallback, which climbs `..` and reads each parent directory
/// to recover the name; under a `$TMPDIR` full of test scratch that costs
/// seconds per call, and the rest of the run crawls. The mutation lane saw
/// this as a mutant the key tests do observe scoring TIMEOUT instead of
/// caught, because the failing test was one of the two that change directory.
pub(crate) struct ProcessStateTestGuard {
    original_dir: Option<PathBuf>,
    _lock: MutexGuard<'static, ()>,
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

/// Keep a poisoned lock usable so one failing test does not cascade into
/// unrelated failures.
pub(crate) fn process_state_test_lock() -> ProcessStateTestGuard {
    let lock = PROCESS_STATE_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    ProcessStateTestGuard {
        original_dir: std::env::current_dir().ok(),
        _lock: lock,
    }
}

#[cfg(test)]
mod tests {
    use super::process_state_test_lock;

    #[test]
    fn dropping_the_guard_restores_the_current_directory() {
        // Read the baseline under the lock: every holder restores the
        // directory on drop, so under the lock it is always the process's
        // original one, whereas an unlocked read could observe another test
        // mid-change.
        let original = {
            let _lock = process_state_test_lock();
            std::env::current_dir().unwrap()
        };
        let scratch = tempfile::tempdir().unwrap();

        {
            let _lock = process_state_test_lock();
            std::env::set_current_dir(scratch.path()).unwrap();
            assert_ne!(std::env::current_dir().unwrap(), original);
        }

        // Re-take the lock before looking: every holder restores the directory
        // on drop, so under the lock it can only be the original one. Reading
        // it unlocked would race another test that is mid-change.
        let _lock = process_state_test_lock();
        assert_eq!(
            std::env::current_dir().unwrap(),
            original,
            "the guard must restore the directory even when the test body never does"
        );
    }
}
