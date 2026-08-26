use std::sync::{Mutex, MutexGuard};

/// Serialize unit tests that observe or mutate process-global state such as
/// the current directory or environment variables.
static PROCESS_STATE_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Keep a poisoned lock usable so one failing test does not cascade into
/// unrelated failures.
pub(crate) fn process_state_test_lock() -> MutexGuard<'static, ()> {
    PROCESS_STATE_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}
