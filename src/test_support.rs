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

/// A `Config` rooted in `cache_dir` with every optional feature off, for
/// tests that need a store without reading the developer's configuration.
pub(crate) fn test_config(cache_dir: PathBuf) -> crate::config::Config {
    crate::config::Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        local_hit_daemon: false,
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        auto_gc: true,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        path_only_env_vars: Vec::new(),
        incremental_crates: Vec::new(),
        key_env_vars: Vec::new(),
        base_dirs: Vec::new(),
        runtime_dir: cache_dir.clone(),
        cache_dir,
        max_size: 1024 * 1024,
        remote: None,
        remote_error: None,
        socket_path_override: None,
        disabled: false,
        cache_executables: false,
        cache_cc_links: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
        prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
        prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
        prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
        min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
        gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
        daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
        s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
        remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
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
