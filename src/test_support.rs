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
pub(crate) struct ProcessStateTestGuard {
    original_dir: Option<PathBuf>,
    _lock: MutexGuard<'static, ()>,
}

impl ProcessStateTestGuard {
    /// Make a fresh [`cwd_dir`] the current directory until the guard drops.
    pub(crate) fn enter(&mut self) -> PathBuf {
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
pub(crate) fn cwd_dir() -> PathBuf {
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
pub(crate) fn process_state_test_lock() -> ProcessStateTestGuard {
    let lock = PROCESS_STATE_TEST_LOCK
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    ProcessStateTestGuard {
        original_dir: std::env::current_dir().ok(),
        _lock: lock,
    }
}

/// kunobi-auth keeps the TOFU cross-process lock in
/// `dirs::config_dir()/kunobi/locks`. That directory follows `XDG_CONFIG_HOME`
/// on Linux and `HOME` on macOS, and the Nix sandbox sets `HOME` to a
/// directory the build cannot write. Hold one of these for any test that
/// calls `TofuStore::trust` or `check_and_pin`.
pub(crate) struct KunobiConfigGuard {
    _lock: ProcessStateTestGuard,
    /// Kept so the directory outlives the tests that pin a service into it.
    /// Read on Unix, where `dirs::config_dir` follows this process's `HOME`.
    #[cfg_attr(not(unix), allow(dead_code))]
    dir: tempfile::TempDir,
    saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
}

impl KunobiConfigGuard {
    pub(crate) fn new() -> Self {
        let lock = process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let saved = ["HOME", "XDG_CONFIG_HOME"]
            .into_iter()
            .map(|key| {
                let previous = std::env::var_os(key);
                unsafe { std::env::set_var(key, dir.path()) };
                (key, previous)
            })
            .collect();
        Self {
            _lock: lock,
            dir,
            saved,
        }
    }

    #[cfg(unix)]
    pub(crate) fn path(&self) -> &std::path::Path {
        self.dir.path()
    }
}

impl Drop for KunobiConfigGuard {
    fn drop(&mut self) {
        for (key, value) in self.saved.drain(..) {
            unsafe {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

/// A `Config` rooted in `cache_dir` with every optional feature off, for
/// tests that need a store without reading the developer's configuration.
/// Start `command` without a controlling terminal, so a child that shows
/// `[kache]` lines ([`crate::notice`]) writes them to the stderr the test
/// reads even when the suite runs in a developer's terminal.
pub(crate) fn without_terminal(command: &mut std::process::Command) -> &mut std::process::Command {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        // SAFETY: runs in the forked child before exec; `setsid` is
        // async-signal-safe and touches no memory of the parent.
        unsafe {
            command.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
    }
    command
}

pub(crate) fn test_config(cache_dir: PathBuf) -> crate::config::Config {
    crate::config::Config {
        fallback: None,
        key_salt: None,
        cc_extra_allowlist_flags: Vec::new(),
        local_only: false,
        remote_readonly: false,
        readonly_store: None,
        pull_request_prefix: None,
        modified_input_guard: false,
        input_predictions: false,
        record_sessions: false,
        volume_stores: Vec::new(),
        windows_hardlink: false,
        shared_hardlink_restores: false,
        deferred_discovery: true,
        out_dir_alias: true,
        deferred_durability: false,
        daemon_publish: true,
        project_rules: crate::config::ProjectRules::default(),
        auto_gc: true,
        index_auto_compact: true,
        auto_clean_orphaned_targets: false,
        auto_share_target_files: false,
        auto_clean_idle_targets_days: 0,
        auto_recover_min_free_bytes: 0,
        scheduler_memory_pressure: true,
        auto_clean_unused_units_days: 0,
        seed_new_targets: false,
        build_script_hermetic: false,
        gc_evict_shared: false,
        storage_layout_advice: true,
        heartbeat_secs: 30,
        explain_miss: false,
        scheduler: true,
        test_lease: None,
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
        trust_codegen_backends: false,
        target_liveness: false,
        clean_incremental: true,
        preserve_incremental: false,
        adaptive_incremental: true,
        event_log_max_size: 10 * 1024 * 1024,
        event_log_keep_lines: 1000,
        compression_level: 3,
        s3_concurrency: 16,
        prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
        remote_key_listing: false,
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

/// Run `command` to completion, retrying while its exec fails with ETXTBSY.
///
/// A test that writes an executable and then runs it races every fork on
/// another test thread: the forked child holds this process's write
/// descriptor until it execs, and exec of the file fails in that window
/// (kunobi-ninja/kache#673). The window is microseconds; an error that lasts
/// past the retries is real.
#[cfg(unix)]
pub(crate) fn output_retrying_etxtbsy(
    command: &mut std::process::Command,
) -> std::io::Result<std::process::Output> {
    for _ in 0..ETXTBSY_RETRIES {
        match command.output() {
            Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            result => return result,
        }
    }
    command.output()
}

#[cfg(unix)]
const ETXTBSY_RETRIES: usize = 50;

/// Wait until no write made so far to `paths`, or to any file under them,
/// can count as one made since a build that starts afterwards
/// ([`crate::cache_key::stamp_written_since`]). A stamp ahead of the clock
/// is no write and is not waited for. The unit-test side of
/// `tests/common`'s `settle_writes`.
pub(crate) fn settle_writes(paths: &[&std::path::Path]) {
    use crate::cache_key::{
        FINE_STAMP_WINDOW_NS, FileFingerprint, stamp_clock_ns, stamp_window_ns, wall_clock_ns,
    };
    fn collect(path: &std::path::Path, stamps: &mut Vec<i64>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).into_iter().flatten().flatten() {
                collect(&entry.path(), stamps);
            }
        } else if let Ok(stamp) = FileFingerprint::from_path(path) {
            stamps.extend([stamp.mtime_ns, stamp.ctime_ns]);
        }
    }
    let now = wall_clock_ns();
    let mut stamps = Vec::new();
    for path in paths {
        collect(path, &mut stamps);
    }
    let settled_at = stamps
        .into_iter()
        .filter(|&stamp| stamp <= now)
        .map(|stamp| stamp.saturating_add(stamp_window_ns(stamp)))
        .fold(
            stamp_clock_ns().saturating_add(FINE_STAMP_WINDOW_NS),
            i64::max,
        );
    while stamp_clock_ns() <= settled_at {
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::{cwd_dir, process_state_test_lock};

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
        let scratch = cwd_dir();

        {
            let _lock = process_state_test_lock();
            std::env::set_current_dir(&scratch).unwrap();
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

    #[test]
    fn an_entered_dir_outlives_the_guard() {
        let (original, entered) = {
            let mut lock = process_state_test_lock();
            let original = std::env::current_dir().unwrap();
            let entered = lock.enter();
            assert_eq!(
                std::env::current_dir().unwrap().canonicalize().unwrap(),
                entered.canonicalize().unwrap()
            );
            (original, entered)
        };

        let _lock = process_state_test_lock();
        assert_eq!(std::env::current_dir().unwrap(), original);
        // A child another test started in it may still be running.
        assert!(
            entered.is_dir(),
            "the entered dir stays until the binary exits"
        );
    }

    /// Linux refuses to exec a file that is open for writing; macOS does not,
    /// so only Linux can hold the error open on purpose.
    #[cfg(target_os = "linux")]
    fn busy_script(dir: &std::path::Path) -> (std::path::PathBuf, std::fs::File) {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt as _;
        let script = dir.join("busy.sh");
        let mut file = std::fs::File::create(&script).unwrap();
        file.write_all(b"#!/bin/sh\nexit 0\n").unwrap();
        file.set_permissions(std::fs::Permissions::from_mode(0o755))
            .unwrap();
        (script, file)
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_spawn_waits_out_a_writer_that_closes() {
        let dir = tempfile::tempdir().unwrap();
        let (script, writer) = busy_script(dir.path());
        let error = std::process::Command::new(&script).output().unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ETXTBSY));

        let closer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            drop(writer);
        });
        let output = super::output_retrying_etxtbsy(&mut std::process::Command::new(&script));
        closer.join().unwrap();
        assert!(output.unwrap().status.success());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_writer_that_stays_open_is_a_real_error() {
        let dir = tempfile::tempdir().unwrap();
        let (script, _writer) = busy_script(dir.path());
        let error =
            super::output_retrying_etxtbsy(&mut std::process::Command::new(&script)).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::ETXTBSY));
    }

    #[cfg(unix)]
    #[test]
    fn other_spawn_errors_are_not_retried() {
        let dir = tempfile::tempdir().unwrap();
        let started = std::time::Instant::now();
        let error = super::output_retrying_etxtbsy(&mut std::process::Command::new(
            dir.path().join("missing"),
        ))
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        // Retrying would sleep 10 ms per attempt, 500 ms in all.
        assert!(started.elapsed() < std::time::Duration::from_millis(250));
    }
}
