//! Shared kache daemon lifecycle helpers for scenario runners.

use std::path::Path;
use std::process::{Command, Stdio};

/// Best-effort: stop any kache daemon bound to `cache_dir`.
///
/// Errors are swallowed — `daemon stop` failing because nothing is running is
/// normal. Call this before removing temp cache directories or resetting a
/// benchmark cache.
pub fn stop(kache_path: &Path, cache_dir: &Path) {
    let _ = Command::new(kache_path)
        .arg("daemon")
        .arg("stop")
        .env("KACHE_CACHE_DIR", cache_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Stop the daemon and wait for its lifetime lock to be released, after
/// accepted publications have drained. Named pipes have no socket file on
/// Windows, so socket existence cannot establish that shutdown finished.
pub fn stop_and_wait(kache_path: &Path, cache_dir: &Path, timeout: std::time::Duration) -> bool {
    stop(kache_path, cache_dir);
    wait_for_run_lock(cache_dir, timeout)
}

fn wait_for_run_lock(cache_dir: &Path, timeout: std::time::Duration) -> bool {
    let lock = match std::fs::OpenOptions::new()
        .write(true)
        .open(cache_dir.join("daemon.run.lock"))
    {
        Ok(file) => file,
        Err(error) => return error.kind() == std::io::ErrorKind::NotFound,
    };
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match lock.try_lock() {
            Ok(()) => return true, // Dropping the file releases our probe lock.
            Err(std::fs::TryLockError::WouldBlock) => {}
            Err(std::fs::TryLockError::Error(_)) => return false,
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// How long a daemon may take to finish its queued publications and exit.
/// The daemon keeps publishing while it makes progress, and a large build
/// (Firefox) can end with minutes of queued work, so this only guards
/// against a daemon that never exits.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15 * 60);

/// A benchmark must not read or replace a cache with queued publications.
pub fn drain(kache_path: &Path, cache_dir: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        stop_and_wait(kache_path, cache_dir, DRAIN_TIMEOUT),
        "daemon did not stop after the build: {}",
        cache_dir.display()
    );
    Ok(())
}

/// Start the daemon a fixture asks for. The runner sets no configuration,
/// so an empty file keeps `KACHE_CONFIG` from reaching the user's own; the
/// daemon then reads what the wrappers see.
pub fn start_for_fixture(kache_path: &Path, cache_dir: &Path) -> anyhow::Result<()> {
    let kache_config = fixture_config(cache_dir);
    std::fs::write(&kache_config, "")
        .map_err(|error| anyhow::anyhow!("writing the daemon's empty config: {error}"))?;
    anyhow::ensure!(
        start(kache_path, cache_dir, &kache_config),
        "the fixture requires a daemon and `kache daemon start` failed"
    );
    Ok(())
}

/// Work the wrappers handed to the daemon is published after they return.
/// Drain it (shutdown finishes the queue) and start a fresh daemon for the
/// next phase, which also proves each phase finds the previous one's entries
/// without the daemon that wrote them.
pub fn restart_after_build(kache_path: &Path, cache_dir: &Path) -> anyhow::Result<()> {
    drain(kache_path, cache_dir)?;
    anyhow::ensure!(
        start(kache_path, cache_dir, &fixture_config(cache_dir)),
        "restarting the daemon failed"
    );
    Ok(())
}

fn fixture_config(cache_dir: &Path) -> std::path::PathBuf {
    cache_dir.join("kache-config.toml")
}

/// Best-effort: start a kache daemon bound to `cache_dir`.
///
/// The wrapper falls back to no-daemon mode if this fails, so callers should
/// warn but not fail the scenario. Returns whether `kache daemon start`
/// succeeded, so a bench can record that its phase ran without one.
pub fn start(kache_path: &Path, cache_dir: &Path, kache_config: &Path) -> bool {
    match Command::new(kache_path)
        .args(["daemon", "start"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", kache_config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => {
            eprintln!("[bench] daemon started");
            true
        }
        Ok(s) => {
            eprintln!("[bench] WARN: daemon start exited {s} (running w/o daemon)");
            false
        }
        Err(e) => {
            eprintln!("[bench] WARN: daemon start failed ({e}) (running w/o daemon)");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn run_lock_child() {
        let Some(dir) = std::env::var_os("KACHE_E2E_LOCK_TEST_DIR") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let lock = std::fs::File::create(dir.join("daemon.run.lock")).unwrap();
        lock.lock().unwrap();
        std::fs::write(dir.join("ready"), "ready").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !dir.join("release").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(lock);
    }

    #[test]
    fn drain_waits_for_the_process_even_without_a_socket_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(wait_for_run_lock(dir.path(), Duration::ZERO));
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "daemon::tests::run_lock_child"])
            .env("KACHE_E2E_LOCK_TEST_DIR", dir.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !dir.path().join("ready").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let ready = dir.path().join("ready").exists();
        let while_held = wait_for_run_lock(dir.path(), Duration::from_millis(25));
        std::fs::write(dir.path().join("release"), "release").unwrap();
        assert!(child.wait().unwrap().success());
        assert!(ready, "child acquired the lifetime lock");
        assert!(
            !while_held,
            "a running daemon must block the cache snapshot"
        );
        // Tests in this binary spawn processes, and a fork taken while a
        // probe holds the lock keeps it until that child execs, so these
        // probes may wait a moment. One that never released its lock still
        // fails.
        assert!(wait_for_run_lock(dir.path(), Duration::from_secs(5)));
        // The probe must release its own lock too.
        assert!(wait_for_run_lock(dir.path(), Duration::from_secs(5)));
    }

    /// A lock released while the drain waits ends the wait with success.
    #[test]
    fn a_lock_released_within_the_timeout_ends_the_wait() {
        let dir = tempfile::tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "daemon::tests::run_lock_child"])
            .env("KACHE_E2E_LOCK_TEST_DIR", dir.path())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while !dir.path().join("ready").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(dir.path().join("ready").exists(), "child took the lock");
        let release = dir.path().join("release");
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            std::fs::write(release, "release").unwrap();
        });
        assert!(wait_for_run_lock(dir.path(), Duration::from_secs(10)));
        releaser.join().unwrap();
        assert!(child.wait().unwrap().success());
    }

    /// A lock file that cannot even be opened is never taken for a stopped
    /// daemon: draining fails at once instead of reading a live cache.
    #[cfg(unix)]
    #[test]
    fn draining_fails_when_the_lock_cannot_be_probed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("daemon.run.lock")).unwrap();
        let stopped = Path::new("true");
        assert!(!stop_and_wait(stopped, dir.path(), Duration::from_secs(10)));
        assert!(drain(stopped, dir.path()).is_err());
        assert!(restart_after_build(stopped, dir.path()).is_err());
    }

    /// The fixture helpers pass on whether `kache daemon start` succeeded,
    /// and a fixture's daemon reads an empty configuration.
    #[cfg(unix)]
    #[test]
    fn fixture_daemons_report_a_failed_start() {
        let dir = tempfile::tempdir().unwrap();
        start_for_fixture(Path::new("true"), dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("kache-config.toml")).unwrap(),
            ""
        );
        assert!(start_for_fixture(Path::new("false"), dir.path()).is_err());
        restart_after_build(Path::new("true"), dir.path()).unwrap();
        assert!(restart_after_build(Path::new("false"), dir.path()).is_err());
    }
}
