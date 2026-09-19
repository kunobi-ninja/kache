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

/// A benchmark must not read or replace a cache with queued publications.
pub fn drain(kache_path: &Path, cache_dir: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        stop_and_wait(kache_path, cache_dir, std::time::Duration::from_secs(40)),
        "daemon did not stop after the build: {}",
        cache_dir.display()
    );
    Ok(())
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
        assert!(wait_for_run_lock(dir.path(), Duration::ZERO));
        // The probe must release its own lock too.
        assert!(wait_for_run_lock(dir.path(), Duration::ZERO));
    }
}
