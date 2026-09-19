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

/// Stop the daemon bound to `cache_dir` and wait until it has gone: its
/// socket is removed when the process exits, after it has drained the work
/// wrappers handed to it. `daemon stop` alone returns as soon as the request
/// is acknowledged, which is too early for a runner that reads the store
/// next. False when the daemon was still there after `timeout`.
pub fn stop_and_wait(kache_path: &Path, cache_dir: &Path, timeout: std::time::Duration) -> bool {
    stop(kache_path, cache_dir);
    let socket = cache_dir.join("daemon.sock");
    let deadline = std::time::Instant::now() + timeout;
    while socket.exists() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    true
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
