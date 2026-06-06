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

/// Best-effort: start a kache daemon bound to `cache_dir`.
///
/// The wrapper falls back to no-daemon mode if this fails, so callers should
/// warn but not fail the scenario.
pub fn start(kache_path: &Path, cache_dir: &Path, kache_config: &Path) {
    match Command::new(kache_path)
        .args(["daemon", "start"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", kache_config)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => eprintln!("[bench] daemon started"),
        Ok(s) => eprintln!("[bench] WARN: daemon start exited {s} (running w/o daemon)"),
        Err(e) => eprintln!("[bench] WARN: daemon start failed ({e}) (running w/o daemon)"),
    }
}
