//! Shared bootstrap for the integration tests that drive the real `kache`
//! binary as a compiler wrapper.
//!
//! These tests can't use `CARGO_BIN_EXE_kache`: the binary under test has to be
//! built with no configured `rustc-wrapper`, so a developer dogfooding kache
//! (or a broken wrapper being debugged) can't influence it. That means a second
//! cargo build into its own target dir.
//!
//! That target dir is ~2 GB, so it is **one stable directory shared by every
//! test binary**, not one per process (#599). Keying it on the PID meant every
//! `cargo test` run left another 2 GB behind per suite, which filled a 926 GB
//! disk twice; the resulting ENOSPC then surfaced as `kache build failed` and
//! `Once instance has previously been poisoned`, reading like a cache-key
//! regression rather than an infrastructure problem.
//!
//! It lives under the workspace target dir so `cargo clean` reclaims it, it is
//! already gitignored, and concurrent suites reuse the same build instead of
//! each doing their own (cargo's build-dir lock serializes them).

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// `<target>/kache-test-bootstrap`, derived from the running test binary at
/// `<target>/<profile>/deps/<test-bin>` so it follows `CARGO_TARGET_DIR`
/// (including `cargo llvm-cov`'s).
fn bootstrap_target_dir() -> PathBuf {
    let mut dir = std::env::current_exe().expect("test binary path");
    dir.pop(); // test binary
    dir.pop(); // deps/
    dir.pop(); // <profile>/
    dir.join("kache-test-bootstrap")
}

/// The bootstrap result, shared by every test in this binary.
///
/// Holds `Err(message)` rather than panicking inside the initializer: a panic
/// there leaves the lock poisoned, so every later test reports "previously
/// poisoned" instead of the real cause. Here each test gets the actual cargo
/// failure — including its stderr, which is where `No space left on device`
/// shows up.
static KACHE_BIN: OnceLock<Result<PathBuf, String>> = OnceLock::new();

/// Builds the `kache` binary under test if it isn't built yet.
pub fn build_kache() {
    let _ = kache_binary();
}

/// Path to the `kache` binary under test, building it on first call.
pub fn kache_binary() -> PathBuf {
    match KACHE_BIN.get_or_init(bootstrap_kache) {
        Ok(path) => path.clone(),
        Err(message) => panic!("{message}"),
    }
}

fn bootstrap_kache() -> Result<PathBuf, String> {
    let target_dir = bootstrap_target_dir();
    let output = std::process::Command::new("cargo")
        .args([
            "build",
            "--bin",
            "kache",
            "--target-dir",
            target_dir.to_str().expect("bootstrap target dir is utf-8"),
            "--config",
            "build.rustc-wrapper=\"\"",
        ])
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .output()
        .map_err(|e| format!("failed to spawn cargo to build kache: {e}"))?;

    if !output.status.success() {
        return Err(format!(
            "kache build failed ({}) in {}\nstdout: {}\nstderr: {}",
            output.status,
            target_dir.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ));
    }

    let mut bin = target_dir.join("debug").join("kache");
    if cfg!(windows) {
        bin.set_extension("exe");
    }
    if !bin.is_file() {
        return Err(format!(
            "kache build reported success but {} is missing",
            bin.display()
        ));
    }
    Ok(bin)
}

/// Keeps tests off the developer's real `~/.config/kache/config.toml`.
pub fn isolated_config_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("config.toml")
}

/// A scratch directory on the *repository's* filesystem, for tests whose
/// subject is how artifacts are materialized.
///
/// `TempDir::new()` lands in `TMPDIR`, so a cache placed there sits on a
/// different filesystem from the build — or on tmpfs, which supports no
/// reflink. Either way the store ingest and the restore take different paths
/// than they would in a real build, and those paths differ in what file mode
/// they leave behind. Anchoring here keeps such tests on whatever filesystem
/// the developer or CI actually builds on, and `cargo clean` reclaims it.
///
/// Only the tests that materialize artifacts need this, and each integration
/// test binary compiles this module separately, so the rest see it as dead.
#[allow(dead_code)]
pub fn scratch_dir() -> PathBuf {
    let dir = bootstrap_target_dir().with_file_name("kache-test-scratch");
    std::fs::create_dir_all(&dir).expect("creating scratch dir");
    dir
}
