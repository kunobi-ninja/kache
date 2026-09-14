//! Validate kache's nvcc adapter against a real CUDA toolkit.
//!
//! Every test skips gracefully when `nvcc` is absent from PATH, so the
//! normal suite stays green on machines without CUDA. The `cuda-toolkit`
//! CI job installs the toolkit (no GPU needed: probing, dependency
//! queries, and `-c` compiles are host-side) and runs this file for
//! real. That lane is the only place that exercises the adapter
//! against genuine driver behavior (kunobi-ninja/kache#1024).

use assert_cmd::Command;
use std::path::Path;
use tempfile::TempDir;

#[allow(dead_code)]
mod common;
use common::hermetic_command;

/// Path to the binary under test. Cargo sets `CARGO_BIN_EXE_kache` to the
/// artifact it built for this integration test.
const KACHE_BIN: &str = env!("CARGO_BIN_EXE_kache");

/// A `kache` invocation wired to a throwaway environment: its own cache dir,
/// config path, and HOME/CARGO_HOME so nothing touches the developer's real
/// setup and no background daemon is contacted.
fn kache(home: &Path, cache_dir: &Path) -> Command {
    let mut cmd = Command::from(hermetic_command(
        KACHE_BIN,
        cache_dir,
        Some(&cache_dir.join("config.toml")),
    ));
    cmd.env("KACHE_LOG", "off")
        .env("HOME", home)
        .env("CARGO_HOME", home.join(".cargo"))
        .env("SHELL", "/bin/bash")
        .env_remove("ZDOTDIR")
        .env("XDG_CONFIG_HOME", home.join(".config"))
        // Daemons spawned during tests must self-exit quickly instead of
        // lingering indefinitely.
        .env("KACHE_DAEMON_IDLE_TIMEOUT", "3");
    cmd
}

struct Env {
    _home: TempDir,
    _cache: TempDir,
    home: std::path::PathBuf,
    cache: std::path::PathBuf,
}

fn env() -> Env {
    let home = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    Env {
        home: home.path().to_path_buf(),
        cache: cache.path().to_path_buf(),
        _home: home,
        _cache: cache,
    }
}

impl Env {
    fn cmd(&self) -> Command {
        kache(&self.home, &self.cache)
    }

    /// Run `kache nvcc ...` in `dir` and assert the compiler exited 0.
    fn kache_nvcc(&self, dir: &Path, args: &[&str]) {
        let mut argv = vec!["nvcc"];
        argv.extend_from_slice(args);
        self.cmd().current_dir(dir).args(argv).assert().success();
    }

    fn report(&self) -> serde_json::Value {
        let out = self
            .cmd()
            .args(["report", "--format", "json"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        serde_json::from_slice(&out).expect("valid json")
    }
}

/// True if a working `nvcc` is on PATH.
fn nvcc_available() -> bool {
    std::process::Command::new("nvcc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// True if a working host compiler is on PATH (nvcc needs one to compile).
fn host_compiler_available() -> bool {
    std::process::Command::new("gcc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn require_toolkit(test: &str) -> bool {
    if !nvcc_available() {
        eprintln!("skipping {test}: no nvcc on PATH");
        return false;
    }
    if !host_compiler_available() {
        eprintln!("skipping {test}: no gcc host compiler on PATH");
        return false;
    }
    true
}

/// A minimal CUDA project: a kernel including a project header through
/// `-Iinclude`, so the dependency closure has a non-trivial input.
fn write_project(dir: &Path) {
    std::fs::create_dir_all(dir.join("include")).unwrap();
    std::fs::write(
        dir.join("include/k.h"),
        "__device__ inline int helper(int x) { return x + 1; }\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("kernel.cu"),
        "#include \"k.h\"\n__global__ void kernel(int *out) { *out = helper(41); }\n",
    )
    .unwrap();
}

fn standard_args() -> Vec<&'static str> {
    vec![
        "-c",
        "kernel.cu",
        "-o",
        "kernel.o",
        "-Iinclude",
        "-DUSE_CUDA",
    ]
}

#[test]
fn nvcc_miss_then_hit() {
    // A real `-c` nvcc compile is cacheable: first run misses and stores,
    // second run is a local hit served from the cache. This exercises the
    // whole adapter against genuine driver behavior: probe (`--version`
    // plus host-compiler discovery via `--dryrun`), key computation
    // (`-M` closure over raw contents), store, and restore.
    if !require_toolkit("nvcc_miss_then_hit") {
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    write_project(project.path());
    let args = standard_args();

    // First compile: cache miss -> compile -> store.
    e.kache_nvcc(project.path(), &args);
    assert!(
        project.path().join("kernel.o").exists(),
        "first compile output"
    );

    // Remove the object so the second run must restore it from cache.
    std::fs::remove_file(project.path().join("kernel.o")).unwrap();

    // Second compile: identical invocation -> local cache hit -> restore.
    e.kache_nvcc(project.path(), &args);
    assert!(
        project.path().join("kernel.o").exists(),
        "second compile should restore the object from cache"
    );

    let v = e.report();
    assert!(
        v["summary"]["local_hits"].as_u64().unwrap_or(0) >= 1,
        "report should record a local hit after the second compile: {v}"
    );
}

#[test]
fn nvcc_header_edit_busts_key() {
    // The key hashes raw header contents (never preprocessed output), so
    // editing a header must miss again. Follows the depinfo regression
    // convention: sleep past coarse mtime granularity before editing.
    if !require_toolkit("nvcc_header_edit_busts_key") {
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    write_project(project.path());
    let args = standard_args();

    e.kache_nvcc(project.path(), &args);
    e.kache_nvcc(project.path(), &args);

    std::thread::sleep(std::time::Duration::from_millis(1100));
    std::fs::write(
        project.path().join("include/k.h"),
        "__device__ inline int helper(int x) { return x + 2; }\n",
    )
    .unwrap();

    e.kache_nvcc(project.path(), &args);

    let v = e.report();
    assert!(
        v["summary"]["misses"].as_u64().unwrap_or(0) >= 2,
        "header edit must miss again (initial miss plus post-edit miss): {v}"
    );
}

#[test]
fn nvcc_cross_checkout_hits() {
    // The same relative tree built at a different absolute path must hit:
    // prefix maps normalize clone-local roots out of the key. All paths
    // stay relative with cwd set per build, so no absolute path leaks in.
    if !require_toolkit("nvcc_cross_checkout_hits") {
        return;
    }
    let e = env();
    let root = TempDir::new().unwrap();
    let dir_a = root.path().join("checkout-a");
    let dir_b = root.path().join("checkout-b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();
    write_project(&dir_a);
    write_project(&dir_b);
    let args = standard_args();

    e.kache_nvcc(&dir_a, &args);
    assert!(dir_a.join("kernel.o").exists(), "checkout A output");

    // Same bytes, different absolute path, same shared cache.
    e.kache_nvcc(&dir_b, &args);
    assert!(
        dir_b.join("kernel.o").exists(),
        "checkout B should restore the object from cache"
    );

    let v = e.report();
    assert!(
        v["summary"]["local_hits"].as_u64().unwrap_or(0) >= 1,
        "report should record a cross-checkout local hit: {v}"
    );
}

#[test]
fn nvcc_depinfo_roundtrips() {
    // `-MD -MF` is the only depinfo shape v1 caches: the generation mode
    // plus an explicit sidecar path. Both artifacts must store on miss
    // and restore on hit.
    if !require_toolkit("nvcc_depinfo_roundtrips") {
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    write_project(project.path());
    let mut args = standard_args();
    args.extend(["-MD", "-MF", "kernel.d"]);

    // First compile: miss -> compile -> store (object + depfile).
    e.kache_nvcc(project.path(), &args);
    assert!(project.path().join("kernel.o").exists(), "object produced");
    assert!(project.path().join("kernel.d").exists(), "depfile produced");

    // Drop both artifacts so the hit must restore them.
    std::fs::remove_file(project.path().join("kernel.o")).unwrap();
    std::fs::remove_file(project.path().join("kernel.d")).unwrap();

    // Second compile: local hit -> restore both artifacts.
    e.kache_nvcc(project.path(), &args);
    assert!(
        project.path().join("kernel.o").exists(),
        "object restored from cache"
    );
    assert!(
        project.path().join("kernel.d").exists(),
        "depfile restored from cache"
    );

    let v = e.report();
    assert!(
        v["summary"]["local_hits"].as_u64().unwrap_or(0) >= 1,
        "report should record a local hit after the second compile: {v}"
    );
}

#[test]
fn nvcc_device_debug_passthrough() {
    // `-G` (device debug) is refused: it still compiles fine, uncached.
    if !require_toolkit("nvcc_device_debug_passthrough") {
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    write_project(project.path());
    let mut args = standard_args();
    args.push("-G");

    e.kache_nvcc(project.path(), &args);
    assert!(
        project.path().join("kernel.o").exists(),
        "passthrough should have produced the object"
    );

    let v = e.report();
    assert!(
        v["summary"]["passthroughs"].as_u64().unwrap_or(0) >= 1,
        "report should record a passthrough: {v}"
    );
}
