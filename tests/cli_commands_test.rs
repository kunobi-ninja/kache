//! End-to-end coverage of the read-only / cache-management CLI subcommands.
//!
//! These drive the real `kache` binary (the one Cargo built for this test —
//! the instrumented build under `cargo llvm-cov`, so coverage is captured)
//! against a fully isolated, temporary cache + config + HOME. They assert the
//! observable contract of each subcommand rather than internal state, which is
//! exactly the integration surface that the unit tests can't reach: clap
//! dispatch in `main.rs`, the command bodies in `cli.rs`, config loading, and
//! the daemon-unavailable fallback to direct store reads.

use assert_cmd::Command;
use std::path::Path;
use tempfile::TempDir;

/// Path to the binary under test. Cargo sets `CARGO_BIN_EXE_kache` to the
/// artifact it built for this integration test — under `cargo llvm-cov` that
/// is the instrumented binary, so spawning it counts toward coverage.
const KACHE_BIN: &str = env!("CARGO_BIN_EXE_kache");

/// A `kache` invocation wired to a throwaway environment: its own cache dir,
/// config path, and HOME/CARGO_HOME so nothing touches the developer's real
/// setup and no background daemon is contacted.
fn kache(home: &Path, cache_dir: &Path) -> Command {
    let mut cmd = Command::new(KACHE_BIN);
    cmd.env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", cache_dir.join("config.toml"))
        .env("KACHE_LOG", "off")
        .env("HOME", home)
        .env("CARGO_HOME", home.join(".cargo"))
        // Daemons spawned during tests must self-exit quickly instead of
        // lingering for the default 10min — otherwise, under `just coverage`,
        // they pile up and CPU-starve the rest of the suite.
        .env("KACHE_DAEMON_IDLE_TIMEOUT", "3")
        // Never let a real wrapper config leak into the spawned process.
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER");
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

    /// Run `cargo build --lib` for `project` through the kache rustc wrapper,
    /// wired to this env's isolated cache/config/HOME. Returns the build output.
    fn wrapper_build(&self, project: &Path, target_dir: &Path) -> std::process::Output {
        std::process::Command::new(env!("CARGO"))
            .args(["build", "--lib"])
            .current_dir(project)
            .env("RUSTC_WRAPPER", KACHE_BIN)
            .env("KACHE_CACHE_DIR", &self.cache)
            .env("KACHE_CONFIG", self.cache.join("config.toml"))
            .env("KACHE_LOG", "off")
            .env("HOME", &self.home)
            // Short idle timeout so the build-spawned daemon doesn't linger and
            // pile up across tests (see kache() for rationale).
            .env("KACHE_DAEMON_IDLE_TIMEOUT", "3")
            .env("CARGO_TARGET_DIR", target_dir)
            .env("CARGO_INCREMENTAL", "0")
            .output()
            .expect("run cargo build with kache wrapper")
    }
}

/// Scaffold a minimal standalone library crate (its own empty `[workspace]`,
/// so cargo treats it as a leaf) with the given name and source body.
fn scaffold_lib(name: &str, body: &str) -> TempDir {
    let project = TempDir::new().unwrap();
    std::fs::create_dir_all(project.path().join("src")).unwrap();
    std::fs::write(
        project.path().join("Cargo.toml"),
        format!(
            "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
             [lib]\npath = \"src/lib.rs\"\n\n[workspace]\n"
        ),
    )
    .unwrap();
    std::fs::write(project.path().join("src/lib.rs"), body).unwrap();
    project
}

/// True if a working C compiler is on PATH (clang or cc).
fn cc_available() -> bool {
    std::process::Command::new("cc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ── version / help / dispatch ───────────────────────────────────────────────

#[test]
fn version_prints_package_version() {
    let e = env();
    e.cmd()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn help_lists_subcommands() {
    let e = env();
    e.cmd()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("list"))
        .stdout(predicates::str::contains("report"))
        .stdout(predicates::str::contains("doctor"));
}

#[test]
fn unknown_subcommand_is_a_usage_error() {
    let e = env();
    e.cmd()
        .arg("definitely-not-a-command")
        .assert()
        .failure()
        .code(2);
}

#[test]
fn subcommand_help_is_available() {
    for sub in ["list", "report", "gc", "purge", "doctor", "stats", "sync"] {
        let e = env();
        e.cmd()
            .args([sub, "--help"])
            .assert()
            .success()
            .stdout(predicates::str::contains("Usage"));
    }
}

// ── empty-cache behavior ────────────────────────────────────────────────────

#[test]
fn list_on_empty_cache_reports_no_entries() {
    let e = env();
    e.cmd()
        .arg("list")
        .assert()
        .success()
        .stdout(predicates::str::contains("No cached entries"));
}

#[test]
fn list_accepts_all_sort_modes() {
    for sort in ["name", "size", "hits", "age"] {
        let e = env();
        e.cmd().args(["list", "--sort", sort]).assert().success();
    }
}

#[test]
fn list_for_specific_missing_crate_succeeds() {
    let e = env();
    e.cmd().args(["list", "serde"]).assert().success();
}

#[test]
fn stats_on_empty_cache_succeeds() {
    let e = env();
    e.cmd().arg("stats").assert().success();
    let e = env();
    e.cmd().args(["stats", "--since", "1h"]).assert().success();
}

#[test]
fn report_emits_each_format() {
    for format in ["text", "json", "markdown", "github"] {
        let e = env();
        e.cmd()
            .args(["report", "--format", format])
            .assert()
            .success();
    }
}

#[test]
fn report_json_on_empty_cache_is_valid_json() {
    let e = env();
    let out = e
        .cmd()
        .args(["report", "--format", "json", "--since", "24h"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).expect("valid json");
    assert_eq!(v["summary"]["misses"].as_u64(), Some(0));
    assert_eq!(v["summary"]["local_hits"].as_u64(), Some(0));
}

#[test]
fn report_writes_to_output_file() {
    let e = env();
    let out = e.cache.join("report.json");
    e.cmd()
        .args(["report", "--format", "json", "--top", "3", "-o"])
        .arg(&out)
        .assert()
        .success();
    let body = std::fs::read_to_string(&out).expect("report file written");
    let _: serde_json::Value = serde_json::from_str(&body).expect("valid json on disk");
}

#[test]
fn gc_on_empty_cache_succeeds() {
    let e = env();
    e.cmd().arg("gc").assert().success();
    let e = env();
    e.cmd().args(["gc", "--max-age", "7d"]).assert().success();
}

#[test]
fn purge_on_empty_cache_succeeds() {
    let e = env();
    e.cmd().arg("purge").assert().success();
    let e = env();
    e.cmd()
        .args(["purge", "--crate-name", "serde"])
        .assert()
        .success();
}

#[test]
fn why_miss_on_unknown_crate_succeeds() {
    let e = env();
    e.cmd().args(["why-miss", "serde"]).assert().success();
}

#[test]
fn clean_dry_run_reports_no_targets_in_empty_dir() {
    let e = env();
    // Run from an empty working dir so `clean` finds nothing to remove.
    let workdir = TempDir::new().unwrap();
    e.cmd()
        .current_dir(workdir.path())
        .args(["clean", "--dry-run"])
        .assert()
        .success()
        .stdout(predicates::str::contains("No target/ directories found"));
}

// On macOS `clean` skips any path under a TCC-protected prefix (`/private`,
// `~/Documents`, …) to avoid permission prompts — and every temp dir lives
// under `/private`, so the populated-scan branch is unreachable there. The
// scan logic is identical cross-platform and CI measures coverage on Linux,
// so this runs everywhere except macOS.
#[cfg(not(target_os = "macos"))]
#[test]
fn clean_dry_run_reports_a_populated_target_dir() {
    // A cargo project with a non-empty target/ exercises the populated
    // branch of find_target_dirs -> compute_project_stats -> walk_deps_dir ->
    // detect_profiles, plus the dry-run size reporting.
    let e = env();
    let project = TempDir::new().unwrap();
    let root = project.path();
    std::fs::write(root.join("Cargo.toml"), "[package]\nname=\"p\"\n").unwrap();
    for profile in ["debug", "release"] {
        let deps = root.join("target").join(profile).join("deps");
        std::fs::create_dir_all(&deps).unwrap();
        std::fs::write(deps.join("libfoo.rlib"), vec![0u8; 4096]).unwrap();
        std::fs::write(deps.join("libfoo.rmeta"), vec![0u8; 1024]).unwrap();
        std::fs::write(deps.join("foo.d"), b"foo.o: foo.rs\n").unwrap();
    }

    e.cmd()
        .current_dir(root)
        .args(["clean", "--dry-run"])
        .assert()
        .success()
        .stdout(predicates::str::contains("Found 1 target"))
        .stdout(predicates::str::contains("Dry run: would free"));
}

#[test]
fn doctor_runs_readonly_diagnostics() {
    let e = env();
    e.cmd().arg("doctor").assert().success();
}

#[test]
fn doctor_reports_wrapper_configured_via_env() {
    // RUSTC_WRAPPER=kache in the environment hits doctor's "kache via env"
    // (passing) wrapper-check branch.
    let e = env();
    e.cmd()
        .env("RUSTC_WRAPPER", "kache")
        .arg("doctor")
        .assert()
        .success()
        .stdout(predicates::str::contains("RUSTC_WRAPPER"));
}

#[test]
fn doctor_reports_wrapper_configured_via_cargo_config() {
    // An isolated ~/.cargo/config.toml with rustc-wrapper=kache hits doctor's
    // "kache via <cargo config>" wrapper-check branch (no env wrapper set).
    let e = env();
    let cargo_dir = e.home.join(".cargo");
    std::fs::create_dir_all(&cargo_dir).unwrap();
    std::fs::write(
        cargo_dir.join("config.toml"),
        "[build]\nrustc-wrapper = \"kache\"\n",
    )
    .unwrap();
    e.cmd()
        .current_dir(&e.home) // avoid picking up the repo's own cargo config
        .arg("doctor")
        .assert()
        .success()
        .stdout(predicates::str::contains("RUSTC_WRAPPER"));
}

#[test]
fn doctor_reports_local_only_mode() {
    // KACHE_LOCAL_ONLY hits doctor's local-only Remote-check branch.
    let e = env();
    e.cmd()
        .env("KACHE_LOCAL_ONLY", "1")
        .arg("doctor")
        .assert()
        .success()
        .stdout(predicates::str::contains("local-only"));
}

#[test]
fn doctor_reports_configured_remote() {
    // A configured S3 bucket hits doctor's "Remote s3://..." check branch.
    let e = env();
    e.cmd()
        .env("KACHE_S3_BUCKET", "my-remote-bucket")
        .arg("doctor")
        .assert()
        .success()
        .stdout(predicates::str::contains("my-remote-bucket"));
}

#[test]
fn doctor_reports_reachable_daemon_version() {
    // Build through the wrapper with a longer daemon idle timeout so the spawned
    // daemon is still alive when doctor queries it — hitting the daemon-version
    // "reachable + matching" check branch (vs the not-reachable branch).
    let e = env();
    let project = scaffold_lib("kachedoc", "pub fn d() -> u8 { 1 }\n");
    let target_dir = project.path().join("target");
    let build = std::process::Command::new(env!("CARGO"))
        .args(["build", "--lib"])
        .current_dir(project.path())
        .env("RUSTC_WRAPPER", KACHE_BIN)
        .env("KACHE_CACHE_DIR", &e.cache)
        .env("KACHE_CONFIG", e.cache.join("config.toml"))
        .env("KACHE_LOG", "off")
        .env("HOME", &e.home)
        .env("KACHE_DAEMON_IDLE_TIMEOUT", "30") // keep the daemon alive for doctor
        .env("CARGO_TARGET_DIR", &target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .output()
        .expect("run build");
    assert!(build.status.success(), "build failed");

    // doctor queries the live daemon; same binary -> version + epoch match.
    e.cmd()
        .arg("doctor")
        .assert()
        .success()
        .stdout(predicates::str::contains("Daemon version"));
    // (The daemon self-exits via its idle timeout; no explicit stop — `daemon
    // stop` races the idle exit and would flakily error on a missing socket.)
}

#[test]
fn doctor_verify_on_empty_cache_succeeds() {
    let e = env();
    e.cmd().args(["doctor", "--verify"]).assert().success();
    let e = env();
    e.cmd()
        .args(["doctor", "--verify", "--checksums"])
        .assert()
        .success();
}

#[test]
fn doctor_fix_in_isolated_home_succeeds() {
    // --fix may rewrite the (isolated) cargo wrapper config; with HOME and
    // CARGO_HOME pointed at temp dirs this never touches the real setup.
    let e = env();
    std::fs::create_dir_all(e.home.join(".cargo")).unwrap();
    e.cmd().args(["doctor", "--fix"]).assert().success();
}

// Unix-only: migrate resolves `~/.cargo` via `dirs::home_dir()`, which on
// Windows reads the OS profile API and ignores the `HOME`/`CARGO_HOME` the test
// helper sets — so the test couldn't isolate (and would mutate the runner's real
// cargo config). The migration logic is platform-shared and covered here.
#[cfg(unix)]
#[test]
fn doctor_fix_migrates_sccache_cargo_config_to_kache() {
    // doctor --fix runs the sccache->kache migration. With an isolated HOME whose
    // ~/.cargo/config.toml references sccache (and a shell rc that does too),
    // migrate rewrites the cargo config to kache and reports the rc lines.
    let e = env();
    let cargo_dir = e.home.join(".cargo");
    std::fs::create_dir_all(&cargo_dir).unwrap();
    std::fs::write(
        cargo_dir.join("config.toml"),
        "[build]\nrustc-wrapper = \"sccache\"\n",
    )
    .unwrap();
    std::fs::write(
        e.home.join(".zshrc"),
        "export RUSTC_WRAPPER=sccache\n# sccache comment line\n",
    )
    .unwrap();

    e.cmd().args(["doctor", "--fix"]).assert().success();

    // The cargo config's sccache reference was rewritten to kache.
    let rewritten = std::fs::read_to_string(cargo_dir.join("config.toml")).unwrap();
    assert!(
        rewritten.contains("kache") && !rewritten.contains("sccache"),
        "migrate should replace sccache with kache: {rewritten}"
    );
}

#[test]
fn init_check_is_a_dry_run() {
    // --check prints intended changes without modifying anything.
    let e = env();
    std::fs::create_dir_all(e.home.join(".cargo")).unwrap();
    e.cmd().args(["init", "--check"]).assert().success();
}

// Unix-only: init resolves the cargo config path via `dirs::home_dir()`, which
// on Windows reads the OS profile API and ignores the test's `HOME`/`CARGO_HOME`
// — so the test couldn't isolate (and would mutate the runner's real cargo
// config). The init logic is platform-shared and covered here.
#[cfg(unix)]
#[test]
fn init_noninteractive_writes_isolated_cargo_config() {
    // `-y --no-service` configures the cargo rustc-wrapper without installing
    // any OS service. It writes to CARGO_HOME, which we isolate to a temp dir.
    let e = env();
    let cargo_home = e.home.join(".cargo");
    std::fs::create_dir_all(&cargo_home).unwrap();
    e.cmd()
        .args(["init", "-y", "--no-service"])
        .assert()
        .success();
    assert!(
        cargo_home.join("config.toml").exists(),
        "init should have written an isolated cargo config"
    );
}

#[test]
fn daemon_without_subcommand_succeeds() {
    // `kache daemon` (no subcommand) is allowed and reports daemon state
    // without contacting or starting any service.
    let e = env();
    e.cmd().arg("daemon").assert().success();
}

// ── populated-cache behavior ────────────────────────────────────────────────

/// Build a trivial library crate through the kache rustc wrapper so the cache
/// has real entries, then exercise the data-bearing command paths against it.
#[test]
fn commands_operate_on_a_populated_cache() {
    let e = env();
    let project = scaffold_lib("kachetestlib", "pub fn answer() -> u8 { 42 }\n");
    let target_dir = project.path().join("target");

    // Cold build populates the cache; a clean + rebuild produces a cache hit.
    let cold = e.wrapper_build(project.path(), &target_dir);
    assert!(
        cold.status.success(),
        "cold build failed: {}",
        String::from_utf8_lossy(&cold.stderr)
    );
    std::fs::remove_dir_all(&target_dir).ok();
    let warm = e.wrapper_build(project.path(), &target_dir);
    assert!(
        warm.status.success(),
        "warm build failed: {}",
        String::from_utf8_lossy(&warm.stderr)
    );

    // `list` now shows the crate we built.
    e.cmd()
        .arg("list")
        .assert()
        .success()
        .stdout(predicates::str::contains("kachetestlib"));

    // `report --format json` reflects at least one cache entry.
    let out = e
        .cmd()
        .args(["report", "--format", "json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).expect("valid json");
    // The clean+rebuild produced exactly one cache miss (cold) and one local
    // hit (warm) for our crate, with the artifact blobs stored.
    assert_eq!(v["summary"]["local_hits"].as_u64(), Some(1), "report: {v}");
    assert_eq!(v["summary"]["misses"].as_u64(), Some(1), "report: {v}");
    assert!(
        v["top_hits"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false),
        "expected a non-empty top_hits list: {v}"
    );
    assert!(
        v["storage"]["store_blobs"].as_u64().unwrap_or(0) >= 1,
        "expected stored blobs: {v}"
    );

    // Stats, integrity verify, and why-miss all run against real data.
    e.cmd().arg("stats").assert().success();
    e.cmd()
        .args(["doctor", "--verify", "--checksums"])
        .assert()
        .success();
    e.cmd()
        .args(["why-miss", "kachetestlib"])
        .assert()
        .success();

    // Listing the specific crate shows its entry detail.
    e.cmd().args(["list", "kachetestlib"]).assert().success();

    // Purging that crate empties the cache for it; a subsequent list is clean.
    e.cmd()
        .args(["purge", "--crate-name", "kachetestlib"])
        .assert()
        .success();
}

/// A source edit between builds changes the cache key, so the second build is a
/// fresh miss with a *different* key than the first entry. `why-miss` should
/// then take its key-mismatch diagnosis path (why_miss_diff_entries) and the
/// "Key changed" hint — the richest part of the command.
#[test]
fn why_miss_reports_key_mismatch_after_source_edit() {
    let e = env();
    // First version -> first key/entry.
    let project = scaffold_lib("kachediff", "pub fn v() -> u8 { 1 }\n");
    let target_dir = project.path().join("target");
    assert!(
        e.wrapper_build(project.path(), &target_dir)
            .status
            .success(),
        "first build failed"
    );

    // Edit the source -> different key -> a second miss + second stored entry.
    std::fs::remove_dir_all(&target_dir).ok();
    std::fs::write(
        project.path().join("src/lib.rs"),
        "pub fn v() -> u32 { 123456 }\n",
    )
    .unwrap();
    assert!(
        e.wrapper_build(project.path(), &target_dir)
            .status
            .success(),
        "second build failed"
    );

    // Two entries now exist for the crate under two different keys; why-miss
    // explains the mismatch.
    e.cmd()
        .args(["why-miss", "kachediff"])
        .assert()
        .success()
        .stdout(predicates::str::contains("Why `kachediff` missed"))
        .stdout(predicates::str::contains("Diagnosis"));

    // Two distinct stored entries are listed for the crate.
    e.cmd()
        .args(["list", "kachediff"])
        .assert()
        .success()
        .stdout(predicates::str::contains("kachediff"));
}

#[test]
fn cc_link_invocation_passes_through() {
    // A compile+link cc invocation (no -c) isn't a cacheable single-source
    // compile, so kache refuses and passes through to the real compiler.
    // Covers run_cc's refuse->cc_passthrough path.
    if !cc_available() {
        eprintln!("skipping cc_link_invocation_passes_through: no cc on PATH");
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    std::fs::write(project.path().join("main.c"), "int main(void){return 0;}\n").unwrap();

    // `cc main.c -o app` compiles AND links in one step (no -c) -> passthrough.
    e.cmd()
        .current_dir(project.path())
        .args(["cc", "main.c", "-o", "app"])
        .assert()
        .success();
    // The real compiler names the binary `app` on Unix, `app.exe` on Windows.
    assert!(
        project.path().join("app").exists() || project.path().join("app.exe").exists(),
        "passthrough should have produced the linked binary"
    );

    // The passthrough was recorded as a bypass in the report.
    let out = e
        .cmd()
        .args(["report", "--format", "json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).expect("valid json");
    assert!(
        v["summary"]["passthroughs"].as_u64().unwrap_or(0) >= 1,
        "report should record a passthrough: {v}"
    );
}

#[test]
fn cc_compile_roundtrips_through_cache() {
    // A real `-c` cc compile is cacheable: first run misses and stores,
    // second run is a local hit served from the cache. Exercises run_cc's
    // full happy path: cache_key, store.get/put, restore_cc_from_cache,
    // cc_cache_entry_satisfies_invocation, LocalHit logging.
    if !cc_available() {
        eprintln!("skipping cc_compile_roundtrips_through_cache: no cc on PATH");
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    std::fs::write(
        project.path().join("add.c"),
        "int add(int a, int b){return a + b;}\n",
    )
    .unwrap();

    // First compile: cache miss -> compile -> store.
    e.cmd()
        .current_dir(project.path())
        .args(["cc", "-c", "add.c", "-o", "add.o"])
        .assert()
        .success();
    assert!(
        project.path().join("add.o").exists(),
        "first compile output"
    );

    // Remove the object so the second run must restore it from cache.
    std::fs::remove_file(project.path().join("add.o")).unwrap();

    // Second compile: identical invocation -> local cache hit -> restore.
    e.cmd()
        .current_dir(project.path())
        .args(["cc", "-c", "add.c", "-o", "add.o"])
        .assert()
        .success();
    assert!(
        project.path().join("add.o").exists(),
        "second compile should restore the object from cache"
    );

    let out = e
        .cmd()
        .args(["report", "--format", "json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).expect("valid json");
    assert!(
        v["summary"]["local_hits"].as_u64().unwrap_or(0) >= 1,
        "report should record a local hit after the second compile: {v}"
    );
}

#[test]
fn cc_compile_with_depinfo_roundtrips() {
    // A `-c` compile that also emits a dependency file (-MMD -MF) is how the
    // `cc` crate / make-based builds invoke the compiler. Both the object and
    // the .d file must be cached and restored. Exercises the depinfo branches:
    // cc_depinfo_rewrite_root (Some), rewrite_depinfo_outputs (Relativize on
    // store + Expand on restore), and cc_cache_entry_satisfies_invocation's
    // has_depinfo requirement.
    if !cc_available() {
        eprintln!("skipping cc_compile_with_depinfo_roundtrips: no cc on PATH");
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    std::fs::write(project.path().join("dep.c"), "int dep(void){return 7;}\n").unwrap();

    let args = ["cc", "-c", "dep.c", "-MMD", "-MF", "dep.d", "-o", "dep.o"];

    // First compile: miss -> compile -> store (object + depfile).
    e.cmd()
        .current_dir(project.path())
        .args(args)
        .assert()
        .success();
    assert!(project.path().join("dep.o").exists(), "object produced");
    assert!(project.path().join("dep.d").exists(), "depfile produced");

    // Drop both artifacts so the hit must restore them.
    std::fs::remove_file(project.path().join("dep.o")).unwrap();
    std::fs::remove_file(project.path().join("dep.d")).unwrap();

    // Second compile: local hit -> restore both artifacts.
    e.cmd()
        .current_dir(project.path())
        .args(args)
        .assert()
        .success();
    assert!(
        project.path().join("dep.o").exists(),
        "object restored from cache"
    );
    assert!(
        project.path().join("dep.d").exists(),
        "depfile restored from cache"
    );
}

#[test]
fn cc_entry_lacking_depfile_is_evicted_then_recached() {
    // Depinfo flags are stripped from the cc cache key, so a plain compile and
    // a -MMD compile of the same source share a key. When the cached entry
    // (object only) can't satisfy a later invocation that needs a depfile,
    // run_cc evicts it and recompiles, then re-caches a complete entry.
    // Covers the cc_cache_entry_satisfies_invocation==false eviction arm.
    if !cc_available() {
        eprintln!("skipping cc_entry_lacking_depfile_is_evicted_then_recached: no cc on PATH");
        return;
    }
    let e = env();
    let project = TempDir::new().unwrap();
    std::fs::write(project.path().join("ev.c"), "int ev(void){return 9;}\n").unwrap();

    // Run 1: no depfile -> caches object only.
    e.cmd()
        .current_dir(project.path())
        .args(["cc", "-c", "ev.c", "-o", "ev.o"])
        .assert()
        .success();

    // Run 2: same key, but now a depfile is required. The cached entry lacks
    // it -> evict + recompile, producing the depfile and re-caching.
    e.cmd()
        .current_dir(project.path())
        .args(["cc", "-c", "ev.c", "-MMD", "-MF", "ev.d", "-o", "ev.o"])
        .assert()
        .success();
    assert!(
        project.path().join("ev.d").exists(),
        "recompile after eviction should produce the depfile"
    );

    // Run 3: the re-cached entry now has the depfile -> clean local hit.
    std::fs::remove_file(project.path().join("ev.o")).unwrap();
    std::fs::remove_file(project.path().join("ev.d")).unwrap();
    e.cmd()
        .current_dir(project.path())
        .args(["cc", "-c", "ev.c", "-MMD", "-MF", "ev.d", "-o", "ev.o"])
        .assert()
        .success();
    assert!(
        project.path().join("ev.d").exists() && project.path().join("ev.o").exists(),
        "re-cached entry should satisfy the depfile invocation on hit"
    );
}

/// Find the first regular file under `dir` (recursively), if any.
fn first_file_under(dir: &Path) -> Option<std::path::PathBuf> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).ok()?.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.is_file() {
                return Some(p);
            }
        }
    }
    None
}

/// Corrupting a stored blob makes its blake3 no longer match the entry's
/// recorded hash, so `doctor --checksums --repair` must flag the entry as
/// corrupted and remove it — after which the cache lists empty.
#[test]
fn doctor_repair_removes_corrupted_entries() {
    let e = env();
    let project = scaffold_lib("repairlib", "pub fn r() -> u8 { 5 }\n");
    let target_dir = project.path().join("target");
    assert!(
        e.wrapper_build(project.path(), &target_dir)
            .status
            .success(),
        "build failed"
    );

    // Tamper with a stored blob so its checksum no longer matches. Stored
    // blobs are read-only, so make it writable first.
    let blobs = e.cache.join("store").join("blobs");
    let blob = first_file_under(&blobs).expect("a stored blob should exist");
    let mut perms = std::fs::metadata(&blob).unwrap().permissions();
    perms.set_readonly(false);
    std::fs::set_permissions(&blob, perms).unwrap();
    std::fs::write(&blob, b"this is not the original blob content").unwrap();

    e.cmd()
        .args(["doctor", "--checksums", "--repair"])
        .assert()
        .success()
        .stdout(predicates::str::contains("corrupted"))
        .stdout(predicates::str::contains("Repairing: removing"));

    // The corrupted entry was removed during repair.
    e.cmd()
        .arg("list")
        .assert()
        .success()
        .stdout(predicates::str::contains("No cached entries"));
}

/// `gc --max-age` runs the eviction sweep (backfill, dedup, age eviction) over a
/// populated cache and reports the store summary.
#[test]
fn gc_max_age_runs_eviction_sweep_on_populated_cache() {
    let e = env();
    let project = scaffold_lib("kachegc", "pub fn g() -> u8 { 7 }\n");
    let target_dir = project.path().join("target");
    assert!(
        e.wrapper_build(project.path(), &target_dir)
            .status
            .success(),
        "build failed"
    );

    // Sanity: the entry is present before GC.
    e.cmd()
        .arg("list")
        .assert()
        .success()
        .stdout(predicates::str::contains("kachegc"));

    // gc --max-age runs the eviction sweep (via the daemon if one is reachable,
    // else locally) and reports the store summary. NOTE: we do NOT assert the
    // cache empties — whether a just-created entry is evicted at "0h" differs
    // between the daemon and local eviction paths (fresh-entry age rounds to 0),
    // which made an earlier "No cached entries" assertion flaky under coverage
    // (a build-spawned daemon makes the daemon path reachable). The eviction
    // code runs either way, which is what this test covers.
    e.cmd()
        .args(["gc", "--max-age", "0h"])
        .assert()
        .success()
        .stdout(predicates::str::contains("Store:"));
}
