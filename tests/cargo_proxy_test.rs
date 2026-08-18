//! End-to-end regression for canonical duplicate Cargo config discovery (#766).

#![cfg(unix)]

use serde_json::Value;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

const KACHE_BIN: &str = env!("CARGO_BIN_EXE_kache");

fn cargo_env(command: &mut Command, home: &Path, cache: &Path, target: &Path) {
    command
        .env("HOME", home)
        .env("CARGO_HOME", home.join(".cargo"))
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("RUSTC_WRAPPER", KACHE_BIN)
        .env("KACHE_CACHE_DIR", cache)
        .env("KACHE_CONFIG", cache.join("config.toml"))
        .env("KACHE_LOG", "off")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTFLAGS");
}

#[test]
fn canonical_cargo_home_alias_keeps_existing_cargo_unit_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = home.join("work/project");
    let cargo_home = home.join(".cargo");
    let cache = dir.path().join("cache");
    let cold_target = dir.path().join("target-cold");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::create_dir_all(&cargo_home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"proxy_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(project.join("src/lib.rs"), "pub fn answer() -> u8 { 42 }\n").unwrap();
    std::fs::write(
        cargo_home.join("config.toml"),
        "[build]\nrustflags = [\"--cfg\", \"kache_proxy_fixture\"]\n",
    )
    .unwrap();

    let mut cold = Command::new(KACHE_BIN);
    cold.args(["cargo", "--", "check", "--quiet"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"));
    cargo_env(&mut cold, &home, &cache, &cold_target);
    let cold = cold.output().unwrap();
    assert!(
        cold.status.success(),
        "cold cargo failed: {}",
        String::from_utf8_lossy(&cold.stderr)
    );

    std::os::unix::fs::symlink(&cargo_home, home.join("work/.cargo")).unwrap();

    let mut warm = Command::new(KACHE_BIN);
    warm.args(["cargo", "--", "check", "--verbose", "--color=never"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"))
        .env("CARGO_TERM_COLOR", "always");
    cargo_env(&mut warm, &home, &cache, &cold_target);
    let warm = warm.output().unwrap();
    assert!(
        warm.status.success(),
        "proxied cargo failed: {}",
        String::from_utf8_lossy(&warm.stderr)
    );
    assert!(
        String::from_utf8_lossy(&warm.stderr).contains("Fresh proxy_fixture"),
        "Cargo should keep the existing unit fresh: {}",
        String::from_utf8_lossy(&warm.stderr)
    );

    let events: Vec<Value> = std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|event: &Value| event["crate_name"] == "proxy_fixture")
        .collect();
    assert_eq!(
        events.len(),
        1,
        "a fresh Cargo unit must not invoke the wrapper again: {events:#?}"
    );
    assert_eq!(events[0]["result"], "miss", "events: {events:#?}");
    assert_eq!(events[0]["compiler_runs"], 1, "events: {events:#?}");
}

#[test]
fn explicit_rustflags_keep_their_cargo_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = home.join("work/project");
    let cargo_home = home.join(".cargo");
    let cache = dir.path().join("cache");
    let target = dir.path().join("target");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::create_dir_all(&cargo_home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"proxy_env_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        project.join("src/lib.rs"),
        "#[cfg(not(from_env))]\ncompile_error!(\"explicit RUSTFLAGS was replaced\");\n",
    )
    .unwrap();
    std::fs::write(
        cargo_home.join("config.toml"),
        "[build]\nrustflags = [\"--cfg\", \"from_config\"]\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(&cargo_home, home.join("work/.cargo")).unwrap();

    let mut command = Command::new(KACHE_BIN);
    command
        .args(["cargo", "--", "check", "--quiet"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"));
    cargo_env(&mut command, &home, &cache, &target);
    command.env("RUSTFLAGS", "--cfg from_env");
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "explicit RUSTFLAGS lost precedence: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn non_utf8_cargo_argument_passes_through_without_cli_panic() {
    let dir = tempfile::tempdir().unwrap();
    let fake_cargo = dir.path().join("cargo");
    std::fs::write(&fake_cargo, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&fake_cargo, std::fs::Permissions::from_mode(0o755)).unwrap();

    let output = Command::new(KACHE_BIN)
        .args(["cargo", "--", "build"])
        .arg(OsString::from_vec(vec![b'f', b'o', 0x80]))
        .env("KACHE_REAL_CARGO", &fake_cargo)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "non-UTF-8 passthrough failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn non_utf8_wrapper_argument_fails_closed_before_compiler_execution() {
    let dir = tempfile::tempdir().unwrap();
    let fake_rustc = dir.path().join("rustc");
    let sentinel = dir.path().join("compiler-ran");
    std::fs::write(
        &fake_rustc,
        format!("#!/bin/sh\ntouch '{}'\n", sentinel.display()),
    )
    .unwrap();
    std::fs::set_permissions(&fake_rustc, std::fs::Permissions::from_mode(0o755)).unwrap();

    let output = Command::new(KACHE_BIN)
        .arg(&fake_rustc)
        .arg(OsString::from_vec(vec![b's', b'r', b'c', b'/', 0x80]))
        .env("RUSTC", &fake_rustc)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!sentinel.exists(), "unsafe compiler fallback was executed");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("cannot be cached safely"),
        "unexpected error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn configured_build_dir_reaches_cargo_without_proxy_override() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    let config_dir = project.join(".cargo");
    let fake_cargo = dir.path().join("cargo");
    let capture = dir.path().join("captured-build-dir");
    std::fs::create_dir_all(home.join(".cargo")).unwrap();
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(
        config_dir.join("config.toml"),
        "[build]\nbuild-dir = \"configured-build\"\n",
    )
    .unwrap();
    std::fs::write(
        &fake_cargo,
        "#!/bin/sh\nprintf '%s' \"${CARGO_BUILD_BUILD_DIR-unset}\" > \"$KACHE_TEST_CAPTURE\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_cargo, std::fs::Permissions::from_mode(0o755)).unwrap();

    let output = Command::new(KACHE_BIN)
        .args(["cargo", "--", "build"])
        .current_dir(&project)
        .env("HOME", &home)
        .env("CARGO_HOME", home.join(".cargo"))
        .env("KACHE_REAL_CARGO", &fake_cargo)
        .env("KACHE_TEST_CAPTURE", &capture)
        .env_remove("CARGO_BUILD_BUILD_DIR")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "proxy failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(capture).unwrap(),
        "unset",
        "a config-defined build-dir must retain Cargo precedence"
    );
}

fn write_shared_target_project(root: &Path, value: &str) {
    std::fs::create_dir_all(root.join("src")).unwrap();
    let manifest = root.join("Cargo.toml");
    let source = root.join("src/main.rs");
    std::fs::write(
        &manifest,
        "[package]\nname = \"proxy_shared_target\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(&source, format!("fn main() {{ print!({value:?}); }}\n")).unwrap();

    // Make both worktrees older than the first produced artifact. This pins
    // Cargo's shared-fingerprint failure deterministically: without a private
    // build-dir, the second worktree is incorrectly declared Fresh.
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    filetime::set_file_mtime(&manifest, old).unwrap();
    filetime::set_file_mtime(&source, old).unwrap();
}

fn proxied_shared_target_build(project: &Path, home: &Path, cache: &Path, target: &Path) -> Output {
    let mut command = Command::new(KACHE_BIN);
    command
        .args(["cargo", "--", "build", "--quiet"])
        .current_dir(project)
        .env("KACHE_REAL_CARGO", env!("CARGO"))
        .env("KACHE_CACHE_EXECUTABLES", "1");
    cargo_env(&mut command, home, cache, target);
    command.output().unwrap()
}

fn run_shared_target_binary(target: &Path) -> String {
    let mut binary = target.join("debug/proxy_shared_target");
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    let output = Command::new(&binary).output().unwrap();
    assert!(output.status.success(), "{} failed", binary.display());
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn cargo_proxy_isolates_fingerprints_while_sharing_final_target_and_kache() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let cache = dir.path().join("cache");
    let shared_target = dir.path().join("shared-target");
    let worktree_a = dir.path().join("worktree-a");
    let worktree_b = dir.path().join("worktree-b");
    let worktree_c = dir.path().join("worktree-c");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    write_shared_target_project(&worktree_a, "alpha");
    write_shared_target_project(&worktree_b, "bravo");
    write_shared_target_project(&worktree_c, "alpha");

    let first = proxied_shared_target_build(&worktree_a, &home, &cache, &shared_target);
    assert!(
        first.status.success(),
        "worktree A failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(run_shared_target_binary(&shared_target), "alpha");

    let second = proxied_shared_target_build(&worktree_b, &home, &cache, &shared_target);
    assert!(
        second.status.success(),
        "worktree B failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        run_shared_target_binary(&shared_target),
        "bravo",
        "Cargo reused worktree A's shared fingerprint without invoking Kache"
    );

    let back_to_a = proxied_shared_target_build(&worktree_a, &home, &cache, &shared_target);
    assert!(back_to_a.status.success());
    assert_eq!(
        run_shared_target_binary(&shared_target),
        "alpha",
        "a Fresh worktree-local unit did not refresh the shared final artifact"
    );

    let relocated = proxied_shared_target_build(&worktree_c, &home, &cache, &shared_target);
    assert!(relocated.status.success());
    assert_eq!(run_shared_target_binary(&shared_target), "alpha");

    for worktree in [&worktree_a, &worktree_b, &worktree_c] {
        assert!(
            worktree.join("target/debug/.fingerprint").is_dir(),
            "intermediate fingerprints must be private to {}",
            worktree.display()
        );
    }
    assert!(
        !shared_target.join("debug/.fingerprint").exists(),
        "the shared final target must not contain Cargo fingerprint state"
    );

    let events: Vec<Value> = std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|event: &Value| event["crate_name"] == "proxy_shared_target")
        .collect();
    assert_eq!(events.len(), 3, "events: {events:#?}");
    assert_eq!(events[0]["result"], "miss");
    assert_ne!(events[1]["result"], "local_hit");
    assert_eq!(events[2]["result"], "local_hit");
    assert_eq!(events[0]["cache_key"], events[2]["cache_key"]);
}
