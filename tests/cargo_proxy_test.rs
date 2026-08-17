//! End-to-end regression for canonical duplicate Cargo config discovery (#766).

#![cfg(unix)]

use serde_json::Value;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

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

    let mut cold = Command::new(env!("CARGO"));
    cold.args(["check", "--quiet"]).current_dir(&project);
    cargo_env(&mut cold, &home, &cache, &cold_target);
    let cold = cold.output().unwrap();
    assert!(
        cold.status.success(),
        "cold cargo failed: {}",
        String::from_utf8_lossy(&cold.stderr)
    );

    std::os::unix::fs::symlink(&cargo_home, home.join("work/.cargo")).unwrap();

    let mut warm = Command::new(KACHE_BIN);
    warm.args(["cargo", "--", "check", "--verbose"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"));
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
