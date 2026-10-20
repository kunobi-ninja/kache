//! Windows Cargo lookup must inspect the same executable that Rust launches.

#![cfg(windows)]

use std::path::{Path, PathBuf};
use std::process::Output;

#[allow(dead_code)]
mod common;

fn cargo_from_path(root: &Path, dirs: &[PathBuf]) -> Output {
    common::hermetic_command(env!("CARGO_BIN_EXE_kache"), &root.join("cache"), None)
        .args(["cargo", "--", "--version"])
        .current_dir(root)
        .env("PATH", std::env::join_paths(dirs).unwrap())
        .env_remove("CARGO")
        .env_remove("KACHE_REAL_CARGO")
        .output()
        .unwrap()
}

fn assert_real_cargo(output: Output) {
    assert!(
        output.status.success(),
        "Cargo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stdout.starts_with(b"cargo "),
        "expected Cargo, got: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn cargo_exe_is_found_without_environment_overrides() {
    let root = tempfile::tempdir().unwrap();
    let cargo_dir = Path::new(env!("CARGO")).parent().unwrap().to_path_buf();
    assert_real_cargo(cargo_from_path(root.path(), &[cargo_dir]));
}

#[test]
fn bare_cargo_does_not_hide_an_exe_that_resolves_to_kache() {
    let root = tempfile::tempdir().unwrap();
    let shims = root.path().join("shims");
    let other = root.path().join("other");
    std::fs::create_dir_all(&shims).unwrap();
    std::fs::create_dir_all(&other).unwrap();
    let kache = other.join("kache.exe");
    std::fs::copy(env!("CARGO_BIN_EXE_kache"), &kache).unwrap();
    std::fs::copy(
        env!("CARGO_BIN_EXE_kache-engine"),
        other.join("kache-engine.exe"),
    )
    .unwrap();
    std::fs::write(shims.join("cargo"), "a bare sibling of the shim").unwrap();
    std::os::windows::fs::symlink_file(&kache, shims.join("cargo.exe")).unwrap();
    let cargo_dir = Path::new(env!("CARGO")).parent().unwrap().to_path_buf();
    assert_real_cargo(cargo_from_path(root.path(), &[shims, cargo_dir]));
}
