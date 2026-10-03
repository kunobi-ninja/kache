//! Build-script declarations must describe the checkout Cargo is building (#1420).

#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::Command;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary, stop_daemon};

struct CacheGuard(PathBuf);

impl Drop for CacheGuard {
    fn drop(&mut self) {
        let mut stop = hermetic_command(
            kache_binary(),
            &self.0,
            Some(&isolated_config_path(&self.0)),
        );
        stop.args(["daemon", "stop"]);
        stop_daemon(&mut stop, &self.0);
    }
}

fn write(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

fn fixture(root: &Path, changed: bool) {
    write(
        &root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/a\", \"crates/b\"]\nresolver = \"2\"\n",
    );
    write(
        &root.join("crates/a/Cargo.toml"),
        "[package]\nname = \"a\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    );
    write(
        &root.join("crates/a/src/lib.rs"),
        if changed {
            "pub fn a() -> u32 { 1 }\n// only in checkout two\n"
        } else {
            "pub fn a() -> u32 { 1 }\n"
        },
    );
    write(
        &root.join("crates/b/Cargo.toml"),
        "[package]\nname = \"b\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\
         [dependencies]\na = { path = \"../a\" }\n",
    );
    write(
        &root.join("crates/b/build.rs"),
        r#"use std::path::PathBuf;
fn main() {
    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let directory = manifest.join("../a/src").canonicalize().unwrap();
    let mode = std::env::var("DECLARATION_MODE").unwrap();
    let declared = match mode.as_str() {
        "relative" => PathBuf::from("../a/src"),
        "file" => directory.join("lib.rs"),
        "directory" => directory.clone(),
        _ => panic!("unknown declaration mode"),
    };
    println!("cargo:rerun-if-changed={}", declared.display());
    println!("cargo:rerun-if-env-changed=DECLARATION_MODE");
    let len = std::fs::read(directory.join("lib.rs")).unwrap().len();
    println!("cargo:rustc-env=BAKED_LEN={len}");
}
"#,
    );
    write(
        &root.join("crates/b/src/main.rs"),
        r#"fn main() {
    let actual = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/../a/src/lib.rs")).unwrap().len();
    let baked: usize = env!("BAKED_LEN").parse().unwrap();
    assert_eq!(baked, actual, "stale build-script replay");
    println!("baked={baked} actual={actual}");
    assert_eq!(a::a(), 1);
}
"#,
    );
}

fn build(workspace: &Path, target: &Path, cache: &Path, mode: &str) -> Vec<serde_json::Value> {
    let mut command = hermetic_command("cargo", cache, Some(&isolated_config_path(cache)));
    command
        .args(["build", "--offline", "--quiet"])
        .current_dir(workspace)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("RUSTFLAGS", "-C debuginfo=0")
        .env("DECLARATION_MODE", mode)
        .env("KACHE_BUILD_SCRIPT_CACHE", "1")
        .env("KACHE_BUILD_SCRIPT_HERMETIC", "0")
        .env_remove("KACHE_BASE_DIR")
        .env_remove("KACHE_DISABLED")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    let event_log = cache.join("events.jsonl");
    let before = std::fs::read_to_string(&event_log)
        .unwrap_or_default()
        .len();
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "Cargo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events = std::fs::read_to_string(event_log).unwrap();
    events[before..]
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| event["crate_name"] == "build_script_run")
        .collect()
}

fn assert_current(target: &Path) {
    let output = Command::new(target.join("debug/b")).output().unwrap();
    assert!(
        output.status.success(),
        "{}: {}",
        target.display(),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn check_declarations(mode: &str) {
    build_kache();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let cache = root.join("cache");
    let _daemon = CacheGuard(cache.clone());
    let first = root.join("first");
    let second = root.join("second");
    fixture(&first, false);
    fixture(&second, true);

    // Distinct out-of-workspace targets share a store, as in the report.
    let target = root.join("t1");
    let recorded = build(&first, &target, &cache, mode);
    assert!(
        recorded.iter().any(|event| event["result"] == "miss"),
        "the first run must be recorded: {recorded:?}"
    );
    assert_current(&target);

    // The fix must retain cache hits across target directories in one checkout.
    let target = root.join("t2");
    let restored = build(&first, &target, &cache, mode);
    assert!(
        restored.iter().any(|event| event["result"] == "local_hit"),
        "the same checkout should restore its run: {restored:?}"
    );
    assert_current(&target);

    let target = root.join("t3");
    build(&second, &target, &cache, mode);
    assert_current(&target);
}

#[test]
fn absolute_directory_declarations_do_not_replay_another_checkouts_inputs() {
    check_declarations("directory");
}

#[test]
fn absolute_file_declarations_do_not_replay_another_checkouts_inputs() {
    check_declarations("file");
}

#[test]
fn relative_directory_declarations_follow_the_current_checkout() {
    check_declarations("relative");
}
