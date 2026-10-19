//! Explicit split-debuginfo policy survives both cache misses and hits.
#![cfg(unix)]

use std::fs;

#[allow(dead_code)]
mod common;
use common::{hermetic_command, kache_binary, settle_writes, stop_daemon};

fn check_debug_bundle_policy(split: &str, expected_bundle: bool) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let cache = root.join("cache");
    let target = root.join("target");
    fs::create_dir(root.join("src")).unwrap();
    fs::create_dir(&cache).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        format!(
            "[package]\nname = \"debug_bundle_policy\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n\n[profile.dev]\ndebug = 2\nsplit-debuginfo = {split:?}\n"
        ),
    )
    .unwrap();
    fs::write(
        root.join("src/main.rs"),
        "fn main() { println!(\"42\"); }\n",
    )
    .unwrap();
    let config = cache.join("config.toml");
    let cache_path = toml::Value::String(cache.to_string_lossy().into_owned());
    fs::write(
        &config,
        format!(
            "[cache]\nlocal_only = true\ncache_executables = true\nadaptive_incremental = false\nlocal_store = {cache_path}\nruntime_dir = {cache_path}\n"
        ),
    )
    .unwrap();
    settle_writes(&[&root]);
    let kache = kache_binary();

    for expected_result in ["miss", "local_hit"] {
        let output = hermetic_command(env!("CARGO"), &cache, Some(&config))
            .args(["build", "--offline"])
            .current_dir(&root)
            .env("RUSTC_WRAPPER", &kache)
            .env("CARGO_TARGET_DIR", &target)
            .env("CARGO_INCREMENTAL", "0")
            .env_remove("RUSTC_WORKSPACE_WRAPPER")
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_RUSTFLAGS")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .filter(|event: &serde_json::Value| event["crate_name"] == "debug_bundle_policy")
            .collect();
        assert_eq!(
            events.last().unwrap()["result"],
            expected_result,
            "{events:?}"
        );
        let deps = target.join("debug/deps");
        let bundles: Vec<_> = fs::read_dir(&deps)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "dSYM"))
            .collect();
        assert_eq!(!bundles.is_empty(), expected_bundle, "{split}: {bundles:?}");
        fs::remove_dir_all(&target).unwrap();
    }
    let mut stop = hermetic_command(&kache, &cache, Some(&config));
    stop.args(["daemon", "stop"]);
    stop_daemon(&mut stop, &cache);
}

#[test]
fn split_debuginfo_off_keeps_cold_and_cached_builds_free_of_debug_bundles() {
    check_debug_bundle_policy("off", false);
}

#[test]
fn unpacked_debuginfo_still_gets_a_relocatable_bundle_on_misses_and_hits() {
    check_debug_bundle_policy("unpacked", cfg!(target_os = "macos"));
}
