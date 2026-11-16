//! Explicit split-debuginfo policy survives both cache misses and hits.
#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};

#[allow(dead_code)]
mod common;
use common::{hermetic_command, kache_binary, settle_writes, stop_daemon};

/// A one-binary package built with `debug = 2` and a local-only cache.
struct Project {
    _dir: tempfile::TempDir,
    root: PathBuf,
    cache: PathBuf,
    target: PathBuf,
    config: PathBuf,
}

impl Project {
    fn new(split: &str) -> Self {
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
        Self {
            _dir: dir,
            root,
            cache,
            target,
            config,
        }
    }

    /// `cargo build` through kache, then the package's events so far.
    fn build(&self) -> Vec<serde_json::Value> {
        self.build_with_stderr().0
    }

    fn build_with_stderr(&self) -> (Vec<serde_json::Value>, String) {
        let output = hermetic_command(env!("CARGO"), &self.cache, Some(&self.config))
            .args(["build", "--offline"])
            .current_dir(&self.root)
            .env("RUSTC_WRAPPER", kache_binary())
            .env("CARGO_TARGET_DIR", &self.target)
            .env("CARGO_INCREMENTAL", "0")
            .env("KACHE_LOG", "kache=warn")
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
        let events = fs::read_to_string(self.cache.join("events.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .filter(|event: &serde_json::Value| event["crate_name"] == "debug_bundle_policy")
            .collect();
        (events, String::from_utf8_lossy(&output.stderr).into_owned())
    }

    fn stop_daemon(&self) {
        let mut stop = hermetic_command(kache_binary(), &self.cache, Some(&self.config));
        stop.args(["daemon", "stop"]);
        stop_daemon(&mut stop, &self.cache);
    }
}

fn check_debug_bundle_policy(split: &str, expected_bundle: bool) {
    let project = Project::new(split);
    for expected_result in ["miss", "local_hit"] {
        let events = project.build();
        assert_eq!(
            events.last().unwrap()["result"],
            expected_result,
            "{events:?}"
        );
        let deps = project.target.join("debug/deps");
        let bundles: Vec<_> = fs::read_dir(&deps)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "dSYM"))
            .collect();
        assert_eq!(!bundles.is_empty(), expected_bundle, "{split}: {bundles:?}");
        // The bundle is restored in place; its stored pieces stay in the store.
        let pieces: Vec<_> = fs::read_dir(&deps)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".dsym."))
            .collect();
        assert!(pieces.is_empty(), "{split} {expected_result}: {pieces:?}");
        fs::remove_dir_all(&project.target).unwrap();
    }
    project.stop_daemon();
}

#[test]
fn split_debuginfo_off_keeps_cold_and_cached_builds_free_of_debug_bundles() {
    check_debug_bundle_policy("off", false);
}

#[test]
fn unpacked_debuginfo_still_gets_a_relocatable_bundle_on_misses_and_hits() {
    check_debug_bundle_policy("unpacked", cfg!(target_os = "macos"));
}

/// A stored bundle that fails to restore turns the hit into a compile, and the
/// event records the cause under the step that failed (kunobi-ninja/kache#1786).
/// The cause quotes a path from the bundle's manifest, so its newline must
/// reach the event log as a space.
#[test]
fn a_bundle_that_fails_to_restore_recompiles_and_records_the_cause() {
    if !cfg!(target_os = "macos") {
        // Only macOS stores a debug bundle.
        return;
    }
    let project = Project::new("unpacked");
    // A durable entry is checked by size alone on the next lookup, so the
    // swapped manifest below reaches the restore instead of failing its hash.
    let mut config = fs::read_to_string(&project.config).unwrap();
    config.push_str("deferred_durability = false\n");
    fs::write(&project.config, config).unwrap();
    assert_eq!(project.build().last().unwrap()["result"], "miss");
    fs::remove_dir_all(&project.target).unwrap();

    // Swap the stored manifest for one naming a path that leaves the bundle,
    // which the restore rejects. Trailing spaces keep the blob's length.
    let blob = stored_bundle_blob(&project.cache);
    let mut forged =
        br#"{"version":1,"files":[{"path":"Contents\nforged row/..","name":"x"}],"empty_files":[]}"#.to_vec();
    let length = usize::try_from(fs::metadata(&blob).unwrap().len()).unwrap();
    forged.resize(length.max(forged.len()), b' ');
    let staged = blob.with_extension("forged");
    fs::write(&staged, forged).unwrap();
    fs::rename(&staged, &blob).unwrap();

    let (events, stderr) = project.build_with_stderr();
    let event = events.last().unwrap();
    assert_eq!(event["result"], "passthrough", "{events:?}");
    let reason = event["passthrough_reason"].as_str().unwrap();
    assert_eq!(
        reason,
        "restore failed: rustc restore: restoring debug bundle: \
         debug bundle manifest has an unsafe path: Contents forged row/.."
    );
    let warning = stderr
        .lines()
        .find(|line| line.contains("restoring local cache hit for debug_bundle_policy failed:"))
        .unwrap_or_else(|| panic!("restore warning missing: {stderr}"));
    assert!(
        warning.contains("Contents forged row/.. — recompiling"),
        "restore warning contains an unflattened manifest path: {stderr}"
    );
    project.stop_daemon();
}

/// The store blob behind the one debug bundle manifest the cache holds.
fn stored_bundle_blob(cache: &Path) -> PathBuf {
    let store = cache.join("store");
    let blobs: Vec<PathBuf> = fs::read_dir(&store)
        .unwrap()
        .filter_map(|entry| fs::read_to_string(entry.unwrap().path().join("meta.json")).ok())
        .flat_map(|meta| {
            let meta: serde_json::Value = serde_json::from_str(&meta).unwrap();
            let files = meta["files"].as_array().cloned().unwrap_or_default();
            files
                .into_iter()
                .filter(|file| {
                    file["name"]
                        .as_str()
                        .is_some_and(|name| name.ends_with(".dsym.json"))
                })
                .map(|file| file["hash"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        })
        .map(|hash| store.join("blobs").join(&hash[..2]).join(&hash))
        .collect();
    assert_eq!(blobs.len(), 1, "{blobs:?}");
    blobs.into_iter().next().unwrap()
}
