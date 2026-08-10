//! Real-Cargo regression for #368: a workspace-owned input read while a
//! proc-macro provider executes must re-key that provider's direct consumers.

use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

const INPUT_ENV: &str = "KACHE_TEST_368_INPUT";
const BIN_INPUT_ENV: &str = "KACHE_TEST_368_BIN_INPUT";

fn write_workspace(root: &Path) {
    std::fs::create_dir_all(root.join("shared")).unwrap();
    std::fs::write(root.join("shared/value.txt"), "alpha").unwrap();
    std::fs::write(root.join("shared/bin-value.txt"), "alpha-bin").unwrap();
    std::fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["macro-provider", "direct-app", "middle-lib", "transitive-app", "bin-reader-macro", "bin-owner", "unlisted-lib"]
resolver = "2"
"#,
    )
    .unwrap();
    std::fs::write(
        root.join(".kache.toml"),
        r#"[[workspace.extra_inputs]]
crates = ["macro-provider"]
inputs = ["shared/value.txt"]
propagate_to_dependents = true

[[workspace.extra_inputs]]
crates = ["bin-owner"]
inputs = ["shared/bin-value.txt"]
propagate_to_dependents = false
"#,
    )
    .unwrap();

    write_package(
        root,
        "macro-provider",
        r#"[package]
name = "macro-provider"
version = "0.1.0"
edition = "2021"

[lib]
proc-macro = true
"#,
        r#"use proc_macro::TokenStream;

#[proc_macro]
pub fn bake(_input: TokenStream) -> TokenStream {
    let path = std::env::var("KACHE_TEST_368_INPUT").expect("input path");
    let value = std::fs::read_to_string(path).expect("read workspace input");
    format!("{value:?}").parse().expect("string literal")
}
"#,
        "lib.rs",
    );
    write_package(
        root,
        "direct-app",
        r#"[package]
name = "direct-app"
version = "0.1.0"
edition = "2021"

[dependencies]
provider_alias = { package = "macro-provider", path = "../macro-provider" }
"#,
        "fn main() { print!(\"{}\", provider_alias::bake!()); }\n",
        "main.rs",
    );
    write_package(
        root,
        "middle-lib",
        r#"[package]
name = "middle-lib"
version = "0.1.0"
edition = "2021"

[dependencies]
macro-provider = { path = "../macro-provider" }
"#,
        r#"pub const VALUE: &str = macro_provider::bake!();

pub fn value() -> &'static str {
    VALUE
}
"#,
        "lib.rs",
    );
    write_package(
        root,
        "transitive-app",
        r#"[package]
name = "transitive-app"
version = "0.1.0"
edition = "2021"

[dependencies]
middle-lib = { path = "../middle-lib" }
"#,
        "fn main() { print!(\"{}\", middle_lib::value()); }\n",
        "main.rs",
    );
    write_package(
        root,
        "bin-reader-macro",
        r#"[package]
name = "bin-reader-macro"
version = "0.1.0"
edition = "2021"

[lib]
proc-macro = true
"#,
        r#"use proc_macro::TokenStream;

#[proc_macro]
pub fn bake_bin(_input: TokenStream) -> TokenStream {
    let path = std::env::var("KACHE_TEST_368_BIN_INPUT").expect("bin input path");
    let value = std::fs::read_to_string(path).expect("read bin workspace input");
    format!("{value:?}").parse().expect("string literal")
}
"#,
        "lib.rs",
    );
    write_package(
        root,
        "bin-owner",
        r#"[package]
name = "bin-owner"
version = "0.1.0"
edition = "2021"
autolib = false

[dependencies]
bin-reader-macro = { path = "../bin-reader-macro" }
"#,
        "fn main() { print!(\"{}\", bin_reader_macro::bake_bin!()); }\n",
        "main.rs",
    );
    write_package(
        root,
        "unlisted-lib",
        r#"[package]
name = "unlisted-lib"
version = "0.1.0"
edition = "2021"
"#,
        "pub const UNLISTED: &str = \"unchanged\";\n",
        "lib.rs",
    );
}

fn write_package(root: &Path, package: &str, manifest: &str, source: &str, name: &str) {
    let package = root.join(package);
    std::fs::create_dir_all(package.join("src")).unwrap();
    std::fs::write(package.join("Cargo.toml"), manifest).unwrap();
    std::fs::write(package.join("src").join(name), source).unwrap();
}

fn cargo_build(workspace: &Path, target: &Path, cache: &Path) -> Output {
    // Keep the shared helper linked in this test binary, but deliberately do
    // not pin it: project discovery must select `<workspace>/.kache.toml`.
    let _unused_machine_config = isolated_config_path(cache);
    Command::new("cargo")
        .args(["build", "--offline", "--workspace", "--verbose"])
        .current_dir(workspace)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_TERM_COLOR", "never")
        .env("KACHE_CACHE_DIR", cache)
        .env("KACHE_CACHE_EXECUTABLES", "1")
        .env("KACHE_LOG", "kache=debug")
        .env(INPUT_ENV, workspace.join("shared/value.txt"))
        .env(BIN_INPUT_ENV, workspace.join("shared/bin-value.txt"))
        .env_remove("KACHE_CONFIG")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("KACHE_DISABLED")
        .output()
        .expect("run Cargo through Kache")
}

fn assert_build_succeeded(output: &Output) {
    assert!(
        output.status.success(),
        "Cargo build failed.\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_binary(target: &Path, name: &str) -> String {
    let mut binary = target.join("debug").join(name);
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    let output = Command::new(&binary).output().unwrap();
    assert!(output.status.success(), "{} failed", binary.display());
    String::from_utf8(output.stdout).unwrap()
}

fn provider_artifact(target: &Path) -> PathBuf {
    let extension = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    std::fs::read_dir(target.join("debug/deps"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension().is_some_and(|ext| ext == extension)
                && path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .is_some_and(|stem| stem.contains("macro_provider"))
        })
        .unwrap_or_else(|| panic!("macro-provider artifact missing under {}", target.display()))
}

fn artifact_hash(path: &Path) -> String {
    blake3::hash(&std::fs::read(path).unwrap())
        .to_hex()
        .to_string()
}

fn events(cache: &Path) -> Vec<Value> {
    std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn latest_by_crate(events: &[Value]) -> BTreeMap<String, &Value> {
    let mut latest = BTreeMap::new();
    for event in events {
        if let Some(name) = event["crate_name"].as_str() {
            latest.insert(name.to_string(), event);
        }
    }
    latest
}

fn event_key<'a>(events: &'a BTreeMap<String, &Value>, name: &str) -> &'a str {
    events[name]["cache_key"]
        .as_str()
        .unwrap_or_else(|| panic!("{name} event has no cache key: {}", events[name]))
}

fn copy_tree(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn workspace_provider_input_rekeys_direct_and_two_hop_consumers() {
    build_kache();
    let workspace = TempDir::new().unwrap();
    let target = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    write_workspace(workspace.path());

    let cold = cargo_build(workspace.path(), target.path(), cache.path());
    assert_build_succeeded(&cold);
    assert_eq!(run_binary(target.path(), "direct-app"), "alpha");
    assert_eq!(run_binary(target.path(), "transitive-app"), "alpha");
    assert_eq!(run_binary(target.path(), "bin-owner"), "alpha-bin");

    let cold_events = events(cache.path());
    let cold_by_crate = latest_by_crate(&cold_events);
    for name in [
        "macro_provider",
        "direct_app",
        "middle_lib",
        "transitive_app",
        "bin_reader_macro",
        "bin_owner",
        "unlisted_lib",
    ] {
        assert!(
            cold_by_crate.contains_key(name),
            "missing cold event for {name}"
        );
    }
    let cold_provider_artifact = provider_artifact(target.path());
    let cold_provider_hash = artifact_hash(&cold_provider_artifact);

    // The workspace root comes from `.kache.toml`, not this external target.
    // Copying the unchanged sources must therefore retain every key and hit.
    let relocated = TempDir::new().unwrap();
    let relocated_target = TempDir::new().unwrap();
    copy_tree(workspace.path(), relocated.path());
    let relocated_build = cargo_build(relocated.path(), relocated_target.path(), cache.path());
    assert_build_succeeded(&relocated_build);
    assert_eq!(run_binary(relocated_target.path(), "direct-app"), "alpha");
    assert_eq!(
        run_binary(relocated_target.path(), "transitive-app"),
        "alpha"
    );
    assert_eq!(
        run_binary(relocated_target.path(), "bin-owner"),
        "alpha-bin"
    );
    let relocated_events = events(cache.path());
    let relocated_by_crate = latest_by_crate(&relocated_events[cold_events.len()..]);
    for name in [
        "macro_provider",
        "direct_app",
        "middle_lib",
        "transitive_app",
        "bin_reader_macro",
        "bin_owner",
        "unlisted_lib",
    ] {
        assert_eq!(
            event_key(&relocated_by_crate, name),
            event_key(&cold_by_crate, name),
            "relocation changed {name}'s key"
        );
        assert_eq!(relocated_by_crate[name]["result"], "local_hit", "{name}");
    }

    // Warm, no-clean edit: the provider artifact itself is byte-identical,
    // while the macro expansion produced in each direct consumer must change.
    std::fs::write(workspace.path().join("shared/value.txt"), "bravo").unwrap();
    std::fs::write(workspace.path().join("shared/bin-value.txt"), "bravo-bin").unwrap();
    let changed = cargo_build(workspace.path(), target.path(), cache.path());
    assert_build_succeeded(&changed);
    assert_eq!(
        run_binary(target.path(), "direct-app"),
        "bravo",
        "direct consumer restored the stale macro expansion"
    );
    assert_eq!(
        run_binary(target.path(), "transitive-app"),
        "bravo",
        "two-hop consumer restored the stale macro expansion"
    );
    assert_eq!(
        run_binary(target.path(), "bin-owner"),
        "bravo-bin",
        "selected bin restored the stale unselected-macro expansion"
    );

    let changed_events = events(cache.path());
    let changed_by_crate = latest_by_crate(&changed_events[relocated_events.len()..]);
    let changed_provider_artifact = provider_artifact(target.path());
    // MSVC proc-macro DLL relinks are not guaranteed to be byte-reproducible,
    // so byte identity is a Unix proof oracle rather than a Windows invariant.
    // Windows still exercises the output and explicit key/result assertions
    // below, which are the behavior this regression protects.
    if !cfg!(windows) {
        assert_eq!(
            artifact_hash(&changed_provider_artifact),
            cold_provider_hash,
            "the proc-macro provider artifact should be byte-identical"
        );
    }
    for name in [
        "macro_provider",
        "direct_app",
        "middle_lib",
        "transitive_app",
        "bin_owner",
    ] {
        assert_ne!(
            event_key(&changed_by_crate, name),
            event_key(&cold_by_crate, name),
            "input edit did not re-key {name}"
        );
        assert_ne!(
            changed_by_crate[name]["result"], "local_hit",
            "{name} restored a stale key"
        );
    }
}
