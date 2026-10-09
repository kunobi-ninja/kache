//! A proc macro that reads a file or a variable its package's build script
//! declares must not be served from an entry built with the old value.
//!
//! The macro reads `<CARGO_MANIFEST_DIR>/data/value.txt` and `APP_MODE`
//! through `std::fs` and `std::env`, so rustc reports neither. Each package
//! declares them in its build script, so Cargo reruns the script and
//! recompiles the package's units after a change:
//!
//! - `app` calls the macro directly and declares the file relatively;
//! - `app2` reaches it through `facade`, which re-exports it, as Tauri's
//!   `generate_context!` arrives, and declares the file by absolute path;
//! - `app3`'s build script declares nothing, so the package is the input;
//! - `app4` lists a declared directory.

#![cfg(unix)]

use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary, stop_daemon};

/// The units whose keys the declared inputs reach.
const UNITS: [&str; 5] = ["app", "app_cli", "app2", "app3", "app4"];

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

fn package(root: &Path, name: &str, dependencies: &str, extra: &str) {
    write(
        &root.join(name).join("Cargo.toml"),
        &format!(
            "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n{extra}\n\
             [dependencies]\n{dependencies}"
        ),
    );
}

const DECLARING_BUILD_SCRIPT: &str = r#"fn main() {
    println!("cargo:rerun-if-changed=data/value.txt");
    println!("cargo:rerun-if-env-changed=APP_MODE");
}
"#;

fn workspace(root: &Path) {
    write(
        &root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"pm\", \"facade\", \"app\", \"app2\", \"app3\", \"app4\"]\nresolver = \"2\"\n",
    );
    package(root, "pm", "", "[lib]\nproc-macro = true\n");
    write(
        &root.join("pm/src/lib.rs"),
        r#"use proc_macro::TokenStream;

fn package_dir() -> std::path::PathBuf {
    std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir").into()
}

#[proc_macro]
pub fn bake(_input: TokenStream) -> TokenStream {
    let value = std::fs::read_to_string(package_dir().join("data/value.txt")).expect("value");
    let mode = std::env::var("APP_MODE").unwrap_or_else(|_| "unset".to_string());
    format!("{:?}", format!("{}:{mode}", value.trim())).parse().unwrap()
}

#[proc_macro]
pub fn listing(_input: TokenStream) -> TokenStream {
    let mut names: Vec<String> = std::fs::read_dir(package_dir().join("assets"))
        .expect("assets")
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    format!("{:?}", names.join(",")).parse().unwrap()
}
"#,
    );
    package(root, "facade", "pm = { path = \"../pm\" }\n", "");
    write(&root.join("facade/src/lib.rs"), "pub use pm::bake;\n");

    package(root, "app", "pm = { path = \"../pm\" }\n", "");
    write(&root.join("app/build.rs"), DECLARING_BUILD_SCRIPT);
    write(
        &root.join("app/src/lib.rs"),
        "pub const VALUE: &str = pm::bake!();\n",
    );
    write(
        &root.join("app/src/bin/app-cli.rs"),
        "fn main() { print!(\"{}\", app::VALUE); }\n",
    );
    write(&root.join("app/data/value.txt"), "v1\n");

    package(root, "app2", "facade = { path = \"../facade\" }\n", "");
    write(
        &root.join("app2/build.rs"),
        r#"fn main() {
    let manifest = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
    println!("cargo:rerun-if-changed={}", manifest.join("data/value.txt").display());
    println!("cargo:rerun-if-env-changed=APP_MODE");
}
"#,
    );
    write(
        &root.join("app2/src/main.rs"),
        "fn main() { print!(\"{}\", facade::bake!()); }\n",
    );
    write(&root.join("app2/data/value.txt"), "v1\n");

    package(root, "app3", "pm = { path = \"../pm\" }\n", "");
    write(&root.join("app3/build.rs"), "fn main() {}\n");
    write(
        &root.join("app3/src/main.rs"),
        "fn main() { print!(\"{}\", pm::bake!()); }\n",
    );
    write(&root.join("app3/data/value.txt"), "v1\n");

    package(root, "app4", "pm = { path = \"../pm\" }\n", "");
    write(
        &root.join("app4/build.rs"),
        "fn main() { println!(\"cargo:rerun-if-changed=assets\"); }\n",
    );
    write(
        &root.join("app4/src/main.rs"),
        "fn main() { print!(\"{}\", pm::listing!()); }\n",
    );
    write(&root.join("app4/assets/a.txt"), "a");
}

/// Builds `workspace` into `target` and returns the newest event of each of
/// [`UNITS`] this build wrote.
fn build(
    workspace: &Path,
    target: &Path,
    cache: &Path,
    mode: Option<&str>,
) -> BTreeMap<String, Value> {
    let env: Vec<(&str, &str)> = mode.map(|mode| ("APP_MODE", mode)).into_iter().collect();
    build_units(workspace, target, cache, &UNITS, &env)
}

/// Builds `workspace` into `target` with `env` set and `APP_MODE` unset
/// unless `env` sets it, and returns the newest event of each of `units`
/// this build wrote.
fn build_units(
    workspace: &Path,
    target: &Path,
    cache: &Path,
    units: &[&str],
    env: &[(&str, &str)],
) -> BTreeMap<String, Value> {
    let mut command = hermetic_command("cargo", cache, Some(&isolated_config_path(cache)));
    command
        .args(["build", "--offline", "--quiet", "--workspace"])
        .current_dir(workspace)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_CACHE_EXECUTABLES", "1")
        .env_remove("APP_MODE")
        .env_remove("KACHE_BASE_DIR")
        .env_remove("KACHE_DISABLED")
        .env_remove("KACHE_DEFERRED_DISCOVERY")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .envs(env.iter().copied());
    let log = cache.join("events.jsonl");
    let before = std::fs::read_to_string(&log).unwrap_or_default().len();
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "Cargo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let mut latest = BTreeMap::new();
    for line in std::fs::read_to_string(log).unwrap()[before..].lines() {
        let event: Value = serde_json::from_str(line).unwrap();
        if let Some(name) = event["crate_name"].as_str()
            && units.contains(&name)
        {
            latest.insert(name.to_string(), event);
        }
    }
    latest
}

fn run(target: &Path, binary: &str) -> String {
    let output = Command::new(target.join("debug").join(binary))
        .output()
        .unwrap();
    assert!(output.status.success(), "{binary} failed");
    String::from_utf8(output.stdout).unwrap()
}

fn outputs(target: &Path) -> [String; 4] {
    ["app-cli", "app2", "app3", "app4"].map(|binary| run(target, binary))
}

fn key(events: &BTreeMap<String, Value>, unit: &str) -> String {
    events
        .get(unit)
        .unwrap_or_else(|| panic!("no event for {unit}: {events:?}"))["cache_key"]
        .as_str()
        .unwrap_or_else(|| panic!("{unit} has no key: {}", events[unit]))
        .to_string()
}

fn result<'a>(events: &'a BTreeMap<String, Value>, unit: &str) -> &'a str {
    events[unit]["result"].as_str().unwrap()
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

/// Rewrites `path` with `body` and a modification time Cargo sees as newer
/// than the last build, however coarse the filesystem clock.
fn edit(path: &Path, body: &str) {
    std::fs::write(path, body).unwrap();
    let later = filetime::FileTime::from_system_time(
        std::time::SystemTime::now() + std::time::Duration::from_secs(2),
    );
    filetime::set_file_mtime(path, later).unwrap();
}

#[test]
fn declared_inputs_of_a_packages_build_script_key_its_units() {
    build_kache();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let cache = root.join("cache");
    let _daemon = CacheGuard(cache.clone());
    let checkout = root.join("checkout");
    workspace(&checkout);
    let target = root.join("target");

    let cold = build(&checkout, &target, &cache, None);
    assert_eq!(
        outputs(&target),
        ["v1:unset", "v1:unset", "v1:unset", "a.txt"].map(String::from)
    );

    // Another target directory and another checkout find the same keys.
    let elsewhere = build(&checkout, &root.join("target2"), &cache, None);
    let moved = root.join("moved");
    copy_tree(&checkout, &moved);
    let relocated = build(&moved, &root.join("target3"), &cache, None);
    for unit in UNITS {
        assert_eq!(key(&elsewhere, unit), key(&cold, unit), "{unit}");
        assert_eq!(result(&elsewhere, unit), "local_hit", "{unit}");
        assert_eq!(key(&relocated, unit), key(&cold, unit), "{unit} moved");
        assert_eq!(result(&relocated, unit), "local_hit", "{unit} moved");
    }

    // A declared file changes: Cargo recompiles, and kache must not restore.
    for package in ["app", "app2", "app3"] {
        edit(&checkout.join(package).join("data/value.txt"), "v2\n");
    }
    edit(&checkout.join("app4/assets/b.txt"), "b");
    let edited = build(&checkout, &target, &cache, None);
    assert_eq!(
        outputs(&target),
        ["v2:unset", "v2:unset", "v2:unset", "a.txt,b.txt"].map(String::from),
        "a stale expansion was restored"
    );
    for unit in UNITS {
        assert_ne!(key(&edited, unit), key(&cold, unit), "{unit}");
        assert_eq!(result(&edited, unit), "miss", "{unit}");
    }

    // A declared variable changes, then goes back.
    let set = build(&checkout, &target, &cache, Some("x"));
    assert_eq!(&outputs(&target)[..2], ["v2:x", "v2:x"]);
    for unit in ["app", "app_cli", "app2"] {
        assert_ne!(key(&set, unit), key(&edited, unit), "{unit}");
        assert_eq!(result(&set, unit), "miss", "{unit}");
    }
    let unset = build(&checkout, &target, &cache, None);
    assert_eq!(&outputs(&target)[..2], ["v2:unset", "v2:unset"]);
    for unit in ["app", "app_cli", "app2"] {
        assert_eq!(key(&unset, unit), key(&edited, unit), "{unit}");
        assert_eq!(result(&unset, unit), "local_hit", "{unit}");
    }

    // The same bytes written again: Cargo recompiles, and the entry is good.
    for package in ["app", "app2", "app3"] {
        edit(&checkout.join(package).join("data/value.txt"), "v2\n");
    }
    let touched = build(&checkout, &target, &cache, None);
    for unit in ["app", "app_cli", "app2", "app3"] {
        assert_eq!(key(&touched, unit), key(&edited, unit), "{unit}");
        assert_eq!(result(&touched, unit), "local_hit", "{unit}");
    }
    assert_eq!(
        outputs(&target),
        ["v2:unset", "v2:unset", "v2:unset", "a.txt,b.txt"].map(String::from)
    );
}

/// The key folds the declared inputs as they were before the compile. A
/// macro that rewrites its declared file while it expands, then reads it,
/// leaves an artifact that key does not describe, so it must not be stored.
#[test]
fn a_declared_input_that_moves_during_the_compile_is_not_stored() {
    build_kache();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().canonicalize().unwrap();
    let cache = root.join("cache");
    let _daemon = CacheGuard(cache.clone());
    let checkout = root.join("checkout");
    write(
        &checkout.join("Cargo.toml"),
        "[workspace]\nmembers = [\"pm\", \"live\"]\nresolver = \"2\"\n",
    );
    package(&checkout, "pm", "", "[lib]\nproc-macro = true\n");
    write(
        &checkout.join("pm/src/lib.rs"),
        r#"use proc_macro::TokenStream;

#[proc_macro]
pub fn rewrite(_input: TokenStream) -> TokenStream {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir");
    let path = std::path::PathBuf::from(manifest).join("data/value.txt");
    std::fs::write(&path, "rewritten\n").expect("write");
    let value = std::fs::read_to_string(&path).expect("value");
    format!("{:?}", value.trim()).parse().unwrap()
}
"#,
    );
    package(&checkout, "live", "pm = { path = \"../pm\" }\n", "");
    write(
        &checkout.join("live/build.rs"),
        "fn main() { println!(\"cargo:rerun-if-changed=data/value.txt\"); }\n",
    );
    write(
        &checkout.join("live/src/lib.rs"),
        "pub const VALUE: &str = pm::rewrite!();\n",
    );
    let value = checkout.join("live/data/value.txt");
    write(&value, "v1\n");
    let target = root.join("target");

    // A fresh cache compiles before the key and keys from what the compile
    // wrote, with the inputs resolved before it.
    let first = build_units(&checkout, &target, &cache, &["live"], &[]);
    assert_eq!(result(&first, "live"), "skipped", "{}", first["live"]);
    assert_eq!(
        first["live"]["skip_reason"], "build-script-inputs-changed",
        "{}",
        first["live"]
    );
    // The original content keys the same and finds nothing stored, whether
    // the compile runs before the key or after a pre-pass.
    for deferred in ["1", "0"] {
        edit(&value, "v1\n");
        let again = build_units(
            &checkout,
            &target,
            &cache,
            &["live"],
            &[("KACHE_DEFERRED_DISCOVERY", deferred)],
        );
        assert_eq!(key(&again, "live"), key(&first, "live"), "{deferred}");
        assert_eq!(result(&again, "live"), "skipped", "{}", again["live"]);
    }
}
