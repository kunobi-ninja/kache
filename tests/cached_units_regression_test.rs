//! Regression tests for the cached-unit paths that a single `cargo` process
//! cannot exercise: several jobs sharing one store, build-script runs
//! restored from the store, Clippy units served from the store, and misses
//! keyed from the compile's own dep-info.
//!
//! Every test drives real `cargo` through `RUSTC_WRAPPER=kache` against a
//! small workspace written into a temporary directory, with the store,
//! runtime and configuration pinned under one cache directory, and reads
//! `events.jsonl` to check what kache did.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;
use tempfile::TempDir;

#[allow(dead_code)]
mod common;
use common::{hermetic_command, kache_binary};

/// A workspace with the unit shapes the cache treats differently: a plain
/// library, a library with a build script (an `OUT_DIR`), a proc-macro and a
/// crate depending on all of them.
fn write_workspace(root: &Path) {
    let write = |relative: &str, content: &str| {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write(
        "Cargo.toml",
        "[workspace]\nmembers = [\"leaf\", \"withbuild\", \"shout\", \"app\"]\nresolver = \"2\"\n",
    );
    write(
        "leaf/Cargo.toml",
        "[package]\nname = \"leaf\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    );
    write(
        "leaf/src/lib.rs",
        "mod extra;\npub fn leaf() -> u32 { extra::extra() + 1 }\n",
    );
    write("leaf/src/extra.rs", "pub fn extra() -> u32 { 40 }\n");
    write(
        "withbuild/Cargo.toml",
        "[package]\nname = \"withbuild\"\nversion = \"0.1.0\"\nedition = \"2021\"\nbuild = \"build.rs\"\n",
    );
    write(
        "withbuild/build.rs",
        r#"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/data.txt");
    println!("cargo:rerun-if-env-changed=WITHBUILD_FLAVOUR");
    let data = std::fs::read_to_string("src/data.txt").unwrap();
    let flavour = std::env::var("WITHBUILD_FLAVOUR").unwrap_or_default();
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    std::fs::create_dir_all(out.join("nested")).unwrap();
    std::fs::write(
        out.join("gen.rs"),
        format!("pub const DATA: &str = {data:?};\npub const FLAVOUR: &str = {flavour:?};\n"),
    )
    .unwrap();
    std::fs::write(out.join("nested/marker.txt"), "marker\n").unwrap();
    println!("cargo:warning=withbuild ran for {}", data.trim());
}
"#,
    );
    write("withbuild/src/data.txt", "alpha\n");
    write(
        "withbuild/src/lib.rs",
        "include!(concat!(env!(\"OUT_DIR\"), \"/gen.rs\"));\npub fn data() -> &'static str { DATA }\n",
    );
    write(
        "shout/Cargo.toml",
        "[package]\nname = \"shout\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\nproc-macro = true\n",
    );
    write(
        "shout/src/lib.rs",
        "use proc_macro::TokenStream;\n#[proc_macro]\npub fn shout(input: TokenStream) -> TokenStream { input }\n",
    );
    write(
        "app/Cargo.toml",
        "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nleaf = { path = \"../leaf\" }\nwithbuild = { path = \"../withbuild\" }\nshout = { path = \"../shout\" }\n",
    );
    write(
        "app/src/lib.rs",
        "pub fn answer() -> u32 { shout::shout!(leaf::leaf()) + withbuild::data().len() as u32 }\n",
    );
    // Old timestamps: the tree memo only memoises settled trees, and Cargo's
    // freshness must not see sources newer than the outputs.
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    for entry in walkdir(root) {
        let _ = filetime::set_file_mtime(&entry, old);
    }
}

fn walkdir(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path.clone());
            }
            out.push(path);
        }
    }
    out
}

fn write_config(cache: &Path) -> PathBuf {
    let config = cache.join("config.toml");
    let quoted = toml::Value::String(cache.to_string_lossy().into_owned()).to_string();
    std::fs::write(
        &config,
        format!(
            "[cache]\nlocal_only = true\nignore_env = true\ninput_predictions = true\n\
             scheduler = true\nlocal_store = {quoted}\nruntime_dir = {quoted}\n"
        ),
    )
    .unwrap();
    config
}

/// `cargo <subcommand>` on the workspace through kache, with its own target
/// directory and the shared cache directory.
fn cargo(
    subcommand: &str,
    workspace: &Path,
    home: &Path,
    cache: &Path,
    target: &Path,
    env: &[(&str, &str)],
) -> Command {
    let mut command = hermetic_command(env!("CARGO"), cache, Some(&write_config(cache)));
    command
        .arg(subcommand)
        .current_dir(workspace)
        .env("HOME", home)
        .env("CARGO_HOME", home.join(".cargo"))
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_LOG", "off")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        .env_remove("WITHBUILD_FLAVOUR")
        .env_remove("KACHE_BUILD_SCRIPT_CACHE");
    for (name, value) in env {
        command.env(name, value);
    }
    command
}

fn run(command: &mut Command) -> Output {
    let output = command.output().expect("spawning cargo");
    assert!(
        output.status.success(),
        "cargo failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// Events written since `mark`, oldest first.
fn events_since(cache: &Path, mark: usize) -> Vec<Value> {
    let path = cache.join("events.jsonl");
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    text.lines()
        .skip(mark)
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("event json"))
        .collect()
}

fn event_count(cache: &Path) -> usize {
    std::fs::read_to_string(cache.join("events.jsonl"))
        .map(|text| text.lines().count())
        .unwrap_or(0)
}

fn field<'a>(event: &'a Value, name: &str) -> &'a str {
    event[name].as_str().unwrap_or("")
}

fn number(event: &Value, name: &str) -> u64 {
    event[name].as_u64().unwrap_or(0)
}

fn rustc_units(events: &[Value]) -> Vec<&Value> {
    events
        .iter()
        .filter(|e| {
            let name = field(e, "crate_name");
            !name.starts_with("build_script")
                && !name.ends_with(".c")
                && name != "unknown"
                && name != "___"
        })
        .collect()
}

fn results_for<'a>(events: &'a [Value], crate_name: &str) -> Vec<&'a str> {
    events
        .iter()
        .filter(|e| field(e, "crate_name") == crate_name)
        .map(|e| field(e, "result"))
        .collect()
}

struct Fixture {
    _dirs: Vec<TempDir>,
    workspace: PathBuf,
    home: PathBuf,
    cache: PathBuf,
}

fn fixture() -> Fixture {
    fixture_from(write_workspace)
}

/// A fixture around the workspace `write` lays out.
fn fixture_from(write: fn(&Path)) -> Fixture {
    let workspace = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    write(workspace.path());
    std::fs::create_dir_all(home.path().join(".cargo")).unwrap();
    Fixture {
        workspace: workspace.path().to_path_buf(),
        home: home.path().to_path_buf(),
        cache: cache.path().to_path_buf(),
        _dirs: vec![workspace, home, cache],
    }
}

fn target(fixture: &Fixture, name: &str) -> PathBuf {
    let dir = fixture.cache.parent().unwrap().join(format!(
        "kache-target-{}-{name}",
        fixture.cache.file_name().unwrap().to_string_lossy()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Two `cargo check`s of the same workspace at once, each with its own
/// target directory and one shared store, then a third in a fresh target
/// directory. The shape that broke twice during development: a unit compiled
/// before its key was known must never be compiled or replayed twice within
/// one job (Cargo panics in its dependency queue when a unit finishes twice),
/// and the third build must be served from the store.
#[test]
fn concurrent_jobs_share_one_store_without_repeating_a_compile() {
    let fx = fixture();
    let (a, b, c) = (target(&fx, "a"), target(&fx, "b"), target(&fx, "c"));
    let mark = event_count(&fx.cache);

    let first = cargo("check", &fx.workspace, &fx.home, &fx.cache, &a, &[])
        .spawn()
        .unwrap();
    let second = cargo("check", &fx.workspace, &fx.home, &fx.cache, &b, &[])
        .spawn()
        .unwrap();
    let first = first.wait_with_output().unwrap();
    let second = second.wait_with_output().unwrap();
    for (name, output) in [("first", first), ("second", second)] {
        assert!(
            output.status.success(),
            "{name} cargo check failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("panicked"),
            "{name} cargo panicked:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let events = events_since(&fx.cache, mark);
    // Two jobs may each compile a unit once; nothing may compile more often
    // than there are jobs (a unit compiled, then compiled again uncached,
    // is what made Cargo see it finish twice).
    let mut runs = std::collections::HashMap::new();
    for event in rustc_units(&events) {
        *runs
            .entry(field(event, "crate_name").to_string())
            .or_insert(0) += number(event, "compiler_runs");
    }
    let repeated: Vec<_> = runs.iter().filter(|(_, n)| **n > 2).collect();
    assert!(
        repeated.is_empty(),
        "units compiled more often than there are jobs: {repeated:?}"
    );
    assert!(
        runs.values().any(|n| *n == 1),
        "at least one unit was compiled by one job and served to the other: {runs:?}"
    );

    let mark = event_count(&fx.cache);
    run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &c,
        &[],
    ));
    let events = events_since(&fx.cache, mark);
    for event in rustc_units(&events) {
        assert_eq!(
            field(event, "result"),
            "local_hit",
            "{} in a fresh target directory should be a hit: {event}",
            field(event, "crate_name")
        );
        assert_eq!(number(event, "compiler_runs"), 0, "{event}");
    }
    assert_eq!(results_for(&events, "build_script_run"), vec!["local_hit"]);
}

/// A miss with no closure record and no remote keys from the compile's own
/// dep-info instead of a pre-pass; the record it leaves lets the next fresh
/// target directory hit, and a module edit produces a new key.
#[test]
fn record_less_misses_key_from_the_emitted_dep_info() {
    let fx = fixture();
    let mark = event_count(&fx.cache);
    run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "cold"),
        &[],
    ));
    let events = events_since(&fx.cache, mark);
    let leaf: Vec<&Value> = events
        .iter()
        .filter(|e| field(e, "crate_name") == "leaf")
        .collect();
    assert_eq!(leaf.len(), 1, "one leaf unit: {leaf:?}");
    assert_eq!(field(leaf[0], "result"), "miss");
    assert_eq!(
        number(leaf[0], "dep_info_runs"),
        0,
        "no pre-pass on a certain miss"
    );
    let cold_key = field(leaf[0], "cache_key").to_string();

    let mark = event_count(&fx.cache);
    run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "warm"),
        &[],
    ));
    let events = events_since(&fx.cache, mark);
    assert_eq!(results_for(&events, "leaf"), vec!["local_hit"]);

    let extra = fx.workspace.join("leaf/src/extra.rs");
    std::fs::write(&extra, "pub fn extra() -> u32 { 41 }\n").unwrap();
    let mark = event_count(&fx.cache);
    run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "edited"),
        &[],
    ));
    let events = events_since(&fx.cache, mark);
    let leaf: Vec<&Value> = events
        .iter()
        .filter(|e| field(e, "crate_name") == "leaf")
        .collect();
    assert_eq!(field(leaf[0], "result"), "miss", "a changed module misses");
    assert_ne!(field(leaf[0], "cache_key"), cold_key);
}

/// A build-script run is restored from the store with its OUT_DIR intact and
/// its declared inputs and variables in the key; the switch turns it off.
#[test]
fn build_script_runs_are_restored_and_keyed_on_their_declarations() {
    let fx = fixture();
    let out_dir_contents = |target: &Path| -> Vec<(String, String)> {
        let mut found = Vec::new();
        for build in std::fs::read_dir(target.join("debug/build")).unwrap() {
            let build = build.unwrap().path();
            if !build
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("withbuild-")
            {
                continue;
            }
            let out = build.join("out");
            if !out.is_dir() {
                continue;
            }
            for entry in walkdir(&out) {
                if entry.is_file() {
                    found.push((
                        entry.strip_prefix(&out).unwrap().display().to_string(),
                        std::fs::read_to_string(&entry).unwrap(),
                    ));
                }
            }
        }
        found.sort();
        found
    };

    let cold = target(&fx, "cold");
    let mark = event_count(&fx.cache);
    let output = run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &cold,
        &[],
    ));
    assert!(String::from_utf8_lossy(&output.stderr).contains("withbuild ran for alpha"));
    assert_eq!(
        results_for(&events_since(&fx.cache, mark), "build_script_run"),
        vec!["miss"]
    );
    let cold_out = out_dir_contents(&cold);
    assert_eq!(
        cold_out.len(),
        2,
        "gen.rs and nested/marker.txt: {cold_out:?}"
    );

    let warm = target(&fx, "warm");
    let mark = event_count(&fx.cache);
    let output = run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &warm,
        &[],
    ));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("withbuild ran for alpha"),
        "the recorded warning is replayed on a hit"
    );
    assert_eq!(
        results_for(&events_since(&fx.cache, mark), "build_script_run"),
        vec!["local_hit"]
    );
    assert_eq!(
        out_dir_contents(&warm),
        cold_out,
        "OUT_DIR restored byte for byte"
    );

    // A declared input changes: the run must miss and the output follow.
    std::fs::write(fx.workspace.join("withbuild/src/data.txt"), "beta\n").unwrap();
    let mark = event_count(&fx.cache);
    let output = run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "data"),
        &[],
    ));
    assert!(String::from_utf8_lossy(&output.stderr).contains("withbuild ran for beta"));
    assert_eq!(
        results_for(&events_since(&fx.cache, mark), "build_script_run"),
        vec!["miss"]
    );

    // A declared variable changes: miss again.
    let mark = event_count(&fx.cache);
    run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "flavour"),
        &[("WITHBUILD_FLAVOUR", "spicy")],
    ));
    assert_eq!(
        results_for(&events_since(&fx.cache, mark), "build_script_run"),
        vec!["miss"]
    );
    let mark = event_count(&fx.cache);
    run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "flavour-again"),
        &[("WITHBUILD_FLAVOUR", "spicy")],
    ));
    assert_eq!(
        results_for(&events_since(&fx.cache, mark), "build_script_run"),
        vec!["local_hit"]
    );

    // Switched off: the script runs and no run event is recorded.
    let mark = event_count(&fx.cache);
    let output = run(&mut cargo(
        "check",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "off"),
        &[("KACHE_BUILD_SCRIPT_CACHE", "0")],
    ));
    assert!(String::from_utf8_lossy(&output.stderr).contains("withbuild ran for beta"));
    assert!(results_for(&events_since(&fx.cache, mark), "build_script_run").is_empty());
}

/// Cargo passes a `links` dependency's metadata to its dependents as
/// `DEP_<LINKS>_<KEY>`, spelling the key as the script printed it. Tauri prints
/// `cargo:core:window__CORE_PLUGIN___PERMISSION_FILES_PATH=...`, and under a
/// `/bin/sh` launcher dash dropped the resulting variable before the dependent
/// script ever ran. Both runs must see every key, cold and restored.
#[test]
fn links_metadata_reaches_dependent_build_scripts_whatever_its_spelling() {
    let workspace = TempDir::new().unwrap();
    let home = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let root = workspace.path();
    let write = |relative: &str, content: &str| {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write(
        "Cargo.toml",
        "[workspace]\nmembers = [\"linked\", \"consumer\"]\nresolver = \"2\"\n",
    );
    write(
        "linked/Cargo.toml",
        "[package]\nname = \"linked\"\nversion = \"0.1.0\"\nedition = \"2021\"\nlinks = \"linked\"\n",
    );
    write("linked/src/lib.rs", "");
    write(
        "linked/build.rs",
        r#"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let files = out.join("core-window-permission-files");
    std::fs::write(&files, "[]").unwrap();
    println!("cargo:core:window__CORE_PLUGIN___PERMISSION_FILES_PATH={}", files.display());
    println!("cargo:dashed-key.with.dots=yes");
    println!("cargo:plain=yes");
}
"#,
    );
    write(
        "consumer/Cargo.toml",
        "[package]\nname = \"consumer\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nlinked = { path = \"../linked\" }\n",
    );
    write("consumer/src/lib.rs", "");
    write(
        "consumer/build.rs",
        r#"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    for name in [
        "DEP_LINKED_CORE:WINDOW__CORE_PLUGIN___PERMISSION_FILES_PATH",
        "DEP_LINKED_DASHED_KEY.WITH.DOTS",
        "DEP_LINKED_PLAIN",
    ] {
        let Some(value) = std::env::var_os(name) else {
            let seen: Vec<String> = std::env::vars_os()
                .filter_map(|(name, _)| name.into_string().ok())
                .filter(|name| name.starts_with("DEP_"))
                .collect();
            panic!("{name} is missing; the script saw {seen:?}");
        };
        if name.ends_with("_PATH") {
            assert!(std::path::Path::new(&value).is_file(), "{value:?}");
        }
    }
    println!("cargo:warning=consumer saw every linked key");
}
"#,
    );
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    for entry in walkdir(root) {
        let _ = filetime::set_file_mtime(&entry, old);
    }
    std::fs::create_dir_all(home.path().join(".cargo")).unwrap();
    let fx = Fixture {
        workspace: root.to_path_buf(),
        home: home.path().to_path_buf(),
        cache: cache.path().to_path_buf(),
        _dirs: vec![workspace, home, cache],
    };

    for (name, expected) in [("cold", "miss"), ("warm", "local_hit")] {
        let mark = event_count(&fx.cache);
        let output = run(&mut cargo(
            "check",
            &fx.workspace,
            &fx.home,
            &fx.cache,
            &target(&fx, name),
            &[],
        ));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("consumer saw every linked key"),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            results_for(&events_since(&fx.cache, mark), "build_script_run"),
            vec![expected, expected],
            "{name}: both scripts go through the launcher"
        );
    }
}

/// `cargo clippy` units are served from the store with their diagnostics,
/// and a change to the lint arguments is a different key.
#[test]
fn clippy_units_hit_with_their_diagnostics() {
    let clippy = Command::new(env!("CARGO"))
        .args(["clippy", "--version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !clippy {
        eprintln!("skipping: cargo clippy is not installed");
        return;
    }
    let fx = fixture();
    std::fs::write(
        fx.workspace.join("leaf/src/extra.rs"),
        "pub fn extra() -> u32 { return 40; }\n",
    )
    .unwrap();

    let mark = event_count(&fx.cache);
    let cold = run(&mut cargo(
        "clippy",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "cold"),
        &[],
    ));
    let cold_err = String::from_utf8_lossy(&cold.stderr).into_owned();
    assert!(cold_err.contains("needless_return"), "{cold_err}");
    let events = events_since(&fx.cache, mark);
    assert_eq!(results_for(&events, "leaf"), vec!["miss"]);

    let mark = event_count(&fx.cache);
    let warm = run(&mut cargo(
        "clippy",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "warm"),
        &[],
    ));
    let warm_err = String::from_utf8_lossy(&warm.stderr).into_owned();
    assert_eq!(
        results_for(&events_since(&fx.cache, mark), "leaf"),
        vec!["local_hit"]
    );
    let warning = |text: &str| -> String {
        text.lines()
            .filter(|line| line.contains("needless_return") || line.contains("unneeded `return`"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(
        warning(&warm_err),
        warning(&cold_err),
        "the hit replays the lint"
    );

    // Different lint arguments are a different unit.
    let mark = event_count(&fx.cache);
    let mut allowed = cargo(
        "clippy",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        &target(&fx, "allowed"),
        &[],
    );
    allowed.args(["--", "-A", "clippy::needless_return"]);
    let allowed = run(&mut allowed);
    assert!(!String::from_utf8_lossy(&allowed.stderr).contains("needless_return"));
    assert_eq!(
        results_for(&events_since(&fx.cache, mark), "leaf"),
        vec!["miss"]
    );
}

/// The variable `bundler`'s build script bakes into its archive.
const BUNDLED_VALUE: &str = "KACHE_TEST_BUNDLED_VALUE";

/// `bundler`'s build script writes a one-object archive whose function returns
/// `KACHE_TEST_BUNDLED_VALUE`, and names it with a link modifier, as scripts
/// that need every object linked do. Its library bundles the archive and
/// `app` prints the value. The object comes from `$RUSTC --emit=obj` and the
/// archive is written by hand, so no C toolchain or `ar` is involved.
fn write_bundling_workspace(root: &Path) {
    let write = |relative: &str, content: &str| {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write(
        "Cargo.toml",
        "[workspace]\nmembers = [\"bundler\", \"app\"]\nresolver = \"2\"\n",
    );
    write(
        "bundler/Cargo.toml",
        "[package]\nname = \"bundler\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    );
    write(
        "bundler/build.rs",
        r##"use std::path::PathBuf;

/// A one-object ar archive. ld64 wants each object on an 8-byte boundary, so
/// for Apple targets the name follows the header, NUL-padded, as Apple's own
/// tools write it.
fn archive(object: &[u8], apple: bool) -> Vec<u8> {
    let header = |name: &str, size: usize| {
        format!("{name:<16}{:<12}{:<6}{:<6}{:<8}{size:<10}`\n", 0, 0, 0, 644)
    };
    let mut bytes = b"!<arch>\n".to_vec();
    if apple {
        let name = b"value.o\0\0\0\0\0";
        bytes.extend_from_slice(header("#1/12", name.len() + object.len()).as_bytes());
        bytes.extend_from_slice(name);
    } else {
        bytes.extend_from_slice(header("value.o/", object.len()).as_bytes());
    }
    bytes.extend_from_slice(object);
    if bytes.len() % 2 == 1 {
        bytes.push(b'\n');
    }
    bytes
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=KACHE_TEST_BUNDLED_VALUE");
    let value: u32 = std::env::var("KACHE_TEST_BUNDLED_VALUE").unwrap().parse().unwrap();
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let source = out.join("value.rs");
    std::fs::write(
        &source,
        format!("#![no_std]\n#[no_mangle]\npub extern \"C\" fn bundled_value() -> u32 {{ {value} }}\n"),
    )
    .unwrap();
    let object = out.join("value.o");
    let status = std::process::Command::new(std::env::var("RUSTC").unwrap())
        .args(["--crate-type=lib", "--crate-name=value", "--emit=obj"])
        .args(["-Cpanic=abort", "-Ccodegen-units=1", "--target"])
        .arg(std::env::var("TARGET").unwrap())
        .arg("-o")
        .arg(&object)
        .arg(&source)
        .status()
        .unwrap();
    assert!(status.success(), "compiling the archive's object failed");
    let apple = std::env::var("CARGO_CFG_TARGET_VENDOR").unwrap() == "apple";
    let bytes = archive(&std::fs::read(&object).unwrap(), apple);
    std::fs::write(out.join("libbundled.a"), bytes).unwrap();
    println!("cargo:rustc-link-search=native={}", out.display());
    println!("cargo:rustc-link-lib=static:+whole-archive=bundled");
}
"##,
    );
    write(
        "bundler/src/lib.rs",
        "extern \"C\" {\n    fn bundled_value() -> u32;\n}\n\n\
         pub fn value() -> u32 {\n    unsafe { bundled_value() }\n}\n",
    );
    write(
        "app/Cargo.toml",
        "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nbundler = { path = \"../bundler\" }\n",
    );
    write(
        "app/src/main.rs",
        "fn main() {\n    println!(\"bundled value {}\", bundler::value());\n}\n",
    );
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    for entry in walkdir(root) {
        let _ = filetime::set_file_mtime(&entry, old);
    }
}

/// A build script that rewrites its archive in place, named
/// `static:+whole-archive=bundled`. The rlib that bundles the archive must
/// recompile when the archive changes. Restoring the rlib built from the old
/// archive links the old object into the binary (#421).
#[test]
fn a_rebuilt_whole_archive_lib_reaches_the_binary() {
    let fx = fixture_from(write_bundling_workspace);
    let build = |target: &Path, value: &str| -> (Vec<String>, String) {
        let mark = event_count(&fx.cache);
        run(&mut cargo(
            "build",
            &fx.workspace,
            &fx.home,
            &fx.cache,
            target,
            &[(BUNDLED_VALUE, value)],
        ));
        let events = events_since(&fx.cache, mark);
        let results = results_for(&events, "bundler")
            .into_iter()
            .map(String::from)
            .collect();
        let output = Command::new(target.join("debug/app")).output().unwrap();
        assert!(output.status.success(), "app failed: {output:?}");
        let printed = String::from_utf8(output.stdout).unwrap().trim().to_string();
        (results, printed)
    };

    let cold = target(&fx, "cold");
    assert_eq!(
        build(&cold, "1"),
        (vec!["miss".to_string()], "bundled value 1".to_string())
    );
    // A fresh target directory restores the library, so the rebuild below
    // cannot pass because the library was never cached.
    let warm = target(&fx, "warm");
    assert_eq!(
        build(&warm, "1"),
        (vec!["local_hit".to_string()], "bundled value 1".to_string())
    );
    // The declared variable changes: Cargo reruns the script, which rewrites
    // the archive under the same name, and recompiles the library.
    assert_eq!(
        build(&warm, "2"),
        (vec!["miss".to_string()], "bundled value 2".to_string()),
        "the library must recompile against the rebuilt archive"
    );
}
