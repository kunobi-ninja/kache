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

/// A `links` crate that builds a bundled C library the way libz-sys does:
/// a pkg-config file naming its `OUT_DIR`, and objects whose debug info names
/// sources under the Cargo home. A dependent reads the pkg-config file
/// through `DEP_*`, as libgit2-sys does. Another target directory must hit
/// both scripts, and the restored file must name the new `OUT_DIR`.
#[test]
fn bundled_c_library_scripts_hit_in_another_target_directory() {
    let fx = fixture_from(|root| {
        let write = |relative: &str, content: &str| {
            let path = root.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        };
        write(
            "Cargo.toml",
            "[workspace]\nmembers = [\"bundled\", \"consumer\"]\nresolver = \"2\"\n",
        );
        write(
            "bundled/Cargo.toml",
            "[package]\nname = \"bundled\"\nversion = \"0.1.0\"\nedition = \"2021\"\nlinks = \"bundled\"\n",
        );
        write("bundled/src/lib.rs", "");
        write(
            "bundled/build.rs",
            r#"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let pkgconfig = out.join("lib/pkgconfig");
    std::fs::create_dir_all(&pkgconfig).unwrap();
    std::fs::write(
        pkgconfig.join("bundled.pc"),
        format!("prefix={}\nlibdir=${{prefix}}/lib\n", out.display()),
    )
    .unwrap();
    let source = format!("{}/registry/src/bundled/adler32.c", std::env::var("CARGO_HOME").unwrap());
    let mut object = b"\x7fELF\0\0".to_vec();
    object.extend_from_slice(source.as_bytes());
    object.push(0);
    std::fs::write(out.join("lib/adler32.o"), object).unwrap();
    println!("cargo:root={}", out.display());
}
"#,
        );
        write(
            "consumer/Cargo.toml",
            "[package]\nname = \"consumer\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nbundled = { path = \"../bundled\" }\n",
        );
        write("consumer/src/lib.rs", "");
        write(
            "consumer/build.rs",
            r#"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let root = std::env::var("DEP_BUNDLED_ROOT").unwrap();
    let pc = std::fs::read_to_string(format!("{root}/lib/pkgconfig/bundled.pc")).unwrap();
    assert_eq!(pc.lines().next().unwrap(), format!("prefix={root}"), "{pc}");
    println!("cargo:warning=consumer read prefix={root}");
}
"#,
        );
        let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
        for entry in walkdir(root) {
            let _ = filetime::set_file_mtime(&entry, old);
        }
    });

    for (name, expected) in [("cold", "miss"), ("warm", "local_hit")] {
        let target = target(&fx, name);
        let mark = event_count(&fx.cache);
        let output = run(&mut cargo(
            "check",
            &fx.workspace,
            &fx.home,
            &fx.cache,
            &target,
            &[],
        ));
        let stderr = String::from_utf8_lossy(&output.stderr);
        let build = target.join("debug/build");
        let out_dir = std::fs::read_dir(&build)
            .unwrap()
            .map(|entry| entry.unwrap().path().join("out"))
            .find(|out| out.join("lib/pkgconfig/bundled.pc").is_file())
            .expect("the bundled OUT_DIR");
        assert!(
            stderr.contains(&format!("consumer read prefix={}", out_dir.display())),
            "{name}: {stderr}"
        );
        assert_eq!(
            std::fs::read_to_string(out_dir.join("lib/pkgconfig/bundled.pc")).unwrap(),
            format!("prefix={}\nlibdir=${{prefix}}/lib\n", out_dir.display()),
            "{name}: the pkg-config file names this OUT_DIR"
        );
        assert_eq!(
            results_for(&events_since(&fx.cache, mark), "build_script_run"),
            vec![expected, expected],
            "{name}"
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
/// `KACHE_TEST_BUNDLED_VALUE` into its OUT_DIR, adds that dir to the search
/// path, and prints `link` (a `cargo:rustc-link-lib` line, or nothing). The
/// object comes from `$RUSTC --emit=obj` and the archive is written by hand, so
/// no C toolchain or `ar` is involved.
fn bundler_build_script(link: &str) -> String {
    format!(
        r##"use std::path::PathBuf;

/// A one-object ar archive. ld64 wants each object on an 8-byte boundary, so
/// for Apple targets the name follows the header, NUL-padded, as Apple's own
/// tools write it.
fn archive(object: &[u8], apple: bool) -> Vec<u8> {{
    let header = |name: &str, size: usize| {{
        format!("{{name:<16}}{{:<12}}{{:<6}}{{:<6}}{{:<8}}{{size:<10}}`\n", 0, 0, 0, 644)
    }};
    let mut bytes = b"!<arch>\n".to_vec();
    if apple {{
        let name = b"value.o\0\0\0\0\0";
        bytes.extend_from_slice(header("#1/12", name.len() + object.len()).as_bytes());
        bytes.extend_from_slice(name);
    }} else {{
        bytes.extend_from_slice(header("value.o/", object.len()).as_bytes());
    }}
    bytes.extend_from_slice(object);
    if bytes.len() % 2 == 1 {{
        bytes.push(b'\n');
    }}
    bytes
}}

fn main() {{
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=KACHE_TEST_BUNDLED_VALUE");
    let value: u32 = std::env::var("KACHE_TEST_BUNDLED_VALUE").unwrap().parse().unwrap();
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let source = out.join("value.rs");
    std::fs::write(
        &source,
        format!("#![no_std]\n#[no_mangle]\npub extern \"C\" fn bundled_value() -> u32 {{{{ {{value}} }}}}\n"),
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
    println!("cargo:rustc-link-search=native={{}}", out.display());
    {link}
}}
"##
    )
}

/// Where the `extern` block that calls the archive's function lives.
#[derive(Clone, Copy, PartialEq)]
enum Caller {
    /// In `bundler`; `app` depends on it.
    Bundler,
    /// In `bundler`; `app` reaches it only through a `mid` library.
    BundlerBehindMid,
    /// In `mid`, which depends on `bundler` for its `-L` alone; `app`
    /// depends on mid.
    Mid,
}

/// A workspace of `bundler` (see [`bundler_build_script`]), a library that
/// calls the archive's function (see [`Caller`]), and `app`, which prints its
/// value. `attribute` goes on the `extern` block.
fn write_bundler_workspace(root: &Path, link: &str, attribute: &str, caller: Caller) {
    let write = |relative: &str, content: &str| {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    let package = |name: &str, dependency: Option<&str>| {
        let mut manifest =
            format!("[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n");
        if let Some(dependency) = dependency {
            manifest.push_str(&format!(
                "\n[dependencies]\n{dependency} = {{ path = \"../{dependency}\" }}\n"
            ));
        }
        manifest
    };
    let mid = caller != Caller::Bundler;
    let members = if mid {
        "[\"bundler\", \"mid\", \"app\"]"
    } else {
        "[\"bundler\", \"app\"]"
    };
    write(
        "Cargo.toml",
        &format!("[workspace]\nmembers = {members}\nresolver = \"2\"\n"),
    );
    write("bundler/Cargo.toml", &package("bundler", None));
    write("bundler/build.rs", &bundler_build_script(link));
    let calling = format!(
        "{attribute}\nextern \"C\" {{\n    fn bundled_value() -> u32;\n}}\n\n\
         pub fn value() -> u32 {{\n    unsafe {{ bundled_value() }}\n}}\n"
    );
    let forwarding = "pub fn value() -> u32 {\n    bundler::value()\n}\n";
    let (bundler, mid_source) = match caller {
        Caller::Bundler | Caller::BundlerBehindMid => (calling.as_str(), forwarding),
        Caller::Mid => (
            "//! Only the build script's search path is used.\n",
            calling.as_str(),
        ),
    };
    write("bundler/src/lib.rs", bundler);
    let direct = if mid {
        write("mid/Cargo.toml", &package("mid", Some("bundler")));
        write("mid/src/lib.rs", mid_source);
        "mid"
    } else {
        "bundler"
    };
    write("app/Cargo.toml", &package("app", Some(direct)));
    write(
        "app/src/main.rs",
        &format!("fn main() {{\n    println!(\"bundled value {{}}\", {direct}::value());\n}}\n"),
    );
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    for entry in walkdir(root) {
        let _ = filetime::set_file_mtime(&entry, old);
    }
}

/// `bundler` names its archive with a link modifier, as scripts that need
/// every object linked do.
fn write_bundling_workspace(root: &Path) {
    write_bundler_workspace(
        root,
        "println!(\"cargo:rustc-link-lib=static:+whole-archive=bundled\");",
        "",
        Caller::Bundler,
    );
}

/// `app` reaches `bundler` through `mid`, and bundler names its archive with
/// a plain `static=` spec.
fn write_transitive_bundling_workspace(root: &Path) {
    write_bundler_workspace(
        root,
        "println!(\"cargo:rustc-link-lib=static=bundled\");",
        "",
        Caller::BundlerBehindMid,
    );
}

/// `bundler` names its archive only in a `#[link]` attribute; its build
/// script adds the search path and no `-l`.
fn write_attribute_bundling_workspace(root: &Path) {
    write_bundler_workspace(
        root,
        "",
        "#[link(name = \"bundled\", kind = \"static\")]",
        Caller::Bundler,
    );
}

/// `mid` names bundler's archive in a `#[link]` attribute, so it bundles an
/// archive from a dependency's OUT_DIR, not its own.
fn write_dependency_attribute_bundling_workspace(root: &Path) {
    write_bundler_workspace(
        root,
        "",
        "#[link(name = \"bundled\", kind = \"static\")]",
        Caller::Mid,
    );
}

/// Build the workspace in `target` with `value` baked into the archive, then
/// run `app`. Returns each named crate's results and what app printed. The
/// binary is cached too, so a stale restore of it would show.
fn build_bundled(
    fx: &Fixture,
    target: &Path,
    value: &str,
    crates: &[&str],
) -> (Vec<Vec<String>>, String) {
    let mark = event_count(&fx.cache);
    let mut command = cargo(
        "build",
        &fx.workspace,
        &fx.home,
        &fx.cache,
        target,
        &[(BUNDLED_VALUE, value)],
    );
    let base = std::fs::read_to_string(fx.cache.join("config.toml")).unwrap();
    let config = fx.cache.join("config-executables.toml");
    std::fs::write(&config, format!("{base}cache_executables = true\n")).unwrap();
    run(command.env("KACHE_CONFIG", &config));
    let events = events_since(&fx.cache, mark);
    let results = crates
        .iter()
        .map(|name| {
            results_for(&events, name)
                .into_iter()
                .map(String::from)
                .collect()
        })
        .collect();
    let output = Command::new(target.join("debug/app")).output().unwrap();
    assert!(output.status.success(), "app failed: {output:?}");
    let printed = String::from_utf8(output.stdout).unwrap().trim().to_string();
    (results, printed)
}

/// A build script that rewrites its archive in place, named
/// `static:+whole-archive=bundled`. The rlib that bundles the archive must
/// recompile when the archive changes. Restoring the rlib built from the old
/// archive links the old object into the binary (#421).
#[test]
fn a_rebuilt_whole_archive_lib_reaches_the_binary() {
    let fx = fixture_from(write_bundling_workspace);
    let build = |target: &Path, value: &str| -> (Vec<String>, String) {
        let (mut results, printed) = build_bundled(&fx, target, value, &["bundler"]);
        (results.remove(0), printed)
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

/// `app` depends on `mid`, which depends on `bundler`. A rebuilt archive
/// changes bundler's rlib but not its metadata, which is what mid compiles
/// against, so mid's rlib stays byte for byte the same, and app names only mid
/// in its `--extern`s. App links the archive through bundler's rlib, so it
/// must still miss: Cargo hands it bundler's `-L`, and the archive there is
/// part of its key.
#[test]
fn a_rebuilt_archive_two_crates_down_reaches_the_binary() {
    let fx = fixture_from(write_transitive_bundling_workspace);
    let crates = ["bundler", "mid", "app"];
    let results = |values: &[&str]| -> Vec<Vec<String>> {
        values.iter().map(|value| vec![value.to_string()]).collect()
    };

    let (cold, printed) = build_bundled(&fx, &target(&fx, "cold"), "1", &crates);
    assert_eq!(cold, results(&["miss", "miss", "miss"]));
    assert_eq!(printed, "bundled value 1");
    let warm = target(&fx, "warm");
    let (restored, printed) = build_bundled(&fx, &warm, "1", &crates);
    assert_eq!(restored, results(&["local_hit", "local_hit", "local_hit"]));
    assert_eq!(printed, "bundled value 1");

    let (rebuilt, printed) = build_bundled(&fx, &warm, "2", &crates);
    assert_eq!(
        printed, "bundled value 2",
        "the binary must link the rebuilt archive: {rebuilt:?}"
    );
    assert_eq!(rebuilt[0], ["miss"], "bundler bundles the new archive");
    assert_eq!(rebuilt[2], ["miss"], "app links it");
}

/// A `#[link(kind = "static")]` attribute bundles the archive into the rlib
/// with no `-l` on the command line. The archive is in bundler's own OUT_DIR,
/// whose archives key its rlib, so the rlib is stored and restored, and a
/// rebuilt archive re-keys it.
#[test]
fn rlib_bundling_its_own_out_dir_archive_is_keyed() {
    let fx = fixture_from(write_attribute_bundling_workspace);
    let crates = ["bundler", "app"];

    let (cold, printed) = build_bundled(&fx, &target(&fx, "cold"), "1", &crates);
    assert_eq!(cold[0], ["miss"]);
    assert_eq!(printed, "bundled value 1");

    let warm = target(&fx, "warm");
    let (second, printed) = build_bundled(&fx, &warm, "1", &crates);
    assert_eq!(second[0], ["local_hit"], "bundler is restored");
    assert_eq!(printed, "bundled value 1");

    let (rebuilt, printed) = build_bundled(&fx, &warm, "2", &crates);
    assert_eq!(printed, "bundled value 2", "{rebuilt:?}");
    assert_eq!(rebuilt[0], ["miss"], "the rebuilt archive re-keys bundler");
}

/// An attribute in `mid` bundles an archive from bundler's OUT_DIR, which
/// Cargo hands mid only as a search path. The key cannot see that archive, so
/// mid's rlib is not stored: a fresh target directory compiles it again, and
/// a rebuilt archive reaches the binary.
#[test]
fn rlib_bundling_unkeyed_archive_is_not_stored() {
    let fx = fixture_from(write_dependency_attribute_bundling_workspace);
    let crates = ["mid", "app"];

    let (cold, printed) = build_bundled(&fx, &target(&fx, "cold"), "1", &crates);
    assert_eq!(cold[0], ["skipped"], "mid is compiled, not stored");
    assert_eq!(printed, "bundled value 1");

    let warm = target(&fx, "warm");
    let (second, printed) = build_bundled(&fx, &warm, "1", &crates);
    assert_eq!(second[0], ["skipped"], "the second build must compile mid");
    assert_eq!(printed, "bundled value 1");

    let (rebuilt, printed) = build_bundled(&fx, &warm, "2", &crates);
    assert_eq!(printed, "bundled value 2", "{rebuilt:?}");
}

/// `stamped`'s build script reports the `ZERO_AR_DATE` it runs with.
#[cfg(target_os = "macos")]
fn write_stamped_workspace(root: &Path) {
    let write = |relative: &str, content: &str| {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    };
    write(
        "Cargo.toml",
        "[package]\nname = \"stamped\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[workspace]\n",
    );
    write(
        "build.rs",
        r#"fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let seen = std::env::var("ZERO_AR_DATE").unwrap_or_else(|_| "unset".to_string());
    println!("cargo:warning=stamped saw ZERO_AR_DATE={seen}");
}
"#,
    );
    write("src/lib.rs", "");
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    for entry in walkdir(root) {
        let _ = filetime::set_file_mtime(&entry, old);
    }
}

/// Apple `ar` stamps each member with its mtime unless `ZERO_AR_DATE` is set,
/// so a script that archives without it writes new bytes on every run. kache
/// runs build scripts with `ZERO_AR_DATE=1` on macOS, keeps a value the user
/// set, and does not restore a run recorded under another value.
#[cfg(target_os = "macos")]
#[test]
fn build_scripts_run_with_zero_ar_date_on_macos() {
    let fx = fixture_from(write_stamped_workspace);
    for (name, value, seen) in [
        ("default", None, "stamped saw ZERO_AR_DATE=1"),
        ("user", Some("0"), "stamped saw ZERO_AR_DATE=0"),
    ] {
        let mut command = cargo(
            "check",
            &fx.workspace,
            &fx.home,
            &fx.cache,
            &target(&fx, name),
            &[],
        );
        match value {
            Some(value) => command.env("ZERO_AR_DATE", value),
            None => command.env_remove("ZERO_AR_DATE"),
        };
        let mark = event_count(&fx.cache);
        let output = run(&mut command);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(seen), "{name}: {stderr}");
        assert_eq!(
            results_for(&events_since(&fx.cache, mark), "build_script_run"),
            vec!["miss"],
            "{name}: a run recorded under another ZERO_AR_DATE is not restored"
        );
    }
}
