//! Real Cargo regression for #723: `extra_inputs` must participate in Cargo's
//! warm-target freshness, not only in Kache's key after the wrapper is invoked.

use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};
use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

const INPUT_PATH_ENV: &str = "KACHE_TEST_DECLARED_INPUT";
const ACTIVATE_CONFIG_ENV: &str = "KACHE_TEST_ACTIVATE_CONFIG";
const CRATE_NAME: &str = "extra_input_warm_target";

fn rustc_path() -> String {
    std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string())
}

/// Build fixed proc-macro scaffolding outside Cargo and outside Kache. Its
/// expansion reads the declared input without using `include_*`, so rustc does
/// not discover the file on its own and the consumer's normal dep-info omits
/// it. This is the minimal shape of an offline sqlx-style compile-time input.
fn build_input_reader(root: &Path) -> PathBuf {
    let source = root.join("input_reader.rs");
    std::fs::write(
        &source,
        r#"
extern crate proc_macro;
use proc_macro::TokenStream;

#[proc_macro]
pub fn declared_value(_input: TokenStream) -> TokenStream {
    let path = std::env::var("KACHE_TEST_DECLARED_INPUT").unwrap();
    let value = std::fs::read_to_string(path).unwrap_or_else(|_| "missing".to_string());
    format!("{value:?}").parse().unwrap()
}
"#,
    )
    .unwrap();

    let out_dir = root.join("input-reader-out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let output = Command::new(rustc_path())
        .args([
            "--crate-name",
            "input_reader",
            "--crate-type",
            "proc-macro",
            "--edition",
            "2021",
            "--out-dir",
        ])
        .arg(&out_dir)
        .arg(&source)
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .output()
        .expect("build input-reader proc macro");
    assert!(
        output.status.success(),
        "building input-reader proc macro failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let dylib_ext = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    std::fs::read_dir(&out_dir)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.extension().is_some_and(|ext| ext == dylib_ext)
                && path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains("input_reader"))
        })
        .unwrap_or_else(|| panic!("input-reader {dylib_ext} missing in {}", out_dir.display()))
}

/// Build a transparent Cargo workspace wrapper that creates `kache.toml` once
/// immediately before rustc. This makes the absent -> active race deterministic
/// without timing sleeps or platform-specific shell scripts.
fn build_activating_workspace_wrapper(root: &Path) -> PathBuf {
    let source = root.join("activate_config_wrapper.rs");
    std::fs::write(
        &source,
        r#"
use std::process::Command;

fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let is_fixture_compile = args.windows(2).any(|pair| {
        pair[0] == "--crate-name" && pair[1] == "extra_input_warm_target"
    });
    if is_fixture_compile {
        let config = std::env::var_os("KACHE_TEST_ACTIVATE_CONFIG").unwrap();
        let config = std::path::PathBuf::from(config);
        if !config.exists() {
            std::fs::write(&config, "extra_inputs = [\"data/**/*.txt\"]\n").unwrap();
        }
    }

    let compiler = &args[0];
    let status = Command::new(compiler).args(&args[1..]).status().unwrap();
    std::process::exit(status.code().unwrap_or(1));
}
"#,
    )
    .unwrap();
    let mut output = root.join("activate-config-wrapper");
    if cfg!(windows) {
        output.set_extension("exe");
    }
    let compile = Command::new(rustc_path())
        .args(["--edition", "2021", "-o"])
        .arg(&output)
        .arg(&source)
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .output()
        .expect("build activating workspace wrapper");
    assert!(
        compile.status.success(),
        "building activating workspace wrapper failed:\n{}",
        String::from_utf8_lossy(&compile.stderr)
    );
    output
}

fn cargo_build(
    project: &Path,
    target_dir: &Path,
    cache_dir: &Path,
    input: &Path,
    input_reader: &Path,
) -> Output {
    cargo_build_with_env(project, target_dir, cache_dir, input, input_reader, &[])
}

fn cargo_build_with_env(
    project: &Path,
    target_dir: &Path,
    cache_dir: &Path,
    input: &Path,
    input_reader: &Path,
    extra_env: &[(&str, &str)],
) -> Output {
    let mut command = Command::new("cargo");
    command
        .args([
            "build",
            "--offline",
            "--verbose",
            "--bin",
            "extra-input-warm-target",
        ])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_TERM_COLOR", "never")
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CACHE_EXECUTABLES", "1")
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("KACHE_LOG", "kache=debug")
        .env(INPUT_PATH_ENV, input)
        .env(
            "RUSTFLAGS",
            format!("--extern=input_reader={}", input_reader.display()),
        )
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("KACHE_DISABLED");
    for (name, value) in extra_env {
        command.env(name, value);
    }
    command.output().expect("run Cargo through Kache")
}

fn assert_build_succeeded(output: &Output) {
    assert!(
        output.status.success(),
        "Cargo build failed.\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn fixture_event_count(cache_dir: &Path) -> usize {
    std::fs::read_to_string(cache_dir.join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["crate_name"] == CRATE_NAME)
        .count()
}

fn last_fixture_event_result(cache_dir: &Path) -> Option<String> {
    std::fs::read_to_string(cache_dir.join("events.jsonl"))
        .ok()?
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["crate_name"] == CRATE_NAME)
        .filter_map(|event| event["result"].as_str().map(str::to_owned))
        .next_back()
}

fn last_fixture_passthrough_reason(cache_dir: &Path) -> Option<String> {
    std::fs::read_to_string(cache_dir.join("events.jsonl"))
        .ok()?
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["crate_name"] == CRATE_NAME)
        .filter_map(|event| event["passthrough_reason"].as_str().map(str::to_owned))
        .next_back()
}

fn run_fixture(target_dir: &Path) -> String {
    let mut binary = target_dir.join("debug").join("extra-input-warm-target");
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    let output = Command::new(&binary)
        .output()
        .unwrap_or_else(|error| panic!("run {}: {error}", binary.display()));
    assert!(output.status.success(), "fixture binary failed");
    String::from_utf8(output.stdout).unwrap()
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

fn run_daemon_command(cache_dir: &Path, subcommand: &str) -> Output {
    let stdout_path = cache_dir.join(format!("daemon-{subcommand}.stdout"));
    let stderr_path = cache_dir.join(format!("daemon-{subcommand}.stderr"));
    let stdout = std::fs::File::create(&stdout_path).unwrap();
    let stderr = std::fs::File::create(&stderr_path).unwrap();
    let mut child = Command::new(kache_binary())
        .args(["daemon", subcommand])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("KACHE_LOCAL_HIT_DAEMON", "1")
        .env("KACHE_DAEMON_IDLE_TIMEOUT", "60")
        .env("KACHE_LOG", "off")
        .env_remove("KACHE_SOCKET_PATH")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .stdin(std::process::Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("kache daemon {subcommand} timed out");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    Output {
        status,
        stdout: std::fs::read(stdout_path).unwrap_or_default(),
        stderr: std::fs::read(stderr_path).unwrap_or_default(),
    }
}

struct DaemonGuard {
    cache_dir: PathBuf,
}

impl DaemonGuard {
    fn start(cache_dir: &Path) -> Self {
        let output = run_daemon_command(cache_dir, "start");
        assert!(
            output.status.success(),
            "daemon start failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Self {
            cache_dir: cache_dir.to_path_buf(),
        }
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = run_daemon_command(&self.cache_dir, "stop");
    }
}

#[allow(clippy::too_many_arguments)]
fn exercise_uncached_lane(
    project: &Path,
    input: &Path,
    input_reader: &Path,
    before: &str,
    after: &str,
    extra_env: &[(&str, &str)],
    expected_result: &str,
    expected_reason: Option<&str>,
) {
    let target = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    std::fs::write(input, before).unwrap();

    let cold = cargo_build_with_env(
        project,
        target.path(),
        cache.path(),
        input,
        input_reader,
        extra_env,
    );
    assert_build_succeeded(&cold);
    assert_eq!(run_fixture(target.path()), before);
    let cold_events = fixture_event_count(cache.path());
    assert!(cold_events > 0);
    assert_eq!(
        last_fixture_event_result(cache.path()).as_deref(),
        Some(expected_result)
    );
    if let Some(reason) = expected_reason {
        assert!(
            last_fixture_passthrough_reason(cache.path())
                .as_deref()
                .is_some_and(|actual| actual.contains(reason)),
            "expected passthrough reason containing {reason:?}, got {:?}",
            last_fixture_passthrough_reason(cache.path())
        );
    }

    let warm = cargo_build_with_env(
        project,
        target.path(),
        cache.path(),
        input,
        input_reader,
        extra_env,
    );
    assert_build_succeeded(&warm);
    assert_eq!(
        fixture_event_count(cache.path()),
        cold_events,
        "unchanged uncached lane must still become Cargo-fresh"
    );

    std::fs::write(input, after).unwrap();
    let changed = cargo_build_with_env(
        project,
        target.path(),
        cache.path(),
        input,
        input_reader,
        extra_env,
    );
    assert_build_succeeded(&changed);
    assert!(fixture_event_count(cache.path()) > cold_events);
    assert_eq!(run_fixture(target.path()), after);
}

fn cargo_reported_fixture_fresh(output: &Output) -> bool {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .any(|line| line.contains("Fresh extra-input-warm-target"))
}

fn exercise_workspace_wrapper_lane(
    project: &Path,
    input: &Path,
    input_reader: &Path,
    before: &str,
    after: &str,
) {
    let target = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let workspace_wrapper = kache_binary().to_string_lossy().into_owned();
    let extra_env = [("RUSTC_WORKSPACE_WRAPPER", workspace_wrapper.as_str())];
    std::fs::write(input, before).unwrap();

    let cold = cargo_build_with_env(
        project,
        target.path(),
        cache.path(),
        input,
        input_reader,
        &extra_env,
    );
    assert_build_succeeded(&cold);
    assert_eq!(run_fixture(target.path()), before);

    let warm = cargo_build_with_env(
        project,
        target.path(),
        cache.path(),
        input,
        input_reader,
        &extra_env,
    );
    assert_build_succeeded(&warm);
    assert!(
        cargo_reported_fixture_fresh(&warm),
        "unchanged double-wrapper build must be Cargo-fresh:\n{}",
        String::from_utf8_lossy(&warm.stderr)
    );

    std::fs::write(input, after).unwrap();
    let changed = cargo_build_with_env(
        project,
        target.path(),
        cache.path(),
        input,
        input_reader,
        &extra_env,
    );
    assert_build_succeeded(&changed);
    assert!(
        !cargo_reported_fixture_fresh(&changed),
        "declared-input edit must make the double-wrapper crate dirty"
    );
    assert_eq!(run_fixture(target.path()), after);

    let settled = cargo_build_with_env(
        project,
        target.path(),
        cache.path(),
        input,
        input_reader,
        &extra_env,
    );
    assert_build_succeeded(&settled);
    assert!(
        cargo_reported_fixture_fresh(&settled),
        "double-wrapper build must settle back to Cargo-fresh"
    );
}

#[test]
fn declared_extra_input_invalidates_a_real_warm_cargo_target() {
    build_kache();

    // Exercise Cargo's actual dep-info grammar: `#`, drive colons, and
    // backslashes are literal; only spaces use the trailing-backslash join.
    let project = tempfile::Builder::new()
        .prefix("kache #workspace")
        .tempdir()
        .unwrap();
    let target_dir = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let scaffolding = TempDir::new().unwrap();
    let input_reader = build_input_reader(scaffolding.path());

    let member = project.path().join("app");
    std::fs::create_dir_all(member.join("src")).unwrap();
    std::fs::write(
        project.path().join("Cargo.toml"),
        "[workspace]\nmembers = [\"app\"]\nresolver = \"2\"\n",
    )
    .unwrap();
    std::fs::write(
        member.join("Cargo.toml"),
        r#"[package]
name = "extra-input-warm-target"
version = "0.1.0"
edition = "2021"
"#,
    )
    .unwrap();
    std::fs::write(member.join("kache.toml"), "extra_inputs = []\n").unwrap();
    std::fs::write(
        member.join("src/main.rs"),
        "fn main() { print!(\"{}\", input_reader::declared_value!()); }\n",
    )
    .unwrap();
    std::fs::create_dir_all(member.join("data")).unwrap();
    let declared_input = member.join("data/declared.txt");
    std::fs::write(&declared_input, "v1").unwrap();

    let cold = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&cold);
    assert_eq!(run_fixture(target_dir.path()), "v1");
    let cold_events = fixture_event_count(cache_dir.path());
    assert!(cold_events > 0, "cold build should invoke Kache");

    // Establish a genuinely warm, Cargo-fresh target before changing the only
    // out-of-band input. No Rust source or build configuration changes below.
    let warm = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&warm);
    assert_eq!(
        fixture_event_count(cache_dir.path()),
        cold_events,
        "unchanged warm build should stay Cargo-fresh"
    );

    // Even an explicit empty declaration watches kache.toml without changing
    // the cache key, so activating it in a warm target invokes Kache.
    std::fs::write(
        member.join("kache.toml"),
        "extra_inputs = [\"data/**/*.txt\"]\n",
    )
    .unwrap();
    let activated = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&activated);
    let activated_events = fixture_event_count(cache_dir.path());
    assert!(
        activated_events > cold_events,
        "editing an empty declaration to active must invoke Kache"
    );

    std::fs::write(&declared_input, "v2").unwrap();

    let changed = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&changed);
    let changed_events = fixture_event_count(cache_dir.path());
    let changed_output = run_fixture(target_dir.path());
    assert!(
        changed_events > activated_events && changed_output == "v2",
        "editing only the declared extra input must make Cargo invoke Kache and refresh the \
         compiled output; Kache events {cold_events} -> {changed_events}, output after build \
         was {changed_output:?}\nCargo stderr:\n{}",
        String::from_utf8_lossy(&changed.stderr),
    );
    assert!(
        String::from_utf8_lossy(&changed.stderr).contains("Compiling extra-input-warm-target"),
        "changed input should make Cargo report a recompilation"
    );

    let unchanged_after_edit = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&unchanged_after_edit);
    assert_eq!(
        fixture_event_count(cache_dir.path()),
        changed_events,
        "unchanged follow-up after an edit must become Cargo-fresh again"
    );

    // A directory watch must catch a genuinely new glob member and its later
    // removal, not only edits/deletion of a previously matched literal path.
    let added_glob_member = member.join("data/added.txt");
    std::fs::write(&added_glob_member, "additional").unwrap();
    let glob_added = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&glob_added);
    let glob_added_events = fixture_event_count(cache_dir.path());
    assert!(
        glob_added_events > changed_events,
        "adding a new file to a declared glob must invoke Kache"
    );

    std::fs::remove_file(&added_glob_member).unwrap();
    let glob_removed = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&glob_removed);
    let glob_removed_events = fixture_event_count(cache_dir.path());
    assert!(
        glob_removed_events > glob_added_events,
        "removing a file from a declared glob must invoke Kache"
    );

    // The config itself is part of Cargo freshness. Changing the declaration
    // must invoke Kache even though no Rust source or compile-time value moved.
    std::fs::write(
        member.join("kache.toml"),
        "extra_inputs = [\"data/**/*.txt\", \"data/future.txt\"]\n",
    )
    .unwrap();
    let config_changed = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&config_changed);
    let config_events = fixture_event_count(cache_dir.path());
    assert!(
        config_events > glob_removed_events,
        "editing kache.toml must invoke Kache"
    );
    assert_eq!(run_fixture(target_dir.path()), "v2");

    let unchanged_after_config = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&unchanged_after_config);
    assert_eq!(
        fixture_event_count(cache_dir.path()),
        config_events,
        "unchanged follow-up after a config edit must become Cargo-fresh"
    );

    // Deleting and then adding the compile-time input exercise both sides of
    // a watch transition: the present file dependency catches deletion, and
    // the bounded parent watch written by the deletion build catches re-add.
    std::fs::remove_file(&declared_input).unwrap();
    let deleted = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&deleted);
    let deleted_events = fixture_event_count(cache_dir.path());
    assert!(
        deleted_events > config_events,
        "deleting a declared input must invoke Kache"
    );
    assert_eq!(run_fixture(target_dir.path()), "missing");

    let unchanged_after_delete = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&unchanged_after_delete);
    assert_eq!(
        fixture_event_count(cache_dir.path()),
        deleted_events,
        "a still-missing input must not make every Cargo build dirty"
    );

    std::fs::write(&declared_input, "v3").unwrap();
    let added = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&added);
    let added_events = fixture_event_count(cache_dir.path());
    assert!(
        added_events > deleted_events,
        "adding a declared input must invoke Kache"
    );
    assert_eq!(run_fixture(target_dir.path()), "v3");

    let unchanged_after_add = cargo_build(
        project.path(),
        target_dir.path(),
        cache_dir.path(),
        &declared_input,
        &input_reader,
    );
    assert_build_succeeded(&unchanged_after_add);
    assert_eq!(
        fixture_event_count(cache_dir.path()),
        added_events,
        "unchanged follow-up after an addition must become Cargo-fresh"
    );

    // Restore the same key into a relocated workspace and target. Cached
    // dep-info is producer-neutral; the wrapper must augment the restored
    // consumer copy before returning to Cargo, including for legacy entries.
    let relocated_project = tempfile::Builder::new()
        .prefix("relocated #workspace")
        .tempdir()
        .unwrap();
    let relocated_member = relocated_project.path().join("app");
    std::fs::create_dir_all(relocated_member.join("src")).unwrap();
    std::fs::create_dir_all(relocated_member.join("data")).unwrap();
    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "app/Cargo.toml",
        "app/kache.toml",
        "app/src/main.rs",
        "app/data/declared.txt",
    ] {
        std::fs::copy(
            project.path().join(relative),
            relocated_project.path().join(relative),
        )
        .unwrap_or_else(|error| panic!("copy relocated fixture {relative}: {error}"));
    }
    let relocated_input = relocated_member.join("data/declared.txt");
    let restored_target = TempDir::new().unwrap();
    let restored = cargo_build(
        relocated_project.path(),
        restored_target.path(),
        cache_dir.path(),
        &relocated_input,
        &input_reader,
    );
    assert_build_succeeded(&restored);
    assert_eq!(
        last_fixture_event_result(cache_dir.path()).as_deref(),
        Some("local_hit"),
        "fresh target should restore the already-cached v3 artifact"
    );
    assert_eq!(run_fixture(restored_target.path()), "v3");
    let restored_events = fixture_event_count(cache_dir.path());

    let restored_unchanged = cargo_build(
        relocated_project.path(),
        restored_target.path(),
        cache_dir.path(),
        &relocated_input,
        &input_reader,
    );
    assert_build_succeeded(&restored_unchanged);
    assert_eq!(
        fixture_event_count(cache_dir.path()),
        restored_events,
        "restored dep-info must leave the next unchanged build Cargo-fresh"
    );

    let restored_dep_info = std::fs::read_dir(restored_target.path().join("debug/deps"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "d"))
        .find_map(|path| {
            let content = std::fs::read_to_string(path).ok()?;
            (content.contains("kache.toml") && content.contains("declared.txt")).then_some(content)
        })
        .expect("restored consumer dep-info");
    let dependencies = restored_dep_info
        .lines()
        .find_map(|line| {
            (!line.starts_with("# env-dep:"))
                .then(|| line.find(": ").map(|separator| &line[separator + 2..]))
                .flatten()
        })
        .expect("Cargo dependency rule");
    let mut parsed_dependencies = Vec::new();
    let mut current = String::new();
    for word in dependencies.split_whitespace() {
        if let Some(continued) = word.strip_suffix('\\') {
            current.push_str(continued);
            current.push(' ');
        } else {
            current.push_str(word);
            parsed_dependencies.push(PathBuf::from(std::mem::take(&mut current)));
        }
    }
    assert!(current.is_empty(), "unterminated Cargo dependency path");
    let canonical_dependencies: Vec<_> = parsed_dependencies
        .iter()
        .filter_map(|path| std::fs::canonicalize(path).ok())
        .collect();
    let producer = std::fs::canonicalize(project.path()).unwrap();
    let consumer = std::fs::canonicalize(relocated_project.path()).unwrap();
    assert!(
        canonical_dependencies
            .iter()
            .all(|dependency| !dependency.starts_with(&producer)),
        "restored dep-info retained the producer workspace: {restored_dep_info}"
    );
    assert!(
        canonical_dependencies.iter().any(|dependency| {
            dependency.starts_with(&consumer)
                && dependency
                    .file_name()
                    .is_some_and(|name| name == "kache.toml")
        }),
        "restored dep-info did not name the current consumer: {restored_dep_info}"
    );

    std::fs::write(&relocated_input, "v4").unwrap();
    let changed_after_restore = cargo_build(
        relocated_project.path(),
        restored_target.path(),
        cache_dir.path(),
        &relocated_input,
        &input_reader,
    );
    assert_build_succeeded(&changed_after_restore);
    assert!(
        fixture_event_count(cache_dir.path()) > restored_events,
        "declared input edit after a local hit must invoke Kache"
    );
    assert_eq!(run_fixture(restored_target.path()), "v4");

    // The daemon-local lane reads metadata from cache A without opening cache
    // B's SQLite. Copy only blobs so a successful fresh-target restore proves
    // `BlobSource::StoreDir` completed the consumer dep-info.
    let _daemon = DaemonGuard::start(cache_dir.path());
    let daemon_socket = cache_dir.path().join("daemon.sock");
    let daemon_socket_text = daemon_socket.to_string_lossy().into_owned();
    let daemon_env = [
        ("KACHE_LOCAL_HIT_DAEMON", "1"),
        ("KACHE_SOCKET_PATH", daemon_socket_text.as_str()),
        ("KACHE_LOCAL_HIT_TIMEOUT_MS", "1000"),
    ];
    // The daemon deliberately sheds lookup work after 50ms. A saturated CI
    // runner may therefore take the sound local fallback even though the
    // donor entry is present. Retry with a fresh consumer cache/target so the
    // test still proves an actual daemon restore without weakening that
    // production latency bound.
    let mut fallback_results = Vec::new();
    let (daemon_cache, daemon_target) = (0..8)
        .find_map(|_| {
            let daemon_cache = TempDir::new().unwrap();
            copy_tree(
                &cache_dir.path().join("store/blobs"),
                &daemon_cache.path().join("store/blobs"),
            );
            let daemon_target = TempDir::new().unwrap();
            let daemon_hit = cargo_build_with_env(
                relocated_project.path(),
                daemon_target.path(),
                daemon_cache.path(),
                &relocated_input,
                &input_reader,
                &daemon_env,
            );
            assert_build_succeeded(&daemon_hit);
            assert_eq!(run_fixture(daemon_target.path()), "v4");
            let result = last_fixture_event_result(daemon_cache.path());
            if result.as_deref() == Some("local_hit")
                && !daemon_cache.path().join("index.db").exists()
            {
                Some((daemon_cache, daemon_target))
            } else {
                fallback_results.push((
                    result,
                    String::from_utf8_lossy(&daemon_hit.stderr).into_owned(),
                    std::fs::read_to_string(daemon_cache.path().join("events.jsonl"))
                        .unwrap_or_default(),
                ));
                None
            }
        })
        .unwrap_or_else(|| {
            panic!(
                "donor daemon never produced a local hit from 8 fresh consumers: {fallback_results:?}"
            )
        });
    let daemon_events = fixture_event_count(daemon_cache.path());

    let daemon_fresh = cargo_build_with_env(
        relocated_project.path(),
        daemon_target.path(),
        daemon_cache.path(),
        &relocated_input,
        &input_reader,
        &daemon_env,
    );
    assert_build_succeeded(&daemon_fresh);
    assert_eq!(
        fixture_event_count(daemon_cache.path()),
        daemon_events,
        "unchanged daemon-restored target must remain Cargo-fresh"
    );

    std::fs::write(&relocated_input, "daemon-v5").unwrap();
    let daemon_miss = cargo_build_with_env(
        relocated_project.path(),
        daemon_target.path(),
        daemon_cache.path(),
        &relocated_input,
        &input_reader,
        &daemon_env,
    );
    assert_build_succeeded(&daemon_miss);
    assert!(fixture_event_count(daemon_cache.path()) > daemon_events);
    assert_eq!(run_fixture(daemon_target.path()), "daemon-v5");
    assert!(
        daemon_cache.path().join("index.db").exists(),
        "new key must miss the donor daemon and compile through the consumer store"
    );

    exercise_uncached_lane(
        relocated_project.path(),
        &relocated_input,
        &input_reader,
        "v4",
        "v5",
        &[("KACHE_MIN_STORE_COMPILE_MS", "18446744073709551615")],
        "skipped",
        None,
    );
    exercise_uncached_lane(
        relocated_project.path(),
        &relocated_input,
        &input_reader,
        "v5",
        "v6",
        &[
            ("KACHE_CACHE_EXECUTABLES", "0"),
            ("KACHE_ADAPTIVE_INCREMENTAL", "0"),
        ],
        "passthrough",
        Some("cache_executables=false"),
    );
    exercise_uncached_lane(
        relocated_project.path(),
        &relocated_input,
        &input_reader,
        "v6",
        "v7",
        &[
            ("KACHE_CACHE_EXECUTABLES", "0"),
            ("KACHE_PRESERVE_INCREMENTAL", "1"),
            ("CARGO_INCREMENTAL", "1"),
        ],
        "passthrough",
        Some("cache_executables=false"),
    );
    exercise_workspace_wrapper_lane(
        relocated_project.path(),
        &relocated_input,
        &input_reader,
        "v7",
        "v8",
    );

    // If the wrapper is already running when kache.toml appears, Kache can and
    // must reject that one invocation. This is separate from the documented
    // already-Fresh bootstrap case below, where Cargo never invokes a wrapper.
    let activation_project = tempfile::Builder::new()
        .prefix("activation-race #workspace")
        .tempdir()
        .unwrap();
    copy_tree(relocated_project.path(), activation_project.path());
    let activation_member = activation_project.path().join("app");
    let activation_config = activation_member.join("kache.toml");
    let activation_input = activation_member.join("data/declared.txt");
    std::fs::remove_file(&activation_config).unwrap();
    std::fs::write(&activation_input, "v8").unwrap();
    let activation_target = TempDir::new().unwrap();
    let activation_cache = TempDir::new().unwrap();
    let activating_wrapper = build_activating_workspace_wrapper(scaffolding.path());
    let activating_wrapper_text = activating_wrapper.to_string_lossy().into_owned();
    let activation_config_text = activation_config.to_string_lossy().into_owned();
    let activation_env = [
        ("RUSTC_WORKSPACE_WRAPPER", activating_wrapper_text.as_str()),
        (ACTIVATE_CONFIG_ENV, activation_config_text.as_str()),
    ];
    let raced = cargo_build_with_env(
        activation_project.path(),
        activation_target.path(),
        activation_cache.path(),
        &activation_input,
        &input_reader,
        &activation_env,
    );
    assert!(
        !raced.status.success()
            && String::from_utf8_lossy(&raced.stderr).contains("extra_inputs declaration changed"),
        "activation race must fail for a clean retry:\n{}",
        String::from_utf8_lossy(&raced.stderr)
    );
    assert!(activation_config.is_file());

    let retried = cargo_build_with_env(
        activation_project.path(),
        activation_target.path(),
        activation_cache.path(),
        &activation_input,
        &input_reader,
        &activation_env,
    );
    assert_build_succeeded(&retried);
    assert_eq!(run_fixture(activation_target.path()), "v8");

    // Kache cannot retrofit Cargo's fingerprint when a previously
    // unconfigured target is already Fresh: Cargo never starts the wrapper.
    // Prove the documented one-time bootstrap, then prove normal tracking.
    let bootstrap_project = tempfile::Builder::new()
        .prefix("bootstrap #workspace")
        .tempdir()
        .unwrap();
    copy_tree(relocated_project.path(), bootstrap_project.path());
    let bootstrap_member = bootstrap_project.path().join("app");
    let bootstrap_input = bootstrap_member.join("data/declared.txt");
    std::fs::remove_file(bootstrap_member.join("kache.toml")).unwrap();
    std::fs::write(&bootstrap_input, "v8").unwrap();
    let bootstrap_target = TempDir::new().unwrap();
    let bootstrap_cache = TempDir::new().unwrap();

    let pre_upgrade = cargo_build(
        bootstrap_project.path(),
        bootstrap_target.path(),
        bootstrap_cache.path(),
        &bootstrap_input,
        &input_reader,
    );
    assert_build_succeeded(&pre_upgrade);
    assert_eq!(run_fixture(bootstrap_target.path()), "v8");
    let pre_upgrade_events = fixture_event_count(bootstrap_cache.path());

    std::fs::write(
        bootstrap_member.join("kache.toml"),
        "extra_inputs = [\"data/**/*.txt\"]\n",
    )
    .unwrap();
    std::fs::write(&bootstrap_input, "v9").unwrap();
    let still_fresh = cargo_build(
        bootstrap_project.path(),
        bootstrap_target.path(),
        bootstrap_cache.path(),
        &bootstrap_input,
        &input_reader,
    );
    assert_build_succeeded(&still_fresh);
    assert!(cargo_reported_fixture_fresh(&still_fresh));
    assert_eq!(run_fixture(bootstrap_target.path()), "v8");
    assert_eq!(
        fixture_event_count(bootstrap_cache.path()),
        pre_upgrade_events,
        "adding kache.toml cannot invoke a wrapper Cargo already considers Fresh"
    );

    let clean = Command::new("cargo")
        .args(["clean", "--package", "extra-input-warm-target"])
        .current_dir(bootstrap_project.path())
        .env("CARGO_TARGET_DIR", bootstrap_target.path())
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .output()
        .unwrap();
    assert!(
        clean.status.success(),
        "bootstrap cargo clean failed:\n{}",
        String::from_utf8_lossy(&clean.stderr)
    );
    let bootstrapped = cargo_build(
        bootstrap_project.path(),
        bootstrap_target.path(),
        bootstrap_cache.path(),
        &bootstrap_input,
        &input_reader,
    );
    assert_build_succeeded(&bootstrapped);
    assert_eq!(run_fixture(bootstrap_target.path()), "v9");

    let bootstrapped_events = fixture_event_count(bootstrap_cache.path());
    std::fs::write(&bootstrap_input, "v10").unwrap();
    let tracked_after_bootstrap = cargo_build(
        bootstrap_project.path(),
        bootstrap_target.path(),
        bootstrap_cache.path(),
        &bootstrap_input,
        &input_reader,
    );
    assert_build_succeeded(&tracked_after_bootstrap);
    assert!(fixture_event_count(bootstrap_cache.path()) > bootstrapped_events);
    assert_eq!(run_fixture(bootstrap_target.path()), "v10");
}
