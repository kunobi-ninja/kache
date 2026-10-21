//! Real Cargo/rustc coverage for the explicit worktree seed experiment.
#![cfg(unix)]

use std::{fs, path::Path, process::Output};

#[allow(dead_code)]
mod common;

fn project(root: &Path, answer: u64) {
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname='worktree-probe'\nversion='0.1.0'\nedition='2024'\n[workspace]\n",
    )
    .unwrap();
    fs::write(root.join("src/main.rs"), format!(
        "#[inline(never)] fn answer() -> u64 {{ {answer} }}\nfn main() {{ println!(\"{{}}|{{}}\", answer(), env!(\"CARGO_MANIFEST_DIR\")); }}\n"
    )).unwrap();
}

fn build(root: &Path, donor: Option<&Path>, extra_flags: &str) -> Output {
    let cache = root.join("cache");
    let mut command = common::hermetic_command("cargo", &cache, Some(&root.join("absent.toml")));
    command
        .current_dir(root)
        .args(["build", "--offline", "--quiet"])
        .env("RUSTC_WRAPPER", env!("CARGO_BIN_EXE_kache"))
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env("CARGO_INCREMENTAL", "1")
        .env("KACHE_PRESERVE_INCREMENTAL", "0")
        .env("KACHE_INCREMENTAL_CRATES", "worktree_probe")
        .env("KACHE_UNSTABLE", "true")
        .env("KACHE_ADAPTIVE_INCREMENTAL", "0")
        // Only this test's compiler children opt into unstable flags.
        .env("RUSTC_BOOTSTRAP", "1")
        .env(
            "RUSTFLAGS",
            format!("-Zremap-cwd-prefix=/src -Zincremental-info {extra_flags}"),
        )
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    if let Some(donor) = donor {
        command.env("KACHE_INCREMENTAL_SEED_FROM", donor);
    } else {
        command.env_remove("KACHE_INCREMENTAL_SEED_FROM");
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn check_binary(root: &Path, answer: u64) {
    let output = std::process::Command::new(root.join("target/debug/worktree-probe"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("{answer}|{}", root.display())
    );
}

fn reused(output: &Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("completely ignoring cache"), "{stderr}");
    stderr.lines().any(|line| {
        line.contains("session directory:")
            && line.contains("files hard-linked")
            && !line.contains(": 0 files")
    })
}

fn supports_cwd_remap() -> bool {
    // Older toolchains cannot take part in this experiment. Keep the ordinary
    // test suite usable when the selected compiler lacks the required flag.
    let help = std::process::Command::new("rustc")
        .args(["-Z", "help"])
        .env("RUSTC_BOOTSTRAP", "1")
        .output()
        .unwrap();
    String::from_utf8_lossy(&help.stdout).contains("remap-cwd-prefix")
}

fn graph_contents(root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn collect(
        root: &Path,
        path: &Path,
        files: &mut std::collections::BTreeMap<std::path::PathBuf, Vec<u8>>,
    ) {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                collect(root, &entry.path(), files);
            } else {
                files.insert(
                    entry.path().strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    collect(root, root, &mut files);
    files
}

fn automatic_supported() -> bool {
    let output = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    let release = text.split_whitespace().nth(1).unwrap_or("");
    let mut parts = release.split('.');
    let major = parts
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let minor = parts
        .next()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    (major, minor) >= (1, 98) && supports_cwd_remap()
}

fn automatic_build(root: &Path, cache: &Path, enabled: bool, force: bool, library: bool) -> Output {
    managed_command(root, cache, enabled, force, library)
        .output()
        .unwrap()
}

fn managed_command(
    root: &Path,
    cache: &Path,
    enabled: bool,
    force: bool,
    library: bool,
) -> std::process::Command {
    let mut command = common::hermetic_command("cargo", cache, Some(&cache.join("absent.toml")));
    command
        .current_dir(root)
        .args(["build", "--offline", "--quiet"])
        .env("RUSTC_WRAPPER", env!("CARGO_BIN_EXE_kache"))
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env("CARGO_INCREMENTAL", "1")
        .env("KACHE_BASE_DIR", root)
        .env("KACHE_ADAPTIVE_INCREMENTAL", "1")
        .env("KACHE_PRESERVE_INCREMENTAL", "0")
        .env("KACHE_LOCAL_ONLY", "1")
        .env("KACHE_CACHE_EXECUTABLES", "1")
        .env(
            "KACHE_VERIFY_INPUT_PREDICTIONS",
            if library { "off" } else { "always" },
        )
        .env_remove("KACHE_UNSTABLE")
        .env_remove("KACHE_INCREMENTAL_SEED_FROM")
        .env_remove("KACHE_FALLBACK")
        .env_remove("KACHE_DISABLED")
        .env_remove("KACHE_KEY_ENV_VARS")
        .env_remove("RUSTC_BOOTSTRAP")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        .env("RUSTFLAGS", "");
    if library {
        command.arg("--lib");
        command.env("KACHE_LOG", "kache=trace");
    }
    if enabled {
        command.env_remove("KACHE_SEED_INCREMENTAL");
    } else {
        command.env("KACHE_SEED_INCREMENTAL", "0");
    }
    if force {
        command.env("KACHE_INCREMENTAL_CRATES", "worktree_probe");
    } else {
        command.env_remove("KACHE_INCREMENTAL_CRATES");
    }
    command
}

fn edited_project(root: &Path, answer: u64) {
    project(root, answer);
    fs::write(
        root.join("src/main.rs"),
        format!("fn main() {{ println!(\"{{}}\", std::hint::black_box({answer}u64)); }}\n"),
    )
    .unwrap();
}

#[test]
fn default_policy_finds_a_donor_after_an_unchanged_cache_restore() {
    if !automatic_supported() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let cache = root.join("cache");
    let donor = root.join("donor");
    let receiver = root.join("receiver");
    let opted_out = root.join("opted-out");
    for answer in [0, 1, 2] {
        edited_project(&donor, answer);
        let output = automatic_build(&donor, &cache, true, false, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let original = graph_contents(&donor.join("target/debug/incremental.kache-auto"));
    for (repo, enabled) in [(&receiver, true), (&opted_out, false)] {
        edited_project(repo, 0);
        let output = automatic_build(repo, &cache, enabled, false, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        edited_project(repo, 3);
        let output = automatic_build(repo, &cache, enabled, false, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let binary = std::process::Command::new(repo.join("target/debug/worktree-probe"))
            .output()
            .unwrap();
        assert!(binary.status.success());
        assert_eq!(String::from_utf8(binary.stdout).unwrap().trim(), "3");
    }
    assert_eq!(
        graph_contents(&donor.join("target/debug/incremental.kache-auto")),
        original
    );
    let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        events
            .iter()
            .any(|event| event["root"] == receiver.to_str().unwrap()
                && event["result"] == "local_hit"),
        "unchanged receiver must restore"
    );
    assert!(
        events
            .iter()
            .any(|event| event["root"] == receiver.to_str().unwrap()
                && event["passthrough_reason"] == "automatic worktree seed"),
        "edited receiver must import"
    );
    assert!(
        !events
            .iter()
            .any(|event| event["root"] == opted_out.to_str().unwrap()
                && event["passthrough_reason"] == "automatic worktree seed"),
        "opt-out must not import"
    );
}

#[test]
fn automatic_bootstrap_retry_preserves_source_observations_and_discards_false_errors() {
    if !automatic_supported() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let cache = root.join("cache");
    project(&root, 0);
    for suffix in ["first", "second"] {
        fs::write(root.join("src/main.rs"), format!(
            "const _: () = assert!(option_env!(\"RUSTC_BOOTSTRAP\").is_none());\nfn main() {{ println!(\"{suffix}:{{}}\", option_env!(\"RUSTC_BOOTSTRAP\").unwrap_or(\"native\")); }}\n"
        )).unwrap();
        let output = automatic_build(&root, &cache, true, true, false);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("E0080"));
        let binary = std::process::Command::new(root.join("target/debug/worktree-probe"))
            .output()
            .unwrap();
        assert!(binary.status.success());
        assert_eq!(
            String::from_utf8(binary.stdout).unwrap().trim(),
            format!("{suffix}:native")
        );
        let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let event = events
            .iter()
            .rev()
            .find(|event| event["crate_name"] == "worktree_probe")
            .unwrap();
        assert_eq!(
            event["passthrough_reason"], "incremental force-list: worktree_probe",
            "{event:#}"
        );
        assert_eq!(
            event["compiler_runs"],
            if suffix == "first" { 2 } else { 1 },
            "{event:#}"
        );
    }
    assert!(
        graph_contents(&root.join("target/debug/incremental.kache-auto"))
            .keys()
            .any(|path| path
                .file_name()
                .is_some_and(|name| name == "bootstrap-env-dep"))
    );

    let failing = root.join("native-error");
    project(&failing, 0);
    fs::write(
        failing.join("src/main.rs"),
        "const _: () = assert!(option_env!(\"RUSTC_BOOTSTRAP\").is_some());\nfn main() {}\n",
    )
    .unwrap();
    let output = automatic_build(&failing, &cache, true, true, false);
    assert!(
        !output.status.success(),
        "native compile error must remain an error"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("E0080"));
    assert!(
        !failing.join("target/debug/worktree-probe").exists(),
        "discarded bootstrap binary must not survive the failed native retry"
    );
    assert!(
        !fs::read_dir(failing.join("target/debug/deps"))
            .unwrap()
            .any(|entry| {
                let path = entry.unwrap().path();
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("worktree_probe-")
                    && path.extension().is_none()
            }),
        "discarded rustc executable must not survive in deps either"
    );
}

#[test]
fn bootstrap_retry_keeps_absolute_include_paths_in_explicit_passthroughs() {
    if !automatic_supported() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    project(&root, 0);
    fs::write(
        root.join("native_path.rs"),
        "fn native_path() -> &'static str { file!() }\n",
    )
    .unwrap();
    fs::write(root.join("src/main.rs"), "const _: () = assert!(option_env!(\"RUSTC_BOOTSTRAP\").is_none());\ninclude!(concat!(env!(\"CARGO_MANIFEST_DIR\"), \"/native_path.rs\"));\nfn main() { println!(\"{}\", native_path()); }\n").unwrap();
    let output = automatic_build(&root, &root.join("cache"), true, true, false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = std::process::Command::new(root.join("target/debug/worktree-probe"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        root.join("native_path.rs").to_str().unwrap()
    );
}

#[test]
fn failed_bootstrap_dep_info_cleanup_keeps_the_native_compile_accounted() {
    if !automatic_supported() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let cache = root.join("cache");
    edited_project(&root, 0);
    let output = automatic_build(&root, &cache, true, true, false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let dep_info = fs::read_dir(root.join("target/debug/deps"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("worktree_probe-")
                && path.extension().is_some_and(|extension| extension == "d")
        })
        .unwrap();
    fs::remove_file(&dep_info).unwrap();
    fs::create_dir(&dep_info).unwrap();
    edited_project(&root, 1);
    let output = automatic_build(&root, &cache, true, true, false);
    assert!(!output.status.success());
    let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let event = events
        .iter()
        .rev()
        .find(|event| event["crate_name"] == "worktree_probe")
        .unwrap();
    assert_eq!(event["exit_code"], 1, "{event:#}");
    assert_eq!(
        event["passthrough_reason"], "incremental force-list: worktree_probe",
        "{event:#}"
    );
    assert_eq!(event["compiler_runs"], 1, "{event:#}");
}

#[test]
fn deferred_compiler_error_does_not_retry_a_stale_bootstrap_observation() {
    if !automatic_supported() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let cache = root.join("cache");
    project(&root, 0);
    let output = std::process::Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let rustc = std::path::PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .join("bin/rustc");
    let compiler = root.join("tool/rustc");
    fs::create_dir(compiler.parent().unwrap()).unwrap();
    kache_fs::testutil::write_executable(
        &compiler,
        "#!/bin/sh\nfor arg in \"$@\"; do\n case \"$arg\" in src/lib.rs)\n  if test -f \"$WORKTREE_PROBE_STOP\"; then\n   echo 'error: controlled compiler stop before dep-info' >&2\n   exit 1\n  fi;;\n esac\ndone\nexec \"$WORKTREE_PROBE_RUSTC\" \"$@\"\n",
    );
    let stop = root.join("stop-compiler");
    let build = || {
        managed_command(&root, &cache, true, false, true)
            .env("RUSTC", &compiler)
            .env("WORKTREE_PROBE_RUSTC", &rustc)
            .env("WORKTREE_PROBE_STOP", &stop)
            .output()
            .unwrap()
    };
    fs::write(root.join("src/lib.rs"), "pub fn answer() -> u64 { 0 }\n").unwrap();
    let output = build();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let dep_info = fs::read_dir(root.join("target/debug/deps"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("worktree_probe-")
                && path.extension().is_some_and(|extension| extension == "d")
        })
        .unwrap();
    fs::write(&dep_info, "# env-dep:RUSTC_BOOTSTRAP=stale\n").unwrap();
    fs::write(&stop, b"stop before touching dep-info").unwrap();
    fs::write(
        root.join("src/lib.rs"),
        "mod missing; pub fn answer() -> u64 { missing::answer() }\n",
    )
    .unwrap();
    let output = build();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("inputs=compile-while-keying"), "{stderr}");
    let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let event = events
        .iter()
        .rev()
        .find(|event| event["crate_name"] == "worktree_probe")
        .unwrap();
    assert_eq!(event["passthrough_reason"], "adaptive seed", "{event:#}");
    assert_eq!(
        event["compiler_runs"], 1,
        "a stale observation must not cause another compile: {event:#}"
    );
    assert!(
        !graph_contents(&root.join("target/debug/incremental.kache-auto"))
            .keys()
            .any(|path| path
                .file_name()
                .is_some_and(|name| name == "bootstrap-env-dep"))
    );
}

#[test]
fn deferred_library_seed_retries_bootstrap_before_any_artifact_lookup() {
    if !automatic_supported() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let cache = root.join("cache");
    project(&root, 0);
    let mut last_stderr = String::new();
    for answer in [0, 1] {
        if answer == 1 {
            fs::write(root.join("src/added.rs"), "pub fn value() -> u64 { 1 }\n").unwrap();
        }
        let body = if answer == 0 {
            "pub fn answer() -> u64 { 0 }"
        } else {
            "mod added; pub fn answer() -> u64 { added::value() }"
        };
        fs::write(
            root.join("src/lib.rs"),
            format!("const _: () = assert!(option_env!(\"RUSTC_BOOTSTRAP\").is_none());\n{body}\n"),
        )
        .unwrap();
        let output = automatic_build(&root, &cache, true, false, true);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("E0080"));
        last_stderr = String::from_utf8(output.stderr).unwrap();
    }
    assert!(
        last_stderr.contains("inputs=compile-while-keying"),
        "{last_stderr}"
    );
    let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let seed = events
        .iter()
        .rev()
        .find(|event| event["crate_name"] == "worktree_probe")
        .unwrap();
    assert_eq!(seed["passthrough_reason"], "adaptive seed", "{seed:#}");
    assert_eq!(
        seed["dep_info_runs"], 1,
        "only the discarded bootstrap discovery counts as dep-info work: {seed:#}"
    );
    assert_eq!(
        seed["compiler_runs"], 1,
        "exactly one native compile follows the discarded discovery: {seed:#}"
    );
    assert!(
        graph_contents(&root.join("target/debug/incremental.kache-auto"))
            .keys()
            .any(|path| path
                .file_name()
                .is_some_and(|name| name == "bootstrap-env-dep"))
    );
    let deps = root.join("target/debug/deps");
    let rlib = fs::read_dir(&deps)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("libworktree_probe-")
                && path
                    .extension()
                    .is_some_and(|extension| extension == "rlib")
        })
        .unwrap();
    let source = root.join("consumer.rs");
    let binary = root.join("consumer");
    fs::write(
        &source,
        "fn main() { println!(\"{}\", worktree_probe::answer()); }\n",
    )
    .unwrap();
    let output = std::process::Command::new("rustc")
        .arg(&source)
        .args(["--edition", "2024", "--extern"])
        .arg(format!("worktree_probe={}", rlib.display()))
        .arg("-L")
        .arg(&deps)
        .arg("-o")
        .arg(&binary)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = std::process::Command::new(binary).output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "1");
}

#[test]
fn deferred_retry_reads_dep_info_when_the_compiler_omits_its_notification() {
    if !automatic_supported() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let cache = root.join("cache");
    project(&root, 0);
    let output = std::process::Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let rustc = std::path::PathBuf::from(String::from_utf8(output.stdout).unwrap().trim())
        .join("bin/rustc");
    let compiler = root.join("tool/rustc");
    fs::create_dir(compiler.parent().unwrap()).unwrap();
    kache_fs::testutil::write_executable(
        &compiler,
        r#"#!/usr/bin/env python3
import json, os, subprocess, sys
real = os.environ['WORKTREE_PROBE_RUSTC']
args = sys.argv[1:]
if 'src/lib.rs' in args and os.environ.get('RUSTC_BOOTSTRAP') == '1':
    result = subprocess.run([real, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sys.stdout.buffer.write(result.stdout)
    for line in result.stderr.splitlines(keepends=True):
        try:
            if json.loads(line).get('emit') == 'dep-info':
                continue
        except (ValueError, UnicodeDecodeError):
            pass
        sys.stderr.buffer.write(line)
    sys.exit(result.returncode)
os.execv(real, [real, *args])
"#,
    );
    for answer in [0, 1] {
        let body = if answer == 0 {
            "pub fn answer() -> u64 { 0 }"
        } else {
            fs::write(root.join("src/added.rs"), "pub fn value() -> u64 { 1 }\n").unwrap();
            "mod added; pub fn answer() -> u64 { added::value() }"
        };
        fs::write(
            root.join("src/lib.rs"),
            format!("const _: () = assert!(option_env!(\"RUSTC_BOOTSTRAP\").is_none());\n{body}\n"),
        )
        .unwrap();
        let output = managed_command(&root, &cache, true, false, true)
            .env("RUSTC", &compiler)
            .env("WORKTREE_PROBE_RUSTC", &rustc)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains("E0080"));
    }
    let events: Vec<serde_json::Value> = fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let event = events
        .iter()
        .rev()
        .find(|event| event["crate_name"] == "worktree_probe")
        .unwrap();
    assert_eq!(event["passthrough_reason"], "adaptive seed", "{event:#}");
    assert_eq!(event["dep_info_runs"], 0, "{event:#}");
    assert_eq!(
        event["compiler_runs"], 2,
        "the reported dep-info still requires a native retry: {event:#}"
    );
}

#[test]
fn imports_across_checkout_paths_and_rechecks_sources_and_compile_time_env() {
    if !supports_cwd_remap() {
        eprintln!("skipping worktree seed test: rustc lacks remap-cwd-prefix");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let donor = root.join("donor");
    let receiver = root.join("deeper/receiver");
    let incompatible = root.join("incompatible");
    let cold = root.join("cold");
    project(&donor, 1);
    project(&receiver, 2);
    project(&incompatible, 3);
    project(&cold, 4);
    let profile = donor.join("target/debug");
    assert!(!reused(&build(&donor, Some(&profile), "")));
    check_binary(&donor, 1);
    let donor_graph = profile.join("incremental.kache-auto");
    let original = graph_contents(&donor_graph);
    let seeded = build(&receiver, Some(&profile), "");
    assert!(
        reused(&seeded),
        "{}",
        String::from_utf8_lossy(&seeded.stderr)
    );
    check_binary(&receiver, 2);
    project(&receiver, 3);
    assert!(reused(&build(&receiver, Some(&profile), "")));
    check_binary(&receiver, 3);
    assert_eq!(
        graph_contents(&donor_graph),
        original,
        "receiver changed the donor graph"
    );
    check_binary(&donor, 1);
    assert!(!reused(&build(
        &incompatible,
        Some(&profile),
        "-Copt-level=1"
    )));
    check_binary(&incompatible, 3);
    assert!(!reused(&build(&cold, None, "")));
    check_binary(&cold, 4);
}

#[test]
fn absolute_source_keeps_file_macro_semantics_without_publishing_a_seed() {
    if !supports_cwd_remap() {
        eprintln!("skipping worktree seed test: rustc lacks remap-cwd-prefix");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let source = root.join("src/main.rs");
    fs::create_dir(root.join("src")).unwrap();
    fs::write(&source, "fn main() { println!(\"{}\", file!()); }").unwrap();
    let profile = root.join("target/debug");
    let deps = profile.join("deps");
    fs::create_dir_all(&deps).unwrap();
    let cache = root.join("cache");
    let output = common::hermetic_command(
        env!("CARGO_BIN_EXE_kache"),
        &cache,
        Some(&root.join("absent.toml")),
    )
    .current_dir(&root)
    .args([
        "rustc",
        "--crate-name",
        "absolute_probe",
        "--crate-type=bin",
        "-Cextra-filename=-1234abcd",
        "-Zremap-cwd-prefix=/src",
    ])
    .arg(&source)
    .arg("--out-dir")
    .arg(&deps)
    .arg(format!(
        "-Cincremental={}",
        profile.join("incremental").display()
    ))
    .env("KACHE_INCREMENTAL_CRATES", "absolute_probe")
    .env("KACHE_UNSTABLE", "true")
    .env("KACHE_PRESERVE_INCREMENTAL", "0")
    .env("KACHE_ADAPTIVE_INCREMENTAL", "0")
    .env("KACHE_INCREMENTAL_SEED_FROM", &profile)
    .env("RUSTC_BOOTSTRAP", "1")
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result = std::process::Command::new(deps.join("absolute_probe-1234abcd"))
        .output()
        .unwrap();
    assert!(result.status.success());
    assert_eq!(
        String::from_utf8(result.stdout).unwrap().trim(),
        "/src/src/main.rs"
    );
    for unit in fs::read_dir(profile.join("incremental.kache-auto/v1")).unwrap() {
        assert!(!unit.unwrap().path().join("worktree-seed-v1").exists());
    }
}

fn automatic_project(root: &Path) {
    project(root, 1);
    fs::write(root.join("build.rs"), r#"fn main() {
        println!("cargo:rustc-env=BUILD_BOOTSTRAP={}", option_env!("RUSTC_BOOTSTRAP").unwrap_or("unset"));
    }"#).unwrap();
    fs::write(root.join("src/main.rs"), r#"fn main() {
        println!("{}|{}", option_env!("RUSTC_BOOTSTRAP").unwrap_or("unset"), env!("BUILD_BOOTSTRAP"));
    }"#).unwrap();
}

fn automatic_command(
    root: &Path,
    unstable: &str,
    bootstrap: Option<&str>,
) -> std::process::Command {
    let mut command = common::hermetic_command(
        "cargo",
        &root.join("cache"),
        Some(&root.join("absent.toml")),
    );
    command
        .current_dir(root)
        .args(["build", "--offline", "--quiet"])
        .env("RUSTC_WRAPPER", env!("CARGO_BIN_EXE_kache"))
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env("CARGO_INCREMENTAL", "1")
        .env("KACHE_PRESERVE_INCREMENTAL", "0")
        .env("KACHE_INCREMENTAL_CRATES", "worktree_probe")
        .env("KACHE_ADAPTIVE_INCREMENTAL", "0")
        .env("KACHE_SEED_INCREMENTAL", "0")
        .env("KACHE_UNSTABLE", unstable)
        .env("KACHE_INCREMENTAL_SEED_FROM", root.join("target/debug"))
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        .env_remove("RUSTC_WORKSPACE_WRAPPER");
    if let Some(value) = bootstrap {
        command.env("RUSTC_BOOTSTRAP", value);
    } else {
        command.env_remove("RUSTC_BOOTSTRAP");
    }
    command
}

fn marker_count(root: &Path) -> usize {
    fs::read_dir(root.join("target/debug/incremental.kache-auto/v1"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|unit| unit.path().join("worktree-seed-v1").is_file())
        .count()
}

fn automatic_output(root: &Path) -> String {
    let result = std::process::Command::new(root.join("target/debug/worktree-probe"))
        .output()
        .unwrap();
    assert!(result.status.success());
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}

#[test]
fn automatic_mode_scopes_bootstrap_and_preserves_explicit_selections() {
    if !supports_cwd_remap() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    for (name, unstable, bootstrap, expected, markers) in [
        ("off", "0", None, "unset|unset", 0),
        (
            "off-with-existing-permission",
            "false",
            Some("worktree_probe"),
            "worktree_probe|worktree_probe",
            0,
        ),
        ("explicit-denial", "true", Some("-1"), "-1|-1", 0),
        (
            "existing-selection",
            "true",
            Some("worktree_probe"),
            "worktree_probe|worktree_probe",
            1,
        ),
    ] {
        let project = root.join(name);
        automatic_project(&project);
        let output = automatic_command(&project, unstable, bootstrap)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(automatic_output(&project), expected);
        assert_eq!(marker_count(&project), markers);
    }
    // Exercise the override only when the selected compiler is stable.
    let vv = std::process::Command::new("rustc")
        .arg("-vV")
        .output()
        .unwrap();
    if String::from_utf8_lossy(&vv.stdout).contains("nightly")
        || String::from_utf8_lossy(&vv.stdout).contains("-dev")
    {
        return;
    }
    let project = root.join("scoped");
    automatic_project(&project);
    let output = automatic_command(&project, "true", None)
        .env_remove("KACHE_INCREMENTAL_SEED_FROM")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        automatic_output(&project),
        "unset|unset",
        "source observations, Cargo and build scripts retain the caller environment"
    );
    assert_eq!(marker_count(&project), 0);
    let project = root.join("source-features");
    automatic_project(&project);
    fs::write(
        project.join("src/main.rs"),
        "#![feature(test)]\nfn main() {}\n",
    )
    .unwrap();
    let output = automatic_command(&project, "true", None).output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("error[E0725]"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(marker_count(&project), 0);
}

#[test]
fn unsupported_compiler_compiles_normally_and_memoizes_negative_probes() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    automatic_project(&root);
    let shim = root.join("rustc-unsupported");
    kache_fs::testutil::write_executable(
        &shim,
        r#"#!/bin/sh
for flag in "$@"; do
    case "$flag" in -Zremap-cwd-prefix=*) printf '%s\n' "${RUSTC_BOOTSTRAP-unset}" >> "$PROBE_COUNTER"; exit 1;; esac
done
exec "$REAL_RUSTC" "$@"
"#,
    );
    let sysroot = std::process::Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .unwrap();
    let real = Path::new(String::from_utf8_lossy(&sysroot.stdout).trim()).join("bin/rustc");
    let count = root.join("probe-counter");
    for edit in 0..2 {
        fs::write(root.join("src/main.rs"), format!("fn main() {{ println!(\"{edit}:{{}}\", option_env!(\"RUSTC_BOOTSTRAP\").unwrap_or(\"unset\")); }}")).unwrap();
        let output = automatic_command(&root, "true", None)
            .env("RUSTC", &shim)
            .env("REAL_RUSTC", &real)
            .env("PROBE_COUNTER", &count)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(automatic_output(&root), format!("{edit}:unset"));
        assert_eq!(marker_count(&root), 0);
    }
    assert_eq!(
        fs::read_to_string(count)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["unset", "1"]
    );
}
