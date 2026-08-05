//! End-to-end coverage for the opt-in incremental passthrough used by
//! mutation-testing workloads.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

fn run_fake_rustc(
    preserve_incremental: bool,
    disabled: bool,
    long_codegen_form: bool,
) -> (Vec<String>, String, Option<serde_json::Value>) {
    let dir = tempfile::tempdir().unwrap();
    let argv_dump = dir.path().join("argv.txt");
    let env_dump = dir.path().join("incremental-env.txt");
    let fake_rustc = dir.path().join("rustc");
    let failing_fallback = dir.path().join("fallback");
    fs::write(
        &fake_rustc,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGV_DUMP\"\nprintf '%s' \"${CARGO_INCREMENTAL-unset}\" > \"$INCREMENTAL_ENV_DUMP\"\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&fake_rustc, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(&failing_fallback, "#!/bin/sh\nexit 77\n").unwrap();
    fs::set_permissions(&failing_fallback, fs::Permissions::from_mode(0o755)).unwrap();

    let source = dir.path().join("lib.rs");
    fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
    let out_dir = dir.path().join("target");
    let incremental_dir = out_dir.join("incremental/mutant");
    fs::create_dir_all(&out_dir).unwrap();

    let incremental_arg = format!("-Cincremental={}", incremental_dir.display());
    let mut command = Command::new(kache_binary());
    command
        .arg(&fake_rustc)
        .args(["--crate-name", "mutant", "--crate-type", "lib"])
        .arg(&source)
        .arg("--out-dir")
        .arg(&out_dir)
        .arg("--emit=metadata")
        .current_dir(dir.path())
        .env("ARGV_DUMP", &argv_dump)
        .env("INCREMENTAL_ENV_DUMP", &env_dump)
        .env("CARGO_INCREMENTAL", "1")
        .env("CARGO_PRIMARY_PACKAGE", "1")
        .env("KACHE_CACHE_DIR", dir.path().join("cache"))
        .env("KACHE_CONFIG", dir.path().join("missing-config.toml"))
        .env("KACHE_LOG", "kache=debug");
    if long_codegen_form {
        command.arg(format!(
            "--codegen=incremental={}",
            incremental_dir.display()
        ));
    } else {
        command.arg(&incremental_arg);
    }
    if preserve_incremental {
        command
            .env("KACHE_PRESERVE_INCREMENTAL", "1")
            .env("KACHE_FALLBACK", &failing_fallback);
    } else {
        command
            .env_remove("KACHE_PRESERVE_INCREMENTAL")
            .env_remove("KACHE_FALLBACK");
    }
    if disabled {
        command.env("KACHE_DISABLED", "1");
    } else {
        command.env_remove("KACHE_DISABLED");
    }

    let output = command.output().expect("failed to run kache wrapper");
    assert!(
        output.status.success(),
        "kache wrapper failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let argv = fs::read_to_string(&argv_dump)
        .expect("fake rustc did not record argv")
        .lines()
        .map(str::to_owned)
        .collect();
    let incremental_env = fs::read_to_string(&env_dump).unwrap();
    let event = fs::read_to_string(dir.path().join("cache/events.jsonl"))
        .ok()
        .and_then(|contents| {
            contents
                .lines()
                .next_back()
                .map(|line| serde_json::from_str(line).unwrap())
        });
    (argv, incremental_env, event)
}

#[test]
fn preserve_incremental_bypasses_cache_without_stripping_the_flag() {
    let (argv, incremental_env, event) = run_fake_rustc(true, false, false);
    let event = event.expect("kache did not record the passthrough event");

    assert!(
        argv.iter()
            .any(|arg| arg.starts_with("-Cincremental=") && arg.ends_with(".kache-preserved")),
        "opt-in passthrough must preserve isolated rustc incremental state: {argv:?}"
    );
    assert_eq!(event["result"], "passthrough");
    assert_eq!(event["passthrough_reason"], "incremental preserved");
    assert!(!event["fallback"].as_bool().unwrap_or(false));
    assert_eq!(event["key_ms"], 0);
    assert_eq!(incremental_env, "1");
}

#[test]
fn default_wrapper_behavior_still_strips_incremental() {
    let (argv, incremental_env, _event) = run_fake_rustc(false, false, false);

    assert!(
        !argv.iter().any(|arg| arg.contains("incremental=")),
        "default behavior must retain the existing safety policy: {argv:?}"
    );
    assert_eq!(incremental_env, "0");
}

#[test]
fn disabled_mode_honors_preserve_incremental() {
    let (argv, incremental_env, event) = run_fake_rustc(true, true, false);

    assert!(
        argv.iter()
            .any(|arg| arg.starts_with("-Cincremental=") && arg.ends_with(".kache-preserved")),
        "disabled mode must preserve isolated rustc incremental state: {argv:?}"
    );
    assert_eq!(incremental_env, "1");
    assert!(
        event.is_none(),
        "disabled mode should not write cache events"
    );
}

#[test]
fn long_codegen_form_is_preserved_safely() {
    let (argv, incremental_env, event) = run_fake_rustc(true, false, true);

    assert!(
        argv.iter().any(|arg| {
            arg.starts_with("--codegen=incremental=") && arg.ends_with(".kache-preserved")
        }),
        "long rustc codegen form must use isolated incremental state: {argv:?}"
    );
    assert_eq!(incremental_env, "1");
    assert_eq!(
        event.unwrap()["passthrough_reason"],
        "incremental preserved"
    );
}
