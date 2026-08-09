//! End-to-end coverage for the opt-in incremental passthrough used by
//! mutation-testing workloads.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

/// How the invocation opts into incremental preservation.
#[derive(Clone, Copy)]
enum PreserveMode<'a> {
    /// Default policy: kache strips `-C incremental`.
    Off,
    /// `KACHE_PRESERVE_INCREMENTAL=1`: every incremental invocation preserved.
    Global,
    /// `KACHE_INCREMENTAL_CRATES=<list>`: only the listed crates preserved.
    /// `failing_fallback` also exports a fallback wrapper that exits 77 —
    /// proof that the exercised lane runs the compiler itself. Leave it off
    /// when the test expects the ordinary cache path, whose key pre-pass
    /// falls back on the fake compiler's missing dep-info output.
    ForceList {
        list: &'a str,
        failing_fallback: bool,
    },
}

fn run_fake_rustc(
    preserve_incremental: bool,
    disabled: bool,
    long_codegen_form: bool,
    response_file: bool,
) -> (Vec<String>, String, Option<serde_json::Value>) {
    run_fake_compiler(
        if preserve_incremental {
            PreserveMode::Global
        } else {
            PreserveMode::Off
        },
        disabled,
        long_codegen_form,
        response_file,
        "rustc",
        false,
    )
}

fn run_fake_forced_rustc(
    force_list: &str,
    disabled: bool,
    failing_fallback: bool,
) -> (Vec<String>, String, Option<serde_json::Value>) {
    run_fake_compiler(
        PreserveMode::ForceList {
            list: force_list,
            failing_fallback,
        },
        disabled,
        false,
        false,
        "rustc",
        false,
    )
}

fn run_fake_configured_rustc(
    preserve_incremental: bool,
    disabled: bool,
    response_file: bool,
) -> (Vec<String>, String, Option<serde_json::Value>) {
    run_fake_compiler(
        if preserve_incremental {
            PreserveMode::Global
        } else {
            PreserveMode::Off
        },
        disabled,
        false,
        response_file,
        "custom-rustc-driver",
        true,
    )
}

fn run_fake_compiler(
    preserve: PreserveMode<'_>,
    disabled: bool,
    long_codegen_form: bool,
    response_file: bool,
    compiler_name: &str,
    configured_rustc: bool,
) -> (Vec<String>, String, Option<serde_json::Value>) {
    let dir = tempfile::tempdir().unwrap();
    let argv_dump = dir.path().join("argv.txt");
    let env_dump = dir.path().join("incremental-env.txt");
    let fake_rustc = dir.path().join(compiler_name);
    let failing_fallback = dir.path().join("fallback");
    fs::write(
        &fake_rustc,
        "#!/bin/sh\n: > \"$ARGV_DUMP\"\nfor arg in \"$@\"; do\n  case \"$arg\" in\n    @*) while IFS= read -r line || [ -n \"$line\" ]; do printf '%s\\n' \"$line\" >> \"$ARGV_DUMP\"; done < \"${arg#@}\";;\n    *) printf '%s\\n' \"$arg\" >> \"$ARGV_DUMP\";;\n  esac\ndone\nprintf '%s' \"${CARGO_INCREMENTAL-unset}\" > \"$INCREMENTAL_ENV_DUMP\"\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&fake_rustc, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(&failing_fallback, "#!/bin/sh\nexit 77\n").unwrap();
    fs::set_permissions(&failing_fallback, fs::Permissions::from_mode(0o755)).unwrap();

    let source = dir.path().join("lib.rs");
    fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
    let profile = dir.path().join("target/debug");
    let out_dir = profile.join("deps");
    let incremental_dir = profile.join("incremental");
    fs::create_dir_all(&out_dir).unwrap();
    fs::create_dir_all(&incremental_dir).unwrap();

    let incremental_arg = format!("-Cincremental={}", incremental_dir.display());
    let mut rustc_args = vec![
        "--crate-name".to_string(),
        "mutant".to_string(),
        "--crate-type".to_string(),
        "lib".to_string(),
        source.to_string_lossy().into_owned(),
        "--out-dir".to_string(),
        out_dir.to_string_lossy().into_owned(),
        "--emit=metadata".to_string(),
        "-Cextra-filename=-1234abcd".to_string(),
    ];
    if long_codegen_form {
        rustc_args.push(format!(
            "--codegen=incremental={}",
            incremental_dir.display()
        ));
    } else {
        rustc_args.push(incremental_arg);
    }
    let mut command = Command::new(kache_binary());
    command
        .arg(&fake_rustc)
        .current_dir(dir.path())
        .env("ARGV_DUMP", &argv_dump)
        .env("INCREMENTAL_ENV_DUMP", &env_dump)
        .env("CARGO_INCREMENTAL", "1")
        .env("CARGO_PRIMARY_PACKAGE", "1")
        .env("KACHE_CACHE_DIR", dir.path().join("cache"))
        .env("KACHE_CONFIG", dir.path().join("missing-config.toml"))
        .env("KACHE_LOG", "kache=debug");
    if response_file {
        let rustc_argfile = dir.path().join("compiler.args");
        fs::write(&rustc_argfile, format!("{}\n", rustc_args.join("\n"))).unwrap();
        command.arg(format!("@{}", rustc_argfile.display()));
    } else {
        command.args(&rustc_args);
    }
    match preserve {
        PreserveMode::Global => {
            command
                .env("KACHE_PRESERVE_INCREMENTAL", "1")
                .env_remove("KACHE_INCREMENTAL_CRATES")
                .env("KACHE_FALLBACK", &failing_fallback);
        }
        // The failing fallback doubles as proof the force-list lane runs the
        // compiler itself: routing through the fallback would exit 77.
        PreserveMode::ForceList {
            list,
            failing_fallback: with_fallback,
        } => {
            command
                .env_remove("KACHE_PRESERVE_INCREMENTAL")
                .env("KACHE_INCREMENTAL_CRATES", list);
            if with_fallback {
                command.env("KACHE_FALLBACK", &failing_fallback);
            } else {
                command.env_remove("KACHE_FALLBACK");
            }
        }
        PreserveMode::Off => {
            command
                .env_remove("KACHE_PRESERVE_INCREMENTAL")
                .env_remove("KACHE_INCREMENTAL_CRATES")
                .env_remove("KACHE_FALLBACK");
        }
    }
    if disabled {
        command.env("KACHE_DISABLED", "1");
    } else {
        command.env_remove("KACHE_DISABLED");
    }
    if configured_rustc {
        command.env("RUSTC", &fake_rustc);
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

fn assert_changed_response_transport_fails_closed(disabled: bool) {
    let dir = tempfile::tempdir().unwrap();
    let fake_rustc = dir.path().join("rustc");
    let compiler_marker = dir.path().join("compiler-ran");
    fs::write(
        &fake_rustc,
        format!("#!/bin/sh\n: > \"{}\"\nexit 0\n", compiler_marker.display()),
    )
    .unwrap();
    fs::set_permissions(&fake_rustc, fs::Permissions::from_mode(0o755)).unwrap();

    let source = dir.path().join("lib.rs");
    let out_dir = dir.path().join("target/debug/deps");
    let incremental = dir.path().join("target/debug/incremental/unit");
    let response = dir.path().join("rustc.args");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
    fs::write(
        &response,
        format!(
            "--crate-name\nmutant\n--crate-type\nlib\n{}\n--out-dir\n{}\n--emit=metadata\n-Cincremental={}\n",
            source.display(),
            out_dir.display(),
            incremental.display()
        ),
    )
    .unwrap();
    let broken_tmpdir = dir.path().join("not-a-directory");
    fs::write(&broken_tmpdir, "file").unwrap();

    let mut command = Command::new(kache_binary());
    command
        .arg(&fake_rustc)
        .arg(format!("@{}", response.display()))
        .current_dir(dir.path())
        .env("TMPDIR", &broken_tmpdir)
        .env("CARGO_INCREMENTAL", "1")
        .env("CARGO_PRIMARY_PACKAGE", "1")
        .env("KACHE_CACHE_DIR", dir.path().join("cache"))
        .env("KACHE_CONFIG", dir.path().join("missing-config.toml"))
        .env("KACHE_PRESERVE_INCREMENTAL", "1");
    if disabled {
        command.env("KACHE_DISABLED", "1");
    } else {
        command.env_remove("KACHE_DISABLED");
    }

    let output = command.output().expect("failed to run kache wrapper");
    assert!(
        !output.status.success(),
        "rewritten response argv must not fall back unsafely"
    );
    assert!(!compiler_marker.exists(), "compiler ran with unsafe argv");
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("materializing rustc response file after rewriting incremental arguments"),
        "unexpected error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_unchanged_response_transport_failure_uses_original_argv() {
    let dir = tempfile::tempdir().unwrap();
    let fake_rustc = dir.path().join("rustc");
    let argv_dump = dir.path().join("argv.txt");
    fs::write(
        &fake_rustc,
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGV_DUMP\"\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&fake_rustc, fs::Permissions::from_mode(0o755)).unwrap();

    let source = dir.path().join("lib.rs");
    let out_dir = dir.path().join("target/debug/deps");
    let response = dir.path().join("compiler.args");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
    fs::write(
        &response,
        format!(
            "--crate-name\nmutant\n--crate-type\nlib\n{}\n--out-dir\n{}\n--emit=metadata\n",
            source.display(),
            out_dir.display(),
        ),
    )
    .unwrap();
    let broken_tmpdir = dir.path().join("not-a-directory");
    fs::write(&broken_tmpdir, "file").unwrap();
    let response_arg = format!("@{}", response.display());

    let output = Command::new(kache_binary())
        .arg(&fake_rustc)
        .arg(&response_arg)
        .current_dir(dir.path())
        .env("ARGV_DUMP", &argv_dump)
        .env("TMPDIR", &broken_tmpdir)
        .env("KACHE_DISABLED", "1")
        .env("KACHE_CACHE_DIR", dir.path().join("cache"))
        .env("KACHE_CONFIG", dir.path().join("missing-config.toml"))
        .output()
        .expect("failed to run kache wrapper");

    assert!(
        output.status.success(),
        "unchanged response argv should use the original transport: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(argv_dump).unwrap(),
        format!("{response_arg}\n")
    );
}

#[test]
fn preserve_incremental_bypasses_cache_without_stripping_the_flag() {
    let (argv, incremental_env, event) = run_fake_rustc(true, false, false, false);
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
fn force_listed_crate_bypasses_cache_without_stripping_the_flag() {
    // Multi-entry, comma+space separated, listing the fake crate `mutant`.
    let (argv, incremental_env, event) = run_fake_forced_rustc("other, mutant", false, true);
    let event = event.expect("kache did not record the force-list passthrough event");

    assert!(
        argv.iter()
            .any(|arg| { arg.contains("incremental.kache-auto") && arg.ends_with("/rustc") }),
        "force-listed crate must use policy-owned rustc incremental state: {argv:?}"
    );
    assert_eq!(event["result"], "passthrough");
    assert_eq!(
        event["passthrough_reason"],
        "incremental force-list: mutant"
    );
    assert!(!event["fallback"].as_bool().unwrap_or(false));
    // An eligible force-listed crate returns before the store is opened and
    // before the dep-info key pass.
    assert_eq!(event["key_ms"], 0);
    assert_eq!(incremental_env, "1");
}

#[test]
fn unlisted_crate_keeps_the_normal_cache_path() {
    // The force list names a different crate, so `mutant` must behave
    // exactly like the default policy: incremental stripped, cache engaged.
    let (argv, incremental_env, event) = run_fake_forced_rustc("other_crate", false, false);

    assert!(
        !argv.iter().any(|arg| arg.contains("incremental=")),
        "an unlisted crate must keep the default stripping policy: {argv:?}"
    );
    assert_eq!(incremental_env, "0");
    // The fake compiler cannot produce dep-info, so the normal cache path
    // degrades to an ordinary "uncacheable" passthrough here — what matters
    // is that the force-list lane (which strips nothing and computes no key)
    // was NOT taken for a crate the list does not name.
    let event = event.expect("kache did not record an event for the unlisted crate");
    let reason = event["passthrough_reason"].as_str().unwrap_or_default();
    assert!(
        !reason.starts_with("incremental force-list"),
        "unlisted crate must not take the force-list passthrough lane: {event}"
    );
    assert!(
        reason.starts_with("uncacheable|"),
        "unlisted crate must reach the normal cache path (which the fake \
         compiler then fails out of): {event}"
    );
}

#[test]
fn disabled_mode_ignores_the_force_list_and_strips_incremental() {
    let (argv, incremental_env, event) = run_fake_forced_rustc("mutant", true, true);

    assert!(
        !argv.iter().any(|arg| arg.contains("incremental=")),
        "disabled mode must not preserve a force-listed Cargo path: {argv:?}"
    );
    assert_eq!(incremental_env, "0");
    assert!(
        event.is_none(),
        "disabled mode should not write cache events"
    );
}

#[test]
fn default_wrapper_behavior_still_strips_incremental() {
    let (argv, incremental_env, _event) = run_fake_rustc(false, false, false, false);

    assert!(
        !argv.iter().any(|arg| arg.contains("incremental=")),
        "default behavior must retain the existing safety policy: {argv:?}"
    );
    assert_eq!(incremental_env, "0");
}

#[test]
fn disabled_mode_honors_preserve_incremental() {
    let (argv, incremental_env, event) = run_fake_rustc(true, true, false, false);

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
    let (argv, incremental_env, event) = run_fake_rustc(true, false, true, false);

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

#[test]
fn disabled_mode_preserves_isolated_incremental_from_response_file() {
    let (argv, incremental_env, event) = run_fake_rustc(true, true, false, true);

    assert!(
        argv.iter()
            .any(|arg| arg.starts_with("-Cincremental=") && arg.ends_with(".kache-preserved")),
        "disabled mode must isolate response-file incremental state: {argv:?}"
    );
    assert_eq!(incremental_env, "1");
    assert!(
        event.is_none(),
        "disabled mode should not write cache events"
    );
}

#[test]
fn disabled_mode_strips_incremental_from_response_file_by_default() {
    let (argv, incremental_env, event) = run_fake_rustc(false, true, false, true);

    assert!(
        !argv.iter().any(|arg| arg.contains("incremental=")),
        "disabled mode must strip response-file incremental state: {argv:?}"
    );
    assert_eq!(incremental_env, "0");
    assert!(
        event.is_none(),
        "disabled mode should not write cache events"
    );
}

#[test]
fn disabled_mode_strips_response_incremental_for_configured_rustc_driver() {
    let (argv, incremental_env, event) = run_fake_configured_rustc(false, true, true);

    assert!(
        !argv.iter().any(|arg| arg.contains("incremental=")),
        "configured rustc driver must use rustc response semantics: {argv:?}"
    );
    assert_eq!(incremental_env, "0");
    assert!(
        event.is_none(),
        "disabled mode should not write cache events"
    );
}

#[test]
fn disabled_changed_response_transport_failure_is_safe() {
    assert_changed_response_transport_fails_closed(true);
}

#[test]
fn enabled_changed_response_transport_failure_is_safe() {
    assert_changed_response_transport_fails_closed(false);
}

#[test]
fn disabled_unchanged_response_transport_failure_uses_original_argv() {
    assert_unchanged_response_transport_failure_uses_original_argv();
}
