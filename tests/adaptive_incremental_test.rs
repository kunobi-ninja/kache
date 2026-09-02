//! End-to-end coverage for Kache's automatic incremental adaptation.

#![cfg(unix)]

use filetime::FileTime;
use serde_json::Value;
use std::fs;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// Only the env plumbing is shared; this file drives the Cargo-built binary,
// so the bootstrap helpers in `common` stay unused here.
#[allow(dead_code)]
mod common;
use common::hermetic_command;

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

fn fixture_events(cache_dir: &Path) -> Vec<Value> {
    fs::read_to_string(cache_dir.join("events.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|event| event["crate_name"] == "adaptive_fixture")
        .collect()
}

fn build_variant(
    project: &Path,
    cache_dir: &Path,
    target_dir: &Path,
    fallback: &Path,
    source_mtime: i64,
    answer: u64,
) -> Value {
    let source = project.join("src/lib.rs");
    fs::write(&source, format!("pub fn answer() -> u64 {{ {answer} }}\n")).unwrap();
    filetime::set_file_mtime(&source, FileTime::from_unix_time(source_mtime, 0)).unwrap();

    let event_count = fixture_events(cache_dir).len();
    let output = hermetic_command(
        "cargo",
        cache_dir,
        Some(&project.join("missing-kache.toml")),
    )
    .args(["build", "--offline", "--quiet", "--lib"])
    .current_dir(project)
    .env("RUSTC_WRAPPER", kache_binary())
    .env("CARGO_TARGET_DIR", target_dir)
    .env("CARGO_INCREMENTAL", "1")
    .env("KACHE_ADAPTIVE_INCREMENTAL", "1")
    .env("KACHE_FALLBACK", fallback)
    .env("FALLBACK_MARKER", project.join("fallback-used"))
    .env("KACHE_LOG", "kache=debug")
    .env_remove("RUSTC_WORKSPACE_WRAPPER")
    .env_remove("KACHE_CLEAN_INCREMENTAL")
    .env_remove("KACHE_DISABLED")
    .env_remove("KACHE_PRESERVE_INCREMENTAL")
    .output()
    .expect("failed to build adaptive fixture");
    assert!(
        output.status.success(),
        "fixture build failed for variant {answer}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // Execute a tiny direct consumer of the produced rlib. Event transitions
    // alone would not catch stale incremental output.
    let deps = target_dir.join("debug/deps");
    let rlib = fs::read_dir(&deps)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with("libadaptive_fixture-") && name.ends_with(".rlib")
                })
        })
        .expect("fixture rlib was not produced");
    let consumer_source = project.join("consumer.rs");
    let consumer_binary = project.join("consumer");
    fs::write(
        &consumer_source,
        "extern crate adaptive_fixture; fn main() { print!(\"{}\", adaptive_fixture::answer()); }\n",
    )
    .unwrap();
    let consumer_compile = Command::new("rustc")
        .arg(&consumer_source)
        .arg("--edition=2024")
        .arg("--extern")
        .arg(format!("adaptive_fixture={}", rlib.display()))
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("-o")
        .arg(&consumer_binary)
        .output()
        .expect("failed to compile fixture consumer");
    assert!(
        consumer_compile.status.success(),
        "consumer compile failed: {}",
        String::from_utf8_lossy(&consumer_compile.stderr),
    );
    let consumer = Command::new(&consumer_binary)
        .output()
        .expect("failed to run fixture consumer");
    assert!(consumer.status.success());
    assert_eq!(
        String::from_utf8_lossy(&consumer.stdout),
        answer.to_string()
    );

    let events = fixture_events(cache_dir);
    assert_eq!(
        events.len(),
        event_count + 1,
        "each source edit should compile the fixture exactly once; events:\n{}",
        fs::read_to_string(cache_dir.join("events.jsonl")).unwrap_or_default(),
    );
    events.into_iter().nth(event_count).unwrap()
}

fn assert_passthrough(event: &Value, reason: &str) {
    assert_eq!(event["result"], "passthrough", "event: {event:#}");
    assert!(
        event["passthrough_reason"]
            .as_str()
            .is_some_and(|value| value.contains(reason)),
        "expected passthrough reason containing {reason:?}; event: {event:#}",
    );
}

fn finished_output(mut child: Child, status: ExitStatus) -> Output {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stdout.take() {
        pipe.read_to_end(&mut stdout).unwrap();
    }
    if let Some(mut pipe) = child.stderr.take() {
        pipe.read_to_end(&mut stderr).unwrap();
    }
    Output {
        status,
        stdout,
        stderr,
    }
}

fn wait_with_timeout(mut child: Child, timeout: Duration) -> Result<Output, String> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(finished_output(child, status)),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                let _ = child.kill();
                let status = child.wait().map_err(|error| error.to_string())?;
                let output = finished_output(child, status);
                return Err(format!(
                    "child timed out\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                ));
            }
            Err(error) => return Err(format!("polling child: {error}")),
        }
    }
}

#[test]
fn source_churn_adapts_then_returns_to_exact_cache_hits() {
    let project = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let target = project.path().join("target");
    let fallback = project.path().join("fallback");
    let fallback_marker = project.path().join("fallback-used");
    let config_path = project.path().join("missing-kache.toml");
    fs::create_dir(project.path().join("src")).unwrap();
    fs::write(
        &fallback,
        "#!/bin/sh\ncase \" $* \" in *\" --crate-name adaptive_fixture \"*) : > \"$FALLBACK_MARKER\";; esac\nexec \"$@\"\n",
    )
    .unwrap();
    fs::set_permissions(&fallback, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(
        project.path().join("Cargo.toml"),
        "[package]\nname = \"adaptive-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .unwrap();

    // Future, monotonically increasing mtimes force Cargo to revisit every
    // variant without sleeps or deleting either incremental-state directory.
    let first_mtime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 3_600;
    let mut tick = 0;
    let mut build = |answer| {
        let event = build_variant(
            project.path(),
            cache.path(),
            &target,
            &fallback,
            first_mtime + tick,
            answer,
        );
        tick += 1;
        event
    };

    let first = build(1);
    assert_eq!(first["result"], "miss", "event: {first:#}");
    assert_eq!(first["compiler_runs"], 1);

    let seed = build(2);
    assert_passthrough(&seed, "adaptive seed");
    assert_eq!(seed["compiler_runs"], 1);
    assert!(!seed["fallback"].as_bool().unwrap_or(false));

    let active = build(3);
    assert_passthrough(&active, "adaptive active");
    assert_eq!(active["key_ms"], 0, "event: {active:#}");

    // A newly configured exclusion must win over already-active policy state
    // and preserve the ordinary fallback contract.
    fs::write(&config_path, "[cache]\nexclude = [\"src/lib.rs\"]\n").unwrap();
    let excluded = build(4);
    assert_passthrough(&excluded, "source excluded");
    assert!(excluded["fallback"].as_bool().unwrap_or(false));
    assert!(fallback_marker.exists(), "configured fallback was bypassed");
    fs::remove_file(&config_path).unwrap();
    fs::remove_file(&fallback_marker).unwrap();

    // Exclusion resets learned state. The next miss learns normally, then a
    // second source-only miss seeds a fresh adaptive run.
    let after_exclusion = build(5);
    assert_eq!(
        after_exclusion["result"], "miss",
        "event: {after_exclusion:#}"
    );
    let reseed = build(6);
    assert_passthrough(&reseed, "adaptive seed");

    for answer in 7..=14 {
        let active = build(answer);
        assert_passthrough(&active, "adaptive active");
        assert_eq!(active["compiler_runs"], 1);
        assert!(!active["fallback"].as_bool().unwrap_or(false));
        assert_eq!(
            active["key_ms"], 0,
            "active adaptation must run before cache-key work: {active:#}",
        );
    }

    let policy_root = target.join("debug/incremental.kache-auto");
    assert!(policy_root.is_dir(), "missing {}", policy_root.display());
    assert!(
        fs::read_dir(&policy_root).unwrap().next().is_some(),
        "adaptive policy tree is empty: {}",
        policy_root.display(),
    );

    let restored = build(1);
    assert_eq!(restored["result"], "local_hit", "event: {restored:#}");
    assert_eq!(restored["compiler_runs"], 0);

    // A hit clears the learned churn state, so a new variant starts learning
    // with a normal stored miss rather than another adaptive passthrough.
    let after_reset = build(15);
    assert_eq!(after_reset["result"], "miss", "event: {after_reset:#}");
    assert_eq!(after_reset["compiler_runs"], 1);
}

#[test]
fn user_facing_executable_without_fallback_uses_default_immediate_adaptive_lane() {
    let project = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let target = project.path().join("target");

    fs::create_dir(project.path().join("src")).unwrap();
    fs::write(
        project.path().join("Cargo.toml"),
        "[package]\nname = \"adaptive-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .unwrap();
    fs::write(
        project.path().join("src/main.rs"),
        "fn main() { print!(\"adaptive executable\"); }\n",
    )
    .unwrap();
    let output = hermetic_command(
        "cargo",
        cache.path(),
        Some(&project.path().join("missing-kache.toml")),
    )
    .args(["build", "--offline", "--quiet", "--bin", "adaptive-fixture"])
    .current_dir(project.path())
    .env("RUSTC_WRAPPER", kache_binary())
    .env("CARGO_TARGET_DIR", &target)
    .env("CARGO_INCREMENTAL", "1")
    .env("KACHE_CACHE_EXECUTABLES", "0")
    .env_remove("KACHE_FALLBACK")
    .env_remove("RUSTC_WORKSPACE_WRAPPER")
    .env_remove("KACHE_ADAPTIVE_INCREMENTAL")
    .env_remove("KACHE_CLEAN_INCREMENTAL")
    .env_remove("KACHE_DISABLED")
    .env_remove("KACHE_PRESERVE_INCREMENTAL")
    .output()
    .expect("failed to build adaptive executable fixture");
    assert!(
        output.status.success(),
        "adaptive executable build failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let executable = target.join("debug/adaptive-fixture");
    let run = Command::new(&executable)
        .output()
        .expect("failed to run adaptive executable fixture");
    assert!(run.status.success());
    assert_eq!(String::from_utf8_lossy(&run.stdout), "adaptive executable");

    let events = fixture_events(cache.path());
    assert_eq!(events.len(), 1, "unexpected fixture events: {events:#?}");
    let event = &events[0];
    assert_passthrough(event, "adaptive passthrough");
    assert_eq!(event["compiler_runs"], 1, "event: {event:#}");
    assert!(
        !event["fallback"].as_bool().unwrap_or(false),
        "event: {event:#}"
    );

    let policy_version = target.join("debug/incremental.kache-auto/v1");
    let unit_dirs: Vec<_> = fs::read_dir(&policy_version)
        .unwrap_or_else(|error| panic!("missing {}: {error}", policy_version.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    assert_eq!(unit_dirs.len(), 1, "adaptive unit dirs: {unit_dirs:#?}");
    let rustc_dir = unit_dirs[0].join("rustc");
    assert!(rustc_dir.is_dir(), "missing {}", rustc_dir.display());
    assert!(
        fs::read_dir(&rustc_dir).unwrap().next().is_some(),
        "isolated rustc incremental state is empty: {}",
        rustc_dir.display(),
    );
}

#[test]
fn user_facing_executable_preserves_configured_fallback_contract() {
    let project = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let target = project.path().join("target");
    let fallback = project.path().join("fallback");
    let fallback_marker = project.path().join("fallback-used");

    fs::create_dir(project.path().join("src")).unwrap();
    fs::write(
        &fallback,
        "#!/bin/sh\ncase \" $* \" in *\" --crate-name adaptive_fixture \"*) : > \"$FALLBACK_MARKER\";; esac\nexec \"$@\"\n",
    )
    .unwrap();
    fs::set_permissions(&fallback, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(
        project.path().join("Cargo.toml"),
        "[package]\nname = \"adaptive-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .unwrap();
    fs::write(
        project.path().join("src/main.rs"),
        "fn main() { print!(\"fallback executable\"); }\n",
    )
    .unwrap();

    let output = hermetic_command(
        "cargo",
        cache.path(),
        Some(&project.path().join("missing-kache.toml")),
    )
    .args(["build", "--offline", "--quiet", "--bin", "adaptive-fixture"])
    .current_dir(project.path())
    .env("RUSTC_WRAPPER", kache_binary())
    .env("CARGO_TARGET_DIR", &target)
    .env("CARGO_INCREMENTAL", "1")
    .env("KACHE_CACHE_EXECUTABLES", "0")
    .env("KACHE_FALLBACK", &fallback)
    .env("FALLBACK_MARKER", &fallback_marker)
    .env_remove("RUSTC_WORKSPACE_WRAPPER")
    .env_remove("KACHE_ADAPTIVE_INCREMENTAL")
    .env_remove("KACHE_CLEAN_INCREMENTAL")
    .env_remove("KACHE_DISABLED")
    .env_remove("KACHE_PRESERVE_INCREMENTAL")
    .output()
    .expect("failed to build fallback executable fixture");
    assert!(
        output.status.success(),
        "fallback executable build failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(fallback_marker.exists(), "configured fallback was bypassed");

    let events = fixture_events(cache.path());
    assert_eq!(events.len(), 1, "unexpected fixture events: {events:#?}");
    let event = &events[0];
    assert_passthrough(event, "user-facing executable");
    assert!(
        event["fallback"].as_bool().unwrap_or(false),
        "event: {event:#}"
    );
}

#[test]
fn contended_immediate_lane_never_shares_incremental_state() {
    let project = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let profile = project.path().join("target/debug");
    let deps = profile.join("deps");
    let incremental = profile.join("incremental");
    fs::create_dir_all(&deps).unwrap();
    fs::create_dir(&incremental).unwrap();

    let source = project.path().join("main.rs");
    let fake_rustc = project.path().join("rustc");
    let owner = project.path().join("compiler-owner");
    let ready = project.path().join("first-ready");
    let release = project.path().join("release-first");
    let contender = project.path().join("contender-seen");
    let first_argv = project.path().join("first-argv.txt");
    let second_argv = project.path().join("second-argv.txt");
    fs::write(&source, "fn main() {}\n").unwrap();
    fs::write(
        &fake_rustc,
        r#"#!/bin/sh
printf '%s\n' "$@" > "$ARGV_DUMP"
incremental=
for arg in "$@"; do
    case "$arg" in
        -Cincremental=*) incremental=${arg#-Cincremental=} ;;
        --codegen=incremental=*) incremental=${arg#--codegen=incremental=} ;;
    esac
done
if mkdir "$OWNER_DIR" 2>/dev/null; then
    if [ -n "$incremental" ]; then
        mkdir -p "$incremental" || exit 91
        printf 'seeded' > "$incremental/fake-state" || exit 92
    fi
    : > "$READY_FILE"
    attempts=0
    while [ ! -f "$RELEASE_FILE" ] && [ "$attempts" -lt 600 ]; do
        sleep 0.05
        attempts=$((attempts + 1))
    done
    [ -f "$RELEASE_FILE" ] || exit 93
else
    : > "$CONTENDER_FILE"
fi
exit 0
"#,
    )
    .unwrap();
    fs::set_permissions(&fake_rustc, fs::Permissions::from_mode(0o755)).unwrap();

    let wrapper = |argv_dump: &Path| {
        let mut command = hermetic_command(
            kache_binary(),
            cache.path(),
            Some(&project.path().join("missing-kache.toml")),
        );
        command
            .arg(&fake_rustc)
            .args(["--crate-name", "adaptive_fixture", "--crate-type", "bin"])
            .arg(&source)
            .arg("--out-dir")
            .arg(&deps)
            .arg("--emit=link")
            .arg("-Cextra-filename=-1234abcd")
            .arg(format!("-Cincremental={}", incremental.display()))
            .current_dir(project.path())
            .env("ARGV_DUMP", argv_dump)
            .env("OWNER_DIR", &owner)
            .env("READY_FILE", &ready)
            .env("RELEASE_FILE", &release)
            .env("CONTENDER_FILE", &contender)
            .env("CARGO_PRIMARY_PACKAGE", "1")
            .env("CARGO_INCREMENTAL", "1")
            .env("KACHE_CACHE_EXECUTABLES", "0")
            .env("KACHE_LOG", "kache=warn")
            .env_remove("RUSTC_WORKSPACE_WRAPPER")
            .env_remove("KACHE_ADAPTIVE_INCREMENTAL")
            .env_remove("KACHE_CLEAN_INCREMENTAL")
            .env_remove("KACHE_DISABLED")
            .env_remove("KACHE_FALLBACK")
            .env_remove("KACHE_PRESERVE_INCREMENTAL")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    };

    let mut first = wrapper(&first_argv).spawn().unwrap();
    let ready_deadline = Instant::now() + Duration::from_secs(30);
    while !ready.exists() {
        if let Some(status) = first.try_wait().unwrap() {
            let output = finished_output(first, status);
            panic!(
                "first wrapper exited before holding incremental state\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        if Instant::now() >= ready_deadline {
            let _ = first.kill();
            let status = first.wait().unwrap();
            let output = finished_output(first, status);
            panic!(
                "first fake rustc did not reach its blocking point\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        thread::sleep(Duration::from_millis(20));
    }

    let second = wrapper(&second_argv).spawn().unwrap();
    // A cold local-store/daemon fallback can consume its own five-second
    // deadline. Keep the process bound above that without relying on the fake
    // compiler's longer self-release deadline.
    let second_output = match wait_with_timeout(second, Duration::from_secs(20)) {
        Ok(output) => output,
        Err(error) => {
            fs::write(&release, b"release").unwrap();
            let _ = first.kill();
            let _ = first.wait();
            panic!("contending wrapper did not fall back promptly: {error}");
        }
    };
    fs::write(&release, b"release").unwrap();
    let first_output = wait_with_timeout(first, Duration::from_secs(10)).unwrap();

    assert!(
        first_output.status.success(),
        "first wrapper failed: {}",
        String::from_utf8_lossy(&first_output.stderr),
    );
    assert!(
        second_output.status.success(),
        "contending wrapper failed: {}",
        String::from_utf8_lossy(&second_output.stderr),
    );
    assert!(contender.exists(), "fake rustc never saw the contender");

    let first_args = fs::read_to_string(&first_argv).unwrap();
    assert!(
        first_args.lines().any(|arg| {
            arg.starts_with("-Cincremental=")
                && arg.contains("/incremental.kache-auto/v1/")
                && arg.ends_with("/rustc")
        }),
        "first wrapper did not hold isolated incremental state: {first_args:?}",
    );
    let second_args = fs::read_to_string(&second_argv).unwrap();
    assert!(
        !second_args.lines().any(|arg| arg.contains("incremental=")),
        "contender reused incremental state instead of falling back: {second_args:?}",
    );
}
