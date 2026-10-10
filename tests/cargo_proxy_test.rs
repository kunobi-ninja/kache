//! End-to-end regression for canonical duplicate Cargo config discovery (#766).

#![cfg(unix)]

use serde_json::Value;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const KACHE_BIN: &str = env!("CARGO_BIN_EXE_kache");

// Only the env plumbing is shared; this file drives the Cargo-built binary,
// so the bootstrap helpers in `common` stay unused here.
#[allow(dead_code)]
mod common;
use common::hermetic_command;

/// `kache cargo` wired to a throwaway HOME, cache, and target directory.
fn proxied_cargo(home: &Path, cache: &Path, target: &Path) -> Command {
    let mut command = hermetic_command(KACHE_BIN, cache, Some(&cache.join("config.toml")));
    command
        .env("HOME", home)
        .env("CARGO_HOME", home.join(".cargo"))
        .env("CARGO_TARGET_DIR", target)
        .env("CARGO_INCREMENTAL", "0")
        .env("RUSTC_WRAPPER", KACHE_BIN)
        .env("KACHE_LOG", "off")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        // An inherited build-dir turns the proxy's worktree isolation off.
        .env_remove("CARGO_BUILD_BUILD_DIR");
    command
}

#[test]
fn cargo_shim_holds_target_lease_for_whole_test() {
    let dir = tempfile::tempdir().unwrap();
    let project = dir.path().join("project");
    let target = project.join("target");
    let cache = dir.path().join("cache");
    let shims = dir.path().join("shims");
    let real = dir.path().join("real");
    let ready = dir.path().join("ready");
    let go = dir.path().join("go");
    std::fs::create_dir_all(target.join("debug")).unwrap();
    std::fs::create_dir_all(&shims).unwrap();
    std::fs::create_dir_all(&real).unwrap();
    std::fs::write(project.join("Cargo.toml"), "[workspace]\n").unwrap();
    std::fs::write(target.join("debug/output"), "still needed").unwrap();
    std::os::unix::fs::symlink(KACHE_BIN, shims.join("cargo")).unwrap();
    let fake = real.join("cargo");
    std::fs::write(
        &fake,
        "#!/bin/sh\n[ \"$1\" = test ] || exit 12\n: > \"$READY\"\nwhile [ ! -e \"$GO\" ]; do sleep 0.02; done\n[ -e \"$TARGET/debug/output\" ] || exit 13\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();

    let base_path = std::env::var_os("PATH").unwrap();
    let path = std::env::join_paths(
        [shims.clone(), real.clone()]
            .into_iter()
            .chain(std::env::split_paths(&base_path)),
    )
    .unwrap();
    let mut command = hermetic_command(shims.join("cargo"), &cache, None);
    let mut child = command
        .arg("test")
        .current_dir(&project)
        .env("PATH", path)
        .env("READY", &ready)
        .env("GO", &go)
        .env("TARGET", &target)
        .env_remove("KACHE_REAL_CARGO")
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while !ready.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "Cargo exited before test started"
        );
        assert!(Instant::now() < deadline, "Cargo did not start the test");
        std::thread::sleep(Duration::from_millis(10));
    }

    let lease = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(cache.join("target-use.lock"))
        .unwrap();
    assert!(
        matches!(lease.try_lock(), Err(std::fs::TryLockError::WouldBlock)),
        "the cargo shim released the target while its test was running"
    );
    assert!(target.join("debug/output").exists());

    std::fs::write(&go, "").unwrap();
    assert!(child.wait().unwrap().success());
    lease.lock().unwrap();
    lease.unlock().unwrap();
}

#[test]
fn cargo_shim_says_it_waits_for_target_cleanup_unless_quiet() {
    const WAITING: &str = "kache: waiting for target cleanup to finish\n";
    for quiet in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let cache = dir.path().join("cache");
        let shims = dir.path().join("shims");
        let real = dir.path().join("real");
        let started = dir.path().join("started");
        let stdout = dir.path().join("stdout");
        let stderr = dir.path().join("stderr");
        for path in [&project, &cache, &shims, &real] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(project.join("Cargo.toml"), "[workspace]\n").unwrap();
        std::os::unix::fs::symlink(KACHE_BIN, shims.join("cargo")).unwrap();
        kache_fs::testutil::write_executable(&real.join("cargo"), "#!/bin/sh\n: > \"$STARTED\"\n");
        // Held the way `kache clean` and the daemon hold it while they delete.
        let cleanup = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(cache.join("target-use.lock"))
            .unwrap();
        cleanup.lock().unwrap();

        let base_path = std::env::var_os("PATH").unwrap();
        let path = std::env::join_paths(
            [shims.clone(), real.clone()]
                .into_iter()
                .chain(std::env::split_paths(&base_path)),
        )
        .unwrap();
        let mut command = hermetic_command(shims.join("cargo"), &cache, None);
        command
            .arg("build")
            .args(quiet.then_some("-q"))
            .current_dir(&project)
            .env("PATH", path)
            .env("STARTED", &started)
            .env("KACHE_LOG", "off")
            .env_remove("KACHE_REAL_CARGO")
            .stdout(std::fs::File::create(&stdout).unwrap())
            .stderr(std::fs::File::create(&stderr).unwrap());
        for (name, _) in std::env::vars_os() {
            let name_text = name.to_string_lossy();
            if name_text.starts_with("KACHE_S3_") || name_text.starts_with("AWS_") {
                command.env_remove(&name);
            }
        }
        let mut child = command.spawn().unwrap();
        if quiet {
            std::thread::sleep(Duration::from_millis(500));
        } else {
            let deadline = Instant::now() + Duration::from_secs(20);
            while !std::fs::read_to_string(&stderr).unwrap().contains(WAITING) {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "the shim exited without waiting"
                );
                assert!(Instant::now() < deadline, "the shim never said it waits");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        assert!(
            !started.exists(),
            "quiet={quiet}: Cargo started while cleanup held the lock"
        );

        cleanup.unlock().unwrap();
        assert!(child.wait().unwrap().success(), "quiet={quiet}");
        assert!(started.exists(), "quiet={quiet}: Cargo never started");
        let said = std::fs::read_to_string(&stderr).unwrap();
        assert_eq!(said.matches(WAITING).count(), usize::from(!quiet), "{said}");
        assert_eq!(
            std::fs::read_to_string(&stdout).unwrap(),
            "",
            "quiet={quiet}"
        );
    }
}

#[test]
fn canonical_cargo_home_alias_keeps_existing_cargo_unit_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = home.join("work/project");
    let cargo_home = home.join(".cargo");
    let cache = dir.path().join("cache");
    let cold_target = dir.path().join("target-cold");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::create_dir_all(&cargo_home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"proxy_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(project.join("src/lib.rs"), "pub fn answer() -> u8 { 42 }\n").unwrap();
    std::fs::write(
        cargo_home.join("config.toml"),
        "[build]\nrustflags = [\"--cfg\", \"kache_proxy_fixture\"]\n",
    )
    .unwrap();

    let mut cold = proxied_cargo(&home, &cache, &cold_target);
    cold.args(["cargo", "--", "check", "--quiet"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"));
    let cold = cold.output().unwrap();
    assert!(
        cold.status.success(),
        "cold cargo failed: {}",
        String::from_utf8_lossy(&cold.stderr)
    );

    std::os::unix::fs::symlink(&cargo_home, home.join("work/.cargo")).unwrap();

    let mut warm = proxied_cargo(&home, &cache, &cold_target);
    warm.args(["cargo", "--", "check", "--verbose", "--color=never"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"))
        .env("CARGO_TERM_COLOR", "always");
    let warm = warm.output().unwrap();
    assert!(
        warm.status.success(),
        "proxied cargo failed: {}",
        String::from_utf8_lossy(&warm.stderr)
    );
    assert!(
        String::from_utf8_lossy(&warm.stderr).contains("Fresh proxy_fixture"),
        "Cargo should keep the existing unit fresh: {}",
        String::from_utf8_lossy(&warm.stderr)
    );

    let events: Vec<Value> = std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|event: &Value| event["crate_name"] == "proxy_fixture")
        .collect();
    assert_eq!(
        events.len(),
        1,
        "a fresh Cargo unit must not invoke the wrapper again: {events:#?}"
    );
    assert_eq!(events[0]["result"], "miss", "events: {events:#?}");
    assert_eq!(events[0]["compiler_runs"], 1, "events: {events:#?}");
}

fn write_key_fixture(project: &Path) {
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"proxy_key_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        project.join("src/lib.rs"),
        "#[cfg(not(kache_proxy_fixture))]\ncompile_error!(\"config rustflags were lost\");\n\
         pub fn answer() -> u8 { 42 }\n",
    )
    .unwrap();
}

#[test]
fn collapsed_rustflags_stay_out_of_the_environment_cargo_passes_on() {
    // Canonical, so the plain clone has no `/var` vs `/private/var` alias of
    // CARGO_HOME on macOS.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let home = root.join("home");
    let cargo_home = home.join(".cargo");
    let cache = root.join("cache");
    let plain = home.join("plain/project");
    let aliased = home.join("aliased/project");
    std::fs::create_dir_all(&cargo_home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(
        cargo_home.join("config.toml"),
        "[build]\nrustflags = [\"--cfg\", \"kache_proxy_fixture\"]\n",
    )
    .unwrap();
    write_key_fixture(&plain);
    write_key_fixture(&aliased);
    std::os::unix::fs::symlink(&cargo_home, home.join("aliased/.cargo")).unwrap();

    // Both clones give rustc the same arguments. Only the aliased clone needs
    // collapsed flags, and only Cargo may see them: the wrapper keys
    // `CARGO_ENCODED_RUSTFLAGS` from its environment, and a nested Cargo
    // started by rustc or a proc macro would adopt it as its own flags.
    for (project, target) in [(&plain, "target-plain"), (&aliased, "target-aliased")] {
        let mut command = proxied_cargo(&home, &cache, &root.join(target));
        command
            .args(["cargo", "--", "check", "--quiet"])
            .current_dir(project)
            .env("KACHE_REAL_CARGO", env!("CARGO"));
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "proxied cargo failed in {}: {}",
            project.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let events: Vec<Value> = std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|event: &Value| event["crate_name"] == "proxy_key_fixture")
        .collect();
    assert_eq!(events.len(), 2, "events: {events:#?}");
    assert_eq!(events[0]["result"], "miss", "events: {events:#?}");
    assert_eq!(
        events[1]["result"], "local_hit",
        "the wrapper saw the proxy's rustflags in its environment: {events:#?}"
    );
    assert_eq!(events[0]["cache_key"], events[1]["cache_key"]);
}

#[test]
fn worktree_build_dir_stays_out_of_nested_cargo_builds() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let home = root.join("home");
    let cache = root.join("cache");
    let guest = root.join("guest");
    let host = root.join("host");
    let nested_target = root.join("nested-target");
    std::fs::create_dir_all(home.join(".cargo")).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::create_dir_all(guest.join("src")).unwrap();
    std::fs::create_dir_all(host.join("src")).unwrap();
    std::fs::write(
        guest.join("Cargo.toml"),
        "[package]\nname = \"proxy_guest\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\n[workspace]\n",
    )
    .unwrap();
    std::fs::write(guest.join("src/lib.rs"), "pub fn guest() {}\n").unwrap();
    std::fs::write(
        host.join("Cargo.toml"),
        "[package]\nname = \"proxy_host\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(host.join("src/lib.rs"), "pub fn host() {}\n").unwrap();
    // The usual guest-build shape: a build script runs Cargo on another
    // workspace and picks a private target directory for it.
    std::fs::write(
        host.join("build.rs"),
        format!(
            "fn main() {{\n    \
                 let status = std::process::Command::new(std::env::var_os(\"CARGO\").unwrap())\n        \
                     .args([\"build\", \"--quiet\", \"--manifest-path\", {:?}, \"--target-dir\", {:?}])\n        \
                     .status()\n        \
                     .unwrap();\n    \
                 assert!(status.success(), \"nested cargo failed\");\n\
             }}\n",
            guest.join("Cargo.toml"),
            nested_target,
        ),
    )
    .unwrap();

    let mut command = proxied_cargo(&home, &cache, &root.join("target"));
    command
        .args(["cargo", "--", "build", "--quiet"])
        .current_dir(&host)
        .env("KACHE_REAL_CARGO", env!("CARGO"))
        .env_remove("RUSTC_WRAPPER");
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "proxied cargo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(
        host.join("target/debug/.fingerprint").is_dir(),
        "the proxied build keeps its own intermediates in the worktree"
    );
    assert!(
        nested_target.join("debug/.fingerprint").is_dir(),
        "the nested build must keep its intermediates where its build script put them"
    );
    assert!(
        !guest.join("target").exists(),
        "the proxy's build-dir redirected a nested Cargo build into its source tree"
    );
}

#[test]
fn collapsed_single_use_rustc_option_passes_cargo_target_probe() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let home = root.join("home");
    let cargo_home = home.join(".cargo");
    let cache = root.join("cache");
    let project = home.join("work/project");
    std::fs::create_dir_all(&cargo_home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    // rustc rejects a repeated `--diagnostic-width`, as it does `--sysroot`.
    // Cargo's first target-info probe runs before `cfg` keys match, so a
    // `cfg(all())` override would leave it with the duplicated source.
    std::fs::write(
        cargo_home.join("config.toml"),
        "[build]\nrustflags = [\"--diagnostic-width\", \"80\", \"--cfg\", \"kache_proxy_fixture\"]\n",
    )
    .unwrap();
    write_key_fixture(&project);
    std::os::unix::fs::symlink(&cargo_home, home.join("work/.cargo")).unwrap();

    let mut command = proxied_cargo(&home, &cache, &root.join("target"));
    command
        .args(["cargo", "--", "check", "--quiet"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"));
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "proxied cargo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A stand-in Cargo that reports 1.91.0 under the `+new` selector and
/// `version` otherwise, and records the arguments of any other command.
fn versioned_fake_cargo(path: &Path, version: &str) {
    kache_fs::testutil::write_executable(
        path,
        format!(
            "#!/bin/sh\ncase \"$*\" in\n  \
                 '+new -V') echo 'cargo 1.91.0 (0000000 2025-10-10)' ;;\n  \
                 *-V) echo 'cargo {version} (0000000 2020-01-01)' ;;\n  \
                 *) printf '%s' \"$*\" > \"$KACHE_TEST_CAPTURE\" ;;\n\
             esac\n"
        ),
    );
}

#[test]
fn build_dir_override_needs_a_cargo_that_knows_the_key() {
    let isolated = "--config build.build-dir=\"{workspace-root}/target\"";
    for (args, version, expected) in [
        // Before 1.63 Cargo rejects `--config`; before 1.91 it warns about the
        // unknown key. Both ignored the exported variable this replaced.
        (&["build"][..], "1.62.1", "build".to_string()),
        (&["build"], "1.90.0", "build".to_string()),
        (&["build"], "1.91.0", format!("{isolated} build")),
        (
            &["+new", "check"],
            "1.62.1",
            format!("+new {isolated} check"),
        ),
        (&["build"], "unknown", "build".to_string()),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let project = dir.path().join("project");
        let fake_cargo = dir.path().join("cargo");
        let capture = dir.path().join("captured-args");
        std::fs::create_dir_all(home.join(".cargo")).unwrap();
        std::fs::create_dir_all(&project).unwrap();
        versioned_fake_cargo(&fake_cargo, version);

        let output = Command::new(KACHE_BIN)
            .arg("cargo")
            .arg("--")
            .args(args)
            .current_dir(&project)
            .env("HOME", &home)
            .env("CARGO_HOME", home.join(".cargo"))
            .env("KACHE_REAL_CARGO", &fake_cargo)
            .env("KACHE_TEST_CAPTURE", &capture)
            .env_remove("CARGO_BUILD_BUILD_DIR")
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_RUSTFLAGS")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "proxy failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            std::fs::read_to_string(&capture).unwrap(),
            expected,
            "args {args:?} on Cargo {version}"
        );
    }
}

#[test]
fn explicit_rustflags_keep_their_cargo_precedence() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = home.join("work/project");
    let cargo_home = home.join(".cargo");
    let cache = dir.path().join("cache");
    let target = dir.path().join("target");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::create_dir_all(&cargo_home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname = \"proxy_env_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(
        project.join("src/lib.rs"),
        "#[cfg(not(from_env))]\ncompile_error!(\"explicit RUSTFLAGS was replaced\");\n",
    )
    .unwrap();
    std::fs::write(
        cargo_home.join("config.toml"),
        "[build]\nrustflags = [\"--cfg\", \"from_config\"]\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(&cargo_home, home.join("work/.cargo")).unwrap();

    let mut command = proxied_cargo(&home, &cache, &target);
    command
        .args(["cargo", "--", "check", "--quiet"])
        .current_dir(&project)
        .env("KACHE_REAL_CARGO", env!("CARGO"));
    command.env("RUSTFLAGS", "--cfg from_env");
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "explicit RUSTFLAGS lost precedence: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn non_utf8_cargo_argument_passes_through_without_cli_panic() {
    let dir = tempfile::tempdir().unwrap();
    let fake_cargo = dir.path().join("cargo");
    kache_fs::testutil::write_executable(&fake_cargo, "#!/bin/sh\nexit 0\n");

    let output = Command::new(KACHE_BIN)
        .args(["cargo", "--", "build"])
        .arg(OsString::from_vec(vec![b'f', b'o', 0x80]))
        .env("KACHE_REAL_CARGO", &fake_cargo)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "non-UTF-8 passthrough failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn non_utf8_wrapper_argument_fails_closed_before_compiler_execution() {
    let dir = tempfile::tempdir().unwrap();
    let fake_rustc = dir.path().join("rustc");
    let sentinel = dir.path().join("compiler-ran");
    kache_fs::testutil::write_executable(
        &fake_rustc,
        format!("#!/bin/sh\ntouch '{}'\n", sentinel.display()),
    );

    let output = Command::new(KACHE_BIN)
        .arg(&fake_rustc)
        .arg(OsString::from_vec(vec![b's', b'r', b'c', b'/', 0x80]))
        .env("RUSTC", &fake_rustc)
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!sentinel.exists(), "unsafe compiler fallback was executed");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("cannot be cached safely"),
        "unexpected error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn configured_build_dir_reaches_cargo_without_proxy_override() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    let config_dir = project.join(".cargo");
    let fake_cargo = dir.path().join("cargo");
    let capture = dir.path().join("captured-build-dir");
    std::fs::create_dir_all(home.join(".cargo")).unwrap();
    std::fs::create_dir_all(&config_dir).unwrap();
    std::fs::write(
        config_dir.join("config.toml"),
        "[build]\nbuild-dir = \"configured-build\"\n",
    )
    .unwrap();
    kache_fs::testutil::write_executable(
        &fake_cargo,
        "#!/bin/sh\nprintf '%s %s' \"${CARGO_BUILD_BUILD_DIR-unset}\" \"$*\" > \"$KACHE_TEST_CAPTURE\"\n",
    );

    let output = Command::new(KACHE_BIN)
        .args(["cargo", "--", "build"])
        .current_dir(&project)
        .env("HOME", &home)
        .env("CARGO_HOME", home.join(".cargo"))
        .env("KACHE_REAL_CARGO", &fake_cargo)
        .env("KACHE_TEST_CAPTURE", &capture)
        .env_remove("CARGO_BUILD_BUILD_DIR")
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "proxy failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(capture).unwrap(),
        "unset build",
        "a config-defined build-dir must retain Cargo precedence"
    );
}

fn write_shared_target_project(root: &Path, value: &str) {
    std::fs::create_dir_all(root.join("src")).unwrap();
    let manifest = root.join("Cargo.toml");
    let source = root.join("src/main.rs");
    std::fs::write(
        &manifest,
        "[package]\nname = \"proxy_shared_target\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    )
    .unwrap();
    std::fs::write(&source, format!("fn main() {{ print!({value:?}); }}\n")).unwrap();

    // Make both worktrees older than the first produced artifact. This pins
    // Cargo's shared-fingerprint failure deterministically: without a private
    // build-dir, the second worktree is incorrectly declared Fresh.
    let old = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    filetime::set_file_mtime(&manifest, old).unwrap();
    filetime::set_file_mtime(&source, old).unwrap();
}

fn proxied_shared_target_build(project: &Path, home: &Path, cache: &Path, target: &Path) -> Output {
    let mut command = proxied_cargo(home, cache, target);
    command
        .args(["cargo", "--", "build", "--quiet"])
        .current_dir(project)
        .env("KACHE_REAL_CARGO", env!("CARGO"))
        .env("KACHE_CACHE_EXECUTABLES", "1");
    command.output().unwrap()
}

fn run_shared_target_binary(target: &Path) -> String {
    let mut binary = target.join("debug/proxy_shared_target");
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    let output = Command::new(&binary).output().unwrap();
    assert!(output.status.success(), "{} failed", binary.display());
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn cargo_proxy_isolates_fingerprints_while_sharing_final_target_and_kache() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let cache = dir.path().join("cache");
    let shared_target = dir.path().join("shared-target");
    let worktree_a = dir.path().join("worktree-a");
    let worktree_b = dir.path().join("worktree-b");
    let worktree_c = dir.path().join("worktree-c");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    write_shared_target_project(&worktree_a, "alpha");
    write_shared_target_project(&worktree_b, "bravo");
    write_shared_target_project(&worktree_c, "alpha");

    let first = proxied_shared_target_build(&worktree_a, &home, &cache, &shared_target);
    assert!(
        first.status.success(),
        "worktree A failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert_eq!(run_shared_target_binary(&shared_target), "alpha");

    let second = proxied_shared_target_build(&worktree_b, &home, &cache, &shared_target);
    assert!(
        second.status.success(),
        "worktree B failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        run_shared_target_binary(&shared_target),
        "bravo",
        "Cargo reused worktree A's shared fingerprint without invoking Kache"
    );

    let back_to_a = proxied_shared_target_build(&worktree_a, &home, &cache, &shared_target);
    assert!(back_to_a.status.success());
    assert_eq!(
        run_shared_target_binary(&shared_target),
        "alpha",
        "a Fresh worktree-local unit did not refresh the shared final artifact"
    );

    let relocated = proxied_shared_target_build(&worktree_c, &home, &cache, &shared_target);
    assert!(relocated.status.success());
    assert_eq!(run_shared_target_binary(&shared_target), "alpha");

    for worktree in [&worktree_a, &worktree_b, &worktree_c] {
        assert!(
            worktree.join("target/debug/.fingerprint").is_dir(),
            "intermediate fingerprints must be private to {}",
            worktree.display()
        );
    }
    assert!(
        !shared_target.join("debug/.fingerprint").exists(),
        "the shared final target must not contain Cargo fingerprint state"
    );

    let events: Vec<Value> = std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .filter(|event: &Value| event["crate_name"] == "proxy_shared_target")
        .collect();
    assert_eq!(events.len(), 3, "events: {events:#?}");
    assert_eq!(events[0]["result"], "miss");
    assert_ne!(events[1]["result"], "local_hit");
    assert_eq!(events[2]["result"], "local_hit", "events: {events:#?}");
    assert_eq!(events[2]["compiler_runs"], 0, "events: {events:#?}");
    assert_eq!(events[0]["cache_key"], events[2]["cache_key"]);
}

/// A fresh unit is protected by Cargo's graph even with an ancient atime.
/// Feature variants that predate the observation may be collected.
#[test]
fn receipt_cleanup_removes_old_feature_units_and_keeps_next_check_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let project = dir.path().join("project");
    let target = project.join("target");
    let cache = dir.path().join("cache");
    std::fs::create_dir_all(project.join("src")).unwrap();
    std::fs::create_dir_all(home.join(".cargo")).unwrap();
    std::fs::write(
        project.join("Cargo.toml"),
        "[package]\nname='liveness_fixture'\nversion='0.1.0'\nedition='2024'\n[features]\nold=[]\n",
    )
    .unwrap();
    std::fs::write(project.join("src/lib.rs"), "pub fn answer() -> u8 { 42 }\n").unwrap();
    let run = |args: &[&str], enabled: bool| {
        let mut command = proxied_cargo(&home, &cache, &target);
        let output = command
            .current_dir(&project)
            .args(args)
            .env("KACHE_REAL_CARGO", env!("CARGO"))
            .env("KACHE_TARGET_LIVENESS", if enabled { "1" } else { "0" })
            .env("KACHE_AUTO_GC", "0")
            .env("KACHE_LOG", "warn")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        output
    };
    run(
        &["cargo", "--", "check", "--offline", "--features", "old"],
        false,
    );
    run(&["cargo", "--", "check", "--offline"], true);
    // Cargo may materialize its own metadata during the first observation.
    run(&["cargo", "--", "check", "--offline"], true);
    let inspect = run(&["clean", "--units", "--json"], true);
    let json: Value = serde_json::from_slice(&inspect.stdout).unwrap();
    assert_eq!(json["preview"], true);
    assert_eq!(json["targets"].as_array().unwrap().len(), 1, "{json}");
    assert_eq!(json["targets"][0]["plan"]["status"], "ready", "{json}");
    assert!(
        json["targets"][0]["plan"]["units"].as_u64().unwrap() > 0,
        "{json}"
    );
    assert!(json["targets"][0]["plan"]["bytes"].as_u64().unwrap() > 0);
    assert!(json["targets"][0]["plan"]["protected"].as_u64().unwrap() > 0);
    let planned_bytes = json["targets"][0]["plan"]["bytes"].clone();
    // Access time is intentionally misleading for every fingerprint file.
    for unit in std::fs::read_dir(target.join("debug/.fingerprint")).unwrap() {
        for file in std::fs::read_dir(unit.unwrap().path()).unwrap() {
            filetime::set_file_atime(
                file.unwrap().path(),
                filetime::FileTime::from_unix_time(1, 0),
            )
            .unwrap();
        }
    }
    let clean = run(&["clean", "--artifacts", "--json", "--yes"], true);
    let json: Value = serde_json::from_slice(&clean.stdout).unwrap();
    assert_eq!(json["preview"], false);
    assert_eq!(json["targets"][0]["removed"]["bytes"], planned_bytes);
    assert!(
        json["targets"][0]["removed"]["units"].as_u64().unwrap() > 0,
        "{json}"
    );
    let warm = run(
        &["cargo", "--", "check", "--offline", "--message-format=json"],
        true,
    );
    let records: Vec<Value> = String::from_utf8(warm.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let artifacts: Vec<_> = records
        .iter()
        .filter(|r| r["reason"] == "compiler-artifact")
        .collect();
    assert!(!artifacts.is_empty());
    assert!(artifacts.iter().all(|a| a["fresh"] == true), "{records:?}");
    let events: Vec<Value> = std::fs::read_to_string(cache.join("events.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let cleanup: Vec<_> = events
        .iter()
        .filter(|e| e["event"] == "target-cleanup")
        .collect();
    assert_eq!(cleanup.len(), 1);
    assert_eq!(cleanup[0]["removed_bytes"], planned_bytes);
    assert_eq!(
        cleanup[0]["removed_units"],
        json["targets"][0]["removed"]["units"]
    );
    // A source edit invalidates evidence rather than authorizing a rebuild.
    std::fs::write(project.join("src/lib.rs"), "pub fn answer() -> u8 { 43 }\n").unwrap();
    let changed = run(&["clean", "--units", "--json", "--yes"], true);
    let changed: Value = serde_json::from_slice(&changed.stdout).unwrap();
    assert_eq!(changed["targets"][0]["plan"]["units"], 0);
    assert_ne!(changed["targets"][0]["plan"]["status"], "ready");
}

struct ReceiptAcceptance {
    _dir: tempfile::TempDir,
    home: std::path::PathBuf,
    project: std::path::PathBuf,
    target: std::path::PathBuf,
    cache: std::path::PathBuf,
    dependency: std::path::PathBuf,
    script_input: std::path::PathBuf,
}

impl ReceiptAcceptance {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let project = dir.path().join("project");
        let target = project.join("target");
        let cache = dir.path().join("cache");
        let dependency = dir.path().join("external-dependency");
        let script_input = dir.path().join("external-input.txt");
        std::fs::create_dir_all(project.join("src")).unwrap();
        std::fs::create_dir_all(dependency.join("src")).unwrap();
        std::fs::create_dir_all(home.join(".cargo")).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname='receipt_acceptance'\nversion='0.1.0'\nedition='2024'\n\
             [dependencies]\nexternal_dep={path='../external-dependency'}\n\
             [features]\nold=['external_dep/old']\n",
        )
        .unwrap();
        std::fs::write(
            dependency.join("Cargo.toml"),
            "[package]\nname='external_dep'\nversion='0.1.0'\nedition='2024'\n[features]\nold=[]\n",
        )
        .unwrap();
        std::fs::write(
            dependency.join("src/lib.rs"),
            "pub fn answer() -> u8 { 42 }\n",
        )
        .unwrap();
        std::fs::write(&script_input, "alpha").unwrap();
        std::fs::create_dir_all(dir.path().join("external-watch")).unwrap();
        std::fs::write(dir.path().join("external-watch/first.txt"), "watched").unwrap();
        std::fs::write(
            project.join("src/lib.rs"),
            "include!(concat!(env!(\"OUT_DIR\"), \"/generated.rs\"));\n\
             pub fn answer() -> u8 { external_dep::answer() }\n",
        )
        .unwrap();
        std::fs::write(
            project.join("build.rs"),
            "fn main() {\n\
             println!(\"cargo:rerun-if-changed=../external-input.txt\");\n\
             println!(\"cargo:rerun-if-changed=../external-watch\");\n\
             println!(\"cargo:rerun-if-env-changed=RECEIPT_BUILD_VALUE\");\n\
             let input = std::fs::read_to_string(\"../external-input.txt\").unwrap();\n\
             let env = std::env::var(\"RECEIPT_BUILD_VALUE\").unwrap();\n\
             let output = std::path::PathBuf::from(std::env::var_os(\"OUT_DIR\").unwrap());\n\
             std::fs::write(output.join(\"generated.rs\"), format!(\"pub const GENERATED: &str = {:?};\\n\", input + &env)).unwrap();\n\
             }\n",
        )
        .unwrap();
        let fixture = Self {
            _dir: dir,
            home,
            project,
            target,
            cache,
            dependency,
            script_input,
        };
        fixture.run(
            &["cargo", "--", "check", "--offline", "--features", "old"],
            false,
            "alpha",
        );
        fixture.capture();
        fixture
    }

    fn run(&self, args: &[&str], enabled: bool, environment: &str) -> Output {
        let output = proxied_cargo(&self.home, &self.cache, &self.target)
            .current_dir(&self.project)
            .args(args)
            .env("KACHE_REAL_CARGO", env!("CARGO"))
            .env("KACHE_TARGET_LIVENESS", if enabled { "1" } else { "0" })
            .env("KACHE_AUTO_GC", "0")
            .env("RECEIPT_BUILD_VALUE", environment)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    }

    fn capture(&self) {
        // The first successful command can create Cargo's own metadata.
        for _ in 0..2 {
            self.run(&["cargo", "--", "check", "--offline"], true, "alpha");
        }
        let preview = self.clean(&["--json"], "alpha");
        assert_eq!(preview["targets"].as_array().unwrap().len(), 1, "{preview}");
        assert_eq!(
            preview["targets"][0]["plan"]["status"], "ready",
            "{preview}"
        );
        assert!(
            preview["targets"][0]["plan"]["units"].as_u64().unwrap() > 0,
            "fixture must contain proven obsolete dependency units: {preview}"
        );
    }

    fn receipt_path(&self) -> std::path::PathBuf {
        let receipts: Vec<_> = std::fs::read_dir(self.cache.join("target-liveness"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(receipts.len(), 1);
        receipts.into_iter().next().unwrap()
    }

    fn clean(&self, flags: &[&str], environment: &str) -> Value {
        let args: Vec<_> = ["clean", "--units"]
            .into_iter()
            .chain(flags.iter().copied())
            .collect();
        serde_json::from_slice(&self.run(&args, true, environment).stdout).unwrap()
    }
}

fn receipt_target_files(target: &Path) -> std::collections::BTreeMap<std::path::PathBuf, Vec<u8>> {
    let mut result = std::collections::BTreeMap::new();
    let mut pending = vec![target.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                pending.push(entry.path());
            } else {
                result.insert(entry.path(), std::fs::read(entry.path()).unwrap());
            }
        }
    }
    result
}

#[test]
fn receipt_cleanup_preserves_live_build_script_and_external_dependency_units() {
    let fixture = ReceiptAcceptance::new();
    let cleaned = fixture.clean(&["--json", "--yes"], "alpha");
    assert!(
        cleaned["targets"][0]["removed"]["units"].as_u64().unwrap() > 0,
        "{cleaned}"
    );
    let warm = fixture.run(
        &["cargo", "--", "check", "--offline", "--message-format=json"],
        true,
        "alpha",
    );
    let records: Vec<Value> = String::from_utf8(warm.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let artifacts: Vec<_> = records
        .iter()
        .filter(|record| record["reason"] == "compiler-artifact")
        .collect();
    assert!(
        artifacts
            .iter()
            .any(|record| record["target"]["name"] == "external_dep")
    );
    assert!(artifacts.iter().any(|record| {
        record["target"]["kind"]
            .as_array()
            .unwrap()
            .iter()
            .any(|kind| kind == "custom-build")
    }));
    assert!(
        artifacts.iter().all(|record| record["fresh"] == true),
        "all live units, including the build script and path dependency, must stay fresh: {records:?}"
    );
    let generated: Vec<_> = receipt_target_files(&fixture.target)
        .into_iter()
        .filter(|(path, _)| path.file_name().is_some_and(|name| name == "generated.rs"))
        .collect();
    assert!(
        !generated.is_empty(),
        "build script outputs must survive cleanup"
    );
    assert!(
        generated
            .iter()
            .all(|(_, contents)| String::from_utf8_lossy(contents).contains("alphaalpha"))
    );
}

#[test]
fn receipt_cleanup_keeps_outputs_after_external_inputs_or_environment_change() {
    let fixture = ReceiptAcceptance::new();
    let unchanged = receipt_target_files(&fixture.target);
    let environment = fixture.clean(&["--json", "--yes"], "beta");
    assert_ne!(
        environment["targets"][0]["plan"]["status"], "ready",
        "{environment}"
    );
    assert_eq!(environment["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), unchanged);

    std::fs::write(
        fixture.dependency.join("src/lib.rs"),
        "pub fn answer() -> u8 { 43 }\n",
    )
    .unwrap();
    let dependency = fixture.clean(&["--json", "--yes"], "alpha");
    assert_ne!(
        dependency["targets"][0]["plan"]["status"], "ready",
        "{dependency}"
    );
    assert_eq!(dependency["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), unchanged);
    fixture.capture();

    let before_script_change = receipt_target_files(&fixture.target);
    std::fs::write(&fixture.script_input, "bravo").unwrap();
    let script = fixture.clean(&["--json", "--yes"], "alpha");
    assert_ne!(script["targets"][0]["plan"]["status"], "ready", "{script}");
    assert_eq!(script["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), before_script_change);
}

#[test]
fn receipt_cleanup_skips_busy_profiles_and_corrupted_receipts() {
    let fixture = ReceiptAcceptance::new();
    let before = receipt_target_files(&fixture.target);
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.target.join("debug/.cargo-lock"))
        .unwrap();
    lock.lock().unwrap();
    let busy = fixture.clean(&["--json", "--yes"], "alpha");
    assert_eq!(busy["targets"][0]["plan"]["status"], "busy", "{busy}");
    let receipt: Value =
        serde_json::from_slice(&std::fs::read(fixture.receipt_path()).unwrap()).unwrap();
    assert_eq!(
        busy["targets"][0]["plan"]["command"], receipt["command"],
        "a busy plan must preserve the recorded command"
    );
    assert_eq!(busy["targets"][0]["plan"]["command"][0], "kache");
    assert_eq!(busy["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), before);
    lock.unlock().unwrap();

    let receipts: Vec<_> = std::fs::read_dir(fixture.cache.join("target-liveness"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(receipts.len(), 1);
    std::fs::write(&receipts[0], b"{broken JSON").unwrap();
    let invalid = fixture.clean(&["--json", "--yes"], "alpha");
    assert_ne!(
        invalid["targets"][0]["plan"]["status"], "ready",
        "{invalid}"
    );
    assert_eq!(invalid["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), before);
}

#[test]
fn receipt_cleanup_json_preview_and_dry_run_preserve_target_inventory() {
    let fixture = ReceiptAcceptance::new();
    let before = receipt_target_files(&fixture.target);
    for flags in [&["--json"][..], &["--json", "--yes", "--dry-run"][..]] {
        let preview = fixture.clean(flags, "alpha");
        assert_eq!(preview["preview"], true);
        assert_eq!(
            preview["targets"][0]["plan"]["status"], "ready",
            "{preview}"
        );
        assert!(
            preview["targets"][0]["plan"]["units"].as_u64().unwrap() > 0,
            "{preview}"
        );
        assert_eq!(preview["targets"][0]["removed"]["units"], 0);
        assert_eq!(receipt_target_files(&fixture.target), before);
    }
}

#[test]
fn receipt_cleanup_rejects_workspace_configuration_and_unreported_source_changes() {
    let fixture = ReceiptAcceptance::new();
    for (relative, appended) in [
        ("src/unused_module.rs", "pub fn unused() {}\n"),
        ("Cargo.toml", "\n# manifest changed\n"),
        ("Cargo.lock", "\n# lockfile changed\n"),
        (".cargo/config.toml", "[build]\njobs=1\n"),
    ] {
        let path = fixture.project.join(relative);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let original = std::fs::read(&path).unwrap_or_default();
        let mut changed = original;
        changed.extend_from_slice(appended.as_bytes());
        let before = receipt_target_files(&fixture.target);
        std::fs::write(&path, changed).unwrap();
        let result = fixture.clean(&["--json", "--yes"], "alpha");
        assert_ne!(
            result["targets"][0]["plan"]["status"], "ready",
            "{relative}: {result}"
        );
        assert_eq!(result["targets"][0]["removed"]["units"], 0);
        assert_eq!(receipt_target_files(&fixture.target), before, "{relative}");
        fixture.capture();
    }
}

#[test]
fn receipt_cleanup_rejects_new_external_build_script_inputs() {
    let fixture = ReceiptAcceptance::new();
    let before = receipt_target_files(&fixture.target);
    std::fs::write(
        fixture._dir.path().join("external-watch/new.txt"),
        "new input",
    )
    .unwrap();
    let result = fixture.clean(&["--json", "--yes"], "alpha");
    assert_ne!(result["targets"][0]["plan"]["status"], "ready", "{result}");
    assert_eq!(result["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), before);
}

#[test]
fn receipt_cleanup_keeps_outputs_when_a_live_fingerprint_changes() {
    let fixture = ReceiptAcceptance::new();
    let receipt: Value =
        serde_json::from_slice(&std::fs::read(fixture.receipt_path()).unwrap()).unwrap();
    let unit = receipt["units"]
        .as_array()
        .unwrap()
        .iter()
        .find(|unit| {
            unit["live"] == true
                && unit["fingerprint"]
                    .as_str()
                    .unwrap()
                    .contains("external_dep")
        })
        .unwrap();
    let fingerprint = Path::new(unit["fingerprint"].as_str().unwrap());
    let file = std::fs::read_dir(fingerprint)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("lib-")
                && path.extension().is_none()
        })
        .unwrap();
    std::fs::write(&file, "changed freshness evidence").unwrap();
    let before = receipt_target_files(&fixture.target);
    let result = fixture.clean(&["--json", "--yes"], "alpha");
    assert_ne!(result["targets"][0]["plan"]["status"], "ready", "{result}");
    assert_eq!(result["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), before);
}

#[test]
fn receipt_cleanup_rejects_parent_directory_paths_in_a_receipt() {
    let fixture = ReceiptAcceptance::new();
    let outside = fixture.project.join("valuable.txt");
    std::fs::write(&outside, "must survive hostile receipt paths").unwrap();
    fixture.capture();
    let receipt_path = fixture.receipt_path();
    let mut receipt: Value =
        serde_json::from_slice(&std::fs::read(&receipt_path).unwrap()).unwrap();
    let unit = receipt["units"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|unit| unit["live"] == false && unit["known"] == true)
        .unwrap();
    unit["parts"] = serde_json::json!([fixture.target.join("../valuable.txt")]);
    std::fs::write(receipt_path, serde_json::to_vec(&receipt).unwrap()).unwrap();
    let before = receipt_target_files(&fixture.target);
    let result = fixture.clean(&["--json", "--yes"], "alpha");
    assert_ne!(result["targets"][0]["plan"]["status"], "ready", "{result}");
    assert_eq!(result["targets"][0]["removed"]["units"], 0);
    assert_eq!(
        std::fs::read_to_string(outside).unwrap(),
        "must survive hostile receipt paths"
    );
    assert_eq!(receipt_target_files(&fixture.target), before);
}

#[test]
fn receipt_cleanup_rejects_a_changed_custom_cargo_executable() {
    let fixture = ReceiptAcceptance::new();
    let cargo = fixture._dir.path().join("custom-cargo");
    let real_cargo = env!("CARGO").replace('\'', "'\\''");
    let script = format!("#!/bin/sh\nexec '{real_cargo}' \"$@\"\n");
    kache_fs::testutil::write_executable(&cargo, &script);
    let run = |args: &[&str]| {
        let output = proxied_cargo(&fixture.home, &fixture.cache, &fixture.target)
            .current_dir(&fixture.project)
            .args(args)
            .env("KACHE_REAL_CARGO", &cargo)
            .env("KACHE_TARGET_LIVENESS", "1")
            .env("KACHE_AUTO_GC", "0")
            .env("RECEIPT_BUILD_VALUE", "alpha")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    for _ in 0..2 {
        run(&["cargo", "--", "check", "--offline"]);
    }
    let before = run(&["clean", "--units", "--json"]);
    let before: Value = serde_json::from_slice(&before.stdout).unwrap();
    assert_eq!(before["targets"][0]["plan"]["status"], "ready", "{before}");
    assert!(before["targets"][0]["plan"]["units"].as_u64().unwrap() > 0);
    let files = receipt_target_files(&fixture.target);
    std::fs::write(&cargo, format!("{script}# updated executable\n")).unwrap();
    let changed = run(&["clean", "--units", "--json", "--yes"]);
    let changed: Value = serde_json::from_slice(&changed.stdout).unwrap();
    assert_ne!(changed["targets"][0]["plan"]["status"], "ready");
    assert_eq!(changed["targets"][0]["plan"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), files);
}

#[test]
fn receipt_cleanup_tracks_default_build_script_inputs_outside_the_workspace() {
    let fixture = ReceiptAcceptance::new();
    let input = fixture.dependency.join("default-input.txt");
    std::fs::write(&input, "old input").unwrap();
    std::fs::write(
        fixture.dependency.join("build.rs"),
        "fn main() { let input = std::fs::read_to_string(\"default-input.txt\").unwrap(); println!(\"cargo:rustc-env=DEFAULT_INPUT={input}\"); }\n",
    ).unwrap();
    for _ in 0..2 {
        fixture.run(&["cargo", "--", "check", "--offline"], true, "alpha");
    }
    let preview = fixture.clean(&["--json"], "alpha");
    assert_eq!(
        preview["targets"][0]["plan"]["status"], "ready",
        "{preview}"
    );
    let warm = fixture.run(
        &["cargo", "--", "check", "--offline", "--message-format=json"],
        true,
        "alpha",
    );
    let artifacts: Vec<Value> = String::from_utf8(warm.stdout)
        .unwrap()
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|record| record["reason"] == "compiler-artifact")
        .collect();
    assert!(!artifacts.is_empty());
    assert!(artifacts.iter().all(|artifact| artifact["fresh"] == true));
    let files = receipt_target_files(&fixture.target);
    std::fs::write(input, "changed default input").unwrap();
    let changed = fixture.clean(&["--json", "--yes"], "alpha");
    assert_ne!(
        changed["targets"][0]["plan"]["status"], "ready",
        "{changed}"
    );
    assert_eq!(changed["targets"][0]["removed"]["units"], 0);
    assert_eq!(receipt_target_files(&fixture.target), files);
}

#[test]
fn receipt_cleanup_preserves_units_in_unobserved_profiles() {
    let fixture = ReceiptAcceptance::new();
    fixture.run(
        &[
            "cargo",
            "--",
            "check",
            "--offline",
            "--release",
            "--features",
            "old",
        ],
        false,
        "alpha",
    );
    fixture.capture();
    let before = receipt_target_files(&fixture.target.join("release"));
    assert!(!before.is_empty());
    let preview = fixture.clean(&["--json"], "alpha");
    assert!(preview["targets"][0]["plan"]["unknown"].as_u64().unwrap() > 0);
    let cleaned = fixture.clean(&["--json", "--yes"], "alpha");
    assert!(cleaned["targets"][0]["removed"]["units"].as_u64().unwrap() > 0);
    assert_eq!(
        receipt_target_files(&fixture.target.join("release")),
        before
    );
}
