//! Cargo routing and compiler ownership use isolated stand-in executables.
#![cfg(any(target_os = "linux", all(target_os = "macos", target_arch = "aarch64")))]

#[allow(dead_code)]
mod common;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const KACHE: &str = env!("CARGO_BIN_EXE_kache");

fn script(path: &Path, text: &str) {
    fs::write(path, text).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

struct Fixture {
    root: tempfile::TempDir,
    cache: PathBuf,
    config: PathBuf,
    shims: PathBuf,
    tools: PathBuf,
    real: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let config = root.path().join(".kache.toml");
        let shims = root.path().join("shims");
        let tools = root.path().join("tools");
        let real = root.path().join("real");
        for dir in [&shims, &tools, &real] {
            fs::create_dir(dir).unwrap();
        }
        std::os::unix::fs::symlink(KACHE, shims.join("cargo")).unwrap();
        script(
            &real.join("cargo"),
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$REAL_LOG\"\nexit 0\n",
        );
        let f = Self {
            root,
            cache,
            config,
            shims,
            tools,
            real,
        };
        f.configure("blaze=true");
        f
    }

    fn configure(&self, setting: &str) {
        fs::write(&self.config, format!("[cache]\n{setting}\nlocal_only=true\nseed_new_targets=false\ntarget_liveness=false\nauto_gc=false\n")).unwrap();
    }

    fn command(&self) -> Command {
        let mut cmd =
            common::hermetic_command(self.shims.join("cargo"), &self.cache, Some(&self.config));
        let path = std::env::join_paths([
            self.shims.clone(),
            self.tools.clone(),
            self.real.clone(),
            PathBuf::from("/usr/bin"),
            PathBuf::from("/bin"),
        ])
        .unwrap();
        cmd.current_dir(self.root.path())
            .env("PATH", path)
            .env("REAL_LOG", self.root.path().join("cargo.log"))
            .env("ENGINE_LOG", self.root.path().join("blaze.log"))
            .env("KACHE_LOG", "off")
            .env_remove("CI")
            .env_remove("GITHUB_ACTIONS")
            .env_remove("GITLAB_CI")
            .env_remove("KACHE_BLAZE")
            .env_remove("KACHE_DISABLED")
            .env_remove("KACHE_REAL_CARGO")
            .env_remove("KACHE_BLAZE_ROUTED")
            .env_remove("KACHE_BLAZE_REAL_CARGO")
            .env_remove("KACHE_BLAZE_CARGO_SHIM");
        cmd
    }

    fn engine(&self) {
        script(
            &self.tools.join("ra-jit"),
            "#!/bin/sh\n[ \"$1\" = cargo ] || exit 83\nshift\nprintf 'engine\\n' >> \"$ENGINE_LOG\"\n[ \"$(wc -l < \"$ENGINE_LOG\")\" -le 1 ] || exit 84\n[ \"$KACHE_BLAZE_ROUTED\" = 1 ] || exit 85\nif [ -n \"$ALTERNATE_PATH\" ]; then PATH=\"$ALTERNATE_PATH\"; export PATH; fi\nexec \"$CARGO\" \"$@\"\n",
        );
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command().args(args).output().unwrap()
    }
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn installing_blaze_after_setup_is_detected_and_returning_cargo_never_loops() {
    let f = Fixture::new();
    let first = f.run(&["build"]);
    success(&first);
    assert!(String::from_utf8_lossy(&first.stderr).contains("using Cargo"));
    f.engine();
    let other = f.root.path().join("other");
    fs::create_dir(&other).unwrap();
    script(&other.join("cargo"), "#!/bin/sh\nexit 91\n");
    let alternate =
        std::env::join_paths([other, PathBuf::from("/usr/bin"), PathBuf::from("/bin")]).unwrap();
    let routed = f
        .command()
        .args(["build", "--bin", "example"])
        .env("ALTERNATE_PATH", alternate)
        .output()
        .unwrap();
    success(&routed);
    assert_eq!(
        fs::read_to_string(f.root.path().join("blaze.log")).unwrap(),
        "engine\n"
    );
    assert_eq!(
        fs::read_to_string(f.root.path().join("cargo.log")).unwrap(),
        "build\nbuild --bin example\n"
    );
    let log = fs::read_to_string(f.cache.join("events.jsonl")).unwrap();
    let events = log
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .collect::<Vec<_>>();
    let event = events.iter().find(|e| e["crate_name"] == "blaze").unwrap();
    assert_eq!(event["result"], "passthrough");
    assert!(
        event["passthrough_reason"]
            .as_str()
            .unwrap()
            .starts_with("blaze-command|")
    );
}

#[test]
fn ordinary_cargo_keeps_explicit_profiles_ci_and_disabled_projects() {
    let f = Fixture::new();
    f.engine();
    for args in [
        vec!["build", "--release"],
        vec!["build", "-r"],
        vec!["build", "-rq"],
        vec!["test", "--profile=dev"],
        vec!["+nightly", "build"],
        vec!["--locked", "build"],
        vec!["clippy"],
    ] {
        success(&f.run(&args));
    }
    success(&f.command().arg("build").env("CI", "true").output().unwrap());
    f.configure("blaze=false");
    success(&f.run(&["build"]));
    f.configure("blaze=true");
    success(
        &f.command()
            .arg("build")
            .env("KACHE_DISABLED", "1")
            .output()
            .unwrap(),
    );
    assert!(!f.root.path().join("blaze.log").exists());
}

#[test]
fn failed_start_falls_back_but_a_started_build_error_does_not() {
    let f = Fixture::new();
    script(
        &f.tools.join("ra-jit"),
        "#!/nonexistent/kache-blaze-interpreter\n",
    );
    let output = f.run(&["build"]);
    success(&output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("could not start Blaze"));
    fs::remove_file(f.root.path().join("cargo.log")).unwrap();
    script(&f.tools.join("ra-jit"), "#!/bin/sh\nexit 42\n");
    let output = f.run(&["build"]);
    assert_eq!(output.status.code(), Some(42));
    assert!(!f.root.path().join("cargo.log").exists());
    // Reporting must not replace Blaze's status when Cargo's manifest is broken.
    fs::write(f.root.path().join("Cargo.toml"), "[invalid").unwrap();
    assert_eq!(f.run(&["build"]).status.code(), Some(42));
    script(&f.tools.join("ra-jit"), "#!/bin/sh\nkill -TERM $$\n");
    let signaled = f.run(&["build"]);
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(signaled.status.signal(), Some(libc::SIGTERM));
    assert!(!f.root.path().join("cargo.log").exists());
}

#[test]
fn blaze_keeps_the_target_cleanup_lease_for_its_whole_command() {
    let f = Fixture::new();
    script(
        &f.tools.join("ra-jit"),
        "#!/bin/sh\n: > \"$READY\"\nattempt=0\nwhile [ ! -e \"$GO\" ]; do attempt=$((attempt + 1)); [ \"$attempt\" -lt 1000 ] || exit 93; sleep 0.01; done\n",
    );
    let ready = f.root.path().join("ready");
    let go = f.root.path().join("go");
    let mut child = f
        .command()
        .arg("build")
        .env("READY", &ready)
        .env("GO", &go)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !ready.exists() {
        assert!(child.try_wait().unwrap().is_none());
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(f.cache.join("target-use.lock"))
        .unwrap();
    let locked = matches!(lock.try_lock(), Err(std::fs::TryLockError::WouldBlock));
    fs::write(go, "").unwrap();
    assert!(child.wait().unwrap().success());
    assert!(
        locked,
        "cleanup could remove a target while Blaze was using it"
    );
    lock.try_lock().unwrap();
    lock.unlock().unwrap();
}

#[test]
fn a_workspace_wrapper_can_report_success_without_rewriting_existing_outputs() {
    let f = Fixture::new();
    let source = f.root.path().join("lib.rs");
    fs::write(&source, "pub fn value() {}\n").unwrap();
    let out = f.root.path().join("outputs");
    fs::create_dir(&out).unwrap();
    let rlib = out.join("libexample.rlib");
    fs::write(&rlib, "existing artifact").unwrap();
    fs::set_permissions(&rlib, fs::Permissions::from_mode(0o444)).unwrap();
    for name in ["ra-jit-rustc", "ra-jit-rustc-first"] {
        let wrapper = f.tools.join(name);
        script(
            &wrapper,
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$ARGV_LOG\"\nexit 0\n",
        );
        let mut cmd = common::hermetic_command(KACHE, &f.cache, Some(&f.config));
        let output = cmd
            .current_dir(f.root.path())
            .arg(&wrapper)
            .args([
                "rustc",
                "--crate-name",
                "example",
                "--crate-type",
                "lib",
                "--emit=link",
            ])
            .arg(&source)
            .arg("--out-dir")
            .arg(&out)
            .args(["-C", "incremental=keep-me"])
            .env("RUSTC_WORKSPACE_WRAPPER", &wrapper)
            .env("ARGV_LOG", f.root.path().join("argv"))
            .env("KACHE_TRUST_DAEMON_EXECUTION", "0")
            .output()
            .unwrap();
        success(&output);
        assert_eq!(fs::read_to_string(&rlib).unwrap(), "existing artifact");
        assert!(
            fs::read_to_string(f.root.path().join("argv"))
                .unwrap()
                .contains("incremental=keep-me")
        );
        let output = common::hermetic_command(KACHE, &f.cache, Some(&f.config))
            .current_dir(f.root.path())
            .arg(&wrapper)
            .args(["rustc", "--crate-name", "___", "--print=file-names", "-"])
            .env("RUSTC_WORKSPACE_WRAPPER", &wrapper)
            .env("ARGV_LOG", f.root.path().join("argv"))
            .env("KACHE_TRUST_DAEMON_EXECUTION", "0")
            .output()
            .unwrap();
        success(&output);
    }
    let log = fs::read_to_string(f.cache.join("events.jsonl")).unwrap();
    assert!(log.contains("workspace compilation handled by Blaze"));
    assert!(!log.contains("\"result\":\"miss\""));
    assert!(
        !log.lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .any(|event| event["crate_name"] == "___"
                && event["passthrough_reason"]
                    .as_str()
                    .is_some_and(|reason| reason.starts_with("blaze|")))
    );
}

#[test]
fn blaze_events_follow_the_manifest_and_workspace_instead_of_the_shell_directory() {
    let f = Fixture::new();
    script(&f.tools.join("ra-jit"), "#!/bin/sh\nexit 0\n");
    let workspace = f.root.path().join("workspace");
    let child = workspace.join("nested");
    fs::create_dir_all(&child).unwrap();
    fs::write(workspace.join("Cargo.toml"), "[workspace]\n").unwrap();
    for args in [
        vec!["build", "--manifest-path", "workspace/Cargo.toml"],
        vec!["build", "--manifest-path=workspace/Cargo.toml"],
    ] {
        success(&f.run(&args));
    }
    success(
        &f.command()
            .args(["run", "--", "--manifest-path=elsewhere"])
            .current_dir(&child)
            .output()
            .unwrap(),
    );
    let events = fs::read_to_string(f.cache.join("events.jsonl")).unwrap();
    let roots = events
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .filter(|e| e["crate_name"] == "blaze")
        .map(|e| e["root"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(roots.len(), 3);
    let workspace = workspace.canonicalize().unwrap();
    assert!(
        roots
            .iter()
            .all(|root| Path::new(root).canonicalize().unwrap() == workspace),
        "{roots:?}, expected {workspace:?}"
    );
}
