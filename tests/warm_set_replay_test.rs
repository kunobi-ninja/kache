//! Process-level proof that an explicit warm step restores Cargo units before
//! their first lookup, without a daemon or a remote fallback during the build.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use serde_json::Value;
use tempfile::TempDir;

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary, stop_daemon};

const NAMESPACE: &str = "warm-replay-fixture";
const SHAPE: &str = "cargo-build-libs-v1";

struct Client {
    root: TempDir,
    config: PathBuf,
    sequence: usize,
}

impl Client {
    fn new() -> Self {
        let root = TempDir::new().unwrap();
        let config = isolated_config_path(root.path());
        Self {
            root,
            config,
            sequence: 0,
        }
    }

    fn configure(&self, remote: Option<&Path>, max_keys: u64) {
        let mut config = format!(
            "[cache]\nignore_env = true\nlocal_store = {}\nruntime_dir = {}\n\
             local_only = {}\nprefetch_enabled = false\n\
             prefetch_max_keys = {max_keys}\nprefetch_max_bytes = \"64MiB\"\n\
             prefetch_deadline_secs = 30\ns3_concurrency = 2\n",
            toml::Value::String(self.root.path().display().to_string()),
            toml::Value::String(self.root.path().display().to_string()),
            remote.is_none(),
        );
        if let Some(remote) = remote {
            config.push_str(&format!(
                "\n[cache.remote]\ntype = \"filesystem\"\npath = {}\nprefix = \"artifacts\"\n",
                toml::Value::String(remote.display().to_string()),
            ));
        }
        std::fs::write(&self.config, config).unwrap();
    }

    fn command(&self, program: impl AsRef<std::ffi::OsStr>) -> Command {
        let mut command = hermetic_command(program, self.root.path(), Some(&self.config));
        for name in [
            "KACHE_DISABLED",
            "KACHE_LOCAL_ONLY",
            "KACHE_REMOTE_READONLY",
            "KACHE_READONLY_STORE",
            "KACHE_BASE_DIR",
            "KACHE_REPOSITORY",
            "KACHE_MANIFEST_KEY",
            "KACHE_PROFILE",
            "KACHE_TARGET",
            "PROFILE",
            "RUSTC_WORKSPACE_WRAPPER",
            "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
            "GITHUB_ACTIONS",
            "GITLAB_CI",
            "CI",
            "GITHUB_SHA",
            "GITHUB_REF",
            "CI_COMMIT_SHA",
            "CI_COMMIT_REF_NAME",
            "KACHE_PARENT_COMMITS",
            "KACHE_CI_STARTED_AT_MS",
            "CARGO_BUILD_TARGET",
            "CARGO_ENCODED_RUSTFLAGS",
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_CONFIG_COUNT",
        ] {
            command.env_remove(name);
        }
        command
            .env("KACHE_LOG", "off")
            .env("KACHE_NAMESPACE", NAMESPACE)
            .env("KACHE_BUILD_SHAPE", SHAPE)
            .env("RUSTFLAGS", "")
            .env("CARGO_INCREMENTAL", "0");
        command
    }

    /// Wait on the actual command, not a guessed download completion time.
    /// File capture also prevents an accidentally started daemon holding pipes.
    fn execute(&mut self, mut command: Command, cwd: &Path) -> Output {
        let sequence = self.sequence;
        self.sequence += 1;
        let out = self.root.path().join(format!("command-{sequence}.out"));
        let err = self.root.path().join(format!("command-{sequence}.err"));
        command
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(&out).unwrap())
            .stderr(std::fs::File::create(&err).unwrap());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let status = runtime.block_on(async {
            let mut command = tokio::process::Command::from(command);
            command.kill_on_drop(true);
            let mut child = command.spawn().unwrap();
            let pid = child.id().unwrap();
            match tokio::time::timeout(Duration::from_secs(120), child.wait()).await {
                Ok(status) => status.unwrap(),
                Err(_) => {
                    #[cfg(unix)]
                    unsafe {
                        libc::kill(-(pid as i32), libc::SIGKILL);
                    }
                    #[cfg(windows)]
                    {
                        let _ = Command::new("taskkill")
                            .args(["/F", "/T", "/PID", &pid.to_string()])
                            .status();
                    }
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    panic!(
                        "fixture command timed out\nstdout: {}\nstderr: {}",
                        std::fs::read_to_string(&out).unwrap(),
                        std::fs::read_to_string(&err).unwrap()
                    );
                }
            }
        });
        Output {
            status,
            stdout: std::fs::read(out).unwrap(),
            stderr: std::fs::read(err).unwrap(),
        }
    }

    fn kache(&mut self, cwd: &Path, args: &[&str]) -> Output {
        let mut command = self.command(kache_binary());
        command.args(args);
        self.execute(command, cwd)
    }

    fn build(&mut self, project: &Path, target: &Path, host: &str) {
        let mut command = self.command("cargo");
        command
            .args(["build", "--locked", "--offline", "--target-dir"])
            .arg(target)
            .env("RUSTC_WRAPPER", kache_binary())
            .env("KACHE_BUILD_TARGET", host);
        assert_success(&self.execute(command, project));
        assert!(
            !self.root.path().join("daemon.run.lock").exists(),
            "local-only build started a daemon"
        );
    }

    fn report(&mut self, project: &Path) -> Value {
        let output = self.kache(project, &["report", "--format", "json", "--since", "1h"]);
        assert_success(&output);
        serde_json::from_slice(&output.stdout).unwrap()
    }

    fn entry_meta(&self, key: &str) -> PathBuf {
        self.root.path().join("store").join(key).join("meta.json")
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let mut command = self.command(kache_binary());
        command.args(["daemon", "stop"]);
        stop_daemon(&mut command, self.root.path());
    }
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

struct Fixture {
    project: TempDir,
    remote: TempDir,
    _producer: Client,
    consumer: Client,
    host: String,
    report_path: PathBuf,
    report: Value,
}

impl Fixture {
    fn new() -> Self {
        build_kache();
        let project = TempDir::new().unwrap();
        let remote = TempDir::new().unwrap();
        std::fs::create_dir_all(project.path().join("dep/src")).unwrap();
        std::fs::create_dir_all(project.path().join("src")).unwrap();
        std::fs::write(
            project.path().join("Cargo.toml"),
            "[package]\nname = \"warm-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\
             [workspace]\n[dependencies]\nwarm-dep = { path = \"dep\" }\n",
        )
        .unwrap();
        std::fs::write(
            project.path().join("src/lib.rs"),
            "pub fn value() -> u64 { warm_dep::value() + 1 }\n",
        )
        .unwrap();
        std::fs::write(
            project.path().join("dep/Cargo.toml"),
            "[package]\nname = \"warm-dep\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::write(
            project.path().join("dep/src/lib.rs"),
            "pub fn value() -> u64 { 41 }\n",
        )
        .unwrap();
        std::fs::write(project.path().join("Cargo.lock"),
            "version = 4\n\n[[package]]\nname = \"warm-dep\"\nversion = \"0.1.0\"\n\n\
             [[package]]\nname = \"warm-fixture\"\nversion = \"0.1.0\"\ndependencies = [\"warm-dep\"]\n").unwrap();
        let mut producer = Client::new();
        producer.configure(None, 32);
        let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
        let mut probe = producer.command(rustc);
        probe.arg("-vV");
        let version = producer.execute(probe, project.path());
        assert_success(&version);
        let text = std::str::from_utf8(&version.stdout).unwrap();
        let host = text
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .unwrap()
            .to_string();
        let producer_target = project.path().join("producer-target");
        producer.build(project.path(), &producer_target, &host);
        let events = producer.report(project.path());
        let compiled: Vec<_> = events["all_events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| {
                ["warm_fixture", "warm_dep"].contains(&event["crate_name"].as_str().unwrap_or(""))
            })
            .collect();
        assert_eq!(
            compiled.len(),
            2,
            "fixture must record both real library compiles: {events}"
        );
        assert!(
            compiled.iter().all(|event| event["compiler_runs"] == 1),
            "producer must compile cold: {events}"
        );
        producer.configure(Some(remote.path()), 32);
        assert_success(&producer.kache(project.path(), &["sync", "--push"]));
        assert_success(
            &producer.kache(project.path(), &["save-manifest", "--namespace", NAMESPACE]),
        );
        let reports = remote
            .path()
            .join("artifacts/_manifests/build-reports/v1")
            .join(NAMESPACE);
        let report_paths: Vec<_> = std::fs::read_dir(&reports)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "json")
            })
            .collect();
        assert_eq!(
            report_paths.len(),
            1,
            "expected one immutable session report: {report_paths:?}"
        );
        let report_path = report_paths[0].clone();
        let report: Value = serde_json::from_slice(&std::fs::read(&report_path).unwrap()).unwrap();
        assert_eq!(
            report["identity"]["build_shape"], SHAPE,
            "producer facts missing: {report}"
        );
        assert_eq!(report["identity"]["target"], host);
        assert_eq!(report["identity"]["profile"], "debug");
        assert_eq!(
            report["entries"].as_array().unwrap().len(),
            2,
            "both units must be replayable: {report}"
        );
        for entry in report["entries"].as_array().unwrap() {
            assert!(
                pack_path(remote.path(), entry).is_file(),
                "synchronous push must publish every reported artifact: {entry}"
            );
        }
        let consumer = Client::new();
        consumer.configure(Some(remote.path()), 32);
        assert!(
            !consumer.root.path().join("store").exists(),
            "consumer must start empty"
        );
        // Cargo freshness cannot supply the consumer's hit; only the remote
        // pack holds the producer output after this point.
        std::fs::remove_dir_all(producer_target).unwrap();
        Self {
            project,
            remote,
            _producer: producer,
            consumer,
            host,
            report_path,
            report,
        }
    }

    fn prefetch(&mut self, extra: &[&str]) -> Output {
        let mut args = vec![
            "prefetch",
            "--namespace",
            NAMESPACE,
            "--build-shape",
            SHAPE,
            "--target",
            &self.host,
            "--profile",
            "debug",
        ];
        args.extend_from_slice(extra);
        self.consumer.kache(self.project.path(), &args)
    }

    fn assert_empty(&self) {
        for entry in self.report["entries"].as_array().unwrap() {
            assert!(
                !self
                    .consumer
                    .entry_meta(entry["cache_key"].as_str().unwrap())
                    .exists(),
                "incompatible replay imported {entry}"
            );
        }
    }
}

fn pack_path(remote: &Path, entry: &Value) -> PathBuf {
    remote
        .join("artifacts/v3/packs")
        .join(entry["crate_name"].as_str().unwrap())
        .join(format!("{}.tar.zst", entry["cache_key"].as_str().unwrap()))
}

#[test]
fn explicit_replay_finishes_before_cargo_and_repeated_replay_skips_local_entries() {
    let mut fixture = Fixture::new();
    let dry_run = fixture.prefetch(&["--dry-run"]);
    assert_success(&dry_run);
    assert_eq!(
        String::from_utf8(dry_run.stdout).unwrap().lines().count(),
        2
    );
    fixture.assert_empty();
    assert_success(&fixture.prefetch(&[]));
    for entry in fixture.report["entries"].as_array().unwrap() {
        assert!(
            fixture
                .consumer
                .entry_meta(entry["cache_key"].as_str().unwrap())
                .is_file(),
            "prefetch returned before local import: {entry}"
        );
        // A second GET would now fail; successful replay proves local skips.
        std::fs::remove_file(pack_path(fixture.remote.path(), entry)).unwrap();
    }
    let repeated = fixture.prefetch(&[]);
    assert_success(&repeated);
    let text = String::from_utf8(repeated.stdout).unwrap();
    assert!(
        text.contains("0 entries; 2 local") && text.contains("0 compressed bytes"),
        "{text}"
    );
    fixture.consumer.configure(None, 32);
    let target = fixture.project.path().join("consumer-target");
    assert!(!target.exists());
    fixture
        .consumer
        .build(fixture.project.path(), &target, &fixture.host);
    let report = fixture.consumer.report(fixture.project.path());
    let events: Vec<_> = report["all_events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| {
            ["warm_fixture", "warm_dep"].contains(&event["crate_name"].as_str().unwrap_or(""))
        })
        .collect();
    assert_eq!(
        events.len(),
        2,
        "both restored units must be observed: {report}"
    );
    assert!(
        events
            .iter()
            .all(|event| event["result"] == "local_hit" && event["compiler_runs"] == 0),
        "explicit replay must avoid both real compiles: {report}"
    );
    assert!(target.join("debug/libwarm_fixture.rlib").is_file());
}

#[test]
fn incompatible_build_facts_do_not_restore_any_entry() {
    let mut fixture = Fixture::new();
    let host = fixture.host.clone();
    for (shape, target, profile) in [
        ("different-shape", host.as_str(), "debug"),
        (SHAPE, "different-target", "debug"),
        (SHAPE, host.as_str(), "release"),
    ] {
        let output = fixture.consumer.kache(
            fixture.project.path(),
            &[
                "prefetch",
                "--namespace",
                NAMESPACE,
                "--build-shape",
                shape,
                "--target",
                target,
                "--profile",
                profile,
            ],
        );
        assert_success(&output);
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("no report has matching"),
            "{output:?}"
        );
        fixture.assert_empty();
    }
    for (field, value) in [
        ("repository", Value::from("other-repository")),
        ("target", Value::from("other-target")),
        ("profile", Value::from("release")),
        ("build_shape", Value::from("cargo-test-libs-v1")),
        ("toolchain_hash", Value::from("0".repeat(64))),
        ("lock_digest", Value::from("0".repeat(64))),
        ("toolchain_hash", Value::Null),
    ] {
        let mut report = fixture.report.clone();
        report["identity"][field] = value;
        std::fs::write(&fixture.report_path, serde_json::to_vec(&report).unwrap()).unwrap();
        let output = fixture.prefetch(&[]);
        assert_success(&output);
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("no report has matching"),
            "{field}: {output:?}"
        );
        fixture.assert_empty();
    }
}

#[test]
fn a_newer_incompatible_report_does_not_hide_the_compatible_session() {
    let mut fixture = Fixture::new();
    let mut incompatible = fixture.report.clone();
    incompatible["session_id"] = Value::from("newer-incompatible-session");
    incompatible["finished_at_ms"] =
        Value::from(fixture.report["finished_at_ms"].as_u64().unwrap() + 1);
    incompatible["identity"]["build_shape"] = Value::from("different-shape");
    let key = fixture.report_path.parent().unwrap().join(format!(
        "{}-{}.json",
        incompatible["session_id"].as_str().unwrap(),
        incompatible["root_hash"].as_str().unwrap()
    ));
    std::fs::write(key, serde_json::to_vec(&incompatible).unwrap()).unwrap();
    assert_success(&fixture.prefetch(&[]));
    for entry in fixture.report["entries"].as_array().unwrap() {
        assert!(
            fixture
                .consumer
                .entry_meta(entry["cache_key"].as_str().unwrap())
                .is_file()
        );
    }
}

#[test]
fn a_missing_pack_reports_partial_failure_and_preserves_the_local_entry() {
    let mut fixture = Fixture::new();
    fixture.consumer.configure(Some(fixture.remote.path()), 1);
    assert_success(&fixture.prefetch(&[]));
    let entries = fixture.report["entries"].as_array().unwrap().clone();
    let first = &entries[0];
    let second = &entries[1];
    let first_meta = fixture
        .consumer
        .entry_meta(first["cache_key"].as_str().unwrap());
    let before = std::fs::read(&first_meta).unwrap();
    assert!(
        !fixture
            .consumer
            .entry_meta(second["cache_key"].as_str().unwrap())
            .exists(),
        "key budget must preserve report order"
    );
    std::fs::remove_file(pack_path(fixture.remote.path(), second)).unwrap();
    fixture.consumer.configure(Some(fixture.remote.path()), 32);
    let output = fixture.prefetch(&[]);
    assert!(
        !output.status.success(),
        "missing pack must report partial failure: {output:?}"
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("1 local") && text.contains("1 failed"),
        "{text}"
    );
    assert_eq!(
        std::fs::read(first_meta).unwrap(),
        before,
        "failed candidate must not replace the existing entry"
    );
    assert!(
        !fixture
            .consumer
            .entry_meta(second["cache_key"].as_str().unwrap())
            .exists()
    );
}

#[test]
fn explicitly_installed_hooks_respect_git_hooks_path_and_retain_foreign_or_edited_hooks() {
    let mut fixture = Fixture::new();
    let mut git = fixture.consumer.command("git");
    git.args(["init", "--quiet"]);
    assert_success(&fixture.consumer.execute(git, fixture.project.path()));
    let hooks = fixture.project.path().join("custom hooks");
    std::fs::create_dir(&hooks).unwrap();
    let mut git = fixture.consumer.command("git");
    git.args(["config", "--local", "core.hooksPath"])
        .arg(&hooks);
    assert_success(&fixture.consumer.execute(git, fixture.project.path()));
    let foreign = b"#!/bin/sh\n# repository hook\nexit 0\n";
    std::fs::write(hooks.join("pre-commit"), foreign).unwrap();
    std::fs::write(hooks.join("post-checkout"), foreign).unwrap();
    let rejected = fixture.prefetch(&["--install-hooks"]);
    assert!(
        !rejected.status.success(),
        "existing hook must prevent installation"
    );
    assert_eq!(std::fs::read(hooks.join("post-checkout")).unwrap(), foreign);
    assert!(!hooks.join("post-merge").exists());
    std::fs::remove_file(hooks.join("post-checkout")).unwrap();
    assert_success(&fixture.prefetch(&["--install-hooks"]));
    let original = std::fs::read(hooks.join("post-merge")).unwrap();
    assert_success(&fixture.prefetch(&["--install-hooks"]));
    assert_eq!(std::fs::read(hooks.join("post-merge")).unwrap(), original);
    assert!(hooks.join("post-checkout").is_file());
    assert_eq!(std::fs::read(hooks.join("pre-commit")).unwrap(), foreign);

    let mut git = fixture.consumer.command("git");
    git.args([
        "-c",
        "user.name=Warm fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--quiet",
        "--allow-empty",
        "-m",
        "fixture owner",
    ]);
    assert_success(&fixture.consumer.execute(git, fixture.project.path()));
    let worktree_root = TempDir::new().unwrap();
    let worktree = worktree_root.path().join("linked worktree");
    let mut git = fixture.consumer.command("git");
    git.args(["worktree", "add", "--quiet", "--detach"])
        .arg(&worktree);
    assert_success(&fixture.consumer.execute(git, fixture.project.path()));
    let host = fixture.host.clone();
    let output = fixture.consumer.kache(
        &worktree,
        &[
            "prefetch",
            "--namespace",
            NAMESPACE,
            "--build-shape",
            SHAPE,
            "--target",
            &host,
            "--profile",
            "debug",
            "--install-hooks",
        ],
    );
    assert_success(&output);
    assert_eq!(
        std::fs::read(hooks.join("post-merge")).unwrap(),
        original,
        "linked worktrees must retain the same repository owner"
    );

    let other_repository = TempDir::new().unwrap();
    // The same lockfile makes the foreign hook able to find the report if the
    // repository guard is missing; an empty store alone would be a weak oracle.
    std::fs::copy(
        fixture.project.path().join("Cargo.lock"),
        other_repository.path().join("Cargo.lock"),
    )
    .unwrap();
    let mut git = fixture.consumer.command("git");
    git.args(["init", "--quiet"]);
    assert_success(&fixture.consumer.execute(git, other_repository.path()));
    let mut git = fixture.consumer.command("git");
    git.args(["config", "--local", "core.hooksPath"])
        .arg(&hooks);
    assert_success(&fixture.consumer.execute(git, other_repository.path()));
    let host = fixture.host.clone();
    for operation in ["--install-hooks", "--uninstall-hooks"] {
        let output = fixture.consumer.kache(
            other_repository.path(),
            &[
                "prefetch",
                "--namespace",
                NAMESPACE,
                "--build-shape",
                SHAPE,
                "--target",
                &host,
                "--profile",
                "debug",
                operation,
            ],
        );
        assert!(
            !output.status.success(),
            "another repository must not manage the owner's hooks: {output:?}"
        );
        assert!(String::from_utf8_lossy(&output.stderr).contains("another repository"));
        assert_eq!(std::fs::read(hooks.join("post-merge")).unwrap(), original);
    }
    let mut git = fixture.consumer.command("git");
    git.args(["hook", "run", "post-checkout"]);
    assert_success(&fixture.consumer.execute(git, other_repository.path()));
    fixture.assert_empty();
    let mut git = fixture.consumer.command("git");
    git.args(["hook", "run", "post-checkout"]);
    assert_success(&fixture.consumer.execute(git, fixture.project.path()));
    for entry in fixture.report["entries"].as_array().unwrap() {
        assert!(
            fixture
                .consumer
                .entry_meta(entry["cache_key"].as_str().unwrap())
                .is_file(),
            "the owner's hook must actually execute replay: {entry}"
        );
    }

    std::fs::write(hooks.join("post-merge"), b"user edited this hook\n").unwrap();
    let rejected = fixture.prefetch(&["--uninstall-hooks"]);
    assert!(
        !rejected.status.success(),
        "edited hook must prevent removal"
    );
    assert!(hooks.join("post-checkout").is_file());
    assert_eq!(
        std::fs::read(hooks.join("post-merge")).unwrap(),
        b"user edited this hook\n"
    );
    std::fs::write(hooks.join("post-merge"), original).unwrap();
    assert_success(&fixture.prefetch(&["--uninstall-hooks"]));
    assert_success(&fixture.prefetch(&["--uninstall-hooks"]));
    assert!(!hooks.join("post-checkout").exists());
    assert!(!hooks.join("post-merge").exists());
    assert_eq!(std::fs::read(hooks.join("pre-commit")).unwrap(), foreign);
}
