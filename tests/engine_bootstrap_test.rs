//! Frontend identity and direct engine daemon bootstrap.

#![cfg(unix)]

use std::path::PathBuf;
use std::process::{Command, ExitStatus};
use std::time::{Duration, Instant};

#[allow(dead_code)]
mod common;

const FRONTEND: &str = env!("CARGO_BIN_EXE_kache");
const ENGINE: &str = env!("CARGO_BIN_EXE_kache-engine");

struct Fixture {
    root: tempfile::TempDir,
    runtime: PathBuf,
    config: PathBuf,
    compiler: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let runtime = root.path().join("runtime");
        let config = root.path().join("config.toml");
        let compiler = root.path().join("rustc");
        std::fs::create_dir(root.path().join("home")).unwrap();
        std::fs::write(
            &config,
            "[cache]\nlocal_only=true\nauto_gc=false\nscheduler=false\nprefetch_enabled=false\n",
        )
        .unwrap();
        kache_fs::testutil::write_executable(
            &compiler,
            "#!/bin/sh\nprintf '%s\\n' \"$PPID\" >> \"$PARENT_RECORD\"\nprintf 'compiler-output\\n'\nprintf 'compiler-error\\n' >&2\nexit 17\n",
        );
        Self {
            root,
            runtime,
            config,
            compiler,
        }
    }

    fn command(&self, program: &str) -> Command {
        let mut command = Command::new(program);
        command
            .env_clear()
            .env("PATH", env!("PATH"))
            .env("HOME", self.root.path().join("home"))
            .env("KACHE_CONFIG", &self.config)
            .env("KACHE_HOST_CONFIG", "")
            .env("KACHE_CACHE_DIR", self.root.path().join("cache"))
            .env("KACHE_RUNTIME_DIR", &self.runtime)
            .env("KACHE_LOG", "off")
            .env("KACHE_DAEMON_IDLE_TIMEOUT", "30")
            .env("PARENT_RECORD", self.root.path().join("parents"))
            .current_dir(self.root.path());
        command
    }

    fn renamed_frontend(&self) -> PathBuf {
        let frontend = self.root.path().join("custom-kache");
        std::fs::copy(FRONTEND, &frontend).unwrap();
        std::fs::copy(ENGINE, self.root.path().join("kache-engine")).unwrap();
        assert!(!self.root.path().join("kache").exists());
        frontend
    }

    fn run_compiler(&self, disabled: bool) -> (u32, ExitStatus) {
        self.run(
            self.command(ENGINE)
                .arg(&self.compiler)
                .arg("--version")
                .env("KACHE_DISABLED", if disabled { "1" } else { "0" }),
        )
    }

    fn run(&self, command: &mut Command) -> (u32, ExitStatus) {
        let mut child = command
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(self.root.path().join("stdout")).unwrap())
            .stderr(std::fs::File::create(self.root.path().join("stderr")).unwrap())
            .spawn()
            .unwrap();
        let pid = child.id();
        let started = Instant::now();
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                return (pid, status);
            }
            if started.elapsed() > Duration::from_secs(15) {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("Kache command timed out");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn assert_compiler_output(&self, status: ExitStatus) -> Vec<u32> {
        let stdout = std::fs::read(self.root.path().join("stdout")).unwrap();
        let stderr = std::fs::read(self.root.path().join("stderr")).unwrap();
        assert_eq!(status.code(), Some(17), "{stdout:?} {stderr:?}");
        assert_eq!(stdout, b"compiler-output\n");
        assert_eq!(stderr, b"compiler-error\n");
        let parents = std::fs::read_to_string(self.root.path().join("parents"))
            .unwrap()
            .lines()
            .map(|value| value.parse().unwrap())
            .collect::<Vec<_>>();
        assert!(!parents.is_empty());
        parents
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        common::stop_daemon(
            self.command(FRONTEND).args(["daemon", "stop"]),
            &self.runtime,
        );
    }
}

#[test]
fn a_renamed_frontend_installs_shims_to_its_original_image() {
    let fixture = Fixture::new();
    let frontend = fixture.renamed_frontend();
    let shims = fixture.root.path().join("shims");
    let (_, status) = fixture.run(
        fixture
            .command(frontend.to_str().unwrap())
            .args(["init", "--shims"])
            .arg(&shims)
            .arg("--canonical-only"),
    );
    let stdout = std::fs::read(fixture.root.path().join("stdout")).unwrap();
    let stderr = std::fs::read(fixture.root.path().join("stderr")).unwrap();
    assert!(status.success(), "{stdout:?} {stderr:?}");
    let original = frontend.canonicalize().unwrap();
    for name in kache_shims::farm::SHIM_NAMES {
        let shim = shims.join(name);
        assert!(std::fs::symlink_metadata(&shim).unwrap().is_symlink());
        assert_eq!(shim.canonicalize().unwrap(), original, "{name}");
    }
    common::wait_for_daemon_exit(&fixture.runtime, Duration::ZERO).unwrap();
}

#[test]
fn cargo_refuses_the_original_frontend_as_its_real_cargo_program() {
    let fixture = Fixture::new();
    let frontend = fixture.renamed_frontend();
    let (_, status) = fixture.run(
        fixture
            .command(frontend.to_str().unwrap())
            .args(["cargo", "--version"])
            .env("KACHE_REAL_CARGO", &frontend),
    );
    let stderr = std::fs::read(fixture.root.path().join("stderr")).unwrap();
    assert_eq!(status.code(), Some(1), "{stderr:?}");
    assert!(
        String::from_utf8_lossy(&stderr).contains("points back to kache"),
        "{stderr:?}"
    );
    common::wait_for_daemon_exit(&fixture.runtime, Duration::ZERO).unwrap();
}

#[test]
fn direct_engine_bootstraps_a_daemon_and_delegates_the_compiler() {
    let fixture = Fixture::new();
    let (engine_pid, status) = fixture.run_compiler(false);
    let parents = fixture.assert_compiler_output(status);
    #[cfg(target_os = "linux")]
    let filtered = std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once(':'))
        .any(|(name, value)| matches!(name, "Seccomp" | "NoNewPrivs") && value.trim() != "0");
    #[cfg(not(target_os = "linux"))]
    let filtered = false;
    if filtered {
        // Linux filters cannot be compared across processes; execution stays local.
        assert_eq!(parents, vec![engine_pid]);
    } else {
        assert!(
            parents.iter().all(|parent| *parent != engine_pid),
            "{parents:?}"
        );
    }
    assert!(
        parents.iter().all(|parent| *parent == parents[0]),
        "{parents:?}"
    );
    assert!(common::wait_for_daemon_exit(&fixture.runtime, Duration::ZERO).is_err());
}

#[test]
fn disabled_direct_engine_executes_locally_without_starting_a_daemon() {
    let fixture = Fixture::new();
    let (engine_pid, status) = fixture.run_compiler(true);
    assert_eq!(fixture.assert_compiler_output(status), vec![engine_pid]);
    common::wait_for_daemon_exit(&fixture.runtime, Duration::ZERO).unwrap();
}
