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
fn direct_engine_bootstraps_a_daemon_and_keeps_the_platform_process_owner() {
    let fixture = Fixture::new();
    let (engine_pid, status) = fixture.run_compiler(false);
    let parents = fixture.assert_compiler_output(status);
    // Every Unix client retains native process creation after handoff.
    assert_eq!(parents, vec![engine_pid]);
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

#[cfg(target_os = "linux")]
#[test]
fn trusted_linux_frontend_keeps_native_children_and_reports_execution_separately() {
    let fixture = Fixture::new();
    let mut config = std::fs::read_to_string(&fixture.config).unwrap();
    config.push_str("trust_daemon_execution=true\n");
    std::fs::write(&fixture.config, config).unwrap();
    let (_, started) = fixture.run(fixture.command(FRONTEND).args(["daemon", "start"]));
    assert!(started.success());
    let image = fixture.root.path().join("parent-image");
    kache_fs::testutil::write_executable(
        &fixture.compiler,
        "#!/bin/sh\n/usr/bin/readlink \"/proc/$PPID/exe\" > \"$PARENT_IMAGE\"\nprintf '%s\\n' \"$PPID\" >> \"$PARENT_RECORD\"\nprintf 'compiler-output\\n'\nprintf 'compiler-error\\n' >&2\nexit 17\n",
    );
    let (frontend_pid, status) = fixture.run(
        fixture
            .command(FRONTEND)
            .arg(&fixture.compiler)
            .arg("--version")
            .env("PARENT_IMAGE", &image),
    );
    assert_eq!(fixture.assert_compiler_output(status), vec![frontend_pid]);
    assert_eq!(
        std::fs::read_to_string(image).unwrap().trim(),
        std::fs::canonicalize(FRONTEND).unwrap().to_str().unwrap()
    );
    let (_, status) = fixture.run(fixture.command(FRONTEND).args(["doctor", "--json"]));
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.root.path().join("stdout")).unwrap())
            .unwrap();
    assert!(status.success(), "{report}");
    let check = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["label"] == "Compiler execution")
        .unwrap();
    assert_eq!(check["pass"], true);
    assert!(
        check["detail"]
            .as_str()
            .unwrap()
            .contains("trusted daemon pipeline")
    );
    assert_eq!(check["optional"], true);
    let (_, status) = fixture.run(fixture.command(FRONTEND).args([
        "init",
        "--yes",
        "--no-shell",
        "--no-service",
        "--json",
    ]));
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.root.path().join("stdout")).unwrap())
            .unwrap();
    assert!(status.success(), "{report}");
    assert_eq!(report["compiler_execution"]["eligible"], true);
    assert_eq!(report["compiler_execution"]["client_trust_enabled"], true);
    assert_eq!(report["compiler_execution"]["daemon_trust_enabled"], true);
}

#[test]
fn trusted_unix_pipeline_restores_real_rust_outputs_without_recompiling() {
    let fixture = Fixture::new();
    let mut config = std::fs::read_to_string(&fixture.config).unwrap();
    config.push_str("trust_daemon_execution=true\n");
    std::fs::write(&fixture.config, config).unwrap();
    let sysroot = Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .unwrap();
    assert!(sysroot.status.success());
    let rustc = PathBuf::from(String::from_utf8(sysroot.stdout).unwrap().trim()).join("bin/rustc");
    let source = fixture.root.path().join("lib.rs");
    std::fs::write(&source, "pub fn answer() -> u64 { 42 }\n").unwrap();
    let output = fixture.root.path().join("out");
    let (_, started) = fixture.run(fixture.command(FRONTEND).args(["daemon", "start"]));
    assert!(started.success());
    let compile = || {
        let (_, status) = fixture.run(
            fixture
                .command(FRONTEND)
                .arg(&rustc)
                .args([
                    "--crate-name",
                    "broker_smoke",
                    "--crate-type=rlib",
                    "--emit=link,dep-info",
                    "--edition=2024",
                ])
                .arg(&source)
                .arg("--out-dir")
                .arg(&output),
        );
        let diagnostics = std::fs::read_to_string(fixture.root.path().join("stderr")).unwrap();
        assert!(status.success(), "{diagnostics}");
    };
    let stats = || {
        let (_, status) = fixture.run(fixture.command(FRONTEND).args(["stats", "--json"]));
        assert!(status.success());
        serde_json::from_slice::<serde_json::Value>(
            &std::fs::read(fixture.root.path().join("stdout")).unwrap(),
        )
        .unwrap()
    };
    std::fs::create_dir(&output).unwrap();
    compile();
    let original = std::fs::read(output.join("libbroker_smoke.rlib")).unwrap();
    let first = stats();
    assert_eq!(first["misses"], 1, "{first}");
    assert_eq!(first["local_hits"], 0, "{first}");
    std::fs::remove_dir_all(&output).unwrap();
    std::fs::create_dir(&output).unwrap();
    compile();
    assert_eq!(
        std::fs::read(output.join("libbroker_smoke.rlib")).unwrap(),
        original
    );
    assert!(output.join("broker_smoke.d").exists());
    let second = stats();
    assert_eq!(second["misses"], 1, "{second}");
    assert_eq!(second["local_hits"], 1, "{second}");
}

#[test]
fn trusted_unix_pipeline_restores_c_objects_through_client_owned_stages() {
    let fixture = Fixture::new();
    let mut config = std::fs::read_to_string(&fixture.config).unwrap();
    config.push_str("trust_daemon_execution=true\n");
    std::fs::write(&fixture.config, config).unwrap();
    let source = fixture.root.path().join("unit.c");
    let target = fixture.root.path().join("unit.o");
    std::fs::write(&source, "int answer(void) { return 42; }\n").unwrap();
    let cc = "cc";
    let (_, started) = fixture.run(fixture.command(FRONTEND).args(["daemon", "start"]));
    assert!(started.success());
    let compile = || {
        let (_, status) = fixture.run(
            fixture
                .command(FRONTEND)
                .arg(cc)
                .arg("-c")
                .arg(&source)
                .arg("-o")
                .arg(&target),
        );
        let diagnostics = std::fs::read_to_string(fixture.root.path().join("stderr")).unwrap();
        assert!(status.success(), "{diagnostics}");
    };
    compile();
    let original = std::fs::read(&target).unwrap();
    std::fs::remove_file(&target).unwrap();
    compile();
    assert_eq!(std::fs::read(&target).unwrap(), original);
    let (_, status) = fixture.run(fixture.command(FRONTEND).args(["stats", "--json"]));
    assert!(status.success());
    let stats: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.root.path().join("stdout")).unwrap())
            .unwrap();
    assert_eq!(stats["misses"], 1, "{stats}");
    assert_eq!(stats["local_hits"], 1, "{stats}");
    assert!(
        !std::fs::read_dir(fixture.root.path())
            .unwrap()
            .any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".kache-restore-"))
    );
}

#[test]
fn killing_a_client_stops_its_native_compiler() {
    let fixture = Fixture::new();
    let mut config = std::fs::read_to_string(&fixture.config).unwrap();
    config.push_str("trust_daemon_execution=true\n");
    std::fs::write(&fixture.config, config).unwrap();
    let (_, started) = fixture.run(fixture.command(FRONTEND).args(["daemon", "start"]));
    assert!(started.success());
    let marker = fixture.root.path().join("compiler-pid");
    kache_fs::testutil::write_executable(
        &fixture.compiler,
        "#!/bin/sh\nprintf '%s' \"$$\" > \"$CHILD_RECORD\"\nexec sleep 30\n",
    );
    let mut client = fixture
        .command(FRONTEND)
        .arg(&fixture.compiler)
        .arg("--version")
        .env("CHILD_RECORD", &marker)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let ready = Instant::now() + Duration::from_secs(5);
    let pid = loop {
        if let Ok(value) = std::fs::read_to_string(&marker)
            && let Ok(pid) = value.parse::<i32>()
        {
            break pid;
        }
        if Instant::now() >= ready {
            let _ = client.kill();
            let _ = client.wait();
            panic!("compiler did not start");
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    client.kill().unwrap();
    client.wait().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        #[cfg(target_os = "linux")]
        let zombie = std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, state)| state.starts_with('Z'))
        });
        #[cfg(not(target_os = "linux"))]
        let zombie = false;
        if zombie || unsafe { libc::kill(pid, 0) } != 0 {
            break;
        }
        if Instant::now() >= deadline {
            // This PID still belongs to the fixture's sleeping compiler.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
            panic!("client exit left its compiler alive");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
