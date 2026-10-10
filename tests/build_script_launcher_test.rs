//! Build-script launchers must work when kache is installed through a symlink.

#![cfg(unix)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[allow(dead_code)]
mod common;
use common::{hermetic_command, stop_daemon};

struct Fixture(tempfile::TempDir);

impl Fixture {
    fn new() -> Self {
        let fixture = Self(tempfile::tempdir().unwrap());
        fs::write(fixture.path().join("empty.toml"), "").unwrap();
        fs::write(fixture.path().join("build.rs"), "fn main() {}\n").unwrap();
        fs::create_dir_all(fixture.unit()).unwrap();
        fixture
    }

    fn path(&self) -> &Path {
        self.0.path()
    }

    fn cache(&self) -> PathBuf {
        self.path().join("cache")
    }

    fn profile(&self) -> PathBuf {
        self.path().join("target/debug")
    }

    fn unit(&self) -> PathBuf {
        self.profile().join("build/pkg-1")
    }

    fn command(&self, binary: &Path) -> Command {
        let mut command =
            hermetic_command(binary, &self.cache(), Some(&self.path().join("empty.toml")));
        command
            .current_dir(self.path())
            .env("KACHE_SOCKET_PATH", self.path().join("k.sock"))
            .env("CARGO_MANIFEST_DIR", self.path())
            .env("CARGO_PKG_NAME", "pkg")
            .env("KACHE_DAEMON_PUBLISH", "0");
        command
    }

    fn compile(&self, binary: &Path) -> Output {
        self.command(binary)
            .args([
                "rustc",
                "--crate-name",
                "build_script_build",
                "--crate-type",
                "bin",
                "build.rs",
                "--out-dir",
            ])
            .arg(self.unit())
            .args(["-C", "extra-filename=-1"])
            .output()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let mut command = self.command(Path::new(env!("CARGO_BIN_EXE_kache")));
        command.args(["daemon", "stop"]);
        stop_daemon(&mut command, &self.cache());
    }
}

#[test]
fn a_relative_kache_symlink_installs_an_executable_pin() {
    let fixture = Fixture::new();
    let cell = fixture.path().join("Cellar/kache/1/bin");
    let bin = fixture.path().join("bin");
    fs::create_dir_all(&cell).unwrap();
    fs::create_dir(&bin).unwrap();
    fs::hard_link(env!("CARGO_BIN_EXE_kache"), cell.join("kache")).unwrap();
    let symlink = bin.join("kache");
    std::os::unix::fs::symlink("../Cellar/kache/1/bin/kache", &symlink).unwrap();
    let output = fixture.compile(&symlink);
    assert!(output.status.success(), "{output:?}");
    let record = fs::read_to_string(fixture.unit().join(".kache-launch")).unwrap();
    let relative = record.split('\0').nth(1).unwrap();
    let pinned = fixture.profile().join(relative);
    assert!(fs::symlink_metadata(&pinned).unwrap().is_file());
    let output = fixture.command(&pinned).arg("--version").output().unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(output.stdout.starts_with(b"kache "));
}

#[test]
fn an_install_failure_records_why_the_script_will_run_uncached() {
    let fixture = Fixture::new();
    fs::write(
        fixture.profile().join(".kache-build-script-shims"),
        "blocked",
    )
    .unwrap();
    let output = fixture.compile(Path::new(env!("CARGO_BIN_EXE_kache")));
    assert!(output.status.success(), "{output:?}");
    assert!(!fixture.unit().join(".kache-launch").exists());
    let script = fixture.unit().join("build_script_build-1");
    let output = fixture.command(&script).output().unwrap();
    assert!(output.status.success(), "{output:?}");
    let log = fs::read_to_string(fixture.cache().join("events.jsonl")).unwrap();
    let events: Vec<serde_json::Value> = log
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let event = events
        .iter()
        .find(|event| event["crate_name"] == "build_script_run")
        .expect("the install failure needs a build-script event");
    assert_eq!(event["result"], "passthrough");
    assert_eq!(
        event["passthrough_reason"],
        "refused|build-script cache bypassed: installing the build-script launcher"
    );
}
