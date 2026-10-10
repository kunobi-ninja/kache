//! Relocated cache hits must load the native library from the new target.

#![cfg(target_os = "macos")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

#[allow(dead_code)]
mod common;
use common::{hermetic_command, kache_binary, settle_writes, stop_daemon};

struct Fixture {
    root: TempDir,
    cache: TempDir,
    config: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let mut fixture = Self {
            root: TempDir::new().unwrap(),
            cache: TempDir::new().unwrap(),
            config: PathBuf::new(),
        };
        let config = fixture.cache.path().join("config.toml");
        fs::write(&config, "[cache]\nlocal_only = true\n").unwrap();
        fs::write(fixture.root.path().join("Cargo.toml"), "[workspace]\n").unwrap();
        fs::write(fixture.root.path().join("main.rs"), "#[link(name = \"foo\")]\nunsafe extern \"C\" { fn foo() -> i32; }\nfn main() { println!(\"{}\", unsafe { foo() }); }\n").unwrap();
        fs::write(
            fixture.root.path().join("foo.c"),
            "int foo(void) { return 7; }\n",
        )
        .unwrap();
        fixture.config = config;
        fixture
    }

    fn command(&self, program: impl AsRef<std::ffi::OsStr>) -> Command {
        let mut command = hermetic_command(program, self.cache.path(), Some(&self.config));
        command.env("KACHE_SOCKET_PATH", self.cache.path().join("k.sock"));
        command
    }

    fn compile(&self, target: &Path, stub: bool) -> (PathBuf, PathBuf) {
        let native = target.join("debug/build/native-1/out");
        let real = native.join("real");
        let search = if stub {
            native.join("stub")
        } else {
            real.clone()
        };
        let output = target.join("debug/deps");
        for dir in [&real, &search, &output] {
            fs::create_dir_all(dir).unwrap();
        }
        let library = real.join("libfoo.dylib");
        let built = Command::new("/usr/bin/cc")
            .args(["-dynamiclib", "-Wl,-install_name"])
            .arg(format!("-Wl,{}", library.display()))
            .arg(self.root.path().join("foo.c"))
            .arg("-o")
            .arg(&library)
            .output()
            .unwrap();
        assert!(
            built.status.success(),
            "{}",
            String::from_utf8_lossy(&built.stderr)
        );
        if stub {
            let arch = if cfg!(target_arch = "aarch64") {
                "arm64"
            } else {
                "x86_64"
            };
            fs::write(search.join("libfoo.tbd"), format!("--- !tapi-tbd\ntbd-version: 4\ntargets: [ {arch}-macos ]\ninstall-name: '{}'\nexports:\n  - targets: [ {arch}-macos ]\n    symbols: [ _foo ]\n...\n", library.display())).unwrap();
        }
        settle_writes(&[self.root.path(), &native]);
        let built = self
            .command(kache_binary())
            .arg(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .args([
                "--crate-name",
                "native_install_name",
                "--crate-type",
                "bin",
                "--edition=2024",
                "--emit=link,dep-info",
                "--out-dir",
            ])
            .arg(&output)
            .arg("-L")
            .arg(format!("native={}", search.display()))
            .arg(self.root.path().join("main.rs"))
            .env("CARGO_TARGET_DIR", target)
            .env("CARGO_MANIFEST_DIR", self.root.path())
            .current_dir(self.root.path())
            .output()
            .unwrap();
        assert!(
            built.status.success(),
            "{}",
            String::from_utf8_lossy(&built.stderr)
        );
        (output.join("native_install_name"), library)
    }

    fn hits(&self) -> u64 {
        let report = self
            .command(kache_binary())
            .args(["report", "--format", "json", "--since", "1h"])
            .output()
            .unwrap();
        assert!(report.status.success());
        let value: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
        value["summary"]["local_hits"].as_u64().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        stop_daemon(
            self.command(kache_binary()).args(["daemon", "stop"]),
            self.cache.path(),
        );
    }
}

fn relocated_library(stub: bool) {
    let fixture = Fixture::new();
    let targets = TempDir::new().unwrap();
    let first = targets.path().join("one");
    let second = targets.path().join("two");
    fixture.compile(&first, stub);
    let hits = fixture.hits();
    fixture.compile(&first, stub);
    assert_eq!(
        fixture.hits(),
        hits + 1,
        "an unchanged install name must hit"
    );
    let (binary, library) = fixture.compile(&second, stub);
    let load_commands = Command::new("/usr/bin/otool")
        .arg("-L")
        .arg(&binary)
        .output()
        .unwrap();
    assert!(load_commands.status.success());
    let text = String::from_utf8(load_commands.stdout).unwrap();
    assert!(text.contains(&library.display().to_string()), "{text}");
    fs::rename(&first, targets.path().join("removed")).unwrap();
    let ran = Command::new(&binary).output().unwrap();
    assert!(
        ran.status.success(),
        "{}",
        String::from_utf8_lossy(&ran.stderr)
    );
    assert_eq!(ran.stdout, b"7\n");
}

#[test]
fn dylib_install_name_follows_the_new_target() {
    relocated_library(false);
}

#[test]
fn text_stub_install_name_follows_the_new_target() {
    relocated_library(true);
}
