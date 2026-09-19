//! Exercise the outer Kache process, so config, passthrough and event wiring are covered.
#![cfg(unix)]
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

fn script(path: &Path, body: &str) {
    kache_fs::testutil::write_executable(path, format!("#!/bin/sh\n{body}\n"));
}

struct Fixture {
    dir: tempfile::TempDir,
    compiler: std::path::PathBuf,
    fallback: std::path::PathBuf,
    args: Vec<String>,
}
impl Fixture {
    fn new(rust: bool, fallback_body: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let compiler = dir.path().join(if rust { "rustc" } else { "cc" });
        let fallback = dir.path().join("fallback");
        script(
            &compiler,
            r#"
for arg in "$@"; do
  if [ "$arg" = "-vV" ]; then
    printf 'rustc 1.98.0\nbinary: rustc\ncommit-hash: fake\ncommit-date: 2026-09-01\nhost: aarch64-apple-darwin\nrelease: 1.98.0\nLLVM version: 22.1.0\n'
    exit 0
  fi
done
printf 'direct\n' >> "$TEST_CALLS"
printf '%s\n' "$@" > "$TEST_ARGS"
pwd > "$TEST_CWD"
printf '%s' "$TEST_SENTINEL" > "$TEST_ENV"
if [ "$TEST_STREAM" = "1" ]; then
  cat
  exit 0
fi
if [ "$TEST_COMPILER_EXIT" != "0" ]; then
  echo 'direct compiler error' >&2
  exit "$TEST_COMPILER_EXIT"
fi
printf 'complete-object' > "$TEST_OBJECT"
"#,
        );
        script(&fallback, fallback_body);
        fs::write(dir.path().join("input.s"), "").unwrap();
        fs::write(dir.path().join("input.rs"), "fn main() {}\n").unwrap();
        let args = if rust {
            // Link passthrough avoids caching; preserve all relevant compiler inputs.
            vec![
                "--crate-name",
                "fallback_fixture",
                "input.rs",
                "--crate-type",
                "bin",
                "--emit=link",
                "-o",
                "object.o",
            ]
        } else {
            vec!["-xassembler-with-cpp", "-c", "input.s", "-o", "object.o"]
        }
        .into_iter()
        .map(String::from)
        .collect();
        Self {
            dir,
            compiler,
            fallback,
            args,
        }
    }
    fn command(&self, compiler_exit: i32) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_kache"));
        command
            .arg(&self.compiler)
            .args(&self.args)
            .current_dir(self.dir.path())
            .env("KACHE_CONFIG", self.dir.path().join("absent.toml"))
            .env("KACHE_HOST_CONFIG", "")
            .env("KACHE_CACHE_DIR", self.dir.path().join("cache"))
            .env("KACHE_BASE_DIR", self.dir.path())
            .env("KACHE_FALLBACK", &self.fallback)
            .env("KACHE_CACHE_EXECUTABLES", "false")
            .env("KACHE_REMOTE", "")
            .env_remove("KACHE_DISABLED")
            .env("TEST_CALLS", self.dir.path().join("calls"))
            .env("TEST_ARGS", self.dir.path().join("args"))
            .env("TEST_CWD", self.dir.path().join("cwd"))
            .env("TEST_ENV", self.dir.path().join("env"))
            .env("TEST_SENTINEL", "same-environment")
            .env("TEST_OBJECT", self.dir.path().join("object.o"))
            .env("TEST_COMPILER_EXIT", compiler_exit.to_string());
        command
    }
    fn run(&self, compiler_exit: i32) -> Output {
        self.command(compiler_exit).output().unwrap()
    }
    fn event(&self) -> serde_json::Value {
        let log = fs::read_to_string(self.dir.path().join("cache/events.jsonl")).unwrap();
        log.lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|event| event["result"] == "passthrough")
            .unwrap()
    }
    fn calls(&self) -> String {
        fs::read_to_string(self.dir.path().join("calls")).unwrap()
    }
}
const FAILING: &str = r#"
printf 'fallback\n' >> "$TEST_CALLS"
printf 'partial' > "$TEST_OBJECT"
echo 'fallback compiler error' >&2
exit 42
"#;

#[test]
fn failed_fallback_retries_once_preserving_compiler_inputs_and_events() {
    for rust in [false, true] {
        let fixture = Fixture::new(rust, FAILING);
        let output = fixture.run(0);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stderr}");
        assert_eq!(fixture.calls(), "fallback\ndirect\n");
        assert_eq!(
            fs::read_to_string(fixture.dir.path().join("args")).unwrap(),
            fixture.args.join("\n") + "\n"
        );
        assert_eq!(
            fs::read_to_string(fixture.dir.path().join("env")).unwrap(),
            "same-environment"
        );
        assert_eq!(
            fs::read_to_string(fixture.dir.path().join("cwd"))
                .unwrap()
                .trim(),
            fixture.dir.path().canonicalize().unwrap().to_str().unwrap()
        );
        assert_eq!(
            fs::read_to_string(fixture.dir.path().join("object.o")).unwrap(),
            "complete-object"
        );
        assert!(stderr.contains("fallback compiler error"));
        assert!(stderr.contains("kache: warning: fallback"));
        assert!(stderr.contains("compiling directly without fallback"));
        let event = fixture.event();
        assert_eq!(event["schema"], 20);
        assert_eq!(event["exit_code"], 0);
        assert_ne!(event["fallback"], true);
        assert_eq!(event["fallback_attempt"]["outcome"], "failed");
        assert_eq!(event["fallback_attempt"]["exit_code"], 42);
        assert_eq!(
            event["fallback_attempt"]["wrapper"],
            fixture.fallback.to_str().unwrap()
        );
    }
}

#[test]
fn genuine_compiler_failure_is_preserved_after_retry() {
    for rust in [false, true] {
        let fixture = Fixture::new(rust, FAILING);
        let output = fixture.run(7);
        assert_eq!(output.status.code(), Some(7));
        assert_eq!(fixture.calls(), "fallback\ndirect\n");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("direct compiler error"));
        assert_eq!(fixture.event()["exit_code"], 7);
    }
}

#[test]
fn successful_and_cancelled_fallbacks_do_not_retry() {
    for rust in [false, true] {
        for (code, outcome) in [(0, "success"), (130, "cancelled"), (143, "cancelled")] {
            let fixture = Fixture::new(
                rust,
                &format!("printf 'fallback\\n' >> \"$TEST_CALLS\"\nexit {code}"),
            );
            let output = fixture.run(9);
            assert_eq!(output.status.code(), Some(code));
            assert_eq!(fixture.calls(), "fallback\n");
            assert!(!String::from_utf8_lossy(&output.stderr).contains("compiling directly"));
            let event = fixture.event();
            assert_eq!(event["fallback"], true);
            assert_eq!(event["fallback_attempt"]["outcome"], outcome);
        }
    }
}

#[test]
fn missing_and_non_executable_fallbacks_compile_directly() {
    for rust in [false, true] {
        for missing in [false, true] {
            let fixture = Fixture::new(rust, "exit 9");
            if missing {
                fs::remove_file(&fixture.fallback).unwrap();
            } else {
                fs::set_permissions(&fixture.fallback, fs::Permissions::from_mode(0o644)).unwrap();
            }
            let output = fixture.run(0);
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(fixture.calls(), "direct\n");
            assert_eq!(
                fixture.event()["fallback_attempt"]["outcome"],
                "spawn_failed"
            );
        }
    }
}

#[cfg(target_os = "macos")]
#[test]
fn sandbox_preflight_bypasses_denied_server_but_keeps_allowed_server() {
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};
    let fixture = Fixture::new(false, FAILING);
    let server_dir = tempfile::tempdir().unwrap();
    let server = server_dir.path().join("sccache");
    // Build a small listener: copying a signed system executable can be killed
    // by macOS code-signing enforcement before the test ever opens a socket.
    let source = server_dir.path().join("listener.c");
    fs::write(
        &source,
        r#"
#include <arpa/inet.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(atoi(argv[1]));
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) return 3;
    if (listen(fd, 16) != 0) return 4;
    for (;;) pause();
}
"#,
    )
    .unwrap();
    assert!(
        Command::new("/usr/bin/cc")
            .arg(&source)
            .arg("-o")
            .arg(&server)
            .status()
            .unwrap()
            .success()
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let denied = fixture.dir.path().canonicalize().unwrap();
    let profile = format!(
        "(version 1)(allow default)(deny file-write* (subpath \"{}\"))",
        denied.display()
    );
    let mut child = Command::new("/usr/bin/sandbox-exec")
        .args(["-p", &profile])
        .arg(&server)
        .arg(port.to_string())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while TcpStream::connect(("127.0.0.1", port)).is_err() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    // The fallback command is never this listener executable. Its basename
    // selects sccache preflight, and its marker proves whether it was invoked.
    let mut fixture = fixture;
    let renamed = fixture.dir.path().join("sccache");
    fs::rename(&fixture.fallback, &renamed).unwrap();
    fixture.fallback = renamed;
    let output = fixture
        .command(0)
        .env("SCCACHE_SERVER_PORT", port.to_string())
        .env_remove("SCCACHE_SERVER_UDS")
        .output()
        .unwrap();
    let allowed = Fixture::new(false, FAILING);
    let mut allowed = allowed;
    let renamed = allowed.dir.path().join("sccache");
    fs::rename(&allowed.fallback, &renamed).unwrap();
    allowed.fallback = renamed;
    let allowed_output = allowed
        .command(0)
        .env("SCCACHE_SERVER_PORT", port.to_string())
        .env_remove("SCCACHE_SERVER_UDS")
        .output()
        .unwrap();
    let _ = child.kill();
    child.wait().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fixture.calls(),
        "direct\n",
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fixture.event()["fallback_attempt"]["outcome"],
        "sandbox_denied"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("sandbox denies writing"));
    assert!(allowed_output.status.success());
    assert_eq!(allowed.calls(), "fallback\ndirect\n");
    assert_eq!(allowed.event()["fallback_attempt"]["outcome"], "failed");
}

#[test]
fn stdin_reaches_the_direct_compiler_once_without_partial_fallback_stdout() {
    use std::io::Write;
    use std::process::Stdio;
    for rust in [false, true] {
        let mut fixture = Fixture::new(
            rust,
            "printf 'fallback\\n' >> \"$TEST_CALLS\"; cat >/dev/null; echo partial; exit 42",
        );
        for arg in &mut fixture.args {
            if arg == "input.s" || arg == "input.rs" {
                *arg = "-".into();
            }
        }
        let mut child = fixture
            .command(0)
            .env("TEST_STREAM", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(b"original input\n")
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"original input\n");
        assert_eq!(fixture.calls(), "direct\n");
        assert_eq!(
            fixture.event()["fallback_attempt"]["outcome"],
            "direct_required"
        );
    }
}
