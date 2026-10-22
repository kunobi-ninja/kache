use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStdout, Command, Output, Stdio};
use std::time::{Duration, Instant};

mod common;
use common::{build_kache, hermetic_command, isolated_config_path, kache_binary};

struct LockHolder {
    child: Child,
    _stdout: BufReader<ChildStdout>,
}

impl LockHolder {
    fn start(path: &Path, shared: bool) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "hold_log_lock_in_child", "--nocapture"])
            .env("KACHE_TEST_EVENT_LOCK", path)
            .env(
                "KACHE_TEST_EVENT_LOCK_SHARED",
                if shared { "1" } else { "0" },
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        loop {
            let mut line = String::new();
            assert_ne!(
                stdout.read_line(&mut line).unwrap(),
                0,
                "lock holder exited before acquiring"
            );
            if line.contains("EVENT_LOCK_HELD") {
                break;
            }
        }
        Self {
            child,
            _stdout: stdout,
        }
    }
}

impl Drop for LockHolder {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        let _ = self.child.wait();
    }
}

#[test]
fn hold_log_lock_in_child() {
    let Some(path) = std::env::var_os("KACHE_TEST_EVENT_LOCK") else {
        return;
    };
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    if std::env::var_os("KACHE_TEST_EVENT_LOCK_SHARED").unwrap() == "1" {
        lock.lock_shared().unwrap();
    } else {
        lock.lock().unwrap();
    }
    println!("EVENT_LOCK_HELD");
    std::io::stdout().flush().unwrap();
    let _ = std::io::stdin().read_exact(&mut [0]);
}

fn run_bounded(mut command: Command) -> (bool, Output) {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let started = Instant::now();
    let returned = loop {
        if child.try_wait().unwrap().is_some() {
            break true;
        }
        if started.elapsed() >= Duration::from_secs(5) {
            child.kill().unwrap();
            break false;
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    (returned, child.wait_with_output().unwrap())
}

fn wrapper(cache: &Path) -> Command {
    build_kache();
    let config = isolated_config_path(cache);
    fs::write(
        &config,
        "[cache]\nauto_gc = false\nheartbeat_secs = 0\ndaemon_publish = false\nevent_log_max_size = \"1B\"\n",
    )
    .unwrap();
    let mut command = hermetic_command(kache_binary(), cache, Some(&config));
    command
        .env("KACHE_AUTO_GC", "0")
        .env("KACHE_HEARTBEAT_SECS", "0")
        .env("KACHE_DAEMON_PUBLISH", "0");
    command
}

#[test]
fn version_probe_returns_while_another_process_holds_the_event_lock() {
    for shared in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        let _daemon = common::DaemonGuard::new(common::kache_binary(), &cache);
        fs::create_dir(&cache).unwrap();
        let lock_path = cache.join("events.jsonl.lock");
        let holder = LockHolder::start(&lock_path, shared);
        let mut command = wrapper(&cache);
        command.args(["rustc", "-vV"]);
        let (returned, output) = run_bounded(command);
        drop(holder);
        assert!(returned, "version probe waited on optional telemetry");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("rustc "));
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path)
            .unwrap();
        lock.try_lock()
            .expect("child exit must release the event lock");
    }
}

#[test]
fn completed_compile_returns_while_another_process_holds_the_event_lock() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let _daemon = common::DaemonGuard::new(common::kache_binary(), &cache);
    fs::create_dir(&cache).unwrap();
    let source = dir.path().join("answer.rs");
    let output_path = dir.path().join("answer.rlib");
    fs::write(&source, "pub fn answer() -> u32 { 42 }\n").unwrap();
    let holder = LockHolder::start(&cache.join("events.jsonl.lock"), true);
    let mut command = wrapper(&cache);
    command
        .args(["rustc", "--crate-name", "answer", "--crate-type", "rlib"])
        .arg(&source)
        .arg("-o")
        .arg(&output_path);
    let (returned, output) = run_bounded(command);
    drop(holder);
    assert!(returned, "completed compiler waited on optional telemetry");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_path.metadata().unwrap().len() > 0);
}

#[test]
fn version_probe_defers_rotation_while_the_ledger_is_locked() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    let _daemon = common::DaemonGuard::new(common::kache_binary(), &cache);
    fs::create_dir(&cache).unwrap();
    let holder = LockHolder::start(&cache.join("savings.json.lock"), false);
    let mut command = wrapper(&cache);
    command.args(["rustc", "-vV"]);
    let (returned, output) = run_bounded(command);
    drop(holder);
    assert!(returned, "rotation waited for telemetry bookkeeping");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(cache.join("events.jsonl").metadata().unwrap().len() > 1);
    let mut command = wrapper(&cache);
    command.args(["rustc", "-vV"]);
    let (returned, output) = run_bounded(command);
    assert!(returned && output.status.success());
    assert!(cache.join("savings.json").is_file());
}
