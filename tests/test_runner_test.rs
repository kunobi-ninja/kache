//! End-to-end coverage for `kache test-runner`, the opt-in Cargo target
//! runner that paces test binaries through the scheduler.
//!
//! A shell script named like a Cargo test binary (`probe-<16 hex>`) stands
//! in for the test. `PROBE_MODE` picks what it does. Its waiting loops give
//! up after a minute, so a failed test leaves nothing running. Unix-only: the
//! probe is a shell script and the signal checks are Unix signals.

#![cfg(unix)]

use std::fs;
use std::io::{Read, Write};
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::time::{Duration, Instant};

// Only the env plumbing is shared; this file drives the Cargo-built binary,
// so the bootstrap helpers in `common` stay unused here.
#[allow(dead_code)]
mod common;
use common::hermetic_command;

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

const PROBE: &str = "probe-0123456789abcdef";
const INNER: &str = "inner-0123456789abcdef";

const PROBE_SCRIPT: &str = r#"#!/bin/sh
case "$PROBE_MODE" in
  exit7) exit 7 ;;
  abort) ulimit -c 0 2>/dev/null; kill -ABRT $$; sleep 5; exit 1 ;;
  echo) exec cat ;;
  env) printf 'lease=%s threads=%s\n' "${KACHE_TEST_LEASE-unset}" "${RUST_TEST_THREADS-unset}" ;;
  started) : > "$PROBE_DIR/started" ;;
  term)
    trap 'exit 0' TERM
    : > "$PROBE_DIR/ready"
    i=0
    while [ "$i" -lt 1200 ]; do sleep 0.05; i=$((i + 1)); done
    exit 3
    ;;
  nested)
    : > "$PROBE_DIR/ready"
    i=0
    while [ ! -e "$PROBE_DIR/go" ] && [ "$i" -lt 3000 ]; do sleep 0.02; i=$((i + 1)); done
    "$KACHE_BIN" test-runner "$PROBE_DIR/inner-0123456789abcdef" || exit 11
    mkdir -p "$PROBE_DIR/out"
    "$KACHE_BIN" rustc --crate-name nested --edition=2021 "$PROBE_DIR/lib.rs" \
      --crate-type lib --emit=dep-info,metadata,link \
      -C metadata=0123456789abcdef -C extra-filename=-0123456789abcdef \
      --out-dir "$PROBE_DIR/out" -L "dependency=$PROBE_DIR/out" || exit 12
    : > "$PROBE_DIR/done"
    ;;
  *) exit 99 ;;
esac
"#;

/// A scratch directory holding the probe, its cache and its config.
struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        kache_fs::testutil::write_executable(&dir.path().join(PROBE), PROBE_SCRIPT);
        kache_fs::testutil::write_executable(
            &dir.path().join(INNER),
            "#!/bin/sh\n: > \"$PROBE_DIR/inner-ran\"\n",
        );
        fs::write(dir.path().join("lib.rs"), "pub fn answer() -> u8 { 42 }\n").unwrap();
        Self { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn cache(&self) -> PathBuf {
        self.path().join("cache")
    }

    fn probe(&self) -> PathBuf {
        self.path().join(PROBE)
    }

    /// `kache test-runner <program> <args>` in its own process group, with
    /// every variable the runner reads cleared.
    fn runner(&self, program: &Path, mode: &str) -> Command {
        let mut command = hermetic_command(
            kache_binary(),
            &self.cache(),
            Some(&self.path().join("missing-kache.toml")),
        );
        command
            .arg("test-runner")
            .arg(program)
            .env("PROBE_MODE", mode)
            .env("PROBE_DIR", self.path())
            .env("KACHE_BIN", kache_binary())
            .env_remove("KACHE_TEST_LEASE")
            .env_remove("RUST_TEST_THREADS")
            .env_remove("KACHE_SCHEDULER")
            .env_remove("KACHE_DISABLED")
            .process_group(0);
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("NEXTEST") {
                command.env_remove(name);
            }
        }
        command
    }

    fn events(&self) -> Vec<serde_json::Value> {
        fs::read_to_string(self.cache().join("events.jsonl"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
}

/// Wait for `child`, killing its whole process group at `timeout`.
fn finish(mut child: Child, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            kill_group(&child);
            let _ = child.wait();
            let output = collect(child, ExitStatus::from_raw(0));
            panic!(
                "test runner still running after {timeout:?}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    collect(child, status)
}

fn collect(mut child: Child, status: ExitStatus) -> Output {
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

fn kill_group(child: &Child) {
    // SAFETY: signals the process group the runner leads.
    unsafe { libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL) };
}

fn run(mut command: Command) -> Output {
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn kache test-runner");
    finish(child, Duration::from_secs(60))
}

fn wait_for_file(path: &Path, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "the runner exited before {} appeared",
            path.display()
        );
        if Instant::now() >= deadline {
            kill_group(child);
            panic!("{} never appeared", path.display());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Wait until the runner ignores SIGINT and SIGQUIT and catches SIGTERM and
/// SIGHUP. It sets them up only after it spawns the test, so a test that is
/// ready says nothing about the runner. Linux shows them in /proc; elsewhere
/// a grace period stands in.
fn wait_for_handlers(child: &mut Child) {
    #[cfg(target_os = "linux")]
    {
        let status = format!("/proc/{}/status", child.id());
        let bit = |signal: libc::c_int| 1u64 << (signal - 1);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let text = fs::read_to_string(&status).unwrap_or_default();
            let mask = |field: &str| {
                text.lines()
                    .find_map(|line| line.strip_prefix(field))
                    .and_then(|hex| u64::from_str_radix(hex.trim(), 16).ok())
                    .unwrap_or(0)
            };
            let ignored = mask("SigIgn:");
            let caught = mask("SigCgt:");
            if ignored & bit(libc::SIGINT) != 0
                && ignored & bit(libc::SIGQUIT) != 0
                && caught & bit(libc::SIGTERM) != 0
                && caught & bit(libc::SIGHUP) != 0
            {
                return;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "the runner exited before it handled signals"
            );
            if Instant::now() >= deadline {
                kill_group(child);
                panic!("the runner never set up its signal handlers:\n{text}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = child;
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The permit pool size kache uses on this machine.
fn pool() -> u32 {
    std::thread::available_parallelism().map_or(1, |n| n.get() as u32)
}

/// Lock every permit slot file in `range` that is free, as a compile would.
fn lock_slots(cache: &Path, range: std::ops::Range<u32>) -> Vec<fs::File> {
    let permits = cache.join("scheduler/permits");
    fs::create_dir_all(&permits).unwrap();
    range
        .filter_map(|index| {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(permits.join(index.to_string()))
                .unwrap();
            file.try_lock().ok().map(|()| file)
        })
        .collect()
}

#[test]
fn exit_codes_pass_through_scheduled_and_plain() {
    let fixture = Fixture::new();
    let scheduled = run(fixture.runner(&fixture.probe(), "exit7"));
    assert_eq!(scheduled.status.code(), Some(7), "{scheduled:?}");

    let fixture = Fixture::new();
    let mut plain = fixture.runner(&fixture.probe(), "exit7");
    plain.env("KACHE_SCHEDULER", "0");
    let plain = run(plain);
    assert_eq!(plain.status.code(), Some(7), "{plain:?}");
    assert!(
        !fixture.cache().join("scheduler").exists(),
        "with the scheduler off the runner leaves no lease files"
    );
}

/// `kache` exits at once, running nothing, when it sees the variables of a
/// compiler probe or a build-script launch. The runner is dispatched before
/// those checks, so a test inherits them and still runs.
#[test]
fn a_test_runs_under_the_probe_and_build_script_variables() {
    let fixture = Fixture::new();
    for (name, value) in [
        ("KACHE_FAMILY_PROBE_ACTIVE", "1"),
        ("KACHE_BUILD_SCRIPT_PATH", "/nonexistent/build-script-build"),
    ] {
        let mut command = fixture.runner(&fixture.probe(), "exit7");
        command.env(name, value);
        let output = run(command);
        assert_eq!(output.status.code(), Some(7), "{name}: {output:?}");
    }
}

#[test]
fn a_missing_binary_fails_like_a_failed_test() {
    let fixture = Fixture::new();
    let missing = fixture.path().join("gone-0123456789abcdef");
    let scheduled = run(fixture.runner(&missing, "exit7"));
    assert_eq!(scheduled.status.code(), Some(101), "{scheduled:?}");
    let mut plain = fixture.runner(&missing, "exit7");
    plain.env("KACHE_SCHEDULER", "0");
    assert_eq!(run(plain).status.code(), Some(101));

    let mut bare = hermetic_command(kache_binary(), &fixture.cache(), None);
    bare.arg("test-runner");
    assert_eq!(run(bare).status.code(), Some(101), "no binary to run");
}

#[test]
fn a_signal_death_passes_through() {
    let fixture = Fixture::new();
    let scheduled = run(fixture.runner(&fixture.probe(), "abort"));
    assert_eq!(
        scheduled.status.signal(),
        Some(libc::SIGABRT),
        "{scheduled:?}"
    );

    let mut plain = fixture.runner(&fixture.probe(), "abort");
    plain.env("KACHE_SCHEDULER", "0");
    let plain = run(plain);
    assert_eq!(plain.status.signal(), Some(libc::SIGABRT), "{plain:?}");
}

#[test]
fn stdin_reaches_the_test() {
    let fixture = Fixture::new();
    let mut child = fixture
        .runner(&fixture.probe(), "echo")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(b"hello\n").unwrap();
    let output = finish(child, Duration::from_secs(60));
    assert!(output.status.success(), "{output:?}");
    assert_eq!(output.stdout, b"hello\n");
}

#[test]
fn only_a_scheduled_test_sees_the_lease() {
    let fixture = Fixture::new();
    let env_of = |command: Command| {
        let output = run(command);
        assert!(output.status.success(), "{output:?}");
        String::from_utf8(output.stdout).unwrap()
    };

    let scheduled = env_of(fixture.runner(&fixture.probe(), "env"));
    let pool = pool();
    if pool > 1 {
        let tests = fixture.cache().join("scheduler/tests");
        let threads = pool - (pool / 4).max(1);
        assert!(
            scheduled.starts_with(&format!("lease={}/", tests.display())),
            "a scheduled test gets the absolute marker path: {scheduled}"
        );
        assert!(
            scheduled.ends_with(&format!(" threads={threads}\n")),
            "an elastic lease sets the thread count to the slots held: {scheduled}"
        );
    } else {
        assert_eq!(scheduled, "lease=unset threads=unset\n", "no test slots");
    }

    let mut declared = fixture.runner(&fixture.probe(), "env");
    declared.env("RUST_TEST_THREADS", "1");
    let declared = env_of(declared);
    assert!(
        declared.ends_with(" threads=1\n"),
        "a declared thread count is left alone: {declared}"
    );

    let mut off = fixture.runner(&fixture.probe(), "env");
    off.env("KACHE_SCHEDULER", "0");
    assert_eq!(env_of(off), "lease=unset threads=unset\n");

    let mut listing = fixture.runner(&fixture.probe(), "env");
    listing.arg("--list");
    assert_eq!(env_of(listing), "lease=unset threads=unset\n");

    // The runner execs this at once, so it must not be written through a
    // descriptor another thread's fork could still hold (ETXTBSY).
    let unhashed = fixture.path().join("demo");
    kache_fs::testutil::write_executable(&unhashed, PROBE_SCRIPT);
    assert_eq!(
        env_of(fixture.runner(&unhashed, "env")),
        "lease=unset threads=unset\n"
    );
}

#[test]
fn termination_reaches_the_test_and_interrupts_do_not_kill_the_runner() {
    let fixture = Fixture::new();
    let mut child = fixture
        .runner(&fixture.probe(), "term")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_file(&fixture.path().join("ready"), &mut child);
    wait_for_handlers(&mut child);
    let pid = child.id() as libc::pid_t;

    // SAFETY: signals the runner process only.
    unsafe { libc::kill(pid, libc::SIGINT) };
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        child.try_wait().unwrap().is_none(),
        "the runner ignores an interrupt aimed at it alone"
    );

    // SAFETY: as above.
    unsafe { libc::kill(pid, libc::SIGTERM) };
    let output = finish(child, Duration::from_secs(30));
    assert_eq!(
        output.status.code(),
        Some(0),
        "the test trapped the forwarded SIGTERM and exited 0: {output:?}"
    );
}

/// The regression the runner exists for: with every slot busy, a test
/// binary must not start until one frees up.
#[test]
fn a_test_waits_for_a_free_slot() {
    let pool = pool();
    if pool < 2 {
        eprintln!("skipped: a pool of one has no test slots");
        return;
    }
    let fixture = Fixture::new();
    // The first launch of a freshly built binary can take seconds (macOS
    // scans it). Launch it once so the wait below is the runner's own.
    let mut warm = fixture.runner(&fixture.probe(), "exit7");
    warm.env("KACHE_SCHEDULER", "0");
    assert_eq!(run(warm).status.code(), Some(7));

    let held = lock_slots(&fixture.cache(), 0..pool + 8);
    assert_eq!(held.len() as u32, pool + 8);

    let mut child = fixture
        .runner(&fixture.probe(), "started")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(500));
    let started = fixture.path().join("started");
    assert!(
        !started.exists(),
        "the test started while every slot was held"
    );
    assert!(child.try_wait().unwrap().is_none());

    drop(held);
    let output = finish(child, Duration::from_secs(60));
    assert!(output.status.success(), "{output:?}");
    assert!(started.exists(), "the test ran once a slot was free");
}

/// A test that runs `kache test-runner` and a cached compile inside itself
/// while every slot is held must finish: the nested processes are covered
/// by the outer test's lease.
#[test]
fn nested_runners_and_compiles_do_not_deadlock() {
    if pool() < 2 {
        eprintln!("skipped: a pool of one has no test slots, so no lease to inherit");
        return;
    }
    let fixture = Fixture::new();
    let mut child = fixture
        .runner(&fixture.probe(), "nested")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    wait_for_file(&fixture.path().join("ready"), &mut child);
    // The runner holds the test slots; take every other one, as running
    // compiles would.
    let _held = lock_slots(&fixture.cache(), 0..pool() + 8);
    fs::write(fixture.path().join("go"), b"go").unwrap();

    let output = finish(child, Duration::from_secs(90));
    assert!(
        output.status.success(),
        "nested run failed: {:?}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(fixture.path().join("inner-ran").exists());
    assert!(fixture.path().join("done").exists());
    let events = fixture.events();
    assert!(
        events
            .iter()
            .any(|event| event["crate_name"] == "nested" && event["result"] == "miss"),
        "the nested compile must take the scheduled miss path: {events:#?}"
    );
}
