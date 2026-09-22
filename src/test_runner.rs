//! `kache test-runner`: an opt-in Cargo target runner that paces test
//! binaries through the machine-wide scheduler.
//!
//! A project opts in with
//! `[target.'cfg(all())'] runner = ["kache", "test-runner"]` in its
//! `.cargo/config.toml`, and Cargo runs every test binary as
//! `kache test-runner <binary> <args>`.
//!
//! A test binary waits for slots above the compile reserve (see
//! [`scheduler::reserve_for`]) and holds them until it exits. Its children
//! inherit the lease marker in [`scheduler::TEST_LEASE_ENV`], so compiles and
//! test runners nested inside it take no slots of their own. Everything else
//! runs unchanged: binaries without Cargo's metadata hash (`cargo run`,
//! doctests), listing, benches, and any run with the scheduler off.
//!
//! The runner never writes to stdout: test harnesses and nextest read it.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use crate::scheduler::{self, TestWant};

/// Wait budget under nextest, which counts the wait toward its own
/// `slow-timeout` and `terminate-after`.
const NEXTEST_WAIT: Duration = Duration::from_secs(120);

/// Exit code when the test binary cannot be run: the code a failed test run
/// gives Cargo.
const RUN_FAILED: i32 = 101;

/// The environment the runner reads, captured once at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TestEnv {
    /// `NEXTEST_EXECUTION_MODE`, set whenever nextest runs the binary.
    pub(crate) nextest_mode: Option<String>,
    /// `RUST_TEST_THREADS`.
    pub(crate) rust_test_threads: Option<String>,
    /// [`scheduler::TEST_LEASE_ENV`] inherited from a test above this one.
    pub(crate) lease: Option<PathBuf>,
    /// `CARGO_PKG_NAME` of the package the binary belongs to.
    pub(crate) pkg_name: Option<String>,
}

impl TestEnv {
    fn capture() -> Self {
        Self {
            nextest_mode: std::env::var("NEXTEST_EXECUTION_MODE").ok(),
            rust_test_threads: std::env::var("RUST_TEST_THREADS").ok(),
            lease: std::env::var_os(scheduler::TEST_LEASE_ENV).map(PathBuf::from),
            pkg_name: std::env::var("CARGO_PKG_NAME").ok(),
        }
    }
}

/// What the runner does with a test binary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Plan {
    /// Run it unchanged.
    Exec,
    /// Take a test lease first. `key` names the binary's RSS history.
    Schedule { key: String, want: TestWant },
}

/// Run `args` (the test binary, then its arguments) and return the exit code
/// for the runner. Returns only when the binary could not be run or has
/// exited; a binary killed by a signal kills the runner with it.
pub fn run(args: &[OsString]) -> i32 {
    if args.is_empty() {
        eprintln!("kache test-runner: expected a test binary to run");
        return RUN_FAILED;
    }
    let env = TestEnv::capture();
    let config = crate::config::Config::load()
        .ok()
        .filter(|config| config.scheduler);
    let cache_dir = config.as_ref().map(|config| config.cache_dir.as_path());
    match (cache_dir, plan(args, &env, cache_dir)) {
        (Some(cache_dir), Plan::Schedule { key, want }) => {
            run_scheduled(args, cache_dir, &env, &key, want)
        }
        _ => exec(args),
    }
}

/// Decide whether to schedule `args`. `cache_dir` is `None` when the
/// scheduler is off or the config could not be loaded.
pub(crate) fn plan(args: &[OsString], env: &TestEnv, cache_dir: Option<&Path>) -> Plan {
    let Some(cache_dir) = cache_dir else {
        return Plan::Exec;
    };
    let Some(stem) = args.first().and_then(|program| artifact_stem(program)) else {
        return Plan::Exec;
    };
    let rest = &args[1..];
    if rest.iter().any(|arg| arg == "--list" || arg == "--bench") {
        return Plan::Exec;
    }
    let root = scheduler::scheduler_root(cache_dir);
    if scheduler::lease_covers(env.lease.as_deref(), &root) {
        return Plan::Exec;
    }
    let key = format!(
        "test:{}:{stem}",
        env.pkg_name.as_deref().unwrap_or_default()
    );
    let floor = scheduler::test_floor(cache_dir, &key);
    let want = if env.nextest_mode.as_deref() == Some("process-per-test") {
        TestWant::Fixed(floor)
    } else if let Some(threads) = declared_threads(rest, env.rust_test_threads.as_deref()) {
        TestWant::Fixed(threads.max(floor))
    } else {
        TestWant::Elastic(floor)
    };
    Plan::Schedule { key, want }
}

/// The name of a Cargo test binary without its `-<16 hex>` metadata suffix
/// or `.exe`. `None` for any other name: `cargo run` binaries and doctests
/// (`rust_out`) carry no hash.
pub(crate) fn artifact_stem(program: &OsStr) -> Option<&str> {
    let name = Path::new(program).file_name()?.to_str()?;
    let name = name.strip_suffix(".exe").unwrap_or(name);
    let (stem, hash) = name.rsplit_once('-')?;
    let hashed = hash.len() == 16 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
    hashed.then_some(stem)
}

/// Threads the binary was told to use: the first valid `--test-threads N`
/// or `--test-threads=N` among `args`, else a valid `RUST_TEST_THREADS`.
/// Zero, garbage and a missing value are not valid.
pub(crate) fn declared_threads(args: &[OsString], env_threads: Option<&str>) -> Option<u32> {
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        let value = if arg == "--test-threads" {
            args.next().and_then(|value| value.to_str())
        } else {
            arg.to_str()
                .and_then(|arg| arg.strip_prefix("--test-threads="))
        };
        if let Some(threads) = value.and_then(positive) {
            return Some(threads);
        }
    }
    env_threads.and_then(positive)
}

fn positive(value: &str) -> Option<u32> {
    value.parse().ok().filter(|threads| *threads > 0)
}

/// How long to wait for a lease before running without one.
pub(crate) fn wait_budget(env: &TestEnv) -> Duration {
    if env.nextest_mode.is_some() {
        NEXTEST_WAIT
    } else {
        scheduler::WAIT_TIMEOUT
    }
}

/// Variables a leased test binary gets: the lease marker, and for an
/// elastic want the number of slots held as libtest's thread count.
/// Without that, libtest starts a thread per CPU and the lease bounds
/// nothing.
fn lease_env(want: TestWant, marker: &Path, held: u32) -> Vec<(&'static str, OsString)> {
    let mut vars = vec![(scheduler::TEST_LEASE_ENV, marker.as_os_str().to_owned())];
    if let TestWant::Elastic(_) = want {
        vars.push(("RUST_TEST_THREADS", held.to_string().into()));
    }
    vars
}

/// How the runner ends once the test binary has exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// Exit with this code.
    Exit(i32),
    /// Die of `signal` as the binary did, or exit with `fallback` when
    /// raising it returns.
    Signal { signal: i32, fallback: i32 },
}

/// Map the binary's exit code or terminating signal to the runner's end.
pub(crate) fn exit_disposition(code: Option<i32>, signal: Option<i32>) -> Disposition {
    match (code, signal) {
        (Some(code), _) => Disposition::Exit(code),
        (None, Some(signal)) => Disposition::Signal {
            signal,
            fallback: 128 + signal,
        },
        (None, None) => Disposition::Exit(RUN_FAILED),
    }
}

/// Run the binary as it is. On Unix the runner becomes the binary.
fn exec(args: &[OsString]) -> i32 {
    let mut command = Command::new(&args[0]);
    command.args(&args[1..]);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let error = command.exec();
        run_failed(&args[0], &error)
    }
    #[cfg(not(unix))]
    {
        match command.status() {
            Ok(status) => status.code().unwrap_or(RUN_FAILED),
            Err(error) => run_failed(&args[0], &error),
        }
    }
}

/// Run the binary under a test lease, then record its peak RSS.
fn run_scheduled(
    args: &[OsString],
    cache_dir: &Path,
    env: &TestEnv,
    key: &str,
    want: TestWant,
) -> i32 {
    let lease = scheduler::acquire_test_lease(cache_dir, want, wait_budget(env));
    let mut command = Command::new(&args[0]);
    command.args(&args[1..]);
    if let Some(lease) = &lease {
        command.envs(lease_env(want, lease.marker_path(), lease.held()));
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return run_failed(&args[0], &error),
    };
    signals::forward_to(child.id());
    let status = child.wait();
    drop(lease);
    scheduler::record_test_rss(cache_dir, key);
    match status {
        Ok(status) => finish(exit_disposition(status.code(), signals::of(&status))),
        Err(error) => run_failed(&args[0], &error),
    }
}

fn run_failed(program: &OsStr, error: &std::io::Error) -> i32 {
    eprintln!(
        "kache test-runner: cannot run {}: {error}",
        Path::new(program).display()
    );
    RUN_FAILED
}

/// The runner's exit code. A signal death re-raises the signal first.
fn finish(disposition: Disposition) -> i32 {
    match disposition {
        Disposition::Exit(code) => code,
        Disposition::Signal { signal, fallback } => {
            signals::raise(signal);
            fallback
        }
    }
}

#[cfg(unix)]
mod signals {
    use std::sync::atomic::{AtomicI32, Ordering};

    static CHILD: AtomicI32 = AtomicI32::new(0);

    extern "C" fn forward(signal: libc::c_int) {
        // SAFETY: `kill` is async-signal-safe, and `forward_to` stores the
        // pid before it installs this handler.
        unsafe { libc::kill(CHILD.load(Ordering::SeqCst), signal) };
    }

    /// Leave terminal interrupts to the child, which shares the process
    /// group and receives them itself, and pass termination on to it. The
    /// runner then exits the way the child does.
    pub(super) fn forward_to(child: u32) {
        CHILD.store(child as libc::pid_t, Ordering::SeqCst);
        let handler = forward as extern "C" fn(libc::c_int) as libc::sighandler_t;
        for (signal, action) in [
            (libc::SIGINT, libc::SIG_IGN),
            (libc::SIGQUIT, libc::SIG_IGN),
            (libc::SIGTERM, handler),
            (libc::SIGHUP, handler),
        ] {
            // SAFETY: a zeroed `sigaction` is a valid empty action; the
            // handler only calls `kill`.
            unsafe {
                let mut sa: libc::sigaction = std::mem::zeroed();
                sa.sa_sigaction = action;
                sa.sa_flags = libc::SA_RESTART;
                libc::sigemptyset(&mut sa.sa_mask);
                libc::sigaction(signal, &sa, std::ptr::null_mut());
            }
        }
    }

    pub(super) fn of(status: &std::process::ExitStatus) -> Option<i32> {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    }

    /// Die of `signal` without dumping core: the child already did.
    pub(super) fn raise(signal: i32) {
        // SAFETY: plain libc calls on values owned here.
        unsafe {
            let no_core = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            libc::setrlimit(libc::RLIMIT_CORE, &no_core);
            libc::signal(signal, libc::SIG_DFL);
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, signal);
            libc::pthread_sigmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
            libc::raise(signal);
        }
    }
}

// Named apart from the Unix module so the Linux mutation lane, which never
// compiles it, can exclude it by name.
#[cfg(windows)]
use windows_signals as signals;

#[cfg(windows)]
mod windows_signals {
    /// Leave Ctrl-C to the child, which shares the console and receives it
    /// itself. The runner then exits the way the child does.
    pub(super) fn forward_to(_child: u32) {
        // SAFETY: a null handler with TRUE makes this process ignore Ctrl-C.
        unsafe {
            windows_sys::Win32::System::Console::SetConsoleCtrlHandler(None, 1);
        }
    }

    pub(super) fn of(_status: &std::process::ExitStatus) -> Option<i32> {
        None
    }

    pub(super) fn raise(_signal: i32) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    fn env() -> TestEnv {
        TestEnv {
            nextest_mode: None,
            rust_test_threads: None,
            lease: None,
            pkg_name: Some("demo".into()),
        }
    }

    const PROBE: &str = "/t/debug/deps/probe-0123456789abcdef";

    #[test]
    fn artifact_stem_requires_cargo_metadata_hash() {
        let stem = |name: &str| artifact_stem(OsStr::new(name)).map(str::to_owned);
        assert_eq!(stem(PROBE).as_deref(), Some("probe"));
        assert_eq!(stem("probe-0123456789abcdef").as_deref(), Some("probe"));
        assert_eq!(
            stem("C:/t/deps/my-crate-0123456789abcdef.exe").as_deref(),
            Some("my-crate")
        );
        assert_eq!(stem("/t/debug/demo"), None, "cargo run binary");
        assert_eq!(stem("/tmp/rustdoctestXYZ/rust_out"), None, "doctest");
        assert_eq!(stem("probe-0123456789abcde"), None, "15 hex digits");
        assert_eq!(stem("probe-0123456789abcdef0"), None, "17 hex digits");
        assert_eq!(stem("probe-0123456789ABCDEF"), None, "uppercase");
        assert_eq!(stem("probe-0123456789abcdeg"), None, "not hex");
    }

    #[test]
    fn declared_threads_reads_argv_before_env() {
        assert_eq!(
            declared_threads(&os(&["--test-threads", "3"]), None),
            Some(3)
        );
        assert_eq!(declared_threads(&os(&["--test-threads=5"]), None), Some(5));
        assert_eq!(
            declared_threads(&os(&["--test-threads=5"]), Some("2")),
            Some(5),
            "argv beats env"
        );
        assert_eq!(declared_threads(&os(&["--exact", "t"]), Some("2")), Some(2));
        assert_eq!(declared_threads(&os(&[]), None), None);
    }

    #[test]
    fn declared_threads_rejects_zero_garbage_and_missing_values() {
        assert_eq!(declared_threads(&os(&["--test-threads=0"]), None), None);
        assert_eq!(declared_threads(&os(&["--test-threads", "x"]), None), None);
        assert_eq!(declared_threads(&os(&["--test-threads"]), None), None);
        assert_eq!(declared_threads(&os(&[]), Some("0")), None);
        assert_eq!(declared_threads(&os(&[]), Some("many")), None);
        assert_eq!(declared_threads(&os(&[]), Some("1")), Some(1));
        assert_eq!(
            declared_threads(&os(&["--test-threads=0"]), Some("4")),
            Some(4),
            "an invalid argv value falls back to env"
        );
    }

    #[test]
    fn plan_execs_when_the_scheduler_is_off() {
        assert_eq!(plan(&os(&[PROBE]), &env(), None), Plan::Exec);
    }

    #[test]
    fn plan_execs_unhashed_binaries() {
        let cache = tempfile::tempdir().unwrap();
        assert_eq!(
            plan(&os(&["/t/debug/demo"]), &env(), Some(cache.path())),
            Plan::Exec
        );
        assert_eq!(plan(&[], &env(), Some(cache.path())), Plan::Exec);
    }

    #[test]
    fn plan_execs_listing_and_benches() {
        let cache = tempfile::tempdir().unwrap();
        for flag in ["--list", "--bench"] {
            assert_eq!(
                plan(&os(&[PROBE, flag]), &env(), Some(cache.path())),
                Plan::Exec,
                "{flag}"
            );
        }
    }

    #[test]
    fn plan_execs_under_a_covering_lease() {
        let cache = tempfile::tempdir().unwrap();
        let tests = scheduler::scheduler_root(cache.path()).join("tests");
        std::fs::create_dir_all(&tests).unwrap();
        let covered = TestEnv {
            lease: Some(tests.join("1")),
            ..env()
        };
        assert_eq!(
            plan(&os(&[PROBE]), &covered, Some(cache.path())),
            Plan::Exec
        );
        let other = tempfile::tempdir().unwrap();
        assert!(matches!(
            plan(&os(&[PROBE]), &covered, Some(other.path())),
            Plan::Schedule { .. }
        ));
    }

    #[test]
    fn plan_wants_elastic_floor_by_default() {
        let cache = tempfile::tempdir().unwrap();
        assert_eq!(
            plan(&os(&[PROBE, "--exact", "t"]), &env(), Some(cache.path())),
            Plan::Schedule {
                key: "test:demo:probe".into(),
                want: TestWant::Elastic(1),
            }
        );
        let no_pkg = TestEnv {
            pkg_name: None,
            ..env()
        };
        assert_eq!(
            plan(&os(&[PROBE]), &no_pkg, Some(cache.path())),
            Plan::Schedule {
                key: "test::probe".into(),
                want: TestWant::Elastic(1),
            }
        );
    }

    #[test]
    fn plan_uses_recorded_rss_as_the_floor() {
        let cache = tempfile::tempdir().unwrap();
        scheduler::write_test_weight(cache.path(), "test:demo:probe", 3 * 512 * 1024 * 1024);
        let want = |args: &[&str], env: &TestEnv| match plan(&os(args), env, Some(cache.path())) {
            Plan::Schedule { want, .. } => want,
            Plan::Exec => panic!("hashed binary must be scheduled"),
        };
        assert_eq!(want(&[PROBE], &env()), TestWant::Elastic(3));
        assert_eq!(
            want(&[PROBE, "--test-threads=2"], &env()),
            TestWant::Fixed(3),
            "the floor beats fewer declared threads"
        );
        assert_eq!(
            want(&[PROBE, "--test-threads=5"], &env()),
            TestWant::Fixed(5),
            "more declared threads beat the floor"
        );
        let per_test = TestEnv {
            nextest_mode: Some("process-per-test".into()),
            ..env()
        };
        assert_eq!(want(&[PROBE], &per_test), TestWant::Fixed(3));
    }

    #[test]
    fn plan_fixes_declared_and_per_test_wants() {
        let cache = tempfile::tempdir().unwrap();
        let want = |env: &TestEnv| match plan(&os(&[PROBE]), env, Some(cache.path())) {
            Plan::Schedule { want, .. } => want,
            Plan::Exec => panic!("hashed binary must be scheduled"),
        };
        let per_test = TestEnv {
            nextest_mode: Some("process-per-test".into()),
            rust_test_threads: Some("4".into()),
            ..env()
        };
        assert_eq!(want(&per_test), TestWant::Fixed(1));
        let threads = TestEnv {
            rust_test_threads: Some("4".into()),
            ..env()
        };
        assert_eq!(want(&threads), TestWant::Fixed(4));
        let other_mode = TestEnv {
            nextest_mode: Some("process-per-binary".into()),
            ..env()
        };
        assert_eq!(want(&other_mode), TestWant::Elastic(1));
    }

    #[test]
    fn wait_budget_is_shorter_under_nextest() {
        assert_eq!(wait_budget(&env()), scheduler::WAIT_TIMEOUT);
        let nextest = TestEnv {
            nextest_mode: Some("process-per-test".into()),
            ..env()
        };
        assert_eq!(wait_budget(&nextest), Duration::from_secs(120));
    }

    #[test]
    fn lease_env_sets_threads_only_for_elastic_wants() {
        let marker = Path::new("/cache/scheduler/tests/2");
        assert_eq!(
            lease_env(TestWant::Elastic(1), marker, 3),
            vec![
                (scheduler::TEST_LEASE_ENV, OsString::from(marker)),
                ("RUST_TEST_THREADS", OsString::from("3")),
            ]
        );
        assert_eq!(
            lease_env(TestWant::Fixed(3), marker, 3),
            vec![(scheduler::TEST_LEASE_ENV, OsString::from(marker))]
        );
    }

    #[test]
    fn exit_disposition_keeps_codes_and_signals() {
        assert_eq!(exit_disposition(Some(7), None), Disposition::Exit(7));
        assert_eq!(exit_disposition(Some(0), Some(9)), Disposition::Exit(0));
        assert_eq!(
            exit_disposition(None, Some(6)),
            Disposition::Signal {
                signal: 6,
                fallback: 134,
            }
        );
        assert_eq!(exit_disposition(None, None), Disposition::Exit(101));
    }

    #[test]
    fn finish_returns_the_exit_code() {
        assert_eq!(finish(Disposition::Exit(7)), 7);
        assert_eq!(finish(Disposition::Exit(0)), 0);
    }
}
