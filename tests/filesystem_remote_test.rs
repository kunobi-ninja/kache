//! Acceptance tests for the filesystem remote backend (kunobi-ninja/kache#414).
//!
//! The issue's headline criterion is a *cross-client* one: "two or more hosts
//! sharing one folder get cross-host cache hits with no S3 server", with "no
//! SQLite file and no hardlink/reflink ever touching the shared folder". Unit
//! tests over the transport cannot show that — they exercise one process's
//! view of the object store, not the property that a SECOND, independently
//! configured client can turn another client's upload into a hit.
//!
//! So these drive the real binary twice over two fully isolated caches that
//! share only the folder: separate `KACHE_CACHE_DIR`, separate config, and a
//! separate build directory, the way two machines mounting one NFS share
//! would be separate.

use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

fn rustc_path() -> String {
    std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string())
}

fn cc_available() -> bool {
    std::process::Command::new("cc")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn toml_path(path: &Path) -> String {
    toml::Value::String(path.to_string_lossy().into_owned()).to_string()
}

/// One independently configured runtime. It normally owns its local cache;
/// #811 coverage can instead point several runtimes at one node-local store.
struct Client {
    _runtime: TempDir,
    cache_dir: PathBuf,
    runtime_dir: PathBuf,
    config_path: PathBuf,
    command_seq: AtomicUsize,
}

impl Client {
    fn new(shared_folder: &Path) -> Self {
        let runtime = TempDir::new().unwrap();
        let cache_dir = runtime.path().to_path_buf();
        Self::with_runtime(shared_folder, cache_dir, runtime)
    }

    /// One CI job runtime backed by a node-local store shared with sibling
    /// jobs. Only the store/index live under `cache_dir`; daemon state and logs
    /// remain owned by this client's private temporary runtime.
    fn for_shared_store(shared_folder: &Path, cache_dir: &Path) -> Self {
        let runtime = TempDir::new().unwrap();
        Self::with_runtime(shared_folder, cache_dir.to_path_buf(), runtime)
    }

    /// A client that derives keys from recorded input closures. The config
    /// carries the flag rather than the environment because these clients set
    /// `ignore_env`.
    fn with_predictions(shared_folder: &Path) -> Self {
        let client = Self::new(shared_folder);
        let config = std::fs::read_to_string(&client.config_path).unwrap();
        std::fs::write(
            &client.config_path,
            config.replace("[cache]\n", "[cache]\ninput_predictions = true\n"),
        )
        .unwrap();
        client
    }

    fn with_runtime(shared_folder: &Path, cache_dir: PathBuf, runtime: TempDir) -> Self {
        let runtime_dir = runtime.path().to_path_buf();
        let config_path = if runtime_dir == cache_dir {
            isolated_config_path(&cache_dir)
        } else {
            runtime_dir.join("config.toml")
        };
        std::fs::write(
            &config_path,
            format!(
                "[cache]\n\
                 ignore_env = true\n\
                 local_store = {}\n\
                 runtime_dir = {}\n\
                 daemon_idle_timeout_secs = 60\n\
                 prefetch_enabled = false\n\n\
                 [cache.remote]\n\
                 type = \"filesystem\"\n\
                 path = {}\n\
                 prefix = \"artifacts\"\n",
                toml_path(&cache_dir),
                toml_path(&runtime_dir),
                toml_path(shared_folder),
            ),
        )
        .unwrap();
        Client {
            _runtime: runtime,
            cache_dir,
            runtime_dir,
            config_path,
            command_seq: AtomicUsize::new(0),
        }
    }

    fn kache(&self) -> std::process::Command {
        let mut cmd = std::process::Command::new(kache_binary());
        cmd.env("KACHE_CONFIG", &self.config_path)
            .env("KACHE_LOG", "off")
            // A BACKSTOP, not the cleanup mechanism: `Client`'s `Drop` stops
            // the daemon explicitly, which keeps it warm for the whole test
            // instead of tearing it down between commands. This only bounds
            // the damage if that stop is ever missed (a hard abort), since the
            // idle timeout is disabled by default (#662) and a leaked daemon
            // would otherwise outlive the suite. The timeout and all
            // file-backed settings are pinned in the ignore-env config above;
            // only operational vars need clearing here.
            .env_remove("KACHE_DISABLED")
            .env_remove("KACHE_NAMESPACE")
            .env_remove("KACHE_BASE_DIR")
            .env_remove("KACHE_SOCKET_PATH")
            .env_remove("RUSTC_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
            // These tests publish to a hermetic filesystem remote. CI runners
            // set GITHUB_ACTIONS/GITLAB_CI, which would force remote_readonly
            // and skip the upload the suite is proving.
            .env_remove("GITHUB_ACTIONS")
            .env_remove("GITLAB_CI")
            .env_remove("CI");
        cmd
    }

    /// Run a kache command with a deadline, capturing output through FILES
    /// rather than pipes.
    ///
    /// Two deliberate choices, both from kunobi-ninja/kache#704, where these
    /// tests hung the Windows job to its 45-minute limit:
    ///
    /// - **Files, not pipes.** `Command::output()` waits for EOF on the
    ///   child's pipes, and EOF only arrives once *every* process holding the
    ///   write end exits — including a background daemon that inherited them.
    ///   A file has no such semantics, so a lingering daemon cannot wedge the
    ///   test.
    /// - **A deadline.** If something still blocks, the test fails in seconds
    ///   naming the exact command, instead of burning the job's whole budget
    ///   and reporting only "timed out".
    fn run_within_at(&self, cwd: Option<&Path>, args: &[&str], deadline: Duration) -> Output {
        // Counter, not the args: an argv carries paths and slashes, which do
        // not belong in a file name.
        let seq = self.command_seq.fetch_add(1, Ordering::Relaxed);
        let out_path = self.runtime_dir.join(format!("cmd-{seq}.out"));
        let err_path = self.runtime_dir.join(format!("cmd-{seq}.err"));
        let stdout = std::fs::File::create(&out_path).expect("creating stdout capture");
        let stderr = std::fs::File::create(&err_path).expect("creating stderr capture");

        let mut command = self.kache();
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        let mut child = command
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .expect("failed to spawn kache");

        let start = Instant::now();
        let status = loop {
            match child.try_wait().expect("polling kache") {
                Some(status) => break status,
                None if start.elapsed() >= deadline => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!(
                        "`kache {}` did not finish within {deadline:?} (kunobi-ninja/kache#704).\n\
                         stdout: {}\nstderr: {}",
                        args.join(" "),
                        std::fs::read_to_string(&out_path).unwrap_or_default(),
                        std::fs::read_to_string(&err_path).unwrap_or_default(),
                    );
                }
                None => std::thread::sleep(Duration::from_millis(50)),
            }
        };

        Output {
            status,
            stdout: std::fs::read(&out_path).unwrap_or_default(),
            stderr: std::fs::read(&err_path).unwrap_or_default(),
        }
    }

    fn run_within(&self, args: &[&str], deadline: Duration) -> Output {
        self.run_within_at(None, args, deadline)
    }

    /// A kache command that should complete promptly.
    fn run(&self, args: &[&str]) -> Output {
        self.run_within(args, Duration::from_secs(90))
    }

    /// Compile a trivial rlib through kache-as-RUSTC_WRAPPER.
    fn compile(&self, src: &Path, out_dir: &Path) {
        let out_dir = out_dir.display().to_string();
        let src = src.display().to_string();
        let rustc = rustc_path();
        let output = self.run(&[
            &rustc,
            "--crate-name",
            "fsremote",
            "--crate-type",
            "lib",
            "--edition",
            "2021",
            "--emit=link",
            "--out-dir",
            &out_dir,
            &src,
        ]);
        assert!(
            output.status.success(),
            "kache rustc failed.\nstderr: {}",
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn compile_cc_checkout(&self, checkout: &Path, source: &str, output: &str) {
        let result = self.run_within_at(
            Some(checkout),
            &["cc", "-c", source, "-o", output, "-O0", "-g0"],
            Duration::from_secs(90),
        );
        assert!(
            result.status.success(),
            "kache cc failed.\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr),
        );
    }

    /// Compile `lib.rs` from an isolated checkout using only relative source
    /// arguments. This makes cross-checkout cache-key normalization part of the
    /// process-level remote reuse contract instead of reusing one source path.
    fn compile_checkout(&self, checkout: &Path, out_dir: &Path) {
        let out_dir = out_dir.display().to_string();
        let rustc = rustc_path();
        let output = self.run_within_at(
            Some(checkout),
            &[
                &rustc,
                "--crate-name",
                "fsremote",
                "--crate-type",
                "lib",
                "--edition",
                "2021",
                "--emit=link",
                "--out-dir",
                &out_dir,
                "lib.rs",
            ],
            Duration::from_secs(90),
        );
        assert!(
            output.status.success(),
            "kache rustc failed.\nstderr: {}",
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// Start this client's daemon and wait until it is ready. The remote
    /// pull path is daemon-mediated by design (`send_remote_check` no-ops
    /// when the socket is unreachable), so a cross-client hit needs one — as
    /// a real host would have, started by an earlier build or the installed
    /// service.
    fn start_daemon(&self) {
        let output = self.run(&["daemon", "start"]);
        assert!(
            output.status.success(),
            "kache daemon start failed.\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// Stop the daemon and wait for its drain phase to complete. Shutdown runs
    /// that phase before releasing its lifetime lock, giving explicit sync and
    /// cross-client reads a stable remote boundary under slow instrumentation.
    /// Unlike the Unix socket path, this works for Windows named pipes too.
    fn stop_daemon_and_wait(&self) {
        let output = self.run(&["daemon", "stop"]);
        assert!(
            output.status.success(),
            "kache daemon stop failed.\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        self.wait_for_daemon_exit(Duration::from_secs(45))
            .unwrap_or_else(|error| panic!("{error}"));
    }

    fn wait_for_daemon_exit(&self, timeout: Duration) -> Result<(), String> {
        let run_lock_path = self.runtime_dir.join("daemon.run.lock");
        let run_lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&run_lock_path)
            .map_err(|error| {
                format!(
                    "opening daemon lifetime lock probe {}: {error}",
                    run_lock_path.display()
                )
            })?;
        let deadline = Instant::now() + timeout;
        loop {
            match run_lock.try_lock() {
                Ok(()) => {
                    run_lock.unlock().map_err(|error| {
                        format!(
                            "releasing daemon lifetime lock probe {}: {error}",
                            run_lock_path.display()
                        )
                    })?;
                    return Ok(());
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    return Err(format!(
                        "failed to probe daemon lifetime lock {}: {error}",
                        run_lock_path.display()
                    ));
                }
                Err(error @ std::fs::TryLockError::WouldBlock) if Instant::now() >= deadline => {
                    return Err(format!(
                        "daemon lifetime lock {} remained held after its drain phase: {error}",
                        run_lock_path.display()
                    ));
                }
                Err(std::fs::TryLockError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn sync(&self, args: &[&str]) -> Output {
        let mut argv = vec!["sync"];
        argv.extend_from_slice(args);
        let output = self.run(&argv);
        assert!(
            output.status.success(),
            "kache sync {args:?} failed.\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr
                .lines()
                .any(|line| line.trim_end().ends_with("failed)")),
            "kache sync {args:?} reported failed transfers despite exiting successfully.\n\
             stdout: {}\nstderr: {stderr}",
            String::from_utf8_lossy(&output.stdout),
        );
        output
    }

    fn report(&self) -> serde_json::Value {
        let output = self.run(&["report", "--format", "json", "--since", "1h"]);
        assert!(output.status.success(), "kache report failed");
        serde_json::from_slice(&output.stdout).expect("report should be valid json")
    }

    /// Per-result event counts from `kache report` over this client's cache.
    fn results(&self) -> Vec<String> {
        self.report()["all_events"]
            .as_array()
            .map(|events| {
                events
                    .iter()
                    .filter_map(|e| e["result"].as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn crate_event<'a>(report: &'a serde_json::Value, crate_name: &str) -> &'a serde_json::Value {
    report["all_events"]
        .as_array()
        .and_then(|events| {
            events
                .iter()
                .rev()
                .find(|event| event["crate_name"] == crate_name)
        })
        .unwrap_or_else(|| panic!("report has no event for {crate_name}: {report}"))
}

fn wait_for_remote_entry(shared: &Path, crate_name: &str, cache_key: &str) {
    let manifest = shared
        .join("artifacts/v3/manifests")
        .join(crate_name)
        .join(format!("{cache_key}.json"));
    let pack = shared
        .join("artifacts/v3/packs")
        .join(crate_name)
        .join(format!("{cache_key}.tar.zst"));
    let deadline = Instant::now() + Duration::from_secs(45);

    loop {
        if manifest.is_file() && pack.is_file() {
            return;
        }
        if Instant::now() >= deadline {
            let mut remote_files = Vec::new();
            collect_files(shared, &mut remote_files);
            panic!(
                "daemon upload did not publish the v3 entry within 45s\n\
                 expected manifest: {}\nexpected pack: {}\nremote files: {remote_files:#?}",
                manifest.display(),
                pack.display(),
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Drop for Client {
    /// Stop this client's daemon when the test ends — including on panic.
    /// Waiting for its lifetime lock matters on Windows, where the daemon can
    /// retain handles into temporary directories during teardown.
    fn drop(&mut self) {
        // Bounded, and through the same file-backed runner: cleanup must never
        // be the thing that wedges a test run (#704). Cleanup failures must
        // not turn an existing test panic into a double panic.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self.run_within(&["daemon", "stop"], Duration::from_secs(30));
            // Upload workers may drain for up to 30s; leave overall shutdown
            // margin before the client's temporary runtime is removed.
            let _ = self.wait_for_daemon_exit(Duration::from_secs(45));
        }));
    }
}

/// kunobi-ninja/kache#414/#696 acceptance: two independently configured clients
/// sharing one folder — and nothing else — get a cross-client cache hit with
/// no S3 server or explicit sync anywhere. Client A publishes through the real
/// asynchronous daemon upload queue; client B exercises the pull-on-miss path.
///
/// Runs everywhere, including Windows, on purpose. An earlier form of this
/// file hung the Windows job to its 45-minute limit (kunobi-ninja/kache#704)
/// while taking ~3s on macOS because a background daemon inherited the
/// caller's pipe handles. Daemon spawning now prevents that leak; file-backed
/// capture and per-command deadlines remain defense in depth and make any
/// future hang fail with a bounded, specific diagnostic.
/// A predicted key must be allowed to ask the REMOTE before it is checked.
///
/// The soundness rule is that a prediction may never claim or store, not that
/// it may never read. An entry on the remote was stored under a key computed
/// from a discovered closure exactly as a local one was, so matching it proves
/// the prediction reproduced that closure.
///
/// Re-deriving before the remote check instead made the feature worthless for
/// the case it exists for: a checkout with an empty local store missed
/// locally on every unit and paid a full dep-info pre-pass before the warm
/// remote was ever asked. This pins that it no longer does.
#[test]
fn a_predicted_key_reaches_the_remote_without_a_pre_pass() {
    build_kache();

    let shared = TempDir::new().unwrap();
    let source = TempDir::new().unwrap();
    std::fs::write(
        source.path().join("lib.rs"),
        "pub fn answer() -> u32 { 42 }\n",
    )
    .unwrap();

    let client = Client::with_predictions(shared.path());
    let out = TempDir::new().unwrap();
    client.start_daemon();

    // Cold: compiles, records the closure, publishes to the folder.
    client.compile_checkout(source.path(), out.path());
    let cold = crate_event(&client.report(), "fsremote").clone();
    assert!(
        cold["result"] == "miss" || cold["result"] == "dup",
        "the first compile must be a real compile: {cold}"
    );
    assert_eq!(
        cold["dep_info_runs"], 1,
        "a cold compile has no record to derive from: {cold}"
    );
    let cache_key = cold["cache_key"].as_str().unwrap().to_owned();
    wait_for_remote_entry(shared.path(), "fsremote", &cache_key);

    // Drop the local artifact and keep the recorded closure. That is the
    // shape of a fresh checkout against a warm remote: the key can be
    // derived, and nothing is stored locally to serve it.
    // Delete the entry's blobs so the local store cannot serve it. The
    // recorded closure lives in the index and survives, which is exactly the
    // asymmetry a fresh checkout has once records are distributed: it can
    // derive the key and has no artifact behind it.
    let mut blobs = Vec::new();
    collect_files(&client.cache_dir.join("store/blobs"), &mut blobs);
    assert!(
        !blobs.is_empty(),
        "the cold compile should have stored blobs"
    );
    for blob in &blobs {
        std::fs::remove_file(blob).unwrap();
    }

    std::fs::remove_dir_all(out.path()).unwrap();
    std::fs::create_dir_all(out.path()).unwrap();
    client.compile_checkout(source.path(), out.path());
    let served = crate_event(&client.report(), "fsremote").clone();

    assert_eq!(
        served["cache_key"], cold["cache_key"],
        "the derived key must be the one the pre-pass produced: {served}"
    );
    assert!(
        served["result"] == "remote_hit" || served["result"] == "prefetch_hit",
        "the remote must serve it: {served}"
    );
    assert_eq!(
        served["dep_info_runs"], 0,
        "a predicted key that the remote can serve must spawn no pre-pass: {served}"
    );
    assert_eq!(
        served["compiler_runs"], 0,
        "nothing may recompile: {served}"
    );

    client.stop_daemon_and_wait();
}

/// A record that no longer describes the tree derives a key nothing was ever
/// stored under. The re-derivation that follows produces a key this build has
/// not looked up yet, and on a shared cache another machine may already hold
/// it — so the lookup phase runs a second pass rather than compiling.
///
/// Two clients, because one client cannot hold a stale record: its own
/// re-derivation refreshes the record before the next build sees it. Only a
/// second machine can publish the true key while the first still believes an
/// older closure.
#[test]
fn a_stale_prediction_still_finds_the_true_key_on_the_remote() {
    build_kache();

    let shared = TempDir::new().unwrap();
    let tree = TempDir::new().unwrap();
    let src = tree.path().join("lib.rs");
    std::fs::write(&src, "pub fn answer() -> u32 { 42 }\n").unwrap();

    // Client A records the closure of the ORIGINAL source and stops there.
    let alpha = Client::with_predictions(shared.path());
    let out_a = TempDir::new().unwrap();
    alpha.start_daemon();
    alpha.compile_checkout(tree.path(), out_a.path());
    let recorded = crate_event(&alpha.report(), "fsremote").clone();
    assert_eq!(recorded["dep_info_runs"], 1, "{recorded}");
    alpha.stop_daemon_and_wait();

    // The tree grows a module. A's record now names lib.rs alone and is stale.
    std::fs::write(tree.path().join("extra.rs"), "pub fn more() -> u32 { 1 }\n").unwrap();
    std::fs::write(
        &src,
        "mod extra;\npub fn answer() -> u32 { 42 + extra::more() }\n",
    )
    .unwrap();

    // Client B compiles the grown tree and publishes the TRUE key. B has its
    // own cache, so nothing about A's record changes.
    let beta = Client::new(shared.path());
    let out_b = TempDir::new().unwrap();
    beta.start_daemon();
    beta.compile_checkout(tree.path(), out_b.path());
    let published = crate_event(&beta.report(), "fsremote").clone();
    assert_ne!(
        published["cache_key"], recorded["cache_key"],
        "a bigger closure must key differently: {published}"
    );
    let true_key = published["cache_key"].as_str().unwrap().to_owned();
    wait_for_remote_entry(shared.path(), "fsremote", &true_key);
    beta.stop_daemon_and_wait();

    // A compiles the grown tree holding a record for the old one. The
    // prediction derives a key nothing ever stored; the re-derivation
    // produces the true key, which only the remote holds.
    alpha.start_daemon();
    alpha.compile_checkout(tree.path(), out_a.path());
    let served = crate_event(&alpha.report(), "fsremote").clone();
    assert_eq!(
        served["cache_key"], published["cache_key"],
        "the stale record must not decide the key: {served}"
    );
    assert_eq!(
        served["compiler_runs"], 0,
        "the remote holds the true key; a second pass must find it \
         instead of recompiling: {served}"
    );
    alpha.stop_daemon_and_wait();
}

#[test]
fn daemon_upload_reaches_an_independent_client_through_one_folder() {
    build_kache();

    let shared = TempDir::new().unwrap();
    let producer_source = TempDir::new().unwrap();
    let consumer_source = TempDir::new().unwrap();
    std::fs::write(
        producer_source.path().join("lib.rs"),
        "pub fn answer() -> u32 { 42 }\n",
    )
    .unwrap();
    std::fs::write(
        consumer_source.path().join("lib.rs"),
        "pub fn answer() -> u32 { 42 }\n",
    )
    .unwrap();

    // Client A compiles cold and publishes to the shared folder.
    let alpha = Client::new(shared.path());
    let out_a = TempDir::new().unwrap();
    alpha.start_daemon();
    alpha.compile_checkout(producer_source.path(), out_a.path());
    let alpha_report = alpha.report();
    let alpha_event = crate_event(&alpha_report, "fsremote");
    assert!(
        alpha_event["result"] == "miss" || alpha_event["result"] == "dup",
        "client A's first compile must be a real compile: {alpha_report}"
    );
    assert_eq!(
        alpha_event["compiler_runs"], 1,
        "the producer must compile instead of restoring: {alpha_report}"
    );
    let cache_key = alpha_event["cache_key"]
        .as_str()
        .expect("producer event should include its cache key")
        .to_owned();

    // The v3 manifest is the remote publication commit point. Polling both
    // objects proves the fire-and-forget wrapper request reached the daemon,
    // drained through its upload worker, and completed without a sync fallback.
    wait_for_remote_entry(shared.path(), "fsremote", &cache_key);
    alpha.stop_daemon_and_wait();

    // Client B has never seen this compile: separate cache, separate config,
    // separate output dir. Its only connection to A is the folder. Its daemon
    // is up, as a real host's would be — the remote pull path is
    // daemon-mediated by design.
    let beta = Client::new(shared.path());
    let out_b = TempDir::new().unwrap();
    beta.start_daemon();
    beta.compile_checkout(consumer_source.path(), out_b.path());
    let beta_report = beta.report();
    let beta_event = crate_event(&beta_report, "fsremote");

    assert_eq!(
        beta_event["result"], "remote_hit",
        "client B must pull A's artifact from the shared folder on miss: {beta_report}"
    );
    assert_eq!(
        beta_event["compiler_runs"], 0,
        "client B must restore without running rustc: {beta_report}"
    );
    assert_eq!(
        beta_event["cache_key"], cache_key,
        "producer and consumer checkouts must resolve to the same key"
    );
    assert!(
        out_b.path().join("libfsremote.rlib").is_file(),
        "client B must end up with the artifact materialized"
    );
    assert_eq!(
        std::fs::read(out_a.path().join("libfsremote.rlib")).unwrap(),
        std::fs::read(out_b.path().join("libfsremote.rlib")).unwrap(),
        "the remotely restored artifact must match the producer byte-for-byte"
    );
    beta.stop_daemon_and_wait();
}

/// C object compiles use the same v3 pack layout and daemon upload/check
/// path as rustc. Two clients sharing a folder get a remote hit without
/// an explicit sync.
#[test]
fn daemon_upload_reaches_an_independent_client_for_a_c_object() {
    if !cc_available() {
        eprintln!("skipping: no working `cc` on PATH");
        return;
    }
    build_kache();

    let shared = TempDir::new().unwrap();
    let producer_source = TempDir::new().unwrap();
    let consumer_source = TempDir::new().unwrap();
    let source = "int answer(void) { return 42; }\n";
    std::fs::write(producer_source.path().join("foo.c"), source).unwrap();
    std::fs::write(consumer_source.path().join("foo.c"), source).unwrap();

    let alpha = Client::new(shared.path());
    alpha.start_daemon();
    alpha.compile_cc_checkout(producer_source.path(), "foo.c", "foo.o");
    let alpha_report = alpha.report();
    let alpha_event = crate_event(&alpha_report, "foo.c");
    assert!(
        alpha_event["result"] == "miss" || alpha_event["result"] == "dup",
        "client A's first C compile must be a real compile: {alpha_report}"
    );
    assert_eq!(
        alpha_event["compiler_runs"], 1,
        "the producer must compile instead of restoring: {alpha_report}"
    );
    let cache_key = alpha_event["cache_key"]
        .as_str()
        .expect("producer event should include its cache key")
        .to_owned();

    wait_for_remote_entry(shared.path(), "foo.c", &cache_key);
    alpha.stop_daemon_and_wait();

    let beta = Client::new(shared.path());
    beta.start_daemon();
    beta.compile_cc_checkout(consumer_source.path(), "foo.c", "foo.o");
    let beta_report = beta.report();
    let beta_event = crate_event(&beta_report, "foo.c");

    assert_eq!(
        beta_event["result"], "remote_hit",
        "client B must pull A's C object from the shared folder on miss: {beta_report}"
    );
    assert_eq!(
        beta_event["compiler_runs"], 0,
        "client B must restore without running cc: {beta_report}"
    );
    assert_eq!(
        beta_event["cache_key"], cache_key,
        "producer and consumer C checkouts must resolve to the same key"
    );
    assert!(
        consumer_source.path().join("foo.o").is_file(),
        "client B must end up with the object materialized"
    );
    assert_eq!(
        std::fs::read(producer_source.path().join("foo.o")).unwrap(),
        std::fs::read(consumer_source.path().join("foo.o")).unwrap(),
        "the remotely restored object must match the producer byte-for-byte"
    );
    beta.stop_daemon_and_wait();
}

/// #811 acceptance: two job-scoped daemon runtimes may concurrently use one
/// node-local store. Restarting one daemon must reopen that same durable cache,
/// not turn the next identical compile into a miss.
#[test]
fn two_job_runtimes_share_one_store_across_daemon_restart() {
    build_kache();

    let remote = TempDir::new().unwrap();
    let node_cache = TempDir::new().unwrap();
    let sources = TempDir::new().unwrap();
    let src = sources.path().join("lib.rs");
    std::fs::write(&src, "pub fn node_cached() -> u32 { 811 }\n").unwrap();

    let alpha = Client::for_shared_store(remote.path(), node_cache.path());
    let beta = Client::for_shared_store(remote.path(), node_cache.path());
    assert_ne!(alpha.runtime_dir, beta.runtime_dir);
    assert_eq!(alpha.cache_dir, beta.cache_dir);

    alpha.start_daemon();
    beta.start_daemon();
    #[cfg(unix)]
    {
        assert!(alpha.runtime_dir.join("daemon.sock").exists());
        assert!(beta.runtime_dir.join("daemon.sock").exists());
    }

    let out_a = TempDir::new().unwrap();
    alpha.compile(&src, out_a.path());
    assert!(
        alpha
            .results()
            .iter()
            .any(|result| result == "miss" || result == "dup"),
        "the first job must populate the shared node store: {:?}",
        alpha.results()
    );

    let out_b = TempDir::new().unwrap();
    beta.compile(&src, out_b.path());
    assert!(
        beta.results().iter().any(|result| result == "local_hit"),
        "the second live daemon must consume the first job's entry: {:?}",
        beta.results()
    );

    beta.stop_daemon_and_wait();
    beta.start_daemon();
    let out_after_restart = TempDir::new().unwrap();
    beta.compile(&src, out_after_restart.path());
    let beta_results = beta.results();
    assert!(
        beta_results
            .iter()
            .filter(|result| result.as_str() == "local_hit")
            .count()
            >= 2,
        "a restarted daemon must replay the same shared cache: {beta_results:?}"
    );
    assert!(node_cache.path().join("store").is_dir());
    assert!(node_cache.path().join("index.db").is_file());
    assert!(!node_cache.path().join("daemon.sock").exists());
    assert!(!node_cache.path().join("events.jsonl").exists());
}

/// A stale SQLite row must never turn a missing node-local blob into a false
/// local hit. The wrapper falls through its ordinary miss path and the daemon
/// restores the already-published v3 object before reporting `remote_hit`.
#[test]
fn missing_shared_store_blob_falls_through_to_v3_restore() {
    build_kache();

    let remote = TempDir::new().unwrap();
    let node_cache = TempDir::new().unwrap();
    let sources = TempDir::new().unwrap();
    let src = sources.path().join("lib.rs");
    std::fs::write(&src, "pub fn restored() -> u32 { 3 }\n").unwrap();
    let client = Client::for_shared_store(remote.path(), node_cache.path());

    let first_out = TempDir::new().unwrap();
    client.compile(&src, first_out.path());
    client.stop_daemon_and_wait();
    client.sync(&["--push"]);

    let mut blobs = Vec::new();
    collect_files(&node_cache.path().join("store/blobs"), &mut blobs);
    let missing_blob = blobs
        .into_iter()
        .find(|path| path.is_file())
        .expect("cold compile must produce a local blob");
    let expected_digest = missing_blob
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    std::fs::remove_file(&missing_blob).unwrap();
    assert!(!missing_blob.exists());

    client.start_daemon();
    let restored_out = TempDir::new().unwrap();
    client.compile(&src, restored_out.path());
    let results = client.results();
    assert!(
        results.iter().any(|result| result == "remote_hit"),
        "missing local content must use the v3 restore path: {results:?}"
    );
    assert!(
        !results.iter().any(|result| result == "local_hit"),
        "the stale local row must never be reported as a hit: {results:?}"
    );
    assert!(missing_blob.is_file(), "v3 import must restore the blob");
    assert_eq!(
        blake3::hash(&std::fs::read(&missing_blob).unwrap())
            .to_hex()
            .as_str(),
        expected_digest
    );
    assert!(restored_out.path().join("libfsremote.rlib").is_file());
}

/// The same cross-client property through the **explicit sync** path, which
/// needs no daemon: `kache sync --pull --all` seeds a fresh client's local
/// store from the shared folder, and the next compile is a plain local hit
/// (kunobi-ninja/kache#414).
#[test]
fn a_fresh_client_can_seed_itself_from_the_shared_folder() {
    build_kache();

    let shared = TempDir::new().unwrap();
    let sources = TempDir::new().unwrap();
    let src = sources.path().join("lib.rs");
    std::fs::write(&src, "pub fn seeded() -> u32 { 11 }\n").unwrap();

    let alpha = Client::new(shared.path());
    let out_a = TempDir::new().unwrap();
    alpha.compile(&src, out_a.path());
    alpha.stop_daemon_and_wait();
    alpha.sync(&["--push"]);

    let beta = Client::new(shared.path());
    let out_b = TempDir::new().unwrap();
    beta.sync(&["--pull", "--all"]);
    beta.compile(&src, out_b.path());
    let beta_results = beta.results();

    assert!(
        beta_results.iter().any(|r| r == "local_hit"),
        "after --pull --all the artifact is local, so the compile is a local hit: {beta_results:?}"
    );
    assert!(out_b.path().join("libfsremote.rlib").is_file());
}

/// kunobi-ninja/kache#774: a remote object that vanished mid-run is a miss,
/// never an error.
///
/// A shared folder's retention policy (or a concurrent collector) may delete
/// a pack between the manifest read and the download. The build must
/// recompile and succeed instead of failing: the folder owner owns eviction,
/// and kache must tolerate its decisions without pausing writers.
#[test]
fn a_vanished_remote_pack_falls_back_to_recompile() {
    build_kache();

    let shared = TempDir::new().unwrap();
    let sources = TempDir::new().unwrap();
    let src = sources.path().join("lib.rs");
    std::fs::write(&src, "pub fn vanished() -> u32 { 13 }\n").unwrap();

    // Seed the shared folder, then simulate retention: manifests stay, every
    // pack for the crate goes.
    let alpha = Client::new(shared.path());
    let out_a = TempDir::new().unwrap();
    alpha.compile(&src, out_a.path());
    alpha.stop_daemon_and_wait();
    alpha.sync(&["--push"]);
    let artifacts = shared.path().join("artifacts");
    let mut all_files = Vec::new();
    collect_files(&artifacts, &mut all_files);
    let packs: Vec<_> = all_files
        .iter()
        .filter(|path| path.components().any(|c| c.as_os_str() == "packs"))
        .collect();
    assert!(
        !packs.is_empty(),
        "expected at least one published pack to delete under {}",
        artifacts.display()
    );
    for pack in &packs {
        std::fs::remove_file(pack).unwrap();
    }

    // The manifest still advertises the entry, but its bytes are gone. The
    // build must succeed by recompiling — recording a miss, not failing.
    // Beta's daemon is up, as a real host's would be: without it the remote
    // pull path is skipped entirely and the test would pass vacuously.
    let beta = Client::new(shared.path());
    let out_b = TempDir::new().unwrap();
    beta.start_daemon();
    beta.compile(&src, out_b.path());
    let beta_results = beta.results();
    assert!(
        beta_results.iter().any(|r| r == "miss"),
        "with the pack gone the compile is a miss, never an error: {beta_results:?}"
    );
    assert!(!beta_results.iter().any(|r| r == "error"));
    assert!(out_b.path().join("libfsremote.rlib").is_file());
    assert_eq!(
        std::fs::read(out_a.path().join("libfsremote.rlib")).unwrap(),
        std::fs::read(out_b.path().join("libfsremote.rlib")).unwrap(),
        "the recompiled artifact must match the original build byte-for-byte"
    );
}

/// kunobi-ninja/kache#704, the user-facing shape: a caller that captures
/// kache's output through PIPES — a build script, a CI wrapper, an IDE
/// integration — must not hang when the invocation auto-starts a daemon.
///
/// On Windows `CreateProcess` hands a child every inheritable handle, not
/// just the redirected stdio, so the daemon used to hold a duplicate of this
/// process's pipe write end and `output()` waited forever for an EOF that
/// could not arrive. The rest of this file captures through files precisely
/// to avoid that; this test deliberately does NOT, because the pipe is the
/// thing under test. It is bounded by a watchdog thread so a regression fails
/// in seconds rather than hanging the job.
#[test]
fn capturing_kache_output_through_pipes_does_not_hang() {
    build_kache();
    let shared = TempDir::new().unwrap();
    let client = Client::new(shared.path());
    let sources = TempDir::new().unwrap();
    let src = sources.path().join("lib.rs");
    std::fs::write(&src, "pub fn piped() -> u32 { 3 }\n").unwrap();
    let out_dir = TempDir::new().unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let mut command = client.kache();
    let src_path = src.display().to_string();
    let out_path = out_dir.path().display().to_string();
    let rustc = rustc_path();
    std::thread::spawn(move || {
        // Plain `output()`: pipes for both streams, exactly what an ordinary
        // caller writes.
        let result = command
            .args([
                &rustc,
                "--crate-name",
                "piped",
                "--crate-type",
                "lib",
                "--edition",
                "2021",
                "--emit=link",
                "--out-dir",
                &out_path,
                &src_path,
            ])
            .output();
        let _ = tx.send(result.map(|o| o.status.success()));
    });

    match rx.recv_timeout(Duration::from_secs(120)) {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => panic!("the piped invocation failed"),
        Ok(Err(e)) => panic!("could not run kache: {e}"),
        Err(_) => panic!(
            "a piped `Command::output()` around kache did not return within 120s — \
             the auto-started daemon is holding the caller's pipe open \
             (kunobi-ninja/kache#704)"
        ),
    }
}

/// Cleanup by `Drop` must actually work: after a client goes out of scope its
/// daemon is gone, not merely asked to leave. Without this the suite would
/// leak a daemon per test, which is what the 3-second idle timeouts elsewhere
/// were compensating for (kunobi-ninja/kache#704).
#[test]
fn dropping_a_client_stops_its_daemon() {
    build_kache();
    let shared = TempDir::new().unwrap();

    let run_lock = {
        let client = Client::new(shared.path());
        client.start_daemon();
        let run_lock_path = client.runtime_dir.join("daemon.run.lock");
        let run_lock = std::fs::OpenOptions::new()
            .write(true)
            .open(&run_lock_path)
            .expect("opening the live daemon's lifetime lock");
        assert!(
            matches!(run_lock.try_lock(), Err(std::fs::TryLockError::WouldBlock)),
            "the running daemon must hold {}",
            run_lock_path.display()
        );
        run_lock
        // `client` drops here — its Drop stops the daemon and waits for this
        // same lifetime lock to become available.
    };

    run_lock
        .try_lock()
        .expect("Client::drop must wait until the daemon releases its lifetime lock");
    run_lock
        .unlock()
        .expect("releasing the post-drop lifetime lock probe");
}

/// The shared folder must stay a pure object store: no SQLite index and no
/// shared inodes, which is what lets it live on NFS/SMB at all (the two
/// reasons #414 gives for why pointing `KACHE_CACHE_DIR` at a share fails).
#[test]
fn the_shared_folder_holds_objects_only() {
    build_kache();

    let shared = TempDir::new().unwrap();
    let sources = TempDir::new().unwrap();
    let src = sources.path().join("lib.rs");
    std::fs::write(&src, "pub fn only_objects() -> u32 { 7 }\n").unwrap();

    let client = Client::new(shared.path());
    let out = TempDir::new().unwrap();
    client.compile(&src, out.path());
    client.stop_daemon_and_wait();
    client.sync(&["--push"]);

    let mut files = Vec::new();
    collect_files(shared.path(), &mut files);
    assert!(
        !files.is_empty(),
        "the push should have written objects to the shared folder"
    );
    for file in &files {
        let name = file.file_name().unwrap_or_default().to_string_lossy();
        assert!(
            !name.contains("index.db"),
            "a SQLite index must never live on the shared folder: {}",
            file.display()
        );
        assert!(
            !name.ends_with("-wal") && !name.ends_with("-shm"),
            "SQLite WAL sidecars must never live on the shared folder: {}",
            file.display()
        );

        // Nothing on the share may be an inode shared with the local store:
        // hardlinks are exactly what does not work across a network mount.
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let meta = std::fs::metadata(file).unwrap();
            assert_eq!(
                meta.nlink(),
                1,
                "shared-folder object must not be a hardlink: {}",
                file.display()
            );
        }
    }
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path);
        }
    }
}
