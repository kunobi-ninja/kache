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
                "[cache.remote]\n\
                 type = \"filesystem\"\n\
                 path = \"{}\"\n\
                 prefix = \"artifacts\"\n",
                shared_folder.display().to_string().replace('\\', "\\\\"),
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
        cmd.env("KACHE_CACHE_DIR", &self.cache_dir)
            .env("KACHE_RUNTIME_DIR", &self.runtime_dir)
            .env("KACHE_CONFIG", &self.config_path)
            .env("KACHE_LOG", "off")
            // A BACKSTOP, not the cleanup mechanism: `Client`'s `Drop` stops
            // the daemon explicitly, which keeps it warm for the whole test
            // instead of tearing it down between commands. This only bounds
            // the damage if that stop is ever missed (a hard abort), since the
            // idle timeout is disabled by default (#662) and a leaked daemon
            // would otherwise outlive the suite.
            .env("KACHE_DAEMON_IDLE_TIMEOUT", "60")
            .env_remove("KACHE_SOCKET_PATH")
            .env_remove("RUSTC_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WRAPPER");
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
    fn run_within(&self, args: &[&str], deadline: Duration) -> Output {
        // Counter, not the args: an argv carries paths and slashes, which do
        // not belong in a file name.
        let seq = self.command_seq.fetch_add(1, Ordering::Relaxed);
        let out_path = self.runtime_dir.join(format!("cmd-{seq}.out"));
        let err_path = self.runtime_dir.join(format!("cmd-{seq}.err"));
        let stdout = std::fs::File::create(&out_path).expect("creating stdout capture");
        let stderr = std::fs::File::create(&err_path).expect("creating stderr capture");

        let mut child = self
            .kache()
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

        let run_lock_path = self.runtime_dir.join("daemon.run.lock");
        let run_lock = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&run_lock_path)
            .expect("opening daemon run lock probe");
        // Upload workers may drain for up to 30s; leave overall shutdown margin.
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            match run_lock.try_lock() {
                Ok(()) => {
                    run_lock.unlock().expect("releasing daemon run lock probe");
                    return;
                }
                Err(std::fs::TryLockError::Error(error)) => {
                    panic!(
                        "failed to probe daemon lifetime lock {}: {error}",
                        run_lock_path.display()
                    );
                }
                Err(error @ std::fs::TryLockError::WouldBlock) if Instant::now() >= deadline => {
                    panic!(
                        "daemon lifetime lock {} remained held after its drain phase: {error}",
                        run_lock_path.display()
                    );
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

    /// Per-result event counts from `kache report` over this client's cache.
    fn results(&self) -> Vec<String> {
        let output = self.run(&["report", "--format", "json", "--since", "1h"]);
        assert!(output.status.success(), "kache report failed");
        let report: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("report should be valid json");
        report["all_events"]
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

impl Drop for Client {
    /// Stop this client's daemon when the test ends — including on panic.
    /// Nothing waits on the daemon's exit: `stop` is a request, and the test
    /// has no reason to block on the shutdown completing. This is what keeps
    /// daemons from piling up across the suite without making them die
    /// between commands.
    fn drop(&mut self) {
        // Bounded, and through the same file-backed runner: cleanup must
        // never be the thing that wedges a test run (#704). Nothing waits on
        // the daemon's own exit — `stop` is a request.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.run_within(&["daemon", "stop"], Duration::from_secs(30))
        }));
    }
}

/// kunobi-ninja/kache#414 acceptance: two independently configured clients
/// sharing one folder — and nothing else — get a cross-client cache hit with
/// no S3 server anywhere. Exercises the **pull-on-miss** path: client B never
/// syncs explicitly, it just compiles and its daemon fetches A's artifact.
///
/// Runs everywhere, including Windows, on purpose. An earlier form of this
/// file hung the Windows job to its 45-minute limit (kunobi-ninja/kache#704)
/// while taking ~3s on macOS because a background daemon inherited the
/// caller's pipe handles. Daemon spawning now prevents that leak; file-backed
/// capture and per-command deadlines remain defense in depth and make any
/// future hang fail with a bounded, specific diagnostic.
#[test]
fn two_independent_clients_share_hits_through_one_folder() {
    build_kache();

    let shared = TempDir::new().unwrap();
    let sources = TempDir::new().unwrap();
    let src = sources.path().join("lib.rs");
    std::fs::write(&src, "pub fn answer() -> u32 { 42 }\n").unwrap();

    // Client A compiles cold and publishes to the shared folder.
    let alpha = Client::new(shared.path());
    let out_a = TempDir::new().unwrap();
    alpha.compile(&src, out_a.path());
    assert!(
        alpha.results().iter().any(|r| r == "miss" || r == "dup"),
        "client A's first compile must be a real compile: {:?}",
        alpha.results()
    );
    alpha.stop_daemon_and_wait();
    let push = alpha.sync(&["--push"]);

    let manifests = shared.path().join("artifacts/v3/manifests");
    assert!(
        manifests.is_dir(),
        "the push must have written the v3 object layout into the shared folder.\n\
         stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&push.stdout),
        String::from_utf8_lossy(&push.stderr),
    );

    // Client B has never seen this compile: separate cache, separate config,
    // separate output dir. Its only connection to A is the folder. Its daemon
    // is up, as a real host's would be — the remote pull path is
    // daemon-mediated by design.
    let beta = Client::new(shared.path());
    let out_b = TempDir::new().unwrap();
    beta.start_daemon();
    beta.compile(&src, out_b.path());
    let beta_results = beta.results();

    assert!(
        beta_results.iter().any(|r| r == "remote_hit"),
        "client B must pull A's artifact from the shared folder on miss: {beta_results:?}"
    );
    assert!(
        out_b.path().join("libfsremote.rlib").is_file(),
        "client B must end up with the artifact materialized"
    );
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
    let cache_dir = client.cache_dir.clone();
    let runtime_dir = client.runtime_dir.clone();
    let config_path = client.config_path.clone();
    let src_path = src.display().to_string();
    let out_path = out_dir.path().display().to_string();
    let rustc = rustc_path();
    std::thread::spawn(move || {
        // Plain `output()`: pipes for both streams, exactly what an ordinary
        // caller writes.
        let result = std::process::Command::new(kache_binary())
            .env("KACHE_CACHE_DIR", &cache_dir)
            .env("KACHE_RUNTIME_DIR", &runtime_dir)
            .env("KACHE_CONFIG", &config_path)
            .env("KACHE_LOG", "off")
            .env("KACHE_DAEMON_IDLE_TIMEOUT", "60")
            .env_remove("KACHE_SOCKET_PATH")
            .env_remove("RUSTC_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
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
#[cfg(unix)]
#[test]
fn dropping_a_client_stops_its_daemon() {
    build_kache();
    let shared = TempDir::new().unwrap();

    let socket = {
        let client = Client::new(shared.path());
        client.start_daemon();
        let socket = client.runtime_dir.join("daemon.sock");
        assert!(
            socket.exists(),
            "the daemon should have published its socket at {}",
            socket.display()
        );
        socket
        // `client` drops here — its Drop issues `daemon stop`.
    };

    // The socket is removed on shutdown; allow a moment for the daemon to
    // finish unlinking it, then assert it is really gone.
    for _ in 0..50 {
        if !socket.exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "daemon socket {} still present after the client was dropped — \
         Drop cleanup did not stop the daemon",
        socket.display()
    );
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
