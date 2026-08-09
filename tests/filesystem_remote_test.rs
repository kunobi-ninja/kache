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

/// One independently configured client: its own local cache and config, both
/// pointing at the shared folder as a filesystem remote.
struct Client {
    _cache: TempDir,
    cache_dir: PathBuf,
    command_seq: AtomicUsize,
}

impl Client {
    fn new(shared_folder: &Path) -> Self {
        let cache = TempDir::new().unwrap();
        let cache_dir = cache.path().to_path_buf();
        std::fs::write(
            isolated_config_path(&cache_dir),
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
            _cache: cache,
            cache_dir,
            command_seq: AtomicUsize::new(0),
        }
    }

    fn kache(&self) -> std::process::Command {
        let mut cmd = std::process::Command::new(kache_binary());
        cmd.env("KACHE_CACHE_DIR", &self.cache_dir)
            .env("KACHE_CONFIG", isolated_config_path(&self.cache_dir))
            .env("KACHE_LOG", "off")
            // A BACKSTOP, not the cleanup mechanism: `Client`'s `Drop` stops
            // the daemon explicitly, which keeps it warm for the whole test
            // instead of tearing it down between commands. This only bounds
            // the damage if that stop is ever missed (a hard abort), since the
            // idle timeout is disabled by default (#662) and a leaked daemon
            // would otherwise outlive the suite.
            .env("KACHE_DAEMON_IDLE_TIMEOUT", "60")
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
        let out_path = self.cache_dir.join(format!("cmd-{seq}.out"));
        let err_path = self.cache_dir.join(format!("cmd-{seq}.err"));
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

    fn sync(&self, args: &[&str]) {
        let mut argv = vec!["sync"];
        argv.extend_from_slice(args);
        let output = self.run(&argv);
        assert!(
            output.status.success(),
            "kache sync {args:?} failed.\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
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
/// while taking ~3s on macOS; the cause is still unknown, and the two
/// candidate mechanisms are now removed rather than avoided — output is
/// captured through files instead of pipes a lingering daemon could hold
/// open, and every command carries a deadline. If it hangs again, CI now
/// says which command and dies in seconds instead of burning the job.
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
    alpha.sync(&["--push"]);

    let manifests = shared.path().join("artifacts/v3/manifests");
    assert!(
        manifests.is_dir(),
        "the push must have written the v3 object layout into the shared folder"
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
        let socket = client.cache_dir.join("daemon.sock");
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
