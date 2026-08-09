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

    /// Run a kache command to completion, with stdio pipes that a spawned
    /// daemon cannot hold open: the daemon inherits handles, so an inherited
    /// stdout would keep `output()` waiting for an EOF that never comes.
    fn run(&self, args: &[&str]) -> std::process::Output {
        self.kache()
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("failed to run kache")
    }

    /// Compile a trivial rlib through kache-as-RUSTC_WRAPPER.
    fn compile(&self, src: &Path, out_dir: &Path) {
        let output = self
            .kache()
            .stdin(std::process::Stdio::null())
            .args([
                rustc_path(),
                "--crate-name".into(),
                "fsremote".into(),
                "--crate-type".into(),
                "lib".into(),
                "--edition".into(),
                "2021".into(),
                "--emit=link".into(),
                "--out-dir".into(),
                out_dir.display().to_string(),
                src.display().to_string(),
            ])
            .output()
            .expect("failed to run kache rustc");
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
        let _ = self.kache().args(["daemon", "stop"]).output();
    }
}

/// kunobi-ninja/kache#414 acceptance: two independently configured clients
/// sharing one folder — and nothing else — get a cross-client cache hit with
/// no S3 server anywhere. Exercises the **pull-on-miss** path: client B never
/// syncs explicitly, it just compiles and its daemon fetches A's artifact.
///
/// Unix-only, and deliberately so: this is the suite's only test that drives
/// `kache daemon start`, and doing that under the Windows named-pipe
/// lifecycle hung the workspace test job to its 45-minute limit. The
/// cross-client contract itself is not platform-specific and is covered on
/// every platform by the daemon-free sync test below; what is unproven is
/// starting and stopping a daemon from inside the Windows test harness,
/// which is worth its own investigation rather than a hung CI job.
#[cfg(unix)]
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
