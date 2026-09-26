use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::args::RustcArgs;
use crate::identity;
use kache_core::BuildIntent;

struct WorkspaceDiscovery {
    crate_names: Vec<String>,
    workspace_root: Option<PathBuf>,
    lock_path: Option<PathBuf>,
}

/// How long discovery may spend in `cargo metadata`, across every candidate
/// manifest. The wrapper that holds the prefetch lock runs discovery before
/// its own compile, so Cargo waits on it (kunobi-ninja/kache#698).
const METADATA_BUDGET: Duration = Duration::from_secs(3);

/// The most `cargo metadata --no-deps` output kept. A workspace lists its
/// own members only, so real output is far smaller; past this the run fails.
const METADATA_OUTPUT_CAP: usize = 8 << 20;

/// Why workspace discovery produced nothing.
#[derive(Debug, PartialEq, Eq)]
enum DiscoveryFailure {
    /// A `Cargo.lock` was found but lists no packages or cannot be parsed.
    UnusableLock,
    /// `cargo` could not be started.
    Spawn,
    /// The budget ran out; the child's process group was killed.
    Timeout,
    /// `cargo metadata` exited unsuccessfully.
    Exit,
    /// The output passed [`METADATA_OUTPUT_CAP`].
    TooLarge,
    /// The output was not the expected JSON, or named no packages.
    Parse,
}

pub fn discover(args: Option<&RustcArgs>) -> Option<BuildIntent> {
    let manifest_dir = std::env::var_os("CARGO_MANIFEST_DIR").map(PathBuf::from);
    let cwd = std::env::current_dir().ok();
    discover_with_context(args, manifest_dir.as_deref(), cwd.as_deref())
}

fn discover_with_context(
    args: Option<&RustcArgs>,
    manifest_dir: Option<&Path>,
    cwd: Option<&Path>,
) -> Option<BuildIntent> {
    let discovery = match discover_workspace(
        OsStr::new("cargo"),
        Instant::now() + METADATA_BUDGET,
        args,
        manifest_dir,
        cwd,
    ) {
        Ok(discovery) => discovery,
        Err(failure) => {
            tracing::debug!("build intent: workspace discovery failed: {failure:?}");
            return None;
        }
    };
    let crate_names = discovery.crate_names;
    if crate_names.is_empty() {
        return None;
    }

    let namespace = std::env::var("KACHE_NAMESPACE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let lock_path = discovery.lock_path.clone().unwrap_or_else(|| {
        discovery
            .workspace_root
            .as_deref()
            .map(|root| root.join("Cargo.lock"))
            .unwrap_or_else(|| PathBuf::from("Cargo.lock"))
    });
    let cargo_lock_deps = namespace
        .as_ref()
        .and_then(|_| load_cargo_lock_deps(&lock_path))
        .unwrap_or_default();

    let identity_key = args.and_then(|args| {
        identity::identity_key(
            &lock_path,
            &identity::target_from_rustc_args(args),
            &identity::profile_from_rustc_args(args),
        )
    });

    Some(BuildIntent {
        crate_names,
        namespace,
        cargo_lock_deps,
        identity_key,
    })
}

pub fn into_build_started_request(
    intent: BuildIntent,
    client_epoch: u64,
    session_id: String,
) -> crate::daemon::BuildStartedRequest {
    crate::daemon::BuildStartedRequest {
        intent,
        client_epoch,
        session_id,
    }
}

fn load_cargo_lock_deps(lock_path: &Path) -> Option<Vec<(String, String)>> {
    crate::shards::parse_cargo_lock(lock_path)
        .map_err(|err| {
            tracing::debug!(
                "build intent: failed to parse {} for shard prefetch: {}",
                lock_path.display(),
                err
            );
            err
        })
        .ok()
}

/// The workspace this compile belongs to: from `Cargo.lock` when one is
/// found, else from `cargo` (the program named by `cargo`) run against each
/// candidate manifest in turn, all before `deadline`.
fn discover_workspace(
    cargo: &OsStr,
    deadline: Instant,
    args: Option<&RustcArgs>,
    manifest_dir: Option<&Path>,
    cwd: Option<&Path>,
) -> Result<WorkspaceDiscovery, DiscoveryFailure> {
    let lock_path = find_lock_path(args, manifest_dir, cwd);
    if let Some(lock_path) = lock_path.as_ref() {
        let crate_names = crate_names_from_lock(lock_path).ok_or(DiscoveryFailure::UnusableLock)?;
        let workspace_root = lock_path.parent().map(Path::to_path_buf);
        return Ok(WorkspaceDiscovery {
            crate_names,
            workspace_root,
            lock_path: Some(lock_path.clone()),
        });
    }

    for manifest_path in candidate_manifest_paths(args, manifest_dir, cwd) {
        match run_cargo_metadata(cargo, Some(&manifest_path), deadline) {
            Ok(discovery) => return Ok(discovery),
            // The budget is shared: nothing is left for the next candidate.
            Err(DiscoveryFailure::Timeout) => return Err(DiscoveryFailure::Timeout),
            Err(_) => {}
        }
    }

    run_cargo_metadata(cargo, None, deadline)
}

fn crate_names_from_lock(lock_path: &Path) -> Option<Vec<String>> {
    let deps = load_cargo_lock_deps(lock_path)?;
    let mut names = Vec::new();
    for (name, _) in deps {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    if names.is_empty() { None } else { Some(names) }
}

fn find_lock_path(
    args: Option<&RustcArgs>,
    manifest_dir: Option<&Path>,
    cwd: Option<&Path>,
) -> Option<PathBuf> {
    for start in candidate_roots(args, manifest_dir, cwd) {
        for ancestor in start.ancestors() {
            let lock = ancestor.join("Cargo.lock");
            if lock.is_file() {
                return Some(lock);
            }
        }
    }
    None
}

fn candidate_roots(
    args: Option<&RustcArgs>,
    manifest_dir: Option<&Path>,
    cwd: Option<&Path>,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let (Some(args), Some(cwd)) = (args, cwd)
        && let Some(root) = args.verified_workspace_root(cwd)
    {
        push_unique(&mut roots, root);
    }
    if let Some(manifest_dir) = manifest_dir {
        push_unique(&mut roots, manifest_dir.to_path_buf());
    }
    if let Some(cwd) = cwd {
        push_unique(&mut roots, cwd.to_path_buf());
    }
    roots
}

fn push_unique(roots: &mut Vec<PathBuf>, path: PathBuf) {
    if !roots.iter().any(|existing| existing == &path) {
        roots.push(path);
    }
}

fn candidate_manifest_paths(
    args: Option<&RustcArgs>,
    manifest_dir: Option<&Path>,
    cwd: Option<&Path>,
) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for root in candidate_roots(args, manifest_dir, cwd) {
        let path = root.join("Cargo.toml");
        if path.is_file() && !candidates.iter().any(|existing| existing == &path) {
            candidates.push(path);
        }
    }
    candidates
}

fn run_cargo_metadata(
    cargo: &OsStr,
    manifest_path: Option<&Path>,
    deadline: Instant,
) -> Result<WorkspaceDiscovery, DiscoveryFailure> {
    let mut command = std::process::Command::new(cargo);
    command
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());

    if let Some(path) = manifest_path {
        command.arg("--manifest-path").arg(path);
    }

    let stdout = run_bounded(command, deadline, METADATA_OUTPUT_CAP)?;
    parse_metadata_packages(&stdout).ok_or(DiscoveryFailure::Parse)
}

/// What the two watcher threads of [`run_bounded`] report.
enum ChildEvent {
    /// All of stdout, and whether it passed the cap.
    Output(Vec<u8>, bool),
    Exit(Option<std::process::ExitStatus>),
}

/// Whether `more` bytes still fit after `kept` under `cap`.
fn fits_output_cap(kept: usize, more: usize, cap: usize) -> bool {
    kept.checked_add(more).is_some_and(|total| total <= cap)
}

/// Run `command` in its own process group and return its stdout, or kill the
/// group once `deadline` passes. Stdout is read to the end even past `cap`,
/// so the child never blocks on a full pipe; only the first `cap` bytes are
/// kept, and a run that passed it fails.
fn run_bounded(
    mut command: std::process::Command,
    deadline: Instant,
    cap: usize,
) -> Result<Vec<u8>, DiscoveryFailure> {
    use std::io::Read;

    crate::platform::configure_process_group(&mut command);
    let mut child = command.spawn().map_err(|_| DiscoveryFailure::Spawn)?;
    let pid = child.id();
    let Some(mut stdout) = child.stdout.take() else {
        crate::platform::kill_process_group(pid);
        let _ = child.wait();
        return Err(DiscoveryFailure::Spawn);
    };

    let (tx, rx) = std::sync::mpsc::channel();
    let tx_output = tx.clone();
    std::thread::spawn(move || {
        let mut kept = Vec::new();
        let mut overflowed = false;
        let mut chunk = [0u8; 8192];
        loop {
            match stdout.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) if fits_output_cap(kept.len(), n, cap) => {
                    kept.extend_from_slice(&chunk[..n]);
                }
                Ok(_) => overflowed = true,
            }
        }
        let _ = tx_output.send(ChildEvent::Output(kept, overflowed));
    });
    std::thread::spawn(move || {
        let _ = tx.send(ChildEvent::Exit(child.wait().ok()));
    });

    let mut output = None;
    let mut exit = None;
    while output.is_none() || exit.is_none() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(ChildEvent::Output(kept, overflowed)) => output = Some((kept, overflowed)),
            Ok(ChildEvent::Exit(status)) => exit = Some(status),
            Err(_) => break,
        }
    }

    match (output, exit) {
        (Some((_, true)), Some(_)) => Err(DiscoveryFailure::TooLarge),
        (Some((kept, false)), Some(Some(status))) if status.success() => Ok(kept),
        (Some(_), Some(_)) => Err(DiscoveryFailure::Exit),
        _ => {
            crate::platform::kill_process_group(pid);
            Err(DiscoveryFailure::Timeout)
        }
    }
}

fn parse_metadata_packages(metadata_json: &[u8]) -> Option<WorkspaceDiscovery> {
    let metadata: serde_json::Value = serde_json::from_slice(metadata_json).ok()?;
    let workspace_root = metadata
        .get("workspace_root")
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from);
    let crate_names = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .map(|packages| {
            let mut names = Vec::new();
            for package in packages {
                if let Some(name) = package.get("name").and_then(serde_json::Value::as_str)
                    && !names.iter().any(|existing| existing == name)
                {
                    names.push(name.to_string());
                }
            }
            names
        })
        .unwrap_or_default();
    if crate_names.is_empty() {
        return None;
    }
    let lock_path = workspace_root.as_ref().map(|root| root.join("Cargo.lock"));
    Some(WorkspaceDiscovery {
        crate_names,
        workspace_root,
        lock_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_lock(root: &Path, packages: &[(&str, &str)]) {
        let mut body = String::from("version = 3\n");
        for (name, version) in packages {
            body.push_str(&format!(
                "\n[[package]]\nname = \"{name}\"\nversion = \"{version}\"\n"
            ));
        }
        std::fs::write(root.join("Cargo.lock"), body).unwrap();
    }

    /// Build a two-member cargo workspace under a temp dir and return its root.
    fn scaffold_workspace() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = [\"app\", \"dep\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        std::fs::create_dir_all(root.join("app/src")).unwrap();
        std::fs::write(
            root.join("app/Cargo.toml"),
            "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n\
             [dependencies]\ndep = { path = \"../dep\" }\n",
        )
        .unwrap();
        std::fs::write(root.join("app/src/lib.rs"), "").unwrap();
        std::fs::create_dir_all(root.join("dep/src")).unwrap();
        std::fs::write(
            root.join("dep/Cargo.toml"),
            "[package]\nname = \"dep\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::write(root.join("dep/src/lib.rs"), "").unwrap();
        dir
    }

    #[test]
    fn candidate_manifests_keep_each_distinct_existing_manifest() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let missing = tempfile::tempdir().unwrap();
        std::fs::write(first.path().join("Cargo.toml"), "[workspace]\n").unwrap();
        std::fs::write(second.path().join("Cargo.toml"), "[workspace]\n").unwrap();

        assert_eq!(
            candidate_manifest_paths(None, Some(first.path()), Some(second.path()),),
            vec![
                first.path().join("Cargo.toml"),
                second.path().join("Cargo.toml")
            ]
        );
        assert!(candidate_manifest_paths(None, Some(missing.path()), None).is_empty());
    }

    #[test]
    fn discover_builds_intent_from_lockfile_without_cargo_metadata() {
        let ws = scaffold_workspace();
        let root = ws.path();
        write_lock(root, &[("app", "0.1.0"), ("dep", "0.1.0")]);
        let out_dir = root.join("target/debug/deps");
        std::fs::create_dir_all(&out_dir).unwrap();

        let args = RustcArgs::parse(&[
            "rustc".to_string(),
            "--out-dir".to_string(),
            out_dir.to_string_lossy().into_owned(),
        ])
        .unwrap();

        let intent = discover_with_context(Some(&args), Some(root), Some(root))
            .expect("discover should resolve the workspace");
        assert!(intent.crate_names.contains(&"app".to_string()));
        assert!(intent.crate_names.contains(&"dep".to_string()));
        assert!(intent.namespace.is_none());
        let key = intent
            .identity_key
            .expect("lockfile yields an identity key");
        assert!(key.starts_with("id/"), "{key}");
        assert!(key.ends_with("/debug"), "{key}");
    }

    #[test]
    fn discover_entrypoint_never_returns_an_empty_default_intent() {
        let intent = discover(None).expect("the Kache workspace should be discoverable");
        assert!(
            intent.crate_names.iter().any(|name| name == "kache"),
            "expected the current workspace packages, got {:?}",
            intent.crate_names
        );
    }

    #[test]
    fn discover_omits_unrelated_workspace_members_absent_from_the_lock() {
        let ws = scaffold_workspace();
        let root = ws.path();
        std::fs::create_dir_all(root.join("unrelated/src")).unwrap();
        std::fs::write(
            root.join("unrelated/Cargo.toml"),
            "[package]\nname = \"unrelated\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::write(root.join("unrelated/src/lib.rs"), "").unwrap();
        write_lock(root, &[("app", "0.1.0"), ("dep", "0.1.0")]);

        let out_dir = root.join("target/debug/deps");
        std::fs::create_dir_all(&out_dir).unwrap();
        let args = RustcArgs::parse(&[
            "rustc".to_string(),
            "--out-dir".to_string(),
            out_dir.to_string_lossy().into_owned(),
        ])
        .unwrap();

        let intent = discover_with_context(Some(&args), Some(root), Some(root)).unwrap();
        assert!(!intent.crate_names.contains(&"unrelated".to_string()));
    }

    #[test]
    fn parse_metadata_packages_reads_workspace_root_and_names() {
        let json = br#"{
            "workspace_root": "/ws",
            "packages": [{"name": "app"}, {"name": "dep"}, {"name": "app"}]
        }"#;
        let discovery = parse_metadata_packages(json).unwrap();
        assert_eq!(discovery.crate_names, vec!["app", "dep"]);
        assert_eq!(discovery.workspace_root.as_deref(), Some(Path::new("/ws")));
    }

    #[test]
    fn parse_metadata_packages_rejects_invalid_json() {
        assert!(parse_metadata_packages(b"not json at all").is_none());
    }

    #[test]
    fn load_cargo_lock_deps_parses_a_valid_lockfile() {
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(
            &lock,
            "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        let deps = load_cargo_lock_deps(&lock).expect("valid lock parses");
        assert_eq!(deps, vec![("serde".to_string(), "1.0.0".to_string())]);
    }

    #[test]
    fn load_cargo_lock_deps_returns_none_for_missing_lockfile() {
        assert!(load_cargo_lock_deps(Path::new("/nonexistent/Cargo.lock")).is_none());
    }

    fn rustc_args_for_out_dir(out_dir: &Path) -> RustcArgs {
        RustcArgs::parse(&[
            "rustc".to_string(),
            "--out-dir".to_string(),
            out_dir.to_string_lossy().into_owned(),
        ])
        .unwrap()
    }

    #[test]
    fn find_lock_path_walks_from_out_dir_to_the_workspace_lock() {
        let ws = scaffold_workspace();
        write_lock(ws.path(), &[("app", "0.1.0")]);
        let out_dir = ws.path().join("target/debug/deps");
        std::fs::create_dir_all(&out_dir).unwrap();
        let found = find_lock_path(
            Some(&rustc_args_for_out_dir(&out_dir)),
            Some(ws.path()),
            Some(ws.path()),
        )
        .unwrap();
        assert_eq!(found, ws.path().join("Cargo.lock"));
    }

    #[test]
    fn candidate_roots_are_nonempty_and_push_unique_dedupes() {
        let ws = scaffold_workspace();
        let out_dir = ws.path().join("target/debug/deps");
        std::fs::create_dir_all(&out_dir).unwrap();
        let roots = candidate_roots(
            Some(&rustc_args_for_out_dir(&out_dir)),
            Some(ws.path()),
            Some(ws.path()),
        );
        assert!(
            roots.iter().any(|root| root == ws.path()),
            "expected workspace root in {roots:?}"
        );

        let mut paths = vec![PathBuf::from("/a")];
        push_unique(&mut paths, PathBuf::from("/a"));
        push_unique(&mut paths, PathBuf::from("/b"));
        assert_eq!(paths, vec![PathBuf::from("/a"), PathBuf::from("/b")]);
    }

    #[test]
    fn external_target_directory_cannot_supply_the_workspace_lock() {
        let ws = scaffold_workspace();
        write_lock(ws.path(), &[("app", "0.1.0"), ("dep", "0.1.0")]);
        let member = ws.path().join("app");

        let external = tempfile::tempdir().unwrap();
        std::fs::write(external.path().join("Cargo.toml"), "[workspace]\n").unwrap();
        write_lock(external.path(), &[("wrong", "9.9.9")]);
        let out_dir = external.path().join("target/debug/deps");
        std::fs::create_dir_all(&out_dir).unwrap();

        let found = find_lock_path(
            Some(&rustc_args_for_out_dir(&out_dir)),
            Some(&member),
            Some(&member),
        )
        .unwrap();
        assert_eq!(found, ws.path().join("Cargo.lock"));
    }

    #[test]
    fn cargo_metadata_discovers_a_workspace_without_a_lockfile() {
        let ws = scaffold_workspace();
        let discovery = run_cargo_metadata(
            OsStr::new("cargo"),
            Some(&ws.path().join("Cargo.toml")),
            Instant::now() + Duration::from_secs(60),
        )
        .expect("metadata should resolve the workspace");
        assert!(discovery.crate_names.contains(&"app".to_string()));
        assert!(discovery.crate_names.contains(&"dep".to_string()));
        assert_eq!(discovery.workspace_root.as_deref(), Some(ws.path()));
    }

    #[test]
    fn crate_names_from_lock_dedupes_and_rejects_empty() {
        let dir = tempfile::tempdir().unwrap();
        write_lock(
            dir.path(),
            &[("serde", "1.0.0"), ("serde", "1.0.1"), ("tokio", "1.0.0")],
        );
        let names = crate_names_from_lock(&dir.path().join("Cargo.lock")).unwrap();
        assert_eq!(names, vec!["serde", "tokio"]);

        let empty = tempfile::tempdir().unwrap();
        write_lock(empty.path(), &[]);
        assert!(crate_names_from_lock(&empty.path().join("Cargo.lock")).is_none());
    }

    #[test]
    fn test_build_intent_into_request_preserves_shard_context() {
        let intent = BuildIntent {
            crate_names: vec!["serde".into(), "tokio".into()],
            namespace: Some("x86_64/hash/release".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
            identity_key: Some("id/abcd/x86_64-unknown-linux-gnu/release".into()),
        };

        let req = into_build_started_request(intent, 42, "sess-test".into());
        assert_eq!(req.intent.crate_names, vec!["serde", "tokio"]);
        assert_eq!(req.intent.namespace.as_deref(), Some("x86_64/hash/release"));
        assert_eq!(req.intent.cargo_lock_deps.len(), 1);
        assert_eq!(
            req.intent.identity_key.as_deref(),
            Some("id/abcd/x86_64-unknown-linux-gnu/release")
        );
        assert_eq!(req.client_epoch, 42);
    }

    #[test]
    fn output_fits_the_cap_up_to_the_last_byte() {
        assert!(fits_output_cap(5, 5, 10));
        assert!(!fits_output_cap(5, 6, 10));
        assert!(fits_output_cap(0, 0, 0));
        assert!(!fits_output_cap(usize::MAX, 1, usize::MAX));
    }

    /// A `cargo` stand-in: a shell script, so these run on Unix only. The
    /// budget and backoff decisions they exercise are covered on every host
    /// by the pure-function tests.
    #[cfg(unix)]
    fn fake_cargo(dir: &Path, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join("fake-cargo");
        std::fs::write(
            &path,
            format!("#!/bin/sh\n[ -n \"$KACHE_FAKE_CARGO_WARMUP\" ] && exit 0\n{body}\n"),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        // Run it once before a test times it. A child another test thread
        // forks while the file is open for writing keeps that descriptor
        // until it execs, and Linux refuses to execute a file open for
        // writing (ETXTBSY); macOS makes the first exec of a new file slow.
        // After one run succeeds, neither can touch the timed runs.
        for _ in 0..500 {
            match std::process::Command::new(&path)
                .env("KACHE_FAKE_CARGO_WARMUP", "1")
                .status()
            {
                Ok(status) => {
                    assert!(status.success(), "fake cargo warm-up: {status}");
                    return path;
                }
                Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("fake cargo warm-up: {error}"),
            }
        }
        panic!("fake cargo stayed busy");
    }

    #[cfg(unix)]
    fn soon(millis: u64) -> Instant {
        Instant::now() + Duration::from_millis(millis)
    }

    #[cfg(unix)]
    #[test]
    fn a_hanging_cargo_is_killed_at_the_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("pid");
        let cargo = fake_cargo(
            dir.path(),
            &format!("echo $$ > '{}'\nexec sleep 30", pid_file.display()),
        );

        let started = Instant::now();
        // Long enough for the script to start and record its pid: the
        // first exec of a freshly written file can be slow on macOS.
        let result = run_cargo_metadata(cargo.as_os_str(), None, soon(2000));
        assert_eq!(result.err(), Some(DiscoveryFailure::Timeout));
        assert!(
            started.elapsed() < Duration::from_secs(6),
            "{:?}",
            started.elapsed()
        );

        // The whole process group is gone, not just abandoned.
        let pid: i32 = std::fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let gone = (0..50).any(|_| {
            // SAFETY: signal 0 only checks that the pid exists.
            let alive = unsafe { libc::kill(pid, 0) } == 0;
            if alive {
                std::thread::sleep(Duration::from_millis(100));
            }
            !alive
        });
        assert!(gone, "fake cargo {pid} outlived its deadline");
    }

    #[cfg(unix)]
    #[test]
    fn a_failed_or_garbled_cargo_run_is_reported_not_parsed() {
        let dir = tempfile::tempdir().unwrap();
        let failing = fake_cargo(dir.path(), "exit 1");
        assert_eq!(
            run_cargo_metadata(failing.as_os_str(), None, soon(10_000)).err(),
            Some(DiscoveryFailure::Exit)
        );

        let garbled = tempfile::tempdir().unwrap();
        let garbled = fake_cargo(garbled.path(), "echo not-json");
        assert_eq!(
            run_cargo_metadata(garbled.as_os_str(), None, soon(10_000)).err(),
            Some(DiscoveryFailure::Parse)
        );

        let missing = dir.path().join("no-such-cargo");
        assert_eq!(
            run_cargo_metadata(missing.as_os_str(), None, soon(10_000)).err(),
            Some(DiscoveryFailure::Spawn)
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_working_cargo_names_the_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let cargo = fake_cargo(
            dir.path(),
            r#"echo '{"workspace_root":"/ws","packages":[{"name":"app"},{"name":"dep"}]}'"#,
        );
        let discovery = run_cargo_metadata(cargo.as_os_str(), None, soon(10_000)).unwrap();
        assert_eq!(discovery.crate_names, vec!["app", "dep"]);
        assert_eq!(discovery.workspace_root.as_deref(), Some(Path::new("/ws")));
    }

    #[cfg(unix)]
    #[test]
    fn output_past_the_cap_fails_after_draining() {
        let dir = tempfile::tempdir().unwrap();
        // Enough to fill a pipe several times: the child must not block.
        let cargo = fake_cargo(dir.path(), "head -c 300000 /dev/zero");
        let mut command = std::process::Command::new(&cargo);
        command.stdout(std::process::Stdio::piped());
        assert_eq!(
            run_bounded(command, soon(10_000), 1000).err(),
            Some(DiscoveryFailure::TooLarge)
        );

        let mut command = std::process::Command::new(&cargo);
        command.stdout(std::process::Stdio::piped());
        assert_eq!(
            run_bounded(command, soon(10_000), 300_000).unwrap().len(),
            300_000
        );
    }

    /// Two workspaces with a `Cargo.toml` and no lockfile anywhere above.
    #[cfg(unix)]
    fn two_manifest_dirs() -> (tempfile::TempDir, tempfile::TempDir) {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        for dir in [&first, &second] {
            std::fs::write(dir.path().join("Cargo.toml"), "[workspace]\n").unwrap();
        }
        (first, second)
    }

    #[cfg(unix)]
    fn calls(log: &Path) -> usize {
        std::fs::read_to_string(log).map_or(0, |log| log.lines().count())
    }

    #[cfg(unix)]
    #[test]
    fn a_timeout_ends_discovery_without_trying_other_manifests() {
        let (first, second) = two_manifest_dirs();
        let bin = tempfile::tempdir().unwrap();
        let log = bin.path().join("calls");
        let cargo = fake_cargo(
            bin.path(),
            &format!("echo x >> '{}'\nexec sleep 30", log.display()),
        );

        let result = discover_workspace(
            cargo.as_os_str(),
            soon(2000),
            None,
            Some(first.path()),
            Some(second.path()),
        );
        assert_eq!(result.err(), Some(DiscoveryFailure::Timeout));
        assert_eq!(
            calls(&log),
            1,
            "the shared budget was spent on the first manifest"
        );
    }

    #[cfg(unix)]
    #[test]
    fn other_failures_try_every_manifest_then_the_bare_command() {
        let (first, second) = two_manifest_dirs();
        let bin = tempfile::tempdir().unwrap();
        let log = bin.path().join("calls");
        let cargo = fake_cargo(
            bin.path(),
            &format!("echo x >> '{}'\nexit 1", log.display()),
        );

        let result = discover_workspace(
            cargo.as_os_str(),
            soon(10_000),
            None,
            Some(first.path()),
            Some(second.path()),
        );
        assert_eq!(result.err(), Some(DiscoveryFailure::Exit));
        assert_eq!(calls(&log), 3);
    }

    #[test]
    fn an_unusable_lockfile_is_a_failure_not_a_metadata_run() {
        let dir = tempfile::tempdir().unwrap();
        write_lock(dir.path(), &[]);
        let result = discover_workspace(
            OsStr::new("no-such-cargo-should-not-run"),
            Instant::now() + Duration::from_secs(10),
            None,
            Some(dir.path()),
            Some(dir.path()),
        );
        assert_eq!(result.err(), Some(DiscoveryFailure::UnusableLock));
    }
}
