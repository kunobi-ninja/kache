use std::path::{Path, PathBuf};

use crate::args::RustcArgs;
use crate::identity;
use kache_core::BuildIntent;

struct WorkspaceDiscovery {
    crate_names: Vec<String>,
    workspace_root: Option<PathBuf>,
    lock_path: Option<PathBuf>,
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
    let discovery = discover_workspace(args, manifest_dir, cwd)?;
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

fn discover_workspace(
    args: Option<&RustcArgs>,
    manifest_dir: Option<&Path>,
    cwd: Option<&Path>,
) -> Option<WorkspaceDiscovery> {
    let lock_path = find_lock_path(args, manifest_dir, cwd);
    if let Some(lock_path) = lock_path.as_ref() {
        let crate_names = crate_names_from_lock(lock_path)?;
        let workspace_root = lock_path.parent().map(Path::to_path_buf);
        return Some(WorkspaceDiscovery {
            crate_names,
            workspace_root,
            lock_path: Some(lock_path.clone()),
        });
    }

    for manifest_path in candidate_manifest_paths(args, manifest_dir, cwd) {
        if let Some(discovery) = run_cargo_metadata(Some(&manifest_path)) {
            return Some(discovery);
        }
    }

    run_cargo_metadata(None)
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

fn run_cargo_metadata(manifest_path: Option<&Path>) -> Option<WorkspaceDiscovery> {
    let mut command = std::process::Command::new("cargo");
    command
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());

    if let Some(path) = manifest_path {
        command.arg("--manifest-path").arg(path);
    }

    let output = command.output().ok()?;

    if !output.status.success() {
        return None;
    }

    parse_metadata_packages(&output.stdout)
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
        let discovery = run_cargo_metadata(Some(&ws.path().join("Cargo.toml")))
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
}
