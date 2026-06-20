use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::args::RustcArgs;
use guppy::graph::{DependencyDirection, PackageGraph};
use kache_core::BuildIntent;

struct MetadataDiscovery {
    crate_names: Vec<String>,
    workspace_root: Option<PathBuf>,
}

pub fn discover(args: Option<&RustcArgs>) -> Option<BuildIntent> {
    let metadata = discover_metadata(args)?;
    let crate_names = metadata.crate_names;
    if crate_names.is_empty() {
        return None;
    }

    let namespace = std::env::var("KACHE_NAMESPACE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let lock_path = metadata
        .workspace_root
        .as_deref()
        .map(|root| root.join("Cargo.lock"))
        .unwrap_or_else(|| PathBuf::from("Cargo.lock"));
    let cargo_lock_deps = namespace
        .as_ref()
        .and_then(|_| load_cargo_lock_deps(&lock_path))
        .unwrap_or_default();

    Some(BuildIntent {
        crate_names,
        namespace,
        cargo_lock_deps,
    })
}

pub fn into_build_started_request(
    intent: BuildIntent,
    client_epoch: u64,
) -> crate::daemon::BuildStartedRequest {
    crate::daemon::BuildStartedRequest {
        intent,
        client_epoch,
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

fn discover_metadata(args: Option<&RustcArgs>) -> Option<MetadataDiscovery> {
    for manifest_path in candidate_manifest_paths(args) {
        if let Some(discovery) = run_cargo_metadata(Some(&manifest_path)) {
            return Some(discovery);
        }
    }

    run_cargo_metadata(None)
}

fn candidate_manifest_paths(args: Option<&RustcArgs>) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(out_dir) = args.and_then(|a| a.out_dir.as_deref())
        && let Some(path) = manifest_from_target_out_dir(out_dir)
        && path.is_file()
    {
        candidates.push(path);
    }

    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        let path = PathBuf::from(manifest_dir).join("Cargo.toml");
        if path.is_file() && !candidates.iter().any(|existing| existing == &path) {
            candidates.push(path);
        }
    }

    candidates
}

fn manifest_from_target_out_dir(out_dir: &Path) -> Option<PathBuf> {
    for ancestor in out_dir.ancestors() {
        if ancestor.file_name().and_then(|name| name.to_str()) == Some("target") {
            return ancestor.parent().map(|root| root.join("Cargo.toml"));
        }
    }
    None
}

fn run_cargo_metadata(manifest_path: Option<&Path>) -> Option<MetadataDiscovery> {
    let mut command = std::process::Command::new("cargo");
    command
        .args(["metadata", "--format-version", "1"])
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

    parse_metadata_graph(&output.stdout, manifest_path)
}

fn parse_metadata_graph(
    metadata_json: &[u8],
    manifest_path: Option<&Path>,
) -> Option<MetadataDiscovery> {
    let graph = PackageGraph::from_json(std::str::from_utf8(metadata_json).ok()?).ok()?;
    let crate_names = graph_crate_order(&graph, manifest_path)?;
    let workspace_root = Some(graph.workspace().root().as_std_path().to_path_buf());

    Some(MetadataDiscovery {
        crate_names,
        workspace_root,
    })
}

fn graph_crate_order(graph: &PackageGraph, manifest_path: Option<&Path>) -> Option<Vec<String>> {
    let package_ids = if let Some(manifest_path) = manifest_path
        && let Some(package) = graph
            .packages()
            .find(|package| paths_match(package.manifest_path().as_std_path(), manifest_path))
    {
        vec![package.id().clone()]
    } else {
        graph
            .workspace()
            .iter()
            .map(|package| package.id().clone())
            .collect::<Vec<_>>()
    };

    let package_set = graph.query_forward(package_ids.iter()).ok()?.resolve();
    let mut seen = HashSet::new();
    let mut ordered = Vec::new();

    for package in package_set.packages(DependencyDirection::Reverse) {
        let name = package.name().to_string();
        if seen.insert(name.clone()) {
            ordered.push(name);
        }
    }

    Some(ordered)
}

fn paths_match(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }

    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_cargo_metadata_uses_guppy_graph_and_workspace_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        std::fs::write(
            root.join("Cargo.toml"),
            r#"[workspace]
members = ["app", "dep", "unrelated"]
resolver = "2"
"#,
        )
        .unwrap();

        std::fs::create_dir_all(root.join("app/src")).unwrap();
        std::fs::write(
            root.join("app/Cargo.toml"),
            r#"[package]
name = "app"
version = "0.1.0"
edition = "2021"

[dependencies]
dep = { path = "../dep" }
"#,
        )
        .unwrap();
        std::fs::write(root.join("app/src/lib.rs"), "").unwrap();

        std::fs::create_dir_all(root.join("dep/src")).unwrap();
        std::fs::write(
            root.join("dep/Cargo.toml"),
            r#"[package]
name = "dep"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();
        std::fs::write(root.join("dep/src/lib.rs"), "").unwrap();

        std::fs::create_dir_all(root.join("unrelated/src")).unwrap();
        std::fs::write(
            root.join("unrelated/Cargo.toml"),
            r#"[package]
name = "unrelated"
version = "0.1.0"
edition = "2021"
"#,
        )
        .unwrap();
        std::fs::write(root.join("unrelated/src/lib.rs"), "").unwrap();

        let discovery = run_cargo_metadata(Some(&root.join("app/Cargo.toml"))).unwrap();
        assert_eq!(discovery.crate_names, vec!["dep", "app"]);
        assert_eq!(discovery.workspace_root.as_deref(), Some(root));
    }

    #[test]
    fn test_manifest_from_target_out_dir_uses_target_parent() {
        let manifest = manifest_from_target_out_dir(Path::new(
            "/repo/apps/tauri/src-tauri/target/release/deps",
        ))
        .unwrap();

        assert_eq!(manifest, Path::new("/repo/apps/tauri/src-tauri/Cargo.toml"));
    }

    #[test]
    fn test_manifest_from_target_out_dir_without_target_is_none() {
        // No `target` component in the path -> nothing to anchor on.
        assert!(manifest_from_target_out_dir(Path::new("/repo/src/deps")).is_none());
    }

    #[test]
    fn test_manifest_from_target_out_dir_picks_nearest_target() {
        // `ancestors()` walks from the leaf upward, so the *deepest* `target`
        // wins when the path is nested (e.g. a workspace target inside a repo
        // that itself sits under another `target`).
        let manifest =
            manifest_from_target_out_dir(Path::new("/work/target/x/inner/target/release/deps"))
                .unwrap();
        assert_eq!(manifest, Path::new("/work/target/x/inner/Cargo.toml"));
    }

    #[test]
    fn test_paths_match_identical_paths() {
        assert!(paths_match(
            Path::new("/some/where/Cargo.toml"),
            Path::new("/some/where/Cargo.toml"),
        ));
    }

    #[test]
    fn test_paths_match_distinct_nonexistent_paths_do_not_match() {
        // Different paths that can't be canonicalized fall through to false.
        assert!(!paths_match(
            Path::new("/nonexistent/a/Cargo.toml"),
            Path::new("/nonexistent/b/Cargo.toml"),
        ));
    }

    // Unix-only: creating a symlink on Windows needs Developer Mode / admin
    // privileges, which CI runners lack. The canonicalization logic under test
    // is platform-shared and covered here on Linux/macOS.
    #[cfg(unix)]
    #[test]
    fn test_paths_match_canonicalizes_equivalent_paths() {
        // A symlink and its target canonicalize to the same real path.
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("Cargo.toml");
        std::fs::write(&real, "").unwrap();
        let link = dir.path().join("link.toml");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(paths_match(&link, &real));
    }

    #[test]
    fn test_parse_metadata_graph_rejects_invalid_json() {
        assert!(parse_metadata_graph(b"not json at all", None).is_none());
    }

    #[test]
    fn test_parse_metadata_graph_rejects_invalid_utf8() {
        assert!(parse_metadata_graph(&[0xff, 0xfe, 0x00], None).is_none());
    }

    #[test]
    fn test_build_intent_into_request_preserves_shard_context() {
        let intent = BuildIntent {
            crate_names: vec!["serde".into(), "tokio".into()],
            namespace: Some("x86_64/hash/release".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let req = into_build_started_request(intent, 42);
        assert_eq!(req.intent.crate_names, vec!["serde", "tokio"]);
        assert_eq!(req.intent.namespace.as_deref(), Some("x86_64/hash/release"));
        assert_eq!(req.intent.cargo_lock_deps.len(), 1);
        assert_eq!(req.client_epoch, 42);
    }
}
