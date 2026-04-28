use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use crate::args::RustcArgs;
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

    parse_metadata(&output.stdout)
}

fn parse_metadata(metadata_json: &[u8]) -> Option<MetadataDiscovery> {
    let value: serde_json::Value = serde_json::from_slice(metadata_json).ok()?;
    let crate_names = parse_metadata_value_crate_names_bfs(&value)?;
    let workspace_root = value
        .get("workspace_root")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from);

    Some(MetadataDiscovery {
        crate_names,
        workspace_root,
    })
}

#[cfg(test)]
fn parse_metadata_crate_names_bfs(metadata_json: &[u8]) -> Option<Vec<String>> {
    let value: serde_json::Value = serde_json::from_slice(metadata_json).ok()?;
    parse_metadata_value_crate_names_bfs(&value)
}

fn parse_metadata_value_crate_names_bfs(value: &serde_json::Value) -> Option<Vec<String>> {
    let packages = value.get("packages")?.as_array()?;
    let mut id_to_name: HashMap<String, String> = HashMap::new();
    for pkg in packages {
        if let (Some(id), Some(name)) = (
            pkg.get("id").and_then(|v| v.as_str()),
            pkg.get("name").and_then(|v| v.as_str()),
        ) {
            id_to_name.insert(id.to_string(), name.to_string());
        }
    }

    let nodes = value.get("resolve")?.get("nodes")?.as_array()?;
    let mut dep_count: HashMap<String, usize> = HashMap::new();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

    for node in nodes {
        let Some(id) = node.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let id = id.to_string();
        let deps = node
            .get("deps")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|d| d.get("pkg").and_then(|v| v.as_str()).map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        dep_count.insert(id.clone(), deps.len());

        for dep_id in deps {
            dep_count.entry(dep_id.clone()).or_insert(0);
            dependents.entry(dep_id).or_default().push(id.clone());
        }
    }

    for reverse_edges in dependents.values_mut() {
        reverse_edges.sort();
    }

    let mut zero_deps: Vec<String> = dep_count
        .iter()
        .filter(|&(_, &count)| count == 0)
        .map(|(id, _)| id.clone())
        .collect();
    zero_deps.sort();
    let mut queue: VecDeque<String> = zero_deps.into();

    let mut seen = HashSet::new();
    let mut ordered_names = Vec::new();

    while let Some(id) = queue.pop_front() {
        if let Some(name) = id_to_name.get(&id)
            && seen.insert(name.clone())
        {
            ordered_names.push(name.clone());
        }
        if let Some(reverse_edges) = dependents.get(&id) {
            for dependent in reverse_edges {
                if let Some(count) = dep_count.get_mut(dependent) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        queue.push_back(dependent.clone());
                    }
                }
            }
        }
    }

    Some(ordered_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_metadata_crate_names_bfs_orders_leaves_first() {
        let metadata = r#"{
          "packages": [
            {"id": "serde 1.0.0 (registry)", "name": "serde"},
            {"id": "tokio 1.0.0 (registry)", "name": "tokio"},
            {"id": "app 0.1.0 (path)", "name": "app"}
          ],
          "resolve": {
            "nodes": [
              {"id": "app 0.1.0 (path)", "deps": [
                {"pkg": "serde 1.0.0 (registry)"},
                {"pkg": "tokio 1.0.0 (registry)"}
              ]},
              {"id": "serde 1.0.0 (registry)", "deps": []},
              {"id": "tokio 1.0.0 (registry)", "deps": []}
            ]
          }
        }"#;

        let names = parse_metadata_crate_names_bfs(metadata.as_bytes()).unwrap();
        assert_eq!(names, vec!["serde", "tokio", "app"]);
    }

    #[test]
    fn test_parse_metadata_crate_names_bfs_deduplicates_names() {
        let metadata = r#"{
          "packages": [
            {"id": "serde 1.0.0 (registry)", "name": "serde"},
            {"id": "serde 1.0.1 (registry)", "name": "serde"}
          ],
          "resolve": {
            "nodes": [
              {"id": "serde 1.0.0 (registry)", "deps": []},
              {"id": "serde 1.0.1 (registry)", "deps": []}
            ]
          }
        }"#;

        let names = parse_metadata_crate_names_bfs(metadata.as_bytes()).unwrap();
        assert_eq!(names, vec!["serde"]);
    }

    #[test]
    fn test_parse_metadata_preserves_workspace_root() {
        let metadata = r#"{
          "workspace_root": "/repo/apps/tauri/src-tauri",
          "packages": [
            {"id": "serde 1.0.0 (registry)", "name": "serde"},
            {"id": "app 0.1.0 (path)", "name": "app"}
          ],
          "resolve": {
            "nodes": [
              {"id": "app 0.1.0 (path)", "deps": [
                {"pkg": "serde 1.0.0 (registry)"}
              ]},
              {"id": "serde 1.0.0 (registry)", "deps": []}
            ]
          }
        }"#;

        let discovery = parse_metadata(metadata.as_bytes()).unwrap();
        assert_eq!(discovery.crate_names, vec!["serde", "app"]);
        assert_eq!(
            discovery.workspace_root.as_deref(),
            Some(Path::new("/repo/apps/tauri/src-tauri"))
        );
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
