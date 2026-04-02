use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;

use kache_core::BuildIntent;

pub fn discover() -> Option<BuildIntent> {
    let crate_names = discover_crate_names_bfs()?;
    if crate_names.is_empty() {
        return None;
    }

    let namespace = std::env::var("KACHE_NAMESPACE")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let cargo_lock_deps = namespace
        .as_ref()
        .and_then(|_| load_cargo_lock_deps(Path::new("Cargo.lock")))
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

fn discover_crate_names_bfs() -> Option<Vec<String>> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    parse_metadata_crate_names_bfs(&output.stdout)
}

fn parse_metadata_crate_names_bfs(metadata_json: &[u8]) -> Option<Vec<String>> {
    let value: serde_json::Value = serde_json::from_slice(metadata_json).ok()?;

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
