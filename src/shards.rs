use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::path::Path;

#[allow(dead_code)]
pub const NUM_SHARDS: usize = 64;

/// A computed set of shards for a dependency graph.
#[allow(dead_code)]
pub struct ShardSet {
    pub namespace: String,
    /// (shard_hash, Vec<(crate_name, version)>)
    pub shards: Vec<(String, Vec<(String, String)>)>,
}

/// Assign each crate to a shard by name, hash each shard's sorted content.
///
/// Sharding is deterministic: `blake3(crate_name) % NUM_SHARDS` assigns the bucket,
/// then the shard content hash is `blake3("name1@v1|name2@v2|...")` over sorted entries.
/// This gives insertion stability -- adding one crate only invalidates one shard.
#[allow(dead_code)]
pub fn compute_shards(namespace: &str, deps: &[(String, String)]) -> ShardSet {
    let mut buckets: BTreeMap<usize, Vec<(String, String)>> = BTreeMap::new();
    for (name, version) in deps {
        let hash = blake3::hash(name.as_bytes());
        let bucket =
            (u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap()) as usize) % NUM_SHARDS;
        buckets
            .entry(bucket)
            .or_default()
            .push((name.clone(), version.clone()));
    }

    let mut shards = Vec::new();
    for (_bucket_id, mut entries) in buckets {
        entries.sort();
        let content = entries
            .iter()
            .map(|(n, v)| format!("{n}@{v}"))
            .collect::<Vec<_>>()
            .join("|");
        let shard_hash = blake3::hash(content.as_bytes()).to_hex().to_string();
        shards.push((shard_hash, entries));
    }

    ShardSet {
        namespace: namespace.to_string(),
        shards,
    }
}

#[allow(dead_code)]
#[derive(serde::Deserialize)]
struct LockFile {
    #[serde(default)]
    package: Vec<LockPackage>,
}

#[allow(dead_code)]
#[derive(serde::Deserialize)]
struct LockPackage {
    name: String,
    version: String,
}

/// Parse a Cargo.lock file into sorted (name, version) pairs.
///
/// Handles Cargo.lock versions 3 and 4. Uses serde deserialization
/// to tolerate extra fields (source, checksum, dependencies).
#[allow(dead_code)]
pub fn parse_cargo_lock(path: &Path) -> Result<Vec<(String, String)>> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("reading Cargo.lock at {}", path.display()))?;

    let lock: LockFile = toml::from_str(&content).context("parsing Cargo.lock as TOML")?;

    let mut deps: Vec<(String, String)> = lock
        .package
        .into_iter()
        .map(|p| (p.name, p.version))
        .collect();

    deps.sort();
    Ok(deps)
}

/// Build a namespace string from known build config.
/// Format: `{target}/{rustc_hash}/{profile}`
#[allow(dead_code)]
pub fn build_namespace(target: &str, rustc_hash: &str, profile: &str) -> String {
    format!("{target}/{rustc_hash}/{profile}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_shards_deterministic() {
        let deps = vec![
            ("serde".to_string(), "1.0.210".to_string()),
            ("tokio".to_string(), "1.36.0".to_string()),
            ("syn".to_string(), "2.0.0".to_string()),
        ];

        let s1 = compute_shards("test", &deps);
        let s2 = compute_shards("test", &deps);

        assert_eq!(s1.shards.len(), s2.shards.len());
        for ((h1, e1), (h2, e2)) in s1.shards.iter().zip(s2.shards.iter()) {
            assert_eq!(h1, h2);
            assert_eq!(e1, e2);
        }
    }

    #[test]
    fn test_compute_shards_single_crate_change_only_affects_one_shard() {
        let deps_v1 = vec![
            ("serde".to_string(), "1.0.210".to_string()),
            ("tokio".to_string(), "1.36.0".to_string()),
            ("syn".to_string(), "2.0.0".to_string()),
        ];

        let deps_v2 = vec![
            ("serde".to_string(), "1.0.211".to_string()),
            ("tokio".to_string(), "1.36.0".to_string()),
            ("syn".to_string(), "2.0.0".to_string()),
        ];

        let s1 = compute_shards("test", &deps_v1);
        let s2 = compute_shards("test", &deps_v2);

        assert_eq!(s1.shards.len(), s2.shards.len());

        let changed: usize = s1
            .shards
            .iter()
            .zip(s2.shards.iter())
            .filter(|((h1, _), (h2, _))| h1 != h2)
            .count();

        assert_eq!(changed, 1, "only one shard should differ");
    }

    #[test]
    fn test_compute_shards_adding_dep_affects_one_shard() {
        let deps_v1 = vec![
            ("serde".to_string(), "1.0.210".to_string()),
            ("tokio".to_string(), "1.36.0".to_string()),
        ];

        let deps_v2 = vec![
            ("serde".to_string(), "1.0.210".to_string()),
            ("tokio".to_string(), "1.36.0".to_string()),
            ("anyhow".to_string(), "1.0.0".to_string()),
        ];

        let s1 = compute_shards("test", &deps_v1);
        let s2 = compute_shards("test", &deps_v2);

        let s1_hashes: std::collections::HashSet<_> =
            s1.shards.iter().map(|(h, _)| h.clone()).collect();
        let s2_hashes: std::collections::HashSet<_> =
            s2.shards.iter().map(|(h, _)| h.clone()).collect();

        let removed: usize = s1_hashes.difference(&s2_hashes).count();
        assert!(removed <= 1, "at most one shard should be invalidated");
    }

    #[test]
    fn test_compute_shards_empty() {
        let s = compute_shards("test", &[]);
        assert!(s.shards.is_empty());
    }

    #[test]
    fn test_compute_shards_entries_sorted_within_shard() {
        let deps: Vec<(String, String)> = (0..100)
            .map(|i| (format!("crate_{i:04}"), format!("0.{i}.0")))
            .collect();

        let s = compute_shards("test", &deps);
        for (_hash, entries) in &s.shards {
            for w in entries.windows(2) {
                assert!(w[0] <= w[1], "entries should be sorted within shard");
            }
        }
    }

    #[test]
    fn test_parse_cargo_lock_v3() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("Cargo.lock");
        let content = r#"version = 3

[[package]]
name = "serde"
version = "1.0.210"

[[package]]
name = "tokio"
version = "1.36.0"

[[package]]
name = "my-crate"
version = "0.1.0"
"#;
        std::fs::write(&lock_path, content).unwrap();

        let deps = parse_cargo_lock(&lock_path).unwrap();
        assert_eq!(deps.len(), 3);
        assert_eq!(deps[0].0, "my-crate");
        assert_eq!(deps[1].0, "serde");
        assert_eq!(deps[2].0, "tokio");
    }

    #[test]
    fn test_parse_cargo_lock_with_extras() {
        let dir = tempfile::tempdir().unwrap();
        let lock_path = dir.path().join("Cargo.lock");
        let content = r#"version = 3

[[package]]
name = "serde"
version = "1.0.210"
source = "registry+https://github.com/rust-lang/crates.io-index"
checksum = "abc123"

[[package]]
name = "tokio"
version = "1.36.0"
dependencies = ["serde"]
"#;
        std::fs::write(&lock_path, content).unwrap();

        let deps = parse_cargo_lock(&lock_path).unwrap();
        assert_eq!(deps.len(), 2);
    }

    #[test]
    fn test_parse_cargo_lock_nonexistent() {
        let result = parse_cargo_lock(Path::new("/nonexistent/Cargo.lock"));
        assert!(result.is_err());
    }

    #[test]
    fn test_build_namespace() {
        let ns = build_namespace("x86_64-unknown-linux-gnu", "abc123", "release");
        assert_eq!(ns, "x86_64-unknown-linux-gnu/abc123/release");
    }
}
