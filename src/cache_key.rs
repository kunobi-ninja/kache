use crate::args::RustcArgs;
use anyhow::{Context, Result};
use std::path::Path;

/// Compute the blake3 cache key for a rustc invocation.
///
/// The key captures everything that affects compilation output:
/// - rustc version (full verbose string)
/// - target triple
/// - crate name and type
/// - codegen options (opt-level, lto, codegen-units, panic, etc.)
/// - feature flags (sorted)
/// - source file hash
/// - dependency artifact hashes
/// - RUSTFLAGS and relevant env vars
/// - linker identity (for bin/dylib caching)
pub fn compute_cache_key(args: &RustcArgs) -> Result<String> {
    let mut hasher = blake3::Hasher::new();

    // rustc version
    let rustc_version = get_rustc_version(&args.rustc)?;
    hasher.update(b"rustc_version:");
    hasher.update(rustc_version.as_bytes());
    hasher.update(b"\n");

    // target triple
    let target = args
        .target
        .as_deref()
        .unwrap_or_else(|| host_target_triple());
    hasher.update(b"target:");
    hasher.update(target.as_bytes());
    hasher.update(b"\n");

    // crate identity
    if let Some(name) = &args.crate_name {
        hasher.update(b"crate_name:");
        hasher.update(name.as_bytes());
        hasher.update(b"\n");
    }

    // crate types
    for ct in &args.crate_types {
        hasher.update(b"crate_type:");
        hasher.update(ct.as_bytes());
        hasher.update(b"\n");
    }

    // edition
    if let Some(edition) = &args.edition {
        hasher.update(b"edition:");
        hasher.update(edition.as_bytes());
        hasher.update(b"\n");
    }

    // codegen options (sorted for determinism)
    let mut codegen_opts: Vec<_> = args
        .codegen_opts
        .iter()
        .filter(|(k, _)| {
            // Skip incremental as it's path-dependent.
            // NOTE: extra-filename is kept — it's a cargo-computed metadata hash that
            // changes when any source file in the crate changes. Without it, kache
            // only hashes the crate root file (e.g., src/main.rs) and misses changes
            // to module files (src/foo.rs), serving stale artifacts.
            k != "incremental"
        })
        .collect();
    codegen_opts.sort_by_key(|(k, _)| k.as_str());
    for (key, value) in &codegen_opts {
        hasher.update(b"codegen:");
        hasher.update(key.as_bytes());
        if let Some(v) = value {
            hasher.update(b"=");
            hasher.update(v.as_bytes());
        }
        hasher.update(b"\n");
    }

    // feature flags (already sorted in args parsing)
    for feat in &args.features {
        hasher.update(b"feature:");
        hasher.update(feat.as_bytes());
        hasher.update(b"\n");
    }

    // cfg flags (non-feature, sorted)
    let mut cfgs: Vec<_> = args
        .cfgs
        .iter()
        .filter(|c| !c.starts_with("feature="))
        .collect();
    cfgs.sort();
    for cfg in &cfgs {
        hasher.update(b"cfg:");
        hasher.update(cfg.as_bytes());
        hasher.update(b"\n");
    }

    // source file hash
    if let Some(source) = &args.source_file {
        let source_hash = hash_file(source).context("hashing source file")?;
        hasher.update(b"source:");
        hasher.update(source_hash.as_bytes());
        hasher.update(b"\n");
    }

    // dependency artifact hashes (sorted by name for determinism)
    let mut externs: Vec<_> = args.externs.iter().filter(|e| e.path.is_some()).collect();
    externs.sort_by_key(|e| &e.name);
    for ext in &externs {
        if let Some(path) = &ext.path {
            // Hash the dependency artifact
            match hash_file(path) {
                Ok(dep_hash) => {
                    hasher.update(b"extern:");
                    hasher.update(ext.name.as_bytes());
                    hasher.update(b"=");
                    hasher.update(dep_hash.as_bytes());
                    hasher.update(b"\n");
                }
                Err(_) => {
                    // Sysroot crate (std, core, etc.) — identity is determined by
                    // rustc version + name, both already in the hash. Use a sentinel
                    // instead of the absolute path to enable cross-machine sharing.
                    hasher.update(b"extern_unreadable:");
                    hasher.update(ext.name.as_bytes());
                    hasher.update(b"\n");
                }
            }
        }
    }

    // RUSTFLAGS (normalized: workspace-root paths replaced with ".")
    if let Ok(rustflags) = std::env::var("RUSTFLAGS") {
        hasher.update(b"RUSTFLAGS:");
        hasher.update(normalize_flags(&rustflags).as_bytes());
        hasher.update(b"\n");
    }

    // CARGO_ENCODED_RUSTFLAGS (cargo's way of passing flags, normalized)
    if let Ok(flags) = std::env::var("CARGO_ENCODED_RUSTFLAGS") {
        hasher.update(b"CARGO_ENCODED_RUSTFLAGS:");
        hasher.update(normalize_flags(&flags).as_bytes());
        hasher.update(b"\n");
    }

    // Relevant CARGO_CFG_* env vars
    for (key, value) in std::env::vars() {
        if key.starts_with("CARGO_CFG_") {
            hasher.update(key.as_bytes());
            hasher.update(b"=");
            hasher.update(value.as_bytes());
            hasher.update(b"\n");
        }
    }

    // Linker identity for bin/dylib targets
    if args.is_executable_output()
        && let Some(linker_id) = get_linker_identity(args)
    {
        hasher.update(b"linker:");
        hasher.update(linker_id.as_bytes());
        hasher.update(b"\n");
    }

    let hash = hasher.finalize();
    Ok(hash.to_hex().to_string())
}

/// Normalize compiler flags by replacing the current working directory with ".".
/// This makes flags like `-L /home/runner/project/lib` portable across machines.
fn normalize_flags(flags: &str) -> String {
    let pwd = std::env::current_dir().unwrap_or_default();
    let pwd_str = pwd.to_string_lossy();
    if pwd_str.is_empty() {
        return flags.to_string();
    }
    flags.replace(&*pwd_str, ".")
}

/// Hash a file using blake3.
pub fn hash_file(path: &Path) -> Result<String> {
    let data = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let hash = blake3::hash(&data);
    Ok(hash.to_hex().to_string())
}

/// Get rustc version string (cached per rustc path).
fn get_rustc_version(rustc: &Path) -> Result<String> {
    let output = std::process::Command::new(rustc)
        .arg("--version")
        .arg("--verbose")
        .output()
        .context("running rustc --version --verbose")?;

    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(version)
}

/// Get the host target triple.
fn host_target_triple() -> &'static str {
    option_env!("TARGET").unwrap_or("unknown")
}

/// Get linker identity string for cache key.
fn get_linker_identity(args: &RustcArgs) -> Option<String> {
    // Check if a custom linker is specified
    let linker = args.get_codegen_opt("linker").unwrap_or("cc");

    let output = std::process::Command::new(linker)
        .arg("--version")
        .output()
        .ok()?;

    let version = String::from_utf8_lossy(&output.stdout);
    let first_line = version.lines().next()?;
    Some(first_line.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::RustcArgs;

    #[test]
    fn test_hash_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rs");
        std::fs::write(&file, b"fn main() {}").unwrap();

        let hash = hash_file(&file).unwrap();
        assert_eq!(hash.len(), 64); // blake3 hex is 64 chars

        // Same content = same hash
        let file2 = dir.path().join("test2.rs");
        std::fs::write(&file2, b"fn main() {}").unwrap();
        let hash2 = hash_file(&file2).unwrap();
        assert_eq!(hash, hash2);

        // Different content = different hash
        let file3 = dir.path().join("test3.rs");
        std::fs::write(&file3, b"fn main() { println!(\"hello\"); }").unwrap();
        let hash3 = hash_file(&file3).unwrap();
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_cache_key_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            "--edition=2021".to_string(),
            "-C".to_string(),
            "opt-level=2".to_string(),
        ];

        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();

        let key1 = compute_cache_key(&parsed1).unwrap();
        let key2 = compute_cache_key(&parsed2).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_changes_with_source() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");

        // First version
        std::fs::write(&source, b"pub fn hello() {}").unwrap();
        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];
        let parsed1 = RustcArgs::parse(&args_vec).unwrap();
        let key1 = compute_cache_key(&parsed1).unwrap();

        // Modified source
        std::fs::write(&source, b"pub fn hello() { println!(\"hi\"); }").unwrap();
        let parsed2 = RustcArgs::parse(&args_vec).unwrap();
        let key2 = compute_cache_key(&parsed2).unwrap();

        assert_ne!(key1, key2);
    }

    #[test]
    fn test_unreadable_dep_produces_stable_key() {
        // Simulate unreadable deps (sysroot crates) from two different paths —
        // the cache key should be identical because we use a sentinel, not the path.
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        // Create two "dep" paths that both point to non-existent files (will fail hash_file)
        let dep_a =
            std::path::PathBuf::from("/home/runner/.rustup/toolchains/stable/lib/libstd.rlib");
        let dep_b =
            std::path::PathBuf::from("/Users/dev/.rustup/toolchains/stable/lib/libstd.rlib");

        let args_vec: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
        ];

        let mut parsed_a = RustcArgs::parse(&args_vec).unwrap();
        parsed_a.externs.push(crate::args::ExternDep {
            name: "std".to_string(),
            path: Some(dep_a),
        });

        let mut parsed_b = RustcArgs::parse(&args_vec).unwrap();
        parsed_b.externs.push(crate::args::ExternDep {
            name: "std".to_string(),
            path: Some(dep_b),
        });

        let key_a = compute_cache_key(&parsed_a).unwrap();
        let key_b = compute_cache_key(&parsed_b).unwrap();
        assert_eq!(
            key_a, key_b,
            "unreadable deps with different paths should produce the same key"
        );
    }

    #[test]
    fn test_normalize_flags() {
        let pwd = std::env::current_dir().unwrap();
        let pwd_str = pwd.to_string_lossy();

        let flags = format!("-L {}/target/release/deps", pwd_str);
        let normalized = normalize_flags(&flags);
        assert_eq!(normalized, "-L ./target/release/deps");

        // Flags without absolute paths should pass through unchanged
        let plain = "-C opt-level=2";
        assert_eq!(normalize_flags(plain), plain);
    }

    #[test]
    fn test_cache_key_changes_with_features() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, b"pub fn hello() {}").unwrap();

        let args1: Vec<String> = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "mylib".to_string(),
            source.to_string_lossy().to_string(),
            "--cfg".to_string(),
            "feature=\"std\"".to_string(),
        ];

        let mut args2 = args1.clone();
        args2.push("--cfg".to_string());
        args2.push("feature=\"derive\"".to_string());

        let parsed1 = RustcArgs::parse(&args1).unwrap();
        let parsed2 = RustcArgs::parse(&args2).unwrap();

        let key1 = compute_cache_key(&parsed1).unwrap();
        let key2 = compute_cache_key(&parsed2).unwrap();

        assert_ne!(key1, key2);
    }
}
