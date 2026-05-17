//! On-disk content-addressed store for compiler probe results.
//!
//! Each [`ResolvedConfig`] is one small JSON file under
//! `<cache_dir>/probes/<key>.json`. The key is the compiler binary's
//! identity (see [`probe_key`]); writes are atomic (temp file +
//! rename) so concurrent `make -j` processes never observe a partial
//! file — a late writer simply wins.

use super::{PROBE_SCHEMA_VERSION, ProbeRequest, ResolvedConfig};
use std::fs::Metadata;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Subdirectory of the kache cache dir that holds probe records.
const PROBE_SUBDIR: &str = "probes";

/// Content-addressed key for a probe, or `None` if the compiler cannot
/// be resolved / stat'd (the caller then probes uncached).
///
/// The key binds the record to the exact compiler binary: its resolved
/// absolute path plus a `stat` fingerprint. `ctime` sits alongside
/// `mtime` on purpose — a `cp -p` or restore-from-backup swap can
/// preserve `mtime` but always bumps `ctime`, so the pair catches a
/// compiler change `mtime` alone would miss. Deriving the key is a
/// `stat`, not a process fork.
pub fn probe_key(prober_id: &str, req: &ProbeRequest<'_>) -> Option<String> {
    let resolved = resolve_program(req.compiler)?;
    let meta = std::fs::metadata(&resolved).ok()?;

    let mut h = blake3::Hasher::new();
    h.update(b"probe_schema:");
    h.update(PROBE_SCHEMA_VERSION.to_string().as_bytes());
    h.update(b"\nprober:");
    h.update(prober_id.as_bytes());
    h.update(b"\ncompiler_path:");
    h.update(resolved.to_string_lossy().as_bytes());
    h.update(b"\ncompiler_stat:");
    h.update(compiler_fingerprint(&meta).as_bytes());
    Some(h.finalize().to_hex().to_string())
}

/// Load a probe record by key, or `None` on any miss: file absent,
/// unreadable, corrupt, or written under a different schema version.
/// A schema mismatch is deliberately a miss, never a wrong hit.
pub fn load(cache_dir: &Path, key: &str) -> Option<ResolvedConfig> {
    let bytes = std::fs::read(record_path(cache_dir, key)).ok()?;
    let config: ResolvedConfig = serde_json::from_slice(&bytes).ok()?;
    (config.schema_version == PROBE_SCHEMA_VERSION).then_some(config)
}

/// Write a probe record. Best-effort: any failure is swallowed — a
/// missed write just means the next process re-probes.
pub fn store(cache_dir: &Path, key: &str, config: &ResolvedConfig) {
    let dir = cache_dir.join(PROBE_SUBDIR);
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    let Ok(json) = serde_json::to_vec_pretty(config) else {
        return;
    };
    // Atomic publish: write a temp file in the destination dir, then
    // rename it over the final name. Readers see either the old record
    // or the new one, never a half-written file.
    let Ok(mut tmp) = tempfile::NamedTempFile::new_in(&dir) else {
        return;
    };
    if tmp.write_all(&json).is_err() {
        return;
    }
    let _ = tmp.persist(record_path(cache_dir, key));
}

fn record_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join(PROBE_SUBDIR).join(format!("{key}.json"))
}

/// A `stat` fingerprint of the compiler binary: size and modification
/// time everywhere, plus change-time and inode on Unix. Any of these
/// shifting means "different binary" → different key → re-probe.
fn compiler_fingerprint(meta: &Metadata) -> String {
    let mut parts = vec![format!("len={}", meta.len())];
    if let Ok(mtime) = meta.modified()
        && let Ok(d) = mtime.duration_since(std::time::UNIX_EPOCH)
    {
        parts.push(format!("mtime={}", d.as_nanos()));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        parts.push(format!("ctime={}.{}", meta.ctime(), meta.ctime_nsec()));
        parts.push(format!("ino={}", meta.ino()));
    }
    parts.join(",")
}

/// Resolve a program name to an absolute path for stat'ing.
///
/// A name containing a path separator is resolved directly (relative
/// to CWD if relative). A bare name (`cc`, `clang-17`) is searched on
/// `$PATH`, mirroring how the OS resolves it at exec time. Returns
/// `None` if nothing matches.
fn resolve_program(program: &str) -> Option<PathBuf> {
    let p = Path::new(program);
    let has_separator = p.parent().is_some_and(|par| !par.as_os_str().is_empty());
    if has_separator {
        return p.canonicalize().ok();
    }
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(program))
        .find(|candidate| candidate.is_file())
        .and_then(|candidate| candidate.canonicalize().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{NamedTempFile, TempDir};

    fn sample(version: &str) -> ResolvedConfig {
        ResolvedConfig {
            schema_version: PROBE_SCHEMA_VERSION,
            prober: "cc".to_string(),
            compiler_name: "clang".to_string(),
            version_line: version.to_string(),
        }
    }

    #[test]
    fn probe_key_is_stable_for_the_same_binary() {
        let compiler = NamedTempFile::new().unwrap();
        let req = ProbeRequest {
            compiler: compiler.path().to_str().unwrap(),
        };
        let k1 = probe_key("cc", &req).unwrap();
        let k2 = probe_key("cc", &req).unwrap();
        assert_eq!(k1, k2);
    }

    #[test]
    fn probe_key_changes_when_the_binary_changes() {
        let mut compiler = NamedTempFile::new().unwrap();
        let path = compiler.path().to_path_buf();
        let path_str = path.to_str().unwrap();

        let k1 = probe_key("cc", &ProbeRequest { compiler: path_str }).unwrap();

        // Rewrite the "binary": size (and mtime/ctime) all move.
        compiler.write_all(b"new compiler bytes").unwrap();
        compiler.flush().unwrap();

        let k2 = probe_key("cc", &ProbeRequest { compiler: path_str }).unwrap();
        assert_ne!(k1, k2, "a changed compiler binary must change the key");
    }

    #[test]
    fn probe_key_is_none_for_a_missing_compiler() {
        let req = ProbeRequest {
            compiler: "/nonexistent/kache-cc-xyz",
        };
        assert!(probe_key("cc", &req).is_none());
    }

    #[test]
    fn probe_key_differs_per_prober_id() {
        let compiler = NamedTempFile::new().unwrap();
        let path_str = compiler.path().to_str().unwrap();
        let a = probe_key("cc", &ProbeRequest { compiler: path_str }).unwrap();
        let b = probe_key("rustc", &ProbeRequest { compiler: path_str }).unwrap();
        assert_ne!(a, b, "prober id must be part of the key");
    }

    #[test]
    fn store_then_load_roundtrips() {
        let cache = TempDir::new().unwrap();
        let cfg = sample("clang 17.0.0");
        store(cache.path(), "deadbeef", &cfg);
        assert_eq!(load(cache.path(), "deadbeef"), Some(cfg));
    }

    #[test]
    fn store_creates_the_probe_subdir() {
        let cache = TempDir::new().unwrap();
        // The cache dir exists but `probes/` does not yet.
        store(cache.path(), "k", &sample("gcc 13"));
        assert!(cache.path().join(PROBE_SUBDIR).join("k.json").is_file());
    }

    #[test]
    fn load_is_none_when_record_absent() {
        let cache = TempDir::new().unwrap();
        assert_eq!(load(cache.path(), "missing"), None);
    }

    #[test]
    fn load_is_none_for_a_corrupt_record() {
        let cache = TempDir::new().unwrap();
        let dir = cache.path().join(PROBE_SUBDIR);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("bad.json"), b"{ not json").unwrap();
        assert_eq!(load(cache.path(), "bad"), None);
    }

    #[test]
    fn load_is_none_on_schema_mismatch() {
        let cache = TempDir::new().unwrap();
        let dir = cache.path().join(PROBE_SUBDIR);
        std::fs::create_dir_all(&dir).unwrap();
        // A record written under a future schema must read as a miss.
        let mut cfg = sample("clang 17");
        cfg.schema_version = PROBE_SCHEMA_VERSION + 1;
        std::fs::write(dir.join("future.json"), serde_json::to_vec(&cfg).unwrap()).unwrap();
        assert_eq!(load(cache.path(), "future"), None);
    }
}
