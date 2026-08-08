//! Direct-mode memo for the dep-info pre-pass (the ccache-manifest analog).
//!
//! `compute_cache_key` runs `rustc --emit=dep-info` per compile to enumerate
//! the source closure. That subprocess (parse + macro expansion) dominates
//! per-hit key overhead — measured 96% of `key_ms` even for trivial crates,
//! and hundreds of ms for proc-macro-heavy ones. This module memoizes the
//! pre-pass *result* so a warm build skips the subprocess entirely.
//!
//! # Soundness
//!
//! The dep-info output is a deterministic function of:
//! 1. the rustc binary (version + the proc-macro dylibs it loads),
//! 2. the expansion-relevant argv,
//! 3. the contents of every file rustc reads (the recorded source list),
//! 4. the values of every env var rustc tracks (`env!`/`option_env!`,
//!    the recorded `# env-dep:` list).
//!
//! A record is reused only when ALL FOUR are revalidated: (1) the rustc
//! `-vV` string is in the digest and every extern artifact (which includes
//! proc-macro dylibs) must match its recorded content hash; (2) the filtered
//! dep-info argv is in the digest; (3) every recorded source file must match
//! its recorded content hash; (4) every recorded env-dep value must compare
//! equal (path-normalized, so worktree-local values like `OUT_DIR` still
//! match across clones). Any mismatch, missing file, or IO error falls back
//! to the real pre-pass — the memo can lose a reuse, never serve a stale
//! source list for changed inputs.
//!
//! Known boundary (shared with ccache's direct mode, and with the existing
//! key computation): inputs rustc itself does not report — a file a proc
//! macro reads without `tracked_path`, or a *new* file that changes module
//! resolution (`foo.rs` vs `foo/mod.rs` ambiguity) — are invisible here
//! exactly as they are invisible to the key today. The memo does not widen
//! that envelope: it revalidates precisely the inputs the key itself uses.
//!
//! # Storage
//!
//! One small JSON file per invocation shape under `<cache_dir>/depinfo/`,
//! atomic temp+rename writes — the [`crate::probe::cache`] pattern. Paths
//! are stored in the key-normalized sentinel form (`<WORKSPACE>/src/lib.rs`)
//! so a record written in one worktree can be reused in another; reuse
//! denormalizes against the *current* normalizer and content-validates, so
//! a wrong denormalization can only cost the reuse, not correctness.

use crate::cache_key::{DepInfo, FileHasher};
use crate::path_normalizer::PathNormalizer;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Bump to invalidate every prior memo record (schema or semantics change).
const MEMO_SCHEMA_VERSION: u32 = 1;

/// Subdirectory of the kache cache dir that holds memo records.
const MEMO_SUBDIR: &str = "depinfo";

/// Records untouched for this long are removed by the lazy GC pass.
const GC_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(30 * 24 * 3600);

/// Minimum interval between GC scans (marker-file mtime gated).
const GC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(24 * 3600);

// ============================================================================
// Record shape
// ============================================================================

/// One memoized dep-info pre-pass result.
#[derive(Debug, Serialize, Deserialize)]
pub struct MemoRecord {
    /// [`MEMO_SCHEMA_VERSION`] at write time; mismatches are ignored on read.
    pub schema: u32,
    /// The invocation digest this record was stored under (sanity re-check on
    /// read — a renamed/corrupted file must not validate).
    pub digest: String,
    /// Source closure: key-normalized path + whole-file blake3, exactly the
    /// hashes the key's Group A folded when the record was written.
    pub sources: Vec<MemoSource>,
    /// `# env-dep:` vars with their PATH-NORMALIZED values (raw values may
    /// embed worktree-local paths; normalized compare keeps records portable).
    pub env_deps: Vec<MemoEnvDep>,
    /// Every `--extern` artifact with a path: key-normalized path + content
    /// hash, or `None` when the artifact was unreadable at record time
    /// (sysroot crates). Proc-macro dylibs live here — an edited macro changes
    /// its dylib content, which must invalidate the memoized expansion result.
    pub externs: Vec<MemoExtern>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemoSource {
    pub path: String,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemoEnvDep {
    pub var: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MemoExtern {
    pub path: String,
    pub hash: Option<String>,
}

/// A validated reuse: the reconstructed [`DepInfo`] plus the content hashes
/// computed during validation, so the key's Group A/B fold them without
/// re-querying the file-hash cache (and without double-counting hash stats).
pub struct ReusedDepInfo {
    pub dep_info: DepInfo,
    /// `(content_hash, absolute path)` pairs — the exact shape the post-pass
    /// source-hash loop produces.
    pub hashed_sources: Vec<(String, PathBuf)>,
    /// Current-worktree extern path → validated content hash (`None` =
    /// unreadable, the sysroot-crate arm).
    pub extern_hashes: HashMap<PathBuf, Option<String>>,
}

// ============================================================================
// Store
// ============================================================================

/// Handle on the on-disk memo directory. Cheap to construct; all IO is
/// per-call and best-effort (a broken memo dir degrades to the real pre-pass,
/// never fails the build).
pub struct DepInfoMemo {
    dir: PathBuf,
}

impl DepInfoMemo {
    pub fn new(cache_dir: &Path) -> Self {
        DepInfoMemo {
            dir: cache_dir.join(MEMO_SUBDIR),
        }
    }

    fn record_path(&self, digest: &str) -> PathBuf {
        self.dir.join(format!("{digest}.json"))
    }

    /// Read the record for `digest`, or `None` (missing, unreadable, schema or
    /// digest mismatch). A successful read refreshes the file's mtime so the
    /// lazy GC treats actively-reused records as live.
    pub fn lookup(&self, digest: &str) -> Option<MemoRecord> {
        let path = self.record_path(digest);
        let data = std::fs::read(&path).ok()?;
        let record: MemoRecord = match serde_json::from_slice(&data) {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!("dep-info memo unreadable at {}: {e}", path.display());
                return None;
            }
        };
        if record.schema != MEMO_SCHEMA_VERSION || record.digest != digest {
            return None;
        }
        let _ = filetime::set_file_mtime(&path, filetime::FileTime::now());
        Some(record)
    }

    /// Persist `record` under its digest. Atomic (temp + rename) so concurrent
    /// wrappers never observe a partial file — a late writer simply wins.
    /// Best-effort: failures are logged at debug, never propagated.
    pub fn store(&self, record: &MemoRecord) {
        if let Err(e) = self.store_inner(record) {
            tracing::debug!("dep-info memo store failed: {e}");
        }
        self.maybe_gc();
    }

    fn store_inner(&self, record: &MemoRecord) -> Result<()> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating {}", self.dir.display()))?;
        let json = serde_json::to_vec(record).context("serializing dep-info memo record")?;
        let mut tmp = tempfile::NamedTempFile::new_in(&self.dir)
            .with_context(|| format!("temp file in {}", self.dir.display()))?;
        std::io::Write::write_all(&mut tmp, &json).context("writing dep-info memo record")?;
        let final_path = self.record_path(&record.digest);
        tmp.persist(&final_path)
            .with_context(|| format!("persisting {}", final_path.display()))?;
        Ok(())
    }

    /// Lazy GC: at most once per [`GC_INTERVAL`] (marker-file mtime gated),
    /// remove records whose mtime is older than [`GC_MAX_AGE`]. Reads refresh
    /// mtime, so only genuinely idle invocation shapes age out.
    fn maybe_gc(&self) {
        let marker = self.dir.join(".last_gc");
        let due = match std::fs::metadata(&marker).and_then(|m| m.modified()) {
            Ok(modified) => modified
                .elapsed()
                .map(|age| age >= GC_INTERVAL)
                .unwrap_or(false),
            Err(_) => true,
        };
        if !due {
            return;
        }
        // Touch the marker FIRST so concurrent wrappers don't all scan.
        if std::fs::write(&marker, b"").is_err() {
            return;
        }
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let old = entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|m| m.elapsed().map(|age| age >= GC_MAX_AGE).unwrap_or(false))
                .unwrap_or(false);
            if old {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
}

// ============================================================================
// Digest
// ============================================================================

/// Flags that never affect dep-info OUTPUT (diagnostics presentation only).
/// Skipping them keeps the digest stable across terminal widths and error
/// format choices. Each entry matches both the `--flag=value` and the
/// two-token `--flag value` spellings.
const COSMETIC_FLAGS: &[&str] = &["--diagnostic-width", "--error-format", "--json", "--color"];

fn is_cosmetic_flag(arg: &str) -> Option<bool> {
    for flag in COSMETIC_FLAGS {
        if arg == *flag {
            return Some(true); // two-token form: skip value too
        }
        if let Some(rest) = arg.strip_prefix(flag)
            && rest.starts_with('=')
        {
            return Some(false); // single-token form
        }
    }
    None
}

/// Digest of everything that selects a memo record: schema version, the rustc
/// identity (`-vV` string + wrapper paths), the working directory (rustc
/// resolves relative paths against it), and the exact filtered argv the
/// pre-pass would run — each path-normalized so two worktrees with the same
/// normalized shape share records. Returns `None` when the CWD is unreadable
/// (no sound digest exists; caller runs the real pass).
pub fn invocation_digest(
    rustc_version: &str,
    rustc: &Path,
    inner_rustc: Option<&Path>,
    source_file: &Path,
    all_args: &[String],
    normalizer: &PathNormalizer,
) -> Option<String> {
    let cwd = std::env::current_dir().ok()?;

    let mut h = blake3::Hasher::new();
    h.update(b"depinfo_memo_schema:");
    h.update(MEMO_SCHEMA_VERSION.to_string().as_bytes());
    h.update(b"\nrustc_version:");
    h.update(rustc_version.as_bytes());
    h.update(b"\nrustc:");
    h.update(normalizer.normalize(rustc.to_string_lossy()).as_bytes());
    h.update(b"\ninner_rustc:");
    match inner_rustc {
        Some(p) => h.update(normalizer.normalize(p.to_string_lossy()).as_bytes()),
        None => h.update(b"-"),
    };
    h.update(b"\ncwd:");
    h.update(normalizer.normalize(cwd.to_string_lossy()).as_bytes());
    h.update(b"\nargs:");
    let dep_args = crate::cache_key::dep_info_invocation_args(source_file, all_args);
    let mut skip_next = false;
    for arg in &dep_args {
        if skip_next {
            skip_next = false;
            continue;
        }
        match is_cosmetic_flag(arg) {
            Some(true) => {
                skip_next = true;
                continue;
            }
            Some(false) => continue,
            None => {}
        }
        h.update(normalizer.normalize(arg).as_bytes());
        h.update(b"\x1f");
    }
    Some(h.finalize().to_hex().to_string())
}

// ============================================================================
// Validation / reuse
// ============================================================================

/// Resolve a stored (normalized) path against the current worktree: try each
/// denormalization candidate, first existing wins. Correctness never rests on
/// this choice — the caller content-validates whatever path comes back.
fn resolve_stored_path(stored: &str, normalizer: &PathNormalizer) -> Option<PathBuf> {
    let candidates = normalizer.denormalize_candidates(stored);
    match candidates.len() {
        0 => None,
        1 => {
            let p = PathBuf::from(&candidates[0]);
            p.exists().then_some(p)
        }
        _ => candidates
            .into_iter()
            .map(PathBuf::from)
            .find(|p| p.exists()),
    }
}

/// Validate `record` against the current tree and env; `Some` only when every
/// source file and extern artifact matches its recorded content hash and every
/// env-dep value compares equal (normalized). The returned [`ReusedDepInfo`]
/// mirrors exactly what the real pre-pass + hash loop would have produced.
pub fn validate(
    record: &MemoRecord,
    args_externs: &[(String, PathBuf)],
    file_hasher: &FileHasher<'_>,
    normalizer: &PathNormalizer,
) -> Option<ReusedDepInfo> {
    // Env deps first — no IO. `parse_env_dep_info` conflates unset with empty
    // (`# env-dep:VAR` and `# env-dep:VAR=` both parse to ""), so the compare
    // mirrors that: the key would be identical either way.
    let mut env_deps = Vec::with_capacity(record.env_deps.len());
    for dep in &record.env_deps {
        let current = std::env::var(&dep.var).unwrap_or_default();
        if normalizer.normalize(&current) != dep.value {
            tracing::debug!(
                "dep-info memo: env-dep {} changed — running real pre-pass",
                dep.var
            );
            return None;
        }
        env_deps.push((dep.var.clone(), current));
    }

    // The digest covers the normalized extern path set (extern flags are in
    // the argv), so a digest match means the SETS agree; what needs checking
    // is content. Index recorded externs by normalized path.
    let recorded_externs: HashMap<&str, &Option<String>> = record
        .externs
        .iter()
        .map(|e| (e.path.as_str(), &e.hash))
        .collect();

    // Resolve stored source paths against this worktree.
    let mut resolved: Vec<PathBuf> = Vec::with_capacity(record.sources.len());
    for source in &record.sources {
        match resolve_stored_path(&source.path, normalizer) {
            Some(path) => resolved.push(path),
            None => {
                tracing::debug!(
                    "dep-info memo: source {} not resolvable — running real pre-pass",
                    source.path
                );
                return None;
            }
        }
    }

    // Batch the hash lookups like the post-pass path does.
    let mut prefetch: Vec<&Path> = resolved.iter().map(PathBuf::as_path).collect();
    prefetch.extend(args_externs.iter().map(|(_, p)| p.as_path()));
    file_hasher.prefetch(&prefetch);

    let mut hashed_sources = Vec::with_capacity(record.sources.len());
    for (source, path) in record.sources.iter().zip(&resolved) {
        match file_hasher.hash(path) {
            Ok(hash) if hash == source.hash => hashed_sources.push((hash, path.clone())),
            Ok(_) => {
                tracing::debug!(
                    "dep-info memo: source {} changed — running real pre-pass",
                    path.display()
                );
                return None;
            }
            Err(e) => {
                tracing::debug!(
                    "dep-info memo: source {} unhashable ({e}) — running real pre-pass",
                    path.display()
                );
                return None;
            }
        }
    }

    let mut extern_hashes = HashMap::with_capacity(args_externs.len());
    for (_name, path) in args_externs {
        let normalized = normalizer.normalize(path.to_string_lossy());
        let Some(recorded) = recorded_externs.get(normalized.as_str()) else {
            // Digest should make this unreachable; treat drift as a miss.
            tracing::debug!(
                "dep-info memo: extern {} not in record — running real pre-pass",
                path.display()
            );
            return None;
        };
        match (file_hasher.hash(path), recorded) {
            (Ok(hash), Some(recorded_hash)) if hash == **recorded_hash => {
                extern_hashes.insert(path.clone(), Some(hash));
            }
            // Unreadable then, unreadable now: the sysroot-crate arm.
            (Err(_), None) => {
                extern_hashes.insert(path.clone(), None);
            }
            // Content changed, or readable-ness flipped either way — the
            // expansion inputs (proc-macro dylibs) may differ.
            _ => {
                tracing::debug!(
                    "dep-info memo: extern {} changed — running real pre-pass",
                    path.display()
                );
                return None;
            }
        }
    }

    // `DepInfo.source_files` is sorted by path (the `parse_dep_info`
    // invariant); resolved paths may sort differently than their stored
    // normalized forms.
    let mut source_files = resolved;
    source_files.sort();

    Some(ReusedDepInfo {
        dep_info: DepInfo {
            source_files,
            env_deps,
        },
        hashed_sources,
        extern_hashes,
    })
}

/// Build the record for a freshly-run pre-pass from hashes the key fold
/// already computed (no extra IO). `None` when any piece is unrecordable —
/// e.g. a source file that failed to hash (the key warns and skips it; a memo
/// must not validate a smaller closure than the real one).
pub fn build_record(
    digest: &str,
    dep_info: &DepInfo,
    hashed_sources: &[(String, PathBuf)],
    extern_hashes: &[(PathBuf, Option<String>)],
    normalizer: &PathNormalizer,
) -> Option<MemoRecord> {
    if hashed_sources.len() != dep_info.source_files.len() {
        return None;
    }
    Some(MemoRecord {
        schema: MEMO_SCHEMA_VERSION,
        digest: digest.to_string(),
        sources: hashed_sources
            .iter()
            .map(|(hash, path)| MemoSource {
                path: normalizer.normalize(path.to_string_lossy()),
                hash: hash.clone(),
            })
            .collect(),
        env_deps: dep_info
            .env_deps
            .iter()
            .map(|(var, val)| MemoEnvDep {
                var: var.clone(),
                value: normalizer.normalize(val),
            })
            .collect(),
        externs: extern_hashes
            .iter()
            .map(|(path, hash)| MemoExtern {
                path: normalizer.normalize(path.to_string_lossy()),
                hash: hash.clone(),
            })
            .collect(),
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_key::hash_file;

    fn write(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    /// A record whose sources/externs are raw paths (empty normalizer) with
    /// their CURRENT content hashes — the shape a just-recorded pre-pass has.
    fn record_for(
        digest: &str,
        sources: &[PathBuf],
        externs: &[(PathBuf, Option<String>)],
    ) -> MemoRecord {
        MemoRecord {
            schema: MEMO_SCHEMA_VERSION,
            digest: digest.to_string(),
            sources: sources
                .iter()
                .map(|p| MemoSource {
                    path: p.to_string_lossy().to_string(),
                    hash: hash_file(p).unwrap(),
                })
                .collect(),
            env_deps: Vec::new(),
            externs: externs
                .iter()
                .map(|(p, h)| MemoExtern {
                    path: p.to_string_lossy().to_string(),
                    hash: h.clone(),
                })
                .collect(),
        }
    }

    #[test]
    fn store_then_lookup_roundtrips() {
        let temp = tempfile::tempdir().unwrap();
        let memo = DepInfoMemo::new(temp.path());
        let digest = "a".repeat(64);
        let record = MemoRecord {
            schema: MEMO_SCHEMA_VERSION,
            digest: digest.clone(),
            sources: vec![MemoSource {
                path: "<WORKSPACE>/src/lib.rs".into(),
                hash: "b".repeat(64),
            }],
            env_deps: vec![MemoEnvDep {
                var: "OUT_DIR".into(),
                value: "<WORKSPACE>/target/out".into(),
            }],
            externs: vec![MemoExtern {
                path: "<WORKSPACE>/target/libdep.rlib".into(),
                hash: Some("c".repeat(64)),
            }],
        };
        memo.store(&record);

        let loaded = memo.lookup(&digest).expect("stored record should load");
        assert_eq!(loaded.digest, digest);
        assert_eq!(loaded.sources.len(), 1);
        assert_eq!(loaded.sources[0].path, "<WORKSPACE>/src/lib.rs");
        assert_eq!(loaded.env_deps[0].var, "OUT_DIR");
        assert_eq!(
            loaded.externs[0].hash.as_deref(),
            Some("c".repeat(64).as_str())
        );
    }

    #[test]
    fn lookup_misses_on_absent_digest_and_rejects_digest_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let memo = DepInfoMemo::new(temp.path());
        assert!(memo.lookup(&"0".repeat(64)).is_none(), "empty memo dir");

        // A record file renamed to a different digest must not validate: the
        // embedded digest is re-checked on read.
        let real = "1".repeat(64);
        let record = record_for(&real, &[], &[]);
        memo.store(&record);
        let stolen = "2".repeat(64);
        std::fs::rename(
            temp.path().join(MEMO_SUBDIR).join(format!("{real}.json")),
            temp.path().join(MEMO_SUBDIR).join(format!("{stolen}.json")),
        )
        .unwrap();
        assert!(memo.lookup(&stolen).is_none(), "digest mismatch must miss");
    }

    #[test]
    fn lookup_rejects_schema_mismatch_and_corrupt_json() {
        let temp = tempfile::tempdir().unwrap();
        let memo = DepInfoMemo::new(temp.path());
        let digest = "3".repeat(64);
        let mut record = record_for(&digest, &[], &[]);
        record.schema = MEMO_SCHEMA_VERSION + 1;
        memo.store(&record);
        assert!(memo.lookup(&digest).is_none(), "future schema must miss");

        let corrupt = "4".repeat(64);
        std::fs::write(
            temp.path()
                .join(MEMO_SUBDIR)
                .join(format!("{corrupt}.json")),
            b"{not json",
        )
        .unwrap();
        assert!(memo.lookup(&corrupt).is_none(), "corrupt record must miss");
    }

    #[test]
    fn validate_reuses_unchanged_inputs() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let inc = write(temp.path(), "included.rs", "pub fn b() {}");
        let ext = write(temp.path(), "libdep.rlib", "rlib-bytes");
        let ext_hash = hash_file(&ext).unwrap();

        let record = record_for(
            "d1",
            &[lib.clone(), inc.clone()],
            &[(ext.clone(), Some(ext_hash.clone()))],
        );
        let fh = FileHasher::new();
        let pn = PathNormalizer::empty();
        let args_externs = vec![("dep".to_string(), ext.clone())];

        let reused = validate(&record, &args_externs, &fh, &pn).expect("unchanged inputs reuse");
        assert_eq!(
            reused.dep_info.source_files,
            vec![inc.clone(), lib.clone()],
            "sorted by path"
        );
        assert!(reused.dep_info.env_deps.is_empty());
        assert_eq!(reused.hashed_sources.len(), 2);
        assert_eq!(
            reused.extern_hashes.get(&ext),
            Some(&Some(ext_hash)),
            "validated extern hash is carried for the key fold"
        );
    }

    #[test]
    fn validate_rejects_changed_source_content() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let record = record_for("d2", &[lib.clone()], &[]);

        // Same length, different bytes — only the content hash can tell.
        std::fs::write(&lib, "pub fn z() {}").unwrap();
        assert!(
            validate(&record, &[], &FileHasher::new(), &PathNormalizer::empty()).is_none(),
            "changed source must fall back to the real pre-pass"
        );
    }

    #[test]
    fn validate_rejects_missing_source() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let record = record_for("d3", &[lib.clone()], &[]);
        std::fs::remove_file(&lib).unwrap();
        assert!(
            validate(&record, &[], &FileHasher::new(), &PathNormalizer::empty()).is_none(),
            "missing source must fall back"
        );
    }

    #[test]
    fn validate_rejects_changed_env_dep() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let mut record = record_for("d4", &[lib], &[]);
        record.env_deps.push(MemoEnvDep {
            var: "KACHE_TEST_DEP_INFO_MEMO_UNSET_VAR".into(),
            value: "recorded-value".into(),
        });
        // The var is unset now (current compares as ""), recorded non-empty.
        assert!(
            validate(&record, &[], &FileHasher::new(), &PathNormalizer::empty()).is_none(),
            "changed env-dep value must fall back"
        );
    }

    #[test]
    fn validate_accepts_env_dep_recorded_for_unset_var() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let mut record = record_for("d5", &[lib], &[]);
        // `parse_env_dep_info` records an unset `option_env!` var as "" — an
        // unset var at reuse time compares equal, mirroring the key's own
        // unset/empty conflation.
        record.env_deps.push(MemoEnvDep {
            var: "KACHE_TEST_DEP_INFO_MEMO_UNSET_VAR".into(),
            value: String::new(),
        });
        let reused = validate(&record, &[], &FileHasher::new(), &PathNormalizer::empty())
            .expect("unset-var record validates against unset var");
        assert_eq!(
            reused.dep_info.env_deps,
            vec![(
                "KACHE_TEST_DEP_INFO_MEMO_UNSET_VAR".to_string(),
                String::new()
            )]
        );
    }

    #[test]
    fn validate_rejects_changed_extern_content() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let ext = write(temp.path(), "libdep.rlib", "old-bytes");
        let ext_hash = hash_file(&ext).unwrap();
        let record = record_for("d6", &[lib], &[(ext.clone(), Some(ext_hash))]);

        // A proc-macro dylib edit changes bytes at the SAME path — expansion
        // inputs differ, so the memoized source closure may be stale.
        std::fs::write(&ext, "new-bytes").unwrap();
        assert!(
            validate(
                &record,
                &[("dep".to_string(), ext)],
                &FileHasher::new(),
                &PathNormalizer::empty()
            )
            .is_none(),
            "changed extern must fall back"
        );
    }

    #[test]
    fn validate_rejects_readability_flip_on_extern() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        // Recorded as unreadable (`None`, the sysroot arm) but readable now.
        let ext = write(temp.path(), "libdep.rlib", "bytes");
        let record = record_for("d7", &[lib], &[(ext.clone(), None)]);
        assert!(
            validate(
                &record,
                &[("dep".to_string(), ext)],
                &FileHasher::new(),
                &PathNormalizer::empty()
            )
            .is_none(),
            "unreadable-then, readable-now must fall back"
        );
    }

    #[test]
    fn validate_accepts_still_unreadable_extern() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let ghost = temp.path().join("libstd.rlib"); // never created
        let record = record_for("d8", &[lib], &[(ghost.clone(), None)]);
        let reused = validate(
            &record,
            &[("std".to_string(), ghost.clone())],
            &FileHasher::new(),
            &PathNormalizer::empty(),
        )
        .expect("still-unreadable extern reuses (sysroot-crate arm)");
        assert_eq!(reused.extern_hashes.get(&ghost), Some(&None));
    }

    #[test]
    fn build_record_refuses_incomplete_source_hashes() {
        let dep_info = DepInfo {
            source_files: vec![PathBuf::from("/a"), PathBuf::from("/b")],
            env_deps: Vec::new(),
        };
        // One of two sources failed to hash: the memo must not validate a
        // smaller closure than the real one.
        let hashed = vec![("h1".to_string(), PathBuf::from("/a"))];
        assert!(build_record("d9", &dep_info, &hashed, &[], &PathNormalizer::empty()).is_none());
    }

    #[test]
    fn cosmetic_flags_do_not_change_the_digest() {
        let temp = tempfile::tempdir().unwrap();
        let lib = write(temp.path(), "lib.rs", "pub fn a() {}");
        let pn = PathNormalizer::empty();
        let base = vec![
            "--crate-name".to_string(),
            "x".to_string(),
            "--edition=2021".to_string(),
        ];
        let mut cosmetic = base.clone();
        cosmetic.extend([
            "--error-format=json".to_string(),
            "--json".to_string(),
            "diagnostic-rendered-ansi".to_string(),
            "--diagnostic-width=120".to_string(),
            "--color".to_string(),
            "always".to_string(),
        ]);
        let rustc = Path::new("/usr/bin/rustc");
        let a = invocation_digest("rustc 1.0", rustc, None, &lib, &base, &pn).unwrap();
        let b = invocation_digest("rustc 1.0", rustc, None, &lib, &cosmetic, &pn).unwrap();
        assert_eq!(
            a, b,
            "diagnostics presentation flags must not fragment the memo"
        );

        let mut semantic = base.clone();
        semantic.push("--cfg=feature=\"extra\"".to_string());
        let c = invocation_digest("rustc 1.0", rustc, None, &lib, &semantic, &pn).unwrap();
        assert_ne!(a, c, "expansion-relevant flags must change the digest");

        let d = invocation_digest("rustc 2.0", rustc, None, &lib, &base, &pn).unwrap();
        assert_ne!(a, d, "rustc version must change the digest");
    }
}
