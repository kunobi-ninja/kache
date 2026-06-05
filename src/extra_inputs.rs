//! User-declared extra cache-key inputs (issue #220, Phase 1: co-located).
//!
//! kache keys a crate on what rustc *reports* — source files (dep-info),
//! `--extern` artifacts, flags. A growing class of crates also read files
//! at **compile time that rustc never reports**: sqlx's `query!` macro reads
//! `.sqlx/query-*.json`, migration macros read `migrations/`, codegen reads
//! data files. Editing one of those changes the compiled output but no `.rs`
//! rustc lists — so kache's key doesn't move and a stale artifact is restored
//! (a false hit).
//!
//! This module lets a crate *declare* those files in a co-located
//! `<crate-dir>/kache.toml`:
//!
//! ```toml
//! extra_inputs = [".sqlx/**/*.json", "migrations/**/*.sql"]
//! ```
//!
//! The declared files' content hashes are folded into that crate's key, so a
//! change to them re-keys (a clean miss) instead of serving a stale hit.
//!
//! ## Safety properties
//! - **Opt-in, union-only.** A crate with no `kache.toml` is untouched (one
//!   `stat`, key byte-identical to today — so no `CACHE_KEY_VERSION` bump is
//!   needed). A misconfigured glob can only *add* inputs → an extra miss,
//!   never a wrong artifact.
//! - **Scoped per crate.** The file lives inside the crate it applies to, so
//!   it can never leak across projects (the central-table hazard deferred to
//!   Phase 2) and a sibling crate's key is unaffected.
//! - **Relocation-stable.** Files are folded by *content hash*, sorted by
//!   hash — moving the worktree doesn't change the key (matches `source:`).
//! - **Config changes count.** The declared pattern strings are folded too,
//!   so editing `kache.toml` re-keys even when it matches zero files.

use crate::cache_key::FileHasher;
use std::path::{Component, Path, PathBuf};

/// The co-located per-crate config file. Deliberately distinct from the
/// project config `.kache.toml` so a crate-local file can never shadow the
/// workspace's remote/store settings via the ancestor walk.
const COLOCATED_NAME: &str = "kache.toml";

/// Minimal schema for `<crate-dir>/kache.toml`. `deny_unknown_fields` makes a
/// stray `remote`/`local_store`/etc. a loud parse error rather than a
/// silently-honored crate-granularity setting — this file is *only* for
/// extra inputs.
#[derive(serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct ColocatedConfig {
    extra_inputs: Vec<String>,
}

/// Compute a digest of a crate's co-located extra inputs, or `None` when the
/// crate declares none (no `kache.toml`, empty list, or non-cacheable
/// invocation). Fold the returned digest into the crate's key via
/// [`crate::cache_key::fold_labeled`].
///
/// `source_file` is the compile's primary source; the crate dir is the
/// nearest ancestor containing a `Cargo.toml`. `file_hasher` is the same
/// memoized hasher the key build holds, so repeated files cost once.
pub(crate) fn digest(
    source_file: Option<&Path>,
    crate_name: &str,
    is_primary: bool,
    file_hasher: &FileHasher<'_>,
) -> Option<String> {
    // Only cacheable compiles pay any cost; everything else short-circuits
    // before the `stat`.
    if !is_primary {
        return None;
    }

    let crate_dir = crate_dir_from_source(source_file?)?;
    let config_path = crate_dir.join(COLOCATED_NAME);

    // Cheap existence check first — the cost paid by the vast majority of
    // crates that don't use the feature is this one `stat`.
    let raw = match std::fs::read(&config_path) {
        Ok(bytes) => bytes,
        Err(_) => return None,
    };

    let text = match std::str::from_utf8(&raw) {
        Ok(t) => t,
        Err(_) => return Some(unparseable_digest(crate_name, &config_path, &raw)),
    };

    let config: ColocatedConfig = match toml::from_str(text) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                "[key:{crate_name}] {} is invalid ({e}); folding it as an opaque \
                 input so the crate rebuilds until fixed",
                config_path.display()
            );
            // We can't know the real inputs, so don't risk a stale hit: fold
            // the raw bytes as an opaque, deterministic component. Same broken
            // file → same key; any edit re-keys.
            return Some(unparseable_digest(crate_name, &config_path, &raw));
        }
    };

    fold_extra_inputs(crate_name, &crate_dir, &config.extra_inputs, file_hasher)
}

/// Fold a crate's co-located extra inputs into an already-computed key.
/// A no-op (returns `base` unchanged) when the crate declares none, so it is
/// safe to call unconditionally from every compiler family's `cache_key`.
pub(crate) fn apply_extra_inputs(
    base: String,
    source_file: Option<&Path>,
    crate_name: &str,
    is_primary: bool,
    file_hasher: &FileHasher<'_>,
) -> String {
    match digest(source_file, crate_name, is_primary, file_hasher) {
        Some(d) => crate::cache_key::fold_labeled(base, "extra_inputs", &d),
        None => base,
    }
}

/// Walk up from the primary source file to the nearest directory containing a
/// `Cargo.toml`. Cargo invokes rustc with cwd = the package source dir, so a
/// relative source path is anchored there. Returns `None` outside cargo's
/// layout (bare `rustc`/`cc` with no enclosing crate) — the feature is then a
/// no-op.
fn crate_dir_from_source(source_file: &Path) -> Option<PathBuf> {
    let cwd = std::env::current_dir().ok();
    let absolute = if source_file.is_absolute() {
        source_file.to_path_buf()
    } else {
        cwd?.join(source_file)
    };

    let mut dir = absolute.parent();
    while let Some(d) = dir {
        if d.join("Cargo.toml").is_file() {
            return Some(d.to_path_buf());
        }
        dir = d.parent();
    }
    None
}

/// Fold the declared pattern set and the content hashes of every matched file
/// into a single hex digest. Returns `None` only when no patterns are
/// declared (an explicit empty list = "no extra inputs" = no fold).
fn fold_extra_inputs(
    crate_name: &str,
    crate_dir: &Path,
    patterns: &[String],
    file_hasher: &FileHasher<'_>,
) -> Option<String> {
    // Normalize + validate the declared patterns. A pattern that escapes the
    // crate dir (absolute or `..`) is a co-located scope violation: warn and
    // skip it (drop to hit-rate, never reach outside the crate).
    let mut normalized: Vec<String> = patterns
        .iter()
        .filter_map(|p| normalize_pattern(crate_name, crate_dir, p))
        .collect();
    if normalized.is_empty() {
        return None;
    }
    normalized.sort();
    normalized.dedup();

    let mut hasher = blake3::Hasher::new();

    // (1) The declared pattern set itself — so editing `kache.toml` re-keys
    // even when it currently matches zero files.
    for pat in &normalized {
        hasher.update(b"extra_input_pattern:");
        hasher.update(pat.as_bytes());
        hasher.update(b"\x1f");
    }

    // (2) Enumerate the matched files on disk.
    let mut matched: Vec<PathBuf> = Vec::new();
    for pat in &normalized {
        // Escape the crate dir's literal bytes so a `[`/`?` in the path can't
        // be read as a glob metachar; the user's pattern is appended raw.
        let full = format!(
            "{}/{}",
            glob::Pattern::escape(&crate_dir.to_string_lossy()),
            pat
        );
        let entries = match glob::glob(&full) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("[key:{crate_name}] bad extra_inputs glob {pat:?}: {e}");
                continue;
            }
        };
        for entry in entries.flatten() {
            if entry.is_file() {
                matched.push(entry);
            }
        }
    }
    matched.sort();
    matched.dedup();

    // Warm the memoized hasher (daemon-backed) in one batch.
    let paths: Vec<&Path> = matched.iter().map(|p| p.as_path()).collect();
    file_hasher.prefetch(&paths);

    // (3) Content hashes (relocation-stable) and unreadable sentinels. Both
    // sorted so the fold order is content-only, never path- or FS-order
    // dependent.
    let mut hashes: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    for path in &matched {
        match file_hasher.hash(path) {
            Ok(h) => hashes.push(h),
            Err(e) => {
                // "Couldn't read" must not collide with "absent": fold a
                // sentinel keyed by the crate-relative path (mirrors
                // `extern_unreadable:`).
                let rel = path.strip_prefix(crate_dir).unwrap_or(path);
                tracing::warn!("[key:{crate_name}] extra_input unreadable {rel:?}: {e}");
                unreadable.push(rel.to_string_lossy().into_owned());
            }
        }
    }
    hashes.sort();
    unreadable.sort();
    for h in &hashes {
        hasher.update(b"extra_input:");
        hasher.update(h.as_bytes());
        hasher.update(b"\x1f");
    }
    for u in &unreadable {
        hasher.update(b"extra_input_unreadable:");
        hasher.update(u.as_bytes());
        hasher.update(b"\x1f");
    }

    let total_bytes: u64 = matched
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
        .sum();
    tracing::debug!(
        "[key:{crate_name}] extra_inputs: {} pattern(s), {} file(s), {} unreadable, {} bytes",
        normalized.len(),
        hashes.len(),
        unreadable.len(),
        total_bytes
    );

    Some(hasher.finalize().to_hex().to_string())
}

/// Expand (`$ENV`/`~`), NFC-normalize, and validate a declared pattern, then
/// reshape directory-style patterns so they actually enumerate. Returns
/// `None` (warn + skip) for a pattern that escapes the crate dir.
fn normalize_pattern(crate_name: &str, crate_dir: &Path, pattern: &str) -> Option<String> {
    let expanded = crate::config::expand_exclude_pattern(pattern);
    // NFC so a typed-NFC pattern matches an NFD on-disk name (matches the
    // PathNormalizer's normalization).
    let normalized: String = {
        use unicode_normalization::UnicodeNormalization;
        expanded.nfc().collect()
    };

    let as_path = Path::new(&normalized);
    if as_path.is_absolute()
        || as_path
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs pattern {pattern:?} escapes the crate dir \
             (absolute or `..`); skipping"
        );
        return None;
    }

    // A bare or trailing-slash directory matches *nothing* under glob, which
    // would silently re-introduce a false hit. Reshape `.sqlx/` → `.sqlx/**/*`
    // and a bare existing dir `.sqlx` → `.sqlx/**/*` so the canonical examples
    // enumerate as intended.
    let reshaped = if normalized.ends_with('/') {
        format!("{}**/*", normalized)
    } else if crate_dir.join(&normalized).is_dir() {
        format!("{}/**/*", normalized)
    } else {
        normalized
    };
    Some(reshaped)
}

/// Deterministic opaque digest for an unreadable / unparseable `kache.toml`:
/// the build re-keys on any edit and never aliases "no file present".
fn unparseable_digest(crate_name: &str, config_path: &Path, raw: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"extra_inputs_unparseable:");
    hasher.update(raw);
    tracing::debug!(
        "[key:{crate_name}] folding {} as opaque (unparseable)",
        config_path.display()
    );
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal crate dir: `Cargo.toml`, `src/lib.rs`, and any
    /// (relative path, contents) files listed. Returns (tempdir, src_path).
    fn crate_fixture(files: &[(&str, &str)]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        std::fs::create_dir_all(root.join("src")).unwrap();
        let src = root.join("src/lib.rs");
        std::fs::write(&src, "// crate\n").unwrap();
        for (rel, contents) in files {
            let p = root.join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, contents).unwrap();
        }
        (dir, src)
    }

    fn dig(src: &Path) -> Option<String> {
        let fh = FileHasher::new();
        digest(Some(src), "x", true, &fh)
    }

    #[test]
    fn no_colocated_file_is_noop() {
        let (_d, src) = crate_fixture(&[]);
        assert_eq!(dig(&src), None);
    }

    #[test]
    fn non_primary_is_noop() {
        let (_d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        let fh = FileHasher::new();
        assert_eq!(digest(Some(&src), "x", false, &fh), None);
    }

    #[test]
    fn empty_list_is_noop() {
        let (_d, src) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        assert_eq!(dig(&src), None);
    }

    #[test]
    fn declared_input_change_rekeys() {
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        let before = dig(&src).expect("declared input folds a digest");

        // Editing the declared file must move the digest.
        std::fs::write(d.path().join(".sqlx/q.json"), "v2").unwrap();
        let after = dig(&src).expect("still folds after edit");
        assert_ne!(before, after);

        // Re-reading without changes is deterministic.
        assert_eq!(after, dig(&src).unwrap());
    }

    #[test]
    fn zero_match_still_folds_pattern_set() {
        // No matching files, but the declared pattern is folded so editing
        // the pattern set re-keys — and it is distinct from "no file".
        let (_d, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]")]);
        let only_pattern = dig(&src).expect("pattern set folds even at zero matches");

        let (_d2, src2) = crate_fixture(&[("kache.toml", "extra_inputs = [\"other/**/*.sql\"]")]);
        let other_pattern = dig(&src2).unwrap();
        assert_ne!(only_pattern, other_pattern);
    }

    #[test]
    fn dir_shaped_patterns_are_equivalent() {
        // `.sqlx/`, `.sqlx`, and `.sqlx/**/*` must enumerate the same set.
        let (d, _src) = crate_fixture(&[(".sqlx/q.json", "v1")]);
        let root = d.path();
        let p1 = normalize_pattern("x", root, ".sqlx/").unwrap();
        let p2 = normalize_pattern("x", root, ".sqlx").unwrap();
        let p3 = normalize_pattern("x", root, ".sqlx/**/*").unwrap();
        assert_eq!(p1, p3);
        assert_eq!(p2, p3);
    }

    #[test]
    fn patterns_escaping_crate_dir_are_rejected() {
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        assert!(normalize_pattern("x", root, "../secrets/**").is_none());
        assert!(normalize_pattern("x", root, "/etc/**").is_none());
        assert!(normalize_pattern("x", root, ".sqlx/**/*.json").is_some());
    }

    #[test]
    fn sibling_crate_without_file_is_unaffected() {
        // One crate declares inputs; a sibling without a kache.toml folds
        // nothing — scoping is per crate.
        let (_d1, src1) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        let (_d2, src2) = crate_fixture(&[(".sqlx/q.json", "v1")]);
        assert!(dig(&src1).is_some());
        assert_eq!(dig(&src2), None);
    }

    #[test]
    fn relocation_is_stable() {
        // Two crates with byte-identical declared inputs at different paths
        // must produce the same digest (content-hash folding, not paths) —
        // this is what survives a worktree move / cross-machine restore.
        let files = &[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*.json\"]"),
            (".sqlx/q.json", "v1"),
        ];
        let (_d1, src1) = crate_fixture(files);
        let (_d2, src2) = crate_fixture(files);
        assert_eq!(dig(&src1), dig(&src2));
        assert!(dig(&src1).is_some());
    }

    #[test]
    fn unparseable_file_folds_opaque_and_rekeys_on_edit() {
        let (d, src) = crate_fixture(&[("kache.toml", "this is = not valid toml [[[")]);
        let before = dig(&src).expect("broken config folds opaque, never silently ignored");
        std::fs::write(d.path().join("kache.toml"), "still = broken ]]]").unwrap();
        let after = dig(&src).unwrap();
        assert_ne!(before, after);
    }

    #[test]
    fn stray_key_is_rejected_as_unparseable() {
        // `deny_unknown_fields`: a non-extra_inputs key is a loud parse error,
        // folded opaque rather than silently honored.
        let (_d, src) =
            crate_fixture(&[("kache.toml", "extra_inputs = []\nlocal_store = \"/tmp\"")]);
        assert!(dig(&src).is_some());
    }
}
