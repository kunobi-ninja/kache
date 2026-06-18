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
//! - **Local & explicit.** The file lives inside the crate it applies to and
//!   only ever affects that crate's key — it can never *implicitly* apply to
//!   other projects (the central-table hazard deferred to Phase 2), and a
//!   sibling crate is unaffected. A pattern *may* deliberately reach outside
//!   the crate (absolute / `..`) when a build genuinely depends on a shared or
//!   machine-specific file; that stays fail-safe but makes the key
//!   host-/layout-specific, so a portability warning fires.
//! - **Relocation-stable, swap-sensitive.** Each file is folded as its
//!   *crate-relative path* + *content hash* (`/`-normalized, sorted). Moving
//!   the worktree or restoring on another machine doesn't change the key
//!   (the path is crate-relative), but swapping two matched files' contents —
//!   where the filename→content binding is load-bearing, e.g. sqlx migration
//!   order — does, because the path travels with the hash.
//! - **Config changes count.** The declared pattern strings are folded too,
//!   so editing `kache.toml` re-keys even when it matches zero files; a
//!   non-empty declaration whose patterns are all rejected still folds (it
//!   never collapses to the unconfigured key).

use crate::cache_key::FileHasher;
use std::path::{Component, Path, PathBuf};

/// The co-located per-crate config file. Deliberately distinct from the
/// project config `.kache.toml` so a crate-local file can never shadow the
/// workspace's remote/store settings via the ancestor walk.
const COLOCATED_NAME: &str = "kache.toml";

/// Above this many matched files, an `extra_inputs` glob is almost certainly
/// over-broad (e.g. accidentally spanning `target/`, or an absolute `/**`):
/// folding that many files busts the key on every change and walks a large
/// tree each compile. Warn so it's visible under default verbosity without
/// failing the build — over-folding is fail-safe, just slow.
const OVER_BROAD_FILE_WARN: usize = 1000;

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
/// into a single hex digest. Returns `None` only for the genuinely-empty
/// declaration (`extra_inputs = []`), an explicit opt-out that must stay
/// byte-identical to having no `kache.toml`. A non-empty declaration always
/// folds *something* — even if every pattern is rejected — so it can never
/// collapse back to the no-config key.
fn fold_extra_inputs(
    crate_name: &str,
    crate_dir: &Path,
    patterns: &[String],
    file_hasher: &FileHasher<'_>,
) -> Option<String> {
    // An explicit empty list is the opt-out: byte-identical to no `kache.toml`.
    if patterns.is_empty() {
        return None;
    }

    // Normalize the declared patterns. Out-of-crate patterns (absolute / `..`)
    // are kept (with a portability warning); only a pattern smuggling in the
    // fold separator is skipped.
    let mut normalized: Vec<String> = patterns
        .iter()
        .filter_map(|p| normalize_pattern(crate_name, crate_dir, p))
        .collect();
    normalized.sort();
    normalized.dedup();

    // The author DECLARED inputs (non-empty list) but every pattern was
    // rejected. Collapsing to `None` here would make the key byte-identical to
    // having no `kache.toml` at all — silently re-opening the exact false hit
    // the feature exists to prevent, while the author believes the file is
    // tracked. Fold the raw declared patterns instead: the key is distinct
    // from no-config and any edit to `kache.toml` re-keys.
    if normalized.is_empty() {
        tracing::warn!(
            "[key:{crate_name}] every extra_inputs pattern was rejected; folding the raw \
             declaration so the crate stays distinct from an unconfigured one"
        );
        let mut hasher = blake3::Hasher::new();
        let mut raw: Vec<&String> = patterns.iter().collect();
        raw.sort();
        raw.dedup();
        for p in raw {
            hasher.update(b"extra_input_all_rejected:");
            hasher.update(p.as_bytes());
            hasher.update(b"\x1f");
        }
        return Some(hasher.finalize().to_hex().to_string());
    }

    let mut hasher = blake3::Hasher::new();

    // (1) The declared pattern set itself — so editing `kache.toml` re-keys
    // even when it currently matches zero files.
    for pat in &normalized {
        hasher.update(b"extra_input_pattern:");
        hasher.update(pat.as_bytes());
        hasher.update(b"\x1f");
    }

    // (2) Enumerate the matched files on disk. A per-entry traversal error
    // (e.g. an unreadable subdir) must NOT silently shrink the matched set
    // into a false hit, so failing paths are folded as `glob_error` sentinels
    // — the same fail-safe stance as the per-file `unreadable` sentinel.
    let mut matched: Vec<PathBuf> = Vec::new();
    let mut glob_errors: Vec<String> = Vec::new();
    for pat in &normalized {
        // An absolute pattern is used as-is; a relative one anchors at the
        // crate dir (whose literal bytes are escaped so a `[`/`?` in the path
        // can't be read as a glob metachar — the user's pattern is appended
        // raw).
        let full = if Path::new(pat).is_absolute() {
            pat.clone()
        } else {
            format!(
                "{}/{}",
                glob::Pattern::escape(&crate_dir.to_string_lossy()),
                pat
            )
        };
        // A recursive glob anchored at the filesystem root (`/**`) walks the
        // entire filesystem on every compile — almost never intended, and the
        // walk itself is the cost, so flag it before globbing.
        if walks_filesystem_root(&full) {
            tracing::warn!(
                "[key:{crate_name}] extra_inputs pattern {pat:?} walks from the filesystem \
                 root — this enumerates the entire filesystem on every compile; narrow it"
            );
        }
        let entries = match glob::glob(&full) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("[key:{crate_name}] bad extra_inputs glob {pat:?}: {e}");
                continue;
            }
        };
        for entry in entries {
            match entry {
                Ok(p) if p.is_file() => matched.push(p),
                Ok(_) => {}
                Err(e) => {
                    let rel = crate_relative_path(crate_dir, e.path());
                    tracing::warn!(
                        "[key:{crate_name}] extra_inputs enumeration error at {rel:?}: {e}"
                    );
                    glob_errors.push(rel);
                }
            }
        }
    }
    matched.sort();
    matched.dedup();

    // Empirical breadth guard: catches an over-broad glob regardless of shape
    // (an absolute `/**`, or a relative `**/*` that accidentally spans
    // `target/`). Over-folding is fail-safe, but it busts the key on every
    // change and re-walks a large tree each compile, so surface it.
    if matched.len() > OVER_BROAD_FILE_WARN {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs matched {} files — likely an over-broad glob; \
             it busts the key on every change and walks a large tree each compile. Narrow it.",
            matched.len()
        );
    }

    // Warm the memoized hasher (daemon-backed) in one batch.
    let paths: Vec<&Path> = matched.iter().map(|p| p.as_path()).collect();
    file_hasher.prefetch(&paths);

    // (3) Fold each readable file as `<crate-relative path>=<content hash>`.
    // The PATH is part of the key, not just the content multiset, so swapping
    // two matched files' contents — or a content-following rename — re-keys.
    // That binding is load-bearing for the inputs this feature targets (sqlx
    // migration order, several `include_str!` sites under one glob): the same
    // bytes at a different filename compile differently. The path is
    // crate-relative with `/` separators, so a worktree move or cross-machine
    // restore is still stable. Unreadable files and enumeration errors fold
    // path-only sentinels under distinct labels that can never alias "absent"
    // or a readable file. All three lists are sorted so the fold order is
    // content-determined, never FS-order dependent.
    let mut readable: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    for path in &matched {
        let rel = crate_relative_path(crate_dir, path);
        match file_hasher.hash(path) {
            Ok(h) => readable.push(format!("{rel}={h}")),
            Err(e) => {
                tracing::warn!("[key:{crate_name}] extra_input unreadable {rel:?}: {e}");
                unreadable.push(rel);
            }
        }
    }
    readable.sort();
    unreadable.sort();
    glob_errors.sort();
    glob_errors.dedup();
    for entry in &readable {
        hasher.update(b"extra_input:");
        hasher.update(entry.as_bytes());
        hasher.update(b"\x1f");
    }
    for u in &unreadable {
        hasher.update(b"extra_input_unreadable:");
        hasher.update(u.as_bytes());
        hasher.update(b"\x1f");
    }
    for g in &glob_errors {
        hasher.update(b"extra_input_glob_error:");
        hasher.update(g.as_bytes());
        hasher.update(b"\x1f");
    }

    // The byte total is a debug-only convenience; don't pay a second `stat`
    // per matched file unless DEBUG is actually being recorded.
    if tracing::enabled!(tracing::Level::DEBUG) {
        let total_bytes: u64 = matched
            .iter()
            .filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len()))
            .sum();
        tracing::debug!(
            "[key:{crate_name}] extra_inputs: {} pattern(s), {} file(s), {} unreadable, \
             {} glob-error(s), {} bytes",
            normalized.len(),
            readable.len(),
            unreadable.len(),
            glob_errors.len(),
            total_bytes
        );
    }

    // A single info!-level confirmation so a default-verbosity build shows the
    // feature is live for this crate (and `why-miss` guidance is actionable).
    tracing::info!(
        "[key:{crate_name}] extra_inputs: folded {} file(s) from {} pattern(s)",
        readable.len(),
        normalized.len()
    );

    Some(hasher.finalize().to_hex().to_string())
}

/// A matched file's path as a stable, crate-relative, `/`-separated string for
/// folding into the key. Crate-relative so a worktree move / cross-machine
/// restore doesn't change it; `/`-normalized so the same layout keys
/// identically across platforms. A path that isn't under `crate_dir` (only
/// reachable via a symlink the author placed inside the crate) falls back to
/// its lossy form — it still folds, it just isn't relocation-stable.
fn crate_relative_path(crate_dir: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(crate_dir).unwrap_or(path);
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// Expand (`$ENV`/`~`) a declared pattern, then reshape directory-style
/// patterns so they actually enumerate. Returns `None` (warn + skip) only for a
/// pattern carrying the fold separator — the one genuinely invalid case.
/// Out-of-crate patterns (absolute / `..`) are *folded*, with a portability
/// warning: reaching outside the crate is the author's explicit, fail-safe
/// choice, but it makes the key host-/layout-specific.
fn normalize_pattern(crate_name: &str, crate_dir: &Path, pattern: &str) -> Option<String> {
    let (normalized, unset_vars) = crate::config::expand_exclude_pattern_collecting(pattern);

    // An unset `$VAR` in a pattern is the one failure mode the rest of this
    // module handles loudly but this path used to swallow: the reference stays a
    // literal, matches nothing, and folds a pattern-set-only key that replays
    // regardless of the files the author meant to track. Warn so the missing
    // var is visible instead of presenting as a clean (but wrong) cache hit.
    if !unset_vars.is_empty() {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs pattern {pattern:?} references unset env var(s) \
             {unset_vars:?}; they stay literal and match nothing — set the var(s) or remove the \
             reference, otherwise this folds a replayable matches-nothing key"
        );
    }

    // A `\x1f` (the fold separator) in a glob is never legitimate and would let
    // a crafted pattern cross the pattern/hash section boundary in the digest.
    // Reject it rather than fold an ambiguous byte stream.
    if normalized.contains('\x1f') {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs pattern {pattern:?} contains a control \
             separator (\\x1f); skipping"
        );
        return None;
    }

    // A pattern may deliberately reach outside the crate — an absolute path to a
    // machine-specific file, or `..` to a tree shared above the crate. That is
    // the author's explicit local choice and stays fail-safe (folding more
    // inputs can only cost an extra miss, never a wrong artifact); it is not
    // kache's place to forbid a real build dependency. But the key then becomes
    // host-/layout-specific, which reduces cross-machine and cross-worktree
    // cache sharing, so flag it rather than silently degrade portability.
    let as_path = Path::new(&normalized);
    if as_path.is_absolute()
        || as_path
            .components()
            .any(|c| matches!(c, Component::ParentDir))
    {
        tracing::warn!(
            "[key:{crate_name}] extra_inputs pattern {pattern:?} reaches outside the crate \
             (absolute or `..`); folding it anyway, but this crate's key is now \
             host-/layout-specific and won't share across machines or worktrees"
        );
    }

    // A bare or trailing-slash directory matches *nothing* under glob, which
    // would silently re-introduce a false hit. Reshape so the canonical
    // examples enumerate: `.sqlx/` and `.sqlx` → `.sqlx/**/*`.
    //
    // When the (de-slashed) pattern names a real on-disk directory it is a
    // LITERAL path, not a user-authored glob, so escape its metachars before
    // appending `/**/*`. Otherwise a directory literally named `data[1]` would
    // be read as a char class, enumerate nothing, and silently drop its files
    // (a false hit). Free-form globs (`.sqlx/**/*.json`) take the `else` arm
    // untouched, so the user's own `*`/`**`/`[…]` keep working.
    //
    // Matching is byte-literal (glob), so a pattern and an on-disk name that
    // differ only by Unicode normalization (NFC vs NFD) won't match. We do NOT
    // force-normalize the pattern: that can only break a match the author's
    // editor already aligned with the on-disk bytes.
    let trimmed = normalized.strip_suffix('/').unwrap_or(&normalized);
    let reshaped = if crate_dir.join(trimmed).is_dir() {
        format!("{}/**/*", glob::Pattern::escape(trimmed))
    } else if normalized.ends_with('/') {
        format!("{normalized}**/*")
    } else {
        normalized
    };
    Some(reshaped)
}

/// True if a glob's literal prefix (the part before the first glob
/// metacharacter) is the filesystem root, so a following `**` would walk the
/// entire filesystem. Detects the `/**` footgun cheaply, before the slow walk.
fn walks_filesystem_root(glob_pattern: &str) -> bool {
    let literal_end = glob_pattern
        .find(['*', '?', '['])
        .unwrap_or(glob_pattern.len());
    // The directory glob starts walking = the literal prefix up to its last
    // separator. No separator → a bare relative stem, never the FS root.
    let Some(slash) = glob_pattern[..literal_end].rfind('/') else {
        return false;
    };
    let base = Path::new(&glob_pattern[..=slash]);
    // `Path::is_absolute()` is false for a bare "/" on Windows (it expects a
    // drive), but "/" is still the current-drive root there and walks a huge
    // tree — treat a leading RootDir as rooted on every platform.
    let rooted = base.is_absolute()
        || matches!(
            base.components().next(),
            Some(std::path::Component::RootDir)
        );
    rooted && base.parent().is_none()
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
    fn out_of_crate_patterns_are_folded_not_rejected() {
        // Reaching outside the crate (absolute / `..`) is the author's explicit,
        // fail-safe choice — folded (with a portability warning), not skipped.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        assert!(normalize_pattern("x", root, "../shared/**").is_some());
        assert!(normalize_pattern("x", root, "/etc/**").is_some());
        assert!(normalize_pattern("x", root, ".sqlx/**/*.json").is_some());
        // The one genuinely-invalid case stays rejected: the fold separator,
        // which could cross the pattern/hash section boundary in the digest.
        assert!(normalize_pattern("x", root, "\u{1f}bad").is_none());
    }

    #[test]
    fn absolute_external_input_folds_and_rekeys() {
        // A pattern may point at a file outside the crate (here a sibling
        // tempdir standing in for a machine-specific path). Its content is
        // folded and an edit re-keys — the key is (correctly) host-specific.
        let ext = tempfile::tempdir().unwrap();
        let ext_file = ext.path().join("shared.json");
        std::fs::write(&ext_file, "v1").unwrap();

        // Forward slashes: backslashes are escape sequences in TOML strings (a
        // raw Windows path would be mis-parsed), and Windows path resolution
        // accepts `/` just fine.
        let toml = format!(
            "extra_inputs = [\"{}\"]",
            ext_file.display().to_string().replace('\\', "/")
        );
        let (_d, src) = crate_fixture(&[("kache.toml", toml.as_str())]);
        let before = dig(&src).expect("absolute external input folds");
        std::fs::write(&ext_file, "v2").unwrap();
        let after = dig(&src).expect("still folds after edit");
        assert_ne!(
            before, after,
            "editing an external declared input must re-key"
        );
    }

    #[test]
    fn walks_filesystem_root_detects_root_globs() {
        assert!(walks_filesystem_root("/**"));
        assert!(walks_filesystem_root("/**/*.json"));
        assert!(!walks_filesystem_root("/usr/**"));
        assert!(!walks_filesystem_root("/home/me/proto/**/*.proto"));
        assert!(!walks_filesystem_root("proto/**/*.proto")); // relative, crate-anchored
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

    #[test]
    fn content_swap_between_matched_files_rekeys() {
        // CARDINAL-SIN GUARD. Two files matched by one glob; swapping their
        // CONTENTS (same filenames, identical content multiset) must re-key —
        // the filename->content binding is load-bearing (sqlx migration order,
        // several include_str! sites under one glob). A path-blind content
        // multiset would alias these two states and serve a stale artifact.
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"migrations/**/*.sql\"]"),
            ("migrations/0001_init.sql", "CREATE A;"),
            ("migrations/0002_add.sql", "CREATE B;"),
        ]);
        let before = dig(&src).expect("two matched files fold a digest");
        std::fs::write(d.path().join("migrations/0001_init.sql"), "CREATE B;").unwrap();
        std::fs::write(d.path().join("migrations/0002_add.sql"), "CREATE A;").unwrap();
        let after = dig(&src).expect("still folds after swap");
        assert_ne!(
            before, after,
            "content swap between matched files must re-key (false-hit guard)"
        );
    }

    #[test]
    fn metachar_dir_name_still_enumerates() {
        // A directory literally named `gen[1]`, declared as a bare dir, must
        // enumerate its files: the reshape escapes the literal `[`/`]` so glob
        // doesn't read them as a char class and silently fold nothing.
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"gen[1]\"]"),
            ("gen[1]/data.bin", "v1"),
        ]);
        let before = dig(&src).expect("metachar-named dir folds its files");
        std::fs::write(d.path().join("gen[1]/data.bin"), "v2").unwrap();
        let after = dig(&src).expect("still folds");
        assert_ne!(
            before, after,
            "a file inside a metachar-named dir must re-key (false-hit guard)"
        );
    }

    #[test]
    fn all_rejected_patterns_fold_distinct_from_no_config_and_rekey() {
        // A non-empty declaration whose patterns are ALL rejected (the `\x1f`
        // separator is the only rejection now) must NOT collapse to the
        // unconfigured key (None) — that silently re-opens the false hit — and
        // editing the declaration must re-key.
        let (d, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"\\u001Fa\"]")]);
        let folded = dig(&src).expect("all-rejected declaration still folds, never None");

        let (_n, none_src) = crate_fixture(&[]);
        assert!(
            dig(&none_src).is_none(),
            "no-config baseline is None (opt-out)"
        );

        std::fs::write(d.path().join("kache.toml"), "extra_inputs = [\"\\u001Fb\"]").unwrap();
        let after = dig(&src).expect("still folds after edit");
        assert_ne!(folded, after, "editing a rejected declaration must re-key");
    }

    #[test]
    fn empty_list_stays_distinct_from_all_rejected() {
        // `extra_inputs = []` is the explicit opt-out (None, byte-identical to
        // no file); a non-empty all-rejected list (`\x1f`) folds Some. They
        // must differ.
        let (_e, empty) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        let (_r, rejected) = crate_fixture(&[("kache.toml", "extra_inputs = [\"\\u001Fx\"]")]);
        assert_eq!(dig(&empty), None);
        assert!(dig(&rejected).is_some());
    }

    #[test]
    fn control_separator_in_pattern_is_rejected() {
        // A `\x1f` (the fold separator) in a pattern can't be folded
        // unambiguously, so normalize_pattern drops it.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        assert!(normalize_pattern("x", root, "a\u{1f}b").is_none());
        assert!(normalize_pattern("x", root, ".sqlx/**/*.json").is_some());
    }

    #[test]
    fn non_utf8_config_folds_opaque_and_rekeys() {
        // A binary/corrupt kache.toml must fold opaque (never silently ignored
        // as if absent), and any edit must re-key.
        let (d, src) = crate_fixture(&[]);
        std::fs::write(d.path().join("kache.toml"), b"\xff\xfe extra_inputs").unwrap();
        let before = dig(&src).expect("non-utf8 config folds opaque, never None");
        std::fs::write(d.path().join("kache.toml"), b"\xff\xfe extra_input").unwrap();
        let after = dig(&src).expect("still folds");
        assert_ne!(before, after);
    }

    #[test]
    fn invalid_glob_pattern_does_not_abort_other_patterns() {
        // `a[b` survives normalization but is an invalid glob; it must warn +
        // skip without dropping a sibling valid pattern's files.
        let (d, src) = crate_fixture(&[
            (
                "kache.toml",
                "extra_inputs = [\"a[b\", \".sqlx/**/*.json\"]",
            ),
            (".sqlx/q.json", "v1"),
        ]);
        let before = dig(&src).expect("valid pattern still folds despite a bad sibling");
        std::fs::write(d.path().join(".sqlx/q.json"), "v2").unwrap();
        let after = dig(&src).unwrap();
        assert_ne!(before, after, "the valid pattern's file still re-keys");
    }

    #[test]
    fn duplicate_pattern_folds_same_as_single() {
        // pattern-level dedup: a repeated pattern must not change the digest.
        let (_d1, s1) = crate_fixture(&[
            (
                "kache.toml",
                "extra_inputs = [\".sqlx/**/*\", \".sqlx/**/*\"]",
            ),
            (".sqlx/q.json", "v1"),
        ]);
        let (_d2, s2) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\".sqlx/**/*\"]"),
            (".sqlx/q.json", "v1"),
        ]);
        assert_eq!(dig(&s1), dig(&s2));
    }

    #[test]
    fn overlapping_patterns_are_order_independent() {
        // Two distinct patterns matching the same file: declaration order must
        // not change the digest (sorted pattern set + deduped matched files).
        let files: &[(&str, &str)] = &[
            (
                "kache.toml",
                "extra_inputs = [\".sqlx/**/*\", \".sqlx/q.json\"]",
            ),
            (".sqlx/q.json", "v1"),
        ];
        let files_rev: &[(&str, &str)] = &[
            (
                "kache.toml",
                "extra_inputs = [\".sqlx/q.json\", \".sqlx/**/*\"]",
            ),
            (".sqlx/q.json", "v1"),
        ];
        let (_d1, s1) = crate_fixture(files);
        let (_d2, s2) = crate_fixture(files_rev);
        assert_eq!(dig(&s1), dig(&s2));
    }

    #[test]
    fn cc_style_c_source_folds_extra_inputs() {
        // The cc seam passes a C source path; crate-dir resolution and folding
        // are family-agnostic, so a co-located kache.toml applies to a cc-rs
        // crate (e.g. a generated header) just as to a Rust one.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        std::fs::write(root.join("kache.toml"), "extra_inputs = [\"include/*.h\"]").unwrap();
        std::fs::create_dir_all(root.join("include")).unwrap();
        std::fs::write(root.join("include/api.h"), "v1").unwrap();
        let csrc = root.join("src/ffi.c");
        std::fs::create_dir_all(csrc.parent().unwrap()).unwrap();
        std::fs::write(&csrc, "/* c */\n").unwrap();

        let fh = FileHasher::new();
        let before = digest(Some(&csrc), "x", true, &fh).expect("C source folds extra inputs");
        std::fs::write(root.join("include/api.h"), "v2").unwrap();
        let fh2 = FileHasher::new();
        let after = digest(Some(&csrc), "x", true, &fh2).unwrap();
        assert_ne!(
            before, after,
            "editing a declared header must re-key the cc crate"
        );
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_file_folds_sentinel_distinct_from_absent() {
        use std::os::unix::fs::PermissionsExt;
        let (d, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/**/*\"]"),
            ("data/secret.bin", "v1"),
        ]);
        let readable = dig(&src).expect("folds the readable file");

        let p = d.path().join("data/secret.bin");
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000)).unwrap();
        // Running as root defeats chmod 000 — skip rather than assert falsely.
        if std::fs::read(&p).is_ok() {
            return;
        }
        let unreadable = dig(&src).expect("unreadable file still folds a sentinel");
        assert_ne!(readable, unreadable, "unreadable must differ from readable");

        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::remove_file(&p).unwrap();
        let absent = dig(&src).expect("zero matches still folds the pattern set");
        assert_ne!(
            unreadable, absent,
            "unreadable must not alias absent (false-hit guard)"
        );
    }
}
