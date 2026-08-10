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
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};
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

/// A declaration with more distinct Cargo directory dependencies than this is
/// almost certainly being used as a generated watch list rather than a small
/// set of input globs. Keep the consumer fingerprint bounded.
const MAX_WATCH_PATHS: usize = 256;

/// Whether this crate has a co-located extra-input declaration.
///
/// Adaptive incremental mode cannot cheaply validate those untracked inputs
/// before its early path, so their presence disables and clears adaptation for
/// the unit. I/O errors fail closed as "declared".
pub(crate) fn declared(source_file: Option<&Path>) -> bool {
    let Some(crate_dir) = source_file.and_then(crate_dir_from_source) else {
        return false;
    };
    match std::fs::symlink_metadata(crate_dir.join(COLOCATED_NAME)) {
        Ok(_) => true,
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

/// Minimal schema for `<crate-dir>/kache.toml`. `deny_unknown_fields` makes a
/// stray `remote`/`local_store`/etc. a loud parse error rather than a
/// silently-honored crate-granularity setting — this file is *only* for
/// extra inputs.
#[derive(serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct ColocatedConfig {
    extra_inputs: Vec<String>,
}

/// One invocation's fully-resolved extra-input declaration.
///
/// The wrapper resolves this once, passes [`Self::digest`] into key
/// computation, then uses [`Self::merge_into_dep_info`] on the consumer-facing
/// dep-info after either compilation or restore. Paths are those of the
/// current consumer worktree; nothing in this value comes from a cached
/// producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtraInputsSnapshot {
    config_path: PathBuf,
    normalized_patterns: Vec<String>,
    /// `None` for an explicit empty declaration: Cargo still watches the
    /// config so later activation is visible, while the cache key remains
    /// byte-identical to an unconfigured crate.
    digest: Option<String>,
    matched_files: Vec<PathBuf>,
    /// Narrow directories Cargo may recursively fingerprint to notice a glob
    /// addition/deletion, or creation of a currently-missing literal input.
    watch_paths: Vec<PathBuf>,
    /// Metadata observed after resolution. This is deliberately outside the
    /// cache-key digest: it detects ABA races (v1 -> transient -> v1, or a
    /// glob member added then removed) before Cargo accepts compiler output.
    observations: Vec<InputObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InputObservation {
    path: PathBuf,
    size: u64,
    mtime_ns: i64,
    ctime_ns: i64,
    inode: i64,
}

fn observe_dependency(path: &Path) -> Result<InputObservation> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("reading extra_inputs metadata for {}", path.display()))?;
    Ok(InputObservation {
        path: path.to_path_buf(),
        size: metadata.len(),
        mtime_ns: crate::cache_key::metadata_mtime_ns(&metadata),
        ctime_ns: crate::cache_key::metadata_ctime_ns(&metadata),
        inode: crate::cache_key::metadata_inode(&metadata),
    })
}

impl ExtraInputsSnapshot {
    /// Resolve a crate's active declaration exactly once.
    ///
    /// `Ok(None)` retains the old no-op cases: non-primary invocation or no
    /// co-located config. An explicit empty declaration returns a config-only
    /// snapshot so Cargo can observe later activation without changing the
    /// cache key. Invalid declarations and unsafe watches fail closed.
    pub(crate) fn resolve(
        source_file: Option<&Path>,
        crate_name: &str,
        is_primary: bool,
        file_hasher: &FileHasher<'_>,
    ) -> Result<Option<Self>> {
        resolve_snapshot(source_file, crate_name, is_primary, file_hasher, true)
    }

    pub(crate) fn digest(&self) -> Option<&str> {
        self.digest.as_deref()
    }

    /// Merge config, matched files, and narrow directory watches into the
    /// first dependency rule of a rustc/Cargo dep-info file.
    ///
    /// Only consumer-facing dep-info should be passed here (after a cache
    /// store/restore boundary), so a restored file never retains a producer's
    /// absolute worktree paths. Current-consumer paths are emitted absolute so
    /// Cargo resolves them correctly even when rustc runs from a workspace root.
    pub(crate) fn merge_into_dep_info(&self, dep_info_path: &Path) -> Result<()> {
        merge_snapshot_into_dep_info(self, dep_info_path)
    }

    /// Pure form used by cache restore: complete and validate the expanded
    /// consumer bytes before any cached artifact is materialized or reported
    /// as a hit.
    pub(crate) fn merge_dep_info_content(&self, content: &str) -> Result<String> {
        merge_snapshot_dep_info_content(self, content)
    }
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
    // Compatibility API for C/C++ and any out-of-tree callers. It deliberately
    // does not enforce Cargo watch-root policy: that policy is Rust/Cargo-only,
    // while the digest bytes must retain their existing semantics. New Rust
    // code should resolve `ExtraInputsSnapshot` and propagate its `Result`.
    match resolve_snapshot(source_file, crate_name, is_primary, file_hasher, false) {
        Ok(snapshot) => snapshot.and_then(|snapshot| snapshot.digest),
        Err(error) => {
            tracing::warn!("[key:{crate_name}] failed to resolve extra_inputs: {error:#}");
            None
        }
    }
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

#[derive(Debug, Clone)]
struct NormalizedInputPattern {
    glob: String,
    watch: WatchIntent,
}

#[derive(Debug, Clone)]
enum WatchIntent {
    /// A matched literal file catches edits/deletion itself. If currently
    /// missing, Cargo watches its nearest narrow existing parent directory.
    Literal(PathBuf),
    /// Cargo recursively fingerprints this literal root directory so a glob
    /// catches edits, additions, and deletions below it.
    DirectoryRoot(PathBuf),
}

fn resolve_snapshot(
    source_file: Option<&Path>,
    crate_name: &str,
    is_primary: bool,
    file_hasher: &FileHasher<'_>,
    strict_watches: bool,
) -> Result<Option<ExtraInputsSnapshot>> {
    if !is_primary {
        return Ok(None);
    }
    let Some(source_file) = source_file else {
        return Ok(None);
    };
    let Some(crate_dir) = crate_dir_from_source(source_file) else {
        return Ok(None);
    };
    let crate_dir = lexical_normalize(&crate_dir);
    let config_path = crate_dir.join(COLOCATED_NAME);

    if strict_watches {
        match std::fs::symlink_metadata(&config_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                anyhow::bail!(
                    "active extra_inputs config {} is a symlink; Cargo canonicalizes dep-info and \
                     cannot notice that link being retargeted safely",
                    config_path.display()
                );
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "reading active extra_inputs config metadata {}",
                        config_path.display()
                    )
                });
            }
        }
    }

    let raw = match std::fs::read(&config_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) if !strict_watches => {
            // Preserve the legacy digest API's unreadable-config no-op. The
            // strict snapshot API fails closed instead.
            tracing::warn!(
                "[key:{crate_name}] cannot read {}: {error}",
                config_path.display()
            );
            return Ok(None);
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "reading active extra_inputs config {}",
                    config_path.display()
                )
            });
        }
    };
    if strict_watches && windows_path_uses_device_namespace(&crate_dir) {
        anyhow::bail!(
            "active extra_inputs cannot enumerate a crate through a Windows verbatim/device \
             namespace path safely; use the ordinary drive or UNC spelling"
        );
    }
    if strict_watches {
        // The parsed pattern set, rather than formatting/comments, defines the
        // key. Still observe the config through the guarded hasher so a rewrite
        // racing this snapshot suppresses cache publication.
        file_hasher.hash(&config_path).with_context(|| {
            format!(
                "hashing active extra_inputs config {}",
                config_path.display()
            )
        })?;
    }

    let opaque_snapshot = |raw: &[u8]| ExtraInputsSnapshot {
        config_path: config_path.clone(),
        normalized_patterns: Vec::new(),
        digest: Some(unparseable_digest(crate_name, &config_path, raw)),
        matched_files: Vec::new(),
        watch_paths: Vec::new(),
        observations: Vec::new(),
    };
    let text = match std::str::from_utf8(&raw) {
        Ok(text) => text,
        Err(_) if !strict_watches => return Ok(Some(opaque_snapshot(&raw))),
        Err(error) => {
            return Err(error).with_context(|| {
                format!(
                    "active extra_inputs config {} must be valid UTF-8",
                    config_path.display()
                )
            });
        }
    };
    let config: ColocatedConfig = match toml::from_str(text) {
        Ok(config) => config,
        Err(error) if strict_watches => {
            return Err(error).with_context(|| {
                format!(
                    "parsing active extra_inputs config {}",
                    config_path.display()
                )
            });
        }
        Err(error) => {
            tracing::warn!(
                "[key:{crate_name}] {} is invalid ({error}); folding it as an opaque \
                 input so the crate rebuilds until fixed",
                config_path.display()
            );
            return Ok(Some(opaque_snapshot(&raw)));
        }
    };

    resolve_declared_inputs(
        crate_name,
        crate_dir,
        config_path,
        &config.extra_inputs,
        file_hasher,
        strict_watches,
    )
}

/// Fold the declared pattern set and the content hashes of every matched file
/// into a single hex digest. Returns `None` only for the genuinely-empty
/// declaration (`extra_inputs = []`), an explicit opt-out that must stay
/// byte-identical to having no `kache.toml`. A non-empty declaration always
/// folds *something* — even if every pattern is rejected — so it can never
/// collapse back to the no-config key.
fn resolve_declared_inputs(
    crate_name: &str,
    crate_dir: PathBuf,
    config_path: PathBuf,
    patterns: &[String],
    file_hasher: &FileHasher<'_>,
    strict_watches: bool,
) -> Result<Option<ExtraInputsSnapshot>> {
    // An explicit empty list is the opt-out: byte-identical to no `kache.toml`.
    if patterns.is_empty() {
        return if strict_watches {
            let observations = vec![observe_dependency(&config_path)?];
            Ok(Some(ExtraInputsSnapshot {
                config_path,
                normalized_patterns: Vec::new(),
                digest: None,
                matched_files: Vec::new(),
                watch_paths: Vec::new(),
                observations,
            }))
        } else {
            Ok(None)
        };
    }

    // Normalize the declared patterns. Out-of-crate patterns (absolute / `..`)
    // are kept (with a portability warning); only a pattern smuggling in the
    // fold separator is skipped.
    let mut by_glob = BTreeMap::new();
    let mut rejected_patterns = Vec::new();
    for pattern in patterns {
        if strict_watches && pattern_uses_dynamic_expansion(pattern) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} uses `$ENV` or `~` expansion; Cargo \
                 cannot notice that expansion changing, so use a stable literal path"
            );
        }
        if strict_watches && windows_pattern_has_ambiguous_root(Path::new(pattern)) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} uses a Windows rooted-without-drive or \
                 drive-relative path; use a fully-qualified absolute path or a crate-relative path"
            );
        }
        if strict_watches && windows_path_uses_device_namespace(Path::new(pattern)) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} uses a Windows verbatim/device namespace \
                 that glob enumeration cannot track safely; use an ordinary drive or UNC path"
            );
        }
        if strict_watches && parent_traversal_follows_glob(pattern) {
            anyhow::bail!(
                "active extra_inputs pattern {pattern:?} traverses `..` after a wildcard; \
                 Cargo cannot derive a bounded watch root that sees new matches"
            );
        }
        if let Some(normalized) = normalize_pattern_info(crate_name, &crate_dir, pattern) {
            by_glob.entry(normalized.glob.clone()).or_insert(normalized);
        } else {
            rejected_patterns.push(pattern);
        }
    }
    if strict_watches && !rejected_patterns.is_empty() {
        anyhow::bail!(
            "active extra_inputs contains {} invalid pattern(s); fix the declaration before Cargo freshness can be completed safely",
            rejected_patterns.len()
        );
    }
    let normalized: Vec<NormalizedInputPattern> = by_glob.into_values().collect();
    let normalized_patterns: Vec<String> = normalized
        .iter()
        .map(|pattern| pattern.glob.clone())
        .collect();

    // The author DECLARED inputs (non-empty list) but every pattern was
    // rejected. Collapsing to `None` here would make the key byte-identical to
    // having no `kache.toml` at all — silently re-opening the exact false hit
    // the feature exists to prevent, while the author believes the file is
    // tracked. Fold the raw declared patterns instead: the key is distinct
    // from no-config and any edit to `kache.toml` re-keys.
    if normalized_patterns.is_empty() {
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
        return Ok(Some(ExtraInputsSnapshot {
            config_path,
            normalized_patterns,
            digest: Some(hasher.finalize().to_hex().to_string()),
            matched_files: Vec::new(),
            watch_paths: Vec::new(),
            observations: Vec::new(),
        }));
    }

    // Reject broad recursive directory dependencies before globbing. Cargo
    // fingerprints a directory recursively; injecting the crate root or a
    // filesystem root would turn every build into an unbounded tree walk.
    let watch_paths = match resolve_watch_paths(&crate_dir, &normalized) {
        Ok(paths) => paths,
        Err(error) if !strict_watches => {
            tracing::warn!("[key:{crate_name}] unsafe Cargo extra_inputs watch: {error:#}");
            Vec::new()
        }
        Err(error) => return Err(error),
    };
    if strict_watches {
        for watch in &watch_paths {
            inspect_safe_watch_tree(watch).with_context(|| {
                format!(
                    "checking active extra_inputs watch {} for byte-preserving enumeration",
                    watch.display()
                )
            })?;
        }
    }

    let mut hasher = blake3::Hasher::new();

    // (1) The declared pattern set itself — so editing `kache.toml` re-keys
    // even when it currently matches zero files.
    for pat in &normalized_patterns {
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
    for normalized in &normalized {
        let pat = &normalized.glob;
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
            Err(error) if strict_watches => {
                return Err(error).with_context(|| {
                    format!("parsing active extra_inputs glob {pat:?} for {crate_name}")
                });
            }
            Err(e) => {
                tracing::warn!("[key:{crate_name}] bad extra_inputs glob {pat:?}: {e}");
                continue;
            }
        };
        for entry in entries {
            match entry {
                Ok(p) if p.is_file() => matched.push(p),
                Ok(_) => {}
                Err(error) if strict_watches => {
                    return Err(error).with_context(|| {
                        format!("enumerating active extra_inputs glob {pat:?} for {crate_name}")
                    });
                }
                Err(e) => {
                    let rel = crate_relative_path(&crate_dir, e.path());
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

    // Recheck after globbing. A symlink inserted between the first preflight
    // and enumeration must not let `glob` follow an unbounded/non-UTF-8 tree
    // that was absent from the key's safety check.
    let observed_watch_dirs = if strict_watches {
        let mut directories = BTreeSet::new();
        for watch in &watch_paths {
            directories.extend(inspect_safe_watch_tree(watch).with_context(|| {
                format!(
                    "rechecking active extra_inputs watch {} after enumeration",
                    watch.display()
                )
            })?);
        }
        directories
    } else {
        BTreeSet::new()
    };

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
        let rel = crate_relative_path(&crate_dir, path);
        if strict_watches && rel.contains('\x1f') {
            anyhow::bail!(
                "active extra_inputs dependency {} contains the cache-key control separator; \
                 rename it before the declaration can be keyed unambiguously",
                path.display()
            );
        }
        match file_hasher.hash(path) {
            Ok(h) => readable.push(format!("{rel}={h}")),
            Err(error) if strict_watches => {
                return Err(error).with_context(|| {
                    format!(
                        "hashing active extra_inputs dependency {} for {crate_name}",
                        path.display()
                    )
                });
            }
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

    let dependencies: BTreeSet<PathBuf> = std::iter::once(config_path.clone())
        .chain(matched.iter().cloned())
        .chain(watch_paths.iter().cloned())
        .collect();
    if strict_watches {
        for dependency in &dependencies {
            if let Some(symlink) = first_symlink_below_common(&crate_dir, dependency) {
                anyhow::bail!(
                    "active extra_inputs dependency {} crosses symlink {}; Cargo canonicalizes \
                     dep-info and cannot notice that link being retargeted safely",
                    dependency.display(),
                    symlink.display()
                );
            }
        }
    }
    let observation_paths: BTreeSet<PathBuf> = std::iter::once(config_path.clone())
        .chain(matched.iter().cloned())
        .chain(observed_watch_dirs)
        .collect();
    let observations = if strict_watches {
        observation_paths
            .iter()
            .map(|path| observe_dependency(path))
            .collect::<Result<Vec<_>>>()?
    } else {
        Vec::new()
    };

    Ok(Some(ExtraInputsSnapshot {
        config_path,
        normalized_patterns,
        digest: Some(hasher.finalize().to_hex().to_string()),
        matched_files: matched,
        watch_paths,
        observations,
    }))
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
#[cfg(test)]
fn normalize_pattern(crate_name: &str, crate_dir: &Path, pattern: &str) -> Option<String> {
    normalize_pattern_info(crate_name, crate_dir, pattern).map(|pattern| pattern.glob)
}

fn normalize_pattern_info(
    crate_name: &str,
    crate_dir: &Path,
    pattern: &str,
) -> Option<NormalizedInputPattern> {
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
    let literal_path = anchor_input_path(crate_dir, Path::new(trimmed));
    let (glob, watch) = if literal_path.is_dir() {
        (
            format!("{}/**/*", glob::Pattern::escape(trimmed)),
            WatchIntent::DirectoryRoot(literal_path),
        )
    } else if normalized.ends_with('/') {
        (
            format!("{normalized}**/*"),
            WatchIntent::DirectoryRoot(literal_path),
        )
    } else if normalized.contains(['*', '?', '[']) {
        let root = literal_glob_root(&normalized);
        (
            normalized,
            WatchIntent::DirectoryRoot(anchor_input_path(crate_dir, &root)),
        )
    } else {
        (normalized, WatchIntent::Literal(literal_path))
    };
    Some(NormalizedInputPattern { glob, watch })
}

fn pattern_uses_dynamic_expansion(pattern: &str) -> bool {
    if pattern == "~" || pattern.starts_with("~/") {
        return true;
    }
    let mut chars = pattern.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '$'
            && chars
                .peek()
                .is_some_and(|next| *next == '{' || *next == '_' || next.is_ascii_alphanumeric())
        {
            return true;
        }
    }
    false
}

#[cfg(windows)]
fn windows_pattern_has_ambiguous_root(path: &Path) -> bool {
    let has_prefix = matches!(path.components().next(), Some(Component::Prefix(_)));
    !path.is_absolute() && (path.has_root() || has_prefix)
}

#[cfg(not(windows))]
fn windows_pattern_has_ambiguous_root(_path: &Path) -> bool {
    false
}

#[cfg(windows)]
fn windows_path_uses_device_namespace(path: &Path) -> bool {
    use std::path::Prefix;

    matches!(
        path.components().next(),
        Some(Component::Prefix(prefix))
            if matches!(
                prefix.kind(),
                Prefix::Verbatim(_)
                    | Prefix::VerbatimUNC(_, _)
                    | Prefix::VerbatimDisk(_)
                    | Prefix::DeviceNS(_)
            )
    )
}

#[cfg(not(windows))]
fn windows_path_uses_device_namespace(_path: &Path) -> bool {
    false
}

fn parent_traversal_follows_glob(pattern: &str) -> bool {
    let mut saw_glob = false;
    for component in Path::new(pattern).components() {
        match component {
            Component::ParentDir if saw_glob => return true,
            Component::Normal(text) => {
                let text = text.to_string_lossy();
                saw_glob |= text.contains(['*', '?', '[']);
            }
            _ => {}
        }
    }
    false
}

/// Literal directory prefix before the first component carrying glob syntax.
/// A pattern such as `migrations/**/*.sql` yields `migrations`; `**/*.sql`
/// yields an empty relative path, which anchors to the crate root and is
/// rejected before enumeration.
fn literal_glob_root(pattern: &str) -> PathBuf {
    let mut root = PathBuf::new();
    for component in Path::new(pattern).components() {
        let text = component.as_os_str().to_string_lossy();
        if text.contains(['*', '?', '[']) {
            break;
        }
        root.push(component.as_os_str());
    }
    root
}

fn anchor_input_path(crate_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        lexical_normalize(path)
    } else {
        lexical_normalize(&crate_dir.join(path))
    }
}

/// Normalize `.`/`..` without requiring the path to exist or resolving
/// symlinks. Missing literal inputs still need a stable parent watch.
fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() && !normalized.has_root() {
                    normalized.push(component.as_os_str());
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

/// Find a user-controlled symlink component between the crate and one of its
/// declared dependencies. Cargo canonicalizes dep-info paths, so watching only
/// the symlink target would miss a later retarget of the link itself.
fn first_symlink_below_common(crate_dir: &Path, dependency: &Path) -> Option<PathBuf> {
    let crate_components: Vec<_> = crate_dir.components().collect();
    let dependency_components: Vec<_> = dependency.components().collect();
    let common_len = crate_components
        .iter()
        .zip(&dependency_components)
        .take_while(|(left, right)| left == right)
        .count();
    let mut probe = PathBuf::new();
    for component in &dependency_components[..common_len] {
        probe.push(component.as_os_str());
    }
    for component in &dependency_components[common_len..] {
        probe.push(component.as_os_str());
        if std::fs::symlink_metadata(&probe).is_ok_and(|metadata| metadata.file_type().is_symlink())
        {
            return Some(probe);
        }
    }
    None
}

/// `glob` works through UTF-8 patterns and silently omits non-UTF-8 directory
/// entries on Unix. Preflight each bounded watch tree with `read_dir`, which
/// preserves `OsStr`, and fail closed instead of keying an incomplete set.
fn inspect_safe_watch_tree(root: &Path) -> Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_path_buf()];
    let mut directories = Vec::new();
    while let Some(directory) = pending.pop() {
        directories.push(directory.clone());
        for entry in std::fs::read_dir(&directory)
            .with_context(|| format!("reading directory {}", directory.display()))?
        {
            let entry =
                entry.with_context(|| format!("reading an entry under {}", directory.display()))?;
            if entry.file_name().to_str().is_none() {
                anyhow::bail!(
                    "directory {} contains a non-UTF-8 name that the configured glob cannot enumerate safely",
                    directory.display()
                );
            }
            let file_type = entry
                .file_type()
                .with_context(|| format!("reading file type for {}", entry.path().display()))?;
            if file_type.is_symlink() {
                anyhow::bail!(
                    "directory {} contains symlink {} that Cargo/glob cannot follow with bounded, byte-preserving freshness semantics",
                    directory.display(),
                    entry.path().display()
                );
            }
            if file_type.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    directories.sort();
    directories.dedup();
    Ok(directories)
}

fn nearest_existing_directory(start: &Path) -> Option<PathBuf> {
    let mut candidate = Some(start);
    while let Some(path) = candidate {
        if path.is_dir() {
            return Some(lexical_normalize(path));
        }
        candidate = path.parent();
    }
    None
}

fn is_filesystem_root(path: &Path) -> bool {
    path.has_root() && path.parent().is_none()
}

fn resolve_watch_paths(
    crate_dir: &Path,
    patterns: &[NormalizedInputPattern],
) -> Result<Vec<PathBuf>> {
    let crate_dir = lexical_normalize(crate_dir);
    let canonical_crate_dir =
        std::fs::canonicalize(&crate_dir).unwrap_or_else(|_| crate_dir.clone());
    let mut watches = BTreeSet::new();

    for pattern in patterns {
        // Invalid globs retain their existing digest semantics and are skipped
        // during enumeration; they do not justify a broad directory watch.
        if glob::Pattern::new(&pattern.glob).is_err() {
            continue;
        }
        let start = match &pattern.watch {
            WatchIntent::Literal(path) if path.is_file() => continue,
            WatchIntent::Literal(path) => path.parent(),
            WatchIntent::DirectoryRoot(path) => Some(path.as_path()),
        };
        let Some(start) = start else {
            anyhow::bail!(
                "extra_inputs pattern {:?} has no directory that Cargo can watch; \
                 place the input under a narrow existing directory",
                pattern.glob
            );
        };
        let Some(watch) = nearest_existing_directory(start) else {
            anyhow::bail!(
                "extra_inputs pattern {:?} has no existing directory to watch; \
                 create a narrow parent directory first",
                pattern.glob
            );
        };

        // Watching the crate root, an ancestor of it, or the filesystem root
        // makes Cargo recursively fingerprint an unrelated/unbounded tree.
        // Check the resolved directory too: an in-crate symlink to `/` must
        // not bypass the lexical guard and hand Cargo a filesystem-root scan.
        let canonical_watch = std::fs::canonicalize(&watch).unwrap_or_else(|_| watch.clone());
        if is_filesystem_root(&watch)
            || is_filesystem_root(&canonical_watch)
            || crate_dir.starts_with(&watch)
            || canonical_crate_dir.starts_with(&canonical_watch)
        {
            anyhow::bail!(
                "extra_inputs pattern {:?} would make Cargo recursively watch broad directory {} \
                 (the crate root or an ancestor); add a literal subdirectory to the pattern, or \
                 create a narrow parent directory for a missing literal input",
                pattern.glob,
                watch.display()
            );
        }

        watches.insert(watch);
        if watches.len() > MAX_WATCH_PATHS {
            anyhow::bail!(
                "extra_inputs resolves to more than {MAX_WATCH_PATHS} directory watches; \
                 consolidate patterns under a smaller set of narrow literal roots"
            );
        }
    }

    Ok(watches.into_iter().collect())
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

#[derive(Debug, Clone, Copy)]
struct FirstMakeRule {
    colon: usize,
    insertion: usize,
}

fn merge_snapshot_into_dep_info(
    snapshot: &ExtraInputsSnapshot,
    dep_info_path: &Path,
) -> Result<()> {
    let metadata = std::fs::metadata(dep_info_path).with_context(|| {
        format!(
            "reading required consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })?;
    if !metadata.is_file() {
        anyhow::bail!(
            "required consumer dep-info {} is not a regular file; refusing to return with an \
             incomplete Cargo fingerprint for active extra_inputs",
            dep_info_path.display()
        );
    }
    let original = std::fs::read_to_string(dep_info_path).with_context(|| {
        format!(
            "reading required consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })?;
    let updated = merge_snapshot_dep_info_content(snapshot, &original).with_context(|| {
        format!(
            "malformed consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })?;
    if updated == original {
        return Ok(());
    }
    crate::atomic::atomic_replace(dep_info_path, updated.as_bytes()).with_context(|| {
        format!(
            "atomically updating consumer dep-info {} for active extra_inputs declaration",
            dep_info_path.display()
        )
    })
}

fn merge_snapshot_dep_info_content(
    snapshot: &ExtraInputsSnapshot,
    original: &str,
) -> Result<String> {
    tracing::trace!(
        "merging extra_inputs dep-info: {} normalized pattern(s), {} matched file(s), {} watch path(s)",
        snapshot.normalized_patterns.len(),
        snapshot.matched_files.len(),
        snapshot.watch_paths.len()
    );
    let rule = first_make_dependency_rule(original)?;
    let existing_words = parse_make_words(&original[rule.colon + 1..rule.insertion])
        .context("parsing Cargo dependency rule")?;
    let compiler_cwd = std::env::current_dir()
        .context("resolving compiler working directory for extra_inputs dep-info")?;
    let existing: BTreeSet<PathBuf> = existing_words
        .iter()
        .map(|word| anchor_input_path(&compiler_cwd, Path::new(word)))
        .collect();

    let mut dependency_paths = BTreeSet::new();
    dependency_paths.insert(lexical_normalize(&snapshot.config_path));
    dependency_paths.extend(
        snapshot
            .matched_files
            .iter()
            .map(|path| lexical_normalize(path)),
    );
    dependency_paths.extend(
        snapshot
            .watch_paths
            .iter()
            .map(|path| lexical_normalize(path)),
    );

    let mut additions = Vec::new();
    for path in dependency_paths {
        if existing.contains(&path) {
            continue;
        }
        // Cargo interprets rustc dep-info paths relative to rustc's working
        // directory, which is normally the workspace root rather than the
        // member crate. Absolute consumer paths are unambiguous and are added
        // only after cache storage/restoration, so they never leak producer
        // worktree paths into cached blobs.
        let text = path.to_str().ok_or_else(|| {
            anyhow::anyhow!(
                "extra_inputs dependency {} is not valid UTF-8 and cannot be represented safely \
                 in Cargo dep-info",
                path.display()
            )
        })?;
        additions.push(make_escape_word(text)?);
    }
    additions.sort();
    additions.dedup();
    if additions.is_empty() {
        return Ok(original.to_string());
    }

    let mut updated = String::with_capacity(
        original.len() + additions.iter().map(String::len).sum::<usize>() + additions.len(),
    );
    updated.push_str(&original[..rule.insertion]);
    updated.push(' ');
    updated.push_str(&additions.join(" "));
    updated.push_str(&original[rule.insertion..]);

    Ok(updated)
}

/// Locate the first line Cargo will parse as rustc dep-info. Cargo recognizes
/// the first line containing `: `; drive-letter colons and `#` remain literal.
fn first_make_dependency_rule(input: &str) -> Result<FirstMakeRule> {
    let mut offset = 0usize;
    for line_with_newline in input.split_inclusive('\n') {
        let line = line_with_newline
            .strip_suffix('\n')
            .unwrap_or(line_with_newline);
        let line = line.strip_suffix('\r').unwrap_or(line);
        // Cargo handles rustc's environment records before looking for the
        // dependency rule. An env value may itself contain `: ` and must not
        // be mistaken for the Make target/dependency separator.
        if line.starts_with("# env-dep:") {
            offset += line_with_newline.len();
            continue;
        }
        if let Some(relative_colon) = line.find(": ") {
            let colon = offset + relative_colon;
            let mut insertion = offset + line.len();
            let bytes = input.as_bytes();
            while insertion > colon + 2 && matches!(bytes[insertion - 1], b' ' | b'\t') {
                insertion -= 1;
            }
            return Ok(FirstMakeRule { colon, insertion });
        }
        offset += line_with_newline.len();
    }

    anyhow::bail!("dep-info contains no Make dependency rule")
}

/// Parse dependency words exactly as Cargo 0.98 / Rust 1.97 does: split on
/// whitespace, then join the next token while the current token ends in `\\`.
fn parse_make_words(input: &str) -> Result<Vec<String>> {
    let mut words = Vec::new();
    let mut tokens = input.split_whitespace();
    while let Some(token) = tokens.next() {
        let mut word = token.to_string();
        while word.ends_with('\\') {
            word.pop();
            word.push(' ');
            word.push_str(
                tokens
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("malformed dep-info format, trailing \\"))?,
            );
        }
        words.push(word);
    }
    Ok(words)
}

/// Parse the dependency side of the same rustc dep-info rule Cargo consumes.
///
/// Keep cache-restore validation on the exact grammar used by
/// [`merge_snapshot_dep_info_content`]: Cargo skips leading metadata records
/// and consumes the first line containing `: `.
pub(crate) fn parse_dep_info_dependencies(input: &str) -> Result<Vec<PathBuf>> {
    let rule = first_make_dependency_rule(input)?;
    parse_make_words(&input[rule.colon + 1..rule.insertion])
        .map(|words| words.into_iter().map(PathBuf::from).collect())
}

fn make_escape_word(input: &str) -> Result<String> {
    if input
        .chars()
        .any(|character| character.is_whitespace() && character != ' ')
    {
        anyhow::bail!(
            "extra_inputs dependency contains unsupported whitespace and cannot enter Cargo dep-info"
        );
    }
    if input.ends_with([' ', '\\']) {
        anyhow::bail!(
            "extra_inputs dependency ends in a space or backslash, which Cargo dep-info cannot represent"
        );
    }
    Ok(input.replace(' ', "\\ "))
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
        assert!(!declared(Some(&src)));
    }

    #[test]
    fn colocated_file_disables_early_adaptation_even_when_empty() {
        let (_d, src) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        assert!(declared(Some(&src)));
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
    fn trailing_slash_on_missing_dir_appends_recursive_glob() {
        // A trailing-slash pattern whose de-slashed form is NOT a real on-disk
        // directory takes the plain `{pattern}**/*` reshape (not the literal-dir
        // escape). Covers normalize_pattern's `else if ends_with('/')` arm.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        let reshaped = normalize_pattern("x", root, "ghostdir/").unwrap();
        assert_eq!(reshaped, "ghostdir/**/*");
    }

    #[test]
    fn unset_env_var_pattern_folds_as_literal() {
        // A pattern referencing an unset $VAR stays literal (matches nothing) and
        // is folded with a warning rather than dropped. Covers the unset-var arm.
        let (d, _src) = crate_fixture(&[]);
        let root = d.path();
        let reshaped =
            normalize_pattern("x", root, "$KACHE_DEFINITELY_UNSET_XYZ/data.json").unwrap();
        // The unexpanded literal survives into the folded pattern.
        assert!(
            reshaped.contains("$KACHE_DEFINITELY_UNSET_XYZ"),
            "unset var should stay literal: {reshaped}"
        );
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

    fn snap(src: &Path) -> ExtraInputsSnapshot {
        let file_hasher = FileHasher::new();
        ExtraInputsSnapshot::resolve(Some(src), "x", true, &file_hasher)
            .expect("snapshot resolution succeeds")
            .expect("fixture has an active declaration")
    }

    #[test]
    fn snapshot_tracks_config_matches_and_narrow_watch_across_add_delete() {
        let (dir, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/**/*.txt\"]"),
            ("data/a.txt", "a"),
            ("data/nested/b.txt", "b"),
        ]);
        let root = dir.path();

        let initial = snap(&src);
        assert_eq!(initial.config_path, root.join("kache.toml"));
        assert_eq!(
            initial.matched_files,
            vec![root.join("data/a.txt"), root.join("data/nested/b.txt")]
        );
        assert_eq!(initial.watch_paths, vec![root.join("data")]);

        // The literal root stays the watch dependency while the matched-file
        // set follows additions and deletions. Cargo recursively fingerprints
        // that one narrow directory, so either transition makes the unit dirty.
        std::fs::write(root.join("data/added.txt"), "added").unwrap();
        let after_add = snap(&src);
        assert!(
            after_add
                .matched_files
                .contains(&root.join("data/added.txt"))
        );
        assert_eq!(after_add.watch_paths, vec![root.join("data")]);

        std::fs::remove_file(root.join("data/a.txt")).unwrap();
        let after_delete = snap(&src);
        assert!(
            !after_delete
                .matched_files
                .contains(&root.join("data/a.txt"))
        );
        assert_eq!(after_delete.watch_paths, vec![root.join("data")]);
    }

    #[test]
    fn snapshot_observes_nested_watch_directories_against_transient_aba() {
        let (dir, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"data/**/*.txt\"]"),
            ("data/stable.txt", "v1"),
            ("data/deep/.keep", ""),
        ]);
        let nested = dir.path().join("data/deep");
        let before = snap(&src);
        assert!(
            before
                .observations
                .iter()
                .any(|observation| observation.path == nested),
            "every traversed directory must participate in hit/publication revalidation"
        );

        // The compiler could observe this member while it exists even though
        // the final matched-file set and content digest return to the original
        // state. A changed nested-directory observation must still reject that
        // result instead of publishing it under the old key.
        let transient = nested.join("transient.txt");
        std::fs::write(&transient, "transient").unwrap();
        std::fs::remove_file(&transient).unwrap();
        filetime::set_file_mtime(
            &nested,
            filetime::FileTime::from_unix_time(2_000_000_000, 123),
        )
        .unwrap();

        let after = snap(&src);
        assert_eq!(before.digest, after.digest, "semantic state returned to v1");
        assert_eq!(before.matched_files, after.matched_files);
        assert_ne!(
            before, after,
            "nested directory metadata must expose an add/remove ABA race"
        );
    }

    #[test]
    fn empty_declaration_watches_config_without_changing_the_key() {
        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = []")]);
        assert_eq!(dig(&src), None, "legacy key semantics stay byte-identical");

        let snapshot = snap(&src);
        assert_eq!(snapshot.digest(), None);
        assert_eq!(snapshot.config_path, dir.path().join("kache.toml"));
        assert!(snapshot.matched_files.is_empty());
        assert!(snapshot.watch_paths.is_empty());
    }

    #[test]
    fn active_snapshot_rejects_dynamic_pattern_expansion() {
        let (_dir, src) =
            crate_fixture(&[("kache.toml", "extra_inputs = [\"$HOME/data/**/*.txt\"]")]);
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("environment-dependent paths cannot stay Cargo-fresh");
        assert!(
            format!("{error:#}").contains("uses `$ENV` or `~` expansion"),
            "{error:#}"
        );
    }

    #[test]
    fn active_snapshot_rejects_parent_traversal_after_wildcard() {
        let (_dir, src) = crate_fixture(&[(
            "kache.toml",
            "extra_inputs = [\"data/*/../../generated/*.json\"]",
        )]);
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("a wildcard must not escape the bounded Cargo watch root");
        assert!(
            format!("{error:#}").contains("traverses `..` after a wildcard"),
            "{error:#}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn active_snapshot_rejects_ambiguous_windows_root_shapes() {
        for pattern in ["/shared/**/*.json", "C:shared/**/*.json"] {
            let config = format!("extra_inputs = ['{pattern}']");
            let (_dir, src) = crate_fixture(&[("kache.toml", config.as_str())]);
            let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
                .expect_err("ambiguous Windows anchoring must fail closed");
            assert!(
                format!("{error:#}").contains("rooted-without-drive or drive-relative"),
                "{error:#}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn active_snapshot_rejects_windows_device_namespace_patterns() {
        for pattern in [
            r"\\?\C:\shared\**\*.json",
            r"\\?\UNC\server\share\**\*.json",
        ] {
            let config = format!("extra_inputs = ['{pattern}']");
            let (_dir, src) = crate_fixture(&[("kache.toml", config.as_str())]);
            let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
                .expect_err("glob cannot safely enumerate a device namespace");
            assert!(
                format!("{error:#}").contains("verbatim/device namespace"),
                "{error:#}"
            );
        }
        assert!(windows_path_uses_device_namespace(Path::new(
            r"\\?\C:\repo\crate"
        )));
        assert!(!windows_path_uses_device_namespace(Path::new(
            r"C:\repo\crate"
        )));
        assert!(!windows_path_uses_device_namespace(Path::new(
            r"\\server\share\crate"
        )));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn active_glob_rejects_non_utf8_names_in_its_watch_tree() {
        use std::os::unix::ffi::OsStringExt;

        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/**/*.txt\"]")]);
        let data = dir.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(
            data.join(std::ffi::OsString::from_vec(vec![0xff])),
            "hidden",
        )
        .unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("glob must not silently omit non-UTF-8 names");
        assert!(format!("{error:#}").contains("non-UTF-8"), "{error:#}");
    }

    #[test]
    fn missing_literal_watches_existing_narrow_parent() {
        let (dir, src) = crate_fixture(&[
            ("kache.toml", "extra_inputs = [\"inputs/missing.json\"]"),
            ("inputs/.keep", ""),
        ]);
        let snapshot = snap(&src);
        assert!(snapshot.matched_files.is_empty());
        assert_eq!(snapshot.watch_paths, vec![dir.path().join("inputs")]);
    }

    #[test]
    fn broad_crate_root_watch_is_rejected_before_globbing() {
        let (_dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"**/*.json\"]")]);
        let file_hasher = FileHasher::new();
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &file_hasher)
            .expect_err("crate-root recursive watch must be rejected");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("recursively watch broad directory"),
            "{rendered}"
        );
        assert!(rendered.contains("literal subdirectory"), "{rendered}");
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_root_watch_is_rejected_without_enumeration() {
        let (_dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"/**/*.json\"]")]);
        let file_hasher = FileHasher::new();
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &file_hasher)
            .expect_err("filesystem-root recursive watch must be rejected");
        assert!(
            format!("{error:#}").contains("recursively watch broad directory"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_to_filesystem_root_cannot_bypass_watch_guard() {
        use std::os::unix::fs::symlink;

        let (dir, src) =
            crate_fixture(&[("kache.toml", "extra_inputs = [\"root-link/**/*.json\"]")]);
        symlink("/", dir.path().join("root-link")).unwrap();
        let file_hasher = FileHasher::new();
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &file_hasher)
            .expect_err("a symlink to the filesystem root must be rejected before globbing");
        assert!(
            format!("{error:#}").contains("recursively watch broad directory"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_snapshot_rejects_symlinked_input_that_cargo_would_canonicalize() {
        use std::os::unix::fs::symlink;

        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/value.txt\"]")]);
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        let target = dir.path().join("target-value.txt");
        std::fs::write(&target, "v1").unwrap();
        symlink(&target, dir.path().join("data/value.txt")).unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("symlink retargeting cannot be represented safely in Cargo dep-info");
        assert!(
            format!("{error:#}").contains("crosses symlink"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_snapshot_rejects_symlinked_empty_config() {
        use std::os::unix::fs::symlink;

        let (dir, src) = crate_fixture(&[]);
        let target = dir.path().join("empty-config.toml");
        std::fs::write(&target, "extra_inputs = []\n").unwrap();
        symlink(&target, dir.path().join("kache.toml")).unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("retargetable empty config must fail closed");
        assert!(
            format!("{error:#}").contains("config") && format!("{error:#}").contains("symlink"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_glob_rejects_symlink_nested_under_its_watch_root() {
        use std::os::unix::fs::symlink;

        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/**/*.txt\"]")]);
        let external = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(external.path().join("value.txt"), "v1").unwrap();
        symlink(external.path(), dir.path().join("data/external")).unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("a glob must not follow a nested symlink outside its bounded watch tree");
        assert!(
            format!("{error:#}").contains("contains symlink"),
            "{error:#}"
        );
    }

    #[test]
    fn active_snapshot_rejects_invalid_config_instead_of_watching_only_config() {
        let (_dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [")]);
        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("invalid active config must fail closed");
        assert!(
            format!("{error:#}").contains("parsing active extra_inputs config"),
            "{error:#}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn active_snapshot_rejects_control_separator_in_matched_filename() {
        let (dir, src) = crate_fixture(&[("kache.toml", "extra_inputs = [\"data/**/*\"]")]);
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(dir.path().join("data/bad\u{1f}name.txt"), "v1").unwrap();

        let error = ExtraInputsSnapshot::resolve(Some(&src), "x", true, &FileHasher::new())
            .expect_err("matched paths must not cross cache-key framing");
        assert!(
            format!("{error:#}").contains("cache-key control separator"),
            "{error:#}"
        );
    }

    #[test]
    fn dep_info_merge_escapes_dedupes_and_preserves_later_rules() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let dep_info = root.join("crate.d");
        let original = concat!(
            "# generated by rustc\n",
            "artifact: src/lib.rs\n",
            "later: keep\\ this\n",
            "# env-dep:VALUE=unchanged\n",
        );
        std::fs::write(&dep_info, original).unwrap();
        let snapshot = ExtraInputsSnapshot {
            config_path: root.join("kache.toml"),
            normalized_patterns: vec!["inputs/**/*".to_string()],
            digest: Some("digest".to_string()),
            matched_files: vec![root.join("space #colon:slash\\name.txt")],
            watch_paths: vec![root.join("watched dir")],
            observations: Vec::new(),
        };

        assert_eq!(
            make_escape_word("space #colon:slash\\name.txt").unwrap(),
            "space\\ #colon:slash\\name.txt"
        );
        snapshot.merge_into_dep_info(&dep_info).unwrap();
        let once = std::fs::read_to_string(&dep_info).unwrap();
        assert!(once.contains("space\\ #colon:slash\\name.txt"), "{once}");
        assert!(once.contains("watched\\ dir"), "{once}");
        assert_eq!(once.matches("kache.toml").count(), 1, "{once}");
        assert!(
            once.ends_with("later: keep\\ this\n# env-dep:VALUE=unchanged\n"),
            "later rules/comments changed:\n{once}"
        );

        // Idempotence is the strongest dedupe check: a second merge performs
        // no rewrite and leaves the complete file byte-identical.
        snapshot.merge_into_dep_info(&dep_info).unwrap();
        assert_eq!(once, std::fs::read_to_string(&dep_info).unwrap());
    }

    #[test]
    fn dep_info_merge_fails_closed_on_missing_and_malformed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let snapshot = ExtraInputsSnapshot {
            config_path: root.join("kache.toml"),
            normalized_patterns: vec!["inputs/**/*".to_string()],
            digest: Some("digest".to_string()),
            matched_files: Vec::new(),
            watch_paths: vec![root.join("inputs")],
            observations: Vec::new(),
        };

        let missing = root.join("missing.d");
        let missing_error = snapshot.merge_into_dep_info(&missing).unwrap_err();
        assert!(format!("{missing_error:#}").contains("required consumer dep-info"));

        let malformed = root.join("malformed.d");
        let malformed_bytes = "not a dependency rule\nstill not a dependency rule\n";
        std::fs::write(&malformed, malformed_bytes).unwrap();
        let malformed_error = snapshot.merge_into_dep_info(&malformed).unwrap_err();
        assert!(format!("{malformed_error:#}").contains("malformed consumer dep-info"));
        assert_eq!(
            std::fs::read_to_string(&malformed).unwrap(),
            malformed_bytes
        );
    }

    #[test]
    fn make_escape_round_trips_windows_drive_space_hash_and_backslashes() {
        let windows = r"C:\work tree\generated#1:file.rs";
        let escaped = make_escape_word(windows).unwrap();
        assert_eq!(escaped, r"C:\work\ tree\generated#1:file.rs");
        assert_eq!(parse_make_words(&escaped).unwrap(), vec![windows]);
        assert!(make_escape_word("unix-name-ending-in-backslash\\").is_err());
        assert!(make_escape_word("unix-name-ending-in-space ").is_err());
    }

    #[test]
    fn dep_info_codec_treats_hash_and_windows_drive_colons_as_literals() {
        let input = concat!(
            "# generated metadata stays literal\n",
            r"C:\target\crate.d: C:\work\ tree\#member\src\lib.rs C:\work\data:1.txt",
            "\nsecond: ignored\n",
        );
        let rule = first_make_dependency_rule(input).unwrap();
        assert_eq!(&input[rule.colon..rule.colon + 2], ": ");
        assert_eq!(
            parse_make_words(&input[rule.colon + 2..rule.insertion]).unwrap(),
            vec![r"C:\work tree\#member\src\lib.rs", r"C:\work\data:1.txt"]
        );
    }

    #[test]
    fn dep_info_codec_skips_env_record_containing_colon_space() {
        let input = concat!(
            "# env-dep:CFG=foo: bar\n",
            "artifact: src/lib.rs data/value.txt\n",
        );
        let rule = first_make_dependency_rule(input).unwrap();
        assert_eq!(
            &input[rule.colon - "artifact".len()..rule.colon],
            "artifact"
        );
        assert_eq!(
            parse_dep_info_dependencies(input).unwrap(),
            vec![PathBuf::from("src/lib.rs"), PathBuf::from("data/value.txt")]
        );
    }

    #[test]
    fn relocated_dep_info_uses_unambiguous_consumer_absolute_paths() {
        let files = &[
            ("kache.toml", "extra_inputs = [\"data/**/*.json\"]"),
            ("data/q.json", "v1"),
        ];
        let (dir_a, src_a) = crate_fixture(files);
        let (dir_b, src_b) = crate_fixture(files);
        let snapshot_a = snap(&src_a);
        let snapshot_b = snap(&src_b);
        assert_eq!(snapshot_a.digest, snapshot_b.digest);

        let dep_a = dir_a.path().join("crate.d");
        let dep_b = dir_b.path().join("crate.d");
        std::fs::write(&dep_a, "artifact: src/lib.rs\n").unwrap();
        std::fs::write(&dep_b, "artifact: src/lib.rs\n").unwrap();
        snapshot_a.merge_into_dep_info(&dep_a).unwrap();
        snapshot_b.merge_into_dep_info(&dep_b).unwrap();
        let output_a = std::fs::read_to_string(&dep_a).unwrap();
        let output_b = std::fs::read_to_string(&dep_b).unwrap();
        let dependencies_a = parse_dep_info_dependencies(&output_a).unwrap();
        let dependencies_b = parse_dep_info_dependencies(&output_b).unwrap();

        assert_ne!(output_a, output_b);
        assert!(dependencies_a.contains(&dir_a.path().join("data/q.json")));
        assert!(dependencies_a.contains(&dir_a.path().join("kache.toml")));
        assert!(dependencies_b.contains(&dir_b.path().join("data/q.json")));
        assert!(dependencies_b.contains(&dir_b.path().join("kache.toml")));
        assert!(
            !dependencies_a
                .iter()
                .any(|path| path.starts_with(dir_b.path()))
        );
        assert!(
            !dependencies_b
                .iter()
                .any(|path| path.starts_with(dir_a.path()))
        );
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
