//! Files a build script names under the target directory, outside `OUT_DIR`.
//!
//! A link search path such as `cargo:rustc-link-search=<target>/debug/gn_out/obj`
//! is rewritten for the next checkout, and rustc then looks for the library
//! there. The bytes live next to `OUT_DIR`, not in it, so restoring `OUT_DIR`
//! alone replays a path with nothing behind it. This module records the named
//! tree and writes it back before that stdout is replayed.
//!
//! Directories Cargo fills itself are not part of the run. A path outside the
//! target directory is left where it is. The files share `OUT_DIR`'s size cap;
//! when they do not fit, nothing is recorded.

use crate::store::{EntryMeta, Store, link};
use anyhow::{Context, Result};
use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

use super::{
    EmptyFile, Environment, Manifest, Symlink, TARGET_DIR_PLACEHOLDER, checked_relative,
    link_stays_inside, set_executable,
};

/// Store-name prefix. The rest is relative to the target directory.
pub(super) const PREFIX: &str = "outside/";

/// Manifest version once a run records files outside `OUT_DIR`. Older kache
/// accepts versions 1 through 3 and reruns rather than replay a link search
/// it cannot restore.
pub(super) const MANIFEST_VERSION: u32 = 4;

const TOO_LARGE: &str = "a named target output is too large to record";

/// What of a named tree is worth storing. Paths are relative to the target
/// directory, with `/` separators. `files` carry the [`PREFIX`] store name.
#[derive(Debug, Default)]
pub(super) struct OutsideContents {
    pub files: Vec<(PathBuf, String)>,
    pub directories: Vec<String>,
    pub empty_files: Vec<EmptyFile>,
    pub symlinks: Vec<Symlink>,
}

impl OutsideContents {
    pub(super) fn is_empty(&self) -> bool {
        self.files.is_empty()
            && self.directories.is_empty()
            && self.empty_files.is_empty()
            && self.symlinks.is_empty()
    }

    fn finish(&mut self) {
        self.files.sort_by(|left, right| left.1.cmp(&right.1));
        self.directories.sort();
        self.empty_files
            .sort_by(|left, right| left.name.cmp(&right.name));
        self.symlinks
            .sort_by(|left, right| left.name.cmp(&right.name));
    }
}

/// `1` when the run stores bytes as it wrote them, `2` when text was rewritten
/// with placeholders, `3` when `OUT_DIR` holds a symlink, and [`MANIFEST_VERSION`]
/// when a file outside `OUT_DIR` was stored. An older reader refuses the higher
/// number and runs the script.
pub(super) fn manifest_version(has_symlinks: bool, rewrites_text: bool, has_outside: bool) -> u32 {
    if has_outside {
        MANIFEST_VERSION
    } else if has_symlinks {
        3
    } else if rewrites_text {
        2
    } else {
        1
    }
}

/// Bytes and entries still available after `OUT_DIR` took its share of the cap.
pub(super) fn budget(used_bytes: u64, used_entries: usize) -> Result<(u64, usize)> {
    anyhow::ensure!(
        used_bytes <= super::MAX_OUT_DIR_BYTES && used_entries <= super::MAX_OUT_DIR_FILES,
        "OUT_DIR is too large to record"
    );
    Ok((
        super::MAX_OUT_DIR_BYTES - used_bytes,
        super::MAX_OUT_DIR_FILES - used_entries,
    ))
}

/// Byte length of the regular files `collect_out_dir` is about to store.
pub(super) fn file_bytes(files: &[(PathBuf, String)]) -> Result<u64> {
    let mut total = 0u64;
    for (path, _) in files {
        total += std::fs::metadata(path)?.len();
    }
    Ok(total)
}

/// Bytes and entries `OUT_DIR` already takes from the shared cap: its regular
/// files, and one entry per relative or rooted symlink.
pub(super) fn out_dir_share(
    files: &[(PathBuf, String)],
    symlinks: &[Symlink],
    rooted_symlinks: usize,
) -> Result<(u64, usize)> {
    Ok((
        file_bytes(files)?,
        files.len() + symlinks.len() + rooted_symlinks,
    ))
}

struct Budget {
    bytes_left: u64,
    entries_left: usize,
}

impl Budget {
    fn take(&mut self, bytes: u64) -> Result<()> {
        anyhow::ensure!(
            self.entries_left > 0 && bytes <= self.bytes_left,
            "{TOO_LARGE}"
        );
        self.entries_left -= 1;
        self.bytes_left -= bytes;
        Ok(())
    }
}

/// The named trees in `stdout`, or an error when one of them cannot be stored.
/// A missing path is skipped: the build that ran the script fails the same
/// way. A symlink as the named path, a tree that leaves itself, or a tree
/// over the remaining cap refuses the whole run.
pub(super) fn collect(
    stdout: &str,
    environment: &Environment,
    used_bytes: u64,
    used_entries: usize,
) -> Result<OutsideContents> {
    let (bytes_left, entries_left) = budget(used_bytes, used_entries)?;
    let mut budget = Budget {
        bytes_left,
        entries_left,
    };
    let mut contents = OutsideContents::default();
    for path in paths_to_snapshot(stdout, environment)? {
        collect_path(&path, environment, &mut budget, &mut contents)?;
    }
    contents.finish();
    Ok(contents)
}

/// Write recorded outside files under this checkout's target directory.
/// Nothing named means nothing to write, including when this `OUT_DIR` has
/// no target directory. A named file whose blob is gone fails the restore,
/// so the script runs instead of rustc seeing a dangling search path.
pub(super) fn restore(
    store: &Store,
    environment: &Environment,
    meta: &EntryMeta,
    manifest: &Manifest,
    rewritten: &BTreeSet<&str>,
) -> Result<u64> {
    if manifest.outside_directories.is_empty()
        && manifest.outside_empty_files.is_empty()
        && manifest.outside_symlinks.is_empty()
        && !meta.files.iter().any(|file| file.name.starts_with(PREFIX))
    {
        return Ok(0);
    }
    let target = target_spelling(environment)?;
    for directory in &manifest.outside_directories {
        std::fs::create_dir_all(
            target.join(checked_outside(directory, environment.target.as_deref())?),
        )?;
    }
    let mut prepared = Vec::new();
    let mut texts = Vec::new();
    let mut size = 0u64;
    for cached in &meta.files {
        let Some(relative) = cached.name.strip_prefix(PREFIX) else {
            continue;
        };
        let path = target.join(checked_outside(relative, environment.target.as_deref())?);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let blob = store.blob_path(&cached.hash);
        anyhow::ensure!(
            blob.is_file(),
            "blob for {} was evicted before restore",
            cached.name
        );
        size += cached.size;
        if rewritten.contains(cached.name.as_str()) {
            texts.push((blob, path, cached.executable));
            continue;
        }
        prepared.push((
            link::prepare_writable_target_from_file(&blob, &path)?,
            cached.executable,
        ));
    }
    for (artifact, executable) in prepared {
        let path = artifact.target().to_path_buf();
        artifact.publish_replacing()?;
        set_executable(&path, executable)?;
    }
    for (blob, path, executable) in texts {
        std::fs::write(&path, environment.denormalize(&std::fs::read(&blob)?))?;
        set_executable(&path, executable)?;
    }
    for empty in &manifest.outside_empty_files {
        let path = target.join(checked_outside(&empty.name, environment.target.as_deref())?);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, b"")?;
        set_executable(&path, empty.executable)?;
    }
    for symlink in &manifest.outside_symlinks {
        checked_outside(&symlink.name, environment.target.as_deref())?;
        super::restore_symlink(target, symlink)?;
    }
    Ok(size)
}

fn target_spelling(environment: &Environment) -> Result<&Path> {
    environment
        .spellings
        .iter()
        .find(|(_, placeholder)| *placeholder == TARGET_DIR_PLACEHOLDER)
        .map(|(path, _)| path.as_path())
        .context("build script has no target directory for files outside OUT_DIR")
}

/// `name` is relative to the target directory and is not one of Cargo's own
/// profile directories.
fn checked_outside<'a>(name: &'a str, target_name: Option<&OsStr>) -> Result<&'a Path> {
    let path = checked_relative(name)?;
    anyhow::ensure!(
        !is_cargo_owned_relative(path, target_name),
        "recorded build-script output is in a cargo directory: {name}"
    );
    Ok(path)
}

fn collect_path(
    path: &Path,
    environment: &Environment,
    budget: &mut Budget,
    contents: &mut OutsideContents,
) -> Result<()> {
    let target = target_root(environment, path)
        .context("a named target output is outside the target directory")?;
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.is_file() {
        return record_file(
            path,
            path.strip_prefix(target)?,
            &metadata,
            budget,
            contents,
        );
    }
    if metadata.is_dir() {
        return walk(
            target,
            environment.target.as_deref(),
            path,
            budget,
            contents,
        );
    }
    anyhow::bail!(
        "a named target output is not a file or directory: {}",
        path.display()
    )
}

fn walk(
    target: &Path,
    target_name: Option<&OsStr>,
    named: &Path,
    budget: &mut Budget,
    contents: &mut OutsideContents,
) -> Result<()> {
    let mut pending = vec![named.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let name = slash_relative(directory.strip_prefix(target)?)?;
        if !name.is_empty() {
            contents.directories.push(name);
        }
        let mut entries: Vec<_> = std::fs::read_dir(&directory)?.collect::<std::io::Result<_>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let path = entry.path();
            let relative = path.strip_prefix(target)?;
            if is_cargo_owned_relative(relative, target_name) {
                continue;
            }
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                record_symlink(named, &path, relative, budget, contents)?;
            } else if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                record_file(&path, relative, &metadata, budget, contents)?;
            } else {
                anyhow::bail!(
                    "a named target output is not a file or directory: {}",
                    path.display()
                )
            }
        }
    }
    Ok(())
}

fn record_file(
    path: &Path,
    relative: &Path,
    metadata: &std::fs::Metadata,
    budget: &mut Budget,
    contents: &mut OutsideContents,
) -> Result<()> {
    let name = slash_relative(relative)?;
    anyhow::ensure!(!name.is_empty(), "a named target output has an unsafe name");
    if metadata.len() == 0 {
        contents.empty_files.push(EmptyFile {
            name,
            executable: kache_store::filesystem::is_executable(metadata),
        });
        return Ok(());
    }
    budget.take(metadata.len())?;
    contents
        .files
        .push((path.to_path_buf(), format!("{PREFIX}{name}")));
    Ok(())
}

fn record_symlink(
    named: &Path,
    path: &Path,
    relative_to_target: &Path,
    budget: &mut Budget,
    contents: &mut OutsideContents,
) -> Result<()> {
    let target = std::fs::read_link(path)?;
    let relative_to_named = path.strip_prefix(named)?;
    anyhow::ensure!(
        link_stays_inside(relative_to_named, &target),
        "a named target output contains a symlink that leaves it: {} -> {}",
        path.display(),
        target.display()
    );
    let name = slash_relative(relative_to_target)?;
    let text = target
        .to_str()
        .context("a named target output symlink is not UTF-8")?;
    // A symlink costs an entry and no bytes. `take(0)` still refuses once the
    // entry cap is spent, and it still fits when only the byte cap is spent.
    budget.take(0)?;
    contents.symlinks.push(Symlink {
        name,
        target: text.to_string(),
    });
    Ok(())
}

fn paths_to_snapshot(stdout: &str, environment: &Environment) -> Result<Vec<PathBuf>> {
    validate_named_paths(stdout, environment)?;
    let mut found = Vec::new();
    for raw in named_output_paths(stdout) {
        let Some(path) = resolve_named_path(&raw, environment) else {
            continue;
        };
        let Some(relative) = relative_to_target(environment, &path) else {
            continue;
        };
        if is_inside_out_dir(&path, environment)
            || is_cargo_owned_relative(relative, environment.target.as_deref())
        {
            continue;
        }
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                anyhow::bail!("a named target output is a symlink: {}", path.display());
            }
            Ok(_) => found.push(path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(dedup_nested(found))
}

/// Absolute dependency metadata paths are inputs provided before this run.
/// Keep both spellings when a link gives the same input another name.
pub(super) fn dependency_roots(
    variables: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for (name, value) in variables {
        if name.as_encoded_bytes().starts_with(b"DEP_") {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                // Keep the spelling the dependency supplied, not a lexical
                // simplification: `link/..` can resolve somewhere other than
                // the parent of `link` when it is a symlink.
                if let Some(canonical) = canonical_with_missing_suffix(&path) {
                    roots.push(canonical);
                }
                roots.push(path);
            }
        }
    }
    roots.sort();
    roots.dedup();
    roots
}

/// Ownership checks need every captured spelling, but hashing those aliases
/// separately walks and budgets the same dependency more than once.
pub(super) fn dependency_inputs(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut inputs: Vec<_> = roots
        .iter()
        .map(|path| canonical_with_missing_suffix(path).unwrap_or_else(|| path.clone()))
        .collect();
    inputs.sort();
    inputs.dedup();
    inputs
}

fn path_aliases(path: &Path) -> Vec<PathBuf> {
    let mut aliases: Vec<_> = normalize_lexical(path).into_iter().collect();
    if let Some(canonical) = canonical_with_missing_suffix(path)
        && !aliases.contains(&canonical)
    {
        aliases.push(canonical);
    }
    aliases
}

/// A missing library below an existing symlink still belongs to the unit
/// that symlink resolves into. Keep the missing suffix without inventing a
/// new spelling for the existing prefix.
fn canonical_with_missing_suffix(path: &Path) -> Option<PathBuf> {
    let mut ancestor = path.to_path_buf();
    let mut missing = Vec::new();
    loop {
        match ancestor.canonicalize() {
            Ok(mut canonical) => {
                canonical.extend(missing.iter().rev());
                return Some(canonical);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(ancestor.file_name()?.to_os_string());
                if !ancestor.pop() {
                    return None;
                }
            }
            Err(_) => return None,
        }
    }
}

/// Cargo owns every profile/build unit except this run's OUT_DIR. The
/// collector skips those directories, so replaying a search there is valid
/// only when a links dependency handed the path down and its contents key
/// the run. An ancestor above that dependency unit grants nothing.
pub(super) fn validate_named_paths(stdout: &str, environment: &Environment) -> Result<()> {
    for raw in named_output_paths(stdout) {
        let raw = Path::new(&raw);
        let absolute = if raw.is_absolute() {
            raw.to_path_buf()
        } else {
            environment.manifest_dir.join(raw)
        };
        anyhow::ensure!(
            !absolute
                .components()
                .any(|component| component == Component::ParentDir)
                || canonical_with_missing_suffix(&absolute).is_some(),
            "a named link path with parent traversal cannot be resolved: {}",
            absolute.display()
        );
        for path in path_aliases(&absolute) {
            if is_inside_out_dir(&path, environment)
                || path_aliases(&environment.out_dir)
                    .iter()
                    .any(|out| path.starts_with(out))
            {
                continue;
            }
            for target in environment.target_roots() {
                for target in path_aliases(target) {
                    let Ok(relative) = path.strip_prefix(&target) else {
                        continue;
                    };
                    let Some(unit) = build_unit(relative, environment.target.as_deref()) else {
                        anyhow::ensure!(
                            !is_cargo_build_root(relative, environment.target.as_deref()),
                            "a named link path is a Cargo build root without an owned unit: {}",
                            path.display()
                        );
                        continue;
                    };
                    let permitted = dependency_provides(&path, &unit, &target, environment);
                    anyhow::ensure!(
                        permitted,
                        "a named link path is in a Cargo build unit not provided by DEP_*: {}",
                        path.display()
                    );
                }
            }
        }
    }
    Ok(())
}

fn is_cargo_build_root(relative: &Path, target_name: Option<&OsStr>) -> bool {
    let parts: Vec<_> = relative.iter().collect();
    (parts.len() == 2 && parts[1] == "build")
        || (parts.len() == 3 && is_target_triple(parts[0], target_name) && parts[2] == "build")
}

/// A named path and a dependency can reach the same target through different
/// aliases. Compare matching views of the path and target, preserving the
/// dependency-unit boundary in each view.
fn dependency_provides(path: &Path, unit: &Path, target: &Path, environment: &Environment) -> bool {
    let Some(unit_identity) = canonical_with_missing_suffix(&target.join(unit)) else {
        return false;
    };
    let target_views: Vec<_> = environment.target_roots().flat_map(path_aliases).collect();
    path_aliases(path).iter().any(|candidate| {
        environment.dependency_roots.iter().any(|dependency| {
            candidate.starts_with(dependency)
                && target_views.iter().any(|target| {
                    dependency
                        .strip_prefix(target)
                        .ok()
                        .and_then(|relative| build_unit(relative, environment.target.as_deref()))
                        .and_then(|unit| canonical_with_missing_suffix(&target.join(unit)))
                        .is_some_and(|identity| identity == unit_identity)
                })
        })
    })
}

/// The unit's prefix below target in either Cargo layout. Unknown names
/// below build are treated as legacy units rather than assumed safe.
fn build_unit(relative: &Path, target_name: Option<&OsStr>) -> Option<PathBuf> {
    let parts: Vec<_> = relative.iter().collect();
    let build = if parts.get(1).copied() == Some(OsStr::new("build")) {
        1
    } else if parts
        .first()
        .copied()
        .is_some_and(|name| is_target_triple(name, target_name))
        && parts.get(2).copied() == Some(OsStr::new("build"))
    {
        2
    } else {
        return None;
    };
    parts.get(build + 1)?;
    let mut end = build + 2;
    if parts
        .get(end)
        .and_then(|part| part.to_str())
        .is_some_and(crate::cargo_layout::is_unit_hash)
    {
        end += 1;
    }
    Some(parts[..end].iter().collect())
}

/// Search paths and linker inputs a script printed. Kinds (`native=`, …) are
/// stripped only when they are one of Cargo's link kinds; anything else is
/// part of the path. `-L` is read from `rustc-link-arg*` and `rustc-flags`.
fn named_output_paths(stdout: &str) -> Vec<String> {
    let mut paths = Vec::new();
    for line in stdout.lines() {
        let Some(directive) = line
            .strip_prefix("cargo::")
            .or_else(|| line.strip_prefix("cargo:"))
        else {
            continue;
        };
        if let Some(value) = directive.strip_prefix("rustc-link-search=") {
            push_search_path(value, &mut paths);
        } else if let Some(value) = link_arg_value(directive) {
            push_link_arg(value, &mut paths);
        } else if let Some(value) = directive.strip_prefix("rustc-flags=") {
            push_link_flags(value, &mut paths);
        }
    }
    paths
}

const LINK_KINDS: &[&str] = &["dependency", "crate", "native", "framework", "all"];

fn push_search_path(value: &str, paths: &mut Vec<String>) {
    let path = match value.split_once('=') {
        Some((kind, path)) if LINK_KINDS.contains(&kind) => path,
        _ => value,
    };
    if !path.is_empty() {
        paths.push(path.to_string());
    }
}

fn link_arg_value(directive: &str) -> Option<&str> {
    // `rustc-link-arg-bin=BIN=FLAG` carries the binary name before the flag.
    if let Some(rest) = directive.strip_prefix("rustc-link-arg-bin=") {
        return rest.split_once('=').map(|(_, flag)| flag);
    }
    let rest = directive
        .strip_prefix("rustc-link-arg")
        .or_else(|| directive.strip_prefix("rustc-cdylib-link-arg"))?;
    let rest = rest
        .strip_prefix("-bins")
        .or_else(|| rest.strip_prefix("-cdylib"))
        .or_else(|| rest.strip_prefix("-tests"))
        .or_else(|| rest.strip_prefix("-examples"))
        .or_else(|| rest.strip_prefix("-benches"))
        .unwrap_or(rest);
    rest.strip_prefix('=')
}

fn push_link_arg(flag: &str, paths: &mut Vec<String>) {
    if flag.starts_with("-L") {
        push_link_flags(flag, paths);
    } else if Path::new(flag).is_absolute() {
        paths.push(flag.to_string());
    }
}

fn push_link_flags(flags: &str, paths: &mut Vec<String>) {
    let mut tokens = flags.split_whitespace();
    while let Some(token) = tokens.next() {
        if let Some(path) = token.strip_prefix("-L") {
            if path.is_empty() {
                if let Some(next) = tokens.next() {
                    push_search_path(next, paths);
                }
            } else {
                push_search_path(path, paths);
            }
        }
    }
}

fn resolve_named_path(raw: &str, environment: &Environment) -> Option<PathBuf> {
    let path = Path::new(raw);
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        environment.manifest_dir.join(path)
    };
    normalize_lexical(&absolute)
}

fn normalize_lexical(path: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return None;
                }
            }
            other => out.push(other),
        }
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

fn relative_to_target<'a>(environment: &Environment, path: &'a Path) -> Option<&'a Path> {
    environment
        .mappings
        .iter()
        .filter(|(_, placeholder)| *placeholder == TARGET_DIR_PLACEHOLDER)
        .find_map(|(root, _)| path.strip_prefix(root).ok())
        .filter(|relative| !relative.as_os_str().is_empty())
}

fn target_root<'a>(environment: &'a Environment, path: &Path) -> Option<&'a Path> {
    environment.mappings.iter().find_map(|(root, placeholder)| {
        (*placeholder == TARGET_DIR_PLACEHOLDER && path.starts_with(root)).then_some(root.as_path())
    })
}

fn is_inside_out_dir(path: &Path, environment: &Environment) -> bool {
    path.starts_with(&environment.out_dir)
        || environment
            .mappings
            .iter()
            .any(|(root, placeholder)| *placeholder == "${KACHE_OUT_DIR}" && path.starts_with(root))
}

/// Cargo's own directories directly under a profile: `<target>/<profile>/deps`
/// and `<target>/<triple>/<profile>/deps`, and the same for `incremental`,
/// `build`, `.fingerprint`, and `examples`. A `build` directory further down,
/// under a tree the script created, is not one of these.
fn is_cargo_owned_relative(relative: &Path, target_name: Option<&OsStr>) -> bool {
    let names: Vec<&OsStr> = relative.iter().collect();
    if names.get(1).copied().is_some_and(owned_dir) {
        return true;
    }
    names.len() >= 3
        && names
            .first()
            .copied()
            .is_some_and(|name| is_target_triple(name, target_name))
        && names.get(2).copied().is_some_and(owned_dir)
}

fn owned_dir(name: &OsStr) -> bool {
    matches!(
        name.to_str(),
        Some("deps" | "incremental" | "build" | ".fingerprint" | "examples")
    )
}

/// Cargo's captured target name also covers one-hyphen and custom targets.
/// Keep the standard-triple fallback for other targets in this tree, while
/// a script's `my-profile/gn_out/build` remains outside Cargo's directories.
fn is_target_triple(name: &OsStr, target_name: Option<&OsStr>) -> bool {
    target_name == Some(name)
        || name
            .to_str()
            .is_some_and(|text| text.matches('-').count() >= 2)
}

fn slash_relative(relative: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => {
                parts.push(
                    part.to_str()
                        .context("a named target output is not UTF-8")?,
                );
            }
            Component::CurDir => {}
            _ => anyhow::bail!(
                "a named target output has an unsafe name: {}",
                relative.display()
            ),
        }
    }
    Ok(parts.join("/"))
}

fn dedup_nested(mut paths: Vec<PathBuf>) -> Vec<PathBuf> {
    paths.sort();
    let mut kept = Vec::new();
    for path in paths {
        if kept.iter().any(|earlier| path.starts_with(earlier)) {
            continue;
        }
        kept.push(path);
    }
    kept
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(stdout: &str) -> Vec<String> {
        named_output_paths(stdout)
    }

    #[test]
    fn link_search_paths_keep_a_kind_only_when_it_is_one_of_cargos() {
        for kind in ["dependency", "crate", "native", "framework", "all"] {
            let stdout = format!("cargo:rustc-link-search={kind}=/t/lib\n");
            assert_eq!(paths(&stdout), vec!["/t/lib".to_string()], "{kind}");
        }
        assert_eq!(
            paths("cargo:rustc-link-search=/t/lib\n"),
            vec!["/t/lib".to_string()]
        );
        assert_eq!(
            paths("cargo:rustc-link-search=notakind=/t/lib\n"),
            vec!["notakind=/t/lib".to_string()],
            "an unknown kind is part of the path"
        );
        assert!(
            paths("cargo:rustc-link-search=native=\n").is_empty(),
            "an empty path is not a search directory"
        );
        assert_eq!(
            paths("cargo::rustc-link-search=/t/lib\n"),
            vec!["/t/lib".to_string()],
            "the double-colon form"
        );
        assert!(
            paths("cargo:warning=see cargo:rustc-link-search=/t/no\n").is_empty(),
            "a path mentioned in a warning is not an instruction"
        );
        assert!(paths("cargo:rustc-link-lib=static=rusty_v8\n").is_empty());
    }

    #[test]
    fn link_args_and_flags_contribute_their_library_paths() {
        assert_eq!(
            paths("cargo:rustc-flags=-L native=/t/g -lfoo\n"),
            vec!["/t/g".to_string()]
        );
        assert_eq!(
            paths("cargo:rustc-flags=-Lnative=/t/g\n"),
            vec!["/t/g".to_string()]
        );
        assert_eq!(
            paths("cargo:rustc-flags=-L/t/g\n"),
            vec!["/t/g".to_string()]
        );
        assert_eq!(
            paths("cargo:rustc-flags=-L /t/g -L/t/h\n"),
            vec!["/t/g".to_string(), "/t/h".to_string()]
        );
        assert_eq!(
            paths("cargo:rustc-link-arg=-Lnative=/t/g\n"),
            vec!["/t/g".to_string()]
        );
        // Absolute on the platform running the test: `/t` has no drive on Windows.
        let archive = if cfg!(windows) {
            r"C:\t\g\lib.a"
        } else {
            "/t/g/lib.a"
        };
        assert_eq!(
            paths(&format!("cargo:rustc-link-arg={archive}\n")),
            vec![archive.to_string()]
        );
        assert!(paths("cargo:rustc-link-arg=-lfoo\n").is_empty());
        assert!(paths("cargo:rustc-link-arg=libfoo.a\n").is_empty());
        assert_eq!(
            paths("cargo:rustc-link-arg-bin=app=-L/t/g\n"),
            vec!["/t/g".to_string()]
        );
        assert_eq!(
            paths(&format!("cargo:rustc-link-arg-bin=app={archive}\n")),
            vec![archive.to_string()]
        );
        assert!(paths("cargo:rustc-link-arg-bin=app\n").is_empty());
        for directive in [
            "rustc-link-arg",
            "rustc-link-arg-bins",
            "rustc-link-arg-cdylib",
            "rustc-cdylib-link-arg",
            "rustc-link-arg-tests",
            "rustc-link-arg-examples",
            "rustc-link-arg-benches",
        ] {
            let stdout = format!("cargo:{directive}={archive}\n");
            assert_eq!(paths(&stdout), vec![archive.to_string()], "{directive}");
        }
    }

    #[test]
    fn manifest_versions_stay_readable_until_a_file_lands_outside_out_dir() {
        assert_eq!(manifest_version(false, false, false), 1);
        assert_eq!(manifest_version(false, true, false), 2);
        assert_eq!(manifest_version(true, false, false), 3);
        assert_eq!(manifest_version(true, true, false), 3);
        assert_eq!(manifest_version(false, false, true), 4);
        assert_eq!(manifest_version(true, true, true), 4);
    }

    #[test]
    fn outside_contents_are_empty_only_when_every_list_is() {
        let mut contents = OutsideContents::default();
        assert!(contents.is_empty());
        contents.files.push((PathBuf::new(), "outside/a".into()));
        assert!(!contents.is_empty(), "a file");
        contents.files.clear();
        contents.directories.push("debug/gn_out".into());
        assert!(!contents.is_empty(), "a directory");
        contents.directories.clear();
        contents.empty_files.push(EmptyFile {
            name: "debug/empty".into(),
            executable: false,
        });
        assert!(!contents.is_empty(), "an empty file");
        contents.empty_files.clear();
        contents.symlinks.push(Symlink {
            name: "debug/lib.so".into(),
            target: "lib.so.1".into(),
        });
        assert!(!contents.is_empty(), "a symlink");
    }

    #[test]
    fn the_remaining_budget_is_what_out_dir_did_not_use() {
        assert_eq!(budget(0, 0).unwrap(), (1 << 30, 50_000));
        assert_eq!(budget(1, 2).unwrap(), ((1 << 30) - 1, 50_000 - 2));
        assert_eq!(budget(1 << 30, 50_000).unwrap(), (0, 0));
        assert!(budget((1 << 30) + 1, 0).is_err(), "bytes over the cap");
        assert!(budget(0, 50_001).is_err(), "entries over the cap");
    }

    #[test]
    fn file_bytes_adds_every_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(file_bytes(&[]).unwrap(), 0);
        let first = dir.path().join("a");
        let second = dir.path().join("b");
        std::fs::write(&first, "ab").unwrap();
        std::fs::write(&second, "cdef").unwrap();
        let files = [(first, "out/a".into()), (second, "out/b".into())];
        assert_eq!(file_bytes(&files).unwrap(), 6);
    }

    #[test]
    fn out_dir_share_counts_each_file_and_symlink_once() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("a");
        let second = dir.path().join("b");
        std::fs::write(&first, "ab").unwrap();
        std::fs::write(&second, "cdef").unwrap();
        let files = [(first, "out/a".into()), (second, "out/b".into())];
        let symlinks = [Symlink {
            name: "lib.so".into(),
            target: "lib.so.1".into(),
        }];
        assert_eq!(out_dir_share(&files, &symlinks, 0).unwrap(), (6, 3));
        assert_eq!(out_dir_share(&files, &[], 0).unwrap(), (6, 2));
        assert_eq!(out_dir_share(&files, &symlinks, 2).unwrap(), (6, 5));
        let (bytes, entries) = out_dir_share(&files, &symlinks, 49_997).unwrap();
        assert_eq!(budget(bytes, entries).unwrap(), ((1 << 30) - 6, 0));
        let (bytes, entries) = out_dir_share(&files, &symlinks, 49_998).unwrap();
        assert!(
            budget(bytes, entries).is_err(),
            "rooted links share the cap"
        );
    }

    #[test]
    fn finish_orders_every_list_by_name() {
        let mut contents = OutsideContents {
            files: vec![
                (PathBuf::from("b"), "outside/b".into()),
                (PathBuf::from("a"), "outside/a".into()),
            ],
            directories: vec!["z".into(), "y".into()],
            empty_files: vec![
                EmptyFile {
                    name: "n".into(),
                    executable: false,
                },
                EmptyFile {
                    name: "m".into(),
                    executable: false,
                },
            ],
            symlinks: vec![
                Symlink {
                    name: "t".into(),
                    target: "x".into(),
                },
                Symlink {
                    name: "s".into(),
                    target: "x".into(),
                },
            ],
        };
        contents.finish();
        let names = |list: Vec<&str>| list.into_iter().map(str::to_owned).collect::<Vec<_>>();
        assert_eq!(
            contents
                .files
                .iter()
                .map(|file| file.1.clone())
                .collect::<Vec<_>>(),
            names(vec!["outside/a", "outside/b"])
        );
        assert_eq!(contents.directories, names(vec!["y", "z"]));
        assert_eq!(
            contents
                .empty_files
                .iter()
                .map(|file| file.name.clone())
                .collect::<Vec<_>>(),
            names(vec!["m", "n"])
        );
        assert_eq!(
            contents
                .symlinks
                .iter()
                .map(|link| link.name.clone())
                .collect::<Vec<_>>(),
            names(vec!["s", "t"])
        );
    }

    #[test]
    fn a_leading_current_directory_is_dropped_from_a_relative_name() {
        assert_eq!(
            slash_relative(Path::new("./debug/gn_out")).unwrap(),
            "debug/gn_out"
        );
        assert!(slash_relative(Path::new("../debug")).is_err());
    }

    /// Two spellings of the target directory: the file is relative to the one
    /// that holds it, not to the first one listed.
    #[test]
    fn a_named_file_is_relative_to_the_target_spelling_that_holds_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut env = sample_env(dir.path());
        let elsewhere = dir.path().join("a-much-longer-other-target");
        std::fs::create_dir_all(&elsewhere).unwrap();
        env.mappings.insert(0, (elsewhere, TARGET_DIR_PLACEHOLDER));
        let lib = write_lib(dir.path());
        let stdout = format!(
            "cargo:rustc-link-search=native={}\n",
            lib.parent().unwrap().display()
        );
        let contents = collect(&stdout, &env, 0, 0).unwrap();
        assert_eq!(
            contents
                .files
                .iter()
                .map(|(_, name)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["outside/debug/gn_out/obj/librusty_v8.a"]
        );
    }

    /// A named path that cannot be inspected fails the recording; only a
    /// missing one is skipped.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_named_path_is_an_error_and_a_missing_one_is_not() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let locked = dir.path().join("target/debug/locked");
        std::fs::create_dir_all(locked.join("inner")).unwrap();
        let missing = format!(
            "cargo:rustc-link-search=native={}\n",
            dir.path().join("target/debug/absent").display()
        );
        assert!(paths_to_snapshot(&missing, &env).unwrap().is_empty());
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let unreadable = format!(
            "cargo:rustc-link-search=native={}\n",
            locked.join("inner").display()
        );
        let result = paths_to_snapshot(&unreadable, &env);
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
        // Root reads through any mode, so there is nothing to refuse.
        if std::fs::read_dir(dir.path()).is_ok() && unsafe { libc::geteuid() } == 0 {
            return;
        }
        assert!(result.is_err(), "{result:?}");
    }

    fn sample_env(root: &Path) -> Environment {
        let target = root.join("target");
        let out_dir = target.join("debug/build/pkg-1/out");
        let manifest_dir = root.join("pkg");
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::create_dir_all(&manifest_dir).unwrap();
        let mut mappings = vec![
            (out_dir.clone(), "${KACHE_OUT_DIR}"),
            (manifest_dir.clone(), "${KACHE_MANIFEST_DIR}"),
            (target, TARGET_DIR_PLACEHOLDER),
        ];
        mappings.sort_by_key(|(path, _)| std::cmp::Reverse(path.as_os_str().len()));
        Environment {
            out_dir,
            manifest_dir,
            spellings: mappings.clone(),
            mappings,
            base_dirs: Vec::new(),
            dependency_roots: Vec::new(),
            target: None,
            ownership_roots: Vec::new(),
        }
    }

    fn write_lib(root: &Path) -> PathBuf {
        let lib = root.join("target/debug/gn_out/obj/librusty_v8.a");
        std::fs::create_dir_all(lib.parent().unwrap()).unwrap();
        std::fs::write(&lib, "arch").unwrap();
        lib
    }

    #[test]
    fn a_sibling_build_unit_requires_a_dependency_path_inside_that_unit() {
        let dir = tempfile::tempdir().unwrap();
        let mut env = sample_env(dir.path());
        for relative in [
            "debug/build/scratch-1/out/lib",
            "my-custom-profile/build/scratch-1/out/lib",
            "x86_64-unknown-linux-gnu/release/build/scratch/0123456789abcdef/out/lib",
        ] {
            let path = dir.path().join("target").join(relative);
            std::fs::create_dir_all(&path).unwrap();
            std::fs::write(path.join("libdemo.a"), "library").unwrap();
            let stdout = format!("cargo:rustc-link-search=native={}\n", path.display());
            assert!(validate_named_paths(&stdout, &env).is_err());
            assert!(
                collect(&stdout, &env, 0, 0).is_err(),
                "record must refuse the whole run"
            );
            for broad in ["target", "target/debug/build"] {
                env.dependency_roots = vec![dir.path().join(broad)];
                assert!(
                    validate_named_paths(&stdout, &env).is_err(),
                    "a DEP path above the unit is not ownership"
                );
            }
            env.dependency_roots = dependency_roots([(
                "DEP_Z_ROOT".into(),
                path.parent().unwrap().as_os_str().to_os_string(),
            )]);
            validate_named_paths(&stdout, &env).unwrap();
            assert!(
                collect(&stdout, &env, 0, 0).unwrap().is_empty(),
                "the dependency owns this output"
            );
            env.dependency_roots = vec![path.join("too-specific")];
            assert!(validate_named_paths(&stdout, &env).is_err());
            let flag = format!("cargo:rustc-flags=-Lnative={}\n", path.display());
            assert!(validate_named_paths(&flag, &env).is_err());
            env.dependency_roots.clear();
        }
        let own = format!("cargo:rustc-link-search={}\n", env.out_dir.display());
        validate_named_paths(&own, &env).unwrap();
        let beside_own = env.out_dir.parent().unwrap().join("scratch");
        let stdout = format!("cargo:rustc-link-arg=-L{}\n", beside_own.display());
        assert!(
            validate_named_paths(&stdout, &env).is_err(),
            "the rest of our Cargo unit is not OUT_DIR"
        );
    }

    #[test]
    fn captured_target_names_keep_cargo_units_out_of_collected_outputs() {
        for target_name in ["wasm32-wasip1", "wasm32-wasip2", "wasm32v1-none", "board"] {
            for (unit, other_unit) in [
                ("dep-0123456789abcdef", "dep-fedcba9876543210"),
                ("dep/0123456789abcdef", "dep/fedcba9876543210"),
            ] {
                let directory = tempfile::tempdir().unwrap();
                let mut env = sample_env(directory.path());
                env.target = Some(target_name.into());
                let build = directory
                    .path()
                    .join("target")
                    .join(target_name)
                    .join("debug/build");
                let dependency = build.join(unit).join("out");
                let other = build.join(other_unit).join("out/lib");
                std::fs::create_dir_all(dependency.join("lib")).unwrap();
                std::fs::create_dir_all(&other).unwrap();
                std::fs::write(dependency.join("lib/libz.a"), "archive").unwrap();
                let stdout = format!(
                    "cargo:rustc-link-search={}\n",
                    dependency.join("lib").display()
                );
                assert!(
                    validate_named_paths(&stdout, &env).is_err(),
                    "{target_name}/{unit}"
                );
                assert!(collect(&stdout, &env, 0, 0).is_err());
                let build_stdout = format!("cargo:rustc-link-search={}\n", build.display());
                assert!(validate_named_paths(&build_stdout, &env).is_err());
                if unit.contains('/') {
                    env.dependency_roots = dependency_roots([(
                        "DEP_CRATE_ROOT".into(),
                        build.join("dep").into_os_string(),
                    )]);
                    assert!(
                        validate_named_paths(&stdout, &env).is_err(),
                        "a crate directory above multiple hashes is not a dependency unit"
                    );
                }
                env.dependency_roots =
                    dependency_roots([("DEP_Z_ROOT".into(), dependency.into_os_string())]);
                validate_named_paths(&stdout, &env).unwrap();
                assert!(
                    collect(&stdout, &env, 0, 0).unwrap().is_empty(),
                    "Cargo's dependency output is an input, never ours to restore"
                );
                let triple_stdout = format!(
                    "cargo:rustc-link-search={}\n",
                    directory.path().join("target").join(target_name).display()
                );
                assert!(
                    collect(&triple_stdout, &env, 0, 0)
                        .unwrap()
                        .files
                        .is_empty(),
                    "walking an enclosing tree must also skip Cargo-owned children"
                );
                let other_stdout = format!("cargo:rustc-link-search={}\n", other.display());
                assert!(
                    validate_named_paths(&other_stdout, &env).is_err(),
                    "another hash in the same crate is a different unit"
                );
                let standard = directory
                    .path()
                    .join("target/aarch64-apple-darwin/release/build/other-0123456789abcdef/out");
                let standard_stdout = format!("cargo:rustc-link-search={}\n", standard.display());
                assert!(
                    validate_named_paths(&standard_stdout, &env).is_err(),
                    "other standard triples retain their ownership checks when TARGET differs"
                );
                assert!(is_cargo_owned_relative(
                    Path::new(&format!("{target_name}/debug/build/{unit}/out")),
                    env.target.as_deref()
                ));
                assert!(
                    checked_outside(
                        &format!("{target_name}/debug/build/{unit}/out/libz.a"),
                        env.target.as_deref()
                    )
                    .is_err()
                );
                assert!(
                    checked_outside("my-profile/gn_out/build/libdemo.a", env.target.as_deref())
                        .is_ok(),
                    "script-owned build trees remain supported"
                );
                let own = format!("cargo:rustc-link-search={}\n", env.out_dir.display());
                validate_named_paths(&own, &env).unwrap();
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn canonical_profile_and_triple_siblings_keep_their_cargo_owner() {
        for alias in ["profile", "triple"] {
            for (unit, other_unit) in [
                ("dep-0123456789abcdef", "dep-fedcba9876543210"),
                ("dep/0123456789abcdef", "dep/fedcba9876543210"),
            ] {
                let directory = tempfile::tempdir().unwrap();
                let mut env = sample_env(directory.path());
                let target_name = OsStr::new("wasm32-wasip2");
                let target = directory.path().join("target");
                let physical = directory.path().join("physical/renamed-target");
                let physical_profile = physical.join("debug");
                let lexical_triple = target.join(target_name);
                let lexical_profile = lexical_triple.join("debug");
                std::fs::create_dir_all(&physical_profile).unwrap();
                if alias == "profile" {
                    std::fs::create_dir_all(&lexical_triple).unwrap();
                    std::os::unix::fs::symlink(&physical_profile, &lexical_profile).unwrap();
                } else {
                    std::os::unix::fs::symlink(&physical, &lexical_triple).unwrap();
                }
                env.out_dir = lexical_profile.join("build/own-0123456789abcdef/out");
                std::fs::create_dir_all(&env.out_dir).unwrap();
                env.target = Some(target_name.to_os_string());
                env.ownership_roots =
                    super::super::ownership_target_roots(&env.out_dir, env.target.as_deref());
                env.mappings
                    .retain(|(_, placeholder)| *placeholder != "${KACHE_OUT_DIR}");
                env.mappings.push((env.out_dir.clone(), "${KACHE_OUT_DIR}"));
                let lexical_dependency = lexical_profile.join("build").join(unit).join("out");
                std::fs::create_dir_all(lexical_dependency.join("lib")).unwrap();
                std::fs::write(lexical_dependency.join("lib/libz.a"), "archive").unwrap();
                let canonical_dependency = lexical_dependency.canonicalize().unwrap();
                for path in [&lexical_dependency, &canonical_dependency] {
                    let stdout =
                        format!("cargo:rustc-link-search={}\n", path.join("lib").display());
                    assert!(
                        validate_named_paths(&stdout, &env).is_err(),
                        "{alias}: {}",
                        path.display()
                    );
                    assert!(collect(&stdout, &env, 0, 0).is_err());
                }
                let metadata = directory.path().join("metadata-outside-target");
                std::os::unix::fs::symlink(&canonical_dependency, &metadata).unwrap();
                env.dependency_roots =
                    dependency_roots([("DEP_Z_ROOT".into(), metadata.into_os_string())]);
                for path in [&lexical_dependency, &canonical_dependency] {
                    let stdout =
                        format!("cargo:rustc-link-search={}\n", path.join("lib").display());
                    validate_named_paths(&stdout, &env).unwrap();
                    assert!(collect(&stdout, &env, 0, 0).unwrap().is_empty());
                }
                let other = physical_profile
                    .join("build")
                    .join(other_unit)
                    .join("out/lib");
                std::fs::create_dir_all(&other).unwrap();
                let stdout = format!("cargo:rustc-link-search={}\n", other.display());
                assert!(validate_named_paths(&stdout, &env).is_err());
                for out in [&env.out_dir, &env.out_dir.canonicalize().unwrap()] {
                    let stdout = format!("cargo:rustc-link-search={}\n", out.display());
                    validate_named_paths(&stdout, &env).unwrap();
                }
                assert_eq!(
                    env.normalize_str(canonical_dependency.to_str().unwrap()),
                    canonical_dependency.to_str().unwrap(),
                    "ownership views must not change stdout path spelling"
                );
            }
        }
    }

    #[test]
    fn dependency_paths_ignore_relative_metadata_and_keep_absolute_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir
            .path()
            .canonicalize()
            .unwrap()
            .join("target/debug/build/z-1/out");
        let variables = [
            ("DEP_Z_ROOT".into(), root.clone().into_os_string()),
            ("DEP_SAME_ROOT".into(), root.clone().into_os_string()),
            ("DEP_VERSION".into(), "1.2.3".into()),
            ("OTHER".into(), dir.path().as_os_str().to_os_string()),
        ];
        assert_eq!(dependency_roots(variables), vec![root]);
        assert_eq!(
            build_unit(Path::new("debug/build/z-1/out/lib"), None),
            Some(PathBuf::from("debug/build/z-1"))
        );
        assert_eq!(
            build_unit(Path::new("debug/build/z/0123456789abcdef/out"), None),
            Some(PathBuf::from("debug/build/z/0123456789abcdef"))
        );
        for path in [
            "debug/build",
            "debug/deps/lib",
            "debug/custom/build/x",
            "debug",
        ] {
            assert_eq!(build_unit(Path::new(path), None), None, "{path}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn canonical_dependencies_distinguish_missing_paths_from_non_directory_errors() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("regular-file");
        std::fs::write(&file, "not a directory").unwrap();
        let blocked = file.join("child");
        assert_eq!(
            blocked.canonicalize().unwrap_err().kind(),
            std::io::ErrorKind::NotADirectory
        );
        assert!(
            canonical_with_missing_suffix(&blocked).is_none(),
            "a regular file cannot become a missing directory prefix"
        );
        assert_eq!(
            canonical_with_missing_suffix(&directory.path().join("missing/child")),
            Some(
                directory
                    .path()
                    .canonicalize()
                    .unwrap()
                    .join("missing/child")
            )
        );
    }

    #[test]
    fn a_named_cargo_build_root_refuses_recording_even_if_a_dependency_names_it() {
        let directory = tempfile::tempdir().unwrap();
        let mut env = sample_env(directory.path());
        for relative in ["debug/build", "x86_64-unknown-linux-gnu/release/build"] {
            let build = directory.path().join("target").join(relative);
            std::fs::create_dir_all(&build).unwrap();
            std::fs::write(build.join("libdemo.a"), "archive").unwrap();
            let stdout = format!("cargo:rustc-link-search={}\n", build.display());
            assert!(validate_named_paths(&stdout, &env).is_err());
            assert!(collect(&stdout, &env, 0, 0).is_err());
            env.dependency_roots =
                dependency_roots([("DEP_BROAD_ROOT".into(), build.into_os_string())]);
            assert!(
                validate_named_paths(&stdout, &env).is_err(),
                "a Cargo build root is never a dependency unit"
            );
            env.dependency_roots.clear();
        }
    }

    #[cfg(unix)]
    #[test]
    fn dependency_and_named_paths_check_canonical_aliases() {
        let dir = tempfile::tempdir().unwrap();
        let mut env = sample_env(dir.path());
        let sibling = dir.path().join("target/debug/build/z-1/out");
        std::fs::create_dir_all(sibling.join("lib")).unwrap();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&sibling, &alias).unwrap();
        let stdout = format!("cargo:rustc-link-search={}\n", alias.join("lib").display());
        assert!(
            validate_named_paths(&stdout, &env).is_err(),
            "an alias cannot hide a sibling unit"
        );
        let missing = format!(
            "cargo:rustc-link-search={}\n",
            alias.join("missing/nested").display()
        );
        assert!(
            validate_named_paths(&missing, &env).is_err(),
            "missing children still belong to the sibling unit"
        );
        env.dependency_roots =
            dependency_roots([("DEP_Z_ROOT".into(), alias.clone().into_os_string())]);
        assert_eq!(env.dependency_roots.len(), 2);
        validate_named_paths(&stdout, &env).unwrap();
        let canonical = format!(
            "cargo:rustc-link-search={}\n",
            sibling.join("lib").display()
        );
        validate_named_paths(&canonical, &env).unwrap();
        let lexical = format!(
            "cargo:rustc-link-search={}/other/../lib\n",
            sibling.display()
        );
        assert!(
            validate_named_paths(&lexical, &env).is_err(),
            "a parent traversal through a missing directory cannot resolve"
        );
        std::fs::create_dir(sibling.join("other")).unwrap();
        validate_named_paths(&lexical, &env).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn target_aliases_match_dependency_metadata_without_parent_traversal_grants() {
        let directory = tempfile::tempdir().unwrap();
        let mut env = sample_env(directory.path());
        let target = directory.path().join("target");
        let physical = directory.path().join("physical-target");
        std::fs::rename(&target, &physical).unwrap();
        std::os::unix::fs::symlink(&physical, &target).unwrap();
        let root = physical.join("debug/build/z-1/out");
        std::fs::create_dir_all(root.join("lib")).unwrap();
        let metadata = directory.path().join("dependency-metadata");
        std::os::unix::fs::symlink(&root, &metadata).unwrap();
        env.dependency_roots = dependency_roots([("DEP_Z_ROOT".into(), metadata.into_os_string())]);
        let named = target.join("debug/build/z-1/out/lib");
        let stdout = format!("cargo:rustc-link-search={}\n", named.display());
        validate_named_paths(&stdout, &env).unwrap();
        let other = format!(
            "cargo:rustc-link-search={}\n",
            target.join("debug/build/other-1/out/lib").display()
        );
        assert!(validate_named_paths(&other, &env).is_err());

        env.dependency_roots = dependency_roots([(
            "DEP_BROAD_ROOT".into(),
            target.join("debug/build").into_os_string(),
        )]);
        assert!(
            validate_named_paths(&stdout, &env).is_err(),
            "an aliased parent of all units grants nothing"
        );

        let external = directory.path().join("external");
        std::fs::create_dir_all(external.join("child")).unwrap();
        std::os::unix::fs::symlink(external.join("child"), root.join("portal")).unwrap();
        let misleading = root.join("portal/..");
        env.dependency_roots =
            dependency_roots([("DEP_MISLEADING_ROOT".into(), misleading.into_os_string())]);
        assert!(
            validate_named_paths(&stdout, &env).is_err(),
            "a lexical parent must not grant the symlink's apparent parent"
        );

        // A real parent traversal which resolves to this unit is still a
        // valid DEP path. The canonical value establishes the ownership.
        env.dependency_roots =
            dependency_roots([("DEP_Z_ROOT".into(), root.join("lib/..").into_os_string())]);
        validate_named_paths(&stdout, &env).unwrap();

        env.dependency_roots.clear();
        std::fs::create_dir_all(root.join("child")).unwrap();
        std::os::unix::fs::symlink(root.join("child"), env.out_dir.join("portal")).unwrap();
        let disguised = format!(
            "cargo:rustc-link-search={}\n",
            env.out_dir.join("portal/../lib").display()
        );
        assert!(
            validate_named_paths(&disguised, &env).is_err(),
            "a lexical OUT_DIR path cannot hide a sibling unit"
        );
        std::os::unix::fs::symlink(
            physical.join("debug/build/not-created-1/out/child"),
            env.out_dir.join("dangling-portal"),
        )
        .unwrap();
        let unresolved = format!(
            "cargo:rustc-link-search={}\n",
            env.out_dir.join("dangling-portal/../lib").display()
        );
        assert!(
            validate_named_paths(&unresolved, &env).is_err(),
            "an unresolved parent traversal is not lexically assumed safe"
        );
    }

    #[test]
    fn a_named_library_is_kept_and_cargo_directories_are_not() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let lib = write_lib(dir.path());
        let deps = dir.path().join("target/debug/deps/libcargo.a");
        std::fs::create_dir_all(deps.parent().unwrap()).unwrap();
        std::fs::write(&deps, "deps").unwrap();
        for name in ["incremental", "build", ".fingerprint", "examples"] {
            let marker = dir.path().join(format!("target/debug/{name}/marker"));
            std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
            std::fs::write(&marker, name).unwrap();
        }
        let stdout = format!(
            "cargo:rustc-link-search=native={}\n\
             cargo:rustc-link-search=native={}\n\
             cargo:rustc-link-search=/usr/lib\n",
            lib.parent().unwrap().display(),
            deps.parent().unwrap().display(),
        );
        let contents = collect(&stdout, &env, 0, 0).unwrap();
        assert_eq!(
            contents
                .files
                .iter()
                .map(|(_, name)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["outside/debug/gn_out/obj/librusty_v8.a"]
        );
        assert!(
            contents
                .directories
                .iter()
                .all(|name| !name.contains("deps") && !name.contains("/build")),
            "{:?}",
            contents.directories
        );
    }

    #[test]
    fn a_path_inside_out_dir_or_outside_the_target_is_not_a_second_copy() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let inside = env.out_dir.join("lib.a");
        std::fs::write(&inside, "in").unwrap();
        let stdout = format!(
            "cargo:rustc-link-search={}\ncargo:rustc-link-search=/usr/lib\n",
            inside.display()
        );
        let contents = collect(&stdout, &env, 0, 0).unwrap();
        assert!(contents.is_empty(), "{contents:?}");
    }

    #[test]
    fn an_out_dir_that_is_not_a_cargo_directory_is_still_not_copied_again() {
        // `is_inside_out_dir` has to win on its own. The usual OUT_DIR sits
        // under `build/`, which the cargo-owned check would also drop.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let target = root.join("target");
        let out_dir = target.join("debug/gn_out/inside");
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::write(out_dir.join("lib.a"), "in").unwrap();
        // The field is the only record of this OUT_DIR. A mapping-only check
        // would copy it.
        let mut mappings = vec![
            (root.join("pkg"), "${KACHE_MANIFEST_DIR}"),
            (target, TARGET_DIR_PLACEHOLDER),
        ];
        mappings.sort_by_key(|(path, _)| std::cmp::Reverse(path.as_os_str().len()));
        let env = Environment {
            out_dir,
            manifest_dir: root.join("pkg"),
            spellings: mappings.clone(),
            mappings,
            base_dirs: Vec::new(),
            dependency_roots: Vec::new(),
            target: None,
            ownership_roots: Vec::new(),
        };
        let stdout = format!(
            "cargo:rustc-link-search={}\n",
            env.out_dir.join("lib.a").display()
        );
        assert!(collect(&stdout, &env, 0, 0).unwrap().is_empty());
    }

    #[test]
    fn a_canonical_out_dir_spelling_is_not_copied_when_only_the_mapping_has_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let target = root.join("target");
        let spelled = target.join("debug/build/pkg-1/out");
        let canonical = target.join("debug/gn_out/canonical-out");
        std::fs::create_dir_all(&spelled).unwrap();
        std::fs::create_dir_all(&canonical).unwrap();
        std::fs::write(canonical.join("lib.a"), "in").unwrap();
        let mut mappings = vec![
            (spelled.clone(), "${KACHE_OUT_DIR}"),
            (canonical.clone(), "${KACHE_OUT_DIR}"),
            (root.join("pkg"), "${KACHE_MANIFEST_DIR}"),
            (target, TARGET_DIR_PLACEHOLDER),
        ];
        mappings.sort_by_key(|(path, _)| std::cmp::Reverse(path.as_os_str().len()));
        let env = Environment {
            out_dir: spelled,
            manifest_dir: root.join("pkg"),
            spellings: mappings.clone(),
            mappings,
            base_dirs: Vec::new(),
            dependency_roots: Vec::new(),
            target: None,
            ownership_roots: Vec::new(),
        };
        let stdout = format!(
            "cargo:rustc-link-search={}\n",
            canonical.join("lib.a").display()
        );
        assert!(
            collect(&stdout, &env, 0, 0).unwrap().is_empty(),
            "the canonical spelling is still OUT_DIR"
        );
    }

    #[test]
    fn relative_and_lexical_paths_resolve_under_the_target_directory() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let lib = write_lib(dir.path());
        let stdout = "cargo:rustc-link-search=../target/debug/../debug/gn_out/obj\n";
        let contents = collect(stdout, &env, 0, 0).unwrap();
        assert_eq!(contents.files.len(), 1, "{contents:?}");
        assert_eq!(
            std::fs::read(&contents.files[0].0).unwrap(),
            std::fs::read(lib).unwrap()
        );

        let missing = "cargo:rustc-link-search=../target/debug/gn_out/missing\n";
        assert!(collect(missing, &env, 0, 0).unwrap().is_empty());

        let root = format!(
            "cargo:rustc-link-search={}\n",
            dir.path().join("target").display()
        );
        assert!(
            collect(&root, &env, 0, 0).unwrap().is_empty(),
            "the target directory itself is not a script output"
        );
    }

    #[test]
    fn a_parent_path_covers_a_child_and_siblings_both_stay() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let obj = dir.path().join("target/debug/gn_out/obj");
        let other = dir.path().join("target/debug/other_out");
        std::fs::create_dir_all(&obj).unwrap();
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(obj.join("librusty_v8.a"), "a").unwrap();
        std::fs::write(other.join("libother.a"), "b").unwrap();
        let stdout = format!(
            "cargo:rustc-link-search={}\n\
             cargo:rustc-link-search={}\n\
             cargo:rustc-link-search={}\n",
            dir.path().join("target/debug/gn_out").display(),
            obj.display(),
            other.display(),
        );
        let contents = collect(&stdout, &env, 0, 0).unwrap();
        let names: Vec<_> = contents
            .files
            .iter()
            .map(|(_, name)| name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "outside/debug/gn_out/obj/librusty_v8.a",
                "outside/debug/other_out/libother.a",
            ]
        );
    }

    #[test]
    fn cargo_owned_detection_matches_profile_directories_only() {
        let owned = |path: &str| is_cargo_owned_relative(Path::new(path), None);
        for name in ["deps", "incremental", "build", ".fingerprint", "examples"] {
            assert!(owned(&format!("debug/{name}")), "{name}");
            assert!(owned(&format!("debug/{name}/nested")), "{name}");
            assert!(
                owned(&format!("aarch64-apple-darwin/release/{name}")),
                "{name} under a triple"
            );
        }
        assert!(!owned("debug/gn_out"));
        assert!(!owned("debug/gn_out/obj"));
        assert!(
            !owned("debug/deps-extra"),
            "the name has to be the whole component"
        );
        assert!(!owned("deps"), "a profile directory has to sit above it");
        assert!(
            !owned("debug/gn_out/build"),
            "a build directory the script created"
        );
        assert!(
            !owned("my-profile/gn_out/build"),
            "one hyphen is a profile, not a triple"
        );
        assert!(!owned("aarch64-apple-darwin/debug/gn_out"));
        assert!(!owned("aarch64-apple-darwin/gn_out"));
    }

    #[test]
    fn the_cap_is_what_out_dir_left_and_counts_files_together() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let lib = write_lib(dir.path());
        std::fs::write(&lib, "abcd").unwrap();
        let stdout = format!("cargo:rustc-link-search={}\n", lib.display());
        assert!(
            collect(&stdout, &env, (1 << 30) - 4, 0).is_ok(),
            "the cap is inclusive"
        );
        assert!(collect(&stdout, &env, (1 << 30) - 3, 0).is_err());
        assert!(collect(&stdout, &env, 0, 50_000 - 1).is_ok());
        assert!(collect(&stdout, &env, 0, 50_000).is_err());

        let second = lib.parent().unwrap().join("second.a");
        std::fs::write(&second, "efg").unwrap();
        let parent = lib.parent().unwrap();
        let both = format!("cargo:rustc-link-search={}\n", parent.display());
        assert!(collect(&both, &env, (1 << 30) - 7, 0).is_ok());
        assert!(
            collect(&both, &env, (1 << 30) - 6, 0).is_err(),
            "sizes add up"
        );
        assert!(collect(&both, &env, 0, 50_000 - 2).is_ok());
        assert!(collect(&both, &env, 0, 50_000 - 1).is_err());
    }

    #[test]
    fn an_empty_directory_and_an_empty_file_are_recorded_without_an_entry() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let empty_dir = dir.path().join("target/debug/gn_out/empty");
        std::fs::create_dir_all(&empty_dir).unwrap();
        let stdout = format!("cargo:rustc-link-search={}\n", empty_dir.display());
        let contents = collect(&stdout, &env, 1 << 30, 50_000).unwrap();
        assert!(contents.files.is_empty());
        assert!(
            contents
                .directories
                .iter()
                .any(|name| name.ends_with("empty"))
        );

        let empty_file = dir.path().join("target/debug/gn_out/obj/zero");
        std::fs::create_dir_all(empty_file.parent().unwrap()).unwrap();
        std::fs::write(&empty_file, "").unwrap();
        let stdout = format!("cargo:rustc-link-arg={}\n", empty_file.display());
        let contents = collect(&stdout, &env, 1 << 30, 50_000).unwrap();
        assert!(contents.files.is_empty());
        assert_eq!(contents.empty_files.len(), 1);
    }

    #[test]
    fn a_profile_directory_keeps_the_script_tree_and_skips_cargo_children() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        write_lib(dir.path());
        let deps = dir.path().join("target/debug/deps/libcargo.a");
        std::fs::create_dir_all(deps.parent().unwrap()).unwrap();
        std::fs::write(&deps, "no").unwrap();
        let stdout = format!(
            "cargo:rustc-link-search={}\n",
            dir.path().join("target/debug").display()
        );
        let contents = collect(&stdout, &env, 0, 0).unwrap();
        assert!(
            contents
                .files
                .iter()
                .any(|(_, name)| name.ends_with("librusty_v8.a"))
        );
        assert!(
            contents
                .files
                .iter()
                .all(|(_, name)| !name.contains("/deps/"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_inside_the_named_tree_is_kept_and_one_that_leaves_is_not() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let obj = dir.path().join("target/debug/gn_out/obj");
        std::fs::create_dir_all(&obj).unwrap();
        std::fs::write(obj.join("librusty_v8.so.1"), "so").unwrap();
        std::os::unix::fs::symlink("librusty_v8.so.1", obj.join("librusty_v8.so")).unwrap();
        let stdout = format!("cargo:rustc-link-search={}\n", obj.display());
        let contents = collect(&stdout, &env, 0, 0).unwrap();
        assert_eq!(contents.symlinks.len(), 1);
        assert_eq!(contents.symlinks[0].target, "librusty_v8.so.1");
        // The entry cap counts the symlink and the file, not the directory.
        assert!(collect(&stdout, &env, 0, 50_000 - 2).is_ok());
        assert!(collect(&stdout, &env, 0, 50_000 - 1).is_err());

        std::os::unix::fs::symlink("/usr/lib/libz.so", obj.join("escape")).unwrap();
        let error = collect(&stdout, &env, 0, 0).unwrap_err().to_string();
        assert!(error.contains("escape"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_costs_an_entry_and_no_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let obj = dir.path().join("target/debug/gn_out/obj");
        std::fs::create_dir_all(&obj).unwrap();
        std::os::unix::fs::symlink("missing", obj.join("lib.so")).unwrap();
        let stdout = format!("cargo:rustc-link-search={}\n", obj.display());
        assert!(
            collect(&stdout, &env, 1 << 30, 50_000 - 1).is_ok(),
            "no bytes left still fits a symlink"
        );
        assert!(collect(&stdout, &env, 0, 50_000).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_as_the_named_path_is_not_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let real = dir.path().join("target/debug/gn_out/obj");
        std::fs::create_dir_all(&real).unwrap();
        std::fs::write(real.join("librusty_v8.a"), "a").unwrap();
        let link = dir.path().join("target/debug/alias");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let stdout = format!("cargo:rustc-link-search={}\n", link.display());
        let error = collect(&stdout, &env, 0, 0).unwrap_err().to_string();
        assert!(error.contains("symlink"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn a_fifo_in_the_named_tree_is_not_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let env = sample_env(dir.path());
        let obj = dir.path().join("target/debug/gn_out/obj");
        std::fs::create_dir_all(&obj).unwrap();
        let fifo = obj.join("pipe");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        let stdout = format!("cargo:rustc-link-search={}\n", obj.display());
        let error = collect(&stdout, &env, 0, 0).unwrap_err().to_string();
        assert!(error.contains("pipe"), "{error}");
    }
}
