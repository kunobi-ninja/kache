//! macOS `.dSYM` bundles stored file by file (kunobi-ninja/kache#1905).
//!
//! The store holds flat, content-addressed files. A bundle is a directory, so
//! each of its regular files is stored as its own artifact (`<bin>.dsym.<n>`)
//! next to a manifest (`<bin>.dsym.json`) that maps them back to paths inside
//! the bundle. Restore clones each blob into a private directory beside the
//! binary and renames it to `<bin>.dSYM`. On APFS a restore then costs
//! metadata, not a second copy of the debug info.
//!
//! Entries written before this layout hold one `<bin>.dsym.tar` instead;
//! [`super::PostRestoreAction::UnpackDebugBundle`] still restores those.

use super::{Artifact, ArtifactKind};
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

const MANIFEST_SUFFIX: &str = ".dsym.json";
const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    /// Regular files with content, each stored under its own artifact name.
    files: Vec<ManifestFile>,
    /// Empty regular files. The store refuses zero-byte artifacts.
    empty_files: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManifestFile {
    /// Path relative to the bundle root, `/`-separated.
    path: String,
    /// Artifact name of the stored file in the same entry.
    name: String,
}

/// The debug-bundle layout a stored artifact name belongs to, if any.
pub(super) fn classify(name: &str) -> Option<ArtifactKind> {
    let (_, tail) = name.rsplit_once(".dsym.")?;
    match tail {
        "tar" => Some(ArtifactKind::DebugBundle),
        "json" => Some(ArtifactKind::DebugBundleManifest),
        _ if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) => {
            Some(ArtifactKind::DebugBundleFile)
        }
        _ => None,
    }
}

/// A bundle's files and manifest, snapshotted into a staging directory that
/// must outlive the store put which hashes them.
#[derive(Debug)]
pub struct StagedDebugBundle {
    manifest: (PathBuf, String),
    files: Vec<(PathBuf, String)>,
}

impl StagedDebugBundle {
    /// The artifacts to store: the manifest, then each file.
    pub fn into_artifacts(self) -> Vec<Artifact> {
        let artifact = |(path, store_name), kind| Artifact {
            path,
            store_name,
            kind,
            required: false,
        };
        std::iter::once(artifact(self.manifest, ArtifactKind::DebugBundleManifest))
            .chain(
                self.files
                    .into_iter()
                    .map(|file| artifact(file, ArtifactKind::DebugBundleFile)),
            )
            .collect()
    }
}

/// Snapshot `bundle_dir` into `staging_dir` as one artifact per regular file
/// plus a manifest, all named after `stem` (the binary's file name).
///
/// Files are snapshotted, not referenced in place, so the store hashes the
/// bundle as it was when packaged. On APFS the snapshot is a clone. Symlinks
/// are skipped, as the tar layout did: dsymutil writes none.
pub(super) fn stage(
    bundle_dir: &Path,
    stem: &str,
    staging_dir: &Path,
) -> Result<StagedDebugBundle> {
    let mut paths = Vec::new();
    super::platform::collect_files_recursively(bundle_dir, bundle_dir, &mut paths)?;
    paths.sort();
    let mut manifest = Manifest {
        version: MANIFEST_VERSION,
        files: Vec::new(),
        empty_files: Vec::new(),
    };
    let mut files = Vec::new();
    for relative in paths {
        let path = portable_path(&relative)?;
        let source = bundle_dir.join(&relative);
        let len = std::fs::metadata(&source)
            .with_context(|| format!("stat {}", source.display()))?
            .len();
        if len == 0 {
            manifest.empty_files.push(path);
            continue;
        }
        let name = format!("{stem}.dsym.{}", files.len());
        let staged = staging_dir.join(&name);
        snapshot(&source, &staged)?;
        manifest.files.push(ManifestFile {
            path,
            name: name.clone(),
        });
        files.push((staged, name));
    }
    // A restore refuses a manifest with no files, so such a bundle is not
    // worth storing.
    anyhow::ensure!(
        !files.is_empty(),
        "debug bundle has no files with content: {}",
        bundle_dir.display()
    );
    let manifest_name = format!("{stem}{MANIFEST_SUFFIX}");
    let manifest_path = staging_dir.join(&manifest_name);
    std::fs::write(
        &manifest_path,
        serde_json::to_vec(&manifest).context("serializing debug bundle manifest")?,
    )
    .with_context(|| format!("writing {}", manifest_path.display()))?;
    Ok(StagedDebugBundle {
        manifest: (manifest_path, manifest_name),
        files,
    })
}

fn portable_path(relative: &Path) -> Result<String> {
    let parts = relative
        .components()
        .map(|component| component.as_os_str().to_str())
        .collect::<Option<Vec<_>>>()
        .with_context(|| format!("debug bundle path is not UTF-8: {}", relative.display()))?;
    Ok(parts.join("/"))
}

fn snapshot(source: &Path, staged: &Path) -> Result<()> {
    if kache_fs::try_reflink(source, staged).is_err() {
        std::fs::copy(source, staged)
            .with_context(|| format!("copying {} to staging", source.display()))?;
    }
    // Store the same mode whatever dsymutil chose, as the tar layout did.
    kache_fs::set_writable_permissions(staged, false)
        .with_context(|| format!("setting mode on {}", staged.display()))
}

/// Bytes a restore cloned from the store, and bytes it had to copy.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RestoredBundle {
    pub cloned: u64,
    pub copied: u64,
}

/// Restore the bundle described by the manifest artifact `manifest` beside
/// `manifest_target` (`<dir>/<bin>.dsym.json` → `<dir>/<bin>.dSYM`).
///
/// The manifest and every file it names are checked before anything is
/// written, and the bundle is assembled in a private sibling directory that
/// is renamed into place last, so a failed restore never leaves a partial
/// `.dSYM` for lldb to trust. `max_bytes` caps the bundle's total size, like
/// the tar layout's extraction cap. The manifest file itself is not written.
pub fn restore(
    store: &crate::store::Store,
    files: &[crate::store::CachedFile],
    manifest: &crate::store::CachedFile,
    manifest_target: &Path,
    max_bytes: u64,
) -> Result<RestoredBundle> {
    restore_via(
        store,
        files,
        manifest,
        manifest_target,
        max_bytes,
        kache_fs::try_reflink,
    )
}

/// [`restore`] with the clone call passed in, so tests can take either
/// branch on any filesystem.
fn restore_via(
    store: &crate::store::Store,
    files: &[crate::store::CachedFile],
    manifest: &crate::store::CachedFile,
    manifest_target: &Path,
    max_bytes: u64,
    reflink: fn(&Path, &Path) -> std::io::Result<()>,
) -> Result<RestoredBundle> {
    let bundle_dir = bundle_dir_for(manifest_target, MANIFEST_SUFFIX)?;
    let blob = store.blob_path(&manifest.hash);
    let bytes = std::fs::read(&blob)
        .with_context(|| format!("reading debug bundle manifest {}", blob.display()))?;
    let parsed: Manifest =
        serde_json::from_slice(&bytes).context("parsing debug bundle manifest")?;
    anyhow::ensure!(
        parsed.version == MANIFEST_VERSION,
        "unsupported debug bundle manifest version {}",
        parsed.version
    );
    anyhow::ensure!(
        !parsed.files.is_empty(),
        "debug bundle manifest lists no files"
    );

    let mut seen = std::collections::HashSet::new();
    let mut planned = Vec::with_capacity(parsed.files.len());
    let mut total = 0u64;
    for file in &parsed.files {
        let relative = checked_path(&file.path, &mut seen)?;
        let cached = files
            .iter()
            .find(|cached| cached.name == file.name)
            .with_context(|| {
                format!(
                    "debug bundle manifest names a file the entry lacks: {}",
                    file.name
                )
            })?;
        total = total.saturating_add(cached.size);
        anyhow::ensure!(
            total <= max_bytes,
            "debug bundle exceeds the {max_bytes}-byte extraction cap"
        );
        planned.push((relative, store.blob_path(&cached.hash), cached.size));
    }
    let mut empty = Vec::with_capacity(parsed.empty_files.len());
    for path in &parsed.empty_files {
        empty.push(checked_path(path, &mut seen)?);
    }

    let staging = new_bundle_staging(&bundle_dir)?;
    let mut restored = RestoredBundle::default();
    for (relative, blob, size) in planned {
        let dest = create_parent(staging.path(), relative)?;
        if reflink(&blob, &dest).is_ok() {
            kache_store::opcounts::record_reflinked(size);
            restored.cloned += size;
        } else {
            std::fs::copy(&blob, &dest)
                .with_context(|| format!("copying {} to {}", blob.display(), dest.display()))?;
            kache_store::opcounts::record_copied(size);
            restored.copied += size;
        }
        // A clone keeps the blob's read-only mode.
        kache_fs::set_writable_permissions(&dest, false)
            .with_context(|| format!("setting mode on {}", dest.display()))?;
    }
    for relative in empty {
        let dest = create_parent(staging.path(), relative)?;
        std::fs::File::create_new(&dest).with_context(|| format!("creating {}", dest.display()))?;
    }
    publish_bundle(staging, &bundle_dir)?;
    Ok(restored)
}

/// A relative path made only of normal components, not seen before.
///
/// Paths are compared without case: APFS is case-insensitive by default, so
/// two names that differ only in case would land on the same file.
fn checked_path<'a>(
    path: &'a str,
    seen: &mut std::collections::HashSet<String>,
) -> Result<&'a Path> {
    let relative = Path::new(path);
    anyhow::ensure!(
        !path.is_empty()
            && relative
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "debug bundle manifest has an unsafe path: {path}"
    );
    anyhow::ensure!(
        seen.insert(path.to_lowercase()),
        "debug bundle manifest lists {path} twice"
    );
    Ok(relative)
}

fn create_parent(root: &Path, relative: &Path) -> Result<PathBuf> {
    let dest = root.join(relative);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    Ok(dest)
}

/// `<dir>/<bin><suffix>` → `<dir>/<bin>.dSYM`.
pub(super) fn bundle_dir_for(artifact: &Path, suffix: &str) -> Result<PathBuf> {
    let file_name = artifact
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| {
            format!(
                "debug bundle has no usable file name: {}",
                artifact.display()
            )
        })?;
    let stem = file_name
        .strip_suffix(suffix)
        .with_context(|| format!("debug bundle artifact is not a `{suffix}`: {file_name}"))?;
    Ok(artifact.with_file_name(format!("{stem}.dSYM")))
}

/// A private directory beside `bundle_dir` to assemble the bundle in.
pub(super) fn new_bundle_staging(bundle_dir: &Path) -> Result<tempfile::TempDir> {
    let parent = bundle_dir
        .parent()
        .with_context(|| format!("debug bundle has no parent dir: {}", bundle_dir.display()))?;
    tempfile::Builder::new()
        .prefix(".kache-dsym-")
        .tempdir_in(parent)
        .context("creating temp dir for debug bundle restore")
}

/// Replace `bundle_dir` with the fully assembled `staging` directory. A stale
/// `.dSYM` from an earlier build would otherwise shadow the restored one for
/// lldb.
///
/// Whatever is at `bundle_dir` is renamed aside first and deleted only after
/// the new bundle is in place, so the path holds either a complete bundle or
/// nothing, never a half-deleted one. If the final rename fails, the old
/// bundle is moved back.
pub(super) fn publish_bundle(staging: tempfile::TempDir, bundle_dir: &Path) -> Result<()> {
    publish_bundle_via(staging, bundle_dir, &mut |from, to| {
        std::fs::rename(from, to)
    })
}

fn publish_bundle_via(
    staging: tempfile::TempDir,
    bundle_dir: &Path,
    rename: &mut dyn FnMut(&Path, &Path) -> std::io::Result<()>,
) -> Result<()> {
    let aside_dir = new_bundle_staging(bundle_dir)?;
    let aside = aside_dir.path().join("previous");
    let moved_aside = match rename(bundle_dir, &aside) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("moving aside stale bundle {}", bundle_dir.display()));
        }
    };
    if let Err(error) = rename(staging.path(), bundle_dir) {
        if moved_aside {
            let _ = rename(&aside, bundle_dir);
        }
        return Err(error).with_context(|| {
            format!(
                "publishing debug bundle {} -> {}",
                staging.path().display(),
                bundle_dir.display()
            )
        });
    }
    // The directory now lives at `bundle_dir`; nothing is left to clean up.
    let _ = staging.keep();
    // Dropping `aside_dir` deletes the old bundle.
    Ok(())
}

#[cfg(test)]
mod tests;
