//! Machine-readable CLI output, and the store-vs-disk split that output uses.
//!
//! Interactive surfaces (`kache monitor`, `kache config`, the `clean` selector)
//! stay human. Agents get `--json` on the commands that diagnose and change
//! disk: stats, gc, clean, doctor, why-miss, list, daemon status.

use anyhow::Result;
use serde::Serialize;
use std::io::IsTerminal;
use std::path::Path;

use crate::sharing::Sharing;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct PathIdentity {
    pub device: u64,
    pub inode: u64,
}

/// Stable directory identity used to reject moved or replaced tracked targets.
pub fn directory_identity(path: &Path) -> Option<PathIdentity> {
    let meta = std::fs::symlink_metadata(path).ok()?;
    if !meta.file_type().is_dir() {
        return None;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(PathIdentity {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        Some(PathIdentity {
            device: meta.volume_serial_number()? as u64,
            inode: meta.file_index()?,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

/// A tracked cleanup target must be a derived directory, never a source root
/// or an ancestor of one.
pub fn target_root_is_safe(target: &Path, workspace_root: &Path) -> bool {
    let Ok(target) = std::path::absolute(target) else {
        return false;
    };
    let Ok(workspace) = std::path::absolute(workspace_root) else {
        return false;
    };
    let cargo_markers = std::fs::read_to_string(target.join("CACHEDIR.TAG"))
        .is_ok_and(|tag| tag.contains("Signature: 8a477f597d28d172789f06886806bc55"))
        && (target.join(".rustc_info.json").is_file()
            || target.join("debug").is_dir()
            || target.join("release").is_dir());
    cargo_markers
        && target.parent().is_some()
        && target != workspace
        && !workspace.starts_with(&target)
        && directory_identity(&target).is_some()
}

/// JSON document version. Bump on breaking field/meaning changes; additive
/// fields do not bump it.
pub const SCHEMA_VERSION: u32 = 1;

/// How complete the clone probe is on this platform.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ClonedCoverage {
    /// macOS getattrlist private/shared sizes are trustworthy.
    Full,
    /// Linux FIEMAP works on some filesystems and not others.
    Partial,
    /// No read-side clone query (Windows ReFS clone is write-only).
    Unknown,
}

/// Bytes the store names vs bytes the filesystem would actually give back.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DiskView {
    pub store_bytes: u64,
    pub store_limit_bytes: u64,
    pub disk_private_bytes: u64,
    pub cloned_into_targets_bytes: u64,
    pub cloned_coverage: ClonedCoverage,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NextAction {
    pub argv: Vec<String>,
    pub why: String,
}

#[derive(Debug, Serialize)]
struct JsonDoc<T: Serialize> {
    schema_version: u32,
    command: &'static str,
    #[serde(flatten)]
    body: T,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    next: Vec<NextAction>,
}

/// Write one JSON document to stdout.
pub fn emit<T: Serialize>(command: &'static str, body: T, next: Vec<NextAction>) -> Result<()> {
    let doc = JsonDoc {
        schema_version: SCHEMA_VERSION,
        command,
        body,
        next,
    };
    serde_json::to_writer_pretty(std::io::stdout(), &doc)?;
    println!();
    Ok(())
}

pub fn stdout_is_tty() -> bool {
    std::io::stdout().is_terminal()
}

pub fn refuse_tui(command: &str, alternative: &str) -> Result<()> {
    anyhow::bail!("`kache {command}` needs a terminal. For scripts and agents, use {alternative}.");
}

pub fn cloned_coverage() -> ClonedCoverage {
    if cfg!(target_os = "macos") {
        ClonedCoverage::Full
    } else if cfg!(target_os = "linux") {
        ClonedCoverage::Partial
    } else {
        ClonedCoverage::Unknown
    }
}

/// Best-effort bytes the filesystem would reclaim by unlinking this blob.
pub fn blob_reclaimable_bytes(path: &Path) -> Option<u64> {
    retainer_from_meta(path).map(|r| r.private_bytes)
}

/// Would unlinking this store name free none of the blob's blocks?
pub fn blob_has_external_retainer(path: &Path) -> bool {
    blob_reclaimable_bytes(path) == Some(0)
}

struct BlobRetainer {
    size: u64,
    cloned: bool,
    private_bytes: u64,
}

fn retainer_from_meta(path: &Path) -> Option<BlobRetainer> {
    let meta = std::fs::metadata(path).ok()?;
    if !meta.is_file() {
        return None;
    }
    let size = meta.len();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.nlink() > 1 {
            return Some(BlobRetainer {
                size,
                cloned: true,
                private_bytes: 0,
            });
        }
    }
    let sharing = crate::sharing::probe(path, size);
    Some(retainer_from_sharing(size, sharing))
}

fn retainer_from_sharing(size: u64, sharing: Sharing) -> BlobRetainer {
    let cloned = sharing.shared && sharing.private_bytes == 0;
    BlobRetainer {
        size,
        cloned,
        private_bytes: if cloned {
            0
        } else {
            sharing.private_bytes.min(size)
        },
    }
}

/// Walk `store_dir/blobs` and split apparent blob bytes into private vs cloned.
pub fn disk_view(store_dir: &Path, store_bytes: u64, store_limit_bytes: u64) -> DiskView {
    let probed = probe_store_blobs(store_dir);
    // Prefer the indexed store size when the walk and the index disagree:
    // the index is what `max_size` bounds. Scale the probe split to it when
    // the walk found anything.
    let (private, cloned) = if probed.apparent_bytes == 0 {
        (store_bytes, 0)
    } else {
        let cloned = ((probed.cloned_bytes as u128 * store_bytes as u128)
            / probed.apparent_bytes as u128) as u64;
        (store_bytes.saturating_sub(cloned), cloned)
    };
    DiskView {
        store_bytes,
        store_limit_bytes,
        disk_private_bytes: private,
        cloned_into_targets_bytes: cloned,
        cloned_coverage: cloned_coverage(),
    }
}

struct ProbeTotals {
    apparent_bytes: u64,
    cloned_bytes: u64,
}

fn probe_store_blobs(store_dir: &Path) -> ProbeTotals {
    let mut totals = ProbeTotals {
        apparent_bytes: 0,
        cloned_bytes: 0,
    };
    let blobs_dir = store_dir.join("blobs");
    let Ok(shards) = std::fs::read_dir(&blobs_dir) else {
        return totals;
    };
    for shard in shards.flatten() {
        let Ok(file_type) = shard.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }
        let Ok(blobs) = std::fs::read_dir(shard.path()) else {
            continue;
        };
        for blob in blobs.flatten() {
            let Some(retainer) = retainer_from_meta(&blob.path()) else {
                continue;
            };
            totals.apparent_bytes = totals.apparent_bytes.saturating_add(retainer.size);
            if retainer.cloned {
                totals.cloned_bytes = totals.cloned_bytes.saturating_add(retainer.size);
            } else {
                let cloned = retainer.size.saturating_sub(retainer.private_bytes);
                totals.cloned_bytes = totals.cloned_bytes.saturating_add(cloned);
            }
        }
    }
    totals
}

pub fn next_for_clones(cloned_bytes: u64) -> Vec<NextAction> {
    if cloned_bytes == 0 {
        return Vec::new();
    }
    vec![NextAction {
        argv: vec![
            "kache".into(),
            "clean".into(),
            "--tracked".into(),
            "--stale".into(),
            "14d".into(),
            "--dry-run".into(),
        ],
        why: "tracked build outputs still hold blocks; cleaning stale target directories is what frees disk".into(),
    }]
}

pub fn next_after_gc(
    disk: &DiskView,
    unreclaimable: usize,
    disk_reclaimed: u64,
    store_removed: u64,
) -> Vec<NextAction> {
    let leftover = store_removed.saturating_sub(disk_reclaimed);
    if unreclaimable > 0 || leftover > 0 || disk.cloned_into_targets_bytes > 0 {
        next_for_clones(disk.cloned_into_targets_bytes.max(leftover))
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retainer_from_sharing_treats_fully_cloned_as_unreclaimable() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: true,
                private_bytes: 0,
            },
        );
        assert!(r.cloned);
        assert_eq!(r.private_bytes, 0);
    }

    #[test]
    fn retainer_from_sharing_treats_private_file_as_reclaimable() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: false,
                private_bytes: 4096,
            },
        );
        assert!(!r.cloned);
        assert_eq!(r.private_bytes, 4096);
    }

    #[test]
    fn retainer_preserves_partial_reclaim_measurement() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: true,
                private_bytes: 1024,
            },
        );
        assert!(!r.cloned, "partly private blobs can reclaim some disk");
        assert_eq!(r.private_bytes, 1024);
    }

    #[test]
    fn cleanup_target_cannot_be_a_source_root_or_its_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let target = workspace.join("target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();

        assert!(target_root_is_safe(&target, &workspace));
        assert!(!target_root_is_safe(&workspace, &workspace));
        assert!(!target_root_is_safe(dir.path(), &workspace));
    }

    #[test]
    fn disk_view_on_empty_store_is_all_private() {
        let dir = tempfile::tempdir().unwrap();
        let view = disk_view(dir.path(), 0, 1024);
        assert_eq!(view.store_bytes, 0);
        assert_eq!(view.disk_private_bytes, 0);
        assert_eq!(view.cloned_into_targets_bytes, 0);
        assert_eq!(view.store_limit_bytes, 1024);
    }

    #[test]
    fn next_for_clones_is_silent_when_nothing_is_cloned() {
        assert!(next_for_clones(0).is_empty());
        assert_eq!(next_for_clones(1)[0].argv[1], "clean");
    }
}
