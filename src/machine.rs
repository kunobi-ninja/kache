//! Machine-readable CLI output, and the store-vs-disk split that output uses.
//!
//! Interactive surfaces (`kache monitor`, `kache config`, the `clean` selector)
//! stay human. Agents get `--json` on the commands that diagnose and change
//! disk: stats, gc, clean, doctor, why-miss, list, daemon status.

use anyhow::Result;
pub use kache_store::filesystem::*;
use serde::Serialize;
use std::path::Path;

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

pub fn require_tty(is_tty: bool, command: &str, alternative: &str) -> Result<()> {
    if is_tty {
        Ok(())
    } else {
        anyhow::bail!(
            "`kache {command}` needs a terminal. For scripts and agents, use {alternative}."
        );
    }
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

/// Walk `store_dir/blobs` and split apparent blob bytes into private vs cloned.
pub fn disk_view(store_dir: &Path, store_bytes: u64, store_limit_bytes: u64) -> DiskView {
    let probed = probe_store_blobs(store_dir);
    disk_view_from_probe(store_bytes, store_limit_bytes, probed)
}

fn disk_view_from_probe(store_bytes: u64, store_limit_bytes: u64, probed: ProbeTotals) -> DiskView {
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

#[derive(Debug, Default)]
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
    clean_tracked_targets_action()
}

fn clean_tracked_targets_action() -> Vec<NextAction> {
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
        clean_tracked_targets_action()
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn disk_view_scales_the_probe_split_to_the_indexed_store_size() {
        let view = disk_view_from_probe(
            1_000,
            2_000,
            ProbeTotals {
                apparent_bytes: 400,
                cloned_bytes: 100,
            },
        );
        assert_eq!(view.disk_private_bytes, 750);
        assert_eq!(view.cloned_into_targets_bytes, 250);
        assert_eq!(view.store_bytes, 1_000);
        assert_eq!(view.store_limit_bytes, 2_000);
    }

    #[test]
    fn blob_probe_ignores_files_outside_shard_directories() {
        let dir = tempfile::tempdir().unwrap();
        let blobs = dir.path().join("blobs");
        std::fs::create_dir_all(&blobs).unwrap();
        std::fs::write(blobs.join("not-a-shard"), vec![0u8; 99]).unwrap();
        let shard = blobs.join("aa");
        std::fs::create_dir_all(&shard).unwrap();
        std::fs::write(shard.join("blob"), vec![0u8; 7]).unwrap();

        let totals = probe_store_blobs(dir.path());
        assert_eq!(totals.apparent_bytes, 7);
    }

    #[test]
    fn terminal_requirement_names_the_command_and_alternative() {
        assert!(require_tty(true, "config", "the alternative").is_ok());
        let error = require_tty(false, "config", "the alternative")
            .unwrap_err()
            .to_string();
        assert!(error.contains("kache config"), "{error}");
        assert!(error.contains("the alternative"), "{error}");
    }

    #[test]
    fn next_for_clones_is_silent_when_nothing_is_cloned() {
        assert!(next_for_clones(0).is_empty());
        assert_eq!(next_for_clones(1)[0].argv[1], "clean");
    }

    #[test]
    fn next_after_gc_covers_each_retention_signal_boundary() {
        let empty = DiskView {
            store_bytes: 0,
            store_limit_bytes: 0,
            disk_private_bytes: 0,
            cloned_into_targets_bytes: 0,
            cloned_coverage: ClonedCoverage::Unknown,
        };
        assert!(next_after_gc(&empty, 0, 0, 0).is_empty());
        assert!(!next_after_gc(&empty, 1, 0, 0).is_empty());
        assert!(!next_after_gc(&empty, 0, 0, 1).is_empty());
        assert!(next_after_gc(&empty, 0, 1, 1).is_empty());

        let cloned = DiskView {
            cloned_into_targets_bytes: 1,
            ..empty
        };
        assert!(!next_after_gc(&cloned, 0, 0, 0).is_empty());
    }
}
