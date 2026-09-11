//! Disk use of a bench run's storage pools, read from the filesystem.
//!
//! A run leaves three pools behind: the cache directory and each clone's
//! objdir. Summing file lengths per pool overstates what they occupy:
//!
//! - a hardlinked file is one inode reached from two paths, so it is counted
//!   once per link;
//! - a backend may move the objdir into its cache directory and leave a
//!   symlink in the clone, so the same bytes land in two pools;
//! - a file's length is not the space allocated for it.
//!
//! [`measure`] resolves each pool's root, skips an objdir that resolves into
//! the cache directory while walking the cache, counts every inode once
//! (files and directories, as `du` does), and sums allocated blocks. A symlink
//! inside a pool is not followed and its own few bytes are not counted.
//!
//! Reflinks are not detected. A reflinked copy is a second inode that shares
//! extents with the first, so on a CoW filesystem (APFS, btrfs, XFS) the
//! shared bytes are counted once per copy. Linux runners on ext4 cannot
//! reflink, so their numbers are exact.
//!
//! Outside Unix the standard library exposes neither inode identity nor
//! allocated blocks, so every path counts at its length. A directory that
//! cannot be read measures as empty: these numbers are reported, never gated
//! on.

use std::collections::HashSet;
use std::fs::Metadata;
use std::path::{Path, PathBuf};

/// Bytes allocated to each pool, and to all of them together.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DiskUsage {
    /// The cache directory, without any objdir that resolves inside it.
    pub cache_bytes: u64,
    /// Each objdir, in the order given, measured where it resolves to.
    pub objdir_bytes: Vec<u64>,
    /// Every pool together, each inode once: what the run occupies on disk.
    pub total_bytes: u64,
}

/// Measure `cache_dir` and `objdirs`. A pool that does not exist measures 0.
pub(crate) fn measure(cache_dir: &Path, objdirs: &[PathBuf]) -> DiskUsage {
    let objdir_roots: Vec<Option<PathBuf>> = objdirs
        .iter()
        .map(|dir| std::fs::canonicalize(dir).ok())
        .collect();
    let relocated: Vec<PathBuf> = objdir_roots.iter().flatten().cloned().collect();
    let mut total = Tally::default();
    let cache_bytes = std::fs::canonicalize(cache_dir)
        .map(|root| pool_bytes(&root, &relocated, &mut total))
        .unwrap_or(0);
    let objdir_bytes = objdir_roots
        .iter()
        .map(|root| {
            root.as_deref()
                .map_or(0, |root| pool_bytes(root, &[], &mut total))
        })
        .collect();
    DiskUsage {
        cache_bytes,
        objdir_bytes,
        total_bytes: total.bytes,
    }
}

/// Allocated bytes under `root` (not counting `root` itself), each inode once,
/// without entering `skip` or following symlinks. Every inode counted here is
/// also offered to `total`, which keeps its own record of what it has seen.
fn pool_bytes(root: &Path, skip: &[PathBuf], total: &mut Tally) -> u64 {
    // An objdir that is the cache directory itself is measured as the objdir.
    if skip.iter().any(|dir| dir == root) {
        return 0;
    }
    let mut pool = Tally::default();
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(metadata) = path.symlink_metadata() else {
                continue;
            };
            if metadata.file_type().is_symlink() || skip.contains(&path) {
                continue;
            }
            if pool.add(&metadata) {
                total.add(&metadata);
            }
            if metadata.is_dir() {
                dirs.push(path);
            }
        }
    }
    pool.bytes
}

#[derive(Default)]
struct Tally {
    seen: HashSet<(u64, u64)>,
    bytes: u64,
}

impl Tally {
    /// Count `metadata` unless its inode was already counted. Returns whether
    /// it was counted.
    fn add(&mut self, metadata: &Metadata) -> bool {
        if file_id(metadata).is_some_and(|id| !self.seen.insert(id)) {
            return false;
        }
        self.bytes += allocated_bytes(metadata);
        true
    }
}

#[cfg(unix)]
fn file_id(metadata: &Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((metadata.dev(), metadata.ino()))
}

/// No stable inode identity on this platform: every path counts.
#[cfg(not(unix))]
fn file_id(_metadata: &Metadata) -> Option<(u64, u64)> {
    None
}

/// `st_blocks` is in 512-byte units on every Unix, whatever the block size.
#[cfg(unix)]
fn allocated_bytes(metadata: &Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks() * 512
}

#[cfg(not(unix))]
fn allocated_bytes(metadata: &Metadata) -> u64 {
    metadata.len()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    fn allocated(path: &Path) -> u64 {
        allocated_bytes(&std::fs::symlink_metadata(path).unwrap())
    }

    fn write(path: &Path, len: usize) -> u64 {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, vec![7u8; len]).unwrap();
        allocated(path)
    }

    #[test]
    fn allocated_bytes_are_blocks_not_length() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("one-byte");
        std::fs::write(&file, b"x").unwrap();
        let metadata = std::fs::metadata(&file).unwrap();
        use std::os::unix::fs::MetadataExt;
        assert_eq!(allocated_bytes(&metadata), metadata.blocks() * 512);
        assert!(allocated_bytes(&metadata) > metadata.len());
    }

    #[test]
    fn disjoint_pools_add_up() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let obj_a = root.path().join("a/target");
        let obj_b = root.path().join("b/target");
        let cache_bytes = write(&cache.join("blob"), 10_000) + write(&cache.join("index"), 3_000);
        let a_bytes = write(&obj_a.join("liba.rlib"), 20_000);
        let b_bytes = write(&obj_b.join("libb.rlib"), 40_000);

        let usage = measure(&cache, &[obj_a, obj_b]);

        assert_eq!(usage.cache_bytes, cache_bytes);
        assert_eq!(usage.objdir_bytes, vec![a_bytes, b_bytes]);
        assert_eq!(usage.total_bytes, cache_bytes + a_bytes + b_bytes);
    }

    #[test]
    fn subdirectories_count_their_own_blocks() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let file_bytes = write(&cache.join("store/blob"), 10_000);
        let dir_bytes = allocated(&cache.join("store"));

        let usage = measure(&cache, &[]);

        assert_eq!(usage.cache_bytes, file_bytes + dir_bytes);
        assert_eq!(usage.total_bytes, file_bytes + dir_bytes);
    }

    #[test]
    fn a_hardlink_is_counted_once_per_pool_and_once_in_total() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let objdir = root.path().join("clone/target");
        let blob = cache.join("blob");
        let blob_bytes = write(&blob, 50_000);
        std::fs::hard_link(&blob, cache.join("key")).unwrap();
        let own_bytes = write(&objdir.join("liby.rlib"), 8_000);
        std::fs::hard_link(&blob, objdir.join("libx.rlib")).unwrap();

        let usage = measure(&cache, std::slice::from_ref(&objdir));

        assert_eq!(usage.cache_bytes, blob_bytes);
        assert_eq!(usage.objdir_bytes, vec![blob_bytes + own_bytes]);
        assert_eq!(usage.total_bytes, blob_bytes + own_bytes);
    }

    #[test]
    fn an_objdir_moved_into_the_cache_counts_once_and_not_as_cache() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let store_bytes = write(&cache.join("blob"), 30_000);
        let managed = cache.join("targets/v1/abc");
        let target_bytes = write(&managed.join("polkadot"), 90_000);
        // The directories above the moved objdir belong to the cache.
        let parents = allocated(&cache.join("targets")) + allocated(&cache.join("targets/v1"));
        let clone = root.path().join("clone");
        std::fs::create_dir_all(&clone).unwrap();
        std::os::unix::fs::symlink(&managed, clone.join("target")).unwrap();

        let usage = measure(&cache, &[clone.join("target")]);

        assert_eq!(usage.cache_bytes, store_bytes + parents);
        assert_eq!(usage.objdir_bytes, vec![target_bytes]);
        assert_eq!(usage.total_bytes, store_bytes + parents + target_bytes);
    }

    #[test]
    fn a_symlink_inside_a_pool_is_not_followed() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let outside = root.path().join("outside");
        write(&outside.join("big"), 100_000);
        let own_bytes = write(&cache.join("own"), 2_000);
        std::os::unix::fs::symlink(outside.join("big"), cache.join("file-link")).unwrap();
        std::os::unix::fs::symlink(&outside, cache.join("dir-link")).unwrap();

        let usage = measure(&cache, &[]);

        assert_eq!(usage.cache_bytes, own_bytes);
        assert_eq!(usage.total_bytes, own_bytes);
    }

    #[test]
    fn an_objdir_that_is_the_cache_dir_is_not_counted_as_cache() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let bytes = write(&cache.join("blob"), 10_000);

        let usage = measure(&cache, std::slice::from_ref(&cache));

        assert_eq!(usage.cache_bytes, 0);
        assert_eq!(usage.objdir_bytes, vec![bytes]);
        assert_eq!(usage.total_bytes, bytes);
    }

    #[test]
    fn the_same_objdir_twice_counts_once_in_total() {
        let root = tempfile::tempdir().unwrap();
        let objdir = root.path().join("target");
        let bytes = write(&objdir.join("artifact"), 10_000);

        let usage = measure(&root.path().join("no-cache"), &[objdir.clone(), objdir]);

        assert_eq!(usage.objdir_bytes, vec![bytes, bytes]);
        assert_eq!(usage.total_bytes, bytes);
    }

    #[test]
    fn missing_pools_measure_zero() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache");
        let obj_bytes = write(&root.path().join("obj/x"), 5_000);

        let usage = measure(
            &cache,
            &[root.path().join("absent"), root.path().join("obj")],
        );

        assert_eq!(usage.cache_bytes, 0);
        assert_eq!(usage.objdir_bytes, vec![0, obj_bytes]);
        assert_eq!(usage.total_bytes, obj_bytes);
    }
}
