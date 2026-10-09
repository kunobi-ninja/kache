//! Stat-walk stamps of directory trees, and the digests memoised under them.
//!
//! A content digest of a tree reads every file in it. One walk that stats
//! every entry yields a stamp of the tree's names, kinds, sizes and times;
//! while a tree has the stamp a digest was recorded under, that digest still
//! holds and no file needs reading.

use std::path::{Path, PathBuf};

/// A digest of every entry under `path` by name, kind, size, modification
/// and change time, from one stat walk. Symlinks contribute their link text
/// only, so a tree with a symlink to something outside it is not memoised.
/// `None` when the tree is larger than the budget or holds a symlink.
pub(crate) struct TreeStamp {
    pub(crate) digest: String,
    /// The newest modification time seen in the walk.
    newest: std::time::SystemTime,
}

impl TreeStamp {
    /// Coarse filesystem clocks make a stamp taken within this window of its
    /// newest write ambiguous.
    pub(crate) const SETTLE: std::time::Duration = std::time::Duration::from_secs(2);

    pub(crate) fn settled_at(&self, now: std::time::SystemTime) -> bool {
        now.duration_since(self.newest)
            .is_ok_and(|age| age >= Self::SETTLE)
    }
}

pub(crate) fn tree_stamp(path: &Path, excluded: &[PathBuf], budget: usize) -> Option<TreeStamp> {
    let mut hasher = blake3::Hasher::new();
    let mut newest = std::time::SystemTime::UNIX_EPOCH;
    let mut remaining = budget;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let mut entries: Vec<_> = std::fs::read_dir(&directory)
            .ok()?
            .collect::<std::io::Result<_>>()
            .ok()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let child = entry.path();
            if excluded.contains(&child) {
                continue;
            }
            remaining = remaining.checked_sub(1)?;
            let metadata = std::fs::symlink_metadata(&child).ok()?;
            if metadata.file_type().is_symlink() {
                return None;
            }
            fold(
                &mut hasher,
                "entry",
                child
                    .strip_prefix(path)
                    .ok()?
                    .as_os_str()
                    .as_encoded_bytes(),
            );
            hasher.update(if metadata.is_dir() { b"dir" } else { b"fil" });
            fold_metadata_stamp(&mut hasher, &metadata);
            if let Ok(modified) = metadata.modified()
                && modified > newest
            {
                newest = modified;
            }
            if metadata.is_dir() {
                pending.push(child);
            }
        }
    }
    Some(TreeStamp {
        digest: hasher.finalize().to_hex().to_string(),
        newest,
    })
}

fn fold(hasher: &mut blake3::Hasher, label: &str, value: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label.as_bytes());
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

/// Size and times of one entry, plus the inode where the platform has one.
fn fold_metadata_stamp(hasher: &mut blake3::Hasher, metadata: &std::fs::Metadata) {
    hasher.update(&metadata.len().to_le_bytes());
    for time in [metadata.modified().ok(), metadata.created().ok()] {
        let nanos = time
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map_or(0, |d| d.as_nanos());
        hasher.update(&nanos.to_le_bytes());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        for value in [
            metadata.ctime() as u64,
            metadata.ctime_nsec() as u64,
            metadata.ino(),
        ] {
            hasher.update(&value.to_le_bytes());
        }
    }
}

/// The digest recorded at `memo`, when it was recorded under `stamp`.
pub(crate) fn memoised_digest(memo: &Path, stamp: &str) -> Option<String> {
    let memo = std::fs::read_to_string(memo).ok()?;
    let (recorded_stamp, digest) = memo.trim_end().split_once('\n')?;
    (recorded_stamp == stamp).then(|| digest.to_string())
}

/// Record `digest` at `memo` under `stamp`.
pub(crate) fn record_digest(memo: &Path, stamp: &str, digest: &str) {
    crate::probe_memo::write_atomic(memo, &format!("{stamp}\n{digest}"));
}
