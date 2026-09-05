//! Cache reporting policy over shared filesystem measurements.
//!
//! The cache retains its existing logical-size fallback when private allocation
//! is unknown. Consumers needing explicit uncertainty use `kache_fs` directly.

use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sharing {
    pub shared: bool,
    pub private_bytes: u64,
}

impl Sharing {
    pub fn unknown_for(size: u64) -> Self {
        Self {
            shared: false,
            private_bytes: size,
        }
    }
}

/// Best-effort cache estimate. Callers account for hardlinks from their metadata.
pub fn probe(path: &Path, size: u64) -> Sharing {
    match kache_fs::measure_file(path) {
        Ok(s) => from_measurement(s.sharing, s.unique, size),
        Err(_) => Sharing::unknown_for(size),
    }
}

fn from_measurement(sharing: kache_fs::Sharing, unique: Option<u64>, size: u64) -> Sharing {
    let shared = matches!(
        sharing,
        kache_fs::Sharing::Partial | kache_fs::Sharing::Full
    );
    // Preserve the cache's treatment of delayed allocation and empty maps.
    if !shared && unique == Some(0) {
        return Sharing::unknown_for(size);
    }
    Sharing {
        shared,
        private_bytes: unique.unwrap_or(size).min(size),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn capability_test_probe(path: &Path, size: u64) -> Option<Sharing> {
        let s = kache_fs::measure_file(path).expect("filesystem measurement");
        s.unique
            .map(|_| from_measurement(s.sharing, s.unique, size))
    }

    #[test]
    fn cache_fallback_and_clamp_preserve_existing_estimates() {
        use kache_fs::Sharing as S;
        assert_eq!(
            from_measurement(S::Unknown, None, 4096),
            Sharing::unknown_for(4096)
        );
        assert_eq!(
            from_measurement(S::None, Some(0), 4096),
            Sharing::unknown_for(4096)
        );
        assert_eq!(
            from_measurement(S::None, Some(0), 0),
            Sharing::unknown_for(0)
        );
        assert_eq!(
            from_measurement(S::None, Some(65536), 4096).private_bytes,
            4096
        );
        assert_eq!(
            from_measurement(S::None, Some(2048), 4096).private_bytes,
            2048
        );
        assert_eq!(
            from_measurement(S::Full, Some(0), 4096),
            Sharing {
                shared: true,
                private_bytes: 0
            }
        );
        assert_eq!(
            from_measurement(S::Partial, None, 4096),
            Sharing {
                shared: true,
                private_bytes: 4096
            }
        );
    }

    #[test]
    fn an_ordinary_private_file_is_not_reported_as_shared() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.bin");
        let bytes = vec![0xABu8; 256 * 1024];
        std::fs::write(&path, &bytes).unwrap();

        let s = probe(&path, bytes.len() as u64);
        assert!(
            !s.shared,
            "a freshly written file shares nothing: {s:?} — a false positive here \
             would make `clean` tell users their build outputs are already cached"
        );
        assert_eq!(
            s.private_bytes,
            bytes.len() as u64,
            "all of an unshared file's bytes are reclaimable: {s:?}"
        );
    }

    #[test]
    fn a_missing_file_falls_back_instead_of_failing() {
        let dir = tempfile::tempdir().unwrap();
        let s = probe(&dir.path().join("does-not-exist"), 1234);
        assert_eq!(s, Sharing::unknown_for(1234));
    }

    #[test]
    fn an_empty_file_reclaims_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        std::fs::write(&path, b"").unwrap();
        let s = probe(&path, 0);
        assert_eq!(s.private_bytes, 0, "an empty file frees no bytes: {s:?}");
    }

    #[cfg(unix)]
    #[test]
    fn a_sparse_file_reports_only_its_allocated_bytes_as_private() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse.bin");
        let size = 64 * 1024 * 1024;

        let mut f = std::fs::File::create(&path).unwrap();
        f.seek(SeekFrom::Start(size - 4096)).unwrap();
        f.write_all(&[0x7Eu8; 4096]).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let allocated = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(&path).unwrap().blocks() * 512
        };
        if allocated >= size {
            eprintln!("skipping: {path:?} was not stored sparsely ({allocated} of {size})");
            return;
        }

        let Some(s) = capability_test_probe(&path, size) else {
            eprintln!("skipping: this filesystem has no storage-sharing probe");
            return;
        };
        assert!(
            s.private_bytes < size,
            "a hole is not reclaimable storage: {s:?} for a {size}-byte file \
             holding {allocated} allocated bytes"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_reflinked_copy_is_detected_as_shared_despite_nlink_1() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orig.bin");
        let dst = dir.path().join("clone.bin");
        // Large enough to occupy real extents rather than living inline in the
        // inode, which would leave nothing to share.
        let bytes = vec![0x5Au8; 8 * 1024 * 1024];
        std::fs::write(&src, &bytes).unwrap();

        if crate::link::try_reflink(&src, &dst).is_err() {
            eprintln!("skipping: no reflink support on this filesystem");
            return;
        }

        let size = bytes.len() as u64;
        let meta = std::fs::metadata(&dst).unwrap();
        assert_eq!(
            meta.nlink(),
            1,
            "a clone is a distinct inode — this is precisely why nlink was the \
             wrong signal (#602)"
        );

        let Some(s) = capability_test_probe(&dst, size) else {
            eprintln!("skipping: this filesystem has reflinks but no storage-sharing probe");
            return;
        };
        assert!(
            s.shared,
            "a reflinked clone must be detected as sharing storage: {s:?}"
        );
        assert!(
            s.private_bytes < size,
            "a fully shared clone must not claim to free its whole apparent size \
             ({} of {size} bytes reported private)",
            s.private_bytes
        );
    }
}
