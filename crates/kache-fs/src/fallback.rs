//! Measurements when extent sharing cannot be queried. Private bytes remain unknown.

use std::io;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use crate::{FileSizing, ProbeCaps, Sharing, SizeProbe};

pub struct FallbackProbe;

impl SizeProbe for FallbackProbe {
    fn measure_file(&self, path: &Path) -> io::Result<FileSizing> {
        let md = std::fs::symlink_metadata(path)?;

        let (inode, nlink) = crate::identity::metadata_identity(path, &md, false)?;
        #[cfg(unix)]
        let allocated = md.blocks().saturating_mul(512);
        #[cfg(not(unix))]
        let allocated = md.len();

        Ok(FileSizing {
            logical: md.len(),
            allocated,
            unique: None,
            sharing: Sharing::Unknown,
            inode,
            nlink,
            clone_id: None,
        })
    }

    fn capabilities(&self) -> ProbeCaps {
        ProbeCaps {
            unique_bytes: false,
            clone_refcount: false,
            name: "fallback",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Confidence, InodeLedger, testutil::TempDir};

    #[test]
    fn reports_unknown_rather_than_claiming_nothing_is_shared() {
        let d = TempDir::new("fallback");
        let f = d.write("f", 8192);

        let s = FallbackProbe.measure_file(&f).unwrap();
        assert_eq!(s.unique, None, "must not pretend every byte is reclaimable");
        assert_eq!(s.sharing, Sharing::Unknown);
        assert_eq!(s.logical, 8192);
        assert!(!s.frees_nothing());
        assert_eq!(s.shared(), None);
    }

    #[test]
    fn aggregates_are_marked_estimated() {
        let d = TempDir::new("fallback-dir");
        d.write("a", 4096);
        d.write("b", 4096);

        let mut ledger = InodeLedger::new();
        let s = FallbackProbe.measure_dir(d.path(), &mut ledger).unwrap();

        assert_eq!(s.files, 2);
        assert_eq!(s.confidence, Confidence::Estimated);
        assert_eq!(
            s.shared_ratio(),
            None,
            "an estimate must not be quoted as a ratio"
        );
        assert_eq!(s.unique, 0, "unknown bytes are never promised as reclaim");
        assert_eq!(s.unknown_files, 2);
        assert_eq!(s.unknown_allocated_bytes, s.allocated);
    }

    #[test]
    fn missing_files_error_rather_than_returning_zeros() {
        assert!(
            FallbackProbe
                .measure_file(Path::new("/nonexistent/cache-fs"))
                .is_err()
        );
    }
}
