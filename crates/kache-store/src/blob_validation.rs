use anyhow::{Context, Result, ensure};
use std::{fs, path::Path};

/// Check type and size from one metadata read. Callers own content verification
/// and any repair or hit accounting after this check.
pub(crate) fn validate_blob_metadata(blob: &Path, expected_size: u64) -> Result<()> {
    let metadata = fs::metadata(blob).context("reading blob metadata")?;
    ensure!(metadata.is_file(), "blob is not a regular file");
    ensure!(
        metadata.len() == expected_size,
        "blob size mismatch (expected {expected_size}, got {})",
        metadata.len()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_matching_files_including_empty_metadata_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob");
        for contents in [b"".as_slice(), b"artifact bytes".as_slice()] {
            fs::write(&blob, contents).unwrap();
            validate_blob_metadata(&blob, contents.len() as u64).unwrap();
        }
    }

    #[test]
    fn rejects_both_shorter_and_longer_files() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob");
        fs::write(&blob, b"12345").unwrap();
        for expected_size in [0, 4, 6] {
            assert!(validate_blob_metadata(&blob, expected_size).is_err());
        }
    }

    #[test]
    fn rejects_directories_even_when_the_size_matches() {
        let dir = tempfile::tempdir().unwrap();
        let size = fs::metadata(dir.path()).unwrap().len();
        assert!(validate_blob_metadata(dir.path(), size).is_err());
    }

    #[test]
    fn rejects_metadata_errors() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob");
        assert!(validate_blob_metadata(&blob, 0).is_err());
        fs::write(&blob, b"bytes").unwrap();
        assert!(validate_blob_metadata(&blob.join("child"), 0).is_err());
    }
}
