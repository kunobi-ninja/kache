//! Sample a volume's file-write clock without substituting the wall clock.
use std::{fs, io, path::Path, time::SystemTime};

/// Write and close a sibling probe before reading its modification time.
/// NTFS can publish the write timestamp only after the last writer closes.
pub fn sample_write_clock(path: &Path) -> io::Result<SystemTime> {
    use std::io::Write;

    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut probe = tempfile::Builder::new()
        .prefix(".kache-clock-")
        .tempfile_in(dir)?;
    probe.write_all(b"t")?;
    // Closing the writer retains a private pathname until metadata is sampled.
    let probe = probe.into_temp_path();
    fs::metadata(&probe)?.modified()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sampling_preserves_target_bytes_cleans_probes_and_uses_the_volume_clock() {
        let root = crate::testutil::TempDir::new("write-clock");
        let target = root.path().join("output");
        for _ in 0..32 {
            fs::write(&target, b"original").unwrap();
            let sampled = sample_write_clock(&target).unwrap();
            assert_eq!(fs::read(&target).unwrap(), b"original");
            assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
            fs::write(&target, b"later write").unwrap();
            assert!(fs::metadata(&target).unwrap().modified().unwrap() >= sampled);
        }
        assert!(sample_write_clock(&root.path().join("absent/output")).is_err());
        assert!(!root.path().join("absent").exists());
    }
}
