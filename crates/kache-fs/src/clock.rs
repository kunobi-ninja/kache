//! Sample a volume's file-write clock without substituting the wall clock.
use std::{fs, io, path::Path, time::SystemTime};

/// Write and close a sibling probe before reading its modification time.
/// NTFS can publish the write timestamp only after the last writer closes.
pub fn sample_write_clock(path: &Path) -> io::Result<SystemTime> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    // `create_new` refuses to open an existing file (and does not follow a
    // symlink/reparse point squatting on the name), so a PID-reuse leftover
    // or a planted file can never be truncated or sampled — the attempt
    // just moves to the next name. Leftovers from a crash between create
    // and remove are benign litter under a distinctive dotted name.
    let mut last_err: Option<std::io::Error> = None;
    for _ in 0..8 {
        let probe = dir.join(format!(
            ".kache-clock-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let created = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe);
        let mut file = match created {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => {
                last_err = Some(e);
                break;
            }
        };
        let sampled = (|| {
            file.write_all(b"t")?;
            drop(file);
            let meta = fs::metadata(&probe)?;
            meta.modified()
        })();
        let _ = fs::remove_file(&probe);
        return sampled;
    }
    Err(last_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "no free write-clock probe name after 8 attempts",
        )
    }))
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
