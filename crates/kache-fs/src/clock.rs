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

/// The modification time that dates the file at `path` as written now,
/// for `futimens`: `UTIME_NOW`, which on Linux resolves through the same
/// timestamp source as writes, except on ZFS.
///
/// ZFS stamps writes with its own coarse clock, while Linux 6.13 and later
/// can resolve `UTIME_NOW` to a finer time that another busy filesystem
/// pushed ahead. A restored file could then read as newer than a build
/// script's `output` Cargo writes next, and Cargo would rebuild dependents.
/// There the time comes from a sibling write instead ([`sample_write_clock`]),
/// which can only sit before later writes.
#[cfg(unix)]
pub fn mtime_written_now(path: &Path) -> io::Result<libc::timespec> {
    let own_clock = cfg!(target_os = "linux") && dates_writes_itself(crate::volume::fs_magic(path));
    mtime_written_now_on(path, own_clock)
}

#[cfg(unix)]
fn mtime_written_now_on(path: &Path, own_clock: bool) -> io::Result<libc::timespec> {
    if !own_clock {
        return Ok(libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_NOW,
        });
    }
    let sampled = sample_write_clock(path)?
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(io::Error::other)?;
    Ok(libc::timespec {
        tv_sec: sampled.as_secs() as _,
        tv_nsec: sampled.subsec_nanos() as libc::c_long,
    })
}

/// Whether a Linux filesystem with this superblock `magic` dates writes with
/// a clock other than the one `UTIME_NOW` reads: ZFS, or one kache could not
/// identify, where sampling is right too, only slower.
#[cfg_attr(not(unix), allow(dead_code))]
fn dates_writes_itself(magic: Option<i64>) -> bool {
    magic.is_none_or(|magic| magic == crate::volume::ZFS_MAGIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_zfs_dates_writes_with_its_own_clock() {
        assert!(dates_writes_itself(Some(0x2FC1_2FC1)));
        assert!(dates_writes_itself(None));
        for other in [0xEF53, 0x9123_683E, 0x5846_5342, 0x0102_1994] {
            assert!(!dates_writes_itself(Some(other)));
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_file_is_dated_no_later_than_writes_that_follow() {
        let root = crate::testutil::TempDir::new("written-now");
        let target = root.path().join("output");
        fs::write(&target, b"restored").unwrap();
        let mtime = mtime_written_now(&target).unwrap();
        if crate::volume::fs_magic(&target) != Some(crate::volume::ZFS_MAGIC) {
            assert_eq!(mtime.tv_nsec, libc::UTIME_NOW);
        }
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);

        // Sampled from a sibling write: never after a write that follows.
        let sampled = mtime_written_now_on(&target, true).unwrap();
        assert_ne!(sampled.tv_nsec, libc::UTIME_NOW);
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
        let later = root.path().join("later");
        fs::write(&later, b"written after").unwrap();
        let written = fs::metadata(&later)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        assert!(
            (sampled.tv_sec as u64, sampled.tv_nsec as u32)
                <= (written.as_secs(), written.subsec_nanos())
        );
    }
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
