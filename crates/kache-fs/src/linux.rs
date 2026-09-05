//! Linux probe via `FS_IOC_FIEMAP`.
//!
//! `st_blocks` charges every reflink owner for the same blocks. FIEMAP exposes
//! sharing without privileged reverse mapping: an extent
//! marked `FIEMAP_EXTENT_SHARED` is pinned by another owner, while an unmarked
//! extent dies with this inode.
//!
//! Some extent states make their length unsuitable for reclaim accounting.
//! Those states deliberately discard the whole result: a plausible partial
//! answer is more dangerous here than admitting that the filesystem cannot
//! tell us.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;

use crate::extents::{self, Accounting, EXTENTS_PER_PAGE, Extent};
use crate::fallback::FallbackProbe;
use crate::{FileSizing, InodeId, ProbeCaps, SizeProbe};

const FS_IOC_FIEMAP: libc::c_ulong = 0xc020_660b;
const FIEMAP_FLAG_SYNC: u32 = 0x0000_0001;

#[repr(C)]
#[derive(Default)]
struct Fiemap {
    _start: u64,
    _length: u64,
    _flags: u32,
    mapped_extents: u32,
    _extent_count: u32,
    _reserved: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FiemapExtent {
    logical: u64,
    _physical: u64,
    length: u64,
    _reserved64: [u64; 2],
    flags: u32,
    _reserved: [u32; 3],
}

#[repr(C)]
struct FiemapPage {
    map: Fiemap,
    extents: [FiemapExtent; EXTENTS_PER_PAGE],
}

impl FiemapPage {
    fn new(start: u64) -> Self {
        Self {
            map: Fiemap {
                _start: start,
                _length: u64::MAX - start,
                _flags: FIEMAP_FLAG_SYNC,
                _extent_count: EXTENTS_PER_PAGE as u32,
                ..Default::default()
            },
            extents: [FiemapExtent::default(); EXTENTS_PER_PAGE],
        }
    }
}

fn fiemap(file: &File) -> io::Result<Option<Accounting>> {
    extents::walk(|start| {
        let mut page = FiemapPage::new(start);
        // SAFETY: the header is immediately followed by the advertised capacity.
        let rc = unsafe {
            libc::ioctl(
                file.as_raw_fd(),
                FS_IOC_FIEMAP as libc::Ioctl,
                &raw mut page,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        let mapped = page.map.mapped_extents as usize;
        let Some(entries) = page.extents.get(..mapped) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "FIEMAP extent count exceeds capacity",
            ));
        };
        Ok(entries
            .iter()
            .map(|e| Extent {
                logical: e.logical,
                length: e.length,
                flags: e.flags,
            })
            .collect())
    })
}

pub(crate) fn measure_file(path: &Path) -> io::Result<FileSizing> {
    LinuxProbe { supported: true }.measure_file(path)
}

pub struct LinuxProbe {
    /// Whether FIEMAP actually answered for a file on this filesystem.
    ///
    /// Without this the probe advertises `unique_bytes: true` everywhere, so on
    /// a filesystem with no FIEMAP support every file falls back to "unknown"
    /// while the aggregate still claims lower-bound precision — overstating how
    /// much the probe knows. Mirrors `ApfsProbe::new`.
    supported: bool,
}

impl LinuxProbe {
    /// `None` when FIEMAP is unavailable here, so the caller uses the portable
    /// probe and the confidence downgrades honestly.
    pub fn new(reference: &Path) -> Option<Self> {
        let probe = LinuxProbe { supported: true };
        let target = if reference.is_file() {
            reference.to_path_buf()
        } else {
            // Any regular file under the reference directory will do.
            std::fs::read_dir(reference)
                .ok()?
                .flatten()
                .map(|e| e.path())
                .find(|p| p.is_file())?
        };
        match probe.measure_file(&target) {
            Ok(s) if s.unique.is_some() => Some(probe),
            _ => None,
        }
    }
}

impl SizeProbe for LinuxProbe {
    fn measure_file(&self, path: &Path) -> io::Result<FileSizing> {
        let path_md = std::fs::symlink_metadata(path)?;
        if !path_md.is_file() {
            return FallbackProbe.measure_file(path);
        }

        let Ok(file) = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
        else {
            return FallbackProbe.measure_file(path);
        };
        let md = file.metadata()?;
        let Ok(Some(accounting)) = fiemap(&file) else {
            return FallbackProbe.measure_file(path);
        };

        Ok(FileSizing {
            logical: md.len(),
            allocated: md.blocks() * 512,
            unique: Some(accounting.unique),
            sharing: accounting.sharing(),
            inode: InodeId {
                dev: md.dev(),
                ino: md.ino(),
            },
            nlink: md.nlink(),
            // FIEMAP says an extent is shared, not which group it belongs to;
            // pairing directories needs owner resolution this probe does not do.
            clone_id: None,
        })
    }

    fn capabilities(&self) -> ProbeCaps {
        ProbeCaps {
            unique_bytes: self.supported,
            clone_refcount: false,
            name: "linux-fiemap",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sharing;
    use crate::testutil::TempDir;

    // Only unavailable ioctls can skip these tests. A malformed reply on a
    // supported filesystem must fail instead of silently becoming a fallback.
    fn measured(path: &Path) -> Option<Accounting> {
        match fiemap(&File::open(path).unwrap()) {
            Ok(Some(s)) => Some(s),
            Err(e)
                if matches!(
                    e.raw_os_error(),
                    Some(libc::ENOTTY) | Some(libc::EOPNOTSUPP)
                ) =>
            {
                None
            }
            other => panic!("unusable FIEMAP reply: {other:?}"),
        }
    }

    #[test]
    fn sparse_file_counts_allocated_extents() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = TempDir::new("sparse");
        let path = dir.path().join("sparse");
        let mut file = File::create(&path).unwrap();
        file.seek(SeekFrom::Start(64 * 1024 * 1024 - 4096)).unwrap();
        file.write_all(&[0x7e; 4096]).unwrap();
        file.sync_all().unwrap();
        let md = file.metadata().unwrap();
        if md.blocks() * 512 >= md.len() {
            return;
        }
        if let Some(s) = measured(&path) {
            assert!(s.unique > 0 && s.unique < md.len());
            assert_eq!(s.sharing(), Sharing::None);
            let measurement = LinuxProbe { supported: true }.measure_file(&path).unwrap();
            assert_eq!(measurement.unique, Some(s.unique));
            assert_eq!(measurement.sharing, Sharing::None);
            assert_eq!(measurement.logical, 64 * 1024 * 1024);
        }
    }

    #[test]
    fn reflinked_file_has_shared_extents_with_one_link() {
        let dir = TempDir::new("reflink-sharing");
        let source = dir.write("source", 256 * 1024);
        let clone = dir.path().join("clone");
        if crate::try_reflink(&source, &clone).is_err() {
            return;
        }
        assert_eq!(std::fs::metadata(&clone).unwrap().nlink(), 1);
        if let Some(s) = measured(&clone) {
            assert_eq!(s.unique, 0);
            assert_eq!(s.sharing(), Sharing::Full);
        }
    }
}
