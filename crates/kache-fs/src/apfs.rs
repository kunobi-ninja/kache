//! APFS/macOS probe via `getattrlist(2)` with `FSOPT_ATTR_CMN_EXTENDED`.
//!
//! `ATTR_CMNEXT_PRIVATESIZE` is the number that matters: bytes not trapped in a
//! clone or snapshot. Other hardlinks still keep those bytes allocated. A clone
//! made by `clonefile(2)` (what a content-addressed cache uses to restore
//! artifacts zero-copy) has an independent inode with `nlink == 1` while sharing
//! every block — so link-count-based "is this shared?" tests report *nothing*
//! shared on exactly the filesystems where sharing is most common.
//!
//! Two traps encoded here:
//!
//! 1. Attributes are returned in **ascending bit order**, not the order you
//!    request them. `ATTR_FILE_ALLOCSIZE` (0x4) precedes `ATTR_FILE_DATALENGTH`
//!    (0x200); the extended set is PRIVATESIZE, CLONEID, EXT_FLAGS, CLONE_REFCNT.
//!    Getting this wrong yields plausible numbers, not an error.
//! 2. Support is per-volume and per-OS, so `ATTR_CMN_RETURNED_ATTRS` is
//!    requested first and checked before any field is trusted.
//!
//! Directory walks use `getattrlistbulk(2)` to read attributes in batches.
//! A per-file query uses `getattrlist` and `lstat`. Unsupported bulk reads
//! fall back to that per-file path.

use std::ffi::{CString, OsStr};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

use crate::apfs_values::sharing as sharing_of;
use crate::{DirEntry, FileSizing, InodeId, ProbeCaps, SizeProbe};

const ATTR_BIT_MAP_COUNT: u16 = 5;
const FSOPT_NOFOLLOW: u32 = 0x0000_0001;
const FSOPT_ATTR_CMN_EXTENDED: u32 = 0x0000_0020;

const ATTR_CMN_NAME: u32 = 0x0000_0001;
const ATTR_CMN_DEVID: u32 = 0x0000_0002;
const ATTR_CMN_OBJTYPE: u32 = 0x0000_0008;
const ATTR_CMN_FILEID: u32 = 0x0200_0000;
const ATTR_CMN_RETURNED_ATTRS: u32 = 0x8000_0000;
const ATTR_FILE_LINKCOUNT: u32 = 0x0000_0001;
const ATTR_FILE_ALLOCSIZE: u32 = 0x0000_0004;
const ATTR_FILE_DATALENGTH: u32 = 0x0000_0200;

const ATTR_CMNEXT_PRIVATESIZE: u32 = 0x0000_0008;
const ATTR_CMNEXT_CLONEID: u32 = 0x0000_0100;
const ATTR_CMNEXT_EXT_FLAGS: u32 = 0x0000_0200;
const ATTR_CMNEXT_CLONE_REFCNT: u32 = 0x0000_1000;

#[repr(C)]
#[derive(Default)]
struct AttrList {
    bitmapcount: u16,
    reserved: u16,
    commonattr: u32,
    volattr: u32,
    dirattr: u32,
    fileattr: u32,
    forkattr: u32,
}

#[repr(C)]
#[derive(Default, Clone, Copy)]
struct AttributeSet {
    commonattr: u32,
    volattr: u32,
    dirattr: u32,
    fileattr: u32,
    forkattr: u32,
}

/// Decoded attributes. Missing fields occupy no space in the kernel reply.
struct Reply {
    returned: AttributeSet,
    allocsize: Option<i64>,
    datalength: Option<i64>,
    privatesize: Option<i64>,
    cloneid: Option<u64>,
    ext_flags: Option<u64>,
}

fn parse_reply(buf: &[u8]) -> Option<Reply> {
    let len = u32::from_ne_bytes(buf.get(..4)?.try_into().ok()?) as usize;
    let mut reader = EntryReader {
        entry: buf.get(..len)?,
        pos: 4,
    };
    let returned = AttributeSet {
        commonattr: reader.u32()?,
        volattr: reader.u32()?,
        dirattr: reader.u32()?,
        fileattr: reader.u32()?,
        forkattr: reader.u32()?,
    };
    let file = returned.fileattr;
    let fork = returned.forkattr;
    let allocsize = if file & ATTR_FILE_ALLOCSIZE != 0 {
        Some(reader.i64()?)
    } else {
        None
    };
    let datalength = if file & ATTR_FILE_DATALENGTH != 0 {
        Some(reader.i64()?)
    } else {
        None
    };
    let privatesize = if fork & ATTR_CMNEXT_PRIVATESIZE != 0 {
        Some(reader.i64()?)
    } else {
        None
    };
    let cloneid = if fork & ATTR_CMNEXT_CLONEID != 0 {
        Some(reader.u64()?)
    } else {
        None
    };
    let ext_flags = if fork & ATTR_CMNEXT_EXT_FLAGS != 0 {
        Some(reader.u64()?)
    } else {
        None
    };
    if fork & ATTR_CMNEXT_CLONE_REFCNT != 0 {
        reader.u32()?;
    }
    Some(Reply {
        returned,
        allocsize,
        datalength,
        privatesize,
        cloneid,
        ext_flags,
    })
}

/// `fsobj_type_t` values from `sys/vnode.h` that the walk treats specially.
const VDIR: u32 = 2;
const VLNK: u32 = 5;

/// Reply buffer for one `getattrlistbulk` call. Entries are variable length,
/// so a few hundred KiB covers most directories in one round trip.
const BULK_BUFFER_BYTES: usize = 256 * 1024;

unsafe extern "C" {
    fn getattrlist(
        path: *const libc::c_char,
        attr_list: *mut libc::c_void,
        attr_buf: *mut libc::c_void,
        attr_buf_size: libc::size_t,
        options: libc::c_ulong,
    ) -> libc::c_int;

    fn getattrlistbulk(
        dirfd: libc::c_int,
        attr_list: *mut libc::c_void,
        attr_buf: *mut libc::c_void,
        attr_buf_size: libc::size_t,
        options: u64,
    ) -> libc::c_int;
}

/// A directory file descriptor closed on drop.
struct DirFd(libc::c_int);

impl Drop for DirFd {
    fn drop(&mut self) {
        // SAFETY: the descriptor was returned by `open` and is closed once.
        unsafe { libc::close(self.0) };
    }
}

/// Sequential reader over one entry of a `getattrlistbulk` reply.
///
/// Attributes appear in ascending bit order within each group, and only the
/// ones the filesystem actually returned are present, so every field is read
/// behind its `returned` bit rather than at a fixed offset.
struct EntryReader<'a> {
    entry: &'a [u8],
    pos: usize,
}

impl<'a> EntryReader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let bytes = self.entry.get(self.pos..self.pos + len)?;
        self.pos += len;
        Some(bytes)
    }

    fn u32(&mut self) -> Option<u32> {
        let bytes = self.take(4)?;
        Some(u32::from_ne_bytes(bytes.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        let bytes = self.take(8)?;
        Some(u64::from_ne_bytes(bytes.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        self.u64().map(|v| v as i64)
    }

    /// An `attrreference_t`: an offset relative to its own position, and a
    /// length that includes the terminating NUL.
    fn reference(&mut self) -> Option<&'a [u8]> {
        let at = self.pos;
        let offset = self.u32()? as i32;
        let len = self.u32()? as usize;
        let start = at.checked_add_signed(offset as isize)?;
        let bytes = self.entry.get(start..start.checked_add(len)?)?;
        Some(bytes.strip_suffix(&[0]).unwrap_or(bytes))
    }
}

/// What one bulk entry says about a file, before it is turned into a sizing.
struct BulkEntry<'a> {
    name: &'a [u8],
    objtype: u32,
    devid: Option<u32>,
    fileid: Option<u64>,
    nlink: Option<u32>,
    allocsize: Option<i64>,
    datalength: Option<i64>,
    privatesize: Option<i64>,
    cloneid: Option<u64>,
    ext_flags: Option<u64>,
}

/// Parse the entry at the start of `buf`. Returns the entry and its total
/// length so the caller can advance to the next one.
fn parse_entry(buf: &[u8]) -> Option<(BulkEntry<'_>, usize)> {
    let len = u32::from_ne_bytes(buf.get(0..4)?.try_into().ok()?) as usize;
    let entry = buf.get(..len)?;
    let mut reader = EntryReader { entry, pos: 4 };
    let returned = AttributeSet {
        commonattr: reader.u32()?,
        volattr: reader.u32()?,
        dirattr: reader.u32()?,
        fileattr: reader.u32()?,
        forkattr: reader.u32()?,
    };
    let common = returned.commonattr;
    let name = if common & ATTR_CMN_NAME != 0 {
        reader.reference()?
    } else {
        return None;
    };
    let devid = (common & ATTR_CMN_DEVID != 0)
        .then(|| reader.u32())
        .flatten();
    let objtype = if common & ATTR_CMN_OBJTYPE != 0 {
        reader.u32()?
    } else {
        return None;
    };
    let fileid = (common & ATTR_CMN_FILEID != 0)
        .then(|| reader.u64())
        .flatten();
    let file = returned.fileattr;
    let nlink = (file & ATTR_FILE_LINKCOUNT != 0)
        .then(|| reader.u32())
        .flatten();
    let allocsize = (file & ATTR_FILE_ALLOCSIZE != 0)
        .then(|| reader.i64())
        .flatten();
    let datalength = (file & ATTR_FILE_DATALENGTH != 0)
        .then(|| reader.i64())
        .flatten();
    let fork = returned.forkattr;
    let privatesize = (fork & ATTR_CMNEXT_PRIVATESIZE != 0)
        .then(|| reader.i64())
        .flatten();
    let cloneid = (fork & ATTR_CMNEXT_CLONEID != 0)
        .then(|| reader.u64())
        .flatten();
    let ext_flags = (fork & ATTR_CMNEXT_EXT_FLAGS != 0)
        .then(|| reader.u64())
        .flatten();
    Some((
        BulkEntry {
            name,
            objtype,
            devid,
            fileid,
            nlink,
            allocsize,
            datalength,
            privatesize,
            cloneid,
            ext_flags,
        },
        len,
    ))
}

pub struct ApfsProbe {
    /// Whether the first successful probe returned PRIVATESIZE.
    unique_supported: bool,
}

impl ApfsProbe {
    /// Returns `None` when this path's filesystem does not report the extended
    /// attributes, so the caller falls back rather than reporting zeros.
    pub fn new(path: &Path) -> Option<Self> {
        let probe = ApfsProbe {
            unique_supported: true,
        };
        // Probe the path itself, else its parent (the path may be a directory
        // or may not exist yet).
        let target = if path.exists() { path } else { path.parent()? };
        match probe.raw(target) {
            Ok(r) if r.returned.forkattr & ATTR_CMNEXT_PRIVATESIZE != 0 => Some(probe),
            _ => None,
        }
    }

    fn raw(&self, path: &Path) -> io::Result<Reply> {
        let c = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;

        let mut list = AttrList {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            commonattr: ATTR_CMN_RETURNED_ATTRS,
            fileattr: ATTR_FILE_ALLOCSIZE | ATTR_FILE_DATALENGTH,
            forkattr: ATTR_CMNEXT_PRIVATESIZE
                | ATTR_CMNEXT_CLONEID
                | ATTR_CMNEXT_EXT_FLAGS
                | ATTR_CMNEXT_CLONE_REFCNT,
            ..Default::default()
        };
        let mut reply = [0u8; 68];

        // SAFETY: both pointers are valid for the sizes passed; the kernel
        // writes at most `attr_buf_size` bytes into `reply`.
        let rc = unsafe {
            getattrlist(
                c.as_ptr(),
                (&raw mut list).cast(),
                reply.as_mut_ptr().cast(),
                reply.len(),
                (FSOPT_ATTR_CMN_EXTENDED | FSOPT_NOFOLLOW) as libc::c_ulong,
            )
        };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        parse_reply(&reply)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid attribute reply"))
    }

    /// Read a directory with `getattrlistbulk`.
    ///
    /// `Ok(None)` means the call is not usable here (an unsupported volume,
    /// or a reply the parser does not understand) and the caller should take
    /// the per-file path instead. `Err` means the directory itself could not
    /// be opened, which the walk records as unreadable.
    fn read_dir_bulk(&self, dir: &Path) -> io::Result<Option<Vec<DirEntry>>> {
        let c = CString::new(dir.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        // SAFETY: `c` is a valid NUL-terminated path.
        let fd = unsafe {
            libc::open(
                c.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = DirFd(fd);

        let mut list = AttrList {
            bitmapcount: ATTR_BIT_MAP_COUNT,
            commonattr: ATTR_CMN_RETURNED_ATTRS
                | ATTR_CMN_NAME
                | ATTR_CMN_DEVID
                | ATTR_CMN_OBJTYPE
                | ATTR_CMN_FILEID,
            fileattr: ATTR_FILE_LINKCOUNT | ATTR_FILE_ALLOCSIZE | ATTR_FILE_DATALENGTH,
            forkattr: ATTR_CMNEXT_PRIVATESIZE | ATTR_CMNEXT_CLONEID | ATTR_CMNEXT_EXT_FLAGS,
            ..Default::default()
        };
        let mut buf = vec![0u8; BULK_BUFFER_BYTES];
        let mut entries = Vec::new();
        loop {
            // SAFETY: `list` and `buf` are valid for the sizes passed; the
            // kernel writes at most `buf.len()` bytes.
            let count = unsafe {
                getattrlistbulk(
                    fd.0,
                    (&raw mut list).cast(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    u64::from(FSOPT_ATTR_CMN_EXTENDED),
                )
            };
            if count < 0 {
                return Ok(None);
            }
            if count == 0 {
                return Ok(Some(entries));
            }
            let mut offset = 0;
            for _ in 0..count {
                let Some((entry, len)) = parse_entry(&buf[offset..]) else {
                    return Ok(None);
                };
                offset += len;
                if let Some(parsed) = self.bulk_entry(dir, &entry) {
                    entries.push(parsed);
                }
            }
        }
    }

    /// Classify one bulk entry. Anything the bulk reply cannot fully describe
    /// is probed on its own, so a gap in one attribute never becomes a wrong
    /// number.
    fn bulk_entry(&self, dir: &Path, entry: &BulkEntry<'_>) -> Option<DirEntry> {
        if entry.name.is_empty() || entry.objtype == VLNK {
            return None;
        }
        let path: PathBuf = dir.join(OsStr::from_bytes(entry.name));
        if entry.objtype == VDIR {
            return Some(DirEntry::Dir(path));
        }
        let (Some(devid), Some(fileid), Some(nlink), Some(allocsize), Some(datalength)) = (
            entry.devid,
            entry.fileid,
            entry.nlink,
            entry.allocsize,
            entry.datalength,
        ) else {
            return self.measure_file(&path).ok().map(DirEntry::File);
        };
        let allocated = allocsize.max(0) as u64;
        let unique = entry.privatesize.and_then(|v| u64::try_from(v).ok());
        let sharing = sharing_of(
            entry.ext_flags.is_some(),
            entry.ext_flags.unwrap_or(0),
            unique,
            allocated,
        );
        Some(DirEntry::File(FileSizing {
            logical: datalength.max(0) as u64,
            allocated,
            unique: crate::apfs_values::private_bytes(unique, sharing, allocated),
            sharing,
            inode: InodeId {
                dev: u64::from(devid),
                ino: fileid,
            },
            nlink: u64::from(nlink),
            clone_id: entry.cloneid,
        }))
    }
}

impl SizeProbe for ApfsProbe {
    fn measure_file(&self, path: &Path) -> io::Result<FileSizing> {
        let r = match self.raw(path) {
            Ok(r) => r,
            Err(_) => return crate::fallback::FallbackProbe.measure_file(path),
        };
        let md = std::fs::symlink_metadata(path)?;

        let allocated = r
            .allocsize
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or_else(|| md.blocks().saturating_mul(512));
        let logical = r
            .datalength
            .and_then(|v| u64::try_from(v).ok())
            .unwrap_or(md.len());
        let unique = r.privatesize.and_then(|v| u64::try_from(v).ok());
        let sharing = sharing_of(
            r.ext_flags.is_some(),
            r.ext_flags.unwrap_or(0),
            unique,
            allocated,
        );
        let unique = crate::apfs_values::private_bytes(unique, sharing, allocated);

        Ok(FileSizing {
            logical,
            allocated,
            unique,
            sharing,
            inode: InodeId {
                dev: md.dev(),
                ino: md.ino(),
            },
            nlink: md.nlink(),
            clone_id: r.cloneid,
        })
    }

    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        match self.read_dir_bulk(dir)? {
            Some(entries) => Ok(entries),
            None => crate::read_dir_one_by_one(self, dir),
        }
    }

    fn capabilities(&self) -> ProbeCaps {
        ProbeCaps {
            unique_bytes: self.unique_supported,
            clone_refcount: true,
            name: "apfs",
        }
    }
}

pub(crate) fn measure_file(path: &Path) -> io::Result<FileSizing> {
    ApfsProbe {
        unique_supported: true,
    }
    .measure_file(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Sharing;
    use std::process::Command;

    fn single_reply(file: u32, fork: u32, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for word in [
            (24 + payload.len()) as u32,
            ATTR_CMN_RETURNED_ATTRS,
            0,
            0,
            file,
            fork,
        ] {
            bytes.extend_from_slice(&word.to_ne_bytes());
        }
        bytes.extend_from_slice(payload);
        bytes
    }

    #[test]
    fn single_reply_decodes_only_present_fields_and_rejects_truncation() {
        let mut payload = Vec::new();
        for value in [8192i64, 5000, 4096, 77, 1] {
            payload.extend_from_slice(&value.to_ne_bytes());
        }
        payload.extend_from_slice(&2u32.to_ne_bytes());
        let bytes = single_reply(
            ATTR_FILE_ALLOCSIZE | ATTR_FILE_DATALENGTH,
            ATTR_CMNEXT_PRIVATESIZE
                | ATTR_CMNEXT_CLONEID
                | ATTR_CMNEXT_EXT_FLAGS
                | ATTR_CMNEXT_CLONE_REFCNT,
            &payload,
        );
        let reply = parse_reply(&bytes).unwrap();
        assert_eq!(reply.allocsize, Some(8192));
        assert_eq!(reply.datalength, Some(5000));
        assert_eq!(reply.privatesize, Some(4096));
        assert_eq!(reply.cloneid, Some(77));
        assert_eq!(reply.ext_flags, Some(1));
        for end in 0..bytes.len() {
            assert!(parse_reply(&bytes[..end]).is_none());
        }

        // Directories omit file attributes, so extended attributes move forward.
        let bytes = single_reply(
            0,
            ATTR_CMNEXT_PRIVATESIZE | ATTR_CMNEXT_EXT_FLAGS,
            &[123i64.to_ne_bytes(), 64i64.to_ne_bytes()].concat(),
        );
        let reply = parse_reply(&bytes).unwrap();
        assert_eq!(reply.allocsize, None);
        assert_eq!(reply.datalength, None);
        assert_eq!(reply.privatesize, Some(123));
        assert_eq!(reply.cloneid, None);
        assert_eq!(reply.ext_flags, Some(64));
        let bytes = single_reply(0, ATTR_CMNEXT_PRIVATESIZE, &[]);
        assert!(
            parse_reply(&bytes).is_none(),
            "declared attribute without data"
        );
    }

    /// Delegates every file to the real probe but never uses the bulk path,
    /// so a walk through it is the per-file answer the bulk one must match.
    struct OneByOne<'a>(&'a ApfsProbe);

    impl SizeProbe for OneByOne<'_> {
        fn measure_file(&self, path: &Path) -> io::Result<FileSizing> {
            self.0.measure_file(path)
        }

        fn capabilities(&self) -> ProbeCaps {
            self.0.capabilities()
        }
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("kache-fs-apfs-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn key(entry: &DirEntry) -> (u8, u64, u64, PathBuf) {
        match entry {
            DirEntry::Dir(path) => (0, 0, 0, path.clone()),
            DirEntry::File(s) => (1, s.inode.dev, s.inode.ino, PathBuf::new()),
        }
    }

    /// The bulk reader is an optimisation and nothing else: for every entry
    /// it must report the same inode, link count, bytes and sharing as
    /// probing that file on its own.
    #[test]
    fn bulk_directory_read_matches_per_file_probing() {
        let dir = scratch("bulk");
        let Some(probe) = ApfsProbe::new(&dir) else {
            return; // volume lacks the extended attributes
        };
        std::fs::write(dir.join("a"), vec![1u8; 3 * 1024 * 1024]).unwrap();
        std::fs::write(dir.join("b"), b"tiny").unwrap();
        std::fs::write(dir.join("empty"), b"").unwrap();
        std::fs::create_dir(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/c"), vec![2u8; 64 * 1024]).unwrap();
        std::fs::hard_link(dir.join("b"), dir.join("b-link")).unwrap();
        std::os::unix::fs::symlink(dir.join("a"), dir.join("a-symlink")).unwrap();
        let _ = Command::new("cp")
            .arg("-c")
            .arg(dir.join("a"))
            .arg(dir.join("a-clone"))
            .status();

        let mut bulk = probe
            .read_dir_bulk(&dir)
            .unwrap()
            .expect("bulk read supported");
        let mut slow = crate::read_dir_one_by_one(&probe, &dir).unwrap();
        bulk.sort_by_key(key);
        slow.sort_by_key(key);
        assert_eq!(bulk.len(), slow.len(), "same entries, symlink excluded");
        assert!(
            bulk.iter()
                .any(|entry| matches!(entry, DirEntry::Dir(path) if path == &dir.join("sub"))),
            "subdirectories are reported for descent"
        );
        for (left, right) in bulk.iter().zip(&slow) {
            match (left, right) {
                (DirEntry::Dir(l), DirEntry::Dir(r)) => assert_eq!(l, r),
                (DirEntry::File(l), DirEntry::File(r)) => {
                    assert_eq!(l.inode, r.inode);
                    assert_eq!(l.nlink, r.nlink);
                    assert_eq!(l.logical, r.logical);
                    assert_eq!(l.allocated, r.allocated);
                    assert_eq!(l.unique, r.unique);
                    assert_eq!(l.sharing, r.sharing);
                    assert_eq!(l.clone_id, r.clone_id);
                }
                other => panic!("entry kinds differ: {other:?}"),
            }
        }

        let mut ledger = crate::InodeLedger::new();
        let whole = probe.measure_dir(&dir, &mut ledger).unwrap();
        let mut ledger = crate::InodeLedger::new();
        let reference = OneByOne(&probe).measure_dir(&dir, &mut ledger).unwrap();
        assert_eq!(whole, reference, "a recursive walk agrees in every figure");
        assert_eq!(
            whole.files, 5,
            "a, b, empty, sub/c, a-clone; the hard link is one inode"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A directory that cannot be opened is an error, not an empty listing.
    #[test]
    fn bulk_read_reports_a_missing_directory() {
        let dir = scratch("bulk-missing");
        let Some(probe) = ApfsProbe::new(&dir) else {
            return;
        };
        assert!(probe.read_dir_bulk(&dir.join("absent")).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The behaviour the whole crate exists for: a clone has nlink == 1 and
    /// shares all blocks, so deleting it frees nothing — and `du` disagrees.
    #[test]
    fn clone_shares_all_blocks_and_frees_nothing() {
        let dir = std::env::temp_dir().join(format!("kache-fs-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let orig = dir.join("orig");
        std::fs::write(&orig, vec![7u8; 4 * 1024 * 1024]).unwrap();

        let clone = dir.join("clone");
        let ok = Command::new("cp").arg("-c").arg(&orig).arg(&clone).status();
        let Ok(st) = ok else {
            return; // no cp -c: nothing to assert
        };
        if !st.success() {
            return;
        }

        let Some(probe) = ApfsProbe::new(&dir) else {
            return; // volume lacks the extended attributes
        };
        let c = probe.measure_file(&clone).unwrap();

        assert_eq!(c.nlink, 1, "a clone is an independent inode");
        assert_eq!(c.sharing, Sharing::Full);
        assert_eq!(
            c.unique,
            Some(0),
            "deleting one side of a clone frees nothing"
        );
        assert!(
            c.allocated >= 4 * 1024 * 1024,
            "yet du still bills it in full"
        );

        std::fs::remove_dir_all(&dir).ok();
    }
}
