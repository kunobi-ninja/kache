//! Does a file share storage with something else on disk, and how much of it
//! would actually come back if the file were deleted (kunobi-ninja/kache#602)?
//!
//! `clean` used to answer "is this artifact backed by the cache?" with
//! `nlink > 1`. That only detects the **hardlink fallback**. On APFS, btrfs and
//! XFS-with-reflink — the path `link_to_target` *prefers* — a restore is a
//! `clonefile`/`FICLONE` reflink, which by design produces an independent inode
//! with `nlink == 1`. So on the filesystems where kache works best, every
//! restored artifact looked "local" and `clean` reported ~0% cached.
//!
//! Measured on macOS/APFS against kache's own trees (#602):
//!
//! | Tree | apparent (`du`) | actually frees | shared |
//! |---|---|---|---|
//! | `target/` | 19.23 GiB | 14.28 GiB | 25.7% |
//! | the store | 50.58 GiB | 19.07 GiB | 62.3% |
//!
//! Two separate questions fall out of that, and this module answers both:
//!
//! - **Is it shared?** — replaces the `nlink > 1` classifier.
//! - **How much is private?** — how much a delete really returns. A reflinked
//!   tree's apparent size overstates what you get back, because the shared
//!   extents stay alive until *both* sides go.
//!
//! ## Fallback is the un-shared answer, deliberately
//!
//! Every probe failure degrades to [`Sharing::unknown_for`], which reports "not
//! shared, all bytes private" — the same answer the old `nlink`-only code gave.
//! That keeps a filesystem we cannot interrogate exactly as accurate as before
//! rather than inventing sharing that may not exist, and it keeps `clean`'s
//! reclaim estimate conservative: claiming bytes are shared when they are not
//! would *understate* what a delete frees, which is the direction that makes a
//! user keep files they could have removed.

use std::path::Path;

/// What the filesystem reports about one file's storage sharing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sharing {
    /// Whether this file shares any storage with another file — a reflink/clone
    /// or a hardlink.
    pub shared: bool,
    /// Bytes that would actually be freed by deleting this file. Equal to the
    /// file's size when nothing is shared; 0 when every block is shared with
    /// something else that stays behind.
    pub private_bytes: u64,
}

impl Sharing {
    /// The answer for a file we could not interrogate: assume nothing is shared
    /// and every byte is private. See the module docs on why this direction.
    pub fn unknown_for(size: u64) -> Self {
        Self {
            shared: false,
            private_bytes: size,
        }
    }
}

/// Probe how `path` shares storage. `size` is the file's apparent length, used
/// for the private-byte fallback.
///
/// `nlink` is folded in by the callers rather than here, because they already
/// hold the `Metadata` and a hardlink is sharing whatever the extent-level
/// probe says.
///
/// Best-effort and never fatal: this runs inside a directory walk over tens of
/// thousands of files, so any error yields [`Sharing::unknown_for`].
pub fn probe(path: &Path, size: u64) -> Sharing {
    #[cfg(target_os = "macos")]
    {
        probe_macos(path, size)
    }
    #[cfg(target_os = "linux")]
    {
        probe_linux(path, size)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        probe_unsupported(path, size)
    }
}

// ── macOS ───────────────────────────────────────────────────────────────────
//
// `getattrlist` with `FSOPT_ATTR_CMN_EXTENDED` exposes exactly the two numbers
// this module wants: `ATTR_CMNEXT_PRIVATESIZE` (bytes freed if this file dies)
// and `ATTR_CMNEXT_EXT_FLAGS` (whether blocks are shared at all).
//
// Two traps, both hit while measuring #602 and both worth naming:
//
//  1. **Ascending bit order.** The kernel packs the requested attributes in
//     ascending order of their bit values, NOT in the order you list them. Get
//     the struct layout wrong and you read plausible-looking garbage rather than
//     an error — which is exactly what produced the numbers later corrected on
//     the issue. `PRIVATESIZE` (0x008) therefore precedes `EXT_FLAGS` (0x200).
//  2. **`ATTR_CMN_RETURNED_ATTRS` is load-bearing, not belt-and-braces.** Which
//     extended attributes a volume actually returns varies by filesystem and OS
//     version, and the reporter measured a volume advertising
//     `VOL_CAP_FMT_CLONE_MAPPING = no` that nonetheless returned usable
//     `PRIVATESIZE`. Trusting capability flags would have skipped a working
//     interface; asking what came back is the reliable check.
#[cfg(target_os = "macos")]
fn probe_macos(path: &Path, size: u64) -> Sharing {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    // <sys/attr.h>. Values transcribed from the SDK header, not from memory:
    // getting one wrong yields plausible garbage rather than an error.
    const FSOPT_ATTR_CMN_EXTENDED: u32 = 0x0000_0020;
    const ATTR_BIT_MAP_COUNT: u16 = 5;
    const ATTR_CMN_RETURNED_ATTRS: u32 = 0x8000_0000;
    // With FSOPT_ATTR_CMN_EXTENDED these two live in the `forkattr` slot.
    const ATTR_CMNEXT_PRIVATESIZE: u32 = 0x0000_0008;
    const ATTR_CMNEXT_EXT_FLAGS: u32 = 0x0000_0200;
    // <sys/stat.h> ext_flags. NB 0x02 is EF_NO_XATTRS, not a sharing bit —
    // testing it would mark every file without xattrs as shared.
    const EF_MAY_SHARE_BLOCKS: u64 = 0x0000_0001;
    const EF_SHARES_ALL_BLOCKS: u64 = 0x0000_0040;

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

    // Field order follows the kernel's ascending-bit-order packing (trap 1):
    // returned_attrs first, then PRIVATESIZE (0x008), then EXT_FLAGS (0x200).
    // Transposing these two is the exact error that produced the numbers later
    // corrected on #602 — and the first version of this code repeated it, which
    // is why `a_reflinked_copy_is_detected_as_shared_despite_nlink_1` exists.
    //
    // `packed` because the kernel writes the values back to back with no
    // padding: `length` (u32) + `returned` (5 × u32) is 24 bytes, which happens
    // to be 8-aligned here, but relying on that silently breaks if the requested
    // attribute set ever changes.
    #[repr(C, packed)]
    struct Buf {
        length: u32,
        returned: AttributeSet,
        /// `off_t` — bytes freed if this file is deleted.
        private_size: i64,
        ext_flags: u64,
    }

    let Ok(c_path) = CString::new(path.as_os_str().as_bytes()) else {
        return Sharing::unknown_for(size);
    };

    // Every field spelled out rather than `..Default::default()`. This is the
    // struct the kernel reads to decide what to write back, so being explicit
    // about the zeroed slots is worth the four extra lines — and it leaves no
    // field for a mutation to silently drop, which the mutation lane cannot
    // otherwise reach inside a `#[cfg(target_os = "macos")]` function.
    let mut attrs = AttrList {
        bitmapcount: ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: ATTR_CMN_RETURNED_ATTRS,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: ATTR_CMNEXT_PRIVATESIZE | ATTR_CMNEXT_EXT_FLAGS,
    };
    let mut buf: Buf = unsafe { std::mem::zeroed() };

    let rc = unsafe {
        libc::getattrlist(
            c_path.as_ptr(),
            &mut attrs as *mut AttrList as *mut libc::c_void,
            &mut buf as *mut Buf as *mut libc::c_void,
            std::mem::size_of::<Buf>(),
            FSOPT_ATTR_CMN_EXTENDED,
        )
    };
    if rc != 0 {
        return Sharing::unknown_for(size);
    }

    // Trap 2: only believe fields the kernel says it actually returned.
    // Copied out of the packed struct before use — taking a reference to an
    // unaligned field is UB.
    let returned_fork = buf.returned.forkattr;
    let got_flags = returned_fork & ATTR_CMNEXT_EXT_FLAGS != 0;
    let got_private = returned_fork & ATTR_CMNEXT_PRIVATESIZE != 0;
    if !got_flags && !got_private {
        return Sharing::unknown_for(size);
    }
    let ext_flags = buf.ext_flags;
    let private_size = buf.private_size;

    let shared = got_flags && ext_flags & (EF_MAY_SHARE_BLOCKS | EF_SHARES_ALL_BLOCKS) != 0;
    let private_bytes = if got_private && private_size >= 0 {
        // PRIVATESIZE is allocated bytes; a file can report more private space
        // than its logical length (block rounding, preallocation). Clamp so a
        // reclaim estimate never exceeds the size we are reporting for the file.
        (private_size as u64).min(size)
    } else {
        size
    };

    Sharing {
        shared,
        private_bytes,
    }
}

// ── Linux ───────────────────────────────────────────────────────────────────
//
// `FS_IOC_FIEMAP` maps a file's extents; `FIEMAP_EXTENT_SHARED` marks the ones
// shared with another inode, which is what btrfs/XFS reflinks produce. Summing
// the unshared extents gives the private-byte figure directly.
//
// The issue proposed this but flagged it as untested (no Linux box). It is
// written to fail safe: any unexpected shape — ioctl error, zero extents, a
// truncated mapping — falls back to "not shared, all private".
/// Length of the FIEMAP window starting at `offset`: everything from there to
/// the end of the address space.
///
/// Its own function because it is only ever exercised past the first batch —
/// a file needs more than `FIEMAP_MAX_EXTENTS` extents to loop again, which no
/// test can arrange reliably on a live filesystem — while getting it wrong
/// (adding instead of subtracting) overflows.
#[cfg(target_os = "linux")]
fn fiemap_window_length(offset: u64) -> u64 {
    u64::MAX - offset
}

/// Does this extent share its blocks with another inode? That is the one bit
/// btrfs/XFS reflinks set, and the whole reason this probe exists.
///
/// Its own function so it can be tested against synthetic flag words: a real
/// shared extent needs a filesystem that can make a reflink, which the CI lane's
/// ext4 cannot, and testing the bit test is what actually matters here.
#[cfg(target_os = "linux")]
fn extent_is_shared(fe_flags: u32) -> bool {
    const FIEMAP_EXTENT_SHARED: u32 = 0x0000_2000;
    fe_flags & FIEMAP_EXTENT_SHARED != 0
}

/// Is this the final extent of the file? Missing it costs an extra ioctl round;
/// seeing it falsely truncates the map and undercounts the file.
#[cfg(target_os = "linux")]
fn extent_is_last(fe_flags: u32) -> bool {
    const FIEMAP_EXTENT_LAST: u32 = 0x0000_0001;
    fe_flags & FIEMAP_EXTENT_LAST != 0
}

/// Did the map come back empty — a fully sparse file, or one stored inline in
/// its inode? Neither is evidence about sharing, so the caller falls back
/// instead of reporting "all private".
#[cfg(target_os = "linux")]
fn mapped_nothing(shared_bytes: u64, private_bytes: u64) -> bool {
    shared_bytes == 0 && private_bytes == 0
}

#[cfg(target_os = "linux")]
fn probe_linux(path: &Path, size: u64) -> Sharing {
    use std::os::fd::AsRawFd;

    const FIEMAP_MAX_EXTENTS: usize = 32;
    const FIEMAP_FLAG_SYNC: u32 = 0x0000_0001;
    // The extent-flag bits live in `extent_is_shared` / `extent_is_last`, which
    // own the bit tests so they can be unit-tested against synthetic flag words.
    // _IOWR('f', 11, struct fiemap). Held as u32 and cast at the call site
    // because libc types the ioctl request differently per target — `c_ulong`
    // on gnu, `c_int` on musl — and the request is a 32-bit value either way.
    // Writing it as `libc::Ioctl` directly would overflow i32 on musl.
    const FS_IOC_FIEMAP: u32 = 0xc020_660b;

    #[repr(C)]
    #[derive(Default, Clone, Copy)]
    struct FiemapExtent {
        fe_logical: u64,
        fe_physical: u64,
        fe_length: u64,
        fe_reserved64: [u64; 2],
        fe_flags: u32,
        fe_reserved: [u32; 3],
    }

    #[repr(C)]
    struct Fiemap {
        fm_start: u64,
        fm_length: u64,
        fm_flags: u32,
        fm_mapped_extents: u32,
        fm_extent_count: u32,
        fm_reserved: u32,
        fm_extents: [FiemapExtent; FIEMAP_MAX_EXTENTS],
    }

    // An empty file shares nothing and frees nothing; skip the syscall.
    if size == 0 {
        return Sharing {
            shared: false,
            private_bytes: 0,
        };
    }

    let Ok(file) = std::fs::File::open(path) else {
        return Sharing::unknown_for(size);
    };

    let mut shared_bytes: u64 = 0;
    let mut private_bytes: u64 = 0;
    let mut any_shared = false;
    let mut offset: u64 = 0;

    // A heavily fragmented file needs more than one batch of extents.
    for _ in 0..64 {
        let mut fm: Fiemap = unsafe { std::mem::zeroed() };
        fm.fm_start = offset;
        fm.fm_length = fiemap_window_length(offset);
        fm.fm_flags = FIEMAP_FLAG_SYNC;
        fm.fm_extent_count = FIEMAP_MAX_EXTENTS as u32;

        let rc = unsafe {
            libc::ioctl(
                file.as_raw_fd(),
                FS_IOC_FIEMAP as libc::Ioctl,
                &mut fm as *mut Fiemap as *mut libc::c_void,
            )
        };
        if rc != 0 {
            return Sharing::unknown_for(size);
        }
        let mapped = fm.fm_mapped_extents as usize;
        if mapped == 0 {
            break;
        }

        let mut last = false;
        for ext in fm.fm_extents.iter().take(mapped.min(FIEMAP_MAX_EXTENTS)) {
            if extent_is_shared(ext.fe_flags) {
                shared_bytes = shared_bytes.saturating_add(ext.fe_length);
                any_shared = true;
            } else {
                private_bytes = private_bytes.saturating_add(ext.fe_length);
            }
            offset = ext.fe_logical.saturating_add(ext.fe_length);
            if extent_is_last(ext.fe_flags) {
                last = true;
            }
        }
        if last {
            break;
        }
    }

    // A file with no mapped extents at all (fully sparse, or inline in the
    // inode) tells us nothing useful — don't claim it is private or shared.
    if mapped_nothing(shared_bytes, private_bytes) {
        return Sharing::unknown_for(size);
    }

    Sharing {
        shared: any_shared,
        // Extents are block-aligned and can overrun the logical length; clamp
        // so a reclaim estimate never exceeds the file's reported size.
        private_bytes: private_bytes.min(size),
    }
}

// ── Everything else ─────────────────────────────────────────────────────────
//
// Windows ReFS block-cloning has no equivalent public query (the restore path
// uses FSCTL_DUPLICATE_EXTENTS_TO_FILE, which is write-only), so there is
// nothing to ask. Callers still fold in `nlink` where the platform has it.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn probe_unsupported(_path: &Path, size: u64) -> Sharing {
    Sharing::unknown_for(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_reports_everything_private_and_unshared() {
        // The conservative direction: a filesystem we can't interrogate must
        // look exactly like the old nlink-only behaviour, never invent sharing.
        let s = Sharing::unknown_for(4096);
        assert!(!s.shared);
        assert_eq!(s.private_bytes, 4096);
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

    #[cfg(target_os = "linux")]
    #[test]
    fn the_fiemap_window_runs_from_the_offset_to_the_end() {
        assert_eq!(fiemap_window_length(0), u64::MAX);
        assert_eq!(fiemap_window_length(4096), u64::MAX - 4096);
    }

    /// The extent flag tests, against synthetic words. A real shared extent
    /// needs a filesystem that can make a reflink, which CI's ext4 cannot, so
    /// this is where the bit arithmetic actually gets checked.
    #[cfg(target_os = "linux")]
    #[test]
    fn extent_flags_are_read_bit_by_bit() {
        const SHARED: u32 = 0x0000_2000;
        const LAST: u32 = 0x0000_0001;
        // 0x0800 is FIEMAP_EXTENT_ENCODED: a neighbouring bit that must not be
        // mistaken for sharing.
        const OTHER: u32 = 0x0000_0800;

        assert!(extent_is_shared(SHARED));
        assert!(extent_is_shared(SHARED | LAST | OTHER));
        assert!(!extent_is_shared(0));
        assert!(
            !extent_is_shared(OTHER | LAST),
            "only the SHARED bit means shared — a false positive here reports \
             private storage as already-cached"
        );

        assert!(extent_is_last(LAST));
        assert!(extent_is_last(LAST | SHARED));
        assert!(!extent_is_last(0));
        assert!(!extent_is_last(SHARED | OTHER));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn an_empty_map_is_not_evidence_of_anything() {
        assert!(mapped_nothing(0, 0));
        assert!(!mapped_nothing(0, 4096), "private bytes were mapped");
        assert!(!mapped_nothing(4096, 0), "shared bytes were mapped");
        assert!(!mapped_nothing(4096, 4096));
    }

    /// A sparse file allocates far fewer bytes than its length, so the honest
    /// answer differs from the fallback. That is what makes this test able to
    /// see a probe that quietly gave up: `unknown_for` would claim the whole
    /// apparent size is reclaimable, and `clean` would overstate what a delete
    /// returns.
    ///
    /// Needs no reflink support, so it works on the ext4 that CI runs on.
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

        let s = probe(&path, size);
        assert!(
            s.private_bytes < size,
            "a hole is not reclaimable storage: {s:?} for a {size}-byte file \
             holding {allocated} allocated bytes"
        );
    }

    /// The case the whole module exists for: a reflinked copy has `nlink == 1`
    /// yet shares all its blocks, which is what made `clean` report 0% cached
    /// on APFS. Skips itself where the platform or filesystem has no reflink.
    #[cfg(any(target_os = "macos", target_os = "linux"))]
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

        let s = probe(&dst, size);
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
