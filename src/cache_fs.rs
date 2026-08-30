//! Detect the filesystem hosting the cache directory and warn when it is not
//! host-local (kunobi-ninja/kache#415, Part A).
//!
//! The index is a WAL-mode SQLite database. SQLite's WAL requires shared memory
//! and working POSIX/`LockFileEx` locking between the processes touching it, so
//! it is only safe on a filesystem owned by **one** machine. Put it on a network
//! or guest-visible virtual mount and the failure mode is not a clean error — it
//! is `SQLITE_CORRUPT`. That is exactly #412: macOS and Linux both wrote one
//! index in a shared home directory and the database became "malformed"; the
//! same report later turned out to involve a `virtiofs` mount (a Linux VM on a
//! Mac sharing the host home dir).
//!
//! #415 Part B made that corruption recoverable (`store::open_index_db`
//! quarantines and rebuilds). This module is Part A: say so *up front*, before
//! the corruption, so the class of report never has to be filed.
//!
//! ## Why a separate module
//!
//! `link.rs` owns *volume capability* probing (does this volume block-clone?),
//! which is a restore-path concern. This is an *index durability* concern on a
//! different surface (wrapper startup, daemon startup, `kache doctor`), so it
//! lives on its own rather than growing either `link.rs` or `store.rs`.
//!
//! ## Polarity: an unknown filesystem stays quiet
//!
//! `link.rs` deliberately resolves an unknown *toward warning* — there, a copy
//! restore is provably happening whatever the probe says, so hedged advice is
//! still true. Here the opposite is right. The overwhelming majority of
//! unrecognised filesystems are ordinary local disks, and telling those users
//! their cache is on a network mount would be both wrong and unactionable. A
//! probe that fails, or a filesystem we do not recognise, therefore produces
//! [`CacheFsVerdict::Unknown`] and says nothing.

use std::path::Path;

/// What the OS reported about the filesystem hosting a path.
///
/// Split from [`classify`] so the decision table is pure and testable on every
/// platform, the way `link::classify_copy_restore` is.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FsProbe {
    /// Filesystem name as the OS spells it (`apfs`, `nfs`, `virtiofs`, `NTFS`),
    /// lowercased on Unix where the kernel's spelling is already lowercase.
    /// `None` when the probe could not name it.
    pub name: Option<String>,
    /// Whether the filesystem is host-local. `None` means "could not tell" and
    /// must not be read as either answer.
    pub is_local: Option<bool>,
    /// Total bytes of the volume that holds the path. `None` when the probe
    /// could not measure it. Used for the disk-share store budget, not for
    /// locality classification.
    pub total_bytes: Option<u64>,
}

impl FsProbe {
    /// A probe that learned nothing (syscall failed, or an unsupported target).
    fn unknown() -> Self {
        Self::default()
    }
}

/// Whether the cache directory's filesystem can safely host the WAL index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheFsVerdict {
    /// Host-local storage — what the index needs. Nothing to say.
    Local,
    /// A network or guest-visible virtual filesystem. This is the #412 case:
    /// the WAL index will eventually corrupt if more than one machine writes it.
    NotLocal { name: String },
    /// The probe failed, or the filesystem is one we do not classify. Silent by
    /// design — see the module docs on polarity.
    Unknown,
}

/// Classify a probe result. Pure, so the table is exercised on every platform
/// rather than only on the one that produced the numbers.
pub fn classify(probe: &FsProbe) -> CacheFsVerdict {
    match probe.is_local {
        Some(true) => CacheFsVerdict::Local,
        Some(false) => CacheFsVerdict::NotLocal {
            // Name the filesystem when we have it; the advisory reads far better
            // as "nfs" than as a generic "network filesystem", and it is the
            // first thing a user needs in order to recognise their own setup.
            name: probe
                .name
                .clone()
                .unwrap_or_else(|| "network or virtual filesystem".to_string()),
        },
        None => CacheFsVerdict::Unknown,
    }
}

/// The user-facing advisory. Pure — takes the facts, returns the text — so it is
/// unit-testable without touching stderr, matching `store_unavailable_message`.
///
/// Deliberately parallel to that message: same `[kache]` prefix, same shape, and
/// the same remedy (`KACHE_CACHE_DIR` on a local path). The difference is tense.
/// `store_unavailable_message` explains a failure that already happened; this one
/// warns before it does, so it must be explicit that the build is working *now*
/// and the risk is corruption *later*. A user who reads this as "kache is broken"
/// when their build is fine will conclude the warning is noise.
pub fn advisory_message(name: &str, cache_dir: &Path) -> String {
    format!(
        "[kache] the cache directory is on {name} ({dir}), which is not host-local\n\
         [kache] storage. The cache index is a WAL-mode SQLite database: it needs\n\
         [kache] working file locking and a single writing machine. On a shared or\n\
         [kache] network mount it can be silently CORRUPTED — builds keep working\n\
         [kache] until they don't, and the cache is then rebuilt from scratch.\n\
         [kache] → set KACHE_CACHE_DIR to a fast, local, single-machine path\n\
         [kache] → to share artifacts BETWEEN machines, use a remote cache (S3 or\n\
         [kache]   a filesystem remote) rather than a shared cache directory",
        name = name,
        dir = cache_dir.display(),
    )
}

/// The whole decision in one pure step: probe result in, advisory text out, or
/// `None` when there is nothing worth saying.
///
/// Exists so the *composition* — classify, then phrase — is covered by a test
/// rather than living untested in each of the three call sites (wrapper startup,
/// daemon startup, `kache doctor`).
pub fn advisory_for(probe: &FsProbe, cache_dir: &Path) -> Option<String> {
    match classify(probe) {
        CacheFsVerdict::NotLocal { name } => Some(advisory_message(&name, cache_dir)),
        CacheFsVerdict::Local | CacheFsVerdict::Unknown => None,
    }
}

/// Probe the filesystem hosting `path`.
///
/// Best-effort and non-fatal by construction: every failure path yields
/// [`FsProbe::unknown`], which classifies to [`CacheFsVerdict::Unknown`] and
/// stays silent. This runs at wrapper startup, so it must never fail a build.
pub fn probe(path: &Path) -> FsProbe {
    probe_impl(path)
}

/// Volume size from a `statfs` block size and block count.
///
/// A zero block size is `None` (the kernel did not report a size), not a
/// zero-byte disk. Overflow is `None`. Pure so the cases are tested on every
/// platform, not only the OS whose `statfs` produced the numbers.
#[cfg_attr(
    not(any(target_os = "macos", target_os = "linux")),
    allow(dead_code)
)]
fn disk_bytes(block_size: u64, blocks: u64) -> Option<u64> {
    if block_size == 0 {
        return None;
    }
    block_size.checked_mul(blocks)
}

/// Linux `statfs` fragment size: `f_frsize` when the kernel reports it, else
/// `f_bsize`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn linux_fragment_bytes(frsize: u64, bsize: u64) -> u64 {
    if frsize > 0 { frsize } else { bsize }
}

/// Windows `GetDiskFreeSpaceExW` reports success as a nonzero BOOL. A zero
/// total is treated as a failed probe, not a zero-byte disk.
#[cfg_attr(not(windows), allow(dead_code))]
fn accepted_volume_total(ok: bool, total: u64) -> Option<u64> {
    (ok && total > 0).then_some(total)
}

#[cfg_attr(not(windows), allow(dead_code))]
fn win32_succeeded(ok: i32) -> bool {
    ok != 0
}

// ── macOS ───────────────────────────────────────────────────────────────────
//
// `statfs` carries both answers directly: `MNT_LOCAL` is the kernel's own
// "this filesystem is local" bit (the same one `df -l` filters on), and
// `f_fstypename` names it. No magic-number table needed.
#[cfg(target_os = "macos")]
fn probe_impl(path: &Path) -> FsProbe {
    let Some(stat) = statfs_of(path) else {
        return FsProbe::unknown();
    };
    FsProbe {
        name: c_str_field_to_string(&stat.f_fstypename),
        is_local: Some(stat.f_flags & (libc::MNT_LOCAL as u32) != 0),
        total_bytes: statfs_total_bytes(&stat),
    }
}

/// Read a fixed-size NUL-padded C char array (`f_fstypename`) as a String.
#[cfg(target_os = "macos")]
fn c_str_field_to_string(field: &[libc::c_char]) -> Option<String> {
    let bytes: Vec<u8> = field
        .iter()
        .take_while(|&&c| c != 0)
        .map(|&c| c as u8)
        .collect();
    if bytes.is_empty() {
        return None;
    }
    String::from_utf8(bytes).ok()
}

// ── Linux ───────────────────────────────────────────────────────────────────
#[cfg(target_os = "linux")]
fn probe_impl(path: &Path) -> FsProbe {
    let Some(stat) = statfs_of(path) else {
        return FsProbe::unknown();
    };
    // `f_type` is not the same type across libcs: `__fsword_t` (i64) on gnu,
    // where this cast is a no-op, but `c_ulong` on musl and `c_uint` on
    // musl/s390x, where it is required. kache ships musl builds and lints on
    // gnu, so the cast has to stay and the lint has to be silenced explicitly
    // rather than "cleaned up" into a musl build failure.
    #[allow(clippy::unnecessary_cast)]
    let magic = stat.f_type as i64;
    let mut probe = classify_linux_magic(magic);
    probe.total_bytes = statfs_total_bytes(&stat);
    probe
}

/// Linux superblock magics, from the kernel's `include/uapi/linux/magic.h`.
///
/// Spelled out here rather than taken from `libc` for three reasons: `libc` is
/// missing several of them (cifs, smb2, 9p, ceph, gfs2, lustre, xfs), it types
/// the ones it does have differently across gnu (`c_long`) and musl (`c_uint`),
/// and — the reason that matters most — hard-coding them lets
/// [`classify_linux_magic`] compile and be **tested on every platform** instead
/// of only on Linux, the same way `link::classify_copy_restore` is.
#[allow(dead_code)]
mod magic {
    pub const NFS: i64 = 0x0000_6969;
    pub const SMB: i64 = 0x0000_517B; // SMB1 via the old smbfs
    pub const CIFS: i64 = 0xFF53_4D42; // SMB1 (cifs.ko)
    pub const SMB2: i64 = 0xFE53_4D42; // SMB2/3 (smb3.ko)
    pub const V9FS: i64 = 0x0102_1997; // 9p — the classic VM shared mount
    pub const CEPH: i64 = 0x00C3_6400;
    pub const GFS2: i64 = 0x0116_1970;
    pub const LUSTRE: i64 = 0x0BD0_0BD0;
    pub const NCP: i64 = 0x0000_564C;
    pub const AFS: i64 = 0x5346_414F; // OpenAFS
    pub const AFS_FS: i64 = 0x6B41_4653; // kAFS (in-kernel, distinct magic)
    pub const FUSE: i64 = 0x6573_5546;
    pub const EXT: i64 = 0x0000_EF53; // shared by ext2/3/4
    pub const BTRFS: i64 = 0x9123_683E;
    pub const XFS: i64 = 0x5846_5342;
    pub const F2FS: i64 = 0xF2F5_2010;
    pub const ZFS: i64 = 0x2FC1_2FC1;
    pub const TMPFS: i64 = 0x0102_1994;
    pub const OVERLAYFS: i64 = 0x794C_7630;
}

/// Linux has no `MNT_LOCAL` equivalent in `statfs`, so locality comes from the
/// superblock magic. Pure and platform-independent so the table is unit-tested
/// everywhere, not only on the OS that produces the numbers.
///
/// Three buckets, and the *silent* one carries the most weight:
///
/// - **Not local** — the filesystems that actually break WAL.
/// - **Local** — named explicitly so a common local filesystem never falls
///   through to `Unknown` and can never be mistaken for remote.
/// - **Unknown** — everything else, silent.
///
/// Two judgement calls worth stating, because both would be noisy if wrong:
///
/// - `overlayfs` is **local**. It is Docker's default upper layer, so warning on
///   it would fire on a large share of all containerised builds — builds that
///   work. A container's cache dir is single-machine unless the user bind-mounts
///   it, and the bind-mount case shows up as the underlying filesystem anyway.
/// - `tmpfs` is **local**. It is volatile, not shared; losing a cache on reboot
///   is a different conversation from corrupting one, and not this warning's job.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn classify_linux_magic(magic: i64) -> FsProbe {
    let named = |name: &str, is_local: bool| FsProbe {
        name: Some(name.to_string()),
        is_local: Some(is_local),
        total_bytes: None,
    };

    match magic {
        magic::NFS => named("nfs", false),
        magic::SMB => named("smbfs", false),
        magic::CIFS => named("cifs (SMB)", false),
        magic::SMB2 => named("smb2/smb3", false),
        magic::V9FS => named("9p", false),
        magic::CEPH => named("ceph", false),
        magic::GFS2 => named("gfs2", false),
        magic::LUSTRE => named("lustre", false),
        magic::NCP => named("ncpfs", false),
        magic::AFS | magic::AFS_FS => named("afs", false),
        // FUSE is the #412 case: `virtiofs` and `sshfs` both land here, and a
        // Linux VM sharing a Mac's home dir is precisely how that index
        // corrupted. FUSE also backs some purely local filesystems, so the
        // advisory names FUSE rather than asserting "network" — the user can
        // tell which of the two they have, and the WAL caveat applies to the
        // guest-visible mounts either way.
        magic::FUSE => named("a FUSE filesystem", false),
        // Explicitly local: never let these reach the Unknown bucket.
        magic::EXT => named("ext2/3/4", true),
        magic::BTRFS => named("btrfs", true),
        magic::XFS => named("xfs", true),
        magic::F2FS => named("f2fs", true),
        magic::ZFS => named("zfs", true),
        magic::TMPFS => named("tmpfs", true),
        magic::OVERLAYFS => named("overlayfs", true),
        _ => FsProbe::unknown(),
    }
}

#[cfg(target_os = "macos")]
fn statfs_total_bytes(stat: &libc::statfs) -> Option<u64> {
    disk_bytes(u64::from(stat.f_bsize), stat.f_blocks)
}

#[cfg(target_os = "linux")]
fn statfs_total_bytes(stat: &libc::statfs) -> Option<u64> {
    let frsize = u64::try_from(stat.f_frsize).unwrap_or(0);
    let bsize = u64::try_from(stat.f_bsize).unwrap_or(0);
    disk_bytes(linux_fragment_bytes(frsize, bsize), stat.f_blocks)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn statfs_of(path: &Path) -> Option<libc::statfs> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    // The cache dir may not exist yet on a first run; fall back to the nearest
    // existing ancestor so a cold start still gets an answer.
    let target = nearest_existing(path)?;
    let c_path = CString::new(target.as_os_str().as_bytes()).ok()?;
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statfs(c_path.as_ptr(), &mut stat) };
    (rc == 0).then_some(stat)
}

/// `path` if it exists, else its nearest existing ancestor. Used so the probe
/// works before `Store::open` has created the cache directory.
#[cfg(any(target_os = "macos", target_os = "linux", windows))]
fn nearest_existing(path: &Path) -> Option<std::path::PathBuf> {
    let mut current = path;
    loop {
        if current.exists() {
            return Some(current.to_path_buf());
        }
        current = current.parent()?;
    }
}

// ── Windows ─────────────────────────────────────────────────────────────────
//
// `GetDriveTypeW` answers locality directly (`DRIVE_REMOTE` for a mapped network
// drive), and a UNC path (`\\server\share`) is remote by construction — it has
// no drive letter for `GetDriveTypeW` to classify, so it is checked first.
#[cfg(windows)]
fn probe_impl(path: &Path) -> FsProbe {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{GetDriveTypeW, GetVolumeInformationW};

    // Values from winbase.h; windows-sys exposes the function but not these.
    const DRIVE_UNKNOWN: u32 = 0;
    const DRIVE_NO_ROOT_DIR: u32 = 1;
    const DRIVE_REMOTE: u32 = 4;

    let Some(target) = nearest_existing(path) else {
        return FsProbe::unknown();
    };

    // A UNC path is a network share whatever the drive-type call makes of it.
    if is_unc_path(&target) {
        return FsProbe {
            name: Some("a network share (UNC path)".to_string()),
            is_local: Some(false),
            total_bytes: volume_total_bytes(&target),
        };
    }

    let Some(root) = volume_root(&target) else {
        return FsProbe::unknown();
    };
    let wide: Vec<u16> = std::ffi::OsStr::new(&root)
        .encode_wide()
        .chain(Some(0))
        .collect();

    let drive_type = unsafe { GetDriveTypeW(wide.as_ptr()) };
    let is_local = match drive_type {
        DRIVE_REMOTE => Some(false),
        // No answer: don't guess in either direction.
        DRIVE_UNKNOWN | DRIVE_NO_ROOT_DIR => None,
        // Fixed / removable / ramdisk / CD-ROM are all machine-local.
        _ => Some(true),
    };

    // Name is best-effort decoration; a failure here must not discard the
    // locality answer we already have.
    let mut fs_name = [0u16; 64];
    let ok = unsafe {
        GetVolumeInformationW(
            wide.as_ptr(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            fs_name.as_mut_ptr(),
            fs_name.len() as u32,
        )
    };
    let name = (ok != 0).then(|| {
        let len = fs_name
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(fs_name.len());
        String::from_utf16_lossy(&fs_name[..len])
    });

    FsProbe {
        name,
        is_local,
        total_bytes: volume_total_bytes(&target),
    }
}

#[cfg(windows)]
fn volume_total_bytes(path: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut total: u64 = 0;
    let ok = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            std::ptr::null_mut(),
            &mut total,
            std::ptr::null_mut(),
        )
    };
    accepted_volume_total(win32_succeeded(ok), total)
}

/// Volume mount root (`C:\`) holding `path`. Mirrors `link::windows_volume_root`
/// — duplicated rather than shared because that one is private to the restore
/// path and this module must not depend on it.
#[cfg(windows)]
fn volume_root(path: &Path) -> Option<String> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetVolumePathNameW;

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut root = [0u16; 260];
    let ok = unsafe { GetVolumePathNameW(wide.as_ptr(), root.as_mut_ptr(), root.len() as u32) };
    if ok == 0 {
        return None;
    }
    let len = root.iter().position(|&c| c == 0).unwrap_or(root.len());
    Some(String::from_utf16_lossy(&root[..len]))
}

/// Whether `path` is a UNC path (`\\server\share`, or the `\\?\UNC\...` form).
/// Pure string inspection, so it is tested on every platform.
#[cfg_attr(not(windows), allow(dead_code))]
fn is_unc_path(path: &Path) -> bool {
    let s = path.to_string_lossy();
    let s = s.replace('/', "\\");
    if let Some(rest) = s.strip_prefix("\\\\?\\") {
        return rest.to_ascii_uppercase().starts_with("UNC\\");
    }
    // `\\server\share` but not the `\\.\` device namespace.
    s.starts_with("\\\\") && !s.starts_with("\\\\.\\")
}

// ── Unsupported targets ─────────────────────────────────────────────────────
#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
fn probe_impl(_path: &Path) -> FsProbe {
    FsProbe::unknown()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_filesystem_says_nothing() {
        let probe = FsProbe {
            name: Some("apfs".to_string()),
            is_local: Some(true),
            total_bytes: None,
        };
        assert_eq!(classify(&probe), CacheFsVerdict::Local);
    }

    #[test]
    fn network_filesystem_is_reported_by_name() {
        let probe = FsProbe {
            name: Some("nfs".to_string()),
            is_local: Some(false),
            total_bytes: None,
        };
        assert_eq!(
            classify(&probe),
            CacheFsVerdict::NotLocal {
                name: "nfs".to_string()
            }
        );
    }

    #[test]
    fn non_local_without_a_name_still_warns_generically() {
        let probe = FsProbe {
            name: None,
            is_local: Some(false),
            total_bytes: None,
        };
        let CacheFsVerdict::NotLocal { name } = classify(&probe) else {
            panic!("a non-local filesystem must warn even when unnamed");
        };
        assert_eq!(name, "network or virtual filesystem");
    }

    #[test]
    fn an_unknown_probe_is_silent_not_a_guess() {
        // The polarity that keeps this advisory from becoming noise: a failed
        // probe must not warn, and must not claim the filesystem is local
        // either — it produces no verdict at all.
        assert_eq!(classify(&FsProbe::unknown()), CacheFsVerdict::Unknown);
        // A named filesystem whose locality is unknown is equally silent.
        let named_but_unplaced = FsProbe {
            name: Some("weirdfs".to_string()),
            is_local: None,
            total_bytes: None,
        };
        assert_eq!(classify(&named_but_unplaced), CacheFsVerdict::Unknown);
    }

    #[test]
    fn the_advisory_names_the_filesystem_and_the_directory() {
        let msg = advisory_message("nfs", Path::new("/mnt/shared/kache"));
        assert!(
            msg.contains("nfs"),
            "message must name the filesystem: {msg}"
        );
        assert!(
            msg.contains("/mnt/shared/kache"),
            "message must show which directory is affected: {msg}"
        );
        assert!(
            msg.contains("KACHE_CACHE_DIR"),
            "message must give the actionable remedy: {msg}"
        );
        // The user's build is working right now; the message must not read as
        // "kache is broken" or it will be dismissed as noise.
        assert!(
            msg.contains("CORRUPTED"),
            "message must state the actual risk: {msg}"
        );
        for line in msg.lines() {
            assert!(
                line.starts_with("[kache]"),
                "every line carries the prefix so it can't be mistaken for compiler output: {line}"
            );
        }
    }

    #[test]
    fn advisory_is_produced_only_for_a_non_local_filesystem() {
        let dir = Path::new("/mnt/shared/kache");
        let advisory = advisory_for(
            &FsProbe {
                name: Some("nfs".to_string()),
                is_local: Some(false),
                total_bytes: None,
            },
            dir,
        );
        assert_eq!(
            advisory.as_deref(),
            Some(advisory_message("nfs", dir)).as_deref()
        );

        // Local and Unknown must both stay silent — this is the composition
        // that decides whether a working build gets nagged.
        for quiet in [
            FsProbe {
                name: Some("apfs".to_string()),
                is_local: Some(true),
                total_bytes: None,
            },
            FsProbe::unknown(),
        ] {
            assert_eq!(
                advisory_for(&quiet, dir),
                None,
                "must stay silent for {quiet:?}"
            );
        }
    }

    #[test]
    fn probe_reports_a_positive_volume_size_for_a_real_directory() {
        let dir = tempfile::tempdir().unwrap();
        let total = probe(dir.path()).total_bytes;
        assert!(
            total.is_some_and(|n| n > 0),
            "a real directory must yield a volume size, got {total:?}"
        );
    }

    #[test]
    fn a_linux_shared_mount_produces_an_advisory_end_to_end() {
        // Table → classify → phrase, in one go: the 9p magic a VM shared mount
        // reports must come out the far end as text naming the filesystem.
        let advisory = advisory_for(&classify_linux_magic(0x0102_1997), Path::new("/kache"))
            .expect("a 9p mount must produce an advisory");
        assert!(advisory.contains("9p"), "{advisory}");
        assert!(advisory.contains("/kache"), "{advisory}");

        // And the Docker default must not.
        assert_eq!(
            advisory_for(&classify_linux_magic(0x794C_7630), Path::new("/kache")),
            None
        );
    }

    #[test]
    fn unc_paths_are_recognised_in_every_spelling() {
        assert!(is_unc_path(Path::new(r"\\server\share\kache")));
        assert!(is_unc_path(Path::new(r"\\?\UNC\server\share\kache")));
        // Device namespace is not a network share.
        assert!(!is_unc_path(Path::new(r"\\.\PhysicalDrive0")));
        // Ordinary local paths.
        assert!(!is_unc_path(Path::new(r"C:\Users\me\.cache\kache")));
        assert!(!is_unc_path(Path::new(r"\\?\C:\Users\me\.cache\kache")));
        assert!(!is_unc_path(Path::new("/home/me/.cache/kache")));
    }

    // The Linux table is pure, so it is exercised on every platform — the
    // literals below are the kernel's magic.h values, deliberately written out
    // rather than referencing `magic::*`, so a typo in the table is caught
    // instead of being asserted against itself.
    #[test]
    fn linux_magic_table_flags_every_shared_mount() {
        let not_local = |magic: i64| {
            let probe = classify_linux_magic(magic);
            assert_eq!(
                probe.is_local,
                Some(false),
                "magic {magic:#x} must be classified as non-local"
            );
            probe.name.unwrap_or_default()
        };

        assert_eq!(not_local(0x0000_6969), "nfs");
        assert_eq!(not_local(0x0000_517B), "smbfs");
        assert_eq!(not_local(0xFF53_4D42), "cifs (SMB)");
        assert_eq!(not_local(0xFE53_4D42), "smb2/smb3");
        assert_eq!(not_local(0x0102_1997), "9p");
        assert_eq!(not_local(0x00C3_6400), "ceph");
        assert_eq!(not_local(0x0116_1970), "gfs2");
        assert_eq!(not_local(0x0BD0_0BD0), "lustre");
        assert_eq!(not_local(0x0000_564C), "ncpfs");
        assert_eq!(not_local(0x5346_414F), "afs");
        assert_eq!(not_local(0x6B41_4653), "afs");
        // virtiofs and sshfs — the mount behind the #412 corruption report.
        assert_eq!(not_local(0x6573_5546), "a FUSE filesystem");
    }

    #[test]
    fn linux_magic_table_leaves_local_disks_alone() {
        for (magic, expected) in [
            (0x0000_EF53_i64, "ext2/3/4"),
            (0x9123_683E, "btrfs"),
            (0x5846_5342, "xfs"),
            (0xF2F5_2010, "f2fs"),
            (0x2FC1_2FC1, "zfs"),
        ] {
            let probe = classify_linux_magic(magic);
            assert_eq!(
                classify(&probe),
                CacheFsVerdict::Local,
                "{expected} must read as local, got {probe:?}"
            );
            assert_eq!(probe.name.as_deref(), Some(expected));
        }
    }

    #[test]
    fn container_filesystems_do_not_warn() {
        // overlayfs is Docker's default and tmpfs is ordinary local scratch.
        // Warning on either would fire on a large share of working builds, so
        // both must classify Local rather than merely Unknown.
        for (magic, name) in [(0x794C_7630_i64, "overlayfs"), (0x0102_1994, "tmpfs")] {
            assert_eq!(
                classify(&classify_linux_magic(magic)),
                CacheFsVerdict::Local,
                "{name} must not produce an advisory"
            );
        }
    }

    #[test]
    fn an_unrecognised_magic_falls_through_to_silence() {
        assert_eq!(
            classify(&classify_linux_magic(0x1234_5678)),
            CacheFsVerdict::Unknown
        );
    }

    #[test]
    fn probing_a_real_local_directory_never_reports_a_network_mount() {
        // The temp dir is local on every platform CI runs on. This guards the
        // whole syscall path against a false positive, which is the failure
        // mode that would make this advisory user-hostile.
        let probe = probe(&std::env::temp_dir());
        assert_ne!(
            probe.is_local,
            Some(false),
            "a local temp dir must never be classified as non-local (probe: {probe:?})"
        );
    }

    #[test]
    fn probing_a_path_that_does_not_exist_yet_uses_its_parent() {
        // First run: the cache dir has not been created. The probe must still
        // resolve, via the nearest existing ancestor, rather than give up.
        let missing = std::env::temp_dir().join("kache-cache-fs-probe-does-not-exist");
        let _ = std::fs::remove_dir_all(&missing);
        let probe = probe(&missing);
        assert_ne!(probe.is_local, Some(false));
        #[cfg(any(target_os = "macos", target_os = "linux", windows))]
        assert!(
            probe.is_local.is_some(),
            "a missing cache dir must still resolve through its parent: {probe:?}"
        );
    }
}
