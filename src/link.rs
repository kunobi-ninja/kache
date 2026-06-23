use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

/// Process-global: restore via hardlink on a non-CoW Windows volume (NTFS)
/// instead of the default copy (#429). Set once from `config.windows_hardlink`
/// at wrapper entry; read on the Windows restore path. Off everywhere else.
static WINDOWS_HARDLINK_RESTORE: AtomicBool = AtomicBool::new(false);

/// Set the Windows hardlink-restore opt-in (from `Config::windows_hardlink`).
/// Call once per process before restoring. No effect off Windows.
pub fn set_windows_hardlink_restore(enabled: bool) {
    WINDOWS_HARDLINK_RESTORE.store(enabled, Ordering::Relaxed);
}

/// Strategy for restoring a cached file to a build output path.
///
/// Restoration always tries reflink (CoW: zero-copy *with* an independent
/// inode) first. The strategy only controls the fallback when reflink is
/// unavailable (e.g. ext4 without `mkfs.ext4 -O reflink`, tmpfs, NTFS):
///
/// - `Hardlink`: fall back to a hardlink (zero-copy via shared inode). For
///   immutable artifacts like `.rlib` / `.rmeta` where the build won't mutate
///   the restored file. On a non-CoW filesystem, mutations would propagate
///   into the cache blob — so callers using this strategy must guarantee
///   the artifact stays untouched (or use `rewrite_depinfo`'s nlink-aware
///   path).
/// - `Copy`: fall back to a plain byte copy (independent file). For
///   executables, dylibs, and proc-macros that may be mutated post-build
///   (codesigning, stripping, etc.).
///
/// On APFS, btrfs, or XFS-with-reflink the two strategies behave identically:
/// both reflink, both produce independent inodes, both are safe against
/// post-build mutation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkStrategy {
    Hardlink,
    Copy,
}

/// Link a cached file to the target output path.
///
/// Tries reflink first (zero-copy + write isolation on supported filesystems);
/// falls back to a strategy-specific path on filesystems without CoW.
pub fn link_to_target(store_path: &Path, target_path: &Path, strategy: LinkStrategy) -> Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent dir for {}", target_path.display()))?;
    }

    // Remove existing file at target (link/clone calls fail if dst exists).
    clear_target(target_path)?;

    // Logical size of the artifact, attributed to whichever restoration
    // mechanism runs below. Best-effort — a metadata failure here must
    // not fail the restore.
    let bytes = fs::metadata(store_path).map(|m| m.len()).unwrap_or(0);

    // Try reflink first. CoW gives us zero-copy *and* mutations don't
    // propagate to the cache blob — strictly better than hardlink when
    // available (APFS, btrfs, XFS-with-reflink).
    if try_reflink(store_path, target_path).is_ok() {
        if matches!(strategy, LinkStrategy::Copy) {
            // Reflink preserves source mode (0o444 for stored blobs);
            // executables/dylibs need 0o755 so cargo can run/load them.
            set_executable_perms(target_path)?;
        }
        tracing::debug!(
            "reflinked {} -> {}",
            store_path.display(),
            target_path.display()
        );
        crate::opcounts::record_reflinked(bytes);
        return Ok(());
    }

    // Reflink unsupported on this filesystem — strategy-specific fallback.
    match strategy {
        // Windows has no reflink on NTFS, so the Hardlink strategy would
        // hardlink the read-only store blob. NTFS stores FILE_ATTRIBUTE_READONLY
        // in the shared MFT record, so EVERY hardlink to a read-only blob is
        // itself read-only — and Windows refuses to delete or rewrite a
        // read-only file (WinError 5). A consumer that owns its output and
        // deletes/rewrites it — e.g. mozbuild's configure `ar_supports_response_files`
        // conftest (#429) — then breaks. There is no way on NTFS to give a
        // hardlink a different read-only state than its blob, so restore via an
        // independent COPY instead: the output is writable and deletable while
        // the store blob stays read-only (integrity preserved). This mirrors
        // `write_restored`, already the proven-safe independent-file path, and
        // costs only working-tree↔store block sharing (LRU is index-based, not
        // mtime-based, so eviction is unaffected). gnu/clang restores keep
        // hardlinking — reflink/hardlink there are writable or CoW-isolated.
        #[cfg(windows)]
        LinkStrategy::Hardlink => {
            if WINDOWS_HARDLINK_RESTORE.load(Ordering::Relaxed) {
                // Opt-in (#429 / `[cache] windows_hardlink`): the caller accepts
                // that this build never deletes/rewrites a restored object, so
                // trade the read-only-output risk for working-tree dedup.
                hardlink_or_copy(store_path, target_path, bytes)
            } else {
                warn_no_cow_restore_once();
                copy_file(store_path, target_path, false)?;
                crate::opcounts::record_copied(bytes);
                Ok(())
            }
        }
        #[cfg(not(windows))]
        LinkStrategy::Hardlink => hardlink_or_copy(store_path, target_path, bytes),
        LinkStrategy::Copy => {
            copy_file(store_path, target_path, true)?;
            crate::opcounts::record_copied(bytes);
            Ok(())
        }
    }
}

/// Hardlink fallback for the `Hardlink` strategy when reflink is unavailable.
/// Falls back to a plain copy on hardlink failure (cross-filesystem).
///
/// On Windows this runs only under the `[cache] windows_hardlink` opt-in — the
/// default restores via copy, because a hardlink to a read-only store blob is
/// itself read-only (shared MFT attribute) and breaks consumers that delete or
/// rewrite their output (#429).
fn hardlink_or_copy(store_path: &Path, target_path: &Path, bytes: u64) -> Result<()> {
    if let Err(e) = fs::hard_link(store_path, target_path) {
        tracing::debug!(
            "hardlink failed ({}), falling back to copy: {} -> {}",
            e,
            store_path.display(),
            target_path.display()
        );
        copy_file(store_path, target_path, false)?;
        crate::opcounts::record_copied(bytes);
        return Ok(());
    }
    tracing::debug!(
        "hardlinked {} -> {}",
        store_path.display(),
        target_path.display()
    );
    crate::opcounts::record_hardlinked(bytes);
    Ok(())
}

/// Warn ONCE per process that cache hits are being restored by copy on this
/// non-CoW Windows volume (NTFS), so the cache and build tree don't share
/// storage. One warning per compilation (each TU is its own process); it fires
/// only when a hit is actually copy-restored, so a compile that misses stays
/// silent. ReFS (a Dev Drive) block-clones instead and never reaches here, and
/// `[cache] windows_hardlink = true` opts back into hardlinking.
#[cfg(windows)]
fn warn_no_cow_restore_once() {
    use std::sync::Once;
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        eprintln!(
            "kache: this volume has no copy-on-write (NTFS), so cache hits are \
             restored by COPY — the cache and build tree do not share storage, \
             roughly doubling disk for cached content. For zero-copy dedup put \
             the cache + build dir on a ReFS Dev Drive, or set \
             `[cache] windows_hardlink = true` (only if your build never \
             deletes or rewrites an object output)."
        );
    });
}

/// Set 0o755 on a file. Applied after a successful reflink for the Copy
/// strategy because the reflink preserves the source's 0o444 mode.
fn set_executable_perms(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("setting executable perms on {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let meta = fs::metadata(path)?;
        let mut perms = meta.permissions();
        perms.set_readonly(false);
        fs::set_permissions(path, perms)?;
    }
    Ok(())
}

/// Try a reflink (copy-on-write) clone.
///
/// `pub(crate)` so the store-ingest path can reflink a freshly-compiled
/// artifact into the content-addressed store (sharing blocks with the
/// build's own output) and account for it, mirroring the restore side.
#[cfg(target_os = "macos")]
pub(crate) fn try_reflink(src: &Path, dst: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let src_c = CString::new(src.as_os_str().as_bytes())?;
    let dst_c = CString::new(dst.as_os_str().as_bytes())?;

    // clonefile(2) on macOS / APFS
    unsafe extern "C" {
        fn clonefile(src: *const libc::c_char, dst: *const libc::c_char, flags: u32)
        -> libc::c_int;
    }

    let ret = unsafe { clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
    if ret == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn try_reflink(src: &Path, dst: &Path) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    let src_file = fs::File::open(src)?;
    let dst_file = fs::File::create(dst)?;

    // FICLONE ioctl on Linux (btrfs, XFS with reflink)
    const FICLONE: libc::c_ulong = 0x40049409;

    // Cast needed: ioctl `request` is c_ulong on glibc but c_int on musl
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE as _, src_file.as_raw_fd()) };
    if ret == 0 {
        Ok(())
    } else {
        // Clean up the created file on failure
        let _ = fs::remove_file(dst);
        Err(std::io::Error::last_os_error().into())
    }
}

/// Windows: ReFS block-clone (copy-on-write) via FSCTL_DUPLICATE_EXTENTS_TO_FILE.
///
/// On a ReFS volume (e.g. a Windows 11 Dev Drive) this gives an INDEPENDENT,
/// WRITABLE destination that still shares blocks with the source — so a restored
/// object dedups against the store blob AND a consumer can freely delete/rewrite
/// it (unlike a hardlink, which would be read-only — #429). NTFS has no
/// block-cloning, so this returns `Err` and the caller falls back to copy.
///
/// Correctness (per the ReFS block-cloning contract): clone only the
/// cluster-aligned PREFIX `[0, clone_len)` — cloning past the source's
/// end-of-file / valid-data-length is undefined — then byte-copy the sub-cluster
/// tail. On ANY error the caller deletes the partial dst, so a failed clone never
/// leaves a wrong file. The fresh dst is created writable and does NOT inherit
/// the store blob's read-only attribute (independent file).
#[cfg(windows)]
pub(crate) fn try_reflink(src: &Path, dst: &Path) -> Result<()> {
    let r = reflink_windows(src, dst);
    if r.is_err() {
        // Clear read-only defensively, then remove any partial dst so the
        // caller's copy starts from a clean slate.
        if let Ok(meta) = fs::metadata(dst) {
            let mut perms = meta.permissions();
            perms.set_readonly(false);
            let _ = fs::set_permissions(dst, perms);
        }
        let _ = fs::remove_file(dst);
    }
    r
}

#[cfg(windows)]
fn reflink_windows(src: &Path, dst: &Path) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::{
        DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE,
    };

    let mut src_file = fs::File::open(src)?;
    let len = src_file.metadata()?.len();

    // Fresh, writable destination (CREATE_ALWAYS + read/write).
    let mut dst_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)?;

    if len == 0 {
        return Ok(()); // empty file: dst already created empty
    }

    // Cluster size of the destination volume; clone ranges must be aligned to it.
    let cluster = windows_cluster_size(dst)?;
    let clone_len = (len / cluster) * cluster;
    if clone_len == 0 {
        // Smaller than a cluster — no aligned range to clone. Copy instead.
        anyhow::bail!("file smaller than ReFS cluster; fall back to copy");
    }

    // Allocate the destination clusters and set EOF to the cloned prefix.
    dst_file.set_len(clone_len)?;

    let src_h = src_file.as_raw_handle();
    let dst_h = dst_file.as_raw_handle();
    // Each FSCTL range must be cluster-aligned and strictly < 4 GiB.
    let max_chunk = (((4u64 << 30) - 1) / cluster) * cluster;
    let mut off = 0u64;
    while off < clone_len {
        let chunk = (clone_len - off).min(max_chunk);
        let data = DUPLICATE_EXTENTS_DATA {
            FileHandle: src_h as _,
            SourceFileOffset: off as i64,
            TargetFileOffset: off as i64,
            ByteCount: chunk as i64,
        };
        let mut returned: u32 = 0;
        let ok = unsafe {
            DeviceIoControl(
                dst_h as _,
                FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                &data as *const DUPLICATE_EXTENTS_DATA as *const _,
                size_of::<DUPLICATE_EXTENTS_DATA>() as u32,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        off += chunk;
    }

    // Byte-copy the sub-cluster tail, if any, then set the exact final length.
    if clone_len < len {
        src_file.seek(SeekFrom::Start(clone_len))?;
        dst_file.seek(SeekFrom::Start(clone_len))?;
        let copied = std::io::copy(&mut (&mut src_file).take(len - clone_len), &mut dst_file)?;
        anyhow::ensure!(copied == len - clone_len, "short tail copy");
    }
    dst_file.set_len(len)?;
    Ok(())
}

/// Allocation (cluster) size of the volume that holds `path`, in bytes.
/// Used to align ReFS block-clone ranges. `path` need not exist; its nearest
/// existing parent volume is resolved.
#[cfg(windows)]
fn windows_cluster_size(path: &Path) -> Result<u64> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{GetDiskFreeSpaceW, GetVolumePathNameW};

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut root = [0u16; 260];
    let ok = unsafe { GetVolumePathNameW(wide.as_ptr(), root.as_mut_ptr(), root.len() as u32) };
    if ok == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let (mut spc, mut bps, mut _free, mut _total): (u32, u32, u32, u32) = (0, 0, 0, 0);
    let ok =
        unsafe { GetDiskFreeSpaceW(root.as_ptr(), &mut spc, &mut bps, &mut _free, &mut _total) };
    if ok == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let cluster = spc as u64 * bps as u64;
    anyhow::ensure!(cluster > 0, "zero cluster size");
    Ok(cluster)
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
pub(crate) fn try_reflink(_src: &Path, _dst: &Path) -> Result<()> {
    anyhow::bail!("reflink not supported on this platform")
}

/// Regular file copy with appropriate permissions.
/// `executable`: if true, sets 0o755 (rwxr-xr-x); otherwise 0o644 (rw-r--r--).
fn copy_file(src: &Path, dst: &Path, executable: bool) -> Result<()> {
    fs::copy(src, dst)
        .with_context(|| format!("copying {} to {}", src.display(), dst.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if executable { 0o755 } else { 0o644 };
        fs::set_permissions(dst, fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    {
        let _ = executable;
        let meta = fs::metadata(dst)?;
        let mut perms = meta.permissions();
        perms.set_readonly(false);
        fs::set_permissions(dst, perms)?;
    }

    tracing::debug!("copied {} -> {}", src.display(), dst.display());
    Ok(())
}

/// Remove any file already at `target_path` so a fresh clone / hardlink /
/// write can take its place. A previous restore may have left a
/// read-only hardlink or reflink of a store blob here.
fn clear_target(target_path: &Path) -> Result<()> {
    if target_path.exists() || target_path.symlink_metadata().is_ok() {
        #[cfg(windows)]
        if let Ok(meta) = fs::metadata(target_path) {
            let mut perms = meta.permissions();
            perms.set_readonly(false);
            let _ = fs::set_permissions(target_path, perms);
        }
        fs::remove_file(target_path)
            .with_context(|| format!("removing existing file at {}", target_path.display()))?;
    }
    Ok(())
}

/// Materialize a restored artifact from content computed in memory.
///
/// Used when a post-restore content transform changed the bytes (dep-info
/// path expansion): the final content is written as a fresh, independent,
/// writable file. By construction it shares no inode with the store blob
/// and is not read-only — this is the "compute the final bytes, then
/// materialize" path, as opposed to linking the blob and patching it in
/// place (which fails on a read-only or inode-shared restore).
///
/// `strategy` mirrors [`link_to_target`]: `Copy` is the OS-loadable set
/// (executables, dylibs) and yields `0o755` so cargo / the OS can run or
/// load the result; `Hardlink` (dep-info `.d` and other immutable kinds)
/// yields `0o644`. Keeping the same `Copy ⟺ executable` proxy in both
/// restore primitives means the "executables stay executable" contract
/// holds no matter which path materializes the file — including a future
/// content transform applied to an executable artifact (issue #298).
pub fn write_restored(target_path: &Path, content: &[u8], strategy: LinkStrategy) -> Result<()> {
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent dir for {}", target_path.display()))?;
    }
    clear_target(target_path)?;
    fs::write(target_path, content)
        .with_context(|| format!("writing restored file {}", target_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = if matches!(strategy, LinkStrategy::Copy) {
            0o755
        } else {
            0o644
        };
        fs::set_permissions(target_path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("setting perms on {}", target_path.display()))?;
    }
    #[cfg(not(unix))]
    {
        // No POSIX mode bits on Windows; `fs::write` already produced a
        // writable file. `strategy` only drives the unix permission split.
        let _ = strategy;
    }
    Ok(())
}

/// Update the mtime of a file to the current time.
/// For hardlinked files, this also updates the store copy (same inode),
/// which conveniently acts as an LRU access time update.
pub fn touch_mtime(path: &Path) -> Result<()> {
    let now = filetime::FileTime::now();

    // On Windows, hardlinked files share permissions with the store blob,
    // which is read-only. We need to temporarily make it writable to update
    // the mtime, then restore the read-only flag.
    #[cfg(windows)]
    {
        let meta = fs::metadata(path)?;
        let was_readonly = meta.permissions().readonly();
        if was_readonly {
            let mut perms = meta.permissions();
            perms.set_readonly(false);
            fs::set_permissions(path, perms)?;
        }
        let result = filetime::set_file_mtime(path, now);
        if was_readonly {
            let mut perms = fs::metadata(path)?.permissions();
            perms.set_readonly(true);
            let _ = fs::set_permissions(path, perms);
        }
        result.with_context(|| format!("updating mtime of {}", path.display()))?;
    }

    #[cfg(not(windows))]
    filetime::set_file_mtime(path, now)
        .with_context(|| format!("updating mtime of {}", path.display()))?;

    Ok(())
}

const DEPINFO_ROOT_SENTINEL: &str = "__kache_root__/";

/// Pure dep-info path rewrite: relativize absolute project paths to a
/// kache-only sentinel, or expand that sentinel back to absolute paths.
/// No I/O.
///
/// This is the in-memory half of the transform. The restore side calls
/// it directly — it computes the final `.d` content from the store blob
/// and materializes the result with [`write_restored`], so it never
/// rewrites a restored, possibly read-only, possibly inode-shared file in
/// place. The store side reaches it via [`rewrite_depinfo`].
pub fn rewrite_depinfo_content(content: &str, project_dir: &Path, mode: DepInfoMode) -> String {
    let project_prefix = format!("{}/", project_dir.display());
    match mode {
        DepInfoMode::Relativize => content.replace(&project_prefix, DEPINFO_ROOT_SENTINEL),
        DepInfoMode::Expand => content.replace(DEPINFO_ROOT_SENTINEL, &project_prefix),
    }
}

/// Rewrite a `.d` (dep-info) file in place.
///
/// Used on the **store** side, where the file is the build's own
/// freshly-written, writable dep-info. The restore side does NOT use this
/// — it computes the rewritten content in memory via
/// [`rewrite_depinfo_content`] and materializes it with [`write_restored`],
/// honoring "compute the final bytes, then materialize" rather than
/// patching a restored file in place.
pub fn rewrite_depinfo(depinfo_path: &Path, project_dir: &Path, mode: DepInfoMode) -> Result<()> {
    let content = fs::read_to_string(depinfo_path)
        .with_context(|| format!("reading dep-info file {}", depinfo_path.display()))?;

    let rewritten = rewrite_depinfo_content(&content, project_dir, mode);

    // Defense-in-depth: if the file is hardlinked (nlink > 1), unlink
    // first so the in-place write can't mutate a shared inode. On the
    // store side `Store::put` copies blobs rather than hardlinking, so
    // this rarely fires — but it keeps `rewrite_depinfo` safe for any
    // caller. Windows exposes no portable nlink count; remove
    // unconditionally there.
    #[cfg(unix)]
    if let Ok(meta) = fs::metadata(depinfo_path) {
        use std::os::unix::fs::MetadataExt;
        if meta.nlink() > 1 {
            let _ = fs::remove_file(depinfo_path);
        }
    }
    #[cfg(not(unix))]
    if depinfo_path.exists() {
        let _ = fs::remove_file(depinfo_path);
    }

    fs::write(depinfo_path, rewritten)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum DepInfoMode {
    /// Replace absolute project paths with a kache sentinel for cross-project cache sharing.
    Relativize,
    /// Expand the kache sentinel back to absolute project paths after restoring.
    Expand,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardlink_strategy_restores_content() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.rlib");
        fs::write(&src, b"rlib content").unwrap();

        let dst = dir.path().join("subdir/output.rlib");
        link_to_target(&src, &dst, LinkStrategy::Hardlink).unwrap();

        assert!(dst.exists());
        assert_eq!(fs::read(&dst).unwrap(), b"rlib content");

        // Hardlink strategy promises zero-copy when possible: reflink (CoW,
        // independent inode) on APFS/btrfs/XFS-with-reflink, or hardlink
        // (shared inode) as fallback. We don't assert which mechanism was
        // used — either satisfies the contract.
    }

    #[cfg(windows)]
    #[test]
    fn windows_hardlink_restore_yields_writable_deletable_output() {
        // #429: on Windows a Hardlink-strategy restore must NOT leave the
        // output read-only (a hardlink to the read-only store blob would be),
        // or a consumer that owns its output — mozbuild's configure conftest —
        // cannot delete/rewrite it (WinError 5). The output must be writable
        // and deletable, while the store blob stays read-only (integrity).
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("deadbeef");
        fs::write(&blob, b"obj bytes").unwrap();
        let mut p = fs::metadata(&blob).unwrap().permissions();
        p.set_readonly(true);
        fs::set_permissions(&blob, p).unwrap();

        let out = dir.path().join("conftest.o");
        link_to_target(&blob, &out, LinkStrategy::Hardlink).unwrap();

        assert_eq!(fs::read(&out).unwrap(), b"obj bytes");
        assert!(
            !fs::metadata(&out).unwrap().permissions().readonly(),
            "restored output must be writable on Windows (#429)"
        );
        // The consumer must be able to delete its own output.
        fs::remove_file(&out).expect("consumer must be able to delete its output (#429)");
        // ...without the store blob losing its read-only integrity guard.
        assert!(
            fs::metadata(&blob).unwrap().permissions().readonly(),
            "store blob must stay read-only after a restore"
        );
    }

    #[test]
    fn test_copy_strategy_isolates_writes_from_source() {
        // The Copy strategy guarantees that mutating the destination cannot
        // corrupt the cache blob. This holds whether reflink (CoW) or a
        // plain copy was used; only a hardlink would break it, and Copy
        // never falls back to hardlink.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        fs::write(&src, b"original").unwrap();

        let dst = dir.path().join("dest.bin");
        link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();

        fs::write(&dst, b"modified").unwrap();
        assert_eq!(
            fs::read(&src).unwrap(),
            b"original",
            "Copy strategy must isolate dst writes from src"
        );
    }

    #[test]
    fn test_copy_strategy() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.bin");
        fs::write(&src, b"binary content").unwrap();

        let dst = dir.path().join("output.bin");
        link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();

        assert!(dst.exists());
        assert_eq!(fs::read(&dst).unwrap(), b"binary content");

        // Should NOT be a hardlink
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let src_ino = fs::metadata(&src).unwrap().ino();
            let dst_ino = fs::metadata(&dst).unwrap().ino();
            assert_ne!(src_ino, dst_ino);
        }
    }

    #[test]
    fn test_overwrite_existing() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("source.rlib");
        fs::write(&src, b"new content").unwrap();

        let dst = dir.path().join("output.rlib");
        fs::write(&dst, b"old content").unwrap();

        link_to_target(&src, &dst, LinkStrategy::Hardlink).unwrap();
        assert_eq!(fs::read(&dst).unwrap(), b"new content");
    }

    #[cfg(unix)]
    #[test]
    fn test_overwrite_readonly_hardlink_preserves_source_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let old_blob = dir.path().join("old-blob.rlib");
        let new_blob = dir.path().join("new-blob.rlib");
        let dst = dir.path().join("output.rlib");

        fs::write(&old_blob, b"old content").unwrap();
        fs::set_permissions(&old_blob, fs::Permissions::from_mode(0o444)).unwrap();
        fs::hard_link(&old_blob, &dst).unwrap();

        fs::write(&new_blob, b"new content").unwrap();
        link_to_target(&new_blob, &dst, LinkStrategy::Hardlink).unwrap();

        assert_eq!(fs::read(&dst).unwrap(), b"new content");
        assert!(
            fs::metadata(&old_blob).unwrap().permissions().readonly(),
            "replacing a restored hardlink must not make the original blob writable"
        );
    }

    #[test]
    fn test_touch_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.rlib");
        fs::write(&file, b"content").unwrap();

        // Set mtime to the past
        let past = filetime::FileTime::from_unix_time(1000000000, 0);
        filetime::set_file_mtime(&file, past).unwrap();

        touch_mtime(&file).unwrap();

        let meta = fs::metadata(&file).unwrap();
        let mtime = filetime::FileTime::from_last_modification_time(&meta);
        assert!(mtime.unix_seconds() > 1000000000);
    }

    #[test]
    fn test_depinfo_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let depfile = dir.path().join("test.d");
        fs::write(
            &depfile,
            "/home/user/project/target/debug/deps/libserde.rlib: /home/user/project/src/lib.rs",
        )
        .unwrap();

        rewrite_depinfo(
            &depfile,
            Path::new("/home/user/project"),
            DepInfoMode::Relativize,
        )
        .unwrap();

        let content = fs::read_to_string(&depfile).unwrap();
        assert!(content.contains("target/debug"));
        assert!(content.contains("src/lib.rs"));
        assert!(content.contains(DEPINFO_ROOT_SENTINEL));
        assert!(!content.contains("/home/user/project/"));

        // Now expand back
        rewrite_depinfo(
            &depfile,
            Path::new("/home/user/project"),
            DepInfoMode::Expand,
        )
        .unwrap();

        let content = fs::read_to_string(&depfile).unwrap();
        assert!(content.contains("/home/user/project/"));
    }

    #[test]
    fn test_depinfo_expand_preserves_parent_relative_paths() {
        let input = "\
foo.o: ../../src/foo.cc ../include/foo.h __kache_root__/generated.h foo/./bar.h
";

        let rewritten =
            rewrite_depinfo_content(input, Path::new("/build/worktree/obj"), DepInfoMode::Expand);

        assert!(
            rewritten.contains("../../src/foo.cc"),
            "parent-relative deps must not be expanded: {rewritten}"
        );
        assert!(
            rewritten.contains("../include/foo.h"),
            "single parent-relative deps must not be expanded: {rewritten}"
        );
        assert!(
            rewritten.contains("/build/worktree/obj/generated.h"),
            "sentinel paths must expand: {rewritten}"
        );
        assert!(
            rewritten.contains("foo/./bar.h"),
            "embedded ./ segments are compiler-owned paths: {rewritten}"
        );
    }

    #[test]
    fn test_depinfo_expand_preserves_firefox_parent_relative_depfile_paths() {
        let input = "\
Unified_mm_ettings-WrongChannel0.o: Unified_mm_ettings-WrongChannel0.mm \\
  ../../../../../../../toolkit/mozapps/update/updater/macos-frameworks/UpdateSettings/UpdateSettings.mm \\
  ../../../../../../../toolkit/mozapps/update/updater/macos-frameworks/UpdateSettings/UpdateSettings.h \\
  __kache_root__/mozilla-config.h
";
        let anchor = Path::new(
            "/Users/lenij/work/kache/tmp/bench/clone-a/obj-kache-bench\
             /toolkit/mozapps/update/updater/macos-frameworks/UpdateSettings-WrongChannel",
        );

        let rewritten = rewrite_depinfo_content(input, anchor, DepInfoMode::Expand);

        assert!(
            rewritten.contains(
                "../../../../../../../toolkit/mozapps/update/updater/macos-frameworks\
                 /UpdateSettings/UpdateSettings.mm"
            ),
            "Firefox-style parent-relative source path must survive restore: {rewritten}"
        );
        assert!(
            rewritten.contains(
                "../../../../../../../toolkit/mozapps/update/updater/macos-frameworks\
                 /UpdateSettings/UpdateSettings.h"
            ),
            "Firefox-style parent-relative header path must survive restore: {rewritten}"
        );
        assert!(
            !rewritten.contains("/./Users/") && !rewritten.contains("WrongChannel/./"),
            "restore must not inject the anchor into ../ paths: {rewritten}"
        );
        assert!(
            rewritten.contains(
                "/Users/lenij/work/kache/tmp/bench/clone-a/obj-kache-bench\
                 /toolkit/mozapps/update/updater/macos-frameworks\
                 /UpdateSettings-WrongChannel/mozilla-config.h"
            ),
            "sentinel paths should still expand at the restore anchor: {rewritten}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_copy_executable_gets_execute_permission() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        // Simulate a blob: read-only, no execute bit (as stored in kache's blob store)
        let src = dir.path().join("blob");
        fs::write(&src, b"ELF fake binary").unwrap();
        fs::set_permissions(&src, fs::Permissions::from_mode(0o444)).unwrap();

        let dst = dir.path().join("test_binary");
        link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();

        let mode = fs::metadata(&dst).unwrap().permissions().mode();
        assert_eq!(mode & 0o111, 0o111, "executable should have +x: {mode:#o}");
        assert_eq!(
            mode & 0o200,
            0o200,
            "executable should be writable: {mode:#o}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_hardlink_fallback_no_execute_permission() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let src_dir = tempfile::tempdir().unwrap(); // different tempdir to force cross-dir

        let src = src_dir.path().join("blob.rlib");
        fs::write(&src, b"rlib content").unwrap();
        fs::set_permissions(&src, fs::Permissions::from_mode(0o444)).unwrap();

        let dst = dir.path().join("output.rlib");

        // Hardlink should succeed (same filesystem), so test copy_file directly
        copy_file(&src, &dst, false).unwrap();

        let mode = fs::metadata(&dst).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o111,
            0,
            "non-executable should NOT have +x: {mode:#o}"
        );
        assert_eq!(mode & 0o200, 0o200, "should be writable: {mode:#o}");
    }

    #[cfg(unix)]
    #[test]
    fn test_copy_readonly_blob_becomes_writable_executable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        // Blob stored as read-only (exactly how kache stores them)
        let src = dir.path().join("blob");
        fs::write(&src, b"test binary content").unwrap();
        fs::set_permissions(&src, fs::Permissions::from_mode(0o444)).unwrap();

        let dst = dir.path().join("my_test-abc123");
        link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();

        // Must be executable (cargo test will try to run this)
        let mode = fs::metadata(&dst).unwrap().permissions().mode();
        assert_eq!(mode & 0o755, 0o755, "expected 0o755, got {mode:#o}");
    }

    #[cfg(unix)]
    #[test]
    fn write_restored_over_readonly_blob_link_isolates_and_stays_writable() {
        // The bug this guards: on restore, a `.d` was linked to the
        // read-only store blob, then a post-restore rewrite tried to
        // edit it in place — failing on the 0o444 mode (reflink case)
        // or corrupting the shared blob (hardlink case). `write_restored`
        // instead materializes the final content as a fresh file.
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.d");
        let target = dir.path().join("sub/restored.d");

        // A read-only store blob, and a prior restore that hardlinked it
        // into place (the worst case — shared inode + read-only).
        fs::write(&blob, b"OLD RELATIVIZED CONTENT").unwrap();
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o444)).unwrap();
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::hard_link(&blob, &target).unwrap();

        write_restored(&target, b"NEW EXPANDED CONTENT", LinkStrategy::Hardlink).unwrap();

        // Final content is in place...
        assert_eq!(fs::read(&target).unwrap(), b"NEW EXPANDED CONTENT");
        // ...the restored file is writable (an in-place edit could not
        // have failed on it)...
        let mode = fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o200,
            0o200,
            "restored file must be writable: {mode:#o}"
        );
        // ...it shares no inode with the blob...
        assert_ne!(
            fs::metadata(&target).unwrap().ino(),
            fs::metadata(&blob).unwrap().ino(),
            "restored file must not share an inode with the store blob"
        );
        // ...and the store blob is byte-for-byte untouched.
        assert_eq!(fs::read(&blob).unwrap(), b"OLD RELATIVIZED CONTENT");
        assert!(
            fs::metadata(&blob).unwrap().permissions().readonly(),
            "store blob must remain read-only"
        );
    }

    #[test]
    fn write_restored_creates_missing_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("a/b/c/out.d");
        write_restored(&target, b"content", LinkStrategy::Hardlink).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"content");
    }

    #[cfg(unix)]
    #[test]
    fn write_restored_copy_strategy_sets_executable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("deps/e2e-abc123");

        // Copy strategy is what Executable / DynamicLibrary kinds use:
        // a materialized executable must be runnable by the OS (#298).
        write_restored(&target, b"ELF fake binary", LinkStrategy::Copy).unwrap();

        let mode = fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(mode & 0o111, 0o111, "executable should have +x: {mode:#o}");
        assert_eq!(
            mode & 0o200,
            0o200,
            "executable should be writable: {mode:#o}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn write_restored_hardlink_strategy_is_not_executable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("sub/restored.d");

        // Hardlink strategy backs dep-info (.d) and other immutable kinds:
        // materialized content stays a plain 0o644 file, never executable.
        write_restored(&target, b"deps: src.rs", LinkStrategy::Hardlink).unwrap();

        let mode = fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o111,
            0,
            "non-executable must NOT have +x: {mode:#o}"
        );
        assert_eq!(mode & 0o200, 0o200, "should be writable: {mode:#o}");
    }
}
