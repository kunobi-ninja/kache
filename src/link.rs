use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
#[cfg(windows)]
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

/// Process-global: restore via hardlink on a non-CoW Windows volume (NTFS)
/// instead of the default copy (#429). Set once from `config.windows_hardlink`
/// at wrapper entry; read on the Windows restore path. Off everywhere else.
static WINDOWS_HARDLINK_RESTORE: AtomicBool = AtomicBool::new(false);

/// Process-global: marker file used to dedup the no-CoW advisory across the
/// hundreds of wrapper processes a single build spawns (#508). Set at wrapper
/// entry; if unset the advisory falls back to once-per-process.
#[cfg(windows)]
static COW_WARN_MARKER: OnceLock<PathBuf> = OnceLock::new();

/// Set the Windows hardlink-restore opt-in (from `Config::windows_hardlink`).
/// Call once per process before restoring. No effect off Windows.
pub fn set_windows_hardlink_restore(enabled: bool) {
    WINDOWS_HARDLINK_RESTORE.store(enabled, Ordering::Relaxed);
}

/// Is the `[cache] windows_hardlink` opt-in active for this process?
/// The store's insert-side blob hardlinking keys off the same flag as
/// restore: without it, a hardlink would propagate the blob's read-only
/// attribute to the build's own output (shared MFT record, #429).
#[cfg(windows)]
pub(crate) fn windows_hardlink_enabled() -> bool {
    WINDOWS_HARDLINK_RESTORE.load(Ordering::Relaxed)
}

/// Set the cross-process dedup marker for the no-CoW advisory. Call once per
/// process before restoring. No effect off Windows.
#[cfg_attr(not(windows), allow(unused_variables))]
pub fn set_cow_warn_marker(path: std::path::PathBuf) {
    #[cfg(windows)]
    let _ = COW_WARN_MARKER.set(path);
}

/// Process-global: surface storage-*layout* advisories ("no CoW",
/// "cross-volume") on copy-restores (#551). On by default; set once from
/// `Config::storage_layout_advice` at wrapper entry. Muting this never mutes
/// fault reports (`UnexpectedOnCowVolume`) — advice and faults have different
/// severities and different audiences.
static STORAGE_LAYOUT_ADVICE: AtomicBool = AtomicBool::new(true);

/// Set the storage-layout advisory toggle (from `Config::storage_layout_advice`).
/// Call once per process before restoring.
pub fn set_storage_layout_advice(enabled: bool) {
    STORAGE_LAYOUT_ADVICE.store(enabled, Ordering::Relaxed);
}

/// Is `[cache] storage_layout_advice` active for this process?
#[cfg(windows)]
fn storage_layout_advice_enabled() -> bool {
    STORAGE_LAYOUT_ADVICE.load(Ordering::Relaxed)
}

/// Strategy for restoring a cached file to a build output path.
///
/// `Hardlink` and `Copy` first try reflink (CoW: zero-copy *with* an
/// independent inode), then use the strategy-specific fallback:
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
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LinkStrategy {
    Hardlink,
    Copy,
}

/// Link a cached file to the target output path.
///
/// Both strategies try reflink first, then use their strategy-specific fallback.
pub fn link_to_target(store_path: &Path, target_path: &Path, strategy: LinkStrategy) -> Result<()> {
    let do_link = || -> Result<()> {
        // Remove existing file at target (link/clone calls fail if dst exists).
        clear_target(target_path)?;

        // Logical size of the artifact, attributed to whichever restoration
        // mechanism runs below. Best-effort — a metadata failure here must
        // not fail the restore.
        let bytes = fs::metadata(store_path).map(|m| m.len()).unwrap_or(0);

        // Try reflink first. CoW gives us zero-copy *and* mutations don't
        // propagate to the cache blob — strictly better than hardlink when
        // available (APFS, btrfs, XFS-with-reflink).
        // Keep the failure reason: on Windows it separates "this volume can't
        // block-clone" from "this one file couldn't be cloned" (#508).
        let reflink_err = match try_reflink(store_path, target_path) {
            Ok(()) => {
                match strategy {
                    LinkStrategy::Hardlink => {}
                    // Reflink preserves source mode (read-only for stored
                    // blobs). Independent restores need consumer-facing
                    // permissions without discarding umask-shaped read bits.
                    LinkStrategy::Copy => set_executable_permissions(target_path)?,
                }
                tracing::debug!(
                    "reflinked {} -> {}",
                    store_path.display(),
                    target_path.display()
                );
                crate::opcounts::record_reflinked(bytes);
                return Ok(());
            }
            Err(e) => e,
        };
        #[cfg(not(windows))]
        let _ = &reflink_err;

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
                    copy_file(store_path, target_path, false)?;
                    warn_no_cow_restore_once(store_path, target_path, bytes, &reflink_err, true);
                    crate::opcounts::record_copied(bytes);
                    Ok(())
                }
            }
            #[cfg(not(windows))]
            LinkStrategy::Hardlink => hardlink_or_copy(store_path, target_path, bytes),
            LinkStrategy::Copy => {
                // Copying here is by design (executables/dylibs may be mutated after
                // the build), so no storage-layout advice — but a large artifact that
                // failed to block-clone on a CoW volume is still a real fault and is
                // reported rather than swallowed.
                copy_file(store_path, target_path, true)?;
                #[cfg(windows)]
                warn_no_cow_restore_once(store_path, target_path, bytes, &reflink_err, false);
                crate::opcounts::record_copied(bytes);
                Ok(())
            }
        }
    };

    let mut err = match do_link() {
        Ok(()) => return Ok(()),
        Err(e) => e,
    };

    if err.chain().any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    }) && let Some(parent) = target_path.parent()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent dir for {}", target_path.display()))?;
        if let Err(e) = do_link() {
            err = e;
        } else {
            return Ok(());
        }
    }

    Err(err)
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

/// Why a Windows cache hit was restored by COPY instead of a block-clone.
///
/// A failed `FSCTL_DUPLICATE_EXTENTS_TO_FILE` does NOT imply the volume lacks
/// copy-on-write, and conflating the two is what made kache cry wolf at every
/// Dev Drive user (#508). The three causes need three different responses:
///
/// - **`CrossVolume`**: cache blob and build output are on different volumes.
///   Block-cloning cannot span volumes (nor can hardlinks), so even two ReFS Dev
///   Drives copy. Fix: co-locate cache + build on one volume — not a filesystem
///   problem, so this is worth saying (#490).
/// - **`NoCow`**: the volume genuinely has no block-cloning (NTFS). Fix: move
///   both onto a ReFS Dev Drive, or opt into `[cache] windows_hardlink`. Worth
///   saying — this is the case that really does double disk usage.
/// - **`SubClusterOnCowVolume`**: the volume DOES block-clone, and this file is
///   smaller than one cluster, so it has no cluster-aligned range (ReFS clones
///   whole clusters; cloning past EOF is undefined). Every `.d` under ~4 KB hits
///   this on a healthy Dev Drive: all 670 warnings in #508 were sub-cluster `.d`
///   files while the `.rlib`/`.rmeta` beside them block-cloned fine. Saying
///   nothing is right — the volume is healthy and there is no advice to give.
/// - **`UnknownCow`**: the capability probe itself failed, so we do NOT know
///   whether this volume block-clones. Still worth saying — a copy-restore is
///   happening either way — but say it *honestly*: asserting "no copy-on-write"
///   from a failed probe would repeat the very mistake #508 is about.
/// - **`SubClusterOnCowVolume`**: the volume DOES block-clone, and this file is
///   smaller than one cluster, so it has no cluster-aligned range (ReFS clones
///   whole clusters; cloning past EOF is undefined). Every `.d` under ~4 KB hits
///   this on a healthy Dev Drive: all 670 warnings in #508 were sub-cluster `.d`
///   files while the `.rlib`/`.rmeta` beside them block-cloned fine. Saying
///   nothing is right — the volume is healthy and there is no advice to give.
/// - **`UnexpectedOnCowVolume`**: the volume block-clones and the file was big
///   enough to clone, yet the clone still failed. That is NOT benign — a filter
///   driver, an integrity-stream mismatch, or a kache bug could silently demote
///   every large artifact to a copy. Surface it (with the OS error) rather than
///   hiding it behind the #508 fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(windows), allow(dead_code))]
enum CopyRestoreCause {
    CrossVolume,
    NoCow,
    UnknownCow,
    SubClusterOnCowVolume,
    UnexpectedOnCowVolume,
}

impl CopyRestoreCause {
    /// Which dedup bucket this advisory belongs to.
    ///
    /// A storage-*layout* advisory ("no CoW", "cross-volume") must NOT be able
    /// to mute a *fault* ("the clone failed unexpectedly") — they have different
    /// severities and different audiences, so they get separate markers. Sharing
    /// one bucket would let a benign cross-volume note swallow the report of a
    /// filter driver silently demoting every large artifact to a copy.
    #[cfg_attr(not(windows), allow(dead_code))]
    fn warn_bucket(self) -> &'static str {
        match self {
            CopyRestoreCause::UnexpectedOnCowVolume => "cow-fault",
            _ => "cow",
        }
    }

    /// Is this cause storage-layout *advice* (mutable via the `Copy` strategy
    /// or `[cache] storage_layout_advice = false`, #551) rather than a fault
    /// report? Exhaustive on purpose: a future variant must decide explicitly
    /// which side it is on — a fault must never be muteable by an advice knob.
    #[cfg_attr(not(windows), allow(dead_code))]
    fn is_layout_advisory(self) -> bool {
        match self {
            CopyRestoreCause::CrossVolume
            | CopyRestoreCause::NoCow
            | CopyRestoreCause::UnknownCow => true,
            CopyRestoreCause::SubClusterOnCowVolume | CopyRestoreCause::UnexpectedOnCowVolume => {
                false
            }
        }
    }
}

/// Classify a copy-restore from the two volume roots, the build volume's
/// block-cloning capability, and whether the file was too small to clone. Pure —
/// kept platform-independent so the decision table is unit-testable off Windows.
///
/// Unknowns are never resolved *toward silence*: a failed capability probe warns
/// as `UnknownCow` (hedged) rather than asserting NTFS, and an unknown size
/// resolves to "unexpected" rather than assuming the benign sub-cluster case.
#[cfg_attr(not(windows), allow(dead_code))]
fn classify_copy_restore(
    cache_vol: Option<&str>,
    build_vol: Option<&str>,
    build_vol_has_cow: Option<bool>,
    file_is_sub_cluster: Option<bool>,
) -> CopyRestoreCause {
    // If we can't resolve a volume for either side, don't assert cross-volume.
    if let (Some(cache), Some(build)) = (cache_vol, build_vol)
        && !cache.eq_ignore_ascii_case(build)
    {
        return CopyRestoreCause::CrossVolume;
    }
    match build_vol_has_cow {
        // Same volume, and it block-clones — the other files in this same build
        // reflink fine. Expected only when the file had no clonable range.
        Some(true) => {
            if file_is_sub_cluster == Some(true) {
                CopyRestoreCause::SubClusterOnCowVolume
            } else {
                CopyRestoreCause::UnexpectedOnCowVolume
            }
        }
        Some(false) => CopyRestoreCause::NoCow,
        None => CopyRestoreCause::UnknownCow,
    }
}

/// Tell the user their cache hits are being restored by COPY — but only when
/// that is actually a problem, and at most once per warn-session window per
/// [bucket](CopyRestoreCause::warn_bucket) across all wrapper processes (#508).
///
/// Both gates matter: a sub-cluster file on a working Dev Drive says nothing at
/// all, and the real advisories dedup through a marker file rather than a
/// per-process `Once` — each rustc is its own process, which is why a single
/// build used to emit the same warning hundreds of times.
///
/// `bytes` is the blob's size and `reflink_err` the clone failure, so a file
/// that was big enough to clone but failed anyway can still be surfaced instead
/// of being swallowed along with the benign sub-cluster case.
///
/// `layout_advice` gates the *configuration* advisories ("no CoW", "cross
/// volume"). The `Copy` strategy (executables, dylibs) always intended to copy,
/// so it passes `false`: it must not start nagging about storage layout. But it
/// still routes through here, because a LARGE artifact that fails to clone on a
/// CoW volume is a genuine fault that must not pass silently just because the
/// restore strategy happened to be `Copy`.
///
/// `[cache] storage_layout_advice = false` (#551) mutes the same advisories
/// process-wide — the user saying their layout is intentional (e.g. an
/// NTFS-only machine that can never host a ReFS Dev Drive). Like the `Copy`
/// strategy, it can never mute a fault.
#[cfg(windows)]
fn warn_no_cow_restore_once(
    store_path: &Path,
    target_path: &Path,
    bytes: u64,
    reflink_err: &anyhow::Error,
    layout_advice: bool,
) {
    let cache_vol = windows_volume_root(store_path);
    let build_vol = windows_volume_root(target_path);
    // A file with no cluster-aligned range can't be block-cloned on ANY volume.
    // If the cluster size can't be read, leave it unknown rather than guessing.
    let sub_cluster = windows_cluster_size(target_path)
        .ok()
        .map(|cluster| bytes > 0 && bytes < cluster);
    let cause = classify_copy_restore(
        cache_vol.as_deref(),
        build_vol.as_deref(),
        windows_volume_supports_block_clone(target_path),
        sub_cluster,
    );

    let message = match cause {
        CopyRestoreCause::SubClusterOnCowVolume => {
            // Not a filesystem problem: the volume block-clones, this file is
            // just smaller than a cluster. Debug-log it and stay quiet.
            tracing::debug!(
                "copy-restored {} — {} bytes is smaller than one cluster, so it has \
                 no cluster-aligned range to block-clone; volume {} does support \
                 copy-on-write",
                target_path.display(),
                bytes,
                build_vol.as_deref().unwrap_or("?"),
            );
            return;
        }
        CopyRestoreCause::UnexpectedOnCowVolume => format!(
            "kache: this volume ({vol}) supports copy-on-write and this file is \
             large enough to block-clone, but the clone FAILED — so the cache hit \
             was restored by COPY and does not share storage blocks with the cache. This \
             is unexpected; please report it with the error below.\n         \
             error:           {err:#}\n         affected output: {target} \
             ({bytes} bytes)",
            vol = build_vol.as_deref().unwrap_or("?"),
            err = reflink_err,
            target = target_path.display(),
            bytes = bytes,
        ),
        CopyRestoreCause::CrossVolume => format!(
            "kache: cache hits are restored by COPY because the cache and build \
             tree are on different volumes ({cache_vol} vs {build_vol}), and \
             copy-on-write block-cloning cannot span volumes — so they do not \
             share storage blocks, roughly doubling disk for cached content. Put the \
             cache and build dir on the SAME volume (ideally a ReFS Dev Drive) \
             for zero-copy dedup. If this layout is intentional, silence this \
             advice with `[cache] storage_layout_advice = false` (clone faults \
             are still reported).\n         cache blob:   {store}\n         \
             build output: {target}",
            cache_vol = cache_vol.as_deref().unwrap_or("?"),
            build_vol = build_vol.as_deref().unwrap_or("?"),
            store = store_path.display(),
            target = target_path.display(),
        ),
        CopyRestoreCause::NoCow => format!(
            "kache: this volume ({vol}) has no copy-on-write, so cache hits are \
             restored by COPY — the cache and build tree do not share storage blocks, \
             roughly doubling disk for cached content. For zero-copy dedup put \
             the cache + build dir on a ReFS Dev Drive, or set \
             `[cache] windows_hardlink = true` (only if your build never \
             deletes or rewrites an object output). If this layout is \
             intentional, silence this advice with \
             `[cache] storage_layout_advice = false` (clone faults are still \
             reported).\n         affected output: {target}",
            vol = build_vol.as_deref().unwrap_or("NTFS"),
            target = target_path.display(),
        ),
        // Probe failed: we do NOT know what this volume supports. Say exactly
        // that — claiming "no copy-on-write" here would be the same unfounded
        // assertion that produced #508 in the first place.
        CopyRestoreCause::UnknownCow => format!(
            "kache: cache hits are restored by COPY, so the cache and build tree \
             do not share storage blocks. kache could not determine whether this volume \
             ({vol}) supports copy-on-write — the capability probe failed. For \
             zero-copy dedup the cache + build dir must be on the same ReFS Dev \
             Drive. If this layout is intentional, silence this advice with \
             `[cache] storage_layout_advice = false` (clone faults are still \
             reported).\n         probe error:     {err:#}\n         affected output: \
             {target}",
            vol = build_vol.as_deref().unwrap_or("?"),
            err = reflink_err,
            target = target_path.display(),
        ),
    };

    // The Copy strategy always meant to copy — it must not nag about storage
    // layout — and `[cache] storage_layout_advice = false` is the user saying
    // their layout is intentional (#551). Either way this mutes only the
    // *advice*: a genuine clone fault still reports (see `layout_advice`).
    if cause.is_layout_advisory() && (!layout_advice || !storage_layout_advice_enabled()) {
        tracing::debug!(
            "copy-restored {} ({:?}; layout advice muted: strategy={}, config={})",
            target_path.display(),
            cause,
            !layout_advice,
            !storage_layout_advice_enabled(),
        );
        return;
    }

    match COW_WARN_MARKER.get() {
        Some(base) => {
            // Separate bucket per severity: a layout advisory must never mute a
            // fault report (and vice versa).
            let marker = bucket_marker(base, cause.warn_bucket());
            let _warned = crate::wrapper::warn_once_per_session(
                &marker,
                crate::wrapper::WARN_SESSION_SECS,
                &message,
            );
        }
        // No marker configured (unit tests, non-wrapper entrypoints): fall back
        // to once-per-process rather than going silent.
        None => {
            use std::sync::Once;
            static WARNED: Once = Once::new();
            WARNED.call_once(|| eprintln!("{message}"));
        }
    }
}

/// Derive a per-bucket marker path from the base marker (`…/kache-cow-warn-<hash>`
/// → `…/kache-cow-warn-<hash>.<bucket>`), so advisories of different severity
/// dedup independently.
#[cfg_attr(not(windows), allow(dead_code))]
fn bucket_marker(base: &Path, bucket: &str) -> std::path::PathBuf {
    let mut name = base.as_os_str().to_os_string();
    name.push(".");
    name.push(bucket);
    std::path::PathBuf::from(name)
}

/// Does the volume holding `path` support ReFS block-cloning (copy-on-write)?
/// `None` if the capability can't be determined.
///
/// This is the question `warn_no_cow_restore_once` actually needs answered —
/// asking the volume directly, rather than inferring "no CoW" from one failed
/// clone of one file (#508).
#[cfg(windows)]
fn windows_volume_supports_block_clone(path: &Path) -> Option<bool> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationW;
    use windows_sys::Win32::System::SystemServices::FILE_SUPPORTS_BLOCK_REFCOUNTING;

    let root = windows_volume_root(path)?;
    let wide: Vec<u16> = std::ffi::OsStr::new(&root)
        .encode_wide()
        .chain(Some(0))
        .collect();
    let mut flags: u32 = 0;
    let ok = unsafe {
        GetVolumeInformationW(
            wide.as_ptr(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut flags,
            std::ptr::null_mut(),
            0,
        )
    };
    if ok == 0 {
        return None;
    }
    Some(flags & FILE_SUPPORTS_BLOCK_REFCOUNTING != 0)
}

/// Volume mount root (e.g. `C:\` or `D:\`) that holds `path`, or `None` if it
/// can't be resolved. `path` need not exist; its nearest existing parent volume
/// is used. Used to tell a cross-volume copy-restore from a no-CoW one (#490).
#[cfg(windows)]
fn windows_volume_root(path: &Path) -> Option<String> {
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

/// Set 0o755 after an executable/dylib reflink. Reflink preserves the store
/// blob's read-only mode, but runtime-loaded artifacts must be executable.
fn set_executable_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))
            .with_context(|| format!("setting executable permissions on {}", path.display()))?;
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
        return Ok(()); // empty file: dst already created empty, nothing to clone
    }

    // Cluster size of the destination volume; clone ranges must be aligned to
    // it. A file smaller than one cluster has no aligned range at all, so bail
    // before the FSCTL rather than issuing a call that cannot succeed — this is
    // the common case for the many small `.d` files restored here (#508).
    let cluster = windows_cluster_size(dst)?;
    if len < cluster {
        anyhow::bail!("file smaller than one cluster; fall back to copy");
    }

    let clone_len = (len / cluster) * cluster;

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

/// A fully-written C/C++ cache artifact awaiting no-clobber publication.
///
/// The staging file is created in the target directory with the same requested
/// mode as a compiler output (`0666`). The kernel therefore applies the current
/// umask and any inherited default ACL exactly where the final file will live.
/// No metadata operation is performed through the final pathname.
pub(crate) struct PreparedWritableTarget {
    staged: tempfile::NamedTempFile,
    target: PathBuf,
    bytes: u64,
}

impl PreparedWritableTarget {
    pub(crate) fn target(&self) -> &Path {
        &self.target
    }

    pub(crate) fn publish(self) -> Result<()> {
        let target = self.target;
        self.staged
            .persist_noclobber(&target)
            .map_err(|error| error.error)
            .with_context(|| {
                format!(
                    "publishing cc output without replacing {}",
                    target.display()
                )
            })?;
        crate::opcounts::record_copied(self.bytes);
        Ok(())
    }
}

fn new_writable_staging_file(target: &Path) -> Result<tempfile::NamedTempFile> {
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut builder = tempfile::Builder::new();
    builder.prefix(".kache-cc-restore-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o666));
    }
    builder
        .tempfile_in(parent)
        .with_context(|| format!("creating cc restore staging file in {}", parent.display()))
}

/// Prepare cached file bytes without touching an existing target entry.
pub(crate) fn prepare_writable_target_from_file(
    src: &Path,
    target: &Path,
) -> Result<PreparedWritableTarget> {
    let mut source = fs::File::open(src)
        .with_context(|| format!("opening cached cc artifact {}", src.display()))?;
    let mut staged = new_writable_staging_file(target)?;
    let bytes = std::io::copy(&mut source, &mut staged).with_context(|| {
        format!(
            "copying cached cc artifact {} for {}",
            src.display(),
            target.display()
        )
    })?;
    Ok(PreparedWritableTarget {
        staged,
        target: target.to_path_buf(),
        bytes,
    })
}

/// Prepare transformed C/C++ bytes with the same absent-only guarantee.
pub(crate) fn prepare_writable_target_from_bytes(
    target: &Path,
    content: &[u8],
) -> Result<PreparedWritableTarget> {
    use std::io::Write;

    let mut staged = new_writable_staging_file(target)?;
    staged
        .write_all(content)
        .with_context(|| format!("writing staged cc output for {}", target.display()))?;
    Ok(PreparedWritableTarget {
        staged,
        target: target.to_path_buf(),
        bytes: content.len() as u64,
    })
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
    let do_write = || -> Result<()> {
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
        let _ = strategy;
        Ok(())
    };

    let mut err = match do_write() {
        Ok(()) => return Ok(()),
        Err(e) => e,
    };

    if err.chain().any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
    }) && let Some(parent) = target_path.parent()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating parent dir for {}", target_path.display()))?;
        if let Err(e) = do_write() {
            err = e;
        } else {
            return Ok(());
        }
    }

    Err(err)
}

/// Stamp a restored file's mtime as "written now", through the same clock
/// the filesystem stamps writes with: `futimens(UTIME_NOW)` on unix, a
/// sibling write-clock sample elsewhere — never a bare `FileTime::now()`.
///
/// The stamp itself is required: cargo re-runs build scripts in a cleaned
/// tree, and its `StaleDependency` freshness rule compares those fresh run
/// outputs against our restored artifacts' mtimes — a restored file older
/// than its unit's build-script `output` is permanently dirty. So restored
/// files must read as "written now".
///
/// The clock choice is the subtle part (kunobi-ninja/kache#677, the #135
/// "flake"): `filetime::FileTime::now()` samples the precise realtime clock,
/// which on Linux runs AHEAD of the coarse clock the kernel stamps file
/// writes with (by up to a tick — the observed inversions are
/// sub-millisecond). A precise-clock touch can therefore postdate files the
/// build tool writes *after* the restore (a build script's `output`), which
/// cargo reads as `StaleDependency` — identical back-to-back builds never
/// reach a no-op. On Linux, `UTIME_NOW` resolves in the kernel through the
/// same timestamp source as writes (fs/utimes.c -> inode current-time), so
/// the restored file's mtime is >= everything written before it and <=
/// everything written after it — exactly the ordering cargo needs. That is a
/// verified kernel property on local Linux filesystems only: macOS resolves
/// `UTIME_NOW` in libsyscall via `gettimeofday` (microseconds, empirically
/// green in the e2e suite on APFS but not the same mechanism), and network /
/// FUSE filesystems make no such guarantee.
pub fn touch_mtime_write_clock(path: &Path) -> Result<()> {
    // Any failure below propagates to `restore_from_cache`'s callers, which
    // treat it as a clean miss and recompile (never a failed build) — so
    // fail-closed checks here cost a recompile, not correctness.
    #[cfg(unix)]
    {
        use std::os::fd::{AsRawFd, FromRawFd};
        use std::os::unix::ffi::OsStrExt;

        // Stamp through an fd rather than the path (kunobi-ninja/kache#682):
        // `O_NOFOLLOW` fails closed if the artifact was swapped for a symlink
        // between materialization and stamp, and `fstat` proves the object is
        // still a regular file before it is re-dated. `O_RDONLY` is
        // deliberate — restored hardlinks may be read-only (0444), and
        // `futimens`'s permission check for an OMIT/NOW pair is against the
        // inode (owner or privileged), not the fd's open mode.
        let cpath = std::ffi::CString::new(path.as_os_str().as_bytes())
            .with_context(|| format!("path contains NUL: {}", path.display()))?;
        // O_NONBLOCK: a swapped-in FIFO would otherwise block `open`
        // forever; for a regular file the flag is a no-op.
        let raw = unsafe {
            libc::open(
                cpath.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            )
        };
        if raw < 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("opening {} to touch it", path.display()));
        }
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };

        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("stat of {} before touch", path.display()));
        }
        if st.st_mode & libc::S_IFMT != libc::S_IFREG {
            anyhow::bail!(
                "refusing to touch {}: not a regular file (mode {:o})",
                path.display(),
                st.st_mode
            );
        }

        // atime is left untouched (UTIME_OMIT); mtime gets the write clock.
        let times = [
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_OMIT,
            },
            libc::timespec {
                tv_sec: 0,
                tv_nsec: libc::UTIME_NOW,
            },
        ];
        if unsafe { libc::futimens(fd.as_raw_fd(), times.as_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("updating mtime of {}", path.display()));
        }
    }

    // Non-unix targets have no UTIME_NOW, so sample the filesystem's write
    // clock directly: write a sibling temp file in the same directory (same
    // volume — timestamp behavior is per-filesystem), read back the mtime it
    // was stamped with, and apply that value to the target. Sampling BEFORE
    // stamping is the safe direction: the stamp can only sit slightly in the
    // past relative to later writes, never ahead of them — the #677/#681
    // inversion was the precise clock stamping AHEAD of write timestamps
    // (measured ~1.8 ms on NTFS by the first Windows CI run of the ordering
    // test below). A sampling failure PROPAGATES: falling back to
    // `FileTime::now()` would silently reintroduce the known-bad precise
    // stamp, whereas an error here degrades to a clean recompile at the
    // restore call sites.
    #[cfg(not(unix))]
    {
        let now = sample_write_clock(path)
            .with_context(|| format!("sampling write clock next to {}", path.display()))?;

        // On Windows, hardlinked files share permissions with the store
        // blob, which is read-only. Temporarily make it writable to update
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
    }

    Ok(())
}

/// Sample the write clock of the filesystem holding `path`: create a sibling
/// temp file (exclusively — never truncating something that already exists),
/// write to it, close it, and read the mtime the filesystem gave that write.
/// The close between write and stat matters on Windows, where the last-write
/// time of an open handle may not be visible to a path-based stat until the
/// handle closes. Returns an error on failure (e.g. the directory is not
/// writable); the caller propagates it, degrading to a recompile.
#[cfg(not(unix))]
fn sample_write_clock(path: &Path) -> Result<filetime::FileTime> {
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
            Ok::<_, std::io::Error>(filetime::FileTime::from_last_modification_time(&meta))
        })();
        let _ = fs::remove_file(&probe);
        return sampled.with_context(|| format!("write-clock probe {}", probe.display()));
    }
    Err(last_err
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow::anyhow!("no free probe name after 8 attempts")))
    .with_context(|| format!("creating write-clock probe in {}", dir.display()))
}

const DEPINFO_ROOT_SENTINEL: &str = "__kache_root__/";
const DEPINFO_CWD_SENTINEL: &str = "__kache_cwd__/";
const DEPINFO_WORKSPACE_SENTINEL: &str = "__kache_workspace__/";

/// rustc's `# env-dep:NAME=value` records carry their own escape grammar:
/// backslash is written `\\` (and newline/CR as `\n`/`\r`), and cargo's
/// parser hard-rejects every other backslash sequence with
/// "unknown escape character". A path prefix can only contribute the
/// backslash case.
const DEPINFO_ENV_DEP_PREFIX: &str = "# env-dep:";

fn escape_depinfo_env_value(prefix: &str) -> String {
    prefix.replace('\\', "\\\\")
}

fn escape_depinfo_make_path(prefix: &str) -> String {
    prefix.replace(' ', "\\ ")
}

/// Pure dep-info path rewrite: relativize absolute project paths to a
/// kache-only sentinel, or expand that sentinel back to absolute paths.
/// No I/O.
///
/// This is the in-memory half of the transform. The restore side calls
/// it directly — it computes the final `.d` content from the store blob
/// and materializes the result with [`write_restored`], so it never
/// rewrites a restored, possibly read-only, possibly inode-shared file in
/// place. The Rust store side calls the two-root variant while creating a
/// private store snapshot; C/C++ keeps using this one-root form.
pub fn rewrite_depinfo_content(content: &str, project_dir: &Path, mode: DepInfoMode) -> String {
    rewrite_depinfo_content_with_sentinel(content, project_dir, DEPINFO_ROOT_SENTINEL, mode)
}

/// Rust dep-info carries two independently relocatable path families: target
/// outputs and sources below Cargo's invocation directory. Re-root both with
/// distinct sentinels so a cached `.d` never leaves Cargo watching a live donor
/// worktree after the artifact moves (#760).
#[cfg(test)]
pub fn rewrite_rustc_depinfo_content(
    content: &str,
    target_dir: &Path,
    working_dir: &Path,
    workspace_dir: Option<&Path>,
    mode: DepInfoMode,
) -> String {
    rewrite_rustc_depinfo_content_with_configured_roots(
        content,
        target_dir,
        working_dir,
        workspace_dir,
        &[],
        mode,
    )
}

pub fn rewrite_rustc_depinfo_content_with_configured_roots(
    content: &str,
    target_dir: &Path,
    working_dir: &Path,
    workspace_dir: Option<&Path>,
    configured_roots: &[(PathBuf, String, u8)],
    mode: DepInfoMode,
) -> String {
    let mut automatic_roots = vec![(
        working_dir.to_path_buf(),
        DEPINFO_CWD_SENTINEL.to_string(),
        4_u8,
        2_u8,
    )];
    if let Some(workspace_dir) = workspace_dir {
        automatic_roots.push((
            workspace_dir.to_path_buf(),
            DEPINFO_WORKSPACE_SENTINEL.to_string(),
            4_u8,
            1_u8,
        ));
    }

    if matches!(mode, DepInfoMode::Relativize) {
        let mut roots = configured_roots
            .iter()
            .map(|(root, sentinel, priority)| {
                (root.clone(), sentinel.clone(), *priority, 0_u8, true)
            })
            .chain(
                automatic_roots
                    .iter()
                    .map(|(root, sentinel, priority, tie)| {
                        (root.clone(), sentinel.clone(), *priority, *tie, false)
                    }),
            )
            .collect::<Vec<_>>();
        roots.sort_by_key(|(root, _, priority, tie, exact)| {
            std::cmp::Reverse((
                *priority,
                u8::from(*exact),
                root.components().count(),
                root.as_os_str().len(),
                *tie,
            ))
        });
        let target_rewritten = rewrite_rustc_depinfo_targets(content, target_dir, mode);
        return roots.into_iter().fold(
            target_rewritten,
            |rewritten, (root, sentinel, _, _, exact)| {
                if exact {
                    rewrite_depinfo_content_with_exact_sentinel(&rewritten, &root, &sentinel, mode)
                } else {
                    rewrite_depinfo_content_with_sentinel(&rewritten, &root, &sentinel, mode)
                }
            },
        );
    }

    // A portable owner may have multiple producer aliases, but the first entry
    // for its sentinel is the designated lexical consumer root. Expand each
    // sentinel exactly once so a longer canonical alias cannot steal restore.
    let mut rewritten = content.to_string();
    let mut expanded = std::collections::HashSet::new();
    for (root, sentinel, _) in configured_roots {
        if expanded.insert(sentinel) {
            rewritten =
                rewrite_depinfo_content_with_exact_sentinel(&rewritten, root, sentinel, mode);
        }
    }
    let rewritten =
        automatic_roots
            .into_iter()
            .fold(rewritten, |rewritten, (root, sentinel, _, _)| {
                rewrite_depinfo_content_with_sentinel(&rewritten, &root, &sentinel, mode)
            });
    rewrite_rustc_depinfo_targets(&rewritten, target_dir, mode)
}

/// Rebase only Makefile target tokens (left of rustc's `: ` delimiter).
/// Dependencies on the right are source inputs and must follow PathNormalizer
/// ownership instead; a generated source beneath `target/` can still belong to
/// workspace/configured source identity.
fn rewrite_rustc_depinfo_targets(content: &str, target_dir: &Path, mode: DepInfoMode) -> String {
    content
        .split_inclusive('\n')
        .map(|line| {
            if line.starts_with(DEPINFO_ENV_DEP_PREFIX) {
                return line.to_string();
            }
            let Some(delimiter) = line.find(": ") else {
                return line.to_string();
            };
            let (targets, dependencies) = line.split_at(delimiter);
            let rewritten = rewrite_depinfo_content_with_sentinel(
                targets,
                target_dir,
                DEPINFO_ROOT_SENTINEL,
                mode,
            );
            format!("{rewritten}{dependencies}")
        })
        .collect()
}

/// Rewrite one exact configured-rule spelling. Configured roots already carry
/// their explicitly constructed lexical/canonical/Windows aliases; adding a
/// fresh canonical alias here could let one configured symlink steal another
/// configured slot's sentinel.
fn rewrite_depinfo_content_with_exact_sentinel(
    content: &str,
    project_dir: &Path,
    sentinel: &str,
    mode: DepInfoMode,
) -> String {
    rewrite_depinfo_content_with_exact_sentinel_for_platform(
        content,
        project_dir,
        sentinel,
        mode,
        cfg!(windows),
    )
}

fn rewrite_depinfo_content_with_exact_sentinel_for_platform(
    content: &str,
    project_dir: &Path,
    sentinel: &str,
    mode: DepInfoMode,
    windows: bool,
) -> String {
    let prefixes = depinfo_prefixes_for_exact_display(&project_dir.to_string_lossy(), windows);
    rewrite_depinfo_content_with_prefixes_and_sentinel(content, &prefixes, sentinel, mode)
}

fn depinfo_prefixes_for_exact_display(raw: &str, windows: bool) -> Vec<String> {
    if windows {
        // Expand uses the first spelling. Verbatim Windows paths (`\\?\...`)
        // do not accept forward slashes, so the native separator must lead.
        vec![format!("{raw}\\"), format!("{raw}/")]
    } else {
        vec![format!("{raw}/")]
    }
}

fn rewrite_depinfo_content_with_sentinel(
    content: &str,
    project_dir: &Path,
    sentinel: &str,
    mode: DepInfoMode,
) -> String {
    let mut roots = vec![project_dir.to_path_buf()];
    if matches!(mode, DepInfoMode::Relativize)
        && let Ok(canonical) = project_dir.canonicalize()
        && canonical != project_dir
    {
        roots.push(canonical);
    }
    let mut prefixes = Vec::new();
    for root in roots {
        prefixes.extend(depinfo_prefixes_for_display(
            &root.to_string_lossy(),
            matches!(mode, DepInfoMode::Relativize),
            cfg!(windows),
        ));
    }
    prefixes.sort();
    prefixes.dedup();
    rewrite_depinfo_content_with_prefixes_and_sentinel(content, &prefixes, sentinel, mode)
}

fn depinfo_prefixes_for_display(
    raw: &str,
    retain_verbatim_alias: bool,
    windows: bool,
) -> Vec<String> {
    let stripped = raw
        .strip_prefix(r"\\?\UNC\")
        .map(|path| format!(r"\\{path}"))
        .or_else(|| raw.strip_prefix(r"\\?\").map(str::to_string))
        .unwrap_or_else(|| raw.to_string());
    let mut displays = vec![stripped.clone()];
    if retain_verbatim_alias && raw != stripped {
        displays.push(raw.to_string());
    }
    let mut prefixes = Vec::new();
    for display in displays {
        prefixes.push(format!("{display}/"));
        if windows {
            prefixes.push(format!("{display}\\"));
        }
    }
    prefixes
}

/// Prefix-parameterized core of the Windows [`rewrite_depinfo_content`]
/// branch, split out so the dual-spelling behavior is unit-testable on every
/// host.
///
/// `# env-dep:NAME=value` values are transformed through rustc's env-dep
/// escape grammar: a backslash-bearing prefix appears there as `\\`, so
/// Relativize matches the escaped spelling (never splitting an escape pair
/// in two) and Expand inserts the escaped spelling (never creating an escape
/// cargo rejects, kunobi-ninja/kache#730). The record's key is an environment
/// variable NAME, never a path — it is left untouched, as is a key-only
/// record without `=`.
#[cfg(test)]
fn rewrite_depinfo_content_with_prefixes(
    content: &str,
    prefixes: &[String],
    mode: DepInfoMode,
) -> String {
    rewrite_depinfo_content_with_prefixes_and_sentinel(
        content,
        prefixes,
        DEPINFO_ROOT_SENTINEL,
        mode,
    )
}

fn rewrite_depinfo_content_with_prefixes_and_sentinel(
    content: &str,
    prefixes: &[String],
    sentinel: &str,
    mode: DepInfoMode,
) -> String {
    content
        .split_inclusive('\n')
        .map(|line| {
            if let Some(record) = line.strip_prefix(DEPINFO_ENV_DEP_PREFIX) {
                let Some((key, value)) = record.split_once('=') else {
                    return line.to_string();
                };
                let value = match mode {
                    DepInfoMode::Relativize => prefixes.iter().fold(value.to_string(), |acc, p| {
                        replace_depinfo_text(
                            &acc,
                            &escape_depinfo_env_value(p),
                            sentinel,
                            cfg!(windows),
                            DepInfoTextContext::EnvValue,
                        )
                    }),
                    DepInfoMode::Expand => replace_depinfo_text(
                        value,
                        sentinel,
                        &escape_depinfo_env_value(&prefixes[0]),
                        false,
                        DepInfoTextContext::EnvValue,
                    ),
                };
                format!("{DEPINFO_ENV_DEP_PREFIX}{key}={value}")
            } else {
                match mode {
                    DepInfoMode::Relativize => prefixes.iter().fold(line.to_string(), |acc, p| {
                        let escaped = escape_depinfo_make_path(p);
                        let escaped = replace_depinfo_text(
                            &acc,
                            &escaped,
                            sentinel,
                            cfg!(windows),
                            DepInfoTextContext::MakeLine,
                        );
                        replace_depinfo_text(
                            &escaped,
                            p,
                            sentinel,
                            cfg!(windows),
                            DepInfoTextContext::MakeLine,
                        )
                    }),
                    DepInfoMode::Expand => replace_depinfo_text(
                        line,
                        sentinel,
                        &escape_depinfo_make_path(&prefixes[0]),
                        false,
                        DepInfoTextContext::MakeLine,
                    ),
                }
            }
        })
        .collect()
}

#[derive(Clone, Copy)]
enum DepInfoTextContext {
    MakeLine,
    EnvValue,
}

fn replace_depinfo_text(
    input: &str,
    needle: &str,
    replacement: &str,
    ascii_case_insensitive: bool,
    context: DepInfoTextContext,
) -> String {
    if needle.is_empty() {
        return input.to_string();
    }
    let input_bytes = input.as_bytes();
    let needle_bytes = needle.as_bytes();
    let mut output = String::with_capacity(input.len());
    let mut copied = 0;
    let mut search = 0;
    while search + needle_bytes.len() <= input_bytes.len() {
        let offset = if ascii_case_insensitive {
            input_bytes[search..]
                .windows(needle_bytes.len())
                .position(|window| window.eq_ignore_ascii_case(needle_bytes))
        } else {
            input[search..].find(needle)
        };
        let Some(offset) = offset else {
            break;
        };
        let found = search + offset;
        let end = found + needle_bytes.len();
        // UTF-8 is self-synchronizing: a valid UTF-8 `needle` cannot match
        // across code-point boundaries. `str::find` guarantees this directly;
        // the ASCII-insensitive byte scan changes only ASCII case.
        if !depinfo_path_prefix_has_left_boundary(input, found, context) {
            search = input[found..]
                .char_indices()
                .nth(1)
                .map_or(input.len(), |(next, _)| found + next);
            continue;
        }
        output.push_str(&input[copied..found]);
        output.push_str(replacement);
        copied = end;
        search = copied;
    }
    output.push_str(&input[copied..]);
    output
}

fn depinfo_path_prefix_has_left_boundary(
    input: &str,
    start: usize,
    context: DepInfoTextContext,
) -> bool {
    if start == 0 {
        return true;
    }
    if matches!(context, DepInfoTextContext::EnvValue) {
        return false;
    }
    let before = &input[..start];
    let Some((delimiter_start, delimiter)) = before.char_indices().next_back() else {
        return false;
    };
    if !delimiter.is_whitespace() && delimiter != ':' {
        return false;
    }
    before[..delimiter_start]
        .chars()
        .rev()
        .take_while(|ch| *ch == '\\')
        .count()
        .is_multiple_of(2)
}

/// Rewrite a `.d` (dep-info) file in place.
///
/// Used on the **store** side, where the file is the build's own
/// freshly-written, writable dep-info. The restore side does NOT use this
/// — it computes the rewritten content in memory via
/// [`rewrite_depinfo_content`] (or its Rust two-root variant), then materializes
/// it with [`write_restored`]. This honors "compute the final bytes, then
/// materialize" rather than patching a restored file in place.
#[cfg(test)]
pub fn rewrite_depinfo(depinfo_path: &Path, project_dir: &Path, mode: DepInfoMode) -> Result<()> {
    let content = fs::read_to_string(depinfo_path)
        .with_context(|| format!("reading dep-info file {}", depinfo_path.display()))?;

    let rewritten = rewrite_depinfo_content(&content, project_dir, mode);
    write_rewritten_depinfo(depinfo_path, rewritten)
}

#[cfg(test)]
fn write_rewritten_depinfo(depinfo_path: &Path, rewritten: String) -> Result<()> {
    // Defense-in-depth: if the file is hardlinked (nlink > 1), unlink
    // first so the in-place write can't mutate a shared inode. On the
    // store side `Store::put` never hardlinks `.d` blobs (DepInfo is
    // excluded from insert-side hardlinking precisely because of this
    // post-put in-place rewrite), so this rarely fires — but it keeps
    // `rewrite_depinfo` safe for any caller. Windows exposes no portable
    // nlink count; remove unconditionally there.
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

    /// The regression from #508: on a ReFS Dev Drive that DOES block-clone, a
    /// sub-cluster file (every `.d` under ~4 KB) falls back to copy — and kache
    /// used to blame the filesystem, once per rustc process, hundreds of times
    /// per build. A healthy volume must produce no advisory at all.
    #[test]
    fn sub_cluster_copy_on_a_cow_volume_is_not_a_no_cow_problem() {
        assert_eq!(
            classify_copy_restore(Some("G:\\"), Some("G:\\"), Some(true), Some(true)),
            CopyRestoreCause::SubClusterOnCowVolume,
        );
    }

    /// But silence must not extend to a file that COULD have been cloned and
    /// wasn't — that would hide a filter driver or a kache bug quietly demoting
    /// every large artifact to a copy.
    #[test]
    fn a_clonable_file_that_fails_to_clone_is_still_surfaced() {
        assert_eq!(
            classify_copy_restore(Some("G:\\"), Some("G:\\"), Some(true), Some(false)),
            CopyRestoreCause::UnexpectedOnCowVolume,
        );
        assert_eq!(
            classify_copy_restore(Some("G:\\"), Some("G:\\"), Some(true), None),
            CopyRestoreCause::UnexpectedOnCowVolume,
            "an unknown size must not be assumed benign",
        );
    }

    #[test]
    fn same_volume_without_block_cloning_is_a_real_no_cow_warning() {
        assert_eq!(
            classify_copy_restore(Some("C:\\"), Some("C:\\"), Some(false), Some(true)),
            CopyRestoreCause::NoCow,
        );
    }

    /// Cross-volume wins over capability: two Dev Drives still can't clone
    /// across the volume boundary, so the co-locate advice is the useful one.
    #[test]
    fn different_volumes_report_cross_volume_even_when_both_support_cow() {
        assert_eq!(
            classify_copy_restore(Some("C:\\"), Some("G:\\"), Some(true), Some(false)),
            CopyRestoreCause::CrossVolume,
        );
        assert_eq!(
            classify_copy_restore(Some("c:\\"), Some("C:\\"), Some(false), Some(true)),
            CopyRestoreCause::NoCow,
            "volume roots compare case-insensitively",
        );
    }

    /// A failed probe must still warn (a copy-restore IS happening) — but as
    /// "unknown", not as "no copy-on-write". Asserting NTFS from a failed probe
    /// would be the same unfounded claim that produced #508.
    #[test]
    fn a_failed_capability_probe_warns_without_claiming_no_cow() {
        assert_eq!(
            classify_copy_restore(Some("C:\\"), Some("C:\\"), None, Some(true)),
            CopyRestoreCause::UnknownCow,
        );
        assert_eq!(
            classify_copy_restore(None, None, None, None),
            CopyRestoreCause::UnknownCow,
        );
    }

    /// A benign storage-layout advisory must not be able to mute a genuine clone
    /// FAULT: they dedup in separate buckets, so one can't swallow the other.
    #[test]
    fn a_layout_advisory_cannot_mute_a_clone_fault() {
        let fault = CopyRestoreCause::UnexpectedOnCowVolume.warn_bucket();
        for benign in [
            CopyRestoreCause::NoCow,
            CopyRestoreCause::CrossVolume,
            CopyRestoreCause::UnknownCow,
        ] {
            assert_ne!(
                benign.warn_bucket(),
                fault,
                "{benign:?} must not share a dedup marker with a clone fault",
            );
        }

        let base = Path::new("/tmp/kache-cow-warn-abc123");
        assert_ne!(
            bucket_marker(base, CopyRestoreCause::NoCow.warn_bucket()),
            bucket_marker(base, fault),
        );
    }

    /// #551: `[cache] storage_layout_advice = false` mutes exactly the three
    /// layout advisories. The sub-cluster case is already silent, and the fault
    /// case must stay un-muteable — advice and faults are different severities.
    #[test]
    fn storage_layout_advice_mutes_advice_never_faults() {
        for advisory in [
            CopyRestoreCause::NoCow,
            CopyRestoreCause::CrossVolume,
            CopyRestoreCause::UnknownCow,
        ] {
            assert!(
                advisory.is_layout_advisory(),
                "{advisory:?} is layout advice and must be muteable",
            );
        }
        for non_advisory in [
            CopyRestoreCause::SubClusterOnCowVolume,
            CopyRestoreCause::UnexpectedOnCowVolume,
        ] {
            assert!(
                !non_advisory.is_layout_advisory(),
                "{non_advisory:?} must not be muteable by the advice knob",
            );
        }
    }

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

    #[cfg(unix)]
    #[test]
    fn writable_copy_is_private_and_compiler_writable() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.o");
        fs::write(&blob, b"cached object").unwrap();
        fs::set_permissions(&blob, fs::Permissions::from_mode(0o400)).unwrap();

        let output = dir.path().join("output.o");
        prepare_writable_target_from_file(&blob, &output)
            .unwrap()
            .publish()
            .unwrap();

        assert_ne!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o200,
            0,
            "restore must be owner-writable regardless of blob permissions"
        );
        assert_ne!(
            fs::metadata(&blob).unwrap().ino(),
            fs::metadata(&output).unwrap().ino(),
            "writable output must never share the blob inode"
        );
        fs::write(&output, b"changed").unwrap();
        assert_eq!(fs::read(&blob).unwrap(), b"cached object");
    }

    #[cfg(windows)]
    #[test]
    fn writable_copy_is_private_and_writable_on_windows() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.obj");
        fs::write(&blob, b"cached object").unwrap();
        let mut perms = fs::metadata(&blob).unwrap().permissions();
        perms.set_readonly(true);
        fs::set_permissions(&blob, perms).unwrap();

        let output = dir.path().join("output.obj");
        prepare_writable_target_from_file(&blob, &output)
            .unwrap()
            .publish()
            .unwrap();

        assert!(!fs::metadata(&output).unwrap().permissions().readonly());
        fs::write(&output, b"changed").unwrap();
        assert_eq!(fs::read(&blob).unwrap(), b"cached object");
        assert!(fs::metadata(&blob).unwrap().permissions().readonly());

        let mut perms = fs::metadata(&blob).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        fs::set_permissions(&blob, perms).unwrap();
    }

    #[test]
    fn writable_materializers_never_replace_existing_target() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.o");
        let linked = dir.path().join("linked.o");
        let written = dir.path().join("written.d");
        fs::write(&blob, b"cached").unwrap();
        fs::write(&linked, b"race winner").unwrap();
        fs::write(&written, b"race winner").unwrap();

        #[cfg(windows)]
        for target in [&linked, &written] {
            let mut permissions = fs::metadata(target).unwrap().permissions();
            permissions.set_readonly(true);
            fs::set_permissions(target, permissions).unwrap();
        }

        assert!(
            prepare_writable_target_from_file(&blob, &linked)
                .unwrap()
                .publish()
                .is_err()
        );
        assert!(
            prepare_writable_target_from_bytes(&written, b"cached depinfo")
                .unwrap()
                .publish()
                .is_err()
        );
        assert_eq!(fs::read(&linked).unwrap(), b"race winner");
        assert_eq!(fs::read(&written).unwrap(), b"race winner");

        #[cfg(windows)]
        for target in [&linked, &written] {
            let mut permissions = fs::metadata(target).unwrap().permissions();
            assert!(permissions.readonly(), "refusal must not chmod the target");
            #[allow(clippy::permissions_set_readonly_false)]
            permissions.set_readonly(false);
            fs::set_permissions(target, permissions).unwrap();
        }
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

    /// The write-clock touch invariant (kunobi-ninja/kache#677, #135): a
    /// touched file's mtime must order between files written before and
    /// after it by the SAME clock the kernel stamps writes with. A
    /// precise-clock touch (`FileTime::now()`) can postdate a file written
    /// immediately after — that inversion is the entire #677 bug. The
    /// before-bound also proves the touch moved the mtime forward from an
    /// arbitrarily old value.
    #[test]
    fn test_touch_write_clock_orders_with_file_writes() {
        let dir = tempfile::tempdir().unwrap();
        let before = dir.path().join("before");
        let touched = dir.path().join("touched.rlib");
        let after = dir.path().join("after");

        fs::write(&before, b"b").unwrap();
        fs::write(&touched, b"content").unwrap();
        let past = filetime::FileTime::from_unix_time(1_000_000_000, 0);
        filetime::set_file_mtime(&touched, past).unwrap();

        touch_mtime_write_clock(&touched).unwrap();
        fs::write(&after, b"a").unwrap();

        let mtime =
            |p: &Path| filetime::FileTime::from_last_modification_time(&fs::metadata(p).unwrap());
        assert!(
            mtime(&before) <= mtime(&touched),
            "touch must move the mtime to now: before={:?} touched={:?}",
            mtime(&before),
            mtime(&touched)
        );
        // The after-bound is the #677 regression assertion and runs
        // everywhere. On unix the stamp goes through futimens(UTIME_NOW);
        // on Windows through a sibling write-clock sample
        // (kunobi-ninja/kache#681) — before that fix, the first Windows CI
        // run of this test measured the precise-clock stamp postdating a
        // file written ~2ms later (an ~1.8ms inversion window on NTFS).
        assert!(
            mtime(&touched) <= mtime(&after),
            "write-clock touch must not postdate a subsequent write: touched={:?} after={:?}",
            mtime(&touched),
            mtime(&after)
        );
    }

    /// The fail-closed half of kunobi-ninja/kache#682: the stamp opens with
    /// O_NOFOLLOW, so a path swapped for a symlink between materialization
    /// and touch is refused instead of re-dating whatever it points at.
    /// The error degrades to a recompile at the restore call sites.
    #[cfg(unix)]
    #[test]
    fn test_touch_refuses_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real.rlib");
        let link = dir.path().join("swapped.rlib");
        fs::write(&real, b"content").unwrap();
        let past = filetime::FileTime::from_unix_time(1_000_000_000, 0);
        filetime::set_file_mtime(&real, past).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        assert!(
            touch_mtime_write_clock(&link).is_err(),
            "touching through a symlink must fail closed"
        );
        let real_mtime =
            filetime::FileTime::from_last_modification_time(&fs::metadata(&real).unwrap());
        assert_eq!(
            real_mtime.unix_seconds(),
            1_000_000_000,
            "the symlink target must not have been re-dated"
        );
    }

    /// A FIFO swapped in for the artifact must be refused promptly: with
    /// O_NONBLOCK the open succeeds without a writer, fstat sees S_IFIFO,
    /// and the touch bails. Without O_NONBLOCK the open would block forever
    /// waiting for a writer — so this test also pins the flag (a mutant
    /// dropping it hangs here instead of passing).
    #[cfg(unix)]
    #[test]
    fn test_touch_refuses_fifo() {
        use std::os::unix::ffi::OsStrExt;
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("swapped.rlib");
        let cpath = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(cpath.as_ptr(), 0o644) }, 0);

        assert!(
            touch_mtime_write_clock(&fifo).is_err(),
            "touching a FIFO must fail closed (and promptly)"
        );
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
    fn rustc_depinfo_rewrite_rebases_target_and_worktree_separately() {
        let producer_target = Path::new("/build/worktree-a/target");
        let producer_cwd = Path::new("/build/worktree-a");
        let input = "/build/worktree-a/target/debug/deps/libdemo.rlib: \
/build/worktree-a/src/lib.rs /build/worktree-a/assets/value.txt\n";

        let stored = rewrite_rustc_depinfo_content(
            input,
            producer_target,
            producer_cwd,
            None,
            DepInfoMode::Relativize,
        );
        assert!(stored.contains("__kache_root__/debug/deps/libdemo.rlib"));
        assert!(stored.contains("__kache_cwd__/src/lib.rs"));
        assert!(stored.contains("__kache_cwd__/assets/value.txt"));
        assert!(!stored.contains("worktree-a"));

        let restored = rewrite_rustc_depinfo_content(
            &stored,
            Path::new("/build/worktree-b/target"),
            Path::new("/build/worktree-b"),
            None,
            DepInfoMode::Expand,
        );
        assert!(restored.contains("/build/worktree-b/target/debug/deps/libdemo.rlib"));
        assert!(restored.contains("/build/worktree-b/src/lib.rs"));
        assert!(restored.contains("/build/worktree-b/assets/value.txt"));
        assert!(!restored.contains("worktree-a"));
    }

    #[test]
    fn rustc_depinfo_rewrite_handles_workspace_and_make_escaped_paths() {
        let producer_workspace = Path::new("/build/work tree-a");
        let producer_target = producer_workspace.join("target");
        let producer_cwd = producer_workspace.join("crates/demo");
        let input = "/build/work\\ tree-a/target/debug/deps/libdemo.rlib: \
/build/work\\ tree-a/crates/demo/src/lib.rs \
/build/work\\ tree-a/shared/schema.txt\n\
# env-dep:SCHEMA_ROOT=/build/work tree-a/shared\n";

        let stored = rewrite_rustc_depinfo_content(
            input,
            &producer_target,
            &producer_cwd,
            Some(producer_workspace),
            DepInfoMode::Relativize,
        );
        assert!(stored.contains("__kache_root__/debug/deps/libdemo.rlib"));
        assert!(stored.contains("__kache_cwd__/src/lib.rs"));
        assert!(stored.contains("__kache_workspace__/shared/schema.txt"));
        assert!(stored.contains("# env-dep:SCHEMA_ROOT=__kache_workspace__/shared"));
        assert!(!stored.contains("work tree-a"));
        assert!(!stored.contains("work\\ tree-a"));

        let consumer_workspace = Path::new("/build/work tree-b");
        let restored = rewrite_rustc_depinfo_content(
            &stored,
            &consumer_workspace.join("target"),
            &consumer_workspace.join("crates/demo"),
            Some(consumer_workspace),
            DepInfoMode::Expand,
        );
        assert!(restored.contains("/build/work\\ tree-b/target/debug/deps/libdemo.rlib"));
        assert!(restored.contains("/build/work\\ tree-b/crates/demo/src/lib.rs"));
        assert!(restored.contains("/build/work\\ tree-b/shared/schema.txt"));
        assert!(restored.contains("# env-dep:SCHEMA_ROOT=/build/work tree-b/shared"));
        assert!(!restored.contains("work tree-a"));
        assert!(!restored.contains("work\\ tree-a"));
    }

    #[test]
    fn rustc_depinfo_rewrite_round_trips_configured_external_roots() {
        let producer = PathBuf::from("/mount-a/generated");
        let consumer = PathBuf::from("/mount-b/generated");
        let sentinel = "__kache_base_dir_0__/".to_string();
        let input = "/work-a/target/demo.d: /mount-a/generated/schema/data.json\n";

        let stored = rewrite_rustc_depinfo_content_with_configured_roots(
            input,
            Path::new("/work-a/target"),
            Path::new("/work-a/crate"),
            Some(Path::new("/work-a")),
            &[(producer, sentinel.clone(), 8)],
            DepInfoMode::Relativize,
        );
        assert!(stored.contains("__kache_base_dir_0__/schema/data.json"));
        assert!(!stored.contains("/mount-a/generated"));

        let restored = rewrite_rustc_depinfo_content_with_configured_roots(
            &stored,
            Path::new("/work-b/target"),
            Path::new("/work-b/crate"),
            Some(Path::new("/work-b")),
            &[(consumer, sentinel, 8)],
            DepInfoMode::Expand,
        );
        assert!(restored.contains("/mount-b/generated/schema/data.json"));
        assert!(!restored.contains("/mount-a/generated"));
    }

    #[test]
    fn configured_root_wins_when_it_equals_the_working_directory() {
        let root = PathBuf::from("/work/crate");
        let stored = rewrite_rustc_depinfo_content_with_configured_roots(
            "/work/target/demo.d: /work/crate/generated.rs\n",
            Path::new("/work/target"),
            &root,
            Some(Path::new("/work")),
            &[(root.clone(), "__kache_base_dir_0__/".to_string(), 8)],
            DepInfoMode::Relativize,
        );
        assert!(stored.contains("__kache_base_dir_0__/generated.rs"));
        assert!(!stored.contains(DEPINFO_CWD_SENTINEL));
    }

    #[test]
    fn configured_ancestor_precedes_more_specific_automatic_roots() {
        let configured = PathBuf::from("/sandbox");
        let stored = rewrite_rustc_depinfo_content_with_configured_roots(
            "/sandbox/worktree-a/target/demo.d: /sandbox/worktree-a/crate/generated.rs\n",
            Path::new("/sandbox/worktree-a/target"),
            Path::new("/sandbox/worktree-a/crate"),
            Some(Path::new("/sandbox/worktree-a")),
            &[(configured, "__kache_base_dir_0__/".to_string(), 8)],
            DepInfoMode::Relativize,
        );
        assert!(stored.starts_with("__kache_root__/demo.d:"), "{stored}");
        assert!(stored.contains("__kache_base_dir_0__/worktree-a/crate/generated.rs"));
        assert!(!stored.contains(DEPINFO_CWD_SENTINEL));
        assert!(!stored.contains(DEPINFO_WORKSPACE_SENTINEL));
    }

    #[test]
    fn target_output_rebases_separately_from_generated_source_ownership() {
        let stored = rewrite_rustc_depinfo_content_with_configured_roots(
            "/work/target/demo.d: /work/target/generated.rs\n",
            Path::new("/work/target"),
            Path::new("/work"),
            Some(Path::new("/work")),
            &[],
            DepInfoMode::Relativize,
        );

        assert_eq!(
            stored,
            "__kache_root__/demo.d: __kache_cwd__/target/generated.rs\n"
        );
    }

    #[test]
    fn external_target_generated_source_round_trips_independently_from_output() {
        let producer_target = PathBuf::from("/external-a/target");
        let consumer_target = PathBuf::from("/external-b/target");
        let target_sentinel = "__kache_target_rule__/".to_string();
        let input = "/external-a/target/debug/deps/demo.d: \
/external-a/target/debug/build/demo/out/generated.rs\n";

        let stored = rewrite_rustc_depinfo_content_with_configured_roots(
            input,
            &producer_target,
            Path::new("/work-a/crate"),
            Some(Path::new("/work-a")),
            &[(producer_target.clone(), target_sentinel.clone(), 3)],
            DepInfoMode::Relativize,
        );
        assert_eq!(
            stored,
            "__kache_root__/debug/deps/demo.d: \
__kache_target_rule__/debug/build/demo/out/generated.rs\n"
        );

        let restored = rewrite_rustc_depinfo_content_with_configured_roots(
            &stored,
            &consumer_target,
            Path::new("/work-b/crate"),
            Some(Path::new("/work-b")),
            &[(consumer_target.clone(), target_sentinel, 3)],
            DepInfoMode::Expand,
        );
        assert_eq!(
            restored,
            "/external-b/target/debug/deps/demo.d: \
/external-b/target/debug/build/demo/out/generated.rs\n"
        );
    }

    #[test]
    fn configured_root_does_not_match_an_interior_path_component() {
        let stored = rewrite_rustc_depinfo_content_with_configured_roots(
            "out: /mnt/sandbox/file.rs /mnt/@/sandbox/punctuation.rs \
/mnt/foo\\ /sandbox/escaped-space.rs /sandbox/owned.rs\n\
# env-dep:ASSET=/mnt/sandbox/asset.txt\n",
            Path::new("/target"),
            Path::new("/work"),
            None,
            &[(
                PathBuf::from("/sandbox"),
                "__kache_base_dir_0__/".to_string(),
                8,
            )],
            DepInfoMode::Relativize,
        );

        assert!(stored.contains("/mnt/sandbox/file.rs"));
        assert!(stored.contains("/mnt/@/sandbox/punctuation.rs"));
        assert!(stored.contains("/mnt/foo\\ /sandbox/escaped-space.rs"));
        assert!(stored.contains("# env-dep:ASSET=/mnt/sandbox/asset.txt"));
        assert!(stored.contains("__kache_base_dir_0__/owned.rs"));
    }

    #[test]
    fn portable_home_root_does_not_steal_nested_workspace_paths() {
        let producer_roots = [(
            PathBuf::from("/home/alice"),
            "__kache_home__/".to_string(),
            1,
        )];
        let stored = rewrite_rustc_depinfo_content_with_configured_roots(
            "out: /home/alice/project/src/lib.rs /home/alice/shared/schema.json\n",
            Path::new("/target"),
            Path::new("/home/alice/project"),
            Some(Path::new("/home/alice/project")),
            &producer_roots,
            DepInfoMode::Relativize,
        );
        assert!(stored.contains("__kache_cwd__/src/lib.rs"), "{stored}");
        assert!(
            stored.contains("__kache_home__/shared/schema.json"),
            "{stored}"
        );

        let consumer_roots = [(
            PathBuf::from("/Users/bob"),
            "__kache_home__/".to_string(),
            1,
        )];
        let restored = rewrite_rustc_depinfo_content_with_configured_roots(
            &stored,
            Path::new("/other-target"),
            Path::new("/Users/bob/project"),
            Some(Path::new("/Users/bob/project")),
            &consumer_roots,
            DepInfoMode::Expand,
        );
        assert!(restored.contains("/Users/bob/project/src/lib.rs"));
        assert!(restored.contains("/Users/bob/shared/schema.json"));
        assert!(!restored.contains("/home/alice"));
    }

    #[test]
    fn depinfo_prefix_replacement_can_match_windows_ascii_case_aliases() {
        assert_eq!(
            replace_depinfo_text(
                r"c:\\WORKTREE\\src\\lib.rs",
                r"C:\\worktree\\",
                DEPINFO_CWD_SENTINEL,
                true,
                DepInfoTextContext::MakeLine,
            ),
            r"__kache_cwd__/src\\lib.rs"
        );
        assert_eq!(
            replace_depinfo_text(
                "unrelated",
                "C:\\worktree\\",
                DEPINFO_CWD_SENTINEL,
                true,
                DepInfoTextContext::MakeLine,
            ),
            "unrelated"
        );
        assert_eq!(
            replace_depinfo_text(
                r"c:\WORKTREE\",
                r"C:\worktree\",
                DEPINFO_CWD_SENTINEL,
                true,
                DepInfoTextContext::MakeLine,
            ),
            DEPINFO_CWD_SENTINEL,
            "a case-insensitive match ending exactly at EOF must be replaced"
        );
        assert_eq!(
            replace_depinfo_text(
                r"keep c:\WORKTREE\src\lib.rs and C:\worktree\asset.txt",
                r"C:\worktree\",
                DEPINFO_CWD_SENTINEL,
                true,
                DepInfoTextContext::MakeLine,
            ),
            r"keep __kache_cwd__/src\lib.rs and __kache_cwd__/asset.txt"
        );
    }

    #[test]
    fn windows_depinfo_prefixes_retain_drive_and_unc_verbatim_aliases() {
        let drive = depinfo_prefixes_for_display(r"\\?\C:\Work", true, true);
        assert!(drive.contains(&r"C:\Work\".to_string()));
        assert!(drive.contains(&r"\\?\C:\Work\".to_string()));
        assert!(drive.contains(&r"C:\Work/".to_string()));
        assert!(drive.contains(&r"\\?\C:\Work/".to_string()));

        let unc = depinfo_prefixes_for_display(r"\\?\UNC\server\share\root", true, true);
        assert!(unc.contains(&r"\\server\share\root\".to_string()));
        assert!(unc.contains(&r"\\?\UNC\server\share\root\".to_string()));

        let expand = depinfo_prefixes_for_display(r"\\?\C:\Work", false, true);
        assert!(!expand.iter().any(|prefix| prefix.starts_with(r"\\?\")));
        assert!(expand.iter().all(|prefix| prefix.starts_with(r"C:\Work")));
    }

    #[test]
    fn configured_windows_roots_keep_verbatim_and_plain_slots_distinct() {
        let verbatim_drive = depinfo_prefixes_for_exact_display(r"\\?\C:\Work", true);
        assert_eq!(verbatim_drive.len(), 2);
        assert!(verbatim_drive.contains(&r"\\?\C:\Work/".to_string()));
        assert!(verbatim_drive.contains(&r"\\?\C:\Work\".to_string()));
        assert!(
            verbatim_drive
                .iter()
                .all(|prefix| prefix.starts_with(r"\\?\"))
        );
        assert!(
            !verbatim_drive
                .iter()
                .any(|prefix| prefix.starts_with(r"C:\Work"))
        );

        let verbatim_unc = depinfo_prefixes_for_exact_display(r"\\?\UNC\server\share", true);
        assert_eq!(verbatim_unc.len(), 2);
        assert!(verbatim_unc.contains(&r"\\?\UNC\server\share/".to_string()));
        assert!(verbatim_unc.contains(&r"\\?\UNC\server\share\".to_string()));
        assert!(
            verbatim_unc
                .iter()
                .all(|prefix| prefix.starts_with(r"\\?\UNC\server\share"))
        );
        assert!(
            !verbatim_unc
                .iter()
                .any(|prefix| prefix.starts_with(r"\\server\share"))
        );

        let input = r"out: \\?\C:\Work\src\verbatim.rs C:\Work\src\plain.rs \\?\UNC\server\share\src\verbatim_unc.rs \\server\share\src\plain_unc.rs";
        let producer_roots = [
            (
                PathBuf::from(r"\\?\C:\Work"),
                "__kache_base_dir_0__/".to_string(),
            ),
            (
                PathBuf::from(r"C:\Work"),
                "__kache_base_dir_1__/".to_string(),
            ),
            (
                PathBuf::from(r"\\?\UNC\server\share"),
                "__kache_base_dir_2__/".to_string(),
            ),
            (
                PathBuf::from(r"\\server\share"),
                "__kache_base_dir_3__/".to_string(),
            ),
        ];
        let stored = producer_roots
            .iter()
            .fold(input.to_string(), |content, (root, sentinel)| {
                rewrite_depinfo_content_with_exact_sentinel_for_platform(
                    &content,
                    root,
                    sentinel,
                    DepInfoMode::Relativize,
                    true,
                )
            });
        assert!(
            stored.contains(r"__kache_base_dir_0__/src\verbatim.rs"),
            "{stored}"
        );
        assert!(
            stored.contains(r"__kache_base_dir_1__/src\plain.rs"),
            "{stored}"
        );
        assert!(
            stored.contains(r"__kache_base_dir_2__/src\verbatim_unc.rs"),
            "{stored}"
        );
        assert!(
            stored.contains(r"__kache_base_dir_3__/src\plain_unc.rs"),
            "{stored}"
        );

        let consumer_roots = [
            (
                PathBuf::from(r"\\?\D:\Other"),
                "__kache_base_dir_0__/".to_string(),
            ),
            (
                PathBuf::from(r"D:\Other"),
                "__kache_base_dir_1__/".to_string(),
            ),
            (
                PathBuf::from(r"\\?\UNC\server2\share2"),
                "__kache_base_dir_2__/".to_string(),
            ),
            (
                PathBuf::from(r"\\server2\share2"),
                "__kache_base_dir_3__/".to_string(),
            ),
        ];
        let restored = consumer_roots
            .iter()
            .fold(stored, |content, (root, sentinel)| {
                rewrite_depinfo_content_with_exact_sentinel_for_platform(
                    &content,
                    root,
                    sentinel,
                    DepInfoMode::Expand,
                    true,
                )
            });
        assert!(restored.contains(r"\\?\D:\Other\src\verbatim.rs"));
        assert!(restored.contains(r"D:\Other\src\plain.rs"));
        assert!(restored.contains(r"\\?\UNC\server2\share2\src\verbatim_unc.rs"));
        assert!(restored.contains(r"\\server2\share2\src\plain_unc.rs"));
        assert!(!restored.contains(r"C:\Work"));
        assert!(!restored.contains(r"\\server\share"));
    }

    #[test]
    #[cfg(windows)]
    fn rustc_depinfo_rewrite_handles_native_windows_cwd_and_env_dep() {
        let input = "D:\\WORK TREE\\target\\debug\\deps\\demo.d: \
D:\\WORK TREE\\crate\\src\\lib.rs\n\
# env-dep:ASSET=D:\\\\WORK TREE\\\\crate\\\\asset.txt\n";
        let stored = rewrite_rustc_depinfo_content(
            input,
            Path::new(r"d:\work tree\target"),
            Path::new(r"d:\work tree\crate"),
            Some(Path::new(r"d:\work tree")),
            DepInfoMode::Relativize,
        );
        assert!(stored.contains("__kache_root__/debug\\deps\\demo.d"));
        assert!(stored.contains("__kache_cwd__/src\\lib.rs"));
        assert!(stored.contains("# env-dep:ASSET=__kache_cwd__/asset.txt"));
        assert!(!stored.to_ascii_lowercase().contains(r"d:\work tree"));
    }

    #[test]
    #[cfg(unix)]
    fn depinfo_relativize_matches_a_canonical_symlink_target() {
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real-target");
        let alias = dir.path().join("target-alias");
        std::fs::create_dir_all(&real).unwrap();
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let canonical = real.canonicalize().unwrap();
        let input = format!(
            "{}/debug/demo.d: {}/debug/input.rs\n",
            canonical.display(),
            canonical.display()
        );

        let stored = rewrite_depinfo_content(&input, &alias, DepInfoMode::Relativize);
        assert_eq!(stored.matches(DEPINFO_ROOT_SENTINEL).count(), 2, "{stored}");
        assert!(!stored.contains(canonical.to_str().unwrap()), "{stored}");
    }

    /// kunobi-ninja/kache#330: Windows dep-info uses backslash separators and
    /// often mixes them (an env-var join appends with `/`). Relativize must
    /// catch both spellings of the prefix, or the builder's absolute paths
    /// ship in the cached entry and poison every other project sharing it.
    #[test]
    #[cfg(windows)]
    fn test_depinfo_relativize_handles_windows_separators() {
        let input = "S:\\proj\\target\\debug\\deps\\demo.d: \
S:\\proj\\target\\debug\\build\\demo-8a22\\out/generated.rs\n";

        let rewritten = rewrite_depinfo_content(
            input,
            Path::new("S:\\proj\\target"),
            DepInfoMode::Relativize,
        );
        assert!(
            !rewritten.contains("S:\\proj\\target"),
            "the builder's absolute prefix must not survive: {rewritten}"
        );
        assert_eq!(
            rewritten.matches(DEPINFO_ROOT_SENTINEL).count(),
            2,
            "both references relativize: {rewritten}"
        );

        // Expansion re-roots at the consumer with `/`, which Windows accepts.
        let expanded = rewrite_depinfo_content(
            &rewritten,
            Path::new("T:\\other\\target"),
            DepInfoMode::Expand,
        );
        assert!(
            expanded.contains("T:\\other\\target/debug\\build\\demo-8a22\\out/generated.rs"),
            "consumer-rooted mixed-separator path: {expanded}"
        );
        assert!(!expanded.contains(DEPINFO_ROOT_SENTINEL));
    }

    /// Mirrors cargo's `unescape_env` grammar for `# env-dep:` values: only
    /// `\\`, `\n`, and `\r` are legal escapes; anything else makes cargo fail
    /// the whole compile with "unknown escape character" (kunobi-ninja/kache#730).
    fn cargo_env_dep_value_parses(value: &str) -> bool {
        let mut chars = value.trim_end_matches('\n').chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('\\') | Some('n') | Some('r') => {}
                    _ => return false,
                }
            }
        }
        true
    }

    /// The oracle itself must be red for the corruption this guards against:
    /// an orphan single backslash before `x86_64` is exactly what cargo
    /// rejected in the nightly Firefox/Windows bench.
    #[test]
    fn test_env_dep_grammar_rejects_orphan_escape() {
        assert!(cargo_env_dep_value_parses(
            r"C:\\proj\\obj\\x86_64-pc-windows-msvc"
        ));
        assert!(!cargo_env_dep_value_parses(
            r"__kache_root__/obj\x86_64-pc-windows-msvc"
        ));
        assert!(!cargo_env_dep_value_parses(r"C:\proj\obj"));
    }

    /// kunobi-ninja/kache#730: `# env-dep:` values escape backslashes as
    /// `\\`. Relativize must match the escaped spelling of a backslash
    /// anchor, never leaving half an escape pair behind, and the result must
    /// stay parseable under cargo's env-dep grammar.
    #[test]
    fn test_depinfo_env_dep_relativize_matches_escaped_prefix() {
        let input = "target\\debug\\deps\\demo.d: C:\\proj\\src\\lib.rs\n\
                     # env-dep:OUT_DIR=C:\\\\proj\\\\obj\\\\x86_64-pc-windows-msvc\\\\out\n";
        let prefixes = ["C:\\proj/".to_string(), "C:\\proj\\".to_string()];

        let rewritten =
            rewrite_depinfo_content_with_prefixes(input, &prefixes, DepInfoMode::Relativize);

        let env_line = rewritten
            .lines()
            .find(|l| l.starts_with(DEPINFO_ENV_DEP_PREFIX))
            .unwrap();
        assert!(
            env_line.contains("# env-dep:OUT_DIR=__kache_root__/obj\\\\x86_64"),
            "escaped anchor must relativize whole, not split an escape pair: {rewritten}"
        );
        assert!(
            cargo_env_dep_value_parses(env_line.split_once('=').unwrap().1),
            "relativized env-dep value must stay parseable: {rewritten}"
        );
        assert!(
            rewritten.contains("__kache_root__/src\\lib.rs"),
            "file-list line keeps the plain-spelling rewrite: {rewritten}"
        );
    }

    /// env-dep record keys are environment variable NAMES, never paths:
    /// a key-only record (no `=`) and the key half of a record must survive
    /// both directions byte-for-byte (cross-model review finding).
    #[test]
    fn test_depinfo_env_dep_never_rewrites_keys() {
        let input = "# env-dep:__kache_root__/WEIRD_KEY\n\
                     # env-dep:__kache_root__/ALSO_WEIRD=__kache_root__/value\n";
        let prefixes = ["C:\\proj/".to_string(), "C:\\proj\\".to_string()];

        let expanded = rewrite_depinfo_content_with_prefixes(input, &prefixes, DepInfoMode::Expand);
        assert!(
            expanded.contains("# env-dep:__kache_root__/WEIRD_KEY\n"),
            "key-only record must be untouched: {expanded}"
        );
        assert!(
            expanded.contains("# env-dep:__kache_root__/ALSO_WEIRD=C:\\\\proj/value"),
            "only the value half may expand: {expanded}"
        );
    }

    /// kunobi-ninja/kache#730, the shape that broke the nightly bench: an
    /// entry relativized under a forward-spelled anchor, expanded under a
    /// backslash-spelled anchor. Expand must insert the escaped spelling into
    /// `# env-dep:` lines or cargo rejects the restored `.d` outright.
    #[test]
    fn test_depinfo_env_dep_expand_inserts_escaped_prefix() {
        let stored = "target/debug/deps/demo.d: __kache_root__/src/lib.rs\n\
                      # env-dep:OUT_DIR=__kache_root__/obj-kache-bench\\\\x86_64-pc-windows-msvc\\\\out\n";
        let prefixes = [
            "C:\\other\\clone-b/".to_string(),
            "C:\\other\\clone-b\\".to_string(),
        ];

        let expanded =
            rewrite_depinfo_content_with_prefixes(stored, &prefixes, DepInfoMode::Expand);

        let env_line = expanded
            .lines()
            .find(|l| l.starts_with(DEPINFO_ENV_DEP_PREFIX))
            .unwrap();
        assert!(
            env_line.contains("OUT_DIR=C:\\\\other\\\\clone-b/obj-kache-bench\\\\x86_64"),
            "expanded env-dep anchor must be escaped: {expanded}"
        );
        assert!(
            cargo_env_dep_value_parses(env_line.split_once('=').unwrap().1),
            "expanded env-dep value must stay parseable: {expanded}"
        );
        assert!(
            expanded.contains("demo.d: C:\\other\\clone-b/src/lib.rs"),
            "file-list line takes the plain anchor (rustc leaves backslashes raw there): {expanded}"
        );

        // Round trip: re-relativizing under the same anchor restores the
        // stored form, so warm entries stay stable across hits.
        let rerelativized =
            rewrite_depinfo_content_with_prefixes(&expanded, &prefixes, DepInfoMode::Relativize);
        assert_eq!(rerelativized, stored, "expand/relativize must round-trip");
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

    #[test]
    fn executable_permission_helper_restores_consumer_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restored-executable");
        fs::write(&path, b"executable").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o400)).unwrap();
        }
        #[cfg(not(unix))]
        {
            let mut permissions = fs::metadata(&path).unwrap().permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&path, permissions).unwrap();
        }

        set_executable_permissions(&path).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o755
            );
        }
        #[cfg(not(unix))]
        assert!(!fs::metadata(&path).unwrap().permissions().readonly());
    }

    #[test]
    fn writable_staging_file_is_created_beside_target() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("outputs");
        fs::create_dir(&parent).unwrap();
        let target = parent.join("output.o");

        let staged = new_writable_staging_file(&target).unwrap();

        assert_eq!(staged.path().parent(), Some(parent.as_path()));
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

    #[test]
    fn link_to_target_optimistic_existing_parent() {
        let dir = tempfile::TempDir::new().unwrap();
        let src = dir.path().join("source.txt");
        fs::write(&src, "test").unwrap();

        // Parent exists
        let dst_dir = dir.path().join("out");
        fs::create_dir_all(&dst_dir).unwrap();
        let dst = dst_dir.join("dest.txt");

        super::link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();
        assert_eq!(fs::read_to_string(&dst).unwrap(), "test");
    }

    #[test]
    fn link_to_target_optimistic_missing_parent() {
        let dir = tempfile::TempDir::new().unwrap();
        let src = dir.path().join("source.txt");
        fs::write(&src, "test").unwrap();

        // Parent does NOT exist
        let dst = dir.path().join("missing").join("dest.txt");

        super::link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();
        assert_eq!(fs::read_to_string(&dst).unwrap(), "test");
    }

    #[test]
    fn link_to_target_optimistic_nested_directories() {
        let dir = tempfile::TempDir::new().unwrap();
        let src = dir.path().join("source.txt");
        fs::write(&src, "test").unwrap();

        // Deeply nested missing parent
        let dst = dir
            .path()
            .join("foo")
            .join("bar")
            .join("baz")
            .join("dest.txt");

        super::link_to_target(&src, &dst, LinkStrategy::Copy).unwrap();
        assert_eq!(fs::read_to_string(&dst).unwrap(), "test");
    }

    #[test]
    #[cfg(unix)] // Permissions are easier to test on Unix
    fn link_to_target_permission_denied_does_not_loop() {
        let dir = tempfile::TempDir::new().unwrap();
        let src = dir.path().join("source.txt");
        fs::write(&src, "test").unwrap();

        let dst_dir = dir.path().join("readonly");
        fs::create_dir_all(&dst_dir).unwrap();

        // Make directory read-only
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dst_dir, fs::Permissions::from_mode(0o555)).unwrap();

        let dst = dst_dir.join("dest.txt");

        let err = super::link_to_target(&src, &dst, LinkStrategy::Copy).unwrap_err();
        let io_err = err.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(io_err.kind(), std::io::ErrorKind::PermissionDenied);

        // Restore permissions so TempDir can clean up
        fs::set_permissions(&dst_dir, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn link_to_target_missing_parent_permission_denied_surfaces_real_cause() {
        let dir = tempfile::TempDir::new().unwrap();
        let src = dir.path().join("source.txt");
        fs::write(&src, "test").unwrap();

        let dst_dir = dir.path().join("readonly_parent");
        fs::create_dir_all(&dst_dir).unwrap();

        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dst_dir, fs::Permissions::from_mode(0o555)).unwrap();

        let dst = dst_dir.join("nested").join("dest.txt");

        let err = super::link_to_target(&src, &dst, LinkStrategy::Copy).unwrap_err();
        let io_err = err.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(io_err.kind(), std::io::ErrorKind::PermissionDenied);

        fs::set_permissions(&dst_dir, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn write_restored_missing_parent_permission_denied_surfaces_real_cause() {
        let dir = tempfile::TempDir::new().unwrap();
        let dst_dir = dir.path().join("readonly_parent");
        fs::create_dir_all(&dst_dir).unwrap();

        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dst_dir, fs::Permissions::from_mode(0o555)).unwrap();

        let dst = dst_dir.join("nested").join("dest.txt");

        let err = super::write_restored(&dst, b"test", LinkStrategy::Copy).unwrap_err();
        let io_err = err.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(io_err.kind(), std::io::ErrorKind::PermissionDenied);

        fs::set_permissions(&dst_dir, fs::Permissions::from_mode(0o755)).unwrap();
    }
}
