//! Portable file operations and allocation measurements.
//!
//! Clone and copy helpers keep native filesystem calls behind one API.
//! Measurements distinguish logical length, allocated bytes and private
//! extents. Unknown private allocation stays `None`; directory totals carry
//! a confidence level and account for hardlinks through an inode ledger.
//! Callers own file placement, permissions policy and deletion decisions.

use std::io;
use std::path::Path;

mod copy;
#[cfg(windows)]
pub use copy::windows_cluster_size;
pub use copy::{copy_writable, set_writable_permissions, try_reflink};

mod identity;
pub use identity::{directory_identity, file_identity};

mod ledger;
pub use ledger::{InodeId, InodeLedger};

#[cfg(target_os = "macos")]
mod apfs;
#[cfg(any(target_os = "macos", test))]
mod apfs_values;
#[cfg(any(target_os = "linux", test))]
mod extents;
mod fallback;
#[cfg(target_os = "linux")]
mod linux;

/// How a file's blocks are shared with other files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Sharing {
    /// The platform gave us no sharing information.
    Unknown,
    /// No blocks shared with any other file.
    None,
    /// Some blocks shared; deleting frees `unique` bytes.
    Partial,
    /// Every block shared; deleting this file frees nothing.
    Full,
}

/// How much to trust an aggregate.
///
/// Exact set-reclaim is not obtainable on every platform: macOS exposes no
/// public API enumerating all owners of a *partially* shared extent, and the
/// Linux reverse-mapping ioctls generally need `CAP_SYS_ADMIN`. So a sum of
/// per-file unique bytes is a defensible lower bound, and says so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[non_exhaustive]
pub enum Confidence {
    Exact,
    LowerBound,
    /// The safe default: claim nothing until a probe proves otherwise.
    #[default]
    Estimated,
}

impl Confidence {
    /// The canonical word for this level, for anything that has to name it.
    ///
    /// Here rather than in each caller because this enum is `#[non_exhaustive]`:
    /// a caller outside this crate cannot match it exhaustively, and the
    /// wildcard arm it would be forced to write is precisely where a later
    /// variant would silently render as something it is not.
    pub fn label(self) -> &'static str {
        match self {
            Confidence::Exact => "exact",
            Confidence::LowerBound => "lower_bound",
            Confidence::Estimated => "estimated",
        }
    }
}

/// What one file costs, and what dies with it.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FileSizing {
    /// Logical length (`st_size` / `ATTR_FILE_DATALENGTH`).
    pub logical: u64,
    /// Allocated bytes, counting shared blocks in full. On platforms without
    /// allocation metadata the fallback uses logical length as an estimate.
    pub allocated: u64,
    /// Bytes private to this inode. Unlinking one of several hardlinks does not
    /// free them; callers must also account for `nlink`. `None` when unsupported.
    pub unique: Option<u64>,
    pub sharing: Sharing,
    pub inode: InodeId,
    pub nlink: u64,
    /// Identifies the clone group this file's blocks belong to, when the
    /// platform reports one. IDs are scoped to `inode.dev` and to the observation;
    /// they are not durable content identities.
    pub clone_id: Option<u64>,
}

impl FileSizing {
    /// Blocks shared with other inodes, when known.
    pub fn shared(&self) -> Option<u64> {
        self.unique.map(|u| self.allocated.saturating_sub(u))
    }

    /// True when deleting this file alone reclaims nothing.
    pub fn frees_nothing(&self) -> bool {
        self.nlink > 1 || matches!(self.unique, Some(0))
    }
}

/// A bounded sample of the clone groups a directory's files belong to.
///
/// Keeps the smallest N ids seen, which makes it deterministic and comparable
/// across directories: two directories that share extents will, if they share
/// many, tend to sample the same low ids.
///
/// An intersection identifies a reported clone group on the same device. It
/// does not quantify shared extents or authorize deletion. An empty intersection
/// does not establish independence: the bounded sample may have missed a group.
/// Compare sketches from the same observation period; device IDs can be reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CloneSketch {
    ids: [u64; Self::CAP],
    len: usize,
    /// Older serialized sketches have no device scope and cannot prove sharing.
    #[cfg_attr(feature = "serde", serde(default))]
    devices: [Option<u64>; Self::CAP],
}

impl Default for CloneSketch {
    fn default() -> Self {
        Self {
            ids: [0; Self::CAP],
            len: 0,
            devices: [None; Self::CAP],
        }
    }
}

impl CloneSketch {
    pub const CAP: usize = 32;

    /// Record a clone group in the device's identity domain.
    pub fn insert(&mut self, device: u64, id: u64) {
        // Discard an old snapshot whose entries have no device scope.
        if self.len > Self::CAP || self.devices[..self.len].iter().any(Option::is_none) {
            *self = Self::default();
        }
        let key = (id, Some(device));
        let position = (0..self.len)
            .find(|&i| (self.ids[i], self.devices[i]) >= key)
            .unwrap_or(self.len);
        if position == Self::CAP
            || (position < self.len && (self.ids[position], self.devices[position]) == key)
        {
            return;
        }
        let end = self.len.min(Self::CAP - 1);
        self.ids.copy_within(position..end, position + 1);
        self.devices.copy_within(position..end, position + 1);
        self.ids[position] = id;
        self.devices[position] = Some(device);
        self.len = (self.len + 1).min(Self::CAP);
    }

    pub fn ids(&self) -> &[u64] {
        &self.ids[..self.len.min(Self::CAP)]
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clone groups this sketch and `other` both sampled.
    pub fn shares_with(&self, other: &CloneSketch) -> usize {
        self.scoped_ids()
            .filter(|identity| other.scoped_ids().any(|other| other == *identity))
            .count()
    }

    fn scoped_ids(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.ids()
            .iter()
            .copied()
            .zip(self.devices)
            .filter_map(|(id, dev)| dev.map(|dev| (id, dev)))
    }
}

/// Aggregate over a directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DirSizing {
    pub logical: u64,
    pub allocated: u64,
    pub unique: u64,
    /// Files whose physical reclaim could not be measured.
    #[cfg_attr(feature = "serde", serde(default))]
    pub unknown_files: u64,
    /// Allocated bytes excluded from `unique` because sharing is unknown.
    #[cfg_attr(feature = "serde", serde(default))]
    pub unknown_allocated_bytes: u64,
    pub files: u64,
    /// Files whose blocks were reported fully shared.
    pub fully_shared_files: u64,
    /// Multiply-linked inodes with at least one name outside this tree.
    #[cfg_attr(feature = "serde", serde(default))]
    pub hardlink_pinned_inodes: u64,
    /// Otherwise-unique bytes retained by those outside names.
    #[cfg_attr(feature = "serde", serde(default))]
    pub hardlink_pinned_bytes: u64,
    /// A sample of the clone groups this directory's shared files belong to,
    /// for identifying which other directories hold the other half.
    pub clones: CloneSketch,
    /// Directories that could not be read, so their contents are missing from
    /// every figure above.
    ///
    /// Silently returning a smaller number for an unreadable tree is the worst
    /// failure this type can have: "0 bytes" reads as "nothing to reclaim"
    /// when the truth is "could not look".
    pub unreadable_dirs: u64,
    pub confidence: Confidence,
}

impl DirSizing {
    /// Were parts of this tree invisible to the measurement?
    pub fn is_partial(&self) -> bool {
        self.unreadable_dirs > 0
    }

    /// Fraction of allocated bytes shared with data outside this file set.
    pub fn shared_ratio(&self) -> Option<f64> {
        if self.allocated == 0 || self.confidence == Confidence::Estimated {
            return None;
        }
        Some(1.0 - (self.unique as f64 / self.allocated as f64))
    }
}

/// What a platform probe can actually report.
#[derive(Debug, Clone, Copy)]
pub struct ProbeCaps {
    pub unique_bytes: bool,
    pub clone_refcount: bool,
    pub name: &'static str,
}

/// One entry of a directory as a probe reports it, already classified.
///
/// Symlinks are absent: a walk never follows them, so a probe need not
/// describe them.
#[derive(Debug, Clone)]
pub enum DirEntry {
    /// A subdirectory to descend into.
    Dir(std::path::PathBuf),
    /// A file (or any other non-directory, non-symlink object) and its cost.
    File(FileSizing),
}

pub trait SizeProbe: Send + Sync {
    fn measure_file(&self, path: &Path) -> io::Result<FileSizing>;
    fn capabilities(&self) -> ProbeCaps;

    /// One directory's entries with their sizings, without recursing.
    ///
    /// The default lists names with `read_dir` and probes each file on its
    /// own, which is at least two system calls per file. A platform with a
    /// bulk attribute interface overrides this to answer for a whole
    /// directory per call; the numbers must be the same either way.
    ///
    /// An error means the directory itself could not be read. Entries that
    /// cannot be measured are skipped, as they are in the default.
    fn read_dir(&self, dir: &Path) -> io::Result<Vec<DirEntry>> {
        read_dir_one_by_one(self, dir)
    }

    /// Recursive sum, counting each inode at most once via `ledger`.
    ///
    /// Unreadable entries are skipped rather than failing the whole walk — a
    /// permission error deep in a tree must not lose the rest of the answer.
    fn measure_dir(&self, path: &Path, ledger: &mut InodeLedger) -> io::Result<DirSizing> {
        let pinned_before = ledger.pinned_outside();
        let mut out = DirSizing {
            confidence: if self.capabilities().unique_bytes {
                Confidence::LowerBound
            } else {
                Confidence::Estimated
            },
            ..Default::default()
        };
        walk(self, path, ledger, &mut out);
        let pinned_after = ledger.pinned_outside();
        out.hardlink_pinned_inodes = pinned_after.inodes.saturating_sub(pinned_before.inodes);
        out.hardlink_pinned_bytes = pinned_after
            .unique_bytes
            .saturating_sub(pinned_before.unique_bytes);
        out.unique = out.unique.saturating_sub(out.hardlink_pinned_bytes);
        Ok(out)
    }
}

/// The portable directory reader: names from `read_dir`, one probe per file.
pub fn read_dir_one_by_one(
    probe: &(impl SizeProbe + ?Sized),
    dir: &Path,
) -> io::Result<Vec<DirEntry>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(dir)?.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if ft.is_symlink() {
            continue; // never follow: the target may live outside the set
        }
        if ft.is_dir() {
            entries.push(DirEntry::Dir(entry.path()));
            continue;
        }
        if let Ok(sizing) = probe.measure_file(&entry.path()) {
            entries.push(DirEntry::File(sizing));
        }
    }
    Ok(entries)
}

fn walk(
    probe: &(impl SizeProbe + ?Sized),
    dir: &Path,
    ledger: &mut InodeLedger,
    out: &mut DirSizing,
) {
    let Ok(entries) = probe.read_dir(dir) else {
        // Record rather than ignore: the caller must be able to tell "this
        // directory is empty" from "this directory could not be opened".
        out.unreadable_dirs += 1;
        return;
    };
    for entry in entries {
        let s = match entry {
            DirEntry::Dir(path) => {
                walk(probe, &path, ledger, out);
                continue;
            }
            DirEntry::File(s) => s,
        };
        if !ledger.admit(&s) {
            continue; // another link to an inode we already counted
        }
        out.files += 1;
        out.logical += s.logical;
        out.allocated += s.allocated;
        if let Some(unique) = s.unique {
            out.unique += unique;
        } else {
            out.unknown_files += 1;
            out.unknown_allocated_bytes += s.allocated;
            out.confidence = Confidence::Estimated;
        }
        if matches!(s.sharing, Sharing::Full) {
            out.fully_shared_files += 1;
        }
        // Only shared files can pair this directory with another one.
        if let Some(id) = s.clone_id
            && matches!(s.sharing, Sharing::Partial | Sharing::Full)
        {
            out.clones.insert(s.inode.dev, id);
        }
    }
}

/// Free and total bytes on the volume holding a path.
///
/// This is the ground truth a reclaim is ultimately graded against: predicted
/// bytes versus the actual `statvfs` delta. Expect the two to differ — on APFS
/// a container shares free space across volumes, and snapshots can hold deleted
/// bytes for hours.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct VolumeUsage {
    pub total: u64,
    pub free: u64,
}

impl VolumeUsage {
    pub fn used(&self) -> u64 {
        self.total.saturating_sub(self.free)
    }

    pub fn used_ratio(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.used() as f64 / self.total as f64
        }
    }
}

/// Volume usage for the filesystem holding `path`.
pub fn volume_usage(path: &Path) -> Option<VolumeUsage> {
    #[cfg(unix)]
    {
        volume_usage_unix(path)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

#[cfg(unix)]
fn volume_usage_unix(path: &Path) -> Option<VolumeUsage> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut s: libc::statvfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path and `s` is a valid out-param.
    if unsafe { libc::statvfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    // f_frsize is the fragment size; f_bavail is what a non-root user can use,
    // which is the number a human recognises as "free".
    Some(volume_bytes(
        s.f_blocks as u64,
        s.f_bavail as u64,
        s.f_frsize as u64,
        s.f_bsize as u64,
    ))
}

#[cfg(any(unix, test))]
fn volume_bytes(blocks: u64, available: u64, fragment_size: u64, block_size: u64) -> VolumeUsage {
    let unit = if fragment_size > 0 {
        fragment_size
    } else {
        block_size
    };
    VolumeUsage {
        total: blocks * unit,
        free: available * unit,
    }
}

/// Best probe for the filesystem holding `path`.
pub fn probe_for(_path: &Path) -> Box<dyn SizeProbe> {
    #[cfg(target_os = "macos")]
    {
        if let Some(p) = apfs::ApfsProbe::new(_path) {
            return Box::new(p);
        }
    }
    #[cfg(target_os = "linux")]
    {
        if let Some(p) = linux::LinuxProbe::new(_path) {
            return Box::new(p);
        }
    }
    Box::new(fallback::FallbackProbe)
}

/// Measure one file without running a separate capability probe first.
/// Missing or unreadable files return an error; unavailable sharing information
/// remains unknown in the returned measurement.
pub fn measure_file(path: &Path) -> io::Result<FileSizing> {
    #[cfg(target_os = "macos")]
    {
        apfs::measure_file(path)
    }
    #[cfg(target_os = "linux")]
    {
        linux::measure_file(path)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        fallback::FallbackProbe.measure_file(path)
    }
}

/// A scratch directory for tests.
///
/// Behind a feature rather than `#[cfg(test)]` because `consumer`'s own tests
/// need it, and a dependency's test code is not compiled for its dependents.
#[cfg(any(test, feature = "testing"))]
pub mod testutil {
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};

    static SEQ: AtomicU32 = AtomicU32::new(0);

    /// A scratch directory removed on drop. Avoids a dev-dependency for a
    /// handful of tests.
    pub struct TempDir(PathBuf);

    impl TempDir {
        pub fn new(tag: &str) -> Self {
            let n = SEQ.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("kache-fs-{}-{tag}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).expect("create temp dir");
            // macOS exposes the temporary directory through `/var`, which is
            // a system symlink to `/private/var`. Keep fixtures in the same
            // canonical namespace that filesystem APIs and Git report.
            TempDir(std::fs::canonicalize(p).expect("canonicalize temp dir"))
        }

        pub fn path(&self) -> &Path {
            &self.0
        }

        /// Write a file of `bytes` with varied content.
        ///
        /// Deliberately not a repeated byte: APFS can store highly
        /// compressible data in far fewer blocks, which makes `allocated`
        /// unstable and size-ordering assertions flaky.
        pub fn write(&self, name: &str, bytes: usize) -> PathBuf {
            let p = self.0.join(name);
            let mut buf = Vec::with_capacity(bytes);
            let mut x: u32 = 0x9E37_79B9;
            while buf.len() < bytes {
                x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            buf.truncate(bytes);
            let mut file = std::fs::File::create(&p).expect("create file");
            file.write_all(&buf).expect("write");
            // Allocation probes need the writes on disk, including on APFS.
            // Windows requires the writable handle for this flush.
            file.sync_all().expect("sync");
            p
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testutil::TempDir;
    use super::*;

    const BLOCK: usize = 64 * 1024;

    #[test]
    fn confidence_labels_remain_distinct_for_consumers() {
        assert_eq!(Confidence::Exact.label(), "exact");
        assert_eq!(Confidence::LowerBound.label(), "lower_bound");
        assert_eq!(Confidence::Estimated.label(), "estimated");
    }

    #[test]
    fn empty_clone_samples_gain_identity_after_an_observation() {
        let mut sample = CloneSketch::default();
        assert!(sample.is_empty());
        assert!(sample.ids().is_empty());
        sample.insert(3, 27);
        assert!(!sample.is_empty());
        assert_eq!(sample.ids(), &[27]);
        assert_eq!(sample.shares_with(&CloneSketch::default()), 0);
    }

    #[test]
    fn volume_usage_handles_empty_and_overreported_free_space() {
        let occupied = VolumeUsage {
            total: 80,
            free: 20,
        };
        assert_eq!(occupied.used(), 60);
        assert_eq!(occupied.used_ratio(), 0.75);
        for usage in [
            VolumeUsage { total: 0, free: 0 },
            VolumeUsage { total: 0, free: 10 },
            VolumeUsage {
                total: 10,
                free: 20,
            },
        ] {
            assert_eq!(usage.used(), 0);
            assert_eq!(usage.used_ratio(), 0.0);
        }
    }

    #[test]
    fn volume_bytes_use_fragment_units_when_available() {
        let fragments = volume_bytes(3, 1, 4096, 16384);
        assert_eq!(fragments.total, 12288);
        assert_eq!(fragments.free, 4096);
        let blocks = volume_bytes(3, 1, 0, 16384);
        assert_eq!(blocks.total, 49152);
        assert_eq!(blocks.free, 16384);
    }

    #[cfg(unix)]
    #[test]
    fn volume_query_requires_a_real_path() {
        let dir = TempDir::new("volume-usage");
        let usage = volume_usage(dir.path()).unwrap();
        assert!(usage.total > 0);
        assert!(usage.free > 0);
        assert!(usage.free <= usage.total);
        assert!(volume_usage(&dir.path().join("missing")).is_none());
        assert!(volume_usage(Path::new("\0")).is_none());
    }

    #[cfg(not(unix))]
    #[test]
    fn unavailable_volume_usage_remains_unknown() {
        let dir = TempDir::new("volume-usage");
        assert!(volume_usage(dir.path()).is_none());
    }

    #[test]
    fn clone_samples_are_bounded_and_scoped_to_the_device() {
        let mut a = CloneSketch::default();
        let mut b = CloneSketch::default();
        for id in (0..100).rev() {
            a.insert(1, id);
        }
        for id in 0..100 {
            b.insert(1, id);
        }
        assert_eq!(a, b);
        assert_eq!(a.ids(), &(0..32).collect::<Vec<_>>());
        b.insert(1, 0);
        assert_eq!(a, b);
        let mut other_device = CloneSketch::default();
        other_device.insert(2, 0);
        assert_eq!(a.shares_with(&other_device), 0);
        other_device.insert(1, 0);
        assert_eq!(a.shares_with(&other_device), 1);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn legacy_clone_samples_cannot_prove_sharing() {
        let mut a = CloneSketch::default();
        a.insert(1, 42);
        let mut value = serde_json::to_value(a).unwrap();
        let round_trip: CloneSketch = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(a, round_trip);
        let mut uncounted = value.clone();
        uncounted["len"] = serde_json::json!(0);
        let mut uncounted: CloneSketch = serde_json::from_value(uncounted).unwrap();
        assert!(uncounted.is_empty());
        uncounted.insert(1, 42);
        assert_eq!(
            uncounted, a,
            "an unused slot cannot suppress an observation"
        );
        value.as_object_mut().unwrap().remove("devices");
        let legacy: CloneSketch = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(legacy.shares_with(&a), 0);
        value["len"] = serde_json::json!(1000);
        let mut malformed: CloneSketch = serde_json::from_value(value).unwrap();
        assert_eq!(malformed.shares_with(&a), 0);
        malformed.insert(1, 42);
        assert_eq!(malformed, a);
    }

    #[test]
    fn an_unreadable_directory_is_reported_not_silently_counted_as_empty() {
        // The dangerous failure: a permission error making a large tree look
        // like 0 bytes, i.e. "nothing to reclaim" instead of "cannot look".
        let d = TempDir::new("unreadable");
        let locked = d.path().join("locked");
        std::fs::create_dir_all(&locked).unwrap();
        std::fs::write(locked.join("payload"), vec![1u8; 8192]).unwrap();
        d.write("visible", 4096);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        }

        let probe = probe_for(d.path());
        let mut ledger = InodeLedger::new();
        let s = probe.measure_dir(d.path(), &mut ledger).unwrap();

        // Restore so the TempDir can clean itself up.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755));
        }

        #[cfg(unix)]
        if !nix_running_as_root() {
            assert_eq!(s.unreadable_dirs, 1, "the locked directory must be counted");
            assert!(s.is_partial(), "and the total must admit it is incomplete");
        }
        assert!(s.files >= 1, "what could be read is still measured");
    }

    /// Root can read anything, so the permission test proves nothing there.
    #[cfg(unix)]
    fn nix_running_as_root() -> bool {
        // SAFETY: getuid is always safe.
        unsafe { libc::getuid() == 0 }
    }

    #[test]
    fn a_fully_readable_tree_reports_no_gaps() {
        let d = TempDir::new("readable");
        d.write("a", 4096);
        let probe = probe_for(d.path());
        let mut ledger = InodeLedger::new();
        let s = probe.measure_dir(d.path(), &mut ledger).unwrap();

        assert_eq!(s.unreadable_dirs, 0);
        assert!(!s.is_partial());
    }

    #[test]
    fn hard_links_are_counted_once() {
        let d = TempDir::new("hardlink");
        let a = d.write("a", BLOCK);
        d.write("b", BLOCK);
        std::fs::hard_link(&a, d.path().join("a-link")).expect("hard link");

        let probe = probe_for(d.path());
        let mut ledger = InodeLedger::new();
        let s = probe.measure_dir(d.path(), &mut ledger).unwrap();

        // Three directory entries, two distinct inodes.
        assert_eq!(s.files, 2, "the second link must not be counted again");
        assert!(s.allocated >= 2 * BLOCK as u64);
        assert!(
            s.allocated < 3 * BLOCK as u64,
            "counting the link would inflate this to 3 blocks"
        );
        // Both names of the linked inode are inside the set, so nothing is
        // pinned from outside.
        assert_eq!(ledger.pinned_outside().inodes, 0);
    }

    #[test]
    fn links_reaching_outside_the_measured_set_are_reported() {
        let outer = TempDir::new("outside");
        let inner = outer.path().join("inner");
        std::fs::create_dir_all(&inner).unwrap();

        let a = outer.write("kept-outside", BLOCK);
        std::fs::hard_link(&a, inner.join("linked")).expect("hard link");

        let probe = probe_for(&inner);
        let mut ledger = InodeLedger::new();
        let s = probe.measure_dir(&inner, &mut ledger).unwrap();

        assert_eq!(s.files, 1);
        let pinned = ledger.pinned_outside();
        assert_eq!(pinned.inodes, 1, "the other name lives outside the walk");
        assert!(pinned.bytes >= BLOCK as u64);
        assert_eq!(s.unique, 0, "another name keeps the inode alive");
        assert_eq!(s.hardlink_pinned_inodes, 1);
        if probe.capabilities().unique_bytes {
            assert!(s.hardlink_pinned_bytes >= BLOCK as u64);
        } else {
            assert_eq!(s.hardlink_pinned_bytes, 0, "private allocation is unknown");
        }
    }

    #[test]
    fn symlinks_are_never_followed() {
        let d = TempDir::new("symlink");
        let _target = d.write("real", BLOCK);
        #[cfg(unix)]
        std::os::unix::fs::symlink(&_target, d.path().join("link")).expect("symlink");

        let probe = probe_for(d.path());
        let mut ledger = InodeLedger::new();
        let s = probe.measure_dir(d.path(), &mut ledger).unwrap();

        assert_eq!(s.files, 1, "the symlink must not add its target again");
    }

    #[test]
    fn nested_directories_are_summed() {
        let d = TempDir::new("nested");
        let sub = d.path().join("a/b/c");
        std::fs::create_dir_all(&sub).unwrap();
        d.write("top", BLOCK);
        std::fs::write(sub.join("deep"), vec![1u8; BLOCK]).unwrap();

        let probe = probe_for(d.path());
        let mut ledger = InodeLedger::new();
        let s = probe.measure_dir(d.path(), &mut ledger).unwrap();

        assert_eq!(s.files, 2);
        assert!(s.allocated >= 2 * BLOCK as u64);
    }

    #[test]
    fn an_unreadable_subtree_does_not_lose_the_rest_of_the_answer() {
        let d = TempDir::new("missing");
        d.write("present", BLOCK);

        let probe = probe_for(d.path());
        let mut ledger = InodeLedger::new();
        // A path that does not exist yields an empty measurement, not an error.
        let s = probe
            .measure_dir(&d.path().join("nope"), &mut ledger)
            .unwrap();
        assert_eq!(s.files, 0);
        assert_eq!(s.allocated, 0);
    }

    #[test]
    fn shared_ratio_is_withheld_when_the_platform_cannot_tell() {
        let estimated = DirSizing {
            allocated: 100,
            unique: 100,
            confidence: Confidence::Estimated,
            ..Default::default()
        };
        assert_eq!(estimated.shared_ratio(), None);

        let known = DirSizing {
            allocated: 100,
            unique: 25,
            confidence: Confidence::LowerBound,
            ..Default::default()
        };
        assert_eq!(known.shared_ratio(), Some(0.75));

        let empty = DirSizing {
            confidence: Confidence::Exact,
            ..Default::default()
        };
        assert_eq!(empty.shared_ratio(), None, "no divide by zero");
    }

    #[test]
    fn directory_totals_keep_private_shared_and_unknown_measurements_distinct() {
        struct FixtureProbe(Vec<FileSizing>);
        impl SizeProbe for FixtureProbe {
            fn measure_file(&self, _path: &Path) -> io::Result<FileSizing> {
                unreachable!("fixture supplies directory measurements")
            }

            fn capabilities(&self) -> ProbeCaps {
                ProbeCaps {
                    unique_bytes: true,
                    clone_refcount: false,
                    name: "fixture",
                }
            }

            fn read_dir(&self, _dir: &Path) -> io::Result<Vec<DirEntry>> {
                Ok(self.0.iter().copied().map(DirEntry::File).collect())
            }
        }

        let partial = FileSizing {
            logical: 10,
            allocated: 20,
            unique: Some(12),
            sharing: Sharing::Partial,
            inode: InodeId { dev: 3, ino: 1 },
            nlink: 1,
            clone_id: Some(7),
        };
        let mut probe = FixtureProbe(vec![
            partial,
            FileSizing {
                logical: 30,
                allocated: 40,
                unique: Some(0),
                sharing: Sharing::Full,
                inode: InodeId { dev: 3, ino: 2 },
                clone_id: Some(8),
                ..partial
            },
            FileSizing {
                logical: 50,
                allocated: 60,
                unique: Some(60),
                sharing: Sharing::None,
                inode: InodeId { dev: 3, ino: 3 },
                clone_id: Some(9),
                ..partial
            },
        ]);
        let known = probe
            .measure_dir(Path::new("."), &mut InodeLedger::new())
            .unwrap();
        assert_eq!(known.confidence, Confidence::LowerBound);
        assert_eq!(
            (known.files, known.logical, known.allocated, known.unique),
            (3, 90, 120, 72)
        );
        assert_eq!(known.fully_shared_files, 1);
        assert_eq!(known.clones.ids(), &[7, 8]);
        assert_eq!((known.unknown_files, known.unknown_allocated_bytes), (0, 0));

        probe.0.push(FileSizing {
            logical: 70,
            allocated: 80,
            unique: None,
            sharing: Sharing::Unknown,
            inode: InodeId { dev: 3, ino: 4 },
            clone_id: Some(10),
            ..partial
        });
        let unknown = probe
            .measure_dir(Path::new("."), &mut InodeLedger::new())
            .unwrap();
        assert_eq!(unknown.confidence, Confidence::Estimated);
        assert_eq!(
            (
                unknown.files,
                unknown.logical,
                unknown.allocated,
                unknown.unique
            ),
            (4, 160, 200, 72)
        );
        assert_eq!(
            (unknown.unknown_files, unknown.unknown_allocated_bytes),
            (1, 80)
        );
        assert_eq!(unknown.fully_shared_files, 1);
        assert_eq!(unknown.clones.ids(), &[7, 8]);
    }

    #[test]
    fn file_sizing_reports_shared_bytes_and_worthlessness() {
        let full = FileSizing {
            logical: 4096,
            allocated: 4096,
            unique: Some(0),
            sharing: Sharing::Full,
            inode: InodeId { dev: 1, ino: 1 },
            nlink: 1,
            clone_id: None,
        };
        assert_eq!(full.shared(), Some(4096));
        assert!(full.frees_nothing());

        let unknown = FileSizing {
            unique: None,
            sharing: Sharing::Unknown,
            ..full
        };
        assert_eq!(
            unknown.shared(),
            None,
            "unknown must not read as fully shared"
        );
        assert!(!unknown.frees_nothing());
        assert!(
            FileSizing {
                nlink: 2,
                ..unknown
            }
            .frees_nothing()
        );
    }

    /// The behaviour the whole module exists for, at directory level: a clone
    /// pair occupies one file's worth of blocks, and `du`-style summation of
    /// `allocated` would report two.
    #[cfg(target_os = "macos")]
    #[test]
    fn cloned_files_do_not_double_count_against_unique() {
        let d = TempDir::new("clone");
        let orig = d.write("orig", 4 * BLOCK);
        let st = std::process::Command::new("cp")
            .arg("-c")
            .arg(&orig)
            .arg(d.path().join("clone"))
            .status();
        if !matches!(st, Ok(s) if s.success()) {
            return; // no clonefile support here
        }

        let probe = probe_for(d.path());
        if !probe.capabilities().unique_bytes {
            return;
        }
        let mut ledger = InodeLedger::new();
        let s = probe.measure_dir(d.path(), &mut ledger).unwrap();

        assert_eq!(s.files, 2, "clones are independent inodes");
        assert!(s.allocated >= 8 * BLOCK as u64, "du bills both copies");
        assert!(
            s.unique < BLOCK as u64,
            "yet the blocks exist once, so almost nothing is private (unique={})",
            s.unique
        );
        // Both members report zero private bytes: sharing is symmetric, with no
        // "original" that owns the extents. Deleting either one alone frees
        // nothing — the bytes only return when the whole clone group goes, which
        // is why reclaim is modelled as a set function and not a per-item sum.
        assert_eq!(
            s.fully_shared_files, 2,
            "neither member frees anything alone"
        );
    }
}
