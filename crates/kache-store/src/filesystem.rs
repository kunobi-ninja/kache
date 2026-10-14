//! Filesystem identity and physical reclamation.

use crate::sharing::Sharing;
use serde::Serialize;
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub struct PathIdentity {
    pub device: u64,
    pub inode: u64,
}

/// Whether a directory recorded as `recorded` and found as `now` at the same
/// path is the same one on a renumbered volume: same inode, new device.
pub fn renumbered(recorded: PathIdentity, now: PathIdentity) -> bool {
    recorded.inode == now.inode && recorded.device != now.device
}

/// Stable directory identity used to reject moved or replaced tracked targets.
pub fn directory_identity(path: &Path) -> Option<PathIdentity> {
    kache_fs::directory_identity(path)
        .ok()
        .map(|id| PathIdentity {
            device: id.dev,
            inode: id.ino,
        })
}

/// What Cargo writes in `CACHEDIR.TAG` at the top of a target directory.
const CARGO_TAG: &[u8] = b"Signature: 8a477f597d28d172789f06886806bc55";
/// How much of `CACHEDIR.TAG` is read: the signature is in its first line.
const TAG_READ_BYTES: u64 = 512;
/// Open a tag without following a symlink or waiting on a FIFO.
#[cfg(unix)]
const OPEN_TAG_FLAGS: i32 = libc::O_NOFOLLOW | libc::O_NONBLOCK;

/// Whether Cargo tagged `target` as its build directory. Only a regular
/// `CACHEDIR.TAG` counts, opened without following a symlink or waiting on
/// a FIFO, and only its start is read, so a planted marker in a directory
/// discovery never built in cannot hang or exhaust the reader.
pub fn cargo_tagged(target: &Path) -> bool {
    use std::io::Read;
    let tag = target.join("CACHEDIR.TAG");
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(OPEN_TAG_FLAGS);
    }
    #[cfg(not(unix))]
    if special_tag_without_open_flags(&tag) {
        return false;
    }
    let Ok(file) = options.open(&tag) else {
        return false;
    };
    // A FIFO opens at once without a writer; it is not a tag.
    if !file.metadata().is_ok_and(|meta| meta.is_file()) {
        return false;
    }
    let mut start = Vec::new();
    file.take(TAG_READ_BYTES).read_to_end(&mut start).is_ok()
        && start
            .windows(CARGO_TAG.len())
            .any(|window| window == CARGO_TAG)
}

/// Without Unix's open flags, refuse a symlink or special file before
/// opening it.
#[cfg(not(unix))]
fn special_tag_without_open_flags(tag: &Path) -> bool {
    !std::fs::symlink_metadata(tag).is_ok_and(|meta| meta.file_type().is_file())
}

/// `path` with `.` dropped and each `..` cancelling the name before it, by
/// name alone. A `..` with no name before it stays, unless it is at the
/// root.
pub fn lexically_normal(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match normal.components().next_back() {
                Some(Component::Normal(_)) => {
                    normal.pop();
                }
                Some(Component::RootDir) => {}
                _ => normal.push(component),
            },
            _ => normal.push(component),
        }
    }
    if normal.as_os_str().is_empty() && !path.as_os_str().is_empty() {
        normal.push(Component::CurDir);
    }
    normal
}

/// How the target registry spells `path`: absolute, with `.` and `..`
/// resolved when that names the same directory, so a target named through
/// a directory deleted later (`<worktree>/../shared`) is still found. A
/// symlink before a `..` can make the resolved name another directory; then
/// the path stays as `std::path::absolute` spells it.
pub fn registry_path(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let lexical = lexically_normal(&absolute);
    let identity = directory_identity(&lexical);
    if identity.is_some() && identity == directory_identity(&absolute) {
        return Ok(lexical);
    }
    Ok(absolute)
}

/// A tracked cleanup target must be a derived directory, never a source root
/// or an ancestor of one.
pub fn target_root_is_safe(target: &Path, workspace_root: &Path) -> bool {
    let Ok(target) = std::path::absolute(target) else {
        return false;
    };
    let Ok(workspace) = std::path::absolute(workspace_root) else {
        return false;
    };
    let cargo_markers = cargo_tagged(&target)
        && (target.join(".rustc_info.json").is_file()
            || target.join("debug").is_dir()
            || target.join("release").is_dir());
    cargo_markers
        && target.parent().is_some()
        && target != workspace
        && !workspace.starts_with(&target)
        && directory_identity(&target).is_some()
}

/// Best-effort bytes the filesystem would reclaim by unlinking this blob.
pub fn blob_reclaimable_bytes(path: &Path) -> Option<u64> {
    retainer_from_meta(path).map(|r| r.private_bytes)
}

/// Would unlinking this store name leave the blob's blocks held by another
/// live file? Blocks held only by snapshots do not count: they are freed
/// once the snapshots are deleted, and keeping the entry would pin the store
/// over its size limit for as long as any snapshot exists.
pub fn blob_has_external_retainer(path: &Path) -> bool {
    retainer_from_meta(path).is_some_and(|r| r.held_by_live_file())
}

pub struct BlobRetainer {
    pub size: u64,
    /// Another live file (hardlink or clone) holds every block.
    pub cloned: bool,
    pub private_bytes: u64,
    /// Bytes only filesystem snapshots hold.
    pub snapshot_bytes: u64,
}

impl BlobRetainer {
    /// True when unlinking frees nothing now and nothing later either.
    pub fn held_by_live_file(&self) -> bool {
        self.private_bytes == 0 && self.snapshot_bytes == 0
    }
}

pub fn retainer_from_meta(path: &Path) -> Option<BlobRetainer> {
    let meta = std::fs::metadata(path).ok()?;
    if !meta.is_file() {
        return None;
    }
    let size = meta.len();
    let (sharing, links) = crate::sharing::probe_with_links(path, size);
    // A blob a build's target directory still hardlinks frees nothing when the
    // store drops its own name (kunobi-ninja/kache#725). This used to read
    // `MetadataExt::nlink` behind `#[cfg(unix)]`, so on Windows the guard did
    // not exist at all and GC evicted still-linked entries, reclaiming no disk
    // and destroying the hits. NTFS has hardlinks, `cache.windows_hardlink`
    // and `cache.shared_hardlink_restores` make them, and a link made by
    // anything else holds the blocks just the same.
    if links > 1 {
        return Some(BlobRetainer {
            size,
            cloned: true,
            private_bytes: 0,
            snapshot_bytes: 0,
        });
    }
    Some(retainer_from_sharing(size, sharing))
}

pub fn retainer_from_sharing(size: u64, sharing: Sharing) -> BlobRetainer {
    let cloned = sharing.shared && sharing.private_bytes == 0;
    BlobRetainer {
        size,
        cloned,
        private_bytes: if cloned {
            0
        } else {
            sharing.private_bytes.min(size)
        },
        snapshot_bytes: sharing.snapshot_bytes,
    }
}

/// Whether any execute bit is set. Always `false` off Unix, where files carry
/// no execute bit.
pub fn is_executable(metadata: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_new_device_with_the_same_inode_is_a_renumbered_volume() {
        let id = |device, inode| PathIdentity { device, inode };
        assert!(renumbered(id(16_777_235, 7), id(16_777_232, 7)));
        assert!(!renumbered(id(1, 7), id(1, 7)), "unchanged");
        assert!(
            !renumbered(id(1, 7), id(1, 8)),
            "replaced on the same volume"
        );
        assert!(
            !renumbered(id(1, 7), id(2, 8)),
            "replaced on another volume"
        );
    }

    #[cfg(unix)]
    #[test]
    fn only_a_regular_tag_marks_a_cargo_target() {
        let dir = tempfile::tempdir().unwrap();
        let target = |name: &str| {
            let target = dir.path().join(name);
            std::fs::create_dir_all(&target).unwrap();
            target
        };
        let signature = "Signature: 8a477f597d28d172789f06886806bc55";

        let tagged = target("tagged");
        std::fs::write(tagged.join("CACHEDIR.TAG"), format!("{signature}\n# Cargo")).unwrap();
        assert!(cargo_tagged(&tagged));

        let late = target("late");
        let padding = " ".repeat(TAG_READ_BYTES as usize);
        std::fs::write(late.join("CACHEDIR.TAG"), format!("{padding}{signature}")).unwrap();
        assert!(!cargo_tagged(&late), "only the start is read");

        // A symlink to a tag, or to something endless, is not followed.
        let linked = target("linked");
        std::os::unix::fs::symlink(tagged.join("CACHEDIR.TAG"), linked.join("CACHEDIR.TAG"))
            .unwrap();
        assert!(!cargo_tagged(&linked));
        let endless = target("endless");
        std::os::unix::fs::symlink("/dev/zero", endless.join("CACHEDIR.TAG")).unwrap();
        assert!(!cargo_tagged(&endless));

        // A FIFO with no writer would block a read forever.
        let fifo = target("fifo");
        let path = std::ffi::CString::new(fifo.join("CACHEDIR.TAG").as_os_str().as_encoded_bytes())
            .unwrap();
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        assert!(!cargo_tagged(&fifo));

        assert!(!cargo_tagged(&target("untagged")));
    }

    #[cfg(unix)]
    #[test]
    fn is_executable_reads_any_execute_bit() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        std::fs::write(&file, "x").unwrap();
        for (mode, expected) in [
            (0o644, false),
            (0o600, false),
            (0o755, true),
            (0o700, true),
            (0o010, true),
        ] {
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(mode)).unwrap();
            assert_eq!(
                is_executable(&std::fs::metadata(&file).unwrap()),
                expected,
                "mode {mode:o}"
            );
        }
    }

    #[test]
    fn retainer_from_sharing_treats_fully_cloned_as_unreclaimable() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: true,
                private_bytes: 0,
                snapshot_bytes: 0,
            },
        );
        assert!(r.cloned);
        assert_eq!(r.private_bytes, 0);
    }

    #[test]
    fn retainer_from_sharing_treats_private_file_as_reclaimable() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: false,
                private_bytes: 4096,
                snapshot_bytes: 0,
            },
        );
        assert!(!r.cloned);
        assert_eq!(r.private_bytes, 4096);
    }

    #[test]
    fn snapshot_only_blobs_do_not_block_eviction() {
        let snapshot = retainer_from_sharing(
            4096,
            Sharing {
                shared: false,
                private_bytes: 0,
                snapshot_bytes: 4096,
            },
        );
        assert!(!snapshot.cloned, "no live file holds the blocks");
        assert_eq!(snapshot.private_bytes, 0, "nothing frees right away");
        assert_eq!(snapshot.snapshot_bytes, 4096);
        assert!(!snapshot.held_by_live_file());

        let cloned = retainer_from_sharing(
            4096,
            Sharing {
                shared: true,
                private_bytes: 0,
                snapshot_bytes: 0,
            },
        );
        assert!(cloned.held_by_live_file());

        let private = retainer_from_sharing(
            4096,
            Sharing {
                shared: false,
                private_bytes: 4096,
                snapshot_bytes: 0,
            },
        );
        assert!(!private.held_by_live_file());
    }

    #[test]
    fn retainer_preserves_partial_reclaim_measurement() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: true,
                private_bytes: 1024,
                snapshot_bytes: 0,
            },
        );
        assert!(!r.cloned, "partly private blobs can reclaim some disk");
        assert_eq!(r.private_bytes, 1024);
    }

    /// The #725 guard on every platform kache ships on. Hardlinks exist on
    /// NTFS too, and the count must not come from `MetadataExt`, which only
    /// Unix has — a Unix-gated guard let Windows GC evict entries whose blobs
    /// a target directory still held, freeing nothing.
    #[test]
    fn a_hardlinked_blob_reclaims_nothing_and_is_reported_as_retained() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.bin");
        std::fs::write(&blob, vec![7u8; 4096]).unwrap();

        let alone = retainer_from_meta(&blob).expect("a plain file measures");
        assert!(!alone.cloned, "an unlinked blob is reclaimable");
        assert_eq!(alone.private_bytes, 4096);
        assert!(!blob_has_external_retainer(&blob));
        assert_eq!(blob_reclaimable_bytes(&blob), Some(4096));

        std::fs::hard_link(&blob, dir.path().join("target-copy.bin")).unwrap();
        let linked = retainer_from_meta(&blob).expect("a hardlinked file measures");
        assert!(linked.cloned, "a second name still holds every block");
        assert_eq!(linked.size, 4096, "the logical size is still reported");
        assert_eq!(linked.private_bytes, 0);
        assert!(blob_has_external_retainer(&blob));
        assert_eq!(blob_reclaimable_bytes(&blob), Some(0));

        assert_eq!(
            retainer_from_meta(dir.path()).map(|r| r.size),
            None,
            "a directory is not a blob"
        );
        assert_eq!(blob_reclaimable_bytes(&dir.path().join("missing")), None);
    }

    #[test]
    fn cleanup_target_cannot_be_a_source_root_or_its_ancestor() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let target = workspace.join("target");
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        std::fs::write(target.join(".rustc_info.json"), "{}").unwrap();

        assert!(target_root_is_safe(&target, &workspace));
        assert!(!target_root_is_safe(&workspace, &workspace));
        assert!(!target_root_is_safe(dir.path(), &workspace));

        let missing_markers = workspace.join("missing-markers");
        std::fs::create_dir_all(&missing_markers).unwrap();
        assert!(!target_root_is_safe(&missing_markers, &workspace));

        let tag_only = workspace.join("tag-only");
        std::fs::create_dir_all(&tag_only).unwrap();
        std::fs::write(
            tag_only.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        assert!(!target_root_is_safe(&tag_only, &workspace));

        let build_dir_marker = workspace.join("build-dir-marker");
        std::fs::create_dir_all(build_dir_marker.join("debug")).unwrap();
        std::fs::write(
            build_dir_marker.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55",
        )
        .unwrap();
        assert!(target_root_is_safe(&build_dir_marker, &workspace));

        let file = workspace.join("ordinary-file");
        std::fs::write(&file, "x").unwrap();
        assert_eq!(directory_identity(&file), None);
        assert_eq!(directory_identity(&workspace.join("missing")), None);
        assert!(directory_identity(&workspace).is_some());
    }

    #[test]
    fn the_registry_resolves_dot_dot_by_name_unless_a_symlink_says_otherwise() {
        let dir = tempfile::tempdir().unwrap();
        let root = std::path::absolute(dir.path()).unwrap();
        let shared = root.join("shared");
        std::fs::create_dir_all(shared.join("debug")).unwrap();
        std::fs::create_dir_all(root.join("worktree")).unwrap();
        let through_worktree = root.join("worktree/../shared/./debug/..");
        assert_eq!(registry_path(&through_worktree).unwrap(), shared);
        // Nothing on disk to compare: the path stays as `absolute` spells it,
        // which on Windows has already resolved the `..`.
        let missing = root.join("gone/../shared");
        assert_eq!(
            registry_path(&missing).unwrap(),
            std::path::absolute(&missing).unwrap()
        );

        #[cfg(unix)]
        {
            let inner = root.join("elsewhere/inner");
            std::fs::create_dir_all(root.join("elsewhere/shared")).unwrap();
            std::fs::create_dir_all(&inner).unwrap();
            std::os::unix::fs::symlink(&inner, root.join("link")).unwrap();
            // `..` after a symlink leaves the link's target, so the name
            // `root/shared` would be another directory.
            let through_link = root.join("link/../shared");
            assert_eq!(registry_path(&through_link).unwrap(), through_link);
            std::os::unix::fs::symlink(root.join("elsewhere"), root.join("alias")).unwrap();
            assert_eq!(
                registry_path(&root.join("alias/inner/../shared")).unwrap(),
                root.join("alias/shared")
            );
        }
    }

    #[test]
    fn a_dot_dot_cancels_only_a_name_before_it() {
        for (path, normal) in [
            ("/../a/../b", "/b"),
            ("/a/./b/../c", "/a/c"),
            ("../a/./b/..", "../a"),
            ("../..", "../.."),
            ("../../target", "../../target"),
            ("./../target", "../target"),
            ("a/../../b", "../b"),
            ("a/..", "."),
            ("", ""),
        ] {
            assert_eq!(
                lexically_normal(Path::new(path)),
                Path::new(normal),
                "{path}"
            );
        }
    }
}
