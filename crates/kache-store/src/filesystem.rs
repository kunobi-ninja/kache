//! Filesystem identity and physical reclamation.

use crate::sharing::Sharing;
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
pub struct PathIdentity {
    pub device: u64,
    pub inode: u64,
}

/// Stable directory identity used to reject moved or replaced tracked targets.
pub fn directory_identity(path: &Path) -> Option<PathIdentity> {
    let meta = std::fs::symlink_metadata(path).ok()?;
    if !meta.file_type().is_dir() {
        return None;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(PathIdentity {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, GetFileInformationByHandle,
        };

        let directory = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
            .ok()?;
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        let ok = unsafe { GetFileInformationByHandle(directory.as_raw_handle() as _, &mut info) };
        win32_call_succeeded(ok).then(|| {
            windows_path_identity_from_parts(
                info.dwVolumeSerialNumber,
                info.nFileIndexHigh,
                info.nFileIndexLow,
            )
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

#[cfg_attr(not(windows), allow(dead_code))]
pub fn win32_call_succeeded(result: i32) -> bool {
    result != 0
}

#[cfg_attr(not(windows), allow(dead_code))]
pub fn windows_path_identity_from_parts(
    volume_serial: u32,
    file_index_high: u32,
    file_index_low: u32,
) -> PathIdentity {
    PathIdentity {
        device: u64::from(volume_serial),
        inode: (u64::from(file_index_high) << 32).saturating_add(u64::from(file_index_low)),
    }
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
    let cargo_markers = std::fs::read_to_string(target.join("CACHEDIR.TAG"))
        .is_ok_and(|tag| tag.contains("Signature: 8a477f597d28d172789f06886806bc55"))
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

/// Would unlinking this store name free none of the blob's blocks?
pub fn blob_has_external_retainer(path: &Path) -> bool {
    blob_reclaimable_bytes(path) == Some(0)
}

pub struct BlobRetainer {
    pub size: u64,
    pub cloned: bool,
    pub private_bytes: u64,
}

pub fn retainer_from_meta(path: &Path) -> Option<BlobRetainer> {
    let meta = std::fs::metadata(path).ok()?;
    if !meta.is_file() {
        return None;
    }
    let size = meta.len();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if meta.nlink() > 1 {
            return Some(BlobRetainer {
                size,
                cloned: true,
                private_bytes: 0,
            });
        }
    }
    let sharing = crate::sharing::probe(path, size);
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_directory_identity_preserves_the_full_file_index() {
        assert!(!win32_call_succeeded(0));
        assert!(win32_call_succeeded(1));
        let identity = windows_path_identity_from_parts(0x1020_3040, 0x1122_3344, 0x5566_7788);
        assert_eq!(identity.device, 0x1020_3040);
        assert_eq!(identity.inode, 0x1122_3344_5566_7788);
    }

    #[test]
    fn retainer_from_sharing_treats_fully_cloned_as_unreclaimable() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: true,
                private_bytes: 0,
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
            },
        );
        assert!(!r.cloned);
        assert_eq!(r.private_bytes, 4096);
    }

    #[test]
    fn retainer_preserves_partial_reclaim_measurement() {
        let r = retainer_from_sharing(
            4096,
            Sharing {
                shared: true,
                private_bytes: 1024,
            },
        );
        assert!(!r.cloned, "partly private blobs can reclaim some disk");
        assert_eq!(r.private_bytes, 1024);
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
}
