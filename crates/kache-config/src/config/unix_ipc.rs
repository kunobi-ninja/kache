//! Unix daemon endpoint selection. Store data and event logs stay on their configured paths.

use anyhow::{Result, bail};
use std::ffi::{CStr, OsStr};
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

fn ipc_ancestor_is_safe(owner: u32, mode: u32, uid: u32, ancestor: &Path, store: &Path) -> bool {
    if owner == uid || owner == 0 {
        return mode & 0o022 == 0 || (ancestor != store && mode & 0o1000 != 0);
    }
    // In a Linux user namespace the host's root UID can be unmapped. A
    // direct mount beneath /, such as Nix's /build, cannot be renamed or
    // unlinked by its parent owner. Check the mount entry itself, not a link.
    #[cfg(target_os = "linux")]
    if ancestor == Path::new("/") && mode & 0o022 == 0 {
        return store
            .ancestors()
            .find(|path| path.parent() == Some(ancestor))
            .is_some_and(is_ipc_mount_root);
    }
    false
}

#[cfg(target_os = "linux")]
fn is_ipc_mount_root(path: &Path) -> bool {
    use rustix::fs::{AtFlags, CWD, StatxAttributes, StatxFlags, statx};
    statx(CWD, path, AtFlags::SYMLINK_NOFOLLOW, StatxFlags::TYPE).is_ok_and(|stat| {
        (stat.stx_attributes & stat.stx_attributes_mask).contains(StatxAttributes::MOUNT_ROOT)
    })
}

/// Return an alternate unscoped socket only for stores that cannot protect IPC.
pub fn shared_store_socket(store: &Path) -> Option<PathBuf> {
    select_socket(store).unwrap_or_else(|error| {
        tracing::debug!(%error, "automatic daemon endpoint is unavailable");
        // All consumers use filesystem paths, including clients that probe the
        // daemon before falling back to local compilation. A child of the POSIX
        // null device can neither be created nor connected to.
        Some(Path::new("/dev/null").join("d.sock"))
    })
}

fn select_socket(store: &Path) -> Result<Option<PathBuf>> {
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    select_socket_with(store, uid, || {
        #[cfg(target_os = "linux")]
        let runtime = Some(Path::new("/run/user").join(uid.to_string()));
        #[cfg(not(target_os = "linux"))]
        let runtime = None;
        [account_home(uid), runtime]
    })
}

fn select_socket_with(
    store: &Path,
    uid: u32,
    anchors: impl FnOnce() -> [Option<PathBuf>; 2],
) -> Result<Option<PathBuf>> {
    if unsafe_store_ancestor(store, uid, true)?.is_none() {
        return Ok(None);
    }

    let identity = canonical_store_path(store)?;
    let mut hash = blake3::Hasher::new();
    hash.update(&uid.to_le_bytes());
    hash.update(identity.as_os_str().as_bytes());
    let digest = hash.finalize().to_hex();
    let key = &digest[..32];
    if let Some(socket) = anchors()
        .into_iter()
        .flatten()
        .find_map(|base| socket_under(&base, key, uid))
    {
        return Ok(Some(socket));
    }
    bail!(
        "no secure private daemon endpoint for shared store {}; set KACHE_RUNTIME_DIR or KACHE_SOCKET_PATH to a private location",
        store.display()
    );
}

/// Recheck the actual path before acquiring coordination locks in the store.
pub fn check_store_ancestry(store: &Path, uid: u32) -> Result<()> {
    if let Some(ancestor) = unsafe_store_ancestor(store, uid, false)? {
        bail!(
            "daemon IPC path {} can be changed by another user; set KACHE_RUNTIME_DIR to a private directory",
            ancestor.display()
        );
    }
    Ok(())
}

fn unsafe_store_ancestor(store: &Path, uid: u32, allow_missing: bool) -> Result<Option<PathBuf>> {
    let (unsafe_path, linked) = check_path(store, uid, allow_missing)?;
    if unsafe_path.is_some() || !linked {
        return Ok(unsafe_path);
    }
    Ok(check_path(&canonical_store_path(store)?, uid, allow_missing)?.0)
}

/// An absent store inherits the trust of its existing ancestors. A symlink in
/// the spelling of the path requires the same check on its resolved ancestors.
fn check_path(root: &Path, uid: u32, allow_missing: bool) -> io::Result<(Option<PathBuf>, bool)> {
    let mut linked = false;
    for path in root.ancestors() {
        let meta = match fs::symlink_metadata(path) {
            Ok(meta) => meta,
            Err(error) if allow_missing && error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let meta = if meta.is_symlink() {
            if path == root {
                return Ok((Some(path.to_owned()), true));
            }
            linked = true;
            fs::metadata(path)?
        } else {
            meta
        };
        if !meta.is_dir() || !ipc_ancestor_is_safe(meta.uid(), meta.mode(), uid, path, root) {
            return Ok((Some(path.to_owned()), linked));
        }
    }
    Ok((None, linked))
}

fn canonical_store_path(store: &Path) -> Result<PathBuf> {
    for ancestor in store.ancestors() {
        match fs::canonicalize(ancestor) {
            Ok(mut resolved) => {
                for component in store.strip_prefix(ancestor)?.components() {
                    match component {
                        Component::Normal(name) => resolved.push(name),
                        Component::ParentDir => {
                            resolved.pop();
                        }
                        _ => {}
                    }
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    bail!("cannot resolve store {}", store.display())
}

/// The account home comes from the effective UID's passwd record. The
/// interactive shell and login manager can supply different HOME and TMPDIR.
fn account_home(uid: u32) -> Option<PathBuf> {
    let mut entry = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    let mut buffer = vec![0u8; 16 * 1024];
    // SAFETY: entry and result are writable, and buffer lives until pw_dir is copied.
    let status = unsafe {
        libc::getpwuid_r(
            uid,
            entry.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    if status != 0 || result.is_null() {
        return None;
    }
    // SAFETY: a successful lookup initializes entry; pw_dir points into buffer.
    let raw = unsafe { entry.assume_init().pw_dir };
    if raw.is_null() {
        return None;
    }
    let home = Path::new(OsStr::from_bytes(unsafe { CStr::from_ptr(raw) }.to_bytes()));
    home.is_absolute().then(|| home.to_owned())
}

fn socket_under(base: &Path, key: &str, uid: u32) -> Option<PathBuf> {
    let base = fs::canonicalize(base).ok()?;
    let meta = fs::metadata(&base).ok()?;
    if meta.uid() != uid || check_path(&base, uid, false).ok()?.0.is_some() {
        return None;
    }

    let namespace = base.join(".kache-ipc");
    let instance = namespace.join(key);
    let mut existing_parent = base.as_path();
    for path in [&namespace, &instance] {
        match fs::symlink_metadata(path) {
            Ok(meta) if meta.is_dir() && meta.uid() == uid && meta.mode() & 0o7777 == 0o700 => {
                existing_parent = path;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            _ => return None,
        }
    }
    let parent = std::ffi::CString::new(existing_parent.as_os_str().as_bytes()).ok()?;
    // SAFETY: parent is a live NUL-terminated path. faccessat uses the same
    // effective credentials as the daemon and accounts for read-only mounts.
    if unsafe {
        libc::faccessat(
            libc::AT_FDCWD,
            parent.as_ptr(),
            libc::W_OK | libc::X_OK,
            libc::AT_EACCESS,
        )
    } != 0
    {
        return None;
    }
    // macOS and BSD allow 103 bytes plus the terminating NUL. Leave room for
    // the longest scoped name before accepting this anchor for any caller.
    if instance
        .join("d-pr-ffffffff.sock")
        .as_os_str()
        .as_encoded_bytes()
        .len()
        > 103
    {
        return None;
    }
    Some(instance.join("d.sock"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn chmod(path: &Path, mode: u32) {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    }

    #[test]
    fn shared_store_identity_survives_creation_and_ancestor_aliases() {
        let fixture = tempfile::Builder::new()
            .prefix("k")
            .tempdir_in("/tmp")
            .unwrap();
        let root = fs::canonicalize(fixture.path()).unwrap();
        let uid = fs::metadata(&root).unwrap().uid();
        let anchor = root.join("user");
        let shared = root.join("shared");
        fs::create_dir(&anchor).unwrap();
        fs::create_dir(&shared).unwrap();
        chmod(&shared, 0o770);
        let select =
            |path: &Path| select_socket_with(path, uid, || [Some(anchor.clone()), None]).unwrap();
        assert_eq!(select(&root.join("private")), None);
        let store = shared.join("one/cache");
        let socket = select(&store).unwrap();
        assert!(socket.starts_with(anchor.join(".kache-ipc")));
        assert_ne!(select(&shared.join("two/cache")).unwrap(), socket);
        assert!(!store.exists());
        assert!(!socket.parent().unwrap().exists());
        fs::create_dir_all(&store).unwrap();
        let alias = root.join("link");
        symlink(&shared, &alias).unwrap();
        assert_eq!(select(&alias.join("one/cache")).unwrap(), socket);
        assert_eq!(select(&store).unwrap(), socket);
        assert!(check_store_ancestry(&store, uid).is_err());
        assert!(check_store_ancestry(&root.join("missing"), uid).is_err());
        chmod(&shared, 0o755);
        assert_eq!(select(&store), None);
        assert!(check_store_ancestry(&store, uid).is_ok());
    }

    #[test]
    fn private_anchor_requires_safe_existing_directories_and_room_for_scope() {
        let fixture = tempfile::Builder::new()
            .prefix("k")
            .tempdir_in("/tmp")
            .unwrap();
        let root = fs::canonicalize(fixture.path()).unwrap();
        let uid = fs::metadata(&root).unwrap().uid();
        let key = "0123456789abcdef0123456789abcdef";
        chmod(&root, 0o700);
        let select = || socket_under(&root, key, uid);
        let socket = select().unwrap();
        assert_eq!(socket, root.join(".kache-ipc").join(key).join("d.sock"));
        assert!(socket_under(&root, key, uid.wrapping_add(1)).is_none());
        let long = root.join("x".repeat(80));
        fs::create_dir(&long).unwrap();
        assert!(socket_under(&long, key, uid).is_none());
        let namespace = root.join(".kache-ipc");
        let instance = namespace.join(key);
        fs::create_dir(&namespace).unwrap();
        chmod(&namespace, 0o755);
        assert!(select().is_none());
        chmod(&namespace, 0o700);
        symlink(&root, &instance).unwrap();
        assert!(select().is_none());
        fs::remove_file(&instance).unwrap();
        fs::create_dir(&instance).unwrap();
        chmod(&instance, 0o700);
        assert_eq!(select().unwrap(), socket);
        chmod(&root, 0o770);
        assert!(select().is_none());
        assert!(select_socket_with(&root, uid, || [Some(long), None]).is_err());
    }

    #[test]
    fn ipc_path_owners_and_sticky_permissions() {
        let me = 1001;
        let store = Path::new("/parent/store");
        let parent = store.parent().unwrap();
        for owner in [0, me] {
            assert!(ipc_ancestor_is_safe(owner, 0o755, me, store, store));
            assert!(ipc_ancestor_is_safe(owner, 0o1777, me, parent, store));
            assert!(!ipc_ancestor_is_safe(owner, 0o1777, me, store, store));
            assert!(!ipc_ancestor_is_safe(owner, 0o775, me, parent, store));
            assert!(!ipc_ancestor_is_safe(owner, 0o707, me, store, store));
        }
        assert!(!ipc_ancestor_is_safe(1002, 0o700, me, store, store));
        assert!(!ipc_ancestor_is_safe(1002, 0o1777, me, parent, store));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_foreign_namespace_root_needs_a_direct_mount_for_ipc() {
        use std::os::unix::fs::symlink;
        let root = Path::new("/");
        let procfs = Path::new("/proc");
        let nested = procfs.join("self");
        assert!(ipc_ancestor_is_safe(65_534, 0o750, 1000, root, &nested));
        assert!(!ipc_ancestor_is_safe(65_534, 0o770, 1000, root, &nested));
        assert!(!ipc_ancestor_is_safe(65_534, 0o750, 1000, procfs, &nested));
        assert!(!ipc_ancestor_is_safe(65_534, 0o750, 1000, root, root));
        assert!(!ipc_ancestor_is_safe(
            65_534,
            0o750,
            1000,
            root,
            Path::new("/no-such-kache-fixture")
        ));

        let dir = tempfile::tempdir().unwrap();
        let link = dir.path().join("proc");
        symlink(procfs, &link).unwrap();
        assert!(!is_ipc_mount_root(&link));
        assert!(!is_ipc_mount_root(dir.path()));
    }
}
