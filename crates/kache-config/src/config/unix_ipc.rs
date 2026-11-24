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
pub fn shared_store_socket(store: &Path) -> Result<Option<PathBuf>> {
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
    bail!("no usable private account directory");
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
    unsafe {
        libc::getpwuid_r(
            uid,
            entry.as_mut_ptr(),
            buffer.as_mut_ptr().cast(),
            buffer.len(),
            &mut result,
        )
    };
    // POSIX sets result to null when no record is available, including on errors.
    let result = std::ptr::NonNull::new(result)?;
    // SAFETY: the record and backing buffer remain live until the path is copied.
    let raw = std::ptr::NonNull::new(unsafe { result.as_ref().pw_dir })?;
    let home = Path::new(OsStr::from_bytes(
        unsafe { CStr::from_ptr(raw.as_ptr()) }.to_bytes(),
    ));
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
    const CREATE_ENTRY: libc::c_int = libc::W_OK | libc::X_OK;
    if unsafe {
        libc::faccessat(
            libc::AT_FDCWD,
            parent.as_ptr(),
            CREATE_ENTRY,
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
    fn canonical_identity_rejects_obstructions_and_checks_hidden_ancestors() {
        let fixture = tempfile::Builder::new()
            .prefix("k")
            .tempdir_in("/tmp")
            .unwrap();
        let root = fs::canonicalize(fixture.path()).unwrap();
        let uid = fs::metadata(&root).unwrap().uid();
        let shared = root.join("shared");
        let nested = shared.join("nested");
        fs::create_dir_all(&nested).unwrap();
        chmod(&shared, 0o770);
        let link = root.join("link");
        symlink(&nested, &link).unwrap();
        assert_eq!(
            unsafe_store_ancestor(&link.join("store"), uid, true).unwrap(),
            Some(shared)
        );
        assert_eq!(unsafe_store_ancestor(&link, uid, true).unwrap(), Some(link));
        let missing = root.join("missing/../tail");
        assert_eq!(canonical_store_path(&missing).unwrap(), root.join("tail"));
        let file = root.join("file");
        fs::write(&file, "").unwrap();
        assert_eq!(check_path(&file, uid, false).unwrap().0, Some(file.clone()));
        assert!(canonical_store_path(&file.join("child")).is_err());
        assert!(check_path(&file.join("child"), uid, true).is_err());
    }

    #[test]
    fn private_anchor_accepts_last_usable_socket_byte() {
        let fixture = tempfile::Builder::new()
            .prefix("k")
            .tempdir_in("/tmp")
            .unwrap();
        let root = fs::canonicalize(fixture.path()).unwrap();
        let uid = fs::metadata(&root).unwrap().uid();
        let key = "0123456789abcdef0123456789abcdef";
        let used = root
            .join("x/.kache-ipc")
            .join(key)
            .join("d-pr-ffffffff.sock")
            .as_os_str()
            .as_bytes()
            .len();
        let base = root.join("x".repeat(104 - used));
        fs::create_dir(&base).unwrap();
        let socket = socket_under(&base, key, uid).unwrap();
        assert_eq!(
            socket
                .with_file_name("d-pr-ffffffff.sock")
                .as_os_str()
                .as_bytes()
                .len(),
            103
        );
        let longer =
            base.with_file_name(format!("{}x", base.file_name().unwrap().to_str().unwrap()));
        fs::create_dir(&longer).unwrap();
        assert!(socket_under(&longer, key, uid).is_none());
        if uid != 0 {
            chmod(&base, 0o500);
            assert!(socket_under(&base, key, uid).is_none());
        }
    }

    #[test]
    fn account_lookup_uses_the_uid_record() {
        // Small build sandboxes can omit a passwd record for the build user.
        // Check a record the system actually provides.
        if let Ok(passwd) = fs::read_to_string("/etc/passwd")
            && let Some(home) = passwd
                .lines()
                .map(|line| line.split(':').collect::<Vec<_>>())
                .find(|fields| fields.get(2) == Some(&"0"))
                .and_then(|fields| fields.get(5).copied())
        {
            assert_eq!(account_home(0).as_deref(), Some(Path::new(home)));
        }
        assert!(account_home(u32::MAX).is_none());
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
