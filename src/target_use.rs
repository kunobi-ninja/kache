//! Process-held lock that keeps target cleanup out of running Cargo commands.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::Path;

/// Unlock explicitly before closing: a concurrent fork can briefly inherit
/// the descriptor, even though it closes on exec.
pub(crate) struct Lease(File);

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn open(cache_dir: &Path) -> io::Result<File> {
    std::fs::create_dir_all(cache_dir)?;
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(cache_dir.join("target-use.lock"))
}

/// Hold while Cargo or a target runner may still use build output.
pub(crate) fn shared(cache_dir: &Path) -> io::Result<Lease> {
    let file = open(cache_dir)?;
    file.lock_shared()?;
    Ok(Lease(file))
}

/// Reserve deletion without waiting for a running command.
pub(crate) fn try_exclusive(cache_dir: &Path) -> io::Result<Option<Lease>> {
    let file = open(cache_dir)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(Lease(file))),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_running_command_blocks_cleanup_until_it_exits() {
        let dir = tempfile::tempdir().unwrap();
        let first = shared(dir.path()).unwrap();
        let second = shared(dir.path()).unwrap();
        assert!(try_exclusive(dir.path()).unwrap().is_none());
        drop(first);
        assert!(try_exclusive(dir.path()).unwrap().is_none());
        drop(second);
        assert!(try_exclusive(dir.path()).unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn closing_a_lease_releases_an_inherited_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let lease = shared(dir.path()).unwrap();
        let inherited = lease.0.try_clone().unwrap();
        drop(lease);
        assert!(try_exclusive(dir.path()).unwrap().is_some());
        drop(inherited);
    }
}
