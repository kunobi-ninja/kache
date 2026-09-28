//! Process-held lock that keeps target cleanup out of running Cargo commands.

use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::Path;

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
pub(crate) fn shared(cache_dir: &Path) -> io::Result<File> {
    let file = open(cache_dir)?;
    file.lock_shared()?;
    Ok(file)
}

/// Reserve deletion without waiting for a running command.
pub(crate) fn try_exclusive(cache_dir: &Path) -> io::Result<Option<File>> {
    let file = open(cache_dir)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
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
}
