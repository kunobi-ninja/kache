use std::fs::File;
use std::io;
use std::path::Path;

/// A writable temporary file awaiting publication.
///
/// Create the stage in the destination directory, write through [`Self::writer`],
/// then choose [`Self::publish_new`] or [`Self::replace`]. Unpublished stages use
/// `tempfile`'s best-effort cleanup on drop, including when publication fails.
///
/// Callers own destination validation and durability requirements. Publication
/// does not sync file contents or the parent directory. If durable contents are
/// needed, sync the open file before publishing and handle directory durability
/// separately.
#[derive(Debug)]
pub struct StagedFile {
    file: tempfile::NamedTempFile,
    cloned: Option<Clone>,
}

#[derive(Debug)]
struct Clone {
    file: File,
    path: std::path::PathBuf,
    _directory: tempfile::TempDir,
}

impl StagedFile {
    /// Create a stage in an existing directory with the supplied filename prefix.
    ///
    /// On Unix, creation requests mode `0666`, filtered by the process umask and
    /// inherited directory ACLs, as for an ordinary writable file.
    pub fn new_in(directory: &Path, prefix: &str) -> io::Result<Self> {
        let mut builder = tempfile::Builder::new();
        builder.prefix(prefix);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o666));
        }
        builder
            .tempfile_in(directory)
            .map(|file| Self { file, cloned: None })
    }

    /// Access the temporary-file writer for streaming bytes into the stage.
    pub fn writer(&mut self) -> &mut impl io::Write {
        self.file_mut()
    }

    /// Access the open file for explicit synchronization or metadata changes.
    pub fn file_mut(&mut self) -> &mut File {
        match &mut self.cloned {
            Some(clone) => &mut clone.file,
            None => self.file.as_file_mut(),
        }
    }

    /// Clone cached bytes while retaining the stage's writable creation mode.
    /// A private sibling directory supplies the absent pathname required by APFS.
    pub fn try_clone_from(&mut self, source: &Path) -> io::Result<()> {
        let in_place = crate::try_clone_extents_into(source, self.file_mut());
        #[cfg(not(any(target_os = "macos", windows)))]
        {
            in_place
        }
        #[cfg(any(target_os = "macos", windows))]
        {
            if in_place.is_ok() {
                return Ok(());
            }
            let permissions = self.file_mut().metadata()?.permissions();
            let directory = tempfile::Builder::new()
                .prefix(".kache-clone-")
                .tempdir_in(
                    self.file
                        .path()
                        .parent()
                        .ok_or_else(|| io::Error::other("stage has no parent"))?,
                )?;
            let path = directory.path().join("artifact");
            crate::try_reflink(source, &path)?;
            std::fs::set_permissions(&path, permissions)?;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;
            self.cloned = Some(Clone {
                file,
                path,
                _directory: directory,
            });
            Ok(())
        }
    }

    /// Publish at an absent destination, refusing an existing directory entry.
    ///
    /// The destination must be on the stage's filesystem. This preserves
    /// `tempfile`'s create-only publication semantics without an existence check.
    pub fn publish_new(self, target: &Path) -> io::Result<()> {
        if let Some(clone) = self.cloned {
            return std::fs::hard_link(&clone.path, target);
        }
        self.file
            .persist_noclobber(target)
            .map(|_| ())
            .map_err(|error| error.error)
    }

    /// Publish at a destination, replacing an existing file entry if present.
    ///
    /// The destination must be on the stage's filesystem. The caller must decide
    /// whether replacement is permitted; this method does not inspect the target.
    pub fn replace(self, target: &Path) -> io::Result<()> {
        if let Some(clone) = self.cloned {
            return std::fs::rename(&clone.path, target);
        }
        self.file
            .persist(target)
            .map(|_| ())
            .map_err(|error| error.error)
    }
}

#[cfg(test)]
mod tests {
    use super::StagedFile;
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    #[test]
    fn cloned_stages_keep_writable_modes_and_publish_independent_bytes() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("blob");
        let probe = root.path().join("clone-probe");
        let target = root.path().join("output");
        fs::write(&source, b"cached").unwrap();
        if crate::try_reflink(&source, &probe).is_err() {
            return;
        }
        fs::remove_file(probe).unwrap();
        let mut stage = StagedFile::new_in(root.path(), ".stage-").unwrap();
        let expected = stage.file_mut().metadata().unwrap().permissions();
        stage.try_clone_from(&source).unwrap();
        assert_eq!(stage.file_mut().metadata().unwrap().permissions(), expected);
        stage.writer().write_all(b"edited").unwrap();
        assert_eq!(fs::read(&source).unwrap(), b"cached");
        stage.publish_new(&target).unwrap();
        assert_eq!(fs::read(target).unwrap(), b"edited");
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
    }

    #[test]
    fn cloning_a_missing_blob_keeps_the_existing_stage_bytes() {
        let root = tempfile::tempdir().unwrap();
        let mut stage = StagedFile::new_in(root.path(), ".stage-").unwrap();
        stage.writer().write_all(b"existing bytes").unwrap();
        assert!(stage.try_clone_from(&root.path().join("missing")).is_err());
        let target = root.path().join("output");
        stage.publish_new(&target).unwrap();
        assert_eq!(fs::read(target).unwrap(), b"existing bytes");
    }

    fn entries(directory: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<_> = fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        paths.sort();
        paths
    }

    #[test]
    fn dropping_an_unpublished_stage_removes_its_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut stage = StagedFile::new_in(dir.path(), ".staged-").unwrap();
        stage.writer().write_all(b"unfinished").unwrap();
        let paths = entries(dir.path());
        assert_eq!(paths.len(), 1);
        assert!(
            paths[0]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(".staged-")
        );
        assert_eq!(fs::read(&paths[0]).unwrap(), b"unfinished");
        drop(stage);
        assert!(entries(dir.path()).is_empty());
    }

    #[test]
    fn publication_installs_written_bytes_and_removes_the_stage() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("output");
        let mut stage = StagedFile::new_in(dir.path(), ".staged-").unwrap();
        stage.writer().write_all(b"complete output").unwrap();
        assert!(!target.exists());
        stage.publish_new(&target).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"complete output");
        assert_eq!(entries(dir.path()), vec![target]);
    }

    #[test]
    fn create_only_publication_preserves_a_race_winner() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("output");
        let mut stage = StagedFile::new_in(dir.path(), ".staged-").unwrap();
        stage.writer().write_all(b"staged output").unwrap();
        fs::write(&target, b"race winner").unwrap();
        assert_eq!(
            stage.publish_new(&target).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read(&target).unwrap(), b"race winner");
        assert_eq!(entries(dir.path()), vec![target]);
    }

    #[test]
    fn replacement_preserves_other_names_for_the_old_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("output");
        let alias = dir.path().join("alias");
        fs::write(&target, b"old output").unwrap();
        fs::hard_link(&target, &alias).unwrap();
        let mut stage = StagedFile::new_in(dir.path(), ".staged-").unwrap();
        stage.writer().write_all(b"new output").unwrap();
        stage.replace(&target).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"new output");
        assert_eq!(fs::read(&alias).unwrap(), b"old output");
        assert_eq!(entries(dir.path()), vec![alias, target]);
    }

    #[test]
    fn failed_publication_removes_the_stage() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("missing").join("output");
        for replace in [false, true] {
            let mut stage = StagedFile::new_in(dir.path(), ".staged-").unwrap();
            stage.writer().write_all(b"unpublished output").unwrap();
            let result = if replace {
                stage.replace(&target)
            } else {
                stage.publish_new(&target)
            };
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
            assert!(entries(dir.path()).is_empty());
        }
    }

    #[cfg(unix)]
    #[test]
    fn create_only_publication_refuses_a_dangling_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("output");
        let destination = dir.path().join("absent");
        std::os::unix::fs::symlink(&destination, &target).unwrap();
        let stage = StagedFile::new_in(dir.path(), ".staged-").unwrap();
        assert_eq!(
            stage.publish_new(&target).unwrap_err().kind(),
            std::io::ErrorKind::AlreadyExists
        );
        assert_eq!(fs::read_link(&target).unwrap(), destination);
        assert!(!destination.exists());
        assert_eq!(entries(dir.path()), vec![target]);
    }

    #[cfg(unix)]
    #[test]
    fn creation_uses_the_kernel_umask() {
        use std::os::unix::fs::PermissionsExt;
        const MARKER: &str = "KACHE_FS_STAGING_TEST_UMASK";
        if let Ok(mask) = std::env::var(MARKER) {
            let (mask, expected) = match mask.as_str() {
                "022" => (0o022, 0o644),
                "077" => (0o077, 0o600),
                _ => panic!("unexpected child fixture umask"),
            };
            // This branch runs only in the single-test child process below.
            unsafe {
                libc::umask(mask);
            }
            let dir = tempfile::tempdir().unwrap();
            let mut stage = StagedFile::new_in(dir.path(), ".staged-").unwrap();
            let mode = stage.file_mut().metadata().unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, expected);
            return;
        }

        for mask in ["022", "077"] {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "staging::tests::creation_uses_the_kernel_umask",
                    "--nocapture",
                ])
                .env(MARKER, mask)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "umask {mask}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        }
    }
}
