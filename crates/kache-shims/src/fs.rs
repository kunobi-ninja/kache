//! The filesystem questions path selection and farm status ask.

use kache_fs::InodeId;
use std::path::{Path, PathBuf};

/// Read-only filesystem access. [`RealFs`] answers from the live
/// filesystem; tests answer from a table of fake paths.
pub trait Fs {
    /// Device and inode of the file `path` reaches, following links.
    fn identity(&self, path: &Path) -> Option<InodeId>;
    /// Whether `path` reaches a regular file that may be executed. On Unix
    /// that needs an execute bit; elsewhere any regular file counts.
    fn is_executable_file(&self, path: &Path) -> bool;
    /// Whether `path` reaches a regular file.
    fn is_file(&self, path: &Path) -> bool;
    /// The text of the symlink at `path`, or `None` when `path` is not one.
    fn read_link(&self, path: &Path) -> Option<PathBuf>;
    /// `path` with every link resolved, or `None` when it reaches nothing.
    fn resolve(&self, path: &Path) -> Option<PathBuf>;
}

/// [`Fs`] against the live filesystem.
#[derive(Debug, Clone, Copy, Default)]
pub struct RealFs;

impl Fs for RealFs {
    fn identity(&self, path: &Path) -> Option<InodeId> {
        kache_fs::file_identity(path).ok()
    }

    fn is_executable_file(&self, path: &Path) -> bool {
        is_executable_file(path)
    }

    fn is_file(&self, path: &Path) -> bool {
        path.is_file()
    }

    fn read_link(&self, path: &Path) -> Option<PathBuf> {
        std::fs::read_link(path).ok()
    }

    fn resolve(&self, path: &Path) -> Option<PathBuf> {
        std::fs::canonicalize(path).ok()
    }
}

/// Whether `path` reaches a regular file that may be executed.
#[cfg(unix)]
pub fn is_executable_file(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path)
        .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

/// Whether `path` reaches a regular file that may be executed.
#[cfg(not(unix))]
pub fn is_executable_file(path: &Path) -> bool {
    path.is_file()
}

/// A table of fake files and links for unit tests.
#[cfg(all(test, unix))]
pub(crate) mod fake {
    use super::Fs;
    use kache_fs::InodeId;
    use std::collections::{BTreeMap, VecDeque};
    use std::ffi::OsString;
    use std::path::{Component, Path, PathBuf};

    #[derive(Default)]
    pub(crate) struct FakeFs {
        /// Regular files: identity and whether the execute bit is set.
        files: BTreeMap<PathBuf, (InodeId, bool)>,
        /// Symlinks: path to link text, relative text resolved against the
        /// link's directory.
        links: BTreeMap<PathBuf, PathBuf>,
        next_ino: u64,
    }

    impl FakeFs {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// An executable file with its own inode.
        pub(crate) fn exe(mut self, path: &str) -> Self {
            self.next_ino += 1;
            let id = InodeId {
                dev: 1,
                ino: self.next_ino,
            };
            self.files.insert(path.into(), (id, true));
            self
        }

        /// A regular file without an execute bit.
        pub(crate) fn plain(mut self, path: &str) -> Self {
            self.next_ino += 1;
            let id = InodeId {
                dev: 1,
                ino: self.next_ino,
            };
            self.files.insert(path.into(), (id, false));
            self
        }

        /// A second name for the file at `existing`: a hardlink, or the same
        /// file seen through a bind mount.
        pub(crate) fn same_file(mut self, path: &str, existing: &str) -> Self {
            let entry = self.files[Path::new(existing)];
            self.files.insert(path.into(), entry);
            self
        }

        pub(crate) fn link(mut self, path: &str, text: &str) -> Self {
            self.links.insert(path.into(), text.into());
            self
        }

        fn is_dir(&self, path: &Path) -> bool {
            path == Path::new("/")
                || self
                    .files
                    .keys()
                    .chain(self.links.keys())
                    .any(|entry| entry != path && entry.starts_with(path))
        }

        fn walk(&self, path: &Path) -> Option<PathBuf> {
            let mut out = PathBuf::from("/");
            let mut pending: VecDeque<Step> = steps(path).collect();
            let mut hops = 0;
            while let Some(step) = pending.pop_front() {
                match step {
                    Step::Root => out = PathBuf::from("/"),
                    Step::Parent => {
                        out.pop();
                    }
                    Step::Name(name) => {
                        out.push(name);
                        if let Some(text) = self.links.get(&out) {
                            hops += 1;
                            if hops > 40 {
                                return None;
                            }
                            out.pop();
                            for step in steps(text).collect::<Vec<_>>().into_iter().rev() {
                                pending.push_front(step);
                            }
                        }
                    }
                }
            }
            Some(out)
        }
    }

    enum Step {
        Root,
        Parent,
        Name(OsString),
    }

    fn steps(path: &Path) -> impl Iterator<Item = Step> + '_ {
        path.components().filter_map(|component| match component {
            Component::RootDir | Component::Prefix(_) => Some(Step::Root),
            Component::CurDir => None,
            Component::ParentDir => Some(Step::Parent),
            Component::Normal(name) => Some(Step::Name(name.to_owned())),
        })
    }

    impl Fs for FakeFs {
        fn identity(&self, path: &Path) -> Option<InodeId> {
            self.files.get(&self.walk(path)?).map(|(id, _)| *id)
        }

        fn is_executable_file(&self, path: &Path) -> bool {
            self.walk(path)
                .and_then(|real| self.files.get(&real))
                .is_some_and(|(_, executable)| *executable)
        }

        fn is_file(&self, path: &Path) -> bool {
            self.walk(path)
                .is_some_and(|real| self.files.contains_key(&real))
        }

        fn read_link(&self, path: &Path) -> Option<PathBuf> {
            self.links.get(path).cloned()
        }

        fn resolve(&self, path: &Path) -> Option<PathBuf> {
            let real = self.walk(path)?;
            (self.files.contains_key(&real) || self.is_dir(&real)).then_some(real)
        }
    }
}
