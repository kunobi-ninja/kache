use std::path::PathBuf;

/// How a size budget treats the bytes a live build output still holds:
/// blobs whose last store reference an entry holds while a target directory
/// hardlinks or clones them too. Evicting such an entry frees no disk, so a
/// sweep keeps it either way; this decides whether its bytes count toward
/// `max_size`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum HeldBytesBudget {
    /// Left out (kunobi-ninja/kache#1206): a sweep stops once the rest of
    /// the store is under its target, so the store can stay over `max_size`
    /// by what build outputs hold.
    #[default]
    LeftOut,
    /// Counted: a sweep evicts other entries until the whole store is under
    /// its target, so `max_size` bounds everything the store holds.
    Counted,
}

impl HeldBytesBudget {
    /// `[cache] count_held_in_max_size`.
    pub fn from_count_held_in_max_size(counted: bool) -> Self {
        if counted {
            Self::Counted
        } else {
            Self::LeftOut
        }
    }

    /// The part of `held` bytes the budget leaves out.
    pub fn left_out(self, held: u64) -> u64 {
        match self {
            Self::LeftOut => held,
            Self::Counted => 0,
        }
    }
}

/// Options needed by local storage; remote and compiler settings stay with the caller.
#[derive(Clone, Debug)]
pub struct Config {
    pub cache_dir: PathBuf,
    pub max_size: u64,
    pub gc_evict_shared: bool,
    /// Whether bytes a build output still holds count toward `max_size`.
    pub held_bytes_budget: HeldBytesBudget,
    pub upload_spool_max_jobs: usize,
    /// Store new entries without an fsync on the build path and flush them
    /// later; a hit on an entry still pending verifies its bytes first.
    pub deferred_durability: bool,
}

impl From<&Config> for Config {
    fn from(config: &Config) -> Self {
        config.clone()
    }
}

impl Config {
    pub fn store_dir(&self) -> PathBuf {
        self.cache_dir.join("store")
    }

    pub fn upload_spool_dir(&self) -> PathBuf {
        self.cache_dir.join("upload-queue")
    }

    pub fn index_db_path(&self) -> PathBuf {
        self.cache_dir.join("index.db")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_paths_belong_to_the_selected_cache_and_have_separate_names() {
        let paths = |config: &Config| {
            [
                config.store_dir(),
                config.upload_spool_dir(),
                config.index_db_path(),
            ]
        };
        let first = Config {
            cache_dir: PathBuf::from("first cache"),
            max_size: 1,
            gc_evict_shared: false,
            held_bytes_budget: HeldBytesBudget::default(),
            upload_spool_max_jobs: 2,
            deferred_durability: false,
        };
        let second = Config {
            cache_dir: PathBuf::from("second/cache"),
            ..first.clone()
        };
        let first_paths = paths(&first);
        for config in [&first, &second] {
            let selected = paths(config);
            for (index, path) in selected.iter().enumerate() {
                assert_eq!(path.parent(), Some(config.cache_dir.as_path()));
                assert!(path.file_name().is_some());
                assert!(!selected[..index].contains(path));
            }
        }
        assert_eq!(
            paths(&first),
            first_paths,
            "resolving a second cache cannot change the first"
        );
        assert!(
            paths(&second)
                .iter()
                .all(|path| !first_paths.contains(path))
        );
    }
}
