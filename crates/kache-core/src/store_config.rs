use std::path::PathBuf;

/// Options needed by local storage; remote and compiler settings stay with the caller.
#[derive(Clone, Debug)]
pub struct Config {
    pub cache_dir: PathBuf,
    pub max_size: u64,
    pub gc_evict_shared: bool,
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
