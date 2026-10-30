use std::path::PathBuf;

/// How long a commit to `index.db` waits for the disk. Each connection sets it
/// when it opens the index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IndexSynchronous {
    /// SQLite's `NORMAL`: fsync at WAL checkpoints, so a power loss cannot
    /// corrupt the index.
    #[default]
    Normal,
    /// SQLite's `OFF`: never fsync. A killed process loses nothing, but a power
    /// loss or kernel crash can corrupt the index, which then rebuilds from
    /// the store. For runners whose disk does not outlive the job.
    Off,
}

impl IndexSynchronous {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "normal" => Some(Self::Normal),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    pub fn pragma_value(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Off => "OFF",
        }
    }
}

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
    pub index_synchronous: IndexSynchronous,
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
    fn index_synchronous_parses_each_mode_and_rejects_the_rest() {
        assert_eq!(
            IndexSynchronous::parse("normal"),
            Some(IndexSynchronous::Normal)
        );
        assert_eq!(
            IndexSynchronous::parse(" OFF "),
            Some(IndexSynchronous::Off)
        );
        assert_eq!(IndexSynchronous::parse("full"), None);
        assert_eq!(IndexSynchronous::default(), IndexSynchronous::Normal);
    }

    #[test]
    fn index_synchronous_names_the_sqlite_pragma_value() {
        assert_eq!(IndexSynchronous::Normal.pragma_value(), "NORMAL");
        assert_eq!(IndexSynchronous::Off.pragma_value(), "OFF");
    }

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
            index_synchronous: IndexSynchronous::default(),
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
