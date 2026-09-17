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
