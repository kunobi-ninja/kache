use std::path::PathBuf;

/// Performance policy for the local SQLite index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IndexMode {
    /// Follow Kache's recommended policy. Currently resolves to conservative.
    #[default]
    Auto,
    /// Keep the established WAL + NORMAL policy.
    Conservative,
    /// Disable index fsync for storage discarded with the job.
    Off,
}

/// A concrete index policy after resolving the automatic mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResolvedIndexMode {
    Conservative,
    Off,
}

impl IndexMode {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "conservative" => Some(Self::Conservative),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Conservative => "conservative",
            Self::Off => "off",
        }
    }

    pub fn resolve(self) -> ResolvedIndexMode {
        match self {
            Self::Auto | Self::Conservative => ResolvedIndexMode::Conservative,
            Self::Off => ResolvedIndexMode::Off,
        }
    }
}

impl ResolvedIndexMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Conservative => "conservative",
            Self::Off => "off",
        }
    }

    pub fn pragma_value(self) -> &'static str {
        match self {
            Self::Conservative => "NORMAL",
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
    pub index_mode: IndexMode,
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
    fn index_mode_parses_each_mode_and_rejects_the_rest() {
        for (value, expected) in [
            ("auto", IndexMode::Auto),
            (" AUTO ", IndexMode::Auto),
            ("conservative", IndexMode::Conservative),
            (" Conservative ", IndexMode::Conservative),
            ("off", IndexMode::Off),
            (" OFF ", IndexMode::Off),
        ] {
            assert_eq!(IndexMode::parse(value), Some(expected), "{value:?}");
        }
        for value in ["", "  ", "normal", "full", "fast", "0"] {
            assert_eq!(IndexMode::parse(value), None, "{value:?}");
        }
        assert_eq!(IndexMode::default(), IndexMode::Auto);
    }

    #[test]
    fn index_modes_resolve_to_the_expected_sqlite_policy() {
        for (mode, configured, effective, pragma) in [
            (IndexMode::Auto, "auto", "conservative", "NORMAL"),
            (
                IndexMode::Conservative,
                "conservative",
                "conservative",
                "NORMAL",
            ),
            (IndexMode::Off, "off", "off", "OFF"),
        ] {
            assert_eq!(mode.as_str(), configured);
            assert_eq!(mode.resolve().as_str(), effective);
            assert_eq!(mode.resolve().pragma_value(), pragma);
        }
        assert_eq!(IndexMode::Auto.resolve(), ResolvedIndexMode::Conservative);
        assert_eq!(
            IndexMode::Conservative.resolve(),
            ResolvedIndexMode::Conservative
        );
        assert_eq!(IndexMode::Off.resolve(), ResolvedIndexMode::Off);
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
            index_mode: IndexMode::default(),
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
