use anyhow::{Context, Result};
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub cache_dir: PathBuf,
    pub max_size: u64,
    pub remote: Option<RemoteConfig>,
    pub disabled: bool,
    pub cache_executables: bool,
    pub clean_incremental: bool,
    pub event_log_max_size: u64,
    pub event_log_keep_lines: usize,
    /// Zstd compression level (1-19, default 3). Lower = faster, higher = smaller.
    pub compression_level: i32,
    /// Max concurrent S3 operations (default 8).
    pub s3_concurrency: u32,
}

#[derive(Debug, Clone)]
pub struct RemoteConfig {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: String,
    /// S3 key prefix for all artifacts (default: "artifacts").
    pub prefix: String,
    /// AWS profile name for credential lookup (e.g. "ceph").
    pub profile: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct FileConfig {
    pub(crate) cache: Option<CacheFileConfig>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct CacheFileConfig {
    pub(crate) local_store: Option<String>,
    pub(crate) local_max_size: Option<String>,
    pub(crate) remote: Option<RemoteFileConfig>,
    pub(crate) cache_executables: Option<bool>,
    pub(crate) clean_incremental: Option<bool>,
    pub(crate) event_log_max_size: Option<String>,
    pub(crate) event_log_keep_lines: Option<usize>,
    pub(crate) compression_level: Option<i32>,
    pub(crate) s3_concurrency: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct RemoteFileConfig {
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub(crate) _type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) profile: Option<String>,
}

/// Tracks which config fields have active env var overrides.
#[allow(dead_code)]
pub(crate) struct EnvOverrides {
    pub(crate) disabled: bool,
    pub(crate) cache_dir: bool,
    pub(crate) max_size: bool,
    pub(crate) cache_executables: bool,
    pub(crate) clean_incremental: bool,
    pub(crate) s3_bucket: bool,
    pub(crate) s3_endpoint: bool,
    pub(crate) s3_region: bool,
    pub(crate) s3_prefix: bool,
    pub(crate) s3_profile: bool,
}

impl EnvOverrides {
    pub(crate) fn detect() -> Self {
        Self {
            disabled: std::env::var("KACHE_DISABLED").is_ok(),
            cache_dir: std::env::var("KACHE_CACHE_DIR").is_ok(),
            max_size: std::env::var("KACHE_MAX_SIZE").is_ok(),
            cache_executables: std::env::var("KACHE_CACHE_EXECUTABLES").is_ok(),
            clean_incremental: std::env::var("KACHE_CLEAN_INCREMENTAL").is_ok(),
            s3_bucket: std::env::var("KACHE_S3_BUCKET").is_ok(),
            s3_endpoint: std::env::var("KACHE_S3_ENDPOINT").is_ok(),
            s3_region: std::env::var("KACHE_S3_REGION").is_ok(),
            s3_prefix: std::env::var("KACHE_S3_PREFIX").is_ok(),
            s3_profile: std::env::var("KACHE_S3_PROFILE").is_ok(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let file_config = Self::load_file_config();

        let disabled = std::env::var("KACHE_DISABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let cache_dir = std::env::var("KACHE_CACHE_DIR")
            .map(PathBuf::from)
            .or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.local_store.as_ref())
                    .map(|s| shellexpand(s))
                    .ok_or(())
            })
            .unwrap_or_else(|_| default_cache_dir());

        let max_size = std::env::var("KACHE_MAX_SIZE")
            .ok()
            .and_then(|s| parse_size(&s))
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.local_max_size.as_ref())
                    .and_then(|s| parse_size(s))
            })
            .unwrap_or(50 * 1024 * 1024 * 1024); // 50 GB

        let cache_executables = std::env::var("KACHE_CACHE_EXECUTABLES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.cache_executables)
                    .unwrap_or(false)
            });

        let clean_incremental = std::env::var("KACHE_CLEAN_INCREMENTAL")
            .map(|v| v != "0" && !v.eq_ignore_ascii_case("false"))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.clean_incremental)
                    .unwrap_or(true)
            });

        let event_log_max_size = file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.event_log_max_size.as_ref())
            .and_then(|s| parse_size(s))
            .unwrap_or(10 * 1024 * 1024); // 10 MB

        let event_log_keep_lines = file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.event_log_keep_lines)
            .unwrap_or(1000);

        let compression_level = std::env::var("KACHE_COMPRESSION_LEVEL")
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.compression_level)
            })
            .unwrap_or(3)
            .clamp(1, 22);

        let s3_concurrency = std::env::var("KACHE_S3_CONCURRENCY")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.s3_concurrency)
            })
            .unwrap_or(8);

        let remote = Self::load_remote_config(&file_config);

        Ok(Config {
            cache_dir,
            max_size,
            remote,
            disabled,
            cache_executables,
            clean_incremental,
            event_log_max_size,
            event_log_keep_lines,
            compression_level,
            s3_concurrency,
        })
    }

    /// Load the raw file config without applying env overrides or defaults.
    /// Returns `(config, file_existed)`.
    pub(crate) fn load_raw_file_config() -> (FileConfig, bool) {
        Self::load_raw_file_config_from(&config_file_path())
    }

    /// Load a raw FileConfig from an explicit path.
    pub(crate) fn load_raw_file_config_from(config_path: &std::path::Path) -> (FileConfig, bool) {
        let existed = config_path.exists();
        if !existed {
            return (FileConfig::default(), false);
        }
        match std::fs::read_to_string(config_path) {
            Ok(content) => match toml::from_str(&content) {
                Ok(cfg) => (cfg, true),
                Err(_) => (FileConfig::default(), true),
            },
            Err(_) => (FileConfig::default(), true),
        }
    }

    /// Serialize and write a FileConfig to the config file path.
    pub(crate) fn save_file_config(config: &FileConfig) -> Result<()> {
        Self::save_file_config_to(config, &config_file_path())
    }

    /// Serialize and write a FileConfig to an explicit path.
    pub(crate) fn save_file_config_to(config: &FileConfig, path: &std::path::Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context("creating config directory")?;
        }
        let content = toml::to_string_pretty(config).context("serializing config")?;
        std::fs::write(path, content).context("writing config file")?;
        Ok(())
    }

    fn load_file_config() -> Result<FileConfig> {
        let config_path = config_file_path();
        if !config_path.exists() {
            return Ok(FileConfig::default());
        }
        let content = std::fs::read_to_string(&config_path).context("reading kache config file")?;
        toml::from_str(&content).context("parsing kache config file")
    }

    fn load_remote_config(file_config: &Result<FileConfig>) -> Option<RemoteConfig> {
        let bucket = std::env::var("KACHE_S3_BUCKET").ok().or_else(|| {
            file_config
                .as_ref()
                .ok()
                .and_then(|c| c.cache.as_ref())
                .and_then(|c| c.remote.as_ref())
                .and_then(|r| r.bucket.clone())
        })?;

        let endpoint = std::env::var("KACHE_S3_ENDPOINT").ok().or_else(|| {
            file_config
                .as_ref()
                .ok()
                .and_then(|c| c.cache.as_ref())
                .and_then(|c| c.remote.as_ref())
                .and_then(|r| r.endpoint.clone())
        });

        let region = std::env::var("KACHE_S3_REGION")
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.remote.as_ref())
                    .and_then(|r| r.region.clone())
            })
            .unwrap_or_else(|| "us-east-1".to_string());

        let prefix = std::env::var("KACHE_S3_PREFIX")
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.remote.as_ref())
                    .and_then(|r| r.prefix.clone())
            })
            .unwrap_or_else(|| "artifacts".to_string());

        let profile = std::env::var("KACHE_S3_PROFILE")
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.remote.as_ref())
                    .and_then(|r| r.profile.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Some(RemoteConfig {
            bucket,
            endpoint,
            region,
            prefix,
            profile,
        })
    }

    pub fn store_dir(&self) -> PathBuf {
        self.cache_dir.join("store")
    }

    pub fn index_db_path(&self) -> PathBuf {
        self.cache_dir.join("index.db")
    }

    pub fn event_log_path(&self) -> PathBuf {
        self.cache_dir.join("events.jsonl")
    }

    pub fn socket_path(&self) -> PathBuf {
        self.cache_dir.join("daemon.sock")
    }
}

pub(crate) fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("kache")
}

pub(crate) fn config_file_path() -> PathBuf {
    // Use XDG convention (~/.config) on all platforms instead of macOS's ~/Library/Application Support
    let config_base = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("/tmp"))
                .join(".config")
        });
    config_base.join("kache").join("config.toml")
}

fn shellexpand(s: &str) -> PathBuf {
    if s.starts_with("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(&s[2..]);
    }
    PathBuf::from(s)
}

pub(crate) fn parse_size(s: &str) -> Option<u64> {
    s.parse::<ByteSize>().ok().map(|b| b.as_u64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_cache_dir() {
        let dir = default_cache_dir();
        assert!(dir.to_string_lossy().contains("kache"));
    }

    #[test]
    fn test_shellexpand() {
        let expanded = shellexpand("~/foo");
        assert!(!expanded.to_string_lossy().starts_with("~/"));
    }

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("50GB"), Some(50_000_000_000));
        assert_eq!(parse_size("1MB"), Some(1_000_000));
        assert!(parse_size("invalid").is_none());
    }

    #[test]
    fn test_file_config_roundtrip() {
        let config = FileConfig {
            cache: Some(CacheFileConfig {
                local_store: Some("~/my/cache".to_string()),
                local_max_size: Some("50GB".to_string()),
                cache_executables: Some(true),
                clean_incremental: Some(false),
                event_log_max_size: Some("10MB".to_string()),
                event_log_keep_lines: Some(500),
                compression_level: Some(3),
                s3_concurrency: Some(8),
                remote: Some(RemoteFileConfig {
                    _type: Some("s3".to_string()),
                    bucket: Some("my-bucket".to_string()),
                    endpoint: Some("https://s3.example.com".to_string()),
                    region: Some("eu-west-1".to_string()),
                    prefix: Some("my-prefix".to_string()),
                    profile: None,
                }),
            }),
        };
        let serialized = toml::to_string_pretty(&config).unwrap();
        let deserialized: FileConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(
            deserialized.cache.as_ref().unwrap().local_store.as_deref(),
            Some("~/my/cache")
        );
        assert_eq!(
            deserialized
                .cache
                .as_ref()
                .unwrap()
                .remote
                .as_ref()
                .unwrap()
                .bucket
                .as_deref(),
            Some("my-bucket")
        );
    }

    #[test]
    fn test_file_config_empty_remote_omitted() {
        let config = FileConfig {
            cache: Some(CacheFileConfig {
                local_store: Some("~/cache".to_string()),
                remote: Some(RemoteFileConfig::default()),
                ..Default::default()
            }),
        };
        let serialized = toml::to_string_pretty(&config).unwrap();
        // Empty remote section should still serialize (just with empty table)
        // but all None fields should be omitted thanks to skip_serializing_if
        assert!(!serialized.contains("bucket"));
        assert!(!serialized.contains("endpoint"));
    }

    #[test]
    fn test_env_overrides_detect() {
        // Just verify it doesn't panic — actual env var presence is environment-dependent
        let overrides = EnvOverrides::detect();
        // In test environment, these are typically not set
        let _ = overrides.disabled;
        let _ = overrides.cache_dir;
    }

    #[test]
    fn test_config_store_dir() {
        let config = Config {
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 8,
        };
        assert_eq!(config.store_dir(), PathBuf::from("/tmp/kache/store"));
    }

    #[test]
    fn test_config_index_db_path() {
        let config = Config {
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 8,
        };
        assert_eq!(config.index_db_path(), PathBuf::from("/tmp/kache/index.db"));
    }

    #[test]
    fn test_config_event_log_path() {
        let config = Config {
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 8,
        };
        assert_eq!(
            config.event_log_path(),
            PathBuf::from("/tmp/kache/events.jsonl")
        );
    }

    #[test]
    fn test_config_socket_path() {
        let config = Config {
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 8,
        };
        assert_eq!(
            config.socket_path(),
            PathBuf::from("/tmp/kache/daemon.sock")
        );
    }

    #[test]
    fn test_config_file_path() {
        let path = config_file_path();
        assert!(path.to_string_lossy().contains("kache"));
        assert!(path.to_string_lossy().ends_with("config.toml"));
    }

    #[test]
    fn test_shellexpand_no_tilde() {
        let path = shellexpand("/absolute/path");
        assert_eq!(path, PathBuf::from("/absolute/path"));
    }

    #[test]
    fn test_shellexpand_relative() {
        let path = shellexpand("relative/path");
        assert_eq!(path, PathBuf::from("relative/path"));
    }

    #[test]
    fn test_parse_size_various() {
        assert_eq!(parse_size("1KB"), Some(1_000));
        assert_eq!(parse_size("10GB"), Some(10_000_000_000));
        assert_eq!(parse_size("0B"), Some(0));
        assert!(parse_size("").is_none());
        assert!(parse_size("abc").is_none());
    }

    #[test]
    fn test_save_and_load_file_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");

        let config = FileConfig {
            cache: Some(CacheFileConfig {
                local_store: Some("/tmp/my-cache".to_string()),
                local_max_size: Some("10GB".to_string()),
                cache_executables: Some(true),
                clean_incremental: None,
                event_log_max_size: None,
                event_log_keep_lines: None,
                compression_level: Some(5),
                s3_concurrency: None,
                remote: None,
            }),
        };

        Config::save_file_config_to(&config, &config_path).unwrap();
        assert!(config_path.exists());

        let (loaded, existed) = Config::load_raw_file_config_from(&config_path);
        assert!(existed);
        assert_eq!(
            loaded.cache.as_ref().unwrap().local_store.as_deref(),
            Some("/tmp/my-cache")
        );
        assert_eq!(loaded.cache.as_ref().unwrap().compression_level, Some(5));
    }

    #[test]
    fn test_load_raw_file_config_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("nonexistent/config.toml");

        let (config, existed) = Config::load_raw_file_config_from(&config_path);
        assert!(!existed);
        assert!(config.cache.is_none());
    }

    #[test]
    fn test_remote_file_config_with_profile() {
        let config = FileConfig {
            cache: Some(CacheFileConfig {
                remote: Some(RemoteFileConfig {
                    _type: Some("s3".to_string()),
                    bucket: Some("mybucket".to_string()),
                    endpoint: None,
                    region: Some("eu-west-1".to_string()),
                    prefix: None,
                    profile: Some("ceph".to_string()),
                }),
                ..Default::default()
            }),
        };
        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(serialized.contains("profile = \"ceph\""));

        let deserialized: FileConfig = toml::from_str(&serialized).unwrap();
        assert_eq!(
            deserialized
                .cache
                .unwrap()
                .remote
                .unwrap()
                .profile
                .as_deref(),
            Some("ceph")
        );
    }
}
