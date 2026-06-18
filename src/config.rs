use anyhow::{Context, Result};
use bytesize::ByteSize;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub const DEFAULT_DAEMON_IDLE_TIMEOUT_SECS: u64 = 10 * 60;
pub const DEFAULT_PLANNER_TIMEOUT_MS: u64 = 750;
pub const DEFAULT_S3_POOL_IDLE_SECS: u64 = 300;

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
    /// Max concurrent S3 operations (default 16).
    pub s3_concurrency: u32,
    /// Daemon idle timeout in seconds (default 600 = 10 minutes). 0 = no timeout.
    pub daemon_idle_timeout_secs: u64,
    /// How long an idle TCP/TLS connection is kept in the S3 client's pool, in
    /// seconds (default 300). Tuned higher than hyper's 90s default so that
    /// gaps between S3 bursts (e.g. between prefetch and post-build sync)
    /// reuse warm TLS sessions instead of re-handshaking. Set lower if you sit
    /// behind a load balancer with an aggressive idle timeout that may drop
    /// connections silently.
    pub s3_pool_idle_secs: u64,
    /// A secondary compiler-wrapper to hand passed-through compiles to.
    /// When kache declines to cache a compile, it
    /// runs `<fallback> <compiler> <args>` instead of the bare
    /// compiler — so the fallback gets a chance to cache what kache
    /// doesn't. `None` = plain passthrough. Set via `KACHE_FALLBACK`
    /// or `[cache] fallback` in the config file.
    pub fallback: Option<String>,
    /// An opaque string folded into every cache key. Lets a project
    /// force a cold cache on a change kache cannot otherwise observe —
    /// e.g. a toolchain-closure bump (glibc/mold/linker, a Nix store
    /// rebuild) that alters compiled output but leaves every tool's
    /// `--version` banner unchanged. Set it to a hash of the toolchain
    /// (or any sentinel) and a change re-keys instead of serving a
    /// stale hit. `None`/empty = no effect (keys are byte-identical to
    /// not setting it). Set via `KACHE_KEY_SALT` or `[cache] key_salt`.
    pub key_salt: Option<String>,
    /// Env vars (besides OUT_DIR) whose values are only ever used to locate
    /// an `include!`'d file, so their absolute path may be normalized in the
    /// cache key — the OUT_DIR path-only contract, still gated by the same
    /// "a source file lives under the value" check. Lets a build opt in
    /// project-specific generated-file locators (e.g. Firefox's
    /// `BUILDCONFIG_RS` / `MOZ_TOPOBJDIR`) without kache hardcoding them, and
    /// without endangering value-baked vars like `CARGO_MANIFEST_DIR` (the
    /// gate keeps those absolute). Set via `KACHE_PATH_ONLY_ENV_VARS`
    /// (comma/space-separated) or `[cache] path_only_env_vars`. Empty (the
    /// default) = only OUT_DIR is normalized.
    pub path_only_env_vars: Vec<String>,
    /// User-declared cc/c++ flags to allow into caching ahead of
    /// built-in support (issue #95). kache's cc allow-list refuses any
    /// flag it doesn't model; listing one here makes kache *stop
    /// refusing* it and fold the flag verbatim into the cache key, so a
    /// different flag value still produces a different key (never a
    /// miscache by value). Matched **exactly** against the command line;
    /// only flags actually present are folded. This can only *add* to the
    /// hashable set — it cannot override structural refusals (link mode,
    /// coverage, multi-arch, PCH, modules, …). Empty = feature off (keys
    /// byte-identical to not setting it). Set via
    /// `KACHE_CC_EXTRA_ALLOWLIST_FLAGS` (whitespace-separated) or
    /// `[cc] extra_allowlist_flags`.
    ///
    /// Sharp edge: host-dependent flags like `-march=native` are a
    /// constant string but compile to per-CPU objects; folded verbatim
    /// they collide across machines. List explicit values, not `native`.
    pub cc_extra_allowlist_flags: Vec<String>,
    /// Strict local-only mode (#221): when on, kache ignores **all** remote
    /// and planner configuration and environment — no S3 bucket, no planner
    /// endpoint, no egress of any kind — so a build is guaranteed hermetic.
    /// Local caching stays fully on (unlike `disabled`, which turns caching
    /// off entirely). A single deterministic switch so a stray `~/.config`
    /// remote or leaked `KACHE_S3_*` / `KACHE_PLANNER_*` env can't pull a
    /// hermetic build off the network. Set via `KACHE_LOCAL_ONLY=1`/`=true`
    /// or `[cache] local_only`; env wins over the file.
    pub local_only: bool,
    /// Opt-in too-new-input guard (kunobi-ninja/kache#324): when on, an
    /// invocation whose keyed inputs were modified at/after the build started is
    /// looked up but NOT stored (its hashes are racy relative to what the
    /// compiler reads). Off by default. Set via `KACHE_MODIFIED_INPUT_GUARD=1`/
    /// `=true` or `[cache] modified_input_guard`; env wins over the file.
    pub modified_input_guard: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannerConfig {
    pub endpoint: String,
    pub timeout_ms: u64,
    pub token: Option<String>,
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
    pub(crate) cc: Option<CcFileConfig>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct CcFileConfig {
    /// User-declared cc flags to allow into caching.
    /// See [`Config::cc_extra_allowlist_flags`].
    pub(crate) extra_allowlist_flags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct CacheFileConfig {
    pub(crate) local_store: Option<String>,
    pub(crate) local_max_size: Option<String>,
    pub(crate) remote: Option<RemoteFileConfig>,
    pub(crate) planner: Option<PlannerFileConfig>,
    /// Strict local-only mode. See [`Config::local_only`].
    pub(crate) local_only: Option<bool>,
    /// Too-new-input guard. See [`Config::modified_input_guard`].
    pub(crate) modified_input_guard: Option<bool>,
    /// Ignore `KACHE_*` env overrides for file-backed settings. File-only by
    /// design (env must not re-enable env). See [`Config::ignore_env_enabled`].
    pub(crate) ignore_env: Option<bool>,
    pub(crate) cache_executables: Option<bool>,
    pub(crate) clean_incremental: Option<bool>,
    pub(crate) exclude: Option<Vec<String>>,
    pub(crate) event_log_max_size: Option<String>,
    pub(crate) event_log_keep_lines: Option<usize>,
    pub(crate) compression_level: Option<i32>,
    pub(crate) s3_concurrency: Option<u32>,
    pub(crate) daemon_idle_timeout_secs: Option<u64>,
    pub(crate) s3_pool_idle_secs: Option<u64>,
    /// Secondary compiler-wrapper for passed-through compiles.
    /// See [`Config::fallback`].
    pub(crate) fallback: Option<String>,
    /// Opaque cache-key salt. See [`Config::key_salt`].
    pub(crate) key_salt: Option<String>,
    /// Path-only env-var allowlist. See [`Config::path_only_env_vars`].
    pub(crate) path_only_env_vars: Option<Vec<String>>,
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

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub(crate) struct PlannerFileConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token: Option<String>,
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
    pub(crate) fallback: bool,
    pub(crate) key_salt: bool,
    pub(crate) cc_extra_allowlist_flags: bool,
    pub(crate) local_only: bool,
}

impl EnvOverrides {
    pub(crate) fn detect() -> Self {
        // When the pinned config sets `ignore_env`, gated env vars no longer win,
        // so they must NOT show as env-locked in the TUI. `KACHE_DISABLED` is
        // ungated and always reflects its real env state.
        let ignore_env = Config::ignore_env_enabled(&Config::load_file_config());
        Self {
            disabled: std::env::var("KACHE_DISABLED").is_ok(),
            local_only: env_or_ignored("KACHE_LOCAL_ONLY", ignore_env).is_ok(),
            cache_dir: env_or_ignored("KACHE_CACHE_DIR", ignore_env).is_ok(),
            max_size: env_or_ignored("KACHE_MAX_SIZE", ignore_env).is_ok(),
            cache_executables: env_or_ignored("KACHE_CACHE_EXECUTABLES", ignore_env).is_ok(),
            clean_incremental: env_or_ignored("KACHE_CLEAN_INCREMENTAL", ignore_env).is_ok(),
            s3_bucket: env_or_ignored("KACHE_S3_BUCKET", ignore_env).is_ok(),
            s3_endpoint: env_or_ignored("KACHE_S3_ENDPOINT", ignore_env).is_ok(),
            s3_region: env_or_ignored("KACHE_S3_REGION", ignore_env).is_ok(),
            s3_prefix: env_or_ignored("KACHE_S3_PREFIX", ignore_env).is_ok(),
            s3_profile: env_or_ignored("KACHE_S3_PROFILE", ignore_env).is_ok(),
            fallback: env_or_ignored("KACHE_FALLBACK", ignore_env).is_ok(),
            key_salt: env_or_ignored("KACHE_KEY_SALT", ignore_env).is_ok(),
            cc_extra_allowlist_flags: env_or_ignored("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", ignore_env)
                .is_ok(),
        }
    }
}

/// Normalize a list of user-declared cc flags: trim each, drop empties,
/// dedupe while preserving first-seen order. Keeps the cache-key fold
/// deterministic and the allow-list free of accidental blanks.
fn normalize_cc_flags(raw: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for flag in raw {
        let trimmed = flag.trim();
        if trimmed.is_empty() || out.iter().any(|f| f == trimmed) {
            continue;
        }
        out.push(trimmed.to_string());
    }
    out
}

/// The `KACHE_*` env vars suppressed by `[cache] ignore_env`: every file-backed
/// setting. Deliberately excludes bootstrap/operational vars that have no file
/// representation — `KACHE_CONFIG` (locates the file itself), `KACHE_DISABLED`
/// (operational kill switch), `KACHE_LOG`/`KACHE_LOG_FILE`/`KACHE_PROGRESS`,
/// `KACHE_NAMESPACE`, `KACHE_BASE_DIR` — and S3 credentials
/// (`KACHE_S3_ACCESS_KEY`/`KACHE_S3_SECRET_KEY`), which are secrets, not config.
/// Used only to warn which overrides are being ignored; the gating itself is
/// done inline via [`env_or_ignored`].
const IGNORE_ENV_GATED_VARS: &[&str] = &[
    "KACHE_CACHE_DIR",
    "KACHE_MAX_SIZE",
    "KACHE_CACHE_EXECUTABLES",
    "KACHE_CLEAN_INCREMENTAL",
    "KACHE_COMPRESSION_LEVEL",
    "KACHE_S3_CONCURRENCY",
    "KACHE_DAEMON_IDLE_TIMEOUT",
    "KACHE_S3_POOL_IDLE_SECS",
    "KACHE_FALLBACK",
    "KACHE_KEY_SALT",
    "KACHE_CC_EXTRA_ALLOWLIST_FLAGS",
    "KACHE_PATH_ONLY_ENV_VARS",
    "KACHE_S3_BUCKET",
    "KACHE_S3_ENDPOINT",
    "KACHE_S3_REGION",
    "KACHE_S3_PREFIX",
    "KACHE_S3_PROFILE",
    "KACHE_LOCAL_ONLY",
    "KACHE_MODIFIED_INPUT_GUARD",
    "KACHE_PLANNER_ENDPOINT",
    "KACHE_PLANNER_TIMEOUT_MS",
    "KACHE_PLANNER_TOKEN",
];

/// Read a `KACHE_*` env var, unless the pinned config asked to ignore env
/// (`[cache] ignore_env = true`). Returns `Err(NotPresent)` when locked, so
/// every existing env -> file -> default fallback arm transparently skips the
/// env value and takes the file/default. A drop-in for `std::env::var` on the
/// file-backed settings (see [`IGNORE_ENV_GATED_VARS`]).
fn env_or_ignored(name: &str, ignore_env: bool) -> Result<String, std::env::VarError> {
    if ignore_env {
        Err(std::env::VarError::NotPresent)
    } else {
        std::env::var(name)
    }
}

/// Warn (once, loudly) which gated `KACHE_*` overrides are present but being
/// ignored because the pinned config set `ignore_env = true`. The whole point
/// of the feature is that a stray machine-global export (e.g. `KACHE_KEY_SALT`)
/// can't *silently* shift the cache key — so make the suppression visible.
fn warn_ignored_env_overrides() {
    let present: Vec<&str> = IGNORE_ENV_GATED_VARS
        .iter()
        .copied()
        .filter(|name| std::env::var_os(name).is_some())
        .collect();
    if !present.is_empty() {
        tracing::warn!(
            "[cache] ignore_env = true: ignoring set env override(s) {present:?} in favor of the \
             config file"
        );
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let file_config = Self::load_file_config();
        let ignore_env = Self::ignore_env_enabled(&file_config);
        if ignore_env {
            warn_ignored_env_overrides();
        }

        // NOTE: `KACHE_DISABLED` is intentionally NOT gated by `ignore_env` —
        // it's an operational kill switch, not a file-backed setting.
        let disabled = std::env::var("KACHE_DISABLED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        let cache_dir = env_or_ignored("KACHE_CACHE_DIR", ignore_env)
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

        let max_size = env_or_ignored("KACHE_MAX_SIZE", ignore_env)
            .ok()
            .and_then(|s| parse_size_checked(&s, "KACHE_MAX_SIZE"))
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.local_max_size.as_ref())
                    .and_then(|s| parse_size_checked(s, "[cache] local_max_size"))
            })
            .unwrap_or(50 * 1024 * 1024 * 1024); // 50 GiB

        let cache_executables = env_or_ignored("KACHE_CACHE_EXECUTABLES", ignore_env)
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or_else(|_| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.cache_executables)
                    .unwrap_or(false)
            });

        let clean_incremental = env_or_ignored("KACHE_CLEAN_INCREMENTAL", ignore_env)
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
            .and_then(|s| parse_size_checked(s, "[cache] event_log_max_size"))
            .unwrap_or(10 * 1024 * 1024); // 10 MiB

        let event_log_keep_lines = file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.event_log_keep_lines)
            .unwrap_or(1000);

        let compression_level = env_or_ignored("KACHE_COMPRESSION_LEVEL", ignore_env)
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

        let s3_concurrency = env_or_ignored("KACHE_S3_CONCURRENCY", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.s3_concurrency)
            })
            .unwrap_or(16);

        let daemon_idle_timeout_secs = env_or_ignored("KACHE_DAEMON_IDLE_TIMEOUT", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.daemon_idle_timeout_secs)
            })
            .unwrap_or(DEFAULT_DAEMON_IDLE_TIMEOUT_SECS);

        let s3_pool_idle_secs = env_or_ignored("KACHE_S3_POOL_IDLE_SECS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.s3_pool_idle_secs)
            })
            .unwrap_or(DEFAULT_S3_POOL_IDLE_SECS);

        // Fallback compiler-wrapper for passed-through compiles. Env
        // wins over the file; empty / "off" / "none" disables it.
        let fallback = env_or_ignored("KACHE_FALLBACK", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.fallback.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| {
                !s.is_empty() && !s.eq_ignore_ascii_case("off") && !s.eq_ignore_ascii_case("none")
            });

        // Cache-key salt. Env wins over the file; an empty / whitespace
        // value is treated as unset so it never silently shifts the key.
        let key_salt = env_or_ignored("KACHE_KEY_SALT", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.key_salt.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        // User-declared cc allowlist flags (issue #95). Env wins over the
        // file: a set `KACHE_CC_EXTRA_ALLOWLIST_FLAGS` (whitespace-separated,
        // possibly empty → disables) replaces the file list entirely.
        let cc_extra_allowlist_flags =
            match env_or_ignored("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", ignore_env) {
                Ok(val) => normalize_cc_flags(val.split_whitespace().map(str::to_string)),
                Err(_) => normalize_cc_flags(
                    file_config
                        .as_ref()
                        .ok()
                        .and_then(|c| c.cc.as_ref())
                        .and_then(|c| c.extra_allowlist_flags.clone())
                        .unwrap_or_default(),
                ),
            };

        // Path-only env-var allowlist (the OUT_DIR-style normalization opt-in).
        // Env wins over the file: a set `KACHE_PATH_ONLY_ENV_VARS`
        // (comma/whitespace-separated) replaces the file list entirely.
        let path_only_env_vars = match env_or_ignored("KACHE_PATH_ONLY_ENV_VARS", ignore_env) {
            Ok(val) => val
                .split([',', ' ', '\t', '\n'])
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect(),
            Err(_) => file_config
                .as_ref()
                .ok()
                .and_then(|c| c.cache.as_ref())
                .and_then(|c| c.path_only_env_vars.clone())
                .unwrap_or_default(),
        };

        // Strict local-only mode (#221): suppress all remote config at the
        // source so every consumer that treats `remote = None` as "no remote"
        // becomes a clean no-op — no S3 client, no uploads, no remote checks.
        // The planner is suppressed symmetrically in `load_planner_config`.
        let local_only = Self::local_only_enabled(&file_config);
        let modified_input_guard = Self::modified_input_guard_enabled(&file_config);
        let remote = if local_only {
            None
        } else {
            Self::load_remote_config(&file_config)
        };

        Ok(Config {
            cache_dir,
            max_size,
            remote,
            disabled,
            local_only,
            modified_input_guard,
            cache_executables,
            clean_incremental,
            event_log_max_size,
            event_log_keep_lines,
            compression_level,
            s3_concurrency,
            daemon_idle_timeout_secs,
            s3_pool_idle_secs,
            fallback,
            key_salt,
            path_only_env_vars,
            cc_extra_allowlist_flags,
        })
    }

    /// Load the raw file config without applying env overrides or defaults.
    /// The config path still honors `KACHE_CONFIG`.
    /// Returns `(config, file_existed)`.
    pub(crate) fn load_raw_file_config() -> (FileConfig, bool) {
        Self::load_raw_file_config_from(&resolve_config_path())
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

    /// Serialize and write a FileConfig to the active config path.
    /// The config path still honors `KACHE_CONFIG`.
    pub(crate) fn save_file_config(config: &FileConfig) -> Result<()> {
        Self::save_file_config_to(config, &resolve_config_path())
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
        let config_path = resolve_config_path();
        if !config_path.exists() {
            return Ok(FileConfig::default());
        }
        let content = std::fs::read_to_string(&config_path).context("reading kache config file")?;
        toml::from_str(&content).context("parsing kache config file")
    }

    fn load_remote_config(file_config: &Result<FileConfig>) -> Option<RemoteConfig> {
        let ignore_env = Self::ignore_env_enabled(file_config);
        let bucket = env_or_ignored("KACHE_S3_BUCKET", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.remote.as_ref())
                    .and_then(|r| r.bucket.clone())
            })?;

        let endpoint = env_or_ignored("KACHE_S3_ENDPOINT", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.remote.as_ref())
                    .and_then(|r| r.endpoint.clone())
            });

        let region = env_or_ignored("KACHE_S3_REGION", ignore_env)
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

        let prefix = env_or_ignored("KACHE_S3_PREFIX", ignore_env)
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

        let profile = env_or_ignored("KACHE_S3_PROFILE", ignore_env)
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

    /// Whether strict local-only mode is active (#221). Env wins over the
    /// file, mirroring the other toggles: `KACHE_LOCAL_ONLY=1`/`=true` (or any
    /// other value to force it *off*, overriding the file), else
    /// `[cache] local_only`, else off.
    /// Whether the pinned config asked kache to ignore `KACHE_*` env overrides
    /// for file-backed settings (`[cache] ignore_env = true`).
    ///
    /// Deliberately **file-only**: an env var must not be able to re-enable env
    /// overrides, or the lockdown a pinned config wants would be trivially
    /// undone by the same stray export it's meant to defend against. The intent
    /// is to let a project pin its config so a machine-global `KACHE_KEY_SALT`
    /// (or any other override) can't silently change behavior — see
    /// [`IGNORE_ENV_GATED_VARS`] for exactly what is and isn't covered.
    fn ignore_env_enabled(file_config: &Result<FileConfig>) -> bool {
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.ignore_env)
            .unwrap_or(false)
    }

    fn local_only_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_LOCAL_ONLY", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.local_only)
            .unwrap_or(false)
    }

    /// Whether the opt-in too-new-input guard is active (kunobi-ninja/kache#324).
    /// Env wins over the file: `KACHE_MODIFIED_INPUT_GUARD=1`/`=true`, else
    /// `[cache] modified_input_guard`, else off.
    fn modified_input_guard_enabled(file_config: &Result<FileConfig>) -> bool {
        let ignore_env = Self::ignore_env_enabled(file_config);
        if let Ok(v) = env_or_ignored("KACHE_MODIFIED_INPUT_GUARD", ignore_env) {
            return v == "1" || v.eq_ignore_ascii_case("true");
        }
        file_config
            .as_ref()
            .ok()
            .and_then(|c| c.cache.as_ref())
            .and_then(|c| c.modified_input_guard)
            .unwrap_or(false)
    }

    pub fn load_planner_config() -> Option<PlannerConfig> {
        let file_config = Self::load_file_config();
        let ignore_env = Self::ignore_env_enabled(&file_config);

        // Strict local-only mode (#221) suppresses the planner entirely —
        // symmetric with `remote` being forced to `None` in `load`.
        if Self::local_only_enabled(&file_config) {
            return None;
        }

        let endpoint = env_or_ignored("KACHE_PLANNER_ENDPOINT", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.planner.as_ref())
                    .and_then(|c| c.endpoint.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;

        let timeout_ms = env_or_ignored("KACHE_PLANNER_TIMEOUT_MS", ignore_env)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.planner.as_ref())
                    .and_then(|c| c.timeout_ms)
            })
            .unwrap_or(DEFAULT_PLANNER_TIMEOUT_MS);

        let token = env_or_ignored("KACHE_PLANNER_TOKEN", ignore_env)
            .ok()
            .or_else(|| {
                file_config
                    .as_ref()
                    .ok()
                    .and_then(|c| c.cache.as_ref())
                    .and_then(|c| c.planner.as_ref())
                    .and_then(|c| c.token.clone())
            })
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Some(PlannerConfig {
            endpoint,
            timeout_ms,
            token,
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

    pub fn transfer_log_path(&self) -> PathBuf {
        self.cache_dir.join("transfers.jsonl")
    }

    pub fn socket_path(&self) -> PathBuf {
        self.cache_dir.join("daemon.sock")
    }

    /// Return true when `source_path` matches one of `[cache].exclude`'s glob
    /// patterns from the active config file.
    pub fn source_excluded(source_path: &Path, roots: &[PathBuf]) -> bool {
        let patterns = Self::load_exclude_patterns();
        source_excluded_by_patterns(&patterns, source_path, roots)
    }

    fn load_exclude_patterns() -> Vec<String> {
        Self::load_file_config()
            .ok()
            .and_then(|c| c.cache)
            .and_then(|c| c.exclude)
            .unwrap_or_default()
            .into_iter()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    }
}

fn source_excluded_by_patterns(patterns: &[String], source_path: &Path, roots: &[PathBuf]) -> bool {
    if patterns.is_empty() {
        return false;
    }

    let candidates = source_candidates(source_path, roots);
    patterns
        .iter()
        .any(|pattern| exclude_pattern_matches(pattern, &candidates))
}

pub(crate) fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join("kache")
}

const PROJECT_CONFIG_NAME: &str = ".kache.toml";

/// Resolve the config file path to actually load from.
/// Priority: `KACHE_CONFIG` env var > nearest `.kache.toml` > XDG user config.
pub(crate) fn resolve_config_path() -> PathBuf {
    resolve_config_path_from(
        std::env::var_os("KACHE_CONFIG").map(PathBuf::from),
        std::env::current_dir().ok(),
    )
}

fn resolve_config_path_from(
    kache_config: Option<PathBuf>,
    current_dir: Option<PathBuf>,
) -> PathBuf {
    if let Some(p) = kache_config {
        return p;
    }

    if let Some(path) = nearest_project_config_path(current_dir.as_deref()) {
        return path;
    }

    config_file_path()
}

fn nearest_project_config_path(current_dir: Option<&std::path::Path>) -> Option<PathBuf> {
    let current_dir = current_dir?;
    for dir in current_dir.ancestors() {
        let candidate = dir.join(PROJECT_CONFIG_NAME);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
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

/// Core `$VAR` / `${VAR}` expander. Returns the expanded string plus the names
/// of every referenced env var that was *unset* (no value and no
/// [`default_env_var_value`]) and so was left as a literal `$VAR` in the output.
///
/// An unset reference matters to cache-key callers: it silently survives as
/// text that matches nothing, so they fold a replayable pattern-set-only key
/// while believing the intended files are tracked. Reporting the unset names
/// lets those callers warn instead of degrading silently.
fn expand_env_vars_collecting<F>(s: &str, lookup: F) -> (String, Vec<String>)
where
    F: Fn(&str) -> Option<String>,
{
    let mut out = String::with_capacity(s.len());
    let mut unset: Vec<String> = Vec::new();
    let mut note_unset = |key: &str| {
        if !unset.iter().any(|k| k == key) {
            unset.push(key.to_string());
        }
    };
    let mut chars = s.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch != '$' {
            out.push(ch);
            continue;
        }

        if chars.peek() == Some(&'{') {
            chars.next();
            let mut key = String::new();
            for c in chars.by_ref() {
                if c == '}' {
                    break;
                }
                key.push(c);
            }
            if let Some(value) = lookup(&key).or_else(|| default_env_var_value(&key)) {
                out.push_str(&value);
            } else {
                note_unset(&key);
                out.push_str("${");
                out.push_str(&key);
                out.push('}');
            }
            continue;
        }

        let mut key = String::new();
        while let Some(c) = chars.peek().copied() {
            if c == '_' || c.is_ascii_alphanumeric() {
                key.push(c);
                chars.next();
            } else {
                break;
            }
        }
        if key.is_empty() {
            out.push('$');
        } else if let Some(value) = lookup(&key).or_else(|| default_env_var_value(&key)) {
            out.push_str(&value);
        } else {
            note_unset(&key);
            out.push('$');
            out.push_str(&key);
        }
    }
    (out, unset)
}

fn default_env_var_value(key: &str) -> Option<String> {
    match key {
        "CARGO_HOME" => {
            dirs::home_dir().map(|home| home.join(".cargo").to_string_lossy().into_owned())
        }
        _ => None,
    }
}

pub(crate) fn expand_exclude_pattern(pattern: &str) -> String {
    expand_exclude_pattern_collecting(pattern).0
}

/// Like [`expand_exclude_pattern`] but also returns the names of env vars that
/// were referenced (`$VAR` / `${VAR}`) but unset. Such references stay literal
/// in the returned pattern and match nothing, so a caller folding the pattern
/// into a cache key warns rather than silently keying on a matches-nothing
/// pattern. See [`expand_env_vars_collecting`].
pub(crate) fn expand_exclude_pattern_collecting(pattern: &str) -> (String, Vec<String>) {
    let (expanded, unset) = expand_env_vars_collecting(pattern, |key| std::env::var(key).ok());
    let s = shellexpand(&expanded).to_string_lossy().into_owned();
    (s, unset)
}

fn push_unique(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.iter().any(|p| p == &path) {
        paths.push(path);
    }
}

fn source_candidates(source_path: &Path, roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    push_unique(&mut candidates, source_path.to_path_buf());

    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let absolute = if source_path.is_absolute() {
        source_path.to_path_buf()
    } else {
        cwd.join(source_path)
    };
    push_unique(&mut candidates, absolute.clone());
    if let Ok(canonical) = std::fs::canonicalize(&absolute) {
        push_unique(&mut candidates, canonical);
    }

    for root in roots {
        let root_abs = if root.is_absolute() {
            root.clone()
        } else {
            cwd.join(root)
        };
        let root_forms = [
            root_abs.clone(),
            std::fs::canonicalize(&root_abs).unwrap_or(root_abs),
        ];
        for root_form in root_forms {
            if !source_path.is_absolute() {
                push_unique(&mut candidates, root_form.join(source_path));
            }
            if let Ok(rel) = absolute.strip_prefix(&root_form) {
                push_unique(&mut candidates, rel.to_path_buf());
            }
        }
    }

    candidates
}

fn exclude_pattern_matches(pattern: &str, candidates: &[PathBuf]) -> bool {
    let expanded = expand_exclude_pattern(pattern);
    let Ok(pattern) = glob::Pattern::new(&expanded) else {
        tracing::warn!("ignoring invalid [cache].exclude glob pattern: {expanded}");
        return false;
    };
    candidates
        .iter()
        .any(|candidate| pattern.matches_path(candidate))
}

pub(crate) fn parse_size(s: &str) -> Option<u64> {
    s.parse::<ByteSize>().ok().map(|b| b.as_u64())
}

/// Parse a human size string, warning loudly when it is set but malformed.
///
/// A value `ByteSize` can't parse (a typo'd unit like `100 gigs`, digit
/// grouping like `1_000`, plain garbage) otherwise degrades silently:
/// `Config::load` falls through to the next source and finally to a hardcoded
/// default, so the cap the user asked for is ignored without a word. `source`
/// names where the value came from (e.g. `KACHE_MAX_SIZE`) so the warning
/// points at the right place.
pub(crate) fn parse_size_checked(value: &str, source: &str) -> Option<u64> {
    let parsed = parse_size(value);
    if parsed.is_none() {
        tracing::warn!(
            "ignoring malformed size {value:?} from {source}: expected an integer with an \
             optional unit like `50GiB`, `512MiB`, or `1000000`; falling back to the next \
             configured source or the default"
        );
    }
    parsed
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    fn config_path_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    struct TestEnvGuard {
        previous: Option<OsString>,
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.as_ref() {
                    Some(value) => std::env::set_var("KACHE_CONFIG", value),
                    None => std::env::remove_var("KACHE_CONFIG"),
                }
            }
        }
    }

    fn set_kache_config_for_test(path: &std::path::Path) -> TestEnvGuard {
        let previous = std::env::var_os("KACHE_CONFIG");
        unsafe {
            std::env::set_var("KACHE_CONFIG", path);
        }
        TestEnvGuard { previous }
    }

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
        assert_eq!(parse_size("50GiB"), Some(50 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("1MiB"), Some(1024 * 1024));
        assert!(parse_size("invalid").is_none());
    }

    #[test]
    fn parse_size_checked_rejects_malformed_and_mirrors_parse_size() {
        // Values ByteSize can't parse: a typo'd unit and digit grouping. These
        // are exactly what used to silently degrade to the hardcoded default.
        for bad in ["100 gigs", "1_000", "abc", ""] {
            assert!(parse_size(bad).is_none(), "expected {bad:?} to be invalid");
            assert!(parse_size_checked(bad, "KACHE_MAX_SIZE").is_none());
        }
        // Valid values pass through unchanged.
        assert_eq!(
            parse_size_checked("2GiB", "KACHE_MAX_SIZE"),
            Some(2 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn ignore_env_makes_file_win_over_env() {
        let _lock = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("config.toml");

        // Restore KACHE_KEY_SALT after the test regardless of outcome.
        struct SaltGuard(Option<OsString>);
        impl Drop for SaltGuard {
            fn drop(&mut self) {
                unsafe {
                    match self.0.as_ref() {
                        Some(v) => std::env::set_var("KACHE_KEY_SALT", v),
                        None => std::env::remove_var("KACHE_KEY_SALT"),
                    }
                }
            }
        }
        let _salt = SaltGuard(std::env::var_os("KACHE_KEY_SALT"));
        unsafe { std::env::set_var("KACHE_KEY_SALT", "from-env") };

        let _g = set_kache_config_for_test(&cfg);

        // ignore_env = true: the pinned file's salt wins; the stray env is
        // ignored (the exact footgun the feature defends against).
        std::fs::write(
            &cfg,
            "[cache]\nignore_env = true\nkey_salt = \"from-file\"\n",
        )
        .unwrap();
        let loaded = Config::load().unwrap();
        assert_eq!(loaded.key_salt.as_deref(), Some("from-file"));

        // Without ignore_env, default precedence holds: env wins over the file.
        std::fs::write(&cfg, "[cache]\nkey_salt = \"from-file\"\n").unwrap();
        let loaded = Config::load().unwrap();
        assert_eq!(loaded.key_salt.as_deref(), Some("from-env"));
    }

    #[test]
    fn test_file_config_roundtrip() {
        let config = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                local_only: None,
                modified_input_guard: None,
                ignore_env: None,
                fallback: None,
                key_salt: None,
                path_only_env_vars: None,
                local_store: Some("~/my/cache".to_string()),
                local_max_size: Some("50GiB".to_string()),
                planner: None,
                cache_executables: Some(true),
                clean_incremental: Some(false),
                exclude: Some(vec!["vendor/problem/**".to_string()]),
                event_log_max_size: Some("10MiB".to_string()),
                event_log_keep_lines: Some(500),
                compression_level: Some(3),
                s3_concurrency: Some(8),
                daemon_idle_timeout_secs: None,
                s3_pool_idle_secs: None,
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
            deserialized.cache.as_ref().unwrap().exclude.as_deref(),
            Some(&["vendor/problem/**".to_string()][..])
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
            cc: None,
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
    fn test_key_salt_file_env_precedence() {
        let _guard = config_path_lock();

        // Save/clear the process-global salt env so the test is
        // deterministic, and restore it on the way out.
        let prev_salt = std::env::var_os("KACHE_KEY_SALT");
        let restore_salt = |v: &Option<OsString>| unsafe {
            match v {
                Some(val) => std::env::set_var("KACHE_KEY_SALT", val),
                None => std::env::remove_var("KACHE_KEY_SALT"),
            }
        };
        restore_salt(&None);

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(&cfg_path, "[cache]\nkey_salt = \"from-file\"\n").unwrap();
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        // File value is picked up.
        assert_eq!(
            Config::load().unwrap().key_salt.as_deref(),
            Some("from-file")
        );

        // Env wins over the file.
        unsafe { std::env::set_var("KACHE_KEY_SALT", "from-env") };
        assert_eq!(
            Config::load().unwrap().key_salt.as_deref(),
            Some("from-env")
        );

        // A whitespace-only value is treated as unset (never silently
        // shifts the key).
        unsafe { std::env::set_var("KACHE_KEY_SALT", "   ") };
        assert_eq!(Config::load().unwrap().key_salt, None);

        restore_salt(&prev_salt);
    }

    #[test]
    fn test_cc_extra_allowlist_flags_file_env_precedence() {
        let _guard = config_path_lock();

        let prev = std::env::var_os("KACHE_CC_EXTRA_ALLOWLIST_FLAGS");
        let restore = |v: &Option<OsString>| unsafe {
            match v {
                Some(val) => std::env::set_var("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", val),
                None => std::env::remove_var("KACHE_CC_EXTRA_ALLOWLIST_FLAGS"),
            }
        };
        restore(&None);

        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("config.toml");
        std::fs::write(
            &cfg_path,
            "[cc]\nextra_allowlist_flags = [\"-ffunction-sections\", \"-fdata-sections\"]\n",
        )
        .unwrap();
        let _cfg_guard = set_kache_config_for_test(&cfg_path);

        // File list is picked up.
        assert_eq!(
            Config::load().unwrap().cc_extra_allowlist_flags,
            vec![
                "-ffunction-sections".to_string(),
                "-fdata-sections".to_string()
            ]
        );

        // Env (whitespace-separated) wins over the file and is normalized:
        // trimmed, empties dropped, deduped, first-seen order preserved.
        unsafe {
            std::env::set_var(
                "KACHE_CC_EXTRA_ALLOWLIST_FLAGS",
                "  -fno-rtti   -fno-rtti -fbravo ",
            )
        };
        assert_eq!(
            Config::load().unwrap().cc_extra_allowlist_flags,
            vec!["-fno-rtti".to_string(), "-fbravo".to_string()]
        );

        // An empty env value disables the feature (overrides the file).
        unsafe { std::env::set_var("KACHE_CC_EXTRA_ALLOWLIST_FLAGS", "   ") };
        assert!(Config::load().unwrap().cc_extra_allowlist_flags.is_empty());

        restore(&prev);
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
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        };
        assert_eq!(config.store_dir(), PathBuf::from("/tmp/kache/store"));
    }

    #[test]
    fn test_config_index_db_path() {
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        };
        assert_eq!(config.index_db_path(), PathBuf::from("/tmp/kache/index.db"));
    }

    #[test]
    fn test_config_event_log_path() {
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        };
        assert_eq!(
            config.event_log_path(),
            PathBuf::from("/tmp/kache/events.jsonl")
        );
    }

    #[test]
    fn test_config_socket_path() {
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir: PathBuf::from("/tmp/kache"),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024,
            event_log_keep_lines: 100,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        };
        assert_eq!(
            config.socket_path(),
            PathBuf::from("/tmp/kache/daemon.sock")
        );
    }

    #[test]
    fn test_source_excluded_matches_relative_pattern_against_root() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("crates/problem/src/lib.rs");
        let patterns = vec!["crates/problem/**".to_string()];

        assert!(source_excluded_by_patterns(
            &patterns,
            &source,
            &[dir.path().to_path_buf()]
        ));
    }

    #[test]
    fn test_source_excluded_matches_source_as_passed() {
        let patterns = vec!["src/*.c".to_string()];

        assert!(source_excluded_by_patterns(
            &patterns,
            Path::new("src/foo.c"),
            &[]
        ));
        assert!(!source_excluded_by_patterns(
            &patterns,
            Path::new("include/foo.h"),
            &[]
        ));
    }

    #[test]
    fn test_exclude_expands_cargo_home_default_when_unset() {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/tmp"));
        let cargo_home = home.join(".cargo").to_string_lossy().into_owned();

        let (expanded, _) = expand_env_vars_collecting("$CARGO_HOME/registry/src/**", |_| None);
        assert_eq!(expanded, format!("{cargo_home}/registry/src/**"));

        let (expanded_braced, _) =
            expand_env_vars_collecting("${CARGO_HOME}/registry/src/**", |_| None);
        assert_eq!(expanded_braced, format!("{cargo_home}/registry/src/**"));
    }

    #[test]
    fn expand_collecting_reports_unset_vars_only_once() {
        let (expanded, unset) =
            expand_env_vars_collecting("$MISSING/$MISSING/${ALSO_MISSING}/x", |_| None);
        // Unset refs stay literal so the caller can see they matched nothing.
        assert_eq!(expanded, "$MISSING/$MISSING/${ALSO_MISSING}/x");
        // Deduplicated, in first-seen order.
        assert_eq!(
            unset,
            vec!["MISSING".to_string(), "ALSO_MISSING".to_string()]
        );
    }

    #[test]
    fn expand_collecting_no_unset_when_resolved_or_defaulted() {
        let (expanded, unset) =
            expand_env_vars_collecting("$FOO/x", |k| (k == "FOO").then(|| "bar".to_string()));
        assert_eq!(expanded, "bar/x");
        assert!(unset.is_empty());

        // CARGO_HOME has a built-in default, so it is not reported as unset.
        let (_, unset_default) = expand_env_vars_collecting("$CARGO_HOME/x", |_| None);
        assert!(unset_default.is_empty());
    }

    #[test]
    fn test_load_config_reads_exclude_patterns() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            r#"
[cache]
exclude = ["src/generated/**", "vendor/problem/**"]
"#,
        )
        .unwrap();

        assert!(Config::source_excluded(
            Path::new("src/generated/lib.rs"),
            &[]
        ));
        assert!(Config::source_excluded(
            Path::new("vendor/problem/foo.c"),
            &[]
        ));
        assert!(!Config::source_excluded(Path::new("src/main.rs"), &[]));
    }

    #[test]
    fn test_config_file_path() {
        let path = config_file_path();
        assert!(path.to_string_lossy().contains("kache"));
        assert!(path.to_string_lossy().ends_with("config.toml"));
    }

    #[test]
    fn test_resolve_config_path_prefers_kache_config() {
        let path = resolve_config_path_from(Some(PathBuf::from("/tmp/managed/config.toml")), None);
        assert_eq!(path, PathBuf::from("/tmp/managed/config.toml"));
    }

    #[test]
    fn test_load_and_save_raw_file_config_use_resolved_path() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("managed/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let config = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                local_store: Some("/tmp/managed-cache".to_string()),
                ..Default::default()
            }),
        };

        Config::save_file_config(&config).unwrap();
        assert!(config_path.exists());

        let (loaded, existed) = Config::load_raw_file_config();
        assert!(existed);
        assert_eq!(
            loaded.cache.as_ref().and_then(|c| c.local_store.as_deref()),
            Some("/tmp/managed-cache")
        );
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
        assert_eq!(parse_size("1KiB"), Some(1024));
        assert_eq!(parse_size("10GiB"), Some(10 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("0B"), Some(0));
        assert!(parse_size("").is_none());
        assert!(parse_size("abc").is_none());
    }

    #[test]
    fn test_save_and_load_file_config() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");

        let config = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                local_only: None,
                modified_input_guard: None,
                ignore_env: None,
                fallback: None,
                key_salt: None,
                path_only_env_vars: None,
                local_store: Some("/tmp/my-cache".to_string()),
                local_max_size: Some("10GiB".to_string()),
                planner: None,
                cache_executables: Some(true),
                clean_incremental: None,
                exclude: None,
                event_log_max_size: None,
                event_log_keep_lines: None,
                compression_level: Some(5),
                s3_concurrency: None,
                daemon_idle_timeout_secs: None,
                s3_pool_idle_secs: None,
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

    /// #221: `[cache] local_only` must suppress BOTH the remote and the
    /// planner, even when a bucket + endpoint are configured.
    #[test]
    fn local_only_via_file_suppresses_remote_and_planner() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let file = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                local_only: Some(true),
                remote: Some(RemoteFileConfig {
                    bucket: Some("hermetic-bucket".to_string()),
                    ..Default::default()
                }),
                planner: Some(PlannerFileConfig {
                    endpoint: Some("https://planner.example.com".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();

        let config = Config::load().unwrap();
        assert!(config.local_only, "local_only must be on");
        assert!(
            config.remote.is_none(),
            "remote must be suppressed under local-only, got {:?}",
            config.remote
        );
        assert!(
            Config::load_planner_config().is_none(),
            "planner must be suppressed under local-only"
        );
    }

    /// #221: the `KACHE_LOCAL_ONLY` env var wins over the file — `=0` forces it
    /// off even when the file enables it, `=1` forces it on.
    #[test]
    fn local_only_env_wins_over_file() {
        let _guard = config_path_lock();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let file = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                local_only: Some(true),
                ..Default::default()
            }),
        };
        Config::save_file_config_to(&file, &config_path).unwrap();

        let prev = std::env::var_os("KACHE_LOCAL_ONLY");
        unsafe { std::env::set_var("KACHE_LOCAL_ONLY", "0") };
        let off = Config::load().unwrap().local_only;
        unsafe { std::env::set_var("KACHE_LOCAL_ONLY", "1") };
        let on = Config::load().unwrap().local_only;
        unsafe {
            match prev {
                Some(v) => std::env::set_var("KACHE_LOCAL_ONLY", v),
                None => std::env::remove_var("KACHE_LOCAL_ONLY"),
            }
        }

        assert!(
            !off,
            "KACHE_LOCAL_ONLY=0 must force local-only OFF despite file=true"
        );
        assert!(on, "KACHE_LOCAL_ONLY=1 must force local-only ON");
    }

    #[test]
    fn test_remote_file_config_with_profile() {
        let config = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                planner: None,
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

    #[test]
    fn test_load_planner_config_from_file() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let config = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                planner: Some(PlannerFileConfig {
                    endpoint: Some("https://planner.example.com".to_string()),
                    timeout_ms: Some(1200),
                    token: Some("secret".to_string()),
                }),
                ..Default::default()
            }),
        };

        Config::save_file_config_to(&config, &config_path).unwrap();

        let loaded = Config::load_planner_config().unwrap();
        assert_eq!(loaded.endpoint, "https://planner.example.com");
        assert_eq!(loaded.timeout_ms, 1200);
        assert_eq!(loaded.token.as_deref(), Some("secret"));
    }

    #[test]
    fn test_load_planner_config_env_overrides_file() {
        let _guard = config_path_lock();

        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("kache/config.toml");
        let _env_guard = set_kache_config_for_test(&config_path);

        let config = FileConfig {
            cc: None,
            cache: Some(CacheFileConfig {
                planner: Some(PlannerFileConfig {
                    endpoint: Some("https://planner.example.com".to_string()),
                    timeout_ms: Some(1200),
                    token: Some("secret".to_string()),
                }),
                ..Default::default()
            }),
        };

        Config::save_file_config_to(&config, &config_path).unwrap();

        struct ScopedVar {
            key: &'static str,
            previous: Option<OsString>,
        }

        impl ScopedVar {
            fn set(key: &'static str, value: &str) -> Self {
                let previous = std::env::var_os(key);
                unsafe {
                    std::env::set_var(key, value);
                }
                Self { key, previous }
            }
        }

        impl Drop for ScopedVar {
            fn drop(&mut self) {
                match &self.previous {
                    Some(value) => unsafe {
                        std::env::set_var(self.key, value);
                    },
                    None => unsafe {
                        std::env::remove_var(self.key);
                    },
                }
            }
        }

        let _endpoint = ScopedVar::set("KACHE_PLANNER_ENDPOINT", "https://env.example.com");
        let _timeout = ScopedVar::set("KACHE_PLANNER_TIMEOUT_MS", "400");
        let _token = ScopedVar::set("KACHE_PLANNER_TOKEN", "env-token");

        let loaded = Config::load_planner_config().unwrap();
        assert_eq!(loaded.endpoint, "https://env.example.com");
        assert_eq!(loaded.timeout_ms, 400);
        assert_eq!(loaded.token.as_deref(), Some("env-token"));
    }

    #[test]
    fn test_resolve_config_path_prefers_project_file() {
        let dir = tempfile::tempdir().unwrap();
        let project_root = dir.path().join("workspace");
        let nested_dir = project_root.join("crate/src");
        std::fs::create_dir_all(&nested_dir).unwrap();

        let project_config = project_root.join(PROJECT_CONFIG_NAME);
        std::fs::write(&project_config, "[cache]\n").unwrap();

        let resolved = resolve_config_path_from(None, Some(nested_dir));
        assert_eq!(resolved, project_config);
    }

    #[test]
    fn test_resolve_config_path_env_overrides_project_file() {
        let dir = tempfile::tempdir().unwrap();
        let project_root = dir.path().join("workspace");
        std::fs::create_dir_all(&project_root).unwrap();

        let project_config = project_root.join(PROJECT_CONFIG_NAME);
        let env_config = dir.path().join("explicit-kache.toml");
        std::fs::write(&project_config, "[cache]\n").unwrap();

        let resolved = resolve_config_path_from(Some(env_config.clone()), Some(project_root));
        assert_eq!(resolved, env_config);
    }

    #[test]
    fn test_resolve_config_path_falls_back_to_global_when_no_project_file() {
        let dir = tempfile::tempdir().unwrap();
        let nested_dir = dir.path().join("workspace/crate");
        std::fs::create_dir_all(&nested_dir).unwrap();

        let resolved = resolve_config_path_from(None, Some(nested_dir));
        assert_eq!(resolved, config_file_path());
    }
}
