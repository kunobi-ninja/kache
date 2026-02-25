mod args;
mod cache_key;
mod cli;
mod compile;
mod config;
mod config_tui;
mod daemon;
mod events;
mod link;
mod remote;
mod service;
mod shards;
mod store;
mod tui;
mod wrapper;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Build version: CI sets KACHE_VERSION from the git tag, local builds use Cargo.toml.
pub const VERSION: &str = match option_env!("KACHE_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};

/// kache: Content-addressed Rust build cache with hardlinks and S3 remote storage.
///
/// When invoked as RUSTC_WRAPPER (arg[1] is a path to rustc), kache acts as a
/// transparent build cache. Otherwise, it provides CLI commands for cache management.
#[derive(Parser)]
#[command(name = "kache", version = VERSION, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// List cache entries, or show details for one crate
    List {
        /// Crate name to show details for (omit to list all)
        crate_name: Option<String>,

        /// Sort by: name, size, hits, age
        #[arg(long, default_value = "name")]
        sort: String,
    },

    /// Run garbage collection (LRU eviction)
    Gc {
        /// Evict entries older than this duration (e.g. 7d, 24h)
        #[arg(long)]
        max_age: Option<String>,
    },

    /// Wipe entire cache or entries for a specific crate
    Purge {
        /// Only purge entries for this crate
        #[arg(long)]
        crate_name: Option<String>,
    },

    /// Recursively find and remove target/ directories under the current directory
    Clean {
        /// Preview what would be removed without deleting
        #[arg(long)]
        dry_run: bool,
    },

    /// Diagnose setup issues
    Doctor {
        /// Auto-fix issues (migrate from sccache, repair config)
        #[arg(long)]
        fix: bool,

        /// Also remove sccache cache and binary (requires --fix)
        #[arg(long, requires = "fix")]
        purge_sccache: bool,
    },

    /// Synchronize local cache with S3 remote (pull + push)
    Sync {
        /// Path to Cargo.toml (default: current directory)
        #[arg(long)]
        manifest_path: Option<String>,
        /// Only download from S3 (skip uploads)
        #[arg(long)]
        pull: bool,
        /// Only upload to S3 (skip downloads)
        #[arg(long)]
        push: bool,
        /// Show what would be synced without transferring
        #[arg(long)]
        dry_run: bool,
        /// Pull all artifacts from S3 (ignore workspace filtering)
        #[arg(long)]
        all: bool,
    },

    /// Save a build manifest for future prefetch warming
    SaveManifest {
        /// Override manifest key (default: host target triple)
        #[arg(long)]
        manifest_key: Option<String>,
        /// Shard namespace: target/rustc_hash/profile. If set and Cargo.lock exists,
        /// uploads content-addressed shards alongside the legacy manifest.
        #[arg(long)]
        namespace: Option<String>,
    },

    /// Daemon management (status, start, stop, install, uninstall, log)
    #[command(subcommand_required = false)]
    Daemon {
        #[command(subcommand)]
        command: Option<DaemonCommands>,
    },

    /// Live TUI dashboard for monitoring builds
    Monitor {
        /// Show events from the last N hours
        #[arg(long)]
        since: Option<String>,
    },

    /// Show cache stats summary (non-interactive)
    Stats {
        /// Show events from the last N hours (e.g. 24h, 1h, 7d)
        #[arg(long, default_value = "24h")]
        since: String,
    },

    /// Diagnose why a specific crate missed the cache
    WhyMiss {
        /// Crate name to investigate
        crate_name: String,
    },

    /// Open the configuration editor
    Config,
}

#[derive(Subcommand)]
enum DaemonCommands {
    /// Run the daemon server in the foreground
    Run,
    /// Start daemon in background (returns immediately)
    Start,
    /// Stop a running daemon
    Stop,
    /// Install daemon as a system service (launchd/systemd)
    Install,
    /// Remove the daemon service
    Uninstall,
    /// Stream daemon logs
    Log,
}

/// Diagnostic log file path.
/// macOS: `~/Library/Logs/kache/kache.log` (visible in Console.app).
/// Linux/other: `<cache_dir>/kache.log`.
pub(crate) fn diagnostic_log_path() -> PathBuf {
    if cfg!(target_os = "macos") {
        dirs::home_dir()
            .unwrap_or_default()
            .join("Library/Logs/kache/kache.log")
    } else {
        config::default_cache_dir().join("kache.log")
    }
}

const MAX_LOG_BYTES: u64 = 5 * 1024 * 1024; // 5 MB

fn init_logging() {
    use std::sync::Mutex;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    // Stderr layer: controlled by KACHE_LOG env var, default warn.
    // This is what appears in build output (wrapper) or gets captured by launchd (daemon).
    let stderr_filter =
        EnvFilter::try_from_env("KACHE_LOG").unwrap_or_else(|_| "kache=warn".parse().unwrap());
    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(stderr_filter);

    // File layer: persistent log at info level (overridable via KACHE_LOG_FILE).
    // Info captures operations and failures without per-crate debug noise.
    // Set KACHE_LOG_FILE=kache=debug for retry-level diagnostics.
    // Silently skipped if the file can't be opened — never fail the build over logging.
    let file_layer = (|| -> Option<_> {
        let path = diagnostic_log_path();
        std::fs::create_dir_all(path.parent()?).ok()?;

        // Simple rotation: truncate if file exceeds 5 MB.
        if std::fs::metadata(&path).is_ok_and(|m| m.len() > MAX_LOG_BYTES) {
            let _ = std::fs::write(&path, b"--- log rotated ---\n");
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .ok()?;

        let file_filter = EnvFilter::try_from_env("KACHE_LOG_FILE")
            .unwrap_or_else(|_| "kache=info".parse().unwrap());

        Some(
            fmt::layer()
                .with_ansi(false)
                .with_writer(Mutex::new(file))
                .with_filter(file_filter),
        )
    })();

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
}

fn main() -> Result<()> {
    init_logging();

    let env_args: Vec<String> = std::env::args().collect();

    // Detect RUSTC_WRAPPER mode: cargo passes the rustc path as arg[1]
    // In this mode: argv[0]=kache, argv[1]=rustc, argv[2..]=rustc args
    if env_args.len() >= 2 && args::looks_like_rustc(&env_args[1]) {
        return run_wrapper_mode(&env_args[1..]);
    }

    // CLI mode: parse subcommands
    let cli = Cli::parse();

    // Config command loads its own raw config — handle before Config::load()
    // so a broken config file can still be fixed via the editor.
    if matches!(cli.command, Some(Commands::Config)) {
        return config_tui::run_config_editor();
    }

    let config = config::Config::load()?;

    match cli.command {
        Some(Commands::List { crate_name, sort }) => {
            cli::list(&config, crate_name.as_deref(), &sort)
        }
        Some(Commands::Gc { max_age }) => {
            let hours = max_age.as_deref().and_then(parse_duration_hours);
            cli::gc(&config, hours)
        }
        Some(Commands::Purge { crate_name }) => cli::purge(&config, crate_name.as_deref()),
        Some(Commands::Clean { dry_run }) => cli::clean(dry_run),
        Some(Commands::Doctor { fix, purge_sccache }) => cli::doctor(fix, purge_sccache),
        Some(Commands::Sync {
            manifest_path,
            pull,
            push,
            dry_run,
            all,
        }) => cli::sync(&config, manifest_path.as_deref(), pull, push, dry_run, all),
        Some(Commands::SaveManifest {
            manifest_key,
            namespace,
        }) => cli::save_manifest(&config, manifest_key.as_deref(), namespace.as_deref()),
        Some(Commands::Daemon { command: None }) => service::status(),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Run),
        }) => daemon::run_server(&config),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Start),
        }) => match daemon::start_daemon_background() {
            Ok(true) => {
                eprintln!("daemon started");
                Ok(())
            }
            Ok(false) => {
                eprintln!("daemon did not start within timeout");
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("failed to start daemon: {e}");
                std::process::exit(1);
            }
        },
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Stop),
        }) => daemon::send_shutdown_request(&config),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Install),
        }) => service::install(),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Uninstall),
        }) => service::uninstall(),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Log),
        }) => service::log(),
        Some(Commands::Stats { since }) => {
            let hours = parse_duration_hours(&since);
            cli::stats(&config, hours)
        }
        Some(Commands::WhyMiss { crate_name }) => cli::why_miss(&config, &crate_name),
        Some(Commands::Monitor { since }) => {
            let hours = since.as_deref().and_then(parse_duration_hours);
            tui::run_monitor(&config, hours)
        }
        Some(Commands::Config) => unreachable!(),
        None => {
            // No subcommand — open the TUI monitor
            tui::run_monitor(&config, None)
        }
    }
}

fn run_wrapper_mode(args: &[String]) -> Result<()> {
    let config = config::Config::load()?;

    if config.disabled {
        // Pass through to rustc directly, but still strip incremental flags
        // to prevent APFS-related corruption in git worktrees on macOS.
        let filtered = compile::strip_incremental_flags(&args[1..]);
        let status = std::process::Command::new(&args[0])
            .args(&filtered)
            .status()?;
        std::process::exit(status.code().unwrap_or(1));
    }

    let exit_code = wrapper::run(&config, args)?;
    std::process::exit(exit_code);
}

/// Parse a duration string like "7d", "24h", "1h" into hours.
fn parse_duration_hours(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(days) = s.strip_suffix('d') {
        days.parse::<u64>().ok().map(|d| d * 24)
    } else if let Some(hours) = s.strip_suffix('h') {
        hours.parse::<u64>().ok()
    } else {
        s.parse::<u64>().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_looks_like_rustc() {
        assert!(args::looks_like_rustc("rustc"));
        assert!(args::looks_like_rustc("/usr/bin/rustc"));
        assert!(args::looks_like_rustc(
            "/home/user/.rustup/toolchains/stable/bin/rustc"
        ));
        assert!(args::looks_like_rustc("clippy-driver"));
        assert!(args::looks_like_rustc("/path/to/bin/clippy-driver"));
        assert!(!args::looks_like_rustc("gcc"));
        assert!(!args::looks_like_rustc("--crate-name"));
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_hours("7d"), Some(168));
        assert_eq!(parse_duration_hours("24h"), Some(24));
        assert_eq!(parse_duration_hours("1h"), Some(1));
        assert_eq!(parse_duration_hours("48"), Some(48));
        assert_eq!(parse_duration_hours("invalid"), None);
    }
}
