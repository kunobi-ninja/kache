mod args;
mod build_intent;
mod cache_key;
mod cli;
mod compile;
mod compiler;
mod config;
mod config_tui;
mod daemon;
mod events;
mod fallback_planner;
mod link;
mod opcounts;
mod path_normalizer;
mod planner_client;
mod platform;
mod probe;
mod remote;
mod remote_layout;
mod remote_plan;
mod report;
mod service;
mod shards;
mod store;
mod transport;
mod tui;
mod wrapper;
mod wrapper_config;

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Build version: CI sets KACHE_VERSION from the git tag (e.g. "v0.1.0"), local builds
/// use Cargo.toml. The leading 'v' is stripped if present.
pub const VERSION: &str = {
    const RAW: &str = match option_env!("KACHE_VERSION") {
        Some(v) => v,
        None => env!("CARGO_PKG_VERSION"),
    };
    let b = RAW.as_bytes();
    if b.len() > 1 && b[0] == b'v' {
        // SAFETY: removing a leading ASCII 'v' preserves UTF-8 validity
        unsafe { core::str::from_utf8_unchecked(b.split_at(1).1) }
    } else {
        RAW
    }
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

    /// Interactive setup: configure cargo wrapper, install and start the daemon
    Init {
        /// Accept all default answers (non-interactive)
        #[arg(long, short = 'y')]
        yes: bool,

        /// Do not install the daemon as a login service
        #[arg(long)]
        no_service: bool,

        /// Print what would change without modifying anything
        #[arg(long)]
        check: bool,
    },

    /// Diagnose setup issues and verify cache integrity
    Doctor {
        /// Auto-fix issues (migrate from sccache, repair config)
        #[arg(long)]
        fix: bool,

        /// Also remove sccache cache and binary (requires --fix)
        #[arg(long, requires = "fix")]
        purge_sccache: bool,

        /// Verify cache integrity (entries, blobs, metadata)
        #[arg(long)]
        verify: bool,

        /// Also verify blob checksums (slower, implies --verify)
        #[arg(long)]
        checksums: bool,

        /// Remove corrupted entries (implies --verify)
        #[arg(long)]
        repair: bool,
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
        /// uploads content-addressed shards alongside the monolithic build manifest.
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

    /// Generate a detailed build report (json, markdown, or text)
    Report {
        /// Output format: json, markdown, github, text
        #[arg(long, default_value = "text")]
        format: String,

        /// Time window (e.g. 24h, 7d, 1h)
        #[arg(long, default_value = "24h")]
        since: String,

        /// Write output to a file instead of stdout
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Number of top entries to show
        #[arg(long, default_value = "10")]
        top: usize,
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
    /// Restart daemon (via launchd/systemd if installed, else manual stop+start)
    Restart,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogMode {
    Wrapper,
    Cli,
    TerminalUi,
}

fn detect_log_mode(env_args: &[String]) -> LogMode {
    if env_args.len() >= 2 {
        let after = &env_args[1..];
        // Real compiler invocation (rustc / cc-family) OR a cc-crate
        // family probe (`kache -E <file>`). Both want wrapper-mode
        // logging (off by default — cargo would otherwise cache the
        // stderr as a stale compiler diagnostic).
        if compiler::detect_compiler(after).is_some()
            || compiler::cc::CcCompiler::recognizes_family_probe(after)
        {
            return LogMode::Wrapper;
        }
    }

    match env_args.get(1).map(String::as_str) {
        Some("monitor" | "config") => LogMode::TerminalUi,
        _ => LogMode::Cli,
    }
}

fn init_logging(mode: LogMode) {
    use std::sync::Mutex;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    // Wrapper mode: cargo captures RUSTC_WRAPPER stderr and caches it as compiler
    // diagnostics, replaying stale warnings on every subsequent build.  Default to
    // silent; users can still opt in via KACHE_LOG for one-off debugging.
    // TUI mode: owns the terminal, stderr must stay silent.
    let stderr_layer = if mode == LogMode::TerminalUi {
        None
    } else {
        let default_filter = if mode == LogMode::Wrapper {
            "off"
        } else {
            "kache=warn"
        };
        let stderr_filter = EnvFilter::try_from_env("KACHE_LOG")
            .unwrap_or_else(|_| default_filter.parse().unwrap());
        Some(
            fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(stderr_filter),
        )
    };

    // File layer: persistent log at info level (overridable via KACHE_LOG_FILE).
    // Skipped in wrapper mode to avoid 2 extra syscalls (stat + open) per crate —
    // the daemon already captures all important events.  CLI/daemon mode gets
    // the file layer for diagnostics.
    let file_layer = if mode == LogMode::Wrapper {
        None
    } else {
        (|| -> Option<_> {
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
        })()
    };

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
}

fn main() -> Result<()> {
    let env_args: Vec<String> = std::env::args().collect();
    let log_mode = detect_log_mode(&env_args);

    // Detect RUSTC_WRAPPER mode: cargo passes the rustc path as arg[1]
    // In this mode: argv[0]=kache, argv[1]=rustc, argv[2..]=rustc args
    let is_wrapper = log_mode == LogMode::Wrapper;
    init_logging(log_mode);

    if is_wrapper {
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
        Some(Commands::Init {
            yes,
            no_service,
            check,
        }) => cli::init(yes, no_service, check),
        Some(Commands::Doctor {
            fix,
            purge_sccache,
            verify,
            checksums,
            repair,
        }) => cli::doctor(
            fix,
            purge_sccache,
            verify || checksums || repair,
            checksums,
            repair,
        ),
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
            command: Some(DaemonCommands::Restart),
        }) => match daemon::restart(&config)? {
            true => Ok(()),
            false => std::process::exit(1),
        },
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Install),
        }) => service::install(),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Uninstall),
        }) => service::uninstall(),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Log),
        }) => service::log(),
        Some(Commands::Report {
            format,
            since,
            output,
            top,
        }) => {
            let hours = parse_duration_hours(&since).unwrap_or(24);
            cli::report(&config, &format, hours, output, top)
        }
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
            // No subcommand — print help. New users often find an unexpected TUI
            // disorienting; they can still launch it explicitly with `kache monitor`.
            use clap::CommandFactory;
            Cli::command().print_help()?;
            println!();
            Ok(())
        }
    }
}

/// Environment breadcrumb a kache wrapper sets before spawning any
/// child compiler. A kache process that sees it already set is running
/// *inside* another kache — see [`run_wrapper_mode`]'s re-entrancy
/// guard.
const KACHE_ACTIVE_ENV: &str = "KACHE_ACTIVE";

/// Run the requested compiler directly, with no caching: `args[0]` is
/// the compiler, `args[1..]` its arguments.
///
/// Incremental flags are stripped (rustc-only — a no-op on cc args) to
/// prevent APFS-related corruption in git worktrees on macOS. Returns
/// the child's exit code.
fn run_compiler_directly(args: &[String]) -> Result<i32> {
    let filtered = compile::strip_incremental_flags(&args[1..]);
    let status = std::process::Command::new(&args[0])
        .args(&filtered)
        .status()?;
    Ok(status.code().unwrap_or(1))
}

fn run_wrapper_mode(args: &[String]) -> Result<()> {
    // Re-entrancy guard. If a child compiler kache spawns is itself a
    // kache wrapper — e.g. `cc` on PATH shadowed by `kache cc` — the
    // nested invocation must run the real compiler directly instead of
    // looping kache → cc → kache → … kache sets KACHE_ACTIVE before
    // spawning any child, so a wrapper that already sees it set knows
    // it is nested. This runs before the `disabled` check below, so
    // the loop is broken even when caching is turned off.
    if std::env::var_os(KACHE_ACTIVE_ENV).is_some() {
        std::process::exit(run_compiler_directly(args)?);
    }
    // SAFETY: wrapper startup is single-threaded — no threads are
    // spawned before this point — so mutating the process environment
    // here is free of data races. Children inherit KACHE_ACTIVE, and a
    // nested kache hits the guard above.
    unsafe {
        std::env::set_var(KACHE_ACTIVE_ENV, "1");
    }

    let config = config::Config::load()?;

    if config.disabled {
        // Caching off — pass straight through to the real compiler.
        std::process::exit(run_compiler_directly(args)?);
    }

    // Compiler-family probe (`kache -E <file>` from the `cc` Rust
    // crate) — handled before compiler dispatch because it is NOT a
    // compiler invocation: it's a passthrough to the system default
    // `cc` so the cc crate sees the real underlying compiler's
    // preprocessor output. Keeping this branch separate from the
    // compiler match keeps `CompilerKind` semantically clean
    // ("which compiler family is this?", not "which kind of thing
    // should kache do?").
    if compiler::cc::CcCompiler::recognizes_family_probe(args) {
        std::process::exit(wrapper::run_cc_probe(args)?);
    }

    // Dispatch by detected compiler kind. detect_log_mode already verified
    // there's a recognized compiler at args[0], so the None branch is just
    // defensive (matches detect_compiler's contract).
    let exit_code = match compiler::detect_compiler(args) {
        Some(compiler::CompilerKind::Rustc) => wrapper::run(&config, args)?,
        Some(compiler::CompilerKind::Cc) => wrapper::run_cc(&config, args)?,
        None => anyhow::bail!(
            "wrapper-mode dispatched but no compiler matched argv[0] = {:?}",
            args.first()
        ),
    };
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
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_hours("7d"), Some(168));
        assert_eq!(parse_duration_hours("24h"), Some(24));
        assert_eq!(parse_duration_hours("1h"), Some(1));
        assert_eq!(parse_duration_hours("48"), Some(48));
        assert_eq!(parse_duration_hours("invalid"), None);
    }

    #[cfg(unix)]
    #[test]
    fn run_compiler_directly_propagates_exit_code() {
        // `args[0]` is the program, `args[1..]` its args. The unix
        // `true` / `false` utilities are the simplest stand-ins for a
        // compiler: they exit 0 / 1 and ignore their arguments.
        assert_eq!(run_compiler_directly(&["true".to_string()]).unwrap(), 0);
        assert_eq!(run_compiler_directly(&["false".to_string()]).unwrap(), 1);
    }

    #[test]
    fn test_detect_log_mode() {
        assert_eq!(detect_log_mode(&["kache".into()]), LogMode::Cli);
        assert_eq!(
            detect_log_mode(&["kache".into(), "monitor".into()]),
            LogMode::TerminalUi
        );
        assert_eq!(
            detect_log_mode(&["kache".into(), "config".into()]),
            LogMode::TerminalUi
        );
        assert_eq!(
            detect_log_mode(&["kache".into(), "stats".into()]),
            LogMode::Cli
        );
        assert_eq!(
            detect_log_mode(&["kache".into(), "rustc".into(), "--crate-name".into()]),
            LogMode::Wrapper
        );
    }
}
