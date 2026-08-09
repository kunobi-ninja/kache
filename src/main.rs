mod args;
mod atomic;
mod build_intent;
mod cache_fs;
mod cache_key;
mod cli;
mod compile;
mod compiler;
mod config;
mod config_tui;
mod daemon;
mod daemon_local;
mod events;
mod eviction;
mod extra_inputs;
mod fallback_planner;
mod heartbeat;
mod incremental_policy;
mod link;
mod miss_chain;
mod native_archive;
mod opcounts;
mod path_normalizer;
mod planner_client;
mod platform;
mod probe;
mod remote;
mod remote_backend;
mod remote_layout;
mod remote_plan;
mod report;
mod service;
mod shards;
// Both callers (`clean`'s classifier and `compute_link_stats`) are `cfg(unix)`,
// because `nlink` is the signal they combine with. Windows ReFS block-cloning
// has no read-side query to implement this against — `FSCTL_DUPLICATE_EXTENTS`
// is write-only — so the module compiles to its unknown-answer stub there and
// would otherwise read as dead code in non-test builds.
#[cfg_attr(not(unix), allow(dead_code))]
mod sharing;
mod store;
mod transport;
mod tui;
mod wrapper;
mod wrapper_config;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Build version: CI sets KACHE_VERSION from the git tag, local builds use
/// Cargo.toml. Only the leading 'v' is stripped, so a prerelease suffix flows
/// through verbatim (release-candidates publish to crates.io, so the tag — and
/// thus --version — carries the full version, e.g. "v0.5.0-rc.4" -> "0.5.0-rc.4").
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

/// kache: Content-addressed build cache for Rust, C/C++ and more, with S3 and filesystem remotes.
///
/// When invoked as RUSTC_WRAPPER (arg[1] is a path to rustc), kache acts as a
/// transparent build cache. Otherwise, it provides CLI commands for cache management.
#[derive(Parser)]
#[command(name = "kache", version = VERSION, about)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
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
        /// Preview what would be removed without deleting (pairs with --yes)
        #[arg(long, short = 'n')]
        dry_run: bool,

        /// Non-interactive: remove all target/ directories without the selector.
        /// For scripts and cron. Preview first with --dry-run.
        #[arg(long, short = 'y')]
        yes: bool,
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

    /// Synchronize the local cache with its configured remote (pull + push)
    Sync {
        /// Path to Cargo.toml (default: current directory)
        #[arg(long)]
        manifest_path: Option<String>,
        /// Only download from the remote (skip uploads)
        #[arg(long)]
        pull: bool,
        /// Only upload to the remote (skip downloads)
        #[arg(long)]
        push: bool,
        /// Show what would be synced without transferring
        #[arg(long)]
        dry_run: bool,
        /// Pull all remote artifacts (ignore workspace filtering)
        #[arg(long)]
        all: bool,
        /// Scope the pull listing to workspace members (one LIST per member)
        /// instead of one LIST per Cargo.lock dependency crate.
        ///
        /// This only narrows the up-front batch pull. Dependency artifacts are
        /// still resolved on demand during the build — the rustc wrapper fetches
        /// any local miss from the remote via the daemon (a remote hit) and the daemon
        /// prefetches by build intent — so deps are not recompiled. Most useful
        /// when the dependency artifacts are already present locally (e.g. a
        /// prebuilt `cargo chef` deps image whose compiled deps sit in target/),
        /// where they're local hits and never need a remote round-trip; in a plain
        /// setup deps are fetched lazily during the build rather than pre-warmed.
        ///
        /// Errors out if the workspace set can't be resolved (cargo metadata
        /// failed or this isn't a Cargo workspace) rather than silently falling
        /// back to a full remote scan.
        #[arg(long, conflicts_with = "all")]
        workspace: bool,
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

    /// Daemon management. With no subcommand, shows daemon status.
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

    /// Generate a detailed build report (json, trace, markdown, or text)
    Report {
        /// Output format: json, trace, perfetto, chrome-trace, markdown, github, text
        #[arg(long, default_value = "text")]
        format: String,

        /// Time window (e.g. 24h, 7d, 1h)
        #[arg(long, default_value = "24h")]
        since: String,

        /// Only include compiler events from this build tree/root
        #[arg(long)]
        root: Option<PathBuf>,

        /// Write output to a file instead of stdout
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Number of top entries to show
        #[arg(long, default_value = "10")]
        top: usize,
    },

    /// Open the configuration editor
    Config,

    /// Generate shell completion scripts
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[derive(Subcommand)]
enum DaemonCommands {
    /// Show daemon status (alias for bare `kache daemon`)
    Status,
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
    let rustc = std::env::var_os("RUSTC");
    detect_log_mode_with_rustc(env_args, rustc.as_deref())
}

fn detect_log_mode_with_rustc(
    env_args: &[String],
    configured_rustc: Option<&std::ffi::OsStr>,
) -> LogMode {
    if env_args.len() >= 2 {
        let after = &env_args[1..];
        // Real compiler invocation (rustc / cc-family) OR a cc-crate
        // family probe (`kache -E <file>`). Both want wrapper-mode
        // logging (off by default — cargo would otherwise cache the
        // stderr as a stale compiler diagnostic).
        if compiler::is_passthrough_compiler_invocation_with(after, configured_rustc)
            || compiler::is_workspace_wrapper_chain(after)
            || compiler::cc::CcCompiler::recognizes_family_probe(after)
            || compiler::detect_compiler(after).is_some()
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
    //
    // CLI/daemon mode: always enabled at the default `kache.log` path — these
    // run rarely, so 2 syscalls (stat + open) per invocation is fine.
    //
    // Wrapper mode: cargo can fan out hundreds of `kache rustc …` processes
    // per build, so the file layer is OFF by default to avoid the per-crate
    // syscalls. Opt in by setting `KACHE_LOG_FILE` explicitly — useful for
    // diagnostics when cargo would otherwise eat wrapper stderr. The path
    // can be overridden via `KACHE_LOG_FILE_PATH` (e.g. the e2e bench writes
    // a per-phase wrapper.log so each cold/warm phase has a clean log).
    let file_layer = {
        let wrapper_opted_in =
            mode == LogMode::Wrapper && std::env::var_os("KACHE_LOG_FILE").is_some();
        let enable_file_layer = mode != LogMode::Wrapper || wrapper_opted_in;

        if !enable_file_layer {
            None
        } else {
            (|| -> Option<_> {
                let path = std::env::var_os("KACHE_LOG_FILE_PATH")
                    .map(PathBuf::from)
                    .unwrap_or_else(diagnostic_log_path);
                std::fs::create_dir_all(path.parent()?).ok()?;

                // Simple rotation: truncate if file exceeds 5 MB.
                // Skipped when a custom path is provided — the caller owns
                // the file lifecycle (e.g. the bench wipes it per phase).
                if std::env::var_os("KACHE_LOG_FILE_PATH").is_none()
                    && std::fs::metadata(&path).is_ok_and(|m| m.len() > MAX_LOG_BYTES)
                {
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
        }
    };

    tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .init();
}

fn main() -> Result<()> {
    if std::env::var_os("KACHE_FAMILY_PROBE_ACTIVE").is_some() {
        // Prevent unbounded recursion when a probed wrapper calls back into kache.
        return Ok(());
    }

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

    // Config and Completions run before Config::load() (broken/missing config).
    match &cli.command {
        Some(Commands::Config) => return config_tui::run_config_editor(),
        Some(Commands::Completions { shell }) => {
            use clap::CommandFactory;
            use clap_complete::generate;
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_string();
            generate(*shell, &mut cmd, name, &mut std::io::stdout());
            return Ok(());
        }
        _ => {}
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
        Some(Commands::Clean { dry_run, yes }) => cli::clean(dry_run, yes),
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
            workspace,
        }) => cli::sync(
            &config,
            manifest_path.as_deref(),
            pull,
            push,
            dry_run,
            all,
            workspace,
        ),
        Some(Commands::SaveManifest {
            manifest_key,
            namespace,
        }) => cli::save_manifest(&config, manifest_key.as_deref(), namespace.as_deref()),
        Some(Commands::Daemon { command: None }) => service::status(),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Status),
        }) => service::status(),
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
            root,
            output,
            top,
        }) => {
            let hours = parse_duration_hours(&since).unwrap_or(24);
            cli::report(&config, &format, hours, root, output, top)
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
        Some(Commands::Completions { .. }) => unreachable!(),
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
/// Incremental flags are stripped by default (rustc-only — a no-op on cc
/// args) to prevent APFS-related corruption in git worktrees on macOS. The
/// explicit preservation mode isolates the incremental directory instead.
/// Returns the child's exit code.
fn run_compiler_directly(
    config: &config::Config,
    args: &[String],
    preserve_incremental: bool,
) -> Result<i32> {
    if is_cc_compiler_invocation(args) {
        let parsed = compiler::cc::CcArgs::parse(args)?;
        wrapper::refuse_legacy_cc_blob_outputs(config, &parsed)?;
    }

    run_compiler_process_directly(args, preserve_incremental)
}

fn is_cc_compiler_invocation(args: &[String]) -> bool {
    if compiler::is_passthrough_compiler_invocation(args) {
        return false;
    }
    if compiler::is_workspace_wrapper_chain(args) {
        return false;
    }
    compiler::detect_compiler(args).is_some_and(|adapter| adapter.id() == compiler::cc::CC_ID)
}

fn run_compiler_process_directly(args: &[String], preserve_incremental: bool) -> Result<i32> {
    // A previous cache-on build may have restored this crate's outputs
    // as read-only hardlinks into the store (0o444, shared inode).
    // Running the real compiler over them in place would fail with
    // EACCES — and a chmod could not help, since the inode is shared
    // with the store blob (truncating it would poison future restores).
    // Unlink them first: a plain remove breaks the hardlink and leaves
    // the store blob intact, exactly as the enabled cache-miss path does
    // before recompiling. Without this, `KACHE_DISABLED=1` (or a nested
    // re-entrant compile) over a warm target dir breaks the build.

    // C/C++ and unknown compiler response files use shell/MSVC tokenization,
    // not rustc's one-argument-per-line format. Their direct passthrough must
    // therefore remain byte-for-byte argv transport.
    let configured_rustc = args.first().is_some_and(|program| {
        std::env::var_os("RUSTC")
            .is_some_and(|rustc| rustc == std::ffi::OsStr::new(program.as_str()))
    });
    let is_nvcc = args
        .first()
        .and_then(|program| compiler::command_basename(program))
        .map(compiler::strip_windows_exe_suffix)
        .is_some_and(|name| name.eq_ignore_ascii_case("nvcc"));
    let rustc_invocation =
        !is_nvcc && (configured_rustc || rustc_args_for_direct_preclean(args).is_some());
    if !rustc_invocation {
        let program = compiler::resolve_program_on_path(&args[0])
            .unwrap_or_else(|| std::path::PathBuf::from(&args[0]));
        let status = std::process::Command::new(program)
            .args(&args[1..])
            .status()?;
        return Ok(status.code().unwrap_or(1));
    }

    // C/C++ output paths have compiler-specific symlink, hardlink, device,
    // and read-only behavior (#645). Only rustc-shaped invocations may remove
    // previous read-only cache restores before compiling.
    let parsed = args::RustcArgs::parse(args).ok();
    if let Some(parsed) = &parsed {
        compile::pre_clean_outputs(
            parsed.output.as_deref(),
            parsed.out_dir.as_deref(),
            parsed.crate_name.as_deref(),
            parsed.extra_filename.as_deref(),
            &parsed.emit,
        );
    }

    // Standard rustc response files must be expanded before applying the
    // incremental policy; filtering the compact `@file` argv cannot see a
    // `-C incremental=...` stored inside it. Recreate a compact response file
    // after rewriting. An unchanged invocation can safely fall back to its
    // original compact argv; a rewritten invocation fails closed because
    // promoting nested `@file` arguments or exceeding argv limits is unsafe.
    let effective_args = parsed
        .as_ref()
        .map_or(&args[1..], |parsed| parsed.all_args.as_slice());
    let isolated_args = if preserve_incremental {
        match compile::isolate_incremental_flags(effective_args) {
            Some(isolated) => Some(isolated),
            None => {
                tracing::warn!(
                    "[kache] incremental directory has no safe sibling path; stripping incremental flags"
                );
                None
            }
        }
    } else {
        None
    };
    let incremental_preserved = isolated_args.is_some();
    let compiler_args = if let Some(isolated_args) = isolated_args {
        isolated_args
    } else {
        compile::strip_incremental_flags(effective_args)
            .into_iter()
            .cloned()
            .collect()
    };
    let compiler_args_changed = compiler_args != effective_args;
    let response_file = if parsed
        .as_ref()
        .is_some_and(args::RustcArgs::has_expanded_argfiles)
    {
        match compile::RustcResponseFile::new(compiler_args.iter().map(|arg| arg.as_str())) {
            Ok(response) => Some(response),
            Err(error) => {
                if compiler_args_changed {
                    return Err(error).context(
                        "materializing rustc response file after rewriting incremental arguments",
                    );
                }
                tracing::warn!(
                    "failed to materialize expanded rustc response file; using unchanged original argv: {error:#}"
                );
                None
            }
        }
    } else {
        None
    };
    let program = compiler::resolve_program_on_path(&args[0])
        .unwrap_or_else(|| std::path::PathBuf::from(&args[0]));
    let mut command = std::process::Command::new(program);
    if !incremental_preserved {
        command.env("CARGO_INCREMENTAL", "0");
    }
    if let Some(inner) = parsed
        .as_ref()
        .and_then(|parsed| parsed.inner_rustc.as_ref())
    {
        command.arg(inner);
    }
    if let Some(response) = &response_file {
        command.arg(response.argument());
    } else if !compiler_args_changed
        && let Some(parsed) = parsed
            .as_ref()
            .filter(|parsed| parsed.has_expanded_argfiles())
    {
        command.args(parsed.raw_args());
    } else {
        command.args(&compiler_args);
    }
    let status = command.status()?;
    Ok(status.code().unwrap_or(1))
}

fn rustc_args_for_direct_preclean(args: &[String]) -> Option<&[String]> {
    match compiler::detect_compiler(args) {
        Some(adapter) if adapter.id() == compiler::rustc::RUSTC_ID => Some(args),
        Some(_) => None,
        None if compiler::is_workspace_wrapper_chain(args) => Some(&args[1..]),
        None => None,
    }
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
        let config = config::Config::load()?;
        // No preservation here: the outer kache already applied the
        // incremental policy, and `isolate_incremental_flags` is
        // not idempotent — a second rewrite would relocate the state to a
        // `.kache-preserved.kache-preserved` sibling nothing else reuses.
        std::process::exit(run_compiler_directly(&config, args, false)?);
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
        // Caching off — pass straight through to the real compiler. The
        // per-crate force list is a managed-cache policy and is inactive here;
        // only the explicit global preservation mode may retain incremental.
        std::process::exit(run_compiler_directly(
            &config,
            args,
            config.preserve_incremental,
        )?);
    }

    // Compiler-family probe (`kache -E <file>` from the `cc` Rust
    // crate) — handled before compiler dispatch because it is NOT a
    // compiler invocation: it's a passthrough to the system default
    // `cc` so the cc crate sees the real underlying compiler's
    // preprocessor output.
    if compiler::cc::CcCompiler::recognizes_family_probe(args) {
        std::process::exit(wrapper::run_cc_probe(args)?);
    }

    let direct_passthrough = compiler::is_workspace_wrapper_chain(args)
        || (compiler::is_passthrough_compiler_invocation(args)
            && !compiler::rustc::RustcCompiler::recognizes(args));
    if direct_passthrough {
        tracing::debug!(
            program = ?args.first(),
            "compiler has no cache adapter; passing through uncached"
        );
        std::process::exit(run_compiler_directly(
            &config,
            args,
            config.preserve_incremental,
        )?);
    }

    let Some(adapter) = compiler::detect_compiler(args) else {
        anyhow::bail!(
            "wrapper-mode dispatched but no compiler adapter matched argv[0] = {:?}",
            args.first()
        );
    };
    let exit_code = if adapter.id() == compiler::rustc::RUSTC_ID {
        wrapper::run(&config, args)?
    } else if adapter.id() == compiler::cc::CC_ID {
        wrapper::run_cc(&config, args)?
    } else {
        anyhow::bail!(
            "detected compiler adapter {} ({}) has no wrapper dispatch",
            adapter.id(),
            adapter.display_name()
        );
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

    #[cfg(unix)]
    fn write_success_program(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;

        let shell = compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        std::fs::write(path, format!("#!{}\nexit 0\n", shell.display())).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[cfg(unix)]
    fn run_generated_compiler_process_directly(
        args: &[String],
        preserve_incremental: bool,
    ) -> Result<i32> {
        match run_compiler_process_directly(args, preserve_incremental) {
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.raw_os_error() == Some(libc::ETXTBSY)) =>
            {
                // Some Linux filesystems briefly keep a just-written test
                // executable busy even after its writer has closed.
                std::thread::sleep(std::time::Duration::from_millis(20));
                run_compiler_process_directly(args, preserve_incremental)
            }
            result => result,
        }
    }

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
        assert_eq!(
            run_compiler_process_directly(&["true".to_string()], false).unwrap(),
            0
        );
        assert_eq!(
            run_compiler_process_directly(&["false".to_string()], false).unwrap(),
            1
        );
    }

    #[test]
    fn rustc_args_for_direct_preclean_truth_table() {
        let rustc = vec!["rustc".to_string()];
        assert_eq!(
            rustc_args_for_direct_preclean(&rustc),
            Some(rustc.as_slice())
        );

        let cc = vec!["cc".to_string()];
        assert_eq!(rustc_args_for_direct_preclean(&cc), None);

        let chained = vec![
            "/tmp/outer-wrapper".to_string(),
            "rustc".to_string(),
            "--crate-name".to_string(),
        ];
        assert_eq!(
            rustc_args_for_direct_preclean(&chained),
            Some(&chained[1..])
        );

        let unknown = vec![
            "/tmp/outer-wrapper".to_string(),
            "other-tool".to_string(),
            "--crate-name".to_string(),
        ];
        assert_eq!(rustc_args_for_direct_preclean(&unknown), None);
    }

    #[test]
    fn direct_cc_safety_check_applies_only_to_cc_invocations() {
        assert!(is_cc_compiler_invocation(&["cc".to_string()]));
        assert!(!is_cc_compiler_invocation(&["rustc".to_string()]));
        assert!(!is_cc_compiler_invocation(&["other-tool".to_string()]));
        assert!(!is_cc_compiler_invocation(&["nvcc".to_string()]));
    }

    #[cfg(unix)]
    #[test]
    fn run_compiler_directly_pre_cleans_readonly_restores() {
        // Regression for #238: a direct-exec passthrough (KACHE_DISABLED
        // / nested re-entrancy) over a target dir that still holds
        // read-only hardlinked restores must unlink them first, or the
        // real compiler hits EACCES overwriting them in place.
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake_rustc = dir.path().join("rustc");
        write_success_program(&fake_rustc);
        let restored = dir.path().join("libfoo.rlib");
        std::fs::write(&restored, b"cached").unwrap();
        std::fs::set_permissions(&restored, std::fs::Permissions::from_mode(0o444)).unwrap();
        assert!(
            std::fs::metadata(&restored)
                .unwrap()
                .permissions()
                .readonly()
        );

        // A `rustc`-named shell shim stands in for the compiler; the
        // rustc-shaped flags drive the pre-clean's out-dir branch.
        let code = run_generated_compiler_process_directly(
            &[
                fake_rustc.to_string_lossy().into_owned(),
                "--crate-name".to_string(),
                "foo".to_string(),
                "--out-dir".to_string(),
                dir.path().to_string_lossy().into_owned(),
            ],
            false,
        )
        .unwrap();

        assert_eq!(code, 0);
        assert!(
            !restored.exists(),
            "read-only restore should have been unlinked before exec"
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_compiler_directly_pre_cleans_workspace_wrapper_chain_restores() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let outer_wrapper = dir.path().join("outer-wrapper");
        write_success_program(&outer_wrapper);
        let fake_rustc = dir.path().join("rustc");
        write_success_program(&fake_rustc);
        let restored = dir.path().join("libfoo.rlib");
        std::fs::write(&restored, b"cached").unwrap();
        std::fs::set_permissions(&restored, std::fs::Permissions::from_mode(0o444)).unwrap();

        let code = run_generated_compiler_process_directly(
            &[
                outer_wrapper.to_string_lossy().into_owned(),
                fake_rustc.to_string_lossy().into_owned(),
                "--crate-name".to_string(),
                "foo".to_string(),
                "--out-dir".to_string(),
                dir.path().to_string_lossy().into_owned(),
            ],
            false,
        )
        .unwrap();

        assert_eq!(code, 0);
        assert!(
            !restored.exists(),
            "workspace-wrapper chains must pre-clean the inner rustc outputs"
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_compiler_directly_keeps_non_rustc_response_argv_verbatim() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let compiler = dir.path().join("fake-cc");
        let dump = dir.path().join("argv.txt");
        let response = dir.path().join("cc.args");
        std::fs::write(
            &compiler,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"{}\"\n",
                dump.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&compiler, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(&response, "-DNAME='two words'\n").unwrap();

        let direct = "direct argument with spaces".to_string();
        let response_arg = format!("@{}", response.display());
        let code = run_generated_compiler_process_directly(
            &[
                compiler.to_string_lossy().into_owned(),
                direct.clone(),
                response_arg.clone(),
            ],
            false,
        )
        .unwrap();

        assert_eq!(code, 0);
        assert_eq!(
            std::fs::read_to_string(dump).unwrap(),
            format!("{direct}\n{response_arg}\n")
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_compiler_directly_preserves_cc_readonly_output() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let fake_cc = dir.path().join("cc");
        write_success_program(&fake_cc);
        let output = dir.path().join("user-owned.o");
        std::fs::write(&output, b"original").unwrap();
        std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o444)).unwrap();
        let before = std::fs::metadata(&output).unwrap();

        let code = run_generated_compiler_process_directly(
            &[
                fake_cc.to_string_lossy().into_owned(),
                "-c".to_string(),
                "foo.c".to_string(),
                "-o".to_string(),
                output.to_string_lossy().into_owned(),
            ],
            false,
        )
        .unwrap();

        assert_eq!(code, 0);
        let after = std::fs::metadata(&output).unwrap();
        assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
        assert_eq!(std::fs::read(&output).unwrap(), b"original");
    }

    #[cfg(unix)]
    #[test]
    fn run_compiler_directly_rewrites_double_wrapper_rustc_response_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let outer = dir.path().join("workspace-wrapper");
        let inner = dir.path().join("rustc");
        let dump = dir.path().join("argv.txt");
        let response = dir.path().join("rustc.args");
        let incremental = dir.path().join("target/debug/incremental/unit");
        std::fs::write(
            &outer,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$1\" > \"{}\"\nshift\nfor arg in \"$@\"; do\n  case \"$arg\" in\n    @*) cat \"${{arg#@}}\" >> \"{}\";;\n    *) printf '%s\\n' \"$arg\" >> \"{}\";;\n  esac\ndone\n",
                dump.display(),
                dump.display(),
                dump.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&outer, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(
            &response,
            format!(
                "--crate-name\nfixture\n-Cincremental={}\n",
                incremental.display()
            ),
        )
        .unwrap();

        let code = run_generated_compiler_process_directly(
            &[
                outer.to_string_lossy().into_owned(),
                inner.to_string_lossy().into_owned(),
                format!("@{}", response.display()),
            ],
            true,
        )
        .unwrap();

        assert_eq!(code, 0);
        let argv = std::fs::read_to_string(dump).unwrap();
        assert!(
            argv.starts_with(&format!("{}\n", inner.display())),
            "{argv}"
        );
        assert!(
            argv.contains(&format!(
                "-Cincremental={}.kache-preserved",
                incremental.display()
            )),
            "{argv}"
        );
        assert!(!argv.contains(&format!("-Cincremental={}\n", incremental.display())));
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
        assert_eq!(
            detect_log_mode(&[
                "kache".into(),
                "C:/Users/dev/.mozbuild/clang/bin/clang.exe".into(),
                "-E".into(),
                "conftest.c".into()
            ]),
            LogMode::Wrapper
        );
        // Issue #514: a cross-compiler basename is a wrapper invocation, not a
        // clap subcommand. Both bare and path-qualified forms take this route.
        assert_eq!(
            detect_log_mode(&[
                "kache".into(),
                "arm-linux-gnueabihf-gcc".into(),
                "--version".into(),
            ]),
            LogMode::Wrapper
        );
        assert_eq!(
            detect_log_mode(&[
                "kache".into(),
                "/opt/cross/bin/aarch64-linux-gnu-g++-13".into(),
                "-c".into(),
                "foo.cc".into(),
            ]),
            LogMode::Wrapper
        );
        // Regression for issue #287: `cargo clippy` on Windows invokes the
        // wrapper as `kache <…>\clippy-driver.exe rustc -vV`. The whole
        // dispatch (not just recognizes()) must select Wrapper mode — before
        // the fix this fell through to Cli and clap-errored with
        // "unrecognized subcommand". The backslash basename + `.exe` suffix
        // resolve identically on every host OS.
        assert_eq!(
            detect_log_mode(&[
                "kache".into(),
                r"G:\.rustup\toolchains\nightly-x86_64-pc-windows-msvc\bin\clippy-driver.exe"
                    .into(),
                "rustc".into(),
                "-vV".into(),
            ]),
            LogMode::Wrapper
        );
        assert_eq!(
            detect_log_mode(&[
                "kache".into(),
                r"C:\Program Files\Rust\bin\rustc.exe".into(),
                "--crate-name".into(),
            ]),
            LogMode::Wrapper
        );
        // Issue #656: Kani replaces Cargo's rustc with `kani-compiler` while
        // preserving RUSTC_WRAPPER, so the compiler path must enter wrapper
        // mode and be passed through instead of being parsed as a CLI command.
        assert_eq!(
            detect_log_mode_with_rustc(
                &[
                    "kache".into(),
                    "/home/user/.kani/kani-0.67.0/bin/kani-compiler".into(),
                    "-vV".into(),
                ],
                Some(std::ffi::OsStr::new(
                    "/home/user/.kani/kani-0.67.0/bin/kani-compiler",
                )),
            ),
            LogMode::Wrapper
        );
        // The same generic passthrough revives the existing nvcc compatibility
        // path, which was previously unreachable because log-mode detection
        // rejected nvcc before `run_wrapper_mode` could pass it through.
        assert_eq!(
            detect_log_mode_with_rustc(
                &[
                    "kache".into(),
                    "/usr/local/cuda/bin/nvcc".into(),
                    "-c".into(),
                    "kernel.cu".into(),
                ],
                None,
            ),
            LogMode::Wrapper
        );
        // Issue #505: unrecognized RUSTC_WORKSPACE_WRAPPER tools like
        // dylint-driver. Cargo passes `kache <wrapper-path> rustc <args>`.
        // Detection keys off the inner rustc (position 2), not the
        // wrapper's name — so any workspace-wrapper tool works.
        assert_eq!(
            detect_log_mode(&[
                "kache".into(),
                "/Users/dev/.dylint_drivers/nightly/dylint-driver".into(),
                "rustc".into(),
                "--crate-name".into(),
            ]),
            LogMode::Wrapper
        );
        // Negative: no path separator in arg[1] → CLI subcommand, not a wrapper.
        assert_eq!(
            detect_log_mode(&["kache".into(), "init".into(), "rustc-project".into(),]),
            LogMode::Cli
        );
    }
}
