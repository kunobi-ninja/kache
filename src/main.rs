mod args;
mod blob_heal;
mod build_script;
use kache_store::atomic;
mod build_intent;
mod build_reports;
mod cache_fs;
mod cache_key;
mod cargo_env;
mod cargo_layout;
mod cargo_proxy;
mod checked_regions;
mod cli;
mod compile;
mod compiler;
mod config;
mod config_tui;
mod daemon;
mod daemon_publish;
mod demand;
mod disk_recovery;
mod events;
mod explain;
mod extra_inputs;
mod fallback;
mod fallback_planner;
mod heartbeat;
mod identity;
mod incremental_policy;
mod index_compact;
#[cfg(unix)]
mod init_shell;
mod key_env;
use kache_store::link;
mod cache_remote;
mod link_probe;
mod machine;
mod maintenance;
mod miss_chain;
mod native_archive;
mod native_link_key;
mod notice;
mod opcounts;
mod otel;
mod out_dir_alias;
mod path_normalizer;
mod phase_trace;
mod planner_auth;
mod planner_client;
mod platform;
mod policy;
mod prediction_share;
mod prestage;
mod probe;
mod probe_memo;
mod remote;
mod remote_backend;
mod remote_layout;
mod remote_pack;
mod remote_plan;
mod remote_resilience;
mod report;
mod run_diff;
mod savings;
mod scheduler;
mod service;
mod shards;
mod since;
// Both callers (`clean`'s classifier and `compute_link_stats`) are `cfg(unix)`,
// because `nlink` is the signal they combine with. Windows ReFS block-cloning
// has no read-side query to implement this against — `FSCTL_DUPLICATE_EXTENTS`
// is write-only — so the module compiles to its unknown-answer stub there and
// would otherwise read as dead code in non-test builds.
#[cfg_attr(not(unix), allow(dead_code))]
use kache_store::sharing;
mod compiler_store;
use compiler_store as store;
mod store_view;
mod target_cleanup;
mod target_dedup;
mod target_seed;
mod target_use;
mod term;
mod test_runner;
#[cfg(test)]
mod test_support;
mod timeline;
mod timeline_client;
mod toolchain_dylib;
mod transport;
mod tui;
mod tui_sessions;
mod unit_prune;
mod verify_compare;
mod volume_gc;
mod warm_set;
mod warm_set_toolchain;
mod worktree_discovery;
mod wrapper;
mod wrapper_config;

// macOS local-network privacy attributes a LaunchAgent's network access to
// responsible code. Kache is a standalone executable rather than an app
// bundle, so embed the identity and usage description directly in its Mach-O
// image. The generated LaunchAgent associates itself with the same bundle id
// in `service::launchd_plist_content` (kunobi-ninja/kache#798).
#[cfg(target_os = "macos")]
#[used]
#[unsafe(link_section = "__TEXT,__info_plist")]
static MACOS_INFO_PLIST: [u8; include_bytes!("../assets/macos/Info.plist").len()] =
    *include_bytes!("../assets/macos/Info.plist");

use anyhow::{Context, Result};
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};
use std::io::IsTerminal;
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
#[command(name = "kache", version = VERSION, about = "Cache Cargo and C/C++ builds; inspect saved time and disk use", styles = term::clap_styles(), after_help = "Start with kache init. Keep using cargo as usual.\nUse kache stats for a summary or kache monitor for live activity.")]
pub(crate) struct Cli {
    /// Machine-readable JSON on stdout, on commands that support it
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    pub(crate) command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Run Cargo with canonical duplicate Cargo-home rustflags collapsed once
    #[command(hide = true)]
    Cargo {
        /// Built-in build/check arguments passed verbatim to Cargo (use `--` first)
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<std::ffi::OsString>,
    },

    /// List cache entries, or one crate's entries in detail
    #[command(display_order = 22)]
    List {
        /// Crate name to show details for (omit to list all)
        crate_name: Option<String>,

        /// Sort by: name, size, hits, age
        #[arg(long, default_value = "name")]
        sort: String,

        /// Print directly instead of using a pager
        #[arg(long)]
        no_pager: bool,
    },

    /// Evict cache entries by the size and age limits, as the daemon does.
    /// Kept for scripts
    #[command(hide = true)]
    Gc {
        /// Evict entries older than this duration (e.g. 7d, 24h)
        #[arg(long, conflicts_with = "stale_schema")]
        max_age: Option<String>,

        /// Remove entries from old or unrecorded cache-key schemas
        #[arg(long)]
        stale_schema: bool,
    },

    /// Remove the whole cache or one crate's entries. Kept for scripts;
    /// `kache clean --cache` empties the cache too
    #[command(hide = true)]
    Purge {
        /// Only purge entries for this crate
        #[arg(long)]
        crate_name: Option<String>,
    },

    /// Free disk space: remove old target directories, or empty the cache
    #[command(display_order = 11)]
    Clean {
        /// Look for target/ directories under this directory instead of using
        /// the ones kache tracks
        #[arg(value_name = "DIR", conflicts_with_all = ["cache", "crate_name"])]
        path: Option<PathBuf>,

        /// Show what would be removed and what kache keeps, and remove nothing
        #[arg(long, short = 'n')]
        dry_run: bool,

        /// Remove without asking. For scripts and cron; preview first with
        /// --dry-run
        #[arg(long, short = 'y')]
        yes: bool,

        /// Targets no build used for this long, and targets whose worktree
        /// is gone (default 14d)
        #[arg(long, value_name = "DURATION")]
        stale: Option<String>,

        /// Empty the cache instead. Asks first. The daemon already keeps it
        /// under its size limit
        #[arg(long)]
        cache: bool,

        /// Only targets whose worktree or workspace no longer exists, however
        /// recently they were built
        #[arg(long, hide = true, conflicts_with_all = ["stale", "cache", "crate_name"])]
        orphans: bool,

        /// With --cache: the same, kept for older scripts
        #[arg(long, hide = true, requires = "cache", conflicts_with_all = ["stale", "stale_schema"])]
        all: bool,

        /// Remove the cache entries of this crate
        #[arg(long = "crate", hide = true, value_name = "NAME", conflicts_with_all = ["cache", "stale"])]
        crate_name: Option<String>,

        /// With --cache, remove entries from old or unrecorded cache-key schemas
        #[arg(long, hide = true, requires = "cache", conflicts_with = "stale")]
        stale_schema: bool,

        /// Accepted for older scripts: tracked targets are the default now
        #[arg(long, hide = true)]
        tracked: bool,
    },

    /// Show tracked target directories. Kept for scripts; `kache clean
    /// --dry-run` shows the same targets
    #[command(hide = true, subcommand_required = false)]
    Targets {
        #[command(subcommand)]
        command: Option<TargetCommands>,
    },

    /// Set up caching for Cargo and C/C++ builds
    #[command(display_order = 1)]
    Init {
        /// Accept all default answers (non-interactive)
        #[arg(long, short = 'y')]
        yes: bool,

        /// Do not install the daemon as a login service
        #[arg(long)]
        no_service: bool,

        /// Leave shell startup files alone: no C/C++ shims on PATH and no test
        /// runner (Cargo native dependencies are still configured)
        #[arg(long)]
        no_shell: bool,

        /// Print what would change without modifying anything
        #[arg(long)]
        check: bool,

        /// Only create the compiler-name links to kache (Unix), in DIR or
        /// ~/.local/lib/kache/shims, and change nothing else
        #[arg(
            long,
            value_name = "DIR",
            num_args = 0..=1,
            conflicts_with_all = ["check", "no_service", "no_shell"]
        )]
        shims: Option<Option<PathBuf>>,

        /// With --shims, also link every other compiler name on PATH (target
        /// triplets)
        #[arg(long, requires = "shims")]
        from_path: bool,

        /// With --shims, link only the unversioned names, not the versioned
        /// compilers on PATH (clang-19, g++-13)
        #[arg(long, requires = "shims", conflicts_with = "from_path")]
        canonical_only: bool,

        /// With --shims, replace existing entries instead of refusing
        #[arg(long, requires = "shims")]
        force: bool,
    },

    /// Diagnose setup issues and verify cache integrity
    #[command(display_order = 2)]
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

    /// Pull from and push to the configured remote
    #[command(display_order = 30)]
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
        /// Allow partial synchronization and exit successfully (code 0) even if some transfers or imports fail
        #[arg(long)]
        allow_partial: bool,
    },

    /// Restore a compatible ordered warm set before a Cargo build
    #[command(display_order = 24)]
    Prefetch(crate::warm_set::Options),

    /// Save a build manifest for future prefetch warming
    #[command(hide = true)]
    SaveManifest {
        /// Override manifest key (default: identity key plus host triple)
        #[arg(long)]
        manifest_key: Option<String>,
        /// Shard namespace: target/rustc_hash/profile. If set and Cargo.lock exists,
        /// uploads content-addressed shards alongside the monolithic build manifest.
        #[arg(long)]
        namespace: Option<String>,
    },

    /// Show the background daemon's status, or start, stop or install it
    #[command(subcommand_required = false, display_order = 40)]
    Daemon {
        #[command(subcommand)]
        command: Option<DaemonCommands>,
    },

    /// Live dashboard of builds and the cache
    #[command(display_order = 12)]
    Monitor {
        /// Preload events from this window (e.g. 15m, 2h, 7d; a bare number
        /// is hours)
        #[arg(long)]
        since: Option<String>,
    },

    /// Hit rate, time saved and cache size; --full for the whole report
    #[command(display_order = 10)]
    Stats {
        /// Event window (e.g. 15m, 2h, 7d; a bare number is hours)
        #[arg(long, default_value = "24h")]
        since: String,

        /// Report the latest recorded activity session. Sessions may span Cargo
        /// commands; events without IDs use a five-minute idle gap per root.
        #[arg(long, conflicts_with = "since")]
        last_build: bool,

        /// Only this build tree/root: the latest session in it with
        /// `--last-build`, its compiler events with `--full`
        #[arg(long)]
        root: Option<PathBuf>,

        /// The full build report instead of the summary
        #[arg(long)]
        full: bool,

        /// Write the full report as json, markdown, github, or trace
        /// (Perfetto / Chrome trace). Implies --full
        #[arg(long, value_name = "FORMAT")]
        format: Option<String>,

        /// Write the full report to a file instead of stdout. Implies --full
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Entries in each top list of the full report. Implies --full
        #[arg(long)]
        top: Option<usize>,

        /// Also append the full report's summary and timing, with the host's
        /// load, to `<cache dir>/telemetry/sessions.jsonl`, which outlives the
        /// runtime dir a CI job deletes. Implies --full
        #[arg(long)]
        record: bool,

        /// Replace cache keys, roots, and object paths in the full report
        /// with `redacted`. Implies --full
        #[arg(long)]
        redact: bool,
    },

    /// Write cache counters as OTLP JSON for Kartero to import later
    #[command(hide = true)]
    Telemetry {
        #[command(subcommand)]
        command: TelemetryCommands,
    },

    /// Explain why a crate, or the latest build, missed the cache
    #[command(display_order = 20, alias = "why-miss")]
    Explain {
        /// The crate to explain. Without one, compare the latest build's
        /// misses with the build before it
        crate_name: Option<String>,

        /// Without a crate: only builds recorded for this build tree
        #[arg(long, conflicts_with = "crate_name")]
        root: Option<PathBuf>,
    },

    /// Compare misses in the two newest sessions of one build root. Kept
    /// for scripts; `kache explain` with no crate explains the same build
    #[command(hide = true)]
    Diff {
        /// Only sessions recorded for this build tree
        #[arg(long)]
        root: Option<PathBuf>,
    },

    /// The full build report. Kept for scripts; `kache stats --full` is the
    /// same report
    #[command(hide = true)]
    Report {
        /// Output format: json, trace, perfetto, chrome-trace, markdown, github, text
        #[arg(long, default_value = "text")]
        format: String,

        /// Event window (e.g. 15m, 2h, 7d; a bare number is hours)
        #[arg(long, default_value = "24h")]
        since: String,

        /// Report the latest recorded activity session. Sessions may span Cargo
        /// commands; events without IDs use a five-minute idle gap per root.
        #[arg(long, conflicts_with = "since")]
        last_build: bool,

        /// Only include compiler events from this build tree/root
        #[arg(long)]
        root: Option<PathBuf>,

        /// Write output to a file instead of stdout
        #[arg(long, short)]
        output: Option<PathBuf>,

        /// Number of top entries to show
        #[arg(long, default_value = "10")]
        top: usize,

        /// Also append this report's summary and timing, with the host's load,
        /// to `<cache dir>/telemetry/sessions.jsonl`, which outlives the
        /// runtime dir a CI job deletes
        #[arg(long)]
        record: bool,

        /// Replace cache keys, roots, and object paths with `redacted`
        #[arg(long)]
        redact: bool,
    },

    /// Open the configuration editor
    #[command(display_order = 3)]
    Config,

    /// Log in to the planner with your Kunobi account
    #[command(display_order = 31)]
    Login {
        /// Sign in on another device (prints a URL and a code)
        #[arg(long)]
        device: bool,
        /// Accept and re-pin the planner's current login issuer when it no
        /// longer matches the one first trusted
        #[arg(long)]
        retrust: bool,
    },

    /// Log out of the planner and revoke the stored Kunobi session
    #[command(display_order = 32)]
    Logout,

    /// Create compiler-name links to kache in a directory you put first on
    /// PATH. Kept for scripts and packages; `kache init --shims` does the
    /// same in the default directory
    #[command(hide = true)]
    InstallShims {
        /// Directory to populate. Defaults to ~/.local/lib/kache/shims
        #[arg(value_name = "DIR")]
        dir: Option<PathBuf>,

        /// Replace existing entries instead of refusing
        #[arg(long)]
        force: bool,

        /// Also wrap every other compiler name on PATH (target triplets)
        #[arg(long)]
        from_path: bool,

        /// Link only the unversioned names, not the versioned compilers on PATH
        #[arg(long, conflicts_with = "from_path")]
        canonical_only: bool,
    },

    /// Generate shell completion scripts
    #[command(display_order = 41)]
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

#[derive(Subcommand)]
enum TargetCommands {
    /// Share target files with blobs the store already holds
    Share {
        /// Clone each match from the stored blob. Without this, only report.
        #[arg(long)]
        apply: bool,

        /// Target directories to read. Defaults to tracked directories.
        #[arg(value_name = "TARGET")]
        paths: Vec<PathBuf>,
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

#[derive(Subcommand)]
enum TelemetryCommands {
    /// Snapshot live cache counters into `metrics.otlp.json` + `schema_version`
    Write {
        /// Directory to write. Created if missing.
        dir: PathBuf,
        /// Bench scenario this dump belongs to (same string as `kache.bench.project`).
        #[arg(long)]
        scenario: Option<String>,
        /// Bench phase this dump belongs to (`cold`, `warm`, `pull`).
        #[arg(long)]
        phase: Option<String>,
    },
    /// Send build timeline records for finished builds to the service
    Push {
        /// Send every session still in the logs, not just the last build
        #[arg(long, conflicts_with = "session")]
        all: bool,
        /// Send one session by id
        #[arg(long)]
        session: Option<String>,
        /// Extra `key=value` label recorded with the build, repeatable
        #[arg(long = "label", value_name = "KEY=VALUE")]
        labels: Vec<String>,
        /// Print the records instead of sending them
        #[arg(long)]
        dry_run: bool,
    },
}

fn command_supports_json(command: &Option<Commands>) -> bool {
    matches!(
        command,
        Some(
            Commands::List { .. }
                | Commands::Gc { .. }
                | Commands::Clean { .. }
                | Commands::Targets { .. }
                | Commands::Doctor { .. }
                | Commands::Stats { .. }
                | Commands::Explain { .. }
                | Commands::Diff { .. }
                | Commands::Init { shims: None, .. }
                | Commands::Report { .. }
                | Commands::Daemon { command: None }
                | Commands::Daemon {
                    command: Some(DaemonCommands::Status),
                }
        )
    )
}

fn doctor_json_is_read_only(
    fix: bool,
    purge_sccache: bool,
    verify: bool,
    checksums: bool,
    repair: bool,
) -> bool {
    ![fix, purge_sccache, verify, checksums, repair]
        .into_iter()
        .any(|enabled| enabled)
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

/// How main reads argv, decided before any of it is parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryRoute {
    /// kache started itself for a subcommand: parse the CLI whatever
    /// argv[0] is. A self-spawn under a shim keeps the shim's name there.
    SelfSpawn,
    /// argv[0] names a compiler: kache runs behind a compiler-name shim.
    Shim,
    /// kache under its own name: argv[1..] picks wrapper or CLI mode.
    Argv,
}

/// Route a process from argv and the [`platform::SELF_SPAWN_ENV`] value.
fn entry_route(argv: &[String], self_spawn: Option<&std::ffi::OsStr>) -> EntryRoute {
    if platform::is_self_spawn(argv, self_spawn) {
        return EntryRoute::SelfSpawn;
    }
    if argv
        .first()
        .is_some_and(|arg0| compiler::shim::invoked_as_compiler(arg0))
    {
        return EntryRoute::Shim;
    }
    EntryRoute::Argv
}

/// The test binary and its arguments when kache runs as a Cargo target
/// runner (`kache test-runner <binary> <args>`). Behind a compiler-name shim
/// argv[1] is a compiler argument, so `cc test-runner` stays a compile.
fn test_runner_args(raw_args: &[std::ffi::OsString]) -> Option<&[std::ffi::OsString]> {
    match raw_args.get(1) {
        Some(arg)
            if arg == "test-runner"
                && !raw_args.first().is_some_and(|arg0| {
                    compiler::shim::invoked_as_compiler(&arg0.to_string_lossy())
                }) =>
        {
            Some(&raw_args[2..])
        }
        _ => None,
    }
}

fn main() -> Result<()> {
    // First, before argv or config: `startup_ms` on every build event is
    // measured from here.
    opcounts::mark_process_start();
    // SAFETY: before logging, runtimes, or other threads start. DaemonCommand
    // exclusively passes the channel descriptor to this process.
    let mut readiness = unsafe { kunobi_daemon::readiness::channel::Notifier::from_env() }
        .context("opening daemon readiness channel")?;
    if let Some(notifier) = &mut readiness {
        let _ = notifier.progress("starting kache");
    }
    // Cargo target runner. Checked before the probe and shim guards, which
    // exit without running anything: a test must always run.
    let raw_args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    if raw_args.first().is_some_and(|arg| {
        std::path::Path::new(arg)
            .file_name()
            .is_some_and(|name| name == "cargo")
    }) && std::env::var_os(platform::SELF_SPAWN_ENV).is_none()
    {
        init_logging(LogMode::Cli);
        return cargo_proxy::run_shim(raw_args[1..].to_vec());
    }
    if let Some(args) = test_runner_args(&raw_args) {
        init_logging(LogMode::Wrapper);
        std::process::exit(test_runner::run(args));
    }
    if std::env::var_os("KACHE_FAMILY_PROBE_ACTIVE").is_some() {
        // Prevent unbounded recursion when a probed wrapper calls back into kache.
        return Ok(());
    }
    // Cargo running a build script through the launcher the rustc wrapper
    // installed. Decided on the environment alone: argv is the script's own.
    if build_script::is_shim_invocation() {
        init_logging(LogMode::Wrapper);
        std::process::exit(build_script::run_shim());
    }

    // Keep the original argv byte-preserving for `kache cargo`. Compiler
    // adapters still parse UTF-8 option syntax; a wrapper invocation with a
    // non-UTF-8 path is detected here and fails closed below.
    let env_args: Option<Vec<String>> = raw_args
        .iter()
        .cloned()
        .map(|arg| arg.into_string())
        .collect::<Result<_, _>>()
        .ok();
    let lossy_args;
    let detection_args = if let Some(args) = env_args.as_deref() {
        args
    } else {
        lossy_args = raw_args
            .iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        &lossy_args
    };
    // Compiler-name shim (#310): invoked as `gcc`/`cc`/… through a PATH
    // symlink, so the compiler is our own argv[0] rather than argv[1]. Checked
    // BEFORE `detect_log_mode`, which only ever inspects argv[1..] and would
    // classify `gcc foo.c` as CLI mode and fail parsing `foo.c` as a
    // subcommand. A process kache started for its own subcommand can carry a
    // shim's name in argv[0] too, so its marker is checked first.
    let self_spawn = std::env::var_os(platform::SELF_SPAWN_ENV);
    let shim_args = match entry_route(detection_args, self_spawn.as_deref()) {
        EntryRoute::Shim => compiler::shim::wrapper_args(detection_args),
        EntryRoute::SelfSpawn => {
            // SAFETY: no thread has been spawned yet. Removing the marker
            // keeps it out of every process this one starts.
            unsafe { std::env::remove_var(platform::SELF_SPAWN_ENV) };
            None
        }
        EntryRoute::Argv => None,
    };
    if let Some(shim_args) = shim_args {
        init_logging(LogMode::Wrapper);
        let shim_args = shim_args.map_err(anyhow::Error::msg)?;
        if env_args.is_none() {
            anyhow::bail!(
                "compiler wrapper arguments contain non-UTF-8 bytes and cannot be cached safely"
            );
        }
        return run_wrapper_mode(&shim_args);
    }

    let log_mode = detect_log_mode(detection_args);

    // Detect RUSTC_WRAPPER mode: cargo passes the rustc path as arg[1]
    // In this mode: argv[0]=kache, argv[1]=rustc, argv[2..]=rustc args
    let is_wrapper = log_mode == LogMode::Wrapper;
    init_logging(log_mode);

    if is_wrapper {
        if let Some(env_args) = env_args {
            return run_wrapper_mode(&env_args[1..]);
        }
        anyhow::bail!(
            "compiler wrapper arguments contain non-UTF-8 bytes and cannot be cached safely"
        );
    }

    // CLI mode: parse subcommands
    let matches = match Cli::command().try_get_matches_from(&raw_args) {
        Ok(matches) => matches,
        Err(error) => {
            let json = raw_args
                .iter()
                .skip(1)
                .take_while(|arg| *arg != "--")
                .any(|arg| arg == "--json");
            if json && error.kind() == clap::error::ErrorKind::DisplayHelp {
                machine::emit_help(Cli::command())?;
                return Ok(());
            }
            if json && error.kind() == clap::error::ErrorKind::DisplayVersion {
                machine::emit(
                    "version",
                    serde_json::json!({ "version": VERSION }),
                    Vec::new(),
                )?;
                return Ok(());
            }
            if json && error.use_stderr() {
                machine::emit_error(
                    "cli",
                    "invalid_arguments",
                    &anyhow::anyhow!(error.to_string()),
                )?;
                eprint!("{error}");
                std::process::exit(2);
            }
            error.exit();
        }
    };
    let cli = Cli::from_arg_matches(&matches)?;
    let json = cli.json;
    let mut names = Vec::new();
    let mut matched = &matches;
    while let Some((name, args)) = matched.subcommand() {
        names.push(name);
        matched = args;
    }
    let command = if names.is_empty() {
        "cli".into()
    } else {
        names.join("-")
    };
    machine::configure(json, command.clone());
    if let Err(error) = run_cli(cli, readiness) {
        if json {
            machine::emit_error(&command, "command_failed", &error)?;
        }
        eprintln!("{} {error:#}", term::paint("error:", term::Style::Error));
        std::process::exit(1);
    }
    Ok(())
}

fn run_cli(cli: Cli, readiness: Option<kunobi_daemon::readiness::channel::Notifier>) -> Result<()> {
    let json = cli.json;
    if json && !command_supports_json(&cli.command) {
        anyhow::bail!(
            "`--json` is supported on init, stats, report, clean, gc, doctor, explain, diff, list, targets, and daemon status."
        );
    }

    // Config and Completions run before Config::load() (broken/missing config).
    match &cli.command {
        Some(Commands::Config) => {
            if json {
                anyhow::bail!("`kache config` is interactive and has no JSON form.");
            }
            crate::machine::require_tty(
                std::io::stdout().is_terminal(),
                "config",
                "`kache config` in a terminal",
            )?;
            return config_tui::run_config_editor();
        }
        Some(Commands::Login { device, retrust }) => {
            if json {
                anyhow::bail!("`kache login` is interactive and has no JSON form.");
            }
            return cli::login::login(*device, *retrust);
        }
        Some(Commands::Logout) => return cli::login::logout(),
        Some(Commands::Cargo { args }) => return cargo_proxy::run(args.clone()),
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

    let (config, config_provenance) = config::Config::load_with_provenance()?;

    match cli.command {
        Some(Commands::List {
            crate_name,
            sort,
            no_pager,
        }) => cli::list(&config, crate_name.as_deref(), &sort, no_pager, json),
        Some(Commands::Gc {
            max_age,
            stale_schema,
        }) => {
            let hours = max_age
                .as_deref()
                .map(|value| {
                    parse_duration_hours(value).ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid --max-age {value:?}; expected hours or a duration like 24h or 7d"
                        )
                    })
                })
                .transpose()?;
            cli::gc(&config, hours, stale_schema, json)
        }
        Some(Commands::Purge { crate_name }) => {
            if json {
                anyhow::bail!("`kache purge` has no JSON form yet; run it without `--json`.");
            }
            cli::purge(&config, crate_name.as_deref())
        }
        Some(Commands::Clean {
            path,
            dry_run,
            yes,
            stale,
            orphans,
            cache,
            all,
            crate_name,
            stale_schema,
            tracked: _,
        }) => {
            let stale_hours = stale
                .as_deref()
                .map(|value| {
                    parse_duration_hours(value).ok_or_else(|| {
                        anyhow::anyhow!(
                            "invalid --stale {value:?}; expected hours or a duration like 24h or 7d"
                        )
                    })
                })
                .transpose()?;
            if let Some(name) = crate_name {
                return cli::clean_cache(&config, cli::CacheClean::Crate(name), dry_run, yes, json);
            }
            if cache {
                // `--all` is what `--cache` does now; it stays for scripts.
                let _ = all;
                let what = cache_clean_choice(stale_schema, stale_hours);
                return cli::clean_cache(&config, what, dry_run, yes, json);
            }
            let scope = clean_scope(path, orphans, stale_hours);
            cli::clean(&config, dry_run, yes, json, scope)
        }
        Some(Commands::Targets { command: None }) => cli::targets(&config, json),
        Some(Commands::Targets {
            command: Some(TargetCommands::Share { apply, paths }),
        }) => target_dedup::run(&config, &paths, apply, json),
        #[cfg(unix)]
        Some(Commands::Init {
            shims: Some(dir),
            from_path,
            canonical_only,
            force,
            ..
        }) => install_shims(dir, force, from_path, canonical_only),
        #[cfg(not(unix))]
        Some(Commands::Init { shims: Some(_), .. }) => Err(anyhow::anyhow!(UNIX_ONLY_SHIMS)),
        Some(Commands::Init {
            yes,
            no_service,
            no_shell,
            check,
            ..
        }) => {
            if json && !yes && !check {
                anyhow::bail!(
                    "`kache init --json` needs `--check` to preview or `--yes` to apply setup."
                );
            }
            cli::init(yes, no_service, no_shell, check)
        }
        Some(Commands::Doctor {
            fix,
            purge_sccache,
            verify,
            checksums,
            repair,
        }) => {
            if json && !doctor_json_is_read_only(fix, purge_sccache, verify, checksums, repair) {
                anyhow::bail!(
                    "`kache doctor --json` reports diagnostics only; run repair options without `--json`."
                );
            }
            cli::doctor(
                fix,
                purge_sccache,
                verify || checksums || repair,
                checksums,
                repair,
                json,
            )
        }
        Some(Commands::Sync {
            manifest_path,
            pull,
            push,
            dry_run,
            all,
            workspace,
            allow_partial,
        }) => cli::sync(
            &config,
            manifest_path.as_deref(),
            pull,
            push,
            dry_run,
            all,
            workspace,
            allow_partial,
        ),
        Some(Commands::Prefetch(options)) => crate::warm_set::run(&config, &options),
        Some(Commands::SaveManifest {
            manifest_key,
            namespace,
        }) => cli::save_manifest(&config, manifest_key.as_deref(), namespace.as_deref()),
        Some(Commands::Daemon { command: None }) => service::status(json),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Status),
        }) => service::status(json),
        Some(Commands::Daemon {
            command: Some(DaemonCommands::Run),
        }) => daemon::run_server(&config, &config_provenance, readiness),
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
            last_build,
            root,
            output,
            top,
            record,
            redact,
        }) => {
            let window = parse_since_window(&since)?;
            cli::report(
                &config,
                &report_format(Some(format), json)?,
                window,
                report::ReportFilter { root, last_build },
                output,
                top,
                record,
                redact,
            )
        }
        Some(Commands::Stats {
            since,
            last_build,
            root,
            full,
            format,
            output,
            top,
            record,
            redact,
        }) => {
            let full = stats_wants_full(
                full,
                format.is_some(),
                output.is_some(),
                top.is_some(),
                record,
                redact,
            );
            if full {
                let format = report_format(format, json)?;
                let window = parse_since_window(&since)?;
                return cli::report(
                    &config,
                    &format,
                    window,
                    report::ReportFilter { root, last_build },
                    output,
                    top.unwrap_or(DEFAULT_REPORT_TOP),
                    record,
                    redact,
                );
            }
            if last_build {
                return cli::stats_last_build(&config, root, json);
            }
            if root.is_some() {
                anyhow::bail!("`--root` needs `--last-build` or `--full`");
            }
            let window = parse_since_window(&since)?;
            cli::stats(&config, &config_provenance, window, json)
        }
        Some(Commands::Telemetry {
            command:
                TelemetryCommands::Write {
                    dir,
                    scenario,
                    phase,
                },
        }) => cli::telemetry_write(&config, &dir, scenario.as_deref(), phase.as_deref()),
        Some(Commands::Telemetry {
            command:
                TelemetryCommands::Push {
                    all,
                    session,
                    labels,
                    dry_run,
                },
        }) => {
            let selection = match (all, session) {
                (true, _) => cli::TimelineSelection::All,
                (false, Some(session)) => cli::TimelineSelection::Session(session),
                (false, None) => cli::TimelineSelection::Latest,
            };
            cli::telemetry_push(&config, &selection, &labels, dry_run)
        }
        Some(Commands::Explain {
            crate_name: Some(crate_name),
            ..
        }) => cli::why_miss(&config, &crate_name, json),
        Some(Commands::Explain {
            crate_name: None,
            root,
        }) => explain::run(&config, root.as_deref(), json),
        Some(Commands::Diff { root }) => {
            if run_diff::run(&config, root.as_deref(), json)? {
                std::process::exit(1);
            }
            Ok(())
        }
        Some(Commands::Monitor { since }) => {
            if json {
                anyhow::bail!("`kache monitor` is interactive; use `kache stats --json`.");
            }
            crate::machine::require_tty(
                std::io::stdout().is_terminal(),
                "monitor",
                "`kache stats --json`",
            )?;
            let window = since.as_deref().map(parse_since_window).transpose()?;
            tui::run_monitor(&config, window)
        }
        #[cfg(unix)]
        Some(Commands::InstallShims {
            dir,
            force,
            from_path,
            canonical_only,
        }) => install_shims(dir, force, from_path, canonical_only),
        #[cfg(not(unix))]
        Some(Commands::InstallShims { .. }) => Err(anyhow::anyhow!(UNIX_ONLY_SHIMS)),
        Some(Commands::Cargo { .. }) => unreachable!(),
        Some(Commands::Config) => unreachable!(),
        Some(Commands::Login { .. } | Commands::Logout) => unreachable!(),
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
/// guard. The value counts the wrappers above the child: the outermost
/// writes `1` and each nested wrapper one more.
const KACHE_ACTIVE_ENV: &str = "KACHE_ACTIVE";

/// How many kache wrappers may sit above this one before the chain is
/// taken to be a loop. Real nesting stays at one or two, such as a
/// compiler or a script it runs calling a shimmed `cc`.
const MAX_WRAPPERS_ABOVE: u32 = 8;

/// How many kache wrappers sit above this process, from the value of
/// [`KACHE_ACTIVE_ENV`]. `None` when it is the outermost. A value that is
/// not a number counts as one: older kache versions always wrote `1`.
fn wrappers_above(active: Option<&std::ffi::OsStr>) -> Option<u32> {
    let active = active?;
    Some(active.to_str().and_then(|v| v.parse().ok()).unwrap_or(1))
}

/// The [`KACHE_ACTIVE_ENV`] value a nested wrapper hands to its compiler.
fn next_wrapper_depth(above: u32) -> u32 {
    above.saturating_add(1)
}

/// Stop a chain of nested wrappers that only a loop could produce.
///
/// Shim resolution skips every PATH entry it can identify as kache, so a
/// loop that still gets here runs through shims it cannot identify: a farm
/// of copies with no marker. Nothing left on PATH is known to be the real
/// compiler, so skipping ahead could pick the wrong one. Failing names the
/// cause, where the loop would otherwise start processes without end.
fn check_wrapper_depth(above: u32, compiler: Option<&str>) -> Result<()> {
    if above < MAX_WRAPPERS_ABOVE {
        return Ok(());
    }
    anyhow::bail!(
        "kache is running inside {above} other kache wrappers to run `{}`, so two \
         compiler shims are running each other in a loop. Another kache install's shim \
         directory is on PATH and holds copies that kache cannot tell apart from a \
         compiler. Remove that directory from PATH, or create an empty `.kache-shims` \
         file in it so every kache skips it.",
        compiler.unwrap_or("?")
    )
}

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

/// Run a Cargo `RUSTC_WRAPPER + RUSTC_WORKSPACE_WRAPPER` chain uncached while
/// retaining Kache-owned freshness for an active `extra_inputs` declaration.
fn preserve_workspace_wrapper_incremental(
    preserve_requested: bool,
    extra_inputs_active: bool,
) -> bool {
    preserve_requested && !extra_inputs_active
}

fn run_workspace_wrapper_chain(config: &config::Config, args: &[String]) -> Result<i32> {
    let parsed = args::RustcArgs::parse(args).context("parsing workspace-wrapper rustc chain")?;
    let extra_inputs = wrapper::resolve_extra_inputs_for_passthrough(config, &parsed)?;
    let preserve_incremental =
        preserve_workspace_wrapper_incremental(config.preserve_incremental, extra_inputs.is_some());
    let exit = run_compiler_directly(config, args, preserve_incremental)?;
    if exit == 0 {
        wrapper::complete_current_extra_inputs_after_success(
            config,
            &parsed,
            extra_inputs.as_ref(),
        )?;
    }
    Ok(exit)
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
    let mut command = std::process::Command::new(&program);
    toolchain_dylib::apply(&mut command, &program);
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

/// Which wrapper entry point an adapter dispatches to.
///
/// Pure so the dispatch choice is unit-testable: `run_wrapper_mode` only
/// executes it. `None` means the adapter has no wrapper entry point yet
/// (the caller bails with the adapter's identity).
fn wrapper_target(adapter: &compiler::CompilerAdapter) -> Option<WrapperTarget> {
    if adapter.id() == compiler::rustc::RUSTC_ID {
        Some(WrapperTarget::Rustc)
    } else if adapter.id() == compiler::rustdoc::RUSTDOC_ID {
        Some(WrapperTarget::Rustdoc)
    } else if adapter.id() == compiler::cc::CC_ID {
        Some(WrapperTarget::Cc)
    } else if adapter.id() == compiler::nvcc::NVCC_ID {
        Some(WrapperTarget::Nvcc)
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WrapperTarget {
    Rustc,
    Rustdoc,
    Cc,
    Nvcc,
}

fn run_wrapper_mode(args: &[String]) -> Result<()> {
    // Re-entrancy guard. If a child compiler kache spawns is itself a
    // kache wrapper — e.g. `cc` on PATH shadowed by `kache cc` — the
    // nested invocation must run the real compiler directly instead of
    // looping kache → cc → kache → … kache sets KACHE_ACTIVE before
    // spawning any child, so a wrapper that already sees it set knows
    // it is nested. This runs before the `disabled` check below, so
    // the loop is broken even when caching is turned off.
    if let Some(above) = wrappers_above(std::env::var_os(KACHE_ACTIVE_ENV).as_deref()) {
        // The compiler run here can itself be a kache shim that nothing
        // identified. Counting the depth ends that loop at a fixed bound.
        check_wrapper_depth(above, args.first().map(String::as_str))?;
        // SAFETY: as below, no thread has been spawned yet.
        unsafe {
            std::env::set_var(KACHE_ACTIVE_ENV, next_wrapper_depth(above).to_string());
        }
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
    scheduler::pressure::set_enabled(config.scheduler_memory_pressure);

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

    if compiler::is_workspace_wrapper_chain(args)
        && !compiler::is_cacheable_workspace_wrapper_chain(args)
    {
        tracing::debug!(
            program = ?args.first(),
            "workspace-wrapper chain is uncached; retaining extra-input Cargo freshness"
        );
        std::process::exit(run_workspace_wrapper_chain(&config, args)?);
    }

    let direct_passthrough = compiler::is_passthrough_compiler_invocation(args)
        && !compiler::rustc::RustcCompiler::recognizes(args);
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
    let exit_code = match wrapper_target(adapter) {
        Some(WrapperTarget::Rustc) => {
            // Cargo's first call for a target directory it has not built:
            // let the daemon copy registry units in before Cargo looks.
            target_seed::before_probe(&config, args);
            // Started here so the trace also covers the OUT_DIR alias check.
            let trace = phase_trace::start("rustc", args);
            // Still single-threaded, like the KACHE_ACTIVE write above:
            // `apply` may change OUT_DIR and the vars under it.
            let exit = match out_dir_alias::apply(&config, args) {
                Some(exit) => exit,
                None => wrapper::run(&config, args)?,
            };
            drop(trace);
            exit
        }
        Some(WrapperTarget::Rustdoc) => compiler::rustdoc::run(&config, args)?,
        Some(WrapperTarget::Cc) => wrapper::run_cc(&config, args)?,
        Some(WrapperTarget::Nvcc) => wrapper::run_nvcc(&config, args)?,
        None => anyhow::bail!(
            "detected compiler adapter {} ({}) has no wrapper dispatch",
            adapter.id(),
            adapter.display_name()
        ),
    };
    std::process::exit(exit_code);
}

/// What `kache clean --cache` removes, from its hidden flags.
fn cache_clean_choice(stale_schema: bool, stale_hours: Option<u64>) -> cli::CacheClean {
    match stale_hours {
        _ if stale_schema => cli::CacheClean::StaleSchema,
        Some(hours) => cli::CacheClean::OlderThan(hours),
        None => cli::CacheClean::All,
    }
}

/// Which target directories `kache clean` looks at.
fn clean_scope(path: Option<PathBuf>, orphans: bool, stale_hours: Option<u64>) -> cli::CleanScope {
    match path {
        Some(dir) => cli::CleanScope::Scan(dir),
        None if orphans => cli::CleanScope::Tracked(cli::TrackedSelection::Orphaned),
        None => cli::CleanScope::Tracked(cli::TrackedSelection::StaleOrOrphaned(
            stale_hours.unwrap_or(cli::DEFAULT_TRACKED_STALE_HOURS),
        )),
    }
}

/// `kache init --shims` and the hidden `kache install-shims`: compiler-name
/// links to kache in `dir`, by default `~/.local/lib/kache/shims`.
#[cfg(unix)]
fn install_shims(
    dir: Option<PathBuf>,
    force: bool,
    from_path: bool,
    canonical_only: bool,
) -> Result<()> {
    let dir = dir
        .or_else(compiler::shim::default_shim_dir)
        .context("no home directory; pass the shim directory to use")?;
    let extra = if canonical_only {
        Vec::new()
    } else {
        let path = std::env::var_os("PATH").unwrap_or_default();
        compiler::shim::names_on_path(&path, from_path)
    };
    cli::install_shims_named(&dir, force, &extra)
}

/// Shims are Unix-only (symlinks); the Windows `.exe` shim story differs
/// (#310).
#[cfg(not(unix))]
const UNIX_ONLY_SHIMS: &str = "shim installation is Unix-only for now: it relies on symlinks, and \
     the Windows `.exe` shim story differs (kunobi-ninja/kache#310). Use `CC`/`CXX` there.";

/// Entries in each top list of the full report by default.
const DEFAULT_REPORT_TOP: usize = 10;

/// `--full`, or any flag that only the full report understands.
fn stats_wants_full(
    full: bool,
    format: bool,
    output: bool,
    top: bool,
    record: bool,
    redact: bool,
) -> bool {
    full || format || output || top || record || redact
}

/// The full report's format: the one asked for, else JSON under `--json`,
/// else text.
fn report_format(format: Option<String>, json: bool) -> Result<String> {
    if json {
        anyhow::ensure!(
            format
                .as_deref()
                .is_none_or(|format| format == "json" || format == "text"),
            "`--json` cannot be combined with a different `--format`."
        );
        return Ok("json".into());
    }
    Ok(format.unwrap_or_else(|| "text".into()))
}

/// Parse a `--since` value, or fail loudly. Falling back to the default on a
/// value that did not parse is how `--since 15m` came to report a 24h window
/// labelled as such (kunobi-ninja/kache#897).
fn parse_since_window(value: &str) -> Result<since::SinceWindow> {
    since::SinceWindow::parse(value).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid --since {value:?}; expected a duration like 15m, 2h, or 7d (a bare number is hours)"
        )
    })
}

/// Parse a duration string like "7d", "24h", "1h" into hours.
fn parse_duration_hours(s: &str) -> Option<u64> {
    let s = s.trim();
    if let Some(days) = s.strip_suffix('d') {
        days.parse::<u64>()
            .ok()
            .and_then(|days| days.checked_mul(24))
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
        let shell = compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        kache_fs::testutil::write_executable(path, format!("#!{}\nexit 0\n", shell.display()));
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

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    /// The auto-GC worker started from a wrapper behind a shim, on macOS where
    /// `current_exe` is the shim path. Before the fix it routed to shim mode
    /// and handed `gc` to the real compiler.
    #[test]
    fn a_self_spawned_gc_under_a_shim_path_routes_to_the_cli() {
        let gc = Some(std::ffi::OsStr::new("gc"));
        let args = argv(&["/x/shims/cc", "gc"]);
        assert_eq!(entry_route(&args, gc), EntryRoute::SelfSpawn);
        assert_eq!(
            detect_log_mode_with_rustc(&args, None),
            LogMode::Cli,
            "the rest of argv must then parse as the gc subcommand"
        );
        // A Windows shim is a copy, so argv[0] stays the compiler's name.
        let daemon = Some(std::ffi::OsStr::new("daemon"));
        let args = argv(&["C:/shims/gcc.exe", "daemon", "run"]);
        assert_eq!(entry_route(&args, daemon), EntryRoute::SelfSpawn);
        assert_eq!(detect_log_mode_with_rustc(&args, None), LogMode::Cli);
    }

    /// The marker alone is not enough: a compile run under a leaked marker
    /// still goes to the compiler.
    #[test]
    fn a_compile_through_a_shim_stays_a_compile() {
        let gc = Some(std::ffi::OsStr::new("gc"));
        let compile = argv(&["/x/shims/cc", "-c", "foo.c"]);
        assert_eq!(entry_route(&compile, gc), EntryRoute::Shim);
        assert_eq!(entry_route(&compile, None), EntryRoute::Shim);
        // Without the marker, `cc gc` compiles a file named gc.
        assert_eq!(
            entry_route(&argv(&["/x/shims/cc", "gc"]), None),
            EntryRoute::Shim
        );
    }

    #[test]
    fn kache_under_its_own_name_routes_on_the_rest_of_argv() {
        assert_eq!(entry_route(&argv(&["kache", "gc"]), None), EntryRoute::Argv);
        assert_eq!(
            entry_route(&argv(&["kache", "cc", "-c", "foo.c"]), None),
            EntryRoute::Argv
        );
        assert_eq!(entry_route(&[], None), EntryRoute::Argv);
        // The self-spawn marker wins over any argv[0].
        assert_eq!(
            entry_route(
                &argv(&["kache", "daemon", "run"]),
                Some(std::ffi::OsStr::new("daemon"))
            ),
            EntryRoute::SelfSpawn
        );
    }

    #[test]
    fn wrappers_above_reads_the_depth_and_treats_old_values_as_one() {
        use std::ffi::OsStr;
        assert_eq!(wrappers_above(None), None, "the outermost wrapper");
        assert_eq!(wrappers_above(Some(OsStr::new("1"))), Some(1));
        assert_eq!(wrappers_above(Some(OsStr::new("5"))), Some(5));
        assert_eq!(wrappers_above(Some(OsStr::new(""))), Some(1));
        assert_eq!(wrappers_above(Some(OsStr::new("yes"))), Some(1));
    }

    #[test]
    fn a_nested_wrapper_hands_down_one_more_level() {
        assert_eq!(next_wrapper_depth(1), 2);
        assert_eq!(next_wrapper_depth(7), 8);
        assert_eq!(next_wrapper_depth(u32::MAX), u32::MAX);
    }

    /// Pinned at the bound: one level short still runs, the bound itself
    /// stops the chain with a message that says how to fix it.
    #[test]
    fn wrapper_depth_is_refused_at_the_bound() {
        assert!(check_wrapper_depth(1, Some("cc")).is_ok());
        assert!(check_wrapper_depth(MAX_WRAPPERS_ABOVE - 1, Some("cc")).is_ok());
        let err = check_wrapper_depth(MAX_WRAPPERS_ABOVE, Some("/copies/cc"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("/copies/cc"), "{err}");
        assert!(err.contains("loop"), "{err}");
        assert!(err.contains(".kache-shims"), "{err}");
        assert!(check_wrapper_depth(MAX_WRAPPERS_ABOVE + 1, None).is_err());
    }

    #[test]
    fn stats_wants_the_full_report_for_any_full_flag() {
        assert!(!stats_wants_full(false, false, false, false, false, false));
        assert!(stats_wants_full(true, false, false, false, false, false));
        assert!(stats_wants_full(false, true, false, false, false, false));
        assert!(stats_wants_full(false, false, true, false, false, false));
        assert!(stats_wants_full(false, false, false, true, false, false));
        assert!(stats_wants_full(false, false, false, false, true, false));
        assert!(stats_wants_full(false, false, false, false, false, true));
    }

    #[test]
    fn report_format_prefers_an_explicit_name() {
        assert_eq!(report_format(None, false).unwrap(), "text");
        assert_eq!(report_format(None, true).unwrap(), "json");
        assert_eq!(report_format(Some("md".to_string()), false).unwrap(), "md");
        assert_eq!(
            report_format(Some("text".to_string()), true).unwrap(),
            "json"
        );
        assert_eq!(
            report_format(Some("json".to_string()), true).unwrap(),
            "json"
        );
        assert!(report_format(Some("md".to_string()), true).is_err());
    }

    #[test]
    fn parse_since_window_accepts_minutes_and_rejects_garbage() {
        assert_eq!(parse_since_window("15m").unwrap().secs(), 900);
        assert_eq!(parse_since_window("2h").unwrap().secs(), 7200);
        assert_eq!(parse_since_window("24").unwrap().secs(), 86_400);
        let err = parse_since_window("soon").unwrap_err().to_string();
        assert!(err.contains("invalid --since \"soon\""), "{err}");
        assert!(err.contains("15m"), "{err}");
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_hours("7d"), Some(168));
        assert_eq!(parse_duration_hours("24h"), Some(24));
        assert_eq!(parse_duration_hours("1h"), Some(1));
        assert_eq!(parse_duration_hours("48"), Some(48));
        assert_eq!(parse_duration_hours("invalid"), None);
        assert_eq!(parse_duration_hours("18446744073709551615d"), None);
    }

    #[test]
    fn targets_share_is_a_subcommand() {
        let list = Cli::try_parse_from(["kache", "targets"]).unwrap();
        assert!(matches!(
            list.command,
            Some(Commands::Targets { command: None })
        ));
        let share =
            Cli::try_parse_from(["kache", "targets", "share", "--apply", "target"]).unwrap();
        assert!(matches!(
            share.command,
            Some(Commands::Targets {
                command: Some(TargetCommands::Share { apply: true, ref paths })
            }) if paths == &vec![PathBuf::from("target")]
        ));
        assert!(Cli::try_parse_from(["kache", "targets-dedup"]).is_err());
    }

    #[test]
    fn clean_flags_keep_targets_and_cache_apart() {
        let parse = |args: &[&str]| Cli::try_parse_from(["kache", "clean"].iter().chain(args));
        assert!(parse(&["--cache"]).is_ok());
        assert!(
            parse(&["--cache", "--all"]).is_ok(),
            "older scripts still parse"
        );
        assert!(parse(&["--cache", "--stale", "7d"]).is_ok());
        assert!(parse(&["--crate", "serde", "--yes"]).is_ok());
        assert!(parse(&["some/dir", "--dry-run"]).is_ok());
        assert!(parse(&["--tracked"]).is_ok(), "older scripts still parse");
        assert!(parse(&["--all"]).is_err(), "--all needs --cache");
        assert!(
            parse(&["--stale-schema"]).is_err(),
            "--stale-schema needs --cache"
        );
        assert!(parse(&["some/dir", "--cache"]).is_err());
        assert!(parse(&["--orphans", "--stale", "7d"]).is_err());
        assert!(parse(&["--crate", "serde", "--cache"]).is_err());
        assert!(parse(&["--cache", "--all", "--stale", "7d"]).is_err());
        for hidden in ["gc", "purge", "targets", "report"] {
            assert!(Cli::try_parse_from(["kache", hidden]).is_ok(), "{hidden}");
        }
    }

    #[test]
    fn clean_flags_pick_what_to_remove() {
        use cli::{CacheClean, CleanScope, TrackedSelection};
        assert_eq!(cache_clean_choice(false, None), CacheClean::All);
        assert_eq!(
            cache_clean_choice(false, Some(24)),
            CacheClean::OlderThan(24)
        );
        assert_eq!(cache_clean_choice(true, None), CacheClean::StaleSchema);
        assert_eq!(
            clean_scope(None, false, None),
            CleanScope::Tracked(TrackedSelection::StaleOrOrphaned(
                cli::DEFAULT_TRACKED_STALE_HOURS
            ))
        );
        assert_eq!(
            clean_scope(None, false, Some(48)),
            CleanScope::Tracked(TrackedSelection::StaleOrOrphaned(48))
        );
        assert_eq!(
            clean_scope(None, true, None),
            CleanScope::Tracked(TrackedSelection::Orphaned)
        );
        assert_eq!(
            clean_scope(Some(PathBuf::from("d")), true, None),
            CleanScope::Scan(PathBuf::from("d"))
        );
    }

    #[test]
    fn init_shims_and_why_miss_without_a_crate_parse() {
        let shims =
            Cli::try_parse_from(["kache", "init", "--shims", "--from-path", "--force"]).unwrap();
        assert!(matches!(
            shims.command,
            Some(Commands::Init {
                shims: Some(None),
                from_path: true,
                force: true,
                ..
            })
        ));
        let package =
            Cli::try_parse_from(["kache", "init", "--shims", "/usr/lib/kache", "--force"]).unwrap();
        assert!(matches!(
            package.command,
            Some(Commands::Init { shims: Some(Some(ref dir)), force: true, .. })
                if dir == std::path::Path::new("/usr/lib/kache")
        ));
        let plain = Cli::try_parse_from(["kache", "init"]).unwrap();
        assert!(matches!(
            plain.command,
            Some(Commands::Init { shims: None, .. })
        ));
        assert!(
            Cli::try_parse_from(["kache", "init", "--from-path"]).is_err(),
            "needs --shims"
        );
        assert!(Cli::try_parse_from(["kache", "init", "--shims", "--check"]).is_err());
        let canonical =
            Cli::try_parse_from(["kache", "init", "--shims", "--canonical-only"]).unwrap();
        assert!(matches!(
            canonical.command,
            Some(Commands::Init {
                canonical_only: true,
                from_path: false,
                ..
            })
        ));
        assert!(Cli::try_parse_from(["kache", "init", "--canonical-only"]).is_err());
        assert!(
            Cli::try_parse_from([
                "kache",
                "init",
                "--shims",
                "--canonical-only",
                "--from-path"
            ])
            .is_err()
        );
        let build = Cli::try_parse_from(["kache", "why-miss", "--root", "x"]).unwrap();
        assert!(matches!(
            build.command,
            Some(Commands::Explain {
                crate_name: None,
                root: Some(_)
            })
        ));
        assert!(Cli::try_parse_from(["kache", "why-miss", "serde", "--root", "x"]).is_err());
        for hidden in ["install-shims", "diff"] {
            assert!(Cli::try_parse_from(["kache", hidden]).is_ok(), "{hidden}");
        }
    }

    #[test]
    fn help_lists_commands_by_task_and_leaves_out_plumbing() {
        use clap::CommandFactory;
        let help = Cli::command().render_help().to_string();
        let listed: Vec<&str> = help
            .lines()
            .skip_while(|line| *line != "Commands:")
            .skip(1)
            .take_while(|line| !line.is_empty())
            .filter_map(|line| line.split_whitespace().next())
            .collect();
        assert_eq!(
            listed,
            [
                "init",
                "doctor",
                "config",
                "stats",
                "clean",
                "monitor",
                "explain",
                "list",
                "prefetch",
                "sync",
                "login",
                "logout",
                "daemon",
                "completions",
                "help",
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn init_shims_creates_the_links_in_the_named_directory() {
        let dir = tempfile::tempdir().unwrap();
        let farm = dir.path().join("farm");
        install_shims(Some(farm.clone()), false, false, true).unwrap();
        for name in ["cc", "c++", "cargo"] {
            assert!(
                farm.join(name)
                    .symlink_metadata()
                    .is_ok_and(|meta| meta.file_type().is_symlink()),
                "{name}"
            );
        }
    }

    #[test]
    fn json_support_is_limited_to_machine_readable_commands() {
        assert!(command_supports_json(&Some(Commands::Diff { root: None })));
        assert!(command_supports_json(&Some(Commands::Targets {
            command: None
        })));
        assert!(command_supports_json(&Some(Commands::Targets {
            command: Some(TargetCommands::Share {
                apply: false,
                paths: Vec::new(),
            }),
        })));
        assert!(command_supports_json(&Some(Commands::Stats {
            since: "24h".to_string(),
            last_build: false,
            root: None,
            full: false,
            format: None,
            output: None,
            top: None,
            record: false,
            redact: false,
        })));
        assert!(command_supports_json(&Some(Commands::Daemon {
            command: Some(DaemonCommands::Status),
        })));
        assert!(!command_supports_json(&Some(Commands::Config)));
        assert!(!command_supports_json(&Some(Commands::Monitor {
            since: None
        })));
        assert!(!command_supports_json(&Some(Commands::Telemetry {
            command: TelemetryCommands::Write {
                dir: PathBuf::from("/tmp"),
                scenario: None,
                phase: None,
            },
        })));
        assert!(!command_supports_json(&None));

        assert!(doctor_json_is_read_only(false, false, false, false, false));
        for flags in [
            (true, false, false, false, false),
            (false, true, false, false, false),
            (false, false, true, false, false),
            (false, false, false, true, false),
            (false, false, false, false, true),
        ] {
            assert!(!doctor_json_is_read_only(
                flags.0, flags.1, flags.2, flags.3, flags.4
            ));
        }
    }

    #[test]
    fn workspace_wrapper_incremental_preservation_requires_opt_in_and_no_extra_inputs() {
        for (requested, extra_inputs_active, expected) in [
            (false, false, false),
            (false, true, false),
            (true, false, true),
            (true, true, false),
        ] {
            assert_eq!(
                preserve_workspace_wrapper_incremental(requested, extra_inputs_active),
                expected
            );
        }
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

    #[test]
    fn wrapper_target_routes_every_adapter() {
        let adapter_for = |argv: &[&str]| {
            compiler::detect_compiler(
                &argv
                    .iter()
                    .map(|arg| (*arg).to_string())
                    .collect::<Vec<_>>(),
            )
            .expect("adapter must match")
        };
        assert_eq!(
            wrapper_target(adapter_for(&["rustc", "--crate-name", "foo"])),
            Some(WrapperTarget::Rustc)
        );
        assert_eq!(
            wrapper_target(adapter_for(&["rustdoc", "--crate-name", "demo"])),
            Some(WrapperTarget::Rustdoc)
        );
        assert_eq!(
            wrapper_target(adapter_for(&["cc", "-c", "foo.c"])),
            Some(WrapperTarget::Cc)
        );
        assert_eq!(
            wrapper_target(adapter_for(&["nvcc", "-c", "kernel.cu"])),
            Some(WrapperTarget::Nvcc)
        );
        let unknown =
            compiler::CompilerAdapter::new(compiler::CompilerId::new("unwired"), "unwired", |_| {
                false
            });
        assert_eq!(wrapper_target(&unknown), None);
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
        let dir = tempfile::tempdir().unwrap();
        let compiler = dir.path().join("fake-cc");
        let dump = dir.path().join("argv.txt");
        let response = dir.path().join("cc.args");
        kache_fs::testutil::write_executable(
            &compiler,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"{}\"\n",
                dump.display()
            ),
        );
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
        let dir = tempfile::tempdir().unwrap();
        let outer = dir.path().join("workspace-wrapper");
        let inner = dir.path().join("rustc");
        let dump = dir.path().join("argv.txt");
        let response = dir.path().join("rustc.args");
        let incremental = dir.path().join("target/debug/incremental/unit");
        kache_fs::testutil::write_executable(
            &outer,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$1\" > \"{}\"\nshift\nfor arg in \"$@\"; do\n  case \"$arg\" in\n    @*) cat \"${{arg#@}}\" >> \"{}\";;\n    *) printf '%s\\n' \"$arg\" >> \"{}\";;\n  esac\ndone\n",
                dump.display(),
                dump.display(),
                dump.display()
            ),
        );
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
    fn test_runner_args_take_everything_after_the_subcommand() {
        let os = |args: &[&str]| -> Vec<std::ffi::OsString> {
            args.iter().map(std::ffi::OsString::from).collect()
        };
        let argv = os(&[
            "kache",
            "test-runner",
            "/t/probe-0123456789abcdef",
            "--exact",
        ]);
        assert_eq!(
            test_runner_args(&argv),
            Some(&argv[2..]),
            "the binary and its arguments"
        );
        let bare = os(&["kache", "test-runner"]);
        assert_eq!(test_runner_args(&bare), Some(&bare[2..]));
        assert_eq!(
            test_runner_args(&os(&["kache", "rustc", "test-runner"])),
            None
        );
        assert_eq!(test_runner_args(&os(&["kache"])), None);
        // A compile through a shim whose first input is named `test-runner`.
        assert_eq!(
            test_runner_args(&os(&["/x/shims/cc", "test-runner", "-o", "app"])),
            None
        );
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
        // nvcc dispatches to run_nvcc via its adapter (#1024), which is
        // wrapper-mode logging like every other compiler path.
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
