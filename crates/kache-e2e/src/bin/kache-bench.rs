//! `kache-bench` — compile-cache benchmark, driven by a bench profile.
//!
//! Builds a real project (Firefox, Substrate, …) twice against one
//! shared kache cache:
//!
//! - **cold**: a fresh clone with an empty cache → every compile is a
//!   miss. This is the baseline.
//! - **warm**: a second, independent working tree at a *different*
//!   absolute path against the same cache → compiles are served from
//!   what cold stored. The different path is deliberate: it mirrors a
//!   real fresh checkout (a new CI runner / a teammate's machine) and
//!   exposes absolute-path leaks in the cache key — a path-dependent
//!   key would miss everything. Both working trees are git worktrees
//!   off a locally-cached reference clone so re-runs don't pay the
//!   network cost twice.
//!
//! The *project-specific* parts — which repo, how to wire kache in, how
//! to build — live in a **bench profile** (`bench-profiles/<name>.toml`);
//! this binary is the project-agnostic measurement engine. See
//! `bench-profiles/README.md` for the profile format. Adding a project
//! is dropping a `.toml`, not editing this file.
//!
//! Reports cold/warm wall-clock, speedup, and hit rate. This is a manual
//! tool: a full run takes tens of minutes to a few hours and needs tens
//! of GB of disk. It is intentionally NOT wired into CI.
//!
//! The benchmark is **self-diagnosing**: it captures kache's own leak
//! detector, measures cross-clone cache-key stability, accounts for the
//! passthrough events `kache report` hides, and fails the run (non-zero
//! exit) when those signals say the run did not validly exercise kache —
//! so a broken run can't masquerade as a tidy speedup.
//!
//! Reuses [`kache_e2e::report`] — the typed `kache report --format json`
//! fetch and parsing are shared with the e2e harness — and
//! [`kache_e2e::bench_profile`] for the profile format.
//!
//! # Running the benchmark
//!
//! ```text
//! just bench firefox               # full cold + warm
//! just bench substrate
//! kache-bench --profile firefox --retry        # re-measure warm only
//! kache-bench --profile firefox --skip-clone   # reuse existing clones
//! kache-bench --profile firefox --ref FIREFOX_152_0_RELEASE   # override the ref
//! ```
//!
//! A profile's `requires` tools must be on `PATH` or the run is skipped;
//! its `setup` (e.g. `./mach bootstrap`) runs once after a fresh clone.
//!
//! # Reading the output
//!
//! Each run prints a summary block and writes `tmp/bench/<profile>/<profile>.json`
//! plus per-phase reports (`report-<phase>.{json,md}`), build logs
//! (`build-<phase>.log`), and kache wrapper logs (`wrapper-<phase>.log`).
//! By default each profile writes under its own `./tmp/bench/<profile>` (so
//! profiles coexist); a `work_dir` lock prevents two runs from sharing one
//! scratch dir. Concurrent runs on one host make the wall-clock numbers
//! unreliable — run sequentially or on separate hosts.
//!
//! The headline metrics — wall-clock, speedup, hit rate — are only
//! meaningful when the **verdict** is `ok`. A `DEGRADED RUN` verdict
//! means at least one diagnostic signal flagged a problem (path-leak
//! warns fired, cross-clone key stability collapsed, kache barely
//! exercised, or cache errors occurred). The summary lists the specific
//! issues; treat the speedup as suspect until they're addressed.
//!
//! # Maintenance
//!
//! To bump a project's pinned version, edit `ref` in its profile (or pass
//! `--ref` for a one-off). If a build tool changes its wrapper-injection
//! knobs, update the profile's `[[file]]` / `[env]`, not this binary.

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use kache_e2e::bench_profile::BenchProfile;
use kache_e2e::report;

/// Where bench profiles live, relative to the invocation CWD (the repo
/// root, the way `just bench` runs the binary).
const PROFILES_DIR: &str = "bench-profiles";

#[derive(Debug, Parser)]
#[command(about = "Compile-cache benchmark for kache, driven by a bench profile.")]
struct Args {
    /// Bench profile to run (required): a name resolved against
    /// `bench-profiles/` (e.g. `firefox`, `substrate`) or a path to a
    /// `.toml`.
    #[arg(long)]
    profile: String,

    /// Override the profile's pinned `ref` (tag/branch/commit) for this
    /// run — e.g. to bench a newer release without editing the profile.
    #[arg(long = "ref")]
    git_ref: Option<String>,

    /// kache binary under test.
    #[arg(long, default_value = "./target/release/kache")]
    kache: PathBuf,

    /// Scratch directory for the worktrees, objdirs and cache. Lives
    /// under the repo's `tmp/` convention; gitignored. The whole
    /// directory tree (clone-a, clone-b, cache, snapshots) can be
    /// `rm -rf`ed at any time; subsequent runs re-derive what they need.
    ///
    /// Defaults to `./tmp/bench/<profile>` (per-profile, so profiles never
    /// share state). Pass an explicit path to override.
    ///
    /// The reference clone is kept at a SIBLING path —
    /// `<work_dir>-clone-ref` (e.g. `./tmp/bench/<profile>-clone-ref`) — so
    /// that a casual `rm -rf <work_dir>` doesn't wipe the network clone.
    /// Re-cloning happens automatically when the reference goes missing
    /// or its HEAD doesn't match the profile's `ref`.
    #[arg(long)]
    work_dir: Option<PathBuf>,

    /// Reuse clones already present under the work dir.
    #[arg(long)]
    skip_clone: bool,

    /// Re-run the profile's `setup` even if its `setup_marker` exists.
    #[arg(long)]
    force_setup: bool,

    /// Skip the cold build: restore the cold-state cache snapshot saved
    /// by the previous full run and only re-measure the warm phase
    /// (~25 minutes saved). Requires a prior successful full run.
    #[arg(long)]
    retry: bool,

    /// Elevate the kache wrapper log to `kache::cache_key=trace` so every
    /// cache-key input (env vars, codegen flags, RUSTFLAGS, remap, …) is
    /// written to `wrapper-<phase>.log`. After warm, the bench diffs the
    /// two phases' traces per crate and emits `key-diff.json` /
    /// `key-diff.md` listing which key inputs diverged across clones —
    /// the actionable signal for path-leak debugging when key stability
    /// drops below 100%. Logs grow by ~50–100 MB per phase; gitignored.
    #[arg(long)]
    trace_keys: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let kache_path = resolve_binary(&args.kache);
    let kache = de_verbatim(
        kache_path
            .canonicalize()
            .with_context(|| format!("kache binary not found at {}", kache_path.display()))?,
    );

    // Resolve a POSIX shell up front so a Windows host missing Git bash fails
    // before the (expensive) clone, not midway through setup.
    let sh = posix_sh()?;

    // Load the project profile (which repo, how to wire kache in, how to
    // build). Everything below this is the project-agnostic measurement
    // engine.
    let mut profile = BenchProfile::resolve(&args.profile, Path::new(PROFILES_DIR))?;
    if let Some(r) = &args.git_ref {
        profile.git_ref = r.clone();
    }
    let missing = profile.missing_requirements();
    if !missing.is_empty() {
        eprintln!(
            "[bench] SKIP profile `{}`: missing required tool(s) on PATH: {}",
            profile.name,
            missing.join(", ")
        );
        return Ok(());
    }
    let objdir = profile.objdir.clone();

    let work_dir_arg = args
        .work_dir
        .unwrap_or_else(|| default_work_dir(&profile.name));
    std::fs::create_dir_all(&work_dir_arg)
        .with_context(|| format!("creating work dir {}", work_dir_arg.display()))?;
    let work_dir = de_verbatim(work_dir_arg.canonicalize()?);
    // Hold an exclusive lock on the work dir for the whole run so a second
    // bench can't share this scratch dir and clobber it. Bound (not `_`) so it
    // lives to the end of `main`; released automatically on process exit.
    let _work_dir_lock = acquire_work_dir_lock(&work_dir)?;

    // `clone-ref` lives at a SIBLING of `work_dir` (not inside it) so
    // the natural `rm -rf <work_dir>` wipe — what someone reaches for
    // to reset the bench — doesn't accidentally torch the locally-cached
    // reference clone. Re-cloning a large project is the bench's single
    // most expensive setup step; making the reference survive a casual
    // scratch-dir wipe is the whole point.
    let clone_ref = clone_ref_path(&work_dir);
    let clone_a = work_dir.join("clone-a");
    let clone_b = work_dir.join("clone-b");
    let cache_dir = work_dir.join("cache");
    let kache_config = work_dir.join("kache-config.toml");
    let event_log = cache_dir.join("events.jsonl");

    eprintln!("=== kache benchmark: {} ===", profile.name);
    eprintln!("ref         : {}", profile.git_ref);
    eprintln!(
        "work dir    : {}  (worktrees + objdirs + cache)",
        work_dir.display()
    );
    eprintln!(
        "clone ref   : {}  (persistent across runs)",
        clone_ref.display()
    );
    eprintln!("kache       : {}", kache.display());

    // kache rotates its event log at 10 MiB, keeping only the last 500
    // lines. A large project build emits tens of thousands of events, so
    // the default would discard nearly everything before the report is
    // read. A large cap keeps every event of a phase intact for its report.
    std::fs::write(&kache_config, "[cache]\nevent_log_max_size = \"8GiB\"\n")
        .context("writing benchmark kache config")?;

    if args.skip_clone || args.retry {
        if !clone_a.is_dir() || !clone_b.is_dir() {
            let flag = if args.retry {
                "--retry"
            } else {
                "--skip-clone"
            };
            bail!(
                "{flag} set but clones are missing under {}",
                work_dir.display()
            );
        }
        let flag = if args.retry {
            "--retry"
        } else {
            "--skip-clone"
        };
        eprintln!("\n[bench] reusing existing clones ({flag})");
    } else {
        clone_worktrees(
            &profile.repo,
            &profile.git_ref,
            &clone_ref,
            &clone_a,
            &clone_b,
        )?;
        run_setup(&profile, &clone_a, &kache, args.force_setup, &sh)?;
    }

    // Wire kache into each worktree (mozconfig write, .cargo/config append,
    // …). Applied once per fresh worktree; on `--skip-clone` the reused
    // tree keeps prior injections, so `write` overwrites cleanly but
    // `append`/`patch` profiles want a full (re-cloning) run.
    for clone in [&clone_a, &clone_b] {
        profile.apply_files(clone, &kache)?;
    }

    // cold: either run it fresh (full run) or restore the snapshot saved
    // by a prior full run (`--retry`) and reuse cold's metrics. Either
    // way we emerge with `cold_metrics` + `cold_raw`.
    let (cold_metrics, cold_raw) = if args.retry {
        retry_load_cold(&profile.name, &kache, &cache_dir, &work_dir)?
    } else {
        run_cold_phase(
            &profile,
            &kache,
            &cache_dir,
            &kache_config,
            &clone_a,
            &work_dir,
            &event_log,
            args.trace_keys,
            &sh,
        )?
    };

    // Reset the event log so warm's report covers only the warm build.
    // Only the observability log is removed — the cache store (`store/`,
    // `index.db`) stays, so warm still hits everything cold populated.
    if event_log.exists() {
        std::fs::remove_file(&event_log)
            .with_context(|| format!("resetting {}", event_log.display()))?;
    }

    // warm: same cache, fresh clone at a different path → served from
    // what cold populated.
    let warm_s = build(
        &profile,
        &clone_b,
        "warm",
        &cache_dir,
        &kache_config,
        &kache,
        &work_dir,
        args.trace_keys,
        &sh,
    )?;
    daemon_stop(&kache, &cache_dir);
    let (warm, warm_raw) = capture_report(&kache, &cache_dir, &work_dir, "warm")?;
    let warm_events = read_event_log(&event_log);
    let (warm_leaks, warm_leak_samples) = scan_leak_warnings(&work_dir.join("wrapper-warm.log"));

    let speedup = if warm_s > 0 {
        cold_metrics.wall_s as f64 / warm_s as f64
    } else {
        0.0
    };

    // Cross-clone cache-key stability: for the deterministic correctness
    // signal, see how many crates produced an identical key in both
    // clones. A path leak in the key shows up here as a near-zero rate.
    let stability = key_stability(&cold_raw, &warm_raw);

    let warm_metrics = PhaseMetrics::from_report(&warm, &warm_raw, warm_s, warm_events, warm_leaks);
    let verdict = Verdict::evaluate(&stability, &warm_metrics);

    // Apparent (`du`) size of each clone's objdir. On APFS the warm
    // clone's apparent size double-counts the bytes it reflinked from
    // the cache — print_summary subtracts those to expose "unique to
    // this clone" disk usage.
    let cold_objdir_bytes = dir_size_kb(&clone_a.join(&objdir)).saturating_mul(1024);
    let warm_objdir_bytes = dir_size_kb(&clone_b.join(&objdir)).saturating_mul(1024);

    // With `--trace-keys`, both phases logged every key-input the hasher
    // consumed (one line per input, prefixed `[key:CRATE]`). Diff the
    // two phases per-crate and aggregate by input field so the run
    // can name what diverged across clones — the actionable signal
    // when `key_stability` < 100%.
    let key_diff_top = if args.trace_keys {
        let cold_trace = parse_key_trace(&work_dir.join("wrapper-cold.log"));
        let warm_trace = parse_key_trace(&work_dir.join("wrapper-warm.log"));
        let divergence = compute_key_divergence(&cold_trace, &warm_trace);
        write_key_diff_reports(&divergence, &work_dir)?
    } else {
        None
    };

    let result = BenchResult {
        project: profile.name.clone(),
        git_ref: profile.git_ref.clone(),
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        cold: cold_metrics,
        warm: warm_metrics,
        speedup: round2(speedup),
        cache_size_mb: round1(dir_size_kb(&cache_dir) as f64 / 1024.0),
        cold_objdir_bytes,
        warm_objdir_bytes,
        key_stability: stability,
        warm_leak_samples,
        verdict,
        key_diff_top: key_diff_top.clone(),
        reports: [
            "report-cold.json",
            "report-cold.md",
            "report-warm.json",
            "report-warm.md",
            "build-cold.log",
            "build-warm.log",
            "wrapper-cold.log",
            "wrapper-warm.log",
        ]
        .map(String::from)
        .to_vec(),
    };

    let out = work_dir.join(format!("{}.json", profile.name));
    std::fs::write(&out, serde_json::to_string_pretty(&result)? + "\n")
        .with_context(|| format!("writing {}", out.display()))?;

    print_summary(&result, &work_dir);
    eprintln!("[bench] summary written to {}", out.display());

    // Fail directly: a degraded run must not exit 0 — a tidy speedup from
    // a run that never validly exercised kache is worse than no number.
    if !result.verdict.ok {
        std::process::exit(1);
    }
    Ok(())
}

/// Default scratch dir for a profile when `--work-dir` is not given:
/// `./tmp/bench/<profile>`, so profiles never share state and a single
/// `rm -rf tmp/bench` cleans every profile at once.
fn default_work_dir(profile_name: &str) -> PathBuf {
    PathBuf::from(format!("./tmp/bench/{profile_name}"))
}

/// Take an exclusive advisory lock on the work dir so two bench runs can't
/// share the same scratch dir and clobber each other. The returned `File` must
/// be held for the whole run; the OS releases the lock when the process exits,
/// so there are no stale locks even on panic or `kill`.
fn acquire_work_dir_lock(work_dir: &Path) -> Result<File> {
    let lock_path = work_dir.join(".bench.lock");
    let file = File::create(&lock_path)
        .with_context(|| format!("creating lock file {}", lock_path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => bail!(
            "another kache-bench run is already using {} — pass a different \
             --work-dir or wait for it to finish",
            work_dir.display()
        ),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(e).with_context(|| format!("locking {}", lock_path.display()))
        }
    }
}

/// Derive the persistent reference-clone path for a given work dir.
/// Sibling, not child: appends `-clone-ref` to the work dir's name so
/// the reference outlives a `rm -rf <work_dir>` wipe.
///
/// Examples: `./tmp/bench` → `./tmp/bench-clone-ref`,
/// `/scratch/foo` → `/scratch/foo-clone-ref`. Falls back to placing the
/// reference next to the work dir under a generic name if the work
/// dir has no file name (root paths, current dir, etc.).
fn clone_ref_path(work_dir: &Path) -> PathBuf {
    let parent = work_dir.parent().unwrap_or(Path::new("."));
    let name = work_dir
        .file_name()
        .map(|n| format!("{}-clone-ref", n.to_string_lossy()))
        .unwrap_or_else(|| "bench-clone-ref".to_string());
    parent.join(name)
}

/// Materialize `clone_a` and `clone_b` at `tag` (the profile's `ref`) as
/// git worktrees off a locally-cached `clone_ref` shallow clone.
///
/// The first run for a given `tag` pays the network cost once — a
/// `--depth=1 --branch=<tag>` clone into `clone_ref`. Every subsequent
/// fresh full run reuses that local reference and only re-creates
/// `clone_a` / `clone_b` as fresh worktrees. When the `ref` changes
/// between runs the cached clone is detected as stale and wiped before
/// the new fetch.
///
/// `clone_ref` is a SIBLING of the bench work dir, not a child — see
/// [`clone_ref_path`] — so `rm -rf <work_dir>` doesn't torch it.
///
/// Worktrees share `clone_ref`'s object database but each lives at its
/// own absolute path with independent working-tree files — the
/// path-leak detector still measures what it always did (the cache key
/// must not depend on which absolute directory the build was run in).
///
/// `--detach` keeps each worktree on the underlying commit rather than
/// creating a branch.
fn clone_worktrees(
    repo: &str,
    tag: &str,
    clone_ref: &Path,
    clone_a: &Path,
    clone_b: &Path,
) -> Result<()> {
    // 1. Bring clone-ref up to the requested tag (network cost, paid
    //    once per tag bump).
    if clone_ref_at_tag(clone_ref, tag)? {
        eprintln!("\n[bench] reusing clone-ref at {tag} (no network)");
    } else {
        if clone_ref.exists() {
            eprintln!("\n[bench] clone-ref is at the wrong tag — wiping and re-cloning");
            std::fs::remove_dir_all(clone_ref)
                .with_context(|| format!("removing stale {}", clone_ref.display()))?;
        } else {
            eprintln!("\n[bench] no clone-ref yet — fetching {repo} @ {tag} (one-time cost)");
        }
        if let Some(parent) = clone_ref.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        run(Command::new("git")
            // core.longpaths: polkadot-sdk / Firefox have paths > 260 chars,
            // so a Windows clone fails without it. Harmless no-op on Unix.
            .args([
                "-c",
                "core.longpaths=true",
                "clone",
                "--depth",
                "1",
                "--branch",
            ])
            .arg(tag)
            .arg(repo)
            .arg(clone_ref))?;
    }

    // 2. Reset clone-a and clone-b. Worktree-aware cleanup first; fall
    //    back to a plain `remove_dir_all` for legacy non-worktree dirs
    //    left over from older bench versions.
    for d in [clone_a, clone_b] {
        if d.exists() {
            // Removing a previous worktree (full source + objdir, often
            // several GB / 100k+ files) is slow and silent — say so, so a
            // re-run doesn't look hung after "reusing clone-ref".
            eprintln!(
                "[bench] removing previous worktree {} (can take a few minutes, no progress output)…",
                d.display()
            );
        }
        reset_worktree_path(clone_ref, d)?;
    }
    // Prune any stale worktree registrations the cleanup might have
    // left behind (e.g., interrupted runs where the worktree dir was
    // removed but the registration in `.git/worktrees/` remains).
    let _ = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["worktree", "prune"])
        .status();

    // 3. Spawn fresh worktrees at `tag`. `--detach` mirrors the old
    //    behavior (detached HEAD on the tag's commit, no branch).
    for d in [clone_a, clone_b] {
        eprintln!("[bench] creating worktree {}", d.display());
        run(Command::new("git")
            .args(["-c", "core.longpaths=true"])
            .arg("-C")
            .arg(clone_ref)
            .args(["worktree", "add", "--detach"])
            .arg(d)
            .arg(tag))?;
    }
    Ok(())
}

/// True when `clone_ref` is a valid git repository whose HEAD is the
/// commit named by `tag`. A missing `.git`, a missing tag ref, or a
/// HEAD pointing elsewhere all return false — the caller wipes and
/// re-clones on any negative.
fn clone_ref_at_tag(clone_ref: &Path, tag: &str) -> Result<bool> {
    if !clone_ref.join(".git").exists() {
        return Ok(false);
    }
    let head = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["rev-parse", "HEAD"])
        .output();
    let tagged = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["rev-parse"])
        .arg(format!("{tag}^{{commit}}"))
        .output();
    Ok(matches!(
        (head, tagged),
        (Ok(h), Ok(t)) if h.status.success() && t.status.success() && h.stdout == t.stdout
    ))
}

/// Best-effort cleanup of a previous worktree at `target`. Tries the
/// worktree-aware command first; falls back to `remove_dir_all` so
/// pre-worktree clone dirs (and corrupt / partially-cleaned trees) are
/// also handled idempotently. Never errors: if the path is already
/// gone, both branches no-op.
fn reset_worktree_path(clone_ref: &Path, target: &Path) -> Result<()> {
    if !target.exists() {
        return Ok(());
    }
    let _ = Command::new("git")
        .arg("-C")
        .arg(clone_ref)
        .args(["worktree", "remove", "--force"])
        .arg(target)
        .status();
    if target.exists() {
        std::fs::remove_dir_all(target)
            .with_context(|| format!("removing {}", target.display()))?;
    }
    Ok(())
}

/// Return `path` if it exists; else `path` with a `.exe` extension if THAT
/// exists; else `path` unchanged (so the caller's not-found error names the
/// path the user gave). Lets a Unix-style `./target/release/kache` resolve to
/// `kache.exe` on Windows. No-op on Unix where the bare path exists.
/// Strip Windows' `\\?\` verbatim / extended-length prefix that
/// `Path::canonicalize` adds — `git` and many tools reject verbatim paths, so
/// every path derived from a canonicalized `work_dir` (clone-ref, worktrees,
/// cache) must be plain. No-op on Unix and for already-plain paths.
fn de_verbatim(p: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        let s = p.to_string_lossy();
        if let Some(rest) = s.strip_prefix(r"\\?\UNC\") {
            return PathBuf::from(format!(r"\\{rest}"));
        }
        if let Some(rest) = s.strip_prefix(r"\\?\") {
            return PathBuf::from(rest.to_string());
        }
    }
    p
}

fn resolve_binary(path: &Path) -> PathBuf {
    if path.exists() {
        return path.to_path_buf();
    }
    let exe = path.with_extension("exe");
    if exe.exists() {
        return exe;
    }
    path.to_path_buf()
}

/// Resolve a POSIX `sh` to run profile setup/build snippets. Unix: `sh`
/// (always on PATH; `Command` resolves it). Windows: Git for Windows'
/// `sh.exe` — PATH, then derived from `git --exec-path`, then well-known
/// install roots — with a clear error if none is found.
fn posix_sh() -> Result<PathBuf> {
    #[cfg(not(windows))]
    {
        Ok(PathBuf::from("sh"))
    }
    #[cfg(windows)]
    {
        find_windows_sh().context(
            "no POSIX `sh` found — install Git for Windows (provides \
             usr\\bin\\sh.exe) or put a POSIX sh on PATH",
        )
    }
}

#[cfg(windows)]
fn find_windows_sh() -> Option<PathBuf> {
    // 1. sh.exe already on PATH.
    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let cand = dir.join("sh.exe");
            if cand.is_file() {
                return Some(cand);
            }
        }
    }
    // 2. Derive from `git --exec-path` (…\Git\mingw64\libexec\git-core).
    if let Ok(out) = Command::new("git").arg("--exec-path").output()
        && out.status.success()
    {
        let exec = PathBuf::from(String::from_utf8_lossy(&out.stdout).trim());
        for anc in exec.ancestors() {
            let cand = anc.join("usr").join("bin").join("sh.exe");
            if cand.is_file() {
                return Some(cand);
            }
        }
    }
    // 3. Well-known install roots.
    for root in [r"C:\Program Files\Git", r"C:\Program Files (x86)\Git"] {
        let cand = Path::new(root).join(r"usr\bin\sh.exe");
        if cand.is_file() {
            return Some(cand);
        }
    }
    None
}

/// Run the profile's one-time `setup` steps (e.g. `./mach bootstrap`,
/// `rustup target add`) in `clone`. Skipped when the profile's
/// `setup_marker` already exists unless `force` is set. Each step runs
/// via `sh -c` with the kache env available for interpolation.
fn run_setup(
    profile: &BenchProfile,
    clone: &Path,
    kache: &Path,
    force: bool,
    sh: &Path,
) -> Result<()> {
    let steps = profile.setup_commands(kache);
    if steps.is_empty() {
        return Ok(());
    }
    if profile.setup_satisfied(force) {
        eprintln!(
            "\n[bench] skipping setup (marker {} exists; --force-setup to redo)",
            profile.setup_marker.as_deref().unwrap_or("")
        );
        return Ok(());
    }
    for step in &steps {
        eprintln!("\n[bench] setup: {step}");
        run(Command::new(sh).arg("-c").arg(step).current_dir(clone))?;
    }
    Ok(())
}

/// Build the project in `clone` with kache wired in; return wall-clock
/// seconds.
///
/// kache writes wrapper-mode diagnostics — in particular
/// `PathNormalizer`'s residual-path leak detector — directly to
/// `wrapper-<phase>.log` via `KACHE_LOG_FILE=…` + `KACHE_LOG_FILE_PATH=…`.
/// Stderr is unreliable here: cargo captures `RUSTC_WRAPPER` stderr and
/// replays it as compiler diagnostics, so warns emitted through stderr
/// never reach `build-<phase>.log`. The dedicated file path side-steps
/// that. `build-<phase>.log` still captures the build tool's own output
/// for failure triage.
///
/// The baseline kache env (`KACHE_CACHE_DIR` / `KACHE_CONFIG` /
/// `RUSTC_WRAPPER` / `KACHE_LOG*`) is set last so a profile's `[env]`
/// can't accidentally override it.
#[allow(clippy::too_many_arguments)]
fn build(
    profile: &BenchProfile,
    clone: &Path,
    phase: &str,
    cache_dir: &Path,
    kache_config: &Path,
    kache: &Path,
    work_dir: &Path,
    trace_keys: bool,
    sh: &Path,
) -> Result<u64> {
    let log_path = work_dir.join(format!("build-{phase}.log"));
    let wrapper_log_path = work_dir.join(format!("wrapper-{phase}.log"));
    let mut log =
        File::create(&log_path).with_context(|| format!("creating {}", log_path.display()))?;
    // Truncate any prior wrapper log so the phase starts clean — kache
    // appends to this file across all parallel wrapper processes.
    File::create(&wrapper_log_path)
        .with_context(|| format!("creating {}", wrapper_log_path.display()))?;
    let build_cmd = profile.build_command(kache);
    eprintln!(
        "\n[bench] [{phase}] building {} in {} (`{build_cmd}`; output -> build-{phase}.log, kache wrapper warns -> wrapper-{phase}.log)",
        profile.name,
        clone.display()
    );
    // Wipe the objdir so every phase is a genuine from-scratch build.
    // No-op for a fresh clone; on --skip-clone it removes the previous
    // run's objdir, which would otherwise make "cold" an incremental
    // build. Done before the timer — it's setup, not build work.
    let objdir = clone.join(&profile.objdir);
    if objdir.exists() {
        std::fs::remove_dir_all(&objdir)
            .with_context(|| format!("wiping objdir {}", objdir.display()))?;
    }
    let started = Instant::now();
    let mut child = Command::new(sh)
        .arg("-c")
        .arg(&build_cmd)
        .current_dir(clone)
        // Profile env first so the baseline below always wins.
        .envs(profile.build_env(kache))
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", kache_config)
        .env("RUSTC_WRAPPER", kache)
        // Incremental is redundant once a compile cache is in play (kache
        // already excludes -Cincremental from the key); off keeps the
        // build dir lean and removes a measurement-noise source. Injected
        // for every profile (Firefox also sets it via mozconfig).
        .env("CARGO_INCREMENTAL", "0")
        // KACHE_LOG still un-mutes stderr (visible if you tail the log),
        // but the authoritative leak-detector signal goes to the file via
        // KACHE_LOG_FILE — cargo eats wrapper stderr. With `--trace-keys`
        // the file gets elevated to `kache::cache_key=trace` so every
        // input that feeds the key hasher (env vars, codegen, RUSTFLAGS,
        // remap, …) lands in the log and the post-warm diff helper can
        // surface which inputs diverged across clones.
        .env("KACHE_LOG", "kache=warn")
        .env(
            "KACHE_LOG_FILE",
            if trace_keys {
                "kache::cache_key=trace,kache=warn"
            } else {
                "kache=warn"
            },
        )
        .env("KACHE_LOG_FILE_PATH", &wrapper_log_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::from(
            log.try_clone().context("cloning build-log handle")?,
        ))
        .spawn()
        .with_context(|| format!("spawning build command in {}", clone.display()))?;

    // Tee the build tool's stdout to the console (live progress) AND the
    // log file. The tool writes its build output — and the kache wrapper
    // diagnostics relayed through it — to stdout, so the log must capture
    // stdout, not just stderr. Byte-oriented so non-UTF-8 build output
    // can't break it.
    if let Some(stdout) = child.stdout.take() {
        let mut reader = std::io::BufReader::new(stdout);
        let mut buf = Vec::new();
        while reader.read_until(b'\n', &mut buf)? != 0 {
            let _ = std::io::stdout().write_all(&buf);
            log.write_all(&buf)
                .with_context(|| format!("writing {}", log_path.display()))?;
            buf.clear();
        }
    }
    let status = child.wait().context("waiting for build command")?;
    if !status.success() {
        bail!(
            "[{phase}] build failed ({status}) — see {}",
            log_path.display()
        );
    }
    Ok(started.elapsed().as_secs())
}

/// Capture kache's report for the phase that just finished: write the
/// full raw JSON (`report-<phase>.json`) and kache's human-readable
/// markdown (`report-<phase>.md`) into `out_dir`, and return the parsed
/// report plus the raw value for the benchmark summary.
fn capture_report(
    kache: &Path,
    cache_dir: &Path,
    out_dir: &Path,
    phase: &str,
) -> Result<(report::KacheReport, serde_json::Value)> {
    let (parsed, raw) = report::fetch_since(kache, cache_dir, "365d")?;

    let json_path = out_dir.join(format!("report-{phase}.json"));
    std::fs::write(&json_path, serde_json::to_string_pretty(&raw)? + "\n")
        .with_context(|| format!("writing {}", json_path.display()))?;

    let md = Command::new(kache)
        .args(["report", "--format", "markdown", "--since", "365d"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .output()
        .with_context(|| format!("running `{} report --format markdown`", kache.display()))?;
    if md.status.success() {
        let md_path = out_dir.join(format!("report-{phase}.md"));
        std::fs::write(&md_path, md.stdout)
            .with_context(|| format!("writing {}", md_path.display()))?;
    }

    Ok((parsed, raw))
}

/// Run the cold phase from scratch: wipe the cache, build, capture the
/// report and event log, scan the build log, then snapshot the cache in
/// its post-cold state so a later `--retry` can restore it without
/// paying for the cold rebuild.
#[allow(clippy::too_many_arguments)]
fn run_cold_phase(
    profile: &BenchProfile,
    kache: &Path,
    cache_dir: &Path,
    kache_config: &Path,
    clone_a: &Path,
    work_dir: &Path,
    event_log: &Path,
    trace_keys: bool,
    sh: &Path,
) -> Result<(PhaseMetrics, serde_json::Value)> {
    daemon_stop(kache, cache_dir);
    if cache_dir.exists() {
        std::fs::remove_dir_all(cache_dir).context("clearing cache dir")?;
    }
    std::fs::create_dir_all(cache_dir)?;
    let cold_s = build(
        profile,
        clone_a,
        "cold",
        cache_dir,
        kache_config,
        kache,
        work_dir,
        trace_keys,
        sh,
    )?;
    daemon_stop(kache, cache_dir);
    let (cold, cold_raw) = capture_report(kache, cache_dir, work_dir, "cold")?;
    // Read the raw event log *before* the caller's reset — it carries the
    // passthrough events `kache report` filters out of `all_events`.
    let cold_events = read_event_log(event_log);
    let (cold_leaks, _) = scan_leak_warnings(&work_dir.join("wrapper-cold.log"));

    // Snapshot the cache in its post-cold state so a future `--retry`
    // run can restore it instead of re-running cold.
    snapshot_dir(cache_dir, &work_dir.join("cache-after-cold"))?;

    Ok((
        PhaseMetrics::from_report(&cold, &cold_raw, cold_s, cold_events, cold_leaks),
        cold_raw,
    ))
}

/// Restore the cold-state cache snapshot from a prior full run and load
/// cold's metrics + report from disk. Used by `--retry` to skip the
/// expensive cold rebuild.
fn retry_load_cold(
    profile_name: &str,
    kache: &Path,
    cache_dir: &Path,
    work_dir: &Path,
) -> Result<(PhaseMetrics, serde_json::Value)> {
    let snapshot = work_dir.join("cache-after-cold");
    let result_json = work_dir.join(format!("{profile_name}.json"));
    let report_cold = work_dir.join("report-cold.json");
    for required in [&snapshot, &result_json, &report_cold] {
        if !required.exists() {
            bail!(
                "--retry: required artifact missing — {} (run `just bench {profile_name}` once first)",
                required.display()
            );
        }
    }
    eprintln!(
        "\n[bench] [retry] restoring cold-state cache from {}",
        snapshot.display()
    );
    daemon_stop(kache, cache_dir);
    snapshot_dir(&snapshot, cache_dir)?;

    let cold_raw: serde_json::Value = read_json(&report_cold)?;
    let prev: serde_json::Value = read_json(&result_json)?;
    let cold_metrics: PhaseMetrics =
        serde_json::from_value(prev["cold"].clone()).with_context(|| {
            format!(
                "loading previous cold metrics from {}",
                result_json.display()
            )
        })?;
    Ok((cold_metrics, cold_raw))
}

/// Parse the raw event log (`events.jsonl`) for one phase.
///
/// `kache report` filters `passthrough` and `skipped` events out of its
/// `all_events`/`total_crates`, so the report alone hides how much of a
/// build kache declined to cache. The raw log keeps everything — that is
/// the only way the benchmark sees the passthrough wall.
///
/// JSONL written with `O_APPEND` can occasionally land two objects on a
/// line, so a streaming `Deserializer` is used rather than line splitting.
fn read_event_log(path: &Path) -> EventLogStats {
    let Ok(file) = File::open(path) else {
        return EventLogStats::default();
    };
    let mut stats = EventLogStats::default();
    let mut reasons: HashMap<String, u64> = HashMap::new();
    let stream = serde_json::Deserializer::from_reader(std::io::BufReader::new(file))
        .into_iter::<serde_json::Value>();
    for item in stream {
        let Ok(ev) = item else { continue };
        stats.total += 1;
        // Each event has a `size` field (bytes) for the cache entry's
        // artifact payload. Sum it across hit / miss buckets so the
        // summary can express coverage by bytes, not just by count.
        let size = ev["size"].as_u64().unwrap_or_default();
        match ev["result"].as_str().unwrap_or_default() {
            "local_hit" | "prefetch_hit" | "remote_hit" => {
                stats.cached += 1;
                stats.hit_bytes += size;
            }
            "miss" => {
                stats.cached += 1;
                stats.miss_bytes += size;
            }
            "passthrough" => {
                stats.passed_through += 1;
                if let Some(reason) = ev["passthrough_reason"].as_str()
                    && !reason.is_empty()
                {
                    *reasons.entry(reason.to_string()).or_default() += 1;
                }
            }
            "error" => stats.errored += 1,
            _ => {}
        }
    }
    let mut top: Vec<ReasonCount> = reasons
        .into_iter()
        .map(|(reason, count)| ReasonCount { reason, count })
        .collect();
    top.sort_by_key(|rc| std::cmp::Reverse(rc.count));
    top.truncate(8);
    stats.top_passthrough = top;
    stats
}

/// Scan kache's wrapper-mode log file for `PathNormalizer` leak detector
/// firings — it `warn!`s, naming the value, when an absolute path
/// survives normalization into a cache key. Returns the count and a few
/// distinct sample lines.
///
/// Reads `wrapper-<phase>.log` (written by kache via `KACHE_LOG_FILE` +
/// `KACHE_LOG_FILE_PATH`), not the mach build log. Wrapper stderr is
/// eaten by cargo, so the file path is the only reliable signal.
fn scan_leak_warnings(log_path: &Path) -> (u64, Vec<String>) {
    const MARKER: &str = "residual absolute path detected";
    let Ok(file) = File::open(log_path) else {
        return (0, Vec::new());
    };
    let mut count = 0u64;
    let mut samples: Vec<String> = Vec::new();
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        if line.contains(MARKER) {
            count += 1;
            let trimmed = line.trim().to_string();
            if samples.len() < 5 && !samples.contains(&trimmed) {
                samples.push(trimmed);
            }
        }
    }
    (count, samples)
}

/// Parse a wrapper-`<phase>`.log written with
/// `KACHE_LOG_FILE=kache::cache_key=trace` and extract every per-crate
/// key-input the hasher consumed. Lines look like
///
///     2026-05-24T16:45:23.123Z TRACE kache::cache_key: [key:gkrust] env_dep:CARGO_MANIFEST_DIR=/abs/path
///
/// Returns a map of `crate_name -> ordered list of input payloads`
/// (everything after the `[key:CRATE]` marker). Same crate may appear
/// multiple times if cargo invokes it under several profiles; the
/// downstream diff treats each phase's payloads as a set, so multi-
/// invocation crates compare cleanly as long as the set matches across
/// clones.
fn parse_key_trace(log_path: &Path) -> HashMap<String, Vec<String>> {
    let mut by_crate: HashMap<String, Vec<String>> = HashMap::new();
    let Ok(file) = File::open(log_path) else {
        return by_crate;
    };
    for line in std::io::BufReader::new(file).lines().map_while(Result::ok) {
        if let Some((crate_name, payload)) = parse_key_line(&line) {
            by_crate.entry(crate_name).or_default().push(payload);
        }
    }
    by_crate
}

fn parse_key_line(line: &str) -> Option<(String, String)> {
    let idx = line.find("[key:")?;
    let after = &line[idx + "[key:".len()..];
    let end = after.find(']')?;
    let crate_name = after[..end].to_string();
    let payload = after[end + 1..].trim().to_string();
    if payload.is_empty() {
        return None;
    }
    Some((crate_name, normalize_payload(&payload)))
}

/// Strip display-only path noise out of a trace payload so cross-clone
/// set-diffing reflects real key-input divergence — not chatty trace
/// formatting.
///
/// `cache_key.rs` emits `source:PATH=hash` for human readability, but
/// the hasher only consumes the content hash. Two clones at different
/// absolute paths that share identical source content emit different
/// trace lines for the same key input. Without normalization the diff
/// counts them as diverging when the cache key is actually stable —
/// observed as 475 false positives in the first trace bench (the bug
/// that made `final` appear as the top diverging field with only 77
/// crates, despite the diff helper claiming 552 diverging crates).
///
/// Normalize to `source:hash` so the comparison reflects what the
/// hasher actually consumes.
fn normalize_payload(payload: &str) -> String {
    if let Some(rest) = payload.strip_prefix("source:")
        && let Some(eq) = rest.rfind('=')
    {
        return format!("source:{}", &rest[eq + 1..]);
    }
    payload.to_string()
}

/// Strip a key-input payload down to its "field name" — the part
/// before the first `=`. Used to bucket diverging payloads so the
/// report can say "12 crates differ on `extern:mozbuild`" rather than
/// dumping individual lines.
fn field_of(payload: &str) -> String {
    let eq = payload.find('=').unwrap_or(payload.len());
    payload[..eq].to_string()
}

#[derive(Debug, Serialize)]
struct KeyFieldAggregate {
    field: String,
    /// Distinct crates in which this field had divergent payloads.
    crates: u64,
    /// Up to 5 sample payloads from cold that had no warm counterpart.
    cold_unique_samples: Vec<String>,
    /// Up to 5 sample payloads from warm that had no cold counterpart.
    warm_unique_samples: Vec<String>,
}

#[derive(Debug, Serialize)]
struct KeyCrateDiff {
    crate_name: String,
    only_in_cold: Vec<String>,
    only_in_warm: Vec<String>,
}

#[derive(Debug, Serialize)]
struct KeyDivergence {
    diverging_crates: u64,
    diverging_fields: u64,
    aggregate_by_field: Vec<KeyFieldAggregate>,
    by_crate: Vec<KeyCrateDiff>,
}

/// Compute the cross-phase key-input divergence: for every crate that
/// appears in both phases, set-diff the cold inputs against the warm
/// inputs and bucket the differences by field name.
///
/// Crates that appear in only one phase are skipped — they don't
/// answer the "same crate, different key" question we're after; the
/// asymmetry is its own (separate) signal.
fn compute_key_divergence(
    cold: &HashMap<String, Vec<String>>,
    warm: &HashMap<String, Vec<String>>,
) -> KeyDivergence {
    let mut by_crate: Vec<KeyCrateDiff> = Vec::new();
    // BTreeMap so the field iteration order is deterministic.
    let mut by_field: BTreeMap<String, (u64, BTreeSet<String>, BTreeSet<String>)> = BTreeMap::new();

    for name in cold.keys() {
        let Some(warm_lines) = warm.get(name) else {
            continue;
        };
        let cold_set: BTreeSet<&str> = cold[name].iter().map(String::as_str).collect();
        let warm_set: BTreeSet<&str> = warm_lines.iter().map(String::as_str).collect();
        if cold_set == warm_set {
            continue;
        }

        let only_cold: Vec<String> = cold_set
            .difference(&warm_set)
            .map(|s| s.to_string())
            .collect();
        let only_warm: Vec<String> = warm_set
            .difference(&cold_set)
            .map(|s| s.to_string())
            .collect();

        // Per-crate distinct fields — one count per crate per field even
        // if a single crate has many diverging payloads under the same
        // field. (Counting payloads inflates the headline number.)
        let mut crate_fields: BTreeSet<String> = BTreeSet::new();
        for p in only_cold.iter().chain(only_warm.iter()) {
            crate_fields.insert(field_of(p));
        }
        for f in &crate_fields {
            let entry = by_field
                .entry(f.clone())
                .or_insert_with(|| (0, BTreeSet::new(), BTreeSet::new()));
            entry.0 += 1;
        }
        // Sample payloads per field (capped at 5 each, to keep the
        // report small but legible).
        for p in &only_cold {
            let entry = by_field
                .entry(field_of(p))
                .or_insert_with(|| (0, BTreeSet::new(), BTreeSet::new()));
            if entry.1.len() < 5 {
                entry.1.insert(p.clone());
            }
        }
        for p in &only_warm {
            let entry = by_field
                .entry(field_of(p))
                .or_insert_with(|| (0, BTreeSet::new(), BTreeSet::new()));
            if entry.2.len() < 5 {
                entry.2.insert(p.clone());
            }
        }

        by_crate.push(KeyCrateDiff {
            crate_name: name.clone(),
            only_in_cold: only_cold,
            only_in_warm: only_warm,
        });
    }

    let diverging_fields = by_field.len() as u64;
    let mut aggregate: Vec<KeyFieldAggregate> = by_field
        .into_iter()
        .map(|(field, (crates, cold_s, warm_s))| KeyFieldAggregate {
            field,
            crates,
            cold_unique_samples: cold_s.into_iter().collect(),
            warm_unique_samples: warm_s.into_iter().collect(),
        })
        .collect();
    aggregate.sort_by_key(|a| std::cmp::Reverse(a.crates));

    KeyDivergence {
        diverging_crates: by_crate.len() as u64,
        diverging_fields,
        aggregate_by_field: aggregate,
        by_crate,
    }
}

/// Write `key-diff.json` (full structured) and `key-diff.md` (top-N
/// summary) next to the other bench artifacts. Returns the top field
/// (most crates) so the run summary can name it inline.
fn write_key_diff_reports(diff: &KeyDivergence, work_dir: &Path) -> Result<Option<String>> {
    let json_path = work_dir.join("key-diff.json");
    let md_path = work_dir.join("key-diff.md");
    std::fs::write(&json_path, serde_json::to_string_pretty(&diff)? + "\n")
        .with_context(|| format!("writing {}", json_path.display()))?;

    let mut md = String::new();
    md.push_str("# kache key-diff — what diverged across cold/warm clones\n\n");
    md.push_str(&format!(
        "- diverging crates: **{}**\n",
        diff.diverging_crates
    ));
    md.push_str(&format!(
        "- diverging input fields: **{}**\n\n",
        diff.diverging_fields
    ));
    md.push_str("## Top diverging fields\n\n");
    for (i, agg) in diff.aggregate_by_field.iter().take(15).enumerate() {
        md.push_str(&format!(
            "### {}. `{}` — {} crate(s)\n\n",
            i + 1,
            agg.field,
            agg.crates
        ));
        if !agg.cold_unique_samples.is_empty() {
            md.push_str("Cold samples:\n");
            for p in agg.cold_unique_samples.iter().take(3) {
                md.push_str(&format!(
                    "- `{}`\n",
                    p.chars().take(160).collect::<String>()
                ));
            }
        }
        if !agg.warm_unique_samples.is_empty() {
            md.push_str("Warm samples:\n");
            for p in agg.warm_unique_samples.iter().take(3) {
                md.push_str(&format!(
                    "- `{}`\n",
                    p.chars().take(160).collect::<String>()
                ));
            }
        }
        md.push('\n');
    }
    std::fs::write(&md_path, md).with_context(|| format!("writing {}", md_path.display()))?;
    Ok(diff
        .aggregate_by_field
        .first()
        .map(|a| format!("{} ({} crates)", a.field, a.crates)))
}

/// Cross-clone cache-key stability: of the crates kache tried to cache in
/// *both* clones, the fraction that produced an identical key set. 100%
/// means the key is path-portable; a near-zero rate is a path leak.
///
/// Deterministic and device-independent — the headline correctness
/// signal, where wall-clock can't be.
fn key_stability(cold_raw: &serde_json::Value, warm_raw: &serde_json::Value) -> KeyStability {
    fn crate_keys(raw: &serde_json::Value) -> HashMap<String, HashSet<String>> {
        let mut map: HashMap<String, HashSet<String>> = HashMap::new();
        if let Some(events) = raw["all_events"].as_array() {
            for ev in events {
                if let (Some(name), Some(key)) =
                    (ev["crate_name"].as_str(), ev["cache_key"].as_str())
                {
                    map.entry(name.to_string())
                        .or_default()
                        .insert(key.to_string());
                }
            }
        }
        map
    }

    let cold = crate_keys(cold_raw);
    let warm = crate_keys(warm_raw);
    let mut compared = 0u64;
    let mut stable = 0u64;
    for (name, warm_keys) in &warm {
        if let Some(cold_keys) = cold.get(name) {
            compared += 1;
            // Stable: every key warm produced was one cold also produced
            // (so a warm lookup could have hit).
            if warm_keys.is_subset(cold_keys) {
                stable += 1;
            }
        }
    }
    let pct = if compared == 0 {
        0.0
    } else {
        stable as f64 / compared as f64 * 100.0
    };
    KeyStability {
        stable_pct: round1(pct),
        stable,
        compared,
    }
}

/// Best-effort `kache daemon stop` so it releases the cache dir's locks.
fn daemon_stop(kache: &Path, cache_dir: &Path) {
    let _ = Command::new(kache)
        .args(["daemon", "stop"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Run a command with inherited stdio, failing on a non-zero exit.
fn run(cmd: &mut Command) -> Result<()> {
    let status = cmd.status().with_context(|| format!("spawning {cmd:?}"))?;
    if !status.success() {
        bail!("command failed ({status}): {cmd:?}");
    }
    Ok(())
}

/// Apparent size of `dir` in KiB via `du -sk`. Returns 0 if `du` is
/// Apparent size of `dir` in KiB — the sum of file lengths, recursive, never
/// following symlinks. Returns 0 on any error (a reported metric, not
/// load-bearing). Cross-platform (no `du`).
fn dir_size_kb(dir: &Path) -> u64 {
    fn walk(dir: &Path, acc: &mut u64) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            let Ok(md) = path.symlink_metadata() else {
                continue;
            };
            if md.file_type().is_symlink() {
                continue;
            }
            if md.is_dir() {
                walk(&path, acc);
            } else {
                *acc += md.len();
            }
        }
    }
    let mut bytes = 0u64;
    walk(dir, &mut bytes);
    bytes / 1024
}

/// Snapshot `src` → `dst` so a later `--retry` can restore the post-cold
/// cache. Unix fast path: CoW reflink via `cp` (APFS clonefile / btrfs/XFS
/// FICLONE) — matters for the multi-GB cache. Portable fallback (and the sole
/// Windows path): a plain recursive copy.
fn snapshot_dir(src: &Path, dst: &Path) -> Result<()> {
    if dst.exists() {
        let _ = std::fs::remove_dir_all(dst);
    }
    #[cfg(unix)]
    {
        let try_cp = |args: &[&str]| -> bool {
            if dst.exists() {
                let _ = std::fs::remove_dir_all(dst);
            }
            Command::new("cp")
                .args(args)
                .arg(src)
                .arg(dst)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        };
        // BSD cp (macOS APFS) → clonefile. GNU cp (Linux btrfs / XFS) →
        // FICLONE. Plain `-R` is the slow-but-portable fallback.
        if try_cp(&["-cR"]) || try_cp(&["-R", "--reflink=auto"]) || try_cp(&["-R"]) {
            return Ok(());
        }
        // A failed cp may have left a partial tree; clear it first.
        if dst.exists() {
            let _ = std::fs::remove_dir_all(dst);
        }
    }
    copy_dir_recursive(src, dst)
        .with_context(|| format!("snapshotting {} -> {}", src.display(), dst.display()))
}

/// Recursively copy `src` into `dst` (creating `dst`) with plain byte copies —
/// no reflink. Cross-platform.
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&from, &to)?;
        } else {
            std::fs::copy(&from, &to)?;
        }
    }
    Ok(())
}

/// Deserialize a JSON file into `T`.
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let s = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&s).with_context(|| format!("parsing JSON from {}", path.display()))
}

fn round1(v: f64) -> f64 {
    (v * 10.0).round() / 10.0
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

/// Human-readable byte size — GB / MB / KB / B.
fn human_bytes(b: u64) -> String {
    let v = b as f64;
    if v >= 1e9 {
        format!("{:.1} GB", v / 1e9)
    } else if v >= 1e6 {
        format!("{:.1} MB", v / 1e6)
    } else if v >= 1e3 {
        format!("{:.1} KB", v / 1e3)
    } else {
        format!("{b} B")
    }
}

fn print_summary(r: &BenchResult, work_dir: &Path) {
    let bar = "=".repeat(64);
    let fmt = |s: u64| format!("{}m {:02}s", s / 60, s % 60);
    eprintln!("\n{bar}");
    eprintln!("  kache benchmark: {} — {}", r.project, r.git_ref);
    eprintln!("{bar}");
    eprintln!(
        "  cold build : {}   (empty cache, baseline)",
        fmt(r.cold.wall_s)
    );
    eprintln!(
        "  warm build : {}   (cache populated by cold)",
        fmt(r.warm.wall_s)
    );
    eprintln!("  speedup    : {:.2}x", r.speedup);
    eprintln!(
        "  warm cache : {} hits / {} misses   {:.1}% hit rate ({:.1}% weighted)",
        r.warm.hits, r.warm.misses, r.warm.hit_rate_pct, r.warm.weighted_hit_rate_pct
    );
    let el = &r.warm.event_log;
    let cacheable = el.hit_bytes.saturating_add(el.miss_bytes);
    let bytes_cov_pct = if cacheable > 0 {
        (el.hit_bytes as f64 / cacheable as f64) * 100.0
    } else {
        0.0
    };
    eprintln!(
        "  bytes      : {} cached / {} recompiled   ({:.1}% of cacheable bytes)",
        human_bytes(el.hit_bytes),
        human_bytes(el.miss_bytes),
        bytes_cov_pct,
    );
    eprintln!("{bar}");

    // ── restore: bytes that flowed cache → warm clone, and how ──
    let st = &r.warm.storage;
    eprintln!(
        "  restore    : warm build pulled {} from the cache",
        human_bytes(st.restored_bytes)
    );
    eprintln!(
        "               reflink {} / hardlink {} / copy {}   ({:.1}% zero-copy)",
        human_bytes(st.reflinked_bytes),
        human_bytes(st.hardlinked_bytes),
        human_bytes(st.copied_bytes),
        st.zero_copy_pct,
    );
    eprintln!("{bar}");

    // ── disk layout: the three pools and what sharing buys ──
    // Apparent = sum of file lengths (matches `du --apparent-size`). On APFS,
    // bytes that kache reflinked from the cache into the warm clone
    // appear in both inodes' apparent sizes but only occupy disk once.
    // Subtracting `warm reflinked_bytes` from the apparent sum gives a
    // conservative estimate of what's actually on disk — conservative
    // because the cold-phase store path also CoW-shares blocks via
    // `fs::copy → clonefile` on APFS, but kache doesn't tally those.
    let sum_apparent = r
        .cold_objdir_bytes
        .saturating_add(r.warm_objdir_bytes)
        .saturating_add(st.blob_bytes);
    let approx_on_disk = sum_apparent.saturating_sub(st.reflinked_bytes);
    eprintln!("  disk layout — three pools, sharing blocks via CoW reflinks");
    eprintln!(
        "    clone-a/obj   {:>10}   cold-built objdir",
        human_bytes(r.cold_objdir_bytes)
    );
    eprintln!(
        "    clone-b/obj   {:>10}   warm-built; {} reflinked from cache",
        human_bytes(r.warm_objdir_bytes),
        human_bytes(st.reflinked_bytes),
    );
    eprintln!(
        "    kache cache   {:>10}   {} blobs, {} dedup vs {} raw",
        human_bytes(st.blob_bytes),
        st.store_blobs,
        human_bytes(st.dedup_saved_bytes),
        human_bytes(st.logical_bytes),
    );
    eprintln!("                  ──────────");
    eprintln!(
        "    sum apparent  {:>10}   what 3 independent pools would cost",
        human_bytes(sum_apparent),
    );
    eprintln!(
        "    ≈ on disk    ~{:>10}   warm-restore CoW saves {} on APFS",
        human_bytes(approx_on_disk),
        human_bytes(st.reflinked_bytes),
    );
    eprintln!("{bar}");

    // ── diagnostics: did the run actually exercise kache? ──
    eprintln!(
        "  key stability : {:.1}%   ({} of {} crates kept an identical key across clones)",
        r.key_stability.stable_pct, r.key_stability.stable, r.key_stability.compared
    );
    if let Some(top) = &r.key_diff_top {
        eprintln!(
            "  key diff      : top diverging input → {}   (see key-diff.{{json,md}})",
            top
        );
    }
    eprintln!(
        "  kache saw     : {} compiles -> {} cached, {} passed through, {} errored",
        el.total, el.cached, el.passed_through, el.errored
    );
    if !el.top_passthrough.is_empty() {
        eprintln!("  passthrough   : top reasons —");
        for rc in el.top_passthrough.iter().take(5) {
            let reason: String = rc.reason.chars().take(72).collect();
            eprintln!("    {:>6}x  {}", rc.count, reason);
        }
    }
    eprintln!(
        "  leak detector : {} firing(s) (kache PathNormalizer; see wrapper-warm.log)",
        r.warm.leak_warnings
    );
    for sample in r.warm_leak_samples.iter().take(3) {
        let s: String = sample.chars().take(96).collect();
        eprintln!("    {s}");
    }

    if !r.warm.top_misses.is_empty() {
        eprintln!("{bar}");
        eprintln!("  costliest warm misses (recompiled despite the cache):");
        for m in r.warm.top_misses.iter().take(5) {
            eprintln!("    {:>8}  {}", fmt(m.compile_time_s), m.crate_name);
        }
    }

    eprintln!("{bar}");
    if r.verdict.ok {
        eprintln!("  VERDICT: ok — the run validly exercised kache.");
    } else {
        eprintln!("  VERDICT: DEGRADED RUN — results are NOT a valid measurement:");
        for issue in &r.verdict.issues {
            eprintln!("    - {issue}");
        }
    }
    eprintln!("{bar}");
    eprintln!(
        "  detailed reports: {}/{{report,build,wrapper}}-{{cold,warm}}.*",
        work_dir.display()
    );
    eprintln!("{bar}");
}

/// Benchmark result document written to `<work-dir>/<project>.json`.
#[derive(Debug, Serialize)]
struct BenchResult {
    /// Profile name, e.g. `firefox` / `substrate`.
    project: String,
    /// The pinned ref (tag/branch/commit) that was built.
    git_ref: String,
    /// `Os-Arch`, e.g. `macos-aarch64`.
    platform: String,
    cold: PhaseMetrics,
    warm: PhaseMetrics,
    /// Cold wall-clock divided by warm wall-clock.
    speedup: f64,
    cache_size_mb: f64,
    /// Apparent bytes (`du`) of clone-a's objdir after the cold build.
    cold_objdir_bytes: u64,
    /// Apparent bytes (`du`) of clone-b's objdir after the warm build.
    /// Of these, ~`warm.storage.reflinked_bytes` are CoW-shared with the
    /// cache on APFS — print_summary subtracts that to report "unique
    /// to this clone".
    warm_objdir_bytes: u64,
    /// Cross-clone cache-key stability — the deterministic correctness
    /// signal.
    key_stability: KeyStability,
    /// Sample lines from kache's leak detector during the warm build.
    warm_leak_samples: Vec<String>,
    /// Whether the run validly exercised kache (drives the exit code).
    verdict: Verdict,
    /// Top diverging key-input field across clones, set only when the
    /// run was invoked with `--trace-keys`. The full per-crate diff
    /// lives in `key-diff.json` / `key-diff.md`; this is the headline
    /// for the run summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    key_diff_top: Option<String>,
    /// Detailed per-phase artifacts written next to this file.
    reports: Vec<String>,
}

/// Per-phase metrics. Each phase's report is isolated (the event log is
/// reset between cold and warm), so these numbers reflect that phase
/// alone — no cumulative bleed.
#[derive(Debug, Serialize, Deserialize)]
struct PhaseMetrics {
    wall_s: u64,
    total_crates: u64,
    hits: u64,
    misses: u64,
    /// Cache errors recorded this phase (e.g. store/restore failures).
    errors: u64,
    /// Hit rate by count.
    hit_rate_pct: f64,
    /// Hit rate weighted by compile cost — tracks real time saved.
    weighted_hit_rate_pct: f64,
    /// Compile time avoided by cache hits, in seconds.
    time_saved_s: u64,
    /// Costliest cache misses this phase (kache's `top_misses`).
    top_misses: Vec<MissEntry>,
    /// Raw event-log accounting — includes the passthrough events the
    /// `kache report` summary hides.
    event_log: EventLogStats,
    /// kache leak-detector firings captured during this phase's build.
    leak_warnings: u64,
    /// Storage savings — restore method (reflink/hardlink/copy) and
    /// content dedup, from the report's `storage` section.
    storage: StorageInfo,
}

impl PhaseMetrics {
    fn from_report(
        report: &report::KacheReport,
        raw: &serde_json::Value,
        wall_s: u64,
        event_log: EventLogStats,
        leak_warnings: u64,
    ) -> Self {
        let s = &report.summary;
        let sum = &raw["summary"];
        let top_misses = raw["top_misses"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|m| {
                        Some(MissEntry {
                            crate_name: m["crate_name"].as_str()?.to_string(),
                            compile_time_s: m["compile_time_ms"].as_u64().unwrap_or(0) / 1000,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        PhaseMetrics {
            wall_s,
            total_crates: s.total_crates,
            hits: s.total_hits(),
            misses: s.misses,
            errors: sum["errors"].as_u64().unwrap_or(0),
            hit_rate_pct: round1(s.hit_rate_pct),
            weighted_hit_rate_pct: round1(sum["weighted_hit_rate_pct"].as_f64().unwrap_or(0.0)),
            time_saved_s: sum["time_saved_ms"].as_u64().unwrap_or(0) / 1000,
            top_misses,
            event_log,
            leak_warnings,
            storage: StorageInfo::from_raw(raw),
        }
    }
}

/// One expensive cache miss, surfaced from kache's `top_misses`.
#[derive(Debug, Serialize, Deserialize)]
struct MissEntry {
    crate_name: String,
    compile_time_s: u64,
}

/// kache's storage-savings breakdown for one phase — surfaced from the
/// report's `storage` section. Restore side: bytes brought back by CoW
/// reflink / hardlink / plain copy. Store side: content-addressed dedup.
#[derive(Debug, Serialize, Deserialize)]
struct StorageInfo {
    reflinked_bytes: u64,
    hardlinked_bytes: u64,
    copied_bytes: u64,
    restored_bytes: u64,
    /// Share of restored bytes that cost no physical copy.
    zero_copy_pct: f64,
    /// Unique content-addressed blobs in the store.
    store_blobs: u64,
    /// Sum of cache-entry sizes — what the store would be without dedup.
    logical_bytes: u64,
    /// Physical bytes the unique blobs occupy.
    blob_bytes: u64,
    /// Bytes saved by content-addressed dedup (`logical - blob`).
    dedup_saved_bytes: u64,
}

impl StorageInfo {
    /// Pull the `storage` section out of a raw `kache report`. Missing
    /// fields default to zero, so a report from an older kache without
    /// the section degrades cleanly.
    fn from_raw(raw: &serde_json::Value) -> Self {
        let st = &raw["storage"];
        let u = |key: &str| st[key].as_u64().unwrap_or(0);
        StorageInfo {
            reflinked_bytes: u("reflinked_bytes"),
            hardlinked_bytes: u("hardlinked_bytes"),
            copied_bytes: u("copied_bytes"),
            restored_bytes: u("restored_bytes"),
            zero_copy_pct: st["zero_copy_pct"].as_f64().unwrap_or(0.0),
            store_blobs: u("store_blobs"),
            logical_bytes: u("logical_bytes"),
            blob_bytes: u("blob_bytes"),
            dedup_saved_bytes: u("dedup_saved_bytes"),
        }
    }
}

/// Raw event-log accounting for one phase — the passthrough wall the
/// `kache report` summary hides.
#[derive(Debug, Serialize, Deserialize, Default)]
struct EventLogStats {
    /// Every event in the log (hits, misses, passthroughs, errors, …).
    total: u64,
    /// Events kache attempted to cache (hits + misses).
    cached: u64,
    /// Compiles kache declined to cache and ran straight through.
    passed_through: u64,
    /// Events that recorded a cache error.
    errored: u64,
    /// Most frequent passthrough reasons, most frequent first.
    top_passthrough: Vec<ReasonCount>,
    /// Bytes of artifact data restored from cache (sum of `size`
    /// over `*_hit` events). The headline numerator of "what
    /// fraction of my build's artifact bytes came from cache".
    hit_bytes: u64,
    /// Bytes of artifact data produced by recompiles (sum of `size`
    /// over `miss` events). The "+ recompiled" partner of `hit_bytes`;
    /// together they form the cacheable-bytes denominator that lets
    /// the summary report a bytes-version of the weighted hit rate.
    miss_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReasonCount {
    reason: String,
    count: u64,
}

/// Cross-clone cache-key stability.
#[derive(Debug, Serialize)]
struct KeyStability {
    /// Percentage of compared crates that produced an identical key set.
    stable_pct: f64,
    /// Crates whose key was identical across the two clones.
    stable: u64,
    /// Crates kache cached in both clones (the comparison denominator).
    compared: u64,
}

/// Whether the run validly exercised kache. A degraded run exits non-zero
/// so a broken run can't pass as a real measurement.
#[derive(Debug, Serialize)]
struct Verdict {
    ok: bool,
    issues: Vec<String>,
}

impl Verdict {
    /// Gate thresholds. A run is degraded if cross-clone key stability
    /// collapsed (the warm phase measured nothing real), if most compiles
    /// never reached the cache, or if kache reported errors.
    fn evaluate(stability: &KeyStability, warm: &PhaseMetrics) -> Self {
        let mut issues = Vec::new();

        if stability.compared > 0 && stability.stable_pct < 50.0 {
            issues.push(format!(
                "cross-clone key stability {:.1}% — the cache key is not \
                 path-portable; the warm phase did not validly measure caching",
                stability.stable_pct
            ));
        }

        let el = &warm.event_log;
        if el.total > 0 {
            let pt_pct = el.passed_through as f64 / el.total as f64 * 100.0;
            if pt_pct > 40.0 {
                issues.push(format!(
                    "{:.0}% of compiles ({} of {}) passed through uncached — \
                     kache barely exercised",
                    pt_pct, el.passed_through, el.total
                ));
            }
        }

        if warm.errors > 0 {
            issues.push(format!(
                "{} cache error(s) during the warm build",
                warm.errors
            ));
        }

        Verdict {
            ok: issues.is_empty(),
            issues,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_binary_prefers_existing_then_exe() {
        let dir = std::env::temp_dir().join(format!("kb-resolvebin-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Only `foo.exe` exists → resolve_binary(dir/foo) returns dir/foo.exe.
        std::fs::write(dir.join("foo.exe"), b"x").unwrap();
        assert_eq!(resolve_binary(&dir.join("foo")), dir.join("foo.exe"));
        // A path that exists as-is is returned unchanged.
        std::fs::write(dir.join("bar"), b"x").unwrap();
        assert_eq!(resolve_binary(&dir.join("bar")), dir.join("bar"));
        // Nothing exists → returned unchanged (so the caller's error names it).
        assert_eq!(resolve_binary(&dir.join("nope")), dir.join("nope"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn posix_sh_resolves_a_shell() {
        let sh = posix_sh().expect("a POSIX sh should be resolvable on the test host");
        let name = sh.file_name().unwrap().to_string_lossy().to_lowercase();
        // Unix: `sh`; Windows CI: `sh.exe`.
        assert!(name.starts_with("sh"), "unexpected shell: {}", sh.display());
    }

    #[test]
    fn de_verbatim_strips_windows_extended_prefix() {
        // Plain paths are untouched on every platform.
        assert_eq!(
            de_verbatim(PathBuf::from("/tmp/x")),
            PathBuf::from("/tmp/x")
        );
        #[cfg(windows)]
        {
            assert_eq!(
                de_verbatim(PathBuf::from(r"\\?\C:\a\b")),
                PathBuf::from(r"C:\a\b")
            );
            assert_eq!(
                de_verbatim(PathBuf::from(r"\\?\UNC\srv\share")),
                PathBuf::from(r"\\srv\share")
            );
        }
    }

    #[test]
    fn parse_key_line_extracts_crate_and_payload_from_default_tracing_format() {
        let line = "2026-05-24T16:45:23.123Z TRACE kache::cache_key: \
                    [key:gkrust] env_dep:CARGO_MANIFEST_DIR=/abs/path";
        let parsed = parse_key_line(line);
        assert_eq!(
            parsed,
            Some((
                "gkrust".to_string(),
                "env_dep:CARGO_MANIFEST_DIR=/abs/path".to_string()
            ))
        );
    }

    #[test]
    fn parse_key_line_returns_none_for_non_key_lines() {
        for line in [
            "",
            "2026-05-24T16:45:23.123Z INFO kache: starting build",
            "WARN kache::path_normalizer: residual absolute path detected",
            "[key:gkrust]",  // payload empty
            "[key:gkrust] ", // payload whitespace only
        ] {
            assert!(
                parse_key_line(line).is_none(),
                "line should not parse: {line:?}"
            );
        }
    }

    #[test]
    fn normalize_payload_strips_display_only_source_path() {
        // `source:PATH=hash` is display-only path noise; the hasher
        // consumes only the right-hand content hash. Normalizing to
        // `source:hash` makes cross-clone set-diffing reflect what's
        // actually in the cache key.
        assert_eq!(
            normalize_payload("source:/Users/a/clone-a/foo.rs=254dfb8084fc2e3f"),
            "source:254dfb8084fc2e3f"
        );
        assert_eq!(
            normalize_payload("source:/Users/b/clone-b/foo.rs=254dfb8084fc2e3f"),
            "source:254dfb8084fc2e3f",
        );
        // Sanity: two clone paths with identical content collapse to
        // the same normalized payload.
        assert_eq!(
            normalize_payload("source:/clone-a/x.rs=H"),
            normalize_payload("source:/clone-b/x.rs=H"),
        );
        // Non-source payloads pass through untouched.
        assert_eq!(
            normalize_payload("env_dep:CARGO_MANIFEST_DIR=/p"),
            "env_dep:CARGO_MANIFEST_DIR=/p"
        );
        assert_eq!(normalize_payload("final=abcdef"), "final=abcdef");
    }

    #[test]
    fn field_of_buckets_payloads_by_prefix() {
        assert_eq!(
            field_of("env_dep:CARGO_MANIFEST_DIR=/x"),
            "env_dep:CARGO_MANIFEST_DIR"
        );
        assert_eq!(field_of("codegen:opt-level=3"), "codegen:opt-level");
        assert_eq!(field_of("RUSTFLAGS=-C debuginfo=2"), "RUSTFLAGS");
        assert_eq!(field_of("final=abcdef"), "final");
        assert_eq!(field_of("feature:foo"), "feature:foo"); // no '=' → whole payload
    }

    #[test]
    fn clone_ref_path_is_a_sibling_not_a_child_of_work_dir() {
        // The reference must live OUTSIDE work_dir so that the natural
        // `rm -rf <work_dir>` wipe (what someone reaches for to reset
        // the bench) doesn't accidentally torch the network clone.
        assert_eq!(
            clone_ref_path(Path::new("./tmp/bench")),
            PathBuf::from("./tmp/bench-clone-ref")
        );
        assert_eq!(
            clone_ref_path(Path::new("/scratch/foo")),
            PathBuf::from("/scratch/foo-clone-ref")
        );
        // Sanity: the reference path is not nested inside work_dir.
        let work = Path::new("./tmp/bench");
        let r = clone_ref_path(work);
        assert!(
            !r.starts_with(work),
            "clone-ref must not live under work_dir, got {r:?}"
        );
    }

    #[test]
    fn default_work_dir_is_per_profile_under_tmp_bench() {
        assert_eq!(
            default_work_dir("substrate"),
            PathBuf::from("./tmp/bench/substrate")
        );
        assert_eq!(
            default_work_dir("firefox"),
            PathBuf::from("./tmp/bench/firefox")
        );
        // clone-ref stays a sibling WITHIN ./tmp/bench (not under work_dir), so
        // `rm -rf ./tmp/bench/<profile>` spares the clone and `rm -rf tmp/bench`
        // wipes every profile at once.
        let wd = default_work_dir("substrate");
        assert_eq!(
            clone_ref_path(&wd),
            PathBuf::from("./tmp/bench/substrate-clone-ref")
        );
        assert!(!clone_ref_path(&wd).starts_with(&wd));
    }

    #[test]
    fn work_dir_lock_is_exclusive() {
        let dir = std::env::temp_dir().join(format!("kache-bench-locktest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // Keep the first guard BOUND for the duration of the second attempt —
        // if it dropped first, the lock would release and the second succeed
        // (false pass).
        let first = acquire_work_dir_lock(&dir).expect("first lock should acquire");
        let second = acquire_work_dir_lock(&dir);
        assert!(
            second.is_err(),
            "second lock on the same work_dir must be refused"
        );
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compute_key_divergence_buckets_diffs_by_field() {
        let mut cold: HashMap<String, Vec<String>> = HashMap::new();
        let mut warm: HashMap<String, Vec<String>> = HashMap::new();
        cold.insert(
            "gkrust".into(),
            vec![
                "env_dep:CARGO_MANIFEST_DIR=/clone-a/path".into(),
                "codegen:opt-level=3".into(),
                "final=AAAA".into(),
            ],
        );
        warm.insert(
            "gkrust".into(),
            vec![
                "env_dep:CARGO_MANIFEST_DIR=/clone-b/path".into(),
                "codegen:opt-level=3".into(),
                "final=BBBB".into(),
            ],
        );
        // Stable cross-clone: same key both times → must not appear in diff.
        cold.insert("stable".into(), vec!["final=XXXX".into()]);
        warm.insert("stable".into(), vec!["final=XXXX".into()]);

        let diff = compute_key_divergence(&cold, &warm);
        assert_eq!(diff.diverging_crates, 1, "stable crate must not appear");
        // Two fields diverge for gkrust: env_dep:CARGO_MANIFEST_DIR and final.
        assert_eq!(diff.aggregate_by_field.len(), 2);
        let fields: Vec<&str> = diff
            .aggregate_by_field
            .iter()
            .map(|a| a.field.as_str())
            .collect();
        assert!(fields.contains(&"env_dep:CARGO_MANIFEST_DIR"));
        assert!(fields.contains(&"final"));
    }

    #[test]
    fn dir_size_kb_sums_file_bytes_recursively() {
        let dir = std::env::temp_dir().join(format!("kb-dirsize-{}", std::process::id()));
        let sub = dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(dir.join("a.bin"), vec![0u8; 1024]).unwrap(); // 1 KiB
        std::fs::write(sub.join("b.bin"), vec![0u8; 2048]).unwrap(); // 2 KiB
        assert_eq!(dir_size_kb(&dir), 3); // (1024 + 2048) / 1024
        // Missing dir → 0 (graceful).
        assert_eq!(dir_size_kb(&dir.join("does-not-exist")), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn copy_dir_recursive_round_trips_a_tree() {
        let base = std::env::temp_dir().join(format!("kb-copytree-{}", std::process::id()));
        let src = base.join("src");
        let dst = base.join("dst");
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::write(src.join("top.txt"), b"top").unwrap();
        std::fs::write(src.join("nested").join("deep.txt"), b"deep").unwrap();
        copy_dir_recursive(&src, &dst).unwrap();
        assert_eq!(std::fs::read(dst.join("top.txt")).unwrap(), b"top");
        assert_eq!(
            std::fs::read(dst.join("nested").join("deep.txt")).unwrap(),
            b"deep"
        );
        let _ = std::fs::remove_dir_all(&base);
    }
}
