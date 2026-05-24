//! `kache-bench` — Firefox compile-cache benchmark.
//!
//! Builds Firefox twice against one shared kache cache:
//!
//! - **cold**: a fresh clone with an empty cache → every compile is a
//!   miss. This is the baseline.
//! - **warm**: a second, independent clone at a *different* absolute path
//!   against the same cache → compiles are served from what cold stored.
//!   The different path is deliberate: it mirrors a real fresh checkout
//!   (a new CI runner / a teammate's machine) and exposes absolute-path
//!   leaks in the cache key — a path-dependent key would miss everything.
//!
//! Reports cold/warm wall-clock, speedup, and hit rate. This is a manual
//! tool: a full run takes tens of minutes to a few hours and needs
//! ~50 GB of disk. It is intentionally NOT wired into CI.
//!
//! The benchmark is **self-diagnosing**: it captures kache's own leak
//! detector, measures cross-clone cache-key stability, accounts for the
//! passthrough events `kache report` hides, and fails the run (non-zero
//! exit) when those signals say the run did not validly exercise kache —
//! so a broken run can't masquerade as a tidy speedup.
//!
//! Reuses [`kache_e2e::report`] — the typed `kache report --format json`
//! fetch and parsing are shared with the e2e harness.
//!
//! NOTE: this currently benchmarks the `kache-alone` configuration. The
//! `sccache-alone` and `kache + sccache` (`KACHE_FALLBACK`) configs are
//! the next step.
//!
//! # Running the benchmark
//!
//! ```text
//! just bench-firefox             # full cold + warm (tens of minutes to hours)
//! just bench-firefox-retry       # restore cold-state snapshot, re-measure warm only (~25 min)
//! just bench-firefox --skip-clone   # reuse existing clones under tmp/bench
//! ```
//!
//! First-time setup requires Firefox build prerequisites — `./mach
//! bootstrap` runs automatically the first time and may prompt for
//! system packages. Subsequent runs reuse `~/.mozbuild`.
//!
//! # Reading the output
//!
//! Each run prints a summary block and writes `tmp/bench/firefox.json`
//! plus per-phase reports (`report-<phase>.{json,md}`), mach build logs
//! (`build-<phase>.log`), and kache wrapper logs (`wrapper-<phase>.log`).
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
//! The benchmark is pinned to a specific Firefox release tag (see the
//! `--tag` default). To bump:
//!
//! 1. Update the `tag` default in [`Args`].
//! 2. Re-run `just bench-firefox` end-to-end and confirm the verdict is
//!    `ok`. If mozbuild has changed flags (build script wrappers,
//!    `--with-compiler-wrapper` semantics, etc.), the mozconfig in
//!    [`write_mozconfig`] may need adjustment.
//! 3. The `OBJDIR` constant matches `MOZ_OBJDIR` in the generated
//!    mozconfig; keep them in sync.
//!
//! The benchmark is intentionally Firefox-specific: it exercises the
//! "large monorepo with bootstrapped toolchain + mixed Rust/C++ + LTO"
//! workload class that the e2e fixtures can't model.

use anyhow::{Context, Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use kache_e2e::report;

/// Per-clone Firefox object directory — matches `MOZ_OBJDIR` in the
/// generated mozconfig. Wiped before each phase so every build is
/// genuinely from-scratch, even on `--skip-clone` (where the reused
/// clone still carries its previous objdir).
const OBJDIR: &str = "obj-kache-bench";

#[derive(Debug, Parser)]
#[command(about = "Firefox compile-cache benchmark for kache.")]
struct Args {
    /// Firefox release tag to clone and build.
    #[arg(long, default_value = "FIREFOX_151_0_RELEASE")]
    tag: String,

    /// kache binary under test.
    #[arg(long, default_value = "./target/release/kache")]
    kache: PathBuf,

    /// Scratch directory for the clones, objdirs and cache (~50 GB).
    /// Lives under the repo's `tmp/` convention; gitignored. The whole
    /// directory tree (clone-a, clone-b, cache, snapshots) can be
    /// `rm -rf`ed at any time and a subsequent run rebuilds what it
    /// needs.
    #[arg(long, default_value = "./tmp/bench")]
    work_dir: PathBuf,

    /// Firefox git repository to clone from.
    #[arg(long, default_value = "https://github.com/mozilla-firefox/firefox.git")]
    repo: String,

    /// Reuse clones already present under the work dir.
    #[arg(long)]
    skip_clone: bool,

    /// Re-run `./mach bootstrap` even if `~/.mozbuild` already exists.
    #[arg(long)]
    force_bootstrap: bool,

    /// Skip the cold build: restore the cold-state cache snapshot saved
    /// by the previous full run and only re-measure the warm phase
    /// (~25 minutes saved). Requires a prior successful full run.
    #[arg(long)]
    retry: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let kache = args
        .kache
        .canonicalize()
        .with_context(|| format!("kache binary not found at {}", args.kache.display()))?;

    std::fs::create_dir_all(&args.work_dir)
        .with_context(|| format!("creating work dir {}", args.work_dir.display()))?;
    let work_dir = args.work_dir.canonicalize()?;

    let clone_a = work_dir.join("clone-a");
    let clone_b = work_dir.join("clone-b");
    let cache_dir = work_dir.join("cache");
    let kache_config = work_dir.join("kache-config.toml");
    let event_log = cache_dir.join("events.jsonl");

    eprintln!("=== kache Firefox benchmark ===");
    eprintln!("Firefox tag : {}", args.tag);
    eprintln!(
        "work dir    : {}  (~50 GB: 2 clones + objdirs + cache)",
        work_dir.display()
    );
    eprintln!("kache       : {}", kache.display());

    // kache rotates its event log at 10 MiB, keeping only the last 500
    // lines. A Firefox build emits tens of thousands of events, so the
    // default would discard nearly everything before the report is read.
    // A large cap keeps every event of a phase intact for its report.
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
        clone_firefox(&args.repo, &args.tag, &clone_a, &clone_b)?;
    }

    bootstrap(&clone_a, args.force_bootstrap)?;

    for clone in [&clone_a, &clone_b] {
        write_mozconfig(clone, &kache)?;
    }

    // cold: either run it fresh (full run) or restore the snapshot saved
    // by a prior full run (`--retry`) and reuse cold's metrics. Either
    // way we emerge with `cold_metrics` + `cold_raw`.
    let (cold_metrics, cold_raw) = if args.retry {
        retry_load_cold(&kache, &cache_dir, &work_dir)?
    } else {
        run_cold_phase(
            &kache,
            &cache_dir,
            &kache_config,
            &clone_a,
            &work_dir,
            &event_log,
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
        &clone_b,
        "warm",
        &cache_dir,
        &kache_config,
        &kache,
        &work_dir,
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

    let result = BenchResult {
        firefox_tag: args.tag,
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        cold: cold_metrics,
        warm: warm_metrics,
        speedup: round2(speedup),
        cache_size_mb: round1(dir_size_kb(&cache_dir) as f64 / 1024.0),
        key_stability: stability,
        warm_leak_samples,
        verdict,
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

    let out = work_dir.join("firefox.json");
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

/// Clone Firefox at `tag` into `clone_a` (shallow, from `repo`), then
/// clone `clone_b` locally from `clone_a`.
///
/// `clone_b` is an independent working tree at the same revision —
/// cloned from `clone_a` so there is no second download. `--no-hardlinks`
/// forces real copies of the git objects so the two trees never share
/// storage on disk.
fn clone_firefox(repo: &str, tag: &str, clone_a: &Path, clone_b: &Path) -> Result<()> {
    for d in [clone_a, clone_b] {
        if d.exists() {
            std::fs::remove_dir_all(d)
                .with_context(|| format!("removing stale {}", d.display()))?;
        }
    }
    eprintln!("\n[bench] cloning Firefox {tag} into clone-a");
    run(Command::new("git")
        .args(["clone", "--depth", "1", "--branch"])
        .arg(tag)
        .arg(repo)
        .arg(clone_a))?;
    eprintln!("[bench] cloning a second independent tree into clone-b");
    run(Command::new("git")
        .args(["clone", "--no-hardlinks"])
        .arg(clone_a)
        .arg(clone_b))?;
    Ok(())
}

/// Run `./mach bootstrap` once. Skipped when `~/.mozbuild` already exists
/// unless `force` is set; the toolchain it installs there is shared
/// across runs.
fn bootstrap(clone: &Path, force: bool) -> Result<()> {
    let mozbuild = home_dir()?.join(".mozbuild");
    if !force && mozbuild.is_dir() {
        eprintln!("\n[bench] skipping bootstrap (~/.mozbuild exists; --force-bootstrap to redo)");
        return Ok(());
    }
    eprintln!("\n[bench] running ./mach bootstrap (one-time; may prompt for system packages)");
    run(Command::new(clone.join("mach"))
        .args(["bootstrap", "--application-choice", "browser"])
        .current_dir(clone))
}

/// Write the benchmark mozconfig into `clone`.
fn write_mozconfig(clone: &Path, kache: &Path) -> Result<()> {
    let body = format!(
        "# Generated by kache-bench — compile-cache benchmark.\n\
         # Optimized, non-debug, no LTO/PGO: representative of a dev build\n\
         # and the fairest workload for a compile cache (LTO would defer\n\
         # codegen to link time and shrink the cacheable -c surface; PGO\n\
         # would build twice). Drop --disable-tests for a larger workload.\n\
         ac_add_options --enable-optimize\n\
         ac_add_options --disable-debug\n\
         ac_add_options --disable-tests\n\
         \n\
         # Prepend kache as the C/C++ compiler launcher. Firefox keeps\n\
         # selecting its own bootstrapped clang; kache wraps each -c step.\n\
         #\n\
         # --with-compiler-wrapper, NOT --with-ccache: --with-ccache makes\n\
         # mozbuild run `<wrapper> -s` for ccache stats at build start, and\n\
         # kache rejects `-s` (exit 2) → the build aborts. The wrapper knob\n\
         # prepends kache identically but skips that ccache stats probe.\n\
         ac_add_options --with-compiler-wrapper={kache}\n\
         \n\
         # Route rustc through kache too (also set on the build env).\n\
         mk_add_options \"export RUSTC_WRAPPER={kache}\"\n\
         \n\
         # Incremental compilation is redundant once a compile cache is in\n\
         # play — kache treats it so (it cleans incremental dirs) and\n\
         # Firefox itself disables it for sccache/buildcache. Off here keeps\n\
         # the objdir lean and removes a measurement-noise source. (Not a\n\
         # correctness fix: kache already excludes -Cincremental from the\n\
         # cache key.)\n\
         mk_add_options \"export CARGO_INCREMENTAL=0\"\n\
         \n\
         # Per-clone objdir so the two builds never collide.\n\
         mk_add_options MOZ_OBJDIR=@TOPSRCDIR@/{objdir}\n",
        kache = kache.display(),
        objdir = OBJDIR,
    );
    let path = clone.join("mozconfig");
    std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))
}

/// Build Firefox in `clone` with kache wired in; return wall-clock
/// seconds.
///
/// kache writes wrapper-mode diagnostics — in particular
/// `PathNormalizer`'s residual-path leak detector — directly to
/// `wrapper-<phase>.log` via `KACHE_LOG_FILE=…` + `KACHE_LOG_FILE_PATH=…`.
/// Stderr is unreliable here: cargo (which mach invokes) captures
/// `RUSTC_WRAPPER` stderr and replays it as compiler diagnostics, so warns
/// emitted through stderr never reach `build-<phase>.log`. The dedicated
/// file path side-steps that. `build-<phase>.log` still captures mach's
/// own build output for failure triage.
fn build(
    clone: &Path,
    phase: &str,
    cache_dir: &Path,
    kache_config: &Path,
    kache: &Path,
    work_dir: &Path,
) -> Result<u64> {
    let log_path = work_dir.join(format!("build-{phase}.log"));
    let wrapper_log_path = work_dir.join(format!("wrapper-{phase}.log"));
    let mut log =
        File::create(&log_path).with_context(|| format!("creating {}", log_path.display()))?;
    // Truncate any prior wrapper log so the phase starts clean — kache
    // appends to this file across all parallel wrapper processes.
    File::create(&wrapper_log_path)
        .with_context(|| format!("creating {}", wrapper_log_path.display()))?;
    eprintln!(
        "\n[bench] [{phase}] building Firefox in {} (output -> build-{phase}.log, kache wrapper warns -> wrapper-{phase}.log)",
        clone.display()
    );
    // Wipe the objdir so every phase is a genuine from-scratch build.
    // No-op for a fresh clone; on --skip-clone it removes the previous
    // run's objdir, which would otherwise make "cold" an incremental
    // build. Done before the timer — it's setup, not build work.
    let objdir = clone.join(OBJDIR);
    if objdir.exists() {
        std::fs::remove_dir_all(&objdir)
            .with_context(|| format!("wiping objdir {}", objdir.display()))?;
    }
    let started = Instant::now();
    let mut child = Command::new(clone.join("mach"))
        .arg("build")
        .current_dir(clone)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", kache_config)
        .env("RUSTC_WRAPPER", kache)
        // KACHE_LOG still un-mutes stderr (visible if you tail the log),
        // but the authoritative leak-detector signal goes to the file via
        // KACHE_LOG_FILE — cargo eats wrapper stderr.
        .env("KACHE_LOG", "kache=warn")
        .env("KACHE_LOG_FILE", "kache=warn")
        .env("KACHE_LOG_FILE_PATH", &wrapper_log_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::from(
            log.try_clone().context("cloning build-log handle")?,
        ))
        .spawn()
        .with_context(|| format!("spawning ./mach build in {}", clone.display()))?;

    // Tee mach's stdout to the console (live progress) AND the log file.
    // mach writes its build output — and the kache wrapper diagnostics
    // relayed through it — to stdout, so the log must capture stdout, not
    // just stderr. Byte-oriented so non-UTF-8 build output can't break it.
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
    let status = child.wait().context("waiting for ./mach build")?;
    if !status.success() {
        bail!(
            "[{phase}] ./mach build failed ({status}) — see {}",
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
fn run_cold_phase(
    kache: &Path,
    cache_dir: &Path,
    kache_config: &Path,
    clone_a: &Path,
    work_dir: &Path,
    event_log: &Path,
) -> Result<(PhaseMetrics, serde_json::Value)> {
    daemon_stop(kache, cache_dir);
    if cache_dir.exists() {
        std::fs::remove_dir_all(cache_dir).context("clearing cache dir")?;
    }
    std::fs::create_dir_all(cache_dir)?;
    let cold_s = build(clone_a, "cold", cache_dir, kache_config, kache, work_dir)?;
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
    kache: &Path,
    cache_dir: &Path,
    work_dir: &Path,
) -> Result<(PhaseMetrics, serde_json::Value)> {
    let snapshot = work_dir.join("cache-after-cold");
    let firefox_json = work_dir.join("firefox.json");
    let report_cold = work_dir.join("report-cold.json");
    for required in [&snapshot, &firefox_json, &report_cold] {
        if !required.exists() {
            bail!(
                "--retry: required artifact missing — {} (run `just bench-firefox` once first)",
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
    let prev: serde_json::Value = read_json(&firefox_json)?;
    let cold_metrics: PhaseMetrics = serde_json::from_value(prev["cold"].clone())
        .context("loading previous cold metrics from firefox.json")?;
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
        match ev["result"].as_str().unwrap_or_default() {
            "local_hit" | "prefetch_hit" | "remote_hit" | "miss" => stats.cached += 1,
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
/// unavailable or fails — the cache size is a reported metric, not a
/// load-bearing one.
fn dir_size_kb(dir: &Path) -> u64 {
    Command::new("du")
        .arg("-sk")
        .arg(dir)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .split_whitespace()
                .next()
                .and_then(|kb| kb.parse().ok())
        })
        .unwrap_or(0)
}

/// Clone a directory tree via the filesystem's CoW reflink mechanism
/// when supported (APFS / btrfs / XFS-reflink); fall back to a plain
/// recursive copy. Used to snapshot the cache after the cold phase so a
/// later `--retry` can restore exactly that state without re-running.
fn snapshot_dir(src: &Path, dst: &Path) -> Result<()> {
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
    bail!("failed to snapshot {} -> {}", src.display(), dst.display())
}

/// Deserialize a JSON file into `T`.
fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let s = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&s).with_context(|| format!("parsing JSON from {}", path.display()))
}

fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
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
    eprintln!("  kache Firefox benchmark — {}", r.firefox_tag);
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
    eprintln!("  cache size : {:.1} MB on disk", r.cache_size_mb);
    let st = &r.warm.storage;
    eprintln!(
        "  storage    : restored {} — {:.1}% zero-copy  (reflink {} / hardlink {} / copy {})",
        human_bytes(st.restored_bytes),
        st.zero_copy_pct,
        human_bytes(st.reflinked_bytes),
        human_bytes(st.hardlinked_bytes),
        human_bytes(st.copied_bytes),
    );
    eprintln!(
        "             : store {} on disk vs {} logical — dedup saved {} ({} blobs)",
        human_bytes(st.blob_bytes),
        human_bytes(st.logical_bytes),
        human_bytes(st.dedup_saved_bytes),
        st.store_blobs,
    );
    eprintln!("{bar}");

    // ── diagnostics: did the run actually exercise kache? ──
    eprintln!(
        "  key stability : {:.1}%   ({} of {} crates kept an identical key across clones)",
        r.key_stability.stable_pct, r.key_stability.stable, r.key_stability.compared
    );
    let el = &r.warm.event_log;
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

/// Benchmark result document written to `<work-dir>/firefox.json`.
#[derive(Debug, Serialize)]
struct BenchResult {
    firefox_tag: String,
    /// `Os-Arch`, e.g. `macos-aarch64`.
    platform: String,
    cold: PhaseMetrics,
    warm: PhaseMetrics,
    /// Cold wall-clock divided by warm wall-clock.
    speedup: f64,
    cache_size_mb: f64,
    /// Cross-clone cache-key stability — the deterministic correctness
    /// signal.
    key_stability: KeyStability,
    /// Sample lines from kache's leak detector during the warm build.
    warm_leak_samples: Vec<String>,
    /// Whether the run validly exercised kache (drives the exit code).
    verdict: Verdict,
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
