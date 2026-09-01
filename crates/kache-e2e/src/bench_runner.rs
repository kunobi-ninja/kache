//! Clone benchmark scenario engine.
//!
//! Builds a real project (Firefox, Substrate, …) twice against one
//! shared compiler cache:
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
//! `--warm-same-tree` inserts a third phase between those two:
//!
//! - **warm-same-tree**: clone-a rebuilt in place with its objdir wiped and the
//!   store left warm. Same absolute path, so it measures restore cost with the
//!   path-portability question held constant — the everyday "I cleaned my
//!   `target/`" case. Off by default; the nightly scenarios would pay for a
//!   third full build they do not read. Added for the per-PR perf gate, which
//!   needs cold, warm, and cross-worktree as three separate numbers.
//!
//! The *project-specific* parts — which repo, how to wire kache in, how
//! to build — live in benchmark scenarios under `scenarios/bench-*`;
//! this binary is the project-agnostic measurement engine. See
//! `scenarios/README.md` for the scenario format.
//!
//! Reports cold/warm wall-clock, speedup, and hit rate. This is a manual
//! tool: a full run takes tens of minutes to a few hours and needs tens
//! of GB of disk. It is intentionally NOT wired into CI.
//!
//! The kache benchmark path is **self-diagnosing** when a scenario declares
//! `checks.assert`: it captures kache's own leak detector, measures
//! cross-clone cache-key stability, accounts for the passthrough events
//! `kache report` hides, and fails the run (non-zero exit) when those
//! signals say the run did not validly exercise kache. The sccache path
//! validates its isolated cache location and base-dir normalization before
//! reporting a cold/warm result.
//!
//! Reuses [`kache_e2e::report`] — the typed `kache report --format json`
//! fetch and parsing are shared with the e2e harness — and
//! [`kache_e2e::bench_profile`] for the clone-scenario format.
//!
//! # Running the benchmark
//!
//! ```text
//! just bench firefox               # full cold + warm
//! just bench substrate
//! kache-scenario --select suite:bench --profile firefox --retry
//! kache-scenario --select suite:bench --profile firefox --skip-clone
//! kache-scenario --select suite:bench --profile firefox --ref FIREFOX_152_0_RELEASE
//! ```
//!
//! A scenario's `requires` tools must be on `PATH` or the run is skipped;
//! its `setup` (e.g. `./mach bootstrap`) runs once after a fresh clone.
//!
//! # Reading the output
//!
//! Each run prints a summary block and writes `tmp/bench/<scenario>/<scenario>.json`
//! plus per-phase reports (`report-<phase>.*`), build logs
//! (`build-<phase>.log`), wrapper logs (`wrapper-<phase>.log`), OTLP
//! gauges (`metrics.otlp.json`, `kache.bench.*`), and cache counters
//! (`cache-otlp-<phase>/metrics.otlp.json`, `kache.cache.*`) for kartero.
//! By default each scenario writes under its own `./tmp/bench/<scenario>` (so
//! scenarios coexist); a `work_dir` lock prevents two runs from sharing one
//! scratch dir. Concurrent runs on one host make the wall-clock numbers
//! unreliable — run sequentially or on separate hosts.
//!
//! The headline metrics — wall-clock, speedup, hit rate — are only
//! meaningful when the **verdict** is `ok`. A `DEGRADED RUN` verdict
//! means at least one configured assertion flagged a problem (path-leak
//! warns fired, cross-clone key stability collapsed, or kache barely
//! exercised). The summary lists the specific issues; treat the speedup
//! as suspect until they're addressed. Note that wrapped-compiler non-zero
//! exits are NOT a degradation signal — see [`Verdict::evaluate`].
//!
//! # Maintenance
//!
//! To bump a project's pinned version, edit `source.ref` in its scenario (or pass
//! `--ref` for a one-off). If a build tool changes its wrapper-injection
//! knobs, update the scenario's `[[file]]` / `[env]`, not this binary.

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

use crate::assertions::{AssertionCheck, all_passed};
use crate::bench_profile::BenchProfile;
use crate::daemon;
use crate::phase::Phase;
use crate::report;
use crate::scenario::{MeasureSpec, ScenarioAssertSpec, Selectors};
use crate::source;

const SCCACHE_BENCH_CACHE_SIZE: &str = "80G";
// Firefox can spend many minutes in non-cacheable link/Rust-LTO work. Keep the
// daemon alive so one cold/warm phase does not lose stats mid-build.
const SCCACHE_BENCH_IDLE_TIMEOUT_SECS: &str = "43200";

#[derive(Debug, Clone)]
pub struct BenchRunConfig {
    pub kache: PathBuf,
    pub sccache: PathBuf,
    pub cache_backend: CacheBackend,
    pub scenarios: PathBuf,
    pub select: Vec<String>,
    pub git_ref: Option<String>,
    pub work_dir: Option<PathBuf>,
    pub skip_clone: bool,
    pub force_setup: bool,
    pub retry: bool,
    pub trace_keys: bool,
    /// Insert the same-worktree warm phase between `cold` and the cross-clone
    /// `warm`. Off by default — the nightly scenarios would pay for a third
    /// full build they do not use. See [`Phase::WarmSameTree`].
    pub warm_same_tree: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CacheBackend {
    Kache,
    Sccache,
}

impl CacheBackend {
    fn label(self) -> &'static str {
        match self {
            CacheBackend::Kache => "kache",
            CacheBackend::Sccache => "sccache",
        }
    }
}

pub fn run_bench(config: BenchRunConfig) -> Result<()> {
    if config.cache_backend == CacheBackend::Sccache && config.trace_keys {
        bail!("--trace-keys is only supported with --cache-backend kache");
    }
    if config.cache_backend == CacheBackend::Sccache && config.warm_same_tree {
        bail!("--warm-same-tree is only supported with --cache-backend kache");
    }
    let cache_tool_arg = match config.cache_backend {
        CacheBackend::Kache => &config.kache,
        CacheBackend::Sccache => &config.sccache,
    };
    let cache_tool_path = resolve_binary(cache_tool_arg);
    let kache = de_verbatim(cache_tool_path.canonicalize().with_context(|| {
        format!(
            "{} binary not found at {}",
            config.cache_backend.label(),
            cache_tool_path.display()
        )
    })?);
    let cache_tool_version = tool_version(&kache);
    let sh = posix_sh()?;

    let scenarios_dir = config
        .scenarios
        .canonicalize()
        .with_context(|| format!("scenarios dir not found at {}", config.scenarios.display()))?;

    // Load exactly one clone benchmark scenario by tag/name selectors. The
    // engine below remains single-scenario; discovery is shared.
    let selectors = Selectors::parse_many(&config.select)?;
    let mut profiles = BenchProfile::discover(&scenarios_dir, &selectors)?;
    if profiles.is_empty() {
        anyhow::bail!(
            "no bench scenario selected under {} (after --select {})",
            scenarios_dir.display(),
            selectors.describe()
        );
    }
    if profiles.len() > 1 {
        anyhow::bail!(
            "bench selection matched multiple scenarios: {}. Add --profile or a narrower --select.",
            profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let mut profile = profiles.remove(0);
    if let Some(r) = &config.git_ref {
        profile.git_ref = r.clone();
    }
    let missing = profile.missing_requirements();
    if !missing.is_empty() {
        eprintln!(
            "[bench] SKIP scenario `{}`: missing required tool(s) on PATH: {}",
            profile.name,
            missing.join(", ")
        );
        return Ok(());
    }
    let objdir = profile.objdir.clone();

    let work_dir_arg = config
        .work_dir
        .unwrap_or_else(|| default_work_dir(&profile.name));
    std::fs::create_dir_all(&work_dir_arg)
        .with_context(|| format!("creating work dir {}", work_dir_arg.display()))?;
    let work_dir = de_verbatim(work_dir_arg.canonicalize()?);
    // Hold an exclusive lock on the work dir for the whole run so a second
    // bench can't share this scratch dir and clobber it. Bound (not `_`) so it
    // lives to the end of `main`; released automatically on process exit.
    let _work_dir_lock = acquire_work_dir_lock(&work_dir)?;
    let run_id = dated_run_id(config.cache_backend);
    let run_archive_dir = work_dir.join("runs").join(&run_id);

    // `clone-ref` lives at a SIBLING of `work_dir` (not inside it) so
    // the natural `rm -rf <work_dir>` wipe — what someone reaches for
    // to reset the bench — doesn't accidentally torch the locally-cached
    // reference clone. Re-cloning a large project is the bench's single
    // most expensive setup step; making the reference survive a casual
    // scratch-dir wipe is the whole point.
    let clone_ref = source::clone_ref_path(&work_dir);
    let clone_a = work_dir.join("clone-a");
    let clone_b = work_dir.join("clone-b");
    let cache_dir = work_dir.join("cache");
    let kache_config = work_dir.join("kache-config.toml");
    let event_log = cache_dir.join("events.jsonl");

    eprintln!(
        "=== {} benchmark: {} ===",
        config.cache_backend.label(),
        profile.name
    );
    eprintln!("ref         : {}", profile.git_ref);
    if !selectors.is_empty() {
        eprintln!("select      : {}", selectors.describe());
    }
    eprintln!(
        "work dir    : {}  (worktrees + objdirs + cache)",
        work_dir.display()
    );
    eprintln!(
        "clone ref   : {}  (persistent across runs)",
        clone_ref.display()
    );
    eprintln!(
        "{}       : {}",
        config.cache_backend.label(),
        kache.display()
    );
    if let Some(version) = &cache_tool_version {
        eprintln!("version     : {version}");
    }

    // kache rotates its event log at 10 MiB, keeping only the last 500
    // lines. A large project build emits tens of thousands of events, so
    // the default would discard nearly everything before the report is
    // read. A large cap keeps every event of a phase intact for its report.
    if config.cache_backend == CacheBackend::Kache {
        std::fs::write(&kache_config, "[cache]\nevent_log_max_size = \"8GiB\"\n")
            .context("writing benchmark kache config")?;
    }

    if profile.is_pull() {
        if config.cache_backend != CacheBackend::Kache {
            bail!("pull scenarios (`ref_next`) are only supported with --cache-backend kache");
        }
        if config.warm_same_tree {
            // A pull scenario already rebuilds clone-a a second time, at a
            // different ref. Adding the same-tree warm phase would silently
            // change what `pull` measures.
            bail!("--warm-same-tree is not supported for pull scenarios (`ref_next`)");
        }
        return run_pull_bench(
            &profile,
            &kache,
            &cache_dir,
            &kache_config,
            &clone_ref,
            &clone_a,
            &work_dir,
            &event_log,
            &objdir,
            &run_id,
            &run_archive_dir,
            cache_tool_version.as_deref(),
            config.force_setup,
            config.trace_keys,
            &sh,
        );
    }

    if config.skip_clone || config.retry {
        if !clone_a.is_dir() || !clone_b.is_dir() {
            let flag = if config.retry {
                "--retry"
            } else {
                "--skip-clone"
            };
            bail!(
                "{flag} set but clones are missing under {}",
                work_dir.display()
            );
        }
        let flag = if config.retry {
            "--retry"
        } else {
            "--skip-clone"
        };
        eprintln!("\n[bench] reusing existing clones ({flag})");
    } else {
        source::clone_worktrees(
            &profile.repo,
            &profile.git_ref,
            &clone_ref,
            &clone_a,
            &clone_b,
        )?;
        run_setup(&profile, &clone_a, &kache, config.force_setup, &sh)?;
    }

    // Wire kache into each worktree (mozconfig write, .cargo/config append,
    // …). Applied once per fresh worktree; on `--skip-clone` the reused
    // tree keeps prior injections, so `write` overwrites cleanly but
    // `append`/`patch` scenarios want a full (re-cloning) run.
    for clone in [&clone_a, &clone_b] {
        profile.apply_files(clone, &kache)?;
    }

    // Snapshot the volume's free space before any build runs, so the real
    // footprint of the cold+warm builds can be measured (free-space delta) and
    // used to VERIFY the sharing-corrected disk-layout estimate. Clones already
    // exist at this point; the cache and objdirs do not (cold wipes the cache
    // first). See `measures_disk_footprint` for when the delta is meaningful.
    let disk_free_before = if measures_disk_footprint(config.retry, config.warm_same_tree) {
        available_bytes(&work_dir)
    } else {
        None
    };

    if config.cache_backend == CacheBackend::Sccache {
        return run_sccache_bench(
            &profile,
            &kache,
            &cache_dir,
            &clone_a,
            &clone_b,
            &work_dir,
            config.retry,
            &sh,
            disk_free_before,
            &objdir,
            &run_id,
            &run_archive_dir,
            cache_tool_version.as_deref(),
        );
    }

    // cold: either run it fresh (full run) or restore the snapshot saved
    // by a prior full run (`--retry`) and reuse cold's metrics. Either
    // way we emerge with `cold_metrics` + `cold_raw`.
    let (cold_metrics, cold_raw) = if config.retry {
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
            config.trace_keys,
            &sh,
        )?
    };

    // warm-same-tree (opt-in): rebuild clone-a — the tree cold just built — with
    // its objdir wiped and the store left warm. Same absolute path, so the
    // measurement isolates restore cost from the path-portability question the
    // cross-clone warm phase answers next. It runs BEFORE that phase because
    // cold's cache snapshot (`cache-after-cold`, used by `--retry`) is already
    // on disk, and because clone-b must stay untouched until it is built once.
    //
    // Anything this rebuild stores (a passthrough that cold did not reach) also
    // benefits the cross-clone warm phase below; that is the same direction the
    // cache already works in and is noted in the summary.
    let warm_same_tree_metrics = if config.warm_same_tree {
        reset_event_log(&event_log)?;
        daemon::start(&kache, &cache_dir, &kache_config);
        let wall_s = build(
            &profile,
            &clone_a,
            Phase::WarmSameTree.name(),
            &cache_dir,
            &kache_config,
            &kache,
            &work_dir,
            CacheBackend::Kache,
            config.trace_keys,
            &sh,
        )?;
        write_cache_otlp_or_warn(
            &kache,
            &cache_dir,
            &kache_config,
            &work_dir.join("cache-otlp-warm-same-tree"),
            &profile.name,
            Phase::WarmSameTree.name(),
        );
        daemon::stop(&kache, &cache_dir);
        let (report, raw) = capture_report(
            &kache,
            &cache_dir,
            &work_dir,
            Phase::WarmSameTree.name(),
            &clone_a,
        )?;
        let events = read_event_log(&event_log);
        let (leaks, _) = scan_leak_warnings(
            &work_dir.join(format!("wrapper-{}.log", Phase::WarmSameTree.name())),
        );
        Some(PhaseMetrics::from_report(
            &report, &raw, wall_s, events, leaks,
        ))
    } else {
        None
    };

    // Reset the event log so warm's report covers only the warm build.
    // Only the observability log is removed — the cache store (`store/`,
    // `index.db`) stays, so warm still hits everything cold populated.
    reset_event_log(&event_log)?;

    // warm: same cache, fresh clone at a different path → served from
    // what cold populated. Bring the daemon up first so the restore uses the
    // async file-hash prefetch path instead of hashing every input inline
    // (cold's daemon was stopped before its report capture).
    daemon::start(&kache, &cache_dir, &kache_config);
    let warm_s = build(
        &profile,
        &clone_b,
        Phase::Warm.name(),
        &cache_dir,
        &kache_config,
        &kache,
        &work_dir,
        CacheBackend::Kache,
        config.trace_keys,
        &sh,
    )?;
    write_cache_otlp_or_warn(
        &kache,
        &cache_dir,
        &kache_config,
        &work_dir.join("cache-otlp-warm"),
        &profile.name,
        "warm",
    );
    daemon::stop(&kache, &cache_dir);
    let (warm, warm_raw) =
        capture_report(&kache, &cache_dir, &work_dir, Phase::Warm.name(), &clone_b)?;
    let warm_events = read_event_log(&event_log);
    let (warm_leaks, warm_leak_samples) =
        scan_leak_warnings(&work_dir.join(format!("wrapper-{}.log", Phase::Warm.name())));

    // Free-space delta = real disk the cold+warm builds consumed (CoW/dedup/
    // compression-honest). `checked_sub` guards the rare case where background
    // activity freed more than the builds wrote, which would not be a usable
    // measurement.
    let disk_measured_bytes = match (disk_free_before, available_bytes(&work_dir)) {
        (Some(before), Some(after)) => before.checked_sub(after),
        _ => None,
    };

    let speedup = if warm_s > 0 {
        cold_metrics.wall_s as f64 / warm_s as f64
    } else {
        0.0
    };
    // Same-tree speedup against the SAME run's cold build — the "did the warm
    // build actually beat the cold one that seeded it" number a timing gate
    // needs before it trusts a wall-clock delta. Present only when the phase ran.
    let warm_same_tree_speedup = warm_same_tree_metrics
        .as_ref()
        .map(|m| phase_speedup(cold_metrics.wall_s, m.wall_s));

    // Cross-clone cache-key stability: for the deterministic correctness
    // signal, see how many crates produced an identical key in both
    // clones. A path leak in the key shows up here as a near-zero rate.
    let stability = key_stability(&cold_raw, &warm_raw);

    let warm_metrics = PhaseMetrics::from_report(&warm, &warm_raw, warm_s, warm_events, warm_leaks);
    let verdict = Verdict::evaluate(
        &stability,
        &warm_metrics,
        profile.checks.assertions.for_phase(Phase::Warm.name()),
    );
    // The same-tree phase gets its OWN verdict from
    // `[checks.assert.warm-same-tree]`, kept beside the cross-clone one rather
    // than folded into it: `verdict` is the shape the nightly telemetry and the
    // report JSON already publish, and both phases name the same checks. Key
    // stability is cross-clone by definition, so it is passed as "nothing
    // compared" — `Verdict::evaluate` skips that check when the denominator is
    // zero. Both verdicts drive the exit code (see the end of this function).
    let warm_same_tree_verdict = warm_same_tree_metrics.as_ref().map(|metrics| {
        Verdict::evaluate(
            &KeyStability::default(),
            metrics,
            profile
                .checks
                .assertions
                .for_phase(Phase::WarmSameTree.name()),
        )
    });
    let mut measure_warnings = bench_measure_warnings(
        &warm_metrics,
        speedup,
        profile.checks.measure.for_phase(Phase::Warm.name()),
    );

    // Apparent size of each clone's objdir. On APFS the warm
    // clone's apparent size double-counts the bytes it reflinked from
    // the cache — print_summary subtracts those to expose "unique to
    // this clone" disk usage.
    let cold_objdir_bytes = dir_size_kb(&clone_a.join(&objdir)).saturating_mul(1024);
    let warm_objdir_bytes = dir_size_kb(&clone_b.join(&objdir)).saturating_mul(1024);

    // Verify: the measured free-space delta should land near the
    // sharing-corrected estimate of the three-pool footprint. They agree on
    // any filesystem — on a CoW one because the shared bytes are subtracted,
    // on a non-CoW one because hardlinked shared bytes are subtracted the same
    // way (or nothing is shared and the estimate stays at the apparent sum).
    // A gap therefore means the storage byte accounting is off (not merely
    // "no CoW"), which is worth surfacing. A 20% band absorbs the cache
    // snapshot, logs, and df noise.
    if let Some(measured) = disk_measured_bytes {
        let warm_st = &warm_metrics.storage;
        // Hardlinked bytes (store ingest and warm restore) are one inode in
        // two pools, exactly like reflinked bytes — both are sharing, not a
        // second copy, so both are subtracted from the apparent sum.
        let store_shared = cold_metrics
            .storage
            .store_reflinked_bytes
            .saturating_add(cold_metrics.storage.store_hardlinked_bytes)
            .saturating_add(warm_st.store_reflinked_bytes)
            .saturating_add(warm_st.store_hardlinked_bytes);
        let shared_total = warm_st
            .reflinked_bytes
            .saturating_add(warm_st.hardlinked_bytes)
            .saturating_add(store_shared);
        let sum_apparent = cold_objdir_bytes
            .saturating_add(warm_objdir_bytes)
            .saturating_add(warm_st.blob_bytes);
        let estimate = sum_apparent.saturating_sub(shared_total);
        let within_band = measured <= estimate.saturating_mul(6) / 5
            && measured >= estimate.saturating_mul(4) / 5;
        if estimate > 0 && !within_band {
            measure_warnings.push(format!(
                "disk: measured on-disk {} diverges from the sharing-corrected estimate {} \
                 (apparent {}) — storage byte accounting may be off",
                human_bytes(measured),
                human_bytes(estimate),
                human_bytes(sum_apparent),
            ));
        }
    }

    // With `--trace-keys`, both phases logged every key-input the hasher
    // consumed (one line per input, prefixed `[key:CRATE]`). Diff the
    // two phases per-crate and aggregate by input field so the run
    // can name what diverged across clones — the actionable signal
    // when `key_stability` < 100%.
    let key_diff_top = if config.trace_keys {
        let cold_trace = parse_key_trace(&work_dir.join("wrapper-cold.log"));
        let warm_trace = parse_key_trace(&work_dir.join("wrapper-warm.log"));
        let divergence = compute_key_divergence(&cold_trace, &warm_trace);
        write_key_diff_reports(&divergence, &work_dir)?
    } else {
        None
    };

    let mut reports = vec![
        "report-cold.json".to_string(),
        "report-cold.md".to_string(),
        "report-warm.json".to_string(),
        "report-warm.md".to_string(),
        "build-cold.log".to_string(),
        "build-warm.log".to_string(),
        "wrapper-cold.log".to_string(),
        "wrapper-warm.log".to_string(),
    ];
    if warm_same_tree_metrics.is_some() {
        reports.extend([
            "report-warm-same-tree.json".to_string(),
            "report-warm-same-tree.md".to_string(),
            "build-warm-same-tree.log".to_string(),
            "wrapper-warm-same-tree.log".to_string(),
        ]);
    }

    let result = BenchResult {
        project: profile.name.clone(),
        git_ref: profile.git_ref.clone(),
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        run_id: run_id.clone(),
        artifact_dir: run_archive_dir.display().to_string(),
        cache_tool_version: cache_tool_version.clone(),
        cold: cold_metrics,
        warm_same_tree: warm_same_tree_metrics,
        warm: warm_metrics,
        speedup: round2(speedup),
        warm_same_tree_speedup,
        cache_size_mb: round1(dir_size_kb(&cache_dir) as f64 / 1024.0),
        cold_objdir_bytes,
        warm_objdir_bytes,
        disk_measured_bytes,
        key_stability: stability,
        warm_leak_samples,
        verdict,
        warm_same_tree_verdict,
        measure_warnings,
        key_diff_top: key_diff_top.clone(),
        reports,
    };

    let out = work_dir.join(format!("{}.json", profile.name));
    std::fs::write(&out, serde_json::to_string_pretty(&result)? + "\n")
        .with_context(|| format!("writing {}", out.display()))?;
    write_otlp_or_warn(
        &work_dir,
        crate::bench_otlp::OtlpRun {
            project: result.project.clone(),
            git_ref: result.git_ref.clone(),
            cache_tool: "kache",
            time_unix_nano: crate::bench_otlp::OtlpRun::now_unix_nano(),
            verdict_ok: result.verdict.ok,
            speedup: Some(result.speedup),
            cache_size_bytes: crate::bench_otlp::OtlpRun::bytes_from_mib(result.cache_size_mb),
            key_stability_pct: Some(result.key_stability.stable_pct),
            disk_measured_bytes: result.disk_measured_bytes,
            phases: vec![
                otlp_phase("cold", &result.cold, result.cold_objdir_bytes),
                otlp_phase("warm", &result.warm, result.warm_objdir_bytes),
            ],
        },
    );

    archive_run_artifacts(&work_dir, &run_archive_dir)?;
    // A broken stderr must not turn a completed benchmark into a failed one —
    // the reports are already on disk by this point.
    let _ = write_summary(&mut std::io::stderr().lock(), &result, &run_archive_dir);
    eprintln!("[bench] summary written to {}", out.display());

    // Preserve the existing manual-bench contract: clone-scale correctness
    // assertions still fail the run, while measurements only warn. The
    // same-tree phase's own verdict is held to the same contract, so a PR gate
    // reading these numbers cannot be handed a phase that measured nothing.
    if run_is_degraded(&result.verdict, result.warm_same_tree_verdict.as_ref()) {
        std::process::exit(1);
    }
    Ok(())
}

/// Should this run measure its own disk footprint as the volume's free-space
/// delta across the build phases?
///
/// Only a plain full run can. `--retry` restores cold from a snapshot instead
/// of building it, so the delta would not cover the cold pool.
/// `--warm-same-tree` wipes and refills clone-a's objdir a second time, so the
/// delta no longer maps onto the three-pool (clone-a/obj + clone-b/obj + store)
/// model the estimate is checked against and the comparison would raise a
/// spurious accounting warning.
fn measures_disk_footprint(retry: bool, warm_same_tree: bool) -> bool {
    !retry && !warm_same_tree
}

/// Speedup of a warm phase against the cold build that seeded it, rounded for
/// the report.
///
/// Zero when the phase's wall-clock rounded down to zero seconds: the engine
/// times whole seconds, and dividing by zero yields an infinity that is not a
/// number any report should carry.
fn phase_speedup(cold_wall_s: u64, phase_wall_s: u64) -> f64 {
    if phase_wall_s > 0 {
        round2(cold_wall_s as f64 / phase_wall_s as f64)
    } else {
        0.0
    }
}

/// Did any configured assertion say this run is not a valid measurement?
///
/// The cross-clone verdict always counts. The same-tree verdict is present only
/// when `--warm-same-tree` ran, and when it is, it is held to the same
/// contract: a PR gate reading these numbers must never be handed a phase that
/// measured nothing.
fn run_is_degraded(verdict: &Verdict, warm_same_tree_verdict: Option<&Verdict>) -> bool {
    !verdict.ok || warm_same_tree_verdict.is_some_and(|same_tree| !same_tree.ok)
}

/// Default scratch dir for a scenario when `--work-dir` is not given:
/// `./tmp/bench/<scenario>`, so scenarios never share state and a single
/// `rm -rf tmp/bench` cleans every scenario at once.
fn default_work_dir(profile_name: &str) -> PathBuf {
    PathBuf::from(format!("./tmp/bench/{profile_name}"))
}

fn dated_run_id(cache_backend: CacheBackend) -> String {
    format!(
        "{}-{}-{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
        cache_backend.label(),
        std::process::id()
    )
}

fn archive_run_artifacts(work_dir: &Path, archive_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(archive_dir)
        .with_context(|| format!("creating run archive {}", archive_dir.display()))?;
    for entry in std::fs::read_dir(work_dir)
        .with_context(|| format!("reading work dir {}", work_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_run_artifact(name) {
            continue;
        }
        let target = archive_dir.join(name);
        std::fs::copy(&path, &target)
            .with_context(|| format!("copying {} to {}", path.display(), target.display()))?;
    }
    Ok(())
}

fn is_run_artifact(name: &str) -> bool {
    name.starts_with("report-")
        || name.starts_with("build-")
        || name.starts_with("wrapper-")
        || name.starts_with("trace-")
        || name.starts_with("key-diff.")
        || (name.starts_with("bench-") && name.ends_with(".json"))
        || name == crate::bench_otlp::METRICS_FILE
        || name == crate::bench_otlp::SCHEMA_VERSION_FILE
}

fn otlp_phase(
    name: &'static str,
    metrics: &PhaseMetrics,
    objdir_bytes: u64,
) -> crate::bench_otlp::OtlpPhase {
    crate::bench_otlp::OtlpPhase {
        name,
        wall_s: metrics.wall_s,
        time_saved_s: Some(metrics.time_saved_s),
        hits: metrics.hits,
        dups: Some(metrics.dups),
        misses: metrics.misses,
        errors: Some(metrics.errors),
        total: Some(metrics.total_crates),
        hit_rate_pct: metrics.hit_rate_pct,
        weighted_hit_rate_pct: Some(metrics.weighted_hit_rate_pct),
        leak_warnings: Some(metrics.leak_warnings),
        objdir_bytes,
    }
}

fn otlp_sccache_phase(
    name: &'static str,
    metrics: &SccachePhaseMetrics,
    objdir_bytes: u64,
) -> crate::bench_otlp::OtlpPhase {
    crate::bench_otlp::OtlpPhase {
        name,
        wall_s: metrics.wall_s,
        time_saved_s: None,
        hits: metrics.cache_hits,
        dups: None,
        misses: metrics.cache_misses,
        errors: None,
        total: Some(metrics.compile_requests),
        hit_rate_pct: metrics.hit_rate_pct,
        weighted_hit_rate_pct: None,
        leak_warnings: None,
        objdir_bytes,
    }
}

fn write_otlp_or_warn(work_dir: &Path, run: crate::bench_otlp::OtlpRun) {
    if let Err(err) = crate::bench_otlp::write_otlp(work_dir, &run) {
        eprintln!("[bench] warning: failed to write OTLP telemetry: {err:#}");
        return;
    }
    eprintln!(
        "[bench] otlp written to {}",
        work_dir.join(crate::bench_otlp::METRICS_FILE).display()
    );
}

/// Snapshot daemon/store counters into `cache-otlp-<phase>/` for Kartero.
/// Separate from the bench gauges (`kache.bench.*`) so the two signals never
/// share a metric name. Call while the phase's daemon is still up. Failures
/// warn; a multi-hour bench must still upload reports.
fn write_cache_otlp_or_warn(
    kache: &Path,
    cache_dir: &Path,
    kache_config: &Path,
    dest: &Path,
    scenario: &str,
    phase: &str,
) -> bool {
    let status = Command::new(kache)
        .args(["telemetry", "write"])
        .arg(dest)
        .args(["--scenario", scenario, "--phase", phase])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", kache_config)
        .env("RUSTC_WRAPPER", "")
        .status();
    match status {
        Ok(code) if code.success() => {
            eprintln!(
                "[bench] cache otlp written to {}",
                dest.join("metrics.otlp.json").display()
            );
            true
        }
        Ok(code) => {
            eprintln!("[bench] warning: kache telemetry write exited {code}");
            false
        }
        Err(err) => {
            eprintln!("[bench] warning: kache telemetry write failed: {err}");
            false
        }
    }
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
            "another benchmark scenario run is already using {} — pass a different \
             --work-dir or wait for it to finish",
            work_dir.display()
        ),
        Err(std::fs::TryLockError::Error(e)) => {
            Err(e).with_context(|| format!("locking {}", lock_path.display()))
        }
    }
}

/// Strip Windows' `\\?\` extended-length prefix after canonicalization.
/// Many external tools, including git, reject verbatim paths.
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

/// Return `path` if it exists, else `path.exe` if that exists. This lets the
/// Unix-style default `./target/release/kache` resolve on Windows.
fn resolve_binary(path: &Path) -> PathBuf {
    if path.exists() {
        return path.to_path_buf();
    }
    let exe = path.with_extension("exe");
    if exe.exists() {
        return exe;
    }
    if path.components().count() == 1
        && let Some(found) = find_on_path(path)
    {
        return found;
    }
    path.to_path_buf()
}

fn find_on_path(program: &Path) -> Option<PathBuf> {
    let name = program.file_name()?;
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path).find_map(|dir| {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            let exe = candidate.with_extension("exe");
            if exe.is_file() {
                return Some(exe);
            }
        }
        None
    })
}

fn tool_version(tool: &Path) -> Option<String> {
    let output = Command::new(tool).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let version = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if version.is_empty() {
        None
    } else {
        Some(version)
    }
}

/// Resolve a POSIX `sh` for scenario setup/build snippets.
fn posix_sh() -> Result<PathBuf> {
    #[cfg(not(windows))]
    {
        Ok(PathBuf::from("sh"))
    }
    #[cfg(windows)]
    {
        find_windows_sh()
            .context("no POSIX `sh` found; install Git for Windows or put sh.exe on PATH")
    }
}

#[cfg(windows)]
fn find_windows_sh() -> Option<PathBuf> {
    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let cand = dir.join("sh.exe");
            if cand.is_file() {
                return Some(cand);
            }
        }
    }

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

    for root in [r"C:\Program Files\Git", r"C:\Program Files (x86)\Git"] {
        let cand = Path::new(root).join(r"usr\bin\sh.exe");
        if cand.is_file() {
            return Some(cand);
        }
    }
    None
}

/// Run the scenario's one-time `setup` steps (e.g. `./mach bootstrap`,
/// `rustup target add`) in `clone`. Skipped when the scenario's
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
/// `RUSTC_WRAPPER` / `KACHE_LOG*`) is set last so a scenario's `[env]`
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
    cache_backend: CacheBackend,
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
        "\n[bench] [{phase}] building {} in {} (`{build_cmd}`; output -> build-{phase}.log, {} wrapper log -> wrapper-{phase}.log)",
        profile.name,
        clone.display(),
        cache_backend.label()
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
    let mut cmd = Command::new(sh);
    cmd.arg("-c")
        .arg(&build_cmd)
        .current_dir(clone)
        // Profile env first so the benchmark baseline below always wins.
        .envs(profile.build_env(kache))
        .env("RUSTC_WRAPPER", kache)
        // Incremental is redundant once a compile cache is in play (kache
        // already excludes -Cincremental from the key); off keeps the
        // build dir lean and removes a measurement-noise source. Injected
        // for every scenario (Firefox also sets it via mozconfig).
        .env("CARGO_INCREMENTAL", "0");
    match cache_backend {
        CacheBackend::Kache => {
            cmd.env("KACHE_CACHE_DIR", cache_dir)
                .env("KACHE_CONFIG", kache_config)
                .env("KACHE_EVENT_ROOT", clone)
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
                .env("KACHE_LOG_FILE_PATH", &wrapper_log_path);
        }
        CacheBackend::Sccache => {
            cmd.env("SCCACHE_DIR", cache_dir)
                .env("SCCACHE_CACHE_SIZE", SCCACHE_BENCH_CACHE_SIZE)
                .env("SCCACHE_IDLE_TIMEOUT", SCCACHE_BENCH_IDLE_TIMEOUT_SECS)
                .env("SCCACHE_SERVER_UDS", sccache_server_uds(cache_dir))
                .env("SCCACHE_BASEDIRS", clone)
                .env("SCCACHE_ERROR_LOG", &wrapper_log_path);
        }
    }
    let mut child = cmd
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
/// markdown (`report-<phase>.md`) into `out_dir`, write a Perfetto/Chrome
/// trace (`trace-<phase>.json`), and return the parsed report plus the raw
/// value for the benchmark summary.
fn capture_report(
    kache: &Path,
    cache_dir: &Path,
    out_dir: &Path,
    phase: &str,
    root: &Path,
) -> Result<(report::KacheReport, serde_json::Value)> {
    let (parsed, raw) = report::fetch_since_with_root(kache, cache_dir, "365d", Some(root))?;

    let json_path = out_dir.join(format!("report-{phase}.json"));
    std::fs::write(&json_path, serde_json::to_string_pretty(&raw)? + "\n")
        .with_context(|| format!("writing {}", json_path.display()))?;

    let md = Command::new(kache)
        .args([
            "report", "--format", "markdown", "--since", "365d", "--root",
        ])
        .arg(root)
        .env("KACHE_CACHE_DIR", cache_dir)
        .output()
        .with_context(|| format!("running `{} report --format markdown`", kache.display()))?;
    if md.status.success() {
        let md_path = out_dir.join(format!("report-{phase}.md"));
        std::fs::write(&md_path, md.stdout)
            .with_context(|| format!("writing {}", md_path.display()))?;
    }

    let trace = Command::new(kache)
        .args([
            "report", "--format", "perfetto", "--since", "365d", "--root",
        ])
        .arg(root)
        .env("KACHE_CACHE_DIR", cache_dir)
        .output()
        .with_context(|| format!("running `{} report --format perfetto`", kache.display()))?;
    if trace.status.success() {
        let trace_path = out_dir.join(format!("trace-{phase}.json"));
        std::fs::write(&trace_path, trace.stdout)
            .with_context(|| format!("writing {}", trace_path.display()))?;
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
    daemon::stop(kache, cache_dir);
    if cache_dir.exists() {
        std::fs::remove_dir_all(cache_dir).context("clearing cache dir")?;
    }
    std::fs::create_dir_all(cache_dir)?;
    daemon::start(kache, cache_dir, kache_config);
    let cold_s = build(
        profile,
        clone_a,
        Phase::Cold.name(),
        cache_dir,
        kache_config,
        kache,
        work_dir,
        CacheBackend::Kache,
        trace_keys,
        sh,
    )?;
    // Dump before stop: process-lifetime counters die with the daemon.
    write_cache_otlp_or_warn(
        kache,
        cache_dir,
        kache_config,
        &work_dir.join("cache-otlp-cold"),
        &profile.name,
        "cold",
    );
    daemon::stop(kache, cache_dir);
    let (cold, cold_raw) = capture_report(kache, cache_dir, work_dir, Phase::Cold.name(), clone_a)?;
    // Read the raw event log *before* the caller's reset — it carries the
    // passthrough events `kache report` filters out of `all_events`.
    let cold_events = read_event_log(event_log);
    let (cold_leaks, _) =
        scan_leak_warnings(&work_dir.join(format!("wrapper-{}.log", Phase::Cold.name())));

    // Snapshot the cache in its post-cold state so a future `--retry`
    // run can restore it instead of re-running cold.
    source::snapshot_dir(cache_dir, &work_dir.join("cache-after-cold"))?;

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
    daemon::stop(kache, cache_dir);
    source::snapshot_dir(&snapshot, cache_dir)?;

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

/// Temporal "daily pull" bench: clone-a is built cold at `git_ref`, then the
/// SAME worktree is checked out to `ref_next` and rebuilt from scratch. The
/// pull phase's hit rate is how much of the full TU set survived the source
/// delta. No cross-clone `warm`, no `clone-b`.
#[allow(clippy::too_many_arguments)]
fn run_pull_bench(
    profile: &BenchProfile,
    kache: &Path,
    cache_dir: &Path,
    kache_config: &Path,
    clone_ref: &Path,
    clone_a: &Path,
    work_dir: &Path,
    event_log: &Path,
    objdir: &str,
    run_id: &str,
    run_archive_dir: &Path,
    cache_tool_version: Option<&str>,
    force_setup: bool,
    trace_keys: bool,
    sh: &Path,
) -> Result<()> {
    let ref_next = profile
        .ref_next
        .as_deref()
        .expect("run_pull_bench requires ref_next");

    // Materialize clone-a at ref A with ref B also fetched (checkout-ready).
    source::clone_worktrees_pull(
        &profile.repo,
        &profile.git_ref,
        ref_next,
        clone_ref,
        clone_a,
    )?;
    run_setup(profile, clone_a, kache, force_setup, sh)?;
    profile.apply_files(clone_a, kache)?;

    // cold: empty cache, build ref A in clone-a -> populate.
    let (cold_metrics, _cold_raw) = run_cold_phase(
        profile,
        kache,
        cache_dir,
        kache_config,
        clone_a,
        work_dir,
        event_log,
        trace_keys,
        sh,
    )?;

    // Reset the event log so the pull report covers only the pull build.
    if event_log.exists() {
        std::fs::remove_file(event_log)
            .with_context(|| format!("resetting {}", event_log.display()))?;
    }

    // Advance the SAME worktree to ref B (the "daily pull") and re-assert the
    // injected files (mozconfig etc.), since checkout may touch tracked files.
    run(Command::new("git")
        .args(["-c", "core.longpaths=true"])
        .arg("-C")
        .arg(clone_a)
        .args(["checkout", "--detach", ref_next]))?;
    profile.apply_files(clone_a, kache)?;

    // pull: same cache, same path, new source. `build()` wipes the objdir at
    // the start of every phase (unconditionally), so this is a from-scratch
    // rebuild at ref_next and kache is asked about every TU.
    daemon::start(kache, cache_dir, kache_config);
    let pull_s = build(
        profile,
        clone_a,
        Phase::Pull.name(),
        cache_dir,
        kache_config,
        kache,
        work_dir,
        CacheBackend::Kache,
        trace_keys,
        sh,
    )?;
    write_cache_otlp_or_warn(
        kache,
        cache_dir,
        kache_config,
        &work_dir.join("cache-otlp-pull"),
        &profile.name,
        "pull",
    );
    daemon::stop(kache, cache_dir);

    let (pull, pull_raw) = capture_report(kache, cache_dir, work_dir, Phase::Pull.name(), clone_a)?;
    let pull_events = read_event_log(event_log);
    let (pull_leaks, _pull_leak_samples) =
        scan_leak_warnings(&work_dir.join(format!("wrapper-{}.log", Phase::Pull.name())));
    let pull_metrics = PhaseMetrics::from_report(&pull, &pull_raw, pull_s, pull_events, pull_leaks);

    // No cross-clone comparison: an empty KeyStability (compared == 0) makes
    // Verdict skip the key-stability check; the pull scenarios also set no
    // min_key_stability_pct.
    let no_stability = KeyStability {
        stable_pct: 0.0,
        stable: 0,
        compared: 0,
    };
    let verdict = Verdict::evaluate(
        &no_stability,
        &pull_metrics,
        profile.checks.assertions.for_phase(Phase::Pull.name()),
    );
    // speedup is meaningless for a temporal rebuild; pass 0.0 so only
    // min_hit_rate_pct / max_wall_s warnings can fire.
    let measure_warnings = bench_measure_warnings(
        &pull_metrics,
        0.0,
        profile.checks.measure.for_phase(Phase::Pull.name()),
    );

    let cold_objdir_bytes = dir_size_kb(&clone_a.join(objdir)).saturating_mul(1024);
    let pull_objdir_bytes = cold_objdir_bytes; // same path; objdir was wiped+rebuilt

    let result = PullBenchResult {
        project: profile.name.clone(),
        git_ref: profile.git_ref.clone(),
        ref_next: ref_next.to_string(),
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        run_id: run_id.to_string(),
        artifact_dir: run_archive_dir.display().to_string(),
        cache_tool_version: cache_tool_version.map(String::from),
        cold: cold_metrics,
        pull: pull_metrics,
        cache_size_mb: round1(dir_size_kb(cache_dir) as f64 / 1024.0),
        cold_objdir_bytes,
        pull_objdir_bytes,
        verdict,
        measure_warnings,
        reports: [
            "report-cold.json",
            "report-cold.md",
            "report-pull.json",
            "report-pull.md",
            "build-cold.log",
            "build-pull.log",
            "wrapper-cold.log",
            "wrapper-pull.log",
        ]
        .map(String::from)
        .to_vec(),
    };

    let out = work_dir.join(format!("{}.json", profile.name));
    std::fs::write(&out, serde_json::to_string_pretty(&result)? + "\n")
        .with_context(|| format!("writing {}", out.display()))?;
    write_otlp_or_warn(
        work_dir,
        crate::bench_otlp::OtlpRun {
            project: result.project.clone(),
            git_ref: result.git_ref.clone(),
            cache_tool: "kache",
            time_unix_nano: crate::bench_otlp::OtlpRun::now_unix_nano(),
            verdict_ok: result.verdict.ok,
            speedup: None,
            cache_size_bytes: crate::bench_otlp::OtlpRun::bytes_from_mib(result.cache_size_mb),
            key_stability_pct: None,
            disk_measured_bytes: None,
            phases: vec![
                otlp_phase("cold", &result.cold, result.cold_objdir_bytes),
                otlp_phase("pull", &result.pull, result.pull_objdir_bytes),
            ],
        },
    );
    archive_run_artifacts(work_dir, run_archive_dir)?;
    print_pull_summary(&result, run_archive_dir);
    eprintln!("[bench] summary written to {}", out.display());

    if !result.verdict.ok {
        std::process::exit(1);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_sccache_bench(
    profile: &BenchProfile,
    sccache: &Path,
    cache_dir: &Path,
    clone_a: &Path,
    clone_b: &Path,
    work_dir: &Path,
    retry: bool,
    sh: &Path,
    disk_free_before: Option<u64>,
    objdir: &str,
    run_id: &str,
    run_archive_dir: &Path,
    cache_tool_version: Option<&str>,
) -> Result<()> {
    let cold_metrics = if retry {
        retry_load_sccache_cold(&profile.name, cache_dir, work_dir)?
    } else {
        run_sccache_cold_phase(profile, sccache, cache_dir, clone_a, work_dir, sh)?
    };
    ensure_sccache_cache_location(&cold_metrics, cache_dir, Phase::Cold.name())?;
    ensure_sccache_base_dirs(&cold_metrics, clone_a, Phase::Cold.name())?;

    sccache_start(sccache, cache_dir, clone_b)?;
    sccache_zero_stats(sccache, cache_dir, clone_b)?;
    let warm_s = build(
        profile,
        clone_b,
        Phase::Warm.name(),
        cache_dir,
        &work_dir.join("sccache-config-unused.toml"),
        sccache,
        work_dir,
        CacheBackend::Sccache,
        false,
        sh,
    )?;
    let warm_metrics = capture_sccache_report(
        sccache,
        cache_dir,
        clone_b,
        work_dir,
        Phase::Warm.name(),
        warm_s,
    )?;
    sccache_stop(sccache, cache_dir);
    ensure_sccache_cache_location(&warm_metrics, cache_dir, Phase::Warm.name())?;
    ensure_sccache_base_dirs(&warm_metrics, clone_b, Phase::Warm.name())?;

    let disk_measured_bytes = match (disk_free_before, available_bytes(work_dir)) {
        (Some(before), Some(after)) => before.checked_sub(after),
        _ => None,
    };
    let speedup = if warm_s > 0 {
        cold_metrics.wall_s as f64 / warm_s as f64
    } else {
        0.0
    };
    let measure_warnings = sccache_measure_warnings(
        &warm_metrics,
        speedup,
        profile.checks.measure.for_phase(Phase::Warm.name()),
    );
    let cold_objdir_bytes = dir_size_kb(&clone_a.join(objdir)).saturating_mul(1024);
    let warm_objdir_bytes = dir_size_kb(&clone_b.join(objdir)).saturating_mul(1024);
    let cache_dir_bytes = dir_size_kb(cache_dir).saturating_mul(1024);
    let sum_apparent_bytes = cold_objdir_bytes
        .saturating_add(warm_objdir_bytes)
        .saturating_add(cache_dir_bytes);

    let result = SccacheBenchResult {
        project: profile.name.clone(),
        git_ref: profile.git_ref.clone(),
        platform: format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
        cache_backend: CacheBackend::Sccache.label().to_string(),
        run_id: run_id.to_string(),
        artifact_dir: run_archive_dir.display().to_string(),
        cache_tool_version: cache_tool_version.map(ToString::to_string),
        sccache_version: warm_metrics
            .version
            .clone()
            .or_else(|| cold_metrics.version.clone()),
        cache_location: warm_metrics
            .cache_location
            .clone()
            .or_else(|| cold_metrics.cache_location.clone()),
        cold: cold_metrics,
        warm: warm_metrics,
        speedup: round2(speedup),
        cache_size_mb: round1(cache_dir_bytes as f64 / 1024.0 / 1024.0),
        cache_dir_bytes,
        cold_objdir_bytes,
        warm_objdir_bytes,
        sum_apparent_bytes,
        disk_measured_bytes,
        measure_warnings,
        reports: [
            "report-cold.sccache.json",
            "report-cold.sccache-adv.txt",
            "report-warm.sccache.json",
            "report-warm.sccache-adv.txt",
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
    write_otlp_or_warn(
        work_dir,
        crate::bench_otlp::OtlpRun {
            project: result.project.clone(),
            git_ref: result.git_ref.clone(),
            cache_tool: "sccache",
            time_unix_nano: crate::bench_otlp::OtlpRun::now_unix_nano(),
            verdict_ok: true,
            speedup: Some(result.speedup),
            cache_size_bytes: result.cache_dir_bytes,
            key_stability_pct: None,
            disk_measured_bytes: result.disk_measured_bytes,
            phases: vec![
                otlp_sccache_phase("cold", &result.cold, result.cold_objdir_bytes),
                otlp_sccache_phase("warm", &result.warm, result.warm_objdir_bytes),
            ],
        },
    );

    archive_run_artifacts(work_dir, run_archive_dir)?;
    print_sccache_summary(&result, run_archive_dir);
    eprintln!("[bench] summary written to {}", out.display());
    Ok(())
}

fn run_sccache_cold_phase(
    profile: &BenchProfile,
    sccache: &Path,
    cache_dir: &Path,
    clone_a: &Path,
    work_dir: &Path,
    sh: &Path,
) -> Result<SccachePhaseMetrics> {
    sccache_stop(sccache, cache_dir);
    if cache_dir.exists() {
        std::fs::remove_dir_all(cache_dir).context("clearing sccache dir")?;
    }
    std::fs::create_dir_all(cache_dir)?;
    sccache_start(sccache, cache_dir, clone_a)?;
    sccache_zero_stats(sccache, cache_dir, clone_a)?;
    let cold_s = build(
        profile,
        clone_a,
        Phase::Cold.name(),
        cache_dir,
        &work_dir.join("sccache-config-unused.toml"),
        sccache,
        work_dir,
        CacheBackend::Sccache,
        false,
        sh,
    )?;
    let cold_metrics = capture_sccache_report(
        sccache,
        cache_dir,
        clone_a,
        work_dir,
        Phase::Cold.name(),
        cold_s,
    )?;
    sccache_stop(sccache, cache_dir);

    source::snapshot_dir(cache_dir, &work_dir.join("cache-after-cold"))?;
    Ok(cold_metrics)
}

fn retry_load_sccache_cold(
    profile_name: &str,
    cache_dir: &Path,
    work_dir: &Path,
) -> Result<SccachePhaseMetrics> {
    let snapshot = work_dir.join("cache-after-cold");
    let result_json = work_dir.join(format!("{profile_name}.json"));
    for required in [&snapshot, &result_json] {
        if !required.exists() {
            bail!(
                "--retry: required artifact missing — {} (run `just bench-sccache {profile_name}` once first)",
                required.display()
            );
        }
    }
    eprintln!(
        "\n[bench] [retry] restoring cold-state sccache dir from {}",
        snapshot.display()
    );
    source::snapshot_dir(&snapshot, cache_dir)?;

    let prev: serde_json::Value = read_json(&result_json)?;
    serde_json::from_value(prev["cold"].clone()).with_context(|| {
        format!(
            "loading previous cold sccache metrics from {}",
            result_json.display()
        )
    })
}

fn sccache_start(sccache: &Path, cache_dir: &Path, base_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(cache_dir)?;
    let _ = std::fs::remove_file(sccache_server_uds(cache_dir));
    let mut cmd = sccache_command(sccache, cache_dir, Some(base_dir));
    cmd.arg("--start-server");
    run(&mut cmd)
}

fn sccache_zero_stats(sccache: &Path, cache_dir: &Path, base_dir: &Path) -> Result<()> {
    let mut cmd = sccache_command(sccache, cache_dir, Some(base_dir));
    cmd.arg("--zero-stats");
    run(&mut cmd)
}

fn sccache_stop(sccache: &Path, cache_dir: &Path) {
    let _ = sccache_command(sccache, cache_dir, None)
        .arg("--stop-server")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = std::fs::remove_file(sccache_server_uds(cache_dir));
}

fn sccache_command(sccache: &Path, cache_dir: &Path, base_dir: Option<&Path>) -> Command {
    let mut cmd = Command::new(sccache);
    cmd.env("SCCACHE_DIR", cache_dir)
        .env("SCCACHE_CACHE_SIZE", SCCACHE_BENCH_CACHE_SIZE)
        .env("SCCACHE_IDLE_TIMEOUT", SCCACHE_BENCH_IDLE_TIMEOUT_SECS)
        .env("SCCACHE_SERVER_UDS", sccache_server_uds(cache_dir));
    if let Some(base_dir) = base_dir {
        cmd.env("SCCACHE_BASEDIRS", base_dir);
    }
    cmd
}

fn sccache_server_uds(cache_dir: &Path) -> PathBuf {
    cache_dir
        .parent()
        .unwrap_or(cache_dir)
        .join("sccache-server.sock")
}

fn ensure_sccache_cache_location(
    metrics: &SccachePhaseMetrics,
    cache_dir: &Path,
    phase: &str,
) -> Result<()> {
    let Some(location) = &metrics.cache_location else {
        return Ok(());
    };
    let expected = cache_dir.display().to_string();
    if !location.contains(&expected) {
        bail!(
            "[{phase}] sccache used cache location `{location}`, expected `{expected}`; \
             benchmark result is invalid"
        );
    }
    Ok(())
}

fn ensure_sccache_base_dirs(
    metrics: &SccachePhaseMetrics,
    base_dir: &Path,
    phase: &str,
) -> Result<()> {
    let expected = normalize_sccache_base_dir(base_dir);
    if metrics
        .base_dirs
        .iter()
        .any(|dir| normalize_sccache_base_dir(Path::new(dir)) == expected)
    {
        return Ok(());
    }
    bail!(
        "[{phase}] sccache used base dirs [{}], expected `{expected}`; \
         benchmark result is invalid",
        metrics.base_dirs.join(", ")
    );
}

fn normalize_sccache_base_dir(path: &Path) -> String {
    path.display()
        .to_string()
        .trim_end_matches(std::path::MAIN_SEPARATOR)
        .to_string()
}

fn capture_sccache_report(
    sccache: &Path,
    cache_dir: &Path,
    base_dir: &Path,
    out_dir: &Path,
    phase: &str,
    wall_s: u64,
) -> Result<SccachePhaseMetrics> {
    let output = sccache_command(sccache, cache_dir, Some(base_dir))
        .args(["--show-stats", "--stats-format", "json"])
        .output()
        .with_context(|| format!("running `{} --show-stats`", sccache.display()))?;
    if !output.status.success() {
        bail!(
            "sccache --show-stats failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let raw: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("parsing sccache stats JSON")?;
    let json_path = out_dir.join(format!("report-{phase}.sccache.json"));
    std::fs::write(&json_path, serde_json::to_string_pretty(&raw)? + "\n")
        .with_context(|| format!("writing {}", json_path.display()))?;

    let adv = sccache_command(sccache, cache_dir, Some(base_dir))
        .arg("--show-adv-stats")
        .output()
        .with_context(|| format!("running `{} --show-adv-stats`", sccache.display()))?;
    if adv.status.success() {
        let adv_path = out_dir.join(format!("report-{phase}.sccache-adv.txt"));
        std::fs::write(&adv_path, adv.stdout)
            .with_context(|| format!("writing {}", adv_path.display()))?;
    }

    Ok(SccachePhaseMetrics::from_raw(&raw, wall_s))
}

/// Drop the event log so the next phase's report covers only that phase.
///
/// Only the observability log goes; the cache store (`store/`, `index.db`)
/// stays, so the next build still hits everything its predecessor populated.
fn reset_event_log(event_log: &Path) -> Result<()> {
    if event_log.exists() {
        std::fs::remove_file(event_log)
            .with_context(|| format!("resetting {}", event_log.display()))?;
    }
    Ok(())
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
    // Keyed by (action, structured reason). The reason is the kache-emitted
    // `category|detail`; print time splits it into the category / description
    // columns.
    let mut reasons: HashMap<(String, String), u64> = HashMap::new();
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
                let reason = ev["passthrough_reason"].as_str().unwrap_or_default();
                // A probe / query (`category` == `not-a-compile`) is not a
                // compile kache failed to cache — count it separately so the
                // passthrough rate stays the actionable refusal signal.
                if reason.split('|').next().unwrap_or_default().trim() == "not-a-compile" {
                    stats.probed += 1;
                } else {
                    stats.passed_through += 1;
                }
                if !reason.is_empty() {
                    // `action`: what kache did with the compile — delegated to a
                    // configured fallback (e.g. sccache) or just ran the real
                    // compiler. Keyed alongside the reason so the two dispositions
                    // never merge into one row.
                    let action = if ev["fallback"].as_bool().unwrap_or(false) {
                        "fallback"
                    } else {
                        "reject"
                    };
                    *reasons
                        .entry((action.to_string(), reason.to_string()))
                        .or_default() += 1;
                }
            }
            "error" => stats.errored += 1,
            _ => {}
        }
    }
    let mut top: Vec<ReasonCount> = reasons
        .into_iter()
        .map(|((action, reason), count)| ReasonCount {
            action,
            reason,
            count,
        })
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
/// key-input the hasher consumed. Lines look like:
///
/// ```text
/// 2026-05-24T16:45:23.123Z TRACE kache::cache_key: [key:gkrust] env_dep:CARGO_MANIFEST_DIR=/abs/path
/// ```
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
/// `link_lib_content:static=NAME=HASH (PATH)` has the same shape of noise:
/// `cache_key.rs` hashes only `link_lib_content:` + the content hash, but the
/// trace line appends the lib name and absolute `.a` path for readability. Two
/// clones with byte-identical libs at different build paths would otherwise
/// over-report as diverging (#470 — `ring`/`zstd_sys`/… show as divergences yet
/// cache-hit). Strip ONLY the trailing ` (PATH)` context, keeping
/// `link_lib_content:static=NAME=HASH`: the path is the per-clone noise, while
/// keeping the lib name preserves both `field_of` bucketing (the aggregate
/// groups under `link_lib_content:static`, not per-hash) and the name↔content
/// association. Genuine content differences (e.g. `quickjs`, `jemalloc`, which
/// bake the build path into the archive) keep distinct hashes and still diverge.
fn normalize_payload(payload: &str) -> String {
    if let Some(rest) = payload.strip_prefix("link_lib_content:") {
        // The path is the trailing ` (PATH)` group. Split on the FIRST ` (`:
        // the lib name (a linker `-l` spec) has no ` (`, so the first one is the
        // hash→path separator even when the PATH itself contains ` (` or `=`.
        let body = rest.split(" (").next().unwrap_or(rest);
        return format!("link_lib_content:{body}");
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

/// How many filtered TU-matching artifacts to keep as samples in the report.
/// A handful is enough for a human to judge whether the filter hid a real key
/// leak; the full set is just noise (and could be large on a unified build).
const FILTERED_TU_SAMPLE_CAP: usize = 20;

#[derive(Debug, Serialize)]
struct KeyDivergence {
    diverging_crates: u64,
    diverging_fields: u64,
    /// Crates dropped as unified-build TU-matching artifacts.
    filtered_tu_artifacts: u64,
    /// A capped SAMPLE of the filtered crates, with the tokens that differed.
    /// Filtered artifacts are excluded from the headline `diverging_*` counts
    /// (they are usually genuine Firefox unified-build noise), but they must
    /// not be invisible: a real path leak whose only divergence is a bare
    /// relative `resolved_token` would otherwise be filtered AND hidden,
    /// reporting "0 diverging fields" while the cache silently over-keys (this
    /// is exactly how the cc dependency-file keyrace masked itself). Surfacing
    /// a sample lets a reviewer confirm the filtered set is really noise.
    filtered_tu_samples: Vec<KeyCrateDiff>,
    aggregate_by_field: Vec<KeyFieldAggregate>,
    by_crate: Vec<KeyCrateDiff>,
}

/// True when a crate's only divergence is bare relative `resolved_token`
/// filenames: a Firefox unified-build TU-matching artifact, not a real key
/// change. Anything with a path or kache sentinel is kept as a real signal.
fn is_tu_matching_artifact(only_cold: &[String], only_warm: &[String]) -> bool {
    let mut any_token = false;
    for p in only_cold.iter().chain(only_warm.iter()) {
        if p.starts_with("final=") {
            continue;
        }
        let Some(val) = p.strip_prefix("resolved_token=") else {
            return false;
        };
        if val.starts_with('/') || val.contains('<') {
            return false;
        }
        any_token = true;
    }
    any_token
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
    let mut filtered_tu_artifacts = 0;
    let mut filtered_tu_samples: Vec<KeyCrateDiff> = Vec::new();
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

        if is_tu_matching_artifact(&only_cold, &only_warm) {
            filtered_tu_artifacts += 1;
            // Keep the divergent tokens so the filtered set is auditable, not
            // silently dropped. Sorted + capped after the loop for a stable,
            // bounded report (HashMap iteration order is non-deterministic).
            filtered_tu_samples.push(KeyCrateDiff {
                crate_name: name.clone(),
                only_in_cold: only_cold,
                only_in_warm: only_warm,
            });
            continue;
        }

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

    // Stable, bounded sample of the filtered set (HashMap order is random).
    filtered_tu_samples.sort_by(|a, b| a.crate_name.cmp(&b.crate_name));
    filtered_tu_samples.truncate(FILTERED_TU_SAMPLE_CAP);

    KeyDivergence {
        diverging_crates: by_crate.len() as u64,
        diverging_fields,
        filtered_tu_artifacts,
        filtered_tu_samples,
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
        "- diverging input fields: **{}**\n",
        diff.diverging_fields
    ));
    md.push_str(&format!(
        "- filtered unified-build TU-matching artifacts: **{}** (not counted above)\n\n",
        diff.filtered_tu_artifacts
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

    // Surface a sample of the filtered TU-matching artifacts so the filter
    // can't silently hide a real key leak (the cc dependency-file keyrace
    // diverged ONLY in a bare relative `resolved_token`, so it was filtered
    // AND invisible — the report read "0 diverging fields"). A reviewer can
    // scan these to confirm they are genuine unified-build noise.
    if !diff.filtered_tu_samples.is_empty() {
        md.push_str(&format!(
            "## Filtered TU-matching artifacts (sample of {}, not counted above)\n\n",
            diff.filtered_tu_artifacts
        ));
        md.push_str(
            "These crates were excluded as unified-build TU-matching noise — their only \
             divergence is a bare relative `resolved_token`. Listed so a real path leak \
             cannot hide behind the filter; scan for anything that is not a build-layout \
             filename.\n\n",
        );
        for c in &diff.filtered_tu_samples {
            md.push_str(&format!("### `{}`\n\n", c.crate_name));
            for p in c.only_in_cold.iter().take(4) {
                md.push_str(&format!(
                    "- cold: `{}`\n",
                    p.chars().take(160).collect::<String>()
                ));
            }
            for p in c.only_in_warm.iter().take(4) {
                md.push_str(&format!(
                    "- warm: `{}`\n",
                    p.chars().take(160).collect::<String>()
                ));
            }
            md.push('\n');
        }
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

/// Run a command with inherited stdio, failing on a non-zero exit.
fn run(cmd: &mut Command) -> Result<()> {
    let status = cmd.status().with_context(|| format!("spawning {cmd:?}"))?;
    if !status.success() {
        bail!("command failed ({status}): {cmd:?}");
    }
    Ok(())
}

/// Apparent size of `dir` in KiB: recursive file lengths, not following
/// symlinks. Returns 0 on error; this is a reported metric, not load-bearing.
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

    let mut bytes = 0;
    walk(dir, &mut bytes);
    bytes / 1024
}

/// Available bytes on the filesystem backing `path`, via `df -kP`.
///
/// `-kP` forces 1024-byte blocks and the POSIX one-line-per-filesystem format,
/// so the "Available" column is field 4 on both macOS and Linux. Returns
/// `None` if `df` is missing or unparseable (e.g. Windows) — the caller treats
/// a missing measurement as "skip the free-space verify", never as an error.
fn available_bytes(path: &Path) -> Option<u64> {
    let out = Command::new("df").arg("-kP").arg(path).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    // Line 0 is the header; the first data line's 4th field is available KiB.
    let avail_kb: u64 = text
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(avail_kb.saturating_mul(1024))
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

#[derive(Debug, Serialize)]
struct SccacheBenchResult {
    project: String,
    git_ref: String,
    platform: String,
    cache_backend: String,
    run_id: String,
    artifact_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_tool_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sccache_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_location: Option<String>,
    cold: SccachePhaseMetrics,
    warm: SccachePhaseMetrics,
    speedup: f64,
    cache_size_mb: f64,
    cache_dir_bytes: u64,
    cold_objdir_bytes: u64,
    warm_objdir_bytes: u64,
    sum_apparent_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    disk_measured_bytes: Option<u64>,
    measure_warnings: Vec<String>,
    reports: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SccachePhaseMetrics {
    wall_s: u64,
    compile_requests: u64,
    requests_executed: u64,
    requests_not_compile: u64,
    requests_not_cacheable: u64,
    non_cacheable_compilations: u64,
    cache_hits: u64,
    cache_misses: u64,
    cache_writes: u64,
    cache_errors: u64,
    cache_read_errors: u64,
    cache_write_errors: u64,
    hit_rate_pct: f64,
    top_not_cached: Vec<SccacheCount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_cache_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_location: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    base_dirs: Vec<String>,
}

impl SccachePhaseMetrics {
    fn from_raw(raw: &serde_json::Value, wall_s: u64) -> Self {
        let stats = &raw["stats"];
        let cache_hits = sum_count_values(&stats["cache_hits"]["counts"]);
        let cache_misses = sum_count_values(&stats["cache_misses"]["counts"]);
        let denominator = cache_hits.saturating_add(cache_misses);
        let hit_rate_pct = if denominator > 0 {
            cache_hits as f64 / denominator as f64 * 100.0
        } else {
            0.0
        };
        SccachePhaseMetrics {
            wall_s,
            compile_requests: stats["compile_requests"].as_u64().unwrap_or(0),
            requests_executed: stats["requests_executed"].as_u64().unwrap_or(0),
            requests_not_compile: stats["requests_not_compile"].as_u64().unwrap_or(0),
            requests_not_cacheable: stats["requests_not_cacheable"].as_u64().unwrap_or(0),
            non_cacheable_compilations: stats["non_cacheable_compilations"].as_u64().unwrap_or(0),
            cache_hits,
            cache_misses,
            cache_writes: stats["cache_writes"].as_u64().unwrap_or(0),
            cache_errors: sum_count_values(&stats["cache_errors"]["counts"]),
            cache_read_errors: stats["cache_read_errors"].as_u64().unwrap_or(0),
            cache_write_errors: stats["cache_write_errors"].as_u64().unwrap_or(0),
            hit_rate_pct: round1(hit_rate_pct),
            top_not_cached: top_sccache_counts(&stats["not_cached"], 8),
            cache_size_bytes: raw["cache_size"].as_u64(),
            max_cache_size_bytes: raw["max_cache_size"].as_u64(),
            version: raw["version"].as_str().map(ToString::to_string),
            cache_location: raw["cache_location"].as_str().map(ToString::to_string),
            base_dirs: raw["basedirs"]
                .as_array()
                .map(|dirs| {
                    dirs.iter()
                        .filter_map(|dir| dir.as_str().map(ToString::to_string))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SccacheCount {
    reason: String,
    count: u64,
}

fn sum_count_values(value: &serde_json::Value) -> u64 {
    match value {
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
        serde_json::Value::Object(map) => map.values().map(sum_count_values).sum(),
        serde_json::Value::Array(items) => items.iter().map(sum_count_values).sum(),
        _ => 0,
    }
}

fn top_sccache_counts(value: &serde_json::Value, limit: usize) -> Vec<SccacheCount> {
    let mut counts = Vec::new();
    collect_sccache_counts(value, "", &mut counts);
    counts.sort_by_key(|count| std::cmp::Reverse(count.count));
    counts.truncate(limit);
    counts
}

fn collect_sccache_counts(value: &serde_json::Value, prefix: &str, out: &mut Vec<SccacheCount>) {
    match value {
        serde_json::Value::Number(n) => {
            let count = n.as_u64().unwrap_or(0);
            if count > 0 && !prefix.is_empty() {
                out.push(SccacheCount {
                    reason: prefix.to_string(),
                    count,
                });
            }
        }
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                let next = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                collect_sccache_counts(value, &next, out);
            }
        }
        _ => {}
    }
}

fn sccache_measure_warnings(
    warm: &SccachePhaseMetrics,
    speedup: f64,
    measure: Option<&MeasureSpec>,
) -> Vec<String> {
    let Some(measure) = measure else {
        return Vec::new();
    };
    let mut warnings = Vec::new();
    if let Some(max) = measure.max_wall_s
        && warm.wall_s > max
    {
        warnings.push(format!(
            "warm wall time {}s exceeded configured warning threshold {}s",
            warm.wall_s, max
        ));
    }
    if let Some(min) = measure.min_hit_rate_pct
        && warm.hit_rate_pct < min
    {
        warnings.push(format!(
            "warm hit rate {:.1}% below configured warning threshold {:.1}%",
            warm.hit_rate_pct, min
        ));
    }
    if let Some(min) = measure.min_speedup
        && speedup < min
    {
        warnings.push(format!(
            "speedup {:.2}x below configured warning threshold {:.2}x",
            speedup, min
        ));
    }
    warnings
}

fn print_sccache_summary(r: &SccacheBenchResult, work_dir: &Path) {
    let bar = "=".repeat(64);
    let fmt = |s: u64| format!("{}m {:02}s", s / 60, s % 60);
    eprintln!("\n{bar}");
    eprintln!("  sccache benchmark: {} — {}", r.project, r.git_ref);
    if let Some(version) = &r.cache_tool_version {
        eprintln!("  cache tool : {version}");
    }
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
        "  warm cache : {} hits / {} misses   {:.1}% hit rate",
        r.warm.cache_hits, r.warm.cache_misses, r.warm.hit_rate_pct
    );
    eprintln!(
        "  requests   : {} compile requests / {} executed / {} not cacheable / {} not compile",
        r.warm.compile_requests,
        r.warm.requests_executed,
        r.warm.requests_not_cacheable,
        r.warm.requests_not_compile,
    );
    eprintln!("{bar}");
    eprintln!("  disk layout — three independent pools");
    eprintln!(
        "    clone-a/obj   {:>10}   cold-built objdir",
        human_bytes(r.cold_objdir_bytes)
    );
    eprintln!(
        "    clone-b/obj   {:>10}   warm-built objdir",
        human_bytes(r.warm_objdir_bytes)
    );
    eprintln!(
        "    sccache       {:>10}   cache dir{}",
        human_bytes(r.cache_dir_bytes),
        r.cache_location
            .as_ref()
            .map(|location| format!(" ({location})"))
            .unwrap_or_default()
    );
    eprintln!("                  ──────────");
    eprintln!(
        "    sum apparent  {:>10}   cold obj + warm obj + sccache cache",
        human_bytes(r.sum_apparent_bytes)
    );
    if let Some(measured) = r.disk_measured_bytes {
        eprintln!(
            "    measured      {:>10}   actual free-space delta (cold+warm builds)",
            human_bytes(measured)
        );
    }
    eprintln!("{bar}");
    if !r.warm.top_not_cached.is_empty() {
        eprintln!("  top not cached:");
        for entry in r.warm.top_not_cached.iter().take(5) {
            eprintln!("    {:>6}x  {}", entry.count, entry.reason);
        }
        eprintln!("{bar}");
    }
    if !r.measure_warnings.is_empty() {
        eprintln!("  MEASURE WARNINGS:");
        for warning in &r.measure_warnings {
            eprintln!("    - {warning}");
        }
        eprintln!("{bar}");
    }
    eprintln!(
        "  detailed reports: {}/{{report,build,wrapper}}-{{cold,warm}}.*",
        work_dir.display()
    );
    eprintln!("{bar}");
}

/// Render the end-of-run benchmark summary a human reads.
///
/// Writes rather than prints so the numbers it reports can be asserted in a
/// test. That matters for the three wall-clocks at the top: the per-PR perf
/// gate compares exactly those, and a summary that quietly stopped reporting
/// one of them would leave the gate reading a blank.
fn write_summary(
    out: &mut dyn std::io::Write,
    r: &BenchResult,
    work_dir: &Path,
) -> std::io::Result<()> {
    let bar = "=".repeat(64);
    let fmt = |s: u64| format!("{}m {:02}s", s / 60, s % 60);
    writeln!(out, "\n{bar}")?;
    writeln!(out, "  kache benchmark: {} — {}", r.project, r.git_ref)?;
    if let Some(version) = &r.cache_tool_version {
        writeln!(out, "  cache tool : {version}")?;
    }
    writeln!(out, "{bar}")?;
    writeln!(
        out,
        "  cold build : {}   (empty cache, baseline)",
        fmt(r.cold.wall_s)
    )?;
    // Only when `--warm-same-tree` ran: the plain "cleaned my target/" case,
    // same absolute path, printed between cold and the cross-clone warm so the
    // three wall-clocks read in the order they were measured.
    if let Some(same_tree) = &r.warm_same_tree {
        writeln!(
            out,
            "  same-tree  : {}   (clone-a rebuilt, objdir wiped, cache populated by cold)",
            fmt(same_tree.wall_s)
        )?;
        writeln!(
            out,
            "               {} hits / {} misses, {:.1}% hit rate, restored {}{}",
            same_tree.hits,
            same_tree.misses,
            same_tree.hit_rate_pct,
            human_bytes(same_tree.storage.restored_bytes),
            r.warm_same_tree_speedup
                .map(|s| format!("   {s:.2}x vs cold"))
                .unwrap_or_default(),
        )?;
    }
    writeln!(
        out,
        "  warm build : {}   (cache populated by cold; DIFFERENT worktree path)",
        fmt(r.warm.wall_s)
    )?;
    writeln!(out, "  speedup    : {:.2}x", r.speedup)?;
    writeln!(
        out,
        "  warm cache : {} hits / {} dups / {} misses   {:.1}% hit rate ({:.1}% weighted)",
        r.warm.hits, r.warm.dups, r.warm.misses, r.warm.hit_rate_pct, r.warm.weighted_hit_rate_pct
    )?;
    let el = &r.warm.event_log;
    let cacheable = el.hit_bytes.saturating_add(el.miss_bytes);
    let bytes_cov_pct = if cacheable > 0 {
        (el.hit_bytes as f64 / cacheable as f64) * 100.0
    } else {
        0.0
    };
    writeln!(
        out,
        "  bytes      : {} cached / {} recompiled   ({:.1}% of cacheable bytes)",
        human_bytes(el.hit_bytes),
        human_bytes(el.miss_bytes),
        bytes_cov_pct,
    )?;
    writeln!(out, "{bar}")?;

    // ── restore: bytes that flowed cache → warm clone, and how ──
    let st = &r.warm.storage;
    writeln!(
        out,
        "  restore    : warm build pulled {} from the cache",
        human_bytes(st.restored_bytes)
    )?;
    writeln!(
        out,
        "               reflink {} / hardlink {} / copy {}   ({:.1}% zero-copy)",
        human_bytes(st.reflinked_bytes),
        human_bytes(st.hardlinked_bytes),
        human_bytes(st.copied_bytes),
        st.zero_copy_pct,
    )?;
    writeln!(out, "{bar}")?;

    // ── disk layout: the three pools and what sharing buys ──
    // Apparent = sum of file lengths. The same physical blocks appear in more
    // than one pool's apparent size whenever kache CoW-reflinked (or, on a
    // non-CoW filesystem, hardlinked) them, so the naive sum triple-counts
    // shared content. Three distinct sharings exist, and each is now a
    // measured byte count (not an untallied side effect):
    //   • warm RESTORE reflink/hardlink — clone-b/obj bytes that share with
    //                             the cache
    //   • cold STORE reflink/hardlink — cache blobs that share with
    //                             clone-a/obj (the blob was cloned or linked
    //                             from the cold build's output)
    //   • warm STORE reflink/hardlink — cache blobs from warm misses that
    //                             share with clone-b/obj
    // Subtracting all three from the apparent sum yields the real footprint.
    let cold_st = &r.cold.storage;
    let store_shared = cold_st
        .store_reflinked_bytes
        .saturating_add(cold_st.store_hardlinked_bytes)
        .saturating_add(st.store_reflinked_bytes)
        .saturating_add(st.store_hardlinked_bytes);
    let shared_total = st
        .reflinked_bytes
        .saturating_add(st.hardlinked_bytes)
        .saturating_add(store_shared);
    let sum_apparent = r
        .cold_objdir_bytes
        .saturating_add(r.warm_objdir_bytes)
        .saturating_add(st.blob_bytes);
    let approx_on_disk = sum_apparent.saturating_sub(shared_total);
    let restore_shared = st.reflinked_bytes.saturating_add(st.hardlinked_bytes);
    writeln!(
        out,
        "  disk layout — three pools, sharing via reflink/hardlink"
    )?;
    writeln!(
        out,
        "    clone-a/obj   {:>10}   cold-built objdir",
        human_bytes(r.cold_objdir_bytes)
    )?;
    writeln!(
        out,
        "    clone-b/obj   {:>10}   warm-built; {} shared from cache",
        human_bytes(r.warm_objdir_bytes),
        human_bytes(restore_shared),
    )?;
    writeln!(
        out,
        "    kache cache   {:>10}   {} blobs; {} shared from build output, {} copied",
        human_bytes(st.blob_bytes),
        st.store_blobs,
        human_bytes(store_shared),
        human_bytes(
            cold_st
                .store_copied_bytes
                .saturating_add(st.store_copied_bytes)
        ),
    )?;
    writeln!(
        out,
        "                  {:>10}   ({} dedup vs {} raw)",
        "",
        human_bytes(st.dedup_saved_bytes),
        human_bytes(st.logical_bytes),
    )?;
    writeln!(out, "                  ──────────")?;
    writeln!(
        out,
        "    sum apparent  {:>10}   what 3 independent pools would cost",
        human_bytes(sum_apparent),
    )?;
    writeln!(
        out,
        "    ≈ on disk    ~{:>10}   zero-copy sharing saves {} ({} restore + {} store)",
        human_bytes(approx_on_disk),
        human_bytes(shared_total),
        human_bytes(restore_shared),
        human_bytes(store_shared),
    )?;
    if let Some(measured) = r.disk_measured_bytes {
        // Ground truth: the volume's free-space delta across the two build
        // phases. CoW/dedup/compression-honest and filesystem-agnostic — it
        // VERIFIES the estimate above rather than trusting kache's own tally.
        writeln!(
            out,
            "    measured      {:>10}   actual free-space delta (cold+warm builds)",
            human_bytes(measured),
        )?;
    }
    writeln!(out, "{bar}")?;

    // ── diagnostics: did the run actually exercise kache? ──
    writeln!(
        out,
        "  key stability : {:.1}%   ({} of {} crates kept an identical key across clones)",
        r.key_stability.stable_pct, r.key_stability.stable, r.key_stability.compared
    )?;
    if let Some(top) = &r.key_diff_top {
        writeln!(
            out,
            "  key diff      : top diverging input → {}   (see key-diff.{{json,md}})",
            top
        )?;
    }
    writeln!(
        out,
        "  kache saw     : {} compiles -> {} cached, {} passed through, {} errored",
        el.total, el.cached, el.passed_through, el.errored
    )?;
    if !el.top_passthrough.is_empty() {
        // Decision table, not an error list: `action  category  description`.
        // The header says the build ran normally so the rows don't read as
        // failures. Fixed-width action/category columns align; the variable
        // description trails last.
        writeln!(
            out,
            "  passthrough   : top reasons (kache declined to cache; the build ran normally) —"
        )?;
        for rc in el.top_passthrough.iter().take(5) {
            // Reason is the kache-emitted `category|detail`; older logs without
            // the `|` fall back to category "—" and the whole string as detail.
            let (category, detail) = rc
                .reason
                .split_once('|')
                .unwrap_or(("—", rc.reason.as_str()));
            let action = if rc.action.is_empty() {
                "reject"
            } else {
                rc.action.as_str()
            };
            let detail: String = detail.chars().take(64).collect();
            writeln!(
                out,
                "    {:>6}x  {:<8}  {:<14}  {}",
                rc.count, action, category, detail
            )?;
        }
    }
    writeln!(
        out,
        "  leak detector : {} firing(s) (kache PathNormalizer; see wrapper-warm.log)",
        r.warm.leak_warnings
    )?;
    for sample in r.warm_leak_samples.iter().take(3) {
        let s: String = sample.chars().take(96).collect();
        writeln!(out, "    {s}")?;
    }

    if !r.warm.top_misses.is_empty() {
        writeln!(out, "{bar}")?;
        writeln!(
            out,
            "  costliest warm misses (recompiled despite the cache):"
        )?;
        for m in r.warm.top_misses.iter().take(5) {
            writeln!(out, "    {:>8}  {}", fmt(m.compile_time_s), m.crate_name)?;
        }
    }

    writeln!(out, "{bar}")?;
    if r.verdict.ok && r.verdict.checks.is_empty() {
        writeln!(out, "  VERDICT: ok — no blocking assertions configured.")?;
    } else if r.verdict.ok {
        writeln!(out, "  VERDICT: ok — the run validly exercised kache.")?;
    } else {
        writeln!(
            out,
            "  VERDICT: DEGRADED RUN — results are NOT a valid measurement:"
        )?;
        for issue in &r.verdict.issues {
            writeln!(out, "    - {issue}")?;
        }
    }
    if let Some(same_tree) = &r.warm_same_tree_verdict {
        if same_tree.ok {
            writeln!(
                out,
                "  VERDICT (same-tree): ok — the phase validly consumed the cache."
            )?;
        } else {
            writeln!(
                out,
                "  VERDICT (same-tree): DEGRADED — that phase measured nothing real:"
            )?;
            for issue in &same_tree.issues {
                writeln!(out, "    - {issue}")?;
            }
        }
    }
    if !r.measure_warnings.is_empty() {
        writeln!(out, "  MEASURE WARNINGS:")?;
        for warning in &r.measure_warnings {
            writeln!(out, "    - {warning}")?;
        }
    }
    writeln!(out, "{bar}")?;
    writeln!(
        out,
        "  detailed reports: {}/{{report,build,wrapper}}-{{cold,warm}}.*",
        work_dir.display()
    )?;
    writeln!(out, "{bar}")?;
    Ok(())
}

fn print_pull_summary(r: &PullBenchResult, archive_dir: &Path) {
    eprintln!("\n===== {} (daily pull) =====", r.project);
    eprintln!("platform    : {}", r.platform);
    eprintln!("ref (cold)  : {}", r.git_ref);
    eprintln!("ref_next    : {}", r.ref_next);
    eprintln!(
        "cold        : {} crates, {:.1}% hit, {}s wall",
        r.cold.total_crates, r.cold.hit_rate_pct, r.cold.wall_s
    );
    eprintln!(
        "pull        : {} crates, {:.1}% hit ({:.1}% weighted), {}s wall, {} passthrough",
        r.pull.total_crates,
        r.pull.hit_rate_pct,
        r.pull.weighted_hit_rate_pct,
        r.pull.wall_s,
        r.pull.event_log.passed_through,
    );
    eprintln!("cache size  : {:.1} MiB", r.cache_size_mb);
    if r.verdict.ok {
        eprintln!("VERDICT     : ok");
    } else {
        eprintln!("VERDICT     : DEGRADED");
        for issue in &r.verdict.issues {
            eprintln!("  - {issue}");
        }
    }
    for w in &r.measure_warnings {
        eprintln!("  warn: {w}");
    }
    eprintln!("artifacts   : {}", archive_dir.display());
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
    /// Stable per-run id used for the archived artifact directory.
    run_id: String,
    /// Timestamped copy of this run's logs/reports. Root-level files remain
    /// the latest run for `--retry`.
    artifact_dir: String,
    /// Output of `<cache-tool> --version`.
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_tool_version: Option<String>,
    cold: PhaseMetrics,
    /// The same-worktree warm rebuild, present only under `--warm-same-tree`.
    /// Absent from every nightly scenario's JSON, so downstream readers must
    /// treat it as optional.
    #[serde(skip_serializing_if = "Option::is_none")]
    warm_same_tree: Option<PhaseMetrics>,
    warm: PhaseMetrics,
    /// Cold wall-clock divided by warm wall-clock.
    speedup: f64,
    /// Cold wall-clock divided by the same-tree warm wall-clock.
    #[serde(skip_serializing_if = "Option::is_none")]
    warm_same_tree_speedup: Option<f64>,
    cache_size_mb: f64,
    /// Apparent bytes of clone-a's objdir after the cold build.
    cold_objdir_bytes: u64,
    /// Apparent bytes of clone-b's objdir after the warm build.
    /// Of these, ~`warm.storage.reflinked_bytes` are CoW-shared with the
    /// cache on APFS — print_summary subtracts that to report "unique
    /// to this clone".
    warm_objdir_bytes: u64,
    /// Real disk the cold+warm builds consumed, measured as the volume's
    /// free-space delta across both phases. CoW/dedup/compression-honest and
    /// filesystem-agnostic — the ground-truth check on the `approx_on_disk`
    /// estimate. `None` on `--retry` (cold is restored, not built, so the
    /// delta wouldn't cover the full three-pool layout).
    #[serde(skip_serializing_if = "Option::is_none")]
    disk_measured_bytes: Option<u64>,
    /// Cross-clone cache-key stability — the deterministic correctness
    /// signal.
    key_stability: KeyStability,
    /// Sample lines from kache's leak detector during the warm build.
    warm_leak_samples: Vec<String>,
    /// Whether the run validly exercised kache (drives the exit code).
    verdict: Verdict,
    /// The same-tree phase's own verdict, from
    /// `[checks.assert.warm-same-tree]`. Also drives the exit code.
    #[serde(skip_serializing_if = "Option::is_none")]
    warm_same_tree_verdict: Option<Verdict>,
    /// Advisory measurement threshold warnings. Never drives exit code.
    measure_warnings: Vec<String>,
    /// Top diverging key-input field across clones, set only when the
    /// run was invoked with `--trace-keys`. The full per-crate diff
    /// lives in `key-diff.json` / `key-diff.md`; this is the headline
    /// for the run summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    key_diff_top: Option<String>,
    /// Detailed per-phase artifacts written next to this file.
    reports: Vec<String>,
}

/// Result of a temporal "daily pull" bench: cold at `git_ref`, then a
/// forced-clean rebuild at `ref_next` in the SAME worktree. There is no
/// cross-clone `warm` phase and no `key_stability` — the signal is the pull
/// phase's own hit rate (how much of the full TU set survived the source
/// delta) plus its passthrough/verdict.
#[derive(Debug, Serialize)]
struct PullBenchResult {
    project: String,
    git_ref: String,
    ref_next: String,
    platform: String,
    run_id: String,
    artifact_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_tool_version: Option<String>,
    cold: PhaseMetrics,
    pull: PhaseMetrics,
    cache_size_mb: f64,
    cold_objdir_bytes: u64,
    pull_objdir_bytes: u64,
    verdict: Verdict,
    measure_warnings: Vec<String>,
    reports: Vec<String>,
}

/// Per-phase metrics. Each phase's report is isolated (the event log is
/// reset between cold and warm), so these numbers reflect that phase
/// alone — no cumulative bleed.
#[derive(Debug, Serialize, Deserialize, Default)]
struct PhaseMetrics {
    wall_s: u64,
    total_crates: u64,
    hits: u64,
    dups: u64,
    misses: u64,
    /// Wrapped-compiler non-zero exits this phase (kache `EventResult::Error`).
    /// This is NOT a kache cache fault: kache routes every store/restore/key/
    /// lock failure to a *passthrough* (bounded by `max_passthrough_pct`), and
    /// only ever records `Error` when the compiler it wrapped exits non-zero. In
    /// a build that completed successfully those are tolerated build-script
    /// feature probes (`rustc --crate-name probe …`). Surfaced for visibility;
    /// it does NOT drive the verdict — see [`Verdict::evaluate`].
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
            dups: s.dups,
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
#[derive(Debug, Serialize, Deserialize, Default)]
struct StorageInfo {
    reflinked_bytes: u64,
    hardlinked_bytes: u64,
    copied_bytes: u64,
    restored_bytes: u64,
    /// Share of restored bytes that cost no physical copy.
    zero_copy_pct: f64,
    /// Bytes that entered the store by a CoW reflink — they share blocks with
    /// the build's own output file, so the store is NOT a second copy of them.
    /// The disk-layout estimate subtracts these (they're double-counted in the
    /// apparent objdir + cache sum).
    store_reflinked_bytes: u64,
    /// Bytes that entered the store by a hardlink (non-CoW filesystem,
    /// immutable artifact kinds) — one inode appearing in both the objdir and
    /// the store, so like reflinked bytes they are subtracted from the
    /// estimate.
    store_hardlinked_bytes: u64,
    /// Bytes that entered the store by a full copy (no CoW) — a genuine second
    /// copy, so they are NOT subtracted from the on-disk estimate.
    store_copied_bytes: u64,
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
            store_reflinked_bytes: u("store_reflinked_bytes"),
            store_hardlinked_bytes: u("store_hardlinked_bytes"),
            store_copied_bytes: u("store_copied_bytes"),
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
    /// Compiles kache declined to cache and ran straight through. Excludes
    /// probe / query invocations (`--print`, `-vV`, `cc -###`) — those are
    /// not compiles, so counting them here would inflate the passthrough
    /// rate the `max_passthrough_pct` gate checks. See `probed`.
    passed_through: u64,
    /// Query / probe invocations that ran uncached — split out of
    /// `passed_through` (a probe is not a compilation).
    probed: u64,
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
    /// What kache did: `reject` (ran the real compiler) or `fallback`
    /// (delegated to a configured fallback wrapper, e.g. sccache).
    #[serde(default)]
    action: String,
    /// The kache-emitted structured reason, `category|detail`.
    reason: String,
    count: u64,
}

/// Cross-clone cache-key stability.
#[derive(Debug, Serialize, Default)]
struct KeyStability {
    /// Percentage of compared crates that produced an identical key set.
    stable_pct: f64,
    /// Crates whose key was identical across the two clones.
    stable: u64,
    /// Crates kache cached in both clones (the comparison denominator).
    compared: u64,
}

fn bench_measure_warnings(
    warm: &PhaseMetrics,
    speedup: f64,
    measure: Option<&MeasureSpec>,
) -> Vec<String> {
    let Some(measure) = measure else {
        return Vec::new();
    };
    let mut warnings = Vec::new();
    if let Some(max) = measure.max_wall_s
        && warm.wall_s > max
    {
        warnings.push(format!(
            "warm wall time {}s exceeded configured warning threshold {}s",
            warm.wall_s, max
        ));
    }
    if let Some(min) = measure.min_hit_rate_pct
        && warm.hit_rate_pct < min
    {
        warnings.push(format!(
            "warm hit rate {:.1}% below configured warning threshold {:.1}%",
            warm.hit_rate_pct, min
        ));
    }
    if let Some(min) = measure.min_speedup
        && speedup < min
    {
        warnings.push(format!(
            "speedup {:.2}x below configured warning threshold {:.2}x",
            speedup, min
        ));
    }
    warnings
}

/// Whether the run validly exercised kache. A degraded run exits non-zero
/// so a broken run can't pass as a real measurement.
#[derive(Debug, Serialize)]
struct Verdict {
    ok: bool,
    issues: Vec<String>,
    checks: Vec<AssertionCheck>,
}

impl Verdict {
    /// Gate thresholds. A run is degraded if cross-clone key stability
    /// collapsed (the warm phase measured nothing real), if most compiles never
    /// reached the cache, or if the phase reported no hits / restored no bytes.
    ///
    /// Applied to one phase's metrics at a time: the cross-clone `warm` phase
    /// against `[checks.assert.warm]`, and — when `--warm-same-tree` ran — the
    /// same-tree phase against `[checks.assert.warm-same-tree]`. Pass a default
    /// [`KeyStability`] for the latter; a zero denominator skips the
    /// cross-clone-only stability check.
    ///
    /// `max_errors` is intentionally NOT a degradation trigger. kache's
    /// `EventResult::Error` counts only wrapped-compiler non-zero exits — never
    /// a kache cache fault (those route to passthrough, gated by
    /// `max_passthrough_pct`). Reaching this verdict means the build itself
    /// succeeded, so any such non-zero compiles are tolerated build-script
    /// feature probes (`rustc --crate-name probe …` testing whether something
    /// compiles): they produce no artifact and are not a caching problem. The
    /// count is surfaced for visibility but never fails the run; genuine kache
    /// faults are caught by `max_passthrough_pct`, key stability, and the leak
    /// detector.
    fn evaluate(
        stability: &KeyStability,
        warm: &PhaseMetrics,
        spec: Option<&ScenarioAssertSpec>,
    ) -> Self {
        let mut checks = Vec::new();
        let mut issues = Vec::new();

        let Some(spec) = spec else {
            return Verdict {
                ok: true,
                issues,
                checks,
            };
        };

        if let Some(min_key_stability_pct) = spec.min_key_stability_pct
            && stability.compared > 0
        {
            if stability.stable_pct < min_key_stability_pct {
                checks.push(AssertionCheck {
                    name: "min_key_stability_pct",
                    expected: format!(">= {min_key_stability_pct:.1}"),
                    actual: format!("{:.1}", stability.stable_pct),
                    passed: false,
                });
                issues.push(format!(
                    "cross-clone key stability {:.1}% — the cache key is not \
                     path-portable; the warm phase did not validly measure caching",
                    stability.stable_pct
                ));
            } else {
                checks.push(AssertionCheck {
                    name: "min_key_stability_pct",
                    expected: format!(">= {min_key_stability_pct:.1}"),
                    actual: format!("{:.1}", stability.stable_pct),
                    passed: true,
                });
            }
        }

        // Did this phase measure anything at all? A phase that reported zero
        // hits, or hits that restored no bytes, recompiled the world — its
        // wall-clock is a build time, not a cache time, and a comparison
        // against it would be a flattering number rather than a measurement.
        if let Some(min_hits) = spec.min_hits {
            let passed = warm.hits >= min_hits;
            checks.push(AssertionCheck {
                name: "min_hits",
                expected: format!(">= {min_hits}"),
                actual: warm.hits.to_string(),
                passed,
            });
            if !passed {
                issues.push(format!(
                    "{} cache hits (need >= {min_hits}) — this phase did not consume \
                     the cache, so its wall-clock measures a rebuild, not a restore",
                    warm.hits
                ));
            }
        }

        if let Some(min_restored_bytes) = spec.min_restored_bytes {
            let restored = warm.storage.restored_bytes;
            let passed = restored >= min_restored_bytes;
            checks.push(AssertionCheck {
                name: "min_restored_bytes",
                expected: format!(">= {min_restored_bytes}"),
                actual: restored.to_string(),
                passed,
            });
            if !passed {
                issues.push(format!(
                    "{} restored from the store (need >= {}) — the phase reported hits \
                     but no output files landed in the build tree",
                    human_bytes(restored),
                    human_bytes(min_restored_bytes),
                ));
            }
        }

        let el = &warm.event_log;
        if let Some(max_passthrough_pct) = spec.max_passthrough_pct
            && el.total > 0
        {
            let pt_pct = el.passed_through as f64 / el.total as f64 * 100.0;
            if pt_pct > max_passthrough_pct {
                checks.push(AssertionCheck {
                    name: "max_passthrough_pct",
                    expected: format!("<= {max_passthrough_pct:.1}"),
                    actual: format!("{pt_pct:.1}"),
                    passed: false,
                });
                issues.push(format!(
                    "{:.0}% of compiles ({} of {}) passed through uncached — \
                     kache barely exercised",
                    pt_pct, el.passed_through, el.total
                ));
            } else {
                checks.push(AssertionCheck {
                    name: "max_passthrough_pct",
                    expected: format!("<= {max_passthrough_pct:.1}"),
                    actual: format!("{pt_pct:.1}"),
                    passed: true,
                });
            }
        }

        if spec.max_errors.is_some() {
            // `warm.errors` (kache `EventResult::Error`) counts wrapped-compiler
            // non-zero exits, NOT kache cache faults — see this fn's doc-comment.
            // The build succeeded to reach here, so any are tolerated build-
            // script probes. Record the count for visibility; never fail on it.
            checks.push(AssertionCheck {
                name: "max_errors",
                expected: "0 kache cache faults".to_string(),
                actual: format!(
                    "0 cache faults ({} tolerated wrapped-compile failure(s))",
                    warm.errors
                ),
                passed: true,
            });
        }

        Verdict {
            ok: all_passed(&checks),
            issues,
            checks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A warm phase with tolerated wrapped-compile failures (build-script
    /// feature probes that rustc rejected — kache `EventResult::Error`) must
    /// NOT degrade the run: those are not kache cache faults. Regression guard
    /// for kunobi-ninja/kache (SurrealDB bench surfaced one such probe).
    #[test]
    fn run_archive_keeps_otlp_sidecars() {
        assert!(is_run_artifact("metrics.otlp.json"));
        assert!(is_run_artifact("schema_version"));
        assert!(is_run_artifact("bench-firefox.json"));
        assert!(!is_run_artifact("kache.log"));
    }

    #[test]
    fn write_cache_otlp_or_warn_runs_telemetry_write() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("invoked.txt");
        let kache = fake_kache_that_records(dir.path(), &marker, 0);
        let dest = dir.path().join("cache-otlp-warm");
        assert!(write_cache_otlp_or_warn(
            &kache,
            dir.path(),
            &dir.path().join("kache.toml"),
            &dest,
            "bench-firefox",
            "warm",
        ));
        let invoked = std::fs::read_to_string(&marker).unwrap();
        assert!(
            invoked.contains("telemetry"),
            "expected telemetry subcommand, got {invoked:?}"
        );
        assert!(
            invoked.contains("write"),
            "expected write subcommand, got {invoked:?}"
        );
        assert!(
            invoked.contains("bench-firefox") && invoked.contains("warm"),
            "expected scenario and phase, got {invoked:?}"
        );
    }

    #[test]
    fn write_cache_otlp_or_warn_is_false_when_kache_exits_nonzero() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("invoked.txt");
        let kache = fake_kache_that_records(dir.path(), &marker, 1);
        assert!(!write_cache_otlp_or_warn(
            &kache,
            dir.path(),
            &dir.path().join("kache.toml"),
            &dir.path().join("cache-otlp-cold"),
            "bench-firefox",
            "cold",
        ));
        assert!(marker.is_file());
    }

    #[test]
    fn write_cache_otlp_or_warn_is_false_when_kache_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!write_cache_otlp_or_warn(
            &dir.path().join("no-such-kache"),
            dir.path(),
            &dir.path().join("kache.toml"),
            &dir.path().join("cache-otlp-pull"),
            "bench-firefox",
            "pull",
        ));
    }

    fn fake_kache_that_records(dir: &Path, marker: &Path, exit: i32) -> PathBuf {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let path = dir.join("fake-kache");
            std::fs::write(
                &path,
                format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$0\" \"$@\" > '{}'\nexit {exit}\n",
                    marker.display()
                ),
            )
            .unwrap();
            std::fs::set_permissions(&path, PermissionsExt::from_mode(0o755)).unwrap();
            path
        }
        #[cfg(windows)]
        {
            let path = dir.join("fake-kache.cmd");
            std::fs::write(
                &path,
                format!(
                    "@echo off\r\necho %* > \"{}\"\r\nexit /b {exit}\r\n",
                    marker.display()
                ),
            )
            .unwrap();
            path
        }
    }

    #[test]
    fn write_otlp_or_warn_writes_artifact_pair() {
        let dir = tempfile::tempdir().unwrap();
        write_otlp_or_warn(
            dir.path(),
            crate::bench_otlp::OtlpRun {
                project: "bench-test".into(),
                git_ref: "main".into(),
                cache_tool: "kache",
                time_unix_nano: "1".into(),
                verdict_ok: true,
                speedup: None,
                cache_size_bytes: 0,
                key_stability_pct: None,
                disk_measured_bytes: None,
                phases: Vec::new(),
            },
        );
        assert!(dir.path().join(crate::bench_otlp::METRICS_FILE).is_file());
        assert!(
            dir.path()
                .join(crate::bench_otlp::SCHEMA_VERSION_FILE)
                .is_file()
        );
    }

    #[test]
    fn wrapped_compile_failures_do_not_degrade_the_run() {
        let stability = KeyStability {
            stable_pct: 96.9,
            stable: 560,
            compared: 578,
        };
        let warm = PhaseMetrics {
            // 5 probes the compiler rejected — these used to flip the verdict.
            errors: 5,
            event_log: EventLogStats {
                total: 800,
                cached: 700,
                passed_through: 100,
                ..Default::default()
            },
            ..Default::default()
        };
        let spec = ScenarioAssertSpec {
            min_key_stability_pct: Some(50.0),
            max_passthrough_pct: Some(40.0),
            max_errors: Some(0),
            ..Default::default()
        };

        let verdict = Verdict::evaluate(&stability, &warm, Some(&spec));

        assert!(
            verdict.ok,
            "tolerated probe failures must not degrade the run: {:?}",
            verdict.issues
        );
        let errs = verdict
            .checks
            .iter()
            .find(|c| c.name == "max_errors")
            .expect("max_errors check is still surfaced");
        assert!(
            errs.passed,
            "max_errors must pass on tolerated probe failures"
        );
        assert!(
            errs.actual.contains('5'),
            "the probe-failure count is surfaced for visibility: {}",
            errs.actual
        );
    }

    #[test]
    fn resolve_binary_prefers_existing_then_exe() {
        let dir = std::env::temp_dir().join(format!("kb-resolvebin-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.exe"), b"x").unwrap();
        assert_eq!(resolve_binary(&dir.join("foo")), dir.join("foo.exe"));
        std::fs::write(dir.join("bar"), b"x").unwrap();
        assert_eq!(resolve_binary(&dir.join("bar")), dir.join("bar"));
        assert_eq!(resolve_binary(&dir.join("nope")), dir.join("nope"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn posix_sh_resolves_a_shell() {
        let sh = posix_sh().expect("a POSIX sh should be resolvable on the test host");
        let name = sh.file_name().unwrap().to_string_lossy().to_lowercase();
        assert!(name.starts_with("sh"), "unexpected shell: {}", sh.display());
    }

    #[test]
    fn de_verbatim_strips_windows_extended_prefix() {
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
    fn normalize_payload_preserves_keyed_source_identity() {
        // Source path-to-content association is a real key input (#760), so
        // diagnostics must not discard it as display-only noise.
        assert_eq!(
            normalize_payload("source:/Users/a/clone-a/foo.rs=254dfb8084fc2e3f"),
            "source:/Users/a/clone-a/foo.rs=254dfb8084fc2e3f"
        );
        assert_ne!(
            normalize_payload("source:/Users/a/clone-a/foo.rs=254dfb8084fc2e3f"),
            normalize_payload("source:/Users/b/clone-b/foo.rs=254dfb8084fc2e3f"),
        );
        // Non-source payloads pass through untouched.
        assert_eq!(
            normalize_payload("env_dep:CARGO_MANIFEST_DIR=/p"),
            "env_dep:CARGO_MANIFEST_DIR=/p"
        );
        assert_eq!(normalize_payload("final=abcdef"), "final=abcdef");
    }

    #[test]
    fn normalize_payload_strips_display_only_link_lib_path() {
        // `link_lib_content:static=NAME=HASH (PATH)` is display-only: the
        // hasher consumes ONLY the content hash (cache_key.rs hashes
        // `link_lib_content:` + content_hash). The lib name and absolute
        // `.a` path are context, so two clones with identical content but
        // different build paths must normalize to the same payload — else
        // the key-diff over-reports them as cross-clone divergences (#470).
        // The path is stripped; the lib name + content hash are kept (so the
        // aggregate still buckets under `link_lib_content:static`, and the
        // name↔content association survives — cross-family review caught that
        // reducing to hash-only would break `field_of`).
        assert_eq!(
            normalize_payload(
                "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a \
                 (/x/clone-a/target/release/build/ring-abc/out/libring_core_0_17_14_.a)"
            ),
            "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a"
        );
        // Same content, different clone path → identical normalized payload
        // (this is the false-divergence the issue is about).
        assert_eq!(
            normalize_payload(
                "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a (/clone-a/out/libring.a)"
            ),
            normalize_payload(
                "link_lib_content:static=ring_core_0_17_14_=218693c3a2c8618a (/clone-b/out/libring.a)"
            ),
        );
        // Genuinely different content (quickjs) must STAY divergent.
        assert_ne!(
            normalize_payload(
                "link_lib_content:static=quickjs=834656493be21f86 (/clone-a/out/libquickjs.a)"
            ),
            normalize_payload(
                "link_lib_content:static=quickjs=3dc0f4dbcc36e69f (/clone-b/out/libquickjs.a)"
            ),
        );
        // A path containing '=' or ' (' must not corrupt the kept name/hash.
        assert_eq!(
            normalize_payload("link_lib_content:static=foo=deadbeef (/odd=dir (x)/out/libfoo.a)"),
            "link_lib_content:static=foo=deadbeef"
        );
        // No trailing path (defensive) passes through unchanged.
        assert_eq!(
            normalize_payload("link_lib_content:static=foo=deadbeef"),
            "link_lib_content:static=foo=deadbeef"
        );
        // Regression guard (cross-family review): the normalized payload must
        // still bucket under the `link_lib_content:static` field, not per-hash.
        assert_eq!(
            field_of(&normalize_payload(
                "link_lib_content:static=quickjs=834656493be21f86 (/clone-a/out/libquickjs.a)"
            )),
            "link_lib_content:static"
        );
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
            source::clone_ref_path(Path::new("./tmp/bench")),
            PathBuf::from("./tmp/bench-clone-ref")
        );
        assert_eq!(
            source::clone_ref_path(Path::new("/scratch/foo")),
            PathBuf::from("/scratch/foo-clone-ref")
        );
        // Sanity: the reference path is not nested inside work_dir.
        let work = Path::new("./tmp/bench");
        let r = source::clone_ref_path(work);
        assert!(
            !r.starts_with(work),
            "clone-ref must not live under work_dir, got {r:?}"
        );
    }

    #[test]
    fn default_work_dir_is_per_scenario_under_tmp_bench() {
        assert_eq!(
            default_work_dir("substrate"),
            PathBuf::from("./tmp/bench/substrate")
        );
        assert_eq!(
            default_work_dir("firefox"),
            PathBuf::from("./tmp/bench/firefox")
        );
        // clone-ref stays a sibling WITHIN ./tmp/bench (not under work_dir), so
        // `rm -rf ./tmp/bench/<scenario>` spares the clone and `rm -rf tmp/bench`
        // wipes every scenario at once.
        let wd = default_work_dir("substrate");
        assert_eq!(
            source::clone_ref_path(&wd),
            PathBuf::from("./tmp/bench/substrate-clone-ref")
        );
        assert!(!source::clone_ref_path(&wd).starts_with(&wd));
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
    fn compute_key_divergence_drops_tu_matching_artifacts() {
        let mut cold: HashMap<String, Vec<String>> = HashMap::new();
        let mut warm: HashMap<String, Vec<String>> = HashMap::new();

        cold.insert(
            "Unified_cpp_x0.cpp".into(),
            vec![
                "resolved_token=foo.cpp".into(),
                "resolved_token=.deps/foo.o.pp".into(),
                "final=AAAA".into(),
            ],
        );
        warm.insert(
            "Unified_cpp_x0.cpp".into(),
            vec![
                "resolved_token=bar.cpp".into(),
                "resolved_token=.deps/bar.o.pp".into(),
                "final=BBBB".into(),
            ],
        );
        cold.insert(
            "real_cc.cpp".into(),
            vec![
                "resolved_token=<BASE_DIR>/a/foo.h".into(),
                "final=CCCC".into(),
            ],
        );
        warm.insert(
            "real_cc.cpp".into(),
            vec![
                "resolved_token=<BASE_DIR>/b/foo.h".into(),
                "final=DDDD".into(),
            ],
        );

        let diff = compute_key_divergence(&cold, &warm);

        assert_eq!(diff.filtered_tu_artifacts, 1);
        assert_eq!(diff.diverging_crates, 1);
        assert_eq!(diff.by_crate[0].crate_name, "real_cc.cpp");
        // The filtered artifact is excluded from the headline count but kept
        // as an auditable sample (with its divergent tokens) — never silently
        // dropped, so a real leak can't hide behind the filter.
        assert_eq!(diff.filtered_tu_samples.len(), 1);
        assert_eq!(diff.filtered_tu_samples[0].crate_name, "Unified_cpp_x0.cpp");
        let toks: Vec<&str> = diff.filtered_tu_samples[0]
            .only_in_cold
            .iter()
            .chain(diff.filtered_tu_samples[0].only_in_warm.iter())
            .map(String::as_str)
            .collect();
        assert!(
            toks.iter()
                .any(|t| t.contains("foo.cpp") || t.contains("bar.cpp")),
            "filtered sample must retain the divergent tokens: {toks:?}"
        );
    }

    fn phase_metrics_for_verdict(errors: u64) -> PhaseMetrics {
        PhaseMetrics {
            wall_s: 1,
            total_crates: 10,
            hits: 6,
            dups: 0,
            misses: 4,
            errors,
            hit_rate_pct: 60.0,
            weighted_hit_rate_pct: 60.0,
            time_saved_s: 0,
            top_misses: Vec::new(),
            event_log: EventLogStats {
                total: 10,
                cached: 7,
                passed_through: 3,
                probed: 0,
                errored: 0,
                top_passthrough: Vec::new(),
                hit_bytes: 0,
                miss_bytes: 0,
            },
            leak_warnings: 0,
            storage: StorageInfo {
                reflinked_bytes: 0,
                hardlinked_bytes: 0,
                copied_bytes: 0,
                restored_bytes: 0,
                zero_copy_pct: 0.0,
                store_reflinked_bytes: 0,
                store_hardlinked_bytes: 0,
                store_copied_bytes: 0,
                store_blobs: 0,
                logical_bytes: 0,
                blob_bytes: 0,
                dedup_saved_bytes: 0,
            },
        }
    }

    #[test]
    fn verdict_has_no_hidden_assertions_without_configured_checks() {
        let stability = KeyStability {
            stable_pct: 0.0,
            stable: 0,
            compared: 4,
        };
        let warm = phase_metrics_for_verdict(99);

        let verdict = Verdict::evaluate(&stability, &warm, None);

        assert!(verdict.ok);
        assert!(verdict.checks.is_empty());
        assert!(verdict.issues.is_empty());
    }

    #[test]
    fn verdict_uses_only_configured_assert_thresholds() {
        let stability = KeyStability {
            stable_pct: 0.0,
            stable: 0,
            compared: 4,
        };
        let warm = phase_metrics_for_verdict(1);
        let spec = ScenarioAssertSpec {
            min_key_stability_pct: None,
            max_passthrough_pct: None,
            max_errors: Some(0),
            ..Default::default()
        };

        let verdict = Verdict::evaluate(&stability, &warm, Some(&spec));

        // Only max_errors is configured, so it's the only check produced. It
        // records the wrapped-compile failure count but never degrades the run —
        // those are tolerated build-script probes, not kache cache faults.
        assert!(verdict.ok);
        assert_eq!(verdict.checks.len(), 1);
        assert_eq!(verdict.checks[0].name, "max_errors");
        assert!(verdict.checks[0].passed);
    }

    #[test]
    fn verdict_uses_configured_assert_thresholds() {
        let stability = KeyStability {
            stable_pct: 75.0,
            stable: 3,
            compared: 4,
        };
        let warm = phase_metrics_for_verdict(0);
        let spec = ScenarioAssertSpec {
            min_key_stability_pct: Some(80.0),
            max_passthrough_pct: Some(20.0),
            max_errors: Some(0),
            ..Default::default()
        };

        let verdict = Verdict::evaluate(&stability, &warm, Some(&spec));

        assert!(!verdict.ok);
        let failed: Vec<&str> = verdict
            .checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| check.name)
            .collect();
        assert_eq!(failed, vec!["min_key_stability_pct", "max_passthrough_pct"]);
    }

    /// The PR perf gate's core safety property: a phase that recompiled the
    /// world must be rejected, not reported as a fast build. Zero hits and zero
    /// restored bytes are the two ways "this measured nothing" shows up.
    #[test]
    fn verdict_rejects_a_phase_that_consumed_no_cache() {
        let spec = ScenarioAssertSpec {
            min_hits: Some(1),
            min_restored_bytes: Some(1024),
            ..Default::default()
        };
        let measured_nothing = PhaseMetrics::default();

        // A default `KeyStability` is how the same-tree phase is evaluated:
        // stability is cross-clone only, so it must stay out of the verdict.
        let verdict = Verdict::evaluate(&KeyStability::default(), &measured_nothing, Some(&spec));

        assert!(!verdict.ok, "a zero-hit phase must not pass");
        assert_eq!(
            failed_check_names(&verdict),
            vec!["min_hits", "min_restored_bytes"]
        );
        assert!(
            verdict
                .checks
                .iter()
                .all(|check| check.name != "min_key_stability_pct"),
            "stability must be skipped when nothing was compared"
        );
        // Both failures must be explained. The issue list is what the summary
        // prints and what a reviewer reads; a verdict that fails without saying
        // why is barely more useful than one that passes wrongly.
        assert_eq!(verdict.issues.len(), 2, "{:?}", verdict.issues);
        assert!(
            verdict.issues[0].contains("cache hits"),
            "{:?}",
            verdict.issues
        );
        assert!(
            verdict.issues[1].contains("restored from the store"),
            "{:?}",
            verdict.issues
        );
    }

    /// A phase that hit the cache and pulled real bytes out of it passes both
    /// validity checks — at the exact floor and above it. The floors are
    /// inclusive: `min_hits = N` means N hits is enough.
    #[test]
    fn verdict_accepts_a_phase_that_hit_and_restored() {
        let spec = ScenarioAssertSpec {
            min_hits: Some(412),
            min_restored_bytes: Some(1024),
            ..Default::default()
        };
        // Exactly at both floors — the boundary an off-by-one moves.
        let at_the_floor = PhaseMetrics {
            hits: 412,
            storage: StorageInfo {
                restored_bytes: 1024,
                ..Default::default()
            },
            ..Default::default()
        };
        let comfortably_over = PhaseMetrics {
            hits: 4_120,
            storage: StorageInfo {
                restored_bytes: 64 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        };

        for measured in [at_the_floor, comfortably_over] {
            let verdict = Verdict::evaluate(&KeyStability::default(), &measured, Some(&spec));

            assert!(verdict.ok, "{:?}", verdict.issues);
            assert_eq!(verdict.checks.len(), 2);
            assert!(verdict.checks.iter().all(|check| check.passed));
            // A passing check must not manufacture an issue. The issue list is
            // the run's "why this is not a measurement" evidence; noise in it
            // makes a degraded run indistinguishable from a healthy one.
            assert!(verdict.issues.is_empty(), "{:?}", verdict.issues);
        }
    }

    /// One short of either floor fails. Together with the at-the-floor case
    /// above this pins both comparisons to `>=` rather than `>`.
    #[test]
    fn verdict_rejects_a_phase_one_short_of_either_floor() {
        let spec = ScenarioAssertSpec {
            min_hits: Some(412),
            min_restored_bytes: Some(1024),
            ..Default::default()
        };

        let one_hit_short = PhaseMetrics {
            hits: 411,
            storage: StorageInfo {
                restored_bytes: 1024,
                ..Default::default()
            },
            ..Default::default()
        };
        let verdict = Verdict::evaluate(&KeyStability::default(), &one_hit_short, Some(&spec));
        assert!(!verdict.ok);
        assert_eq!(failed_check_names(&verdict), vec!["min_hits"]);
        assert_eq!(verdict.issues.len(), 1, "{:?}", verdict.issues);

        let one_byte_short = PhaseMetrics {
            hits: 412,
            storage: StorageInfo {
                restored_bytes: 1023,
                ..Default::default()
            },
            ..Default::default()
        };
        let verdict = Verdict::evaluate(&KeyStability::default(), &one_byte_short, Some(&spec));
        assert!(!verdict.ok);
        assert_eq!(failed_check_names(&verdict), vec!["min_restored_bytes"]);
        assert_eq!(verdict.issues.len(), 1, "{:?}", verdict.issues);
    }

    /// Hits without bytes is the subtler failure: kache answered every query,
    /// but nothing landed in the build tree, so the wall-clock is not a restore.
    #[test]
    fn verdict_rejects_hits_that_restored_nothing() {
        let spec = ScenarioAssertSpec {
            min_hits: Some(1),
            min_restored_bytes: Some(1024),
            ..Default::default()
        };
        let hollow = PhaseMetrics {
            hits: 412,
            ..Default::default()
        };

        let verdict = Verdict::evaluate(&KeyStability::default(), &hollow, Some(&spec));

        assert!(!verdict.ok);
        assert_eq!(failed_check_names(&verdict), vec!["min_restored_bytes"]);
        // Exactly one issue: the hits check passed, so it contributes nothing.
        assert_eq!(verdict.issues.len(), 1, "{:?}", verdict.issues);
        assert!(
            verdict.issues[0].contains("restored from the store"),
            "{:?}",
            verdict.issues
        );
    }

    fn failed_check_names(verdict: &Verdict) -> Vec<&'static str> {
        verdict
            .checks
            .iter()
            .filter(|check| !check.passed)
            .map(|check| check.name)
            .collect()
    }

    fn verdict_with(ok: bool) -> Verdict {
        Verdict {
            ok,
            issues: if ok {
                Vec::new()
            } else {
                vec!["something was wrong".to_string()]
            },
            checks: Vec::new(),
        }
    }

    /// The exit-code decision, as a truth table. Both verdicts have to hold,
    /// and a scenario that never ran the same-tree phase has nothing to add.
    /// This is the last gate between a broken run and a green PR check, so
    /// every combination is pinned rather than sampled.
    #[test]
    fn a_run_is_degraded_when_either_verdict_says_so() {
        let ok = verdict_with(true);
        let bad = verdict_with(false);

        assert!(
            !run_is_degraded(&ok, None),
            "a passing run without the same-tree phase is not degraded"
        );
        assert!(
            !run_is_degraded(&ok, Some(&ok)),
            "both verdicts passing is not degraded"
        );
        assert!(
            run_is_degraded(&ok, Some(&bad)),
            "the same-tree verdict alone must be able to fail the run"
        );
        assert!(
            run_is_degraded(&bad, Some(&ok)),
            "the cross-clone verdict alone must be able to fail the run"
        );
        assert!(run_is_degraded(&bad, None));
        assert!(run_is_degraded(&bad, Some(&bad)));
    }

    /// The free-space delta is only meaningful for a plain full run. Both
    /// opt-outs are independent, so all four combinations are pinned.
    #[test]
    fn only_a_plain_full_run_measures_its_disk_footprint() {
        assert!(measures_disk_footprint(false, false));
        // --retry restores cold from a snapshot, so the delta misses that pool.
        assert!(!measures_disk_footprint(true, false));
        // --warm-same-tree refills clone-a's objdir twice, so the delta no
        // longer maps onto the three-pool model.
        assert!(!measures_disk_footprint(false, true));
        assert!(!measures_disk_footprint(true, true));
    }

    /// The warm-vs-cold speedup, including the zero-second boundary the
    /// engine's whole-second timing can actually produce on a small subject.
    #[test]
    fn phase_speedup_divides_cold_by_the_phase_and_guards_zero() {
        // A three-times-faster warm build.
        assert_eq!(phase_speedup(300, 100), 3.0);
        // Not a ratio a multiply or a remainder would produce.
        assert_eq!(phase_speedup(90, 60), 1.5);
        // Slower than cold is reported as-is; it is the validity gates' job to
        // reject that, not this helper's to hide it.
        assert_eq!(phase_speedup(60, 120), 0.5);
        // Exactly one second is the smallest measurable phase and must still
        // divide rather than fall into the zero guard.
        assert_eq!(phase_speedup(42, 1), 42.0);
        // Zero: dividing would yield infinity, which no report should carry.
        assert_eq!(phase_speedup(300, 0), 0.0);
        assert_eq!(phase_speedup(0, 0), 0.0);
    }

    /// Resetting the log between phases is what keeps each phase's report to
    /// its own build. A reset that silently did nothing would fold the previous
    /// phase's events into the next one's counters — and those counters are
    /// what the validity gates read.
    #[test]
    fn reset_event_log_removes_the_log_and_tolerates_its_absence() {
        let dir = tempfile::tempdir().unwrap();
        let event_log = dir.path().join("events.jsonl");
        std::fs::write(&event_log, b"{\"kind\":\"hit\"}\n").unwrap();

        reset_event_log(&event_log).expect("removing an existing log succeeds");
        assert!(
            !event_log.exists(),
            "the event log must be gone after a reset"
        );

        // Called again — and before the first phase, when no log exists yet.
        reset_event_log(&event_log).expect("a missing log is not an error");
        assert!(!event_log.exists());
    }

    /// `--warm-same-tree` is rejected for the sccache backend and accepted for
    /// kache. Both halves of the guard matter: rejecting the flag on kache
    /// would disable the perf gate outright, and accepting it on sccache would
    /// run a phase that backend has no code path for.
    #[test]
    fn warm_same_tree_is_rejected_only_on_the_sccache_backend() {
        const REJECTION: &str = "--warm-same-tree is only supported with --cache-backend kache";
        // Non-existent tool paths: the guard under test runs before the binary
        // is resolved, so every case below stops at resolution with a different
        // message and nothing touches a clone, a cache, or a compiler.
        let missing = std::env::temp_dir().join("kache-perf-gate-no-such-binary");
        let config = |cache_backend, warm_same_tree| BenchRunConfig {
            kache: missing.clone(),
            sccache: missing.clone(),
            cache_backend,
            scenarios: PathBuf::from("./scenarios"),
            select: vec!["suite:bench".to_string()],
            git_ref: None,
            work_dir: None,
            skip_clone: false,
            force_setup: false,
            retry: false,
            trace_keys: false,
            warm_same_tree,
        };

        let rejected = run_bench(config(CacheBackend::Sccache, true))
            .expect_err("sccache + --warm-same-tree must be rejected")
            .to_string();
        assert!(rejected.contains(REJECTION), "{rejected}");

        // The same flag on the kache backend gets past the guard and fails
        // later, on the missing binary.
        let allowed = run_bench(config(CacheBackend::Kache, true))
            .expect_err("the fake kache binary cannot resolve")
            .to_string();
        assert!(!allowed.contains(REJECTION), "{allowed}");

        // ...and so does the sccache backend without the flag.
        let allowed = run_bench(config(CacheBackend::Sccache, false))
            .expect_err("the fake sccache binary cannot resolve")
            .to_string();
        assert!(!allowed.contains(REJECTION), "{allowed}");
    }

    fn bench_result_fixture() -> BenchResult {
        let phase = |wall_s: u64, hits: u64| PhaseMetrics {
            wall_s,
            hits,
            ..Default::default()
        };
        BenchResult {
            project: "bench-pr-cargo".to_string(),
            git_ref: "797e8a9".to_string(),
            platform: "linux-x86_64".to_string(),
            run_id: "20260901T000000Z-kache-1".to_string(),
            artifact_dir: "tmp/perf-gate/head/runs/x".to_string(),
            cache_tool_version: Some("kache 0.16.1".to_string()),
            cold: phase(300, 0),
            warm_same_tree: Some(phase(100, 412)),
            warm: phase(120, 400),
            speedup: 2.5,
            warm_same_tree_speedup: Some(3.0),
            cache_size_mb: 1024.0,
            cold_objdir_bytes: 1 << 30,
            warm_objdir_bytes: 1 << 30,
            disk_measured_bytes: None,
            key_stability: KeyStability::default(),
            warm_leak_samples: Vec::new(),
            verdict: verdict_with(true),
            warm_same_tree_verdict: Some(verdict_with(true)),
            measure_warnings: Vec::new(),
            key_diff_top: None,
            reports: Vec::new(),
        }
    }

    /// The summary is the only place a human sees the run's numbers, and the
    /// perf gate compares exactly the three wall-clocks at the top of it. All
    /// three must be reported, each labelled for the phase it came from.
    #[test]
    fn summary_reports_all_three_phase_wall_clocks() {
        let text = summary_text(&bench_result_fixture());

        assert!(
            text.contains("cold build : 5m 00s"),
            "cold wall-clock missing:\n{text}"
        );
        assert!(
            text.contains("same-tree  : 1m 40s"),
            "same-tree wall-clock missing:\n{text}"
        );
        assert!(
            text.contains("warm build : 2m 00s"),
            "cross-clone wall-clock missing:\n{text}"
        );
        assert!(
            text.contains("3.00x vs cold"),
            "the same-tree speedup is what says the cache helped:\n{text}"
        );
        assert!(
            text.contains("VERDICT: ok"),
            "the verdict must be stated:\n{text}"
        );
        assert!(
            text.contains("VERDICT (same-tree): ok"),
            "the same-tree verdict must be stated separately:\n{text}"
        );
        assert!(
            text.contains("/tmp/run"),
            "the artifact directory must be pointed at:\n{text}"
        );
    }

    /// A run without the same-tree phase — every nightly scenario — must not
    /// grow same-tree lines it has no numbers for.
    #[test]
    fn summary_omits_the_same_tree_block_when_the_phase_did_not_run() {
        let mut result = bench_result_fixture();
        result.warm_same_tree = None;
        result.warm_same_tree_speedup = None;
        result.warm_same_tree_verdict = None;

        let text = summary_text(&result);

        assert!(text.contains("cold build : 5m 00s"), "{text}");
        assert!(text.contains("warm build : 2m 00s"), "{text}");
        assert!(!text.contains("same-tree"), "{text}");
    }

    /// A degraded same-tree phase must say so, and say why. This is the line a
    /// reviewer reads when the perf gate refuses to report a delta.
    #[test]
    fn summary_explains_a_degraded_same_tree_phase() {
        let mut result = bench_result_fixture();
        result.warm_same_tree_verdict = Some(Verdict {
            ok: false,
            issues: vec!["0 cache hits (need >= 100)".to_string()],
            checks: Vec::new(),
        });

        let text = summary_text(&result);

        assert!(text.contains("VERDICT (same-tree): DEGRADED"), "{text}");
        assert!(text.contains("0 cache hits (need >= 100)"), "{text}");
    }

    /// The cross-clone verdict has three distinct wordings, and the difference
    /// between two of them is the whole point: "no blocking assertions
    /// configured" means the scenario declared no gate, while "validly
    /// exercised kache" means a gate ran and passed. A reader who cannot tell
    /// those apart cannot tell an unguarded scenario from a guarded one.
    #[test]
    fn summary_distinguishes_an_unguarded_run_from_a_guarded_one() {
        let passed_check = AssertionCheck {
            name: "min_hits",
            expected: ">= 100".to_string(),
            actual: "412".to_string(),
            passed: true,
        };

        let mut unguarded = bench_result_fixture();
        unguarded.verdict = Verdict {
            ok: true,
            issues: Vec::new(),
            checks: Vec::new(),
        };
        let text = summary_text(&unguarded);
        assert!(
            text.contains("VERDICT: ok — no blocking assertions configured."),
            "{text}"
        );

        let mut guarded = bench_result_fixture();
        guarded.verdict = Verdict {
            ok: true,
            issues: Vec::new(),
            checks: vec![passed_check],
        };
        let text = summary_text(&guarded);
        assert!(
            text.contains("VERDICT: ok — the run validly exercised kache."),
            "{text}"
        );

        let mut degraded = bench_result_fixture();
        degraded.verdict = Verdict {
            ok: false,
            issues: vec!["cross-clone key stability 3.1%".to_string()],
            checks: Vec::new(),
        };
        let text = summary_text(&degraded);
        assert!(text.contains("VERDICT: DEGRADED RUN"), "{text}");
        assert!(text.contains("cross-clone key stability 3.1%"), "{text}");
    }

    /// The two optional diagnostic sections appear only when they have content.
    /// An empty "costliest misses" or "measure warnings" heading reads as a
    /// finding that is not there.
    #[test]
    fn summary_shows_optional_sections_only_when_they_have_content() {
        let quiet = bench_result_fixture();
        let text = summary_text(&quiet);
        assert!(!text.contains("costliest warm misses"), "{text}");
        assert!(!text.contains("MEASURE WARNINGS"), "{text}");

        let mut noisy = bench_result_fixture();
        noisy.warm.top_misses = vec![MissEntry {
            crate_name: "libgit2-sys".to_string(),
            compile_time_s: 71,
        }];
        noisy.measure_warnings = vec!["warm hit rate 12.0% below threshold 50.0%".to_string()];
        let text = summary_text(&noisy);
        assert!(text.contains("costliest warm misses"), "{text}");
        assert!(text.contains("libgit2-sys"), "{text}");
        assert!(text.contains("MEASURE WARNINGS"), "{text}");
        assert!(text.contains("warm hit rate 12.0%"), "{text}");
    }

    fn summary_text(result: &BenchResult) -> String {
        let mut out = Vec::new();
        write_summary(&mut out, result, Path::new("/tmp/run")).unwrap();
        String::from_utf8(out).expect("the summary is UTF-8")
    }

    #[test]
    fn dir_size_kb_sums_file_bytes_recursively() {
        let dir = std::env::temp_dir().join(format!("kb-dirsize-{}", std::process::id()));
        let sub = dir.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(dir.join("a.bin"), vec![0u8; 1024]).unwrap();
        std::fs::write(sub.join("b.bin"), vec![0u8; 2048]).unwrap();
        assert_eq!(dir_size_kb(&dir), 3);
        assert_eq!(dir_size_kb(&dir.join("does-not-exist")), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pull_bench_result_serializes_with_pull_phase() {
        let r = PullBenchResult {
            project: "bench-firefox-pull".into(),
            git_ref: "aaaa".into(),
            ref_next: "bbbb".into(),
            platform: "linux-x86_64".into(),
            run_id: "run-1".into(),
            artifact_dir: "/tmp/run-1".into(),
            cache_tool_version: Some("kache 0.8.0".into()),
            cold: PhaseMetrics::default(),
            pull: PhaseMetrics::default(),
            cache_size_mb: 1.0,
            cold_objdir_bytes: 10,
            pull_objdir_bytes: 20,
            verdict: Verdict {
                ok: true,
                issues: vec![],
                checks: vec![],
            },
            measure_warnings: vec![],
            reports: vec!["report-pull.md".into()],
        };
        let j = serde_json::to_value(&r).unwrap();
        assert_eq!(j["ref_next"], "bbbb");
        assert!(j.get("pull").is_some());
        assert!(
            j.get("warm").is_none(),
            "pull result must not carry a warm phase"
        );
    }
}
