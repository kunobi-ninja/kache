//! Lifecycle execution: drives one fixture through cold → warm → noop.
//!
//! The phase shape is hardcoded here (not in the fixture toml) because it's
//! universal across every fixture the harness drives:
//!
//! - **cold**: clean + build → populates an empty cache.
//! - **warm**: clean + build → must hit the cache populated by cold.
//! - **noop**: build (no clean) → nothing should recompile.
//!
//! Each fixture gets its **own** isolated `KACHE_CACHE_DIR` (a fresh
//! `tempfile::TempDir`) so the embedded `kache report` covers only that
//! fixture's events. Without isolation, the warm/noop reports would show
//! events from earlier fixtures (or earlier runs) — making per-fixture
//! metric assertions impossible to write tightly.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use tempfile::TempDir;

use crate::assertions::{
    AssertionCheck, all_passed, apply_metric_assertions, apply_noop_assertions,
    count_misses_by_crate, relocate_diff_artifact_check,
};
use crate::fixture::{Fixture, Verify};
use crate::report;
use crate::result::{FixtureResult, PhaseResult, VerifyResult, fixture_status};

/// One lifecycle phase. Order matters — `cold` populates the cache,
/// `warm` consumes it, `noop` checks incrementality on top of `warm`,
/// and `relocate` builds the same source from a *different* working
/// directory to catch path-leak bugs in the cache key.
#[derive(Debug, Clone, Copy)]
pub enum Phase {
    Cold,
    Warm,
    Noop,
    /// Same source, different absolute path, shared cache. The build
    /// runs in a fresh temp directory populated with a copy of the
    /// fixture, then cleaned. If absolute paths leak into the cache
    /// key (target dir, `$HOME`, build root), the relocated build
    /// misses everything cold/warm populated — visible as zero hits
    /// against the same source. Without this phase, the bug class is
    /// invisible because every other phase rebuilds at the same
    /// path and trivially hits.
    Relocate,
    /// Relocate, then edit a source file before building. The inverse
    /// of `Relocate`: where that phase proves the key is
    /// path-*independent* (a relocated build hits), this proves the
    /// key is still content-*sensitive* — the edited build MUST miss.
    /// Together they pin the key from both sides and guard the
    /// stale-restore bug class (a content change wrongly served from
    /// cache). Only runs when the fixture declares `[modify]`.
    RelocateModified,
    /// Build a *second* time in the relocated directory, without
    /// cleaning, immediately after `Relocate`. The contract: nothing
    /// recompiles.
    ///
    /// `Relocate` proves a relocated build *hits* the cache; it does
    /// not prove the build it leaves behind is *stable*. The cache
    /// hits restore each crate's dep-info (`.d`) file, and cargo reads
    /// those `.d` files on the next build to decide what is up to
    /// date. If a cached `.d` carries the producing build's absolute
    /// paths, cargo's freshness `stat()` fails at the relocated path,
    /// the crate is marked dirty, and the next build recompiles it —
    /// a cache hit that bought nothing. The in-place `Noop` phase
    /// cannot catch this: it rebuilds where the `.d` paths are
    /// trivially valid. `RelocateNoop` is the only phase that exposes
    /// stale dep-info paths. Per-fixture opt-in via
    /// `[assertions.relocate-noop]`.
    RelocateNoop,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Cold => "cold",
            Phase::Warm => "warm",
            Phase::Noop => "noop",
            Phase::Relocate => "relocate",
            Phase::RelocateModified => "relocate-modified",
            Phase::RelocateNoop => "relocate-noop",
        }
    }

    /// Should this phase run a `clean` step before `build`? `noop` and
    /// `relocate-noop` skip the clean (that's literally what makes them
    /// no-op tests — they rebuild on top of a populated tree);
    /// `relocate` runs in a fresh dir that's already clean, but we
    /// still run `clean` defensively in case `cp -R` brought a stale
    /// target/ along.
    fn cleans_first(self) -> bool {
        !matches!(self, Phase::Noop | Phase::RelocateNoop)
    }

    /// Should this phase run the fixture's `[verify]` (run the binary,
    /// check stdout)? Every phase but `relocate-modified` does — that
    /// phase deliberately edits the source, so the program's output
    /// changes and the fixture's stdout contract no longer applies.
    /// Its contract is purely "the cache missed", checked via metrics.
    fn runs_verify(self) -> bool {
        !matches!(self, Phase::RelocateModified)
    }
}

/// Run every phase against `fixture` and return the aggregated result.
///
/// The runner owns the cache dir lifecycle: a fresh `TempDir` is created
/// at the top, used across every phase (so cold's writes stay visible to
/// warm and noop), and cleaned up by `Drop` when the function returns.
/// The daemon is stopped before exit so it releases the cache dir's locks
/// before `TempDir` removes the files underneath it.
pub fn run_fixture(fixture: &Fixture, kache_path: &Path) -> Result<FixtureResult> {
    let cache_dir = TempDir::new().context("creating per-fixture cache dir")?;
    eprintln!(
        "--- {} (cache: {})",
        fixture.name,
        cache_dir.path().display()
    );

    // Defensive: stop any inherited daemon from a previous fixture's
    // run before we start measuring.
    stop_daemon(kache_path, cache_dir.path());

    // Per-phase report deltas: snapshot the cumulative kache report
    // before each phase, subtract afterwards. Without this, `kache
    // report --since 1h` is cumulative across phases — warm's hits
    // would inflate noop's report, hiding (e.g.) the case where
    // relocate added new misses but the cumulative hit count still
    // looks healthy. `prev_summary` rolls forward through every phase.
    //
    // `prev_event_count` plays the same role for `all_events`: it's
    // the count of events seen *before* this phase, so the suffix
    // beyond it is what the phase produced. Per-crate assertions
    // need this slice; the aggregate summary delta isn't enough.
    let mut prev_summary = report::empty_summary();
    let mut prev_event_count: usize = 0;

    // Differential-test baseline: `artifact path → bytes` captured on
    // the cold build (a real compile). Every cache-hit phase compares
    // its restored artifact against this. Rolls forward through all
    // phases; empty unless the fixture declares `[diff]`.
    let mut diff_baseline: std::collections::HashMap<String, Vec<u8>> =
        std::collections::HashMap::new();

    // `relocate` is held outside the loop because it needs its own
    // `cwd` (a copy of the fixture in a fresh tempdir) — kept distinct
    // here so the in-place phases (cold/warm/noop) stay simple and the
    // relocate-specific dir lifecycle doesn't leak into them.
    let phases = [Phase::Cold, Phase::Warm, Phase::Noop];
    let mut phase_results = Vec::with_capacity(phases.len() + 2);
    let mut short_circuit = false;
    for phase in phases {
        let (result, post) = run_phase(
            phase,
            fixture,
            &fixture.dir,
            kache_path,
            cache_dir.path(),
            &prev_summary,
            prev_event_count,
            &mut diff_baseline,
        )?;
        if let Some((s, c)) = post {
            prev_summary = s;
            prev_event_count = c;
        }
        let failed = result.status == "fail";
        phase_results.push(result);
        if failed {
            // Short-circuit: if cold fails, warm and noop are meaningless.
            // Recorded phases keep the `phase` field so consumers see
            // exactly where the run stopped.
            short_circuit = true;
            break;
        }
    }

    if !short_circuit {
        match prepare_relocated_dir(&fixture.dir) {
            Ok(relocated) => {
                // Defense-in-depth: wipe the original fixture's build
                // artifacts BEFORE running relocate. Without this,
                // a false-cache-hit binary at the relocated path
                // would still embed the original location's paths
                // (OUT_DIR, etc.) — and those paths would still
                // resolve at runtime because cold/warm/noop
                // populated `fixture.dir/target/...`. Verify would
                // pass and the bug would slip through. With the
                // wipe, a false-hit binary tries to read from a
                // path that no longer exists → verify fails →
                // bug caught even when the metric assertion didn't
                // declare `min_misses`. Belt-and-braces for
                // out-dir-runtime-style fixtures.
                let _ = run_step(
                    &fixture.commands.clean,
                    fixture,
                    &fixture.dir,
                    cache_dir.path(),
                );

                let (mut result, post) = run_phase(
                    Phase::Relocate,
                    fixture,
                    relocated.path(),
                    kache_path,
                    cache_dir.path(),
                    &prev_summary,
                    prev_event_count,
                    &mut diff_baseline,
                )?;
                // Roll the report cursor forward so the following
                // phases' deltas reflect only their own events.
                if let Some((s, c)) = post {
                    prev_summary = s;
                    prev_event_count = c;
                }

                // relocate-noop: build a SECOND time in the SAME
                // relocated tree, without cleaning. The relocate phase
                // above cache-hit every crate and restored their
                // dep-info (`.d`) files; this phase proves those `.d`
                // files carry paths valid at the relocated location —
                // i.e. cargo's freshness check finds them and does NOT
                // recompile. Must run BEFORE `differential_relocate_check`,
                // which re-cleans the tree and rebuilds it cold against
                // a fresh cache (which would write fresh, trivially-valid
                // `.d` files and mask the stale-dep-info bug). Opt-in:
                // only fixtures declaring `[assertions.relocate-noop]`.
                // Held in an Option so it is pushed AFTER the relocate
                // phase result (which the diff check below still mutates).
                let mut relocate_noop_result: Option<PhaseResult> = None;
                if result.status == "pass" && fixture.assertions.relocate_noop.is_some() {
                    let (noop_result, post) = run_phase(
                        Phase::RelocateNoop,
                        fixture,
                        relocated.path(),
                        kache_path,
                        cache_dir.path(),
                        &prev_summary,
                        prev_event_count,
                        &mut diff_baseline,
                    )?;
                    if let Some((s, c)) = post {
                        prev_summary = s;
                        prev_event_count = c;
                    }
                    relocate_noop_result = Some(noop_result);
                }

                // Differential relocate check: the relocate phase above
                // built with kache enabled, so its `[diff]` artifacts
                // are cache-restored. Now rebuild the SAME relocated
                // tree against a fresh empty cache (kache misses → real
                // compiles) and prove the restored bytes equal a
                // genuine fresh compile at the new path. Only runs for
                // `[diff]` fixtures whose relocate phase passed — a
                // fresh-vs-restored compare is meaningless if the
                // relocate build itself failed. Failures append to
                // `result` and flip its status. Runs after relocate-noop
                // because it re-cleans the relocated tree.
                if result.status == "pass" && fixture.diff.is_some() {
                    let extra = differential_relocate_check(fixture, relocated.path(), kache_path);
                    if !extra.is_empty() {
                        if !all_passed(&extra) {
                            result.status = "fail".to_string();
                        }
                        result.assertions.extend(extra);
                    }
                }

                let mut relocate_failed = result.status == "fail";
                phase_results.push(result);
                if let Some(noop_result) = relocate_noop_result {
                    if noop_result.status == "fail" {
                        relocate_failed = true;
                    }
                    phase_results.push(noop_result);
                }
                // `relocated` (TempDir) drops here, removing the copy.

                // relocate-modified: relocate again, edit a source
                // file, rebuild — the build MUST miss. Skipped if the
                // fixture declares no `[modify]`, or if relocate
                // itself failed (a modified build is meaningless then).
                if !relocate_failed && let Some(modify) = &fixture.modify {
                    phase_results.push(run_relocate_modified(
                        fixture,
                        modify,
                        kache_path,
                        cache_dir.path(),
                        &prev_summary,
                        prev_event_count,
                        &mut diff_baseline,
                    )?);
                }
            }
            Err(e) => {
                // Surface as a fail with diagnostic context. We don't
                // want to silently skip the relocate check just because
                // `cp` had a hiccup.
                phase_results.push(PhaseResult {
                    phase: Phase::Relocate.name().to_string(),
                    status: "fail".to_string(),
                    build_wall_s: 0,
                    build_exit_code: -1,
                    verify: None,
                    kache_report: serde_json::json!({}),
                    assertions: vec![AssertionCheck {
                        name: "prepare_relocated_dir",
                        expected: "successful copy".to_string(),
                        actual: format!("{e:?}"),
                        passed: false,
                    }],
                });
            }
        }
    }

    // Stop the daemon so it releases the cache dir's locks before
    // TempDir's Drop removes the files.
    let _ = Command::new(kache_path)
        .arg("daemon")
        .arg("stop")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    let status = fixture_status(&phase_results);
    Ok(FixtureResult {
        name: fixture.name.clone(),
        status,
        phases: phase_results,
    })
}

/// Drive the `relocate-modified` phase: copy the fixture to a fresh
/// path, apply the fixture's one source edit, then run the phase.
///
/// Copy / edit failures are recorded as a failed [`PhaseResult`]
/// rather than raised — matching how the relocate phase treats a
/// `cp` hiccup, so one fixture's setup glitch never aborts the run.
fn run_relocate_modified(
    fixture: &Fixture,
    modify: &crate::fixture::ModifySpec,
    kache_path: &Path,
    cache_dir: &Path,
    prev_summary: &crate::report::ReportSummary,
    prev_event_count: usize,
    diff_baseline: &mut std::collections::HashMap<String, Vec<u8>>,
) -> Result<PhaseResult> {
    let fail = |name: &'static str, expected: String, actual: String| PhaseResult {
        phase: Phase::RelocateModified.name().to_string(),
        status: "fail".to_string(),
        build_wall_s: 0,
        build_exit_code: -1,
        verify: None,
        kache_report: serde_json::json!({}),
        assertions: vec![AssertionCheck {
            name,
            expected,
            actual,
            passed: false,
        }],
    };

    let relocated = match prepare_relocated_dir(&fixture.dir) {
        Ok(d) => d,
        Err(e) => {
            return Ok(fail(
                "prepare_relocated_dir",
                "successful copy".to_string(),
                format!("{e:?}"),
            ));
        }
    };
    if let Err(e) = apply_modification(relocated.path(), modify) {
        return Ok(fail(
            "apply_modification",
            format!("replace `{}` in {}", modify.find, modify.file),
            format!("{e:?}"),
        ));
    }
    let (result, _post) = run_phase(
        Phase::RelocateModified,
        fixture,
        relocated.path(),
        kache_path,
        cache_dir,
        prev_summary,
        prev_event_count,
        diff_baseline,
    )?;
    Ok(result)
}

/// Differential relocate check — the strong form of the `[diff]` test.
///
/// The relocate phase (already run by the caller) built the fixture at
/// a fresh absolute path with kache **enabled**, so its `[diff]`
/// artifacts are *cache-restored* blobs. The existing `diff_match[...]`
/// check compares those against the cold baseline — but a relocate hit
/// restores the very blob cold cached, so that comparison only proves
/// store/restore fidelity, not path-independence.
///
/// This closes that gap: it snapshots the cache-restored artifacts,
/// then re-cleans the relocated tree and rebuilds it against a brand-
/// new **empty** cache. With nothing to hit, kache misses every unit
/// and runs its real compile path — the *same* path that populated the
/// shared cache at cold — yielding a faithful fresh kache build at the
/// relocated path. Byte-equality between the restored and the fresh
/// artifacts positively proves no machine-local build path leaked into
/// the cached artifact, so a cross-path cache hit is provably safe.
///
/// The fresh build must NOT use `KACHE_DISABLED`: that bypasses kache's
/// compile path entirely (straight passthrough to the bare compiler),
/// so it would miss kache's own object-normalizing flags — e.g. the
/// `-ffile-prefix-map` injection — and the comparison would be
/// apples-to-oranges. A cache *miss* is the correct baseline.
///
/// Returns one `relocate_diff_match[<artifact>]` check per `[diff]`
/// artifact. Setup failures (clean/build/read) surface as a failed
/// check rather than aborting. `kache_path` is used only to stop the
/// fresh cache's daemon before its `TempDir` is removed.
fn differential_relocate_check(
    fixture: &Fixture,
    relocated: &Path,
    kache_path: &Path,
) -> Vec<AssertionCheck> {
    let Some(diff) = &fixture.diff else {
        return Vec::new();
    };

    // (1) Snapshot the cache-restored artifacts the relocate build left
    // on disk. A missing artifact here is a fixture misconfiguration —
    // surface it as a failed check, mirroring `diff_artifact_present`.
    let mut restored: Vec<(String, Vec<u8>)> = Vec::with_capacity(diff.artifacts.len());
    let mut failures: Vec<AssertionCheck> = Vec::new();
    for artifact in &diff.artifacts {
        match read_declared_artifact(relocated, artifact) {
            Ok(bytes) => restored.push((artifact.clone(), bytes)),
            Err(e) => failures.push(AssertionCheck {
                name: "relocate_diff_restored_present",
                expected: format!("readable cache-restored artifact `{artifact}`"),
                actual: format!("{e}"),
                passed: false,
            }),
        }
    }
    if !failures.is_empty() {
        return failures;
    }

    // (2) Re-clean and rebuild the SAME relocated tree against a fresh
    // EMPTY cache. With nothing to hit, kache misses every unit and
    // runs its real compile path — the genuine fresh-build baseline.
    let fresh_cache = match TempDir::new() {
        Ok(dir) => dir,
        Err(e) => {
            return vec![AssertionCheck {
                name: "relocate_diff_cache",
                expected: "fresh cache directory created".to_string(),
                actual: format!("{e}"),
                passed: false,
            }];
        }
    };
    for (step_name, cmd) in [
        ("relocate_diff_clean", &fixture.commands.clean),
        ("relocate_diff_build", &fixture.commands.build),
    ] {
        let failure = match run_step(cmd, fixture, relocated, fresh_cache.path()) {
            Ok(s) if s.exit.success() => None,
            Ok(s) => Some(format!("exit {}", s.exit.code().unwrap_or(1))),
            Err(e) => Some(format!("{e:?}")),
        };
        if let Some(actual) = failure {
            stop_daemon(kache_path, fresh_cache.path());
            return vec![AssertionCheck {
                name: step_name,
                expected: "fresh-cache step succeeds".to_string(),
                actual,
                passed: false,
            }];
        }
    }
    // The fresh build may have spawned a daemon bound to `fresh_cache`;
    // stop it before the TempDir drops and removes the directory.
    stop_daemon(kache_path, fresh_cache.path());

    // (3) + (4) Snapshot the fresh artifacts and byte-compare each
    // against the cache-restored snapshot from step (1).
    let mut checks = Vec::with_capacity(restored.len());
    for (artifact, restored_bytes) in &restored {
        match read_declared_artifact(relocated, artifact) {
            Ok(fresh) => {
                checks.push(relocate_diff_artifact_check(
                    artifact,
                    restored_bytes,
                    &fresh,
                ));
            }
            Err(e) => checks.push(AssertionCheck {
                name: "relocate_diff_fresh_present",
                expected: format!("readable fresh-compile artifact `{artifact}`"),
                actual: format!("{e}"),
                passed: false,
            }),
        }
    }
    checks
}

/// Apply the fixture's single find/replace edit to a source file in a
/// relocated copy. Fails loudly if `find` is absent — a no-op edit
/// would make the `relocate-modified` phase test nothing.
fn apply_modification(dir: &Path, modify: &crate::fixture::ModifySpec) -> Result<()> {
    let path = dir.join(&modify.file);
    let content = std::fs::read_to_string(&path)
        .with_context(|| format!("reading {} for modification", path.display()))?;
    if !content.contains(&modify.find) {
        anyhow::bail!(
            "`{}` not found in {} — the edit would be a no-op",
            modify.find,
            modify.file
        );
    }
    std::fs::write(&path, content.replace(&modify.find, &modify.replace))
        .with_context(|| format!("writing modified {}", path.display()))
}

/// Run a single phase: optional clean, build, verify, fetch report,
/// apply assertions. Returns a [`PhaseResult`] regardless of outcome —
/// failures are recorded, not raised. (Errors that prevent recording
/// at all — e.g. inability to spawn — propagate via `Result`.)
///
/// `cwd` is the working directory for the build/clean/verify commands.
/// In-place phases (cold/warm/noop) pass `&fixture.dir`; the relocate
/// phase passes a fresh tempdir containing a copy of the fixture so
/// path-leak bugs become visible as cache misses.
///
/// `prev_summary` is the cumulative kache report from the end of the
/// previous phase (or [`report::empty_summary`] for the first phase).
/// Assertions are applied against the delta `(post - prev)` so each
/// phase's metrics reflect ITS work, not the cumulative since fixture
/// start. The post-phase summary is returned so the caller can roll
/// it forward as `prev_summary` for the next phase.
///
/// `diff_baseline` carries the differential-test state: the cold phase
/// records each declared artifact's bytes into it; cache-hit phases
/// read it back and assert byte-equality.
// Orchestration glue — the inputs are genuinely independent (phase,
// dirs, the rolling report cursor, the rolling diff baseline). Bundling
// them into a struct would obscure more than it clarifies.
#[allow(clippy::too_many_arguments)]
fn run_phase(
    phase: Phase,
    fixture: &Fixture,
    cwd: &Path,
    kache_path: &Path,
    cache_dir: &Path,
    prev_summary: &crate::report::ReportSummary,
    prev_event_count: usize,
    diff_baseline: &mut std::collections::HashMap<String, Vec<u8>>,
) -> Result<(PhaseResult, Option<(crate::report::ReportSummary, usize)>)> {
    if phase.cleans_first() {
        let status = run_step(&fixture.commands.clean, fixture, cwd, cache_dir)?;
        if !status.exit.success() {
            // Clean failures are unusual but not crashes; record as a
            // failed phase with build_wall_s=0 so consumers see what
            // happened.
            return Ok((
                PhaseResult {
                    phase: phase.name().to_string(),
                    status: "fail".to_string(),
                    build_wall_s: 0,
                    build_exit_code: status.exit.code().unwrap_or(1),
                    verify: None,
                    kache_report: serde_json::json!({}),
                    assertions: vec![AssertionCheck {
                        name: "clean_step",
                        expected: "exit 0".to_string(),
                        actual: format!("exit {}", status.exit.code().unwrap_or(1)),
                        passed: false,
                    }],
                },
                None,
            ));
        }
    }

    let started = Instant::now();
    let build = run_step(&fixture.commands.build, fixture, cwd, cache_dir)?;
    let build_wall_s = started.elapsed().as_secs();
    let build_exit_code = build.exit.code().unwrap_or(1);

    if !build.exit.success() {
        return Ok((
            PhaseResult {
                phase: phase.name().to_string(),
                status: "fail".to_string(),
                build_wall_s,
                build_exit_code,
                verify: None,
                kache_report: serde_json::json!({}),
                assertions: vec![AssertionCheck {
                    name: "build_exit_code",
                    expected: "0".to_string(),
                    actual: build_exit_code.to_string(),
                    passed: false,
                }],
            },
            None,
        ));
    }

    // Verify: run the artifact, check stdout contract. Skipped for
    // `relocate-modified` — that phase edits the source, so the
    // program's output no longer matches the fixture's contract.
    let verify = if phase.runs_verify() {
        fixture
            .verify
            .as_ref()
            .map(|v| run_verify(v, fixture, cwd, cache_dir))
    } else {
        None
    };

    let (typed, raw) = report::fetch(kache_path, cache_dir)?;
    // Per-phase delta: subtract the previous cumulative summary so
    // assertions reflect THIS phase's hits/misses, not the running
    // total since fixture start. Without this, e.g. relocate's poor
    // hit rate is masked by warm's accumulated successes.
    let delta = typed.summary.delta_since(prev_summary);

    // Per-crate miss counts for THIS phase only. `all_events` is
    // append-only and time-ordered, so the suffix from
    // `prev_event_count` onwards is the events this phase produced.
    // Without this slicing, per-crate assertions would reflect the
    // cumulative miss history (so warm/noop's misses would inflate
    // relocate's per-crate counts and mask false hits).
    let new_events = if prev_event_count <= typed.all_events.len() {
        &typed.all_events[prev_event_count..]
    } else {
        // Defensive: report shrank between phases (shouldn't happen,
        // but be robust to a future kache change).
        &[][..]
    };
    let phase_misses_by_crate = count_misses_by_crate(new_events);

    let mut checks = match phase {
        Phase::Cold => fixture
            .assertions
            .cold
            .as_ref()
            .map(|spec| apply_metric_assertions(spec, &delta, &phase_misses_by_crate, new_events))
            .unwrap_or_default(),
        Phase::Warm => fixture
            .assertions
            .warm
            .as_ref()
            .map(|spec| apply_metric_assertions(spec, &delta, &phase_misses_by_crate, new_events))
            .unwrap_or_default(),
        Phase::Noop => fixture
            .assertions
            .noop
            .as_ref()
            .map(|spec| apply_noop_assertions(spec, new_events))
            .unwrap_or_default(),
        Phase::RelocateNoop => fixture
            .assertions
            .relocate_noop
            .as_ref()
            .map(|spec| apply_noop_assertions(spec, new_events))
            .unwrap_or_default(),
        Phase::Relocate => fixture
            .assertions
            .relocate
            .as_ref()
            .map(|spec| apply_metric_assertions(spec, &delta, &phase_misses_by_crate, new_events))
            .unwrap_or_default(),
        Phase::RelocateModified => fixture
            .assertions
            .relocate_modified
            .as_ref()
            .map(|spec| apply_metric_assertions(spec, &delta, &phase_misses_by_crate, new_events))
            .unwrap_or_default(),
    };

    // Differential check: a cache-hit phase's restored artifact must
    // be byte-identical to the cold build's real-compiler output.
    // Cold records the baseline; warm + relocate compare against it.
    // (noop restores nothing; relocate-modified's artifact differs by
    // design — neither is a hit-vs-fresh comparison.)
    if let Some(diff) = &fixture.diff {
        for artifact in &diff.artifacts {
            match read_declared_artifact(cwd, artifact) {
                Ok(bytes) => match phase {
                    Phase::Cold => {
                        diff_baseline.insert(artifact.clone(), bytes);
                    }
                    Phase::Warm | Phase::Relocate => {
                        checks.push(crate::assertions::diff_artifact_check(
                            artifact,
                            diff_baseline.get(artifact),
                            &bytes,
                        ));
                    }
                    Phase::Noop | Phase::RelocateNoop | Phase::RelocateModified => {}
                },
                Err(e) => {
                    if matches!(phase, Phase::Cold | Phase::Warm | Phase::Relocate) {
                        checks.push(AssertionCheck {
                            name: "diff_artifact_present",
                            expected: format!("readable artifact `{artifact}`"),
                            actual: format!("{e}"),
                            passed: false,
                        });
                    }
                }
            }
        }
    }

    // Direct dep-info assertion: a restored `.d` must be path-expanded,
    // not left in kache's relativized form. This inspects the `.d`
    // content itself — unlike the `should_not_recompile` proxy, which
    // can pass even when the restored `.d` is broken (the #100 bug:
    // cargo treated a simple crate Fresh regardless). Opt-in per
    // fixture via `check_depinfo`.
    if fixture.check_depinfo {
        let reason = inspect_restored_depinfo(cwd);
        checks.push(AssertionCheck {
            name: "depinfo_expanded",
            expected: "restored .d files carry no relativization sentinel".to_string(),
            actual: reason.clone().unwrap_or_else(|| "ok".to_string()),
            passed: reason.is_none(),
        });
    }

    // Verify failures count toward phase pass/fail too.
    let verify_passed = verify.as_ref().map(|v| v.passed).unwrap_or(true);
    if let Some(v) = &verify {
        checks.push(AssertionCheck {
            name: "verify",
            expected: "artifact runs and stdout matches".to_string(),
            actual: v.failure_reason.clone().unwrap_or_else(|| "ok".to_string()),
            passed: v.passed,
        });
    }

    let status = if all_passed(&checks) && verify_passed {
        "pass"
    } else {
        "fail"
    };
    let post_event_count = typed.all_events.len();
    Ok((
        PhaseResult {
            phase: phase.name().to_string(),
            status: status.to_string(),
            build_wall_s,
            build_exit_code,
            verify,
            kache_report: raw,
            assertions: checks,
        },
        Some((typed.summary, post_event_count)),
    ))
}

struct StepOutcome {
    exit: std::process::ExitStatus,
}

/// Copy `src` (a fixture directory) into a fresh tempdir for the
/// relocate phase to build in. Returns the owning [`TempDir`] so the
/// caller drops it when the phase completes.
///
/// We shell out to `cp -R src/. dst/` (POSIX-portable: works with BSD
/// cp on macOS and GNU cp on Linux) instead of hand-rolling a
/// recursive copy in Rust. Any stale `target/` / `build/` that comes
/// along is cleaned by `fixture.commands.clean` at the start of the
/// phase, so the build runs against a pristine tree at a different
/// path. Performance is not a concern — the largest fixture is a
/// few hundred KB of source.
/// A relocated fixture copy. Owns the [`TempDir`] for RAII cleanup but
/// exposes a **long-form** root path via [`path`](Self::path).
///
/// On Windows the system tempdir is often an 8.3 short path (the
/// self-hosted runner's `NetworkService` profile resolves `TEMP` to
/// `C:\Windows\SERVIC~1\NETWOR~1\...`). cargo derives `OUT_DIR` from the
/// build cwd in that same short form, but kache's path-normalizer
/// canonicalizes its rule prefixes to long form — so the short `OUT_DIR`
/// never matched and leaked into the cache key, making the relocate phase
/// miss (kunobi-ninja/kache#201). Building under the canonicalized long
/// path keeps `OUT_DIR` in the form the normalizer expects. No-op on Unix
/// (canonicalize only resolves symlinks there).
struct RelocatedDir {
    _temp: TempDir,
    root: PathBuf,
}

impl RelocatedDir {
    fn path(&self) -> &Path {
        &self.root
    }
}

fn prepare_relocated_dir(src: &Path) -> Result<RelocatedDir> {
    let dst = TempDir::new().context("creating relocated tempdir")?;
    let status = Command::new("cp")
        .arg("-R")
        .arg(format!("{}/.", src.display()))
        .arg(dst.path())
        .status()
        .context("spawning cp -R for relocate phase")?;
    if !status.success() {
        anyhow::bail!(
            "cp -R {}/. {} exited {}",
            src.display(),
            dst.path().display(),
            status
        );
    }
    copy_toolchain_pin(src, dst.path());
    // WINDOWS ONLY: long-form, `\\?\`-stripped root (see [`RelocatedDir`]).
    // On Unix this canonicalize is both unnecessary (no 8.3 short names)
    // and harmful: it resolves the macOS `/tmp` → `/private/tmp` symlink,
    // which perturbs rustup's `rust-toolchain.toml` resolution at the
    // relocated cwd (the build picks a different toolchain → different
    // `rustc_version` → spurious relocate misses). Keep the tempdir path
    // verbatim on Unix so relocate behaves exactly as before this change.
    let root = if cfg!(windows) {
        std::fs::canonicalize(dst.path())
            .map(|c| crate::portable_path(&c))
            .unwrap_or_else(|_| dst.path().to_path_buf())
    } else {
        dst.path().to_path_buf()
    };
    Ok(RelocatedDir { _temp: dst, root })
}

/// Carry the active Rust toolchain pin into the relocated tree.
///
/// The cold/warm/noop phases build the fixture in place inside the
/// worktree, where rustup applies the repo's `rust-toolchain.toml`.
/// The relocated copy lives in a tempdir with no such file anywhere up
/// its path, so a build there would fall back to rustup's *default*
/// toolchain — a different `rustc`. Since `rustc`'s version is part of
/// every cache key, that mismatch masquerades as a relocate cache miss,
/// turning a pure path-portability test into an accidental toolchain
/// test (kache issue #96).
///
/// Copying the pin (found by walking up from the fixture dir) into the
/// relocated dir makes both builds resolve the same toolchain. A no-op
/// when no pin exists — both locations then use rustup's default,
/// which is already consistent.
fn copy_toolchain_pin(src: &Path, dst: &Path) {
    for dir in src.ancestors() {
        for name in ["rust-toolchain.toml", "rust-toolchain"] {
            let pin = dir.join(name);
            if pin.is_file() {
                let _ = std::fs::copy(&pin, dst.join(name));
                return;
            }
        }
    }
}

/// Best-effort: stop any kache daemon bound to `cache_dir`.
///
/// Errors are swallowed — `daemon stop` failing because nothing is
/// running is normal. Called at fixture startup, and after the
/// differential relocate check's fresh-cache build, so no daemon holds
/// a cache directory while its `TempDir` is being removed.
fn stop_daemon(kache_path: &Path, cache_dir: &Path) {
    let _ = Command::new(kache_path)
        .arg("daemon")
        .arg("stop")
        .env("KACHE_CACHE_DIR", cache_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Run one shell command in `cwd` with the fixture's env.
///
/// Uses `sh -c` so commands can include redirects / pipes naturally.
/// Both stdout and stderr are captured and echoed back so CI logs show
/// what happened without cracking open results.json. The captured
/// streams are no longer consulted by the no-op assertion (which now
/// uses kache's event log directly — see [`apply_noop_assertions`]),
/// but they remain valuable build-failure diagnostics.
///
/// `cwd` is decoupled from `fixture.dir` so the relocate phase can
/// run the same command in a copy of the fixture at a different
/// absolute path.
fn run_step(cmd: &str, fixture: &Fixture, cwd: &Path, cache_dir: &Path) -> Result<StepOutcome> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .current_dir(cwd)
        .env("KACHE_CACHE_DIR", cache_dir)
        .envs(&fixture.env)
        .output()
        .with_context(|| format!("spawning `{cmd}` in {}", cwd.display()))?;

    // Echo both streams so CI logs show what happened. The no-op
    // assertion no longer consults stdout/stderr (it reads kache's
    // event log instead — issue #135), so we don't carry the captures
    // any further; they're useful only as build-failure diagnostics
    // in the live CI log.
    std::io::Write::write_all(&mut std::io::stderr(), &output.stdout).ok();
    std::io::Write::write_all(&mut std::io::stderr(), &output.stderr).ok();
    Ok(StepOutcome {
        exit: output.status,
    })
}

/// Run [`Verify::run`] in `cwd`, check exit + stdout contract.
fn run_verify(spec: &Verify, fixture: &Fixture, cwd: &Path, cache_dir: &Path) -> VerifyResult {
    let output = match Command::new("sh")
        .arg("-c")
        .arg(&spec.run)
        .current_dir(cwd)
        .env("KACHE_CACHE_DIR", cache_dir)
        .envs(&fixture.env)
        .stderr(Stdio::inherit())
        .output()
    {
        Ok(o) => o,
        Err(e) => {
            return VerifyResult {
                exit_code: -1,
                stdout: String::new(),
                passed: false,
                failure_reason: Some(format!("spawn failed: {e}")),
            };
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let exit_code = output.status.code().unwrap_or(-1);

    if exit_code != spec.expected_exit_code {
        return VerifyResult {
            exit_code,
            stdout,
            passed: false,
            failure_reason: Some(format!(
                "exit code {} != expected {}",
                exit_code, spec.expected_exit_code
            )),
        };
    }

    for needle in &spec.expected_stdout_contains {
        if !stdout.contains(needle) {
            return VerifyResult {
                exit_code,
                stdout,
                passed: false,
                failure_reason: Some(format!("stdout missing substring: `{needle}`")),
            };
        }
    }

    // Optional binary-content inspection: read the artifact via
    // `strings` and grep for substrings that must NOT appear. Catches
    // output-byte path leaks the runtime check can't see (e.g. a
    // path embedded in DWARF that happens to still resolve at the
    // restored location). `strings` is on every macOS / Linux dev
    // box (part of binutils); on Windows we'd use `dumpbin` (out of
    // scope until Windows e2e — see #77).
    if let Some(reason) = inspect_binary(&spec.run, cwd, &spec.forbidden_substrings) {
        return VerifyResult {
            exit_code,
            stdout,
            passed: false,
            failure_reason: Some(reason),
        };
    }

    VerifyResult {
        exit_code,
        stdout,
        passed: true,
        failure_reason: None,
    }
}

/// Read the binary at `run_cmd` (which is the verify command — its
/// first whitespace-separated token is the executable path) via
/// `strings`, return `Some(reason)` if any forbidden substring is
/// found, `None` otherwise.
///
/// Empty `forbidden` list = no inspection. Failure to spawn `strings`
/// or read its output is treated as "skip" not "fail" — the inspection
/// is best-effort defense-in-depth, not a load-bearing check. The
/// metric assertions and the runtime verify already exist; this just
/// adds another lens on top.
///
/// LIMITATION: this lens is effectively Unix-only — `strings` (binutils)
/// is not provisioned on the Windows runner, so it skips there, and the
/// fixtures' `forbidden_substrings` list Unix-form prefixes. The
/// cross-platform guard against build-path leaks is the byte-equality
/// `relocate_diff_match` check, which runs everywhere; this `strings`
/// scan is a redundant Unix lens on top. (A raw-byte scan was tried to
/// make it work on Windows but false-positived on the toolchain's
/// embedded std-source path, so it was reverted.)
fn inspect_binary(run_cmd: &str, cwd: &Path, forbidden: &[String]) -> Option<String> {
    if forbidden.is_empty() {
        return None;
    }
    // The verify command can be a full shell expression; the binary
    // path is the first token. Splitting on whitespace handles the
    // common case (`./target/release/foo` or `./build/foo`); fixtures
    // with more exotic verify commands can opt out by leaving
    // `forbidden_substrings` empty.
    let bin_token = run_cmd.split_whitespace().next()?;
    let raw_path = if bin_token.starts_with('/') {
        PathBuf::from(bin_token)
    } else {
        cwd.join(bin_token)
    };
    // The fixture's verify command names a suffix-less relative path
    // (`./target/release/foo`); on Windows the real artifact is
    // `foo.exe`. Resolve the actual on-disk file, trying the path as
    // written first and the platform-suffixed variant second.
    let Some(bin_path) = resolve_artifact(&raw_path) else {
        // Defensive — if the verify command has already failed at
        // the spawn step, we wouldn't be here. So a missing binary
        // is more likely a fixture misconfig.
        return Some(format!(
            "binary inspection: artifact not found at `{}`",
            crate::portable_path(&raw_path).display()
        ));
    };

    let strings_output = Command::new("strings")
        .arg(&bin_path)
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !strings_output.status.success() {
        // Couldn't run `strings` — skip silently. CI on a host
        // without binutils will see this as "no inspection ran",
        // which is OK; the test still has its other assertions.
        return None;
    }
    let bytes = strings_output.stdout;
    let haystack = String::from_utf8_lossy(&bytes);
    for needle in forbidden {
        if haystack.contains(needle.as_str()) {
            // Show a small window around the first hit for diagnostic
            // value (which file / construct exposed the leak).
            let idx = haystack.find(needle.as_str()).unwrap_or(0);
            let start = idx.saturating_sub(40);
            let end = (idx + needle.len() + 80).min(haystack.len());
            return Some(format!(
                "binary contains forbidden substring `{}` in {}: ...{}...",
                needle,
                bin_path.display(),
                &haystack[start..end].replace('\n', " ")
            ));
        }
    }
    None
}

/// Candidate on-disk paths for an inspected artifact, in priority
/// order. [`portable_path`](crate::portable_path) first tidies the mixed
/// `\.`/`/` separators that [`Path::join`] leaves on Windows. Then, when
/// `exe_suffix` is non-empty and not already present, a suffixed variant
/// is appended as a fallback (the fixture names `./target/release/foo`
/// but Windows produces `foo.exe`). Pure — no filesystem access — so the
/// suffix logic is testable on any host.
fn artifact_candidates(path: &Path, exe_suffix: &str) -> Vec<PathBuf> {
    let portable = crate::portable_path(path);
    let mut candidates = vec![portable.clone()];
    let already_suffixed = portable
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|ext| ext == exe_suffix.trim_start_matches('.'));
    if !exe_suffix.is_empty() && !already_suffixed {
        let mut suffixed = portable.into_os_string();
        suffixed.push(exe_suffix);
        candidates.push(PathBuf::from(suffixed));
    }
    candidates
}

/// The first existing [`artifact_candidates`] entry for `path`, using the
/// platform's [`EXE_SUFFIX`](std::env::consts::EXE_SUFFIX). `None` if no
/// candidate exists on disk. No-op fallback on Unix where `EXE_SUFFIX` is
/// empty.
fn resolve_artifact(path: &Path) -> Option<PathBuf> {
    artifact_candidates(path, std::env::consts::EXE_SUFFIX)
        .into_iter()
        .find(|c| c.exists())
}

/// Read a fixture-declared artifact under `base`, applying the same
/// platform-exe-suffix resolution as [`resolve_artifact`].
///
/// Fixtures name diff/run artifacts without a suffix (`target/release/foo`);
/// on Windows the real file is `foo.exe`. Reading the literal joined path
/// would `ENOENT`. Falls back to the unresolved join so a genuinely missing
/// artifact still produces a meaningful "not found" error.
fn read_declared_artifact(base: &Path, artifact: &str) -> std::io::Result<Vec<u8>> {
    let raw = base.join(artifact);
    let resolved = resolve_artifact(&raw).unwrap_or(raw);
    std::fs::read(resolved)
}

/// Scan every dep-info (`.d`) file under `root` for kache's target-dir
/// relativization sentinel.
///
/// kache relativizes `.d` files on store (`<target>/...` → `./...`) and
/// must expand them back on restore. A real rustc `.d` writes its
/// output targets as ABSOLUTE paths, so a restored `.d` containing
/// `./debug/` or `./release/` can only mean the restore-side expansion
/// silently failed (the #100 bug class — the in-place `noop` /
/// `should_not_recompile` proxy can miss it because cargo may treat a
/// simple crate Fresh regardless). Returns `Some(reason)` naming the
/// first offending file, or `None` if every `.d` is clean.
fn inspect_restored_depinfo(root: &Path) -> Option<String> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("d") {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            for sentinel in ["./debug/", "./release/"] {
                if content.contains(sentinel) {
                    return Some(format!(
                        "restored dep-info `{}` carries kache's relativization \
                         sentinel `{}` — the restore-side path expansion did not run",
                        path.display(),
                        sentinel,
                    ));
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{artifact_candidates, inspect_restored_depinfo, resolve_artifact};
    use std::path::{Path, PathBuf};

    #[test]
    fn artifact_candidates_unix_is_single_unchanged() {
        // Empty suffix (Unix): the path as written is the only candidate.
        assert_eq!(
            artifact_candidates(Path::new("/proj/target/release/foo"), ""),
            vec![PathBuf::from("/proj/target/release/foo")]
        );
    }

    #[test]
    fn artifact_candidates_appends_windows_exe_suffix() {
        // Windows: try the suffix-less path, then `foo.exe`.
        assert_eq!(
            artifact_candidates(Path::new("/proj/target/release/foo"), ".exe"),
            vec![
                PathBuf::from("/proj/target/release/foo"),
                PathBuf::from("/proj/target/release/foo.exe"),
            ]
        );
    }

    #[test]
    fn artifact_candidates_does_not_double_suffix() {
        // A fixture that already names `foo.exe` must not get `foo.exe.exe`.
        assert_eq!(
            artifact_candidates(Path::new("/proj/target/release/foo.exe"), ".exe"),
            vec![PathBuf::from("/proj/target/release/foo.exe")]
        );
    }

    #[test]
    fn resolve_artifact_finds_existing_suffixless_binary() {
        // The Unix shape: the exact path exists, no suffix needed.
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("app");
        std::fs::write(&bin, b"\x7fELF").unwrap();
        assert_eq!(resolve_artifact(&bin).as_deref(), Some(bin.as_path()));
    }

    #[test]
    fn resolve_artifact_missing_is_none() {
        let dir = tempfile::tempdir().unwrap();
        assert!(resolve_artifact(&dir.path().join("nope")).is_none());
    }

    #[test]
    fn inspect_restored_depinfo_passes_on_absolute_paths() {
        // A correct rustc `.d`: output targets are absolute.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("crate-abc.d"),
            "/abs/proj/target/debug/deps/crate-abc.rlib: /abs/proj/src/lib.rs\n",
        )
        .unwrap();
        assert!(inspect_restored_depinfo(dir.path()).is_none());
    }

    #[test]
    fn inspect_restored_depinfo_catches_relativization_sentinel() {
        // A `.d` left in kache's relativized form — the #100 bug
        // signature. Must be caught.
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            nested.join("crate-abc.d"),
            "./debug/deps/crate-abc.rlib: src/lib.rs\n",
        )
        .unwrap();
        let reason = inspect_restored_depinfo(dir.path());
        assert!(reason.is_some(), "must catch a `./debug/` relativized .d");
        assert!(reason.unwrap().contains("./debug/"));
    }

    #[test]
    fn inspect_restored_depinfo_no_d_files_is_clean() {
        // A fixture with no `.d` files at all (e.g. a C `make` build):
        // nothing to check, trivially clean.
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("main.c"), "int main(){}").unwrap();
        assert!(inspect_restored_depinfo(dir.path()).is_none());
    }
}
