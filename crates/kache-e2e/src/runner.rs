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
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;
use tempfile::TempDir;

use crate::assertions::{
    AssertionCheck, all_passed, apply_metric_assertions, apply_noop_assertions,
    count_misses_by_crate,
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
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Cold => "cold",
            Phase::Warm => "warm",
            Phase::Noop => "noop",
            Phase::Relocate => "relocate",
        }
    }

    /// Should this phase run a `clean` step before `build`? `noop`
    /// skips the clean (that's literally what makes it the no-op test);
    /// `relocate` runs in a fresh dir that's already clean, but we
    /// still run `clean` defensively in case `cp -R` brought a stale
    /// target/ along.
    fn cleans_first(self) -> bool {
        !matches!(self, Phase::Noop)
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
    // run before we start measuring. Errors are intentionally swallowed
    // — `daemon stop` failing because nothing was running is normal.
    let _ = Command::new(kache_path)
        .arg("daemon")
        .arg("stop")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

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

    // `relocate` is held outside the loop because it needs its own
    // `cwd` (a copy of the fixture in a fresh tempdir) — kept distinct
    // here so the in-place phases (cold/warm/noop) stay simple and the
    // relocate-specific dir lifecycle doesn't leak into them.
    let phases = [Phase::Cold, Phase::Warm, Phase::Noop];
    let mut phase_results = Vec::with_capacity(phases.len() + 1);
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

                let (result, _post) = run_phase(
                    Phase::Relocate,
                    fixture,
                    relocated.path(),
                    kache_path,
                    cache_dir.path(),
                    &prev_summary,
                    prev_event_count,
                )?;
                phase_results.push(result);
                // `relocated` (TempDir) drops here, removing the copy.
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
fn run_phase(
    phase: Phase,
    fixture: &Fixture,
    cwd: &Path,
    kache_path: &Path,
    cache_dir: &Path,
    prev_summary: &crate::report::ReportSummary,
    prev_event_count: usize,
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

    // Verify: run the artifact, check stdout contract.
    let verify = fixture
        .verify
        .as_ref()
        .map(|v| run_verify(v, fixture, cwd, cache_dir));

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
            .map(|spec| apply_noop_assertions(spec, &build.stdout))
            .unwrap_or_default(),
        Phase::Relocate => fixture
            .assertions
            .relocate
            .as_ref()
            .map(|spec| apply_metric_assertions(spec, &delta, &phase_misses_by_crate, new_events))
            .unwrap_or_default(),
    };

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
    stdout: String,
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
fn prepare_relocated_dir(src: &Path) -> Result<TempDir> {
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
    Ok(dst)
}

/// Run one shell command in `cwd` with the fixture's env.
///
/// Uses `sh -c` so commands can include redirects / pipes naturally.
/// stdout is captured (assertions read it for `recompile_marker`);
/// stderr is inherited so failures are visible in CI logs without
/// needing to crack open results.json.
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
        .stderr(Stdio::inherit())
        .output()
        .with_context(|| format!("spawning `{cmd}` in {}", cwd.display()))?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    // Echo build stdout so CI logs show what happened, even though
    // we capture it for assertion checks.
    eprint!("{stdout}");
    Ok(StepOutcome {
        exit: output.status,
        stdout,
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
    let bin_path = if bin_token.starts_with('/') {
        std::path::PathBuf::from(bin_token)
    } else {
        cwd.join(bin_token)
    };
    if !bin_path.exists() {
        // Defensive — if the verify command has already failed at
        // the spawn step, we wouldn't be here. So a missing binary
        // is more likely a fixture misconfig.
        return Some(format!(
            "binary inspection: artifact not found at `{}`",
            bin_path.display()
        ));
    }

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
