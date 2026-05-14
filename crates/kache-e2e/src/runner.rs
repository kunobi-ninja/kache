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
};
use crate::fixture::{Fixture, Verify};
use crate::report;
use crate::result::{FixtureResult, PhaseResult, VerifyResult, fixture_status};

/// One lifecycle phase. Order matters — `cold` populates the cache,
/// `warm` consumes it, `noop` checks incrementality on top of `warm`.
#[derive(Debug, Clone, Copy)]
pub enum Phase {
    Cold,
    Warm,
    Noop,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Cold => "cold",
            Phase::Warm => "warm",
            Phase::Noop => "noop",
        }
    }

    /// Should this phase run a `clean` step before `build`? Only `noop`
    /// skips the clean — that's literally what makes it the no-op test.
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

    let phases = [Phase::Cold, Phase::Warm, Phase::Noop];
    let mut phase_results = Vec::with_capacity(phases.len());
    for phase in phases {
        let result = run_phase(phase, fixture, kache_path, cache_dir.path())?;
        let failed = result.status == "fail";
        phase_results.push(result);
        if failed {
            // Short-circuit: if cold fails, warm and noop are meaningless.
            // Recorded phases keep the `phase` field so consumers see
            // exactly where the run stopped.
            break;
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
fn run_phase(
    phase: Phase,
    fixture: &Fixture,
    kache_path: &Path,
    cache_dir: &Path,
) -> Result<PhaseResult> {
    if phase.cleans_first() {
        let status = run_step(&fixture.commands.clean, fixture, cache_dir)?;
        if !status.exit.success() {
            // Clean failures are unusual but not crashes; record as a
            // failed phase with build_wall_s=0 so consumers see what
            // happened.
            return Ok(PhaseResult {
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
            });
        }
    }

    let started = Instant::now();
    let build = run_step(&fixture.commands.build, fixture, cache_dir)?;
    let build_wall_s = started.elapsed().as_secs();
    let build_exit_code = build.exit.code().unwrap_or(1);

    if !build.exit.success() {
        return Ok(PhaseResult {
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
        });
    }

    // Verify: run the artifact, check stdout contract.
    let verify = fixture
        .verify
        .as_ref()
        .map(|v| run_verify(v, fixture, cache_dir));

    let (_typed, raw) = report::fetch(kache_path, cache_dir)?;
    let typed = serde_json::from_value::<crate::report::KacheReport>(raw.clone())?;

    let mut checks = match phase {
        Phase::Cold => fixture
            .assertions
            .cold
            .as_ref()
            .map(|spec| apply_metric_assertions(spec, &typed.summary))
            .unwrap_or_default(),
        Phase::Warm => fixture
            .assertions
            .warm
            .as_ref()
            .map(|spec| apply_metric_assertions(spec, &typed.summary))
            .unwrap_or_default(),
        Phase::Noop => fixture
            .assertions
            .noop
            .as_ref()
            .map(|spec| apply_noop_assertions(spec, &build.stdout))
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
    Ok(PhaseResult {
        phase: phase.name().to_string(),
        status: status.to_string(),
        build_wall_s,
        build_exit_code,
        verify,
        kache_report: raw,
        assertions: checks,
    })
}

struct StepOutcome {
    exit: std::process::ExitStatus,
    stdout: String,
}

/// Run one shell command in the fixture dir with the fixture's env.
///
/// Uses `sh -c` so commands can include redirects / pipes naturally.
/// stdout is captured (assertions read it for `recompile_marker`);
/// stderr is inherited so failures are visible in CI logs without
/// needing to crack open results.json.
fn run_step(cmd: &str, fixture: &Fixture, cache_dir: &Path) -> Result<StepOutcome> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .current_dir(&fixture.dir)
        .env("KACHE_CACHE_DIR", cache_dir)
        .envs(&fixture.env)
        .stderr(Stdio::inherit())
        .output()
        .with_context(|| format!("spawning `{cmd}` in {}", fixture.dir.display()))?;

    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    // Echo build stdout so CI logs show what happened, even though
    // we capture it for assertion checks.
    eprint!("{stdout}");
    Ok(StepOutcome {
        exit: output.status,
        stdout,
    })
}

/// Run [`Verify::run`] in the fixture dir, check exit + stdout contract.
fn run_verify(spec: &Verify, fixture: &Fixture, cache_dir: &Path) -> VerifyResult {
    let output = match Command::new("sh")
        .arg("-c")
        .arg(&spec.run)
        .current_dir(&fixture.dir)
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

    VerifyResult {
        exit_code,
        stdout,
        passed: true,
        failure_reason: None,
    }
}
