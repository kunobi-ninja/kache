//! Result records — the JSON shape the harness writes to disk.
//!
//! The output file (`results.json`) is the harness's contract with the
//! outside world: CI artifacts, dashboards, and PR diff tooling all read
//! this. Field shapes are intentionally stable; new optional fields are
//! preferred over breaking renames.

use serde::Serialize;
use std::collections::HashMap;

use crate::assertions::AssertionCheck;

/// Top-level results document written to `--out`.
#[derive(Debug, Serialize)]
pub struct RunResults {
    /// Absolute path to the kache binary that was tested.
    pub kache_binary: String,
    /// `kache --version` output, captured once at startup.
    pub kache_version: String,
    /// Platform string, formatted as `Os-Arch` (e.g. `Darwin-arm64`).
    pub platform: String,
    /// Per-fixture results, in discovery order (alphabetical by name).
    pub fixtures: Vec<FixtureResult>,
}

#[derive(Debug, Serialize)]
pub struct FixtureResult {
    pub name: String,
    /// `"pass"` if every phase passed; `"fail"` otherwise. Sized as a
    /// string (not bool) so future states (`"skip"`, `"flaky"`) can land
    /// without breaking consumers.
    pub status: String,
    pub phases: Vec<PhaseResult>,
}

#[derive(Debug, Serialize)]
pub struct PhaseResult {
    /// `"cold" | "warm" | "noop"`.
    pub phase: String,
    pub status: String,
    /// Wall-clock seconds for the build step (last step in the phase).
    pub build_wall_s: u64,
    /// Build's exit code. Non-zero short-circuits the rest of the phase.
    pub build_exit_code: i32,
    /// Verify result, if `[verify]` was declared on the fixture.
    pub verify: Option<VerifyResult>,
    /// Full `kache report --format json` output for the fixture's
    /// isolated cache dir. Forwarded raw so downstream consumers can
    /// read fields the harness doesn't currently assert on.
    pub kache_report: serde_json::Value,
    /// One entry per assertion that was evaluated. Empty if the fixture
    /// declared no assertions for this phase.
    pub assertions: Vec<AssertionCheck>,
}

#[derive(Debug, Serialize)]
pub struct VerifyResult {
    pub exit_code: i32,
    pub stdout: String,
    pub passed: bool,
    /// What failed, if anything (`"exit_code mismatch"`,
    /// `"missing substring: foo"`). Empty when `passed = true`.
    pub failure_reason: Option<String>,
}

/// Aggregate phase statuses into a fixture status.
pub fn fixture_status(phases: &[PhaseResult]) -> String {
    if phases.iter().all(|p| p.status == "pass") {
        "pass".to_string()
    } else {
        "fail".to_string()
    }
}

/// Helper for emitting the per-test-run env / metadata used when
/// constructing [`RunResults`]. Centralized here so the binary can stay
/// focused on orchestration.
pub fn collect_meta(kache_path: &std::path::Path) -> HashMap<&'static str, String> {
    let mut m = HashMap::new();
    m.insert("kache_binary", kache_path.display().to_string());
    m.insert(
        "platform",
        format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
    );
    m
}
