//! Assertion application + result records.
//!
//! Each declared assertion in `[assertions.<phase>]` becomes one
//! [`AssertionCheck`] in the result, recording **expected**, **actual**,
//! and pass/fail. This means a failing run shows exactly which constraint
//! tripped and with what value — diff-friendly between runs, no need to
//! re-run with verbose flags to find the failure.

use serde::Serialize;

use crate::fixture::{MetricAssertions, NoopAssertions};
use crate::report::{Event, ReportSummary};
use std::collections::HashMap;

/// One assertion outcome. Field shape is stable enough that downstream
/// tooling (CI annotations, dashboards) can rely on it.
#[derive(Debug, Clone, Serialize)]
pub struct AssertionCheck {
    /// Short identifier matching the toml field (e.g. `"min_hits"`).
    pub name: &'static str,
    /// Human-readable description of the constraint
    /// (e.g. `">= 1"`, `"<= 0"`).
    pub expected: String,
    /// Stringified actual value pulled from the report or stdout.
    pub actual: String,
    pub passed: bool,
}

impl AssertionCheck {
    fn min<T: PartialOrd + std::fmt::Display>(name: &'static str, threshold: T, actual: T) -> Self {
        let passed = actual >= threshold;
        Self {
            name,
            expected: format!(">= {threshold}"),
            actual: actual.to_string(),
            passed,
        }
    }

    fn max<T: PartialOrd + std::fmt::Display>(name: &'static str, threshold: T, actual: T) -> Self {
        let passed = actual <= threshold;
        Self {
            name,
            expected: format!("<= {threshold}"),
            actual: actual.to_string(),
            passed,
        }
    }
}

/// Count misses per crate from a slice of events (e.g. the delta
/// between two phase snapshots).
///
/// Helper for per-crate assertions — exposed so consumers can pre-
/// compute the map once and pass it to multiple checks if needed.
pub fn count_misses_by_crate(events: &[Event]) -> HashMap<String, u64> {
    let mut by_crate: HashMap<String, u64> = HashMap::new();
    for event in events {
        if event.result == "miss" {
            *by_crate.entry(event.crate_name.clone()).or_insert(0) += 1;
        }
    }
    by_crate
}

/// Differential check: a cache-hit phase's restored artifact must be
/// byte-identical to the cold build's real-compiler output.
///
/// `baseline` is the cold-phase bytes for this artifact, or `None` if
/// cold never recorded it (a fixture misconfiguration — surfaced as a
/// failed check rather than silently skipped).
pub fn diff_artifact_check(
    artifact: &str,
    baseline: Option<&Vec<u8>>,
    actual: &[u8],
) -> AssertionCheck {
    // Box-leak the artifact-qualified name for `name: &'static str`;
    // bounded — a fixture declares a small fixed set of artifacts.
    let name: &'static str = Box::leak(format!("diff_match[{artifact}]").into_boxed_str());
    let Some(base) = baseline else {
        return AssertionCheck {
            name,
            expected: "cold-phase baseline recorded".to_string(),
            actual: "no baseline — cold did not produce this artifact".to_string(),
            passed: false,
        };
    };
    let passed = base.as_slice() == actual;
    let actual = if passed {
        format!("byte-identical ({} bytes)", actual.len())
    } else {
        let first = base.iter().zip(actual).position(|(a, b)| a != b);
        format!(
            "differs (cold {} B, restored {} B{})",
            base.len(),
            actual.len(),
            first
                .map(|i| format!(", first at byte {i}"))
                .unwrap_or_default()
        )
    };
    AssertionCheck {
        name,
        expected: "byte-identical to cold build".to_string(),
        actual,
        passed,
    }
}

/// Differential relocate check: a cache-restored artifact (built at the
/// relocated path with kache hitting) must be byte-identical to a FRESH
/// real compile at that *same* relocated path (kache disabled).
///
/// This is strictly stronger than [`diff_artifact_check`] on the
/// relocate phase. That check compares the relocate hit against the
/// cold baseline — but a relocate *hit* restores the very blob cold
/// cached, so the comparison only proves store/restore fidelity, not
/// path-independence. Here `fresh` is a genuine compile at the new
/// path: byte-equality positively proves no machine-local build path
/// leaked into the artifact, so a cross-path cache hit is provably safe.
///
/// `restored` is the cache-hit relocate-phase artifact; `fresh` is the
/// `KACHE_DISABLED=1` compile at the relocated path. The check is named
/// `relocate_diff_match[<artifact>]` — distinct from `diff_match[...]`
/// so both appear side-by-side in results JSON.
pub fn relocate_diff_artifact_check(
    artifact: &str,
    restored: &[u8],
    fresh: &[u8],
) -> AssertionCheck {
    // Box-leak the artifact-qualified name for `name: &'static str`;
    // bounded — a fixture declares a small fixed set of artifacts.
    let name: &'static str = Box::leak(format!("relocate_diff_match[{artifact}]").into_boxed_str());
    let passed = restored == fresh;
    let actual = if passed {
        format!("byte-identical ({} bytes)", fresh.len())
    } else {
        let first = restored.iter().zip(fresh).position(|(a, b)| a != b);
        format!(
            "differs (restored {} B, fresh {} B{})",
            restored.len(),
            fresh.len(),
            first
                .map(|i| format!(", first at byte {i}"))
                .unwrap_or_default()
        )
    };
    AssertionCheck {
        name,
        expected: "byte-identical to fresh compile at relocated path".to_string(),
        actual,
        passed,
    }
}

/// Apply [`MetricAssertions`] against a [`ReportSummary`]. Each declared
/// constraint produces one check; absent constraints are silently skipped
/// (this is how a fixture opts in to only the assertions it cares about).
///
/// `phase_misses_by_crate` is the per-crate miss count for THIS phase
/// (the delta between pre/post event snapshots). Required because
/// per-crate assertions can't be derived from the aggregate summary —
/// the summary's `misses` field is a sum, not a per-name breakdown.
///
/// `phase_events` is this phase's event slice — used to sum the
/// op-count fields (`compiler_runs`, `preprocessor_runs`) for the
/// `max_*_runs` assertions, which the aggregate summary doesn't carry.
pub fn apply_metric_assertions(
    spec: &MetricAssertions,
    summary: &ReportSummary,
    phase_misses_by_crate: &HashMap<String, u64>,
    phase_events: &[Event],
) -> Vec<AssertionCheck> {
    let mut checks = Vec::new();
    if let Some(min) = spec.min_entries_after {
        checks.push(AssertionCheck::min(
            "min_entries_after",
            min,
            summary.total_crates,
        ));
    }
    if let Some(max) = spec.max_entries_after {
        checks.push(AssertionCheck::max(
            "max_entries_after",
            max,
            summary.total_crates,
        ));
    }
    if let Some(min) = spec.min_hits {
        checks.push(AssertionCheck::min("min_hits", min, summary.total_hits()));
    }
    if let Some(min) = spec.min_misses {
        checks.push(AssertionCheck::min("min_misses", min, summary.misses));
    }
    if let Some(max) = spec.max_misses {
        checks.push(AssertionCheck::max("max_misses", max, summary.misses));
    }
    if let Some(min) = spec.min_hit_rate_pct {
        checks.push(AssertionCheck::min(
            "min_hit_rate_pct",
            min,
            summary.hit_rate_pct,
        ));
    }
    // Per-crate miss assertions: declared as a map in the toml, one
    // check per (crate_name, min_count) pair. Sorted by crate_name
    // so check ordering is deterministic across runs (helps with
    // diffing results.json snapshots in CI).
    let mut per_crate_pairs: Vec<(&String, &u64)> = spec.min_misses_per_crate.iter().collect();
    per_crate_pairs.sort_by_key(|(name, _)| name.as_str());
    for (crate_name, min) in per_crate_pairs {
        let actual = phase_misses_by_crate.get(crate_name).copied().unwrap_or(0);
        let passed = actual >= *min;
        checks.push(AssertionCheck {
            // Box-leak the crate-qualified name so it lives long
            // enough for `name: &'static str`. Acceptable: each
            // fixture declares a small fixed set of these, so the
            // total leak is bounded.
            name: Box::leak(format!("min_misses_for[{crate_name}]").into_boxed_str()),
            expected: format!(">= {min}"),
            actual: actual.to_string(),
            passed,
        });
    }
    // Op-count budgets: summed across this phase's events. These are
    // deterministic (counts, not timings) so they gate CI reliably —
    // `max_compiler_runs = 0` is the headline "the cache actually
    // skipped the compile" assertion.
    if let Some(max) = spec.max_compiler_runs {
        let total: u32 = phase_events.iter().map(|e| e.compiler_runs).sum();
        checks.push(AssertionCheck::max("max_compiler_runs", max, total));
    }
    if let Some(max) = spec.max_preprocessor_runs {
        let total: u32 = phase_events.iter().map(|e| e.preprocessor_runs).sum();
        checks.push(AssertionCheck::max("max_preprocessor_runs", max, total));
    }
    if let Some(max) = spec.max_probe_runs {
        let total: u32 = phase_events.iter().map(|e| e.probe_runs).sum();
        checks.push(AssertionCheck::max("max_probe_runs", max, total));
    }
    checks
}

/// Apply [`NoopAssertions`] using kache's per-phase event log.
///
/// **Authoritative signal: `sum(compiler_runs) == 0` across the noop
/// phase's events.** If kache ran the underlying compiler for any
/// crate in this phase, the cache failed to serve the build — that's
/// the *real* "did we recompile" question.
///
/// Why not grep cargo's stdout for `Compiling` (the old approach)?
/// Because cargo prints `Compiling <crate>` *before* invoking the
/// `RUSTC_WRAPPER`, regardless of whether that wrapper hits the cache
/// or runs rustc. Cargo's freshness check is mtime-driven, which is
/// non-deterministic under sub-second restore timing (issue #135) —
/// the stdout marker fires when cargo *announced* a build, not when
/// rustc actually ran. The event log records what kache *did*, which
/// is the deterministic signal: zero compiler invocations means every
/// crate was served from cache, no matter what cargo printed.
///
/// `recompile_marker` in the fixture spec is preserved for backward
/// compatibility (fixtures still parse) but is no longer consulted.
pub fn apply_noop_assertions(spec: &NoopAssertions, phase_events: &[Event]) -> Vec<AssertionCheck> {
    if !spec.should_not_recompile {
        // Fixture explicitly accepts recompilation (skeleton case).
        return vec![AssertionCheck {
            name: "should_not_recompile",
            expected: "false (no constraint)".to_string(),
            actual: "n/a".to_string(),
            passed: true,
        }];
    }

    let total_compiler_runs: u32 = phase_events.iter().map(|e| e.compiler_runs).sum();
    let recompiled_crates: Vec<&str> = phase_events
        .iter()
        .filter(|e| e.compiler_runs > 0)
        .map(|e| e.crate_name.as_str())
        .collect();
    vec![AssertionCheck {
        name: "should_not_recompile",
        expected: "sum(compiler_runs) == 0 across phase events".to_string(),
        actual: if total_compiler_runs == 0 {
            format!(
                "0 compiler_runs across {} event(s) — kache served everything",
                phase_events.len()
            )
        } else {
            format!(
                "{} compiler_run(s) across {} event(s); recompiled crate(s): {}",
                total_compiler_runs,
                phase_events.len(),
                recompiled_crates.join(", ")
            )
        },
        passed: total_compiler_runs == 0,
    }]
}

/// True iff every check in `checks` passed.
pub fn all_passed(checks: &[AssertionCheck]) -> bool {
    checks.iter().all(|c| c.passed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(hits: u64, misses: u64, total: u64, rate: f64) -> ReportSummary {
        ReportSummary {
            hit_rate_pct: rate,
            total_crates: total,
            local_hits: hits,
            prefetch_hits: 0,
            remote_hits: 0,
            misses,
        }
    }

    #[test]
    fn metric_assertions_only_evaluate_declared_constraints() {
        // Empty spec → no checks at all. Ensures fixtures that declare
        // [assertions.cold] = {} still parse and produce zero noise.
        let spec = MetricAssertions {
            min_entries_after: None,
            max_entries_after: None,
            min_hits: None,
            min_misses: None,
            max_misses: None,
            min_hit_rate_pct: None,
            min_misses_per_crate: HashMap::new(),
            max_compiler_runs: None,
            max_preprocessor_runs: None,
            max_probe_runs: None,
        };
        let checks = apply_metric_assertions(&spec, &summary(0, 0, 0, 0.0), &HashMap::new(), &[]);
        assert!(checks.is_empty());
    }

    #[test]
    fn min_hits_passes_when_actual_meets_threshold() {
        let spec = MetricAssertions {
            min_entries_after: None,
            max_entries_after: None,
            min_hits: Some(1),
            min_misses: None,
            max_misses: None,
            min_hit_rate_pct: None,
            min_misses_per_crate: HashMap::new(),
            max_compiler_runs: None,
            max_preprocessor_runs: None,
            max_probe_runs: None,
        };
        let checks = apply_metric_assertions(&spec, &summary(5, 0, 5, 100.0), &HashMap::new(), &[]);
        assert!(all_passed(&checks));
    }

    #[test]
    fn min_hits_fails_when_actual_below_threshold() {
        let spec = MetricAssertions {
            min_entries_after: None,
            max_entries_after: None,
            min_hits: Some(1),
            min_misses: None,
            max_misses: None,
            min_hit_rate_pct: None,
            min_misses_per_crate: HashMap::new(),
            max_compiler_runs: None,
            max_preprocessor_runs: None,
            max_probe_runs: None,
        };
        let checks = apply_metric_assertions(&spec, &summary(0, 5, 5, 0.0), &HashMap::new(), &[]);
        assert!(!all_passed(&checks));
        assert_eq!(checks[0].actual, "0");
        assert_eq!(checks[0].expected, ">= 1");
    }

    fn event(crate_name: &str, result: &str, compiler_runs: u32) -> Event {
        Event {
            crate_name: crate_name.to_string(),
            result: result.to_string(),
            compiler_runs,
            preprocessor_runs: 0,
            probe_runs: 0,
        }
    }

    #[test]
    fn noop_skipped_constraint_passes_unconditionally() {
        let spec = NoopAssertions {
            should_not_recompile: false,
            recompile_marker: None,
        };
        // Even with a real recompile event, the assertion is satisfied
        // because the fixture explicitly opted out.
        let checks = apply_noop_assertions(&spec, &[event("foo", "miss", 1)]);
        assert!(all_passed(&checks));
    }

    #[test]
    fn noop_passes_when_every_event_was_a_cache_hit() {
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: None,
        };
        // All local_hits → zero compiler_runs → assertion passes
        // regardless of what cargo's stdout looked like (which was the
        // flaky #135 signal).
        let events = vec![event("foo", "local_hit", 0), event("bar", "local_hit", 0)];
        let checks = apply_noop_assertions(&spec, &events);
        assert!(all_passed(&checks));
        assert!(checks[0].actual.contains("0 compiler_runs"));
    }

    #[test]
    fn noop_fails_when_any_event_spawned_the_compiler() {
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: None,
        };
        // One miss → kache spawned rustc → noop didn't deliver.
        let events = vec![event("foo", "local_hit", 0), event("bar", "miss", 1)];
        let checks = apply_noop_assertions(&spec, &events);
        assert!(!all_passed(&checks));
        assert!(checks[0].actual.contains("bar"));
    }

    #[test]
    fn noop_passes_on_an_empty_event_window() {
        // The phase didn't produce any cache events at all — cargo
        // skipped rustc entirely. That's the *strongest* form of "no
        // recompile" so the assertion must pass.
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: None,
        };
        let checks = apply_noop_assertions(&spec, &[]);
        assert!(all_passed(&checks));
    }

    #[test]
    fn diff_artifact_check_passes_on_identical_bytes() {
        let base = vec![1u8, 2, 3, 4];
        let check = diff_artifact_check("build/foo.o", Some(&base), &[1, 2, 3, 4]);
        assert!(check.passed);
        assert!(check.actual.contains("byte-identical"));
    }

    #[test]
    fn diff_artifact_check_fails_on_differing_bytes() {
        let base = vec![1u8, 2, 3, 4];
        let check = diff_artifact_check("build/foo.o", Some(&base), &[1, 2, 9, 4]);
        assert!(!check.passed);
        // The first differing byte is reported for diagnostics.
        assert!(check.actual.contains("first at byte 2"));
    }

    #[test]
    fn diff_artifact_check_fails_when_no_baseline() {
        // Cold never recorded the artifact → can't compare → fail
        // loudly rather than silently pass.
        let check = diff_artifact_check("build/foo.o", None, &[1, 2, 3]);
        assert!(!check.passed);
    }

    #[test]
    fn relocate_diff_artifact_check_passes_on_identical_bytes() {
        let restored = vec![1u8, 2, 3, 4];
        let check = relocate_diff_artifact_check("build/foo.o", &restored, &[1, 2, 3, 4]);
        assert!(check.passed);
        assert!(check.actual.contains("byte-identical"));
        assert_eq!(check.name, "relocate_diff_match[build/foo.o]");
    }

    #[test]
    fn relocate_diff_artifact_check_fails_on_differing_bytes() {
        // A cache-restored artifact that differs from a fresh compile
        // at the relocated path means a build path leaked into the
        // cached blob — surfaced with the first differing byte.
        let restored = vec![1u8, 2, 3, 4];
        let check = relocate_diff_artifact_check("build/foo.o", &restored, &[1, 2, 9, 4]);
        assert!(!check.passed);
        assert!(check.actual.contains("first at byte 2"));
    }
}
