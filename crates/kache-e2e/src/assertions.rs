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
    checks
}

/// Apply [`NoopAssertions`] against the build's stdout. The marker is
/// required when `should_not_recompile = true` — without one the check
/// would silently pass on every run, which is worse than a hard error.
pub fn apply_noop_assertions(spec: &NoopAssertions, build_stdout: &str) -> Vec<AssertionCheck> {
    if !spec.should_not_recompile {
        // Fixture explicitly accepts recompilation (skeleton case).
        return vec![AssertionCheck {
            name: "should_not_recompile",
            expected: "false (no constraint)".to_string(),
            actual: "n/a".to_string(),
            passed: true,
        }];
    }

    match &spec.recompile_marker {
        Some(marker) => {
            let recompiled = build_stdout.contains(marker);
            vec![AssertionCheck {
                name: "should_not_recompile",
                expected: format!("stdout does NOT contain `{marker}`"),
                actual: if recompiled {
                    format!("found `{marker}` in stdout")
                } else {
                    format!("`{marker}` absent")
                },
                passed: !recompiled,
            }]
        }
        None => vec![AssertionCheck {
            name: "should_not_recompile",
            expected: "recompile_marker configured".to_string(),
            actual: "no marker set; cannot evaluate".to_string(),
            passed: false,
        }],
    }
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
        };
        let checks = apply_metric_assertions(&spec, &summary(0, 5, 5, 0.0), &HashMap::new(), &[]);
        assert!(!all_passed(&checks));
        assert_eq!(checks[0].actual, "0");
        assert_eq!(checks[0].expected, ">= 1");
    }

    #[test]
    fn noop_skipped_constraint_passes_unconditionally() {
        let spec = NoopAssertions {
            should_not_recompile: false,
            recompile_marker: None,
        };
        let checks = apply_noop_assertions(&spec, "Compiling foo v0.1.0");
        assert!(all_passed(&checks));
    }

    #[test]
    fn noop_without_marker_fails_loudly() {
        // Documents the explicit-beats-magic choice: a true assertion
        // with no marker can't silently pass.
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: None,
        };
        let checks = apply_noop_assertions(&spec, "anything");
        assert!(!all_passed(&checks));
    }

    #[test]
    fn noop_with_marker_detects_recompilation() {
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: Some("Compiling".to_string()),
        };
        let recompiled = apply_noop_assertions(&spec, "   Compiling foo v0.1.0");
        let clean = apply_noop_assertions(&spec, "Finished `release` profile");
        assert!(!all_passed(&recompiled));
        assert!(all_passed(&clean));
    }
}
