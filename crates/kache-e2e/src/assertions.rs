//! Assertion application + result records.
//!
//! Each declared assertion in `[assertions.<phase>]` becomes one
//! [`AssertionCheck`] in the result, recording **expected**, **actual**,
//! and pass/fail. This means a failing run shows exactly which constraint
//! tripped and with what value — diff-friendly between runs, no need to
//! re-run with verbose flags to find the failure.

use serde::Serialize;

use crate::fixture::{MetricAssertions, NoopAssertions};
use crate::report::ReportSummary;

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

/// Apply [`MetricAssertions`] against a [`ReportSummary`]. Each declared
/// constraint produces one check; absent constraints are silently skipped
/// (this is how a fixture opts in to only the assertions it cares about).
pub fn apply_metric_assertions(
    spec: &MetricAssertions,
    summary: &ReportSummary,
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
        };
        let checks = apply_metric_assertions(&spec, &summary(0, 0, 0, 0.0));
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
        };
        let checks = apply_metric_assertions(&spec, &summary(5, 0, 5, 100.0));
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
        };
        let checks = apply_metric_assertions(&spec, &summary(0, 5, 5, 0.0));
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
