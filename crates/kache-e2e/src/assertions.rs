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

/// A compact hex+ASCII window of two byte slices around their first
/// difference, for diagnosing *why* two artifacts diverge (a PE/COFF
/// `TimeDateStamp` looks like 4 changed bytes in the header; an embedded
/// machine-local path shows up as readable ASCII). Shows up to 24 bytes
/// starting a little before `first`.
fn byte_window(label_a: &str, a: &[u8], label_b: &str, b: &[u8], first: usize) -> String {
    let start = first.saturating_sub(4);
    let end = (start + 24).min(a.len()).min(b.len());
    let hex = |s: &[u8]| {
        s.iter()
            .map(|x| format!("{x:02x}"))
            .collect::<Vec<_>>()
            .join("")
    };
    let asc = |s: &[u8]| {
        s.iter()
            .map(|&x| {
                if (0x20..0x7f).contains(&x) {
                    x as char
                } else {
                    '.'
                }
            })
            .collect::<String>()
    };
    format!(
        " @{start} {label_a}=[{}|{}] {label_b}=[{}|{}]",
        hex(&a[start..end]),
        asc(&a[start..end]),
        hex(&b[start..end]),
        asc(&b[start..end])
    )
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
                .map(|i| format!(
                    ", first at byte {i}{}",
                    byte_window("cold", base, "restored", actual, i)
                ))
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
                .map(|i| format!(
                    ", first at byte {i}{}",
                    byte_window("restored", restored, "fresh", fresh, i)
                ))
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
    if let Some(max) = spec.max_dep_info_runs {
        let total: u32 = phase_events.iter().map(|e| e.dep_info_runs).sum();
        checks.push(AssertionCheck::max("max_dep_info_runs", max, total));
    }
    if let Some(max) = spec.max_prediction_mismatches {
        let total: u32 = phase_events.iter().map(|e| e.prediction_mismatches).sum();
        checks.push(AssertionCheck::max("max_prediction_mismatches", max, total));
    }
    checks
}

/// Apply [`NoopAssertions`] using kache's per-phase event log.
///
/// Two checks, both required:
///
/// 1. `sum(compiler_runs) == 0`: kache never spawned the underlying
///    compiler — the cache served whatever the build tool asked for.
/// 2. **No non-passthrough events at all**: the build tool asked for
///    *nothing* — it reached a true no-op. Probe/query invocations
///    (`rustc -vV` and friends) run even on a fresh build and land as
///    `passthrough` events, so only those are exempt.
///
/// Check 2 exists because check 1 alone is kache grading its own
/// homework (kunobi-ninja/kache#677): restore-time mtime stamping kept
/// cargo permanently dirty, re-dispatching every unit on every build —
/// but each dispatch was a cache hit, so `compiler_runs` stayed 0 and
/// the suite stayed green. The event-count form is still deterministic
/// (unlike the original stdout `Compiling` grep, which raced cargo's
/// output, issue #135) while measuring what the *consumer* did rather
/// than what kache did.
///
/// `recompile_marker` in the fixture spec is preserved for backward
/// compatibility (fixtures still parse) but is no longer consulted.
pub fn apply_noop_assertions(spec: &NoopAssertions, phase_events: &[Event]) -> Vec<AssertionCheck> {
    apply_noop_assertions_mode(spec, phase_events, DispatchCheck::Hard)
}

/// How the no-dispatch check (check 2) is enforced.
#[derive(Clone, Copy, PartialEq)]
pub enum DispatchCheck {
    /// Any non-probe dispatch fails the phase.
    Hard,
    /// Dispatches are recorded in the check's `actual` but do not fail
    /// the phase. The ONLY user is the relocate-noop phase on Windows,
    /// where the first rebuild after a relocated warm restore still
    /// re-dispatches one all-hit wave (kunobi-ninja/kache#686). The
    /// carve-out is phase- and platform-scoped, keeps reporting what it
    /// sees, and is removed by that issue — this is deliberately NOT the
    /// silent global weakening that hid #677 after #136.
    ReportOnly,
}

pub fn apply_noop_assertions_mode(
    spec: &NoopAssertions,
    phase_events: &[Event],
    dispatch_check: DispatchCheck,
) -> Vec<AssertionCheck> {
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
    // A fresh no-op build must not dispatch ANY compile request to the
    // wrapper — not even ones kache serves from cache. `compiler_runs == 0`
    // alone is blind to that (kunobi-ninja/kache#677: restore-time mtime
    // stamping kept cargo re-dispatching every unit as a cache hit, and this
    // assertion stayed green). Only probe/query invocations (`rustc -vV`,
    // `--print`, i.e. `not-a-compile` passthroughs) are exempt: they run
    // even on a fresh build. Other passthroughs are NOT exempt — a compile
    // kache declined (excluded source, refused flags, uncached executable)
    // is still a compile the build tool dispatched, and it records
    // `compiler_runs == 0`, so the first check cannot see it either.
    let dispatched: Vec<String> = phase_events
        .iter()
        .filter(|e| !e.is_probe_passthrough())
        .map(|e| format!("{} ({})", e.crate_name, e.result))
        .collect();
    vec![
        AssertionCheck {
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
        },
        AssertionCheck {
            name: "noop_no_compile_dispatch",
            expected: match dispatch_check {
                DispatchCheck::Hard => {
                    "no non-passthrough events (build tool reached a true no-op)".to_string()
                }
                DispatchCheck::ReportOnly => {
                    "report-only on this platform/phase (kunobi-ninja/kache#686)".to_string()
                }
            },
            actual: if dispatched.is_empty() {
                format!(
                    "0 compile dispatches across {} event(s)",
                    phase_events.len()
                )
            } else {
                format!(
                    "{} compile dispatch(es): {}",
                    dispatched.len(),
                    dispatched.join(", ")
                )
            },
            passed: dispatched.is_empty() || dispatch_check == DispatchCheck::ReportOnly,
        },
    ]
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
            dups: 0,
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
            max_dep_info_runs: None,
            max_prediction_mismatches: None,
        };
        let checks = apply_metric_assertions(&spec, &summary(0, 0, 0, 0.0), &HashMap::new(), &[]);
        assert!(checks.is_empty());
    }

    /// The number that qualifies predictions for wider use. It only means
    /// anything if a non-zero count actually fails the phase.
    #[test]
    fn prediction_mismatches_are_summed_and_bounded() {
        let spec = |max: Option<u32>| MetricAssertions {
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
            max_dep_info_runs: None,
            max_prediction_mismatches: max,
        };
        let with_mismatches = |counts: &[u32]| -> Vec<Event> {
            counts
                .iter()
                .enumerate()
                .map(|(i, n)| {
                    let mut e = event(&format!("c{i}"), "local_hit", 0);
                    e.prediction_mismatches = *n;
                    e
                })
                .collect()
        };

        // Not asked for, not checked.
        assert!(
            apply_metric_assertions(
                &spec(None),
                &summary(0, 0, 0, 0.0),
                &HashMap::new(),
                &with_mismatches(&[3])
            )
            .is_empty()
        );

        // Every prediction agreed: the phase is evidence, and it passes.
        let agreed = apply_metric_assertions(
            &spec(Some(0)),
            &summary(0, 0, 0, 0.0),
            &HashMap::new(),
            &with_mismatches(&[0, 0, 0]),
        );
        assert_eq!(agreed.len(), 1);
        assert!(agreed[0].passed, "{agreed:?}");

        // One disagreement anywhere in the phase fails it. Summed, not per
        // event: a hole in the argument is a hole wherever it shows up.
        let disagreed = apply_metric_assertions(
            &spec(Some(0)),
            &summary(0, 0, 0, 0.0),
            &HashMap::new(),
            &with_mismatches(&[0, 1, 0]),
        );
        assert_eq!(disagreed.len(), 1);
        assert!(
            !disagreed[0].passed,
            "a mismatch must fail the phase: {disagreed:?}"
        );
        assert_eq!(disagreed[0].actual, "1");
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
            max_dep_info_runs: None,
            max_prediction_mismatches: None,
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
            max_dep_info_runs: None,
            max_prediction_mismatches: None,
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
            dep_info_runs: 0,
            prediction_mismatches: 0,
            passthrough_reason: String::new(),
        }
    }

    fn passthrough_event(crate_name: &str, reason: &str) -> Event {
        Event {
            passthrough_reason: reason.to_string(),
            ..event(crate_name, "passthrough", 0)
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
    fn noop_fails_when_units_were_dispatched_even_as_hits() {
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: None,
        };
        // All local_hits → zero compiler_runs, so the old check passes —
        // but the build tool still re-dispatched two units on a build
        // that should have been a no-op. That's the #677 failure shape
        // and it must fail the phase.
        let events = vec![event("foo", "local_hit", 0), event("bar", "local_hit", 0)];
        let checks = apply_noop_assertions(&spec, &events);
        assert!(checks[0].passed, "compiler_runs check should still pass");
        assert!(
            !checks[1].passed,
            "dispatch check must catch hit-served re-dispatch"
        );
        assert!(!all_passed(&checks));
        assert!(checks[1].actual.contains("foo (local_hit)"));
    }

    #[test]
    fn noop_allows_probe_passthrough_events() {
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: None,
        };
        // A fresh cargo build still runs `rustc -vV` through the wrapper;
        // those probe/query invocations land as `not-a-compile`
        // passthrough events and must not fail the no-op contract.
        let events = vec![passthrough_event(
            "",
            "not-a-compile|query / probe (--print, -vV)",
        )];
        let checks = apply_noop_assertions(&spec, &events);
        assert!(all_passed(&checks));
    }

    #[test]
    fn noop_fails_on_compile_shaped_passthrough() {
        let spec = NoopAssertions {
            should_not_recompile: true,
            recompile_marker: None,
        };
        // A compile kache DECLINED (excluded source, refused flags,
        // uncached executable) is still a compile the build tool
        // dispatched on a phase that should be a no-op. These events
        // record `compiler_runs == 0` (the passthrough path never calls
        // record_compiler_run), so only the reason category can catch
        // them — exempting the whole passthrough class is a false
        // negative.
        let events = vec![passthrough_event(
            "build_script_build",
            "unsupported|user-facing executable (cache_executables=false)",
        )];
        let checks = apply_noop_assertions(&spec, &events);
        assert!(checks[0].passed, "compiler_runs stays 0 on this path");
        assert!(
            !checks[1].passed,
            "compile-shaped passthrough must fail the dispatch check"
        );
        assert!(checks[1].actual.contains("build_script_build"));
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
