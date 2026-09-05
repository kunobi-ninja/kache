//! Typed view of `kache report --format json` output.
//!
//! Only fields used by assertion checks are typed explicitly; the rest is
//! captured as raw JSON and forwarded into the result file. This keeps the
//! harness compatible with future report fields without churn — kache can
//! grow new metrics, the harness keeps passing them through.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;

/// Expected report schema version supported by this E2E reader.
pub const SUPPORTED_REPORT_SCHEMA_VERSION: u32 = 1;

/// Raw `kache report --format json` document, deserialized with the
/// summary surface lifted out for assertions and the full body kept as
/// `serde_json::Value` for verbatim forwarding.
#[derive(Debug, Clone, Deserialize)]
pub struct KacheReport {
    pub schema_version: u32,
    pub summary: ReportSummary,
    /// Time-ordered event list — every cache lookup the wrapper has
    /// recorded inside the report's window. Used for per-crate
    /// assertions that the aggregate `summary` can't express
    /// (e.g. "this specific crate must miss on relocate"). Order is
    /// append-only, so a phase's events are the suffix beyond the
    /// previous phase's snapshot.
    #[serde(default)]
    pub all_events: Vec<Event>,
}

/// Per-crate cache event from `kache report`. Subset of the actual
/// schema — only fields used by assertions are typed; new fields
/// from kache pass through via the raw report.
#[derive(Debug, Clone, Deserialize)]
pub struct Event {
    pub crate_name: String,
    /// `"hit"` | `"miss"` | other future variants. Compared as a
    /// string to stay compatible with kache adding new event kinds.
    pub result: String,
    /// Times kache spawned the underlying compiler for this event
    /// (0 on a hit, 1 on a miss). `#[serde(default)]` so reports from
    /// an older kache without the field still deserialize.
    #[serde(default)]
    pub compiler_runs: u32,
    /// Times kache spawned the preprocessor (`cc -E`) for this event.
    #[serde(default)]
    pub preprocessor_runs: u32,
    /// Times kache spawned the rustc dep-info pre-pass for this event.
    /// `0` on a warm predicted hit. `#[serde(default)]` so reports from
    /// an older kache without the field still deserialize.
    #[serde(default)]
    pub dep_info_runs: u32,
    /// Times kache spawned a compiler probe (`cc --version` / `cc -###`)
    /// for this event. `#[serde(default)]` so reports from an older
    /// kache without the field still deserialize.
    #[serde(default)]
    pub probe_runs: u32,
    /// `category|detail` string on passthrough events (empty otherwise).
    /// The `not-a-compile` category marks probe/query invocations
    /// (`rustc -vV`, `--print`), which run even on a fresh no-op build;
    /// every other category is a real compile the wrapper declined.
    #[serde(default)]
    pub passthrough_reason: String,
}

impl Event {
    /// True iff this event is a probe/query invocation rather than a
    /// compile request — the only wrapper traffic a true no-op build
    /// produces. Mirrors kache's own `is_probe_passthrough` classifier.
    pub fn is_probe_passthrough(&self) -> bool {
        self.result == "passthrough"
            && self
                .passthrough_reason
                .split('|')
                .next()
                .is_some_and(|category| category.trim() == "not-a-compile")
    }
}

/// Subset of the `summary` block that assertions read against.
///
/// Field names mirror the report verbatim. New fields land here only
/// when an assertion needs them; everything else stays in the raw value.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReportSummary {
    pub hit_rate_pct: f64,
    pub total_crates: u64,
    pub local_hits: u64,
    pub prefetch_hits: u64,
    pub remote_hits: u64,
    #[serde(default)]
    pub dups: u64,
    pub misses: u64,
}

impl ReportSummary {
    /// Aggregate hits across all sources (local + prefetch + remote).
    pub fn total_hits(&self) -> u64 {
        self.local_hits + self.prefetch_hits + self.remote_hits
    }

    /// Entries where the compiler ran because the cache key missed.
    pub fn total_compiled(&self) -> u64 {
        self.dups + self.misses
    }

    /// Compute `self - earlier` for per-phase delta semantics.
    ///
    /// `kache report --since 1h` is cumulative over the time window —
    /// a single phase's hits/misses count is meaningless because
    /// earlier phases inflate the totals. Snapshotting before/after
    /// each phase and subtracting gives the per-phase signal that
    /// per-phase assertions want.
    ///
    /// `hit_rate_pct` is recomputed from the delta hits/dups/misses (NOT
    /// subtracted, since rates don't subtract meaningfully). Returns
    /// `0.0` when the delta `total_crates` is zero — a phase that
    /// did nothing is `0%`, not `NaN`.
    pub fn delta_since(&self, earlier: &ReportSummary) -> ReportSummary {
        let local_hits = self.local_hits.saturating_sub(earlier.local_hits);
        let prefetch_hits = self.prefetch_hits.saturating_sub(earlier.prefetch_hits);
        let remote_hits = self.remote_hits.saturating_sub(earlier.remote_hits);
        let dups = self.dups.saturating_sub(earlier.dups);
        let misses = self.misses.saturating_sub(earlier.misses);
        let hits = local_hits + prefetch_hits + remote_hits;
        let total_crates = hits + dups + misses;
        let hit_rate_pct = if total_crates == 0 {
            0.0
        } else {
            (hits as f64 / total_crates as f64) * 100.0
        };
        ReportSummary {
            hit_rate_pct,
            total_crates,
            local_hits,
            prefetch_hits,
            remote_hits,
            dups,
            misses,
        }
    }
}

/// An empty (all-zeroes) summary, used as the "before first phase"
/// snapshot so the delta logic stays uniform across all phases —
/// no special-casing for the first one.
pub fn empty_summary() -> ReportSummary {
    ReportSummary {
        hit_rate_pct: 0.0,
        total_crates: 0,
        local_hits: 0,
        prefetch_hits: 0,
        remote_hits: 0,
        dups: 0,
        misses: 0,
    }
}

/// Invoke `<kache> report --format json --since 1h` against `cache_dir`.
///
/// Thin wrapper over [`fetch_since`] with the 1-hour window the e2e
/// harness uses for its short-lived fixtures.
pub fn fetch(kache_path: &Path, cache_dir: &Path) -> Result<(KacheReport, serde_json::Value)> {
    fetch_since(kache_path, cache_dir, "1h")
}

/// Invoke `<kache> report --format json --since <since>` against the
/// given `cache_dir` and return both the typed report and the raw value.
///
/// `since` is a kache duration string (`"1h"`, `"7d"`, `"365d"`). The
/// e2e fixtures finish in seconds so `"1h"` is plenty; the Firefox
/// benchmark passes a much wider window because a cold build runs for
/// hours.
///
/// The raw value is what gets written to the result file so consumers
/// can inspect any field, not only the ones typed out here.
pub fn fetch_since(
    kache_path: &Path,
    cache_dir: &Path,
    since: &str,
) -> Result<(KacheReport, serde_json::Value)> {
    fetch_since_with_root(kache_path, cache_dir, since, None)
}

/// Parse and validate report JSON bytes against the supported schema version.
pub fn parse_report_json(bytes: &[u8]) -> Result<(KacheReport, serde_json::Value)> {
    let raw: serde_json::Value =
        serde_json::from_slice(bytes).context("parsing kache report JSON")?;
    let typed: KacheReport =
        serde_json::from_value(raw.clone()).context("extracting summary from kache report")?;
    if typed.schema_version != SUPPORTED_REPORT_SCHEMA_VERSION {
        anyhow::bail!(
            "unsupported report schema_version {} (expected {})",
            typed.schema_version,
            SUPPORTED_REPORT_SCHEMA_VERSION
        );
    }
    Ok((typed, raw))
}

pub fn fetch_since_with_root(
    kache_path: &Path,
    cache_dir: &Path,
    since: &str,
    root: Option<&Path>,
) -> Result<(KacheReport, serde_json::Value)> {
    let mut cmd = Command::new(kache_path);
    cmd.args(["report", "--format", "json", "--since", since])
        .env("KACHE_CACHE_DIR", cache_dir);
    if let Some(root) = root {
        cmd.arg("--root").arg(root);
    }
    let output = cmd
        .output()
        .with_context(|| format!("running `{} report`", kache_path.display()))?;

    if !output.status.success() {
        anyhow::bail!(
            "kache report exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    parse_report_json(&output.stdout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_since_keeps_hits_dups_and_misses_separate() {
        let earlier = ReportSummary {
            hit_rate_pct: 0.0,
            total_crates: 3,
            local_hits: 1,
            prefetch_hits: 0,
            remote_hits: 0,
            dups: 1,
            misses: 1,
        };
        let later = ReportSummary {
            hit_rate_pct: 50.0,
            total_crates: 7,
            local_hits: 3,
            prefetch_hits: 1,
            remote_hits: 0,
            dups: 2,
            misses: 1,
        };

        let delta = later.delta_since(&earlier);

        assert_eq!(delta.total_hits(), 3);
        assert_eq!(delta.dups, 1);
        assert_eq!(delta.misses, 0);
        assert_eq!(delta.total_crates, 4);
        assert_eq!(delta.hit_rate_pct, 75.0);
    }

    #[test]
    fn kache_report_deserialization_accepts_supported_schema_version_and_additive_fields() {
        let json = serde_json::json!({
            "schema_version": 1,
            "summary": {
                "hit_rate_pct": 50.0,
                "total_crates": 2,
                "local_hits": 1,
                "prefetch_hits": 0,
                "remote_hits": 0,
                "dups": 0,
                "misses": 1
            },
            "all_events": [
                {
                    "crate_name": "foo",
                    "result": "local_hit",
                    "compiler_runs": 0,
                    "preprocessor_runs": 0,
                    "probe_runs": 0,
                    "passthrough_reason": ""
                }
            ],
            "future_unrecognized_metric": 42,
            "future_nested_object": { "new_field": "value" }
        });

        let typed: KacheReport = serde_json::from_value(json).expect("should deserialize");
        assert_eq!(typed.schema_version, 1);
        assert_eq!(typed.summary.total_crates, 2);
        assert_eq!(typed.all_events.len(), 1);
        assert_eq!(typed.all_events[0].crate_name, "foo");
    }

    #[test]
    fn kache_report_rejects_missing_or_incompatible_schema_version() {
        // Missing schema_version fails typed deserialization
        let no_version = serde_json::json!({
            "summary": {
                "hit_rate_pct": 0.0,
                "total_crates": 0,
                "local_hits": 0,
                "prefetch_hits": 0,
                "remote_hits": 0,
                "dups": 0,
                "misses": 0
            }
        });
        assert!(serde_json::from_value::<KacheReport>(no_version).is_err());
    }

    #[test]
    fn parse_report_json_accepts_supported_version() {
        let json = serde_json::json!({
            "schema_version": SUPPORTED_REPORT_SCHEMA_VERSION,
            "summary": {
                "hit_rate_pct": 100.0,
                "total_crates": 1,
                "local_hits": 1,
                "prefetch_hits": 0,
                "remote_hits": 0,
                "dups": 0,
                "misses": 0
            }
        });
        let bytes = serde_json::to_vec(&json).unwrap();
        let (typed, raw) = parse_report_json(&bytes).expect("valid supported report");
        assert_eq!(typed.schema_version, SUPPORTED_REPORT_SCHEMA_VERSION);
        assert_eq!(raw["schema_version"], SUPPORTED_REPORT_SCHEMA_VERSION);
    }

    #[test]
    fn parse_report_json_rejects_unsupported_version() {
        let json = serde_json::json!({
            "schema_version": SUPPORTED_REPORT_SCHEMA_VERSION + 1,
            "summary": {
                "hit_rate_pct": 100.0,
                "total_crates": 1,
                "local_hits": 1,
                "prefetch_hits": 0,
                "remote_hits": 0,
                "dups": 0,
                "misses": 0
            }
        });
        let bytes = serde_json::to_vec(&json).unwrap();
        let err = parse_report_json(&bytes).expect_err("should reject unsupported version");
        assert!(
            err.to_string()
                .contains("unsupported report schema_version")
        );
    }
}
