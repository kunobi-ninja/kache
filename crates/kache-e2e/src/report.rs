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

/// Raw `kache report --format json` document, deserialized with the
/// summary surface lifted out for assertions and the full body kept as
/// `serde_json::Value` for verbatim forwarding.
#[derive(Debug, Clone, Deserialize)]
pub struct KacheReport {
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
    pub misses: u64,
}

impl ReportSummary {
    /// Aggregate hits across all sources (local + prefetch + remote).
    pub fn total_hits(&self) -> u64 {
        self.local_hits + self.prefetch_hits + self.remote_hits
    }

    /// Compute `self - earlier` for per-phase delta semantics.
    ///
    /// `kache report --since 1h` is cumulative over the time window —
    /// a single phase's hits/misses count is meaningless because
    /// earlier phases inflate the totals. Snapshotting before/after
    /// each phase and subtracting gives the per-phase signal that
    /// per-phase assertions want.
    ///
    /// `hit_rate_pct` is recomputed from the delta hits/misses (NOT
    /// subtracted, since rates don't subtract meaningfully). Returns
    /// `0.0` when the delta `total_crates` is zero — a phase that
    /// did nothing is `0%`, not `NaN`.
    pub fn delta_since(&self, earlier: &ReportSummary) -> ReportSummary {
        let local_hits = self.local_hits.saturating_sub(earlier.local_hits);
        let prefetch_hits = self.prefetch_hits.saturating_sub(earlier.prefetch_hits);
        let remote_hits = self.remote_hits.saturating_sub(earlier.remote_hits);
        let misses = self.misses.saturating_sub(earlier.misses);
        let total_crates = self.total_crates.saturating_sub(earlier.total_crates);
        let hits = local_hits + prefetch_hits + remote_hits;
        let hit_rate_pct = if hits + misses == 0 {
            0.0
        } else {
            (hits as f64 / (hits + misses) as f64) * 100.0
        };
        ReportSummary {
            hit_rate_pct,
            total_crates,
            local_hits,
            prefetch_hits,
            remote_hits,
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
        misses: 0,
    }
}

/// Invoke `<kache> report --format json --since 1h` against the given
/// `cache_dir` and return both the typed summary and the raw value.
///
/// The raw value is what gets written to the result file so users can
/// inspect any field, not only the ones the harness asserts against.
pub fn fetch(kache_path: &Path, cache_dir: &Path) -> Result<(KacheReport, serde_json::Value)> {
    let output = Command::new(kache_path)
        .args(["report", "--format", "json", "--since", "1h"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .output()
        .with_context(|| format!("running `{} report`", kache_path.display()))?;

    if !output.status.success() {
        anyhow::bail!(
            "kache report exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let raw: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("parsing kache report JSON")?;
    let typed: KacheReport =
        serde_json::from_value(raw.clone()).context("extracting summary from kache report")?;
    Ok((typed, raw))
}
