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
