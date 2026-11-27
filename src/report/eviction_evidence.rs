//! Store-wide observations have their own horizon, independent of build events.
use kache_store::EvictionHorizonEvidence;
use serde::{Deserialize, Serialize};

pub(super) const HORIZON_SECS: u64 = 604_800;
const LIMITATIONS: &[&str] = &[
    "All-store observations tagged with value-density shadow decisions, independent of the build-event window and root filter.",
    "Latest live eviction and first shadow-only sweep per key; repeated evictions overwrite history and cohorts can overlap.",
    "Immature and invalid observations are excluded from demand and cost totals. Retained rows are not a complete sweep history.",
    "Live demand counts first recorded post-eviction requests. Read-only clients and overwritten records can hide demand; absence of a stamp does not prove obsolescence.",
    "Shadow demand bounds allow unrecorded requests because hit stamps are throttled, overwritten and reset on reinsertion.",
    "Costs are gross measured compile time, excluding unknown costs and cache-serving cost. Logical bytes include shared blobs and do not estimate physically reclaimed bytes.",
    "These observations do not select a policy winner or change live eviction.",
];

#[derive(Debug, Serialize, Deserialize)]
pub struct EvictionEvidenceReport {
    pub horizon_secs: u64,
    pub as_of_unix_secs: i64,
    pub limitations: Vec<String>,
    pub stores: Vec<StoreEvidence>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoreEvidence {
    /// Index into storage.stores, avoiding a second copy of private paths.
    pub store_index: usize,
    pub evidence: Option<EvictionHorizonEvidence>,
    pub error: Option<String>,
}

pub(super) fn collect(
    observations: Vec<Result<EvictionHorizonEvidence, String>>,
    as_of: i64,
) -> EvictionEvidenceReport {
    let stores = observations
        .into_iter()
        .enumerate()
        .map(|(store_index, result)| match result {
            Ok(evidence) => StoreEvidence {
                store_index,
                evidence: Some(evidence),
                error: None,
            },
            Err(error) => StoreEvidence {
                store_index,
                evidence: None,
                error: Some(error),
            },
        })
        .collect();
    EvictionEvidenceReport {
        horizon_secs: HORIZON_SECS,
        as_of_unix_secs: as_of,
        limitations: LIMITATIONS.iter().map(|s| (*s).into()).collect(),
        stores,
    }
}

/// The `stats --full` block, or nothing when no store has observed an
/// eviction yet. The JSON report keeps the full [`LIMITATIONS`] list; this
/// text is the part a person can read on one screen.
pub(super) fn lines(report: &EvictionEvidenceReport) -> Vec<String> {
    if !has_observations(report) {
        return Vec::new();
    }
    let mut lines = vec![
        String::new(),
        format!(
            "Eviction observations · {} horizon (all stores)",
            horizon_label(report.horizon_secs)
        ),
    ];
    for store in &report.stores {
        let Some(evidence) = &store.evidence else {
            lines.push(format!(
                "  Store {}: {}",
                store.store_index,
                store.error.as_deref().unwrap_or("unavailable")
            ));
            continue;
        };
        for (name, cohort) in [
            ("both rankings would delete", &evidence.live_shadow_agreed),
            (
                "live deleted, other ranking would keep",
                &evidence.live_shadow_kept,
            ),
            (
                "other ranking would delete, live kept",
                &evidence.shadow_only,
            ),
        ] {
            lines.extend(cohort_lines(store.store_index, name, cohort));
        }
    }
    lines.push(format!(
        "  Judged rows have finished the {} horizon.",
        horizon_label(report.horizon_secs)
    ));
    lines.push("  Recent rows are not in the compiled-again count.".to_string());
    lines.push(
        "  This covers every store, independent of the build-event window and root filter."
            .to_string(),
    );
    lines.push("  Logical bytes include shared blobs and are not disk space freed.".to_string());
    lines.push("  These observations do not change what gets deleted.".to_string());
    lines
}

fn cohort_lines(
    store: usize,
    name: &str,
    cohort: &kache_store::EvictionEvidenceCohort,
) -> Vec<String> {
    let mut counts = format!(
        "  Store {store}, {name}: {} judged, {} recent",
        cohort.mature, cohort.immature
    );
    if cohort.invalid > 0 {
        counts.push_str(&format!(", {} unusable", cohort.invalid));
    }
    let mut lines = vec![counts];
    let demanded = cohort.demand_within_horizon_lower > 0
        || cohort.demand_within_horizon_upper > 0
        || cohort.demanded_gross_compile_cost_ms_lower > 0
        || cohort.demanded_gross_compile_cost_ms_upper > 0
        || cohort.unknown_compile_cost > 0;
    if !demanded {
        return lines;
    }
    let demand = span(
        cohort.demand_within_horizon_lower,
        cohort.demand_within_horizon_upper,
    );
    let cost = if cohort.demanded_gross_compile_cost_ms_lower
        == cohort.demanded_gross_compile_cost_ms_upper
    {
        crate::term::duration_ms(cohort.demanded_gross_compile_cost_ms_lower)
    } else {
        format!(
            "{}–{}",
            crate::term::duration_ms(cohort.demanded_gross_compile_cost_ms_lower),
            crate::term::duration_ms(cohort.demanded_gross_compile_cost_ms_upper)
        )
    };
    let per = cohort
        .demanded_cost_ms_per_logical_gib()
        .map(|(lower, upper)| {
            if lower == upper {
                format!("{lower:.1} ms/logical GiB")
            } else {
                format!("{lower:.1}–{upper:.1} ms/logical GiB")
            }
        })
        .unwrap_or_else(|| "unavailable".to_string());
    let mut again = format!("    Compiled again: {demand}, {cost} ({per})");
    if cohort.unknown_compile_cost > 0 {
        again.push_str(&format!(", unknown cost {}", cohort.unknown_compile_cost));
    }
    lines.push(again);
    lines
}

fn span(lower: u64, upper: u64) -> String {
    if lower == upper {
        lower.to_string()
    } else {
        format!("{lower}–{upper}")
    }
}

/// Whether any store recorded an observation in any cohort.
fn has_observations(report: &EvictionEvidenceReport) -> bool {
    report.stores.iter().any(|store| {
        store.evidence.as_ref().is_some_and(|evidence| {
            [
                &evidence.live_shadow_agreed,
                &evidence.live_shadow_kept,
                &evidence.shadow_only,
            ]
            .iter()
            .any(|cohort| cohort.observations > 0)
        })
    })
}

/// `7d`, `12h`, `90s`: the horizon in its largest whole unit.
fn horizon_label(secs: u64) -> String {
    const DAY: u64 = 86_400;
    const HOUR: u64 = 3_600;
    if secs > 0 && secs.is_multiple_of(DAY) {
        format!("{}d", secs / DAY)
    } else if secs > 0 && secs.is_multiple_of(HOUR) {
        format!("{}h", secs / HOUR)
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests;
