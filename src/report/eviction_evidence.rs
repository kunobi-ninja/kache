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

pub(super) fn lines(report: &EvictionEvidenceReport) -> Vec<String> {
    let mut lines = vec![
        String::new(),
        format!(
            "Eviction observations · {}s horizon (all stores)",
            report.horizon_secs
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
            ("live/shadow agreed", &evidence.live_shadow_agreed),
            ("live/shadow kept", &evidence.live_shadow_kept),
            ("shadow only", &evidence.shadow_only),
        ] {
            let cost = cohort
                .demanded_cost_ms_per_logical_gib()
                .map(|(lower, upper)| format!("{lower:.1}–{upper:.1}ms/logical GiB"))
                .unwrap_or_else(|| "unavailable".into());
            lines.push(format!("  Store {} {name}: {} mature, {} immature, {} invalid; recorded demand {}–{}; unknown cost {}; gross demanded compile cost {}–{}ms ({cost})",store.store_index,cohort.mature,cohort.immature,cohort.invalid,cohort.demand_within_horizon_lower,cohort.demand_within_horizon_upper,cohort.unknown_compile_cost,cohort.demanded_gross_compile_cost_ms_lower,cohort.demanded_gross_compile_cost_ms_upper));
        }
    }
    lines.extend(report.limitations.iter().map(|limit| format!("  {limit}")));
    lines
}

#[cfg(test)]
mod tests;
