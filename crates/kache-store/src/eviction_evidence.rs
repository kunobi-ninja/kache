//! Fixed-horizon evidence from the bounded, per-key eviction observations.
//! These cohorts describe recorded demand, not a counterfactual policy winner.

use anyhow::{Result, anyhow, ensure};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

/// One observation per key: latest live eviction or first shadow-only sweep.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionEvidenceCohort {
    pub observations: u64,
    pub immature: u64,
    pub invalid: u64,
    pub mature: u64,
    pub unknown_compile_cost: u64,
    pub demand_within_horizon_lower: u64,
    pub demand_within_horizon_upper: u64,
    pub known_cost_logical_bytes: u64,
    /// Gross measured compile cost of known-cost observations, including
    /// those without recorded demand. Cache-serving cost was not recorded.
    pub gross_compile_cost_ms: u64,
    pub demanded_gross_compile_cost_ms_lower: u64,
    pub demanded_gross_compile_cost_ms_upper: u64,
}

impl EvictionEvidenceCohort {
    /// Logical entry bytes include shared blobs. This is not cost per byte
    /// physically reclaimed, and unknown compile costs are excluded.
    pub fn demanded_cost_ms_per_logical_gib(&self) -> Option<(f64, f64)> {
        if self.known_cost_logical_bytes == 0 {
            return None;
        }
        let scale = 1_073_741_824.0 / self.known_cost_logical_bytes as f64;
        Some((
            self.demanded_gross_compile_cost_ms_lower as f64 * scale,
            self.demanded_gross_compile_cost_ms_upper as f64 * scale,
        ))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvictionHorizonEvidence {
    pub as_of_unix_secs: i64,
    pub horizon_secs: u64,
    pub live_shadow_agreed: EvictionEvidenceCohort,
    pub live_shadow_kept: EvictionEvidenceCohort,
    pub shadow_only: EvictionEvidenceCohort,
}

struct Observation {
    start: Option<i64>,
    demand: Option<i64>,
    has_demand: bool,
    size: i64,
    cost: i64,
    stamped_hit: Option<i64>,
    later_eviction: Option<i64>,
    later_hit_count_rose: bool,
}

fn observe(
    cohort: &mut EvictionEvidenceCohort,
    row: Observation,
    shadow: bool,
    as_of: i64,
    horizon: i64,
) {
    cohort.observations += 1;
    let Some(start) = row.start.filter(|start| *start <= as_of) else {
        cohort.invalid += 1;
        return;
    };
    if row.has_demand
        && row
            .demand
            .is_none_or(|demand| demand < start || demand > as_of)
    {
        cohort.invalid += 1;
        return;
    }
    let Some(end) = start.checked_add(horizon).filter(|end| *end <= as_of) else {
        cohort.immature += 1;
        return;
    };
    cohort.mature += 1;
    let demand = row.demand.is_some_and(|demand| demand <= end);
    // A timestamped hit, or an increased hit count captured by a later
    // eviction within the horizon, proves at least one request. Later stamps
    // cannot disprove earlier requests: counters are throttled and overwritten.
    let observed_hit = row
        .stamped_hit
        .is_some_and(|hit| hit >= start && hit <= end)
        || (row.later_hit_count_rose && row.later_eviction.is_some_and(|eviction| eviction <= end));
    let lower = demand || (shadow && observed_hit);
    let upper = lower || shadow;
    cohort.demand_within_horizon_lower += u64::from(lower);
    cohort.demand_within_horizon_upper += u64::from(upper);
    if row.cost <= 0 {
        cohort.unknown_compile_cost += 1;
        return;
    }
    let cost = row.cost as u64;
    cohort.known_cost_logical_bytes = cohort
        .known_cost_logical_bytes
        .saturating_add(row.size.max(0) as u64);
    cohort.gross_compile_cost_ms = cohort.gross_compile_cost_ms.saturating_add(cost);
    if lower {
        cohort.demanded_gross_compile_cost_ms_lower = cohort
            .demanded_gross_compile_cost_ms_lower
            .saturating_add(cost);
    }
    if upper {
        cohort.demanded_gross_compile_cost_ms_upper = cohort
            .demanded_gross_compile_cost_ms_upper
            .saturating_add(cost);
    }
}

/// One SELECT gives all cohorts the same SQLite read snapshot. No migrations,
/// backfills, pruning or access stamps are performed by this reader.
pub(crate) fn read(
    db: &Connection,
    as_of_unix_secs: i64,
    horizon_secs: u64,
) -> Result<EvictionHorizonEvidence> {
    ensure!(
        horizon_secs > 0,
        "eviction observation horizon must be positive"
    );
    let horizon = i64::try_from(horizon_secs)
        .map_err(|_| anyhow!("eviction observation horizon is too large"))?;
    let mut report = EvictionHorizonEvidence {
        as_of_unix_secs,
        horizon_secs,
        ..Default::default()
    };
    let mut query = db.prepare(
        "SELECT CASE shadow_would_evict WHEN 1 THEN 0 ELSE 1 END,
                unixepoch(evicted_at), unixepoch(demanded_at), demanded_at IS NOT NULL,
                size, compile_time_ms, NULL, NULL, 0
           FROM eviction_tombstones
          WHERE shadow_policy = 'value-density' AND shadow_would_evict IN (0, 1)
         UNION ALL
         SELECT 2, unixepoch(sv.swept_at), unixepoch(t.demanded_at),
                t.demanded_at IS NOT NULL, sv.size, sv.compile_time_ms,
                CASE WHEN e.hit_count > sv.hit_count THEN unixepoch(e.last_accessed) END,
                unixepoch(t.evicted_at), COALESCE(t.hit_count > sv.hit_count, 0)
           FROM shadow_victims sv
           LEFT JOIN entries e ON e.cache_key = sv.cache_key
           LEFT JOIN eviction_tombstones t ON t.cache_key = sv.cache_key
                                         AND t.evicted_at >= sv.swept_at
          WHERE sv.shadow_policy = 'value-density'",
    )?;
    let rows = query.query_map([], |row| {
        Ok((
            row.get::<_, u8>(0)?,
            Observation {
                start: row.get(1)?,
                demand: row.get(2)?,
                has_demand: row.get(3)?,
                size: row.get(4)?,
                cost: row.get(5)?,
                stamped_hit: row.get(6)?,
                later_eviction: row.get(7)?,
                later_hit_count_rose: row.get(8)?,
            },
        ))
    })?;
    for row in rows {
        let (kind, observation) = row?;
        let cohort = match kind {
            0 => &mut report.live_shadow_agreed,
            1 => &mut report.live_shadow_kept,
            _ => &mut report.shadow_only,
        };
        observe(cohort, observation, kind == 2, as_of_unix_secs, horizon);
    }
    Ok(report)
}

#[cfg(test)]
mod tests;
