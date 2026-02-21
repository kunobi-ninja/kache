use anyhow::Result;
use kache_protocol::{CrateStats, StatsResponse};

use crate::db::Database;

pub fn get_stats(db: &Database) -> Result<StatsResponse> {
    db.with_conn(|conn| {
        let total_builds: i64 =
            conn.query_row("SELECT COUNT(*) FROM build_sessions", [], |r| r.get(0))?;

        let total_artifacts: i64 =
            conn.query_row("SELECT COUNT(*) FROM cache_key_inputs", [], |r| r.get(0))?;

        let total_events: i64 =
            conn.query_row("SELECT COUNT(*) FROM build_events", [], |r| r.get(0))?;

        let total_hits: i64 = conn.query_row(
            "SELECT COUNT(*) FROM build_events WHERE result = 'hit'",
            [],
            |r| r.get(0),
        )?;

        let hit_rate = if total_events > 0 {
            total_hits as f64 / total_events as f64
        } else {
            0.0
        };

        let mut stmt = conn.prepare(
            "SELECT crate_name,
                    SUM(CASE WHEN result = 'hit' THEN 1 ELSE 0 END) as hits,
                    SUM(CASE WHEN result = 'miss' THEN 1 ELSE 0 END) as misses,
                    AVG(elapsed_ms) as avg_ms
             FROM build_events
             GROUP BY crate_name
             ORDER BY (hits + misses) DESC
             LIMIT 20",
        )?;

        let top_crates = stmt
            .query_map([], |row| {
                Ok(CrateStats {
                    name: row.get(0)?,
                    hits: row.get::<_, i64>(1)? as u64,
                    misses: row.get::<_, i64>(2)? as u64,
                    avg_compile_ms: row.get::<_, f64>(3).unwrap_or(0.0) as u64,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(StatsResponse {
            total_builds: total_builds as u64,
            hit_rate,
            total_artifacts: total_artifacts as u64,
            top_crates,
        })
    })
}
