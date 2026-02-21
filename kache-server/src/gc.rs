use anyhow::{Result, bail};
use tracing::info;

use crate::db::Database;

/// Run garbage collection: delete metadata older than `max_age`.
/// Age format: "30d", "7d", "90d" etc.
pub fn run(db: &Database, max_age: &str) -> Result<()> {
    let days = parse_days(max_age)?;

    db.with_conn(|conn| {
        let cutoff = format!("-{days} days");

        let events_deleted: usize = conn.execute(
            "DELETE FROM build_events WHERE created_at < datetime('now', ?1)",
            [&cutoff],
        )?;

        let sessions_deleted: usize = conn.execute(
            "DELETE FROM build_sessions WHERE started_at < datetime('now', ?1)
             AND id NOT IN (SELECT DISTINCT session_id FROM build_events)",
            [&cutoff],
        )?;

        let edges_deleted: usize = conn.execute(
            "DELETE FROM dep_edges WHERE last_seen < datetime('now', ?1)",
            [&cutoff],
        )?;

        let keys_deleted: usize = conn.execute(
            "DELETE FROM cache_key_inputs WHERE created_at < datetime('now', ?1)",
            [&cutoff],
        )?;

        info!(
            events_deleted,
            sessions_deleted, edges_deleted, keys_deleted, days, "garbage collection complete"
        );

        // Reclaim disk space
        conn.execute_batch("VACUUM")?;

        Ok(())
    })
}

fn parse_days(s: &str) -> Result<u32> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('d') {
        Ok(n.parse()?)
    } else {
        bail!("invalid duration format: {s:?} (expected e.g. \"30d\")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_days() {
        assert_eq!(parse_days("30d").unwrap(), 30);
        assert_eq!(parse_days("7d").unwrap(), 7);
        assert!(parse_days("invalid").is_err());
    }
}
