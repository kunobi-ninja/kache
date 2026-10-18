use super::*;
use rusqlite::{Transaction, TransactionBehavior};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const WRITE_BATCH: usize = 128;

pub(super) fn ensure_schema(db: &Connection) -> rusqlite::Result<()> {
    // Another wrapper may be upgrading the same index. Serialize the column
    // probe and DDL, and leave no partially upgraded backfill schema on error.
    let tx = Transaction::new_unchecked(db, TransactionBehavior::Immediate)?;
    let columns = {
        let mut stmt = tx.prepare("SELECT name FROM pragma_table_info('entries')")?;
        stmt.query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?
    };
    for (name, statement) in [
        (
            "compile_time_backfilled",
            "ALTER TABLE entries ADD COLUMN compile_time_backfilled INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "compile_time_retry_at",
            "ALTER TABLE entries ADD COLUMN compile_time_retry_at INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "compile_time_retry_count",
            "ALTER TABLE entries ADD COLUMN compile_time_retry_count INTEGER NOT NULL DEFAULT 0",
        ),
    ] {
        if !columns.contains(name) {
            tx.execute_batch(statement)?;
        }
    }
    tx.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_entries_compile_time_pending
         ON entries(compile_time_retry_at, cache_key)
         WHERE committed = 1 AND compile_time_ms = 0 AND compile_time_backfilled = 0",
    )?;
    tx.commit()
}

fn budget_expired(elapsed: Duration) -> bool {
    elapsed >= Duration::from_secs(1)
}

fn retry_after(now: i64, failures: i64) -> (i64, i64) {
    let exponent = failures.clamp(0, 6) as u32;
    let delay = (60i64 << exponent).min(3600);
    (now.saturating_add(delay), i64::from(exponent + 1))
}

#[derive(Debug, Clone)]
pub(super) struct Candidate {
    rowid: i64,
    key: String,
    content_hash: Option<String>,
    retry_at: i64,
    failures: i64,
}

pub(super) struct Outcome {
    candidate: Candidate,
    cost: Option<i64>,
}

pub(super) const SELECT_DUE: &str =
    "SELECT rowid, cache_key, content_hash, compile_time_retry_at, compile_time_retry_count
     FROM entries
     WHERE committed = 1 AND compile_time_ms = 0 AND compile_time_backfilled = 0
       AND compile_time_retry_at <= ?1
     ORDER BY compile_time_retry_at, cache_key LIMIT ?2";

impl<P: ArtifactPolicy> ArtifactStore<P> {
    pub(super) fn backfill_compile_times_bounded(&self) -> Result<usize> {
        let started = Instant::now();
        let now = i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())?;
        self.backfill_compile_times_batch(COMPILE_TIME_BACKFILL_BATCH, now, || {
            budget_expired(started.elapsed())
        })
    }

    #[cfg(test)]
    pub(super) fn backfill_compile_times_limited(&self, limit: i64) -> Result<usize> {
        self.backfill_compile_times_batch(limit, 0, || false)
    }

    pub(super) fn compile_time_candidates(&self, limit: i64, now: i64) -> Result<Vec<Candidate>> {
        let mut stmt = self.db.prepare(SELECT_DUE)?;
        Ok(stmt
            .query_map(params![now, limit], |row| {
                Ok(Candidate {
                    rowid: row.get(0)?,
                    key: row.get(1)?,
                    content_hash: row.get(2)?,
                    retry_at: row.get(3)?,
                    failures: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub(super) fn compile_time_outcome(&self, candidate: Candidate) -> Outcome {
        let read = || -> Result<i64> {
            let bytes = fs::read(self.entry_dir(&candidate.key).join("meta.json"))?;
            let meta: EntryMeta = serde_json::from_slice(&bytes)?;
            anyhow::ensure!(meta.cache_key == candidate.key, "metadata key mismatch");
            Ok(i64::try_from(meta.compile_time_ms)?)
        };
        let cost = match read() {
            Ok(cost) => Some(cost),
            Err(error) => {
                tracing::debug!(key = %candidate.key, %error, "compile-cost backfill deferred");
                None
            }
        };
        Outcome { candidate, cost }
    }

    pub(super) fn apply_compile_time_outcomes(
        &self,
        outcomes: &[Outcome],
        now: i64,
    ) -> Result<usize> {
        let tx = self.db.unchecked_transaction()?;
        let mut recovered = 0;
        {
            let mut success = tx.prepare(
                "UPDATE entries SET compile_time_ms = ?1, compile_time_backfilled = 1,
                     compile_time_retry_at = 0, compile_time_retry_count = 0
                 WHERE rowid = ?2 AND cache_key = ?3 AND committed = 1
                   AND compile_time_ms = 0 AND compile_time_backfilled = 0
                   AND compile_time_retry_at = ?4 AND compile_time_retry_count = ?5
                   AND content_hash IS ?6",
            )?;
            let mut deferred = tx.prepare(
                "UPDATE entries SET compile_time_retry_at = ?1, compile_time_retry_count = ?2
                 WHERE rowid = ?3 AND cache_key = ?4 AND committed = 1
                   AND compile_time_ms = 0 AND compile_time_backfilled = 0
                   AND compile_time_retry_at = ?5 AND compile_time_retry_count = ?6
                   AND content_hash IS ?7",
            )?;
            for outcome in outcomes {
                let candidate = &outcome.candidate;
                if let Some(cost) = outcome.cost {
                    let changed = success.execute(params![
                        cost,
                        candidate.rowid,
                        candidate.key,
                        candidate.retry_at,
                        candidate.failures,
                        candidate.content_hash,
                    ])?;
                    // Keep the public count of recovered positive costs. A
                    // processed zero still means unknown to eviction ranking.
                    if cost > 0 {
                        recovered += changed;
                    }
                } else {
                    let (retry_at, failures) = retry_after(now, candidate.failures);
                    deferred.execute(params![
                        retry_at,
                        failures,
                        candidate.rowid,
                        candidate.key,
                        candidate.retry_at,
                        candidate.failures,
                        candidate.content_hash,
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok(recovered)
    }

    pub(super) fn backfill_compile_times_batch(
        &self,
        limit: i64,
        now: i64,
        mut exhausted: impl FnMut() -> bool,
    ) -> Result<usize> {
        self.refuse_write("backfill compile times")?;
        if limit <= 0 {
            return Ok(0);
        }
        let mut attempted = 0_i64;
        let mut recovered = 0;
        // Each query is capped to the remaining count, so this cannot overshoot.
        while attempted != limit {
            let page_size = (limit - attempted).min(WRITE_BATCH as i64);
            let candidates = self.compile_time_candidates(page_size, now)?;
            if candidates.is_empty() {
                break;
            }
            let mut outcomes = Vec::with_capacity(candidates.len());
            let mut stopped = false;
            for candidate in candidates {
                // Always attempt one row: a slow initial SELECT must not turn
                // the soft time budget into permanent starvation.
                if attempted > 0 && exhausted() {
                    stopped = true;
                    break;
                }
                outcomes.push(self.compile_time_outcome(candidate));
                attempted += 1;
            }
            if !outcomes.is_empty() {
                recovered += self.apply_compile_time_outcomes(&outcomes, now)?;
            }
            if stopped {
                break;
            }
        }
        tracing::debug!(attempted, recovered, "compile-cost backfill pass");
        Ok(recovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soft_budget_has_an_independent_boundary() {
        assert!(!budget_expired(Duration::from_millis(999)));
        assert!(budget_expired(Duration::from_millis(1000)));
        assert!(budget_expired(Duration::from_millis(1001)));
    }

    #[test]
    fn retry_delay_grows_to_one_hour_and_saturates() {
        for (failures, next, count) in [
            (-1, 1060, 1),
            (0, 1060, 1),
            (1, 1120, 2),
            (2, 1240, 3),
            (5, 2920, 6),
            (6, 4600, 7),
            (7, 4600, 7),
            (i64::MAX, 4600, 7),
        ] {
            assert_eq!(retry_after(1000, failures), (next, count));
        }
        assert_eq!(retry_after(i64::MAX - 1, 0), (i64::MAX, 1));
    }
}
