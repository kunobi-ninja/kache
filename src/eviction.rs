//! Eviction *selection*, decoupled from the removal *mechanism*
//! (kunobi-ninja/kache#595).
//!
//! `Store` historically had three eviction entry points — size-pressure,
//! age-based, and content-duplicate — each with its own hardcoded `SELECT`,
//! all funnelling into the same `remove_entry_guarded`. Mechanism was already
//! unified; selection was not.
//!
//! This module makes selection a pure function of observable per-entry
//! features. Two consequences matter:
//!
//! - **Shadow evaluation.** A candidate policy can rank the same candidate set
//!   and log what it *would* evict, while the live policy decides what actually
//!   goes. That comparison is what #594 needs, and it is impractical while the
//!   ranking is an `ORDER BY` clause inside the removal loop.
//! - **Signals beyond SQL columns.** Selection written as SQL can only see
//!   columns of `entries`; `compile_time_ms` lives in each entry's `meta.json`
//!   and is therefore invisible to eviction today (#594). With features
//!   assembled in Rust, adding a signal is a struct field.
//!
//! Policies never remove anything. The pin/grace check, blob refcount
//! decrement, and refuse-on-corrupt-meta guard stay in
//! `Store::remove_entry_guarded` — a policy that could bypass them would
//! reintroduce the mid-restore eviction race (#326, #182).

use std::cmp::Ordering;
use std::collections::HashMap;

/// The observable state of one cache entry, as seen by an eviction policy.
///
/// Deliberately small: it is materialized for every entry in the store on each
/// sweep, so new fields should earn their bytes.
#[derive(Debug, Clone)]
pub(crate) struct EntryFeatures {
    pub key: String,
    pub size: i64,
    pub hit_count: i64,
    /// Hours since `last_accessed`. May be negative under clock skew; policies
    /// are responsible for clamping (the previous SQL clamped at 0.01).
    pub idle_hours: f64,
    /// Content address of the entry's artifact set, when known.
    pub content_hash: Option<String>,
    pub committed: bool,
}

/// Ranks or filters eviction candidates. Pure: no I/O, no store mutation.
pub(crate) trait EvictionPolicy {
    /// Stable identifier, for logging and shadow-mode comparison.
    fn name(&self) -> &'static str;

    /// Candidates to evict, worst-first. The caller stops early when a size
    /// budget is satisfied, so a policy may return more keys than are needed.
    fn select(&self, candidates: &[EntryFeatures]) -> Vec<String>;
}

/// Priority score for size-pressure eviction; **lower is evicted first**.
///
/// `(hit_count + 1) / (idle_hours * size_mb)` — prefers evicting large, stale,
/// rarely-hit entries, degrading to LRU with a size tiebreaker when ages are
/// similar. The clamps reproduce the `MAX(..., 0.01)` / `MAX(..., 0.001)` of
/// the SQL this replaced, which exist to keep a just-accessed or empty entry
/// from dividing by zero.
///
/// Note this score has no notion of what an entry costs to *rebuild*; see #594.
pub(crate) fn size_pressure_score(e: &EntryFeatures) -> f64 {
    let idle = e.idle_hours.max(0.01);
    let size_mb = (e.size as f64 / 1_048_576.0).max(0.001);
    (e.hit_count as f64 + 1.0) / (idle * size_mb)
}

/// Size-pressure eviction: rank every entry by [`size_pressure_score`].
pub(crate) struct SizePressurePolicy;

impl EvictionPolicy for SizePressurePolicy {
    fn name(&self) -> &'static str {
        "size-pressure"
    }

    fn select(&self, candidates: &[EntryFeatures]) -> Vec<String> {
        let mut ranked: Vec<&EntryFeatures> = candidates.iter().collect();
        // Total order: NaN cannot arise from the clamped score, but sort_by
        // demands a total comparator, so fall back to Equal rather than panic.
        ranked.sort_by(|a, b| {
            size_pressure_score(a)
                .partial_cmp(&size_pressure_score(b))
                .unwrap_or(Ordering::Equal)
        });
        ranked.into_iter().map(|e| e.key.clone()).collect()
    }
}

/// Age-based eviction: every entry untouched for more than `hours`.
///
/// Replaces `WHERE last_accessed < datetime('now', '-N hours')`. One deliberate
/// difference: `datetime()` compares at whole-second granularity, whereas
/// `idle_hours` comes from `julianday()` and is sub-second. An entry sitting
/// within the same second as the cutoff is therefore now selected where the
/// old query kept it. Immaterial for a retention sweep measured in hours, and
/// the finer comparison is the more truthful one, but it is a difference and
/// not a pure refactor.
pub(crate) struct OlderThanPolicy {
    pub hours: u64,
}

impl EvictionPolicy for OlderThanPolicy {
    fn name(&self) -> &'static str {
        "older-than"
    }

    fn select(&self, candidates: &[EntryFeatures]) -> Vec<String> {
        let cutoff = self.hours as f64;
        candidates
            .iter()
            .filter(|e| e.idle_hours > cutoff)
            .map(|e| e.key.clone())
            .collect()
    }
}

/// Content-duplicate eviction: when several committed entries share one
/// `content_hash`, keep the most recently accessed and evict the rest.
///
/// Ties at the group maximum are all kept, reproducing the strict `<` of the
/// previous `WHERE e.last_accessed < dups.newest_access`.
pub(crate) struct DuplicatePolicy;

impl EvictionPolicy for DuplicatePolicy {
    fn name(&self) -> &'static str {
        "duplicate"
    }

    fn select(&self, candidates: &[EntryFeatures]) -> Vec<String> {
        // Group committed, content-addressed entries by content hash. Idle time
        // is the inverse of recency, so the group's *minimum* idle is its most
        // recently accessed member.
        let mut newest: HashMap<&str, f64> = HashMap::new();
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for e in candidates {
            if !e.committed {
                continue;
            }
            let Some(hash) = e.content_hash.as_deref() else {
                continue;
            };
            *counts.entry(hash).or_insert(0) += 1;
            newest
                .entry(hash)
                .and_modify(|m| *m = m.min(e.idle_hours))
                .or_insert(e.idle_hours);
        }

        candidates
            .iter()
            .filter(|e| {
                if !e.committed {
                    return false;
                }
                let Some(hash) = e.content_hash.as_deref() else {
                    return false;
                };
                // Only groups with a genuine duplicate, and only members
                // strictly older than the group's newest access.
                counts.get(hash).copied().unwrap_or(0) > 1
                    && newest
                        .get(hash)
                        .is_some_and(|newest_idle| e.idle_hours > *newest_idle)
            })
            .map(|e| e.key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feat(key: &str, size: i64, hits: i64, idle: f64) -> EntryFeatures {
        EntryFeatures {
            key: key.into(),
            size,
            hit_count: hits,
            idle_hours: idle,
            content_hash: None,
            committed: true,
        }
    }

    /// The score must rank a large, stale, unused entry below a small, fresh,
    /// frequently-hit one — the ordering the size-pressure sweep depends on.
    #[test]
    fn size_pressure_ranks_large_stale_unused_first() {
        let big_stale = feat("big", 600 * 1024 * 1024, 0, 15.0);
        let small_hot = feat("small", 14 * 1024, 9, 0.1);
        assert!(size_pressure_score(&big_stale) < size_pressure_score(&small_hot));

        let order = SizePressurePolicy.select(&[small_hot, big_stale]);
        assert_eq!(order, vec!["big", "small"]);
    }

    /// A just-accessed entry (idle ~0) and a zero-size entry must not produce a
    /// division blow-up — the reason the SQL carried MAX() clamps.
    #[test]
    fn size_pressure_score_is_finite_at_the_clamps() {
        assert!(size_pressure_score(&feat("fresh", 1024, 0, 0.0)).is_finite());
        assert!(size_pressure_score(&feat("empty", 0, 0, 5.0)).is_finite());
        // Clock skew must not invert the ordering into +inf.
        assert!(size_pressure_score(&feat("skewed", 1024, 0, -3.0)).is_finite());
    }

    #[test]
    fn older_than_selects_strictly_older_entries() {
        let c = vec![
            feat("old", 100, 0, 48.5),
            feat("exactly", 100, 0, 24.0),
            feat("fresh", 100, 0, 1.0),
        ];
        let picked = OlderThanPolicy { hours: 24 }.select(&c);
        assert_eq!(
            picked,
            vec!["old"],
            "boundary entry must be kept (strict >)"
        );
    }

    /// Keep the most recently accessed member of each content group, evict the
    /// rest, and never touch a group with only one member.
    #[test]
    fn duplicate_keeps_newest_and_ignores_singletons() {
        let mut a = feat("dup_new", 100, 0, 1.0);
        let mut b = feat("dup_old", 100, 0, 9.0);
        let mut c = feat("dup_older", 100, 0, 20.0);
        let mut lone = feat("lone", 100, 0, 99.0);
        a.content_hash = Some("h1".into());
        b.content_hash = Some("h1".into());
        c.content_hash = Some("h1".into());
        lone.content_hash = Some("h2".into());

        let picked = DuplicatePolicy.select(&[a, b, c, lone]);
        assert_eq!(picked, vec!["dup_old", "dup_older"]);
    }

    /// Uncommitted entries and entries without a content hash are invisible to
    /// duplicate eviction — they may be mid-write.
    #[test]
    fn duplicate_ignores_uncommitted_and_hashless() {
        let mut committed = feat("committed", 100, 0, 1.0);
        let mut uncommitted = feat("uncommitted", 100, 0, 9.0);
        committed.content_hash = Some("h".into());
        uncommitted.content_hash = Some("h".into());
        uncommitted.committed = false;

        assert!(DuplicatePolicy.select(&[committed, uncommitted]).is_empty());
    }

    /// Ties at the group's newest access are all kept: the previous SQL used a
    /// strict `<` against the group max, so two equally-fresh duplicates both
    /// survive rather than one being arbitrarily dropped.
    #[test]
    fn duplicate_keeps_all_entries_tied_at_newest() {
        let mut a = feat("tie_a", 100, 0, 5.0);
        let mut b = feat("tie_b", 100, 0, 5.0);
        a.content_hash = Some("h".into());
        b.content_hash = Some("h".into());
        assert!(DuplicatePolicy.select(&[a, b]).is_empty());
    }
}
