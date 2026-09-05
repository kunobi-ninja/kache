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

/// Percentage of `max_size` a size-pressure sweep evicts *down to*.
pub const EVICT_TARGET_PERCENT: u64 = 90;

/// The low edge of GC's hysteresis band: the size a sweep evicts down to.
///
/// Compute through `u128` so arbitrary byte-sized limits get an exact floor
/// without overflowing near `u64::MAX`.
pub fn eviction_target(max_size: u64) -> u64 {
    ((max_size as u128 * EVICT_TARGET_PERCENT as u128) / 100) as u64
}

/// The high edge of GC's hysteresis band: the size a sweep *fires* at.
///
/// Trigger and target were both [`eviction_target`], so "evict to 90% — avoids
/// boundary thrashing" described a band that did not exist: a store parked on
/// the 90% line re-triggered on essentially every pass, each one evicting just
/// enough to dip under before the next `put` pushed it back over. Firing at the
/// configured cap and stopping at 90% gives the sweep the 10% of headroom the
/// comment always claimed.
pub fn over_eviction_trigger(physical_size: u64, max_size: u64) -> bool {
    physical_size > max_size
}

/// The observable state of one cache entry, as seen by an eviction policy.
///
/// Deliberately small: it is materialized for every entry in the store on each
/// sweep, so new fields should earn their bytes.
#[derive(Debug, Clone)]
pub struct EntryFeatures {
    pub key: String,
    pub size: i64,
    pub hit_count: i64,
    /// Hours since `last_accessed`. May be negative under clock skew; policies
    /// are responsible for clamping (the previous SQL clamped at 0.01).
    pub idle_hours: f64,
    /// Content address of the entry's artifact set, when known.
    pub content_hash: Option<String>,
    pub committed: bool,
    /// What a miss on this entry would cost to rebuild, in milliseconds
    /// (kunobi-ninja/kache#594). `0` means unknown — either a pre-#594 entry
    /// not yet backfilled, or a compile too fast to register. No policy ships
    /// today that reads this; it is here so a value-aware policy can be
    /// written and shadow-evaluated against the current one.
    pub compile_time_ms: i64,
    /// Physical bytes evicting this entry would actually free right now:
    /// the sizes of blobs it holds every remaining reference to
    /// (kunobi-ninja/kache#608). Zero for an entry whose blobs are all
    /// shared — evicting it destroys rebuild value and reclaims nothing.
    /// `None` means unknown (entry not yet backfilled into `entry_blobs`);
    /// policies fall back to the logical `size`.
    pub reclaimable_bytes: Option<i64>,
}

/// Ranks or filters eviction candidates. Pure: no I/O, no store mutation.
pub trait EvictionPolicy {
    /// Stable identifier, for logging and shadow-mode comparison.
    fn name(&self) -> &'static str;

    /// Candidates to evict, worst-first. The caller stops early when a size
    /// budget is satisfied, so a policy may return more keys than are needed.
    fn select(&self, candidates: &[EntryFeatures]) -> Vec<String>;
}

/// Priority score for size-pressure eviction; **lower is evicted first**.
///
/// `(hit_count + 1) / (idle_hours * reclaimable_mb)` — prefers evicting stale,
/// rarely-hit entries whose removal actually frees bytes, degrading to LRU
/// with a size tiebreaker when ages are similar. The size term is the entry's
/// *marginal reclaimable* bytes (blobs it holds the last references to), not
/// its logical size: a 500 MB entry whose blobs are all shared frees nothing,
/// while a 200 MB entry with unique blobs frees 200 MB, and a size-pressure
/// sweep exists to free bytes (kunobi-ninja/kache#608). Entries not yet
/// backfilled into `entry_blobs` (`None`) rank on logical size as before.
/// The clamps reproduce the `MAX(..., 0.01)` / `MAX(..., 0.001)` of the SQL
/// this replaced, which exist to keep a just-accessed or free-nothing entry
/// from dividing by zero — a fully-shared entry thus ranks as if it were
/// tiny, i.e. last among equally stale candidates.
///
/// Note this score has no notion of what an entry costs to *rebuild*; see #594.
pub fn size_pressure_score(e: &EntryFeatures) -> f64 {
    let idle = e.idle_hours.max(0.01);
    let bytes = e.reclaimable_bytes.unwrap_or(e.size);
    let size_mb = (bytes as f64 / 1_048_576.0).max(0.001);
    (e.hit_count as f64 + 1.0) / (idle * size_mb)
}

/// Size-pressure eviction: rank every entry by [`size_pressure_score`].
pub struct SizePressurePolicy;

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

/// Retention value per reclaimable byte; **lower is evicted first**
/// (kunobi-ninja/kache#594, shadow candidate — never the live policy yet).
///
/// `compile_time_ms / reclaimable_mb`: what a re-miss on this entry costs to
/// rebuild, per byte its eviction would actually free. This is deliberately
/// the PURE density score from the #594 analysis, with no recency or
/// hit-count terms bolted on: reuse probability belongs as a multiplier
/// estimated from the tombstone demand stream, not as an arbitrary divisor —
/// and the demand stream this shadow feeds is exactly what will estimate it.
///
/// Two knowingly rough edges, acceptable because this only ever shadows:
/// entries whose `compile_time_ms` is still 0 (not yet backfilled, or a
/// sub-millisecond compile) rank as worthless and become the shadow's first
/// victims, and the serve-from-cache cost is treated as negligible rather
/// than subtracted from the rebuild cost.
pub fn value_density_score(e: &EntryFeatures) -> f64 {
    let bytes = e.reclaimable_bytes.unwrap_or(e.size);
    let size_mb = (bytes as f64 / 1_048_576.0).max(0.001);
    e.compile_time_ms as f64 / size_mb
}

/// Pure rebuild-cost-density ranking — the #594 shadow candidate.
pub struct ValueDensityPolicy;

impl EvictionPolicy for ValueDensityPolicy {
    fn name(&self) -> &'static str {
        "value-density"
    }

    fn select(&self, candidates: &[EntryFeatures]) -> Vec<String> {
        let mut ranked: Vec<&EntryFeatures> = candidates.iter().collect();
        ranked.sort_by(|a, b| {
            value_density_score(a)
                .partial_cmp(&value_density_score(b))
                .unwrap_or(Ordering::Equal)
        });
        ranked.into_iter().map(|e| e.key.clone()).collect()
    }
}

/// The set of keys a policy's ranking would evict to free `bytes_needed` —
/// the shadow half of a #594 comparison. Walks `order` accumulating each
/// entry's *expected* reclaim (its marginal reclaimable bytes, logical size
/// when unknown) until the budget is met.
///
/// An approximation over the pre-sweep snapshot, not a simulation: the live
/// walk budgets on bytes each removal ACTUALLY freed, where a shared blob's
/// bytes materialize only when its last surviving twin goes, while this walk
/// sees every twin's marginal bytes as the snapshot reported them (zero for
/// all of them) and never updates as it goes. Unknown reclaim falls back to
/// logical size, which can overestimate progress and shorten the prefix, and
/// entries the live sweep skips (recency-pinned, lost removal races) still
/// consume this budget. Good enough for a per-entry agreement verdict;
/// a faithful counterfactual sweep would need dynamically updated blob
/// refcounts and the live eligibility rules (#594 follow-up territory).
pub fn would_evict_for_budget(
    candidates: &[EntryFeatures],
    order: &[String],
    bytes_needed: u64,
) -> std::collections::HashSet<String> {
    let by_key: HashMap<&str, &EntryFeatures> =
        candidates.iter().map(|e| (e.key.as_str(), e)).collect();
    let mut victims = std::collections::HashSet::new();
    let mut freed: u64 = 0;
    for key in order {
        if freed >= bytes_needed {
            break;
        }
        let Some(e) = by_key.get(key.as_str()) else {
            continue;
        };
        freed += e.reclaimable_bytes.unwrap_or(e.size).max(0) as u64;
        victims.insert(key.clone());
    }
    victims
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
pub struct OlderThanPolicy {
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
/// `content_hash`, keep the most recently accessed and evict the rest —
/// but only where removal actually frees bytes.
///
/// Ties at the group maximum are all kept, reproducing the strict `<` of the
/// previous `WHERE e.last_accessed < dups.newest_access`.
///
/// Blobs are refcounted and shared, so deleting a key whose blobs are all held
/// by another entry destroys hit history without reclaiming space. Candidates
/// are eligible only when backfill proves a positive marginal reclaim. Unknown
/// legacy rows fail closed until a later bounded backfill maps them.
pub struct DuplicatePolicy;

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

        let mut eligible: Vec<&EntryFeatures> = candidates
            .iter()
            .filter(|e| {
                if !e.committed {
                    return false;
                }
                let Some(hash) = e.content_hash.as_deref() else {
                    return false;
                };
                // Removing this key must actually free bytes; a fully shared
                // entry costs its hit history and reclaims nothing.
                if !matches!(e.reclaimable_bytes, Some(bytes) if bytes > 0) {
                    return false;
                }
                // Only groups with a genuine duplicate, and only members
                // strictly older than the group's newest access.
                counts.get(hash).copied().unwrap_or(0) > 1
                    && newest
                        .get(hash)
                        .is_some_and(|newest_idle| e.idle_hours > *newest_idle)
            })
            .collect();

        // The removal walk may stop once its byte target is met, so return
        // victims worst-first: stalest duplicate first, then key order for a
        // stable choice among equally old entries.
        eligible.sort_by(|a, b| {
            b.idle_hours
                .total_cmp(&a.idle_hours)
                .then_with(|| a.key.cmp(&b.key))
        });
        eligible.into_iter().map(|e| e.key.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eviction_target_is_exact_for_arbitrary_limits_without_overflow() {
        assert_eq!(eviction_target(1_010), 909);
        assert_eq!(
            eviction_target(u64::MAX),
            ((u64::MAX as u128 * EVICT_TARGET_PERCENT as u128) / 100) as u64
        );
    }

    #[test]
    fn eviction_trigger_is_strictly_above_the_configured_cap() {
        assert!(!over_eviction_trigger(999, 1_000));
        assert!(!over_eviction_trigger(1_000, 1_000));
        assert!(over_eviction_trigger(1_001, 1_000));
    }

    fn feat(key: &str, size: i64, hits: i64, idle: f64) -> EntryFeatures {
        EntryFeatures {
            key: key.into(),
            size,
            hit_count: hits,
            idle_hours: idle,
            content_hash: None,
            committed: true,
            compile_time_ms: 0,
            reclaimable_bytes: None,
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
        // A fully-shared entry (reclaims nothing) hits the same clamp.
        let mut shared = feat("shared", 1024, 0, 5.0);
        shared.reclaimable_bytes = Some(0);
        assert!(size_pressure_score(&shared).is_finite());
    }

    /// The size term is *marginal reclaimable* bytes, not logical size
    /// (kunobi-ninja/kache#608): an entry with unique blobs must be evicted
    /// before an equally stale, larger entry whose blobs are all shared —
    /// evicting the latter frees nothing.
    #[test]
    fn size_pressure_prefers_entries_that_actually_free_bytes() {
        let mut shared_big = feat("shared_big", 500 * 1024 * 1024, 0, 10.0);
        shared_big.reclaimable_bytes = Some(0);
        let mut unique_small = feat("unique_small", 200 * 1024 * 1024, 0, 10.0);
        unique_small.reclaimable_bytes = Some(200 * 1024 * 1024);

        assert!(size_pressure_score(&unique_small) < size_pressure_score(&shared_big));
        let order = SizePressurePolicy.select(&[shared_big, unique_small]);
        assert_eq!(order, vec!["unique_small", "shared_big"]);
    }

    /// An entry not yet backfilled into `entry_blobs` (None) ranks on its
    /// logical size — the pre-#608 behavior — rather than as free-nothing.
    #[test]
    fn size_pressure_falls_back_to_logical_size_when_reclaimable_unknown() {
        let unknown = feat("unknown", 600 * 1024 * 1024, 0, 15.0);
        let small_hot = feat("small", 14 * 1024, 9, 0.1);
        assert!(size_pressure_score(&unknown) < size_pressure_score(&small_hot));
    }

    /// kunobi-ninja/kache#594: the shadow candidate ranks by rebuild cost per
    /// reclaimable byte — a big cheap entry goes before a small expensive
    /// one, the opposite of what size pressure prefers.
    #[test]
    fn value_density_evicts_cheap_per_byte_first() {
        let mut big_cheap = feat("big_cheap", 600 * 1024 * 1024, 0, 15.0);
        big_cheap.compile_time_ms = 300; // 0.3s for 600 MB
        let mut small_expensive = feat("small_expensive", 3 * 1024 * 1024, 0, 15.0);
        small_expensive.compile_time_ms = 6_000; // 6s for 3 MB

        assert!(value_density_score(&big_cheap) < value_density_score(&small_expensive));
        let order = ValueDensityPolicy.select(&[small_expensive, big_cheap]);
        assert_eq!(order, vec!["big_cheap", "small_expensive"]);
    }

    /// Density divides by marginal reclaimable bytes when known: an entry
    /// whose blobs are all shared frees nothing, so its retention costs
    /// nothing and it ranks LAST (infinite-ish density from the clamp).
    #[test]
    fn value_density_uses_reclaimable_bytes_and_ranks_unknown_cost_first() {
        let mut shared = feat("shared", 500 * 1024 * 1024, 0, 10.0);
        shared.reclaimable_bytes = Some(0);
        shared.compile_time_ms = 1_000;
        let mut unique = feat("unique", 10 * 1024 * 1024, 0, 10.0);
        unique.reclaimable_bytes = Some(10 * 1024 * 1024);
        unique.compile_time_ms = 1_000;
        assert!(value_density_score(&unique) < value_density_score(&shared));

        // Zero compile time (not yet backfilled) ranks as worthless — a
        // documented shadow-only rough edge.
        let unbackfilled = feat("unbackfilled", 1024, 0, 10.0);
        assert_eq!(value_density_score(&unbackfilled), 0.0);
    }

    /// Below the 0.001 MB clamp the size term saturates, so sub-clamp
    /// entries order purely by rebuild cost. Pins the clamp's placement on
    /// the MB-converted value: mis-scaling the conversion (which is
    /// order-preserving everywhere else) un-saturates these and flips them.
    #[test]
    fn value_density_orders_sub_clamp_entries_by_cost_alone() {
        let mut a = feat("a", 500, 0, 1.0);
        a.compile_time_ms = 1;
        let mut b = feat("b", 1000, 0, 1.0);
        b.compile_time_ms = 2;
        // Both clamp to 0.001 MB: scores are 1000 vs 2000, cheaper first.
        assert!(value_density_score(&a) < value_density_score(&b));
        let order = ValueDensityPolicy.select(&[b, a]);
        assert_eq!(order, vec!["a", "b"]);
    }

    /// The budget walk takes the ranking's prefix whose expected reclaim
    /// covers `bytes_needed`, and no more.
    #[test]
    fn would_evict_for_budget_takes_the_covering_prefix() {
        let mut a = feat("a", 100, 0, 1.0);
        a.reclaimable_bytes = Some(100);
        let mut b = feat("b", 100, 0, 1.0);
        b.reclaimable_bytes = Some(0); // fully shared: frees nothing
        let mut c = feat("c", 100, 0, 1.0);
        c.reclaimable_bytes = Some(100);
        let d = feat("d", 100, 0, 1.0); // unknown: expected reclaim = logical 100

        let candidates = vec![a, b, c, d];
        let order: Vec<String> = ["a", "b", "c", "d"].iter().map(|s| s.to_string()).collect();

        let victims = would_evict_for_budget(&candidates, &order, 150);
        // a frees 100 (< 150), b frees 0, c reaches 200 — d is spared.
        assert!(victims.contains("a") && victims.contains("b") && victims.contains("c"));
        assert!(!victims.contains("d"));

        assert!(would_evict_for_budget(&candidates, &order, 0).is_empty());
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
        b.reclaimable_bytes = Some(100);
        c.reclaimable_bytes = Some(100);

        let picked = DuplicatePolicy.select(&[a, b, c, lone]);
        assert_eq!(picked, vec!["dup_older", "dup_old"]);
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
        a.reclaimable_bytes = Some(100);
        b.reclaimable_bytes = Some(100);
        assert!(DuplicatePolicy.select(&[a, b]).is_empty());
    }

    /// kunobi-ninja/kache#709: a duplicate whose blob is still referenced
    /// must be skipped because removing it frees no physical bytes.
    #[test]
    fn duplicate_skips_victim_whose_blob_is_still_referenced() {
        let mut newest = feat("newest", 100, 0, 1.0);
        let mut oldest = feat("oldest", 100, 0, 9.0);
        newest.content_hash = Some("h".into());
        oldest.content_hash = Some("h".into());
        oldest.reclaimable_bytes = Some(0);

        assert!(
            DuplicatePolicy.select(&[newest, oldest]).is_empty(),
            "a zero-marginal-byte duplicate must never be selected"
        );
    }

    #[test]
    fn duplicate_fails_closed_when_marginal_bytes_are_unknown() {
        let mut newest = feat("newest", 100, 0, 1.0);
        let mut legacy = feat("legacy", 100, 0, 9.0);
        newest.content_hash = Some("h".into());
        legacy.content_hash = Some("h".into());
        assert_eq!(legacy.reclaimable_bytes, None);

        assert!(DuplicatePolicy.select(&[newest, legacy]).is_empty());
    }

    #[test]
    fn duplicate_still_evicts_victim_with_positive_marginal_bytes() {
        let mut newest = feat("newest", 100, 0, 1.0);
        let mut oldest = feat("oldest", 100, 0, 9.0);
        newest.content_hash = Some("h".into());
        oldest.content_hash = Some("h".into());
        oldest.reclaimable_bytes = Some(100);

        assert_eq!(DuplicatePolicy.select(&[newest, oldest]), vec!["oldest"]);
    }
}
