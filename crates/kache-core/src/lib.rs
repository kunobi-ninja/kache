#[cfg(feature = "planning")]
use std::collections::{HashMap, HashSet};

#[cfg(feature = "planning")]
use anyhow::Result;
#[cfg(feature = "planning")]
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BuildIntent {
    #[serde(default)]
    pub crate_names: Vec<String>,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub cargo_lock_deps: Vec<(String, String)>,
}

/// Which source produced a candidate, i.e. how much to trust it
/// (kunobi-ninja/kache#617).
///
/// `Unknown` is the `#[serde(other)]` arm so a newer planner naming a source
/// this build has never heard of degrades to "untrusted" instead of failing
/// the whole plan.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(kani, derive(kani::Arbitrary))]
#[serde(rename_all = "snake_case")]
pub enum CandidateSource {
    /// Exact lockfile-shard match: this build's dependency set produced it.
    Shard,
    /// This machine built this crate before.
    History,
    /// A crate NAME matched something in the remote listing. A crate name is
    /// not a build identity, so this is a guess.
    KeyCache,
    #[default]
    #[serde(other)]
    Unknown,
}

impl CandidateSource {
    /// Confidence rank, lower is better. Only the ORDER matters; these are not
    /// probabilities and must not be presented as any.
    pub fn confidence_rank(self) -> u8 {
        match self {
            CandidateSource::Shard => 0,
            CandidateSource::History => 1,
            CandidateSource::KeyCache => 2,
            CandidateSource::Unknown => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrefetchCandidate {
    pub cache_key: String,
    pub crate_name: String,
    /// What a miss would cost to rebuild. `None` = unknown, which is NOT the
    /// same as zero: an un-backfilled store row reads 0, and treating that as
    /// "free to fetch and worthless to have" would bury it (#617).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compile_time_ms: Option<u64>,
    /// Stored artifact size. An admission and ranking ESTIMATE, never a
    /// promise about compressed transfer bytes: anything enforced has to be
    /// counted on the wire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub source: CandidateSource,
    /// Position in the build's dependency order, i.e. roughly when the build
    /// will ask for it. `None` = not in the intent's crate list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demand_index: Option<u32>,
}

impl PrefetchCandidate {
    /// A candidate with no metadata, as the pre-#617 wire produced.
    pub fn new(cache_key: String, crate_name: String) -> Self {
        Self {
            cache_key,
            crate_name,
            compile_time_ms: None,
            size_bytes: None,
            source: CandidateSource::Unknown,
            demand_index: None,
        }
    }

    pub fn with_source(mut self, source: CandidateSource) -> Self {
        self.source = source;
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PrefetchDisposition {
    Execute,
    UseFallback,
    DoNothing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrefetchPlan {
    #[serde(default)]
    pub plan_id: Option<String>,
    #[serde(default)]
    pub planner: Option<String>,
    pub disposition: PrefetchDisposition,
    #[serde(default)]
    pub candidates: Vec<PrefetchCandidate>,
}

/// How many candidates each source may contribute to one plan
/// (kunobi-ninja/kache#616).
///
/// These bound plan COMPOSITION, which is a different job from the daemon's
/// key/byte/time budgets: those bound resource use and are the trust boundary,
/// these stop one low-confidence source from crowding out better candidates
/// before the budget is even reached. `0` disables a cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanLimits {
    /// Key-cache variants per crate. A crate name is not a build identity, so
    /// of `n` variants at most one can be the right one; taking many is paying
    /// `n` downloads for at most one hit.
    pub key_cache_per_crate: usize,
    /// Key-cache candidates across the whole plan, so a build with hundreds of
    /// unresolved crates cannot fill the plan with guesses.
    pub key_cache_total: usize,
    /// History entries per crate. Unlike shards, where one crate legitimately
    /// has several compile units, extra history rows for one crate are older
    /// variants.
    pub history_per_crate: usize,
}

impl Default for PlanLimits {
    fn default() -> Self {
        Self {
            key_cache_per_crate: 2,
            key_cache_total: 64,
            history_per_crate: 2,
        }
    }
}

/// Urgency bucket width, in dependency-order positions (#617).
///
/// Demand order is bucketed rather than used exactly because it comes from a
/// guppy graph traversal, which only approximates when cargo will actually ask
/// (cargo reorders for parallelism, build scripts, proc macros, features).
/// Treating position 40 and 45 as meaningfully different is false precision;
/// 40 versus 400 is real. Roughly the prefetch concurrency, so one window is
/// about one wave of downloads.
#[cfg(feature = "planning")]
const URGENCY_BUCKET: u32 = 16;

/// Sort key for dispatch order, lowest first (#617).
///
/// Lexicographic, deliberately, rather than a weighted score: a weighted sum
/// needs coefficients, and nothing can calibrate them until #618 makes
/// "arrived before it was demanded" measurable. Every element here is an
/// ordering, not a magnitude.
///
/// 1. Urgency bucket. Prefetch races the build, so a high-value artifact
///    needed at minute eight loses to a medium-value one needed at second
///    five. Candidates with no demand index sort last.
/// 2. Confidence. Within one wave, prefer the source most likely to be right.
/// 3. Value, descending. Expensive rebuilds first, so the limited slots buy
///    the most avoided work. Unknown cost sorts after known cost rather than
///    being scored as zero.
///
/// Callers must apply this as a STABLE sort: equal keys keep source order,
/// which is the planner's confidence-merge order.
#[cfg(feature = "planning")]
pub fn dispatch_sort_key(candidate: &PrefetchCandidate) -> (u32, u8, std::cmp::Reverse<u64>) {
    let bucket = candidate
        .demand_index
        .map(|index| index / URGENCY_BUCKET)
        .unwrap_or(u32::MAX);
    (
        bucket,
        candidate.source.confidence_rank(),
        // `None` -> 0 -> sorts last under Reverse, without claiming the
        // candidate is worthless.
        std::cmp::Reverse(candidate.compile_time_ms.unwrap_or(0)),
    )
}

#[cfg(feature = "planning")]
fn sort_candidates_for_dispatch(candidates: &mut [PrefetchCandidate]) {
    candidates.sort_by_key(dispatch_sort_key);
}

/// Truncate `items` to `limit`, returning how many were dropped. `0` disables.
///
/// Shared by every composition cap so "0 means unlimited" is defined once
/// rather than re-derived at each call site.
#[cfg(feature = "planning")]
fn cap_to(items: &mut Vec<PrefetchCandidate>, limit: usize) -> usize {
    if limit == 0 || items.len() <= limit {
        return 0;
    }
    let dropped = items.len() - limit;
    items.truncate(limit);
    dropped
}

/// What a plan left out, so a truncated plan is distinguishable from one that
/// had nothing more to offer (#616).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PlanComposition {
    pub from_shards: usize,
    pub from_history: usize,
    pub from_key_cache: usize,
    /// Dropped by [`PlanLimits`], by source.
    pub dropped_history_per_crate: usize,
    pub dropped_key_cache_per_crate: usize,
    pub dropped_key_cache_total: usize,
}

impl PlanComposition {
    pub fn dropped_total(&self) -> usize {
        self.dropped_history_per_crate
            + self.dropped_key_cache_per_crate
            + self.dropped_key_cache_total
    }
}

#[cfg(feature = "planning")]
#[async_trait]
pub trait PlannerDataSource {
    async fn shard_candidates(
        &self,
        namespace: &str,
        deps: &[(String, String)],
    ) -> Result<Vec<PrefetchCandidate>>;

    async fn history_candidates(&self, crate_names: &[String]) -> Result<Vec<PrefetchCandidate>>;

    async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>>;
}

#[cfg(feature = "planning")]
pub async fn build_prefetch_plan<T>(
    source: &T,
    intent: &BuildIntent,
    planner_name: &str,
) -> Result<PrefetchPlan>
where
    T: PlannerDataSource + Sync + ?Sized,
{
    build_prefetch_plan_with_limits(source, intent, planner_name, PlanLimits::default())
        .await
        .map(|(plan, _composition)| plan)
}

/// [`build_prefetch_plan`] with explicit composition limits, also returning
/// what the limits dropped so a caller can report it (#616).
#[cfg(feature = "planning")]
pub async fn build_prefetch_plan_with_limits<T>(
    source: &T,
    intent: &BuildIntent,
    planner_name: &str,
    limits: PlanLimits,
) -> Result<(PrefetchPlan, PlanComposition)>
where
    T: PlannerDataSource + Sync + ?Sized,
{
    let crate_order = crate_query_order(intent);
    let demand_index = demand_index_map(&crate_order);
    let mut seen = HashSet::new();
    let mut resolved_crates = HashSet::new();
    let mut candidates = Vec::new();
    let mut composition = PlanComposition::default();

    // Sources are merged in descending order of confidence, each one filling
    // only the crates the ones before it left unresolved. Shard lookups used
    // to RETURN as soon as they produced anything (kunobi-ninja/kache#614),
    // but a shard hit is exact per bucket: one dependency bump invalidates one
    // of `NUM_SHARDS` buckets while the rest still match, so a single matching
    // shard was enough to short-circuit history and key-cache recovery for
    // every crate in every bucket that missed.
    if let Some(namespace) = intent.namespace.as_deref()
        && !intent.cargo_lock_deps.is_empty()
    {
        // A failed shard lookup is not fatal: the sources below still run.
        if let Ok(shard_candidates) = source
            .shard_candidates(namespace, &intent.cargo_lock_deps)
            .await
        {
            for candidate in order_candidates_by_crate_order(shard_candidates, intent) {
                resolved_crates.insert(candidate.crate_name.clone());
                if seen.insert(candidate.cache_key.clone()) {
                    candidates.push(candidate.with_source(CandidateSource::Shard));
                }
            }
        }
    }

    let unresolved = |resolved: &HashSet<String>| -> Vec<String> {
        crate_order
            .iter()
            .filter(|name| !resolved.contains(*name))
            .cloned()
            .collect()
    };

    composition.from_shards = candidates.len();

    let history_query = unresolved(&resolved_crates);
    if !history_query.is_empty() {
        // Group by crate so the per-crate cap keeps the FIRST entries, which
        // both data sources return most-recently-used first.
        let mut by_crate: HashMap<String, Vec<PrefetchCandidate>> = HashMap::new();
        for candidate in order_candidates_by_crate_order(
            source.history_candidates(&history_query).await?,
            intent,
        ) {
            by_crate
                .entry(candidate.crate_name.clone())
                .or_default()
                .push(candidate.with_source(CandidateSource::History));
        }

        for crate_name in &history_query {
            let Some(mut for_crate) = by_crate.remove(crate_name) else {
                continue;
            };
            composition.dropped_history_per_crate +=
                cap_to(&mut for_crate, limits.history_per_crate);
            for candidate in for_crate {
                resolved_crates.insert(candidate.crate_name.clone());
                if seen.insert(candidate.cache_key.clone()) {
                    composition.from_history += 1;
                    candidates.push(candidate);
                }
            }
        }
    }

    // The key cache is the weakest source: it maps a crate NAME to every cache
    // key in the bucket, with no target, toolchain, profile, or feature
    // dimension, so of `n` variants at most one can be right. Capping is not a
    // fix for that (dimensioning the remote layout is, separately) but it stops
    // the guesses crowding out better candidates (#616).
    for crate_name in unresolved(&resolved_crates) {
        if limits.key_cache_total > 0 && composition.from_key_cache >= limits.key_cache_total {
            // Whatever this crate would have offered is dropped wholesale; count
            // it so the truncation is visible rather than inferred.
            composition.dropped_key_cache_total += source
                .key_cache_keys_for_crate(&crate_name)
                .await?
                .into_iter()
                .filter(|key| !seen.contains(key))
                .count();
            continue;
        }

        let mut for_crate: Vec<PrefetchCandidate> = source
            .key_cache_keys_for_crate(&crate_name)
            .await?
            .into_iter()
            .map(|cache_key| {
                PrefetchCandidate::new(cache_key, crate_name.clone())
                    .with_source(CandidateSource::KeyCache)
            })
            .collect();

        composition.dropped_key_cache_per_crate +=
            cap_to(&mut for_crate, limits.key_cache_per_crate);

        for candidate in for_crate {
            if limits.key_cache_total > 0 && composition.from_key_cache >= limits.key_cache_total {
                composition.dropped_key_cache_total += 1;
                continue;
            }
            if seen.insert(candidate.cache_key.clone()) {
                composition.from_key_cache += 1;
                candidates.push(candidate);
            }
        }
    }

    // Stamp demand position so the daemon can rank without the intent: it only
    // receives the plan, and `PrefetchRequest::from_plan` drops everything else.
    for candidate in &mut candidates {
        candidate.demand_index = demand_index.get(&candidate.crate_name).copied();
    }

    // Dispatch order (#617). Stable, so equal keys keep the confidence-merge
    // order the sources were appended in.
    sort_candidates_for_dispatch(&mut candidates);

    Ok((execute_plan(planner_name, candidates), composition))
}

#[cfg(feature = "planning")]
fn execute_plan(planner_name: &str, candidates: Vec<PrefetchCandidate>) -> PrefetchPlan {
    let planner = planner_name.trim();
    PrefetchPlan {
        plan_id: None,
        planner: Some(if planner.is_empty() {
            "planner".to_string()
        } else {
            planner.to_string()
        }),
        disposition: PrefetchDisposition::Execute,
        candidates,
    }
}

/// crate name -> position in dependency order, for [`dispatch_sort_key`].
#[cfg(feature = "planning")]
fn demand_index_map(crate_order: &[String]) -> HashMap<String, u32> {
    crate_order
        .iter()
        .enumerate()
        .map(|(index, name)| (name.clone(), index as u32))
        .collect()
}

#[cfg(feature = "planning")]
fn crate_query_order(intent: &BuildIntent) -> Vec<String> {
    let mut seen = HashSet::new();
    intent
        .crate_names
        .iter()
        .filter(|crate_name| seen.insert((*crate_name).clone()))
        .cloned()
        .collect()
}

#[cfg(feature = "planning")]
fn order_candidates_by_crate_order(
    mut candidates: Vec<PrefetchCandidate>,
    intent: &BuildIntent,
) -> Vec<PrefetchCandidate> {
    if intent.crate_names.is_empty() {
        return candidates;
    }

    let mut priority = HashMap::new();
    for (index, crate_name) in crate_query_order(intent).iter().enumerate() {
        priority.entry(crate_name.clone()).or_insert(index);
    }

    let mut indexed_candidates = candidates.drain(..).enumerate().collect::<Vec<_>>();
    indexed_candidates.sort_by_key(|(index, candidate)| {
        (
            priority
                .get(&candidate.crate_name)
                .copied()
                .unwrap_or(usize::MAX),
            *index,
        )
    });
    indexed_candidates
        .into_iter()
        .map(|(_, candidate)| candidate)
        .collect()
}

/// Bounded model-checking harnesses for the planner's dispatch contract.
///
/// These mirror the three lexicographic priorities documented by
/// [`dispatch_sort_key`] and cover every primitive input value, rather than a
/// sampled set. Kani injects its support crate when `cargo kani` sets
/// `cfg(kani)`, so normal builds carry no additional dependency.
#[cfg(all(kani, feature = "planning"))]
mod kani_proofs {
    use super::*;

    fn candidate(
        source: CandidateSource,
        compile_time_ms: Option<u64>,
        demand_index: Option<u32>,
    ) -> PrefetchCandidate {
        PrefetchCandidate {
            cache_key: String::new(),
            crate_name: String::new(),
            compile_time_ms,
            size_bytes: None,
            source,
            demand_index,
        }
    }

    #[kani::proof]
    fn known_demand_always_precedes_unknown_demand() {
        let demand_index: u32 = kani::any();
        let known = candidate(kani::any(), kani::any(), Some(demand_index));
        let unknown = candidate(kani::any(), kani::any(), None);
        kani::cover!();

        assert!(dispatch_sort_key(&known) < dispatch_sort_key(&unknown));
    }

    #[kani::proof]
    fn earlier_urgency_bucket_always_wins() {
        let earlier: u32 = kani::any();
        let later: u32 = kani::any();
        kani::assume(earlier / URGENCY_BUCKET < later / URGENCY_BUCKET);
        let earlier_candidate = candidate(kani::any(), kani::any(), Some(earlier));
        let later_candidate = candidate(kani::any(), kani::any(), Some(later));
        kani::cover!();

        assert!(dispatch_sort_key(&earlier_candidate) < dispatch_sort_key(&later_candidate));
    }

    #[kani::proof]
    fn confidence_always_wins_within_a_bucket() {
        let better_source: CandidateSource = kani::any();
        let worse_source: CandidateSource = kani::any();
        kani::assume(better_source.confidence_rank() < worse_source.confidence_rank());
        let better_demand: u32 = kani::any();
        let worse_demand: u32 = kani::any();
        kani::assume(better_demand / URGENCY_BUCKET == worse_demand / URGENCY_BUCKET);
        let better = candidate(better_source, kani::any(), Some(better_demand));
        let worse = candidate(worse_source, kani::any(), Some(worse_demand));
        kani::cover!();

        assert!(dispatch_sort_key(&better) < dispatch_sort_key(&worse));
    }

    #[kani::proof]
    fn higher_compile_cost_wins_after_urgency_and_confidence() {
        let cheaper: u64 = kani::any();
        let costlier: u64 = kani::any();
        kani::assume(cheaper < costlier);
        let cheaper_demand: u32 = kani::any();
        let costlier_demand: u32 = kani::any();
        kani::assume(cheaper_demand / URGENCY_BUCKET == costlier_demand / URGENCY_BUCKET);
        let source: CandidateSource = kani::any();
        let cheaper_candidate = candidate(source, Some(cheaper), Some(cheaper_demand));
        let costlier_candidate = candidate(source, Some(costlier), Some(costlier_demand));
        kani::cover!();

        assert!(dispatch_sort_key(&costlier_candidate) < dispatch_sort_key(&cheaper_candidate));
    }

    #[kani::proof]
    fn known_positive_compile_cost_precedes_unknown_cost() {
        let known_cost: u64 = kani::any();
        kani::assume(known_cost > 0);
        let demand_index: u32 = kani::any();
        let source: CandidateSource = kani::any();
        let known = candidate(source, Some(known_cost), Some(demand_index));
        let unknown = candidate(source, None, Some(demand_index));
        kani::cover!();

        assert!(dispatch_sort_key(&known) < dispatch_sort_key(&unknown));
    }

    #[kani::proof]
    fn dispatch_sort_preserves_equal_key_order() {
        let source: CandidateSource = kani::any();
        let compile_time_ms: Option<u64> = kani::any();
        let demand_index: Option<u32> = kani::any();
        let mut first = candidate(source, compile_time_ms, demand_index);
        first.size_bytes = Some(1);
        let mut second = candidate(source, compile_time_ms, demand_index);
        second.size_bytes = Some(2);
        let mut candidates = [first, second];

        sort_candidates_for_dispatch(&mut candidates);
        kani::cover!();

        assert_eq!(candidates[0].size_bytes, Some(1));
        assert_eq!(candidates[1].size_bytes, Some(2));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "planning")]
    use std::collections::HashMap;

    #[cfg(feature = "planning")]
    use anyhow::anyhow;

    #[cfg(feature = "planning")]
    use proptest::prelude::*;

    #[cfg(feature = "planning")]
    #[derive(Default)]
    struct FakePlannerDataSource {
        shard_candidates: Vec<PrefetchCandidate>,
        shard_error: bool,
        history_candidates: Vec<PrefetchCandidate>,
        history_by_crate: HashMap<String, String>,
        key_cache: HashMap<String, Vec<String>>,
    }

    #[cfg(feature = "planning")]
    #[async_trait]
    impl PlannerDataSource for FakePlannerDataSource {
        async fn shard_candidates(
            &self,
            _namespace: &str,
            _deps: &[(String, String)],
        ) -> Result<Vec<PrefetchCandidate>> {
            if self.shard_error {
                Err(anyhow!("shard lookup failed"))
            } else {
                Ok(self.shard_candidates.clone())
            }
        }

        async fn history_candidates(
            &self,
            crate_names: &[String],
        ) -> Result<Vec<PrefetchCandidate>> {
            if !self.history_candidates.is_empty() {
                return Ok(self.history_candidates.clone());
            }

            Ok(crate_names
                .iter()
                .filter_map(|crate_name| {
                    self.history_by_crate.get(crate_name).map(|cache_key| {
                        PrefetchCandidate::new(cache_key.clone(), crate_name.clone())
                    })
                })
                .collect())
        }

        async fn key_cache_keys_for_crate(&self, crate_name: &str) -> Result<Vec<String>> {
            Ok(self.key_cache.get(crate_name).cloned().unwrap_or_default())
        }
    }

    #[test]
    fn test_build_intent_serde_roundtrip() {
        let intent = BuildIntent {
            crate_names: vec!["serde".into(), "tokio".into()],
            namespace: Some("x86_64/hash/release".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let json = serde_json::to_string(&intent).unwrap();
        let parsed: BuildIntent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, intent);
    }

    #[test]
    fn test_build_intent_defaults_missing_fields() {
        let parsed: BuildIntent = serde_json::from_str(r#"{"crate_names":["serde"]}"#).unwrap();
        assert_eq!(parsed.crate_names, vec!["serde"]);
        assert!(parsed.namespace.is_none());
        assert!(parsed.cargo_lock_deps.is_empty());
    }

    #[test]
    fn test_prefetch_plan_serde_roundtrip() {
        let plan = PrefetchPlan {
            plan_id: Some("plan-1".into()),
            planner: Some("local".into()),
            disposition: PrefetchDisposition::Execute,
            candidates: vec![PrefetchCandidate::new("abc".into(), "serde".into())],
        };

        let json = serde_json::to_string(&plan).unwrap();
        let parsed: PrefetchPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, plan);
    }

    #[test]
    fn test_prefetch_plan_missing_disposition_is_rejected() {
        let err = serde_json::from_str::<PrefetchPlan>(
            r#"{"planner":"legacy","candidates":[{"cache_key":"abc","crate_name":"serde"}]}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn test_prefetch_plan_do_nothing_roundtrip() {
        let plan = PrefetchPlan {
            plan_id: Some("plan-2".into()),
            planner: Some("remote".into()),
            disposition: PrefetchDisposition::DoNothing,
            candidates: vec![],
        };

        let json = serde_json::to_string(&plan).unwrap();
        let parsed: PrefetchPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, plan);
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_prefers_shard_candidates() {
        let source = FakePlannerDataSource {
            shard_candidates: vec![PrefetchCandidate::new("from-shard".into(), "serde".into())],
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: Some("linux/hash/release".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        assert_eq!(plan.disposition, PrefetchDisposition::Execute);
        assert_eq!(plan.planner.as_deref(), Some("fallback"));
        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(plan.candidates[0].cache_key, "from-shard");
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_falls_back_to_history_and_key_cache() {
        let mut source = FakePlannerDataSource {
            shard_error: true,
            history_candidates: vec![PrefetchCandidate::new("history-key".into(), "serde".into())],
            ..Default::default()
        };
        source.key_cache.insert(
            "tokio".into(),
            vec!["tokio-key".into(), "history-key".into()],
        );

        let intent = BuildIntent {
            crate_names: vec!["serde".into(), "tokio".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        assert_eq!(plan.disposition, PrefetchDisposition::Execute);
        assert_eq!(plan.candidates.len(), 2);
        assert_eq!(plan.candidates[0].cache_key, "history-key");
        assert_eq!(plan.candidates[1].cache_key, "tokio-key");
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_orders_shard_candidates_by_crate_order() {
        let source = FakePlannerDataSource {
            shard_candidates: vec![
                PrefetchCandidate::new("app-key".into(), "app".into()),
                PrefetchCandidate::new("dep-key".into(), "dep".into()),
                PrefetchCandidate::new("middle-key".into(), "middle".into()),
            ],
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["dep".into(), "middle".into(), "app".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("dep".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["dep-key", "middle-key", "app-key"]);
    }

    /// A PARTIAL shard hit must not suppress the lower-confidence sources
    /// (kunobi-ninja/kache#614).
    ///
    /// Shard matching is exact per bucket, so a dependency bump invalidates
    /// one bucket while the rest still match. The planner used to return as
    /// soon as shards produced anything, so the crates in the missed buckets
    /// were dropped from the plan even though history and the key cache could
    /// resolve them.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_fills_crates_a_partial_shard_hit_missed() {
        let mut source = FakePlannerDataSource {
            // Only `dep` is in a bucket that still matches.
            shard_candidates: vec![PrefetchCandidate::new("dep-shard-key".into(), "dep".into())],
            history_by_crate: HashMap::from([("middle".into(), "middle-history-key".into())]),
            ..Default::default()
        };
        source
            .key_cache
            .insert("app".into(), vec!["app-key-cache-key".into()]);

        let intent = BuildIntent {
            crate_names: vec!["dep".into(), "middle".into(), "app".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("dep".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec!["dep-shard-key", "middle-history-key", "app-key-cache-key"],
            "each source should fill the crates the higher-confidence ones left unresolved"
        );
    }

    /// A crate the shards already resolved is not re-queried from the
    /// lower-confidence sources (#614): shard keys are exact, history and the
    /// key cache are not, so they only fill gaps.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_does_not_requery_shard_resolved_crates() {
        let mut source = FakePlannerDataSource {
            shard_candidates: vec![PrefetchCandidate::new(
                "serde-shard-key".into(),
                "serde".into(),
            )],
            history_by_crate: HashMap::from([("serde".into(), "serde-stale-history-key".into())]),
            ..Default::default()
        };
        source
            .key_cache
            .insert("serde".into(), vec!["serde-stale-key-cache-key".into()]);

        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            namespace: Some("linux/hash/debug".into()),
            cargo_lock_deps: vec![("serde".into(), "1.0.0".into())],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["serde-shard-key"]);
    }

    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_build_prefetch_plan_queries_history_by_crate_order() {
        let source = FakePlannerDataSource {
            history_by_crate: HashMap::from([
                ("app".into(), "app-key".into()),
                ("dep".into(), "dep-key".into()),
                ("middle".into(), "middle-key".into()),
            ]),
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["dep".into(), "middle".into(), "app".into()],
            namespace: None,
            cargo_lock_deps: vec![],
        };

        let plan = build_prefetch_plan(&source, &intent, "fallback")
            .await
            .unwrap();

        let keys = plan
            .candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["dep-key", "middle-key", "app-key"]);
    }

    #[cfg(feature = "planning")]
    proptest! {
        #[test]
        fn property_crate_query_order_preserves_first_occurrence(
            crate_ids in proptest::collection::vec(any::<u8>(), 1..64),
        ) {
            let mut crate_names = crate_ids
                .into_iter()
                .map(|id| format!("crate-{id}"))
                .collect::<Vec<_>>();
            crate_names.push(crate_names[0].clone());
            let intent = BuildIntent {
                crate_names: crate_names.clone(),
                ..Default::default()
            };

            let mut expected = Vec::new();
            for crate_name in crate_names {
                if !expected.contains(&crate_name) {
                    expected.push(crate_name);
                }
            }

            prop_assert_eq!(crate_query_order(&intent), expected);
        }

        #[test]
        fn property_candidate_ordering_is_a_stable_priority_permutation(
            requested_ids in proptest::collection::vec(0u8..8, 1..24),
            candidate_ids in proptest::collection::vec(0u8..16, 0..48),
        ) {
            let requested_names = requested_ids
                .into_iter()
                .map(|id| format!("crate-{id}"))
                .collect::<Vec<_>>();
            let intent = BuildIntent {
                crate_names: requested_names.clone(),
                ..Default::default()
            };

            // Interleave two unknown and two known candidates so even minimal
            // shrunk inputs exercise both priority and stable tie ordering.
            let mut candidate_names = Vec::with_capacity(candidate_ids.len() + 4);
            candidate_names.push("not-requested".to_string());
            candidate_names.push(requested_names[0].clone());
            candidate_names.extend(
                candidate_ids
                    .into_iter()
                    .map(|id| format!("crate-{id}")),
            );
            candidate_names.push("not-requested".to_string());
            candidate_names.push(requested_names[0].clone());

            let candidates = candidate_names
                .into_iter()
                .enumerate()
                .map(|(index, crate_name)| {
                    PrefetchCandidate::new(format!("key-{index}"), crate_name)
                })
                .collect::<Vec<_>>();

            let mut expected = candidates.clone();
            expected.sort_by_key(|candidate| {
                requested_names
                    .iter()
                    .position(|name| name == &candidate.crate_name)
                    .unwrap_or(usize::MAX)
            });

            prop_assert_eq!(
                order_candidates_by_crate_order(candidates, &intent),
                expected
            );
        }
    }
    // ── Composition caps and ranking (#616, #617) ────────────────────────

    #[cfg(feature = "planning")]
    fn candidate(
        key: &str,
        crate_name: &str,
        source: CandidateSource,
        compile_time_ms: Option<u64>,
        demand_index: Option<u32>,
    ) -> PrefetchCandidate {
        PrefetchCandidate {
            cache_key: key.into(),
            crate_name: crate_name.into(),
            compile_time_ms,
            size_bytes: None,
            source,
            demand_index,
        }
    }

    /// Confidence ordering is the whole point of the rank; assert the ORDER,
    /// not the literal numbers.
    #[test]
    fn test_confidence_rank_orders_sources() {
        assert!(
            CandidateSource::Shard.confidence_rank() < CandidateSource::History.confidence_rank()
        );
        assert!(
            CandidateSource::History.confidence_rank()
                < CandidateSource::KeyCache.confidence_rank()
        );
        assert!(
            CandidateSource::KeyCache.confidence_rank()
                < CandidateSource::Unknown.confidence_rank()
        );
    }

    /// An unrecognised source name degrades to `Unknown` instead of failing the
    /// whole plan (forward compatibility with a newer planner).
    #[test]
    fn test_unknown_candidate_source_deserializes() {
        let candidate: PrefetchCandidate =
            serde_json::from_str(r#"{"cache_key":"k","crate_name":"c","source":"telepathy"}"#)
                .unwrap();
        assert_eq!(candidate.source, CandidateSource::Unknown);
    }

    /// A pre-#617 candidate has no metadata and must not be rejected, and the
    /// missing fields must read as unknown rather than zero.
    #[test]
    fn test_legacy_candidate_wire_still_parses() {
        let candidate: PrefetchCandidate =
            serde_json::from_str(r#"{"cache_key":"k","crate_name":"c"}"#).unwrap();
        assert_eq!(candidate.compile_time_ms, None);
        assert_eq!(candidate.size_bytes, None);
        assert_eq!(candidate.demand_index, None);
        assert_eq!(candidate.source, CandidateSource::Unknown);
    }

    /// Urgency dominates value: prefetch races the build, so a costly artifact
    /// needed late loses to a cheaper one needed now (#617).
    #[cfg(feature = "planning")]
    #[test]
    fn test_dispatch_order_prefers_urgency_over_value() {
        let urgent_cheap = candidate("a", "a", CandidateSource::Shard, Some(10), Some(0));
        let late_expensive = candidate("b", "b", CandidateSource::Shard, Some(100_000), Some(500));
        assert!(dispatch_sort_key(&urgent_cheap) < dispatch_sort_key(&late_expensive));
    }

    /// Within one urgency window, value decides.
    #[cfg(feature = "planning")]
    #[test]
    fn test_dispatch_order_prefers_value_within_a_bucket() {
        let cheap = candidate("a", "a", CandidateSource::Shard, Some(10), Some(0));
        let costly = candidate("b", "b", CandidateSource::Shard, Some(5_000), Some(1));
        assert!(
            dispatch_sort_key(&costly) < dispatch_sort_key(&cheap),
            "same bucket, same source: the expensive rebuild goes first"
        );
    }

    /// Positions inside one bucket are treated as equally urgent: guppy order
    /// only approximates demand time, so finer distinctions are false precision.
    #[cfg(feature = "planning")]
    #[test]
    fn test_dispatch_order_buckets_nearby_positions_together() {
        let first = candidate("a", "a", CandidateSource::Shard, Some(10), Some(0));
        let nearby = candidate("b", "b", CandidateSource::Shard, Some(10), Some(15));
        let next_bucket = candidate("c", "c", CandidateSource::Shard, Some(10), Some(16));
        assert_eq!(
            dispatch_sort_key(&first),
            dispatch_sort_key(&nearby),
            "positions 0 and 15 share a bucket"
        );
        assert!(
            dispatch_sort_key(&nearby) < dispatch_sort_key(&next_bucket),
            "position 16 starts the next bucket"
        );
    }

    /// Within a bucket, confidence beats value: a shard match is worth more
    /// than a bigger number from a guess.
    #[cfg(feature = "planning")]
    #[test]
    fn test_dispatch_order_prefers_confidence_within_a_bucket() {
        let shard_cheap = candidate("a", "a", CandidateSource::Shard, Some(1), Some(0));
        let guess_costly = candidate("b", "b", CandidateSource::KeyCache, Some(99_999), Some(0));
        assert!(dispatch_sort_key(&shard_cheap) < dispatch_sort_key(&guess_costly));
    }

    /// Unknown cost sorts after known cost, but is not dropped and is not
    /// treated as zero-value against a *different* bucket.
    #[cfg(feature = "planning")]
    #[test]
    fn test_dispatch_order_places_unknown_cost_last_within_its_bucket() {
        let known = candidate("a", "a", CandidateSource::Shard, Some(1), Some(0));
        let unknown = candidate("b", "b", CandidateSource::Shard, None, Some(0));
        assert!(dispatch_sort_key(&known) < dispatch_sort_key(&unknown));

        // ...but an unknown-cost candidate needed NOW still beats a known-cost
        // one needed much later.
        let late_known = candidate("c", "c", CandidateSource::Shard, Some(50_000), Some(999));
        assert!(dispatch_sort_key(&unknown) < dispatch_sort_key(&late_known));
    }

    /// A candidate outside the intent's crate list sorts last rather than first.
    #[cfg(feature = "planning")]
    #[test]
    fn test_dispatch_order_places_unknown_demand_last() {
        let known = candidate("a", "a", CandidateSource::Shard, Some(1), Some(100_000));
        let no_demand = candidate("b", "b", CandidateSource::Shard, Some(50_000), None);
        assert!(dispatch_sort_key(&known) < dispatch_sort_key(&no_demand));
    }

    /// The production dispatch path applies the key with a stable sort.
    #[cfg(feature = "planning")]
    #[test]
    fn test_dispatch_sort_orders_candidates_and_preserves_ties() {
        let late = candidate("late", "late", CandidateSource::Shard, Some(1), Some(32));
        let first_tie = candidate("first", "first", CandidateSource::Shard, Some(10), Some(0));
        let second_tie = candidate(
            "second",
            "second",
            CandidateSource::Shard,
            Some(10),
            Some(0),
        );
        let mut candidates = vec![late, first_tie, second_tie];

        sort_candidates_for_dispatch(&mut candidates);

        let keys = candidates
            .iter()
            .map(|candidate| candidate.cache_key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, vec!["first", "second", "late"]);
    }

    /// The per-crate key-cache cap keeps the first N and reports the rest.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_key_cache_per_crate_cap() {
        let mut source = FakePlannerDataSource::default();
        source.key_cache.insert(
            "serde".into(),
            vec!["k1".into(), "k2".into(), "k3".into(), "k4".into()],
        );
        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let (plan, composition) = build_prefetch_plan_with_limits(
            &source,
            &intent,
            "fallback",
            PlanLimits {
                key_cache_per_crate: 2,
                ..PlanLimits::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(plan.candidates.len(), 2, "capped to two variants");
        assert_eq!(composition.from_key_cache, 2);
        assert_eq!(composition.dropped_key_cache_per_crate, 2);
        assert!(composition.dropped_total() > 0, "truncation is visible");
    }

    /// The plan-wide key-cache cap stops one weak source filling the plan, and
    /// counts what whole crates it skipped.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_key_cache_total_cap_counts_skipped_crates() {
        let mut source = FakePlannerDataSource::default();
        for name in ["a", "b", "c"] {
            source
                .key_cache
                .insert(name.into(), vec![format!("{name}-k1")]);
        }
        let intent = BuildIntent {
            crate_names: vec!["a".into(), "b".into(), "c".into()],
            ..Default::default()
        };

        let (plan, composition) = build_prefetch_plan_with_limits(
            &source,
            &intent,
            "fallback",
            PlanLimits {
                key_cache_total: 1,
                ..PlanLimits::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(plan.candidates.len(), 1);
        assert_eq!(composition.from_key_cache, 1);
        assert_eq!(
            composition.dropped_key_cache_total, 2,
            "the two skipped crates are counted, not silently missing"
        );
    }

    /// `0` disables a cap rather than dropping everything.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_zero_limit_disables_the_cap() {
        let mut source = FakePlannerDataSource::default();
        source
            .key_cache
            .insert("serde".into(), vec!["k1".into(), "k2".into(), "k3".into()]);
        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let (plan, composition) = build_prefetch_plan_with_limits(
            &source,
            &intent,
            "fallback",
            PlanLimits {
                key_cache_per_crate: 0,
                key_cache_total: 0,
                history_per_crate: 0,
            },
        )
        .await
        .unwrap();

        assert_eq!(plan.candidates.len(), 3);
        assert_eq!(composition.dropped_total(), 0);
    }

    /// Candidates are stamped with their source and demand position, so the
    /// daemon can rank without the intent (it only receives the plan).
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_plan_stamps_source_and_demand_index() {
        let source = FakePlannerDataSource {
            history_by_crate: HashMap::from([
                ("dep".into(), "dep-key".into()),
                ("app".into(), "app-key".into()),
            ]),
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["dep".into(), "app".into()],
            ..Default::default()
        };

        let (plan, _) =
            build_prefetch_plan_with_limits(&source, &intent, "fallback", PlanLimits::default())
                .await
                .unwrap();

        let dep = plan
            .candidates
            .iter()
            .find(|c| c.crate_name == "dep")
            .expect("dep planned");
        let app = plan
            .candidates
            .iter()
            .find(|c| c.crate_name == "app")
            .expect("app planned");
        assert_eq!(dep.source, CandidateSource::History);
        assert_eq!(dep.demand_index, Some(0));
        assert_eq!(app.demand_index, Some(1), "position follows crate order");
    }

    /// `cap_to` computes the DROPPED count, not a ratio. 5 items capped to 2
    /// drops 3; a divide would say 2. (Mutation-driven: `-` vs `/` agree on
    /// 4-and-2, so the earlier cap test could not tell them apart.)
    #[cfg(feature = "planning")]
    #[test]
    fn test_cap_to_reports_the_dropped_count() {
        let mut items: Vec<PrefetchCandidate> = (0..5)
            .map(|i| PrefetchCandidate::new(format!("k{i}"), "c".into()))
            .collect();
        assert_eq!(cap_to(&mut items, 2), 3);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].cache_key, "k0", "keeps the FIRST N");

        let mut exact: Vec<PrefetchCandidate> =
            vec![PrefetchCandidate::new("a".into(), "c".into())];
        assert_eq!(cap_to(&mut exact, 1), 0, "exactly at the limit drops none");
        assert_eq!(cap_to(&mut exact, 0), 0, "0 disables the cap");
        assert_eq!(exact.len(), 1);
    }

    /// `dropped_total` sums every drop class. Each field is distinct so a
    /// swapped operator or a missing term changes the result.
    #[test]
    fn test_dropped_total_sums_every_class() {
        let composition = PlanComposition {
            dropped_history_per_crate: 1,
            dropped_key_cache_per_crate: 2,
            dropped_key_cache_total: 4,
            ..PlanComposition::default()
        };
        assert_eq!(composition.dropped_total(), 7);
        assert_eq!(PlanComposition::default().dropped_total(), 0);
    }

    /// The per-crate history cap keeps the most-recent entries and reports the
    /// rest. History is ordered most-recently-used first by both data sources,
    /// so the cap keeps the freshest variants.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_history_per_crate_cap() {
        let source = FakePlannerDataSource {
            history_candidates: vec![
                PrefetchCandidate::new("serde-newest".into(), "serde".into()),
                PrefetchCandidate::new("serde-older".into(), "serde".into()),
                PrefetchCandidate::new("serde-oldest".into(), "serde".into()),
            ],
            ..Default::default()
        };
        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let (plan, composition) = build_prefetch_plan_with_limits(
            &source,
            &intent,
            "fallback",
            PlanLimits {
                history_per_crate: 1,
                ..PlanLimits::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(composition.from_history, 1);
        assert_eq!(composition.dropped_history_per_crate, 2);
        assert_eq!(
            plan.candidates
                .iter()
                .map(|c| c.cache_key.as_str())
                .collect::<Vec<_>>(),
            vec!["serde-newest"]
        );
    }

    /// Once the plan-wide key-cache budget is spent, a later crate is skipped
    /// WHOLESALE, and everything it would have offered is counted, not just the
    /// per-crate-capped subset.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_key_cache_total_skips_a_whole_crate_and_counts_all_of_it() {
        let mut source = FakePlannerDataSource::default();
        source.key_cache.insert("a".into(), vec!["a1".into()]);
        source.key_cache.insert(
            "b".into(),
            vec![
                "b1".into(),
                "b2".into(),
                "b3".into(),
                "b4".into(),
                "b5".into(),
            ],
        );
        let intent = BuildIntent {
            crate_names: vec!["a".into(), "b".into()],
            ..Default::default()
        };

        let (_plan, composition) = build_prefetch_plan_with_limits(
            &source,
            &intent,
            "fallback",
            PlanLimits {
                key_cache_per_crate: 2,
                key_cache_total: 1,
                history_per_crate: 2,
            },
        )
        .await
        .unwrap();

        assert_eq!(composition.from_key_cache, 1, "only crate `a` fits");
        assert_eq!(
            composition.dropped_key_cache_total, 5,
            "all five of `b`'s variants are counted, not the two a per-crate \
             cap would have admitted"
        );
        assert_eq!(
            composition.dropped_key_cache_per_crate, 0,
            "`b` was skipped before the per-crate cap applied"
        );
    }

    /// The budget can also run out PART WAY through one crate's variants: the
    /// remainder is dropped and counted individually.
    #[cfg(feature = "planning")]
    #[tokio::test]
    async fn test_key_cache_total_can_run_out_mid_crate() {
        let mut source = FakePlannerDataSource::default();
        source
            .key_cache
            .insert("serde".into(), vec!["k1".into(), "k2".into(), "k3".into()]);
        let intent = BuildIntent {
            crate_names: vec!["serde".into()],
            ..Default::default()
        };

        let (_plan, composition) = build_prefetch_plan_with_limits(
            &source,
            &intent,
            "fallback",
            PlanLimits {
                key_cache_per_crate: 2,
                key_cache_total: 1,
                history_per_crate: 2,
            },
        )
        .await
        .unwrap();

        assert_eq!(composition.from_key_cache, 1);
        assert_eq!(
            composition.dropped_key_cache_per_crate, 1,
            "k3 dropped by the per-crate cap"
        );
        assert_eq!(
            composition.dropped_key_cache_total, 1,
            "k2 dropped by the plan-wide cap, mid-crate"
        );
    }
}
