//! Prometheus metrics for the planner, scraped into SigNoz.
//!
//! The planner's operational question is not "is the process up" — Kubernetes
//! already answers that, and a readiness probe is what finally surfaced the
//! 13-day CrashLoopBackOff in kunobi-ninja/kache#747. It is "of the requests
//! that arrive, how many produce a usable plan, and when they do not, why".
//!
//! A planner that answers every request with `use_fallback` is indistinguishable
//! from a healthy one at the Kubernetes level: it is ready, it serves 200s, it
//! burns no CPU. Every client silently falls back to building from scratch and
//! the cache quietly stops paying for itself. `planner_requests_total` split by
//! outcome is the signal that separates those two worlds, which is why the
//! fallback reasons are kept distinct rather than collapsed into one counter —
//! "no state configured" is a deployment mistake, "planning error" is a bug, and
//! "no candidates" is a cold or mismatched cache. They need different responses.

use prometheus::{
    Encoder, HistogramVec, IntCounterVec, IntGauge, Registry, TextEncoder, histogram_opts, opts,
};
use std::sync::OnceLock;

/// Why a planner request ended the way it did.
///
/// Carried as the `outcome` label. Deliberately an enum rather than free
/// strings: the label set is bounded by construction, so no handler can grow
/// the cardinality of this metric by accident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// A plan with at least one candidate — the case the planner exists for.
    Execute,
    /// No service-side state is configured. A deployment problem, not a cache
    /// state: the planner cannot answer anything useful until it is fixed.
    FallbackNoState,
    /// Planning itself failed. A bug or a broken database; the request still
    /// gets a usable answer, so nothing else surfaces it.
    FallbackPlanningError,
    /// State exists but resolved nothing. A cold or mismatched cache — expected
    /// early on, worth alerting on if it stays high.
    FallbackNoCandidates,
    /// Rejected: missing or wrong bearer token.
    Unauthorized,
    /// Rejected: the planner has not finished coming up.
    NotReady,
}

impl Outcome {
    fn as_label(self) -> &'static str {
        match self {
            Outcome::Execute => "execute",
            Outcome::FallbackNoState => "fallback_no_state",
            Outcome::FallbackPlanningError => "fallback_planning_error",
            Outcome::FallbackNoCandidates => "fallback_no_candidates",
            Outcome::Unauthorized => "unauthorized",
            Outcome::NotReady => "not_ready",
        }
    }

    /// Every variant, so each series is registered at startup.
    ///
    /// A counter that has never been incremented is absent from `/metrics`
    /// entirely, and absent reads as zero on some panels and as "no data" on
    /// others. Initialising all of them makes "no fallbacks yet" and "this
    /// planner is not reporting" distinguishable from the first scrape.
    const ALL: [Outcome; 6] = [
        Outcome::Execute,
        Outcome::FallbackNoState,
        Outcome::FallbackPlanningError,
        Outcome::FallbackNoCandidates,
        Outcome::Unauthorized,
        Outcome::NotReady,
    ];
}

pub(crate) struct Metrics {
    registry: Registry,
    requests: IntCounterVec,
    duration: HistogramVec,
    candidates: HistogramVec,
    ready: IntGauge,
}

impl Metrics {
    fn new() -> Self {
        let registry = Registry::new();

        let requests = IntCounterVec::new(
            opts!(
                "kache_planner_requests_total",
                "Planner requests by outcome. `execute` means a usable plan was returned; \
                 every other value means the client fell back to building without help."
            ),
            &["outcome"],
        )
        .expect("planner request counter is statically valid");

        let duration = HistogramVec::new(
            histogram_opts!(
                "kache_planner_request_duration_seconds",
                "Time to answer a planner request. Buckets are tight at the low end: the \
                 planner sits in front of a build, so its own latency is pure overhead.",
                vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
            ),
            &["outcome"],
        )
        .expect("planner duration histogram is statically valid");

        let candidates = HistogramVec::new(
            histogram_opts!(
                "kache_planner_plan_candidates",
                "Candidates per returned plan. Separates a planner that answers with one \
                 lucky artefact from one resolving a whole dependency set.",
                vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0]
            ),
            &[] as &[&str],
        )
        .expect("planner candidate histogram is statically valid");

        let ready = IntGauge::with_opts(opts!(
            "kache_planner_ready",
            "1 when the planner is serving, 0 while it is still coming up. Mirrors /readyz \
             so the same fact is available to a scrape without an HTTP probe."
        ))
        .expect("planner ready gauge is statically valid");

        registry
            .register(Box::new(requests.clone()))
            .expect("request counter registers once");
        registry
            .register(Box::new(duration.clone()))
            .expect("duration histogram registers once");
        registry
            .register(Box::new(candidates.clone()))
            .expect("candidate histogram registers once");
        registry
            .register(Box::new(ready.clone()))
            .expect("ready gauge registers once");

        for outcome in Outcome::ALL {
            requests.with_label_values(&[outcome.as_label()]);
            duration.with_label_values(&[outcome.as_label()]);
        }

        Self {
            registry,
            requests,
            duration,
            candidates,
            ready,
        }
    }

    /// Record a finished planner request.
    pub(crate) fn record_request(&self, outcome: Outcome, seconds: f64) {
        let label = outcome.as_label();
        self.requests.with_label_values(&[label]).inc();
        self.duration.with_label_values(&[label]).observe(seconds);
    }

    /// Record the size of a plan that was actually returned for execution.
    pub(crate) fn record_plan_candidates(&self, count: usize) {
        self.candidates
            .with_label_values(&[] as &[&str])
            .observe(count as f64);
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.ready.set(i64::from(ready));
    }

    /// Render the registry in the Prometheus text format.
    pub(crate) fn render(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        if let Err(error) = encoder.encode(&self.registry.gather(), &mut buffer) {
            // Losing a scrape must never take the planner down with it.
            tracing::warn!(%error, "encoding metrics failed");
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}

/// Process-wide metrics.
///
/// A single registry for the process, because a scrape has one endpoint to read
/// and Prometheus counters are meaningless if they reset per request.
pub(crate) fn metrics() -> &'static Metrics {
    static METRICS: OnceLock<Metrics> = OnceLock::new();
    METRICS.get_or_init(Metrics::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_outcome_has_a_distinct_label() {
        let labels: Vec<&str> = Outcome::ALL.iter().map(|o| o.as_label()).collect();
        let mut unique = labels.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(
            labels.len(),
            unique.len(),
            "two outcomes share a label, so they would be summed into one series: {labels:?}"
        );
    }

    /// Counters must exist at zero before anything happens.
    ///
    /// Prometheus omits a labelled series that was never touched, and an absent
    /// series is not the same as zero: a dashboard cannot tell "no fallbacks"
    /// from "this planner is not reporting". Pinning this because it is silent
    /// when it regresses — the endpoint still returns 200, just with fewer
    /// series than the panels expect.
    #[test]
    fn all_outcome_series_exist_before_any_request() {
        let rendered = Metrics::new().render();
        for outcome in Outcome::ALL {
            let series = format!(
                "kache_planner_requests_total{{outcome=\"{}\"}}",
                outcome.as_label()
            );
            assert!(
                rendered.contains(&series),
                "{series} missing from a fresh registry; absent reads as no-data, not zero"
            );
        }
    }

    #[test]
    fn recording_a_request_increments_its_outcome_only() {
        let m = Metrics::new();
        m.record_request(Outcome::Execute, 0.01);
        let rendered = m.render();
        assert!(rendered.contains("kache_planner_requests_total{outcome=\"execute\"} 1"));
        assert!(
            rendered.contains("kache_planner_requests_total{outcome=\"unauthorized\"} 0"),
            "an unrelated outcome moved, so the label is not selecting one series"
        );
    }

    /// Value of a sample line, ignoring `# HELP` / `# TYPE`.
    ///
    /// Substring matching on the whole render is not safe here: `# HELP
    /// kache_planner_ready 1 when the planner is serving…` contains the exact
    /// text `kache_planner_ready 1`, so a `contains` assertion passes whatever
    /// the gauge holds. That is not hypothetical — it let a mutant that stubbed
    /// out `set_ready` survive the changed-line lane. Match the sample line.
    fn sample(rendered: &str, name: &str) -> Option<f64> {
        rendered
            .lines()
            .filter(|line| !line.starts_with('#'))
            .find_map(|line| {
                let (key, value) = line.rsplit_once(' ')?;
                (key == name).then(|| value.parse().ok())?
            })
    }

    #[test]
    fn ready_gauge_tracks_both_directions() {
        let m = Metrics::new();

        m.set_ready(false);
        assert_eq!(
            sample(&m.render(), "kache_planner_ready"),
            Some(0.0),
            "gauge did not follow set_ready(false)"
        );

        m.set_ready(true);
        assert_eq!(
            sample(&m.render(), "kache_planner_ready"),
            Some(1.0),
            "gauge did not follow set_ready(true)"
        );
    }

    /// Plan size has to actually reach the histogram.
    ///
    /// Nothing else observes this one: the request counter moves whether or not
    /// the candidate count is recorded, so a `record_plan_candidates` that did
    /// nothing would leave every other assertion here green while the only
    /// signal describing how much work a plan saves silently stayed empty.
    #[test]
    fn recording_plan_candidates_observes_the_histogram() {
        let m = Metrics::new();
        let before = sample(&m.render(), "kache_planner_plan_candidates_count").unwrap_or(0.0);
        let sum_before = sample(&m.render(), "kache_planner_plan_candidates_sum").unwrap_or(0.0);

        m.record_plan_candidates(7);

        let rendered = m.render();
        assert_eq!(
            sample(&rendered, "kache_planner_plan_candidates_count"),
            Some(before + 1.0),
            "observation was not counted"
        );
        assert_eq!(
            sample(&rendered, "kache_planner_plan_candidates_sum"),
            Some(sum_before + 7.0),
            "observation was counted but its value was lost"
        );
    }
}
