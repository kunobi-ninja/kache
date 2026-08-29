//! OTLP JSON snapshot of live cache counters, for Kartero to pick up later.
//!
//! Same on-disk contract as the bench emitter (`metrics.otlp.json` +
//! `schema_version`): the file is already an OTLP/HTTP
//! `ExportMetricsServiceRequest` body. There is no collector POST from kache
//! itself — CI uploads the files and Kartero imports them.
//!
//! Metric names live under `kache.cache.*` / `kache.prefetch.*` (scope
//! `kache.cache`). Bench gauges stay in `kache.bench.*` and must not be mixed
//! into this payload. Kartero drops non-gauge series, so this dump is a
//! point-in-time gauge snapshot of daemon/store totals, not cumulative sums.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Major schema version in the sidecar file and as a resource attribute.
pub(crate) const SCHEMA_VERSION: u32 = 1;

pub(crate) const METRICS_FILE: &str = "metrics.otlp.json";
pub(crate) const SCHEMA_VERSION_FILE: &str = "schema_version";

const SCOPE_NAME: &str = "kache.cache";
const DEFAULT_SERVICE_NAME: &str = "kache";

/// Cheap snapshot of process-lifetime daemon counters plus store gauges.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OtelSnapshot {
    pub remote_kind: &'static str,
    pub store_max: u64,
    pub store_size: Option<u64>,
    pub store_entries: Option<u64>,
    pub pending_uploads: Option<u64>,
    pub active_downloads: Option<u64>,
    pub s3_concurrency_total: u64,
    pub s3_concurrency_used: u64,
    pub uploads_completed: u64,
    pub uploads_failed: u64,
    pub uploads_skipped: u64,
    pub uploads_suppressed: u64,
    pub downloads_completed: u64,
    pub downloads_failed: u64,
    pub downloads_suppressed: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub remote_check_roundtrips: u64,
    pub negative_hits: u64,
    pub negative_entries: u64,
    pub remote_degraded: bool,
    pub prefetch_downloads: u64,
    pub prefetch_bytes: u64,
    pub prefetch_keys_used: u64,
    pub prefetch_keys_cancelled: u64,
    pub prefetch_keys_over_budget: u64,
    pub prefetch_plans_advisory: u64,
    pub prefetch_plans_fallback: u64,
    pub prefetch_list_requests: u64,
    pub prefetch_list_failures: u64,
    pub prefetch_pack_requests: u64,
    pub prefetch_v3_requests: u64,
    pub prefetch_cancelled: bool,
    pub prefetch_last_plan_candidates: u64,
    pub prefetch_last_plan_wall_ms: u64,
}

pub(crate) fn write_otlp(
    dir: &Path,
    snap: &OtelSnapshot,
    service_version: &str,
    scenario: Option<&str>,
    phase: Option<&str>,
) -> Result<()> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating telemetry dir {}", dir.display()))?;
    let body = serialize_metrics(
        snap,
        DEFAULT_SERVICE_NAME,
        service_version,
        &unix_nano_now(),
        scenario,
        phase,
    );
    let metrics_path = dir.join(METRICS_FILE);
    std::fs::write(
        &metrics_path,
        serde_json::to_string(&body).context("serializing OTLP metrics")? + "\n",
    )
    .with_context(|| format!("writing {}", metrics_path.display()))?;
    std::fs::write(dir.join(SCHEMA_VERSION_FILE), format!("{SCHEMA_VERSION}\n"))
        .with_context(|| format!("writing {}", dir.join(SCHEMA_VERSION_FILE).display()))?;
    Ok(())
}

pub(crate) fn serialize_metrics(
    snap: &OtelSnapshot,
    service_name: &str,
    service_version: &str,
    time_unix_nano: &str,
    scenario: Option<&str>,
    phase: Option<&str>,
) -> Value {
    let mut resource = vec![
        str_attr("service.name", service_name),
        str_attr("service.version", service_version),
        str_attr(
            "kache.telemetry.schema_version",
            &SCHEMA_VERSION.to_string(),
        ),
        str_attr("kache.cache.remote", snap.remote_kind),
    ];
    // Same string as `kache.bench.project` so a SigNoz query can join
    // daemon counters to the bench that produced them.
    if let Some(scenario) = scenario.filter(|s| !s.is_empty()) {
        resource.push(str_attr("kache.cache.scenario", scenario));
    }
    // Benches stop the daemon between phases, so counters are per daemon
    // lifetime. Tag the phase so cold and warm dumps do not collide.
    if let Some(phase) = phase.filter(|s| !s.is_empty()) {
        resource.push(str_attr("kache.cache.phase", phase));
    }
    json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": resource
            },
            "scopeMetrics": [{
                "scope": {
                    "name": SCOPE_NAME,
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "metrics": metrics_for(snap, time_unix_nano),
            }]
        }]
    })
}

fn metrics_for(snap: &OtelSnapshot, now: &str) -> Vec<Value> {
    let mut metrics = Vec::new();

    if let Some(size) = snap.store_size {
        metrics.push(gauge(
            "kache.cache.store.size",
            "By",
            vec![as_int(size, now, &[])],
        ));
    }
    if let Some(entries) = snap.store_entries {
        metrics.push(gauge(
            "kache.cache.store.entries",
            "{entry}",
            vec![as_int(entries, now, &[])],
        ));
    }
    metrics.push(gauge(
        "kache.cache.store.max",
        "By",
        vec![as_int(snap.store_max, now, &[])],
    ));
    if let Some(pending) = snap.pending_uploads {
        metrics.push(gauge(
            "kache.cache.uploads.pending",
            "{upload}",
            vec![as_int(pending, now, &[])],
        ));
    }
    if let Some(active) = snap.active_downloads {
        metrics.push(gauge(
            "kache.cache.downloads.active",
            "{download}",
            vec![as_int(active, now, &[])],
        ));
    }
    metrics.push(gauge(
        "kache.cache.s3.concurrency",
        "{permit}",
        vec![
            as_int(
                snap.s3_concurrency_used,
                now,
                &[str_attr("kache.cache.limit", "used")],
            ),
            as_int(
                snap.s3_concurrency_total,
                now,
                &[str_attr("kache.cache.limit", "total")],
            ),
        ],
    ));
    metrics.push(gauge(
        "kache.cache.remote.degraded",
        "1",
        vec![as_int(u64::from(snap.remote_degraded), now, &[])],
    ));
    metrics.push(gauge(
        "kache.cache.negative_entries",
        "{entry}",
        vec![as_int(snap.negative_entries, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.cancelled",
        "1",
        vec![as_int(u64::from(snap.prefetch_cancelled), now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.last_plan.candidates",
        "{candidate}",
        vec![as_int(snap.prefetch_last_plan_candidates, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.last_plan.wall",
        "ms",
        vec![as_int(snap.prefetch_last_plan_wall_ms, now, &[])],
    ));

    metrics.push(gauge(
        "kache.cache.uploads",
        "{upload}",
        vec![
            as_int(snap.uploads_completed, now, &result_attr("completed")),
            as_int(snap.uploads_failed, now, &result_attr("failed")),
            as_int(snap.uploads_skipped, now, &result_attr("skipped")),
            as_int(snap.uploads_suppressed, now, &result_attr("suppressed")),
        ],
    ));
    metrics.push(gauge(
        "kache.cache.downloads",
        "{download}",
        vec![
            as_int(snap.downloads_completed, now, &result_attr("completed")),
            as_int(snap.downloads_failed, now, &result_attr("failed")),
            as_int(snap.downloads_suppressed, now, &result_attr("suppressed")),
        ],
    ));
    metrics.push(gauge(
        "kache.cache.bytes",
        "By",
        vec![
            as_int(
                snap.bytes_uploaded,
                now,
                &[str_attr("kache.cache.direction", "upload")],
            ),
            as_int(
                snap.bytes_downloaded,
                now,
                &[str_attr("kache.cache.direction", "download")],
            ),
        ],
    ));
    metrics.push(gauge(
        "kache.cache.remote_checks",
        "{check}",
        vec![as_int(snap.remote_check_roundtrips, now, &[])],
    ));
    metrics.push(gauge(
        "kache.cache.negative_hits",
        "{hit}",
        vec![as_int(snap.negative_hits, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.downloads",
        "{download}",
        vec![as_int(snap.prefetch_downloads, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.bytes",
        "By",
        vec![as_int(snap.prefetch_bytes, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.keys_used",
        "{key}",
        vec![as_int(snap.prefetch_keys_used, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.keys_cancelled",
        "{key}",
        vec![as_int(snap.prefetch_keys_cancelled, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.keys_over_budget",
        "{key}",
        vec![as_int(snap.prefetch_keys_over_budget, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.plans",
        "{plan}",
        vec![
            as_int(
                snap.prefetch_plans_advisory,
                now,
                &[str_attr("kache.prefetch.kind", "advisory")],
            ),
            as_int(
                snap.prefetch_plans_fallback,
                now,
                &[str_attr("kache.prefetch.kind", "fallback")],
            ),
        ],
    ));
    metrics.push(gauge(
        "kache.prefetch.list.requests",
        "{request}",
        vec![as_int(snap.prefetch_list_requests, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.list.failures",
        "{request}",
        vec![as_int(snap.prefetch_list_failures, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.pack.requests",
        "{request}",
        vec![as_int(snap.prefetch_pack_requests, now, &[])],
    ));
    metrics.push(gauge(
        "kache.prefetch.v3.requests",
        "{request}",
        vec![as_int(snap.prefetch_v3_requests, now, &[])],
    ));
    metrics
}

fn result_attr(result: &str) -> Vec<Value> {
    vec![str_attr("kache.cache.result", result)]
}

fn unix_nano_now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}

fn str_attr(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

fn as_int(value: u64, time_unix_nano: &str, attributes: &[Value]) -> Value {
    json!({
        "asInt": value.to_string(),
        "timeUnixNano": time_unix_nano,
        "attributes": attributes,
    })
}

fn gauge(name: &str, unit: &str, data_points: Vec<Value>) -> Value {
    json!({
        "name": name,
        "unit": unit,
        "gauge": { "dataPoints": data_points }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn sample_snap() -> OtelSnapshot {
        OtelSnapshot {
            remote_kind: "s3",
            store_max: 50 * 1024 * 1024 * 1024,
            store_size: Some(1234),
            store_entries: Some(9),
            pending_uploads: Some(2),
            active_downloads: Some(1),
            s3_concurrency_total: 16,
            s3_concurrency_used: 3,
            uploads_completed: 10,
            uploads_failed: 1,
            uploads_skipped: 2,
            uploads_suppressed: 0,
            downloads_completed: 8,
            downloads_failed: 0,
            downloads_suppressed: 1,
            bytes_uploaded: 100,
            bytes_downloaded: 200,
            remote_check_roundtrips: 5,
            negative_hits: 4,
            negative_entries: 3,
            remote_degraded: false,
            prefetch_downloads: 7,
            prefetch_bytes: 70,
            prefetch_keys_used: 6,
            prefetch_keys_cancelled: 1,
            prefetch_keys_over_budget: 0,
            prefetch_plans_advisory: 2,
            prefetch_plans_fallback: 1,
            prefetch_list_requests: 3,
            prefetch_list_failures: 0,
            prefetch_pack_requests: 1,
            prefetch_v3_requests: 4,
            prefetch_cancelled: false,
            prefetch_last_plan_candidates: 12,
            prefetch_last_plan_wall_ms: 40,
        }
    }

    fn metric<'a>(body: &'a Value, name: &str) -> &'a Value {
        body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == name)
            .unwrap_or_else(|| panic!("missing metric {name}"))
    }

    fn all_attr_keys(body: &Value) -> BTreeSet<String> {
        let mut keys = BTreeSet::new();
        for attr in body["resourceMetrics"][0]["resource"]["attributes"]
            .as_array()
            .unwrap()
        {
            keys.insert(attr["key"].as_str().unwrap().to_string());
        }
        for m in body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
        {
            let points = m["gauge"]["dataPoints"].as_array().unwrap();
            for point in points {
                for attr in point["attributes"].as_array().unwrap() {
                    keys.insert(attr["key"].as_str().unwrap().to_string());
                }
            }
        }
        keys
    }

    #[test]
    fn attribute_set_is_the_allowlist() {
        let body = serialize_metrics(
            &sample_snap(),
            "kache",
            "0.16.0",
            "1700000000000000000",
            None,
            None,
        );
        let expected: BTreeSet<_> = [
            "service.name",
            "service.version",
            "kache.telemetry.schema_version",
            "kache.cache.remote",
            "kache.cache.result",
            "kache.cache.direction",
            "kache.cache.limit",
            "kache.prefetch.kind",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(all_attr_keys(&body), expected);
        let dumped = body.to_string();
        assert!(!dumped.contains("kache.bench."));
        assert!(!dumped.contains("run_id"));
        assert!(!dumped.contains("cicd."));
        assert!(!dumped.contains("cache_key"));
        assert!(!dumped.contains("\"sum\""));
    }

    #[test]
    fn scope_is_cache_not_bench() {
        let body = serialize_metrics(&sample_snap(), "kache", "0.16.0", "1", None, None);
        assert_eq!(
            body["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"],
            SCOPE_NAME
        );
    }

    #[test]
    fn counters_are_int_gauges() {
        let body = serialize_metrics(
            &sample_snap(),
            "kache",
            "0.16.0",
            "1700000000000000000",
            None,
            None,
        );
        let uploads = metric(&body, "kache.cache.uploads");
        assert!(uploads.get("sum").is_none());
        assert_eq!(uploads["gauge"]["dataPoints"][0]["asInt"], "10");
        assert_eq!(
            uploads["gauge"]["dataPoints"][0]["attributes"][0]["value"]["stringValue"],
            "completed"
        );
    }

    #[test]
    fn write_otlp_emits_kartero_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        write_otlp(dir.path(), &sample_snap(), "0.16.0", None, None).unwrap();
        let metrics = dir.path().join(METRICS_FILE);
        let version = dir.path().join(SCHEMA_VERSION_FILE);
        assert!(metrics.is_file());
        assert_eq!(std::fs::read_to_string(version).unwrap().trim(), "1");
        let body: Value = serde_json::from_str(&std::fs::read_to_string(metrics).unwrap()).unwrap();
        assert_eq!(
            body["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"],
            "kache.cache"
        );
    }

    #[test]
    fn scenario_is_the_join_key_to_the_bench() {
        let body = serialize_metrics(
            &sample_snap(),
            "kache",
            "0.16.0",
            "1",
            Some("bench-firefox"),
            Some("warm"),
        );
        assert!(all_attr_keys(&body).contains("kache.cache.scenario"));
        assert!(all_attr_keys(&body).contains("kache.cache.phase"));
        let attrs = body["resourceMetrics"][0]["resource"]["attributes"]
            .as_array()
            .unwrap();
        let scenario = attrs
            .iter()
            .find(|a| a["key"] == "kache.cache.scenario")
            .unwrap();
        assert_eq!(scenario["value"]["stringValue"], "bench-firefox");
        let phase = attrs
            .iter()
            .find(|a| a["key"] == "kache.cache.phase")
            .unwrap();
        assert_eq!(phase["value"]["stringValue"], "warm");
        assert!(
            !body.to_string().contains("kache.bench."),
            "join key must not pull bench metric names onto the cache dump"
        );
    }
}
