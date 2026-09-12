//! OTLP JSON snapshot of live cache counters, for Kartero to pick up later.
//!
//! Same on-disk contract as the bench emitter (`metrics.otlp.json` +
//! `schema_version`): the file is already an OTLP/HTTP
//! `ExportMetricsServiceRequest` body. There is no collector POST from kache
//! itself — CI uploads the files and Kartero imports them.
//!
//! Metric names live under `kache.cache.*` / `kache.prefetch.*` (scope
//! `kache.cache`). Bench gauges stay in `kache.bench.*` and must not be mixed
//! into this payload.
//!
//! Instrument choice follows what the number is. Store totals, queue depths
//! and flags are read at an instant and are gauges. Everything the daemon only
//! ever adds to is a cumulative sum carrying the process start as
//! `startTimeUnixNano`, so a restart reads as a counter reset rather than as
//! a cliff, and `rate`/`increase` mean what they say. These were gauges while
//! Kartero delivered gauges only; it takes sums from 0.4.0.

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
    machine: &MachineSnapshot,
    service_version: &str,
    scenario: Option<&str>,
    phase: Option<&str>,
) -> Result<()> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating telemetry dir {}", dir.display()))?;
    let body = serialize_metrics_with(
        snap,
        machine,
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

#[cfg(test)]
pub(crate) fn serialize_metrics(
    snap: &OtelSnapshot,
    service_name: &str,
    service_version: &str,
    time_unix_nano: &str,
    scenario: Option<&str>,
    phase: Option<&str>,
) -> Value {
    serialize_metrics_with(
        snap,
        &MachineSnapshot::default(),
        service_name,
        service_version,
        time_unix_nano,
        scenario,
        phase,
    )
}

/// The daemon's counters plus the machine's shared-cache figures, in one
/// payload so a dashboard can put GC outcomes and index growth next to the
/// traffic that caused them.
pub(crate) fn serialize_metrics_with(
    snap: &OtelSnapshot,
    machine: &MachineSnapshot,
    service_name: &str,
    service_version: &str,
    time_unix_nano: &str,
    scenario: Option<&str>,
    phase: Option<&str>,
) -> Value {
    let mut metrics = metrics_for(snap, time_unix_nano);
    metrics.extend(machine_metrics(machine, time_unix_nano));
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
    // Several runner slots share one machine's cache; `service.instance.id` is
    // the OTel key for which instance this is, and one the collector admits.
    if !machine.host.is_empty() {
        resource.push(str_attr("service.instance.id", &machine.host));
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
                "metrics": metrics,
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

    metrics.push(cum_sum(
        "kache.cache.uploads",
        "{upload}",
        vec![
            as_sum_int(snap.uploads_completed, now, &result_attr("completed")),
            as_sum_int(snap.uploads_failed, now, &result_attr("failed")),
            as_sum_int(snap.uploads_skipped, now, &result_attr("skipped")),
            as_sum_int(snap.uploads_suppressed, now, &result_attr("suppressed")),
        ],
    ));
    metrics.push(cum_sum(
        "kache.cache.downloads",
        "{download}",
        vec![
            as_sum_int(snap.downloads_completed, now, &result_attr("completed")),
            as_sum_int(snap.downloads_failed, now, &result_attr("failed")),
            as_sum_int(snap.downloads_suppressed, now, &result_attr("suppressed")),
        ],
    ));
    metrics.push(cum_sum(
        "kache.cache.bytes",
        "By",
        vec![
            as_sum_int(
                snap.bytes_uploaded,
                now,
                &[str_attr("kache.cache.direction", "upload")],
            ),
            as_sum_int(
                snap.bytes_downloaded,
                now,
                &[str_attr("kache.cache.direction", "download")],
            ),
        ],
    ));
    metrics.push(cum_sum(
        "kache.cache.remote_checks",
        "{check}",
        vec![as_sum_int(snap.remote_check_roundtrips, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.cache.negative_hits",
        "{hit}",
        vec![as_sum_int(snap.negative_hits, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.downloads",
        "{download}",
        vec![as_sum_int(snap.prefetch_downloads, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.bytes",
        "By",
        vec![as_sum_int(snap.prefetch_bytes, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.keys_used",
        "{key}",
        vec![as_sum_int(snap.prefetch_keys_used, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.keys_cancelled",
        "{key}",
        vec![as_sum_int(snap.prefetch_keys_cancelled, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.keys_over_budget",
        "{key}",
        vec![as_sum_int(snap.prefetch_keys_over_budget, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.plans",
        "{plan}",
        vec![
            as_sum_int(
                snap.prefetch_plans_advisory,
                now,
                &[str_attr("kache.prefetch.kind", "advisory")],
            ),
            as_sum_int(
                snap.prefetch_plans_fallback,
                now,
                &[str_attr("kache.prefetch.kind", "fallback")],
            ),
        ],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.list.requests",
        "{request}",
        vec![as_sum_int(snap.prefetch_list_requests, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.list.failures",
        "{request}",
        vec![as_sum_int(snap.prefetch_list_failures, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.pack.requests",
        "{request}",
        vec![as_sum_int(snap.prefetch_pack_requests, now, &[])],
    ));
    metrics.push(cum_sum(
        "kache.prefetch.v3.requests",
        "{request}",
        vec![as_sum_int(snap.prefetch_v3_requests, now, &[])],
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

/// Fixed for the life of the process, which is exactly what a cumulative
/// sum's start time has to be: every point in this process shares one window.
fn process_start_unix_nano() -> &'static str {
    use std::sync::OnceLock;
    static START: OnceLock<String> = OnceLock::new();
    START.get_or_init(unix_nano_now).as_str()
}

fn as_sum_int(value: u64, time_unix_nano: &str, attributes: &[Value]) -> Value {
    json!({
        "asInt": value.to_string(),
        "timeUnixNano": time_unix_nano,
        "startTimeUnixNano": process_start_unix_nano(),
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

fn cum_sum(name: &str, unit: &str, data_points: Vec<Value>) -> Value {
    json!({
        "name": name,
        "unit": unit,
        "sum": {
            "aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE",
            "isMonotonic": true,
            "dataPoints": data_points
        }
    })
}

// ── Machine snapshot ────────────────────────────────────────────────────────

/// The host's shared cache, written beside the daemon's counters: the index
/// every build on the machine reads and writes (its size and the rows in each
/// table) and the GC runs recorded in `gc_stats.json`. Every figure is
/// optional, so a busy or missing index costs a gap in the series, never a
/// wait.
#[derive(Debug, Clone, Default)]
pub(crate) struct MachineSnapshot {
    /// Which machine these figures describe, as `service.instance.id`.
    pub host: String,
    /// `index.db` plus its `-wal`, in bytes.
    pub index_bytes: Option<u64>,
    /// Rowid high-water mark per index table: the largest rowid, not a row
    /// count. Deletions leave it where it was, so it overstates a table that
    /// shrank; in exchange it is one b-tree descent, where `COUNT(*)` reads the
    /// table or its smallest index.
    pub table_rows: Vec<(&'static str, u64)>,
    pub gc: Option<crate::report::GcStatsPersisted>,
}

fn machine_metrics(snap: &MachineSnapshot, now: &str) -> Vec<Value> {
    let mut metrics = Vec::new();
    if let Some(bytes) = snap.index_bytes {
        metrics.push(gauge(
            "kache.cache.index.size",
            "By",
            vec![as_int(bytes, now, &[])],
        ));
    }
    if !snap.table_rows.is_empty() {
        metrics.push(gauge(
            "kache.cache.index.rows",
            "{row}",
            snap.table_rows
                .iter()
                .map(|(table, rows)| as_int(*rows, now, &[str_attr("kache.cache.table", table)]))
                .collect(),
        ));
    }
    if let Some(gc) = &snap.gc {
        metrics.extend(gc_metrics(gc, now));
    }
    metrics
}

fn gc_metrics(gc: &crate::report::GcStatsPersisted, now: &str) -> Vec<Value> {
    let mut metrics = Vec::new();
    if let Ok(last_run) = chrono::DateTime::parse_from_rfc3339(&gc.last_run) {
        metrics.push(gauge(
            "kache.cache.gc.last_run.time",
            "s",
            vec![as_int(last_run.timestamp().max(0) as u64, now, &[])],
        ));
    }
    for (name, unit, value) in [
        (
            "kache.cache.gc.last_run.entries_evicted",
            "{entry}",
            gc.entries_evicted as u64,
        ),
        ("kache.cache.gc.last_run.bytes_freed", "By", gc.bytes_freed),
        (
            "kache.cache.gc.last_run.entries_failed",
            "{entry}",
            gc.entries_failed as u64,
        ),
        (
            "kache.cache.gc.last_run.entries_locked",
            "{entry}",
            gc.entries_locked as u64,
        ),
        ("kache.cache.gc.last_run.duration", "ms", gc.duration_ms),
    ] {
        metrics.push(gauge(name, unit, vec![as_int(value, now, &[])]));
    }

    // The totals accumulate across runs and drivers since `since`, which is
    // exactly what a cumulative sum's start time has to be.
    let totals = &gc.totals;
    let start = chrono::DateTime::parse_from_rfc3339(&totals.since)
        .ok()
        .and_then(|since| since.timestamp_nanos_opt());
    if let (true, Some(start)) = (totals.runs > 0, start) {
        let start = start.max(0).to_string();
        for (name, unit, value) in [
            ("kache.cache.gc.runs", "{run}", totals.runs),
            (
                "kache.cache.gc.entries_evicted",
                "{entry}",
                totals.entries_evicted,
            ),
            ("kache.cache.gc.bytes_freed", "By", totals.bytes_freed),
            (
                "kache.cache.gc.entries_failed",
                "{entry}",
                totals.entries_failed,
            ),
            (
                "kache.cache.gc.entries_locked",
                "{entry}",
                totals.entries_locked,
            ),
        ] {
            metrics.push(cum_sum(
                name,
                unit,
                vec![json!({
                    "asInt": value.to_string(),
                    "timeUnixNano": now,
                    "startTimeUnixNano": start,
                    "attributes": [],
                })],
            ));
        }
    }
    metrics
}

/// This machine's host name, or empty when the OS will not say.
#[cfg(unix)]
pub(crate) fn host_name() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: `buf` is valid for `buf.len()` bytes, and gethostname writes at
    // most that many.
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if rc != 0 {
        return String::new();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

#[cfg(not(unix))]
pub(crate) fn host_name() -> String {
    std::env::var("COMPUTERNAME").unwrap_or_default()
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
            let points = m["gauge"]["dataPoints"]
                .as_array()
                .or_else(|| m["sum"]["dataPoints"].as_array())
                .unwrap_or_else(|| panic!("metric {} carries no data points", m["name"]));
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
    }

    fn machine_snap() -> MachineSnapshot {
        MachineSnapshot {
            host: "ci-mini".to_string(),
            index_bytes: Some(29_074_419_712),
            table_rows: vec![("entries", 2_085_333), ("cc_preprocess_memos", 874_517)],
            gc: Some(crate::report::GcStatsPersisted {
                last_run: "2026-09-12T12:11:05+00:00".to_string(),
                entries_evicted: 560,
                bytes_freed: 8_373_732_071,
                entries_failed: 3,
                entries_locked: 2,
                duration_ms: 5801,
                totals: crate::report::GcTotals {
                    since: "2026-09-01T00:00:00+00:00".to_string(),
                    runs: 4,
                    entries_evicted: 900,
                    bytes_freed: 10,
                    entries_failed: 70,
                    entries_locked: 60,
                },
                ..Default::default()
            }),
        }
    }

    fn metric_names(body: &Value) -> BTreeSet<String> {
        body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap().to_string())
            .collect()
    }

    fn with_machine(machine: &MachineSnapshot) -> Value {
        serialize_metrics_with(
            &sample_snap(),
            machine,
            "kache",
            "0.19.0",
            "1789200000000000000",
            None,
            None,
        )
    }

    #[test]
    fn machine_figures_ride_with_the_daemon_counters() {
        let body = with_machine(&machine_snap());
        let names = metric_names(&body);
        assert!(
            names.contains("kache.cache.uploads"),
            "daemon counters stay"
        );
        assert!(names.contains("kache.cache.index.rows"), "{names:?}");
        let resource = body["resourceMetrics"][0]["resource"]["attributes"]
            .as_array()
            .unwrap();
        assert!(
            resource.iter().any(
                |a| a["key"] == "service.instance.id" && a["value"]["stringValue"] == "ci-mini"
            )
        );

        let rows = metric(&body, "kache.cache.index.rows");
        let points = rows["gauge"]["dataPoints"].as_array().unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(
            points[1]["attributes"][0]["value"]["stringValue"],
            "cc_preprocess_memos"
        );
        assert_eq!(points[1]["asInt"], "874517");
        assert_eq!(
            metric(&body, "kache.cache.index.size")["gauge"]["dataPoints"][0]["asInt"],
            "29074419712"
        );
        // Kartero drops attribute keys outside its allowlist; these are the
        // families it admits.
        for key in all_attr_keys(&body) {
            assert!(
                [
                    "kache.cache.",
                    "kache.prefetch.",
                    "kache.telemetry.",
                    "service."
                ]
                .iter()
                .any(|family| key.starts_with(family)),
                "{key} is not allowlisted"
            );
        }
    }

    #[test]
    fn gc_totals_are_cumulative_sums_from_their_first_run() {
        let body = with_machine(&machine_snap());
        let runs = metric(&body, "kache.cache.gc.runs");
        assert_eq!(
            runs["sum"]["aggregationTemporality"],
            "AGGREGATION_TEMPORALITY_CUMULATIVE"
        );
        let point = &runs["sum"]["dataPoints"][0];
        assert_eq!(point["asInt"], "4");
        let since = chrono::DateTime::parse_from_rfc3339("2026-09-01T00:00:00+00:00")
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        assert_eq!(point["startTimeUnixNano"], since.to_string());
        assert_eq!(
            metric(&body, "kache.cache.gc.entries_locked")["sum"]["dataPoints"][0]["asInt"],
            "60"
        );
        assert_eq!(
            metric(&body, "kache.cache.gc.last_run.entries_locked")["gauge"]["dataPoints"][0]["asInt"],
            "2"
        );
    }

    #[test]
    fn gc_stats_without_totals_emit_no_cumulative_sums() {
        let mut snap = machine_snap();
        snap.gc.as_mut().unwrap().totals = crate::report::GcTotals::default();
        let names = metric_names(&with_machine(&snap));
        assert!(names.contains("kache.cache.gc.last_run.entries_evicted"));
        assert!(!names.contains("kache.cache.gc.runs"), "{names:?}");
    }

    /// A snapshot with nothing readable (no index yet, no GC record) must leave
    /// the daemon payload exactly as it was.
    #[test]
    fn an_empty_machine_snapshot_adds_nothing() {
        let plain = serialize_metrics(&sample_snap(), "kache", "0.19.0", "1", None, None);
        let with_empty = serialize_metrics_with(
            &sample_snap(),
            &MachineSnapshot::default(),
            "kache",
            "0.19.0",
            "1",
            None,
            None,
        );
        assert_eq!(plain, with_empty);
        assert!(
            !metric_names(&plain)
                .iter()
                .any(|name| name.starts_with("kache.cache.index.")
                    || name.starts_with("kache.cache.gc."))
        );
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
    fn counters_are_cumulative_sums() {
        let body = serialize_metrics(
            &sample_snap(),
            "kache",
            "0.16.0",
            "1700000000000000000",
            None,
            None,
        );
        let uploads = metric(&body, "kache.cache.uploads");
        assert!(uploads.get("gauge").is_none());
        assert_eq!(
            uploads["sum"]["aggregationTemporality"],
            "AGGREGATION_TEMPORALITY_CUMULATIVE"
        );
        assert_eq!(uploads["sum"]["isMonotonic"], true);
        let point = &uploads["sum"]["dataPoints"][0];
        assert_eq!(point["asInt"], "10");
        assert_eq!(point["attributes"][0]["value"]["stringValue"], "completed");
        // Without a usable start time a cumulative point has no window, and a
        // restart is indistinguishable from a real drop. Assert it is a
        // parseable nanosecond count rather than merely a string: an empty or
        // non-numeric one satisfies "is a string" and describes nothing.
        let start = point["startTimeUnixNano"]
            .as_str()
            .expect("cumulative points carry a start time");
        let start: u64 = start
            .parse()
            .unwrap_or_else(|_| panic!("start time must be decimal nanoseconds, got {start:?}"));
        assert!(start > 0, "start time must be a real instant");

        // Every point in one process shares one window, so a reader can
        // compare them without checking each start individually.
        let starts: BTreeSet<&str> = uploads["sum"]["dataPoints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["startTimeUnixNano"].as_str().unwrap())
            .collect();
        assert_eq!(starts.len(), 1, "all points must share one start time");
    }

    /// Numbers read at an instant must not become counters: summing two
    /// readings of a store size produces something that means nothing.
    #[test]
    fn point_in_time_readings_stay_gauges() {
        let body = serialize_metrics(
            &sample_snap(),
            "kache",
            "0.16.0",
            "1700000000000000000",
            None,
            None,
        );
        for name in [
            "kache.cache.store.size",
            "kache.cache.store.entries",
            "kache.cache.store.max",
            "kache.cache.uploads.pending",
            "kache.cache.downloads.active",
            "kache.cache.s3.concurrency",
            "kache.cache.remote.degraded",
            "kache.cache.negative_entries",
            "kache.prefetch.cancelled",
            "kache.prefetch.last_plan.candidates",
            "kache.prefetch.last_plan.wall",
        ] {
            assert!(
                metric(&body, name).get("sum").is_none(),
                "{name} must stay a gauge"
            );
        }
    }

    #[test]
    fn write_otlp_emits_kartero_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        write_otlp(
            dir.path(),
            &sample_snap(),
            &MachineSnapshot::default(),
            "0.16.0",
            None,
            None,
        )
        .unwrap();
        let metrics = dir.path().join(METRICS_FILE);
        let version = dir.path().join(SCHEMA_VERSION_FILE);
        assert!(metrics.is_file());
        assert_eq!(std::fs::read_to_string(version).unwrap().trim(), "1");
        let body: Value = serde_json::from_str(&std::fs::read_to_string(metrics).unwrap()).unwrap();
        assert_eq!(
            body["resourceMetrics"][0]["scopeMetrics"][0]["scope"]["name"],
            "kache.cache"
        );
        let ts = body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["gauge"]["dataPoints"]
            [0]["timeUnixNano"]
            .as_str()
            .expect("timeUnixNano is a string");
        assert!(
            ts.parse::<u128>().expect("unix nano") > 0,
            "dump timestamp must be a positive integer, got {ts:?}"
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
