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
    // No host attribute: the file's shipper adds host identity, and a new
    // resource attribute would split the existing daemon counter series.
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
/// every build on the machine reads and writes (its size and each table's largest
/// rowid) and the GC runs recorded in `gc_stats.json`. Every figure is
/// optional, so a busy or missing index costs a gap in the series, never a
/// wait.
#[derive(Debug, Clone, Default)]
pub(crate) struct MachineSnapshot {
    /// Registered blob bytes, the physical size GC compares with max_size.
    pub store_physical_bytes: Option<u64>,
    /// `index.db` plus its `-wal`, in bytes.
    pub index_bytes: Option<u64>,
    /// The `-wal` file alone, also inside `index_bytes`.
    pub wal_bytes: Option<u64>,
    /// Bytes `index.db` holds in free pages, which compaction returns to the
    /// disk. Also inside `index_bytes`.
    pub index_free_bytes: Option<u64>,
    /// Disagreement between `blobs` and `entry_blobs`. Unowned bytes are
    /// inside `store_physical_bytes`, and no eviction can free them until the
    /// daemon heals the blob index.
    pub blob_drift: Option<crate::store::BlobRefcountDrift>,
    /// The largest rowid per index table. This counts writes, not rows: the
    /// tables written with `INSERT OR REPLACE` give a replaced row the next
    /// rowid, so it grows with every insert and every replacement, and a
    /// delete never lowers it. Its one merit is cost, a single b-tree descent
    /// where `COUNT(*)` reads the whole table. Index size is the figure to
    /// trust for how big the index is.
    pub rowid_high_water: Vec<(&'static str, u64)>,
    pub gc: Option<crate::report::GcStatsPersisted>,
}

fn machine_metrics(snap: &MachineSnapshot, now: &str) -> Vec<Value> {
    let mut metrics = Vec::new();
    if let Some(bytes) = snap.store_physical_bytes {
        metrics.push(gauge(
            "kache.cache.store.physical_size",
            "By",
            vec![as_int(bytes, now, &[])],
        ));
    }
    if let Some(drift) = &snap.blob_drift {
        metrics.push(gauge(
            "kache.cache.store.unowned.size",
            "By",
            vec![as_int(drift.unowned_bytes, now, &[])],
        ));
        metrics.push(gauge(
            "kache.cache.store.refcount_drift",
            "{blob}",
            vec![as_int(drift.mismatched(), now, &[])],
        ));
    }
    if let Some(bytes) = snap.index_bytes {
        metrics.push(gauge(
            "kache.cache.index.size",
            "By",
            vec![as_int(bytes, now, &[])],
        ));
    }
    if let Some(bytes) = snap.index_free_bytes {
        metrics.push(gauge(
            "kache.cache.index.free.size",
            "By",
            vec![as_int(bytes, now, &[])],
        ));
    }
    if let Some(bytes) = snap.wal_bytes {
        metrics.push(gauge(
            "kache.cache.index.wal.size",
            "By",
            vec![as_int(bytes, now, &[])],
        ));
    }
    if !snap.rowid_high_water.is_empty() {
        metrics.push(gauge(
            "kache.cache.index.rowid_high_water",
            "1",
            snap.rowid_high_water
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
            "kache.cache.gc.last_run.entries_pinned",
            "{entry}",
            gc.entries_pinned as u64,
        ),
        (
            "kache.cache.gc.last_run.entries_unreclaimable",
            "{entry}",
            gc.entries_unreclaimable as u64,
        ),
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
        (
            "kache.cache.gc.last_run.entries_busy_snapshot",
            "{entry}",
            gc.entries_busy_snapshot as u64,
        ),
        (
            "kache.cache.gc.last_run.entries_recent_prefiltered",
            "{entry}",
            gc.entries_recent_prefiltered as u64,
        ),
        ("kache.cache.gc.last_run.duration", "ms", gc.duration_ms),
        (
            "kache.cache.gc.last_run.evict_write",
            "ms",
            gc.evict_write_ms,
        ),
    ] {
        metrics.push(gauge(name, unit, vec![as_int(value, now, &[])]));
    }
    // Reported only by a run that did the housekeeping, so an eviction after
    // an upload does not read as "no lock files left".
    for (name, unit, value) in [
        (
            "kache.cache.gc.last_run.key_locks_removed",
            "{file}",
            gc.key_locks_removed,
        ),
        (
            "kache.cache.gc.last_run.key_locks_remaining",
            "{file}",
            gc.key_locks_remaining,
        ),
        (
            "kache.cache.gc.last_run.predictions_pruned",
            "{row}",
            gc.predictions_pruned,
        ),
        (
            "kache.cache.gc.last_run.file_hashes_pruned",
            "{row}",
            gc.file_hashes_pruned,
        ),
    ] {
        if let Some(value) = value {
            metrics.push(gauge(name, unit, vec![as_int(value as u64, now, &[])]));
        }
    }

    metrics
}

/// Fill `loads` with the 1, 5 and 15 minute load averages and return how
/// many the OS wrote, or zero where it keeps none. Only the syscall, so what
/// counts as a sample is decided in [`one_minute_load`].
pub(crate) fn sample_load_averages(loads: &mut [f64; 3]) -> i32 {
    #[cfg(unix)]
    {
        // SAFETY: `loads` holds the three samples getloadavg may write.
        unsafe { libc::getloadavg(loads.as_mut_ptr(), 3) }
    }
    #[cfg(not(unix))]
    {
        let _ = loads;
        i32::default()
    }
}

/// The one-minute load from a [`sample_load_averages`] result, kept only when
/// the OS wrote it and it is a real, non-negative number.
pub(crate) fn one_minute_load(written: i32, one_minute: f64) -> Option<f64> {
    (written >= 1 && one_minute.is_finite() && one_minute >= 0.0).then_some(one_minute)
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

    #[test]
    fn one_minute_load_keeps_only_a_real_sample() {
        assert_eq!(one_minute_load(1, 2.5), Some(2.5));
        assert_eq!(one_minute_load(3, 1.5), Some(1.5));
        assert_eq!(one_minute_load(1, 0.0), Some(0.0), "an idle host");
        assert_eq!(one_minute_load(0, 2.5), None, "nothing written");
        assert_eq!(one_minute_load(-1, 2.5), None, "no load average here");
        assert_eq!(one_minute_load(1, -0.5), None);
        assert_eq!(one_minute_load(1, f64::NAN), None);
        assert_eq!(one_minute_load(1, f64::INFINITY), None);
    }

    #[cfg(unix)]
    #[test]
    fn the_os_writes_all_three_load_samples() {
        let mut loads = [-1.0; 3];
        assert_eq!(sample_load_averages(&mut loads), 3);
        assert!(
            loads.iter().all(|load| load.is_finite() && *load >= 0.0),
            "{loads:?}"
        );
    }

    fn machine_snap() -> MachineSnapshot {
        MachineSnapshot {
            store_physical_bytes: Some(107_000_000_000),
            index_bytes: Some(29_074_419_712),
            wal_bytes: Some(1_073_741_824),
            index_free_bytes: Some(27_917_287_424),
            blob_drift: Some(crate::store::BlobRefcountDrift {
                unowned: 1_430,
                unowned_bytes: 28_991_029_248,
                too_high: 571,
                too_high_bytes: 9_000_000_000,
                ..Default::default()
            }),
            rowid_high_water: vec![("entries", 2_085_333), ("cc_preprocess_memos", 874_517)],
            gc: Some(crate::report::GcStatsPersisted {
                last_run: "2026-09-12T12:11:05+00:00".to_string(),
                entries_evicted: 560,
                bytes_freed: 8_373_732_071,
                entries_failed: 3,
                entries_locked: 2,
                entries_busy_snapshot: 1,
                entries_recent_prefiltered: 20,
                entries_pinned: 25,
                entries_unreclaimable: 42,
                duration_ms: 5801,
                evict_write_ms: 4200,
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
        assert!(
            names.contains("kache.cache.index.rowid_high_water"),
            "{names:?}"
        );
        let resource = body["resourceMetrics"][0]["resource"]["attributes"]
            .as_array()
            .unwrap();
        assert!(
            !resource
                .iter()
                .any(|a| a["key"] == "service.instance.id" || a["key"] == "host.name"),
            "no host identity in the payload: {resource:?}"
        );

        let rows = metric(&body, "kache.cache.index.rowid_high_water");
        assert_eq!(rows["unit"], "1", "a write counter, not rows");
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
        assert_eq!(
            metric(&body, "kache.cache.store.physical_size")["gauge"]["dataPoints"][0]["asInt"],
            "107000000000"
        );
        let free = metric(&body, "kache.cache.index.free.size");
        assert_eq!(free["unit"], "By");
        assert_eq!(free["gauge"]["dataPoints"][0]["asInt"], "27917287424");
        let wal = metric(&body, "kache.cache.index.wal.size");
        assert_eq!(wal["unit"], "By");
        assert_eq!(wal["gauge"]["dataPoints"][0]["asInt"], "1073741824");
        let unowned = metric(&body, "kache.cache.store.unowned.size");
        assert_eq!(unowned["unit"], "By");
        assert_eq!(unowned["gauge"]["dataPoints"][0]["asInt"], "28991029248");
        let drift = metric(&body, "kache.cache.store.refcount_drift");
        assert_eq!(drift["unit"], "{blob}");
        assert_eq!(drift["gauge"]["dataPoints"][0]["asInt"], "2001");
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

    /// GC figures describe the last run only, as gauges. Adding runs up is
    /// the backend's job, over the lines in `gc-runs.jsonl`.
    #[test]
    fn gc_figures_are_last_run_gauges() {
        let body = with_machine(&machine_snap());
        let got: Vec<(String, String)> = body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["name"].as_str().unwrap().starts_with("kache.cache.gc."))
            .map(|m| {
                assert!(m.get("sum").is_none(), "no cumulative sums: {m}");
                (
                    m["name"].as_str().unwrap().to_string(),
                    m["gauge"]["dataPoints"][0]["asInt"]
                        .as_str()
                        .unwrap()
                        .to_string(),
                )
            })
            .collect();
        let last_run = chrono::DateTime::parse_from_rfc3339("2026-09-12T12:11:05+00:00")
            .unwrap()
            .timestamp()
            .to_string();
        let want: Vec<(String, String)> = [
            ("kache.cache.gc.last_run.time", last_run.as_str()),
            ("kache.cache.gc.last_run.entries_evicted", "560"),
            ("kache.cache.gc.last_run.bytes_freed", "8373732071"),
            ("kache.cache.gc.last_run.entries_pinned", "25"),
            ("kache.cache.gc.last_run.entries_unreclaimable", "42"),
            ("kache.cache.gc.last_run.entries_failed", "3"),
            ("kache.cache.gc.last_run.entries_locked", "2"),
            ("kache.cache.gc.last_run.entries_busy_snapshot", "1"),
            ("kache.cache.gc.last_run.entries_recent_prefiltered", "20"),
            ("kache.cache.gc.last_run.duration", "5801"),
            ("kache.cache.gc.last_run.evict_write", "4200"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        assert_eq!(got, want);
    }

    /// Housekeeping counts are gauges of the last run that did any; the
    /// snapshot above has none and `gc_figures_are_last_run_gauges` pins that
    /// it emits none.
    #[test]
    fn gc_housekeeping_counts_are_gauges_when_the_run_recorded_them() {
        let mut machine = machine_snap();
        let gc = machine.gc.as_mut().unwrap();
        gc.key_locks_removed = Some(20_000);
        gc.key_locks_remaining = Some(64_496);
        gc.predictions_pruned = Some(0);
        gc.file_hashes_pruned = Some(1_234);
        let body = with_machine(&machine);
        let got: Vec<(String, String, String)> =
            body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|m| {
                    let name = m["name"].as_str().unwrap();
                    name.contains("key_locks") || name.contains("_pruned")
                })
                .map(|m| {
                    (
                        m["name"].as_str().unwrap().to_string(),
                        m["unit"].as_str().unwrap().to_string(),
                        m["gauge"]["dataPoints"][0]["asInt"]
                            .as_str()
                            .unwrap()
                            .to_string(),
                    )
                })
                .collect();
        let want: Vec<(String, String, String)> = [
            (
                "kache.cache.gc.last_run.key_locks_removed",
                "{file}",
                "20000",
            ),
            (
                "kache.cache.gc.last_run.key_locks_remaining",
                "{file}",
                "64496",
            ),
            ("kache.cache.gc.last_run.predictions_pruned", "{row}", "0"),
            (
                "kache.cache.gc.last_run.file_hashes_pruned",
                "{row}",
                "1234",
            ),
        ]
        .into_iter()
        .map(|(name, unit, value)| (name.to_string(), unit.to_string(), value.to_string()))
        .collect();
        assert_eq!(got, want);
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
