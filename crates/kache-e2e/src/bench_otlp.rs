//! OTLP JSON gauges for one completed bench run.
//!
//! Built with `serde_json` next to the human-readable report. There is no
//! OpenTelemetry SDK: the file is already the body of an OTLP/HTTP
//! `ExportMetricsServiceRequest`, so a later collector can POST it as-is
//! after adding the CI envelope.
//!
//! Mapping is the §10 table in kartero's 2026-08-27 artifact-transport spec.
//! Gauges only. `run_id` is never emitted. `cicd.*` / `vcs.*` are not
//! asserted here.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Major schema version carried in the artifact name (`telemetry-otlp-v1`)
/// and as a resource attribute. A breaking change gets a new number and a
/// new artifact name.
pub const SCHEMA_VERSION: u32 = 1;

/// OTLP metrics request body, one signal, one request.
pub const METRICS_FILE: &str = "metrics.otlp.json";

/// Sidecar so a collector can reject an unknown major version without
/// parsing the request body.
pub const SCHEMA_VERSION_FILE: &str = "schema_version";

const SCOPE_NAME: &str = "kache.bench";

/// One completed bench, already reduced to the fields OTLP is allowed to
/// carry. Callers map `BenchResult` / `PullBenchResult` / `SccacheBenchResult`
/// into this; the writer does not know those types.
pub struct OtlpRun {
    pub project: String,
    pub git_ref: String,
    pub cache_tool: &'static str,
    pub time_unix_nano: String,
    pub verdict_ok: bool,
    pub speedup: Option<f64>,
    pub cache_size_bytes: u64,
    pub key_stability_pct: Option<f64>,
    pub disk_measured_bytes: Option<u64>,
    pub phases: Vec<OtlpPhase>,
}

pub struct OtlpPhase {
    pub name: &'static str,
    pub wall_s: u64,
    pub time_saved_s: Option<u64>,
    pub hits: u64,
    pub dups: Option<u64>,
    pub misses: u64,
    pub errors: Option<u64>,
    pub total: Option<u64>,
    pub hit_rate_pct: f64,
    pub weighted_hit_rate_pct: Option<f64>,
    pub leak_warnings: Option<u64>,
    pub objdir_bytes: u64,
}

impl OtlpRun {
    pub fn now_unix_nano() -> String {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        d.as_nanos().to_string()
    }

    /// `cache_size_mb` on the report is KiB/1024 rounded to 1 decimal — MiB,
    /// not SI MB. Convert through 1024² so the metric unit can be `By`.
    pub fn bytes_from_mib(mib: f64) -> u64 {
        (mib * 1024.0 * 1024.0).round() as u64
    }
}

pub fn write_otlp(work_dir: &Path, run: &OtlpRun) -> Result<()> {
    let body = serialize_metrics(run);
    let metrics_path = work_dir.join(METRICS_FILE);
    std::fs::write(
        &metrics_path,
        serde_json::to_string(&body).context("serializing OTLP metrics")? + "\n",
    )
    .with_context(|| format!("writing {}", metrics_path.display()))?;
    let version_path = work_dir.join(SCHEMA_VERSION_FILE);
    std::fs::write(&version_path, format!("{SCHEMA_VERSION}\n"))
        .with_context(|| format!("writing {}", version_path.display()))?;
    Ok(())
}

pub fn serialize_metrics(run: &OtlpRun) -> Value {
    json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [
                    str_attr("kache.telemetry.schema_version", &SCHEMA_VERSION.to_string()),
                    str_attr("kache.bench.git_ref", &run.git_ref),
                ]
            },
            "scopeMetrics": [{
                "scope": {
                    "name": SCOPE_NAME,
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "metrics": metrics_for(run),
            }]
        }]
    })
}

fn metrics_for(run: &OtlpRun) -> Vec<Value> {
    let mut metrics = Vec::new();

    let mut duration_points = Vec::new();
    let mut saved_points = Vec::new();
    let mut unit_points = Vec::new();
    let mut hit_rate_points = Vec::new();
    let mut weighted_points = Vec::new();
    let mut leak_points = Vec::new();
    let mut objdir_points = Vec::new();

    for phase in &run.phases {
        let phase_attrs = common_attrs(run, Some(phase.name));
        duration_points.push(as_double(
            phase.wall_s as f64,
            &run.time_unix_nano,
            &phase_attrs,
        ));
        if let Some(saved) = phase.time_saved_s {
            saved_points.push(as_double(saved as f64, &run.time_unix_nano, &phase_attrs));
        }
        push_unit_points(&mut unit_points, run, phase);
        hit_rate_points.push(as_double(
            phase.hit_rate_pct,
            &run.time_unix_nano,
            &phase_attrs,
        ));
        if let Some(weighted) = phase.weighted_hit_rate_pct {
            weighted_points.push(as_double(weighted, &run.time_unix_nano, &phase_attrs));
        }
        if let Some(leaks) = phase.leak_warnings {
            leak_points.push(as_int(leaks, &run.time_unix_nano, &phase_attrs));
        }
        objdir_points.push(as_int(
            phase.objdir_bytes,
            &run.time_unix_nano,
            &phase_attrs,
        ));
    }

    metrics.push(gauge("kache.bench.build.duration", "s", duration_points));
    if !saved_points.is_empty() {
        metrics.push(gauge("kache.bench.compile.time_saved", "s", saved_points));
    }
    if !unit_points.is_empty() {
        metrics.push(gauge("kache.bench.compile.units", "{unit}", unit_points));
    }
    metrics.push(gauge("kache.bench.cache.hit_rate", "%", hit_rate_points));
    if !weighted_points.is_empty() {
        metrics.push(gauge(
            "kache.bench.cache.weighted_hit_rate",
            "%",
            weighted_points,
        ));
    }
    if !leak_points.is_empty() {
        metrics.push(gauge("kache.bench.leak_warnings", "{warning}", leak_points));
    }

    let run_attrs = common_attrs(run, None);
    if let Some(speedup) = run.speedup {
        metrics.push(gauge(
            "kache.bench.speedup",
            "1",
            vec![as_double(speedup, &run.time_unix_nano, &run_attrs)],
        ));
    }
    metrics.push(gauge(
        "kache.bench.cache.size",
        "By",
        vec![as_int(
            run.cache_size_bytes,
            &run.time_unix_nano,
            &run_attrs,
        )],
    ));
    metrics.push(gauge("kache.bench.objdir.size", "By", objdir_points));
    if let Some(disk) = run.disk_measured_bytes {
        metrics.push(gauge(
            "kache.bench.disk.consumed",
            "By",
            vec![as_int(disk, &run.time_unix_nano, &run_attrs)],
        ));
    }
    if let Some(stable) = run.key_stability_pct {
        metrics.push(gauge(
            "kache.bench.key_stability",
            "%",
            vec![as_double(stable, &run.time_unix_nano, &run_attrs)],
        ));
    }
    metrics.push(gauge(
        "kache.bench.verdict.ok",
        "1",
        vec![as_int(
            u64::from(run.verdict_ok),
            &run.time_unix_nano,
            &run_attrs,
        )],
    ));
    metrics
}

fn push_unit_points(out: &mut Vec<Value>, run: &OtlpRun, phase: &OtlpPhase) {
    let push = |out: &mut Vec<Value>, result: &str, value: u64| {
        let attrs = unit_attrs(run, phase.name, result);
        out.push(as_int(value, &run.time_unix_nano, &attrs));
    };
    push(out, "hit", phase.hits);
    if let Some(dups) = phase.dups {
        push(out, "dup", dups);
    }
    push(out, "miss", phase.misses);
    if let Some(errors) = phase.errors {
        push(out, "error", errors);
    }
    if let Some(total) = phase.total {
        push(out, "total", total);
    }
}

fn common_attrs(run: &OtlpRun, phase: Option<&str>) -> Vec<Value> {
    let mut attrs = vec![
        str_attr("kache.bench.project", &run.project),
        str_attr("kache.bench.cache_tool", run.cache_tool),
    ];
    if let Some(phase) = phase {
        attrs.push(str_attr("kache.bench.phase", phase));
    }
    attrs
}

fn unit_attrs(run: &OtlpRun, phase: &str, result: &str) -> Vec<Value> {
    let mut attrs = common_attrs(run, Some(phase));
    attrs.push(str_attr("kache.bench.result", result));
    attrs
}

fn gauge(name: &str, unit: &str, data_points: Vec<Value>) -> Value {
    json!({
        "name": name,
        "unit": unit,
        "gauge": { "dataPoints": data_points }
    })
}

fn str_attr(key: &str, value: &str) -> Value {
    json!({"key": key, "value": {"stringValue": value}})
}

fn as_double(value: f64, time_unix_nano: &str, attributes: &[Value]) -> Value {
    json!({
        "asDouble": value,
        "timeUnixNano": time_unix_nano,
        "attributes": attributes,
    })
}

fn as_int(value: u64, time_unix_nano: &str, attributes: &[Value]) -> Value {
    json!({
        "asInt": value.to_string(),
        "timeUnixNano": time_unix_nano,
        "attributes": attributes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn kache_run() -> OtlpRun {
        OtlpRun {
            project: "bench-firefox".into(),
            git_ref: "FIREFOX_140_0_RELEASE".into(),
            cache_tool: "kache",
            time_unix_nano: "1700000000000000000".into(),
            verdict_ok: true,
            speedup: Some(4.2),
            cache_size_bytes: 12 * 1024 * 1024,
            key_stability_pct: Some(96.9),
            disk_measured_bytes: Some(3_000_000_000),
            phases: vec![
                OtlpPhase {
                    name: "cold",
                    wall_s: 400,
                    time_saved_s: Some(0),
                    hits: 0,
                    dups: Some(0),
                    misses: 500,
                    errors: Some(3),
                    total: Some(503),
                    hit_rate_pct: 0.0,
                    weighted_hit_rate_pct: Some(0.0),
                    leak_warnings: Some(0),
                    objdir_bytes: 8_000_000_000,
                },
                OtlpPhase {
                    name: "warm",
                    wall_s: 95,
                    time_saved_s: Some(280),
                    hits: 480,
                    dups: Some(10),
                    misses: 10,
                    errors: Some(0),
                    total: Some(500),
                    hit_rate_pct: 96.0,
                    weighted_hit_rate_pct: Some(97.5),
                    leak_warnings: Some(2),
                    objdir_bytes: 8_100_000_000,
                },
            ],
        }
    }

    fn names(body: &Value) -> Vec<&str> {
        body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["name"].as_str().unwrap())
            .collect()
    }

    fn metric<'a>(body: &'a Value, name: &str) -> &'a Value {
        body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["name"] == name)
            .unwrap_or_else(|| panic!("missing metric {name}"))
    }

    fn attr_map(point: &Value) -> std::collections::BTreeMap<String, String> {
        point["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| {
                (
                    a["key"].as_str().unwrap().to_string(),
                    a["value"]["stringValue"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    }

    fn all_attr_keys(body: &Value) -> std::collections::BTreeSet<String> {
        let mut keys = std::collections::BTreeSet::new();
        for attr in body["resourceMetrics"][0]["resource"]["attributes"]
            .as_array()
            .unwrap()
        {
            keys.insert(attr["key"].as_str().unwrap().to_string());
        }
        for metric in body["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .unwrap()
        {
            for point in metric["gauge"]["dataPoints"].as_array().unwrap() {
                for attr in point["attributes"].as_array().unwrap() {
                    keys.insert(attr["key"].as_str().unwrap().to_string());
                }
            }
        }
        keys
    }

    #[test]
    fn time_unix_nano_is_a_decimal_string() {
        let body = serialize_metrics(&kache_run());
        let point = &metric(&body, "kache.bench.speedup")["gauge"]["dataPoints"][0];
        assert!(point["timeUnixNano"].is_string());
        assert_eq!(point["timeUnixNano"], "1700000000000000000");
        assert!(point.get("asDouble").is_some());
    }

    #[test]
    fn current_time_unix_nano_is_nonzero_decimal() {
        let timestamp = OtlpRun::now_unix_nano();
        let nanos = timestamp.parse::<u128>().unwrap();
        assert!(nanos > 1_000_000_000_000_000_000);
    }

    #[test]
    fn integer_counts_use_as_int_string() {
        let body = serialize_metrics(&kache_run());
        let point = &metric(&body, "kache.bench.cache.size")["gauge"]["dataPoints"][0];
        assert_eq!(point["asInt"], "12582912");
        assert!(point.get("asDouble").is_none());
    }

    #[test]
    fn cache_size_is_bytes_not_megabytes() {
        assert_eq!(OtlpRun::bytes_from_mib(1.5), 1_572_864);
    }

    #[test]
    fn run_id_platform_and_cicd_are_absent() {
        let body = serialize_metrics(&kache_run());
        let dumped = body.to_string();
        assert!(!dumped.contains("run_id"));
        assert!(!dumped.contains("run-1"));
        assert!(!dumped.contains("cicd."));
        assert!(!dumped.contains("vcs."));
        assert!(!dumped.contains("macos"));
        let keys = all_attr_keys(&body);
        for forbidden in [
            "kache.bench.run_id",
            "kache.bench.platform",
            "cicd.pipeline.name",
            "vcs.repository.url.full",
        ] {
            assert!(!keys.contains(forbidden), "{forbidden} must not be emitted");
        }
    }

    #[test]
    fn attribute_set_is_the_allowlist() {
        let body = serialize_metrics(&kache_run());
        let keys = all_attr_keys(&body);
        let expected: std::collections::BTreeSet<_> = [
            "kache.telemetry.schema_version",
            "kache.bench.git_ref",
            "kache.bench.project",
            "kache.bench.cache_tool",
            "kache.bench.phase",
            "kache.bench.result",
        ]
        .into_iter()
        .map(str::to_string)
        .collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn compile_units_carry_result_and_phase() {
        let body = serialize_metrics(&kache_run());
        let points = metric(&body, "kache.bench.compile.units")["gauge"]["dataPoints"]
            .as_array()
            .unwrap();
        let results: Vec<_> = points
            .iter()
            .map(|p| {
                let attrs = attr_map(p);
                (
                    attrs["kache.bench.phase"].clone(),
                    attrs["kache.bench.result"].clone(),
                    p["asInt"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert!(results.contains(&("warm".into(), "hit".into(), "480".into())));
        assert!(results.contains(&("warm".into(), "dup".into(), "10".into())));
        assert!(results.contains(&("cold".into(), "error".into(), "3".into())));
        assert!(results.contains(&("warm".into(), "total".into(), "500".into())));
    }

    #[test]
    fn pull_phase_is_a_dimension_not_a_new_metric() {
        let mut run = kache_run();
        run.speedup = None;
        run.key_stability_pct = None;
        run.phases = vec![OtlpPhase {
            name: "pull",
            wall_s: 120,
            time_saved_s: Some(40),
            hits: 100,
            dups: Some(0),
            misses: 20,
            errors: Some(0),
            total: Some(120),
            hit_rate_pct: 83.3,
            weighted_hit_rate_pct: Some(80.0),
            leak_warnings: Some(0),
            objdir_bytes: 1,
        }];
        let body = serialize_metrics(&run);
        let duration = &metric(&body, "kache.bench.build.duration")["gauge"]["dataPoints"][0];
        assert_eq!(attr_map(duration)["kache.bench.phase"], "pull");
        assert!(!names(&body).contains(&"kache.bench.speedup"));
        assert!(!names(&body).contains(&"kache.bench.key_stability"));
    }

    #[test]
    fn sccache_omits_kache_only_gauges() {
        let run = OtlpRun {
            project: "bench-firefox-sccache".into(),
            git_ref: "FIREFOX_140_0_RELEASE".into(),
            cache_tool: "sccache",
            time_unix_nano: "1".into(),
            verdict_ok: true,
            speedup: Some(1.1),
            cache_size_bytes: 100,
            key_stability_pct: None,
            disk_measured_bytes: None,
            phases: vec![OtlpPhase {
                name: "warm",
                wall_s: 10,
                time_saved_s: None,
                hits: 4,
                dups: None,
                misses: 1,
                errors: None,
                total: Some(5),
                hit_rate_pct: 80.0,
                weighted_hit_rate_pct: None,
                leak_warnings: None,
                objdir_bytes: 9,
            }],
        };
        let body = serialize_metrics(&run);
        let listed = names(&body);
        assert!(!listed.contains(&"kache.bench.compile.time_saved"));
        assert!(!listed.contains(&"kache.bench.cache.weighted_hit_rate"));
        assert!(!listed.contains(&"kache.bench.leak_warnings"));
        assert!(!listed.contains(&"kache.bench.key_stability"));
        assert!(!listed.contains(&"kache.bench.disk.consumed"));
        let units = metric(&body, "kache.bench.compile.units")["gauge"]["dataPoints"]
            .as_array()
            .unwrap();
        let results: Vec<_> = units
            .iter()
            .map(|p| attr_map(p)["kache.bench.result"].clone())
            .collect();
        assert_eq!(results, vec!["hit", "miss", "total"]);
    }

    #[test]
    fn degraded_run_still_emits_with_verdict_zero() {
        let mut run = kache_run();
        run.verdict_ok = false;
        let body = serialize_metrics(&run);
        let point = &metric(&body, "kache.bench.verdict.ok")["gauge"]["dataPoints"][0];
        assert_eq!(point["asInt"], "0");
        assert!(names(&body).contains(&"kache.bench.cache.hit_rate"));
    }

    #[test]
    fn write_otlp_replaces_seeded_failure_artifact() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(METRICS_FILE), "seeded failure\n").unwrap();
        std::fs::write(dir.path().join(SCHEMA_VERSION_FILE), "1\n").unwrap();
        write_otlp(dir.path(), &kache_run()).unwrap();
        let body: Value =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join(METRICS_FILE)).unwrap())
                .unwrap();
        assert_eq!(
            body["resourceMetrics"][0]["resource"]["attributes"][0]["value"]["stringValue"],
            "1"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join(SCHEMA_VERSION_FILE)).unwrap(),
            "1\n"
        );
    }
}
