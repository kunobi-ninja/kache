use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::cli::format_duration_ms;
use crate::config::Config;
use crate::daemon::{TransferDirection, TransferEvent};
use crate::events::{self, BuildEvent, EventResult};

// ── Data Model ──────────────────────────────────────────────────────────────

/// Persisted GC stats written by the daemon to gc_stats.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcStatsPersisted {
    pub last_run: String,
    pub entries_evicted: usize,
    pub bytes_freed: u64,
    pub blobs_removed: usize,
    pub duration_ms: u64,
}

/// GC summary included in build reports when GC ran recently.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcSummary {
    pub last_run: String,
    pub entries_evicted: usize,
    pub bytes_freed: u64,
    pub blobs_removed: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BuildReport {
    pub meta: ReportMeta,
    pub summary: ReportSummary,
    #[serde(default)]
    pub timeline: ReportTimeline,
    pub timing: TimingBreakdown,
    pub storage: StorageBreakdown,
    pub network: Option<NetworkAnalysis>,
    pub prefetch: PrefetchAnalysis,
    pub top_misses: Vec<CrateDetail>,
    pub top_hits: Vec<CrateDetail>,
    pub all_events: Vec<CrateDetail>,
    #[serde(default, rename = "traceEvents", alias = "trace_events")]
    pub trace_events: Vec<TraceEvent>,
    #[serde(
        default = "default_trace_display_time_unit",
        rename = "displayTimeUnit"
    )]
    pub display_time_unit: String,
    #[serde(default)]
    pub bypass: BypassAnalysis,
    pub errors_detail: Vec<ErrorDetail>,
    pub suggestions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc: Option<GcSummary>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReportMeta {
    pub kache_version: String,
    pub generated_at: String,
    pub since_hours: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_filter: Option<String>,
}

fn default_trace_display_time_unit() -> String {
    "ms".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReportSummary {
    pub hit_rate_pct: f64,
    pub weighted_hit_rate_pct: Option<f64>,
    pub time_saved_ms: u64,
    pub total_crates: usize,
    pub local_hits: usize,
    pub prefetch_hits: usize,
    pub remote_hits: usize,
    #[serde(default)]
    pub dups: usize,
    pub misses: usize,
    pub errors: usize,
    #[serde(default)]
    pub passthroughs: usize,
    /// Query / probe invocations (`--print`, `-vV`, `cc -###`) that ran
    /// uncached. Counted separately from `passthroughs` because a probe is
    /// not a compilation — lumping it in inflates the "couldn't cache"
    /// signal (a clean build does many probes, zero real passthroughs).
    #[serde(default)]
    pub probes: usize,
    #[serde(default)]
    pub skipped: usize,
    #[serde(default)]
    pub fallbacks: usize,
    pub total_duration_ms: u64,
    /// Percentage of total compile time avoided by cache: time_saved / (time_saved + miss_compile_time).
    #[serde(default)]
    pub cache_efficiency_pct: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ReportTimeline {
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
    #[serde(default)]
    pub start_unix_ms: Option<i64>,
    #[serde(default)]
    pub end_unix_ms: Option<i64>,
    pub duration_ms: u64,
    pub event_count: usize,
    pub cacheable_count: usize,
    pub hit_count: usize,
    pub compiled_count: usize,
    pub passthrough_count: usize,
    /// Query / probe invocations, split out of `passthrough_count` (see
    /// [`ReportSummary::probes`]).
    #[serde(default)]
    pub probe_count: usize,
    pub skipped_count: usize,
    pub error_count: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimingBreakdown {
    pub hit_time_ms: u64,
    pub miss_time_ms: u64,
    pub avg_hit_ms: f64,
    pub avg_miss_ms: f64,
    pub avg_hit_overhead_ms: f64,
    pub miss_compile_time_ms: u64,
    #[serde(default)]
    pub avg_key_ms: f64,
    #[serde(default)]
    pub avg_lookup_ms: f64,
    #[serde(default)]
    pub avg_restore_ms: f64,
    #[serde(default)]
    pub avg_store_ms: f64,
    #[serde(default)]
    pub total_key_ms: u64,
    #[serde(default)]
    pub total_lookup_ms: u64,
    #[serde(default)]
    pub total_restore_ms: u64,
    #[serde(default)]
    pub total_store_ms: u64,
}

/// kache's storage efficiency — both halves of it.
///
/// **Restore side** — how cache-hit restores landed on disk: kache prefers
/// a CoW reflink (physically zero-copy *and* write-isolated), falling back
/// to a hardlink, then a full copy.
///
/// **Store side** — content-addressed dedup: an artifact whose content is
/// already in the store is referenced, not stored a second time. And how a
/// new blob entered the store: a CoW reflink (shares blocks with the build's
/// own output — not a second physical copy) or a full copy on a filesystem
/// without CoW.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageBreakdown {
    /// Bytes restored by CoW reflink — zero physical copy.
    pub reflinked_bytes: u64,
    /// Bytes restored by hardlink — zero physical copy, shared inode.
    pub hardlinked_bytes: u64,
    /// Bytes restored by a full physical copy.
    pub copied_bytes: u64,
    /// Total bytes restored from cache (reflinked + hardlinked + copied).
    pub restored_bytes: u64,
    /// Share of restored bytes that cost no physical copy (reflink + hardlink).
    pub zero_copy_pct: f64,
    /// Bytes ingested into the store by a CoW reflink (shares blocks with the
    /// build's output — the store is not a second physical copy of these).
    pub store_reflinked_bytes: u64,
    /// Bytes ingested into the store by a full physical copy (a real second
    /// copy — the fallback on a filesystem without CoW).
    pub store_copied_bytes: u64,
    /// Unique content-addressed blobs in the local store.
    pub store_blobs: u64,
    /// Sum of every cache entry's logical size — what the store would
    /// occupy with no dedup.
    pub logical_bytes: u64,
    /// Physical bytes the unique blobs occupy on disk.
    pub blob_bytes: u64,
    /// Bytes saved by content-addressed dedup (`logical_bytes - blob_bytes`).
    pub dedup_saved_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkAnalysis {
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub uploads_ok: usize,
    pub uploads_failed: usize,
    pub downloads_ok: usize,
    pub downloads_failed: usize,
    pub avg_download_ms: f64,
    pub p95_download_ms: u64,
    pub max_download_ms: u64,
    /// Throughput based on total wall-clock time (includes local restore work).
    pub throughput_mbps: f64,
    /// Throughput based on network time only (S3 GET + body collection).
    pub network_throughput_mbps: f64,
    /// Throughput based on response body time only.
    #[serde(default)]
    pub body_throughput_mbps: f64,
    /// Largest aggregate phase for successful downloads, derived from raw phase totals.
    #[serde(default)]
    pub dominant_download_phase: String,
    #[serde(default)]
    pub dominant_download_phase_ms: u64,
    #[serde(default)]
    pub dominant_download_phase_pct: f64,
    /// Time spent waiting for response headers across GET requests.
    #[serde(default)]
    pub total_request_ms: u64,
    /// Time spent reading response bodies across GET requests.
    #[serde(default)]
    pub total_body_ms: u64,
    /// Time spent waiting for S3 concurrency permits.
    #[serde(default)]
    pub total_semaphore_wait_ms: u64,
    /// Time spent on HEAD/existence checks before downloads.
    #[serde(default)]
    pub total_head_ms: u64,
    /// Total number of GET requests issued for successful downloads.
    #[serde(default)]
    pub total_get_requests: u32,
    /// Compression ratio (original / compressed). 0 if no data.
    pub compression_ratio: f64,
    /// Total original (uncompressed) bytes downloaded.
    pub original_bytes_down: u64,
    /// Total time spent in zstd decompression (ms).
    pub total_decompress_ms: u64,
    /// Total time spent extracting downloaded archives (ms).
    #[serde(default)]
    pub total_extract_ms: u64,
    /// Disk I/O time for downloads (directly measured when available), ms.
    pub total_disk_io_ms: u64,
    /// Total time spent importing downloaded entries into SQLite.
    #[serde(default)]
    pub total_import_ms: u64,
    /// Total upload compression time (ms).
    #[serde(default)]
    pub total_compression_ms: u64,
    /// Total upload HEAD check time (ms).
    #[serde(default)]
    pub total_head_checks_ms: u64,
    /// Number of v2 blobs that were already local (dedup savings).
    pub blobs_skipped: u32,
    /// Total v2 blobs across all downloads.
    pub blobs_total: u32,
    #[serde(default)]
    pub v1_downloads: usize,
    #[serde(default)]
    pub v2_downloads: usize,
    #[serde(default)]
    pub v3_downloads: usize,
    #[serde(default)]
    pub unknown_format_downloads: usize,
    pub slowest_downloads: Vec<TransferDetail>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrefetchAnalysis {
    pub prefetch_hits: usize,
    pub total_hits: usize,
    pub contribution_pct: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BypassAnalysis {
    pub passthroughs: usize,
    /// Query / probe invocations, excluded from `passthroughs` (see
    /// [`ReportSummary::probes`]).
    #[serde(default)]
    pub probes: usize,
    pub skipped: usize,
    pub fallbacks: usize,
    pub direct_passthroughs: usize,
    pub reasons: Vec<BypassReason>,
    pub slowest: Vec<BypassDetail>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BypassReason {
    pub result: String,
    pub route: String,
    pub reason: String,
    pub count: usize,
    pub failures: usize,
    pub max_elapsed_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BypassDetail {
    pub crate_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root: String,
    pub result: String,
    pub route: String,
    pub reason: String,
    #[serde(default)]
    pub start_time: String,
    #[serde(default)]
    pub end_time: String,
    #[serde(default)]
    pub start_unix_ms: i64,
    #[serde(default)]
    pub end_unix_ms: i64,
    pub elapsed_ms: u64,
    pub exit_code: Option<i32>,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceEvent {
    pub name: String,
    pub cat: String,
    pub ph: String,
    /// Chrome trace timestamp in microseconds.
    pub ts: i64,
    /// Chrome trace duration in microseconds.
    pub dur: u64,
    pub pid: u32,
    pub tid: u32,
    /// Perfetto color hint keyed to the result (hit / miss / passthrough …), so
    /// the timeline is legible at a glance. Top-level chrome-trace field;
    /// omitted when absent (#456).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cname: Option<String>,
    pub args: TraceArgs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceArgs {
    pub crate_name: String,
    pub root: String,
    pub result: String,
    pub route: String,
    pub reason: String,
    pub cache_key: String,
    pub elapsed_ms: u64,
    pub compile_time_ms: u64,
    pub overhead_ms: u64,
    pub size: u64,
    pub compiler_runs: u32,
    pub preprocessor_runs: u32,
    pub probe_runs: u32,
    pub store_output_blobs: u32,
    pub store_duplicate_blobs: u32,
    pub store_new_blobs: u32,
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrateDetail {
    pub crate_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub root: String,
    pub result: String,
    #[serde(default)]
    pub start_time: String,
    #[serde(default)]
    pub end_time: String,
    #[serde(default)]
    pub start_unix_ms: i64,
    #[serde(default)]
    pub end_unix_ms: i64,
    pub elapsed_ms: u64,
    pub compile_time_ms: u64,
    pub overhead_ms: u64,
    pub size: u64,
    pub cache_key: String,
    #[serde(default)]
    pub store_output_blobs: u32,
    #[serde(default)]
    pub store_duplicate_blobs: u32,
    #[serde(default)]
    pub store_new_blobs: u32,
    /// Times kache spawned the underlying compiler (0 on a hit, 1 on a
    /// dup/miss). Deterministic; the e2e harness asserts on it.
    #[serde(default)]
    pub compiler_runs: u32,
    /// Times kache spawned the preprocessor (`cc -E`) — once per C/C++
    /// compile for the cache key, 0 for rustc.
    #[serde(default)]
    pub preprocessor_runs: u32,
    /// Times kache spawned a compiler probe (`cc --version` / `cc -###`).
    /// Memoized on disk — one per build per flag set, 0 once warm.
    #[serde(default)]
    pub probe_runs: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferDetail {
    pub crate_name: String,
    pub direction: String,
    #[serde(default)]
    pub format: String,
    #[serde(default)]
    pub cache_key: String,
    #[serde(default)]
    pub object_key: String,
    pub compressed_bytes: u64,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub network_ms: u64,
    #[serde(default)]
    pub semaphore_wait_ms: u64,
    #[serde(default)]
    pub head_ms: u64,
    #[serde(default)]
    pub request_ms: u64,
    #[serde(default)]
    pub body_ms: u64,
    #[serde(default)]
    pub decompress_ms: u64,
    #[serde(default)]
    pub extract_ms: u64,
    #[serde(default)]
    pub disk_io_ms: u64,
    #[serde(default)]
    pub import_ms: u64,
    #[serde(default)]
    pub request_count: u32,
    #[serde(default)]
    pub blobs_skipped: u32,
    #[serde(default)]
    pub blobs_total: u32,
    pub throughput_mbps: f64,
    pub ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub crate_name: String,
    pub cache_key: String,
    pub timestamp: String,
}

// ── Report Generation ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct ReportFilter {
    pub root: Option<PathBuf>,
}

pub fn generate_report(config: &Config, hours: u64, top: usize) -> Result<BuildReport> {
    generate_report_with_filter(config, hours, top, &ReportFilter::default())
}

pub fn generate_report_with_filter(
    config: &Config,
    hours: u64,
    top: usize,
    filter: &ReportFilter,
) -> Result<BuildReport> {
    let since = Utc::now() - chrono::Duration::hours(hours as i64);
    let mut build_events = events::read_events_since(&config.event_log_path(), since)?;
    let root_filter = filter.root.as_deref().map(normalize_filter_root);
    if let Some(root) = root_filter.as_deref() {
        build_events.retain(|event| event_matches_root(event, root));
    }
    let since_ts = since.timestamp() as u64;
    let transfers = if root_filter.is_some() {
        Vec::new()
    } else {
        events::read_transfers_since(&config.transfer_log_path(), since_ts).unwrap_or_default()
    };

    let stats = events::compute_stats(&build_events);
    let total_compiled = stats.dups + stats.misses;
    let total_cacheable =
        stats.local_hits + stats.prefetch_hits + stats.remote_hits + total_compiled;
    let total_hits = stats.local_hits + stats.prefetch_hits + stats.remote_hits;
    let bypass = build_bypass_analysis(&build_events, top);
    let timeline = build_report_timeline(&build_events);
    let trace_events = build_trace_events(&build_events);

    let hit_rate = if total_cacheable > 0 {
        (total_hits as f64 / total_cacheable as f64) * 100.0
    } else {
        0.0
    };

    let total_compile = stats.hit_compile_time_ms + stats.miss_compile_time_ms;
    let weighted = if total_compile > 0 {
        Some((stats.hit_compile_time_ms as f64 / total_compile as f64) * 100.0)
    } else {
        None
    };

    // Build CrateDetail list
    let all_events: Vec<CrateDetail> = build_events
        .iter()
        .filter(|e| !matches!(e.result, EventResult::Skipped | EventResult::Passthrough))
        .map(to_crate_detail)
        .collect();

    let mut misses: Vec<CrateDetail> = build_events
        .iter()
        .filter(|e| matches!(e.result, EventResult::Dup | EventResult::Miss))
        .map(to_crate_detail)
        .collect();
    misses.sort_by_key(|entry| std::cmp::Reverse(entry.compile_time_ms));

    let mut hits: Vec<CrateDetail> = build_events
        .iter()
        .filter(|e| {
            matches!(
                e.result,
                EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit
            )
        })
        .map(to_crate_detail)
        .collect();
    hits.sort_by_key(|entry| std::cmp::Reverse(entry.compile_time_ms));

    let errors_detail: Vec<ErrorDetail> = build_events
        .iter()
        .filter(|e| matches!(e.result, EventResult::Error))
        .map(|e| ErrorDetail {
            crate_name: e.crate_name.clone(),
            cache_key: e.cache_key.clone(),
            timestamp: e.ts.to_rfc3339(),
        })
        .collect();

    // Timing
    let avg_hit_ms = if total_hits > 0 {
        stats.hit_elapsed_ms as f64 / total_hits as f64
    } else {
        0.0
    };
    let avg_miss_ms = if total_compiled > 0 {
        stats.miss_elapsed_ms as f64 / total_compiled as f64
    } else {
        0.0
    };
    let avg_hit_overhead = if total_hits > 0 && stats.hit_compile_time_ms > 0 {
        let overhead = stats.hit_elapsed_ms.saturating_sub(0); // hit_elapsed_ms IS the overhead for hits
        overhead as f64 / total_hits as f64
    } else {
        avg_hit_ms
    };

    // Network
    let network = if transfers.is_empty() {
        None
    } else {
        Some(build_network_analysis(&transfers, top))
    };

    // Prefetch
    let prefetch = PrefetchAnalysis {
        prefetch_hits: stats.prefetch_hits,
        total_hits,
        contribution_pct: if total_hits > 0 {
            (stats.prefetch_hits as f64 / total_hits as f64) * 100.0
        } else {
            0.0
        },
    };

    // Suggestions
    let suggestions = generate_suggestions(
        &stats,
        &prefetch,
        &network,
        root_filter.is_some(),
        &misses,
        total_cacheable,
        total_hits,
    );

    // Storage: restore side — how cache-hit restores landed on disk
    // (reflink / hardlink / copy). Store side — content-addressed dedup,
    // queried from the blob store. Both are deterministic byte counts,
    // independent of machine speed. A missing/unreadable store degrades
    // to zeroed dedup stats rather than failing the whole report.
    let restored_bytes = stats.reflinked_bytes + stats.hardlinked_bytes + stats.copied_bytes;
    let blob_stats = crate::store::Store::open(config)
        .and_then(|s| s.blob_stats())
        .unwrap_or_default();
    let storage = StorageBreakdown {
        reflinked_bytes: stats.reflinked_bytes,
        hardlinked_bytes: stats.hardlinked_bytes,
        copied_bytes: stats.copied_bytes,
        restored_bytes,
        zero_copy_pct: if restored_bytes > 0 {
            let zero_copy = stats.reflinked_bytes + stats.hardlinked_bytes;
            let pct = zero_copy as f64 / restored_bytes as f64 * 100.0;
            (pct * 10.0).round() / 10.0
        } else {
            0.0
        },
        store_reflinked_bytes: stats.store_reflinked_bytes,
        store_copied_bytes: stats.store_copied_bytes,
        store_blobs: blob_stats.total_blobs as u64,
        logical_bytes: blob_stats.total_logical_size,
        blob_bytes: blob_stats.total_blob_size,
        dedup_saved_bytes: blob_stats.savings,
    };

    Ok(BuildReport {
        meta: ReportMeta {
            kache_version: crate::VERSION.to_string(),
            generated_at: Utc::now().to_rfc3339(),
            since_hours: hours,
            root_filter: root_filter.clone(),
        },
        summary: ReportSummary {
            hit_rate_pct: (hit_rate * 10.0).round() / 10.0,
            weighted_hit_rate_pct: weighted.map(|w| (w * 10.0).round() / 10.0),
            time_saved_ms: stats.hit_compile_time_ms,
            total_crates: total_cacheable,
            local_hits: stats.local_hits,
            prefetch_hits: stats.prefetch_hits,
            remote_hits: stats.remote_hits,
            dups: stats.dups,
            misses: stats.misses,
            errors: stats.errors,
            passthroughs: bypass.passthroughs,
            probes: bypass.probes,
            skipped: bypass.skipped,
            fallbacks: bypass.fallbacks,
            total_duration_ms: stats.total_elapsed_ms,
            cache_efficiency_pct: {
                let denom = stats.hit_compile_time_ms + stats.miss_compile_time_ms;
                if denom > 0 {
                    let raw = (stats.hit_compile_time_ms as f64 / denom as f64) * 100.0;
                    (raw * 10.0).round() / 10.0
                } else {
                    0.0
                }
            },
        },
        timeline,
        timing: TimingBreakdown {
            hit_time_ms: stats.hit_elapsed_ms,
            miss_time_ms: stats.miss_elapsed_ms,
            avg_hit_ms: (avg_hit_ms * 10.0).round() / 10.0,
            avg_miss_ms: (avg_miss_ms * 10.0).round() / 10.0,
            avg_hit_overhead_ms: (avg_hit_overhead * 10.0).round() / 10.0,
            miss_compile_time_ms: stats.miss_compile_time_ms,
            avg_key_ms: if total_cacheable > 0 {
                (stats.total_key_ms as f64 / total_cacheable as f64 * 10.0).round() / 10.0
            } else {
                0.0
            },
            avg_lookup_ms: if total_cacheable > 0 {
                (stats.total_lookup_ms as f64 / total_cacheable as f64 * 10.0).round() / 10.0
            } else {
                0.0
            },
            avg_restore_ms: if total_hits > 0 {
                (stats.total_restore_ms as f64 / total_hits as f64 * 10.0).round() / 10.0
            } else {
                0.0
            },
            avg_store_ms: if total_compiled > 0 {
                (stats.total_store_ms as f64 / total_compiled as f64 * 10.0).round() / 10.0
            } else {
                0.0
            },
            total_key_ms: stats.total_key_ms,
            total_lookup_ms: stats.total_lookup_ms,
            total_restore_ms: stats.total_restore_ms,
            total_store_ms: stats.total_store_ms,
        },
        storage,
        network,
        prefetch,
        top_misses: misses.into_iter().take(top).collect(),
        top_hits: hits.into_iter().take(top).collect(),
        all_events,
        trace_events,
        display_time_unit: default_trace_display_time_unit(),
        bypass,
        errors_detail,
        suggestions,
        gc: load_gc_summary(&config.cache_dir, hours),
    })
}

fn normalize_filter_root(root: &Path) -> String {
    let abs = if root.is_absolute() {
        root.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(root)
    };
    std::fs::canonicalize(&abs)
        .unwrap_or(abs)
        .to_string_lossy()
        .into_owned()
}

fn event_matches_root(event: &BuildEvent, root: &str) -> bool {
    if event.root.is_empty() {
        return false;
    }
    event.root == root
        || event
            .root
            .strip_prefix(root)
            .is_some_and(|suffix| suffix.starts_with(std::path::MAIN_SEPARATOR))
}

/// Load GC stats from gc_stats.json if GC ran within the report window.
fn load_gc_summary(cache_dir: &std::path::Path, hours: u64) -> Option<GcSummary> {
    let path = cache_dir.join("gc_stats.json");
    let content = std::fs::read_to_string(&path).ok()?;
    let persisted: GcStatsPersisted = serde_json::from_str(&content).ok()?;

    // Only include if GC ran within the report window
    let last_run = chrono::DateTime::parse_from_rfc3339(&persisted.last_run).ok()?;
    let cutoff = Utc::now() - chrono::Duration::hours(hours as i64);
    if last_run < cutoff {
        return None;
    }

    Some(GcSummary {
        last_run: persisted.last_run,
        entries_evicted: persisted.entries_evicted,
        bytes_freed: persisted.bytes_freed,
        blobs_removed: persisted.blobs_removed,
    })
}

fn build_report_timeline(events: &[BuildEvent]) -> ReportTimeline {
    let Some(first) = events.first() else {
        return ReportTimeline::default();
    };

    let mut start = event_start(first);
    let mut end = first.ts;
    let mut cacheable_count = 0;
    let mut hit_count = 0;
    let mut compiled_count = 0;
    let mut passthrough_count = 0;
    let mut probe_count = 0;
    let mut skipped_count = 0;
    let mut error_count = 0;

    for event in events {
        start = start.min(event_start(event));
        end = end.max(event.ts);
        match event.result {
            EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit => {
                cacheable_count += 1;
                hit_count += 1;
            }
            EventResult::Dup | EventResult::Miss => {
                cacheable_count += 1;
                compiled_count += 1;
            }
            EventResult::Error => {
                error_count += 1;
            }
            // A probe / query is a passthrough event but not a compile —
            // counted on its own so `passthrough_count` means refusals.
            EventResult::Passthrough if is_probe_passthrough(event) => {
                probe_count += 1;
            }
            EventResult::Passthrough => {
                passthrough_count += 1;
            }
            EventResult::Skipped => {
                skipped_count += 1;
            }
        }
    }

    let duration_ms = end
        .signed_duration_since(start)
        .num_milliseconds()
        .try_into()
        .unwrap_or(0);

    ReportTimeline {
        start_time: Some(start.to_rfc3339()),
        end_time: Some(end.to_rfc3339()),
        start_unix_ms: Some(start.timestamp_millis()),
        end_unix_ms: Some(end.timestamp_millis()),
        duration_ms,
        event_count: events.len(),
        cacheable_count,
        hit_count,
        compiled_count,
        passthrough_count,
        probe_count,
        skipped_count,
        error_count,
    }
}

fn build_trace_events(events: &[BuildEvent]) -> Vec<TraceEvent> {
    let lanes = assign_trace_lanes(events);
    events
        .iter()
        .enumerate()
        .map(|(index, event)| to_trace_event(event, lanes[index]))
        .collect()
}

/// Assign each event to a worker lane by greedy interval packing on start time:
/// an event takes the lowest-numbered lane whose previous slice has already
/// ended, so concurrent compiles land on distinct lanes and the lane count is
/// the build's peak concurrency — a compact, readable timeline instead of one
/// lane per event. Deterministic (ties broken by event index), and the small
/// `0..N` lane ids never leak OS thread ids (#456).
fn assign_trace_lanes(events: &[BuildEvent]) -> Vec<u32> {
    let mut order: Vec<usize> = (0..events.len()).collect();
    order.sort_by_key(|&i| (event_start(&events[i]).timestamp_micros(), i));

    let mut lane_free_at: Vec<i64> = Vec::new();
    let mut lanes = vec![0u32; events.len()];
    for &i in &order {
        let start = event_start(&events[i]).timestamp_micros();
        let end = start.saturating_add(events[i].elapsed_ms.saturating_mul(1000) as i64);
        let lane = match lane_free_at.iter().position(|&free| free <= start) {
            Some(l) => {
                lane_free_at[l] = end;
                l
            }
            None => {
                lane_free_at.push(end);
                lane_free_at.len() - 1
            }
        };
        lanes[i] = u32::try_from(lane).unwrap_or(u32::MAX);
    }
    lanes
}

/// Slice label prefix + Perfetto color hint (`cname`) for a result, so hits,
/// misses, dups, and passthroughs are visually distinct in the timeline without
/// clicking into a slice (#456).
fn trace_result_style(result: EventResult) -> (&'static str, &'static str) {
    match result {
        EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit => {
            ("hit", "good")
        }
        EventResult::Miss => ("miss", "bad"),
        EventResult::Dup => ("dup", "olive"),
        EventResult::Passthrough => ("passthrough", "grey"),
        EventResult::Error => ("error", "terrible"),
        EventResult::Skipped => ("skipped", "grey"),
    }
}

fn to_trace_event(event: &BuildEvent, lane: u32) -> TraceEvent {
    let overhead_ms = event_overhead_ms(event);
    let start = event_start(event);
    let (label, cname) = trace_result_style(event.result);
    TraceEvent {
        name: format!("{label}: {}", event.crate_name),
        cat: "kache".to_string(),
        ph: "X".to_string(),
        ts: start.timestamp_micros(),
        dur: event.elapsed_ms.saturating_mul(1000),
        pid: 1,
        tid: lane,
        cname: Some(cname.to_string()),
        args: TraceArgs {
            crate_name: event.crate_name.clone(),
            root: event.root.clone(),
            result: event.result.to_string(),
            route: bypass_route(event).to_string(),
            reason: bypass_reason(event),
            cache_key: event.cache_key.clone(),
            elapsed_ms: event.elapsed_ms,
            compile_time_ms: event.compile_time_ms,
            overhead_ms,
            size: event.size,
            compiler_runs: event.compiler_runs,
            preprocessor_runs: event.preprocessor_runs,
            probe_runs: event.probe_runs,
            store_output_blobs: event.store_output_blobs,
            store_duplicate_blobs: event.store_duplicate_blobs,
            store_new_blobs: event.store_new_blobs,
            exit_code: event.exit_code,
        },
    }
}

fn to_crate_detail(e: &BuildEvent) -> CrateDetail {
    let overhead = event_overhead_ms(e);
    let (start_time, end_time, start_unix_ms, end_unix_ms) = event_timeline(e);
    CrateDetail {
        crate_name: e.crate_name.clone(),
        root: e.root.clone(),
        result: e.result.to_string(),
        start_time,
        end_time,
        start_unix_ms,
        end_unix_ms,
        elapsed_ms: e.elapsed_ms,
        compile_time_ms: e.compile_time_ms,
        overhead_ms: overhead,
        size: e.size,
        cache_key: e.cache_key.clone(),
        store_output_blobs: e.store_output_blobs,
        store_duplicate_blobs: e.store_duplicate_blobs,
        store_new_blobs: e.store_new_blobs,
        compiler_runs: e.compiler_runs,
        preprocessor_runs: e.preprocessor_runs,
        probe_runs: e.probe_runs,
    }
}

fn event_overhead_ms(e: &BuildEvent) -> u64 {
    if matches!(
        e.result,
        EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit
    ) {
        e.elapsed_ms
    } else {
        e.elapsed_ms.saturating_sub(e.compile_time_ms)
    }
}

fn build_bypass_analysis(events: &[BuildEvent], top: usize) -> BypassAnalysis {
    let mut details: Vec<BypassDetail> = events
        .iter()
        .filter(|event| {
            matches!(
                event.result,
                EventResult::Passthrough | EventResult::Skipped
            )
        })
        .map(to_bypass_detail)
        .collect();

    // A probe / query (`--print`, `-vV`, `cc -###`) is recorded as a
    // passthrough event but is NOT a compile kache failed to cache. Split
    // it into its own count so `passthroughs` stays the actionable signal.
    let probes = details
        .iter()
        .filter(|detail| {
            detail.result == "passthrough"
                && passthrough_category(&detail.reason) == "not-a-compile"
        })
        .count();
    let passthroughs = details
        .iter()
        .filter(|detail| {
            detail.result == "passthrough"
                && passthrough_category(&detail.reason) != "not-a-compile"
        })
        .count();
    let skipped = details
        .iter()
        .filter(|detail| detail.result == "skipped")
        .count();
    let fallbacks = details
        .iter()
        .filter(|detail| detail.route == "fallback")
        .count();
    let direct_passthroughs = details
        .iter()
        .filter(|detail| detail.route == "direct")
        .count();

    let mut grouped = std::collections::BTreeMap::<(String, String, String), BypassReason>::new();
    for detail in &details {
        let key = (
            detail.result.clone(),
            detail.route.clone(),
            detail.reason.clone(),
        );
        let entry = grouped.entry(key).or_insert_with(|| BypassReason {
            result: detail.result.clone(),
            route: detail.route.clone(),
            reason: detail.reason.clone(),
            count: 0,
            failures: 0,
            max_elapsed_ms: 0,
        });
        entry.count += 1;
        if detail.exit_code.is_some_and(|code| code != 0) {
            entry.failures += 1;
        }
        entry.max_elapsed_ms = entry.max_elapsed_ms.max(detail.elapsed_ms);
    }

    let mut reasons: Vec<BypassReason> = grouped.into_values().collect();
    reasons.sort_by_key(|reason| {
        (
            std::cmp::Reverse(reason.count),
            std::cmp::Reverse(reason.max_elapsed_ms),
            reason.reason.clone(),
        )
    });
    reasons.truncate(top);

    details.sort_by_key(|detail| std::cmp::Reverse(detail.elapsed_ms));
    details.truncate(top);

    BypassAnalysis {
        passthroughs,
        probes,
        skipped,
        fallbacks,
        direct_passthroughs,
        reasons,
        slowest: details,
    }
}

fn to_bypass_detail(e: &BuildEvent) -> BypassDetail {
    let (start_time, end_time, start_unix_ms, end_unix_ms) = event_timeline(e);
    BypassDetail {
        crate_name: e.crate_name.clone(),
        root: e.root.clone(),
        result: e.result.to_string(),
        route: bypass_route(e).to_string(),
        reason: bypass_reason(e),
        start_time,
        end_time,
        start_unix_ms,
        end_unix_ms,
        elapsed_ms: e.elapsed_ms,
        exit_code: e.exit_code,
        timestamp: e.ts.to_rfc3339(),
    }
}

fn event_timeline(e: &BuildEvent) -> (String, String, i64, i64) {
    let start = event_start(e);
    (
        start.to_rfc3339(),
        e.ts.to_rfc3339(),
        start.timestamp_millis(),
        e.ts.timestamp_millis(),
    )
}

fn event_start(e: &BuildEvent) -> DateTime<Utc> {
    let elapsed_ms = i64::try_from(e.elapsed_ms).unwrap_or(i64::MAX);
    e.ts.checked_sub_signed(chrono::Duration::milliseconds(elapsed_ms))
        .unwrap_or(e.ts)
}

fn bypass_route(e: &BuildEvent) -> &'static str {
    match e.result {
        EventResult::Passthrough if e.fallback => "fallback",
        EventResult::Passthrough => "direct",
        EventResult::Skipped => "skipped",
        _ => "n/a",
    }
}

fn bypass_reason(e: &BuildEvent) -> String {
    let reason = e.passthrough_reason.trim();
    if reason.is_empty() {
        "unknown".to_string()
    } else {
        reason.to_string()
    }
}

/// Coarse category of a passthrough reason — the `category` half of the
/// structured `category|detail` string kache emits (see
/// `wrapper::refuse_reason_string`). `not-a-compile` marks a query / probe
/// (`--print`, `-vV`, `cc -###`); `unsupported` marks a real compile kache
/// refused to cache. Policy skips / legacy events carry no prefix, so the
/// whole reason is returned and won't match the probe category.
fn passthrough_category(reason: &str) -> &str {
    reason.split('|').next().unwrap_or("").trim()
}

/// Whether a passthrough event is a probe / query rather than a compile
/// kache failed to cache. Probes get their own count so `passthroughs`
/// stays the actionable "compiles we couldn't cache" signal — a probe is
/// not a compilation at all (see [`crate::compiler::RefuseReason::NotPrimary`]).
fn is_probe_passthrough(e: &BuildEvent) -> bool {
    matches!(e.result, EventResult::Passthrough)
        && passthrough_category(&e.passthrough_reason) == "not-a-compile"
}

fn build_network_analysis(transfers: &[TransferEvent], top: usize) -> NetworkAnalysis {
    let mut bytes_up = 0u64;
    let mut bytes_down = 0u64;
    let mut uploads_ok = 0usize;
    let mut uploads_failed = 0usize;
    let mut downloads_ok = 0usize;
    let mut downloads_failed = 0usize;
    let mut download_latencies: Vec<u64> = Vec::new();
    let mut total_download_bytes = 0u64;
    let mut total_download_ms = 0u64;
    let mut total_network_ms = 0u64;
    let mut total_request_ms = 0u64;
    let mut total_body_ms = 0u64;
    let mut total_semaphore_wait_ms = 0u64;
    let mut total_head_ms = 0u64;
    let mut total_get_requests = 0u32;
    let mut total_original_bytes = 0u64;
    let mut total_decompress_ms = 0u64;
    let mut total_extract_ms = 0u64;
    let mut total_disk_io_ms_measured = 0u64;
    let mut has_disk_io_measurement = false;
    let mut total_import_ms = 0u64;
    let mut total_compression_ms = 0u64;
    let mut total_head_checks_ms = 0u64;
    let mut blobs_skipped = 0u32;
    let mut blobs_total = 0u32;
    let mut v1_downloads = 0usize;
    let mut v2_downloads = 0usize;
    let mut v3_downloads = 0usize;
    let mut unknown_format_downloads = 0usize;

    for t in transfers {
        match t.direction {
            TransferDirection::Upload => {
                if t.ok {
                    uploads_ok += 1;
                    bytes_up += t.compressed_bytes;
                    total_compression_ms += t.compression_ms;
                    total_head_checks_ms += t.head_checks_ms;
                } else {
                    uploads_failed += 1;
                }
            }
            TransferDirection::Download => {
                if t.ok {
                    downloads_ok += 1;
                    bytes_down += t.compressed_bytes;
                    download_latencies.push(t.elapsed_ms);
                    total_download_bytes += t.compressed_bytes;
                    total_original_bytes += t.original_bytes;
                    total_decompress_ms += t.decompress_ms;
                    total_extract_ms += t.extract_ms;
                    total_import_ms += t.import_ms;
                    total_semaphore_wait_ms += t.semaphore_wait_ms;
                    total_head_ms += t.head_ms;
                    total_request_ms += t.request_ms;
                    total_body_ms += t.body_ms;
                    total_get_requests += t.request_count;
                    if t.disk_io_ms > 0 {
                        total_disk_io_ms_measured += t.disk_io_ms;
                        has_disk_io_measurement = true;
                    }
                    blobs_skipped += t.blobs_skipped;
                    blobs_total += t.blobs_total;
                    match t.format.as_str() {
                        "v1" => v1_downloads += 1,
                        "v2" => v2_downloads += 1,
                        "v3" => v3_downloads += 1,
                        _ => unknown_format_downloads += 1,
                    }
                    total_download_ms += t.elapsed_ms;
                    // network_ms defaults to 0 for older log entries
                    total_network_ms += if t.network_ms > 0 {
                        t.network_ms
                    } else {
                        t.elapsed_ms
                    };
                } else {
                    downloads_failed += 1;
                }
            }
        }
    }

    download_latencies.sort_unstable();

    let avg_download_ms = if !download_latencies.is_empty() {
        total_download_ms as f64 / download_latencies.len() as f64
    } else {
        0.0
    };

    let p95_download_ms = if !download_latencies.is_empty() {
        let idx = (download_latencies.len() * 95 / 100).min(download_latencies.len() - 1);
        download_latencies[idx]
    } else {
        0
    };

    let max_download_ms = download_latencies.last().copied().unwrap_or(0);

    // Wall-clock throughput (includes decompression + disk I/O)
    let throughput_mbps = if total_download_ms > 0 {
        (total_download_bytes as f64 / (1024.0 * 1024.0)) / (total_download_ms as f64 / 1000.0)
    } else {
        0.0
    };

    // Network-only throughput (S3 GET + body collection, excludes decompress/disk)
    let network_throughput_mbps = if total_network_ms > 0 {
        (total_download_bytes as f64 / (1024.0 * 1024.0)) / (total_network_ms as f64 / 1000.0)
    } else {
        0.0
    };

    // Body-only throughput isolates raw object-store transfer once bytes start flowing.
    let body_throughput_mbps = if total_body_ms > 0 {
        (total_download_bytes as f64 / (1024.0 * 1024.0)) / (total_body_ms as f64 / 1000.0)
    } else {
        0.0
    };

    // Slowest downloads
    let mut download_details: Vec<TransferDetail> = transfers
        .iter()
        .filter(|t| matches!(t.direction, TransferDirection::Download) && t.ok)
        .map(|t| {
            let tp = if t.elapsed_ms > 0 {
                (t.compressed_bytes as f64 / (1024.0 * 1024.0)) / (t.elapsed_ms as f64 / 1000.0)
            } else {
                0.0
            };
            TransferDetail {
                crate_name: t.crate_name.clone(),
                direction: "download".to_string(),
                format: t.format.clone(),
                cache_key: t.cache_key.clone(),
                object_key: t.object_key.clone(),
                compressed_bytes: t.compressed_bytes,
                elapsed_ms: t.elapsed_ms,
                network_ms: t.network_ms,
                semaphore_wait_ms: t.semaphore_wait_ms,
                head_ms: t.head_ms,
                request_ms: t.request_ms,
                body_ms: t.body_ms,
                decompress_ms: t.decompress_ms,
                extract_ms: t.extract_ms,
                disk_io_ms: t.disk_io_ms,
                import_ms: t.import_ms,
                request_count: t.request_count,
                blobs_skipped: t.blobs_skipped,
                blobs_total: t.blobs_total,
                throughput_mbps: (tp * 10.0).round() / 10.0,
                ok: t.ok,
            }
        })
        .collect();
    download_details.sort_by_key(|entry| std::cmp::Reverse(entry.elapsed_ms));

    let compression_ratio = if total_download_bytes > 0 && total_original_bytes > 0 {
        total_original_bytes as f64 / total_download_bytes as f64
    } else {
        0.0
    };

    // Disk I/O: use directly measured value when available, otherwise approximate
    let total_disk_io_ms = if has_disk_io_measurement {
        total_disk_io_ms_measured
    } else {
        total_download_ms.saturating_sub(total_network_ms + total_decompress_ms + total_extract_ms)
    };
    let phase_totals = [
        ("wait", total_semaphore_wait_ms),
        ("HEAD", total_head_ms),
        ("request", total_request_ms),
        ("body", total_body_ms),
        ("decompress", total_decompress_ms),
        ("extract", total_extract_ms),
        ("import", total_import_ms),
        ("disk", total_disk_io_ms),
    ];
    let phase_total_ms: u64 = phase_totals.iter().map(|(_, ms)| *ms).sum();
    let (dominant_phase, dominant_phase_ms) = phase_totals
        .iter()
        .copied()
        .max_by_key(|(_, ms)| *ms)
        .unwrap_or(("unknown", 0));
    let dominant_phase_pct = if phase_total_ms > 0 {
        dominant_phase_ms as f64 / phase_total_ms as f64 * 100.0
    } else {
        0.0
    };

    NetworkAnalysis {
        bytes_up,
        bytes_down,
        uploads_ok,
        uploads_failed,
        downloads_ok,
        downloads_failed,
        avg_download_ms: (avg_download_ms * 10.0).round() / 10.0,
        p95_download_ms,
        max_download_ms,
        throughput_mbps: (throughput_mbps * 10.0).round() / 10.0,
        network_throughput_mbps: (network_throughput_mbps * 10.0).round() / 10.0,
        body_throughput_mbps: (body_throughput_mbps * 10.0).round() / 10.0,
        dominant_download_phase: dominant_phase.to_string(),
        dominant_download_phase_ms: dominant_phase_ms,
        dominant_download_phase_pct: (dominant_phase_pct * 10.0).round() / 10.0,
        total_request_ms,
        total_body_ms,
        total_semaphore_wait_ms,
        total_head_ms,
        total_get_requests,
        compression_ratio: (compression_ratio * 10.0).round() / 10.0,
        original_bytes_down: total_original_bytes,
        total_decompress_ms,
        total_extract_ms,
        total_disk_io_ms,
        total_import_ms,
        total_compression_ms,
        total_head_checks_ms,
        blobs_skipped,
        blobs_total,
        v1_downloads,
        v2_downloads,
        v3_downloads,
        unknown_format_downloads,
        slowest_downloads: download_details.into_iter().take(top).collect(),
    }
}

fn generate_suggestions(
    stats: &events::EventStats,
    prefetch: &PrefetchAnalysis,
    network: &Option<NetworkAnalysis>,
    root_filtered: bool,
    top_misses: &[CrateDetail],
    total_cacheable: usize,
    total_hits: usize,
) -> Vec<String> {
    let mut suggestions = Vec::new();

    // High miss share
    let total_compiled = stats.dups + stats.misses;
    if total_cacheable > 0 && stats.miss_compile_time_ms > 0 {
        let miss_share = stats.miss_compile_time_ms as f64
            / (stats.miss_compile_time_ms + stats.hit_compile_time_ms) as f64
            * 100.0;
        if miss_share > 80.0 && total_compiled > 3 {
            let top_names: Vec<&str> = top_misses
                .iter()
                .take(3)
                .map(|c| c.crate_name.as_str())
                .collect();
            suggestions.push(format!(
                "{:.0}% of compile time spent on compiled cache-key misses — improve hit rate for {}",
                miss_share,
                if top_names.is_empty() {
                    "top compiled cache-key misses".to_string()
                } else {
                    top_names
                        .iter()
                        .map(|n| format!("`{n}`"))
                        .collect::<Vec<_>>()
                        .join(", ")
                },
            ));
        }
    }

    // High hit overhead
    if total_hits > 0 {
        let avg_overhead = stats.hit_elapsed_ms as f64 / total_hits as f64;
        if avg_overhead > 50.0 {
            suggestions.push(format!(
                "Average cache hit overhead is {:.0}ms — check disk I/O or consider faster storage",
                avg_overhead
            ));
        }
    }

    // Low prefetch contribution
    if prefetch.total_hits > 10 && prefetch.contribution_pct < 20.0 {
        suggestions.push(
            "Prefetch contributed <20% of hits — check namespace/shard configuration".to_string(),
        );
    }

    // Network issues
    if let Some(net) = network {
        let total_downloads = net.downloads_ok + net.downloads_failed;
        if total_downloads > 0 {
            let fail_rate = net.downloads_failed as f64 / total_downloads as f64 * 100.0;
            if fail_rate > 10.0 {
                suggestions.push(format!(
                    "{:.0}% of downloads failed — check network connectivity and S3 credentials",
                    fail_rate
                ));
            }
        }
        if net.downloads_ok > 0 && net.total_get_requests > net.downloads_ok as u32 * 3 {
            suggestions.push(format!(
                "Downloads fan out to {:.1} GETs per cache hit — check remote layout granularity or prefer pack-first downloads on CI",
                net.total_get_requests as f64 / net.downloads_ok as f64
            ));
        }
        if net.total_semaphore_wait_ms > 10_000 {
            suggestions.push(format!(
                "Aggregate S3 semaphore wait totaled {} — tune concurrency only if the object store can absorb it",
                format_duration_ms(net.total_semaphore_wait_ms)
            ));
        }
        if net.total_request_ms > 30_000 && net.total_request_ms > net.total_body_ms {
            suggestions.push(format!(
                "Aggregate request/header latency ({}) exceeds body transfer ({}) — check RGW/request path, connection reuse, or object fan-out",
                format_duration_ms(net.total_request_ms),
                format_duration_ms(net.total_body_ms)
            ));
        }
        if net.total_extract_ms > 30_000 && net.total_extract_ms > net.total_body_ms {
            suggestions.push(format!(
                "Aggregate archive extract time ({}) exceeds body transfer ({}) — profile zstd/tar extraction and SQLite import separately",
                format_duration_ms(net.total_extract_ms),
                format_duration_ms(net.total_body_ms)
            ));
        }
    }

    if root_filtered {
        suggestions.push(
            "Network transfer data omitted because transfer events are not root-scoped yet"
                .to_string(),
        );
    } else if network.is_none() {
        suggestions.push("No network transfer data available for this session".to_string());
    }

    suggestions
}

// ── Output Formatters ───────────────────────────────────────────────────────

fn bypass_total(bypass: &BypassAnalysis) -> usize {
    // Probes included so the detail tables still render for a build whose
    // only uncached activity was query/probe invocations.
    bypass.passthroughs + bypass.probes + bypass.skipped
}

fn format_bypass_summary(bypass: &BypassAnalysis) -> String {
    let mut parts = Vec::new();
    if bypass.passthroughs > 0 {
        let mut part = format!("{} passthrough", bypass.passthroughs);
        if bypass.passthroughs != 1 {
            part.push('s');
        }
        if bypass.fallbacks > 0 {
            part.push_str(&format!(" ({} via fallback)", bypass.fallbacks));
        }
        parts.push(part);
    }
    if bypass.skipped > 0 {
        let mut part = format!("{} skipped", bypass.skipped);
        if bypass.skipped == 1 {
            part = "1 skipped".to_string();
        }
        parts.push(part);
    }
    if bypass.probes > 0 {
        // Probes are not refusals — label them so a clean build's probe
        // traffic doesn't read as a caching problem.
        parts.push(format!("{} probe{}", bypass.probes, plural(bypass.probes)));
    }
    if parts.is_empty() {
        "none".to_string()
    } else {
        parts.join(" / ")
    }
}

/// `""` for 1, `"s"` otherwise — for pluralizing count labels.
fn plural(n: usize) -> &'static str {
    if n == 1 { "" } else { "s" }
}

fn markdown_cell(value: &str) -> String {
    value.replace('\n', " ").replace('|', "\\|")
}

fn format_exit_code(exit_code: Option<i32>) -> String {
    exit_code
        .map(|code| code.to_string())
        .unwrap_or_else(|| "-".to_string())
}

fn cache_roi(report: &BuildReport) -> Option<f64> {
    if report.summary.time_saved_ms > 0 && report.timing.hit_time_ms > 0 {
        let raw = report.summary.time_saved_ms as f64 / report.timing.hit_time_ms as f64;
        Some((raw * 10.0).round() / 10.0)
    } else {
        None
    }
}

fn cache_overhead_summary(report: &BuildReport) -> Option<String> {
    let total_hits =
        report.summary.local_hits + report.summary.prefetch_hits + report.summary.remote_hits;
    if total_hits == 0 || report.timing.hit_time_ms == 0 {
        return None;
    }
    Some(format!(
        "{} aggregate, avg {:.0}ms/hit",
        format_duration_ms(report.timing.hit_time_ms),
        report.timing.avg_hit_ms
    ))
}

fn has_storage_data(storage: &StorageBreakdown) -> bool {
    storage.restored_bytes > 0
        || storage.logical_bytes > 0
        || storage.blob_bytes > 0
        || storage.dedup_saved_bytes > 0
        || storage.store_reflinked_bytes > 0
        || storage.store_copied_bytes > 0
}

fn push_storage_table(lines: &mut Vec<String>, storage: &StorageBreakdown) {
    lines.push("| Metric | Value |".to_string());
    lines.push("|---|---|".to_string());
    if storage.restored_bytes > 0 {
        lines.push(format!(
            "| Restored bytes | {} total: {} reflink, {} hardlink, {} copied |",
            format_bytes(storage.restored_bytes),
            format_bytes(storage.reflinked_bytes),
            format_bytes(storage.hardlinked_bytes),
            format_bytes(storage.copied_bytes),
        ));
        lines.push(format!(
            "| Zero-copy restores | {:.1}% |",
            storage.zero_copy_pct
        ));
    }
    if storage.logical_bytes > 0 || storage.blob_bytes > 0 {
        lines.push(format!(
            "| Store footprint | {} logical -> {} blobs, {} dedup saved |",
            format_bytes(storage.logical_bytes),
            format_bytes(storage.blob_bytes),
            format_bytes(storage.dedup_saved_bytes),
        ));
        lines.push(format!("| Store blobs | {} |", storage.store_blobs));
    }
    let ingested = storage.store_reflinked_bytes + storage.store_copied_bytes;
    if ingested > 0 {
        // Reflinked ingest shares blocks with the build's own output, so it
        // adds ~no physical disk; only copied ingest is a genuine second copy.
        let reflink_pct = storage.store_reflinked_bytes as f64 / ingested as f64 * 100.0;
        lines.push(format!(
            "| Store ingest | {} reflinked (CoW), {} copied — {:.1}% shared with build output |",
            format_bytes(storage.store_reflinked_bytes),
            format_bytes(storage.store_copied_bytes),
            (reflink_pct * 10.0).round() / 10.0,
        ));
    }
}

fn push_error_table(lines: &mut Vec<String>, errors: &[ErrorDetail]) {
    lines.push("| Crate | Time | Key |".to_string());
    lines.push("|---|---|---|".to_string());
    for err in errors.iter().take(10) {
        let key_short = if err.cache_key.len() > 12 {
            &err.cache_key[..12]
        } else {
            &err.cache_key
        };
        lines.push(format!(
            "| `{}` | {} | `{}` |",
            markdown_cell(&err.crate_name),
            err.timestamp,
            markdown_cell(key_short),
        ));
    }
    if errors.len() > 10 {
        lines.push(format!("| *... {} more* | | |", errors.len() - 10));
    }
}

fn push_bypass_tables(lines: &mut Vec<String>, bypass: &BypassAnalysis) {
    if !bypass.reasons.is_empty() {
        lines.push("| Result | Route | Reason | Count | Failures | Max time |".to_string());
        lines.push("|---|---|---|---:|---:|---:|".to_string());
        for reason in &bypass.reasons {
            lines.push(format!(
                "| {} | {} | {} | {} | {} | {} |",
                reason.result,
                reason.route,
                markdown_cell(&reason.reason),
                reason.count,
                reason.failures,
                format_duration_ms(reason.max_elapsed_ms),
            ));
        }
    }

    if !bypass.slowest.is_empty() {
        lines.push(String::new());
        lines.push("**Slowest bypassed invocations:**".to_string());
        lines.push(String::new());
        lines.push("| Crate | Result | Route | Time | Exit | Reason |".to_string());
        lines.push("|---|---|---|---:|---:|---|".to_string());
        for detail in &bypass.slowest {
            lines.push(format!(
                "| `{}` | {} | {} | {} | {} | {} |",
                markdown_cell(&detail.crate_name),
                detail.result,
                detail.route,
                format_duration_ms(detail.elapsed_ms),
                format_exit_code(detail.exit_code),
                markdown_cell(&detail.reason),
            ));
        }
    }
}

pub fn format_json(report: &BuildReport) -> Result<String> {
    Ok(serde_json::to_string_pretty(report)?)
}

pub fn format_trace_json(report: &BuildReport) -> Result<String> {
    #[derive(Serialize)]
    struct TraceOutput<'a> {
        #[serde(rename = "displayTimeUnit")]
        display_time_unit: &'a str,
        #[serde(rename = "traceEvents")]
        trace_events: Vec<serde_json::Value>,
    }

    // Lead with chrome-trace metadata events that name the process and each
    // worker lane, so Perfetto labels them "kache" / "worker N" instead of
    // "Process 1" / "Thread <n>" (#456).
    let mut trace_events: Vec<serde_json::Value> = Vec::new();
    trace_events.push(trace_metadata_event("process_name", 0, "kache"));
    let mut lanes: Vec<u32> = report.trace_events.iter().map(|e| e.tid).collect();
    lanes.sort_unstable();
    lanes.dedup();
    for tid in lanes {
        trace_events.push(trace_metadata_event(
            "thread_name",
            tid,
            &format!("worker {tid}"),
        ));
    }
    for event in &report.trace_events {
        trace_events.push(serde_json::to_value(event)?);
    }

    Ok(serde_json::to_string_pretty(&TraceOutput {
        display_time_unit: &report.display_time_unit,
        trace_events,
    })?)
}

/// A chrome-trace `M` (metadata) event used to label the process / worker lanes.
fn trace_metadata_event(name: &str, tid: u32, value: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "ph": "M",
        "pid": 1,
        "tid": tid,
        "args": { "name": value },
    })
}

pub fn format_markdown(report: &BuildReport) -> String {
    use crate::cli::format_duration_ms;

    let mut lines = Vec::new();
    let s = &report.summary;
    let t = &report.timing;

    let total_hits = s.local_hits + s.prefetch_hits + s.remote_hits;
    let total_compiled = s.dups + s.misses;
    lines.push("### kache build report".to_string());
    lines.push(String::new());
    lines.push(format!(
        "**{:.1}% hit rate** — {}/{} cacheable crates from cache, {} compiled | **{} compile work avoided**",
        s.hit_rate_pct,
        total_hits,
        s.total_crates,
        total_compiled,
        format_duration_ms(s.time_saved_ms),
    ));
    lines.push(String::new());

    // Summary table
    lines.push("#### Summary".to_string());
    lines.push("| Metric | Value |".to_string());
    lines.push("|---|---|".to_string());
    lines.push(format!("| Window | last {}h |", report.meta.since_hours));
    lines.push(format!("| Hit rate (count) | {:.1}% |", s.hit_rate_pct));
    if let Some(w) = s.weighted_hit_rate_pct {
        lines.push(format!("| Hit rate (compile-cost weighted) | {:.1}% |", w));
    }
    lines.push(format!(
        "| Compile work avoided | {} aggregate |",
        format_duration_ms(s.time_saved_ms)
    ));
    if let Some(overhead) = cache_overhead_summary(report) {
        lines.push(format!("| Cache hit overhead | {} |", overhead));
    }
    if let Some(roi) = cache_roi(report) {
        lines.push(format!(
            "| Cache ROI | {:.1}x compile work per cache-hit overhead |",
            roi
        ));
    }
    if t.miss_compile_time_ms > 0 {
        lines.push(format!(
            "| Miss compile work | {} aggregate |",
            format_duration_ms(t.miss_compile_time_ms)
        ));
    }
    lines.push(format!("| Total crates | {} |", s.total_crates));
    lines.push(format!(
        "| Hits | {} (local: {}, prefetch: {}, remote: {}) |",
        total_hits, s.local_hits, s.prefetch_hits, s.remote_hits
    ));
    if s.dups > 0 {
        lines.push(format!("| Dups | {} |", s.dups));
    }
    lines.push(format!("| Misses | {} |", s.misses));
    if s.errors > 0 {
        lines.push(format!("| Errors | {} |", s.errors));
    }
    if s.passthroughs > 0 || s.skipped > 0 || s.probes > 0 {
        lines.push(format!(
            "| Passthroughs / skipped | {} |",
            format_bypass_summary(&report.bypass)
        ));
    }
    lines.push(String::new());

    // Timing table
    let total_ms = t.hit_time_ms + t.miss_time_ms;
    lines.push("#### Timing".to_string());
    lines.push("| Phase | Aggregate time | % of tracked wrapper time |".to_string());
    lines.push("|---|---|---|".to_string());
    let hit_pct = if total_ms > 0 {
        t.hit_time_ms as f64 / total_ms as f64 * 100.0
    } else {
        0.0
    };
    let miss_pct = if total_ms > 0 {
        t.miss_time_ms as f64 / total_ms as f64 * 100.0
    } else {
        0.0
    };
    lines.push(format!(
        "| Cache hits (wrapper overhead) | {} | {:.1}% |",
        format_duration_ms(t.hit_time_ms),
        hit_pct
    ));
    lines.push(format!(
        "| Compiles (wrapper total) | {} | {:.1}% |",
        format_duration_ms(t.miss_time_ms),
        miss_pct
    ));
    lines.push(String::new());

    // Network table
    if let Some(net) = &report.network {
        lines.push("#### Network".to_string());
        lines.push("| Metric | Value |".to_string());
        lines.push("|---|---|".to_string());
        lines.push(format!(
            "| Downloaded | {} ({} crates) |",
            format_bytes(net.bytes_down),
            net.downloads_ok
        ));
        lines.push(format!(
            "| Uploaded | {} ({} crates) |",
            format_bytes(net.bytes_up),
            net.uploads_ok
        ));
        lines.push(format!(
            "| Avg download time | {:.0}ms |",
            net.avg_download_ms
        ));
        lines.push(format!("| P95 download time | {}ms |", net.p95_download_ms));
        lines.push(format!(
            "| Throughput (network) | {:.1} MB/s |",
            net.network_throughput_mbps
        ));
        lines.push(format!(
            "| Throughput (body only) | {:.1} MB/s |",
            net.body_throughput_mbps
        ));
        lines.push(format!(
            "| Throughput (incl. restore) | {:.1} MB/s |",
            net.throughput_mbps
        ));
        if !net.dominant_download_phase.is_empty() && net.dominant_download_phase_ms > 0 {
            lines.push(format!(
                "| Dominant aggregate download phase | {} — {} ({:.1}%) |",
                net.dominant_download_phase,
                format_duration_ms(net.dominant_download_phase_ms),
                net.dominant_download_phase_pct
            ));
        }
        if net.compression_ratio > 0.0 {
            lines.push(format!(
                "| Compression ratio | {:.1}x ({} → {}) |",
                net.compression_ratio,
                format_bytes(net.original_bytes_down),
                format_bytes(net.bytes_down)
            ));
        }
        if net.total_semaphore_wait_ms > 0
            || net.total_head_ms > 0
            || net.total_decompress_ms > 0
            || net.total_extract_ms > 0
            || net.total_import_ms > 0
            || net.total_disk_io_ms > 0
        {
            lines.push(format!(
                "| Aggregate download phase time | wait {}ms, HEAD {}ms, request {}ms, body {}ms, decompress {}ms, extract {}ms, import {}ms, disk I/O {}ms |",
                net.total_semaphore_wait_ms,
                net.total_head_ms,
                net.total_request_ms,
                net.total_body_ms,
                net.total_decompress_ms,
                net.total_extract_ms,
                net.total_import_ms,
                net.total_disk_io_ms
            ));
        }
        if net.blobs_total > 0 {
            lines.push(format!(
                "| Blob dedup | {} / {} blobs already local ({:.0}% skipped) |",
                net.blobs_skipped,
                net.blobs_total,
                if net.blobs_total > 0 {
                    net.blobs_skipped as f64 / net.blobs_total as f64 * 100.0
                } else {
                    0.0
                }
            ));
        }
        if net.downloads_failed > 0 {
            lines.push(format!("| Failed downloads | {} |", net.downloads_failed));
        }
        if net.uploads_failed > 0 {
            lines.push(format!("| Failed uploads | {} |", net.uploads_failed));
        }
        lines.push(String::new());

        // Slowest downloads
        if !net.slowest_downloads.is_empty() {
            lines.push("#### Slowest Downloads".to_string());
            lines.push(
                "| Crate | Size | Time | Key | Wait/HEAD | Req/Body | Extract/Import |".to_string(),
            );
            lines.push("|---|---|---|---|---|---|---|".to_string());
            for d in &net.slowest_downloads {
                let key = if d.cache_key.is_empty() {
                    "?"
                } else {
                    &d.cache_key[..d.cache_key.len().min(12)]
                };
                lines.push(format!(
                    "| `{}` | {} | {}ms | `{}` | {}/{}ms | {}/{}ms | {}/{}ms |",
                    d.crate_name,
                    format_bytes(d.compressed_bytes),
                    d.elapsed_ms,
                    key,
                    d.semaphore_wait_ms,
                    d.head_ms,
                    d.request_ms,
                    d.body_ms,
                    d.extract_ms.max(d.decompress_ms),
                    d.import_ms,
                ));
            }
            let repro_keys: Vec<_> = net
                .slowest_downloads
                .iter()
                .filter(|d| !d.object_key.is_empty())
                .take(3)
                .collect();
            if !repro_keys.is_empty() {
                lines.push(String::new());
                lines.push("Raw object keys for reproduction:".to_string());
                for d in repro_keys {
                    lines.push(format!("- `{}`: `{}`", d.crate_name, d.object_key));
                }
            }
            lines.push(String::new());
        }
    }

    if has_storage_data(&report.storage) {
        lines.push("#### Storage".to_string());
        push_storage_table(&mut lines, &report.storage);
        lines.push(String::new());
    }

    // Prefetch
    let p = &report.prefetch;
    lines.push("#### Prefetch".to_string());
    lines.push("| Metric | Value |".to_string());
    lines.push("|---|---|".to_string());
    lines.push(format!(
        "| Prefetch hits | {} / {} total hits |",
        p.prefetch_hits, p.total_hits
    ));
    lines.push(format!("| Contribution | {:.1}% |", p.contribution_pct));
    lines.push(String::new());

    if bypass_total(&report.bypass) > 0 {
        lines.push("#### Passthroughs & Skips".to_string());
        push_bypass_tables(&mut lines, &report.bypass);
        lines.push(String::new());
    }

    if !report.errors_detail.is_empty() {
        lines.push("#### Errors".to_string());
        push_error_table(&mut lines, &report.errors_detail);
        lines.push(String::new());
    }

    // Top compiled cache-key misses (dups + misses)
    if !report.top_misses.is_empty() {
        lines.push("#### Top Compiled Cache-Key Misses".to_string());
        lines.push("| Crate | Compile time | Size | Key |".to_string());
        lines.push("|---|---|---|---|".to_string());
        for c in &report.top_misses {
            let key_short = if c.cache_key.len() > 12 {
                &c.cache_key[..12]
            } else {
                &c.cache_key
            };
            lines.push(format!(
                "| `{}` | {} | {} | `{}` |",
                c.crate_name,
                format_duration_ms(c.compile_time_ms),
                format_bytes(c.size),
                key_short,
            ));
        }
        lines.push(String::new());
    }

    // Top cache hits
    if !report.top_hits.is_empty() {
        lines.push("#### Top Cache Hits (most expensive cached)".to_string());
        lines.push("| Crate | Compile cost | Size | Key |".to_string());
        lines.push("|---|---|---|---|".to_string());
        for c in &report.top_hits {
            let key_short = if c.cache_key.len() > 12 {
                &c.cache_key[..12]
            } else {
                &c.cache_key
            };
            lines.push(format!(
                "| `{}` | {} | {} | `{}` |",
                c.crate_name,
                format_duration_ms(c.compile_time_ms),
                format_bytes(c.size),
                key_short,
            ));
        }
        lines.push(String::new());
    }

    // Suggestions
    if !report.suggestions.is_empty() {
        lines.push("#### Suggestions".to_string());
        for s in &report.suggestions {
            lines.push(format!("- {s}"));
        }
        lines.push(String::new());
    }

    // GC
    if let Some(gc) = &report.gc {
        lines.push("#### GC".to_string());
        lines.push("| Metric | Value |".to_string());
        lines.push("|---|---|".to_string());
        lines.push(format!("| Last run | {} |", gc.last_run));
        lines.push(format!("| Entries evicted | {} |", gc.entries_evicted));
        lines.push(format!(
            "| Bytes freed | {} |",
            format_bytes(gc.bytes_freed)
        ));
        lines.push(format!("| Blobs removed | {} |", gc.blobs_removed));
        lines.push(String::new());
    }

    lines.join("\n")
}

/// GitHub-optimized markdown: compact key metrics always visible, details in collapsible sections.
/// Designed to be posted directly as a PR comment by kache-action.
pub fn format_github(report: &BuildReport) -> String {
    use crate::cli::format_duration_ms;

    let mut lines = Vec::new();
    let s = &report.summary;
    let t = &report.timing;
    let total_hits = s.local_hits + s.prefetch_hits + s.remote_hits;
    let total_compiled = s.dups + s.misses;

    // Header
    lines.push("### kache build cache".to_string());
    lines.push(String::new());
    lines.push(format!(
        "**{:.1}%** hit rate — {}/{} cacheable crates from cache, {} compiled | **{} compile work avoided**",
        s.hit_rate_pct,
        total_hits,
        s.total_crates,
        total_compiled,
        format_duration_ms(s.time_saved_ms),
    ));
    lines.push(String::new());

    // ── Key metrics (always visible) ──
    lines.push("| | |".to_string());
    lines.push("|---|---|".to_string());
    lines.push(format!(
        "| **Window** | last {}h |",
        report.meta.since_hours
    ));
    lines.push(format!(
        "| **Crates** | {} cached / {} compiled / {} total |",
        total_hits, total_compiled, s.total_crates
    ));
    if s.dups > 0 {
        lines.push(format!(
            "| **Dups** | {} storage duplicates after compile |",
            s.dups
        ));
    }
    lines.push(format!(
        "| **Hit rate** | {:.1}% count{} |",
        s.hit_rate_pct,
        s.weighted_hit_rate_pct
            .map(|w| format!(" / {:.1}% by compile cost", w))
            .unwrap_or_default()
    ));
    lines.push(format!(
        "| **Compile work avoided** | {} aggregate |",
        format_duration_ms(s.time_saved_ms)
    ));
    if let Some(overhead) = cache_overhead_summary(report) {
        lines.push(format!("| **Cache hit overhead** | {} |", overhead));
    }
    if let Some(roi) = cache_roi(report) {
        lines.push(format!(
            "| **Cache ROI** | {:.1}x compile work per cache-hit overhead |",
            roi
        ));
    }
    if t.miss_compile_time_ms > 0 {
        lines.push(format!(
            "| **Miss compile work** | {} aggregate |",
            format_duration_ms(t.miss_compile_time_ms)
        ));
    }
    if s.errors > 0 {
        lines.push(format!("| **Errors** | {} |", s.errors));
    }
    if s.passthroughs > 0 || s.skipped > 0 || s.probes > 0 {
        lines.push(format!(
            "| **Passthroughs / skipped** | {} |",
            format_bypass_summary(&report.bypass)
        ));
    }

    // ── Suggestions (always visible — actionable) ──
    if !report.suggestions.is_empty() {
        lines.push(String::new());
        for sg in &report.suggestions {
            lines.push(format!("> {sg}"));
        }
    }

    // ── Top cache-key misses (collapsed) ──
    if !report.top_misses.is_empty() {
        lines.push(String::new());
        lines.push("<details>".to_string());
        lines.push(format!(
            "<summary><strong>Top compiled cache-key misses</strong> ({} compiled)</summary>",
            total_compiled
        ));
        lines.push(String::new());
        lines.push("| Crate | Compile time | Size |".to_string());
        lines.push("|-------|-------------|------|".to_string());
        for c in report.top_misses.iter().take(10) {
            lines.push(format!(
                "| `{}` | {} | {} |",
                c.crate_name,
                format_duration_ms(c.compile_time_ms),
                format_bytes(c.size),
            ));
        }
        if total_compiled > 10 {
            lines.push(format!("| *... {} more* | | |", total_compiled - 10));
        }
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── Top hits (collapsed) ──
    if !report.top_hits.is_empty() {
        lines.push(String::new());
        lines.push("<details>".to_string());
        lines.push(format!(
            "<summary><strong>Expensive cache hits</strong> — top {} by avoided compile work</summary>",
            report.top_hits.len().min(10)
        ));
        lines.push(String::new());
        lines.push("| Crate | Avoided compile work | Size |".to_string());
        lines.push("|-------|----------------------|------|".to_string());
        for c in report.top_hits.iter().take(10) {
            lines.push(format!(
                "| `{}` | {} | {} |",
                markdown_cell(&c.crate_name),
                format_duration_ms(c.compile_time_ms),
                format_bytes(c.size),
            ));
        }
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── Passthroughs / skipped (collapsed) ──
    if bypass_total(&report.bypass) > 0 {
        lines.push(String::new());
        lines.push("<details>".to_string());
        lines.push(format!(
            "<summary><strong>Passthroughs & skips</strong> — {}</summary>",
            format_bypass_summary(&report.bypass)
        ));
        lines.push(String::new());
        push_bypass_tables(&mut lines, &report.bypass);
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── Errors (collapsed) ──
    if !report.errors_detail.is_empty() {
        lines.push(String::new());
        lines.push("<details>".to_string());
        lines.push(format!(
            "<summary><strong>Errors</strong> — {}</summary>",
            report.errors_detail.len()
        ));
        lines.push(String::new());
        push_error_table(&mut lines, &report.errors_detail);
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── Network (collapsed) ──
    if let Some(net) = &report.network {
        let net_tp = if net.network_throughput_mbps > 0.0 {
            net.network_throughput_mbps
        } else {
            net.throughput_mbps
        };

        lines.push(String::new());
        lines.push("<details>".to_string());
        let dominant_summary =
            if !net.dominant_download_phase.is_empty() && net.dominant_download_phase_ms > 0 {
                format!(", dominant aggregate {}", net.dominant_download_phase)
            } else {
                String::new()
            };
        lines.push(format!(
            "<summary><strong>Network</strong> — {} downloaded, {:.0} MB/s body{}</summary>",
            format_bytes(net.bytes_down),
            net.body_throughput_mbps,
            dominant_summary
        ));
        lines.push(String::new());
        lines.push("| | |".to_string());
        lines.push("|---|---|".to_string());
        lines.push(format!(
            "| Downloaded | {} ({} crates) |",
            format_bytes(net.bytes_down),
            net.downloads_ok
        ));
        if net.uploads_ok > 0 || net.uploads_failed > 0 {
            lines.push(format!(
                "| Uploaded | {} ({} crates) |",
                format_bytes(net.bytes_up),
                net.uploads_ok
            ));
            if net.total_compression_ms > 0 || net.total_head_checks_ms > 0 {
                lines.push(format!(
                    "| Upload time split | compress {}ms + HEAD checks {}ms |",
                    net.total_compression_ms, net.total_head_checks_ms,
                ));
            }
        }
        lines.push(format!(
            "| Download time | avg {:.0}ms · p95 {}ms |",
            net.avg_download_ms, net.p95_download_ms
        ));
        if net.v1_downloads > 0
            || net.v2_downloads > 0
            || net.v3_downloads > 0
            || net.unknown_format_downloads > 0
        {
            lines.push(format!(
                "| Download format | v1 {} · v2 {} · v3 {} · unknown {} |",
                net.v1_downloads, net.v2_downloads, net.v3_downloads, net.unknown_format_downloads
            ));
        }
        if net.total_get_requests > 0 {
            let req_per_download = net.total_get_requests as f64 / net.downloads_ok.max(1) as f64;
            lines.push(format!(
                "| GET fan-out | {} GETs total · {:.1} per download |",
                net.total_get_requests, req_per_download
            ));
        }
        lines.push(format!(
            "| Throughput | {:.1} MB/s body · {:.1} MB/s request+body · {:.1} MB/s end-to-end |",
            net.body_throughput_mbps, net_tp, net.throughput_mbps
        ));
        if !net.dominant_download_phase.is_empty() && net.dominant_download_phase_ms > 0 {
            lines.push(format!(
                "| Dominant aggregate download phase | {} — {} ({:.1}%) |",
                net.dominant_download_phase,
                format_duration_ms(net.dominant_download_phase_ms),
                net.dominant_download_phase_pct
            ));
        }
        if net.compression_ratio > 0.0 {
            lines.push(format!(
                "| Compression | {:.1}x ({} → {}) |",
                net.compression_ratio,
                format_bytes(net.original_bytes_down),
                format_bytes(net.bytes_down)
            ));
        }
        if net.total_semaphore_wait_ms > 0
            || net.total_head_ms > 0
            || net.total_decompress_ms > 0
            || net.total_extract_ms > 0
            || net.total_import_ms > 0
            || net.total_disk_io_ms > 0
        {
            lines.push(format!(
                "| Aggregate download phase time | wait {}ms · HEAD {}ms · request {}ms · body {}ms · decompress {}ms · extract {}ms · import {}ms · disk {}ms |",
                net.total_semaphore_wait_ms,
                net.total_head_ms,
                net.total_request_ms,
                net.total_body_ms,
                net.total_decompress_ms,
                net.total_extract_ms,
                net.total_import_ms,
                net.total_disk_io_ms
            ));
        }
        if net.blobs_total > 0 {
            let pct = if net.blobs_total > 0 {
                net.blobs_skipped as f64 / net.blobs_total as f64 * 100.0
            } else {
                0.0
            };
            lines.push(format!(
                "| Blob dedup | {}/{} already local ({:.0}% saved) |",
                net.blobs_skipped, net.blobs_total, pct
            ));
        }
        if net.downloads_failed > 0 {
            lines.push(format!("| Failed downloads | {} |", net.downloads_failed));
        }
        if net.uploads_failed > 0 {
            lines.push(format!("| Failed uploads | {} |", net.uploads_failed));
        }

        // Slowest downloads sub-table
        if !net.slowest_downloads.is_empty() {
            lines.push(String::new());
            lines.push("**Slowest downloads:**".to_string());
            lines.push(String::new());
            lines.push(
                "| Crate | Fmt | Size | Time | GETs | Key | Wait/HEAD | Req/Body | Extract/Import |"
                    .to_string(),
            );
            lines.push(
                "|-------|-----|------|------|------|-----|-----------|----------|----------------|"
                    .to_string(),
            );
            for d in net.slowest_downloads.iter().take(5) {
                let key = if d.cache_key.is_empty() {
                    "?"
                } else {
                    &d.cache_key[..d.cache_key.len().min(12)]
                };
                lines.push(format!(
                    "| `{}` | {} | {} | {}ms | {} | `{}` | {}/{}ms | {}/{}ms | {}/{}ms |",
                    d.crate_name,
                    if d.format.is_empty() { "?" } else { &d.format },
                    format_bytes(d.compressed_bytes),
                    d.elapsed_ms,
                    d.request_count,
                    key,
                    d.semaphore_wait_ms,
                    d.head_ms,
                    d.request_ms,
                    d.body_ms,
                    d.extract_ms.max(d.decompress_ms),
                    d.import_ms,
                ));
            }
            let repro_keys: Vec<_> = net
                .slowest_downloads
                .iter()
                .filter(|d| !d.object_key.is_empty())
                .take(3)
                .collect();
            if !repro_keys.is_empty() {
                lines.push(String::new());
                lines.push("Raw object keys for reproduction:".to_string());
                for d in repro_keys {
                    lines.push(format!("- `{}`: `{}`", d.crate_name, d.object_key));
                }
            }
        }
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── Storage (collapsed) ──
    if has_storage_data(&report.storage) {
        lines.push(String::new());
        lines.push("<details>".to_string());
        let storage_summary = if report.storage.restored_bytes > 0 {
            format!(
                "{:.1}% zero-copy restores, {} restored",
                report.storage.zero_copy_pct,
                format_bytes(report.storage.restored_bytes)
            )
        } else {
            format!(
                "{} logical, {} blobs",
                format_bytes(report.storage.logical_bytes),
                format_bytes(report.storage.blob_bytes)
            )
        };
        lines.push(format!(
            "<summary><strong>Storage</strong> — {}</summary>",
            storage_summary
        ));
        lines.push(String::new());
        push_storage_table(&mut lines, &report.storage);
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── Timing & Prefetch (collapsed) ──
    let total_ms = t.hit_time_ms + t.miss_time_ms;
    let p = &report.prefetch;
    if total_ms > 0 || p.total_hits > 0 {
        lines.push(String::new());
        lines.push("<details>".to_string());
        lines.push("<summary><strong>Timing & Prefetch</strong></summary>".to_string());
        lines.push(String::new());
        if total_ms > 0 {
            let hit_pct = t.hit_time_ms as f64 / total_ms as f64 * 100.0;
            let miss_pct = t.miss_time_ms as f64 / total_ms as f64 * 100.0;
            lines.push("| Phase | Aggregate time | % of tracked wrapper time |".to_string());
            lines.push("|-------|------|---|".to_string());
            lines.push(format!(
                "| Cache hits (wrapper overhead) | {} | {:.1}% |",
                format_duration_ms(t.hit_time_ms),
                hit_pct
            ));
            lines.push(format!(
                "| Compiles (wrapper total) | {} | {:.1}% |",
                format_duration_ms(t.miss_time_ms),
                miss_pct
            ));
        }
        // Per-crate timing breakdown
        if t.total_key_ms > 0 || t.total_lookup_ms > 0 || t.total_restore_ms > 0 {
            lines.push(format!(
                "| Hit overhead | avg {:.0}ms key + {:.0}ms lookup + {:.0}ms restore |",
                t.avg_key_ms, t.avg_lookup_ms, t.avg_restore_ms
            ));
        }
        if t.total_store_ms > 0 {
            lines.push(format!(
                "| Miss overhead | avg {:.0}ms key + {:.0}ms lookup + {:.0}ms store |",
                t.avg_key_ms, t.avg_lookup_ms, t.avg_store_ms
            ));
        }
        if p.total_hits > 0 {
            lines.push(String::new());
            lines.push(format!(
                "**Prefetch:** {}/{} hits ({:.1}%)",
                p.prefetch_hits, p.total_hits, p.contribution_pct
            ));
        }
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── GC (collapsed, only if GC ran recently) ──
    if let Some(gc) = &report.gc {
        lines.push(String::new());
        lines.push("<details>".to_string());
        lines.push(format!(
            "<summary><strong>GC</strong> — {} entries evicted, {} freed</summary>",
            gc.entries_evicted,
            format_bytes(gc.bytes_freed),
        ));
        lines.push(String::new());
        lines.push("| | |".to_string());
        lines.push("|---|---|".to_string());
        lines.push(format!("| Last run | {} |", gc.last_run));
        lines.push(format!("| Entries evicted | {} |", gc.entries_evicted));
        lines.push(format!(
            "| Bytes freed | {} |",
            format_bytes(gc.bytes_freed)
        ));
        lines.push(format!("| Blobs removed | {} |", gc.blobs_removed));
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    lines.push(String::new());
    lines.push(format!(
        "*Posted by [kache-action](https://github.com/kunobi-ninja/kache-action) · kache v{} · last {}h*",
        report.meta.kache_version,
        report.meta.since_hours,
    ));

    lines.join("\n")
}

pub fn format_text(report: &BuildReport) -> String {
    use crate::cli::format_duration_ms;

    let mut lines = Vec::new();
    let s = &report.summary;
    let t = &report.timing;
    let total_hits = s.local_hits + s.prefetch_hits + s.remote_hits;
    let total_compiled = s.dups + s.misses;

    lines.push(format!(
        "kache build report (last {}h)",
        report.meta.since_hours
    ));
    lines.push(format!(
        "  {:.1}% hit rate — {}/{} cacheable crates cached, {} compiled",
        s.hit_rate_pct, total_hits, s.total_crates, total_compiled,
    ));
    if s.dups > 0 {
        lines.push(format!(
            "  Dups: {} storage duplicates after compile",
            s.dups
        ));
    }
    if let Some(w) = s.weighted_hit_rate_pct {
        lines.push(format!("  {:.1}% by compile cost", w));
    }
    lines.push(format!(
        "  Compile work avoided: {} aggregate",
        format_duration_ms(s.time_saved_ms)
    ));
    if let Some(overhead) = cache_overhead_summary(report) {
        lines.push(format!("  Cache hit overhead: {overhead}"));
    }
    if let Some(roi) = cache_roi(report) {
        lines.push(format!(
            "  Cache ROI: {:.1}x compile work per cache-hit overhead",
            roi
        ));
    }
    if t.miss_compile_time_ms > 0 {
        lines.push(format!(
            "  Miss compile work: {} aggregate",
            format_duration_ms(t.miss_compile_time_ms)
        ));
    }
    if s.errors > 0 {
        lines.push(format!("  Errors: {}", s.errors));
    }
    if s.passthroughs > 0 || s.skipped > 0 || s.probes > 0 {
        lines.push(format!(
            "  Passthroughs/skipped: {}",
            format_bypass_summary(&report.bypass)
        ));
    }
    lines.push(String::new());

    // Timing
    lines.push("Timing:".to_string());
    lines.push(format!(
        "  Hits overhead: {} aggregate (avg {:.0}ms/hit)",
        format_duration_ms(t.hit_time_ms),
        t.avg_hit_ms
    ));
    lines.push(format!(
        "  Compiles: {} (avg {:.0}ms/crate)",
        format_duration_ms(t.miss_time_ms),
        t.avg_miss_ms
    ));
    if t.total_key_ms > 0 || t.total_lookup_ms > 0 || t.total_restore_ms > 0 {
        lines.push(format!(
            "  Hit overhead: avg {:.0}ms key + {:.0}ms lookup + {:.0}ms restore",
            t.avg_key_ms, t.avg_lookup_ms, t.avg_restore_ms
        ));
    }
    if t.total_store_ms > 0 {
        lines.push(format!(
            "  Miss overhead: avg {:.0}ms key + {:.0}ms lookup + {:.0}ms store",
            t.avg_key_ms, t.avg_lookup_ms, t.avg_store_ms
        ));
    }
    lines.push(String::new());

    // Network
    if let Some(net) = &report.network {
        lines.push("Network:".to_string());
        lines.push(format!(
            "  Downloaded: {} ({} ok, {} failed)",
            format_bytes(net.bytes_down),
            net.downloads_ok,
            net.downloads_failed
        ));
        lines.push(format!(
            "  Uploaded: {} ({} ok, {} failed)",
            format_bytes(net.bytes_up),
            net.uploads_ok,
            net.uploads_failed
        ));
        lines.push(format!(
            "  Latency: avg {:.0}ms, p95 {}ms, max {}ms",
            net.avg_download_ms, net.p95_download_ms, net.max_download_ms
        ));
        lines.push(format!(
            "  Throughput: {:.1} MB/s body, {:.1} MB/s request+body, {:.1} MB/s incl. restore",
            net.body_throughput_mbps, net.network_throughput_mbps, net.throughput_mbps
        ));
        if !net.dominant_download_phase.is_empty() && net.dominant_download_phase_ms > 0 {
            lines.push(format!(
                "  Dominant aggregate phase: {} — {} ({:.1}%)",
                net.dominant_download_phase,
                format_duration_ms(net.dominant_download_phase_ms),
                net.dominant_download_phase_pct
            ));
        }
        if net.compression_ratio > 0.0 {
            lines.push(format!(
                "  Compression: {:.1}x ratio ({} → {})",
                net.compression_ratio,
                format_bytes(net.original_bytes_down),
                format_bytes(net.bytes_down)
            ));
        }
        if net.total_semaphore_wait_ms > 0
            || net.total_head_ms > 0
            || net.total_decompress_ms > 0
            || net.total_extract_ms > 0
            || net.total_import_ms > 0
            || net.total_disk_io_ms > 0
        {
            lines.push(format!(
                "  Aggregate phase time: wait {}ms, HEAD {}ms, request {}ms, body {}ms, decompress {}ms, extract {}ms, import {}ms, disk I/O {}ms",
                net.total_semaphore_wait_ms,
                net.total_head_ms,
                net.total_request_ms,
                net.total_body_ms,
                net.total_decompress_ms,
                net.total_extract_ms,
                net.total_import_ms,
                net.total_disk_io_ms
            ));
        }
        if net.blobs_total > 0 {
            lines.push(format!(
                "  Blob dedup: {}/{} already local ({:.0}% skipped)",
                net.blobs_skipped,
                net.blobs_total,
                net.blobs_skipped as f64 / net.blobs_total.max(1) as f64 * 100.0
            ));
        }
        lines.push(String::new());
    }

    if has_storage_data(&report.storage) {
        lines.push("Storage:".to_string());
        if report.storage.restored_bytes > 0 {
            lines.push(format!(
                "  Restored: {} ({:.1}% zero-copy, {} copied)",
                format_bytes(report.storage.restored_bytes),
                report.storage.zero_copy_pct,
                format_bytes(report.storage.copied_bytes)
            ));
        }
        if report.storage.logical_bytes > 0 || report.storage.blob_bytes > 0 {
            lines.push(format!(
                "  Store: {} logical -> {} blobs ({} dedup saved)",
                format_bytes(report.storage.logical_bytes),
                format_bytes(report.storage.blob_bytes),
                format_bytes(report.storage.dedup_saved_bytes)
            ));
        }
        lines.push(String::new());
    }

    // Prefetch
    lines.push(format!(
        "Prefetch: {} / {} hits ({:.1}%)",
        report.prefetch.prefetch_hits, report.prefetch.total_hits, report.prefetch.contribution_pct
    ));
    lines.push(String::new());

    if bypass_total(&report.bypass) > 0 {
        lines.push("Passthroughs/skips:".to_string());
        for reason in &report.bypass.reasons {
            lines.push(format!(
                "  {} via {}: {} ({} total, {} failed, max {})",
                reason.result,
                reason.route,
                reason.reason,
                reason.count,
                reason.failures,
                format_duration_ms(reason.max_elapsed_ms)
            ));
        }
        if !report.bypass.slowest.is_empty() {
            lines.push("  Slowest:".to_string());
            for detail in &report.bypass.slowest {
                lines.push(format!(
                    "    {} — {} via {}, {}, exit {}, {}",
                    detail.crate_name,
                    detail.result,
                    detail.route,
                    format_duration_ms(detail.elapsed_ms),
                    format_exit_code(detail.exit_code),
                    detail.reason
                ));
            }
        }
        lines.push(String::new());
    }

    // Top compiled cache-key misses
    if !report.top_misses.is_empty() {
        lines.push("Top compiled cache-key misses:".to_string());
        for c in &report.top_misses {
            lines.push(format!(
                "  {} — {} ({})",
                c.crate_name,
                format_duration_ms(c.compile_time_ms),
                format_bytes(c.size),
            ));
        }
        lines.push(String::new());
    }

    if !report.top_hits.is_empty() {
        lines.push("Expensive cache hits:".to_string());
        for c in &report.top_hits {
            lines.push(format!(
                "  {} — {} avoided ({})",
                c.crate_name,
                format_duration_ms(c.compile_time_ms),
                format_bytes(c.size),
            ));
        }
        lines.push(String::new());
    }

    if !report.errors_detail.is_empty() {
        lines.push("Errors:".to_string());
        for err in report.errors_detail.iter().take(10) {
            lines.push(format!("  {} — {}", err.crate_name, err.timestamp));
        }
        lines.push(String::new());
    }

    // Suggestions
    if !report.suggestions.is_empty() {
        lines.push("Suggestions:".to_string());
        for s in &report.suggestions {
            lines.push(format!("  - {s}"));
        }
        lines.push(String::new());
    }

    // GC
    if let Some(gc) = &report.gc {
        lines.push("GC:".to_string());
        lines.push(format!("  Last run: {}", gc.last_run));
        lines.push(format!("  Entries evicted: {}", gc.entries_evicted));
        lines.push(format!("  Bytes freed: {}", format_bytes(gc.bytes_freed)));
        lines.push(format!("  Blobs removed: {}", gc.blobs_removed));
        lines.push(String::new());
    }

    lines.join("\n")
}

pub fn format_bytes(bytes: u64) -> String {
    let b = bytes as f64;
    if b >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1} GB", b / (1024.0 * 1024.0 * 1024.0))
    } else if b >= 1024.0 * 1024.0 {
        format!("{:.1} MB", b / (1024.0 * 1024.0))
    } else if b >= 1024.0 {
        format!("{:.1} KB", b / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn write_gc_stats(dir: &std::path::Path, last_run: chrono::DateTime<Utc>) {
        let persisted = format!(
            r#"{{"last_run":"{}","entries_evicted":3,"bytes_freed":4096,"blobs_removed":5,"duration_ms":12}}"#,
            last_run.to_rfc3339()
        );
        std::fs::write(dir.join("gc_stats.json"), persisted).unwrap();
    }

    #[test]
    fn load_gc_summary_returns_recent_run_within_window() {
        let dir = tempfile::tempdir().unwrap();
        write_gc_stats(dir.path(), Utc::now() - chrono::Duration::hours(1));
        let gc = load_gc_summary(dir.path(), 24).expect("recent gc run is within the 24h window");
        assert_eq!(gc.entries_evicted, 3);
        assert_eq!(gc.bytes_freed, 4096);
        assert_eq!(gc.blobs_removed, 5);
    }

    #[test]
    fn load_gc_summary_drops_run_older_than_window() {
        let dir = tempfile::tempdir().unwrap();
        write_gc_stats(dir.path(), Utc::now() - chrono::Duration::hours(48));
        assert!(
            load_gc_summary(dir.path(), 24).is_none(),
            "a run 48h ago must fall outside the 24h window"
        );
    }

    #[test]
    fn load_gc_summary_absent_when_no_stats_file() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_gc_summary(dir.path(), 24).is_none());
    }

    #[test]
    fn load_gc_summary_absent_on_malformed_stats_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("gc_stats.json"), b"not json").unwrap();
        assert!(load_gc_summary(dir.path(), 24).is_none());
    }

    fn test_event(
        crate_name: &str,
        result: EventResult,
        elapsed_ms: u64,
        compile_time_ms: u64,
        size: u64,
        cache_key: &str,
    ) -> BuildEvent {
        BuildEvent {
            ts: Utc::now(),
            crate_name: crate_name.to_string(),
            root: String::new(),
            version: "0.1.0".to_string(),
            result,
            elapsed_ms,
            compile_time_ms,
            size,
            cache_key: cache_key.to_string(),
            schema: 8,
            key_ms: 0,
            key_hash_hits: 0,
            key_hash_misses: 0,
            key_hash_bytes: 0,
            lookup_ms: 0,
            restore_ms: 0,
            store_ms: 0,
            store_output_blobs: 0,
            store_duplicate_blobs: 0,
            store_new_blobs: 0,
            compiler_runs: 0,
            preprocessor_runs: 0,
            probe_runs: 0,
            reflinked_bytes: 0,
            hardlinked_bytes: 0,
            copied_bytes: 0,
            store_reflinked_bytes: 0,
            store_copied_bytes: 0,
            passthrough_reason: String::new(),
            fallback: false,
            exit_code: None,
        }
    }

    fn test_transfer(
        crate_name: &str,
        direction: TransferDirection,
        format: &str,
        compressed_bytes: u64,
        elapsed_ms: u64,
        ok: bool,
    ) -> TransferEvent {
        TransferEvent {
            schema: 2,
            crate_name: crate_name.to_string(),
            direction,
            format: format.to_string(),
            cache_key: format!("{crate_name}-key"),
            object_key: format!("prefix/v3/packs/{crate_name}/{crate_name}-key.tar.zst"),
            compressed_bytes,
            elapsed_ms,
            network_ms: elapsed_ms / 2, // simulate network = half of total
            semaphore_wait_ms: 0,
            head_ms: 0,
            request_ms: elapsed_ms / 5,
            body_ms: elapsed_ms / 3,
            request_count: 4,
            original_bytes: compressed_bytes * 3, // simulate ~3x compression ratio
            decompress_ms: elapsed_ms / 4,        // simulate decompress = quarter of total
            extract_ms: 0,
            disk_io_ms: 0,
            import_ms: 0,
            compression_ms: 0,
            head_checks_ms: 0,
            blobs_skipped: 0,
            blobs_total: 2,
            ok,
            timestamp: Utc::now().timestamp() as u64,
        }
    }

    fn write_test_events(dir: &std::path::Path) -> Config {
        let root_a = dir.join("checkout-a");
        let root_b = dir.join("checkout-b");
        std::fs::create_dir_all(&root_a).unwrap();
        std::fs::create_dir_all(&root_b).unwrap();
        let root_a = root_a
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let root_b = root_b
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };

        // Write build events
        let mut passthrough = test_event("build.rs", EventResult::Passthrough, 250, 0, 0, "");
        passthrough.passthrough_reason = "refused: unsupported rustc invocation".to_string();
        passthrough.fallback = true;
        passthrough.exit_code = Some(0);

        let mut skipped = test_event("doc-test", EventResult::Skipped, 0, 0, 0, "");
        skipped.passthrough_reason = "explicitly skipped".to_string();

        let mut dup = test_event(
            "my_lib",
            EventResult::Dup,
            5000,
            4800,
            3 * 1024 * 1024,
            "def456789012",
        );
        dup.store_output_blobs = 1;
        dup.store_duplicate_blobs = 1;

        let mut events = vec![
            test_event(
                "serde",
                EventResult::LocalHit,
                5,
                300,
                1024 * 1024,
                "abc123def456",
            ),
            test_event(
                "tokio",
                EventResult::PrefetchHit,
                8,
                500,
                2 * 1024 * 1024,
                "bcd234",
            ),
            test_event(
                "regex",
                EventResult::RemoteHit,
                120,
                400,
                512 * 1024,
                "cde345",
            ),
            dup,
            test_event(
                "my_app",
                EventResult::Miss,
                8000,
                7500,
                5 * 1024 * 1024,
                "efg567",
            ),
            test_event("broken", EventResult::Error, 10, 0, 0, "err001"),
            passthrough,
            skipped,
        ];
        for e in &mut events {
            e.root = root_a.clone();
        }
        events[5].root = root_b;
        for e in &events {
            events::log_event(&config.event_log_path(), e).unwrap();
        }

        // Write transfer events
        let transfers = vec![
            test_transfer(
                "serde",
                TransferDirection::Download,
                "v3",
                500_000,
                150,
                true,
            ),
            test_transfer(
                "tokio",
                TransferDirection::Download,
                "v3",
                1_000_000,
                300,
                true,
            ),
            test_transfer(
                "regex",
                TransferDirection::Download,
                "v3",
                200_000,
                80,
                true,
            ),
            test_transfer(
                "my_lib",
                TransferDirection::Upload,
                "v3",
                2_000_000,
                500,
                true,
            ),
            test_transfer(
                "my_app",
                TransferDirection::Upload,
                "v3",
                3_000_000,
                700,
                true,
            ),
            test_transfer("fail_dl", TransferDirection::Download, "v3", 0, 50, false),
        ];
        for t in &transfers {
            events::log_transfer(&config.transfer_log_path(), t).unwrap();
        }

        config
    }

    #[test]
    fn test_generate_report_with_all_result_types() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let report = generate_report(&config, 24, 10).unwrap();

        assert_eq!(report.summary.total_crates, 5); // excludes errors from cacheable count
        assert_eq!(report.summary.local_hits, 1);
        assert_eq!(report.summary.prefetch_hits, 1);
        assert_eq!(report.summary.remote_hits, 1);
        assert_eq!(report.summary.dups, 1);
        assert_eq!(report.summary.misses, 1);
        assert_eq!(report.summary.errors, 1);
        assert_eq!(report.summary.passthroughs, 1);
        assert_eq!(report.summary.skipped, 1);
        assert_eq!(report.summary.fallbacks, 1);
        assert_eq!(report.bypass.reasons.len(), 2);
        assert_eq!(report.timeline.event_count, 8);
        assert_eq!(report.timeline.cacheable_count, 5);
        assert_eq!(report.timeline.hit_count, 3);
        assert_eq!(report.timeline.compiled_count, 2);
        assert_eq!(report.timeline.passthrough_count, 1);
        assert_eq!(report.timeline.skipped_count, 1);
        assert_eq!(report.timeline.error_count, 1);
        assert!(report.timeline.duration_ms > 0);
        assert!(report.timeline.start_unix_ms.unwrap() <= report.timeline.end_unix_ms.unwrap());
        assert_eq!(report.trace_events.len(), 8);
        let serde_trace = report
            .trace_events
            .iter()
            .find(|event| event.args.crate_name == "serde")
            .unwrap();
        assert_eq!(serde_trace.cat, "kache");
        assert_eq!(serde_trace.ph, "X");
        // The display name carries the result label; the bare crate name stays
        // in args (#456).
        assert_eq!(serde_trace.name, "hit: serde");
        assert_eq!(serde_trace.cname.as_deref(), Some("good"));
        assert_eq!(serde_trace.dur, 5_000);
        assert_eq!(serde_trace.args.result, "local_hit");
        assert_eq!(serde_trace.args.cache_key, "abc123def456");
        assert_eq!(serde_trace.args.overhead_ms, 5);
        assert!(report.summary.hit_rate_pct > 0.0);
        assert!(report.summary.time_saved_ms > 0);
        let serde_event = report
            .all_events
            .iter()
            .find(|event| event.crate_name == "serde")
            .unwrap();
        assert!(!serde_event.start_time.is_empty());
        assert!(!serde_event.end_time.is_empty());
        assert_eq!(
            serde_event.end_unix_ms - serde_event.start_unix_ms,
            serde_event.elapsed_ms as i64
        );
        let passthrough_detail = report
            .bypass
            .slowest
            .iter()
            .find(|detail| detail.crate_name == "build.rs")
            .unwrap();
        assert_eq!(
            passthrough_detail.end_unix_ms - passthrough_detail.start_unix_ms,
            passthrough_detail.elapsed_ms as i64
        );
        let network = report.network.as_ref().unwrap();
        assert_eq!(network.v3_downloads, 3);
        assert_eq!(network.v2_downloads, 0);
        assert_eq!(network.total_get_requests, 12);
    }

    /// A probe / query (`category` == `not-a-compile`) must be counted as a
    /// probe, NOT a passthrough — `passthroughs` is the actionable "compiles
    /// we couldn't cache" signal, and a probe is not a compile. A real
    /// refusal (`unsupported|…`) stays a passthrough.
    #[test]
    fn probe_events_split_out_of_passthroughs() {
        let mut probe = test_event("rustc", EventResult::Passthrough, 5, 0, 0, "");
        probe.passthrough_reason = "not-a-compile|query / probe (--print, -vV)".to_string();

        let mut refusal = test_event("a.c", EventResult::Passthrough, 90, 0, 0, "");
        refusal.passthrough_reason = "unsupported|cc link mode — not yet".to_string();

        let events = vec![probe, refusal];

        let bypass = build_bypass_analysis(&events, 10);
        assert_eq!(bypass.probes, 1, "the query/probe must count as a probe");
        assert_eq!(
            bypass.passthroughs, 1,
            "only the real refusal stays a passthrough"
        );

        let timeline = build_report_timeline(&events);
        assert_eq!(timeline.probe_count, 1);
        assert_eq!(timeline.passthrough_count, 1);

        // The summary line labels probes distinctly so a clean build's probe
        // traffic doesn't read as a caching problem.
        let summary = format_bypass_summary(&bypass);
        assert!(summary.contains("1 probe"), "got: {summary}");
        assert!(summary.contains("1 passthrough"), "got: {summary}");
    }

    #[test]
    fn test_json_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let report = generate_report(&config, 24, 10).unwrap();

        let json = format_json(&report).unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(raw.get("traceEvents").is_some());
        assert!(raw.get("trace_events").is_none());
        assert_eq!(raw["displayTimeUnit"], "ms");

        let parsed: BuildReport = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.summary.total_crates, report.summary.total_crates);
        assert_eq!(parsed.summary.misses, report.summary.misses);
        assert_eq!(parsed.top_misses.len(), report.top_misses.len());
        assert_eq!(parsed.timeline.event_count, report.timeline.event_count);
        assert_eq!(parsed.trace_events.len(), report.trace_events.len());
        assert_eq!(
            parsed.trace_events[0].args.result,
            report.trace_events[0].args.result
        );
        assert_eq!(
            parsed.all_events[0].start_unix_ms,
            report.all_events[0].start_unix_ms
        );
        assert_eq!(parsed.all_events[0].end_time, report.all_events[0].end_time);
    }

    #[test]
    fn test_report_filters_by_root() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let root = dir.path().join("checkout-a");
        let root = root.canonicalize().unwrap();

        let report = generate_report_with_filter(
            &config,
            24,
            10,
            &ReportFilter {
                root: Some(root.clone()),
            },
        )
        .unwrap();

        let root = root.to_string_lossy().into_owned();
        assert_eq!(report.meta.root_filter.as_deref(), Some(root.as_str()));
        assert_eq!(report.timeline.event_count, 7);
        assert_eq!(report.timeline.error_count, 0);
        assert!(report.network.is_none());
        assert!(
            report
                .suggestions
                .iter()
                .any(|s| s.contains("Network transfer data omitted"))
        );
        assert!(report.all_events.iter().all(|event| event.root == root));
        assert!(
            report
                .trace_events
                .iter()
                .all(|event| event.args.root == root)
        );
    }

    #[test]
    fn test_trace_json_format_is_minimal_chrome_trace_container() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let report = generate_report(&config, 24, 10).unwrap();

        let json = format_trace_json(&report).unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(raw.as_object().unwrap().len(), 2);
        assert_eq!(raw["displayTimeUnit"], "ms");
        assert!(raw.get("summary").is_none());
        assert!(raw.get("all_events").is_none());

        let arr = raw["traceEvents"].as_array().unwrap();
        // Metadata events (process_name + one thread_name per lane) are prepended
        // ahead of the rich `X` slices.
        let metadata: Vec<_> = arr.iter().filter(|e| e["ph"] == "M").collect();
        let slices: Vec<_> = arr.iter().filter(|e| e["ph"] == "X").collect();
        assert_eq!(slices.len(), report.trace_events.len());
        assert_eq!(arr[0]["name"], "process_name");
        assert_eq!(arr[0]["args"]["name"], "kache");
        assert!(
            metadata.iter().any(|e| e["name"] == "thread_name"),
            "lanes must be named via thread_name metadata"
        );
        // Result is surfaced in the slice name + color, not only in args.
        assert!(
            slices.iter().all(|e| {
                let n = e["name"].as_str().unwrap();
                n.starts_with("hit: ")
                    || n.starts_with("miss: ")
                    || n.starts_with("dup: ")
                    || n.starts_with("passthrough: ")
                    || n.starts_with("error: ")
                    || n.starts_with("skipped: ")
            }),
            "every slice name must carry its result label"
        );
        assert!(slices.iter().all(|e| e.get("cname").is_some()));
    }

    #[test]
    fn assign_trace_lanes_packs_concurrent_events() {
        // `event_start` = ts - elapsed_ms, so set ts (the end) to place each
        // slice on the timeline. Two overlapping events must get distinct lanes;
        // a later non-overlapping event reuses lane 0 instead of a fresh one.
        let base = Utc::now();
        let mk = |name: &str, result, key: &str, start_off_ms: i64, dur_ms: u64| {
            let mut e = test_event(name, result, dur_ms, 0, 0, key);
            e.ts = base + chrono::Duration::milliseconds(start_off_ms + dur_ms as i64);
            e
        };
        let a = mk("a", EventResult::LocalHit, "k1", 0, 100); // [0, 100)
        let b = mk("b", EventResult::Miss, "k2", 10, 100); // [10, 110), overlaps a
        let c = mk("c", EventResult::LocalHit, "k3", 200, 50); // [200, 250), after both

        let lanes = assign_trace_lanes(&[a, b, c]);
        assert_eq!(lanes[0], 0, "first event takes lane 0");
        assert_eq!(lanes[1], 1, "an overlapping event takes a fresh lane");
        assert_eq!(lanes[2], 0, "a non-overlapping later event reuses lane 0");
    }

    #[test]
    fn trace_result_style_distinguishes_hit_and_miss() {
        assert_eq!(trace_result_style(EventResult::LocalHit), ("hit", "good"));
        assert_eq!(trace_result_style(EventResult::RemoteHit), ("hit", "good"));
        assert_eq!(trace_result_style(EventResult::Miss), ("miss", "bad"));
        assert_eq!(
            trace_result_style(EventResult::Passthrough),
            ("passthrough", "grey")
        );
        // Hit and miss must not share a color.
        assert_ne!(
            trace_result_style(EventResult::LocalHit).1,
            trace_result_style(EventResult::Miss).1
        );
    }

    #[test]
    fn test_markdown_contains_sections() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let report = generate_report(&config, 24, 10).unwrap();

        let md = format_markdown(&report);
        assert!(md.contains("### kache build report"));
        assert!(md.contains("#### Summary"));
        assert!(md.contains("#### Timing"));
        assert!(md.contains("#### Network"));
        assert!(md.contains("#### Prefetch"));
        assert!(md.contains("#### Passthroughs & Skips"));
        assert!(md.contains("#### Top Compiled Cache-Key Misses"));
        assert!(md.contains("#### Suggestions"));
    }

    #[test]
    fn test_missing_transfer_data() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.path().to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };

        // Only write build events, no transfers
        let event = test_event("serde", EventResult::LocalHit, 5, 300, 1024, "abc");
        events::log_event(&config.event_log_path(), &event).unwrap();

        let report = generate_report(&config, 24, 10).unwrap();
        assert!(report.network.is_none());
        assert!(report.suggestions.iter().any(|s| s.contains("No network")));
    }

    #[test]
    fn test_suggestion_high_miss_share() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.path().to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };

        // Mostly misses — should trigger high miss share suggestion
        for i in 0..10 {
            let e = test_event(
                &format!("miss_{i}"),
                EventResult::Miss,
                5000,
                4500,
                1024 * 1024,
                &format!("key_{i}"),
            );
            events::log_event(&config.event_log_path(), &e).unwrap();
        }
        let hit = test_event("hit", EventResult::LocalHit, 5, 100, 1024, "hk");
        events::log_event(&config.event_log_path(), &hit).unwrap();

        let report = generate_report(&config, 24, 10).unwrap();
        assert!(
            report
                .suggestions
                .iter()
                .any(|s| s.contains("compile time spent on compiled cache-key misses"))
        );
    }

    #[test]
    fn test_suggestion_high_hit_overhead() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.path().to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };

        // Several hits, each with a high elapsed time -> avg overhead > 50ms
        // triggers the "cache hit overhead" suggestion.
        for i in 0..5 {
            let e = test_event(
                &format!("hit_{i}"),
                EventResult::LocalHit,
                300, // elapsed_ms (overhead)
                100,
                1024 * 1024,
                &format!("hk_{i}"),
            );
            events::log_event(&config.event_log_path(), &e).unwrap();
        }

        let report = generate_report(&config, 24, 10).unwrap();
        assert!(
            report
                .suggestions
                .iter()
                .any(|s| s.contains("cache hit overhead")),
            "expected hit-overhead suggestion: {:?}",
            report.suggestions
        );
    }

    #[test]
    fn test_suggestion_network_download_failures_and_fanout() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.path().to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };

        // One OK download (test_transfer sets request_count=4 -> 4 GETs for 1 hit,
        // > 3x, triggers the fan-out suggestion) plus two failed downloads
        // (>10% failure rate triggers the network-failure suggestion).
        let transfers = [
            test_transfer("ok_dl", TransferDirection::Download, "v3", 1000, 80, true),
            test_transfer("bad1", TransferDirection::Download, "v3", 0, 50, false),
            test_transfer("bad2", TransferDirection::Download, "v3", 0, 50, false),
        ];
        for t in &transfers {
            events::log_transfer(&config.transfer_log_path(), t).unwrap();
        }

        let report = generate_report(&config, 24, 10).unwrap();
        let joined = report.suggestions.join("\n");
        assert!(
            joined.contains("downloads failed"),
            "expected download-failure suggestion: {:?}",
            report.suggestions
        );
        assert!(
            joined.contains("GETs per cache hit"),
            "expected GET fan-out suggestion: {:?}",
            report.suggestions
        );
    }

    #[test]
    fn test_suggestion_network_latency_thresholds() {
        // A download with high semaphore-wait, request/header latency exceeding
        // body transfer, and extract time exceeding body transfer triggers the
        // three latency-threshold suggestions (report.rs 999-1018) that the
        // fixed-ratio test_transfer helper can't reach.
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.path().to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };

        let slow = TransferEvent {
            schema: 2,
            crate_name: "slow".to_string(),
            direction: TransferDirection::Download,
            format: "v3".to_string(),
            cache_key: "slow-key".to_string(),
            object_key: "prefix/v3/packs/slow/slow-key.tar.zst".to_string(),
            compressed_bytes: 1000,
            elapsed_ms: 80_000,
            network_ms: 40_000,
            semaphore_wait_ms: 11_000, // > 10s -> semaphore-wait suggestion
            head_ms: 0,
            request_ms: 31_000, // > 30s AND > body_ms -> request-latency suggestion
            body_ms: 1_000,
            request_count: 1,
            original_bytes: 3000,
            decompress_ms: 0,
            extract_ms: 31_000, // > 30s AND > body_ms -> extract-time suggestion
            disk_io_ms: 0,
            import_ms: 0,
            compression_ms: 0,
            head_checks_ms: 0,
            blobs_skipped: 0,
            blobs_total: 2,
            ok: true,
            timestamp: Utc::now().timestamp() as u64,
        };
        events::log_transfer(&config.transfer_log_path(), &slow).unwrap();

        let report = generate_report(&config, 24, 10).unwrap();
        let joined = report.suggestions.join("\n");
        assert!(
            joined.contains("semaphore wait"),
            "expected semaphore-wait suggestion: {:?}",
            report.suggestions
        );
        assert!(
            joined.contains("request/header latency"),
            "expected request-latency suggestion: {:?}",
            report.suggestions
        );
        assert!(
            joined.contains("archive extract time"),
            "expected extract-time suggestion: {:?}",
            report.suggestions
        );
    }

    #[test]
    fn test_github_format_has_collapsible_sections() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let report = generate_report(&config, 24, 10).unwrap();

        let gh = format_github(&report);
        assert!(gh.contains("### kache build cache"));
        assert!(gh.contains("kache-action"));
        // Key metrics always visible
        assert!(gh.contains("**Crates**"));
        assert!(gh.contains("**Hit rate**"));
        assert!(gh.contains("**Compile work avoided**"));
        assert!(gh.contains("**Cache hit overhead**"));
        assert!(gh.contains("**Cache ROI**"));
        assert!(gh.contains("**Passthroughs / skipped**"));
        // Details in collapsible sections
        assert!(gh.contains("<details>"));
        assert!(gh.contains("<summary><strong>Top compiled cache-key misses</strong>"));
        assert!(gh.contains("<summary><strong>Passthroughs & skips</strong>"));
        assert!(gh.contains("via fallback"));
        assert!(gh.contains("refused: unsupported rustc invocation"));
        assert!(gh.contains("<summary><strong>Network</strong>"));
        assert!(gh.contains("<summary><strong>Timing & Prefetch</strong>"));
        assert!(gh.contains("Download format"));
        assert!(gh.contains("GET fan-out"));
        assert!(gh.contains("v3 3"));
        assert!(gh.contains("request"));
        assert!(gh.contains("body"));
    }

    #[test]
    fn test_text_output() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let report = generate_report(&config, 24, 10).unwrap();

        let text = format_text(&report);
        assert!(text.contains("kache build report"));
        assert!(text.contains("hit rate"));
        assert!(text.contains("Timing:"));
        assert!(text.contains("Network:"));
        assert!(text.contains("Passthroughs/skips:"));
    }

    #[test]
    fn render_includes_storage_and_gc_sections_when_present() {
        // generate_report from synthetic events yields no storage/gc data, so
        // the has_storage_data and gc=Some render branches stay cold. Populate
        // them on a generated report and confirm all three formats render the
        // Storage and GC sections (markdown:1415/text:2111/github:1853 + GC).
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let mut report = generate_report(&config, 24, 10).unwrap();

        report.storage = StorageBreakdown {
            reflinked_bytes: 1024,
            hardlinked_bytes: 512,
            copied_bytes: 256,
            restored_bytes: 1792,
            zero_copy_pct: 85.7,
            store_blobs: 4,
            logical_bytes: 4096,
            blob_bytes: 2048,
            dedup_saved_bytes: 2048,
            store_reflinked_bytes: 0,
            store_copied_bytes: 0,
        };
        report.gc = Some(GcSummary {
            last_run: "2026-06-19T12:00:00+00:00".to_string(),
            entries_evicted: 7,
            bytes_freed: 9000,
            blobs_removed: 3,
        });

        for rendered in [format_markdown(&report), format_github(&report)] {
            assert!(rendered.contains("Storage"), "missing Storage section");
        }
        let text = format_text(&report);
        assert!(text.contains("Storage:"), "text missing Storage section");
        // GC summary surfaces its evicted-entry count in every format.
        for rendered in [
            format_markdown(&report),
            format_github(&report),
            format_text(&report),
        ] {
            let lower = rendered.to_lowercase();
            assert!(
                lower.contains("evicted") && lower.contains('7'),
                "GC section with evicted count should appear"
            );
        }
    }

    #[test]
    fn render_network_and_error_sections_with_all_optional_fields() {
        // Synthetic events from write_test_events yield a network section
        // without uploads, compression, blob-dedup, failures, the dominant
        // aggregate phase, or an error table — so those optional rows stay
        // cold in all three renderers. Populate a fully-loaded NetworkAnalysis
        // plus an errors_detail list on a generated report and confirm every
        // format surfaces the upload, compression, dedup, failure, dominant-
        // phase, aggregate-phase, GET-fan-out, and error-table branches.
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let mut report = generate_report(&config, 24, 10).unwrap();

        report.network = Some(NetworkAnalysis {
            bytes_up: 5 * 1024 * 1024,
            bytes_down: 20 * 1024 * 1024,
            uploads_ok: 4,
            uploads_failed: 2,
            downloads_ok: 10,
            downloads_failed: 3,
            avg_download_ms: 42.0,
            p95_download_ms: 90,
            max_download_ms: 120,
            throughput_mbps: 50.0,
            network_throughput_mbps: 60.0,
            body_throughput_mbps: 70.0,
            dominant_download_phase: "body".to_string(),
            dominant_download_phase_ms: 800,
            dominant_download_phase_pct: 55.5,
            total_request_ms: 100,
            total_body_ms: 800,
            total_semaphore_wait_ms: 30,
            total_head_ms: 40,
            total_get_requests: 25,
            compression_ratio: 3.2,
            original_bytes_down: 64 * 1024 * 1024,
            total_decompress_ms: 50,
            total_extract_ms: 60,
            total_disk_io_ms: 70,
            total_import_ms: 80,
            total_compression_ms: 15,
            total_head_checks_ms: 25,
            blobs_skipped: 6,
            blobs_total: 10,
            v1_downloads: 1,
            v2_downloads: 2,
            v3_downloads: 7,
            unknown_format_downloads: 1,
            slowest_downloads: vec![TransferDetail {
                crate_name: "serde".to_string(),
                direction: "download".to_string(),
                format: "v3".to_string(),
                cache_key: "abcdef0123456789deadbeef".to_string(),
                object_key: "rust/abc/serde".to_string(),
                compressed_bytes: 2 * 1024 * 1024,
                elapsed_ms: 120,
                network_ms: 100,
                semaphore_wait_ms: 5,
                head_ms: 6,
                request_ms: 7,
                body_ms: 80,
                decompress_ms: 9,
                extract_ms: 10,
                disk_io_ms: 11,
                import_ms: 12,
                request_count: 4,
                blobs_skipped: 2,
                blobs_total: 5,
                throughput_mbps: 40.0,
                ok: true,
            }],
        });
        report.errors_detail = vec![ErrorDetail {
            crate_name: "boom".to_string(),
            cache_key: "f00dcafef00dcafe".to_string(),
            timestamp: "2026-06-19T12:00:00+00:00".to_string(),
        }];

        for rendered in [
            format_markdown(&report),
            format_github(&report),
            format_text(&report),
        ] {
            let lower = rendered.to_lowercase();
            // Upload row (uploads_ok > 0) and its compress/HEAD split.
            assert!(
                lower.contains("upload"),
                "missing upload section: {rendered}"
            );
            // Compression ratio row (compression_ratio > 0).
            assert!(
                lower.contains("compress"),
                "missing compression row: {rendered}"
            );
            // Blob dedup row (blobs_total > 0).
            assert!(
                lower.contains("dedup"),
                "missing blob dedup row: {rendered}"
            );
            // The slowest download crate is listed.
            assert!(
                lower.contains("serde"),
                "missing slowest download: {rendered}"
            );
            // The error table lists the failing crate.
            assert!(lower.contains("boom"), "missing error entry: {rendered}");
        }
    }

    #[test]
    fn test_empty_report() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.path().to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };

        let report = generate_report(&config, 24, 10).unwrap();
        assert_eq!(report.summary.total_crates, 0);
        assert_eq!(report.summary.hit_rate_pct, 0.0);
        assert!(report.network.is_none());
        assert!(report.top_misses.is_empty());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn test_markdown_cell_escapes_pipes_and_newlines() {
        assert_eq!(markdown_cell("a|b"), "a\\|b");
        assert_eq!(markdown_cell("line1\nline2"), "line1 line2");
        assert_eq!(markdown_cell("plain"), "plain");
    }

    #[test]
    fn test_format_exit_code() {
        assert_eq!(format_exit_code(Some(0)), "0");
        assert_eq!(format_exit_code(Some(101)), "101");
        assert_eq!(format_exit_code(None), "-");
    }

    #[test]
    fn test_bypass_total_sums_passthroughs_probes_and_skipped() {
        let bypass = BypassAnalysis {
            passthroughs: 3,
            probes: 1,
            skipped: 2,
            ..Default::default()
        };
        assert_eq!(bypass_total(&bypass), 6);
        assert_eq!(bypass_total(&BypassAnalysis::default()), 0);
    }

    #[test]
    fn test_format_bypass_summary_variants() {
        assert_eq!(format_bypass_summary(&BypassAnalysis::default()), "none");

        // Singular vs plural, with fallback breakdown.
        let one = BypassAnalysis {
            passthroughs: 1,
            ..Default::default()
        };
        assert_eq!(format_bypass_summary(&one), "1 passthrough");

        let many = BypassAnalysis {
            passthroughs: 3,
            fallbacks: 2,
            skipped: 1,
            ..Default::default()
        };
        assert_eq!(
            format_bypass_summary(&many),
            "3 passthroughs (2 via fallback) / 1 skipped"
        );
    }

    #[test]
    fn test_bypass_route_reflects_result_and_fallback() {
        let mut e = test_event("c", EventResult::Passthrough, 1, 0, 0, "k");
        assert_eq!(bypass_route(&e), "direct");
        e.fallback = true;
        assert_eq!(bypass_route(&e), "fallback");
        e.result = EventResult::Skipped;
        assert_eq!(bypass_route(&e), "skipped");
        e.result = EventResult::Miss;
        assert_eq!(bypass_route(&e), "n/a");
    }

    #[test]
    fn test_bypass_reason_defaults_to_unknown() {
        let mut e = test_event("c", EventResult::Passthrough, 1, 0, 0, "k");
        assert_eq!(bypass_reason(&e), "unknown");
        e.passthrough_reason = "  linker invocation  ".to_string();
        assert_eq!(bypass_reason(&e), "linker invocation");
    }

    #[test]
    fn test_build_bypass_analysis_counts_groups_and_sorts() {
        let mut direct = test_event("a", EventResult::Passthrough, 30, 0, 0, "k");
        direct.passthrough_reason = "linker".to_string();

        let mut fallback = test_event("b", EventResult::Passthrough, 50, 0, 0, "k");
        fallback.passthrough_reason = "linker".to_string();
        fallback.fallback = true;

        let mut skipped = test_event("c", EventResult::Skipped, 5, 0, 0, "k");
        skipped.passthrough_reason = "disabled".to_string();

        // A non-bypass event must be ignored entirely.
        let miss = test_event("d", EventResult::Miss, 999, 0, 0, "k");

        let analysis = build_bypass_analysis(&[direct, fallback, skipped, miss], 10);

        assert_eq!(analysis.passthroughs, 2);
        assert_eq!(analysis.skipped, 1);
        assert_eq!(analysis.fallbacks, 1);
        assert_eq!(analysis.direct_passthroughs, 1);
        // "linker" appears under two different routes (direct + fallback), so it
        // groups into two reason rows; "disabled" is a third.
        assert_eq!(analysis.reasons.len(), 3);
        // Slowest-first ordering: the 50ms fallback leads.
        assert_eq!(analysis.slowest.first().unwrap().elapsed_ms, 50);
        assert!(analysis.slowest.iter().all(|d| d.crate_name != "d"));
    }

    #[test]
    fn test_build_network_analysis_aggregates_transfers() {
        let transfers = vec![
            test_transfer("serde", TransferDirection::Upload, "v3", 1_000, 40, true),
            test_transfer("tokio", TransferDirection::Upload, "v3", 0, 10, false), // failed
            test_transfer("regex", TransferDirection::Download, "v3", 2_000, 80, true),
            test_transfer("syn", TransferDirection::Download, "v3", 0, 5, false), // failed
        ];
        let na = build_network_analysis(&transfers, 10);
        assert_eq!(na.uploads_ok, 1);
        assert_eq!(na.uploads_failed, 1);
        assert_eq!(na.downloads_ok, 1);
        assert_eq!(na.downloads_failed, 1);
        assert_eq!(na.bytes_up, 1_000);
        assert_eq!(na.bytes_down, 2_000);
        assert!(na.max_download_ms >= 80);
    }

    #[test]
    fn test_push_storage_table_renders_rows() {
        let storage = StorageBreakdown {
            reflinked_bytes: 800,
            hardlinked_bytes: 150,
            copied_bytes: 50,
            restored_bytes: 1000,
            zero_copy_pct: 95.0,
            store_blobs: 12,
            logical_bytes: 5000,
            blob_bytes: 3000,
            dedup_saved_bytes: 2000,
            store_reflinked_bytes: 0,
            store_copied_bytes: 0,
        };
        let mut lines = Vec::new();
        push_storage_table(&mut lines, &storage);
        let joined = lines.join("\n");
        assert!(joined.contains("Restored bytes"));
        assert!(joined.contains("Zero-copy restores"));
        assert!(joined.contains("Store footprint"));
        assert!(joined.contains("Store blobs"));
    }

    #[test]
    fn report_counts_each_download_transfer_format() {
        // Downloads logged with v1 / v2 / unknown formats exercise the
        // format-match arms in the transfer aggregation (not just the v3 arm
        // the other tests use).
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir: dir.path().to_path_buf(),
            max_size: 1024,
            remote: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
        };
        for fmt in ["v1", "v2", "weird-future-format"] {
            let t = test_transfer(fmt, TransferDirection::Download, fmt, 500, 40, true);
            events::log_transfer(&config.transfer_log_path(), &t).unwrap();
        }

        // Must aggregate without panicking across all format arms.
        let report = generate_report(&config, 24, 10).unwrap();
        assert!(
            report.network.is_some(),
            "downloads should yield network analysis"
        );
    }

    #[test]
    fn push_storage_table_renders_store_ingest_line() {
        // Non-zero store ingest (reflinked + copied bytes from importing remote
        // entries) renders the "Store ingest" line with a reflink-share %.
        // Covers push_storage_table's ingest>0 branch.
        let storage = StorageBreakdown {
            reflinked_bytes: 0,
            hardlinked_bytes: 0,
            copied_bytes: 0,
            restored_bytes: 0,
            zero_copy_pct: 0.0,
            store_blobs: 4,
            logical_bytes: 4096,
            blob_bytes: 2048,
            dedup_saved_bytes: 0,
            store_reflinked_bytes: 3000,
            store_copied_bytes: 1000,
        };
        let mut lines = Vec::new();
        push_storage_table(&mut lines, &storage);
        let joined = lines.join("\n");
        assert!(joined.contains("Store ingest"), "got: {joined}");
        assert!(joined.contains("reflinked (CoW)"));
    }

    #[test]
    fn github_storage_summary_without_restores_shows_logical_and_blobs() {
        // When nothing was restored this run, the github Storage summary falls
        // back to the "{logical} logical, {blobs} blobs" form (the else arm).
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let mut report = generate_report(&config, 24, 10).unwrap();
        report.storage = StorageBreakdown {
            reflinked_bytes: 0,
            hardlinked_bytes: 0,
            copied_bytes: 0,
            restored_bytes: 0, // -> else branch
            zero_copy_pct: 0.0,
            store_blobs: 7,
            logical_bytes: 9000,
            blob_bytes: 5000,
            dedup_saved_bytes: 4000,
            store_reflinked_bytes: 0,
            store_copied_bytes: 0,
        };
        let gh = format_github(&report);
        assert!(
            gh.contains("logical") && gh.contains("blobs"),
            "summary: {gh}"
        );
    }

    #[test]
    fn test_push_error_table_truncates_at_ten() {
        let errors: Vec<ErrorDetail> = (0..12)
            .map(|i| ErrorDetail {
                crate_name: format!("crate{i}"),
                cache_key: "0123456789abcdef".to_string(),
                timestamp: "2025-01-01T00:00:00".to_string(),
            })
            .collect();
        let mut lines = Vec::new();
        push_error_table(&mut lines, &errors);
        let joined = lines.join("\n");
        assert!(joined.contains("| Crate | Time | Key |"));
        assert!(
            joined.contains("2 more"),
            "should note the overflow beyond 10"
        );
    }

    #[test]
    fn test_push_bypass_tables_renders_reasons_and_slowest() {
        let bypass = BypassAnalysis {
            passthroughs: 1,
            reasons: vec![BypassReason {
                result: "passthrough".to_string(),
                route: "direct".to_string(),
                reason: "linker".to_string(),
                count: 3,
                failures: 1,
                max_elapsed_ms: 1500,
            }],
            slowest: vec![BypassDetail {
                crate_name: "foo".to_string(),
                root: String::new(),
                result: "passthrough".to_string(),
                route: "direct".to_string(),
                reason: "linker".to_string(),
                start_time: String::new(),
                end_time: String::new(),
                start_unix_ms: 0,
                end_unix_ms: 0,
                elapsed_ms: 1500,
                exit_code: Some(0),
                timestamp: "2025-01-01T00:00:00".to_string(),
            }],
            ..Default::default()
        };
        let mut lines = Vec::new();
        push_bypass_tables(&mut lines, &bypass);
        let joined = lines.join("\n");
        assert!(joined.contains("| Result | Route | Reason"));
        assert!(joined.contains("Slowest bypassed invocations"));
        assert!(joined.contains("foo"));
    }

    #[test]
    fn test_build_network_analysis_empty_is_zeroed() {
        let na = build_network_analysis(&[], 10);
        assert_eq!(na.uploads_ok, 0);
        assert_eq!(na.downloads_ok, 0);
        assert_eq!(na.bytes_up, 0);
        assert_eq!(na.bytes_down, 0);
    }

    #[test]
    fn test_build_bypass_analysis_respects_top_limit() {
        let events: Vec<BuildEvent> = (0..5)
            .map(|i| {
                let mut e = test_event(&format!("c{i}"), EventResult::Passthrough, i, 0, 0, "k");
                e.passthrough_reason = format!("reason{i}");
                e
            })
            .collect();
        let analysis = build_bypass_analysis(&events, 2);
        assert_eq!(analysis.passthroughs, 5, "totals count all events");
        assert!(analysis.reasons.len() <= 2, "reasons truncated to top");
        assert!(analysis.slowest.len() <= 2, "slowest truncated to top");
    }
}
