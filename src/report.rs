use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};

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
    pub timing: TimingBreakdown,
    pub network: Option<NetworkAnalysis>,
    pub prefetch: PrefetchAnalysis,
    pub top_misses: Vec<CrateDetail>,
    pub top_hits: Vec<CrateDetail>,
    pub all_events: Vec<CrateDetail>,
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
    pub misses: usize,
    pub errors: usize,
    pub total_duration_ms: u64,
    /// Percentage of total compile time avoided by cache: time_saved / (time_saved + miss_compile_time).
    #[serde(default)]
    pub cache_efficiency_pct: f64,
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
    /// Throughput based on total wall-clock time (includes decompression + disk I/O).
    pub throughput_mbps: f64,
    /// Throughput based on network time only (S3 GET + body collection).
    pub network_throughput_mbps: f64,
    /// Time spent waiting for response headers across GET requests.
    #[serde(default)]
    pub total_request_ms: u64,
    /// Time spent reading response bodies across GET requests.
    #[serde(default)]
    pub total_body_ms: u64,
    /// Total number of GET requests issued for successful downloads.
    #[serde(default)]
    pub total_get_requests: u32,
    /// Compression ratio (original / compressed). 0 if no data.
    pub compression_ratio: f64,
    /// Total original (uncompressed) bytes downloaded.
    pub original_bytes_down: u64,
    /// Total time spent in zstd decompression (ms).
    pub total_decompress_ms: u64,
    /// Disk I/O time for downloads (directly measured when available), ms.
    pub total_disk_io_ms: u64,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrateDetail {
    pub crate_name: String,
    pub result: String,
    pub elapsed_ms: u64,
    pub compile_time_ms: u64,
    pub overhead_ms: u64,
    pub size: u64,
    pub cache_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferDetail {
    pub crate_name: String,
    pub direction: String,
    #[serde(default)]
    pub format: String,
    pub compressed_bytes: u64,
    pub elapsed_ms: u64,
    #[serde(default)]
    pub network_ms: u64,
    #[serde(default)]
    pub request_ms: u64,
    #[serde(default)]
    pub body_ms: u64,
    #[serde(default)]
    pub decompress_ms: u64,
    #[serde(default)]
    pub disk_io_ms: u64,
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

pub fn generate_report(config: &Config, hours: u64, top: usize) -> Result<BuildReport> {
    let since = Utc::now() - chrono::Duration::hours(hours as i64);
    let build_events = events::read_events_since(&config.event_log_path(), since)?;
    let since_ts = since.timestamp() as u64;
    let transfers =
        events::read_transfers_since(&config.transfer_log_path(), since_ts).unwrap_or_default();

    let stats = events::compute_stats(&build_events);
    let total_cacheable = stats.local_hits + stats.prefetch_hits + stats.remote_hits + stats.misses;
    let total_hits = stats.local_hits + stats.prefetch_hits + stats.remote_hits;

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
        .filter(|e| !matches!(e.result, EventResult::Skipped))
        .map(to_crate_detail)
        .collect();

    let mut misses: Vec<CrateDetail> = build_events
        .iter()
        .filter(|e| matches!(e.result, EventResult::Miss))
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
    let avg_miss_ms = if stats.misses > 0 {
        stats.miss_elapsed_ms as f64 / stats.misses as f64
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
        &misses,
        total_cacheable,
        total_hits,
    );

    Ok(BuildReport {
        meta: ReportMeta {
            kache_version: crate::VERSION.to_string(),
            generated_at: Utc::now().to_rfc3339(),
            since_hours: hours,
        },
        summary: ReportSummary {
            hit_rate_pct: (hit_rate * 10.0).round() / 10.0,
            weighted_hit_rate_pct: weighted.map(|w| (w * 10.0).round() / 10.0),
            time_saved_ms: stats.hit_compile_time_ms,
            total_crates: total_cacheable,
            local_hits: stats.local_hits,
            prefetch_hits: stats.prefetch_hits,
            remote_hits: stats.remote_hits,
            misses: stats.misses,
            errors: stats.errors,
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
            avg_store_ms: if stats.misses > 0 {
                (stats.total_store_ms as f64 / stats.misses as f64 * 10.0).round() / 10.0
            } else {
                0.0
            },
            total_key_ms: stats.total_key_ms,
            total_lookup_ms: stats.total_lookup_ms,
            total_restore_ms: stats.total_restore_ms,
            total_store_ms: stats.total_store_ms,
        },
        network,
        prefetch,
        top_misses: misses.into_iter().take(top).collect(),
        top_hits: hits.into_iter().take(top).collect(),
        all_events,
        errors_detail,
        suggestions,
        gc: load_gc_summary(&config.cache_dir, hours),
    })
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

fn to_crate_detail(e: &BuildEvent) -> CrateDetail {
    let overhead = if matches!(
        e.result,
        EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit
    ) {
        e.elapsed_ms
    } else {
        e.elapsed_ms.saturating_sub(e.compile_time_ms)
    };
    CrateDetail {
        crate_name: e.crate_name.clone(),
        result: e.result.to_string(),
        elapsed_ms: e.elapsed_ms,
        compile_time_ms: e.compile_time_ms,
        overhead_ms: overhead,
        size: e.size,
        cache_key: e.cache_key.clone(),
    }
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
    let mut total_get_requests = 0u32;
    let mut total_original_bytes = 0u64;
    let mut total_decompress_ms = 0u64;
    let mut total_disk_io_ms_measured = 0u64;
    let mut has_disk_io_measurement = false;
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
                compressed_bytes: t.compressed_bytes,
                elapsed_ms: t.elapsed_ms,
                network_ms: t.network_ms,
                request_ms: t.request_ms,
                body_ms: t.body_ms,
                decompress_ms: t.decompress_ms,
                disk_io_ms: t.disk_io_ms,
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
        total_download_ms.saturating_sub(total_network_ms + total_decompress_ms)
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
        total_request_ms,
        total_body_ms,
        total_get_requests,
        compression_ratio: (compression_ratio * 10.0).round() / 10.0,
        original_bytes_down: total_original_bytes,
        total_decompress_ms,
        total_disk_io_ms,
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
    top_misses: &[CrateDetail],
    total_cacheable: usize,
    total_hits: usize,
) -> Vec<String> {
    let mut suggestions = Vec::new();

    // High miss share
    if total_cacheable > 0 && stats.miss_compile_time_ms > 0 {
        let miss_share = stats.miss_compile_time_ms as f64
            / (stats.miss_compile_time_ms + stats.hit_compile_time_ms) as f64
            * 100.0;
        if miss_share > 80.0 && stats.misses > 3 {
            let top_names: Vec<&str> = top_misses
                .iter()
                .take(3)
                .map(|c| c.crate_name.as_str())
                .collect();
            suggestions.push(format!(
                "{:.0}% of compile time spent on misses — improve hit rate for {}",
                miss_share,
                if top_names.is_empty() {
                    "top misses".to_string()
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
    }

    if network.is_none() {
        suggestions.push("No network transfer data available for this session".to_string());
    }

    suggestions
}

// ── Output Formatters ───────────────────────────────────────────────────────

pub fn format_json(report: &BuildReport) -> Result<String> {
    Ok(serde_json::to_string_pretty(report)?)
}

pub fn format_markdown(report: &BuildReport) -> String {
    use crate::cli::format_duration_ms;

    let mut lines = Vec::new();
    let s = &report.summary;

    let total_hits = s.local_hits + s.prefetch_hits + s.remote_hits;
    lines.push("### kache build report".to_string());
    lines.push(String::new());
    lines.push(format!(
        "**{:.1}% hit rate** — {}/{} crates from cache, {} compiled | **{} saved**",
        s.hit_rate_pct,
        total_hits,
        s.total_crates,
        s.misses,
        format_duration_ms(s.time_saved_ms),
    ));
    lines.push(String::new());

    // Summary table
    lines.push("#### Summary".to_string());
    lines.push("| Metric | Value |".to_string());
    lines.push("|---|---|".to_string());
    lines.push(format!("| Hit rate (count) | {:.1}% |", s.hit_rate_pct));
    if let Some(w) = s.weighted_hit_rate_pct {
        lines.push(format!("| Hit rate (weighted) | {:.1}% |", w));
    }
    lines.push(format!(
        "| Time saved | {} |",
        format_duration_ms(s.time_saved_ms)
    ));
    lines.push(format!(
        "| Cache efficiency | {:.1}% |",
        s.cache_efficiency_pct
    ));
    lines.push(format!("| Total crates | {} |", s.total_crates));
    lines.push(format!(
        "| Hits | {} (local: {}, prefetch: {}, remote: {}) |",
        total_hits, s.local_hits, s.prefetch_hits, s.remote_hits
    ));
    lines.push(format!("| Misses | {} |", s.misses));
    if s.errors > 0 {
        lines.push(format!("| Errors | {} |", s.errors));
    }
    lines.push(String::new());

    // Timing table
    let t = &report.timing;
    let total_ms = t.hit_time_ms + t.miss_time_ms;
    lines.push("#### Timing".to_string());
    lines.push("| Phase | Time | % of total |".to_string());
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
        "| Cache hits (wrapper) | {} | {:.1}% |",
        format_duration_ms(t.hit_time_ms),
        hit_pct
    ));
    lines.push(format!(
        "| Misses (compile) | {} | {:.1}% |",
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
            "| Throughput (incl. decompress) | {:.1} MB/s |",
            net.throughput_mbps
        ));
        if net.compression_ratio > 0.0 {
            lines.push(format!(
                "| Compression ratio | {:.1}x ({} → {}) |",
                net.compression_ratio,
                format_bytes(net.original_bytes_down),
                format_bytes(net.bytes_down)
            ));
        }
        if net.total_decompress_ms > 0 || net.total_disk_io_ms > 0 {
            lines.push(format!(
                "| Time breakdown | network {}ms, decompress {}ms, disk I/O {}ms |",
                net.avg_download_ms as u64 * net.downloads_ok as u64,
                net.total_decompress_ms,
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
            lines.push("| Crate | Size | Latency | Throughput |".to_string());
            lines.push("|---|---|---|---|".to_string());
            for d in &net.slowest_downloads {
                lines.push(format!(
                    "| `{}` | {} | {}ms | {:.1} MB/s |",
                    d.crate_name,
                    format_bytes(d.compressed_bytes),
                    d.elapsed_ms,
                    d.throughput_mbps,
                ));
            }
            lines.push(String::new());
        }
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

    // Top cache misses
    if !report.top_misses.is_empty() {
        lines.push("#### Top Cache Misses".to_string());
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
    let total_hits = s.local_hits + s.prefetch_hits + s.remote_hits;

    // Header
    lines.push("### kache build cache".to_string());
    lines.push(String::new());
    lines.push(format!(
        "**{:.1}%** hit rate — {}/{} crates from cache, {} compiled | **{} saved**",
        s.hit_rate_pct,
        total_hits,
        s.total_crates,
        s.misses,
        format_duration_ms(s.time_saved_ms),
    ));
    lines.push(String::new());

    // ── Key metrics (always visible) ──
    lines.push("| | |".to_string());
    lines.push("|---|---|".to_string());
    lines.push(format!(
        "| **Crates** | {} cached / {} compiled / {} total |",
        total_hits, s.misses, s.total_crates
    ));
    lines.push(format!(
        "| **Hit rate** | {:.1}%{} |",
        s.hit_rate_pct,
        s.weighted_hit_rate_pct
            .map(|w| format!(" ({:.1}% weighted by cost)", w))
            .unwrap_or_default()
    ));
    lines.push(format!(
        "| **Time saved** | {} |",
        format_duration_ms(s.time_saved_ms)
    ));
    lines.push(format!(
        "| **Efficiency** | {:.1}% of compile time saved by cache |",
        s.cache_efficiency_pct
    ));
    if s.errors > 0 {
        lines.push(format!("| **Errors** | {} |", s.errors));
    }

    // ── Suggestions (always visible — actionable) ──
    if !report.suggestions.is_empty() {
        lines.push(String::new());
        for sg in &report.suggestions {
            lines.push(format!("> {sg}"));
        }
    }

    // ── Top misses (collapsed) ──
    if !report.top_misses.is_empty() {
        lines.push(String::new());
        lines.push("<details>".to_string());
        lines.push(format!(
            "<summary><strong>Top cache misses</strong> ({} compiled)</summary>",
            s.misses
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
        if s.misses > 10 {
            lines.push(format!("| *... {} more* | | |", s.misses - 10));
        }
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
        lines.push(format!(
            "<summary><strong>Network</strong> — {} downloaded, {:.0} MB/s</summary>",
            format_bytes(net.bytes_down),
            net_tp,
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
            "| Throughput | {:.1} MB/s network · {:.1} MB/s end-to-end |",
            net_tp, net.throughput_mbps
        ));
        if net.compression_ratio > 0.0 {
            lines.push(format!(
                "| Compression | {:.1}x ({} → {}) |",
                net.compression_ratio,
                format_bytes(net.original_bytes_down),
                format_bytes(net.bytes_down)
            ));
        }
        if net.total_decompress_ms > 0 || net.total_disk_io_ms > 0 {
            lines.push(format!(
                "| Time split | request {}ms · body {}ms · decompress {}ms · disk {}ms |",
                net.total_request_ms,
                net.total_body_ms,
                net.total_decompress_ms,
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
            lines.push("| Crate | Fmt | Size | Time | GETs | Req/Body | Decomp/Disk |".to_string());
            lines.push("|-------|-----|------|------|------|----------|-------------|".to_string());
            for d in net.slowest_downloads.iter().take(5) {
                lines.push(format!(
                    "| `{}` | {} | {} | {}ms | {} | {}/{}ms | {}/{}ms |",
                    d.crate_name,
                    if d.format.is_empty() { "?" } else { &d.format },
                    format_bytes(d.compressed_bytes),
                    d.elapsed_ms,
                    d.request_count,
                    d.request_ms,
                    d.body_ms,
                    d.decompress_ms,
                    d.disk_io_ms,
                ));
            }
        }
        lines.push(String::new());
        lines.push("</details>".to_string());
    }

    // ── Timing & Prefetch (collapsed) ──
    let t = &report.timing;
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
            lines.push("| Phase | Time | % |".to_string());
            lines.push("|-------|------|---|".to_string());
            lines.push(format!(
                "| Cache hits | {} | {:.1}% |",
                format_duration_ms(t.hit_time_ms),
                hit_pct
            ));
            lines.push(format!(
                "| Compiling misses | {} | {:.1}% |",
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
    lines.push(
        "*Posted by [kache-action](https://github.com/kunobi-ninja/kache-action)*".to_string(),
    );

    lines.join("\n")
}

pub fn format_text(report: &BuildReport) -> String {
    use crate::cli::format_duration_ms;

    let mut lines = Vec::new();
    let s = &report.summary;
    let total_hits = s.local_hits + s.prefetch_hits + s.remote_hits;

    lines.push(format!(
        "kache build report (last {}h)",
        report.meta.since_hours
    ));
    lines.push(format!(
        "  {:.1}% hit rate — {}/{} cached, {} compiled",
        s.hit_rate_pct, total_hits, s.total_crates, s.misses,
    ));
    if let Some(w) = s.weighted_hit_rate_pct {
        lines.push(format!("  {:.1}% weighted by compile cost", w));
    }
    lines.push(format!(
        "  Time saved: {}",
        format_duration_ms(s.time_saved_ms)
    ));
    lines.push(format!(
        "  Cache efficiency: {:.1}% of compile time saved by cache",
        s.cache_efficiency_pct
    ));
    if s.errors > 0 {
        lines.push(format!("  Errors: {}", s.errors));
    }
    lines.push(String::new());

    // Timing
    let t = &report.timing;
    lines.push("Timing:".to_string());
    lines.push(format!(
        "  Hits: {} (avg {:.0}ms/crate)",
        format_duration_ms(t.hit_time_ms),
        t.avg_hit_ms
    ));
    lines.push(format!(
        "  Misses: {} (avg {:.0}ms/crate)",
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
            "  Throughput: {:.1} MB/s network, {:.1} MB/s incl. decompress",
            net.network_throughput_mbps, net.throughput_mbps
        ));
        if net.compression_ratio > 0.0 {
            lines.push(format!(
                "  Compression: {:.1}x ratio ({} → {})",
                net.compression_ratio,
                format_bytes(net.original_bytes_down),
                format_bytes(net.bytes_down)
            ));
        }
        if net.total_decompress_ms > 0 || net.total_disk_io_ms > 0 {
            lines.push(format!(
                "  Time split: network {}ms, decompress {}ms, disk I/O {}ms",
                net.avg_download_ms as u64 * net.downloads_ok as u64,
                net.total_decompress_ms,
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

    // Prefetch
    lines.push(format!(
        "Prefetch: {} / {} hits ({:.1}%)",
        report.prefetch.prefetch_hits, report.prefetch.total_hits, report.prefetch.contribution_pct
    ));
    lines.push(String::new());

    // Top misses
    if !report.top_misses.is_empty() {
        lines.push("Top misses:".to_string());
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
            version: "0.1.0".to_string(),
            result,
            elapsed_ms,
            compile_time_ms,
            size,
            cache_key: cache_key.to_string(),
            schema: 2,
            key_ms: 0,
            lookup_ms: 0,
            restore_ms: 0,
            store_ms: 0,
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
            schema: 1,
            crate_name: crate_name.to_string(),
            direction,
            format: format.to_string(),
            compressed_bytes,
            elapsed_ms,
            network_ms: elapsed_ms / 2, // simulate network = half of total
            request_ms: elapsed_ms / 5,
            body_ms: elapsed_ms / 3,
            request_count: 4,
            original_bytes: compressed_bytes * 3, // simulate ~3x compression ratio
            decompress_ms: elapsed_ms / 4,        // simulate decompress = quarter of total
            disk_io_ms: 0,
            compression_ms: 0,
            head_checks_ms: 0,
            blobs_skipped: 0,
            blobs_total: 2,
            ok,
            timestamp: Utc::now().timestamp() as u64,
        }
    }

    fn write_test_events(dir: &std::path::Path) -> Config {
        let config = Config {
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
        };

        // Write build events
        let events = vec![
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
            test_event(
                "my_lib",
                EventResult::Miss,
                5000,
                4800,
                3 * 1024 * 1024,
                "def456789012",
            ),
            test_event(
                "my_app",
                EventResult::Miss,
                8000,
                7500,
                5 * 1024 * 1024,
                "efg567",
            ),
            test_event("broken", EventResult::Error, 10, 0, 0, "err001"),
        ];
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
        assert_eq!(report.summary.misses, 2);
        assert_eq!(report.summary.errors, 1);
        assert!(report.summary.hit_rate_pct > 0.0);
        assert!(report.summary.time_saved_ms > 0);
        let network = report.network.as_ref().unwrap();
        assert_eq!(network.v3_downloads, 3);
        assert_eq!(network.v2_downloads, 0);
        assert_eq!(network.total_get_requests, 12);
    }

    #[test]
    fn test_json_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config = write_test_events(dir.path());
        let report = generate_report(&config, 24, 10).unwrap();

        let json = format_json(&report).unwrap();
        let parsed: BuildReport = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.summary.total_crates, report.summary.total_crates);
        assert_eq!(parsed.summary.misses, report.summary.misses);
        assert_eq!(parsed.top_misses.len(), report.top_misses.len());
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
        assert!(md.contains("#### Top Cache Misses"));
        assert!(md.contains("#### Suggestions"));
    }

    #[test]
    fn test_missing_transfer_data() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
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
                .any(|s| s.contains("compile time spent on misses"))
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
        assert!(gh.contains("**Time saved**"));
        // Details in collapsible sections
        assert!(gh.contains("<details>"));
        assert!(gh.contains("<summary><strong>Top cache misses</strong>"));
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
    }

    #[test]
    fn test_empty_report() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
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
}
