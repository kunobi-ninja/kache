use anyhow::{Context, Result};
use bytesize::ByteSize;
use std::io::IsTerminal;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use std::sync::Arc;

use crate::config::Config;
use crate::daemon;
use crate::events;
use crate::since::SinceWindow;
use crate::store::{STAGING_SWEEP_GRACE, Store};

// ── Stats snapshot (daemon-first, fallback to direct) ──────────────────────

/// Cached store + event stats, refreshed periodically.
/// Used by both the TUI monitor and `kache stats` CLI.
pub(crate) struct StatsSnapshot {
    pub total_size: u64,
    pub max_size: u64,
    pub entry_count: usize,
    pub entries: Vec<daemon::StatsEntry>,
    pub event_stats: daemon::EventStatsResponse,
    pub daemon_connected: bool,
    pub daemon_version: String,
    pub daemon_build_epoch: u64,
    pub pending_uploads: usize,
    pub active_downloads: usize,
    pub s3_concurrency_total: usize,
    pub s3_concurrency_used: usize,
    pub uploads_completed: u64,
    pub uploads_failed: u64,
    pub uploads_skipped: u64,
    pub uploads_suppressed: u64,
    pub downloads_completed: u64,
    pub downloads_failed: u64,
    pub downloads_suppressed: u64,
    /// RemoteChecks that reached S3 vs. answers from the negative cache
    /// (kunobi-ninja/kache#564), plus the breaker state (#327). Zeroed when
    /// the daemon is unreachable.
    pub remote_check_roundtrips: u64,
    pub negative_hits: u64,
    pub negative_entries: u64,
    pub remote_degraded: bool,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub recent_transfers: Vec<daemon::TransferEvent>,
    pub blob_stats: Option<crate::store::BlobStats>,
    /// Recent daemon-owned session summaries, or direct-store summaries when
    /// no daemon was reachable.
    pub recent_summaries: Vec<crate::events::BuildSummaryEvent>,
    /// Phase-0 prefetch/planning observability (#485); zeroed when the daemon
    /// is unreachable.
    pub prefetch: daemon::PrefetchStatsSnapshot,
    /// In-flight miss compiles (kunobi-ninja/kache#131); empty when the
    /// daemon is unreachable (the TUI then falls back to tailed heartbeats).
    pub in_flight: Vec<daemon::InFlightEntry>,
    /// The daemon's effective config from the stats response
    /// (kunobi-ninja/kache#689); `None` when the daemon is unreachable or
    /// predates effective-config reporting.
    pub daemon_effective_config: Option<daemon::EffectiveConfig>,
}

impl Default for StatsSnapshot {
    fn default() -> Self {
        Self {
            total_size: 0,
            max_size: 0,
            entry_count: 0,
            entries: Vec::new(),
            event_stats: daemon::EventStatsResponse {
                local_hits: 0,
                prefetch_hits: 0,
                remote_hits: 0,
                dups: 0,
                misses: 0,
                errors: 0,
                total_elapsed_ms: 0,
                hit_elapsed_ms: 0,
                miss_elapsed_ms: 0,
                hit_compile_time_ms: 0,
                miss_compile_time_ms: 0,
                store_output_blobs: 0,
                store_duplicate_blobs: 0,
                store_new_blobs: 0,
            },
            daemon_connected: false,
            daemon_version: String::new(),
            daemon_build_epoch: 0,
            pending_uploads: 0,
            active_downloads: 0,
            s3_concurrency_total: 0,
            s3_concurrency_used: 0,
            uploads_completed: 0,
            uploads_failed: 0,
            uploads_skipped: 0,
            uploads_suppressed: 0,
            downloads_completed: 0,
            downloads_failed: 0,
            downloads_suppressed: 0,
            remote_check_roundtrips: 0,
            negative_hits: 0,
            negative_entries: 0,
            remote_degraded: false,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            recent_transfers: Vec::new(),
            blob_stats: None,
            recent_summaries: Vec::new(),
            prefetch: daemon::PrefetchStatsSnapshot::default(),
            in_flight: Vec::new(),
            daemon_effective_config: None,
        }
    }
}

pub fn count_hit_rate(es: &daemon::EventStatsResponse) -> f64 {
    let total = es.local_hits + es.prefetch_hits + es.remote_hits + es.dups + es.misses;
    if total > 0 {
        ((es.local_hits + es.prefetch_hits + es.remote_hits) as f64 / total as f64) * 100.0
    } else {
        0.0
    }
}

pub fn compile_weighted_hit_rate(es: &daemon::EventStatsResponse) -> Option<f64> {
    let total = es.hit_compile_time_ms + es.miss_compile_time_ms;
    if total > 0 {
        Some((es.hit_compile_time_ms as f64 / total as f64) * 100.0)
    } else {
        None
    }
}

/// Try daemon first, fall back to direct reads.
///
/// With `announce_auto_start` set, the daemon-unreachable path says on stderr
/// that it is starting a daemon inheriting this process's environment
/// (kunobi-ninja/kache#689) — otherwise the same command silently flips from
/// "daemon's config" to "my config" depending on daemon liveness. The CLI
/// passes true; the TUI passes false because its raw-mode alternate screen
/// owns the terminal.
pub(crate) fn fetch_stats_snapshot(
    config: &Config,
    include_entries: bool,
    sort_by: &str,
    window: SinceWindow,
    announce_auto_start: bool,
    include_summaries: bool,
) -> StatsSnapshot {
    // Try daemon
    if let Ok(resp) = daemon::send_stats_request_options(
        config,
        include_entries,
        include_summaries,
        Some(sort_by),
        Some(window),
    ) {
        return StatsSnapshot {
            total_size: resp.total_size,
            max_size: resp.max_size,
            entry_count: resp.entry_count,
            entries: resp.entries.unwrap_or_default(),
            event_stats: resp.events,
            daemon_connected: true,
            daemon_version: resp.version,
            daemon_build_epoch: resp.build_epoch,
            pending_uploads: resp.pending_uploads,
            active_downloads: resp.active_downloads,
            s3_concurrency_total: resp.s3_concurrency_total,
            s3_concurrency_used: resp.s3_concurrency_used,
            uploads_completed: resp.uploads_completed,
            uploads_failed: resp.uploads_failed,
            uploads_skipped: resp.uploads_skipped,
            uploads_suppressed: resp.uploads_suppressed,
            downloads_completed: resp.downloads_completed,
            downloads_failed: resp.downloads_failed,
            downloads_suppressed: resp.downloads_suppressed,
            remote_check_roundtrips: resp.remote_check_roundtrips,
            negative_hits: resp.negative_hits,
            negative_entries: resp.negative_entries,
            remote_degraded: resp.remote_degraded,
            bytes_uploaded: resp.bytes_uploaded,
            bytes_downloaded: resp.bytes_downloaded,
            recent_transfers: resp.recent_transfers,
            blob_stats: resp.blob_stats,
            recent_summaries: resp.recent_summaries,
            prefetch: resp.prefetch,
            in_flight: resp.in_flight,
            daemon_effective_config: resp.effective_config,
        };
    }

    // Daemon unreachable or stale socket: best-effort auto-start for monitor/stats UX.
    // This path is not used by compile-time hot operations.
    if announce_auto_start {
        eprintln!(
            "kache: no daemon reachable at {}; starting one inheriting this process's environment",
            config.socket_path().display()
        );
    }
    if daemon::start_daemon_background().unwrap_or(false)
        && let Ok(resp) = daemon::send_stats_request_options(
            config,
            include_entries,
            include_summaries,
            Some(sort_by),
            Some(window),
        )
    {
        return StatsSnapshot {
            total_size: resp.total_size,
            max_size: resp.max_size,
            entry_count: resp.entry_count,
            entries: resp.entries.unwrap_or_default(),
            event_stats: resp.events,
            daemon_connected: true,
            daemon_version: resp.version,
            daemon_build_epoch: resp.build_epoch,
            pending_uploads: resp.pending_uploads,
            active_downloads: resp.active_downloads,
            s3_concurrency_total: resp.s3_concurrency_total,
            s3_concurrency_used: resp.s3_concurrency_used,
            uploads_completed: resp.uploads_completed,
            uploads_failed: resp.uploads_failed,
            uploads_skipped: resp.uploads_skipped,
            uploads_suppressed: resp.uploads_suppressed,
            downloads_completed: resp.downloads_completed,
            downloads_failed: resp.downloads_failed,
            downloads_suppressed: resp.downloads_suppressed,
            remote_check_roundtrips: resp.remote_check_roundtrips,
            negative_hits: resp.negative_hits,
            negative_entries: resp.negative_entries,
            remote_degraded: resp.remote_degraded,
            bytes_uploaded: resp.bytes_uploaded,
            bytes_downloaded: resp.bytes_downloaded,
            recent_transfers: resp.recent_transfers,
            blob_stats: resp.blob_stats,
            recent_summaries: resp.recent_summaries,
            prefetch: resp.prefetch,
            in_flight: resp.in_flight,
            daemon_effective_config: resp.effective_config,
        };
    }

    // Fallback: direct reads (no daemon reachable).
    snapshot_from_direct_reads(config, include_entries, sort_by, window, include_summaries)
}

/// Build a [`StatsSnapshot`] by reading the store and event log directly, with no
/// daemon. Split out from [`fetch_stats_snapshot`]'s fallback so it is unit-
/// testable against a seeded cache without a running (or auto-started) daemon.
pub(crate) fn snapshot_from_direct_reads(
    config: &Config,
    include_entries: bool,
    sort_by: &str,
    window: SinceWindow,
    include_summaries: bool,
) -> StatsSnapshot {
    let store = Store::open(config).ok();
    let total_size = store
        .as_ref()
        .and_then(|s| s.total_size().ok())
        .unwrap_or(0);
    let entry_count = store
        .as_ref()
        .and_then(|s| s.entry_count().ok())
        .unwrap_or(0);

    let entries = if include_entries {
        store
            .as_ref()
            .and_then(|s| s.list_entries(sort_by).ok())
            .unwrap_or_default()
            .into_iter()
            .map(|e| daemon::StatsEntry {
                cache_key: e.cache_key,
                crate_name: e.crate_name,
                crate_type: e.crate_type,
                profile: e.profile,
                size: e.size,
                hit_count: e.hit_count,
                created_at: e.created_at,
                last_accessed: e.last_accessed,
                content_hash: e.content_hash,
            })
            .collect()
    } else {
        Vec::new()
    };

    let since = window.cutoff(chrono::Utc::now());
    let event_list = events::read_events_since(&config.event_log_path(), since).unwrap_or_default();
    let es = events::compute_stats(&event_list);

    let recent_summaries = if include_summaries {
        let mut summaries =
            crate::events::read_summaries(&config.summary_log_path()).unwrap_or_default();
        let keep_from = summaries.len().saturating_sub(5);
        summaries.drain(..keep_from);
        summaries
    } else {
        Vec::new()
    };

    StatsSnapshot {
        total_size,
        max_size: config.max_size,
        entry_count,
        entries,
        event_stats: daemon::EventStatsResponse {
            local_hits: es.local_hits,
            prefetch_hits: es.prefetch_hits,
            remote_hits: es.remote_hits,
            dups: es.dups,
            misses: es.misses,
            errors: es.errors,
            total_elapsed_ms: es.total_elapsed_ms,
            hit_elapsed_ms: es.hit_elapsed_ms,
            miss_elapsed_ms: es.miss_elapsed_ms,
            hit_compile_time_ms: es.hit_compile_time_ms,
            miss_compile_time_ms: es.miss_compile_time_ms,
            store_output_blobs: es.store_output_blobs,
            store_duplicate_blobs: es.store_duplicate_blobs,
            store_new_blobs: es.store_new_blobs,
        },
        daemon_connected: false,
        daemon_version: String::new(),
        daemon_build_epoch: 0,
        pending_uploads: 0,
        active_downloads: 0,
        s3_concurrency_total: 0,
        s3_concurrency_used: 0,
        uploads_completed: 0,
        uploads_failed: 0,
        uploads_skipped: 0,
        uploads_suppressed: 0,
        downloads_completed: 0,
        downloads_failed: 0,
        downloads_suppressed: 0,
        remote_check_roundtrips: 0,
        negative_hits: 0,
        negative_entries: 0,
        remote_degraded: false,
        bytes_uploaded: 0,
        bytes_downloaded: 0,
        recent_transfers: Vec::new(),
        blob_stats: store.as_ref().and_then(|s| s.blob_stats().ok()),
        recent_summaries,
        prefetch: daemon::PrefetchStatsSnapshot::default(),
        in_flight: Vec::new(),
        daemon_effective_config: None,
    }
}

/// Write cache counters as OTLP JSON for Kartero (`metrics.otlp.json` +
/// `schema_version`). Uses the running daemon when reachable; otherwise the
/// local store. Does not auto-start a daemon, so a finished bench dumps what
/// is already on disk instead of an empty new process.
pub fn telemetry_write(
    config: &Config,
    dir: &std::path::Path,
    scenario: Option<&str>,
    phase: Option<&str>,
) -> Result<()> {
    let snap = match daemon::send_stats_request_options(
        config,
        false,
        false,
        None,
        Some(SinceWindow::DEFAULT),
    ) {
        Ok(resp) => StatsSnapshot {
            total_size: resp.total_size,
            max_size: resp.max_size,
            entry_count: resp.entry_count,
            entries: resp.entries.unwrap_or_default(),
            event_stats: resp.events,
            daemon_connected: true,
            daemon_version: resp.version,
            daemon_build_epoch: resp.build_epoch,
            pending_uploads: resp.pending_uploads,
            active_downloads: resp.active_downloads,
            s3_concurrency_total: resp.s3_concurrency_total,
            s3_concurrency_used: resp.s3_concurrency_used,
            uploads_completed: resp.uploads_completed,
            uploads_failed: resp.uploads_failed,
            uploads_skipped: resp.uploads_skipped,
            uploads_suppressed: resp.uploads_suppressed,
            downloads_completed: resp.downloads_completed,
            downloads_failed: resp.downloads_failed,
            downloads_suppressed: resp.downloads_suppressed,
            remote_check_roundtrips: resp.remote_check_roundtrips,
            negative_hits: resp.negative_hits,
            negative_entries: resp.negative_entries,
            remote_degraded: resp.remote_degraded,
            bytes_uploaded: resp.bytes_uploaded,
            bytes_downloaded: resp.bytes_downloaded,
            recent_transfers: resp.recent_transfers,
            blob_stats: resp.blob_stats,
            recent_summaries: resp.recent_summaries,
            prefetch: resp.prefetch,
            in_flight: resp.in_flight,
            daemon_effective_config: resp.effective_config,
        },
        Err(_) => snapshot_from_direct_reads(config, false, "size", SinceWindow::DEFAULT, false),
    };
    crate::otel::write_otlp(
        dir,
        &otel_snapshot_from_stats(config, &snap),
        crate::VERSION,
        scenario,
        phase,
    )?;
    eprintln!(
        "wrote {} and {}",
        dir.join(crate::otel::METRICS_FILE).display(),
        dir.join(crate::otel::SCHEMA_VERSION_FILE).display()
    );
    Ok(())
}

fn otel_snapshot_from_stats(config: &Config, snap: &StatsSnapshot) -> crate::otel::OtelSnapshot {
    crate::otel::OtelSnapshot {
        remote_kind: config
            .remote
            .as_ref()
            .map(|remote| remote.backend_kind())
            .unwrap_or("none"),
        store_max: snap.max_size,
        store_size: Some(snap.total_size),
        store_entries: Some(snap.entry_count as u64),
        pending_uploads: Some(snap.pending_uploads as u64),
        active_downloads: Some(snap.active_downloads as u64),
        s3_concurrency_total: snap.s3_concurrency_total as u64,
        s3_concurrency_used: snap.s3_concurrency_used as u64,
        uploads_completed: snap.uploads_completed,
        uploads_failed: snap.uploads_failed,
        uploads_skipped: snap.uploads_skipped,
        uploads_suppressed: snap.uploads_suppressed,
        downloads_completed: snap.downloads_completed,
        downloads_failed: snap.downloads_failed,
        downloads_suppressed: snap.downloads_suppressed,
        bytes_uploaded: snap.bytes_uploaded,
        bytes_downloaded: snap.bytes_downloaded,
        remote_check_roundtrips: snap.remote_check_roundtrips,
        negative_hits: snap.negative_hits,
        negative_entries: snap.negative_entries,
        remote_degraded: snap.remote_degraded,
        prefetch_downloads: snap.prefetch.downloads_completed,
        prefetch_bytes: snap.prefetch.bytes_downloaded,
        prefetch_keys_used: snap.prefetch.keys_used,
        prefetch_keys_cancelled: snap.prefetch.keys_cancelled,
        prefetch_keys_over_budget: snap.prefetch.keys_over_budget,
        prefetch_plans_advisory: snap.prefetch.plans_advisory,
        prefetch_plans_fallback: snap.prefetch.plans_fallback,
        prefetch_list_requests: snap.prefetch.list_requests_total,
        prefetch_list_failures: snap.prefetch.list_failures_total,
        prefetch_pack_requests: snap.prefetch.pack_requests_total,
        prefetch_v3_requests: snap.prefetch.v3_requests_total,
        prefetch_cancelled: snap.prefetch.cancelled,
        prefetch_last_plan_candidates: snap.prefetch.last_plan_candidates,
        prefetch_last_plan_wall_ms: snap.prefetch.last_plan_wall_ms,
    }
}

// ── kache stats ────────────────────────────────────────────────────────────

fn cloned_targets_line(disk: &crate::machine::DiskView) -> Option<String> {
    (disk.cloned_into_targets_bytes > 0).then(|| {
        format!(
            "On disk:    {} private; {} cloned into target/",
            ByteSize(disk.disk_private_bytes),
            ByteSize(disk.cloned_into_targets_bytes)
        )
    })
}

/// Print a one-shot stats summary to stdout.
pub fn stats(
    config: &Config,
    provenance: &crate::config::ConfigFileProvenance,
    window: SinceWindow,
    json: bool,
) -> Result<()> {
    let snap = fetch_stats_snapshot(config, false, "size", window, true, true);

    // #689: the numbers below are daemon-first, so before rendering them say
    // when the daemon's effective config disagrees with this invocation's —
    // otherwise a config edit that has not reached the daemon masquerades as
    // "kache ignores config files".
    if let Some(eff) = &snap.daemon_effective_config {
        for warning in config_mismatch_warnings(config, provenance, eff) {
            eprintln!("{warning}");
        }
    }

    let store_bytes = snap
        .blob_stats
        .as_ref()
        .map(|s| s.total_blob_size)
        .unwrap_or(snap.total_size);
    let disk = crate::machine::disk_view(&config.store_dir(), store_bytes, snap.max_size);

    if json {
        #[derive(serde::Serialize)]
        struct Body<'a> {
            disk: crate::machine::DiskView,
            entries: usize,
            hit_rate_pct: f64,
            local_hits: usize,
            prefetch_hits: usize,
            remote_hits: usize,
            dups: usize,
            misses: usize,
            daemon_connected: bool,
            /// Whole hours, rounded down (0 for a sub-hour window). Kept for
            /// consumers that predate `since_secs`.
            hours: u64,
            since_secs: u64,
            /// The window as requested: `15m`, `2h`, `24h`.
            since: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            remote: Option<&'a str>,
        }
        let hit_rate = count_hit_rate(&snap.event_stats);
        let remote = config.remote.as_ref().map(|r| r.describe());
        return crate::machine::emit(
            "stats",
            Body {
                disk: disk.clone(),
                entries: snap.entry_count,
                hit_rate_pct: hit_rate,
                local_hits: snap.event_stats.local_hits,
                prefetch_hits: snap.event_stats.prefetch_hits,
                remote_hits: snap.event_stats.remote_hits,
                dups: snap.event_stats.dups,
                misses: snap.event_stats.misses,
                daemon_connected: snap.daemon_connected,
                hours: window.hours(),
                since_secs: window.secs(),
                since: window.label(),
                remote: remote.as_deref(),
            },
            crate::machine::next_for_clones(disk.cloned_into_targets_bytes),
        );
    }

    for line in render_stats(&snap, config, window) {
        println!("{line}");
    }
    if let Some(line) = cloned_targets_line(&disk) {
        println!("{line}");
    }

    // Recent per-session prefetch summaries (#583 P0.5): the durable record
    // (survives daemon restarts) behind the live snapshot above. Keys/bytes
    // are daemon-visible lower bounds; join events.jsonl by session_id for
    // full attribution.
    let summaries = &snap.recent_summaries;
    if !summaries.is_empty() {
        println!("Sessions (last {}):", summaries.len().min(5));
        for s in summaries.iter().rev().take(5) {
            let cancelled = if s.cancelled { ", CANCELLED" } else { "" };
            println!(
                "  {} [{}] {}: {}/{} candidates downloaded ({}), {} used, {} demanded ({}){}",
                s.ts.format("%m-%d %H:%M"),
                if s.session_id.is_empty() {
                    "legacy"
                } else {
                    &s.session_id
                },
                s.plan_source,
                s.downloaded_keys,
                s.candidate_keys,
                ByteSize(s.downloaded_bytes),
                s.used_keys,
                s.demanded_keys,
                s.closure_reason,
                cancelled,
            );
        }
    }
    Ok(())
}

/// Render the `kache stats` summary lines from a fetched snapshot. Pure (no I/O)
/// so the dedup / weighted-hit / miss-share / daemon / remote display branches
/// are unit-testable from crafted snapshots without a daemon or store.
pub(crate) fn render_stats(
    snap: &StatsSnapshot,
    config: &Config,
    window: SinceWindow,
) -> Vec<String> {
    let mut lines = Vec::new();

    // Store line
    let store_pct = if snap.max_size > 0 {
        (snap.total_size as f64 / snap.max_size as f64) * 100.0
    } else {
        0.0
    };
    lines.push(format!(
        "Store:      {} / {} ({} entries, {:.0}%)",
        ByteSize(snap.total_size),
        crate::config::describe_max_size(
            snap.max_size,
            crate::cache_fs::probe(&config.cache_dir).total_bytes,
        ),
        snap.entry_count,
        store_pct,
    ));

    // Content dedup stats
    if let Some(blob_stats) = snap
        .blob_stats
        .as_ref()
        .filter(|stats| stats.total_blobs > 0)
    {
        let savings_pct = if blob_stats.total_logical_size > 0 {
            blob_stats.savings as f64 / blob_stats.total_logical_size as f64 * 100.0
        } else {
            0.0
        };
        lines.push(format!(
            "Dedup:      {} unique blobs, {} physical, {:.1}% savings",
            blob_stats.total_blobs,
            ByteSize(blob_stats.total_blob_size),
            savings_pct,
        ));
    }

    // Hit rate
    let es = &snap.event_stats;
    let hit_rate = count_hit_rate(es);
    lines.push(format!(
        "Hit rate:   {hit_rate:.1}% (local: {}, prefetch: {}, remote: {}, dup: {}, miss: {})",
        es.local_hits, es.prefetch_hits, es.remote_hits, es.dups, es.misses,
    ));
    if let Some(weighted) = compile_weighted_hit_rate(es) {
        lines.push(format!("Weighted:   {weighted:.1}% by compile cost"));
    }
    if es.total_elapsed_ms > 0 {
        let miss_share = (es.miss_elapsed_ms as f64 / es.total_elapsed_ms as f64) * 100.0;
        lines.push(format!(
            "Miss share: {:.1}% of wrapper time ({})",
            miss_share,
            format_duration_ms(es.miss_elapsed_ms)
        ));
    }

    let time_saved = if es.hit_compile_time_ms > 0 {
        format_duration_ms(es.hit_compile_time_ms)
    } else {
        "n/a".to_string()
    };
    lines.push(format!(
        "Time saved: {time_saved} (estimated compile work avoided, last {window})"
    ));

    // Daemon status
    if snap.daemon_connected {
        let my_epoch = crate::daemon::build_epoch();
        let mismatch = if snap.daemon_build_epoch != my_epoch {
            " (MISMATCH — auto-restart pending)"
        } else {
            ""
        };
        // Name the config file the daemon loaded (#689): the store cap and
        // policy lines describe THAT file, which need not be the one this
        // invocation resolved.
        let config_note = snap
            .daemon_effective_config
            .as_ref()
            .map(|eff| format!(", config {}", eff.config_path))
            .unwrap_or_default();
        lines.push(format!(
            "Daemon:     v{} (epoch {}{config_note}){mismatch}",
            snap.daemon_version, snap.daemon_build_epoch,
        ));
    } else {
        lines.push("Daemon:     offline".to_string());
    }

    // Remote state belongs to the daemon just like the counters above it.
    // Fall back to this process only for an older daemon, and label the guess.
    let (remote_status, daemon_has_remote, remote_source) = match &snap.daemon_effective_config {
        Some(eff) => (
            remote_status(
                eff.remote_description.as_deref(),
                eff.local_only,
                eff.remote_error.as_deref(),
            ),
            eff.remote_description.is_some(),
            "",
        ),
        None => (
            remote_status(
                config
                    .remote
                    .as_ref()
                    .map(|remote| remote.describe())
                    .as_deref(),
                config.local_only,
                config.remote_error.as_deref(),
            ),
            config.remote.is_some(),
            if snap.daemon_connected {
                " [client config — daemon did not report its remote state]"
            } else {
                ""
            },
        ),
    };
    lines.push(format!("Remote:     {remote_status}{remote_source}"));

    // Remote resilience (kunobi-ninja/kache#327, #564): breaker state and
    // negative-cache effectiveness (hits avoided vs. round trips paid). Shown
    // only once the daemon has remote-check traffic to report, so existing
    // output stays unchanged for quiet or local-only setups.
    if snap.daemon_connected && config.remote.is_some() && has_remote_resilience_activity(snap) {
        let degraded = if snap.remote_degraded {
            " — DEGRADED (reads suppressed, uploads deferred)"
        } else {
            ""
        };
        lines.push(format!(
            "Resilience: {} remote round trips, {} negative-cache hits ({} remembered), {} restores suppressed / {} uploads deferred{degraded}",
            snap.remote_check_roundtrips,
            snap.negative_hits,
            snap.negative_entries,
            snap.downloads_suppressed,
            snap.uploads_suppressed,
        ));
    }

    // Prefetch/planning baseline (#485 Phase 0). Shown only when the daemon
    // has something to report, so local-only output stays unchanged.
    let pf = &snap.prefetch;
    // The policy line states what the DAEMON is doing, so it renders the
    // daemon's effective policy (#652/#689) — this process's config may say
    // "disabled" while a long-lived daemon is still LISTing and planning.
    // Only a daemon too old to report a policy falls back to client config,
    // labeled, because then the line is a guess rather than a daemon fact.
    let (prefetch_enabled, prefetch_source) = match &snap.daemon_effective_config {
        Some(eff) => (eff.prefetch_enabled, ""),
        None => (
            config.prefetch_enabled,
            " [client config — daemon did not report its policy]",
        ),
    };
    if snap.daemon_connected && daemon_has_remote && !prefetch_enabled {
        lines.push(format!(
            "Prefetch:   disabled (exact remote lookup and uploads remain enabled){prefetch_source}"
        ));
    } else if snap.daemon_connected
        && (pf.downloads_completed > 0
            || pf.plans_advisory + pf.plans_fallback > 0
            || pf.last_list_key_count > 0)
    {
        let used_pct = if pf.downloads_completed > 0 {
            (pf.keys_used as f64 / pf.downloads_completed as f64) * 100.0
        } else {
            0.0
        };
        let cancelled = if pf.cancelled { ", CANCELLED" } else { "" };
        lines.push(format!(
            "Prefetch:   {} downloads ({}), {} used ({:.0}%), {} cancelled{}",
            pf.downloads_completed,
            ByteSize(pf.bytes_downloaded),
            pf.keys_used,
            used_pct,
            pf.keys_cancelled,
            cancelled,
        ));
        lines.push(format!(
            "Planning:   {} advisory / {} fallback plans (last: {} candidates)",
            pf.plans_advisory, pf.plans_fallback, pf.last_plan_candidates,
        ));
        if pf.pack_requests_total + pf.v3_requests_total > 0 {
            lines.push(format!(
                "Transport:  pack {} requests / {}, v3 {} requests / {}; {} validation failures, {} v3 fallbacks",
                pf.pack_requests_total,
                ByteSize(pf.pack_bytes_downloaded),
                pf.v3_requests_total,
                ByteSize(pf.v3_bytes_downloaded),
                pf.pack_validation_failures,
                pf.pack_fallback_entries,
            ));
        }
        if pf.last_plan_wall_ms > 0 {
            lines.push(format!(
                "Plan wall:  {} ms last / {} ms total",
                pf.last_plan_wall_ms, pf.plan_wall_ms_total,
            ));
        }
        if pf.last_list_key_count > 0 {
            let (refresh_secs, refresh_source) = match &snap.daemon_effective_config {
                Some(eff) => (eff.remote_key_cache_refresh_secs, ""),
                None => (
                    config.remote_key_cache_refresh_secs,
                    "; client config — daemon did not report its cadence",
                ),
            };
            let refresh = if refresh_secs == 0 {
                "one initial population; periodic refresh disabled".to_string()
            } else {
                format!("refreshes every {refresh_secs}s")
            };
            lines.push(format!(
                "Key LIST:   {} keys in {} ms ({refresh}{refresh_source})",
                pf.last_list_key_count, pf.last_list_duration_ms,
            ));
        }
        // Cumulative LIST cost (#583 P0.5): the totals the P3 decision gate
        // reads. Rendered only once refreshes have happened.
        if pf.list_requests_total > 0 {
            lines.push(format!(
                "LIST total: {} requests ({} failed), {} ms, {} keys returned",
                pf.list_requests_total,
                pf.list_failures_total,
                pf.list_duration_ms_total,
                pf.list_keys_total,
            ));
        }
        if pf.dedup_join_waits > 0 {
            lines.push(format!(
                "Join-wait:  {} waits, {} ms total (in-flight download dedup)",
                pf.dedup_join_waits, pf.dedup_join_wait_ms,
            ));
        }
    }

    lines
}

fn has_remote_resilience_activity(snap: &StatsSnapshot) -> bool {
    snap.remote_check_roundtrips > 0
        || snap.negative_hits > 0
        || snap.downloads_suppressed > 0
        || snap.uploads_suppressed > 0
        || snap.remote_degraded
}

/// Credential-free remote state rendered by both the client config fallback
/// and the daemon's effective-config snapshot.
fn remote_status(
    remote_description: Option<&str>,
    local_only: bool,
    remote_error: Option<&str>,
) -> String {
    if let Some(remote) = remote_description {
        remote.to_string()
    } else if local_only {
        "local-only mode (remote + planner ignored)".to_string()
    } else if let Some(reason) = remote_error {
        format!("MISCONFIGURED — {reason}")
    } else {
        "not configured".to_string()
    }
}

/// One warning line per rendered stats field where this process's resolved
/// config disagrees with the daemon's effective config
/// (kunobi-ninja/kache#689). Each line names both values and both sources, so
/// "the daemon shows 50 GiB after I set 117 GiB" reads as the config-delivery
/// problem it is, never as "kache ignores config files". Empty when the two
/// agree. Config provenance is itself meaningful: different resolved paths
/// warn even when their current rendered values happen to match.
/// Pure (no I/O) so every divergence branch is unit-testable without a daemon.
pub(crate) fn config_mismatch_warnings(
    config: &Config,
    provenance: &crate::config::ConfigFileProvenance,
    eff: &daemon::EffectiveConfig,
) -> Vec<String> {
    let daemon_side = format!(
        "daemon (started {}, config {})",
        format_epoch_ms_utc(eff.started_at_ms),
        eff.config_path
    );
    let client_side = format!("this process's config ({})", provenance.path.display());
    let remedy = format!(
        "the daemon's value is in effect; edit its watched config ({}) and let it restart; \
         environment overrides require restarting it from an environment it inherits",
        eff.config_path
    );

    let mut warnings = Vec::new();
    if eff.config_path != provenance.path.display().to_string() {
        warnings.push(format!(
            "warning: daemon loaded config {}; this process resolved {} — values may diverge; \
             edit the daemon's watched config to apply persistent changes",
            eff.config_path,
            provenance.path.display(),
        ));
    } else if eff
        .config_fingerprint
        .as_deref()
        .is_some_and(|fingerprint| fingerprint != provenance.fingerprint)
    {
        warnings.push(format!(
            "warning: daemon and this process read different snapshots of config {} — the \
             daemon's loaded values remain in effect until its watched-file restart completes",
            eff.config_path,
        ));
    }
    if eff.max_size != config.max_size {
        warnings.push(format!(
            "warning: {daemon_side} has local_max_size={}; {client_side} says {} — {remedy}",
            ByteSize(eff.max_size),
            ByteSize(config.max_size),
        ));
    }
    if eff.cache_dir != config.cache_dir.display().to_string() {
        warnings.push(format!(
            "warning: {daemon_side} has local_store={}; {client_side} says {} — the daemon's \
             numbers describe ITS store; {remedy}",
            eff.cache_dir,
            config.cache_dir.display(),
        ));
    }
    if !eff.runtime_dir.is_empty() && eff.runtime_dir != config.runtime_dir.display().to_string() {
        warnings.push(format!(
            "warning: {daemon_side} has runtime_dir={}; {client_side} says {} — {remedy}",
            eff.runtime_dir,
            config.runtime_dir.display(),
        ));
    }
    if eff.prefetch_enabled != config.prefetch_enabled {
        warnings.push(format!(
            "warning: {daemon_side} has prefetch_enabled={}; {client_side} says {} — {remedy}",
            eff.prefetch_enabled, config.prefetch_enabled,
        ));
    }
    let daemon_remote = remote_status(
        eff.remote_description.as_deref(),
        eff.local_only,
        eff.remote_error.as_deref(),
    );
    let client_remote = remote_status(
        config
            .remote
            .as_ref()
            .map(|remote| remote.describe())
            .as_deref(),
        config.local_only,
        config.remote_error.as_deref(),
    );
    if daemon_remote != client_remote {
        warnings.push(format!(
            "warning: {daemon_side} has remote={daemon_remote}; {client_side} says \
             {client_remote} — {remedy}"
        ));
    }
    if (eff.remote_description.is_some() || config.remote.is_some())
        && eff.remote_key_cache_refresh_secs != config.remote_key_cache_refresh_secs
    {
        warnings.push(format!(
            "warning: {daemon_side} has remote_key_cache_refresh_secs={}; {client_side} says {} \
             — {remedy}",
            eff.remote_key_cache_refresh_secs, config.remote_key_cache_refresh_secs,
        ));
    }
    warnings
}

/// Format a Unix-millisecond timestamp as a short UTC datetime for the
/// mismatch warnings; `0` (an old daemon's serde default) stays honest as
/// "unknown time" instead of claiming 1970.
fn format_epoch_ms_utc(ms: u64) -> String {
    if ms == 0 {
        return "unknown time".to_string();
    }
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms as i64)
        .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "unknown time".to_string())
}

// ── kache report ──────────────────────────────────────────────────────────

pub fn report(
    config: &Config,
    format: &str,
    window: SinceWindow,
    root: Option<std::path::PathBuf>,
    output: Option<std::path::PathBuf>,
    top: usize,
) -> Result<()> {
    let report = if root.is_some() {
        let filter = crate::report::ReportFilter { root };
        crate::report::generate_report_with_filter(config, window, top, &filter)?
    } else {
        crate::report::generate_report(config, window, top)?
    };

    let text = match format {
        "json" => crate::report::format_json(&report)?,
        "trace" | "perfetto" | "chrome-trace" => crate::report::format_trace_json(&report)?,
        "markdown" | "md" => crate::report::format_markdown(&report),
        "github" | "gh" => crate::report::format_github(&report),
        _ => crate::report::format_text(&report),
    };

    if let Some(path) = output {
        std::fs::write(&path, &text)
            .with_context(|| format!("writing report to {}", path.display()))?;
        eprintln!("Report written to {}", path.display());
    } else {
        println!("{text}");
    }

    Ok(())
}

// ── kache why-miss ─────────────────────────────────────────────────────────

/// Truncate a cache key to its 12-char hex prefix for display.
fn key_short(key: &str) -> &str {
    if key.len() > 12 { &key[..12] } else { key }
}

/// Format a SQLite datetime string (e.g. "2024-03-12 10:30:00") as a
/// human-readable relative time like "2h ago", "3d ago", etc.
fn format_relative_time(sqlite_dt: &str) -> String {
    let parsed = chrono::NaiveDateTime::parse_from_str(sqlite_dt, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|naive| {
            chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc)
        });

    match parsed {
        Some(dt) => {
            let dur = chrono::Utc::now().signed_duration_since(dt);
            let secs = dur.num_seconds().max(0);
            if secs < 60 {
                "just now".to_string()
            } else if secs < 3600 {
                format!("{}m ago", secs / 60)
            } else if secs < 86400 {
                format!("{}h ago", secs / 3600)
            } else {
                format!("{}d ago", secs / 86400)
            }
        }
        None => sqlite_dt.to_string(),
    }
}

/// Diagnose cache misses for a specific crate by inspecting the event log
/// and the local store.
/// The store-failure banner for [`why_miss`], or `None` when the compile was
/// stored normally (kunobi-ninja/kache#629).
///
/// A compile that ran and could not be stored answers the question outright, and
/// it outranks the key analysis `why_miss` prints below it: the key never got
/// the chance to matter, because nothing was written for a later build to match
/// against. Deliberately not labelled `Diagnosis:` — that heading belongs to the
/// stored-entry analysis, and two of them read as contradictory findings.
fn store_failure_banner(miss: &crate::events::BuildEvent) -> Option<String> {
    if miss.store_error.is_empty() {
        return None;
    }
    Some(format!(
        "  NOT CACHED: this compile ran and its outputs failed to store,\n  \
         so the crate misses on every build until the cause is fixed.\n    \
         reason: {}\n  \
         (the key analysis below is secondary: nothing was stored to match)",
        miss.store_error
    ))
}

/// The exact-key lookup-rejection diagnosis for [`why_miss`].
///
/// Without this persisted pre-compile reason, a replacement entry written
/// under the same key makes the current store look like a successful cold
/// population. With other entries present, the old fallback was worse: it
/// claimed a key mismatch even though lookup found the exact key (#655).
fn lookup_rejection_banner(
    miss: &crate::events::BuildEvent,
    same_key_present: bool,
) -> Option<String> {
    if miss.lookup_rejection.is_empty() {
        return None;
    }
    let replacement = if same_key_present {
        "currently present under the same key"
    } else {
        "not currently present in the local store"
    };
    Some(format!(
        "  Diagnosis: matching key was found but rejected before restore\n    \
         reason: {}\n    \
         replacement: {replacement}",
        miss.lookup_rejection
    ))
}

/// Conservative fallback for events written before lookup rejections were
/// persisted. Seeing the same non-empty key earlier proves that the miss was
/// not caused by different inputs, but old logs cannot distinguish cleanup,
/// eviction, corruption, or rejection.
fn legacy_repeated_same_key_banner(
    miss: &crate::events::BuildEvent,
    prior_same_key_miss: bool,
) -> Option<&'static str> {
    if miss.schema >= 15 || !miss.lookup_rejection.is_empty() || !prior_same_key_miss {
        return None;
    }
    Some(
        "  Diagnosis: repeated miss for the same cache key\n    \
         this older event did not record whether the entry was absent, evicted, invalid, or rejected\n    \
         the miss was not caused by a cache-key change",
    )
}

pub fn why_miss(config: &Config, crate_name: &str, json: bool) -> Result<()> {
    let all_events = events::read_events(&config.event_log_path())?;
    let crate_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.crate_name == crate_name)
        .collect();

    if crate_events.is_empty() {
        if json {
            #[derive(serde::Serialize)]
            struct Body<'a> {
                crate_name: &'a str,
                diagnosis: &'static str,
            }
            return crate::machine::emit(
                "why-miss",
                Body {
                    crate_name,
                    diagnosis: "no_events",
                },
                vec![crate::machine::NextAction {
                    argv: vec![
                        "cargo".into(),
                        "build".into(),
                        "-p".into(),
                        crate_name.into(),
                    ],
                    why: "no wrapper events yet; build the crate first".into(),
                }],
            );
        }
        println!("No events found for `{crate_name}`.");
        println!("\nTip: Build the crate first, then re-run this command:");
        println!("  cargo build -p {crate_name}");
        return Ok(());
    }

    // ── Find last entry miss ───────────────────────────────────────────
    let last_miss = crate_events.iter().rev().find(|e| {
        matches!(
            e.result,
            events::EventResult::Dup | events::EventResult::Miss
        )
    });

    if last_miss.is_none() {
        if json {
            #[derive(serde::Serialize)]
            struct Body<'a> {
                crate_name: &'a str,
                diagnosis: &'static str,
            }
            return crate::machine::emit(
                "why-miss",
                Body {
                    crate_name,
                    diagnosis: "all_hits",
                },
                Vec::new(),
            );
        }
        println!("No misses or dups found for `{crate_name}` -- all events are hits!");
        println!("\nRecent events:");
        for event in crate_events.iter().rev().take(5).rev() {
            let time = event.ts.format("%Y-%m-%dT%H:%M:%S");
            println!(
                "  [{time}] {:<14} key: {}  {}",
                event.result.to_string(),
                key_short(&event.cache_key),
                ByteSize(event.size),
            );
        }
        return Ok(());
    }

    let miss = last_miss.unwrap();
    let last_miss_index = crate_events
        .iter()
        .position(|event| std::ptr::eq(*event, *miss))
        .expect("last miss came from crate_events");
    let prior_same_key_miss = last_miss_index > 0 && !miss.cache_key.is_empty() && {
        let previous = crate_events[last_miss_index - 1];
        previous.cache_key == miss.cache_key
            && matches!(
                previous.result,
                events::EventResult::Dup | events::EventResult::Miss
            )
    };
    if json {
        return why_miss_json(config, crate_name, miss, prior_same_key_miss);
    }

    // ── Header ─────────────────────────────────────────────────────────
    println!("Why `{crate_name}` missed:\n");

    let miss_time = miss.ts.format("%Y-%m-%dT%H:%M:%S");
    let miss_key_display = key_short(&miss.cache_key);
    println!(
        "  Last {}: {miss_time} (key: {miss_key_display})",
        miss.result
    );

    // A compile that ran and could not be stored answers the question outright,
    // and it outranks the key-diff analysis below: the key never got the chance
    // to matter, because nothing was written for a later build to match against
    // (kunobi-ninja/kache#629).
    if let Some(banner) = store_failure_banner(miss) {
        println!();
        println!("{banner}");
    }

    // Show miss metadata if it was subsequently stored
    if !miss.cache_key.is_empty() {
        let meta_path = config.store_dir().join(&miss.cache_key).join("meta.json");
        if let Ok(content) = std::fs::read_to_string(&meta_path)
            && let Ok(meta) = serde_json::from_str::<crate::store::EntryMeta>(&content)
        {
            if !meta.target.is_empty() {
                println!("    target:   {}", meta.target);
            }
            if !meta.profile.is_empty() {
                println!("    profile:  {}", meta.profile);
            }
            if !meta.features.is_empty() {
                println!("    features: {}", meta.features.join(", "));
            }
        }
    }

    // ── Stored entries for this crate ──────────────────────────────────
    let store = Store::open(config)?;
    let all_entries = store.list_entries("name")?;
    let stored: Vec<_> = all_entries
        .iter()
        .filter(|e| e.crate_name == crate_name)
        .collect();

    println!();

    if stored.is_empty() {
        println!("  Stored entries for `{crate_name}`: (none)");
        println!();
        if let Some(banner) = lookup_rejection_banner(miss, false) {
            println!("{banner}");
        } else if let Some(banner) = legacy_repeated_same_key_banner(miss, prior_same_key_miss) {
            println!("{banner}");
        } else {
            println!("  Diagnosis: never cached -- first build of this crate");
        }
    } else {
        // Show stored entries (cap at 10 most recent)
        println!(
            "  Stored entries for `{crate_name}` ({} total):",
            stored.len()
        );
        let show_count = stored.len().min(10);
        let hidden = stored.len().saturating_sub(10);
        for entry in stored.iter().rev().take(show_count) {
            let ek = key_short(&entry.cache_key);
            let accessed = format_relative_time(&entry.last_accessed);
            let size = ByteSize(entry.size);
            let hits = entry.hit_count;
            let profile_tag = if entry.profile.is_empty() {
                String::new()
            } else {
                format!(", profile: {}", entry.profile)
            };
            let crate_type_tag = if entry.crate_type.is_empty() {
                String::new()
            } else {
                format!(", type: {}", entry.crate_type)
            };
            let match_indicator = if entry.cache_key == miss.cache_key {
                " <-- entry-miss key (stored after compile)"
            } else {
                ""
            };

            // Read meta.json for richer diff info
            let mut features_tag = String::new();
            let mut target_tag = String::new();
            let meta_path = store.entry_dir(&entry.cache_key).join("meta.json");
            if let Ok(content) = std::fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<crate::store::EntryMeta>(&content)
            {
                if !meta.features.is_empty() {
                    features_tag = format!(", features: [{}]", meta.features.join(", "));
                }
                if !meta.target.is_empty() {
                    target_tag = format!(", target: {}", meta.target);
                }
            }

            println!(
                "    - key: {ek} (last accessed: {accessed}, size: {size}, hits: {hits}{profile_tag}{crate_type_tag}{target_tag}{features_tag}){match_indicator}"
            );
        }
        if hidden > 0 {
            println!("    ... and {hidden} older entries");
        }

        // ── Diagnosis ──────────────────────────────────────────────────
        println!();

        let miss_key_stored = stored.iter().any(|e| e.cache_key == miss.cache_key);
        let other_entries: Vec<_> = stored
            .iter()
            .filter(|e| e.cache_key != miss.cache_key)
            .collect();

        if let Some(banner) = lookup_rejection_banner(miss, miss_key_stored) {
            println!("{banner}");
        } else if let Some(banner) = legacy_repeated_same_key_banner(miss, prior_same_key_miss) {
            println!("{banner}");
        } else if miss_key_stored && !other_entries.is_empty() {
            println!(
                "  Diagnosis: key mismatch -- {} other entr{} exist but {} matched the current build inputs",
                other_entries.len(),
                if other_entries.len() == 1 { "y" } else { "ies" },
                if other_entries.len() == 1 {
                    "it"
                } else {
                    "none"
                },
            );
            why_miss_diff_entries(config, &store, miss, &other_entries);
        } else if miss_key_stored {
            println!("  Diagnosis: first build with these inputs -- entry is now cached");
        } else if !other_entries.is_empty() {
            println!(
                "  Diagnosis: key mismatch -- {} entr{} exist but none match key {}",
                other_entries.len(),
                if other_entries.len() == 1 { "y" } else { "ies" },
                miss_key_display,
            );
            why_miss_diff_entries(config, &store, miss, &other_entries);
        } else {
            println!("  Diagnosis: no matching entries found");
        }
    }

    // ── Dependency cascade ────────────────────────────────────────────
    // The diagnosis above compares this crate's stored entries against each
    // other, which in a cascade says the same undifferentiated thing for every
    // crate downstream of the one that actually moved. Walk the recorded
    // dependency digests instead and name the crate at the bottom (#609).
    print_extern_chain(&all_events, miss, config.explain_miss);

    // ── Recent event history ──────────────────────────────────────────
    println!("\n  Recent events:");
    let recent: Vec<_> = crate_events.iter().rev().take(5).collect();
    for event in recent.iter().rev() {
        let time = event.ts.format("%H:%M:%S");
        let ek = key_short(&event.cache_key);
        let elapsed = if event.elapsed_ms > 1000 {
            format!("{:.1}s", event.elapsed_ms as f64 / 1000.0)
        } else {
            format!("{}ms", event.elapsed_ms)
        };
        println!(
            "    [{time}] {:<14} key: {ek}  {elapsed}  {}",
            event.result.to_string(),
            ByteSize(event.size),
        );
    }

    // ── Key changed hint ──────────────────────────────────────────────
    let last_hit = crate_events.iter().rev().find(|e| {
        matches!(
            e.result,
            events::EventResult::LocalHit
                | events::EventResult::RemoteHit
                | events::EventResult::PrefetchHit
        )
    });

    if let (Some(hit), Some(miss_ev)) = (last_hit, last_miss)
        && hit.cache_key != miss_ev.cache_key
        && miss_ev.ts > hit.ts
    {
        println!(
            "\n  Key changed: {} (last hit) -> {} ({})",
            key_short(&hit.cache_key),
            key_short(&miss_ev.cache_key),
            miss_ev.result,
        );
    }

    // ── Active key salt ───────────────────────────────────────────────
    // The salt is folded into every key but isn't recorded per entry, so a
    // salt change can't be diffed against a stored entry — it shifts the key
    // wholesale and looks like a clean miss. Surfacing the active salt makes
    // that cause visible: a stray machine-global `KACHE_KEY_SALT`, or a
    // rotated salt, alone explains every miss here.
    if let Some(salt) = config.key_salt.as_deref().filter(|s| !s.is_empty()) {
        println!("\n  Active key_salt: {salt:?}");
        println!(
            "    (folded into every key; if it changed or was set unexpectedly since the \
             last hit, that alone shifts the key and explains the miss)"
        );
    }

    println!("\n  For full key component details, run:");
    println!(
        "    KACHE_LOG=trace cargo build -p {crate_name} 2>&1 | grep '\\[key:{crate_name}\\]'"
    );

    Ok(())
}

/// Render the `extern:` cascade for a miss, when one is recorded (#609).
///
/// Prints nothing when the miss is not downstream of a dependency change, so
/// the ordinary single-crate case reads exactly as before. When the digests
/// were never recorded, says how to turn them on rather than staying silent
/// about a diagnosis it could have given.
fn print_extern_chain(
    all_events: &[events::BuildEvent],
    miss: &events::BuildEvent,
    explain_miss: bool,
) {
    // The walk is driven by position in the oldest-first slice, so the exact
    // event has to be located rather than re-found by name and timestamp
    // (which can collide).
    let Some(miss_index) = all_events.iter().position(|e| std::ptr::eq(e, miss)) else {
        return;
    };
    let Some(chain) = crate::miss_chain::analyze(all_events, miss_index) else {
        if !explain_miss && miss.key_externs.is_empty() {
            println!(
                "\n  Dependency cascade: not analyzed (no per-dependency digests recorded).\n    \
                 Enable [cache] explain_miss to record them, then rebuild."
            );
        }
        return;
    };

    let direct: Vec<&str> = chain.direct.iter().map(|d| d.name.as_str()).collect();
    println!(
        "\n  Dependency cascade: this miss is downstream of {} that changed ({})",
        if direct.len() == 1 {
            "a dependency".to_string()
        } else {
            format!("{} dependencies", direct.len())
        },
        direct.join(", ")
    );

    for root in &chain.roots {
        let via = if root.branches > 1 {
            format!(" (reached by {} branches)", root.branches)
        } else {
            String::new()
        };
        match &root.kind {
            crate::miss_chain::RootKind::Groups(groups) => println!(
                "    root: {}{via} -- own inputs changed: {}",
                root.crate_name,
                groups.join(", ")
            ),
            crate::miss_chain::RootKind::NothingRecorded => println!(
                "    root: {}{via} -- dependencies stable and no traced input group changed \
                 (key salt or extra inputs?)",
                root.crate_name
            ),
            // Everything below is an unresolved endpoint, not a cause. Worded
            // so it can't be read as "this crate is why you missed".
            crate::miss_chain::RootKind::NoMissRecorded => println!(
                "    unresolved: {}{via} -- artifact differs, but it has no compile recorded in \
                 this event window",
                root.crate_name
            ),
            crate::miss_chain::RootKind::NoBaseline => println!(
                "    unresolved: {}{via} -- nothing earlier to compare it against",
                root.crate_name
            ),
            crate::miss_chain::RootKind::NoDiffableHistory => println!(
                "    unresolved: {}{via} -- its own dependency history is not comparable, so the \
                 cascade may continue below it",
                root.crate_name
            ),
            crate::miss_chain::RootKind::LimitReached => println!(
                "    unresolved: {}{via} -- still descending when the walk limit was reached",
                root.crate_name
            ),
        }
        if root.path.len() > 1 {
            let mut path: Vec<&str> = root.path.iter().map(|h| h.crate_name.as_str()).collect();
            path.push(root.crate_name.as_str());
            println!("      via: {}", path.join(" <- "));
        }
        for group in &root.passthroughs {
            println!(
                "      {}x uncached compile in {}: {}",
                group.count, root.crate_name, group.reason
            );
        }
        if !root.passthroughs.is_empty() {
            println!(
                "      (uncached compiles make the artifact vary per checkout; attributed by \
                 package directory)"
            );
        }
    }

    if !chain.has_resolved_root() {
        println!(
            "    note: no endpoint could be resolved -- the recorded history does not explain \
             this miss"
        );
    }
    if let Some(reason) = chain.truncated {
        println!("    note: walk stopped early -- {reason}");
    }
}

fn why_miss_json(
    config: &Config,
    crate_name: &str,
    miss: &events::BuildEvent,
    prior_same_key_miss: bool,
) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Body<'a> {
        crate_name: &'a str,
        diagnosis: &'a str,
        last_result: String,
        cache_key: &'a str,
        store_error: &'a str,
        lookup_rejection: &'a str,
        stored_entries: usize,
    }

    let store = Store::open(config)?;
    let stored: Vec<_> = store
        .list_entries("name")?
        .into_iter()
        .filter(|e| crate_name_matches(crate_name, &e.crate_name))
        .collect();
    let miss_key_stored = stored
        .iter()
        .any(|e| cache_key_matches(&e.cache_key, &miss.cache_key));
    let diagnosis = why_miss_diagnosis(
        &miss.store_error,
        &miss.lookup_rejection,
        stored.len(),
        miss_key_stored,
    );
    let _ = prior_same_key_miss;
    crate::machine::emit(
        "why-miss",
        Body {
            crate_name,
            diagnosis,
            last_result: miss.result.to_string(),
            cache_key: &miss.cache_key,
            store_error: &miss.store_error,
            lookup_rejection: &miss.lookup_rejection,
            stored_entries: stored.len(),
        },
        Vec::new(),
    )
}

fn why_miss_diagnosis(
    store_error: &str,
    lookup_rejection: &str,
    stored_entries: usize,
    miss_key_stored: bool,
) -> &'static str {
    if !store_error.is_empty() {
        "not_cached"
    } else if !lookup_rejection.is_empty() {
        "lookup_rejected"
    } else if stored_entries == 0 {
        "never_cached"
    } else if miss_key_stored {
        "first_build_now_cached"
    } else {
        "key_mismatch"
    }
}

fn cache_key_matches(stored: &str, missed: &str) -> bool {
    stored == missed
}

/// Compare the miss event's stored metadata against other stored entries
/// to surface what likely differs (target, profile, features).
fn why_miss_diff_entries(
    config: &Config,
    store: &Store,
    miss: &events::BuildEvent,
    other_entries: &[&&crate::store::EntryInfo],
) {
    // Load metadata for the miss key (if stored)
    let miss_meta = if !miss.cache_key.is_empty() {
        let meta_path = config.store_dir().join(&miss.cache_key).join("meta.json");
        std::fs::read_to_string(&meta_path)
            .ok()
            .and_then(|c| serde_json::from_str::<crate::store::EntryMeta>(&c).ok())
    } else {
        None
    };

    let Some(miss_meta) = miss_meta else {
        return;
    };

    let mut other_metas = Vec::new();

    for entry in other_entries {
        let meta_path = store.entry_dir(&entry.cache_key).join("meta.json");
        let other_meta = std::fs::read_to_string(&meta_path)
            .ok()
            .and_then(|c| serde_json::from_str::<crate::store::EntryMeta>(&c).ok());

        let Some(other) = other_meta else {
            continue;
        };

        other_metas.push((key_short(&entry.cache_key).to_string(), other));
    }

    let (diffs, extra) = why_miss_diff_messages(
        &miss_meta,
        other_metas.iter().map(|(ek, meta)| (ek.as_str(), meta)),
        5,
    );
    if !diffs.is_empty() {
        println!("  Differences detected:");
        for diff in &diffs {
            println!("    - {diff}");
        }
        if extra > 0 {
            println!("    ... and {extra} more");
        }
    }
}

fn why_miss_diff_messages<'a, I>(
    miss_meta: &crate::store::EntryMeta,
    other_entries: I,
    limit: usize,
) -> (Vec<String>, usize)
where
    I: IntoIterator<Item = (&'a str, &'a crate::store::EntryMeta)>,
{
    let mut diffs: Vec<String> = Vec::new();
    for (ek, other) in other_entries {
        if miss_meta.target != other.target {
            diffs.push(format!(
                "different target vs {ek}: \"{}\" vs \"{}\"",
                miss_meta.target, other.target
            ));
        }
        if miss_meta.profile != other.profile {
            diffs.push(format!(
                "different profile vs {ek}: \"{}\" vs \"{}\"",
                miss_meta.profile, other.profile
            ));
        }
        if miss_meta.features != other.features {
            let miss_feats = if miss_meta.features.is_empty() {
                "(none)".to_string()
            } else {
                miss_meta.features.join(", ")
            };
            let other_feats = if other.features.is_empty() {
                "(none)".to_string()
            } else {
                other.features.join(", ")
            };
            diffs.push(format!(
                "different features vs {ek}: [{miss_feats}] vs [{other_feats}]"
            ));
        }
        if miss_meta.crate_types != other.crate_types {
            diffs.push(format!(
                "different crate types vs {ek}: {:?} vs {:?}",
                miss_meta.crate_types, other.crate_types
            ));
        }

        if miss_meta.target == other.target
            && miss_meta.profile == other.profile
            && miss_meta.features == other.features
            && miss_meta.crate_types == other.crate_types
        {
            diffs.push(format!(
                "same config as {ek} -- likely source code, dependency, or rustc version change"
            ));
        }
    }

    let mut unique_diffs: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for diff in &diffs {
        // Normalize: strip the key prefix to group identical diagnoses.
        let normalized = if let Some(pos) = diff.find(" -- ") {
            diff[pos..].to_string()
        } else {
            diff.clone()
        };
        if seen.insert(normalized) {
            unique_diffs.push(diff.clone());
        }
    }

    let extra = unique_diffs.len().saturating_sub(limit);
    (unique_diffs.into_iter().take(limit).collect(), extra)
}

pub fn format_duration_ms(ms: u64) -> String {
    let secs = ms / 1000;
    if secs >= 3600 {
        format!("~{:.1}h", secs as f64 / 3600.0)
    } else if secs >= 60 {
        format!("~{:.0}min", secs as f64 / 60.0)
    } else if secs > 0 {
        format!("~{secs}s")
    } else {
        format!("~{ms}ms")
    }
}

/// How an eviction sweep went, phrased so "0" is never left unexplained.
///
/// `kache gc` used to print a bare `evicted 0 entries` next to a store sitting
/// at 912% of its limit. That is correct behaviour reported terribly: every
/// candidate was within the idle grace, so the sweep deliberately left them. A
/// user who reads "0" beside "912%" concludes GC is broken and stops trying —
/// which is plausibly how #497 became a 113 GB bug report rather than a
/// self-service fix (kunobi-ninja/kache#509).
///
/// Pure so the phrasing is unit-testable without running a sweep.
pub(crate) fn describe_eviction(stats: &crate::store::GcStats, over_limit: bool) -> String {
    let grace_secs = crate::store::EVICTION_IDLE_GRACE.as_secs();
    let plural = |n: usize| if n == 1 { "entry" } else { "entries" };

    if stats.entries_evicted > 0 {
        let mut msg = format!(
            " dropped {} {} from the store ({}).",
            stats.entries_evicted,
            plural(stats.entries_evicted),
            ByteSize(stats.bytes_freed)
        );
        msg.push_str(&format!(
            "\n  {} became free on disk.",
            ByteSize(stats.disk_bytes_reclaimed)
        ));
        let leftover = stats.bytes_freed.saturating_sub(stats.disk_bytes_reclaimed);
        if leftover > 0 {
            msg.push_str(&format!(
                "\n  {} remains cloned in build outputs.",
                ByteSize(leftover)
            ));
        }
        if stats.entries_pinned > 0 {
            msg.push_str(&format!(
                "\n  {} more {} accessed within the last {grace_secs}s or awaiting a durable \
                 remote upload and left in place; re-run `kache gc` once builds and uploads \
                 are idle.",
                stats.entries_pinned,
                plural(stats.entries_pinned),
            ));
        }
        if stats.entries_unreclaimable > 0 {
            msg.push_str(&format!(
                "\n  {} {} left in place because clones still hold their blocks. \
                 Inspect stale outputs with `kache clean --tracked --stale 14d --dry-run`, then run `kache gc` again.",
                stats.entries_unreclaimable,
                plural(stats.entries_unreclaimable),
            ));
        }
        return msg;
    }

    if stats.entries_unreclaimable > 0 {
        return format!(
            " nothing reclaimable on disk.\n  {} {} cloned into build outputs \
             (same bytes as target/, not extra).\n  Remove stale outputs with \
             `kache clean --tracked --stale 14d --dry-run`, then run `kache gc` again.",
            stats.entries_unreclaimable,
            plural(stats.entries_unreclaimable),
        );
    }

    // Nothing evicted. Say why, because this is the case that reads as a bug.
    if stats.entries_pinned > 0 {
        return format!(
            " evicted 0 entries.\n  {} {} were selected but accessed within the last \
             {grace_secs}s or are awaiting a durable remote upload, so they were left in \
             place. Re-run `kache gc` once builds and uploads are idle.",
            stats.entries_pinned,
            plural(stats.entries_pinned),
        );
    }
    if over_limit {
        return format!(
            " evicted 0 entries.\n  The store is still over its limit but nothing was \
             eligible. Entries accessed within the last {grace_secs}s are never evicted; \
             if this persists with no builds running, `kache doctor --verify` will \
             report entries that cannot be removed."
        );
    }
    " evicted 0 entries (nothing to evict).".to_string()
}

/// Is the store over its configured budget after a sweep?
///
/// Pure, and split out of the two `gc` paths for the same reason
/// [`describe_eviction`] is: it decides which of the two "evicted 0" messages a
/// user sees, and that decision is worth testing without running a sweep. A
/// store whose size cannot be read is reported as within budget — the same
/// direction the callers already took, since claiming "over limit" on missing
/// data would send the user chasing an eviction problem they may not have.
pub(crate) fn store_over_limit(total_size: Option<u64>, max_size: u64) -> bool {
    total_size.is_some_and(|size| size > max_size)
}

// ── Project stats ──────────────────────────────────────────────────────────

#[derive(Default)]
struct ProjectStats {
    total_bytes: u64,
    cached_bytes: u64,
    /// Scan-time estimate of bytes returned if this whole target/ disappears.
    /// Unlike `cached_bytes`, this uses private extents and collapses hardlinks.
    estimated_reclaimable_bytes: u64,
    #[allow(dead_code)] // tracked but not yet surfaced in the clean TUI
    cached_files: u64,
    local_bytes: u64,
    local_files: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectBucket {
    Incremental,
    BuildScripts,
    Fingerprints,
    Binaries,
    Deps,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct FileIdentity {
    device: u64,
    inode: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HardlinkObservation {
    id: FileIdentity,
    total_links: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StorageObservation {
    sharing: crate::sharing::Sharing,
    hardlink: Option<HardlinkObservation>,
}

#[derive(Debug, Clone, Copy)]
struct HardlinkGroup {
    private_bytes: u64,
    links_seen: u64,
    total_links: u64,
}

/// Estimate physical reclaim without treating every shared file as wholly
/// unreclaimable. Reflinks contribute their private extents; hardlinked paths
/// are collapsed by inode and contribute once only when every link is inside
/// this target/. If another hardlink remains elsewhere, deleting this target/
/// cannot remove the inode and contributes zero.
#[derive(Default)]
struct ReclaimEstimator {
    single_link_private_bytes: u64,
    hardlinks: std::collections::HashMap<FileIdentity, HardlinkGroup>,
}

impl ReclaimEstimator {
    fn record(&mut self, size: u64, observation: StorageObservation) {
        let private_bytes = observation.sharing.private_bytes.min(size);
        if let Some(link) = observation.hardlink {
            let group = self.hardlinks.entry(link.id).or_insert(HardlinkGroup {
                private_bytes,
                links_seen: 0,
                total_links: link.total_links.max(1),
            });
            // Metadata can change while the scan runs. The minimum private-byte
            // answer and maximum link count are the conservative combination.
            group.private_bytes = group.private_bytes.min(private_bytes);
            group.links_seen = group.links_seen.saturating_add(1);
            group.total_links = group.total_links.max(link.total_links.max(1));
        } else {
            self.single_link_private_bytes =
                self.single_link_private_bytes.saturating_add(private_bytes);
        }
    }

    fn estimated_reclaimable_bytes(&self) -> u64 {
        self.hardlinks
            .values()
            .filter(|group| group.links_seen >= group.total_links)
            .fold(self.single_link_private_bytes, |total, group| {
                total.saturating_add(group.private_bytes)
            })
    }

    fn hardlink_has_external_ref(&self, id: FileIdentity) -> bool {
        self.hardlinks
            .get(&id)
            .is_some_and(|group| group.links_seen < group.total_links)
    }
}

#[derive(Debug, Clone, Copy)]
struct CacheCandidate {
    size: u64,
    bucket: ProjectBucket,
    reflink_shared: bool,
    hardlink_id: Option<FileIdentity>,
}

#[cfg(unix)]
fn clamp_private_bytes(size: u64, private_bytes: u64, allocated_bytes: Option<u64>) -> u64 {
    let private_bytes = private_bytes.min(size);
    allocated_bytes.map_or(private_bytes, |allocated| {
        private_bytes.min(allocated.min(size))
    })
}

#[cfg(unix)]
fn observe_storage(path: &std::path::Path, meta: &std::fs::Metadata) -> StorageObservation {
    let size = meta.len();
    let mut sharing = crate::sharing::probe(path, size);
    sharing.private_bytes = clamp_private_bytes(
        size,
        sharing.private_bytes,
        Some(meta.blocks().saturating_mul(512)),
    );
    let hardlink = (meta.nlink() > 1).then_some(HardlinkObservation {
        id: FileIdentity {
            device: meta.dev(),
            inode: meta.ino(),
        },
        total_links: meta.nlink(),
    });
    StorageObservation { sharing, hardlink }
}

/// Reconstruct Windows' 64-bit file index from the two DWORDs returned by
/// `GetFileInformationByHandle`. Kept platform-neutral so the Linux mutation
/// lane can exercise the packing rule that the Windows syscall wrapper uses.
#[cfg_attr(not(windows), allow(dead_code))]
fn windows_file_identity_from_parts(
    volume_serial: u32,
    file_index_high: u32,
    file_index_low: u32,
) -> FileIdentity {
    FileIdentity {
        device: u64::from(volume_serial),
        inode: (u64::from(file_index_high) << 32).saturating_add(u64::from(file_index_low)),
    }
}

/// Turn a successful or failed Windows identity query into conservative
/// reclaim evidence. This is pure so every link-count boundary remains covered
/// on the Linux mutation workers as well as by the hosted Windows tests.
#[cfg_attr(not(windows), allow(dead_code))]
fn windows_storage_observation(
    size: u64,
    identity: Option<(FileIdentity, u64)>,
) -> StorageObservation {
    match identity {
        Some((id, total_links)) => StorageObservation {
            sharing: crate::sharing::Sharing::unknown_for(size),
            hardlink: (total_links > 1).then_some(HardlinkObservation { id, total_links }),
        },
        None => StorageObservation {
            // Without a stable identity we cannot rule out an external
            // hardlink, so claiming the file's full length would overstate
            // physical reclaim. Omit it from the estimate instead.
            sharing: crate::sharing::Sharing {
                shared: false,
                private_bytes: 0,
            },
            hardlink: None,
        },
    }
}

#[cfg(windows)]
fn query_windows_file_identity(path: &std::path::Path) -> Option<(FileIdentity, u64)> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_FLAG_BACKUP_SEMANTICS, GetFileInformationByHandle,
    };

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .ok()?;
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    (ok != 0).then_some((
        windows_file_identity_from_parts(
            info.dwVolumeSerialNumber,
            info.nFileIndexHigh,
            info.nFileIndexLow,
        ),
        u64::from(info.nNumberOfLinks),
    ))
}

#[cfg(windows)]
fn observe_storage_windows(path: &std::path::Path, meta: &std::fs::Metadata) -> StorageObservation {
    windows_storage_observation(meta.len(), query_windows_file_identity(path))
}

#[cfg(windows)]
use self::observe_storage_windows as observe_storage;

/// Conservative fallback for targets without a native sharing/identity probe.
/// The pure helper keeps the actual fallback value visible to Linux mutation
/// testing even though the platform wrapper itself is cfg-only.
#[cfg_attr(any(unix, windows), allow(dead_code))]
fn unsupported_storage_observation(size: u64) -> StorageObservation {
    StorageObservation {
        sharing: crate::sharing::Sharing::unknown_for(size),
        hardlink: None,
    }
}

#[cfg(not(any(unix, windows)))]
fn observe_storage_unsupported(
    _path: &std::path::Path,
    meta: &std::fs::Metadata,
) -> StorageObservation {
    unsupported_storage_observation(meta.len())
}

#[cfg(not(any(unix, windows)))]
use self::observe_storage_unsupported as observe_storage;

fn directory_identity(path: &std::path::Path) -> Option<FileIdentity> {
    let meta = std::fs::symlink_metadata(path).ok()?;
    if !meta.file_type().is_dir() {
        return None;
    }

    #[cfg(unix)]
    {
        Some(FileIdentity {
            device: meta.dev(),
            inode: meta.ino(),
        })
    }
    #[cfg(windows)]
    {
        query_windows_file_identity(path).map(|(identity, _)| identity)
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

fn add_local_bytes(
    stats: &mut ProjectStats,
    breakdown: &mut CategoryBreakdown,
    bucket: ProjectBucket,
    size: u64,
) {
    stats.local_bytes = stats.local_bytes.saturating_add(size);
    stats.local_files = stats.local_files.saturating_add(1);
    match bucket {
        ProjectBucket::Incremental => {
            breakdown.incremental = breakdown.incremental.saturating_add(size)
        }
        ProjectBucket::BuildScripts => {
            breakdown.build_scripts = breakdown.build_scripts.saturating_add(size)
        }
        ProjectBucket::Fingerprints => {
            breakdown.fingerprints = breakdown.fingerprints.saturating_add(size)
        }
        ProjectBucket::Binaries => breakdown.binaries = breakdown.binaries.saturating_add(size),
        ProjectBucket::Deps => breakdown.deps_local = breakdown.deps_local.saturating_add(size),
        ProjectBucket::Other => breakdown.other = breakdown.other.saturating_add(size),
    }
}

fn record_scanned_file(
    stats: &mut ProjectStats,
    breakdown: &mut CategoryBreakdown,
    reclaim: &mut ReclaimEstimator,
    cache_candidates: &mut Vec<CacheCandidate>,
    size: u64,
    bucket: ProjectBucket,
    cache_eligible: bool,
    observation: StorageObservation,
) {
    stats.total_bytes = stats.total_bytes.saturating_add(size);
    reclaim.record(size, observation);
    if cache_eligible {
        cache_candidates.push(CacheCandidate {
            size,
            bucket,
            reflink_shared: observation.sharing.shared,
            hardlink_id: observation.hardlink.map(|link| link.id),
        });
    } else {
        add_local_bytes(stats, breakdown, bucket, size);
    }
}

fn finalize_cache_candidates(
    stats: &mut ProjectStats,
    breakdown: &mut CategoryBreakdown,
    reclaim: &ReclaimEstimator,
    candidates: &[CacheCandidate],
) {
    for candidate in candidates {
        // A hardlink is useful evidence of cache backing only when another link
        // survives outside this target/. Links wholly inside the target are a
        // local link group, not evidence that kache's store retains the inode.
        let cache_backed = candidate.reflink_shared
            || candidate
                .hardlink_id
                .is_some_and(|id| reclaim.hardlink_has_external_ref(id));
        if cache_backed {
            stats.cached_bytes = stats.cached_bytes.saturating_add(candidate.size);
            stats.cached_files = stats.cached_files.saturating_add(1);
        } else {
            add_local_bytes(stats, breakdown, candidate.bucket, candidate.size);
        }
    }
}

fn walk_project_dir(
    dir: &std::path::Path,
    bucket: ProjectBucket,
    cache_eligible: bool,
    stats: &mut ProjectStats,
    breakdown: &mut CategoryBreakdown,
    reclaim: &mut ReclaimEstimator,
    cache_candidates: &mut Vec<CacheCandidate>,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        // Never follow a target/ symlink into storage that clean itself will
        // not recursively remove. Omitting the link's tiny allocation keeps the
        // estimate conservative and, more importantly, bounded to this tree.
        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            walk_project_dir(
                &path,
                bucket,
                cache_eligible,
                stats,
                breakdown,
                reclaim,
                cache_candidates,
            );
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        let observation = observe_storage(&path, &meta);
        record_scanned_file(
            stats,
            breakdown,
            reclaim,
            cache_candidates,
            meta.len(),
            bucket,
            cache_eligible,
            observation,
        );
    }
}

/// Analyze a project's target/ directory: which files share storage with kache's
/// store (reflinked or hardlinked) vs local-only, with per-category breakdown.
fn compute_project_stats(target_dir: &std::path::Path) -> (ProjectStats, CategoryBreakdown) {
    let mut stats = ProjectStats {
        total_bytes: 0,
        cached_bytes: 0,
        estimated_reclaimable_bytes: 0,
        cached_files: 0,
        local_bytes: 0,
        local_files: 0,
    };
    let mut breakdown = CategoryBreakdown::default();
    let mut reclaim = ReclaimEstimator::default();
    let mut cache_candidates = Vec::new();

    let profiles = ["debug", "release", "profiling", "coverage"];
    for profile in &profiles {
        let profile_dir = target_dir.join(profile);
        if !std::fs::symlink_metadata(&profile_dir).is_ok_and(|meta| meta.file_type().is_dir()) {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&profile_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };

            if file_type.is_symlink() {
                continue;
            }

            if file_type.is_dir() {
                match name_str.as_ref() {
                    "incremental" => {
                        walk_project_dir(
                            &path,
                            ProjectBucket::Incremental,
                            false,
                            &mut stats,
                            &mut breakdown,
                            &mut reclaim,
                            &mut cache_candidates,
                        );
                    }
                    ".fingerprint" => {
                        walk_project_dir(
                            &path,
                            ProjectBucket::Fingerprints,
                            false,
                            &mut stats,
                            &mut breakdown,
                            &mut reclaim,
                            &mut cache_candidates,
                        );
                    }
                    "build" => {
                        walk_project_dir(
                            &path,
                            ProjectBucket::BuildScripts,
                            false,
                            &mut stats,
                            &mut breakdown,
                            &mut reclaim,
                            &mut cache_candidates,
                        );
                    }
                    "deps" => {
                        walk_project_dir(
                            &path,
                            ProjectBucket::Deps,
                            true,
                            &mut stats,
                            &mut breakdown,
                            &mut reclaim,
                            &mut cache_candidates,
                        );
                    }
                    _ => {
                        walk_project_dir(
                            &path,
                            ProjectBucket::Other,
                            false,
                            &mut stats,
                            &mut breakdown,
                            &mut reclaim,
                            &mut cache_candidates,
                        );
                    }
                }
            } else if file_type.is_file() {
                let Ok(meta) = std::fs::metadata(&path) else {
                    continue;
                };
                let size = meta.len();
                let binary = is_binary_artifact(&path);
                let bucket = if binary {
                    ProjectBucket::Binaries
                } else {
                    ProjectBucket::Other
                };
                let observation = observe_storage(&path, &meta);
                record_scanned_file(
                    &mut stats,
                    &mut breakdown,
                    &mut reclaim,
                    &mut cache_candidates,
                    size,
                    bucket,
                    !binary,
                    observation,
                );
            }
        }
    }

    // Files directly in target/ (CACHEDIR.TAG, .rustc_info.json, etc.)
    if let Ok(entries) = std::fs::read_dir(target_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if entry.file_type().is_ok_and(|file_type| file_type.is_file())
                && let Ok(meta) = std::fs::metadata(&path)
            {
                let observation = observe_storage(&path, &meta);
                record_scanned_file(
                    &mut stats,
                    &mut breakdown,
                    &mut reclaim,
                    &mut cache_candidates,
                    meta.len(),
                    ProjectBucket::Other,
                    false,
                    observation,
                );
            }
        }
    }

    finalize_cache_candidates(&mut stats, &mut breakdown, &reclaim, &cache_candidates);
    stats.estimated_reclaimable_bytes = reclaim.estimated_reclaimable_bytes();
    (stats, breakdown)
}

/// Whether a file in `target/` is a binary-shaped artifact (executable
/// or dynamic library) for stats bucketing purposes.
///
/// Delegates to [`crate::compiler::classify_by_filename`] so the rustc
/// extension table lives in one place. The extensionless case is treated
/// as a binary because in target/ scans (the only context this is called
/// from) the rustc convention is that bin output has no extension on Unix.
fn is_binary_artifact(path: &std::path::Path) -> bool {
    use crate::compiler::{ArtifactKind, classify_by_filename};
    use crate::link::LinkStrategy;

    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    let kind = classify_by_filename(name);
    match kind {
        // Mutable runtime-loaded artifacts: bin, dylib, etc.
        kind if kind.link_strategy() == LinkStrategy::Copy => true,
        // Convention: extensionless file in target/ = bin output on Unix.
        ArtifactKind::Other("extensionless") => true,
        _ => false,
    }
}

/// List all cached entries, or show details for a specific crate.
pub fn list(
    config: &Config,
    crate_name: Option<&str>,
    sort_by: &str,
    no_pager: bool,
    json: bool,
) -> Result<()> {
    let store = Store::open(config)?;

    if json {
        #[derive(serde::Serialize)]
        struct EntryBody<'a> {
            cache_key: &'a str,
            crate_name: &'a str,
            crate_type: &'a str,
            profile: &'a str,
            size: u64,
            hits: u64,
            created_at: &'a str,
            last_accessed: &'a str,
        }
        let entries = store.list_entries(sort_by)?;
        let matching: Vec<&crate::store::EntryInfo> = if let Some(name) = crate_name {
            entries
                .iter()
                .filter(|e| crate_name_matches(name, &e.crate_name))
                .collect()
        } else {
            entries.iter().collect()
        };
        let body: Vec<EntryBody> = matching
            .iter()
            .map(|e| EntryBody {
                cache_key: &e.cache_key,
                crate_name: &e.crate_name,
                crate_type: &e.crate_type,
                profile: &e.profile,
                size: e.size,
                hits: e.hit_count,
                created_at: &e.created_at,
                last_accessed: &e.last_accessed,
            })
            .collect();
        #[derive(serde::Serialize)]
        struct Body<T> {
            entries: Vec<T>,
        }
        return crate::machine::emit("list", Body { entries: body }, Vec::new());
    }

    if let Some(name) = crate_name {
        // Detail view for a specific crate
        let entries = store.list_entries("name")?;
        let matching: Vec<_> = entries.iter().filter(|e| e.crate_name == name).collect();

        if matching.is_empty() {
            println!("No cached entries for '{name}'.");
            return Ok(());
        }

        let mut lines = Vec::new();
        for entry in &matching {
            lines.push(format!("Cache key: {}", &entry.cache_key[..16]));
            lines.push(format!("  Crate:    {}", entry.crate_name));
            push_nonempty_detail(&mut lines, "  Type:     ", &entry.crate_type);
            push_nonempty_detail(&mut lines, "  Profile:  ", &entry.profile);
            lines.push(format!("  Size:     {}", ByteSize(entry.size)));
            lines.push(format!("  Hits:     {}", entry.hit_count));
            lines.push(format!("  Created:  {}", entry.created_at));
            lines.push(format!("  Accessed: {}", entry.last_accessed));

            let meta_path = store.entry_dir(&entry.cache_key).join("meta.json");
            if let Ok(content) = std::fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<crate::store::EntryMeta>(&content)
            {
                push_nonempty_detail(&mut lines, "  Features: ", &meta.features.join(", "));
                push_nonempty_detail(&mut lines, "  Target:   ", &meta.target);
                lines.push("  Files:".to_string());
                for file in &meta.files {
                    lines.push(format!("    {} ({})", file.name, ByteSize(file.size)));
                }
            }
            lines.push(String::new());
        }
        write_paged(&lines, no_pager);
    } else {
        // Summary view of all entries
        let entries = store.list_entries(sort_by)?;

        if entries.is_empty() {
            println!("No cached entries.");
            return Ok(());
        }

        let mut lines = vec![
            format!(
                "{:<30} {:<10} {:<8} {:>10} {:>6} {:>12} {:>12}",
                "Crate", "Type", "Profile", "Size", "Hits", "Created", "Accessed"
            ),
            "-".repeat(92),
        ];

        for entry in &entries {
            let crate_type = if entry.crate_type.is_empty() {
                "-"
            } else {
                &entry.crate_type
            };
            let profile = if entry.profile.is_empty() {
                "-"
            } else {
                &entry.profile
            };
            lines.push(format!(
                "{:<30} {:<10} {:<8} {:>10} {:>6} {:>12} {:>12}",
                entry.crate_name,
                crate_type,
                profile,
                ByteSize(entry.size).to_string(),
                entry.hit_count,
                &entry.created_at[..10],
                &entry.last_accessed[..10],
            ));
        }

        lines.push(format!("\n{} entries", entries.len()));
        write_paged(&lines, no_pager);
    }

    Ok(())
}

fn crate_name_matches(requested: &str, actual: &str) -> bool {
    requested == actual
}

fn push_nonempty_detail(lines: &mut Vec<String>, prefix: &str, value: &str) {
    if !value.is_empty() {
        lines.push(format!("{prefix}{value}"));
    }
}

/// Resolve the pager to a direct process argv. Quotes group whitespace but are
/// not shell syntax: there is no expansion, operator handling, or interpolation.
fn resolve_pager_argv(
    no_pager: bool,
    stdout_is_terminal: bool,
    kache_pager: Option<&str>,
    pager: Option<&str>,
    is_windows: bool,
) -> Option<Vec<String>> {
    if no_pager || !stdout_is_terminal {
        return None;
    }

    let command = match kache_pager {
        Some(command) => command,
        None => pager.unwrap_or(if is_windows { "more.com" } else { "less -FRX" }),
    };
    let argv = parse_pager_argv(command)?;
    if argv.first().is_none_or(|program| program.is_empty())
        || (argv.len() == 1 && argv[0] == "cat")
    {
        None
    } else {
        Some(argv)
    }
}

/// Split a pager command into argv without invoking a shell. Single and double
/// quotes may group whitespace and are removed; every other character remains
/// literal. An unmatched quote makes the command invalid.
fn parse_pager_argv(command: &str) -> Option<Vec<String>> {
    let mut argv = Vec::new();
    let mut word = String::new();
    let mut quote = None;
    let mut word_started = false;

    for character in command.chars() {
        if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else {
                word.push(character);
            }
            continue;
        }

        match character {
            '\'' | '"' => {
                quote = Some(character);
                word_started = true;
            }
            character if character.is_whitespace() => {
                if word_started {
                    argv.push(std::mem::take(&mut word));
                    word_started = false;
                }
            }
            character => {
                word.push(character);
                word_started = true;
            }
        }
    }

    if quote.is_some() {
        return None;
    }
    if word_started {
        argv.push(word);
    }
    Some(argv)
}

fn write_pager_lines<W: std::io::Write>(writer: &mut W, lines: &[String]) -> bool {
    for line in lines {
        if writer.write_all(line.as_bytes()).is_err() {
            return false;
        }
        if writer.write_all(b"\n").is_err() {
            return false;
        }
    }
    true
}

/// Write output lines to a pager when stdout is a terminal, else plain stdout.
/// `KACHE_PAGER` > `$PAGER` > platform default; `cat` or empty disables. Invalid
/// commands and spawn failures fall back to plain output. An early pager exit
/// stops further delivery without failing the command or reprinting the listing.
fn write_paged(lines: &[String], no_pager: bool) {
    let plain = || {
        for line in lines {
            println!("{line}");
        }
    };

    let kache_pager = std::env::var_os("KACHE_PAGER");
    let pager = std::env::var_os("PAGER");
    let Some(argv) = resolve_pager_argv(
        no_pager,
        std::io::stdout().is_terminal(),
        kache_pager
            .as_deref()
            .map(|value| value.to_str().unwrap_or("")),
        pager.as_deref().map(|value| value.to_str().unwrap_or("")),
        cfg!(windows),
    ) else {
        plain();
        return;
    };

    let mut argv = argv.into_iter();
    let Some(program) = argv.next() else {
        plain();
        return;
    };
    let mut child = match std::process::Command::new(program)
        .args(argv)
        .stdin(std::process::Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => {
            plain();
            return;
        }
    };

    if let Some(stdin) = child.stdin.as_mut() {
        let _ = write_pager_lines(stdin, lines);
    }
    drop(child.stdin.take());
    let _ = child.wait();
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcMode {
    Cli,
    Background,
}

impl GcMode {
    pub fn from_env() -> Self {
        if std::env::var_os("KACHE_AUTO_GC_WORKER").is_some() {
            GcMode::Background
        } else {
            GcMode::Cli
        }
    }
}

/// Run garbage collection locally under `gc.lock`.
pub fn run_gc_local(config: &Config, mode: GcMode) -> Result<crate::store::GcStats> {
    let verbose = mode == GcMode::Cli;
    let store = Store::open(config)?;
    let _gc_lock = match store.try_gc_lock()? {
        Some(lock) => lock,
        None => {
            if verbose {
                println!("Another GC is already running; skipping.");
            }
            return Ok(skipped_gc_stats());
        }
    };
    let mut combined = crate::store::GcStats::default();

    if verbose {
        print!("Backfilling content hashes...");
        std::io::Write::flush(&mut std::io::stdout()).ok();
    }
    let backfilled = store.backfill_content_hashes().unwrap_or(0);
    if verbose {
        if backfilled > 0 {
            println!(" {backfilled} entries updated.");
        } else {
            println!(" up to date.");
        }
    }

    // Rebuild cost for pre-#594 entries; same sweep, same convergence.
    if verbose {
        print!("Backfilling compile times...");
        std::io::Write::flush(&mut std::io::stdout()).ok();
    }
    let costs = store.backfill_compile_times().unwrap_or(0);
    if verbose {
        if costs > 0 {
            println!(" {costs} entries updated.");
        } else {
            println!(" up to date.");
        }
    }

    // Entry→blob rows for pre-#608 entries; same sweep, same convergence.
    if verbose {
        print!("Backfilling entry blob maps...");
        std::io::Write::flush(&mut std::io::stdout()).ok();
    }
    let mapped = store.backfill_entry_blobs().unwrap_or(0);
    if verbose {
        if mapped > 0 {
            println!(" {mapped} entries updated.");
        } else {
            println!(" up to date.");
        }
    }

    // Automatic age retention runs first so later pressure policies observe
    // the reduced physical store. `0` keeps it disabled.
    if config.gc_max_age_hours > 0 {
        if verbose {
            print!(
                "Evicting entries older than {}h...",
                config.gc_max_age_hours
            );
            std::io::Write::flush(&mut std::io::stdout()).ok();
        }
        let age_stats = store.evict_older_than(config.gc_max_age_hours)?;
        add_gc_stats(&mut combined, &age_stats);
        if verbose {
            if age_stats.entries_evicted > 0 {
                println!(
                    " dropped {} entries from the store ({}); {} became free on disk.",
                    age_stats.entries_evicted,
                    crate::report::format_bytes(age_stats.bytes_freed),
                    crate::report::format_bytes(age_stats.disk_bytes_reclaimed),
                );
            } else {
                println!(" none old enough.");
            }
        }
    }

    if verbose {
        print!("Deduplicating entries...");
        std::io::Write::flush(&mut std::io::stdout()).ok();
    }
    let dedup_stats = store.evict_duplicate_entries().unwrap_or_default();
    add_gc_stats(&mut combined, &dedup_stats);
    if verbose {
        if dedup_stats.entries_evicted > 0 {
            println!(" removed {} duplicates.", dedup_stats.entries_evicted);
        } else {
            println!(" no duplicates found.");
        }
    }

    if verbose {
        print!("Running eviction...");
        std::io::Write::flush(&mut std::io::stdout()).ok();
    }
    let evict_stats = store.evict()?;
    add_gc_stats(&mut combined, &evict_stats);
    if verbose {
        let over_limit = store_over_limit(store.physical_size().ok(), config.max_size);
        println!("{}", describe_eviction(&evict_stats, over_limit));
    }

    Ok(combined)
}

fn skipped_gc_stats() -> crate::store::GcStats {
    crate::store::GcStats {
        skipped: true,
        ..crate::store::GcStats::default()
    }
}

fn add_gc_stats(total: &mut crate::store::GcStats, part: &crate::store::GcStats) {
    total.entries_evicted = total.entries_evicted.saturating_add(part.entries_evicted);
    total.bytes_freed = total.bytes_freed.saturating_add(part.bytes_freed);
    total.blobs_removed = total.blobs_removed.saturating_add(part.blobs_removed);
    total.duration_ms = total.duration_ms.saturating_add(part.duration_ms);
    total.entries_pinned = total.entries_pinned.saturating_add(part.entries_pinned);
    total.entries_unreclaimable = total
        .entries_unreclaimable
        .saturating_add(part.entries_unreclaimable);
    total.disk_bytes_reclaimed = total
        .disk_bytes_reclaimed
        .saturating_add(part.disk_bytes_reclaimed);
    total.skipped |= part.skipped;
}

fn gc_stats_from_breakdown(report: &crate::daemon::GcBreakdown) -> crate::store::GcStats {
    let mut total = crate::store::GcStats::default();
    for part in [&report.age, &report.duplicate, &report.size] {
        total.entries_evicted = total.entries_evicted.saturating_add(part.entries_evicted);
        total.bytes_freed = total.bytes_freed.saturating_add(part.bytes_freed);
        total.entries_pinned = total.entries_pinned.saturating_add(part.entries_pinned);
        total.disk_bytes_reclaimed = total
            .disk_bytes_reclaimed
            .saturating_add(part.disk_bytes_reclaimed);
        total.entries_unreclaimable = total
            .entries_unreclaimable
            .saturating_add(part.entries_unreclaimable);
    }
    total
}

fn human_gc_output(json: bool) -> bool {
    !json
}

fn emit_gc_json(config: &Config, skipped: bool, stats: &crate::store::GcStats) -> Result<()> {
    let store = Store::open(config)?;
    let store_bytes = store.physical_size().unwrap_or(0);
    let disk = crate::machine::disk_view(&config.store_dir(), store_bytes, config.max_size);
    #[derive(serde::Serialize)]
    struct Body {
        skipped: bool,
        disk: crate::machine::DiskView,
        entries: usize,
        entries_dropped: usize,
        store_bytes_removed: u64,
        disk_bytes_reclaimed: u64,
        entries_pinned: usize,
        entries_unreclaimable: usize,
    }
    let next = crate::machine::next_after_gc(
        &disk,
        stats.entries_unreclaimable,
        stats.disk_bytes_reclaimed,
        stats.bytes_freed,
    );
    crate::machine::emit(
        "gc",
        Body {
            skipped,
            disk,
            entries: store.entry_count().unwrap_or(0),
            entries_dropped: stats.entries_evicted,
            store_bytes_removed: stats.bytes_freed,
            disk_bytes_reclaimed: stats.disk_bytes_reclaimed,
            entries_pinned: stats.entries_pinned,
            entries_unreclaimable: stats.entries_unreclaimable,
        },
        next,
    )
}

/// Run garbage collection via the daemon.
pub fn gc(
    config: &Config,
    max_age_hours: Option<u64>,
    stale_schema: bool,
    json: bool,
) -> Result<()> {
    if stale_schema {
        let store = Store::open(config)?;
        let _gc_lock = match store.try_gc_lock()? {
            Some(lock) => lock,
            None => {
                if json {
                    return emit_gc_json(config, true, &crate::store::GcStats::default());
                }
                println!("Another GC is already running; skipping.");
                return Ok(());
            }
        };
        let stats = store.evict_stale_key_schemas(crate::cache_key::CACHE_KEY_VERSION)?;
        if json {
            return emit_gc_json(config, false, &stats);
        }
        println!(
            "Stale-schema GC:{}\nCurrent key schema: {}.",
            describe_eviction(&stats, false),
            crate::cache_key::CACHE_KEY_VERSION,
        );
        let total_size = store.total_size()?;
        let entry_count = store.entry_count()?;
        println!("Store: {} ({} entries)", ByteSize(total_size), entry_count);
        return Ok(());
    }

    let mode = GcMode::from_env();
    if mode == GcMode::Background {
        let sleep_secs = std::env::var("KACHE_AUTO_GC_RETRY_DELAY_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(121);

        // Run background sweep-sleep-sweep loop
        run_gc_local(config, GcMode::Background).ok();
        std::thread::sleep(std::time::Duration::from_secs(sleep_secs));
        run_gc_local(config, GcMode::Background).ok();
        return Ok(());
    }

    let mut combined = crate::store::GcStats::default();
    match crate::daemon::send_gc_request(config, max_age_hours) {
        Ok(outcome) if outcome.skipped => {
            if json {
                return emit_gc_json(config, true, &combined);
            }
            println!("Another GC is already running; skipping.");
        }
        Ok(outcome) => {
            if let Some(report) = outcome.breakdown.as_ref() {
                combined = gc_stats_from_breakdown(report);
                if human_gc_output(json) {
                    if let Some(hours) = max_age_hours {
                        println!("Age GC ({hours}h):");
                    } else {
                        println!(
                            "GC complete: age {}, duplicates {}, size {}.",
                            report.age.entries_evicted,
                            report.duplicate.entries_evicted,
                            report.size.entries_evicted,
                        );
                    }
                    let store = Store::open(config)?;
                    let over_limit = store_over_limit(store.physical_size().ok(), config.max_size);
                    println!("{}", describe_eviction(&combined, over_limit));
                }
            } else if human_gc_output(json) {
                if let Some(hours) = max_age_hours {
                    println!(
                        "Evicted {} entries older than {hours}h.",
                        outcome.evicted.unwrap_or(0)
                    );
                } else {
                    println!(
                        "Evicted {} entries (daemon returned no per-policy breakdown).",
                        outcome.evicted.unwrap_or(0)
                    );
                }
                combined.entries_evicted = outcome.evicted.unwrap_or(0);
            }
        }
        Err(e) => {
            if human_gc_output(json) {
                println!("Daemon GC failed ({e}), running locally...");
            }
            if let Some(hours) = max_age_hours {
                let store = Store::open(config)?;
                let _gc_lock = match store.try_gc_lock()? {
                    Some(lock) => lock,
                    None => {
                        if json {
                            return emit_gc_json(config, true, &combined);
                        }
                        println!("Another GC is already running; skipping.");
                        return Ok(());
                    }
                };
                if human_gc_output(json) {
                    print!("Running eviction...");
                    std::io::Write::flush(&mut std::io::stdout()).ok();
                }
                let evict_stats = store.evict_older_than(hours)?;
                combined = evict_stats.clone();
                if human_gc_output(json) {
                    let over_limit = store_over_limit(store.physical_size().ok(), config.max_size);
                    println!("{}", describe_eviction(&evict_stats, over_limit));
                }
            } else {
                combined = run_gc_local(
                    config,
                    if json {
                        GcMode::Background
                    } else {
                        GcMode::Cli
                    },
                )?;
            }
        }
    }

    if json {
        return emit_gc_json(config, false, &combined);
    }

    let store = Store::open(config)?;
    let total_size = store.total_size()?;
    let entry_count = store.entry_count()?;
    println!(
        "Store: {} / {} ({} entries)",
        ByteSize(total_size),
        crate::config::describe_max_size(
            config.max_size,
            crate::cache_fs::probe(&config.cache_dir).total_bytes,
        ),
        entry_count
    );

    Ok(())
}

/// Wipe the entire cache or entries for a specific crate.
pub fn purge(config: &Config, crate_filter: Option<&str>) -> Result<()> {
    let store = Store::open(config)?;

    // Purge is a bulk mutation like a GC sweep, so it holds the same
    // cross-process lock every GC driver takes: a sweep either finishes
    // before the purge starts or sees the store only after the purge is
    // done, never a half-wiped one. Per-blob safety against concurrent
    // builds still comes from the delete-row-first transactional gates.
    let _gc_lock = store.acquire_gc_lock().context("locking GC for purge")?;

    if let Some(name) = crate_filter {
        let entries = store.list_entries("name")?;
        let mut removed = 0;
        let mut skipped = 0;
        for entry in &entries {
            if entry.crate_name == name {
                // A corrupt entry (unloadable meta.json) refuses removal to
                // avoid leaking blob refcounts (#276); report it and keep going.
                if let Err(e) = store.remove_entry(&entry.cache_key) {
                    eprintln!("  skipped {}: {e:#}", entry.cache_key);
                    skipped += 1;
                    continue;
                }
                removed += 1;
            }
        }
        println!("Removed {removed} entries for '{name}'.");
        if skipped > 0 {
            println!(
                "Skipped {skipped} corrupt entr{} (see warnings above).",
                if skipped == 1 { "y" } else { "ies" }
            );
        }
    } else {
        store.clear()?;
        println!("Cleared entire local store.");
    }

    Ok(())
}

/// Outcome of one key press in the interactive `clean` selector.
#[derive(Debug, PartialEq, Eq)]
enum CleanStep {
    /// Stay in the loop (cursor/selection may have changed).
    Continue,
    /// Quit without deleting.
    Cancel,
    /// Delete the currently-selected targets.
    Confirm,
}

/// Apply one key press to the `clean` selector state. Pure (mutates the passed
/// `selected`/`cursor`), so the navigation/selection logic is unit-testable
/// without a terminal.
fn clean_handle_key(
    code: crossterm::event::KeyCode,
    selected: &mut [bool],
    cursor: &mut usize,
    len: usize,
) -> CleanStep {
    use crossterm::event::KeyCode;
    match code {
        KeyCode::Char('q') | KeyCode::Esc => return CleanStep::Cancel,
        KeyCode::Up => *cursor = cursor.saturating_sub(1),
        KeyCode::Down if *cursor + 1 < len => *cursor += 1,
        KeyCode::Char(' ') if *cursor < selected.len() => {
            selected[*cursor] = !selected[*cursor];
            if *cursor + 1 < len {
                *cursor += 1;
            }
        }
        KeyCode::Char('a') => {
            for s in selected.iter_mut() {
                *s = true;
            }
        }
        KeyCode::Char('n') => {
            for s in selected.iter_mut() {
                *s = false;
            }
        }
        KeyCode::Enter => return CleanStep::Confirm,
        _ => {}
    }
    CleanStep::Continue
}

/// Ignore key-repeat/release notifications: one physical key press must apply
/// exactly one selector action on terminals that report all key event kinds.
fn clean_handle_event(
    event: crossterm::event::Event,
    selected: &mut [bool],
    cursor: &mut usize,
    len: usize,
) -> CleanStep {
    use crossterm::event::{Event, KeyEventKind};
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            clean_handle_key(key.code, selected, cursor, len)
        }
        _ => CleanStep::Continue,
    }
}

/// Render one frame of the interactive `clean` selector. Extracted from the
/// event loop so it can be unit-tested against a ratatui `TestBackend` with a
/// fixed `targets`/`selected`/`cursor` state (the real loop owns the terminal).
fn draw_clean(
    frame: &mut ratatui::Frame,
    targets: &[TargetEntry],
    selected: &[bool],
    cursor: usize,
    root: &std::path::Path,
) {
    use ratatui::prelude::*;
    use ratatui::widgets::*;

    // Keep the raw per-row/header totals, but label the selection's scan-time
    // physical-reclaim estimate explicitly. It is not the cached-file total:
    // partial reflinks and hardlink groups need extent/link-aware accounting.
    let selected_size: u64 = targets
        .iter()
        .zip(selected.iter())
        .filter(|(_, s)| **s)
        .map(|(t, _)| t.estimated_reclaimable_bytes)
        .sum();
    let selected_count = selected.iter().filter(|s| **s).count();
    let total_size: u64 = targets.iter().map(|t| t.size).sum();
    let total_cached: u64 = targets.iter().map(|t| t.cached_bytes).sum();

    let area = frame.area();

    let chunks = Layout::vertical([
        Constraint::Length(3), // Header
        Constraint::Min(5),    // Table
        Constraint::Length(4), // Detail panel
        Constraint::Length(3), // Help
    ])
    .split(area);

    // Header
    let header = Paragraph::new(format!(
        " {} dirs ({} total, {} cached)    Selected: {} (est. {})",
        targets.len(),
        ByteSize(total_size),
        ByteSize(total_cached),
        selected_count,
        ByteSize(selected_size),
    ))
    .block(Block::bordered().title(" kache clean "));
    frame.render_widget(header, chunks[0]);

    // List
    let rows: Vec<Row> = targets
        .iter()
        .zip(selected.iter())
        .enumerate()
        .map(|(i, (t, sel))| {
            let rel = t.path.strip_prefix(root).unwrap_or(&t.path);
            let checkbox = if *sel { "[x]" } else { "[ ]" };
            let profile_str = if t.profiles.is_empty() {
                String::new()
            } else {
                format!("[{}]", t.profiles.join(", "))
            };
            let style = if i == cursor {
                Style::default().add_modifier(Modifier::REVERSED)
            } else if *sel {
                Style::default().fg(Color::Red)
            } else {
                Style::default()
            };
            Row::new(vec![
                Cell::from(format!(" {checkbox}")),
                Cell::from(format!("{}", rel.display())),
                Cell::from(format!("{:>10}", ByteSize(t.size))),
                Cell::from(format!("{:>10}", ByteSize(t.cached_bytes))),
                Cell::from(profile_str),
            ])
            .style(style)
        })
        .collect();

    let widths = [
        Constraint::Length(5),
        Constraint::Min(20),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(16),
    ];

    let table =
        Table::new(rows, widths).block(Block::bordered().title(" Select directories to remove "));
    frame.render_widget(table, chunks[1]);

    // Detail panel — breakdown for cursor row
    let current = &targets[cursor];
    let b = &current.breakdown;
    let rel = current.path.strip_prefix(root).unwrap_or(&current.path);
    let cached_pct = if current.size > 0 {
        (current.cached_bytes as f64 / current.size as f64) * 100.0
    } else {
        0.0
    };
    let detail_title = format!(
        " {} — {} total, {} cached ({:.0}%) ",
        rel.display(),
        ByteSize(current.size),
        ByteSize(current.cached_bytes),
        cached_pct,
    );
    let detail_lines = vec![
        Line::from(vec![
            Span::styled("  incremental: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:>10}", ByteSize(b.incremental))),
            Span::raw("   "),
            Span::styled("build: ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:>10}", ByteSize(b.build_scripts))),
            Span::raw("   "),
            Span::styled("deps (local): ", Style::default().fg(Color::Yellow)),
            Span::raw(format!("{:>10}", ByteSize(b.deps_local))),
        ]),
        Line::from(vec![
            Span::styled("  fingerprint: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:>10}", ByteSize(b.fingerprints))),
            Span::raw("   "),
            Span::styled("binaries: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:>7}", ByteSize(b.binaries))),
            Span::raw("   "),
            Span::styled("other: ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!("{:>17}", ByteSize(b.other))),
        ]),
    ];
    let detail = Paragraph::new(detail_lines).block(Block::bordered().title(detail_title));
    frame.render_widget(detail, chunks[2]);

    // Help bar
    let help = Paragraph::new(
        " space: toggle  a: select all  n: select none  enter: delete selected  q: cancel",
    )
    .style(Style::default().fg(Color::DarkGray))
    .block(Block::bordered());
    frame.render_widget(help, chunks[3]);
}

#[derive(Debug, Clone, serde::Serialize)]
struct CleanSkipped {
    path: String,
    reason: String,
}

const DEFAULT_TRACKED_STALE_HOURS: u64 = 336;

fn path_was_removed(path: &std::path::Path) -> bool {
    !path.exists()
}

/// Find and remove target directories, either below cwd or from the bounded
/// machine-local registry populated by the compiler wrapper.
pub fn clean(
    config: &Config,
    dry_run: bool,
    yes: bool,
    json: bool,
    tracked: bool,
    stale_hours: Option<u64>,
) -> Result<()> {
    use crossterm::event;
    use ratatui::prelude::*;
    use std::io::stdout;

    let root = std::env::current_dir()?;
    let (mut targets, skipped) = if tracked {
        tracked_target_entries(config, stale_hours.unwrap_or(DEFAULT_TRACKED_STALE_HOURS))?
    } else {
        let mut targets = Vec::new();
        find_target_dirs(&root, &mut targets);
        (targets, Vec::new())
    };

    if targets.is_empty() {
        if json {
            #[derive(serde::Serialize)]
            struct Body {
                targets: [(); 0],
                skipped: Vec<CleanSkipped>,
                removed_paths: Vec<String>,
                changed: bool,
                estimated_reclaimed_bytes: u64,
            }
            return crate::machine::emit(
                "clean",
                Body {
                    targets: [],
                    skipped,
                    removed_paths: Vec::new(),
                    changed: false,
                    estimated_reclaimed_bytes: 0,
                },
                Vec::new(),
            );
        }
        for item in &skipped {
            println!("Skipped {}: {}", item.path, item.reason);
        }
        if tracked {
            println!("No stale tracked target directories found.");
        } else {
            println!("No target/ directories found.");
        }
        return Ok(());
    }

    // Sort by size descending
    targets.sort_by_key(|entry| std::cmp::Reverse(entry.size));

    let emit_clean_json = |targets: &[TargetEntry], removed_paths: Vec<String>, reclaimed: u64| {
        #[derive(serde::Serialize)]
        struct TargetBody {
            path: String,
            apparent_bytes: u64,
            cached_bytes: u64,
            estimated_reclaimable_bytes: u64,
        }
        #[derive(serde::Serialize)]
        struct Body {
            targets: Vec<TargetBody>,
            skipped: Vec<CleanSkipped>,
            removed_paths: Vec<String>,
            changed: bool,
            estimated_reclaimed_bytes: u64,
        }
        let body = Body {
            targets: targets
                .iter()
                .map(|t| TargetBody {
                    path: t.path.display().to_string(),
                    apparent_bytes: t.size,
                    cached_bytes: t.cached_bytes,
                    estimated_reclaimable_bytes: t.estimated_reclaimable_bytes,
                })
                .collect(),
            skipped: skipped.clone(),
            changed: !removed_paths.is_empty(),
            removed_paths,
            estimated_reclaimed_bytes: reclaimed,
        };
        crate::machine::emit("clean", body, Vec::new())
    };

    // `--json` without `--yes` is a dry-run. Agents should not enter the TUI.
    if json && !yes {
        return emit_clean_json(&targets, Vec::new(), 0);
    }

    // `--dry-run` takes precedence over `--yes`: preview only, never delete.
    if dry_run {
        for line in render_clean_dry_run(&targets, &root) {
            println!("{line}");
        }
        for item in &skipped {
            println!("Skipped {}: {}", item.path, item.reason);
        }
        return Ok(());
    }

    // `--yes`: non-interactive, remove every discovered target/ dir. Meant for
    // scripts and cron where the interactive selector cannot run.
    if yes {
        let to_remove: Vec<_> = targets.iter().map(RemovalTarget::from_entry).collect();
        let (removed, estimated_reclaimed, apparent_gap) = remove_targets(&to_remove, &root, json);
        let removed_paths: Vec<String> = to_remove
            .iter()
            .filter(|target| path_was_removed(&target.path))
            .map(|target| target.path.display().to_string())
            .collect();
        if tracked {
            let store = Store::open(config)?;
            for target in &to_remove {
                if path_was_removed(&target.path) {
                    store.forget_target_root(&target.path)?;
                }
            }
        }
        if json {
            return emit_clean_json(&targets, removed_paths, estimated_reclaimed);
        }
        println!(
            "\nRemoved {removed} target/ dirs; estimated reclaimed {}{}",
            ByteSize(estimated_reclaimed),
            estimate_context_note(apparent_gap)
        );
        return Ok(());
    }

    crate::machine::require_tty(
        std::io::stdout().is_terminal(),
        "clean",
        "`kache clean --dry-run` or `kache clean --json`",
    )?;

    // TUI mode — interactive selection
    let mut selected: Vec<bool> = vec![false; targets.len()];
    let mut cursor: usize = 0;

    // Scoped so the guard restores the terminal before the post-TUI
    // summary prints to the real screen.
    let result = {
        let _terminal_mode = crate::tui::TerminalModeGuard::enter()?;
        let backend = CrosstermBackend::new(stdout());
        let mut terminal = Terminal::new(backend)?;

        loop {
            terminal.draw(|frame| draw_clean(frame, &targets, &selected, cursor, &root))?;

            if event::poll(std::time::Duration::from_millis(100))? {
                match clean_handle_event(event::read()?, &mut selected, &mut cursor, targets.len())
                {
                    CleanStep::Cancel => break None,
                    CleanStep::Confirm => {
                        let to_remove: Vec<_> = targets
                            .iter()
                            .zip(selected.iter())
                            .filter(|(_, s)| **s)
                            .map(|(t, _)| RemovalTarget::from_entry(t))
                            .collect();
                        break Some(to_remove);
                    }
                    CleanStep::Continue => {}
                }
            }
        }
    };

    // Process deletions outside TUI
    match result {
        None => {
            println!("Cancelled.");
        }
        Some(to_remove) if to_remove.is_empty() => {
            println!("Nothing selected.");
        }
        Some(to_remove) => {
            let (removed, estimated_reclaimed, apparent_gap) =
                remove_targets(&to_remove, &root, false);
            if tracked {
                let store = Store::open(config)?;
                for target in &to_remove {
                    if path_was_removed(&target.path) {
                        store.forget_target_root(&target.path)?;
                    }
                }
            }
            println!(
                "\nRemoved {removed} target/ dirs; estimated reclaimed {}{}",
                ByteSize(estimated_reclaimed),
                estimate_context_note(apparent_gap)
            );
        }
    }

    Ok(())
}

#[derive(Debug)]
struct RemovalTarget {
    path: std::path::PathBuf,
    scanned_identity: Option<FileIdentity>,
    estimated_reclaimable: u64,
    apparent_gap: u64,
}

impl RemovalTarget {
    fn from_entry(entry: &TargetEntry) -> Self {
        Self {
            path: entry.path.clone(),
            scanned_identity: entry.scan_identity,
            estimated_reclaimable: entry.estimated_reclaimable_bytes,
            apparent_gap: entry.size.saturating_sub(entry.estimated_reclaimable_bytes),
        }
    }
}

/// Delete each validated target/ dir,
/// printing a per-directory `removed` / `failed` line (paths shown relative to
/// `root`). A failure on one directory is reported and skipped, never aborting
/// the rest. Returns the scan-time estimates only for directories whose
/// `remove_dir_all` completed successfully. The actual filesystem delta can
/// differ after a concurrent change or a partially-completed failed removal.
fn remove_targets(
    to_remove: &[RemovalTarget],
    root: &std::path::Path,
    quiet: bool,
) -> (usize, u64, u64) {
    let mut estimated_reclaimed = 0u64;
    let mut apparent_gap = 0u64;
    let mut removed = 0usize;
    for target in to_remove {
        let rel = target.path.strip_prefix(root).unwrap_or(&target.path);
        let current_identity = directory_identity(&target.path);
        if target.scanned_identity.is_none() || current_identity != target.scanned_identity {
            if human_clean_output(quiet) {
                println!(
                    "  failed  {} — directory changed since scan; refusing to remove",
                    rel.display()
                );
            }
            continue;
        }
        match std::fs::remove_dir_all(&target.path) {
            Ok(()) => {
                estimated_reclaimed =
                    estimated_reclaimed.saturating_add(target.estimated_reclaimable);
                apparent_gap = apparent_gap.saturating_add(target.apparent_gap);
                removed += 1;
                if human_clean_output(quiet) {
                    println!("  removed {}", rel.display());
                }
            }
            Err(e) => {
                if human_clean_output(quiet) {
                    println!("  failed  {} — {e}", rel.display());
                }
            }
        }
    }
    (removed, estimated_reclaimed, apparent_gap)
}

fn human_clean_output(quiet: bool) -> bool {
    !quiet
}

/// Explain the gap between apparent size and estimated physical reclaim without
/// pretending every shared extent belongs to kache or survives the selected
/// deletion set. The gap can also contain sparse holes and duplicate hardlinks.
fn estimate_context_note(apparent_gap: u64) -> String {
    if apparent_gap > 0 {
        format!(
            " ({} of apparent size is shared, sparse, or duplicate)",
            ByteSize(apparent_gap)
        )
    } else {
        String::new()
    }
}

fn render_clean_dry_run(targets: &[TargetEntry], root: &std::path::Path) -> Vec<String> {
    let total_size: u64 = targets.iter().map(|t| t.size).sum();
    let total_cached: u64 = targets.iter().map(|t| t.cached_bytes).sum();
    let mut lines = vec![format!(
        "Found {} target/ director{} ({} total, {} cached)\n",
        targets.len(),
        if targets.len() == 1 { "y" } else { "ies" },
        ByteSize(total_size),
        ByteSize(total_cached),
    )];
    let max_path = targets
        .iter()
        .map(|t| {
            let rel = t.path.strip_prefix(root).unwrap_or(&t.path);
            format!("{}", rel.display()).len()
        })
        .max()
        .unwrap_or(40);
    let w = max_path.max(10);

    for t in targets {
        let rel = t.path.strip_prefix(root).unwrap_or(&t.path);
        let profile_str = if t.profiles.is_empty() {
            String::new()
        } else {
            format!("  [{}]", t.profiles.join(", "))
        };
        lines.push(format!(
            "  {:<w$}  {:>10}  cached: {:>10}{profile_str}",
            rel.display(),
            ByteSize(t.size),
            ByteSize(t.cached_bytes)
        ));
    }
    let estimated_reclaimable: u64 = targets.iter().map(|t| t.estimated_reclaimable_bytes).sum();
    lines.push(format!(
        "\nDry run: estimated to free {}{}",
        ByteSize(estimated_reclaimable),
        estimate_context_note(total_size.saturating_sub(estimated_reclaimable))
    ));
    lines
}

#[derive(Default)]
pub(crate) struct CategoryBreakdown {
    pub incremental: u64,
    pub build_scripts: u64,
    pub fingerprints: u64,
    pub binaries: u64,
    pub deps_local: u64,
    pub other: u64,
}

pub(crate) struct TargetEntry {
    pub path: std::path::PathBuf,
    pub size: u64,
    pub cached_bytes: u64,
    pub estimated_reclaimable_bytes: u64,
    pub(crate) scan_identity: Option<FileIdentity>,
    pub profiles: Vec<String>,
    pub breakdown: CategoryBreakdown,
    /// Marked true when a rescan starts; cleared when fresh data arrives.
    pub stale: bool,
}

fn tracked_target_entries(
    config: &Config,
    stale_hours: u64,
) -> Result<(Vec<TargetEntry>, Vec<CleanSkipped>)> {
    let store = Store::open(config)?;
    let tracked = store.tracked_target_roots(stale_hours)?;
    let cwd = std::env::current_dir()?;
    let mut targets = Vec::new();
    let mut skipped = Vec::new();

    for tracked in tracked {
        let display = tracked.path.display().to_string();
        let skip = |reason: &str| CleanSkipped {
            path: display.clone(),
            reason: reason.to_string(),
        };
        if !tracked.path.exists() {
            skipped.push(skip("path no longer exists; registry entry removed"));
            store.forget_target_root(&tracked.path)?;
            continue;
        }
        if cwd.starts_with(&tracked.workspace_root) {
            skipped.push(skip("belongs to the current workspace"));
            continue;
        }
        if !crate::machine::target_root_is_safe(&tracked.path, &tracked.workspace_root) {
            skipped.push(skip("path is no longer a safe derived target directory"));
            store.forget_target_root(&tracked.path)?;
            continue;
        }
        if crate::machine::directory_identity(&tracked.path) != Some(tracked.identity) {
            skipped.push(skip("directory identity changed; registry entry removed"));
            store.forget_target_root(&tracked.path)?;
            continue;
        }

        let Some(scan_identity) = directory_identity(&tracked.path) else {
            skipped.push(skip("directory identity is unavailable"));
            continue;
        };
        let (stats, breakdown) = compute_project_stats(&tracked.path);
        if directory_identity(&tracked.path) != Some(scan_identity) {
            skipped.push(skip("directory changed while it was scanned"));
            continue;
        }
        let profiles = detect_profiles(&tracked.path);
        targets.push(TargetEntry {
            path: tracked.path,
            size: stats.total_bytes,
            cached_bytes: stats.cached_bytes,
            estimated_reclaimable_bytes: stats.estimated_reclaimable_bytes,
            scan_identity: Some(scan_identity),
            profiles,
            breakdown,
            stale: false,
        });
    }

    Ok((targets, skipped))
}

/// Returns true if `path` is under a macOS directory that would trigger a TCC
/// (Transparency, Consent, Control) permission prompt or is a system path that
/// never contains Rust projects.  The check uses full-path prefix matching so it
/// works at any recursion depth and regardless of the starting scan directory.
///
/// Called *before* `read_dir` so the prompt is never triggered.
#[cfg(target_os = "macos")]
fn is_macos_protected(path: &std::path::Path) -> bool {
    use std::sync::OnceLock;

    static PREFIXES: OnceLock<Vec<std::path::PathBuf>> = OnceLock::new();

    let prefixes = PREFIXES.get_or_init(|| {
        let mut v: Vec<std::path::PathBuf> = vec![
            "/System".into(),
            "/Library".into(),
            "/private".into(),
            "/Applications".into(),
            "/Volumes".into(),
            "/Network".into(),
        ];
        if let Some(home) = dirs::home_dir() {
            for name in [
                "Desktop",
                "Documents",
                "Downloads",
                "Library",
                "Pictures",
                "Music",
                "Movies",
                "Applications",
                "Public",
            ] {
                v.push(home.join(name));
            }
        }
        v
    });

    prefixes.iter().any(|p| path.starts_with(p))
}

#[cfg(not(target_os = "macos"))]
fn is_macos_protected(_path: &std::path::Path) -> bool {
    false
}

/// Walk directories to find Cargo.toml + target/ pairs.
pub(crate) fn find_target_dirs(dir: &std::path::Path, results: &mut Vec<TargetEntry>) {
    // Check *before* read_dir to avoid triggering macOS TCC permission prompts.
    if is_macos_protected(dir) {
        return;
    }

    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    let mut has_cargo_toml = false;
    let mut subdirs = Vec::new();

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Skip hidden dirs, node_modules, .git
        if name_str.starts_with('.') || name_str == "node_modules" {
            continue;
        }

        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if name_str == "Cargo.toml" && file_type.is_file() {
            has_cargo_toml = true;
        }

        if file_type.is_dir() {
            subdirs.push((name_str.to_string(), entry.path()));
        }
    }

    if has_cargo_toml
        && let Some(target) = subdirs.iter().find(|(n, _)| n == "target")
        && let Some(scan_identity) = directory_identity(&target.1)
    {
        let (ps, breakdown) = compute_project_stats(&target.1);
        // Do not publish estimates for a directory that was replaced while
        // it was being scanned. Deletion checks the same identity again.
        if ps.total_bytes > 0 && directory_identity(&target.1) == Some(scan_identity) {
            let profiles = detect_profiles(&target.1);
            results.push(TargetEntry {
                path: target.1.clone(),
                size: ps.total_bytes,
                cached_bytes: ps.cached_bytes,
                estimated_reclaimable_bytes: ps.estimated_reclaimable_bytes,
                scan_identity: Some(scan_identity),
                profiles,
                breakdown,
                stale: false,
            });
        }
    }

    // Recurse into subdirs (but not into target/ itself)
    for (name, path) in &subdirs {
        if name != "target" {
            find_target_dirs(path, results);
        }
    }
}

/// Detect which build profiles exist in a target/ directory.
fn detect_profiles(target_dir: &std::path::Path) -> Vec<String> {
    let known = [
        ("debug", "debug"),
        ("release", "release"),
        ("profiling", "profiling"),
        ("coverage", "coverage"),
    ];
    let mut profiles = Vec::new();
    for (dir_name, label) in &known {
        let p = target_dir.join(dir_name);
        if std::fs::symlink_metadata(&p).is_ok_and(|meta| meta.file_type().is_dir()) {
            profiles.push(label.to_string());
        }
    }
    profiles
}

fn fallback_is_sccache(config: Option<&crate::config::Config>) -> bool {
    config
        .and_then(|cfg| cfg.fallback.as_deref())
        .is_some_and(is_sccache_program)
}

fn is_sccache_program(value: &str) -> bool {
    let name = std::path::Path::new(value)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(value);
    name.eq_ignore_ascii_case("sccache") || name.eq_ignore_ascii_case("sccache.exe")
}

fn active_sccache_migration_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    !trimmed.starts_with('#') && trimmed.contains("sccache") && !trimmed.contains("KACHE_FALLBACK")
}

/// Check environment for sccache and configuration issues.
/// When `fix` is true, also run the sccache→kache migration after diagnostics.
/// The daemon is only needed when remote work happens: async uploads, remote
/// checks, or planner prefetch. When neither a remote cache nor a planner is
/// configured (including strict local-only mode, which suppresses both), the
/// daemon is optional and `kache doctor` should not flag its absence as a problem.
fn daemon_needed(remote_configured: bool, planner_configured: bool) -> bool {
    remote_configured || planner_configured
}

fn daemon_service_check(
    installed: bool,
    healthy_daemon_reachable: bool,
) -> (bool, Option<&'static str>) {
    let pass = installed || healthy_daemon_reachable;
    (pass, (!pass).then_some("kache daemon install"))
}

/// Whether a `doctor` check counts toward the "N issue(s) found" total. Checks
/// downgraded to informational (`optional`) never count, even when they fail.
fn is_doctor_issue(pass: bool, optional: bool) -> bool {
    !pass && !optional
}

/// Labels of the daemon-related checks that become informational when the
/// daemon is optional. Kept in sync with the check constructions in
/// [`doctor`]; module-level so the disposition logic below is unit-testable.
const DAEMON_CHECK_LABELS: [&str; 5] = [
    "Daemon version",
    "Daemon service",
    "Daemon processes",
    "Stale locks",
    "Service exe",
];

/// Whether a failing doctor check is informational rather than an issue:
/// daemon checks when no remote/planner needs a daemon (#443), the
/// compiler probe when there is no `cc` at all to diagnose (#626), and
/// C/C++ shims (PATH masquerade is opt-in).
fn doctor_check_is_optional(label: &str, daemon_optional: bool, probe_no_compiler: bool) -> bool {
    (daemon_optional && DAEMON_CHECK_LABELS.contains(&label))
        || (label == "Compiler probe" && probe_no_compiler)
        || label == "C/C++ shims"
}

/// The daemon footnote prints when at least one daemon check failed but was
/// downgraded to informational; other downgrades (the compiler probe) carry
/// self-explanatory detail lines and get no footnote.
fn daemon_footnote_needed(daemon_optional: bool, results: &[(&str, bool)]) -> bool {
    daemon_optional
        && results
            .iter()
            .any(|(label, pass)| !pass && DAEMON_CHECK_LABELS.contains(label))
}

/// Wording for the "Daemon version" check, as `(pass, detail, fix hint)`.
///
/// Split out of [`doctor`] because the interesting part is the mid-upgrade
/// states, and those are the ones a live run is least likely to catch. `daemon`
/// is the version and build epoch of a daemon that answered; `starting_epoch` is
/// the build epoch of one that holds the run lock but has not bound its socket
/// yet. Both absent means nothing is running.
///
/// `doctor` reports these states rather than repairing them: restarting a stale
/// daemon costs an 8s wait for the replacement to come up, which is `--fix`'s
/// job, not a diagnostic's (kunobi-ninja/kache#720).
///
/// Build epochs are executable mtimes, and `0` means "could not be read". Two
/// builds are therefore only ever *ordered* through
/// [`crate::daemon::client_epoch_is_newer`], which rejects a zero on either side
/// and rejects equal epochs. Everything it declines to order is genuinely
/// unknown, and gets said so rather than guessed: telling someone their binary is
/// stale on the strength of an unreadable mtime sends them to reinstall a kache
/// that was fine.
/// Whether leftover lock files are abandoned rather than in use.
///
/// Split out because the mistake it guards against is a boolean one: any of the
/// three conditions weakening turns "no daemon owns these" into a claim about a
/// daemon that is running, and `doctor` then tells someone to restart a healthy
/// one to clean up files it is still using. Counts rather than booleans, so the
/// call site hands over raw observations and keeps no logic of its own.
fn stale_locks_are_abandoned(
    lock_files: usize,
    daemon_live: bool,
    daemon_processes: usize,
) -> bool {
    lock_files > 0 && !daemon_live && daemon_processes == 0
}

/// Whether `doctor` should replace a daemon left over from before an upgrade.
///
/// The one-line answer to the bug this all started from: only under `--fix`.
/// Restarting costs the 8s the replacement needs to bind its socket, and a
/// diagnostic that silently spends it — then reports the state it just
/// invalidated — is what made a routine upgrade look like a broken install
/// (kunobi-ninja/kache#720).
fn should_restart_stale_daemon(fix_requested: bool, daemon_is_stale: bool) -> bool {
    fix_requested && daemon_is_stale
}

/// What `doctor --fix` prints after trying to replace a stale daemon, or `None`
/// when it came up and the check below will say so.
///
/// A restart that did not finish must not be reported as one that is still
/// finishing: `Ok(false)` means the replacement was spawned but had not bound its
/// socket in time, while `Err` means the handoff failed outright and there may be
/// no replacement at all. Claiming "still coming up in the background" for both
/// invites the reader to ignore a real failure.
fn stale_restart_note(outcome: &anyhow::Result<bool>) -> Option<String> {
    match outcome {
        Ok(true) => None,
        Ok(false) => Some(
            "\x1b[2mthe replacement did not bind its socket within the startup \
             timeout\x1b[0m"
                .into(),
        ),
        Err(error) => Some(format!("\x1b[31mrestart failed: {error:#}\x1b[0m")),
    }
}

fn daemon_version_check(
    daemon: Option<(&str, u64)>,
    starting_epoch: Option<u64>,
    my_version: &str,
    my_epoch: u64,
) -> (bool, String, Option<String>) {
    match (daemon, starting_epoch) {
        (Some((version, epoch)), _) if epoch > 0 && version == my_version && epoch == my_epoch => {
            (true, format!("v{version} (epoch {epoch})"), None)
        }
        // Left running across an upgrade: the common case, and the one worth
        // naming outright so the version pair does not read as a corrupt install.
        //
        // Reading its stats is what tells the daemon it is stale — it schedules
        // a graceful restart on any request from a newer binary — so by the time
        // this prints, the handoff is already under way. It exits cleanly, which
        // launchd's `SuccessfulExit=false` and systemd's `Restart=on-failure`
        // both decline to act on, so something has to start the replacement.
        (Some((version, epoch)), _) if crate::daemon::client_epoch_is_newer(my_epoch, epoch) => (
            false,
            format!(
                "daemon v{version} (epoch {epoch}) predates binary v{my_version} \
                 (epoch {my_epoch}) — it is shutting down now"
            ),
            Some(
                "the next build starts the replacement; `kache doctor --fix` or \
                 `kache daemon start` to do it now"
                    .into(),
            ),
        ),
        // The daemon is the newer build: an old binary is on PATH, and
        // restarting the daemon would be the wrong advice.
        (Some((version, epoch)), _) if crate::daemon::client_epoch_is_newer(epoch, my_epoch) => (
            false,
            format!(
                "daemon v{version} (epoch {epoch}) is newer than binary v{my_version} \
                 (epoch {my_epoch})"
            ),
            Some("this binary is the stale one — reinstall kache or fix PATH".into()),
        ),
        // Mismatched, but in no determinable order: an unreadable mtime on either
        // side, or one build epoch carrying two version strings. Say that much and
        // no more — the two arms above are the ones that name a culprit, and
        // naming the wrong one sends someone to reinstall a working install.
        (Some((version, epoch)), _) => (
            false,
            format!(
                "daemon v{version} (epoch {epoch}) does not match binary v{my_version} \
                 (epoch {my_epoch}), and their build order cannot be determined"
            ),
            // Deliberately not "restart the daemon": in an unordered state the
            // daemon may be the newer build, and restarting it through this
            // binary would downgrade it.
            Some("work out which kache build should be running, then restart from that one".into()),
        ),
        // Nothing answered, but a daemon is on its way up. Reporting "not
        // reachable → start the daemon" here is what made a routine upgrade look
        // like a broken install.
        //
        // Passing: the check asks whether the daemon matches this binary, and the
        // coordinator file answers yes. Not yet accepting connections is what the
        // detail says, not a fault to count against the install.
        //
        // Phrased around the epoch, not the version: coordinator state carries no
        // version string, and one mtime second can carry two of them, so the
        // matching build is all this state actually establishes.
        (None, Some(epoch)) if epoch > 0 && epoch == my_epoch => (
            true,
            format!("a daemon of this build (epoch {epoch}) is starting — not serving yet"),
            None,
        ),
        (None, Some(epoch)) => (
            false,
            format!(
                "a daemon (epoch {epoch}) is starting; this binary is v{my_version} \
                 (epoch {my_epoch})"
            ),
            Some("re-run `kache doctor` in a moment".into()),
        ),
        (None, None) => (
            false,
            "daemon not reachable".into(),
            Some("start daemon with `kache daemon start` or `kache daemon install`".into()),
        ),
    }
}

pub fn doctor(
    fix: bool,
    purge_sccache: bool,
    verify: bool,
    checksums: bool,
    repair: bool,
    json: bool,
) -> Result<()> {
    let home = dirs::home_dir().unwrap_or_default();
    let config = crate::config::Config::load().ok();
    let sccache_is_fallback = fallback_is_sccache(config.as_ref());

    // The daemon only matters when remote work is configured (cache remote or a
    // planner endpoint). When neither is set — including strict local-only mode,
    // which suppresses both — the daemon is optional (see README), so its checks
    // are shown for diagnostics but never counted as issues. See #443.
    let daemon_optional = !daemon_needed(
        config.as_ref().is_some_and(|c| c.remote.is_some()),
        crate::config::Config::load_planner_config().is_some(),
    );

    struct Check {
        label: &'static str,
        pass: bool,
        detail: String,
        fix: Option<String>,
    }

    // Live compiler probe (#626): a toolchain whose `cc -###` resolves no
    // compile line makes every probe-keyed C/C++ flag refuse to cache —
    // builds stay correct but silently lose caching, with zero signal. Run
    // here so the check below reports the live compiler, and so a host with
    // no `cc` at all downgrades to informational instead of failing doctor.
    let probe_diag = crate::probe::live_probe_diagnostic();
    let probe_no_compiler = matches!(probe_diag, crate::probe::LiveProbeDiagnostic::NoCompiler);

    let check_is_optional =
        |label: &str| doctor_check_is_optional(label, daemon_optional, probe_no_compiler);

    let mut checks: Vec<Check> = Vec::new();

    // 1. Binary on PATH
    let which_cmd = if cfg!(windows) { "where" } else { "which" };
    let (bin_pass, bin_detail) = if let Ok(output) =
        std::process::Command::new(which_cmd).arg("kache").output()
        && output.status.success()
    {
        let path = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_string();
        (true, path)
    } else {
        (false, "not found".into())
    };
    checks.push(Check {
        label: "Binary",
        pass: bin_pass,
        detail: bin_detail,
        fix: if bin_pass {
            None
        } else {
            Some(format!(
                "cargo install --path . or add {} to PATH",
                cargo_home_dir().join("bin").display()
            ))
        },
    });

    // 2. RUSTC_WRAPPER
    let (wrapper_pass, wrapper_detail, wrapper_fix) =
        match crate::wrapper_config::resolve_wrapper_setting() {
            Some(crate::wrapper_config::WrapperSetting::Environment { value })
                if value.contains("kache") =>
            {
                (true, "kache via env".into(), None)
            }
            Some(crate::wrapper_config::WrapperSetting::Environment { value })
                if value.contains("sccache") =>
            {
                (
                    false,
                    format!("sccache ({value})"),
                    Some("export RUSTC_WRAPPER=kache".into()),
                )
            }
            Some(crate::wrapper_config::WrapperSetting::Environment { value }) => (
                false,
                format!("{value} (not kache)"),
                Some("export RUSTC_WRAPPER=kache".into()),
            ),
            Some(crate::wrapper_config::WrapperSetting::CargoConfig { value, path })
                if value.contains("kache") =>
            {
                (
                    true,
                    format!("kache via {}", crate::wrapper_config::display_path(&path)),
                    None,
                )
            }
            Some(crate::wrapper_config::WrapperSetting::CargoConfig { value, path }) => (
                false,
                format!("{value} in {}", crate::wrapper_config::display_path(&path)),
                Some(format!(
                    "replace `rustc-wrapper = \"{value}\"` with `rustc-wrapper = \"kache\"` in {}",
                    path.display()
                )),
            ),
            None => (
                false,
                "not set".into(),
                Some(format!(
                    "set `build.rustc-wrapper = \"kache\"` in {} or export RUSTC_WRAPPER=kache",
                    cargo_config_target_path().display()
                )),
            ),
        };
    checks.push(Check {
        label: "RUSTC_WRAPPER",
        pass: wrapper_pass,
        detail: wrapper_detail,
        fix: wrapper_fix,
    });

    // 3. Cargo config
    let (cargo_pass, cargo_detail, cargo_fix) = match crate::wrapper_config::cargo_wrapper_setting()
    {
        Some((value, path)) if value.contains("kache") => (
            true,
            format!("kache in {}", crate::wrapper_config::display_path(&path)),
            None,
        ),
        Some((value, path)) => (
            false,
            format!("{value} in {}", crate::wrapper_config::display_path(&path)),
            Some(format!(
                "replace `rustc-wrapper = \"{value}\"` with `rustc-wrapper = \"kache\"` in {}",
                path.display()
            )),
        ),
        None => (true, "not set".to_string(), None),
    };
    checks.push(Check {
        label: "Cargo config",
        pass: cargo_pass,
        detail: cargo_detail,
        fix: cargo_fix,
    });

    // 4. Cache directory
    if let Some(ref cfg) = config {
        let exists = cfg.cache_dir.exists();
        checks.push(Check {
            label: "Cache dir",
            pass: true,
            detail: if exists {
                cfg.cache_dir.display().to_string()
            } else {
                format!(
                    "{} (will be created on first build)",
                    cfg.cache_dir.display()
                )
            },
            fix: None,
        });

        // 4b. Is that directory on storage the WAL index can actually live on?
        // A shared or network mount is the #412 corruption case, and `doctor` is
        // where a user looks when they suspect their setup — so report it here
        // as a real issue rather than only as a build-time advisory (#415).
        let fs_probe = crate::cache_fs::probe(&cfg.cache_dir);
        checks.push(match crate::cache_fs::classify(&fs_probe) {
            crate::cache_fs::CacheFsVerdict::NotLocal { name } => Check {
                label: "Cache FS",
                pass: false,
                detail: format!("{name} — not host-local storage"),
                fix: Some(
                    "the cache index is a WAL SQLite database and can be corrupted on a \
                     shared or network mount: set `cache.local_store`/`KACHE_CACHE_DIR` to a \
                     local, single-machine path. To share artifacts between machines, \
                     configure a remote cache instead of a shared cache directory"
                        .to_string(),
                ),
            },
            // Local, or unrecognised. Informational either way — worth printing
            // because it is the first thing to ask about in a corruption report.
            verdict => Check {
                label: "Cache FS",
                pass: true,
                detail: match (&fs_probe.name, &verdict) {
                    (Some(name), crate::cache_fs::CacheFsVerdict::Local) => {
                        format!("{name} (local)")
                    }
                    (Some(name), _) => format!("{name} (locality unknown)"),
                    (None, _) => "could not determine filesystem".to_string(),
                },
                fix: None,
            },
        });

        match Store::open(cfg) {
            Ok(_) => checks.push(Check {
                label: "Store DB",
                pass: true,
                detail: cfg.index_db_path().display().to_string(),
                fix: None,
            }),
            Err(e) => checks.push(Check {
                label: "Store DB",
                pass: false,
                detail: format!("{} ({e})", cfg.index_db_path().display()),
                fix: Some(format!(
                    "ensure {} is writable; if builds run in a sandboxed or ephemeral env, move `cache.local_store`/`KACHE_CACHE_DIR` to a stable local directory",
                    cfg.cache_dir.display()
                )),
            }),
        }
    }

    // 5. Remote cache
    if let Some(ref cfg) = config
        && let Some(ref remote) = cfg.remote
    {
        checks.push(Check {
            label: "Remote",
            pass: true,
            detail: remote.describe(),
            fix: None,
        });
        let writes = if let Some(forced) = crate::policy::forced_remote_readonly() {
            format!("read-only — {}", forced.reason)
        } else if cfg.remote_readonly {
            "read-only (KACHE_REMOTE_READONLY or cache.remote_readonly)".to_string()
        } else {
            "read-write".to_string()
        };
        checks.push(Check {
            label: "Remote writes",
            pass: true,
            detail: writes,
            fix: None,
        });
    } else if let Some(ref cfg) = config
        && cfg.local_only
    {
        // Strict local-only mode (#221): make the hermetic state explicit so a
        // suppressed remote/planner doesn't read as a misconfiguration.
        checks.push(Check {
            label: "Remote",
            pass: true,
            detail: "local-only mode — remote + planner ignored (KACHE_LOCAL_ONLY)".to_string(),
            fix: None,
        });
    }

    // 6. Shell rc sccache remnants
    let mut rc_issues = Vec::new();
    for rc in [".zshrc", ".bashrc", ".bash_profile", ".profile"] {
        let rc_path = home.join(rc);
        if let Ok(content) = std::fs::read_to_string(&rc_path)
            && content.contains("sccache")
        {
            let has_active = content.lines().any(active_sccache_migration_line);
            if has_active {
                rc_issues.push(format!("~/{rc}"));
            }
        }
    }
    if !rc_issues.is_empty() {
        checks.push(Check {
            label: "Shell config",
            pass: false,
            detail: format!("sccache references in {}", rc_issues.join(", ")),
            fix: Some("run `kache doctor --fix` to clean up".into()),
        });
    }

    // 7. sccache daemon running
    if let Ok(output) = std::process::Command::new("pgrep")
        .args(["-x", "sccache"])
        .output()
        && output.status.success()
    {
        if sccache_is_fallback {
            checks.push(Check {
                label: "sccache",
                pass: true,
                detail: "daemon is running as fallback wrapper".into(),
                fix: None,
            });
        } else {
            checks.push(Check {
                label: "sccache",
                pass: false,
                detail: "daemon is running".into(),
                fix: Some("sccache --stop-server".into()),
            });
        }
    }

    // 8. Daemon version match
    //
    // `send_stats_request_without_restart` skips the client-side stale-daemon
    // restart that `send_stats_request` performs, so an upgrade left half-applied
    // is described in the report instead of stalling it for the 8s the
    // replacement takes to bind its socket (kunobi-ninja/kache#720). `--fix` opts
    // into that wait below.
    let my_version = crate::VERSION;
    let mut healthy_daemon_reachable = false;
    if let Some(ref cfg) = config {
        let my_epoch = crate::daemon::build_epoch();
        let mut stats = crate::daemon::send_stats_request_without_restart(cfg, false).ok();

        let is_stale = |stats: &Option<crate::daemon::StatsResponse>| {
            stats
                .as_ref()
                .is_some_and(|s| crate::daemon::client_epoch_is_newer(my_epoch, s.build_epoch))
        };

        if should_restart_stale_daemon(fix, is_stale(&stats)) {
            // Attributed rather than silent: this is where doctor's runtime goes
            // when it is slow, so say what it is waiting for.
            println!("  Restarting daemon left over from before the upgrade...");
            let outcome = crate::daemon::restart_daemon_for_stale_client(cfg);
            if let Some(note) = stale_restart_note(&outcome) {
                println!("  {note}");
            }
            // Re-read either way: the outgoing daemon is gone now, so the report
            // should describe what is there, not what answered a moment ago.
            stats = crate::daemon::send_stats_request_without_restart(cfg, false).ok();
        }

        healthy_daemon_reachable = stats.is_some();

        let (pass, detail, fix_hint) = daemon_version_check(
            stats.as_ref().map(|s| (s.version.as_str(), s.build_epoch)),
            crate::daemon::starting_daemon_epoch(cfg),
            my_version,
            my_epoch,
        );
        checks.push(Check {
            label: "Daemon version",
            pass,
            detail,
            fix: fix_hint,
        });
    }

    // 9. Daemon service installed
    if let Some(service_path) = crate::service::service_file_path() {
        let installed = service_path.exists();
        let (pass, fix) = daemon_service_check(installed, healthy_daemon_reachable);
        checks.push(Check {
            label: "Daemon service",
            pass,
            detail: if installed {
                service_path.display().to_string()
            } else if healthy_daemon_reachable {
                "not installed; healthy on-demand daemon is reachable".into()
            } else {
                "not installed".into()
            },
            fix: fix.map(str::to_string),
        });
    }

    // 10. Lingering live kache daemon processes — if the socket isn't reachable
    //     but `kache daemon run` processes exist, something got stuck.
    //     `kache daemon restart` now force-recovers this automatically.
    //
    //     Liveness here is a bare socket connect, not a stats request. Asking the
    //     daemon anything is what makes an outgoing one shut down and what could
    //     spend the 8s restart wait a second time; connecting asks nothing, costs
    //     nothing, and — unlike reusing check 8's answer — is true *now*, so a
    //     daemon that died in between is not covered for (kunobi-ninja/kache#720).
    if let Some(ref cfg) = config {
        let reachable = crate::daemon::daemon_is_live(cfg);
        let pids = crate::daemon::find_daemon_pids();
        if !reachable && !pids.is_empty() {
            let pids_str = pids
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            checks.push(Check {
                label: "Daemon processes",
                pass: false,
                detail: format!(
                    "{} live daemon process(es) (pid {pids_str}), socket unreachable",
                    pids.len()
                ),
                fix: Some(
                    "kache daemon restart  (auto-kills lingering processes + cleans stale files)"
                        .into(),
                ),
            });
        } else if pids.len() > 1 {
            let pids_str = pids
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            checks.push(Check {
                label: "Daemon processes",
                pass: false,
                detail: format!(
                    "{} daemon processes running (pid {pids_str}), expected 1",
                    pids.len()
                ),
                fix: Some(
                    "kache daemon restart  (keeps one daemon and removes stale processes)".into(),
                ),
            });
        }
    }

    // 11. Stale lock files — when no daemon is running, leftover lock files
    //     are legacy cruft from an unclean shutdown. Harmless but worth
    //     surfacing so users know `daemon restart` will tidy them up.
    //
    //     "In use" includes a daemon that holds the run lock but has not bound
    //     its socket yet, which a socket probe alone reads as absent — that is
    //     how a mid-upgrade handoff came to be reported as leftover cruft
    //    .
    if let Some(ref cfg) = config {
        let sock = cfg.socket_path();
        let mut stale_files = Vec::new();
        for ext in ["lock", "run.lock"] {
            let p = sock.with_extension(ext);
            if p.exists() {
                stale_files.push(p);
            }
        }
        if stale_locks_are_abandoned(
            stale_files.len(),
            crate::daemon::daemon_is_live(cfg),
            crate::daemon::find_daemon_pids().len(),
        ) {
            if fix {
                for f in &stale_files {
                    let _ = std::fs::remove_file(f);
                }
                checks.push(Check {
                    label: "Stale locks",
                    pass: true,
                    detail: format!("removed {} legacy lock file(s)", stale_files.len()),
                    fix: None,
                });
            } else {
                let fix_hint = if cfg!(windows) {
                    "kache doctor --fix  (removes stale lock files)"
                } else {
                    "kache daemon restart  (removes stale files and starts fresh)"
                };
                checks.push(Check {
                    label: "Stale locks",
                    pass: false,
                    detail: format!(
                        "{} legacy lock file(s) from a previous daemon",
                        stale_files.len()
                    ),
                    fix: Some(fix_hint.into()),
                });
            }
        }
    }

    // 12. Service plist exe mismatch (macOS/Linux) — if the registered
    //     service points to a binary that no longer exists or differs from
    //     the current `kache`, the daemon will relaunch the wrong binary.
    if let Some(service_path) = crate::service::service_file_path()
        && service_path.exists()
        && let Some(mismatch) = crate::service::service_exe_mismatch(&service_path)
    {
        checks.push(Check {
            label: "Service exe",
            pass: false,
            detail: format!(
                "plist points to {} but current exe is {}",
                mismatch.installed.display(),
                mismatch.current.display()
            ),
            fix: Some("kache daemon install  (re-registers against current binary)".into()),
        });
    }

    // Informational: rust-only setups skip the farm, so a miss here is not
    // an issue. Failures tell Make/PKGBUILD users why gcc is not kache.
    let shim_status = crate::compiler::shim::live_shim_path_status();
    checks.push(Check {
        label: "C/C++ shims",
        pass: shim_status.on_path,
        detail: shim_status.detail,
        fix: shim_status.fix,
    });

    // Compiler probe (#626): reported from the live toolchain, bypassing the
    // probe cache, so a stale stored "unresolved" record can't mask a fixed
    // toolchain — or a fresh breakage.
    checks.push(match probe_diag {
        crate::probe::LiveProbeDiagnostic::Resolved { version_line } => Check {
            label: "Compiler probe",
            pass: true,
            detail: format!("cc -### resolves ({version_line})"),
            fix: None,
        },
        crate::probe::LiveProbeDiagnostic::NoCompiler => Check {
            label: "Compiler probe",
            pass: false,
            detail: "no `cc` on PATH; configured or cross compilers were not checked".into(),
            fix: None,
        },
        crate::probe::LiveProbeDiagnostic::ProbeError { detail } => Check {
            label: "Compiler probe",
            pass: false,
            detail,
            fix: Some("fix the compiler diagnostic failure and rerun `kache doctor`".into()),
        },
        crate::probe::LiveProbeDiagnostic::Unresolved {
            version_line,
            stderr_head,
        } => Check {
            label: "Compiler probe",
            pass: false,
            detail: format!("`cc -###` resolved no compile line ({version_line})"),
            fix: Some(format!(
                "probe-keyed C/C++ flags will refuse to cache on this toolchain; \
                 report the `-###` output below\n{}",
                if stderr_head.is_empty() {
                    "(no -### stderr)"
                } else {
                    &stderr_head
                }
            )),
        },
    });

    // Print
    let version = crate::VERSION;
    let rustc_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let issues = checks
        .iter()
        .filter(|c| is_doctor_issue(c.pass, check_is_optional(c.label)))
        .count();

    if json {
        #[derive(serde::Serialize)]
        struct CheckBody {
            label: &'static str,
            pass: bool,
            optional: bool,
            detail: String,
            fix: Option<String>,
        }
        #[derive(serde::Serialize)]
        struct Body {
            version: &'static str,
            rustc: String,
            issues: usize,
            checks: Vec<CheckBody>,
        }
        let next = if doctor_has_issues(issues) {
            vec![crate::machine::NextAction {
                argv: vec!["kache".into(), "doctor".into(), "--fix".into()],
                why: "one or more required checks failed".into(),
            }]
        } else {
            Vec::new()
        };
        crate::machine::emit(
            "doctor",
            Body {
                version,
                rustc: rustc_version,
                issues,
                checks: checks
                    .iter()
                    .map(|c| CheckBody {
                        label: c.label,
                        pass: c.pass,
                        optional: check_is_optional(c.label),
                        detail: c.detail.clone(),
                        fix: c.fix.clone(),
                    })
                    .collect(),
            },
            next,
        )?;
        if fix {
            migrate(purge_sccache)?;
        }
        if verify && let Some(ref cfg) = config {
            let outcome = self::verify(cfg, checksums, repair)?;
            if doctor_has_issues(outcome.unresolved_integrity_findings()) {
                std::process::exit(1);
            }
        }
        return Ok(());
    }

    println!();
    println!("  kache v{version}    {rustc_version}");
    println!();

    let label_width = checks.iter().map(|c| c.label.len()).max().unwrap_or(0);

    let check_results: Vec<(&str, bool)> = checks.iter().map(|c| (c.label, c.pass)).collect();
    let downgraded_daemon = daemon_footnote_needed(daemon_optional, &check_results);
    for check in &checks {
        let optional = check_is_optional(check.label);
        // A failing optional check is informational, not a problem: render it
        // with a neutral dimmed marker rather than the red ✗.
        let icon = if check.pass {
            "\x1b[32m✓\x1b[0m"
        } else if optional {
            "\x1b[2m•\x1b[0m"
        } else {
            "\x1b[31m✗\x1b[0m"
        };
        println!(
            "  {icon} {:<width$}  {}",
            check.label,
            check.detail,
            width = label_width,
        );
        if let Some(ref fix) = check.fix {
            println!(
                "    {:<width$}  \x1b[33m→ {fix}\x1b[0m",
                "",
                width = label_width,
            );
        }
    }

    println!();
    if issues == 0 {
        println!("  \x1b[32mAll checks passed.\x1b[0m");
    } else {
        println!("  \x1b[31m{issues} issue(s) found.\x1b[0m");
    }
    if downgraded_daemon {
        println!(
            "  \x1b[2mDaemon checks are informational: no remote cache or planner \
             configured (the daemon is optional for local-only use).\x1b[0m"
        );
    }
    println!();

    if fix {
        println!("Running migration...\n");
        migrate(purge_sccache)?;
    }

    // Cache integrity verification. Unresolved integrity findings make the
    // process exit non-zero so a scheduled `kache doctor --verify` can gate
    // a CI job (kunobi-ninja/kache#176). Orphan blobs are deliberately NOT
    // part of that condition: they are reclaimable space, not wrong bytes,
    // and GC clears them without anyone's intervention.
    if verify {
        if let Some(ref cfg) = config {
            println!();
            let outcome = self::verify(cfg, checksums, repair)?;
            let unresolved = outcome.unresolved_integrity_findings();
            if unresolved > 0 {
                anyhow::bail!(
                    "cache integrity check failed: {unresolved} corrupted \
                     {} remain{} (missing blobs: {}, checksum failures: {}){}",
                    if unresolved == 1 { "entry" } else { "entries" },
                    if unresolved == 1 { "s" } else { "" },
                    outcome.missing_blobs,
                    outcome.checksum_failures,
                    if repair {
                        " — `--repair` could not remove them"
                    } else {
                        " — rerun with `--repair` to remove them"
                    },
                );
            }
        } else {
            println!("  Cannot verify: no valid config found");
        }
    }

    Ok(())
}

fn doctor_has_issues(issues: usize) -> bool {
    issues > 0
}

/// Migrate from sccache to kache (called by `doctor --fix`).
fn migrate(purge_sccache: bool) -> Result<()> {
    let home = dirs::home_dir().unwrap_or_default();
    let mut actions: Vec<String> = Vec::new();

    // 1. Stop sccache daemon if running
    if let Ok(output) = std::process::Command::new("pgrep")
        .args(["-x", "sccache"])
        .output()
        && output.status.success()
    {
        println!("Stopping sccache daemon...");
        let _ = std::process::Command::new("sccache")
            .arg("--stop-server")
            .status();
        actions.push("Stopped sccache daemon".into());
    }

    // 2. Replace sccache in $CARGO_HOME/config.toml (fallback to ~/.cargo)
    let cargo_dir = cargo_home_dir();
    for name in ["config.toml", "config"] {
        let cargo_config = cargo_dir.join(name);
        if let Ok(content) = std::fs::read_to_string(&cargo_config)
            && content.contains("sccache")
        {
            let new_content = content.replace("sccache", "kache");
            std::fs::write(&cargo_config, new_content)?;
            actions.push(format!(
                "Replaced sccache with kache in {}",
                cargo_config.display()
            ));
        }
    }

    // 3. Show what to change in shell rc
    let mut rc_changes: Vec<(String, Vec<(usize, String)>)> = Vec::new();
    for rc in [".zshrc", ".bashrc", ".bash_profile", ".profile"] {
        let rc_path = home.join(rc);
        if let Ok(content) = std::fs::read_to_string(&rc_path) {
            let sccache_lines: Vec<_> = content
                .lines()
                .enumerate()
                .filter(|(_, l)| l.contains("sccache") && !l.trim_start().starts_with('#'))
                .map(|(n, l)| (n + 1, l.to_string()))
                .collect();
            if !sccache_lines.is_empty() {
                rc_changes.push((rc.to_string(), sccache_lines));
            }
        }
    }

    // 4. Purge sccache cache and binary if requested
    if purge_sccache {
        // Remove sccache local cache
        let sccache_cache_dirs = [
            home.join("Library/Caches/Mozilla.sccache"), // macOS
            home.join(".cache/sccache"),                 // Linux
        ];
        for cache_dir in &sccache_cache_dirs {
            if cache_dir.exists() {
                let size = dir_size(cache_dir);
                std::fs::remove_dir_all(cache_dir)?;
                actions.push(format!(
                    "Removed sccache cache {} ({})",
                    cache_dir.display(),
                    ByteSize(size)
                ));
            }
        }

        // Uninstall sccache binary if cargo-installed
        if let Ok(output) =
            std::process::Command::new(if cfg!(windows) { "where" } else { "which" })
                .arg("sccache")
                .output()
            && output.status.success()
        {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let sccache_path = std::path::PathBuf::from(&path);
            let cargo_bin = cargo_dir.join("bin");
            let resolved_sccache = sccache_path.canonicalize().unwrap_or(sccache_path);
            let resolved_cargo_bin = cargo_bin.canonicalize().unwrap_or(cargo_bin);

            if resolved_sccache.starts_with(resolved_cargo_bin) {
                println!("Uninstalling sccache via cargo...");
                let status = std::process::Command::new("cargo")
                    .args(["uninstall", "sccache"])
                    .status();
                if status.map(|s| s.success()).unwrap_or(false) {
                    actions.push("Uninstalled sccache (cargo uninstall)".into());
                }
            } else {
                actions.push(format!(
                    "sccache at {path} not cargo-installed — remove manually if desired"
                ));
            }
        }
    }

    // Print summary
    println!("\nMigration summary:");
    if actions.is_empty() && rc_changes.is_empty() {
        println!("  No sccache configuration found. Nothing to migrate.");
        println!("\n  If RUSTC_WRAPPER isn't set yet, add to ~/.zshrc:");
        println!("    export RUSTC_WRAPPER=kache");
        return Ok(());
    }

    for action in &actions {
        println!("  ✓ {action}");
    }

    if !rc_changes.is_empty() {
        println!("\n  Manual changes needed in shell rc files:");
        for (rc, lines) in &rc_changes {
            println!("\n  ~/{rc}:");
            for (line_num, line) in lines {
                let trimmed = line.trim();
                if trimmed.starts_with("export RUSTC_WRAPPER") {
                    // RUSTC_WRAPPER line → replace with kache
                    println!("    line {line_num}:");
                    println!("      - {line}");
                    println!("      + export RUSTC_WRAPPER=kache");
                } else if trimmed.starts_with("export SCCACHE_") {
                    // SCCACHE_* env vars → remove (not relevant to kache)
                    println!("    line {line_num}: (remove)");
                    println!("      - {line}");
                } else {
                    // Other sccache references → flag for manual review
                    println!("    line {line_num}: (review)");
                    println!("      {line}");
                }
            }
        }
        println!("\n  After editing, run: source ~/.zshrc");
    }

    if !purge_sccache {
        println!(
            "\n  Tip: run `kache doctor --fix --purge-sccache` to also remove sccache cache and binary"
        );
    }

    println!("\n  Then verify with: kache doctor");
    Ok(())
}

/// Synchronize the local cache with its remote: pull missing artifacts, push new ones.
///
/// Works directly against the remote (no daemon required). Safe to run alongside the daemon —
/// downloads use atomic extraction, imports use INSERT OR REPLACE, and uploads are idempotent.
pub fn sync(
    config: &Config,
    manifest_path: Option<&str>,
    pull_only: bool,
    push_only: bool,
    dry_run: bool,
    pull_all: bool,
    pull_workspace: bool,
    allow_partial: bool,
) -> Result<()> {
    let remote = config.require_remote()?;

    let store = Store::open(config)?;
    let workspace_crates = workspace_filter(manifest_path);

    // For the default filtered pull: parse Cargo.lock for every dependency crate
    // name. Skipped under --workspace, which scopes the pull to workspace members
    // only (the deps are expected to be provided some other way).
    let lock_crates = if !pull_all && !pull_workspace && !push_only {
        parse_cargo_lock_crate_names()
    } else {
        None
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    rt.block_on(sync_inner(
        config,
        &store,
        remote,
        workspace_crates.as_ref(),
        pull_only,
        push_only,
        dry_run,
        pull_all,
        lock_crates.as_ref(),
        pull_workspace,
        allow_partial,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn sync_inner(
    config: &Config,
    store: &Store,
    remote: &crate::config::RemoteConfig,
    workspace_crates: Option<&std::collections::HashSet<String>>,
    pull_only: bool,
    push_only: bool,
    dry_run: bool,
    pull_all: bool,
    lock_crates: Option<&std::collections::HashSet<String>>,
    pull_workspace: bool,
    allow_partial: bool,
) -> Result<()> {
    // Validate `--workspace` BEFORE connecting: `create_backend` resolves
    // credentials, which can launch a `credential_process` or block on an SSO
    // prompt. Failing after that for a reason knowable up front wastes the user's
    // time and can leave an interactive prompt hanging.
    if pull_workspace && workspace_crates.is_none_or(|crates| crates.is_empty()) {
        anyhow::bail!(
            "--workspace: no workspace members resolved (cargo metadata failed or this is not              a Cargo workspace); refusing to fall back to a full remote scan"
        );
    }

    let backend = crate::remote_backend::create_backend(remote, config.s3_pool_idle_secs)
        .await
        .context("connecting to the remote — check its configuration and access")?;
    sync_with_client(
        backend.as_ref(),
        config,
        store,
        remote,
        workspace_crates,
        pull_only,
        push_only,
        dry_run,
        pull_all,
        lock_crates,
        pull_workspace,
        allow_partial,
    )
    .await
}

/// The remote-driven body of `sync`, with the backend injected so tests can
/// drive it against a mock. Lists remote keys, diffs against the local store,
/// then (unless `dry_run`) pulls missing artifacts and pushes local-only ones.
#[allow(clippy::too_many_arguments)]
async fn sync_with_client(
    backend: &dyn crate::remote_backend::RemoteBackend,
    config: &Config,
    store: &Store,
    remote: &crate::config::RemoteConfig,
    workspace_crates: Option<&std::collections::HashSet<String>>,
    pull_only: bool,
    push_only: bool,
    dry_run: bool,
    pull_all: bool,
    lock_crates: Option<&std::collections::HashSet<String>>,
    pull_workspace: bool,
    allow_partial: bool,
) -> Result<()> {
    let planner = crate::remote_plan::RemotePlanner::new(config);

    // For pull: scope the remote key listing to crate prefixes when possible (one
    // LIST per crate). `--workspace` narrows that to workspace members only;
    // otherwise it's the Cargo.lock dep set; `--all` (or no filter) lists the
    // whole bucket.
    let s3_keys = if !push_only {
        if pull_workspace {
            // `--workspace` must resolve to a non-empty workspace set. If cargo
            // metadata failed or this isn't a Cargo workspace, refuse to fall
            // back to a full-remote scan — that's the exact opposite of what the
            // flag asks for (and `lock_crates` is None here, so the dep path
            // can't catch it either).
            let crates = workspace_crates.filter(|c| !c.is_empty()).ok_or_else(|| {
                anyhow::anyhow!(
                    "--workspace: no workspace members resolved (cargo metadata                      failed or this is not a Cargo workspace); refusing to fall                      back to a full remote scan"
                )
            })?;
            eprint!(
                "Listing remote keys for {} workspace crates...",
                crates.len()
            );
            let keys = planner
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(backend, remote)
                .list_keys_for_crates(crates)
                .await
                .context("listing remote keys for workspace crates")?;
            eprintln!(" {} keys", keys.len());
            keys
        } else if !pull_all
            && let Some(crates) = lock_crates
            && !crates.is_empty()
        {
            eprint!("Listing remote keys for {} crates...", crates.len());
            let keys = planner
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(backend, remote)
                .list_keys_for_crates(crates)
                .await
                .context("listing remote keys for dependency crates")?;
            eprintln!(" {} keys", keys.len());
            keys
        } else {
            eprint!("Listing remote keys...");
            let keys = planner
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(backend, remote)
                .list_keys()
                .await
                .context("listing remote keys")?;
            eprintln!(" {} keys", keys.len());
            keys
        }
    } else {
        // Push-only mode still lists remote keys to find what's already uploaded.
        eprint!("Listing remote keys...");
        let keys = planner
            .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
            .layout(backend, remote)
            .list_keys()
            .await
            .context("listing remote keys")?;
        eprintln!(" {} keys", keys.len());
        keys
    };

    let local_entries = store.list_entries("name")?;

    // to_pull: remote keys not present on disk locally — (cache_key, crate_name).
    let to_pull: Vec<(String, String)> = if !push_only {
        s3_keys
            .iter()
            .filter(|(k, _)| {
                let entry_dir = config.store_dir().join(k.as_str());
                !entry_dir.exists()
            })
            .map(|(k, cn)| (k.clone(), cn.clone()))
            .collect()
    } else {
        Vec::new()
    };

    // to_push: local entries on disk but not in the remote, filtered by workspace.
    // Includes (cache_key, crate_name) for crate-prefixed uploads.
    let to_push: Vec<(String, String)> = if !pull_only && !config.remote_readonly {
        local_entries
            .iter()
            .filter(|e| {
                if let Some(ws) = workspace_crates {
                    ws.contains(&e.crate_name)
                } else {
                    true
                }
            })
            .filter(|e| {
                let entry_dir = config.store_dir().join(&e.cache_key);
                entry_dir.exists() && !s3_keys.contains_key(&e.cache_key)
            })
            .map(|e| (e.cache_key.clone(), e.crate_name.clone()))
            .collect()
    } else {
        Vec::new()
    };

    if to_pull.is_empty() && to_push.is_empty() {
        println!("Nothing to sync.");
        return Ok(());
    }

    println!(
        "Plan: pull {} artifact{}, push {} artifact{}",
        to_pull.len(),
        if to_pull.len() == 1 { "" } else { "s" },
        to_push.len(),
        if to_push.len() == 1 { "" } else { "s" },
    );

    if dry_run {
        for (key, crate_name) in &to_pull {
            println!("  pull  {}... ({})", &key[..16.min(key.len())], crate_name);
        }
        for (key, crate_name) in &to_push {
            println!("  push  {}... ({})", &key[..16.min(key.len())], crate_name);
        }
        return Ok(());
    }

    let max_concurrent = (config.s3_concurrency as usize).max(1);
    let mut total_failed = 0;

    // ── Pull phase ──────────────────────────────────────────────
    if !to_pull.is_empty() {
        let total = to_pull.len();
        let ok = std::sync::atomic::AtomicUsize::new(0);
        let fail = std::sync::atomic::AtomicUsize::new(0);
        let mut in_flight = futures::stream::FuturesUnordered::new();

        for (key, crate_name) in to_pull {
            // Bounded concurrency: wait for a slot
            while in_flight.len() >= max_concurrent {
                use futures::StreamExt;
                in_flight.next().await;
                eprint!(
                    "\r  Downloading: {}/{}",
                    ok.load(std::sync::atomic::Ordering::Relaxed)
                        + fail.load(std::sync::atomic::Ordering::Relaxed),
                    total,
                );
            }

            let remote_cfg = remote.clone();
            let cfg = config.clone();
            let download_plan = planner.plan(crate::remote_plan::RemoteWorkload::SyncPull);
            let ok_ref = &ok;
            let fail_ref = &fail;

            // We do NOT tokio::spawn — FuturesUnordered polls futures cooperatively
            // on the current thread. This avoids Send requirements for Store.
            in_flight.push(async move {
                // Re-check: daemon (or a parallel sync) may have downloaded it
                let entry_dir = cfg.store_dir().join(&key);
                if entry_dir.exists() {
                    ok_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }

                let blobs_dir = cfg.store_dir().join("blobs");
                let result = download_plan
                    .layout(backend, &remote_cfg)
                    .download_entry(&key, &crate_name, &entry_dir, &blobs_dir)
                    .await;
                match result {
                    Ok(_bytes) => {
                        // Import into index — opens a fresh Store (cheap with WAL).
                        // INSERT OR REPLACE is idempotent if daemon also imported.
                        let mut imported = false;
                        match Store::open(&cfg) {
                            Ok(s) => match s.import_restored_entry(&key) {
                                Ok(_) => {
                                    imported = true;
                                }
                                Err(e) => {
                                    eprintln!(
                                        "\n  error: import {}...: {e}",
                                        &key[..16.min(key.len())]
                                    );
                                }
                            },
                            Err(e) => {
                                eprintln!(
                                    "\n  error: open store for import {}...: {e}",
                                    &key[..16.min(key.len())]
                                );
                            }
                        }
                        if imported {
                            ok_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            fail_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        eprintln!("\n  error: pull {}...: {e}", &key[..16.min(key.len())]);
                        fail_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });
        }

        // Drain remaining
        use futures::StreamExt;
        while in_flight.next().await.is_some() {
            eprint!(
                "\r  Downloading: {}/{}",
                ok.load(std::sync::atomic::Ordering::Relaxed)
                    + fail.load(std::sync::atomic::Ordering::Relaxed),
                total,
            );
        }
        let ok_count = ok.load(std::sync::atomic::Ordering::Relaxed);
        let fail_count = fail.load(std::sync::atomic::Ordering::Relaxed);
        total_failed += fail_count;
        eprintln!(
            "\r  Downloaded:  {ok_count}/{total}{}",
            if fail_count > 0 {
                format!(" ({fail_count} failed)")
            } else {
                String::new()
            },
        );
    }

    // ── Push phase ──────────────────────────────────────────────
    if !to_push.is_empty() {
        let total = to_push.len();
        let ok = std::sync::atomic::AtomicUsize::new(0);
        let fail = std::sync::atomic::AtomicUsize::new(0);
        let mut in_flight = futures::stream::FuturesUnordered::new();

        for (key, crate_name) in to_push {
            while in_flight.len() >= max_concurrent {
                use futures::StreamExt;
                in_flight.next().await;
                eprint!(
                    "\r  Uploading: {}/{}",
                    ok.load(std::sync::atomic::Ordering::Relaxed)
                        + fail.load(std::sync::atomic::Ordering::Relaxed),
                    total,
                );
            }

            let remote_cfg = remote.clone();
            let cfg = config.clone();
            let upload_plan = planner.plan(crate::remote_plan::RemoteWorkload::SyncPush);
            let ok_ref = &ok;
            let fail_ref = &fail;

            in_flight.push(async move {
                let entry_dir = cfg.store_dir().join(&key);
                if !entry_dir.exists() {
                    // Entry disappeared (GC or purge) — record failure
                    eprintln!(
                        "\n  error: push {}...: local entry disappeared before upload",
                        &key[..16.min(key.len())]
                    );
                    fail_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }

                let blobs_dir = cfg.store_dir().join("blobs");
                match upload_plan
                    .layout(backend, &remote_cfg)
                    .upload_entry(
                        &key,
                        &crate_name,
                        &entry_dir,
                        &blobs_dir,
                        cfg.compression_level,
                    )
                    .await
                {
                    Ok(_bytes) => {
                        ok_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("\n  error: push {}...: {e}", &key[..16.min(key.len())]);
                        fail_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            });
        }

        use futures::StreamExt;
        while in_flight.next().await.is_some() {
            eprint!(
                "\r  Uploading: {}/{}",
                ok.load(std::sync::atomic::Ordering::Relaxed)
                    + fail.load(std::sync::atomic::Ordering::Relaxed),
                total,
            );
        }
        let ok_count = ok.load(std::sync::atomic::Ordering::Relaxed);
        let fail_count = fail.load(std::sync::atomic::Ordering::Relaxed);
        total_failed += fail_count;
        eprintln!(
            "\r  Uploaded:  {ok_count}/{total}{}",
            if fail_count > 0 {
                format!(" ({fail_count} failed)")
            } else {
                String::new()
            },
        );
    }

    if total_failed > 0 && !allow_partial {
        anyhow::bail!("{total_failed} transfer(s) or import(s) failed during sync");
    }

    Ok(())
}

/// Save a build manifest recording which cache keys were used with their cost data.
///
/// Reads events.jsonl to collect cache keys, compile times, and artifact sizes,
/// then uploads to `{prefix}/_manifests/{manifest_key}.json`.
///
/// When `namespace` is provided and Cargo.lock exists, also computes and uploads
/// content-addressed shards to `{prefix}/_manifests/v3/{namespace}/shards/{hash}.json`.
pub fn save_manifest(
    config: &Config,
    manifest_key: Option<&str>,
    namespace: Option<&str>,
) -> Result<()> {
    save_manifest_impl(config, manifest_key, namespace, None, true, true)
}

pub(crate) fn save_manifest_auto_for_session(
    config: &Config,
    manifest_key: &str,
    session_id: &str,
) -> Result<()> {
    // The daemon does not own the calling workspace's Cargo.lock or namespace.
    // Publish the exact session manifest only; explicit save-manifest calls own
    // shard publication because they run from the workspace.
    save_manifest_impl(
        config,
        Some(manifest_key),
        None,
        Some(session_id),
        false,
        false,
    )
}

/// Shards are content-addressed under the first published key only. Later
/// keys (legacy host triple, extra aliases) get the JSON manifest without
/// duplicating shard objects.
fn shard_namespace_for_publish_key(index: usize, namespace: Option<&str>) -> Option<&str> {
    if index == 0 { namespace } else { None }
}

fn save_manifest_impl(
    config: &Config,
    manifest_key: Option<&str>,
    namespace: Option<&str>,
    session_id: Option<&str>,
    announce: bool,
    allow_env_namespace: bool,
) -> Result<()> {
    if config.remote_readonly {
        tracing::debug!("skipping manifest save (read-only mode)");
        return Ok(());
    }

    let remote = config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No remote configured"))?;

    let events = crate::events::read_events(&config.event_log_path())?;
    let entries = manifest_entries_from_events(&events, session_id);

    if entries.is_empty() {
        if announce {
            eprintln!("No build events found, skipping manifest save");
        }
        return Ok(());
    }

    let keys = match manifest_key {
        Some(key) => vec![key.to_string()],
        None => {
            crate::identity::manifest_publish_keys(std::path::Path::new("Cargo.lock"), None, None)
        }
    };
    let env_namespace = allow_env_namespace
        .then(|| std::env::var("KACHE_NAMESPACE").ok())
        .flatten()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let effective_namespace = namespace
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(String::from)
        .or(env_namespace);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    let pool_idle_secs = config.s3_pool_idle_secs;
    let entry_count = entries.len();
    let published = keys.clone();
    rt.block_on(async {
        let backend = crate::remote_backend::create_backend(remote, pool_idle_secs).await?;
        for (index, key) in keys.iter().enumerate() {
            let shard_namespace =
                shard_namespace_for_publish_key(index, effective_namespace.as_deref());
            upload_manifest_and_shards(
                &backend,
                remote,
                key,
                shard_namespace,
                std::path::Path::new("Cargo.lock"),
                entries.clone(),
            )
            .await?;
        }
        Ok::<(), anyhow::Error>(())
    })?;

    if announce {
        eprintln!(
            "Saved manifest: {entry_count} entries for '{}'",
            published.join("', '")
        );
    }
    Ok(())
}

/// Collapse build events into deduplicated manifest entries.
///
/// Only cacheable outcomes (hits/dup/miss with a non-empty key) contribute, and
/// when a crate appears under one cache_key multiple times the entry with the
/// largest compile time wins (cargo may invoke rustc repeatedly with differing
/// flags). Pure — extracted so the dedup logic is unit-testable without S3.
fn manifest_entries_from_events(
    events: &[crate::events::BuildEvent],
    session_id: Option<&str>,
) -> Vec<crate::remote::ManifestEntry> {
    let mut by_key = std::collections::HashMap::<String, crate::remote::ManifestEntry>::new();
    let mut order = Vec::new();
    for e in events {
        if session_id.is_some_and(|session_id| e.session_id != session_id) {
            continue;
        }
        if e.cache_key.is_empty() {
            continue;
        }
        match e.result {
            crate::events::EventResult::LocalHit
            | crate::events::EventResult::PrefetchHit
            | crate::events::EventResult::RemoteHit
            | crate::events::EventResult::Dup
            | crate::events::EventResult::Miss => {}
            _ => continue,
        }
        let entry = crate::remote::ManifestEntry {
            cache_key: e.cache_key.clone(),
            crate_name: e.crate_name.clone(),
            compile_time_ms: if e.compile_time_ms > 0 {
                e.compile_time_ms
            } else {
                e.elapsed_ms
            },
            artifact_size: e.size,
        };
        if let Some(existing) = by_key.get_mut(&e.cache_key) {
            if entry.compile_time_ms > existing.compile_time_ms {
                *existing = entry;
            }
        } else {
            order.push(e.cache_key.clone());
            by_key.insert(e.cache_key.clone(), entry);
        }
    }
    order
        .into_iter()
        .filter_map(|key| by_key.remove(&key))
        .collect()
}

/// Upload the monolithic build manifest and, when a namespace is given and a
/// `Cargo.lock` exists at `lock_path`, the content-addressed shard indexes.
///
/// Takes the backend by reference so tests can drive it against a mock (the
/// production caller injects a real one from `create_backend`).
async fn upload_manifest_and_shards(
    backend: &Arc<dyn crate::remote_backend::RemoteBackend>,
    remote: &crate::config::RemoteConfig,
    key: &str,
    namespace: Option<&str>,
    lock_path: &std::path::Path,
    entries: Vec<crate::remote::ManifestEntry>,
) -> Result<()> {
    let manifest = crate::remote::BuildManifest {
        version: 3,
        created: chrono::Utc::now().to_rfc3339(),
        manifest_key: key.to_string(),
        entries: entries.clone(),
    };

    // Always upload the monolithic build manifest.
    crate::remote::upload_manifest(backend.as_ref(), &remote.prefix, key, &manifest).await?;

    // Upload sharded build-manifest indexes if a namespace is provided and Cargo.lock exists.
    if let Some(ns) = namespace {
        if lock_path.exists() {
            let shard_count =
                upload_shards(backend, &remote.prefix, ns, lock_path, &entries).await?;
            eprintln!("Uploaded {shard_count} shards for namespace '{ns}'");
        } else {
            eprintln!("No Cargo.lock found, skipping shard upload");
        }
    } else {
        eprintln!("No namespace provided, skipping shard upload");
    }

    Ok(())
}

/// Compute and upload content-addressed shards from Cargo.lock deps + build events.
///
/// Returns the number of shards uploaded.
async fn upload_shards(
    backend: &Arc<dyn crate::remote_backend::RemoteBackend>,
    prefix: &str,
    namespace: &str,
    lock_path: &std::path::Path,
    entries: &[crate::remote::ManifestEntry],
) -> Result<usize> {
    let deps = crate::shards::parse_cargo_lock(lock_path)?;
    let shard_set = crate::shards::compute_shards(namespace, &deps);

    // crate_name -> its manifest entry (keep the first match per crate). The
    // whole entry, not just the cache key: shards now persist compile cost and
    // artifact size so the planner can rank by them (kunobi-ninja/kache#617).
    let mut crate_to_entry =
        std::collections::HashMap::<&str, &crate::remote::ManifestEntry>::new();
    for e in entries {
        crate_to_entry.entry(&e.crate_name).or_insert(e);
    }

    // Build Shard objects, skipping crates that have no build event
    let mut uploads = Vec::new();
    for (shard_hash, shard_deps) in &shard_set.shards {
        let shard_entries: Vec<crate::remote::ShardEntry> = shard_deps
            .iter()
            .filter_map(|(name, _version)| {
                crate_to_entry
                    .get(name.as_str())
                    .map(|&entry| crate::remote::ShardEntry {
                        cache_key: entry.cache_key.clone(),
                        crate_name: name.clone(),
                        compile_time_ms: Some(entry.compile_time_ms),
                        artifact_size: Some(entry.artifact_size),
                    })
            })
            .collect();

        if shard_entries.is_empty() {
            continue;
        }

        let shard = crate::remote::Shard {
            version: 3,
            entries: shard_entries,
        };
        uploads.push((shard_hash.clone(), shard));
    }

    // Upload shards in parallel (up to 16 concurrent)
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(16));
    let mut handles = Vec::new();
    for (hash, shard) in uploads {
        let backend = Arc::clone(backend);
        let prefix = prefix.to_string();
        let namespace = namespace.to_string();
        let permit = sem.clone().acquire_owned().await?;
        handles.push(tokio::spawn(async move {
            let result =
                crate::remote::upload_shard(backend.as_ref(), &prefix, &namespace, &hash, &shard)
                    .await;
            drop(permit);
            result
        }));
    }

    let mut uploaded = 0;
    for handle in handles {
        handle.await.context("shard upload task panicked")??;
        uploaded += 1;
    }

    Ok(uploaded)
}

/// Build a workspace crate name filter from Cargo.toml metadata.
/// Returns None if no manifest is found (= no filtering, include everything).
fn workspace_filter(manifest_path: Option<&str>) -> Option<std::collections::HashSet<String>> {
    manifest_path
        .map(|mp| match get_workspace_crate_names(mp) {
            Ok(names) => names.into_iter().collect(),
            Err(e) => {
                eprintln!("Warning: cargo metadata failed for {mp}: {e}");
                std::collections::HashSet::new()
            }
        })
        .or_else(|| {
            if std::path::Path::new("Cargo.toml").exists() {
                match get_workspace_crate_names("Cargo.toml") {
                    Ok(names) => Some(names.into_iter().collect()),
                    Err(e) => {
                        eprintln!("Warning: cargo metadata failed: {e}");
                        None
                    }
                }
            } else {
                None
            }
        })
}

/// Parse `cargo metadata` to get workspace package names.
fn get_workspace_crate_names(manifest_path: &str) -> Result<Vec<String>> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .arg("--manifest-path")
        .arg(manifest_path)
        .output()
        .context("running cargo metadata")?;

    if !output.status.success() {
        anyhow::bail!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("parsing cargo metadata")?;

    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array);

    let names: Vec<String> = match packages {
        Some(pkgs) => pkgs
            .iter()
            .filter_map(|p| {
                p.get("name")
                    .and_then(serde_json::Value::as_str)
                    .map(String::from)
            })
            .collect(),
        None => Vec::new(),
    };

    Ok(names)
}

/// Parse Cargo.lock to extract all crate names (direct + transitive dependencies).
/// Returns None if no Cargo.lock is found in the current directory.
fn parse_cargo_lock_crate_names() -> Option<std::collections::HashSet<String>> {
    parse_cargo_lock_crate_names_from(std::path::Path::new("Cargo.lock"))
}

fn parse_cargo_lock_crate_names_from(
    lock_path: &std::path::Path,
) -> Option<std::collections::HashSet<String>> {
    if !lock_path.exists() {
        return None;
    }
    let content = std::fs::read_to_string(lock_path).ok()?;
    let lock: toml::Value = toml::from_str(&content).ok()?;
    let packages = lock.get("package")?.as_array()?;
    let names: std::collections::HashSet<String> = packages
        .iter()
        .filter_map(|p| p.get("name")?.as_str().map(String::from))
        .collect();
    Some(names)
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut size = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                size += dir_size(&p);
            } else if let Ok(meta) = p.metadata() {
                size += meta.len();
            }
        }
    }
    size
}

/// Verify cache integrity: check all entries and blobs for consistency.
/// Outcome of a store integrity verification pass (kunobi-ninja/kache#176).
///
/// Separates **integrity** findings — a store that can serve broken or lost
/// data — from **reclaimable** ones (orphan blobs are wasted space, never
/// wrong bytes), because only the former should fail a CI run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VerifyOutcome {
    pub total_entries: usize,
    pub valid_entries: usize,
    /// Entries whose metadata or blobs did not check out.
    pub corrupted_entries: usize,
    /// Referenced blob files absent from the store.
    pub missing_blobs: usize,
    /// Blobs whose bytes no longer match their content address.
    pub checksum_failures: usize,
    /// On-disk blobs no entry references — space, not corruption.
    pub orphaned_blobs: usize,
    /// Corrupted entries `--repair` actually removed.
    pub corrupted_removed: usize,
    /// Derived blob-index rows that disagree with committed entry metadata.
    pub index_drift: usize,
}

impl VerifyOutcome {
    /// Integrity problems still present when the pass finished — what a CI
    /// run must fail on. Repair removes corrupted entries, so anything it
    /// could not remove still counts (kunobi-ninja/kache#176).
    pub fn unresolved_integrity_findings(&self) -> usize {
        self.corrupted_entries - self.corrupted_removed + self.index_drift
    }
}

fn reconciled_index_message(drift: crate::store::BlobIndexDrift) -> Option<String> {
    (drift.total() != 0).then(|| {
        format!(
            "Repairing: reconciled {} entry mappings and {} blob rows.",
            drift.entry_mappings, drift.blobs
        )
    })
}

fn should_print_repair_tip(
    corrupted_entries: usize,
    orphaned_blobs: usize,
    index_drift: usize,
    repair: bool,
) -> bool {
    let findings = corrupted_entries
        .saturating_add(orphaned_blobs)
        .saturating_add(index_drift);
    !repair && findings != 0
}

/// Hash each unique blob **once**, streaming, across a bounded worker pool,
/// and return the hashes whose bytes no longer match their content address
/// (kunobi-ninja/kache#176).
///
/// The previous scrub read every blob with `fs::read` once per *referencing
/// entry*: a blob shared by N entries was fully buffered and hashed N times,
/// serially — the dedup that makes the store cheap made verification
/// quadratic in exactly the stores that need it most. Streaming keeps RSS
/// flat on multi-hundred-MB rlibs, and the work is embarrassingly parallel.
///
/// Unreadable blobs are reported as failures: a blob that cannot be read is
/// as unusable as one that hashes wrong.
fn scrub_blob_checksums(
    blobs: &[(String, std::path::PathBuf)],
) -> std::collections::HashSet<String> {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    if blobs.is_empty() {
        return std::collections::HashSet::new();
    }
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(8)
        .min(blobs.len());
    let cursor = AtomicUsize::new(0);
    let failures = Mutex::new(std::collections::HashSet::new());

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let idx = cursor.fetch_add(1, Ordering::Relaxed);
                    let Some((hash, path)) = blobs.get(idx) else {
                        return;
                    };
                    let computed = std::fs::File::open(path).and_then(|file| {
                        let mut hasher = blake3::Hasher::new();
                        hasher.update_reader(file)?;
                        Ok(hasher.finalize().to_hex().to_string())
                    });
                    match computed {
                        Ok(computed) if &computed == hash => {}
                        Ok(computed) => {
                            tracing::warn!(
                                "blob {} checksum mismatch (computed {})",
                                &hash[..16.min(hash.len())],
                                &computed[..16.min(computed.len())]
                            );
                            failures.lock().expect("scrub mutex").insert(hash.clone());
                        }
                        Err(e) => {
                            tracing::warn!("blob {} unreadable: {e}", &hash[..16.min(hash.len())]);
                            failures.lock().expect("scrub mutex").insert(hash.clone());
                        }
                    }
                }
            });
        }
    });

    failures.into_inner().expect("scrub mutex")
}

pub fn verify(config: &Config, checksums: bool, repair: bool) -> Result<VerifyOutcome> {
    let store = Store::open(config)?;

    // Adopt entries the index doesn't know about before verifying it (#415).
    // `verify` walks the *index*, so an index that lost its rows (quarantined
    // after corruption, or deleted) looks empty and clean while a full store of
    // artifacts sits on disk unreferenced. Rebuilding first is what makes
    // `--repair` able to fix the corruption case rather than just describe it.
    if repair {
        match store.rebuild_index_from_store() {
            Ok(stats) if stats.entries_rebuilt > 0 => println!(
                "Adopted {} unreferenced cache {} from the store ({} blob references).",
                stats.entries_rebuilt,
                if stats.entries_rebuilt == 1 {
                    "entry"
                } else {
                    "entries"
                },
                stats.blobs_registered,
            ),
            Ok(_) => {}
            Err(e) => println!("Warning: could not rebuild index rows from the store: {e:#}"),
        }
    }

    let entries = store.list_entries("name")?;
    let store_dir = config.store_dir();
    let blobs_dir = store_dir.join("blobs");

    let mut total_entries: usize = 0;
    let mut valid_entries: usize = 0;
    let mut corrupted_entries: usize = 0;
    let mut missing_blobs: usize = 0;
    let mut corrupted_keys: Vec<String> = Vec::new();
    // Unique blobs to checksum, and which entries reference each — collected
    // during the walk so the scrub hashes every blob ONCE regardless of how
    // many entries share it (kunobi-ninja/kache#176).
    let mut blobs_to_scrub: std::collections::HashMap<String, std::path::PathBuf> =
        std::collections::HashMap::new();
    let mut entries_by_blob: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    // Per-entry state, resolved after the scrub.
    let mut entry_keys: Vec<String> = Vec::new();
    let mut entry_ok_flags: Vec<bool> = Vec::new();

    // Track all blob hashes referenced by valid entries
    let mut referenced_blobs: std::collections::HashSet<String> = std::collections::HashSet::new();

    println!("Verifying {} cache entries...", entries.len());

    for (entry_index, entry) in entries.iter().enumerate() {
        total_entries += 1;
        entry_keys.push(entry.cache_key.clone());
        entry_ok_flags.push(true);

        let entry_dir = store_dir.join(&entry.cache_key);
        let meta_path = entry_dir.join("meta.json");

        // Check metadata file exists and parses
        let meta = match std::fs::read_to_string(&meta_path) {
            Ok(content) => match serde_json::from_str::<crate::store::EntryMeta>(&content) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        "entry {} has invalid meta.json: {e}",
                        &entry.cache_key[..16.min(entry.cache_key.len())]
                    );
                    corrupted_entries += 1;
                    corrupted_keys.push(entry.cache_key.clone());
                    continue;
                }
            },
            Err(e) => {
                tracing::warn!(
                    "entry {} missing meta.json: {e}",
                    &entry.cache_key[..16.min(entry.cache_key.len())]
                );
                corrupted_entries += 1;
                corrupted_keys.push(entry.cache_key.clone());
                continue;
            }
        };

        // Check all referenced blob files exist and optionally verify checksums
        let mut entry_ok = true;
        for cached_file in &meta.files {
            let blob_path = store.blob_path(&cached_file.hash);

            if !blob_path.is_file() {
                tracing::warn!(
                    "entry {} missing blob {} (file: {})",
                    &entry.cache_key[..16.min(entry.cache_key.len())],
                    &cached_file.hash[..16.min(cached_file.hash.len())],
                    cached_file.name
                );
                missing_blobs += 1;
                entry_ok = false;
                continue;
            }

            // Size check
            if let Ok(file_meta) = std::fs::metadata(&blob_path)
                && file_meta.len() != cached_file.size
            {
                tracing::warn!(
                    "entry {} blob {} size mismatch (expected {}, got {})",
                    &entry.cache_key[..16.min(entry.cache_key.len())],
                    &cached_file.hash[..16.min(cached_file.hash.len())],
                    cached_file.size,
                    file_meta.len()
                );
                entry_ok = false;
                continue;
            }

            // Checksums are deferred to one deduplicated, parallel,
            // streaming pass after the walk (kunobi-ninja/kache#176) — here
            // we only record which unique blobs to scrub and who references
            // them.
            if checksums {
                blobs_to_scrub
                    .entry(cached_file.hash.clone())
                    .or_insert_with(|| blob_path.clone());
                entries_by_blob
                    .entry(cached_file.hash.clone())
                    .or_default()
                    .push(entry_index);
            }

            referenced_blobs.insert(cached_file.hash.clone());
        }

        if !entry_ok {
            entry_ok_flags[entry_index] = false;
        }
    }

    // One deduplicated, streaming, bounded-parallel scrub of every unique
    // blob, then attribute each failure back to the entries referencing it
    // (kunobi-ninja/kache#176).
    // Scrub whatever the walk collected. Deliberately UNGUARDED: the walk
    // only collects when `--checksums` asked for it, so a second `if
    // checksums` here would be redundant — and two guards on one condition
    // make each individually unobservable, which is how a mutation gate
    // reports an "equivalent" mutant that is really a missing test.
    let work: Vec<(String, std::path::PathBuf)> = blobs_to_scrub.into_iter().collect();
    let blobs_scrubbed = work.len();
    let failed = scrub_blob_checksums(&work);
    let checksum_failures = failed.len();
    for hash in &failed {
        for idx in entries_by_blob.get(hash).into_iter().flatten() {
            entry_ok_flags[*idx] = false;
        }
    }

    for (idx, ok) in entry_ok_flags.iter().enumerate() {
        if *ok {
            valid_entries += 1;
        } else {
            corrupted_entries += 1;
            corrupted_keys.push(entry_keys[idx].clone());
        }
    }

    // Scan for orphaned blobs (on-disk blobs not referenced by any entry)
    let mut total_blobs_on_disk: usize = 0;
    let mut orphaned_blobs: usize = 0;

    if blobs_dir.exists()
        && let Ok(prefix_dirs) = std::fs::read_dir(&blobs_dir)
    {
        for prefix_entry in prefix_dirs.flatten() {
            if !prefix_entry.path().is_dir() {
                continue;
            }
            if let Ok(blob_files) = std::fs::read_dir(prefix_entry.path()) {
                for blob_entry in blob_files.flatten() {
                    let path = blob_entry.path();
                    if !path.is_file() {
                        continue;
                    }
                    total_blobs_on_disk += 1;
                    if let Some(name) = path.file_name().and_then(|n| n.to_str())
                        && !referenced_blobs.contains(name)
                    {
                        orphaned_blobs += 1;
                    }
                }
            }
        }
    }

    // Repair: remove corrupted entries. Count what actually went: an entry
    // repair could not remove is still an unresolved finding, and the exit
    // status must say so (kunobi-ninja/kache#176).
    let mut corrupted_removed: usize = 0;
    if repair && !corrupted_keys.is_empty() {
        println!(
            "Repairing: removing {} corrupted entries...",
            corrupted_keys.len()
        );
        for key in &corrupted_keys {
            match store.remove_entry(key) {
                Ok(()) => corrupted_removed += 1,
                Err(e) => tracing::warn!(
                    "failed to remove corrupted entry {}: {e}",
                    &key[..16.min(key.len())]
                ),
            }
        }
    }

    // `entries` plus each committed meta.json are authoritative; `blobs` and
    // `entry_blobs` are derived acceleration structures. Compare them under
    // the store write lock so a concurrent publisher/remover cannot create a
    // transient mismatch, and rebuild them atomically when requested (#819).
    let index_drift = if repair {
        match store.reconcile_blob_index() {
            Ok(drift) => {
                if let Some(message) = reconciled_index_message(drift) {
                    println!("{message}");
                }
                0
            }
            Err(e) => {
                tracing::warn!("blob index reconciliation failed: {e:#}");
                1
            }
        }
    } else if corrupted_entries == 0 {
        match store.blob_index_drift() {
            Ok(drift) => drift.total(),
            Err(e) => {
                tracing::warn!("blob index verification failed: {e:#}");
                1
            }
        }
    } else {
        // Corrupt metadata is already an unresolved integrity finding and
        // cannot safely serve as the source of truth for a graph comparison.
        0
    };

    // Repair: reclaim orphaned blob files (counted above). These are never
    // reclaimed by normal GC, so without this they leak invisibly to
    // size-based eviction. A small grace leaves any blob a concurrent build
    // is materializing untouched.
    if repair && orphaned_blobs > 0 {
        match store.sweep_orphan_blobs(std::time::Duration::from_secs(60)) {
            Ok(swept) => println!(
                "Repairing: reclaimed {} orphan blobs ({})",
                swept.removed,
                ByteSize(swept.bytes_reclaimed)
            ),
            Err(e) => tracing::warn!("orphan-blob sweep failed: {e}"),
        }
    }

    // Repair: reclaim put-phase staging snapshots abandoned by a crash
    // between staging and publish (review finding #3). Reported even when
    // empty so the repair narrative always accounts for every reclaim pass.
    //
    // The grace matches the daemon's GC sweep deliberately: a staging file
    // belonging to a put running in ANOTHER process is indistinguishable
    // from a crash leftover, and unlinking one fails that put at publish
    // time. An hour is long enough that only a dead process's snapshot is
    // ever old enough to reclaim.
    if repair {
        let swept_staging = store.sweep_stale_staging(STAGING_SWEEP_GRACE);
        println!(
            "Repairing: reclaimed {} stale staging files ({})",
            swept_staging.removed,
            ByteSize(swept_staging.bytes_reclaimed)
        );
    }

    // Compute store size
    let store_size = store.total_size().unwrap_or(0);

    println!();
    println!("Cache verification complete");
    println!(
        "  Entries: {} total, {} valid, {} corrupted",
        total_entries, valid_entries, corrupted_entries
    );
    println!(
        "  Blobs: {} total, {} orphaned, {} missing, {} scrubbed, {} checksum failures",
        total_blobs_on_disk, orphaned_blobs, missing_blobs, blobs_scrubbed, checksum_failures
    );
    println!("  Blob index drift: {index_drift}");
    println!("  Store size: {}", ByteSize(store_size));

    if should_print_repair_tip(corrupted_entries, orphaned_blobs, index_drift, repair) {
        println!();
        println!(
            "Tip: run `kache doctor --repair` to remove corrupted entries, reconcile the blob index, and reclaim orphaned blobs."
        );
    }

    Ok(VerifyOutcome {
        total_entries,
        valid_entries,
        corrupted_entries,
        missing_blobs,
        checksum_failures,
        orphaned_blobs,
        corrupted_removed,
        index_drift,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // ── List pager resolution ───────────────────────────────────────────────

    #[test]
    fn detail_fields_render_only_nonempty_values() {
        let mut lines = Vec::new();
        push_nonempty_detail(&mut lines, "  Type:     ", "");
        assert!(lines.is_empty());

        push_nonempty_detail(&mut lines, "  Type:     ", "lib");
        push_nonempty_detail(&mut lines, "  Features: ", "serde, std");
        assert_eq!(
            lines,
            vec![
                "  Type:     lib".to_string(),
                "  Features: serde, std".to_string(),
            ]
        );
    }

    struct FailOnWrite {
        fail_on_call: usize,
        calls: usize,
    }

    impl std::io::Write for FailOnWrite {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            if self.calls == self.fail_on_call {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "pager exited",
                ))
            } else {
                Ok(buffer.len())
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn pager_line_writer_stops_on_content_or_newline_errors() {
        let lines = vec!["first".to_string(), "second".to_string()];
        let mut output = Vec::new();
        assert!(write_pager_lines(&mut output, &lines));
        assert_eq!(output, b"first\nsecond\n");

        let mut content_error = FailOnWrite {
            fail_on_call: 1,
            calls: 0,
        };
        assert!(!write_pager_lines(&mut content_error, &lines));
        assert_eq!(content_error.calls, 1);

        let mut newline_error = FailOnWrite {
            fail_on_call: 2,
            calls: 0,
        };
        assert!(!write_pager_lines(&mut newline_error, &lines));
        assert_eq!(newline_error.calls, 2);
    }

    #[test]
    fn pager_resolution_obeys_tty_disable_and_environment_precedence() {
        assert_eq!(
            resolve_pager_argv(false, true, Some("most -R"), Some("less -S"), false),
            Some(vec!["most".to_string(), "-R".to_string()])
        );
        assert_eq!(
            resolve_pager_argv(false, true, None, Some("less -S"), false),
            Some(vec!["less".to_string(), "-S".to_string()])
        );
        assert_eq!(
            resolve_pager_argv(true, true, Some("less"), None, false),
            None
        );
        assert_eq!(
            resolve_pager_argv(false, false, Some("less"), None, false),
            None
        );

        // A present but empty KACHE_PAGER wins over PAGER and disables paging.
        assert_eq!(
            resolve_pager_argv(false, true, Some(""), Some("less"), false),
            None
        );
        assert_eq!(
            resolve_pager_argv(false, true, Some("cat"), Some("less"), false),
            None
        );
        assert_eq!(
            resolve_pager_argv(false, true, None, Some("cat"), false),
            None
        );
    }

    #[test]
    fn pager_resolution_uses_platform_defaults() {
        assert_eq!(
            resolve_pager_argv(false, true, None, None, false),
            Some(vec!["less".to_string(), "-FRX".to_string()])
        );
        assert_eq!(
            resolve_pager_argv(false, true, None, None, true),
            Some(vec!["more.com".to_string()])
        );
    }

    #[test]
    fn pager_resolution_groups_quotes_without_shell_evaluation() {
        assert_eq!(
            resolve_pager_argv(
                false,
                true,
                Some(r#""C:\Program Files\Git\usr\bin\less.exe" -FRX"#),
                None,
                true
            ),
            Some(vec![
                r"C:\Program Files\Git\usr\bin\less.exe".to_string(),
                "-FRX".to_string()
            ])
        );
        assert_eq!(
            resolve_pager_argv(
                false,
                true,
                Some("less --prompt='literal ; $HOME | text'"),
                None,
                false
            ),
            Some(vec![
                "less".to_string(),
                "--prompt=literal ; $HOME | text".to_string()
            ])
        );
        assert_eq!(
            resolve_pager_argv(false, true, Some("less 'unterminated"), None, false),
            None
        );
    }

    // ── Doctor check dispositions (kunobi-ninja/kache#443, #626) ──────────

    /// Which failing checks are informational: the full truth table for
    /// daemon-optional and probe-no-compiler downgrades.
    #[test]
    fn doctor_check_optionality_truth_table() {
        // Daemon labels downgrade exactly when the daemon is optional.
        assert!(doctor_check_is_optional("Daemon version", true, false));
        assert!(!doctor_check_is_optional("Daemon version", false, false));
        // The compiler probe downgrades exactly when there is no cc at all.
        assert!(doctor_check_is_optional("Compiler probe", false, true));
        assert!(!doctor_check_is_optional("Compiler probe", false, false));
        // probe_no_compiler must not leak onto other labels, nor
        // daemon_optional onto the probe.
        assert!(!doctor_check_is_optional("Binary", true, true));
        assert!(!doctor_check_is_optional("Daemon version", false, true));
        assert!(!doctor_check_is_optional("Compiler probe", true, false));
        // Non-daemon, non-probe labels are never optional, except C/C++
        // shims: PATH masquerade is opt-in and rust-only setups must not
        // fail doctor for skipping it.
        assert!(!doctor_check_is_optional("Remote", true, true));
        assert!(doctor_check_is_optional("C/C++ shims", false, false));
        assert!(doctor_check_is_optional("C/C++ shims", true, true));
    }

    #[test]
    fn daemon_service_is_satisfied_by_install_or_healthy_on_demand_daemon() {
        assert_eq!(daemon_service_check(true, false), (true, None));
        assert_eq!(daemon_service_check(false, true), (true, None));
        assert_eq!(
            daemon_service_check(false, false),
            (false, Some("kache daemon install"))
        );
    }

    /// The daemon footnote prints only for a FAILING daemon check under an
    /// optional daemon — never for passing daemon checks, failing non-daemon
    /// checks, or a required daemon.
    #[test]
    fn daemon_footnote_only_for_downgraded_daemon_failures() {
        assert!(daemon_footnote_needed(
            true,
            &[("Daemon service", false), ("Binary", true)]
        ));
        assert!(!daemon_footnote_needed(false, &[("Daemon service", false)]));
        assert!(!daemon_footnote_needed(
            true,
            &[("Daemon service", true), ("Binary", true)]
        ));
        assert!(!daemon_footnote_needed(
            true,
            &[("Compiler probe", false), ("Binary", false)]
        ));
        assert!(!daemon_footnote_needed(true, &[]));
    }

    // ── Daemon version reporting (kunobi-ninja/kache#720) ──────────────────

    /// The upgrade window: a daemon from before the upgrade is still answering.
    /// It must read as an upgrade left to finish, not as a version conflict, and
    /// the hint must point at the flag that actually restarts it.
    #[test]
    fn daemon_version_check_names_the_pending_upgrade() {
        let (pass, detail, fix) = daemon_version_check(Some(("0.13.0", 100)), None, "0.14.0", 200);
        assert!(!pass);
        assert!(detail.contains("predates"), "{detail}");
        assert!(
            detail.contains("0.13.0") && detail.contains("0.14.0"),
            "{detail}"
        );
        assert!(detail.contains("shutting down"), "{detail}");
        assert!(fix.unwrap().contains("doctor --fix"));
    }

    /// Epochs are executable mtimes and `0` means unreadable, so several
    /// mismatches are genuinely unordered. Guessing a culprit there sends someone
    /// to reinstall a working kache, so every one of them must decline to.
    #[test]
    fn daemon_version_check_does_not_invent_an_order_it_cannot_determine() {
        for (daemon, my_version, my_epoch, case) in [
            (("0.13.0", 0), "0.14.0", 200, "daemon epoch unreadable"),
            (("0.13.0", 200), "0.14.0", 0, "binary epoch unreadable"),
            (("0.13.0", 0), "0.14.0", 0, "neither epoch readable"),
            (
                ("0.13.0", 200),
                "0.14.0",
                200,
                "one build, two version strings",
            ),
        ] {
            let (pass, detail, fix) =
                daemon_version_check(Some(daemon), None, my_version, my_epoch);
            assert!(!pass, "{case}: {detail}");
            assert!(detail.contains("cannot be determined"), "{case}: {detail}");
            let fix = fix.unwrap();
            assert!(
                !fix.contains("this binary is the stale one"),
                "{case}: {fix}"
            );
        }

        // Equal version strings and unreadable epochs are not evidence of the
        // same build either — that pair must not pass.
        let (pass, detail, _) = daemon_version_check(Some(("0.14.0", 0)), None, "0.14.0", 0);
        assert!(!pass, "{detail}");
    }

    /// The other direction — an old binary against a newer daemon — must not
    /// advise restarting the daemon, which would downgrade it.
    #[test]
    fn daemon_version_check_blames_the_binary_when_the_daemon_is_newer() {
        let (pass, detail, fix) = daemon_version_check(Some(("0.14.0", 200)), None, "0.13.0", 100);
        assert!(!pass);
        assert!(detail.contains("newer than binary"), "{detail}");
        let fix = fix.unwrap();
        assert!(fix.contains("this binary is the stale one"), "{fix}");
        assert!(!fix.contains("daemon start"), "{fix}");
    }

    /// Matching build: the only passing state, and it stays terse.
    #[test]
    fn daemon_version_check_passes_on_identical_build() {
        let (pass, detail, fix) = daemon_version_check(Some(("0.14.0", 200)), None, "0.14.0", 200);
        assert!(pass);
        assert_eq!(detail, "v0.14.0 (epoch 200)");
        assert!(fix.is_none());
    }

    /// Same version string, different build — a locally rebuilt daemon is stale
    /// even though the version reads identical.
    #[test]
    fn daemon_version_check_catches_same_version_different_build() {
        let (pass, detail, _) = daemon_version_check(Some(("0.14.0", 100)), None, "0.14.0", 200);
        assert!(!pass, "{detail}");
        assert!(detail.contains("predates"), "{detail}");
    }

    /// The window that made a routine upgrade look like a broken install: no
    /// daemon answers yet because the replacement is still binding its socket.
    /// Reporting "not reachable → start the daemon" there is actively wrong, and
    /// so is counting a healthy transient against the install — the coordinator
    /// file says the right build is coming up, which is what this check asks.
    #[test]
    fn daemon_version_check_reports_a_daemon_that_is_still_starting() {
        let (pass, detail, fix) = daemon_version_check(None, Some(200), "0.14.0", 200);
        assert!(pass, "{detail}");
        assert!(detail.contains("starting"), "{detail}");
        assert!(fix.is_none());
        // Coordinator state has no version string, so none may be asserted here.
        assert!(!detail.contains("v0.14.0"), "{detail}");

        // A starting daemon of some other build gets named as such rather than
        // silently claimed to be this one.
        let (pass, detail, _) = daemon_version_check(None, Some(100), "0.14.0", 200);
        assert!(!pass, "{detail}");
        assert!(detail.contains("epoch 100"), "{detail}");
        assert!(detail.contains("0.14.0"), "{detail}");

        // An unreadable epoch on both sides is not a match, so it must not pass
        // through the equality arm.
        let (pass, detail, _) = daemon_version_check(None, Some(0), "0.14.0", 0);
        assert!(!pass, "{detail}");
    }

    /// Lock files are only cruft when nothing owns them. Each condition is
    /// load-bearing on its own: weaken any one and doctor tells someone to
    /// restart a healthy daemon to clean up files it is still using.
    #[test]
    fn stale_locks_are_abandoned_needs_every_condition() {
        assert!(stale_locks_are_abandoned(1, false, 0));
        assert!(stale_locks_are_abandoned(2, false, 0));

        assert!(!stale_locks_are_abandoned(0, false, 0), "no lock files");
        assert!(!stale_locks_are_abandoned(1, true, 0), "daemon serving");
        assert!(!stale_locks_are_abandoned(1, false, 1), "daemon process");
        assert!(!stale_locks_are_abandoned(0, true, 2), "none of them");
    }

    /// The behaviour this whole change exists for: a plain `doctor` run reports
    /// a stale daemon, it does not replace it and pay the startup wait. Only
    /// `--fix` opts into that.
    #[test]
    fn only_fix_restarts_a_stale_daemon() {
        assert!(should_restart_stale_daemon(true, true));

        assert!(!should_restart_stale_daemon(false, true), "plain doctor");
        assert!(!should_restart_stale_daemon(true, false), "nothing stale");
        assert!(!should_restart_stale_daemon(false, false), "neither");
    }

    /// A restart that did not finish must never read as one that is still
    /// finishing — that is what turns a failed `--fix` into a silent no-op in the
    /// reader's head.
    #[test]
    fn stale_restart_note_never_claims_a_replacement_that_may_not_exist() {
        assert!(stale_restart_note(&Ok(true)).is_none());

        let timed_out = stale_restart_note(&Ok(false)).unwrap();
        assert!(timed_out.contains("did not bind"), "{timed_out}");
        assert!(!timed_out.contains("background"), "{timed_out}");

        let failed = stale_restart_note(&Err(anyhow::anyhow!("spawn refused"))).unwrap();
        assert!(failed.contains("restart failed"), "{failed}");
        assert!(failed.contains("spawn refused"), "{failed}");
    }

    /// Nothing answering and nothing coming up keeps the original wording.
    #[test]
    fn daemon_version_check_reports_an_absent_daemon() {
        let (pass, detail, fix) = daemon_version_check(None, None, "0.14.0", 200);
        assert!(!pass);
        assert_eq!(detail, "daemon not reachable");
        assert!(fix.unwrap().contains("kache daemon start"));
    }

    /// A daemon that answered wins over the coordinator file: a leftover
    /// `Starting` record must not relabel a reachable daemon as starting.
    #[test]
    fn daemon_version_check_prefers_the_daemon_that_answered() {
        let (pass, detail, _) =
            daemon_version_check(Some(("0.14.0", 200)), Some(100), "0.14.0", 200);
        assert!(pass, "{detail}");
        assert_eq!(detail, "v0.14.0 (epoch 200)");
    }

    // ── Eviction reporting (kunobi-ninja/kache#509) ────────────────────────

    fn gc_stats(evicted: usize, pinned: usize, bytes: u64) -> crate::store::GcStats {
        crate::store::GcStats {
            entries_evicted: evicted,
            bytes_freed: bytes,
            entries_pinned: pinned,
            ..Default::default()
        }
    }

    #[test]
    fn cloned_targets_summary_only_appears_for_retained_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let mut disk = crate::machine::disk_view(dir.path(), 0, 1024);
        assert!(cloned_targets_line(&disk).is_none());

        disk.disk_private_bytes = 3;
        disk.cloned_into_targets_bytes = 7;
        let line = cloned_targets_line(&disk).expect("cloned blocks need a summary");
        assert!(line.contains("3 B"), "{line}");
        assert!(line.contains("7 B"), "{line}");
    }

    #[test]
    fn gc_machine_output_boundaries_are_explicit() {
        assert!(human_gc_output(false));
        assert!(!human_gc_output(true));

        let stats = skipped_gc_stats();
        assert!(stats.skipped);
        assert_eq!(stats.entries_evicted, 0);
        assert_eq!(DEFAULT_TRACKED_STALE_HOURS, 14 * 24);

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("target");
        std::fs::create_dir(&path).unwrap();
        assert!(!path_was_removed(&path));
        std::fs::remove_dir(&path).unwrap();
        assert!(path_was_removed(&path));
        assert!(human_clean_output(false));
        assert!(!human_clean_output(true));
        assert!(!doctor_has_issues(0));
        assert!(doctor_has_issues(1));
    }

    #[test]
    fn why_miss_json_diagnosis_has_distinct_precedence() {
        assert_eq!(
            why_miss_diagnosis("write failed", "", 0, false),
            "not_cached"
        );
        assert_eq!(
            why_miss_diagnosis("", "metadata mismatch", 0, false),
            "lookup_rejected"
        );
        assert_eq!(why_miss_diagnosis("", "", 0, false), "never_cached");
        assert_eq!(
            why_miss_diagnosis("", "", 1, true),
            "first_build_now_cached"
        );
        assert_eq!(why_miss_diagnosis("", "", 1, false), "key_mismatch");
        assert!(cache_key_matches("same", "same"));
        assert!(!cache_key_matches("stored", "missed"));
    }

    #[test]
    fn json_list_crate_filter_is_exact() {
        assert!(crate_name_matches("serde", "serde"));
        assert!(!crate_name_matches("serde", "serde_json"));
    }

    #[test]
    fn daemon_gc_breakdown_sums_every_policy_field() {
        fn policy(n: u64) -> crate::daemon::GcPolicyOutcome {
            crate::daemon::GcPolicyOutcome {
                entries_evicted: n as usize,
                bytes_freed: n * 10,
                entries_pinned: (n * 100) as usize,
                disk_bytes_reclaimed: n * 1_000,
                entries_unreclaimable: (n * 10_000) as usize,
            }
        }
        let report = crate::daemon::GcBreakdown {
            mode: crate::daemon::GcRequestMode::Automatic,
            age: policy(1),
            duplicate: policy(2),
            size: policy(3),
        };
        let total = gc_stats_from_breakdown(&report);
        assert_eq!(total.entries_evicted, 6);
        assert_eq!(total.bytes_freed, 60);
        assert_eq!(total.entries_pinned, 600);
        assert_eq!(total.disk_bytes_reclaimed, 6_000);
        assert_eq!(total.entries_unreclaimable, 60_000);

        let mut accumulated = crate::store::GcStats {
            entries_evicted: 1,
            bytes_freed: 2,
            entries_pinned: 3,
            blobs_removed: 4,
            duration_ms: 7,
            entries_unreclaimable: 5,
            disk_bytes_reclaimed: 6,
            skipped: false,
        };
        let part = crate::store::GcStats {
            entries_evicted: 10,
            bytes_freed: 20,
            entries_pinned: 30,
            blobs_removed: 40,
            duration_ms: 70,
            entries_unreclaimable: 50,
            disk_bytes_reclaimed: 60,
            skipped: true,
        };
        add_gc_stats(&mut accumulated, &part);
        assert_eq!(accumulated.entries_evicted, 11);
        assert_eq!(accumulated.bytes_freed, 22);
        assert_eq!(accumulated.entries_pinned, 33);
        assert_eq!(accumulated.blobs_removed, 44);
        assert_eq!(accumulated.duration_ms, 77);
        assert_eq!(accumulated.entries_unreclaimable, 55);
        assert_eq!(accumulated.disk_bytes_reclaimed, 66);
        assert!(accumulated.skipped);
    }

    #[test]
    fn evicting_nothing_because_everything_is_pinned_explains_itself() {
        // The #509 report: `evicted 0 entries` printed next to a store at 912%.
        // The number is right and the message is useless, so the user concludes
        // GC is broken. The output must name the grace and say what to do.
        let msg = describe_eviction(&gc_stats(0, 24, 0), true);
        assert!(
            msg.contains("24"),
            "must say how many were held back: {msg}"
        );
        assert!(
            msg.contains("120s"),
            "must name the grace period so the wait is bounded and knowable: {msg}"
        );
        assert!(
            msg.contains("Re-run"),
            "must tell the user what to do next: {msg}"
        );
        assert!(
            msg.contains("durable remote upload"),
            "the shared pin counter must describe upload-backed entries too: {msg}"
        );
    }

    #[test]
    fn evicting_nothing_while_over_limit_still_explains_the_grace() {
        // Nothing pinned and nothing evicted, but still over budget: the user
        // needs to know the idle rule exists, or "0" reads as a broken GC.
        let msg = describe_eviction(&gc_stats(0, 0, 0), true);
        assert!(msg.contains("over its limit"), "{msg}");
        assert!(msg.contains("120s"), "{msg}");
    }

    #[test]
    fn an_empty_store_does_not_imply_something_is_wrong() {
        // Under budget with nothing to do is the normal case and must not
        // inherit the alarming phrasing of the over-limit one.
        let msg = describe_eviction(&gc_stats(0, 0, 0), false);
        assert!(msg.contains("nothing to evict"), "{msg}");
        assert!(
            !msg.contains("over its limit"),
            "a healthy store must not be told it is over budget: {msg}"
        );
    }

    #[test]
    fn a_successful_eviction_reports_bytes_and_still_flags_pinned_entries() {
        let mut stats = gc_stats(12, 3, 5 * 1024 * 1024);
        stats.disk_bytes_reclaimed = 4 * 1024 * 1024;
        stats.entries_unreclaimable = 2;
        let msg = describe_eviction(&stats, false);
        assert!(msg.contains("12 entries"), "{msg}");
        assert!(msg.contains("MiB") || msg.contains("MB"), "{msg}");
        assert!(
            msg.contains('3'),
            "entries left behind matter even on a successful sweep — they are \
            why the store may still be over budget: {msg}"
        );
        assert!(msg.contains("became free on disk"), "{msg}");
        assert!(msg.contains("remains cloned"), "{msg}");
        assert!(msg.contains("2 entries left in place"), "{msg}");
    }

    #[test]
    fn fully_retained_eviction_has_a_cleanup_path() {
        let mut stats = gc_stats(0, 0, 0);
        stats.entries_unreclaimable = 1;
        let msg = describe_eviction(&stats, false);
        assert!(msg.contains("1 entry cloned"), "{msg}");
        assert!(msg.contains("clean --tracked"), "{msg}");
    }

    #[test]
    fn eviction_messages_are_singular_for_one_entry() {
        let mut stats = gc_stats(1, 0, 1024);
        stats.disk_bytes_reclaimed = 1024;
        let msg = describe_eviction(&stats, false);
        assert!(msg.contains("1 entry"), "{msg}");
        assert!(!msg.contains("1 entries"), "{msg}");
        assert!(!msg.contains("remains cloned"), "{msg}");
        assert!(!msg.contains("more 0 entries"), "{msg}");
        assert!(!msg.contains("0 entries left in place"), "{msg}");
    }

    #[test]
    fn a_clean_sweep_says_nothing_about_entries_left_behind() {
        // Nothing pinned: the grace-period paragraph would be noise at best,
        // and at worst reads as "some entries are stuck" on a sweep that
        // emptied everything it was asked to.
        let msg = describe_eviction(&gc_stats(7, 0, 4096), false);
        assert!(msg.contains("7 entries"), "{msg}");
        assert!(
            !msg.contains("in use within the last"),
            "no entries were held back, so the grace note must not appear: {msg}"
        );
    }

    #[test]
    fn over_limit_is_decided_by_a_strict_comparison_against_the_budget() {
        assert!(store_over_limit(Some(1025), 1024));
        assert!(
            !store_over_limit(Some(1024), 1024),
            "exactly at budget is within it: a store that fits must not be told \
             it is over"
        );
        assert!(!store_over_limit(Some(512), 1024));
        assert!(
            !store_over_limit(None, 1024),
            "an unreadable size must not be reported as over budget — that sends \
             the user chasing an eviction problem they may not have"
        );
    }

    /// Hardlink identity is tracked separately from extent sharing so callers
    /// can distinguish an external survivor from two names inside one target/.
    #[cfg(unix)]
    #[test]
    fn a_hardlinked_artifact_records_its_link_group() {
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("blob.bin");
        let linked = dir.path().join("target-copy.bin");
        fs::write(&blob, vec![0u8; 4096]).unwrap();
        fs::hard_link(&blob, &linked).unwrap();

        let meta = fs::metadata(&linked).unwrap();
        let observation = observe_storage(&linked, &meta);
        let hardlink = observation.hardlink.expect("nlink > 1 records a group");
        assert_eq!(hardlink.total_links, 2);
        let mut reclaim = ReclaimEstimator::default();
        reclaim.record(meta.len(), observation);
        assert_eq!(reclaim.estimated_reclaimable_bytes(), 0);
        assert!(reclaim.hardlink_has_external_ref(hardlink.id));

        let plain = dir.path().join("plain.bin");
        fs::write(&plain, vec![0u8; 4096]).unwrap();
        let plain_meta = fs::metadata(&plain).unwrap();
        let plain_observation = observe_storage(&plain, &plain_meta);
        assert!(plain_observation.hardlink.is_none());
        assert!(!plain_observation.sharing.shared);
    }

    fn zero_event_stats() -> daemon::EventStatsResponse {
        daemon::EventStatsResponse {
            local_hits: 0,
            prefetch_hits: 0,
            remote_hits: 0,
            dups: 0,
            misses: 0,
            errors: 0,
            total_elapsed_ms: 0,
            hit_elapsed_ms: 0,
            miss_elapsed_ms: 0,
            hit_compile_time_ms: 0,
            miss_compile_time_ms: 0,
            store_output_blobs: 0,
            store_duplicate_blobs: 0,
            store_new_blobs: 0,
        }
    }

    #[test]
    fn test_daemon_needed_when_remote_configured() {
        assert!(daemon_needed(true, false));
    }

    #[test]
    fn test_daemon_needed_when_planner_configured() {
        assert!(daemon_needed(false, true));
    }

    #[test]
    fn test_daemon_needed_when_both_configured() {
        assert!(daemon_needed(true, true));
    }

    #[test]
    fn test_daemon_not_needed_when_local_only_or_unconfigured() {
        assert!(!daemon_needed(false, false));
    }

    #[test]
    fn test_optional_failing_check_is_not_an_issue() {
        // A downgraded (optional) daemon check that failed must not count.
        assert!(!is_doctor_issue(false, true));
    }

    #[test]
    fn test_genuine_failing_check_is_an_issue() {
        assert!(is_doctor_issue(false, false));
    }

    #[test]
    fn test_passing_check_is_never_an_issue() {
        assert!(!is_doctor_issue(true, false));
        assert!(!is_doctor_issue(true, true));
    }

    #[test]
    fn test_count_hit_rate_zero_total_is_zero() {
        assert_eq!(count_hit_rate(&zero_event_stats()), 0.0);
    }

    #[test]
    fn test_count_hit_rate_counts_all_hit_kinds() {
        let es = daemon::EventStatsResponse {
            local_hits: 3,
            prefetch_hits: 2,
            remote_hits: 1,
            dups: 0,
            misses: 4,
            ..zero_event_stats()
        };
        // (3+2+1) hits / (6+4) total = 60%
        assert!((count_hit_rate(&es) - 60.0).abs() < 1e-9);
    }

    #[test]
    fn test_count_hit_rate_all_hits_is_hundred() {
        let es = daemon::EventStatsResponse {
            local_hits: 5,
            ..zero_event_stats()
        };
        assert!((count_hit_rate(&es) - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_compile_weighted_hit_rate_none_when_no_compile_time() {
        assert_eq!(compile_weighted_hit_rate(&zero_event_stats()), None);
    }

    #[test]
    fn test_compile_weighted_hit_rate_weights_by_time() {
        let es = daemon::EventStatsResponse {
            hit_compile_time_ms: 750,
            miss_compile_time_ms: 250,
            ..zero_event_stats()
        };
        let r = compile_weighted_hit_rate(&es).unwrap();
        assert!((r - 75.0).abs() < 1e-9);
    }

    #[test]
    fn test_key_short_truncates_long_keys() {
        assert_eq!(key_short("0123456789abcdefghij"), "0123456789ab");
        assert_eq!(key_short("short"), "short");
        // Exactly 12 chars: not truncated (len > 12 is the cutoff).
        assert_eq!(key_short("123456789012"), "123456789012");
    }

    #[test]
    fn test_format_duration_ms_buckets() {
        assert_eq!(format_duration_ms(0), "~0ms");
        assert_eq!(format_duration_ms(500), "~500ms");
        assert_eq!(format_duration_ms(1_000), "~1s");
        assert_eq!(format_duration_ms(59_000), "~59s");
        assert_eq!(format_duration_ms(60_000), "~1min");
        assert_eq!(format_duration_ms(3_600_000), "~1.0h");
        assert_eq!(format_duration_ms(7_200_000), "~2.0h");
    }

    #[test]
    fn test_format_relative_time_invalid_passes_through() {
        assert_eq!(format_relative_time("not a date"), "not a date");
    }

    #[test]
    fn test_format_relative_time_buckets() {
        let now = chrono::Utc::now();
        let fmt = |dt: chrono::DateTime<chrono::Utc>| {
            format_relative_time(&dt.format("%Y-%m-%d %H:%M:%S").to_string())
        };
        assert_eq!(fmt(now - chrono::Duration::seconds(10)), "just now");
        assert_eq!(fmt(now - chrono::Duration::minutes(5)), "5m ago");
        assert_eq!(fmt(now - chrono::Duration::hours(3)), "3h ago");
        assert_eq!(fmt(now - chrono::Duration::days(2)), "2d ago");
        // A future timestamp clamps to "just now" (secs.max(0)).
        assert_eq!(fmt(now + chrono::Duration::hours(1)), "just now");
    }

    #[test]
    fn test_is_binary_artifact_extensions() {
        // Non-binary artifacts
        assert!(!is_binary_artifact(std::path::Path::new("libfoo.d")));
        assert!(!is_binary_artifact(std::path::Path::new("libfoo.rmeta")));
        assert!(!is_binary_artifact(std::path::Path::new("libfoo.rlib")));

        // Binary artifacts
        assert!(is_binary_artifact(std::path::Path::new("myapp")));
        assert!(is_binary_artifact(std::path::Path::new("libfoo.dylib")));
        assert!(is_binary_artifact(std::path::Path::new("libfoo.so")));
        assert!(is_binary_artifact(std::path::Path::new("myapp.exe")));
        assert!(is_binary_artifact(std::path::Path::new("mylib.dll")));

        // Unknown extension defaults to non-binary
        assert!(!is_binary_artifact(std::path::Path::new("file.txt")));
    }

    #[test]
    fn test_detect_profiles_empty() {
        let dir = tempfile::tempdir().unwrap();
        let profiles = detect_profiles(dir.path());
        assert!(profiles.is_empty());
    }

    #[test]
    fn test_detect_profiles_with_dirs() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join("debug")).unwrap();
        fs::create_dir(dir.path().join("release")).unwrap();

        let profiles = detect_profiles(dir.path());
        assert!(profiles.contains(&"debug".to_string()));
        assert!(profiles.contains(&"release".to_string()));
        assert!(!profiles.contains(&"profiling".to_string()));
    }

    #[test]
    fn test_detect_profiles_all() {
        let dir = tempfile::tempdir().unwrap();
        for name in &["debug", "release", "profiling", "coverage"] {
            fs::create_dir(dir.path().join(name)).unwrap();
        }

        let profiles = detect_profiles(dir.path());
        assert_eq!(profiles.len(), 4);
    }

    #[test]
    fn test_sccache_program_detection_accepts_paths() {
        assert!(is_sccache_program("sccache"));
        assert!(is_sccache_program("/opt/homebrew/bin/sccache"));
        assert!(is_sccache_program("sccache.exe"));
        assert!(!is_sccache_program("kache"));
        assert!(!is_sccache_program("sccache-wrapper"));
    }

    #[test]
    fn test_sccache_rc_detection_ignores_fallback_setting() {
        assert!(!active_sccache_migration_line("# RUSTC_WRAPPER=sccache"));
        assert!(!active_sccache_migration_line(
            "export KACHE_FALLBACK=sccache"
        ));
        assert!(active_sccache_migration_line(
            "export RUSTC_WRAPPER=sccache"
        ));
        assert!(active_sccache_migration_line("rustc-wrapper = \"sccache\""));
    }

    #[test]
    fn test_fallback_is_sccache() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = save_manifest_config(dir.path().to_path_buf(), None);

        // No config / no fallback -> false.
        assert!(!fallback_is_sccache(None));
        assert!(!fallback_is_sccache(Some(&cfg)));

        // Fallback set to an sccache binary (incl. a full path) -> true.
        cfg.fallback = Some("sccache".to_string());
        assert!(fallback_is_sccache(Some(&cfg)));
        cfg.fallback = Some("/usr/local/bin/sccache".to_string());
        assert!(fallback_is_sccache(Some(&cfg)));

        // A non-sccache fallback -> false.
        cfg.fallback = Some("/usr/bin/gcc".to_string());
        assert!(!fallback_is_sccache(Some(&cfg)));
    }

    #[test]
    fn test_dir_size_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(dir_size(dir.path()), 0);
    }

    #[test]
    fn test_dir_size_with_files() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("a.txt"), vec![0u8; 100]).unwrap();
        fs::write(dir.path().join("b.txt"), vec![0u8; 200]).unwrap();

        let size = dir_size(dir.path());
        assert!(size >= 300, "expected >= 300, got {}", size);
    }

    #[test]
    fn test_dir_size_recursive() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("file.txt"), vec![0u8; 50]).unwrap();

        let size = dir_size(dir.path());
        assert!(size >= 50);
    }

    #[test]
    fn test_dir_size_nonexistent() {
        assert_eq!(dir_size(std::path::Path::new("/nonexistent/path")), 0);
    }

    #[test]
    fn test_find_target_dirs_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mut results = Vec::new();
        find_target_dirs(dir.path(), &mut results);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_target_dirs_with_cargo_project() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("myproject");
        fs::create_dir(&project).unwrap();
        fs::write(project.join("Cargo.toml"), "[package]\nname = \"test\"").unwrap();

        let target = project.join("target");
        fs::create_dir(&target).unwrap();
        let debug = target.join("debug");
        fs::create_dir(&debug).unwrap();
        fs::write(debug.join("test.rlib"), vec![0u8; 100]).unwrap();

        let mut results = Vec::new();
        find_target_dirs(dir.path(), &mut results);
        assert_eq!(results.len(), 1);
        assert!(results[0].size >= 100);
        assert!(results[0].profiles.contains(&"debug".to_string()));
    }

    #[test]
    fn find_target_dirs_ignores_an_empty_cargo_target() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("empty-project");
        fs::create_dir_all(project.join("target")).unwrap();
        fs::write(project.join("Cargo.toml"), "[package]\nname = \"empty\"").unwrap();

        let mut results = Vec::new();
        find_target_dirs(dir.path(), &mut results);

        assert!(
            results.is_empty(),
            "an empty target has no reclaimable bytes and must not be offered for deletion"
        );
    }

    #[test]
    fn test_find_target_dirs_skips_hidden() {
        let dir = tempfile::tempdir().unwrap();
        let hidden = dir.path().join(".hidden");
        fs::create_dir(&hidden).unwrap();
        fs::write(hidden.join("Cargo.toml"), "[package]").unwrap();
        fs::create_dir(hidden.join("target")).unwrap();

        let mut results = Vec::new();
        find_target_dirs(dir.path(), &mut results);
        assert!(results.is_empty());
    }

    #[test]
    fn test_find_target_dirs_skips_node_modules() {
        let dir = tempfile::tempdir().unwrap();
        let nm = dir.path().join("node_modules");
        fs::create_dir(&nm).unwrap();
        fs::write(nm.join("Cargo.toml"), "[package]").unwrap();
        fs::create_dir(nm.join("target")).unwrap();

        let mut results = Vec::new();
        find_target_dirs(dir.path(), &mut results);
        assert!(results.is_empty());
    }

    #[test]
    fn test_compute_project_stats_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let (stats, breakdown) = compute_project_stats(dir.path());
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.cached_bytes, 0);
        assert_eq!(stats.estimated_reclaimable_bytes, 0);
        assert_eq!(breakdown.incremental, 0);
    }

    #[test]
    fn reclaim_estimator_uses_private_bytes_for_partial_reflinks() {
        let mut reclaim = ReclaimEstimator::default();
        reclaim.record(
            10_000,
            StorageObservation {
                sharing: crate::sharing::Sharing {
                    shared: true,
                    private_bytes: 4_000,
                },
                hardlink: None,
            },
        );

        assert_eq!(
            reclaim.estimated_reclaimable_bytes(),
            4_000,
            "a partially shared reflink reclaims its private extents, not zero"
        );
    }

    #[cfg(unix)]
    #[test]
    fn reclaim_estimator_clamps_private_bytes_to_allocated_storage() {
        assert_eq!(clamp_private_bytes(1 << 30, 1 << 30, Some(0)), 0);
        assert_eq!(clamp_private_bytes(10_000, 8_000, Some(4_096)), 4_096);
        assert_eq!(clamp_private_bytes(10_000, 4_000, Some(8_192)), 4_000);
    }

    #[test]
    fn reclaim_estimator_collapses_internal_hardlink_groups() {
        let id = FileIdentity {
            device: 7,
            inode: 11,
        };
        let observation = StorageObservation {
            sharing: crate::sharing::Sharing::unknown_for(100),
            hardlink: Some(HardlinkObservation { id, total_links: 2 }),
        };
        let mut stats = ProjectStats::default();
        let mut breakdown = CategoryBreakdown::default();
        let mut reclaim = ReclaimEstimator::default();
        let mut candidates = Vec::new();

        for _ in 0..2 {
            record_scanned_file(
                &mut stats,
                &mut breakdown,
                &mut reclaim,
                &mut candidates,
                100,
                ProjectBucket::Deps,
                true,
                observation,
            );
        }
        finalize_cache_candidates(&mut stats, &mut breakdown, &reclaim, &candidates);

        assert_eq!(
            reclaim.estimated_reclaimable_bytes(),
            100,
            "two names for one inode reclaim that inode only once"
        );
        assert_eq!(
            stats.cached_bytes, 0,
            "hardlinks wholly inside target/ are not evidence of a store link"
        );
        assert_eq!(stats.local_bytes, 200, "both apparent paths stay local");
    }

    #[test]
    fn cache_candidates_accept_each_independent_store_sharing_signal() {
        let external_id = FileIdentity {
            device: 17,
            inode: 23,
        };
        let mut reclaim = ReclaimEstimator::default();
        reclaim.record(
            200,
            StorageObservation {
                sharing: crate::sharing::Sharing::unknown_for(200),
                hardlink: Some(HardlinkObservation {
                    id: external_id,
                    total_links: 2,
                }),
            },
        );

        let candidates = [
            CacheCandidate {
                size: 100,
                bucket: ProjectBucket::Deps,
                reflink_shared: true,
                hardlink_id: None,
            },
            CacheCandidate {
                size: 200,
                bucket: ProjectBucket::Deps,
                reflink_shared: false,
                hardlink_id: Some(external_id),
            },
        ];
        let mut stats = ProjectStats::default();
        let mut breakdown = CategoryBreakdown::default();
        finalize_cache_candidates(&mut stats, &mut breakdown, &reclaim, &candidates);

        assert_eq!(stats.cached_bytes, 300);
        assert_eq!(stats.cached_files, 2);
        assert_eq!(stats.local_bytes, 0);
        assert_eq!(breakdown.deps_local, 0);
    }

    #[test]
    fn reclaim_estimator_counts_binary_reflink_private_bytes() {
        let mut stats = ProjectStats::default();
        let mut breakdown = CategoryBreakdown::default();
        let mut reclaim = ReclaimEstimator::default();
        let mut candidates = Vec::new();

        record_scanned_file(
            &mut stats,
            &mut breakdown,
            &mut reclaim,
            &mut candidates,
            100,
            ProjectBucket::Binaries,
            false,
            StorageObservation {
                sharing: crate::sharing::Sharing {
                    shared: true,
                    private_bytes: 25,
                },
                hardlink: None,
            },
        );
        finalize_cache_candidates(&mut stats, &mut breakdown, &reclaim, &candidates);

        assert_eq!(reclaim.estimated_reclaimable_bytes(), 25);
        assert_eq!(stats.cached_bytes, 0, "binary bucketing stays unchanged");
        assert_eq!(breakdown.binaries, 100);
    }

    #[test]
    fn windows_identity_parts_preserve_volume_and_both_file_index_halves() {
        let identity = windows_file_identity_from_parts(0x1020_3040, 0x1122_3344, 0x5566_7788);
        assert_eq!(identity.device, 0x1020_3040);
        assert_eq!(identity.inode, 0x1122_3344_5566_7788);
        assert_ne!(
            identity,
            windows_file_identity_from_parts(0x1020_3040, 0x1122_3344, 0x5566_7789),
            "the low DWORD remains part of the stable identity"
        );
    }

    #[test]
    fn windows_storage_observation_covers_identity_and_link_count_boundaries() {
        let id = FileIdentity {
            device: 5,
            inode: 8,
        };

        let single = windows_storage_observation(4096, Some((id, 1)));
        assert_eq!(single.sharing.private_bytes, 4096);
        assert!(
            single.hardlink.is_none(),
            "one link is not a hardlink group"
        );

        let linked = windows_storage_observation(4096, Some((id, 2)));
        assert_eq!(
            linked.hardlink,
            Some(HardlinkObservation { id, total_links: 2 })
        );

        let unavailable = windows_storage_observation(4096, None);
        assert_eq!(unavailable.sharing.private_bytes, 0);
        assert!(!unavailable.sharing.shared);
        assert!(unavailable.hardlink.is_none());
    }

    #[test]
    fn unsupported_storage_observation_treats_the_whole_file_as_private() {
        let observation = unsupported_storage_observation(4096);
        assert_eq!(observation.sharing.private_bytes, 4096);
        assert!(!observation.sharing.shared);
        assert!(observation.hardlink.is_none());
    }

    #[cfg(windows)]
    #[test]
    fn windows_hardlinks_share_one_reclaimable_file() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.rlib");
        let second = dir.path().join("second.rlib");
        fs::write(&first, vec![0u8; 4096]).unwrap();
        fs::hard_link(&first, &second).unwrap();

        let first_meta = fs::metadata(&first).unwrap();
        let second_meta = fs::metadata(&second).unwrap();
        let (first_identity, first_links) =
            query_windows_file_identity(&first).expect("first hardlink has a Windows identity");
        let (second_identity, second_links) =
            query_windows_file_identity(&second).expect("second hardlink has a Windows identity");
        assert_eq!(first_identity, second_identity);
        assert_eq!((first_links, second_links), (2, 2));

        let distinct = dir.path().join("distinct.rlib");
        fs::write(&distinct, vec![0u8; 4096]).unwrap();
        let (distinct_identity, distinct_links) =
            query_windows_file_identity(&distinct).expect("a plain file has a Windows identity");
        assert_ne!(first_identity, distinct_identity);
        assert_eq!(distinct_links, 1);
        assert!(
            query_windows_file_identity(&dir.path().join("missing.rlib")).is_none(),
            "a failed open must not invent an identity"
        );

        let first_observation = observe_storage(&first, &first_meta);
        let second_observation = observe_storage(&second, &second_meta);
        assert_eq!(first_observation.hardlink, second_observation.hardlink);
        let distinct_observation = observe_storage(&distinct, &fs::metadata(&distinct).unwrap());
        assert!(distinct_observation.hardlink.is_none());

        let mut reclaim = ReclaimEstimator::default();
        reclaim.record(first_meta.len(), first_observation);
        reclaim.record(second_meta.len(), second_observation);
        assert_eq!(reclaim.estimated_reclaimable_bytes(), 4096);
    }

    #[test]
    fn test_compute_project_stats_with_profiles() {
        let dir = tempfile::tempdir().unwrap();
        let debug = dir.path().join("debug");
        fs::create_dir(&debug).unwrap();

        // incremental dir
        let incr = debug.join("incremental");
        fs::create_dir(&incr).unwrap();
        fs::write(incr.join("data"), vec![0u8; 100]).unwrap();

        // .fingerprint dir
        let fp = debug.join(".fingerprint");
        fs::create_dir(&fp).unwrap();
        fs::write(fp.join("hash"), vec![0u8; 50]).unwrap();

        // build dir
        let build = debug.join("build");
        fs::create_dir(&build).unwrap();
        fs::write(build.join("script"), vec![0u8; 30]).unwrap();

        // deps dir
        let deps = debug.join("deps");
        fs::create_dir(&deps).unwrap();
        fs::write(deps.join("libfoo.rlib"), vec![0u8; 200]).unwrap();

        let (stats, breakdown) = compute_project_stats(dir.path());
        assert!(stats.total_bytes > 0);
        assert!(breakdown.incremental >= 100);
        assert!(breakdown.fingerprints >= 50);
        assert!(breakdown.build_scripts >= 30);
    }

    #[cfg(unix)]
    #[test]
    fn compute_project_stats_classifies_an_externally_hardlinked_rlib_as_cached() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target");
        let debug = target.join("debug");
        fs::create_dir_all(&debug).unwrap();

        let retained_blob = dir.path().join("store-blob.rlib");
        fs::write(&retained_blob, vec![0u8; 4096]).unwrap();
        fs::hard_link(&retained_blob, debug.join("libcached.rlib")).unwrap();

        let (stats, breakdown) = compute_project_stats(&target);
        assert_eq!(stats.total_bytes, 4096);
        assert_eq!(stats.cached_bytes, 4096);
        assert_eq!(stats.cached_files, 1);
        assert_eq!(stats.local_bytes, 0);
        assert_eq!(breakdown.other, 0);
    }

    #[cfg(unix)]
    #[test]
    fn compute_project_stats_does_not_follow_profile_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::write(outside.path().join("outside.rlib"), vec![0u8; 4096]).unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("debug")).unwrap();

        let (stats, _) = compute_project_stats(dir.path());
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.estimated_reclaimable_bytes, 0);
        assert!(detect_profiles(dir.path()).is_empty());
    }

    #[test]
    fn test_compute_project_stats_classifies_remaining_buckets() {
        // Profile files/directories -> binaries, deps-local, and other buckets.
        let dir = tempfile::tempdir().unwrap();
        let debug = dir.path().join("debug");
        let deps_nested = debug.join("deps").join("nested");
        let other_dir = debug.join("examples");
        fs::create_dir_all(&deps_nested).unwrap();
        fs::create_dir_all(&other_dir).unwrap();
        fs::write(debug.join("runner"), vec![0u8; 11]).unwrap();
        fs::write(deps_nested.join("libdep.rmeta"), vec![0u8; 13]).unwrap();
        fs::write(other_dir.join("note.txt"), vec![0u8; 17]).unwrap();
        fs::write(dir.path().join("CACHEDIR.TAG"), vec![0u8; 19]).unwrap();

        let (stats, breakdown) = compute_project_stats(dir.path());
        assert_eq!(stats.total_bytes, 60);
        assert!(breakdown.binaries >= 11, "got {}", breakdown.binaries);
        assert!(breakdown.deps_local >= 13, "got {}", breakdown.deps_local);
        assert!(breakdown.other >= 36, "got {}", breakdown.other);
        assert_eq!(
            stats.local_files, 4,
            "nested local files are counted individually"
        );
    }

    #[test]
    fn test_parse_cargo_lock_crate_names_nonexistent() {
        // When Cargo.lock doesn't exist in cwd, should return None
        // We can't guarantee cwd lacks Cargo.lock, so just test the function doesn't panic
        let _ = parse_cargo_lock_crate_names();
    }

    #[test]
    fn test_parse_cargo_lock_crate_names_from_valid_missing_and_bad_files() {
        // Cargo.lock parser -> valid names, missing file, and malformed TOML.
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(
            &lock,
            "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n\n\
             [[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();

        let names = parse_cargo_lock_crate_names_from(&lock).unwrap();
        assert!(names.contains("serde"));
        assert!(names.contains("tokio"));
        assert_eq!(
            parse_cargo_lock_crate_names_from(&dir.path().join("missing.lock")),
            None
        );
        std::fs::write(&lock, "not valid toml [[[[").unwrap();
        assert_eq!(parse_cargo_lock_crate_names_from(&lock), None);
    }

    #[test]
    fn test_is_macos_protected() {
        // On non-macOS the stub always returns false — verify that invariant
        // and skip the positive-match assertions.
        if !cfg!(target_os = "macos") {
            assert!(!is_macos_protected(std::path::Path::new("/System/Library")));
            assert!(!is_macos_protected(std::path::Path::new("/tmp/build")));
            return;
        }

        // System paths
        assert!(is_macos_protected(std::path::Path::new("/System/Library")));
        assert!(is_macos_protected(std::path::Path::new(
            "/Library/Preferences"
        )));
        assert!(is_macos_protected(std::path::Path::new(
            "/Applications/Xcode.app"
        )));
        assert!(is_macos_protected(std::path::Path::new(
            "/Volumes/External"
        )));
        assert!(is_macos_protected(std::path::Path::new("/private/var")));
        assert!(is_macos_protected(std::path::Path::new("/Network/Servers")));

        // Home TCC dirs (if home is available)
        if let Some(home) = dirs::home_dir() {
            assert!(is_macos_protected(&home.join("Desktop")));
            assert!(is_macos_protected(&home.join("Documents")));
            assert!(is_macos_protected(&home.join("Downloads")));
            assert!(is_macos_protected(&home.join("Library")));
            assert!(is_macos_protected(&home.join("Pictures")));
            assert!(is_macos_protected(&home.join("Music")));
            assert!(is_macos_protected(&home.join("Movies")));
            assert!(is_macos_protected(&home.join("Applications")));
            assert!(is_macos_protected(&home.join("Public")));
            // Nested paths under protected dirs are also caught
            assert!(is_macos_protected(&home.join("Documents/subfolder")));

            // Developer directories are NOT protected
            assert!(!is_macos_protected(&home.join("projects")));
            assert!(!is_macos_protected(&home.join("src")));
            assert!(!is_macos_protected(&home.join("work")));
            assert!(!is_macos_protected(&home.join(".config")));
        }

        // Arbitrary dev paths are not protected
        assert!(!is_macos_protected(std::path::Path::new("/tmp/build")));
        assert!(!is_macos_protected(std::path::Path::new("/Users/dev/code")));
    }

    #[test]
    fn test_category_breakdown_default() {
        let b = CategoryBreakdown::default();
        assert_eq!(b.incremental, 0);
        assert_eq!(b.build_scripts, 0);
        assert_eq!(b.fingerprints, 0);
        assert_eq!(b.binaries, 0);
        assert_eq!(b.deps_local, 0);
        assert_eq!(b.other, 0);
    }

    #[test]
    fn dry_run_never_writes_an_init_edit() {
        assert!(should_write_init_step(false, true));
        assert!(!should_write_init_step(true, true));
        assert!(!should_write_init_step(false, false));
        assert!(!should_write_init_step(true, false));
    }

    #[test]
    fn test_cargo_wrapper_edit_create() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let plan = plan_cargo_wrapper_edit(&path).unwrap();
        assert!(matches!(plan, CargoWrapperPlan::Create));
        let new = apply_cargo_wrapper_edit("", &plan);
        assert_eq!(new, "[build]\nrustc-wrapper = \"kache\"\n");
    }

    #[test]
    fn test_cargo_wrapper_edit_replace() {
        let existing = "[build]\nrustc-wrapper = \"sccache\"\n";
        let plan = CargoWrapperPlan::Replace("sccache".into());
        let new = apply_cargo_wrapper_edit(existing, &plan);
        assert_eq!(new, "[build]\nrustc-wrapper = \"kache\"\n");
    }

    #[test]
    fn test_cargo_wrapper_edit_replace_quote_styles_and_miss() {
        // Replace plan -> single-quoted, compact, and no-match branches.
        let single = apply_cargo_wrapper_edit(
            "[build]\nrustc-wrapper = 'sccache'\n",
            &CargoWrapperPlan::Replace("sccache".into()),
        );
        assert_eq!(single, "[build]\nrustc-wrapper = \"kache\"\n");

        let compact = apply_cargo_wrapper_edit(
            "[build]\nrustc-wrapper=\"sccache\"\n",
            &CargoWrapperPlan::Replace("sccache".into()),
        );
        assert_eq!(compact, "[build]\nrustc-wrapper = \"kache\"\n");

        let unchanged = apply_cargo_wrapper_edit(
            "[build]\nrustc-wrapper = \"other\"\n",
            &CargoWrapperPlan::Replace("sccache".into()),
        );
        assert_eq!(unchanged, "[build]\nrustc-wrapper = \"other\"\n");
    }

    #[test]
    fn test_cargo_wrapper_edit_add_under_build() {
        let existing = "[build]\njobs = 4\n";
        let plan = CargoWrapperPlan::AddUnderBuild;
        let new = apply_cargo_wrapper_edit(existing, &plan);
        assert!(new.contains("rustc-wrapper = \"kache\""));
        assert!(new.contains("jobs = 4"));
    }

    #[test]
    fn test_cargo_wrapper_edit_append_section() {
        let existing = "[net]\nretry = 3\n";
        let plan = CargoWrapperPlan::AppendSection;
        let new = apply_cargo_wrapper_edit(existing, &plan);
        assert!(new.contains("[net]"));
        assert!(new.trim_end().ends_with("rustc-wrapper = \"kache\""));
    }

    #[test]
    fn test_backup_path_has_kache_backup_suffix() {
        let path = std::path::Path::new("/tmp/cargo/config.toml");
        let backup = backup_path_for(path).unwrap();
        let name = backup.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("config.toml.kache-backup."), "got {name}");
        // Timestamp is a 15-char suffix: YYYYMMDD-HHMMSS
        assert_eq!(name.len(), "config.toml.kache-backup.".len() + 15);
        assert_eq!(backup.parent(), path.parent());
    }

    #[test]
    fn test_cargo_wrapper_edit_already_set() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[build]\nrustc-wrapper = \"kache\"\n").unwrap();
        let plan = plan_cargo_wrapper_edit(&path).unwrap();
        assert!(matches!(plan, CargoWrapperPlan::AlreadySet));
    }

    // The planner's Replace / AddUnderBuild / AppendSection arms are reached by
    // reading a real config file (the apply tests above build those plans by
    // hand). Drive each shape through the file-reading path, then apply the
    // resulting plan to confirm the round-trip lands kache as the wrapper.
    #[test]
    fn test_plan_cargo_wrapper_edit_replace_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[build]\nrustc-wrapper = \"sccache\"\n").unwrap();
        let plan = plan_cargo_wrapper_edit(&path).unwrap();
        assert_eq!(plan, CargoWrapperPlan::Replace("sccache".into()));
        let new = apply_cargo_wrapper_edit(&std::fs::read_to_string(&path).unwrap(), &plan);
        assert_eq!(new, "[build]\nrustc-wrapper = \"kache\"\n");
    }

    #[test]
    fn test_plan_cargo_wrapper_edit_add_under_build_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[build]\njobs = 8\n").unwrap();
        let plan = plan_cargo_wrapper_edit(&path).unwrap();
        assert_eq!(plan, CargoWrapperPlan::AddUnderBuild);
        let new = apply_cargo_wrapper_edit(&std::fs::read_to_string(&path).unwrap(), &plan);
        assert!(new.contains("jobs = 8"));
        assert!(new.contains("rustc-wrapper = \"kache\""));
    }

    #[test]
    fn test_plan_cargo_wrapper_edit_append_section_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "[net]\nretry = 2\n").unwrap();
        let plan = plan_cargo_wrapper_edit(&path).unwrap();
        assert_eq!(plan, CargoWrapperPlan::AppendSection);
        let new = apply_cargo_wrapper_edit(&std::fs::read_to_string(&path).unwrap(), &plan);
        assert!(new.contains("[net]"));
        assert!(new.contains("[build]"));
        assert!(new.contains("rustc-wrapper = \"kache\""));
    }

    #[test]
    fn test_plan_cargo_wrapper_edit_rejects_malformed_toml() {
        // A file that isn't valid TOML surfaces the parse-error context arm.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "this = = not valid toml\n").unwrap();
        let err = plan_cargo_wrapper_edit(&path).unwrap_err();
        assert!(
            err.to_string().contains("parsing"),
            "expected a parse-context error, got: {err}"
        );
    }

    #[test]
    fn test_get_workspace_crate_names_lists_members() {
        // A two-member workspace; `cargo metadata --no-deps` should report both.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(
            root.join("Cargo.toml"),
            "[workspace]\nmembers = [\"a\", \"b\"]\nresolver = \"2\"\n",
        )
        .unwrap();
        for m in ["a", "b"] {
            std::fs::create_dir_all(root.join(m).join("src")).unwrap();
            std::fs::write(
                root.join(m).join("Cargo.toml"),
                format!("[package]\nname = \"{m}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n"),
            )
            .unwrap();
            std::fs::write(root.join(m).join("src/lib.rs"), "").unwrap();
        }

        let names = get_workspace_crate_names(root.join("Cargo.toml").to_str().unwrap()).unwrap();
        assert!(names.contains(&"a".to_string()), "got {names:?}");
        assert!(names.contains(&"b".to_string()), "got {names:?}");
    }

    #[test]
    fn test_get_workspace_crate_names_errors_on_bad_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("Cargo.toml");
        std::fs::write(&bad, "this is not valid toml [[[").unwrap();
        assert!(get_workspace_crate_names(bad.to_str().unwrap()).is_err());
    }

    #[test]
    fn test_workspace_filter_bad_manifest_is_empty_set() {
        // Explicit bad manifest path -> warning branch with empty filter.
        let dir = tempfile::tempdir().unwrap();
        let bad = dir.path().join("Cargo.toml");
        std::fs::write(&bad, "not toml [[[[").unwrap();

        let filter = workspace_filter(Some(bad.to_str().unwrap())).unwrap();
        assert!(filter.is_empty());
    }

    // ── backend-neutral remote tests ──────────────────────────────────────────

    #[derive(Default)]
    struct BackendCalls {
        gets: Vec<String>,
        puts: Vec<String>,
        lists: Vec<String>,
    }

    struct TestBackend {
        inner: crate::remote_backend::OpenDalBackend,
        calls: std::sync::Mutex<BackendCalls>,
        fail_put: bool,
    }

    impl TestBackend {
        fn memory() -> Arc<Self> {
            Arc::new(Self {
                inner: crate::remote_backend::memory_backend(),
                calls: std::sync::Mutex::new(BackendCalls::default()),
                fail_put: false,
            })
        }

        fn failing_put() -> Arc<Self> {
            Arc::new(Self {
                inner: crate::remote_backend::memory_backend(),
                calls: std::sync::Mutex::new(BackendCalls::default()),
                fail_put: true,
            })
        }

        async fn seed(&self, key: &str, body: impl Into<Vec<u8>>) {
            crate::remote_backend::RemoteBackend::put(&self.inner, key, body.into(), None)
                .await
                .expect("seed remote object");
        }

        fn get_calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().gets.clone()
        }

        fn put_calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().puts.clone()
        }

        fn list_calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().lists.clone()
        }
    }

    fn as_remote_backend(
        backend: &Arc<TestBackend>,
    ) -> Arc<dyn crate::remote_backend::RemoteBackend> {
        backend.clone()
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for TestBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            crate::remote_backend::RemoteBackend::head(&self.inner, key).await
        }

        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            self.calls.lock().unwrap().gets.push(key.to_string());
            crate::remote_backend::RemoteBackend::get(&self.inner, key, max_bytes).await
        }

        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            self.calls.lock().unwrap().puts.push(key.to_string());
            if self.fail_put {
                anyhow::bail!("injected PUT failure for {key}");
            }
            crate::remote_backend::RemoteBackend::put(&self.inner, key, body, content_type).await
        }

        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.calls.lock().unwrap().lists.push(prefix.to_string());
            crate::remote_backend::RemoteBackend::list(&self.inner, prefix).await
        }

        fn describe(&self, key: &str) -> String {
            crate::remote_backend::RemoteBackend::describe(&self.inner, key)
        }
    }

    #[tokio::test]
    async fn upload_shards_uploads_one_shard_per_nonempty_bucket() {
        // Two deps that both have build events -> they land in (likely) two
        // shards; assert the upload count equals the shards that had entries.
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(
            &lock,
            "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n\n\
             [[package]]\nname = \"tokio\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();

        let entries = vec![
            crate::remote::ManifestEntry {
                cache_key: "k-serde".to_string(),
                crate_name: "serde".to_string(),
                compile_time_ms: 1,
                artifact_size: 1,
            },
            crate::remote::ManifestEntry {
                cache_key: "k-tokio".to_string(),
                crate_name: "tokio".to_string(),
                compile_time_ms: 1,
                artifact_size: 1,
            },
        ];

        // Compute how many shards actually carry entries, so the test is robust
        // to the bucket assignment.
        let deps = crate::shards::parse_cargo_lock(&lock).unwrap();
        let shard_set = crate::shards::compute_shards("ns", &deps);
        let expected = shard_set.shards.len();

        let backend = TestBackend::memory();
        let client = as_remote_backend(&backend);

        let uploaded = upload_shards(&client, "prefix", "ns", &lock, &entries)
            .await
            .expect("upload_shards should succeed");
        assert_eq!(uploaded, expected);
        let puts = backend.put_calls();
        assert_eq!(puts.len(), expected);
        assert!(
            puts.iter()
                .all(|key| key.starts_with("prefix/_manifests/v3/ns/shards/"))
        );
    }

    #[tokio::test]
    async fn upload_shards_skips_when_no_entries_match() {
        // Deps present but no matching build events -> no shards uploaded, so
        // no S3 requests are made.
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(
            &lock,
            "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();

        let backend = TestBackend::memory();
        let client = as_remote_backend(&backend);
        let uploaded = upload_shards(&client, "prefix", "ns", &lock, &[])
            .await
            .expect("should succeed with nothing to upload");
        assert_eq!(uploaded, 0);
        assert!(backend.put_calls().is_empty());
    }

    #[tokio::test]
    async fn upload_shards_errors_on_malformed_lockfile() {
        // Bad Cargo.lock -> parse error before any shard upload.
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(&lock, "not valid toml [[[[").unwrap();
        let backend = TestBackend::memory();
        let client = as_remote_backend(&backend);

        let err = upload_shards(&client, "prefix", "ns", &lock, &[])
            .await
            .expect_err("bad lockfile should error");
        assert!(
            err.to_string().contains("TOML") || err.to_string().contains("parse"),
            "got {err}"
        );
        assert!(backend.put_calls().is_empty());
    }

    fn save_manifest_config(
        cache_dir: std::path::PathBuf,
        remote: Option<crate::config::RemoteConfig>,
    ) -> Config {
        use crate::config::{
            DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
            DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS,
        };
        Config {
            remote_error: None,
            socket_path_override: None,
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            gc_evict_shared: false,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            scheduler: true,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            runtime_dir: cache_dir.clone(),
            cache_dir,
            max_size: 1024 * 1024,
            remote,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            prefetch_enabled: crate::config::DEFAULT_PREFETCH_ENABLED,
            remote_key_cache_refresh_secs: crate::config::DEFAULT_REMOTE_KEY_CACHE_REFRESH_SECS,
            prefetch_max_keys: crate::config::DEFAULT_PREFETCH_MAX_KEYS,
            prefetch_max_bytes: crate::config::DEFAULT_PREFETCH_MAX_BYTES,
            prefetch_deadline_secs: crate::config::DEFAULT_PREFETCH_DEADLINE_SECS,
            min_store_compile_ms: crate::config::DEFAULT_MIN_STORE_COMPILE_MS,
            gc_max_age_hours: crate::config::DEFAULT_GC_MAX_AGE_HOURS,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        }
    }

    fn put_entry(config: &Config, key: &str, crate_name: &str, dir: &std::path::Path) {
        let store = Store::open(config).unwrap();
        let src = dir.join(format!("{key}.rlib"));
        std::fs::write(&src, format!("artifact bytes for {key}")).unwrap();
        store
            .put(
                key,
                crate_name,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(src, format!("{key}.rlib"))],
                "",
                "",
            )
            .unwrap();
    }

    fn overwrite_entry_meta(
        config: &Config,
        key: &str,
        crate_name: &str,
        mut meta: crate::store::EntryMeta,
    ) {
        meta.cache_key = key.to_string();
        meta.crate_name = crate_name.to_string();
        std::fs::write(
            config.store_dir().join(key).join("meta.json"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn why_miss_no_events_prints_tip() {
        // No events for the crate -> the "build it first" tip path.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        why_miss(&config, "ghost", false).expect("why_miss with no events should succeed");
    }

    #[test]
    fn why_miss_shows_stored_metadata_for_a_missed_key() {
        // A Miss event whose cache_key has a stored entry on disk drives the
        // miss-metadata display (target/profile) + the stored-entries listing.
        // Covers why_miss's metadata branch (cli.rs ~471-490+).
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        // Seed a store entry for "serde" so meta.json (with target/profile) exists.
        put_entry(&config, "serdemisskey", "serde", dir.path());
        // Log a Miss event for that crate + key.
        crate::events::log_event(
            &config.event_log_path(),
            &build_event(
                "serde",
                crate::events::EventResult::Miss,
                1234,
                1300,
                4096,
                "serdemisskey",
            ),
        )
        .unwrap();

        why_miss(&config, "serde", false).expect("why_miss should succeed for a missed key");
    }

    #[test]
    fn why_miss_leads_with_a_store_failure_when_there_was_one() {
        // The store-failure banner replaces guesswork about the key: nothing was
        // stored, so no later build could have matched (kunobi-ninja/kache#629).
        let mut miss = build_event(
            "lint_crate",
            crate::events::EventResult::Miss,
            1000,
            900,
            4096,
            "somekey",
        );
        assert!(
            store_failure_banner(&miss).is_none(),
            "a normal miss gets no banner"
        );

        miss.store_error = "creating blob shard directory: Permission denied (os error 13)".into();
        let banner = store_failure_banner(&miss).expect("failed store should produce a banner");
        assert!(banner.contains("NOT CACHED"));
        assert!(banner.contains("Permission denied (os error 13)"));
        // The stored-entry analysis owns the `Diagnosis:` label; a second one
        // here would read as a competing conclusion.
        assert!(!banner.contains("Diagnosis:"), "banner: {banner}");
    }

    #[test]
    fn why_miss_prioritizes_same_key_lookup_rejection() {
        let mut miss = build_event(
            "foo.c",
            crate::events::EventResult::Miss,
            10,
            20,
            30,
            "same-key",
        );
        assert!(lookup_rejection_banner(&miss, true).is_none());

        miss.lookup_rejection =
            "matching entry lacks dep-info required by this invocation".to_string();
        let banner = lookup_rejection_banner(&miss, true).unwrap();
        assert!(banner.contains("matching key was found but rejected"));
        assert!(banner.contains("lacks dep-info required"));
        assert!(banner.contains("currently present under the same key"));
        assert!(!banner.contains("key mismatch"), "banner: {banner}");
    }

    #[test]
    fn why_miss_legacy_same_key_repeat_does_not_claim_key_mismatch() {
        let mut miss = build_event(
            "foo.c",
            crate::events::EventResult::Miss,
            10,
            20,
            30,
            "same-key",
        );
        miss.schema = 14;

        assert!(legacy_repeated_same_key_banner(&miss, false).is_none());
        let banner = legacy_repeated_same_key_banner(&miss, true).unwrap();
        assert!(banner.contains("repeated miss for the same cache key"));
        assert!(banner.contains("older event did not record"));
        assert!(banner.contains("not caused by a cache-key change"));
        assert!(
            !banner.contains("Diagnosis: key mismatch"),
            "banner: {banner}"
        );

        miss.schema = 15;
        assert!(legacy_repeated_same_key_banner(&miss, true).is_none());
    }

    #[test]
    fn why_miss_all_hits_reports_no_misses() {
        // Events exist but none are Miss/Dup -> the "all events are hits" path.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        crate::events::log_event(
            &config.event_log_path(),
            &build_event(
                "tokio",
                crate::events::EventResult::LocalHit,
                0,
                5,
                4096,
                "tokiohitkey",
            ),
        )
        .unwrap();

        why_miss(&config, "tokio", false).expect("why_miss with only hits should succeed");
    }

    #[test]
    fn why_miss_reports_many_stored_diffs() {
        // Stored miss + many other entries -> diff printer cap branch.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "misskeymanydiffs", "serde", dir.path());
        overwrite_entry_meta(
            &config,
            "misskeymanydiffs",
            "serde",
            diff_meta("wasm32", "release", &[], &["lib"]),
        );
        for i in 0..3 {
            let key = format!("otherkeymanydiffs{i}");
            let feature = format!("feat{i}");
            put_entry(&config, &key, "serde", dir.path());
            overwrite_entry_meta(
                &config,
                &key,
                "serde",
                diff_meta(
                    &format!("target{i}"),
                    &format!("profile{i}"),
                    &[&feature],
                    &["bin"],
                ),
            );
        }
        crate::events::log_event(
            &config.event_log_path(),
            &build_event(
                "serde",
                crate::events::EventResult::Miss,
                123,
                130,
                4096,
                "misskeymanydiffs",
            ),
        )
        .unwrap();

        why_miss(&config, "serde", false).expect("why_miss should print capped diffs");
    }

    #[test]
    fn why_miss_diff_messages_cap_feature_and_type_diffs() {
        // Diff helper -> empty miss features, crate-type diffs, and output cap.
        let miss = diff_meta("wasm32", "release", &[], &["lib"]);
        let others: Vec<_> = (0..3)
            .map(|i| {
                (
                    format!("other{i}"),
                    diff_meta("x86_64", "debug", &[&format!("feat{i}")], &["bin"]),
                )
            })
            .collect();
        let (messages, extra) = why_miss_diff_messages(
            &miss,
            others.iter().map(|(key, meta)| (key.as_str(), meta)),
            5,
        );

        assert_eq!(messages.len(), 5);
        assert!(extra > 0, "expected capped messages, got {messages:?}");
        assert!(messages.iter().any(|m| m.contains("different target")));
        assert!(messages.iter().any(|m| m.contains("different profile")));
        assert!(
            messages.iter().any(|m| m.contains("[(none)] vs [feat0]")),
            "got {messages:?}"
        );
        assert!(messages.iter().any(|m| m.contains("different crate types")));
    }

    #[test]
    fn why_miss_diff_messages_dedupes_same_config_and_empty_other_features() {
        // Diff helper -> empty other features and same-config de-duplication.
        let miss = diff_meta("x86_64", "debug", &["feat"], &["lib"]);
        let others = [
            (
                "empty-features",
                diff_meta("x86_64", "debug", &[], &["lib"]),
            ),
            ("same-a", diff_meta("x86_64", "debug", &["feat"], &["lib"])),
            ("same-b", diff_meta("x86_64", "debug", &["feat"], &["lib"])),
        ];
        let (messages, extra) =
            why_miss_diff_messages(&miss, others.iter().map(|(key, meta)| (*key, meta)), 5);

        assert_eq!(extra, 0);
        assert!(
            messages.iter().any(|m| m.contains("[feat] vs [(none)]")),
            "got {messages:?}"
        );
        assert_eq!(
            messages
                .iter()
                .filter(|m| m.contains("likely source code"))
                .count(),
            1
        );
    }

    #[test]
    fn telemetry_write_emits_cache_scope_not_bench() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let out = dir.path().join("otlp");
        telemetry_write(&config, &out, Some("bench-firefox"), Some("warm"))
            .expect("telemetry write");
        let body = std::fs::read_to_string(out.join("metrics.otlp.json")).unwrap();
        assert!(
            body.contains("\"kache.cache\""),
            "cache OTLP must use the kache.cache scope"
        );
        assert!(
            body.contains("\"kache.cache.scenario\""),
            "bench dumps must name the scenario"
        );
        assert!(
            body.contains("bench-firefox"),
            "scenario value must match kache.bench.project"
        );
        assert!(
            body.contains("\"kache.cache.phase\""),
            "bench dumps must name the phase"
        );
        assert!(
            body.contains("warm"),
            "phase value must match the bench phase"
        );
        assert!(
            !body.contains("kache.bench."),
            "cache dump must not mix bench gauges"
        );
        assert!(
            !body.contains("\"sum\""),
            "Kartero drops non-gauge series; cache dump must be gauges"
        );
        assert_eq!(
            std::fs::read_to_string(out.join("schema_version"))
                .unwrap()
                .trim(),
            "1"
        );
    }

    #[test]
    fn purge_skips_corrupt_filtered_entry() {
        // purge(crate) -> corrupt meta removal error is skipped, not fatal.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "badpurgekey", "bad", dir.path());
        std::fs::write(
            config.store_dir().join("badpurgekey").join("meta.json"),
            b"{ not valid json",
        )
        .unwrap();

        purge(&config, Some("bad")).expect("purge skips corrupt entries");
        assert!(
            config
                .store_dir()
                .join("badpurgekey")
                .join("meta.json")
                .exists(),
            "corrupt entry remains accounted-for after skipped purge"
        );
    }

    /// Purge is a bulk mutation and must serialize behind the same
    /// cross-process lock every GC driver takes; unlocked, a purge can
    /// interleave with a live sweep and each observes the other's
    /// half-removed state.
    #[test]
    fn purge_waits_for_the_gc_lock() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "purgelockkey", "locked", dir.path());

        let store = crate::store::Store::open(&config).unwrap();
        let gc_lock = store.try_gc_lock().unwrap().expect("uncontended lock");

        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let cfg = config.clone();
        let worker = std::thread::spawn(move || {
            let result = purge(&cfg, None);
            let _ = done_tx.send(());
            result
        });

        // While the "sweep" holds gc.lock the purge must not complete.
        // (A scheduling hiccup can only delay the mutant's completion
        // signal, never produce a false failure for the real code.)
        assert!(
            done_rx
                .recv_timeout(std::time::Duration::from_millis(300))
                .is_err(),
            "purge completed while the GC lock was held"
        );
        drop(gc_lock);
        done_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("purge proceeds once the GC lock is released");
        worker.join().unwrap().expect("purge succeeds");
        let store = crate::store::Store::open(&config).unwrap();
        assert_eq!(store.entry_count().unwrap(), 0, "store cleared");
    }

    #[test]
    fn verify_reports_valid_entries_on_a_clean_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "validkey1", "serde", dir.path());

        // A clean store verifies with checksums and no repair needed.
        verify(&config, true, false).expect("verify of a clean store should succeed");
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)] // incremental snapshot setup reads clearer
    fn render_stats_rich_snapshot_covers_all_lines() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
        config.remote.as_mut().unwrap().prefix = "artifacts".to_string();

        let mut snap = StatsSnapshot::default();
        snap.total_size = 5000;
        snap.max_size = 10000;
        snap.entry_count = 3;
        snap.daemon_connected = true;
        snap.daemon_version = "9.9.9".to_string();
        snap.daemon_build_epoch = crate::daemon::build_epoch(); // matches -> no mismatch
        snap.event_stats.local_hits = 8;
        snap.event_stats.misses = 2;
        snap.event_stats.total_elapsed_ms = 1000;
        snap.event_stats.miss_elapsed_ms = 300;
        snap.event_stats.hit_compile_time_ms = 5000;
        snap.event_stats.miss_compile_time_ms = 2000;

        snap.blob_stats = Some(crate::store::BlobStats {
            total_blobs: 4,
            total_blob_size: 2048,
            total_logical_size: 4096,
            savings: 2048,
        });
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("Store:"));
        assert!(out.contains("Dedup:      4 unique blobs"));
        assert!(out.contains("Hit rate:"));
        assert!(out.contains("Weighted:"));
        assert!(out.contains("Miss share:"));
        assert!(out.contains("Time saved:"));
        assert!(out.contains("Daemon:     v9.9.9"));
        assert!(
            !out.contains("MISMATCH"),
            "matching epoch -> no mismatch tag"
        );
        assert!(out.contains("Remote:     s3://"));
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn render_stats_resilience_requires_both_sources_and_any_single_signal() {
        let dir = tempfile::tempdir().unwrap();
        let remote_config =
            save_manifest_config(dir.path().join("remote-cache"), Some(test_remote_cfg()));
        let local_config = save_manifest_config(dir.path().join("local-cache"), None);

        let mut quiet = StatsSnapshot::default();
        quiet.daemon_connected = true;
        assert!(
            render_stats(&quiet, &remote_config, SinceWindow::DEFAULT)
                .iter()
                .all(|line| !line.starts_with("Resilience:"))
        );

        let signals = [
            (
                "round trips",
                StatsSnapshot {
                    remote_check_roundtrips: 1,
                    ..Default::default()
                },
            ),
            (
                "negative hits",
                StatsSnapshot {
                    negative_hits: 1,
                    ..Default::default()
                },
            ),
            (
                "download suppression",
                StatsSnapshot {
                    downloads_suppressed: 1,
                    ..Default::default()
                },
            ),
            (
                "upload suppression",
                StatsSnapshot {
                    uploads_suppressed: 1,
                    ..Default::default()
                },
            ),
            (
                "degraded state",
                StatsSnapshot {
                    remote_degraded: true,
                    ..Default::default()
                },
            ),
        ];
        for (signal, mut snap) in signals {
            snap.daemon_connected = true;
            assert!(
                render_stats(&snap, &remote_config, SinceWindow::DEFAULT)
                    .iter()
                    .any(|line| line.starts_with("Resilience:")),
                "{signal} must independently render the resilience section"
            );
        }

        let disconnected = StatsSnapshot {
            negative_hits: 1,
            ..Default::default()
        };
        assert!(
            render_stats(&disconnected, &remote_config, SinceWindow::DEFAULT)
                .iter()
                .all(|line| !line.starts_with("Resilience:"))
        );
        let connected_without_remote = StatsSnapshot {
            daemon_connected: true,
            negative_hits: 1,
            ..Default::default()
        };
        assert!(
            render_stats(
                &connected_without_remote,
                &local_config,
                SinceWindow::DEFAULT
            )
            .iter()
            .all(|line| !line.starts_with("Resilience:"))
        );
    }

    /// The #485 Phase-0 prefetch section renders when the daemon reports
    /// activity and stays absent for a quiet/offline daemon, so local-only
    /// `kache stats` output is unchanged.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn render_stats_prefetch_section_gated_on_activity() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
        // Quiet daemon: no prefetch lines at all.
        let mut quiet = StatsSnapshot::default();
        quiet.daemon_connected = true;
        let out = render_stats(&quiet, &config, SinceWindow::DEFAULT).join("\n");
        assert!(!out.contains("Prefetch:"));
        assert!(!out.contains("Planning:"));

        // Active daemon: all lines present with the right arithmetic.
        let mut snap = StatsSnapshot::default();
        snap.daemon_connected = true;
        snap.prefetch = crate::daemon::PrefetchStatsSnapshot {
            downloads_completed: 4,
            bytes_downloaded: 2048,
            keys_used: 2,
            keys_cancelled: 3,
            keys_over_budget: 0,
            cancelled: true,
            plans_advisory: 1,
            plans_fallback: 2,
            last_plan_candidates: 7,
            dedup_join_waits: 5,
            dedup_join_wait_ms: 1234,
            last_list_duration_ms: 88,
            last_list_key_count: 250_000,
            list_requests_total: 0,
            list_failures_total: 0,
            list_duration_ms_total: 0,
            list_keys_total: 0,
            pack_requests_total: 3,
            pack_bytes_downloaded: 4096,
            v3_requests_total: 1,
            v3_bytes_downloaded: 1024,
            pack_validation_failures: 1,
            pack_fallback_entries: 1,
            last_plan_wall_ms: 250,
            plan_wall_ms_total: 500,
        };
        let mut eff = effective_config_like(&config);
        eff.remote_key_cache_refresh_secs = 7;
        snap.daemon_effective_config = Some(eff);
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("Prefetch:   4 downloads"));
        assert!(out.contains("2 used (50%)"));
        assert!(out.contains("CANCELLED"));
        assert!(out.contains("Planning:   1 advisory / 2 fallback plans (last: 7 candidates)"));
        assert!(out.contains("Transport:  pack 3 requests"));
        assert!(out.contains("v3 1 requests"));
        assert!(out.contains("Plan wall:  250 ms last / 500 ms total"));
        assert!(out.contains("Key LIST:   250000 keys in 88 ms (refreshes every 7s)"));
        assert!(!out.contains("daemon did not report its cadence"));
        assert!(out.contains("Join-wait:  5 waits, 1234 ms total"));

        let mut initial_only = config.clone();
        initial_only.remote_key_cache_refresh_secs = 0;
        let mut eff = effective_config_like(&initial_only);
        eff.remote_key_cache_refresh_secs = 0;
        snap.daemon_effective_config = Some(eff);
        let out = render_stats(&snap, &initial_only, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains(
            "Key LIST:   250000 keys in 88 ms (one initial population; periodic refresh disabled)"
        ));

        let mut disabled = config.clone();
        disabled.prefetch_enabled = false;
        let out = render_stats(&quiet, &disabled, SinceWindow::DEFAULT).join("\n");
        assert!(
            out.contains("Prefetch:   disabled (exact remote lookup and uploads remain enabled)")
        );
        assert!(!out.contains("Planning:"));
        assert!(!out.contains("Key LIST:"));

        snap.prefetch.last_list_key_count = 0;
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("Prefetch:   4 downloads"));
        assert!(
            !out.contains("Key LIST:"),
            "zero listed keys must not render a LIST status line: {out}"
        );
        snap.prefetch.last_list_key_count = 250_000;

        // Transport activity is the sum of two independent counters.  Neither
        // a zero total nor a zero wall clock sample should render a line.
        snap.prefetch.pack_requests_total = 0;
        snap.prefetch.v3_requests_total = 0;
        snap.prefetch.last_plan_wall_ms = 0;
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(!out.contains("Transport:"));
        assert!(!out.contains("Plan wall:"));

        snap.prefetch.pack_requests_total = 1;
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("Transport:  pack 1 requests"));
        snap.prefetch.pack_requests_total = 0;
        snap.prefetch.v3_requests_total = 1;
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("v3 1 requests"));
    }

    /// A daemon-shaped [`crate::daemon::EffectiveConfig`] mirroring `config`,
    /// with a distinct config path so tests can tell daemon-side rendering
    /// from client-side rendering.
    fn effective_config_like(config: &Config) -> crate::daemon::EffectiveConfig {
        crate::daemon::EffectiveConfig {
            max_size: config.max_size,
            cache_dir: config.cache_dir.display().to_string(),
            runtime_dir: config.runtime_dir.display().to_string(),
            config_path: "/daemon-home/.config/kache/config.toml".to_string(),
            config_fingerprint: Some("daemon-fingerprint".to_string()),
            prefetch_enabled: config.prefetch_enabled,
            remote_description: config.remote.as_ref().map(|remote| remote.describe()),
            local_only: config.local_only,
            remote_error: config.remote_error.clone(),
            remote_key_cache_refresh_secs: config.remote_key_cache_refresh_secs,
            socket_path: config.socket_path().display().to_string(),
            started_at_ms: 1_700_000_000_000,
        }
    }

    /// #652/#689: the prefetch policy line renders the DAEMON's effective
    /// policy. A client config saying "disabled" must not produce a disabled
    /// line while the daemon reports prefetch enabled — and the daemon's
    /// config path is surfaced on the Daemon line.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn render_stats_prefetch_policy_prefers_daemon_effective() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
        config.prefetch_enabled = false; // client says disabled…

        let mut snap = StatsSnapshot::default();
        snap.daemon_connected = true;
        let mut eff = effective_config_like(&config);
        eff.prefetch_enabled = true; // …but the daemon is still planning
        snap.daemon_effective_config = Some(eff);

        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(
            !out.contains("Prefetch:   disabled"),
            "must not claim disabled while the daemon reports enabled: {out}"
        );
        assert!(out.contains("config /daemon-home/.config/kache/config.toml"));

        // And the inverse: the daemon reports disabled, so the line shows it
        // even though this process's config says enabled — unlabeled, because
        // it is a daemon fact.
        config.prefetch_enabled = true;
        let mut eff = effective_config_like(&config);
        eff.prefetch_enabled = false;
        snap.daemon_effective_config = Some(eff);
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(
            out.contains("Prefetch:   disabled (exact remote lookup and uploads remain enabled)")
        );
        assert!(!out.contains("client config"));
    }

    /// Old-daemon fallback (#689): a daemon that predates effective-config
    /// reporting leaves `daemon_effective_config` empty, so the policy line
    /// falls back to this process's config — labeled as such, because it can
    /// disagree with what the daemon is actually doing.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn render_stats_prefetch_policy_labels_client_fallback_for_old_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().join("cache"), Some(test_remote_cfg()));
        config.prefetch_enabled = false;

        let mut snap = StatsSnapshot::default();
        snap.daemon_connected = true; // reachable, but reported no config

        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains(
            "Prefetch:   disabled (exact remote lookup and uploads remain enabled) \
             [client config — daemon did not report its policy]"
        ));
        assert!(
            !out.contains(", config "),
            "no daemon config path to show without a report: {out}"
        );
    }

    /// #689: each rendered field that differs between the daemon's effective
    /// config and this process's resolved config produces one warning naming
    /// both values and both sources; agreement produces none.
    #[test]
    fn config_mismatch_warnings_name_both_sides_per_field() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let same = crate::config::ConfigFileProvenance {
            path: "/daemon-home/.config/kache/config.toml".into(),
            fingerprint: "daemon-fingerprint".to_string(),
        };
        let other_path = crate::config::ConfigFileProvenance {
            path: "/cli-home/kache-repro.toml".into(),
            fingerprint: "client-fingerprint".to_string(),
        };

        // Full agreement is silent.
        let eff = effective_config_like(&config);
        assert!(config_mismatch_warnings(&config, &same, &eff).is_empty());

        // LIST cadence is irrelevant when neither side has a remote. A
        // stale/default cadence alone must not produce a mismatch warning.
        let mut cadence_only = eff.clone();
        cadence_only.remote_key_cache_refresh_secs += 1;
        assert!(config_mismatch_warnings(&config, &same, &cadence_only).is_empty());

        // Different config provenance is visible even while selected values
        // happen to match: edits to one file will not reach the other process.
        let warnings = config_mismatch_warnings(&config, &other_path, &eff);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("daemon loaded config"), "{warnings:?}");
        assert!(warnings[0].contains("values may diverge"), "{warnings:?}");

        // The path can stay fixed while its contents change between process
        // starts. Exact loaded fingerprints keep that mismatch visible.
        let changed = crate::config::ConfigFileProvenance {
            path: same.path.clone(),
            fingerprint: "changed-fingerprint".to_string(),
        };
        let warnings = config_mismatch_warnings(&config, &changed, &eff);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("different snapshots"), "{warnings:?}");

        // An intermediate/older daemon that reports effective values but no
        // fingerprint must not create a false mismatch warning.
        let mut old_eff = eff.clone();
        old_eff.config_fingerprint = None;
        assert!(config_mismatch_warnings(&config, &same, &old_eff).is_empty());

        // Runtime placement is daemon-owned too, but an older daemon that
        // cannot report it must not create a false mismatch.
        let mut runtime_eff = effective_config_like(&config);
        runtime_eff.runtime_dir = "/somewhere/runtime".to_string();
        let warnings = config_mismatch_warnings(&config, &same, &runtime_eff);
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("runtime_dir=/somewhere/runtime"));
        runtime_eff.runtime_dir.clear();
        assert!(config_mismatch_warnings(&config, &same, &runtime_eff).is_empty());

        // Store cap differs.
        let mut eff = effective_config_like(&config);
        eff.max_size = config.max_size * 2;
        let warnings = config_mismatch_warnings(&config, &same, &eff);
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0].contains("local_max_size=2.0 MiB"),
            "{warnings:?}"
        );
        assert!(warnings[0].contains("says 1.0 MiB"), "{warnings:?}");
        assert!(
            warnings[0].contains("daemon (started 2023-11-14 22:13 UTC, config /daemon-home/.config/kache/config.toml)"),
            "{warnings:?}"
        );
        assert!(
            warnings[0].contains("this process's config (/daemon-home/.config/kache/config.toml)"),
            "{warnings:?}"
        );
        assert!(
            warnings[0].contains("the daemon's value is in effect"),
            "{warnings:?}"
        );

        // Every rendered field differs -> one warning per field, and the
        // store divergence calls out that the numbers describe another store.
        let mut eff = effective_config_like(&config);
        eff.max_size += 1;
        eff.cache_dir = "/somewhere/else".to_string();
        eff.prefetch_enabled = !config.prefetch_enabled;
        eff.remote_description = Some("s3://daemon-bucket/artifacts".to_string());
        eff.remote_key_cache_refresh_secs += 1;
        eff.started_at_ms = 0; // old field default must not claim 1970
        let warnings = config_mismatch_warnings(&config, &same, &eff);
        assert_eq!(warnings.len(), 5);
        assert!(
            warnings[1].contains("local_store=/somewhere/else"),
            "{warnings:?}"
        );
        assert!(
            warnings[1].contains("the daemon's numbers describe ITS store"),
            "{warnings:?}"
        );
        assert!(
            warnings[2].contains("prefetch_enabled=false"),
            "{warnings:?}"
        );
        assert!(warnings[3].contains("remote=s3://daemon-bucket/artifacts"));
        assert!(warnings[4].contains("remote_key_cache_refresh_secs=61"));
        assert!(warnings[0].contains("started unknown time"), "{warnings:?}");
    }

    /// #689: remote state and LIST cadence are daemon-owned. Both mismatch
    /// directions render the daemon's truth, never the invoking shell's.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn render_stats_remote_state_prefers_daemon_effective() {
        let dir = tempfile::tempdir().unwrap();
        let client_remote =
            save_manifest_config(dir.path().join("client-remote"), Some(test_remote_cfg()));
        let mut snap = StatsSnapshot::default();
        snap.daemon_connected = true;
        let mut eff = effective_config_like(&client_remote);
        eff.remote_description = None;
        eff.remote_key_cache_refresh_secs = 7;
        snap.daemon_effective_config = Some(eff);
        let out = render_stats(&snap, &client_remote, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("Remote:     not configured"), "{out}");
        assert!(!out.contains("Remote:     s3://"), "{out}");

        let client_local = save_manifest_config(dir.path().join("client-local"), None);
        let mut eff = effective_config_like(&client_local);
        eff.remote_description = Some("s3://daemon-bucket/artifacts".to_string());
        snap.daemon_effective_config = Some(eff);
        let out = render_stats(&snap, &client_local, SinceWindow::DEFAULT).join("\n");
        assert!(
            out.contains("Remote:     s3://daemon-bucket/artifacts"),
            "{out}"
        );
        assert!(!out.contains("client config"), "{out}");
    }

    #[test]
    #[allow(clippy::field_reassign_with_default)] // incremental snapshot setup reads clearer
    fn render_stats_daemon_mismatch_and_local_only() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().join("cache"), None);
        config.local_only = true;

        let mut snap = StatsSnapshot::default();
        snap.daemon_connected = true;
        snap.daemon_version = "1.0.0".to_string();
        snap.daemon_build_epoch = crate::daemon::build_epoch().wrapping_add(1); // mismatch
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("MISMATCH — auto-restart pending"));
        assert!(out.contains("local-only mode"));
    }

    #[test]
    fn render_stats_offline_and_not_configured() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let snap = StatsSnapshot::default(); // daemon_connected=false
        let out = render_stats(&snap, &config, SinceWindow::DEFAULT).join("\n");
        assert!(out.contains("Daemon:     offline"));
        assert!(out.contains("Remote:     not configured"));
        // No blobs -> no Dedup line.
        assert!(!out.contains("Dedup:"));
    }

    #[test]
    fn render_stats_handles_zero_limits_and_zero_logical_dedup() {
        // Zero max/logical sizes -> percentage branches stay finite.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let snap = StatsSnapshot {
            total_size: 500,
            max_size: 0,
            blob_stats: Some(crate::store::BlobStats {
                total_blobs: 2,
                total_blob_size: 500,
                total_logical_size: 0,
                savings: 0,
            }),
            ..Default::default()
        };

        let window = SinceWindow::parse("15m").unwrap();
        let out = render_stats(&snap, &config, window).join("\n");
        assert!(out.contains("Store:      500 B / 0 B (0 entries, 0%)"));
        assert!(out.contains("Dedup:      2 unique blobs, 500 B physical, 0.0% savings"));
        // #897: the label names the requested window, not a hardcoded 24h.
        assert!(
            out.contains("Time saved: n/a (estimated compile work avoided, last 15m)"),
            "{out}"
        );

        let no_blobs = StatsSnapshot {
            blob_stats: Some(crate::store::BlobStats {
                total_blobs: 0,
                total_blob_size: 0,
                total_logical_size: 0,
                savings: 0,
            }),
            ..Default::default()
        };
        let out = render_stats(&no_blobs, &config, window).join("\n");
        assert!(
            !out.contains("Dedup:"),
            "an empty blob snapshot must not render a dedup line: {out}"
        );
    }

    #[test]
    fn snapshot_from_direct_reads_returns_only_the_newest_five_summaries() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let summary_path = config.summary_log_path();
        std::fs::create_dir_all(summary_path.parent().unwrap()).unwrap();
        let mut summaries = (0..7)
            .map(|index| {
                format!(
                    "{{\"ts\":\"2026-08-09T00:00:0{index}Z\",\"schema\":1,\"session_id\":\"s{index}\"}}"
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        summaries.push('\n');
        std::fs::write(summary_path, summaries).unwrap();

        let snap = snapshot_from_direct_reads(&config, false, "size", SinceWindow::DEFAULT, true);
        let ids = snap
            .recent_summaries
            .iter()
            .map(|summary| summary.session_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["s2", "s3", "s4", "s5", "s6"]);
    }

    #[test]
    fn snapshot_from_direct_reads_reflects_store_and_events() {
        // No daemon: the snapshot is built from direct store + event-log reads.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "serdekey", "serde", dir.path());
        // Log a couple of events so event_stats is populated.
        crate::events::log_event(
            &config.event_log_path(),
            &build_event(
                "serde",
                crate::events::EventResult::LocalHit,
                0,
                5,
                4096,
                "serdekey",
            ),
        )
        .unwrap();
        crate::events::log_event(
            &config.event_log_path(),
            &build_event(
                "tokio",
                crate::events::EventResult::Miss,
                900,
                950,
                8192,
                "tk",
            ),
        )
        .unwrap();

        let snap = snapshot_from_direct_reads(&config, true, "name", SinceWindow::DEFAULT, false);
        assert!(!snap.daemon_connected, "direct reads report no daemon");
        assert_eq!(snap.entry_count, 1);
        assert_eq!(snap.entries.len(), 1);
        assert_eq!(snap.entries[0].crate_name, "serde");
        assert_eq!(snap.event_stats.local_hits, 1);
        assert_eq!(snap.event_stats.misses, 1);
        assert_eq!(snap.max_size, config.max_size);
    }

    /// #897: `--since` must narrow the counters, not just the label. A 15m
    /// window excludes the three-hour-old miss that a 24h window includes.
    #[test]
    fn snapshot_from_direct_reads_honors_a_sub_hour_window() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let now = chrono::Utc::now();
        let mut recent = build_event(
            "serde",
            crate::events::EventResult::LocalHit,
            0,
            5,
            4096,
            "a",
        );
        recent.ts = now - chrono::Duration::minutes(10);
        let mut old = build_event(
            "tokio",
            crate::events::EventResult::Miss,
            900,
            950,
            8192,
            "b",
        );
        old.ts = now - chrono::Duration::hours(3);
        crate::events::log_event(&config.event_log_path(), &recent).unwrap();
        crate::events::log_event(&config.event_log_path(), &old).unwrap();

        let narrow = SinceWindow::parse("15m").unwrap();
        let snap = snapshot_from_direct_reads(&config, false, "size", narrow, false);
        assert_eq!(snap.event_stats.local_hits, 1);
        assert_eq!(snap.event_stats.misses, 0, "a 3h-old miss is outside 15m");

        let wide = SinceWindow::parse("24h").unwrap();
        let snap = snapshot_from_direct_reads(&config, false, "size", wide, false);
        assert_eq!(snap.event_stats.local_hits, 1);
        assert_eq!(snap.event_stats.misses, 1);
    }

    #[test]
    fn snapshot_from_direct_reads_without_entries_skips_listing() {
        // include_entries=false -> entries list is empty even with a populated store.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "k1", "serde", dir.path());
        let snap = snapshot_from_direct_reads(&config, false, "size", SinceWindow::DEFAULT, false);
        assert!(
            snap.entries.is_empty(),
            "entries omitted when not requested"
        );
        assert_eq!(snap.entry_count, 1, "count still reflects the store");
    }

    #[test]
    fn sync_without_remote_errors() {
        // The sync entry point bails before building a runtime/client when no
        // remote is configured. Covers sync()'s no-remote guard.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let err = sync(&config, None, false, false, false, false, false, false)
            .expect_err("sync without a remote must error");
        assert!(
            err.to_string().contains("No remote configured"),
            "got: {err}"
        );
    }

    #[test]
    fn repair_tip_and_reconciliation_message_cover_every_boundary() {
        assert!(!should_print_repair_tip(0, 0, 0, false));
        assert!(should_print_repair_tip(1, 0, 0, false));
        assert!(should_print_repair_tip(0, 1, 0, false));
        assert!(should_print_repair_tip(0, 0, 1, false));
        assert!(!should_print_repair_tip(1, 1, 1, true));

        assert_eq!(
            reconciled_index_message(crate::store::BlobIndexDrift::default()),
            None
        );
        assert_eq!(
            reconciled_index_message(crate::store::BlobIndexDrift {
                entry_mappings: 1,
                blobs: 2,
            })
            .as_deref(),
            Some("Repairing: reconciled 1 entry mappings and 2 blob rows.")
        );
    }

    #[test]
    fn verify_detects_missing_blob_and_missing_meta_then_repairs() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let store = Store::open(&config).unwrap();

        // Entry A: blob deleted -> "missing blob" path.
        put_entry(&config, "missingblobkey", "aaa", dir.path());
        let meta_a = store.get("missingblobkey").unwrap().unwrap();
        let blob_a = store.blob_path(&meta_a.files[0].hash);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                blob_a.parent().unwrap(),
                std::fs::Permissions::from_mode(0o755),
            );
        }
        let _ = std::fs::remove_file(&blob_a);

        // Entry B: meta.json deleted -> "missing meta" path.
        put_entry(&config, "missingmetakey", "bbb", dir.path());
        std::fs::remove_file(config.store_dir().join("missingmetakey").join("meta.json")).unwrap();

        // Entry C: meta.json corrupted -> "invalid meta" path.
        put_entry(&config, "badmetakey", "ccc", dir.path());
        std::fs::write(
            config.store_dir().join("badmetakey").join("meta.json"),
            b"{ not valid json",
        )
        .unwrap();

        // Entry D: a clean valid entry.
        put_entry(&config, "validkey", "ddd", dir.path());

        // repair=true attempts to remove every corrupted entry; the call must
        // succeed regardless of whether each removal is permitted.
        verify(&config, false, true).expect("verify --repair should succeed");

        // The valid entry survives. The missing-blob entry has a *parseable*
        // meta, so repair can (and does) remove it. The missing-meta and
        // corrupt-meta entries are deliberately NOT removed (#276: refusing to
        // orphan blob refcounts), so we don't assert their removal — running
        // verify again must still succeed over the remaining corrupt entries.
        let store2 = Store::open(&config).unwrap();
        assert!(store2.get("validkey").unwrap().is_some());
        assert!(store2.get("missingblobkey").unwrap().is_none());
        verify(&config, false, false).expect("a second verify pass should succeed");
    }

    #[test]
    fn verify_reports_and_repairs_blob_index_drift() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "driftkey", "drift", dir.path());
        let store = Store::open(&config).unwrap();
        let hash = store.get("driftkey").unwrap().unwrap().files[0]
            .hash
            .clone();
        drop(store);

        let db = crate::store::open_index_db(&config.index_db_path()).unwrap();
        db.execute(
            "UPDATE blobs SET refcount = 77 WHERE hash = ?1",
            rusqlite::params![hash],
        )
        .unwrap();
        drop(db);

        let drifted = verify(&config, false, false).unwrap();
        assert_eq!(drifted.index_drift, 1, "{drifted:?}");
        assert_eq!(drifted.unresolved_integrity_findings(), 1, "{drifted:?}");

        let repaired = verify(&config, false, true).unwrap();
        assert_eq!(repaired.index_drift, 0, "{repaired:?}");
        assert_eq!(repaired.unresolved_integrity_findings(), 0, "{repaired:?}");
        let clean = verify(&config, false, false).unwrap();
        assert_eq!(clean.index_drift, 0, "{clean:?}");
        assert_eq!(clean.unresolved_integrity_findings(), 0, "{clean:?}");
    }

    #[test]
    fn verify_detects_checksum_mismatch_with_checksums_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let store = Store::open(&config).unwrap();
        put_entry(&config, "corruptblobkey", "eee", dir.path());

        // Corrupt the blob in place, same size so only the checksum differs.
        let meta = store.get("corruptblobkey").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        #[cfg(not(unix))]
        {
            let mut p = std::fs::metadata(&blob).unwrap().permissions();
            p.set_readonly(false);
            std::fs::set_permissions(&blob, p).unwrap();
        }
        std::fs::write(&blob, vec![b'X'; meta.files[0].size as usize]).unwrap();

        // checksums=true detects the content mismatch and reports it as an
        // unresolved integrity finding (kunobi-ninja/kache#176).
        let outcome = verify(&config, true, false).expect("verify with checksums should succeed");
        assert_eq!(outcome.checksum_failures, 1, "{outcome:?}");
        assert_eq!(outcome.corrupted_entries, 1, "{outcome:?}");
        assert_eq!(
            outcome.unresolved_integrity_findings(),
            1,
            "without --repair the finding stands: {outcome:?}"
        );

        // --repair removes the entry, so nothing is left unresolved and a CI
        // run gated on this exits zero.
        let repaired = verify(&config, true, true).expect("verify --repair should succeed");
        assert_eq!(repaired.corrupted_removed, repaired.corrupted_entries);
        assert_eq!(repaired.unresolved_integrity_findings(), 0, "{repaired:?}");

        // And the store is clean afterwards.
        let clean = verify(&config, true, false).expect("verify after repair should succeed");
        assert_eq!(clean.corrupted_entries, 0, "{clean:?}");
        assert_eq!(clean.unresolved_integrity_findings(), 0, "{clean:?}");
    }

    /// kunobi-ninja/kache#176: the scrub hashes each unique blob ONCE, however
    /// many entries share it — the dedup that makes the store cheap must not
    /// make verification quadratic. Two entries sharing one corrupt blob
    /// produce ONE checksum failure while marking BOTH entries corrupt.
    #[test]
    fn verify_scrubs_each_unique_blob_once_and_marks_every_referencing_entry() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let store = Store::open(&config).unwrap();

        // Two entries, identical content → one shared blob. Each put gets
        // its OWN source path: on Linux the store hardlinks the source into
        // the read-only blob store, so reusing one path would make the
        // second write fail with EACCES (macOS reflinks, and would not).
        for key in ["sharedkey_a", "sharedkey_b"] {
            let src = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&src, b"shared artifact bytes").unwrap();
            store
                .put(
                    key,
                    "shared",
                    &["lib".to_string()],
                    &[],
                    "",
                    "dev",
                    &[(src, "lib.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
        }
        let meta = store.get("sharedkey_a").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        assert_eq!(
            store.get("sharedkey_b").unwrap().unwrap().files[0].hash,
            meta.files[0].hash,
            "the two entries must share one blob for this test to mean anything"
        );

        let mut perms = std::fs::metadata(&blob).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o644);
        }
        #[cfg(not(unix))]
        perms.set_readonly(false);
        std::fs::set_permissions(&blob, perms).unwrap();
        std::fs::write(&blob, vec![b'X'; meta.files[0].size as usize]).unwrap();

        // Without --checksums the scrub does not run, so silent corruption
        // is invisible: that is exactly what the flag buys, and asserting it
        // pins the flag's meaning.
        let unchecked = verify(&config, false, false).expect("verify should succeed");
        assert_eq!(
            unchecked.checksum_failures, 0,
            "checksums=false must not hash anything: {unchecked:?}"
        );
        assert_eq!(
            unchecked.unresolved_integrity_findings(),
            0,
            "same-size corruption is undetectable without --checksums: {unchecked:?}"
        );

        let outcome = verify(&config, true, false).expect("verify should succeed");
        assert_eq!(
            outcome.checksum_failures, 1,
            "one shared blob is one failure, not one per referencing entry: {outcome:?}"
        );
        assert_eq!(
            outcome.corrupted_entries, 2,
            "but both entries referencing it are corrupt: {outcome:?}"
        );
        assert_eq!(outcome.unresolved_integrity_findings(), 2, "{outcome:?}");
    }

    /// A healthy store reports nothing unresolved, so a CI gate on
    /// `doctor --verify` stays green (kunobi-ninja/kache#176).
    #[test]
    fn verify_reports_no_findings_for_a_clean_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        put_entry(&config, "cleanstorekey", "ggg", dir.path());

        let outcome = verify(&config, true, false).expect("verify should succeed");
        assert_eq!(outcome.corrupted_entries, 0, "{outcome:?}");
        assert_eq!(outcome.checksum_failures, 0, "{outcome:?}");
        assert_eq!(outcome.missing_blobs, 0, "{outcome:?}");
        assert_eq!(outcome.unresolved_integrity_findings(), 0, "{outcome:?}");
        assert!(outcome.valid_entries >= 1, "{outcome:?}");
    }

    #[test]
    fn verify_detects_blob_size_mismatch() {
        // Blob metadata length mismatch -> entry is marked corrupt.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().join("cache"), None);
        let store = Store::open(&config).unwrap();
        put_entry(&config, "sizemismatchkey", "fff", dir.path());
        let meta = store.get("sizemismatchkey").unwrap().unwrap();
        let blob = store.blob_path(&meta.files[0].hash);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o644)).unwrap();
        }
        #[cfg(not(unix))]
        {
            let mut p = std::fs::metadata(&blob).unwrap().permissions();
            p.set_readonly(false);
            std::fs::set_permissions(&blob, p).unwrap();
        }
        std::fs::write(&blob, vec![b'Y'; meta.files[0].size as usize + 1]).unwrap();

        verify(&config, false, false).expect("verify with size mismatch should succeed");
    }

    #[test]
    fn save_manifest_without_remote_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), None);
        let err = save_manifest(&config, None, None).expect_err("no remote -> error");
        assert!(
            err.to_string().contains("No remote configured"),
            "got {err}"
        );
    }

    #[test]
    fn automatic_manifest_save_without_remote_errors() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), None);
        let err = save_manifest_auto_for_session(&config, "key", "session")
            .expect_err("no remote -> error");
        assert!(
            err.to_string().contains("No remote configured"),
            "got {err}"
        );
    }

    #[test]
    fn only_the_primary_manifest_key_publishes_shards() {
        assert_eq!(shard_namespace_for_publish_key(0, Some("ns")), Some("ns"));
        assert_eq!(shard_namespace_for_publish_key(1, Some("ns")), None);
        assert_eq!(shard_namespace_for_publish_key(0, None), None);
    }

    #[test]
    fn save_manifest_with_no_events_returns_ok_before_touching_remote() {
        // A remote is configured, but the event log is empty, so save_manifest
        // returns Ok early ("No build events found") without creating a remote
        // client or making any network call.
        let dir = tempfile::tempdir().unwrap();
        let remote = crate::config::RemoteConfig::test_s3("b", "p");
        let config = save_manifest_config(dir.path().to_path_buf(), Some(remote));
        // No event log written -> read_events yields empty -> early Ok.
        save_manifest(&config, Some("mykey"), None).expect("empty events -> Ok");
    }

    fn build_event(
        crate_name: &str,
        result: crate::events::EventResult,
        compile_time_ms: u64,
        elapsed_ms: u64,
        size: u64,
        cache_key: &str,
    ) -> crate::events::BuildEvent {
        crate::events::BuildEvent {
            ts: chrono::Utc::now(),
            session_id: String::new(),
            crate_name: crate_name.to_string(),
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
            startup_ms: 0,
            dep_info_ms: 0,
            dep_info_runs: 0,
            flight_wait_ms: 0,
            permit_wait_ms: 0,
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
            store_hardlinked_bytes: 0,
            store_copied_bytes: 0,
            root: String::new(),
            passthrough_reason: String::new(),
            store_error: String::new(),
            lookup_rejection: String::new(),
            verify_compare: String::new(),
            fallback: false,
            exit_code: None,
            key_fields: Default::default(),
            key_diff: Vec::new(),
            key_externs: Default::default(),
            key_externs_recorded: false,
            unit_id: String::new(),
            extern_units: Default::default(),
        }
    }

    fn diff_meta(
        target: &str,
        profile: &str,
        features: &[&str],
        crate_types: &[&str],
    ) -> crate::store::EntryMeta {
        crate::store::EntryMeta {
            cache_key: "k".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "c".to_string(),
            crate_types: crate_types.iter().map(|v| (*v).to_string()).collect(),
            files: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            features: features.iter().map(|v| (*v).to_string()).collect(),
            target: target.to_string(),
            profile: profile.to_string(),
            compile_time_ms: 0,
            emit_kinds: Vec::new(),
        }
    }

    #[test]
    fn manifest_entries_from_events_dedups_and_filters() {
        use crate::events::EventResult;
        let events = vec![
            // Same key twice: the larger compile time wins.
            build_event("serde", EventResult::Miss, 100, 0, 10, "k-serde"),
            build_event("serde", EventResult::LocalHit, 900, 0, 10, "k-serde"),
            // A distinct cacheable entry.
            build_event("tokio", EventResult::Dup, 50, 0, 20, "k-tokio"),
            // Ignored: empty cache_key.
            build_event("nokey", EventResult::Miss, 5, 0, 0, ""),
            // Ignored: non-cacheable outcomes.
            build_event("passth", EventResult::Passthrough, 5, 0, 0, "k-p"),
            build_event("skip", EventResult::Skipped, 5, 0, 0, "k-s"),
        ];

        let mut entries = manifest_entries_from_events(&events, None);
        entries.sort_by(|a, b| a.crate_name.cmp(&b.crate_name));

        assert_eq!(entries.len(), 2, "only the two cacheable keys survive");
        let serde = entries.iter().find(|e| e.crate_name == "serde").unwrap();
        assert_eq!(serde.compile_time_ms, 900, "larger compile time wins");
        assert!(entries.iter().any(|e| e.crate_name == "tokio"));
    }

    #[test]
    fn manifest_entry_ties_keep_the_first_observation() {
        use crate::events::EventResult;
        let events = vec![
            build_event("first", EventResult::Miss, 100, 0, 10, "same-key"),
            build_event("second", EventResult::Miss, 100, 0, 20, "same-key"),
        ];

        let entries = manifest_entries_from_events(&events, None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].crate_name, "first");
        assert_eq!(entries[0].artifact_size, 10);
    }

    #[test]
    fn automatic_manifest_entries_are_scoped_to_the_build_session() {
        use crate::events::EventResult;
        let mut current = build_event("current", EventResult::Miss, 100, 0, 10, "current-key");
        current.session_id = "current-session".into();
        let mut other = build_event("other", EventResult::Miss, 200, 0, 20, "other-key");
        other.session_id = "other-session".into();

        let entries = manifest_entries_from_events(&[other, current], Some("current-session"));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].crate_name, "current");
        assert_eq!(entries[0].cache_key, "current-key");
    }

    #[test]
    fn manifest_entries_from_events_falls_back_to_elapsed_when_no_compile_time() {
        use crate::events::EventResult;
        // compile_time_ms == 0 -> the entry's compile_time_ms uses elapsed_ms.
        let events = vec![build_event("x", EventResult::Miss, 0, 77, 1, "k")];
        let entries = manifest_entries_from_events(&events, None);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].compile_time_ms, 77);
    }

    #[test]
    fn manifest_entries_from_events_accepts_remote_and_prefetch_hits() {
        use crate::events::EventResult;
        // Prefetch/remote hits are cacheable manifest inputs.
        let events = vec![
            build_event(
                "prefetch",
                EventResult::PrefetchHit,
                12,
                3,
                10,
                "k-prefetch",
            ),
            build_event("remote", EventResult::RemoteHit, 34, 5, 20, "k-remote"),
            build_event("error", EventResult::Error, 99, 99, 99, "k-error"),
        ];

        let mut entries = manifest_entries_from_events(&events, None);
        entries.sort_by(|a, b| a.crate_name.cmp(&b.crate_name));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].crate_name, "prefetch");
        assert_eq!(entries[0].artifact_size, 10);
        assert_eq!(entries[1].crate_name, "remote");
        assert_eq!(entries[1].compile_time_ms, 34);
    }

    fn test_remote_cfg() -> crate::config::RemoteConfig {
        crate::config::RemoteConfig::test_s3("bucket", "prefix")
    }

    #[tokio::test]
    async fn upload_manifest_and_shards_uploads_manifest_only_without_namespace() {
        // No namespace -> exactly one object (the monolithic manifest).
        let backend = TestBackend::memory();
        let client = as_remote_backend(&backend);
        let remote = test_remote_cfg();
        let entries = vec![crate::remote::ManifestEntry {
            cache_key: "k".to_string(),
            crate_name: "c".to_string(),
            compile_time_ms: 1,
            artifact_size: 1,
        }];
        upload_manifest_and_shards(
            &client,
            &remote,
            "mykey",
            None,
            std::path::Path::new("/nonexistent/Cargo.lock"),
            entries,
        )
        .await
        .expect("manifest-only upload should succeed");
        assert_eq!(backend.put_calls(), vec!["prefix/_manifests/mykey.json"]);
    }

    #[tokio::test]
    async fn upload_manifest_and_shards_skips_shards_when_lock_missing() {
        // Namespace given but Cargo.lock absent -> still only the manifest.
        let backend = TestBackend::memory();
        let client = as_remote_backend(&backend);
        let remote = test_remote_cfg();
        let entries = vec![crate::remote::ManifestEntry {
            cache_key: "k".to_string(),
            crate_name: "c".to_string(),
            compile_time_ms: 1,
            artifact_size: 1,
        }];
        upload_manifest_and_shards(
            &client,
            &remote,
            "mykey",
            Some("ns"),
            std::path::Path::new("/nonexistent/Cargo.lock"),
            entries,
        )
        .await
        .expect("upload should succeed, shards skipped");
        assert_eq!(backend.put_calls(), vec!["prefix/_manifests/mykey.json"]);
    }

    #[tokio::test]
    async fn upload_manifest_and_shards_uploads_shards_when_lock_present() {
        // Namespace + a real Cargo.lock with deps that match the entries -> the
        // manifest PUT plus one PUT per non-empty shard.
        let dir = tempfile::tempdir().unwrap();
        let lock = dir.path().join("Cargo.lock");
        std::fs::write(
            &lock,
            "version = 3\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.0\"\n",
        )
        .unwrap();
        let entries = vec![crate::remote::ManifestEntry {
            cache_key: "k-serde".to_string(),
            crate_name: "serde".to_string(),
            compile_time_ms: 1,
            artifact_size: 1,
        }];
        let deps = crate::shards::parse_cargo_lock(&lock).unwrap();
        let expected_shards = crate::shards::compute_shards("ns", &deps).shards.len();

        let backend = TestBackend::memory();
        let client = as_remote_backend(&backend);
        let remote = test_remote_cfg();

        upload_manifest_and_shards(&client, &remote, "mykey", Some("ns"), &lock, entries)
            .await
            .expect("upload with shards should succeed");
        let puts = backend.put_calls();
        assert_eq!(puts.len(), expected_shards + 1);
        assert!(puts.contains(&"prefix/_manifests/mykey.json".to_string()));
        assert_eq!(
            puts.iter()
                .filter(|key| key.starts_with("prefix/_manifests/v3/ns/shards/"))
                .count(),
            expected_shards
        );
    }

    fn sync_test_cache_key(seed: &str) -> String {
        blake3::hash(seed.as_bytes()).to_hex().to_string()
    }

    #[tokio::test]
    async fn sync_with_client_dry_run_empty_remote_reports_nothing() {
        // Empty remote + empty local store: the diff is empty and sync reports
        // "Nothing to sync" after one list call.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let backend = TestBackend::memory();
        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            false,
            false,
            true,
            false,
            None,
            false,
            false,
        )
        .await
        .expect("dry-run sync over empty remote should succeed");
        assert_eq!(backend.list_calls(), vec!["prefix/v3/manifests/"]);
    }

    #[tokio::test]
    async fn sync_with_client_workspace_pull_scopes_listing_to_workspace_members() {
        // `--workspace` must scope the pull listing to workspace members and
        // ignore the Cargo.lock dep set.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let workspace: std::collections::HashSet<String> =
            ["wsfoo".to_string()].into_iter().collect();
        let lock: std::collections::HashSet<String> = ["dep_a".to_string(), "dep_b".to_string()]
            .into_iter()
            .collect();

        let backend = TestBackend::memory();
        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            Some(&workspace), // workspace_crates
            true,             // pull_only
            false,            // push_only
            true,             // dry_run
            false,            // pull_all
            Some(&lock),      // lock_crates — must be ignored under --workspace
            true,             // pull_workspace
            false,            // allow_partial
        )
        .await
        .expect("workspace-scoped pull should list only the workspace member(s)");
        assert_eq!(backend.list_calls(), vec!["prefix/v3/manifests/wsfoo/"]);
    }

    #[tokio::test]
    async fn sync_with_client_workspace_pull_errors_when_no_workspace_resolved() {
        // `--workspace` with an unresolved (None) or empty workspace set must
        // error, NOT silently fall back to a full-remote scan.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let empty: std::collections::HashSet<String> = std::collections::HashSet::new();
        let cases: [Option<&std::collections::HashSet<String>>; 2] = [None, Some(&empty)];

        for workspace_crates in cases {
            let backend = TestBackend::memory();
            let err = sync_with_client(
                backend.as_ref(),
                &config,
                &store,
                &remote,
                workspace_crates, // None or empty → cannot scope to workspace
                true,             // pull_only
                false,            // push_only
                true,             // dry_run
                false,            // pull_all
                None,             // lock_crates (always None under --workspace)
                true,             // pull_workspace
                false,            // allow_partial
            )
            .await
            .expect_err("--workspace with no resolved members must error, not scan the bucket");
            assert!(
                err.to_string().contains("no workspace members resolved"),
                "unexpected error: {err}"
            );
            assert!(
                backend.list_calls().is_empty(),
                "guard must run before listing the remote"
            );
        }
    }

    #[tokio::test]
    async fn sync_with_client_push_uploads_local_only_entry() {
        // A populated local store + an empty remote: push-only sync uploads the
        // local entry end-to-end (real pack creation + manifest) through the
        // backend. Exercises the push loop and upload_entry, not just planning.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();

        // Materialize one cache entry with a single artifact file.
        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        let artifact = src_dir.join("libfoo.rlib");
        std::fs::write(&artifact, b"artifact bytes").unwrap();
        store
            .put(
                "pushkey123",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let remote = test_remote_cfg();
        let backend = TestBackend::memory();

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            false,
            true,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect("push sync should succeed");
        let puts = backend.put_calls();
        assert_eq!(puts.len(), 2);
        assert!(puts.contains(&"prefix/v3/packs/foo/pushkey123.tar.zst".to_string()));
        assert!(puts.contains(&"prefix/v3/manifests/foo/pushkey123.json".to_string()));
    }

    #[tokio::test]
    async fn sync_with_client_push_throttles_with_low_concurrency() {
        // Two local entries with concurrency=1 force the push loop's
        // max-concurrency wait branch (the second upload waits for the first).
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        config.s3_concurrency = 1;
        let store = Store::open(&config).unwrap();

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        for (key, cn) in [("pusha1", "aaa"), ("pushb2", "bbb")] {
            let artifact = src_dir.join(format!("{cn}.rlib"));
            std::fs::write(&artifact, format!("{cn} bytes")).unwrap();
            store
                .put(
                    key,
                    cn,
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "debug",
                    &[(artifact, format!("{cn}.rlib"))],
                    "",
                    "",
                )
                .unwrap();
        }

        let remote = test_remote_cfg();
        let backend = TestBackend::memory();

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            false,
            true,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect("throttled push sync should succeed");
        assert_eq!(backend.put_calls().len(), 4);
    }

    #[tokio::test]
    async fn sync_with_client_dry_run_plans_pull_for_remote_only_key() {
        // The remote lists a manifest for a key absent from the local store, so
        // the dry-run plan schedules a pull and returns without transferring.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();
        let key = sync_test_cache_key("dry-run-remote-only");

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/serde/{key}.json"),
                b"{}".to_vec(),
            )
            .await;
        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            false,
            false,
            true,
            false,
            None,
            false,
            false,
        )
        .await
        .expect("dry-run sync planning a pull should succeed");
        assert!(
            backend.get_calls().is_empty(),
            "dry-run must not download the pack"
        );
    }

    #[tokio::test]
    async fn sync_with_client_pull_loop_records_failure_and_returns_err_by_default() {
        // A remote-only key drives a real (non-dry-run) pull. The served pack is
        // garbage, so download_entry errors — the pull loop must record the
        // failure and return Err by default.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();
        let key = sync_test_cache_key("failed-pull");

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/serde/{key}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(
                &format!("prefix/v3/packs/serde/{key}.tar.zst"),
                b"not a valid pack".to_vec(),
            )
            .await;

        let err = sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect_err("pull sync should return Err when a download fails by default");
        assert!(
            err.to_string()
                .contains("1 transfer(s) or import(s) failed")
        );
        assert_eq!(
            backend.get_calls(),
            vec![format!("prefix/v3/packs/serde/{key}.tar.zst")]
        );
    }

    #[tokio::test]
    async fn sync_with_client_pull_allows_partial_when_flag_set() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();
        let key = sync_test_cache_key("failed-pull-partial");

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/serde/{key}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(
                &format!("prefix/v3/packs/serde/{key}.tar.zst"),
                b"not a valid pack".to_vec(),
            )
            .await;

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            true,
        )
        .await
        .expect("pull sync with allow_partial should complete Ok despite failed download");
        assert_eq!(
            backend.get_calls(),
            vec![format!("prefix/v3/packs/serde/{key}.tar.zst")]
        );
    }

    #[tokio::test]
    async fn sync_with_client_push_reports_failed_uploads_and_returns_err_by_default() {
        // A local-only entry is scheduled for push, but the backend rejects
        // uploads, so upload_entry errors and the loop records a failure.
        // Returns Err by default.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        let artifact = src_dir.join("foo.rlib");
        std::fs::write(&artifact, b"foo bytes").unwrap();
        store
            .put(
                "pushfail1aaaa",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, "foo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let backend = TestBackend::failing_put();

        let err = sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            false,
            true,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect_err("push sync should return Err when an upload fails by default");
        assert!(
            err.to_string()
                .contains("1 transfer(s) or import(s) failed")
        );
        assert_eq!(
            backend.put_calls(),
            vec!["prefix/v3/packs/foo/pushfail1aaaa.tar.zst"]
        );
    }

    #[tokio::test]
    async fn sync_with_client_push_allows_partial_when_flag_set() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        let artifact = src_dir.join("foo.rlib");
        std::fs::write(&artifact, b"foo bytes").unwrap();
        store
            .put(
                "pushfail1aaaa",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, "foo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let backend = TestBackend::failing_put();

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            false,
            true,
            false,
            false,
            None,
            false,
            true,
        )
        .await
        .expect("push sync with allow_partial should complete Ok even when an upload fails");
        assert_eq!(
            backend.put_calls(),
            vec!["prefix/v3/packs/foo/pushfail1aaaa.tar.zst"]
        );
    }

    #[tokio::test]
    async fn sync_with_client_pull_throttles_with_low_concurrency() {
        // Two remote-only keys with concurrency=1 force the pull loop's
        // max-concurrency wait branch (the second download waits for the first
        // to drain a slot). Packs are garbage so each download fails fast, but
        // the throttle path is still exercised; with allow_partial, the sync completes Ok.
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        config.s3_concurrency = 1;
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let backend = TestBackend::memory();
        for (crate_name, key) in [
            ("aaa", sync_test_cache_key("throttled-pull-a")),
            ("bbb", sync_test_cache_key("throttled-pull-b")),
        ] {
            backend
                .seed(
                    &format!("prefix/v3/manifests/{crate_name}/{key}.json"),
                    b"{}".to_vec(),
                )
                .await;
            backend
                .seed(
                    &format!("prefix/v3/packs/{crate_name}/{key}.tar.zst"),
                    b"not a pack".to_vec(),
                )
                .await;
        }

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            true,
        )
        .await
        .expect("throttled pull sync should complete Ok with allow_partial");
        assert_eq!(backend.get_calls().len(), 2);
    }

    /// Build a valid v3 entry pack (tar.zst) for `key`/`crate_name` from a
    /// throwaway store, so tests can serve it as a GET body to drive the
    /// download-success path.
    fn build_entry_pack(key: &str, crate_name: &str) -> Vec<u8> {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = save_manifest_config(tmp.path().to_path_buf(), None);
        let store = Store::open(&cfg).unwrap();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let artifact = src.join("libfoo.rlib");
        std::fs::write(&artifact, b"real artifact bytes").unwrap();
        store
            .put(
                key,
                crate_name,
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();
        let entry_dir = store.entry_dir(key);
        let meta: crate::store::EntryMeta =
            serde_json::from_slice(&std::fs::read(entry_dir.join("meta.json")).unwrap()).unwrap();
        crate::remote_layout::create_entry_pack_zstd(&entry_dir, &store.blobs_dir(), &meta, 3)
            .unwrap()
    }

    #[tokio::test]
    async fn sync_with_client_pull_downloads_and_imports_entry() {
        // Remote lists a key absent locally; the GET returns a VALID pack, so
        // the pull downloads, extracts, and imports it into the local store.
        // Covers the pull SUCCESS path (download_entry + import), not just the
        // error path.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let key = sync_test_cache_key("successful-pull");
        let pack = build_entry_pack(&key, "serde");

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/serde/{key}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(&format!("prefix/v3/packs/serde/{key}.tar.zst"), pack)
            .await;

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect("pull sync should succeed");

        // The entry was imported into the local store.
        assert!(
            config.store_dir().join(&key).join("meta.json").exists(),
            "pulled entry should be materialized in the local store"
        );
        assert_eq!(
            backend.get_calls(),
            vec![format!("prefix/v3/packs/serde/{key}.tar.zst")]
        );
    }

    struct DisappearingBackend {
        inner: Arc<TestBackend>,
        on_put_delete: std::path::PathBuf,
    }

    #[async_trait::async_trait]
    impl crate::remote_backend::RemoteBackend for DisappearingBackend {
        async fn head(&self, key: &str) -> Result<bool> {
            self.inner.as_ref().head(key).await
        }
        async fn get(
            &self,
            key: &str,
            max_bytes: Option<u64>,
        ) -> Result<Option<crate::remote_backend::GetObject>> {
            self.inner.as_ref().get(key, max_bytes).await
        }
        async fn put(&self, key: &str, body: Vec<u8>, content_type: Option<&str>) -> Result<()> {
            if self.on_put_delete.exists() {
                let _ = std::fs::remove_dir_all(&self.on_put_delete);
            }
            self.inner.as_ref().put(key, body, content_type).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.as_ref().list(prefix).await
        }
        fn describe(&self, key: &str) -> String {
            self.inner.as_ref().describe(key)
        }
    }

    #[tokio::test]
    async fn sync_with_client_push_fails_when_local_entry_disappears() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        config.s3_concurrency = 1;
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();

        // Materialize two entries ("aaa" sorts before "zzz")
        for (k, cn) in [("keep_first", "aaa"), ("disappear_second", "zzz")] {
            let artifact = src_dir.join(format!("{cn}.rlib"));
            std::fs::write(&artifact, format!("{cn} bytes")).unwrap();
            store
                .put(
                    k,
                    cn,
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "debug",
                    &[(artifact, format!("{cn}.rlib"))],
                    "",
                    "",
                )
                .unwrap();
        }

        let disappear_dir = config.store_dir().join("disappear_second");
        let backend = DisappearingBackend {
            inner: TestBackend::memory(),
            on_put_delete: disappear_dir,
        };

        let err = sync_with_client(
            &backend, &config, &store, &remote, None, false, true, false, false, None, false, false,
        )
        .await
        .expect_err("disappeared local entry must cause non-zero exit by default");
        assert!(
            err.to_string()
                .contains("1 transfer(s) or import(s) failed")
        );
    }

    /// Build a tar.zst pack containing an invalid meta.json to test import failure.
    fn build_invalid_meta_pack() -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        let meta_bytes = b"{\"not\": \"valid meta\"}";
        let mut header = tar::Header::new_gnu();
        header.set_path("meta.json").unwrap();
        header.set_size(meta_bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar_builder.append(&header, &meta_bytes[..]).unwrap();
        let tar_data = tar_builder.into_inner().unwrap();
        zstd::encode_all(&tar_data[..], 3).unwrap()
    }

    #[tokio::test]
    async fn sync_with_client_pull_fails_when_import_fails_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let key = sync_test_cache_key("invalid-meta-pull");
        let pack = build_invalid_meta_pack();

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/foo/{key}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(&format!("prefix/v3/packs/foo/{key}.tar.zst"), pack)
            .await;

        let err = sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect_err("pull sync should return Err when import fails by default");
        assert!(
            err.to_string()
                .contains("1 transfer(s) or import(s) failed")
        );
    }

    #[tokio::test]
    async fn sync_with_client_pull_allows_partial_when_import_fails() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let key = sync_test_cache_key("invalid-meta-pull-allow");
        let pack = build_invalid_meta_pack();

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/foo/{key}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(&format!("prefix/v3/packs/foo/{key}.tar.zst"), pack)
            .await;

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            true,
        )
        .await
        .expect("pull sync with allow_partial should return Ok even when import fails");
    }

    #[tokio::test]
    async fn sync_with_client_mixed_success_and_failure_completes_all_and_fails_default() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let key_good = sync_test_cache_key("mixed-good");
        let key_bad = sync_test_cache_key("mixed-bad");
        let pack_good = build_entry_pack(&key_good, "serde");

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/serde/{key_good}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(
                &format!("prefix/v3/packs/serde/{key_good}.tar.zst"),
                pack_good,
            )
            .await;

        backend
            .seed(
                &format!("prefix/v3/manifests/tokio/{key_bad}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(
                &format!("prefix/v3/packs/tokio/{key_bad}.tar.zst"),
                b"corrupted bytes".to_vec(),
            )
            .await;

        // Default behavior: completes all transfers, but returns Err
        let err = sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect_err("mixed sync should complete all transfers and return Err by default");
        assert!(
            err.to_string()
                .contains("1 transfer(s) or import(s) failed")
        );

        // Both GET calls were executed (no fail-fast abort)
        assert_eq!(backend.get_calls().len(), 2);
        // The good pack was successfully imported
        assert!(
            config
                .store_dir()
                .join(&key_good)
                .join("meta.json")
                .exists(),
            "good entry should be materialized"
        );
    }

    #[tokio::test]
    async fn sync_with_client_mixed_success_and_failure_with_allow_partial_returns_ok() {
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let key_good = sync_test_cache_key("mixed-good-2");
        let key_bad = sync_test_cache_key("mixed-bad-2");
        let pack_good = build_entry_pack(&key_good, "serde");

        let backend = TestBackend::memory();
        backend
            .seed(
                &format!("prefix/v3/manifests/serde/{key_good}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(
                &format!("prefix/v3/packs/serde/{key_good}.tar.zst"),
                pack_good,
            )
            .await;

        backend
            .seed(
                &format!("prefix/v3/manifests/tokio/{key_bad}.json"),
                b"{}".to_vec(),
            )
            .await;
        backend
            .seed(
                &format!("prefix/v3/packs/tokio/{key_bad}.tar.zst"),
                b"corrupted bytes".to_vec(),
            )
            .await;

        // With allow_partial: completes all transfers and returns Ok
        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            true,
            false,
            false,
            false,
            None,
            false,
            true,
        )
        .await
        .expect("mixed sync with allow_partial should complete Ok");

        assert_eq!(backend.get_calls().len(), 2);
        assert!(
            config
                .store_dir()
                .join(&key_good)
                .join("meta.json")
                .exists(),
            "good entry should be materialized"
        );
    }

    #[test]
    fn draw_clean_renders_target_table() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let targets = vec![
            TargetEntry {
                path: std::path::PathBuf::from("/work/proj-a/target"),
                size: 5_000_000,
                cached_bytes: 3_000_000,
                estimated_reclaimable_bytes: 2_000_000,
                scan_identity: None,
                profiles: vec!["debug".to_string(), "release".to_string()],
                breakdown: CategoryBreakdown::default(),
                stale: false,
            },
            TargetEntry {
                path: std::path::PathBuf::from("/work/proj-b/target"),
                size: 2_000_000,
                cached_bytes: 0,
                estimated_reclaimable_bytes: 2_000_000,
                scan_identity: None,
                profiles: vec![],
                breakdown: CategoryBreakdown::default(),
                stale: false,
            },
        ];
        // First row selected (the one carrying cached bytes), cursor on it.
        let selected = vec![true, false];

        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal
            .draw(|frame| draw_clean(frame, &targets, &selected, 0, std::path::Path::new("/work")))
            .expect("clean selector draw should succeed");
        let buffer = terminal.backend().buffer().clone();
        let rendered: String = buffer.content().iter().map(|c| c.symbol()).collect();
        assert!(rendered.contains("kache clean"), "header should render");
        assert!(
            rendered.contains("proj-a") && rendered.contains("proj-b"),
            "both target rows should render"
        );
        // The selected row's checkbox is set.
        assert!(rendered.contains("[x]"), "selected row shows a checked box");
        // Selection uses the separate scan-time reclaim estimate, not cached bytes.
        assert!(
            rendered.contains("Selected: 1 (est. 1.9 MiB)"),
            "selected total subtracts cached bytes, got: {rendered}"
        );
        assert!(
            !rendered.contains("Selected: 1 (est. 4.8 MiB)"),
            "selected total must not report the raw size"
        );
        // The header's dir totals stay raw (4.8 + 1.9 MiB, 2.9 MiB cached).
        assert!(
            rendered.contains("2 dirs (6.7 MiB total, 2.9 MiB cached)"),
            "header totals remain raw sizes"
        );
    }

    #[test]
    fn render_clean_dry_run_formats_plural_profiles_and_fallback_paths() {
        // Dry-run formatter -> plural/singular, profile tags, and strip fallback.
        let root = std::path::Path::new("/work");
        let single = vec![TargetEntry {
            path: std::path::PathBuf::from("/work/proj/target"),
            size: 1024,
            cached_bytes: 512,
            estimated_reclaimable_bytes: 512,
            scan_identity: None,
            profiles: vec!["debug".to_string()],
            breakdown: CategoryBreakdown::default(),
            stale: false,
        }];
        let single_out = render_clean_dry_run(&single, root).join("\n");
        assert!(single_out.contains("Found 1 target/ directory"));
        assert!(single_out.contains("proj/target"));
        assert!(single_out.contains("[debug]"));
        assert!(single_out.contains("Dry run: estimated to free 512 B"));
        assert!(single_out.contains("(512 B of apparent size is shared, sparse, or duplicate)"));

        let many = vec![
            TargetEntry {
                path: std::path::PathBuf::from("/work/proj-a/target"),
                size: 10,
                cached_bytes: 0,
                estimated_reclaimable_bytes: 10,
                scan_identity: None,
                profiles: Vec::new(),
                breakdown: CategoryBreakdown::default(),
                stale: false,
            },
            TargetEntry {
                path: std::path::PathBuf::from("/outside/proj-b/target"),
                size: 20,
                cached_bytes: 5,
                estimated_reclaimable_bytes: 15,
                scan_identity: None,
                profiles: vec!["release".to_string()],
                breakdown: CategoryBreakdown::default(),
                stale: false,
            },
        ];
        let many_out = render_clean_dry_run(&many, root).join("\n");
        assert!(many_out.contains("Found 2 target/ directories"));
        assert!(many_out.contains("/outside/proj-b/target"));
        assert!(many_out.contains("Dry run: estimated to free 25 B"));
        assert!(many_out.contains("(5 B of apparent size is shared, sparse, or duplicate)"));
        assert!(!many_out.contains("estimated to free 30 B"));
    }

    #[test]
    fn clean_handle_key_navigation_and_selection() {
        use crossterm::event::KeyCode;
        let mut selected = vec![false, false, false];
        let mut cursor = 0usize;
        let len = 3;

        // Down moves the cursor; clamped at the end.
        assert_eq!(
            clean_handle_key(KeyCode::Down, &mut selected, &mut cursor, len),
            CleanStep::Continue
        );
        assert_eq!(cursor, 1);
        // Up moves back; saturates at 0.
        clean_handle_key(KeyCode::Up, &mut selected, &mut cursor, len);
        assert_eq!(cursor, 0);
        clean_handle_key(KeyCode::Up, &mut selected, &mut cursor, len);
        assert_eq!(cursor, 0, "up saturates at 0");

        // Space toggles the current row and advances.
        clean_handle_key(KeyCode::Char(' '), &mut selected, &mut cursor, len);
        assert!(selected[0]);
        assert_eq!(cursor, 1);

        // Select-all / select-none.
        clean_handle_key(KeyCode::Char('a'), &mut selected, &mut cursor, len);
        assert!(selected.iter().all(|s| *s));
        clean_handle_key(KeyCode::Char('n'), &mut selected, &mut cursor, len);
        assert!(selected.iter().all(|s| !*s));
    }

    #[test]
    fn clean_handle_key_handles_boundaries() {
        use crossterm::event::KeyCode;
        // Empty/edge state -> no panic and cursor stays bounded.
        let mut empty = Vec::new();
        let mut empty_cursor = 0usize;
        assert_eq!(
            clean_handle_key(KeyCode::Char(' '), &mut empty, &mut empty_cursor, 0),
            CleanStep::Continue
        );
        assert_eq!(empty_cursor, 0);

        let mut selected = vec![false, false];
        let mut cursor = 1usize;
        clean_handle_key(KeyCode::Down, &mut selected, &mut cursor, 2);
        assert_eq!(cursor, 1, "down clamps at last row");
        clean_handle_key(KeyCode::Char(' '), &mut selected, &mut cursor, 2);
        assert!(selected[1]);
        assert_eq!(cursor, 1, "space on last row does not advance");

        cursor = 10;
        clean_handle_key(KeyCode::Char(' '), &mut selected, &mut cursor, 2);
        assert_eq!(cursor, 10, "out-of-range cursor is ignored");
    }

    #[test]
    fn remove_targets_deletes_all_and_reports_estimates() {
        // Two real target/ dirs under a root; --yes removes every one and sums
        // the scan-time estimate/gap only for successful removals.
        let root = tempfile::tempdir().unwrap();
        let a = root.path().join("proj-a/target");
        let b = root.path().join("proj-b/target");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();

        let to_remove = vec![
            RemovalTarget {
                path: a.clone(),
                scanned_identity: directory_identity(&a),
                estimated_reclaimable: 60,
                apparent_gap: 40,
            },
            RemovalTarget {
                path: b.clone(),
                scanned_identity: directory_identity(&b),
                estimated_reclaimable: 200,
                apparent_gap: 0,
            },
        ];
        let (removed, estimated_reclaimed, apparent_gap) =
            remove_targets(&to_remove, root.path(), false);

        assert_eq!(removed, 2, "both target/ dirs removed");
        assert_eq!(estimated_reclaimed, 260);
        assert_eq!(apparent_gap, 40);
        assert!(!a.exists() && !b.exists(), "directories are gone from disk");
    }

    #[test]
    fn remove_targets_skips_failures_without_aborting() {
        // A missing path fails to remove; a real one after it still succeeds and
        // only the removed dir's bytes are counted.
        let root = tempfile::tempdir().unwrap();
        let missing = root.path().join("gone/target");
        let real = root.path().join("proj/target");
        std::fs::create_dir_all(&real).unwrap();

        let to_remove = vec![
            RemovalTarget {
                path: missing,
                scanned_identity: None,
                estimated_reclaimable: 90,
                apparent_gap: 10,
            },
            RemovalTarget {
                path: real.clone(),
                scanned_identity: directory_identity(&real),
                estimated_reclaimable: 150,
                apparent_gap: 50,
            },
        ];
        let (removed, estimated_reclaimed, apparent_gap) =
            remove_targets(&to_remove, root.path(), false);

        assert_eq!(removed, 1, "only the existing dir counts as removed");
        assert_eq!(
            estimated_reclaimed, 150,
            "failed dir's estimate is not counted"
        );
        assert_eq!(apparent_gap, 50, "failed dir's gap is not counted");
        assert!(!real.exists(), "the reachable dir was still removed");
    }

    #[test]
    fn remove_targets_refuses_a_directory_replaced_after_scan() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("proj/target");
        let moved = root.path().join("proj/scanned-target");
        std::fs::create_dir_all(&target).unwrap();
        let scanned_identity = directory_identity(&target);
        std::fs::rename(&target, &moved).unwrap();
        std::fs::create_dir(&target).unwrap();

        let to_remove = vec![RemovalTarget {
            path: target.clone(),
            scanned_identity,
            estimated_reclaimable: 100,
            apparent_gap: 0,
        }];
        let (removed, estimated_reclaimed, apparent_gap) =
            remove_targets(&to_remove, root.path(), false);

        assert_eq!((removed, estimated_reclaimed, apparent_gap), (0, 0, 0));
        assert!(
            target.exists(),
            "replacement directory must be left untouched"
        );
        assert!(
            moved.exists(),
            "the scanned directory was moved, not removed"
        );
    }

    #[test]
    fn clean_handle_key_cancel_and_confirm() {
        use crossterm::event::KeyCode;
        let mut selected = vec![true];
        let mut cursor = 0usize;
        assert_eq!(
            clean_handle_key(KeyCode::Char('q'), &mut selected, &mut cursor, 1),
            CleanStep::Cancel
        );
        assert_eq!(
            clean_handle_key(KeyCode::Esc, &mut selected, &mut cursor, 1),
            CleanStep::Cancel
        );
        assert_eq!(
            clean_handle_key(KeyCode::Enter, &mut selected, &mut cursor, 1),
            CleanStep::Confirm
        );
        // An unhandled key is a no-op Continue.
        assert_eq!(
            clean_handle_key(KeyCode::Char('z'), &mut selected, &mut cursor, 1),
            CleanStep::Continue
        );
    }

    #[test]
    fn clean_event_applies_press_but_ignores_release() {
        use crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
        let key_event = |kind| {
            Event::Key(KeyEvent::new_with_kind(
                KeyCode::Char('q'),
                KeyModifiers::NONE,
                kind,
            ))
        };
        let mut selected = vec![false];
        let mut cursor = 0;

        assert_eq!(
            clean_handle_event(
                key_event(KeyEventKind::Release),
                &mut selected,
                &mut cursor,
                1,
            ),
            CleanStep::Continue,
            "key release must not repeat the action"
        );
        assert_eq!(
            clean_handle_event(
                key_event(KeyEventKind::Repeat),
                &mut selected,
                &mut cursor,
                1,
            ),
            CleanStep::Continue,
            "key repeat must not repeat the action"
        );
        assert_eq!(
            clean_handle_event(
                key_event(KeyEventKind::Press),
                &mut selected,
                &mut cursor,
                1,
            ),
            CleanStep::Cancel,
            "key press must apply the action"
        );
    }

    #[tokio::test]
    async fn sync_with_client_push_skipped_when_remote_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        config.remote_readonly = true;
        let store = Store::open(&config).unwrap();

        // Materialize one cache entry with a single artifact file.
        let src_dir = dir.path().join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        let artifact = src_dir.join("libfoo.rlib");
        std::fs::write(&artifact, b"artifact bytes").unwrap();
        store
            .put(
                "pushkey123",
                "foo",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "debug",
                &[(artifact, "libfoo.rlib".to_string())],
                "",
                "",
            )
            .unwrap();

        let remote = test_remote_cfg();
        // Since remote_readonly is true, the plan lists the remote keys but
        // does not push.
        let backend = TestBackend::memory();

        sync_with_client(
            backend.as_ref(),
            &config,
            &store,
            &remote,
            None,
            false,
            true,
            false,
            false,
            None,
            false,
            false,
        )
        .await
        .expect("push sync should succeed (by skipping pushes)");
        assert!(backend.put_calls().is_empty());
    }

    #[tokio::test]
    async fn save_manifest_skipped_when_remote_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        config.remote_readonly = true;

        // Create an event so save_manifest wouldn't normally skip due to empty events.
        let event_log = config.event_log_path();
        std::fs::create_dir_all(event_log.parent().unwrap()).unwrap();
        let event = serde_json::json!({
            "ts": chrono::Utc::now().to_rfc3339(),
            "crate_name": "foo",
            "result": "Miss",
            "elapsed_ms": 100,
            "compile_time_ms": 100,
            "size": 10,
            "cache_key": "key123"
        });
        let mut file = std::fs::File::create(&event_log).unwrap();
        use std::io::Write;
        writeln!(file, "{event}").unwrap();

        // Calling save_manifest should return Ok immediately without creating
        // a remote client or making any calls.
        save_manifest(&config, Some("mykey"), None)
            .expect("save_manifest should succeed by doing nothing");
    }
}

// ── Init ──────────────────────────────────────────────────────────────────
//
// Interactive setup that resolves the common doctor issues:
//   1. Writes `build.rustc-wrapper = "kache"` to $CARGO_HOME/config.toml
//      (fallback to ~/.cargo/config.toml)
//   1b. Adds HOST_CC / HOST_CXX / CC_KNOWN_WRAPPER_CUSTOM under `[env]`
//       when those keys are absent. Never sets CC or CXX.
//   1c. Unix: offers compiler-name shims in ~/.local/lib/kache/shims.
//       Does not edit PATH or shell rc.
//   2. Installs the daemon as a login service (launchd/systemd)
//   3. Starts the daemon
//
// Each step is skipped if already satisfied, so re-running is safe.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CargoWrapperPlan {
    /// File doesn't exist — create it with a fresh `[build]` section.
    Create,
    /// File exists but has a different wrapper (e.g. sccache) — replace the value.
    Replace(String),
    /// File has a `[build]` section but no `rustc-wrapper` — insert the key.
    AddUnderBuild,
    /// File exists with no `[build]` section — append one.
    AppendSection,
    /// Already set to kache.
    AlreadySet,
}

pub(crate) fn plan_cargo_wrapper_edit(path: &std::path::Path) -> Result<CargoWrapperPlan> {
    if !path.exists() {
        return Ok(CargoWrapperPlan::Create);
    }
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let parsed: toml::Value =
        toml::from_str(&content).with_context(|| format!("parsing {}", path.display()))?;
    let current = parsed
        .get("build")
        .and_then(|b| b.get("rustc-wrapper"))
        .and_then(|v| v.as_str());
    match current {
        Some("kache") => Ok(CargoWrapperPlan::AlreadySet),
        Some(other) => Ok(CargoWrapperPlan::Replace(other.to_string())),
        None if parsed.get("build").is_some() => Ok(CargoWrapperPlan::AddUnderBuild),
        None => Ok(CargoWrapperPlan::AppendSection),
    }
}

pub(crate) fn apply_cargo_wrapper_edit(existing: &str, plan: &CargoWrapperPlan) -> String {
    match plan {
        CargoWrapperPlan::AlreadySet => existing.to_string(),
        CargoWrapperPlan::Create => "[build]\nrustc-wrapper = \"kache\"\n".into(),
        CargoWrapperPlan::Replace(old) => {
            // Try each quoting style; fall back to just single-line textual replace.
            let candidates = [
                format!("rustc-wrapper = \"{old}\""),
                format!("rustc-wrapper = '{old}'"),
                format!("rustc-wrapper=\"{old}\""),
            ];
            for cand in &candidates {
                if existing.contains(cand) {
                    return existing.replacen(cand, "rustc-wrapper = \"kache\"", 1);
                }
            }
            existing.to_string()
        }
        CargoWrapperPlan::AddUnderBuild => {
            let mut out = String::with_capacity(existing.len() + 32);
            let mut inserted = false;
            for line in existing.lines() {
                out.push_str(line);
                out.push('\n');
                if !inserted && line.trim() == "[build]" {
                    out.push_str("rustc-wrapper = \"kache\"\n");
                    inserted = true;
                }
            }
            if !inserted {
                if !out.ends_with('\n') {
                    out.push('\n');
                }
                out.push_str("\n[build]\nrustc-wrapper = \"kache\"\n");
            }
            out
        }
        CargoWrapperPlan::AppendSection => {
            let mut out = existing.to_string();
            if !out.is_empty() && !out.ends_with('\n') {
                out.push('\n');
            }
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str("[build]\nrustc-wrapper = \"kache\"\n");
            out
        }
    }
}

/// Whether an init file-edit step should write. Dry-run (`check`) never
/// writes, even if the operator would have accepted the prompt.
fn should_write_init_step(check: bool, accepted: bool) -> bool {
    !check && accepted
}

fn prompt_yes_no(question: &str, default_yes: bool, auto_yes: bool) -> Result<bool> {
    use std::io::{BufRead, Write};

    let suffix = if default_yes { "[Y/n]" } else { "[y/N]" };
    print!("  {question} {suffix} ");
    std::io::stdout().flush().ok();

    if auto_yes {
        println!("y");
        return Ok(true);
    }

    let stdin = std::io::stdin();
    let mut line = String::new();
    stdin.lock().read_line(&mut line)?;
    let trimmed = line.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Ok(default_yes);
    }
    Ok(matches!(trimmed.as_str(), "y" | "yes"))
}

/// Build a timestamped sibling path for a pre-edit backup.
///
/// Format: `<name>.kache-backup.YYYYMMDD-HHMMSS`. Timestamped so repeated
/// runs don't silently overwrite an earlier backup.
fn backup_path_for(path: &std::path::Path) -> Option<std::path::PathBuf> {
    use chrono::Utc;
    let file_name = path.file_name()?.to_string_lossy().into_owned();
    let timestamp = Utc::now().format("%Y%m%d-%H%M%S");
    Some(path.with_file_name(format!("{file_name}.kache-backup.{timestamp}")))
}

/// `$CARGO_HOME`, falling back to `~/.cargo` (cargo's documented default).
fn cargo_home_dir() -> std::path::PathBuf {
    if let Some(cargo_home) = std::env::var_os("CARGO_HOME").filter(|value| !value.is_empty()) {
        let cargo_home = std::path::PathBuf::from(cargo_home);
        if cargo_home.is_absolute() {
            cargo_home
        } else {
            std::env::current_dir().unwrap_or_default().join(cargo_home)
        }
    } else {
        dirs::home_dir().unwrap_or_default().join(".cargo")
    }
}

fn cargo_config_target_path() -> std::path::PathBuf {
    let cargo_dir = cargo_home_dir();
    let with_ext = cargo_dir.join("config.toml");
    let legacy = cargo_dir.join("config");
    // Prefer the file that already exists; fall back to the canonical name.
    if legacy.exists() && !with_ext.exists() {
        legacy
    } else {
        with_ext
    }
}

pub fn init(yes: bool, no_service: bool, check: bool) -> Result<()> {
    println!();
    println!("  kache init — set up cache wrapper and daemon");
    println!();

    if check {
        println!("  (dry-run — no files will be modified)");
        println!();
    }

    // ── Step 1: cargo config wrapper ─────────────────────────────
    let cargo_path = cargo_config_target_path();
    let plan = plan_cargo_wrapper_edit(&cargo_path)?;

    match &plan {
        CargoWrapperPlan::AlreadySet => {
            println!(
                "  \x1b[32m✓\x1b[0m rustc-wrapper already set to kache in {}",
                crate::wrapper_config::display_path(&cargo_path)
            );
        }
        other => {
            let (summary, question) = match other {
                CargoWrapperPlan::Create => (
                    format!("create {} with rustc-wrapper = kache", cargo_path.display()),
                    "Create cargo config?".to_string(),
                ),
                CargoWrapperPlan::Replace(old) => (
                    format!(
                        "replace rustc-wrapper = \"{old}\" with \"kache\" in {}",
                        cargo_path.display()
                    ),
                    format!("Replace existing wrapper ({old}) with kache?"),
                ),
                CargoWrapperPlan::AddUnderBuild => (
                    format!(
                        "add rustc-wrapper = \"kache\" to existing [build] section in {}",
                        cargo_path.display()
                    ),
                    "Add rustc-wrapper = kache?".to_string(),
                ),
                CargoWrapperPlan::AppendSection => (
                    format!(
                        "append [build] section with rustc-wrapper = \"kache\" to {}",
                        cargo_path.display()
                    ),
                    "Append [build] section?".to_string(),
                ),
                CargoWrapperPlan::AlreadySet => unreachable!(),
            };
            println!("  \x1b[33m→\x1b[0m {summary}");
            if should_write_init_step(check, prompt_yes_no(&question, true, yes)?) {
                if let Some(parent) = cargo_path.parent() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("creating {}", parent.display()))?;
                }
                // Back up existing content before overwriting, so users can restore
                // if something goes sideways. Skipped for brand-new files (nothing
                // to preserve).
                if cargo_path.exists()
                    && let Some(backup_path) = backup_path_for(&cargo_path)
                {
                    std::fs::copy(&cargo_path, &backup_path)
                        .with_context(|| format!("writing backup to {}", backup_path.display()))?;
                    println!(
                        "    \x1b[32m✓\x1b[0m backup saved to {}",
                        backup_path.display()
                    );
                }
                let existing = std::fs::read_to_string(&cargo_path).unwrap_or_default();
                let new = apply_cargo_wrapper_edit(&existing, &plan);
                std::fs::write(&cargo_path, new)
                    .with_context(|| format!("writing {}", cargo_path.display()))?;
                println!("    \x1b[32m✓\x1b[0m wrote {}", cargo_path.display());
            }
        }
    }

    // ── Step 1b: cargo [env] host C wrappers ─────────────────────
    // HOST_CC/HOST_CXX wrap host compiles from the `cc` crate without replacing
    // `cargo build --target`'s cross compiler. CC/CXX are never set here.
    let env_missing = crate::cargo_env::missing_assignments_from_path(&cargo_path)?;
    if env_missing.is_empty() {
        println!(
            "  \x1b[32m✓\x1b[0m cargo [env] host C wrappers already set in {}",
            crate::wrapper_config::display_path(&cargo_path)
        );
    } else {
        let names = env_missing
            .iter()
            .map(|assignment| assignment.name)
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "  \x1b[33m→\x1b[0m set {names} in {} (does not set CC or CXX)",
            crate::wrapper_config::display_path(&cargo_path)
        );
        if should_write_init_step(
            check,
            prompt_yes_no(
                "Set host C compiler wrappers for Cargo build scripts?",
                true,
                yes,
            )?,
        ) {
            if let Some(parent) = cargo_path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            if cargo_path.exists()
                && let Some(backup_path) = backup_path_for(&cargo_path)
            {
                std::fs::copy(&cargo_path, &backup_path)
                    .with_context(|| format!("writing backup to {}", backup_path.display()))?;
                println!(
                    "    \x1b[32m✓\x1b[0m backup saved to {}",
                    backup_path.display()
                );
            }
            let existing = std::fs::read_to_string(&cargo_path).unwrap_or_default();
            let new = crate::cargo_env::apply_cargo_env_edit(&existing, &env_missing);
            std::fs::write(&cargo_path, new)
                .with_context(|| format!("writing {}", cargo_path.display()))?;
            println!("    \x1b[32m✓\x1b[0m wrote {}", cargo_path.display());
        }
    }

    // ── Step 1c: C/C++ compiler-name shims (Unix) ───────────────
    // Creates ~/.local/lib/kache/shims. Does not edit shell rc or PATH.
    #[cfg(unix)]
    {
        let shim_dir = crate::compiler::shim::default_shim_dir();
        if shim_dir_is_ready(&shim_dir) {
            println!(
                "  \x1b[32m✓\x1b[0m C/C++ shims already in {}",
                shim_dir.display()
            );
            println!("    export PATH=\"{}:$PATH\"", shim_dir.display());
        } else {
            println!(
                "  \x1b[33m→\x1b[0m install C/C++ compiler shims in {}",
                shim_dir.display()
            );
            if should_write_init_step(
                check,
                prompt_yes_no(
                    "Install C/C++ compiler shims for Make, CMake, and PKGBUILD?",
                    true,
                    yes,
                )?,
            ) {
                install_shims(&shim_dir, false)?;
            }
        }
    }

    // ── Step 2: daemon service ───────────────────────────────────
    let service_path = crate::service::service_file_path();
    let service_installed = service_path.as_ref().is_some_and(|p| p.exists());
    let service_mismatch = service_path
        .as_deref()
        .filter(|p| p.exists())
        .and_then(crate::service::service_exe_mismatch);
    let mut service_action_taken = false;

    if no_service {
        println!("  \x1b[33m→\x1b[0m skipping service install (--no-service)");
    } else if let Some(mismatch) = service_mismatch {
        println!("  \x1b[33m→\x1b[0m update daemon service to current kache binary");
        println!("    installed: {}", mismatch.installed.display());
        println!("    current:   {}", mismatch.current.display());
        if !check && prompt_yes_no("Update service?", true, yes)? {
            crate::service::install()?;
            service_action_taken = true;
        }
    } else if service_installed {
        println!(
            "  \x1b[32m✓\x1b[0m daemon service already installed at {}",
            service_path.as_ref().unwrap().display()
        );
    } else {
        println!("  \x1b[33m→\x1b[0m install daemon as a login service (launchd/systemd)");
        if !check && prompt_yes_no("Install service?", true, yes)? {
            crate::service::install()?;
            service_action_taken = true;
        }
    }

    // ── Step 3: daemon running ───────────────────────────────────
    // service::install() on macOS/Linux also starts the daemon, so skip the
    // manual start if we just installed it.
    let config = crate::config::Config::load().ok();
    let is_daemon_reachable = |cfg: &Option<crate::config::Config>| {
        cfg.as_ref()
            .is_some_and(|c| crate::daemon::send_stats_request(c, false, None, None).is_ok())
    };

    let mut daemon_step_failed = false;

    if is_daemon_reachable(&config) {
        println!("  \x1b[32m✓\x1b[0m daemon is running");
    } else if service_action_taken {
        // Service install typically starts the daemon. Give it a moment and re-check.
        std::thread::sleep(std::time::Duration::from_millis(500));
        if is_daemon_reachable(&config) {
            println!("  \x1b[32m✓\x1b[0m daemon started by service");
        } else {
            println!("  \x1b[33m→\x1b[0m daemon not reachable yet — it may take a few seconds");
        }
    } else if service_installed {
        // Service is installed (from a previous run) but daemon isn't reachable.
        // Prefer `launchctl kickstart` / `systemctl restart` over a manual spawn
        // so the service manager clears any stale state (lockfiles, half-dead
        // processes) and owns the new process.
        println!("  \x1b[33m→\x1b[0m restart daemon via service manager (daemon offline)");
        if !check
            && prompt_yes_no("Restart daemon?", true, yes)?
            && let Some(ref cfg) = config
        {
            match crate::daemon::restart(cfg)? {
                true => println!("    \x1b[32m✓\x1b[0m daemon restarted"),
                false => {
                    println!("    \x1b[31m✗\x1b[0m daemon did not restart — see `kache doctor`");
                    daemon_step_failed = true;
                }
            }
        }
    } else {
        println!("  \x1b[33m→\x1b[0m start daemon in background");
        if !check && prompt_yes_no("Start daemon now?", true, yes)? {
            match crate::daemon::start_daemon_background()? {
                true => println!("    \x1b[32m✓\x1b[0m daemon started"),
                false => {
                    println!("    \x1b[31m✗\x1b[0m daemon did not start within timeout");
                    daemon_step_failed = true;
                }
            }
        }
    }

    println!();
    if check {
        println!("  Dry run complete — re-run without --check to apply.");
        println!();
        Ok(())
    } else if daemon_step_failed {
        println!("  \x1b[31m✗\x1b[0m Setup incomplete — see messages above.");
        println!("     Run \x1b[1mkache doctor\x1b[0m for diagnostics.");
        println!();
        anyhow::bail!("init did not complete: daemon not reachable");
    } else {
        println!("  Setup complete. Run \x1b[1mkache doctor\x1b[0m to verify.");
        println!();
        Ok(())
    }
}

/// True when `dir` already holds the canonical compiler-name farm pointing
/// at this kache binary. Used by `kache init` so a second run is a no-op.
#[cfg(unix)]
fn shim_dir_is_ready(dir: &std::path::Path) -> bool {
    let Ok(exe) = std::env::current_exe() else {
        return false;
    };
    let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
    crate::compiler::shim::SHIM_NAMES.iter().all(|name| {
        let link = dir.join(name);
        std::fs::canonicalize(&link).is_ok_and(|real| real == exe)
    })
}

/// Populate `dir` with compiler-name symlinks pointing at this kache binary
/// (kunobi-ninja/kache#310).
///
/// Prepending the result to `PATH` routes every build's compiler calls through
/// kache with no `CC`/`CXX` edits and no per-project build-system changes.
///
/// Unix-only: it creates symlinks, and the Windows `.exe` shim story differs
/// (kunobi-ninja/kache#310). The unsupported message lives in the command
/// dispatch so this stays a single, fully testable definition rather than two
/// same-named ones the mutation lane cannot tell apart.
#[cfg(unix)]
pub(crate) fn install_shims(dir: &std::path::Path, force: bool) -> anyhow::Result<()> {
    install_shims_named(dir, force, &[])
}

#[cfg(unix)]
pub(crate) fn install_shims_named(
    dir: &std::path::Path,
    force: bool,
    extra_names: &[String],
) -> anyhow::Result<()> {
    let exe = std::env::current_exe().context("locating the kache binary")?;
    // Resolve so the shims survive kache being invoked through its own
    // symlink, and so `resolve_real_compiler`'s identity check (which
    // canonicalizes) reliably recognizes them as kache.
    let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
    std::fs::create_dir_all(dir)
        .with_context(|| format!("creating shim directory {}", dir.display()))?;

    let mut names: Vec<String> = crate::compiler::shim::SHIM_NAMES
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    for extra in extra_names {
        if !crate::compiler::shim::invoked_as_compiler(extra) {
            anyhow::bail!("`{extra}` is not a compiler name kache can wrap");
        }
        if !names.iter().any(|n| n == extra) {
            names.push(extra.clone());
        }
    }

    let mut created = Vec::new();
    let mut skipped = Vec::new();
    for name in &names {
        let link = dir.join(name);
        match std::fs::symlink_metadata(&link) {
            Ok(_) if !force => {
                skipped.push(name.clone());
                continue;
            }
            Ok(_) => std::fs::remove_file(&link)
                .with_context(|| format!("replacing existing {}", link.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| format!("inspecting {}", link.display()));
            }
        }
        std::os::unix::fs::symlink(&exe, &link)
            .with_context(|| format!("creating shim {}", link.display()))?;
        created.push(name.clone());
    }

    println!(
        "Created {} shim(s) in {} -> {}",
        created.len(),
        dir.display(),
        exe.display()
    );
    if !created.is_empty() {
        println!("  {}", created.join(", "));
    }
    if !skipped.is_empty() {
        println!(
            "Skipped {} existing entr(ies): {} (use --force to replace)",
            skipped.len(),
            skipped.join(", ")
        );
    }
    println!();
    println!("Add it to PATH ahead of your toolchain:");
    println!("  export PATH=\"{}:$PATH\"", dir.display());
    println!();
    // The ordering caveat is the one way this silently does nothing: a shim
    // dir appended rather than prepended is never consulted.
    println!(
        "The directory must come BEFORE the real toolchain on PATH, and the real \
         compilers must remain on PATH behind it — kache runs them."
    );
    println!(
        "Make, CMake, autotools, and Arch PKGBUILDs that invoke gcc/cc/clang \
         from PATH then go through kache. No CC/CXX edit and no shell wrapper."
    );
    println!(
        "For makepkg, put PATH=\"{}:$PATH\" in ~/.makepkg.conf.",
        dir.display()
    );
    Ok(())
}

#[cfg(all(test, unix))]
mod shim_install_tests {
    use super::install_shims;
    use crate::compiler::shim::SHIM_NAMES;
    use std::os::unix::fs::PermissionsExt;

    fn is_symlink(path: &std::path::Path) -> bool {
        std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink())
    }

    #[test]
    fn extra_compiler_name_is_installed_next_to_the_canonical_farm() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        super::install_shims_named(&shims, false, &["gcc-13".into()]).unwrap();

        let exe = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let link = shims.join("gcc-13");
        assert!(is_symlink(&link));
        assert_eq!(std::fs::read_link(&link).unwrap(), exe);
        for name in SHIM_NAMES {
            assert!(
                is_symlink(&shims.join(name)),
                "{name} must still be installed"
            );
        }
    }

    #[test]
    fn empty_dir_is_not_ready() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            !super::shim_dir_is_ready(dir.path()),
            "an empty directory must not count as an installed farm"
        );
    }

    #[test]
    fn install_makes_the_dir_ready_and_a_wrong_target_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        install_shims(&shims, false).unwrap();
        assert!(
            super::shim_dir_is_ready(&shims),
            "the installer must produce a farm that init treats as already done"
        );

        let other = dir.path().join("other");
        std::fs::create_dir_all(&other).unwrap();
        let not_kache = other.join("not-kache");
        std::fs::write(&not_kache, b"#!/bin/sh\n").unwrap();
        for name in SHIM_NAMES {
            std::fs::remove_file(shims.join(name)).unwrap();
            std::os::unix::fs::symlink(not_kache.as_path(), shims.join(name)).unwrap();
        }
        assert!(
            !super::shim_dir_is_ready(&shims),
            "links that do not resolve to this kache must not look ready"
        );
    }

    #[test]
    fn extra_name_that_is_not_a_compiler_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        let err = super::install_shims_named(&shims, false, &["gcc-ar".into()]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("gcc-ar"), "{msg}");
        assert!(msg.contains("not a compiler name"), "{msg}");
    }

    #[test]
    fn installs_a_symlink_for_every_shim_name() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        install_shims(&shims, false).unwrap();

        let exe = std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        for name in SHIM_NAMES {
            let link = shims.join(name);
            assert!(is_symlink(&link), "{name} must be a symlink");
            assert_eq!(
                std::fs::read_link(&link).unwrap(),
                exe,
                "{name} must point at this binary"
            );
        }
    }

    /// Without `--force` an existing entry is left exactly as it was. Silently
    /// overwriting whatever sits in the target directory would be a
    /// destructive default for a command users may point at `~/.local/bin`.
    #[test]
    fn existing_entries_are_preserved_unless_forced() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        let occupied = shims.join(SHIM_NAMES[0]);
        std::fs::write(&occupied, b"a real compiler wrapper").unwrap();

        install_shims(&shims, false).unwrap();

        assert!(
            !is_symlink(&occupied),
            "existing entry must not be replaced"
        );
        assert_eq!(
            std::fs::read(&occupied).unwrap(),
            b"a real compiler wrapper",
            "existing content must be untouched"
        );
        // The rest are still installed: one collision must not abort the run.
        for name in &SHIM_NAMES[1..] {
            assert!(is_symlink(&shims.join(name)), "{name} should be installed");
        }
    }

    #[test]
    fn force_replaces_an_existing_entry() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        let occupied = shims.join(SHIM_NAMES[0]);
        std::fs::write(&occupied, b"stale").unwrap();

        install_shims(&shims, true).unwrap();

        assert!(is_symlink(&occupied), "--force must replace the entry");
    }

    /// Re-running with `--force` is the "I moved the kache binary" refresh, so
    /// it must not fail on its own previous output or accumulate entries.
    #[test]
    fn forced_reinstall_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        install_shims(&shims, false).unwrap();
        install_shims(&shims, true).unwrap();

        for name in SHIM_NAMES {
            assert!(is_symlink(&shims.join(name)));
        }
        assert_eq!(
            std::fs::read_dir(&shims).unwrap().count(),
            SHIM_NAMES.len(),
            "reinstall must not accumulate entries"
        );
    }

    /// An inspection failure that is NOT "missing" must surface rather than be
    /// treated as an empty slot.
    #[test]
    fn unreadable_target_directory_is_an_error() {
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: running as root, mode 000 does not deny access");
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        std::fs::set_permissions(&shims, std::fs::Permissions::from_mode(0o000)).unwrap();

        let result = install_shims(&shims, false);
        std::fs::set_permissions(&shims, std::fs::Permissions::from_mode(0o755)).unwrap();

        // The MESSAGE matters, not just the failure: treating a
        // permission error as "nothing there" would fall through to the
        // symlink call and report the wrong stage.
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("inspecting"),
            "an inspection failure must be reported as such, got: {err}"
        );
    }
}
