use anyhow::{Context, Result};
use bytesize::ByteSize;
use std::io::IsTerminal;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use std::sync::Arc;

use crate::cache_remote::V3Prefetch;
use crate::config::Config;
use crate::daemon;
use crate::events;
use crate::machine::{PathIdentity, directory_identity};
use crate::since::SinceWindow;
use crate::store::{STAGING_SWEEP_GRACE, Store};

pub mod login;
mod miss_diagnosis;
use miss_diagnosis::{Cause, MissDiagnosis};

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

/// Index tables whose size explains a store: the entry and blob maps that grow
/// with the cache, and the key-side caches and tombstones beside them.
const MACHINE_INDEX_TABLES: [&str; 7] = [
    "entries",
    "blobs",
    "entry_blobs",
    "file_hashes",
    "cc_preprocess_memos",
    "input_predictions",
    "eviction_tombstones",
];

/// Read the shared cache without getting in a build's way: a read-only
/// connection (`query_only`, 25 ms busy timeout), no schema work and no
/// daemon, so a figure the index is too busy to answer is left out rather
/// than waited for. Read-only also means closing the connection can never
/// checkpoint the WAL into `index.db`.
pub(crate) fn machine_snapshot(config: &Config) -> crate::otel::MachineSnapshot {
    let db_path = config.index_db_path();
    let index_bytes = index_file_bytes(config);
    let mut snap = crate::otel::MachineSnapshot {
        index_bytes,
        wal_bytes: index_wal_bytes(config),
        gc: crate::report::read_gc_stats(&config.cache_dir),
        ..Default::default()
    };
    if index_bytes.is_none() {
        return snap;
    }
    let Ok(db) = crate::store::open_index_db_readonly(&db_path) else {
        return snap;
    };
    snap.store_physical_bytes = db
        .query_row("SELECT COALESCE(SUM(size), 0) FROM blobs", [], |row| {
            row.get::<_, i64>(0)
        })
        .ok()
        .map(|bytes| bytes.max(0) as u64);
    snap.index_free_bytes = index_free_bytes(&db);
    snap.blob_drift = crate::store::blob_refcount_drift(&db).ok();
    for table in MACHINE_INDEX_TABLES {
        let top: rusqlite::Result<Option<i64>> =
            db.query_row(&rowid_high_water_sql(table), [], |row| row.get(0));
        if let Ok(top) = top {
            snap.rowid_high_water
                .push((table, top.unwrap_or(0).max(0) as u64));
        }
    }
    snap
}

/// Bytes `index.db` holds in free pages: both pragmas read the database
/// header, so this costs no table scan.
fn index_free_bytes(db: &rusqlite::Connection) -> Option<u64> {
    let pragma = |sql: &str| db.query_row(sql, [], |row| row.get::<_, i64>(0)).ok();
    free_page_bytes(
        pragma("PRAGMA freelist_count")?,
        pragma("PRAGMA page_size")?,
    )
}

fn free_page_bytes(pages: i64, page_size: i64) -> Option<u64> {
    u64::try_from(pages)
        .ok()?
        .checked_mul(u64::try_from(page_size).ok()?)
}

/// A table's rowid high-water mark: one seek to the last leaf of its b-tree,
/// where `COUNT(*)` would read every page of a table that can hold gigabytes.
fn rowid_high_water_sql(table: &str) -> String {
    format!("SELECT MAX(rowid) FROM {table}")
}

/// `index.db` plus its `-wal`, or `None` when there is no index yet.
fn index_file_bytes(config: &Config) -> Option<u64> {
    let db_path = config.index_db_path();
    let mut wal = db_path.clone().into_os_string();
    wal.push("-wal");
    std::fs::metadata(&db_path)
        .ok()
        .map(|db| db.len() + std::fs::metadata(&wal).map(|w| w.len()).unwrap_or(0))
}

/// The index's `-wal` file alone: 0 when it is absent, `None` when there is
/// no index. A WAL that stays large means checkpoints are not keeping up
/// with the writes.
fn index_wal_bytes(config: &Config) -> Option<u64> {
    let db_path = config.index_db_path();
    std::fs::metadata(&db_path).ok()?;
    let mut wal = db_path.into_os_string();
    wal.push("-wal");
    Some(std::fs::metadata(&wal).map(|w| w.len()).unwrap_or(0))
}

/// The machine-level session log. In the cache dir, which every job on the
/// host shares, not the runtime dir a CI job deletes when it ends.
pub(crate) fn session_log_path(config: &Config) -> std::path::PathBuf {
    config.cache_dir.join("telemetry").join("sessions.jsonl")
}

/// Append `report` to the machine-level session log (`kache report
/// --record`): its summary and timing breakdown, plus what the host looked
/// like at that moment. Meant for the end of a CI job, before its runtime dir
/// and the events in it are deleted.
pub(crate) fn record_session(config: &Config, report: &crate::report::BuildReport) -> Result<()> {
    let mut loads = [0.0; 3];
    let written = crate::otel::sample_load_averages(&mut loads);
    let machine = crate::report::SessionMachine {
        load_1m: crate::otel::one_minute_load(written, loads[0]),
        cpus: std::thread::available_parallelism()
            .ok()
            .and_then(|n| u32::try_from(n.get()).ok()),
        index_bytes: index_file_bytes(config),
        store_max: config.max_size,
    };
    let record = crate::report::SessionRecord::from_report(report, machine);
    let path = session_log_path(config);
    crate::events::append_json_line(&path, &record)?;
    crate::events::rotate_if_needed(
        &path,
        config.event_log_max_size,
        config.event_log_keep_lines,
    )
}

/// Write cache counters as OTLP JSON for Kartero (`metrics.otlp.json` +
/// `schema_version`). Uses the running daemon when reachable; otherwise the
/// local store. Does not auto-start a daemon, so a finished bench dumps what
/// is already on disk instead of an empty new process.
/// Which sessions `kache telemetry push` sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimelineSelection {
    /// The session that finished last: what a CI post-step wants.
    Latest,
    /// Every session still in the logs.
    All,
    /// One session by id.
    Session(String),
}

/// Send build timeline records to the configured service.
///
/// Records are advisory, so this reports problems and returns `Ok`: a CI
/// post-step must not fail a green job because a record did not land.
pub fn telemetry_push(
    config: &Config,
    selection: &TimelineSelection,
    labels: &[String],
    dry_run: bool,
) -> Result<()> {
    let labels = parse_labels(labels)?;
    let (records, events_seen, versions) = collect_timelines(config, selection, labels)?;
    if records.is_empty() {
        println!("{}", nothing_to_send(events_seen, &versions));
        return Ok(());
    }

    if dry_run {
        println!("{}", serde_json::to_string_pretty(&records)?);
        return Ok(());
    }

    let Some(planner) = crate::config::Config::load_planner_config() else {
        println!(
            "no service configured; set KACHE_PLANNER_ENDPOINT to send {} record(s)",
            records.len()
        );
        return Ok(());
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    for record in &records {
        match rt.block_on(crate::timeline_client::push_timeline(&planner, record)) {
            Ok(answer) => println!(
                "sent session {} ({} units): {}",
                record.session_id,
                record.units.len(),
                answer.stored
            ),
            Err(error) => eprintln!("warning: session {} not sent: {error:#}", record.session_id),
        }
    }
    Ok(())
}

/// Why there was nothing to send.
///
/// Wrappers before kache 0.24 do not stamp a build session on their events, so
/// their logs cannot be grouped into builds. Saying that beats reporting an
/// empty log directory.
fn nothing_to_send(events_seen: usize, versions: &std::collections::BTreeSet<String>) -> String {
    if events_seen == 0 {
        return "no builds in the event log".to_string();
    }
    let versions: Vec<&str> = versions.iter().map(String::as_str).collect();
    format!(
        "{events_seen} compile(s) in the log, none stamped with a build session: \
         the wrapper that wrote them ({}) predates session ids. Upgrade kache on the builder.",
        if versions.is_empty() {
            "unknown version".to_string()
        } else {
            versions.join(", ")
        }
    )
}

/// Read the logs and build the records `selection` asks for, with what the log
/// held in case it produced nothing.
fn collect_timelines(
    config: &Config,
    selection: &TimelineSelection,
    labels: std::collections::BTreeMap<String, String>,
) -> Result<(
    Vec<kache_core::timeline::BuildTimeline>,
    usize,
    std::collections::BTreeSet<String>,
)> {
    let events = events::read_events(&config.event_log_path())?;
    let transfers = events::read_transfers(&config.transfer_log_path())?;
    let summaries = events::read_summaries(&config.summary_log_path())?;
    let env = crate::timeline::EnvSnapshot::from_env();
    let records = crate::timeline::build_timelines(&crate::timeline::TimelineInputs {
        events: &events,
        transfers: &transfers,
        summaries: &summaries,
        env: &env,
        labels,
        log: kache_core::timeline::LogLimits {
            event_log_max_size: config.event_log_max_size,
            event_log_keep_lines: config.event_log_keep_lines as u64,
        },
        kache_version: crate::VERSION.to_string(),
    });
    let versions = events
        .iter()
        .filter(|event| !event.version.is_empty())
        .map(|event| event.version.clone())
        .collect();
    Ok((select_timelines(records, selection), events.len(), versions))
}

/// Apply the selection to records the builder returned oldest first.
fn select_timelines(
    records: Vec<kache_core::timeline::BuildTimeline>,
    selection: &TimelineSelection,
) -> Vec<kache_core::timeline::BuildTimeline> {
    match selection {
        TimelineSelection::All => records,
        TimelineSelection::Latest => records.into_iter().next_back().into_iter().collect(),
        TimelineSelection::Session(wanted) => records
            .into_iter()
            .filter(|record| &record.session_id == wanted)
            .collect(),
    }
}

/// `--label key=value`, repeatable.
fn parse_labels(labels: &[String]) -> Result<std::collections::BTreeMap<String, String>> {
    labels
        .iter()
        .map(|label| {
            let (key, value) = label
                .split_once('=')
                .with_context(|| format!("label `{label}` is not key=value"))?;
            let key = key.trim();
            if key.is_empty() {
                anyhow::bail!("label `{label}` has an empty key");
            }
            Ok((key.to_string(), value.trim().to_string()))
        })
        .collect()
}

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
        &machine_snapshot(config),
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
    let cloned = disk.cloned_into_targets_bytes;
    let snapshot = disk.snapshot_retained_bytes;
    if cloned == 0 && snapshot == 0 {
        return None;
    }
    let mut line = format!("On disk:    {} private", ByteSize(disk.disk_private_bytes));
    if cloned > 0 {
        line.push_str(&format!("; {} cloned into target/", ByteSize(cloned)));
    }
    if snapshot > 0 {
        line.push_str(&format!(
            "; {} held only by filesystem snapshots",
            ByteSize(snapshot)
        ));
    }
    Some(line)
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
    let host_config = host_config_in_effect(&crate::config::host_config_status());
    let machine = machine_snapshot(config);

    if json {
        #[derive(serde::Serialize)]
        struct Body<'a> {
            disk: crate::machine::DiskView,
            /// This machine's shared index (`index.db` plus `-wal`) and each
            /// table's largest rowid, as `kache telemetry write` reports them.
            /// The rowid grows with every insert and replacement; it is not
            /// a row count.
            #[serde(skip_serializing_if = "Option::is_none")]
            index_bytes: Option<u64>,
            /// The `-wal` file alone, also counted in `index_bytes`.
            #[serde(skip_serializing_if = "Option::is_none")]
            index_wal_bytes: Option<u64>,
            #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
            index_rowid_high_water: std::collections::BTreeMap<&'static str, u64>,
            /// The last GC run, from `gc_stats.json`.
            #[serde(skip_serializing_if = "Option::is_none")]
            gc: Option<crate::report::GcStatsPersisted>,
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
            /// The host config file merged under the chosen one, when there is
            /// one in effect.
            #[serde(skip_serializing_if = "Option::is_none")]
            host_config: Option<&'a str>,
        }
        let hit_rate = count_hit_rate(&snap.event_stats);
        let remote = config.remote.as_ref().map(|r| r.describe());
        return crate::machine::emit(
            "stats",
            Body {
                disk: disk.clone(),
                index_bytes: machine.index_bytes,
                index_wal_bytes: machine.wal_bytes,
                index_rowid_high_water: machine.rowid_high_water.iter().copied().collect(),
                gc: machine.gc.clone(),
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
                host_config: host_config.as_deref(),
            },
            crate::machine::next_for_clones(&disk),
        );
    }

    for line in render_stats(&snap, config, window) {
        println!("{line}");
    }
    if let Some(path) = &host_config {
        println!("Host config: {path}");
    }
    if let Some(line) = cloned_targets_line(&disk) {
        println!("{line}");
    }
    for line in machine_lines(&machine) {
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
            let status = summary_status_suffix(s.cancelled, s.incomplete);
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
                status,
            );
        }
    }
    Ok(())
}

fn summary_status_suffix(cancelled: bool, incomplete: bool) -> &'static str {
    match (cancelled, incomplete) {
        (false, false) => "",
        (true, false) => ", CANCELLED",
        (false, true) => ", INCOMPLETE (partial totals)",
        (true, true) => ", CANCELLED, INCOMPLETE (partial totals)",
    }
}

/// `kache stats` lines for the machine's shared index and GC record: what the
/// cache costs the host beyond its artifacts, and whether GC keeps up. Pure,
/// like [`render_stats`].
fn machine_lines(machine: &crate::otel::MachineSnapshot) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(bytes) = machine.index_bytes {
        let mut rows = machine.rowid_high_water.clone();
        rows.sort_by_key(|&(_, rows)| std::cmp::Reverse(rows));
        let top: Vec<String> = rows
            .iter()
            .take(3)
            .map(|(table, rows)| format!("{table} {rows}"))
            .collect();
        let wal = machine
            .wal_bytes
            .map(|wal| format!(", WAL {}", ByteSize(wal)))
            .unwrap_or_default();
        if top.is_empty() {
            lines.push(format!("Index:     {}{wal}", ByteSize(bytes)));
        } else {
            lines.push(format!(
                "Index:     {}{wal} (rowid high-water: {})",
                ByteSize(bytes),
                top.join(", ")
            ));
        }
    }
    if let Some(gc) = &machine.gc {
        let source = if gc.source.is_empty() {
            "daemon"
        } else {
            gc.source.as_str()
        };
        let mut line = format!(
            "GC:        last run {} ({source}): {} evicted",
            gc.last_run, gc.entries_evicted
        );
        if gc.entries_failed > 0 {
            line.push_str(&format!(
                ", {} failed ({} lost the index write lock)",
                gc.entries_failed, gc.entries_locked
            ));
        }
        if gc.evict_write_ms > 0 {
            line.push_str(&format!(", {} ms in index writes", gc.evict_write_ms));
        }
        lines.push(line);
    }
    lines
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

pub fn stats_last_build(
    config: &Config,
    root: Option<std::path::PathBuf>,
    json: bool,
) -> Result<()> {
    let filter = crate::report::ReportFilter {
        root,
        last_build: true,
    };
    let report =
        crate::report::generate_report_with_filter(config, SinceWindow::DEFAULT, 10, &filter)?;
    if json {
        #[derive(serde::Serialize)]
        struct Body {
            report: crate::report::BuildReport,
        }
        crate::machine::emit("stats", Body { report }, Vec::new())
    } else {
        println!("{}", crate::report::format_text(&report));
        Ok(())
    }
}

pub fn report(
    config: &Config,
    format: &str,
    window: SinceWindow,
    filter: crate::report::ReportFilter,
    output: Option<std::path::PathBuf>,
    top: usize,
    record: bool,
) -> Result<()> {
    let report = if filter.root.is_some() || filter.last_build {
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

    // The session line is a side effect of the report: a cache dir it cannot
    // write costs that line and a warning, never the report or the exit code.
    // `record_sessions` records every report as `--record` does.
    let recorded = if record || config.record_sessions {
        record_session(config, &report)
    } else {
        Ok(())
    };
    if let Err(e) = recorded {
        eprintln!("warning: this session was not recorded: {e:#}");
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
/// it outranks key comparisons: nothing was written for a later build to match.
/// The terminal keeps the established `NOT CACHED` heading for this failure.
fn store_failure_banner(miss: &crate::events::BuildEvent) -> Option<String> {
    if miss.store_error.is_empty() {
        return None;
    }
    Some(format!(
        "  NOT CACHED: this compile ran and its outputs failed to store,\n  \
         so the crate misses on every build until the cause is fixed.\n    \
         reason: {}",
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
    let store = Store::open(config)?;
    let all_entries = store.list_entries("name")?;
    let stored: Vec<_> = all_entries
        .iter()
        .filter(|e| e.crate_name == crate_name)
        .collect();
    let same_key_present = stored.iter().any(|e| e.cache_key == miss.cache_key);
    let miss_index = all_events
        .iter()
        .position(|event| std::ptr::eq(event, *miss));
    let chain = miss_index.and_then(|index| crate::miss_chain::analyze(&all_events, index));
    let mut diagnosis = MissDiagnosis::new(
        miss,
        prior_same_key_miss,
        stored.len(),
        same_key_present,
        chain,
        config.explain_miss,
    );
    diagnosis.checkout =
        miss_index.and_then(|index| crate::miss_chain::compare_checkout(&all_events, index));
    if json {
        return why_miss_json(crate_name, miss, stored.len(), &diagnosis);
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

    println!();

    if stored.is_empty() {
        println!("  Stored entries for `{crate_name}`: (none)");
        println!();
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
    }
    print_miss_diagnosis(config, &store, miss, &stored, &diagnosis);

    // Render the dependency analysis shared with JSON output.
    print_checkout_comparison(&diagnosis);
    print_extern_chain(&diagnosis);

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

/// Render the comparison with another checkout, when the miss's own build tree
/// had no earlier build of the crate to compare with.
fn print_checkout_comparison(diagnosis: &MissDiagnosis) {
    let Some(checkout) = &diagnosis.checkout else {
        return;
    };
    // "Build tree", not "checkout": a registry crate built with the same
    // features and profile has one unit id in unrelated projects too.
    println!(
        "\n  Other build tree: no earlier build of this crate in {}",
        checkout.root
    );
    println!(
        "    compared with the same unit built in: {}",
        checkout.baseline_root
    );
    for line in checkout_comparison_lines(checkout) {
        println!("    {line}");
    }
}

fn checkout_comparison_lines(checkout: &crate::miss_chain::CheckoutComparison) -> Vec<String> {
    use crate::miss_chain::CheckoutVerdict;
    let dependencies = checkout
        .dependencies
        .iter()
        .map(|d| d.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let mut lines = Vec::new();
    match checkout.verdict {
        CheckoutVerdict::PathOnly => lines.push(
            "same cache key in both build trees: no key input differs, so the key is not why \
             this missed (the entry was evicted, not stored yet, or failed to store)"
                .to_string(),
        ),
        CheckoutVerdict::OwnInputs => {
            lines.push(format!("own inputs differ: {}", checkout.groups.join(", ")));
            if !checkout.dependencies.is_empty() {
                lines.push(format!("dependencies differ: {dependencies}"));
            }
            // `remap` is the one group that holds raw checkout paths on
            // purpose, so the normalization note would be wrong on its own.
            if checkout.groups.iter().any(|g| g != "remap") {
                lines.push(
                    "(input groups hash path-normalized inputs, so this is a real input \
                     difference or a path that escaped normalization)"
                        .to_string(),
                );
            }
            if checkout.groups.iter().any(|g| g == "remap") {
                lines.push(
                    "(remap: path remapping differs between the builds, or it is off and the key \
                     holds the checkout path)"
                        .to_string(),
                );
            }
        }
        CheckoutVerdict::Dependencies => lines.push(format!(
            "own inputs match after path normalization; dependencies differ: {dependencies}"
        )),
        CheckoutVerdict::Untraced => lines.push(
            "no input group or dependency differs, but the keys do (key salt or extra inputs?)"
                .to_string(),
        ),
    }
    lines
}

/// Render the `extern:` cascade for a miss, when one is recorded (#609).
///
/// Prints nothing when the miss is not downstream of a dependency change, so
/// the ordinary single-crate case reads exactly as before. When the digests
/// were never recorded, says how to turn them on rather than staying silent
/// about a diagnosis it could have given.
fn print_extern_chain(diagnosis: &MissDiagnosis) {
    let Some(chain) = &diagnosis.dependency_chain else {
        if diagnosis.dependency_recording_missing {
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
            crate::miss_chain::RootKind::PathOnly => println!(
                "    root: {}{via} -- same key in both build trees, but its artifact differs \
                 (its output is not reproducible across build trees)",
                root.crate_name
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

fn print_miss_diagnosis(
    config: &Config,
    store: &Store,
    miss: &events::BuildEvent,
    stored: &[&crate::store::EntryInfo],
    diagnosis: &MissDiagnosis,
) {
    match diagnosis.cause {
        Cause::NotCached => {} // The store-failure banner already leads the report.
        Cause::LookupRejected => {
            if let Some(banner) = lookup_rejection_banner(miss, diagnosis.same_key_present) {
                println!("{banner}");
            }
        }
        Cause::RepeatedSameKey => {
            if let Some(banner) = legacy_repeated_same_key_banner(miss, true) {
                println!("{banner}");
            }
        }
        Cause::NeverCached => println!("  Diagnosis: never cached -- first build of this crate"),
        Cause::FirstBuildNowCached => {
            println!("  Diagnosis: first build with these inputs -- entry is now cached");
        }
        Cause::KeyMismatch => {
            println!(
                "  Diagnosis: key mismatch -- {} other stored entries differ from key {}",
                diagnosis.other_entries,
                key_short(&miss.cache_key),
            );
            let other_entries: Vec<_> = stored
                .iter()
                .filter(|e| e.cache_key != miss.cache_key)
                .collect();
            why_miss_diff_entries(config, store, miss, &other_entries);
        }
    }
}

fn why_miss_json(
    crate_name: &str,
    miss: &events::BuildEvent,
    stored_entries: usize,
    diagnosis: &MissDiagnosis,
) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Body<'a> {
        crate_name: &'a str,
        diagnosis: Cause,
        last_result: String,
        cache_key: &'a str,
        store_error: &'a str,
        lookup_rejection: &'a str,
        stored_entries: usize,
        dependency_chain: &'a Option<crate::miss_chain::Chain>,
        dependency_recording_missing: bool,
        checkout_comparison: &'a Option<crate::miss_chain::CheckoutComparison>,
    }

    crate::machine::emit(
        "why-miss",
        Body {
            crate_name,
            diagnosis: diagnosis.cause,
            last_result: miss.result.to_string(),
            cache_key: &miss.cache_key,
            store_error: &miss.store_error,
            lookup_rejection: &miss.lookup_rejection,
            stored_entries,
            dependency_chain: &diagnosis.dependency_chain,
            dependency_recording_missing: diagnosis.dependency_recording_missing,
            checkout_comparison: &diagnosis.checkout,
        },
        Vec::new(),
    )
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

/// ` (<size>)` after a count of held entries, when the sweep measured it.
fn held_bytes_note(bytes_held: u64) -> String {
    if bytes_held == 0 {
        String::new()
    } else {
        format!(" ({})", ByteSize(bytes_held))
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
                "\n  {} is still held by clones in build outputs or by filesystem snapshots.",
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
                "\n  {} {}{} left in place because clones still hold their blocks. \
                 Inspect stale outputs with `kache clean --tracked --stale 14d --dry-run`, then run `kache gc` again.",
                stats.entries_unreclaimable,
                plural(stats.entries_unreclaimable),
                held_bytes_note(stats.bytes_held),
            ));
        }
        return msg;
    }

    if stats.entries_unreclaimable > 0 {
        return format!(
            " nothing reclaimable on disk.\n  {} {}{} cloned into build outputs \
             (same bytes as target/, not extra).\n  Remove stale outputs with \
             `kache clean --tracked --stale 14d --dry-run`, then run `kache gc` again.",
            stats.entries_unreclaimable,
            plural(stats.entries_unreclaimable),
            held_bytes_note(stats.bytes_held),
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
                snapshot_bytes: 0,
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

/// Count a profile's `build` directory. Before Cargo 1.100 it holds build
/// scripts and their `OUT_DIR`s. From 1.100 it holds every unit as
/// `<pkg>/<hash>/`, with the unit's `fingerprint/`, the `out/` rustc or a
/// build script wrote, and a build-script run's `run/`. Anything else counts
/// as build scripts, as the whole directory did before.
fn walk_build_dir(
    dir: &std::path::Path,
    stats: &mut ProjectStats,
    breakdown: &mut CategoryBreakdown,
    reclaim: &mut ReclaimEstimator,
    cache_candidates: &mut Vec<CacheCandidate>,
) {
    for (package, is_dir) in real_entries(dir) {
        let name = package.file_name().unwrap_or_default().to_string_lossy();
        if !is_dir || crate::cargo_layout::legacy_unit_package(&name).is_some() {
            count_path(
                &package,
                is_dir,
                ProjectBucket::BuildScripts,
                false,
                stats,
                breakdown,
                reclaim,
                cache_candidates,
            );
            continue;
        }
        for (unit, is_dir) in real_entries(&package) {
            let name = unit.file_name().unwrap_or_default().to_string_lossy();
            if !is_dir || !crate::cargo_layout::is_unit_hash(&name) {
                count_path(
                    &unit,
                    is_dir,
                    ProjectBucket::BuildScripts,
                    false,
                    stats,
                    breakdown,
                    reclaim,
                    cache_candidates,
                );
                continue;
            }
            let out_bucket = per_unit_out_bucket(&unit);
            for (part, is_dir) in real_entries(&unit) {
                let (bucket, cache_eligible) = match part.file_name().and_then(|name| name.to_str())
                {
                    Some("fingerprint") => (ProjectBucket::Fingerprints, false),
                    Some("out") => out_bucket,
                    _ => (ProjectBucket::BuildScripts, false),
                };
                count_path(
                    &part,
                    is_dir,
                    bucket,
                    cache_eligible,
                    stats,
                    breakdown,
                    reclaim,
                    cache_candidates,
                );
            }
        }
    }
}

/// `dir`'s entries that are regular files or real directories, with whether
/// each is a directory. Symlinks are skipped, as [`walk_project_dir`] skips
/// them.
fn real_entries(dir: &std::path::Path) -> Vec<(std::path::PathBuf, bool)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let kind = entry.file_type().ok()?;
            (kind.is_dir() || kind.is_file()).then(|| (entry.path(), kind.is_dir()))
        })
        .collect()
}

/// Count one file, or everything below one directory, in `bucket`.
#[allow(clippy::too_many_arguments)]
fn count_path(
    path: &std::path::Path,
    is_dir: bool,
    bucket: ProjectBucket,
    cache_eligible: bool,
    stats: &mut ProjectStats,
    breakdown: &mut CategoryBreakdown,
    reclaim: &mut ReclaimEstimator,
    cache_candidates: &mut Vec<CacheCandidate>,
) {
    if is_dir {
        walk_project_dir(
            path,
            bucket,
            cache_eligible,
            stats,
            breakdown,
            reclaim,
            cache_candidates,
        );
        return;
    }
    let Ok(meta) = std::fs::metadata(path) else {
        return;
    };
    let observation = observe_storage(path, &meta);
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

/// The bucket a per-unit `out` counts in. A build script's binary and a
/// run's `OUT_DIR` count as build scripts, as they do in the legacy layout;
/// every other unit's outputs count as deps.
fn per_unit_out_bucket(unit_dir: &std::path::Path) -> (ProjectBucket, bool) {
    let compiled_build_script = std::fs::read_dir(unit_dir.join("out")).is_ok_and(|entries| {
        entries.flatten().any(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("build_script_")
        })
    });
    if unit_dir.join("run").is_dir() || compiled_build_script {
        (ProjectBucket::BuildScripts, false)
    } else {
        (ProjectBucket::Deps, true)
    }
}

/// Analyze a project's target/ directory: which files share storage with kache's
/// store (reflinked or hardlinked) vs local-only, with per-category breakdown.
fn compute_project_stats(target_dir: &std::path::Path) -> (ProjectStats, CategoryBreakdown) {
    let mut stats = ProjectStats {
        total_bytes: 0,
        cached_bytes: 0,
        estimated_reclaimable_bytes: 0,
        local_bytes: 0,
        local_files: 0,
    };
    let mut breakdown = CategoryBreakdown::default();
    let mut reclaim = ReclaimEstimator::default();
    let mut cache_candidates = Vec::new();

    for (_, profile_dir) in profile_dirs(target_dir) {
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
                        walk_build_dir(
                            &path,
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
    /// The detached worker a build spawned is automatic; a `kache gc` the
    /// user typed is not, and may evict what the remote just delivered.
    pub fn sweep_origin(self) -> crate::store::SweepOrigin {
        match self {
            GcMode::Cli => crate::store::SweepOrigin::Requested,
            GcMode::Background => crate::store::SweepOrigin::Automatic,
        }
    }

    pub fn from_env() -> Self {
        if std::env::var_os("KACHE_AUTO_GC_WORKER").is_some() {
            GcMode::Background
        } else {
            GcMode::Cli
        }
    }
}

/// How many entries one durability sweep flushes before yielding. The daemon
/// sweeps again two seconds later, so a long backlog drains in batches rather
/// than in one hold of the store.
pub(crate) const DURABILITY_FLUSH_BATCH: usize = 64;

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
    // Entries stored without an fsync reach disk before anything is judged
    // by age or size; a flusher that never ran is caught up here.
    match store.flush_durability(usize::MAX) {
        Ok(0) => {}
        Ok(flushed) => tracing::info!("flushed {flushed} entries pending durability"),
        Err(error) => tracing::warn!("durability flush before gc failed: {error:#}"),
    }
    let mut combined = crate::store::GcStats::default();
    let started = std::time::Instant::now();

    if verbose {
        print!("Backfilling content hashes...");
        std::io::Write::flush(&mut std::io::stdout()).ok();
    }
    crate::wrapper::prune_session_markers(
        config,
        crate::wrapper::SESSION_MARKER_RETENTION,
        std::time::SystemTime::now(),
    );
    crate::build_script::sweep_hermetic_out_dirs_for_gc(&config.cache_dir);
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
    let dedup_stats = store
        .evict_duplicate_entries_for(mode.sweep_origin())
        .unwrap_or_default();
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
    let evict_stats = store.evict_for(mode.sweep_origin())?;
    add_gc_stats(&mut combined, &evict_stats);
    if verbose {
        let over_limit = store_over_limit(
            store
                .physical_size()
                .ok()
                .map(|size| size.saturating_sub(evict_stats.bytes_held)),
            config.max_size,
        );
        println!("{}", describe_eviction(&evict_stats, over_limit));
    }

    // Key lock files and input predictions grow with every distinct key and
    // eviction removes neither (#1126).
    let housekeeping = store.sweep_housekeeping();
    combined.housekeeping = Some(housekeeping);
    if verbose {
        println!(
            "Housekeeping: removed {} stale key locks ({} remain), {} unused input predictions, \
             {} old file hashes.",
            housekeeping.key_locks_removed,
            housekeeping.key_locks_remaining,
            housekeeping.predictions_pruned,
            housekeeping.file_hashes_pruned,
        );
    }

    combined.duration_ms = started.elapsed().as_millis() as u64;
    // Still under gc.lock, so the record cannot race another driver.
    // The auto-GC worker used to discard this outcome entirely. A failed write
    // must not fail the sweep it describes.
    let source = if mode == GcMode::Background {
        "auto"
    } else {
        "manual"
    };
    if let Err(e) = crate::report::record_gc_run(config, source, &combined) {
        tracing::debug!(
            "gc: could not record {}: {e:#}",
            crate::report::GC_STATS_FILE
        );
    }

    Ok(combined)
}

/// [`run_gc_local`] on the main store, then on each `[cache.volumes]` shard
/// under the shard's own `gc.lock` and budget (kunobi-ninja/kache#974).
/// Returns the main store's stats; a shard that fails is logged and skipped.
/// Each store's lock is its own, so a GC that finds the main store busy
/// still sweeps the shards that are not.
pub fn run_gc_local_with_shards(config: &Config, mode: GcMode) -> Result<crate::store::GcStats> {
    let main = run_gc_local(config, mode)?;
    crate::volume_gc::run_on_volume_shards(config, |shard| {
        if let Some(heading) = shard_sweep_heading(mode, &shard.cache_dir) {
            println!("{heading}");
        }
        run_gc_local(shard, mode)
    });
    Ok(main)
}

/// What `kache gc` prints before it sweeps a shard: nothing unless the sweep
/// itself prints its progress.
fn shard_sweep_heading(mode: GcMode, shard: &std::path::Path) -> Option<String> {
    (mode == GcMode::Cli).then(|| format!("Volume shard {}:", shard.display()))
}

/// The auto-GC worker for the main store, then for each `[cache.volumes]`
/// shard. Each store has its own trigger, budget, and backoff.
pub fn run_auto_gc_workers(config: &Config, retry_delay: std::time::Duration) {
    run_auto_gc_worker(config, retry_delay);
    crate::volume_gc::run_on_volume_shards(config, |shard| {
        run_auto_gc_worker(shard, retry_delay);
        Ok(())
    });
}

/// `kache gc --max-age` on each `[cache.volumes]` shard when the daemon could
/// not take it. A shard whose `gc.lock` is held is left to that GC.
fn evict_shards_older_than(config: &Config, hours: u64) -> Vec<crate::store::GcStats> {
    crate::volume_gc::run_on_volume_shards(config, |shard| {
        let store = Store::open(shard)?;
        let Some(_gc_lock) = store.try_gc_lock()? else {
            return Ok(skipped_gc_stats());
        };
        evict_older_than_recorded(&store, shard, hours)
    })
    .into_iter()
    .map(|(_, stats)| stats)
    .collect()
}

/// One line per `[cache.volumes]` shard for the summary `kache gc` prints.
/// Shards without an index are left out, as GC leaves them.
fn shard_store_lines(config: &Config) -> Vec<String> {
    crate::volume_gc::run_on_volume_shards(config, |shard| {
        let store = Store::open(shard)?;
        Ok(format!(
            "Volume shard {}: {} / {} ({} entries)",
            shard.cache_dir.display(),
            ByteSize(store.total_size()?),
            crate::config::describe_max_size(
                shard.max_size,
                crate::volume_gc::filesystem_bytes(&shard.cache_dir),
            ),
            store.entry_count()?
        ))
    })
    .into_iter()
    .map(|(_, line)| line)
    .collect()
}

/// The detached worker the wrapper spawns under size pressure when no daemon
/// takes its hint: sweep, and if live builds pinned entries (or another
/// driver held `gc.lock`), wait `retry_delay` for them to age out and sweep
/// again. The worker exits afterwards, so this is its only later chance. It
/// then records where the store ended so every automatic driver backs off
/// while it stays over budget. A worker whose sweeps both lost `gc.lock`
/// leaves the backoff to the driver that held it.
pub fn run_auto_gc_worker(config: &Config, retry_delay: std::time::Duration) {
    let first = auto_gc_worker_sweep(config);
    let second = first
        .as_ref()
        .is_some_and(auto_gc_retry_wanted)
        .then(|| {
            std::thread::sleep(retry_delay);
            auto_gc_worker_sweep(config)
        })
        .flatten();
    // The later sweep's probe is the fresher account of what is held.
    let Some(last) = [second, first]
        .into_iter()
        .flatten()
        .find(|stats| !stats.skipped)
    else {
        return;
    };
    match Store::open(config).and_then(|store| store.physical_size()) {
        Ok(size) => crate::wrapper::record_auto_gc_outcome(config, size, last.bytes_held),
        Err(e) => tracing::debug!("auto-gc: store size after the sweep unknown: {e:#}"),
    }
}

/// One worker sweep, if the shared trigger still calls for it. The wrapper
/// checked before spawning, but a daemon that acknowledged the hint too late
/// may have swept since, and the first sweep may have cleared the pressure.
fn auto_gc_worker_sweep(config: &Config) -> Option<crate::store::GcStats> {
    let size = Store::open(config)
        .and_then(|store| store.physical_size())
        .ok()?;
    if !crate::wrapper::auto_gc_sweep_due(config, size) {
        return None;
    }
    run_gc_local(config, GcMode::Background).ok()
}

/// Whether a second worker sweep can do better than the first: only when the
/// first left entries a live build pinned, or never got `gc.lock`.
fn auto_gc_retry_wanted(first: &crate::store::GcStats) -> bool {
    first.skipped || first.entries_pinned > 0
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
    total.entries_failed = total.entries_failed.saturating_add(part.entries_failed);
    total.entries_locked = total.entries_locked.saturating_add(part.entries_locked);
    total.entries_busy_snapshot = total
        .entries_busy_snapshot
        .saturating_add(part.entries_busy_snapshot);
    total.entries_recent_prefiltered = total
        .entries_recent_prefiltered
        .saturating_add(part.entries_recent_prefiltered);
    total.entries_import_pinned = total
        .entries_import_pinned
        .saturating_add(part.entries_import_pinned);
    total.evict_write_ms = total.evict_write_ms.saturating_add(part.evict_write_ms);
    // A measurement of the store, not work done: two sweeps probing the same
    // held bytes must not add up to twice as many.
    total.bytes_held = total.bytes_held.max(part.bytes_held);
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
        total.entries_failed = total.entries_failed.saturating_add(part.entries_failed);
        total.entries_locked = total.entries_locked.saturating_add(part.entries_locked);
        total.evict_write_ms = total.evict_write_ms.saturating_add(part.evict_write_ms);
        total.bytes_held = total.bytes_held.max(part.bytes_held);
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

/// Record a GC run `kache gc` made itself. A failed write costs the record,
/// never the GC.
fn record_manual_gc_run(config: &Config, stats: &crate::store::GcStats) {
    if let Err(e) = crate::report::record_gc_run(config, "manual", stats) {
        tracing::warn!("recording GC run: {e:#}");
    }
}

/// `kache gc --max-age` run locally because the daemon could not take it. The
/// caller holds gc.lock, so the recorded totals cannot race another driver.
fn evict_older_than_recorded(
    store: &Store,
    config: &Config,
    hours: u64,
) -> Result<crate::store::GcStats> {
    let started = std::time::Instant::now();
    let mut stats = store.evict_older_than(hours)?;
    stats.duration_ms = started.elapsed().as_millis() as u64;
    record_manual_gc_run(config, &stats);
    Ok(stats)
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
        let started = std::time::Instant::now();
        let mut stats = store.evict_stale_key_schemas(crate::cache_key::CACHE_KEY_VERSION)?;
        stats.duration_ms = started.elapsed().as_millis() as u64;
        record_manual_gc_run(config, &stats);
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

        run_auto_gc_workers(config, std::time::Duration::from_secs(sleep_secs));
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
                    let over_limit = store_over_limit(
                        store
                            .physical_size()
                            .ok()
                            .map(|size| size.saturating_sub(combined.bytes_held)),
                        config.max_size,
                    );
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
                let evict_stats = evict_older_than_recorded(&store, config, hours)?;
                drop(_gc_lock);
                evict_shards_older_than(config, hours);
                combined = evict_stats.clone();
                if human_gc_output(json) {
                    let over_limit = store_over_limit(
                        store
                            .physical_size()
                            .ok()
                            .map(|size| size.saturating_sub(evict_stats.bytes_held)),
                        config.max_size,
                    );
                    println!("{}", describe_eviction(&evict_stats, over_limit));
                }
            } else {
                combined = run_gc_local_with_shards(
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
    for line in shard_store_lines(config) {
        println!("{line}");
    }

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
        // Runs a target directory still links to stay: removing them would
        // break its next build.
        let sweep = crate::build_script::sweep_hermetic_out_dirs(
            &config.cache_dir,
            std::time::Duration::ZERO,
        )?;
        println!(
            "Removed {} build-script runs; kept {} that target directories link to.",
            sweep.removed, sweep.kept
        );
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

pub(crate) const DEFAULT_TRACKED_STALE_HOURS: u64 = 336;

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
    tracked: Option<TrackedSelection>,
) -> Result<()> {
    use crossterm::event;
    use ratatui::prelude::*;
    use std::io::stdout;

    let root = std::env::current_dir()?;
    let (mut targets, skipped, orphans) = if let Some(selection) = tracked {
        tracked_target_entries(config, selection)?
    } else {
        let mut targets = Vec::new();
        find_target_dirs(&root, &mut targets);
        (targets, Vec::new(), std::collections::HashSet::new())
    };
    let tracked = tracked.is_some();

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

    let emit_clean_json = |targets: &[TargetEntry],
                           skipped: &[CleanSkipped],
                           removed_paths: Vec<String>,
                           reclaimed: u64| {
        #[derive(serde::Serialize)]
        struct TargetBody {
            path: String,
            apparent_bytes: u64,
            cached_bytes: u64,
            estimated_reclaimable_bytes: u64,
            /// The workspace this target was built from no longer exists.
            orphaned: bool,
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
                    orphaned: orphans.contains(&t.path),
                })
                .collect(),
            skipped: skipped.to_vec(),
            changed: !removed_paths.is_empty(),
            removed_paths,
            estimated_reclaimed_bytes: reclaimed,
        };
        crate::machine::emit("clean", body, Vec::new())
    };

    // `--json` without `--yes` is a dry-run. Agents should not enter the TUI.
    if json && !yes {
        return emit_clean_json(&targets, &skipped, Vec::new(), 0);
    }

    // `--dry-run` takes precedence over `--yes`: preview only, never delete.
    if dry_run {
        for line in render_clean_dry_run(&targets, &root, &orphans) {
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
        let mut skipped = skipped.clone();
        let (removed, estimated_reclaimed, apparent_gap) =
            remove_targets(&to_remove, &root, json, &mut skipped);
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
            return emit_clean_json(&targets, &skipped, removed_paths, estimated_reclaimed);
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
                remove_targets(&to_remove, &root, false, &mut Vec::new());
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
    scanned_identity: Option<PathIdentity>,
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
    skipped: &mut Vec<CleanSkipped>,
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
        if target_in_use(&target.path) {
            if human_clean_output(quiet) {
                println!("  skipped {} — {TARGET_IN_USE}", rel.display());
            }
            skipped.push(CleanSkipped {
                path: target.path.display().to_string(),
                reason: TARGET_IN_USE.to_string(),
            });
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

const TARGET_IN_USE: &str = "in use by a running build";

/// Whether a Cargo process holds one of `target`'s build locks. Cargo takes
/// an exclusive lock on `<profile>/.cargo-lock` (and
/// `<triple>/<profile>/.cargo-lock`) for the length of a build, so failing to
/// take it here means a build is writing into this directory now.
pub(crate) fn target_in_use(target: &std::path::Path) -> bool {
    cargo_lock_files(target, 3).iter().any(|lock| {
        std::fs::File::open(lock)
            .is_ok_and(|file| matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock)))
    })
}

/// `.cargo-lock` files at most `depth` directories below `dir`. Symlinks
/// are not followed.
fn cargo_lock_files(dir: &std::path::Path, depth: usize) -> Vec<std::path::PathBuf> {
    let mut locks = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return locks;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_file() && entry.file_name() == ".cargo-lock" {
            locks.push(entry.path());
        } else if file_type.is_dir() && depth > 0 {
            locks.extend(cargo_lock_files(&entry.path(), depth - 1));
        }
    }
    locks
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

fn render_clean_dry_run(
    targets: &[TargetEntry],
    root: &std::path::Path,
    orphans: &std::collections::HashSet<std::path::PathBuf>,
) -> Vec<String> {
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
        let orphan_str = if orphans.contains(&t.path) {
            "  (worktree deleted)"
        } else {
            ""
        };
        lines.push(format!(
            "  {:<w$}  {:>10}  cached: {:>10}{profile_str}{orphan_str}",
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
    pub(crate) scan_identity: Option<PathIdentity>,
    pub profiles: Vec<String>,
    pub breakdown: CategoryBreakdown,
    /// Marked true when a rescan starts; cleared when fresh data arrives.
    pub stale: bool,
}

/// Whether the worktree a tracked target was built from is still there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetState {
    Live,
    WorktreeDeleted,
}

/// One tracked target directory, as `kache targets` reports it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub(crate) struct TargetRow {
    path: String,
    workspace: String,
    state: TargetState,
    /// Seconds since a build last used it.
    idle_seconds: u64,
    profiles: Vec<String>,
    /// Bytes its files add up to, counting shared blocks in full.
    apparent_bytes: u64,
    /// Estimated bytes deleting it would give back: its private extents,
    /// with hardlinks and blocks shared with the store left out.
    reclaimable_bytes: u64,
    /// Bytes restored from kache's store.
    cached_bytes: u64,
}

/// Every tracked target directory that still exists, the most freeable
/// first. Missing ones are left for `kache clean --tracked` to forget.
fn target_rows(config: &Config, now: i64) -> Result<Vec<TargetRow>> {
    let store = Store::open(config)?;
    let tracked: Vec<_> = store
        .tracked_target_roots(0)?
        .into_iter()
        .filter(|tracked| tracked.path.is_dir())
        .collect();
    // Each scan walks a whole target and probes extents; they are independent
    // and bound by the filesystem, so run several at once.
    let workers = std::thread::available_parallelism()
        .map_or(4, std::num::NonZeroUsize::get)
        .clamp(1, 8);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let scanned = std::sync::Mutex::new(Vec::with_capacity(tracked.len()));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(root) = tracked.get(index) else {
                        break;
                    };
                    let stats = compute_project_stats(&root.path).0;
                    let profiles = detect_profiles(&root.path);
                    if let Ok(mut scanned) = scanned.lock() {
                        scanned.push((index, stats, profiles));
                    }
                }
            });
        }
    });
    let scanned = scanned
        .into_inner()
        .unwrap_or_else(|error| error.into_inner());
    let mut rows = Vec::with_capacity(scanned.len());
    for (index, stats, profiles) in scanned {
        let tracked = &tracked[index];
        rows.push(TargetRow {
            path: tracked.path.display().to_string(),
            workspace: tracked.workspace_root.display().to_string(),
            state: if workspace_is_gone(&tracked.workspace_root) {
                TargetState::WorktreeDeleted
            } else {
                TargetState::Live
            },
            idle_seconds: now.saturating_sub(tracked.last_seen).max(0) as u64,
            profiles,
            apparent_bytes: stats.total_bytes,
            reclaimable_bytes: stats.estimated_reclaimable_bytes,
            cached_bytes: stats.cached_bytes,
        });
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row.reclaimable_bytes));
    Ok(rows)
}

/// `3d`, `5h`, `12m`, `40s`: the largest whole unit.
fn format_idle(seconds: u64) -> String {
    match seconds {
        s if s >= 86_400 => format!("{}d", s / 86_400),
        s if s >= 3_600 => format!("{}h", s / 3_600),
        s if s >= 60 => format!("{}m", s / 60),
        s => format!("{s}s"),
    }
}

fn render_targets(rows: &[TargetRow]) -> Vec<String> {
    let apparent: u64 = rows.iter().map(|row| row.apparent_bytes).sum();
    let reclaimable: u64 = rows.iter().map(|row| row.reclaimable_bytes).sum();
    let mut lines = vec![format!(
        "{} tracked target director{}: {} on disk, {} freeable\n",
        rows.len(),
        if rows.len() == 1 { "y" } else { "ies" },
        ByteSize(apparent),
        ByteSize(reclaimable)
    )];
    lines.push(format!(
        "  {:>10}  {:>10}  {:>5}  WORKSPACE",
        "FREEABLE", "ON DISK", "IDLE"
    ));
    for row in rows {
        let deleted = if row.state == TargetState::WorktreeDeleted {
            "  (worktree deleted)"
        } else {
            ""
        };
        lines.push(format!(
            "  {:>10}  {:>10}  {:>5}  {}{deleted}",
            ByteSize(row.reclaimable_bytes).to_string(),
            ByteSize(row.apparent_bytes).to_string(),
            format_idle(row.idle_seconds),
            row.workspace
        ));
    }
    let orphans = rows
        .iter()
        .filter(|row| row.state == TargetState::WorktreeDeleted)
        .count();
    if orphans > 0 {
        lines.push(format!(
            "\nRemove the {orphans} deleted worktree{} targets: kache clean --orphans --yes",
            if orphans == 1 { "'s" } else { "s'" }
        ));
    }
    lines
}

/// The clean that removes deleted worktrees' targets, when there are any.
fn targets_next_actions(rows: &[TargetRow]) -> Vec<crate::machine::NextAction> {
    let orphans = rows
        .iter()
        .filter(|row| row.state == TargetState::WorktreeDeleted)
        .count();
    if orphans == 0 {
        return Vec::new();
    }
    vec![crate::machine::NextAction {
        argv: ["kache", "clean", "--orphans", "--yes"]
            .map(String::from)
            .to_vec(),
        why: format!("{orphans} target(s) belong to deleted worktrees"),
    }]
}

/// Show every tracked target directory with what deleting it would free
/// and whether its worktree still exists.
pub fn targets(config: &Config, json: bool) -> Result<()> {
    let rows = target_rows(config, kache_store::markers::now_epoch_secs() as i64)?;
    if json {
        #[derive(serde::Serialize)]
        struct Body {
            targets: Vec<TargetRow>,
            apparent_bytes: u64,
            reclaimable_bytes: u64,
        }
        let next = targets_next_actions(&rows);
        return crate::machine::emit(
            "targets",
            Body {
                apparent_bytes: rows.iter().map(|row| row.apparent_bytes).sum(),
                reclaimable_bytes: rows.iter().map(|row| row.reclaimable_bytes).sum(),
                targets: rows,
            },
            next,
        );
    }
    if rows.is_empty() {
        println!("No tracked target directories. Builds through kache register theirs.");
        return Ok(());
    }
    for line in render_targets(&rows) {
        println!("{line}");
    }
    Ok(())
}

/// Which tracked targets a clean considers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackedSelection {
    /// Not seen for this many hours, or orphaned.
    StaleOrOrphaned(u64),
    /// Only targets whose workspace is gone.
    Orphaned,
}

impl TrackedSelection {
    fn includes(self, orphaned: bool, idle_seconds: i64) -> bool {
        match self {
            Self::Orphaned => orphaned,
            Self::StaleOrOrphaned(hours) => {
                orphaned || idle_seconds >= hours.saturating_mul(3600).min(i64::MAX as u64) as i64
            }
        }
    }
}

/// A workspace is gone when looking it up finds nothing while the
/// directory holding it still exists. The parent check keeps an unmounted
/// volume or a network share that is offline from reading as a deleted
/// worktree.
pub(crate) fn workspace_is_gone(workspace_root: &std::path::Path) -> bool {
    // Only a lookup that finds nothing counts: a permission or I/O error on
    // a network share is not a deleted worktree.
    let missing = std::fs::symlink_metadata(workspace_root)
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound);
    missing && workspace_root.parent().is_some_and(std::path::Path::is_dir)
}

fn tracked_target_entries(
    config: &Config,
    selection: TrackedSelection,
) -> Result<(
    Vec<TargetEntry>,
    Vec<CleanSkipped>,
    std::collections::HashSet<std::path::PathBuf>,
)> {
    let store = Store::open(config)?;
    // Every tracked root: an orphan qualifies however recently it was seen.
    let tracked = store.tracked_target_roots(0)?;
    let now = kache_store::markers::now_epoch_secs() as i64;
    let cwd = std::env::current_dir()?;
    let mut targets = Vec::new();
    let mut skipped = Vec::new();
    let mut orphans = std::collections::HashSet::new();

    for tracked in tracked {
        let orphaned = workspace_is_gone(&tracked.workspace_root);
        if !selection.includes(orphaned, now.saturating_sub(tracked.last_seen)) {
            continue;
        }
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
        if orphaned {
            orphans.insert(tracked.path.clone());
        }
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

    Ok((targets, skipped, orphans))
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
/// Hidden directories that hold worktrees, scanned for targets anyway.
const WORKTREE_CONTAINERS: [&str; 2] = [".worktrees", ".claude"];

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

        // Skip hidden dirs, node_modules, .git. `.worktrees` and `.claude`
        // are where worktree tools and coding agents put their checkouts.
        if (name_str.starts_with('.') && !WORKTREE_CONTAINERS.contains(&name_str.as_ref()))
            || name_str == "node_modules"
        {
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
    profile_dirs(target_dir)
        .into_iter()
        .map(|(label, _)| label)
        .collect()
}

/// Profile names every Cargo target directory can hold, recognised even
/// before Cargo has written into them.
const KNOWN_PROFILES: [&str; 4] = ["debug", "release", "profiling", "coverage"];

/// The profile directories of a target directory, labelled by their path
/// below it: a known profile name, any directory Cargo has built a profile
/// into (custom `[profile.ci]`), and `<triple>/<profile>` when `--target`
/// was given. Symlinks are never followed.
fn profile_dirs(target_dir: &std::path::Path) -> Vec<(String, std::path::PathBuf)> {
    fn real_dirs(dir: &std::path::Path) -> Vec<(String, std::path::PathBuf)> {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return Vec::new();
        };
        let mut dirs: Vec<_> = entries
            .flatten()
            .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
            .map(|entry| {
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    entry.path(),
                )
            })
            .collect();
        dirs.sort();
        dirs
    }
    fn is_profile(name: &str, dir: &std::path::Path) -> bool {
        KNOWN_PROFILES.contains(&name)
            || [".cargo-lock", ".fingerprint", "deps"]
                .iter()
                .any(|marker| std::fs::symlink_metadata(dir.join(marker)).is_ok())
    }
    let mut profiles = Vec::new();
    for (name, path) in real_dirs(target_dir) {
        if is_profile(&name, &path) {
            profiles.push((name, path));
            continue;
        }
        for (inner, inner_path) in real_dirs(&path) {
            if is_profile(&inner, &inner_path) {
                profiles.push((format!("{name}/{inner}"), inner_path));
            }
        }
    }
    // Known profiles first, in their usual order, then the rest by name.
    profiles.sort_by_key(|(label, _)| {
        (
            KNOWN_PROFILES
                .iter()
                .position(|known| known == label)
                .unwrap_or(KNOWN_PROFILES.len()),
            label.clone(),
        )
    });
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

/// The host config path `kache stats` names, only when the host file is
/// actually merged in: absent, disabled or unusable files are not in effect.
fn host_config_in_effect(status: &crate::config::HostConfigStatus) -> Option<String> {
    match status {
        crate::config::HostConfigStatus::Present { path, .. } => Some(path.display().to_string()),
        _ => None,
    }
}

/// Wording for the "Host config" check, as `(pass, detail, fix hint)`. Pure,
/// so each state is testable without a host file on the machine.
fn doctor_host_config_check(
    status: &crate::config::HostConfigStatus,
) -> (bool, String, Option<String>) {
    use crate::config::{HostConfigStatus, HostKeySource};
    match status {
        HostConfigStatus::Disabled => (true, "off (KACHE_HOST_CONFIG is empty)".to_string(), None),
        HostConfigStatus::Absent { path } => (true, format!("none at {}", path.display()), None),
        HostConfigStatus::Invalid { path, error } => (
            false,
            format!("{} is ignored: {error}", path.display()),
            Some(format!("fix or remove {}", path.display())),
        ),
        HostConfigStatus::Present { path, keys } => {
            let keys = if keys.is_empty() {
                "sets nothing".to_string()
            } else {
                keys.iter()
                    .map(|entry| match &entry.source {
                        HostKeySource::Host => entry.key.clone(),
                        HostKeySource::ChosenFile(file) => {
                            format!("{} (overridden by {})", entry.key, file.display())
                        }
                        HostKeySource::Env(var) => format!("{} (overridden by {var})", entry.key),
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            (true, format!("{}: {keys}", path.display()), None)
        }
    }
}

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
    startup_log: &str,
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
            Some(format!(
                "start daemon with `kache daemon start` or `kache daemon install`; \
                 if it does not start, the reason is in {startup_log}"
            )),
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

    // 3b. Host config layer
    let (host_pass, host_detail, host_fix) =
        doctor_host_config_check(&crate::config::host_config_status());
    checks.push(Check {
        label: "Host config",
        pass: host_pass,
        detail: host_detail,
        fix: host_fix,
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

        // 4c. Hardlink layout (#835): can the build tree hardlink into
        // `<store>/staging`? EXDEV across two bind mounts of one filesystem
        // is the most likely cause of 0% multi-link blobs on ext4 CI, with
        // both ingest and restore falling silently to a copy. The probe
        // creates a temp file in the build tree (current dir as proxy) and
        // links it into staging, then removes both — observability only.
        {
            let build_dir = std::env::current_dir().unwrap_or_else(|_| cfg.cache_dir.clone());
            let staging_dir = cfg.store_dir().join("staging");
            let probe = crate::link_probe::probe_link_layout(&build_dir, &staging_dir);
            let detail = crate::link_probe::format_probe_detail(&probe);
            let full_detail = format!(
                "{} (build: {}, staging: {})",
                detail,
                build_dir.display(),
                staging_dir.display()
            );
            checks.push(Check {
                label: "Link layout",
                pass: probe.hardlink_supported,
                detail: full_detail,
                fix: if probe.hardlink_supported {
                    None
                } else {
                    Some(
                        "put the cache and build tree on the SAME mount for zero-copy sharing; \
                         if this layout is intentional, expect copies (see `kache report` copy reasons)"
                            .to_string(),
                    )
                },
            });
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
            &crate::service::startup_log(cfg).to_string(),
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

    // Multiple cache instances may each own a daemon. Readiness above checks
    // this instance; a machine-wide process count cannot diagnose its health.

    // Startup and ownership locks are persistent. Never unlink their inodes,
    // even while idle: another process may already have opened the same file.

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
    let remote_cache: Arc<dyn crate::cache_remote::CacheRemote> =
        Arc::new(crate::cache_remote::V3Remote::new(backend, remote.clone()));
    sync_with_client(
        remote_cache.as_ref(),
        config,
        store,
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
    remote_cache: &dyn crate::cache_remote::CacheRemote,
    config: &Config,
    store: &Store,
    workspace_crates: Option<&std::collections::HashSet<String>>,
    pull_only: bool,
    push_only: bool,
    dry_run: bool,
    pull_all: bool,
    lock_crates: Option<&std::collections::HashSet<String>>,
    pull_workspace: bool,
    allow_partial: bool,
) -> Result<()> {
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
            let keys = remote_cache
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
            let keys = remote_cache
                .list_keys_for_crates(crates)
                .await
                .context("listing remote keys for dependency crates")?;
            eprintln!(" {} keys", keys.len());
            keys
        } else {
            eprint!("Listing remote keys...");
            let keys = remote_cache
                .list_keys()
                .await
                .context("listing remote keys")?;
            eprintln!(" {} keys", keys.len());
            keys
        }
    } else {
        // Push-only mode still lists remote keys to find what's already uploaded.
        eprint!("Listing remote keys...");
        let keys = remote_cache
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
            // Build-script runs describe one host's probes; they never leave it.
            .filter(|e| e.crate_name != crate::build_script::CRATE_NAME)
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

            let cfg = config.clone();
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
                let result = remote_cache
                    .download_entry(&key, &crate_name, &entry_dir, &blobs_dir, None)
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

            let cfg = config.clone();
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
                match remote_cache
                    .upload_entry(
                        &key,
                        &crate_name,
                        &entry_dir,
                        &blobs_dir,
                        cfg.compression_level,
                        None,
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

    // The daemon's auto-publish runs in the daemon's environment, which may
    // belong to an earlier job, so only a publish from the job itself names
    // the commit.
    let commit = manifest_commit(session_id, |name| std::env::var(name).ok());

    let pool_idle_secs = config.s3_pool_idle_secs;
    let entry_count = entries.len();
    let published = keys.clone();
    rt.block_on(async {
        let backend = crate::remote_backend::create_backend(remote, pool_idle_secs).await?;
        let remote_cache = Arc::new(crate::cache_remote::V3Remote::new(backend, remote.clone()));
        for (index, key) in keys.iter().enumerate() {
            let shard_namespace =
                shard_namespace_for_publish_key(index, effective_namespace.as_deref());
            upload_manifest_and_shards(
                &remote_cache,
                key,
                shard_namespace,
                std::path::Path::new("Cargo.lock"),
                entries.clone(),
                commit.as_deref(),
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
/// The commit to record with a published manifest: GitHub Actions'
/// `GITHUB_SHA`, else GitLab's `CI_COMMIT_SHA`, blank values counting as
/// unset. `None` for the daemon's per-session publish (`session_id` set),
/// whose environment may belong to an earlier job.
fn manifest_commit(
    session_id: Option<&str>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Option<String> {
    if session_id.is_some() {
        return None;
    }
    ["GITHUB_SHA", "CI_COMMIT_SHA"]
        .into_iter()
        .filter_map(lookup)
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

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
/// Takes the remote cache by reference so tests can drive it against a mock
/// (the production caller injects a real one from `create_backend`).
async fn upload_manifest_and_shards(
    remote_cache: &Arc<crate::cache_remote::V3Remote>,
    key: &str,
    namespace: Option<&str>,
    lock_path: &std::path::Path,
    entries: Vec<crate::remote::ManifestEntry>,
    commit: Option<&str>,
) -> Result<()> {
    let manifest = crate::remote::BuildManifest {
        version: 3,
        created: chrono::Utc::now().to_rfc3339(),
        manifest_key: key.to_string(),
        entries: entries.clone(),
    };

    // Always upload the monolithic build manifest.
    remote_cache
        .put_build_manifest(key, &manifest, commit)
        .await?;

    // Upload sharded build-manifest indexes if a namespace is provided and Cargo.lock exists.
    if let Some(ns) = namespace {
        if lock_path.exists() {
            let shard_count = upload_shards(remote_cache, ns, lock_path, &entries, commit).await?;
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
    remote_cache: &Arc<crate::cache_remote::V3Remote>,
    namespace: &str,
    lock_path: &std::path::Path,
    entries: &[crate::remote::ManifestEntry],
    commit: Option<&str>,
) -> Result<usize> {
    let deps = crate::shards::parse_cargo_lock(lock_path)?;
    let shard_set = crate::shards::compute_shards(&deps);

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
        let remote_cache = Arc::clone(remote_cache);
        let namespace = namespace.to_string();
        let commit = commit.map(str::to_string);
        let permit = sem.clone().acquire_owned().await?;
        handles.push(tokio::spawn(async move {
            let result = remote_cache
                .put_shard(&namespace, &hash, &shard, commit.as_deref())
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
        let handoffs = crate::daemon_publish::sweep_orphaned_handoffs(config, STAGING_SWEEP_GRACE);
        println!(
            "Repairing: reclaimed {} abandoned cc handoffs ({})",
            handoffs.removed,
            ByteSize(handoffs.bytes_reclaimed)
        );
        let memos = store.file_hash_cache();
        match memos.prune_cc_preprocess_memos() {
            Ok((removed, inputs)) if removed + inputs > 0 => println!(
                "Repairing: removed {removed} stale C/C++ memos and {inputs} unreferenced inputs"
            ),
            Ok(_) => {}
            Err(error) => println!("Warning: could not prune C/C++ memos: {error}"),
        }
        match memos.prune_input_predictions() {
            Ok(removed) => println!("Repairing: removed {removed} unused input predictions"),
            Err(error) => println!("Warning: could not prune input predictions: {error}"),
        }
        match memos.prune_file_hashes() {
            Ok(removed) => println!("Repairing: removed {removed} old file hashes"),
            Err(error) => println!("Warning: could not prune file hashes: {error}"),
        }
        match memos.compact_sparse_index() {
            Ok(Some((before, after))) => println!(
                "Repairing: compacted index file from {} to {}",
                ByteSize(before),
                ByteSize(after)
            ),
            Ok(None) => {}
            Err(error) => println!("Warning: index compaction deferred: {error:#}"),
        }
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
mod tests;

// ── Init ──────────────────────────────────────────────────────────────────
//
// Interactive setup that resolves the common doctor issues:
//   1. Writes `build.rustc-wrapper = "kache"` to $CARGO_HOME/config.toml
//      (fallback to ~/.cargo/config.toml)
//   1b. Adds HOST_CC / HOST_CXX / CC_KNOWN_WRAPPER_CUSTOM under `[env]`
//       when those keys are absent. Never sets CC or CXX.
//   1c. Unix: offers compiler-name shims in ~/.local/lib/kache/shims.
//       Saves shell PATH setup after confirmation; activation needs a new terminal.
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

/// Install the login service without failing init. A machine that cannot
/// register one (Task Scheduler without admin rights on Windows, a Linux host
/// whose systemd refuses the unit) still gets the daemon started by the next
/// init step (#1080).
fn install_login_service() -> bool {
    match crate::service::install() {
        Ok(()) => true,
        Err(error) => {
            println!("  • Login service: not installed ({error:#})");
            false
        }
    }
}

/// Set when a prompt found stdin closed, so `init` can tell "declined" apart
/// from "nobody was there to answer" (#1080).
static PROMPT_HAD_NO_INPUT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

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
    if stdin.lock().read_line(&mut line)? == 0 {
        println!("skipped (no input; use --yes to accept defaults)");
        PROMPT_HAD_NO_INPUT.store(true, std::sync::atomic::Ordering::Relaxed);
        return Ok(false);
    }
    let trimmed = line.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Ok(default_yes);
    }
    Ok(matches!(trimmed.as_str(), "y" | "yes"))
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

pub fn init(yes: bool, no_service: bool, no_shell: bool, check: bool) -> Result<()> {
    println!("\n  Set up Kache\n");
    if check {
        println!("  Preview only. No files or services will change.\n");
    }

    let cargo_path = cargo_config_target_path();
    let plan = plan_cargo_wrapper_edit(&cargo_path)?;
    let existing = if plan == CargoWrapperPlan::Create {
        String::new()
    } else {
        std::fs::read_to_string(&cargo_path).context("read Cargo configuration")?
    };
    let env_missing = crate::cargo_env::missing_assignments_from_path(&cargo_path)?;
    let mut cargo_ready = plan == CargoWrapperPlan::AlreadySet && env_missing.is_empty();
    if cargo_ready {
        println!("  ✓ Cargo caching: configured");
    } else {
        println!("  Cargo caching: Rust and native dependencies");
        println!(
            "    Config: {}",
            crate::wrapper_config::display_path(&cargo_path)
        );
        let question = if let CargoWrapperPlan::Replace(old) = &plan {
            format!("Replace {old} with Kache for Cargo builds?")
        } else {
            "Enable caching for Cargo builds?".into()
        };
        if check {
            println!("    Would configure Cargo. Existing compiler choices are preserved.");
        } else if prompt_yes_no(&question, true, yes)? {
            let wrapped = apply_cargo_wrapper_edit(&existing, &plan);
            let updated = crate::cargo_env::apply_cargo_env_edit(&wrapped, &env_missing);
            // Do not report success if a nonstandard existing wrapper could
            // not be replaced by the formatting-preserving editor.
            let parsed: toml::Value = toml::from_str(&updated)?;
            anyhow::ensure!(
                parsed
                    .get("build")
                    .and_then(|v| v.get("rustc-wrapper"))
                    .and_then(toml::Value::as_str)
                    == Some("kache"),
                "could not update Cargo's existing wrapper; edit {} manually",
                cargo_path.display()
            );
            if let Some(parent) = cargo_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if cargo_path.exists() {
                use std::io::Write;
                let mut backup = tempfile::Builder::new()
                    .prefix(".kache-cargo-backup-")
                    .tempfile_in(cargo_path.parent().context("Cargo config has no parent")?)?;
                backup.write_all(existing.as_bytes())?;
                backup.as_file().sync_all()?;
                let (_, backup) = backup.keep()?;
                println!(
                    "    Backup: {}",
                    crate::wrapper_config::display_path(&backup)
                );
            }
            std::fs::write(&cargo_path, updated).context("save Cargo configuration")?;
            cargo_ready = true;
            println!("  ✓ Cargo caching: configured");
        } else {
            println!("  • Cargo caching: skipped");
        }
    }

    #[cfg(unix)]
    let shell_pending = init_compiler_setup(yes, no_shell, check)?;
    #[cfg(not(unix))]
    let shell_pending = {
        let _ = no_shell;
        false
    };

    // ── Step 2: daemon service ───────────────────────────────────
    let service_path = crate::service::service_file_path();
    let service_installed = service_path.as_ref().is_some_and(|p| p.exists());
    let service_mismatch = service_path
        .as_deref()
        .filter(|p| p.exists())
        .and_then(crate::service::service_exe_mismatch);
    let mut service_action_taken = false;

    if no_service {
        println!("  \x1b[33m→\x1b[0m Login service: skipped (--no-service)");
    } else if !crate::service::login_service_available() {
        // Containers and CI runners have no systemd user manager. Installing
        // the unit would fail, so start the daemon directly below (#1080).
        println!("  • Login service: unavailable (no systemd user session)");
    } else if let Some(mismatch) = service_mismatch {
        println!("  \x1b[33m→\x1b[0m Background service: update to this Kache binary");
        println!("    installed: {}", mismatch.installed.display());
        println!("    current:   {}", mismatch.current.display());
        if !check && prompt_yes_no("Update service?", true, yes)? {
            service_action_taken = install_login_service();
        }
    } else if service_installed {
        println!(
            "  \x1b[32m✓\x1b[0m Login service: configured ({})",
            service_path.as_ref().unwrap().display()
        );
    } else {
        println!("  \x1b[33m→\x1b[0m Background service: start Kache when you log in");
        if !check && prompt_yes_no("Start Kache automatically at login?", true, yes)? {
            service_action_taken = install_login_service();
        }
    }

    // ── Step 3: daemon running ───────────────────────────────────
    // service::install() on macOS/Linux also starts the daemon, so skip the
    // manual start if we just installed it.
    if check {
        println!("  Background cache: would check and start if needed.");
        println!("\n  Preview only. Run kache init to apply.\n");
        return Ok(());
    }
    let config = crate::config::Config::load().ok();
    let is_daemon_reachable = |cfg: &Option<crate::config::Config>| {
        cfg.as_ref()
            .is_some_and(|c| crate::daemon::send_health_request(c).is_ok())
    };

    let mut daemon_step_failed = false;

    if is_daemon_reachable(&config) {
        println!("  \x1b[32m✓\x1b[0m Background cache: running");
    } else if service_action_taken {
        // Service install typically starts the daemon. Give it a moment and re-check.
        std::thread::sleep(std::time::Duration::from_millis(500));
        if is_daemon_reachable(&config) {
            println!("  \x1b[32m✓\x1b[0m Background cache: started");
        } else {
            println!(
                "  \x1b[33m→\x1b[0m Background cache: still starting; check with kache doctor"
            );
        }
    } else if service_installed {
        // Service is installed (from a previous run) but daemon isn't reachable.
        // The shared coordinator drains this instance and uses its installed
        // manager only when that manager owns the configured runtime path.
        println!("  \x1b[33m→\x1b[0m Background cache: needs restart");
        if !check
            && prompt_yes_no("Restart daemon?", true, yes)?
            && let Some(ref cfg) = config
        {
            match crate::daemon::restart(cfg)? {
                true => println!("    \x1b[32m✓\x1b[0m Background cache: running"),
                false => {
                    println!("    \x1b[31m✗\x1b[0m daemon did not restart — see `kache doctor`");
                    daemon_step_failed = true;
                }
            }
        }
    } else {
        println!("  \x1b[33m→\x1b[0m Background cache: not running");
        if !check && prompt_yes_no("Start daemon now?", true, yes)? {
            match crate::daemon::start_daemon_background()? {
                true => println!("    \x1b[32m✓\x1b[0m Background cache: running"),
                false => {
                    println!("    \x1b[31m✗\x1b[0m daemon did not start within timeout");
                    daemon_step_failed = true;
                }
            }
        }
    }

    println!();
    if daemon_step_failed {
        println!("  Background cache setup failed. Run kache doctor for details.\n");
        anyhow::bail!("init did not complete: daemon not reachable");
    }
    if !cargo_ready && PROMPT_HAD_NO_INPUT.load(std::sync::atomic::Ordering::Relaxed) {
        anyhow::bail!(
            "init changed nothing because stdin had no answers; rerun with --yes to accept the defaults"
        );
    }
    if cargo_ready {
        println!("  Ready for Cargo builds. Use cargo as usual.");
    }
    if shell_pending {
        println!("  Open a new terminal to activate C/C++ caching.");
    }
    println!("  Run kache doctor to check this terminal.\n");
    Ok(())
}

#[cfg(unix)]
fn init_compiler_setup(yes: bool, no_shell: bool, check: bool) -> Result<bool> {
    use crate::init_shell::{Edit, Shell};
    if no_shell {
        println!("  • Terminal C/C++ caching: skipped (--no-shell)");
        return Ok(false);
    }
    let shim_dir = crate::compiler::shim::default_shim_dir();
    let home = dirs::home_dir().context("could not find your home directory")?;
    let shell = std::env::var_os("SHELL")
        .as_deref()
        .and_then(|shell| Shell::detect(std::path::Path::new(shell)));
    let Some(shell) = shell else {
        println!("  • Terminal C/C++ caching: shell not supported for automatic setup");
        println!(
            "    Use kache install-shims, then add {} to your shell's PATH.",
            shim_dir.display()
        );
        return Ok(false);
    };
    let zdotdir = std::env::var_os("ZDOTDIR")
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from);
    let xdg = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from);
    let activation = shell.activation(&shim_dir)?;
    let edits = shell
        .paths(&home, zdotdir.as_deref(), xdg.as_deref())?
        .into_iter()
        .map(|path| Edit::plan(path, &activation))
        .collect::<Result<Vec<_>>>();
    let edits = match edits {
        Ok(edits) => edits,
        Err(error) => {
            println!("  • Terminal C/C++ caching: shell config needs manual attention");
            println!("    {error}");
            println!("    No shell files were changed.");
            return Ok(false);
        }
    };
    let shims_ready = shim_dir_is_ready(&shim_dir);
    let needs_edit = edits.iter().any(Edit::changed);
    if !shims_ready || needs_edit {
        println!("  C/C++ caching: terminal builds using cc, gcc or clang");
        for edit in edits.iter().filter(|edit| edit.changed()) {
            println!(
                "    Shell config: {}",
                crate::wrapper_config::display_path(&edit.path)
            );
        }
        if check {
            println!("    Would install compiler links and save the shell setup.");
            return Ok(false);
        }
        if !prompt_yes_no("Enable C/C++ caching in new terminals?", true, yes)? {
            println!("  • Terminal C/C++ caching: skipped");
            return Ok(false);
        }
        if !shims_ready {
            migrate_homebrew_shims(&shim_dir, &shim_target_executable()?)?;
            install_shims_named_with_output(&shim_dir, false, &[], false)?;
            anyhow::ensure!(
                shim_dir_is_ready(&shim_dir),
                "existing files in {} prevent compiler setup; inspect them before using kache install-shims --force",
                shim_dir.display()
            );
        }
        for edit in &edits {
            if let Some(backup) = edit.apply()? {
                println!(
                    "    Backup: {}",
                    crate::wrapper_config::display_path(&backup)
                );
            }
        }
    }
    if crate::compiler::shim::live_shim_path_status().on_path {
        println!("  ✓ Terminal C/C++ caching: active");
        Ok(false)
    } else {
        println!("  ✓ Terminal C/C++ caching: configured for new terminals");
        println!("    For this terminal, run:");
        println!("    {}", shell.command(&shim_dir)?);
        Ok(true)
    }
}

/// True when `dir` already holds the canonical compiler-name farm for the
/// active shim target. Used by `kache init` so a second run is a no-op.
#[cfg(unix)]
fn shim_dir_is_ready_for_executable(dir: &std::path::Path, exe: &std::path::Path) -> bool {
    let resolved = std::fs::canonicalize(exe).unwrap_or_else(|_| exe.to_path_buf());
    let homebrew_opt = homebrew_opt_shim_target(&resolved).as_deref() == Some(exe);
    crate::compiler::shim::SHIM_NAMES.iter().all(|name| {
        let link = dir.join(name);
        if homebrew_opt {
            std::fs::read_link(&link).is_ok_and(|target| target == exe)
        } else {
            std::fs::canonicalize(&link).is_ok_and(|real| real == resolved)
        }
    })
}

/// Refresh links written by older Kache releases without replacing other files.
#[cfg(unix)]
fn migrate_homebrew_shims(dir: &std::path::Path, target: &std::path::Path) -> Result<()> {
    let Ok(resolved) = std::fs::canonicalize(target) else {
        return Ok(());
    };
    if homebrew_opt_shim_target(&resolved).as_deref() != Some(target) {
        return Ok(());
    }
    let Some(formula) = target.parent().and_then(std::path::Path::parent) else {
        return Ok(());
    };
    let Some(prefix) = formula.parent().and_then(std::path::Path::parent) else {
        return Ok(());
    };
    let Some(formula_name) = formula.file_name() else {
        return Ok(());
    };
    let cellar_formula = prefix.join("Cellar").join(formula_name);
    for name in crate::compiler::shim::SHIM_NAMES {
        let link = dir.join(name);
        let Ok(previous) = std::fs::read_link(&link) else {
            continue;
        };
        let owned = previous.file_name() == target.file_name()
            && previous
                .parent()
                .is_some_and(|bin| bin.file_name().is_some_and(|n| n == "bin"))
            && previous
                .parent()
                .and_then(std::path::Path::parent)
                .and_then(std::path::Path::parent)
                == Some(cellar_formula.as_path());
        if !owned {
            continue;
        }
        std::fs::remove_file(&link)
            .with_context(|| format!("removing old shim {}", link.display()))?;
        std::os::unix::fs::symlink(target, &link)
            .with_context(|| format!("refreshing shim {}", link.display()))?;
    }
    Ok(())
}

#[cfg(unix)]
fn shim_dir_is_ready(dir: &std::path::Path) -> bool {
    shim_target_executable().is_ok_and(|exe| shim_dir_is_ready_for_executable(dir, &exe))
}

/// Return Homebrew's stable executable path when `exe` is in a Cellar keg.
///
/// The `opt/<formula>` symlink is repointed on upgrade, unlike a versioned keg
/// path. Keep this path non-canonical when writing compiler shims; callers that
/// compare executable identities still canonicalize their inputs.
#[cfg(unix)]
fn homebrew_opt_shim_target(exe: &std::path::Path) -> Option<std::path::PathBuf> {
    let bin = exe.parent()?;
    if bin.file_name()? != "bin" {
        return None;
    }
    let version = bin.parent()?;
    let formula = version.parent()?;
    let cellar = formula.parent()?;
    if cellar.file_name()? != "Cellar" {
        return None;
    }

    let target = cellar
        .parent()?
        .join("opt")
        .join(formula.file_name()?)
        .join("bin")
        .join(exe.file_name()?);
    target.is_file().then_some(target)
}

#[cfg(unix)]
fn shim_target_for_executable(exe: std::path::PathBuf) -> std::path::PathBuf {
    let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
    homebrew_opt_shim_target(&exe).unwrap_or(exe)
}

#[cfg(unix)]
fn shim_target_executable() -> Result<std::path::PathBuf> {
    let exe = std::env::current_exe().context("locating the kache binary")?;
    Ok(shim_target_for_executable(exe))
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
pub(crate) fn install_shims_named(
    dir: &std::path::Path,
    force: bool,
    extra_names: &[String],
) -> anyhow::Result<()> {
    install_shims_named_with_output(dir, force, extra_names, true)
}

#[cfg(unix)]
fn install_shims_named_with_output(
    dir: &std::path::Path,
    force: bool,
    extra_names: &[String],
    verbose: bool,
) -> anyhow::Result<()> {
    // Resolve aliases for identity checks, but preserve Homebrew's opt path in
    // the links so a formula upgrade repoints the existing shim farm.
    let exe = shim_target_executable()?;
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
    // The marker hides every entry in the directory from every kache, so a
    // directory that also holds a real compiler, or anything else, stays
    // unmarked. The links in it are still recognized by what they point at.
    let marked = crate::compiler::shim::holds_only_shims(dir, Some(&exe), &|path| {
        std::fs::canonicalize(path).ok()
    });
    if marked {
        crate::compiler::shim::write_shim_marker(dir)
            .with_context(|| format!("marking shim directory {}", dir.display()))?;
    }

    if !verbose {
        return Ok(());
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
    if !marked {
        println!(
            "Left {} without a {} marker: it holds files that are not kache links.",
            dir.display(),
            crate::compiler::shim::SHIM_DIR_MARKER
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
    fn install_shims(dir: &std::path::Path, force: bool) -> anyhow::Result<()> {
        super::install_shims_named(dir, force, &[])
    }
    use crate::compiler::shim::SHIM_NAMES;
    use std::os::unix::fs::PermissionsExt;

    fn is_symlink(path: &std::path::Path) -> bool {
        std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink())
    }

    #[test]
    fn homebrew_shim_target_uses_the_upgrade_stable_opt_path() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("homebrew");
        let keg = prefix.join("Cellar/kache/0.20.0");
        let exe = keg.join("bin/kache");
        std::fs::create_dir_all(exe.parent().unwrap()).unwrap();
        std::fs::write(&exe, b"kache").unwrap();

        let prefix = std::fs::canonicalize(&prefix).unwrap();
        let opt = prefix.join("opt/kache");
        std::fs::create_dir_all(opt.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(prefix.join("Cellar/kache/0.20.0"), &opt).unwrap();

        let target = super::shim_target_for_executable(exe.clone());
        assert_eq!(target, opt.join("bin/kache"));
        assert_eq!(
            std::fs::canonicalize(target).unwrap(),
            std::fs::canonicalize(exe).unwrap(),
            "the stable opt link must resolve to this keg"
        );
    }

    #[test]
    fn homebrew_opt_target_refreshes_shims_after_an_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("homebrew");
        let old_keg = prefix.join("Cellar/kache/0.19.0");
        let new_keg = prefix.join("Cellar/kache/0.20.0");
        let old_exe = old_keg.join("bin/kache");
        let new_exe = new_keg.join("bin/kache");
        for exe in [&old_exe, &new_exe] {
            std::fs::create_dir_all(exe.parent().unwrap()).unwrap();
            std::fs::write(exe, b"kache").unwrap();
        }

        let prefix = std::fs::canonicalize(&prefix).unwrap();
        let opt = prefix.join("opt/kache");
        std::fs::create_dir_all(opt.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(prefix.join("Cellar/kache/0.20.0"), &opt).unwrap();

        let shims = dir.path().join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        for name in SHIM_NAMES {
            std::os::unix::fs::symlink(opt.join("bin/kache"), shims.join(name)).unwrap();
        }

        let target = super::shim_target_for_executable(old_exe.clone());
        assert!(super::shim_dir_is_ready_for_executable(&shims, &target));
        assert!(
            !super::shim_dir_is_ready_for_executable(&shims, &old_exe),
            "a versioned old-keg target must be refreshed through opt"
        );
    }

    #[test]
    fn migrates_versioned_homebrew_shims_before_and_after_upgrade() {
        for formula in ["kache", "kache-unstable"] {
            let dir = tempfile::tempdir().unwrap();
            let prefix = dir.path().join("homebrew");
            let current = prefix.join(format!("Cellar/{formula}/0.20.0/bin/kache"));
            std::fs::create_dir_all(current.parent().unwrap()).unwrap();
            std::fs::write(&current, b"kache").unwrap();
            let prefix = std::fs::canonicalize(prefix).unwrap();
            let opt = prefix.join(format!("opt/{formula}"));
            std::fs::create_dir_all(opt.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(current.parent().unwrap().parent().unwrap(), &opt).unwrap();
            let target = opt.join("bin/kache");
            let shims = dir.path().join("shims");
            std::fs::create_dir_all(&shims).unwrap();

            for old_version in ["0.20.0", "0.19.0"] {
                let old = prefix.join(format!("Cellar/{formula}/{old_version}/bin/kache"));
                for name in SHIM_NAMES {
                    let link = shims.join(name);
                    if std::fs::symlink_metadata(&link).is_ok() {
                        std::fs::remove_file(&link).unwrap();
                    }
                    std::os::unix::fs::symlink(&old, &link).unwrap();
                }
                assert!(!super::shim_dir_is_ready_for_executable(&shims, &target));
                super::migrate_homebrew_shims(&shims, &target).unwrap();
                assert!(super::shim_dir_is_ready_for_executable(&shims, &target));
                for name in SHIM_NAMES {
                    assert_eq!(std::fs::read_link(shims.join(name)).unwrap(), target);
                }
            }
        }
    }

    #[test]
    fn migration_preserves_unrelated_links() {
        let dir = tempfile::tempdir().unwrap();
        let prefix = dir.path().join("homebrew");
        let current = prefix.join("Cellar/kache/0.20.0/bin/kache");
        std::fs::create_dir_all(current.parent().unwrap()).unwrap();
        std::fs::write(&current, b"kache").unwrap();
        let prefix = std::fs::canonicalize(prefix).unwrap();
        let opt = prefix.join("opt/kache");
        std::fs::create_dir_all(opt.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(current.parent().unwrap().parent().unwrap(), &opt).unwrap();
        let shims = dir.path().join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        let link = shims.join("cc");
        std::os::unix::fs::symlink("/usr/bin/clang", &link).unwrap();
        let other_formula = shims.join("gcc");
        let unstable = prefix.join("Cellar/kache-unstable/0.19.0/bin/kache");
        std::os::unix::fs::symlink(&unstable, &other_formula).unwrap();
        let regular_file = shims.join("clang");
        std::fs::write(&regular_file, b"custom compiler").unwrap();
        super::migrate_homebrew_shims(&shims, &opt.join("bin/kache")).unwrap();
        assert_eq!(
            std::fs::read_link(link).unwrap(),
            std::path::Path::new("/usr/bin/clang")
        );
        assert_eq!(std::fs::read_link(other_formula).unwrap(), unstable);
        assert_eq!(std::fs::read(regular_file).unwrap(), b"custom compiler");
    }

    #[test]
    fn homebrew_shim_target_requires_a_cellar_keg_with_a_live_opt_target() {
        let dir = tempfile::tempdir().unwrap();
        let missing_opt = dir.path().join("Cellar/kache/0.20.0/bin/kache");
        std::fs::create_dir_all(missing_opt.parent().unwrap()).unwrap();
        std::fs::write(&missing_opt, b"kache").unwrap();
        assert_eq!(
            super::shim_target_for_executable(missing_opt.clone()),
            std::fs::canonicalize(&missing_opt).unwrap()
        );

        let outside_cellar = dir.path().join("packages/kache/0.20.0/bin/kache");
        std::fs::create_dir_all(outside_cellar.parent().unwrap()).unwrap();
        std::fs::write(&outside_cellar, b"kache").unwrap();
        assert_eq!(
            super::shim_target_for_executable(outside_cellar.clone()),
            std::fs::canonicalize(&outside_cellar).unwrap()
        );
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
            SHIM_NAMES.len() + 1,
            "reinstall must not accumulate entries beyond the shims and the marker"
        );
    }

    /// Another kache install on PATH finds this farm by its marker, so it can
    /// skip the farm even when the shims are not symlinks to a `kache` file.
    #[test]
    fn install_marks_the_directory_it_populates() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        install_shims(&shims, false).unwrap();
        assert!(crate::compiler::shim::has_shim_marker(&shims));
    }

    /// A run that created nothing leaves the directory unmarked: every name
    /// there belongs to something else, and a marker would hide it.
    #[test]
    fn install_that_creates_nothing_does_not_mark_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        for name in SHIM_NAMES {
            std::fs::write(shims.join(name), b"a real compiler").unwrap();
        }
        install_shims(&shims, false).unwrap();
        assert!(!crate::compiler::shim::has_shim_marker(&shims));
    }

    /// A run that skips a real compiler must not mark the directory even when
    /// it created other links there. The marker would hide that compiler from
    /// every kache, so a shim elsewhere on PATH would run a different one.
    #[test]
    fn install_next_to_a_real_compiler_does_not_mark_the_directory() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        std::fs::create_dir_all(&shims).unwrap();
        let clang = shims.join("clang");
        std::fs::write(&clang, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&clang, std::fs::Permissions::from_mode(0o755)).unwrap();

        install_shims(&shims, false).unwrap();

        assert!(is_symlink(&shims.join("cc")), "free names are still linked");
        assert!(!crate::compiler::shim::has_shim_marker(&shims));
        let exe = std::env::current_exe().unwrap();
        assert_eq!(
            crate::compiler::shim::resolve_real_compiler_on(
                "clang",
                std::slice::from_ref(&shims),
                Some(&exe)
            ),
            Some(clang),
            "the real clang must stay visible to shim resolution"
        );
    }

    /// A farm made before the marker existed gets it on a rerun, even though
    /// every name is already taken, because every name is a kache link.
    #[test]
    fn reinstall_over_an_unmarked_farm_marks_it() {
        let dir = tempfile::tempdir().unwrap();
        let shims = dir.path().join("shims");
        install_shims(&shims, false).unwrap();
        std::fs::remove_file(shims.join(crate::compiler::shim::SHIM_DIR_MARKER)).unwrap();

        install_shims(&shims, false).unwrap();

        assert!(crate::compiler::shim::has_shim_marker(&shims));
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
