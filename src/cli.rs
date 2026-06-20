use anyhow::{Context, Result};
use bytesize::ByteSize;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use crate::config::Config;
use crate::daemon;
use crate::events;
use crate::store::Store;

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
    pub downloads_completed: u64,
    pub downloads_failed: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub recent_transfers: Vec<daemon::TransferEvent>,
    pub blob_stats: Option<crate::store::BlobStats>,
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
            downloads_completed: 0,
            downloads_failed: 0,
            bytes_uploaded: 0,
            bytes_downloaded: 0,
            recent_transfers: Vec::new(),
            blob_stats: None,
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
pub(crate) fn fetch_stats_snapshot(
    config: &Config,
    include_entries: bool,
    sort_by: &str,
    hours: Option<u64>,
) -> StatsSnapshot {
    let event_hours = hours.or(Some(24));
    let blob_stats = || {
        Store::open(config)
            .ok()
            .and_then(|store| store.blob_stats().ok())
    };

    // Try daemon
    if let Ok(resp) =
        daemon::send_stats_request(config, include_entries, Some(sort_by), event_hours)
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
            downloads_completed: resp.downloads_completed,
            downloads_failed: resp.downloads_failed,
            bytes_uploaded: resp.bytes_uploaded,
            bytes_downloaded: resp.bytes_downloaded,
            recent_transfers: resp.recent_transfers,
            blob_stats: blob_stats(),
        };
    }

    // Daemon unreachable or stale socket: best-effort auto-start for monitor/stats UX.
    // This path is not used by compile-time hot operations.
    if daemon::start_daemon_background().unwrap_or(false)
        && let Ok(resp) =
            daemon::send_stats_request(config, include_entries, Some(sort_by), event_hours)
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
            downloads_completed: resp.downloads_completed,
            downloads_failed: resp.downloads_failed,
            bytes_uploaded: resp.bytes_uploaded,
            bytes_downloaded: resp.bytes_downloaded,
            recent_transfers: resp.recent_transfers,
            blob_stats: blob_stats(),
        };
    }

    // Fallback: direct reads
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

    let h = event_hours.unwrap_or(24);
    let since = chrono::Utc::now() - chrono::Duration::hours(h as i64);
    let event_list = events::read_events_since(&config.event_log_path(), since).unwrap_or_default();
    let es = events::compute_stats(&event_list);

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
        downloads_completed: 0,
        downloads_failed: 0,
        bytes_uploaded: 0,
        bytes_downloaded: 0,
        recent_transfers: Vec::new(),
        blob_stats: store.as_ref().and_then(|s| s.blob_stats().ok()),
    }
}

// ── kache stats ────────────────────────────────────────────────────────────

/// Print a one-shot stats summary to stdout.
pub fn stats(config: &Config, hours: Option<u64>) -> Result<()> {
    let hours = hours.unwrap_or(24);
    let snap = fetch_stats_snapshot(config, false, "size", Some(hours));

    // Store line
    let store_pct = if snap.max_size > 0 {
        (snap.total_size as f64 / snap.max_size as f64) * 100.0
    } else {
        0.0
    };
    println!(
        "Store:      {} / {} ({} entries, {:.0}%)",
        ByteSize(snap.total_size),
        ByteSize(snap.max_size),
        snap.entry_count,
        store_pct,
    );

    // Content dedup stats
    let store = Store::open(config)?;
    let blob_stats = store.blob_stats()?;
    if blob_stats.total_blobs > 0 {
        let savings_pct = if blob_stats.total_logical_size > 0 {
            blob_stats.savings as f64 / blob_stats.total_logical_size as f64 * 100.0
        } else {
            0.0
        };
        println!(
            "Dedup:      {} unique blobs, {} physical, {:.1}% savings",
            blob_stats.total_blobs,
            ByteSize(blob_stats.total_blob_size),
            savings_pct,
        );
    }

    // Hit rate
    let es = &snap.event_stats;
    let hit_rate = count_hit_rate(es);
    println!(
        "Hit rate:   {hit_rate:.1}% (local: {}, prefetch: {}, remote: {}, dup: {}, miss: {})",
        es.local_hits, es.prefetch_hits, es.remote_hits, es.dups, es.misses,
    );
    if let Some(weighted) = compile_weighted_hit_rate(es) {
        println!("Weighted:   {weighted:.1}% by compile cost");
    }
    if es.total_elapsed_ms > 0 {
        let miss_share = (es.miss_elapsed_ms as f64 / es.total_elapsed_ms as f64) * 100.0;
        println!(
            "Miss share: {:.1}% of wrapper time ({})",
            miss_share,
            format_duration_ms(es.miss_elapsed_ms)
        );
    }

    let time_saved = if es.hit_compile_time_ms > 0 {
        format_duration_ms(es.hit_compile_time_ms)
    } else {
        "n/a".to_string()
    };
    println!("Time saved: {time_saved} (estimated compile work avoided, last {hours}h)");

    // Daemon status
    if snap.daemon_connected {
        let my_epoch = crate::daemon::build_epoch();
        let mismatch = if snap.daemon_build_epoch != my_epoch {
            " (MISMATCH — auto-restart pending)"
        } else {
            ""
        };
        println!(
            "Daemon:     v{} (epoch {}){mismatch}",
            snap.daemon_version, snap.daemon_build_epoch,
        );
    } else {
        println!("Daemon:     offline");
    }

    // Remote
    if let Some(ref remote) = config.remote {
        let prefix = if remote.prefix.is_empty() {
            String::new()
        } else {
            format!("/{}", remote.prefix)
        };
        println!("Remote:     s3://{}{prefix}", remote.bucket);
    } else if config.local_only {
        println!("Remote:     local-only mode (remote + planner ignored)");
    } else {
        println!("Remote:     not configured");
    }

    Ok(())
}

// ── kache report ──────────────────────────────────────────────────────────

pub fn report(
    config: &Config,
    format: &str,
    hours: u64,
    root: Option<std::path::PathBuf>,
    output: Option<std::path::PathBuf>,
    top: usize,
) -> Result<()> {
    let report = if root.is_some() {
        let filter = crate::report::ReportFilter { root };
        crate::report::generate_report_with_filter(config, hours, top, &filter)?
    } else {
        crate::report::generate_report(config, hours, top)?
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
pub fn why_miss(config: &Config, crate_name: &str) -> Result<()> {
    let all_events = events::read_events(&config.event_log_path())?;
    let crate_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.crate_name == crate_name)
        .collect();

    if crate_events.is_empty() {
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

    // ── Header ─────────────────────────────────────────────────────────
    println!("Why `{crate_name}` missed:\n");

    let miss_time = miss.ts.format("%Y-%m-%dT%H:%M:%S");
    let miss_key_display = key_short(&miss.cache_key);
    println!(
        "  Last {}: {miss_time} (key: {miss_key_display})",
        miss.result
    );

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
        println!("  Diagnosis: never cached -- first build of this crate");
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

        if miss_key_stored && !other_entries.is_empty() {
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

    let mut diffs: Vec<String> = Vec::new();

    for entry in other_entries {
        let meta_path = store.entry_dir(&entry.cache_key).join("meta.json");
        let other_meta = std::fs::read_to_string(&meta_path)
            .ok()
            .and_then(|c| serde_json::from_str::<crate::store::EntryMeta>(&c).ok());

        let Some(other) = other_meta else {
            continue;
        };

        let ek = key_short(&entry.cache_key);

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

        // If target, profile, features, and crate_types all match,
        // the difference is likely source code changes, dependency updates,
        // or rustc version.
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

    if !diffs.is_empty() {
        // Deduplicate diff messages and cap output
        let mut unique_diffs: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for diff in &diffs {
            // Normalize: strip the key prefix to group identical diagnoses
            let normalized = if let Some(pos) = diff.find(" -- ") {
                diff[pos..].to_string()
            } else {
                diff.clone()
            };
            if seen.insert(normalized) {
                unique_diffs.push(diff.clone());
            }
        }
        println!("  Differences detected:");
        for diff in unique_diffs.iter().take(5) {
            println!("    - {diff}");
        }
        if unique_diffs.len() > 5 {
            println!("    ... and {} more", unique_diffs.len() - 5);
        }
    }
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

// ── Project stats ──────────────────────────────────────────────────────────

struct ProjectStats {
    total_bytes: u64,
    cached_bytes: u64,
    #[allow(dead_code)] // tracked but not yet surfaced in the clean TUI
    cached_files: u64,
    local_bytes: u64,
    local_files: u64,
}

/// Analyze a project's target/ directory: which files are hardlinked from
/// kache's cache (nlink > 1) vs local-only (nlink == 1), with per-category breakdown.
fn compute_project_stats(target_dir: &std::path::Path) -> (ProjectStats, CategoryBreakdown) {
    let mut stats = ProjectStats {
        total_bytes: 0,
        cached_bytes: 0,
        cached_files: 0,
        local_bytes: 0,
        local_files: 0,
    };
    let mut breakdown = CategoryBreakdown::default();

    let profiles = ["debug", "release", "profiling", "coverage"];
    for profile in &profiles {
        let profile_dir = target_dir.join(profile);
        if !profile_dir.is_dir() {
            continue;
        }
        let Ok(entries) = std::fs::read_dir(&profile_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if path.is_dir() {
                match name_str.as_ref() {
                    "incremental" => {
                        let size = dir_size(&path);
                        breakdown.incremental += size;
                        stats.total_bytes += size;
                        stats.local_bytes += size;
                    }
                    ".fingerprint" => {
                        let size = dir_size(&path);
                        breakdown.fingerprints += size;
                        stats.total_bytes += size;
                        stats.local_bytes += size;
                    }
                    "build" => {
                        let size = dir_size(&path);
                        breakdown.build_scripts += size;
                        stats.total_bytes += size;
                        stats.local_bytes += size;
                    }
                    "deps" => {
                        walk_deps_dir(&path, &mut stats, &mut breakdown);
                    }
                    _ => {
                        let size = dir_size(&path);
                        breakdown.other += size;
                        stats.total_bytes += size;
                        stats.local_bytes += size;
                    }
                }
            } else {
                let Ok(meta) = std::fs::metadata(&path) else {
                    continue;
                };
                let size = meta.len();
                stats.total_bytes += size;

                if is_binary_artifact(&path) {
                    breakdown.binaries += size;
                    stats.local_bytes += size;
                    stats.local_files += 1;
                } else {
                    #[cfg(unix)]
                    {
                        if meta.nlink() > 1 {
                            stats.cached_bytes += size;
                            stats.cached_files += 1;
                        } else {
                            breakdown.other += size;
                            stats.local_bytes += size;
                            stats.local_files += 1;
                        }
                    }
                    #[cfg(not(unix))]
                    {
                        breakdown.other += size;
                        stats.local_bytes += size;
                        stats.local_files += 1;
                    }
                }
            }
        }
    }

    // Files directly in target/ (CACHEDIR.TAG, .rustc_info.json, etc.)
    if let Ok(entries) = std::fs::read_dir(target_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file()
                && let Ok(meta) = std::fs::metadata(&path)
            {
                breakdown.other += meta.len();
                stats.total_bytes += meta.len();
                stats.local_bytes += meta.len();
                stats.local_files += 1;
            }
        }
    }

    (stats, breakdown)
}

fn walk_deps_dir(
    dir: &std::path::Path,
    stats: &mut ProjectStats,
    breakdown: &mut CategoryBreakdown,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_deps_dir(&path, stats, breakdown);
            continue;
        }
        let Ok(meta) = std::fs::metadata(&path) else {
            continue;
        };
        let size = meta.len();
        stats.total_bytes += size;

        #[cfg(unix)]
        {
            if meta.nlink() > 1 {
                stats.cached_bytes += size;
                stats.cached_files += 1;
            } else {
                breakdown.deps_local += size;
                stats.local_bytes += size;
                stats.local_files += 1;
            }
        }
        #[cfg(not(unix))]
        {
            breakdown.deps_local += size;
            stats.local_bytes += size;
            stats.local_files += 1;
        }
    }
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

pub(crate) struct LinkStats {
    pub store_bytes: u64,
    pub linked_refs: u64,
    pub saved_bytes: u64,
}

/// Walk the blob store and compute hardlink statistics.
///
/// Blobs live in `store_dir/blobs/{shard}/{hash}`. For each blob,
/// nlink > 1 means hardlinks exist in target/ dirs, saving space.
pub(crate) fn compute_link_stats(store_dir: &std::path::Path) -> LinkStats {
    let mut stats = LinkStats {
        store_bytes: 0,
        linked_refs: 0,
        saved_bytes: 0,
    };

    let blobs_dir = store_dir.join("blobs");
    let Ok(shards) = std::fs::read_dir(&blobs_dir) else {
        return stats;
    };

    for shard in shards.flatten() {
        let Ok(file_type) = shard.file_type() else {
            continue;
        };
        if !file_type.is_dir() {
            continue;
        }

        let Ok(blobs) = std::fs::read_dir(shard.path()) else {
            continue;
        };

        for blob in blobs.flatten() {
            let Ok(meta) = blob.metadata() else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }

            let size = meta.len();
            stats.store_bytes += size;

            #[cfg(unix)]
            {
                let nlink = meta.nlink();
                if nlink > 1 {
                    let extra = nlink - 1;
                    stats.linked_refs += extra;
                    stats.saved_bytes += size * extra;
                }
            }
        }
    }

    stats
}

/// List all cached entries, or show details for a specific crate.
pub fn list(config: &Config, crate_name: Option<&str>, sort_by: &str) -> Result<()> {
    let store = Store::open(config)?;

    if let Some(name) = crate_name {
        // Detail view for a specific crate
        let entries = store.list_entries("name")?;
        let matching: Vec<_> = entries.iter().filter(|e| e.crate_name == name).collect();

        if matching.is_empty() {
            println!("No cached entries for '{name}'.");
            return Ok(());
        }

        for entry in &matching {
            println!("Cache key: {}", &entry.cache_key[..16]);
            println!("  Crate:    {}", entry.crate_name);
            if !entry.crate_type.is_empty() {
                println!("  Type:     {}", entry.crate_type);
            }
            if !entry.profile.is_empty() {
                println!("  Profile:  {}", entry.profile);
            }
            println!("  Size:     {}", ByteSize(entry.size));
            println!("  Hits:     {}", entry.hit_count);
            println!("  Created:  {}", entry.created_at);
            println!("  Accessed: {}", entry.last_accessed);

            let meta_path = store.entry_dir(&entry.cache_key).join("meta.json");
            if let Ok(content) = std::fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<crate::store::EntryMeta>(&content)
            {
                if !meta.features.is_empty() {
                    println!("  Features: {}", meta.features.join(", "));
                }
                if !meta.target.is_empty() {
                    println!("  Target:   {}", meta.target);
                }
                println!("  Files:");
                for file in &meta.files {
                    println!("    {} ({})", file.name, ByteSize(file.size));
                }
            }
            println!();
        }
    } else {
        // Summary view of all entries
        let entries = store.list_entries(sort_by)?;

        if entries.is_empty() {
            println!("No cached entries.");
            return Ok(());
        }

        println!(
            "{:<30} {:<10} {:<8} {:>10} {:>6} {:>12} {:>12}",
            "Crate", "Type", "Profile", "Size", "Hits", "Created", "Accessed"
        );
        println!("{}", "-".repeat(92));

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
            println!(
                "{:<30} {:<10} {:<8} {:>10} {:>6} {:>12} {:>12}",
                entry.crate_name,
                crate_type,
                profile,
                ByteSize(entry.size).to_string(),
                entry.hit_count,
                &entry.created_at[..10],
                &entry.last_accessed[..10],
            );
        }

        println!("\n{} entries", entries.len());
    }

    Ok(())
}

/// Run garbage collection via the daemon.
pub fn gc(config: &Config, max_age_hours: Option<u64>) -> Result<()> {
    match crate::daemon::send_gc_request(config, max_age_hours) {
        Ok(outcome) if outcome.skipped => {
            println!("Another GC is already running; skipping.");
        }
        Ok(outcome) => {
            if let Some(hours) = max_age_hours {
                println!(
                    "Evicted {} entries older than {hours}h.",
                    outcome.evicted.unwrap_or(0)
                );
            } else {
                println!(
                    "Evicted {} entries to stay under size limit.",
                    outcome.evicted.unwrap_or(0)
                );
            }
        }
        Err(e) => {
            println!("Daemon GC failed ({e}), running locally...");
            let store = Store::open(config)?;

            // Cross-process GC mutual exclusion (kunobi-ninja/kache#326): bail if
            // another GC (e.g. a live daemon's sweep) already holds gc.lock.
            let _gc_lock = match store.try_gc_lock()? {
                Some(lock) => lock,
                None => {
                    println!("Another GC is already running; skipping.");
                    return Ok(());
                }
            };

            // Backfill content hashes for legacy entries
            print!("Backfilling content hashes...");
            std::io::Write::flush(&mut std::io::stdout()).ok();
            let backfilled = store.backfill_content_hashes().unwrap_or(0);
            if backfilled > 0 {
                println!(" {backfilled} entries updated.");
            } else {
                println!(" up to date.");
            }

            // Evict duplicate entries
            print!("Deduplicating entries...");
            std::io::Write::flush(&mut std::io::stdout()).ok();
            let dedup_stats = store.evict_duplicate_entries().unwrap_or_default();
            if dedup_stats.entries_evicted > 0 {
                println!(" removed {} duplicates.", dedup_stats.entries_evicted);
            } else {
                println!(" no duplicates found.");
            }

            // Size/age-based eviction
            print!("Running eviction...");
            std::io::Write::flush(&mut std::io::stdout()).ok();
            let evict_stats = if let Some(hours) = max_age_hours {
                store.evict_older_than(hours)?
            } else {
                store.evict()?
            };
            println!(" evicted {} entries.", evict_stats.entries_evicted);
        }
    }

    let store = Store::open(config)?;
    let total_size = store.total_size()?;
    let entry_count = store.entry_count()?;
    println!("Store: {} ({} entries)", ByteSize(total_size), entry_count);

    Ok(())
}

/// Wipe the entire cache or entries for a specific crate.
pub fn purge(config: &Config, crate_filter: Option<&str>) -> Result<()> {
    let store = Store::open(config)?;

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

    let selected_size: u64 = targets
        .iter()
        .zip(selected.iter())
        .filter(|(_, s)| **s)
        .map(|(t, _)| t.size)
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
        " {} dirs ({} total, {} cached)    Selected: {} ({})",
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

/// Recursively find and remove target/ directories (TUI selector).
pub fn clean(dry_run: bool) -> Result<()> {
    use crossterm::ExecutableCommand;
    use crossterm::event::{self, Event, KeyEventKind};
    use crossterm::terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    };
    use ratatui::prelude::*;
    use std::io::stdout;

    let root = std::env::current_dir()?;
    let mut targets: Vec<TargetEntry> = Vec::new();

    find_target_dirs(&root, &mut targets);

    if targets.is_empty() {
        println!("No target/ directories found.");
        return Ok(());
    }

    // Sort by size descending
    targets.sort_by_key(|entry| std::cmp::Reverse(entry.size));

    if dry_run {
        // Non-interactive dry run — just print
        let total_size: u64 = targets.iter().map(|t| t.size).sum();
        let total_cached: u64 = targets.iter().map(|t| t.cached_bytes).sum();
        println!(
            "Found {} target/ director{} ({} total, {} cached)\n",
            targets.len(),
            if targets.len() == 1 { "y" } else { "ies" },
            ByteSize(total_size),
            ByteSize(total_cached),
        );
        let max_path = targets
            .iter()
            .map(|t| {
                let rel = t.path.strip_prefix(&root).unwrap_or(&t.path);
                format!("{}", rel.display()).len()
            })
            .max()
            .unwrap_or(40);
        let w = max_path.max(10);

        for t in &targets {
            let rel = t.path.strip_prefix(&root).unwrap_or(&t.path);
            let profile_str = if t.profiles.is_empty() {
                String::new()
            } else {
                format!("  [{}]", t.profiles.join(", "))
            };
            println!(
                "  {:<w$}  {:>10}  cached: {:>10}{profile_str}",
                rel.display(),
                ByteSize(t.size),
                ByteSize(t.cached_bytes)
            );
        }
        println!("\nDry run: would free {}", ByteSize(total_size));
        return Ok(());
    }

    // TUI mode — interactive selection
    let mut selected: Vec<bool> = vec![false; targets.len()];
    let mut cursor: usize = 0;

    enable_raw_mode()?;
    stdout().execute(EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;

    let result = loop {
        terminal.draw(|frame| draw_clean(frame, &targets, &selected, cursor, &root))?;

        if event::poll(std::time::Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match clean_handle_key(key.code, &mut selected, &mut cursor, targets.len()) {
                CleanStep::Cancel => break None,
                CleanStep::Confirm => {
                    let to_remove: Vec<_> = targets
                        .iter()
                        .zip(selected.iter())
                        .filter(|(_, s)| **s)
                        .map(|(t, _)| (t.path.clone(), t.size))
                        .collect();
                    break Some(to_remove);
                }
                CleanStep::Continue => {}
            }
        }
    };

    disable_raw_mode()?;
    stdout().execute(LeaveAlternateScreen)?;

    // Process deletions outside TUI
    match result {
        None => {
            println!("Cancelled.");
        }
        Some(to_remove) if to_remove.is_empty() => {
            println!("Nothing selected.");
        }
        Some(to_remove) => {
            let mut freed = 0u64;
            let mut removed = 0usize;
            for (path, size) in &to_remove {
                let rel = path.strip_prefix(&root).unwrap_or(path);
                match std::fs::remove_dir_all(path) {
                    Ok(()) => {
                        freed += size;
                        removed += 1;
                        println!("  removed {}", rel.display());
                    }
                    Err(e) => {
                        println!("  failed  {} — {e}", rel.display());
                    }
                }
            }
            println!(
                "\nRemoved {removed} target/ dirs, freed {}",
                ByteSize(freed)
            );
        }
    }

    Ok(())
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
    pub profiles: Vec<String>,
    pub breakdown: CategoryBreakdown,
    /// Marked true when a rescan starts; cleared when fresh data arrives.
    pub stale: bool,
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

    if has_cargo_toml && let Some(target) = subdirs.iter().find(|(n, _)| n == "target") {
        let (ps, breakdown) = compute_project_stats(&target.1);
        if ps.total_bytes > 0 {
            let profiles = detect_profiles(&target.1);
            results.push(TargetEntry {
                path: target.1.clone(),
                size: ps.total_bytes,
                cached_bytes: ps.cached_bytes,
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
        if p.is_dir() {
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
pub fn doctor(
    fix: bool,
    purge_sccache: bool,
    verify: bool,
    checksums: bool,
    repair: bool,
) -> Result<()> {
    let home = dirs::home_dir().unwrap_or_default();
    let config = crate::config::Config::load().ok();
    let sccache_is_fallback = fallback_is_sccache(config.as_ref());

    struct Check {
        label: &'static str,
        pass: bool,
        detail: String,
        fix: Option<String>,
    }

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
            Some("cargo install --path . or add ~/.cargo/bin to PATH".into())
        },
    });

    // 2. RUSTC_WRAPPER
    let (wrapper_pass, wrapper_detail, wrapper_fix) = match crate::wrapper_config::resolve_wrapper_setting() {
        Some(crate::wrapper_config::WrapperSetting::Environment { value }) if value.contains("kache") => {
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
            Some("set `build.rustc-wrapper = \"kache\"` in ~/.cargo/config.toml or export RUSTC_WRAPPER=kache".into()),
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
            detail: format!("s3://{}", remote.bucket),
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
    let my_version = crate::VERSION;
    if let Some(ref cfg) = config {
        match crate::daemon::send_stats_request(cfg, false, None, None) {
            Ok(stats) => {
                let my_epoch = crate::daemon::build_epoch();
                let version_match = stats.version == my_version && stats.build_epoch == my_epoch;
                checks.push(Check {
                    label: "Daemon version",
                    pass: version_match,
                    detail: if version_match {
                        format!("v{} (epoch {})", stats.version, stats.build_epoch)
                    } else {
                        format!(
                            "daemon v{} (epoch {}) vs binary v{} (epoch {})",
                            stats.version, stats.build_epoch, my_version, my_epoch
                        )
                    },
                    fix: if version_match {
                        None
                    } else {
                        Some("kache daemon stop && kache daemon start (or just run a build — auto-restart will handle it)".into())
                    },
                });
            }
            Err(_) => {
                checks.push(Check {
                    label: "Daemon version",
                    pass: false,
                    detail: "daemon not reachable".into(),
                    fix: Some(
                        "start daemon with `kache daemon start` or `kache daemon install`".into(),
                    ),
                });
            }
        }
    }

    // 9. Daemon service installed
    if let Some(service_path) = crate::service::service_file_path() {
        let installed = service_path.exists();
        checks.push(Check {
            label: "Daemon service",
            pass: installed,
            detail: if installed {
                service_path.display().to_string()
            } else {
                "not installed".into()
            },
            fix: if installed {
                None
            } else {
                Some("kache daemon install".into())
            },
        });
    }

    // 10. Lingering live kache daemon processes — if the socket isn't reachable
    //     but `kache daemon run` processes exist, something got stuck.
    //     `kache daemon restart` now force-recovers this automatically.
    if let Some(ref cfg) = config {
        let reachable = crate::daemon::send_stats_request(cfg, false, None, None).is_ok();
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
    if let Some(ref cfg) = config {
        let sock = cfg.socket_path();
        let mut stale_files = Vec::new();
        for ext in ["lock", "run.lock"] {
            let p = sock.with_extension(ext);
            if p.exists() {
                stale_files.push(p);
            }
        }
        if !stale_files.is_empty()
            && crate::daemon::find_daemon_pids().is_empty()
            && crate::daemon::send_stats_request(cfg, false, None, None).is_err()
        {
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

    // extra_inputs warm-target coverage: a crate that declares extra_inputs but
    // whose build script won't re-trigger rustc gets stale artifacts when a
    // tracked file changes in a warm target. Only surfaced when at least one
    // crate in the tree declares extra_inputs (no noise for the common case).
    if let Ok(cwd) = std::env::current_dir() {
        let audit = crate::extra_inputs::audit_rerun_coverage(&cwd);
        if audit.declaring > 0 {
            if audit.gaps.is_empty() {
                checks.push(Check {
                    label: "extra_inputs",
                    pass: true,
                    detail: format!(
                        "{} crate(s), build scripts re-trigger rustc",
                        audit.declaring
                    ),
                    fix: None,
                });
            } else {
                let listed: Vec<String> = audit
                    .gaps
                    .iter()
                    .take(5)
                    .map(|g| {
                        let rel = g.crate_dir.strip_prefix(&cwd).unwrap_or(&g.crate_dir);
                        let shown = if rel.as_os_str().is_empty() {
                            ".".to_string()
                        } else {
                            rel.display().to_string()
                        };
                        format!("{shown} ({})", g.reason)
                    })
                    .collect();
                let more = audit.gaps.len().saturating_sub(listed.len());
                let mut detail = listed.join(", ");
                if more > 0 {
                    detail.push_str(&format!(", +{more} more"));
                }
                checks.push(Check {
                    label: "extra_inputs",
                    pass: false,
                    detail,
                    fix: Some(
                        "edits to these tracked files won't re-key in a warm target; emit \
                         cargo:rerun-if-changed for them from build.rs (see configuration docs)"
                            .into(),
                    ),
                });
            }
        }
    }

    // Print
    let version = crate::VERSION;
    let rustc_version = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!();
    println!("  kache v{version}    {rustc_version}");
    println!();

    let label_width = checks.iter().map(|c| c.label.len()).max().unwrap_or(0);

    for check in &checks {
        let icon = if check.pass {
            "\x1b[32m✓\x1b[0m"
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

    let issues = checks.iter().filter(|c| !c.pass).count();
    println!();
    if issues == 0 {
        println!("  \x1b[32mAll checks passed.\x1b[0m");
    } else {
        println!("  \x1b[31m{issues} issue(s) found.\x1b[0m");
    }
    println!();

    if fix {
        println!("Running migration...\n");
        migrate(purge_sccache)?;
    }

    // Cache integrity verification
    if verify {
        if let Some(ref cfg) = config {
            println!();
            self::verify(cfg, checksums, repair)?;
        } else {
            println!("  Cannot verify: no valid config found");
        }
    }

    Ok(())
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

    // 2. Replace sccache in ~/.cargo/config.toml
    for name in ["config.toml", "config"] {
        let cargo_config = home.join(".cargo").join(name);
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
        if let Ok(output) = std::process::Command::new("which").arg("sccache").output()
            && output.status.success()
        {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if path.contains(".cargo/bin") {
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

/// Synchronize local cache with S3 remote: pull missing artifacts, push new ones.
///
/// Works directly against S3 (no daemon required). Safe to run alongside the daemon —
/// downloads use atomic extraction, imports use INSERT OR REPLACE, and S3 PUTs are idempotent.
pub fn sync(
    config: &Config,
    manifest_path: Option<&str>,
    pull_only: bool,
    push_only: bool,
    dry_run: bool,
    pull_all: bool,
) -> Result<()> {
    let remote = config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No remote configured. Run `kache config` to set up S3."))?;

    let store = Store::open(config)?;
    let workspace_crates = workspace_filter(manifest_path);

    // For filtered pull: parse Cargo.lock to get all dependency crate names
    let lock_crates = if !pull_all && !push_only {
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
) -> Result<()> {
    let client = crate::remote::create_s3_client(remote, config.s3_pool_idle_secs)
        .await
        .context("connecting to S3 — check credentials and endpoint")?;
    sync_with_client(
        &client,
        config,
        store,
        remote,
        workspace_crates,
        pull_only,
        push_only,
        dry_run,
        pull_all,
        lock_crates,
    )
    .await
}

/// The S3-driven body of `sync`, with the client injected so tests can drive it
/// against a mock S3 server. Lists remote keys, diffs against the local store,
/// then (unless `dry_run`) pulls missing artifacts and pushes local-only ones.
#[allow(clippy::too_many_arguments)]
async fn sync_with_client(
    client: &aws_sdk_s3::Client,
    config: &Config,
    store: &Store,
    remote: &crate::config::RemoteConfig,
    workspace_crates: Option<&std::collections::HashSet<String>>,
    pull_only: bool,
    push_only: bool,
    dry_run: bool,
    pull_all: bool,
    lock_crates: Option<&std::collections::HashSet<String>>,
) -> Result<()> {
    let planner = crate::remote_plan::RemotePlanner::new(config);

    // For pull: if we have Cargo.lock crate names and --all is not set,
    // use filtered listing (only crate-prefixed keys) for efficiency.
    let s3_keys = if !push_only {
        if !pull_all
            && let Some(crates) = lock_crates
            && !crates.is_empty()
        {
            eprint!("Listing S3 keys for {} crates...", crates.len());
            let keys = planner
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(client, remote)
                .list_keys_for_crates(crates)
                .await
                .context("listing S3 keys for workspace crates")?;
            eprintln!(" {} keys", keys.len());
            keys
        } else {
            eprint!("Listing S3 keys...");
            let keys = planner
                .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
                .layout(client, remote)
                .list_keys()
                .await
                .context("listing S3 keys")?;
            eprintln!(" {} keys", keys.len());
            keys
        }
    } else {
        // push-only mode: still need to list S3 keys to know what's already uploaded
        eprint!("Listing S3 keys...");
        let keys = planner
            .plan(crate::remote_plan::RemoteWorkload::KeyDiscovery)
            .layout(client, remote)
            .list_keys()
            .await
            .context("listing S3 keys")?;
        eprintln!(" {} keys", keys.len());
        keys
    };

    let local_entries = store.list_entries("name")?;

    // to_pull: S3 keys not present on disk locally — (cache_key, crate_name).
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

    // to_push: local entries on disk but not in S3, filtered by workspace.
    // Includes (cache_key, crate_name) for crate-prefixed uploads.
    let to_push: Vec<(String, String)> = if !pull_only {
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

            let client = client.clone();
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
                    .layout(&client, &remote_cfg)
                    .download_entry(&key, &crate_name, &entry_dir, &blobs_dir)
                    .await;
                match result {
                    Ok(_bytes) => {
                        // Import into index — opens a fresh Store (cheap with WAL).
                        // INSERT OR REPLACE is idempotent if daemon also imported.
                        if let Ok(s) = Store::open(&cfg)
                            && let Err(e) = s.import_restored_entry(&key)
                        {
                            eprintln!("\n  warn: import {}...: {e}", &key[..16.min(key.len())]);
                        }
                        ok_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

            let client = client.clone();
            let remote_cfg = remote.clone();
            let cfg = config.clone();
            let upload_plan = planner.plan(crate::remote_plan::RemoteWorkload::SyncPush);
            let ok_ref = &ok;
            let fail_ref = &fail;

            in_flight.push(async move {
                let entry_dir = cfg.store_dir().join(&key);
                if !entry_dir.exists() {
                    // Entry disappeared (GC or purge) — skip
                    fail_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }

                let blobs_dir = cfg.store_dir().join("blobs");
                match upload_plan
                    .layout(&client, &remote_cfg)
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
        eprintln!(
            "\r  Uploaded:  {ok_count}/{total}{}",
            if fail_count > 0 {
                format!(" ({fail_count} failed)")
            } else {
                String::new()
            },
        );
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
    let remote = config
        .remote
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No remote configured"))?;

    let events = crate::events::read_events(&config.event_log_path())?;
    let entries = manifest_entries_from_events(&events);

    if entries.is_empty() {
        eprintln!("No build events found, skipping manifest save");
        return Ok(());
    }

    let key = manifest_key
        .map(String::from)
        .unwrap_or_else(default_manifest_key);
    let env_namespace = std::env::var("KACHE_NAMESPACE")
        .ok()
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
    rt.block_on(async {
        let client = crate::remote::create_s3_client(remote, pool_idle_secs).await?;
        upload_manifest_and_shards(
            &client,
            remote,
            &key,
            effective_namespace.as_deref(),
            std::path::Path::new("Cargo.lock"),
            entries,
        )
        .await
    })?;

    eprintln!("Saved manifest: {entry_count} entries for '{key}'");
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
) -> Vec<crate::remote::ManifestEntry> {
    let mut by_key = std::collections::HashMap::<String, crate::remote::ManifestEntry>::new();
    for e in events {
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
        by_key
            .entry(e.cache_key.clone())
            .and_modify(|existing| {
                if entry.compile_time_ms > existing.compile_time_ms {
                    *existing = entry.clone();
                }
            })
            .or_insert(entry);
    }
    by_key.into_values().collect()
}

/// Upload the monolithic build manifest and, when a namespace is given and a
/// `Cargo.lock` exists at `lock_path`, the content-addressed shard indexes.
///
/// Takes the S3 client by reference so tests can drive it against a mock S3
/// server (the production caller injects a real client from `create_s3_client`).
async fn upload_manifest_and_shards(
    client: &aws_sdk_s3::Client,
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
    crate::remote::upload_manifest(client, &remote.bucket, &remote.prefix, key, &manifest).await?;

    // Upload sharded build-manifest indexes if a namespace is provided and Cargo.lock exists.
    if let Some(ns) = namespace {
        if lock_path.exists() {
            let shard_count = upload_shards(
                client,
                &remote.bucket,
                &remote.prefix,
                ns,
                lock_path,
                &entries,
            )
            .await?;
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
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    namespace: &str,
    lock_path: &std::path::Path,
    entries: &[crate::remote::ManifestEntry],
) -> Result<usize> {
    let deps = crate::shards::parse_cargo_lock(lock_path)?;
    let shard_set = crate::shards::compute_shards(namespace, &deps);

    // Build a lookup from crate_name -> cache_key (keep the first match per crate)
    let mut crate_to_key = std::collections::HashMap::<&str, &str>::new();
    for e in entries {
        crate_to_key.entry(&e.crate_name).or_insert(&e.cache_key);
    }

    // Build Shard objects, skipping crates that have no build event
    let mut uploads = Vec::new();
    for (shard_hash, shard_deps) in &shard_set.shards {
        let shard_entries: Vec<crate::remote::ShardEntry> = shard_deps
            .iter()
            .filter_map(|(name, _version)| {
                crate_to_key
                    .get(name.as_str())
                    .map(|&cache_key| crate::remote::ShardEntry {
                        cache_key: cache_key.to_string(),
                        crate_name: name.clone(),
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
        let client = client.clone();
        let bucket = bucket.to_string();
        let prefix = prefix.to_string();
        let namespace = namespace.to_string();
        let permit = sem.clone().acquire_owned().await?;
        handles.push(tokio::spawn(async move {
            let result =
                crate::remote::upload_shard(&client, &bucket, &prefix, &namespace, &hash, &shard)
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

/// Default manifest key: host target triple at runtime.
pub(crate) fn default_manifest_key() -> String {
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    match os {
        "linux" => format!("{arch}-unknown-linux-gnu"),
        "macos" => format!("{arch}-apple-darwin"),
        "windows" => format!("{arch}-pc-windows-msvc"),
        _ => format!("{arch}-unknown-{os}"),
    }
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
    let lock_path = std::path::Path::new("Cargo.lock");
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
pub fn verify(config: &Config, checksums: bool, repair: bool) -> Result<()> {
    let store = Store::open(config)?;

    let entries = store.list_entries("name")?;
    let store_dir = config.store_dir();
    let blobs_dir = store_dir.join("blobs");

    let mut total_entries: usize = 0;
    let mut valid_entries: usize = 0;
    let mut corrupted_entries: usize = 0;
    let mut missing_blobs: usize = 0;
    let mut checksum_failures: usize = 0;
    let mut corrupted_keys: Vec<String> = Vec::new();

    // Track all blob hashes referenced by valid entries
    let mut referenced_blobs: std::collections::HashSet<String> = std::collections::HashSet::new();

    println!("Verifying {} cache entries...", entries.len());

    for entry in &entries {
        total_entries += 1;

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

            // Checksum verification
            if checksums {
                match std::fs::read(&blob_path) {
                    Ok(data) => {
                        let computed = blake3::hash(&data).to_hex().to_string();
                        if computed != cached_file.hash {
                            tracing::warn!(
                                "entry {} blob {} checksum mismatch (expected {}, got {})",
                                &entry.cache_key[..16.min(entry.cache_key.len())],
                                cached_file.name,
                                &cached_file.hash[..16.min(cached_file.hash.len())],
                                &computed[..16]
                            );
                            checksum_failures += 1;
                            entry_ok = false;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            "entry {} blob {} unreadable: {e}",
                            &entry.cache_key[..16.min(entry.cache_key.len())],
                            &cached_file.hash[..16.min(cached_file.hash.len())]
                        );
                        entry_ok = false;
                    }
                }
            }

            referenced_blobs.insert(cached_file.hash.clone());
        }

        if entry_ok {
            valid_entries += 1;
        } else {
            corrupted_entries += 1;
            corrupted_keys.push(entry.cache_key.clone());
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

    // Repair: remove corrupted entries
    if repair && !corrupted_keys.is_empty() {
        println!(
            "Repairing: removing {} corrupted entries...",
            corrupted_keys.len()
        );
        for key in &corrupted_keys {
            if let Err(e) = store.remove_entry(key) {
                tracing::warn!(
                    "failed to remove corrupted entry {}: {e}",
                    &key[..16.min(key.len())]
                );
            }
        }
    }

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

    // Compute store size
    let store_size = store.total_size().unwrap_or(0);

    println!();
    println!("Cache verification complete");
    println!(
        "  Entries: {} total, {} valid, {} corrupted",
        total_entries, valid_entries, corrupted_entries
    );
    println!(
        "  Blobs: {} total, {} orphaned, {} missing, {} checksum failures",
        total_blobs_on_disk, orphaned_blobs, missing_blobs, checksum_failures
    );
    println!("  Store size: {}", ByteSize(store_size));

    if (corrupted_entries > 0 || orphaned_blobs > 0) && !repair {
        println!();
        println!(
            "Tip: run `kache doctor --repair` to remove corrupted entries and reclaim orphaned blobs."
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
    fn test_compute_link_stats_empty() {
        let dir = tempfile::tempdir().unwrap();
        let stats = compute_link_stats(dir.path());
        assert_eq!(stats.store_bytes, 0);
        assert_eq!(stats.linked_refs, 0);
        assert_eq!(stats.saved_bytes, 0);
    }

    #[test]
    fn test_compute_link_stats_nonexistent() {
        let stats = compute_link_stats(std::path::Path::new("/nonexistent"));
        assert_eq!(stats.store_bytes, 0);
    }

    #[test]
    fn test_compute_link_stats_with_files() {
        let dir = tempfile::tempdir().unwrap();
        // Blobs live in blobs/{shard}/{hash}
        let shard = dir.path().join("blobs").join("ab");
        fs::create_dir_all(&shard).unwrap();
        fs::write(shard.join("abcdef1234567890"), vec![0u8; 500]).unwrap();
        fs::write(shard.join("abcdef9876543210"), vec![0u8; 300]).unwrap();

        let stats = compute_link_stats(dir.path());
        assert_eq!(stats.store_bytes, 800);
    }

    #[test]
    fn test_compute_project_stats_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let (stats, breakdown) = compute_project_stats(dir.path());
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.cached_bytes, 0);
        assert_eq!(breakdown.incremental, 0);
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

    #[test]
    fn test_parse_cargo_lock_crate_names_nonexistent() {
        // When Cargo.lock doesn't exist in cwd, should return None
        // We can't guarantee cwd lacks Cargo.lock, so just test the function doesn't panic
        let _ = parse_cargo_lock_crate_names();
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
    fn test_default_manifest_key_matches_host_triple_shape() {
        let key = default_manifest_key();
        assert!(key.starts_with(std::env::consts::ARCH), "got {key}");
        let expected_vendor_os = match std::env::consts::OS {
            "linux" => "-unknown-linux-gnu",
            "macos" => "-apple-darwin",
            "windows" => "-pc-windows-msvc",
            other => return assert!(key.contains(other)),
        };
        assert!(key.ends_with(expected_vendor_os), "got {key}");
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

    // ── upload_shards against a mock S3 ──────────────────────────────────────
    use aws_smithy_http_client::test_util::wire::{ReplayedEvent, WireMockServer};

    async fn mock_s3(events: Vec<ReplayedEvent>) -> (WireMockServer, aws_sdk_s3::Client) {
        let server = WireMockServer::start(events).await;
        let conf = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "AK", "SK", None, None, "test",
            ))
            .endpoint_url(server.endpoint_url())
            .http_client(server.http_client())
            .force_path_style(true)
            .build();
        (server, aws_sdk_s3::Client::from_conf(conf))
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

        // One OK per upload (over-provision is fine; each request consumes one).
        let events = std::iter::repeat_with(ReplayedEvent::ok)
            .take(expected + 2)
            .collect();
        let (server, client) = mock_s3(events).await;

        let uploaded = upload_shards(&client, "bucket", "prefix", "ns", &lock, &entries)
            .await
            .expect("upload_shards should succeed");
        assert_eq!(uploaded, expected);
        server.shutdown();
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

        let (server, client) = mock_s3(vec![]).await;
        let uploaded = upload_shards(&client, "bucket", "prefix", "ns", &lock, &[])
            .await
            .expect("should succeed with nothing to upload");
        assert_eq!(uploaded, 0);
        server.shutdown();
    }

    fn save_manifest_config(
        cache_dir: std::path::PathBuf,
        remote: Option<crate::config::RemoteConfig>,
    ) -> Config {
        use crate::config::{DEFAULT_DAEMON_IDLE_TIMEOUT_SECS, DEFAULT_S3_POOL_IDLE_SECS};
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            modified_input_guard: false,
            path_only_env_vars: Vec::new(),
            cache_dir,
            max_size: 1024 * 1024,
            remote,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            event_log_max_size: 1024 * 1024,
            event_log_keep_lines: 1000,
            compression_level: 3,
            s3_concurrency: 16,
            daemon_idle_timeout_secs: DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: DEFAULT_S3_POOL_IDLE_SECS,
        }
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
    fn save_manifest_with_no_events_returns_ok_before_touching_s3() {
        // A remote is configured, but the event log is empty, so save_manifest
        // returns Ok early ("No build events found") without creating an S3
        // client or making any network call.
        let dir = tempfile::tempdir().unwrap();
        let remote = crate::config::RemoteConfig {
            bucket: "b".to_string(),
            endpoint: Some("http://127.0.0.1:1".to_string()),
            region: "us-east-1".to_string(),
            prefix: "p".to_string(),
            profile: None,
        };
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
            root: String::new(),
            passthrough_reason: String::new(),
            fallback: false,
            exit_code: None,
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

        let mut entries = manifest_entries_from_events(&events);
        entries.sort_by(|a, b| a.crate_name.cmp(&b.crate_name));

        assert_eq!(entries.len(), 2, "only the two cacheable keys survive");
        let serde = entries.iter().find(|e| e.crate_name == "serde").unwrap();
        assert_eq!(serde.compile_time_ms, 900, "larger compile time wins");
        assert!(entries.iter().any(|e| e.crate_name == "tokio"));
    }

    #[test]
    fn manifest_entries_from_events_falls_back_to_elapsed_when_no_compile_time() {
        use crate::events::EventResult;
        // compile_time_ms == 0 -> the entry's compile_time_ms uses elapsed_ms.
        let events = vec![build_event("x", EventResult::Miss, 0, 77, 1, "k")];
        let entries = manifest_entries_from_events(&events);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].compile_time_ms, 77);
    }

    fn test_remote_cfg() -> crate::config::RemoteConfig {
        crate::config::RemoteConfig {
            bucket: "bucket".to_string(),
            endpoint: None,
            region: "us-east-1".to_string(),
            prefix: "prefix".to_string(),
            profile: None,
        }
    }

    #[tokio::test]
    async fn upload_manifest_and_shards_uploads_manifest_only_without_namespace() {
        // No namespace -> exactly one PutObject (the monolithic manifest).
        let (server, client) = mock_s3(vec![ReplayedEvent::ok()]).await;
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
        assert_eq!(server.events().len(), 3, "one request round-trip");
        server.shutdown();
    }

    #[tokio::test]
    async fn upload_manifest_and_shards_skips_shards_when_lock_missing() {
        // Namespace given but Cargo.lock absent -> still only the manifest PUT.
        let (server, client) = mock_s3(vec![ReplayedEvent::ok()]).await;
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
        assert_eq!(server.events().len(), 3, "manifest only; no shard PUTs");
        server.shutdown();
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

        // manifest PUT + one PUT per shard (over-provision OK).
        let events = std::iter::repeat_with(ReplayedEvent::ok)
            .take(expected_shards + 2)
            .collect();
        let (server, client) = mock_s3(events).await;
        let remote = test_remote_cfg();

        upload_manifest_and_shards(&client, &remote, "mykey", Some("ns"), &lock, entries)
            .await
            .expect("upload with shards should succeed");
        server.shutdown();
    }

    /// A `ListObjectsV2` response listing the given manifest object keys.
    fn list_bucket_xml(keys: &[&str]) -> String {
        let contents: String = keys
            .iter()
            .map(|k| {
                format!(
                    "<Contents><Key>{k}</Key><LastModified>2025-01-01T00:00:00.000Z</LastModified>\
                     <ETag>\"x\"</ETag><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>"
                )
            })
            .collect();
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Name>bucket</Name><Prefix>prefix/v3/manifests/</Prefix>\
             <KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>\
             {contents}</ListBucketResult>",
            keys.len()
        )
    }

    #[tokio::test]
    async fn sync_with_client_dry_run_empty_remote_reports_nothing() {
        // Empty remote + empty local store: the list returns no keys, so the
        // diff is empty and sync reports "Nothing to sync" (one list call).
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let (server, client) = mock_s3(vec![ReplayedEvent::with_body(list_bucket_xml(&[]))]).await;
        sync_with_client(
            &client, &config, &store, &remote, None, false, false, true, false, None,
        )
        .await
        .expect("dry-run sync over empty remote should succeed");
        server.shutdown();
    }

    #[tokio::test]
    async fn sync_with_client_push_uploads_local_only_entry() {
        // A populated local store + an empty remote: push-only sync uploads the
        // local entry end-to-end (real pack creation + manifest PUT) through the
        // mock. Exercises the push loop and upload_entry, not just planning.
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
        // list (empty remote) + PUTs for the pack and manifest (over-provisioned).
        let mut events = vec![ReplayedEvent::with_body(list_bucket_xml(&[]))];
        events.extend(std::iter::repeat_with(ReplayedEvent::ok).take(4));
        let (server, client) = mock_s3(events).await;

        sync_with_client(
            &client, &config, &store, &remote, None, false, true, false, false, None,
        )
        .await
        .expect("push sync should succeed");
        server.shutdown();
    }

    #[tokio::test]
    async fn sync_with_client_push_throttles_with_low_concurrency() {
        // Two local entries with s3_concurrency=1 force the push loop's
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
        // list (empty) + PUTs for two entries' pack+manifest (over-provisioned).
        let mut events = vec![ReplayedEvent::with_body(list_bucket_xml(&[]))];
        events.extend(std::iter::repeat_with(ReplayedEvent::ok).take(8));
        let (server, client) = mock_s3(events).await;

        sync_with_client(
            &client, &config, &store, &remote, None, false, true, false, false, None,
        )
        .await
        .expect("throttled push sync should succeed");
        server.shutdown();
    }

    #[tokio::test]
    async fn sync_with_client_dry_run_plans_pull_for_remote_only_key() {
        // The remote lists a manifest for a key absent from the local store, so
        // the dry-run plan schedules a pull and returns without transferring.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let xml = list_bucket_xml(&["prefix/v3/manifests/serde/abc123def456.json"]);
        let (server, client) = mock_s3(vec![ReplayedEvent::with_body(xml)]).await;
        sync_with_client(
            &client, &config, &store, &remote, None, false, false, true, false, None,
        )
        .await
        .expect("dry-run sync planning a pull should succeed");
        server.shutdown();
    }

    #[tokio::test]
    async fn sync_with_client_pull_loop_tolerates_a_failed_download() {
        // A remote-only key drives a real (non-dry-run) pull. The served pack is
        // garbage, so download_entry errors — the pull loop must record the
        // failure and still complete Ok (per-item errors don't abort the sync).
        // Exercises the pull orchestration + download path + error handling.
        let dir = tempfile::tempdir().unwrap();
        let config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let xml = list_bucket_xml(&["prefix/v3/manifests/serde/abc123def456.json"]);
        let mut events = vec![ReplayedEvent::with_body(xml)];
        // The pack GET returns non-zstd bytes -> download_entry fails.
        events.extend(
            std::iter::repeat_with(|| ReplayedEvent::with_body(b"not a valid pack")).take(4),
        );
        let (server, client) = mock_s3(events).await;

        sync_with_client(
            &client, &config, &store, &remote, None, true, false, false, false, None,
        )
        .await
        .expect("pull sync should complete Ok even when a download fails");
        server.shutdown();
    }

    #[tokio::test]
    async fn sync_with_client_push_reports_failed_uploads() {
        // A local-only entry is scheduled for push, but the upload PUTs get a
        // non-retryable 403, so upload_entry errors and the loop records a
        // failure. Covers the push error arm + the "fail_count > 0" upload
        // summary branch (cli.rs ~2651 + 2673-2682). Per-item errors don't
        // abort the sync, so it still returns Ok.
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

        // list (empty) then 403s for every upload PUT (over-provisioned).
        let mut events = vec![ReplayedEvent::with_body(list_bucket_xml(&[]))];
        events.extend(std::iter::repeat_with(|| ReplayedEvent::status(403)).take(6));
        let (server, client) = mock_s3(events).await;

        sync_with_client(
            &client, &config, &store, &remote, None, false, true, false, false, None,
        )
        .await
        .expect("push sync should complete Ok even when an upload fails");
        server.shutdown();
    }

    #[tokio::test]
    async fn sync_with_client_pull_throttles_with_low_concurrency() {
        // Two remote-only keys with s3_concurrency=1 force the pull loop's
        // max-concurrency wait branch (the second download waits for the first
        // to drain a slot). Packs are garbage so each download fails fast, but
        // the throttle path is still exercised; the sync completes Ok.
        let dir = tempfile::tempdir().unwrap();
        let mut config = save_manifest_config(dir.path().to_path_buf(), Some(test_remote_cfg()));
        config.s3_concurrency = 1;
        let store = Store::open(&config).unwrap();
        let remote = test_remote_cfg();

        let xml = list_bucket_xml(&[
            "prefix/v3/manifests/aaa/key1111111111aa.json",
            "prefix/v3/manifests/bbb/key2222222222bb.json",
        ]);
        let mut events = vec![ReplayedEvent::with_body(xml)];
        // Each download attempt makes several GETs; over-provision garbage bodies.
        events.extend(std::iter::repeat_with(|| ReplayedEvent::with_body(b"not a pack")).take(8));
        let (server, client) = mock_s3(events).await;

        sync_with_client(
            &client, &config, &store, &remote, None, true, false, false, false, None,
        )
        .await
        .expect("throttled pull sync should complete Ok");
        server.shutdown();
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

        let key = "abc123def456aaaa";
        let pack = build_entry_pack(key, "serde");

        let xml = list_bucket_xml(&["prefix/v3/manifests/serde/abc123def456aaaa.json"]);
        let events = vec![
            ReplayedEvent::with_body(xml),
            ReplayedEvent::with_body(&pack),
        ];
        let (server, client) = mock_s3(events).await;

        sync_with_client(
            &client, &config, &store, &remote, None, true, false, false, false, None,
        )
        .await
        .expect("pull sync should succeed");

        // The entry was imported into the local store.
        assert!(
            config.store_dir().join(key).join("meta.json").exists(),
            "pulled entry should be materialized in the local store"
        );
        server.shutdown();
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
                profiles: vec!["debug".to_string(), "release".to_string()],
                breakdown: CategoryBreakdown::default(),
                stale: false,
            },
            TargetEntry {
                path: std::path::PathBuf::from("/work/proj-b/target"),
                size: 2_000_000,
                cached_bytes: 0,
                profiles: vec![],
                breakdown: CategoryBreakdown::default(),
                stale: false,
            },
        ];
        // Second row selected, cursor on the first row.
        let selected = vec![false, true];

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
}

// ── Init ──────────────────────────────────────────────────────────────────
//
// Interactive setup that resolves the common doctor issues:
//   1. Writes `build.rustc-wrapper = "kache"` to ~/.cargo/config.toml
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

fn cargo_config_target_path() -> std::path::PathBuf {
    let home = dirs::home_dir().unwrap_or_default();
    let cargo_dir = home.join(".cargo");
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
            if !check && prompt_yes_no(&question, true, yes)? {
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
