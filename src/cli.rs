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
#[allow(dead_code)]
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
                misses: 0,
                errors: 0,
                total_elapsed_ms: 0,
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
        }
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
            misses: es.misses,
            errors: es.errors,
            total_elapsed_ms: es.total_elapsed_ms,
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

    // Hit rate
    let es = &snap.event_stats;
    let total = es.local_hits + es.prefetch_hits + es.remote_hits + es.misses;
    let hit_rate = if total > 0 {
        ((es.local_hits + es.prefetch_hits + es.remote_hits) as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    println!(
        "Hit rate:   {hit_rate:.1}% (local: {}, prefetch: {}, remote: {}, miss: {})",
        es.local_hits, es.prefetch_hits, es.remote_hits, es.misses,
    );

    // Time saved — conservative: sum elapsed_ms for hit events only
    let time_saved = if es.total_elapsed_ms > 0 && total > 0 {
        let hits = (es.local_hits + es.prefetch_hits + es.remote_hits) as u64;
        let saved_ms = if total > 0 {
            (es.total_elapsed_ms as f64 * hits as f64 / total as f64) as u64
        } else {
            0
        };
        format_duration_ms(saved_ms)
    } else {
        "n/a".to_string()
    };
    println!("Time saved: {time_saved} (last {hours}h)");

    // Daemon status
    if snap.daemon_connected {
        let my_epoch = crate::daemon::build_epoch();
        let mismatch = if snap.daemon_build_epoch != my_epoch {
            " (MISMATCH — restart daemon)"
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
    } else {
        println!("Remote:     not configured");
    }

    Ok(())
}

// ── kache why-miss ─────────────────────────────────────────────────────────

/// Diagnose cache misses for a specific crate by inspecting the event log.
pub fn why_miss(config: &Config, crate_name: &str) -> Result<()> {
    let all_events = events::read_events(&config.event_log_path())?;
    let crate_events: Vec<_> = all_events
        .iter()
        .filter(|e| e.crate_name == crate_name)
        .collect();

    if crate_events.is_empty() {
        println!("No events found for '{crate_name}'.");
        println!("\nTip: Build the crate first, then re-run this command:");
        println!("  cargo build -p {crate_name}");
        return Ok(());
    }

    // Show last 5 events
    println!("Recent events for '{crate_name}':");
    let recent: Vec<_> = crate_events.iter().rev().take(5).collect();
    for event in recent.iter().rev() {
        let time = event.ts.format("%H:%M:%S");
        let key_short = if event.cache_key.len() > 16 {
            &event.cache_key[..16]
        } else {
            &event.cache_key
        };
        let elapsed = if event.elapsed_ms > 1000 {
            format!("{:.1}s", event.elapsed_ms as f64 / 1000.0)
        } else {
            format!("{}ms", event.elapsed_ms)
        };
        println!(
            "  [{time}] {:<12} key: {key_short}...  {elapsed}  {}",
            event.result.to_string(),
            ByteSize(event.size),
        );
    }

    // Find last hit and last miss to compare keys
    let last_hit = crate_events.iter().rev().find(|e| {
        matches!(
            e.result,
            events::EventResult::LocalHit | events::EventResult::RemoteHit
        )
    });
    let last_miss = crate_events
        .iter()
        .rev()
        .find(|e| matches!(e.result, events::EventResult::Miss));

    if let (Some(hit), Some(miss)) = (last_hit, last_miss)
        && hit.cache_key != miss.cache_key
    {
        println!("\nKey changed between last hit and last miss:");
        let hit_short = if hit.cache_key.len() > 16 {
            &hit.cache_key[..16]
        } else {
            &hit.cache_key
        };
        let miss_short = if miss.cache_key.len() > 16 {
            &miss.cache_key[..16]
        } else {
            &miss.cache_key
        };
        println!("  hit  key: {hit_short}...");
        println!("  miss key: {miss_short}...");
    }

    // Show meta.json details for the last miss
    if let Some(miss) = last_miss {
        if !miss.cache_key.is_empty() {
            println!(
                "\nLast miss key: {}...",
                &miss.cache_key[..16.min(miss.cache_key.len())]
            );
            println!("  elapsed: {}ms", miss.elapsed_ms);

            // Try to read meta.json from the store (it exists for miss events
            // only if the crate was subsequently compiled and stored)
            let meta_path = config.store_dir().join(&miss.cache_key).join("meta.json");
            if let Ok(content) = std::fs::read_to_string(&meta_path)
                && let Ok(meta) = serde_json::from_str::<crate::store::EntryMeta>(&content)
            {
                if !meta.target.is_empty() {
                    println!("  target:  {}", meta.target);
                }
                if !meta.features.is_empty() {
                    println!("  features: {}", meta.features.join(", "));
                }
                println!("  files: {}", meta.files.len());
            }
        }
    } else {
        println!("\nNo misses found for '{crate_name}' — all events are hits!");
        return Ok(());
    }

    println!("\nFor full key component details, run:");
    println!("  KACHE_LOG=trace cargo build -p {crate_name} 2>&1 | grep '\\[key:{crate_name}\\]'");

    Ok(())
}

fn format_duration_ms(ms: u64) -> String {
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

fn is_binary_artifact(path: &std::path::Path) -> bool {
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    match ext {
        "d" | "rmeta" | "rlib" => false,
        "" | "dylib" | "so" | "exe" | "dll" => true,
        _ => false,
    }
}

pub(crate) struct LinkStats {
    pub store_bytes: u64,
    pub linked_refs: u64,
    pub saved_bytes: u64,
}

/// Walk the store and compute hardlink statistics.
///
/// For each cached file, nlink > 1 means other hardlinks exist
/// (in target/ dirs). Each extra link saves one copy's worth of space.
pub(crate) fn compute_link_stats(store_dir: &std::path::Path) -> LinkStats {
    let mut stats = LinkStats {
        store_bytes: 0,
        linked_refs: 0,
        saved_bytes: 0,
    };

    let Ok(entries) = std::fs::read_dir(store_dir) else {
        return stats;
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        // Each subdirectory is a cache entry
        let Ok(files) = std::fs::read_dir(&path) else {
            continue;
        };

        for file in files.flatten() {
            let fpath = file.path();
            if !fpath.is_file() {
                continue;
            }
            // Skip meta.json — it's not a build artifact
            if fpath.file_name().map(|n| n == "meta.json").unwrap_or(false) {
                continue;
            }

            let Ok(meta) = std::fs::metadata(&fpath) else {
                continue;
            };

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
        Ok(evicted) => {
            if let Some(hours) = max_age_hours {
                println!(
                    "Evicted {} entries older than {hours}h.",
                    evicted.unwrap_or(0)
                );
            } else {
                println!(
                    "Evicted {} entries to stay under size limit.",
                    evicted.unwrap_or(0)
                );
            }
        }
        Err(e) => {
            println!("Daemon GC failed ({e}), running locally...");
            // Fallback: run GC directly
            let store = Store::open(config)?;
            let evicted = if let Some(hours) = max_age_hours {
                store.evict_older_than(hours)?
            } else {
                store.evict()?
            };
            println!("Evicted {evicted} entries.");
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
        for entry in &entries {
            if entry.crate_name == name {
                store.remove_entry(&entry.cache_key)?;
                removed += 1;
            }
        }
        println!("Removed {removed} entries for '{name}'.");
    } else {
        store.clear()?;
        println!("Cleared entire local store.");
    }

    Ok(())
}

/// Recursively find and remove target/ directories (TUI selector).
pub fn clean(dry_run: bool) -> Result<()> {
    use crossterm::ExecutableCommand;
    use crossterm::event::{self, Event, KeyCode, KeyEventKind};
    use crossterm::terminal::{
        EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    };
    use ratatui::prelude::*;
    use ratatui::widgets::*;
    use std::io::stdout;

    let root = std::env::current_dir()?;
    let mut targets: Vec<TargetEntry> = Vec::new();

    find_target_dirs(&root, &mut targets);

    if targets.is_empty() {
        println!("No target/ directories found.");
        return Ok(());
    }

    // Sort by size descending
    targets.sort_by(|a, b| b.size.cmp(&a.size));

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
        let selected_size: u64 = targets
            .iter()
            .zip(selected.iter())
            .filter(|(_, s)| **s)
            .map(|(t, _)| t.size)
            .sum();
        let selected_count = selected.iter().filter(|s| **s).count();
        let total_size: u64 = targets.iter().map(|t| t.size).sum();
        let total_cached: u64 = targets.iter().map(|t| t.cached_bytes).sum();

        terminal.draw(|frame| {
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
                    let rel = t.path.strip_prefix(&root).unwrap_or(&t.path);
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

            let table = Table::new(rows, widths)
                .block(Block::bordered().title(" Select directories to remove "));
            frame.render_widget(table, chunks[1]);

            // Detail panel — breakdown for cursor row
            let current = &targets[cursor];
            let b = &current.breakdown;
            let rel = current.path.strip_prefix(&root).unwrap_or(&current.path);
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
        })?;

        if event::poll(std::time::Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break None,
                KeyCode::Up => {
                    cursor = cursor.saturating_sub(1);
                }
                KeyCode::Down => {
                    if cursor + 1 < targets.len() {
                        cursor += 1;
                    }
                }
                KeyCode::Char(' ') => {
                    selected[cursor] = !selected[cursor];
                    if cursor + 1 < targets.len() {
                        cursor += 1;
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
                KeyCode::Enter => {
                    let to_remove: Vec<_> = targets
                        .iter()
                        .zip(selected.iter())
                        .filter(|(_, s)| **s)
                        .map(|(t, _)| (t.path.clone(), t.size))
                        .collect();
                    break Some(to_remove);
                }
                _ => {}
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

        if name_str == "Cargo.toml" && entry.path().is_file() {
            has_cargo_toml = true;
        }

        if entry.path().is_dir() {
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

/// Check environment for sccache and configuration issues.
/// When `fix` is true, also run the sccache→kache migration after diagnostics.
pub fn doctor(fix: bool, purge_sccache: bool) -> Result<()> {
    let home = dirs::home_dir().unwrap_or_default();
    let config = crate::config::Config::load().ok();

    struct Check {
        label: &'static str,
        pass: bool,
        detail: String,
        fix: Option<String>,
    }

    let mut checks: Vec<Check> = Vec::new();

    // 1. Binary on PATH
    let (bin_pass, bin_detail) = if let Ok(output) =
        std::process::Command::new("which").arg("kache").output()
        && output.status.success()
    {
        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
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
    let (wrapper_pass, wrapper_detail, wrapper_fix) = match std::env::var("RUSTC_WRAPPER") {
        Ok(v) if v.contains("kache") => (true, "kache".into(), None),
        Ok(v) if v.contains("sccache") => (
            false,
            format!("sccache ({v})"),
            Some("export RUSTC_WRAPPER=kache".into()),
        ),
        Ok(v) => (
            false,
            format!("{v} (not kache)"),
            Some("export RUSTC_WRAPPER=kache".into()),
        ),
        Err(_) => (
            false,
            "not set".into(),
            Some("add `export RUSTC_WRAPPER=kache` to ~/.zshrc".into()),
        ),
    };
    checks.push(Check {
        label: "RUSTC_WRAPPER",
        pass: wrapper_pass,
        detail: wrapper_detail,
        fix: wrapper_fix,
    });

    // 3. Cargo config
    let mut cargo_pass = true;
    let mut cargo_detail = "no conflicts".to_string();
    let mut cargo_fix = None;
    for name in ["config.toml", "config"] {
        let cargo_config = home.join(".cargo").join(name);
        if let Ok(content) = std::fs::read_to_string(&cargo_config) {
            if content.contains("sccache") {
                cargo_pass = false;
                cargo_detail = format!("sccache in {}", cargo_config.display());
                cargo_fix = Some(format!(
                    "replace `rustc-wrapper = \"sccache\"` with `rustc-wrapper = \"kache\"` in {}",
                    cargo_config.display()
                ));
            } else if content.contains("kache") {
                cargo_detail = format!("kache in {}", cargo_config.display());
            }
        }
    }
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
    }

    // 6. Shell rc sccache remnants
    let mut rc_issues = Vec::new();
    for rc in [".zshrc", ".bashrc", ".bash_profile", ".profile"] {
        let rc_path = home.join(rc);
        if let Ok(content) = std::fs::read_to_string(&rc_path)
            && content.contains("sccache")
        {
            let has_active = content
                .lines()
                .any(|l| l.contains("sccache") && !l.trim_start().starts_with('#'));
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
        checks.push(Check {
            label: "sccache",
            pass: false,
            detail: "daemon is running".into(),
            fix: Some("sccache --stop-server".into()),
        });
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
    let client = crate::remote::create_s3_client(remote)
        .await
        .context("connecting to S3 — check credentials and endpoint")?;

    // For pull: if we have Cargo.lock crate names and --all is not set,
    // use filtered listing (only crate-prefixed keys) for efficiency.
    let s3_keys = if !push_only {
        if !pull_all
            && let Some(crates) = lock_crates
            && !crates.is_empty()
        {
            eprint!("Listing S3 keys for {} crates...", crates.len());
            let keys = crate::remote::list_keys_for_crates(
                &client,
                &remote.bucket,
                &remote.prefix,
                crates,
            )
            .await
            .context("listing S3 keys for workspace crates")?;
            eprintln!(" {} keys", keys.len());
            keys
        } else {
            eprint!("Listing S3 keys...");
            let keys = crate::remote::list_keys(&client, &remote.bucket, &remote.prefix)
                .await
                .context("listing S3 keys")?;
            eprintln!(" {} keys", keys.len());
            keys
        }
    } else {
        // push-only mode: still need to list S3 keys to know what's already uploaded
        eprint!("Listing S3 keys...");
        let keys = crate::remote::list_keys(&client, &remote.bucket, &remote.prefix)
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
            let bucket = remote.bucket.clone();
            let prefix = remote.prefix.clone();
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

                match crate::remote::download_with_client(
                    &client,
                    &bucket,
                    &prefix,
                    &key,
                    &entry_dir,
                    &crate_name,
                )
                .await
                {
                    Ok(_bytes) => {
                        // Import into index — opens a fresh Store (cheap with WAL).
                        // INSERT OR REPLACE is idempotent if daemon also imported.
                        if let Ok(s) = Store::open(&cfg)
                            && let Err(e) = s.import_downloaded_entry(&key)
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
            let bucket = remote.bucket.clone();
            let prefix = remote.prefix.clone();
            let cfg = config.clone();
            let ok_ref = &ok;
            let fail_ref = &fail;

            in_flight.push(async move {
                let entry_dir = cfg.store_dir().join(&key);
                if !entry_dir.exists() {
                    // Entry disappeared (GC or purge) — skip
                    fail_ref.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return;
                }

                match crate::remote::upload_with_client(
                    &client,
                    &bucket,
                    &prefix,
                    &key,
                    &entry_dir,
                    cfg.compression_level,
                    &crate_name,
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
/// content-addressed shards to `{prefix}/_manifests/v2/{namespace}/shards/{hash}.json`.
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

    // Deduplicate by cache_key — keep the entry with the largest compile time
    // (a crate may appear multiple times if cargo invokes rustc with different flags)
    let mut by_key = std::collections::HashMap::<String, crate::remote::ManifestEntry>::new();
    for e in &events {
        if e.cache_key.is_empty() {
            continue;
        }
        match e.result {
            crate::events::EventResult::LocalHit
            | crate::events::EventResult::PrefetchHit
            | crate::events::EventResult::RemoteHit
            | crate::events::EventResult::Miss => {}
            _ => continue,
        }
        let entry = crate::remote::ManifestEntry {
            cache_key: e.cache_key.clone(),
            crate_name: e.crate_name.clone(),
            compile_time_ms: e.elapsed_ms,
            artifact_size: e.size,
        };
        by_key
            .entry(e.cache_key.clone())
            .and_modify(|existing| {
                if e.elapsed_ms > existing.compile_time_ms {
                    *existing = entry.clone();
                }
            })
            .or_insert(entry);
    }

    let entries: Vec<crate::remote::ManifestEntry> = by_key.into_values().collect();

    if entries.is_empty() {
        eprintln!("No build events found, skipping manifest save");
        return Ok(());
    }

    let key = manifest_key
        .map(String::from)
        .unwrap_or_else(default_manifest_key);

    let manifest = crate::remote::BuildManifest {
        version: 0, // v1 legacy format
        created: chrono::Utc::now().to_rfc3339(),
        manifest_key: key.clone(),
        entries: entries.clone(),
    };

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    rt.block_on(async {
        let client = crate::remote::create_s3_client(remote).await?;

        // Always upload the v1 legacy manifest
        crate::remote::upload_manifest(&client, &remote.bucket, &remote.prefix, &key, &manifest)
            .await?;

        // Upload v2 shards if namespace is provided and Cargo.lock exists
        if let Some(ns) = namespace {
            let lock_path = std::path::Path::new("Cargo.lock");
            if lock_path.exists() {
                let shard_count = upload_shards(
                    &client,
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
        }

        Ok::<(), anyhow::Error>(())
    })?;

    eprintln!("Saved manifest: {} entries for '{key}'", entries.len());
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
            version: 2,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

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
        let entry = dir.path().join("abc123");
        fs::create_dir(&entry).unwrap();
        fs::write(entry.join("lib.rlib"), vec![0u8; 500]).unwrap();
        fs::write(entry.join("meta.json"), "{}").unwrap(); // should be skipped

        let stats = compute_link_stats(dir.path());
        assert!(stats.store_bytes >= 500);
        // meta.json should be excluded
        assert!(stats.store_bytes < 600);
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
}
