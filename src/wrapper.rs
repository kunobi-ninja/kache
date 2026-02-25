use anyhow::{Context, Result};
use chrono::Utc;
use std::path::Path;

use crate::args::RustcArgs;
use crate::cache_key::compute_cache_key;
use crate::compile;
use crate::config::Config;
use crate::events::{self, BuildEvent, EventResult};
use crate::link::{self, DepInfoMode, LinkStrategy};
use crate::store::Store;

/// Run kache in RUSTC_WRAPPER mode.
///
/// This is the hot path — called once per crate by cargo.
/// Flow: parse args → compute cache key → check store → link on hit → compile on miss → store → link
pub fn run(config: &Config, wrapper_args: &[String]) -> Result<i32> {
    let start = std::time::Instant::now();

    // Parse the rustc arguments (wrapper_args[0] is the rustc path)
    let args = RustcArgs::parse(wrapper_args).context("parsing rustc arguments")?;

    // If this isn't a primary compilation (no source file), just pass through to rustc
    if !args.is_primary {
        return passthrough(&args);
    }

    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");

    // Check if caching should be skipped for this crate type
    if args.is_executable_output() && !config.cache_executables {
        tracing::debug!("skipping cache for executable output: {}", crate_name);
        log_event(config, crate_name, EventResult::Skipped, 0, 0, "");
        return passthrough(&args);
    }

    // Compute the cache key
    let cache_key = match compute_cache_key(&args) {
        Ok(key) => key,
        Err(e) => {
            tracing::warn!("failed to compute cache key for {}: {}", crate_name, e);
            return passthrough(&args);
        }
    };

    tracing::debug!("cache key for {}: {}", crate_name, &cache_key[..16]);

    // Open the store
    let store = match Store::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!("failed to open store: {}", e);
            return passthrough(&args);
        }
    };

    // 1. Check local store
    if let Some(meta) = store.get(&cache_key)? {
        // Safety: skip entries with no cached files (poisoned by earlier bugs)
        if meta.files.is_empty() {
            tracing::warn!(
                "cache entry for {} has no files, evicting and recompiling",
                crate_name
            );
            let _ = store.remove_entry(&cache_key);
        } else {
            tracing::debug!("local cache hit for {} ({})", crate_name, &cache_key[..16]);
            restore_from_cache(config, &store, &args, &meta)?;
            let elapsed = start.elapsed().as_millis() as u64;
            let size: u64 = meta.files.iter().map(|f| f.size).sum();
            log_event(
                config,
                crate_name,
                EventResult::LocalHit,
                elapsed,
                size,
                &cache_key,
            );
            // Print cached stdout/stderr
            if !meta.stdout.is_empty() {
                print!("{}", meta.stdout);
            }
            if !meta.stderr.is_empty() {
                eprint!("{}", meta.stderr);
            }
            return Ok(0);
        }
    }

    // Build-session detection: send prefetch hint before remote work.
    // Placed after local-hit check so warm-cache invocations skip this entirely.
    maybe_trigger_prefetch(config);

    // 2. Check remote cache via daemon (if configured)
    if config.remote.is_some() {
        let entry_dir = store.entry_dir(&cache_key);
        match crate::daemon::send_remote_check(config, &cache_key, &entry_dir, crate_name) {
            Some(result) if result.found => {
                // Daemon downloaded it — now read from local store and restore
                if let Some(meta) = store.get(&cache_key)? {
                    let event_result = if result.prefetched {
                        tracing::debug!(
                            "prefetch cache hit for {} ({})",
                            crate_name,
                            &cache_key[..16]
                        );
                        EventResult::PrefetchHit
                    } else {
                        tracing::debug!(
                            "remote cache hit for {} ({})",
                            crate_name,
                            &cache_key[..16]
                        );
                        EventResult::RemoteHit
                    };
                    restore_from_cache(config, &store, &args, &meta)?;
                    let elapsed = start.elapsed().as_millis() as u64;
                    let size: u64 = meta.files.iter().map(|f| f.size).sum();
                    log_event(config, crate_name, event_result, elapsed, size, &cache_key);
                    if !meta.stdout.is_empty() {
                        print!("{}", meta.stdout);
                    }
                    if !meta.stderr.is_empty() {
                        eprint!("{}", meta.stderr);
                    }
                    return Ok(0);
                }
            }
            Some(_) => {} // not in remote, continue to compile
            None => {}    // daemon unreachable, continue to compile
        }
    }

    // 3. Cache miss — try to acquire build lock
    let lock = match store.try_lock(&cache_key)? {
        Some(lock) => lock,
        None => {
            // Another process is building this key — wait for it
            tracing::debug!("waiting for {} to be built by another process", crate_name);
            if store.wait_for_committed(&cache_key)? {
                // It's now available
                if let Some(meta) = store.get(&cache_key)? {
                    restore_from_cache(config, &store, &args, &meta)?;
                    let elapsed = start.elapsed().as_millis() as u64;
                    let size: u64 = meta.files.iter().map(|f| f.size).sum();
                    log_event(
                        config,
                        crate_name,
                        EventResult::LocalHit,
                        elapsed,
                        size,
                        &cache_key,
                    );
                    return Ok(0);
                }
            }
            // If waiting failed, fall through to compile
            tracing::warn!("wait for {} failed, compiling ourselves", crate_name);
            // Compile without caching
            return passthrough(&args);
        }
    };

    // 4. Compile
    tracing::debug!(
        "cache miss for {}, compiling ({})",
        crate_name,
        &cache_key[..16]
    );
    let result = compile::run_rustc(
        &args.rustc,
        args.inner_rustc.as_deref(),
        &args.all_args,
        args.output.as_deref(),
        args.out_dir.as_deref(),
        args.crate_name.as_deref(),
        args.extra_filename.as_deref(),
        args.has_coverage_instrumentation(),
    )?;

    // Print rustc output
    if !result.stdout.is_empty() {
        print!("{}", result.stdout);
    }
    if !result.stderr.is_empty() {
        eprint!("{}", result.stderr);
    }

    // Don't cache failures
    if result.exit_code != 0 {
        let elapsed = start.elapsed().as_millis() as u64;
        log_event(
            config,
            crate_name,
            EventResult::Error,
            elapsed,
            0,
            &cache_key,
        );
        drop(lock);
        return Ok(result.exit_code);
    }

    // 5. Store the output files
    let target = args.target.as_deref().unwrap_or("host");
    let profile = match args.get_codegen_opt("opt-level") {
        Some("0") | None => "dev",
        Some("s") | Some("z") => "release-size",
        _ => "release",
    };

    if let Err(e) = store.put(
        &cache_key,
        crate_name,
        &args.crate_types,
        &args.features,
        target,
        profile,
        &result.output_files,
        &result.stdout,
        &result.stderr,
    ) {
        tracing::warn!("failed to store cache entry: {}", e);
    }

    // 6. Async upload to remote (if configured) — sends job to the daemon
    if config.remote.is_some() {
        let entry_dir = store.entry_dir(&cache_key);
        if let Err(e) = crate::daemon::send_upload_job(config, &cache_key, &entry_dir, crate_name) {
            tracing::warn!("failed to send upload job to daemon: {}", e);
        }
    }

    // 7. Clean incremental dir — with kache caching, incremental compilation is redundant
    if config.clean_incremental
        && let Some(incr_dir) = &args.incremental
        && incr_dir.is_dir()
        && let Err(e) = std::fs::remove_dir_all(incr_dir)
    {
        tracing::debug!(
            "failed to clean incremental dir {}: {}",
            incr_dir.display(),
            e
        );
    }

    let elapsed = start.elapsed().as_millis() as u64;
    let size: u64 = result
        .output_files
        .iter()
        .map(|(p, _)| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();
    log_event(
        config,
        crate_name,
        EventResult::Miss,
        elapsed,
        size,
        &cache_key,
    );

    drop(lock);
    Ok(result.exit_code)
}

/// Restore cached artifacts to the target output paths.
fn restore_from_cache(
    _config: &Config,
    store: &Store,
    args: &RustcArgs,
    meta: &crate::store::EntryMeta,
) -> Result<()> {
    // Determine where output files go: either -o parent dir, or --out-dir
    let output_dir = if let Some(output) = &args.output {
        output.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else if let Some(dir) = &args.out_dir {
        dir.clone()
    } else {
        anyhow::bail!("no output path (-o) or output directory (--out-dir) in args");
    };

    let strategy = if args.is_executable_output() {
        LinkStrategy::Copy
    } else {
        LinkStrategy::Hardlink
    };

    for cached_file in &meta.files {
        let store_path = store.cached_file_path(&meta.cache_key, &cached_file.name);

        // For -o mode, the primary output goes to the exact -o path;
        // for --out-dir mode, everything goes into the directory.
        let target_path = if let Some(output) = &args.output {
            if cached_file.name == output.file_name().unwrap_or_default().to_string_lossy() {
                output.clone()
            } else {
                output_dir.join(&cached_file.name)
            }
        } else {
            output_dir.join(&cached_file.name)
        };

        link::link_to_target(&store_path, &target_path, strategy).with_context(|| {
            format!(
                "linking {} -> {}",
                store_path.display(),
                target_path.display()
            )
        })?;

        // Update mtime so cargo doesn't think output is stale
        link::touch_mtime(&target_path)?;

        // Handle dep-info files: expand relative paths
        if cached_file.name.ends_with(".d")
            && let Ok(pwd) = std::env::current_dir()
        {
            let _ = link::rewrite_depinfo(&target_path, &pwd, DepInfoMode::Expand);
        }

        // macOS code signing for executables
        if args.is_executable_output() && !cached_file.name.ends_with(".d") {
            compile::codesign_adhoc(&target_path)?;
        }
    }

    Ok(())
}

/// Pass through to rustc without caching.
///
/// Even on the passthrough path, we strip incremental flags to prevent
/// APFS-related corruption in git worktrees on macOS.
fn passthrough(args: &RustcArgs) -> Result<i32> {
    let filtered = compile::strip_incremental_flags(&args.all_args);
    let stripped = args.all_args.len() - filtered.len();
    if stripped > 0 {
        tracing::info!(
            "[kache] passthrough: stripped {} incremental flag(s) for {}",
            stripped,
            args.crate_name.as_deref().unwrap_or("unknown")
        );
    }

    let mut cmd = std::process::Command::new(&args.rustc);
    cmd.env("CARGO_INCREMENTAL", "0");
    // Double-wrapper: pass the inner rustc path as first arg to the workspace wrapper
    if let Some(inner) = &args.inner_rustc {
        cmd.arg(inner);
    }
    cmd.args(&filtered);
    let status = cmd
        .status()
        .with_context(|| format!("executing {}", args.rustc.display()))?;
    Ok(status.code().unwrap_or(1))
}

/// Log a build event.
fn log_event(
    config: &Config,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    size: u64,
    cache_key: &str,
) {
    let event = BuildEvent {
        ts: Utc::now(),
        crate_name: crate_name.to_string(),
        version: crate::VERSION.to_string(),
        result,
        elapsed_ms,
        size,
        cache_key: cache_key.to_string(),
        schema: 1,
    };
    let _ = events::log_event(&config.event_log_path(), &event);
    let _ = events::rotate_if_needed(
        &config.event_log_path(),
        config.event_log_max_size,
        config.event_log_keep_lines,
    );
}

/// Check for a new build session and trigger a prefetch hint to the daemon.
/// Uses a marker file with flock to ensure only one wrapper process per
/// build session sends the hint — without this, N parallel rustc invocations
/// would all race past the mtime check and send duplicate prefetch requests.
fn maybe_trigger_prefetch(config: &Config) {
    if config.remote.is_none() {
        return;
    }

    let marker = config.cache_dir.join(".build-session");
    // 2 minutes: long enough to span gaps between crate compilations within
    // a single `cargo build`, short enough that a new `cargo test` after an
    // edit still triggers a fresh prefetch.
    let session_timeout = std::time::Duration::from_secs(120);

    // Fast non-blocking check: if the marker is fresh, skip entirely.
    if let Ok(meta) = std::fs::metadata(&marker)
        && let Ok(modified) = meta.modified()
        && modified.elapsed().unwrap_or(session_timeout) < session_timeout
    {
        return; // Still in the same build session
    }

    // Marker is stale or missing — try to acquire an exclusive lock so only
    // one process does the (expensive) cargo-metadata + daemon RPC.
    let _ = std::fs::create_dir_all(&config.cache_dir);
    let Ok(lock_file) = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&marker)
    else {
        return;
    };
    use std::os::unix::io::AsRawFd;
    let got_lock =
        unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0;
    if !got_lock {
        return; // Another wrapper is already sending the prefetch hint
    }

    // Re-check under the lock — another process may have updated the marker
    // between our first check and acquiring the lock.
    if let Ok(meta) = std::fs::metadata(&marker)
        && let Ok(modified) = meta.modified()
        && modified.elapsed().unwrap_or(session_timeout) < session_timeout
        // The file must have content (written by a previous winner); an empty
        // file means WE just created it above and should proceed.
        && meta.len() > 0
    {
        return;
    }

    // Touch the marker (write a byte so the re-check above can distinguish
    // "freshly locked but not yet written" from "already completed").
    let _ = std::fs::write(&marker, b"1");

    // Gather ALL dependency crate names in compilation order (leaves first).
    // This gives the daemon a comprehensive prefetch list that works even on
    // cold CI runners where the local SQLite store is empty.
    let crate_names = match get_all_crate_names_bfs() {
        Some(names) if !names.is_empty() => names,
        _ => return,
    };

    tracing::info!(
        "build session detected, sending prefetch hint for {} crates",
        crate_names.len()
    );

    crate::daemon::send_build_started(config, &crate_names);
}

/// Run `cargo metadata` to get ALL dependency crate names in BFS compilation
/// order (leaves first — crates with no dependencies are compiled first by cargo).
///
/// Uses the resolve graph from cargo metadata to topologically sort packages,
/// so the daemon prefetches crates in the order they'll be needed.
/// Takes ~200–500ms depending on project size (runs once per build session).
fn get_all_crate_names_bfs() -> Option<Vec<String>> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;

    // Build package_id → name map
    let packages = value.get("packages")?.as_array()?;
    let mut id_to_name: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for pkg in packages {
        if let (Some(id), Some(name)) = (
            pkg.get("id").and_then(|v| v.as_str()),
            pkg.get("name").and_then(|v| v.as_str()),
        ) {
            id_to_name.insert(id.to_string(), name.to_string());
        }
    }

    // Parse the dependency graph from resolve.nodes
    let nodes = value.get("resolve")?.get("nodes")?.as_array()?;
    let mut dep_count: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut dependents: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for node in nodes {
        let id = node.get("id")?.as_str()?.to_string();
        let deps = node
            .get("deps")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|d| d.get("pkg").and_then(|v| v.as_str()).map(String::from))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        dep_count.insert(id.clone(), deps.len());

        // Record reverse edges: for each dependency, note that `id` depends on it
        for dep_id in deps {
            dep_count.entry(dep_id.clone()).or_insert(0);
            dependents.entry(dep_id).or_default().push(id.clone());
        }
    }

    // Kahn's algorithm: BFS from leaves (packages with 0 dependencies)
    let mut queue: std::collections::VecDeque<String> = dep_count
        .iter()
        .filter(|&(_, &count)| count == 0)
        .map(|(id, _)| id.clone())
        .collect();

    let mut seen = std::collections::HashSet::new();
    let mut ordered_names = Vec::new();

    while let Some(id) = queue.pop_front() {
        if let Some(name) = id_to_name.get(&id)
            && seen.insert(name.clone())
        {
            ordered_names.push(name.clone());
        }
        if let Some(deps) = dependents.get(&id) {
            for dep_id in deps {
                if let Some(count) = dep_count.get_mut(dep_id) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        queue.push_back(dep_id.clone());
                    }
                }
            }
        }
    }

    Some(ordered_names)
}
