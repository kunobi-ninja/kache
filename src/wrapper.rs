use crate::store::StoreHashExt;
use anyhow::{Context, Result};
use bytesize::ByteSize;
use chrono::Utc;
pub(crate) use kache_store::markers::*;
use std::path::{Component, Path, PathBuf};

use crate::args::RustcArgs;
use crate::cache_key::FileHashStats;
use crate::cache_key::FileHasher;
use crate::compile;
use crate::compiler::cc::CcCompiler;
use crate::compiler::nvcc::NvccCompiler;
use crate::compiler::rustc::RustcCompiler;
use crate::compiler::{
    ArtifactKind, ArtifactSet, Compiler, KeyCtx, classify_by_filename, plan_post_restore, platform,
};
use crate::config::Config;
use crate::events::{self, BuildEvent, EventResult};
use crate::incremental_policy::{AdaptiveUnit, Lease};
use crate::link;
use crate::scheduler::{self, FlightIdentity, MissGuard};
use crate::store::{BuildClaim, EntryMeta, KeyLock, Store, StorePutResult};

mod remote;
use remote::{
    NegativeReply, acquire_entry, compiler_remote_enabled, compiler_upload_enabled,
    maybe_enqueue_upload,
};

mod hit;
use hit::HitCompletion;

mod rustc_hit;
use rustc_hit::RustcHitContext;

/// Check whether progress lines should be shown ([`crate::notice`]).
///
/// Controlled by `KACHE_PROGRESS` env var (off by default):
/// - `1` / `hits`    — print hits only
/// - `verbose` / `all` — print hits, dups, misses, and in-flight heartbeats
/// - anything else / unset — silent
fn progress_level() -> u8 {
    match std::env::var("KACHE_PROGRESS").as_deref() {
        Ok("1" | "hits") => 1,
        Ok("verbose" | "all") => 2,
        _ => 0,
    }
}

/// Heartbeats describe an in-progress cache miss, so only the verbose progress
/// modes show them.
fn heartbeat_lines_enabled(level: u8) -> bool {
    level >= 2
}

/// The progress-line label for a result at a given verbosity `level`, or `None`
/// when the line should be suppressed. Pure (no env / I/O) so the level gating
/// is unit-testable without touching `KACHE_PROGRESS` or stderr.
fn progress_label(result: EventResult, level: u8) -> Option<&'static str> {
    match result {
        EventResult::LocalHit => Some("local hit"),
        EventResult::PrefetchHit => Some("prefetch hit"),
        EventResult::RemoteHit => Some("remote hit"),
        EventResult::Dup if level < 2 => None,
        EventResult::Dup => Some("dup"),
        EventResult::Miss if level < 2 => None,
        EventResult::Miss => Some("miss"),
        EventResult::Error => Some("error"),
        EventResult::Passthrough => None,
        EventResult::Skipped => None,
    }
}

/// Show a concise progress line ([`crate::notice`]).
fn print_progress(crate_name: &str, result: EventResult, elapsed_ms: u64, size: u64) {
    let level = progress_level();
    if level == 0 {
        return;
    }

    let Some(label) = progress_label(result, level) else {
        return;
    };

    let size_str = if size > 0 {
        format!(", {}", ByteSize(size))
    } else {
        String::new()
    };

    let elapsed_str = if elapsed_ms >= 1000 {
        format!("{:.1}s", elapsed_ms as f64 / 1000.0)
    } else {
        format!("{}ms", elapsed_ms)
    };

    crate::notice::show_requested(&format!(
        "[kache] {crate_name}: {label} ({elapsed_str}{size_str})"
    ));
}

/// Build the user-facing diagnostic shown when the cache index can't be
/// opened (e.g. `Store::open` fails with a disk I/O / locking error).
///
/// Kept pure — takes the error, returns the text — so it's unit-testable
/// without touching stderr. Deliberately **generic**: it must not name any
/// specific environment (containers, cross, podman, network mounts). The
/// cause is described in terms of the underlying storage requirement so the
/// guidance applies to every case where the index can't be opened.
fn store_unavailable_message(err: &anyhow::Error) -> String {
    format!(
        "[kache] the cache index could not be opened after retries ({err:#}).\n\
         [kache] Caching is disabled for this build — compilation still succeeds,\n\
         [kache] just without cache hits or stores (everything builds uncached).\n\
         [kache] This is usually a storage issue: the cache directory is on a\n\
         [kache] filesystem that doesn't support reliable file locking, or it is\n\
         [kache] being accessed from more than one machine at the same time.\n\
         [kache] → set KACHE_CACHE_DIR to a fast, local, single-machine path"
    )
}

/// How long a one-shot warning stays "already emitted" for. Matches the
/// prefetch session window: the hundreds of wrapper processes a build spawns
/// all fall inside one window, so only the first of them warns, while a fresh
/// `cargo` command after a gap this long warns again. It is a sliding window,
/// not true build identity — a build that keeps hitting the cache for longer
/// than this re-warns once per window rather than exactly once. That is the
/// same trade-off `maybe_trigger_prefetch` and the store advisory already make,
/// and it still turns #508's 670 lines into a handful.
pub(crate) use kache_store::markers::WARN_SESSION_SECS;

/// Kache-only semantic inputs that rustc incremental compilation cannot infer
/// from argv. A change selects a fresh per-unit incremental directory before
/// the early adaptive path can run.
fn adaptive_policy_guard(config: &Config) -> [u8; 32] {
    fn fold(hasher: &mut blake3::Hasher, label: &[u8], value: &[u8]) {
        hasher.update(&(label.len() as u64).to_le_bytes());
        hasher.update(label);
        hasher.update(&(value.len() as u64).to_le_bytes());
        hasher.update(value);
    }

    let mut hasher = blake3::Hasher::new();
    fold(&mut hasher, b"policy", b"adaptive-incremental-v1");
    if let Some(salt) = config.key_salt.as_deref() {
        fold(&mut hasher, b"key-salt", salt.as_bytes());
    }
    if let Some(env_guard) = crate::cache_key::key_env_guard(&config.key_env_vars) {
        fold(&mut hasher, b"key-env", env_guard.as_bytes());
    }
    for base_dir in &config.base_dirs {
        fold(&mut hasher, b"base-dir", base_dir.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn adaptive_mode_enabled(config: &Config) -> bool {
    config.adaptive_incremental && !config.preserve_incremental
}

fn preserve_incremental_requested(config: &Config, args: &RustcArgs) -> bool {
    config.preserve_incremental && args.incremental.is_some()
}

fn force_incremental_requested(config: &Config, args: &RustcArgs) -> bool {
    args.incremental.is_some()
        && args
            .crate_name
            .as_deref()
            .is_some_and(|crate_name| config.incremental_crate_forced(crate_name))
}

fn adaptive_seed_allowed(config: &Config, args: &RustcArgs) -> bool {
    adaptive_mode_enabled(config) && !force_incremental_requested(config, args)
}

/// Build the one safety-checked unit used by both adaptive and force-list
/// incremental compiles. Declared inputs are checked only after the narrow
/// Cargo layout is known to be eligible; rejecting them also clears any old
/// private state for that unit.
fn managed_incremental_unit<F>(
    config: &Config,
    args: &RustcArgs,
    cargo_primary: bool,
    extra_inputs_declared: F,
) -> Option<AdaptiveUnit>
where
    F: FnOnce() -> bool,
{
    if !adaptive_mode_enabled(config) && !force_incremental_requested(config, args) {
        return None;
    }
    let guard = adaptive_policy_guard(config);
    let unit = AdaptiveUnit::eligible(args, cargo_primary, &guard)?;
    if extra_inputs_declared() {
        let _ = unit.reset();
        return None;
    }
    Some(unit)
}

fn incremental_fast_path_allowed(
    has_refuse_reasons: bool,
    source_excluded: bool,
    skip_user_facing: bool,
) -> bool {
    !has_refuse_reasons && !source_excluded && !skip_user_facing
}

/// Whether this unit is refused caching outright: the compiler's own refusal
/// list, or a codegen backend dylib kache will not replay. Either one also
/// keeps the unit off the managed-incremental fast path.
fn unit_refuses_caching(has_refuse_reasons: bool, untrusted_codegen_backend: bool) -> bool {
    has_refuse_reasons || untrusted_codegen_backend
}

fn incremental_cleanup_enabled(config: &Config) -> bool {
    config.clean_incremental && !config.preserve_incremental
}

fn disable_incremental_env(incremental_preserved: bool) -> bool {
    !incremental_preserved
}

/// Dedup-marker path for a warn-once-per-build-session message of `kind`
/// (`"store"`, `"cow"`, …).
///
/// Lives in the **OS temp dir**, keyed by a hash of the cache directory — NOT
/// under the cache dir itself. For the store warning the cache dir is exactly
/// the filesystem we can't rely on (broken locking / shared across machines),
/// so the marker that coordinates "warn only once" must live on a local,
/// writable filesystem instead. Keying by cache dir keeps two builds against
/// two different caches from silencing each other.
pub(crate) fn warn_marker_path(kind: &str, cache_dir: &Path) -> PathBuf {
    let hash = blake3::hash(cache_dir.as_os_str().as_encoded_bytes()).to_hex();
    std::env::temp_dir().join(format!("kache-{kind}-warn-{}", &hash[..16]))
}

/// Emit the `store_unavailable_message` to stderr **at most once per build
/// session**, even across the 300+ parallel wrapper processes a single build
/// spawns. Always records the full error in the debug log regardless.
///
/// Cross-process dedup uses the same flock-on-a-marker pattern as
/// `maybe_trigger_prefetch`, but the marker lives locally (see
/// `warn_marker_path`) because the cache dir can't be trusted here.
fn warn_store_unavailable_once(config: &Config, err: &anyhow::Error) {
    // Full detail always goes to the debug log for `KACHE_LOG` users.
    tracing::warn!("failed to open store: {:#}", err);

    let marker = warn_marker_path("store", &config.cache_dir);
    warn_once_per_session(&marker, WARN_SESSION_SECS, &store_unavailable_message(err));
}

/// Warn — at most once per build session — when the cache directory sits on a
/// filesystem that cannot safely host the WAL index (kunobi-ninja/kache#415).
///
/// This is the *preventive* twin of [`warn_store_unavailable_once`]: that one
/// fires after the index has already failed to open, this one fires while
/// everything still works, so the user can move the cache before it corrupts
/// (#412). Both dedup through the same marker machinery but on separate buckets,
/// so a pre-emptive advisory can never mute an actual store failure.
///
/// Cheap enough for the hot path: one `statfs` (or `GetDriveTypeW`) plus the
/// marker `stat` that `warn_once_per_session` already does, and only when the
/// verdict is actually non-local do we touch the lock.
pub(crate) fn warn_nonlocal_cache_fs_once(config: &Config) {
    let probe = crate::cache_fs::probe(&config.cache_dir);
    // Local, or the probe couldn't tell — either way, say nothing.
    let Some(message) = crate::cache_fs::advisory_for(&probe, &config.cache_dir) else {
        return;
    };

    tracing::warn!(
        cache_dir = %config.cache_dir.display(),
        filesystem = ?probe.name,
        "cache directory is not on host-local storage; the WAL index can corrupt"
    );

    let marker = warn_marker_path("cachefs", &config.cache_dir);
    warn_once_per_session(&marker, WARN_SESSION_SECS, &message);
}

// ── Opportunistic size-pressure GC (kunobi-ninja/kache#497) ─────────────────
//
// Store-wide eviction used to be triggered only from daemon-owned paths (the
// periodic GC task and the post-upload check), so a local-only build with no
// running daemon grew the store past `max_size` without bound. The wrapper
// now performs a cheap, throttled size check after storing a new entry and,
// when the store has outgrown `max_size` (plus slack), spawns a *detached*
// `kache gc` — the eviction itself never runs inside the compile hot path,
// and `gc.lock` (kunobi-ninja/kache#326) serializes concurrent GC drivers so
// racing wrappers cannot double-scan.
//
// Every automatic driver (this check, the daemon's post-upload check, its
// hinted sweep and the size pass of its periodic sweep) asks
// [`auto_gc_sweep_due`] whether to sweep and reports where the store ended
// through [`record_auto_gc_outcome`], so they share one trigger and one
// backoff (kunobi-ninja/kache#1127). With a daemon running the wrapper sweeps
// nothing itself: it sends the daemon a hint.

/// How often the wrapper is willing to re-run the store-size query. Between
/// checks the hot-path cost is a single `stat()` on the stamp file.
const AUTO_GC_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// Slack over `max_size` before a background GC is spawned, in percent.
/// `evict()` already targets 90% of `max_size`; triggering only above 110%
/// keeps the two thresholds apart so the store doesn't thrash at the boundary.
const AUTO_GC_SLACK_PERCENT: u64 = 10;

/// Longest the auto-GC backoff grows. The daemon's periodic sweep runs every
/// six hours, so a store that stays over budget still sees a sweep this often.
const AUTO_GC_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(2 * 3600);

/// Where the auto-GC worker leaves its backoff when a sweep could not bring
/// the store back under the trigger.
fn auto_gc_backoff_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("auto-gc-backoff.json")
}

/// Size above which an automatic sweep starts: `max_size` plus slack.
fn auto_gc_threshold(max_size: u64) -> u64 {
    max_size.saturating_add(max_size / 100 * AUTO_GC_SLACK_PERCENT)
}

/// A sweep ended with the store still over the trigger. Most often the
/// remaining bytes are blobs that target directories still hardlink or
/// clone, which no eviction can free, so another sweep five minutes later
/// frees nothing and only competes with builds for the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct AutoGcBackoff {
    /// Unix seconds when the sweep finished.
    since: u64,
    /// No automatic sweep before `since + interval_secs`.
    interval_secs: u64,
    /// Physical store size the sweep left behind.
    size_after: u64,
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn read_auto_gc_backoff(cache_dir: &Path) -> Option<AutoGcBackoff> {
    let json = std::fs::read(auto_gc_backoff_path(cache_dir)).ok()?;
    serde_json::from_slice(&json).ok()
}

/// The backoff after a sweep that left the store at `size_after`: none once
/// the store is back under the trigger, otherwise double the previous
/// interval, starting from the check interval, up to [`AUTO_GC_MAX_BACKOFF`].
fn next_auto_gc_backoff(
    previous: Option<AutoGcBackoff>,
    now: u64,
    size_after: u64,
    max_size: u64,
) -> Option<AutoGcBackoff> {
    if size_after <= auto_gc_threshold(max_size) {
        return None;
    }
    let interval_secs = previous
        .map_or(AUTO_GC_CHECK_INTERVAL.as_secs(), |b| b.interval_secs)
        .saturating_mul(2)
        .min(AUTO_GC_MAX_BACKOFF.as_secs());
    Some(AutoGcBackoff {
        since: now,
        interval_secs,
        size_after,
    })
}

/// Whether `backoff` still suppresses a sweep of a store now at `total`.
/// Growth past what the last sweep left, by more than the slack, is new data
/// a sweep may be able to free, so it ends the backoff early.
fn auto_gc_backoff_holds(
    backoff: Option<AutoGcBackoff>,
    now: u64,
    total: u64,
    max_size: u64,
) -> bool {
    let Some(backoff) = backoff else {
        return false;
    };
    let grown = total.saturating_sub(backoff.size_after);
    now < backoff.since.saturating_add(backoff.interval_secs)
        && grown <= max_size / 100 * AUTO_GC_SLACK_PERCENT
}

/// Whether an automatic sweep of a store now at `total` should wait out the
/// backoff the last sweep left.
pub(crate) fn auto_gc_backing_off(config: &Config, total: u64) -> bool {
    auto_gc_backoff_holds(
        read_auto_gc_backoff(&config.cache_dir),
        unix_now_secs(),
        total,
        config.max_size,
    )
}

/// The start condition of every automatic sweep, whichever driver asks: the
/// store is over the trigger and the last sweep left no backoff that still
/// holds. `kache gc` does not ask.
pub(crate) fn auto_gc_sweep_due(config: &Config, total: u64) -> bool {
    total > auto_gc_threshold(config.max_size) && !auto_gc_backing_off(config, total)
}

/// Called by every automatic driver after a sweep: store the next backoff, or
/// clear it once the store is back under the trigger.
pub(crate) fn record_auto_gc_outcome(config: &Config, size_after: u64) {
    let path = auto_gc_backoff_path(&config.cache_dir);
    let next = next_auto_gc_backoff(
        read_auto_gc_backoff(&config.cache_dir),
        unix_now_secs(),
        size_after,
        config.max_size,
    );
    let Some(next) = next else {
        let _ = std::fs::remove_file(&path);
        return;
    };
    tracing::info!(
        "auto-gc: store still at {} after the sweep (max {}); next automatic sweep in {}s at the earliest",
        size_after,
        config.max_size,
        next.interval_secs
    );
    if let Ok(json) = serde_json::to_vec(&next)
        && let Err(e) = crate::atomic::atomic_replace(&path, &json)
    {
        tracing::debug!("auto-gc: could not write {}: {e:#}", path.display());
    }
}

/// Test hook for the other drivers' tests: the recorded backoff interval.
#[cfg(test)]
pub(crate) fn auto_gc_backoff_interval_for_test(cache_dir: &Path) -> Option<u64> {
    read_auto_gc_backoff(cache_dir).map(|backoff| backoff.interval_secs)
}

/// Test hook: move the recorded backoff into the past until it has expired.
#[cfg(test)]
pub(crate) fn expire_auto_gc_backoff_for_test(cache_dir: &Path) {
    let mut backoff = read_auto_gc_backoff(cache_dir).expect("a recorded backoff");
    backoff.since -= backoff.interval_secs;
    let json = serde_json::to_vec(&backoff).unwrap();
    std::fs::write(auto_gc_backoff_path(cache_dir), json).unwrap();
}

/// Throttle stamp for the auto-GC size check. Lives next to the store so all
/// wrappers sharing a cache dir share the throttle.
fn auto_gc_stamp_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("auto-gc-check.stamp")
}

/// Decide whether a background GC should be spawned: auto-GC enabled, the
/// throttle interval elapsed, and the store over `max_size` plus slack.
/// Touches the stamp *before* the size query so concurrent wrappers don't
/// stampede on the SQLite `SUM`. Split from [`maybe_spawn_auto_gc`] so the
/// decision is unit-testable without spawning processes.
fn auto_gc_wanted(config: &Config, store: &Store) -> bool {
    if !config.auto_gc {
        return false;
    }
    let stamp = auto_gc_stamp_path(&config.cache_dir);
    if let Ok(meta) = std::fs::metadata(&stamp) {
        match meta.modified().ok().and_then(|m| m.elapsed().ok()) {
            Some(age) if age < AUTO_GC_CHECK_INTERVAL => return false,
            // `elapsed()` errs when the mtime is in the future (clock skew /
            // another process just touched it) — treat as fresh and skip.
            None => return false,
            _ => {}
        }
    }

    // Physical on-disk bytes, not the logical per-entry sum: the logical
    // figure over-reports by the dedup savings and would spawn GC while the
    // disk is comfortable (#608).
    let total = match store.physical_size() {
        Ok(total) => total,
        Err(e) => {
            tracing::debug!("auto-gc: store size query failed: {e:#}");
            return false;
        }
    };
    if !auto_gc_sweep_due(config, total) {
        let now_str = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs().to_string())
            .unwrap_or_default();
        let _ = std::fs::write(&stamp, now_str);
        return false;
    }

    // Exceeded threshold — claim this check slot before spawning GC
    let now_str = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_default();
    if std::fs::write(&stamp, now_str).is_err() {
        return false;
    }

    tracing::info!(
        "auto-gc: store size {} exceeds max {} (+{}% slack), triggering background GC",
        total,
        config.max_size,
        AUTO_GC_SLACK_PERCENT
    );
    true
}

/// After a store: if [`auto_gc_wanted`] says so, get a sweep started without
/// waiting for it.
pub(crate) fn maybe_spawn_auto_gc(config: &Config, store: &Store) {
    let _trace = crate::phase_trace::phase("auto_gc_check");
    run_auto_gc_check(
        config,
        store,
        crate::daemon::send_gc_hint,
        spawn_auto_gc_worker,
    );
}

/// A running daemon owns automatic eviction, so it gets a hint and nothing is
/// spawned. With no daemon, or one that does not know the hint, the detached
/// worker sweeps. The throttle stamp covers both, so a build sends at most
/// one hint per check interval.
fn run_auto_gc_check(
    config: &Config,
    store: &Store,
    hint_daemon: impl FnOnce(&Config) -> bool,
    spawn_worker: impl FnOnce(&Config),
) {
    if !auto_gc_wanted(config, store) {
        return;
    }
    if hint_daemon(config) {
        tracing::info!("auto-gc: handed the sweep to the daemon");
        return;
    }
    spawn_worker(config);
}

/// The `kache gc` worker command. `exe` is `current_exe`, which under a
/// compiler shim can be the shim, so this goes through
/// [`crate::platform::self_command`] to run as `kache gc` rather than `cc gc`.
fn auto_gc_worker_command(exe: &Path) -> std::process::Command {
    let mut cmd = crate::platform::self_command(exe, "gc");
    cmd.env("KACHE_AUTO_GC_WORKER", "1")
        .stdin(std::process::Stdio::null());
    cmd
}

/// Spawn a fully detached `kache gc`. Never waits on the child; stdio is null
/// so it cannot pollute the compiler's output streams.
fn spawn_auto_gc_worker(config: &Config) {
    let exe = match std::env::current_exe() {
        Ok(exe) => exe,
        Err(e) => {
            tracing::warn!("auto-gc: cannot resolve current executable: {e}");
            return;
        }
    };
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(config.cache_dir.join("auto-gc.log"));

    let mut cmd = auto_gc_worker_command(&exe);

    match log_file {
        Ok(f) => {
            if let Ok(dup) = f.try_clone() {
                cmd.stdout(dup);
            } else {
                cmd.stdout(std::process::Stdio::null());
            }
            cmd.stderr(f);
        }
        Err(_) => {
            cmd.stdout(std::process::Stdio::null());
            cmd.stderr(std::process::Stdio::null());
        }
    }

    crate::platform::configure_detached_process(&mut cmd);

    match cmd.spawn() {
        Ok(_) => tracing::info!("auto-gc: spawned background `kache gc`"),
        Err(e) => tracing::warn!("auto-gc: failed to spawn `kache gc`: {e}"),
    }
}

/// After a put under `[cache] deferred_durability`: the entry's blobs are on
/// disk but not flushed, and something has to flush them.
///
/// The daemon does, on its own short sweep: it is the process that already
/// outlives a build, and the one a build's teardown stops before anything
/// inspects the store. Nothing is spawned here, so no kache process is left
/// touching the store after the build that started it has finished.
///
/// Without a reachable daemon there is nobody to hand the work to, so this
/// entry is flushed here and now. That costs what an inline fsync always
/// cost, and it keeps a store that never sees a daemon from accumulating
/// entries whose every hit re-reads them to verify.
fn flush_or_hand_off_durability(config: &Config, store: &Store, cache_key: &str) {
    let _trace = crate::phase_trace::phase("durability_flush");
    if !config.deferred_durability {
        return;
    }
    if crate::transport::is_reachable(&config.socket_path()) {
        return;
    }
    if let Err(error) = store.flush_entry_durability(cache_key) {
        tracing::debug!("durability flush failed for {cache_key}: {error:#}");
    }
}

fn event_result_for_store_put(put: StorePutResult) -> EventResult {
    if put.is_full_dup() {
        EventResult::Dup
    } else {
        EventResult::Miss
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CcStoreDecision {
    admission_skipped: bool,
    should_store: bool,
}

fn cc_store_decision(store_candidate: bool, admitted: bool) -> CcStoreDecision {
    CcStoreDecision {
        admission_skipped: store_candidate && !admitted,
        should_store: admitted && store_candidate,
    }
}

fn event_result_for_store_admission(
    store_candidate: bool,
    admitted: bool,
    put: StorePutResult,
) -> EventResult {
    if cc_store_decision(store_candidate, admitted).admission_skipped {
        EventResult::Skipped
    } else {
        event_result_for_store_put(put)
    }
}

/// Apply the local admission threshold without suppressing remote publication.
/// A publish-capable path with a writable remote needs the local canonical
/// entry as its upload source. Callers without remote publication must pass
/// `false` so a configured threshold remains effective.
fn store_admits_compile(config: &Config, compile_time_ms: u64, publishes_to_remote: bool) -> bool {
    let writable_remote = config.remote.is_some() && !config.remote_readonly;
    (publishes_to_remote && writable_remote)
        || config.min_store_compile_ms == 0
        || compile_time_ms >= config.min_store_compile_ms
}

/// GCC/Clang objects (and clang-cl without CodeView debug) use the rustc
/// remote pipeline. clang-cl debug objects embed un-remapped paths.
fn cc_publishes_to_remote(parsed: &crate::compiler::cc::CcArgs) -> bool {
    !parsed.embeds_codeview_debug()
}

fn should_store_cc_result(exit_code: i32, has_artifacts: bool) -> bool {
    exit_code == 0 && has_artifacts
}

/// A deferred compile whose key a peer committed meanwhile: its own outputs
/// stand, the entry is theirs.
fn cc_peer_committed_precompile(precompiled: bool, committed: bool) -> bool {
    precompiled && committed
}

/// Whether a committed entry is restored over this invocation's outputs:
/// never over a compile that already ran, and only when it fits.
fn cc_restore_committed(precompiled: bool, entry_ok: bool) -> bool {
    !precompiled && entry_ok
}

/// Whether a clean compile may be stored: not when an input moved under it,
/// and not when a peer already published the key.
fn cc_store_candidate(clean: bool, inputs_changed: bool, peer_committed: bool) -> bool {
    clean && !inputs_changed && !peer_committed
}

fn cc_output_path_requires_passthrough(path: &Path) -> bool {
    crate::compiler::cc::output_path_requires_compiler_semantics(path)
}

/// Forward a `cc`-crate compiler-family probe (`kache -E <file>`) to
/// the real underlying compiler.
///
/// **Why this exists.** When `CC="kache <compiler>"`, the `cc` Rust
/// crate detects compiler family by running `Command::new(program).
/// arg("-E").arg(tmp.path())` — and `program` is just the first
/// whitespace-split component (`kache`), with the trailing `<compiler>`
/// arg dropped (kache is not in the crate's known-wrapper allowlist).
/// So kache gets called with argv that starts with a flag, not a
/// recognized compiler. Without this passthrough, kache clap-errors,
/// the cc crate falls back to a default family guess — and on Windows
/// MSVC that default is GNU, which is unsupported for the target, so
/// the whole build aborts (issue #286).
///
/// **Which compiler we forward to.** The answer the cc crate wants is
/// whatever the *underlying* compiler would say. We recover it from the
/// same `CC`/`CXX` environment variable the cc crate read — it still
/// holds `kache <compiler>` — via
/// [`resolve_probe_compiler`](crate::compiler::cc::resolve_probe_compiler).
/// That gives the genuine family on every platform, including the
/// Windows `clang-cl` case where no `cc` exists on PATH. Only when no
/// kache-wrapped compiler variable is present do we fall back to the
/// system `cc` (the original unix behaviour).
///
/// stdout / stderr inherit so the cc crate reads the preprocessor
/// output verbatim. Exit code propagates so a real probe failure
/// (missing compiler, malformed probe file) still surfaces.
pub fn run_cc_probe(args: &[String]) -> Result<i32> {
    let program = probe_forward_compiler();
    let status = std::process::Command::new(&program)
        .args(args)
        .status()
        .with_context(|| {
            format!("spawning `{program}` to forward cc-crate compiler-family probe")
        })?;
    Ok(status.code().unwrap_or(1))
}

/// Resolve the compiler a cc-crate family probe should forward to: the
/// real compiler recovered from `CC`/`CXX`, else the system `cc`.
fn probe_forward_compiler() -> String {
    let self_stem = std::env::current_exe()
        .ok()
        .as_deref()
        .and_then(Path::file_stem)
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "kache".to_string());

    // `vars_os` + lossy filter rather than `vars()`, which panics if
    // *any* environment variable holds non-UTF-8 (plausible on Windows).
    let env_vars = std::env::vars_os()
        .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)));

    // Cargo sets `TARGET` for build scripts — the same triple the cc
    // crate keys its `CC_<target>` lookup on — so kache can resolve the
    // exact variable the cc crate read when several are kache-wrapped.
    let target = std::env::var("TARGET").ok();

    crate::compiler::cc::resolve_probe_compiler(&self_stem, target.as_deref(), env_vars)
        .unwrap_or_else(|| "cc".to_string())
}

/// After a local+remote miss: join a machine-wide flight, then take a
/// permit. Lock order is flight → permit → the caller's `claim_build`.
/// Hits and passthroughs must not call this.
fn take_recheck_hit(
    store: &Store,
    cache_key: &str,
    entry_ok: &impl Fn(&EntryMeta) -> bool,
) -> Option<EntryMeta> {
    match store.get(cache_key) {
        Ok(Some(meta)) if entry_ok(&meta) => Some(meta),
        _ => None,
    }
}

/// Directory used to pick a `[cache.volumes]` shard for a rustc invocation.
fn volume_route_path_rustc(args: &RustcArgs) -> PathBuf {
    if let Some(dir) = &args.out_dir {
        return dir.clone();
    }
    if let Some(out) = &args.output {
        if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
            return parent.to_path_buf();
        }
        return out.clone();
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

/// Directory used to pick a `[cache.volumes]` shard for a cc invocation.
fn volume_route_path_cc(parsed: &crate::compiler::cc::CcArgs) -> PathBuf {
    if let Some(out) = &parsed.output {
        if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
            return parent.to_path_buf();
        }
        return out.clone();
    }
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn volume_cache_dirs_match(routed: &Path, main: &Path) -> bool {
    routed == main
}

/// Open the volume shard (or main store) plus an optional main-store fallback.
fn open_primary_and_fallback(config: &Config, route: &Path) -> Result<(Store, Option<Store>)> {
    let routed = config.routed_for_path(route);
    let primary = Store::open(&routed)?;
    if volume_cache_dirs_match(&routed.cache_dir, &config.cache_dir) {
        return Ok((primary, None));
    }
    let fallback = match Store::open(config) {
        Ok(store) => Some(store),
        Err(e) => {
            tracing::warn!(
                "main store unavailable for volume-shard fallback ({}): {e:#}",
                config.cache_dir.display()
            );
            None
        }
    };
    Ok((primary, fallback))
}

/// Local lookup: volume shard first, then the main store. The returned
/// store is the one whose blobs must be restored.
fn lookup_local_entry<'a>(
    primary: &'a Store,
    fallback: Option<&'a Store>,
    cache_key: &str,
) -> Result<Option<(&'a Store, crate::store::EntryMeta)>> {
    crate::demand::record(cache_key);
    let _trace = crate::phase_trace::phase("lookup");
    if let Some(meta) = primary.get(cache_key)? {
        return Ok(Some((primary, meta)));
    }
    if let Some(fallback) = fallback
        && let Some(meta) = fallback.get(cache_key)?
    {
        return Ok(Some((fallback, meta)));
    }
    Ok(None)
}

fn admit_scheduler_miss(
    config: &Config,
    store: &Store,
    cache_key: &str,
    identity: FlightIdentity,
    crate_name: &str,
    is_link: bool,
    entry_ok: impl Fn(&EntryMeta) -> bool,
) -> (MissGuard, Option<EntryMeta>) {
    if !config.scheduler {
        return (MissGuard::empty(), None);
    }
    let identity = identity.with_key(cache_key);
    loop {
        match scheduler::begin_miss(
            &config.cache_dir,
            true,
            &identity,
            crate_name,
            is_link,
            config.test_lease.as_deref(),
        ) {
            scheduler::BeginMiss::Recheck => {
                if let Some(meta) = take_recheck_hit(store, cache_key, &entry_ok) {
                    return (MissGuard::empty(), Some(meta));
                }
            }
            scheduler::BeginMiss::Compile(guard) => return (guard, None),
        }
    }
}

/// Where a wrapper invocation's clock starts.
///
/// When `main` pinned the process start, `elapsed_ms` is anchored there so the
/// event spans everything cargo waited for, and the time already spent (argv,
/// logging, config load) is recorded as `startup_ms`. Without a pinned start
/// (unit tests, library callers) the clock starts now and startup stays zero.
fn wrapper_entry() -> std::time::Instant {
    match crate::opcounts::process_start() {
        Some(start) => {
            crate::opcounts::record_startup(start.elapsed());
            start
        }
        None => std::time::Instant::now(),
    }
}

/// Run kache as a CUDA `nvcc` compiler wrapper (`CUDACXX="kache nvcc"`,
/// `NVCC="kache nvcc"`, or `CMAKE_CUDA_COMPILER_LAUNCHER=kache`).
///
/// Phase 1 (kunobi-ninja/kache#1024): parse → refuse-check → passthrough.
/// Every invocation runs the real `nvcc` with the original argv; the value
/// is recognition + a recorded passthrough reason (visible in
/// `report`/`why-miss`), proving the dispatch path before phase 2 wires
/// key → local store → remote check/upload.
/// Run kache as a CUDA `nvcc` compiler wrapper (`CUDACXX="kache nvcc"`,
/// `NVCC="kache nvcc"`, or `CMAKE_CUDA_COMPILER_LAUNCHER=kache`).
///
/// Caches the single-source `-c` object compile: parse, refuse-check,
/// cache key (`nvcc --version` plus host version plus flags plus the
/// `-M` content closure), local lookup, remote check, then restore on
/// hit or compile plus store plus upload on miss. Anything else goes
/// through [`nvcc_passthrough`].
///
/// Scope notes (kunobi-ninja/kache#1024): main store only (no volume
/// routing or scheduler admission yet); no fallback wrapper; any
/// restore failure recompiles via passthrough (nvcc always rewrites
/// `-o` outputs fresh, so no partial-restore abort).
pub fn run_nvcc(config: &Config, wrapper_args: &[String]) -> Result<i32> {
    let _trace = crate::phase_trace::start("nvcc", wrapper_args);
    let start = wrapper_entry();
    let invocation_start_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or(0);
    crate::link::set_windows_hardlink_restore(config.windows_hardlink);
    crate::link::set_shared_hardlink_restores(config.shared_hardlink_restores);
    crate::link::set_storage_layout_advice(config.storage_layout_advice);
    crate::link::set_layout_advice_to_log(true);
    crate::link::set_cow_warn_marker(warn_marker_path("cow", &config.cache_dir));
    warn_nonlocal_cache_fs_once(config);
    // Shared with the cc knob for now; a dedicated `[nvcc]` knob is a
    // follow-up once the flag set deserves its own namespace (#1024).
    let compiler =
        NvccCompiler::with_extra_allowlist_flags(config.cc_extra_allowlist_flags.clone())
            .with_base_dirs(config.base_dirs.clone());
    let parsed = compiler
        .parse(wrapper_args)
        .context("parsing nvcc arguments")?;
    let event_root = nvcc_event_root();

    // The crate-name slot in events / metadata is the source file
    // name — the closest analogue to rustc's crate name.
    let crate_name = parsed
        .sources
        .first()
        .and_then(|s| s.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".to_string());

    // Refuse-to-cache check: non-empty = this invocation isn't a
    // cacheable single-source `-c` compile. Passthrough.
    let refuse = compiler.refuse_reasons(&parsed);
    if !refuse.is_empty() {
        let reasons: Vec<&str> = refuse.iter().map(|r| r.description()).collect();
        tracing::debug!("nvcc: passthrough ({})", reasons.join("; "));
        let reason = refuse_reason_string(&refuse);
        return nvcc_passthrough_with_event(
            config,
            &parsed,
            &crate_name,
            &event_root,
            start,
            reason,
        );
    }

    // User bypass rules (#222): declared per project, evaluated before any key
    // work, same fail-closed contract as `exclude` below — a match only ever
    // means "do not cache".
    if let Some(reason) = config
        .project_rules
        .user_bypass_reason(&crate_name, &parsed.rest)
    {
        tracing::debug!("nvcc invocation bypassed by user rule: {reason}");
        return nvcc_passthrough_with_event(
            config,
            &parsed,
            &crate_name,
            &event_root,
            start,
            reason,
        );
    }

    let current_dir = std::env::current_dir().ok();
    let exclude_roots: Vec<_> = current_dir.iter().cloned().collect();
    if let Some(source) = parsed.sources.first()
        && config.project_rules.source_excluded(source, &exclude_roots)
    {
        tracing::debug!("nvcc source excluded from cache: {}", source.display());
        return nvcc_passthrough_with_event(
            config,
            &parsed,
            &crate_name,
            &event_root,
            start,
            format!("source excluded: {}", source.display()),
        );
    }

    // Never compile over an output that still shares a read-only cache
    // blob: the write would fail (EACCES) or, worse, poison the shared
    // inode. Bail loudly instead.
    nvcc_legacy_blob_check(config, &parsed)?;

    let store = match Store::open(config) {
        Ok(store) => store,
        Err(e) => {
            warn_store_unavailable_once(config, &e);
            return nvcc_passthrough_with_event(
                config,
                &parsed,
                &crate_name,
                &event_root,
                start,
                format!("store unavailable: {e}"),
            );
        }
    };

    // Compute the cache key (probes `nvcc --version` + host version,
    // runs `nvcc -M` for the dependency closure). On any failure fall
    // back to passthrough, which runs the real compiler and surfaces
    // the real diagnostic.
    let key_start = std::time::Instant::now();
    let mut file_hasher = store.file_hasher();
    file_hasher.arm_too_new_guard(invocation_start_ns, 0);
    let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
    let key_ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &config.cache_dir,
        key_salt: config.key_salt.as_deref(),
        key_env_vars: &config.key_env_vars,
        extra_inputs_digest: None,
    };
    let cache_key = match compiler.cache_key(&parsed, &key_ctx) {
        Ok(k) => k,
        Err(e) => {
            tracing::debug!("nvcc cache key failed for {crate_name}: {e} — passthrough");
            return nvcc_passthrough_with_event(
                config,
                &parsed,
                &crate_name,
                &event_root,
                start,
                format!("uncacheable|{e}"),
            );
        }
    };
    let key_ms = key_start.elapsed().as_millis() as u64;
    tracing::debug!("nvcc cache key for {}: {}", crate_name, &cache_key[..16]);

    // ── Local cache lookup ───────────────────────────────────────
    let lookup_start = std::time::Instant::now();
    let lookup = match lookup_local_entry(&store, None, &cache_key) {
        Ok(lookup) => lookup,
        Err(e) => {
            tracing::warn!("nvcc local store lookup failed for {crate_name}: {e} — recompiling");
            return nvcc_passthrough_with_event(
                config,
                &parsed,
                &crate_name,
                &event_root,
                start,
                format!("store lookup failed: {e}"),
            );
        }
    };
    let lookup_ms = lookup_start.elapsed().as_millis() as u64;
    let mut lookup_rejection = String::new();
    if let Some((hit_store, meta)) = lookup {
        if meta.files.is_empty() {
            // Poisoned entry — evict and recompile.
            tracing::warn!("nvcc cache entry for {crate_name} has no files, evicting");
            lookup_rejection = "matching entry has no cached artifacts".to_string();
            let _ = hit_store.remove_entry(&cache_key);
        } else if let Some(reason) = nvcc_cache_entry_rejection_reason(&parsed, &meta) {
            tracing::warn!(
                "nvcc cache entry for {crate_name} lacks artifacts required by this invocation ({reason}), evicting"
            );
            lookup_rejection = reason.to_string();
            let _ = hit_store.remove_entry(&cache_key);
        } else {
            let restore_start = std::time::Instant::now();
            if let Err(e) = restore_nvcc_from_cache(hit_store, &parsed, &meta) {
                tracing::warn!(
                    "restoring nvcc cache hit for {crate_name} failed: {e} — recompiling"
                );
                return nvcc_passthrough_with_event(
                    config,
                    &parsed,
                    &crate_name,
                    &event_root,
                    start,
                    format!("restore failed: {e}"),
                );
            }
            let restore_ms = restore_start.elapsed().as_millis() as u64;
            tracing::debug!(
                "nvcc local cache hit for {crate_name} ({})",
                &cache_key[..16]
            );
            HitCompletion {
                event_root: &event_root,
                crate_name: &crate_name,
                result: EventResult::LocalHit,
                cache_key: &cache_key,
                start,
                key_ms,
                key_hash_stats: FileHashStats::default(),
                lookup_ms,
                restore_ms,
            }
            .report(config, &meta);
            return Ok(0);
        }
    }

    if let Some(exit) = nvcc_try_remote_hit(
        config,
        &store,
        &parsed,
        &cache_key,
        &crate_name,
        &event_root,
        start,
        key_ms,
        lookup_ms,
    )? {
        return Ok(exit);
    }

    // ── Cache miss — compile, then store ─────────────────────────
    // Recheck the blob sharing at the last wrapper boundary: key
    // computation took long enough for another process to restore over
    // our outputs.
    nvcc_legacy_blob_check(config, &parsed)?;

    let mut committed = None;
    let mut _build_lock = None;
    match store.claim_build(&cache_key) {
        Ok(BuildClaim::Acquired(lock)) => _build_lock = Some(lock),
        Ok(BuildClaim::Committed(meta)) => committed = Some(*meta),
        Ok(BuildClaim::Contended) => {
            tracing::debug!("waiting for nvcc {crate_name} to be built by another process");
            committed = store
                .wait_for_committed(&cache_key)
                .unwrap_or(false)
                .then(|| store.get(&cache_key).ok().flatten())
                .flatten()
                .filter(|meta| nvcc_cache_entry_rejection_reason(&parsed, meta).is_none());
        }
        Err(e) => {
            tracing::debug!("nvcc claim_build failed ({e:#}); compiling without a key lock");
        }
    }

    if let Some(meta) =
        committed.filter(|meta| nvcc_cache_entry_rejection_reason(&parsed, meta).is_none())
    {
        let restore_start = std::time::Instant::now();
        if let Err(e) = restore_nvcc_from_cache(&store, &parsed, &meta) {
            tracing::warn!(
                "restoring nvcc coalesced hit for {crate_name} failed: {e} — recompiling"
            );
            return nvcc_passthrough_with_event(
                config,
                &parsed,
                &crate_name,
                &event_root,
                start,
                format!("restore failed: {e}"),
            );
        }
        let restore_ms = restore_start.elapsed().as_millis() as u64;
        HitCompletion {
            event_root: &event_root,
            crate_name: &crate_name,
            result: EventResult::LocalHit,
            cache_key: &cache_key,
            start,
            key_ms,
            key_hash_stats: FileHashStats::default(),
            lookup_ms,
            restore_ms,
        }
        .report(config, &meta);
        return Ok(0);
    }

    let compile_start = std::time::Instant::now();
    let result = match compiler.execute(&parsed) {
        Ok(r) => r,
        // A spawn-level failure must not abort the build: fall back to
        // passthrough so the user sees the real compiler error rather
        // than a kache anyhow chain.
        Err(e) => {
            return nvcc_passthrough_with_event(
                config,
                &parsed,
                &crate_name,
                &event_root,
                start,
                format!("compiler spawn failed: {e}"),
            );
        }
    };
    let compile_time_ms = compile_start.elapsed().as_millis() as u64;

    replay_diagnostics(
        &result.stdout,
        result.pending_stderr(),
        std::io::stdout(),
        std::io::stderr(),
    );

    // Only store a clean compile that produced its object. Anything
    // else returns the exit code and lets the build see the failure.
    let store_start = std::time::Instant::now();
    let mut store_put = StorePutResult::default();
    let mut store_error = String::new();
    let store_candidate = should_store_cc_result(result.exit_code, !result.artifacts.is_empty());
    // nvcc entries are portable by construction (prefix-mapped objects,
    // pinned epoch, rewritten dep-info), so every stored entry may
    // publish to a writable remote.
    let admitted = store_admits_compile(config, compile_time_ms, true);
    let store_decision = cc_store_decision(store_candidate, admitted);
    if store_decision.admission_skipped {
        tracing::debug!(
            crate_name = %crate_name,
            compile_time_ms,
            min_store_compile_ms = config.min_store_compile_ms,
            "admission: compile too cheap to store"
        );
    }
    if store_decision.should_store {
        let depinfo_anchor = nvcc_depinfo_rewrite_root(&parsed);
        let target = crate::compiler::nvcc::nvcc_target_label(&parsed.deferred_flags);
        match prepare_cc_store_files(&result.artifacts, depinfo_anchor.as_deref()) {
            Ok(prepared) => match store.put_with_compile_time_independent(
                &cache_key,
                &crate_name,
                &[], // crate_types: n/a for nvcc objects
                &[], // features: n/a
                &target,
                "", // profile: n/a (opt level is in the key)
                &prepared.files,
                &result.stdout,
                &result.stderr,
                compile_time_ms,
            ) {
                Ok(put) => {
                    store_put = put;
                    // Store grew — throttled size check + detached background GC if over
                    // budget (kunobi-ninja/kache#497). Never blocks the compile path.
                    maybe_spawn_auto_gc(config, &store);
                    flush_or_hand_off_durability(config, &store, &cache_key);
                    maybe_enqueue_upload(config, &store, &cache_key, &crate_name, true);
                }
                Err(e) => {
                    store_error = store_error_for_event(&e);
                    tracing::warn!(
                        "failed to store nvcc cache entry for {crate_name}: {store_error}"
                    );
                }
            },
            Err(e) => {
                store_error = store_error_for_event(&e);
                tracing::warn!(
                    "failed to prepare nvcc cache entry for {crate_name}: {store_error}"
                );
            }
        }
    }
    let store_ms = store_start.elapsed().as_millis() as u64;

    let elapsed = start.elapsed().as_millis() as u64;
    let size = result.artifacts.total_size();
    let event_result = event_result_for_store_admission(store_candidate, admitted, store_put);
    log_event_with_store_and_lookup_outcome(
        config,
        &event_root,
        &crate_name,
        event_result,
        elapsed,
        compile_time_ms,
        size,
        &cache_key,
        key_ms,
        FileHashStats::default(),
        lookup_ms,
        0,
        store_ms,
        store_put,
        store_error,
        lookup_rejection,
    );
    print_progress(&crate_name, event_result, elapsed, size);
    Ok(result.exit_code)
}

/// Run an `nvcc` invocation without caching — invoke the compiler with
/// the original argv, propagate the exit code.
///
/// A refusal promises to preserve `nvcc`'s behavior exactly, so this
/// never injects flags (prefix maps, `SOURCE_DATE_EPOCH`): those belong
/// to the phase-2 cache-miss execution path.
fn nvcc_passthrough(parsed: &crate::compiler::nvcc::NvccArgs) -> Result<PassthroughOutput> {
    crate::opcounts::record_compiler_run();
    let status = std::process::Command::new(&parsed.program)
        .args(&parsed.rest)
        .status()
        .with_context(|| format!("executing {}", parsed.program))?;
    Ok(PassthroughOutput {
        exit_code: status.code().unwrap_or(1),
        fallback: false,
        fallback_attempt: None,
    })
}

fn nvcc_passthrough_with_event<R: Into<String>>(
    config: &Config,
    parsed: &crate::compiler::nvcc::NvccArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    let output = nvcc_passthrough(parsed)?;
    log_passthrough_event(
        config,
        root,
        crate_name,
        start.elapsed().as_millis() as u64,
        reason.into(),
        &output,
    );
    Ok(output.exit_code)
}

fn nvcc_event_root() -> String {
    nvcc_event_root_in(std::env::var_os("OUT_DIR"))
}

fn nvcc_event_root_in(out_dir: Option<std::ffi::OsString>) -> String {
    event_root_string(
        event_root_override()
            .or_else(|| out_dir_workspace(out_dir))
            .or_else(|| std::env::current_dir().ok()),
    )
}

/// Refuse to invoke the compiler over outputs that still share a
/// read-only cache blob (same contract as the cc path): the write
/// would fail with EACCES, and a chmod could not help since the inode
/// is shared with the store.
fn nvcc_legacy_blob_check(config: &Config, parsed: &crate::compiler::nvcc::NvccArgs) -> Result<()> {
    let store_dir = config.store_dir();
    let mut outputs = Vec::new();
    if let Some(object) = parsed.object_output_path() {
        outputs.push(object);
    }
    if let Some(depfile) = parsed.depinfo_output_path() {
        outputs.push(depfile);
    }
    for output in outputs {
        if let Some(blob) = Store::matching_readonly_blob_inode(&store_dir, &output)? {
            anyhow::bail!(
                "refusing to invoke the compiler because {} still shares the \
                 read-only cache blob {}; remove the build output and retry",
                output.display(),
                blob.display()
            );
        }
    }
    Ok(())
}

/// Why a cached entry cannot satisfy this invocation, if it cannot: no
/// object, or a requested dep-info the entry lacks. `None` restores.
fn nvcc_cache_entry_rejection_reason(
    parsed: &crate::compiler::nvcc::NvccArgs,
    meta: &crate::store::EntryMeta,
) -> Option<&'static str> {
    let has_object = meta
        .files
        .iter()
        .any(|file| classify_by_filename(&file.name) == ArtifactKind::Object);
    let has_depinfo = meta
        .files
        .iter()
        .any(|file| classify_by_filename(&file.name) == ArtifactKind::DepInfo);

    if !has_object {
        Some("matching entry lacks the object artifact required by this invocation")
    } else if parsed.depinfo_output_path().is_some() && !has_depinfo {
        Some("matching entry lacks dep-info required by this invocation")
    } else {
        None
    }
}

fn nvcc_depinfo_rewrite_root(
    parsed: &crate::compiler::nvcc::NvccArgs,
) -> Option<std::path::PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    nvcc_depinfo_rewrite_root_from_cwd(parsed, &cwd)
}

fn nvcc_depinfo_rewrite_root_from_cwd(
    parsed: &crate::compiler::nvcc::NvccArgs,
    cwd: &Path,
) -> Option<std::path::PathBuf> {
    use std::path::Component;
    parsed.depinfo_output_path()?;

    let object_anchor = parsed.object_output_path().and_then(|object| {
        absolute_clean_path(&object, cwd)
            .parent()
            .map(Path::to_path_buf)
    })?;
    let source_anchor = parsed
        .sources
        .first()
        .map(|source| absolute_clean_path(source, cwd))
        .and_then(|source| source.parent().map(Path::to_path_buf));

    source_anchor
        .and_then(|source| common_path_prefix(&source, &object_anchor))
        .filter(|root| root.components().any(|c| matches!(c, Component::Normal(_))))
        .or(Some(object_anchor))
}

/// Restore a cached nvcc entry: the object to `-o`, the dep-info to
/// `-MF` when requested (entries without it restore fine — the flag
/// decides). Unknown kinds are skipped, never placed. Any failure
/// bails to recompilation: nvcc rewrites `-o` outputs fresh, so a
/// half-restored state is safe to compile over.
fn restore_nvcc_from_cache(
    store: &Store,
    parsed: &crate::compiler::nvcc::NvccArgs,
    meta: &crate::store::EntryMeta,
) -> Result<()> {
    let depinfo_anchor =
        nvcc_depinfo_rewrite_root(parsed).unwrap_or_else(|| Path::new(".").to_path_buf());
    let mut prepared = Vec::new();
    let mut targets = std::collections::HashSet::new();

    for cached in &meta.files {
        let kind = classify_by_filename(&cached.name);
        let target = match kind {
            ArtifactKind::Object => parsed
                .object_output_path()
                .context("nvcc restore: cannot determine object output path")?,
            ArtifactKind::DepInfo => match parsed.depinfo_output_path() {
                Some(path) => path,
                None => {
                    tracing::debug!(
                        "nvcc restore: cached dep-info {} not requested by invocation; skipping",
                        cached.name
                    );
                    continue;
                }
            },
            _ => {
                tracing::debug!(
                    "nvcc restore: cached artifact {} has unsupported kind {:?}; skipping",
                    cached.name,
                    kind
                );
                continue;
            }
        };

        // Recheck immediately before the path-based restore: the
        // initial check ran before key computation, and another
        // process may have restored over our outputs since.
        if cc_output_path_requires_passthrough(&target) {
            anyhow::bail!(
                "nvcc restore: output path changed and now requires compiler passthrough semantics"
            );
        }
        anyhow::ensure!(
            targets.insert(target.clone()),
            "nvcc restore: cache entry maps multiple artifacts to {}",
            target.display()
        );
        prepared.push(prepare_cc_cached_artifact(
            store,
            cached,
            &target,
            kind,
            &depinfo_anchor,
        )?);
    }
    publish_prepared_cc_artifacts(prepared)
}

/// After a local miss, ask the daemon for an exact remote entry.
/// Returns `Some(exit)` when the hit was restored (or the restore fell
/// through to passthrough). `None` means continue to compile.
/// nvcc entries are portable by construction, so every stored entry
/// may publish: the only gate is a configured remote.
fn nvcc_try_remote_hit(
    config: &Config,
    store: &Store,
    parsed: &crate::compiler::nvcc::NvccArgs,
    cache_key: &str,
    crate_name: &str,
    event_root: &str,
    start: std::time::Instant,
    key_ms: u64,
    lookup_ms: u64,
) -> Result<Option<i32>> {
    let Some((meta, event_result)) = acquire_entry(
        config,
        store,
        cache_key,
        crate_name,
        NegativeReply::ContinueCompile,
    ) else {
        return Ok(None);
    };
    let restore_start = std::time::Instant::now();
    if let Err(e) = restore_nvcc_from_cache(store, parsed, &meta) {
        tracing::warn!(
            "restoring nvcc remote cache hit for {crate_name} failed: {e} — recompiling"
        );
        return Ok(Some(nvcc_passthrough_with_event(
            config,
            parsed,
            crate_name,
            event_root,
            start,
            format!("restore failed: {e}"),
        )?));
    }
    let restore_ms = restore_start.elapsed().as_millis() as u64;
    HitCompletion {
        event_root,
        crate_name,
        result: event_result,
        cache_key,
        start,
        key_ms,
        key_hash_stats: FileHashStats::default(),
        lookup_ms,
        restore_ms,
    }
    .report(config, &meta);
    Ok(Some(0))
}

/// Run kache as a C-family compiler wrapper (`CC=kache cc`,
/// `CXX=kache c++`, etc.).
///
/// Caches the single-source `-c` object compile: parse → refuse-check
/// → cache key (preprocessor hash) → local store lookup → restore the
/// `.o` on hit, or compile + store on dup/miss. Everything else (link
/// mode, multi-source, unsafe flags) routes through [`cc_passthrough`].
///
/// Local and remote hits share compiler-specific restoration. Miss-path
/// flights, permits, and per-key build locks match rustc.
pub fn run_cc(config: &Config, wrapper_args: &[String]) -> Result<i32> {
    let _trace = crate::phase_trace::start("cc", wrapper_args);
    let start = wrapper_entry();
    let invocation_start_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or(0);
    run_cc_inner(config, wrapper_args, start, invocation_start_ns)
}

/// A C compile that already ran because its key was deferred: the key is
/// derived from what the compile read, and the outputs are in place.
struct CcPrecompiled {
    result: crate::compile::CompileResult,
    compile_time_ms: u64,
    inputs: Option<crate::compiler::cc::CcCapturedInputs>,
    /// The discovery flight held through the store, so peers of the same
    /// unit wait for the memo instead of compiling too.
    flight: Option<crate::store::StoreLock>,
}

thread_local! {
    /// Set while a deferred C compile is being keyed and stored, so a
    /// passthrough taken on that path returns the compile's exit code
    /// instead of running the compiler a second time.
    static CC_PRECOMPILED_EXIT: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

fn run_cc_inner(
    config: &Config,
    wrapper_args: &[String],
    start: std::time::Instant,
    invocation_start_ns: i64,
) -> Result<i32> {
    crate::link::set_windows_hardlink_restore(config.windows_hardlink);
    crate::link::set_shared_hardlink_restores(config.shared_hardlink_restores);
    crate::link::set_storage_layout_advice(config.storage_layout_advice);
    crate::link::set_layout_advice_to_log(true);
    crate::link::set_cow_warn_marker(warn_marker_path("cow", &config.cache_dir));
    warn_nonlocal_cache_fs_once(config);
    let compiler = CcCompiler::with_extra_allowlist_flags(config.cc_extra_allowlist_flags.clone())
        .with_cache_cc_links(config.cache_cc_links)
        .with_base_dirs(config.base_dirs.clone());
    let trace_parse = crate::phase_trace::phase("cc_parse");
    let parsed = compiler
        .parse(wrapper_args)
        .context("parsing cc-family arguments")?;
    drop(trace_parse);
    if crate::compiler::cc::cc_is_internal_key_probe() {
        let crate_name = parsed
            .sources
            .first()
            .and_then(|s| s.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown".to_string());
        return cc_direct_passthrough_with_event(
            config,
            &parsed,
            &crate_name,
            &cc_event_root(&parsed),
            start,
            "cc key probe".to_string(),
        );
    }
    let event_root = cc_event_root(&parsed);

    // The crate-name slot in events / metadata is the source file
    // name for cc — the closest analogue to rustc's crate name.
    let crate_name = parsed
        .sources
        .first()
        .and_then(|s| s.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".to_string());

    // Refuse-to-cache check: non-empty = this invocation isn't a
    // cacheable single-source `-c` compile (link mode, multi-arch,
    // PCH, modules, etc. — see CcArgs::refuse_reasons). Passthrough.
    let trace_preflight = crate::phase_trace::phase("cc_preflight");
    let refuse = compiler.refuse_reasons(&parsed);
    if !refuse.is_empty() {
        let reasons: Vec<&str> = refuse.iter().map(|r| r.description()).collect();
        tracing::debug!(
            "{}: passthrough ({})",
            compiler.id().as_str(),
            reasons.join("; ")
        );
        let reason = refuse_reason_string(&refuse);
        return if parsed.requires_compiler_output_semantics() {
            cc_direct_passthrough_with_event(
                config,
                &parsed,
                &crate_name,
                &event_root,
                start,
                reason,
            )
        } else {
            cc_passthrough_with_event(config, &parsed, &crate_name, &event_root, start, reason)
        };
    }

    // User bypass rules (#222): declared per project, evaluated before any key
    // work, same fail-closed contract as `exclude` below — a match only ever
    // means "do not cache".
    if let Some(reason) = config
        .project_rules
        .user_bypass_reason(&crate_name, &parsed.rest)
    {
        tracing::debug!("cc invocation bypassed by user rule: {reason}");
        return cc_passthrough_with_event(config, &parsed, &crate_name, &event_root, start, reason);
    }

    let current_dir = std::env::current_dir().ok();
    let exclude_roots: Vec<_> = current_dir.iter().cloned().collect();
    if let Some(source) = parsed.sources.first()
        && config.project_rules.source_excluded(source, &exclude_roots)
    {
        tracing::debug!("cc source excluded from cache: {}", source.display());
        return cc_passthrough_with_event(
            config,
            &parsed,
            &crate_name,
            &event_root,
            start,
            format!("source excluded: {}", source.display()),
        );
    }
    drop(trace_preflight);

    let trace_store_open = crate::phase_trace::phase("store_open");
    let (store, fallback_store) =
        match open_primary_and_fallback(config, &volume_route_path_cc(&parsed)) {
            Ok(pair) => {
                drop(trace_store_open);
                pair
            }
            Err(e) => {
                warn_store_unavailable_once(config, &e);
                return cc_passthrough_with_event(
                    config,
                    &parsed,
                    &crate_name,
                    &event_root,
                    start,
                    format!("store unavailable: {e}"),
                );
            }
        };

    let invocation = CcStoreInvocation {
        compiler,
        parsed,
        store,
        fallback_store,
        crate_name,
        event_root,
        start,
        invocation_start_ns,
    };
    run_cc_with_store(config, &invocation, None)
}

/// Reused after a compile-first miss, including the store connection and
/// compiler policy that admitted the invocation before it ran.
struct CcStoreInvocation {
    compiler: CcCompiler,
    parsed: crate::compiler::cc::CcArgs,
    store: Store,
    fallback_store: Option<Store>,
    crate_name: String,
    event_root: String,
    start: std::time::Instant,
    invocation_start_ns: i64,
}

fn run_cc_with_store(
    config: &Config,
    invocation: &CcStoreInvocation,
    mut precompiled: Option<CcPrecompiled>,
) -> Result<i32> {
    let CcStoreInvocation {
        compiler,
        parsed,
        store,
        fallback_store,
        crate_name,
        event_root,
        start,
        invocation_start_ns,
    } = invocation;
    let start = *start;
    let invocation_start_ns = *invocation_start_ns;
    // Compute the cache key (runs `cc -E -P` for the preprocessor
    // hash). On any failure — preprocessor error, missing compiler —
    // fall back to passthrough, which runs the real compiler and
    // surfaces the real diagnostic.
    let key_start = std::time::Instant::now();
    let mut file_hasher = store.file_hasher();
    // Memo publication always uses the too-new guard: a header modified while
    // preprocessing cannot safely describe the captured expansion.
    file_hasher.arm_too_new_guard(invocation_start_ns, 0);
    let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
    let key_ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &config.cache_dir,
        key_salt: config.key_salt.as_deref(),
        key_env_vars: &config.key_env_vars,
        extra_inputs_digest: None,
    };
    let mut captured_inputs_changed = false;
    let discovery = match precompiled.as_mut().and_then(|pre| pre.inputs.take()) {
        Some(inputs) => {
            captured_inputs_changed = inputs.inputs_changed();
            crate::compiler::cc::CcKeyDiscovery::Captured(inputs)
        }
        // Compiling first forgoes the lookup a key would have allowed, so it
        // is only for a certain miss: nowhere but this store could hold the
        // entry, and no fallback wrapper is waiting to be asked.
        None if precompiled.is_none()
            && config.deferred_discovery
            && config.remote.is_none()
            && config.fallback.is_none()
            && crate::compiler::cc::cc_direct_key_eligible(parsed) =>
        {
            crate::compiler::cc::CcKeyDiscovery::Deferrable
        }
        None => crate::compiler::cc::CcKeyDiscovery::Expansion,
    };
    let keyed = compiler.cache_key_with(parsed, &key_ctx, discovery);
    let keyed = match keyed {
        Ok(crate::compiler::cc::CcKeyOutcome::Deferred(deferred)) => {
            // No memo describes this unit's read set. Hold the discovery
            // flight so peers wait for the memo this compile leaves, then
            // ask once more: the previous owner may have published it.
            let flight = crate::scheduler::join_discovery_flight(
                &config.cache_dir,
                &format!("cc:{}", deferred.memo_key),
            );
            // Only a wait can have let a previous owner publish; a flight
            // taken at once keeps the answer already in hand.
            let keyed = if flight.waited {
                compiler.cache_key_with(
                    parsed,
                    &key_ctx,
                    crate::compiler::cc::CcKeyDiscovery::Deferrable,
                )
            } else {
                Ok(crate::compiler::cc::CcKeyOutcome::Deferred(deferred))
            };
            let flight = flight.lock;
            match keyed {
                Ok(crate::compiler::cc::CcKeyOutcome::Deferred(_)) => {
                    return cc_compile_before_key(config, invocation, &file_hasher, flight);
                }
                other => other,
            }
        }
        other => other,
    };
    let cache_key = match keyed {
        Ok(crate::compiler::cc::CcKeyOutcome::Key(k)) => k,
        Ok(crate::compiler::cc::CcKeyOutcome::Deferred(_)) => {
            unreachable!("a deferred cc key is resolved above")
        }
        Err(e) => {
            tracing::debug!(
                "cc cache key failed for {}: {} — passthrough",
                crate_name,
                e
            );
            let reason = format!("uncacheable|{e}");
            return if cc_key_error_skips_fallback(&e) {
                cc_direct_passthrough_with_event(
                    config, parsed, crate_name, event_root, start, reason,
                )
            } else {
                cc_passthrough_with_event(config, parsed, crate_name, event_root, start, reason)
            };
        }
    };
    let key_ms = key_start.elapsed().as_millis() as u64;
    tracing::debug!("cc cache key for {}: {}", crate_name, &cache_key[..16]);

    // ── Local cache lookup ───────────────────────────────────────
    let lookup_start = std::time::Instant::now();
    let trace_lookup = crate::phase_trace::phase("lookup");
    // A compile that already ran is keyed for storing, not for a hit: its
    // outputs are in place and its diagnostics were shown.
    let lookup = if precompiled.is_some() {
        None
    } else {
        match lookup_local_entry(store, fallback_store.as_ref(), &cache_key) {
            Ok(lookup) => {
                drop(trace_lookup);
                lookup
            }
            Err(e) => {
                tracing::warn!(
                    "cc local store lookup failed for {}: {} — recompiling",
                    crate_name,
                    e
                );
                return cc_passthrough_with_event(
                    config,
                    parsed,
                    crate_name,
                    event_root,
                    start,
                    format!("store lookup failed: {e}"),
                );
            }
        }
    };
    let lookup_ms = lookup_start.elapsed().as_millis() as u64;
    let mut lookup_rejection = String::new();
    if let Some((hit_store, meta)) = lookup {
        if meta.files.is_empty() {
            // Poisoned entry (earlier bug) — evict and recompile.
            tracing::warn!("cc cache entry for {} has no files, evicting", crate_name);
            lookup_rejection = "matching entry has no cached artifacts".to_string();
            let _ = hit_store.remove_entry(&cache_key);
        } else if let Some(reason) = cc_cache_entry_rejection_reason(parsed, &meta) {
            tracing::warn!(
                "cc cache entry for {} lacks artifacts required by this invocation ({reason}), evicting",
                crate_name,
            );
            lookup_rejection = reason.to_string();
            let _ = hit_store.remove_entry(&cache_key);
        } else {
            let restore_start = std::time::Instant::now();
            let trace_restore = crate::phase_trace::phase("restore");
            let restored = restore_cc_from_cache(hit_store, parsed, &meta);
            drop(trace_restore);
            if let Err(e) = restored {
                if e.downcast_ref::<PartialCcRestore>().is_some() {
                    return Err(e);
                }
                tracing::warn!(
                    "restoring cc cache hit for {} failed: {} — recompiling",
                    crate_name,
                    e
                );
                return cc_passthrough_with_event(
                    config,
                    parsed,
                    crate_name,
                    event_root,
                    start,
                    format!("restore failed: {e}"),
                );
            }
            let restore_ms = restore_start.elapsed().as_millis() as u64;
            tracing::debug!(
                "cc local cache hit for {} ({})",
                crate_name,
                &cache_key[..16]
            );
            let trace_report = crate::phase_trace::phase("event_report");
            HitCompletion {
                event_root,
                crate_name,
                result: EventResult::LocalHit,
                cache_key: &cache_key,
                start,
                key_ms,
                key_hash_stats: FileHashStats::default(),
                lookup_ms,
                restore_ms,
            }
            .report(config, &meta);
            drop(trace_report);

            let _trace = crate::phase_trace::phase("memo_commit");
            compiler.commit_preprocess_memo(&file_hasher);

            return Ok(0);
        }
    }

    if precompiled.is_none()
        && let Some(exit) = cc_try_remote_hit(
            config,
            store,
            compiler,
            parsed,
            &file_hasher,
            &cache_key,
            crate_name,
            event_root,
            start,
            key_ms,
            lookup_ms,
        )?
    {
        return Ok(exit);
    }

    // ── Cache miss — compile, then store ─────────────────────────
    // Key generation and lookup can take long enough for another process to
    // create an output. Recheck at the last possible wrapper boundary and run
    // the selected compiler directly if its pathname semantics are now needed.
    if precompiled.is_none() && parsed.requires_compiler_output_semantics() {
        return cc_direct_passthrough_with_event(
            config,
            parsed,
            crate_name,
            event_root,
            start,
            "output appeared before compiler execution",
        );
    }

    let (miss_guard, scheduled_hit) = if precompiled.is_none() {
        admit_scheduler_miss(
            config,
            store,
            &cache_key,
            FlightIdentity::cc(crate_name),
            crate_name,
            false,
            |meta| cc_scheduled_hit_ok(parsed, meta),
        )
    } else {
        (MissGuard::empty(), None)
    };

    let mut committed = scheduled_hit;
    let mut _build_lock = None;
    if committed.is_none() {
        match store.claim_build(&cache_key) {
            Ok(BuildClaim::Acquired(lock)) => _build_lock = Some(lock),
            Ok(BuildClaim::Committed(meta)) => committed = Some(*meta),
            Ok(BuildClaim::Contended) => {
                tracing::debug!(
                    "waiting for cc {} to be built by another process",
                    crate_name
                );
                committed = store
                    .wait_for_committed(&cache_key)
                    .unwrap_or(false)
                    .then(|| store.get(&cache_key).ok().flatten())
                    .flatten()
                    .filter(|meta| cc_scheduled_hit_ok(parsed, meta));
            }
            Err(e) => {
                tracing::debug!("cc claim_build failed ({e:#}); compiling without a key lock");
            }
        }
    }

    // A peer published this key while a deferred compile ran: the outputs
    // here are this compile's own, so nothing is restored and nothing more
    // is stored.
    let peer_committed = cc_peer_committed_precompile(precompiled.is_some(), committed.is_some());
    if let Some(meta) = committed.filter(|meta| {
        cc_restore_committed(precompiled.is_some(), cc_scheduled_hit_ok(parsed, meta))
    }) {
        let restore_start = std::time::Instant::now();
        if let Err(e) = restore_cc_from_cache(store, parsed, &meta) {
            if e.downcast_ref::<PartialCcRestore>().is_some() {
                return Err(e);
            }
            tracing::warn!(
                "restoring cc coalesced hit for {} failed: {} — recompiling",
                crate_name,
                e
            );
            return cc_passthrough_with_event(
                config,
                parsed,
                crate_name,
                event_root,
                start,
                format!("restore failed: {e}"),
            );
        }
        let restore_ms = restore_start.elapsed().as_millis() as u64;
        HitCompletion {
            event_root,
            crate_name,
            result: EventResult::LocalHit,
            cache_key: &cache_key,
            start,
            key_ms,
            key_hash_stats: FileHashStats::default(),
            lookup_ms,
            restore_ms,
        }
        .report(config, &meta);
        compiler.commit_preprocess_memo(&file_hasher);
        return Ok(0);
    }

    let _flight = precompiled.as_mut().and_then(|pre| pre.flight.take());
    let (result, compile_time_ms, inputs_changed) = match precompiled.take() {
        Some(pre) => {
            // Inputs are fingerprinted after a deferred compile; one written
            // since this invocation started may not be what the compiler
            // read, so neither the entry nor the memo may describe it. The
            // capture ran on the outer hasher; this one saw only what the
            // key hashed afterwards, so both verdicts count.
            let changed = captured_inputs_changed || file_hasher.too_new();
            if changed {
                tracing::debug!(
                    "cc: {} read an input modified during the build; not storing it",
                    crate_name
                );
            }
            (pre.result, pre.compile_time_ms, changed)
        }
        None => {
            let compile_start = std::time::Instant::now();
            let result = match compiler.execute(parsed) {
                Ok(r) => r,
                // A spawn-level failure (missing binary, ENOMEM, fork pressure
                // under load) must not abort the build: fall back to
                // passthrough so the configured fallback wrapper still gets a
                // chance and the user sees the real compiler error rather
                // than a kache anyhow chain.
                Err(e) => {
                    return cc_passthrough_with_event(
                        config,
                        parsed,
                        crate_name,
                        event_root,
                        start,
                        format!("compiler spawn failed: {e}"),
                    );
                }
            };
            miss_guard.record_compile_rss(crate_name);
            let compile_time_ms = compile_start.elapsed().as_millis() as u64;
            replay_diagnostics(
                &result.stdout,
                result.pending_stderr(),
                std::io::stdout(),
                std::io::stderr(),
            );
            (result, compile_time_ms, false)
        }
    };

    // Only store on a clean compile that actually produced its
    // object file. A failed compile (exit != 0) or one whose output
    // discovery came up empty is not cacheable — return the exit
    // code and let cargo see the failure.
    let store_start = std::time::Instant::now();
    let mut store_put = StorePutResult::default();
    let mut store_error = String::new();
    let store_candidate = cc_store_candidate(
        should_store_cc_result(result.exit_code, !result.artifacts.is_empty()),
        inputs_changed,
        peer_committed,
    );
    // Without a live daemon, keep the ordinary staging and memo path.
    // The lifetime lock works for both Unix sockets and Windows pipes.
    let daemon_publish = config.daemon_publish
        && crate::daemon::existing_daemon_run_lock_is_held(&config.socket_path()).unwrap_or(false);
    // A deferred compile's memo goes to the daemon with the entry; the
    // wrapper records it only if the hand-off does not happen (below). Only
    // a deferred compile captures one, and a compile that is no store
    // candidate neither hands it off nor records it.
    let handoff_memo = daemon_publish
        .then(|| compiler.captured_preprocess_memo())
        .flatten();
    if store_candidate && handoff_memo.is_none() {
        compiler.commit_preprocess_memo(&file_hasher);
    }
    let publishes_to_remote = cc_publishes_to_remote(parsed);
    let admitted = store_admits_compile(config, compile_time_ms, publishes_to_remote);
    let store_decision = cc_store_decision(store_candidate, admitted);
    if store_decision.admission_skipped {
        tracing::debug!(
            crate_name = %crate_name,
            compile_time_ms,
            min_store_compile_ms = config.min_store_compile_ms,
            "admission: compile too cheap to store"
        );
    }
    if store_decision.should_store
        && cc_store_revalidates_include_dirs(parsed.mode)
        && !compiler.include_dir_names_still_match(parsed)
    {
        tracing::debug!(
            crate_name = %crate_name,
            "cc include-dir names changed during compile; skipping store"
        );
    } else if store_decision.should_store {
        let _trace = crate::phase_trace::phase("store");
        let depinfo_anchor = cc_depinfo_rewrite_root(parsed);
        let target = parsed.cache_target_arch();
        let staging_dir = daemon_publish.then(|| crate::daemon_publish::handoff_dir(config));
        match prepare_cc_store_files_in(
            &result.artifacts,
            depinfo_anchor.as_deref(),
            staging_dir.as_deref(),
        ) {
            Ok(prepared) => {
                let stdout = if crate::compiler::cc::cc_expansion_is_stdout(parsed) {
                    ""
                } else {
                    &result.stdout
                };
                // The put and everything after it are wall-clock time on a
                // build script's serial C compiles. Hand them to the daemon
                // when it will take them; it holds the key from then on.
                if daemon_publish {
                    let handoff = CcHandoff {
                        cache_key: &cache_key,
                        crate_name,
                        target: &target,
                        files: &prepared.files,
                        stdout,
                        stderr: &result.stderr,
                        compile_time_ms,
                        publishes_to_remote,
                        event_root,
                        start,
                        size: result.artifacts.total_size(),
                        key_ms,
                        lookup_ms,
                        lookup_rejection: &lookup_rejection,
                        store_start,
                        memo: handoff_memo,
                    };
                    match hand_off_cc_store(config, store, &mut _build_lock, handoff) {
                        CcHandoffOutcome::Accepted => {
                            compiler.discard_preprocess_memo();
                            return Ok(result.exit_code);
                        }
                        CcHandoffOutcome::Done => {
                            compiler.commit_preprocess_memo(&file_hasher);
                            return Ok(result.exit_code);
                        }
                        CcHandoffOutcome::Publish => {
                            compiler.commit_preprocess_memo(&file_hasher);
                        }
                    }
                }
                match store.put_with_compile_time_independent(
                    &cache_key,
                    crate_name,
                    &[], // crate_types: n/a for cc objects
                    &[], // features: n/a
                    &target,
                    "", // profile: n/a (opt level is in the key)
                    &prepared.files,
                    stdout,
                    &result.stderr,
                    compile_time_ms,
                ) {
                    Ok(result) => {
                        store_put = result;
                        // Store grew — throttled size check + detached background GC if over
                        // budget (kunobi-ninja/kache#497). Never blocks the compile path.
                        maybe_spawn_auto_gc(config, store);
                        flush_or_hand_off_durability(config, store, &cache_key);
                        maybe_enqueue_upload(
                            config,
                            store,
                            &cache_key,
                            crate_name,
                            publishes_to_remote,
                        );
                    }
                    Err(e) => {
                        store_error = store_error_for_event(&e);
                        tracing::warn!(
                            "failed to store cc cache entry for {}: {}",
                            crate_name,
                            store_error
                        );
                    }
                }
            }
            Err(e) => {
                store_error = store_error_for_event(&e);
                tracing::warn!(
                    "failed to prepare cc cache entry for {}: {}",
                    crate_name,
                    store_error
                );
            }
        }
    }
    // A skipped admission or failed snapshot never transfers the memo.
    // Keep it for the next invocation even when no artifact was stored.
    if store_candidate {
        compiler.commit_preprocess_memo(&file_hasher);
    }
    let store_ms = store_start.elapsed().as_millis() as u64;

    let elapsed = start.elapsed().as_millis() as u64;
    let size = result.artifacts.total_size();
    let event_result = event_result_for_store_admission(store_candidate, admitted, store_put);
    log_event_with_store_and_lookup_outcome(
        config,
        event_root,
        crate_name,
        event_result,
        elapsed,
        compile_time_ms,
        size,
        &cache_key,
        key_ms,
        FileHashStats::default(),
        lookup_ms,
        0,
        store_ms,
        store_put,
        store_error,
        lookup_rejection,
    );
    print_progress(crate_name, event_result, elapsed, size);
    Ok(result.exit_code)
}

/// Whether a failed cc key must bypass the fallback wrapper: the key could not
/// see a file the assembler reads, and a wrapper keyed on the same
/// preprocessor output cannot see it either (kunobi-ninja/kache#1015).
fn cc_key_error_skips_fallback(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::compiler::cc::CcHiddenInput>()
        .is_some()
}

/// Format a refusal as the structured passthrough reason `category|detail`
/// the report renderers parse into columns. `category` is the coarse class
/// (`unsupported` / `not-a-compile`) of the first reason; `detail` joins the
/// specific reasons. Deliberately NOT prefixed "refused:" / "failed:" — a
/// refusal is a scope decision (the build runs the compiler normally), not an
/// error, and the renderer supplies the `action` (`reject` / `fallback`).
fn refuse_reason_string(refuse: &[crate::compiler::RefuseReason]) -> String {
    let category = refuse.first().map_or("unsupported", |r| r.category());
    let detail = refuse
        .iter()
        .map(|r| r.description())
        .collect::<Vec<_>>()
        .join("; ");
    format!("{category}|{detail}")
}

/// Run a cc-family invocation without caching — invoke the compiler
/// with the original argv, propagate stdout / stderr / exit.
fn cc_passthrough(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
) -> Result<PassthroughOutput> {
    cc_passthrough_impl(config, parsed, false)
}

fn cc_direct_passthrough(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
) -> Result<PassthroughOutput> {
    cc_passthrough_impl(config, parsed, true)
}

fn cc_passthrough_impl(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
    force_direct: bool,
) -> Result<PassthroughOutput> {
    let mut fallback_attempt = None;
    // Configured fallback wrapper: `<fallback> <cc> <args>`.
    // kache's C/C++ coverage is narrower than its rustc support, so
    // the fallback is most valuable on this path. Falls through to a
    // direct compilation if the fallback fails.
    if let Some(fb) = config.fallback.as_deref()
        && !force_direct
        && !parsed.requires_compiler_output_semantics()
    {
        let mut cmd = std::process::Command::new(fb);
        cmd.arg(&parsed.program);
        cmd.args(&parsed.rest);
        let outputs: Vec<&Path> = parsed
            .output
            .as_deref()
            .map(Path::new)
            .into_iter()
            .collect();
        let attempt = crate::fallback::run(cmd, fb, &outputs, &parsed.rest);
        if let Some(exit_code) = attempt.terminal_code() {
            return Ok(PassthroughOutput {
                exit_code,
                fallback: true,
                fallback_attempt: Some(attempt),
            });
        }
        fallback_attempt = Some(attempt);
    }

    // A refusal means Kache has promised to preserve the selected compiler's
    // behavior exactly. The cache-miss execution path injects prefix-map flags
    // and SOURCE_DATE_EPOCH for reproducible cache entries, so it cannot be
    // reused here: even an added flag can change how a compiler replaces an
    // existing output path (#645).
    refuse_legacy_cc_blob_outputs(config, parsed)?;
    crate::opcounts::record_compiler_run();
    let status = std::process::Command::new(&parsed.program)
        .args(&parsed.rest)
        .status()
        .with_context(|| format!("executing {}", parsed.program))?;
    Ok(PassthroughOutput {
        exit_code: status.code().unwrap_or(1),
        fallback: false,
        fallback_attempt,
    })
}

pub(crate) fn refuse_legacy_cc_blob_outputs(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
) -> Result<()> {
    let store_dir = config.store_dir();
    for output in parsed.compiler_output_paths() {
        if let Some(blob) = Store::matching_readonly_blob_inode(&store_dir, &output)? {
            anyhow::bail!(
                "refusing to invoke the compiler because {} still shares the \
                 read-only cache blob {}; remove the build output and retry",
                output.display(),
                blob.display()
            );
        }
    }
    Ok(())
}

fn cache_entry_has_files(meta: &crate::store::EntryMeta) -> bool {
    !meta.files.is_empty()
}

fn cc_scheduled_hit_ok(
    parsed: &crate::compiler::cc::CcArgs,
    meta: &crate::store::EntryMeta,
) -> bool {
    cache_entry_has_files(meta) && cc_cache_entry_rejection_reason(parsed, meta).is_none()
}

#[cfg(test)]
fn cc_cache_entry_satisfies_invocation(
    parsed: &crate::compiler::cc::CcArgs,
    meta: &crate::store::EntryMeta,
) -> bool {
    cc_cache_entry_rejection_reason(parsed, meta).is_none()
}

/// Where a cached preprocess artifact goes, if this invocation is the one
/// that asked for it.
///
/// A preprocess output is not an object and has no fixed extension, so the
/// only thing that identifies it is the name the invocation named. `None` for
/// any other mode, and for an entry naming a different file: restoring one of
/// those would report a hit and leave the build with the wrong bytes, or with
/// none at all.
fn cc_preprocess_restore_target(
    parsed: &crate::compiler::cc::CcArgs,
    cached_name: &str,
) -> Option<std::path::PathBuf> {
    if parsed.mode != crate::compiler::cc::CompileMode::Preprocess {
        return None;
    }
    let target = parsed.object_output_path()?;
    let names_match = target
        .file_name()
        .is_some_and(|name| name.to_string_lossy() == cached_name);
    names_match.then_some(target)
}

fn cc_store_revalidates_include_dirs(mode: crate::compiler::cc::CompileMode) -> bool {
    mode == crate::compiler::cc::CompileMode::Compile
}

fn cc_cache_entry_rejection_reason(
    parsed: &crate::compiler::cc::CcArgs,
    meta: &crate::store::EntryMeta,
) -> Option<&'static str> {
    let has_object = meta
        .files
        .iter()
        .any(|file| classify_by_filename(&file.name) == ArtifactKind::Object);
    let has_depinfo = meta
        .files
        .iter()
        .any(|file| classify_by_filename(&file.name) == ArtifactKind::DepInfo);

    let has_named_output = meta
        .files
        .iter()
        .any(|file| cc_preprocess_restore_target(parsed, &file.name).is_some());
    let has_stdout = meta
        .files
        .iter()
        .any(|file| file.name == crate::compiler::cc::CC_STDOUT_STORE_NAME);

    match parsed.mode {
        crate::compiler::cc::CompileMode::Compile if !has_object => {
            Some("matching entry lacks the object artifact required by this invocation")
        }
        crate::compiler::cc::CompileMode::Preprocess
            if crate::compiler::cc::cc_expansion_is_stdout(parsed) && !has_stdout =>
        {
            Some(
                "matching entry lacks the preprocessor stdout artifact required by this invocation",
            )
        }
        crate::compiler::cc::CompileMode::Preprocess
            if parsed.output.is_some() && !has_named_output =>
        {
            Some("matching entry lacks the preprocessed output required by this invocation")
        }
        crate::compiler::cc::CompileMode::Link if meta.files.is_empty() => {
            Some("matching entry lacks the link artifact required by this invocation")
        }
        _ if parsed.depinfo_output_path().is_some() && !has_depinfo => {
            Some("matching entry lacks dep-info required by this invocation")
        }
        _ => None,
    }
}

fn cc_depinfo_rewrite_root(parsed: &crate::compiler::cc::CcArgs) -> Option<std::path::PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    cc_depinfo_rewrite_root_from_cwd(parsed, &cwd)
}

fn rustc_event_root(args: &RustcArgs) -> String {
    let written_to = args.out_dir.clone().or_else(|| {
        args.output
            .as_deref()
            .and_then(Path::parent)
            .map(Path::to_path_buf)
    });
    event_root_string(event_root_override().or_else(|| {
        written_to
            .as_deref()
            .and_then(cargo_workspace_of)
            .or_else(|| args.workspace_root())
            .or_else(|| std::env::current_dir().ok())
    }))
}

fn cc_event_root(parsed: &crate::compiler::cc::CcArgs) -> String {
    cc_event_root_in(parsed, std::env::var_os("OUT_DIR"))
}

fn cc_event_root_in(
    parsed: &crate::compiler::cc::CcArgs,
    out_dir: Option<std::ffi::OsString>,
) -> String {
    event_root_string(
        event_root_override()
            .or_else(|| out_dir_workspace(out_dir))
            .or_else(|| cc_depinfo_rewrite_root(parsed).or_else(|| std::env::current_dir().ok())),
    )
}

/// The workspace whose Cargo target directory holds `dir`: the parent of the
/// nearest ancestor carrying Cargo's `CACHEDIR.TAG`.
///
/// Every unit of one `cargo build` writes somewhere below that directory, so
/// this gives a build script (`target/debug/build/<pkg>`), the rustc probes it
/// runs (`.../<pkg>/out`), and its cc compiles the same root as the crates.
/// Deriving the root from the output layout instead named `target` or
/// `target/debug` for those units and split one build into several (#1081).
/// The tag's text is checked, because other tools also write `CACHEDIR.TAG`.
fn cargo_workspace_of(dir: &Path) -> Option<PathBuf> {
    dir.ancestors()
        .find(|ancestor| is_cargo_cachedir_tag(&ancestor.join("CACHEDIR.TAG")))
        .and_then(Path::parent)
        .map(Path::to_path_buf)
}

fn is_cargo_cachedir_tag(path: &Path) -> bool {
    std::fs::read_to_string(path).is_ok_and(|tag| tag.contains("created by cargo"))
}

/// The workspace of the build script a compiler runs under, from the
/// `OUT_DIR` Cargo gives every build script and its child processes.
fn out_dir_workspace(out_dir: Option<std::ffi::OsString>) -> Option<PathBuf> {
    let out_dir = out_dir.filter(|value| !value.is_empty())?;
    cargo_workspace_of(Path::new(&out_dir))
}

fn event_root_override() -> Option<PathBuf> {
    std::env::var_os("KACHE_EVENT_ROOT")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn event_root_string(root: Option<PathBuf>) -> String {
    let Some(root) = root else {
        return String::new();
    };
    let abs = if root.is_absolute() {
        root
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

fn cc_depinfo_rewrite_root_from_cwd(
    parsed: &crate::compiler::cc::CcArgs,
    cwd: &Path,
) -> Option<std::path::PathBuf> {
    parsed.depinfo_output_path()?;

    let object_anchor = parsed
        .depinfo_anchor()
        .map(|anchor| absolute_clean_path(&anchor, cwd))?;
    let source_anchor = parsed
        .sources
        .first()
        .map(|source| absolute_clean_path(source, cwd))
        .and_then(|source| source.parent().map(Path::to_path_buf));

    source_anchor
        .and_then(|source| common_path_prefix(&source, &object_anchor))
        .filter(|root| root.components().any(|c| matches!(c, Component::Normal(_))))
        .or(Some(object_anchor))
}

fn absolute_clean_path(path: &Path, cwd: &Path) -> std::path::PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    };
    clean_path(&absolute)
}

fn clean_path(path: &Path) -> std::path::PathBuf {
    let mut cleaned = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !cleaned.pop() {
                    cleaned.push(component.as_os_str());
                }
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                cleaned.push(component.as_os_str());
            }
        }
    }
    if cleaned.as_os_str().is_empty() {
        Path::new(".").to_path_buf()
    } else {
        cleaned
    }
}

fn common_path_prefix(left: &Path, right: &Path) -> Option<std::path::PathBuf> {
    let mut prefix = std::path::PathBuf::new();
    let mut matched = false;
    for (left_component, right_component) in left.components().zip(right.components()) {
        if left_component != right_component {
            break;
        }
        prefix.push(left_component.as_os_str());
        matched = true;
    }
    matched.then_some(prefix)
}

#[derive(Debug)]
struct PartialCcRestore;

impl std::fmt::Display for PartialCcRestore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cc cache restore published only part of the output set")
    }
}

impl std::error::Error for PartialCcRestore {}

fn prepare_cc_cached_artifact(
    store: &Store,
    cached: &crate::store::CachedFile,
    target: &Path,
    kind: ArtifactKind,
    depinfo_anchor: &Path,
) -> Result<link::PreparedWritableTarget> {
    let transforms: Vec<_> = plan_post_restore(kind)
        .into_iter()
        .filter(|action| action.is_content_transform())
        .collect();

    let blob = store.blob_path(&cached.hash);
    if !blob.exists() {
        anyhow::bail!(
            "cc restore: blob for {} (hash {}) was evicted before restore: {}",
            cached.name,
            &cached.hash[..16.min(cached.hash.len())],
            blob.display()
        );
    }

    if transforms.is_empty() {
        return link::prepare_writable_target_from_file(&blob, target).with_context(|| {
            format!(
                "cc restore: staging {} -> {}",
                blob.display(),
                target.display()
            )
        });
    }

    let mut content = std::fs::read(&blob)
        .with_context(|| format!("cc restore: reading blob {}", blob.display()))?;
    for action in transforms {
        content = action.transform(content, depinfo_anchor);
    }
    link::prepare_writable_target_from_bytes(target, &content)
        .with_context(|| format!("cc restore: staging transformed {}", target.display()))
}

fn apply_cc_post_publish_actions(published: &[(PathBuf, ArtifactKind)]) -> Result<()> {
    let host = platform::current();
    for (path, kind) in published {
        for action in plan_post_restore(*kind) {
            if action.is_content_transform() {
                continue;
            }
            action.apply(path, &*host).with_context(|| {
                format!("cc restore: applying {action:?} to {}", path.display())
            })?;
        }
    }
    Ok(())
}

fn publish_prepared_cc_artifacts(prepared: Vec<link::PreparedWritableTarget>) -> Result<()> {
    publish_prepared_cc_artifacts_with(prepared, |_, _| Ok(()))
}

fn publish_prepared_cc_artifacts_with(
    prepared: Vec<link::PreparedWritableTarget>,
    mut before_publish: impl FnMut(usize, &Path) -> Result<()>,
) -> Result<()> {
    // Validate the whole set before making any final pathname visible.
    let mut replace_existing = Vec::with_capacity(prepared.len());
    for artifact in &prepared {
        if cc_output_path_requires_passthrough(artifact.target()) {
            anyhow::bail!(
                "cc restore: output path changed and now requires compiler passthrough semantics"
            );
        }
        replace_existing.push(std::fs::symlink_metadata(artifact.target()).is_ok());
    }

    for (index, (artifact, replace_existing)) in
        prepared.into_iter().zip(replace_existing).enumerate()
    {
        if let Err(error) = before_publish(index, artifact.target()) {
            return if index == 0 {
                Err(error)
            } else {
                Err(error.context(PartialCcRestore))
            };
        }
        let publish = if replace_existing {
            if cc_output_path_requires_passthrough(artifact.target()) {
                Err(anyhow::anyhow!(
                    "cc restore: output path changed and now requires compiler passthrough semantics"
                ))
            } else {
                artifact.publish_replacing()
            }
        } else {
            artifact.publish()
        };
        if let Err(error) = publish {
            return if index == 0 {
                Err(error)
            } else {
                Err(error.context(PartialCcRestore))
            };
        }
    }
    Ok(())
}

fn restore_cc_stdout_from_cache(
    store: &Store,
    meta: &crate::store::EntryMeta,
    writer: &mut impl std::io::Write,
) -> Result<()> {
    let cached = meta
        .files
        .iter()
        .find(|file| file.name == crate::compiler::cc::CC_STDOUT_STORE_NAME)
        .context("cc restore: -E stdout entry has no stdout.i blob")?;
    let blob = store.blob_path(&cached.hash);
    let mut file = std::fs::File::open(&blob)
        .with_context(|| format!("cc restore: opening {}", blob.display()))?;
    std::io::copy(&mut file, writer).context("cc restore: writing -E stdout")?;
    writer.flush().context("cc restore: flushing -E stdout")?;
    Ok(())
}

/// Restore cached cc artifacts to this invocation's output paths.
///
/// Every artifact is staged first. Absent paths use no-clobber publication;
/// validated ordinary existing outputs are atomically replaced. If a race wins
/// after publication starts, the caller receives `PartialCcRestore` and must
/// not run the compiler over the partially restored output set.
fn restore_cc_from_cache(
    store: &Store,
    parsed: &crate::compiler::cc::CcArgs,
    meta: &crate::store::EntryMeta,
) -> Result<()> {
    if crate::compiler::cc::cc_expansion_is_stdout(parsed) {
        return restore_cc_stdout_from_cache(store, meta, &mut std::io::stdout());
    }
    if parsed.requires_compiler_output_semantics() {
        anyhow::bail!("cc restore: existing output requires compiler passthrough semantics");
    }

    let depinfo_anchor =
        cc_depinfo_rewrite_root(parsed).unwrap_or_else(|| Path::new(".").to_path_buf());
    let mut prepared = Vec::new();
    let mut published_kinds = Vec::new();
    let mut targets = std::collections::HashSet::new();

    for cached in &meta.files {
        let kind = classify_by_filename(&cached.name);
        let target = match kind {
            ArtifactKind::Object => parsed
                .object_output_path()
                .context("cc restore: cannot determine object output path")?,
            ArtifactKind::DepInfo => match parsed.depinfo_output_path() {
                Some(path) => path,
                None => {
                    tracing::debug!(
                        "cc restore: cached dep-info {} not requested by invocation; skipping",
                        cached.name
                    );
                    continue;
                }
            },
            ArtifactKind::Executable
            | ArtifactKind::DynamicLibrary
            | ArtifactKind::WasmModule
            | ArtifactKind::Other("extensionless")
                if parsed.mode == crate::compiler::cc::CompileMode::Link =>
            {
                parsed
                    .object_output_path()
                    .context("cc restore: cannot determine link output path")?
            }
            ArtifactKind::DebugSidecar
            | ArtifactKind::DebugBundle
            | ArtifactKind::Library
            | ArtifactKind::Other(_)
                if parsed.mode == crate::compiler::cc::CompileMode::Link =>
            {
                let parent = parsed
                    .object_output_path()
                    .and_then(|path| path.parent().map(PathBuf::from))
                    .unwrap_or_else(|| PathBuf::from("."));
                parent.join(&cached.name)
            }
            // Anything else is this invocation's own preprocessor output or
            // nothing we can place. Asking once keeps the answer and the
            // decision to use it from ever disagreeing.
            _ => match cc_preprocess_restore_target(parsed, &cached.name) {
                Some(target) => target,
                None => {
                    tracing::debug!(
                        "cc restore: cached artifact {} has unsupported kind {:?}; skipping",
                        cached.name,
                        kind
                    );
                    continue;
                }
            },
        };

        // Recheck immediately before the path-based restore. The initial
        // refusal happens before lookup; this catches ordinary path changes
        // during key computation and narrows the window before restore.
        if cc_output_path_requires_passthrough(&target) {
            anyhow::bail!(
                "cc restore: output path changed and now requires compiler passthrough semantics"
            );
        }
        anyhow::ensure!(
            targets.insert(target.clone()),
            "cc restore: cache entry maps multiple artifacts to {}",
            target.display()
        );
        prepared.push(prepare_cc_cached_artifact(
            store,
            cached,
            &target,
            kind,
            &depinfo_anchor,
        )?);
        published_kinds.push((target, kind));
    }
    publish_prepared_cc_artifacts(prepared)?;
    apply_cc_post_publish_actions(&published_kinds)?;
    #[cfg(unix)]
    if parsed.mode == crate::compiler::cc::CompileMode::Link
        && let Some(output) = parsed.object_output_path()
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(&output)
            .with_context(|| format!("cc restore: stat {}", output.display()))?
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&output, permissions)
            .with_context(|| format!("cc restore: chmod +x {}", output.display()))?;
    }
    Ok(())
}

/// After a local miss, ask the daemon for an exact remote entry.
/// Returns `Some(exit)` when the hit was restored (or the restore fell through
/// to passthrough). `None` means continue to compile.
fn cc_try_remote_hit(
    config: &Config,
    store: &Store,
    compiler: &CcCompiler,
    parsed: &crate::compiler::cc::CcArgs,
    file_hasher: &crate::cache_key::FileHasher<'_>,
    cache_key: &str,
    crate_name: &str,
    event_root: &str,
    start: std::time::Instant,
    key_ms: u64,
    lookup_ms: u64,
) -> Result<Option<i32>> {
    if !compiler_remote_enabled(config, cc_publishes_to_remote(parsed)) {
        return Ok(None);
    }
    let Some((meta, event_result)) = acquire_entry(
        config,
        store,
        cache_key,
        crate_name,
        NegativeReply::ContinueCompile,
    ) else {
        return Ok(None);
    };
    let restore_start = std::time::Instant::now();
    if let Err(e) = restore_cc_from_cache(store, parsed, &meta) {
        if e.downcast_ref::<PartialCcRestore>().is_some() {
            return Err(e);
        }
        tracing::warn!(
            "restoring cc remote cache hit for {} failed: {} — recompiling",
            crate_name,
            e
        );
        return Ok(Some(cc_passthrough_with_event(
            config,
            parsed,
            crate_name,
            event_root,
            start,
            format!("restore failed: {e}"),
        )?));
    }
    let restore_ms = restore_start.elapsed().as_millis() as u64;
    HitCompletion {
        event_root,
        crate_name,
        result: event_result,
        cache_key,
        start,
        key_ms,
        key_hash_stats: FileHashStats::default(),
        lookup_ms,
        restore_ms,
    }
    .report(config, &meta);
    compiler.commit_preprocess_memo(file_hasher);
    Ok(Some(0))
}

/// Run kache in RUSTC_WRAPPER mode.
///
/// This is the hot path — called once per crate by cargo.
/// Flow: parse args → compute cache key → check store → link on hit → compile on miss → store → link
pub fn run(config: &Config, wrapper_args: &[String]) -> Result<i32> {
    let _trace = crate::phase_trace::start("rustc", wrapper_args);
    let start = wrapper_entry();
    crate::link::set_windows_hardlink_restore(config.windows_hardlink);
    crate::link::set_shared_hardlink_restores(config.shared_hardlink_restores);
    crate::link::set_storage_layout_advice(config.storage_layout_advice);
    crate::link::set_layout_advice_to_log(true);
    crate::link::set_cow_warn_marker(warn_marker_path("cow", &config.cache_dir));
    warn_nonlocal_cache_fs_once(config);
    // Wall-clock build-start (ns since epoch) for the optional too-new-input
    // guard; compared against keyed inputs' mtime/ctime (kunobi-ninja/kache#324).
    let invocation_start_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);

    // Parse the rustc arguments (wrapper_args[0] is the rustc path).
    // Routed through the Compiler trait — see src/compiler/mod.rs. RustcArgs
    // remains the canonical parsed shape; the trait gives us a stable contract
    // when adding gcc/clang.
    let compiler = RustcCompiler::new().with_base_dirs(config.base_dirs.clone());
    let args = compiler
        .parse(wrapper_args)
        .context("parsing rustc arguments")?;
    // Resolve once before any cache/passthrough fast path. The same snapshot
    // drives the key and the final Cargo-facing dep-info, so a concurrent
    // config/glob change cannot make those two views disagree.
    let extra_inputs_key_start = std::time::Instant::now();
    let mut extra_inputs_hasher =
        crate::cache_key::FileHasher::new().with_daemon(config.socket_path());
    if config.modified_input_guard {
        extra_inputs_hasher.arm_too_new_guard(invocation_start_ns, 0);
    }
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    let trace_extra = crate::phase_trace::phase("extra_inputs_resolve");
    let extra_inputs =
        crate::extra_inputs::ExtraInputsSnapshot::resolve_for_rustc(&args, &extra_inputs_hasher)
            .with_context(|| format!("resolving extra_inputs for {crate_name}"))?;

    drop(trace_extra);
    validate_extra_inputs_freshness_mode(&args, extra_inputs.is_some())?;

    let extra_inputs_hash_stats = extra_inputs_hasher.stats();
    let extra_inputs_too_new = extra_inputs_hasher.too_new();
    let extra_inputs_guard_inputs = extra_inputs_hasher.take_guarded_inputs();
    let extra_inputs_key_ms = extra_inputs_key_start.elapsed().as_millis() as u64;
    // A fallback cache does not know Kache's extra-input digest. If Kache
    // declines an invocation, delegating it could restore the exact stale
    // artifact this declaration is meant to prevent. Keep the fallback for
    // ordinary crates, but use a plain compiler passthrough for this one.
    let mut safe_extra_inputs_config = None;
    if extra_inputs.is_some() && (config.fallback.is_some() || config.preserve_incremental) {
        let mut safe = config.clone();
        if safe.fallback.take().is_some() {
            tracing::debug!("disabling fallback cache for active extra_inputs crate {crate_name}");
        }
        if safe.preserve_incremental {
            tracing::debug!(
                "disabling preserved incremental state for active extra_inputs crate {crate_name}"
            );
            safe.preserve_incremental = false;
        }
        safe_extra_inputs_config = Some(safe);
    }
    let effective_config = safe_extra_inputs_config.as_ref().unwrap_or(config);
    let exit = run_parsed_rustc(
        effective_config,
        &compiler,
        &args,
        start,
        invocation_start_ns,
        extra_inputs.as_ref(),
        extra_inputs_hash_stats,
        extra_inputs_too_new,
        extra_inputs_key_ms,
        extra_inputs_guard_inputs,
        None,
    )?;

    if exit == 0 {
        complete_current_extra_inputs_after_success(
            effective_config,
            &args,
            extra_inputs.as_ref(),
        )?;
        // Cargo hardlinks and runs the binary after this wrapper returns, so
        // this is the last moment to put the launcher in its place.
        crate::build_script::install_shim(&args);
    }
    Ok(exit)
}

/// Event root for a build-script run: the workspace whose target holds its
/// `OUT_DIR`, so the run joins the build that triggered it, falling back to
/// the package's manifest directory outside a tagged target (#1081).
pub(crate) fn build_script_event_root(out_dir: &Path, manifest_dir: &Path) -> String {
    event_root_string(
        event_root_override()
            .or_else(|| cargo_workspace_of(out_dir))
            .or_else(|| Some(manifest_dir.to_path_buf())),
    )
}

/// Event for one build-script run, on the same log as compiler events.
#[allow(clippy::too_many_arguments)]
pub(crate) fn log_build_script_event(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
    store_put: StorePutResult,
) {
    log_event_with_store_and_lookup_outcome(
        config,
        root,
        crate_name,
        result,
        elapsed_ms,
        0,
        size,
        cache_key,
        key_ms,
        FileHashStats::default(),
        lookup_ms,
        restore_ms,
        store_ms,
        store_put,
        String::new(),
        String::new(),
    );
}

pub(crate) fn resolve_extra_inputs_for_passthrough(
    config: &Config,
    args: &RustcArgs,
) -> Result<Option<crate::extra_inputs::ExtraInputsSnapshot>> {
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    let hasher = crate::cache_key::FileHasher::new().with_daemon(config.socket_path());
    let snapshot = crate::extra_inputs::ExtraInputsSnapshot::resolve_for_rustc(args, &hasher)
        .with_context(|| format!("resolving extra_inputs for {crate_name}"))?;
    validate_extra_inputs_freshness_mode(args, snapshot.is_some())?;
    Ok(snapshot)
}

fn validate_extra_inputs_freshness_mode(args: &RustcArgs, active: bool) -> Result<()> {
    if active && args.checksum_freshness_enabled() {
        anyhow::bail!(
            "extra_inputs cannot safely complete Cargo checksum-freshness dep-info yet; \
             disable -Z checksum-freshness, or run the whole Cargo command with \
             KACHE_DISABLED=1 while retaining matching cargo:rerun-if-changed directives"
        );
    }
    Ok(())
}

pub(crate) fn complete_current_extra_inputs_after_success(
    config: &Config,
    args: &RustcArgs,
    original: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
) -> Result<()> {
    let current = resolve_extra_inputs_for_passthrough(config, args)?;
    if current.as_ref() != original {
        anyhow::bail!(
            "extra_inputs declaration changed while the compiler wrapper was running; retry the build"
        );
    }
    match current.as_ref() {
        Some(snapshot) => complete_extra_inputs_dep_info(args, snapshot),
        None => Ok(()),
    }
}

pub(crate) fn complete_extra_inputs_dep_info(
    args: &RustcArgs,
    snapshot: &crate::extra_inputs::ExtraInputsSnapshot,
) -> Result<()> {
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    let Some(dep_info_path) = args.dep_info_path() else {
        tracing::debug!(
            "extra_inputs dep-info completion skipped because rustc did not request a supported \
             dep-info path for {crate_name}; non-Cargo callers retain their own freshness mechanism"
        );
        return Ok(());
    };
    snapshot
        .merge_into_dep_info(&dep_info_path)
        .with_context(|| {
            format!(
                "completing Cargo dep-info {} for extra_inputs",
                dep_info_path.display()
            )
        })
}

fn extra_inputs_changed_during_compile(
    config: &Config,
    args: &RustcArgs,
    before: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
    invocation_start_ns: i64,
) -> bool {
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    let mut hasher = crate::cache_key::FileHasher::new().with_daemon(config.socket_path());
    hasher.arm_too_new_guard(invocation_start_ns, 0);
    let after = match crate::extra_inputs::ExtraInputsSnapshot::resolve_for_rustc(args, &hasher) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            tracing::warn!(
                "not caching {crate_name}: extra_inputs could not be revalidated after compile: {error:#}"
            );
            return true;
        }
    };
    if before != after.as_ref() {
        tracing::warn!(
            "not caching {crate_name}: extra_inputs changed while the compiler was running"
        );
        return true;
    }
    if key_inputs_changed_during_compile(hasher.too_new(), &hasher.take_guarded_inputs()) {
        tracing::warn!(
            "not caching {crate_name}: extra_inputs may have changed while the compiler was running"
        );
        return true;
    }
    false
}

fn run_parsed_rustc(
    config: &Config,
    compiler: &RustcCompiler,
    args: &RustcArgs,
    start: std::time::Instant,
    invocation_start_ns: i64,
    extra_inputs: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
    extra_inputs_hash_stats: FileHashStats,
    extra_inputs_too_new: bool,
    extra_inputs_key_ms: u64,
    extra_inputs_guard_inputs: Vec<crate::cache_key::FileFingerprint>,
    mut precompiled: Option<Precompiled>,
) -> Result<i32> {
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    let event_root = rustc_event_root(args);
    // In-flight heartbeats (kunobi-ninja/kache#131): armed once per wrapper
    // process; the monitor only actually starts if this invocation reaches a
    // miss compile, and only beats once the compile outlives one cadence.
    crate::heartbeat::set_heartbeat_ctx(
        config.heartbeat_secs,
        config.event_log_path(),
        config.socket_path(),
        event_root.clone(),
        heartbeat_lines_enabled(progress_level()),
    );
    // A fresh machine has no prediction rows; let the key ask the remote for
    // a portable one before paying the pre-pass (kunobi-ninja/kache#1011).
    crate::cache_key::set_remote_rows(remote_prediction_rows(config));
    // Mutation testing repeatedly changes a local crate while keeping its
    // dependencies stable. Exact artifact keys necessarily miss for each new
    // mutant, while rustc's incremental state is designed for this workload.
    // In the explicit hybrid mode, bypass before opening the store or running
    // the dep-info key pass; non-incremental dependencies still use kache.
    let preserve_incremental = preserve_incremental_requested(config, args);
    if preserve_incremental && compile::isolate_incremental_flags(&args.all_args).is_some() {
        tracing::debug!("preserving incremental compilation for {crate_name}");
        return preserved_incremental_with_event(config, args, crate_name, &event_root, start);
    }
    if preserve_incremental {
        tracing::warn!(
            "[kache] incremental directory for {crate_name} has no safe sibling path; stripping incremental flags"
        );
    }
    // A force-listed unit may skip the cache only through the same narrow,
    // policy-owned layout as adaptive incremental. Unsafe/non-Cargo paths,
    // hidden inputs, and lease contention simply leave `adaptive_unit` empty
    // (or fail to grant a lease) and continue through the normal cache path,
    // where Cargo's original incremental argument is stripped.
    let force_incremental = force_incremental_requested(config, args);
    let adaptive_policy_for_invocation = adaptive_seed_allowed(config, args);
    let trace_adaptive = crate::phase_trace::phase("adaptive_unit");
    let adaptive_unit = managed_incremental_unit(
        config,
        args,
        std::env::var_os("CARGO_PRIMARY_PACKAGE").is_some(),
        || extra_inputs.is_some(),
    );
    drop(trace_adaptive);

    // Evaluate every cheap cache-eligibility gate before the learned fast
    // path. In particular, changing an exclusion or executable-cache policy
    // must take effect immediately even when this unit was already active.
    let refuse = compiler.refuse_reasons(args);
    // A codegen backend loaded from a dylib can write files rustc never
    // reports (cuda-oxide writes device artifacts next to the crate), and a
    // hit would restore the artifacts without them. Such compiles bypass the
    // cache unless the user trusts the backend.
    let untrusted_codegen_backend =
        untrusted_codegen_backend(args.codegen_backend_dylib(), config.trust_codegen_backends);
    let current_dir = std::env::current_dir().ok();
    let workspace_root = args.path_normalization_root().map(Path::to_path_buf);
    let exclude_roots: Vec<_> = workspace_root
        .iter()
        .chain(current_dir.iter())
        .cloned()
        .collect();
    let excluded_source = args
        .source_file
        .as_ref()
        .filter(|source| config.project_rules.source_excluded(source, &exclude_roots));
    // User bypass rules (#222). Same fail-closed contract as `exclude`, and
    // gating the incremental fast path on it too: a bypassed unit must not
    // slip back into caching through the managed-incremental route.
    let user_bypass = config
        .project_rules
        .user_bypass_reason(crate_name, &args.all_args);
    let skip_user_facing = args.is_user_facing_executable() && !config.cache_executables;

    if incremental_fast_path_allowed(
        unit_refuses_caching(!refuse.is_empty(), untrusted_codegen_backend.is_some()),
        excluded_source.is_some() || user_bypass.is_some(),
        skip_user_facing,
    ) {
        if force_incremental {
            if let Some(lease) = adaptive_unit.as_ref().and_then(AdaptiveUnit::try_immediate) {
                return adaptive_incremental_with_event(
                    config,
                    args,
                    crate_name,
                    &event_root,
                    start,
                    lease,
                    format!("incremental force-list: {crate_name}"),
                    None,
                );
            }
        } else if let Some(lease) = adaptive_unit.as_ref().and_then(AdaptiveUnit::try_active) {
            return adaptive_incremental_with_event(
                config,
                args,
                crate_name,
                &event_root,
                start,
                lease,
                "adaptive active",
                None,
            );
        }
    }
    // Daemon-assisted local hits (kunobi-ninja/kache#565): defer the SQLite
    // open — the daemon path only opens the store when it doesn't serve the
    // hit. Incremental invocations keep the classic path: clean-incremental
    // registration needs the store up front, and restoring final artifacts
    // around live incremental state is exactly the kind of interaction an
    // experimental fast path should stay out of.
    let daemon_local = config.local_hit_daemon && args.is_primary && args.incremental.is_none();
    let rustc_route = volume_route_path_rustc(args);
    let mut fallback_store = None;
    let trace_store_open = crate::phase_trace::phase("store_open");
    let store = if daemon_local {
        None
    } else if args.is_primary || (config.clean_incremental && args.incremental.is_some()) {
        match open_primary_and_fallback(config, &rustc_route) {
            Ok((primary, fallback)) => {
                fallback_store = fallback;
                Some(primary)
            }
            Err(e) => {
                warn_store_unavailable_once(config, &e);
                None
            }
        }
    } else {
        None
    };

    if incremental_cleanup_enabled(config)
        && let Some(incr_dir) = &args.incremental
        && let Some(store) = &store
        && let Err(e) = store.remember_incremental_dir(incr_dir)
    {
        tracing::warn!(
            "failed to register incremental dir {}: {}",
            incr_dir.display(),
            e
        );
    }
    // Checked before the refusals below: those may hand the compile to a
    // configured fallback cache, which would replay the same incomplete
    // outputs.
    if untrusted_codegen_backend.is_some() {
        tracing::debug!("rustc codegen backend dylib not trusted; running rustc directly");
        reset_adaptive_unit(adaptive_unit.as_ref());
        return rustc_direct_passthrough_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            UNTRUSTED_CODEGEN_BACKEND_REASON,
        );
    }

    // Bypass the cache when the compiler tells us we can't safely cache this
    // invocation (today: only NotPrimary; future: response files, coverage,
    // time macros, etc.).
    if !refuse.is_empty() {
        let reasons: Vec<&str> = refuse.iter().map(|r| r.description()).collect();
        tracing::debug!(
            "{}: bypassing cache ({})",
            compiler.id().as_str(),
            reasons.join("; ")
        );
        reset_adaptive_unit(adaptive_unit.as_ref());
        return passthrough_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            refuse_reason_string(&refuse),
        );
    }

    if let Some(source) = excluded_source {
        tracing::debug!("rustc source excluded from cache: {}", source.display());
        reset_adaptive_unit(adaptive_unit.as_ref());
        return passthrough_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            format!("source excluded: {}", source.display()),
        );
    }

    if let Some(reason) = user_bypass {
        tracing::debug!("rustc invocation bypassed by user rule: {reason}");
        reset_adaptive_unit(adaptive_unit.as_ref());
        return passthrough_with_event(config, args, crate_name, &event_root, start, reason);
    }

    // Skip-cache only for *user-facing* executables (`bin` / `--test`).
    // dylib / cdylib / proc-macro stay cacheable: they're rustc's
    // internal artifacts, not user-shipped binaries, and verify-then-
    // sign on restore (`PostRestoreAction::Sign`) keeps macOS dyld
    // happy. Without this distinction, every proc-macro recompiled
    // fresh per build, producing non-byte-identical `.dylib` output
    // that broke downstream cache keys via `extern:` hashes.
    if skip_user_facing {
        tracing::debug!("skipping cache for user-facing executable: {}", crate_name);
        return intentional_passthrough_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            adaptive_unit.as_ref(),
            "user-facing executable (cache_executables=false)",
        );
    }

    if !daemon_local && store.is_none() {
        return passthrough_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            "store unavailable",
        );
    }

    // Compute the cache key (store-free on the daemon fast path).
    let keyed = match compute_rustc_cache_key(
        config,
        compiler,
        args,
        workspace_root.as_deref(),
        invocation_start_ns,
        store.as_ref(),
        extra_inputs.and_then(crate::extra_inputs::ExtraInputsSnapshot::digest),
        extra_inputs_hash_stats,
        extra_inputs_too_new,
        extra_inputs_key_ms,
        extra_inputs_guard_inputs,
        match precompiled.as_mut().and_then(|pre| pre.dep_info.take()) {
            Some(dep_info) => KeyDiscovery::Emitted(dep_info),
            None if deferral_allowed(config, args, adaptive_unit.is_some(), extra_inputs) => {
                KeyDiscovery::Deferrable
            }
            None => KeyDiscovery::Immediate,
        },
    ) {
        Ok(keyed) => keyed,
        Err(e) => {
            // `{e:#}` — the alternate form walks the cause chain. Plain
            // `{e}` prints only the outermost context, which is how the
            // substrate bench's 60 dep-info refusals stayed undiagnosable:
            // the log said "dep-info pre-pass failed for src/lib.rs" and
            // dropped rustc's own reason underneath it (kunobi-ninja/kache#431).
            tracing::warn!("failed to compute cache key for {}: {:#}", crate_name, e);
            let reason = format!("uncacheable|{e:#}");
            let exit =
                passthrough_with_event(config, args, crate_name, &event_root, start, &*reason)?;
            // The pre-pass expands macros, so a macro that cannot write fails
            // here first; its error carries rustc's own message.
            if exit != 0 {
                crate::out_dir_alias::after_failed_compile(&reason);
            }
            return Ok(exit);
        }
    };
    let ComputedKey {
        mut cache_key,
        deferred,
        discovery_flight: _discovery_flight,
        predicted,
        mut key_ms,
        mut key_hash_stats,
        mut key_too_new,
        mut guard_inputs,
    } = keyed;
    if deferred {
        // No record and nowhere else the entry could be: compile now, then
        // key from what rustc emitted. `_discovery_flight` stays held across
        // the recursion so peers wait for this compile.
        tracing::debug!("no closure record for {crate_name}; compiling before keying");
        let compile_start = std::time::Instant::now();
        let result = match compiler.execute_streaming(args) {
            Ok(result) => result,
            Err(e) => {
                return passthrough_with_event(
                    config,
                    args,
                    crate_name,
                    &event_root,
                    start,
                    format!("compiler spawn failed: {e}"),
                );
            }
        };
        let compile_time_ms = compile_start.elapsed().as_millis() as u64;
        replay_diagnostics(
            &result.stdout,
            result.pending_stderr(),
            std::io::stdout(),
            std::io::stderr(),
        );
        if result.exit_code != 0 {
            crate::out_dir_alias::after_failed_compile(&result.stderr);
            let elapsed = start.elapsed().as_millis() as u64;
            log_event_with_hash_stats(
                config,
                &event_root,
                crate_name,
                EventResult::Error,
                elapsed,
                compile_time_ms,
                0,
                "",
                key_ms,
                key_hash_stats,
                0,
                0,
                0,
            );
            print_progress(crate_name, EventResult::Error, elapsed, 0);
            return Ok(result.exit_code);
        }
        let emitted = args
            .dep_info_path()
            .zip(args.source_file.as_deref())
            .map(|(path, source)| crate::cache_key::dep_info_from_emitted(&path, source));
        let dep_info = match emitted {
            Some(Ok(dep_info)) => dep_info,
            other => {
                tracing::debug!(
                    "not caching {crate_name}: the compile left no readable dep-info ({:?})",
                    other.map(|r| r.map(|_| ()))
                );
                let elapsed = start.elapsed().as_millis() as u64;
                log_event_with_hash_stats(
                    config,
                    &event_root,
                    crate_name,
                    EventResult::Skipped,
                    elapsed,
                    compile_time_ms,
                    0,
                    "",
                    key_ms,
                    key_hash_stats,
                    0,
                    0,
                    0,
                );
                print_progress(crate_name, EventResult::Skipped, elapsed, 0);
                return Ok(result.exit_code);
            }
        };
        let exit_code = result.exit_code;
        PRECOMPILED_EXIT.with(|cell| cell.set(Some(exit_code)));
        let stored = run_parsed_rustc(
            config,
            compiler,
            args,
            start,
            invocation_start_ns,
            extra_inputs,
            extra_inputs_hash_stats,
            extra_inputs_too_new,
            extra_inputs_key_ms,
            guard_inputs,
            Some(Precompiled {
                result,
                compile_time_ms,
                dep_info: Some(dep_info),
            }),
        );
        PRECOMPILED_EXIT.with(|cell| cell.set(None));
        // Whatever the store step reported, the compile succeeded and its
        // outputs are in place.
        return stored.or(Ok(exit_code));
    }
    crate::out_dir_alias::register_after_key(crate::cache_key::take_last_key_bakes_out_dir());
    // A force-list request that could not obtain its immediate lease must not
    // retry through the post-key adaptive seed path in the same invocation.
    // It stays on the normal cache path with incremental stripped.
    let adaptive_key_fields = if adaptive_policy_for_invocation {
        adaptive_unit
            .as_ref()
            .and_then(|_| crate::cache_key::peek_last_key_fields())
    } else {
        None
    };

    let hit_context = RustcHitContext {
        config,
        compiler,
        args,
        crate_name,
        event_root: &event_root,
        start,
        extra_inputs,
    };

    // Daemon fast path (kunobi-ninja/kache#565): ask the running daemon
    // before opening SQLite. A served hit returns here; every other outcome
    // (miss, fallback, no daemon, restore failure) opens the store and runs
    // the fully local path below with the already-computed key.
    let mut store = store;
    // A compile that already ran (deferred discovery) has printed rustc's
    // diagnostics, including the artifact notifications Cargo pipelines on.
    // Restoring a peer's entry now would replay them a second time and Cargo
    // would see the unit finish twice; the outputs are in place, so only the
    // store step below is left.
    if daemon_local && precompiled.is_none() {
        if let Some(exit) = try_daemon_local_hit(&hit_context, &cache_key, key_ms, key_hash_stats) {
            reset_adaptive_unit(adaptive_unit.as_ref());
            return Ok(exit);
        }
        match open_primary_and_fallback(config, &rustc_route) {
            Ok((s, fallback)) => {
                store = Some(s);
                fallback_store = fallback;
            }
            Err(e) => warn_store_unavailable_once(config, &e),
        }
    }
    let store = match store {
        Some(store) => store,
        None => {
            return passthrough_with_event(
                config,
                args,
                crate_name,
                &event_root,
                start,
                "store unavailable",
            );
        }
    };

    drop(trace_store_open);
    let trace_remember = crate::phase_trace::phase("remember_target_root");
    if args.is_primary
        && let Some(target_dir) = args.target_dir()
        && let Some(workspace_root) = workspace_root.as_deref()
        && let Err(e) = store.remember_target_root(&target_dir, workspace_root)
    {
        tracing::warn!(
            "failed to register target root {}: {}",
            target_dir.display(),
            e
        );
    }
    drop(trace_remember);

    tracing::debug!("cache key for {}: {}", crate_name, &cache_key[..16]);

    // A prediction may READ the cache, local or remote, before it has been
    // checked. The soundness argument does not distinguish the two: an entry
    // anywhere was stored under a key computed from a discovered closure, so
    // matching it proves the prediction reproduced that closure. What a
    // prediction may not do is CLAIM or STORE, so the re-derivation moved to
    // the point below where both lookups have missed.
    //
    // Re-deriving before the remote check, as this did originally, made the
    // whole feature worthless for the case it was built for: a fresh clone has
    // an empty local store, so every unit missed locally and paid the pre-pass
    // before the warm remote was ever asked.
    //
    // The loop runs at most twice. A re-derivation that changes the key has
    // produced a key nothing has looked up yet, and on a shared cache another
    // machine may well hold it; a second pass is one lookup against a compile.
    // Summed across both passes: a second lookup is real time this
    // invocation spent looking.
    let mut lookup_ms = 0_u64;
    let mut record_closure = should_record_closure(predicted, false);
    let mut rederived = false;
    while precompiled.is_none() {
        // 1. Check local store (volume shard, then main)
        let lookup_start = std::time::Instant::now();
        let lookup_result = match lookup_local_entry(&store, fallback_store.as_ref(), &cache_key) {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(
                    "local store lookup failed for {}: {} — recompiling",
                    crate_name,
                    e
                );
                return passthrough_with_event(
                    config,
                    args,
                    crate_name,
                    &event_root,
                    start,
                    format!("store lookup failed: {e}"),
                );
            }
        };
        lookup_ms = lookup_ms.saturating_add(lookup_start.elapsed().as_millis() as u64);

        // A closure that came from a record is already recorded; re-writing it on
        // every hit would be a database write per compile for no new information.
        // A re-derivation is the opposite case: its closure is what the record
        // should have said, so writing it is what repairs a stale row.

        if let Some((hit_store, meta)) = lookup_result {
            // Safety: skip entries with no cached files (poisoned by earlier bugs)
            if meta.files.is_empty() {
                tracing::warn!(
                    "cache entry for {} has no files, evicting and recompiling",
                    crate_name
                );
                let _ = hit_store.remove_entry(&cache_key);
            } else {
                tracing::debug!("local cache hit for {} ({})", crate_name, &cache_key[..16]);
                if let Err(e) = hit_context.restore_and_finish(
                    BlobSource::Store(hit_store),
                    &meta,
                    EventResult::LocalHit,
                    &cache_key,
                    key_ms,
                    key_hash_stats,
                    lookup_ms,
                    record_closure.then_some(&store),
                ) {
                    tracing::warn!(
                        "restoring local cache hit for {} failed: {} — recompiling",
                        crate_name,
                        e
                    );
                    return passthrough_with_event(
                        config,
                        args,
                        crate_name,
                        &event_root,
                        start,
                        format!("restore failed: {e}"),
                    );
                }
                reset_adaptive_unit(adaptive_unit.as_ref());

                return Ok(0);
            }
        }

        // Build-session detection: send prefetch hint before remote work.
        // Placed after local-hit check so warm-cache invocations skip this entirely.
        maybe_trigger_prefetch(config, args);

        // 2. Check remote cache via daemon (if configured)
        if let Some(restored) = try_rustc_remote_hit(
            &hit_context,
            &store,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            record_closure,
        ) {
            if let Err(e) = restored {
                tracing::warn!(
                    "restoring cache hit for {} failed: {} — recompiling",
                    crate_name,
                    e
                );
                return passthrough_with_event(
                    config,
                    args,
                    crate_name,
                    &event_root,
                    start,
                    format!("restore failed: {e}"),
                );
            }
            reset_adaptive_unit(adaptive_unit.as_ref());
            return Ok(0);
        }

        if !owes_rederivation(predicted, rederived) {
            break;
        }
        rederived = true;
        record_closure = should_record_closure(predicted, rederived);
        let previous_key = cache_key.clone();
        match recompute_key_without_prediction(
            config,
            compiler,
            args,
            workspace_root.as_deref(),
            invocation_start_ns,
            Some(&store),
            extra_inputs.and_then(crate::extra_inputs::ExtraInputsSnapshot::digest),
        ) {
            Ok(recomputed) => {
                cache_key = recomputed.cache_key;
                // Accumulate rather than replace: the first computation's
                // measurements already include the extra-inputs resolve, and
                // this second pass is real time this invocation spent.
                (key_ms, key_hash_stats, key_too_new) = combine_key_measurements(
                    key_ms,
                    recomputed.key_ms,
                    key_hash_stats,
                    recomputed.key_hash_stats,
                    key_too_new,
                    recomputed.key_too_new,
                );
                guard_inputs.extend(recomputed.guard_inputs);
            }
            // The pre-pass failed, which is the ordinary uncacheable case.
            Err(e) => {
                return passthrough_with_event(
                    config,
                    args,
                    crate_name,
                    &event_root,
                    start,
                    format!("uncacheable|{e:#}"),
                );
            }
        }
        if cache_key == previous_key {
            // The prediction was right. Both lookups already answered for
            // this key; asking again would be the same two misses.
            break;
        }
    }

    // Exact local and remote lookups both missed. A second nearby miss whose
    // stable key groups match may seed isolated incremental state. The result
    // is deliberately not stored under the normal artifact key.
    if let (Some(unit), Some(fields)) = (adaptive_unit.as_ref(), adaptive_key_fields.as_ref())
        && let Some(lease) = unit.try_seed(&cache_key, fields)
    {
        return adaptive_incremental_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            lease,
            "adaptive seed",
            Some((&cache_key, key_ms, key_hash_stats, lookup_ms)),
        );
    }

    // 3. Cache miss — join the machine-wide flight, take a permit, then
    // claim the key and re-check under the build lock.
    let (miss_guard, scheduled_hit) = admit_scheduler_miss(
        config,
        &store,
        &cache_key,
        FlightIdentity::rustc(crate_name, &args.crate_types, args.emits_link()),
        crate_name,
        args.invokes_linker(),
        cache_entry_has_files,
    );
    let (lock, committed) = if let Some(meta) = scheduled_hit {
        (None, Some(meta))
    } else {
        match store.claim_build(&cache_key) {
            Ok(BuildClaim::Acquired(lock)) => (Some(lock), None),
            Ok(BuildClaim::Committed(meta)) => (None, Some(*meta)),
            Err(e) => {
                tracing::warn!(
                    "claiming build for {} failed: {} — recompiling",
                    crate_name,
                    e
                );
                return passthrough_with_event(
                    config,
                    args,
                    crate_name,
                    &event_root,
                    start,
                    format!("build claim failed: {e}"),
                );
            }
            Ok(BuildClaim::Contended) => {
                // Another process is building this key — wait for it
                tracing::debug!("waiting for {} to be built by another process", crate_name);
                let committed = store
                    .wait_for_committed(&cache_key)
                    .unwrap_or(false)
                    .then(|| store.get(&cache_key).ok().flatten())
                    .flatten();
                (None, committed)
            }
        }
    };

    if let Some(meta) = committed.filter(|_| precompiled.is_none()) {
        if let Err(e) = hit_context.restore_and_finish(
            BlobSource::Store(&store),
            &meta,
            EventResult::LocalHit,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            record_closure.then_some(&store),
        ) {
            tracing::warn!(
                "restoring cache hit for {} failed: {} — recompiling",
                crate_name,
                e
            );
            return passthrough_with_event(
                config,
                args,
                crate_name,
                &event_root,
                start,
                format!("restore failed: {e}"),
            );
        }
        reset_adaptive_unit(adaptive_unit.as_ref());
        return Ok(0);
    }

    let Some(lock) = lock else {
        tracing::warn!("wait for {} failed, compiling ourselves", crate_name);
        return passthrough_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            "build lock wait failed",
        );
    };

    // 4. Compile
    tracing::debug!(
        "cache miss for {}, compiling ({})",
        crate_name,
        &cache_key[..16]
    );
    let compile_start = std::time::Instant::now();
    let precompiled_time = precompiled.as_ref().map(|pre| pre.compile_time_ms);
    let mut result = match precompiled.take() {
        // Compiled before keying (deferred discovery); its output was
        // already replayed.
        Some(pre) => pre.result,
        None => match compiler.execute_streaming(args) {
            Ok(r) => r,
            // A spawn-level failure (missing binary, ENOMEM, fork pressure under
            // load) must not abort the build: fall back to passthrough so the
            // configured fallback wrapper still gets a chance and the user sees the
            // real compiler error rather than a kache anyhow chain.
            Err(e) => {
                return passthrough_with_event(
                    config,
                    args,
                    crate_name,
                    &event_root,
                    start,
                    format!("compiler spawn failed: {e}"),
                );
            }
        },
    };
    miss_guard.record_compile_rss(crate_name);
    let compile_time_ms =
        precompiled_time.unwrap_or_else(|| compile_start.elapsed().as_millis() as u64);

    // Print rustc output
    if precompiled_time.is_none() {
        replay_diagnostics(
            &result.stdout,
            result.pending_stderr(),
            std::io::stdout(),
            std::io::stderr(),
        );
    }

    // Don't cache failures
    if result.exit_code != 0 {
        crate::out_dir_alias::after_failed_compile(&result.stderr);
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            &event_root,
            crate_name,
            EventResult::Error,
            elapsed,
            0,
            0,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Error, elapsed, 0);
        drop(lock);
        return Ok(result.exit_code);
    }

    // too-new-input guard (kunobi-ninja/kache#324): if any keyed input was
    // modified within this build window, the hashes feeding the cache key are
    // racy versus what rustc actually read — refuse to store (the compile
    // already ran and is in place; we just don't cache it). Off by default;
    // the lookup above still ran, so a sound prior entry can still be served.
    // A tripped wall-clock flag is excused when post-compile verification
    // proves no guarded input changed: the flag also fires across clock
    // domains where nothing is actually racy.
    let extra_inputs_racy = args.is_primary
        && extra_inputs_changed_during_compile(config, args, extra_inputs, invocation_start_ns);
    let key_inputs_changed = key_inputs_changed_during_compile(key_too_new, &guard_inputs);
    if should_skip_cache_store_for_input_race(
        extra_inputs_racy,
        config.modified_input_guard,
        key_inputs_changed,
    ) {
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            &event_root,
            crate_name,
            EventResult::Skipped,
            elapsed,
            0,
            0,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Skipped, elapsed, 0);
        drop(lock);
        return Ok(result.exit_code);
    }

    // A shared read-only OUT_DIR that gained a file or a write bit, or a
    // dep-info that lists a file under the alias root, is not stored.
    if !crate::out_dir_alias::store_gate(args) {
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            &event_root,
            crate_name,
            EventResult::Skipped,
            elapsed,
            compile_time_ms,
            0,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Skipped, elapsed, 0);
        drop(lock);
        return Ok(result.exit_code);
    }

    // Emit-coverage gate (kunobi-ninja/kache#325): refuse to store an entry that
    // doesn't physically contain an output for every `--emit` kind this
    // invocation requested. The discovered output set is authoritative for cargo
    // builds (rustc's `--json=artifacts` reports every file), so this only fires
    // on the directory-scan fallback or an unclassified emit — exactly the paths
    // that can silently capture a partial set. Storing a partial entry would let
    // a later identical invocation hit it and find a requested `--emit=obj` /
    // `llvm-ir` missing. The compile already ran and is in place; we just decline
    // to cache it (mirrors the too-new guard above).
    if let Some(missing) = missing_requested_emit(args, &result.artifacts) {
        tracing::warn!(
            "not caching {}: discovered outputs do not cover requested --emit {} \
             (have {:?}) — refusing to store a partial entry",
            crate_name,
            missing,
            result
                .artifacts
                .outputs()
                .iter()
                .map(|a| a.store_name.as_str())
                .collect::<Vec<_>>()
        );
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            &event_root,
            crate_name,
            EventResult::Skipped,
            elapsed,
            compile_time_ms,
            0,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Skipped, elapsed, 0);
        drop(lock);
        return Ok(result.exit_code);
    }

    // Bundle audit: an rlib must not be stored when it carries an archive from
    // its `-L` dirs that the key did not hash (a `#[link(kind = "static")]`
    // attribute, say). The compile already ran; we only decline to cache it.
    let native_archives = crate::cache_key::take_last_key_native_archives().unwrap_or_default();
    let unaudited = match unaudited_native_bundle(args, &result.artifacts, &native_archives) {
        Ok(None) => None,
        Ok(Some(member)) => Some(format!(
            "its rlib bundles `{member}` from an archive the key does not hash"
        )),
        Err(error) => Some(format!("its native bundle audit failed: {error:#}")),
    };
    if let Some(reason) = unaudited {
        tracing::warn!("not caching {crate_name}: {reason}");
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            &event_root,
            crate_name,
            EventResult::Skipped,
            elapsed,
            compile_time_ms,
            0,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Skipped, elapsed, 0);
        drop(lock);
        return Ok(result.exit_code);
    }

    // Put-side admission control: the compile already ran and its outputs are
    // in place; a configured threshold may decline local retention. A writable
    // remote always reaches the store-and-upload path below.
    if !store_admits_compile(config, compile_time_ms, true) {
        tracing::debug!(
            crate_name = %crate_name,
            compile_time_ms,
            min_store_compile_ms = config.min_store_compile_ms,
            "admission: compile too cheap to store"
        );
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            &event_root,
            crate_name,
            EventResult::Skipped,
            elapsed,
            compile_time_ms,
            0,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Skipped, elapsed, 0);
        clean_incremental_dir(config, args);
        drop(lock);
        return Ok(result.exit_code);
    }

    if let (Some(unit), Some(fields)) = (adaptive_unit.as_ref(), adaptive_key_fields.as_ref()) {
        let _ = unit.observe_normal_miss(&cache_key, fields);
    }

    // 5. Store the output files
    let target = args.target.as_deref().unwrap_or("host");
    let profile = match args.get_codegen_opt("opt-level") {
        Some("0") | None => "dev",
        Some("s") | Some("z") => "release-size",
        _ => "release",
    };

    // Rust dep-info is normalized into a private staging file before Store::put
    // reads it. Cargo's compiler-owned `.d` stays untouched, while the cached
    // blob gets target/package/workspace sentinels instead of donor paths.
    let depinfo_anchor = args.target_dir();
    let depinfo_working_dir = current_dir.as_deref().unwrap_or_else(|| Path::new("."));
    let depinfo_workspace_dir = args.path_normalization_root();
    let depinfo_configured_roots =
        configured_rustc_depinfo_roots(config, depinfo_workspace_dir, depinfo_anchor.as_deref());

    // Validate the compiler's consumer-facing dep-info before Store::put makes
    // an entry observable. The staging transform below cannot alter its input.
    if let Some(snapshot) = extra_inputs
        && let Err(error) =
            validate_extra_inputs_dep_info_before_store(args, &result.artifacts, snapshot)
    {
        return Err(error).context("validating extra_inputs dep-info before cache commit");
    }

    // Store-time debug bundle (kunobi-ninja/kache#319): a macOS `-g`
    // executable's `N_OSO` debug map points at per-build `.o` files that a
    // restoring build won't have — so while they still exist, bake a
    // self-contained `.dSYM` and cache it (as one flat tar; the store holds
    // flat files only) alongside the entry. Restore unpacks it next to the
    // binary, where lldb prefers it over the stale debug map. The staging
    // TempDir must outlive `store.put*` below, which hashes the tar at this
    // path — same lifetime pattern as `prepare_cc_store_files`.
    let mut _debug_bundle_staging: Option<tempfile::TempDir> = None;
    if wants_debug_bundle(args)
        && let Some((exec_path, exec_name)) =
            find_executable_output(compiler, args, &result.artifacts)
    {
        match tempfile::tempdir() {
            Ok(staging) => {
                match platform::current().package_debug_bundle(&exec_path, staging.path()) {
                    Ok(Some(tar_path)) => {
                        result.artifacts.push(crate::compiler::Artifact {
                            path: tar_path,
                            // Single path component (`is_safe_artifact_name`
                            // gates restore) derived from the executable's
                            // store name: `foo-abc` → `foo-abc.dsym.tar`.
                            store_name: format!("{exec_name}.dsym.tar"),
                            kind: ArtifactKind::DebugBundle,
                            required: false,
                        });
                        _debug_bundle_staging = Some(staging);
                    }
                    // None (non-macOS host, tool missing/failed) is the
                    // documented best-effort degradation: cache the
                    // binary without a bundle.
                    Ok(None) => {}
                    Err(e) => {
                        tracing::warn!(
                            "failed to package debug bundle for {}: {e:#}",
                            exec_path.display()
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to create debug bundle staging dir: {e}");
            }
        }
    }

    let prepared_store = match prepare_rustc_store_files(
        &result.artifacts,
        depinfo_anchor.as_deref(),
        depinfo_working_dir,
        depinfo_workspace_dir,
        &depinfo_configured_roots,
    ) {
        Ok(prepared) => prepared,
        Err(error) => {
            tracing::warn!(
                "not caching {}: dep-info could not be staged safely: {error:#}",
                crate_name
            );
            let elapsed = start.elapsed().as_millis() as u64;
            log_event_with_hash_stats(
                config,
                &event_root,
                crate_name,
                EventResult::Skipped,
                elapsed,
                compile_time_ms,
                0,
                &cache_key,
                key_ms,
                key_hash_stats,
                lookup_ms,
                0,
                0,
            );
            print_progress(crate_name, EventResult::Skipped, elapsed, 0);
            clean_incremental_dir(config, args);
            drop(lock);
            return Ok(result.exit_code);
        }
    };

    // Finish Cargo's consumer-facing dep-info before Store::put makes the
    // neutral staged blob observable. The store reads only the private staged
    // `.d`, so completing Cargo's compiler-owned file cannot change it.
    if let Some(snapshot) = extra_inputs {
        complete_extra_inputs_dep_info(args, snapshot)
            .context("completing extra_inputs dep-info before cache publication")?;
    }

    let store_start = std::time::Instant::now();
    let trace_store = crate::phase_trace::phase("store");
    let mut store_put = StorePutResult::default();
    let mut store_error = String::new();
    match store.put_with_compile_time(
        &cache_key,
        crate_name,
        &args.crate_types,
        &args.features,
        target,
        profile,
        &prepared_store.files,
        &result.stdout,
        &result.stderr,
        compile_time_ms,
    ) {
        Ok(result) => {
            store_put = result;
            if let Some(unit) = args.get_codegen_opt("metadata")
                && let Err(e) = store.record_entry_unit(&cache_key, unit)
            {
                tracing::debug!("recording the unit of {crate_name}'s entry failed: {e}");
            }
            // Store grew — throttled size check + detached background GC if over
            // budget (kunobi-ninja/kache#497). Never blocks the compile path.
            maybe_spawn_auto_gc(config, &store);
            flush_or_hand_off_durability(config, &store, &cache_key);
        }
        // Name the crate, as the cc path already does: a failed store leaves that
        // unit re-compiling on every build while the aggregate hit rate barely
        // moves, and the crate name is the only thread back to it (#624). The
        // reason also rides the event, so `report` / `why-miss` can say the miss
        // is permanent rather than cold (#629).
        Err(e) => {
            store_error = store_error_for_event(&e);
            tracing::warn!(
                "failed to store cache entry for {}: {}",
                crate_name,
                store_error
            );
        }
    }
    drop(trace_store);
    let store_ms = store_start.elapsed().as_millis() as u64;

    // 6. Queue remote publication through the shared durable upload path.
    maybe_enqueue_upload(config, &store, &cache_key, crate_name, true);

    record_input_prediction(config, Some(&store), args, record_closure);

    // 7. Clean incremental dir, as with kache's caching, incremental compilation is redundant
    clean_incremental_dir(config, args);

    let elapsed = start.elapsed().as_millis() as u64;
    let size = result.artifacts.total_size();
    let event_result = event_result_for_store_put(store_put);
    log_event_with_store_outcome(
        config,
        &event_root,
        crate_name,
        event_result,
        elapsed,
        compile_time_ms,
        size,
        &cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        0,
        store_ms,
        store_put,
        store_error,
    );
    print_progress(crate_name, event_result, elapsed, size);

    drop(lock);
    Ok(result.exit_code)
}

struct PreparedCcStoreFiles {
    files: Vec<(PathBuf, String)>,
    _temporary_files: Vec<tempfile::TempPath>,
}

/// Freeze store inputs without rewriting or later reopening compiler-owned
/// output paths.
///
/// Every artifact is copied into a private temporary file before Store::put
/// hashes it. This keeps a concurrent replacement of a compiler output from
/// publishing different bytes under the hash chosen for the original path.
/// Dep-info normalization happens while creating that private snapshot.
/// What the daemon needs to store a finished cc compile and log its event.
struct CcHandoff<'a> {
    cache_key: &'a str,
    crate_name: &'a str,
    target: &'a str,
    /// `(staged path, store name)`: the wrapper's private snapshots.
    files: &'a [(PathBuf, String)],
    stdout: &'a str,
    stderr: &'a str,
    compile_time_ms: u64,
    publishes_to_remote: bool,
    event_root: &'a str,
    start: std::time::Instant,
    size: u64,
    key_ms: u64,
    lookup_ms: u64,
    lookup_rejection: &'a str,
    store_start: std::time::Instant,
    /// The read-set memo for the daemon to record with the entry.
    memo: Option<crate::daemon_publish::CcMemoHandoff>,
}

enum CcHandoffOutcome {
    /// The daemon holds the key and will store the entry and its memo; the
    /// event is its.
    Accepted,
    /// A peer took the key while the daemon was declining; its entry
    /// counts and the event is written. Nothing more to store here.
    Done,
    /// Store here, with the key lock back in the caller's hands.
    Publish,
}

/// Offer the compile to the daemon. On acceptance the wrapper is done: the
/// daemon owns the snapshots, the key lock and the event. On any refusal
/// the caller stores as before, with its lock re-taken; if a peer took the
/// key in the meantime, that peer's entry is the one that counts and only
/// the event is written here.
fn hand_off_cc_store(
    config: &Config,
    store: &Store,
    build_lock: &mut Option<KeyLock>,
    handoff: CcHandoff<'_>,
) -> CcHandoffOutcome {
    use crate::daemon_publish::{Handoff, PublishCcRequest};
    // Keep volume-local publication on the wrapper until the daemon can
    // claim and write that same shard.
    if !volume_cache_dirs_match(store.cache_dir(), &config.cache_dir) {
        return CcHandoffOutcome::Publish;
    }
    let snapshots = match crate::daemon_publish::snapshot_for_handoff(config, handoff.files) {
        Ok(snapshots) => snapshots,
        Err(error) => {
            tracing::debug!("cc hand-off: could not snapshot outputs: {error:#}");
            return CcHandoffOutcome::Publish;
        }
    };
    let elapsed = handoff.start.elapsed().as_millis() as u64;
    let trace_event = crate::phase_trace::phase("handoff_event");
    let event = build_event_details(
        config,
        handoff.event_root,
        handoff.crate_name,
        EventResult::Miss,
        elapsed,
        handoff.compile_time_ms,
        handoff.size,
        handoff.cache_key,
        handoff.key_ms,
        FileHashStats::default(),
        handoff.lookup_ms,
        0,
        handoff.store_start.elapsed().as_millis() as u64,
        StorePutResult::default(),
        String::new(),
        String::new(),
        handoff.lookup_rejection.to_string(),
        false,
        None,
        None,
    );
    drop(trace_event);
    let request = PublishCcRequest {
        client_epoch: crate::daemon::build_epoch(),
        cache_key: handoff.cache_key.to_string(),
        crate_name: handoff.crate_name.to_string(),
        target: handoff.target.to_string(),
        files: snapshots,
        stdout: handoff.stdout.to_string(),
        stderr: handoff.stderr.to_string(),
        compile_time_ms: handoff.compile_time_ms,
        publishes_to_remote: compiler_upload_enabled(config, handoff.publishes_to_remote),
        event,
        memo: handoff.memo,
    };
    // The daemon takes the key itself before it answers; ours must be gone
    // first, since a file lock cannot be shared across processes.
    *build_lock = None;
    match crate::daemon_publish::hand_off_cc_publish(config, &request) {
        Handoff::Accepted => {
            print_progress(handoff.crate_name, EventResult::Miss, elapsed, handoff.size);
            CcHandoffOutcome::Accepted
        }
        Handoff::Declined(reason) => {
            tracing::debug!("cc hand-off declined for {}: {reason}", handoff.crate_name);
            crate::daemon_publish::remove_handoff_files(&request.files);
            match store.claim_build(handoff.cache_key) {
                Ok(BuildClaim::Acquired(lock)) => {
                    *build_lock = Some(lock);
                    CcHandoffOutcome::Publish
                }
                Ok(BuildClaim::Committed(_)) | Ok(BuildClaim::Contended) => {
                    // A peer holds or stored the key in the gap; it publishes.
                    let mut event = request.event;
                    event.elapsed_ms = handoff.start.elapsed().as_millis() as u64;
                    // An independent peer won the key. That is ordinary
                    // contention, not a failed store. An accepted daemon job
                    // is resolved by its receipt before reaching this branch.
                    write_event(config, &event);
                    print_progress(
                        handoff.crate_name,
                        EventResult::Miss,
                        event.elapsed_ms,
                        handoff.size,
                    );
                    CcHandoffOutcome::Done
                }
                Err(error) => {
                    tracing::warn!(
                        "reclaiming {} after a declined hand-off failed: {error:#}; storing without a lock",
                        handoff.crate_name
                    );
                    CcHandoffOutcome::Publish
                }
            }
        }
    }
}

fn prepare_cc_store_files(
    artifacts: &ArtifactSet,
    depinfo_anchor: Option<&Path>,
) -> Result<PreparedCcStoreFiles> {
    prepare_cc_store_files_in(artifacts, depinfo_anchor, None)
}

/// [`prepare_cc_store_files`] with the snapshots in `staging_dir` when one
/// is given: a hand-off then links them into place instead of copying the
/// object a second time. An unusable directory falls back to the system
/// temporary directory.
fn prepare_cc_store_files_in(
    artifacts: &ArtifactSet,
    depinfo_anchor: Option<&Path>,
    staging_dir: Option<&Path>,
) -> Result<PreparedCcStoreFiles> {
    use std::io::{Read, Write};

    let staging_dir = staging_dir.filter(|dir| std::fs::create_dir_all(dir).is_ok());
    let mut files = Vec::with_capacity(artifacts.outputs().len());
    let mut temporary_files = Vec::with_capacity(artifacts.outputs().len());
    for artifact in artifacts.outputs() {
        let mut builder = tempfile::Builder::new();
        builder.prefix("kache-cc-artifact-");
        let staged = match staging_dir {
            Some(dir) => builder.tempfile_in(dir).or_else(|_| builder.tempfile()),
            None => builder.tempfile(),
        }
        .context("cc store: creating private artifact staging file")?;
        let staged = staged.into_temp_path();

        if artifact.kind == ArtifactKind::DepInfo {
            let anchor = depinfo_anchor.context("cc store: missing dep-info rewrite anchor")?;
            let mut content = String::new();
            std::fs::File::open(&artifact.path)
                .with_context(|| format!("cc store: opening dep-info {}", artifact.path.display()))?
                .read_to_string(&mut content)
                .with_context(|| {
                    format!("cc store: reading dep-info {}", artifact.path.display())
                })?;
            let normalized =
                link::rewrite_depinfo_content(&content, anchor, link::DepInfoMode::Relativize);
            std::fs::write(&staged, normalized.as_bytes())
                .context("cc store: writing normalized dep-info staging file")?;
        } else if artifact.kind == ArtifactKind::DebugBundle
            && std::fs::metadata(&artifact.path).is_ok_and(|meta| meta.is_dir())
        {
            platform::build_deterministic_tar(&artifact.path, &staged).with_context(|| {
                format!(
                    "cc store: packaging debug bundle {}",
                    artifact.path.display()
                )
            })?;
        } else {
            let mut source = std::fs::File::open(&artifact.path).with_context(|| {
                format!("cc store: opening artifact {}", artifact.path.display())
            })?;
            let mut dest = std::fs::File::create(&staged).with_context(|| {
                format!(
                    "cc store: creating staging file for {}",
                    artifact.path.display()
                )
            })?;
            std::io::copy(&mut source, &mut dest).with_context(|| {
                format!("cc store: copying artifact {}", artifact.path.display())
            })?;
            dest.flush()
                .context("cc store: flushing private artifact staging file")?;
        }
        files.push((staged.to_path_buf(), artifact.store_name.clone()));
        temporary_files.push(staged);
    }

    Ok(PreparedCcStoreFiles {
        files,
        _temporary_files: temporary_files,
    })
}

#[derive(Debug)]
struct PreparedRustcStoreFiles {
    files: Vec<(PathBuf, String)>,
    _temporary_files: Vec<tempfile::TempPath>,
}

/// Freeze rustc store inputs without modifying compiler-owned outputs.
///
/// Dep-info is normalized while copying it into a private staging file. The
/// store therefore observes one immutable snapshot and a failed rewrite can
/// only skip caching; it can never leave Cargo's output partially rewritten.
fn prepare_rustc_store_files(
    artifacts: &ArtifactSet,
    target_dir: Option<&Path>,
    working_dir: &Path,
    workspace_dir: Option<&Path>,
    configured_roots: &[(PathBuf, String, u8)],
) -> Result<PreparedRustcStoreFiles> {
    use std::io::{Read, Write};

    let mut files = Vec::with_capacity(artifacts.outputs().len());
    let mut temporary_files = Vec::with_capacity(artifacts.outputs().len());
    for artifact in artifacts.outputs() {
        if artifact.kind != ArtifactKind::DepInfo {
            // Preserve the compiler-owned path (and therefore executable mode)
            // for ordinary artifacts. Only dep-info needs transformed bytes.
            files.push((artifact.path.clone(), artifact.store_name.clone()));
            continue;
        }

        let mut staged = tempfile::Builder::new()
            .prefix("kache-rustc-artifact-")
            .tempfile()
            .context("rustc store: creating private artifact staging file")?;
        let anchor = target_dir.context("rustc store: missing dep-info rewrite anchor")?;
        let mut content = String::new();
        std::fs::File::open(&artifact.path)
            .with_context(|| format!("rustc store: opening dep-info {}", artifact.path.display()))?
            .read_to_string(&mut content)
            .with_context(|| {
                format!("rustc store: reading dep-info {}", artifact.path.display())
            })?;
        let normalized = link::rewrite_rustc_depinfo_content_with_configured_roots(
            &content,
            anchor,
            working_dir,
            workspace_dir,
            configured_roots,
            link::DepInfoMode::Relativize,
        );
        staged
            .write_all(normalized.as_bytes())
            .context("rustc store: writing normalized dep-info staging file")?;
        staged
            .flush()
            .context("rustc store: flushing private artifact staging file")?;
        let staged = staged.into_temp_path();
        files.push((staged.to_path_buf(), artifact.store_name.clone()));
        temporary_files.push(staged);
    }

    Ok(PreparedRustcStoreFiles {
        files,
        _temporary_files: temporary_files,
    })
}

fn configured_rustc_depinfo_roots(
    config: &Config,
    workspace_root: Option<&Path>,
    target_dir: Option<&Path>,
) -> Vec<(PathBuf, String, u8)> {
    crate::path_normalizer::PathNormalizer::from_env(workspace_root)
        .with_target_dir(target_dir)
        .with_base_dirs(&config.base_dirs)
        .depinfo_source_roots()
        .into_iter()
        .map(|root| (root.root, root.depinfo_sentinel, root.priority))
        .collect()
}

fn validate_extra_inputs_dep_info_before_store(
    args: &RustcArgs,
    artifacts: &ArtifactSet,
    snapshot: &crate::extra_inputs::ExtraInputsSnapshot,
) -> Result<()> {
    let expected_name = args
        .dep_info_path()
        .and_then(|path| path.file_name().map(std::ffi::OsStr::to_os_string));
    let mut saw_dep_info = false;
    for artifact in artifacts.outputs() {
        if artifact.kind != ArtifactKind::DepInfo {
            continue;
        }
        if expected_name
            .as_ref()
            .is_some_and(|expected| artifact.path.file_name() != Some(expected.as_os_str()))
        {
            continue;
        }
        saw_dep_info = true;
        let raw = std::fs::read_to_string(&artifact.path)
            .with_context(|| format!("reading producer dep-info {}", artifact.path.display()))?;
        snapshot
            .merge_dep_info_content(&raw)
            .with_context(|| format!("completing producer dep-info {}", artifact.path.display()))?;
    }
    anyhow::ensure!(
        expected_name.is_none() || saw_dep_info,
        "successful rustc invocation produced no expected dep-info artifact required by active extra_inputs"
    );
    Ok(())
}

/// Whether this invocation actually emits debug info that a store-time debug
/// bundle could carry (kunobi-ninja/kache#319). rustc's default is no debug
/// info, so an absent `-Cdebuginfo` counts as off, as do the explicit "none"
/// spellings; everything else (`1`, `2`, `line-tables-only`, ...) produces
/// DWARF worth bundling. `-g` desugars to `-Cdebuginfo=2` at parse time.
fn rustc_debuginfo_enabled(args: &RustcArgs) -> bool {
    args.debuginfo_enabled()
}

/// Store-time gate for [`crate::compiler::Platform::package_debug_bundle`]:
/// only user-facing executables (`bin` / `--test`) reach the executable cache
/// path, and only debug-carrying ones have anything for a `.dSYM` to hold.
/// No `cache_executables` check here — a non-user-facing invocation never
/// stores an executable, and a user-facing one only reaches the store when
/// `cache_executables` already let it past the passthrough gate.
/// The executable artifact of this invocation, if any — the binary the
/// store-time debug bundle is baked FROM. Classification is contextual
/// (extensionless bins need the crate-type), so this rides classify_output
/// rather than filenames (kunobi-ninja/kache#319).
fn find_executable_output(
    compiler: &RustcCompiler,
    args: &RustcArgs,
    artifacts: &crate::compiler::ArtifactSet,
) -> Option<(std::path::PathBuf, String)> {
    artifacts
        .outputs()
        .iter()
        .find(|a| compiler.classify_output(args, &a.store_name) == ArtifactKind::Executable)
        .map(|a| (a.path.clone(), a.store_name.clone()))
}

fn wants_debug_bundle(args: &RustcArgs) -> bool {
    args.is_user_facing_executable() && rustc_debuginfo_enabled(args)
}

/// How to materialize one restored artifact.
///
/// `kind` comes from the compile context, which does not always identify an
/// executable. A `[[test]] harness = false` target supplies its own `main`, so
/// cargo invokes rustc with neither `--test` nor `--crate-type`; its
/// extensionless output classifies as `Other("rustc:unknown")`, whose strategy
/// is `Hardlink` — no `0o755` on restore, and cargo then fails the run with
/// "Permission denied (os error 13)".
///
/// The executable bit recorded at insert time is the reliable signal, and the
/// insert side already trusts it over the filename (`store::hardlink_eligible`
/// refuses to hardlink anything carrying a mode bit). Restore trusts it the
/// same way, which also keeps executables on the independent-inode path so a
/// post-build `strip` or codesign cannot reach back into the shared blob.
///
/// The exception is `shared_loadable`, the executable kind of this compile
/// that nothing rewrites in place (see [`shared_inode_loadable`]). It may
/// share the blob's inode like an rlib.
fn restore_link_strategy(
    kind: ArtifactKind,
    executable: bool,
    shared_loadable: Option<ArtifactKind>,
) -> link::LinkStrategy {
    if shared_loadable == Some(kind) {
        link::LinkStrategy::ExecutableHardlink
    } else if executable {
        link::LinkStrategy::Copy
    } else {
        kind.link_strategy()
    }
}

/// The executable output of this compile that nothing rewrites in place after
/// a restore, so it may share the store blob's inode like an rlib: a
/// proc-macro's dylib or a build script's binary. rustc refuses to write over
/// a read-only output, the wrapper's pre-clean removes one first, and the
/// build-script launcher only renames or removes the binary it preserves.
///
/// User-facing binaries, tests, examples and other dylibs stay private
/// copies, because a post-build `strip` rewrites them in place. The platform
/// decides the rest: macOS may re-sign a restored loadable in place.
fn shared_inode_loadable(args: &RustcArgs, platform_allows: bool) -> Option<ArtifactKind> {
    if !platform_allows {
        None
    } else if args.crate_types == ["proc-macro"] {
        Some(ArtifactKind::DynamicLibrary)
    } else if crate::build_script::build_script_output(args).is_some() {
        Some(ArtifactKind::Executable)
    } else {
        None
    }
}

/// Whether a restored artifact's bytes are still the store blob's bytes.
///
/// Only [`RestoredBytes::ExactBlobCopy`] may be paired with the blob's
/// recorded digest in the file-hash memo (kunobi-ninja/kache#540) — a rewritten
/// artifact hashes to something the entry never recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RestoredBytes {
    /// Reflinked, hardlinked or copied verbatim: `cached_file.hash` describes
    /// exactly what was on disk at this fingerprint. The fingerprint is carried
    /// rather than re-read later, so the claim stays true even if something
    /// overwrites the artifact right afterwards.
    ExactBlobCopy(crate::cache_key::FileFingerprint),
    /// kache transformed the content itself (dep-info re-rooting), an external
    /// post-restore tool mutated the file in place (codesigning), or the
    /// artifact could not be fingerprinted at all.
    Rewritten,
}

/// Materialize one cached blob at its invocation-specific output path.
///
/// The caller owns target-path resolution because that is compiler-specific
/// (`rustc --out-dir` vs. cc `-o` / `-MF`). Once the target and kind are
/// known, restore mechanics are shared: apply content transforms in memory,
/// materialize the result (leaving mtimes strategy-natural, see below), then
/// run external post-restore actions.
///
/// ## GC-vs-restore invariant (kunobi-ninja/kache#326, #182)
///
/// This path holds neither the SQLite write lock nor a key lock, so in
/// principle a concurrent GC could unlink a blob between the `exists()` check
/// and the read/link below. Two things make that safe:
///   1. Eviction's active-pin guard (`Store::remove_entry_guarded`) refuses to
///      unlink a blob whose entry was accessed within `EVICTION_IDLE_GRACE` —
///      and `Store::get` bumps `last_accessed` immediately before this runs — so
///      a blob being restored is not an eviction candidate.
///   2. If a blob is nonetheless gone (explicit `kache rm` / `clear`, or the
///      vanishingly small residual race), every error here propagates to
///      `restore_from_cache`'s callers, which treat it as a **clean miss and
///      recompile** — never a false hit. ENOENT is called out below so the
///      degradation reads as the benign race it is rather than corruption.
fn materialize_cached_artifact(
    blobs: &BlobSource<'_>,
    cached_file: &crate::store::CachedFile,
    target_path: &Path,
    kind: ArtifactKind,
    shared_loadable: Option<ArtifactKind>,
    depinfo_anchor: &Path,
    depinfo_working_dir: &Path,
    depinfo_workspace_dir: Option<&Path>,
    depinfo_configured_roots: &[(PathBuf, String, u8)],
    platform: &dyn crate::compiler::Platform,
    context: &str,
    extra_inputs: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
) -> Result<RestoredBytes> {
    let store_path = blobs.blob_path(&cached_file.hash);
    if !store_path.exists() {
        // Blob gone before we could open it — almost always a concurrent GC /
        // purge of this entry (kunobi-ninja/kache#182). Surface it as a restore
        // miss; the caller recompiles, never serves a partial hit.
        anyhow::bail!(
            "{context}: blob for {} (hash {}) was evicted before restore — \
             treating as a cache miss: {}",
            cached_file.name,
            &cached_file.hash[..16.min(cached_file.hash.len())],
            store_path.display()
        );
    }

    let plan = plan_post_restore(kind);
    let transforms: Vec<_> = plan
        .iter()
        .copied()
        .filter(|action| action.is_content_transform())
        .collect();

    let complete_extra_inputs = extra_inputs.filter(|_| kind == ArtifactKind::DepInfo);
    let transformed = if transforms.is_empty() && complete_extra_inputs.is_none() {
        None
    } else {
        let original = std::fs::read(&store_path)
            .with_context(|| format!("{context}: reading blob {}", store_path.display()))?;
        let mut content = original.clone();
        for action in &transforms {
            content = action.transform(content, depinfo_anchor);
        }
        if kind == ArtifactKind::DepInfo {
            content = match String::from_utf8(content) {
                Ok(text) => crate::link::rewrite_rustc_depinfo_content_with_configured_roots(
                    &text,
                    depinfo_anchor,
                    depinfo_working_dir,
                    depinfo_workspace_dir,
                    depinfo_configured_roots,
                    link::DepInfoMode::Expand,
                )
                .into_bytes(),
                Err(error) => error.into_bytes(),
            };
        }
        if let Some(snapshot) = complete_extra_inputs {
            let text = String::from_utf8(content)
                .with_context(|| format!("{context}: dep-info is not valid UTF-8"))?;
            content = snapshot
                .merge_dep_info_content(&text)
                .with_context(|| format!("{context}: completing extra_inputs dep-info"))?
                .into_bytes();
        }
        if content == original {
            None
        } else {
            Some(content)
        }
    };

    let strategy = restore_link_strategy(kind, cached_file.executable, shared_loadable);
    let rewrote_content = transformed.is_some();
    match transformed {
        Some(content) => {
            // Freshly written bytes already carry a write-clock mtime by
            // construction — no stamp needed (and none wanted: an explicit
            // stamp is the unverified clock path on non-Linux platforms).
            link::write_restored(target_path, &content, strategy)
                .with_context(|| format!("{context}: writing {}", target_path.display()))?;
        }
        None => {
            link::link_to_target(&store_path, target_path, strategy).with_context(|| {
                format!(
                    "{context}: linking {} -> {}",
                    store_path.display(),
                    target_path.display()
                )
            })?;
            // A link/clone keeps the blob's old mtime, so it must be
            // re-stamped to read as "written now" — through the same clock
            // ordinary file writes use; see `touch_mtime_write_clock` for
            // the full invariant (kunobi-ninja/kache#677, #135). Not
            // stamping at all is wrong too: cargo re-runs build scripts in
            // a cleaned tree and its `StaleDependency` rule then finds our
            // old-mtime restored artifacts older than the fresh script
            // outputs (permanently dirty again — tried and falsified
            // against cargo's fingerprint log).
            //
            // On a non-CoW Unix filesystem the hardlink fallback retains at
            // most one named target consumer per blob. Later consumers are
            // copied before this stamp, so it cannot re-date a still-linked
            // artifact another process is reading (#794). The first consumer
            // still shares with the store blob; changing the blob mtime does
            // not affect SQLite `last_accessed` eviction ranking, though it can
            // conservatively delay the later orphan-blob age sweep. The Windows
            // hardlink opt-in deliberately retains its documented legacy risk.
            link::touch_mtime_write_clock(target_path)
                .with_context(|| format!("{context}: touching {}", target_path.display()))?;
        }
    }

    // Byte-exactness is decided here, at the one site that knows what the
    // restore actually did (kunobi-ninja/kache#540). A content transform
    // already means the bytes are kache's, not the blob's. External actions
    // are handed a real file and may rewrite it — macOS re-signs an
    // invalidated binary, the Linux and Windows impls do nothing — so instead
    // of predicting per platform, fingerprint the artifact across them and let
    // an unchanged fingerprint prove nothing was touched. Cheap (a stat, or two
    // when such an action is planned) and it stays honest when a new action or
    // platform is added.
    //
    // The closing fingerprint is returned, not re-read by the caller: it is the
    // one that was observed to hold the blob's bytes.
    let external: Vec<_> = plan
        .iter()
        .copied()
        .filter(|action| !action.is_content_transform())
        .collect();
    // Content rewrites are already rejected below. Capture the pre-action
    // fingerprint whenever an external action exists so that action must prove
    // it left the restored bytes untouched.
    let before = (!external.is_empty())
        .then(|| crate::cache_key::FileFingerprint::from_path(target_path).ok())
        .flatten();

    // A blob whose restored bytes this host already verified needs no second
    // check: the file about to be checked is that blob, byte for byte.
    let verified = (!rewrote_content)
        .then(|| platform.verified_loadable_dir())
        .flatten()
        .map(|dir| VerifiedLoadable::new(&dir, &cached_file.hash));
    let known_loadable = verified.as_ref().is_some_and(VerifiedLoadable::is_recorded);
    let mut verified_now = false;
    for action in &external {
        if known_loadable && matches!(action, crate::compiler::PostRestoreAction::Sign(_)) {
            continue;
        }
        let loadability = action
            .apply(target_path, platform)
            .with_context(|| format!("{context}: applying {action:?}"))?;
        if loadability == crate::compiler::Loadability::Verified {
            verified_now = true;
        }
    }

    if rewrote_content {
        return Ok(RestoredBytes::Rewritten);
    }
    let Ok(after) = crate::cache_key::FileFingerprint::from_path(target_path) else {
        return Ok(RestoredBytes::Rewritten);
    };
    let untouched = external.is_empty() || before.is_some_and(|before| before == after);
    if untouched
        && verified_now
        && let Some(verified) = &verified
    {
        verified.record();
    }
    Ok(if untouched {
        RestoredBytes::ExactBlobCopy(after)
    } else {
        RestoredBytes::Rewritten
    })
}

/// The memo that one store blob, restored unchanged, passed this host's
/// loadability check: an empty file named by the blob's hash under
/// [`crate::compiler::Platform::verified_loadable_dir`].
struct VerifiedLoadable(PathBuf);

impl VerifiedLoadable {
    fn new(dir: &Path, hash: &str) -> Self {
        Self(dir.join(hash))
    }

    fn is_recorded(&self) -> bool {
        self.0.is_file()
    }

    /// Best effort: without the memo the next restore runs the check again.
    fn record(&self) {
        let written = self
            .0
            .parent()
            .map_or(Ok(()), std::fs::create_dir_all)
            .and_then(|()| std::fs::write(&self.0, b""));
        if let Err(error) = written {
            tracing::debug!(
                "could not remember {} as loadable: {error}",
                self.0.display()
            );
        }
    }
}

/// Restore cached artifacts to the target output paths.
/// Return the first requested `--emit` kind not covered by the discovered
/// output set, or `None` when every gated requested kind is present
/// (kunobi-ninja/kache#325).
///
/// Only kinds in [`crate::compiler::GATED_EMIT_KINDS`] are checked; an exotic
/// emit kache can't map to a stored file is ignored so the gate never refuses on
/// a kind it can't reason about. A bare invocation with no `--emit` yields
/// `None`. A lib `--emit=link` also producing `.rmeta` is fine — coverage is
/// superset-tolerant.
fn missing_requested_emit(args: &RustcArgs, artifacts: &ArtifactSet) -> Option<String> {
    let present: std::collections::HashSet<&str> = artifacts
        .outputs()
        .iter()
        .filter_map(|a| crate::compiler::emit_kind_for_filename(&a.store_name))
        .collect();
    args.emit
        .iter()
        .find(|kind| {
            crate::compiler::GATED_EMIT_KINDS.contains(&kind.as_str())
                && !present.contains(kind.as_str())
        })
        .cloned()
}

/// The first member of this compile's rlib that rustc bundled from an archive
/// in the unit's scanned `-L` dirs the key did not hash, if any.
///
/// A `#[link(kind = "static")]` attribute bundles an archive that no `-l` on
/// argv names, so the key holds the attribute text but not the archive bytes.
/// Storing that rlib would restore it after the archive is rebuilt in place.
/// Only rlibs with a scanned native dir are audited, the units whose key
/// carries the `native_bundle_audit` marker; a system library dir outside the
/// build tree is never read. A file in those dirs that is no `ar` archive (a
/// linker script) cannot be bundled, so it is no candidate.
fn unaudited_native_bundle(
    args: &RustcArgs,
    artifacts: &ArtifactSet,
    native: &crate::cache_key::KeyedNativeArchives,
) -> Result<Option<String>> {
    if !crate::cache_key::needs_native_bundle_audit(args, &native.dirs) {
        return Ok(None);
    }
    let Some(rlib) = artifacts
        .outputs()
        .iter()
        .find(|artifact| artifact.store_name.ends_with(".rlib"))
    else {
        return Ok(None);
    };
    let members = without_import_members(&crate::native_archive::members(&rlib.path)?);
    let keyed = bundle_credits(&native.bundled)?;
    if unkeyed_rlib_members(&members, &keyed).is_empty() {
        return Ok(None);
    }
    let mut candidates = Vec::new();
    for dir in &native.dirs {
        for archive in crate::cache_key::native_dir_archives(dir)? {
            if !native.archives.contains(&archive)
                && crate::native_archive::has_archive_magic(&archive)?
            {
                candidates.extend(archive_names(&archive)?);
            }
        }
    }
    Ok(unaudited_bundled_member(&members, &keyed, &candidates))
}

/// The names of the rlib members an archive the key hashed accounts for:
/// the file name of one rustc packs as a single member, the member names of
/// one it unpacks. A keyed archive the rlib does not carry accounts for none,
/// so a same-named member of an unkeyed archive stays unaccounted for.
fn bundle_credits(bundled: &[crate::cache_key::BundledArchive]) -> Result<Vec<String>> {
    let mut credits = Vec::new();
    for archive in bundled {
        if archive.packed {
            credits.extend(
                archive
                    .path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned()),
            );
        } else {
            credits.extend(crate::native_archive::member_names(&archive.path)?);
        }
    }
    Ok(credits)
}

/// The rlib's member names without its import-library members: every member
/// that shares a name with a COFF short import object, which is the DLL name
/// (`kernel32.dll`). rustc writes these for `kind = "raw-dylib"` from the
/// source text, not from a file in the `-L` dirs. An import library bundled
/// as a static archive is not audited either.
fn without_import_members(members: &[crate::native_archive::ArchiveMember]) -> Vec<String> {
    let dlls: std::collections::HashSet<&str> = members
        .iter()
        .filter(|member| member.short_import)
        .map(|member| member.name.as_str())
        .collect();
    members
        .iter()
        .filter(|member| !dlls.contains(member.name.as_str()))
        .map(|member| member.name.clone())
        .collect()
}

/// An archive's member names and its own file name, which rustc uses for the
/// single member that packs a `+whole-archive` library.
fn archive_names(archive: &Path) -> Result<Vec<String>> {
    let mut names = crate::native_archive::member_names(archive)?;
    names.extend(
        archive
            .file_name()
            .map(|name| name.to_string_lossy().into_owned()),
    );
    Ok(names)
}

/// Members rustc writes into every rlib: the symbol and name tables, the
/// crate metadata and the codegen units, with their split DWARF objects.
fn is_rustc_rlib_member(name: &str) -> bool {
    matches!(name, "/" | "//" | "/SYM64/")
        || name.starts_with("__.SYMDEF")
        || name.starts_with("lib.rmeta")
        || name.ends_with(".rcgu.o")
        || name.ends_with(".rcgu.dwo")
}

/// The rlib members that are neither rustc's own nor accounted for by a
/// `keyed` name. Each keyed name covers one member, so a name bundled twice
/// needs two keyed sources.
fn unkeyed_rlib_members<'a>(rlib_members: &'a [String], keyed: &[String]) -> Vec<&'a str> {
    let mut remaining: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for name in keyed {
        *remaining.entry(name).or_default() += 1;
    }
    rlib_members
        .iter()
        .map(String::as_str)
        .filter(|member| !is_rustc_rlib_member(member))
        .filter(|member| match remaining.get_mut(member) {
            Some(count) if *count > 0 => {
                *count -= 1;
                false
            }
            _ => true,
        })
        .collect()
}

/// The first unkeyed rlib member (see [`unkeyed_rlib_members`]) that one of
/// the unkeyed archives in the unit's native dirs could have supplied, by a
/// member name or its file name. A member that matches nothing there is left
/// alone: rustc bundles only from `native=`/`all=` dirs and the sysroot.
fn unaudited_bundled_member(
    rlib_members: &[String],
    keyed: &[String],
    candidates: &[String],
) -> Option<String> {
    unkeyed_rlib_members(rlib_members, keyed)
        .into_iter()
        .find(|member| candidates.iter().any(|candidate| candidate == member))
        .map(str::to_string)
}

struct ComputedKey {
    cache_key: String,
    /// No key yet: the closure has no record and no remote could hold the
    /// entry, so the wrapper compiles first and keys from the emitted
    /// dep-info (`cache_key` is empty). `discovery_flight` is still held so
    /// peers of the same unit wait for this compile instead of repeating it.
    deferred: bool,
    discovery_flight: Option<crate::store::StoreLock>,
    /// Did this key come from a recorded closure rather than the pre-pass?
    /// The caller owes it a re-derivation before the key may reach anything
    /// that stores or publishes.
    predicted: bool,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    key_too_new: bool,
    /// Fingerprints hashed for the key (plus the extra-inputs resolve) while
    /// the too-new guard was armed, carried past the compile for
    /// clock-independent verification.
    guard_inputs: Vec<crate::cache_key::FileFingerprint>,
}

/// A compile that ran before its key was known (deferred discovery), handed
/// back into the keyed flow.
struct Precompiled {
    result: crate::compile::CompileResult,
    compile_time_ms: u64,
    /// Taken by the key computation; `None` afterwards.
    dep_info: Option<crate::cache_key::DepInfo>,
}

/// Compile-before-key is only sound where the miss is certain from the local
/// store alone: no remote to consult, no fallback store, no adaptive
/// incremental unit and no extra-inputs declaration, the last two keying more
/// than the closure. The key computation then defers only when it can prove
/// the miss: no closure record for the unit, or no entry for the crate.
fn deferral_allowed(
    config: &Config,
    args: &RustcArgs,
    adaptive: bool,
    extra_inputs: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
) -> bool {
    // The compile must emit the dep-info the key is derived from afterwards
    // (Cargo always asks for it; a bare rustc invocation may not).
    args.dep_info_path().is_some()
        && config.deferred_discovery
        && config.remote.is_none()
        && config.fallback.is_none()
        && !adaptive
        && extra_inputs.is_none()
}

/// Where the key may fetch a portable prediction row this machine lacks: the
/// daemon's remote, when predictions are on and a remote is configured.
fn remote_prediction_rows(config: &Config) -> Option<crate::cache_key::RemoteRows> {
    if !fetches_remote_predictions(config) {
        return None;
    }
    let config = config.clone();
    Some(Box::new(move |identity: &str| {
        crate::daemon::send_prediction_fetch(&config, identity)
    }))
}

/// A row is asked of the remote only with predictions on and a remote set.
fn fetches_remote_predictions(config: &Config) -> bool {
    config.input_predictions && config.remote.is_some()
}

/// Remember the input closure this invocation discovered, so a later build of
/// the same unit can derive its key without spawning the pre-pass again.
///
/// Called only where the invocation actually succeeded: a hit that restored,
/// or a compile that exited zero. A failed compile is the case to leave alone
/// — its sources are usually mid-edit, and a record written from them would
/// only be re-validated away later at the cost of the write.
///
/// Silent and best-effort throughout. Every reason to give up (feature off, no
/// store, no closure to record, no identity) costs a future pre-pass and
/// nothing else, so none of them is worth a warning on a successful build.
fn record_input_prediction(config: &Config, store: Option<&Store>, args: &RustcArgs, wanted: bool) {
    let _trace = crate::phase_trace::phase("prediction_record");
    // Taken before any gate. The closure belongs to this invocation whether or
    // not it gets written, and leaving it in the stash would let whatever key
    // is computed next on this thread record it under a different identity.
    let dep_info = crate::cache_key::take_last_dep_info();
    if !config.input_predictions || !wanted {
        return;
    }
    let Some(dep_info) = dep_info else {
        return;
    };
    let Some(store) = store else {
        return;
    };
    let file_hasher = store.file_hasher();
    if !file_hasher.supports_input_predictions() {
        return;
    }
    let Some(identity) = crate::cache_key::rustc_prediction_identity(args) else {
        return;
    };
    // Present exactly when the key was computed under the tree guard; the
    // record must carry it or the guard will never accept the record.
    let tree = crate::cache_key::take_last_tree_digest();
    let registry = crate::cache_key::registry_src_of(&std::env::vars_os().collect::<Vec<_>>());
    // A workspace or path unit gets a row another checkout of the workspace
    // can use, when the guard was taken before rustc ran (kunobi-ninja/kache#1005).
    let workspace = crate::cache_key::workspace_record(args, &dep_info, tree.as_deref());
    file_hasher.record_input_prediction(
        &identity,
        args.crate_name.as_deref(),
        &dep_info,
        crate::cache_key::same_tree_guard(
            tree.clone(),
            crate::cache_key::is_workspace_unit(args),
            workspace.is_some(),
        ),
    );
    if let Some((identity, record)) = workspace {
        file_hasher.record_portable_prediction(&identity, args.crate_name.as_deref(), &record);
        publish_prediction(
            config,
            &identity,
            crate::prediction_share::for_remote_portable(&record, registry.as_deref()),
        );
    }
    // A source under target keeps the shared row out. A registry unit whose
    // only such sources are its own OUT_DIR gets a relocated row instead.
    if crate::cache_key::shared_prediction_can_record(args, &dep_info) {
        if let Some(identity) = crate::cache_key::rustc_shared_prediction_identity(args) {
            file_hasher.record_input_prediction(
                &identity,
                args.crate_name.as_deref(),
                &dep_info,
                tree.clone(),
            );
            // A registry unit's row serves any machine whose Cargo home has
            // the same path.
            if let Some(registry) = &registry {
                let row = crate::cache_key::InputPrediction::from_dep_info(&dep_info, tree);
                publish_prediction(
                    config,
                    &identity,
                    crate::prediction_share::for_remote_plain(&row, registry),
                );
            }
        }
    } else if let Some((identity, record)) =
        crate::cache_key::relocatable_record(args, &dep_info, tree.as_deref())
    {
        file_hasher.record_portable_prediction(&identity, args.crate_name.as_deref(), &record);
        publish_prediction(
            config,
            &identity,
            crate::prediction_share::for_remote_portable(&record, registry.as_deref()),
        );
    }
}

/// Hand a row to the daemon for the remote, when it can travel at all.
fn publish_prediction(
    config: &Config,
    identity: &str,
    row: Option<crate::prediction_share::SharedPrediction>,
) {
    if let Some(row) = row {
        crate::daemon::send_prediction_publish(config, identity, row);
    }
}

fn should_skip_cache_store_for_input_race(
    extra_inputs_racy: bool,
    modified_input_guard: bool,
    key_too_new: bool,
) -> bool {
    extra_inputs_racy || (modified_input_guard && key_too_new)
}

/// Whether keyed inputs actually changed during the compile. A tripped
/// wall-clock flag alone is not proof: it also fires when the filesystem
/// clock runs ahead of the host (NFS skew, future-stamped checkouts). When
/// every guarded input still matches its hash-time fingerprint with a strong
/// identity, nothing changed and the store refusal is excused. Anything else
/// — a mismatch, a missing file, a weak identity — keeps the refusal.
fn key_inputs_changed_during_compile(
    key_too_new: bool,
    guard_inputs: &[crate::cache_key::FileFingerprint],
) -> bool {
    key_too_new && !FileHasher::guarded_inputs_unchanged_since_hash(guard_inputs)
}

fn combine_key_measurements(
    key_ms: u64,
    extra_inputs_key_ms: u64,
    key_hash_stats: FileHashStats,
    extra_inputs_hash_stats: FileHashStats,
    key_too_new: bool,
    extra_inputs_too_new: bool,
) -> (u64, FileHashStats, bool) {
    (
        key_ms + extra_inputs_key_ms,
        FileHashStats {
            cache_hits: key_hash_stats.cache_hits + extra_inputs_hash_stats.cache_hits,
            cache_misses: key_hash_stats.cache_misses + extra_inputs_hash_stats.cache_misses,
            bytes_hashed: key_hash_stats.bytes_hashed + extra_inputs_hash_stats.bytes_hashed,
        },
        key_too_new || extra_inputs_too_new,
    )
}

/// Compute the rustc cache key. With `store` present the hasher is backed by
/// the persistent SQLite hash cache; without it (daemon fast path,
/// kunobi-ninja/kache#565) a store-free hasher still batches hashing through
/// the daemon. The key value is identical either way — the cache only changes
/// how it's computed.
/// How the key may learn a closure it has no record of.
enum KeyDiscovery {
    /// Run the dep-info pre-pass, as always.
    Immediate,
    /// Stop and let the wrapper compile first (local store only).
    Deferrable,
    /// The compile already ran; this is its emitted closure. The too-new
    /// guard is armed regardless of configuration: an input written during
    /// the compile must not be keyed as if the compiler had read it.
    Emitted(crate::cache_key::DepInfo),
    /// Run the pre-pass again for a predicted key that missed. The caller
    /// may hold this unit's discovery flight, and that lock is not
    /// re-entrant, so this computation joins no flight.
    Rederived,
}

/// Where this key computation may wait for a peer discovering the same
/// unit: nowhere without the scheduler, and nowhere for a re-derivation,
/// which would otherwise wait on the flight its own caller holds.
fn discovery_flight_dir(config: &Config, discovery: &KeyDiscovery) -> Option<PathBuf> {
    (config.scheduler && !matches!(discovery, KeyDiscovery::Rederived))
        .then(|| config.cache_dir.clone())
}

#[allow(clippy::too_many_arguments)]
fn compute_rustc_cache_key(
    config: &Config,
    compiler: &RustcCompiler,
    args: &RustcArgs,
    workspace_root: Option<&Path>,
    invocation_start_ns: i64,
    store: Option<&Store>,
    extra_inputs_digest: Option<&str>,
    extra_inputs_hash_stats: FileHashStats,
    extra_inputs_too_new: bool,
    extra_inputs_key_ms: u64,
    mut extra_inputs_guard_inputs: Vec<crate::cache_key::FileFingerprint>,
    discovery: KeyDiscovery,
) -> Result<ComputedKey> {
    let key_start = std::time::Instant::now();
    let emitted = matches!(discovery, KeyDiscovery::Emitted(_));
    let flight_dir = discovery_flight_dir(config, &discovery);
    crate::cache_key::set_defer_discovery(matches!(discovery, KeyDiscovery::Deferrable));
    if let KeyDiscovery::Emitted(dep_info) = discovery {
        crate::cache_key::provide_dep_info(dep_info);
    }
    let mut file_hasher = match store {
        Some(store) => store.file_hasher_with_daemon(config.socket_path()),
        None => crate::cache_key::FileHasher::new().with_daemon(config.socket_path()),
    }
    .with_input_predictions(config.input_predictions)
    .with_prediction_flights(flight_dir);
    if config.modified_input_guard || emitted {
        // Flag keyed inputs touched at/after this invocation started — their
        // content at hash time may differ from what rustc reads, so we'll look
        // up but refuse to store (kunobi-ninja/kache#324).
        file_hasher.arm_too_new_guard(invocation_start_ns, 0);
    }
    // Workspace root for normalization: use the output-derived candidate only
    // when it is verified against Cargo's cwd. An external target directory
    // otherwise points at an unrelated parent; keying and rustc injection must
    // both fall back to cwd through `RustcArgs::path_normalization_root`.
    // Re-virtualize rust std sources to `/rustc/<hash>` so profilers resolve
    // them (kunobi-ninja/kache#485). MUST match the injection-side normalizer in
    // `RustcCompiler::execute`, or the key would represent one remap rule set
    // and the binary another.
    let path_normalizer = crate::path_normalizer::PathNormalizer::from_env(workspace_root)
        .with_target_dir(args.target_dir().as_deref())
        .with_base_dirs(&config.base_dirs)
        .with_path_only_env_vars(config.path_only_env_vars.clone())
        .with_rust_src_rule(
            crate::cache_key::get_rustc_sysroot(args).as_deref(),
            crate::cache_key::get_rustc_commit_hash(&args.rustc).as_deref(),
        );
    let key_ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &config.cache_dir,
        key_salt: config.key_salt.as_deref(),
        key_env_vars: &config.key_env_vars,
        extra_inputs_digest,
    };
    let cache_key = match compiler.cache_key(args, &key_ctx) {
        Ok(cache_key) => cache_key,
        Err(error)
            if error
                .downcast_ref::<crate::cache_key::DeferredDiscovery>()
                .is_some() =>
        {
            crate::cache_key::set_defer_discovery(false);
            return Ok(ComputedKey {
                cache_key: String::new(),
                deferred: true,
                discovery_flight: file_hasher.take_discovery_flight(),
                predicted: false,
                key_ms: key_start.elapsed().as_millis() as u64,
                key_hash_stats: file_hasher.stats(),
                key_too_new: false,
                guard_inputs: extra_inputs_guard_inputs,
            });
        }
        Err(error) => {
            crate::cache_key::set_defer_discovery(false);
            return Err(error);
        }
    };
    crate::cache_key::set_defer_discovery(false);
    let key_hash_stats = file_hasher.stats();
    extra_inputs_guard_inputs.extend(file_hasher.take_guarded_inputs());
    let (key_ms, key_hash_stats, key_too_new) = combine_key_measurements(
        key_start.elapsed().as_millis() as u64,
        extra_inputs_key_ms,
        key_hash_stats,
        extra_inputs_hash_stats,
        file_hasher.too_new(),
        extra_inputs_too_new,
    );
    Ok(ComputedKey {
        cache_key,
        deferred: false,
        discovery_flight: file_hasher.take_discovery_flight(),
        predicted: crate::cache_key::take_last_key_used_prediction(),
        key_ms,
        key_hash_stats,
        key_too_new,
        guard_inputs: extra_inputs_guard_inputs,
    })
}

/// Does this key still owe a re-derivation before it may CLAIM or STORE?
///
/// Reached only once both the local and the remote lookup have missed, since
/// a hit returns from inside the loop. So the question is no longer "did we
/// find it" but only "is this key still a guess": a key that was never
/// predicted is the discovered one already, and so is one already re-derived.
///
/// Reading the cache under a predicted key is deliberately NOT gated here. An
/// entry anywhere was stored under a key computed from a discovered closure,
/// so matching it proves the prediction reproduced that closure — the same
/// argument for a remote entry as for a local one.
fn owes_rederivation(predicted: bool, already_rederived: bool) -> bool {
    predicted && !already_rederived
}

/// Should this invocation write what it discovered back to the record?
///
/// A closure that came from a record is already recorded, and rewriting it on
/// every hit would be a database write per compile for no new information. A
/// re-derivation is the opposite case: its closure is what the record should
/// have said, so writing it is what repairs a stale row.
fn should_record_closure(predicted: bool, rederived: bool) -> bool {
    !predicted || rederived
}

/// Recompute the key with the dep-info pre-pass, ignoring any record.
///
/// Used for exactly one thing: turning a predicted key that missed into a key
/// discovered the slow way, before the invocation is allowed to store, claim
/// or ask a remote anything.
fn recompute_key_without_prediction(
    config: &Config,
    compiler: &RustcCompiler,
    args: &RustcArgs,
    workspace_root: Option<&Path>,
    invocation_start_ns: i64,
    store: Option<&Store>,
    extra_inputs_digest: Option<&str>,
) -> Result<ComputedKey> {
    let mut without = config.clone();
    without.input_predictions = false;
    compute_rustc_cache_key(
        &without,
        compiler,
        args,
        workspace_root,
        invocation_start_ns,
        store,
        extra_inputs_digest,
        FileHashStats::default(),
        false,
        0,
        Vec::new(),
        KeyDiscovery::Rederived,
    )
}

/// Complete an entry made available by a remote check. `None` means no entry;
/// a restore error stays distinct so the caller recompiles without reporting a hit.
fn try_rustc_remote_hit(
    hit: &RustcHitContext<'_>,
    store: &Store,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    lookup_ms: u64,
    record_closure: bool,
) -> Option<Result<()>> {
    let (meta, result) = acquire_entry(
        hit.config,
        store,
        cache_key,
        hit.crate_name,
        NegativeReply::CheckConcurrentEntry,
    )?;
    Some(hit.restore_and_finish(
        BlobSource::Store(store),
        &meta,
        result,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        record_closure.then_some(store),
    ))
}

/// Daemon fast path (kunobi-ninja/kache#565): returns `Some(exit_code)` only
/// when the daemon served a hit AND the restore succeeded. Every other
/// outcome returns `None` and the caller runs the fully local path — which
/// owns eviction/repair for whatever the daemon or restore stumbled on.
fn try_daemon_local_hit(
    hit: &RustcHitContext<'_>,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
) -> Option<i32> {
    let _trace = crate::phase_trace::phase("daemon_hit_path");
    let lookup_start = std::time::Instant::now();
    let target_dir = hit.args.target_dir();
    let trace_lookup = crate::phase_trace::phase("lookup");
    let reply = crate::daemon::send_local_lookup(
        hit.config,
        cache_key,
        target_dir.as_deref(),
        hit.args.path_normalization_root(),
    )?;
    drop(trace_lookup);
    let lookup_ms = lookup_start.elapsed().as_millis() as u64;
    if reply.outcome != "hit" {
        return None;
    }
    let meta = reply.meta?;
    if meta.files.is_empty() || meta.cache_key != cache_key {
        return None;
    }

    if let Err(e) = hit.restore_and_finish(
        BlobSource::StoreDir(hit.config.store_dir()),
        &meta,
        EventResult::LocalHit,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        None,
    ) {
        // Includes a blob evicted between the daemon's pin and our reflink —
        // the local path below recompiles; never serve a partial hit.
        tracing::warn!(
            "daemon local hit restore failed for {}: {} — running local path",
            hit.crate_name,
            e
        );
        return None;
    }
    Some(0)
}

/// Where restore reads blobs from: an open store (classic path) or just the
/// store directory (kunobi-ninja/kache#565 daemon path — the blob layout is
/// shared, so no SQLite handle is needed to resolve content-addressed paths).
enum BlobSource<'a> {
    Store(&'a Store),
    StoreDir(PathBuf),
}

impl BlobSource<'_> {
    fn blob_path(&self, hash: &str) -> PathBuf {
        match self {
            BlobSource::Store(store) => store.blob_path(hash),
            BlobSource::StoreDir(dir) => crate::store::blob_path_in_store_dir(dir, hash),
        }
    }

    /// Evict a broken entry when a store handle exists. The daemon path has
    /// none; its caller falls back to the classic path, which re-detects the
    /// breakage via `Store::get`/restore and evicts there.
    fn remove_entry(&self, cache_key: &str) {
        if let BlobSource::Store(store) = self {
            let _ = store.remove_entry(cache_key);
        }
    }

    /// Tell the file-hash memo what these just-restored artifacts hash to
    /// (kunobi-ninja/kache#540).
    ///
    /// A restored `.rlib`/`.rmeta` is a compiler input for every downstream
    /// crate in the same build, and hashing it is how those crates' cache keys
    /// get computed. The entry already carries a verified blake3 for each
    /// blob, and an [`RestoredBytes::ExactBlobCopy`] restore put exactly those
    /// bytes on disk, so the read is redundant — this is the restore-side
    /// counterpart to the seeding `Store::put` already does for
    /// freshly-compiled outputs. Mis-seeding cannot outlive the file: the memo
    /// is keyed on size + mtime + ctime + inode, so any later write to the
    /// artifact retires the row rather than serving it.
    ///
    /// Each pair is recorded against the fingerprint that was observed to hold
    /// the blob's bytes, never against a fresh stat of the path. That is what
    /// makes a late write harmless rather than dangerous: if anything
    /// overwrote the artifact after its restore, the row simply stops matching
    /// and the file gets hashed for real. Recording in order also means the
    /// last write wins for a path, matching what survives on disk.
    ///
    /// Best-effort by construction — `record_verified_file_hash` drops files
    /// below the memo's size floor, which the hasher would not consult anyway.
    /// The daemon-assisted local-hit path (`local_hit_daemon`, off by default)
    /// holds no store handle here and is not seeded; it would need the daemon
    /// to record on its behalf.
    fn record_known_file_hashes(&self, restored: &[(crate::cache_key::FileFingerprint, &str)]) {
        let BlobSource::Store(store) = self else {
            return;
        };
        let _trace = crate::phase_trace::phase("memo_restored");
        let restored: Vec<_> = restored
            .iter()
            .map(|(fingerprint, hash)| (fingerprint.clone(), *hash))
            .collect();
        store.record_verified_file_hashes(&restored);
    }
}

/// Replay cached compiler diagnostics to the given sinks, exactly as a fresh
/// compile would emit them — so a cache hit, or a coalesced restore, never
/// swallows the original warnings and notes. Empty streams write nothing.
///
/// Split out (and written to injectable sinks) so the "non-empty stream is
/// replayed, empty stream is skipped" contract is unit-testable without
/// capturing the process's real stdout/stderr.
fn replay_diagnostics(
    stdout: &str,
    stderr: &str,
    mut out: impl std::io::Write,
    mut err: impl std::io::Write,
) {
    if !stdout.is_empty() {
        let _ = write!(out, "{stdout}");
    }
    if !stderr.is_empty() {
        let _ = write!(err, "{stderr}");
    }
}

fn replay_cached_diagnostics(
    meta: &crate::store::EntryMeta,
    out: impl std::io::Write,
    err: impl std::io::Write,
) {
    replay_diagnostics(&meta.stdout, &meta.stderr, out, err);
}

/// When `KACHE_VERIFY` is on, recompile into a staging directory and compare
/// those artifacts to the files just restored.
///
/// Fail-open: never fails the restore, never overwrites restored outputs, and
/// never prints the qualification rustc's diagnostics onto the hit's
/// stdout/stderr (cargo fingerprints those streams).
fn maybe_verify_restored_hit(
    compiler: &RustcCompiler,
    args: &RustcArgs,
    restored: &[(String, PathBuf)],
) {
    if !crate::verify_compare::enabled() {
        return;
    }
    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");
    match run_verify_recompile(compiler, args, restored) {
        Ok(report) => {
            let summary = report.event_summary();
            match report.worst() {
                crate::verify_compare::DivergenceClass::Match => {
                    tracing::info!(
                        crate_name,
                        summary = %summary,
                        "KACHE_VERIFY: restored artifacts match a fresh compile"
                    );
                }
                crate::verify_compare::DivergenceClass::PathDebug => {
                    tracing::warn!(
                        crate_name,
                        summary = %summary,
                        "KACHE_VERIFY: path/debug divergence versus a fresh compile"
                    );
                }
                crate::verify_compare::DivergenceClass::Content => {
                    tracing::error!(
                        crate_name,
                        summary = %summary,
                        "KACHE_VERIFY: content mismatch versus a fresh compile; serving the restored hit"
                    );
                }
            }
            crate::verify_compare::record_report(summary);
        }
        Err(error) => {
            let summary = format!("recompile-failed: {error:#}");
            tracing::error!(
                crate_name,
                error = %error,
                "KACHE_VERIFY: recompile failed; serving the restored hit"
            );
            crate::verify_compare::record_report(summary);
        }
    }
}

fn run_verify_recompile(
    compiler: &RustcCompiler,
    args: &RustcArgs,
    restored: &[(String, PathBuf)],
) -> Result<crate::verify_compare::CompareReport> {
    let staging = tempfile::Builder::new()
        .prefix("kache-verify-")
        .tempdir()
        .context("creating KACHE_VERIFY staging directory")?;
    let staged_args = retarget_rustc_args_for_staging(args, staging.path());
    let result = crate::opcounts::suspend_spawn_counts(|| compiler.execute(&staged_args))
        .context("running KACHE_VERIFY recompile")?;
    verify_recompile_exit_status(result.exit_code, &result.stderr)?;
    let mut compiled_by_name = std::collections::BTreeMap::new();
    for artifact in result.artifacts.outputs() {
        compiled_by_name.insert(artifact.store_name.clone(), artifact.path.clone());
        if let Some(file_name) = artifact.path.file_name() {
            compiled_by_name
                .entry(file_name.to_string_lossy().into_owned())
                .or_insert_with(|| artifact.path.clone());
        }
    }
    for (name, _) in restored {
        let staged = staging.path().join(name);
        if staged.is_file() {
            compiled_by_name.entry(name.clone()).or_insert(staged);
        }
    }
    Ok(crate::verify_compare::compare_named_artifacts(
        restored,
        &compiled_by_name,
    ))
}

fn verify_recompile_exit_status(exit_code: i32, stderr: &str) -> Result<()> {
    if exit_code == 0 {
        return Ok(());
    }
    anyhow::bail!(
        "rustc exited {exit_code}{}",
        verify_recompile_stderr_hint(stderr)
    )
}

fn verify_recompile_stderr_hint(stderr: &str) -> String {
    let line = stderr
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('{'))
        .unwrap_or("");
    if line.is_empty() {
        String::new()
    } else {
        format!(" ({line})")
    }
}

/// Point this invocation's output flags at `staging` without changing the
/// frozen path-normalization root, so the qualification compile writes a
/// private tree and does not pre-clean restored artifacts.
fn retarget_rustc_args_for_staging(args: &RustcArgs, staging: &Path) -> RustcArgs {
    let mut staged = args.clone();
    staged.all_args = rewrite_output_argv(&args.all_args, staging);
    staged.out_dir = args.out_dir.as_ref().map(|_| staging.to_path_buf());
    if let Some(output) = &args.output {
        let name = output.file_name().unwrap_or_default();
        staged.output = Some(staging.join(name));
    }
    if let Some(dep) = &args.dep_info_output {
        let name = dep.file_name().unwrap_or_default();
        staged.dep_info_output = Some(staging.join(name));
    }
    staged
}

fn rewrite_output_argv(argv: &[String], staging: &Path) -> Vec<String> {
    let staging_s = staging.display().to_string();
    let mut out = Vec::with_capacity(argv.len());
    let mut args = argv.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--out-dir" => {
                out.push(arg.clone());
                if args.next().is_some() {
                    out.push(staging_s.clone());
                }
            }
            "-o" => {
                out.push(arg.clone());
                if let Some(next) = args.next() {
                    let name = Path::new(next).file_name().unwrap_or_default();
                    out.push(staging.join(name).to_string_lossy().into_owned());
                }
            }
            "--emit" => {
                out.push(arg.clone());
                if let Some(next) = args.next() {
                    out.push(rewrite_emit_value(next, staging));
                }
            }
            _ if arg.starts_with("--out-dir=") => {
                out.push(format!("--out-dir={staging_s}"));
            }
            _ => {
                if let Some(value) = arg.strip_prefix("--emit=") {
                    out.push(format!("--emit={}", rewrite_emit_value(value, staging)));
                } else {
                    out.push(arg.clone());
                }
            }
        }
    }
    out
}

fn rewrite_emit_value(value: &str, staging: &Path) -> String {
    value
        .split(',')
        .map(|part| match part.split_once('=') {
            Some((kind, path)) if !path.is_empty() => {
                let name = Path::new(path).file_name().unwrap_or_default();
                format!("{kind}={}", staging.join(name).display())
            }
            _ => part.to_string(),
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn restore_from_cache(
    config: &Config,
    compiler: &RustcCompiler,
    blobs: &BlobSource<'_>,
    args: &RustcArgs,
    meta: &crate::store::EntryMeta,
    extra_inputs: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
) -> Result<()> {
    let _trace = crate::phase_trace::phase("restore");
    let current = resolve_extra_inputs_for_passthrough(config, args)
        .context("revalidating extra_inputs before cache-hit publication")?;
    anyhow::ensure!(
        current.as_ref() == extra_inputs,
        "extra_inputs declaration changed during cache lookup; refusing the stale hit"
    );

    // Emit-coverage gate (kunobi-ninja/kache#325): a stored entry must contain
    // outputs covering every `--emit` kind this invocation requested. An entry
    // that doesn't — a partial store from a pre-gate / directory-scan producer,
    // or on-disk corruption — is evicted and surfaced as an error so the caller
    // recompiles a complete entry. Entries with no recorded `emit_kinds`
    // (pre-gate `meta.json`) skip the check, so no mass invalidation.
    if !meta.covers_requested_emit(&args.emit) {
        blobs.remove_entry(&meta.cache_key);
        anyhow::bail!(
            "cached entry for {} covers --emit {:?} but this invocation requested {:?} \
             — evicting partial entry and recompiling",
            meta.crate_name,
            meta.emit_kinds,
            args.emit
        );
    }

    // Legacy entries may predate emit-kind metadata and therefore bypass the
    // coverage gate above. Active extra inputs still require a real `.d` blob:
    // without one the outer success epilogue would fail after reporting a hit,
    // leaving the same unusable entry to brick every retry.
    let expected_dep_info_name = extra_inputs.and_then(|_| {
        args.dep_info_path()
            .and_then(|path| path.file_name().map(std::ffi::OsStr::to_os_string))
    });
    if let Some(expected) = &expected_dep_info_name
        && !meta.files.iter().any(|file| {
            matches!(
                crate::compiler::classify_by_filename(&file.name),
                crate::compiler::ArtifactKind::DepInfo
            ) && Path::new(&file.name).file_name() == Some(expected.as_os_str())
        })
    {
        blobs.remove_entry(&meta.cache_key);
        anyhow::bail!(
            "cached entry for {} has no dep-info artifact named {} required by active \
             extra_inputs; evicting the legacy entry and recompiling",
            meta.crate_name,
            expected.to_string_lossy()
        );
    }

    // Determine where output files go: either -o parent dir, or --out-dir
    let output_dir = if let Some(output) = &args.output {
        output.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else if let Some(dir) = &args.out_dir {
        dir.clone()
    } else {
        anyhow::bail!("no output path (-o) or output directory (--out-dir) in args");
    };

    // Ensure the output directory exists before restoring any files.
    // This avoids redundant `create_dir_all` syscalls per file (issue #563)
    // while preventing missing-directory diagnostics on Windows.
    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("creating output directory {}", output_dir.display()))?;

    // Anchors for dep-info (`.d`) expansion. Cached blobs independently
    // relativize the producer's target directory and package working
    // directory; restore re-roots both for this invocation so Cargo watches
    // the consumer worktree rather than a live donor (#760).
    // Falls back to cwd only for ad-hoc invocations outside cargo's
    // layout, where there is no cached `.d` to rewrite anyway.
    let cargo_target_dir = args.target_dir();
    let depinfo_anchor = cargo_target_dir
        .clone()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| Path::new(".").to_path_buf());
    let depinfo_working_dir =
        std::env::current_dir().unwrap_or_else(|_| Path::new(".").to_path_buf());
    let depinfo_workspace_dir = args.path_normalization_root();
    let depinfo_configured_roots =
        configured_rustc_depinfo_roots(config, depinfo_workspace_dir, cargo_target_dir.as_deref());

    // Dep-info validation gate (kunobi-ninja/kache#330): a restored `.d`
    // whose paths do not resolve for THIS consumer poisons cargo's
    // freshness check with MissingFile and the crate recompiles on every
    // subsequent build, forever — the recompile is served by the same
    // entry, restoring the same broken `.d`, so the loop never breaks.
    // Field report: entries stored before the Windows separator fix in
    // `rewrite_depinfo_content` carry the builder's absolute paths.
    // Validate every referenced path BEFORE materializing anything; a
    // miss evicts the entry so the recompile stores a portable one —
    // self-healing, mirroring the emit-coverage gate above.
    for cached_file in &meta.files {
        if !matches!(
            crate::compiler::classify_by_filename(&cached_file.name),
            crate::compiler::ArtifactKind::DepInfo
        ) {
            continue;
        }
        if expected_dep_info_name.as_ref().is_some_and(|expected| {
            Path::new(&cached_file.name).file_name() != Some(expected.as_os_str())
        }) {
            continue;
        }
        let blob = blobs.blob_path(&cached_file.hash);
        let raw = match read_cached_dep_info_blob(&blob, extra_inputs.is_some()) {
            Ok(Some(raw)) => raw,
            Ok(None) => continue,
            Err(error) => {
                blobs.remove_entry(&meta.cache_key);
                return Err(error).with_context(|| {
                    format!(
                        "cached dep-info for {} is unreadable or not UTF-8; evicting the entry",
                        meta.crate_name
                    )
                });
            }
        };
        let expanded = crate::link::rewrite_rustc_depinfo_content_with_configured_roots(
            &raw,
            &depinfo_anchor,
            &depinfo_working_dir,
            depinfo_workspace_dir,
            &depinfo_configured_roots,
            link::DepInfoMode::Expand,
        );
        let expanded = if let Some(snapshot) = extra_inputs {
            match snapshot.merge_dep_info_content(&expanded) {
                Ok(completed) => completed,
                Err(error) => {
                    blobs.remove_entry(&meta.cache_key);
                    return Err(error).with_context(|| {
                        format!(
                            "cached dep-info for {} cannot be completed safely; evicting the entry",
                            meta.crate_name
                        )
                    });
                }
            }
        } else {
            expanded
        };
        let dependencies = match crate::extra_inputs::parse_dep_info_dependencies(&expanded) {
            Ok(dependencies) if !dependencies.is_empty() => dependencies,
            Ok(_) => {
                blobs.remove_entry(&meta.cache_key);
                anyhow::bail!(
                    "cached dep-info for {} has no dependencies; evicting the entry and recompiling",
                    meta.crate_name
                );
            }
            Err(error) => {
                blobs.remove_entry(&meta.cache_key);
                return Err(error).with_context(|| {
                    format!(
                        "cached dep-info for {} is malformed; evicting the entry",
                        meta.crate_name
                    )
                });
            }
        };
        for dep in dependencies {
            if !dep.exists() {
                blobs.remove_entry(&meta.cache_key);
                anyhow::bail!(
                    "cached dep-info for {} references {} which does not resolve here — \
                     evicting the entry and recompiling (#330)",
                    meta.crate_name,
                    dep.display()
                );
            }
        }
    }

    // One platform per restore, shared across every cached file. The
    // detect call is cheap (cfg cascade) but doing it once keeps the
    // tracing context coherent and lets a future per-restore override
    // (e.g. cross-restore from a Linux cache to a macOS host) plug in
    // at one site.
    let platform = platform::current();
    tracing::debug!(
        "restoring {} files via platform={}",
        meta.files.len(),
        platform.name()
    );
    let shared_loadable = shared_inode_loadable(args, platform.may_share_restored_loadables());

    // Artifacts that came back as verbatim blob copies, each paired with the
    // digest the entry already recorded for it (kunobi-ninja/kache#540).
    let mut exact_restores: Vec<(crate::cache_key::FileFingerprint, &str)> = Vec::new();
    let mut restored_paths: Vec<(String, PathBuf)> = Vec::with_capacity(meta.files.len());

    for cached_file in &meta.files {
        // Defense-in-depth trust-boundary check (kunobi-ninja/kache#211):
        // `import_downloaded_entry` already rejects unsafe names, but a name that
        // is absolute or contains `..` would escape `--out-dir` on join
        // (`dir.join("/abs") == "/abs"`), overwriting files outside `target/`.
        // Refuse to restore such an entry — the caller recompiles.
        if !crate::remote_layout::is_safe_artifact_name(&cached_file.name) {
            anyhow::bail!(
                "refusing to restore cache entry with unsafe artifact name {:?}",
                cached_file.name
            );
        }

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

        // Per-file dispatch by artifact kind: `classify_output` picks
        // the kind, `plan_post_restore` the actions — no ad-hoc filename
        // matching at the call site.
        let kind = compiler.classify_output(args, &cached_file.name);
        let restored = materialize_cached_artifact(
            blobs,
            cached_file,
            &target_path,
            kind,
            shared_loadable,
            &depinfo_anchor,
            &depinfo_working_dir,
            depinfo_workspace_dir,
            &depinfo_configured_roots,
            &*platform,
            "rustc restore",
            extra_inputs,
        )?;
        if let RestoredBytes::ExactBlobCopy(fingerprint) = restored {
            exact_restores.push((fingerprint, &cached_file.hash));
        }
        restored_paths.push((cached_file.name.clone(), target_path));
    }

    blobs.record_known_file_hashes(&exact_restores);

    maybe_verify_restored_hit(compiler, args, &restored_paths);

    Ok(())
}

fn read_cached_dep_info_blob(
    path: &Path,
    extra_inputs_active: bool,
) -> std::io::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(Some(raw)),
        Err(error) if extra_inputs_active => Err(error),
        Err(_) => Ok(None),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PassthroughOutput {
    exit_code: i32,
    fallback: bool,
    fallback_attempt: Option<crate::fallback::Attempt>,
}

/// Pass through to rustc without caching.
///
/// If a fallback wrapper is configured, the compile is handed to it
/// (`<fallback> <rustc> <args>`) instead — kache declined to cache it,
/// so the fallback gets a chance. By default, even plain passthroughs
/// strip incremental flags to prevent APFS-related corruption in git
/// worktrees on macOS. The explicit preservation mode instead moves
/// incremental state to a stable path that kache never registers for GC.
fn passthrough(
    args: &RustcArgs,
    fallback: Option<&str>,
    preserve_incremental: bool,
) -> Result<PassthroughOutput> {
    let isolated_args = preserve_incremental
        .then(|| compile::isolate_incremental_flags(&args.all_args))
        .flatten();
    let incremental_preserved = args.incremental.is_some() && isolated_args.is_some();
    let compiler_args = if let Some(isolated_args) = isolated_args {
        isolated_args
    } else {
        compile::strip_incremental_flags(&args.all_args)
            .into_iter()
            .cloned()
            .collect()
    };
    passthrough_args(args, fallback, &compiler_args, incremental_preserved)
}

fn passthrough_direct_args<'a>(
    args: &'a RustcArgs,
    compiler_args: &'a [String],
    compiler_args_changed: bool,
) -> Vec<&'a String> {
    if args.has_expanded_argfiles() && !compiler_args_changed {
        args.raw_args().iter().collect()
    } else {
        compiler_args.iter().collect()
    }
}

fn compiler_args_changed(args: &RustcArgs, compiler_args: &[String]) -> bool {
    compiler_args != args.all_args.as_slice()
}

fn stripped_incremental_count(args: &RustcArgs, compiler_args: &[String]) -> Option<usize> {
    let count = args.all_args.len().saturating_sub(compiler_args.len());
    (count > 0).then_some(count)
}

fn handle_response_file_error(
    error: anyhow::Error,
    compiler_args_changed: bool,
) -> Result<Option<compile::RustcResponseFile>> {
    if compiler_args_changed {
        return Err(error)
            .context("materializing rustc response file after rewriting incremental arguments");
    }
    tracing::warn!(
        "failed to materialize expanded rustc response file; using unchanged original argv: {error:#}"
    );
    Ok(None)
}

/// Run a rustc passthrough with an already-decided argument vector.
///
/// Explicit preservation may supply an already-isolated argument vector.
/// Ordinary passthroughs retain the configured fallback-wrapper contract.
fn passthrough_args(
    args: &RustcArgs,
    fallback: Option<&str>,
    compiler_args: &[String],
    incremental_preserved: bool,
) -> Result<PassthroughOutput> {
    let compiler_args_changed = compiler_args_changed(args, compiler_args);
    let stripped_incremental = stripped_incremental_count(args, compiler_args);
    if incremental_preserved {
        tracing::info!(
            "[kache] passthrough: preserving isolated incremental state for {}",
            args.crate_name.as_deref().unwrap_or("unknown")
        );
    } else if let Some(stripped) = stripped_incremental {
        tracing::info!(
            "[kache] passthrough: stripped {} incremental flag(s) for {}",
            stripped,
            args.crate_name.as_deref().unwrap_or("unknown")
        );
    }

    // Keep successfully-expanded invocations compact and apply Kache's
    // incremental policy before re-serializing them. On a temp-file failure,
    // reuse the original compact argv only if no effective argument changed.
    // A rewritten invocation fails closed: expanded argv could promote a
    // nested `@file` to top-level expansion or exceed the platform argv limit,
    // while raw argv could leak Cargo's non-isolated incremental directory.
    let response_file = if args.has_expanded_argfiles() {
        match compile::RustcResponseFile::new(compiler_args.iter().map(|arg| arg.as_str())) {
            Ok(response) => Some(response),
            Err(error) => handle_response_file_error(error, compiler_args_changed)?,
        }
    } else {
        None
    };
    let direct_args = response_file
        .is_none()
        .then(|| passthrough_direct_args(args, compiler_args, compiler_args_changed));

    // A prior cache hit may have restored read-only (0444) hardlinks into the
    // target dir; rustc can't overwrite those and fails with EACCES. The cached
    // path pre-cleans them in `run_rustc`, and the disabled/re-entrant path does
    // so in `run_compiler_directly` — but a kache-declined *passthrough* (refuse
    // reason, non-primary, etc.) ran straight into the read-only outputs. Clean
    // them here too. When the parse couldn't recover crate_name/extra-filename
    // this still can't act, but `pre_clean_outputs` now logs that at debug
    // (rio-build#51 / kache#242).
    compile::pre_clean_outputs(
        args.output.as_deref(),
        args.out_dir.as_deref(),
        args.crate_name.as_deref(),
        args.extra_filename.as_deref(),
        &args.emit,
    );

    let mut fallback_attempt = None;
    // Configured fallback wrapper: `<fallback> <rustc> [<inner-rustc>]
    // <args>`. Failures fall through to direct compilation.
    if let Some(fb) = fallback {
        let mut cmd = std::process::Command::new(fb);
        if disable_incremental_env(incremental_preserved) {
            cmd.env("CARGO_INCREMENTAL", "0");
        }
        cmd.arg(&args.rustc);
        if let Some(inner) = &args.inner_rustc {
            cmd.arg(inner);
        }
        if let Some(response) = &response_file {
            cmd.arg(response.argument());
        } else if let Some(direct) = &direct_args {
            cmd.args(direct);
        }
        let outputs: Vec<&Path> = args
            .output
            .as_deref()
            .map(Path::new)
            .into_iter()
            .chain(args.out_dir.as_deref().map(Path::new))
            .collect();
        let attempt = crate::fallback::run(cmd, fb, &outputs, compiler_args);
        if let Some(exit_code) = attempt.terminal_code() {
            return Ok(PassthroughOutput {
                exit_code,
                fallback: true,
                fallback_attempt: Some(attempt),
            });
        }
        fallback_attempt = Some(attempt);
    }

    let mut cmd = std::process::Command::new(&args.rustc);
    if disable_incremental_env(incremental_preserved) {
        cmd.env("CARGO_INCREMENTAL", "0");
    }
    // Double-wrapper: pass the inner rustc path as first arg to the workspace wrapper
    if let Some(inner) = &args.inner_rustc {
        cmd.arg(inner);
    }
    if let Some(response) = &response_file {
        cmd.arg(response.argument());
    } else if let Some(direct) = &direct_args {
        cmd.args(direct);
    }
    let status = cmd
        .status()
        .with_context(|| format!("executing {}", args.rustc.display()))?;
    Ok(PassthroughOutput {
        exit_code: status.code().unwrap_or(1),
        fallback: false,
        fallback_attempt,
    })
}

fn reset_adaptive_unit(unit: Option<&AdaptiveUnit>) {
    if let Some(unit) = unit {
        let _ = unit.reset();
    }
}

/// Run a user-facing executable that artifact caching already excludes.
/// Eligible Cargo-primary units preserve isolated incremental state immediately
/// when no configured fallback owns declined compilations. Other rejection
/// classes keep the configured fallback contract and do not call this helper.
#[allow(clippy::too_many_arguments)]
fn intentional_passthrough_with_event<R: Into<String>>(
    config: &Config,
    args: &RustcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    adaptive_unit: Option<&AdaptiveUnit>,
    reason: R,
) -> Result<i32> {
    let reason = reason.into();
    if config.fallback.is_none()
        && let Some(lease) = adaptive_unit.and_then(AdaptiveUnit::try_immediate)
    {
        return adaptive_incremental_with_event(
            config,
            args,
            crate_name,
            root,
            start,
            lease,
            format!("adaptive passthrough: {reason}"),
            None,
        );
    }
    passthrough_with_event(config, args, crate_name, root, start, reason)
}

/// Compile with policy-owned incremental state and never publish the result
/// under Kache's normal artifact key. The lease serializes users of that
/// unit's private rustc state through the child lifetime; lock contention
/// falls back to the normal cache path.
#[allow(clippy::too_many_arguments)]
fn adaptive_incremental_with_event<R: Into<String>>(
    config: &Config,
    args: &RustcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    lease: Lease,
    reason: R,
    keyed: Option<(&str, u64, FileHashStats, u64)>,
) -> Result<i32> {
    let reason = reason.into();
    let kind = lease.kind();
    let compiler_args = lease.compiler_args(args);
    let compile_start = std::time::Instant::now();
    let compiler = RustcCompiler::new().with_base_dirs(config.base_dirs.clone());
    let compile = if kind == crate::incremental_policy::LeaseKind::Immediate {
        compiler.execute_passthrough_preserving_incremental(args, &compiler_args)
    } else {
        compiler.execute_preserving_incremental(args, &compiler_args)
    };
    let result = match compile {
        Ok(result) => result,
        Err(error) => {
            let _ = lease.finish(false);
            tracing::warn!("adaptive incremental compiler spawn failed for {crate_name}: {error}");
            return passthrough_with_event(
                config,
                args,
                crate_name,
                root,
                start,
                format!("adaptive compiler spawn failed: {error}"),
            );
        }
    };
    let compile_time_ms = compile_start.elapsed().as_millis() as u64;
    replay_diagnostics(
        &result.stdout,
        result.pending_stderr(),
        std::io::stdout(),
        std::io::stderr(),
    );
    let reusable = lease.finish(result.exit_code == 0);
    tracing::debug!(
        ?kind,
        reusable,
        "adaptive incremental compiler lease finished"
    );

    let (cache_key, key_ms, key_hash_stats, lookup_ms) =
        keyed.unwrap_or(("", 0, FileHashStats::default(), 0));
    log_event_details(
        config,
        root,
        crate_name,
        EventResult::Passthrough,
        start.elapsed().as_millis() as u64,
        compile_time_ms,
        0,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        0,
        0,
        StorePutResult::default(),
        reason,
        String::new(),
        String::new(),
        false,
        Some(result.exit_code),
        None,
    );
    Ok(result.exit_code)
}

thread_local! {
    /// Set while the keyed flow re-enters after a deferred compile: the
    /// compiler already ran and printed its diagnostics (with the artifact
    /// notifications Cargo pipelines on), so no branch may run it again.
    static PRECOMPILED_EXIT: std::cell::Cell<Option<i32>> = const { std::cell::Cell::new(None) };
}

fn passthrough_with_event<R: Into<String>>(
    config: &Config,
    args: &RustcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    if let Some(exit_code) = PRECOMPILED_EXIT.with(std::cell::Cell::get) {
        let reason = reason.into();
        tracing::debug!("{crate_name}: compiled, not stored: {reason}");
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            root,
            crate_name,
            EventResult::Skipped,
            elapsed,
            0,
            0,
            "",
            0,
            FileHashStats::default(),
            0,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Skipped, elapsed, 0);
        return Ok(exit_code);
    }
    let output = passthrough(
        args,
        config.fallback.as_deref(),
        config.preserve_incremental,
    )?;
    log_passthrough_event(
        config,
        root,
        crate_name,
        start.elapsed().as_millis() as u64,
        reason.into(),
        &output,
    );
    Ok(output.exit_code)
}

/// The codegen backend dylib that keeps a rustc compile out of the cache: any
/// dylib when backends are untrusted, and a trusted one kache cannot key.
fn untrusted_codegen_backend(backend: Option<&str>, trusted: bool) -> Option<&str> {
    backend.filter(|backend| !trusted || !crate::args::codegen_backend_is_keyable(backend))
}

/// Passthrough reason for a rustc compile whose codegen backend dylib is not
/// trusted. The string is a contract: reports group passthroughs by it.
const UNTRUSTED_CODEGEN_BACKEND_REASON: &str = "unsupported|rustc codegen backend dylib (-Zcodegen-backend=<path>) may write files kache cannot restore; set cache.trust_codegen_backends and pass the backend as a path to cache it";

/// Run rustc without caching and without the configured fallback, for
/// compiles no cache can replay correctly.
fn rustc_direct_passthrough_with_event(
    config: &Config,
    args: &RustcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: &str,
) -> Result<i32> {
    if PRECOMPILED_EXIT.with(std::cell::Cell::get).is_some() {
        return passthrough_with_event(config, args, crate_name, root, start, reason);
    }
    let output = passthrough(args, None, config.preserve_incremental)?;
    log_passthrough_event(
        config,
        root,
        crate_name,
        start.elapsed().as_millis() as u64,
        reason.to_string(),
        &output,
    );
    Ok(output.exit_code)
}

/// Run the explicit preserve-incremental lane directly. Kache owns this
/// compiler strategy; ordinary rejected invocations still use the configured
/// fallback pipeline.
fn preserved_incremental_with_event(
    config: &Config,
    args: &RustcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
) -> Result<i32> {
    let output = passthrough(args, None, true)?;
    log_passthrough_event(
        config,
        root,
        crate_name,
        start.elapsed().as_millis() as u64,
        "incremental preserved".to_string(),
        &output,
    );
    Ok(output.exit_code)
}

/// A deferred C compile: run the compiler with dependency capture, then key
/// and store through the ordinary path with the result in hand.
fn cc_compile_before_key(
    config: &Config,
    invocation: &CcStoreInvocation,
    file_hasher: &crate::cache_key::FileHasher<'_>,
    flight: Option<crate::store::StoreLock>,
) -> Result<i32> {
    let CcStoreInvocation {
        compiler,
        parsed,
        crate_name,
        event_root,
        start,
        ..
    } = invocation;
    let start = *start;
    tracing::debug!("no read-set memo for {crate_name}; compiling before keying");
    let compile_start = std::time::Instant::now();
    let (result, inputs) = match compiler.execute_capturing_inputs(parsed, file_hasher) {
        Ok(pair) => pair,
        Err(e) => {
            return cc_passthrough_with_event(
                config,
                parsed,
                crate_name,
                event_root,
                start,
                format!("compiler spawn failed: {e}"),
            );
        }
    };
    let compile_time_ms = compile_start.elapsed().as_millis() as u64;
    replay_diagnostics(
        &result.stdout,
        result.pending_stderr(),
        std::io::stdout(),
        std::io::stderr(),
    );
    if result.exit_code != 0 {
        let elapsed = start.elapsed().as_millis() as u64;
        log_event_with_hash_stats(
            config,
            event_root,
            crate_name,
            EventResult::Error,
            elapsed,
            compile_time_ms,
            0,
            "",
            0,
            FileHashStats::default(),
            0,
            0,
            0,
        );
        print_progress(crate_name, EventResult::Error, elapsed, 0);
        return Ok(result.exit_code);
    }
    let exit_code = result.exit_code;
    if inputs.is_none() {
        return cc_precompiled_skipped(
            config,
            crate_name,
            event_root,
            start,
            "the compile left no usable read set".to_string(),
            exit_code,
        );
    }
    CC_PRECOMPILED_EXIT.with(|cell| cell.set(Some(exit_code)));
    let stored = run_cc_with_store(
        config,
        invocation,
        Some(CcPrecompiled {
            result,
            compile_time_ms,
            inputs,
            flight,
        }),
    );
    CC_PRECOMPILED_EXIT.with(|cell| cell.set(None));
    stored.or(Ok(exit_code))
}

/// The compile ran; whatever stopped the store, its exit code stands.
fn cc_precompiled_skipped(
    config: &Config,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: String,
    exit_code: i32,
) -> Result<i32> {
    tracing::debug!("{crate_name}: compiled, not stored: {reason}");
    let elapsed = start.elapsed().as_millis() as u64;
    log_event_with_hash_stats(
        config,
        root,
        crate_name,
        EventResult::Skipped,
        elapsed,
        0,
        0,
        "",
        0,
        FileHashStats::default(),
        0,
        0,
        0,
    );
    print_progress(crate_name, EventResult::Skipped, elapsed, 0);
    Ok(exit_code)
}

fn cc_passthrough_with_event<R: Into<String>>(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    if let Some(exit_code) = CC_PRECOMPILED_EXIT.with(std::cell::Cell::get) {
        return cc_precompiled_skipped(config, crate_name, root, start, reason.into(), exit_code);
    }
    let output = cc_passthrough(config, parsed)?;
    log_passthrough_event(
        config,
        root,
        crate_name,
        start.elapsed().as_millis() as u64,
        reason.into(),
        &output,
    );
    Ok(output.exit_code)
}

fn cc_direct_passthrough_with_event<R: Into<String>>(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    if let Some(exit_code) = CC_PRECOMPILED_EXIT.with(std::cell::Cell::get) {
        return cc_precompiled_skipped(config, crate_name, root, start, reason.into(), exit_code);
    }
    let output = cc_direct_passthrough(config, parsed)?;
    log_passthrough_event(
        config,
        root,
        crate_name,
        start.elapsed().as_millis() as u64,
        reason.into(),
        &output,
    );
    Ok(output.exit_code)
}

#[allow(clippy::too_many_arguments)]
fn log_event_with_hash_stats(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
) {
    log_event_with_store_stats(
        config,
        root,
        crate_name,
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        restore_ms,
        store_ms,
        StorePutResult::default(),
    );
}

/// Render a failed `Store::put` for the event log and the report.
///
/// `{:#}` keeps anyhow's whole context chain — the outer context alone
/// ("creating blob shard directory") never names the cause. Two guards, because
/// unlike the `WARN` this string is persisted and re-rendered inside JSON, a
/// text table and a markdown table:
/// - control characters (a newline from a nested compiler error) become spaces,
///   so one failure cannot break the row it is printed in;
/// - the result is capped, so a pathological error message cannot bloat every
///   event line in the log.
///
/// Hardening for shape, not secrecy: it does not redact. The reason is derived
/// from filesystem and SQLite errors, so it can carry absolute paths, and a
/// report shared outside the machine carries them too.
pub(crate) fn store_error_for_event(error: &anyhow::Error) -> String {
    const MAX_CHARS: usize = 2048;

    let rendered = format!("{error:#}");
    let mut chars = rendered.chars();
    let mut bounded: String = chars
        .by_ref()
        .take(MAX_CHARS)
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect();
    if chars.next().is_some() {
        bounded.push_str("… [truncated]");
    }
    bounded
}

#[allow(clippy::too_many_arguments)]
fn log_event_with_store_stats(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
    store_put: StorePutResult,
) {
    let _trace = crate::phase_trace::phase("event_report");
    log_event_with_store_outcome(
        config,
        root,
        crate_name,
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        restore_ms,
        store_ms,
        store_put,
        String::new(),
    );
}

/// Like [`log_event_with_store_stats`], but carries the reason `Store::put`
/// failed so the compile is recorded as the *repeating* miss it is
/// (kunobi-ninja/kache#629). `store_error` is empty on the normal path.
#[allow(clippy::too_many_arguments)]
fn log_event_with_store_outcome(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
    store_put: StorePutResult,
    store_error: String,
) {
    log_event_with_store_and_lookup_outcome(
        config,
        root,
        crate_name,
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        restore_ms,
        store_ms,
        store_put,
        store_error,
        String::new(),
    );
}

/// Like [`log_event_with_store_outcome`], but records why an exact-key cache
/// entry was rejected before the replacement compile (kunobi-ninja/kache#655).
#[allow(clippy::too_many_arguments)]
fn log_event_with_store_and_lookup_outcome(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
    store_put: StorePutResult,
    store_error: String,
    lookup_rejection: String,
) {
    log_event_details(
        config,
        root,
        crate_name,
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        restore_ms,
        store_ms,
        store_put,
        String::new(),
        store_error,
        lookup_rejection,
        false,
        None,
        None,
    );
}

fn log_passthrough_event(
    config: &Config,
    root: &str,
    crate_name: &str,
    elapsed_ms: u64,
    reason: String,
    output: &PassthroughOutput,
) {
    log_event_details(
        config,
        root,
        crate_name,
        EventResult::Passthrough,
        elapsed_ms,
        0,
        0,
        "",
        0,
        FileHashStats::default(),
        0,
        0,
        0,
        StorePutResult::default(),
        reason,
        String::new(),
        String::new(),
        output.fallback,
        Some(output.exit_code),
        output.fallback_attempt.clone(),
    );
}

#[allow(clippy::too_many_arguments)]
fn log_event_details(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
    store_put: StorePutResult,
    passthrough_reason: String,
    store_error: String,
    lookup_rejection: String,
    fallback: bool,
    exit_code: Option<i32>,
    fallback_attempt: Option<crate::fallback::Attempt>,
) {
    let event = build_event_details(
        config,
        root,
        crate_name,
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        restore_ms,
        store_ms,
        store_put,
        passthrough_reason,
        store_error,
        lookup_rejection,
        fallback,
        exit_code,
        fallback_attempt,
    );
    write_event(config, &event);
}

/// Append `event` to the event log and rotate the logs. Best-effort: nothing
/// here may fail a build.
pub(crate) fn write_event(config: &Config, event: &BuildEvent) {
    let _trace = crate::phase_trace::phase("event_log");
    let _ = events::log_event(&config.event_log_path(), event);
    let _ = events::rotate_if_needed(
        &config.event_log_path(),
        config.event_log_max_size,
        config.event_log_keep_lines,
    );
    let _ = events::rotate_transfers_if_needed(
        &config.transfer_log_path(),
        config.event_log_max_size,
        config.event_log_keep_lines,
    );
}

/// The event for one invocation, built from its measurements and this
/// process's counters. Written by [`write_event`], here or, for a compile
/// handed to the daemon, there.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_event_details(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
    store_put: StorePutResult,
    passthrough_reason: String,
    store_error: String,
    lookup_rejection: String,
    fallback: bool,
    exit_code: Option<i32>,
    fallback_attempt: Option<crate::fallback::Attempt>,
) -> BuildEvent {
    // Session attribution (#583 P0.5): join or open the root's build session
    // and refresh the marker so the 5-minute window measures inactivity. Both
    // are best-effort; an empty id only means the marker was unusable.
    let session_id = session_id_for_event(
        config,
        root,
        invocation_started_secs(now_epoch_secs(), elapsed_ms),
    );
    refresh_session_marker(config, root, &session_id);

    // Per-group key digests of this compile's key computation (empty for cc /
    // passthrough). Consumed here, at the single write site, so no signature
    // threading (kunobi-ninja/kache#131).
    let key_fields = crate::cache_key::take_last_key_fields().unwrap_or_default();
    // Always consumed, so the stash never leaks into a later compile in this
    // process; persisted only under `explain_miss` (#609). Unlike `key_diff`,
    // this rides HITS too — the chain walk diffs a miss against the last hit,
    // so a hit with no recorded externs leaves nothing to diff against.
    // `Some(map)` means a rustc key was computed for this compile, even when
    // the map is empty (a crate with no dependencies) — which the cascade walk
    // must be able to tell apart from "not recorded". Persisted only under
    // `explain_miss`.
    let recorded_externs = crate::cache_key::take_last_key_externs();
    let key_externs_recorded = config.explain_miss && recorded_externs.is_some();
    let key_externs = if key_externs_recorded {
        recorded_externs.unwrap_or_default()
    } else {
        Default::default()
    };
    // Unit identities ride the same stash-and-gate as the digests they explain
    // (kunobi-ninja/kache#627): taken unconditionally so nothing leaks into the
    // next compile in this process, persisted only under `explain_miss`, and
    // only together with `key_externs` — a unit id with no digests to join is
    // dead weight on the wire.
    let recorded_extern_units = crate::cache_key::take_last_key_extern_units();
    let recorded_unit_id = crate::cache_key::take_last_key_unit_id();
    let (unit_id, extern_units) = if key_externs_recorded {
        (
            recorded_unit_id.unwrap_or_default(),
            recorded_extern_units.unwrap_or_default(),
        )
    } else {
        (String::new(), Default::default())
    };
    let key_diff = explain_miss_diff(config, root, crate_name, result, cache_key, &key_fields);
    BuildEvent {
        ts: Utc::now(),
        crate_name: crate_name.to_string(),
        root: root.to_string(),
        version: crate::VERSION.to_string(),
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key: cache_key.to_string(),
        schema: 21,
        demands: crate::demand::take(),
        session_id,
        key_ms,
        key_hash_hits: key_hash_stats.cache_hits,
        key_hash_misses: key_hash_stats.cache_misses,
        key_hash_bytes: key_hash_stats.bytes_hashed,
        lookup_ms,
        restore_ms,
        store_ms,
        // Phases measured outside the wrapper's own timers (schema 17): the
        // process-global accumulators, read here like the op-counters below.
        startup_ms: crate::opcounts::startup_ms(),
        dep_info_ms: crate::opcounts::dep_info_ms(),
        dep_info_runs: crate::opcounts::dep_info_runs(),
        prediction_mismatches: u32::try_from(crate::opcounts::prediction_mismatches())
            .unwrap_or(u32::MAX),
        flight_wait_ms: crate::opcounts::flight_wait_ms(),
        permit_wait_ms: crate::opcounts::permit_wait_ms(),
        store_output_blobs: store_put.output_blobs,
        store_duplicate_blobs: store_put.duplicate_blobs,
        store_new_blobs: store_put.new_blobs,
        // Read the process-global op-counters: this `kache` process
        // handled exactly this one compile, so the counts are its own.
        compiler_runs: crate::opcounts::compiler_runs(),
        preprocessor_runs: crate::opcounts::preprocessor_runs(),
        probe_runs: crate::opcounts::probe_runs(),
        reflinked_bytes: crate::opcounts::reflinked_bytes(),
        hardlinked_bytes: crate::opcounts::hardlinked_bytes(),
        copied_bytes: crate::opcounts::copied_bytes(),
        store_reflinked_bytes: crate::opcounts::store_reflinked_bytes(),
        store_hardlinked_bytes: crate::opcounts::store_hardlinked_bytes(),
        store_copied_bytes: crate::opcounts::store_copied_bytes(),
        store_copy_cross_device_bytes: crate::opcounts::store_copy_cross_device_bytes(),
        store_copy_permission_bytes: crate::opcounts::store_copy_permission_bytes(),
        store_copy_ineligible_bytes: crate::opcounts::store_copy_ineligible_bytes(),
        store_copy_other_bytes: crate::opcounts::store_copy_other_bytes(),
        restore_copy_cross_device_bytes: crate::opcounts::restore_copy_cross_device_bytes(),
        restore_copy_permission_bytes: crate::opcounts::restore_copy_permission_bytes(),
        restore_copy_exclusive_bytes: crate::opcounts::restore_copy_exclusive_bytes(),
        restore_copy_other_bytes: crate::opcounts::restore_copy_other_bytes(),
        passthrough_reason,
        store_error,
        store_handed_off: false,
        daemon_store_ms: 0,
        lookup_rejection,
        verify_compare: crate::verify_compare::take_last_report(),
        fallback,
        fallback_attempt,
        exit_code,
        key_fields,
        key_diff,
        key_externs,
        key_externs_recorded,
        unit_id,
        extern_units,
    }
}

/// `[cache] explain_miss` (kunobi-ninja/kache#131): on a miss for a crate
/// that previously HIT in this build tree, name the key input groups whose
/// digests changed — turning "kache misses more than I expect" into "field X
/// changed". Costs one event-log read per miss, which is why it's opt-in;
/// returns empty (and reads nothing) when disabled, on non-miss results, or
/// when this compile produced no group digests (cc path).
/// Caveat (documented, not fixed): the last-hit baseline matches on
/// `crate_name + root`, which conflates duplicate crate versions and
/// host-vs-target units of the same crate — the named groups are then
/// approximate. Precise unit identity would need the metadata hash, which is
/// deliberately not keyed. Acceptable for an opt-in diagnostic.
fn explain_miss_diff(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    cache_key: &str,
    key_fields: &std::collections::BTreeMap<String, String>,
) -> Vec<String> {
    if !config.explain_miss
        || !matches!(result, EventResult::Miss | EventResult::Dup)
        || key_fields.is_empty()
    {
        return Vec::new();
    }
    let events = match events::read_events(&config.event_log_path()) {
        Ok(events) => events,
        Err(_) => return Vec::new(),
    };
    let Some(last_hit) = events.iter().rev().find(|e| {
        e.crate_name == crate_name
            && e.root == root
            && !e.key_fields.is_empty()
            && matches!(
                e.result,
                EventResult::LocalHit | EventResult::PrefetchHit | EventResult::RemoteHit
            )
    }) else {
        return Vec::new();
    };
    // Same final key as the last hit: nothing changed — the entry was
    // evicted (GC, size pressure) or the store was cleared. Without this
    // check an identical-fields diff would mislabel the miss as
    // `salt_or_extra_inputs` (cross-family review finding).
    if last_hit.cache_key == cache_key {
        crate::notice::show(&format!(
            "[kache] miss: crate {crate_name} (key unchanged since last hit —              entry evicted or store cleared?)"
        ));
        return vec!["none:entry-evicted".to_string()];
    }
    let mut changed: Vec<String> = key_fields
        .iter()
        .filter(|(group, digest)| last_hit.key_fields.get(*group) != Some(digest))
        .map(|(group, _)| group.clone())
        .collect();
    // A group present only in the OLD event also counts as a change.
    changed.extend(
        last_hit
            .key_fields
            .keys()
            .filter(|g| !key_fields.contains_key(*g))
            .cloned(),
    );
    changed.sort();
    changed.dedup();
    if changed.is_empty() {
        // Final keys differ but no traced group does: the difference sits in
        // the post-hoc folds (key salt / extra inputs).
        changed.push("salt_or_extra_inputs".to_string());
    }
    let ago = Utc::now()
        .signed_duration_since(last_hit.ts)
        .num_minutes()
        .max(0);
    crate::notice::show(&format!(
        "[kache] miss: crate {} (last hit {}m ago; key changed in: {})",
        crate_name,
        ago,
        changed.join(", ")
    ));
    changed
}

/// Send the daemon a prefetch hint once per build session.
///
/// The session itself comes from [`session_id_for_event`], so it exists with
/// or without a remote. A separate `.prefetch` marker records which session
/// the hint went out for, and a flock on it keeps N parallel rustc
/// invocations from all sending one. The marker is written only after a
/// successful discovery, so a failed attempt (cargo metadata hanging on a git
/// dependency, say) is retried by the next compile of the same build.
fn maybe_trigger_prefetch(config: &Config, args: &RustcArgs) {
    if config.remote.is_none() {
        return;
    }
    let root = rustc_event_root(args);
    let session_id = session_id_for_event(config, &root, now_epoch_secs());
    if session_id.is_empty() {
        return;
    }
    let marker = prefetch_marker_path(config, &root);
    if prefetch_marker_names(
        &std::fs::read_to_string(&marker).unwrap_or_default(),
        &session_id,
    ) {
        return;
    }
    let Some(lock_file) = open_marker_for_lock(&marker) else {
        return;
    };
    // std::fs::File::try_lock (1.89+) is cross-platform: flock(2) on Unix,
    // LockFileEx on Windows. Lock auto-releases when `lock_file` is dropped.
    if lock_file.try_lock().is_err() {
        return; // Another wrapper is already sending the prefetch hint
    }
    // Re-check through the locked handle: another process may have sent the
    // hint between our first read and acquiring the lock, and on Windows the
    // lock blocks reads from any other handle (#348).
    if prefetch_marker_names(&read_locked_marker(&lock_file), &session_id) {
        return;
    }

    // Gather ALL dependency crate names in compilation order (leaves first).
    // This gives the daemon a comprehensive prefetch list that works even on
    // cold CI runners where the local SQLite store is empty.
    let build_intent = match crate::build_intent::discover(Some(args)) {
        Some(intent) => intent,
        _ => return,
    };

    let shard_prefetch_enabled =
        build_intent.namespace.is_some() && !build_intent.cargo_lock_deps.is_empty();

    tracing::info!(
        "build session detected, sending prefetch hint for {} crates (shard context: {})",
        build_intent.crate_names.len(),
        if shard_prefetch_enabled {
            "available"
        } else {
            "fallback"
        }
    );

    crate::daemon::send_build_started(
        config,
        crate::build_intent::into_build_started_request(
            build_intent,
            crate::daemon::build_epoch(),
            session_id.clone(),
        ),
    );
    write_locked_marker(&lock_file, &session_id);
}

/// Where [`maybe_trigger_prefetch`] records the session it sent a hint for.
fn prefetch_marker_path(config: &Config, root: &str) -> PathBuf {
    session_marker_path(config, root).with_extension("prefetch")
}

fn prefetch_marker_names(content: &str, session_id: &str) -> bool {
    content.trim() == session_id
}

fn read_locked_marker(mut file: &std::fs::File) -> String {
    use std::io::{Read, Seek, SeekFrom};
    let mut content = String::new();
    if file.seek(SeekFrom::Start(0)).is_ok() {
        let _ = file.read_to_string(&mut content);
    }
    content
}

/// Replace a marker's content through the handle that owns its lock, the
/// only handle Windows lets write to it (#348).
fn write_locked_marker(mut file: &std::fs::File, content: &str) {
    use std::io::{Seek, SeekFrom, Write};
    let _ = file.set_len(0);
    let _ = file.seek(SeekFrom::Start(0));
    let _ = file.write_all(content.as_bytes());
    let _ = file.flush();
}

/// Check if the marker file contains a timestamp within `timeout_secs` of now.
/// Returns `false` if the marker does not exist, contains a stale/corrupt
/// timestamp, or is a symlink/non-regular file.
/// Root-scoped session-marker path: `.build-sessions/<hash(root)>` under the
/// runtime dir (kunobi-ninja/kache#583 P0.5).
///
/// Scoping by build root (not one cache-global `.build-session`) stops
/// parallel repositories sharing a cache dir from suppressing each other's
/// prefetch plans. The legacy `.build-session` file is left alone: old
/// wrappers keep using it independently; the worst mixed-fleet outcome is a
/// redundant BuildStarted, which the daemon coalesces.
pub(crate) fn session_marker_path(config: &Config, root: &str) -> std::path::PathBuf {
    let hash = blake3::hash(root.as_bytes()).to_hex();
    config
        .runtime_dir
        .join(".build-sessions")
        .join(&hash.as_str()[..16])
}

/// The build session an invocation of `root` that started at
/// `started_secs` belongs to, opening a new one when the root has none.
/// Best-effort by design: session attribution must never fail a build, so an
/// unusable marker yields an empty id.
///
/// Every compile, hit, and passthrough goes through here, with or without a
/// remote. Sessions used to be opened only by the remote prefetch trigger,
/// so a local-only cache recorded no session ids at all (#1081).
///
/// A session is open for `started_secs` when its marker was touched within
/// the inactivity window before that moment. Judging at the invocation's
/// start, not when it logs, keeps a crate that compiles for longer than the
/// window (LLVM-sized) inside its build. The marker is re-read under an
/// exclusive lock before minting, so parallel compiles of one build agree
/// on a single id.
pub(crate) fn session_id_for_event(config: &Config, root: &str, started_secs: u64) -> String {
    if root.is_empty() {
        return String::new();
    }
    let marker = session_marker_path(config, root);
    if let Some(id) = open_session_id(
        &std::fs::read_to_string(&marker).unwrap_or_default(),
        started_secs,
    ) {
        return id;
    }
    if let Some(parent) = marker.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Some(lock_file) = open_marker_for_lock(&marker) else {
        return String::new();
    };
    if lock_file.lock().is_err() {
        return String::new();
    }
    if let Some(id) = open_session_id(&read_locked_marker(&lock_file), started_secs) {
        return id;
    }
    let id = mint_session_id(root);
    write_session_marker(&lock_file, &id);
    id
}

/// When an invocation that has run for `elapsed_ms` started, in epoch seconds.
fn invocation_started_secs(now_secs: u64, elapsed_ms: u64) -> u64 {
    now_secs.saturating_sub(elapsed_ms / 1000)
}

/// The session id in marker `content` when that session was still open at
/// `at_secs`.
fn open_session_id(content: &str, at_secs: u64) -> Option<String> {
    let (touched, id) = parse_session_marker(content)?;
    (!id.is_empty() && timestamp_is_fresh_at(touched, BUILD_SESSION_SECS, at_secs)).then_some(id)
}

/// Refresh the session marker's timestamp so the 5-minute window measures
/// INACTIVITY, not age since the first crate — a long build must not have its
/// session expire mid-way.
///
/// Atomic replace (write temp + rename), not truncate-in-place: readers must
/// never observe an empty/partial marker (cross-family review finding), and
/// rename is best-effort on Windows where the destination may be locked by a
/// concurrent trigger. Guarded on the id still matching — if a newer build
/// re-minted the marker between our read and this refresh, we must not
/// resurrect the old session over it.
pub(crate) fn refresh_session_marker(config: &Config, root: &str, session_id: &str) {
    if root.is_empty() || session_id.is_empty() {
        return;
    }
    let marker = session_marker_path(config, root);
    match std::fs::read_to_string(&marker) {
        Ok(content) => match parse_session_marker(&content) {
            Some((_, id)) if id == session_id => {}
            _ => return, // superseded or unreadable — never clobber
        },
        Err(_) => return,
    }
    let tmp = marker.with_extension(format!("tmp.{}", std::process::id()));
    let record = format!("v1 {} {}", now_epoch_secs(), session_id);
    if std::fs::write(&tmp, record).is_err() {
        return;
    }
    if std::fs::rename(&tmp, &marker).is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
}

/// Write a `v1 <now> <session_id>` record through the caller's locked handle
/// (same Windows mandatory-lock rationale as [`write_marker_timestamp`]).
fn write_session_marker(mut file: &std::fs::File, session_id: &str) {
    use std::io::{Seek, SeekFrom, Write};
    let record = format!("v1 {} {}", now_epoch_secs(), session_id);
    let _ = file.set_len(0);
    let _ = file.seek(SeekFrom::Start(0));
    let _ = file.write_all(record.as_bytes());
    let _ = file.flush();
}

/// Mint a new session id: hex(blake3(root, pid, nanos, seq))[..16]. Opaque and
/// dependency-free; uniqueness only needs to hold per cache dir per window.
///
/// `seq` is what makes two ids from one process distinct. `nanos` alone is not:
/// the clock's real resolution can be coarser than the gap between two
/// back-to-back calls, so `SystemTime::now()` returns the same value twice and
/// the digests collide. That is rare on a fast bare-metal host and routine in a
/// build sandbox or a loaded VM, which is why it surfaced as a flaky test in
/// Nix builds (#756) rather than in CI.
fn mint_session_id(root: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut hasher = blake3::Hasher::new();
    hasher.update(root.as_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    hasher.update(&nanos.to_le_bytes());
    hasher.update(&seq.to_le_bytes());
    hasher.finalize().to_hex().as_str()[..16].to_string()
}

/// How long GC keeps a session or prefetch marker after its last touch.
/// Sessions close after [`BUILD_SESSION_SECS`] idle; a day leaves room for a
/// compile that runs for hours.
pub(crate) const SESSION_MARKER_RETENTION: std::time::Duration =
    std::time::Duration::from_secs(86_400);

/// Remove session and prefetch markers untouched for at least `retention`,
/// returning how many were removed. Every event root gets a marker (#1081),
/// so a machine that builds many trees would otherwise collect them in the
/// runtime dir forever.
pub(crate) fn prune_session_markers(
    config: &Config,
    retention: std::time::Duration,
    now: std::time::SystemTime,
) -> usize {
    let Ok(entries) = std::fs::read_dir(config.runtime_dir.join(".build-sessions")) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .filter(|entry| {
            entry.file_type().is_ok_and(|kind| kind.is_file())
                && entry
                    .metadata()
                    .and_then(|meta| meta.modified())
                    .is_ok_and(|touched| {
                        now.duration_since(touched)
                            .is_ok_and(|age| age >= retention)
                    })
        })
        .filter(|entry| std::fs::remove_file(entry.path()).is_ok())
        .count()
}

/// The build-session inactivity window (shared by trigger + attribution).
pub(crate) const BUILD_SESSION_SECS: u64 = 300;

/// Remove the incremental compilation directory for this crate.
/// With kache caching, incremental compilation is redundant and the dirs waste disk space.
fn clean_incremental_dir(config: &Config, args: &RustcArgs) {
    if incremental_cleanup_enabled(config)
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
}

#[cfg(test)]
mod tests {
    mod hit;
    mod rustc_hit;

    use super::*;
    use crate::cache_key::FileHasher;
    use crate::transport::{ListenerOptions, socket_name};
    use std::ffi::OsString;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct TestEnvGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl TestEnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, previous }
        }

        fn remove(key: &'static str) -> Self {
            let previous = std::env::var_os(key);
            unsafe {
                std::env::remove_var(key);
            }
            Self { key, previous }
        }
    }

    impl Drop for TestEnvGuard {
        fn drop(&mut self) {
            unsafe {
                match &self.previous {
                    Some(value) => std::env::set_var(self.key, value),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| (*arg).to_string()).collect()
    }

    fn rustc_args(args: &[&str]) -> RustcArgs {
        RustcCompiler::new().parse(&s(args)).unwrap()
    }

    /// The rule that keeps a prediction from ever being an authority.
    ///
    /// Reached only once both lookups have missed, so it is not asking "did we
    /// find it" — a hit has already returned. It asks whether this key is
    /// still a guess, and a guess may not claim or store.
    #[test]
    fn a_predicted_key_owes_a_rederivation_until_it_has_had_one() {
        assert!(
            owes_rederivation(true, false),
            "a key that came from a record and matched nothing is still a guess"
        );
        assert!(
            !owes_rederivation(true, true),
            "one re-derivation is enough; a second would be the same pre-pass"
        );
        assert!(
            !owes_rederivation(false, false),
            "a key that was never predicted is already the discovered one"
        );
        assert!(!owes_rederivation(false, true));
    }

    #[test]
    fn only_a_fresh_closure_is_worth_recording() {
        assert!(
            should_record_closure(false, false),
            "a closure discovered by the pre-pass is what the record is for"
        );
        assert!(
            !should_record_closure(true, false),
            "a closure that CAME from the record is already in it; rewriting \
             it would be a database write per compile for nothing"
        );
        assert!(
            should_record_closure(true, true),
            "a re-derivation is what repairs a stale row"
        );
        assert!(should_record_closure(false, true));
    }

    /// Recording is opt-in and needs somewhere to record. Neither refusal may
    /// leave the discovered closure sitting in the thread-local stash, where
    /// the next key computed on this thread would find it and record another
    /// unit's inputs under its own identity.
    #[test]
    fn input_predictions_record_only_when_enabled_and_backed_by_a_store() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "demo"]);
        let closure = crate::cache_key::DepInfo {
            source_files: vec![std::path::PathBuf::from("src/lib.rs")],
            env_deps: Vec::new(),
        };
        let stash = || crate::cache_key::stash_last_dep_info_for_test(closure.clone());
        let identity = crate::cache_key::rustc_prediction_identity(&args)
            .expect("an invocation with a crate root has an identity");
        let recorded = |store: &Store| store.file_hasher().input_prediction(&identity).is_some();

        // Off by default, which is the state every user is in today.
        assert!(!config.input_predictions);
        stash();
        record_input_prediction(&config, Some(&store), &args, true);
        assert!(
            !recorded(&store),
            "the feature is off; nothing may be written"
        );
        assert!(
            crate::cache_key::take_last_dep_info().is_none(),
            "a declined recording must still clear the stash"
        );

        // On, but with no store to record into: the daemon's store-free path.
        config.input_predictions = true;
        stash();
        record_input_prediction(&config, None, &args, true);
        assert!(!recorded(&store));
        assert!(crate::cache_key::take_last_dep_info().is_none());

        // On, with a store, and a closure to record.
        stash();
        record_input_prediction(&config, Some(&store), &args, true);
        assert!(
            recorded(&store),
            "an enabled build with a store must remember what it discovered"
        );
        let record = store.file_hasher().input_prediction(&identity).unwrap();
        assert_eq!(record.sources, closure.source_files);

        // And with nothing in the stash there is nothing to record: an
        // invocation that never ran a pre-pass must not write an empty closure
        // over a good one.
        record_input_prediction(&config, Some(&store), &args, true);
        assert_eq!(
            store
                .file_hasher()
                .input_prediction(&identity)
                .unwrap()
                .sources,
            closure.source_files,
            "a recording with no closure must leave the existing one alone"
        );
    }

    /// A registry unit whose closure reads its own OUT_DIR cannot have the
    /// shared row, so it gets the relocated one; without such a source it
    /// gets the shared row.
    #[test]
    fn a_registry_unit_reading_its_out_dir_records_a_relocated_row() {
        if std::process::Command::new("rustc")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: no rustc");
            return;
        }
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.input_predictions = true;
        let store = Store::open(&config).unwrap();
        let package = dir.path().join("home/registry/src/index-1/kt-1.0.0");
        std::fs::create_dir_all(package.join("src")).unwrap();
        let target = dir.path().join("a/target");
        let out = target.join("debug/build/kt-1/out");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("gen.rs"), "pub fn g() {}\n").unwrap();
        let _manifest =
            crate::config::tests::set_env_for_test("CARGO_MANIFEST_DIR", Some(package.as_os_str()));
        let _out = crate::config::tests::set_env_for_test("OUT_DIR", Some(out.as_os_str()));
        let lib = package.join("src/lib.rs");
        let args = rustc_args(&[
            "rustc",
            "--crate-name",
            "kt",
            lib.to_str().unwrap(),
            "--out-dir",
            target.join("debug/deps").to_str().unwrap(),
        ]);
        let shared = crate::cache_key::rustc_shared_prediction_identity(&args)
            .expect("a registry unit with an absolute target has a shared identity");
        let reads_out_dir = crate::cache_key::DepInfo {
            source_files: vec![lib.clone(), out.join("gen.rs")],
            env_deps: Vec::new(),
        };
        let (relocatable, _) =
            crate::cache_key::relocatable_record(&args, &reads_out_dir, Some("tree")).unwrap();

        crate::cache_key::stash_last_dep_info_for_test(reads_out_dir);
        crate::cache_key::stash_last_tree_digest_for_test("tree");
        record_input_prediction(&config, Some(&store), &args, true);
        let hasher = store.file_hasher();
        assert!(hasher.input_prediction(&shared).is_none());
        let record = hasher.portable_prediction(&relocatable).unwrap();
        assert_eq!(record.tree, "tree");

        let package_only = crate::cache_key::DepInfo {
            source_files: vec![lib],
            env_deps: Vec::new(),
        };
        crate::cache_key::stash_last_dep_info_for_test(package_only.clone());
        crate::cache_key::stash_last_tree_digest_for_test("tree-2");
        record_input_prediction(&config, Some(&store), &args, true);
        assert_eq!(
            hasher.input_prediction(&shared).unwrap().sources,
            package_only.source_files
        );
    }

    fn eligible_incremental_args(temp: &tempfile::TempDir, crate_name: &str) -> RustcArgs {
        let profile = temp.path().join("target/debug");
        let out_dir = profile.join("deps");
        let incremental = profile.join("incremental");
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::create_dir_all(&incremental).unwrap();
        RustcCompiler::new()
            .parse(&[
                "rustc".to_string(),
                "--crate-name".to_string(),
                crate_name.to_string(),
                temp.path()
                    .join("src/lib.rs")
                    .to_string_lossy()
                    .into_owned(),
                "--out-dir".to_string(),
                out_dir.to_string_lossy().into_owned(),
                "-C".to_string(),
                format!("incremental={}", incremental.display()),
                "-Cextra-filename=-1234abcd".to_string(),
            ])
            .unwrap()
    }

    #[test]
    fn store_unavailable_message_is_actionable() {
        let err = anyhow::anyhow!("disk I/O error")
            .context("opening index database /mnt/c/Users/x/kache/index.db");
        let msg = store_unavailable_message(&err);

        // Surfaces the real underlying error so users can diagnose.
        assert!(msg.contains("disk I/O error"), "msg = {msg}");
        // States the impact plainly.
        assert!(
            msg.to_lowercase().contains("caching is disabled"),
            "msg = {msg}"
        );
        // Points at the general cause (locking / multi-machine) — not anything
        // specific to containers/cross/podman.
        assert!(
            msg.contains("locking") || msg.contains("more than one machine"),
            "msg = {msg}"
        );
        // Gives the actionable remediation.
        assert!(msg.contains("KACHE_CACHE_DIR"), "msg = {msg}");
        // Reassures the build still succeeds.
        assert!(
            msg.contains("uncached") || msg.contains("succeeds"),
            "msg = {msg}"
        );
        // Stays generic: must NOT name the specific reporter's environment.
        assert!(!msg.to_lowercase().contains("podman"), "msg = {msg}");
        assert!(!msg.to_lowercase().contains("container"), "msg = {msg}");
    }

    #[test]
    fn store_warn_marker_is_local_and_keyed_by_cache_dir() {
        let a = warn_marker_path("store", Path::new("/mnt/c/Users/x/kache"));
        let b = warn_marker_path("store", Path::new("/home/y/.cache/kache"));
        let tmp = std::env::temp_dir();

        // Lives in the OS temp dir (local), NOT under the (possibly broken)
        // cache dir — the whole point is that the cache mount can't be relied
        // on for locking, so the dedup marker must not live there.
        assert!(a.starts_with(&tmp), "marker {a:?} not under temp {tmp:?}");
        assert!(
            !a.starts_with("/mnt/c"),
            "marker must not live on the cache mount: {a:?}"
        );
        // Distinct cache dirs get distinct markers (independent dedup).
        assert_ne!(a, b);
        // Same cache dir is stable across calls, so the 300+ parallel wrapper
        // processes all agree on one marker and only one of them warns.
        assert_eq!(
            a,
            warn_marker_path("store", Path::new("/mnt/c/Users/x/kache"))
        );
    }

    #[test]
    fn store_unavailable_warning_dedups_within_session() {
        // Unique synthetic cache dir so this test's marker can't collide with
        // other tests running in the same binary. The dir need not exist — the
        // warning only ever touches the local marker, never the cache dir.
        let cache_dir = PathBuf::from("/nonexistent/kache-469-dedup-test-cache-dir-7f3a2b1c");
        let marker = warn_marker_path("store", &cache_dir);
        let _ = std::fs::remove_file(&marker);
        assert!(
            !marker_is_fresh(&marker, 300),
            "precondition: no marker yet"
        );

        let cfg = test_config(cache_dir);
        let err = anyhow::anyhow!("disk I/O error");

        warn_store_unavailable_once(&cfg, &err);

        // After the first warning the marker is fresh, so the remaining parallel
        // wrappers in the same session stay silent.
        assert!(
            marker_is_fresh(&marker, 300),
            "marker should be fresh after the first warning"
        );

        let _ = std::fs::remove_file(&marker);
    }

    #[test]
    #[cfg(unix)]
    fn maybe_trigger_prefetch_refuses_symlinked_build_session() {
        let temp = tempfile::TempDir::new().unwrap();
        let target = temp.path().join("target_file");
        std::fs::write(&target, "target content").unwrap();

        let marker = temp.path().join(".build-session");
        std::os::unix::fs::symlink(&target, &marker).unwrap();

        let mut config = test_config(temp.path().to_path_buf());
        // Enable remote so prefetch actually triggers its path
        config.remote = Some(crate::config::RemoteConfig::test_s3(
            "test-bucket",
            "kache/",
        ));

        // Use dummy args
        let args = rustc_args(&["rustc", "foo.rs"]);

        // This must NOT modify the target file
        super::maybe_trigger_prefetch(&config, &args);

        // Verify target file remains completely untouched
        let content = std::fs::read_to_string(&target).unwrap();
        assert_eq!(content, "target content");
    }

    // ── Opportunistic size-pressure GC (kunobi-ninja/kache#497) ─────────────

    #[test]
    fn volume_route_path_rustc_prefers_out_dir_then_output_parent() {
        let mut args = rustc_args(&["rustc", "foo.rs"]);
        args.out_dir = Some(PathBuf::from("/mnt/biglake/target/debug"));
        args.output = Some(PathBuf::from("/other/libfoo.rlib"));
        assert_eq!(
            super::volume_route_path_rustc(&args),
            PathBuf::from("/mnt/biglake/target/debug")
        );
        args.out_dir = None;
        assert_eq!(
            super::volume_route_path_rustc(&args),
            PathBuf::from("/other")
        );
        args.output = Some(PathBuf::from("libfoo.rlib"));
        assert_eq!(
            super::volume_route_path_rustc(&args),
            PathBuf::from("libfoo.rlib")
        );
    }

    #[test]
    fn volume_route_path_cc_uses_output_parent() {
        let mut parsed = crate::compiler::cc::CcArgs {
            program: "cc".into(),
            rest: Vec::new(),
            sources: vec![PathBuf::from("a.c")],
            output: Some(PathBuf::from("/mnt/biglake/build/a.o")),
            mode: crate::compiler::cc::CompileMode::Compile,
            includes: Vec::new(),
            defines: Vec::new(),
            optimization: None,
            debug_level: None,
            std: None,
            pic: false,
            depinfo: None,
            language_override: None,
            family: crate::compiler::cc::ToolFamily::Gnu,
        };
        assert_eq!(
            super::volume_route_path_cc(&parsed),
            PathBuf::from("/mnt/biglake/build")
        );
        parsed.output = Some(PathBuf::from("a.o"));
        assert_eq!(super::volume_route_path_cc(&parsed), PathBuf::from("a.o"));
    }

    #[test]
    fn volume_cache_dirs_match_is_path_equality() {
        assert!(super::volume_cache_dirs_match(
            Path::new("/cache/main"),
            Path::new("/cache/main")
        ));
        assert!(!super::volume_cache_dirs_match(
            Path::new("/cache/shard"),
            Path::new("/cache/main")
        ));
    }

    #[test]
    fn open_primary_and_fallback_skips_fallback_when_unmapped() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().to_path_buf());
        let (_, fallback) =
            super::open_primary_and_fallback(&cfg, Path::new("/unmapped/out.rlib")).unwrap();
        assert!(
            fallback.is_none(),
            "the main store must not open itself as a fallback"
        );
    }

    #[test]
    fn lookup_local_entry_prefers_primary_then_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let primary_cfg = test_config(dir.path().join("primary"));
        let main_cfg = test_config(dir.path().join("main"));
        let primary = Store::open(&primary_cfg).unwrap();
        let fallback = Store::open(&main_cfg).unwrap();
        put_test_entry(&fallback, dir.path(), "vol-fallback-key");
        let miss = super::lookup_local_entry(&primary, Some(&fallback), "no-such-key").unwrap();
        assert!(miss.is_none());
        let hit = super::lookup_local_entry(&primary, Some(&fallback), "vol-fallback-key")
            .unwrap()
            .expect("fallback must serve a key the shard does not have");
        assert_eq!(hit.1.crate_name, "test-crate");
        put_test_entry(&primary, dir.path(), "vol-primary-key");
        let primary_hit =
            super::lookup_local_entry(&primary, Some(&fallback), "vol-primary-key").unwrap();
        assert!(primary_hit.is_some());
    }

    /// Store a small entry so the store has a nonzero size.
    fn put_test_entry(store: &Store, dir: &std::path::Path, key: &str) {
        let src = dir.join(format!("{key}.o"));
        std::fs::write(&src, vec![0xABu8; 4096]).unwrap();
        store
            .put(
                key,
                "test-crate",
                &[],
                &[],
                "host",
                "dev",
                &[(src, format!("{key}.o"))],
                "",
                "",
            )
            .unwrap();
    }

    #[test]
    fn auto_gc_wanted_fires_only_over_budget_and_respects_throttle() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        let store = Store::open(&cfg).unwrap();
        put_test_entry(&store, dir.path(), "auto-gc-key-1");

        // Under budget (max_size = 1 MiB, entry = 4 KiB): no GC wanted,
        // but the stamp is written to throttle future check intervals.
        assert!(
            !auto_gc_wanted(&cfg, &store),
            "under budget must not trigger"
        );
        let stamp = auto_gc_stamp_path(&cfg.cache_dir);
        assert!(stamp.exists(), "under budget must create the stamp");

        // Over budget but with a fresh stamp: check is throttled.
        cfg.max_size = 1024; // 1 KiB budget, store holds 4 KiB (> +10% slack)
        assert!(
            !auto_gc_wanted(&cfg, &store),
            "fresh stamp must throttle the check even if over budget"
        );

        // Age the stamp past the interval → over-budget check now fires.
        let old = std::time::SystemTime::now() - (AUTO_GC_CHECK_INTERVAL * 2);
        let stamp_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&stamp)
            .unwrap();
        stamp_file.set_modified(old).unwrap();
        drop(stamp_file);
        assert!(
            auto_gc_wanted(&cfg, &store),
            "over budget with an expired stamp must trigger"
        );
        // ... and the successful check re-stamps, throttling the next one.
        assert!(
            !auto_gc_wanted(&cfg, &store),
            "the triggering check must re-claim the stamp"
        );
    }

    #[test]
    fn auto_gc_wanted_respects_disable_and_slack() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        let store = Store::open(&cfg).unwrap();
        put_test_entry(&store, dir.path(), "auto-gc-key-2");

        // Disabled: never triggers, regardless of size.
        cfg.auto_gc = false;
        cfg.max_size = 1;
        assert!(!auto_gc_wanted(&cfg, &store), "auto_gc=false must disable");

        // Enabled but within the +10% slack band: no trigger. The store holds
        // exactly 4096 bytes; max_size 4000 → threshold 4400 ≥ 4096.
        cfg.auto_gc = true;
        cfg.max_size = 4000;
        assert!(
            !auto_gc_wanted(&cfg, &store),
            "inside the slack band must not trigger"
        );
    }

    /// Age the auto-GC throttle stamp past the check interval.
    fn expire_auto_gc_stamp(cfg: &Config) {
        let stamp = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(auto_gc_stamp_path(&cfg.cache_dir))
            .unwrap();
        stamp
            .set_modified(std::time::SystemTime::now() - AUTO_GC_CHECK_INTERVAL * 2)
            .unwrap();
    }

    /// Store an idle entry whose blob a target directory still hardlinks:
    /// eviction reaches it and cannot free it.
    fn put_retained_entry(store: &Store, dir: &std::path::Path, key: &str) {
        let src = dir.join(format!("{key}.o"));
        std::fs::write(&src, &key.as_bytes().repeat(4096)[..4096]).unwrap();
        store
            .put(
                key,
                "test-crate",
                &[],
                &[],
                "host",
                "dev",
                &[(src, format!("{key}.o"))],
                "",
                "",
            )
            .unwrap();
        let meta = store.get(key).unwrap().unwrap();
        std::fs::hard_link(
            store.blob_path(&meta.files[0].hash),
            dir.join(format!("{key}-target.o")),
        )
        .unwrap();
        store.set_last_accessed_for_test(key, "-1 hour");
    }

    /// The production thrash: a store over budget whose bytes target
    /// directories still hold. Every sweep freed nothing and the next check,
    /// five minutes later, spawned another one, around the clock, contending
    /// with builds for the index each time.
    #[test]
    fn auto_gc_backs_off_while_a_sweep_leaves_the_store_over_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1024;
        let store = Store::open(&cfg).unwrap();
        put_retained_entry(&store, dir.path(), "retained-1");
        put_retained_entry(&store, dir.path(), "retained-2");

        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert!(store.contains("retained-1") && store.contains("retained-2"));
        let backoff = read_auto_gc_backoff(&cfg.cache_dir).expect("backoff recorded");
        assert_eq!(backoff.interval_secs, 600);
        assert_eq!(backoff.size_after, 8192);

        expire_auto_gc_stamp(&cfg);
        assert!(
            !auto_gc_wanted(&cfg, &store),
            "a sweep that could not clear the pressure must not re-run at the next check"
        );

        // The worker honours the backoff too: nothing to double yet.
        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert_eq!(auto_gc_backoff_interval_for_test(&cfg.cache_dir), Some(600));

        expire_auto_gc_backoff_for_test(&cfg.cache_dir);
        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert_eq!(
            read_auto_gc_backoff(&cfg.cache_dir).unwrap().interval_secs,
            1200,
            "another fruitless sweep doubles the wait"
        );

        // New bytes past the slack may be reclaimable: the backoff yields.
        put_test_entry(&store, dir.path(), "fresh");
        expire_auto_gc_stamp(&cfg);
        assert!(auto_gc_wanted(&cfg, &store));
    }

    #[test]
    fn auto_gc_worker_leaves_the_backoff_alone_when_it_did_not_sweep() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1024;
        let store = Store::open(&cfg).unwrap();
        put_retained_entry(&store, dir.path(), "retained");

        let gc_lock = store.try_gc_lock().unwrap().expect("gc lock");
        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert_eq!(read_auto_gc_backoff(&cfg.cache_dir), None);

        drop(gc_lock);
        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert!(read_auto_gc_backoff(&cfg.cache_dir).is_some());
    }

    /// Store an idle entry of exactly `size` bytes.
    fn put_sized_entry(store: &Store, dir: &std::path::Path, key: &str, size: usize) {
        let src = dir.join(format!("{key}.o"));
        std::fs::write(&src, &key.as_bytes().repeat(size)[..size]).unwrap();
        store
            .put(
                key,
                "test-crate",
                &[],
                &[],
                "host",
                "dev",
                &[(src.clone(), format!("{key}.o"))],
                "",
                "",
            )
            .unwrap();
        // A source left behind can share the blob's blocks and retain it.
        std::fs::remove_file(&src).unwrap();
        store.set_last_accessed_for_test(key, "-1 hour");
    }

    /// Sweeps the recorder has seen, one line each.
    fn recorded_gc_runs(cfg: &Config) -> usize {
        std::fs::read_to_string(crate::report::gc_runs_log_path(&cfg.cache_dir))
            .map_or(0, |log| log.lines().count())
    }

    /// Under a compiler shim `current_exe` can be the shim. The worker must
    /// still run as `kache gc`: before the fix it ran as `cc gc`, the real
    /// compiler failed on it, and automatic GC never ran.
    #[test]
    fn auto_gc_worker_runs_gc_even_from_a_shim_path() {
        let exe = Path::new("/x/shims/cc");
        let cmd = auto_gc_worker_command(exe);
        assert_eq!(cmd.get_program(), exe.as_os_str());
        let args: Vec<&std::ffi::OsStr> = cmd.get_args().collect();
        assert_eq!(args, ["gc"]);
        let env = |name: &str| {
            cmd.get_envs()
                .find(|(key, _)| *key == name)
                .and_then(|(_, value)| value)
        };
        assert_eq!(env("KACHE_AUTO_GC_WORKER"), Some(std::ffi::OsStr::new("1")));
        let argv = ["/x/shims/cc".to_string(), "gc".to_string()];
        assert!(
            crate::platform::is_self_spawn(&argv, env(crate::platform::SELF_SPAWN_ENV)),
            "the child must route to the CLI despite its shim-named argv[0]"
        );
        // argv[0] has no getter. The worker must be `self_command` plus its
        // own variable, and platform's tests check that command's argv[0]
        // with a real child. Comparing the two Debug strings does not depend
        // on how std formats them.
        let mut expected = crate::platform::self_command(exe, "gc");
        expected.env("KACHE_AUTO_GC_WORKER", "1");
        assert_eq!(format!("{cmd:?}"), format!("{expected:?}"));
    }

    #[test]
    fn auto_gc_constants_are_pinned() {
        assert_eq!(AUTO_GC_CHECK_INTERVAL.as_secs(), 300);
        assert_eq!(AUTO_GC_SLACK_PERCENT, 10);
        assert_eq!(AUTO_GC_MAX_BACKOFF.as_secs(), 7200);
        // Start above 110% of max_size, stop at 90%.
        assert_eq!(auto_gc_threshold(1000), 1100);
        assert_eq!(kache_store::eviction::eviction_target(1000), 900);
    }

    #[test]
    fn auto_gc_sweep_due_starts_above_the_trigger_and_honours_the_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1000;
        assert!(!auto_gc_sweep_due(&cfg, 1099));
        assert!(!auto_gc_sweep_due(&cfg, 1100));
        assert!(auto_gc_sweep_due(&cfg, 1101));

        record_auto_gc_outcome(&cfg, 1101);
        assert!(!auto_gc_sweep_due(&cfg, 1101), "a held backoff defers");
        assert!(!auto_gc_sweep_due(&cfg, 1201));
        assert!(auto_gc_sweep_due(&cfg, 1202), "growth past the slack");
        expire_auto_gc_backoff_for_test(&cfg.cache_dir);
        assert!(auto_gc_sweep_due(&cfg, 1101));
    }

    #[test]
    fn auto_gc_wanted_starts_one_byte_above_the_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1000;
        let store = Store::open(&cfg).unwrap();
        put_sized_entry(&store, dir.path(), "at-the-trigger", 1100);
        assert_eq!(store.physical_size().unwrap(), 1100);
        assert!(!auto_gc_wanted(&cfg, &store));

        put_sized_entry(&store, dir.path(), "x", 1);
        assert_eq!(store.physical_size().unwrap(), 1101);
        expire_auto_gc_stamp(&cfg);
        assert!(auto_gc_wanted(&cfg, &store));
    }

    #[test]
    fn auto_gc_check_hints_a_reachable_daemon_and_spawns_no_worker() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1024;
        let store = Store::open(&cfg).unwrap();
        put_test_entry(&store, dir.path(), "over-budget");
        let daemon = RemoteCheckReplyDaemon::with_reply(
            cfg.socket_path(),
            serde_json::json!({ "ok": true }),
        );
        wait_until_reachable(&cfg.socket_path());

        let spawned = AtomicUsize::new(0);
        let spawn = |_: &Config| {
            spawned.fetch_add(1, Ordering::SeqCst);
        };
        run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
        assert_eq!(daemon.request_count(), 1);
        assert_eq!(spawned.load(Ordering::SeqCst), 0);

        // A fresh stamp: the rest of the build sends nothing.
        run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
        assert_eq!(daemon.request_count(), 1);
        assert_eq!(spawned.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn auto_gc_check_spawns_the_worker_when_no_daemon_listens() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1024;
        let store = Store::open(&cfg).unwrap();
        put_test_entry(&store, dir.path(), "over-budget");

        let spawned = AtomicUsize::new(0);
        let spawn = |_: &Config| {
            spawned.fetch_add(1, Ordering::SeqCst);
        };
        run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
        assert_eq!(spawned.load(Ordering::SeqCst), 1);

        run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, spawn);
        assert_eq!(spawned.load(Ordering::SeqCst), 1, "fresh stamp");
    }

    /// A daemon from before the hint answers it with an error.
    #[test]
    fn auto_gc_check_spawns_the_worker_when_the_daemon_rejects_the_hint() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1024;
        let store = Store::open(&cfg).unwrap();
        put_test_entry(&store, dir.path(), "over-budget");
        let daemon = RemoteCheckReplyDaemon::with_reply(
            cfg.socket_path(),
            serde_json::json!({ "ok": false, "error": "invalid request: unknown variant" }),
        );
        wait_until_reachable(&cfg.socket_path());

        let spawned = AtomicUsize::new(0);
        run_auto_gc_check(&cfg, &store, crate::daemon::send_gc_hint, |_: &Config| {
            spawned.fetch_add(1, Ordering::SeqCst);
        });
        assert_eq!(daemon.request_count(), 1);
        assert_eq!(spawned.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn auto_gc_check_does_nothing_under_the_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(dir.path().to_path_buf());
        let store = Store::open(&cfg).unwrap();
        put_test_entry(&store, dir.path(), "fits");
        let called = AtomicUsize::new(0);
        let hint = |_: &Config| {
            called.fetch_add(1, Ordering::SeqCst);
            false
        };
        let spawn = |_: &Config| {
            called.fetch_add(1, Ordering::SeqCst);
        };
        run_auto_gc_check(&cfg, &store, hint, spawn);
        assert_eq!(called.load(Ordering::SeqCst), 0);
    }

    /// Another driver's fruitless sweep holds the worker back as well.
    #[test]
    fn auto_gc_worker_waits_out_a_backoff_another_driver_left() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1024;
        cfg.record_sessions = true;
        let store = Store::open(&cfg).unwrap();
        put_sized_entry(&store, dir.path(), "evictable", 4096);
        record_auto_gc_outcome(&cfg, 4096);

        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert!(store.contains("evictable"));
        assert_eq!(recorded_gc_runs(&cfg), 0);
        assert_eq!(auto_gc_backoff_interval_for_test(&cfg.cache_dir), Some(600));

        // Once it expires the worker sweeps, fits the store and clears it.
        expire_auto_gc_backoff_for_test(&cfg.cache_dir);
        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert!(!store.contains("evictable"));
        assert_eq!(
            recorded_gc_runs(&cfg),
            1,
            "a store that fits needs no retry"
        );
        assert_eq!(auto_gc_backoff_interval_for_test(&cfg.cache_dir), None);
    }

    /// The second sweep exists for entries a live build pins. Entries a
    /// target directory retains stay retained, so the worker sweeps once.
    #[test]
    fn auto_gc_worker_retries_only_for_pinned_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1024;
        cfg.record_sessions = true;
        let store = Store::open(&cfg).unwrap();
        put_retained_entry(&store, dir.path(), "retained");
        crate::cli::run_auto_gc_worker(&cfg, std::time::Duration::ZERO);
        assert_eq!(recorded_gc_runs(&cfg), 1);

        let pinned_dir = tempfile::tempdir().unwrap();
        let mut pinned_cfg = test_config(pinned_dir.path().to_path_buf());
        pinned_cfg.max_size = 1024;
        pinned_cfg.record_sessions = true;
        let pinned_store = Store::open(&pinned_cfg).unwrap();
        // Just stored: inside the idle grace, so eviction pins it.
        put_test_entry(&pinned_store, pinned_dir.path(), "pinned");
        crate::cli::run_auto_gc_worker(&pinned_cfg, std::time::Duration::ZERO);
        assert_eq!(recorded_gc_runs(&pinned_cfg), 2);
        assert_eq!(
            auto_gc_backoff_interval_for_test(&pinned_cfg.cache_dir),
            Some(600),
            "two sweeps of one worker record one outcome"
        );
    }

    /// The backoff is stored and read back across processes, so its clock
    /// must be the wall clock, not a constant every process agrees on.
    #[test]
    fn unix_now_secs_reads_the_wall_clock() {
        let wall = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        };
        let before = wall();
        let now = unix_now_secs();
        let after = wall();
        assert!(
            before <= now && now <= after,
            "{before} <= {now} <= {after}"
        );
    }

    #[test]
    fn next_auto_gc_backoff_doubles_to_the_cap_and_clears_under_budget() {
        // max 1000: the trigger is 1100.
        assert_eq!(next_auto_gc_backoff(None, 50, 1100, 1000), None);
        let first = next_auto_gc_backoff(None, 50, 1101, 1000).unwrap();
        assert_eq!(
            first,
            AutoGcBackoff {
                since: 50,
                interval_secs: 600,
                size_after: 1101,
            }
        );
        let second = next_auto_gc_backoff(Some(first), 90, 2000, 1000).unwrap();
        assert_eq!(
            second,
            AutoGcBackoff {
                since: 90,
                interval_secs: 1200,
                size_after: 2000,
            }
        );
        let long = AutoGcBackoff {
            interval_secs: 5000,
            ..second
        };
        assert_eq!(
            next_auto_gc_backoff(Some(long), 90, 2000, 1000)
                .unwrap()
                .interval_secs,
            7200
        );
        assert_eq!(next_auto_gc_backoff(Some(second), 100, 900, 1000), None);
    }

    #[test]
    fn auto_gc_backoff_holds_until_it_expires_or_the_store_grows() {
        let backoff = Some(AutoGcBackoff {
            since: 1000,
            interval_secs: 600,
            size_after: 5000,
        });
        // max 1000: the slack is 100 bytes.
        assert!(!auto_gc_backoff_holds(None, 1000, 5000, 1000));
        assert!(auto_gc_backoff_holds(backoff, 1599, 5000, 1000));
        assert!(!auto_gc_backoff_holds(backoff, 1600, 5000, 1000));
        assert!(auto_gc_backoff_holds(backoff, 1000, 5100, 1000));
        assert!(!auto_gc_backoff_holds(backoff, 1000, 5101, 1000));
        assert!(auto_gc_backoff_holds(backoff, 1000, 4000, 1000));
    }

    #[test]
    fn record_auto_gc_outcome_persists_the_backoff_until_the_store_fits() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = test_config(dir.path().to_path_buf());
        cfg.max_size = 1000;
        let before = unix_now_secs();
        record_auto_gc_outcome(&cfg, 5000);
        let backoff = read_auto_gc_backoff(&cfg.cache_dir).unwrap();
        assert_eq!((backoff.interval_secs, backoff.size_after), (600, 5000));
        assert!(backoff.since >= before && backoff.since <= unix_now_secs());
        assert!(auto_gc_backing_off(&cfg, 5000));

        record_auto_gc_outcome(&cfg, 5000);
        assert_eq!(
            read_auto_gc_backoff(&cfg.cache_dir).unwrap().interval_secs,
            1200
        );

        record_auto_gc_outcome(&cfg, 900);
        assert!(!auto_gc_backoff_path(&cfg.cache_dir).exists());
        assert!(!auto_gc_backing_off(&cfg, 5000));
    }

    /// #131: explain_miss names exactly the key groups whose digests changed
    /// vs the crate's last hit in the same tree — and stays silent (and
    /// log-read-free) when disabled, on hits, or with no prior hit.
    #[test]
    fn explain_miss_diff_names_changed_groups() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().to_path_buf());
        config.explain_miss = true;

        let fields_now: std::collections::BTreeMap<String, String> = [
            ("args".to_string(), "bbbb".to_string()),
            ("sources".to_string(), "ssss".to_string()),
        ]
        .into();

        // No prior hit in the log → nothing to diff against.
        assert!(
            explain_miss_diff(
                &config,
                "/w",
                "gkrust",
                EventResult::Miss,
                "newkey",
                &fields_now
            )
            .is_empty()
        );

        let hit: crate::events::BuildEvent = serde_json::from_str(
            r#"{"ts":"2026-07-23T00:00:00Z","crate_name":"gkrust","root":"/w",
                "result":"local_hit","elapsed_ms":1,"size":1,
                "key_fields":{"args":"aaaa","sources":"ssss","link":"llll"}}"#,
        )
        .unwrap();
        events::log_event(&config.event_log_path(), &hit).unwrap();

        let diff = explain_miss_diff(
            &config,
            "/w",
            "gkrust",
            EventResult::Miss,
            "newkey",
            &fields_now,
        );
        assert_eq!(
            diff,
            vec!["args".to_string(), "link".to_string()],
            "changed digest + group missing from the new key both count"
        );

        // Same fields as the hit → the difference must be in post-hoc folds.
        let unchanged: std::collections::BTreeMap<String, String> = [
            ("args".to_string(), "aaaa".to_string()),
            ("sources".to_string(), "ssss".to_string()),
            ("link".to_string(), "llll".to_string()),
        ]
        .into();
        assert_eq!(
            explain_miss_diff(
                &config,
                "/w",
                "gkrust",
                EventResult::Miss,
                "newkey",
                &unchanged
            ),
            vec!["salt_or_extra_inputs".to_string()],
        );

        // Off by default / hits: no diagnostics.
        assert!(
            explain_miss_diff(
                &config,
                "/w",
                "gkrust",
                EventResult::LocalHit,
                "newkey",
                &fields_now
            )
            .is_empty()
        );
        config.explain_miss = false;
        assert!(
            explain_miss_diff(
                &config,
                "/w",
                "gkrust",
                EventResult::Miss,
                "newkey",
                &fields_now
            )
            .is_empty()
        );
    }

    /// After a deferred compile the keyed flow must never run the compiler
    /// again: Cargo has already consumed the first run's artifact
    /// notifications, and a second set makes it finish the unit twice.
    #[test]
    fn a_passthrough_after_a_deferred_compile_keeps_the_first_exit_code() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let args = RustcArgs::parse(&[
            dir.path().join("no-such-rustc").display().to_string(),
            "--crate-name".into(),
            "kt".into(),
            "src/lib.rs".into(),
        ])
        .unwrap();
        PRECOMPILED_EXIT.with(|cell| cell.set(Some(0)));
        let exit = passthrough_with_event(
            &config,
            &args,
            "kt",
            "root",
            std::time::Instant::now(),
            "build lock wait failed",
        );
        PRECOMPILED_EXIT.with(|cell| cell.set(None));
        assert_eq!(exit.unwrap(), 0, "the compiler must not run a second time");
        assert!(
            passthrough_with_event(
                &config,
                &args,
                "kt",
                "root",
                std::time::Instant::now(),
                "build lock wait failed",
            )
            .is_err(),
            "without a deferred compile the passthrough runs the (missing) compiler"
        );
    }

    #[test]
    fn compile_before_key_needs_a_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().to_path_buf());
        let parse = |argv: &[&str]| {
            RustcArgs::parse(&argv.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };
        let cargo_like = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            "src/lib.rs",
            "--emit=dep-info,metadata",
            "--out-dir",
            "/t/debug/deps",
        ]);
        let no_dep_info = parse(&[
            "rustc",
            "--crate-name",
            "kt",
            "src/lib.rs",
            "--emit=link",
            "--out-dir",
            "/t/debug/deps",
        ]);
        assert!(
            deferral_allowed(&config, &cargo_like, false, None),
            "predictions off still defers a provable miss"
        );
        config.input_predictions = true;
        assert!(deferral_allowed(&config, &cargo_like, false, None));
        assert!(
            !deferral_allowed(&config, &no_dep_info, false, None),
            "nothing to key from"
        );
        config.deferred_discovery = false;
        assert!(
            !deferral_allowed(&config, &cargo_like, false, None),
            "switched off"
        );
        config.deferred_discovery = true;
        assert!(
            !deferral_allowed(&config, &cargo_like, true, None),
            "adaptive unit"
        );
        config.fallback = Some("sccache".to_string());
        assert!(
            !deferral_allowed(&config, &cargo_like, false, None),
            "fallback store"
        );
        config.fallback = None;
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        assert!(
            !deferral_allowed(&config, &cargo_like, false, None),
            "remote configured"
        );
    }

    fn test_config(cache_dir: PathBuf) -> Config {
        crate::test_support::test_config(cache_dir)
    }

    #[test]
    fn rows_are_asked_of_the_remote_only_with_predictions_and_a_remote() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().to_path_buf());
        config.remote = None;
        config.input_predictions = true;
        assert!(!fetches_remote_predictions(&config));
        config.remote = Some(crate::config::RemoteConfig {
            prefix: "artifacts".to_string(),
            backend: crate::config::RemoteBackendConfig::Filesystem(
                crate::config::FilesystemRemoteConfig {
                    root: dir.path().join("remote"),
                    atomic_write_dir: dir.path().join("staging"),
                },
            ),
        });
        assert!(fetches_remote_predictions(&config));
        config.input_predictions = false;
        assert!(!fetches_remote_predictions(&config));
    }

    #[test]
    fn configured_rustc_depinfo_roots_cover_every_restorable_anchor() {
        // This is the set the store side relativizes dep-info against. Losing a
        // root leaves a live producer path in the stored `.d`, so a relocated
        // hit lets cargo validate freshness against the donor's worktree
        // instead of the consumer's (#760).
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let workspace = base.join("workspace");
        let target = base.join("shared-target");
        let vendored = base.join("vendored-sources");
        for path in [&workspace, &target, &vendored] {
            std::fs::create_dir_all(path).unwrap();
        }

        let config = Config {
            base_dirs: vec![vendored.to_string_lossy().into_owned()],
            ..test_config(base.join("cache"))
        };
        let roots = configured_rustc_depinfo_roots(&config, Some(&workspace), Some(&target));

        let found = |root: &Path, sentinel: &str| {
            roots
                .iter()
                .any(|(path, depinfo_sentinel, _)| path == root && depinfo_sentinel == sentinel)
        };
        assert!(
            found(&workspace, "__kache_workspace__/"),
            "workspace root missing from {roots:?}"
        );
        assert!(
            found(&target, "__kache_target_rule__/"),
            "external target root missing from {roots:?}"
        );
        assert!(
            found(&vendored, "__kache_base_dir_0__/"),
            "configured base dir missing from {roots:?}"
        );

        // Priorities are what break ties when roots nest, so they must be the
        // real ranks rather than a uniform placeholder.
        let workspace_priority = roots
            .iter()
            .find(|(path, _, _)| path == &workspace)
            .map(|(_, _, priority)| *priority)
            .unwrap();
        let target_priority = roots
            .iter()
            .find(|(path, _, _)| path == &target)
            .map(|(_, _, priority)| *priority)
            .unwrap();
        assert!(
            workspace_priority > target_priority,
            "the workspace must outrank an external target ({workspace_priority} vs {target_priority})"
        );
    }

    #[test]
    fn input_race_store_suppression_truth_table() {
        for (extra_inputs_racy, guard_enabled, key_too_new, expected) in [
            (false, false, false, false),
            (false, false, true, false),
            (false, true, false, false),
            (false, true, true, true),
            (true, false, false, true),
            (true, false, true, true),
            (true, true, false, true),
            (true, true, true, true),
        ] {
            assert_eq!(
                should_skip_cache_store_for_input_race(
                    extra_inputs_racy,
                    guard_enabled,
                    key_too_new,
                ),
                expected
            );
        }
    }

    #[test]
    fn key_inputs_changed_excuses_skewed_clocks_but_not_real_changes() {
        use crate::cache_key::FileFingerprint;

        // No tripped flag: nothing to excuse, whatever was recorded.
        assert!(!key_inputs_changed_during_compile(false, &[]));
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("input.rs");
        std::fs::write(&file, b"pub fn x() {}").unwrap();
        let recorded = FileFingerprint::from_path(&file).unwrap();
        assert!(!key_inputs_changed_during_compile(
            false,
            std::slice::from_ref(&recorded)
        ));
        // Tripped flag with nothing verifiable stays a refusal.
        assert!(key_inputs_changed_during_compile(true, &[]));
        #[cfg(unix)]
        {
            // Tripped flag, untouched inputs: a skewed clock, not a race.
            assert!(!key_inputs_changed_during_compile(
                true,
                std::slice::from_ref(&recorded)
            ));
            // Tripped flag, rewritten inputs: a real race.
            std::fs::write(&file, b"pub fn x() { 1 }").unwrap();
            assert!(key_inputs_changed_during_compile(
                true,
                std::slice::from_ref(&recorded)
            ));
        }
    }

    #[test]
    fn key_measurements_include_extra_input_time_stats_and_races() {
        let local = FileHashStats {
            cache_hits: 11,
            cache_misses: 13,
            bytes_hashed: 17,
        };
        let extra = FileHashStats {
            cache_hits: 2,
            cache_misses: 3,
            bytes_hashed: 5,
        };

        let (key_ms, combined, too_new) =
            combine_key_measurements(19, 7, local, extra, false, true);
        assert_eq!(key_ms, 26);
        assert_eq!(combined.cache_hits, 13);
        assert_eq!(combined.cache_misses, 16);
        assert_eq!(combined.bytes_hashed, 22);
        assert!(too_new, "an extra input race must propagate");

        assert!(combine_key_measurements(0, 0, local, extra, true, false).2);
        assert!(!combine_key_measurements(0, 0, local, extra, false, false).2);
    }

    #[test]
    fn only_active_extra_inputs_make_unreadable_cached_dep_info_immediately_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.d");
        assert_eq!(read_cached_dep_info_blob(&missing, false).unwrap(), None);
        assert!(read_cached_dep_info_blob(&missing, true).is_err());

        let readable = dir.path().join("readable.d");
        std::fs::write(&readable, "foo: src/lib.rs\n").unwrap();
        assert_eq!(
            read_cached_dep_info_blob(&readable, true).unwrap(),
            Some("foo: src/lib.rs\n".to_string())
        );
    }

    #[test]
    fn adaptive_mode_requires_opt_in_without_explicit_preservation() {
        let mut config = test_config(PathBuf::from("cache"));
        for (adaptive, preserve, expected) in [
            (false, false, false),
            (false, true, false),
            (true, true, false),
            (true, false, true),
        ] {
            config.adaptive_incremental = adaptive;
            config.preserve_incremental = preserve;
            assert_eq!(adaptive_mode_enabled(&config), expected);
        }

        let without_incremental = rustc_args(&["rustc", "src/lib.rs"]);
        let with_incremental = rustc_args(&["rustc", "src/lib.rs", "-Cincremental=incremental"]);
        config.preserve_incremental = false;
        assert!(!preserve_incremental_requested(&config, &with_incremental));
        config.preserve_incremental = true;
        assert!(!preserve_incremental_requested(
            &config,
            &without_incremental
        ));
        assert!(preserve_incremental_requested(&config, &with_incremental));
    }

    #[test]
    fn incremental_force_list_requires_incremental_and_managed_layout() {
        let mut config = test_config(PathBuf::from("cache"));
        config.adaptive_incremental = false;
        assert!(
            !config.incremental_crate_forced("tap_lib"),
            "empty force-list must force nothing"
        );
        config.incremental_crates =
            crate::config::normalize_incremental_crates(["tap-lib".to_string()]);
        // Matching is against rustc's crate name; spelling normalization does
        // not make the Cargo package name authoritative.
        assert!(config.incremental_crate_forced("tap_lib"));
        assert!(config.incremental_crate_forced("tap-lib"));
        assert!(!config.incremental_crate_forced("other"));

        let no_incremental = rustc_args(&["rustc", "--crate-name", "tap_lib", "src/lib.rs"]);
        assert!(!force_incremental_requested(&config, &no_incremental));
        assert!(
            managed_incremental_unit(&config, &no_incremental, true, || {
                panic!("hidden-input discovery must not run for an ineligible invocation")
            })
            .is_none()
        );

        let temp = tempfile::tempdir().unwrap();
        let args = eligible_incremental_args(&temp, "tap_lib");
        assert!(force_incremental_requested(&config, &args));
        let unit = managed_incremental_unit(&config, &args, true, || false).unwrap();
        let lease = unit.try_immediate().unwrap();
        let compiler_args = lease.compiler_args(&args);
        let original = args.incremental.as_ref().unwrap().display().to_string();
        assert!(
            compiler_args
                .iter()
                .any(|arg| arg.contains("incremental.kache-auto") && arg.ends_with("rustc")),
            "force-list must use policy-owned incremental state: {compiler_args:?}"
        );
        assert!(
            !compiler_args.iter().any(|arg| arg.ends_with(&original)),
            "the original Cargo incremental path must never reach rustc"
        );
        assert!(!lease.finish(false));
    }

    #[test]
    fn force_list_never_retries_through_adaptive_seed_policy() {
        let mut config = test_config(PathBuf::from("cache"));
        config.adaptive_incremental = true;
        config.incremental_crates = vec!["tap_lib".to_string()];
        let args = rustc_args(&[
            "rustc",
            "--crate-name",
            "tap_lib",
            "src/lib.rs",
            "-Cincremental=incremental",
        ]);

        assert!(force_incremental_requested(&config, &args));
        assert!(adaptive_mode_enabled(&config));
        assert!(
            !adaptive_seed_allowed(&config, &args),
            "a force-listed invocation must not enter adaptive seed policy"
        );

        config.incremental_crates.clear();
        assert!(adaptive_seed_allowed(&config, &args));
        config.adaptive_incremental = false;
        assert!(!adaptive_seed_allowed(&config, &args));
    }

    #[test]
    fn force_list_hidden_inputs_and_cache_exclusions_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path().join("cache"));
        config.adaptive_incremental = false;
        config.incremental_crates = vec!["tap_lib".to_string()];
        let args = eligible_incremental_args(&temp, "tap_lib");

        assert!(managed_incremental_unit(&config, &args, true, || true).is_none());
        assert!(incremental_fast_path_allowed(false, false, false));
        assert!(!incremental_fast_path_allowed(false, true, false));
        assert!(!incremental_fast_path_allowed(false, false, true));
        assert!(!incremental_fast_path_allowed(true, false, false));
        // Either refusal alone keeps a unit off the fast path.
        assert!(!unit_refuses_caching(false, false));
        assert!(unit_refuses_caching(true, false));
        assert!(unit_refuses_caching(false, true));
        assert!(unit_refuses_caching(true, true));

        let stripped: Vec<_> = compile::strip_incremental_flags(&args.all_args)
            .into_iter()
            .cloned()
            .collect();
        assert!(
            !stripped.iter().any(|arg| arg.contains("incremental=")),
            "a rejected force-list invocation must retain the safe cache argv"
        );
    }

    #[test]
    fn incremental_cleanup_requires_opt_in_without_preservation() {
        let mut config = test_config(PathBuf::from("cache"));
        for (clean, preserve, expected) in [
            (false, false, false),
            (false, true, false),
            (true, true, false),
            (true, false, true),
        ] {
            config.clean_incremental = clean;
            config.preserve_incremental = preserve;
            assert_eq!(incremental_cleanup_enabled(&config), expected);
        }

        assert!(disable_incremental_env(false));
        assert!(!disable_incremental_env(true));
    }

    #[test]
    fn adaptive_policy_guard_tracks_kache_semantic_inputs() {
        const ENV_KEY: &str = "KACHE_WRAPPER_POLICY_GUARD_TEST";

        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        let baseline = adaptive_policy_guard(&config);

        config.key_salt = Some("salt-a".to_string());
        assert_ne!(adaptive_policy_guard(&config), baseline);
        config.key_salt = None;

        let _env = TestEnvGuard::set(ENV_KEY, "value-a");
        config.key_env_vars = vec![ENV_KEY.to_string()];
        let env_a = adaptive_policy_guard(&config);
        unsafe { std::env::set_var(ENV_KEY, "value-b") };
        assert_ne!(adaptive_policy_guard(&config), env_a);
        config.key_env_vars.clear();

        config.base_dirs = vec![dir.path().display().to_string()];
        assert_ne!(adaptive_policy_guard(&config), baseline);
    }

    fn meta_with_diagnostics(stdout: &str, stderr: &str) -> crate::store::EntryMeta {
        crate::store::EntryMeta {
            cache_key: "k".to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "c".to_string(),
            crate_types: vec![],
            files: vec![],
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            features: vec![],
            target: String::new(),
            profile: String::new(),
            compile_time_ms: 0,
            emit_kinds: vec![],
        }
    }

    fn entry_meta_with_files(names: &[&str]) -> crate::store::EntryMeta {
        let mut meta = meta_with_diagnostics("", "");
        meta.files = names
            .iter()
            .map(|name| crate::store::CachedFile {
                name: (*name).to_string(),
                size: 1,
                hash: "0123456789abcdef".to_string(),
                executable: false,
            })
            .collect();
        meta
    }

    #[test]
    fn replay_cached_diagnostics_writes_nonempty_and_skips_empty() {
        // Non-empty streams are replayed verbatim, each to its own sink. This is
        // the contract the coalesced-restore (and every cache-hit) path relies on
        // to avoid swallowing the original compiler warnings/notes.
        let m = meta_with_diagnostics("warning: unused\n", "error: boom\n");
        let mut out = Vec::new();
        let mut err = Vec::new();
        replay_cached_diagnostics(&m, &mut out, &mut err);
        assert_eq!(out, b"warning: unused\n");
        assert_eq!(err, b"error: boom\n");

        // Empty streams write nothing — the `!is_empty()` guard is load-bearing:
        // dropping it (as a mutant does) would make the non-empty case above emit
        // nothing, which the assertions catch.
        let empty = meta_with_diagnostics("", "");
        let mut out2 = Vec::new();
        let mut err2 = Vec::new();
        replay_cached_diagnostics(&empty, &mut out2, &mut err2);
        assert!(out2.is_empty(), "empty stdout must not be written");
        assert!(err2.is_empty(), "empty stderr must not be written");
    }

    #[test]
    fn replay_diagnostics_forwards_both_compiler_streams() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        replay_diagnostics("compiler stdout\n", "compiler stderr\n", &mut out, &mut err);
        assert_eq!(out, b"compiler stdout\n");
        assert_eq!(err, b"compiler stderr\n");
    }

    #[test]
    fn passthrough_direct_args_preserve_only_unchanged_response_transport() {
        let dir = tempfile::tempdir().unwrap();
        let response = dir.path().join("rustc.args");
        std::fs::write(&response, "--crate-name\nfixture\nsrc/lib.rs\n").unwrap();
        let response_arg = format!("@{}", response.display());
        let args = RustcArgs::parse(&["rustc".to_string(), response_arg.clone()]).unwrap();

        let unchanged = passthrough_direct_args(&args, &args.all_args, false);
        assert!(!compiler_args_changed(&args, &args.all_args));
        assert_eq!(stripped_incremental_count(&args, &args.all_args), None);
        assert_eq!(
            unchanged.iter().map(|arg| arg.as_str()).collect::<Vec<_>>(),
            vec![response_arg.as_str()]
        );

        let rewritten = vec!["--crate-name".to_string(), "rewritten".to_string()];
        assert!(compiler_args_changed(&args, &rewritten));
        assert_eq!(stripped_incremental_count(&args, &rewritten), Some(1));
        let changed = passthrough_direct_args(&args, &rewritten, true);
        assert_eq!(
            changed.iter().map(|arg| arg.as_str()).collect::<Vec<_>>(),
            vec!["--crate-name", "rewritten"]
        );

        assert!(
            handle_response_file_error(anyhow::anyhow!("unchanged transport"), false)
                .unwrap()
                .is_none()
        );
        assert!(handle_response_file_error(anyhow::anyhow!("rewritten transport"), true).is_err());
    }

    fn cached_file(name: &str, hash: &str) -> crate::store::CachedFile {
        crate::store::CachedFile {
            name: name.to_string(),
            size: 1,
            hash: hash.to_string(),
            executable: false,
        }
    }

    fn entry_meta(
        cache_key: &str,
        files: Vec<crate::store::CachedFile>,
        emit_kinds: &[&str],
    ) -> crate::store::EntryMeta {
        crate::store::EntryMeta {
            cache_key: cache_key.to_string(),
            key_schema: crate::cache_key::CACHE_KEY_VERSION,
            crate_name: "foo".to_string(),
            crate_types: vec!["lib".to_string()],
            files,
            stdout: String::new(),
            stderr: String::new(),
            features: Vec::new(),
            target: "host".to_string(),
            profile: "dev".to_string(),
            compile_time_ms: 7,
            emit_kinds: emit_kinds.iter().map(|kind| (*kind).to_string()).collect(),
        }
    }

    fn create_blob(store: &Store, hash: &str, content: &[u8]) {
        let blob = store.blob_path(hash);
        std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
        std::fs::write(blob, content).unwrap();
    }

    #[test]
    fn active_extra_inputs_store_requires_the_expected_dep_info_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let source = project.join("src/lib.rs");
        let out_dir = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname='foo'\nversion='0.1.0'\n",
        )
        .unwrap();
        std::fs::write(project.join("kache.toml"), "extra_inputs = []\n").unwrap();
        std::fs::write(&source, "pub fn f() {}\n").unwrap();

        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "foo",
            "--emit",
            "dep-info",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);
        let snapshot = crate::extra_inputs::ExtraInputsSnapshot::resolve(
            args.source_file.as_deref(),
            "foo",
            args.is_primary,
            &FileHasher::new(),
        )
        .unwrap()
        .unwrap();
        let artifacts = ArtifactSet::new(Vec::new());
        let error = validate_extra_inputs_dep_info_before_store(&args, &artifacts, &snapshot)
            .expect_err("active extra_inputs requires the dep-info Cargo requested");
        assert!(
            format!("{error:#}").contains("no expected dep-info artifact"),
            "{error:#}"
        );
    }

    #[test]
    fn active_extra_inputs_store_accepts_the_expected_dep_info_artifact() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("project");
        let source = project.join("src/lib.rs");
        let out_dir = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname='foo'\nversion='0.1.0'\n",
        )
        .unwrap();
        std::fs::write(project.join("kache.toml"), "extra_inputs = []\n").unwrap();
        std::fs::write(&source, "pub fn f() {}\n").unwrap();

        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "foo",
            "--emit",
            "dep-info",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);
        let snapshot = crate::extra_inputs::ExtraInputsSnapshot::resolve(
            args.source_file.as_deref(),
            "foo",
            args.is_primary,
            &FileHasher::new(),
        )
        .unwrap()
        .unwrap();
        let metadata = out_dir.join("libfoo.rmeta");
        std::fs::write(&metadata, b"metadata").unwrap();
        let dep_info = out_dir.join("foo.d");
        std::fs::write(&dep_info, format!("foo: {}\n", source.display())).unwrap();
        let artifacts = ArtifactSet::new(vec![
            crate::compiler::Artifact {
                path: metadata,
                store_name: "libfoo.rmeta".to_string(),
                kind: ArtifactKind::Metadata,
                required: true,
            },
            crate::compiler::Artifact {
                path: dep_info,
                store_name: "foo.d".to_string(),
                kind: ArtifactKind::DepInfo,
                required: true,
            },
        ]);

        validate_extra_inputs_dep_info_before_store(&args, &artifacts, &snapshot)
            .expect("the expected producer dep-info artifact is valid");
    }

    #[test]
    fn restore_rejects_dep_info_with_no_dependencies() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let project = dir.path().join("project");
        let source = project.join("src/lib.rs");
        let out_dir = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname='foo'\nversion='0.1.0'\n",
        )
        .unwrap();
        std::fs::write(&source, "pub fn f() {}\n").unwrap();

        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "foo",
            "--emit",
            "dep-info",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);
        let dep_info = "foo: \n";
        let hash = blake3::hash(dep_info.as_bytes()).to_hex().to_string();
        create_blob(&store, &hash, dep_info.as_bytes());
        let mut file = cached_file("foo.d", &hash);
        file.size = dep_info.len() as u64;
        let meta = entry_meta("empty-dependencies", vec![file], &["dep-info"]);

        let error = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .expect_err("empty dependency rules must be evicted");
        assert!(
            format!("{error:#}").contains("has no dependencies"),
            "{error:#}"
        );
    }

    #[test]
    fn cc_store_freezes_private_artifacts_without_mutating_compiler_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let object = dir.path().join("foo.o");
        let depinfo = dir.path().join("foo.d");
        let original = format!(
            "{}/foo.o: {}/src/foo.c\n",
            dir.path().display(),
            dir.path().display()
        );
        std::fs::write(&object, b"object").unwrap();
        std::fs::write(&depinfo, &original).unwrap();
        let artifacts = ArtifactSet::new(vec![
            crate::compiler::Artifact {
                path: object.clone(),
                store_name: "foo.o".to_string(),
                kind: ArtifactKind::Object,
                required: true,
            },
            crate::compiler::Artifact {
                path: depinfo.clone(),
                store_name: "foo.d".to_string(),
                kind: ArtifactKind::DepInfo,
                required: true,
            },
        ]);

        let prepared = prepare_cc_store_files(&artifacts, Some(dir.path())).unwrap();

        assert_eq!(std::fs::read_to_string(&depinfo).unwrap(), original);
        assert_ne!(prepared.files[0].0, object);
        assert_ne!(prepared.files[1].0, depinfo);
        assert_eq!(std::fs::read(&prepared.files[0].0).unwrap(), b"object");
        assert!(
            std::fs::read_to_string(&prepared.files[1].0)
                .unwrap()
                .contains("__kache_root__/")
        );

        std::fs::write(&object, b"concurrent replacement").unwrap();
        std::fs::write(&depinfo, b"concurrent replacement").unwrap();
        assert_eq!(
            std::fs::read(&prepared.files[0].0).unwrap(),
            b"object",
            "Store::put must read the frozen object snapshot"
        );
        assert!(
            std::fs::read_to_string(&prepared.files[1].0)
                .unwrap()
                .contains("__kache_root__/"),
            "Store::put must read the frozen normalized dep-info snapshot"
        );
    }

    /// Rust store staging leaves compiler outputs untouched while the cached
    /// dep-info round trip re-roots target, package, and workspace paths.
    #[test]
    fn rustc_store_staging_round_trips_depinfo_without_mutating_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let producing_workspace = dir.path().join("worktree-a");
        let producing_working_dir = producing_workspace.join("member");
        let producing_target = producing_workspace.join("target");
        let depfile = producing_target.join("release/deps/foo-abc.d");
        let rlib = producing_target.join("release/deps/libfoo-abc.rlib");
        std::fs::create_dir_all(depfile.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&producing_working_dir).unwrap();
        let original = format!(
            "{}: {} {}\n",
            rlib.display(),
            producing_working_dir.join("src/lib.rs").display(),
            producing_workspace.join("shared/asset.txt").display(),
        );
        std::fs::write(&depfile, &original).unwrap();
        std::fs::write(&rlib, b"rlib bytes").unwrap();
        let outputs = ArtifactSet::new(vec![
            crate::compiler::Artifact {
                path: depfile.clone(),
                store_name: "foo-abc.d".to_string(),
                kind: ArtifactKind::DepInfo,
                required: true,
            },
            crate::compiler::Artifact {
                path: rlib.clone(),
                store_name: "libfoo-abc.rlib".to_string(),
                kind: ArtifactKind::Library,
                required: true,
            },
        ]);

        let prepared = prepare_rustc_store_files(
            &outputs,
            Some(&producing_target),
            &producing_working_dir,
            Some(&producing_workspace),
            &[],
        )
        .unwrap();
        assert_eq!(std::fs::read_to_string(&depfile).unwrap(), original);
        assert_eq!(std::fs::read(&rlib).unwrap(), b"rlib bytes");
        assert_ne!(prepared.files[0].0, depfile);
        assert_eq!(prepared.files[1].0, rlib);
        assert_eq!(std::fs::read(&prepared.files[1].0).unwrap(), b"rlib bytes");

        let stored = std::fs::read_to_string(&prepared.files[0].0).unwrap();
        assert!(stored.contains("__kache_root__/release/deps/libfoo-abc.rlib"));
        assert!(stored.contains("__kache_cwd__/src/lib.rs"));
        assert!(stored.contains("__kache_workspace__/shared/asset.txt"));
        assert!(!stored.contains(producing_workspace.to_str().unwrap()));

        let restoring_workspace = dir.path().join("worktree-b");
        let restoring_working_dir = restoring_workspace.join("member");
        let restoring_target = restoring_workspace.join("target");
        let restored = link::rewrite_rustc_depinfo_content(
            &stored,
            &restoring_target,
            &restoring_working_dir,
            Some(&restoring_workspace),
            link::DepInfoMode::Expand,
        );
        assert!(
            restored.contains(
                restoring_target
                    .join("release/deps/libfoo-abc.rlib")
                    .to_str()
                    .unwrap()
            )
        );
        assert!(restored.contains(restoring_working_dir.join("src/lib.rs").to_str().unwrap()));
        assert!(
            restored.contains(
                restoring_workspace
                    .join("shared/asset.txt")
                    .to_str()
                    .unwrap()
            )
        );
    }

    /// Any staging failure skips cache publication without changing an output
    /// that was already read successfully.
    #[test]
    fn rustc_store_staging_refuses_missing_depinfo_without_mutating_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let valid = dir.path().join("valid.d");
        let original = format!(
            "{}/valid: {}/input.rs\n",
            dir.path().display(),
            dir.path().display()
        );
        std::fs::write(&valid, &original).unwrap();
        let outputs = ArtifactSet::new(vec![
            crate::compiler::Artifact {
                path: valid.clone(),
                store_name: "valid.d".to_string(),
                kind: ArtifactKind::DepInfo,
                required: true,
            },
            crate::compiler::Artifact {
                path: dir.path().join("missing.d"),
                store_name: "missing.d".to_string(),
                kind: ArtifactKind::DepInfo,
                required: true,
            },
        ]);
        let error = prepare_rustc_store_files(
            &outputs,
            Some(dir.path()),
            dir.path(),
            Some(dir.path()),
            &[],
        )
        .expect_err("a missing dep-info must prevent cache publication");
        assert!(format!("{error:#}").contains("opening dep-info"));
        assert_eq!(std::fs::read_to_string(valid).unwrap(), original);
    }

    #[test]
    fn cc_cache_entry_requires_depinfo_when_invocation_requests_it() {
        fn meta(names: &[&str]) -> crate::store::EntryMeta {
            crate::store::EntryMeta {
                cache_key: "key".to_string(),
                key_schema: crate::cache_key::CACHE_KEY_VERSION,
                crate_name: "foo.c".to_string(),
                crate_types: vec![],
                files: names
                    .iter()
                    .map(|name| crate::store::CachedFile {
                        name: (*name).to_string(),
                        size: 1,
                        hash: "0123456789abcdef".to_string(),
                        executable: false,
                    })
                    .collect(),
                stdout: String::new(),
                stderr: String::new(),
                features: vec![],
                target: String::new(),
                profile: String::new(),
                compile_time_ms: 0,
                emit_kinds: Vec::new(),
            }
        }

        let with_depinfo_args: Vec<String> = ["cc", "-c", "foo.c", "-o", "foo.o", "-MMD"]
            .into_iter()
            .map(String::from)
            .collect();
        let with_depinfo = CcCompiler::new().parse(&with_depinfo_args).unwrap();
        assert!(!cc_cache_entry_satisfies_invocation(
            &with_depinfo,
            &meta(&["foo.o"])
        ));
        assert!(cc_cache_entry_satisfies_invocation(
            &with_depinfo,
            &meta(&["foo.o", "foo.d"])
        ));
        assert!(cc_cache_entry_satisfies_invocation(
            &with_depinfo,
            &meta(&["foo.o", "foo.o.pp"])
        ));
        assert!(
            !cc_cache_entry_satisfies_invocation(&with_depinfo, &meta(&["foo.o", "foo.d.tmp"])),
            "a pre-fix raw compound dep-info entry must self-heal, not become trusted"
        );
        assert_eq!(
            cc_cache_entry_rejection_reason(&with_depinfo, &meta(&["foo.o", "foo.d.tmp"])),
            Some("matching entry lacks dep-info required by this invocation")
        );
        assert!(cc_cache_entry_satisfies_invocation(
            &with_depinfo,
            &meta(&["foo.o", crate::compiler::cc::CC_DEPINFO_STORE_NAME])
        ));

        let object_only_args: Vec<String> = ["cc", "-c", "foo.c", "-o", "foo.o"]
            .into_iter()
            .map(String::from)
            .collect();
        let object_only = CcCompiler::new().parse(&object_only_args).unwrap();
        assert!(cc_cache_entry_satisfies_invocation(
            &object_only,
            &meta(&["foo.o", "foo.d"])
        ));
    }

    /// No dep-info output means there is no safe anchor for `.d` rewriting, so
    /// the cc helper must leave the compile output untouched.
    #[test]
    fn cc_depinfo_rewrite_root_none_without_depinfo_request() {
        let args = s(&["cc", "-c", "foo.c", "-o", "foo.o"]);
        let parsed = CcCompiler::new().parse(&args).unwrap();

        assert_eq!(
            cc_depinfo_rewrite_root_from_cwd(&parsed, Path::new("/work/repo")),
            None
        );
    }

    #[test]
    fn cc_depinfo_rewrite_root_uses_common_source_and_object_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("repo");
        let cwd = root.join("obj-kache-bench").join("config");
        let source = root.join("config").join("pathsub.c");
        let args: Vec<String> = vec![
            "cc".to_string(),
            "-c".to_string(),
            source.to_string_lossy().into_owned(),
            "-o".to_string(),
            "host_pathsub.o".to_string(),
            "-MMD".to_string(),
            "-MF".to_string(),
            ".deps/host_pathsub.o.pp".to_string(),
        ];
        let parsed = CcCompiler::new().parse(&args).unwrap();

        assert_eq!(cc_depinfo_rewrite_root_from_cwd(&parsed, &cwd), Some(root));
    }

    /// When source and object paths only share the filesystem root, the helper
    /// falls back to the object anchor rather than relativizing against `/`.
    #[cfg(unix)]
    #[test]
    fn cc_depinfo_rewrite_root_falls_back_to_object_anchor_for_unrelated_paths() {
        let cwd = Path::new("/work/build");
        let source = Path::new("/src-only/foo.c");
        let object_dir = Path::new("/obj-only");
        let object = object_dir.join("foo.o");
        let args = vec![
            "cc".to_string(),
            "-c".to_string(),
            source.to_string_lossy().into_owned(),
            "-o".to_string(),
            object.to_string_lossy().into_owned(),
            "-MMD".to_string(),
        ];
        let parsed = CcCompiler::new().parse(&args).unwrap();

        assert_eq!(
            cc_depinfo_rewrite_root_from_cwd(&parsed, cwd),
            Some(object_dir.to_path_buf())
        );
    }

    /// Refusal reasons are serialized as `category|detail` for reporting; an
    /// empty list keeps the defensive default category with an empty detail.
    #[test]
    fn refuse_reason_string_formats_category_and_joined_details() {
        use crate::compiler::RefuseReason;

        assert_eq!(refuse_reason_string(&[]), "unsupported|");
        assert_eq!(
            refuse_reason_string(&[
                RefuseReason::Unsupported("first unsupported — not yet"),
                RefuseReason::Unsupported("second unsupported — not yet"),
            ]),
            "unsupported|first unsupported — not yet; second unsupported — not yet"
        );
        assert_eq!(
            refuse_reason_string(&[RefuseReason::NotPrimary]),
            "not-a-compile|query / probe (--print, -vV)"
        );
    }

    /// A cc restore should skip cached dep-info when this invocation did not
    /// request it, and skip unsupported sidecars without needing their blobs.
    #[test]
    fn restore_cc_from_cache_skips_unrequested_depinfo_and_unknown_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let args = s(&["cc", "-c", "foo.c", "-o", "foo.o"]);
        let parsed = CcCompiler::new().parse(&args).unwrap();
        let meta = entry_meta(
            "cc-skip-key",
            vec![
                cached_file("foo.d", "0123456789abcdef"),
                cached_file("readme.txt", "fedcba9876543210"),
            ],
            &[],
        );

        restore_cc_from_cache(&store, &parsed, &meta).unwrap();
    }

    /// Degenerate cc invocations with no object path fail before blob access,
    /// giving callers a clean miss instead of materializing to an unknown path.
    /// A preprocess hit has to land the expansion where `-o` asked for it.
    /// Nothing else in the restore path knows how to place a `.i`, so if this
    /// arm stops firing the caller reports a hit and leaves no output at all.
    #[test]
    fn restore_cc_from_cache_writes_the_preprocessed_output() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        create_blob(&store, hash, b"# 1 \"unit.c\"\nint expanded;\n");

        let output = dir.path().join("unit.i");
        let output_str = output.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&["cc", "-E", "unit.c", "-o", &output_str]))
            .unwrap();
        let meta = entry_meta("cc-preprocess-key", vec![cached_file("unit.i", hash)], &[]);

        restore_cc_from_cache(&store, &parsed, &meta).unwrap();
        assert_eq!(
            std::fs::read(&output).unwrap(),
            b"# 1 \"unit.c\"\nint expanded;\n",
            "the cached expansion must reach the path -o named"
        );

        let stdout_parsed = CcCompiler::new()
            .parse(&s(&["cc", "-E", "unit.c"]))
            .unwrap();
        let stdout_meta = entry_meta(
            "cc-stdout-key",
            vec![cached_file(crate::compiler::cc::CC_STDOUT_STORE_NAME, hash)],
            &[],
        );
        let mut restored = Vec::new();
        restore_cc_stdout_from_cache(&store, &stdout_meta, &mut restored).unwrap();
        assert_eq!(
            restored, b"# 1 \"unit.c\"\nint expanded;\n",
            "a stdout -E hit must replay the blob on stdout"
        );
        assert_eq!(
            cc_cache_entry_rejection_reason(&stdout_parsed, &stdout_meta),
            None
        );
        assert_eq!(
            cc_cache_entry_rejection_reason(&stdout_parsed, &meta),
            Some(
                "matching entry lacks the preprocessor stdout artifact required by this invocation"
            )
        );

        // An entry naming some other file is not this invocation's output, and
        // skipping it must not invent one.
        let other = dir.path().join("other.i");
        let other_str = other.to_string_lossy().into_owned();
        let mismatched = CcCompiler::new()
            .parse(&s(&["cc", "-E", "unit.c", "-o", &other_str]))
            .unwrap();
        restore_cc_from_cache(&store, &mismatched, &meta).unwrap();
        assert!(
            !other.exists(),
            "an entry for a different name must leave nothing behind"
        );
    }

    /// The rule that decides where a cached preprocess artifact goes, and
    /// whether it belongs to this invocation at all. Restore and hit
    /// qualification both use it, so they cannot disagree.
    #[test]
    fn cc_preprocess_restore_target_matches_only_the_named_output() {
        use crate::compiler::cc::CcArgs;
        let parse = |args: &[&str]| {
            CcArgs::parse(&args.iter().map(|a| (*a).to_string()).collect::<Vec<_>>()).unwrap()
        };

        let preprocess = parse(&["cc", "-E", "unit.c", "-o", "build/unit.i"]);
        assert_eq!(
            cc_preprocess_restore_target(&preprocess, "unit.i"),
            Some(std::path::PathBuf::from("build/unit.i")),
            "the cached name is the file this invocation asked for"
        );
        assert_eq!(
            cc_preprocess_restore_target(&preprocess, "other.i"),
            None,
            "an entry naming a different file must not be written here"
        );
        assert_eq!(
            cc_preprocess_restore_target(&preprocess, "unit.o"),
            None,
            "an object is not this invocation's output"
        );

        // Every other mode is somebody else's business, whatever the name.
        for args in [
            ["cc", "-c", "unit.c", "-o", "unit.i"].as_slice(),
            ["cc", "unit.c", "-o", "unit.i"].as_slice(),
        ] {
            let other = parse(args);
            assert_eq!(
                cc_preprocess_restore_target(&other, "unit.i"),
                None,
                "{args:?} is not a preprocess and must not take this path"
            );
        }
    }

    /// A preprocess entry qualifies on the file the invocation asked for,
    /// not on an object it was never going to produce. Getting this wrong
    /// reports a hit and leaves the build without its output.
    #[test]
    fn cc_entry_qualification_accepts_a_preprocess_output() {
        use crate::compiler::cc::CcArgs;
        let args: Vec<String> = ["cc", "-E", "unit.c", "-o", "unit.i"]
            .iter()
            .map(|a| (*a).to_string())
            .collect();
        let parsed = CcArgs::parse(&args).unwrap();

        let entry = |name: &str| entry_meta_with_files(&[name]);

        assert_eq!(
            cc_cache_entry_rejection_reason(&parsed, &entry("unit.i")),
            None,
            "the named expansion is what this invocation needs"
        );
        assert_eq!(
            cc_cache_entry_rejection_reason(&parsed, &entry("other.i")),
            Some("matching entry lacks the preprocessed output required by this invocation"),
            "an entry naming a different output cannot serve this one"
        );
        assert_eq!(
            cc_cache_entry_rejection_reason(&parsed, &entry("unit.o")),
            Some("matching entry lacks the preprocessed output required by this invocation"),
            "an object is not a preprocess output"
        );

        // And an ordinary compile still requires its object.
        let compile_args: Vec<String> = ["cc", "-c", "unit.c", "-o", "unit.o"]
            .iter()
            .map(|a| (*a).to_string())
            .collect();
        let compile = CcArgs::parse(&compile_args).unwrap();
        assert_eq!(
            cc_cache_entry_rejection_reason(&compile, &entry("unit.o")),
            None
        );
        assert_eq!(
            cc_cache_entry_rejection_reason(&compile, &entry("unit.i")),
            Some("matching entry lacks the object artifact required by this invocation")
        );

        let link_args: Vec<String> = ["cc", "a.o", "b.o", "-o", "prog"]
            .iter()
            .map(|a| (*a).to_string())
            .collect();
        let link = CcArgs::parse(&link_args).unwrap();
        assert_eq!(
            cc_cache_entry_rejection_reason(&link, &entry_meta_with_files(&[])),
            Some("matching entry lacks the link artifact required by this invocation"),
            "an empty link entry cannot serve the binary"
        );
        assert_eq!(
            cc_cache_entry_rejection_reason(&link, &entry("prog")),
            None,
            "any stored file is enough for a link hit"
        );
        assert!(
            cc_store_revalidates_include_dirs(crate::compiler::cc::CompileMode::Compile),
            "object compiles re-check include-dir names before store"
        );
        assert!(
            !cc_store_revalidates_include_dirs(crate::compiler::cc::CompileMode::Link),
            "links have no include-dir snapshot and must still store"
        );
    }

    #[test]
    fn restore_cc_from_cache_writes_the_link_binary_and_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let bin_hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let map_hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        create_blob(&store, bin_hash, b"ELF");
        create_blob(&store, map_hash, b"MAP");

        let output = dir.path().join("out");
        let output_str = output.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&["cc", "a.o", "b.o", "-o", &output_str]))
            .unwrap();
        let meta = entry_meta(
            "cc-link-key",
            vec![
                cached_file("app", bin_hash),
                cached_file("app.map", map_hash),
            ],
            &[],
        );

        restore_cc_from_cache(&store, &parsed, &meta).unwrap();
        assert_eq!(
            std::fs::read(&output).unwrap(),
            b"ELF",
            "the primary link artifact must land at -o even when the stored name differs"
        );
        assert!(
            !dir.path().join("app").exists(),
            "the stored extensionless name is not the restore destination"
        );
        assert_eq!(
            std::fs::read(dir.path().join("app.map")).unwrap(),
            b"MAP",
            "link sidecars must land next to the binary under their stored name"
        );

        let object = dir.path().join("unit.o");
        let object_str = object.to_string_lossy().into_owned();
        let compile = CcCompiler::new()
            .parse(&s(&["cc", "-c", "unit.c", "-o", &object_str]))
            .unwrap();
        restore_cc_from_cache(&store, &compile, &meta).unwrap();
        assert!(
            !object.exists(),
            "an executable blob must not restore onto a compile -o path"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_ne!(
                std::fs::metadata(&output).unwrap().permissions().mode() & 0o100,
                0,
                "restored link outputs must be owner-executable"
            );
        }
    }

    #[test]
    fn restore_cc_from_cache_signs_an_exe_and_a_dylib() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let exe_hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let so_hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        create_blob(&store, exe_hash, b"MZ exe");
        create_blob(&store, so_hash, b"\x7fELF so");

        let exe = dir.path().join("out.exe");
        let exe_str = exe.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&["cc", "a.o", "-o", &exe_str]))
            .unwrap();
        restore_cc_from_cache(
            &store,
            &parsed,
            &entry_meta("cc-exe-key", vec![cached_file("app.exe", exe_hash)], &[]),
        )
        .unwrap();
        assert_eq!(std::fs::read(&exe).unwrap(), b"MZ exe");

        let dylib = dir.path().join("libfoo.so");
        let dylib_str = dylib.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&["cc", "a.o", "-o", &dylib_str]))
            .unwrap();
        restore_cc_from_cache(
            &store,
            &parsed,
            &entry_meta("cc-so-key", vec![cached_file("libfoo.so", so_hash)], &[]),
        )
        .unwrap();
        assert_eq!(std::fs::read(&dylib).unwrap(), b"\x7fELF so");
    }

    #[test]
    fn prepare_cc_store_files_tars_a_dsym_directory_and_restore_unpacks_it() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();

        let dsym = dir.path().join("prog.dSYM");
        std::fs::create_dir_all(dsym.join("Contents/Resources/DWARF")).unwrap();
        std::fs::write(dsym.join("Contents/Resources/DWARF/prog"), b"dwarf-bytes").unwrap();

        let artifacts = ArtifactSet::new(vec![crate::compiler::Artifact {
            path: dsym,
            kind: ArtifactKind::DebugBundle,
            store_name: "prog.dsym.tar".to_string(),
            required: false,
        }]);
        let prepared = prepare_cc_store_files(&artifacts, None).unwrap();
        assert_eq!(prepared.files.len(), 1);
        assert_eq!(prepared.files[0].1, "prog.dsym.tar");
        let tar_bytes = std::fs::read(&prepared.files[0].0).unwrap();
        assert!(
            !tar_bytes.is_empty(),
            "a .dSYM directory must be stored as a tar, not copied as a file"
        );

        let tar_hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let bin_hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        create_blob(&store, tar_hash, &tar_bytes);
        create_blob(&store, bin_hash, b"ELF");
        let output = dir.path().join("build").join("prog");
        std::fs::create_dir_all(output.parent().unwrap()).unwrap();
        let output_str = output.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&["cc", "a.o", "-o", &output_str]))
            .unwrap();
        restore_cc_from_cache(
            &store,
            &parsed,
            &entry_meta(
                "cc-dsym-key",
                vec![
                    cached_file("prog", bin_hash),
                    cached_file("prog.dsym.tar", tar_hash),
                ],
                &[],
            ),
        )
        .unwrap();
        assert_eq!(std::fs::read(&output).unwrap(), b"ELF");

        let tar_path = dir.path().join("build").join("prog.dsym.tar");
        assert!(tar_path.is_file(), "the bundle tar itself must be restored");
        let dwarf = dir
            .path()
            .join("build")
            .join("prog.dSYM/Contents/Resources/DWARF/prog");
        assert_eq!(std::fs::read(&dwarf).unwrap(), b"dwarf-bytes");
    }

    #[test]
    fn prepare_cc_store_files_copies_an_already_packed_dsym_tar() {
        let dir = tempfile::tempdir().unwrap();
        let tar_path = dir.path().join("prog.dsym.tar");
        std::fs::write(&tar_path, b"already-packed-tar").unwrap();
        let artifacts = ArtifactSet::new(vec![crate::compiler::Artifact {
            path: tar_path,
            kind: ArtifactKind::DebugBundle,
            store_name: "prog.dsym.tar".to_string(),
            required: false,
        }]);
        let prepared = prepare_cc_store_files(&artifacts, None).unwrap();
        assert_eq!(
            std::fs::read(&prepared.files[0].0).unwrap(),
            b"already-packed-tar",
            "a DebugBundle that is already a tar file must be copied, not re-tarred as a directory"
        );
    }

    #[test]
    fn restore_cc_from_cache_requires_object_output_for_object_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let args = s(&["cc", "-c"]);
        let parsed = CcCompiler::new().parse(&args).unwrap();
        let meta = entry_meta(
            "cc-object-key",
            vec![cached_file("foo.o", "0123456789abcdef")],
            &[],
        );

        let err = restore_cc_from_cache(&store, &parsed, &meta)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("cannot determine object output path"),
            "unexpected error: {err}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn restore_cc_object_is_writable_private_and_keeps_blob_immutable() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        create_blob(&store, hash, b"cached object");
        std::fs::set_permissions(
            store.blob_path(hash),
            std::fs::Permissions::from_mode(0o400),
        )
        .unwrap();

        let output = dir.path().join("output.o");
        let output_str = output.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&["cc", "-c", "foo.c", "-o", &output_str]))
            .unwrap();
        let meta = entry_meta("cc-private-key", vec![cached_file("foo.o", hash)], &[]);

        restore_cc_from_cache(&store, &parsed, &meta).unwrap();

        let output_meta = std::fs::metadata(&output).unwrap();
        let blob_meta = std::fs::metadata(store.blob_path(hash)).unwrap();
        assert_ne!(output_meta.permissions().mode() & 0o200, 0);
        assert_eq!(output_meta.permissions().mode() & 0o111, 0);
        assert_ne!(output_meta.ino(), blob_meta.ino());
        std::fs::write(&output, b"changed").unwrap();
        assert_eq!(
            std::fs::read(store.blob_path(hash)).unwrap(),
            b"cached object"
        );
        assert!(blob_meta.permissions().readonly());
    }

    #[test]
    fn restore_cc_from_cache_replaces_existing_plain_object() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "abababababababababababababababababababababababababababababababab";
        create_blob(&store, hash, b"cached object");

        let output = dir.path().join("output.o");
        std::fs::write(&output, b"stale object").unwrap();
        let output_str = output.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&["cc", "-c", "foo.c", "-o", &output_str]))
            .unwrap();
        let meta = entry_meta("cc-replace-key", vec![cached_file("foo.o", hash)], &[]);

        restore_cc_from_cache(&store, &parsed, &meta).unwrap();

        assert_eq!(std::fs::read(&output).unwrap(), b"cached object");
        assert_eq!(
            std::fs::read(store.blob_path(hash)).unwrap(),
            b"cached object"
        );
    }

    #[test]
    fn cc_restore_revalidates_existing_target_before_replacing_it() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output.o");
        std::fs::write(&output, b"race winner").unwrap();
        let prepared =
            vec![link::prepare_writable_target_from_bytes(&output, b"cached object").unwrap()];

        let error = publish_prepared_cc_artifacts_with(prepared, |_, target| {
            let mut permissions = std::fs::metadata(target)?.permissions();
            permissions.set_readonly(true);
            std::fs::set_permissions(target, permissions)?;
            Ok(())
        })
        .unwrap_err();

        assert!(error.to_string().contains("requires compiler passthrough"));
        assert_eq!(std::fs::read(&output).unwrap(), b"race winner");

        // TempDir cleanup cannot remove a read-only Windows file.
        #[cfg(windows)]
        {
            let mut permissions = std::fs::metadata(&output).unwrap().permissions();
            #[allow(clippy::permissions_set_readonly_false)]
            permissions.set_readonly(false);
            std::fs::set_permissions(&output, permissions).unwrap();
        }
    }

    #[test]
    fn cc_restore_marks_partial_publication_and_preserves_race_winner() {
        let dir = tempfile::tempdir().unwrap();
        let object = dir.path().join("foo.o");
        let depinfo = dir.path().join("foo.d");
        let prepared = vec![
            link::prepare_writable_target_from_bytes(&object, b"cached object").unwrap(),
            link::prepare_writable_target_from_bytes(&depinfo, b"cached depinfo").unwrap(),
        ];

        let error = publish_prepared_cc_artifacts_with(prepared, |index, target| {
            if index == 1 {
                std::fs::write(target, b"race winner")?;
            }
            Ok(())
        })
        .unwrap_err();

        assert!(
            error.downcast_ref::<PartialCcRestore>().is_some(),
            "{error:#}"
        );
        assert_eq!(
            error.to_string(),
            "cc cache restore published only part of the output set"
        );
        assert_eq!(std::fs::read(&object).unwrap(), b"cached object");
        assert_eq!(std::fs::read(&depinfo).unwrap(), b"race winner");
    }

    #[test]
    fn cc_restore_classifies_hook_failures_by_publication_progress() {
        let dir = tempfile::tempdir().unwrap();
        let first_target = dir.path().join("first.o");
        let first =
            vec![link::prepare_writable_target_from_bytes(&first_target, b"first").unwrap()];
        let first_error = publish_prepared_cc_artifacts_with(first, |_, _| {
            anyhow::bail!("fail before first publication")
        })
        .unwrap_err();

        assert!(first_error.downcast_ref::<PartialCcRestore>().is_none());
        assert!(!first_target.exists());

        let object = dir.path().join("object.o");
        let depinfo = dir.path().join("object.d");
        let prepared = vec![
            link::prepare_writable_target_from_bytes(&object, b"cached object").unwrap(),
            link::prepare_writable_target_from_bytes(&depinfo, b"cached depinfo").unwrap(),
        ];
        let later_error = publish_prepared_cc_artifacts_with(prepared, |index, _| {
            if index == 1 {
                anyhow::bail!("fail after first publication");
            }
            Ok(())
        })
        .unwrap_err();

        assert!(
            later_error.downcast_ref::<PartialCcRestore>().is_some(),
            "{later_error:#}"
        );
        assert_eq!(std::fs::read(&object).unwrap(), b"cached object");
        assert!(!depinfo.exists());
    }

    /// Regression for #645: cache restore must not choose symlink semantics on
    /// the compiler's behalf. GCC writes through this path while some clang
    /// versions replace it, so the wrapper must refuse the hit and passthrough.
    #[cfg(unix)]
    #[test]
    fn restore_cc_from_cache_refuses_symlinked_object_output() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        create_blob(&store, hash, b"cached object");

        let target = dir.path().join("real.o");
        let output = dir.path().join("link.o");
        std::fs::write(&target, b"original").unwrap();
        symlink(&target, &output).unwrap();

        let output_str = output.to_string_lossy().into_owned();
        let args = s(&["cc", "-c", "foo.c", "-o", &output_str]);
        let parsed = CcCompiler::new().parse(&args).unwrap();
        let meta = entry_meta("cc-symlink-key", vec![cached_file("foo.o", hash)], &[]);

        let err = restore_cc_from_cache(&store, &parsed, &meta)
            .unwrap_err()
            .to_string();

        assert!(err.contains("requires compiler passthrough"), "{err}");
        assert!(
            std::fs::symlink_metadata(&output)
                .unwrap()
                .file_type()
                .is_symlink(),
            "refused cache restore must leave the -o symlink in place"
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"original");
        assert_eq!(
            std::fs::read(store.blob_path(hash)).unwrap(),
            b"cached object",
            "refusing the hit must not mutate the cache blob"
        );
    }

    /// Missing store blobs are surfaced as restore misses, which lets callers
    /// recompile instead of serving a partial cache hit.
    #[test]
    fn materialize_cached_artifact_reports_missing_blob_as_cache_miss() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let cached = cached_file("libfoo.rlib", "0123456789abcdef");
        let target = dir.path().join("target").join("libfoo.rlib");
        let platform = platform::current();

        let err = materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::Library,
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("was evicted before restore"),
            "unexpected error: {err}"
        );
    }

    /// Dep-info blobs are transformed before materialization so the store blob
    /// stays rooted at the producing build while the target is restored here.
    #[test]
    fn materialize_cached_artifact_expands_depinfo_blob_without_rewriting_store_blob() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let stored = "__kache_root__/debug/deps/libfoo.rlib: __kache_cwd__/src/lib.rs\n";
        create_blob(&store, hash, stored.as_bytes());
        let cached = cached_file("foo.d", hash);
        let target = dir
            .path()
            .join("target")
            .join("debug")
            .join("deps")
            .join("foo.d");
        let anchor = dir.path().join("target");
        let platform = platform::current();

        materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::DepInfo,
            None,
            &anchor,
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap();

        let restored = std::fs::read_to_string(&target).unwrap();
        assert!(
            restored.starts_with(&format!(
                "{}{}debug/deps/libfoo.rlib:",
                anchor.display(),
                std::path::MAIN_SEPARATOR
            )),
            "dep-info should be expanded at restore anchor, got: {restored}"
        );
        assert!(
            restored.contains(&format!(
                "{}{}src/lib.rs",
                dir.path().display(),
                std::path::MAIN_SEPARATOR
            )),
            "dep-info source should be expanded at the consumer cwd: {restored}"
        );
        assert_eq!(
            std::fs::read_to_string(store.blob_path(hash)).unwrap(),
            stored,
            "content transforms must not mutate the store blob"
        );
    }

    /// A `[[test]] harness = false` target is compiled without `--test` and
    /// without `--crate-type`, so its extensionless output classifies as
    /// `Other("rustc:unknown")` — the compile context simply never says
    /// "executable". The mode bit recorded at insert time does, and restore
    /// must honour it: otherwise the restored test binary comes back 0o644 and
    /// cargo fails the run with "Permission denied (os error 13)".
    #[cfg(unix)]
    #[test]
    fn materialize_cached_artifact_restores_executable_bit_recorded_at_insert() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        create_blob(&store, hash, b"\x7fELF harness=false test binary");
        // Store blobs are read-only and carry no executable bit, so a restore
        // that never chmods cannot produce a runnable file.
        std::fs::set_permissions(
            store.blob_path(hash),
            std::fs::Permissions::from_mode(0o444),
        )
        .unwrap();

        let mut cached = cached_file("harness-a1b2c3d4e5f60718", hash);
        cached.executable = true;
        let target = dir.path().join("target").join("harness-a1b2c3d4e5f60718");
        let platform = platform::current();

        materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::Other("rustc:unknown"),
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap();

        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_ne!(
            mode & 0o111,
            0,
            "restored test binary must stay executable, got {mode:o}"
        );
    }

    /// The converse: an artifact that was not executable at insert time must
    /// not acquire the bit on restore.
    #[cfg(unix)]
    #[test]
    fn materialize_cached_artifact_leaves_non_executable_artifacts_unexecutable() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        create_blob(&store, hash, b"rlib bytes");

        let cached = cached_file("libfoo.rlib", hash);
        let target = dir.path().join("target").join("libfoo.rlib");
        let platform = platform::current();

        materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::Library,
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap();

        let mode = std::fs::metadata(&target).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o111,
            0,
            "library must not become executable, got {mode:o}"
        );
    }

    // ── shared-inode restores of proc-macros and build scripts ─────────

    #[test]
    fn restore_link_strategy_shares_only_the_named_loadable_kind() {
        use link::LinkStrategy::{Copy, ExecutableHardlink, Hardlink};
        let proc_macro = Some(ArtifactKind::DynamicLibrary);
        let build_script = Some(ArtifactKind::Executable);
        assert_eq!(
            restore_link_strategy(ArtifactKind::DynamicLibrary, true, proc_macro),
            ExecutableHardlink
        );
        assert_eq!(
            restore_link_strategy(ArtifactKind::Executable, true, build_script),
            ExecutableHardlink
        );
        // The rest of the same compile keeps its ordinary strategy.
        assert_eq!(
            restore_link_strategy(ArtifactKind::DepInfo, false, proc_macro),
            Hardlink
        );
        assert_eq!(
            restore_link_strategy(ArtifactKind::Executable, true, proc_macro),
            Copy
        );
        // Without the gate every executable is a private copy.
        assert_eq!(
            restore_link_strategy(ArtifactKind::DynamicLibrary, true, None),
            Copy
        );
        assert_eq!(
            restore_link_strategy(ArtifactKind::Other("rustc:unknown"), true, None),
            Copy
        );
        assert_eq!(
            restore_link_strategy(ArtifactKind::Library, false, None),
            Hardlink
        );
    }

    #[test]
    fn only_proc_macros_and_build_scripts_may_share_a_restored_inode() {
        let compile = |crate_name: &str, crate_type: &str, out_dir: &str| {
            rustc_args(&[
                "rustc",
                "--crate-name",
                crate_name,
                "--crate-type",
                crate_type,
                "src/lib.rs",
                "--out-dir",
                out_dir,
                "-C",
                "extra-filename=-1",
            ])
        };
        let proc_macro = compile("serde_derive", "proc-macro", "/t/debug/deps");
        let build_script = compile("build_script_build", "bin", "/t/debug/build/pkg-1");
        assert_eq!(
            shared_inode_loadable(&proc_macro, true),
            Some(ArtifactKind::DynamicLibrary)
        );
        assert_eq!(
            shared_inode_loadable(&build_script, true),
            Some(ArtifactKind::Executable)
        );
        // `strip` may rewrite these in place after the build.
        for (label, other) in [
            ("user bin", compile("hk", "bin", "/t/debug/deps")),
            ("cdylib", compile("plugin", "cdylib", "/t/debug/deps")),
            ("dylib", compile("shared", "dylib", "/t/debug/deps")),
            ("lib", compile("serde", "lib", "/t/debug/deps")),
            (
                "bin named like a build script",
                compile("build_script_x", "bin", "/t/debug/deps"),
            ),
            (
                "test harness",
                rustc_args(&["rustc", "--crate-name", "hk", "--test", "src/main.rs"]),
            ),
        ] {
            assert_eq!(shared_inode_loadable(&other, true), None, "{label}");
        }
        // Platforms that may rewrite a restored loadable share nothing.
        assert_eq!(shared_inode_loadable(&proc_macro, false), None);
        assert_eq!(shared_inode_loadable(&build_script, false), None);
    }

    /// The Linux restore path for a proc-macro: the file comes back loadable,
    /// the mtime stamp works on a read-only shared inode, and the blob keeps
    /// its bytes and mode. Without reflink the file shares the blob's inode.
    #[cfg(unix)]
    #[test]
    fn materialize_shared_loadable_restores_a_loadable_file_and_leaves_the_blob() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "5555555555555555555555555555555555555555555555555555555555555555";
        create_blob(&store, hash, b"proc-macro dylib");
        let blob = store.blob_path(hash);
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o555)).unwrap();
        let reflinks = crate::link::try_reflink(&blob, &dir.path().join("probe")).is_ok();

        let mut cached = cached_file("libfoo_macros-1.so", hash);
        cached.executable = true;
        let target = dir.path().join("target/debug/deps/libfoo_macros-1.so");
        let restored = materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::DynamicLibrary,
            Some(ArtifactKind::DynamicLibrary),
            dir.path(),
            dir.path(),
            None,
            &[],
            &crate::compiler::platform::LinuxPlatform,
            "test restore",
            None,
        )
        .unwrap();

        assert!(matches!(restored, RestoredBytes::ExactBlobCopy(_)));
        let restored_meta = std::fs::metadata(&target).unwrap();
        assert_eq!(restored_meta.permissions().mode() & 0o111, 0o111);
        assert_eq!(std::fs::read(&target).unwrap(), b"proc-macro dylib");
        let blob_meta = std::fs::metadata(&blob).unwrap();
        assert_eq!(blob_meta.permissions().mode() & 0o777, 0o555);
        assert_eq!(std::fs::read(&blob).unwrap(), b"proc-macro dylib");
        assert_eq!(restored_meta.ino() == blob_meta.ino(), !reflinks);
    }

    /// End to end on Linux without reflink: a proc-macro hit shares the
    /// blob's inode, while a user binary restored the same way stays a
    /// private, writable copy that `strip` cannot turn into a blob write.
    #[cfg(target_os = "linux")]
    #[test]
    fn restore_from_cache_shares_a_proc_macro_inode_but_copies_a_user_binary() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let out_dir = dir.path().join("target/debug/deps");
        let out_dir_str = out_dir.to_str().unwrap();
        let restore = |crate_name: &str, crate_type: &str, file: &str, hash: &str| {
            create_blob(&store, hash, b"linked output");
            let blob = store.blob_path(hash);
            std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o555)).unwrap();
            let args = rustc_args(&[
                "rustc",
                "--crate-name",
                crate_name,
                "--crate-type",
                crate_type,
                "src/lib.rs",
                "--emit",
                "link",
                "--out-dir",
                out_dir_str,
                "-C",
                "extra-filename=-1",
            ]);
            let mut cached = cached_file(file, hash);
            cached.executable = true;
            let meta = entry_meta(crate_name, vec![cached], &["link"]);
            restore_from_cache(
                &config,
                &RustcCompiler::new(),
                &BlobSource::Store(&store),
                &args,
                &meta,
                None,
            )
            .unwrap();
            (blob, out_dir.join(file))
        };

        let (macro_blob, macro_target) = restore(
            "foo_macros",
            "proc-macro",
            "libfoo_macros-1.so",
            "6666666666666666666666666666666666666666666666666666666666666666",
        );
        if crate::link::try_reflink(&macro_blob, &dir.path().join("probe")).is_ok() {
            eprintln!("reflink available; the no-CoW restore path is not reachable here");
            return;
        }
        let ino = |path: &Path| std::fs::metadata(path).unwrap().ino();
        assert_eq!(ino(&macro_target), ino(&macro_blob));

        let (bin_blob, bin_target) = restore(
            "hk",
            "bin",
            "hk-1",
            "7777777777777777777777777777777777777777777777777777777777777777",
        );
        assert_ne!(ino(&bin_target), ino(&bin_blob));
        let mode = std::fs::metadata(&bin_target).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o755);
    }

    // ── restored-blob digest reuse (kunobi-ninja/kache#540) ──────────

    /// A plain library restore is a verbatim blob copy, so the entry's recorded
    /// digest still describes the file on disk and may be reused as its hash.
    #[test]
    fn materialize_reports_a_plain_library_restore_as_an_exact_blob_copy() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "1111111111111111111111111111111111111111111111111111111111111111";
        create_blob(&store, hash, b"rlib bytes");
        let cached = cached_file("libfoo.rlib", hash);
        let target = dir.path().join("target").join("libfoo.rlib");
        let platform = platform::current();

        let restored = materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::Library,
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap();

        assert!(matches!(restored, RestoredBytes::ExactBlobCopy(_)));
        assert_eq!(std::fs::read(&target).unwrap(), b"rlib bytes");
    }

    /// Dep-info is re-rooted for this consumer on the way out of the store, so
    /// what lands on disk is not what the blob's digest describes.
    #[test]
    fn materialize_reports_a_rewritten_depinfo_restore_as_not_exact() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "2222222222222222222222222222222222222222222222222222222222222222";
        create_blob(
            &store,
            hash,
            b"__kache_root__/debug/deps/libfoo.rlib: __kache_cwd__/src/lib.rs\n",
        );
        let cached = cached_file("foo.d", hash);
        let target = dir.path().join("target").join("foo.d");
        let platform = platform::current();

        let restored = materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::DepInfo,
            None,
            &dir.path().join("target"),
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap();

        assert_eq!(restored, RestoredBytes::Rewritten);
    }

    /// An external post-restore action that leaves the file alone — every
    /// platform but macOS-arm64 signing, plus macOS when the existing signature
    /// is still valid — keeps the restore exact.
    #[test]
    fn materialize_reports_an_untouched_external_action_as_an_exact_blob_copy() {
        use crate::compiler::platform::tests::CountingPlatform;

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "3333333333333333333333333333333333333333333333333333333333333333";
        create_blob(&store, hash, b"\x7fELF proc-macro");
        let cached = cached_file("libmac.so", hash);
        let target = dir.path().join("target").join("libmac.so");
        let platform = CountingPlatform::new();

        let restored = materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::DynamicLibrary,
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &platform,
            "test restore",
            None,
        )
        .unwrap();

        assert_eq!(platform.ensure_calls(), 1, "signing hook should have run");
        assert!(matches!(restored, RestoredBytes::ExactBlobCopy(_)));
    }

    /// A restored artifact that is overwritten before the seed lands must not
    /// hand the new bytes the old blob's digest. Seeding records the
    /// fingerprint observed at restore, so the overwritten file misses the memo
    /// and is hashed for real; re-stating the path at seed time instead would
    /// pair the new file's fingerprint with the old file's hash.
    #[test]
    fn seeding_does_not_attach_the_blobs_digest_to_a_file_overwritten_since_restore() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();

        let content = vec![b'r'; 128 * 1024];
        let source = dir.path().join("source.rlib");
        std::fs::write(&source, &content).unwrap();
        let hash = crate::cache_key::hash_file(&source).unwrap();
        create_blob(&store, &hash, &content);

        let cached = cached_file("libfoo.rlib", &hash);
        let target = dir.path().join("target").join("libfoo.rlib");
        let platform = platform::current();
        let blobs = BlobSource::Store(&store);

        let RestoredBytes::ExactBlobCopy(fingerprint) = materialize_cached_artifact(
            &blobs,
            &cached,
            &target,
            ArtifactKind::Library,
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap() else {
            panic!("a plain library restore should be exact");
        };

        // Someone else lands on the same output path before we get to record.
        let replacement = vec![b'z'; 256 * 1024];
        std::fs::remove_file(&target).unwrap();
        std::fs::write(&target, &replacement).unwrap();

        blobs.record_known_file_hashes(&[(fingerprint, hash.as_str())]);

        assert!(
            matches!(
                store.file_hash_lookup(&target),
                crate::cache_key::FileHashLookup::NeedsHash(_)
            ),
            "the overwritten file must not inherit the restored blob's digest"
        );
        assert_ne!(
            store.file_hasher().hash(&target).unwrap(),
            hash,
            "hashing the overwritten file must return its own digest"
        );
    }

    /// The guard that matters: an external tool that DOES rewrite the artifact
    /// (macOS re-signing an invalidated binary) leaves bytes the entry's digest
    /// no longer describes, so the restore must not be reported as exact.
    #[test]
    fn materialize_reports_a_mutating_external_action_as_not_exact() {
        /// Stands in for `codesign` re-signing a restored binary.
        struct RewritingPlatform(PathBuf);
        impl crate::compiler::Platform for RewritingPlatform {
            fn name(&self) -> &'static str {
                "rewriting"
            }
            fn ensure_binary_loadable(&self, path: &Path) -> Result<crate::compiler::Loadability> {
                let mut content = std::fs::read(path)?;
                content.extend_from_slice(b"signature");
                std::fs::write(path, content)?;
                // Claims a pass, so only the rewrite keeps it from the memo.
                Ok(crate::compiler::Loadability::Verified)
            }
            fn verified_loadable_dir(&self) -> Option<PathBuf> {
                Some(self.0.clone())
            }
            fn package_debug_bundle(
                &self,
                _binary: &Path,
                _staging_dir: &Path,
            ) -> Result<Option<PathBuf>> {
                Ok(None)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "4444444444444444444444444444444444444444444444444444444444444444";
        create_blob(&store, hash, b"\x7fELF unsigned");
        let cached = cached_file("libmac.so", hash);
        let target = dir.path().join("target").join("libmac.so");

        let restored = materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::DynamicLibrary,
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &RewritingPlatform(dir.path().join("verified")),
            "test restore",
            None,
        )
        .unwrap();

        assert_eq!(restored, RestoredBytes::Rewritten);
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"\x7fELF unsignedsignature",
            "the double should have rewritten the restored artifact"
        );
        assert!(
            !dir.path().join("verified").join(hash).exists(),
            "a check that changed the file proves nothing about the blob"
        );
    }

    /// Restore `hash` as a dynamic library at `target` through `platform`.
    fn restore_loadable(
        store: &Store,
        hash: &str,
        target: &Path,
        platform: &dyn crate::compiler::Platform,
    ) -> RestoredBytes {
        let anchor = target.parent().unwrap();
        materialize_cached_artifact(
            &BlobSource::Store(store),
            &cached_file("libmac.so", hash),
            target,
            ArtifactKind::DynamicLibrary,
            None,
            anchor,
            anchor,
            None,
            &[],
            platform,
            "test restore",
            None,
        )
        .unwrap()
    }

    /// A blob restored unchanged that passed the check once is not checked
    /// again on this host, and both restores stay exact.
    #[test]
    fn a_blob_that_passed_the_loadability_check_is_not_checked_again() {
        use crate::compiler::platform::tests::CountingPlatform;

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&test_config(dir.path().join("cache"))).unwrap();
        let hash = "5555555555555555555555555555555555555555555555555555555555555555";
        create_blob(&store, hash, b"\x7fELF signed");
        let memo = dir.path().join("verified");
        let platform = CountingPlatform::verifying_into(memo.clone());

        let first = restore_loadable(&store, hash, &dir.path().join("a/libmac.so"), &platform);
        assert_eq!(platform.ensure_calls(), 1);
        assert!(
            memo.join(hash).is_file(),
            "the pass is remembered by blob hash"
        );

        let second = restore_loadable(&store, hash, &dir.path().join("b/libmac.so"), &platform);
        assert_eq!(
            platform.ensure_calls(),
            1,
            "the second restore skips the check"
        );
        assert!(matches!(first, RestoredBytes::ExactBlobCopy(_)));
        assert!(matches!(second, RestoredBytes::ExactBlobCopy(_)));
    }

    /// Only a pass is remembered: a host that proved nothing checks every time.
    #[test]
    fn an_unproven_loadability_check_runs_on_every_restore() {
        struct Unproven(PathBuf, std::sync::atomic::AtomicUsize);
        impl crate::compiler::Platform for Unproven {
            fn name(&self) -> &'static str {
                "unproven"
            }
            fn ensure_binary_loadable(&self, _path: &Path) -> Result<crate::compiler::Loadability> {
                self.1.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(crate::compiler::Loadability::Unverified)
            }
            fn verified_loadable_dir(&self) -> Option<PathBuf> {
                Some(self.0.clone())
            }
            fn package_debug_bundle(
                &self,
                _binary: &Path,
                _staging_dir: &Path,
            ) -> Result<Option<PathBuf>> {
                Ok(None)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&test_config(dir.path().join("cache"))).unwrap();
        let hash = "6666666666666666666666666666666666666666666666666666666666666666";
        create_blob(&store, hash, b"\x7fELF unchecked");
        let platform = Unproven(dir.path().join("verified"), Default::default());

        restore_loadable(&store, hash, &dir.path().join("a/libmac.so"), &platform);
        restore_loadable(&store, hash, &dir.path().join("b/libmac.so"), &platform);
        assert_eq!(platform.1.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert!(!dir.path().join("verified").join(hash).exists());
    }

    // ── debug bundles (kunobi-ninja/kache#319) ───────────────────────

    /// The bundle is baked from the EXECUTABLE output, never a sibling
    /// artifact — an inverted classification match would hand dsymutil the
    /// dep-info file.
    #[test]
    fn find_executable_output_picks_the_binary_not_siblings() {
        let compiler = RustcCompiler::new();
        let args = rustc_args(&[
            "rustc",
            "src/main.rs",
            "--crate-name",
            "tool",
            "--crate-type",
            "bin",
            "--emit",
            "dep-info,link",
            "--out-dir",
            "target/debug/deps",
        ]);
        let artifacts = crate::compiler::ArtifactSet::new(vec![
            crate::compiler::Artifact {
                path: std::path::PathBuf::from("target/debug/deps/tool.d"),
                store_name: "tool.d".to_string(),
                kind: crate::compiler::ArtifactKind::DepInfo,
                required: false,
            },
            crate::compiler::Artifact {
                path: std::path::PathBuf::from("target/debug/deps/tool"),
                store_name: "tool".to_string(),
                kind: crate::compiler::ArtifactKind::Executable,
                required: true,
            },
        ]);
        let (path, name) = find_executable_output(&compiler, &args, &artifacts)
            .expect("the bin invocation has an executable output");
        assert_eq!(name, "tool");
        assert_eq!(path, std::path::PathBuf::from("target/debug/deps/tool"));
    }

    #[test]
    fn rustc_debuginfo_enabled_treats_absent_zero_and_none_as_off() {
        let base = ["rustc", "src/main.rs", "--crate-name", "foo"];
        // rustc's default is no debug info.
        assert!(!rustc_debuginfo_enabled(&rustc_args(&base)));
        // The two explicit "off" spellings.
        let mut with = base.to_vec();
        with.extend(["-C", "debuginfo=0"]);
        assert!(!rustc_debuginfo_enabled(&rustc_args(&with)));
        let mut with = base.to_vec();
        with.extend(["-C", "debuginfo=none"]);
        assert!(!rustc_debuginfo_enabled(&rustc_args(&with)));
    }

    #[test]
    fn rustc_debuginfo_enabled_recognizes_debug_levels() {
        let base = ["rustc", "src/main.rs", "--crate-name", "foo"];
        for level in ["1", "2", "line-tables-only"] {
            let mut with = base.to_vec();
            let opt = format!("debuginfo={level}");
            with.extend(["-C", &opt]);
            assert!(
                rustc_debuginfo_enabled(&rustc_args(&with)),
                "debuginfo={level} must count as debug info on"
            );
        }
        // `-g` desugars to `-Cdebuginfo=2` at parse time.
        let mut with = base.to_vec();
        with.push("-g");
        assert!(rustc_debuginfo_enabled(&rustc_args(&with)));
        // A later value wins over an earlier one (rustc's last-wins rule).
        let mut with = base.to_vec();
        with.extend(["-g", "-C", "debuginfo=0"]);
        assert!(!rustc_debuginfo_enabled(&rustc_args(&with)));
    }

    #[test]
    fn wants_debug_bundle_requires_user_facing_and_debuginfo() {
        // Both legs of the conjunction must hold — a lib with `-g` never
        // stores an executable, and a bin without `-g` has no DWARF for a
        // `.dSYM` to carry (#319).
        let bin_g = rustc_args(&[
            "rustc",
            "src/main.rs",
            "--crate-name",
            "foo",
            "--crate-type",
            "bin",
            "-g",
        ]);
        assert!(wants_debug_bundle(&bin_g));

        let test_g = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo", "--test", "-g"]);
        assert!(wants_debug_bundle(&test_g));

        let bin_nodebug = rustc_args(&[
            "rustc",
            "src/main.rs",
            "--crate-name",
            "foo",
            "--crate-type",
            "bin",
        ]);
        assert!(!wants_debug_bundle(&bin_nodebug));

        let lib_g = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--crate-type",
            "lib",
            "-g",
        ]);
        assert!(!wants_debug_bundle(&lib_g));
    }

    /// Tar bytes shaped like a store-time debug bundle: entries relative to
    /// the bundle root, the layout `unpack_debug_bundle` re-creates.
    fn debug_bundle_tar(dwarf_name: &str, dwarf: &[u8]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, content) in [
            ("Contents/Info.plist".to_string(), b"plist".as_slice()),
            (format!("Contents/Resources/DWARF/{dwarf_name}"), dwarf),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(0);
            header.set_entry_type(tar::EntryType::Regular);
            builder.append_data(&mut header, path, content).unwrap();
        }
        builder.into_inner().unwrap()
    }

    /// End-to-end restore of a cached DebugBundle artifact through the same
    /// `materialize_cached_artifact` path the wrapper's restore loop uses:
    /// the tar is hardlinked from the blob, then the external unpack action
    /// publishes the sibling `.dSYM` bundle (#319).
    #[test]
    fn materialize_cached_artifact_unpacks_debug_bundle_beside_binary() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        create_blob(&store, hash, &debug_bundle_tar("foo-abc123", b"dwarf!"));

        let cached = cached_file("foo-abc123.dsym.tar", hash);
        let deps = dir.path().join("target").join("debug").join("deps");
        std::fs::create_dir_all(&deps).unwrap();
        let target = deps.join("foo-abc123.dsym.tar");
        let platform = platform::current();

        materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::DebugBundle,
            None,
            dir.path(),
            dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap();

        // The tar is materialized (it is the cached artifact)...
        assert!(target.is_file(), "the bundle tar itself must be restored");
        // ...and the unpack action published the sibling bundle dir.
        let dwarf = deps.join("foo-abc123.dSYM/Contents/Resources/DWARF/foo-abc123");
        assert_eq!(std::fs::read(&dwarf).unwrap(), b"dwarf!");
    }

    /// macOS-only integration leg (no-op elsewhere): package a REAL `-g`
    /// binary's debug map into a bundle tar, restore that tar into a
    /// different directory via `materialize_cached_artifact`, and assert
    /// the restored `.dSYM`'s UUID equals the binary's. UUID identity is
    /// the exact criterion lldb uses to adopt an adjacent bundle, so this
    /// pins the property that makes the stale `N_OSO` records inert (#319).
    #[test]
    fn debug_bundle_round_trip_preserves_dwarf_uuid_on_macos() {
        if !cfg!(target_os = "macos") {
            return;
        }
        let dwarfdump_uuid = |path: &Path| -> String {
            let out = std::process::Command::new("dwarfdump")
                .arg("--uuid")
                .arg(path)
                .output()
                .expect("dwarfdump must be runnable on the macOS test host");
            let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
            // "UUID: <uuid> (<arch>) <path>" — take the UUID token.
            stdout
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_string()
        };

        // A real `-g` binary whose DWARF still lives in a per-build `.o` —
        // compile and link separately so that `.o` persists (the N_OSO debug
        // map shape this whole feature exists for).
        let build_dir = tempfile::tempdir().unwrap();
        let source = build_dir.path().join("hello.c");
        std::fs::write(&source, "int main(void) { return 0; }\n").unwrap();
        let object = build_dir.path().join("hello.o");
        let binary = build_dir.path().join("hello-bin");
        let compile = std::process::Command::new("cc")
            .args(["-g", "-c"])
            .arg(&source)
            .arg("-o")
            .arg(&object)
            .status()
            .expect("cc must be runnable on the macOS test host");
        assert!(compile.success(), "cc -g -c failed");
        let link = std::process::Command::new("cc")
            .arg(&object)
            .arg("-o")
            .arg(&binary)
            .status()
            .expect("cc link must be runnable on the macOS test host");
        assert!(link.success(), "cc link failed");

        // Store side: bake + tar the bundle while the `.o` exists.
        use crate::compiler::platform::Platform as _;
        let staging = tempfile::tempdir().unwrap();
        let tar_path = crate::compiler::platform::MacOsPlatform
            .package_debug_bundle(&binary, staging.path())
            .unwrap()
            .expect("macOS host must package a bundle for a -g binary");

        // Cache + restore side, in a directory the `.o` never existed in.
        let restore_dir = tempfile::tempdir().unwrap();
        let config = test_config(restore_dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let hash = crate::cache_key::hash_file(&tar_path).unwrap();
        let blob = store.blob_path(&hash);
        std::fs::create_dir_all(blob.parent().unwrap()).unwrap();
        std::fs::copy(&tar_path, &blob).unwrap();
        let cached = cached_file("hello-bin.dsym.tar", &hash);
        let deps = restore_dir.path().join("deps");
        std::fs::create_dir_all(&deps).unwrap();
        let target = deps.join("hello-bin.dsym.tar");
        let platform = platform::current();
        materialize_cached_artifact(
            &BlobSource::Store(&store),
            &cached,
            &target,
            ArtifactKind::DebugBundle,
            None,
            restore_dir.path(),
            restore_dir.path(),
            None,
            &[],
            &*platform,
            "test restore",
            None,
        )
        .unwrap();

        let bundle = deps.join("hello-bin.dSYM");
        let bundle_uuid = dwarfdump_uuid(&bundle);
        let binary_uuid = dwarfdump_uuid(&binary);
        assert!(
            !binary_uuid.is_empty(),
            "dwarfdump produced no UUID for the binary"
        );
        assert_eq!(
            bundle_uuid, binary_uuid,
            "restored .dSYM UUID must match the binary's — that match is \
             what makes lldb adopt the bundle over the stale debug map"
        );
    }

    // ── fallback wrapper ─────────────────────────────────────────────

    #[cfg(unix)]
    #[test]
    fn stripped_fallback_receives_incremental_disabled_env() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let fallback = dir.path().join("fallback");
        let env_dump = dir.path().join("incremental-env.txt");
        kache_fs::testutil::write_executable(
            &fallback,
            format!(
                "#!/bin/sh\nprintf '%s' \"${{CARGO_INCREMENTAL-unset}}\" > '{}'\nexit 0\n",
                env_dump.display()
            ),
        );

        let source = dir.path().join("lib.rs");
        std::fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
        let args = RustcArgs::parse(&[
            dir.path().join("missing-rustc").display().to_string(),
            source.display().to_string(),
            format!("-Cincremental={}", dir.path().join("incremental").display()),
        ])
        .unwrap();
        let compiler_args: Vec<String> = compile::strip_incremental_flags(&args.all_args)
            .into_iter()
            .cloned()
            .collect();
        let _incremental = TestEnvGuard::set("CARGO_INCREMENTAL", "1");

        let output = passthrough_args(&args, fallback.to_str(), &compiler_args, false).unwrap();
        assert!(output.fallback);
        assert_eq!(std::fs::read_to_string(env_dump).unwrap(), "0");
    }

    #[cfg(unix)]
    #[test]
    fn immediate_adaptive_compile_keeps_passthrough_remap_policy() {
        let dir = tempfile::tempdir().unwrap();
        let profile = dir.path().join("target/debug");
        let deps = profile.join("deps");
        let incremental = profile.join("incremental");
        std::fs::create_dir_all(&deps).unwrap();
        std::fs::create_dir(&incremental).unwrap();

        let source = dir.path().join("lib.rs");
        let rustc = dir.path().join("rustc");
        let argv_dump = dir.path().join("argv.txt");
        std::fs::write(&source, "pub fn answer() -> u8 { 42 }\n").unwrap();
        kache_fs::testutil::write_executable(
            &rustc,
            format!(
                r#"#!/bin/sh
printf '%s\n' "$@" > '{}'
incremental=
for arg in "$@"; do
    case "$arg" in
        -Cincremental=*) incremental=${{arg#-Cincremental=}} ;;
        --codegen=incremental=*) incremental=${{arg#--codegen=incremental=}} ;;
    esac
done
if [ -n "$incremental" ]; then
    mkdir -p "$incremental"
    printf 'state' > "$incremental/state.bin"
fi
exit 0
"#,
                argv_dump.display()
            ),
        );

        let mut args = RustcArgs::parse(&[
            rustc.display().to_string(),
            "--crate-name".to_string(),
            "adaptive_fixture".to_string(),
            "--crate-type".to_string(),
            "lib".to_string(),
            source.display().to_string(),
            "--out-dir".to_string(),
            deps.display().to_string(),
            "--emit=metadata".to_string(),
            "-Cextra-filename=-1234abcd".to_string(),
            format!("-Cincremental={}", incremental.display()),
        ])
        .unwrap();
        args.is_primary = true;
        args.path_normalize_disabled = false;

        let mut config = test_config(dir.path().join("cache"));
        config.base_dirs = vec![dir.path().display().to_string()];
        let guard = adaptive_policy_guard(&config);
        let unit = AdaptiveUnit::eligible(&args, true, &guard).unwrap();
        let lease = unit.try_immediate().unwrap();

        let exit = adaptive_incremental_with_event(
            &config,
            &args,
            "adaptive_fixture",
            &dir.path().display().to_string(),
            std::time::Instant::now(),
            lease,
            "adaptive passthrough",
            None,
        )
        .unwrap();
        assert_eq!(exit, 0);

        let argv = std::fs::read_to_string(argv_dump).unwrap();
        let rustc_incremental = argv
            .lines()
            .find_map(|arg| {
                arg.strip_prefix("-Cincremental=")
                    .or_else(|| arg.strip_prefix("--codegen=incremental="))
            })
            .expect("adaptive compilation did not receive an incremental directory");
        assert!(
            std::path::Path::new(rustc_incremental)
                .join("state.bin")
                .is_file(),
            "successful adaptive compilation discarded reusable rustc state"
        );

        assert!(
            !argv
                .lines()
                .any(|arg| arg.starts_with("--remap-path-prefix")),
            "an immediate passthrough unexpectedly injected remap arguments: {argv:?}"
        );
    }

    #[test]
    fn clean_path_collapses_dot_and_dotdot() {
        assert_eq!(clean_path(Path::new("a/./b")), PathBuf::from("a/b"));
        assert_eq!(clean_path(Path::new("a/b/../c")), PathBuf::from("a/c"));
        assert_eq!(clean_path(Path::new("./a/b")), PathBuf::from("a/b"));
        // A leading `..` with nothing to pop is preserved.
        assert_eq!(clean_path(Path::new("../a")), PathBuf::from("../a"));
        // Cleaning down to nothing yields ".".
        assert_eq!(clean_path(Path::new("a/..")), PathBuf::from("."));
        assert_eq!(clean_path(Path::new(".")), PathBuf::from("."));
    }

    #[cfg(unix)]
    #[test]
    fn clean_path_preserves_absolute_root() {
        assert_eq!(clean_path(Path::new("/a/./b/../c")), PathBuf::from("/a/c"));
    }

    #[test]
    fn absolute_clean_path_joins_relative_to_cwd() {
        let cwd = Path::new("/work/project");
        assert_eq!(
            absolute_clean_path(Path::new("src/../lib.rs"), cwd),
            PathBuf::from("/work/project/lib.rs")
        );
        // An already-absolute path ignores cwd but is still cleaned.
        assert_eq!(
            absolute_clean_path(Path::new("/etc/./hosts"), cwd),
            PathBuf::from("/etc/hosts")
        );
    }

    #[test]
    fn common_path_prefix_returns_shared_ancestor() {
        assert_eq!(
            common_path_prefix(Path::new("/a/b/c"), Path::new("/a/b/d")),
            Some(PathBuf::from("/a/b"))
        );
        assert_eq!(
            common_path_prefix(Path::new("/a/b"), Path::new("/a/b")),
            Some(PathBuf::from("/a/b"))
        );
    }

    #[test]
    fn common_path_prefix_none_when_nothing_shared() {
        // Different roots / first components share nothing.
        assert_eq!(common_path_prefix(Path::new("a/b"), Path::new("x/y")), None);
    }

    #[test]
    fn progress_label_gates_by_result_and_verbosity() {
        // Hits always show at level 1+.
        assert_eq!(progress_label(EventResult::LocalHit, 1), Some("local hit"));
        assert_eq!(
            progress_label(EventResult::PrefetchHit, 1),
            Some("prefetch hit")
        );
        assert_eq!(
            progress_label(EventResult::RemoteHit, 1),
            Some("remote hit")
        );
        assert_eq!(progress_label(EventResult::Error, 1), Some("error"));

        // Dup / Miss are suppressed at level 1 but shown at verbose level 2.
        assert_eq!(progress_label(EventResult::Dup, 1), None);
        assert_eq!(progress_label(EventResult::Miss, 1), None);
        assert_eq!(progress_label(EventResult::Dup, 2), Some("dup"));
        assert_eq!(progress_label(EventResult::Miss, 2), Some("miss"));

        // Passthrough / Skipped never produce a line, even when verbose.
        assert_eq!(progress_label(EventResult::Passthrough, 2), None);
        assert_eq!(progress_label(EventResult::Skipped, 2), None);
    }

    #[test]
    fn heartbeat_lines_require_verbose_progress() {
        assert!(!heartbeat_lines_enabled(0));
        assert!(!heartbeat_lines_enabled(1));
        assert!(heartbeat_lines_enabled(2));
    }

    /// `KACHE_PROGRESS` parsing is the only env-dependent part of progress
    /// output; the scoped guard keeps the process-global var restored.
    #[test]
    fn progress_level_parses_supported_env_values() {
        let _lock = crate::test_support::process_state_test_lock();
        let _guard = TestEnvGuard::remove("KACHE_PROGRESS");
        assert_eq!(progress_level(), 0);

        unsafe {
            std::env::set_var("KACHE_PROGRESS", "1");
        }
        assert_eq!(progress_level(), 1);
        unsafe {
            std::env::set_var("KACHE_PROGRESS", "hits");
        }
        assert_eq!(progress_level(), 1);
        unsafe {
            std::env::set_var("KACHE_PROGRESS", "verbose");
        }
        assert_eq!(progress_level(), 2);
        unsafe {
            std::env::set_var("KACHE_PROGRESS", "all");
        }
        assert_eq!(progress_level(), 2);
        unsafe {
            std::env::set_var("KACHE_PROGRESS", "nope");
        }
        assert_eq!(progress_level(), 0);
    }

    /// The probe-forwarder resolves a kache-wrapped `CC` without spawning it;
    /// `run_cc_probe` itself is left untested here because it runs a compiler.
    #[test]
    fn probe_forward_compiler_recovers_real_compiler_from_cc_env() {
        // The probe cache key fingerprints the WHOLE process environment
        // (`probe::cache::env_fingerprint`), so mutating `CC`/`TARGET` here
        // mid-flight flips a concurrently-running probe test's key and makes
        // its memoization assertion flake. Serialize behind the same lock the
        // env-mutating probe tests hold.
        let _lock = crate::config::config_path_lock();
        let self_stem = std::env::current_exe()
            .ok()
            .as_deref()
            .and_then(Path::file_stem)
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "kache".to_string());
        let wrapped = format!("{self_stem} clang");
        let _target = TestEnvGuard::remove("TARGET");
        let _cc = TestEnvGuard::set("CC", &wrapped);

        assert_eq!(probe_forward_compiler(), "clang");
    }

    #[test]
    fn event_result_for_store_put_maps_dup_vs_miss() {
        use crate::store::StorePutResult;
        // Every output blob was a duplicate -> Dup.
        let dup = StorePutResult {
            output_blobs: 2,
            duplicate_blobs: 2,
            new_blobs: 0,
        };
        assert!(matches!(event_result_for_store_put(dup), EventResult::Dup));
        // At least one new blob -> Miss.
        let partial = StorePutResult {
            output_blobs: 2,
            duplicate_blobs: 1,
            new_blobs: 1,
        };
        assert!(matches!(
            event_result_for_store_put(partial),
            EventResult::Miss
        ));
        // No output blobs -> not a full dup -> Miss.
        let empty = StorePutResult {
            output_blobs: 0,
            duplicate_blobs: 0,
            new_blobs: 0,
        };
        assert!(matches!(
            event_result_for_store_put(empty),
            EventResult::Miss
        ));
    }

    #[test]
    fn store_admission_preserves_writable_remote_publication() {
        let mut config = test_config(PathBuf::from("cache"));
        config.min_store_compile_ms = 1_000;

        assert!(!store_admits_compile(&config, 999, true));
        assert!(store_admits_compile(&config, 1_000, true));

        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        assert!(store_admits_compile(&config, 1, true));
        assert!(
            !store_admits_compile(&config, 1, false),
            "a path without remote publication must still apply local admission"
        );

        config.remote_readonly = true;
        assert!(!store_admits_compile(&config, 1, true));
    }

    #[test]
    fn disabled_store_admission_accepts_every_compile() {
        let config = test_config(PathBuf::from("cache"));
        assert!(store_admits_compile(&config, 0, false));
        assert!(store_admits_compile(&config, 5, true));
    }

    #[test]
    fn cc_admission_skip_is_reported_only_for_cacheable_outputs() {
        let put = StorePutResult::default();
        assert!(matches!(
            event_result_for_store_admission(true, false, put),
            EventResult::Skipped
        ));
        assert!(matches!(
            event_result_for_store_admission(false, false, put),
            EventResult::Miss
        ));
    }

    #[test]
    fn cc_store_decision_distinguishes_every_candidate_and_admission_state() {
        let cases = [
            (false, false, false, false),
            (false, true, false, false),
            (true, false, true, false),
            (true, true, false, true),
        ];

        for (candidate, admitted, admission_skipped, should_store) in cases {
            assert_eq!(
                cc_store_decision(candidate, admitted),
                CcStoreDecision {
                    admission_skipped,
                    should_store,
                },
                "candidate={candidate}, admitted={admitted}"
            );
        }
    }

    #[test]
    fn cc_store_gate_requires_success_and_artifacts() {
        let cases = [
            (0, true, true),
            (1, true, false),
            (0, false, false),
            (1, false, false),
        ];

        for (exit_code, has_artifacts, expected) in cases {
            assert_eq!(
                should_store_cc_result(exit_code, has_artifacts),
                expected,
                "exit={exit_code}, artifacts={has_artifacts}"
            );
        }
    }

    fn parse_cc(args: &[&str]) -> crate::compiler::cc::CcArgs {
        crate::compiler::cc::CcArgs::parse(&s(args)).unwrap()
    }

    /// `cc_event_root` honors the override exactly (pins whole-body
    /// mutants on a helper the diff would otherwise leave uncovered).
    #[test]
    fn cc_event_root_honors_override() {
        let _lock = crate::test_support::process_state_test_lock();
        let sentinel = if cfg!(windows) {
            r"C:\cc-root-sentinel"
        } else {
            "/cc-root-sentinel"
        };
        let _guard = TestEnvGuard::set("KACHE_EVENT_ROOT", sentinel);
        let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
        assert_eq!(
            cc_event_root(&parsed),
            std::path::PathBuf::from(sentinel)
                .to_string_lossy()
                .as_ref()
        );
    }

    fn spool_intent_count(config: &Config) -> usize {
        let dir = config.upload_spool_dir();
        match std::fs::read_dir(&dir) {
            Ok(entries) => entries.filter_map(Result::ok).count(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => panic!("reading upload spool {}: {error}", dir.display()),
        }
    }

    fn seed_cc_object_entry(store: &Store, cache_key: &str, dir: &Path) {
        let object = dir.join("foo.o");
        std::fs::write(&object, b"object bytes").unwrap();
        store
            .put_with_compile_time_independent(
                cache_key,
                "foo.c",
                &[],
                &[],
                "x86_64-unknown-linux-gnu",
                "",
                &[(object, "foo.o".to_string())],
                "",
                "",
                12,
            )
            .unwrap();
    }

    /// Keep auto-start from `send_upload_job` off the developer's daemon.
    /// Persist uses the test `Config`; daemon spawn reloads `KACHE_CONFIG`.
    fn isolate_daemon_autostart(dir: &Path) -> (TestEnvGuard, TestEnvGuard) {
        let config_path = dir.join("isolated-kache.toml");
        let cache_dir = dir.join("isolated-cache");
        std::fs::create_dir_all(&cache_dir).unwrap();
        let store = toml::Value::String(cache_dir.to_string_lossy().into_owned());
        std::fs::write(
            &config_path,
            format!(
                "[cache]\n\
                 local_only = true\n\
                 ignore_env = true\n\
                 local_store = {store}\n\
                 runtime_dir = {store}\n\
                 daemon_idle_timeout_secs = 1\n"
            ),
        )
        .unwrap();
        (
            TestEnvGuard::set(
                "KACHE_CONFIG",
                config_path.to_str().expect("utf-8 isolated config path"),
            ),
            TestEnvGuard::set(
                "KACHE_CACHE_DIR",
                cache_dir.to_str().expect("utf-8 isolated cache path"),
            ),
        )
    }

    /// Answers `RemoteCheck` with a fixed `found` flag. `send_remote_check`
    /// probes reachability before the real request, so the accept loop must
    /// survive empty connections. Local-only tests also use this listener to
    /// intercept accidental uploads without starting a real daemon.
    struct RemoteCheckReplyDaemon {
        stop: Arc<AtomicBool>,
        requests: Arc<AtomicUsize>,
        handle: Option<std::thread::JoinHandle<()>>,
        socket_path: PathBuf,
    }

    impl RemoteCheckReplyDaemon {
        fn spawn(socket_path: PathBuf, found: bool) -> Self {
            Self::with_reply(
                socket_path,
                serde_json::json!({ "ok": true, "found": found }),
            )
        }

        fn with_reply(socket_path: PathBuf, reply: serde_json::Value) -> Self {
            Self::with_delayed_reply(socket_path, reply, std::time::Duration::ZERO)
        }

        fn with_delayed_reply(
            socket_path: PathBuf,
            reply: serde_json::Value,
            delay: std::time::Duration,
        ) -> Self {
            let body = format!("{reply}\n");
            if let Some(parent) = socket_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            let name = socket_name(&socket_path).expect("fake remote-check socket name");
            let listener = ListenerOptions::new()
                .name(name)
                .create_sync()
                .expect("bind fake remote-check daemon");
            let stop = Arc::new(AtomicBool::new(false));
            let requests = Arc::new(AtomicUsize::new(0));
            let stop_thread = Arc::clone(&stop);
            let requests_thread = Arc::clone(&requests);
            let handle = std::thread::spawn(move || {
                use crate::transport::prelude::*;
                while !stop_thread.load(Ordering::SeqCst) {
                    let mut stream = match listener.accept() {
                        Ok(stream) => stream,
                        Err(_) => break,
                    };
                    if stop_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];
                    loop {
                        match stream.read(&mut chunk) {
                            Ok(0) => break,
                            Ok(n) => {
                                buf.extend_from_slice(&chunk[..n]);
                                if buf.contains(&b'\n') {
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                    if buf.is_empty() {
                        continue;
                    }
                    requests_thread.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(delay);
                    let _ = stream.write_all(body.as_bytes());
                }
            });
            Self {
                stop,
                requests,
                handle: Some(handle),
                socket_path,
            }
        }

        fn request_count(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
    }

    impl Drop for RemoteCheckReplyDaemon {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = crate::transport::is_reachable(&self.socket_path);
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn wait_until_reachable(path: &Path) {
        let start = std::time::Instant::now();
        while !crate::transport::is_reachable(path) {
            assert!(
                start.elapsed() < std::time::Duration::from_secs(2),
                "fake remote-check daemon did not become reachable at {}",
                path.display()
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn try_cc_remote_hit(
        config: &Config,
        store: &Store,
        parsed: &crate::compiler::cc::CcArgs,
        cache_key: &str,
    ) -> Option<i32> {
        cc_try_remote_hit(
            config,
            store,
            &CcCompiler::new(),
            parsed,
            &FileHasher::new(),
            cache_key,
            "foo.c",
            "foo.c",
            std::time::Instant::now(),
            0,
            0,
        )
        .unwrap()
    }

    #[test]
    fn remote_demand_wait_includes_success_and_failed_reply() {
        for ok in [true, false] {
            let _ = crate::demand::take();
            let dir = tempfile::tempdir().unwrap();
            let mut config = test_config(dir.path().join("cache"));
            config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
            let store = Store::open(&config).unwrap();
            let key = blake3::hash(b"remote-demand-wait").to_hex().to_string();
            seed_cc_object_entry(&store, &key, dir.path());
            let daemon = RemoteCheckReplyDaemon::with_delayed_reply(
                config.socket_path(),
                serde_json::json!({ "ok": ok, "found": true }),
                std::time::Duration::from_millis(25),
            );
            wait_until_reachable(&config.socket_path());
            let result = acquire_entry(
                &config,
                &store,
                &key,
                "foo.c",
                NegativeReply::ContinueCompile,
            );
            assert_eq!(result.is_some(), ok);
            let demands = crate::demand::take();
            assert_eq!(demands.len(), 1);
            assert_eq!(demands[0].cache_key, key);
            assert!(demands[0].first_demand_at_ms > 0);
            assert!(demands[0].remote_wait_ms >= 25);
            assert_eq!(daemon.request_count(), 1);
        }
    }

    #[test]
    fn remote_disabled_does_not_create_a_demand_or_wait() {
        let _ = crate::demand::take();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        assert!(
            acquire_entry(
                &config,
                &store,
                "key",
                "foo.c",
                NegativeReply::ContinueCompile
            )
            .is_none()
        );
        assert!(crate::demand::take().is_empty());
    }

    #[test]
    fn remote_acquisition_preserves_negative_reply_policy_and_provenance() {
        for (found, prefetched, policy, expected) in [
            (false, false, NegativeReply::ContinueCompile, None),
            (false, true, NegativeReply::ContinueCompile, None),
            (
                false,
                false,
                NegativeReply::CheckConcurrentEntry,
                Some(EventResult::LocalHit),
            ),
            (
                false,
                true,
                NegativeReply::CheckConcurrentEntry,
                Some(EventResult::LocalHit),
            ),
            (
                true,
                false,
                NegativeReply::ContinueCompile,
                Some(EventResult::RemoteHit),
            ),
            (
                true,
                true,
                NegativeReply::ContinueCompile,
                Some(EventResult::PrefetchHit),
            ),
            (
                true,
                false,
                NegativeReply::CheckConcurrentEntry,
                Some(EventResult::RemoteHit),
            ),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = test_config(dir.path().join("cache"));
            config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
            let store = Store::open(&config).unwrap();
            let key = blake3::hash(b"remote-entry-provenance")
                .to_hex()
                .to_string();
            seed_cc_object_entry(&store, &key, dir.path());
            let daemon = RemoteCheckReplyDaemon::with_reply(
                config.socket_path(),
                serde_json::json!({ "ok": true, "found": found, "prefetched": prefetched }),
            );
            wait_until_reachable(&config.socket_path());
            let result = acquire_entry(&config, &store, &key, "foo.c", policy);
            assert_eq!(result.as_ref().map(|(_, origin)| *origin), expected);
            if let Some((meta, _)) = result {
                assert_eq!(meta.cache_key, key);
                assert_eq!(meta.crate_name, "foo.c");
            }
            assert!(daemon.request_count() >= 1);
        }
    }

    #[test]
    fn remote_acquisition_requires_a_reply_and_readable_entry() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"remote-entry-validation")
            .to_hex()
            .to_string();
        seed_cc_object_entry(&store, &key, dir.path());
        assert!(
            acquire_entry(
                &config,
                &store,
                &key,
                "foo.c",
                NegativeReply::CheckConcurrentEntry
            )
            .is_none()
        );
        let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
        wait_until_reachable(&config.socket_path());
        let missing = blake3::hash(b"missing-entry").to_hex().to_string();
        assert!(
            acquire_entry(
                &config,
                &store,
                &missing,
                "foo.c",
                NegativeReply::ContinueCompile
            )
            .is_none()
        );
        std::fs::write(store.entry_dir(&key).join("meta.json"), b"invalid json").unwrap();
        assert!(
            acquire_entry(
                &config,
                &store,
                &key,
                "foo.c",
                NegativeReply::ContinueCompile
            )
            .is_none()
        );
        assert!(daemon.request_count() >= 2);
    }

    #[test]
    fn clang_cl_debug_does_not_bypass_local_admission() {
        let mut config = test_config(PathBuf::from("cache"));
        config.min_store_compile_ms = 1_000;
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));

        let cl_debug = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "/Z7"]);
        assert!(
            !store_admits_compile(&config, 1, cc_publishes_to_remote(&cl_debug)),
            "clang-cl debug must keep the local cheap-compile threshold"
        );

        let gnu = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
        assert!(
            store_admits_compile(&config, 1, cc_publishes_to_remote(&gnu)),
            "publishable cc compiles must store so they can upload"
        );
    }

    #[test]
    fn cc_remote_publication_skips_clang_cl_debug_only() {
        let gnu = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
        let gnu_debug = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o", "-g2"]);
        let cl = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj"]);
        let cl_debug = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "/Z7"]);
        let cl_g = parse_cc(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "-g"]);

        assert!(cc_publishes_to_remote(&gnu));
        assert!(cc_publishes_to_remote(&gnu_debug));
        assert!(cc_publishes_to_remote(&cl));
        assert!(!cc_publishes_to_remote(&cl_debug));
        assert!(!cc_publishes_to_remote(&cl_g));
    }

    #[test]
    fn cc_upload_enqueue_requires_a_configured_remote_and_publication() {
        let mut config = test_config(PathBuf::from("cache"));
        assert!(!compiler_remote_enabled(&config, true));
        assert!(!compiler_remote_enabled(&config, false));

        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        assert!(compiler_remote_enabled(&config, true));
        assert!(!compiler_remote_enabled(&config, false));

        config.remote_readonly = true;
        assert!(
            compiler_remote_enabled(&config, true),
            "readonly is enforced inside send_upload_job, matching rustc"
        );
    }

    #[test]
    fn daemon_handoff_preserves_the_wrappers_upload_policy() {
        let mut config = test_config(PathBuf::from("cache"));
        assert!(!compiler_upload_enabled(&config, true));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        assert!(compiler_upload_enabled(&config, true));
        assert!(!compiler_upload_enabled(&config, false));
        config.remote_readonly = true;
        assert!(!compiler_upload_enabled(&config, true));
    }

    #[test]
    fn cc_try_remote_hit_skips_the_daemon_when_enqueue_is_false() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"cc-remote-skip-enqueue").to_hex().to_string();
        seed_cc_object_entry(&store, &key, dir.path());

        let output = dir.path().join("restored.o");
        let output_arg = output.to_string_lossy().into_owned();
        let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", &output_arg]);

        let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
        wait_until_reachable(&config.socket_path());
        let requests_before = daemon.request_count();

        assert!(try_cc_remote_hit(&config, &store, &parsed, &key).is_none());
        assert_eq!(
            daemon.request_count(),
            requests_before,
            "enqueue=false must not send RemoteCheck"
        );
        assert!(
            !output.exists(),
            "skipping the daemon must not restore a local store entry"
        );
    }

    #[test]
    fn cc_try_remote_hit_does_not_restore_on_a_remote_miss() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"cc-remote-miss-no-restore")
            .to_hex()
            .to_string();
        seed_cc_object_entry(&store, &key, dir.path());

        let output = dir.path().join("restored.o");
        let output_arg = output.to_string_lossy().into_owned();
        let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", &output_arg]);

        let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
        wait_until_reachable(&config.socket_path());

        assert!(try_cc_remote_hit(&config, &store, &parsed, &key).is_none());
        assert!(
            daemon.request_count() >= 1,
            "a configured remote must still ask the daemon"
        );
        assert!(
            !output.exists(),
            "found=false must not restore even when the local store already has the entry"
        );
    }

    /// A declined hand-off from the main store re-claims the key; a peer
    /// that took it in the gap publishes, and the wrapper only logs its
    /// event.
    #[test]
    fn a_declined_hand_off_yields_to_a_peer_that_took_the_key() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"cc-handoff-peer").to_hex().to_string();
        let BuildClaim::Acquired(_peer) = store.claim_build(&key).unwrap() else {
            panic!("fresh key");
        };
        let object = dir.path().join("foo.o");
        std::fs::write(&object, b"object bytes").unwrap();
        let files = vec![(object, "foo.o".to_string())];
        let now = std::time::Instant::now();
        let handoff = CcHandoff {
            cache_key: &key,
            crate_name: "foo.c",
            target: "x86_64",
            files: &files,
            stdout: "",
            stderr: "",
            compile_time_ms: 5,
            publishes_to_remote: false,
            event_root: "",
            start: now,
            size: 12,
            key_ms: 0,
            lookup_ms: 0,
            lookup_rejection: "",
            store_start: now,
            memo: None,
        };

        // No daemon: the offer is declined and the key is the peer's.
        let mut lock = None;
        let outcome = hand_off_cc_store(&config, &store, &mut lock, handoff);

        assert!(matches!(outcome, CcHandoffOutcome::Done));
        assert!(lock.is_none());
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].result, EventResult::Miss);
        assert!(!events[0].store_handed_off);
    }

    #[test]
    fn cc_store_enqueues_an_upload_intent_when_a_writable_remote_is_configured() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"cc-upload-intent").to_hex().to_string();
        seed_cc_object_entry(&store, &key, dir.path());

        let _lock = crate::config::config_path_lock();
        let _isolated = isolate_daemon_autostart(dir.path());
        maybe_enqueue_upload(&config, &store, &key, "foo.c", true);

        assert_eq!(spool_intent_count(&config), 1);
        assert!(
            config
                .upload_spool_dir()
                .join(format!("{key}.json"))
                .is_file()
        );
    }

    #[test]
    fn clang_cl_debug_store_does_not_enqueue_an_upload_intent() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        let store = Store::open(&config).unwrap();
        let key = blake3::hash(b"cc-cl-debug-no-upload").to_hex().to_string();
        seed_cc_object_entry(&store, &key, dir.path());

        let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
        let _lock = crate::config::config_path_lock();
        let _isolated = isolate_daemon_autostart(dir.path());
        maybe_enqueue_upload(&config, &store, &key, "foo.c", false);

        assert_eq!(spool_intent_count(&config), 0);
    }

    #[test]
    fn cc_store_does_not_enqueue_when_remote_is_readonly_or_absent() {
        let dir = tempfile::tempdir().unwrap();
        let key = blake3::hash(b"cc-no-upload-gates").to_hex().to_string();

        let mut readonly = test_config(dir.path().join("readonly"));
        readonly.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        readonly.remote_readonly = true;
        let readonly_store = Store::open(&readonly).unwrap();
        seed_cc_object_entry(&readonly_store, &key, dir.path());

        let local = test_config(dir.path().join("local"));
        let local_store = Store::open(&local).unwrap();
        seed_cc_object_entry(&local_store, &key, dir.path());

        let _readonly_daemon = RemoteCheckReplyDaemon::spawn(readonly.socket_path(), false);
        let _local_daemon = RemoteCheckReplyDaemon::spawn(local.socket_path(), false);
        let _lock = crate::config::config_path_lock();
        let _isolated = isolate_daemon_autostart(dir.path());
        maybe_enqueue_upload(&readonly, &readonly_store, &key, "foo.c", true);
        maybe_enqueue_upload(&local, &local_store, &key, "foo.c", true);

        assert_eq!(spool_intent_count(&readonly), 0);
        assert_eq!(spool_intent_count(&local), 0);
    }

    #[test]
    fn cc_output_path_passthrough_allows_plain_files_but_refuses_symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output.o");

        assert!(!cc_output_path_requires_passthrough(&output));
        std::fs::write(&output, b"existing").unwrap();
        assert!(!cc_output_path_requires_passthrough(&output));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let dangling = dir.path().join("dangling.o");
            symlink(dir.path().join("missing-target"), &dangling).unwrap();
            assert!(cc_output_path_requires_passthrough(&dangling));
        }
    }

    #[cfg(unix)]
    #[test]
    fn cc_passthrough_forwards_the_original_arguments_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake_cc = dir.path().join("cc");
        let capture = dir.path().join("capture.c");
        let output = dir.path().join("output.o");
        let fallback = dir.path().join("fallback");
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        kache_fs::testutil::write_executable(
            &fake_cc,
            format!(
                "#!{}\ncapture=\"$1\"\nshift\nprintf '%s\\n' \"$@\" > \"$capture\"\n",
                shell.display()
            ),
        );
        kache_fs::testutil::write_executable(
            &fallback,
            format!("#!{}\nprintf 'fallback\\n' > \"$2\"\n", shell.display()),
        );
        std::fs::write(&output, b"existing output").unwrap();
        std::fs::set_permissions(&output, std::fs::Permissions::from_mode(0o444)).unwrap();

        let capture_arg = capture.to_string_lossy().into_owned();
        let output_arg = output.to_string_lossy().into_owned();
        let parsed = CcCompiler::new()
            .parse(&s(&[
                &fake_cc.to_string_lossy(),
                &capture_arg,
                "-c",
                "-o",
                &output_arg,
            ]))
            .unwrap();

        let mut config = test_config(dir.path().join("cache"));
        config.fallback = fallback.to_str().map(ToOwned::to_owned);
        let result = cc_passthrough(&config, &parsed).unwrap();

        assert_eq!(result.exit_code, 0);
        assert_eq!(
            std::fs::read_to_string(&capture).unwrap(),
            format!("-c\n-o\n{output_arg}\n"),
            "unsafe output passthrough must bypass fallback and cache-only flags"
        );
    }

    /// Phase 1 nvcc (#1024): a refused invocation runs the real compiler
    /// with the original argv and propagates its exit code — and records
    /// the passthrough reason. The nonzero exit kills the `Ok(0)` /
    /// `Ok(1)` / `Ok(-1)` body mutants in both `run_nvcc` and
    /// `nvcc_passthrough_with_event`; the sentinel root kills the
    /// `nvcc_event_root` value mutants.
    #[cfg(unix)]
    #[test]
    fn nvcc_passthrough_propagates_exit_code_and_reasons() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let fake_nvcc = dir.path().join("nvcc");
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        kache_fs::testutil::write_executable(
            &fake_nvcc,
            format!("#!{}\nexit 3\n", shell.display()),
        );

        let _root_guard = TestEnvGuard::set("KACHE_EVENT_ROOT", "/nvcc-phase1-root");
        let config = test_config(dir.path().join("cache"));
        let exit = run_nvcc(
            &config,
            &s(&[
                &fake_nvcc.to_string_lossy(),
                "-dlink",
                "a.o",
                "-o",
                "dlink.o",
            ]),
        )
        .unwrap();
        assert_eq!(exit, 3);

        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].root, "/nvcc-phase1-root");
        assert_eq!(events[0].result, EventResult::Passthrough);
        assert!(
            events[0].passthrough_reason.contains("device-link"),
            "unexpected reason: {}",
            events[0].passthrough_reason
        );
    }

    /// Write the fake nvcc toolchain with baked-in paths (no
    /// environment needed, so tests stay parallel): `nvcc` answers
    /// `--version`, `--dryrun` (naming `host`), `-M` (the source plus
    /// `headers`), and compiles by writing `-o`/`-MF` outputs while
    /// recording invocations, argv, and `SOURCE_DATE_EPOCH`. Only the
    /// exit codes (`exit_code`, `m_exit_code`) baked in as literals.
    #[cfg(unix)]
    #[allow(clippy::too_many_arguments)]
    fn write_fake_nvcc_toolchain(
        dir: &Path,
        source: &Path,
        headers: &str,
        version: &str,
        host_version: &str,
        count: &Path,
        argv_record: Option<&Path>,
        epoch_record: Option<&Path>,
        exit_code: i32,
        m_exit_code: i32,
    ) -> (PathBuf, PathBuf) {
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        let host = dir.join("host-gcc");
        kache_fs::testutil::write_executable(
            &host,
            format!(
                "#!{}\nprintf '%s\\n' '{hv}'\n",
                shell.display(),
                hv = host_version
            ),
        );
        let nvcc = dir.join("nvcc");
        let argv_line = argv_record
            .map(|p| {
                format!(
                    "if [ -n \"{p}\" ]; then printf '%s\\n' \"$@\" >> \"{p}\"; fi\n",
                    p = p.display()
                )
            })
            .unwrap_or_default();
        let epoch_line = epoch_record
            .map(|p| {
                format!(
                    "if [ -n \"{p}\" ]; then printf '%s\\n' \"${{SOURCE_DATE_EPOCH:-<unset>}}\" >> \"{p}\"; fi\n",
                    p = p.display()
                )
            })
            .unwrap_or_default();
        kache_fs::testutil::write_executable(
            &nvcc,
            format!(
                "#!{sh}\n\
                 if [ \"$1\" = \"--version\" ]; then\n\
                 printf '%s\\n' '{ver}'\n\
                 exit 0\n\
                 fi\n\
                 if [ \"$1\" = \"--dryrun\" ]; then\n\
                 printf '%s\\n' '#$ _HERE_=/usr/local/cuda/bin' '{host} -c -x c++'\n\
                 exit 0\n\
                 fi\n\
                 if [ \"$1\" = \"-M\" ]; then\n\
                 shift\n\
                 printf '%s:' \"fake.o\"\n\
                 printf ' %s' \"$1\"\n\
                 for f in {headers}; do printf ' %s' \"$f\"; done\n\
                 printf '\\n'\n\
                 exit {m_exit_code}\n\
                 fi\n\
                 printf 'run\\n' >> \"{count}\"\n\
                 {argv_line}                 {epoch_line}                 out=\"\"; dep=\"\"\n\
                 while [ $# -gt 0 ]; do\n\
                 case \"$1\" in\n\
                 -o) out=\"$2\"; shift 2;;\n\
                 -o*) out=\"${{1#-o}}\"; shift;;\n\
                 -MF) dep=\"$2\"; shift 2;;\n\
                 -MF*) dep=\"${{1#-MF}}\"; shift;;\n\
                 *) shift;;\n\
                 esac\n\
                 done\n\
                 printf 'object-bytes\\n' > \"$out\"\n\
                 if [ -n \"$dep\" ]; then printf '%s: %s %s\\n' \"$out\" \"{source}\" \"{headers}\" > \"$dep\"; fi\n\
                 exit {exit_code}\n",
                sh = shell.display(),
                host = host.display(),
                source = source.display(),
                count = count.display(),
                ver = version,
                headers = headers,
            ),
        );
        (nvcc, host)
    }

    /// One standard fake-toolchain project: `work/kernel.cu` including
    /// `work/inc/h.h`, the toolchain scripts, and the standard compile
    /// argv. All paths are baked into the scripts, so tests need no
    /// environment and run parallel. `argv_record` / `epoch_record` name
    /// optional record files under `dir` (argv capture, epoch capture).
    /// Returns `(work, nvcc, count, argv)`.
    #[cfg(unix)]
    #[allow(clippy::too_many_arguments)]
    fn setup_nvcc_case(
        dir: &tempfile::TempDir,
        argv_record: Option<&str>,
        epoch_record: Option<&str>,
        exit_code: i32,
        m_exit_code: i32,
    ) -> (PathBuf, PathBuf, PathBuf, Vec<String>) {
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(
            work.join("kernel.cu"),
            "#include \"inc/h.h\"\n__global__ void k() {}\n",
        )
        .unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
        let count = dir.path().join("count");
        let argv_path = argv_record.map(|name| dir.path().join(name));
        let epoch_path = epoch_record.map(|name| dir.path().join(name));
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            work.join("inc/h.h").to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            argv_path.as_deref(),
            epoch_path.as_deref(),
            exit_code,
            m_exit_code,
        );
        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        (work, nvcc, count, argv)
    }

    /// A fake-toolchain nvcc invocation over absolute tempdir paths (the
    /// wrapper never chdirs in tests). Returns the argv vector.
    #[cfg(unix)]
    fn nvcc_compile_argv(nvcc: &Path, work: &Path, extra: &[&str]) -> Vec<String> {
        let mut argv = vec![
            nvcc.to_string_lossy().into_owned(),
            "-c".to_string(),
            work.join("kernel.cu").to_string_lossy().into_owned(),
            "-o".to_string(),
            work.join("kernel.o").to_string_lossy().into_owned(),
            "-MF".to_string(),
            work.join("kernel.d").to_string_lossy().into_owned(),
            format!("-I{}", work.join("inc").display()),
            "-DUSE_CUDA".to_string(),
            "-gencode".to_string(),
            "arch=compute_80,code=sm_80".to_string(),
        ];
        argv.extend(extra.iter().map(|e| e.to_string()));
        argv
    }

    /// Miss, then hit: the second identical invocation restores the
    /// object and the dep-info without reforking the compiler, and the
    /// dep-info round-trips byte-identically (store relativize +
    /// restore expand are inverses under one anchor).
    #[cfg(unix)]
    #[test]
    fn nvcc_miss_then_hit_round_trips_object_and_depinfo() {
        // Pinned epoch: Nix builders (and reproducibility-minded
        // developers) export SOURCE_DATE_EPOCH, which the wrapper
        // honors verbatim — fix both epoch inputs for determinism.
        let _lock = crate::test_support::process_state_test_lock();
        let _epoch_env = TestEnvGuard::remove("SOURCE_DATE_EPOCH");
        let _epoch_knob = TestEnvGuard::remove("KACHE_NVCC_SOURCE_DATE_EPOCH");
        let dir = tempfile::tempdir().unwrap();
        let (work, _nvcc, count, argv) = setup_nvcc_case(&dir, Some("argv"), Some("epoch"), 0, 0);
        let argv_file = dir.path().join("argv");
        let epoch_file = dir.path().join("epoch");
        let config = test_config(dir.path().join("cache"));
        let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            spool_intent_count(&config),
            0,
            "local-only compile queued an upload"
        );
        assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");
        assert_eq!(
            std::fs::read_to_string(work.join("kernel.o")).unwrap(),
            "object-bytes\n"
        );
        // The execute path injected the prefix maps and pinned the epoch.
        let recorded_argv = std::fs::read_to_string(&argv_file).unwrap();
        assert!(
            recorded_argv.contains("-ffile-prefix-map="),
            "missing prefix-map injection in: {recorded_argv}"
        );
        assert_eq!(std::fs::read_to_string(&epoch_file).unwrap(), "0\n");
        let stored_depinfo = std::fs::read(work.join("kernel.d")).unwrap();

        std::fs::remove_file(work.join("kernel.o")).unwrap();
        std::fs::remove_file(work.join("kernel.d")).unwrap();
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\n",
            "hit must not refork the compiler"
        );
        assert_eq!(
            std::fs::read(work.join("kernel.o")).unwrap(),
            b"object-bytes\n"
        );
        assert_eq!(
            std::fs::read(work.join("kernel.d")).unwrap(),
            stored_depinfo
        );

        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].result, EventResult::Miss);
        assert_eq!(events[1].result, EventResult::LocalHit);
        // No remote configured: nothing is queued for upload.
        assert_eq!(spool_intent_count(&config), 0);
    }

    /// A dep-info the entry lacks evicts and recompiles: an object-only
    /// entry cannot satisfy `-MF`, and the recompiled entry (object +
    /// dep-info) hits afterwards.
    #[cfg(unix)]
    #[test]
    fn nvcc_depinfo_demand_evicts_and_recompiles() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let (work, nvcc, count, _) = setup_nvcc_case(&dir, None, None, 0, 0);
        let config = test_config(dir.path().join("cache"));
        let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

        // Without -MF: object-only entry.
        let bare = vec![
            nvcc.to_string_lossy().into_owned(),
            "-c".to_string(),
            work.join("kernel.cu").to_string_lossy().into_owned(),
            "-o".to_string(),
            work.join("kernel.o").to_string_lossy().into_owned(),
        ];
        assert_eq!(run_nvcc(&config, &bare).unwrap(), 0);
        assert_eq!(
            spool_intent_count(&config),
            0,
            "local-only compile queued an upload"
        );
        // With -MF: the object-only entry cannot satisfy it — evict,
        // recompile, store both artifacts…
        let with_dep = nvcc_compile_argv(&nvcc, &work, &[]);
        assert_eq!(run_nvcc(&config, &with_dep).unwrap(), 0);
        // …and the combined entry hits.
        std::fs::remove_file(work.join("kernel.o")).unwrap();
        std::fs::remove_file(work.join("kernel.d")).unwrap();
        assert_eq!(run_nvcc(&config, &with_dep).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\n",
            "exactly two compiles: miss, then evict-and-recompile"
        );
        assert!(work.join("kernel.o").is_file());
        assert!(work.join("kernel.d").is_file());
    }

    /// A configured remote without a reachable daemon falls through to
    /// the local path: check (absent) → compile → queued upload intent.
    #[cfg(unix)]
    #[test]
    fn nvcc_remote_absent_daemon_falls_through_to_compile() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
        let count = dir.path().join("count");
        let headers = work.join("inc").join("h.h");
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            headers.to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            None,
            None,
            0,
            0,
        );

        let _isolated = isolate_daemon_autostart(dir.path());
        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));

        // A reachable fake daemon answers the upload send instantly, so
        // no real daemon is auto-started (that path costs ~15s and would
        // leak a daemon onto the machine).
        let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
        wait_until_reachable(&config.socket_path());

        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");
        assert_eq!(spool_intent_count(&config), 1);
        assert!(
            daemon.request_count() >= 1,
            "the upload send must reach the daemon"
        );
    }

    /// Remote-hit plumbing without a real download: seed the entry
    /// with a live miss, then drive `nvcc_try_remote_hit` against the
    /// fake daemon directly (mirrors the cc remote-hit tests — the
    /// seeded local entry stands in for the download).
    #[cfg(unix)]
    fn seed_nvcc_entry(
        config: &Config,
        argv: &[String],
    ) -> (Store, crate::compiler::nvcc::NvccArgs, String) {
        let store = Store::open(config).unwrap();
        let compiler =
            NvccCompiler::with_extra_allowlist_flags(config.cc_extra_allowlist_flags.clone());
        let parsed = compiler.parse(argv).unwrap();
        assert_eq!(run_nvcc(config, argv).unwrap(), 0);
        let file_hasher = FileHasher::new();
        let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
        let ctx = KeyCtx {
            file_hasher: &file_hasher,
            path_normalizer: &path_normalizer,
            cache_dir: &config.cache_dir,
            key_salt: config.key_salt.as_deref(),
            key_env_vars: &config.key_env_vars,
            extra_inputs_digest: None,
        };
        let key = compiler.cache_key(&parsed, &ctx).unwrap();
        (store, parsed, key)
    }

    /// No configured remote: the daemon is never contacted and the
    /// local entry is left alone.
    #[cfg(unix)]
    #[test]
    fn nvcc_try_remote_hit_skips_daemon_without_remote() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

        let count = dir.path().join("count");
        let headers = work.join("inc").join("h.h");
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            headers.to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            None,
            None,
            0,
            0,
        );

        let config = test_config(dir.path().join("cache"));
        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
        wait_until_reachable(&config.socket_path());
        let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
        assert_eq!(
            spool_intent_count(&config),
            0,
            "seeding must remain local-only"
        );
        std::fs::remove_file(work.join("kernel.o")).unwrap();
        std::fs::remove_file(work.join("kernel.d")).unwrap();

        let requests_before = daemon.request_count();
        let start = std::time::Instant::now();

        assert!(
            nvcc_try_remote_hit(
                &config,
                &store,
                &parsed,
                &key,
                "kernel.cu",
                "/nvcc-root",
                start,
                0,
                0,
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(
            daemon.request_count(),
            requests_before,
            "no remote must mean no RemoteCheck"
        );
        assert!(
            !work.join("kernel.o").exists(),
            "skipping the daemon must not restore"
        );
    }

    /// A remote miss restores nothing, even with a seeded local entry —
    /// but the daemon is asked.
    #[cfg(unix)]
    #[test]
    fn nvcc_try_remote_hit_does_not_restore_on_remote_miss() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

        let count = dir.path().join("count");
        let headers = work.join("inc").join("h.h");
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            headers.to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            None,
            None,
            0,
            0,
        );

        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        // Up before the seed: the seeding miss uploads, and the send
        // must reach a daemon instantly instead of auto-starting a real
        // one (~15s, plus a stray daemon on the machine).
        let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);
        wait_until_reachable(&config.socket_path());
        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
        std::fs::remove_file(work.join("kernel.o")).unwrap();
        std::fs::remove_file(work.join("kernel.d")).unwrap();

        let start = std::time::Instant::now();

        assert!(
            nvcc_try_remote_hit(
                &config,
                &store,
                &parsed,
                &key,
                "kernel.cu",
                "/nvcc-root",
                start,
                0,
                0,
            )
            .unwrap()
            .is_none()
        );
        assert!(
            daemon.request_count() >= 1,
            "a configured remote must ask the daemon"
        );
        assert!(
            !work.join("kernel.o").exists(),
            "found=false must not restore even with a seeded entry"
        );
    }

    /// A found remote entry restores without compiling and reports
    /// RemoteHit.
    #[cfg(unix)]
    #[test]
    fn nvcc_try_remote_hit_restores_on_found() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

        let count = dir.path().join("count");
        let headers = work.join("inc").join("h.h");
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            headers.to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            None,
            None,
            0,
            0,
        );

        let mut config = test_config(dir.path().join("cache"));
        config.remote = Some(crate::config::RemoteConfig::test_s3("bucket", "artifacts"));
        // Up before the seed (see the miss test): the empty store keeps
        // the seeding compile a miss even with found=true.
        let daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), true);
        wait_until_reachable(&config.socket_path());
        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
        std::fs::remove_file(work.join("kernel.o")).unwrap();
        std::fs::remove_file(work.join("kernel.d")).unwrap();

        let start = std::time::Instant::now();

        assert_eq!(
            nvcc_try_remote_hit(
                &config,
                &store,
                &parsed,
                &key,
                "kernel.cu",
                "/nvcc-root",
                start,
                0,
                0,
            )
            .unwrap(),
            Some(0)
        );
        assert!(work.join("kernel.o").is_file());
        assert!(work.join("kernel.d").is_file());
        assert!(
            daemon.request_count() >= 1,
            "a configured remote must ask the daemon"
        );
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.last().unwrap().result, EventResult::RemoteHit);
    }

    /// A failing compile stores nothing and propagates the exit code:
    /// the rerun compiles again.
    #[cfg(unix)]
    #[test]
    fn nvcc_failed_compile_stores_nothing() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let (_work, _nvcc, count, argv) = setup_nvcc_case(&dir, None, None, 1, 0);
        let config = test_config(dir.path().join("cache"));

        assert_eq!(run_nvcc(&config, &argv).unwrap(), 1);
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 1);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\n",
            "failures must never store"
        );
    }

    /// A failing key (here: `-M` exits) passes through to a live
    /// compile and stores nothing.
    #[cfg(unix)]
    #[test]
    fn nvcc_key_failure_passes_through_uncached() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let (_work, _nvcc, count, argv) = setup_nvcc_case(&dir, None, None, 0, 1);
        let config = test_config(dir.path().join("cache"));
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\n",
            "key failures must never store"
        );
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert!(
            events.iter().all(|e| e.result == EventResult::Passthrough
                && e.passthrough_reason.contains("uncacheable")),
            "key failures pass through with reason"
        );
    }

    /// Key sensitivity: a header edit and a flag change each bust the
    /// key (closure contents and verbatim flags are folded).
    #[cfg(unix)]
    #[test]
    fn nvcc_header_and_flag_edits_bust_the_key() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(work.join("kernel.cu"), "#include \"inc/h.h\"\n").unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
        let count = dir.path().join("count");
        let headers = work.join("inc").join("h.h");
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            headers.to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            None,
            None,
            0,
            0,
        );

        let config = test_config(dir.path().join("cache"));
        let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            spool_intent_count(&config),
            0,
            "local-only compile queued an upload"
        );
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");

        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n// edit\n").unwrap();
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\n",
            "header edit must bust the key"
        );

        let changed = nvcc_compile_argv(&nvcc, &work, &["-DOTHER"]);
        assert_eq!(run_nvcc(&config, &changed).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\nrun\n",
            "flag change must bust the key"
        );
    }

    /// Invisible driver inputs join the key: setting
    /// `NVCC_PREPEND_FLAGS` busts it (miss, then hit under the new
    /// environment), while smuggled preprocessor inputs pass through
    /// uncached instead of miscaching.
    #[cfg(unix)]
    #[test]
    fn nvcc_driver_env_joins_the_key() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let (_work, _nvcc, count, argv) = setup_nvcc_case(&dir, None, None, 0, 0);
        let config = test_config(dir.path().join("cache"));
        let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            spool_intent_count(&config),
            0,
            "local-only compile queued an upload"
        );
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(std::fs::read_to_string(&count).unwrap(), "run\n");

        let _prepend = TestEnvGuard::set("NVCC_PREPEND_FLAGS", "-O2");
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\n",
            "driver env change must bust the key"
        );
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\n",
            "same environment must hit"
        );
        drop(_prepend);

        let _smuggled = TestEnvGuard::set("NVCC_PREPEND_FLAGS", "-I/secret");
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\nrun\n",
            "smuggled preprocessor inputs must recompile, never store"
        );
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert!(
            events
                .last()
                .unwrap()
                .passthrough_reason
                .contains("uncacheable"),
            "smuggled inputs pass through with reason"
        );
    }

    /// A too-cheap compile is skipped (never stored): the rerun
    /// compiles again.
    #[cfg(unix)]
    #[test]
    fn nvcc_admission_skipped_when_too_cheap() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();
        let count = dir.path().join("count");
        let headers = work.join("inc").join("h.h");
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            headers.to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            None,
            None,
            0,
            0,
        );

        let mut config = test_config(dir.path().join("cache"));
        config.min_store_compile_ms = u64::MAX;

        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(run_nvcc(&config, &argv).unwrap(), 0);
        assert_eq!(
            std::fs::read_to_string(&count).unwrap(),
            "run\nrun\n",
            "skipped compiles must never store"
        );
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert!(
            events.iter().all(|e| e.result == EventResult::Skipped),
            "cheap compiles report Skipped"
        );
    }

    /// Compiling over an output that still shares a read-only cache
    /// blob refuses loudly instead of failing with EACCES (or worse).
    #[cfg(unix)]
    #[test]
    fn nvcc_legacy_blob_check_refuses_shared_inode() {
        let _lock = crate::test_support::process_state_test_lock();
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let content = b"cached object";
        let hash = blake3::hash(content).to_hex().to_string();
        create_blob(&store, &hash, content);
        let blob = store.blob_path(&hash);
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o444)).unwrap();

        let output = dir.path().join("kernel.o");
        std::fs::hard_link(&blob, &output).unwrap();
        drop(store);

        let nvcc = dir.path().join("nvcc");
        let argv = vec![
            nvcc.to_string_lossy().into_owned(),
            "-c".to_string(),
            "kernel.cu".to_string(),
            "-o".to_string(),
            output.to_string_lossy().into_owned(),
        ];
        let err = run_nvcc(&config, &argv).unwrap_err();
        assert!(
            format!("{err:#}").contains("read-only cache blob"),
            "unexpected error: {err:#}"
        );
        assert_eq!(
            output.metadata().unwrap().ino(),
            blob.metadata().unwrap().ino()
        );
    }

    /// Entry/invocation compatibility, all four combinations: the
    /// object is always required; the dep-info only when requested.
    #[test]
    fn nvcc_cache_entry_rejection_covers_all_combinations() {
        let with_dep = |extra: &[&str]| {
            let mut argv = vec!["nvcc", "-c", "k.cu", "-o", "k.o"];
            argv.extend(extra);
            NvccCompiler::with_extra_allowlist_flags(Vec::new())
                .parse(&argv.iter().map(|a| a.to_string()).collect::<Vec<_>>())
                .unwrap()
        };
        let requested = with_dep(&["-MF", "k.d"]);
        let unrequested = with_dep(&[]);
        let object_only = entry_meta_with_files(&["k.o"]);
        let both = entry_meta_with_files(&["k.o", "k.d"]);
        let dep_only = entry_meta_with_files(&["k.d"]);
        let empty = entry_meta_with_files(&[]);

        assert!(nvcc_cache_entry_rejection_reason(&requested, &both).is_none());
        assert!(nvcc_cache_entry_rejection_reason(&unrequested, &object_only).is_none());
        assert!(nvcc_cache_entry_rejection_reason(&unrequested, &both).is_none());
        assert!(nvcc_cache_entry_rejection_reason(&requested, &object_only).is_some());
        assert!(nvcc_cache_entry_rejection_reason(&requested, &dep_only).is_some());
        assert!(nvcc_cache_entry_rejection_reason(&requested, &empty).is_some());
        assert!(nvcc_cache_entry_rejection_reason(&unrequested, &empty).is_some());
    }

    /// A cached entry whose blob is gone fails the restore loudly
    /// (fail-closed) instead of restoring thin air.
    #[cfg(unix)]
    #[test]
    fn nvcc_restore_fails_closed_on_missing_blob() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(work.join("inc")).unwrap();
        std::fs::write(work.join("kernel.cu"), "__global__ void k() {}\n").unwrap();
        std::fs::write(work.join("inc").join("h.h"), "#pragma once\n").unwrap();

        let count = dir.path().join("count");
        let headers = work.join("inc").join("h.h");
        let (nvcc, _host) = write_fake_nvcc_toolchain(
            dir.path(),
            &work.join("kernel.cu"),
            headers.to_str().unwrap(),
            "nvcc: NVIDIA (R) Cuda compiler driver",
            "gcc (GCC) 13.2.0",
            &count,
            None,
            None,
            0,
            0,
        );

        let config = test_config(dir.path().join("cache"));
        let _daemon = RemoteCheckReplyDaemon::spawn(config.socket_path(), false);

        let argv = nvcc_compile_argv(&nvcc, &work, &[]);
        let (store, parsed, key) = seed_nvcc_entry(&config, &argv);
        assert_eq!(
            spool_intent_count(&config),
            0,
            "local-only compile queued an upload"
        );

        let meta = store.get(&key).unwrap().unwrap();
        for file in &meta.files {
            std::fs::remove_file(store.blob_path(&file.hash)).unwrap();
        }
        let err = restore_nvcc_from_cache(&store, &parsed, &meta).unwrap_err();
        assert!(
            format!("{err:#}").contains("evicted"),
            "unexpected error: {err:#}"
        );
    }

    /// The env-reading wrapper honors the same anchor rule against the
    /// real working directory (kills whole-body mutants on the wrapper
    /// that the `_from_cwd` tests cannot see).
    #[test]
    fn nvcc_depinfo_rewrite_root_uses_current_dir() {
        let _lock = crate::test_support::process_state_test_lock();
        let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .parse(&[
                "nvcc".to_string(),
                "-c".to_string(),
                "src/k.cu".to_string(),
                "-o".to_string(),
                "build/k.o".to_string(),
                "-MF".to_string(),
                "build/k.d".to_string(),
            ])
            .unwrap();
        let cwd = std::env::current_dir().unwrap();
        let anchor = nvcc_depinfo_rewrite_root(&parsed).unwrap();
        assert!(
            anchor.starts_with(&cwd),
            "anchor {anchor:?} must live under the current dir {cwd:?}"
        );
        assert_eq!(
            Some(anchor),
            nvcc_depinfo_rewrite_root_from_cwd(&parsed, &cwd)
        );
    }

    /// Dep-info anchors: same-tree outputs anchor on the common prefix,
    /// disjoint trees fall back to the object dir, and no dep-info
    /// request needs no anchor at all.
    #[test]
    fn nvcc_depinfo_rewrite_root_anchors() {
        let _lock = crate::test_support::process_state_test_lock();
        let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .parse(&[
                "nvcc".to_string(),
                "-c".to_string(),
                "src/k.cu".to_string(),
                "-o".to_string(),
                "build/k.o".to_string(),
                "-MF".to_string(),
                "build/k.d".to_string(),
            ])
            .unwrap();
        let cwd = Path::new("/work");
        assert_eq!(
            nvcc_depinfo_rewrite_root_from_cwd(&parsed, cwd),
            Some(PathBuf::from("/work"))
        );

        let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .parse(&[
                "nvcc".to_string(),
                "-c".to_string(),
                "src/k.cu".to_string(),
                "-o".to_string(),
                "/elsewhere/k.o".to_string(),
                "-MF".to_string(),
                "/elsewhere/k.d".to_string(),
            ])
            .unwrap();
        assert_eq!(
            nvcc_depinfo_rewrite_root_from_cwd(&parsed, cwd),
            Some(PathBuf::from("/elsewhere"))
        );

        let parsed = crate::compiler::nvcc::NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .parse(&["nvcc".to_string(), "-c".to_string(), "k.cu".to_string()])
            .unwrap();
        assert_eq!(nvcc_depinfo_rewrite_root_from_cwd(&parsed, cwd), None);
    }

    /// #1015: a fallback wrapper such as sccache keys on the same preprocessor
    /// output, so it would serve the stale object kache just refused to cache.
    /// Only that refusal skips it; other key failures keep the fallback.
    #[test]
    fn only_hidden_input_key_failures_skip_the_fallback() {
        let hidden = anyhow::Error::new(crate::compiler::cc::CcHiddenInput {
            construct: ".incbin",
        });
        assert!(cc_key_error_skips_fallback(&hidden));
        assert!(!cc_key_error_skips_fallback(&anyhow::anyhow!(
            "cc -E key probe exited 1"
        )));
    }

    #[test]
    fn untrusted_codegen_backend_bypasses_unless_trusted_and_keyable() {
        assert_eq!(untrusted_codegen_backend(None, false), None);
        assert_eq!(untrusted_codegen_backend(None, true), None);
        assert_eq!(
            untrusted_codegen_backend(Some("/b/backend.so"), false),
            Some("/b/backend.so"),
            "an untrusted backend always bypasses"
        );
        assert_eq!(
            untrusted_codegen_backend(Some("/b/backend.so"), true),
            None,
            "a trusted backend passed as a path is cached"
        );
        assert_eq!(
            untrusted_codegen_backend(Some("backend.so"), true),
            Some("backend.so"),
            "a trusted bare file name cannot be keyed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn cc_direct_passthrough_bypasses_configured_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let fake_cc = dir.path().join("cc");
        let fallback = dir.path().join("fallback");
        let compiler_marker = dir.path().join("compiler-ran");
        let fallback_marker = dir.path().join("fallback-ran");
        let output = dir.path().join("output.o");
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");

        kache_fs::testutil::write_executable(
            &fake_cc,
            format!(
                "#!{}\nprintf direct > '{}'\n",
                shell.display(),
                compiler_marker.display()
            ),
        );
        kache_fs::testutil::write_executable(
            &fallback,
            format!(
                "#!{}\nprintf fallback > '{}'\n",
                shell.display(),
                fallback_marker.display()
            ),
        );

        let parsed = CcCompiler::new()
            .parse(&s(&[
                &fake_cc.to_string_lossy(),
                "-c",
                "foo.c",
                "-o",
                &output.to_string_lossy(),
            ]))
            .unwrap();
        assert!(
            !parsed.requires_compiler_output_semantics(),
            "the fallback branch must be eligible except for force_direct"
        );

        let mut config = test_config(dir.path().join("cache"));
        config.fallback = fallback.to_str().map(ToOwned::to_owned);
        let result = cc_direct_passthrough(&config, &parsed).unwrap();

        assert_eq!(result.exit_code, 0);
        assert!(!result.fallback);
        assert!(compiler_marker.exists());
        assert!(!fallback_marker.exists());
    }

    #[cfg(unix)]
    #[test]
    fn cc_direct_passthrough_refuses_legacy_cache_blob_hardlink() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let content = b"cached object";
        let hash = blake3::hash(content).to_hex().to_string();
        create_blob(&store, &hash, content);
        let blob = store.blob_path(&hash);
        std::fs::set_permissions(&blob, std::fs::Permissions::from_mode(0o444)).unwrap();

        let output = dir.path().join("output.o");
        std::fs::hard_link(&blob, &output).unwrap();
        drop(store);
        let index_path = config.index_db_path();
        std::fs::remove_file(&index_path).unwrap();
        std::fs::create_dir(&index_path).unwrap();

        let marker = dir.path().join("compiler-ran");
        let fake_cc = dir.path().join("cc");
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        kache_fs::testutil::write_executable(
            &fake_cc,
            format!(
                "#!{}\nprintf ran > '{}'\n",
                shell.display(),
                marker.display()
            ),
        );

        let parsed = CcCompiler::new()
            .parse(&s(&[
                &fake_cc.to_string_lossy(),
                "-c",
                "foo.c",
                "-o",
                &output.to_string_lossy(),
            ]))
            .unwrap();
        let error = cc_direct_passthrough(&config, &parsed).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("shares the read-only cache blob")
        );
        assert!(
            !marker.exists(),
            "compiler must not run over a shared blob inode"
        );
        assert!(
            index_path.is_dir(),
            "the direct-mode safety check must not open or repair the cache index"
        );
        assert_eq!(std::fs::read(&blob).unwrap(), content);
        assert_eq!(
            std::fs::metadata(&blob).unwrap().ino(),
            std::fs::metadata(&output).unwrap().ino()
        );
    }

    #[test]
    fn local_daemon_fast_path_records_demand_before_reply() {
        let _ = crate::demand::take();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        assert!(
            crate::daemon::send_local_lookup(&config, "daemon-local-key", None, None).is_none()
        );
        let demands = crate::demand::take();
        assert_eq!(demands.len(), 1);
        assert_eq!(demands[0].cache_key, "daemon-local-key");
        assert!(demands[0].first_demand_at_ms > 0);
        assert_eq!(demands[0].remote_wait_ms, 0);
    }

    #[test]
    fn local_hit_demand_reaches_event_without_remote_wait() {
        let _ = crate::demand::take();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        put_test_entry(&store, dir.path(), "local-demand-key");
        let before = chrono::Utc::now().timestamp_millis() as u64;
        assert!(
            lookup_local_entry(&store, None, "local-demand-key")
                .unwrap()
                .is_some()
        );
        let after = chrono::Utc::now().timestamp_millis() as u64;
        log_event_with_store_stats(
            &config,
            "/repo",
            "foo",
            EventResult::LocalHit,
            10,
            20,
            30,
            "local-demand-key",
            0,
            FileHashStats::default(),
            0,
            0,
            0,
            StorePutResult::default(),
        );
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].schema, 21);
        let demands = &events[0].demands;
        assert_eq!(demands.len(), 1);
        assert_eq!(demands[0].cache_key, "local-demand-key");
        assert!((before..=after).contains(&demands[0].first_demand_at_ms));
        assert_eq!(demands[0].remote_wait_ms, 0);
        assert!(crate::demand::take().is_empty());
    }

    /// Store stats and hash stats should be carried into the event JSONL entry
    /// because reports rely on these schema-9 fields.
    #[test]
    fn log_event_with_store_stats_persists_timing_hash_and_store_fields() {
        let _ = crate::verify_compare::take_last_report();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store_put = StorePutResult {
            output_blobs: 3,
            duplicate_blobs: 1,
            new_blobs: 2,
        };
        let hash_stats = FileHashStats {
            cache_hits: 4,
            cache_misses: 5,
            bytes_hashed: 6,
        };

        log_event_with_store_stats(
            &config,
            "/repo",
            "foo",
            EventResult::Miss,
            10,
            20,
            30,
            "cache-key",
            40,
            hash_stats,
            50,
            60,
            70,
            store_put,
        );

        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.root, "/repo");
        assert_eq!(event.crate_name, "foo");
        assert_eq!(event.result, EventResult::Miss);
        assert_eq!(event.elapsed_ms, 10);
        assert_eq!(event.compile_time_ms, 20);
        assert_eq!(event.size, 30);
        assert_eq!(event.cache_key, "cache-key");
        assert_eq!(event.schema, 21);
        assert_eq!(event.key_ms, 40);
        assert_eq!(event.key_hash_hits, 4);
        assert_eq!(event.key_hash_misses, 5);
        assert_eq!(event.key_hash_bytes, 6);
        assert_eq!(event.lookup_ms, 50);
        assert_eq!(event.restore_ms, 60);
        assert_eq!(event.store_ms, 70);
        assert_eq!(event.store_output_blobs, 3);
        assert_eq!(event.store_duplicate_blobs, 1);
        assert_eq!(event.store_new_blobs, 2);
        assert!(
            event.store_error.is_empty(),
            "a successful store records no failure reason"
        );
        assert!(
            event.verify_compare.is_empty(),
            "verify off must not invent a verify_compare class"
        );
    }

    /// Schema 17: the phases measured outside the wrapper's own timers reach
    /// the event through the process-global accumulators. The counters only
    /// grow and other tests in this binary add real milliseconds to them, so
    /// each field is fed a magnitude ten times the last: a lower bound and a
    /// band catch a zeroed field and a swapped one, whatever ran before.
    #[test]
    fn log_event_records_the_wrapper_phase_accumulators() {
        let _ = crate::verify_compare::take_last_report();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));

        const STARTUP_MS: u64 = 5_000;
        const DEP_INFO_MS: u64 = 50_000;
        const FLIGHT_MS: u64 = 500_000;
        const PERMIT_MS: u64 = 5_000_000;
        let before = [
            crate::opcounts::startup_ms(),
            crate::opcounts::dep_info_ms(),
            crate::opcounts::flight_wait_ms(),
            crate::opcounts::permit_wait_ms(),
        ];
        let runs_before = crate::opcounts::dep_info_runs();
        crate::opcounts::record_startup(std::time::Duration::from_millis(STARTUP_MS));
        crate::opcounts::record_dep_info_run(std::time::Duration::from_millis(DEP_INFO_MS));
        crate::opcounts::record_flight_wait(std::time::Duration::from_millis(FLIGHT_MS));
        crate::opcounts::record_permit_wait(std::time::Duration::from_millis(PERMIT_MS));

        log_event_with_hash_stats(
            &config,
            "/repo",
            "foo",
            EventResult::Miss,
            100,
            20,
            30,
            "cache-key",
            40,
            FileHashStats::default(),
            50,
            0,
            60,
        );

        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        let event = &events[0];
        assert_eq!(event.schema, 21);
        // Whatever other tests add is real time, far under the next band.
        for (name, value, floor, fed) in [
            ("startup_ms", event.startup_ms, before[0], STARTUP_MS),
            ("dep_info_ms", event.dep_info_ms, before[1], DEP_INFO_MS),
            ("flight_wait_ms", event.flight_wait_ms, before[2], FLIGHT_MS),
            ("permit_wait_ms", event.permit_wait_ms, before[3], PERMIT_MS),
        ] {
            assert!(value >= floor + fed, "{name} = {value}, fed {fed}");
            assert!(
                value < floor + fed * 10,
                "{name} = {value} carries another field's magnitude"
            );
        }
        assert!(event.dep_info_runs > runs_before);
        assert_eq!(event.wait_ms(), event.flight_wait_ms + event.permit_wait_ms);
    }

    /// With a pinned process start the wrapper clock is anchored there and
    /// the time already spent is recorded as startup.
    #[test]
    fn wrapper_entry_anchors_at_the_pinned_process_start() {
        crate::opcounts::mark_process_start();
        let pinned = crate::opcounts::process_start().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let before = crate::opcounts::startup_ms();
        let start = wrapper_entry();
        assert_eq!(start, pinned, "elapsed_ms must span from process start");
        assert!(
            crate::opcounts::startup_ms() >= before + 5,
            "the time before wrapper entry must be recorded as startup"
        );
    }

    #[test]
    fn store_error_for_event_keeps_the_chain_but_bounds_the_shape() {
        // The whole anyhow chain, not just the outermost context — that is the
        // half that names the cause.
        let err = anyhow::anyhow!("Permission denied (os error 13)")
            .context("creating blob shard directory");
        assert_eq!(
            store_error_for_event(&err),
            "creating blob shard directory: Permission denied (os error 13)"
        );

        // A newline would break the report row this string is printed inside.
        let multiline = anyhow::anyhow!("line one\nline two\r\tline three");
        let flattened = store_error_for_event(&multiline);
        assert!(!flattened.contains('\n') && !flattened.contains('\r'));
        assert_eq!(flattened, "line one line two  line three");

        // And it cannot grow without bound: this reason is persisted on every
        // failing compile.
        let huge = anyhow::anyhow!("x".repeat(5000));
        let capped = store_error_for_event(&huge);
        assert!(capped.ends_with("… [truncated]"));
        assert_eq!(
            capped.chars().count(),
            2048 + "… [truncated]".chars().count()
        );
    }

    /// A failed `Store::put` stays a `Miss` (the compiler ran) but carries the
    /// reason, so the report can tell a cold miss from one that repeats forever
    /// (kunobi-ninja/kache#629).
    #[test]
    fn log_event_with_store_outcome_persists_the_store_failure_reason() {
        let _ = crate::verify_compare::take_last_report();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));

        log_event_with_store_outcome(
            &config,
            "/repo",
            "foo",
            EventResult::Miss,
            10,
            20,
            30,
            "cache-key",
            0,
            FileHashStats::default(),
            0,
            0,
            0,
            StorePutResult::default(),
            "refusing to cache zero-byte artifact: libfoo.rlib".to_string(),
        );

        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        let event = &events[0];
        assert_eq!(
            event.result,
            EventResult::Miss,
            "the compiler ran, so it stays a miss and stays in the hit-rate denominator"
        );
        assert_eq!(
            event.store_error,
            "refusing to cache zero-byte artifact: libfoo.rlib"
        );
        assert!(
            event.verify_compare.is_empty(),
            "a store-failure miss must not invent a verify_compare class"
        );
    }

    #[test]
    fn log_event_persists_same_key_lookup_rejection() {
        let _ = crate::verify_compare::take_last_report();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));

        log_event_with_store_and_lookup_outcome(
            &config,
            "/repo",
            "foo.c",
            EventResult::Miss,
            10,
            20,
            30,
            "same-key",
            0,
            FileHashStats::default(),
            1,
            0,
            2,
            StorePutResult {
                output_blobs: 2,
                duplicate_blobs: 0,
                new_blobs: 2,
            },
            String::new(),
            "matching entry lacks dep-info required by this invocation".to_string(),
        );

        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        let event = &events[0];
        assert_eq!(event.result, EventResult::Miss);
        assert_eq!(event.cache_key, "same-key");
        assert_eq!(event.schema, 21);
        assert_eq!(
            event.lookup_rejection,
            "matching entry lacks dep-info required by this invocation"
        );
        assert!(event.store_error.is_empty());
        assert!(
            event.verify_compare.is_empty(),
            "lookup rejection must not invent a verify_compare class"
        );
    }

    /// `verify_compare` (schema 16) is read from the hit-qualification stash.
    /// Empty when verify did not run; the class string when it did.
    #[test]
    fn log_event_persists_verify_compare_class_on_hit() {
        let _ = crate::verify_compare::take_last_report();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));

        log_event_with_hash_stats(
            &config,
            "/repo",
            "foo",
            EventResult::LocalHit,
            1,
            20,
            30,
            "hit-key",
            0,
            FileHashStats::default(),
            0,
            0,
            0,
        );
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events[0].schema, 21);
        assert_eq!(events[0].result, EventResult::LocalHit);
        assert!(
            events[0].verify_compare.is_empty(),
            "no qualification run must leave verify_compare empty"
        );

        crate::verify_compare::record_report("content: libfoo.rlib (byte mismatch)".to_string());
        log_event_with_hash_stats(
            &config,
            "/repo",
            "foo",
            EventResult::LocalHit,
            2,
            20,
            30,
            "hit-key",
            0,
            FileHashStats::default(),
            0,
            1,
            0,
        );
        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].schema, 21);
        assert_eq!(
            events[1].verify_compare,
            "content: libfoo.rlib (byte mismatch)"
        );
    }

    /// Passthrough events intentionally omit cache timings but preserve the
    /// structured reason, fallback marker, and compiler exit code.
    #[test]
    fn log_passthrough_event_persists_reason_fallback_and_exit_code() {
        let _ = crate::verify_compare::take_last_report();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let output = PassthroughOutput {
            exit_code: 42,
            fallback: true,
            fallback_attempt: None,
        };

        log_passthrough_event(
            &config,
            "/repo",
            "foo",
            17,
            "unsupported|cc link mode — not yet".to_string(),
            &output,
        );

        let events = crate::events::read_events(&config.event_log_path()).unwrap();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.result, EventResult::Passthrough);
        assert_eq!(event.elapsed_ms, 17);
        assert_eq!(
            event.passthrough_reason,
            "unsupported|cc link mode — not yet"
        );
        assert!(event.fallback);
        assert_eq!(event.exit_code, Some(42));
        assert_eq!(event.cache_key, "");
        assert!(
            event.verify_compare.is_empty(),
            "passthrough must not invent a verify_compare class"
        );
    }

    /// The emit gate must reject a missing requested output kind, while
    /// accepting supersets and ignoring kinds kache cannot classify.
    #[test]
    fn missing_requested_emit_detects_only_gated_absent_outputs() {
        let mut args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "metadata,link,llvm-ir,llvm-bc",
        ]);
        let artifacts = ArtifactSet::from_output_files(
            vec![
                (PathBuf::from("libfoo.rlib"), "libfoo.rlib".to_string()),
                (PathBuf::from("libfoo.rmeta"), "libfoo.rmeta".to_string()),
                (PathBuf::from("foo.ll"), "foo.ll".to_string()),
            ],
            classify_by_filename,
        );

        assert_eq!(
            missing_requested_emit(&args, &artifacts),
            Some("llvm-bc".to_string())
        );

        args.emit = vec![
            "metadata".to_string(),
            "link".to_string(),
            "debug-info".to_string(),
        ];
        assert_eq!(missing_requested_emit(&args, &artifacts), None);
    }

    fn names(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| item.to_string()).collect()
    }

    #[test]
    fn unaudited_bundled_member_finds_only_uncovered_archive_members() {
        let rustc_own = [
            "/",
            "//",
            "/SYM64/",
            "__.SYMDEF SORTED",
            "lib.rmeta",
            "lib.rmeta-link",
            "mylib-0123.mylib.a1b2-cgu.0.rcgu.o",
            "mylib-0123.mylib.a1b2-cgu.0.rcgu.dwo",
        ];
        let mut candidates = names(&rustc_own);
        candidates.extend(names(&["util.o", "libfoo.a"]));
        assert_eq!(
            unaudited_bundled_member(&names(&rustc_own), &[], &candidates),
            None,
            "rustc's own members are never foreign"
        );

        let with = |extra: &[&str]| {
            let mut members = names(&rustc_own);
            members.extend(names(extra));
            members
        };
        assert_eq!(
            unaudited_bundled_member(&with(&["util.o"]), &names(&["util.o"]), &candidates),
            None,
            "a keyed archive covers its member"
        );
        assert_eq!(
            unaudited_bundled_member(&with(&["libfoo.a"]), &[], &candidates),
            Some("libfoo.a".to_string()),
            "a packed +whole-archive library matches its file name"
        );
        assert_eq!(
            unaudited_bundled_member(
                &with(&["util.o", "util.o"]),
                &names(&["util.o"]),
                &candidates
            ),
            Some("util.o".to_string()),
            "one keyed name covers one member"
        );
        assert_eq!(
            unaudited_bundled_member(&with(&["other.o"]), &[], &candidates),
            None,
            "a member no candidate holds came from elsewhere"
        );
    }

    /// A GNU `ar` archive of short-named members, each holding `data` (of
    /// even length, so no member needs padding).
    fn ar_archive_of(members: &[(&str, &[u8])]) -> Vec<u8> {
        let mut bytes = b"!<arch>\n".to_vec();
        for (member, data) in members {
            assert_eq!(data.len() % 2, 0);
            let name = format!("{member}/");
            bytes.extend_from_slice(
                format!(
                    "{name:<16}{:<12}{:<6}{:<6}{:<8}{:<10}`\n",
                    0,
                    0,
                    0,
                    644,
                    data.len()
                )
                .as_bytes(),
            );
            bytes.extend_from_slice(data);
        }
        bytes
    }

    /// [`ar_archive_of`] with two bytes in each member.
    fn ar_archive(members: &[&str]) -> Vec<u8> {
        let members: Vec<(&str, &[u8])> = members.iter().map(|name| (*name, &b"xx"[..])).collect();
        ar_archive_of(&members)
    }

    fn keyed_native(
        archives: Vec<PathBuf>,
        bundled: Vec<(PathBuf, bool)>,
        dirs: Vec<PathBuf>,
    ) -> crate::cache_key::KeyedNativeArchives {
        crate::cache_key::KeyedNativeArchives {
            archives,
            bundled: bundled
                .into_iter()
                .map(|(path, packed)| crate::cache_key::BundledArchive { path, packed })
                .collect(),
            dirs,
        }
    }

    /// An `ArtifactSet` holding one rlib with these members.
    fn rlib_artifacts(dir: &Path, members: &[(&str, &[u8])]) -> ArtifactSet {
        let rlib = dir.join("libmylib.rlib");
        std::fs::write(&rlib, ar_archive_of(members)).unwrap();
        ArtifactSet::from_output_files(
            vec![(rlib, "libmylib.rlib".to_string())],
            classify_by_filename,
        )
    }

    /// An rlib that carries a member of an unkeyed archive in its `-L` dir is
    /// refused; the same rlib with that archive keyed, a unit that is no rlib,
    /// and an rlib with no native dir are not audited.
    #[test]
    fn unaudited_native_bundle_reads_the_rlib_and_its_native_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let archive = out.join("libbundled.a");
        std::fs::write(&archive, ar_archive(&["value.o"])).unwrap();
        std::fs::write(out.join("libother.a"), ar_archive(&["other.o"])).unwrap();
        let artifacts = rlib_artifacts(dir.path(), &[("lib.rmeta", b"xx"), ("value.o", b"xx")]);
        let lib = rustc_args(&["rustc", "src/lib.rs", "--crate-type", "lib"]);

        let unkeyed = keyed_native(vec![], vec![], vec![out.clone()]);
        assert_eq!(
            unaudited_native_bundle(&lib, &artifacts, &unkeyed).unwrap(),
            Some("value.o".to_string())
        );
        let keyed = keyed_native(
            vec![archive.clone()],
            vec![(archive.clone(), false)],
            vec![out.clone()],
        );
        assert_eq!(
            unaudited_native_bundle(&lib, &artifacts, &keyed).unwrap(),
            None
        );
        let bin = rustc_args(&["rustc", "src/main.rs", "--crate-type", "bin"]);
        assert_eq!(
            unaudited_native_bundle(&bin, &artifacts, &unkeyed).unwrap(),
            None
        );
        assert_eq!(
            unaudited_native_bundle(&lib, &artifacts, &keyed_native(vec![], vec![], vec![]))
                .unwrap(),
            None
        );
        assert_eq!(
            unaudited_native_bundle(&lib, &ArtifactSet::default(), &unkeyed).unwrap(),
            None,
            "no rlib output, nothing to audit"
        );

        // glibc ships `libm.a` as a linker script; rustc cannot bundle it.
        std::fs::write(out.join("libm.a"), b"/* GNU ld script */\nGROUP ( x )\n").unwrap();
        assert_eq!(
            unaudited_native_bundle(&lib, &artifacts, &unkeyed).unwrap(),
            Some("value.o".to_string()),
            "a file that is no archive is no candidate"
        );

        std::fs::write(out.join("libthin.a"), b"!<thin>\n").unwrap();
        assert!(
            unaudited_native_bundle(&lib, &artifacts, &unkeyed).is_err(),
            "a candidate that cannot be read refuses the store"
        );
        assert_eq!(
            unaudited_native_bundle(&lib, &artifacts, &keyed).unwrap(),
            None,
            "candidates are read only for an uncovered member"
        );
    }

    /// A keyed archive accounts only for the members the rlib takes from it.
    /// A packed `+whole-archive` library is one member under its file name,
    /// so its own members must not cover an unkeyed archive's same-named one.
    #[test]
    fn bundle_audit_credits_only_what_the_rlib_carries() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out");
        std::fs::create_dir_all(&out).unwrap();
        let foo = out.join("libfoo.a");
        std::fs::write(&foo, ar_archive(&["util.o"])).unwrap();
        std::fs::write(out.join("libbar.a"), ar_archive(&["util.o"])).unwrap();
        let lib = rustc_args(&["rustc", "src/lib.rs", "--crate-type", "lib"]);
        let audit = |members: &[(&str, &[u8])], bundled: Vec<(PathBuf, bool)>| {
            let artifacts = rlib_artifacts(dir.path(), members);
            let native = keyed_native(vec![foo.clone()], bundled, vec![out.clone()]);
            unaudited_native_bundle(&lib, &artifacts, &native).unwrap()
        };

        let packed = [
            ("lib.rmeta", &b"xx"[..]),
            ("libfoo.a", b"xx"),
            ("util.o", b"xx"),
        ];
        assert_eq!(
            audit(&packed, vec![(foo.clone(), true)]),
            Some("util.o".to_string()),
            "bar's util.o beside a packed foo"
        );
        assert_eq!(
            audit(&packed, vec![]),
            Some("util.o".to_string()),
            "a keyed archive the rlib does not bundle covers nothing"
        );
        let unpacked = [("lib.rmeta", &b"xx"[..]), ("util.o", b"xx")];
        assert_eq!(audit(&unpacked, vec![(foo.clone(), false)]), None);

        assert_eq!(
            bundle_credits(&keyed_native(vec![], vec![(foo.clone(), true)], vec![]).bundled)
                .unwrap(),
            ["libfoo.a"]
        );
        assert_eq!(
            bundle_credits(&keyed_native(vec![], vec![(foo.clone(), false)], vec![]).bundled)
                .unwrap(),
            ["util.o"]
        );
    }

    /// rustc writes `raw-dylib` imports into the rlib as import-library
    /// members named after the DLL; an import library in the `-L` dirs holds
    /// members of the same names, and neither is a bundled archive.
    #[test]
    fn bundle_audit_leaves_raw_dylib_import_members_alone() {
        const STUB: &[u8] = b"\0\0\xff\xff\x64\x86stub";
        let member = |name: &str, short_import| crate::native_archive::ArchiveMember {
            name: name.to_string(),
            short_import,
        };
        assert_eq!(
            without_import_members(&[
                member("lib.rmeta", false),
                member("kernel32.dll", true),
                member("kernel32.dll", false),
                member("util.o", false),
            ]),
            ["lib.rmeta", "util.o"]
        );

        let dir = tempfile::tempdir().unwrap();
        let lib_dir = dir.path().join("lib");
        std::fs::create_dir_all(&lib_dir).unwrap();
        std::fs::write(
            lib_dir.join("windows.0.53.0.lib"),
            ar_archive_of(&[("kernel32.dll", STUB), ("kernel32.dll", b"desc")]),
        )
        .unwrap();
        let artifacts = rlib_artifacts(
            dir.path(),
            &[
                ("lib.rmeta", b"xx"),
                ("kernel32.dll", STUB),
                ("kernel32.dll", b"desc"),
            ],
        );
        let lib = rustc_args(&["rustc", "src/lib.rs", "--crate-type", "lib"]);
        let native = keyed_native(vec![], vec![], vec![lib_dir]);
        assert_eq!(
            unaudited_native_bundle(&lib, &artifacts, &native).unwrap(),
            None
        );
    }

    /// An entry whose recorded emit set is narrower than the invocation is
    /// evicted and reported as a restore miss instead of serving a partial hit.
    /// kunobi-ninja/kache#330: a cached `.d` whose expanded paths do not
    /// resolve for THIS consumer poisons cargo's freshness check into a
    /// permanent recompile loop (the recompile restores the same broken
    /// `.d`). The restore gate must evict the entry and miss so the
    /// recompile stores a portable one.
    #[test]
    fn restore_evicts_entry_whose_depinfo_references_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let out_dir = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(&out_dir).unwrap();
        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "dep-info,link",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);

        // A real source the .d also lists, so only the donor-absolute path
        // is missing — the exact field-report shape.
        let real_src = dir.path().join("lib.rs");
        std::fs::write(&real_src, "pub fn f() {}\n").unwrap();
        let dep_content = format!(
            "{}/foo.rlib: {} /donor/project/target/debug/build/gen-8a22/out/generated.rs\n",
            out_dir.display(),
            real_src.display(),
        );
        let dep_hash = blake3::hash(dep_content.as_bytes()).to_hex().to_string();
        let rlib_hash = blake3::hash(b"rlib bytes").to_hex().to_string();
        create_blob(&store, &dep_hash, dep_content.as_bytes());
        create_blob(&store, &rlib_hash, b"rlib bytes");

        let mut dep_file = cached_file("foo.d", &dep_hash);
        dep_file.size = dep_content.len() as u64;
        let mut rlib_file = cached_file("libfoo.rlib", &rlib_hash);
        rlib_file.size = "rlib bytes".len() as u64;
        let meta = entry_meta(
            "poisoned-key",
            vec![dep_file, rlib_file],
            &["dep-info", "link"],
        );
        let entry_dir = store.entry_dir(&meta.cache_key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();
        store.insert_entry_row_for_test("poisoned-key");

        let err = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("does not resolve here"),
            "unexpected error: {err}"
        );
        assert!(
            !store.entry_dir(&meta.cache_key).join("meta.json").exists(),
            "the poisoned entry must be evicted so the recompile stores a portable one"
        );
        assert!(
            !out_dir.join("foo.d").exists(),
            "nothing may be materialized before the gate"
        );
    }

    #[test]
    fn restore_evicts_incomplete_extra_inputs_depinfo_entries() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let project = dir.path().join("project");
        let source = project.join("src/lib.rs");
        std::fs::create_dir_all(project.join("src")).unwrap();
        std::fs::create_dir_all(project.join("data")).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname='foo'\nversion='0.1.0'\n",
        )
        .unwrap();
        std::fs::write(&source, "pub fn f() {}\n").unwrap();
        std::fs::write(
            project.join("kache.toml"),
            "extra_inputs = [\"data/**/*.txt\"]\n",
        )
        .unwrap();
        std::fs::write(project.join("data/value.txt"), "v1").unwrap();

        let out_dir = dir.path().join("target/debug/deps");
        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "foo",
            "--emit",
            "dep-info",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);
        let snapshot = crate::extra_inputs::ExtraInputsSnapshot::resolve(
            args.source_file.as_deref(),
            "foo",
            args.is_primary,
            &crate::cache_key::FileHasher::new(),
        )
        .unwrap()
        .unwrap();

        let malformed = "not rustc dep-info\n";
        let dep_hash = blake3::hash(malformed.as_bytes()).to_hex().to_string();
        create_blob(&store, &dep_hash, malformed.as_bytes());
        let mut dep_file = cached_file("foo.d", &dep_hash);
        dep_file.size = malformed.len() as u64;
        let meta = entry_meta("malformed-extra-key", vec![dep_file], &["dep-info"]);
        let entry_dir = store.entry_dir(&meta.cache_key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();
        store.insert_entry_row_for_test(&meta.cache_key);

        let error = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            Some(&snapshot),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("cannot be completed safely"),
            "{error:#}"
        );
        assert!(
            !entry_dir.join("meta.json").exists(),
            "malformed cached dep-info must be evicted before hit publication"
        );
        assert!(!out_dir.join("foo.d").exists());

        // A pre-emit-gate entry has no `emit_kinds`, so the generic coverage
        // check intentionally accepts it. Active extra inputs must still
        // require a concrete dep-info artifact before publishing a hit.
        let rlib_bytes = b"legacy rlib";
        let rlib_hash = blake3::hash(rlib_bytes).to_hex().to_string();
        create_blob(&store, &rlib_hash, rlib_bytes);
        let mut rlib_file = cached_file("libfoo.rlib", &rlib_hash);
        rlib_file.size = rlib_bytes.len() as u64;
        let legacy_meta = entry_meta("legacy-no-depinfo-key", vec![rlib_file], &[]);
        let legacy_entry_dir = store.entry_dir(&legacy_meta.cache_key);
        std::fs::create_dir_all(&legacy_entry_dir).unwrap();
        std::fs::write(
            legacy_entry_dir.join("meta.json"),
            serde_json::to_string(&legacy_meta).unwrap(),
        )
        .unwrap();
        store.insert_entry_row_for_test(&legacy_meta.cache_key);

        let error = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &legacy_meta,
            Some(&snapshot),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("has no dep-info artifact"),
            "{error:#}"
        );
        assert!(
            !legacy_entry_dir.join("meta.json").exists(),
            "legacy entry without dep-info must be evicted before hit publication"
        );

        // A differently named `.d` is not the output Cargo expects for this
        // unit. Treat it exactly like a missing legacy dep-info artifact.
        let wrong_dep = "other: src/lib.rs\n";
        let wrong_hash = blake3::hash(wrong_dep.as_bytes()).to_hex().to_string();
        create_blob(&store, &wrong_hash, wrong_dep.as_bytes());
        let mut wrong_file = cached_file("other.d", &wrong_hash);
        wrong_file.size = wrong_dep.len() as u64;
        let wrong_meta = entry_meta("legacy-wrong-depinfo-key", vec![wrong_file], &[]);
        let wrong_entry_dir = store.entry_dir(&wrong_meta.cache_key);
        std::fs::create_dir_all(&wrong_entry_dir).unwrap();
        std::fs::write(
            wrong_entry_dir.join("meta.json"),
            serde_json::to_string(&wrong_meta).unwrap(),
        )
        .unwrap();
        store.insert_entry_row_for_test(&wrong_meta.cache_key);

        let error = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &wrong_meta,
            Some(&snapshot),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("has no dep-info artifact named foo.d"),
            "{error:#}"
        );
        assert!(!wrong_entry_dir.join("meta.json").exists());

        // Cargo skips env-dep records even when their values contain `: `.
        // Restore validation must inspect the following Make rule and evict a
        // consumer-invalid path instead of accepting an empty dependency set.
        let missing_dependency = project.join("does-not-exist.rs");
        let env_prefixed = format!(
            "# env-dep:CFG=foo: bar\nfoo: {}\n",
            missing_dependency.display()
        );
        let env_hash = blake3::hash(env_prefixed.as_bytes()).to_hex().to_string();
        create_blob(&store, &env_hash, env_prefixed.as_bytes());
        let mut env_file = cached_file("foo.d", &env_hash);
        env_file.size = env_prefixed.len() as u64;
        let env_meta = entry_meta("env-prefixed-depinfo-key", vec![env_file], &["dep-info"]);
        let env_entry_dir = store.entry_dir(&env_meta.cache_key);
        std::fs::create_dir_all(&env_entry_dir).unwrap();
        std::fs::write(
            env_entry_dir.join("meta.json"),
            serde_json::to_string(&env_meta).unwrap(),
        )
        .unwrap();
        store.insert_entry_row_for_test(&env_meta.cache_key);

        let error = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &env_meta,
            Some(&snapshot),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("does not resolve here"),
            "{error:#}"
        );
        assert!(!env_entry_dir.join("meta.json").exists());
    }

    #[test]
    fn compile_revalidation_rejects_nested_directory_aba() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let project = dir.path().join("project");
        let source = project.join("src/lib.rs");
        let nested = project.join("data/deep");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname='foo'\nversion='0.1.0'\n",
        )
        .unwrap();
        std::fs::write(&source, "pub fn f() {}\n").unwrap();
        std::fs::write(
            project.join("kache.toml"),
            "extra_inputs = [\"data/**/*.txt\"]\n",
        )
        .unwrap();
        std::fs::write(project.join("data/stable.txt"), "v1").unwrap();

        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "foo",
            "--emit",
            "dep-info",
            "--out-dir",
            dir.path().join("out").to_str().unwrap(),
        ]);
        let before = crate::extra_inputs::ExtraInputsSnapshot::resolve(
            args.source_file.as_deref(),
            "foo",
            args.is_primary,
            &crate::cache_key::FileHasher::new(),
        )
        .unwrap()
        .unwrap();

        let transient = nested.join("transient.txt");
        std::fs::write(&transient, "transient").unwrap();
        std::fs::remove_file(&transient).unwrap();
        filetime::set_file_mtime(
            &nested,
            filetime::FileTime::from_unix_time(2_000_000_000, 123),
        )
        .unwrap();
        let after = crate::extra_inputs::ExtraInputsSnapshot::resolve(
            args.source_file.as_deref(),
            "foo",
            args.is_primary,
            &crate::cache_key::FileHasher::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(before.digest(), after.digest());
        assert_ne!(before, after);
        assert!(extra_inputs_changed_during_compile(
            &config,
            &args,
            Some(&before),
            i64::MAX,
        ));
    }

    #[test]
    fn activation_from_none_is_rejected_on_miss_hit_and_success_paths() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let project = dir.path().join("project");
        let source = project.join("src/lib.rs");
        let out_dir = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::create_dir_all(project.join("data")).unwrap();
        std::fs::write(
            project.join("Cargo.toml"),
            "[package]\nname='foo'\nversion='0.1.0'\n",
        )
        .unwrap();
        std::fs::write(&source, "pub fn f() {}\n").unwrap();
        std::fs::write(project.join("data/value.txt"), "v1").unwrap();

        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "foo",
            "--emit",
            "dep-info",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);
        let initial = crate::extra_inputs::ExtraInputsSnapshot::resolve(
            args.source_file.as_deref(),
            "foo",
            args.is_primary,
            &crate::cache_key::FileHasher::new(),
        )
        .unwrap();
        assert!(initial.is_none());

        std::fs::write(
            project.join("kache.toml"),
            "extra_inputs = [\"data/**/*.txt\"]\n",
        )
        .unwrap();

        // Miss/store and uncached passthrough lanes both use these two guards:
        // publication is suppressed, then a successful compiler exit is turned
        // into a retry instead of accepting dep-info that omitted the new config.
        assert!(extra_inputs_changed_during_compile(
            &config,
            &args,
            initial.as_ref(),
            i64::MAX,
        ));
        let error = complete_current_extra_inputs_after_success(&config, &args, initial.as_ref())
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("extra_inputs declaration changed"),
            "{error:#}"
        );

        // A cache hit must reject the same None -> Some transition before any
        // artifact is materialized.
        let dep_info = format!("foo: {}\n", source.display());
        let dep_hash = blake3::hash(dep_info.as_bytes()).to_hex().to_string();
        create_blob(&store, &dep_hash, dep_info.as_bytes());
        let mut dep_file = cached_file("foo.d", &dep_hash);
        dep_file.size = dep_info.len() as u64;
        let meta = entry_meta("pre-activation-key", vec![dep_file], &["dep-info"]);
        let error = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            initial.as_ref(),
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("changed during cache lookup"),
            "{error:#}"
        );
        assert!(!out_dir.join("foo.d").exists());
    }

    #[test]
    fn active_extra_inputs_reject_checksum_freshness_with_actionable_fallback() {
        let checksum_args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "dep-info,link",
            "-Z",
            "checksum-hash-algorithm=blake3",
        ]);
        let error = validate_extra_inputs_freshness_mode(&checksum_args, true).unwrap_err();
        let rendered = format!("{error:#}");
        for expected in [
            "extra_inputs cannot safely complete Cargo checksum-freshness dep-info yet",
            "disable -Z checksum-freshness",
            "KACHE_DISABLED=1",
            "cargo:rerun-if-changed",
        ] {
            assert!(rendered.contains(expected), "{rendered}");
        }
        assert!(validate_extra_inputs_freshness_mode(&checksum_args, false).is_ok());

        let normal_args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "dep-info,link",
        ]);
        assert!(validate_extra_inputs_freshness_mode(&normal_args, true).is_ok());
    }

    #[test]
    fn restore_from_cache_rejects_entry_missing_requested_emit_kind() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "metadata,link",
            "--out-dir",
            "target/debug/deps",
        ]);
        let meta = entry_meta(
            "partial-key",
            vec![cached_file("libfoo.rmeta", "0123456789abcdef")],
            &["metadata"],
        );
        // Production reaches this path through `get()`, so the entry always
        // has a DB row — and removal only cleans a directory whose row it
        // owns (#670). Register a real entry, then overwrite its meta.json
        // with the partial one under test.
        let seed = dir.path().join("seed.rmeta");
        std::fs::write(&seed, b"seed").unwrap();
        store
            .put(
                &meta.cache_key,
                "foo",
                &["lib".into()],
                &[],
                "",
                "dev",
                &[(seed, "libfoo.rmeta".into())],
                "",
                "",
            )
            .unwrap();
        let entry_dir = store.entry_dir(&meta.cache_key);
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();

        let err = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("evicting partial entry"),
            "unexpected error: {err}"
        );
        assert!(
            !store.entry_dir(&meta.cache_key).exists(),
            "partial entry directory should be evicted"
        );
    }

    /// kunobi-ninja/kache#540: the restored `.rlib` is a compiler input for
    /// every downstream crate in this build, and the entry already carries its
    /// verified digest — so the restore must leave that digest in the file-hash
    /// memo instead of letting the next cache key re-read the whole file.
    #[test]
    fn restore_seeds_the_file_hash_memo_with_the_restored_blobs_digest() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let out_dir = dir.path().join("target/debug/deps");

        // Above the memo's persistence floor, or no row would be kept at all.
        let content = vec![b'r'; 128 * 1024];
        let source = dir.path().join("source.rlib");
        std::fs::write(&source, &content).unwrap();
        let hash = crate::cache_key::hash_file(&source).unwrap();
        create_blob(&store, &hash, &content);

        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "link",
            "--out-dir",
            &out_dir.to_string_lossy(),
        ]);
        let meta = entry_meta("seed-key", vec![cached_file("libfoo.rlib", &hash)], &[]);

        restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .unwrap();

        let restored = out_dir.join("libfoo.rlib");
        assert_eq!(std::fs::read(&restored).unwrap(), content);

        match store.file_hash_lookup(&restored) {
            crate::cache_key::FileHashLookup::Hit(memoized) => assert_eq!(
                memoized, hash,
                "the memo must serve the digest the entry recorded"
            ),
            _ => panic!("restored artifact was not memoized"),
        }

        // And the payoff: hashing it as an input reads no bytes.
        let hasher = store.file_hasher();
        assert_eq!(hasher.hash(&restored).unwrap(), hash);
        let stats = hasher.stats();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(
            stats.bytes_hashed, 0,
            "a memoized restore must not re-read the artifact"
        );
    }

    /// The converse, and the reason the memo stays sound: a dep-info file is
    /// re-rooted on restore, so its bytes are not the blob's and its recorded
    /// digest must never be memoized for the restored path.
    #[test]
    fn restore_does_not_memoize_rewritten_dep_info() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        // `--out-dir` is what the dep-info anchor is derived from: the restored
        // `.d` is re-rooted at `<tmp>/target`.
        let out_dir = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(&out_dir).unwrap();

        let source = dir.path().join("src/lib.rs");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, "pub fn f() {}\n").unwrap();

        // Padded past the memo's size floor so the assertion below can only
        // fail because seeding was skipped, not because the file was too small.
        let stored = format!(
            "__kache_root__/debug/deps/libfoo.rlib: {}\n#{}\n",
            source.display(),
            "p".repeat(128 * 1024)
        );
        let hash = {
            let blob = dir.path().join("blob.d");
            std::fs::write(&blob, &stored).unwrap();
            crate::cache_key::hash_file(&blob).unwrap()
        };
        create_blob(&store, &hash, stored.as_bytes());

        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "dep-info",
            "--out-dir",
            &out_dir.to_string_lossy(),
        ]);
        let meta = entry_meta("depinfo-key", vec![cached_file("foo.d", &hash)], &[]);

        restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .unwrap();

        let restored = out_dir.join("foo.d");
        assert_ne!(
            std::fs::read(&restored).unwrap(),
            stored.as_bytes(),
            "dep-info should have been re-rooted for this consumer"
        );
        assert!(
            matches!(
                store.file_hash_lookup(&restored),
                crate::cache_key::FileHashLookup::NeedsHash(_)
            ),
            "a rewritten artifact must not inherit the blob's digest"
        );
    }

    /// Restore refuses artifact names that would escape `--out-dir`; this is a
    /// local trust-boundary check independent of remote import validation.
    #[test]
    fn restore_from_cache_rejects_unsafe_artifact_name() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "link",
            "--out-dir",
            "target/debug/deps",
        ]);
        let meta = entry_meta(
            "unsafe-key",
            vec![cached_file("../escape.rlib", "0123456789abcdef")],
            &[],
        );

        let err = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.contains("unsafe artifact name"),
            "unexpected error: {err}"
        );
    }

    /// A rustc cache entry cannot be restored unless the invocation gives an
    /// exact `-o` path or an `--out-dir` for artifact placement.
    #[test]
    fn restore_from_cache_requires_output_location() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo"]);
        let meta = entry_meta("no-output-key", Vec::new(), &[]);

        let err = restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("no output path"), "unexpected error: {err}");
    }

    #[test]
    fn retarget_rustc_args_rewrites_out_dir_output_and_emit_paths() {
        let staging = PathBuf::from("/tmp/kache-verify-stage");
        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--out-dir",
            "/orig/target/debug/deps",
            "-o",
            "/orig/target/debug/deps/libfoo.rlib",
            "--emit",
            "dep-info=/orig/target/debug/deps/foo.d,link",
        ]);
        let original_root = args.path_normalization_root().map(Path::to_path_buf);
        let staged = retarget_rustc_args_for_staging(&args, &staging);
        assert_eq!(staged.out_dir.as_deref(), Some(staging.as_path()));
        assert_eq!(
            staged.output.as_deref(),
            Some(staging.join("libfoo.rlib").as_path())
        );
        assert_eq!(
            staged.dep_info_output.as_deref(),
            Some(staging.join("foo.d").as_path())
        );
        assert_eq!(
            staged.path_normalization_root(),
            original_root.as_deref(),
            "qualification compile must keep the frozen remap root"
        );
        assert!(
            staged
                .all_args
                .iter()
                .any(|arg| arg == staging.to_str().unwrap()),
            "argv --out-dir value must move to staging: {:?}",
            staged.all_args
        );
        assert!(
            staged
                .all_args
                .iter()
                .any(|arg| arg.contains("dep-info=") && arg.contains("foo.d")),
            "explicit dep-info emit path must move to staging: {:?}",
            staged.all_args
        );
        assert!(
            !staged
                .all_args
                .iter()
                .any(|arg| arg.contains("/orig/target")),
            "original output paths must not remain in argv: {:?}",
            staged.all_args
        );
    }

    #[test]
    fn verify_recompile_exit_status_reports_the_first_plain_stderr_line() {
        assert!(verify_recompile_exit_status(0, "error: ignored on success").is_ok());

        let error = verify_recompile_exit_status(
            2,
            "\n{\"message\":\"json diagnostic\"}\n  error: compile failed  \n",
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "rustc exited 2 (error: compile failed)");
        assert_eq!(verify_recompile_stderr_hint(""), "");
        assert_eq!(
            verify_recompile_stderr_hint("\n{\"message\":\"json only\"}\n"),
            ""
        );
        assert_eq!(
            verify_recompile_stderr_hint("\n warning: plain diagnostic \n"),
            " (warning: plain diagnostic)"
        );
    }

    #[test]
    fn rewrite_output_argv_rewrites_separate_values_without_shifting_other_args() {
        let staging = Path::new("/tmp/stage");
        let rewritten = rewrite_output_argv(
            &[
                "rustc".into(),
                "--out-dir".into(),
                "/old/deps".into(),
                "-o".into(),
                "/old/deps/libfoo.rlib".into(),
                "--emit".into(),
                "metadata,dep-info=/old/deps/foo.d".into(),
                "--cfg".into(),
                "feature=\"x\"".into(),
            ],
            staging,
        );
        assert_eq!(
            rewritten,
            vec![
                "rustc".to_string(),
                "--out-dir".to_string(),
                staging.display().to_string(),
                "-o".to_string(),
                staging.join("libfoo.rlib").display().to_string(),
                "--emit".to_string(),
                format!("metadata,dep-info={}", staging.join("foo.d").display()),
                "--cfg".to_string(),
                "feature=\"x\"".to_string(),
            ]
        );
    }

    #[test]
    fn rewrite_output_argv_preserves_dangling_flags_and_empty_emit_paths() {
        let staging = Path::new("/tmp/stage");
        for flag in ["--out-dir", "-o", "--emit"] {
            let argv = vec!["rustc".to_string(), flag.to_string()];
            assert_eq!(rewrite_output_argv(&argv, staging), argv);
        }
        assert_eq!(
            rewrite_emit_value("metadata,dep-info=", staging),
            "metadata,dep-info="
        );
        assert_eq!(
            rewrite_emit_value("dep-info=/old/foo.d", staging),
            format!("dep-info={}", staging.join("foo.d").display())
        );
    }

    #[test]
    fn rewrite_output_argv_handles_attached_out_dir_and_emit() {
        let staging = Path::new("/tmp/stage");
        let rewritten = rewrite_output_argv(
            &[
                "rustc".into(),
                "--out-dir=/old/deps".into(),
                "--emit=metadata,dep-info=/old/foo.d".into(),
                "-C".into(),
                "extra-filename=-abc".into(),
            ],
            staging,
        );
        assert_eq!(
            rewritten,
            vec![
                "rustc".to_string(),
                format!("--out-dir={}", staging.display()),
                format!(
                    "--emit=metadata,dep-info={}",
                    staging.join("foo.d").display()
                ),
                "-C".to_string(),
                "extra-filename=-abc".to_string(),
            ]
        );
    }

    #[test]
    fn restore_from_cache_with_verify_on_is_fail_open_when_recompile_fails() {
        let _lock = crate::test_support::process_state_test_lock();
        let _verify = TestEnvGuard::set("KACHE_VERIFY", "1");
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let out_dir = dir.path().join("target/debug/deps");
        let content = b"restored-bytes";
        let source = dir.path().join("source.rlib");
        std::fs::write(&source, content).unwrap();
        let hash = crate::cache_key::hash_file(&source).unwrap();
        create_blob(&store, &hash, content);

        let args = rustc_args(&[
            "/no-such-rustc-kache-verify",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "link",
            "--out-dir",
            &out_dir.to_string_lossy(),
        ]);
        let mut file = cached_file("libfoo.rlib", &hash);
        file.size = content.len() as u64;
        let meta = entry_meta("verify-fail-open", vec![file], &["link"]);

        restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .expect("qualification recompile failure must not fail the restore");

        assert_eq!(std::fs::read(out_dir.join("libfoo.rlib")).unwrap(), content);
        let summary = crate::verify_compare::take_last_report();
        assert!(
            summary.starts_with("recompile-failed:"),
            "expected a recompile-failed note, got {summary:?}"
        );
    }

    #[test]
    fn restore_from_cache_skips_verify_when_flag_off() {
        let _lock = crate::test_support::process_state_test_lock();
        let _verify = TestEnvGuard::remove("KACHE_VERIFY");
        let _ = crate::verify_compare::take_last_report();
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let store = Store::open(&config).unwrap();
        let out_dir = dir.path().join("target/debug/deps");
        let content = b"off-path-bytes";
        let source = dir.path().join("source.rlib");
        std::fs::write(&source, content).unwrap();
        let hash = crate::cache_key::hash_file(&source).unwrap();
        create_blob(&store, &hash, content);

        let args = rustc_args(&[
            "rustc",
            "src/lib.rs",
            "--crate-name",
            "foo",
            "--emit",
            "link",
            "--out-dir",
            &out_dir.to_string_lossy(),
        ]);
        let mut file = cached_file("libfoo.rlib", &hash);
        file.size = content.len() as u64;
        let meta = entry_meta("verify-off", vec![file], &["link"]);

        restore_from_cache(
            &config,
            &RustcCompiler::new(),
            &BlobSource::Store(&store),
            &args,
            &meta,
            None,
        )
        .unwrap();

        assert_eq!(std::fs::read(out_dir.join("libfoo.rlib")).unwrap(), content);
        assert!(
            crate::verify_compare::take_last_report().is_empty(),
            "flag off must not stash a verify_compare note"
        );
    }

    #[test]
    fn session_marker_paths_differ_per_root() {
        // Root-scoped markers: parallel repos sharing one cache dir must not
        // suppress each other's sessions (#583 P0.5).
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let a = session_marker_path(&config, "/repo/a");
        let b = session_marker_path(&config, "/repo/b");
        assert_ne!(a, b);
        assert!(a.parent().unwrap().ends_with(".build-sessions"));
    }

    #[test]
    fn session_markers_are_job_scoped_when_store_is_shared() {
        let dir = tempfile::tempdir().unwrap();
        let shared_cache = dir.path().join("shared-cache");
        let mut a = test_config(shared_cache.clone());
        let mut b = test_config(shared_cache);
        a.runtime_dir = dir.path().join("job-a");
        b.runtime_dir = dir.path().join("job-b");

        assert_eq!(a.store_dir(), b.store_dir());
        assert_ne!(
            session_marker_path(&a, "/repo"),
            session_marker_path(&b, "/repo")
        );
        assert!(session_marker_path(&a, "/repo").starts_with(&a.runtime_dir));
        assert!(session_marker_path(&b, "/repo").starts_with(&b.runtime_dir));
    }

    #[test]
    fn remote_prefetch_creates_a_fresh_marker_in_the_job_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let source = workspace.join("src/lib.rs");
        let out_dir = workspace.join("target/debug/deps");
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&out_dir).unwrap();
        std::fs::write(
            workspace.join("Cargo.toml"),
            "[package]\nname = 'runtime-prefetch-test'\nversion = '0.1.0'\n",
        )
        .unwrap();
        std::fs::write(&source, "pub fn value() -> u8 { 1 }\n").unwrap();

        let mut config = test_config(dir.path().join("shared-cache"));
        config.runtime_dir = dir.path().join("job-runtime");
        config.remote = Some(crate::config::RemoteConfig::test_s3(
            "test-bucket",
            "artifacts",
        ));
        let args = rustc_args(&[
            "rustc",
            source.to_str().unwrap(),
            "--crate-name",
            "runtime_prefetch_test",
            "--out-dir",
            out_dir.to_str().unwrap(),
        ]);

        maybe_trigger_prefetch(&config, &args);

        let root = std::fs::canonicalize(&workspace).unwrap();
        let marker = session_marker_path(&config, root.to_str().unwrap());
        assert!(marker.starts_with(&config.runtime_dir));
        let content = std::fs::read_to_string(&marker).expect("session marker created");
        let (_, session_id) = parse_session_marker(&content).expect("valid v1 marker");
        assert!(!session_id.is_empty());
        assert!(timestamp_is_fresh(&content, BUILD_SESSION_SECS));
        assert!(!config.cache_dir.join(".build-sessions").exists());
        // The hint is recorded against the session it was sent for.
        let sent = prefetch_marker_path(&config, root.to_str().unwrap());
        assert_eq!(std::fs::read_to_string(sent).unwrap(), session_id);
    }

    #[test]
    fn session_id_for_event_joins_the_open_session_and_opens_one_after_idle() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let root = "/some/workspace";
        let marker = session_marker_path(&config, root);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();

        // Touched within the window before the invocation started: join it.
        std::fs::write(&marker, "v1 10000 sess42").unwrap();
        assert_eq!(session_id_for_event(&config, root, 10000), "sess42");
        assert_eq!(
            session_id_for_event(&config, root, 10000 + BUILD_SESSION_SECS - 1),
            "sess42"
        );

        // A long compile that started while the session was open stays in
        // it, however late it logs (#583).
        assert_eq!(session_id_for_event(&config, root, 10100), "sess42");

        // Idle for the whole window: a new build, recorded in the marker.
        let started = 10000 + BUILD_SESSION_SECS;
        let minted = session_id_for_event(&config, root, started);
        assert!(!minted.is_empty());
        assert_ne!(minted, "sess42");
        let (_, recorded) =
            parse_session_marker(&std::fs::read_to_string(&marker).unwrap()).unwrap();
        assert_eq!(recorded, minted);
        // Every later compile of that build joins it.
        assert_eq!(
            session_id_for_event(&config, root, now_epoch_secs()),
            minted
        );
    }

    #[test]
    fn session_id_for_event_opens_a_session_without_a_usable_marker() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());

        // No marker yet: the first event opens the session (#1081).
        let first = session_id_for_event(&config, "/fresh", now_epoch_secs());
        assert_eq!(first.len(), 16);

        // A corrupt or id-less marker is not a session to join.
        let marker = session_marker_path(&config, "/broken");
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        for content in ["garbage", &format!("v1 {} ", now_epoch_secs())] {
            std::fs::write(&marker, content).unwrap();
            assert_eq!(
                session_id_for_event(&config, "/broken", now_epoch_secs()).len(),
                16
            );
        }

        // Empty root: never a session, never a marker.
        assert_eq!(session_id_for_event(&config, "", now_epoch_secs()), "");
    }

    #[cfg(unix)]
    #[test]
    fn session_id_for_event_refuses_a_symlinked_marker() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let marker = session_marker_path(&config, "/linked");
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        let target = dir.path().join("target_file");
        std::fs::write(&target, "untouched").unwrap();
        std::os::unix::fs::symlink(&target, &marker).unwrap();

        assert_eq!(
            session_id_for_event(&config, "/linked", now_epoch_secs()),
            ""
        );
        assert_eq!(std::fs::read_to_string(target).unwrap(), "untouched");
    }

    #[test]
    fn prune_session_markers_removes_only_markers_idle_past_retention() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let sessions = config.runtime_dir.join(".build-sessions");
        std::fs::create_dir_all(sessions.join("not-a-marker")).unwrap();
        let now = std::time::SystemTime::now();
        let retention = std::time::Duration::from_secs(3600);
        let marker = |name: &str, age: std::time::Duration| {
            let path = sessions.join(name);
            let file = std::fs::File::create(&path).unwrap();
            file.set_modified(now - age).unwrap();
            path
        };
        let idle = marker("idle", retention);
        let idle_prefetch = marker("idle.prefetch", retention * 2);
        let recent = marker("recent", retention - std::time::Duration::from_secs(1));

        assert_eq!(prune_session_markers(&config, retention, now), 2);
        assert!(!idle.exists());
        assert!(!idle_prefetch.exists());
        assert!(recent.exists());
        assert!(sessions.join("not-a-marker").is_dir());

        // GC keeps a marker for a day after its last touch.
        assert_eq!(SESSION_MARKER_RETENTION.as_secs(), 24 * 3600);

        // Nothing to prune, or no directory at all, is not an error.
        assert_eq!(prune_session_markers(&config, retention, now), 0);
        let empty = test_config(dir.path().join("elsewhere"));
        assert_eq!(prune_session_markers(&empty, retention, now), 0);
    }

    #[test]
    fn invocation_started_secs_counts_back_whole_seconds() {
        assert_eq!(invocation_started_secs(1000, 0), 1000);
        assert_eq!(invocation_started_secs(1000, 1999), 999);
        assert_eq!(invocation_started_secs(1000, 400_000), 600);
        assert_eq!(invocation_started_secs(10, 400_000), 0);
    }

    #[test]
    fn locked_marker_helpers_replace_and_read_the_whole_content() {
        let dir = tempfile::TempDir::new().unwrap();
        let marker = dir.path().join("marker");
        std::fs::write(&marker, "a much longer previous record").unwrap();
        let file = open_marker_for_lock(&marker).unwrap();
        assert_eq!(read_locked_marker(&file), "a much longer previous record");
        write_locked_marker(&file, "short");
        assert_eq!(read_locked_marker(&file), "short");
        assert_eq!(std::fs::read_to_string(&marker).unwrap(), "short");
    }

    #[test]
    fn prefetch_marker_is_per_root_and_names_one_session() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let path = prefetch_marker_path(&config, "/repo");
        assert_eq!(
            path.parent(),
            session_marker_path(&config, "/repo").parent()
        );
        assert_ne!(path, session_marker_path(&config, "/repo"));
        assert_ne!(path, prefetch_marker_path(&config, "/other"));

        assert!(prefetch_marker_names("abc\n", "abc"));
        assert!(!prefetch_marker_names("abd", "abc"));
        assert!(!prefetch_marker_names("", "abc"));
    }

    /// Cargo's target directory, tagged the way Cargo tags it.
    fn tagged_target(workspace: &Path) -> PathBuf {
        let target = workspace.join("target");
        std::fs::create_dir_all(target.join("debug/build/anyhow-1234/out")).unwrap();
        std::fs::create_dir_all(target.join("debug/deps")).unwrap();
        std::fs::write(
            target.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n\
             # This file is a cache directory tag created by cargo.\n",
        )
        .unwrap();
        target
    }

    #[test]
    fn cargo_workspace_of_finds_the_tagged_target_from_any_unit_dir() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("app");
        let target = tagged_target(&workspace);
        for unit_dir in [
            "debug/deps",
            "debug/build/anyhow-1234",
            "debug/build/anyhow-1234/out",
        ] {
            assert_eq!(
                cargo_workspace_of(&target.join(unit_dir)).as_deref(),
                Some(workspace.as_path()),
                "{unit_dir}"
            );
        }

        // Without a tag, or with another tool's tag, there is no answer.
        let untagged = dir.path().join("plain/target/debug/deps");
        std::fs::create_dir_all(&untagged).unwrap();
        assert_eq!(cargo_workspace_of(&untagged), None);
        let other = dir.path().join("other/cache");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        assert_eq!(cargo_workspace_of(&other.join("x")), None);
    }

    #[test]
    fn build_script_units_share_the_workspace_event_root() {
        let dir = tempfile::TempDir::new().unwrap();
        let workspace = dir.path().join("app");
        let target = tagged_target(&workspace);
        let expected = std::fs::canonicalize(&workspace)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let out = |dir: &str| target.join(dir).to_string_lossy().into_owned();

        // A crate, a build script, and a probe the build script runs: one
        // build, one root (#1081).
        for out_dir in [
            out("debug/deps"),
            out("debug/build/anyhow-1234"),
            out("debug/build/anyhow-1234/out"),
        ] {
            let args = rustc_args(&[
                "rustc",
                "src/lib.rs",
                "--crate-name",
                "x",
                "--out-dir",
                &out_dir,
            ]);
            assert_eq!(rustc_event_root(&args), expected, "{out_dir}");
        }
        let output = target.join("debug/build/anyhow-1234/out/probe.rlib");
        let args = rustc_args(&["rustc", "probe.rs", "-o", output.to_str().unwrap()]);
        assert_eq!(rustc_event_root(&args), expected);

        // A build-script run joins it through its OUT_DIR; outside a tagged
        // target it keeps its package directory.
        assert_eq!(
            build_script_event_root(
                &target.join("debug/build/anyhow-1234/out"),
                &dir.path().join("pkg")
            ),
            expected
        );
        let untagged = dir.path().join("pkg");
        std::fs::create_dir_all(&untagged).unwrap();
        assert_eq!(
            build_script_event_root(&untagged.join("out"), &untagged),
            std::fs::canonicalize(&untagged).unwrap().to_string_lossy()
        );

        // cc and nvcc under a build script find it through OUT_DIR.
        let out_dir = Some(std::ffi::OsString::from(out("debug/build/anyhow-1234/out")));
        let parsed = parse_cc(&["gcc", "-c", "foo.c", "-o", "foo.o"]);
        assert_eq!(cc_event_root_in(&parsed, out_dir.clone()), expected);
        assert_eq!(nvcc_event_root_in(out_dir), expected);
        assert_eq!(out_dir_workspace(Some(std::ffi::OsString::new())), None);
        assert_eq!(out_dir_workspace(None), None);
    }

    #[test]
    fn refresh_session_marker_extends_own_session_but_never_clobbers_newer() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let root = "/ws";
        let marker = session_marker_path(&config, root);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();

        // Refreshing our own (stale) session bumps the timestamp.
        std::fs::write(&marker, "v1 1000 mine").unwrap();
        refresh_session_marker(&config, root, "mine");
        let (ts, id) = parse_session_marker(&std::fs::read_to_string(&marker).unwrap()).unwrap();
        assert_eq!(id, "mine");
        assert!(ts > 1000, "timestamp must be refreshed");

        // A newer session re-minted the marker: our refresh must not
        // resurrect the old id over it.
        std::fs::write(&marker, format!("v1 {} newer", now_epoch_secs())).unwrap();
        refresh_session_marker(&config, root, "mine");
        let (_, id) = parse_session_marker(&std::fs::read_to_string(&marker).unwrap()).unwrap();
        assert_eq!(id, "newer");
    }

    #[test]
    fn mint_session_id_is_opaque_and_distinct() {
        // A tight loop is the point: it drives the interval between calls below
        // the clock's resolution, which is exactly the case where the old
        // nanos-only digest repeated itself.
        let ids: Vec<String> = (0..256).map(|_| mint_session_id("/repo")).collect();

        assert!(ids.iter().all(|id| id.len() == 16));
        let unique: std::collections::HashSet<&String> = ids.iter().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "the seq counter makes ids distinct even when the clock does not move"
        );
    }

    /// With no remote configured there is no hint to send, so prefetch
    /// detection touches nothing; sessions come from event logging instead.
    #[test]
    fn maybe_trigger_prefetch_returns_immediately_without_remote() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let config = test_config(cache_dir.clone());
        let args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo"]);

        maybe_trigger_prefetch(&config, &args);

        assert!(!cache_dir.join(".build-session").exists());
        assert!(!config.runtime_dir.join(".build-sessions").exists());
    }

    /// Incremental cleanup only removes a real directory when the config flag
    /// is enabled; absent paths and disabled cleanup are silent no-ops.
    #[test]
    fn clean_incremental_dir_respects_config_and_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let incremental = dir.path().join("incremental");
        std::fs::create_dir_all(&incremental).unwrap();
        std::fs::write(incremental.join("state.bin"), b"state").unwrap();
        let mut config = test_config(dir.path().join("cache"));
        let mut args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo"]);
        args.incremental = Some(incremental.clone());

        config.clean_incremental = false;
        clean_incremental_dir(&config, &args);
        assert!(incremental.exists());

        config.clean_incremental = true;
        clean_incremental_dir(&config, &args);
        assert!(!incremental.exists());

        clean_incremental_dir(&config, &args);
    }

    #[test]
    fn event_root_string_none_is_empty() {
        assert_eq!(event_root_string(None), "");
    }

    #[test]
    fn event_root_string_absolute_path_is_canonicalized() {
        // An existing absolute path canonicalizes to its real path.
        let dir = tempfile::tempdir().unwrap();
        let real = std::fs::canonicalize(dir.path()).unwrap();
        let got = event_root_string(Some(dir.path().to_path_buf()));
        assert_eq!(got, real.to_string_lossy());
    }

    #[test]
    fn event_root_string_relative_path_is_joined_to_cwd_and_absolute() {
        // A relative root is resolved against the current dir, yielding an
        // absolute path (canonicalize falls back to the joined path when the
        // target doesn't exist). Covers the relative-branch join.
        let got = event_root_string(Some(PathBuf::from("kache-nonexistent-rel-xyz")));
        assert!(
            Path::new(&got).is_absolute(),
            "relative root must resolve to an absolute path: {got}"
        );
        assert!(
            got.ends_with("kache-nonexistent-rel-xyz"),
            "resolved path should retain the relative segment: {got}"
        );
    }

    #[test]
    fn event_root_override_reads_kache_event_root_env() {
        // KACHE_EVENT_ROOT, when set and non-empty, overrides the event root.
        let _lock = crate::test_support::process_state_test_lock();
        let _guard = TestEnvGuard::set("KACHE_EVENT_ROOT", "/some/forest/root");
        assert_eq!(
            event_root_override(),
            Some(PathBuf::from("/some/forest/root"))
        );
        // Empty value is treated as unset.
        unsafe {
            std::env::set_var("KACHE_EVENT_ROOT", "");
        }
        assert_eq!(event_root_override(), None);
    }

    #[test]
    fn cache_entry_has_files_rejects_empty_entries() {
        assert!(!cache_entry_has_files(&entry_meta_with_files(&[])));
        assert!(cache_entry_has_files(&entry_meta_with_files(&[
            "libfoo.rlib"
        ])));
    }

    #[test]
    fn cc_scheduled_hit_ok_rejects_empty_files_and_incomplete_entries() {
        let with_depinfo = CcCompiler::new()
            .parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-MMD"]))
            .unwrap();
        let object_only = CcCompiler::new()
            .parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"]))
            .unwrap();

        assert!(
            !cc_scheduled_hit_ok(&object_only, &entry_meta_with_files(&[])),
            "an empty file list must not restore"
        );
        assert!(
            !cc_scheduled_hit_ok(&with_depinfo, &entry_meta_with_files(&["foo.o"])),
            "a rejection reason must not restore"
        );
        assert!(cc_scheduled_hit_ok(
            &object_only,
            &entry_meta_with_files(&["foo.o"])
        ));
        assert!(cc_scheduled_hit_ok(
            &with_depinfo,
            &entry_meta_with_files(&["foo.o", "foo.d"])
        ));
    }

    fn seed_store_entry(dir: &std::path::Path, key: &str) -> (Config, Store, EntryMeta) {
        let config = test_config(dir.to_path_buf());
        let store = Store::open(&config).unwrap();
        let artifact = dir.join("seed.rlib");
        std::fs::write(&artifact, b"cached-bytes").unwrap();
        store
            .put(
                key,
                "seed",
                &["lib".to_string()],
                &[],
                "x86_64-unknown-linux-gnu",
                "dev",
                &[(artifact, "libseed.rlib".to_string())],
                "stdout-diag",
                "stderr-diag",
            )
            .unwrap();
        let meta = store.get(key).unwrap().expect("seeded entry");
        (config, store, meta)
    }

    #[test]
    fn take_recheck_hit_returns_only_entries_the_predicate_accepts() {
        let dir = tempfile::tempdir().unwrap();
        let key = "recheck-key";
        let (_config, store, meta) = seed_store_entry(dir.path(), key);

        let hit = take_recheck_hit(&store, key, &|_| true).expect("accepted meta");
        assert_eq!(hit.cache_key, meta.cache_key);
        assert!(
            take_recheck_hit(&store, key, &|_| false).is_none(),
            "a rejecting predicate must not return the stored meta"
        );
        assert!(take_recheck_hit(&store, "missing", &|_| true).is_none());
    }

    /// With deferred durability the entry is left for the daemon when one is
    /// listening, and flushed here when none is: either way nothing outlives
    /// the build, and a store never sees a daemon does not accumulate
    /// unflushed entries. With the feature off, a put is already durable.
    #[test]
    fn a_pending_entry_waits_for_the_daemon_or_is_flushed_here() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.deferred_durability = true;
        config.socket_path_override = Some(dir.path().join("absent.sock"));
        let store = Store::open(&config).unwrap();
        let output = dir.path().join("out.rlib");
        let put = |key: &str, bytes: &[u8]| {
            let output = dir.path().join(format!("{key}.rlib"));
            std::fs::write(&output, bytes).unwrap();
            store
                .put(
                    key,
                    "pending_crate",
                    &["lib".to_string()],
                    &[],
                    "x86_64-unknown-linux-gnu",
                    "dev",
                    &[(output, "libout.rlib".to_string())],
                    "",
                    "",
                )
                .unwrap();
        };
        let _ = &output;

        put("no_daemon", b"artifact-one");
        assert_eq!(store.pending_durability().unwrap(), 1);
        flush_or_hand_off_durability(&config, &store, "no_daemon");
        assert_eq!(
            store.pending_durability().unwrap(),
            0,
            "without a daemon the entry is flushed here"
        );

        // A listening socket: the daemon owns the flush, so this leaves the
        // entry pending and starts nothing. Bound through the same transport
        // the wrapper probes, which on Windows is a named pipe.
        let socket = dir.path().join("live.sock");
        let listener = crate::transport::ListenerOptions::new()
            .name(crate::transport::socket_name(&socket).expect("socket name"))
            .create_sync()
            .expect("bind listener");
        config.socket_path_override = Some(socket);
        put("with_daemon", b"artifact-two");
        assert_eq!(store.pending_durability().unwrap(), 1);
        flush_or_hand_off_durability(&config, &store, "with_daemon");
        assert_eq!(
            store.pending_durability().unwrap(),
            1,
            "a reachable daemon is left to flush it"
        );
        drop(listener);

        config.deferred_durability = false;
        flush_or_hand_off_durability(&config, &store, "with_daemon");
        assert_eq!(
            store.pending_durability().unwrap(),
            1,
            "the switch being off says nothing about an entry already pending"
        );
    }

    #[test]
    fn a_deferred_cc_compile_is_stored_unless_a_peer_beat_it_or_an_input_moved() {
        assert!(
            !cc_peer_committed_precompile(false, true),
            "an ordinary miss restores instead"
        );
        assert!(!cc_peer_committed_precompile(true, false));
        assert!(cc_peer_committed_precompile(true, true));
        assert!(cc_store_candidate(true, false, false));
        assert!(
            !cc_store_candidate(false, false, false),
            "a failed or output-less compile"
        );
        assert!(
            !cc_store_candidate(true, true, false),
            "an input written during the build"
        );
        assert!(
            !cc_store_candidate(true, false, true),
            "the peer's entry stands"
        );
        assert!(cc_restore_committed(false, true));
        assert!(
            !cc_restore_committed(false, false),
            "an entry that does not fit is not restored"
        );
        assert!(
            !cc_restore_committed(true, true),
            "never over a compile's own outputs"
        );
        assert!(!cc_restore_committed(true, false));
    }

    #[test]
    fn admit_scheduler_miss_off_returns_empty_without_leases() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().to_path_buf());
        config.scheduler = false;
        let store = Store::open(&config).unwrap();
        let (guard, hit) = admit_scheduler_miss(
            &config,
            &store,
            "key",
            FlightIdentity::cc("a.c"),
            "a.c",
            false,
            |_| true,
        );
        assert!(guard.is_empty());
        assert!(hit.is_none());
        assert!(
            !dir.path().join("scheduler").exists(),
            "off switch must not create lease files"
        );
    }

    #[test]
    fn admit_scheduler_miss_on_owns_the_flight() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let store = Store::open(&config).unwrap();
        let (guard, hit) = admit_scheduler_miss(
            &config,
            &store,
            "key",
            FlightIdentity::rustc("owned", &["lib".into()], false),
            "owned",
            false,
            |_| true,
        );
        assert!(hit.is_none());
        assert!(
            !guard.is_empty(),
            "an enabled miss must take a flight and permit"
        );
    }

    fn spawn_flight_holder(
        dir: &std::path::Path,
        crate_name: &str,
        key: &str,
    ) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "wrapper::tests::hold_scheduler_flight_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("KACHE_TEST_SCHEDULER_ROOT", dir)
            .env("KACHE_TEST_FLIGHT_CRATE", crate_name)
            .env("KACHE_TEST_FLIGHT_KEY", key)
            .spawn()
            .unwrap()
    }

    fn wait_flight_ready(dir: &std::path::Path, child: &mut std::process::Child) {
        let ready = dir.join("lock-ready");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready.exists() && std::time::Instant::now() < deadline {
            assert!(
                child.try_wait().unwrap().is_none(),
                "scheduler fixture exited before becoming ready"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(ready.exists(), "scheduler fixture did not become ready");
    }

    #[test]
    fn admit_scheduler_miss_recheck_returns_accepted_meta() {
        let dir = tempfile::tempdir().unwrap();
        let crate_name = "recheck_accept";
        let key = "recheck-accept-key";
        let (_config, store, seeded) = seed_store_entry(dir.path(), key);
        let expected_key = seeded.cache_key.clone();
        drop(store);
        let mut child = spawn_flight_holder(dir.path(), crate_name, key);
        wait_flight_ready(dir.path(), &mut child);

        let cache = dir.path().to_path_buf();
        let waiter = std::thread::spawn(move || {
            let config = test_config(cache);
            let store = Store::open(&config).unwrap();
            admit_scheduler_miss(
                &config,
                &store,
                key,
                FlightIdentity::rustc(crate_name, &["lib".into()], false),
                crate_name,
                false,
                |_| true,
            )
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        std::fs::write(dir.path().join("go"), b"go").unwrap();
        let (guard, hit) = waiter.join().unwrap();
        assert!(
            guard.is_empty(),
            "a Recheck hit must not keep the flight or permit"
        );
        let hit = hit.expect("accepted Recheck meta");
        assert_eq!(hit.cache_key, expected_key);
        let _ = child.wait();
    }

    #[test]
    fn admit_scheduler_miss_recheck_skips_rejected_meta() {
        let dir = tempfile::tempdir().unwrap();
        let crate_name = "recheck_reject";
        let key = "recheck-reject-key";
        let (_config, store, _seeded) = seed_store_entry(dir.path(), key);
        drop(store);
        let mut child = spawn_flight_holder(dir.path(), crate_name, key);
        wait_flight_ready(dir.path(), &mut child);

        let cache = dir.path().to_path_buf();
        let waiter = std::thread::spawn(move || {
            let config = test_config(cache);
            let store = Store::open(&config).unwrap();
            admit_scheduler_miss(
                &config,
                &store,
                key,
                FlightIdentity::rustc(crate_name, &["lib".into()], false),
                crate_name,
                false,
                |_| false,
            )
        });
        std::thread::sleep(std::time::Duration::from_millis(200));
        std::fs::write(dir.path().join("go"), b"go").unwrap();
        let (guard, hit) = waiter.join().unwrap();
        assert!(
            hit.is_none(),
            "a rejecting predicate must not restore the stored meta"
        );
        assert!(
            !guard.is_empty(),
            "rejected Recheck must loop and compile as the next owner"
        );
        let _ = child.wait();
    }

    #[test]
    #[ignore = "subprocess fixture for admit_scheduler_miss recheck tests"]
    fn hold_scheduler_flight_fixture() {
        let root =
            PathBuf::from(std::env::var_os("KACHE_TEST_SCHEDULER_ROOT").expect("fixture root"));
        let crate_name = std::env::var("KACHE_TEST_FLIGHT_CRATE").expect("fixture crate name");
        let key = std::env::var("KACHE_TEST_FLIGHT_KEY").expect("fixture cache key");
        let identity = FlightIdentity::rustc(&crate_name, &["lib".into()], false).with_key(&key);
        let guard = match scheduler::begin_miss(&root, true, &identity, &crate_name, false, None) {
            scheduler::BeginMiss::Compile(guard) => guard,
            scheduler::BeginMiss::Recheck => panic!("fixture must own the flight"),
        };
        std::fs::write(root.join("lock-ready"), b"ready").unwrap();
        let go = root.join("go");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !go.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        drop(guard);
    }

    /// A key derived from the emitted dep-info arms the too-new guard even
    /// with the modified-input guard off: the compile already ran, so an input
    /// written since it started may not match what rustc read.
    #[test]
    fn a_key_from_emitted_dep_info_always_arms_the_too_new_guard() {
        if std::process::Command::new("rustc")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: no rustc");
            return;
        }
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.modified_input_guard = false;
        config.input_predictions = true;
        let src = dir.path().join("src");
        std::fs::create_dir_all(&src).unwrap();
        let lib = src.join("lib.rs");
        let out = dir.path().join("debug").join("deps");
        std::fs::create_dir_all(&out).unwrap();
        let args = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "--crate-name",
                "kt",
                lib.to_str().unwrap(),
                "--emit=dep-info,metadata",
                "--out-dir",
                out.to_str().unwrap(),
            ]))
            .unwrap();
        let invocation_start_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        )
        .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&lib, "pub fn v() {}\n").unwrap();
        let closure = crate::cache_key::DepInfo {
            source_files: vec![lib.clone()],
            env_deps: Vec::new(),
        };
        let compiler = RustcCompiler::new();
        let keyed = compute_rustc_cache_key(
            &config,
            &compiler,
            &args,
            None,
            invocation_start_ns,
            None,
            None,
            FileHashStats::default(),
            false,
            0,
            Vec::new(),
            KeyDiscovery::Emitted(closure),
        )
        .unwrap();
        assert!(!keyed.cache_key.is_empty());
        assert!(!keyed.deferred);
        assert!(
            keyed.key_too_new,
            "a source written after the invocation started is too new"
        );
    }

    #[test]
    fn a_rederivation_never_waits_on_a_discovery_flight() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.scheduler = true;
        let flights = |discovery: KeyDiscovery| discovery_flight_dir(&config, &discovery);
        assert_eq!(
            flights(KeyDiscovery::Immediate),
            Some(config.cache_dir.clone())
        );
        assert_eq!(
            flights(KeyDiscovery::Deferrable),
            Some(config.cache_dir.clone())
        );
        assert_eq!(
            flights(KeyDiscovery::Rederived),
            None,
            "the caller may already hold the flight"
        );
        config.scheduler = false;
        assert_eq!(
            discovery_flight_dir(&config, &KeyDiscovery::Immediate),
            None
        );
    }

    /// A predicted key that missed is re-derived while the invocation may
    /// still hold the unit's discovery flight. A flight lock is not
    /// re-entrant, so joining it again would wait on itself for the whole
    /// flight timeout. The re-derivation must not join one at all.
    #[test]
    fn a_rederived_key_holds_no_discovery_flight() {
        if std::process::Command::new("rustc")
            .arg("--version")
            .output()
            .is_err()
        {
            eprintln!("skipped: no rustc");
            return;
        }
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let mut config = test_config(dir.path().join("cache"));
        config.scheduler = true;
        config.input_predictions = true;
        let store = Store::open(&config).unwrap();
        let lib = dir.path().join("src/lib.rs");
        std::fs::create_dir_all(lib.parent().unwrap()).unwrap();
        std::fs::write(&lib, "pub fn v() {}\n").unwrap();
        let out = dir.path().join("target/debug/deps");
        std::fs::create_dir_all(&out).unwrap();
        let args = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "--crate-name",
                "kt",
                lib.to_str().unwrap(),
                "--crate-type",
                "lib",
                "--emit=dep-info,metadata",
                "--out-dir",
                out.to_str().unwrap(),
            ]))
            .unwrap();
        let keyed = recompute_key_without_prediction(
            &config,
            &RustcCompiler::new(),
            &args,
            None,
            0,
            Some(&store),
            None,
        )
        .unwrap();
        assert!(!keyed.cache_key.is_empty());
        assert!(
            keyed.discovery_flight.is_none(),
            "a re-derivation joined a discovery flight"
        );
    }
}
