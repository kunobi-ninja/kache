use crate::store::StoreHashExt;
use anyhow::{Context, Result};
use bytesize::ByteSize;
use chrono::Utc;
pub(crate) use kache_store::markers::*;
use std::path::{Component, Path, PathBuf};

use crate::args::{ExternDep, RustcArgs};
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
use crate::maintenance::unix_now_secs;
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
    /// Bytes of it the sweep found held by target directories, which no
    /// sweep can free (kunobi-ninja/kache#1206).
    #[serde(default)]
    held: u64,
}

/// How long a sweep's `held` figure stands in for a fresh probe. Deleting a
/// worktree frees what it held without changing the store's size, so after
/// this the trigger goes back to the full size and the next sweep measures
/// again. The daemon's periodic sweep runs this often.
const AUTO_GC_HELD_TTL: std::time::Duration = std::time::Duration::from_secs(6 * 3600);

/// The bytes a recorded sweep found held, while that figure is recent.
fn recent_held_bytes(backoff: Option<AutoGcBackoff>, now: u64) -> u64 {
    backoff
        .filter(|b| now < b.since.saturating_add(AUTO_GC_HELD_TTL.as_secs()))
        .map_or(0, |b| b.held)
}

fn read_auto_gc_backoff(cache_dir: &Path) -> Option<AutoGcBackoff> {
    let json = std::fs::read(auto_gc_backoff_path(cache_dir)).ok()?;
    serde_json::from_slice(&json).ok()
}

/// The record after a sweep that left the store at `size_after`, `held` of
/// it held by target directories. Back under the trigger once the held bytes
/// are left out, there is no backoff, and a record remains only to carry
/// `held`. Otherwise the interval doubles, starting from the check interval,
/// up to [`AUTO_GC_MAX_BACKOFF`].
fn next_auto_gc_backoff(
    previous: Option<AutoGcBackoff>,
    now: u64,
    size_after: u64,
    held: u64,
    max_size: u64,
) -> Option<AutoGcBackoff> {
    if size_after.saturating_sub(held) <= auto_gc_threshold(max_size) {
        return (held > 0).then_some(AutoGcBackoff {
            since: now,
            interval_secs: 0,
            size_after,
            held,
        });
    }
    let interval_secs = previous
        .map_or(AUTO_GC_CHECK_INTERVAL.as_secs(), |b| b.interval_secs)
        .max(AUTO_GC_CHECK_INTERVAL.as_secs())
        .saturating_mul(2)
        .min(AUTO_GC_MAX_BACKOFF.as_secs());
    Some(AutoGcBackoff {
        since: now,
        interval_secs,
        size_after,
        held,
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

/// The start condition of every automatic sweep, whichever driver asks: the
/// store is over the trigger, leaving out what the last sweep found held by
/// target directories, and that sweep left no backoff that still holds.
/// `kache gc` does not ask.
pub(crate) fn auto_gc_sweep_due(config: &Config, total: u64) -> bool {
    auto_gc_due_at(
        read_auto_gc_backoff(&config.cache_dir),
        unix_now_secs(),
        total,
        config.max_size,
    )
}

fn auto_gc_due_at(backoff: Option<AutoGcBackoff>, now: u64, total: u64, max_size: u64) -> bool {
    total.saturating_sub(recent_held_bytes(backoff, now)) > auto_gc_threshold(max_size)
        && !auto_gc_backoff_holds(backoff, now, total, max_size)
}

/// Called by every automatic driver after a sweep that left the store at
/// `size_after`, `held` bytes of it held by target directories: store the
/// next backoff, or clear it once the store is back under the trigger.
pub(crate) fn record_auto_gc_outcome(config: &Config, size_after: u64, held: u64) {
    let path = auto_gc_backoff_path(&config.cache_dir);
    let next = next_auto_gc_backoff(
        read_auto_gc_backoff(&config.cache_dir),
        unix_now_secs(),
        size_after,
        held,
        config.max_size,
    );
    let Some(next) = next else {
        let _ = std::fs::remove_file(&path);
        return;
    };
    tracing::info!("{}", auto_gc_outcome_line(&next, config.max_size));
    if let Ok(json) = serde_json::to_vec(&next)
        && let Err(e) = crate::atomic::atomic_replace(&path, &json)
    {
        tracing::debug!("auto-gc: could not write {}: {e:#}", path.display());
    }
}

/// What a recorded sweep outcome says in the log.
fn auto_gc_outcome_line(outcome: &AutoGcBackoff, max_size: u64) -> String {
    if outcome.interval_secs > 0 {
        format!(
            "auto-gc: store still at {} after the sweep, {} of it held by target directories \
             (max {max_size}); next automatic sweep in {}s at the earliest",
            outcome.size_after, outcome.held, outcome.interval_secs
        )
    } else {
        format!(
            "auto-gc: store at {} after the sweep, {} of it held by target directories \
             that no sweep can free (max {max_size})",
            outcome.size_after, outcome.held
        )
    }
}

/// Test hook: move the recorded sweep `secs` into the past.
#[cfg(test)]
pub(crate) fn age_auto_gc_record_for_test(cache_dir: &Path, secs: u64) {
    let mut record = read_auto_gc_backoff(cache_dir).expect("a recorded sweep");
    record.since -= secs;
    let json = serde_json::to_vec(&record).unwrap();
    std::fs::write(auto_gc_backoff_path(cache_dir), json).unwrap();
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
    // A `[cache.volumes]` shard is judged against its own budget and backoff.
    let swept = config.for_store_dir(store.cache_dir(), crate::volume_gc::filesystem_bytes);
    if !auto_gc_sweep_due(&swept, total) {
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
        swept.max_size,
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
    maybe_notice_unknown_layout(config, args, &event_root, now_epoch_secs());
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
    let rustc_route = volume_route_path_rustc(args);
    let mut fallback_store = None;
    let trace_store_open = crate::phase_trace::phase("store_open");
    let store = if args.is_primary || (config.clean_incremental && args.incremental.is_some()) {
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

    let Some(store) = store else {
        return passthrough_with_event(
            config,
            args,
            crate_name,
            &event_root,
            start,
            "store unavailable",
        );
    };

    let keyed = match compute_rustc_cache_key(
        config,
        compiler,
        args,
        workspace_root.as_deref(),
        invocation_start_ns,
        Some(&store),
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
            // `{:#}` walks the cause chain; see `uncacheable_reason`.
            tracing::warn!("failed to compute cache key for {}: {:#}", crate_name, e);
            return passthrough_with_event(
                config,
                args,
                crate_name,
                &event_root,
                start,
                uncacheable_reason(&e),
            );
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
        // Cargo starts pipelined consumers on the rmeta, before this key.
        crate::out_dir_alias::register_before_compile();
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
        after_rustc_exit(result.exit_code, &result.stderr, &args.externs);
        if result.exit_code != 0 {
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

    drop(trace_store_open);
    let trace_remember = crate::phase_trace::phase("remember_target_root");
    // Any unit may be a build's first; the hint is rate-limited per target.
    if let Some(target_dir) = prestage_target_dir(args) {
        crate::prestage::maybe_hint(config, &target_dir);
    }
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
                    hit_store,
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
                    uncacheable_reason(&e),
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
            &store,
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
    after_rustc_exit(result.exit_code, &result.stderr, &args.externs);
    if result.exit_code != 0 {
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
            // A large executable just stored is the next build's restore.
            if stores_prestage_candidate(&prepared_store.files)
                && let Some(output_dir) = rustc_output_dir(args)
                && let Some(meta) = store.stored_meta(&cache_key)
            {
                let shared =
                    shared_inode_loadable(args, platform::current().may_share_restored_loadables());
                remember_prestaged_executables(config, compiler, args, &output_dir, shared, &meta);
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

/// Whether a store's outputs include a file large enough to prestage.
fn stores_prestage_candidate(files: &[(PathBuf, String)]) -> bool {
    files.iter().any(|(path, _)| {
        std::fs::metadata(path).is_ok_and(|m| m.len() >= crate::prestage::MIN_BYTES)
    })
}

/// Whether a copy the daemon staged for `cached_file` took its place at
/// `target_path` ([`crate::prestage`]). Only a large private copy is staged.
fn take_prestaged(
    strategy: link::LinkStrategy,
    cached_file: &crate::store::CachedFile,
    target_path: &Path,
) -> bool {
    strategy == link::LinkStrategy::Copy
        && cached_file.size >= crate::prestage::MIN_BYTES
        && crate::prestage::take(target_path, &cached_file.hash)
}

/// The target directory a compile writes under, for [`crate::prestage`].
/// [`RustcArgs::target_dir`] answers the profile directory for a build
/// script compiled into `<profile>/build/<unit>`; this answers its parent.
fn prestage_target_dir(args: &RustcArgs) -> Option<PathBuf> {
    if let Some(out_dir) = &args.out_dir
        && let Some(profile) = crate::cargo_layout::build_script_dir_profile(out_dir)
    {
        return profile.parent().map(Path::to_path_buf);
    }
    args.target_dir()
}

/// Where a compile's outputs go: the `-o` path's directory, or `--out-dir`.
fn rustc_output_dir(args: &RustcArgs) -> Option<PathBuf> {
    match (&args.output, &args.out_dir) {
        (Some(output), _) => Some(output.parent().unwrap_or(Path::new(".")).to_path_buf()),
        (None, Some(dir)) => Some(dir.clone()),
        (None, None) => None,
    }
}

/// Where a cached output goes: the exact `-o` path for the primary output
/// in `-o` mode, the output directory for everything else.
fn artifact_target_path(args: &RustcArgs, output_dir: &Path, name: &str) -> PathBuf {
    match &args.output {
        Some(output) if name == output.file_name().unwrap_or_default().to_string_lossy() => {
            output.clone()
        }
        _ => output_dir.join(name),
    }
}

/// Record the entry's large private-copy executables for this target
/// directory, so the daemon can copy them ahead of the next build's restore
/// (crate::prestage).
fn remember_prestaged_executables(
    config: &Config,
    compiler: &RustcCompiler,
    args: &RustcArgs,
    output_dir: &Path,
    shared_loadable: Option<ArtifactKind>,
    meta: &crate::store::EntryMeta,
) {
    let Some(target_dir) = prestage_target_dir(args) else {
        return;
    };
    for file in &meta.files {
        let kind = compiler.classify_output(args, &file.name);
        if file.size >= crate::prestage::MIN_BYTES
            && restore_link_strategy(kind, file.executable, shared_loadable)
                == link::LinkStrategy::Copy
        {
            crate::prestage::remember(
                &config.cache_dir,
                &target_dir,
                &artifact_target_path(args, output_dir, &file.name),
                &file.hash,
                file.size,
            );
        }
    }
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
    store: &Store,
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
    let store_path = store.blob_path(&cached_file.hash);
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
            // A large executable may already sit beside its destination,
            // copied by the daemon while the build ran (crate::prestage).
            let staged = take_prestaged(strategy, cached_file, target_path);
            if !staged {
                link::link_to_target(&store_path, target_path, strategy).with_context(|| {
                    format!(
                        "{context}: linking {} -> {}",
                        store_path.display(),
                        target_path.display()
                    )
                })?;
            }
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

/// Compute the rustc cache key. With `store` present the hasher is backed by
/// the persistent SQLite hash cache; without it a store-free hasher still
/// batches hashing through the daemon. The key value is identical either way:
/// the cache only changes how it's computed.
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
        store,
        &meta,
        result,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        record_closure.then_some(store),
    ))
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
fn record_known_file_hashes(store: &Store, restored: &[(crate::cache_key::FileFingerprint, &str)]) {
    let _trace = crate::phase_trace::phase("memo_restored");
    let restored: Vec<_> = restored
        .iter()
        .map(|(fingerprint, hash)| (fingerprint.clone(), *hash))
        .collect();
    store.record_verified_file_hashes(&restored);
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
    store: &Store,
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
        let _ = store.remove_entry(&meta.cache_key);
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
        let _ = store.remove_entry(&meta.cache_key);
        anyhow::bail!(
            "cached entry for {} has no dep-info artifact named {} required by active \
             extra_inputs; evicting the legacy entry and recompiling",
            meta.crate_name,
            expected.to_string_lossy()
        );
    }

    let Some(output_dir) = rustc_output_dir(args) else {
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
        let blob = store.blob_path(&cached_file.hash);
        let raw = match read_cached_dep_info_blob(&blob, extra_inputs.is_some()) {
            Ok(Some(raw)) => raw,
            Ok(None) => continue,
            Err(error) => {
                let _ = store.remove_entry(&meta.cache_key);
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
                    let _ = store.remove_entry(&meta.cache_key);
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
                let _ = store.remove_entry(&meta.cache_key);
                anyhow::bail!(
                    "cached dep-info for {} has no dependencies; evicting the entry and recompiling",
                    meta.crate_name
                );
            }
            Err(error) => {
                let _ = store.remove_entry(&meta.cache_key);
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
                let _ = store.remove_entry(&meta.cache_key);
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
    remember_prestaged_executables(config, compiler, args, &output_dir, shared_loadable, meta);

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

        let target_path = artifact_target_path(args, &output_dir, &cached_file.name);

        // Per-file dispatch by artifact kind: `classify_output` picks
        // the kind, `plan_post_restore` the actions — no ad-hoc filename
        // matching at the call site.
        let kind = compiler.classify_output(args, &cached_file.name);
        let restored = materialize_cached_artifact(
            store,
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

    record_known_file_hashes(store, &exact_restores);

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
    after_rustc_exit(result.exit_code, &result.stderr, &args.externs);
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

/// Category of the passthrough reason for a rustc compile kache could not
/// key. Everything after it is the key error.
const UNCACHEABLE_REASON: &str = "uncacheable|";

/// The passthrough reason for a compile whose key could not be computed.
///
/// `{error:#}`: the alternate form walks the cause chain. Plain `{error}`
/// prints only the outermost context, which is how the substrate bench's 60
/// dep-info refusals stayed undiagnosable: the log said "dep-info pre-pass
/// failed for src/lib.rs" and dropped rustc's own reason underneath it
/// (kunobi-ninja/kache#431).
fn uncacheable_reason(error: &anyhow::Error) -> String {
    format!("{UNCACHEABLE_REASON}{error:#}")
}

/// What a passthrough reason quotes of rustc's own output. A failed pre-pass
/// ran rustc's macro expansion, so a macro that cannot write fails there
/// first, and the key error carries rustc's first error line. Every other
/// reason is kache's own words: a store error that says "Permission denied"
/// is not a macro failing to write.
fn rustc_output_in(reason: &str) -> &str {
    reason.strip_prefix(UNCACHEABLE_REASON).unwrap_or_default()
}

/// After a failed compile, the hint and the deny markers for a shared
/// read-only `OUT_DIR` a macro could not write into. Both need rustc's words.
/// `rustc_output` is the captured stderr of a keyed, deferred or adaptive
/// compile, or the key error an `uncacheable|` passthrough reason quotes (see
/// `rustc_output_in`).
///
/// Every other passthrough sends rustc's stderr straight to Cargo: a
/// compiler refusal such as `--unpretty`, a user bypass or exclude rule, the
/// preserve-incremental and untrusted-backend lanes, and kache's own errors.
/// A write failure there gets rustc's error alone. Covering those lanes would
/// mean capturing their stderr.
fn after_rustc_exit(exit_code: i32, rustc_output: &str, externs: &[ExternDep]) {
    if exit_code != 0 {
        crate::out_dir_alias::after_failed_compile(rustc_output, externs);
    }
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
    let reason = reason.into();
    let output = passthrough(
        args,
        config.fallback.as_deref(),
        config.preserve_incremental,
    )?;
    let elapsed = start.elapsed().as_millis() as u64;
    after_rustc_exit(output.exit_code, rustc_output_in(&reason), &args.externs);
    log_passthrough_event(config, root, crate_name, elapsed, reason, &output);
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
/// invocations from all sending one. A failed discovery (cargo metadata
/// hanging on a git dependency, say) is recorded too, and later compiles wait
/// out a backoff before trying again, since the wrapper that tries holds up
/// its own compile (kunobi-ninja/kache#698).
fn maybe_trigger_prefetch(config: &Config, args: &RustcArgs) {
    maybe_trigger_prefetch_with(config, args, now_epoch_secs(), || {
        crate::build_intent::discover(Some(args))
    });
}

/// What [`maybe_trigger_prefetch_with`] did, so a test can tell which of its
/// early returns it took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PrefetchTrigger {
    NoRemote,
    NoSession,
    NotDue,
    /// The marker cannot be opened as a regular file.
    NoMarker,
    /// Another process holds the marker's lock: it is sending the hint.
    Busy,
    DiscoveryFailed,
    Sent,
}

/// [`maybe_trigger_prefetch`] with the clock and the discovery supplied.
fn maybe_trigger_prefetch_with(
    config: &Config,
    args: &RustcArgs,
    now: u64,
    discover: impl FnOnce() -> Option<kache_core::BuildIntent>,
) -> PrefetchTrigger {
    if config.remote.is_none() {
        return PrefetchTrigger::NoRemote;
    }
    let root = rustc_event_root(args);
    let session_id = session_id_for_event(config, &root, now);
    if session_id.is_empty() {
        return PrefetchTrigger::NoSession;
    }
    let marker = prefetch_marker_path(config, &root);
    if !prefetch_due(
        &parse_prefetch_marker(&std::fs::read_to_string(&marker).unwrap_or_default()),
        &session_id,
        now,
    ) {
        return PrefetchTrigger::NotDue;
    }
    let Some(lock_file) = open_marker_for_lock(&marker) else {
        return PrefetchTrigger::NoMarker;
    };
    // std::fs::File::try_lock (1.89+) is cross-platform: flock(2) on Unix,
    // LockFileEx on Windows. Lock auto-releases when `lock_file` is dropped.
    if lock_file.try_lock().is_err() {
        return PrefetchTrigger::Busy; // Another wrapper is already sending the prefetch hint
    }
    // Re-check through the locked handle: another process may have sent the
    // hint between our first read and acquiring the lock, and on Windows the
    // lock blocks reads from any other handle (#348).
    let locked = read_locked_marker(&lock_file);
    let state = parse_prefetch_marker(&locked);
    if !prefetch_due(&state, &session_id, now) {
        return PrefetchTrigger::NotDue;
    }

    // Gather ALL dependency crate names in compilation order (leaves first).
    // This gives the daemon a comprehensive prefetch list that works even on
    // cold CI runners where the local SQLite store is empty.
    let Some(build_intent) = discover() else {
        let record = failed_prefetch_marker(&session_id, now, &state);
        tracing::debug!("prefetch hint skipped, discovery failed; marker now {record}");
        write_locked_marker(&lock_file, &record);
        return PrefetchTrigger::DiscoveryFailed;
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
    PrefetchTrigger::Sent
}

/// The output directory of a compile Cargo drove into a directory no layout
/// in [`crate::cargo_layout`] knows, or `None`.
///
/// `cargo_crate_name` is `CARGO_CRATE_NAME`, which Cargo sets to the crate it
/// compiles. A rustc that a build script runs to probe the compiler inherits
/// the script's environment but compiles a crate of its own name, so it does
/// not count, and neither does a compile no Cargo drove.
fn unrecognized_cargo_layout<'a>(
    args: &'a RustcArgs,
    cargo_crate_name: Option<&std::ffi::OsStr>,
) -> Option<&'a Path> {
    let out_dir = args.out_dir.as_deref()?;
    let crate_name = args.crate_name.as_deref()?;
    (cargo_crate_name == Some(std::ffi::OsStr::new(crate_name))
        && !crate::cargo_layout::is_cargo_unit_dir(out_dir))
    .then_some(out_dir)
}

/// Tell the person once per build session when Cargo writes somewhere kache
/// does not recognize. The compile still runs and caches, but build-script
/// caching, the shared `OUT_DIR` and the cross-checkout path mapping that
/// depend on the layout are off, and a newer Cargo is the likely cause.
/// Only a directory under Cargo's own `CACHEDIR.TAG` counts: another build
/// system can set Cargo's variables for rustc without using its layout.
/// Returns whether the line was shown.
fn maybe_notice_unknown_layout(config: &Config, args: &RustcArgs, root: &str, now: u64) -> bool {
    let cargo_crate_name = std::env::var_os("CARGO_CRATE_NAME");
    let Some(out_dir) = unrecognized_cargo_layout(args, cargo_crate_name.as_deref()) else {
        return false;
    };
    if cargo_workspace_of(out_dir).is_none() {
        return false;
    }
    tracing::debug!("unrecognized Cargo layout: {}", out_dir.display());
    let session_id = session_id_for_event(config, root, now);
    if session_id.is_empty() {
        return false;
    }
    let marker = session_marker_path(config, root).with_extension("layout");
    if std::fs::read_to_string(&marker).is_ok_and(|seen| seen == session_id) {
        return false;
    }
    let Some(lock_file) = open_marker_for_lock(&marker) else {
        return false;
    };
    if lock_file.try_lock().is_err() || read_locked_marker(&lock_file) == session_id {
        return false;
    }
    write_locked_marker(&lock_file, &session_id);
    crate::notice::show(&format!(
        "[kache] Cargo is writing to a directory layout kache does not recognize ({}). \
         Builds still work, but build-script caching and cross-checkout reuse are reduced. \
         Please report your `cargo -V` at https://github.com/kunobi-ninja/kache/issues",
        out_dir.display()
    ));
    true
}

/// Where [`maybe_trigger_prefetch`] records the session it sent a hint for.
fn prefetch_marker_path(config: &Config, root: &str) -> PathBuf {
    session_marker_path(config, root).with_extension("prefetch")
}

/// First wait after a failed discovery; each further failure doubles it.
const PREFETCH_RETRY_MIN_SECS: u64 = 30;
/// Longest wait between discovery attempts.
const PREFETCH_RETRY_MAX_SECS: u64 = 600;

/// What the `.prefetch` marker records for a build root.
#[derive(Debug, PartialEq, Eq)]
enum PrefetchMarker<'a> {
    /// Nothing usable: never written, or unreadable.
    Unset,
    /// The hint went out for this session.
    Sent(&'a str),
    /// Discovery failed `attempts` times in a row; wait until `until`.
    Failed { until: u64, attempts: u32 },
}

/// Read a marker: a session id after a hint, or
/// `fail:<session>:<until>:<attempts>` after failed discoveries.
fn parse_prefetch_marker(content: &str) -> PrefetchMarker<'_> {
    let content = content.trim();
    if content.is_empty() {
        return PrefetchMarker::Unset;
    }
    let Some(record) = content.strip_prefix("fail:") else {
        return PrefetchMarker::Sent(content);
    };
    // The session id comes first, so split from the right.
    let mut fields = record.rsplitn(3, ':');
    let attempts = fields.next().and_then(|field| field.parse().ok());
    let until = fields.next().and_then(|field| field.parse().ok());
    match (until, attempts) {
        (Some(until), Some(attempts)) => PrefetchMarker::Failed { until, attempts },
        _ => PrefetchMarker::Unset,
    }
}

/// Whether this compile should try to send the hint.
fn prefetch_due(marker: &PrefetchMarker<'_>, session_id: &str, now: u64) -> bool {
    match marker {
        PrefetchMarker::Unset => true,
        PrefetchMarker::Sent(sent) => *sent != session_id,
        PrefetchMarker::Failed { until, .. } => now >= *until,
    }
}

/// Wait after the `attempts`-th failure in a row: 30 s, 60 s, 120 s, ...,
/// never more than ten minutes.
fn prefetch_retry_secs(attempts: u32) -> u64 {
    let doublings = attempts.saturating_sub(1).min(16);
    (PREFETCH_RETRY_MIN_SECS << doublings).min(PREFETCH_RETRY_MAX_SECS)
}

/// The marker to write after a discovery that failed at `now`, following
/// `previous`. A failure run carries across sessions: a workspace whose
/// discovery hangs does not stop hanging when the next build starts.
fn failed_prefetch_marker(session_id: &str, now: u64, previous: &PrefetchMarker<'_>) -> String {
    let attempts = match previous {
        PrefetchMarker::Failed { attempts, .. } => attempts.saturating_add(1),
        _ => 1,
    };
    let until = now.saturating_add(prefetch_retry_secs(attempts));
    format!("fail:{session_id}:{until}:{attempts}")
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
mod tests;
