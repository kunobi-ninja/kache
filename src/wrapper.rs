use anyhow::{Context, Result};
use bytesize::ByteSize;
use chrono::Utc;
use std::path::{Component, Path, PathBuf};

use crate::args::RustcArgs;
use crate::cache_key::FileHashStats;
use crate::compile;
use crate::compiler::cc::CcCompiler;
use crate::compiler::rustc::RustcCompiler;
use crate::compiler::{
    ArtifactKind, ArtifactSet, Compiler, KeyCtx, classify_by_filename, plan_post_restore, platform,
};
use crate::config::Config;
use crate::events::{self, BuildEvent, EventResult};
use crate::incremental_policy::{AdaptiveUnit, Lease};
use crate::link;
use crate::store::{BuildClaim, Store, StorePutResult};

/// Check whether progress lines should be printed to stderr.
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
/// modes may write them to the compiler wrapper's stderr. Cargo fingerprints
/// that stderr and replays it on later builds; keeping the default silent
/// prevents stale `still compiling` lines from appearing at build start.
fn heartbeat_stderr_enabled(level: u8) -> bool {
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

/// Print a concise progress line to stderr.
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

    eprintln!("[kache] {crate_name}: {label} ({elapsed_str}{size_str})");
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
pub(crate) const WARN_SESSION_SECS: u64 = 300;

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

/// Print `message` to stderr at most once per [`WARN_SESSION_SECS`] window,
/// even across the hundreds of parallel wrapper processes a single build spawns.
///
/// Each wrapper is its own process, so a `static Once` only dedups within one
/// compilation — a build then repeats the same advisory hundreds of times
/// (kunobi-ninja/kache#508). Dedup therefore has to be cross-process: a marker
/// file holding a timestamp, guarded by an flock so two wrappers can't decide
/// to warn simultaneously.
///
/// Best-effort by construction: if the marker can't be created we warn rather
/// than go silent — a duplicated advisory beats a swallowed one.
///
/// Returns whether this call actually emitted the message (for tests).
pub(crate) fn warn_once_per_session(marker: &Path, session_secs: u64, message: &str) -> bool {
    if let Ok(metadata) = std::fs::symlink_metadata(marker)
        && metadata.file_type().is_symlink()
    {
        eprintln!("{message}");
        return true;
    }
    if marker_is_fresh(marker, session_secs) {
        return false; // already warned this session
    }
    let Some(lock_file) = open_marker_for_lock(marker) else {
        eprintln!("{message}");
        return true;
    };
    match lock_file.try_lock() {
        Ok(()) => {}
        // Contended: another wrapper is emitting this warning right now.
        Err(std::fs::TryLockError::WouldBlock) => return false,
        // The lock itself is broken — NOT contention (e.g. a filesystem with no
        // working locks). Treating that as "someone else is warning" would
        // silence the advisory forever, so warn best-effort instead.
        Err(std::fs::TryLockError::Error(e)) => {
            tracing::debug!("warn-once marker lock failed ({e}); warning anyway");
            eprintln!("{message}");
            return true;
        }
    }
    // Re-check under the lock — another wrapper may have warned between our
    // first check and acquiring the lock. Read through the handle that OWNS the
    // lock: on Windows the lock is mandatory (`LockFileEx`) and blocks
    // cross-handle reads, so `marker_is_fresh` (which opens its own handle)
    // would always read "stale" here and let a second wrapper warn again — on
    // the very platform this advisory targets. Same reason
    // `write_marker_timestamp` writes through the locked handle (#348).
    finish_warn_once_per_session(&lock_file, session_secs, message)
}

/// Re-check and update a warn-once marker while its lock is held, then release
/// the lock explicitly. Relying on `File` drop is racy with concurrent process
/// spawning: a fork can briefly inherit a duplicate descriptor that keeps the
/// lock alive after this function returns.
fn finish_warn_once_per_session(
    lock_file: &std::fs::File,
    session_secs: u64,
    message: &str,
) -> bool {
    let emitted = if marker_file_is_fresh(lock_file, session_secs) {
        false
    } else {
        eprintln!("{message}");
        write_marker_timestamp(lock_file);
        true
    };
    let _ = std::fs::File::unlock(lock_file);
    emitted
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

/// How often the wrapper is willing to re-run the store-size query. Between
/// checks the hot-path cost is a single `stat()` on the stamp file.
const AUTO_GC_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(300);

/// Slack over `max_size` before a background GC is spawned, in percent.
/// `evict()` already targets 90% of `max_size`; triggering only above 110%
/// keeps the two thresholds apart so the store doesn't thrash at the boundary.
const AUTO_GC_SLACK_PERCENT: u64 = 10;

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
    let threshold = config
        .max_size
        .saturating_add(config.max_size / 100 * AUTO_GC_SLACK_PERCENT);
    if total <= threshold {
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

/// Spawn a fully detached `kache gc` if [`auto_gc_wanted`] says so. Never
/// waits on the child; stdio is null so it cannot pollute the compiler's
/// output streams.
fn maybe_spawn_auto_gc(config: &Config, store: &Store) {
    if !auto_gc_wanted(config, store) {
        return;
    }
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

    let mut cmd = std::process::Command::new(exe);
    cmd.arg("gc")
        .env("KACHE_AUTO_GC_WORKER", "1")
        .stdin(std::process::Stdio::null());

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

fn should_store_cc_result(exit_code: i32, has_artifacts: bool) -> bool {
    exit_code == 0 && has_artifacts
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

/// Run kache as a C-family compiler wrapper (`CC=kache cc`,
/// `CXX=kache c++`, etc.).
///
/// Caches the single-source `-c` object compile: parse → refuse-check
/// → cache key (preprocessor hash) → local store lookup → restore the
/// `.o` on hit, or compile + store on dup/miss. Everything else (link
/// mode, multi-source, unsafe flags) routes through [`cc_passthrough`].
///
/// This is the local-cache path. Remote cache + build-lock
/// coordination (which `wrapper::run` has for rustc) are deliberate
/// follow-ups — single-machine caching is the shipped concept.
pub fn run_cc(config: &Config, wrapper_args: &[String]) -> Result<i32> {
    let start = std::time::Instant::now();
    crate::link::set_windows_hardlink_restore(config.windows_hardlink);
    crate::link::set_storage_layout_advice(config.storage_layout_advice);
    crate::link::set_cow_warn_marker(warn_marker_path("cow", &config.cache_dir));
    warn_nonlocal_cache_fs_once(config);
    let compiler = CcCompiler::with_extra_allowlist_flags(config.cc_extra_allowlist_flags.clone())
        .with_base_dirs(config.base_dirs.clone());
    let parsed = compiler
        .parse(wrapper_args)
        .context("parsing cc-family arguments")?;
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
    if let Some(reason) = Config::user_bypass_reason(&crate_name, &parsed.rest) {
        tracing::debug!("cc invocation bypassed by user rule: {reason}");
        return cc_passthrough_with_event(config, &parsed, &crate_name, &event_root, start, reason);
    }

    let current_dir = std::env::current_dir().ok();
    let exclude_roots: Vec<_> = current_dir.iter().cloned().collect();
    if let Some(source) = parsed.sources.first()
        && Config::source_excluded(source, &exclude_roots)
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

    let store = match Store::open(config) {
        Ok(store) => store,
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

    // Compute the cache key (runs `cc -E -P` for the preprocessor
    // hash). On any failure — preprocessor error, missing compiler —
    // fall back to passthrough, which runs the real compiler and
    // surfaces the real diagnostic.
    let key_start = std::time::Instant::now();
    let file_hasher = crate::cache_key::FileHasher::new();
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
            tracing::debug!(
                "cc cache key failed for {}: {} — passthrough",
                crate_name,
                e
            );
            return cc_passthrough_with_event(
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
    tracing::debug!("cc cache key for {}: {}", crate_name, &cache_key[..16]);

    // ── Local cache lookup ───────────────────────────────────────
    let lookup_start = std::time::Instant::now();
    let lookup = match store.get(&cache_key) {
        Ok(lookup) => lookup,
        Err(e) => {
            tracing::warn!(
                "cc local store lookup failed for {}: {} — recompiling",
                crate_name,
                e
            );
            return cc_passthrough_with_event(
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
    if let Some(meta) = lookup {
        if meta.files.is_empty() {
            // Poisoned entry (earlier bug) — evict and recompile.
            tracing::warn!("cc cache entry for {} has no files, evicting", crate_name);
            lookup_rejection = "matching entry has no cached artifacts".to_string();
            let _ = store.remove_entry(&cache_key);
        } else if let Some(reason) = cc_cache_entry_rejection_reason(&parsed, &meta) {
            tracing::warn!(
                "cc cache entry for {} lacks artifacts required by this invocation ({reason}), evicting",
                crate_name,
            );
            lookup_rejection = reason.to_string();
            let _ = store.remove_entry(&cache_key);
        } else {
            let restore_start = std::time::Instant::now();
            if let Err(e) = restore_cc_from_cache(&store, &parsed, &meta) {
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
                    &parsed,
                    &crate_name,
                    &event_root,
                    start,
                    format!("restore failed: {e}"),
                );
            }
            let restore_ms = restore_start.elapsed().as_millis() as u64;
            let elapsed = start.elapsed().as_millis() as u64;
            let size: u64 = meta.files.iter().map(|f| f.size).sum();
            tracing::debug!(
                "cc local cache hit for {} ({})",
                crate_name,
                &cache_key[..16]
            );
            log_event(
                config,
                &event_root,
                &crate_name,
                EventResult::LocalHit,
                elapsed,
                meta.compile_time_ms,
                size,
                &cache_key,
                key_ms,
                lookup_ms,
                restore_ms,
                0,
            );
            print_progress(&crate_name, EventResult::LocalHit, elapsed, size);
            // Replay the cached compiler diagnostics so warnings still
            // surface on a cache hit.
            if !meta.stdout.is_empty() {
                print!("{}", meta.stdout);
            }
            if !meta.stderr.is_empty() {
                eprint!("{}", meta.stderr);
            }

            return Ok(0);
        }
    }

    // ── Cache miss — compile, then store ─────────────────────────
    // Key generation and lookup can take long enough for another process to
    // create an output. Recheck at the last possible wrapper boundary and run
    // the selected compiler directly if its pathname semantics are now needed.
    if parsed.requires_compiler_output_semantics() {
        return cc_direct_passthrough_with_event(
            config,
            &parsed,
            &crate_name,
            &event_root,
            start,
            "output appeared before compiler execution",
        );
    }
    let compile_start = std::time::Instant::now();
    let result = match compiler.execute(&parsed) {
        Ok(r) => r,
        // A spawn-level failure (missing binary, ENOMEM, fork pressure under
        // load) must not abort the build: fall back to passthrough so the
        // configured fallback wrapper still gets a chance and the user sees the
        // real compiler error rather than a kache anyhow chain.
        Err(e) => {
            return cc_passthrough_with_event(
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

    if !result.stdout.is_empty() {
        print!("{}", result.stdout);
    }
    if !result.stderr.is_empty() {
        eprint!("{}", result.stderr);
    }

    // Only store on a clean compile that actually produced its
    // object file. A failed compile (exit != 0) or one whose output
    // discovery came up empty is not cacheable — return the exit
    // code and let cargo see the failure.
    let store_start = std::time::Instant::now();
    let mut store_put = StorePutResult::default();
    let mut store_error = String::new();
    let store_candidate = should_store_cc_result(result.exit_code, !result.artifacts.is_empty());
    // The CC path has no remote upload pipeline, so remote configuration must
    // not bypass its local-store admission threshold.
    let admitted = store_admits_compile(config, compile_time_ms, false);
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
        let depinfo_anchor = cc_depinfo_rewrite_root(&parsed);
        let target = parsed.cache_target_arch();
        match prepare_cc_store_files(&result.artifacts, depinfo_anchor.as_deref()) {
            Ok(prepared) => match store.put_with_compile_time_independent(
                &cache_key,
                &crate_name,
                &[], // crate_types: n/a for cc objects
                &[], // features: n/a
                &target,
                "", // profile: n/a (opt level is in the key)
                &prepared.files,
                &result.stdout,
                &result.stderr,
                compile_time_ms,
            ) {
                Ok(result) => {
                    store_put = result;
                    // Store grew — throttled size check + detached background GC if over
                    // budget (kunobi-ninja/kache#497). Never blocks the compile path.
                    maybe_spawn_auto_gc(config, &store);
                }
                Err(e) => {
                    store_error = store_error_for_event(&e);
                    tracing::warn!(
                        "failed to store cc cache entry for {}: {}",
                        crate_name,
                        store_error
                    );
                }
            },
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
    // Configured fallback wrapper: `<fallback> <cc> <args>`.
    // kache's C/C++ coverage is narrower than its rustc support, so
    // the fallback is most valuable on this path. Falls through to a
    // plain passthrough if the fallback is not on PATH.
    if let Some(fb) = config.fallback.as_deref()
        && !force_direct
        && !parsed.requires_compiler_output_semantics()
    {
        let mut cmd = std::process::Command::new(fb);
        cmd.arg(&parsed.program);
        cmd.args(&parsed.rest);
        if let Some(output) = run_fallback(cmd, fb)? {
            return Ok(output);
        }
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

#[cfg(test)]
fn cc_cache_entry_satisfies_invocation(
    parsed: &crate::compiler::cc::CcArgs,
    meta: &crate::store::EntryMeta,
) -> bool {
    cc_cache_entry_rejection_reason(parsed, meta).is_none()
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

    if !has_object {
        Some("matching entry lacks the object artifact required by this invocation")
    } else if parsed.depinfo_output_path().is_some() && !has_depinfo {
        Some("matching entry lacks dep-info required by this invocation")
    } else {
        None
    }
}

fn cc_depinfo_rewrite_root(parsed: &crate::compiler::cc::CcArgs) -> Option<std::path::PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    cc_depinfo_rewrite_root_from_cwd(parsed, &cwd)
}

fn rustc_event_root(args: &RustcArgs) -> String {
    event_root_string(event_root_override().or_else(|| {
        args.workspace_root()
            .or_else(|| std::env::current_dir().ok())
    }))
}

fn cc_event_root(parsed: &crate::compiler::cc::CcArgs) -> String {
    event_root_string(
        event_root_override()
            .or_else(|| cc_depinfo_rewrite_root(parsed).or_else(|| std::env::current_dir().ok())),
    )
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
    let plan = plan_post_restore(kind);
    anyhow::ensure!(
        plan.iter().all(|action| action.is_content_transform()),
        "cc restore: artifact requires a post-publish path mutation"
    );

    let blob = store.blob_path(&cached.hash);
    if !blob.exists() {
        anyhow::bail!(
            "cc restore: blob for {} (hash {}) was evicted before restore: {}",
            cached.name,
            &cached.hash[..16.min(cached.hash.len())],
            blob.display()
        );
    }

    if plan.is_empty() {
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
    for action in plan {
        content = action.transform(content, depinfo_anchor);
    }
    link::prepare_writable_target_from_bytes(target, &content)
        .with_context(|| format!("cc restore: staging transformed {}", target.display()))
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
    if parsed.requires_compiler_output_semantics() {
        anyhow::bail!("cc restore: existing output requires compiler passthrough semantics");
    }

    let depinfo_anchor =
        cc_depinfo_rewrite_root(parsed).unwrap_or_else(|| Path::new(".").to_path_buf());
    let mut prepared = Vec::new();
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
            _ => {
                tracing::debug!(
                    "cc restore: cached artifact {} has unsupported kind {:?}; skipping",
                    cached.name,
                    kind
                );
                continue;
            }
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
    }
    publish_prepared_cc_artifacts(prepared)
}

/// Run kache in RUSTC_WRAPPER mode.
///
/// This is the hot path — called once per crate by cargo.
/// Flow: parse args → compute cache key → check store → link on hit → compile on miss → store → link
pub fn run(config: &Config, wrapper_args: &[String]) -> Result<i32> {
    let start = std::time::Instant::now();
    crate::link::set_windows_hardlink_restore(config.windows_hardlink);
    crate::link::set_storage_layout_advice(config.storage_layout_advice);
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
    let extra_inputs =
        crate::extra_inputs::ExtraInputsSnapshot::resolve_for_rustc(&args, &extra_inputs_hasher)
            .with_context(|| format!("resolving extra_inputs for {crate_name}"))?;

    validate_extra_inputs_freshness_mode(&args, extra_inputs.is_some())?;

    let extra_inputs_hash_stats = extra_inputs_hasher.stats();
    let extra_inputs_too_new = extra_inputs_hasher.too_new();
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
    )?;

    if exit == 0 {
        complete_current_extra_inputs_after_success(
            effective_config,
            &args,
            extra_inputs.as_ref(),
        )?;
    }
    Ok(exit)
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
    if before != after.as_ref() || hasher.too_new() {
        tracing::warn!(
            "not caching {crate_name}: extra_inputs changed while the compiler was running"
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
        heartbeat_stderr_enabled(progress_level()),
    );
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
    let adaptive_unit = managed_incremental_unit(
        config,
        args,
        std::env::var_os("CARGO_PRIMARY_PACKAGE").is_some(),
        || extra_inputs.is_some(),
    );

    // Evaluate every cheap cache-eligibility gate before the learned fast
    // path. In particular, changing an exclusion or executable-cache policy
    // must take effect immediately even when this unit was already active.
    let refuse = compiler.refuse_reasons(args);
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
        .filter(|source| Config::source_excluded(source, &exclude_roots));
    // User bypass rules (#222). Same fail-closed contract as `exclude`, and
    // gating the incremental fast path on it too: a bypassed unit must not
    // slip back into caching through the managed-incremental route.
    let user_bypass = Config::user_bypass_reason(crate_name, &args.all_args);
    let skip_user_facing = args.is_user_facing_executable() && !config.cache_executables;

    if incremental_fast_path_allowed(
        !refuse.is_empty(),
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
    let store = if daemon_local {
        None
    } else if args.is_primary || (config.clean_incremental && args.incremental.is_some()) {
        match Store::open(config) {
            Ok(store) => Some(store),
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
    ) {
        Ok(keyed) => keyed,
        Err(e) => {
            // `{e:#}` — the alternate form walks the cause chain. Plain
            // `{e}` prints only the outermost context, which is how the
            // substrate bench's 60 dep-info refusals stayed undiagnosable:
            // the log said "dep-info pre-pass failed for src/lib.rs" and
            // dropped rustc's own reason underneath it (kunobi-ninja/kache#431).
            tracing::warn!("failed to compute cache key for {}: {:#}", crate_name, e);
            return passthrough_with_event(
                config,
                args,
                crate_name,
                &event_root,
                start,
                format!("uncacheable|{e:#}"),
            );
        }
    };
    let ComputedKey {
        cache_key,
        key_ms,
        key_hash_stats,
        key_too_new,
    } = keyed;
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

    // Daemon fast path (kunobi-ninja/kache#565): ask the running daemon
    // before opening SQLite. A served hit returns here; every other outcome
    // (miss, fallback, no daemon, restore failure) opens the store and runs
    // the fully local path below with the already-computed key.
    let mut store = store;
    if daemon_local {
        if let Some(exit) = try_daemon_local_hit(
            config,
            compiler,
            args,
            &cache_key,
            crate_name,
            &event_root,
            start,
            key_ms,
            key_hash_stats,
            extra_inputs,
        ) {
            reset_adaptive_unit(adaptive_unit.as_ref());
            return Ok(exit);
        }
        match Store::open(config) {
            Ok(s) => store = Some(s),
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

    tracing::debug!("cache key for {}: {}", crate_name, &cache_key[..16]);

    // 1. Check local store
    let lookup_start = std::time::Instant::now();
    let lookup_result = match store.get(&cache_key) {
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
    let lookup_ms = lookup_start.elapsed().as_millis() as u64;
    if let Some(meta) = lookup_result {
        // Safety: skip entries with no cached files (poisoned by earlier bugs)
        if meta.files.is_empty() {
            tracing::warn!(
                "cache entry for {} has no files, evicting and recompiling",
                crate_name
            );
            let _ = store.remove_entry(&cache_key);
        } else {
            tracing::debug!("local cache hit for {} ({})", crate_name, &cache_key[..16]);
            let restore_start = std::time::Instant::now();
            if let Err(e) = restore_from_cache(
                config,
                compiler,
                &BlobSource::Store(&store),
                args,
                &meta,
                extra_inputs,
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
            let restore_ms = restore_start.elapsed().as_millis() as u64;
            let elapsed = start.elapsed().as_millis() as u64;
            let size: u64 = meta.files.iter().map(|f| f.size).sum();
            log_event_with_hash_stats(
                config,
                &event_root,
                crate_name,
                EventResult::LocalHit,
                elapsed,
                meta.compile_time_ms,
                size,
                &cache_key,
                key_ms,
                key_hash_stats,
                lookup_ms,
                restore_ms,
                0,
            );
            print_progress(crate_name, EventResult::LocalHit, elapsed, size);
            // Print cached stdout/stderr
            if !meta.stdout.is_empty() {
                print!("{}", meta.stdout);
            }
            if !meta.stderr.is_empty() {
                eprint!("{}", meta.stderr);
            }
            clean_incremental_dir(config, args);
            reset_adaptive_unit(adaptive_unit.as_ref());

            return Ok(0);
        }
    }

    // Build-session detection: send prefetch hint before remote work.
    // Placed after local-hit check so warm-cache invocations skip this entirely.
    maybe_trigger_prefetch(config, args);

    // 2. Check remote cache via daemon (if configured)
    if config.remote.is_some() {
        let entry_dir = store.entry_dir(&cache_key);
        match crate::daemon::send_remote_check(config, &cache_key, &entry_dir, crate_name) {
            Some(result) if result.found => {
                // Daemon downloaded it — now read from local store and restore
                if let Ok(Some(meta)) = store.get(&cache_key) {
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
                    let restore_start = std::time::Instant::now();
                    if let Err(e) = restore_from_cache(
                        config,
                        compiler,
                        &BlobSource::Store(&store),
                        args,
                        &meta,
                        extra_inputs,
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
                    let restore_ms = restore_start.elapsed().as_millis() as u64;
                    let elapsed = start.elapsed().as_millis() as u64;
                    let size: u64 = meta.files.iter().map(|f| f.size).sum();
                    log_event_with_hash_stats(
                        config,
                        &event_root,
                        crate_name,
                        event_result,
                        elapsed,
                        meta.compile_time_ms,
                        size,
                        &cache_key,
                        key_ms,
                        key_hash_stats,
                        lookup_ms,
                        restore_ms,
                        0,
                    );
                    print_progress(crate_name, event_result, elapsed, size);
                    if !meta.stdout.is_empty() {
                        print!("{}", meta.stdout);
                    }
                    if !meta.stderr.is_empty() {
                        eprint!("{}", meta.stderr);
                    }
                    clean_incremental_dir(config, args);
                    reset_adaptive_unit(adaptive_unit.as_ref());
                    return Ok(0);
                }
            }
            Some(_) => {} // not in remote, continue to compile
            None => {}    // daemon unreachable, continue to compile
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

    // 3. Cache miss — claim the key, then re-check under the build lock.
    let (lock, committed) = match store.claim_build(&cache_key) {
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
    };

    if let Some(meta) = committed {
        let restore_start = std::time::Instant::now();
        if let Err(e) = restore_from_cache(
            config,
            compiler,
            &BlobSource::Store(&store),
            args,
            &meta,
            extra_inputs,
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
        let restore_ms = restore_start.elapsed().as_millis() as u64;
        let elapsed = start.elapsed().as_millis() as u64;
        let size: u64 = meta.files.iter().map(|f| f.size).sum();
        log_event_with_hash_stats(
            config,
            &event_root,
            crate_name,
            EventResult::LocalHit,
            elapsed,
            meta.compile_time_ms,
            size,
            &cache_key,
            key_ms,
            key_hash_stats,
            lookup_ms,
            restore_ms,
            0,
        );
        // Replay the original compiler diagnostics, exactly as the other hit
        // sites do, so a coalesced compile does not swallow warnings or notes.
        replay_cached_diagnostics(&meta, std::io::stdout(), std::io::stderr());
        clean_incremental_dir(config, args);
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
    let mut result = match compiler.execute(args) {
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
    };
    let compile_time_ms = compile_start.elapsed().as_millis() as u64;

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
    let extra_inputs_racy = args.is_primary
        && extra_inputs_changed_during_compile(config, args, extra_inputs, invocation_start_ns);
    if should_skip_cache_store_for_input_race(
        extra_inputs_racy,
        config.modified_input_guard,
        key_too_new,
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
            // Store grew — throttled size check + detached background GC if over
            // budget (kunobi-ninja/kache#497). Never blocks the compile path.
            maybe_spawn_auto_gc(config, &store);
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
    let store_ms = store_start.elapsed().as_millis() as u64;

    // 6. Async upload to remote (if configured) — sends job to the daemon
    if config.remote.is_some() {
        let entry_dir = store.entry_dir(&cache_key);
        if let Err(e) = crate::daemon::send_upload_job(config, &cache_key, &entry_dir, crate_name) {
            tracing::warn!("failed to send upload job to daemon: {}", e);
        }
    }

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
fn prepare_cc_store_files(
    artifacts: &ArtifactSet,
    depinfo_anchor: Option<&Path>,
) -> Result<PreparedCcStoreFiles> {
    use std::io::{Read, Write};

    let mut files = Vec::with_capacity(artifacts.outputs().len());
    let mut temporary_files = Vec::with_capacity(artifacts.outputs().len());
    for artifact in artifacts.outputs() {
        let mut staged = tempfile::Builder::new()
            .prefix("kache-cc-artifact-")
            .tempfile()
            .context("cc store: creating private artifact staging file")?;

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
            staged
                .write_all(normalized.as_bytes())
                .context("cc store: writing normalized dep-info staging file")?;
        } else {
            let mut source = std::fs::File::open(&artifact.path).with_context(|| {
                format!("cc store: opening artifact {}", artifact.path.display())
            })?;
            std::io::copy(&mut source, &mut staged).with_context(|| {
                format!("cc store: copying artifact {}", artifact.path.display())
            })?;
        }
        staged
            .flush()
            .context("cc store: flushing private artifact staging file")?;
        let staged = staged.into_temp_path();
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
/// Whether this invocation actually emits debug info that a store-time debug
/// bundle could carry (kunobi-ninja/kache#319). rustc's default is no debug
/// info, so an absent `-Cdebuginfo` counts as off, as do the explicit "none"
/// spellings; everything else (`1`, `2`, `line-tables-only`, ...) produces
/// DWARF worth bundling. `-g` desugars to `-Cdebuginfo=2` at parse time.
fn rustc_debuginfo_enabled(args: &RustcArgs) -> bool {
    match args.get_codegen_opt("debuginfo") {
        None => false,
        Some("0") => false,
        Some("none") => false,
        Some(_) => true,
    }
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

fn restore_link_strategy(kind: ArtifactKind, executable: bool) -> link::LinkStrategy {
    if executable {
        link::LinkStrategy::Copy
    } else {
        kind.link_strategy()
    }
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
    depinfo_anchor: &Path,
    depinfo_working_dir: &Path,
    depinfo_workspace_dir: Option<&Path>,
    depinfo_configured_roots: &[(PathBuf, String, u8)],
    platform: &dyn crate::compiler::Platform,
    context: &str,
    extra_inputs: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
) -> Result<()> {
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

    let strategy = restore_link_strategy(kind, cached_file.executable);
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

    for action in &plan {
        if !action.is_content_transform() {
            action
                .apply(target_path, platform)
                .with_context(|| format!("{context}: applying {action:?}"))?;
        }
    }

    Ok(())
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

struct ComputedKey {
    cache_key: String,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    key_too_new: bool,
}

fn should_skip_cache_store_for_input_race(
    extra_inputs_racy: bool,
    modified_input_guard: bool,
    key_too_new: bool,
) -> bool {
    extra_inputs_racy || (modified_input_guard && key_too_new)
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
) -> Result<ComputedKey> {
    let key_start = std::time::Instant::now();
    let mut file_hasher = match store {
        Some(store) => store.file_hasher_with_daemon(config.socket_path()),
        None => crate::cache_key::FileHasher::new().with_daemon(config.socket_path()),
    };
    if config.modified_input_guard {
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
    let cache_key = compiler.cache_key(args, &key_ctx)?;
    let key_hash_stats = file_hasher.stats();
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
        key_ms,
        key_hash_stats,
        key_too_new,
    })
}

/// Daemon fast path (kunobi-ninja/kache#565): returns `Some(exit_code)` only
/// when the daemon served a hit AND the restore succeeded. Every other
/// outcome returns `None` and the caller runs the fully local path — which
/// owns eviction/repair for whatever the daemon or restore stumbled on.
#[allow(clippy::too_many_arguments)]
fn try_daemon_local_hit(
    config: &Config,
    compiler: &RustcCompiler,
    args: &RustcArgs,
    cache_key: &str,
    crate_name: &str,
    event_root: &str,
    start: std::time::Instant,
    key_ms: u64,
    key_hash_stats: FileHashStats,
    extra_inputs: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
) -> Option<i32> {
    let lookup_start = std::time::Instant::now();
    let reply = crate::daemon::send_local_lookup(config, cache_key)?;
    let lookup_ms = lookup_start.elapsed().as_millis() as u64;
    if reply.outcome != "hit" {
        return None;
    }
    let meta = reply.meta?;
    if meta.files.is_empty() || meta.cache_key != cache_key {
        return None;
    }

    let restore_start = std::time::Instant::now();
    let blobs = BlobSource::StoreDir(config.store_dir());
    if let Err(e) = restore_from_cache(config, compiler, &blobs, args, &meta, extra_inputs) {
        // Includes a blob evicted between the daemon's pin and our reflink —
        // the local path below recompiles; never serve a partial hit.
        tracing::warn!(
            "daemon local hit restore failed for {}: {} — running local path",
            crate_name,
            e
        );
        return None;
    }
    let restore_ms = restore_start.elapsed().as_millis() as u64;
    let elapsed = start.elapsed().as_millis() as u64;
    let size: u64 = meta.files.iter().map(|f| f.size).sum();
    log_event_with_hash_stats(
        config,
        event_root,
        crate_name,
        EventResult::LocalHit,
        elapsed,
        meta.compile_time_ms,
        size,
        cache_key,
        key_ms,
        key_hash_stats,
        lookup_ms,
        restore_ms,
        0,
    );
    print_progress(crate_name, EventResult::LocalHit, elapsed, size);
    if !meta.stdout.is_empty() {
        print!("{}", meta.stdout);
    }
    if !meta.stderr.is_empty() {
        eprint!("{}", meta.stderr);
    }
    clean_incremental_dir(config, args);
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

fn restore_from_cache(
    config: &Config,
    compiler: &RustcCompiler,
    blobs: &BlobSource<'_>,
    args: &RustcArgs,
    meta: &crate::store::EntryMeta,
    extra_inputs: Option<&crate::extra_inputs::ExtraInputsSnapshot>,
) -> Result<()> {
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
        materialize_cached_artifact(
            blobs,
            cached_file,
            &target_path,
            kind,
            &depinfo_anchor,
            &depinfo_working_dir,
            depinfo_workspace_dir,
            &depinfo_configured_roots,
            &*platform,
            "rustc restore",
            extra_inputs,
        )?;
    }

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PassthroughOutput {
    exit_code: i32,
    fallback: bool,
}

/// Run a configured fallback compiler-wrapper.
///
/// `cmd` is the fully-built `<fallback> <compiler> <args...>` command.
/// Returns `Some(output)` if the fallback ran; returns `None` — so
/// the caller does a plain passthrough — when the fallback binary is
/// not found on `PATH`. A misconfigured fallback must never fail a
/// build, so `NotFound` degrades gracefully; any other spawn error
/// propagates.
fn run_fallback(mut cmd: std::process::Command, name: &str) -> Result<Option<PassthroughOutput>> {
    match cmd.status() {
        Ok(status) => Ok(Some(PassthroughOutput {
            exit_code: status.code().unwrap_or(1),
            fallback: true,
        })),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::warn!(
                "[kache] fallback wrapper `{}` not found on PATH — plain passthrough",
                name
            );
            Ok(None)
        }
        Err(e) => Err(e).with_context(|| format!("executing fallback wrapper `{name}`")),
    }
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

    // Configured fallback wrapper: `<fallback> <rustc> [<inner-rustc>]
    // <args>`. Falls through to a plain passthrough if the fallback
    // binary is not on PATH.
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
        if let Some(output) = run_fallback(cmd, fb)? {
            return Ok(output);
        }
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
        &result.stderr,
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
    );
    Ok(result.exit_code)
}

fn passthrough_with_event<R: Into<String>>(
    config: &Config,
    args: &RustcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
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

fn cc_passthrough_with_event<R: Into<String>>(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
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

/// Log a build event.
fn log_event(
    config: &Config,
    root: &str,
    crate_name: &str,
    result: EventResult,
    elapsed_ms: u64,
    compile_time_ms: u64,
    size: u64,
    cache_key: &str,
    key_ms: u64,
    lookup_ms: u64,
    restore_ms: u64,
    store_ms: u64,
) {
    log_event_with_hash_stats(
        config,
        root,
        crate_name,
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key,
        key_ms,
        FileHashStats::default(),
        lookup_ms,
        restore_ms,
        store_ms,
    );
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
fn store_error_for_event(error: &anyhow::Error) -> String {
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
) {
    // Session attribution (#583 P0.5): read the root-scoped session id and
    // refresh the marker so the 5-minute window measures inactivity. Both are
    // best-effort; an empty id just means a legacy or session-less build.
    let session_id = current_session_id(config, root);
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
    let event = BuildEvent {
        ts: Utc::now(),
        crate_name: crate_name.to_string(),
        root: root.to_string(),
        version: crate::VERSION.to_string(),
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key: cache_key.to_string(),
        schema: 15,
        session_id,
        key_ms,
        key_hash_hits: key_hash_stats.cache_hits,
        key_hash_misses: key_hash_stats.cache_misses,
        key_hash_bytes: key_hash_stats.bytes_hashed,
        lookup_ms,
        restore_ms,
        store_ms,
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
        passthrough_reason,
        store_error,
        lookup_rejection,
        fallback,
        exit_code,
        key_fields,
        key_diff,
        key_externs,
        key_externs_recorded,
        unit_id,
        extern_units,
    };
    let _ = events::log_event(&config.event_log_path(), &event);
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
        eprintln!(
            "[kache] miss: crate {crate_name} (key unchanged since last hit —              entry evicted or store cleared?)"
        );
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
    eprintln!(
        "[kache] miss: crate {} (last hit {}m ago; key changed in: {})",
        crate_name,
        ago,
        changed.join(", ")
    );
    changed
}

/// Check for a new build session and trigger a prefetch hint to the daemon.
/// Uses a marker file with flock to ensure only one wrapper process per
/// build session sends the hint — without this, N parallel rustc invocations
/// would all race past the check and send duplicate prefetch requests.
fn maybe_trigger_prefetch(config: &Config, args: &RustcArgs) {
    if config.remote.is_none() {
        return;
    }

    // Root-scoped marker (#583 P0.5): parallel repos sharing a cache dir get
    // independent sessions instead of suppressing each other's plans. Falls
    // back to the legacy cache-global path when no workspace root is known.
    let root = args
        .workspace_root()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    let marker = if root.is_empty() {
        config.cache_dir.join(".build-session")
    } else {
        session_marker_path(config, &root)
    };
    // 5 minutes: long enough to span gaps between sequential cargo commands
    // in CI (check → clippy → test → tarpaulin are ~2 min apart), short
    // enough that a new `cargo test` after an edit still triggers a fresh
    // prefetch.  The BFS prefetch sends ALL crates, so re-triggering within
    // the same session provides no benefit. Event logging refreshes the
    // marker, so this measures INACTIVITY, not build age.
    let session_timeout_secs: u64 = BUILD_SESSION_SECS;

    // Fast non-blocking check: if the marker contains a fresh timestamp, skip.
    // We store a Unix epoch inside the file instead of relying on filesystem
    // mtime, which can be unreliable on overlayfs (Docker) and network mounts.
    if marker_is_fresh(&marker, session_timeout_secs) {
        return; // Still in the same build session
    }

    // Marker is stale or missing — try to acquire an exclusive lock so only
    // one process does the (expensive) cargo-metadata + daemon RPC.
    // Create the marker's parent (`.build-sessions/` for root-scoped markers,
    // the cache dir itself for the legacy path) — without this a fresh cache
    // dir would fail the marker open and never establish a session
    // (cross-family review finding).
    if let Some(parent) = marker.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Some(lock_file) = open_marker_for_lock(&marker) else {
        return;
    };
    // std::fs::File::try_lock (1.89+) is cross-platform: flock(2) on Unix,
    // LockFileEx on Windows. Lock auto-releases when `lock_file` is dropped.
    if lock_file.try_lock().is_err() {
        return; // Another wrapper is already sending the prefetch hint
    }

    // Re-check under the lock — another process may have updated the marker
    // between our first check and acquiring the lock.
    if marker_file_is_fresh(&lock_file, session_timeout_secs) {
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

    // Mint the session id here — this wrapper won the marker lock, so it is
    // the one process per build that establishes session identity (#583).
    let session_id = mint_session_id(&root);

    crate::daemon::send_build_started(
        config,
        crate::build_intent::into_build_started_request(
            build_intent,
            crate::daemon::build_epoch(),
            session_id.clone(),
        ),
    );

    // Write the marker AFTER the prefetch send so a failed/hung attempt
    // (e.g. cargo metadata hangs on a git dep) doesn't block retries for the
    // full session timeout. Write through `lock_file` — the handle that owns
    // the exclusive lock — so the record lands even on Windows, where the
    // lock is mandatory and a second handle could not write (kache #348).
    write_session_marker(&lock_file, &session_id);
}

/// Open a marker file safely for locking and updating. Refuses symlinks and
/// non-regular files up front, and opens with `O_NOFOLLOW` on Unix to prevent
/// symlink attacks and arbitrary file truncation in shared temporary directories.
fn open_marker_for_lock(marker: &Path) -> Option<std::fs::File> {
    if let Ok(metadata) = std::fs::symlink_metadata(marker) {
        if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
            return None;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if (metadata.file_attributes() & 0x400) != 0 {
                return None; // Refuse reparse points explicitly
            }
        }
    }

    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT (0x00200000) opens the reparse point itself
        // without following it to the target file.
        options.custom_flags(0x0020_0000);
    }

    let file = options.open(marker).ok()?;

    // Post-open verification: ensure the opened file handle itself is a regular file.
    let meta = file.metadata().ok()?;
    if !meta.file_type().is_file() {
        return None;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if (meta.file_attributes() & 0x400) != 0 {
            return None; // Refuse reparse points explicitly
        }
    }

    Some(file)
}

/// Check if the marker file contains a timestamp within `timeout_secs` of now.
/// Returns `false` if the marker does not exist, contains a stale/corrupt
/// timestamp, or is a symlink/non-regular file.
/// Root-scoped session-marker path: `.build-sessions/<hash(root)>` under the
/// cache dir (kunobi-ninja/kache#583 P0.5).
///
/// Scoping by build root (not one cache-global `.build-session`) stops
/// parallel repositories sharing a cache dir from suppressing each other's
/// prefetch plans. The legacy `.build-session` file is left alone: old
/// wrappers keep using it independently; the worst mixed-fleet outcome is a
/// redundant BuildStarted, which the daemon coalesces.
pub(crate) fn session_marker_path(config: &Config, root: &str) -> std::path::PathBuf {
    let hash = blake3::hash(root.as_bytes()).to_hex();
    config
        .cache_dir
        .join(".build-sessions")
        .join(&hash.as_str()[..16])
}

/// Parse a session marker: `v1 <unix-epoch-secs> <session_id>`, or the legacy
/// bare `<unix-epoch-secs>` (empty session id). Returns `(timestamp, id)`.
fn parse_session_marker(content: &str) -> Option<(u64, String)> {
    let content = content.trim();
    if let Some(rest) = content.strip_prefix("v1 ") {
        let mut parts = rest.splitn(2, ' ');
        let ts: u64 = parts.next()?.parse().ok()?;
        let id = parts.next().unwrap_or("").trim().to_string();
        return Some((ts, id));
    }
    content.parse().ok().map(|ts| (ts, String::new()))
}

/// The current build session id for `root`, or empty when no session marker
/// exists. Best-effort by design — session attribution must never fail a
/// build.
///
/// Deliberately does NOT check freshness: freshness gates the TRIGGER (should
/// a new session start?), not attribution. A single crate compiling longer
/// than the inactivity window (LLVM-sized) must not fragment its session —
/// any newer build would have re-minted the marker under the trigger lock, so
/// whatever id is present is the most recent session for this root
/// (cross-family review finding).
pub(crate) fn current_session_id(config: &Config, root: &str) -> String {
    if root.is_empty() {
        return String::new();
    }
    let marker = session_marker_path(config, root);
    if let Ok(metadata) = std::fs::symlink_metadata(&marker)
        && (metadata.file_type().is_symlink() || !metadata.file_type().is_file())
    {
        return String::new();
    }
    let Ok(content) = std::fs::read_to_string(&marker) else {
        return String::new();
    };
    match parse_session_marker(&content) {
        Some((_, id)) => id,
        None => String::new(),
    }
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

fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The build-session inactivity window (shared by trigger + attribution).
pub(crate) const BUILD_SESSION_SECS: u64 = 300;

/// Is `ts` within `timeout_secs` of `now`? Extracted (with an injectable
/// `now`) so freshness is unit-testable without clock races.
fn timestamp_is_fresh_at(ts: u64, timeout_secs: u64, now: u64) -> bool {
    now.saturating_sub(ts) < timeout_secs
}

fn marker_is_fresh(marker: &std::path::Path, timeout_secs: u64) -> bool {
    if let Ok(metadata) = std::fs::symlink_metadata(marker)
        && (metadata.file_type().is_symlink() || !metadata.file_type().is_file())
    {
        return false;
    }
    let content = match std::fs::read_to_string(marker) {
        Ok(c) if !c.is_empty() => c,
        _ => return false,
    };
    timestamp_is_fresh(&content, timeout_secs)
}

/// Marker freshness read through an ALREADY-OPEN handle. A caller holding the
/// exclusive lock must use this rather than [`marker_is_fresh`]: on Windows the
/// lock is mandatory and blocks reads from any other handle (#348).
fn marker_file_is_fresh(mut file: &std::fs::File, timeout_secs: u64) -> bool {
    use std::io::{Read, Seek, SeekFrom};

    let mut content = String::new();
    if file.seek(SeekFrom::Start(0)).is_err() || file.read_to_string(&mut content).is_err() {
        return false;
    }
    timestamp_is_fresh(&content, timeout_secs)
}

/// Is a marker's timestamp within `timeout_secs` of now? Accepts both the
/// legacy bare-epoch format and the v1 session record (`v1 <ts> <id>`), so
/// freshness checks work across marker generations.
fn timestamp_is_fresh(content: &str, timeout_secs: u64) -> bool {
    match parse_session_marker(content) {
        Some((ts, _)) => timestamp_is_fresh_at(ts, timeout_secs, now_epoch_secs()),
        None => false, // legacy "1" marker or corrupt — treat as stale
    }
}

/// Write the current Unix epoch to the marker file, reusing the caller's
/// already-locked handle.
///
/// The caller holds an exclusive lock on this file (see `maybe_trigger_prefetch`).
/// On Windows that lock is *mandatory* (`LockFileEx`), so writing through a
/// *separate* handle (e.g. `std::fs::write`) to the locked file fails with a
/// lock violation — the timestamp never lands and every rustc re-detects a new
/// build session, re-firing the prefetch hint. Writing through the same handle
/// that owns the lock is always permitted. (kache #348)
fn write_marker_timestamp(mut file: &std::fs::File) {
    use std::io::{Seek, SeekFrom, Write};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Truncate any previous (longer) timestamp and rewrite from the start.
    let _ = file.set_len(0);
    let _ = file.seek(SeekFrom::Start(0));
    let _ = file.write_all(now.to_string().as_bytes());
    let _ = file.flush();
}

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
    use super::*;
    use crate::cache_key::FileHasher;
    use std::ffi::OsString;
    use std::path::PathBuf;

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

    /// A build spawns hundreds of wrapper processes, so "warn once" has to hold
    /// ACROSS processes, not just within one (#508). The marker is the only
    /// thing carrying that state — a second wrapper hitting a fresh marker must
    /// stay quiet, and a stale marker must let the advisory through again.
    #[test]
    fn warn_once_per_session_dedups_across_processes_via_the_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("cow-warn");

        assert!(
            warn_once_per_session(&marker, 300, "advisory"),
            "first wrapper in the session must warn"
        );
        assert!(
            !warn_once_per_session(&marker, 300, "advisory"),
            "a later wrapper in the same session must stay quiet"
        );

        // A session window of 0 makes any marker stale — a fresh `cargo` command
        // after a gap warns again rather than staying silent forever.
        assert!(
            warn_once_per_session(&marker, 0, "advisory"),
            "a stale marker must let the advisory through again"
        );
    }

    #[test]
    fn warn_once_per_session_unlocks_even_with_a_duplicated_descriptor() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("cow-warn");
        let lock_file = open_marker_for_lock(&marker).unwrap();
        lock_file.try_lock().unwrap();
        let inherited = lock_file.try_clone().unwrap();

        assert!(finish_warn_once_per_session(&lock_file, 0, "advisory"));
        drop(lock_file);

        let contender = open_marker_for_lock(&marker).unwrap();
        assert!(
            contender.try_lock().is_ok(),
            "the explicit unlock must release the lock even while a duplicated \
             descriptor remains open"
        );
        let _ = contender.unlock();
        drop(inherited);
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
    fn warn_once_per_session_refuses_symlink() {
        let temp = tempfile::TempDir::new().unwrap();
        let target = temp.path().join("target_file");
        std::fs::write(&target, "some sensitive content").unwrap();

        let marker = temp.path().join("marker_symlink");
        std::os::unix::fs::symlink(&target, &marker).unwrap();

        // If we call warn_once_per_session, it should print the message,
        // but it MUST NOT truncate or modify the target file!
        let warned = warn_once_per_session(&marker, 300, "warning message");
        assert!(warned);

        // Verify target file is untouched.
        let content = std::fs::read_to_string(&target).unwrap();
        assert_eq!(content, "some sensitive content");
    }

    #[test]
    #[cfg(unix)]
    fn open_marker_for_lock_refuses_symlink_target() {
        let temp = tempfile::TempDir::new().unwrap();
        let target = temp.path().join("target_file");
        std::fs::write(&target, "sensitive target content").unwrap();

        let marker = temp.path().join("marker_symlink");
        std::os::unix::fs::symlink(&target, &marker).unwrap();

        // open_marker_for_lock must refuse symlink targets up front
        assert!(open_marker_for_lock(&marker).is_none());

        // Verify target file remains completely untouched
        let content = std::fs::read_to_string(&target).unwrap();
        assert_eq!(content, "sensitive target content");
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

    fn test_config(cache_dir: PathBuf) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            local_hit_daemon: false,
            windows_hardlink: false,
            auto_gc: true,
            storage_layout_advice: true,
            heartbeat_secs: 30,
            explain_miss: false,
            path_only_env_vars: Vec::new(),
            incremental_crates: Vec::new(),
            key_env_vars: Vec::new(),
            base_dirs: Vec::new(),
            cache_dir,
            max_size: 1024 * 1024,
            remote: None,
            remote_error: None,
            socket_path_override: None,
            disabled: false,
            cache_executables: false,
            clean_incremental: true,
            preserve_incremental: false,
            adaptive_incremental: true,
            event_log_max_size: 10 * 1024 * 1024,
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
            daemon_idle_timeout_secs: crate::config::DEFAULT_DAEMON_IDLE_TIMEOUT_SECS,
            s3_pool_idle_secs: crate::config::DEFAULT_S3_POOL_IDLE_SECS,
            remote_restore_timeout_secs: crate::config::DEFAULT_REMOTE_RESTORE_TIMEOUT_SECS,
            remote_negative_ttl_secs: crate::config::DEFAULT_REMOTE_NEGATIVE_TTL_SECS,
        }
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

    /// Regression for kache #348: the build-session marker must record a fresh
    /// timestamp even though `maybe_trigger_prefetch` writes it *while still
    /// holding the exclusive lock* on the same file.
    ///
    /// On Windows `File::try_lock` is a *mandatory* `LockFileEx` lock, so a
    /// write through a *second* handle (`std::fs::write`) to the locked file
    /// fails with a lock violation — the timestamp never lands, the marker
    /// stays empty, and every subsequent rustc re-detects a "new build
    /// session" and re-fires the prefetch hint (the 1147-crate spam in the
    /// bug report). On Unix `flock(2)` is advisory and the second write
    /// succeeds, which is why this only reproduces on Windows and was never
    /// caught by the Linux/macOS `cargo test` jobs.
    #[test]
    fn build_session_marker_persists_while_lock_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");

        // Mirror maybe_trigger_prefetch exactly: open the marker, take the
        // exclusive lock, then persist the freshness timestamp while the lock
        // is still held.
        let lock_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&marker)
            .unwrap();
        assert!(
            lock_file.try_lock().is_ok(),
            "the first lock on a fresh marker must succeed"
        );

        write_marker_timestamp(&lock_file);

        // Release the lock before reading back: on Windows a mandatory lock
        // also blocks cross-handle reads, so `marker_is_fresh` (which opens
        // its own handle) could only observe the write after we unlock.
        let _ = std::fs::File::unlock(&lock_file);
        drop(lock_file);

        assert!(
            marker_is_fresh(&marker, 300),
            "marker must record a fresh timestamp even though the writer held \
             the exclusive lock; otherwise every rustc re-fires the prefetch hint"
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

    #[test]
    fn run_fallback_missing_binary_degrades_to_none() {
        // A configured-but-absent fallback wrapper must never fail a
        // build — `NotFound` degrades to `None` so the caller does a
        // plain passthrough.
        let name = "kache-no-such-fallback-binary-zzz";
        let cmd = std::process::Command::new(name);
        assert!(matches!(run_fallback(cmd, name), Ok(None)));
    }

    #[cfg(unix)]
    #[test]
    fn run_fallback_runs_an_existing_command() {
        // `true` exists on every unix and exits 0 — the fallback ran,
        // so its exit code is returned.
        let cmd = std::process::Command::new("true");
        assert!(matches!(
            run_fallback(cmd, "true"),
            Ok(Some(PassthroughOutput {
                exit_code: 0,
                fallback: true
            }))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn stripped_fallback_receives_incremental_disabled_env() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fallback = dir.path().join("fallback");
        let env_dump = dir.path().join("incremental-env.txt");
        std::fs::write(
            &fallback,
            format!(
                "#!/bin/sh\nprintf '%s' \"${{CARGO_INCREMENTAL-unset}}\" > '{}'\nexit 0\n",
                env_dump.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fallback, std::fs::Permissions::from_mode(0o755)).unwrap();

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
        use std::os::unix::fs::PermissionsExt;

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
        std::fs::write(
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
        )
        .unwrap();
        std::fs::set_permissions(&rustc, std::fs::Permissions::from_mode(0o755)).unwrap();

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
    fn heartbeat_stderr_requires_verbose_progress() {
        assert!(!heartbeat_stderr_enabled(0));
        assert!(!heartbeat_stderr_enabled(1));
        assert!(heartbeat_stderr_enabled(2));
    }

    /// `KACHE_PROGRESS` parsing is the only env-dependent part of progress
    /// output; the scoped guard keeps the process-global var restored.
    #[test]
    fn progress_level_parses_supported_env_values() {
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
        std::fs::write(
            &fake_cc,
            format!(
                "#!{}\ncapture=\"$1\"\nshift\nprintf '%s\\n' \"$@\" > \"$capture\"\n",
                shell.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_cc, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(
            &fallback,
            format!("#!{}\nprintf 'fallback\\n' > \"$2\"\n", shell.display()),
        )
        .unwrap();
        std::fs::set_permissions(&fallback, std::fs::Permissions::from_mode(0o755)).unwrap();
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

    #[cfg(unix)]
    #[test]
    fn cc_direct_passthrough_bypasses_configured_fallback() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let fake_cc = dir.path().join("cc");
        let fallback = dir.path().join("fallback");
        let compiler_marker = dir.path().join("compiler-ran");
        let fallback_marker = dir.path().join("fallback-ran");
        let output = dir.path().join("output.o");
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");

        std::fs::write(
            &fake_cc,
            format!(
                "#!{}\nprintf direct > '{}'\n",
                shell.display(),
                compiler_marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_cc, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(
            &fallback,
            format!(
                "#!{}\nprintf fallback > '{}'\n",
                shell.display(),
                fallback_marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fallback, std::fs::Permissions::from_mode(0o755)).unwrap();

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
        std::fs::write(
            &fake_cc,
            format!(
                "#!{}\nprintf ran > '{}'\n",
                shell.display(),
                marker.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake_cc, std::fs::Permissions::from_mode(0o755)).unwrap();

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

    /// Store stats and hash stats should be carried into the event JSONL entry
    /// because reports rely on these schema-9 fields.
    #[test]
    fn log_event_with_store_stats_persists_timing_hash_and_store_fields() {
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
        assert_eq!(event.schema, 15);
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
    }

    #[test]
    fn log_event_persists_same_key_lookup_rejection() {
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
        assert_eq!(event.schema, 15);
        assert_eq!(
            event.lookup_rejection,
            "matching entry lacks dep-info required by this invocation"
        );
        assert!(event.store_error.is_empty());
    }

    /// Passthrough events intentionally omit cache timings but preserve the
    /// structured reason, fallback marker, and compiler exit code.
    #[test]
    fn log_passthrough_event_persists_reason_fallback_and_exit_code() {
        let dir = tempfile::tempdir().unwrap();
        let config = test_config(dir.path().join("cache"));
        let output = PassthroughOutput {
            exit_code: 42,
            fallback: true,
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
    fn marker_is_fresh_reads_timestamp_and_window() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // A just-written stamp is fresh within the window.
        std::fs::write(&marker, now.to_string()).unwrap();
        assert!(marker_is_fresh(&marker, 60));

        // An old stamp is stale.
        std::fs::write(&marker, (now - 120).to_string()).unwrap();
        assert!(!marker_is_fresh(&marker, 60));

        // Empty, missing, and legacy/non-numeric markers are treated as stale.
        std::fs::write(&marker, "").unwrap();
        assert!(!marker_is_fresh(&marker, 60));
        std::fs::write(&marker, "1-legacy").unwrap();
        assert!(!marker_is_fresh(&marker, 60));
        assert!(!marker_is_fresh(&dir.path().join("nope"), 60));
    }

    /// A marker written slightly in the future can happen under clock skew; the
    /// saturating age calculation should treat it as fresh, not stale.
    #[test]
    fn marker_is_fresh_accepts_future_timestamp_from_clock_skew() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        std::fs::write(&marker, (now + 60).to_string()).unwrap();
        assert!(marker_is_fresh(&marker, 300));
    }

    #[test]
    fn session_marker_roundtrip_carries_id_and_freshness() {
        // v1 record: fresh timestamp + id parse back out.
        let now = now_epoch_secs();
        let content = format!("v1 {now} abcd1234efgh5678");
        let (ts, id) = parse_session_marker(&content).expect("v1 record parses");
        assert_eq!(ts, now);
        assert_eq!(id, "abcd1234efgh5678");
        assert!(timestamp_is_fresh(&content, BUILD_SESSION_SECS));
    }

    #[test]
    fn session_marker_accepts_legacy_bare_timestamp() {
        // Old wrappers wrote a bare epoch; it parses with an empty id, so
        // freshness checks work across marker generations (mixed fleets).
        let now = now_epoch_secs();
        let (ts, id) = parse_session_marker(&now.to_string()).expect("legacy parses");
        assert_eq!(ts, now);
        assert!(id.is_empty());
        // Corrupt / non-numeric stays stale.
        assert!(parse_session_marker("garbage").is_none());
        assert!(parse_session_marker("v1 notanumber id").is_none());
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
    fn current_session_id_reads_marker_regardless_of_age() {
        let dir = tempfile::TempDir::new().unwrap();
        let config = test_config(dir.path().to_path_buf());
        let root = "/some/workspace";
        let marker = session_marker_path(&config, root);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();

        // Fresh marker → id comes back.
        std::fs::write(&marker, format!("v1 {} sess42", now_epoch_secs())).unwrap();
        assert_eq!(current_session_id(&config, root), "sess42");

        // STALE marker still yields the id: freshness gates the trigger, not
        // attribution — a >5-minute crate compile must not fragment its
        // session (any newer build would have re-minted the marker).
        std::fs::write(&marker, "v1 1000 sess42").unwrap();
        assert_eq!(current_session_id(&config, root), "sess42");

        // Corrupt marker → empty.
        std::fs::write(&marker, "garbage").unwrap();
        assert_eq!(current_session_id(&config, root), "");

        // Empty root → always empty, never panics.
        assert_eq!(current_session_id(&config, ""), "");
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

    #[test]
    fn write_marker_timestamp_roundtrips_to_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(".build-session");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&marker)
            .unwrap();
        write_marker_timestamp(&file);
        drop(file);
        // The stamp it wrote must read back as fresh and be a parseable epoch.
        let content = std::fs::read_to_string(&marker).unwrap();
        assert!(content.trim().parse::<u64>().is_ok(), "got {content:?}");
        assert!(marker_is_fresh(&marker, 60));
    }

    /// With no remote configured, prefetch detection is a no-op and should not
    /// even create the build-session marker directory.
    #[test]
    fn maybe_trigger_prefetch_returns_immediately_without_remote() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let config = test_config(cache_dir.clone());
        let args = rustc_args(&["rustc", "src/lib.rs", "--crate-name", "foo"]);

        maybe_trigger_prefetch(&config, &args);

        assert!(!cache_dir.join(".build-session").exists());
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
        // No other unit test reads this var, so a scoped set/restore is safe.
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
}
