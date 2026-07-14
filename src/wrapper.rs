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
use crate::link;
use crate::store::{Store, StorePutResult};

/// Check whether progress lines should be printed to stderr.
///
/// Controlled by `KACHE_PROGRESS` env var (off by default):
/// - `1` / `hits`    — print hits only
/// - `verbose` / `all` — print hits, dups, and misses
/// - anything else / unset — silent
fn progress_level() -> u8 {
    match std::env::var("KACHE_PROGRESS").as_deref() {
        Ok("1" | "hits") => 1,
        Ok("verbose" | "all") => 2,
        _ => 0,
    }
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
    if marker_is_fresh(marker, session_secs) {
        return false; // already warned this session
    }
    let Ok(lock_file) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(marker)
    else {
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
    if marker_file_is_fresh(&lock_file, session_secs) {
        return false;
    }
    eprintln!("{message}");
    write_marker_timestamp(&lock_file);
    true
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

fn event_result_for_store_put(put: StorePutResult) -> EventResult {
    if put.is_full_dup() {
        EventResult::Dup
    } else {
        EventResult::Miss
    }
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
    crate::link::set_cow_warn_marker(warn_marker_path("cow", &config.cache_dir));
    let compiler = CcCompiler::with_extra_allowlist_flags(config.cc_extra_allowlist_flags.clone());
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
        return cc_passthrough_with_event(
            config,
            &parsed,
            &crate_name,
            &event_root,
            start,
            refuse_reason_string(&refuse),
        );
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
    if let Some(meta) = lookup {
        if meta.files.is_empty() {
            // Poisoned entry (earlier bug) — evict and recompile.
            tracing::warn!("cc cache entry for {} has no files, evicting", crate_name);
            let _ = store.remove_entry(&cache_key);
        } else if !cc_cache_entry_satisfies_invocation(&parsed, &meta) {
            tracing::warn!(
                "cc cache entry for {} lacks artifacts required by this invocation, evicting",
                crate_name
            );
            let _ = store.remove_entry(&cache_key);
        } else {
            let restore_start = std::time::Instant::now();
            if let Err(e) = restore_cc_from_cache(&store, &parsed, &meta) {
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
    if result.exit_code == 0 && !result.artifacts.is_empty() {
        let depinfo_anchor = cc_depinfo_rewrite_root(&parsed);
        if let Some(anchor) = depinfo_anchor.as_deref() {
            rewrite_depinfo_outputs(&result.artifacts, anchor, link::DepInfoMode::Relativize);
        }

        let target = parsed.cache_target_arch();
        let store_files = result.artifacts.store_files();
        match store.put_with_compile_time(
            &cache_key,
            &crate_name,
            &[], // crate_types: n/a for cc objects
            &[], // features: n/a
            &target,
            "", // profile: n/a (opt level is in the key)
            &store_files,
            &result.stdout,
            &result.stderr,
            compile_time_ms,
        ) {
            Ok(result) => store_put = result,
            Err(e) => tracing::warn!("failed to store cc cache entry for {}: {}", crate_name, e),
        }

        if let Some(anchor) = depinfo_anchor.as_deref() {
            rewrite_depinfo_outputs(&result.artifacts, anchor, link::DepInfoMode::Expand);
        }
    }
    let store_ms = store_start.elapsed().as_millis() as u64;

    let elapsed = start.elapsed().as_millis() as u64;
    let size = result.artifacts.total_size();
    let event_result = event_result_for_store_put(store_put);
    log_event_with_store_stats(
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
    parsed: &crate::compiler::cc::CcArgs,
    fallback: Option<&str>,
) -> Result<PassthroughOutput> {
    // Configured fallback wrapper: `<fallback> <cc> <args>`.
    // kache's C/C++ coverage is narrower than its rustc support, so
    // the fallback is most valuable on this path. Falls through to a
    // plain passthrough if the fallback is not on PATH.
    if let Some(fb) = fallback {
        let mut cmd = std::process::Command::new(fb);
        cmd.arg(&parsed.program);
        cmd.args(&parsed.rest);
        if let Some(output) = run_fallback(cmd, fb)? {
            return Ok(output);
        }
    }

    let compiler = CcCompiler::new();
    let result = compiler.execute(parsed)?;
    if !result.stdout.is_empty() {
        print!("{}", result.stdout);
    }
    if !result.stderr.is_empty() {
        eprint!("{}", result.stderr);
    }
    Ok(PassthroughOutput {
        exit_code: result.exit_code,
        fallback: false,
    })
}

fn cc_cache_entry_satisfies_invocation(
    parsed: &crate::compiler::cc::CcArgs,
    meta: &crate::store::EntryMeta,
) -> bool {
    let has_object = meta
        .files
        .iter()
        .any(|file| classify_by_filename(&file.name) == ArtifactKind::Object);
    let has_depinfo = meta
        .files
        .iter()
        .any(|file| classify_by_filename(&file.name) == ArtifactKind::DepInfo);

    has_object && (parsed.depinfo_output_path().is_none() || has_depinfo)
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

/// Restore cached cc artifacts to this invocation's output paths.
///
/// Object files go to the current `-o` target. Dep-info sidecars go to the
/// current `-MF` target, or the compiler's default `.d` next to the object.
fn restore_cc_from_cache(
    store: &Store,
    parsed: &crate::compiler::cc::CcArgs,
    meta: &crate::store::EntryMeta,
) -> Result<()> {
    let depinfo_anchor =
        cc_depinfo_rewrite_root(parsed).unwrap_or_else(|| Path::new(".").to_path_buf());
    let platform = platform::current();

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

        materialize_cached_artifact(
            store,
            cached,
            &target,
            kind,
            &depinfo_anchor,
            &*platform,
            "cc restore",
        )?;
    }
    Ok(())
}

/// Run kache in RUSTC_WRAPPER mode.
///
/// This is the hot path — called once per crate by cargo.
/// Flow: parse args → compute cache key → check store → link on hit → compile on miss → store → link
pub fn run(config: &Config, wrapper_args: &[String]) -> Result<i32> {
    let start = std::time::Instant::now();
    crate::link::set_windows_hardlink_restore(config.windows_hardlink);
    crate::link::set_cow_warn_marker(warn_marker_path("cow", &config.cache_dir));
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
    let compiler = RustcCompiler::new();
    let args = compiler
        .parse(wrapper_args)
        .context("parsing rustc arguments")?;
    let event_root = rustc_event_root(&args);
    let store = if args.is_primary || (config.clean_incremental && args.incremental.is_some()) {
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

    if config.clean_incremental
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

    let crate_name = args.crate_name.as_deref().unwrap_or("unknown");

    // Bypass the cache when the compiler tells us we can't safely cache this
    // invocation (today: only NotPrimary; future: response files, coverage,
    // time macros, etc.).
    let refuse = compiler.refuse_reasons(&args);
    if !refuse.is_empty() {
        let reasons: Vec<&str> = refuse.iter().map(|r| r.description()).collect();
        tracing::debug!(
            "{}: bypassing cache ({})",
            compiler.id().as_str(),
            reasons.join("; ")
        );
        return passthrough_with_event(
            config,
            &args,
            crate_name,
            &event_root,
            start,
            refuse_reason_string(&refuse),
        );
    }

    let current_dir = std::env::current_dir().ok();
    let workspace_root = args.workspace_root().or_else(|| current_dir.clone());
    let exclude_roots: Vec<_> = workspace_root
        .iter()
        .chain(current_dir.iter())
        .cloned()
        .collect();

    if let Some(source) = &args.source_file
        && Config::source_excluded(source, &exclude_roots)
    {
        tracing::debug!("rustc source excluded from cache: {}", source.display());
        return passthrough_with_event(
            config,
            &args,
            crate_name,
            &event_root,
            start,
            format!("source excluded: {}", source.display()),
        );
    }

    // Skip-cache only for *user-facing* executables (`bin` / `--test`).
    // dylib / cdylib / proc-macro stay cacheable: they're rustc's
    // internal artifacts, not user-shipped binaries, and verify-then-
    // sign on restore (`PostRestoreAction::Sign`) keeps macOS dyld
    // happy. Without this distinction, every proc-macro recompiled
    // fresh per build, producing non-byte-identical `.dylib` output
    // that broke downstream cache keys via `extern:` hashes.
    if args.is_user_facing_executable() && !config.cache_executables {
        tracing::debug!("skipping cache for user-facing executable: {}", crate_name);
        return passthrough_with_event(
            config,
            &args,
            crate_name,
            &event_root,
            start,
            "user-facing executable (cache_executables=false)",
        );
    }

    let store = match store {
        Some(store) => store,
        None => {
            return passthrough_with_event(
                config,
                &args,
                crate_name,
                &event_root,
                start,
                "store unavailable",
            );
        }
    };

    // Compute the cache key
    let key_start = std::time::Instant::now();
    let mut file_hasher = store.file_hasher_with_daemon(config.socket_path());
    if config.modified_input_guard {
        // Flag keyed inputs touched at/after this invocation started — their
        // content at hash time may differ from what rustc reads, so we'll look
        // up but refuse to store (kunobi-ninja/kache#324).
        file_hasher.arm_too_new_guard(invocation_start_ns, 0);
    }
    // Workspace root for normalization: derive from `--out-dir`
    // (see `RustcArgs::workspace_root` for the rationale — cargo
    // cd's into each transitive dep's source dir, so CWD is the
    // wrong anchor). Falls back to CWD if --out-dir isn't set
    // (defensive — cargo always sets it for cacheable invocations).
    // Re-virtualize rust std sources to `/rustc/<hash>` so profilers resolve
    // them (kunobi-ninja/kache#485). MUST match the injection-side normalizer in
    // `RustcCompiler::execute`, or the key would represent one remap rule set
    // and the binary another.
    let path_normalizer =
        crate::path_normalizer::PathNormalizer::from_env(workspace_root.as_deref())
            .with_path_only_env_vars(config.path_only_env_vars.clone())
            .with_rust_src_rule(
                crate::cache_key::get_rustc_sysroot(&args).as_deref(),
                crate::cache_key::get_rustc_commit_hash(&args.rustc).as_deref(),
            );
    let key_ctx = KeyCtx {
        file_hasher: &file_hasher,
        path_normalizer: &path_normalizer,
        cache_dir: &config.cache_dir,
        key_salt: config.key_salt.as_deref(),
    };
    let cache_key = match compiler.cache_key(&args, &key_ctx) {
        Ok(key) => key,
        Err(e) => {
            tracing::warn!("failed to compute cache key for {}: {}", crate_name, e);
            return passthrough_with_event(
                config,
                &args,
                crate_name,
                &event_root,
                start,
                format!("uncacheable|{e}"),
            );
        }
    };
    let key_hash_stats = file_hasher.stats();
    let key_ms = key_start.elapsed().as_millis() as u64;

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
                &args,
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
            if let Err(e) = restore_from_cache(config, &compiler, &store, &args, &meta) {
                tracing::warn!(
                    "restoring local cache hit for {} failed: {} — recompiling",
                    crate_name,
                    e
                );
                return passthrough_with_event(
                    config,
                    &args,
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
            clean_incremental_dir(config, &args);
            return Ok(0);
        }
    }

    // Build-session detection: send prefetch hint before remote work.
    // Placed after local-hit check so warm-cache invocations skip this entirely.
    maybe_trigger_prefetch(config, &args);

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
                    if let Err(e) = restore_from_cache(config, &compiler, &store, &args, &meta) {
                        tracing::warn!(
                            "restoring cache hit for {} failed: {} — recompiling",
                            crate_name,
                            e
                        );
                        return passthrough_with_event(
                            config,
                            &args,
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
                    clean_incremental_dir(config, &args);
                    return Ok(0);
                }
            }
            Some(_) => {} // not in remote, continue to compile
            None => {}    // daemon unreachable, continue to compile
        }
    }

    // 3. Cache miss — try to acquire build lock
    let lock = match store.try_lock(&cache_key) {
        Ok(Some(lock)) => lock,
        Err(e) => {
            tracing::warn!(
                "acquiring build lock for {} failed: {} — recompiling",
                crate_name,
                e
            );
            return passthrough_with_event(
                config,
                &args,
                crate_name,
                &event_root,
                start,
                format!("build lock unavailable: {e}"),
            );
        }
        Ok(None) => {
            // Another process is building this key — wait for it
            tracing::debug!("waiting for {} to be built by another process", crate_name);
            if store.wait_for_committed(&cache_key).unwrap_or(false) {
                // It's now available
                if let Ok(Some(meta)) = store.get(&cache_key) {
                    let restore_start = std::time::Instant::now();
                    if let Err(e) = restore_from_cache(config, &compiler, &store, &args, &meta) {
                        tracing::warn!(
                            "restoring cache hit for {} failed: {} — recompiling",
                            crate_name,
                            e
                        );
                        return passthrough_with_event(
                            config,
                            &args,
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
                    // Replay the original compiler diagnostics, exactly as
                    // the other hit sites do — otherwise the process that
                    // loses the build-lock race silently swallows the
                    // cached warnings/notes.
                    if !meta.stdout.is_empty() {
                        print!("{}", meta.stdout);
                    }
                    if !meta.stderr.is_empty() {
                        eprint!("{}", meta.stderr);
                    }
                    clean_incremental_dir(config, &args);
                    return Ok(0);
                }
            }
            // If waiting failed, fall through to compile
            tracing::warn!("wait for {} failed, compiling ourselves", crate_name);
            // Compile without caching
            return passthrough_with_event(
                config,
                &args,
                crate_name,
                &event_root,
                start,
                "build lock wait failed",
            );
        }
    };

    // 4. Compile
    tracing::debug!(
        "cache miss for {}, compiling ({})",
        crate_name,
        &cache_key[..16]
    );
    let compile_start = std::time::Instant::now();
    let result = match compiler.execute(&args) {
        Ok(r) => r,
        // A spawn-level failure (missing binary, ENOMEM, fork pressure under
        // load) must not abort the build: fall back to passthrough so the
        // configured fallback wrapper still gets a chance and the user sees the
        // real compiler error rather than a kache anyhow chain.
        Err(e) => {
            return passthrough_with_event(
                config,
                &args,
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
    if config.modified_input_guard && file_hasher.too_new() {
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
    if let Some(missing) = missing_requested_emit(&args, &result.artifacts) {
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

    // 5. Store the output files
    let target = args.target.as_deref().unwrap_or("host");
    let profile = match args.get_codegen_opt("opt-level") {
        Some("0") | None => "dev",
        Some("s") | Some("z") => "release-size",
        _ => "release",
    };

    // Relativize dep-info (`.d`) files before they are cached so the
    // stored blob is worktree-independent. cargo records each crate's
    // inputs in its `.d` by *absolute* path; without this, a cached
    // `.d` carries the producing build's paths, and a build that
    // restores it at a different location finds those paths missing on
    // its freshness `stat()` and recompiles — a cache hit that saved
    // nothing. `store.put*` hashes the file at its on-disk source path,
    // so we rewrite in place; the matching expand below restores the
    // absolute paths the *current* build's cargo still needs to see.
    let depinfo_anchor = args.target_dir();
    if let Some(anchor) = depinfo_anchor.as_deref() {
        rewrite_depinfo_outputs(&result.artifacts, anchor, link::DepInfoMode::Relativize);
    }

    let store_start = std::time::Instant::now();
    let store_files = result.artifacts.store_files();
    let mut store_put = StorePutResult::default();
    match store.put_with_compile_time(
        &cache_key,
        crate_name,
        &args.crate_types,
        &args.features,
        target,
        profile,
        &store_files,
        &result.stdout,
        &result.stderr,
        compile_time_ms,
    ) {
        Ok(result) => store_put = result,
        Err(e) => tracing::warn!("failed to store cache entry: {}", e),
    }
    let store_ms = store_start.elapsed().as_millis() as u64;

    // Expand the on-disk `.d` files back to absolute paths. The store
    // already captured the relativized form above; this leaves the
    // current build's `.d` valid for cargo's own freshness checks.
    if let Some(anchor) = depinfo_anchor.as_deref() {
        rewrite_depinfo_outputs(&result.artifacts, anchor, link::DepInfoMode::Expand);
    }

    // 6. Async upload to remote (if configured) — sends job to the daemon
    if config.remote.is_some() {
        let entry_dir = store.entry_dir(&cache_key);
        if let Err(e) = crate::daemon::send_upload_job(config, &cache_key, &entry_dir, crate_name) {
            tracing::warn!("failed to send upload job to daemon: {}", e);
        }
    }

    // 7. Clean incremental dir, as with kache's caching, incremental compilation is redundant
    clean_incremental_dir(config, &args);

    let elapsed = start.elapsed().as_millis() as u64;
    let size = result.artifacts.total_size();
    let event_result = event_result_for_store_put(store_put);
    log_event_with_store_stats(
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
    );
    print_progress(crate_name, event_result, elapsed, size);

    drop(lock);
    Ok(result.exit_code)
}

/// Rewrite every dep-info (`.d`) file in a compiler artifact set, in place,
/// against `anchor`.
///
/// Dep-info files are identified by the adapter-provided artifact kind,
/// so the store and restore sides stay in lock-step on what counts as a
/// `.d`.
///
/// Rewrite failures are logged and skipped, not propagated: a malformed
/// `.d` must not abort an otherwise-successful cache store.
fn rewrite_depinfo_outputs(artifacts: &ArtifactSet, anchor: &Path, mode: link::DepInfoMode) {
    for artifact in artifacts.outputs() {
        if artifact.kind != ArtifactKind::DepInfo {
            continue;
        }
        if let Err(e) = link::rewrite_depinfo(&artifact.path, anchor, mode) {
            tracing::warn!(
                "failed to rewrite dep-info {} ({:?}): {}",
                artifact.path.display(),
                mode,
                e
            );
        }
    }
}

/// Materialize one cached blob at its invocation-specific output path.
///
/// The caller owns target-path resolution because that is compiler-specific
/// (`rustc --out-dir` vs. cc `-o` / `-MF`). Once the target and kind are
/// known, restore mechanics are shared: apply content transforms in memory,
/// materialize the result, touch mtime, then run external post-restore actions.
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
    depinfo_anchor: &Path,
    platform: &dyn crate::compiler::Platform,
    context: &str,
) -> Result<()> {
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

    let transformed = if transforms.is_empty() {
        None
    } else {
        let original = std::fs::read(&store_path)
            .with_context(|| format!("{context}: reading blob {}", store_path.display()))?;
        let mut content = original.clone();
        for action in &transforms {
            content = action.transform(content, depinfo_anchor);
        }
        if content == original {
            None
        } else {
            Some(content)
        }
    };

    match transformed {
        Some(content) => {
            link::write_restored(target_path, &content, kind.link_strategy())
                .with_context(|| format!("{context}: writing {}", target_path.display()))?;
        }
        None => {
            link::link_to_target(&store_path, target_path, kind.link_strategy()).with_context(
                || {
                    format!(
                        "{context}: linking {} -> {}",
                        store_path.display(),
                        target_path.display()
                    )
                },
            )?;
        }
    }

    link::touch_mtime(target_path)
        .with_context(|| format!("{context}: touching {}", target_path.display()))?;

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

fn restore_from_cache(
    _config: &Config,
    compiler: &RustcCompiler,
    store: &Store,
    args: &RustcArgs,
    meta: &crate::store::EntryMeta,
) -> Result<()> {
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

    // Determine where output files go: either -o parent dir, or --out-dir
    let output_dir = if let Some(output) = &args.output {
        output.parent().unwrap_or(Path::new(".")).to_path_buf()
    } else if let Some(dir) = &args.out_dir {
        dir.clone()
    } else {
        anyhow::bail!("no output path (-o) or output directory (--out-dir) in args");
    };

    // Anchor for dep-info (`.d`) path expansion. Cached `.d` blobs hold
    // paths relativized against the *producing* build's target dir
    // (`<target>/...` → kache's dep-info sentinel); on restore they must
    // be re-rooted at *this* invocation's target dir so cargo's freshness
    // `stat()`s find them. `args.target_dir()` derives that from
    // `--out-dir` / `-o` — cargo's cwd is the package source dir, so it
    // cannot be used.
    // Falls back to cwd only for ad-hoc invocations outside cargo's
    // layout, where there is no cached `.d` to rewrite anyway.
    let depinfo_anchor = args
        .target_dir()
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| Path::new(".").to_path_buf());

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
            store,
            cached_file,
            &target_path,
            kind,
            &depinfo_anchor,
            &*platform,
            "rustc restore",
        )?;
    }

    Ok(())
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
/// so the fallback gets a chance. Even on the plain passthrough path,
/// we strip incremental flags to prevent APFS-related corruption in
/// git worktrees on macOS.
fn passthrough(args: &RustcArgs, fallback: Option<&str>) -> Result<PassthroughOutput> {
    let filtered = compile::strip_incremental_flags(&args.all_args);
    let stripped = args.all_args.len() - filtered.len();
    if stripped > 0 {
        tracing::info!(
            "[kache] passthrough: stripped {} incremental flag(s) for {}",
            stripped,
            args.crate_name.as_deref().unwrap_or("unknown")
        );
    }

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
    );

    // Configured fallback wrapper: `<fallback> <rustc> [<inner-rustc>]
    // <args>`. Falls through to a plain passthrough if the fallback
    // binary is not on PATH.
    if let Some(fb) = fallback {
        let mut cmd = std::process::Command::new(fb);
        cmd.env("CARGO_INCREMENTAL", "0");
        cmd.arg(&args.rustc);
        if let Some(inner) = &args.inner_rustc {
            cmd.arg(inner);
        }
        cmd.args(&filtered);
        if let Some(output) = run_fallback(cmd, fb)? {
            return Ok(output);
        }
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
    Ok(PassthroughOutput {
        exit_code: status.code().unwrap_or(1),
        fallback: false,
    })
}

fn passthrough_with_event<R: Into<String>>(
    config: &Config,
    args: &RustcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    let output = passthrough(args, config.fallback.as_deref())?;
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

fn cc_passthrough_with_event<R: Into<String>>(
    config: &Config,
    parsed: &crate::compiler::cc::CcArgs,
    crate_name: &str,
    root: &str,
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    let output = cc_passthrough(parsed, config.fallback.as_deref())?;
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
    fallback: bool,
    exit_code: Option<i32>,
) {
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
        schema: 9,
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
        store_copied_bytes: crate::opcounts::store_copied_bytes(),
        passthrough_reason,
        fallback,
        exit_code,
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

/// Check for a new build session and trigger a prefetch hint to the daemon.
/// Uses a marker file with flock to ensure only one wrapper process per
/// build session sends the hint — without this, N parallel rustc invocations
/// would all race past the check and send duplicate prefetch requests.
fn maybe_trigger_prefetch(config: &Config, args: &RustcArgs) {
    if config.remote.is_none() {
        return;
    }

    let marker = config.cache_dir.join(".build-session");
    // 5 minutes: long enough to span gaps between sequential cargo commands
    // in CI (check → clippy → test → tarpaulin are ~2 min apart), short
    // enough that a new `cargo test` after an edit still triggers a fresh
    // prefetch.  The BFS prefetch sends ALL crates, so re-triggering within
    // the same session provides no benefit.
    let session_timeout_secs: u64 = 300;

    // Fast non-blocking check: if the marker contains a fresh timestamp, skip.
    // We store a Unix epoch inside the file instead of relying on filesystem
    // mtime, which can be unreliable on overlayfs (Docker) and network mounts.
    if marker_is_fresh(&marker, session_timeout_secs) {
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
    // std::fs::File::try_lock (1.89+) is cross-platform: flock(2) on Unix,
    // LockFileEx on Windows. Lock auto-releases when `lock_file` is dropped.
    if lock_file.try_lock().is_err() {
        return; // Another wrapper is already sending the prefetch hint
    }

    // Re-check under the lock — another process may have updated the marker
    // between our first check and acquiring the lock.
    if marker_is_fresh(&marker, session_timeout_secs) {
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
        crate::build_intent::into_build_started_request(build_intent, crate::daemon::build_epoch()),
    );

    // Write current epoch AFTER the prefetch succeeds so a failed/hung attempt
    // (e.g. cargo metadata hangs on a git dep) doesn't block retries for the
    // full session timeout. Write through `lock_file` — the handle that owns
    // the exclusive lock — so the timestamp lands even on Windows, where the
    // lock is mandatory and a second handle could not write (kache #348).
    write_marker_timestamp(&lock_file);
}

/// Check if the marker file contains a timestamp within `timeout_secs` of now.
fn marker_is_fresh(marker: &std::path::Path, timeout_secs: u64) -> bool {
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

/// Is a marker's Unix-epoch timestamp within `timeout_secs` of now?
fn timestamp_is_fresh(content: &str, timeout_secs: u64) -> bool {
    let stamp: u64 = match content.trim().parse() {
        Ok(s) => s,
        Err(_) => return false, // legacy "1" marker or corrupt — treat as stale
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    now.saturating_sub(stamp) < timeout_secs
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
}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn test_config(cache_dir: PathBuf) -> Config {
        Config {
            fallback: None,
            key_salt: None,
            cc_extra_allowlist_flags: Vec::new(),
            local_only: false,
            remote_readonly: false,
            modified_input_guard: false,
            windows_hardlink: false,
            path_only_env_vars: Vec::new(),
            cache_dir,
            max_size: 1024 * 1024,
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
        }
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

    /// `rewrite_depinfo_outputs` rewrites dep-info files in place and the
    /// relativize→expand round trip re-roots the cached blob's paths at
    /// the restoring build's target dir — the property that lets dep-info
    /// produced at one location be restored valid at another.
    #[test]
    fn rewrite_depinfo_outputs_round_trips_depinfo_files_across_target_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let depfile = dir.path().join("foo-abc.d");
        let mozilla_depfile = dir.path().join("host_pathsub.o.pp");
        let producing_target = std::path::Path::new("/build/worktree-a/target");
        std::fs::write(
            &depfile,
            format!(
                "{}/release/deps/libfoo-abc.rlib: src/lib.rs",
                producing_target.display()
            ),
        )
        .unwrap();
        std::fs::write(
            &mozilla_depfile,
            format!(
                "{}/config/host_pathsub.o: {}/config/pathsub.c",
                producing_target.display(),
                producing_target.display()
            ),
        )
        .unwrap();

        let outputs = ArtifactSet::from_output_files(
            vec![
                (depfile.clone(), "foo-abc.d".to_string()),
                (mozilla_depfile.clone(), "host_pathsub.o.pp".to_string()),
            ],
            classify_by_filename,
        );

        // Store side: relativize against the producing build's target dir.
        rewrite_depinfo_outputs(&outputs, producing_target, link::DepInfoMode::Relativize);
        let stored = std::fs::read_to_string(&depfile).unwrap();
        assert!(
            stored.starts_with("__kache_root__/release/deps/libfoo-abc.rlib:"),
            "stored `.d` must be relativized, got: {stored}"
        );
        assert!(
            !stored.contains("/build/worktree-a/"),
            "no producing-worktree path may survive relativization: {stored}"
        );
        let stored_mozilla = std::fs::read_to_string(&mozilla_depfile).unwrap();
        assert!(
            stored_mozilla.starts_with(
                "__kache_root__/config/host_pathsub.o: __kache_root__/config/pathsub.c"
            ),
            "stored `.pp` must be relativized, got: {stored_mozilla}"
        );

        // Restore side: expand against a *different* target dir.
        let restoring_target = std::path::Path::new("/build/worktree-b/target");
        rewrite_depinfo_outputs(&outputs, restoring_target, link::DepInfoMode::Expand);
        let restored = std::fs::read_to_string(&depfile).unwrap();
        assert!(
            restored.starts_with("/build/worktree-b/target/release/deps/libfoo-abc.rlib:"),
            "restored `.d` must be re-rooted at the restoring target dir, got: {restored}"
        );
        // Source paths that were already package-relative are untouched.
        assert!(restored.contains("src/lib.rs"), "got: {restored}");
        let restored_mozilla = std::fs::read_to_string(&mozilla_depfile).unwrap();
        assert!(
            restored_mozilla.starts_with(
                "/build/worktree-b/target/config/host_pathsub.o: /build/worktree-b/target/config/pathsub.c"
            ),
            "restored `.pp` must be re-rooted at the restoring target dir, got: {restored_mozilla}"
        );
    }

    /// Only dep-info files are touched — the helper identifies them via
    /// `ArtifactKind::DepInfo`, so an `.rlib` (or any non-dep-info artifact)
    /// in the same output set is left byte-for-byte intact.
    #[test]
    fn rewrite_depinfo_outputs_ignores_non_dep_info_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let rlib = dir.path().join("libfoo-abc.rlib");
        // Content that *looks* rewritable but must not be: an `.rlib` is
        // not dep-info, so the helper must skip it entirely.
        let original = "/build/worktree-a/target/release/deps/x";
        std::fs::write(&rlib, original).unwrap();

        let outputs = ArtifactSet::from_output_files(
            vec![(rlib.clone(), "libfoo-abc.rlib".to_string())],
            classify_by_filename,
        );
        rewrite_depinfo_outputs(
            &outputs,
            std::path::Path::new("/build/worktree-a/target"),
            link::DepInfoMode::Relativize,
        );

        assert_eq!(
            std::fs::read_to_string(&rlib).unwrap(),
            original,
            "non-`.d` artifacts must be left untouched"
        );
    }

    /// A missing `.d` file is logged and skipped, not propagated — a
    /// malformed output set must never abort an otherwise-good store.
    #[test]
    fn rewrite_depinfo_outputs_is_silent_on_missing_file() {
        let outputs = ArtifactSet::from_output_files(
            vec![(PathBuf::from("/nonexistent/foo.d"), "foo.d".to_string())],
            classify_by_filename,
        );
        // Must not panic.
        rewrite_depinfo_outputs(
            &outputs,
            std::path::Path::new("/some/target"),
            link::DepInfoMode::Relativize,
        );
    }

    #[test]
    fn cc_cache_entry_requires_depinfo_when_invocation_requests_it() {
        fn meta(names: &[&str]) -> crate::store::EntryMeta {
            crate::store::EntryMeta {
                cache_key: "key".to_string(),
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
            &store,
            &cached,
            &target,
            ArtifactKind::Library,
            dir.path(),
            &*platform,
            "test restore",
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
        let stored = "__kache_root__/debug/deps/libfoo.rlib: src/lib.rs\n";
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
            &store,
            &cached,
            &target,
            ArtifactKind::DepInfo,
            &anchor,
            &*platform,
            "test restore",
        )
        .unwrap();

        let restored = std::fs::read_to_string(&target).unwrap();
        assert!(
            restored.starts_with(&format!("{}/debug/deps/libfoo.rlib:", anchor.display())),
            "dep-info should be expanded at restore anchor, got: {restored}"
        );
        assert_eq!(
            std::fs::read_to_string(store.blob_path(hash)).unwrap(),
            stored,
            "content transforms must not mutate the store blob"
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
        assert_eq!(event.schema, 9);
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
        let entry_dir = store.entry_dir(&meta.cache_key);
        std::fs::create_dir_all(&entry_dir).unwrap();
        std::fs::write(
            entry_dir.join("meta.json"),
            serde_json::to_string(&meta).unwrap(),
        )
        .unwrap();

        let err = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta)
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

        let err = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta)
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

        let err = restore_from_cache(&config, &RustcCompiler::new(), &store, &args, &meta)
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
