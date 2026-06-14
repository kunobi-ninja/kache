use anyhow::{Context, Result};
use bytesize::ByteSize;
use chrono::Utc;
use std::path::{Component, Path};

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

/// Print a concise progress line to stderr.
fn print_progress(crate_name: &str, result: EventResult, elapsed_ms: u64, size: u64) {
    let level = progress_level();
    if level == 0 {
        return;
    }

    let label = match result {
        EventResult::LocalHit => "local hit",
        EventResult::PrefetchHit => "prefetch hit",
        EventResult::RemoteHit => "remote hit",
        EventResult::Dup if level < 2 => return,
        EventResult::Dup => "dup",
        EventResult::Miss if level < 2 => return,
        EventResult::Miss => "miss",
        EventResult::Error => "error",
        EventResult::Passthrough => return,
        EventResult::Skipped => return,
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
    let compiler = CcCompiler::with_extra_allowlist_flags(config.cc_extra_allowlist_flags.clone());
    let parsed = compiler
        .parse(wrapper_args)
        .context("parsing cc-family arguments")?;

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
            start,
            format!("refused: {}", reasons.join("; ")),
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
            start,
            format!("source excluded: {}", source.display()),
        );
    }

    let store = match Store::open(config) {
        Ok(store) => store,
        Err(e) => {
            tracing::warn!("failed to open store for cc: {}", e);
            return cc_passthrough_with_event(
                config,
                &parsed,
                &crate_name,
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
                start,
                format!("cache key failed: {e}"),
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
    let store = if args.is_primary || (config.clean_incremental && args.incremental.is_some()) {
        match Store::open(config) {
            Ok(store) => Some(store),
            Err(e) => {
                tracing::warn!("failed to open store: {}", e);
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
            start,
            format!("refused: {}", reasons.join("; ")),
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
            start,
            "user-facing executable (cache_executables=false)",
        );
    }

    let store = match store {
        Some(store) => store,
        None => {
            return passthrough_with_event(config, &args, crate_name, start, "store unavailable");
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
    let path_normalizer =
        crate::path_normalizer::PathNormalizer::from_env(workspace_root.as_deref())
            .with_path_only_env_vars(config.path_only_env_vars.clone());
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
                start,
                format!("cache key failed: {e}"),
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
                    start,
                    format!("restore failed: {e}"),
                );
            }
            let restore_ms = restore_start.elapsed().as_millis() as u64;
            let elapsed = start.elapsed().as_millis() as u64;
            let size: u64 = meta.files.iter().map(|f| f.size).sum();
            log_event_with_hash_stats(
                config,
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
                            start,
                            format!("restore failed: {e}"),
                        );
                    }
                    let restore_ms = restore_start.elapsed().as_millis() as u64;
                    let elapsed = start.elapsed().as_millis() as u64;
                    let size: u64 = meta.files.iter().map(|f| f.size).sum();
                    log_event_with_hash_stats(
                        config,
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
                            start,
                            format!("restore failed: {e}"),
                        );
                    }
                    let restore_ms = restore_start.elapsed().as_millis() as u64;
                    let elapsed = start.elapsed().as_millis() as u64;
                    let size: u64 = meta.files.iter().map(|f| f.size).sum();
                    log_event_with_hash_stats(
                        config,
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
        anyhow::bail!(
            "{context}: blob missing for {} (hash {}): {}",
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
fn restore_from_cache(
    _config: &Config,
    compiler: &RustcCompiler,
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
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    let output = passthrough(args, config.fallback.as_deref())?;
    log_passthrough_event(
        config,
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
    start: std::time::Instant,
    reason: R,
) -> Result<i32> {
    let output = cc_passthrough(parsed, config.fallback.as_deref())?;
    log_passthrough_event(
        config,
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
    crate_name: &str,
    elapsed_ms: u64,
    reason: String,
    output: &PassthroughOutput,
) {
    log_event_details(
        config,
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
        version: crate::VERSION.to_string(),
        result,
        elapsed_ms,
        compile_time_ms,
        size,
        cache_key: cache_key.to_string(),
        schema: 8,
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
    // full session timeout.
    write_marker_timestamp(&marker);
}

/// Check if the marker file contains a timestamp within `timeout_secs` of now.
fn marker_is_fresh(marker: &std::path::Path, timeout_secs: u64) -> bool {
    let content = match std::fs::read_to_string(marker) {
        Ok(c) if !c.is_empty() => c,
        _ => return false,
    };
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

/// Write the current Unix epoch to the marker file.
fn write_marker_timestamp(marker: &std::path::Path) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let _ = std::fs::write(marker, now.to_string());
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
    use std::path::PathBuf;

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
}
