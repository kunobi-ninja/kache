//! C-family compiler (cc / gcc / g++ / clang / clang++ / c++).
//!
//! **C/C++ caching is live for the single-source `-c` compile.**
//! A `cc -c foo.c -o foo.o` invocation gets a content-addressed
//! cache entry; an identical re-invocation restores the `.o` without
//! running the compiler.
//!
//! What's cached:
//! - **`-c` object compiles**, exactly one source per invocation.
//!   The cache key is the preprocessor expansion (`cc -E -P` with
//!   `SOURCE_DATE_EPOCH` pinned) plus compiler identity, target
//!   arch, and codegen flags. The preprocessor hash captures the
//!   source and every transitively-included header, so any header
//!   change invalidates the key with no separate dependency
//!   tracking. `-E -P` strips line markers so header *paths* don't
//!   leak — the key is portable across machines and worktrees.
//!
//! What passes through (refused, see [`CcArgs::refuse_reasons`]):
//! - Link mode (whole-program caching is a separate, harder problem)
//! - Preprocess (`-E`) / assemble (`-S`) modes
//! - Multi-source compiles, multi-arch fat binaries
//! - Response files, coverage instrumentation, split DWARF,
//!   precompiled headers, modules, output-to-stdout
//! - Any flag not classified by [`CC_FLAGS`] (see [`classify_cc_flag`])
//!   — an unmodeled codegen flag, a cross-target, profiling, or simply
//!   a flag kache has not classified. Refused so an unknown flag is
//!   never silently cached. The table is declarative; see
//!   [`crate::compiler::flags`] for the matcher / classification
//!   vocabulary it uses.
//!
//! Future work (separate PRs):
//! - Link-mode / whole-executable caching
//! - `ar` archive caching
//! - Cross-machine cache sharing for C/C++ artifacts: SDKROOT
//!   sentinel + Mach-O OSO record stripping (issue #78)
//! - Dep-info (`.d`) file caching alongside the `.o`

use anyhow::{Context, Result};
use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use super::flags::{FlagClass, FlagSpec, Matcher};
use super::{
    ArtifactKind, CompileResult, Compiler, CompilerKind, KeyCtx, RefuseReason, classify_by_filename,
};

/// What stage the compiler is being asked to produce.
///
/// Cargo's `cc` crate (and most build systems) use `-c` for the
/// per-file compile step that produces a `.o`, then a separate
/// invocation that links them into the final executable / library.
/// Caching is most valuable for `Compile` mode (the per-file work
/// gets reused across invocations); `Link` mode caching is harder
/// (depends on every input `.o`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompileMode {
    /// `-c`: produce object file(s) from source. The default cache
    /// target for kache's cc support.
    Compile,
    /// (no `-c` flag): compile + link, producing an executable or
    /// dynamic library. Realistic to cache eventually but more
    /// failure-prone (linker version, link order, native lib search
    /// paths).
    Link,
    /// `-E`: preprocess only — emits the source after macro expansion.
    /// Used by build systems for header probing; rarely cached.
    /// Note: also matches the `cc` crate's family probe shape, which
    /// is handled BEFORE this parser via [`CcCompiler::recognizes_family_probe`].
    Preprocess,
    /// `-S`: produce assembly output. Niche; same caching profile
    /// as `Compile` in principle but rarely worth the engineering.
    Assemble,
}

/// `-O0` … `-O3`, plus the size and debug variants. Stored as the
/// raw character (`'0'`..`'3'`, `'s'`, `'z'`, `'g'`) so the cache
/// key can hash it directly without re-stringification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptLevel {
    O0,
    O1,
    O2,
    O3,
    /// `-Os` — optimize for size.
    Os,
    /// `-Oz` — optimize for size, more aggressive (clang-only).
    Oz,
    /// `-Og` — optimize while preserving debuggability.
    Og,
}

/// Dependency-info generation flags (`-MMD` / `-MD` / `-MF` / `-MT`).
///
/// Cargo uses these to figure out which headers a `.o` depends on
/// for incremental rebuild. kache caches the `.o` directly, so the
/// dep-info file is generated as a side effect — but its CONTENTS
/// (a Make-style dependency list) embed absolute paths that need
/// the same path-normalization treatment as rustc's dep-info.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DepInfoSpec {
    /// `-MD` (true) or `-MMD` (false). True = include system headers
    /// in the dep-info output; false = user headers only.
    pub include_system: bool,
    /// `-MF foo.d`: where to write the dep-info file. `None` means
    /// the compiler picks a default (typically next to the `.o`).
    pub output: Option<PathBuf>,
    /// `-MT target`: the make target name for dep-info entries.
    /// Defaults to the output object name.
    pub target: Option<String>,
}

/// Parsed C-family invocation.
///
/// Field order roughly matches the cache-key construction order
/// (compiler family + version, then flags affecting code gen, then
/// flags affecting layout, then sources). Keeping that consistency
/// makes the cache_key implementation (PR5-B) easier to read.
#[derive(Debug, Clone)]
pub struct CcArgs {
    /// argv[0] — the compiler binary path the wrapper was invoked as.
    pub program: String,
    /// argv[1..] verbatim — preserved for passthrough / re-execution.
    pub rest: Vec<String>,

    /// Source files (`.c`, `.cpp`, `.cc`, `.cxx`, `.m`, `.mm`).
    /// May be empty for link-only invocations or pure flag probes.
    pub sources: Vec<PathBuf>,
    /// Output path from `-o`. `None` = compiler default (varies by mode).
    pub output: Option<PathBuf>,
    /// What stage the compiler was asked to produce.
    pub mode: CompileMode,
    /// Include search paths from `-I dir` / `-Idir` (in declaration
    /// order — order matters for header search semantics).
    pub includes: Vec<PathBuf>,
    /// Defines from `-D NAME` / `-D NAME=VALUE` (declaration order).
    pub defines: Vec<(String, Option<String>)>,
    /// Optimization level.
    pub optimization: Option<OptLevel>,
    /// Debug-info level: `0` = none (`-g0`), through `3` = max
    /// (`-g3`). Bare `-g` is treated as `2` (compiler default).
    pub debug_level: Option<u8>,
    /// Language standard from `-std=c11` / `-std=c++17` etc.
    /// Stored without the `-std=` prefix.
    pub std: Option<String>,
    /// Position-independent code (`-fPIC` / `-fpic`).
    pub pic: bool,
    /// Dependency-info generation flags. `None` = no dep-info.
    pub depinfo: Option<DepInfoSpec>,
    /// Language override from `-x c` / `-x c++` / `-x objective-c`.
    /// Without this flag, the compiler infers from source extension.
    pub language_override: Option<String>,
}

/// Source file extensions the parser recognizes as C-family input.
/// Anything else gets ignored (left in `rest` for passthrough).
const SOURCE_EXTENSIONS: &[&str] = &[
    "c", "cc", "cpp", "cxx", "c++", "C", // C / C++
    "m", "mm", "M", // Objective-C / Objective-C++
    "i", "ii", // already-preprocessed
    "S", "s", "sx", // assembly
];

impl CcArgs {
    pub fn parse(args: &[String]) -> Result<Self> {
        let (program, rest) = args
            .split_first()
            .context("cc invocation missing argv[0]")?;

        let mut parsed = CcArgs {
            program: program.clone(),
            rest: rest.to_vec(),
            sources: Vec::new(),
            output: None,
            mode: CompileMode::Link, // default: compile + link
            includes: Vec::new(),
            defines: Vec::new(),
            optimization: None,
            debug_level: None,
            std: None,
            pic: false,
            depinfo: None,
            language_override: None,
        };

        // Walk argv with a peekable iterator so flags-with-separate-args
        // (e.g. `-o foo.o`, `-I /path`) can consume the next token.
        let mut iter = rest.iter().enumerate().peekable();
        let mut depinfo: Option<DepInfoSpec> = None;
        while let Some((_idx, arg)) = iter.next() {
            // Most flags fall into one of three shapes:
            //   - sticky:  `-O2`, `-Idir`, `-DNAME=val` (value glued to flag)
            //   - separate: `-o file`, `-I dir`, `-D NAME` (value in next arg)
            //   - bare:    `-c`, `-fPIC`, `-MMD` (no value)
            // We try sticky-prefix matches first (they're unambiguous),
            // then exact-match flags, then fall through to "next arg" form
            // for known-separate flags.
            match arg.as_str() {
                // ── Compile mode ─────────────────────────────────
                "-c" => parsed.mode = CompileMode::Compile,
                "-E" => parsed.mode = CompileMode::Preprocess,
                "-S" => parsed.mode = CompileMode::Assemble,

                // ── Output ───────────────────────────────────────
                "-o" => {
                    if let Some((_, val)) = iter.next() {
                        parsed.output = Some(PathBuf::from(val));
                    }
                }

                // ── PIC ──────────────────────────────────────────
                "-fPIC" | "-fpic" => parsed.pic = true,

                // ── Debug ────────────────────────────────────────
                // Bare `-g` is the compiler's default level (typically 2).
                "-g" => parsed.debug_level = Some(2),
                "-g0" => parsed.debug_level = Some(0),
                "-g1" => parsed.debug_level = Some(1),
                "-g2" => parsed.debug_level = Some(2),
                "-g3" => parsed.debug_level = Some(3),

                // ── Optimization ─────────────────────────────────
                // Bare `-O` is `-O1` per gcc/clang convention.
                "-O" | "-O1" => parsed.optimization = Some(OptLevel::O1),
                "-O0" => parsed.optimization = Some(OptLevel::O0),
                "-O2" => parsed.optimization = Some(OptLevel::O2),
                "-O3" => parsed.optimization = Some(OptLevel::O3),
                "-Os" => parsed.optimization = Some(OptLevel::Os),
                "-Oz" => parsed.optimization = Some(OptLevel::Oz),
                "-Og" => parsed.optimization = Some(OptLevel::Og),

                // ── Dep-info: bare flags ─────────────────────────
                "-MD" => {
                    let d = depinfo.get_or_insert_with(DepInfoSpec::default);
                    d.include_system = true;
                }
                "-MMD" => {
                    let d = depinfo.get_or_insert_with(DepInfoSpec::default);
                    d.include_system = false;
                }
                "-MF" => {
                    if let Some((_, val)) = iter.next() {
                        let d = depinfo.get_or_insert_with(DepInfoSpec::default);
                        d.output = Some(PathBuf::from(val));
                    }
                }
                "-MT" | "-MQ" => {
                    if let Some((_, val)) = iter.next() {
                        let d = depinfo.get_or_insert_with(DepInfoSpec::default);
                        d.target = Some(val.clone());
                    }
                }

                // ── Language override (`-x c++` etc.) ────────────
                "-x" => {
                    if let Some((_, val)) = iter.next() {
                        parsed.language_override = Some(val.clone());
                    }
                }

                // ── Include / Define: separate-arg form ──────────
                "-I" => {
                    if let Some((_, val)) = iter.next() {
                        parsed.includes.push(PathBuf::from(val));
                    }
                }
                "-D" => {
                    if let Some((_, val)) = iter.next() {
                        parsed.defines.push(parse_define(val));
                    }
                }

                // ── Sticky-prefix forms ──────────────────────────
                _ if let Some(rest) = arg.strip_prefix("-I") => {
                    parsed.includes.push(PathBuf::from(rest));
                }
                _ if let Some(rest) = arg.strip_prefix("-D") => {
                    parsed.defines.push(parse_define(rest));
                }
                _ if let Some(rest) = arg.strip_prefix("-std=") => {
                    parsed.std = Some(rest.to_string());
                }

                // ── Source files (positional) ────────────────────
                _ if !arg.starts_with('-') && looks_like_source(arg) => {
                    parsed.sources.push(PathBuf::from(arg));
                }

                // Unknown / unhandled — leave in `rest` for passthrough.
                _ => {}
            }
        }
        parsed.depinfo = depinfo;

        Ok(parsed)
    }

    /// Enumerate refuse-to-cache reasons the parsed invocation
    /// triggers. Returns an empty vector for "looks safe to cache".
    ///
    /// Each detection is conservative — we'd rather refuse a
    /// cacheable invocation than miscache an unsafe one. Specific
    /// patterns covered:
    ///
    /// - **Response files** (`@file.rsp`): the actual flags live in
    ///   another file we'd need to read + hash separately.
    /// - **Multi-arch fat binaries** (`-arch x86_64 -arch arm64`):
    ///   output is a single file containing multiple object slices,
    ///   doesn't fit the per-source-per-output model.
    /// - **Coverage instrumentation** (`--coverage`,
    ///   `-fprofile-arcs`, `-ftest-coverage`): coverage tools need
    ///   the original source paths in profraw data; cache hits
    ///   would break coverage mapping.
    /// - **Split DWARF** (`-gsplit-dwarf`): produces a separate
    ///   `.dwo` file alongside the `.o`; output discovery would
    ///   need to know about the pair.
    /// - **Precompiled headers** (`-include-pch`, `-emit-pch`):
    ///   PCHs are non-portable across compiler versions and depend
    ///   on the entire include graph at PCH-build time.
    /// - **Modules** (`-fmodules`, `-fcxx-modules`): module
    ///   compilation has its own dependency model; doesn't fit the
    ///   per-TU cache model.
    /// - **Any flag not classified by [`CC_FLAGS`]**: the cache key
    ///   captures the preprocessor expansion plus the codegen flags
    ///   kache explicitly models (`FlagClass::ModeledInKey`) plus the
    ///   resolved `cc -###` tokens (`FlagClass::CapturedByProbe`). A
    ///   flag whose object-file effect is in none of those — an
    ///   unmodeled codegen flag (`-Ofast`, `-ffast-math`, `-march=…`,
    ///   `-ffunction-sections`), a cross-target (`-target`,
    ///   `--target=`), profiling (`-pg`), or a flag kache has never
    ///   seen — would miscache. The table is the source of truth;
    ///   anything it does not classify is refused with the offending
    ///   flags named in the reason.
    /// - **Output to stdout** (`-o -`): not a cacheable artifact.
    /// - **Preprocess / Assemble mode**: `-E` and `-S` produce
    ///   developer-facing output that's rarely worth caching and
    ///   tangles with the cc-crate probe pattern.
    pub fn refuse_reasons(&self) -> Vec<RefuseReason> {
        let mut reasons = Vec::new();

        // ── Non-`-c` mode refusals (short-circuit) ──
        //
        // First, check whether the invocation is the `-c` object
        // compile shape the flag classifier was designed for. If not,
        // the flag-classifier output below ("unsupported flag(s): -E")
        // is misleading — those flags aren't blocking caching of a
        // compile, they belong to a different invocation pattern.
        // Short-circuit to keep the reason list focused.
        //
        // All variants here are `Unsupported` — none of them are
        // conceptually uncacheable. `-E` / `-S` / link / output-to-
        // stdout are all deterministic input-to-output functions; the
        // reason kache doesn't cache them today is engineering
        // priority, not feasibility. Messages include "(not yet
        // supported)" so this is explicit in the bench output and
        // anyone reading `kache report`.
        match self.mode {
            CompileMode::Compile => {}
            CompileMode::Link => reasons.push(RefuseReason::Unsupported(
                "cc: link mode (whole-program caching not yet supported)",
            )),
            CompileMode::Preprocess => reasons.push(RefuseReason::Unsupported(
                "cc: preprocessor mode -E (not yet supported)",
            )),
            CompileMode::Assemble => reasons.push(RefuseReason::Unsupported(
                "cc: assembly mode -S (not yet supported)",
            )),
        }

        // Output to stdout — `-o -` is unambiguous; an `-o` followed
        // by a literal `-` arg. Cacheable in principle (cache the
        // stdout bytes); not yet implemented.
        if let Some(output) = &self.output
            && output.as_os_str() == "-"
        {
            reasons.push(RefuseReason::Unsupported(
                "cc: output to stdout (not yet supported)",
            ));
        }

        // If a non-`-c` refusal accumulated, return early — running
        // the flag classifier or feature checks would add misleading
        // noise ("unsupported flag(s): -E" when the real cause is
        // "this is preprocessor mode"). The single-source check is NOT
        // short-circuited here: a feature like a response file
        // (`@foo.opts`) appears to the parser as zero sources, and the
        // feature explanation is more useful than the bare symptom.
        if !reasons.is_empty() {
            return reasons;
        }

        // ── Feature refusals ──
        //
        // The invocation IS a single-source object compile, but uses a
        // feature kache doesn't model yet. These are the actionable
        // refusals: adding support would convert future invocations
        // into hits.

        // Response files: any arg starting with `@` (typically a
        // path to a file containing additional flags). The flags
        // inside aren't visible to our parser without recursive
        // expansion + path normalization.
        if self.rest.iter().any(|a| a.starts_with('@')) {
            reasons.push(RefuseReason::Unsupported(
                "cc: response file @file (expansion not yet supported)",
            ));
        }

        // Multi-arch (`-arch X -arch Y` produces a fat binary).
        // Single `-arch` is fine — many cc invocations specify it.
        let arch_count = self.rest.windows(2).filter(|w| w[0] == "-arch").count();
        if arch_count > 1 {
            reasons.push(RefuseReason::Unsupported(
                "cc: multi-arch -arch X -arch Y (fat-binary caching not yet supported)",
            ));
        }

        // Coverage instrumentation.
        for flag in &["--coverage", "-fprofile-arcs", "-ftest-coverage"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported(
                    "cc: coverage instrumentation (not yet supported)",
                ));
                break;
            }
        }

        // Split DWARF (separate .dwo file alongside .o).
        if self.rest.iter().any(|a| a == "-gsplit-dwarf") {
            reasons.push(RefuseReason::Unsupported(
                "cc: -gsplit-dwarf (not yet supported)",
            ));
        }

        // Precompiled headers.
        for flag in &["-include-pch", "-emit-pch"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported(
                    "cc: precompiled headers (not yet supported)",
                ));
                break;
            }
        }
        // `*.pch` / `*.gch` as -include argument also indicates PCH.
        let mut iter = self.rest.iter().peekable();
        while let Some(arg) = iter.next() {
            if arg == "-include"
                && let Some(next) = iter.peek()
                && (next.ends_with(".pch") || next.ends_with(".gch"))
            {
                reasons.push(RefuseReason::Unsupported(
                    "cc: precompiled headers (not yet supported)",
                ));
                break;
            }
        }

        // Modules (clang/gcc).
        for flag in &["-fmodules", "-fcxx-modules"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported("cc: modules (not yet supported)"));
                break;
            }
        }

        // Classifier gate — the structural safety net.
        //
        // kache's cc cache key captures the preprocessor expansion
        // plus the codegen flags it *explicitly* models (optimization,
        // debug level, `-std`, PIC, target arch) plus the resolved
        // `cc -###` token stream. A flag whose effect is captured by
        // none of those would change the object file WITHOUT changing
        // the key — a silent miscache.
        //
        // [`CC_FLAGS`] declares which flags fall into which category;
        // [`classify_cc_flag`] returns `None` for anything outside the
        // table. Unclassified flags include the genuinely unsafe
        // (`-Ofast`, `-march=native`), the cross-targets (`-target`,
        // `--target=`), profiling (`-pg`), and any flag kache has not
        // yet seen — all force a passthrough. The rejected flags are
        // named in the reason so it is visible which flags blocked
        // caching (and therefore which rows to add to `CC_FLAGS`).
        let rejected: Vec<&str> = self
            .rest
            .iter()
            .map(String::as_str)
            .filter(|a| classify_cc_flag(a).is_none())
            .collect();
        if !rejected.is_empty() {
            // Leak a per-invocation summary so it can ride in
            // `RefuseReason::Unsupported(&'static str)`. The wrapper
            // process handles one compile then exits, so the leak is
            // bounded and short-lived.
            let detail: &'static str = Box::leak(
                format!("cc: unsupported flag(s): {}", rejected.join(" ")).into_boxed_str(),
            );
            tracing::debug!("{detail} — passthrough");
            reasons.push(RefuseReason::Unsupported(detail));
        }

        // Single-source contract — last, so feature refusals
        // (response file, PCH-as-input, ...) get a chance to explain
        // *why* there's no parseable single source. Without that
        // ordering, `cc @foo.opts` would land here instead of getting
        // the more specific "response file (@file)" reason.
        //
        // Reported as `Unsupported` with "(not yet supported)" wording:
        // multi-source `cc -c a.c b.c` is conceptually N independent
        // single-source compiles bundled into one invocation —
        // per-source caching is on the roadmap, just unimplemented.
        // Zero-source falls under the same "kache doesn't yet handle
        // this invocation pattern" bucket; a future expansion of
        // response files or improved probe-vs-compile detection would
        // convert most of these.
        if self.sources.len() > 1 {
            reasons.push(RefuseReason::Unsupported(
                "cc: multi-source compile (per-source split not yet supported)",
            ));
        } else if self.sources.is_empty() {
            reasons.push(RefuseReason::Unsupported(
                "cc: no source file (not yet supported)",
            ));
        }

        reasons
    }

    /// The object file a `-c` compile produces.
    ///
    /// `-o <path>` if explicit; otherwise the gcc/clang default —
    /// the source file's stem with a `.o` extension, in the current
    /// working directory. Returns `None` only for degenerate
    /// invocations with no source (which `refuse_reasons` already
    /// rejects, so callers on the cache path won't hit `None`).
    pub fn object_output_path(&self) -> Option<PathBuf> {
        if let Some(o) = &self.output {
            return Some(o.clone());
        }
        let stem = self.sources.first()?.file_stem()?;
        Some(PathBuf::from(format!("{}.o", stem.to_string_lossy())))
    }

    /// Target architecture for cache-key / metadata purposes:
    /// an explicit `-arch X` if present, else the host arch.
    pub fn cache_target_arch(&self) -> String {
        cc_target_arch(self)
    }

    /// The subset of `rest` that identifies the *compile configuration*
    /// — per-translation-unit noise removed: source files, the `-o`
    /// output path, and dependency-file flags (`-MF`/`-MT`/`-MQ`) with
    /// their values. The resolved-invocation probe (`cc -###`) is
    /// memoized on this, so every TU of a build that shares a flag set
    /// reuses one probe record instead of re-resolving per file.
    pub fn config_args(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut iter = self.rest.iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "-o" | "-MF" | "-MT" | "-MQ" => {
                    iter.next(); // also drop the flag's value
                }
                _ if self
                    .sources
                    .iter()
                    .any(|s| s.to_str() == Some(arg.as_str())) => {}
                _ => out.push(arg.clone()),
            }
        }
        out
    }
}

/// Cache key schema version for C-family compiles. Bump when the key
/// composition changes in a way that could collide with old entries.
const CC_CACHE_KEY_VERSION: u32 = 2;

/// Resolve the target architecture for the cache key: an explicit
/// `-arch X` flag if present, else the host arch. (Multi-`-arch` is
/// refused upstream, so at most one value is found here.)
fn cc_target_arch(parsed: &CcArgs) -> String {
    parsed
        .rest
        .windows(2)
        .find(|w| w[0] == "-arch")
        .map(|w| w[1].clone())
        .unwrap_or_else(|| std::env::consts::ARCH.to_string())
}

/// Build the argv for a preprocess-only run: the original args with
/// mode/output/dep-info flags stripped and `-E -P` forced.
///
/// - `-c` / `-S` removed — we force `-E` (preprocess only).
/// - `-o <arg>` removed — preprocessed output must go to stdout, not
///   a file (we capture and hash it).
/// - `-MMD` / `-MD` / `-MF` / `-MT` / `-MQ` / `-MP` / `-MG` removed —
///   dep-info generation is irrelevant to preprocessor *content* and
///   `-MF` would redirect output.
/// - `-E -P` prepended. `-P` suppresses line markers
///   (`# 1 "/abs/path/header.h"`), so the hash captures expanded
///   *content* without leaking machine-local header paths — that's
///   what makes the key portable across machines.
fn build_preprocess_args(parsed: &CcArgs) -> Vec<String> {
    let mut out = vec!["-E".to_string(), "-P".to_string()];
    let mut iter = parsed.rest.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-c" | "-S" => {}
            "-o" | "-MF" | "-MT" | "-MQ" => {
                iter.next(); // also drop the flag's value
            }
            "-MMD" | "-MD" | "-MP" | "-MG" => {}
            _ => out.push(arg.clone()),
        }
    }
    out
}

/// Hash the preprocessor expansion of the translation unit.
///
/// Runs `<cc> -E -P ...` with `SOURCE_DATE_EPOCH` pinned so the
/// `__DATE__` / `__TIME__` macros expand deterministically (without
/// this the hash would change every second → ~0% hit rate). The
/// expansion includes every `#include`d header transitively, so any
/// header change invalidates the key automatically — no separate
/// dependency tracking needed.
fn preprocess_hash(parsed: &CcArgs) -> Result<String> {
    let pp_args = build_preprocess_args(parsed);
    crate::opcounts::record_preprocessor_run();
    let output = Command::new(&parsed.program)
        .args(&pp_args)
        // Pin the build timestamp. gcc + clang both honor
        // SOURCE_DATE_EPOCH for __DATE__ / __TIME__ expansion.
        .env("SOURCE_DATE_EPOCH", "0")
        .output()
        .with_context(|| format!("running preprocessor `{}`", parsed.program))?;
    if !output.status.success() {
        // Preprocess failed — the real compile would also fail.
        // Bail so the wrapper falls back to passthrough, which runs
        // the real compiler and surfaces the real diagnostic.
        anyhow::bail!("preprocessor exited {} for cache key", output.status);
    }
    Ok(blake3::hash(&output.stdout).to_hex().to_string())
}

/// Whether a positional argument looks like a C-family source file
/// (matches one of the recognized extensions in [`SOURCE_EXTENSIONS`]).
/// Conservative: extensionless files or unknown extensions are NOT
/// treated as sources, even if they happen to be C code in practice.
fn looks_like_source(arg: &str) -> bool {
    Path::new(arg)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| SOURCE_EXTENSIONS.contains(&e))
        .unwrap_or(false)
}

/// Parse a `-D NAME` or `-D NAME=VALUE` argument value.
fn parse_define(s: &str) -> (String, Option<String>) {
    match s.split_once('=') {
        Some((name, value)) => (name.to_string(), Some(value.to_string())),
        None => (s.to_string(), None),
    }
}

/// Cc flag classification table — the declarative source of truth
/// for "how does kache treat this argument?".
///
/// Each row pairs a [`Matcher`] with a [`FlagClass`] and a `source`
/// reference. See [`crate::compiler::flags`] for the matcher /
/// classification vocabulary and for the audit / extensibility
/// guarantees this shape delivers.
///
/// **Adding a flag**: drop a row in the appropriate `class`
/// section, point `source` at the issue / PR that introduced it,
/// and write a test for the new pattern. Done.
///
/// **Reading the table**: `class` answers "why is this safe?".
/// `ModeledInKey` = the parser extracts it into a typed field.
/// `CapturedByProbe` = `cc -###` resolves it into `-cc1` tokens
/// the cache key already hashes. `PreprocessorCaptured` = the
/// preprocessor expansion hash subsumes its effect.
/// `NoObjectEffect` = it doesn't change the resulting object.
///
/// **Anything not in the table** is refused with `cc: unsupported
/// flag(s): …` — see [`CcArgs::refuse_reasons`]. The omission is
/// the safety signal: a flag kache has never seen could miscache,
/// so the conservative default is to passthrough.
pub static CC_FLAGS: &[FlagSpec] = &[
    // ── ModeledInKey: parser extracts into a typed field ──
    FlagSpec {
        // `-O` family: bare, digit (`-O0`..`-O3`), `-Os`/`-Oz`, `-Og`.
        // The regex names the family in one row; an out-of-set value
        // (`-Ofast`) deliberately falls through to refusal because the
        // parser doesn't model it. See `CcArgs::parse`.
        matcher: Matcher::Regex(r"-O[0-3sz]?|-Og"),
        class: FlagClass::ModeledInKey,
        source: "PR #94 — opt level. Regex captures family; -Ofast/+others fall through to refuse.",
    },
    FlagSpec {
        // `-g` family: bare or with a level digit (`-g0`..`-g3`). The
        // parser extracts the level into `debug_level`. Variants like
        // `-gdwarf-5` / `-ggdb` / `-gline-tables-only` change debug
        // info but aren't modeled, so they're not on this row.
        matcher: Matcher::Regex(r"-g[0-3]?"),
        class: FlagClass::ModeledInKey,
        source: "PR #94 — debug level. Regex captures `-g`/`-g0..3`; -gdwarf-* etc. refuse.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fPIC"),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fpic"),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Prefix("-std="),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
    },
    FlagSpec {
        // Single `-arch <value>`. The parser sets `cache_target_arch`
        // from the resolved arch; multi-`-arch X -arch Y` is refused
        // separately in the procedural pass of `refuse_reasons`.
        matcher: Matcher::Exact("-arch"),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
    },
    // ── CapturedByProbe: `cc -###` resolved tokens differentiate ──
    //
    // Each row's effect on the resulting object is captured by the
    // resolved `cc -###` `-cc1` token stream that the cache key
    // already hashes (see `cache_key`'s `resolved:` tokens). Identical
    // user-facing flags → identical resolved tokens → same key;
    // different values → different tokens → different key. Safety holds
    // when the probe resolves on the host compiler (clang today; gcc's
    // `-###` is on the roadmap but currently returns `None`, in which
    // case these flags would silently no-op for keying — the same
    // gap that already exists for `-O2`/`-fPIC`/etc., not introduced
    // by this row.
    //
    // Initial population sourced from the Firefox/Gecko Darwin
    // baseline (kunobi-ninja/kache#114): ~4,476 single-source compiles
    // per Firefox build that previously passed through unnecessarily.
    FlagSpec {
        matcher: Matcher::Prefix("-mmacosx-version-min="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — Darwin deployment target.",
    },
    FlagSpec {
        matcher: Matcher::Prefix("-fstrict-flex-arrays="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — strict-flex-arrays codegen knob.",
    },
    FlagSpec {
        matcher: Matcher::Prefix("-ffp-contract="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — fp-contract codegen knob.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-pthread"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — pthread feature switch (also visible via _REENTRANT in preprocessor).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fstack-protector-strong"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — stack-protector codegen mode.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-math-errno"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — math-errno codegen knob.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-strict-aliasing"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — alias-analysis codegen knob.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-omit-frame-pointer"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — frame-pointer codegen knob.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-funwind-tables"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — unwind-tables codegen knob.",
    },
    // Firefox debug-info & clang argument-wrapper flags
    // (kunobi-ninja/kache#117). The debug-info flags affect DWARF
    // sections of the object; clang's `-###` expands them into
    // `-cc1 -dwarf-version=4` / `-dwarf-linkage-names=Abstract` / etc.,
    // so the resolved-tokens hash differentiates them per-value.
    //
    // `-gdwarf-4` is *not* wildcarded over the DWARF version digit on
    // purpose: `-gdwarf-5` produces a different (larger, newer-toolchain-
    // dependent) object and isn't part of #117's evidence. If another
    // workload needs it, file a follow-up and add a row.
    FlagSpec {
        matcher: Matcher::Exact("-gdwarf-4"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #117 — DWARF v4 emission (Firefox baseline).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-gsimple-template-names"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #117 — clang template-name compression in debug info.",
    },
    FlagSpec {
        // `-mllvm=` passes through to LLVM. Different `-mllvm`
        // values can do arbitrary codegen things, so a `Prefix("-mllvm=")`
        // wildcard would silently accept unmodeled codegen flags. List
        // specific values that workloads need; `-Mllvm=…` etc. still
        // refuse.
        matcher: Matcher::Exact("-mllvm=-dwarf-linkage-names=Abstract"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #117 — LLVM debug-info abstraction (Firefox baseline). Listed by exact value rather than `-mllvm=*` wildcard so unmodeled LLVM flags still refuse.",
    },
    // C++ ABI, RTTI, and exception flags (kunobi-ninja/kache#116).
    // Each row affects the resulting object materially — `-fno-rtti`
    // omits RTTI tables, `-fno-exceptions` skips exception-handling
    // tables, `-stdlib=libc++` vs `libstdc++` selects a different C++
    // standard library with different ABI defaults. Clang's `-###`
    // captures all of them in the resolved `-cc1` invocation, so the
    // cache key differentiates per-value via the resolved-tokens hash.
    //
    // Both the positive and negative forms are listed (`-frtti` /
    // `-fno-rtti`, `-fexceptions` / `-fno-exceptions`) because a build
    // may explicitly request either mode — they're conflicting and the
    // cache must distinguish them, which is automatic via the probe.
    FlagSpec {
        // `-stdlib=libc++` (clang default on macOS), `-stdlib=libstdc++`
        // (typical on Linux). Values are a small fixed set; the probe
        // resolves each into a distinct `-cc1` form.
        matcher: Matcher::Prefix("-stdlib="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ standard-library selector (libc++ / libstdc++).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-exceptions"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ exception mode (off).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fexceptions"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ exception mode (on).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-rtti"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ RTTI mode (off).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-frtti"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ RTTI mode (on).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-sized-deallocation"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ sized-deallocation (disabled).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-aligned-new"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ aligned new/delete (disabled).",
    },
    // ELF symbol-visibility defaults (Firefox bench evidence, post-#146).
    // `-fvisibility=hidden` and `-fvisibility-inlines-hidden` are pure-
    // codegen knobs that change the object's exported symbol table; same
    // source + same flag pair → same object bytes. Clang's `cc -###`
    // resolves each into a distinct `-cc1 -fvisibility hidden` /
    // `-fvisibility-inlines-hidden` token, so the resolved-tokens hash
    // differentiates them. Single highest-volume passthrough on a
    // Firefox warm build: 2987 of 3475 refused compiles came from this
    // pair (86% of the cc passthrough wall).
    //
    // Listed by `Exact` value (not `Prefix("-fvisibility=")`) so
    // unmodeled visibility modes (`default`, `protected`, `internal`)
    // still refuse — same conservative convention as the #116 cluster.
    FlagSpec {
        matcher: Matcher::Exact("-fvisibility=hidden"),
        class: FlagClass::CapturedByProbe,
        source: "Firefox bench evidence (post-#146) — symbol visibility default = hidden.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fvisibility-inlines-hidden"),
        class: FlagClass::CapturedByProbe,
        source: "Firefox bench evidence (post-#146) — inline-function visibility default = hidden.",
    },
    // Target / arch / WASM / ObjC / section flags
    // (kunobi-ninja/kache#115). Each row affects the resulting object
    // materially — `--target=` changes the entire output architecture,
    // `-march=` picks a CPU baseline, `-msimd128` enables WASM SIMD,
    // section flags reshape the object layout. Clang's `cc -###`
    // resolves each into the `-cc1` token stream (target triple,
    // target-cpu, target-feature list, language mode, section options),
    // so the resolved-tokens hash differentiates per-value and a
    // cross-target hit can't serve a foreign object.
    //
    // These flags were previously in the refuse-list (catch-all "would
    // serve a foreign object" guard); the explicit classification
    // makes them safe via the probe, with the boundary tests pinning
    // adjacent / unmodeled cases.
    FlagSpec {
        // Sticky `--target=arm64-apple-macosx` / `--target=wasm32-wasi`
        // / `--target=aarch64-linux-gnu`. The probe resolves the
        // triple into a `-cc1 -triple <value>` token, so different
        // targets produce different keys.
        matcher: Matcher::Prefix("--target="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — cross-compilation target triple (sticky form).",
    },
    FlagSpec {
        // Separate-arg form: `-target <triple>`. The value classifies
        // as a positional (no leading `-`), so this row only needs to
        // accept the flag itself.
        matcher: Matcher::Exact("-target"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — cross-compilation target triple (separate-arg form).",
    },
    FlagSpec {
        // `-march=` family: `native`, `armv8-a`, `armv8.2-a+dotprod`,
        // `armv8.2-a+i8mm`, etc. The probe captures the resolved
        // `-target-cpu` and `-target-feature` list, so `native` on
        // host A vs host B produces different keys (correct: they're
        // different objects).
        matcher: Matcher::Prefix("-march="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — architecture selection. `Prefix` is safe because the probe resolves the value into target-cpu/target-feature tokens.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-msimd128"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — WASM SIMD128 enable.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-ffunction-sections"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — function-per-section object layout.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fdata-sections"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — data-per-section object layout.",
    },
    FlagSpec {
        // `-Wa,*` passes through to the assembler. Different `-Wa,*`
        // values do arbitrary assembler things — listed as `Exact` for
        // the specific Firefox value (per #115's evidence) so a wildcard
        // `Prefix("-Wa,")` doesn't silently accept unmodeled assembler
        // flags. `--noexecstack` sets a section flag on the object.
        matcher: Matcher::Exact("-Wa,--noexecstack"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — assembler: non-executable stack section flag. Listed by exact value rather than `-Wa,*` wildcard so unmodeled assembler flags still refuse.",
    },
    FlagSpec {
        // Separate-arg form: `-x <lang>`. Value is positional. The
        // probe resolves the language mode into the `-cc1` invocation.
        matcher: Matcher::Exact("-x"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — language override (separate-arg form).",
    },
    FlagSpec {
        // Sticky form. `-xobjective-c++` is the one #115 lists; other
        // sticky forms (`-xc`, `-xc++`, `-xobjective-c`) would need
        // explicit rows when they show up in a real workload.
        matcher: Matcher::Exact("-xobjective-c++"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — Objective-C++ language override (sticky form).",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fobjc-exceptions"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — Objective-C exception model.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fobjc-arc"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — Objective-C ARC mode.",
    },
    // ── PreprocessorCaptured: cc -E -P expansion hash subsumes effect ──
    FlagSpec {
        matcher: Matcher::Prefix("-D"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Prefix("-U"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Prefix("-I"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Prefix("--sysroot"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-include"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-imacros"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-isystem"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-iquote"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-idirafter"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-isysroot"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-nostdinc"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-nostdinc++"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-undef"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
    },
    // ── NoObjectEffect: diagnostics / dep-info / build mechanics ──
    FlagSpec {
        // `-W*` warnings — `-Werror` is included (it changes success/
        // failure of the compile, not the resulting object bytes).
        // The regex EXCLUDES `-Wl,*` / `-Wa,*` / `-Wp,*` (linker /
        // assembler / preprocessor passthrough forms that change the
        // resulting object); they need separate handling and aren't
        // covered here.
        matcher: Matcher::Regex(r"-W[^,]*"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94 — warnings. Regex excludes `-Wl,*`/`-Wa,*`/`-Wp,*` passthrough forms.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-w"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Prefix("-pedantic"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Prefix("-fdiagnostics-"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fcolor-diagnostics"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-color-diagnostics"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        // Dep-info generation: -MD, -MMD, -MF, -MT, -MQ, -MP, -MG.
        // All write the `.d` sidecar; none affect the object. Regex
        // captures the family; alternatives are equally tight in this
        // table layout but the row stays declarative this way.
        matcher: Matcher::Regex(r"-MM?D|-M[FTQPG]"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94 — dep-info sidecar flags.",
    },
    FlagSpec {
        matcher: Matcher::Exact("-c"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-o"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-pipe"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("-v"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    FlagSpec {
        matcher: Matcher::Exact("--verbose"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
    },
    // Clang argument-wrapper flags (kunobi-ninja/kache#117). These
    // bracket a section of the command line where clang suppresses
    // unused-argument warnings; they only affect diagnostics, never
    // the resulting object. Listed as `Exact` (not a paired/regional
    // matcher) because each flag classifies independently for caching
    // purposes — kache doesn't care whether they appear together.
    FlagSpec {
        matcher: Matcher::Exact("--start-no-unused-arguments"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #117 — clang unused-argument warning region (open).",
    },
    FlagSpec {
        matcher: Matcher::Exact("--end-no-unused-arguments"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #117 — clang unused-argument warning region (close).",
    },
];

/// Classify a cc argument. Wraps [`crate::compiler::flags::classify_against`]
/// over [`CC_FLAGS`] with a lazy regex cache. Returns `None` for any
/// argument no row matches — the caller treats that as "unsupported
/// flag, refuse to cache".
fn classify_cc_flag(arg: &str) -> Option<FlagClass> {
    static CACHE: OnceLock<HashMap<&'static str, Regex>> = OnceLock::new();
    crate::compiler::flags::classify_against(
        arg,
        CC_FLAGS,
        CACHE.get_or_init(|| crate::compiler::flags::build_regex_cache(CC_FLAGS)),
    )
}

/// The `-ffile-prefix-map` flag that rewrites the absolute build
/// directory to a relative `.`.
///
/// A `-g` compile bakes the absolute build directory into the object's
/// DWARF (`DW_AT_comp_dir`) and into `__FILE__` expansions, so the same
/// source compiled at two different paths yields byte-different
/// objects. kache is content-addressed: an object cached at one path
/// and restored at another would then carry a stale machine-local
/// build path. Mapping the build dir to `.` makes the object
/// path-independent — the cc analogue of the `--remap-path-prefix`
/// kache injects for rustc (kache #78).
///
/// `None` if the working directory can't be resolved; the compile then
/// runs unmodified — no worse than before.
fn file_prefix_map_arg() -> Option<String> {
    let cwd = std::env::current_dir().ok()?;
    Some(format!("-ffile-prefix-map={}=.", cwd.display()))
}

#[derive(Default)]
pub struct CcCompiler;

impl CcCompiler {
    pub fn new() -> Self {
        Self
    }

    /// Does this argv invoke a C-family compiler?
    ///
    /// Matches `cc`, `c++`, `gcc`, `g++`, `clang`, `clang++` and
    /// versioned variants (`gcc-13`, `clang++-17`). Path-prefixed
    /// forms (`/usr/bin/cc`) work via [`Path::file_name`].
    ///
    /// Owns its own detection rule so `super::detect_compiler` is a
    /// thin delegating dispatch rather than a central registry of
    /// "what's a compiler" knowledge.
    pub fn recognizes(args: &[String]) -> bool {
        let Some(arg0) = args.first() else {
            return false;
        };
        let path = Path::new(arg0);
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return false;
        };

        // Exact matches for the canonical command names.
        if matches!(name, "cc" | "c++" | "gcc" | "g++" | "clang" | "clang++") {
            return true;
        }

        // Versioned variants: gcc-13, clang-15, g++-12, etc.
        let stem = name.split('-').next().unwrap_or("");
        matches!(stem, "cc" | "c++" | "gcc" | "g++" | "clang" | "clang++")
            && name.len() > stem.len()
            && name.as_bytes()[stem.len()] == b'-'
    }

    /// Does this argv match the `cc` Rust crate's compiler-family
    /// probe shape, `kache -E <file>`?
    ///
    /// The cc crate uses this probe to detect compiler family
    /// (gcc / clang / MSVC) by reading `__VERSION__` from preprocessor
    /// output. It hardcodes `Command::new(program).arg("-E").arg(file)`,
    /// dropping any trailing args from `CC="kache cc"` — so without
    /// explicit passthrough kache would clap-error and the probe
    /// would silently fall back to a default family guess. Today
    /// that's a logged warning; once C/C++ caching lands and family
    /// identifies the cache key, it becomes silent miscaching across
    /// machines.
    ///
    /// Match is intentionally tight (`-E` + at least one more arg).
    /// Other probe shapes (`-?`, `-dumpmachine`, `-dumpversion`) can
    /// land here when their absence becomes a real symptom —
    /// over-broad matching would mask legitimate CLI typos.
    ///
    /// **Not a [`CompilerKind`].** A probe is a non-compiler invocation
    /// pattern that happens to need passthrough. The dispatch in
    /// `run_wrapper_mode` checks this *before* the compiler match.
    pub fn recognizes_family_probe(args: &[String]) -> bool {
        args.len() >= 2 && args[0] == "-E"
    }
}

impl Compiler for CcCompiler {
    type Parsed = CcArgs;

    fn kind(&self) -> CompilerKind {
        CompilerKind::Cc
    }

    fn parse(&self, args: &[String]) -> Result<CcArgs> {
        CcArgs::parse(args)
    }

    fn refuse_reasons(&self, parsed: &CcArgs) -> Vec<RefuseReason> {
        // Per-case detection from the parsed shape. The skeleton
        // catch-all is gone — single-source `-c` compiles with no
        // unsafe flags now produce an EMPTY refuse list, which is the
        // signal to the wrapper that this invocation is cacheable.
        parsed.refuse_reasons()
    }

    fn cache_key(&self, parsed: &CcArgs, ctx: &KeyCtx<'_, '_>) -> Result<String> {
        // Preconditions (guaranteed by the wrapper checking
        // refuse_reasons first): `-c` mode, exactly one source.
        let mut hasher = blake3::Hasher::new();

        hasher.update(b"cc_key_version:");
        hasher.update(CC_CACHE_KEY_VERSION.to_string().as_bytes());
        hasher.update(b"\n");

        // Compiler identity: family name (cc / gcc / clang — affects
        // codegen defaults) + the version string.
        let program_name = Path::new(&parsed.program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(parsed.program.as_str());
        hasher.update(b"compiler:");
        hasher.update(program_name.as_bytes());
        hasher.update(b"\n");
        // Compiler probe, memoized: the version line (`cc --version`,
        // compiler identity) and the resolved invocation (`cc -###`,
        // the driver's fully-expanded `-cc1` line). One probe per build
        // per flag set; the rest of the build reads the record.
        let config_args = parsed.config_args();
        let resolved = crate::probe::probe(
            ctx.cache_dir,
            &crate::probe::CcProber,
            &crate::probe::ProbeRequest {
                compiler: &parsed.program,
                args: &parsed.rest,
                key_args: &config_args,
            },
        )?;
        hasher.update(b"compiler_version:");
        hasher.update(resolved.version_line.as_bytes());
        hasher.update(b"\n");

        // Resolved compiler invocation: the `cc -###` `-cc1` line with
        // host-local paths sentinelled. Captures codegen the modeled
        // flags below miss — compiler defaults (`-mrelocation-model`,
        // `-ffp-contract`, the resolved `-target-cpu` and feature set).
        // `None` when `-###` could not be resolved (e.g. gcc, until the
        // gcc prober lands); the modeled flags then carry the key
        // alone, exactly as before.
        //
        // Tokens are hashed IN ORDER, and order is significant — that
        // is correct, not an oversight. `cc -###` is deterministic, so
        // the same (compiler, flags, env) always yields the same token
        // order: the key is stable, with no spurious misses. The tokens
        // must NOT be sorted — they interleave flag/value pairs as
        // adjacent elements (`-target-cpu`, `apple-m1`), so sorting the
        // flat list would scramble those pairs. The only cost of
        // order-significance is that two *different* flag invocations
        // that happen to resolve to the same object (same tokens,
        // different order) get different keys — a cache miss, never a
        // miscache. That is the safe direction.
        if let Some(tokens) = &resolved.resolved_tokens {
            hasher.update(b"resolved:");
            for tok in tokens {
                hasher.update(tok.as_bytes());
                hasher.update(b"\x1f");
            }
            hasher.update(b"\n");
        }

        // Target architecture.
        hasher.update(b"arch:");
        hasher.update(cc_target_arch(parsed).as_bytes());
        hasher.update(b"\n");

        // Codegen-affecting flags. These are partly redundant with
        // the preprocessor hash (defines affect macro expansion,
        // -std gates language features) but the redundancy is cheap
        // and defends against e.g. -std affecting codegen without
        // changing the expanded text.
        if let Some(opt) = parsed.optimization {
            hasher.update(b"opt:");
            hasher.update(format!("{opt:?}").as_bytes());
            hasher.update(b"\n");
        }
        if let Some(dbg) = parsed.debug_level {
            hasher.update(b"debug:");
            hasher.update(&[dbg]);
            hasher.update(b"\n");
        }
        if let Some(std) = &parsed.std {
            hasher.update(b"std:");
            hasher.update(std.as_bytes());
            hasher.update(b"\n");
        }
        hasher.update(b"pic:");
        hasher.update(&[parsed.pic as u8]);
        hasher.update(b"\n");

        // Preprocessor expansion — the load-bearing input. Captures
        // the source plus every transitively-included header plus
        // macro expansion. `-E -P` strips line markers so header
        // PATHS don't leak (cross-machine portable); SOURCE_DATE_EPOCH
        // pins __DATE__/__TIME__ (stable across builds).
        let pp_hash = preprocess_hash(parsed)?;
        hasher.update(b"preprocessed:");
        hasher.update(pp_hash.as_bytes());
        hasher.update(b"\n");

        Ok(hasher.finalize().to_hex().to_string())
    }

    fn execute(&self, parsed: &CcArgs) -> Result<CompileResult> {
        // Invoke the underlying compiler with the original argv, plus a
        // `-ffile-prefix-map` so the object doesn't embed the absolute
        // build directory — see `file_prefix_map_arg`. Appended last so
        // it wins over any user-supplied map for the same prefix.
        crate::opcounts::record_compiler_run();
        let mut command = Command::new(&parsed.program);
        command.args(&parsed.rest);
        if let Some(flag) = file_prefix_map_arg() {
            command.arg(flag);
        }
        let output = command
            .output()
            .with_context(|| format!("executing {}", parsed.program))?;
        let exit_code = output.status.code().unwrap_or(1);

        // Output discovery: on a successful `-c` compile, the object
        // file is the cacheable artifact. Skip on failure (nothing to
        // cache) or non-Compile mode (refused upstream anyway). The
        // store name is the bare filename so restore can place it at
        // whatever `-o` path the warm invocation requests.
        let output_files = if exit_code == 0 && parsed.mode == CompileMode::Compile {
            match parsed.object_output_path() {
                Some(obj) if obj.exists() => {
                    let name = obj
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    vec![(obj, name)]
                }
                _ => Vec::new(),
            }
        } else {
            Vec::new()
        };

        Ok(CompileResult {
            exit_code,
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            output_files,
        })
    }

    fn classify_output(&self, _parsed: &CcArgs, name: &str) -> ArtifactKind {
        // Caching is not active; classification only matters once outputs
        // get stored. Delegate to the shared filename-based classifier so
        // when the cc store path lands, the kinds it produces are already
        // consistent with the rustc table for shared extensions (.o, .a,
        // .dylib, etc.).
        classify_by_filename(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    // ── recognize ────────────────────────────────────────────────

    #[test]
    fn recognizes_canonical_command_names() {
        for name in [
            "cc",
            "c++",
            "gcc",
            "g++",
            "clang",
            "clang++",
            "/usr/bin/cc",
            "/usr/bin/gcc",
            "/usr/local/bin/clang++",
        ] {
            assert!(
                CcCompiler::recognizes(&s(&[name])),
                "should recognize {name}"
            );
        }
    }

    #[test]
    fn recognizes_versioned_variants() {
        for name in ["gcc-13", "clang-15", "g++-12", "clang++-17"] {
            assert!(
                CcCompiler::recognizes(&s(&[name])),
                "should recognize versioned {name}"
            );
        }
    }

    #[test]
    fn recognizes_family_probe_matches_dash_e_with_file_arg() {
        assert!(CcCompiler::recognizes_family_probe(&s(&[
            "-E",
            "/tmp/probe.c"
        ])));
        assert!(CcCompiler::recognizes_family_probe(&s(&[
            "-E",
            "/tmp/detect_compiler_family.c"
        ])));
    }

    #[test]
    fn recognizes_family_probe_rejects_dash_e_alone() {
        assert!(!CcCompiler::recognizes_family_probe(&s(&["-E"])));
    }

    #[test]
    fn recognizes_family_probe_rejects_non_probe_shapes() {
        for argv in [
            vec![],
            s(&["-c", "foo.c"]),
            s(&["--version"]),
            s(&["-dumpmachine"]),
            s(&["report"]),
            s(&["foo.c"]),
        ] {
            assert!(
                !CcCompiler::recognizes_family_probe(&argv),
                "should NOT recognize {argv:?} as cc-probe"
            );
        }
    }

    #[test]
    fn recognizes_rejects_non_c_compilers() {
        for name in [
            "rustc",
            "ld",
            "ar",
            "make",
            "cmake",
            "ccache",
            "--crate-name",
        ] {
            assert!(
                !CcCompiler::recognizes(&s(&[name])),
                "should NOT recognize {name}"
            );
        }
        assert!(!CcCompiler::recognizes(&[]));
    }

    // ── parser: program / rest ──────────────────────────────────

    #[test]
    fn parse_splits_program_from_rest() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
        assert_eq!(parsed.program, "cc");
        assert_eq!(parsed.rest, vec!["-c", "foo.c", "-o", "foo.o"]);
    }

    // ── parser: compile mode ────────────────────────────────────

    #[test]
    fn parse_default_mode_is_link() {
        // No `-c`, `-E`, `-S` → default cargo / cc-crate "compile + link" shape.
        let parsed = CcArgs::parse(&s(&["cc", "foo.c", "-o", "foo"])).unwrap();
        assert_eq!(parsed.mode, CompileMode::Link);
    }

    #[test]
    fn parse_dash_c_sets_compile_mode() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
        assert_eq!(parsed.mode, CompileMode::Compile);
    }

    #[test]
    fn parse_dash_e_sets_preprocess_mode() {
        let parsed = CcArgs::parse(&s(&["cc", "-E", "foo.c"])).unwrap();
        assert_eq!(parsed.mode, CompileMode::Preprocess);
    }

    #[test]
    fn parse_dash_s_sets_assemble_mode() {
        let parsed = CcArgs::parse(&s(&["cc", "-S", "foo.c"])).unwrap();
        assert_eq!(parsed.mode, CompileMode::Assemble);
    }

    // ── parser: output ──────────────────────────────────────────

    #[test]
    fn parse_dash_o_sets_output() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "build/foo.o"])).unwrap();
        assert_eq!(parsed.output, Some(PathBuf::from("build/foo.o")));
    }

    #[test]
    fn parse_no_output_means_compiler_default() {
        // Without `-o`, the compiler picks (e.g., `a.out` for link mode).
        let parsed = CcArgs::parse(&s(&["cc", "foo.c"])).unwrap();
        assert_eq!(parsed.output, None);
    }

    // ── parser: sources ─────────────────────────────────────────

    #[test]
    fn parse_collects_source_files_by_extension() {
        let parsed =
            CcArgs::parse(&s(&["cc", "main.c", "util.c", "-o", "foo", "lib.cpp"])).unwrap();
        assert_eq!(
            parsed.sources,
            vec![
                PathBuf::from("main.c"),
                PathBuf::from("util.c"),
                PathBuf::from("lib.cpp"),
            ]
        );
    }

    #[test]
    fn parse_recognizes_objc_and_assembly_extensions() {
        // Coverage of the long extension list — pin all the obscure
        // ones so a future ergonomic cleanup of SOURCE_EXTENSIONS
        // (e.g. removing the `.M` Objective-C uppercase variant)
        // doesn't silently break parsing.
        for src in &[
            "foo.m", "foo.mm", "foo.M", // Objective-C / C++
            "foo.i", "foo.ii", // pre-preprocessed
            "foo.s", "foo.S", "foo.sx", // assembly
        ] {
            let parsed = CcArgs::parse(&s(&["cc", "-c", src])).unwrap();
            assert_eq!(
                parsed.sources,
                vec![PathBuf::from(src)],
                "expected {src} to be recognized as a source"
            );
        }
    }

    #[test]
    fn parse_ignores_non_source_positional_args() {
        // Positional args without a recognized source extension stay
        // in `rest` (so they're passed through verbatim) but don't
        // count as sources.
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-lpthread"])).unwrap();
        assert_eq!(parsed.sources, vec![PathBuf::from("foo.c")]);
        // Library link flags etc. live in `rest` for re-execution.
        assert!(parsed.rest.contains(&"-lpthread".to_string()));
    }

    // ── parser: includes ────────────────────────────────────────

    #[test]
    fn parse_includes_separate_arg_form() {
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            "foo.c",
            "-I",
            "include",
            "-I",
            "/usr/local/include",
        ]))
        .unwrap();
        assert_eq!(
            parsed.includes,
            vec![
                PathBuf::from("include"),
                PathBuf::from("/usr/local/include"),
            ]
        );
    }

    #[test]
    fn parse_includes_sticky_form() {
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            "foo.c",
            "-Iinclude",
            "-I/usr/local/include",
        ]))
        .unwrap();
        assert_eq!(
            parsed.includes,
            vec![
                PathBuf::from("include"),
                PathBuf::from("/usr/local/include"),
            ]
        );
    }

    // ── parser: defines ─────────────────────────────────────────

    #[test]
    fn parse_defines_with_and_without_values() {
        let parsed = CcArgs::parse(&s(&[
            "cc", "-c", "foo.c", "-DFOO", "-DBAR=42", "-D", "BAZ=qux",
        ]))
        .unwrap();
        assert_eq!(
            parsed.defines,
            vec![
                ("FOO".to_string(), None),
                ("BAR".to_string(), Some("42".to_string())),
                ("BAZ".to_string(), Some("qux".to_string())),
            ]
        );
    }

    // ── parser: optimization / debug / std / pic ────────────────

    #[test]
    fn parse_optimization_levels() {
        for (flag, expected) in [
            ("-O0", OptLevel::O0),
            ("-O1", OptLevel::O1),
            ("-O", OptLevel::O1), // bare -O = -O1
            ("-O2", OptLevel::O2),
            ("-O3", OptLevel::O3),
            ("-Os", OptLevel::Os),
            ("-Oz", OptLevel::Oz),
            ("-Og", OptLevel::Og),
        ] {
            let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", flag])).unwrap();
            assert_eq!(parsed.optimization, Some(expected), "for {flag}");
        }
    }

    #[test]
    fn parse_debug_levels() {
        for (flag, expected) in [
            ("-g", 2u8), // bare -g = compiler default (2)
            ("-g0", 0),
            ("-g1", 1),
            ("-g2", 2),
            ("-g3", 3),
        ] {
            let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", flag])).unwrap();
            assert_eq!(parsed.debug_level, Some(expected), "for {flag}");
        }
    }

    #[test]
    fn parse_std_strips_prefix() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-std=c++17"])).unwrap();
        assert_eq!(parsed.std, Some("c++17".to_string()));
    }

    #[test]
    fn parse_pic_flags() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-fPIC"])).unwrap();
        assert!(parsed.pic);
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-fpic"])).unwrap();
        assert!(parsed.pic);
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c"])).unwrap();
        assert!(!parsed.pic);
    }

    // ── parser: depinfo ─────────────────────────────────────────

    #[test]
    fn parse_depinfo_mmd_excludes_system_headers() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD"])).unwrap();
        let d = parsed.depinfo.expect("dep-info should be set");
        assert!(!d.include_system);
        assert_eq!(d.output, None);
        assert_eq!(d.target, None);
    }

    #[test]
    fn parse_depinfo_md_includes_system_headers() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MD"])).unwrap();
        let d = parsed.depinfo.expect("dep-info should be set");
        assert!(d.include_system);
    }

    #[test]
    fn parse_depinfo_mf_sets_output_path() {
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MF", "build/foo.d"])).unwrap();
        let d = parsed.depinfo.expect("dep-info should be set");
        assert_eq!(d.output, Some(PathBuf::from("build/foo.d")));
    }

    #[test]
    fn parse_depinfo_mt_sets_target_name() {
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MT", "build/foo.o"])).unwrap();
        let d = parsed.depinfo.expect("dep-info should be set");
        assert_eq!(d.target, Some("build/foo.o".to_string()));
    }

    #[test]
    fn parse_no_depinfo_flags_means_no_depinfo_struct() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
        assert!(parsed.depinfo.is_none());
    }

    // ── parser: language override ───────────────────────────────

    #[test]
    fn parse_language_override() {
        let parsed = CcArgs::parse(&s(&["cc", "-x", "c++", "-c", "src"])).unwrap();
        assert_eq!(parsed.language_override, Some("c++".to_string()));
    }

    // ── classifier table validation ─────────────────────────────

    /// Every `Matcher::Regex` row in [`CC_FLAGS`] must compile as a
    /// valid anchored regex. CI safety: production lookups assume
    /// pre-validated patterns; a typo in a row should fail here, not
    /// at first use on a developer's machine.
    #[test]
    fn cc_flags_table_regexes_compile() {
        crate::compiler::flags::assert_table_regexes_compile(CC_FLAGS);
    }

    // ── refuse-to-cache: per-case ───────────────────────────────

    fn refuse_descriptions(args: &[&str]) -> Vec<&'static str> {
        let parsed = CcArgs::parse(&s(args)).unwrap();
        parsed
            .refuse_reasons()
            .iter()
            .map(|r| r.description())
            .collect()
    }

    #[test]
    fn refuses_response_files() {
        let descs = refuse_descriptions(&["cc", "-c", "@flags.rsp"]);
        assert!(
            descs.iter().any(|d| d.contains("response file")),
            "expected response-file refuse, got: {descs:?}"
        );
    }

    #[test]
    fn refuses_multi_arch() {
        // Single -arch is fine; multi -arch produces a fat binary.
        let single = refuse_descriptions(&["cc", "-c", "foo.c", "-arch", "arm64"]);
        assert!(!single.iter().any(|d| d.contains("multi-arch")));

        let multi =
            refuse_descriptions(&["cc", "-c", "foo.c", "-arch", "arm64", "-arch", "x86_64"]);
        assert!(
            multi.iter().any(|d| d.contains("multi-arch")),
            "expected multi-arch refuse, got: {multi:?}"
        );
    }

    #[test]
    fn refuses_coverage_instrumentation() {
        for flag in &["--coverage", "-fprofile-arcs", "-ftest-coverage"] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", flag]);
            assert!(
                descs.iter().any(|d| d.contains("coverage")),
                "expected coverage refuse for {flag}, got: {descs:?}"
            );
        }
    }

    #[test]
    fn refuses_split_dwarf() {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-gsplit-dwarf"]);
        assert!(
            descs.iter().any(|d| d.contains("gsplit-dwarf")),
            "expected gsplit-dwarf refuse, got: {descs:?}"
        );
    }

    #[test]
    fn refuses_precompiled_headers() {
        // The `-include foo.pch` form
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-include", "stdafx.pch"]);
        assert!(
            descs.iter().any(|d| d.contains("precompiled")),
            "expected PCH refuse, got: {descs:?}"
        );
        // The explicit `-emit-pch` form
        let descs = refuse_descriptions(&["cc", "-c", "foo.h", "-emit-pch"]);
        assert!(
            descs.iter().any(|d| d.contains("precompiled")),
            "expected PCH refuse for -emit-pch, got: {descs:?}"
        );
    }

    #[test]
    fn refuses_modules() {
        for flag in &["-fmodules", "-fcxx-modules"] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", flag]);
            assert!(
                descs.iter().any(|d| d.contains("modules")),
                "expected modules refuse for {flag}, got: {descs:?}"
            );
        }
    }

    #[test]
    fn refuses_output_to_stdout() {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "-"]);
        assert!(
            descs.iter().any(|d| d.contains("stdout")),
            "expected stdout-output refuse, got: {descs:?}"
        );
    }

    #[test]
    fn refuses_flags_unclassified_in_cc_flags_table() {
        // Flags whose object-file effect kache does not capture in the
        // cache key — i.e. no row in `CC_FLAGS` matches them. Each
        // would miscache → must passthrough. Spans every shape, not
        // just `-f…` / `-m…`: unmodeled optimization / debug variants,
        // cross-targets, profiling.
        for flag in &[
            // unmodeled -f… / -m… codegen flags
            "-ffast-math",
            "-fsanitize=address",
            "-funroll-loops",
            "-fno-pic",
            "-mtune=skylake",
            "-mavx2",
            // unmodeled optimization / debug variants
            "-Ofast",
            "-gdwarf-5",
            "-ggdb",
            "-gline-tables-only",
            // profiling instrumentation
            "-pg",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                descs.iter().any(|d| d.contains("unsupported flag")),
                "expected classifier refuse for {flag}, got: {descs:?}"
            );
        }
    }

    #[test]
    fn cc_flags_table_classifies_known_cache_safe_flags() {
        // Flags kache fully accounts for: modeled codegen (opt / debug
        // / std / pic / arch), preprocessor-captured (defines /
        // includes / sysroot), and no-object-effect (warnings /
        // dep-info / mechanics). None should trip the classifier.
        for flag in &[
            "-O2",
            "-O0",
            "-Og",
            "-g",
            "-g2",
            "-std=c11",
            "-fPIC",
            "-fpic", // modeled codegen
            "-DFOO=1",
            "-Iinclude",
            "-isystem",
            "-include",
            "-nostdinc",
            "-undef", // preprocessor
            "-Wall",
            "-Wextra",
            "-Werror",
            "-Wno-unused",
            "-w",
            "-pedantic", // diagnostics
            "-pipe",
            "-MMD",
            "-MF",
            "-fdiagnostics-color", // mechanics / dep-info / diag
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} is cache-safe and must NOT trip the classifier, got: {descs:?}"
            );
        }
    }

    /// Gecko/Darwin baseline flags (kunobi-ninja/kache#114): codegen
    /// knobs whose effect is captured by clang's `cc -###` resolved
    /// invocation (which the cache key already hashes), so they're
    /// cache-safe even though kache doesn't model them explicitly.
    /// These were the inaugural `FlagClass::CapturedByProbe` rows in
    /// `CC_FLAGS` (#137).
    ///
    /// Each was previously refused as "unsupported flag" and forced
    /// passthrough on Firefox builds — over 4,400 single-source
    /// compiles per build, per the issue's evidence.
    #[test]
    fn classifier_accepts_gecko_darwin_baseline_flags() {
        for flag in &[
            "-mmacosx-version-min=10.15",
            "-mmacosx-version-min=11.0",
            "-pthread",
            "-fstack-protector-strong",
            "-fstrict-flex-arrays=1",
            "-fstrict-flex-arrays=3",
            "-fno-math-errno",
            "-fno-strict-aliasing",
            "-ffp-contract=off",
            "-ffp-contract=on",
            "-fno-omit-frame-pointer",
            "-funwind-tables",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} should be classified (Gecko/Darwin baseline), got: {descs:?}"
            );
        }
    }

    /// A representative Firefox-style C compile: pile the full
    /// Gecko/Darwin baseline onto one `cc -c` invocation and assert
    /// the classifier accepts it. This is the headline contract from
    /// #114: this exact shape should *cache*, not passthrough.
    #[test]
    fn classifier_accepts_realistic_firefox_compile() {
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.c",
            "-o",
            "foo.o",
            "-O2",
            "-g",
            "-std=gnu11",
            "-mmacosx-version-min=10.15",
            "-pthread",
            "-fno-strict-aliasing",
            "-fno-math-errno",
            "-funwind-tables",
            "-fstack-protector-strong",
            "-fno-omit-frame-pointer",
            "-ffp-contract=off",
            "-fstrict-flex-arrays=1",
            // Mixed with already-allowed flags to confirm no cross-
            // contamination from the additions.
            "-Wall",
            "-Wno-unused-parameter",
            "-DMOZILLA_INTERNAL_API=1",
            "-I/some/include",
        ]);
        assert!(
            descs.is_empty(),
            "realistic Firefox compile should be fully cacheable, got: {descs:?}"
        );
    }

    /// Pin the boundary: variants OUTSIDE the listed set must still
    /// passthrough — we are not opening `-fno-*` / `-fstack-protector*`
    /// as wildcards.
    #[test]
    fn classifier_does_not_overreach_gecko_darwin_family() {
        for flag in &[
            // Inverse forms not listed in #114
            "-fmath-errno",
            "-fstrict-aliasing",
            "-fomit-frame-pointer",
            "-fno-unwind-tables",
            // Adjacent stack-protector variants not on the list
            "-fstack-protector",
            "-fstack-protector-all",
            // Lookalike that isn't the macOS deployment-target flag
            "-mmacosx-min-version=10.15",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} is NOT on the #114 list and must still refuse, got: {descs:?}"
            );
        }
    }

    /// Firefox debug-info & clang argument-wrapper flags
    /// (kunobi-ninja/kache#117). Each row was previously refused as
    /// "unsupported flag" — 4,275 single-source compiles per Firefox
    /// build, per the issue's evidence.
    #[test]
    fn classifier_accepts_firefox_debug_info_and_wrapper_flags() {
        for flag in &[
            "-gdwarf-4",
            "-gsimple-template-names",
            "-mllvm=-dwarf-linkage-names=Abstract",
            "--start-no-unused-arguments",
            "--end-no-unused-arguments",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} should be classified (#117 baseline), got: {descs:?}"
            );
        }
    }

    /// The argument-wrapper pair must work *together* on one
    /// invocation — that's the canonical clang usage shape
    /// (`--start-no-unused-arguments … <flags> … --end-no-unused-arguments`).
    /// Each flag classifies independently, but the test pins the
    /// realistic usage and guards against a future refactor that
    /// accidentally treats them as a region requiring special pairing.
    #[test]
    fn classifier_accepts_unused_arguments_wrapper_pair() {
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.c",
            "-o",
            "foo.o",
            "-O2",
            "--start-no-unused-arguments",
            "-Wno-unused-command-line-argument",
            "--end-no-unused-arguments",
        ]);
        assert!(
            descs.is_empty(),
            "wrapped pair should be fully cacheable, got: {descs:?}"
        );
    }

    /// Pin the boundary on #117's additions: adjacent variants must
    /// still passthrough so unsupported codegen flags don't slip in
    /// under the new rows.
    #[test]
    fn classifier_does_not_overreach_117_additions() {
        for flag in &[
            // DWARF version variants not on the #117 list
            "-gdwarf-3",
            "-gdwarf-5",
            "-gdwarf",
            // Other -g* options (already documented as out-of-set)
            "-gline-tables-only",
            // -mllvm wildcards must stay refused. The exact-string row
            // for `-mllvm=-dwarf-linkage-names=Abstract` does NOT open
            // `-mllvm=*` as a prefix; that's deliberate (per the issue's
            // out-of-scope note).
            "-mllvm=-some-other-flag",
            "-mllvm=-inline-threshold=1000",
            // Lookalike wrapper flags
            "--start-no-unused",
            "--no-unused-arguments",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} is NOT on the #117 list and must still refuse, got: {descs:?}"
            );
        }
    }

    /// C++ ABI / RTTI / exception flags (kunobi-ninja/kache#116).
    /// Each row affects the resulting object materially, and clang's
    /// `cc -###` resolved tokens differentiate them — RTTI on vs off,
    /// exceptions on vs off, and `-stdlib=libc++` vs `libstdc++` all
    /// produce distinct keys via the probe.
    #[test]
    fn classifier_accepts_cpp_abi_rtti_exception_flags() {
        for flag in &[
            "-stdlib=libc++",
            "-stdlib=libstdc++",
            "-fno-exceptions",
            "-fexceptions",
            "-fno-rtti",
            "-frtti",
            "-fno-sized-deallocation",
            "-fno-aligned-new",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} should be classified (#116 baseline), got: {descs:?}"
            );
        }
    }

    /// A realistic Firefox-style C++ compile: pile the full #116
    /// baseline plus already-allowed flags onto one `cc -c` invocation
    /// and assert the classifier accepts it as fully cacheable.
    #[test]
    fn classifier_accepts_realistic_firefox_cpp_compile() {
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.cpp",
            "-o",
            "foo.o",
            "-O2",
            "-g",
            "-std=gnu++17",
            "-stdlib=libc++",
            "-fno-exceptions",
            "-fno-rtti",
            "-fno-sized-deallocation",
            "-fno-aligned-new",
            // Mixed with previously-allowed Gecko/Darwin baseline flags
            // (#114) to confirm no cross-contamination between
            // additions.
            "-mmacosx-version-min=10.15",
            "-fno-strict-aliasing",
            "-fstack-protector-strong",
            "-Wall",
            "-DMOZILLA_INTERNAL_API=1",
        ]);
        assert!(
            descs.is_empty(),
            "realistic Firefox C++ compile should be fully cacheable, got: {descs:?}"
        );
    }

    /// Pin the boundary on #116: adjacent forms / lookalikes must
    /// still refuse so unmodeled codegen flags don't slip past via
    /// the new rows.
    #[test]
    fn classifier_does_not_overreach_116_additions() {
        for flag in &[
            // Sanitizers aren't on #116's list — they remained refused
            // before and must stay refused. (Visibility flags moved to
            // their own cluster, post-#146 — see
            // `classifier_does_not_overreach_visibility_additions`.)
            "-fsanitize=undefined",
            // Aligned-new POSITIVE form not on the list. The negative
            // form (`-fno-aligned-new`) is what Firefox uses; if a
            // workload needs `-faligned-new`, file a follow-up.
            "-faligned-new",
            "-fsized-deallocation",
            // `-stdlib=` lookalike that isn't actually the C++ stdlib
            // selector.
            "-fstdlib=libc++",
            // `-fno-rt*`/`-fno-ex*` near-matches that aren't on the list.
            "-fno-rt",
            "-fno-rttis",
            "-fexception",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
            assert!(
                descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} is NOT on the #116 list and must still refuse, got: {descs:?}"
            );
        }
    }

    /// ELF symbol-visibility defaults (Firefox bench evidence, post-#146).
    /// Both flags must classify so the warm Firefox build's largest
    /// passthrough bucket (2987 events) becomes cacheable.
    #[test]
    fn classifier_accepts_visibility_flags() {
        for flag in &["-fvisibility=hidden", "-fvisibility-inlines-hidden"] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} should classify (visibility cluster), got: {descs:?}"
            );
        }
    }

    /// Pin the boundary on the visibility cluster: only the two exact
    /// values Firefox uses are accepted. Other `-fvisibility=` modes
    /// and the negative form of `-fvisibility-inlines-hidden` must
    /// still refuse so unmodeled visibility codegen can't slip past.
    #[test]
    fn classifier_does_not_overreach_visibility_additions() {
        for flag in &[
            // Other -fvisibility= values aren't listed (Exact, not Prefix).
            "-fvisibility=default",
            "-fvisibility=protected",
            "-fvisibility=internal",
            // Bare / lookalikes / typos.
            "-fvisibility",
            "-fvisible=hidden",
            // Negative form of the inlines flag — different codegen.
            "-fno-visibility-inlines-hidden",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
            assert!(
                descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} is NOT on the visibility list and must still refuse, got: {descs:?}"
            );
        }
    }

    /// Target / arch / WASM / ObjC / section flags
    /// (kunobi-ninja/kache#115). Each row affects the resulting object
    /// materially; clang's `cc -###` resolves each into the `-cc1`
    /// token stream so the cache key differentiates per-value.
    #[test]
    fn classifier_accepts_target_arch_objc_flags() {
        for flag in &[
            // Sticky --target= for several real triples Firefox uses
            "--target=arm64-apple-macosx",
            "--target=wasm32-wasi",
            "--target=aarch64-linux-gnu",
            // Separate-arg form
            "-target",
            // -march= family — native + specific microarchs
            "-march=native",
            "-march=armv8-a",
            "-march=armv8.2-a+dotprod",
            "-march=armv8.2-a+i8mm",
            // WASM SIMD
            "-msimd128",
            // Section layout
            "-ffunction-sections",
            "-fdata-sections",
            // Assembler passthrough (specific value, not wildcard)
            "-Wa,--noexecstack",
            // Language override forms
            "-x",
            "-xobjective-c++",
            // ObjC codegen modes
            "-fobjc-exceptions",
            "-fobjc-arc",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} should be classified (#115 baseline), got: {descs:?}"
            );
        }
    }

    /// A realistic Firefox-style cross-compile invocation:
    /// `cc -c foo.c -O2 -g --target=wasm32-wasi -msimd128 …` (the
    /// WASM bundling pipeline) plus previously-allowed flags.
    /// Headline contract from #115's acceptance criteria — "tests
    /// cover wasm target flags".
    #[test]
    fn classifier_accepts_realistic_firefox_wasm_compile() {
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.c",
            "-o",
            "foo.o",
            "-O2",
            "-g",
            "-std=gnu11",
            "--target=wasm32-wasi",
            "-msimd128",
            "-ffunction-sections",
            "-fdata-sections",
            "-fno-strict-aliasing",
            "-Wa,--noexecstack",
            "-Wall",
            "-DMOZILLA_BUILD=1",
        ]);
        assert!(
            descs.is_empty(),
            "realistic Firefox WASM compile should be fully cacheable, got: {descs:?}"
        );
    }

    /// Realistic ObjC++ Firefox compile — the language override goes
    /// through, the ObjC-specific codegen flags go through. Pins
    /// #115's third acceptance criterion ("ObjC/ObjC++ language mode
    /// flags").
    #[test]
    fn classifier_accepts_realistic_firefox_objc_compile() {
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.mm",
            "-o",
            "foo.o",
            "-O2",
            "-g",
            "-xobjective-c++",
            "-fobjc-arc",
            "-fobjc-exceptions",
            "-fno-exceptions",
            "-fno-rtti",
            "-stdlib=libc++",
            "-mmacosx-version-min=11.0",
            "-march=armv8-a",
        ]);
        assert!(
            descs.is_empty(),
            "realistic Firefox ObjC++ compile should be fully cacheable, got: {descs:?}"
        );
    }

    /// Pin the boundary on #115: adjacent / unmodeled forms must
    /// still refuse so wildcards stay scoped to what #115 actually
    /// covers.
    #[test]
    fn classifier_does_not_overreach_115_additions() {
        for flag in &[
            // `-Wa,*` wildcard is NOT opened — only the specific
            // `--noexecstack` value is. Other assembler passthroughs
            // refuse.
            "-Wa,-mfp",
            "-Wa,--something-else",
            // `-x` sticky-form variants not on the list. The list
            // names `-xobjective-c++` explicitly; other languages
            // refuse until their workload appears.
            "-xc",
            "-xc++",
            "-xobjective-c",
            // ObjC variants not on the list
            "-fno-objc-arc",
            "-fobjc-weak",
            // Section flags not on the list (similar shape, distinct
            // codegen)
            "-fno-function-sections",
            "-fno-data-sections",
            // SIMD adjacent — not `-msimd128`
            "-msse4.2",
            "-mavx512f",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} is NOT on the #115 list and must still refuse, got: {descs:?}"
            );
        }
    }

    #[test]
    fn refuse_reason_names_the_rejected_flags() {
        // The refusal must report *which* flags blocked caching — that
        // visibility is what makes "add support over time" actionable.
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.c",
            "-o",
            "foo.o",
            "-ffast-math",
            "-fsanitize=address",
        ]);
        let detail = descs
            .iter()
            .find(|d| d.contains("unsupported flag"))
            .expect("expected an unsupported-flag refuse reason");
        assert!(
            detail.contains("-ffast-math"),
            "reason should name the flag: {detail}"
        );
        assert!(
            detail.contains("-fsanitize=address"),
            "reason should name every rejected flag: {detail}"
        );
    }

    #[test]
    fn refuses_preprocess_and_assemble_modes() {
        let preprocess = refuse_descriptions(&["cc", "-E", "foo.c"]);
        assert!(
            preprocess.iter().any(|d| d.contains("preprocessor")),
            "expected preprocessor-mode refuse, got: {preprocess:?}"
        );

        let assemble = refuse_descriptions(&["cc", "-S", "foo.c"]);
        assert!(
            assemble.iter().any(|d| d.contains("assembly")),
            "expected assembly-mode refuse, got: {assemble:?}"
        );
    }

    /// Non-`-c` mode refusals (preprocessor, assembly, link,
    /// output-to-stdout) must NOT carry "unsupported flag(s)" noise.
    /// Mixing them mis-categorizes a correctly-refused non-compile
    /// as a kache classifier gap. Each refusal is `Unsupported` with
    /// "(not yet supported)" in the message — none of these are
    /// conceptually uncacheable, just deferred.
    #[test]
    fn non_compile_refusal_does_not_carry_unsupported_flag_noise() {
        let compiler = CcCompiler::new();

        // Preprocessor mode. Pre-refactor this returned BOTH
        // "unsupported flag(s): -xc -P -E" AND "preprocessor mode
        // (-E)", inflating the "classifier gap" bucket. Post-refactor
        // only the mode refusal fires.
        let parsed = compiler
            .parse(&s(&["cc", "-xc", "-P", "-E", "foo.c"]))
            .unwrap();
        let reasons = compiler.refuse_reasons(&parsed);
        let descs: Vec<_> = reasons.iter().map(|r| r.description()).collect();
        assert!(
            descs.iter().any(|d| d.contains("preprocessor mode")),
            "preprocessor mode must be reported, got: {descs:?}"
        );
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "preprocessor-mode refusal must not carry 'unsupported flag' noise, got: {descs:?}"
        );
        // Must read as a deferral, not a permanent limitation.
        assert!(
            descs.iter().any(|d| d.contains("not yet supported")),
            "preprocessor mode message must read as deferral ('not yet supported'), got: {descs:?}"
        );

        // Link mode — also `Unsupported` with "(not yet supported)".
        // Same short-circuit: the flag classifier's complaint about
        // `-fuse-ld=lld` would be misleading because the issue is
        // "link mode", not the flag.
        let parsed = compiler
            .parse(&s(&["cc", "foo.o", "-fuse-ld=lld", "-o", "out"]))
            .unwrap();
        let reasons = compiler.refuse_reasons(&parsed);
        let descs: Vec<_> = reasons.iter().map(|r| r.description()).collect();
        assert!(
            descs.iter().any(|d| d.contains("link mode")),
            "link mode must be reported, got: {descs:?}"
        );
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "link-mode refusal must not carry 'unsupported flag' noise, got: {descs:?}"
        );
        assert!(
            reasons
                .iter()
                .any(|r| matches!(r, RefuseReason::Unsupported(d) if d.contains("link mode"))),
            "link mode must classify as Unsupported (roadmap), got: {reasons:?}"
        );
    }

    /// The complement: a real single-source compile with a single
    /// unmodeled flag MUST still report "unsupported flag(s)" — that
    /// case is exactly what the bench's "classifier gap" bucket is
    /// for, and what the next CC_FLAGS row would fix.
    #[test]
    fn compile_mode_unmodeled_flag_still_reports_unsupported_flag() {
        let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", "-Ofast"]);
        assert!(
            descs.iter().any(|d| d.contains("unsupported flag")),
            "compile-mode unmodeled flag must still report 'unsupported flag', got: {descs:?}"
        );
    }

    #[test]
    fn refuses_nothing_for_clean_compile_invocation() {
        // The shape we WANT to cache: compile-only, single source,
        // explicit output, common flags. Only the skeleton catch-all
        // should fire (added in Compiler::refuse_reasons, not in
        // CcArgs::refuse_reasons), so the parser-level check is empty.
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            "src/foo.c",
            "-o",
            "build/foo.o",
            "-O2",
            "-g",
            "-fPIC",
            "-Iinclude",
        ]))
        .unwrap();
        assert!(
            parsed.refuse_reasons().is_empty(),
            "clean compile invocation should have no parser-level refuse reasons; got: {:?}",
            parsed.refuse_reasons()
        );
    }

    // ── Compiler trait: refuse / execute / classify ─────────────

    #[test]
    fn refuse_reasons_empty_for_cacheable_single_source_compile() {
        // The skeleton catch-all is GONE. A single-source `-c`
        // compile with no unsafe flags now produces an EMPTY refuse
        // list — that's the signal to the wrapper that the
        // invocation is cacheable. When this test starts failing,
        // either a new refuse rule landed (intentional) or caching
        // got accidentally disabled (the bug to investigate).
        let compiler = CcCompiler::new();
        let parsed = compiler
            .parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"]))
            .unwrap();
        assert!(
            compiler.refuse_reasons(&parsed).is_empty(),
            "single-source -c compile must be cacheable, got: {:?}",
            compiler
                .refuse_reasons(&parsed)
                .iter()
                .map(|r| r.description())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn refuse_reasons_refuses_link_mode() {
        // Link (the default mode — no `-c`) is not cacheable in this
        // phase. Whole-program caching is a separate, harder problem.
        let compiler = CcCompiler::new();
        let parsed = compiler.parse(&s(&["cc", "foo.c", "-o", "foo"])).unwrap();
        let descs: Vec<_> = compiler
            .refuse_reasons(&parsed)
            .iter()
            .map(|r| r.description())
            .collect();
        assert!(
            descs.iter().any(|d| d.contains("link mode")),
            "link invocation must be refused, got: {descs:?}"
        );
    }

    #[test]
    fn refuse_reasons_refuses_multi_source_compile() {
        // `-c a.c b.c` produces two .o files — outside the
        // single-translation-unit cache model. Per-source caching is
        // on the roadmap, message reads as deferral.
        let compiler = CcCompiler::new();
        let parsed = compiler.parse(&s(&["cc", "-c", "a.c", "b.c"])).unwrap();
        let reasons = compiler.refuse_reasons(&parsed);
        let descs: Vec<_> = reasons.iter().map(|r| r.description()).collect();
        assert!(
            descs.iter().any(|d| d.contains("multi-source")),
            "multi-source compile must be refused, got: {descs:?}"
        );
        assert!(
            descs.iter().any(|d| d.contains("not yet supported")),
            "multi-source message must read as deferral, got: {descs:?}"
        );
    }

    // ── object_output_path ──────────────────────────────────────

    #[test]
    fn object_output_path_uses_explicit_dash_o() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "src/foo.c", "-o", "build/foo.o"])).unwrap();
        assert_eq!(
            parsed.object_output_path(),
            Some(PathBuf::from("build/foo.o"))
        );
    }

    #[test]
    fn object_output_path_defaults_to_source_stem_dot_o() {
        // Without `-o`, gcc/clang default the object name to the
        // source stem + `.o` in the current directory.
        let parsed = CcArgs::parse(&s(&["cc", "-c", "src/foo.c"])).unwrap();
        assert_eq!(parsed.object_output_path(), Some(PathBuf::from("foo.o")));
    }

    // ── build_preprocess_args ───────────────────────────────────

    #[test]
    fn build_preprocess_args_forces_dash_e_dash_p_and_strips_mode() {
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-O2", "-Iinc"])).unwrap();
        let pp = build_preprocess_args(&parsed);
        // -E -P prepended.
        assert_eq!(&pp[0], "-E");
        assert_eq!(&pp[1], "-P");
        // -c and -o <arg> stripped (no file redirection of pp output).
        assert!(!pp.iter().any(|a| a == "-c"));
        assert!(!pp.iter().any(|a| a == "-o"));
        assert!(!pp.iter().any(|a| a == "foo.o"));
        // Preprocessing-relevant flags kept.
        assert!(pp.iter().any(|a| a == "-O2"));
        assert!(pp.iter().any(|a| a == "-Iinc"));
        assert!(pp.iter().any(|a| a == "foo.c"));
    }

    #[test]
    fn build_preprocess_args_strips_dep_info_flags() {
        // -MF would redirect dep-info output; -MMD/-MD/-MT are
        // irrelevant to preprocessor *content*. All stripped.
        let parsed = CcArgs::parse(&s(&[
            "cc", "-c", "foo.c", "-MMD", "-MF", "foo.d", "-MT", "foo.o",
        ]))
        .unwrap();
        let pp = build_preprocess_args(&parsed);
        for stripped in &["-MMD", "-MF", "foo.d", "-MT", "foo.o"] {
            assert!(
                !pp.iter().any(|a| a == stripped),
                "{stripped} should be stripped from preprocess args, got {pp:?}"
            );
        }
    }

    #[test]
    fn execute_returns_error_when_compiler_binary_missing() {
        let compiler = CcCompiler::new();
        let parsed = compiler
            .parse(&["this-binary-does-not-exist-pls-fail-1234567890".to_string()])
            .unwrap();
        let result = compiler.execute(&parsed);
        assert!(
            result.is_err(),
            "execute() must return Err when the compiler binary can't be spawned"
        );
    }

    #[test]
    fn file_prefix_map_arg_maps_the_cwd_to_dot() {
        // `execute` injects this so a `-g` object doesn't embed the
        // absolute build directory — making it path-independent.
        let arg = file_prefix_map_arg().expect("cwd resolves in tests");
        assert!(
            arg.starts_with("-ffile-prefix-map="),
            "unexpected flag shape: {arg}"
        );
        assert!(arg.ends_with("=."), "build dir must map to `.`: {arg}");
    }

    #[cfg(unix)]
    #[test]
    fn execute_propagates_non_zero_exit_when_compiler_runs_and_fails() {
        let compiler = CcCompiler::new();
        let parsed = compiler.parse(&["false".to_string()]).unwrap();
        let result = compiler
            .execute(&parsed)
            .expect("a failed-but-spawned compiler is Ok(non-zero), not Err");
        assert_ne!(
            result.exit_code, 0,
            "non-zero exit must reach the caller via CompileResult.exit_code"
        );
    }

    #[test]
    fn classify_output_delegates_to_shared_classifier() {
        let compiler = CcCompiler::new();
        let parsed = compiler.parse(&s(&["cc"])).unwrap();
        assert_eq!(
            compiler.classify_output(&parsed, "foo.o"),
            ArtifactKind::Object
        );
        assert_eq!(
            compiler.classify_output(&parsed, "libfoo.dylib"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            compiler.classify_output(&parsed, "foo.d"),
            ArtifactKind::DepInfo
        );
    }
}
