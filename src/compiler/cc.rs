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
//! - Any flag outside the allow-list (`flag_is_cache_safe`) — an
//!   unmodeled codegen flag, a cross-target, profiling, or simply a
//!   flag kache has not classified. Refused so an unknown flag is
//!   never silently cached.
//!
//! Future work (separate PRs):
//! - Link-mode / whole-executable caching
//! - `ar` archive caching
//! - Cross-machine cache sharing for C/C++ artifacts: SDKROOT
//!   sentinel + Mach-O OSO record stripping (issue #78)
//! - Dep-info (`.d`) file caching alongside the `.o`

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

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
    /// - **Any flag outside the allow-list**: the cache key captures
    ///   the preprocessor expansion plus the codegen flags kache
    ///   explicitly models (optimization / debug / `-std` / PIC /
    ///   arch). A flag whose object-file effect is *not* captured —
    ///   an unmodeled codegen flag (`-Ofast`, `-ffast-math`,
    ///   `-march=…`, `-ffunction-sections`), a cross-target
    ///   (`-target`, `--target=`), profiling (`-pg`), or a flag kache
    ///   has never seen — would miscache. `flag_is_cache_safe` is the
    ///   allow-list; anything it does not recognize is refused, with
    ///   the offending flags named in the reason.
    /// - **Output to stdout** (`-o -`): not a cacheable artifact.
    /// - **Preprocess / Assemble mode**: `-E` and `-S` produce
    ///   developer-facing output that's rarely worth caching and
    ///   tangles with the cc-crate probe pattern.
    pub fn refuse_reasons(&self) -> Vec<RefuseReason> {
        let mut reasons = Vec::new();

        // Response files: any arg starting with `@` (typically a
        // path to a file containing additional flags). The flags
        // inside aren't visible to our parser without recursive
        // expansion + path normalization.
        if self.rest.iter().any(|a| a.starts_with('@')) {
            reasons.push(RefuseReason::Unsupported("cc: response file (@file)"));
        }

        // Multi-arch (`-arch X -arch Y` produces a fat binary).
        // Single `-arch` is fine — many cc invocations specify it.
        let arch_count = self.rest.windows(2).filter(|w| w[0] == "-arch").count();
        if arch_count > 1 {
            reasons.push(RefuseReason::Unsupported(
                "cc: multi-arch (-arch X -arch Y)",
            ));
        }

        // Coverage instrumentation.
        for flag in &["--coverage", "-fprofile-arcs", "-ftest-coverage"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported("cc: coverage instrumentation"));
                break;
            }
        }

        // Split DWARF (separate .dwo file alongside .o).
        if self.rest.iter().any(|a| a == "-gsplit-dwarf") {
            reasons.push(RefuseReason::Unsupported("cc: -gsplit-dwarf"));
        }

        // Precompiled headers.
        for flag in &["-include-pch", "-emit-pch"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported("cc: precompiled headers"));
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
                reasons.push(RefuseReason::Unsupported("cc: precompiled headers"));
                break;
            }
        }

        // Modules (clang/gcc).
        for flag in &["-fmodules", "-fcxx-modules"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported("cc: modules"));
                break;
            }
        }

        // Allow-list gate — the structural safety net.
        //
        // kache's cc cache key captures the preprocessor expansion
        // plus the codegen flags it *explicitly* models (optimization,
        // debug level, `-std`, PIC, target arch). A flag OUTSIDE that
        // captured set would change the object file WITHOUT changing
        // the key — a silent miscache.
        //
        // So this is an allow-list, not a deny-list: `flag_is_cache_safe`
        // names the categories kache can account for, and ANYTHING
        // else — an unmodeled codegen flag (`-Ofast`, `-ffast-math`,
        // `-march=native`, `-ffunction-sections`), a cross-target
        // (`-target`, `--target=`), profiling (`-pg`), or a flag kache
        // has simply never seen — forces a passthrough. The rejected
        // flags are named in the reason, so it is visible which flags
        // blocked caching (and therefore which to add support for).
        let rejected: Vec<&str> = self
            .rest
            .iter()
            .map(String::as_str)
            .filter(|a| !flag_is_cache_safe(a))
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

        // Output to stdout — `-o -` is unambiguous; an `-o` followed
        // by a literal `-` arg.
        if let Some(output) = &self.output
            && output.as_os_str() == "-"
        {
            reasons.push(RefuseReason::Unsupported("cc: output to stdout"));
        }

        // Mode gate: kache caches only the `-c` object-compile step.
        //   - Link: whole-program caching (depends on every input .o,
        //     linker version, link order) — a much harder problem,
        //     deferred. The default mode, so this refusal is common.
        //   - Preprocess (-E) / Assemble (-S): developer-facing output,
        //     rarely worth caching, and -E tangles with the cc-crate
        //     family probe.
        match self.mode {
            CompileMode::Compile => {}
            CompileMode::Link => reasons.push(RefuseReason::Unsupported(
                "cc: link mode (whole-program caching not yet supported)",
            )),
            CompileMode::Preprocess => {
                reasons.push(RefuseReason::Unsupported("cc: preprocessor mode (-E)"))
            }
            CompileMode::Assemble => {
                reasons.push(RefuseReason::Unsupported("cc: assembly mode (-S)"))
            }
        }

        // Single-source contract: kache caches exactly one source per
        // invocation (one .o out). Multi-source `-c a.c b.c` produces
        // several .o files — uncommon (cargo's `cc` crate does one
        // source per invocation); zero sources is a link-only / probe
        // step. Both fall outside the per-translation-unit cache model.
        if self.sources.len() != 1 {
            reasons.push(RefuseReason::Unsupported("cc: not a single-source compile"));
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

/// Optimization flags kache models in the cc cache key — the exact
/// set `CcArgs::parse` extracts into `optimization`. A variant outside
/// this set (`-Ofast`) is NOT modeled, so it is not cache-safe.
const MODELED_OPT_FLAGS: &[&str] = &["-O", "-O0", "-O1", "-O2", "-O3", "-Os", "-Oz", "-Og"];

/// Debug flags kache models — the exact set `CcArgs::parse` extracts
/// into `debug_level`. A variant outside this set (`-gdwarf-5`,
/// `-ggdb`, `-gline-tables-only`) changes the object's debug info but
/// is not modeled, so it is not cache-safe.
const MODELED_DEBUG_FLAGS: &[&str] = &["-g", "-g0", "-g1", "-g2", "-g3"];

/// Whether a cc argument is safe to cache past — i.e. its effect on
/// the object file is either fully captured by kache's cache key, or
/// it has no effect on the object at all.
///
/// This is the **allow-list**. An argument is safe only if it falls
/// into one of the recognized categories below. ANYTHING else — an
/// unmodeled codegen flag, a cross-target, a flag kache has simply
/// never seen — is unsafe, and [`CcArgs::refuse_reasons`] forces a
/// passthrough. Erring toward "unsafe" is deliberate: an over-refusal
/// costs a cache miss; an under-refusal is a silent miscache.
fn flag_is_cache_safe(arg: &str) -> bool {
    // Non-flag positional — a source file, or the value of a
    // separate-argument flag (`-I dir`, `-o file`). Sources are
    // counted by the parser; flag values are harmless here.
    if !arg.starts_with('-') {
        return true;
    }

    // (1) Codegen flags kache models directly in the cache key.
    if MODELED_OPT_FLAGS.contains(&arg)
        || MODELED_DEBUG_FLAGS.contains(&arg)
        || arg == "-fPIC"
        || arg == "-fpic"
        || arg.starts_with("-std=")
        // Single `-arch` — the resolved target arch is hashed.
        // Multi-`-arch` is refused separately in `refuse_reasons`.
        || arg == "-arch"
    {
        return true;
    }

    // (2) Preprocessor-captured: the flag's entire compile-time effect
    //     is the header content / macros the `cc -E -P` expansion
    //     sees, and the cache key hashes that expansion verbatim.
    if arg.starts_with("-D")          // -DNAME      / -D NAME
        || arg.starts_with("-U")      // -UNAME      / -U NAME
        || arg.starts_with("-I")      // -Idir       / -I dir
        || arg.starts_with("--sysroot")
        || matches!(
            arg,
            "-include" | "-imacros" | "-isystem" | "-iquote" | "-idirafter"
                | "-isysroot" | "-nostdinc" | "-nostdinc++" | "-undef"
        )
    {
        return true;
    }

    // (3) No effect on the object of a successful compile: warnings
    //     (incl. `-Werror` — changes success/failure, not the bytes),
    //     dependency-file generation (writes a `.d` sidecar), and
    //     build mechanics. `-W?,` passthrough forms (`-Wl,`, `-Wp,`,
    //     `-Wa,`) carry a comma and are NOT treated as warnings —
    //     they fall through to "unsafe".
    if (arg.starts_with("-W") && !arg.contains(','))
        || arg == "-w"
        || arg.starts_with("-pedantic")
        || arg.starts_with("-fdiagnostics-")
        || matches!(arg, "-fcolor-diagnostics" | "-fno-color-diagnostics")
        || matches!(arg, "-MD" | "-MMD" | "-MF" | "-MT" | "-MQ" | "-MP" | "-MG")
        || matches!(arg, "-c" | "-o" | "-pipe" | "-v" | "--verbose")
    {
        return true;
    }

    // Everything else is unsafe — see `refuse_reasons`.
    false
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

    fn cache_key(&self, parsed: &CcArgs, ctx: &KeyCtx<'_>) -> Result<String> {
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
        // Invoke the underlying compiler with the original argv.
        crate::opcounts::record_compiler_run();
        let output = Command::new(&parsed.program)
            .args(&parsed.rest)
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
    fn refuses_flags_outside_the_allow_list() {
        // Flags whose object-file effect kache does not capture in the
        // cache key. Each would miscache → must passthrough. Spans
        // every shape, not just `-f…` / `-m…`: unmodeled optimization
        // / debug variants, cross-targets, profiling.
        for flag in &[
            // unmodeled -f… / -m… codegen flags
            "-ffast-math",
            "-fsanitize=address",
            "-funroll-loops",
            "-fno-rtti",
            "-fno-pic",
            "-fvisibility=hidden",
            "-ffunction-sections",
            "-march=native",
            "-mtune=skylake",
            "-mavx2",
            "-mmacosx-version-min=14.0",
            // unmodeled optimization / debug variants
            "-Ofast",
            "-gdwarf-5",
            "-ggdb",
            "-gline-tables-only",
            // cross-compilation target (would serve a foreign object)
            "-target",
            "--target=aarch64-linux-gnu",
            // profiling instrumentation
            "-pg",
            // language override — not modeled in the key
            "-x",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.c", "-o", "foo.o", flag]);
            assert!(
                descs.iter().any(|d| d.contains("unsupported flag")),
                "expected allow-list refuse for {flag}, got: {descs:?}"
            );
        }
    }

    #[test]
    fn allow_list_does_not_refuse_cache_safe_flags() {
        // Flags kache fully accounts for: modeled codegen (opt / debug
        // / std / pic / arch), preprocessor-captured (defines /
        // includes / sysroot), and no-object-effect (warnings /
        // dep-info / mechanics). None should trip the allow-list.
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
                "{flag} is cache-safe and must NOT trip the allow-list, got: {descs:?}"
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
            "--target=aarch64-linux-gnu",
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
            detail.contains("--target=aarch64-linux-gnu"),
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
        // single-translation-unit cache model.
        let compiler = CcCompiler::new();
        let parsed = compiler.parse(&s(&["cc", "-c", "a.c", "b.c"])).unwrap();
        let descs: Vec<_> = compiler
            .refuse_reasons(&parsed)
            .iter()
            .map(|r| r.description())
            .collect();
        assert!(
            descs.iter().any(|d| d.contains("single-source")),
            "multi-source compile must be refused, got: {descs:?}"
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
