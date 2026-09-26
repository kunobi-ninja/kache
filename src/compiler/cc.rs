//! C-family compiler (cc / gcc / g++ / clang / clang++ / c++).
//!
//! **C/C++ caching is live for the single-source `-c` compile.**
//! A `cc -c foo.c -o foo.o` invocation gets a content-addressed
//! cache entry; an identical re-invocation restores the `.o` and, when
//! requested, its `.d` dep-info sidecar without running the compiler.
//!
//! What's cached:
//! - **`-c` object compiles**, exactly one source per invocation.
//!   The cache key is the preprocessor expansion (`cc -E -P` with
//!   `SOURCE_DATE_EPOCH` pinned) plus compiler identity, target
//!   arch, and codegen flags. The preprocessor hash captures the
//!   source and every transitively-included header. The cold probe also writes
//!   a private complete dependency list; a later local invocation can reuse
//!   the expansion hash only while every source/header fingerprint matches,
//!   otherwise it preprocesses again. The key also digests **names** in
//!   user include dirs (`-I`, `-iquote`, and the source file's directory)
//!   so a header that appears in an earlier search dir cannot shadow a
//!   previously-read one without changing the key. `-E -P` strips line
//!   markers so header *paths* don't leak — the object key remains portable
//!   across machines and worktrees, while the optimization itself is
//!   deliberately local.
//!
//! What passes through (refused, see [`CcArgs::refuse_reasons`]):
//! - Link mode unless `[cache] cache_cc_links` / `KACHE_CACHE_CC_LINKS` is on
//!   (epic #762 / #259)
//! - Assemble (`-S`) mode
//! - Multi-source compiles, multi-arch fat binaries
//! - Response files, coverage instrumentation, split DWARF,
//!   precompiled headers, modules, `-o -`
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
//! - Mach-O OSO record stripping for cross-machine sharing of *linked*
//!   artifacts (issue #78) — deferred until link-mode caching exists, since
//!   `-c` object compiles carry no linker-emitted `N_OSO` records. The
//!   SDKROOT half of #78 is handled: the Apple SDK path is mapped to the
//!   `/kache/sdkroot` prefix-map target ([`CC_SDKROOT_SENTINEL`]).

use anyhow::{Context, Result};
use regex::Regex;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use super::flags::{Dialect, FlagClass, FlagSpec, Matcher};
use super::{
    Artifact, ArtifactKind, ArtifactSet, CompileResult, Compiler, CompilerAdapter, CompilerId,
    KeyCtx, RefuseReason, classify_by_filename,
};

/// Compiler driver family of a cc-wrapper invocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolFamily {
    /// gcc / cc and compatible drivers.
    Gnu,
    /// clang / clang++ in the default (gcc-compatible) driver mode.
    Clang,
    /// clang in MSVC driver mode (`clang-cl` or `--driver-mode=cl`).
    ClangCl,
}

/// Store name for a `-E` expansion that went to stdout (no `-o`).
pub(crate) const CC_STDOUT_STORE_NAME: &str = "stdout.i";

/// `-E` without `-o` (and without `-o -`): the expansion is process stdout.
pub(crate) fn cc_expansion_is_stdout(parsed: &CcArgs) -> bool {
    parsed.mode == CompileMode::Preprocess && parsed.output.is_none()
}

/// The `-E -P` / `/EP` key probe sets this so a kache shim does not cache
/// the probe as a user-facing stdout preprocess.
pub(crate) fn cc_is_internal_key_probe() -> bool {
    std::env::var_os("KACHE_CC_KEY_PROBE").is_some()
}

impl ToolFamily {
    /// The flag dialect this family speaks (Gnu and Clang share one).
    pub fn dialect(self) -> Dialect {
        match self {
            ToolFamily::Gnu | ToolFamily::Clang => Dialect::Gnu,
            ToolFamily::ClangCl => Dialect::Cl,
        }
    }

    /// Detect the family from argv0 and the argument list.
    ///
    /// `clang-cl` (including versioned/target-prefixed forms) or any argv
    /// carrying `--driver-mode=cl` is `ClangCl`. A
    /// `clang`/`clang++`/`clang-<n>` basename is `Clang`.
    /// `zigcc` wrappers (cargo-zigbuild) are also `Clang` — zig's cc is
    /// clang-based.
    /// Everything else (gcc, cc, g++, c++) is `Gnu`. Bare `cl` is NOT
    /// special-cased — real MSVC `cl.exe` stays out of scope.
    pub fn detect(program: &str, rest: &[String]) -> ToolFamily {
        let name = super::command_basename(program)
            .map(super::strip_windows_exe_suffix)
            .unwrap_or(program)
            .to_ascii_lowercase();
        if rest.iter().any(|a| a == "--driver-mode=cl") {
            return ToolFamily::ClangCl;
        }
        if name == "zigcc" || name.starts_with("zigcc-") {
            return ToolFamily::Clang;
        }
        named_tool_family(&name).unwrap_or(ToolFamily::Gnu)
    }
}

pub const CC_ID: CompilerId = CompilerId::new("cc");
pub const ADAPTER: CompilerAdapter =
    CompilerAdapter::new(CC_ID, "C-family compiler", CcCompiler::recognizes);

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
    /// True when the invocation actually asks the compiler to emit dep-info
    /// in compile mode (`-MD` / `-MMD`). Path/target modifiers alone do not
    /// create a depfile.
    pub emit: bool,
    /// `-MD` (true) or `-MMD` (false). True = include system headers
    /// in the dep-info output; false = user headers only.
    pub include_system: bool,
    /// `-MP`: add phony targets for each dependency.
    pub phony_targets: bool,
    /// `-MG`: treat missing headers as generated files in dependency output.
    pub missing_generated: bool,
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
    /// Detected compiler driver family (selects the flag dialect).
    pub family: ToolFamily,
}

/// Source file extensions the parser recognizes as C-family input.
/// Anything else gets ignored (left in `rest` for passthrough).
const SOURCE_EXTENSIONS: &[&str] = &[
    "c", "cc", "cpp", "cxx", "c++", "C", // C / C++
    "m", "mm", "M", // Objective-C / Objective-C++
    "i", "ii", // already-preprocessed
    "S", "s", "sx", // assembly
];

/// `-x` language overrides whose compilation is representable by one
/// preprocessor output and one code-generation pass. Anything outside
/// this list is refused: multi-pass languages (CUDA, HIP) compile the
/// same TU once per target with different predefined macros
/// (`__CUDA_ARCH__`), so a single `-E` output cannot soundly key them.
const LANGUAGE_OVERRIDE_ALLOWLIST: &[&str] = &[
    "c",
    "c++",
    "objective-c",
    "objective-c++",
    "assembler",
    "assembler-with-cpp",
    "cpp-output",
    "c++-cpp-output",
    "objective-c-cpp-output",
    "objective-c++-cpp-output",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CcArgValueForm {
    Flag,
    Separated,
    Concatenated { prefix: &'static str },
    CanBeSeparated { prefix: &'static str },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CcArgAction {
    SetMode(CompileMode),
    SetOutput,
    SetPic,
    SetDebugLevel(u8),
    SetOptimization(OptLevel),
    SetStd,
    DepIncludeSystem(bool),
    DepPhonyTargets,
    DepMissingGenerated,
    DepOutput,
    DepTarget,
    LanguageOverride,
    Include,
    Define,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CcArgBucket {
    Structural,
    ModeledInKey,
    ProbeKeyed,
    Preprocessor,
    RawKeyed,
    Artifact,
    NoObjectEffect,
    TooHard,
}

#[derive(Debug, Clone, Copy)]
struct CcArgSpec {
    matcher: Matcher,
    value_form: CcArgValueForm,
    action: CcArgAction,
    bucket: CcArgBucket,
    source: &'static str,
    /// Dialect this row applies to. `None` = any dialect.
    dialect: Option<Dialect>,
}

#[derive(Debug, Clone)]
struct ParsedCcArg {
    spec: &'static CcArgSpec,
    value: Option<String>,
    consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CcArgAnalysis<'a> {
    arg: &'a str,
    class: Option<FlagClass>,
    bucket: CcArgBucket,
    normalized: Vec<String>,
    refusal: Option<&'static str>,
    source: Option<&'static str>,
}

static CC_ARG_SPECS: &[CcArgSpec] = &[
    CcArgSpec {
        matcher: Matcher::Exact("-c"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetMode(CompileMode::Compile),
        bucket: CcArgBucket::Structural,
        source: "compile mode marker",
        dialect: None,
    },
    CcArgSpec {
        // MSVC `/c` slash spelling of the compile-only marker. clang-cl
        // accepts both `-c` and `/c`; without this kache misreads `/c`
        // builds as link mode and passes them through (box-confirmed).
        matcher: Matcher::Exact("/c"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetMode(CompileMode::Compile),
        bucket: CcArgBucket::Structural,
        source: "compile mode marker (cl)",
        dialect: Some(Dialect::Cl),
    },
    CcArgSpec {
        matcher: Matcher::Exact("-E"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetMode(CompileMode::Preprocess),
        bucket: CcArgBucket::Structural,
        source: "preprocess mode marker",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-S"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetMode(CompileMode::Assemble),
        bucket: CcArgBucket::Structural,
        source: "assembly mode marker",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-o"),
        value_form: CcArgValueForm::Separated,
        action: CcArgAction::SetOutput,
        bucket: CcArgBucket::Artifact,
        source: "primary output path",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-fPIC"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetPic,
        bucket: CcArgBucket::ModeledInKey,
        source: "position-independent code",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-fpic"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetPic,
        bucket: CcArgBucket::ModeledInKey,
        source: "position-independent code",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-g"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetDebugLevel(2),
        bucket: CcArgBucket::ModeledInKey,
        source: "debug-info level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-g0"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetDebugLevel(0),
        bucket: CcArgBucket::ModeledInKey,
        source: "debug-info level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-g1"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetDebugLevel(1),
        bucket: CcArgBucket::ModeledInKey,
        source: "debug-info level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-g2"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetDebugLevel(2),
        bucket: CcArgBucket::ModeledInKey,
        source: "debug-info level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-g3"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetDebugLevel(3),
        bucket: CcArgBucket::ModeledInKey,
        source: "debug-info level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-O"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::O1),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-O0"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::O0),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-O1"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::O1),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-O2"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::O2),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-O3"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::O3),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-Os"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::Os),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-Oz"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::Oz),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Exact("-Og"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::SetOptimization(OptLevel::Og),
        bucket: CcArgBucket::ModeledInKey,
        source: "optimization level",
        dialect: None,
    },
    // ── GNU dep-info rows — gnu dialect only ─────────────────────
    // In clang-cl mode, `-MD`/`-MMD` are CRT-selection flags (matching
    // MSVC `/MD`/`/MDd`), `-MT`/`-MF`/`-MQ` are also CRT/output
    // spellings, and `-MP`/`-MG` are unrelated. Tagging these rows
    // `Dialect::Gnu` makes the parser skip them entirely under clang-cl
    // so they fall through to the flag classifier / unknown-flag path.
    CcArgSpec {
        matcher: Matcher::Exact("-MD"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::DepIncludeSystem(true),
        bucket: CcArgBucket::NoObjectEffect,
        source: "dependency sidecar",
        dialect: Some(Dialect::Gnu),
    },
    CcArgSpec {
        matcher: Matcher::Exact("-MMD"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::DepIncludeSystem(false),
        bucket: CcArgBucket::NoObjectEffect,
        source: "dependency sidecar",
        dialect: Some(Dialect::Gnu),
    },
    CcArgSpec {
        matcher: Matcher::Exact("-MP"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::DepPhonyTargets,
        bucket: CcArgBucket::NoObjectEffect,
        source: "dependency sidecar phony targets",
        dialect: Some(Dialect::Gnu),
    },
    CcArgSpec {
        matcher: Matcher::Exact("-MG"),
        value_form: CcArgValueForm::Flag,
        action: CcArgAction::DepMissingGenerated,
        bucket: CcArgBucket::NoObjectEffect,
        source: "dependency sidecar generated headers",
        dialect: Some(Dialect::Gnu),
    },
    CcArgSpec {
        matcher: Matcher::Exact("-MF"),
        value_form: CcArgValueForm::Separated,
        action: CcArgAction::DepOutput,
        bucket: CcArgBucket::Artifact,
        source: "dependency output path",
        dialect: Some(Dialect::Gnu),
    },
    CcArgSpec {
        matcher: Matcher::Exact("-MT"),
        value_form: CcArgValueForm::Separated,
        action: CcArgAction::DepTarget,
        bucket: CcArgBucket::NoObjectEffect,
        source: "dependency target",
        dialect: Some(Dialect::Gnu),
    },
    CcArgSpec {
        matcher: Matcher::Exact("-MQ"),
        value_form: CcArgValueForm::Separated,
        action: CcArgAction::DepTarget,
        bucket: CcArgBucket::NoObjectEffect,
        source: "dependency target",
        dialect: Some(Dialect::Gnu),
    },
    CcArgSpec {
        matcher: Matcher::Prefix("-x"),
        value_form: CcArgValueForm::CanBeSeparated { prefix: "-x" },
        action: CcArgAction::LanguageOverride,
        bucket: CcArgBucket::ProbeKeyed,
        source: "language override",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Prefix("-I"),
        value_form: CcArgValueForm::CanBeSeparated { prefix: "-I" },
        action: CcArgAction::Include,
        bucket: CcArgBucket::Preprocessor,
        source: "include search path",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Prefix("-D"),
        value_form: CcArgValueForm::CanBeSeparated { prefix: "-D" },
        action: CcArgAction::Define,
        bucket: CcArgBucket::Preprocessor,
        source: "preprocessor define",
        dialect: None,
    },
    CcArgSpec {
        matcher: Matcher::Prefix("-std="),
        value_form: CcArgValueForm::Concatenated { prefix: "-std=" },
        action: CcArgAction::SetStd,
        bucket: CcArgBucket::ModeledInKey,
        source: "language standard",
        dialect: None,
    },
    // ── clang-cl output and standard rows (#285) ─────────────────
    // clang-cl uses `-Fo<obj>` / `/Fo<obj>` (concatenated, no space)
    // for the object output path, analogous to gnu `-o <obj>`.
    CcArgSpec {
        matcher: Matcher::Prefix("-Fo"),
        value_form: CcArgValueForm::Concatenated { prefix: "-Fo" },
        action: CcArgAction::SetOutput,
        bucket: CcArgBucket::Artifact,
        source: "clang-cl object output (#285)",
        dialect: Some(Dialect::Cl),
    },
    CcArgSpec {
        matcher: Matcher::Prefix("/Fo"),
        value_form: CcArgValueForm::Concatenated { prefix: "/Fo" },
        action: CcArgAction::SetOutput,
        bucket: CcArgBucket::Artifact,
        source: "clang-cl object output (#285)",
        dialect: Some(Dialect::Cl),
    },
    // clang-cl language standard: `-std:c++20` / `/std:c++20`.
    // The value after the prefix (e.g. `c++20`) is stored in `parsed.std`.
    CcArgSpec {
        matcher: Matcher::Prefix("-std:"),
        value_form: CcArgValueForm::Concatenated { prefix: "-std:" },
        action: CcArgAction::SetStd,
        bucket: CcArgBucket::ModeledInKey,
        source: "clang-cl language standard (#285)",
        dialect: Some(Dialect::Cl),
    },
    CcArgSpec {
        matcher: Matcher::Prefix("/std:"),
        value_form: CcArgValueForm::Concatenated { prefix: "/std:" },
        action: CcArgAction::SetStd,
        bucket: CcArgBucket::ModeledInKey,
        source: "clang-cl language standard (#285)",
        dialect: Some(Dialect::Cl),
    },
];

impl CcArgs {
    pub fn parse(args: &[String]) -> Result<Self> {
        let (program, rest) = args
            .split_first()
            .context("cc invocation missing argv[0]")?;

        let family = ToolFamily::detect(program, rest);

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
            family,
        };

        // Walk argv through a table-driven parser so spelling variants
        // like `-x c` / `-xc` and `-I dir` / `-Idir` share one rule.
        let mut depinfo: Option<DepInfoSpec> = None;
        let mut idx = 0;
        while idx < rest.len() {
            if let Some(arg) = parse_cc_arg_at(rest, idx, family.dialect()) {
                apply_cc_arg(&mut parsed, &mut depinfo, &arg);
                idx += arg.consumed;
                continue;
            }

            let arg = &rest[idx];
            if !arg.starts_with('-') && looks_like_source(arg) {
                parsed.sources.push(PathBuf::from(arg));
            }
            idx += 1;
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
    ///   unmodeled codegen flag (`-Ofast`, `-fsanitize=address`),
    ///   profiling (`-pg`), or a flag kache has
    ///   never seen — would miscache. The table is the source of truth;
    ///   anything it does not classify is refused with the offending
    ///   flags named in the reason.
    /// - **Output to stdout** (`-o -`): not a cacheable artifact.
    /// - **Assemble mode**: `-S` is still refused. `-E` to a named file
    ///   or to stdout is cached.
    pub fn refuse_reasons(&self, extra_allowlist_flags: &[String]) -> Vec<RefuseReason> {
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
                "cc link mode (whole-program caching) — not yet",
            )),
            // `-E` to a named file or to stdout is a single-output compile:
            // the expansion is the artifact. `-o -` is still refused below.
            CompileMode::Preprocess => {}
            CompileMode::Assemble => {
                reasons.push(RefuseReason::Unsupported("cc assembly mode -S — not yet"))
            }
        }

        // Output to stdout — `-o -` is unambiguous; an `-o` followed
        // by a literal `-` arg. Cacheable in principle (cache the
        // stdout bytes); not yet implemented.
        if let Some(output) = &self.output
            && output.as_os_str() == "-"
        {
            reasons.push(RefuseReason::Unsupported("cc output to stdout — not yet"));
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

        if self.requires_compiler_output_semantics() {
            reasons.push(RefuseReason::Unsupported(
                "existing output path requires compiler write semantics — caching not yet supported",
            ));
        }

        // ── Feature refusals ──
        //
        // The invocation IS a single-source object compile, but uses a
        // feature kache doesn't model yet. These are the actionable
        // refusals: adding support would convert future invocations
        // into hits.

        // Multi-pass languages. CUDA and HIP split compilation into
        // host and device passes over the same TU; `__CUDA_ARCH__`
        // differs between them, so one `-E` output cannot safely key
        // the invocation. Fail closed: only overrides known to be
        // single-pass stay cacheable.
        if let Some(language) = &self.language_override
            && !LANGUAGE_OVERRIDE_ALLOWLIST.contains(&language.as_str())
        {
            reasons.push(RefuseReason::Unsupported(
                "cc language override -x outside the single-pass C family — not yet",
            ));
        }

        // CUDA sources are not in SOURCE_EXTENSIONS, so they stay in
        // `rest`; recognizing them here avoids misreporting a
        // CUDA-shaped compile as having no source file.
        let cuda_input = self
            .rest
            .iter()
            .any(|arg| arg.ends_with(".cu") || arg.ends_with(".cuh"));
        if cuda_input {
            reasons.push(RefuseReason::Unsupported(
                "cc CUDA source input (.cu/.cuh) — not yet",
            ));
        }

        // Response files: any arg starting with `@` (typically a
        // path to a file containing additional flags). The flags
        // inside aren't visible to our parser without recursive
        // expansion + path normalization.
        if self.rest.iter().any(|a| a.starts_with('@')) {
            reasons.push(RefuseReason::Unsupported(
                "cc response file @file (expansion) — not yet",
            ));
        }

        // Multi-arch (`-arch X -arch Y` produces a fat binary).
        // Single `-arch` is fine — many cc invocations specify it.
        let arch_count = self.rest.windows(2).filter(|w| w[0] == "-arch").count();
        if arch_count > 1 {
            reasons.push(RefuseReason::Unsupported(
                "cc multi-arch -arch X -arch Y (fat-binary caching) — not yet",
            ));
        }

        // Coverage instrumentation.
        for flag in &["--coverage", "-fprofile-arcs", "-ftest-coverage"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported(
                    "cc coverage instrumentation — not yet",
                ));
                break;
            }
        }

        // Split DWARF (separate .dwo file alongside .o).
        if self.rest.iter().any(|a| a == "-gsplit-dwarf") {
            reasons.push(RefuseReason::Unsupported("cc -gsplit-dwarf — not yet"));
        }

        // Precompiled headers.
        for flag in &["-include-pch", "-emit-pch"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported(
                    "cc precompiled headers — not yet",
                ));
                break;
            }
        }
        // `*.pch` / `*.gch` as a forced-include argument also indicates PCH.
        // All three spellings of the same option have to be checked, or the
        // long forms (modeled `PreprocessorCaptured` for #580) would carry a
        // PCH past a refusal the short form catches.
        let is_pch = |p: &str| p.ends_with(".pch") || p.ends_with(".gch");
        let mut iter = self.rest.iter().peekable();
        while let Some(arg) = iter.next() {
            let pch = match arg.strip_prefix("--include=") {
                Some(value) => is_pch(value),
                None => {
                    (arg == "-include" || arg == "--include")
                        && iter.peek().is_some_and(|next| is_pch(next))
                }
            };
            if pch {
                reasons.push(RefuseReason::Unsupported(
                    "cc precompiled headers — not yet",
                ));
                break;
            }
        }

        // Modules (clang/gcc).
        for flag in &["-fmodules", "-fcxx-modules"] {
            if self.rest.iter().any(|a| a == flag) {
                reasons.push(RefuseReason::Unsupported("cc modules — not yet"));
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
        let rejected = classify_and_trace_cc_flags(self, extra_allowlist_flags);
        if !rejected.is_empty() {
            // Leak a per-invocation summary so it can ride in
            // `RefuseReason::Unsupported(&'static str)`. The wrapper
            // process handles one compile then exits, so the leak is
            // bounded and short-lived.
            let detail: &'static str = Box::leak(
                format!("cc unsupported flag(s): {} — not yet", rejected.join(" "))
                    .into_boxed_str(),
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
                "cc multi-source compile (per-source split) — not yet",
            ));
        } else if self.sources.is_empty() && !cuda_input {
            // Suppressed for CUDA inputs: the dedicated refusal above
            // already names the real cause.
            reasons.push(RefuseReason::Unsupported("cc no source file — not yet"));
        }

        reasons
    }

    /// The object file a `-c` compile produces.
    ///
    /// `-o <path>` if explicit; otherwise the compiler default — the
    /// source file's stem with a `.o` (gnu dialect) or `.obj` (cl
    /// dialect) extension, in the current working directory. Returns
    /// `None` only for degenerate invocations with no source (which
    /// `refuse_reasons` already rejects, so callers on the cache path
    /// won't hit `None`).
    pub fn object_output_path(&self) -> Option<PathBuf> {
        if let Some(o) = &self.output {
            return Some(o.clone());
        }
        let stem = self.sources.first()?.file_stem()?;
        let ext = match self.family.dialect() {
            Dialect::Cl => "obj",
            Dialect::Gnu => "o",
        };
        Some(PathBuf::from(format!("{}.{ext}", stem.to_string_lossy())))
    }

    /// The dep-info file a compile produces when `-MD` / `-MMD` is active.
    ///
    /// `-MF <path>` wins. Otherwise gcc/clang derive the depfile from the
    /// object output by replacing its extension with `.d`.
    pub fn depinfo_output_path(&self) -> Option<PathBuf> {
        let depinfo = self.depinfo.as_ref()?;
        if !depinfo.emit {
            return None;
        }
        if let Some(output) = &depinfo.output {
            return Some(output.clone());
        }
        let mut object = self.object_output_path()?;
        object.set_extension("d");
        Some(object)
    }

    /// Every filesystem output selected by a compile-mode invocation.
    ///
    /// Cacheable invocations have one source, but refused multi-source
    /// invocations still need exact passthrough safety checks for each default
    /// object and dep-info path. Non-compile modes deliberately return no
    /// object-shaped paths: `-E foo.c` does not select `foo.o`.
    pub(crate) fn compiler_output_paths(&self) -> Vec<PathBuf> {
        if self.mode != CompileMode::Compile {
            return Vec::new();
        }

        let objects = if let Some(output) = &self.output {
            vec![output.clone()]
        } else {
            let ext = match self.family.dialect() {
                Dialect::Cl => "obj",
                Dialect::Gnu => "o",
            };
            self.sources
                .iter()
                .filter_map(|source| {
                    source
                        .file_stem()
                        .map(|stem| PathBuf::from(format!("{}.{ext}", stem.to_string_lossy())))
                })
                .collect()
        };

        let mut paths = objects.clone();
        if let Some(depinfo) = &self.depinfo
            && depinfo.emit
        {
            if let Some(output) = &depinfo.output {
                paths.push(output.clone());
            } else {
                paths.extend(objects.into_iter().map(|mut object| {
                    object.set_extension("d");
                    object
                }));
            }
        }
        paths
    }

    /// Whether an existing output needs the selected compiler's own path
    /// handling instead of cache materialization.
    ///
    /// Ordinary compiler-owned outputs are private, owner-writable regular
    /// files. Kache may replace those on a hit and let the compiler overwrite
    /// them on a miss. Symlinks, hardlinks, read-only files and non-regular
    /// paths still need the selected compiler's exact pathname semantics.
    pub(crate) fn requires_compiler_output_semantics(&self) -> bool {
        self.compiler_output_paths()
            .into_iter()
            .any(|path| output_path_requires_compiler_semantics(&path))
    }

    /// Anchor used to relativize/expand C/C++ dep-info target paths.
    pub fn depinfo_anchor(&self) -> Option<PathBuf> {
        self.depinfo_output_path()?;
        let object = self.object_output_path()?;
        Some(
            object
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
        )
    }

    /// Target architecture for cache-key / metadata purposes:
    /// an explicit `-arch X` if present, else the host arch.
    pub fn cache_target_arch(&self) -> String {
        cc_target_arch(self)
    }

    /// True when this clang-cl compile requests debug info that CodeView
    /// records with un-remapped paths. Those objects stay in the local store.
    pub fn embeds_codeview_debug(&self) -> bool {
        cl_debug_present(self)
    }

    /// The subset of `rest` that identifies the *compile configuration*
    /// — per-translation-unit noise removed: source files, the `-o`
    /// output path, and (under the Gnu dialect only) dependency-file
    /// flags (`-MF`/`-MT`/`-MQ`) with their values. Under the Cl dialect
    /// those `-M*` spellings are CRT selection (codegen), so they are
    /// kept. The resolved-invocation probe (`cc -###`) is memoized on
    /// this, so every TU of a build that shares a flag set reuses one
    /// probe record instead of re-resolving per file.
    pub fn config_args(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut iter = self.rest.iter();
        // Per-TU noise to drop from the probe-memo key. GnuDialect drops
        // the dep-target flags (`-MT`/`-MF`/`-MQ`) and their values; the
        // cl dialect must NOT — there `-MT`/`-MD` are CRT-selection
        // codegen (CapturedByProbe) and stripping them from the memo key
        // would collapse distinct CRTs into one record (false hit, #285).
        let drops_value: &[&str] = match self.family.dialect() {
            Dialect::Gnu => &["-o", "-MF", "-MT", "-MQ"],
            Dialect::Cl => &["-o"],
        };
        while let Some(arg) = iter.next() {
            if drops_value.contains(&arg.as_str()) {
                iter.next(); // also drop the flag's value
            } else if self.family.dialect() == Dialect::Cl
                && (arg.starts_with("-Fo") || arg.starts_with("/Fo"))
            {
                // `-Fo<obj>` / `/Fo<obj>` — concatenated output token,
                // per-TU noise. Strip it from the probe-memo key so the
                // same config with different output paths reuses one probe
                // record. The value is embedded in the token (no next-arg
                // to consume).
            } else if self
                .sources
                .iter()
                .any(|s| s.to_str() == Some(arg.as_str()))
            {
                // source file — per-TU
            } else {
                out.push(arg.clone());
            }
        }
        out
    }
}

pub(crate) fn output_path_requires_compiler_semantics(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(meta) => !meta.file_type().is_file() || !regular_output_is_replaceable(path, &meta),
        Err(err) => err.kind() != std::io::ErrorKind::NotFound,
    }
}

fn regular_output_is_replaceable(path: &Path, meta: &std::fs::Metadata) -> bool {
    regular_output_is_independent(path, meta) && regular_output_is_owner_writable(meta)
}

fn regular_output_is_owner_writable(meta: &std::fs::Metadata) -> bool {
    if meta.permissions().readonly() {
        return false;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        // SAFETY: geteuid has no arguments, pointers, or preconditions.
        let current_uid = unsafe { libc::geteuid() };
        meta.uid() == current_uid && meta.permissions().mode() & 0o200 != 0
    }

    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(unix)]
fn regular_output_is_independent(_path: &Path, meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    meta.nlink() == 1
}

#[cfg(windows)]
fn regular_output_is_independent_windows(path: &Path, _meta: &std::fs::Metadata) -> bool {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
    };

    let Ok(file) = std::fs::File::open(path) else {
        return false;
    };
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    let ok = unsafe { GetFileInformationByHandle(file.as_raw_handle() as _, &mut info) };
    ok != 0 && info.nNumberOfLinks == 1
}
#[cfg(windows)]
use self::regular_output_is_independent_windows as regular_output_is_independent;

#[cfg(not(any(unix, windows)))]
fn regular_output_is_independent_unsupported(_path: &Path, _meta: &std::fs::Metadata) -> bool {
    true
}
#[cfg(not(any(unix, windows)))]
use self::regular_output_is_independent_unsupported as regular_output_is_independent;

fn parse_cc_arg_at(args: &[String], idx: usize, dialect: Dialect) -> Option<ParsedCcArg> {
    let arg = args.get(idx)?;
    CC_ARG_SPECS
        .iter()
        .find_map(|spec| parse_cc_arg_with_spec(spec, args, idx, arg, dialect))
}

fn parse_cc_arg_with_spec(
    spec: &'static CcArgSpec,
    args: &[String],
    idx: usize,
    arg: &str,
    dialect: Dialect,
) -> Option<ParsedCcArg> {
    // Skip rows that are restricted to a different dialect (mirrors the
    // classifier's dialect filter in `flags::classify_against`).
    if let Some(d) = spec.dialect
        && d != dialect
    {
        return None;
    }
    match spec.value_form {
        CcArgValueForm::Flag => cc_arg_spec_matches(spec, arg).then_some(ParsedCcArg {
            spec,
            value: None,
            consumed: 1,
        }),
        CcArgValueForm::Separated => cc_arg_spec_matches(spec, arg).then(|| ParsedCcArg {
            spec,
            value: args.get(idx + 1).cloned(),
            consumed: if args.get(idx + 1).is_some() { 2 } else { 1 },
        }),
        CcArgValueForm::Concatenated { prefix } => {
            arg.strip_prefix(prefix).map(|value| ParsedCcArg {
                spec,
                value: Some(value.to_string()),
                consumed: 1,
            })
        }
        CcArgValueForm::CanBeSeparated { prefix } => {
            if arg == prefix {
                Some(ParsedCcArg {
                    spec,
                    value: args.get(idx + 1).cloned(),
                    consumed: if args.get(idx + 1).is_some() { 2 } else { 1 },
                })
            } else {
                arg.strip_prefix(prefix)
                    .filter(|value| !value.is_empty())
                    .map(|value| ParsedCcArg {
                        spec,
                        value: Some(value.to_string()),
                        consumed: 1,
                    })
            }
        }
    }
}

fn cc_arg_spec_matches(spec: &CcArgSpec, arg: &str) -> bool {
    match spec.matcher {
        Matcher::Exact(s) => arg == s,
        Matcher::Prefix(s) => arg.starts_with(s),
        Matcher::Regex(pat) => Regex::new(&format!("^(?:{pat})$"))
            .map(|re| re.is_match(arg))
            .unwrap_or(false),
    }
}

fn apply_cc_arg(parsed: &mut CcArgs, depinfo: &mut Option<DepInfoSpec>, arg: &ParsedCcArg) {
    match arg.spec.action {
        CcArgAction::SetMode(mode) => parsed.mode = mode,
        CcArgAction::SetOutput => {
            if let Some(value) = &arg.value {
                parsed.output = Some(PathBuf::from(value));
            }
        }
        CcArgAction::SetPic => parsed.pic = true,
        CcArgAction::SetDebugLevel(level) => parsed.debug_level = Some(level),
        CcArgAction::SetOptimization(level) => parsed.optimization = Some(level),
        CcArgAction::SetStd => {
            if let Some(value) = &arg.value {
                parsed.std = Some(value.clone());
            }
        }
        CcArgAction::DepIncludeSystem(include_system) => {
            let d = depinfo.get_or_insert_with(DepInfoSpec::default);
            d.emit = true;
            d.include_system = include_system;
        }
        CcArgAction::DepPhonyTargets => {
            let d = depinfo.get_or_insert_with(DepInfoSpec::default);
            d.phony_targets = true;
        }
        CcArgAction::DepMissingGenerated => {
            let d = depinfo.get_or_insert_with(DepInfoSpec::default);
            d.missing_generated = true;
        }
        CcArgAction::DepOutput => {
            if let Some(value) = &arg.value {
                let d = depinfo.get_or_insert_with(DepInfoSpec::default);
                d.output = Some(PathBuf::from(value));
            }
        }
        CcArgAction::DepTarget => {
            if let Some(value) = &arg.value {
                let d = depinfo.get_or_insert_with(DepInfoSpec::default);
                d.target = Some(value.clone());
            }
        }
        CcArgAction::LanguageOverride => {
            if let Some(value) = &arg.value {
                parsed.language_override = Some(value.clone());
            }
        }
        CcArgAction::Include => {
            if let Some(value) = &arg.value {
                parsed.includes.push(PathBuf::from(value));
            }
        }
        CcArgAction::Define => {
            if let Some(value) = &arg.value {
                parsed.defines.push(parse_define(value));
            }
        }
    }
}

/// Cache key schema version for C-family compiles. Bump when the key
/// composition or restored artifact semantics change in a way that
/// could collide with old entries.
///
/// v4: `.pp` dependency sidecars are restored as dep-info and C/C++
/// dep-info path rewriting uses the common source/object root. Older
/// entries may contain machine-local source paths in `.pp` blobs.
///
/// v5: dep-info blobs use an explicit kache sentinel instead of `./`
/// for stored project-root paths. The old marker collided with ordinary
/// make depfile parent paths such as `../foo.h` during restore.
///
/// v6: C/C++ object compiles now inject prefix maps for the common
/// source/build root, not just the compiler CWD, and the preprocessor
/// stdout is normalized with the same maps before hashing. Older
/// entries may embed clone-local paths in `__FILE__`, debug info, or
/// preprocessor-expanded string literals.
///
/// The cc recipe now shares [`crate::cache_key::CACHE_KEY_VERSION`] with
/// the rustc recipe (one number to bump). The `cc_key_version:` label
/// plus disjoint fields keep cc and rustc entries from ever colliding;
/// the shared number just means one bump invalidates both recipes.
// Prefix-map targets are ABSOLUTE, profiler-resolvable spellings rather than
// angle-bracket sentinels (kunobi-ninja/kache#485). Absolute targets stop a
// DWARF consumer composing a relative target onto `DW_AT_comp_dir`, and the
// `<CC_BUILD>` cwd target uses `/proc/self/cwd` on Linux so samply / gdb / perf
// launched from the build directory resolve C/C++ sources through the kernel
// with no configuration (the Bazel trick). The other roots are ancestors of —
// or unrelated to — the cwd, so they get distinct `/kache/*` roots (a debugger
// `source-map`s them, or a future `/proc/self/cwd/..` scheme resolves the
// common-root case; see #485). These strings are also folded into the cc cache
// key, so changing them is covered by the `CACHE_KEY_VERSION` bump.
const CC_ROOT_SENTINEL: &str = "/kache/cc-root";
#[cfg(target_os = "linux")]
const CC_BUILD_SENTINEL: &str = "/proc/self/cwd";
#[cfg(not(target_os = "linux"))]
const CC_BUILD_SENTINEL: &str = "/kache/cc-build";
const CC_SOURCE_SENTINEL: &str = "/kache/cc-source";
/// Stable store-side name for a C/C++ dependency artifact.
///
/// `-MF` accepts arbitrary filenames. Inferring the artifact kind from that
/// filename loses the parser's knowledge for compound or extensionless names
/// such as OpenSSL's `a_bitstr.d.tmp` (kunobi-ninja/kache#655). The restore
/// target always comes from the current parsed invocation, so the cache entry
/// can use one semantic name ending in `.d`. Besides making every future `-MF`
/// spelling restoreable, this keeps pre-fix `.d.tmp` entries untrusted: their
/// old raw name still fails the coverage gate once, is evicted, and is replaced
/// with a normalized entry without a store-wide cache-version bump.
pub(crate) const CC_DEPINFO_STORE_NAME: &str = "__kache_cc_depinfo.d";
/// Target for a user-declared `KACHE_BASE_DIR` (ccache `CCACHE_BASEDIR`
/// analog). Shares the spelling with the rustc `<BASE_DIR>` target (same
/// concept, compiler-independent) and stays distinct from the derived roots so
/// an explicit base dir can't collide with a `/kache/cc-root` subtree.
const CC_BASE_SENTINEL: &str = "/kache/base-dir";
/// Sentinel for the Apple SDK root (issue #78). The resolved `cc -###`
/// tokens embed the SDK path (`-isysroot /…/MacOSX14.2.sdk`,
/// `-internal-isystem /…/usr/include`), which differs across Xcode and
/// Command Line Tools installs and between machines. Stripping it to a
/// sentinel lets two builds with the same SDK *contents* at different
/// paths share a key — differing SDK *contents* still diverge via
/// `compiler_version` and the preprocessor expansion, so this only ever
/// merges keys that would otherwise miss, never miscaches. A distinct
/// sentinel so the SDK can't collide with a project root.
const CC_SDKROOT_SENTINEL: &str = "/kache/sdkroot";
/// A build script's `OUT_DIR` and the Cargo target directory above it. Every
/// build directory spells these differently, and a `cc`-crate compile names
/// them in `-I` for generated headers, so without the map the read-set memo
/// is private to one build directory and peers cannot coalesce.
const CC_OUT_DIR_SENTINEL: &str = "/kache/cc-out-dir";
const CC_TARGET_SENTINEL: &str = "/kache/cc-target";
/// Another crate's build-script output directory, named by crate rather
/// than by unit: `-I<target>/debug/build/libz-sys-<hash>/out/include`
/// reaches the memo as `/kache/cc-dep-out/libz-sys/include`. Cargo's
/// metadata hash follows the feature set of the whole build, so `cargo
/// test` and `cargo check` give the same dependency different hashes; with
/// the hash in the name, the two jobs could never share a memo.
const CC_DEP_OUT_DIR_SENTINEL: &str = "/kache/cc-dep-out";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CcPrefixMap {
    from: String,
    to: String,
}

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

/// Build the argv for a preprocess-only run, dialect-dependent.
///
/// **Gnu dialect** — the original args with mode/output/dep-info flags
/// stripped and `-E -P` forced:
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
///
/// **Cl dialect** — `/EP` is the MSVC equivalent (preprocess to stdout,
/// no line markers; gnu `-E -P` writes nothing to stdout under clang-cl).
/// Only compile-mode (`-c`/`-S`) and output (`-o`, `-Fo`/`/Fo`) flags are
/// stripped; the `-M*` spellings are CRT-selection codegen in this dialect
/// (they affect `_MT`/`_DLL` defines), so they are KEPT in the expansion.
fn build_preprocess_args(parsed: &CcArgs) -> Vec<String> {
    match parsed.family.dialect() {
        Dialect::Gnu => {
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
        Dialect::Cl => {
            // `/EP` = preprocess to stdout, no line markers (MSVC
            // equivalent of gnu `-E -P`). Drop compile-mode + output
            // flags; keep preprocessor-affecting flags so the hash
            // reflects them.
            let mut out = vec!["/EP".to_string()];
            let mut iter = parsed.rest.iter();
            while let Some(arg) = iter.next() {
                match arg.as_str() {
                    "-c" | "-S" => {}
                    "-o" => {
                        iter.next();
                    }
                    // Attached output form (`-Fofoo.obj` / `/Fofoo.obj`).
                    // clang-cl build systems use the attached form
                    // exclusively; a space-separated `/Fo obj` would leave
                    // a stray token, but such an invocation refuses before
                    // this point (output flags are unmodeled until Layer 2).
                    _ if arg.starts_with("-Fo") || arg.starts_with("/Fo") => {}
                    _ => out.push(arg.clone()),
                }
            }
            out
        }
    }
}

/// Hash the preprocessor expansion of the translation unit.
///
/// Runs `<cc> -E -P …` (gnu) or `<cc> /EP …` (clang-cl) — see
/// [`build_preprocess_args`] — with `SOURCE_DATE_EPOCH` pinned so the
/// `__DATE__` / `__TIME__` macros expand deterministically (without
/// this the hash would change every second → ~0% hit rate; gcc, clang,
/// and clang-cl all honor it). The expansion includes every `#include`d
/// header transitively, so any header change invalidates the key
/// automatically — no separate dependency tracking needed. Bails (→
/// passthrough) if the preprocessor yields empty stdout.
const CC_PREPROCESS_MEMO_TARGET: &str = "__kache_preprocess_memo";

fn add_preprocess_dep_capture(
    parsed: &CcArgs,
    pp_args: Vec<String>,
    dep_path: &Path,
) -> Vec<String> {
    let path = dep_path
        .to_str()
        .expect("dependency capture is enabled only for UTF-8 temp paths")
        .to_string();
    let extra = match parsed.family.dialect() {
        Dialect::Gnu => vec![
            "-MD".to_string(),
            "-MF".to_string(),
            path,
            "-MT".to_string(),
            CC_PREPROCESS_MEMO_TARGET.to_string(),
        ],
        Dialect::Cl => vec![
            "-Xclang".to_string(),
            "-dependency-file".to_string(),
            "-Xclang".to_string(),
            path,
            "-Xclang".to_string(),
            "-MT".to_string(),
            "-Xclang".to_string(),
            CC_PREPROCESS_MEMO_TARGET.to_string(),
            // clang-cl excludes system headers from dependency output unless
            // this cc1 option is present. A partial header set is never safe
            // for direct-mode reuse.
            "-Xclang".to_string(),
            "-sys-header-deps".to_string(),
        ],
    };
    compose_cc_args(&pp_args, extra)
}

/// Parse the Make dependency rule emitted into the probe's private file.
/// Continuations, escaped spaces/hash/backslashes, and Make's `$$` spelling
/// are decoded. The target may remain user-selected under clang-cl when its
/// earlier forwarded `-MT` wins; only the private output path is authoritative.
/// Any unfamiliar/malformed shape disables memoization.
fn parse_preprocess_dependencies(raw: &str, cwd: &Path) -> Result<Vec<PathBuf>> {
    let bytes = raw.as_bytes();
    let mut logical = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' && bytes.get(index + 1) == Some(&b'\n') {
            logical.push(b' ');
            index = index.saturating_add(2);
        } else if bytes[index] == b'\\'
            && bytes.get(index + 1) == Some(&b'\r')
            && bytes.get(index + 2) == Some(&b'\n')
        {
            logical.push(b' ');
            index = index.saturating_add(3);
        } else {
            logical.push(bytes[index]);
            index = index.saturating_add(1);
        }
    }
    let logical = std::str::from_utf8(&logical).context("cc dependency file is not UTF-8")?;
    let (rule, separator) = logical
        .lines()
        .find_map(|line| {
            line.find(": ")
                .or_else(|| line.find(":\t"))
                .map(|separator| (line, separator))
        })
        .context("cc dependency file has no Make rule")?;
    let dependencies = &rule[separator + 1..];

    let mut paths = Vec::new();
    let mut word = String::new();
    let mut chars = dependencies.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\\' => match chars.peek().copied() {
                Some(next) if matches!(next, ' ' | '\t' | '#' | '\\' | ':') => {
                    word.push(next);
                    chars.next();
                }
                Some(_) => word.push('\\'),
                None => anyhow::bail!("cc dependency rule ends in an escape"),
            },
            '$' if chars.peek() == Some(&'$') => {
                word.push('$');
                chars.next();
            }
            '#' => break,
            whitespace if whitespace.is_whitespace() => {
                if !word.is_empty() {
                    paths.push(PathBuf::from(std::mem::take(&mut word)));
                }
            }
            other => word.push(other),
        }
    }
    if !word.is_empty() {
        paths.push(PathBuf::from(word));
    }
    if paths.is_empty() {
        anyhow::bail!("cc dependency rule contains no inputs");
    }
    for path in &mut paths {
        let text = path.to_string_lossy();
        let bytes = text.as_bytes();
        let windows_absolute = bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\');
        if path.is_relative() && !windows_absolute && !text.starts_with("\\\\") {
            *path = cwd.join(&*path);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

#[derive(Debug)]
struct PreprocessHash {
    hash: String,
    fingerprints: Option<Vec<crate::cache_key::CcPreprocessMemoInput>>,
    /// The expansion spells out a checkout root, so its object does too and
    /// the key is bound to this checkout (see [`hash_cc_expansion`]).
    path_bound: bool,
}

/// The `-E` key probe's argv: the preprocess args plus the same
/// `-ffile-prefix-map` rules the real compile gets.
///
/// With the maps applied, `__FILE__` expands to its sentinel exactly as it
/// will in the object. A checkout root still spelled out in the expansion is
/// then text the compiler copies into the object verbatim, such as a `-D`
/// value or a string in a generated header (kunobi-ninja/kache#1004).
fn key_preprocess_args(parsed: &CcArgs, prefix_maps: &[CcPrefixMap]) -> Vec<String> {
    compose_cc_args(
        &build_preprocess_args(parsed),
        file_prefix_map_args(prefix_maps),
    )
}

#[derive(Debug, PartialEq, Eq)]
struct CcExpansionHash {
    hash: String,
    path_bound: bool,
}

/// Hash a preprocessor expansion for the cc key.
///
/// The probe ran with the compile's prefix maps (see [`key_preprocess_args`]),
/// so any root left in the expansion is one `-ffile-prefix-map` does not
/// rewrite, and the object will carry it. Mapping it to a sentinel would give
/// every checkout the same key for objects that differ. Such an expansion is
/// hashed as it is instead: the key hits in this checkout and misses in every
/// other one. A portable expansion hashes its mapped form, as before.
///
/// A bound hash also folds every mapped root, not only the ones the expansion
/// spells out. `execute` skips the object scan for a bound key, so a root that
/// reaches the object some other way must still separate two checkouts even
/// when the expansion only names a root they share, such as a base dir.
fn hash_cc_expansion(raw: Vec<u8>, prefix_maps: &[CcPrefixMap]) -> CcExpansionHash {
    let mapped = apply_cc_prefix_maps_to_bytes(raw.clone(), prefix_maps);
    if mapped == raw {
        return CcExpansionHash {
            hash: blake3::hash(&mapped).to_hex().to_string(),
            path_bound: false,
        };
    }
    let mut roots: Vec<&str> = prefix_maps
        .iter()
        .map(|map| map.from.as_str())
        .filter(|from| !from.is_empty())
        .collect();
    roots.sort_unstable();
    roots.dedup();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache.cc.path-bound-expansion.v1\0");
    for root in roots {
        hasher.update(root.as_bytes());
        hasher.update(b"\0");
    }
    hasher.update(b"\n");
    hasher.update(&raw);
    CcExpansionHash {
        hash: hasher.finalize().to_hex().to_string(),
        path_bound: true,
    }
}

/// Assembler directives that read another file at assembly time.
const CC_ASSEMBLER_FILE_DIRECTIVES: [&str; 2] = [".incbin", ".include"];

/// Assembler facilities that substitute into their bodies, and so can build a
/// file directive whose name never appears in the expansion (`.\op "\file"`
/// invoked as `emit incbin, payload.bin`, or `.inc\()bin` inside `.rept`).
const CC_ASSEMBLER_MACRO_DIRECTIVES: [&str; 4] = [".macro", ".irp", ".irpc", ".rept"];

/// Why the assembler may read a file the key cannot see, if it may.
///
/// `.incbin` and assembler `.include` run after preprocessing, so no depfile
/// lists what they read: a changed file would be served from the old object
/// (kunobi-ninja/kache#1015). They reach the expansion from `.S` and `.s`
/// sources and from inline `asm` strings in C, where the compiler joins
/// adjacent literals and decodes escapes only after preprocessing. So the
/// expansion is checked as written and as the compiler reads its strings, and
/// an `asm` whose text is computed rather than written is refused outright.
fn cc_assembler_hidden_input(expansion: &[u8]) -> Option<&'static str> {
    if let Some(found) = cc_assembler_text_hidden_input(expansion) {
        return Some(found);
    }
    let literals = cc_string_literals(expansion);
    if literals.computed_asm {
        return Some("asm((...))");
    }
    if literals.named_escape {
        return Some(r"\N{...}");
    }
    cc_assembler_text_hidden_input(&literals.text)
}

/// The first construct in assembler text that may read an unseen file.
///
/// Directives are matched ignoring case, as the assembler does, and must end
/// at a word boundary. A file directive counts when the next non-blank byte
/// opens its operand: a quote, or a backslash for an escaped quote in a C
/// string or a macro argument. That keeps C++ member access such as
/// `opts.include(` out. A macro facility counts when a blank follows it;
/// `.altmacro` and the `\()` separator count anywhere.
fn cc_assembler_text_hidden_input(text: &[u8]) -> Option<&'static str> {
    let file = CC_ASSEMBLER_FILE_DIRECTIVES.into_iter().find(|directive| {
        cc_directive_operands(text, directive.as_bytes()).any(|rest| {
            rest.iter()
                .find(|byte| !matches!(byte, b' ' | b'\t'))
                .is_some_and(|byte| matches!(byte, b'"' | b'\\'))
        })
    });
    file.or_else(|| {
        CC_ASSEMBLER_MACRO_DIRECTIVES.into_iter().find(|directive| {
            cc_directive_operands(text, directive.as_bytes()).any(|rest| {
                rest.first()
                    .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
            })
        })
    })
    .or_else(|| {
        cc_directive_operands(text, b".altmacro")
            .next()
            .map(|_| ".altmacro")
    })
    .or_else(|| {
        text.windows(3)
            .any(|window| window == br"\()")
            .then_some(r"\()")
    })
}

/// String literals in a C or C++ expansion, read the way the compiler reads
/// them after preprocessing.
#[derive(Debug, Default, PartialEq, Eq)]
struct CcStringLiterals {
    /// Each run of adjacent literals joined, escapes decoded, one run per line.
    text: Vec<u8>,
    /// An `asm` whose text is an expression rather than a string literal.
    computed_asm: bool,
    /// A C++23 named escape (`\N{LATIN SMALL LETTER B}`). It can spell any
    /// character, and decoding it would need the Unicode name table.
    named_escape: bool,
}

/// Literal prefixes that open a raw string (`R"delim(...)delim"`).
const CC_RAW_STRING_PREFIXES: [&[u8]; 5] = [b"R", b"u8R", b"uR", b"UR", b"LR"];

/// Encoding prefixes of ordinary string and character literals.
const CC_LITERAL_PREFIXES: [&[u8]; 4] = [b"u8", b"u", b"U", b"L"];

/// Read the string literals in `src`, skipping character literals and numbers
/// so a quote inside them cannot throw the reading out of step. Raw strings
/// are taken as written. Assembly sources are not C; for them the plain text
/// check is the one that counts, and a misreading here can only refuse more.
fn cc_string_literals(src: &[u8]) -> CcStringLiterals {
    let mut literals = CcStringLiterals::default();
    let mut joining = false;
    let mut i = 0;
    while let Some(&byte) = src.get(i) {
        let next = if byte.is_ascii_whitespace() {
            i + 1
        } else if byte == b'/' && matches!(src.get(i + 1), Some(b'*' | b'/')) {
            // Comments only survive a `-C` probe. The compiler reads one as a
            // blank, so literals on either side still join.
            cc_skip_comment(src, i)
        } else {
            cc_read_token(src, i, &mut literals, &mut joining)
        };
        debug_assert!(next > i, "string literal reader must advance");
        i = next;
    }
    literals
}

/// Read the token starting at `i`, which is not a blank or a comment, and
/// return the index after it. String literals are decoded into `literals`;
/// `joining` tracks whether the previous token was one.
fn cc_read_token(
    src: &[u8],
    i: usize,
    literals: &mut CcStringLiterals,
    joining: &mut bool,
) -> usize {
    let word_end = cc_word_end(src, i);
    let word = &src[i..word_end];
    let quote = src.get(word_end).copied();
    let plain = word.is_empty() || CC_LITERAL_PREFIXES.contains(&word);
    let raw_paren = (quote == Some(b'"') && CC_RAW_STRING_PREFIXES.contains(&word))
        .then(|| cc_raw_delimiter(src, word_end))
        .flatten();
    if raw_paren.is_some() || (quote == Some(b'"') && plain) {
        if !*joining {
            literals.text.push(b'\n');
        }
        *joining = true;
        return match raw_paren {
            Some(paren) => cc_read_raw_string(src, word_end, paren, &mut literals.text),
            None => cc_read_string(src, word_end, literals),
        };
    }
    *joining = false;
    if quote == Some(b'\'') && plain {
        return cc_skip_char_literal(src, word_end);
    }
    if word.is_empty() {
        return i + 1;
    }
    if matches!(word, b"asm" | b"__asm" | b"__asm__") && !cc_asm_text_is_literal(src, word_end) {
        literals.computed_asm = true;
    }
    word_end
}

/// End of the identifier or number starting at `start`, or `start` if neither
/// starts there. A number takes C++14 digit separators, whose quote would
/// otherwise read as a character literal.
fn cc_word_end(src: &[u8], start: usize) -> usize {
    let is_word = |byte: &u8| byte.is_ascii_alphanumeric() || *byte == b'_';
    if !src.get(start).is_some_and(is_word) {
        return start;
    }
    let number = src[start].is_ascii_digit();
    let mut i = start;
    while let Some(byte) = src.get(i) {
        let separator = number && *byte == b'\'' && src.get(i + 1).is_some_and(is_word);
        if !is_word(byte) && !separator {
            break;
        }
        let next = i + 1;
        debug_assert!(next > i, "word reader must advance");
        i = next;
    }
    i
}

/// Decode the string literal whose opening quote is at `open` into
/// `literals.text`, and return the index after it. A literal cut off by a
/// newline ends there.
fn cc_read_string(src: &[u8], open: usize, literals: &mut CcStringLiterals) -> usize {
    let mut i = open + 1;
    while let Some(&byte) = src.get(i) {
        let next = match byte {
            b'"' | b'\n' => return i + 1,
            b'\\' => cc_decode_escape(src, i + 1, literals),
            _ => {
                literals.text.push(byte);
                i + 1
            }
        };
        debug_assert!(next > i, "string reader must advance");
        i = next;
    }
    i
}

/// Decode the escape whose letter is at `at` into `literals.text`, returning
/// the index after it. Control escapes other than newline and tab become a
/// blank: the directive check only needs to know that they separate words. A
/// named escape is flagged rather than decoded.
fn cc_decode_escape(src: &[u8], at: usize, literals: &mut CcStringLiterals) -> usize {
    let out = &mut literals.text;
    let digits = |from: usize, max: usize, radix: u32| {
        let len = src[from..]
            .iter()
            .take(max)
            .take_while(|byte| char::from(**byte).is_digit(radix))
            .count();
        let value = src[from..from + len].iter().fold(0u32, |value, byte| {
            value
                .wrapping_mul(radix)
                .wrapping_add(char::from(*byte).to_digit(radix).unwrap_or(0))
        });
        (value, from + len)
    };
    let push_code_point = |out: &mut Vec<u8>, value: u32| {
        let mut utf8 = [0; 4];
        let decoded = char::from_u32(value).unwrap_or(' ');
        out.extend_from_slice(decoded.encode_utf8(&mut utf8).as_bytes());
    };
    let Some(&kind) = src.get(at) else {
        return at;
    };
    // C23 and C++23 delimited escapes: `\x{2e}`, `\o{56}`, `\u{62}`. One
    // without its `}` does not compile, so its object is never stored.
    if matches!(kind, b'x' | b'o' | b'u') && src.get(at + 1) == Some(&b'{') {
        let (value, end) = digits(at + 2, usize::MAX, if kind == b'o' { 8 } else { 16 });
        if kind == b'u' {
            push_code_point(out, value);
        } else {
            out.push(value as u8);
        }
        return end + usize::from(src.get(end) == Some(&b'}'));
    }
    match kind {
        b'x' => {
            let (value, end) = digits(at + 1, usize::MAX, 16);
            out.push(value as u8);
            end
        }
        b'0'..=b'7' => {
            let (value, end) = digits(at, 3, 8);
            out.push(value as u8);
            end
        }
        b'u' | b'U' => {
            let (value, end) = digits(at + 1, if kind == b'u' { 4 } else { 8 }, 16);
            push_code_point(out, value);
            end
        }
        b'n' => {
            out.push(b'\n');
            at + 1
        }
        b't' => {
            out.push(b'\t');
            at + 1
        }
        b'r' | b'f' | b'v' | b'a' | b'b' => {
            out.push(b' ');
            at + 1
        }
        b'N' if src.get(at + 1) == Some(&b'{') => {
            literals.named_escape = true;
            let close = src[at..]
                .iter()
                .position(|byte| matches!(byte, b'}' | b'"' | b'\n'));
            close.map_or(src.len(), |offset| {
                at + offset + usize::from(src[at + offset] == b'}')
            })
        }
        other => {
            out.push(other);
            at + 1
        }
    }
}

/// The offset of the `(` that ends a raw string's delimiter, if the quote at
/// `open` starts a valid one: at most 16 characters, none of them a blank,
/// backslash, parenthesis or quote. Otherwise the prefix is an identifier and
/// the quote opens an ordinary string, as the compiler would read it.
fn cc_raw_delimiter(src: &[u8], open: usize) -> Option<usize> {
    let body = src.get(open + 1..)?;
    let paren = body.iter().take(17).position(|byte| *byte == b'(')?;
    body[..paren]
        .iter()
        .all(|byte| !byte.is_ascii_whitespace() && !matches!(byte, b'\\' | b')' | b'"'))
        .then_some(paren)
}

/// Copy the raw string whose opening quote is at `open` and whose delimiter
/// ends at `paren` into `out`, returning the index after it. Its text is
/// taken as written, escapes included.
fn cc_read_raw_string(src: &[u8], open: usize, paren: usize, out: &mut Vec<u8>) -> usize {
    let body = &src[open + 1..];
    let mut close = vec![b')'];
    close.extend_from_slice(&body[..paren]);
    close.push(b'"');
    let text = &body[paren + 1..];
    let len = text
        .windows(close.len())
        .position(|window| window == close.as_slice())
        .unwrap_or(text.len());
    out.extend_from_slice(&text[..len]);
    open + 1 + paren + 1 + len + close.len()
}

/// The index after the character literal whose opening quote is at `open`.
fn cc_skip_char_literal(src: &[u8], open: usize) -> usize {
    let mut i = open + 1;
    while let Some(&byte) = src.get(i) {
        let next = match byte {
            b'\\' => i + 2,
            b'\'' | b'\n' => return i + 1,
            _ => i + 1,
        };
        debug_assert!(next > i, "character literal reader must advance");
        i = next;
    }
    i
}

/// The index after the `/* */` or `//` comment starting at `start`.
fn cc_skip_comment(src: &[u8], start: usize) -> usize {
    let close: &[u8] = if src.get(start + 1) == Some(&b'*') {
        b"*/"
    } else {
        b"\n"
    };
    let rest = start + 2;
    src[rest.min(src.len())..]
        .windows(close.len())
        .position(|window| window == close)
        .map_or(src.len(), |offset| rest + offset + close.len())
}

/// Whether the `asm` keyword ending at `after` takes a written string. Clang
/// accepts a constant expression in its place, which can build any text, so
/// anything else counts as computed. A word not followed by `(` is not an
/// `asm` statement.
fn cc_asm_text_is_literal(src: &[u8], after: usize) -> bool {
    let skip_blanks = |mut i: usize| loop {
        let next = if src.get(i).is_some_and(u8::is_ascii_whitespace) {
            i + 1
        } else if src.get(i) == Some(&b'/') && matches!(src.get(i + 1), Some(b'*' | b'/')) {
            cc_skip_comment(src, i)
        } else {
            return i;
        };
        debug_assert!(next > i, "asm reader must advance");
        i = next;
    };
    let mut i = skip_blanks(after);
    loop {
        let end = cc_word_end(src, i);
        match &src[i..end] {
            b"volatile" | b"__volatile__" | b"__volatile" | b"inline" | b"__inline__" | b"goto" => {
                i = skip_blanks(end)
            }
            b"" if src.get(i) == Some(&b'(') => break,
            _ => return true,
        }
    }
    let start = skip_blanks(i + 1);
    let end = cc_word_end(src, start);
    let word = &src[start..end];
    src.get(end) == Some(&b'"')
        && (word.is_empty()
            || CC_LITERAL_PREFIXES.contains(&word)
            || CC_RAW_STRING_PREFIXES.contains(&word))
}

/// The text after each whole-word, case-insensitive occurrence of `name`.
fn cc_directive_operands<'a>(
    expansion: &'a [u8],
    name: &'a [u8],
) -> impl Iterator<Item = &'a [u8]> + 'a {
    expansion
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'.')
        .filter_map(move |(start, _)| {
            let candidate = expansion.get(start..start + name.len())?;
            if !candidate.eq_ignore_ascii_case(name) {
                return None;
            }
            let rest = &expansion[start + name.len()..];
            let continues_word = rest
                .first()
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_');
            (!continues_word).then_some(rest)
        })
}

/// Whether `bytes` contain the raw spelling of any mapped root.
///
/// A plain substring search, unlike [`apply_cc_prefix_maps_to_bytes`]: object
/// files separate strings with NUL bytes, which the configured-root token rule
/// does not treat as a boundary, and a missed root here is a false hit.
fn bytes_embed_mapped_root(bytes: &[u8], prefix_maps: &[CcPrefixMap]) -> bool {
    prefix_maps
        .iter()
        .map(|map| map.from.as_bytes())
        .filter(|from| !from.is_empty())
        .any(|from| {
            bytes
                .iter()
                .enumerate()
                .filter(|(_, byte)| **byte == from[0])
                .any(|(start, _)| bytes[start..].starts_with(from))
        })
}

/// Whether the object at `path` embeds a raw mapped root.
fn cc_object_embeds_mapped_root(path: &Path, prefix_maps: &[CcPrefixMap]) -> std::io::Result<bool> {
    std::fs::read(path).map(|bytes| bytes_embed_mapped_root(&bytes, prefix_maps))
}

/// Why a successful compile's outputs must not be stored, if they must not.
///
/// Only a key bound to this checkout may hold an object that names it.
/// Anything the scan cannot vouch for is not stored either: an object that
/// cannot be read, or outputs with no object to read (`object_embeds_root`
/// returns `None`). With no artifacts there is nothing to store or scan.
fn cc_unsafe_to_store(
    no_artifacts: bool,
    key_path_bound: bool,
    object_embeds_root: impl FnOnce() -> Option<std::io::Result<bool>>,
) -> Option<String> {
    if no_artifacts || key_path_bound {
        return None;
    }
    match object_embeds_root() {
        None => Some("has no object to check for checkout roots".to_string()),
        Some(Ok(false)) => None,
        Some(Ok(true)) => {
            Some("embeds a checkout root the prefix maps did not rewrite".to_string())
        }
        Some(Err(error)) => Some(format!(
            "could not be read to check for checkout roots: {error}"
        )),
    }
}

/// The key cannot see a file the assembler would read (kunobi-ninja/kache#1015).
///
/// Its own type so the wrapper can tell it from other key failures: this TU
/// is unsafe for any cache keyed on the preprocessor output, so it must run
/// the compiler directly rather than through a configured fallback wrapper.
#[derive(Debug)]
pub(crate) struct CcHiddenInput {
    pub(crate) construct: &'static str,
}

impl std::fmt::Display for CcHiddenInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The fixed text leads so one `known_passthrough` prefix in the bench
        // scenarios covers every construct.
        write!(
            f,
            "cc: the assembler may read a file the cache key cannot see (`{}`)",
            self.construct
        )
    }
}

impl std::error::Error for CcHiddenInput {}

fn preprocess_hash(
    parsed: &CcArgs,
    prefix_maps: &[CcPrefixMap],
    file_hasher: &crate::cache_key::FileHasher<'_>,
    capture_dependencies: bool,
) -> Result<PreprocessHash> {
    let pp_args = key_preprocess_args(parsed, prefix_maps);
    let dep_temp = if capture_dependencies {
        match tempfile::Builder::new()
            .prefix("kache-cc-preprocess-")
            .tempdir()
        {
            Ok(temp) => Some(temp),
            Err(error) => {
                tracing::debug!("cc preprocess dependency tempdir unavailable: {error}");
                None
            }
        }
    } else {
        None
    };
    let dep_path = dep_temp
        .as_ref()
        .map(|dir| dir.path().join("inputs.d"))
        .filter(|path| path.to_str().is_some());
    let pp_args = dep_path.as_deref().map_or(pp_args.clone(), |path| {
        add_preprocess_dep_capture(parsed, pp_args.clone(), path)
    });
    let run = |args: &[String]| {
        crate::opcounts::record_preprocessor_run();
        let mut command = Command::new(&parsed.program);
        command.args(args);
        // If `program` is a kache shim, this probe must not re-enter the
        // cache (it would try to key another -E to stdout). The wrapper
        // passthroughs when this is set.
        command.env("KACHE_CC_KEY_PROBE", "1");
        // Pin the build timestamp so `__DATE__` / `__TIME__` expand
        // deterministically. The real compile uses the same value.
        if let Some(epoch) = effective_source_date_epoch() {
            command.env("SOURCE_DATE_EPOCH", epoch);
        }
        command
            .output()
            .with_context(|| format!("running preprocessor `{}`", parsed.program))
    };
    let mut output = run(&pp_args)?;
    if !output.status.success() && dep_path.is_some() {
        // An otherwise supported compiler may not implement the private
        // dependency flags. Preserve the existing cache-key behavior and just
        // leave this invocation unmemoized.
        tracing::debug!("cc preprocess dependency capture failed; retrying without memo capture");
        output = run(&key_preprocess_args(parsed, prefix_maps))?;
    }
    if !output.status.success() {
        // Preprocess failed — the real compile would also fail.
        // Bail so the wrapper falls back to passthrough, which runs
        // the real compiler and surfaces the real diagnostic. Worded as the
        // `uncacheable` passthrough detail, not a failure: often a configure
        // probe that is *meant* to fail.
        anyhow::bail!(
            "cc -E key probe exited {}",
            output
                .status
                .code()
                .map_or_else(|| "by signal".to_string(), |c| c.to_string())
        );
    }
    if output.stdout.is_empty() {
        // Zero preprocessor output: a mis-detected family ran the wrong
        // preprocess flags (gnu `-E -P` under clang-cl writes to a file),
        // or a degenerate empty TU. Either way refuse rather than hash
        // nothing → passthrough.
        anyhow::bail!("cc -E key probe produced no output");
    }
    if let Some(construct) = cc_assembler_hidden_input(&output.stdout) {
        // Refuse before the memo can record this TU: a memo hit skips the
        // probe, and the file the assembler reads is in no fingerprint.
        return Err(CcHiddenInput { construct }.into());
    }
    let CcExpansionHash { hash, path_bound } = hash_cc_expansion(output.stdout, prefix_maps);
    // A path-bound expansion is never memoized. The memo is read from other
    // checkouts through mapped names, and a hit skips the probe that would
    // have noticed the root.
    // Fingerprinted for a path-bound expansion too: the memo carries the
    // binding, and the key folds the reading checkout's roots.
    let fingerprints = dep_path.as_deref().and_then(|path| {
        let dependencies = std::fs::read_to_string(path)
            .context("reading cc preprocess dependency file")
            .and_then(|raw| {
                let cwd = std::env::current_dir().context("reading cc compiler directory")?;
                parse_preprocess_dependencies(&raw, &cwd)
            });
        match dependencies {
            Ok(paths) => {
                // Record each input under its prefix-mapped spelling, the form
                // the expansion above was hashed in. That is what lets another
                // checkout read the memo: the path a second worktree resolves
                // an input to differs, the mapped name does not.
                let mapped: Vec<(String, PathBuf)> = paths
                    .into_iter()
                    .map(|path| (cc_mapped_path(&path, prefix_maps), path))
                    .collect();
                file_hasher.cc_preprocess_fingerprints(
                    &mapped,
                    &cc_prefix_maps_key(prefix_maps),
                    &|path| cc_mapped_content_hash(path, prefix_maps),
                )
            }
            Err(error) => {
                tracing::debug!("cc preprocess dependency capture unavailable: {error:#}");
                None
            }
        }
    });
    Ok(PreprocessHash {
        hash,
        fingerprints,
        path_bound,
    })
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
/// `ParserHandled` = the parser routes it to a structural field used
/// for refusal / execution flow rather than object-content keying.
/// `CapturedByProbe` = `cc -###` resolves it into `-cc1` tokens
/// the cache key already hashes. `RawKeyed` = the argument is folded
/// directly into the cache key. `PreprocessorCaptured` = the
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
        dialect: None,
    },
    FlagSpec {
        // `-g` family: bare or with a level digit (`-g0`..`-g3`). The
        // parser extracts the level into `debug_level`. Variants like
        // `-gdwarf-5` / `-ggdb` / `-gline-tables-only` change debug
        // info but aren't modeled, so they're not on this row.
        matcher: Matcher::Regex(r"-g[0-3]?"),
        class: FlagClass::ModeledInKey,
        source: "PR #94 — debug level. Regex captures `-g`/`-g0..3`; -gdwarf-* etc. refuse.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fPIC"),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fpic"),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-std="),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        // Single `-arch <value>`. The parser sets `cache_target_arch`
        // from the resolved arch; multi-`-arch X -arch Y` is refused
        // separately in the procedural pass of `refuse_reasons`.
        matcher: Matcher::Exact("-arch"),
        class: FlagClass::ModeledInKey,
        source: "PR #94",
        dialect: None,
    },
    // ── ParserHandled: parser routes to structural invocation state ──
    FlagSpec {
        matcher: Matcher::Exact("-c"),
        class: FlagClass::ParserHandled,
        source: "PR #94 — compile mode marker parsed into CompileMode.",
        dialect: None,
    },
    FlagSpec {
        // End-of-options marker. Does not change object bytes; Firefox's
        // Windows clang-cl nightly still refused it as an unclassified flag.
        matcher: Matcher::Exact("--"),
        class: FlagClass::NoObjectEffect,
        source: "Firefox Windows 0.20 nightly — end-of-options marker.",
        dialect: None,
    },
    FlagSpec {
        // MSVC `/c` slash spelling of the compile-mode marker (cl only).
        // The parser already routes `/c` to CompileMode::Compile via
        // CC_ARG_SPECS; this row tells the unsupported-flag classifier the
        // token is known (ParserHandled) so it isn't rejected. Without it,
        // a `/c` clang-cl compile is refused as `unsupported flag(s): /c`
        // (box-confirmed). Mirrors the `-c` row above. (#312)
        matcher: Matcher::Exact("/c"),
        class: FlagClass::ParserHandled,
        source: "Issue #312 — MSVC /c compile-mode marker, cl dialect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-E"),
        class: FlagClass::ParserHandled,
        source: "Flag audit — preprocessor mode marker parsed into CompileMode.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-S"),
        class: FlagClass::ParserHandled,
        source: "Flag audit — assembly mode marker parsed into CompileMode.",
        dialect: None,
    },
    FlagSpec {
        // `--driver-mode=<mode>` selects the driver dialect clang speaks —
        // `cl` turns a plain `clang` into clang-cl (a real cross-/Linux way
        // to get MSVC-driver behavior, and how kache's own Linux e2e drives
        // the clang-cl path). The token is consumed structurally by
        // `ToolFamily::detect`; it carries no object effect of its own, and
        // the dialect's actual codegen/preprocessor consequences are already
        // keyed (the `-E`/`/EP` preprocessor hash sees the MSVC predefined
        // macros, the `-###` probe sees the resolved cc1 line). Without this
        // row every `clang --driver-mode=cl` compile refused on the token
        // itself. Prefix-matched so `=gcc`/`=g++`/`=cpp` are covered too.
        matcher: Matcher::Prefix("--driver-mode="),
        class: FlagClass::ParserHandled,
        source: "Issue #411 — driver-mode selector; consumed by ToolFamily::detect, effects keyed via preprocessor + -### probe.",
        dialect: None,
    },
    // ── CapturedByProbe: `cc -###` resolved tokens differentiate ──
    //
    // Each row's effect on the resulting object is captured by the
    // resolved `cc -###` `-cc1` token stream that the cache key
    // already hashes (see `cache_key`'s `resolved:` tokens). Identical
    // user-facing flags → identical resolved tokens → same key;
    // different values → different tokens → different key. Safety holds
    // only when the probe resolves on the host compiler. If it does
    // not, `cache_key` refuses probe-keyed flags before preprocessing
    // so these rows cannot silently under-key.
    //
    // Initial population sourced from the Firefox/Gecko Darwin
    // baseline (kunobi-ninja/kache#114): ~4,476 single-source compiles
    // per Firefox build that previously passed through unnecessarily.
    FlagSpec {
        matcher: Matcher::Prefix("-mmacosx-version-min="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — Darwin deployment target.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-fstrict-flex-arrays="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — strict-flex-arrays codegen knob.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-ffp-contract="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — fp-contract codegen knob.",
        dialect: None,
    },
    FlagSpec {
        // Clang's automatic-variable hardening mode changes generated code and
        // is forwarded verbatim to `-cc1`, so the resolved-token hash safely
        // distinguishes it from the default mode. Keep the row exact on the
        // Firefox-reported spelling; adjacent modes remain fail-closed until
        // there is workload evidence for them.
        matcher: Matcher::Exact("-ftrivial-auto-var-init=pattern"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #849 — Firefox automatic-variable pattern initialization; Apple clang forwards the value to -cc1 verbatim (verified -###).",
        dialect: None,
    },
    // ── Codegen knobs: one sorted stem list, BOTH polarities ──
    //
    // `-f(?:no-)?<stem>` matches `-f<stem>` AND `-fno-<stem>` for every stem
    // below. Each is a codegen knob clang/gcc forward to `-cc1`, so the
    // resolved-token hash captures whichever polarity is passed — both are
    // equally safe (CapturedByProbe; the probe resolves the actual codegen).
    // Listing the STEM once instead of each `-f`/`-fno-` spelling structurally
    // prevents the "modeled one polarity, missed the other" passthrough class
    // that recurred across the nightly benches: `-ftrapping-math` (#422) and
    // `-fomit-frame-pointer` (#426) each slipped through because only the
    // opposite polarity was listed. Add a knob = add one sorted stem; both
    // polarities are then covered, and a stray knob on every TU can't silently
    // void the cache (the #411 class).
    //
    // The matcher is anchored `^(?:…)$` by `cc_arg_spec_matches`, so this never
    // partial-matches a longer flag. Math stems: `-ffast-math` defines
    // `__FAST_MATH__`/`__FINITE_MATH_ONLY__` (seen by the `-E` hash), but its
    // optimizer assumptions (reassociation, no-inf/no-nan, FP contraction) are
    // invisible to `-E` — only the `-cc1` stream captures them, so these are
    // `CapturedByProbe`, not `PreprocessorCaptured`. Verified against
    // `clang -###`. Stems are kept ALPHABETICAL for maintenance.
    FlagSpec {
        matcher: Matcher::Regex(concat!(
            r"-f(?:no-)?(?:",
            "associative-math|asynchronous-unwind-tables|cxx-exceptions|data-sections|",
            "fast-math|finite-math-only|freestanding|function-sections|math-errno|",
            "merge-all-constants|omit-frame-pointer|reciprocal-math|rounding-math|",
            "semantic-interposition|signaling-nans|signed-zeros|strict-aliasing|",
            "trapping-math|unsafe-math-optimizations|unroll-loops|unwind-tables|wrapv",
            ")",
        )),
        class: FlagClass::CapturedByProbe,
        source: "#114/#245/#418/#422/#426/#580/#856 + 0.20 nightly — codegen knobs, both polarities, resolved into -cc1 tokens. One sorted stem per knob covers -f<stem> AND -fno-<stem> (prevents the missed-polarity passthrough class). unroll-loops / asynchronous-unwind-tables from lance/Firefox passthroughs. cxx-exceptions / freestanding from Firefox Windows nightly.",
        dialect: None,
    },
    FlagSpec {
        // `-mrecip=<value>` (x86 reciprocal-estimate codegen). Prefix-matched so
        // `=none`/`=all`/`=default,...` are all covered; the value rides through
        // to `-cc1` so different settings key differently.
        matcher: Matcher::Prefix("-mrecip="),
        class: FlagClass::CapturedByProbe,
        source: "Firefox nightly bench — reciprocal-estimate codegen selector; value forwarded to -cc1 (verified clang -###).",
        dialect: None,
    },
    FlagSpec {
        // `-mno-omit-leaf-frame-pointer` (clang): forces frame pointers for
        // leaf functions too. Current cc-rs emits it when Rust requests
        // forced frame pointers, and in debug-mode tool setup alongside
        // `-fno-omit-frame-pointer` — so it lands on every aws-lc-sys TU of
        // a macOS debug build (#839). Real codegen effect: clang's `-###`
        // resolves it to `-mframe-pointer=all` against the default
        // `-mframe-pointer=non-leaf`, which the resolved-token hash
        // separates. gcc spells the equivalent knob `-momit-leaf-frame-
        // pointer` (opposite default, same `-m` shape); a driver whose
        // `-###` doesn't resolve keeps refusing fail-closed.
        matcher: Matcher::Exact("-mno-omit-leaf-frame-pointer"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #839 — cc-rs forced-frame-pointer flag (aws-lc-sys debug builds); clang -### resolves to -mframe-pointer=all.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-pthread"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — pthread feature switch (also visible via _REENTRANT in preprocessor).",
        dialect: None,
    },
    FlagSpec {
        // Firefox nightly still passed `-fno-stack-protector` through because
        // only `-fstack-protector-strong` was listed. Cover the family
        // (bare / -strong / -all, both polarities); clang forwards the
        // resulting -stack-protector* token to -cc1.
        matcher: Matcher::Regex(r"-f(?:no-)?stack-protector(?:-strong|-all)?"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 + Firefox 0.20 nightly — stack-protector family, both polarities.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fstack-clash-protection"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #245 — stack-clash-protection codegen hardening (Firefox).",
        dialect: None,
    },
    // `--param <name>=<value>` / `--param=<name>=<value>` — backend tuning
    // knobs (`ssp-buffer-size`, `max-inline-insns-auto`, …). Real codegen
    // effect under gcc, which forwards the pair verbatim to cc1, so the
    // resolved-token hash separates one value from another. Classifying the
    // flag `CapturedByProbe` also *forces* the probe
    // (`cc_flags_need_resolved_invocation`), and the key bails when it can't
    // resolve — the separated value token is inert on its own, so without
    // that forcing it would under-key. clang accepts the option and drops it
    // before cc1: no token, and no object difference either, so collapsing
    // those keys is correct rather than lossy.
    //
    // aws-lc-sys passes `--param ssp-buffer-size=4` on its 6 jitterentropy
    // TUs (#580). Those live in the same archive as the `--include=` TUs
    // above, so both rows are needed for the `.a` to converge cross-clone.
    FlagSpec {
        matcher: Matcher::Exact("--param"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #580 — aws-lc-sys jitterentropy `--param ssp-buffer-size=4`; gcc forwards the pair to cc1, clang drops it.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("--param="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #580 — joined spelling of --param; value rides through to cc1.",
        dialect: None,
    },
    // `-fsanitize-undefined-strip-path-components=-1` (clang-only). aws-lc-sys
    // 0.44 adds it to the same jitterentropy TUs as the `--param` rows above
    // once a compile probe confirms support (`builder/cc_builder.rs`): Clang's
    // UBSan check metadata at -O0 can embed the full source path, and the
    // option strips leading path components from it. Path metadata rather
    // than codegen, but it still changes object bytes, so it must be keyed —
    // clang forwards the option, value included, verbatim into the resolved
    // `-cc1` line, and the resolved-token hash captures it. Exact-matched on
    // the one observed value: every integer value clang accepts rides through
    // to cc1 (verified `=0`/`=2` on Apple clang 21), so widening is safe when
    // a workload needs it, but `=-1` is the only spelling in evidence and the
    // exact row keeps unobserved values refusing — the `-gdwarf-4` precedent.
    FlagSpec {
        matcher: Matcher::Exact("-fsanitize-undefined-strip-path-components=-1"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #840 — aws-lc-sys 0.44 jitterentropy UBSan path-stripping; Apple clang forwards the value to -cc1 verbatim (verified -###).",
        dialect: None,
    },
    // (math-errno, strict-aliasing, omit-frame-pointer, unwind-tables —
    // #114/#426 — are now covered by the sorted codegen-knob stem list above,
    // both polarities.)
    // `-ffile-reproducible` / `-fno-file-reproducible` (clang). Firefox's
    // Windows build passes `-ffile-reproducible` (it also `-Werror`-probes
    // for it — see `clang_cl_invocation_injects_no_flags_issue_299`). The
    // flag controls how clang renders embedded paths (`__FILE__`, debug
    // info) for reproducibility; clang forwards it to `-cc1`, so its full
    // effect is captured by the resolved-token hash. `CapturedByProbe`,
    // not `NoObjectEffect`: it can change object bytes (the `__FILE__`
    // path separator), so it must be keyed. Dialect-agnostic — the flag is
    // gnu-spelled and accepted by both the default driver and clang-cl;
    // under a driver whose `-###` doesn't resolve, the probe contract
    // refuses rather than under-keys. (Issue #411 — Firefox/Windows.)
    FlagSpec {
        matcher: Matcher::Exact("-ffile-reproducible"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #411 — clang reproducible embedded paths; keyed via -### resolved tokens.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-file-reproducible"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #411 — clang reproducible embedded paths (negation); keyed via -### resolved tokens.",
        dialect: None,
    },
    // Firefox debug-info & clang argument-wrapper flags
    // (kunobi-ninja/kache#117). The debug-info flags affect DWARF
    // sections of the object; clang's `-###` expands them into
    // `-cc1 -dwarf-version=4` / `-dwarf-linkage-names=Abstract` / etc.,
    // so the resolved-tokens hash differentiates them per-value.
    //
    // The DWARF version digit is *not* wildcarded on purpose:
    // `-gdwarf-5` produces a different (larger, newer-toolchain-
    // dependent) object and isn't part of any observed evidence. Each
    // version is listed exactly as workloads surface it; the probe's
    // resolved `-dwarf-version=N` token keeps the keys distinct.
    FlagSpec {
        matcher: Matcher::Exact("-gdwarf-4"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #117 — DWARF v4 emission (Firefox baseline).",
        dialect: None,
    },
    // cc-rs selects `-gdwarf-2` for every Apple target when debug info is
    // enabled, so every Rust workspace with native deps hits this on macOS
    // debug/test profiles (aws-lc-sys was the sampled root).
    FlagSpec {
        matcher: Matcher::Exact("-gdwarf-2"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #838 — DWARF v2 emission; cc-rs default on Apple targets.",
        dialect: None,
    },
    // ring's Darwin build.rs requires full debug information so Apple's
    // dead strip can see which symbols are used. Apple clang's `-###`
    // resolves `-gfull` to `-debug-info-kind=standalone -dwarf-version=5`,
    // the same cc1 tokens as ordinary `-g` on that compiler; a no-debug
    // invocation has neither. Exact, not a `-g*` prefix: neighbouring
    // Apple spellings such as `-gused` stay refused until a workload
    // needs them. (Issue #857)
    FlagSpec {
        matcher: Matcher::Exact("-gfull"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #857 — Darwin full debug info; ring 0.17 dead-strip contract. Apple clang -### resolves it to standalone DWARF; keyed via those tokens.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-gsimple-template-names"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #117 — clang template-name compression in debug info.",
        dialect: None,
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
        dialect: None,
    },
    // Compiler path-remapping flags: `-ffile-prefix-map` (= `-fdebug-prefix-map`
    // + `-fmacro-prefix-map`). Build systems pass these to make the OBJECT
    // path-portable — e.g. Firefox's `--enable-path-remapping` emits
    // `-fdebug-prefix-map=<objdir>=/topobjdir/`, a `<srcdir>` map, and an SDK
    // map. Each is `<flag>=<from>=<to>`. Clang's `-###` captures them in the
    // resolved invocation, and kache normalizes every resolved token through
    // its own cc prefix maps before hashing — so a per-checkout `<from>`
    // (the objdir/srcdir) collapses to a sentinel (two clones → one key),
    // while a genuinely different `<to>`, or an unrelated `<from>` like the
    // SDK path (identical across clones), still differentiates correctly.
    //
    // Without these rows the entire compile refused ("unsupported flag(s):
    // -fdebug-prefix-map=…"), so a build enabling its OWN path remapping
    // silently disabled all cc caching (kunobi-ninja/kache: Firefox bench saw
    // 4090+ TUs pass through uncached). `CapturedByProbe`, not
    // `CapturedByPreprocessor`: `-fdebug-prefix-map` only rewrites debug-info
    // paths in the object (not the preprocessed text), so the preprocessor
    // hash would under-key it — the resolved `-###` token stream is what
    // captures the flag's full effect.
    FlagSpec {
        matcher: Matcher::Prefix("-ffile-prefix-map="),
        class: FlagClass::CapturedByProbe,
        source: "Build-system path remapping (e.g. Firefox --enable-path-remapping). Resolved-token hash captures it; per-checkout `from` normalized via cc prefix maps.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-fdebug-prefix-map="),
        class: FlagClass::CapturedByProbe,
        source: "Build-system debug-info path remapping. Resolved-token hash captures it; per-checkout `from` normalized via cc prefix maps.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-fmacro-prefix-map="),
        class: FlagClass::CapturedByProbe,
        source: "Build-system __FILE__ path remapping. Resolved-token hash captures it; per-checkout `from` normalized via cc prefix maps.",
        dialect: None,
    },
    FlagSpec {
        // `aws-lc-sys` passes this spelling for assembler sources because
        // `-fdebug-prefix-map` does not reach GNU as. GCC's `-###` output puts
        // it only on the separate `as` subprocess, while kache's probe hashes
        // the `cc1`/`cc1plus` subprocess. Key it directly rather than claiming
        // it is probe-captured. The anchored regex admits exactly one `-Wa`
        // sub-option: GCC splits commas into additional assembler arguments,
        // which must remain unsupported until modeled separately.
        matcher: Matcher::Regex(r"-Wa,--debug-prefix-map=[^,=]+=[^,]*"),
        class: FlagClass::RawKeyed,
        source: "Issue #644 — GNU assembler debug path remapping; raw-keyed because the cc1 probe omits the separate assembler subprocess.",
        dialect: Some(Dialect::Gnu),
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
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-exceptions"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ exception mode (off).",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fexceptions"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ exception mode (on).",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-rtti"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ RTTI mode (off).",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-frtti"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ RTTI mode (on).",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-sized-deallocation"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ sized-deallocation (disabled).",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-faligned-new"),
        class: FlagClass::CapturedByProbe,
        source: "RocksDB C++17 aligned new/delete (enabled); resolved cc1 tokens distinguish this from the default and -fno-aligned-new.",
        dialect: Some(Dialect::Gnu),
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-aligned-new"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #116 — C++ aligned new/delete (disabled).",
        dialect: None,
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
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fvisibility-inlines-hidden"),
        class: FlagClass::CapturedByProbe,
        source: "Firefox bench evidence (post-#146) — inline-function visibility default = hidden.",
        dialect: None,
    },
    // (`-f[no-]semantic-interposition` — the SINGLE flag that degraded the first
    // LLVM CMake/Ninja bench to a 0% hit rate, on every Release TU — is now in
    // the sorted codegen-knob stem list above, both polarities.)
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
        dialect: None,
    },
    FlagSpec {
        // Separate-arg form: `-target <triple>`. The value classifies
        // as a positional (no leading `-`), so this row only needs to
        // accept the flag itself.
        matcher: Matcher::Exact("-target"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — cross-compilation target triple (separate-arg form).",
        dialect: None,
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
        dialect: None,
    },
    // ── Cross-target ABI / ISA-state selectors (issue #823) ──
    //
    // The Rust `cc` crate injects these on its own for cross targets:
    // `-mabi=` (riscv), `-mfloat-abi=` + `-mfpu=` (arm), `-mthumb` (thumbv*)
    // — so refusing any one of them makes every cross-compiled C TU on that
    // architecture uncacheable, not just hand-written invocations.
    //
    // Each is `RawKeyed`, deliberately NOT `CapturedByProbe` like their
    // `-march=` neighbour. Every value in these sets is a closed enumeration
    // with no host-relative member (there is no `-mabi=native` the way
    // `-mtune=`/`-mcpu=` have one), so the flag text alone determines the
    // object — the exact property a verbatim fold keys. Clang does lower
    // them into the resolved cc1 line (verified: `-### --target=
    // riscv64-unknown-linux-gnu -mabi=lp64d` yields `"-target-abi" "lp64d"`
    // against `"lp64"` for `-mabi=lp64`), so the probe would key them on
    // clang — but probe-captured soundness would rest on every supported
    // driver spelling the effect into that line, and a driver that omitted
    // it would serve one ABI's object to the other: a broken binary, not a
    // missed hit. The verbatim fold cannot do that.
    FlagSpec {
        // Target ABI: `lp64d`/`lp64`/`ilp32` on riscv, `ms`/`sysv` on
        // x86-64, `aapcs-linux` on arm.
        matcher: Matcher::Prefix("-mabi="),
        class: FlagClass::RawKeyed,
        source: "Issue #823 — target ABI selection; closed value set, keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        // Float ABI: `soft` / `softfp` / `hard`. cc-rs injects
        // `-mfloat-abi=hard` for every *eabihf target.
        matcher: Matcher::Prefix("-mfloat-abi="),
        class: FlagClass::RawKeyed,
        source: "Issue #823 — arm float ABI; closed value set (soft/softfp/hard), keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        // Code model: `tiny`/`small`/`kernel`/`medium`/`large` (x86,
        // aarch64), `medlow`/`medany` (riscv). Common in riscv firmware
        // and kernel builds.
        matcher: Matcher::Prefix("-mcmodel="),
        class: FlagClass::RawKeyed,
        source: "Issue #823 — code model; closed value set, keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        // Concrete arm FPU names — the standardized set gcc and clang
        // both document. cc-rs injects `vfpv3-d16` / `vfp` / `neon`.
        //
        // `-mfpu=auto` is DELIBERATELY not matched and keeps refusing: it
        // resolves from `-march`/`-mcpu` (and the toolchain's configured
        // default when those are absent) inside cc1, so its text does not
        // determine the object, and gcc's driver passes the literal `auto`
        // to cc1 — the probe cannot capture the resolution either.
        matcher: Matcher::Regex(
            r"-mfpu=(?:none|vfp|vfpv2|vfpv3(?:-fp16|-d16(?:-fp16)?|xd(?:-fp16)?)?|vfpv4(?:-d16)?|fpv4-sp-d16|fpv5-(?:sp-)?d16|fp-armv8(?:-fullfp16)?|neon(?:-fp16|-vfpv3|-vfpv4|-fp-armv8)?|crypto-neon-fp-armv8)",
        ),
        class: FlagClass::RawKeyed,
        source: "Issue #823 — concrete arm FPU selection; enumerated so `-mfpu=auto` (resolved inside cc1, not text-deterministic) still refuses.",
        dialect: None,
    },
    FlagSpec {
        // Instruction-set state: Thumb vs ARM encoding, both polarities —
        // conflicting occurrences are last-one-wins, which the raw fold
        // preserves because it hashes in argv order with duplicates kept.
        // Valueless and text-deterministic; cc-rs injects `-mthumb` for
        // thumbv* targets.
        matcher: Matcher::Regex(r"-m(?:(?:no-)?thumb|arm)"),
        class: FlagClass::RawKeyed,
        source: "Issue #823 — arm/thumb instruction-set state, keyed verbatim in argv order.",
        dialect: None,
    },
    FlagSpec {
        // x87 vs SSE math. Closed set; no `native`/`auto`.
        matcher: Matcher::Regex(r"-mfpmath=(?:sse|387|both)"),
        class: FlagClass::RawKeyed,
        source: "Issue #826 — x87/SSE fp math; enumerated so unknown values still refuse.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Regex(r"-mtls-dialect=(?:gnu2?|trad|desc)"),
        class: FlagClass::RawKeyed,
        source: "Issue #826 — TLS dialect; closed value set, keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        // aarch64 pointer-auth / BTI / GCS. Combinations gcc documents;
        // `native`/`auto` are not in this set and keep refusing.
        matcher: Matcher::Regex(
            r"-mbranch-protection=(?:none|standard|pac-ret(?:\+leaf)?(?:\+b-key)?(?:\+bti)?|bti|gcs)",
        ),
        class: FlagClass::RawKeyed,
        source: "Issue #826 — aarch64 branch protection; enumerated closed set.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-mgeneral-regs-only"),
        class: FlagClass::RawKeyed,
        source: "Issue #826 — restrict to general registers; valueless, keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Regex(r"-mstack-protector-guard=(?:tls|global|sysreg)"),
        class: FlagClass::RawKeyed,
        source: "Issue #826 — stack-protector guard location; enumerated so host-relative values refuse.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-mstack-protector-guard-offset="),
        class: FlagClass::RawKeyed,
        source: "Issue #826 — stack-protector guard offset; numeric, keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-mstack-protector-guard-reg="),
        class: FlagClass::RawKeyed,
        source: "Issue #826 — stack-protector guard register name; keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-msimd128"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — WASM SIMD128 enable.",
        dialect: None,
    },
    FlagSpec {
        // x86 width / SIMD / ISA feature flags seen in Firefox passthroughs
        // (#375, extended). These materially change the object by selecting the
        // target width or enabled ISA features. The resolved `cc -###` stream
        // records the resulting target triple / `-target-feature` set, so the
        // cache key differentiates each value (and a `-mno-` form flips it).
        // The nightly Firefox bench passed media/codec TUs through on
        // `-mavx -mbmi2 -mf16c`; the set below covers the x86 codec ISA family
        // (libvpx/dav1d/aom). Enumerated explicitly (with optional `no-`)
        // rather than opening `-m*`, so the remaining value-taking knobs
        // (`-mtune=`, `-mcpu=`, `-mcmodel=`) and unmodeled `-m` flags still
        // refuse. `-mabi=` is modeled separately above.
        matcher: Matcher::Regex(
            r"^-m(?:no-)?(?:32|64|mmx|sse|sse2|sse3|ssse3|sse4|sse4\.1|sse4\.2|sse4a|avx|avx2|avxvnni|avx512[a-z0-9]+|fma|fma4|f16c|bmi|bmi2|abm|popcnt|lzcnt|aes|vaes|pclmul|vpclmulqdq|gfni|sha|movbe|rdrnd|rdseed|adx|fsgsbase|xsave|xsaveopt|xsavec|xsaves|prfchw|clflushopt|clwb|cldemote|fxsr)$",
        ),
        class: FlagClass::CapturedByProbe,
        source: "Issue #375 (extended, Firefox nightly bench) — x86 width + SIMD/ISA codec feature flags; resolved into target-cpu/target-feature tokens.",
        dialect: None,
    },
    // (`-f[no-]function-sections` / `-f[no-]data-sections` — #115 — are now in
    // the sorted codegen-knob stem list above, both polarities.)
    FlagSpec {
        // `-Wa,*` passes through to the assembler. Different `-Wa,*`
        // values do arbitrary assembler things — listed as `Exact` for
        // the specific Firefox value (per #115's evidence) so a wildcard
        // `Prefix("-Wa,")` doesn't silently accept unmodeled assembler
        // flags. `--noexecstack` sets a section flag on the object.
        matcher: Matcher::Exact("-Wa,--noexecstack"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — assembler: non-executable stack section flag. Listed by exact value rather than `-Wa,*` wildcard so unmodeled assembler flags still refuse.",
        dialect: None,
    },
    FlagSpec {
        // Separate-arg form: `-x <lang>`. Value is positional. The
        // probe resolves the language mode into the `-cc1` invocation.
        matcher: Matcher::Exact("-x"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — language override (separate-arg form).",
        dialect: None,
    },
    FlagSpec {
        // Sticky language override forms. The parser records the
        // language for invocation shape, and the probe resolves the
        // language mode into the `-cc1` invocation. One regex row
        // covers the sticky forms while `-x <lang>` stays an exact
        // row because its language value is a separate argv token.
        matcher: Matcher::Regex(r"-x(?:c|c\+\+|objective-c|objective-c\+\+)"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 / flag audit — sticky language override forms.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fobjc-exceptions"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — Objective-C exception model.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fobjc-arc"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — Objective-C ARC mode.",
        dialect: None,
    },
    // ── PreprocessorCaptured: cc -E -P expansion hash subsumes effect ──
    FlagSpec {
        matcher: Matcher::Prefix("-D"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-U"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-I"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("--sysroot"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-include"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    // `--include=<file>` / `--include <file>` — the long spellings of
    // `-include`. Same forced-include semantics, so the header's bytes land
    // in the `-E` expansion the key already hashes. `Prefix("--include=")`
    // (not a bare `--include` prefix) so the neighbouring `--include-*`
    // options (`--include-directory=`, `--include-with-prefix=`, …) keep
    // refusing until they are modeled on their own terms.
    //
    // aws-lc-sys drives its BoringSSL symbol-prefixing through the `=` form
    // (`builder/cc_builder.rs`: `--include=` for non-cl, `/FI` for cl), so
    // before this row 56 of its TUs passed through uncached, the `.a`
    // diverged per checkout, and the `extern:` content hash re-keyed the
    // whole rustls/TLS subtree above it (#580).
    FlagSpec {
        matcher: Matcher::Prefix("--include="),
        class: FlagClass::PreprocessorCaptured,
        source: "Issue #580 — aws-lc-sys boringssl_prefix_symbols forced include; `=` form of -include.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("--include"),
        class: FlagClass::PreprocessorCaptured,
        source: "Issue #580 — separated form of --include=<file>.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-imacros"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-isystem"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-iquote"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-idirafter"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-isysroot"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-nostdinc"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-nostdinc++"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-undef"),
        class: FlagClass::PreprocessorCaptured,
        source: "PR #94",
        dialect: None,
    },
    // ── NoObjectEffect: diagnostics / dep-info / build mechanics ──
    //
    // Outcome gates come FIRST: classification is first-match, so the
    // `-Werror` / `-pedantic-errors` rows below must precede the general
    // diagnostics rows they would otherwise be shadowed by.
    FlagSpec {
        // `-Werror[=spec]` and `-Wno-error[=spec]`: escalate (or de-escalate)
        // warnings into/out of hard errors. They cannot change the object
        // bytes of a *successful* compile, but they change whether the
        // compile succeeds at all — and a hit replays success. Two
        // invocations differing only here must not share a key: an entry
        // stored under `-Wno-error=foo` must not serve green to a `-Werror`
        // build that `foo` should have failed (review finding #2). Both
        // spellings are RawKeyed so any combination keys distinctly.
        //
        // The tail is deliberately open-ended: every spelling that starts
        // `-Werror` / `-Wno-error` is about turning diagnostics into errors,
        // including the dashed legacy aliases GCC and clang still accept
        // (`-Werror-implicit-function-declaration`). Keying one of these
        // unnecessarily would only cost a hit; missing one serves a green
        // hit to a build that should have failed, so the row errs wide.
        matcher: Matcher::Regex(r"-W(no-)?error.*"),
        class: FlagClass::RawKeyed,
        source: "review #2 — outcome gate: -Werror/-Wno-error change success vs failure; keyed verbatim.",
        dialect: None,
    },
    FlagSpec {
        // `-pedantic-errors` is the error-flavored member of the -pedantic
        // family: unlike plain `-pedantic` (diagnostics-only) it makes
        // non-conforming code fail to compile. Keyed for the same reason as
        // `-Werror` above; the plain `-pedantic` prefix row further down
        // keeps its NoObjectEffect treatment.
        matcher: Matcher::Exact("-pedantic-errors"),
        class: FlagClass::RawKeyed,
        source: "review #2 — outcome gate: -pedantic-errors changes success vs failure.",
        dialect: None,
    },
    FlagSpec {
        // `-W*` warnings — the remaining, genuinely diagnostics-only
        // warning flags (`-Werror`/`-Wno-error` are keyed above). The
        // regex EXCLUDES `-Wl,*` / `-Wa,*` / `-Wp,*` (linker /
        // assembler / preprocessor passthrough forms that change the
        // resulting object); they need separate handling and aren't
        // covered here.
        matcher: Matcher::Regex(r"-W[^,]*"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94 — warnings. Regex excludes `-Wl,*`/`-Wa,*`/`-Wp,*` passthrough forms.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-w"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-pedantic"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Prefix("-fdiagnostics-"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fcolor-diagnostics"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-color-diagnostics"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        // Forces ANSI color escapes in diagnostics regardless of TTY
        // detection — terminal coloring only, no object effect. A real
        // clang driver flag in both dialects (Firefox's Windows clang-cl
        // build passes it bare, not just `-Xclang` forwarded as #411
        // covered), with no clang-cl spelling collision. Same family as
        // `-fcolor-diagnostics` above. Re-added after #430's codegen-knob
        // refactor dropped it (issue #438; originally #425/#424).
        matcher: Matcher::Exact("-fansi-escape-codes"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #424/#438 — Firefox/Windows bare diagnostics flag; ANSI color escapes only, no object effect.",
        dialect: None,
    },
    FlagSpec {
        // Dep-info generation: -MD, -MMD, -MF, -MT, -MQ, -MP, -MG.
        // All write the `.d` sidecar; none affect the object. Regex
        // captures the family; alternatives are equally tight in this
        // table layout but the row stays declarative this way.
        matcher: Matcher::Regex(r"-MM?D|-M[FTQPG]"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94 — gcc dep-info flags. Gnu-dialect ONLY: in cl mode -MD/-MT/-MTd/-MDd are CRT selection (codegen), -MP is multi-process; they must not classify as inert dep-info (issue #285).",
        dialect: Some(Dialect::Gnu),
    },
    FlagSpec {
        matcher: Matcher::Exact("-o"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-P"),
        class: FlagClass::NoObjectEffect,
        source: "Flag audit — preprocessor line-marker suppression has no compile-mode object effect.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-pipe"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        // `-fno-lto` on a `-c` compile is a no-op: LTO is off by default, so the
        // object is native code either way — `clang -###` resolves IDENTICAL
        // `-cc1` tokens with and without it (verified). Firefox passes it
        // defensively per-TU and the nightly bench passed those TUs through.
        // `NoObjectEffect` (drop from key) is correct and honest here. The
        // positive forms — `-flto` / `-flto=thin` / `-ffat-lto-objects` — DO
        // change the output (LLVM bitcode) and stay unmodeled/refused, so the
        // dangerous `-flto -fno-lto` combination still passes through (the
        // unmodeled `-flto` refuses) rather than silently sharing this key.
        matcher: Matcher::Exact("-fno-lto"),
        class: FlagClass::NoObjectEffect,
        source: "Firefox nightly bench — explicit non-LTO `-c` compile; identical -cc1 tokens vs absent (verified clang -###). -flto/-flto=* stay refused.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-v"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("--verbose"),
        class: FlagClass::NoObjectEffect,
        source: "PR #94",
        dialect: None,
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
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("--end-no-unused-arguments"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #117 — clang unused-argument warning region (close).",
        dialect: None,
    },
    // ── clang-cl flag classification (#285) ──────────────────────
    //
    // All rows below carry `dialect: Some(Dialect::Cl)` — they apply
    // exclusively to clang-cl invocations. Gnu/clang dialect behaviour
    // is unchanged.

    // NoObjectEffect — output path and conformance flags that do not
    // affect the resulting object bytes.
    FlagSpec {
        // `-Fo<obj>` / `/Fo<obj>` — object output path. Classified here
        // so the classifier gate doesn't refuse it; the parser extracts
        // it into `CcArgs.output` (Artifact bucket).
        matcher: Matcher::Prefix("-Fo"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #285 — clang-cl object output path, no object-content effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/Fo"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #285 — clang-cl object output path, no object-content effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        // `-Zc:inline` / `/Zc:inline` — clang-cl ignores this flag
        // entirely in cc1; confirmed via -### that it generates no cc1
        // token and produces no object-content difference.
        matcher: Matcher::Exact("-Zc:inline"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #285 — clang-cl ignores -Zc:inline (no cc1 token, no object effect).",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Zc:inline"),
        class: FlagClass::NoObjectEffect,
        source: "Issue #285 — clang-cl ignores /Zc:inline (no cc1 token, no object effect).",
        dialect: Some(Dialect::Cl),
    },
    // ModeledInKey — language standard is extracted by the parser into
    // `CcArgs.std` and folded directly into the cache key.
    FlagSpec {
        matcher: Matcher::Prefix("-std:"),
        class: FlagClass::ModeledInKey,
        source: "Issue #285 — clang-cl language standard (-std:c++NN); modeled in key via CcArgs.std.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/std:"),
        class: FlagClass::ModeledInKey,
        source: "Issue #285 — clang-cl language standard (/std:c++NN); modeled in key via CcArgs.std.",
        dialect: Some(Dialect::Cl),
    },
    // PreprocessorCaptured — forced-include headers enter the /EP
    // preprocessor hash, so their content is already in the key.
    FlagSpec {
        matcher: Matcher::Prefix("-FI"),
        class: FlagClass::PreprocessorCaptured,
        source: "Issue #285 — clang-cl forced include; content captured by /EP preprocessor hash.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/FI"),
        class: FlagClass::PreprocessorCaptured,
        source: "Issue #285 — clang-cl forced include; content captured by /EP preprocessor hash.",
        dialect: Some(Dialect::Cl),
    },
    // CapturedByProbe — keyed via the clang-cl -### resolved-token
    // stream. Different values produce different cc1 tokens → different
    // keys. Safe only when the probe resolves; `cc_flags_need_resolved_invocation`
    // ensures the key refuses if the probe is unavailable.
    FlagSpec {
        // `-guard:cf` / `-guard:cf,nochecks` / `/guard:cf` etc. The
        // `Prefix` wildcard is intentional and safe here BECAUSE this is
        // CapturedByProbe: clang-cl -### resolves each guard variant into
        // a distinct -cc1 token (e.g. `-cfguard`) so the key differentiates
        // per-value automatically. An unrecognized guard variant still
        // produces a distinct -### token → distinct key (no miscache risk).
        matcher: Matcher::Prefix("-guard:"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl Control Flow Guard. Prefix wildcard safe: -### resolves each variant to a distinct -cc1 token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/guard:"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl Control Flow Guard (/guard: spelling). Prefix wildcard safe: -### resolves each variant to a distinct -cc1 token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        // `-fms-compatibility-version=<ver>` — MSVC version emulation.
        // `Prefix` wildcard is safe here BECAUSE this is CapturedByProbe:
        // clang-cl -### reflects the exact version number into a -cc1
        // token, so different versions produce different keys.
        matcher: Matcher::Prefix("-fms-compatibility-version="),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — MSVC compatibility version. Prefix wildcard safe: -### reflects the exact version into a -cc1 token.",
        dialect: Some(Dialect::Cl),
    },
    // Function/global inlining and frame-pointer optimizations.
    FlagSpec {
        matcher: Matcher::Exact("-Gy"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl function-level linking (COMDAT). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Gy"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl function-level linking (COMDAT). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-Gw"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl global data optimization (COMDAT). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Gw"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl global data optimization (COMDAT). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-Oy"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl omit frame pointer (MSVC spelling of -fomit-frame-pointer). Keyed via -### resolved tokens. -Oy- was modeled in #285; Firefox Windows uses the enable polarity.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Oy"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl omit frame pointer (slash spelling). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-Oy-"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl frame-pointer omission disabled. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-Zl"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl omit default library name from the object (.drectve). Firefox Windows nightly. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Zl"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl omit default library name from the object (.drectve). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("-fp:"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl floating-point model (-fp:fast/precise/strict). Firefox Windows nightly. Prefix safe: -### reflects the model into cc1.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/fp:"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl floating-point model (/fp:fast/precise/strict). Prefix safe: -### reflects the model into cc1.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/clang:"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl /clang:<flag> forwards a clang driver flag. Firefox Windows uses /clang:-fno-finite-math-only. Prefix safe: -### captures the forwarded token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("-clang:"),
        class: FlagClass::CapturedByProbe,
        source: "clang-cl -clang:<flag> dash spelling of /clang:. Prefix safe: -### captures the forwarded token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/WX"),
        class: FlagClass::RawKeyed,
        source: "clang-cl warnings-as-errors (/WX). Outcome gate, same contract as -Werror.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-WX"),
        class: FlagClass::RawKeyed,
        source: "clang-cl warnings-as-errors (-WX). Outcome gate, same contract as -Werror.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Regex(r"/W(all|[0-4])"),
        class: FlagClass::NoObjectEffect,
        source: "clang-cl warning level (/W0-/W4, /Wall). Diagnostics only. Dash -Wall is already covered by the dialect-agnostic -W* row.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Oy-"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl frame-pointer omission disabled. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    // CRT (C Runtime) selection flags. Under Cl these are codegen flags
    // (they define `_MT`/`_DLL` macros and link the appropriate CRT),
    // not dep-info markers (cf. the Gnu-dialect row above which tags
    // the same spellings as NoObjectEffect).
    FlagSpec {
        matcher: Matcher::Exact("-MD"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded DLL (dynamic). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-MDd"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded DLL debug. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-MT"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded static. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-MTd"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded static debug. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/MD"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded DLL (dynamic). Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/MDd"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded DLL debug. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/MT"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded static. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/MTd"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl CRT: multithreaded static debug. Keyed via -### resolved tokens.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        // clang-cl optimization levels, `-` and `/` spellings, keyed via
        // the cc1 level in `-###`. NOTE: the bare `-O1`/`-O2` spellings
        // are already matched by the earlier dialect-agnostic `-O[0-3sz]?`
        // ModeledInKey row (→ `parsed.optimization`), so this row only
        // fires for `/O1`/`/O2`/`-Od`/`/Od`/`-Ox`/`/Ox`. Both paths key
        // the level, just by different mechanisms. `-Os`/`-Oz`/`-Ofast`
        // are not in this set and still refuse.
        matcher: Matcher::Regex(r"[-/]O[12dx]"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl optimization levels. Bare -O1/-O2 are caught earlier as ModeledInKey; this row covers /Onn and -Od/-Ox. -### resolves each to a distinct cc1 level. -Ofast/-Os/-Oz not matched → still refuse.",
        dialect: Some(Dialect::Cl),
    },
    // ── clang-cl Layer 4: Firefox-corpus flag classification (#285) ──
    //
    // All rows carry `dialect: Some(Dialect::Cl)`. Gnu/clang dialect
    // behaviour is unchanged.

    // ── CapturedByProbe: exception / RTTI / stack-protector / misc codegen ──
    //
    // Each row's effect is reflected in the clang-cl -### token stream:
    //   -EHsc   → -fexceptions + -fcxx-exceptions
    //   -GR-    → -fno-rtti
    //   -GS-    → removes -stack-protector from cc1
    //   -Brepro → removes -mincremental-linker-compatible from cc1
    //   -utf-8  → clang-cl is UTF-8 by default; the flag is inert (no cc1 token)
    //             but remains CapturedByProbe — keyed if it ever produces a token,
    //             inert if not; the probe requirement is always met for clang-cl.
    //   -Zc:*   → various conformance knobs reflected into cc1 tokens per value.
    //             NOTE: `-Zc:inline` / `/Zc:inline` are listed BEFORE these Prefix
    //             rows (exact rows appear earlier in the table) so they continue to
    //             resolve as NoObjectEffect via first-match.
    // `Prefix` wildcards on CapturedByProbe are safe: the -### stream captures the
    // exact value (or the flag is inert), so an unknown suffix still produces a
    // distinct key (no miscache risk).
    // clang-cl source-language override: `-TP`/`/TP` force every input to
    // compile as C++, `-TC`/`/TC` force C (MSVC `/TP` / `/TC`). This
    // changes the language the front end uses (and thus the object), but
    // clang-cl resolves it into the `-cc1 -x c++` / `-x c` token, so the
    // resolved-token hash differentiates it — the same treatment as the
    // gnu `-x <lang>` override. (Issue #411 — Firefox/Windows compiles
    // `Unified_cpp_*.cpp` with `-TP`.)
    FlagSpec {
        matcher: Matcher::Exact("-TP"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #411 — clang-cl force-C++ source mode (-TP). -### resolves it into the -cc1 -x c++ token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/TP"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #411 — clang-cl force-C++ source mode (/TP). -### resolves it into the -cc1 -x c++ token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-TC"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #411 — clang-cl force-C source mode (-TC). -### resolves it into the -cc1 -x c token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/TC"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #411 — clang-cl force-C source mode (/TC). -### resolves it into the -cc1 -x c token.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("-EH"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl exception model (-EHsc, -EHs-c-, …). Prefix safe: -### resolves each variant into distinct -fexceptions/-fcxx-exceptions tokens (or their negations).",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/EH"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl exception model (/EH* spelling). Prefix safe: -### resolves each variant into distinct -fexceptions/-fcxx-exceptions tokens (or their negations).",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-GR"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl RTTI enabled (-GR). -### reflects -frtti.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-GR-"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl RTTI disabled (-GR-). -### reflects -fno-rtti.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/GR"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl RTTI enabled (/GR). -### reflects -frtti.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/GR-"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl RTTI disabled (/GR-). -### reflects -fno-rtti.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-GS"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl stack-buffer-security-check enabled (-GS). -### reflects -stack-protector.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-GS-"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl stack-buffer-security-check disabled (-GS-). -### removes -stack-protector.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/GS"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl stack-buffer-security-check enabled (/GS). -### reflects -stack-protector.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/GS-"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl stack-buffer-security-check disabled (/GS-). -### removes -stack-protector.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-Brepro"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl reproducible build (-Brepro). -### removes -mincremental-linker-compatible.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Brepro"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl reproducible build (/Brepro). -### removes -mincremental-linker-compatible.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("-utf-8"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl UTF-8 source/execution charset (-utf-8). clang-cl is UTF-8 by default; the flag produces no cc1 token but is inert — CapturedByProbe is safe (keyed if token present, inert if not; probe always resolves for clang-cl).",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/utf-8"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl UTF-8 source/execution charset (/utf-8). Same rationale as -utf-8.",
        dialect: Some(Dialect::Cl),
    },
    // -Zc: conformance flags. The Exact rows for -Zc:inline / /Zc:inline
    // appear EARLIER in the table and resolve first (NoObjectEffect), so
    // only non-inline -Zc: values reach these Prefix rows.
    FlagSpec {
        matcher: Matcher::Prefix("-Zc:"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl conformance flags (-Zc:wchar_t, -Zc:forScope, …). Prefix safe: -### captures the exact value (or flag is inert); placed AFTER the -Zc:inline Exact row so that spelling resolves NoObjectEffect first.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/Zc:"),
        class: FlagClass::CapturedByProbe,
        source: "#285 Layer 4 — clang-cl conformance flags (/Zc:* spelling). Prefix safe: -### captures the exact value (or flag is inert); placed AFTER the /Zc:inline Exact row so that spelling resolves NoObjectEffect first.",
        dialect: Some(Dialect::Cl),
    },
    // ── PreprocessorCaptured: -FC makes __FILE__ expand to the full path ──
    //
    // clang-cl's `/EP` preprocessor hash captures `__FILE__` expansions,
    // so `-FC`'s effect (full path in `__FILE__`) is already in the key.
    FlagSpec {
        matcher: Matcher::Exact("-FC"),
        class: FlagClass::PreprocessorCaptured,
        source: "#285 Layer 4 — clang-cl full-path __FILE__ (-FC). Makes __FILE__ expand to the absolute source path; that expansion is captured by the /EP preprocessor hash.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/FC"),
        class: FlagClass::PreprocessorCaptured,
        source: "#285 Layer 4 — clang-cl full-path __FILE__ (/FC). Makes __FILE__ expand to the absolute source path; that expansion is captured by the /EP preprocessor hash.",
        dialect: Some(Dialect::Cl),
    },
    // ── NoObjectEffect: diagnostics / build mechanics ──
    //
    // None of these flags change the resulting object bytes.
    FlagSpec {
        matcher: Matcher::Exact("-nologo"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl suppress banner (-nologo). Pure build-output mechanic; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/nologo"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl suppress banner (/nologo). Pure build-output mechanic; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        // `-wdNNNN` / `/wdNNNN` — disable a specific warning by number.
        // Warnings only affect diagnostics, never the object.
        matcher: Matcher::Prefix("-wd"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl disable warning (-wdNNNN). Diagnostics only; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/wd"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl disable warning (/wdNNNN). Diagnostics only; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        // `-FS` / `/FS` — force synchronous PDB writes (serializes access
        // to the shared .pdb across parallel compilations). Pure build
        // mechanic; has no effect on the object file content.
        matcher: Matcher::Exact("-FS"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl force synchronous PDB writes (-FS). Build mechanic; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/FS"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl force synchronous PDB writes (/FS). Build mechanic; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        // `-Gm-` / `/Gm-` — disable minimal rebuild (deprecated MSVC flag).
        // Has no effect on the object content.
        matcher: Matcher::Exact("-Gm-"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl minimal rebuild disabled (-Gm-, deprecated). No object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Exact("/Gm-"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl minimal rebuild disabled (/Gm-, deprecated). No object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        // `-external:W<n>` / `/external:W<n>` and similar external-header
        // warning-level flags. These only affect diagnostics for headers
        // treated as "external" (system headers); no object effect.
        matcher: Matcher::Prefix("-external:"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl external-header warning level (-external:*). Diagnostics only; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    FlagSpec {
        matcher: Matcher::Prefix("/external:"),
        class: FlagClass::NoObjectEffect,
        source: "#285 Layer 4 — clang-cl external-header warning level (/external:*). Diagnostics only; no object effect.",
        dialect: Some(Dialect::Cl),
    },
    // ── clang-cl debug-info flags (#312) ─────────────────────────
    //
    // MSVC `/Z7`/`/Zi`/`/ZI`/`/Zd` and their `-` spellings embed
    // CodeView debug info in the object. Their codegen effect is in the
    // `-cc1` line (`-gcodeview`, `-debug-info-kind`, edit-and-continue),
    // so the variant is keyed via the `cc -###` resolved tokens —
    // `CapturedByProbe`. This also enforces the safety contract: if the
    // probe is unavailable the compile bails rather than under-keying
    // (box-confirmed: `/Z7` and `/Zi` resolve identically and produce
    // identical objects, but `/ZI` differs). The PATH inputs the debug
    // object embeds (source/output/compilation-dir) are a separate
    // concern, folded into the key by `cl_debug_path_inputs`.
    FlagSpec {
        matcher: Matcher::Regex(r"[-/]Z[7iId]"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #312 — clang-cl CodeView debug-info flags; variant captured via cc -### resolved tokens; embedded paths folded by cl_debug_path_inputs.",
        dialect: Some(Dialect::Cl),
    },
];

#[derive(Debug, Default)]
struct FlagClassificationSummary {
    modeled_in_key: usize,
    raw_keyed: usize,
    captured_by_probe: usize,
    preprocessor_captured: usize,
    no_object_effect: usize,
    parser_handled: usize,
    /// Unmodeled by the built-in table but opted into caching via the
    /// user's `[cc] extra_allowlist_flags` allow-list (issue #95).
    user_allowed: usize,
    unmodeled: usize,
}

impl FlagClassificationSummary {
    fn record(&mut self, class: Option<FlagClass>) {
        match class {
            Some(FlagClass::ModeledInKey) => self.modeled_in_key += 1,
            Some(FlagClass::RawKeyed) => self.raw_keyed += 1,
            Some(FlagClass::CapturedByProbe) => self.captured_by_probe += 1,
            Some(FlagClass::PreprocessorCaptured) => self.preprocessor_captured += 1,
            Some(FlagClass::NoObjectEffect) => self.no_object_effect += 1,
            Some(FlagClass::ParserHandled) => self.parser_handled += 1,
            None => self.unmodeled += 1,
        }
    }
}

/// Classify the parsed flags, emitting per-flag and per-compile traces,
/// and return the tokens that should *refuse* (force passthrough).
///
/// `extra_allowlist_flags` is the user's allow-list (issue #95): a flag the
/// built-in table doesn't model is normally rejected, but if it exactly
/// matches an allow-list entry it is accepted instead (logged as
/// `user-allowed (config)`) and folded verbatim into the cache key by
/// [`CcCompiler::cache_key`].
fn classify_and_trace_cc_flags<'a>(
    parsed: &'a CcArgs,
    extra_allowlist_flags: &[String],
) -> Vec<&'a str> {
    let subject = parsed
        .sources
        .first()
        .map(|source| source.display().to_string())
        .unwrap_or_else(|| parsed.program.clone());
    let mut summary = FlagClassificationSummary::default();
    let mut rejected = Vec::new();

    let dialect = parsed.family.dialect();
    let mut idx = 0;
    while idx < parsed.rest.len() {
        let arg = &parsed.rest[idx];

        // `-Xclang <tok>` forwards `<tok>` straight to the cc1 front end,
        // so the driver-level table can't classify it — `<tok>` means
        // whatever cc1 makes of it (cc1 `-MP` is dep-info phony targets,
        // not the clang-cl driver's `/MP`). Classify the forwarded token
        // against the cc1 allow-list and consume the pair, so its value
        // (itself a separate `-Xclang <value>`) isn't reprocessed.
        // Issue #411 (Firefox/Windows).
        if arg == "-Xclang" {
            // The marker only forwards; it has no object effect itself.
            summary.record(Some(FlagClass::NoObjectEffect));
            let Some(inner) = parsed.rest.get(idx + 1) else {
                // Trailing `-Xclang` with no operand: nothing to forward.
                tracing::trace!("[cc:{subject}] flag -Xclang (no operand) -> NoObjectEffect");
                idx += 1;
                continue;
            };
            let class = classify_xclang_forwarded(inner, dialect);
            summary.record(class);
            match class {
                Some(class) => tracing::trace!(
                    "[cc:{subject}] flag -Xclang {inner} -> {class:?} [forwarded cc1]"
                ),
                None if extra_allowlist_flags.iter().any(|f| f == inner) => {
                    summary.user_allowed += 1;
                    tracing::trace!("[cc:{subject}] flag -Xclang {inner} -> user-allowed (config)");
                }
                None => {
                    tracing::trace!(
                        "[cc:{subject}] flag -Xclang {inner} -> unmodeled [forwarded cc1]"
                    );
                    rejected.push(inner.as_str());
                }
            }
            idx += 2;
            continue;
        }

        let analysis = analyze_cc_arg(arg, dialect);
        summary.record(analysis.class);
        match analysis.class {
            Some(class) => tracing::trace!(
                "[cc:{subject}] flag {arg} -> {class:?} [{:?}]",
                analysis.bucket
            ),
            None if extra_allowlist_flags.iter().any(|f| f == arg) => {
                summary.user_allowed += 1;
                tracing::trace!(
                    "[cc:{subject}] flag {arg} -> user-allowed (config) [verbatim-keyed]"
                );
            }
            None => {
                tracing::trace!(
                    "[cc:{subject}] flag {arg} -> unmodeled [{:?}]",
                    analysis.bucket
                );
                rejected.push(arg.as_str());
            }
        }
        idx += 1;
    }

    if !parsed.rest.is_empty() {
        tracing::debug!(
            "[cc:{subject}] flag classify: {} modeled / {} raw-keyed / {} probe / {} preprocessor / {} no-effect / {} parser-handled / {} user-allowed / {} unmodeled",
            summary.modeled_in_key,
            summary.raw_keyed,
            summary.captured_by_probe,
            summary.preprocessor_captured,
            summary.no_object_effect,
            summary.parser_handled,
            summary.user_allowed,
            summary.unmodeled
        );
    }

    rejected
}

/// Select the user-declared flags (issue #95) to fold verbatim into the
/// cache key: the command-line tokens that (a) match an allow-list entry
/// exactly and (b) the built-in table does NOT model — i.e. exactly the
/// "user-allowed" set from [`classify_and_trace_cc_flags`]. Sorted +
/// deduped so argv order and repeated flags never perturb the key, and a
/// configured-but-absent flag is excluded (it has no codegen effect).
fn cc_extra_flags_for_key<'a>(
    parsed: &'a CcArgs,
    extra_allowlist_flags: &[String],
) -> Vec<&'a str> {
    if extra_allowlist_flags.is_empty() {
        return Vec::new();
    }
    let dialect = parsed.family.dialect();
    let mut matched: Vec<&str> = parsed
        .rest
        .iter()
        .map(String::as_str)
        .filter(|arg| {
            classify_cc_flag(arg, dialect).is_none()
                && extra_allowlist_flags.iter().any(|f| f == arg)
        })
        .collect();
    matched.sort_unstable();
    matched.dedup();
    matched
}

const WA_DEBUG_PREFIX_MAP_PREFIX: &str = "-Wa,--debug-prefix-map=";

/// Built-in flags whose object effect is not present in the resolved `cc1`
/// stream and therefore must be folded directly into the cache key.
///
/// Preserve argv order and duplicates: assembler option precedence may be
/// order-sensitive. For GNU as's `--debug-prefix-map=OLD=NEW`, normalize only
/// the location-dependent `OLD`; `NEW` is object material and stays verbatim.
fn cc_raw_flags_for_key(parsed: &CcArgs, prefix_maps: &[CcPrefixMap]) -> Vec<Vec<u8>> {
    let dialect = parsed.family.dialect();
    parsed
        .rest
        .iter()
        .filter(|arg| classify_cc_flag(arg, dialect) == Some(FlagClass::RawKeyed))
        .map(|arg| normalize_raw_keyed_cc_flag(arg, prefix_maps))
        .collect()
}

fn normalize_raw_keyed_cc_flag(arg: &str, prefix_maps: &[CcPrefixMap]) -> Vec<u8> {
    let Some(mapping) = arg.strip_prefix(WA_DEBUG_PREFIX_MAP_PREFIX) else {
        return arg.as_bytes().to_vec();
    };
    let Some((from, to)) = mapping.split_once('=') else {
        // The classifier rejects this malformed spelling, but keeping the
        // fallback lossless makes this helper safe if called independently.
        return arg.as_bytes().to_vec();
    };

    let from = apply_cc_prefix_maps_to_bytes(from.as_bytes().to_vec(), prefix_maps);
    let mut normalized = Vec::with_capacity(arg.len());
    normalized.extend_from_slice(WA_DEBUG_PREFIX_MAP_PREFIX.as_bytes());
    normalized.extend_from_slice(&from);
    normalized.push(b'=');
    normalized.extend_from_slice(to.as_bytes());
    normalized
}

fn analyze_cc_arg(arg: &str, dialect: Dialect) -> CcArgAnalysis<'_> {
    let class = classify_cc_flag(arg, dialect);
    let spec = cc_arg_spec_for_token(arg, dialect);
    CcArgAnalysis {
        arg,
        class,
        bucket: cc_arg_bucket(class, spec),
        normalized: normalize_cc_arg(arg, dialect),
        refusal: class.is_none().then_some("cc: unsupported flag"),
        source: spec.map(|spec| spec.source),
    }
}

fn cc_arg_bucket(class: Option<FlagClass>, spec: Option<&'static CcArgSpec>) -> CcArgBucket {
    if class.is_none() {
        return CcArgBucket::TooHard;
    }
    if let Some(spec) = spec {
        return spec.bucket;
    }
    match class {
        Some(FlagClass::ModeledInKey) => CcArgBucket::ModeledInKey,
        Some(FlagClass::RawKeyed) => CcArgBucket::RawKeyed,
        Some(FlagClass::ParserHandled) => CcArgBucket::Structural,
        Some(FlagClass::CapturedByProbe) => CcArgBucket::ProbeKeyed,
        Some(FlagClass::PreprocessorCaptured) => CcArgBucket::Preprocessor,
        Some(FlagClass::NoObjectEffect) => CcArgBucket::NoObjectEffect,
        None => CcArgBucket::TooHard,
    }
}

fn normalize_cc_arg(arg: &str, dialect: Dialect) -> Vec<String> {
    let Some(spec) = cc_arg_spec_for_token(arg, dialect) else {
        return vec![arg.to_string()];
    };
    match spec.value_form {
        CcArgValueForm::Flag | CcArgValueForm::Separated => vec![arg.to_string()],
        CcArgValueForm::Concatenated { prefix } => arg
            .strip_prefix(prefix)
            .map(|value| vec![prefix.to_string(), value.to_string()])
            .unwrap_or_else(|| vec![arg.to_string()]),
        CcArgValueForm::CanBeSeparated { prefix } => {
            if arg == prefix {
                vec![prefix.to_string()]
            } else {
                arg.strip_prefix(prefix)
                    .filter(|value| !value.is_empty())
                    .map(|value| vec![prefix.to_string(), value.to_string()])
                    .unwrap_or_else(|| vec![arg.to_string()])
            }
        }
    }
}

fn cc_arg_spec_for_token(arg: &str, dialect: Dialect) -> Option<&'static CcArgSpec> {
    CC_ARG_SPECS.iter().find(|spec| {
        // Skip rows restricted to a different dialect, so a token shared
        // across dialects (e.g. `-MT`) resolves to the row for the active
        // dialect — not whichever appears first in the table.
        if spec.dialect.is_some_and(|d| d != dialect) {
            return false;
        }
        match spec.value_form {
            CcArgValueForm::Flag | CcArgValueForm::Separated => cc_arg_spec_matches(spec, arg),
            CcArgValueForm::Concatenated { prefix } => arg.starts_with(prefix),
            CcArgValueForm::CanBeSeparated { prefix } => {
                arg == prefix
                    || arg
                        .strip_prefix(prefix)
                        .is_some_and(|value| !value.is_empty())
            }
        }
    })
}

/// Classify a cc argument. Wraps [`crate::compiler::flags::classify_against`]
/// over [`CC_FLAGS`] with a lazy regex cache. Returns `None` for any
/// argument no row matches — the caller treats that as "unsupported
/// flag, refuse to cache".
fn classify_cc_flag(arg: &str, dialect: Dialect) -> Option<FlagClass> {
    static CACHE: OnceLock<crate::compiler::flags::RegexCache> = OnceLock::new();
    crate::compiler::flags::classify_against(
        arg,
        CC_FLAGS,
        CACHE.get_or_init(|| crate::compiler::flags::build_regex_cache(CC_FLAGS)),
        dialect,
    )
}

/// cc1 frontend flags that clang's driver forwards verbatim via
/// `-Xclang <flag>`. Each is inert for object-content caching — dep-info
/// sidecar emission or terminal diagnostics — so an `-Xclang`-wrapped one
/// is safe to cache past. A forwarded flag NOT on this list still refuses
/// (see [`classify_xclang_forwarded`]), so an `-Xclang`-wrapped *codegen*
/// flag can never slip through. Sourced from Firefox's Windows build
/// (issue #411).
///
/// These are deliberately classified only in forwarded (`-Xclang`)
/// position, NOT as bare driver flags: e.g. cc1 `-MP` means "emit phony
/// dep targets", whereas the clang-cl driver's `/MP` is multi-process
/// compilation — different flags that happen to share a spelling.
const XCLANG_INERT_CC1_FLAGS: &[&str] = &[
    // ── dep-info sidecar family ──────────────────────────────────────
    // Every one of these only shapes the `.d`/`.pp` dependency sidecar;
    // none changes a byte of the object. They are listed as a *family*,
    // not just the two flags issue #411 happened to surface, because they
    // co-occur: cc1 rejects `-dependency-file` unless a `-MT`/`-MQ` target
    // accompanies it, so a build that forwards one forwards several. (cc1
    // spellings — distinct from the clang-cl driver's `/MT` CRT-selection
    // flag, which is why these are recognized only in forwarded position.)
    "-dependency-file", // write the dep sidecar (path is a separate -Xclang value)
    "-MT",              // dependency target name
    "-MQ",              // dependency target name, quoted for make
    "-MP",              // emit a phony target per header
    "-MG",              // tolerate missing (generated) headers
    "-MV",              // NMake/Visual Studio style dependency output
    "-sys-header-deps", // include system headers in the dep output
    "-module-file-deps", // include module files in the dep output
    "-dependency-dot",  // write DOT-format header deps (path is a separate value)
    // ── terminal diagnostics ─────────────────────────────────────────
    "-fansi-escape-codes", // emit ANSI color escapes regardless of TTY detection
];

/// Classify a single cc1 token forwarded to the front end via `-Xclang`.
///
/// Returns `NoObjectEffect` for an allow-listed inert cc1 flag
/// ([`XCLANG_INERT_CC1_FLAGS`]) or for a bare value token — e.g. the
/// dependency-file path, itself forwarded as its own `-Xclang <value>`
/// pair.
///
/// A forwarded flag that matches a modeled `CapturedByProbe` codegen knob
/// (`-Xclang -ffp-contract=off`, the Firefox/Windows clang-cl shape of #428)
/// is allowed and keyed via the probe: `-Xclang` forwards it to cc1, where the
/// `cc -###` resolved cc1 line records it (verified: `-Xclang -ffp-contract=off`
/// appends `-ffp-contract=off` to the dump), so the cache key already
/// differentiates it from the default. This is safe, NOT "blind": the bare
/// operand token also classifies as `ProbeKeyed` at the driver level, so
/// `cc_flags_need_resolved_invocation` forces the probe and refuses to cache if
/// it cannot resolve — there is no under-keying path. An UNMODELED `-Xclang`
/// codegen flag (not in `CC_FLAGS`) still returns `None` and refuses.
fn classify_xclang_forwarded(inner: &str, dialect: Dialect) -> Option<FlagClass> {
    if !inner.starts_with('-') {
        // A value token (e.g. the dependency-file path). No object effect;
        // its content is irrelevant to the resulting `.obj`.
        return Some(FlagClass::NoObjectEffect);
    }
    // Inert cc1 flags FIRST: a forwarded `-MT`/`-MP`/… is the cc1 dep-info flag,
    // which must NOT be confused with the clang-cl driver flag of the same
    // spelling (`/MT` = CRT selection, a CapturedByProbe codegen knob). The
    // driver-level `classify_cc_flag` below would misread the cc1 spelling as
    // the driver flag, so the forwarded-position allow-list wins.
    if XCLANG_INERT_CC1_FLAGS.contains(&inner) {
        return Some(FlagClass::NoObjectEffect);
    }
    if classify_cc_flag(inner, dialect) == Some(FlagClass::CapturedByProbe) {
        return Some(FlagClass::CapturedByProbe);
    }
    None
}

fn cc_flags_need_resolved_invocation(parsed: &CcArgs) -> bool {
    let dialect = parsed.family.dialect();
    // Any driver-level probe-keyed flag forces the resolved `cc -###`
    // invocation so the key captures its codegen effect.
    if parsed
        .rest
        .iter()
        .any(|arg| analyze_cc_arg(arg, dialect).bucket == CcArgBucket::ProbeKeyed)
    {
        return true;
    }
    // A `-Xclang`-forwarded CapturedByProbe knob is keyed via the resolved cc1
    // line too (#428), so it must force the probe as well. The flat scan above
    // already catches self-contained shapes whose operand also matches a driver
    // row (e.g. `-ffp-contract=`), but a cc1-only forwarded knob would slip
    // past it — without the probe its codegen effect would not be keyed.
    parsed.rest.windows(2).any(|w| {
        w[0] == "-Xclang"
            && classify_xclang_forwarded(&w[1], dialect) == Some(FlagClass::CapturedByProbe)
    })
}

/// MSVC debug-info markers that embed absolute CodeView paths into the
/// object. clang-cl puts debug info in the `.obj` for all of these (no
/// compile-time PDB — box-confirmed), so each is a single cacheable
/// artifact once the embedded path inputs are keyed.
const CL_DEBUG_FLAGS: &[&str] = &["/Z7", "/Zi", "/ZI", "/Zd", "-Z7", "-Zi", "-ZI", "-Zd"];

/// Whether this clang-cl invocation requests debug info (native MSVC
/// spelling or a `-g` form parsed into `debug_level`). Only meaningful
/// for `Dialect::Cl`; the caller gates on dialect.
///
/// NOTE: `cl_debug_present` does NOT imply the `-###` probe is forced.
/// The native `/Z*` spellings are modeled `CapturedByProbe` (the
/// `/Z7`-vs-`/ZI` variant split is keyed via resolved tokens, bailing if
/// the probe is absent). The bare `-g` form has no such variant — it is
/// `ModeledInKey` via `debug_level` and its embedded paths are folded
/// here — so a `-g`-only clang-cl compile keys correctly without the
/// probe. Don't assume `cl_debug_present ⇒ probe required`.
fn cl_debug_present(parsed: &CcArgs) -> bool {
    parsed.family.dialect() == Dialect::Cl
        && (parsed.debug_level.is_some_and(|d| d > 0)
            || parsed
                .rest
                .iter()
                .any(|a| CL_DEBUG_FLAGS.contains(&a.as_str())))
}

/// The per-TU path inputs a clang-cl debug object embeds in CodeView that
/// the cache key would otherwise miss: the source file path(s) as spelled
/// on the command line, the output object name from `-Fo`/`-o`, and the
/// effective compilation directory (an explicit `-fdebug-compilation-dir`
/// or `-ffile-compilation-dir` value if present, otherwise the OS cwd).
/// These are exactly the tokens `config_args()` strips (source, output)
/// plus the compilation directory. `-I` dirs and flags are already in the
/// key via the resolved tokens. Capture, not remap — clang-cl stays
/// path-literal (#299/#312). `None` when this is not a clang-cl debug
/// compile (no fold; non-debug objects don't embed these, so
/// cross-CWD/name hits stay correct).
/// Per-TU path strings that can appear verbatim in the resolved `cc -###`
/// token stream and MUST be kept out of the cache key.
///
/// The resolved-invocation probe is memoized per *flag set* — `config_args`
/// strips the source, `-o`/`-Fo` output, and dep-file values so ONE probe
/// record serves every TU of a build. Absolute paths in the resolved tokens
/// are already blanked to a sentinel ([`crate::probe`]), and the trailing
/// source input token too — but a RELATIVE per-TU path that appears as a
/// flag *value* (`-main-file-name u00.c`, `-o build/u00.o`) survives,
/// leaving the shared record's tokens TU-specific. Serially the leak is
/// consistent (cold==warm → still hits); under `make -j` the TUs race over
/// whose paths the first-probing TU wrote into the shared record, leaking
/// one TU's source/output into another TU's key, so the key is
/// non-deterministic and the warm rebuild intermittently MISSES.
///
/// Blanking these tokens (to the path sentinel, inside the shared probe
/// record before it is stored) is safe in the never-miscache direction: the
/// source CONTENT is already captured by the preprocessor-expansion hash and
/// the output path has no object-content effect, so this only ever merges
/// keys that differ solely in a per-TU path. The set mirrors what
/// `config_args` removes from the probe-memo key, and is handed to the probe
/// so [`crate::probe`] sentinels these values out of the resolved tokens.
fn cc_resolved_per_tu_paths(parsed: &CcArgs) -> Vec<String> {
    let mut set = HashSet::new();
    // Insert a path both as written AND by basename: the cc1 line spells
    // the same file differently per token — `-o build/u00.o` keeps the
    // path, but `-main-file-name u00.c` uses only the basename. Blanking
    // must catch every spelling or a residual per-TU token still races.
    let mut add = |p: &Path| {
        set.insert(p.to_string_lossy().into_owned());
        if let Some(name) = p.file_name() {
            set.insert(name.to_string_lossy().into_owned());
        }
    };
    for src in &parsed.sources {
        add(src);
    }
    if let Some(o) = &parsed.output {
        add(o);
    }
    if let Some(o) = parsed.object_output_path() {
        add(&o);
    }
    if let Some(d) = parsed.depinfo_output_path() {
        add(&d);
    }
    if let Some(t) = parsed.depinfo.as_ref().and_then(|d| d.target.clone()) {
        set.insert(t);
    }
    // Never blank an empty token (a no-op path would blank real tokens).
    set.remove("");
    set.into_iter().collect()
}

fn cl_debug_path_inputs(parsed: &CcArgs) -> Option<Vec<String>> {
    if !cl_debug_present(parsed) {
        return None;
    }
    let mut out = Vec::new();
    // Paths are encoded lossily; on Windows (where clang-cl runs) paths
    // are always valid UTF-16 → UTF-8, so no two distinct paths collapse.
    for src in &parsed.sources {
        out.push(format!("src={}", src.to_string_lossy()));
    }
    if let Some(o) = &parsed.output {
        out.push(format!("out={}", o.to_string_lossy()));
    }
    // NOTE (#312 follow-up): the compilation-dir spellings below are
    // NOT yet modeled in CC_FLAGS, so a clang-cl debug compile that
    // passes one EXPLICITLY currently hits the unmodeled-flag refusal
    // (passthrough — safe, not a miscache) before reaching this fold.
    // The common case (clang auto-injects -fdebug-compilation-dir at
    // -cc1, not on the driver line) is unaffected. Modeling these flags
    // to also cache the explicit-dir case is a deferred follow-up.
    let dir = parsed
        .rest
        .iter()
        .find_map(|a| {
            [
                "-fdebug-compilation-dir=",
                "-ffile-compilation-dir=",
                "/fdebug-compilation-dir=",
                "/ffile-compilation-dir=",
            ]
            .iter()
            .find_map(|p| a.strip_prefix(p))
            .map(str::to_string)
        })
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        });
    if let Some(d) = dir {
        out.push(format!("dir={d}"));
    }
    Some(out)
}

thread_local! {
    /// The prefix maps of the last invocation keyed in this process. One
    /// wrapper keys the same invocation up to three times (memo lookup, the
    /// re-check after a discovery flight, the key after a deferred compile)
    /// and the maps only depend on the arguments, the process environment
    /// and the configured base dirs.
    static PREFIX_MAPS_MEMO: std::cell::RefCell<Option<PrefixMapsMemo>> =
        const { std::cell::RefCell::new(None) };
}

struct PrefixMapsMemo {
    identity: Vec<String>,
    base_dirs: Vec<String>,
    maps: Vec<CcPrefixMap>,
}

/// Prefix maps that make C/C++ objects path-stable across worktrees.
///
/// A `-g` compile bakes paths into DWARF (`DW_AT_comp_dir`) and
/// `__FILE__` expansions. Firefox also exposes this through headers
/// whose macros stringify absolute include paths after preprocessing.
/// Mapping only the compiler CWD misses sibling objdir/source paths
/// like `<checkout>/obj/dist/include`, so derive the common root of
/// the source and build directories and map that instead.
///
/// The fallback split roots handle out-of-tree builds where source and
/// object directories do not share a useful project root. Distinct
/// sentinels avoid collapsing unrelated paths to the same spelling.
fn cc_prefix_maps(parsed: &CcArgs, configured_base_dirs: &[String]) -> Vec<CcPrefixMap> {
    // Everything the uncached computation reads besides its arguments.
    let env = |name: &str| {
        std::env::var_os(name)
            .map(|v| v.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let mut identity = Vec::with_capacity(parsed.rest.len() + 6);
    identity.push(parsed.program.clone());
    identity.extend(parsed.rest.iter().cloned());
    identity.push(
        std::env::current_dir()
            .map(|d| d.to_string_lossy().into_owned())
            .unwrap_or_default(),
    );
    for name in [
        "KACHE_CC_PATH_NORMALIZE",
        "KACHE_BASE_DIR",
        "SDKROOT",
        "OUT_DIR",
    ] {
        identity.push(env(name));
    }
    let memoized = PREFIX_MAPS_MEMO.with(|memo| {
        memo.borrow().as_ref().and_then(|last| {
            (last.identity == identity && last.base_dirs == configured_base_dirs)
                .then(|| last.maps.clone())
        })
    });
    if let Some(maps) = memoized {
        return maps;
    }
    let maps = cc_prefix_maps_uncached(parsed, configured_base_dirs);
    PREFIX_MAPS_MEMO.with(|memo| {
        *memo.borrow_mut() = Some(PrefixMapsMemo {
            identity,
            base_dirs: configured_base_dirs.to_vec(),
            maps: maps.clone(),
        });
    });
    maps
}

fn cc_prefix_maps_uncached(parsed: &CcArgs, configured_base_dirs: &[String]) -> Vec<CcPrefixMap> {
    // `KACHE_CC_PATH_NORMALIZE=0` disables cc path normalization entirely:
    // no maps → the key hashes raw paths AND `execute` injects no
    // `-ffile-prefix-map`. The conservative escape hatch — cc keys become
    // path-literal (no cross-machine cc sharing, but zero normalization
    // miscache risk). Default on.
    if !cc_path_normalize_enabled() {
        return Vec::new();
    }
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(_) => return Vec::new(),
    };
    let base = std::env::var_os("KACHE_BASE_DIR").filter(|v| !v.is_empty());
    // `SDKROOT` is the Apple-clang env that pins the SDK when no explicit
    // `-isysroot` is on the command line; read here (the only env access)
    // and threaded into the deterministic core for testability.
    let sdkroot = std::env::var_os("SDKROOT").filter(|v| !v.is_empty());
    let mut maps = cc_prefix_maps_cfg(
        parsed,
        &cwd,
        base.as_deref().map(Path::new),
        sdkroot.as_deref().map(Path::new),
        configured_base_dirs,
    );
    if !maps.is_empty()
        && let Some(out_dir) = std::env::var_os("OUT_DIR").filter(|v| !v.is_empty())
    {
        push_cargo_out_dir_maps(&mut maps, &cwd, Path::new(&out_dir));
        let mut include_dirs = cc_user_include_dirs(parsed, &cwd);
        include_dirs.extend(
            cc_flag_dir_values(&parsed.rest, "-isystem")
                .into_iter()
                .map(|dir| absolutize_path(&cwd, Path::new(dir))),
        );
        push_cargo_dep_out_dir_maps(&mut maps, &cwd, Path::new(&out_dir), &include_dirs);
        maps.sort_by_key(|m| std::cmp::Reverse(m.from.len()));
    }
    maps
}

/// Map the build-script output directories of the other crates this compile
/// includes from (`-I<target>/debug/build/libz-sys-<hash>/out/include`, or
/// `.../build/libz-sys/<hash>/out/include` from Cargo 1.100) to
/// [`CC_DEP_OUT_DIR_SENTINEL`] plus the crate name, dropping Cargo's
/// metadata hash. The compile's own `OUT_DIR` keeps its map. A crate name
/// that resolves to two units in one compile keeps the hash: the sentinel
/// would no longer name one directory.
fn push_cargo_dep_out_dir_maps(
    maps: &mut Vec<CcPrefixMap>,
    cwd: &Path,
    out_dir: &Path,
    include_dirs: &[PathBuf],
) {
    let own = absolutize_path(cwd, out_dir);
    let Some(target) = crate::build_script::target_dir(&own) else {
        return;
    };
    let mut units: std::collections::BTreeMap<String, Vec<PathBuf>> =
        std::collections::BTreeMap::new();
    for dir in include_dirs {
        let Some((unit_out, name)) = crate::cargo_layout::unit_out_dir_under(&target, dir) else {
            continue;
        };
        if unit_out == own {
            continue;
        }
        let dirs = units.entry(name).or_default();
        if !dirs.contains(&unit_out) {
            dirs.push(unit_out);
        }
    }
    for (name, dirs) in units {
        let [unit_out] = dirs.as_slice() else {
            continue;
        };
        let to = format!("{CC_DEP_OUT_DIR_SENTINEL}/{name}");
        for root in [canonicalize_or_self(unit_out), unit_out.clone()] {
            let from = root.to_string_lossy().to_string();
            if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
                maps.push(CcPrefixMap {
                    from,
                    to: to.clone(),
                });
            }
        }
    }
}

/// Map a build script's `OUT_DIR`, and the target directory it sits in, to
/// sentinels shared by every build directory. Appended after the derived
/// roots; the caller re-sorts so the longest prefix still wins.
fn push_cargo_out_dir_maps(maps: &mut Vec<CcPrefixMap>, cwd: &Path, out_dir: &Path) {
    let out_abs = absolutize_path(cwd, out_dir);
    let mut roots: Vec<(PathBuf, &'static str)> = Vec::new();
    if let Some(target) = crate::build_script::target_dir(&out_abs) {
        roots.push((canonicalize_or_self(&target), CC_TARGET_SENTINEL));
        roots.push((target, CC_TARGET_SENTINEL));
    }
    roots.push((canonicalize_or_self(&out_abs), CC_OUT_DIR_SENTINEL));
    roots.push((out_abs, CC_OUT_DIR_SENTINEL));
    for (root, to) in roots {
        let from = root.to_string_lossy().to_string();
        if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
            maps.push(CcPrefixMap {
                from,
                to: to.to_string(),
            });
        }
    }
}

/// The Apple SDK path this invocation pins, for the `<SDKROOT>` map.
///
/// Prefers an explicit `-isysroot <path>` (cargo's `cc` crate passes it on
/// Apple targets via `apple_sdk_root()`; mozbuild and CMake toolchains do
/// too), falling back to the `SDKROOT` env value the env-reading wrapper
/// threads in. Returns `None` when neither is present — a bare `cc -c`
/// that lets clang resolve the SDK via `xcrun` internally is not
/// normalized yet (issue #78).
fn cc_sdk_root(parsed: &CcArgs, sdkroot_env: Option<&Path>) -> Option<PathBuf> {
    let mut iter = parsed.rest.iter();
    while let Some(arg) = iter.next() {
        if arg == "-isysroot"
            && let Some(path) = iter.next()
            && !path.is_empty()
        {
            return Some(PathBuf::from(path));
        }
    }
    sdkroot_env.map(Path::to_path_buf)
}

/// Deterministic core of [`cc_prefix_maps`] (reads no env) — the derived
/// roots plus an optional user `base_dir` (`KACHE_BASE_DIR`) and the
/// Apple SDK root (explicit `-isysroot`, else `sdk_root` from `SDKROOT`).
fn cc_prefix_maps_cfg(
    parsed: &CcArgs,
    cwd: &Path,
    base_dir: Option<&Path>,
    sdk_root: Option<&Path>,
    configured_base_dirs: &[String],
) -> Vec<CcPrefixMap> {
    // clang-cl ignores `-ffile-prefix-map`, so prefix-mapping the key over
    // an object that still embeds raw paths would miscache. Until Layer 3
    // proves a cl path-remap, cl keys stay path-literal (unnormalised
    // `-###` abs paths make them per-machine — misses, never miscache) and
    // `execute` injects nothing. (The MSVC dialect doesn't use
    // `-isysroot`/`SDKROOT` anyway, on any host.)
    if parsed.family.dialect() == Dialect::Cl {
        return Vec::new();
    }
    let mut maps: Vec<CcPrefixMap> = Vec::new();

    // User-declared base dir (ccache `CCACHE_BASEDIR` analog). An explicit
    // root stripped to `<CC_BASE>` — covers paths the derived roots miss,
    // e.g. objdir-built TUs whose `__FILE__` points into the source tree
    // *above* the (narrow) derived root. A distinct sentinel so it can't
    // collide with a derived `<CC_ROOT>` subtree.
    if let Some(base) = base_dir {
        let base_abs = absolutize_path(cwd, base);
        for root in [base_abs.clone(), canonicalize_or_self(&base_abs)] {
            let from = root.to_string_lossy().to_string();
            if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
                maps.push(CcPrefixMap {
                    from,
                    to: CC_BASE_SENTINEL.to_string(),
                });
            }
        }
    }

    // File-configured extra roots. Share PathNormalizer's lexical-first alias
    // reservation and Windows path variants so Rust and C/C++ assign the same
    // stable target on every host.
    for (from, to) in crate::path_normalizer::configured_base_dir_prefix_maps(configured_base_dirs)
    {
        if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
            maps.push(CcPrefixMap { from, to });
        }
    }

    // Explicit roots win exact-prefix ties; derived roots cover everything
    // else. Longest-prefix sorting below handles overlapping configured roots.
    for map in cc_prefix_maps_for(parsed, cwd) {
        if !maps.iter().any(|existing| existing.from == map.from) {
            maps.push(map);
        }
    }

    // Apple SDK root (issue #78). The SDK path leaks into the key via the
    // resolved `cc -###` tokens; map it to `<SDKROOT>` so the same SDK at
    // a different install path (Xcode vs Command Line Tools, a teammate's
    // machine, a CI runner) keys identically. An explicit `-isysroot`
    // wins over the `SDKROOT` env value (`cc_sdk_root`). Distinct sentinel
    // — never a project root.
    if let Some(sdk) = cc_sdk_root(parsed, sdk_root) {
        let sdk_abs = absolutize_path(cwd, &sdk);
        for root in [sdk_abs.clone(), canonicalize_or_self(&sdk_abs)] {
            let from = root.to_string_lossy().to_string();
            if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
                maps.push(CcPrefixMap {
                    from,
                    to: CC_SDKROOT_SENTINEL.to_string(),
                });
            }
        }
    }

    // Longest `from` first so the most specific prefix wins in the byte
    // normalizer (covers the derived roots, the base dir, and the SDK).
    maps.sort_by_key(|m| std::cmp::Reverse(m.from.len()));
    maps
}

/// Whether cc path normalization is active. `KACHE_CC_PATH_NORMALIZE` set
/// to `0` / `false` / `off` / `no` disables it; default on.
fn cc_path_normalize_enabled() -> bool {
    parse_cc_normalize_toggle(std::env::var("KACHE_CC_PATH_NORMALIZE").ok().as_deref())
}

fn parse_cc_normalize_toggle(value: Option<&str>) -> bool {
    match value {
        Some(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        None => true,
    }
}

/// The effective `SOURCE_DATE_EPOCH` kache pins on *both* the `-E` key probe
/// and the real compile, so a translation unit that bakes
/// `__DATE__` / `__TIME__` / `__TIMESTAMP__` into its object produces bytes
/// that match the time-stable cache key — no stale-timestamp false hit (#423).
///
/// Resolution (see [`resolve_source_date_epoch`]):
/// - the build's own `SOURCE_DATE_EPOCH` if it exported one (honored as-is, so
///   the key reflects the date the object actually bakes);
/// - otherwise `"0"`, kache's default pin that makes the key time-independent
///   so warm rebuilds hit;
/// - `None` only when the build set nothing *and* the user opted out via
///   `KACHE_CC_SOURCE_DATE_EPOCH=passthrough` — then kache pins nothing and the
///   object bakes wall-clock. The unpinned `-E` probe then produces a
///   time-dependent key, so a stale wall-clock object is very unlikely to be
///   reused (best-effort, not a guarantee: two probes landing in the same
///   second expand identically, so a cross-second cold store can still be hit).
fn effective_source_date_epoch() -> Option<std::ffi::OsString> {
    resolve_source_date_epoch(
        std::env::var_os("SOURCE_DATE_EPOCH"),
        source_date_epoch_passthrough(),
    )
}

/// Pure resolution of the effective `SOURCE_DATE_EPOCH` (env read separately so
/// this stays unit-testable). A build-exported value is honored **verbatim** —
/// the raw bytes, untrimmed — so kache never normalizes a value the compiler
/// would otherwise reject (e.g. `" 123 "`, `""`, non-UTF-8) into a different,
/// accepted one, which would turn a failing compile into a cached success. Only
/// when the build set nothing does kache pin its default `"0"`, unless the
/// caller opted out.
pub(super) fn resolve_source_date_epoch(
    build_value: Option<std::ffi::OsString>,
    passthrough: bool,
) -> Option<std::ffi::OsString> {
    match build_value {
        Some(v) => Some(v),
        None if passthrough => None,
        None => Some(std::ffi::OsString::from("0")),
    }
}

/// Whether the build opted out of kache's default `SOURCE_DATE_EPOCH=0` pin via
/// `KACHE_CC_SOURCE_DATE_EPOCH=passthrough` (aliases: `wallclock` / `off`), for
/// the rare project that must bake the real wall-clock time into its objects.
/// Ignored when the build exports its own `SOURCE_DATE_EPOCH` (always honored).
fn source_date_epoch_passthrough() -> bool {
    std::env::var("KACHE_CC_SOURCE_DATE_EPOCH")
        .ok()
        .map(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "passthrough" || v == "wallclock" || v == "off"
        })
        .unwrap_or(false)
}

fn cc_memo_os_bytes(value: &OsStr) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        value.as_bytes().to_vec()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        value.encode_wide().flat_map(u16::to_le_bytes).collect()
    }
    #[cfg(not(any(unix, windows)))]
    {
        value.to_string_lossy().into_owned().into_bytes()
    }
}

fn fold_cc_memo_field(hasher: &mut blake3::Hasher, label: &[u8], value: &[u8]) {
    hasher.update(&(label.len() as u64).to_le_bytes());
    hasher.update(label);
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
    // Values are logged as digests only: an environment value may be a secret.
    tracing::trace!(
        target: "kache::cc_memo_key",
        "memo-field {}={}",
        String::from_utf8_lossy(label),
        &blake3::hash(value).to_hex()[..12]
    );
}

/// Environment variables that cannot change a preprocessor expansion and do
/// change between two otherwise identical invocations. Checked before the
/// allow list below, so a name matching both stays out.
const CC_MEMO_VOLATILE_ENV: &[&str] = &[
    // Shell bookkeeping, rewritten per command.
    "_",
    "OLDPWD",
    "PWD",
    "SHLVL",
    // Build-driver parallelism: `--jobserver-fds=3,5` names this run's fds.
    "CARGO_MAKEFLAGS",
    "MAKEFLAGS",
    "MFLAGS",
    "MAKELEVEL",
    "NUM_JOBS",
];

/// Environment variables the preprocessor reads, by the GCC, Clang and MSVC
/// documentation. Names are matched exactly; [`cc_memo_env_pattern_is_keyed`]
/// widens this to families of names.
const CC_MEMO_KEYED_ENV: &[&str] = &[
    // GCC "Environment Variables Affecting GCC"; Clang honours the same set.
    "CPATH",
    "C_INCLUDE_PATH",
    "CPLUS_INCLUDE_PATH",
    "OBJC_INCLUDE_PATH",
    "OBJCPLUS_INCLUDE_PATH",
    "GCC_EXEC_PREFIX",
    "COMPILER_PATH",
    "LIBRARY_PATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "DEPENDENCIES_OUTPUT",
    "SUNPRO_DEPENDENCIES",
    "SOURCE_DATE_EPOCH",
    // Clang driver.
    "CCC_OVERRIDE_OPTIONS",
    "CLANG_CONFIG_FILE_SYSTEM_DIR",
    "CLANG_CONFIG_FILE_USER_DIR",
    "CLANG_NO_DEFAULT_CONFIG",
    "SDKROOT",
    "MACOSX_DEPLOYMENT_TARGET",
    "IPHONEOS_DEPLOYMENT_TARGET",
    "TVOS_DEPLOYMENT_TARGET",
    "WATCHOS_DEPLOYMENT_TARGET",
    "XROS_DEPLOYMENT_TARGET",
    "DRIVERKIT_DEPLOYMENT_TARGET",
    // MSVC.
    "INCLUDE",
    "EXTERNAL_INCLUDE",
    "CL",
    "_CL_",
    "VCINSTALLDIR",
    "VCToolsInstallDir",
    "VCToolsVersion",
    "WindowsSdkDir",
    "WindowsSDKVersion",
    "UCRTVersion",
    "Platform",
];

/// Families of names that can reach a preprocessor without being on the
/// documented list: toolchain wrappers and vendor drivers read `*_INCLUDE*`,
/// `*SYSROOT*`, `*SDK*` and `*FLAGS` variables of their own. Keying a whole
/// family costs at most a spurious miss.
fn cc_memo_env_pattern_is_keyed(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    [
        "INCLUDE",
        "SYSROOT",
        "SDK",
        "DEPLOYMENT_TARGET",
        "CLANG",
        "GCC",
        "CCACHE",
    ]
    .iter()
    .any(|needle| upper.contains(needle))
        || upper.ends_with("FLAGS")
}

/// Is this a variable the memo key folds?
///
/// The memo identifies a preprocessor run, so the environment it keys is the
/// environment a preprocessor reads: the documented include, locale, SDK and
/// driver variables, plus families of names that vendor wrappers use. Folding
/// everything else made the memo unreachable in practice: a CI runner sets
/// per-run variables (`GITHUB_RUN_ID`, per-job temporary paths, tokens), so
/// no run could ever match the previous one and every warm build preprocessed
/// every translation unit again. kache's own `KACHE_*` settings steer the
/// wrapper, never the preprocessor, and are never keyed.
fn cc_memo_env_is_keyed(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    if name.starts_with("KACHE_") || CC_MEMO_VOLATILE_ENV.contains(&name) {
        return false;
    }
    // `DEP_<links>_<KEY>` is Cargo's `links` metadata for build scripts. No
    // compiler reads it; the build script turns it into argv, which is
    // keyed. Folding it split the memo between jobs whose dependency graphs
    // differ only in crates the C compile never sees.
    if name.starts_with("DEP_") {
        return false;
    }
    CC_MEMO_KEYED_ENV.contains(&name) || cc_memo_env_pattern_is_keyed(name)
}

/// Local identity for a preprocessor invocation. The environment is included
/// as selected by [`cc_memo_env_is_keyed`].
fn cc_preprocess_memo_key(
    parsed: &CcArgs,
    prefix_maps: &[CcPrefixMap],
    compiler_version: &str,
) -> Option<String> {
    let epoch = effective_source_date_epoch()?;
    let cwd = std::env::current_dir().ok()?;
    let compiler_path = super::resolve_program_on_path(&parsed.program)?;
    let compiler_metadata = std::fs::metadata(&compiler_path).ok()?;
    let mut hasher = blake3::Hasher::new();
    // v4: the probe now runs with the compile's prefix maps, and path-bound
    // expansions are never recorded. A v3 record may hold the mapped hash of
    // one of those, which would give another checkout its key (#1004).
    // v5: TUs whose expansion `.incbin`s or `.include`s a file are refused. A
    // v4 record of one would skip the probe that refuses it (#1015).
    fold_cc_memo_field(&mut hasher, b"schema", b"cc-preprocess-memo-v5");
    fold_cc_memo_field(
        &mut hasher,
        b"compiler-program",
        cc_memo_os_bytes(OsStr::new(&parsed.program)).as_slice(),
    );
    fold_cc_memo_field(
        &mut hasher,
        b"compiler-path",
        cc_memo_os_bytes(compiler_path.as_os_str()).as_slice(),
    );
    fold_cc_memo_field(
        &mut hasher,
        b"compiler-size",
        &compiler_metadata.len().to_le_bytes(),
    );
    fold_cc_memo_field(
        &mut hasher,
        b"compiler-mtime",
        &crate::cache_key::metadata_mtime_ns(&compiler_metadata).to_le_bytes(),
    );
    fold_cc_memo_field(
        &mut hasher,
        b"compiler-ctime",
        &crate::cache_key::metadata_ctime_ns(&compiler_metadata).to_le_bytes(),
    );
    fold_cc_memo_field(
        &mut hasher,
        b"compiler-inode",
        &crate::cache_key::metadata_inode(&compiler_metadata).to_le_bytes(),
    );
    fold_cc_memo_field(
        &mut hasher,
        b"compiler-version",
        compiler_version.as_bytes(),
    );
    // The mapped cwd, not the raw one: two checkouts of the same tree differ
    // here and nowhere the expansion can see, so folding the raw path gave
    // every worktree its own memo.
    fold_cc_memo_field(
        &mut hasher,
        b"cwd",
        cc_mapped_path(&cwd, prefix_maps).as_bytes(),
    );
    fold_cc_memo_field(
        &mut hasher,
        b"source-date-epoch",
        cc_memo_os_bytes(&epoch).as_slice(),
    );
    // Likewise the argv: `-I` and the source path carry the checkout root.
    // The maps are folded below, so two trees agree here only when they agree
    // about what the mapping means.
    for arg in build_preprocess_args(parsed) {
        let mapped = apply_cc_prefix_maps_to_bytes(arg.into_bytes(), prefix_maps);
        fold_cc_memo_field(&mut hasher, b"arg", &mapped);
    }
    // The source's own bytes, so two units that spell their source the same
    // way share nothing. Every tree-sitter grammar compiles `src/parser.c`
    // from its crate directory with the same flags, and with that directory
    // mapped to a sentinel their memos collided: each grammar found the
    // previous one's record, failed to validate it, and then ran the
    // preprocessor the memo exists to avoid. A source that cannot be read
    // keys as such; its compile fails on its own.
    for source in &parsed.sources {
        let content = crate::cache_key::hash_file(&absolutize_path(&cwd, source))
            .unwrap_or_else(|_| "unreadable".to_string());
        fold_cc_memo_field(&mut hasher, b"source-content", content.as_bytes());
    }
    // Targets only. The sources are the per-checkout roots this whole change
    // exists to keep out; the targets are the sentinels both trees share, and
    // they are what decides whether two mappings mean the same thing.
    // Sorted: the maps come ordered by source length, which is a property
    // of one checkout's paths, not of what the mapping means.
    let mut targets: Vec<&str> = prefix_maps.iter().map(|map| map.to.as_str()).collect();
    targets.sort_unstable();
    targets.dedup();
    for target in targets {
        fold_cc_memo_field(&mut hasher, b"prefix-to", target.as_bytes());
    }

    // Values through the same maps as the argv: a `cc`-crate build hands a
    // dependency's include directory down as `DEP_<links>_INCLUDE`, a path
    // under this build directory's target, and it must not give every build
    // directory its own memo any more than `-I` does.
    let mut environment: Vec<(Vec<u8>, Vec<u8>)> = std::env::vars_os()
        .filter(|(name, _)| cc_memo_env_is_keyed(name))
        .map(|(name, value)| {
            // Mapped as text, then encoded like every other memo field: on
            // Windows the field encoding is UTF-16 and the maps are UTF-8.
            let mapped = match value.to_str() {
                Some(text) => {
                    let mapped =
                        apply_cc_prefix_maps_to_bytes(text.as_bytes().to_vec(), prefix_maps);
                    cc_memo_os_bytes(OsStr::new(String::from_utf8_lossy(&mapped).as_ref()))
                }
                None => cc_memo_os_bytes(&value),
            };
            (cc_memo_os_bytes(&name), mapped)
        })
        .collect();
    environment.sort();
    for (name, value) in environment {
        fold_cc_memo_field(&mut hasher, b"env-name", &name);
        fold_cc_memo_field(&mut hasher, b"env-value", &value);
    }
    Some(hasher.finalize().to_hex().to_string())
}

fn cc_prefix_maps_for(parsed: &CcArgs, cwd: &Path) -> Vec<CcPrefixMap> {
    let cwd_abs = absolutize_path(cwd, cwd);
    let Some(source) = parsed.sources.first() else {
        return prefix_maps_from_roots([(cwd_abs, CC_BUILD_SENTINEL)]);
    };
    let source_abs = absolutize_path(cwd, source);
    let source_parent = source_abs
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| source_abs.clone());

    let mut roots: Vec<(PathBuf, &'static str)> = Vec::new();
    push_cwd_source_roots(&mut roots, &cwd_abs, &source_parent);

    let cwd_canon = canonicalize_or_self(&cwd_abs);
    let source_canon = canonicalize_or_self(&source_abs);
    let source_canon_parent = source_canon
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or(source_canon);
    if let Some(common) = common_ancestor(&cwd_canon, &source_canon_parent)
        && stable_cc_common_root(&common, &cwd_canon, &source_canon_parent)
    {
        roots.push((common, CC_ROOT_SENTINEL));
    }

    // Objdir-generated TUs (`Unified_cpp_*`, generated `.cpp`) live in the
    // build dir, so cwd ≈ source-dir and the roots above collapse to a
    // narrow objdir subdir — missing `__FILE__` paths into `dist/include`
    // and the source tree. The `-I` dirs span the repo, so fold them in:
    // the common ancestor of cwd and each include reaches the repo root.
    // `stable_cc_common_root`/`useful_cc_prefix` already drop the out-of-
    // tree ones — a system `-I` gives `/` (0 components) and `$HOME`-rooted
    // toolchain dirs give a 2-component ancestor, both below the ≥3 bound —
    // so only genuine in-tree roots survive. This is what makes
    // cross-checkout cc caching work automatically (no `KACHE_BASE_DIR`).
    for include in &parsed.includes {
        let include_abs = absolutize_path(cwd, include);
        for (a, b) in [
            (&cwd_abs, include_abs.clone()),
            (&cwd_canon, canonicalize_or_self(&include_abs)),
        ] {
            if let Some(common) = common_ancestor(a, &b)
                && stable_cc_common_root(&common, a, &b)
            {
                roots.push((common, CC_ROOT_SENTINEL));
            }
        }
    }

    prefix_maps_from_roots(roots)
}

/// Push the build (cwd) and source-dir roots, choosing the sentinel scheme by
/// TOPOLOGY rather than absolute location, so the same build converges
/// regardless of where its tree lives — the fix for out-of-tree build trees
/// (kunobi-ninja/kache#304, #394).
///
/// - **Nested** (one dir is under the other — the ordinary under-source build,
///   where the common ancestor IS the build or source dir): a single
///   `<CC_ROOT>` at the common ancestor is stable and preserves the
///   build↔source relative structure. Unchanged from before.
/// - **Sibling / out-of-tree** (the common ancestor is a *strict* ancestor of
///   both): the single-root scheme is fragile — whether the common ancestor is
///   judged "useful" depends on absolute properties (its depth, whether it is a
///   temp dir), so the SAME logical build picks `<CC_ROOT>` at one location and
///   split `<CC_BUILD>`/`<CC_SOURCE>` at another, and the cc key diverges.
///   Always use the split sentinels here (location-independent), and still fold
///   the shared root for paths under it (sibling includes) when it is a stable,
///   non-degenerate prefix. Prefix maps apply longest-first, so the split maps
///   (more specific) win for the build/source paths while `<CC_ROOT>` covers the
///   rest.
fn push_cwd_source_roots(
    roots: &mut Vec<(PathBuf, &'static str)>,
    cwd_abs: &Path,
    source_parent: &Path,
) {
    let common = common_ancestor(cwd_abs, source_parent);
    let nested = matches!(&common, Some(c) if c == cwd_abs || c == source_parent);
    if nested {
        if let Some(common) = common {
            roots.push((common, CC_ROOT_SENTINEL));
        }
    } else {
        roots.push((cwd_abs.to_path_buf(), CC_BUILD_SENTINEL));
        // Prefer the shared root over the source file's immediate parent. A
        // `<CC_ROOT>` at the common ancestor already strips the (clone-varying)
        // absolute prefix while PRESERVING the relative path below it
        // (e.g. `security/sandbox/chromium/base/location.cc`). The more-specific
        // `<CC_SOURCE>` at `source_parent` would win under longest-match and
        // collapse that parent directory to a flat sentinel — which gains no
        // cross-clone stability (the relative path is clone-invariant either
        // way) but BREAKS code that `static_assert`s on `__FILE__`, e.g.
        // Chromium's `base/location.cc`
        // (`StrEndsWith(__FILE__, …, "base/location.cc")`), failing the cold
        // Firefox build. Folding `<CC_ROOT>` keeps `__FILE__` ending in
        // `…/base/location.cc` so the assert holds.
        //
        // Gate on the common having a normal component — i.e. anything but a
        // bare filesystem root (`/`, a drive root) that would over-map unrelated
        // absolutes. This is a LOCATION-INDEPENDENT test, so the prefix-map
        // sentinel SET (hashed into the key) does not flip with absolute
        // location and an out-of-tree build still converges across machines / a
        // relocate (kunobi-ninja/kache#304, #394). Fall back to `<CC_SOURCE>`
        // only when there is no usable shared root, so a sibling source's
        // absolute path still cannot leak into the key.
        match common {
            Some(common) if has_normal_component(&common) => {
                roots.push((common, CC_ROOT_SENTINEL));
            }
            _ => {
                roots.push((source_parent.to_path_buf(), CC_SOURCE_SENTINEL));
            }
        }
    }
}

/// Whether `path` has at least one normal component — true for any real
/// directory, false only for a bare filesystem root (`/`) or a drive/UNC root.
/// Folding a bare root to a sentinel would collapse unrelated absolute paths.
fn has_normal_component(path: &Path) -> bool {
    path.components()
        .any(|c| matches!(c, std::path::Component::Normal(_)))
}

fn prefix_maps_from_roots<I>(roots: I) -> Vec<CcPrefixMap>
where
    I: IntoIterator<Item = (PathBuf, &'static str)>,
{
    let mut maps = Vec::new();
    for (root, to) in roots {
        let from = root.to_string_lossy().to_string();
        if from.is_empty() || maps.iter().any(|m: &CcPrefixMap| m.from == from) {
            continue;
        }
        maps.push(CcPrefixMap {
            from,
            to: to.to_string(),
        });
    }
    maps.sort_by_key(|m| std::cmp::Reverse(m.from.len()));
    maps
}

fn absolutize_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn canonicalize_or_self(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn common_ancestor(a: &Path, b: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for (left, right) in a.components().zip(b.components()) {
        if left != right {
            break;
        }
        out.push(left.as_os_str());
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

fn useful_cc_prefix(path: &Path) -> bool {
    path.components()
        .filter(|c| matches!(c, std::path::Component::Normal(_)))
        .count()
        >= 3
}

fn stable_cc_common_root(common: &Path, cwd: &Path, source_parent: &Path) -> bool {
    if common == cwd || common == source_parent {
        return true;
    }
    if common_is_temp_dir(common) {
        return false;
    }
    useful_cc_prefix(common) || common_is_below_temp_dir(common)
}

fn common_is_below_temp_dir(common: &Path) -> bool {
    let temp_dir = canonicalize_or_self(&std::env::temp_dir());
    let common = canonicalize_or_self(common);
    common != temp_dir && common.starts_with(temp_dir)
}

fn common_is_temp_dir(common: &Path) -> bool {
    canonicalize_or_self(common) == canonicalize_or_self(&std::env::temp_dir())
}

/// Substitute build-path prefixes with their targets in a byte buffer (resolved
/// `-###` tokens, preprocessor stdout) for the cache key.
///
/// SINGLE left-to-right pass: at each position the most-specific matching map
/// wins, its target is emitted, and the cursor skips past the source WITHOUT
/// re-scanning the emitted target. This is deliberate now that targets are real
/// absolute paths (`/proc/self/cwd`, `/kache/*`, kunobi-ninja/kache#485): a
/// naive per-map sequential replace could re-match an earlier map's target with
/// a later map's source (e.g. a pathological `KACHE_BASE_DIR=/proc/self`
/// rewriting the `/proc/self/cwd` just written), diverging the key from the
/// compiler's single-application `-ffile-prefix-map`. Single-pass matches the
/// compiler's semantics and is identical to the old behavior for the normal case
/// of non-overlapping absolute source prefixes.
/// One path in the spelling the prefix maps give it, lossily for non-UTF-8.
///
/// The same rewrite the expansion and the resolved tokens already get, so a
/// path recorded here reads the same from any checkout the maps cover.
fn cc_mapped_path(path: &Path, prefix_maps: &[CcPrefixMap]) -> String {
    // Text, not `cc_memo_os_bytes`: that encodes UTF-16 on Windows, and the
    // map sources and targets are UTF-8, so nothing would ever match and
    // every Windows memo missed. The maps are applied to argv strings the
    // same way.
    let text = path.to_string_lossy().into_owned().into_bytes();
    String::from_utf8_lossy(&apply_cc_prefix_maps_to_bytes(text, prefix_maps)).into_owned()
}

/// blake3 of a file's contents with the prefix maps applied.
///
/// The expansion is hashed this way, so an input has to be compared this way
/// too. A `-sys` crate's generated config header names its own build
/// directory: the raw bytes differ between two checkouts, the mapped bytes do
/// not, and the expansion each produces is identical. Comparing raw bytes
/// alone therefore made the memo stricter than the key it feeds. `None` when
/// the file cannot be read, which the caller treats as "cannot reuse".
/// Identity of a map set for the mapped-hash memo: every `from => to` pair,
/// ordered by source. Two invocations with the same pairs rewrite bytes the
/// same way, whatever order the pairs were derived in.
fn cc_prefix_maps_key(prefix_maps: &[CcPrefixMap]) -> String {
    let mut pairs: Vec<(&str, &str)> = prefix_maps
        .iter()
        .filter(|map| !map.from.is_empty())
        .map(|map| (map.from.as_str(), map.to.as_str()))
        .collect();
    pairs.sort_unstable();
    pairs.dedup();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache.cc.maps.v1\0");
    for (from, to) in pairs {
        hasher.update(from.as_bytes());
        hasher.update(b"\0");
        hasher.update(to.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().to_hex().to_string()
}

fn cc_mapped_content_hash(path: &Path, prefix_maps: &[CcPrefixMap]) -> Option<String> {
    let bytes = std::fs::read(path).ok()?;
    let mapped = apply_cc_prefix_maps_to_bytes(bytes, prefix_maps);
    Some(blake3::hash(&mapped).to_hex().to_string())
}

/// Candidate real paths a mapped spelling could name in this invocation.
///
/// The inverse of [`cc_mapped_path`], and inexact by nature: two roots can
/// share one sentinel, so this yields every prefix that could have produced
/// the recorded name, most specific first. Picking wrong is not a
/// correctness problem — the caller compares contents, so a candidate whose
/// bytes differ is a miss and the expansion is recomputed. A mapped path
/// with no matching sentinel yields nothing and the memo is skipped.
fn cc_unmapped_path_candidates(mapped: &str, prefix_maps: &[CcPrefixMap]) -> Vec<PathBuf> {
    let mut maps: Vec<&CcPrefixMap> = prefix_maps
        .iter()
        .filter(|map| !map.from.is_empty() && !map.to.is_empty())
        .collect();
    maps.sort_by_key(|map| std::cmp::Reverse(map.to.len()));

    let mut candidates: Vec<PathBuf> = Vec::new();
    for map in maps {
        if let Some(rest) = mapped.strip_prefix(map.to.as_str()) {
            let candidate = PathBuf::from(format!("{}{rest}", map.from));
            if !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }
    }
    // An absolute path outside every mapped root (a system header, the
    // toolchain) was recorded verbatim and needs no inverse.
    if candidates.is_empty() && Path::new(mapped).is_absolute() {
        candidates.push(PathBuf::from(mapped));
    }
    candidates
}

fn apply_cc_prefix_maps_to_bytes(bytes: Vec<u8>, prefix_maps: &[CcPrefixMap]) -> Vec<u8> {
    // Most-specific (longest source) first so it wins at any position where two
    // sources overlap. (`cc_prefix_maps` already sorts this way; re-sort here so
    // callers passing ad-hoc maps get the same precedence.)
    let mut maps: Vec<&CcPrefixMap> = prefix_maps.iter().filter(|m| !m.from.is_empty()).collect();
    maps.sort_by_key(|m| std::cmp::Reverse(m.from.len()));

    // A map can only start at a byte its `from` starts with, and every
    // `from` is an absolute path. Skipping to those bytes in bulk keeps a
    // 10 MB generated parser from costing sixty million prefix tests.
    let mut leading: Vec<u8> = maps.iter().filter_map(|m| m.from.bytes().next()).collect();
    leading.sort_unstable();
    leading.dedup();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i..].iter().position(|byte| leading.contains(byte)) {
            Some(0) => {}
            Some(offset) => {
                // The slice is taken from `next`, so a step that does not
                // move forward fails here instead of looping.
                let next = i + offset;
                out.extend_from_slice(&bytes[i..next]);
                i = next;
            }
            None => {
                out.extend_from_slice(&bytes[i..]);
                break;
            }
        }
        let matched = maps.iter().find(|m| {
            let from = m.from.as_bytes();
            bytes[i..].starts_with(from)
                && (!is_configured_cc_prefix_map(m)
                    || configured_cc_prefix_starts_path_token(&bytes, i, from))
        });
        if let Some(m) = matched {
            out.extend_from_slice(m.to.as_bytes());
            i += m.from.len();
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

fn is_configured_cc_prefix_map(map: &CcPrefixMap) -> bool {
    map.to
        .strip_prefix("/kache/base-dir-")
        .is_some_and(|index| !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit()))
}

fn configured_cc_prefix_starts_path_token(input: &[u8], start: usize, prefix: &[u8]) -> bool {
    if !cc_windows_absolute_prefix(prefix) && cc_follows_windows_drive_prefix(input, start) {
        return false;
    }
    let before = &input[..start];
    start == 0
        || before.ends_with(b"-I")
        || before.ends_with(b"-L")
        || before.ends_with(b"-F")
        || before.ends_with(b"-B")
        || before.last().is_some_and(|byte| {
            byte.is_ascii_whitespace()
                || matches!(
                    byte,
                    b'=' | b':' | b';' | b',' | b'"' | b'\'' | b'(' | b'[' | b'{' | b'@'
                )
        })
}

fn cc_windows_absolute_prefix(prefix: &[u8]) -> bool {
    (prefix.len() >= 3
        && prefix[0].is_ascii_alphabetic()
        && prefix[1] == b':'
        && matches!(prefix[2], b'/' | b'\\'))
        || prefix.starts_with(b"//")
        || prefix.starts_with(b"\\\\")
}

fn cc_follows_windows_drive_prefix(input: &[u8], start: usize) -> bool {
    if start < 2 || input[start - 1] != b':' || !input[start - 2].is_ascii_alphabetic() {
        return false;
    }
    let lead = &input[..start - 2];
    lead.is_empty()
        || lead.ends_with(b"-I")
        || lead.ends_with(b"-L")
        || lead.ends_with(b"-F")
        || lead.ends_with(b"-B")
        || lead.last().is_some_and(|byte| {
            byte.is_ascii_whitespace()
                || matches!(
                    byte,
                    b'=' | b':' | b';' | b',' | b'"' | b'\'' | b'(' | b'[' | b'{' | b'@'
                )
        })
}

/// The compiler receives the broadest map first and the most specific
/// last, mirroring rustc remap ordering. The byte-normalizer above
/// applies most-specific first.
fn file_prefix_map_args(prefix_maps: &[CcPrefixMap]) -> Vec<String> {
    prefix_maps
        .iter()
        .rev()
        .map(|m| format!("-ffile-prefix-map={}={}", m.from, m.to))
        .collect()
}

/// Compose the final argv for an `execute` invocation: the original
/// args with kache's `appended` flags placed *before* the `--`
/// end-of-options separator if one is present, otherwise at the end.
///
/// The clang / clang-cl driver treats every token after `--` as an
/// input file, not a flag — and cc-rs emits `--` before the source on
/// clang-cl invocations. Appending `-ffile-prefix-map=…` after that
/// separator makes the driver see the flags as extra source files,
/// producing `clang-cl: error: cannot specify '-Fo…' when compiling
/// multiple source files` (#300). Splicing them in ahead of `--` keeps
/// them classified as options. With no `--` present this is a plain
/// append, identical to the prior behaviour.
///
/// Splices before the *first* bare `--` — the only token clang/clang-cl/
/// gcc treat as the end-of-options marker (later `--` are inputs). It
/// matches `rest` literally, so a `--` that is some option's separated
/// value, or one hidden inside an `@response-file`, is not recognised;
/// both are out of scope for the cc-rs `-c` compiles that reach here.
fn compose_cc_args(rest: &[String], appended: Vec<String>) -> Vec<String> {
    if appended.is_empty() {
        return rest.to_vec();
    }
    match rest.iter().position(|a| a == "--") {
        Some(sep) => {
            let mut out = Vec::with_capacity(rest.len() + appended.len());
            out.extend_from_slice(&rest[..sep]);
            out.extend(appended);
            out.extend_from_slice(&rest[sep..]);
            out
        }
        None => {
            let mut out = rest.to_vec();
            out.extend(appended);
            out
        }
    }
}

fn cc_trace_name(parsed: &CcArgs) -> String {
    parsed
        .sources
        .first()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "cc".to_string())
}

/// Both halves of the shadowing digest, as the key computes them.
#[cfg(test)]
fn digest_cc_include_shadowing(parsed: &CcArgs, read_inputs: &[PathBuf]) -> Result<String> {
    digest_cc_shadowing_names(parsed, &cc_shadowing_names(parsed, read_inputs))
}

/// Digest the shadowing risk for the headers a preprocess actually read.
///
/// The walk below exists because a header appearing in an earlier include
/// directory can shadow one that was read, without changing any recorded
/// fingerprint. It answers that by listing everything that could exist. This
/// answers it by looking only at what could actually shadow: a new file can
/// only take the place of a header we read if it carries the **same relative
/// name** and sits **earlier in the search order** than the directory that
/// provided it.
///
/// So for each header read, resolve which user include directory first
/// provides its relative name and fold that position. A file appearing ahead
/// of it moves the position and therefore the key; a file appearing anywhere
/// else cannot shadow anything and is correctly ignored, where the walk would
/// have invalidated every unit naming that directory.
///
/// Costs one `stat` per candidate directory up to the first that provides the
/// name, rather than a full enumeration, so there is no cap to exceed. A stat
/// that fails for any reason other than absence fails closed, as the walk
/// does for an unreadable directory.
///
/// This half is the relative names a compile's read set could be shadowed
/// under, in the order the digest folds them. It depends on the read set
/// and the include directories alone, so the post-compile recheck reuses it
/// and only lists the directories again ([`digest_cc_shadowing_names`]).
fn cc_shadowing_names(
    parsed: &CcArgs,
    read_inputs: &[PathBuf],
) -> std::collections::BTreeSet<PathBuf> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let dirs = cc_user_include_dirs(parsed, &cwd);

    // One entry per distinct relative name: the same header read twice, or
    // two units reading it, resolve identically.
    let mut names: std::collections::BTreeSet<PathBuf> = std::collections::BTreeSet::new();
    for input in read_inputs {
        let absolute = absolutize_path(&cwd, input);
        // Two spellings could have reached this file, and the search order
        // is checked for both. The path relative to the MOST SPECIFIC user
        // directory containing it, which is what an `#include "sub/h.h"`
        // resolves through; directories nest, so the first match is not
        // necessarily the one that provided it. And the bare file name,
        // which is what an angle include resolves through and the only
        // spelling available for a header read from outside every user
        // directory. Folding both over-detects rather than under-detects:
        // an extra resolution can only cost a miss.
        let mut candidates: Vec<PathBuf> = Vec::new();
        if let Some(relative) = dirs
            .iter()
            .filter_map(|dir| absolute.strip_prefix(dir).ok())
            .min_by_key(|relative| relative.components().count())
        {
            candidates.push(relative.to_path_buf());
        }
        if let Some(file_name) = absolute.file_name() {
            candidates.push(PathBuf::from(file_name));
        }
        names.extend(candidates);
    }
    names
}

/// Digest where each of `names` is first provided in the include search
/// order, read from the directories as they are now.
fn digest_cc_shadowing_names(
    parsed: &CcArgs,
    names: &std::collections::BTreeSet<PathBuf>,
) -> Result<String> {
    digest_cc_shadowing_names_stamped(parsed, names).map(|(digest, _)| digest)
}

/// [`digest_cc_shadowing_names`], with the stamps of every directory it
/// listed when all of them can vouch for their listing (see
/// [`CcListingStamp`]), so a later check can confirm the digest without
/// listing again.
fn digest_cc_shadowing_names_stamped(
    parsed: &CcArgs,
    names: &std::collections::BTreeSet<PathBuf>,
) -> Result<(String, Option<CcListingStamps>)> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let dirs = cc_user_include_dirs(parsed, &cwd);
    let mut hasher = blake3::Hasher::new();
    let mut listings = CcDirectoryListings::default();
    for name in names {
        hasher.update(name.as_os_str().as_encoded_bytes());
        hasher.update(b"\x1f");
        match cc_first_include_dir_providing_cached(&dirs, name, &mut listings)? {
            Some(index) => {
                hasher.update(b"@");
                hasher.update(index.to_string().as_bytes());
            }
            None => {
                hasher.update(b"-");
            }
        }
        hasher.update(b"\n");
    }
    Ok((
        hasher.finalize().to_hex().to_string(),
        listings.stamps(&dirs),
    ))
}

/// What a directory looked like just before its listing was read: absent, or
/// present with this fingerprint. Creating, removing or renaming an entry
/// sets a directory's mtime to the current time, so an unchanged fingerprint
/// means an unchanged listing, provided the mtime was already older than the
/// read by more than any filesystem's timestamp granularity. A directory
/// modified more recently than that has no stamp and is listed again. The
/// fingerprint also holds the ctime, which moves when anything resets the
/// mtime by hand.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CcListingStamp {
    Absent,
    Present(crate::cache_key::FileFingerprint),
}

/// Each listed directory with its stamp.
type CcListingStamps = Vec<(PathBuf, CcListingStamp)>;

/// Two seconds covers the coarsest directory timestamps in use (FAT's).
const CC_LISTING_SETTLED_NS: i64 = 2_000_000_000;

/// Whether a directory modified at `mtime_ns` had settled by `read_at`:
/// modified more than [`CC_LISTING_SETTLED_NS`] before it.
fn cc_listing_settled(mtime_ns: i64, read_at: i64) -> bool {
    mtime_ns < read_at - CC_LISTING_SETTLED_NS
}

impl CcListingStamp {
    /// The stamp of `directory` now, or `None` when it cannot vouch for a
    /// listing read right after it.
    fn take(directory: &Path) -> Option<Self> {
        let read_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos();
        let read_at = i64::try_from(read_at).ok()?;
        match crate::cache_key::FileFingerprint::from_path(directory) {
            Ok(fingerprint) if cc_listing_settled(fingerprint.mtime_ns, read_at) => {
                Some(Self::Present(fingerprint))
            }
            Ok(_) => None,
            Err(_) => match std::fs::metadata(directory) {
                Err(error) if error.kind() == ErrorKind::NotFound => Some(Self::Absent),
                _ => None,
            },
        }
    }

    /// Whether `directory` still looks exactly as stamped.
    fn holds(&self, directory: &Path) -> bool {
        match self {
            Self::Absent => {
                std::fs::metadata(directory).is_err_and(|error| error.kind() == ErrorKind::NotFound)
            }
            Self::Present(fingerprint) => crate::cache_key::FileFingerprint::from_path(directory)
                .is_ok_and(|now| now == *fingerprint),
        }
    }
}

/// Index of the first user include directory that provides `name`, stopping
/// at the first hit. `None` when no directory does, which is itself a fact
/// worth keying: a directory later gaining the name changes it.
#[cfg(test)]
fn cc_first_include_dir_providing(dirs: &[PathBuf], name: &Path) -> Result<Option<usize>> {
    cc_first_include_dir_providing_cached(dirs, name, &mut CcDirectoryListings::default())
}

/// Directory listings read once per invocation. Resolving a few hundred
/// header names against a dozen include directories one `stat` at a time was
/// the largest cost of a warm C hit; listing each directory once answers the
/// same question from memory.
///
/// An include directory's own listing is found by its position in the search
/// order: most header names have no directory part, and hashing the joined
/// path for each (name, directory) pair was most of a C miss's CPU on
/// libgit2. A name with a directory part (`sys/types.h`) is listed by path.
#[derive(Default)]
struct CcDirectoryListings {
    listings: foldhash::HashMap<PathBuf, CcStampedListing>,
    by_position: Vec<Option<CcStampedListing>>,
}

struct CcDirectoryListing {
    names: foldhash::HashSet<std::ffi::OsString>,
    /// Lower-cased names, so a case-insensitive filesystem's answer can be
    /// confirmed with one `stat` instead of assumed from the exact spelling.
    /// `None` for a directory shown to be case-sensitive, where only the
    /// exact spelling resolves.
    folded: Option<foldhash::HashSet<String>>,
}

/// A listing, or its absence, with the stamp taken just before it was read.
struct CcStampedListing {
    listing: Option<CcDirectoryListing>,
    stamp: Option<CcListingStamp>,
}

impl CcStampedListing {
    fn read(directory: &Path) -> Result<Self> {
        let stamp = CcListingStamp::take(directory);
        Ok(Self {
            listing: CcDirectoryListing::read(directory)?,
            stamp,
        })
    }
}

impl CcDirectoryListing {
    /// `None` when the directory cannot be listed as a directory: absent, or
    /// an intermediate component is a file, which the compiler skips too.
    fn read(directory: &Path) -> Result<Option<Self>> {
        match std::fs::read_dir(directory) {
            Ok(entries) => {
                let mut names = foldhash::HashSet::default();
                for entry in entries {
                    names.insert(entry?.file_name());
                }
                let folded = (!Self::case_sensitive(directory, &names)).then(|| {
                    names
                        .iter()
                        .map(|name| name.to_string_lossy().to_lowercase())
                        .collect()
                });
                Ok(Some(Self { names, folded }))
            }
            Err(error)
                if error.kind() == ErrorKind::NotFound
                    || error.kind() == ErrorKind::NotADirectory =>
            {
                Ok(None)
            }
            Err(error) => anyhow::bail!(
                "cc include directory {} is unreadable ({error})",
                directory.display()
            ),
        }
    }

    /// Whether `directory` resolves only exact spellings: an entry's name
    /// with its ASCII letters case-swapped is not in the listing and does not
    /// resolve. Anything else, including a listing with no ASCII letter to
    /// swap, counts as case-insensitive.
    fn case_sensitive(directory: &Path, names: &foldhash::HashSet<std::ffi::OsString>) -> bool {
        let Some(swapped) = names.iter().find_map(|name| {
            let name = name.to_str()?;
            name.bytes().any(|b| b.is_ascii_alphabetic()).then(|| {
                name.chars()
                    .map(|c| {
                        if c.is_ascii_lowercase() {
                            c.to_ascii_uppercase()
                        } else {
                            c.to_ascii_lowercase()
                        }
                    })
                    .collect::<String>()
            })
        }) else {
            return false;
        };
        !names.contains(OsStr::new(&swapped))
            && std::fs::symlink_metadata(directory.join(&swapped))
                .is_err_and(|error| error.kind() == ErrorKind::NotFound)
    }

    /// Whether `directory`, which this lists, has an entry named `file_name`.
    fn provides(&self, directory: &Path, file_name: &OsStr) -> Result<bool> {
        if self.names.contains(file_name) {
            return Ok(true);
        }
        let Some(folded) = &self.folded else {
            return Ok(false);
        };
        // A case-insensitive filesystem (macOS, Windows, but also a casefold
        // ext4 or a mounted share on Linux) resolves `foo.h` to `Foo.h`; the
        // listing does not. When only the case differs, ask the filesystem,
        // which is what the compiler does.
        if folded.contains(&file_name.to_string_lossy().to_lowercase()) {
            return match std::fs::symlink_metadata(directory.join(file_name)) {
                Ok(_) => Ok(true),
                Err(error)
                    if error.kind() == ErrorKind::NotFound
                        || error.kind() == ErrorKind::NotADirectory =>
                {
                    Ok(false)
                }
                Err(error) => anyhow::bail!(
                    "cc include candidate {} is unreadable ({error})",
                    directory.join(file_name).display()
                ),
            };
        }
        Ok(false)
    }
}

impl CcDirectoryListings {
    /// Whether `directory` contains an entry named `file_name`, of any kind.
    fn contains(&mut self, directory: &Path, file_name: &OsStr) -> Result<bool> {
        let listing = match self.listings.entry(directory.to_path_buf()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(CcStampedListing::read(directory)?)
            }
        };
        match &listing.listing {
            Some(listing) => listing.provides(directory, file_name),
            None => Ok(false),
        }
    }

    /// [`Self::contains`] for the include directory at `position` in the
    /// search order, found without hashing its path.
    fn contains_at(
        &mut self,
        position: usize,
        directory: &Path,
        file_name: &OsStr,
    ) -> Result<bool> {
        if self.by_position.len() <= position {
            self.by_position.resize_with(position + 1, || None);
        }
        let slot = &mut self.by_position[position];
        if slot.is_none() {
            *slot = Some(CcStampedListing::read(directory)?);
        }
        match slot.as_ref().and_then(|stamped| stamped.listing.as_ref()) {
            Some(listing) => listing.provides(directory, file_name),
            None => Ok(false),
        }
    }

    /// Every directory listed so far with its stamp; `None` when any of them
    /// has none. `dirs` is the search order `by_position` indexes.
    fn stamps(&self, dirs: &[PathBuf]) -> Option<CcListingStamps> {
        let positioned = self
            .by_position
            .iter()
            .zip(dirs)
            .filter_map(|(slot, dir)| slot.as_ref().map(|stamped| (dir, stamped)));
        positioned
            .chain(self.listings.iter())
            .map(|(dir, stamped)| Some((dir.clone(), stamped.stamp.clone()?)))
            .collect()
    }
}

fn cc_first_include_dir_providing_cached(
    dirs: &[PathBuf],
    name: &Path,
    listings: &mut CcDirectoryListings,
) -> Result<Option<usize>> {
    let Some(file_name) = name.file_name() else {
        return Ok(None);
    };
    let parent = name
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    for (index, dir) in dirs.iter().enumerate() {
        let provided = match parent {
            Some(parent) => listings.contains(&dir.join(parent), file_name)?,
            None => listings.contains_at(index, dir, file_name)?,
        };
        if provided {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

/// Maximum file names walked across every user include dir. A partial
/// listing could miss the shadowing header the digest exists to catch,
/// so overflow is fail-closed passthrough rather than a truncated key.
const CC_INCLUDE_DIR_NAME_CAP: usize = 8192;

const CC_INCLUDE_DIR_NAME_EXTENSIONS: &[&str] = &[
    "h", "hh", "hpp", "hxx", "h++", "cuh", "c", "cc", "cpp", "cxx", "c++", "m", "mm", "i", "ii",
    "inl", "inc", "def", "pch", "gch",
];

fn digest_cc_include_dir_names(parsed: &CcArgs) -> Result<String> {
    digest_cc_include_dir_names_capped(parsed, CC_INCLUDE_DIR_NAME_CAP)
}

fn digest_cc_include_dir_names_capped(parsed: &CcArgs, cap: usize) -> Result<String> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let exempt = cc_system_include_dirs(parsed, &cwd);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"include_dir_names.v1\n");
    let mut seen = 0usize;
    for dir in cc_user_include_dirs(parsed, &cwd) {
        if exempt.iter().any(|root| dir.starts_with(root)) {
            continue;
        }
        hasher.update(b"dir\n");
        collect_include_dir_names(&dir, Path::new(""), &mut hasher, &mut seen, cap)?;
        hasher.update(b"enddir\n");
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn cc_user_include_dirs(parsed: &CcArgs, cwd: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut seen = HashSet::new();
    let mut push = |path: PathBuf| {
        let abs = absolutize_path(cwd, &path);
        if seen.insert(abs.clone()) {
            dirs.push(abs);
        }
    };
    if let Some(source) = parsed.sources.first() {
        let parent = source.parent().filter(|p| !p.as_os_str().is_empty());
        push(parent.map_or_else(|| cwd.to_path_buf(), Path::to_path_buf));
    }
    for value in cc_flag_dir_values(&parsed.rest, "-iquote") {
        push(PathBuf::from(value));
    }
    for include in &parsed.includes {
        push(include.clone());
    }
    dirs
}

fn cc_system_include_dirs(parsed: &CcArgs, cwd: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut seen = HashSet::new();
    let mut push = |path: PathBuf| {
        let abs = absolutize_path(cwd, &path);
        if seen.insert(abs.clone()) {
            dirs.push(abs);
        }
    };
    for value in cc_flag_dir_values(&parsed.rest, "-isystem") {
        push(PathBuf::from(value));
    }
    for value in cc_flag_dir_values(&parsed.rest, "-isysroot") {
        push(PathBuf::from(value));
        push(PathBuf::from(value).join("usr/include"));
    }
    if let Ok(sdk) = std::env::var("SDKROOT") {
        let path = PathBuf::from(sdk);
        if path.is_dir() {
            push(path.clone());
            push(path.join("usr/include"));
        }
    }
    dirs
}

fn cc_flag_dir_values<'a>(rest: &'a [String], flag: &'a str) -> Vec<&'a str> {
    let mut values = Vec::new();
    let mut args = rest.iter();
    while let Some(arg) = args.next() {
        let Some(suffix) = arg.strip_prefix(flag) else {
            continue;
        };
        if suffix.is_empty() {
            if let Some(value) = args.next() {
                values.push(value.as_str());
            }
        } else if let Some(value) = suffix.strip_prefix('=')
            && !value.is_empty()
        {
            values.push(value);
        }
    }
    values
}

fn collect_include_dir_names(
    dir: &Path,
    rel: &Path,
    hasher: &mut blake3::Hasher,
    seen: &mut usize,
    cap: usize,
) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            anyhow::bail!("cc include dir {} is unreadable ({error})", dir.display())
        }
    };

    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("reading {}", dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("stat {}", entry.path().display()))?;
        let name = entry.file_name();
        if file_type.is_symlink() {
            // Hash the name (it can shadow) but do not recurse; a
            // symlink dir can loop.
            if cc_include_name_counts(&name) {
                files.push(name);
            }
            continue;
        }
        if file_type.is_dir() {
            subdirs.push(name);
            continue;
        }
        if cc_include_name_counts(&name) {
            files.push(name);
        }
    }
    files.sort();
    subdirs.sort();

    for name in files {
        *seen += 1;
        if *seen > cap {
            anyhow::bail!("cc include dir name walk exceeded {cap} entries");
        }
        let path = rel.join(&name);
        hasher.update(path.as_os_str().as_encoded_bytes());
        hasher.update(b"\n");
    }
    for name in subdirs {
        *seen += 1;
        if *seen > cap {
            anyhow::bail!("cc include dir name walk exceeded {cap} entries");
        }
        collect_include_dir_names(&dir.join(&name), &rel.join(&name), hasher, seen, cap)?;
    }
    Ok(())
}

fn cc_include_name_counts(name: &OsStr) -> bool {
    let Some(ext) = Path::new(name).extension() else {
        return true;
    };
    let ext = ext.to_string_lossy();
    CC_INCLUDE_DIR_NAME_EXTENSIONS
        .iter()
        .any(|keep| ext.eq_ignore_ascii_case(keep))
}

struct PendingCcPreprocessMemo {
    memo_key: String,
    preprocessed_hash: String,
    fingerprints: Vec<crate::cache_key::CcPreprocessMemoInput>,
    /// The maps the expansion and the inputs were hashed under. Revalidation
    /// at commit time has to use the same ones.
    prefix_maps: Vec<CcPrefixMap>,
    /// The inputs came from a compile's own read set, fingerprinted after
    /// it ran, rather than from a preprocess that preceded the compile.
    captured: bool,
}

/// How the key learns what the translation unit reads.
///
/// The memo answers first in every mode. Without a memo the classic path
/// preprocesses (`-E`) to learn the read set and hash the expansion. The
/// deferrable path instead hands the decision back to the wrapper, which
/// compiles once with dependency capture and keys from what that compile
/// read; the captured read set then comes back through `Captured`.
pub(crate) enum CcKeyDiscovery {
    Expansion,
    Deferrable,
    Captured(CcCapturedInputs),
}

pub(crate) enum CcKeyOutcome {
    Key(String),
    /// No memo for this invocation: compile first, then key from the read
    /// set. Carries the memo identity so peers of the same unit coalesce.
    Deferred(CcDeferredKey),
}

pub(crate) struct CcDeferredKey {
    pub(crate) memo_key: String,
}

/// The files a compile with dependency capture read, fingerprinted the way
/// the memo records them (mapped name, content hash under the prefix maps).
pub(crate) struct CcCapturedInputs {
    fingerprints: Vec<crate::cache_key::CcPreprocessMemoInput>,
    /// The object spells a checkout root the prefix maps did not rewrite,
    /// so its key is bound to this checkout (see `hash_cc_expansion`).
    path_bound: bool,
    /// An input was written after the invocation started, as seen by the
    /// hasher that fingerprinted the captured read set. The wrapper's
    /// re-entry keys with a fresh hasher, so the bit travels here; an entry
    /// or memo must not describe what the compiler may not have read.
    inputs_changed: bool,
}

impl CcCapturedInputs {
    pub(crate) fn inputs_changed(&self) -> bool {
        self.inputs_changed
    }
}

/// Memo hash prefix for a path-bound read set: the digest is portable, the
/// key that uses it folds the reading checkout's roots as well.
const CC_MEMO_PATH_BOUND_PREFIX: &str = "pb:";

/// Whether the memo identity of this invocation is spelled entirely in
/// mapped or system paths. If a checkout root reaches the identity raw, a
/// record for the same unit may exist under another checkout's spelling, and
/// the miss is not certain enough to compile first.
fn cc_memo_identity_portable(parsed: &CcArgs, cwd: &Path, prefix_maps: &[CcPrefixMap]) -> bool {
    let portable = |path: &Path| {
        let absolute = absolutize_path(cwd, path);
        let raw = absolute.to_string_lossy().into_owned();
        let mapped = cc_mapped_path(&absolute, prefix_maps);
        // The build and source sentinels stand for "wherever this invocation
        // runs": which one a path lands under depends on the checkout's
        // layout, so a spelling under them is not one other checkouts share.
        (mapped != raw
            && !mapped.starts_with(CC_BUILD_SENTINEL)
            && !mapped.starts_with(CC_SOURCE_SENTINEL))
            || cc_system_path(&raw)
    };
    portable(cwd)
        && parsed.sources.iter().all(|source| portable(source))
        && parsed.includes.iter().all(|include| portable(include))
}

/// A path the toolchain owns, the same on every checkout of one machine.
fn cc_system_path(path: &str) -> bool {
    ["/usr/", "/opt/", "/Library/", "/Applications/", "/nix/"]
        .iter()
        .any(|root| path.starts_with(root))
}

/// One digest standing for the read set: what the memo would otherwise hold
/// as the expansion hash. Hashed over the mapped content of every file read
/// and nothing else, as the expansion was with `-P`: where a file sits, and
/// whether its directory could be mapped, does not enter, so two checkouts of
/// the same tree agree even under an unmappable root. The source is one of
/// the files, so include order and every textual choice are in it.
fn cc_direct_inputs_digest(fingerprints: &[crate::cache_key::CcPreprocessMemoInput]) -> String {
    // A toolchain file is the same bytes on every checkout and can spell a
    // directory a checkout happens to be under (glibc's `P_tmpdir`); its raw
    // hash is the portable one. A project file may spell its own checkout
    // root, which the maps make portable.
    let mut contents: Vec<&str> = fingerprints
        .iter()
        .map(|input| {
            if cc_system_path(input.local_path()) {
                input.content.as_str()
            } else {
                input.mapped.as_str()
            }
        })
        .collect();
    contents.sort_unstable();
    contents.dedup();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"kache.cc.direct-inputs.v2\0");
    hasher.update(&(contents.len() as u64).to_le_bytes());
    for content in contents {
        hasher.update(content.as_bytes());
        hasher.update(b"\n");
    }
    hasher.finalize().to_hex().to_string()
}

/// The memo's hash column for a read-set digest, flagged when path-bound.
fn cc_memo_hash(digest: &str, path_bound: bool) -> String {
    if path_bound {
        format!("{CC_MEMO_PATH_BOUND_PREFIX}{digest}")
    } else {
        digest.to_string()
    }
}

/// The digest and the path-bound flag a memo hash column carries.
fn cc_memo_hash_parts(hash: &str) -> (&str, bool) {
    match hash.strip_prefix(CC_MEMO_PATH_BOUND_PREFIX) {
        Some(digest) => (digest, true),
        None => (hash, false),
    }
}

/// Make target the compile-first dependency capture writes; never a real file.
const CC_DIRECT_CAPTURE_TARGET: &str = "__kache_direct_capture";

/// Whether the key may come from a compile-first read set instead of the
/// expansion: the invocation has the right shape and the driver is known
/// to accept the flags the capture adds.
pub(crate) fn cc_direct_key_eligible(parsed: &CcArgs) -> bool {
    cc_direct_key_shape_ok(parsed)
        && cc_driver_captures_dependencies(&parsed.program, &crate::config::probe_memo_dir())
}

/// Only the GNU dialect adds a private `-MD -MF`; a caller that asked for
/// `-MMD` gets a depfile without system headers, which is not a complete
/// read set, so it keeps the classic path.
fn cc_direct_key_shape_ok(parsed: &CcArgs) -> bool {
    parsed.family.dialect() == Dialect::Gnu
        && parsed.mode == CompileMode::Compile
        && parsed.sources.len() == 1
        && parsed
            .depinfo
            .as_ref()
            .is_none_or(|depinfo| !depinfo.emit || depinfo.include_system)
}

/// Whether `program` compiles an object while writing a usable dependency
/// file under the flags a compile-first run adds. The `-E` probe used to
/// vouch for the driver before any flag was injected; without it, this one
/// compile of an empty unit does, once per driver binary, remembered in
/// the probe memo directory.
fn cc_driver_captures_dependencies(program: &str, memo_dir: &Path) -> bool {
    let Some(digest) = cc_direct_probe_material(program) else {
        return false;
    };
    let path = crate::probe_memo::memo_path(memo_dir, "cc-direct-capture", "txt", &digest);
    if let Some(body) = crate::probe_memo::read_verified(&path, &digest) {
        return body.trim() == "ok";
    }
    let supported = {
        let _trace = crate::phase_trace::phase("cc_direct_probe");
        cc_direct_probe(program)
    };
    tracing::debug!("cc: probed {program} for compile-first capture: {supported}");
    crate::probe_memo::write_verified(&path, &digest, if supported { "ok" } else { "no" });
    supported
}

fn cc_direct_probe_material(program: &str) -> Option<String> {
    let resolved = super::resolve_program_on_path(program)?;
    let canonical = std::fs::canonicalize(&resolved).ok()?;
    let metadata = std::fs::metadata(&canonical).ok()?;
    let mut material = crate::probe_memo::Material::new("cc-direct-capture-v1");
    material.push(program.as_bytes());
    material.push(canonical.as_os_str().as_encoded_bytes());
    material.push(&metadata.len().to_le_bytes());
    material.push(&crate::cache_key::metadata_mtime_ns(&metadata).to_le_bytes());
    Some(material.digest())
}

fn cc_direct_probe(program: &str) -> bool {
    let Ok(dir) = tempfile::Builder::new()
        .prefix("kache-cc-direct-probe-")
        .tempdir()
    else {
        return false;
    };
    let root = dir.path();
    if fs::write(
        root.join("probe.c"),
        "int kache_direct_probe(void) { return 0; }\n",
    )
    .is_err()
    {
        return false;
    }
    let Some(prefix_map) = root
        .to_str()
        .map(|root| format!("-ffile-prefix-map={root}=/kache/probe"))
    else {
        return false;
    };
    let output = Command::new(program)
        .args([
            "-c",
            "probe.c",
            "-o",
            "probe.o",
            "-MD",
            "-MF",
            "probe.d",
            "-MT",
            CC_DIRECT_CAPTURE_TARGET,
            prefix_map.as_str(),
        ])
        .current_dir(root)
        // A kache shim standing in for the compiler must run it, not key it.
        .env("KACHE_CC_KEY_PROBE", "1")
        .output();
    let Ok(output) = output else {
        return false;
    };
    let depfile = fs::read_to_string(root.join("probe.d")).unwrap_or_default();
    let ok =
        output.status.success() && root.join("probe.o").is_file() && depfile.contains("probe.c");
    tracing::debug!(
        "cc: {program} compile-first capture probe: exit {} object {} depfile {}: {}",
        output.status,
        root.join("probe.o").is_file(),
        depfile.contains("probe.c"),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    ok
}

/// The first construct in any of `paths` that could make the assembler read
/// a file no fingerprint covers. The classic path scans the expansion; a
/// compile-first key never sees one, so it scans what the compile read.
#[cfg(test)]
fn cc_inputs_hide_assembler_input(paths: &[PathBuf]) -> Option<&'static str> {
    paths.iter().find_map(|path| {
        let bytes = fs::read(path).ok()?;
        cc_raw_assembler_hidden_input(&bytes)
    })
}

/// [`cc_assembler_hidden_input`] for unexpanded source text. The directive
/// and named-escape checks carry over: a directive the expansion would
/// contain is spelled in some input, macro pieces included, as the escaped
/// quote a stringised operand leaves. The computed-operand check does not:
/// in raw text an `asm` whose operand is not a literal is a macro
/// definition, as in every libc's symbol-aliasing headers, not a hidden
/// string.
fn cc_raw_assembler_hidden_input(text: &[u8]) -> Option<&'static str> {
    if let Some(found) = cc_assembler_text_hidden_input(text) {
        return Some(found);
    }
    let literals = cc_string_literals(text);
    if literals.named_escape {
        return Some(r"\N{...}");
    }
    cc_assembler_text_hidden_input(&literals.text)
}

#[derive(Default)]
pub struct CcCompiler {
    /// User-declared flags (issue #95) that kache's built-in allow-list
    /// doesn't model but the user opted into caching. A flag here stops
    /// refusing and is folded verbatim into the cache key. Empty in the
    /// common case (and for every existing `CcCompiler::new()` caller).
    extra_allowlist_flags: Vec<String>,
    /// Opt-in whole-program link caching (epic #762 / #259).
    cache_cc_links: bool,
    /// Deterministically ordered `[paths].base_dirs` roots applied to both the
    /// cc key probes and the real compiler invocation.
    base_dirs: Vec<String>,
    pending_preprocess_memo: RefCell<Option<PendingCcPreprocessMemo>>,
    /// The include digest folded into the key, so the publish-time recheck
    /// can reproduce it exactly.
    pending_include_dir_digest: RefCell<Option<PendingIncludeDirDigest>>,
    /// The last key bound itself to this checkout's roots. `execute` stores an
    /// object that embeds a raw root only under such a key.
    key_path_bound: Cell<bool>,
}

/// See [`CcCompiler::include_dir_names_still_match`].
struct PendingIncludeDirDigest {
    digest: String,
    /// The shadowing names the digest was computed over; `None` when the
    /// directories were walked.
    names: Option<std::collections::BTreeSet<PathBuf>>,
    /// Stamps of the directories the digest listed, when all could vouch
    /// for their listing.
    stamps: Option<CcListingStamps>,
}

const C_FAMILY_DRIVERS: [(&str, ToolFamily); 7] = [
    ("clang-cl", ToolFamily::ClangCl),
    ("clang++", ToolFamily::Clang),
    ("clang", ToolFamily::Clang),
    ("gcc", ToolFamily::Gnu),
    ("g++", ToolFamily::Gnu),
    ("c++", ToolFamily::Gnu),
    ("cc", ToolFamily::Gnu),
];

fn is_compiler_version_suffix(suffix: &str) -> bool {
    !suffix.is_empty()
        && suffix.split('.').all(|component| {
            !component.is_empty() && component.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn strip_compiler_qualifiers(mut name: &str) -> (&str, bool) {
    let mut removed_version = false;
    let mut removed_mingw_flavor = false;
    while let Some((head, suffix)) = name.rsplit_once('-') {
        if !removed_version && is_compiler_version_suffix(suffix) {
            removed_version = true;
            name = head;
        } else if !removed_mingw_flavor && matches!(suffix, "posix" | "win32") {
            removed_mingw_flavor = true;
            name = head;
        } else {
            break;
        }
    }
    (name, removed_mingw_flavor)
}

fn named_tool_family(name: &str) -> Option<ToolFamily> {
    let (base, removed_mingw_flavor) = strip_compiler_qualifiers(name);
    C_FAMILY_DRIVERS.iter().find_map(|(driver, family)| {
        let exact = base == *driver;
        let target_prefixed = base.strip_suffix(driver).is_some_and(|prefix| {
            prefix.strip_suffix('-').is_some_and(|target| {
                !target.is_empty() && target.bytes().any(|byte| byte.is_ascii_alphanumeric())
            })
        });
        if !exact && !target_prefixed {
            return None;
        }
        // `-posix` and `-win32` are MinGW GCC-compatible alternatives, not
        // generic suffixes that should make clang-family tools look compilable.
        if removed_mingw_flavor && *family != ToolFamily::Gnu {
            return None;
        }
        Some(*family)
    })
}

fn is_unresolvable_bare_program(program: &str) -> bool {
    if program.contains('/') {
        return false;
    }
    if program.contains('\\') {
        return false;
    }
    super::resolve_program_on_path(program).is_none()
}

impl CcCompiler {
    #[cfg(test)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a user-declared cc flag allow-list (issue #95),
    /// typically `config.cc_extra_allowlist_flags`.
    pub fn with_extra_allowlist_flags(extra_allowlist_flags: Vec<String>) -> Self {
        Self {
            extra_allowlist_flags,
            cache_cc_links: false,
            base_dirs: Vec::new(),
            pending_preprocess_memo: RefCell::new(None),
            pending_include_dir_digest: RefCell::new(None),
            key_path_bound: Cell::new(false),
        }
    }

    pub fn with_cache_cc_links(mut self, cache_cc_links: bool) -> Self {
        self.cache_cc_links = cache_cc_links;
        self
    }

    /// clang-cl's `-###` dump uses Windows path tokens; GNU/clang do not.
    fn cc_link_probe_is_windows_aware(family: ToolFamily) -> bool {
        family.dialect() != Dialect::Cl
    }

    fn cache_key_for_link(&self, parsed: &CcArgs, ctx: &KeyCtx<'_, '_>) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"cc_link_key_version:1\n");
        let program_name = Path::new(&parsed.program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(parsed.program.as_str());
        hasher.update(b"compiler:");
        hasher.update(program_name.as_bytes());
        hasher.update(b"\n");
        let config_args = parsed.config_args();
        let resolved = crate::probe::probe(
            ctx.cache_dir,
            &crate::probe::CcProber,
            &crate::probe::ProbeRequest {
                compiler: &parsed.program,
                args: &parsed.rest,
                key_args: &config_args,
                per_tu_paths: &[],
                windows_aware: Self::cc_link_probe_is_windows_aware(parsed.family),
            },
        )?;
        hasher.update(b"compiler_version:");
        hasher.update(resolved.version_line.as_bytes());
        hasher.update(b"\n");

        let expanded = expand_cc_response_files(&parsed.rest)?;
        hasher.update(b"argv:");
        for arg in &expanded {
            hasher.update(ctx.path_normalizer.normalize(arg).as_bytes());
            hasher.update(b"\x1f");
        }
        hasher.update(b"\n");

        let mut inputs = parsed.sources.clone();
        inputs.extend(cc_link_file_inputs(&expanded));
        inputs.sort();
        inputs.dedup();
        hasher.update(b"inputs:\n");
        for input in &inputs {
            let digest = ctx
                .file_hasher
                .hash(input)
                .with_context(|| format!("hashing link input {}", input.display()))?;
            hasher.update(digest.as_bytes());
            hasher.update(b" ");
            hasher.update(
                ctx.path_normalizer
                    .normalize(input.to_string_lossy())
                    .as_bytes(),
            );
            hasher.update(b"\n");
        }
        let key = hasher.finalize().to_hex().to_string();
        let key = crate::cache_key::apply_key_env_vars(key, ctx.key_env_vars, "cc-link");
        Ok(crate::cache_key::apply_key_salt(
            key,
            ctx.key_salt,
            "cc-link",
        ))
    }

    pub fn with_base_dirs(mut self, base_dirs: Vec<String>) -> Self {
        self.base_dirs = base_dirs;
        self.base_dirs.sort();
        self.base_dirs.dedup();
        self
    }

    /// Publish the dependency snapshot from a full preprocess only after a
    /// successful compile or cache restore revalidated every input.
    pub(crate) fn commit_preprocess_memo(&self, file_hasher: &crate::cache_key::FileHasher<'_>) {
        let Some(pending) = self.pending_preprocess_memo.borrow_mut().take() else {
            return;
        };
        let _trace = crate::phase_trace::phase("memo_commit");
        file_hasher.cc_preprocess_memo_record_if_unchanged(
            &pending.memo_key,
            &pending.preprocessed_hash,
            &pending.fingerprints,
            &|path| cc_mapped_content_hash(path, &pending.prefix_maps),
        );
    }

    /// The memo a deferred compile would publish, for a daemon to record in
    /// the wrapper's stead. The inputs were fingerprinted after the compile
    /// under the too-new guard, so a file written since the invocation
    /// started already kept the entry from being stored; nothing is left
    /// to revalidate. Only meaningful when the memo came from a capture:
    /// an expansion's inputs predate the compile and stay on the
    /// revalidating [`Self::commit_preprocess_memo`] path.
    pub(crate) fn captured_preprocess_memo(&self) -> Option<crate::daemon_publish::CcMemoHandoff> {
        let pending = self.pending_preprocess_memo.borrow();
        let pending = pending.as_ref()?;
        if !pending.captured {
            return None;
        }
        Some(crate::daemon_publish::CcMemoHandoff {
            memo_key: pending.memo_key.clone(),
            preprocessed_hash: pending.preprocessed_hash.clone(),
            inputs: pending.fingerprints.clone(),
        })
    }

    /// Forget the pending memo: a daemon took it.
    pub(crate) fn discard_preprocess_memo(&self) {
        self.pending_preprocess_memo.borrow_mut().take();
    }

    /// True when user include-dir names still match the digest folded into
    /// the key. A new shadowing header during compile must not be published
    /// under the old key.
    pub(crate) fn include_dir_names_still_match(&self, parsed: &CcArgs) -> bool {
        let pending = self.pending_include_dir_digest.borrow();
        let Some(pending) = pending.as_ref() else {
            return false;
        };
        // Directories whose stamps still hold list as they did for the key,
        // so the digest over them is unchanged.
        if let Some(stamps) = &pending.stamps
            && stamps.iter().all(|(dir, stamp)| stamp.holds(dir))
        {
            return true;
        }
        // Recompute the way the key did, over the same names. Resolving
        // against a different set of names than the key used would compare
        // two unrelated digests and refuse every store.
        let now = match &pending.names {
            Some(names) => digest_cc_shadowing_names(parsed, names),
            None => digest_cc_include_dir_names(parsed),
        };
        now.is_ok_and(|now| now == pending.digest)
    }

    /// Does this argv invoke a C-family compiler?
    ///
    /// Matches `cc`, `c++`, `gcc`, `g++`, `clang`, `clang++`, their
    /// versioned variants (`gcc-13`, `clang++-17`), and target-prefixed
    /// cross compilers (`arm-linux-gnueabihf-gcc`). Path-prefixed forms
    /// (`/usr/bin/cc`, `C:\path\clang.exe`) and Windows `.exe` suffixes
    /// are accepted.
    ///
    /// Owns its own detection rule; `super::detect_compiler` reaches it
    /// through this module's [`ADAPTER`] descriptor.
    pub fn recognizes(args: &[String]) -> bool {
        if super::is_workspace_wrapper_chain(args) {
            return false;
        }
        let Some(arg0) = args.first() else {
            return false;
        };
        let Some(name) = super::command_basename(arg0) else {
            return false;
        };
        let name = super::strip_windows_exe_suffix(name).to_ascii_lowercase();

        // Cross toolchains conventionally prefix the canonical driver with a
        // target triple. Match exact, versioned, target-prefixed, and supported
        // MinGW alternative names while leaving arbitrary companion suffixes
        // (`gcc-ar`, `gcc-nm`, `clang-format`) rejected.
        if named_tool_family(&name).is_some() {
            return true;
        }

        // zig cc wrappers generated by cargo-zigbuild (commonly used
        // for cross-compilation with glibc version pinning). These wrapper
        // scripts are named `zigcc-{target}.{glibc_ver}-{hash}.sh` and
        // delegate to `cargo-zigbuild zig cc -- ...`. Zig's cc is clang-based
        // (defines `__clang__`), so we treat it as a clang-family compiler.
        // See https://github.com/rust-cross/cargo-zigbuild.
        //
        // NOTE: A better long-term approach is dynamic compiler detection
        // via `-E` probing (as sccache does). That would cover *any*
        // cc-compatible wrapper regardless of its filename, rather than
        // maintaining a name-based allowlist. Tracked in follow-up.
        if name == "zigcc" || name.starts_with("zigcc-") {
            return true;
        }

        // ── Slow path: `-E` probe for unknown binaries ──
        if super::is_kache_subcommand_or_flag(&name) {
            return false;
        }
        if is_unresolvable_bare_program(arg0) {
            return false;
        }
        // A version/info query (e.g. Kani's `kani-compiler -vV`, #656) compiles
        // nothing, so there is nothing to cache — and *running* an unknown
        // program just to sniff its family would add a spurious invocation to a
        // pure passthrough. Leave it unrecognized so it passes through untouched.
        if super::is_version_or_info_query(&args[1..]) {
            return false;
        }

        crate::probe::probe_compiler_family(arg0).is_some()
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
    /// **Not a compiler adapter.** A probe is a non-compiler invocation
    /// pattern that happens to need passthrough. The dispatch in
    /// `run_wrapper_mode` checks this *before* the compiler match.
    pub fn recognizes_family_probe(args: &[String]) -> bool {
        args.len() >= 2 && args[0] == "-E"
    }
}

/// Does `key` name a `CC`/`CXX` compiler variable the `cc` crate reads?
///
/// Mirrors the crate's `getenv_with_target_prefixes("CC"|"CXX")`: the
/// bare name, a `<target>` suffix (`CC_aarch64_pc_windows_msvc`), or a
/// `TARGET_`/`HOST_` prefix. Deliberately excludes neighbours like
/// `CFLAGS`, `CXXFLAGS`, and `CCACHE_*` whose values are not
/// `<wrapper> <compiler>` pairs.
fn is_cc_family_env_key(key: &str) -> bool {
    let base = key
        .strip_prefix("TARGET_")
        .or_else(|| key.strip_prefix("HOST_"))
        .unwrap_or(key);
    base == "CC" || base == "CXX" || base.starts_with("CC_") || base.starts_with("CXX_")
}

/// Is `key` a C++ (`CXX`) compiler variable, as opposed to C (`CC`)?
fn is_cxx_env_key(key: &str) -> bool {
    let base = key
        .strip_prefix("TARGET_")
        .or_else(|| key.strip_prefix("HOST_"))
        .unwrap_or(key);
    base == "CXX" || base.starts_with("CXX_")
}

/// Does `token` (a path or bare name) refer to the kache binary itself?
fn probe_token_is_self(token: &str, self_stem: &str) -> bool {
    super::command_basename(token)
        .map(super::strip_windows_exe_suffix)
        .is_some_and(|name| name.eq_ignore_ascii_case(self_stem))
}

/// Recover the real compiler the `cc` crate dropped from a family probe.
///
/// When `CC="kache <compiler>"` the cc crate mis-parses it — kache is
/// not in the crate's hard-coded known-wrapper allowlist (`ccache`,
/// `sccache`, `distcc`, …), so it treats kache as the *compiler* and
/// `<compiler>` as a leading argument, then drops that argument when it
/// runs the family probe (`Command::new(path).arg("-E").arg(file)`).
/// kache therefore receives `kache -E <file>` with no compiler to
/// forward to.
///
/// The compiler is still recoverable: the very `CC`/`CXX` variable the
/// cc crate read still holds `kache <compiler>` in our environment.
/// Scan those variables, find the one whose first whitespace token is
/// us, and return `<compiler>` so the probe can forward to the real
/// thing — yielding the genuine compiler family instead of a wrong
/// default guess (issue #286: `cc` is absent on Windows MSVC, so the
/// old hard-coded `cc` forward failed and the build fell back to an
/// unsupported GNU family).
///
/// Selection mirrors the cc crate's own `getenv_with_target_prefixes`
/// precedence so kache forwards to the exact variable the crate read
/// when several are kache-wrapped (mozbuild sets a host *and* a target
/// compiler): for a given `<name>` in `CC`, then `CXX`, the order is
/// `<name>_<target>`, `<name>_<target-underscored>`, `TARGET_<name>`,
/// `<name>`, `HOST_<name>`. `target` comes from cargo's `TARGET` env
/// var (set for build scripts). When `target` is `None`, selection
/// falls back to a deterministic order (CC before CXX, then the
/// lexicographically smallest key) so it never depends on environment
/// iteration order.
///
/// `CC` is preferred over `CXX` because the probe file is C and kache
/// cannot tell from `-E <file>` alone whether the cc crate's probe
/// belongs to a C or C++ `Build`. When `CC` and `CXX` are kache-wrapped
/// with *different* compiler families this can mislabel a C++ probe —
/// harmless in practice (the cc crate treats GNU and Clang identically;
/// only MSVC diverges, and a kache-wrapped MSVC `CXX` paired with a
/// non-MSVC `CC` does not occur in real toolchains).
///
/// Returns `None` when no kache-wrapped compiler variable is present.
pub(crate) fn resolve_probe_compiler<I>(
    self_stem: &str,
    target: Option<&str>,
    env_vars: I,
) -> Option<String>
where
    I: IntoIterator<Item = (String, String)>,
{
    // Collect every kache-wrapped CC/CXX variable: key -> real compiler.
    let mut wrapped: HashMap<String, String> = HashMap::new();
    for (key, value) in env_vars {
        if !is_cc_family_env_key(&key) {
            continue;
        }
        let mut tokens = value.split_whitespace();
        let Some(first) = tokens.next() else { continue };
        // The first token must be us; otherwise this is a plain
        // compiler, not a kache-wrapped one.
        if !probe_token_is_self(first, self_stem) {
            continue;
        }
        let Some(real) = tokens.next() else { continue };
        // Guard against a degenerate `CC="kache kache"`.
        if probe_token_is_self(real, self_stem) {
            continue;
        }
        wrapped.entry(key).or_insert_with(|| real.to_string());
    }
    if wrapped.is_empty() {
        return None;
    }

    // cc-crate precedence: most-specific target var first, CC before CXX.
    for name in ["CC", "CXX"] {
        if let Some(t) = target {
            if let Some(c) = wrapped.get(&format!("{name}_{t}")) {
                return Some(c.clone());
            }
            let underscored = t.replace('-', "_");
            if underscored != t
                && let Some(c) = wrapped.get(&format!("{name}_{underscored}"))
            {
                return Some(c.clone());
            }
            if let Some(c) = wrapped.get(&format!("TARGET_{name}")) {
                return Some(c.clone());
            }
        }
        if let Some(c) = wrapped.get(name) {
            return Some(c.clone());
        }
        if let Some(c) = wrapped.get(&format!("HOST_{name}")) {
            return Some(c.clone());
        }
    }

    // No precedence key matched (e.g. only a target-suffixed var for an
    // unknown target): deterministic fallback — CC family before CXX,
    // then the lexicographically smallest key.
    let mut keys: Vec<&String> = wrapped.keys().collect();
    keys.sort_by(|a, b| {
        is_cxx_env_key(a)
            .cmp(&is_cxx_env_key(b))
            .then_with(|| a.cmp(b))
    });
    keys.first().map(|k| wrapped[*k].clone())
}

impl Compiler for CcCompiler {
    type Parsed = CcArgs;

    fn id(&self) -> CompilerId {
        CC_ID
    }

    fn parse(&self, args: &[String]) -> Result<CcArgs> {
        CcArgs::parse(args)
    }

    fn refuse_reasons(&self, parsed: &CcArgs) -> Vec<RefuseReason> {
        // Per-case detection from the parsed shape. The skeleton
        // catch-all is gone — single-source `-c` compiles with no
        // unsafe flags now produce an EMPTY refuse list, which is the
        // signal to the wrapper that this invocation is cacheable.
        let mut reasons = parsed.refuse_reasons(&self.extra_allowlist_flags);
        if self.cache_cc_links && parsed.mode == CompileMode::Link {
            // Link mode short-circuits CcArgs::refuse_reasons before
            // feature checks such as `@file`, so only the link-mode
            // reason is present to strip. expand_cc_response_files
            // still folds `@file` contents into the key.
            reasons.retain(|reason| match reason {
                RefuseReason::Unsupported(detail) => !detail.contains("cc link mode"),
                _ => true,
            });
            if parsed.output.is_none() {
                reasons.push(RefuseReason::Unsupported(
                    "cc link mode without -o — not cacheable",
                ));
            }
        }
        reasons
    }

    fn cache_key(&self, parsed: &CcArgs, ctx: &KeyCtx<'_, '_>) -> Result<String> {
        match self.cache_key_with(parsed, ctx, CcKeyDiscovery::Expansion)? {
            CcKeyOutcome::Key(key) => Ok(key),
            CcKeyOutcome::Deferred(_) => {
                anyhow::bail!("cc key deferred without a compile-first caller")
            }
        }
    }

    fn execute(&self, parsed: &CcArgs) -> Result<CompileResult> {
        self.execute_with_extra_args(parsed, Vec::new(), false)
            .map(|(result, _)| result)
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

impl CcCompiler {
    /// The key, or the request to compile first when `discovery` allows it
    /// and no memo describes this invocation's read set.
    pub(crate) fn cache_key_with(
        &self,
        parsed: &CcArgs,
        ctx: &KeyCtx<'_, '_>,
        discovery: CcKeyDiscovery,
    ) -> Result<CcKeyOutcome> {
        if parsed.mode == CompileMode::Link {
            return self.cache_key_for_link(parsed, ctx).map(CcKeyOutcome::Key);
        }
        // Preconditions (guaranteed by the wrapper checking
        // refuse_reasons first): `-c` mode, exactly one source.
        self.pending_preprocess_memo.borrow_mut().take();
        self.key_path_bound.set(false);
        let _trace = crate::phase_trace::phase("key");
        let mut hasher = blake3::Hasher::new();
        let trace_name = cc_trace_name(parsed);
        let prefix_maps = {
            let _trace = crate::phase_trace::phase("cc_prefix_maps");
            cc_prefix_maps(parsed, &self.base_dirs)
        };
        for map in &prefix_maps {
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] prefix_map {} => {}",
                trace_name,
                map.from,
                map.to
            );
        }

        hasher.update(b"cc_key_version:");
        hasher.update(crate::cache_key::CACHE_KEY_VERSION.to_string().as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] cc_key_version={}",
            trace_name,
            crate::cache_key::CACHE_KEY_VERSION
        );

        if !self.base_dirs.is_empty() {
            hasher.update(b"configured_base_dirs.v1:");
            hasher.update(self.base_dirs.len().to_string().as_bytes());
            hasher.update(b"\n");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] configured_base_dirs={}",
                trace_name,
                self.base_dirs.len()
            );
        }

        let mut prefix_sentinels: Vec<&str> = Vec::new();
        for map in &prefix_maps {
            if !prefix_sentinels.contains(&map.to.as_str()) {
                prefix_sentinels.push(map.to.as_str());
            }
        }
        prefix_sentinels.sort_unstable();

        // Expansions that spell out a checkout root are hashed raw (see
        // `hash_cc_expansion`). Naming the scheme retires entries stored before
        // it, which mapped those roots to a sentinel and could hand another
        // checkout an object naming this one (kunobi-ninja/kache#1004).
        hasher.update(b"expansion_roots:literal-bound.v1\n");

        hasher.update(b"prefix_maps:");
        for sentinel in prefix_sentinels {
            hasher.update(sentinel.as_bytes());
            hasher.update(b"\x1f");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] cc_prefix_map={}",
                trace_name,
                sentinel
            );
        }
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
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] compiler={}",
            trace_name,
            program_name
        );
        // Compiler probe, memoized: the version line (`cc --version`,
        // compiler identity) and the resolved invocation (`cc -###`,
        // the driver's fully-expanded `-cc1` line). One probe per build
        // per flag set; the rest of the build reads the record.
        let config_args = parsed.config_args();
        // Per-TU paths to blank from the shared probe record's resolved
        // tokens, so the record is invariant across the build's TUs and
        // parallel builds don't race on whose paths it holds (#keyrace).
        let per_tu_paths = cc_resolved_per_tu_paths(parsed);
        let trace_probe = crate::phase_trace::phase("cc_probe");
        let resolved = crate::probe::probe(
            ctx.cache_dir,
            &crate::probe::CcProber,
            &crate::probe::ProbeRequest {
                compiler: &parsed.program,
                args: &parsed.rest,
                key_args: &config_args,
                per_tu_paths: &per_tu_paths,
                // Sentinel Windows paths only for gnu/clang (objects are
                // remapped via -ffile-prefix-map). clang-cl keeps raw
                // native paths → key stays path-literal (#299/#312).
                windows_aware: parsed.family.dialect() != Dialect::Cl,
            },
        )?;
        drop(trace_probe);
        if resolved.resolved_tokens.is_none() && cc_flags_need_resolved_invocation(parsed) {
            anyhow::bail!("cc: resolved invocation unavailable for probe-captured flags");
        }
        hasher.update(b"compiler_version:");
        hasher.update(resolved.version_line.as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] compiler_version={}",
            trace_name,
            resolved.version_line
        );

        // Resolved compiler invocation: the `cc -###` `-cc1` line with
        // host-local paths sentinelled. Captures codegen the modeled
        // flags below miss — compiler defaults (`-mrelocation-model`,
        // `-ffp-contract`, the resolved `-target-cpu` and feature set).
        // If `-###` cannot be resolved, we can only proceed when no
        // accepted flag relies on those resolved tokens for safety.
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
                // Resolved `cc -###` tokens carry absolute build paths —
                // `-I` dirs, `-D NAME="/abs/.../foo.ico"` defines, input /
                // `-o` paths — that embed the build directory. Hashing them
                // raw makes the key path-dependent, so two builds of the
                // same TU at different paths (a teammate's checkout, a CI
                // runner, the bench's cross-clone warm phase) miss. Run them
                // through the SAME prefix maps as the preprocessor stdout so
                // the build root collapses to `<CC_ROOT>`/`<CC_BUILD>` and
                // the key is path-portable. Mapping only ever merges keys
                // that differ solely in build path (same object, remapped at
                // compile time via `-ffile-prefix-map`) — never a miscache.
                let mapped = apply_cc_prefix_maps_to_bytes(tok.clone().into_bytes(), &prefix_maps);
                hasher.update(&mapped);
                hasher.update(b"\x1f");
                tracing::trace!(
                    target: "kache::cache_key",
                    "[key:{}] resolved_token={}",
                    trace_name,
                    String::from_utf8_lossy(&mapped)
                );
            }
            hasher.update(b"\n");
        }

        // Target architecture.
        let arch = cc_target_arch(parsed);
        hasher.update(b"arch:");
        hasher.update(arch.as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] arch={}",
            trace_name,
            arch
        );

        // Codegen-affecting flags. These are partly redundant with
        // the preprocessor hash (defines affect macro expansion,
        // -std gates language features) but the redundancy is cheap
        // and defends against e.g. -std affecting codegen without
        // changing the expanded text.
        if let Some(opt) = parsed.optimization {
            hasher.update(b"opt:");
            hasher.update(format!("{opt:?}").as_bytes());
            hasher.update(b"\n");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] opt={opt:?}",
                trace_name
            );
        }
        if let Some(dbg) = parsed.debug_level {
            hasher.update(b"debug:");
            hasher.update(&[dbg]);
            hasher.update(b"\n");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] debug={dbg}",
                trace_name
            );
        }
        // clang-cl debug objects embed per-TU path inputs the rest of
        // the key misses: the source path/filename and output (-Fo) name
        // (both stripped from config_args, so the memoized `-###` tokens
        // can't be trusted for them) and the compilation dir (CWD, not in
        // the key at all). Fold them so distinct objects never share a
        // key — capture, not remap (clang-cl is path-literal; #299/#312).
        if let Some(paths) = cl_debug_path_inputs(parsed) {
            hasher.update(b"cl_debug_paths:");
            for p in &paths {
                hasher.update(p.as_bytes());
                hasher.update(b"\x1f");
                tracing::trace!(
                    target: "kache::cache_key",
                    "[key:{}] cl_debug_path={}",
                    trace_name,
                    p
                );
            }
            hasher.update(b"\n");
        }
        if let Some(std) = &parsed.std {
            hasher.update(b"std:");
            hasher.update(std.as_bytes());
            hasher.update(b"\n");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] std={}",
                trace_name,
                std
            );
        }
        hasher.update(b"pic:");
        hasher.update(&[parsed.pic as u8]);
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] pic={}",
            trace_name,
            parsed.pic
        );

        // Built-in raw-keyed flags have object effects the resolved compiler
        // probe does not expose (notably GNU assembler-only `-Wa` options).
        // Hash their normalized argument bytes in argv order; a length prefix
        // keeps adjacent arguments unambiguous without assuming any character
        // cannot appear in an argv element.
        let raw_flags = cc_raw_flags_for_key(parsed, &prefix_maps);
        if !raw_flags.is_empty() {
            hasher.update(b"cc_raw_flags:");
            for flag in raw_flags {
                hasher.update(&(flag.len() as u64).to_le_bytes());
                hasher.update(&flag);
                tracing::trace!(
                    target: "kache::cache_key",
                    "[key:{}] cc_raw_flag={}",
                    trace_name,
                    String::from_utf8_lossy(&flag)
                );
            }
            hasher.update(b"\n");
        }

        // User-declared cc flags (issue #95). The built-in table doesn't
        // model these; the user opted them into caching via
        // `[cc] extra_allowlist_flags`. kache can't know how each affects
        // codegen, so it folds the flag string *verbatim* — a different
        // flag (or value) is a different string, hence a different key
        // (never a miscache by value). Only flags actually present on the
        // command line are folded (an unused allow-list entry has no
        // codegen effect and must not move the key), sorted + deduped so
        // argv order and repeats don't perturb the key.
        let matched = cc_extra_flags_for_key(parsed, &self.extra_allowlist_flags);
        if !matched.is_empty() {
            hasher.update(b"cc_extra_flags:");
            for flag in matched {
                hasher.update(flag.as_bytes());
                hasher.update(b"\x1f");
                tracing::trace!(
                    target: "kache::cache_key",
                    "[key:{}] cc_extra_flag={}",
                    trace_name,
                    flag
                );
            }
            hasher.update(b"\n");
        }

        // The object bytes do not depend on dep-info flags, but the cached
        // artifact set now can include a `.d` sidecar. Key the dep-info
        // content shape so an object-only entry never satisfies an invocation
        // that expects dependency output, and so flags like `-MD` vs `-MMD`
        // or `-MT` do not share incompatible sidecars.
        // Every field folded here is also emitted to the `kache::cache_key`
        // trace target so `KACHE_E2E_KEYTRACE` can attribute a cross-clone miss
        // to dep-info (these were previously folded but untraced, leaving such
        // divergences invisible in the keytrace diff).
        hasher.update(b"depinfo:");
        if let Some(depinfo) = parsed.depinfo.as_ref().filter(|d| d.emit) {
            hasher.update(b"1\n");
            hasher.update(b"depinfo_include_system:");
            hasher.update(&[depinfo.include_system as u8]);
            hasher.update(b"\n");
            hasher.update(b"depinfo_phony_targets:");
            hasher.update(&[depinfo.phony_targets as u8]);
            hasher.update(b"\n");
            hasher.update(b"depinfo_missing_generated:");
            hasher.update(&[depinfo.missing_generated as u8]);
            hasher.update(b"\n");
            // `depinfo_target` is the make target from `-MT` (else the object
            // file *name*, basename-only). It is hashed raw — keep an eye on it
            // in the trace: a build-path-bearing `-MT` would leak here.
            let depinfo_target: std::borrow::Cow<str> = if let Some(target) = &depinfo.target {
                std::borrow::Cow::Borrowed(target.as_str())
            } else if let Some(object) = parsed.object_output_path()
                && let Some(name) = object.file_name()
            {
                std::borrow::Cow::Owned(name.to_string_lossy().into_owned())
            } else {
                std::borrow::Cow::Borrowed("")
            };
            hasher.update(b"depinfo_target:");
            hasher.update(depinfo_target.as_bytes());
            hasher.update(b"\n");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] depinfo=1 include_system={} phony_targets={} missing_generated={} target={}",
                trace_name,
                depinfo.include_system,
                depinfo.phony_targets,
                depinfo.missing_generated,
                depinfo_target
            );
        } else {
            hasher.update(b"0\n");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] depinfo=0",
                trace_name
            );
        }

        // Preprocessor expansion — the load-bearing input. Captures
        // the source plus every transitively-included header plus
        // macro expansion. `-E -P` strips line markers so header
        // PATHS don't leak (cross-machine portable); SOURCE_DATE_EPOCH
        // pins __DATE__/__TIME__ (stable across builds).
        let trace_memo_key = crate::phase_trace::phase("cc_memo_key");
        let memo_key = ctx
            .file_hasher
            .supports_cc_preprocess_memo()
            .then(|| cc_preprocess_memo_key(parsed, &prefix_maps, &resolved.version_line))
            .flatten();
        drop(trace_memo_key);
        // The read set comes back with the expansion, from the memo when it
        // answers and from the dependency capture otherwise. It is what makes
        // the shadowing resolution below possible.
        let (pp_hash, read_inputs) = if let CcKeyDiscovery::Captured(captured) = discovery {
            // The compile already ran and this is what it read: the digest
            // of that read set stands where the expansion hash would, and
            // the memo records it under the same identity.
            let digest = cc_direct_inputs_digest(&captured.fingerprints);
            self.key_path_bound.set(captured.path_bound);
            let read_inputs = captured
                .fingerprints
                .iter()
                .map(|input| PathBuf::from(input.local_path()))
                .collect::<Vec<_>>();
            if let Some(memo_key) = memo_key {
                self.pending_preprocess_memo
                    .borrow_mut()
                    .replace(PendingCcPreprocessMemo {
                        memo_key,
                        preprocessed_hash: cc_memo_hash(&digest, captured.path_bound),
                        fingerprints: captured.fingerprints,
                        prefix_maps: prefix_maps.clone(),
                        captured: true,
                    });
            }
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] preprocessed_memo=captured",
                trace_name
            );
            (digest, Some(read_inputs))
        } else if let Some((memo_hash, satisfied)) = memo_key.as_ref().and_then(|key| {
            let _trace = crate::phase_trace::phase("cc_memo_lookup");
            ctx.file_hasher.cc_preprocess_memo_lookup(
                key,
                |name| cc_unmapped_path_candidates(name, &prefix_maps),
                &|path| cc_mapped_content_hash(path, &prefix_maps),
            )
        }) {
            let (memo_hash, path_bound) = cc_memo_hash_parts(&memo_hash);
            self.key_path_bound.set(path_bound);
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] preprocessed_memo=hit path_bound={path_bound}",
                trace_name
            );
            (memo_hash.to_string(), Some(satisfied))
        } else if let (CcKeyDiscovery::Deferrable, Some(memo_key)) = (&discovery, &memo_key)
            && cc_direct_key_eligible(parsed)
            && std::env::current_dir()
                .is_ok_and(|cwd| cc_memo_identity_portable(parsed, &cwd, &prefix_maps))
            && !ctx.file_hasher.cc_preprocess_memo_recorded(memo_key)
        {
            // Nothing recorded for this invocation: the compile has to run,
            // so let it run first and key from what it read. A memo that
            // exists but no longer matches keeps the preprocess below, which
            // can still find the entry an earlier state of the tree left.
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] preprocessed_memo=none deferred",
                trace_name
            );
            return Ok(CcKeyOutcome::Deferred(CcDeferredKey {
                memo_key: memo_key.clone(),
            }));
        } else {
            let _trace = crate::phase_trace::phase("cc_preprocess");
            let preprocessed =
                preprocess_hash(parsed, &prefix_maps, ctx.file_hasher, memo_key.is_some())?;
            self.key_path_bound.set(preprocessed.path_bound);
            let read_inputs = preprocessed.fingerprints.as_ref().map(|inputs| {
                inputs
                    .iter()
                    .map(|input| PathBuf::from(input.local_path()))
                    .collect::<Vec<_>>()
            });
            // With the read set in hand the key is its digest, the same key a
            // compile-first run derives, so the two paths find each other's
            // entries. The expansion hash stands in only when the dependency
            // capture gave nothing to fingerprint.
            let hash = match &preprocessed.fingerprints {
                Some(fingerprints) => cc_direct_inputs_digest(fingerprints),
                None => preprocessed.hash.clone(),
            };
            if let (Some(memo_key), Some(fingerprints)) = (memo_key, preprocessed.fingerprints) {
                self.pending_preprocess_memo
                    .borrow_mut()
                    .replace(PendingCcPreprocessMemo {
                        memo_key,
                        preprocessed_hash: cc_memo_hash(&hash, preprocessed.path_bound),
                        fingerprints,
                        prefix_maps: prefix_maps.clone(),
                        captured: false,
                    });
            }
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] preprocessed_memo=miss",
                trace_name
            );
            (hash, read_inputs)
        };
        hasher.update(b"preprocessed:");
        hasher.update(pp_hash.as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] preprocessed={}",
            trace_name,
            pp_hash
        );
        // A read set whose object spells a checkout root is keyed to that
        // checkout: the digest is portable, the roots folded here are not,
        // which is the point. Another checkout reading the same memo folds
        // its own roots and misses.
        if self.key_path_bound.get() {
            let mut roots: Vec<&str> = prefix_maps
                .iter()
                .map(|map| map.from.as_str())
                .filter(|from| !from.is_empty())
                .collect();
            roots.sort_unstable();
            roots.dedup();
            hasher.update(b"path_bound_roots:");
            for root in &roots {
                hasher.update(root.as_bytes());
                hasher.update(b"\0");
            }
            hasher.update(b"\n");
            tracing::trace!(
                target: "kache::cache_key",
                "[key:{}] path_bound_roots={}",
                trace_name,
                roots.len()
            );
        }

        // Shadowing. A header appearing in an earlier `-I`/`-iquote` dir (or
        // next to the source) can take the place of one that was read without
        // changing any fingerprint, so the key has to notice it. With the read
        // set in hand this resolves only the names that could actually be
        // shadowed; without it (a compiler whose dependency capture failed)
        // fall back to enumerating the directories, which is what the capped
        // walk has always done.
        let trace_shadowing = crate::phase_trace::phase("cc_shadowing");
        let names = read_inputs
            .as_deref()
            .map(|inputs| cc_shadowing_names(parsed, inputs));
        let (include_dir_digest, stamps, include_dir_mode) = match &names {
            Some(names) => {
                let (digest, stamps) = digest_cc_shadowing_names_stamped(parsed, names)?;
                (digest, stamps, "resolved")
            }
            None => (digest_cc_include_dir_names(parsed)?, None, "walked"),
        };
        drop(trace_shadowing);
        self.pending_include_dir_digest
            .borrow_mut()
            .replace(PendingIncludeDirDigest {
                digest: include_dir_digest.clone(),
                names,
                stamps,
            });
        hasher.update(b"include_dir_names:");
        hasher.update(include_dir_digest.as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] include_dir_names={} mode={}",
            trace_name,
            include_dir_digest,
            include_dir_mode
        );

        let key = hasher.finalize().to_hex().to_string();
        // A cc-rs crate's C sources can carry the same out-of-band inputs as
        // its Rust siblings; the crate dir is the source file's nearest
        // enclosing `Cargo.toml`. Reaching cache_key means refuse_reasons
        // already gated this invocation, so it is exactly one source and
        // unconditionally cacheable — pass `is_primary = true`. The assert
        // pins that precondition so a future caller that bypasses the gate
        // fails loudly instead of silently anchoring extra_inputs to the
        // first of several sources.
        debug_assert_eq!(
            parsed.sources.len(),
            1,
            "cc cache_key expects a single-source compile (refuse_reasons gates the rest)"
        );
        let key = crate::extra_inputs::apply_extra_inputs(
            key,
            parsed.sources.first().map(|p| p.as_path()),
            &trace_name,
            true,
            ctx.file_hasher,
        );
        let key = crate::cache_key::apply_key_env_vars(key, ctx.key_env_vars, &trace_name);
        let key = crate::cache_key::apply_key_salt(key, ctx.key_salt, &trace_name);
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] final={}",
            trace_name,
            &key[..16]
        );
        Ok(CcKeyOutcome::Key(key))
    }

    /// Compile first, capturing what the compiler read, for a unit whose key
    /// was deferred. The read set comes from the caller's own `-MD` depfile
    /// when there is one, else from a private one the compile is asked to
    /// write. `None` inputs mean the compile ran but cannot be keyed: the
    /// capture failed, an input is unreadable, or a file the assembler reads
    /// hides from every fingerprint.
    pub(crate) fn execute_capturing_inputs(
        &self,
        parsed: &CcArgs,
        file_hasher: &crate::cache_key::FileHasher<'_>,
    ) -> Result<(CompileResult, Option<CcCapturedInputs>)> {
        let caller_depfile = parsed.depinfo_output_path();
        let private = if caller_depfile.is_none() {
            match tempfile::Builder::new()
                .prefix("kache-cc-capture-")
                .tempdir()
            {
                Ok(dir) => Some(dir),
                Err(error) => {
                    tracing::debug!("cc capture tempdir unavailable: {error}");
                    None
                }
            }
        } else {
            None
        };
        let private_path = private
            .as_ref()
            .map(|dir| dir.path().join("inputs.d"))
            .filter(|path| path.to_str().is_some());
        let extra = match &private_path {
            Some(path) => vec![
                "-MD".to_string(),
                "-MF".to_string(),
                path.to_str().expect("checked UTF-8").to_string(),
                "-MT".to_string(),
                CC_DIRECT_CAPTURE_TARGET.to_string(),
            ],
            None => Vec::new(),
        };
        let (result, path_bound) = self.execute_with_extra_args(parsed, extra, true)?;
        if result.exit_code != 0 {
            return Ok((result, None));
        }
        let depfile = match (caller_depfile, private_path) {
            (Some(path), _) => path,
            (None, Some(path)) => path,
            (None, None) => return Ok((result, None)),
        };
        let inputs = {
            let _trace = crate::phase_trace::phase("cc_capture");
            self.captured_inputs(parsed, &depfile, file_hasher)
        };
        let inputs_changed = file_hasher.too_new();
        Ok((
            result,
            inputs.map(|fingerprints| CcCapturedInputs {
                fingerprints,
                path_bound,
                inputs_changed,
            }),
        ))
    }

    fn captured_inputs(
        &self,
        parsed: &CcArgs,
        depfile: &Path,
        file_hasher: &crate::cache_key::FileHasher<'_>,
    ) -> Option<Vec<crate::cache_key::CcPreprocessMemoInput>> {
        let raw = match fs::read_to_string(depfile) {
            Ok(raw) => raw,
            Err(error) => {
                tracing::debug!("cc capture depfile unreadable: {error}");
                return None;
            }
        };
        let cwd = std::env::current_dir().ok()?;
        let mut paths = match parse_preprocess_dependencies(&raw, &cwd) {
            Ok(paths) => paths,
            Err(error) => {
                tracing::debug!("cc capture depfile unusable: {error:#}");
                return None;
            }
        };
        // The depfile lists the source itself; adding it again is harmless,
        // the fingerprints are deduplicated by name.
        for source in &parsed.sources {
            paths.push(if source.is_absolute() {
                source.clone()
            } else {
                cwd.join(source)
            });
        }
        let prefix_maps = cc_prefix_maps(parsed, &self.base_dirs);
        let mapped: Vec<(String, PathBuf)> = paths
            .into_iter()
            .map(|path| (cc_mapped_path(&path, &prefix_maps), path))
            .collect();
        let inputs = file_hasher.cc_preprocess_fingerprints(
            &mapped,
            &cc_prefix_maps_key(&prefix_maps),
            &|path| cc_mapped_content_hash(path, &prefix_maps),
        )?;
        // After the fingerprints: the scan is memoised by the content hash
        // they carry, so a header shared by every unit is read once.
        if let Some(construct) = file_hasher.cc_inputs_hide_assembler_input(&inputs, &|path| {
            let bytes = fs::read(path).ok()?;
            Some(cc_raw_assembler_hidden_input(&bytes))
        }) {
            tracing::debug!(
                "cc: {} not keyed from its read set: the assembler may read a file the key cannot see (`{construct}`)",
                cc_trace_name(parsed)
            );
            return None;
        }
        Some(inputs)
    }

    /// Run the compiler. With `bind_on_embedded_root`, an object that spells
    /// a checkout root is kept and reported as path-bound instead of being
    /// dropped: the caller keys it to this checkout.
    fn execute_with_extra_args(
        &self,
        parsed: &CcArgs,
        extra: Vec<String>,
        bind_on_embedded_root: bool,
    ) -> Result<(CompileResult, bool)> {
        let mut path_bound = false;
        // Invoke the underlying compiler with the original argv, plus a
        // set of `-ffile-prefix-map` rules so the object doesn't embed
        // clone-local build/source roots. Spliced in before any `--`
        // separator (see `compose_cc_args`) so the driver still reads
        // them as flags, then last among the flags so they win over any
        // user-supplied map for the same prefix.
        crate::opcounts::record_compiler_run();
        let _trace = crate::phase_trace::phase("cc_compile");
        let mut command = Command::new(&parsed.program);
        let prefix_maps = cc_prefix_maps(parsed, &self.base_dirs);
        let mut appended = file_prefix_map_args(&prefix_maps);
        appended.extend(extra);
        let args = compose_cc_args(&parsed.rest, appended);
        command.args(&args);
        // Pin the same effective SOURCE_DATE_EPOCH the `-E` key probe used, so a
        // TU baking __DATE__/__TIME__/__TIMESTAMP__ produces an object whose date
        // matches its time-stable cache key. Mirrors the existing __FILE__
        // normalization (we already rewrite paths via -ffile-prefix-map); pinning
        // the date is the same stance. Opt out with
        // KACHE_CC_SOURCE_DATE_EPOCH=passthrough (#423).
        if let Some(epoch) = effective_source_date_epoch() {
            command.env("SOURCE_DATE_EPOCH", epoch);
        }
        let output = command
            .output()
            .with_context(|| format!("executing {}", parsed.program))?;
        let exit_code = output.status.code().unwrap_or(1);

        // Output discovery: on a successful compile the named output is the
        // cacheable artifact. Skip on failure, and skip for the modes that
        // are still refused upstream, where discovery would guess at a file
        // the invocation never wrote. `-E` writing to a named file reaches
        // here now, and its expansion is discovered the same way an object
        // is. Objects retain their basename; dep-info gets a semantic store
        // name because `-MF` permits arbitrary suffixes and restore already
        // takes its real destination from the current invocation
        // (kunobi-ninja/kache#655).
        let discovers_outputs = matches!(parsed.mode, CompileMode::Compile)
            || (parsed.mode == CompileMode::Link && self.cache_cc_links)
            || (parsed.mode == CompileMode::Preprocess && parsed.output.is_some());
        let mut keepalive = Vec::new();
        let mut artifacts = if exit_code == 0 && discovers_outputs {
            discover_cc_output_artifacts(parsed)
        } else {
            ArtifactSet::empty()
        };
        if exit_code == 0 && cc_expansion_is_stdout(parsed) {
            match stage_cc_stdout_artifact(&output.stdout) {
                Ok((artifact, temp)) => {
                    artifacts = ArtifactSet::new(vec![artifact]);
                    keepalive.push(temp);
                }
                Err(error) => {
                    tracing::warn!("cc: staging -E stdout failed: {error:#}; not caching it");
                }
            }
        } else {
            // Safety net for #1004. The key probe catches roots in the expansion.
            // A root that reaches the object some other way would still be stored
            // under a key every checkout shares, so keep the object for this build
            // and store nothing. It only sees roots spelled out as plain bytes.
            let embeds = parsed
                .object_output_path()
                .map(|path| cc_object_embeds_mapped_root(&path, &prefix_maps));
            if bind_on_embedded_root && !artifacts.is_empty() && matches!(embeds, Some(Ok(true))) {
                path_bound = true;
            } else {
                let unsafe_to_store =
                    cc_unsafe_to_store(artifacts.is_empty(), self.key_path_bound.get(), || embeds);
                artifacts = match unsafe_to_store {
                    None => artifacts,
                    Some(reason) => {
                        tracing::warn!("cc: {} {reason}; not caching it", cc_trace_name(parsed));
                        ArtifactSet::empty()
                    }
                };
            }
        }

        Ok((
            CompileResult {
                exit_code,
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                pending_stderr: None,
                artifacts,
                keepalive,
            },
            path_bound,
        ))
    }
}

fn stage_cc_stdout_artifact(bytes: &[u8]) -> Result<(Artifact, tempfile::TempPath)> {
    let mut staged = tempfile::Builder::new()
        .prefix("kache-cc-stdout-")
        .suffix(".i")
        .tempfile()
        .context("cc: creating -E stdout staging file")?;
    staged
        .write_all(bytes)
        .context("cc: writing -E stdout staging file")?;
    staged
        .flush()
        .context("cc: flushing -E stdout staging file")?;
    let temp = staged.into_temp_path();
    Ok((
        Artifact {
            path: temp.to_path_buf(),
            store_name: CC_STDOUT_STORE_NAME.to_string(),
            kind: ArtifactKind::Other("stdout"),
            required: true,
        },
        temp,
    ))
}

fn expand_cc_response_files(args: &[String]) -> Result<Vec<String>> {
    let mut expanded = Vec::new();
    for arg in args {
        let Some(path) = arg.strip_prefix('@') else {
            expanded.push(arg.clone());
            continue;
        };
        let body =
            fs::read_to_string(path).with_context(|| format!("reading cc response file {path}"))?;
        expanded.extend(body.split_whitespace().map(str::to_string));
    }
    Ok(expanded)
}

fn cc_link_file_inputs(args: &[String]) -> Vec<PathBuf> {
    let mut inputs = Vec::new();
    let mut skip_next = false;
    for arg in args {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "-o" || arg == "-I" || arg == "-L" || arg == "-include" {
            skip_next = true;
            continue;
        }
        if arg.starts_with('-') {
            continue;
        }
        let path = PathBuf::from(arg);
        let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
        if matches!(
            ext,
            "o" | "obj" | "a" | "lib" | "so" | "dylib" | "dll" | "res" | "lo"
        ) {
            inputs.push(path);
        }
    }
    inputs
}

fn discover_cc_link_sidecars(output: &Path) -> Vec<Artifact> {
    let Some(stem) = output.file_stem().map(|name| name.to_os_string()) else {
        return Vec::new();
    };
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let mut sidecars = Vec::new();
    for (suffix, kind) in [
        (".map", ArtifactKind::Other("map")),
        (".pdb", ArtifactKind::DebugSidecar),
        (".lib", ArtifactKind::Library),
        (".exp", ArtifactKind::Other("exp")),
    ] {
        let mut name = stem.clone();
        name.push(suffix);
        let path = parent.join(name);
        if std::fs::symlink_metadata(&path).is_ok_and(|meta| meta.file_type().is_file()) {
            let store_name = path
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_default();
            sidecars.push(Artifact {
                path,
                kind,
                store_name,
                required: false,
            });
        }
    }
    let dsym = parent.join(format!(
        "{}.dSYM",
        output
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default()
    ));
    if dsym.is_dir() {
        let file_name = output
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        sidecars.push(Artifact {
            path: dsym,
            kind: ArtifactKind::DebugBundle,
            store_name: format!("{file_name}.dsym.tar"),
            required: false,
        });
    }
    sidecars
}

/// Discover a successful C/C++ compile's cacheable outputs while preserving
/// the parsed role of `-MF` instead of trying to recover it from a filename.
fn discover_cc_output_artifacts(parsed: &CcArgs) -> ArtifactSet {
    // Store only lexical regular files. Following a symlink here would let an
    // output such as `/dev/null` or a FIFO enter the blob-ingest path, while a
    // symlink needs compiler-specific replacement semantics on later runs.
    fn is_plain_file(path: &std::path::Path) -> bool {
        std::fs::symlink_metadata(path).is_ok_and(|meta| {
            meta.file_type().is_file() && regular_output_is_independent(path, &meta)
        })
    }

    let Some(object) = parsed
        .object_output_path()
        .filter(|path| is_plain_file(path))
    else {
        return ArtifactSet::empty();
    };
    let object_name = object
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let mut outputs = vec![Artifact {
        path: object.clone(),
        kind: classify_by_filename(&object_name),
        store_name: object_name,
        required: true,
    }];
    if parsed.mode == CompileMode::Link {
        outputs.extend(discover_cc_link_sidecars(&object));
    }

    if let Some(depinfo) = parsed
        .depinfo_output_path()
        .filter(|path| is_plain_file(path))
    {
        outputs.push(Artifact {
            path: depinfo,
            store_name: CC_DEPINFO_STORE_NAME.to_string(),
            kind: ArtifactKind::DepInfo,
            required: true,
        });
    }

    ArtifactSet::new(outputs)
}

#[cfg(test)]
mod tests;
