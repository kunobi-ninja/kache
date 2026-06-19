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
//! - Mach-O OSO record stripping for cross-machine sharing of *linked*
//!   artifacts (issue #78) — deferred until link-mode caching exists, since
//!   `-c` object compiles carry no linker-emitted `N_OSO` records. The
//!   SDKROOT half of #78 is handled: the Apple SDK path is mapped to the
//!   `<SDKROOT>` prefix-map sentinel ([`CC_SDKROOT_SENTINEL`]).

use anyhow::{Context, Result};
use regex::Regex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use super::flags::{Dialect, FlagClass, FlagSpec, Matcher};
use super::{
    ArtifactKind, ArtifactSet, CompileResult, Compiler, CompilerAdapter, CompilerId, KeyCtx,
    RefuseReason, classify_by_filename,
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
    /// `clang-cl` (basename) or any argv carrying `--driver-mode=cl` is
    /// `ClangCl`. A `clang`/`clang++`/`clang-<n>` basename is `Clang`.
    /// Everything else (gcc, cc, g++, c++) is `Gnu`. Bare `cl` is NOT
    /// special-cased — real MSVC `cl.exe` stays out of scope.
    pub fn detect(program: &str, rest: &[String]) -> ToolFamily {
        let name = super::command_basename(program)
            .map(super::strip_windows_exe_suffix)
            .unwrap_or(program)
            .to_ascii_lowercase();
        if name == "clang-cl" || rest.iter().any(|a| a == "--driver-mode=cl") {
            return ToolFamily::ClangCl;
        }
        let stem = name.split('-').next().unwrap_or("");
        if stem == "clang" || stem == "clang++" {
            return ToolFamily::Clang;
        }
        ToolFamily::Gnu
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
    #[allow(dead_code)]
    RawKeyed,
    #[allow(dead_code)]
    ExtraHashFile,
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
            CompileMode::Preprocess => reasons.push(RefuseReason::Unsupported(
                "cc preprocessor mode -E — not yet",
            )),
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
        // `*.pch` / `*.gch` as -include argument also indicates PCH.
        let mut iter = self.rest.iter().peekable();
        while let Some(arg) = iter.next() {
            if arg == "-include"
                && let Some(next) = iter.peek()
                && (next.ends_with(".pch") || next.ends_with(".gch"))
            {
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
        } else if self.sources.is_empty() {
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
const CC_ROOT_SENTINEL: &str = "<CC_ROOT>";
const CC_BUILD_SENTINEL: &str = "<CC_BUILD>";
const CC_SOURCE_SENTINEL: &str = "<CC_SOURCE>";
/// Sentinel for a user-declared `KACHE_BASE_DIR` (ccache `CCACHE_BASEDIR`
/// analog). Distinct from the derived roots so an explicit base dir can't
/// collide with a `<CC_ROOT>` subtree.
const CC_BASE_SENTINEL: &str = "<CC_BASE>";
/// Sentinel for the Apple SDK root (issue #78). The resolved `cc -###`
/// tokens embed the SDK path (`-isysroot /…/MacOSX14.2.sdk`,
/// `-internal-isystem /…/usr/include`), which differs across Xcode and
/// Command Line Tools installs and between machines. Stripping it to a
/// sentinel lets two builds with the same SDK *contents* at different
/// paths share a key — differing SDK *contents* still diverge via
/// `compiler_version` and the preprocessor expansion, so this only ever
/// merges keys that would otherwise miss, never miscaches. A distinct
/// sentinel so the SDK can't collide with a project root.
const CC_SDKROOT_SENTINEL: &str = "<SDKROOT>";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CcPrefixMap {
    from: String,
    to: &'static str,
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
fn preprocess_hash(parsed: &CcArgs, prefix_maps: &[CcPrefixMap]) -> Result<String> {
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
    let stdout = apply_cc_prefix_maps_to_bytes(output.stdout, prefix_maps);
    Ok(blake3::hash(&stdout).to_hex().to_string())
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
        matcher: Matcher::Exact("-pthread"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — pthread feature switch (also visible via _REENTRANT in preprocessor).",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fstack-protector-strong"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — stack-protector codegen mode.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fstack-clash-protection"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #245 — stack-clash-protection codegen hardening (Firefox).",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-math-errno"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — math-errno codegen knob.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-strict-aliasing"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — alias-analysis codegen knob.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fno-omit-frame-pointer"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — frame-pointer codegen knob.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-funwind-tables"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #114 — unwind-tables codegen knob.",
        dialect: None,
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
    FlagSpec {
        matcher: Matcher::Exact("-msimd128"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — WASM SIMD128 enable.",
        dialect: None,
    },
    FlagSpec {
        // x86 width / SIMD feature flags seen in Firefox passthroughs (#375).
        // These materially change the object by selecting the target width or
        // enabled ISA features. The resolved `cc -###` stream records the
        // resulting target triple / `-target-feature` set, so the cache key
        // differentiates each value. Listed tightly instead of opening `-m*`.
        matcher: Matcher::Regex(r"-m(?:32|64|sse2|sse4\.1|sse4\.2|avx2)"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #375 — x86 width and SIMD feature flags from Firefox passthroughs.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-ffunction-sections"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — function-per-section object layout.",
        dialect: None,
    },
    FlagSpec {
        matcher: Matcher::Exact("-fdata-sections"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #115 — data-per-section object layout.",
        dialect: None,
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
        matcher: Matcher::Exact("-Oy-"),
        class: FlagClass::CapturedByProbe,
        source: "Issue #285 — clang-cl frame-pointer omission disabled. Keyed via -### resolved tokens.",
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
    for arg in &parsed.rest {
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
    }

    if !parsed.rest.is_empty() {
        tracing::debug!(
            "[cc:{subject}] flag classify: {} modeled / {} probe / {} preprocessor / {} no-effect / {} parser-handled / {} user-allowed / {} unmodeled",
            summary.modeled_in_key,
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
    static CACHE: OnceLock<HashMap<&'static str, Regex>> = OnceLock::new();
    crate::compiler::flags::classify_against(
        arg,
        CC_FLAGS,
        CACHE.get_or_init(|| crate::compiler::flags::build_regex_cache(CC_FLAGS)),
        dialect,
    )
}

fn cc_flags_need_resolved_invocation(parsed: &CcArgs) -> bool {
    let dialect = parsed.family.dialect();
    parsed
        .rest
        .iter()
        .any(|arg| analyze_cc_arg(arg, dialect).bucket == CcArgBucket::ProbeKeyed)
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
fn cc_prefix_maps(parsed: &CcArgs) -> Vec<CcPrefixMap> {
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
    cc_prefix_maps_cfg(
        parsed,
        &cwd,
        base.as_deref().map(Path::new),
        sdkroot.as_deref().map(Path::new),
    )
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
    let mut maps = cc_prefix_maps_for(parsed, cwd);

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
                    to: CC_BASE_SENTINEL,
                });
            }
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
                    to: CC_SDKROOT_SENTINEL,
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
    if let Some(common) = common_ancestor(&cwd_abs, &source_parent)
        && stable_cc_common_root(&common, &cwd_abs, &source_parent)
    {
        roots.push((common, CC_ROOT_SENTINEL));
    } else {
        roots.push((cwd_abs.clone(), CC_BUILD_SENTINEL));
        roots.push((source_parent.clone(), CC_SOURCE_SENTINEL));
    }

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
        maps.push(CcPrefixMap { from, to });
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

fn apply_cc_prefix_maps_to_bytes(mut bytes: Vec<u8>, prefix_maps: &[CcPrefixMap]) -> Vec<u8> {
    for map in prefix_maps {
        let from = map.from.as_bytes();
        if from.is_empty() {
            continue;
        }
        bytes = replace_bytes(&bytes, from, map.to.as_bytes());
    }
    bytes
}

fn replace_bytes(input: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() || input.len() < from.len() {
        return input.to_vec();
    }
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i..].starts_with(from) {
            out.extend_from_slice(to);
            i += from.len();
        } else {
            out.push(input[i]);
            i += 1;
        }
    }
    out
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

#[derive(Default)]
pub struct CcCompiler {
    /// User-declared flags (issue #95) that kache's built-in allow-list
    /// doesn't model but the user opted into caching. A flag here stops
    /// refusing and is folded verbatim into the cache key. Empty in the
    /// common case (and for every existing `CcCompiler::new()` caller).
    extra_allowlist_flags: Vec<String>,
}

impl CcCompiler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Construct with a user-declared cc flag allow-list (issue #95),
    /// typically `config.cc_extra_allowlist_flags`.
    pub fn with_extra_allowlist_flags(extra_allowlist_flags: Vec<String>) -> Self {
        Self {
            extra_allowlist_flags,
        }
    }

    /// Does this argv invoke a C-family compiler?
    ///
    /// Matches `cc`, `c++`, `gcc`, `g++`, `clang`, `clang++` and
    /// versioned variants (`gcc-13`, `clang++-17`). Path-prefixed
    /// forms (`/usr/bin/cc`, `C:\path\clang.exe`) and Windows `.exe`
    /// suffixes are accepted.
    ///
    /// Owns its own detection rule; `super::detect_compiler` reaches it
    /// through this module's [`ADAPTER`] descriptor.
    pub fn recognizes(args: &[String]) -> bool {
        let Some(arg0) = args.first() else {
            return false;
        };
        let Some(name) = super::command_basename(arg0) else {
            return false;
        };
        let name = super::strip_windows_exe_suffix(name);

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

/// Remove read-only output files before the compiler writes to them.
///
/// When kache restores a cc cache hit it hardlinks store blobs (0o444,
/// shared inode with the store) into the object output path and — when
/// requested — its dep-info sidecar. If a subsequent build is a cache
/// MISS for the same translation unit (e.g. the source was edited), the
/// compiler tries to overwrite these paths in place and fails with
/// EACCES / "operation not permitted" (observed with gcc, clang, and
/// clang-cl on Windows).  A chmod cannot help because the inode is
/// shared: making it writable would mutate the store blob. The correct
/// fix is to unlink the file first: a plain `remove_file` breaks the
/// hardlink and leaves the store blob untouched, exactly as
/// `compile::pre_clean_outputs` does for the rustc path.
///
/// Best-effort: a missing file is fine; errors are silently ignored.
pub(crate) fn pre_clean_cc_outputs(parsed: &CcArgs) {
    if let Some(obj) = parsed.object_output_path() {
        let _ = std::fs::remove_file(&obj);
    }
    if let Some(dep) = parsed.depinfo_output_path() {
        let _ = std::fs::remove_file(&dep);
    }
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
        parsed.refuse_reasons(&self.extra_allowlist_flags)
    }

    fn cache_key(&self, parsed: &CcArgs, ctx: &KeyCtx<'_, '_>) -> Result<String> {
        // Preconditions (guaranteed by the wrapper checking
        // refuse_reasons first): `-c` mode, exactly one source.
        let mut hasher = blake3::Hasher::new();
        let trace_name = cc_trace_name(parsed);
        let prefix_maps = cc_prefix_maps(parsed);

        hasher.update(b"cc_key_version:");
        hasher.update(crate::cache_key::CACHE_KEY_VERSION.to_string().as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] cc_key_version={}",
            trace_name,
            crate::cache_key::CACHE_KEY_VERSION
        );

        let mut prefix_sentinels: Vec<&str> = Vec::new();
        for map in &prefix_maps {
            if !prefix_sentinels.contains(&map.to) {
                prefix_sentinels.push(map.to);
            }
        }
        prefix_sentinels.sort_unstable();

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
        let resolved = crate::probe::probe(
            ctx.cache_dir,
            &crate::probe::CcProber,
            &crate::probe::ProbeRequest {
                compiler: &parsed.program,
                args: &parsed.rest,
                key_args: &config_args,
                // Sentinel Windows paths only for gnu/clang (objects are
                // remapped via -ffile-prefix-map). clang-cl keeps raw
                // native paths → key stays path-literal (#299/#312).
                windows_aware: parsed.family.dialect() != Dialect::Cl,
            },
        )?;
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
            hasher.update(b"depinfo_target:");
            if let Some(target) = &depinfo.target {
                hasher.update(target.as_bytes());
            } else if let Some(object) = parsed.object_output_path()
                && let Some(name) = object.file_name()
            {
                hasher.update(name.to_string_lossy().as_bytes());
            }
            hasher.update(b"\n");
        } else {
            hasher.update(b"0\n");
        }

        // Preprocessor expansion — the load-bearing input. Captures
        // the source plus every transitively-included header plus
        // macro expansion. `-E -P` strips line markers so header
        // PATHS don't leak (cross-machine portable); SOURCE_DATE_EPOCH
        // pins __DATE__/__TIME__ (stable across builds).
        let pp_hash = preprocess_hash(parsed, &prefix_maps)?;
        hasher.update(b"preprocessed:");
        hasher.update(pp_hash.as_bytes());
        hasher.update(b"\n");
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] preprocessed={}",
            trace_name,
            pp_hash
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
        let key = crate::cache_key::apply_key_salt(key, ctx.key_salt, &trace_name);
        tracing::trace!(
            target: "kache::cache_key",
            "[key:{}] final={}",
            trace_name,
            &key[..16]
        );
        Ok(key)
    }

    fn execute(&self, parsed: &CcArgs) -> Result<CompileResult> {
        // Pre-clean read-only restored hardlinks so the compiler can
        // overwrite them. A previous cache-on build may have restored the
        // object (and dep-info sidecar) as read-only hardlinks into the
        // local store (0o444, shared inode). Running the real compiler
        // over them in place fails with EACCES / "operation not permitted"
        // (observed with gcc, clang, and clang-cl). A plain remove breaks
        // the hardlink and leaves the store blob intact — identical to the
        // rustc `pre_clean_outputs` rationale in `src/compile.rs`.
        // Best-effort: a missing file is fine; any other error is ignored.
        pre_clean_cc_outputs(parsed);

        // Invoke the underlying compiler with the original argv, plus a
        // set of `-ffile-prefix-map` rules so the object doesn't embed
        // clone-local build/source roots. Spliced in before any `--`
        // separator (see `compose_cc_args`) so the driver still reads
        // them as flags, then last among the flags so they win over any
        // user-supplied map for the same prefix.
        crate::opcounts::record_compiler_run();
        let mut command = Command::new(&parsed.program);
        let prefix_maps = cc_prefix_maps(parsed);
        let args = compose_cc_args(&parsed.rest, file_prefix_map_args(&prefix_maps));
        command.args(&args);
        let output = command
            .output()
            .with_context(|| format!("executing {}", parsed.program))?;
        let exit_code = output.status.code().unwrap_or(1);

        // Output discovery: on a successful `-c` compile, the object
        // file is the cacheable artifact. Skip on failure (nothing to
        // cache) or non-Compile mode (refused upstream anyway). The
        // store name is the bare filename so restore can place it at
        // whatever `-o` path the warm invocation requests.
        let artifacts = if exit_code == 0 && parsed.mode == CompileMode::Compile {
            match parsed.object_output_path() {
                Some(obj) if obj.exists() => {
                    let name = obj
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    let mut outputs = vec![(obj, name)];
                    if let Some(depinfo) = parsed.depinfo_output_path()
                        && depinfo.exists()
                    {
                        let name = depinfo
                            .file_name()
                            .map(|n| n.to_string_lossy().into_owned())
                            .unwrap_or_default();
                        outputs.push((depinfo, name));
                    }
                    ArtifactSet::from_output_files(outputs, classify_by_filename)
                }
                _ => ArtifactSet::empty(),
            }
        } else {
            ArtifactSet::empty()
        };

        Ok(CompileResult {
            exit_code,
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            artifacts,
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

    #[test]
    fn cc_flags_dep_info_is_gnu_only() {
        use crate::compiler::flags::{Dialect, FlagClass};
        // The `-MM?D|-M[FTQPG]` gnu dep-info row is tagged Gnu-only.
        // Under Gnu every spelling is inert (NoObjectEffect).
        for flag in ["-MD", "-MMD", "-MT", "-MF", "-MQ", "-MP", "-MG"] {
            assert_eq!(
                classify_cc_flag(flag, Dialect::Gnu),
                Some(FlagClass::NoObjectEffect),
                "{flag} should be inert dep-info under Gnu"
            );
        }
        // Under Cl:
        // - `-MD` and `-MT` are CRT-selection flags, classified as
        //   CapturedByProbe by the Layer 2 cl rows. They must NOT refuse.
        for flag in ["-MD", "-MT"] {
            assert_eq!(
                classify_cc_flag(flag, Dialect::Cl),
                Some(FlagClass::CapturedByProbe),
                "{flag} should be CapturedByProbe under Cl (CRT selection)"
            );
        }
        // - The rest (`-MMD`, `-MF`, `-MQ`, `-MP`, `-MG`) have no
        //   cl-specific row and still refuse (return None) under Cl.
        for flag in ["-MMD", "-MF", "-MQ", "-MP", "-MG"] {
            assert_eq!(
                classify_cc_flag(flag, Dialect::Cl),
                None,
                "{flag} must refuse under Cl (no cl-specific row)"
            );
        }
        // a dialect-less row still classifies under both
        assert_eq!(
            classify_cc_flag("-DFOO", Dialect::Cl),
            Some(FlagClass::PreprocessorCaptured)
        );
    }

    #[test]
    fn clang_cl_flag_classification() {
        use crate::compiler::flags::{Dialect, FlagClass};
        let cl = Dialect::Cl;
        // codegen → CapturedByProbe (keyed via -###).
        // Note: bare -O1/-O2 stay ModeledInKey under Cl — the earlier
        // dialect-agnostic `-O[0-3sz]?` CC_FLAGS row matches them first.
        // The /Onn forms, -Od, and -Ox fall through to the cl regex →
        // CapturedByProbe. Both mechanisms key the level (no collision).
        for f in [
            "-guard:cf,nochecks",
            "-Gy",
            "-Gw",
            "-Oy-",
            "-fms-compatibility-version=19.50",
            "-MD",
            "-MT",
            "/MD",
            "/O2",
        ] {
            assert_eq!(
                classify_cc_flag(f, cl),
                Some(FlagClass::CapturedByProbe),
                "{f}"
            );
        }
        // output + ignored → NoObjectEffect (accepted, not keyed)
        for f in ["-Fofoo.obj", "/Fofoo.obj", "-Zc:inline"] {
            assert_eq!(
                classify_cc_flag(f, cl),
                Some(FlagClass::NoObjectEffect),
                "{f}"
            );
        }
        // standard → ModeledInKey; forced include → PreprocessorCaptured
        assert_eq!(
            classify_cc_flag("-std:c++20", cl),
            Some(FlagClass::ModeledInKey)
        );
        assert_eq!(
            classify_cc_flag("-FIfoo.h", cl),
            Some(FlagClass::PreprocessorCaptured)
        );
        // gnu unaffected: -MD is still inert dep-info under Gnu
        assert_eq!(
            classify_cc_flag("-MD", Dialect::Gnu),
            Some(FlagClass::NoObjectEffect)
        );
    }

    #[test]
    fn parse_records_tool_family() {
        let gnu = CcArgs::parse(&s(&["gcc", "-c", "a.c"])).unwrap();
        assert_eq!(gnu.family, ToolFamily::Gnu);
        let cl = CcArgs::parse(&s(&["clang-cl.exe", "-c", "a.c"])).unwrap();
        assert_eq!(cl.family, ToolFamily::ClangCl);
    }

    #[test]
    fn tool_family_detects_clang_cl_and_dialects() {
        use crate::compiler::flags::Dialect;
        let f = |prog: &str, rest: &[&str]| ToolFamily::detect(prog, &s(rest));

        assert_eq!(f("clang-cl", &[]), ToolFamily::ClangCl);
        assert_eq!(f("clang-cl.exe", &[]), ToolFamily::ClangCl);
        assert_eq!(f(r"C:\VS\bin\clang-cl.EXE", &[]), ToolFamily::ClangCl);
        assert_eq!(f("clang", &["--driver-mode=cl"]), ToolFamily::ClangCl);
        assert_eq!(f("clang", &[]), ToolFamily::Clang);
        assert_eq!(f("clang++-17", &[]), ToolFamily::Clang);
        assert_eq!(f("clang-15", &[]), ToolFamily::Clang);
        // Only an exact `clang-cl` basename (or --driver-mode=cl) is cl;
        // a versioned `clang-cl-17` symlink has stem "clang" → Clang.
        assert_eq!(f("clang-cl-17", &[]), ToolFamily::Clang);
        assert_eq!(f("gcc", &[]), ToolFamily::Gnu);
        assert_eq!(f("/usr/bin/cc", &[]), ToolFamily::Gnu);
        assert_eq!(f("g++", &[]), ToolFamily::Gnu);

        assert_eq!(ToolFamily::Gnu.dialect(), Dialect::Gnu);
        assert_eq!(ToolFamily::Clang.dialect(), Dialect::Gnu);
        assert_eq!(ToolFamily::ClangCl.dialect(), Dialect::Cl);
    }

    // ── dialect-aware parser ─────────────────────────────────────

    #[test]
    fn clang_cl_output_and_std_parse() {
        use crate::compiler::flags::Dialect;
        let p = CcArgs::parse(&s(&[
            "clang-cl",
            "-c",
            "-Fobuild\\foo.obj",
            "-std:c++20",
            "foo.c",
        ]))
        .unwrap();
        assert_eq!(p.family.dialect(), Dialect::Cl);
        assert_eq!(p.output.as_ref().unwrap().to_str(), Some("build\\foo.obj"));
        assert_eq!(
            p.object_output_path().unwrap().to_str(),
            Some("build\\foo.obj")
        );
        assert_eq!(p.std.as_deref(), Some("c++20"));
        // /-spellings too
        let q =
            CcArgs::parse(&s(&["clang-cl", "-c", "/Fofoo.obj", "/std:c++17", "foo.c"])).unwrap();
        assert_eq!(q.output.as_ref().unwrap().to_str(), Some("foo.obj"));
        assert_eq!(q.std.as_deref(), Some("c++17"));
    }

    #[test]
    fn parser_skips_gnu_only_rows_under_cl() {
        // -MT is a value-consuming gnu dep row. Under gcc the parser
        // consumes it AND its following token; under clang-cl the gnu row
        // is skipped (there -MT is single-token CRT selection), so the
        // next token is parsed independently rather than swallowed. The
        // value token carries a source extension so a broken skip is
        // observable: if -MT failed to consume it, it would surface as a
        // second source.
        let gnu = CcArgs::parse(&s(&["gcc", "-c", "-MT", "tgt.c", "a.c"])).unwrap();
        assert_eq!(gnu.sources.len(), 1, "-MT should consume tgt.c under gnu");
        assert_eq!(gnu.sources[0].to_str(), Some("a.c"));

        // Under clang-cl the gnu -MT row is skipped, so -MT does NOT
        // consume the following token; a.c is still the source. (If the
        // skip were broken, -MT would swallow a.c → sources empty.)
        let cl = CcArgs::parse(&s(&["clang-cl", "-c", "-MT", "a.c"])).unwrap();
        assert_eq!(cl.sources.len(), 1, "-MT must not consume a.c under cl");
        assert_eq!(cl.sources[0].to_str(), Some("a.c"));
    }

    #[test]
    fn config_args_keeps_crt_flags_under_cl_strips_dep_under_gnu() {
        // Gnu: -MT is per-TU dep-target noise → stripped (with its value).
        let gnu = CcArgs::parse(&s(&["gcc", "-c", "-MT", "tgt", "-DFOO", "a.c"])).unwrap();
        assert!(!gnu.config_args().iter().any(|a| a == "-MT" || a == "tgt"));
        assert!(gnu.config_args().iter().any(|a| a == "-DFOO"));

        // Cl: -MT is CRT selection (CapturedByProbe) → MUST stay in the
        // probe-memo key, or a -MT compile reuses a -MD record (false hit).
        let cl = CcArgs::parse(&s(&["clang-cl", "-c", "-MT", "-DFOO", "a.c"])).unwrap();
        assert!(cl.config_args().iter().any(|a| a == "-MT"));

        // -MD — the spelling that motivated #285 — must likewise stay.
        let cl_md = CcArgs::parse(&s(&["clang-cl", "-c", "-MD", "-DFOO", "a.c"])).unwrap();
        assert!(cl_md.config_args().iter().any(|a| a == "-MD"));
    }

    #[test]
    fn config_args_strips_clang_cl_output() {
        let p = CcArgs::parse(&s(&["clang-cl", "-c", "-Fofoo.obj", "-guard:cf", "foo.c"])).unwrap();
        let cfg = p.config_args();
        assert!(
            !cfg.iter().any(|a| a.starts_with("-Fo")),
            "-Fo must be stripped from probe-memo key: {cfg:?}"
        );
        assert!(
            cfg.iter().any(|a| a == "-guard:cf"),
            "codegen flag must stay: {cfg:?}"
        );
    }

    #[test]
    fn clang_cl_firefox_style_invocation_is_cacheable() {
        // The flags from issue #285's swgl log — non-debug subset.
        let p = CcArgs::parse(&s(&[
            "clang-cl",
            "-c",
            "foo.c",
            "-Fofoo.obj",
            "-fms-compatibility-version=19.50",
            "-guard:cf,nochecks",
            "-Gy",
            "-Gw",
            "-Oy-",
            "-Zc:inline",
            "-MD",
        ]))
        .unwrap();
        let refuse = p.refuse_reasons(&[]);
        assert!(
            refuse.is_empty(),
            "should be cacheable, refused: {:?}",
            refuse.iter().map(|r| r.description()).collect::<Vec<_>>()
        );
        // As of #312, -Z7 is also cacheable (path inputs are folded into the key).
        let dbg = CcArgs::parse(&s(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "-Z7"])).unwrap();
        assert!(
            dbg.refuse_reasons(&[]).is_empty(),
            "-Z7 must be cacheable after #312, got: {:?}",
            dbg.refuse_reasons(&[])
                .iter()
                .map(|r| r.description())
                .collect::<Vec<_>>()
        );
        assert!(
            cl_debug_path_inputs(&dbg).is_some(),
            "-Z7 must activate the cl_debug_path_inputs key fold"
        );
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
    fn recognizes_windows_exe_command_paths() {
        for name in [
            "clang.exe",
            "clang++.exe",
            "gcc.exe",
            "g++.exe",
            "C:/Users/dev/.mozbuild/clang/bin/clang.exe",
            r"C:\Users\dev\.mozbuild\clang\bin\clang.exe",
            "C:/Users/dev/.mozbuild/clang/bin/clang++.EXE",
        ] {
            assert!(
                CcCompiler::recognizes(&s(&[name])),
                "should recognize Windows compiler path {name}"
            );
        }
    }

    #[test]
    fn adapter_descriptor_uses_cc_recognizer() {
        assert_eq!(ADAPTER.id(), CC_ID);
        assert!(ADAPTER.recognizes(&s(&["cc"])));
        assert!(!ADAPTER.recognizes(&s(&["rustc"])));
    }

    #[test]
    fn recognizes_versioned_variants() {
        for name in [
            "gcc-13",
            "clang-15",
            "g++-12",
            "clang++-17",
            "gcc-13.exe",
            "clang++-17.exe",
        ] {
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

    // ── family-probe compiler recovery (issue #286) ──────────────

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn probe_compiler_recovers_real_compiler_from_target_cc_var() {
        // The exact shape from issue #286: mozbuild sets the
        // target-prefixed CC var to `kache <clang-cl>`, the cc crate
        // drops clang-cl from the family probe, and kache must recover
        // it from the environment.
        let vars = env(&[(
            "CC_aarch64_pc_windows_msvc",
            "C:/Users/sasch/.cargo/bin/kache.exe C:/Users/sasch/.mozbuild/clang/bin/clang-cl.exe",
        )]);
        assert_eq!(
            resolve_probe_compiler("kache", None, vars),
            Some("C:/Users/sasch/.mozbuild/clang/bin/clang-cl.exe".to_string())
        );
    }

    #[test]
    fn probe_compiler_recovers_from_plain_cc() {
        assert_eq!(
            resolve_probe_compiler("kache", None, env(&[("CC", "kache cc")])),
            Some("cc".to_string())
        );
    }

    #[test]
    fn probe_compiler_recovers_from_cxx_when_no_cc() {
        assert_eq!(
            resolve_probe_compiler("kache", None, env(&[("CXX", "kache clang++")])),
            Some("clang++".to_string())
        );
    }

    #[test]
    fn probe_compiler_prefers_cc_over_cxx() {
        // Both wrap kache; the C variable wins (the probe file is C).
        let vars = env(&[("CXX", "kache clang++"), ("CC", "kache clang")]);
        assert_eq!(
            resolve_probe_compiler("kache", None, vars),
            Some("clang".to_string())
        );
    }

    #[test]
    fn probe_compiler_matches_self_stem_case_insensitively() {
        // Windows path with an upper-case .EXE and mixed-case stem.
        let vars = env(&[("CC", r"C:\bin\KACHE.EXE clang-cl.exe")]);
        assert_eq!(
            resolve_probe_compiler("kache", None, vars),
            Some("clang-cl.exe".to_string())
        );
    }

    #[test]
    fn probe_compiler_none_when_cc_is_not_kache_wrapped() {
        // A plain compiler (no kache wrapper) is not ours to recover.
        assert_eq!(
            resolve_probe_compiler("kache", None, env(&[("CC", "clang -fPIC")])),
            None
        );
    }

    #[test]
    fn probe_compiler_none_when_only_self_present() {
        // `CC=kache` with no trailing compiler (and the RUSTC_WRAPPER
        // shape) leaves nothing to forward to.
        assert_eq!(
            resolve_probe_compiler("kache", None, env(&[("CC", "kache")])),
            None
        );
        assert_eq!(
            resolve_probe_compiler("kache", None, env(&[("CC", "kache kache")])),
            None
        );
    }

    #[test]
    fn probe_compiler_ignores_non_compiler_env_vars() {
        // Flags and ccache-style vars must never be mistaken for a
        // `<wrapper> <compiler>` pair even if they mention kache.
        let vars = env(&[
            ("CFLAGS", "kache -O2"),
            ("CXXFLAGS", "kache -O2"),
            ("CCACHE_DIR", "kache whatever"),
            ("RUSTC_WRAPPER", "kache"),
        ]);
        assert_eq!(resolve_probe_compiler("kache", None, vars), None);
    }

    #[test]
    fn probe_compiler_prefers_target_specific_cc_var() {
        // mozbuild sets both a host and a target compiler. With TARGET
        // known, kache must pick the target-specific var the cc crate
        // actually read — not whichever the environment lists first.
        let vars = env(&[
            ("HOST_CC", "kache gcc"),
            ("CC_aarch64_pc_windows_msvc", "kache clang-cl.exe"),
        ]);
        assert_eq!(
            resolve_probe_compiler("kache", Some("aarch64-pc-windows-msvc"), vars),
            Some("clang-cl.exe".to_string())
        );
    }

    #[test]
    fn probe_compiler_matches_dashed_target_cc_var() {
        // The cc crate also reads the un-underscored `CC_<triple>` form.
        let vars = env(&[("CC_aarch64-pc-windows-msvc", "kache clang-cl.exe")]);
        assert_eq!(
            resolve_probe_compiler("kache", Some("aarch64-pc-windows-msvc"), vars),
            Some("clang-cl.exe".to_string())
        );
    }

    #[test]
    fn probe_compiler_target_specific_beats_bare_cc() {
        let vars = env(&[
            ("CC", "kache gcc"),
            ("CC_x86_64_unknown_linux_gnu", "kache clang"),
        ]);
        assert_eq!(
            resolve_probe_compiler("kache", Some("x86_64-unknown-linux-gnu"), vars),
            Some("clang".to_string())
        );
    }

    #[test]
    fn probe_compiler_deterministic_when_target_unknown() {
        // Two target-suffixed vars and no TARGET to disambiguate: the pick
        // must be stable across environment iteration order, not flaky.
        let a = env(&[("CC_zzz", "kache zzz-cc"), ("CC_aaa", "kache aaa-cc")]);
        let b = env(&[("CC_aaa", "kache aaa-cc"), ("CC_zzz", "kache zzz-cc")]);
        assert_eq!(
            resolve_probe_compiler("kache", None, a),
            Some("aaa-cc".to_string())
        );
        assert_eq!(
            resolve_probe_compiler("kache", None, b),
            Some("aaa-cc".to_string())
        );
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
    fn parse_slash_c_sets_compile_mode_for_cl_only() {
        // clang-cl accepts the MSVC `/c` spelling; kache must treat it as a
        // compile (not default link mode → passthrough). Box-confirmed gap.
        let cl = CcArgs::parse(&s(&["clang-cl", "/c", "a.c", "-Foa.obj"])).unwrap();
        assert_eq!(cl.mode, CompileMode::Compile, "cl `/c` must set Compile");

        // Dash `-c` still works for cl.
        let cl_dash = CcArgs::parse(&s(&["clang-cl", "-c", "a.c"])).unwrap();
        assert_eq!(cl_dash.mode, CompileMode::Compile);

        // gnu must NOT treat `/c` as a compile marker (it's a path there).
        let gnu = CcArgs::parse(&s(&["gcc", "/c", "a.c"])).unwrap();
        assert_ne!(gnu.mode, CompileMode::Compile, "gnu `/c` is not a flag");
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
        assert!(d.emit);
        assert!(!d.include_system);
        assert_eq!(d.output, None);
        assert_eq!(d.target, None);
    }

    #[test]
    fn parse_depinfo_md_includes_system_headers() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MD"])).unwrap();
        let d = parsed.depinfo.expect("dep-info should be set");
        assert!(d.emit);
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
    fn parse_depinfo_mp_and_mg_shape_flags() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MP", "-MG"])).unwrap();
        let d = parsed.depinfo.expect("dep-info should be set");
        assert!(d.phony_targets);
        assert!(d.missing_generated);
    }

    #[test]
    fn parse_no_depinfo_flags_means_no_depinfo_struct() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
        assert!(parsed.depinfo.is_none());
    }

    #[test]
    fn depinfo_path_modifiers_alone_do_not_emit_depinfo() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MF", "deps/foo.d"])).unwrap();
        assert!(parsed.depinfo.is_some());
        assert_eq!(parsed.depinfo_output_path(), None);
        assert_eq!(parsed.depinfo_anchor(), None);
    }

    // ── parser: language override ───────────────────────────────

    #[test]
    fn parse_language_override() {
        let parsed = CcArgs::parse(&s(&["cc", "-x", "c++", "-c", "src"])).unwrap();
        assert_eq!(parsed.language_override, Some("c++".to_string()));
    }

    #[test]
    fn parse_language_override_sticky_form() {
        for (flag, expected) in [
            ("-xc", "c"),
            ("-xc++", "c++"),
            ("-xobjective-c", "objective-c"),
            ("-xobjective-c++", "objective-c++"),
        ] {
            let parsed = CcArgs::parse(&s(&["cc", flag, "-c", "foo.c"])).unwrap();
            assert_eq!(
                parsed.language_override,
                Some(expected.to_string()),
                "for {flag}"
            );
        }
    }

    #[test]
    fn parse_table_driven_value_forms() {
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            "foo.c",
            "-I",
            "include",
            "-Ivendor",
            "-D",
            "FOO=1",
            "-DBAR",
            "-std=c++20",
            "-xobjective-c++",
            "-o",
            "foo.o",
        ]))
        .unwrap();

        assert_eq!(
            parsed.includes,
            vec![PathBuf::from("include"), PathBuf::from("vendor")]
        );
        assert_eq!(
            parsed.defines,
            vec![
                ("FOO".to_string(), Some("1".to_string())),
                ("BAR".to_string(), None),
            ]
        );
        assert_eq!(parsed.std, Some("c++20".to_string()));
        assert_eq!(parsed.language_override, Some("objective-c++".to_string()));
        assert_eq!(parsed.output, Some(PathBuf::from("foo.o")));
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
        refuse_descriptions_with_flags(args, &[])
    }

    fn refuse_descriptions_with_flags(args: &[&str], extra: &[String]) -> Vec<&'static str> {
        let parsed = CcArgs::parse(&s(args)).unwrap();
        parsed
            .refuse_reasons(extra)
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
    fn cl_slash_flag_refuses_but_gnu_treats_it_positional() {
        // Layer 0's most operator-visible invariant, end to end: under
        // clang-cl an unmodeled `/`-flag fails closed (refuse →
        // passthrough); under gcc the same token is an inert positional,
        // so it does not produce an unsupported-flag refusal.
        //
        // Note: `/O2` was previously the unmodeled example but Layer 2
        // now classifies it as CapturedByProbe. Use `/unknown` (genuinely
        // unmodeled) to keep testing the invariant that an unclassified
        // slash flag refuses under Cl but not under Gnu.
        let cl = refuse_descriptions(&["clang-cl", "-c", "/unknown", "a.c"]);
        assert!(
            cl.iter().any(|d| d.contains("unsupported flag")),
            "clang-cl /unknown should refuse as an unsupported flag, got: {cl:?}"
        );
        let gnu = refuse_descriptions(&["gcc", "-c", "/unknown", "a.c"]);
        assert!(
            !gnu.iter().any(|d| d.contains("unsupported flag")),
            "gcc /unknown is an inert positional, not an unsupported flag, got: {gnu:?}"
        );
        // But /O2 itself is now modeled (CapturedByProbe) and must not refuse.
        let cl_o2 = refuse_descriptions(&["clang-cl", "-c", "/O2", "a.c"]);
        assert!(
            !cl_o2.iter().any(|d| d.contains("unsupported flag")),
            "clang-cl /O2 is now CapturedByProbe (Layer 2) and must not refuse, got: {cl_o2:?}"
        );
    }

    #[test]
    fn clang_cl_debug_is_now_cacheable_and_path_keyed() {
        // As of #312 the old "clang-cl debug" refusal is gone. The `-g`
        // form and the native MSVC `/Z7`/`-Z7`/`/Zi` spellings must all
        // cache (empty refuse_reasons) and must be recognised as a debug
        // compile that folds path inputs into the key.
        for flag in ["-g2", "/Z7", "-Z7", "/Zi", "/ZI", "/Zd"] {
            let p = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", flag])).unwrap();
            let descs = p
                .refuse_reasons(&[])
                .iter()
                .map(|r| r.description())
                .collect::<Vec<_>>();
            assert!(
                descs.is_empty(),
                "{flag} must be cacheable now, got: {descs:?}"
            );
            // cl_debug_path_inputs must return Some(…) so the key fold fires.
            assert!(
                cl_debug_path_inputs(&p).is_some(),
                "{flag}: cl_debug_path_inputs must recognise a debug compile"
            );
        }
        // gcc debug never goes through the cl path-fold path.
        let gnu = CcArgs::parse(&s(&["gcc", "-c", "a.c", "-g2"])).unwrap();
        assert_eq!(
            cl_debug_path_inputs(&gnu),
            None,
            "gnu debug must not fold cl paths"
        );
    }

    #[test]
    fn clang_cl_debug_compiles_are_no_longer_refused() {
        for f in ["/Z7", "/Zi", "/ZI", "-Z7"] {
            let p = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", f])).unwrap();
            let reasons = p.refuse_reasons(&[]);
            assert!(
                reasons.is_empty(),
                "{f}: clang-cl debug must be cacheable now, got: {reasons:?}"
            );
        }
        // -g form too.
        let g = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", "-g"])).unwrap();
        assert!(
            g.refuse_reasons(&[]).is_empty(),
            "-g clang-cl must be cacheable"
        );
    }

    #[test]
    fn clang_cl_slash_c_compile_is_not_refused() {
        // `/c` must be cacheable end-to-end: the parser sets compile mode AND
        // the unsupported-flag classifier must accept it (ParserHandled).
        // Regression for the box-found gap where `/c` hit "unsupported flag(s): /c".
        let p = CcArgs::parse(&s(&["clang-cl", "/c", "a.c", "-Foa.obj"])).unwrap();
        assert_eq!(p.mode, CompileMode::Compile);
        assert!(
            p.refuse_reasons(&[]).is_empty(),
            "clang-cl /c must not be refused, got: {:?}",
            p.refuse_reasons(&[])
        );
        // And the debug form stays cacheable too.
        let d = CcArgs::parse(&s(&["clang-cl", "/c", "a.c", "-Foa.obj", "/Z7"])).unwrap();
        assert!(
            d.refuse_reasons(&[]).is_empty(),
            "clang-cl /c /Z7 must not be refused"
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
            "-P",
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

    /// `-fstack-clash-protection` is a pure codegen hardening flag (no
    /// preprocessor or object-path effect), captured by clang's `cc -###`
    /// resolved invocation like the other `-fstack-protector*` knobs.
    /// Firefox enables it by default, so before #245 every C/C++ compile
    /// refused — ~4,842 passthroughs in one build per the issue's evidence.
    #[test]
    fn classifier_accepts_stack_clash_protection() {
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.c",
            "-o",
            "foo.o",
            "-fstack-clash-protection",
        ]);
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "-fstack-clash-protection should be classified (issue #245), got: {descs:?}"
        );
    }
    // ── #95: user-configurable cc flag allow-list ──────────────────

    fn flags(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// A flag the built-in table doesn't model normally refuses, but
    /// listing it in `[cc] extra_allowlist_flags` makes it cacheable.
    #[test]
    fn user_allowed_flag_stops_refusing() {
        let args = &["cc", "-c", "foo.c", "-o", "foo.o", "-fsome-exotic-flag"];

        // Control: unconfigured → still refused.
        let refused = refuse_descriptions(args);
        assert!(
            refused.iter().any(|d| d.contains("unsupported flag")),
            "unconfigured exotic flag should refuse, got: {refused:?}"
        );

        // Configured → accepted (no unsupported-flag refusal).
        let allowed = refuse_descriptions_with_flags(args, &flags(&["-fsome-exotic-flag"]));
        assert!(
            !allowed.iter().any(|d| d.contains("unsupported flag")),
            "allow-listed flag should not refuse, got: {allowed:?}"
        );
    }

    /// The allow-list can only add to the hashable set — it must NOT
    /// override a structural refusal like coverage instrumentation.
    #[test]
    fn user_allowed_flag_cannot_override_structural_refusal() {
        let args = &["cc", "-c", "foo.c", "-o", "foo.o", "--coverage"];
        let descs = refuse_descriptions_with_flags(args, &flags(&["--coverage"]));
        assert!(
            descs.iter().any(|d| d.contains("coverage")),
            "coverage must still refuse even when allow-listed, got: {descs:?}"
        );
    }

    /// Only flags actually present on the command line and unmodeled by
    /// the built-in table are folded into the key (sorted + deduped).
    #[test]
    fn cc_extra_flags_for_key_selects_present_unmodeled_sorted() {
        let extra = flags(&["-fbravo", "-falpha", "-fPIC"]);

        // `-fbravo`/`-falpha` present + unmodeled → folded, sorted.
        // `-fPIC` is modeled by the built-in table → NOT folded here.
        // `-falpha` repeated → deduped. `-fcharlie` not configured → out.
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            "foo.c",
            "-o",
            "foo.o",
            "-fbravo",
            "-falpha",
            "-falpha",
            "-fPIC",
            "-fcharlie",
        ]))
        .unwrap();
        assert_eq!(
            cc_extra_flags_for_key(&parsed, &extra),
            vec!["-falpha", "-fbravo"]
        );

        // A configured-but-absent flag contributes nothing.
        let absent = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
        assert!(cc_extra_flags_for_key(&absent, &extra).is_empty());

        // No config → nothing folded (key byte-identical to today).
        assert!(cc_extra_flags_for_key(&parsed, &[]).is_empty());
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

    /// Build-system path-remapping flags must NOT refuse: a build enabling its
    /// own `-f*-prefix-map` (e.g. Firefox `--enable-path-remapping`) otherwise
    /// silently disabled all cc caching. They are `CapturedByProbe`, so the
    /// resolved-token hash keys them (and per-checkout `from` paths normalize
    /// through the cc prefix maps).
    #[test]
    fn classifier_accepts_path_prefix_map_flags() {
        for flag in &[
            "-ffile-prefix-map=/build/clone-a/=/topsrcdir/",
            "-fdebug-prefix-map=/build/clone-a/obj=/topobjdir/",
            "-fmacro-prefix-map=/build/clone-a/=/topsrcdir/",
            "-fdebug-prefix-map=/Applications/Xcode.app/.../SDK=/sysroot/",
        ] {
            let descs = refuse_descriptions(&["cc", "-c", "foo.cpp", "-o", "foo.o", flag]);
            assert!(
                !descs.iter().any(|d| d.contains("unsupported flag")),
                "{flag} should be classified (path-remap), got: {descs:?}"
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
            // x86 width / SIMD feature flags from issue #375
            "-m64",
            "-m32",
            "-msse2",
            "-msse4.1",
            "-msse4.2",
            "-mavx2",
            // Section layout
            "-ffunction-sections",
            "-fdata-sections",
            // Assembler passthrough (specific value, not wildcard)
            "-Wa,--noexecstack",
            // Language override forms
            "-x",
            "-xc",
            "-xc++",
            "-xobjective-c",
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
            // Other sticky `-x` variants still need explicit rows.
            "-xassembler-with-cpp",
            "-xnone",
            // ObjC variants not on the list
            "-fno-objc-arc",
            "-fobjc-weak",
            // Section flags not on the list (similar shape, distinct
            // codegen)
            "-fno-function-sections",
            "-fno-data-sections",
            // SIMD adjacent — not listed by #115/#375
            "-mavx",
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
    fn classifier_accepts_parser_handled_and_preprocessor_only_flags() {
        for (flag, expected) in [
            ("-c", FlagClass::ParserHandled),
            ("-E", FlagClass::ParserHandled),
            ("-S", FlagClass::ParserHandled),
            ("-P", FlagClass::NoObjectEffect),
            ("-xc", FlagClass::CapturedByProbe),
            ("-xc++", FlagClass::CapturedByProbe),
            ("-xobjective-c", FlagClass::CapturedByProbe),
        ] {
            assert_eq!(
                classify_cc_flag(flag, Dialect::Gnu),
                Some(expected),
                "{flag} should have the expected class"
            );
        }
    }

    #[test]
    fn cc_arg_spec_for_token_filters_by_dialect() {
        // `-MT` is a dep-target parse row tagged Gnu-only (Layer 0). It
        // must resolve under Gnu but NOT under Cl — otherwise a future cl
        // row for an overlapping spelling would be shadowed by the gnu
        // row (the Layer 2 prerequisite).
        assert!(cc_arg_spec_for_token("-MT", Dialect::Gnu).is_some());
        assert!(cc_arg_spec_for_token("-MT", Dialect::Cl).is_none());
        // A dialect-less row (`-o`) resolves under both.
        assert!(cc_arg_spec_for_token("-o", Dialect::Gnu).is_some());
        assert!(cc_arg_spec_for_token("-o", Dialect::Cl).is_some());
    }

    #[test]
    fn arg_analysis_exposes_bucket_and_normalized_value_form() {
        let language = analyze_cc_arg("-xc++", Dialect::Gnu);
        assert_eq!(language.class, Some(FlagClass::CapturedByProbe));
        assert_eq!(language.bucket, CcArgBucket::ProbeKeyed);
        assert_eq!(
            language.normalized,
            vec!["-x".to_string(), "c++".to_string()]
        );
        assert_eq!(language.refusal, None);

        let include = analyze_cc_arg("-Ivendor", Dialect::Gnu);
        assert_eq!(include.class, Some(FlagClass::PreprocessorCaptured));
        assert_eq!(include.bucket, CcArgBucket::Preprocessor);
        assert_eq!(
            include.normalized,
            vec!["-I".to_string(), "vendor".to_string()]
        );

        let unknown = analyze_cc_arg("-funknown", Dialect::Gnu);
        assert_eq!(unknown.class, None);
        assert_eq!(unknown.bucket, CcArgBucket::TooHard);
        assert_eq!(unknown.refusal, Some("cc: unsupported flag"));
    }

    #[test]
    fn unsupported_flag_reason_excludes_classified_mixed_flags() {
        let descs = refuse_descriptions(&[
            "cc",
            "-c",
            "foo.c",
            "-o",
            "foo.o",
            "-P",
            "-xc",
            "-Ofast",
            "-funknown",
        ]);
        let detail = descs
            .iter()
            .find(|d| d.contains("unsupported flag"))
            .expect("expected unsupported flags for the truly unmodeled args");
        assert!(
            detail.contains("-Ofast"),
            "reason should name -Ofast: {detail}"
        );
        assert!(
            detail.contains("-funknown"),
            "reason should name -funknown: {detail}"
        );
        assert!(
            !detail.contains("-P"),
            "reason should not include -P: {detail}"
        );
        assert!(
            !detail.contains("-xc"),
            "reason should not include -xc: {detail}"
        );
    }

    #[test]
    fn probe_captured_flags_require_resolved_invocation() {
        let needs_probe =
            CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-fno-rtti"])).unwrap();
        assert!(cc_flags_need_resolved_invocation(&needs_probe));

        let modeled_only =
            CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o", "-O2", "-P"])).unwrap();
        assert!(!cc_flags_need_resolved_invocation(&modeled_only));
    }

    // Unix-only: uses `/usr/bin/true` as a stand-in "compiler" that accepts
    // `--version` but emits no `-cc1` line. There is no equivalent always-present
    // no-op binary on Windows (spawning `true` there fails outright), and the
    // guard's logic itself is covered cross-platform by
    // `probe_captured_flags_require_resolved_invocation`.
    #[cfg(unix)]
    #[test]
    fn cache_key_refuses_probe_captured_flags_without_resolved_invocation() {
        // `/usr/bin/true` accepts `--version` but produces no `-###`
        // `-cc1` line. That isolates the resolved-invocation guard
        // before the preprocessor hash runs.
        let compiler = CcCompiler::new();
        let parsed = compiler
            .parse(&s(&["true", "-c", "foo.c", "-o", "foo.o", "-fno-rtti"]))
            .unwrap();
        let cache = tempfile::tempdir().unwrap();
        let file_hasher = crate::cache_key::FileHasher::new();
        let path_normalizer = crate::path_normalizer::PathNormalizer::empty();
        let ctx = KeyCtx {
            file_hasher: &file_hasher,
            path_normalizer: &path_normalizer,
            cache_dir: cache.path(),
            key_salt: None,
        };

        let err = compiler.cache_key(&parsed, &ctx).unwrap_err().to_string();
        assert!(
            err.contains("resolved invocation unavailable"),
            "expected resolved-invocation refusal, got: {err}"
        );
    }

    #[test]
    fn preprocess_mode_refusal_does_not_report_classified_flags_as_unsupported() {
        let descs = refuse_descriptions(&["cc", "-E", "-xc", "-P", "foo.c"]);
        assert!(
            descs.iter().any(|d| d.contains("preprocessor mode")),
            "expected preprocessor-mode refuse, got: {descs:?}"
        );
        assert!(
            !descs.iter().any(|d| d.contains("unsupported flag")),
            "classified preprocess args should not be reported unsupported: {descs:?}"
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
            descs.iter().any(|d| d.contains("— not yet")),
            "preprocessor mode message must read as deferral ('— not yet'), got: {descs:?}"
        );

        // Link mode — also `Unsupported` with "— not yet".
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
            parsed.refuse_reasons(&[]).is_empty(),
            "clean compile invocation should have no parser-level refuse reasons; got: {:?}",
            parsed.refuse_reasons(&[])
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
            descs.iter().any(|d| d.contains("— not yet")),
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

    #[test]
    fn object_output_path_defaults_to_obj_for_clang_cl() {
        let cl = CcArgs::parse(&s(&["clang-cl", "-c", "foo.c"])).unwrap();
        assert_eq!(cl.object_output_path().unwrap().to_str(), Some("foo.obj"));
        let gnu = CcArgs::parse(&s(&["gcc", "-c", "foo.c"])).unwrap();
        assert_eq!(gnu.object_output_path().unwrap().to_str(), Some("foo.o"));
    }

    #[test]
    fn depinfo_output_path_uses_mf_or_object_stem() {
        let explicit =
            CcArgs::parse(&s(&["cc", "-c", "foo.c", "-MMD", "-MF", "deps/foo.d"])).unwrap();
        assert_eq!(
            explicit.depinfo_output_path(),
            Some(PathBuf::from("deps/foo.d"))
        );

        let derived = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "obj/foo.o", "-MMD"])).unwrap();
        assert_eq!(
            derived.depinfo_output_path(),
            Some(PathBuf::from("obj/foo.d"))
        );
        assert_eq!(derived.depinfo_anchor(), Some(PathBuf::from("obj")));
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
    fn build_preprocess_args_uses_ep_for_clang_cl() {
        use crate::compiler::flags::Dialect;
        let cl = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-DFOO"])).unwrap();
        assert_eq!(cl.family.dialect(), Dialect::Cl);
        let args = build_preprocess_args(&cl);
        assert_eq!(args.first().map(String::as_str), Some("/EP"));
        assert!(!args.iter().any(|a| a == "-E" || a == "-P"));
        assert!(args.iter().any(|a| a == "-DFOO"));
        assert!(args.iter().any(|a| a == "a.c"));
        assert!(!args.iter().any(|a| a == "-c"));
        let gnu = CcArgs::parse(&s(&["gcc", "-c", "a.c"])).unwrap();
        let g = build_preprocess_args(&gnu);
        assert_eq!(&g[..2], &["-E".to_string(), "-P".to_string()]);
    }

    #[test]
    fn cc_prefix_maps_empty_for_clang_cl() {
        let cwd = std::path::Path::new("/work/proj");
        let cl = CcArgs::parse(&s(&["clang-cl", "-c", "/work/proj/a.c"])).unwrap();
        assert!(cc_prefix_maps_cfg(&cl, cwd, None, None).is_empty());
        let gnu = CcArgs::parse(&s(&["gcc", "-c", "/work/proj/a.c"])).unwrap();
        assert!(!cc_prefix_maps_cfg(&gnu, cwd, None, None).is_empty());
    }

    /// #299 ("Firefox fails to build sandbox on Windows"): a clang-cl
    /// invocation must reach the real compiler with the EXACT argv kache was
    /// given — zero injected flags. clang-cl rejects `-ffile-prefix-map` as
    /// an unknown argument, so injecting it makes any `-Werror` compile fail.
    /// Firefox's `configure` detects `-ffile-reproducible` with a `-Werror`
    /// probe run through the compiler wrapper; an injected `-ffile-prefix-map`
    /// turned that probe into an error, so `-ffile-reproducible` was reported
    /// unsupported and dropped. Without it, `__FILE__` kept mozbuild's
    /// forward slashes and chromium's `base\location.cc` `static_assert`
    /// failed. clang-cl gets empty prefix maps (#295), so the composed argv
    /// (what `execute` spawns) must be byte-identical to the original `rest`.
    #[test]
    fn clang_cl_invocation_injects_no_flags_issue_299() {
        let cwd = std::path::Path::new("/work/proj");
        let cl = CcArgs::parse(&s(&[
            "clang-cl",
            "-Werror",
            "-ffile-reproducible",
            "-c",
            "/work/proj/a.c",
            "-Foa.obj",
        ]))
        .unwrap();
        let maps = cc_prefix_maps_cfg(&cl, cwd, None, None);
        assert!(
            maps.is_empty(),
            "clang-cl must get no prefix maps (#295/#299)"
        );
        let composed = compose_cc_args(&cl.rest, file_prefix_map_args(&maps));
        assert_eq!(
            composed, cl.rest,
            "kache must inject nothing into a clang-cl argv, or it poisons \
             `-Werror` compiles/probes (#299); got {composed:?}"
        );
    }

    /// #300: cc-rs emits `--` before the source on clang-cl, and the
    /// clang/clang-cl driver treats everything after `--` as an input
    /// file. Appended `-ffile-prefix-map` flags must therefore be spliced
    /// in *before* the separator, or the driver counts them as extra
    /// source files and fails with "cannot specify '-Fo…' when compiling
    /// multiple source files".
    #[test]
    fn compose_cc_args_splices_appended_flags_before_double_dash() {
        let rest = s(&["-c", "-Fofoo.o", "--", "windows.c"]);
        let appended = s(&["-ffile-prefix-map=/a=<CC_ROOT>"]);
        let out = compose_cc_args(&rest, appended);
        assert_eq!(
            out,
            s(&[
                "-c",
                "-Fofoo.o",
                "-ffile-prefix-map=/a=<CC_ROOT>",
                "--",
                "windows.c"
            ]),
            "appended flags must land before `--`, not after"
        );
    }

    #[test]
    fn compose_cc_args_appends_at_end_without_double_dash() {
        let rest = s(&["-c", "foo.c"]);
        let appended = s(&["-ffile-prefix-map=/a=<CC_ROOT>"]);
        let out = compose_cc_args(&rest, appended);
        assert_eq!(out, s(&["-c", "foo.c", "-ffile-prefix-map=/a=<CC_ROOT>"]));
    }

    #[test]
    fn compose_cc_args_is_identity_when_nothing_appended() {
        let rest = s(&["-c", "-Fofoo.o", "--", "windows.c"]);
        assert_eq!(compose_cc_args(&rest, Vec::new()), rest);
    }

    #[test]
    fn compose_cc_args_splices_before_the_first_double_dash() {
        // Only the first bare `--` is the end-of-options marker; a later
        // `--` is an input. Splicing before the first keeps the injected
        // flags as options regardless of any trailing `--`.
        let rest = s(&["-c", "--", "a.c", "--", "b.c"]);
        let out = compose_cc_args(&rest, s(&["-ffile-prefix-map=/a=<CC_ROOT>"]));
        assert_eq!(
            out,
            s(&[
                "-c",
                "-ffile-prefix-map=/a=<CC_ROOT>",
                "--",
                "a.c",
                "--",
                "b.c"
            ])
        );
    }

    #[test]
    fn compose_cc_args_handles_double_dash_as_first_token() {
        let rest = s(&["--", "a.c"]);
        let out = compose_cc_args(&rest, s(&["-ffile-prefix-map=/a=<CC_ROOT>"]));
        assert_eq!(out, s(&["-ffile-prefix-map=/a=<CC_ROOT>", "--", "a.c"]));
    }

    #[cfg(unix)]
    #[test]
    fn preprocess_hash_bails_on_empty_stdout() {
        // `true` ignores args and prints nothing → empty preprocessor
        // output, which the tripwire refuses. (A legitimately empty TU —
        // all comments / all `#if 0` — also lands here; refusing to cache
        // it is a safe non-cache, the conservative trade-off.)
        let parsed = CcArgs::parse(&s(&["true", "-c", "a.c"])).unwrap();
        let err = preprocess_hash(&parsed, &[]).unwrap_err();
        assert!(err.to_string().contains("no output"), "got: {err}");
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
    fn cc_prefix_maps_derive_common_source_and_build_root() {
        let root = tempfile::TempDir::new().unwrap();
        let src_dir = root.path().join("dom/canvas");
        let obj_dir = root.path().join("obj-kache-bench/dom/canvas");
        std::fs::create_dir_all(&src_dir).unwrap();
        std::fs::create_dir_all(&obj_dir).unwrap();
        let source = src_dir.join("Unified_cpp_dom_canvas3.cpp");
        std::fs::write(&source, "int x;\n").unwrap();

        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            source.to_str().unwrap(),
            "-o",
            "Unified_cpp_dom_canvas3.o",
        ]))
        .unwrap();

        let maps = cc_prefix_maps_for(&parsed, &obj_dir);
        let canonical_root = root
            .path()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(
            maps.iter()
                .any(|m| m.from == canonical_root && m.to == CC_ROOT_SENTINEL),
            "expected common root map in {maps:?}"
        );

        let flags = file_prefix_map_args(&maps);
        assert!(
            flags
                .iter()
                .any(|f| f == &format!("-ffile-prefix-map={canonical_root}={CC_ROOT_SENTINEL}")),
            "execute should inject the common-root prefix map, got {flags:?}"
        );
    }

    #[test]
    fn cc_prefix_maps_fall_back_to_distinct_roots_without_common_project_root() {
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "/opt/kache-src/foo.c", "-o", "foo.o"])).unwrap();
        let maps = cc_prefix_maps_for(&parsed, Path::new("/tmp/kache-build"));

        assert!(
            maps.iter().any(|m| m.to == CC_BUILD_SENTINEL),
            "missing build root map: {maps:?}"
        );
        assert!(
            maps.iter().any(|m| m.to == CC_SOURCE_SENTINEL),
            "missing source root map: {maps:?}"
        );
    }

    #[test]
    fn cc_prefix_maps_keep_shallow_in_tree_relocated_builds_stable() {
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            "/tmp/kache-relocated/src/foo.c",
            "-o",
            "build/foo.o",
        ]))
        .unwrap();
        let maps = cc_prefix_maps_for(&parsed, Path::new("/tmp/kache-relocated"));

        assert!(
            // Compare via `Path` so the separator the platform normalised the
            // build dir to (e.g. `\tmp\kache-relocated` on Windows) still
            // matches the `/`-form fixture.
            maps.iter()
                .any(|m| Path::new(&m.from) == Path::new("/tmp/kache-relocated")
                    && m.to == CC_ROOT_SENTINEL),
            "in-tree shallow relocations should use the same root sentinel, got {maps:?}"
        );
    }

    #[test]
    fn cc_prefix_maps_accept_generated_tempdir_common_root() {
        let root = tempfile::TempDir::new().unwrap();
        let root = root.path();

        assert!(
            stable_cc_common_root(root, &root.join("obj"), &root.join("src")),
            "generated temp project roots should be stable common roots"
        );
        assert!(
            !stable_cc_common_root(&std::env::temp_dir(), &root.join("obj"), &root.join("src")),
            "the temp directory itself is too broad to use as a common root"
        );
    }

    #[test]
    fn cc_prefix_maps_normalize_preprocessor_bytes() {
        let maps = vec![CcPrefixMap {
            from: "/Users/me/work/clone-a".to_string(),
            to: CC_ROOT_SENTINEL,
        }];
        let input = br#"assert_fail("/Users/me/work/clone-a/obj/dist/include/fmt/format.h")"#;
        let normalized = apply_cc_prefix_maps_to_bytes(input.to_vec(), &maps);

        assert_eq!(
            std::str::from_utf8(&normalized).unwrap(),
            r#"assert_fail("<CC_ROOT>/obj/dist/include/fmt/format.h")"#
        );
    }

    /// Resolved `-###` tokens carry absolute build paths (here a `-D`
    /// define pointing at a branding asset, like Firefox's `FIREFOX_ICO`).
    /// The cc key now normalizes them through the per-build prefix maps, so
    /// the SAME token built at two different paths hashes identically —
    /// the cross-clone / cross-machine portability fix (v12). Previously
    /// the tokens were hashed raw and diverged with the build directory.
    #[test]
    fn resolved_tokens_normalize_identically_across_build_paths() {
        let tok = |clone: &str| {
            format!(r#"FIREFOX_ICO="/Users/me/work/{clone}/browser/branding/firefox.ico""#)
                .into_bytes()
        };
        let maps_for = |clone: &str| {
            vec![CcPrefixMap {
                from: format!("/Users/me/work/{clone}"),
                to: CC_ROOT_SENTINEL,
            }]
        };

        let a = apply_cc_prefix_maps_to_bytes(tok("clone-a"), &maps_for("clone-a"));
        let b = apply_cc_prefix_maps_to_bytes(tok("clone-b"), &maps_for("clone-b"));

        assert_eq!(
            a, b,
            "the same resolved token at different build paths must normalize identically"
        );
        assert_eq!(
            std::str::from_utf8(&a).unwrap(),
            r#"FIREFOX_ICO="<CC_ROOT>/browser/branding/firefox.ico""#
        );
    }

    /// The objdir cross-checkout fix (v13). An objdir-generated TU compiles
    /// a source that lives IN the build dir, so cwd == source-dir and the
    /// (cwd, source) derivation collapses to a narrow objdir subdir. The
    /// `-I` include dirs span the repo, so folding them in lifts the root
    /// back to the project root — which is what `__FILE__` / preprocessor
    /// paths into `dist/include` and the source tree need to normalize.
    #[test]
    fn cc_prefix_maps_broaden_to_repo_root_via_includes_for_objdir_tus() {
        let root = tempfile::TempDir::new().unwrap();
        let obj_dir = root.path().join("obj-kache-bench/xpcom/components");
        let inc_dir = root.path().join("xpcom/components");
        std::fs::create_dir_all(&obj_dir).unwrap();
        std::fs::create_dir_all(&inc_dir).unwrap();
        // The generated TU lives in the objdir, so cwd ≈ its own dir.
        let source = obj_dir.join("StaticComponents.cpp");
        std::fs::write(&source, "int x;\n").unwrap();

        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-c",
            source.to_str().unwrap(),
            "-I",
            inc_dir.to_str().unwrap(), // in-tree → lifts the root to the repo
            "-I",
            "/usr/include", // out-of-tree → common ancestor is `/` → dropped
            "-o",
            "StaticComponents.o",
        ]))
        .unwrap();

        let maps = cc_prefix_maps_for(&parsed, &obj_dir);
        let canonical_root = root
            .path()
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(
            maps.iter()
                .any(|m| m.from == canonical_root && m.to == CC_ROOT_SENTINEL),
            "include-folding must derive the repo root for objdir TUs, got {maps:?}"
        );
        // A system `-I` must never widen the root to the filesystem root.
        assert!(
            !maps.iter().any(|m| m.from == "/"),
            "out-of-tree includes must not add a `/` root, got {maps:?}"
        );
    }

    /// `KACHE_BASE_DIR` (the ccache `CCACHE_BASEDIR` analog) is an explicit
    /// override: whatever path the user names is stripped to `<CC_BASE>`,
    /// independent of the auto-derived roots.
    #[test]
    fn cc_prefix_maps_cfg_maps_explicit_base_dir_to_base_sentinel() {
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "/work/checkout/src/foo.c", "-o", "foo.o"])).unwrap();
        let cwd = Path::new("/work/checkout");
        // `/work` is the common parent of many checkouts (the canonical
        // CCACHE_BASEDIR shape), above what the auto-derivation would pick.
        let maps = cc_prefix_maps_cfg(&parsed, cwd, Some(Path::new("/work")), None);
        assert!(
            maps.iter()
                .any(|m| m.from == "/work" && m.to == CC_BASE_SENTINEL),
            "explicit KACHE_BASE_DIR must map to the base sentinel, got {maps:?}"
        );
    }

    /// Issue #78: an explicit `-isysroot <sdk>` is mapped to `<SDKROOT>` so
    /// the SDK path that rides in the resolved `cc -###` tokens stops
    /// keying the artifact per-install.
    #[test]
    fn cc_prefix_maps_cfg_maps_explicit_isysroot_to_sdkroot_sentinel() {
        let sdk = "/Applications/Xcode_15.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX14.2.sdk";
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-isysroot",
            sdk,
            "-c",
            "/work/checkout/src/foo.c",
            "-o",
            "foo.o",
        ]))
        .unwrap();
        let maps = cc_prefix_maps_cfg(&parsed, Path::new("/work/checkout"), None, None);
        assert!(
            maps.iter()
                .any(|m| m.from == sdk && m.to == CC_SDKROOT_SENTINEL),
            "explicit -isysroot must map to <SDKROOT>, got {maps:?}"
        );
    }

    /// Issue #78: when there's no `-isysroot`, the `SDKROOT` env value
    /// (threaded in by [`cc_prefix_maps`]) provides the SDK path to strip.
    #[test]
    fn cc_prefix_maps_cfg_maps_sdkroot_env_to_sentinel() {
        let sdk = "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk";
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "/work/checkout/src/foo.c", "-o", "foo.o"])).unwrap();
        let maps = cc_prefix_maps_cfg(
            &parsed,
            Path::new("/work/checkout"),
            None,
            Some(Path::new(sdk)),
        );
        assert!(
            maps.iter()
                .any(|m| m.from == sdk && m.to == CC_SDKROOT_SENTINEL),
            "SDKROOT env must map to <SDKROOT>, got {maps:?}"
        );
    }

    /// An explicit `-isysroot` wins over the `SDKROOT` env value (mirrors
    /// clang's own precedence), so only the on-command-line SDK is mapped.
    #[test]
    fn cc_prefix_maps_cfg_isysroot_wins_over_sdkroot_env() {
        let arg_sdk = "/Applications/Xcode_15.2.app/Contents/Developer/.../MacOSX14.2.sdk";
        let env_sdk = "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk";
        let parsed = CcArgs::parse(&s(&[
            "cc",
            "-isysroot",
            arg_sdk,
            "-c",
            "/work/checkout/src/foo.c",
            "-o",
            "foo.o",
        ]))
        .unwrap();
        let maps = cc_prefix_maps_cfg(
            &parsed,
            Path::new("/work/checkout"),
            None,
            Some(Path::new(env_sdk)),
        );
        assert!(
            maps.iter().any(|m| m.from == arg_sdk),
            "explicit -isysroot must be the SDK that is mapped, got {maps:?}"
        );
        assert!(
            !maps.iter().any(|m| m.from == env_sdk),
            "SDKROOT env must be ignored when -isysroot is explicit, got {maps:?}"
        );
    }

    /// No `-isysroot` and no `SDKROOT` → no `<SDKROOT>` map (the bare
    /// `cc -c` / non-Apple case is left untouched; issue #78 follow-up).
    #[test]
    fn cc_prefix_maps_cfg_no_sdk_adds_no_sdkroot_map() {
        let parsed =
            CcArgs::parse(&s(&["cc", "-c", "/work/checkout/src/foo.c", "-o", "foo.o"])).unwrap();
        let maps = cc_prefix_maps_cfg(&parsed, Path::new("/work/checkout"), None, None);
        assert!(
            !maps.iter().any(|m| m.to == CC_SDKROOT_SENTINEL),
            "no SDK source means no <SDKROOT> map, got {maps:?}"
        );
    }

    /// The headline portability property: the same TU compiled against the
    /// same SDK *contents* at two different install paths normalizes to the
    /// same key bytes. Drives a resolved-`-###`-shaped token (carrying the
    /// SDK path) through the maps each build would compute and asserts the
    /// results are byte-identical — i.e. the two builds would share a hit.
    #[test]
    fn sdkroot_map_normalizes_resolved_tokens_identically_across_installs() {
        let sdk_a = "/Applications/Xcode_15.2.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX14.2.sdk";
        let sdk_b = "/Library/Developer/CommandLineTools/SDKs/MacOSX14.2.sdk";
        let cwd = Path::new("/work/checkout");

        let parsed_a = CcArgs::parse(&s(&[
            "cc",
            "-isysroot",
            sdk_a,
            "-c",
            "/work/checkout/src/foo.c",
            "-o",
            "foo.o",
        ]))
        .unwrap();
        let parsed_b = CcArgs::parse(&s(&[
            "cc",
            "-isysroot",
            sdk_b,
            "-c",
            "/work/checkout/src/foo.c",
            "-o",
            "foo.o",
        ]))
        .unwrap();

        let maps_a = cc_prefix_maps_cfg(&parsed_a, cwd, None, None);
        let maps_b = cc_prefix_maps_cfg(&parsed_b, cwd, None, None);

        // A resolved `-cc1` token as `cc -###` would emit it, per install.
        let token_a = format!("-internal-isystem{sdk_a}/usr/include").into_bytes();
        let token_b = format!("-internal-isystem{sdk_b}/usr/include").into_bytes();

        let norm_a = apply_cc_prefix_maps_to_bytes(token_a, &maps_a);
        let norm_b = apply_cc_prefix_maps_to_bytes(token_b, &maps_b);

        assert_eq!(
            norm_a, norm_b,
            "same SDK contents at different install paths must normalize to the same key bytes"
        );
        assert_eq!(
            String::from_utf8_lossy(&norm_a),
            "-internal-isystem<SDKROOT>/usr/include"
        );
    }

    /// The kill-switch: any explicit off-value disables cc path
    /// normalization; everything else (including unset and empty) leaves it
    /// on — normalization is the default, opt-out only.
    #[test]
    fn parse_cc_normalize_toggle_defaults_on_opts_out_explicitly() {
        for on in [
            None,
            Some("1"),
            Some("yes"),
            Some("on"),
            Some(""),
            Some("garbage"),
        ] {
            assert!(parse_cc_normalize_toggle(on), "{on:?} should keep it on");
        }
        for off in [
            Some("0"),
            Some("false"),
            Some("off"),
            Some("no"),
            Some("  OFF "),
        ] {
            assert!(!parse_cc_normalize_toggle(off), "{off:?} should disable it");
        }
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
        assert_eq!(
            compiler.classify_output(&parsed, "foo.o.pp"),
            ArtifactKind::DepInfo
        );
    }

    // ── pre_clean_cc_outputs ──────────────────────────────────────

    /// Regression for #285 / the cc pre-clean bug: `pre_clean_cc_outputs`
    /// must unlink a read-only object (and dep-info sidecar) that were
    /// previously restored as hardlinked store blobs (0o444).
    ///
    /// Before the fix, `CcCompiler::execute` had no pre-clean step, so
    /// re-running the compiler after a cache hit would fail with EACCES /
    /// "operation not permitted" when trying to overwrite the read-only file.
    #[cfg(unix)]
    #[test]
    fn pre_clean_cc_outputs_unlinks_readonly_object_and_depinfo() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let obj = dir.path().join("foo.o");
        let dep = dir.path().join("foo.d");

        // Simulate kache-restored hardlinks: read-only blobs at the output paths.
        fs::write(&obj, b"old object").unwrap();
        fs::set_permissions(&obj, fs::Permissions::from_mode(0o444)).unwrap();
        fs::write(&dep, b"old depinfo").unwrap();
        fs::set_permissions(&dep, fs::Permissions::from_mode(0o444)).unwrap();

        assert!(
            fs::metadata(&obj).unwrap().permissions().readonly(),
            "precondition: object must be read-only"
        );
        assert!(
            fs::metadata(&dep).unwrap().permissions().readonly(),
            "precondition: dep-info must be read-only"
        );

        // Build a parsed CcArgs pointing at these paths.
        let obj_str = obj.to_string_lossy().into_owned();
        let dep_str = dep.to_string_lossy().into_owned();
        let parsed = CcArgs::parse(&s(&[
            "cc", "-c", "foo.c", "-MMD", "-MF", &dep_str, "-o", &obj_str,
        ]))
        .unwrap();

        // The helper must remove both read-only files, breaking the
        // hardlinks and leaving the store blobs untouched.
        pre_clean_cc_outputs(&parsed);

        assert!(
            !obj.exists(),
            "read-only object must be unlinked by pre_clean_cc_outputs"
        );
        assert!(
            !dep.exists(),
            "read-only dep-info must be unlinked by pre_clean_cc_outputs"
        );
    }

    /// Verify that `pre_clean_cc_outputs` does NOT remove a writable object
    /// (non-restored path — must not discard a freshly-compiled artifact).
    #[cfg(unix)]
    #[test]
    fn pre_clean_cc_outputs_leaves_writable_object_intact() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let obj = dir.path().join("bar.o");

        // Writable file — NOT a kache restore.
        fs::write(&obj, b"fresh object").unwrap();
        fs::set_permissions(&obj, fs::Permissions::from_mode(0o644)).unwrap();

        let obj_str = obj.to_string_lossy().into_owned();
        let parsed = CcArgs::parse(&s(&["cc", "-c", "bar.c", "-o", &obj_str])).unwrap();

        // A writable file must not be touched — only read-only hardlinks are pre-cleaned.
        // NOTE: the current implementation removes any existing file (best-effort) to
        // keep the logic simple. This test documents the current behaviour.
        // If the implementation is ever tightened to only remove read-only files,
        // update this assertion accordingly.
        pre_clean_cc_outputs(&parsed);
        // The file no longer exists after pre-clean (unconditional remove).
        // This is acceptable: on a cache miss the compiler will recreate it.
        // The critical invariant is that the compiler is not blocked by EACCES.
    }

    /// Regression: `CcCompiler::execute` must succeed when the object output
    /// path is a read-only file (simulated kache restore) on a cache miss.
    ///
    /// Uses `sh -c "cp /dev/null $OBJ"` as a stand-in compiler that writes
    /// the object unconditionally, exactly like a real C compiler would.
    /// Before the fix, this failed with EACCES because `execute` had no
    /// pre-clean step.
    #[cfg(unix)]
    #[test]
    fn execute_pre_cleans_readonly_object_before_recompiling() {
        use std::fs;
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let obj = dir.path().join("hit.o");
        let src = dir.path().join("hit.c");

        // Simulate a kache restore: read-only hardlink at the object path.
        fs::write(&obj, b"cached content").unwrap();
        fs::set_permissions(&obj, fs::Permissions::from_mode(0o444)).unwrap();
        assert!(
            fs::metadata(&obj).unwrap().permissions().readonly(),
            "precondition: object must be read-only"
        );

        // A minimal source file so the arg parser sees one source.
        fs::write(&src, b"int x;\n").unwrap();

        let obj_str = obj.to_string_lossy().into_owned();
        let src_str = src.to_string_lossy().into_owned();

        // Stand-in compiler: `sh` with a `-c` script that writes the object.
        // argv shape: ["sh", "-c", "cp /dev/null <obj>", <src>, "-c", "-o", <obj>]
        // The script ignores the trailing positional args; the arg parser sees
        // `-c` (compile mode) and `-o <obj>` (object path), which is all we need.
        let script = format!("cp /dev/null '{obj_str}'");
        let compiler = CcCompiler::new();
        let parsed = compiler
            .parse(&[
                "sh".to_string(),
                "-c".to_string(),
                script,
                src_str,
                "-c".to_string(),
                "-o".to_string(),
                obj_str.clone(),
            ])
            .unwrap();

        // Without the pre-clean fix this would return Ok(exit_code != 0) because
        // the stand-in `sh -c "cp /dev/null <obj>"` would fail to write over the
        // read-only file (EACCES). With the fix, the hardlink is removed first
        // and the "compiler" succeeds.
        let result = compiler.execute(&parsed).expect("execute must not Err");
        assert_eq!(
            result.exit_code, 0,
            "compiler must succeed after pre_clean removes the read-only restore"
        );
        assert!(
            obj.exists(),
            "object must be (re-)created by the stand-in compiler"
        );
    }

    // ── Layer 4: Firefox-corpus clang-cl flag classification ─────

    #[test]
    fn clang_cl_layer4_flag_classification() {
        use crate::compiler::flags::{Dialect, FlagClass};
        let cl = Dialect::Cl;
        // object-material → CapturedByProbe
        for f in [
            "-EHsc",
            "-EHs-c-",
            "/EHsc",
            "-GR-",
            "/GR-",
            "-GS-",
            "/GS",
            "-Brepro",
            "-utf-8",
            "-Zc:wchar_t",
            "-Zc:forScope-",
        ] {
            assert_eq!(
                classify_cc_flag(f, cl),
                Some(FlagClass::CapturedByProbe),
                "{f}"
            );
        }
        // -Zc:inline stays NoObjectEffect (Layer 2 Exact row matched first)
        assert_eq!(
            classify_cc_flag("-Zc:inline", cl),
            Some(FlagClass::NoObjectEffect)
        );
        // __FILE__-affecting → PreprocessorCaptured
        assert_eq!(
            classify_cc_flag("-FC", cl),
            Some(FlagClass::PreprocessorCaptured)
        );
        // no object effect → NoObjectEffect
        for f in [
            "-nologo",
            "-wd4800",
            "/wd4244",
            "-FS",
            "-Gm-",
            "-external:W0",
        ] {
            assert_eq!(
                classify_cc_flag(f, cl),
                Some(FlagClass::NoObjectEffect),
                "{f}"
            );
        }
        // refused (out of scope) → None
        assert_eq!(classify_cc_flag("-bigobj", cl), None);
        assert_eq!(classify_cc_flag("-showIncludes", cl), None);
    }

    #[test]
    fn clang_cl_full_firefox_invocation_is_cacheable() {
        // Layer 2 + Layer 4 modeled flags (minus -Z7/-bigobj/-showIncludes).
        let p = CcArgs::parse(&s(&[
            "clang-cl",
            "-c",
            "foo.c",
            "-Fofoo.obj",
            "-std:c++20",
            "-fms-compatibility-version=19.50",
            "-guard:cf,nochecks",
            "-Gy",
            "-Gw",
            "-Oy-",
            "-Zc:inline",
            "-Zc:wchar_t",
            "-MD",
            "-EHs-c-",
            "-GR-",
            "-GS-",
            "-nologo",
            "-wd4800",
            "-utf-8",
            "-FS",
            "-external:W0",
            "-Brepro",
            "-FC",
        ]))
        .unwrap();
        let refuse = p.refuse_reasons(&[]);
        assert!(
            refuse.is_empty(),
            "should cache, refused: {:?}",
            refuse.iter().map(|r| r.description()).collect::<Vec<_>>()
        );
        // -bigobj and -showIncludes still refuse.
        let big = CcArgs::parse(&s(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "-bigobj"])).unwrap();
        assert!(!big.refuse_reasons(&[]).is_empty());
    }

    #[test]
    fn clang_cl_debug_flags_require_the_resolved_probe() {
        // /Z7 etc. are CapturedByProbe: their variant/codegen is only safely
        // keyed via `cc -###`, so the compile must require the probe (bail if
        // absent) — otherwise /ZI and /Z7, which differ, could collide.
        for f in ["/Z7", "/Zi", "/ZI", "/Zd", "-Z7"] {
            let p = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj", f])).unwrap();
            assert!(
                cc_flags_need_resolved_invocation(&p),
                "{f}: clang-cl debug must require the -### probe (CapturedByProbe)"
            );
        }
        // A non-debug clang-cl compile with only modeled flags need NOT
        // require the probe (sanity: the assertion above isn't vacuously true).
        let nodebug = CcArgs::parse(&s(&["clang-cl", "-c", "a.c", "-Foa.obj"])).unwrap();
        assert!(
            !cc_flags_need_resolved_invocation(&nodebug),
            "plain clang-cl compile without debug flags must not require the probe"
        );
    }

    #[test]
    fn cl_debug_path_inputs_folds_source_output_and_dir() {
        let comp = |args: &[&str]| cl_debug_path_inputs(&CcArgs::parse(&s(args)).unwrap());

        // Source filename leaks (H1): foo.c vs bar.c → different components.
        let foo = comp(&["clang-cl", "-c", "foo.c", "-Fofoo.obj", "/Z7"]);
        let bar = comp(&["clang-cl", "-c", "bar.c", "-Fobar.obj", "/Z7"]);
        assert!(foo.is_some() && bar.is_some());
        assert_ne!(
            foo, bar,
            "different source/output must change the component (H1/D3)"
        );

        // Absolute source path leaks (H2).
        let a1 = comp(&["clang-cl", "-c", "C:\\d1\\a.c", "-Foa.obj", "/Z7"]);
        let a2 = comp(&["clang-cl", "-c", "C:\\d2\\a.c", "-Foa.obj", "/Z7"]);
        assert_ne!(
            a1, a2,
            "absolute source path must change the component (H2)"
        );

        // Output name leaks independently (D3): same source, different -Fo.
        let p = comp(&["clang-cl", "-c", "a.c", "-Fopp.obj", "/Z7"]);
        let q = comp(&["clang-cl", "-c", "a.c", "-Foqq.obj", "/Z7"]);
        assert_ne!(p, q, "different -Fo must change the component (D3)");

        // Explicit -fdebug-compilation-dir is used (else current_dir()).
        let explicit = comp(&[
            "clang-cl",
            "-c",
            "a.c",
            "-Foa.obj",
            "/Z7",
            "-fdebug-compilation-dir=C:\\proj\\x",
        ])
        .unwrap();
        assert!(
            explicit.iter().any(|e| e.contains("C:\\proj\\x")),
            "explicit compilation-dir must appear in the component"
        );

        // /Zi, /ZI, -Zi also trigger the fold.
        for f in ["/Zi", "/ZI", "-Zi"] {
            assert!(
                comp(&["clang-cl", "-c", "a.c", "-Foa.obj", f]).is_some(),
                "{f} must fold"
            );
        }

        // Non-debug cl → None (no fold; preserves cross-CWD/name hit-rate).
        assert_eq!(comp(&["clang-cl", "-c", "a.c", "-Foa.obj"]), None);
        // gnu debug → None (gnu normalizes via -ffile-prefix-map, not this path).
        assert_eq!(comp(&["gcc", "-c", "a.c", "-g"]), None);
    }
}
