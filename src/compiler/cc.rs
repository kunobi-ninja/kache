//! C-family compiler skeleton (cc / gcc / g++ / clang / clang++ / c++).
//!
//! **Phase 0 — passthrough only.** This module establishes the wrapper-mode
//! detection plumbing and invocation surface for C-family compilers. It does
//! NOT cache anything yet: `refuse_reasons` returns
//! [`RefuseReason::Unsupported`] unconditionally, which routes every C/C++
//! invocation through the wrapper's passthrough path. Real caching (arg
//! parsing, preprocessor hash, output classification, refuse-to-cache list)
//! lands incrementally in follow-up PRs against the e2e test surface this
//! commit also adds.
//!
//! What works today:
//! - `CC=kache cc` and `CXX=kache c++` are recognized as wrapper invocations
//!   instead of failing the CLI subcommand parser.
//! - The compiler is invoked with the original argv; stdout / stderr / exit
//!   code are propagated transparently.
//! - The skeleton validates the [`Compiler`] trait against a second compiler
//!   family without any per-compiler shortcuts in shared code.
//!
//! Future work (separate PRs):
//! - Argument parsing (top ~10 flags first, refuse the rest)
//! - Preprocessor-based cache key (`cc -E` + blake3 + `SOURCE_DATE_EPOCH`
//!   injection to neutralize `__DATE__` / `__TIME__` macros)
//! - Output discovery (`.o`, `.d`, executables, `.dylib`/`.so`/`.dll`)
//! - Refuse-to-cache list (response files, multi-arch, `--coverage`,
//!   `-gsplit-dwarf`, PCH, modules)

use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;

use super::{
    ArtifactKind, CompileResult, Compiler, CompilerKind, KeyCtx, RefuseReason, classify_by_filename,
};

/// Recognise a C-family compiler invocation by argv[0].
///
/// Matches: `cc`, `gcc`, `g++`, `clang`, `clang++`, `c++`, plus version
/// suffixes like `gcc-13`, `clang-15`. Path-prefixed forms
/// (`/usr/bin/cc`) work via [`Path::file_name`].
pub fn looks_like_cc(arg: &str) -> bool {
    let path = Path::new(arg);
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

/// Parsed C-family invocation. Skeleton stores only what's needed to
/// re-execute the compiler verbatim — real arg classification (preprocessor
/// vs common vs unhashed vs refuse) lands in a follow-up.
#[derive(Debug, Clone)]
pub struct CcArgs {
    /// argv[0] — the compiler binary path the wrapper was invoked as.
    pub program: String,
    /// argv[1..] — passed through to the compiler unchanged.
    pub rest: Vec<String>,
}

impl CcArgs {
    pub fn parse(args: &[String]) -> Result<Self> {
        let (program, rest) = args
            .split_first()
            .context("cc invocation missing argv[0]")?;
        Ok(Self {
            program: program.clone(),
            rest: rest.to_vec(),
        })
    }
}

#[derive(Default)]
pub struct CcCompiler;

impl CcCompiler {
    pub fn new() -> Self {
        Self
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

    fn refuse_reasons(&self, _parsed: &CcArgs) -> Vec<RefuseReason> {
        // Skeleton: refuse every C/C++ invocation. Wrapper sees this and
        // routes to passthrough. Removing this is the signal that real
        // caching has landed.
        vec![RefuseReason::Unsupported(
            "cc-family caching not yet implemented (skeleton only)",
        )]
    }

    fn cache_key(&self, _parsed: &CcArgs, _ctx: &KeyCtx<'_>) -> Result<String> {
        // Unreachable while refuse_reasons returns Unsupported. Documented
        // as a precondition rather than panicking so a future regression
        // (wrapper calling cache_key without checking refuse) gets a clear
        // error instead of silent miscaching.
        anyhow::bail!("CcCompiler::cache_key called while caching is not yet implemented")
    }

    fn execute(&self, parsed: &CcArgs) -> Result<CompileResult> {
        // Plain passthrough: invoke the underlying compiler with the
        // original argv, capture stdout/stderr/exit. No output discovery
        // (caching not active), so output_files stays empty.
        let output = Command::new(&parsed.program)
            .args(&parsed.rest)
            .output()
            .with_context(|| format!("executing {}", parsed.program))?;

        Ok(CompileResult {
            exit_code: output.status.code().unwrap_or(1),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            output_files: Vec::new(),
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
    fn looks_like_cc_matches_canonical_command_names() {
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
            assert!(looks_like_cc(name), "should recognize {name}");
        }
    }

    #[test]
    fn looks_like_cc_matches_versioned_variants() {
        for name in ["gcc-13", "clang-15", "g++-12", "clang++-17"] {
            assert!(looks_like_cc(name), "should recognize versioned {name}");
        }
    }

    #[test]
    fn looks_like_cc_rejects_non_c_compilers() {
        for name in [
            "rustc",
            "ld",
            "ar",
            "make",
            "cmake",
            "ccache",
            "--crate-name",
        ] {
            assert!(!looks_like_cc(name), "should NOT recognize {name}");
        }
    }

    #[test]
    fn parse_splits_program_from_rest() {
        let parsed = CcArgs::parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"])).unwrap();
        assert_eq!(parsed.program, "cc");
        assert_eq!(parsed.rest, vec!["-c", "foo.c", "-o", "foo.o"]);
    }

    #[test]
    fn refuse_reasons_always_unsupported_in_skeleton() {
        // Locks the skeleton contract: until real caching lands, every
        // C/C++ invocation must route through passthrough. When this
        // test starts failing, real caching has arrived (or someone
        // skipped the refuse step — that's the bug to investigate).
        let compiler = CcCompiler::new();
        let parsed = compiler
            .parse(&s(&["cc", "-c", "foo.c", "-o", "foo.o"]))
            .unwrap();
        let reasons = compiler.refuse_reasons(&parsed);
        assert!(matches!(reasons.as_slice(), [RefuseReason::Unsupported(_)]));
    }

    #[test]
    fn classify_output_delegates_to_shared_classifier() {
        // Sanity: c-family extensions go through the same source of
        // truth as rustc's extension table.
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
