//! Rustc implementation of the [`Compiler`] trait.
//!
//! Phase 0: a thin facade over the existing free functions in
//! [`crate::args`], [`crate::cache_key`], and [`crate::compile`]. Those
//! functions remain the canonical implementations; the trait simply gives
//! callers a stable shape that future compilers (gcc, clang) will match.

use anyhow::Result;
use std::path::Path;

use crate::args::RustcArgs;
use crate::cache_key::compute_cache_key;
use crate::compile;

use super::{CompileResult, Compiler, CompilerKind, KeyCtx, RefuseReason};

/// Check if an argv element looks like an invocation of rustc (or
/// clippy-driver, which wraps rustc). Used by [`super::detect_compiler`] to
/// route argv into the rustc dispatch path.
pub fn looks_like_rustc(arg: &str) -> bool {
    let path = Path::new(arg);
    match path.file_name() {
        Some(name) => {
            let name = name.to_string_lossy();
            name == "rustc" || name.starts_with("rustc") || name == "clippy-driver"
        }
        None => false,
    }
}

#[derive(Default)]
pub struct RustcCompiler;

impl RustcCompiler {
    pub fn new() -> Self {
        Self
    }
}

impl Compiler for RustcCompiler {
    type Parsed = RustcArgs;

    fn kind(&self) -> CompilerKind {
        CompilerKind::Rustc
    }

    fn parse(&self, args: &[String]) -> Result<RustcArgs> {
        RustcArgs::parse(args)
    }

    fn refuse_reasons(&self, parsed: &RustcArgs) -> Vec<RefuseReason> {
        let mut reasons = Vec::new();
        if !parsed.is_primary {
            reasons.push(RefuseReason::NotPrimary);
        }
        reasons
    }

    fn cache_key(&self, parsed: &RustcArgs, ctx: &KeyCtx<'_>) -> Result<String> {
        compute_cache_key(parsed, ctx.file_hasher)
    }

    fn execute(&self, parsed: &RustcArgs) -> Result<CompileResult> {
        compile::run_rustc(
            &parsed.rustc,
            parsed.inner_rustc.as_deref(),
            &parsed.all_args,
            parsed.output.as_deref(),
            parsed.out_dir.as_deref(),
            parsed.crate_name.as_deref(),
            parsed.extra_filename.as_deref(),
            parsed.has_coverage_instrumentation(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn looks_like_rustc_matches_rustc_and_clippy_driver() {
        assert!(looks_like_rustc("rustc"));
        assert!(looks_like_rustc("/usr/bin/rustc"));
        assert!(looks_like_rustc(
            "/home/user/.rustup/toolchains/stable/bin/rustc"
        ));
        assert!(looks_like_rustc("clippy-driver"));
        assert!(looks_like_rustc("/path/to/bin/clippy-driver"));
        assert!(!looks_like_rustc("gcc"));
        assert!(!looks_like_rustc("--crate-name"));
    }

    #[test]
    fn kind_is_rustc() {
        assert_eq!(RustcCompiler::new().kind(), CompilerKind::Rustc);
    }

    #[test]
    fn refuse_reasons_returns_not_primary_for_query_invocations() {
        // `rustc -vV` is a version query, not a primary compilation
        let parsed = RustcCompiler::new().parse(&s(&["rustc", "-vV"])).unwrap();
        let reasons = RustcCompiler::new().refuse_reasons(&parsed);
        assert!(matches!(reasons.as_slice(), [RefuseReason::NotPrimary]));
    }

    #[test]
    fn refuse_reasons_empty_for_primary_compilation() {
        let parsed = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "src/lib.rs",
                "--crate-name",
                "foo",
                "--crate-type",
                "lib",
            ]))
            .unwrap();
        assert!(parsed.is_primary);
        let reasons = RustcCompiler::new().refuse_reasons(&parsed);
        assert!(reasons.is_empty());
    }
}
