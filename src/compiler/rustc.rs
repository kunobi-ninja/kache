//! Rustc implementation of the [`Compiler`] trait.
//!
//! Phase 0: a thin facade over the existing free functions in
//! [`crate::args`], [`crate::cache_key`], and [`crate::compile`]. Those
//! functions remain the canonical implementations; the trait simply gives
//! callers a stable shape that future compilers (gcc, clang) will match.

use anyhow::Result;

use crate::args::RustcArgs;
use crate::cache_key::compute_cache_key;
use crate::compile;

use super::{CompileResult, Compiler, CompilerKind, KeyCtx, RefuseReason};

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
