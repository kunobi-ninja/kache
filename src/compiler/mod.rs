//! Compiler abstraction.
//!
//! Each supported compiler (today: rustc; planned: gcc, clang, msvc) implements
//! the [`Compiler`] trait. The wrapper picks an implementation based on argv[0]
//! inspection ([`detect_compiler`]) and dispatches by static type — there is no
//! `dyn Compiler`, intentionally, because each compiler keeps its native parsed
//! representation as an associated type.
//!
//! **Phase 0 scope.** The trait covers the operations with a clean generic
//! shape today: `parse`, `refuse_reasons`, `cache_key`, `execute`. Storage
//! metadata (crate types, features, target/profile) and restoration logic
//! still touch [`crate::args::RustcArgs`] fields directly in
//! [`crate::wrapper`]; those move behind the trait when adding a second
//! compiler forces the abstraction. Forward-looking surface (output-artifact
//! categorization, version-probe identity) lands with the PR that needs it.

use anyhow::Result;

pub mod rustc;

pub use crate::compile::CompileResult;

/// Identifies a compiler family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilerKind {
    Rustc,
    // Future: Gcc, Clang, Msvc
}

/// Reason an invocation cannot be cached. Empty list = cacheable.
#[derive(Debug, Clone)]
pub enum RefuseReason {
    /// Not a primary compilation (e.g. `--print`, `-vV`, query mode).
    NotPrimary,
}

/// Compiler-agnostic context passed to [`Compiler::cache_key`].
pub struct KeyCtx<'a> {
    pub file_hasher: &'a crate::cache_key::FileHasher,
}

/// A cacheable compiler.
///
/// Implementations are state-light. Each owns its native parsed
/// representation as `Self::Parsed` so we don't flatten compiler-specific
/// shapes into one generic struct.
pub trait Compiler {
    type Parsed;

    fn kind(&self) -> CompilerKind;

    /// Parse raw argv into the compiler's native representation.
    /// Caller has already established this is the right compiler kind via
    /// [`detect_compiler`].
    fn parse(&self, args: &[String]) -> Result<Self::Parsed>;

    /// Reasons (if any) this invocation must bypass the cache.
    /// Empty Vec = cacheable.
    fn refuse_reasons(&self, parsed: &Self::Parsed) -> Vec<RefuseReason>;

    /// Compute the cache key for a parsed invocation.
    fn cache_key(&self, parsed: &Self::Parsed, ctx: &KeyCtx<'_>) -> Result<String>;

    /// Execute the compilation, capturing exit code, stdout, stderr, and
    /// the list of output files produced.
    fn execute(&self, parsed: &Self::Parsed) -> Result<CompileResult>;
}

/// Detect which compiler family an argv vector is invoking.
/// Returns `None` if no supported compiler matches — caller should fall
/// through to direct execution.
pub fn detect_compiler(args: &[String]) -> Option<CompilerKind> {
    if args.is_empty() {
        return None;
    }
    if crate::args::looks_like_rustc(&args[0]) {
        return Some(CompilerKind::Rustc);
    }
    None
}
