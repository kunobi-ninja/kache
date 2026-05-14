//! Compiler abstraction.
//!
//! Each supported compiler (today: rustc; planned: gcc, clang, msvc) implements
//! the [`Compiler`] trait. The wrapper picks an implementation based on argv[0]
//! inspection ([`detect_compiler`]) and dispatches by static type — there is no
//! `dyn Compiler`, intentionally, because each compiler keeps its native parsed
//! representation as an associated type.
//!
//! **Scope.** The trait covers the operations with a clean generic shape
//! today: `parse`, `refuse_reasons`, `cache_key`, `execute`, and
//! `classify_output` (per-file kind classification used by the wrapper to
//! dispatch link strategy and post-restore processing without filename
//! pattern matching). Storage metadata (crate types, features,
//! target/profile) and the restore loop's path resolution still touch
//! [`crate::args::RustcArgs`] fields directly in [`crate::wrapper`]; those
//! move behind the trait when adding a second compiler forces the
//! abstraction.

use anyhow::Result;
use std::path::PathBuf;

use crate::link::LinkStrategy;

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

/// Categorization of a compiler output file.
///
/// Used by the wrapper to drive two decisions per restored file without
/// scattering filename pattern matching: which [`LinkStrategy`] to use, and
/// which post-restore processing to apply (dep-info path expansion, codesign,
/// etc.). Centralizing the dispatch on `ArtifactKind` is what makes "skip
/// codesign for `.o`" or "rewrite paths in `.d`" structurally enforced
/// instead of dependent on remembering to add a string-suffix check at every
/// call site.
///
/// Open enum: future compilers extend with [`ArtifactKind::Other`] without
/// touching shared code; the safe default for an unrecognized kind is
/// `Hardlink` + no post-processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    /// Linkable static library (`.rlib`, future C/C++ `.a` / `.lib`).
    Library,
    /// Dynamic library (`.dylib`, `.so`, `.dll`). Mutable post-build on
    /// macOS (codesigning).
    DynamicLibrary,
    /// Metadata-only artifact (Rust `.rmeta`).
    Metadata,
    /// Object file (`.o`, `.obj`, `.rcgu.o`). Linker input only — never loaded
    /// directly, never codesigned.
    Object,
    /// Dependency-info file (`.d`). Content references absolute paths that
    /// need rewriting on store/restore for cross-worktree portability.
    DepInfo,
    /// Executable. Mutable post-build (codesigning, stripping).
    Executable,
    /// Debug info sidecar (`.dwo`, `.pdb`, `.dSYM`).
    DebugSidecar,
    /// Compiler-specific output that doesn't fit the categories above.
    /// Defaults to immutable handling.
    Other(&'static str),
}

impl ArtifactKind {
    /// Link strategy for restoring this kind. Mutable artifacts (executables,
    /// dynamic libraries) must end up as independent files on filesystems
    /// without CoW reflink, so post-build mutations don't propagate into the
    /// cache blob. Immutable kinds may share an inode (hardlink fallback).
    pub fn link_strategy(self) -> LinkStrategy {
        match self {
            ArtifactKind::Executable | ArtifactKind::DynamicLibrary => LinkStrategy::Copy,
            _ => LinkStrategy::Hardlink,
        }
    }
}

/// A compiler output file with its [`ArtifactKind`] for dispatch purposes.
#[allow(dead_code)] // produced by future Compiler::outputs(), not yet wired
#[derive(Debug, Clone)]
pub struct OutputArtifact {
    pub path: PathBuf,
    pub kind: ArtifactKind,
}

/// Why a signature is being applied. Today the only purpose is
/// [`SigningPurpose::OsLoading`], but `Sign(SigningPurpose)` is structured
/// this way so future cases (distribution signing, supply-chain attestation)
/// add a new variant rather than a new action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigningPurpose {
    /// Re-establish a signature so the OS will load this artifact.
    /// macOS arm64 → ad-hoc codesign. Linux / Windows → no-op today.
    OsLoading,
}

/// One thing that needs to happen to a restored artifact before it's
/// ready for use. The wrapper composes a per-file plan via
/// [`plan_post_restore`] and applies each action in order.
///
/// Adding a new action variant means: one arm in
/// [`PostRestoreAction::apply`], one condition in [`plan_post_restore`],
/// and (if helpful) tests covering the relevant `ArtifactKind` mappings.
/// The wrapper does not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PostRestoreAction {
    /// Rewrite absolute paths inside a `.d` (dep-info) file so cargo's
    /// freshness stat()s find them in the current worktree's `target/`.
    ExpandDepInfoPaths,

    /// Apply a signature for the given purpose. Cross-platform — no-op on
    /// platforms that don't require it.
    Sign(SigningPurpose),
}

/// Compose the post-restore action sequence for an artifact, given its
/// kind. Pure function — testable per kind without filesystem.
///
/// Today the plan only depends on `kind`. When `Platform` lands as a
/// first-class abstraction, this signature gains `&platform` and signing
/// becomes conditional on the platform actually requiring it.
pub fn plan_post_restore(kind: ArtifactKind) -> Vec<PostRestoreAction> {
    let mut plan = Vec::new();
    if matches!(kind, ArtifactKind::DepInfo) {
        plan.push(PostRestoreAction::ExpandDepInfoPaths);
    }
    if matches!(
        kind,
        ArtifactKind::Executable | ArtifactKind::DynamicLibrary
    ) {
        plan.push(PostRestoreAction::Sign(SigningPurpose::OsLoading));
    }
    plan
}

impl PostRestoreAction {
    /// Execute this action against a restored artifact at `path`.
    pub fn apply(&self, path: &std::path::Path) -> Result<()> {
        match self {
            PostRestoreAction::ExpandDepInfoPaths => {
                if let Ok(pwd) = std::env::current_dir() {
                    let _ =
                        crate::link::rewrite_depinfo(path, &pwd, crate::link::DepInfoMode::Expand);
                }
                Ok(())
            }
            PostRestoreAction::Sign(SigningPurpose::OsLoading) => {
                crate::compile::codesign_adhoc(path)
            }
        }
    }
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

    /// Classify an output file by its filename, given the parsed invocation
    /// for context (e.g. crate type to disambiguate executables from
    /// libraries when both share a no-extension shape).
    ///
    /// `name` is the filename only — no path components. Returns
    /// [`ArtifactKind::Other`] when the file doesn't match any known pattern;
    /// callers default to immutable / no-post-processing behavior in that
    /// case.
    fn classify_output(&self, parsed: &Self::Parsed, name: &str) -> ArtifactKind;
}

/// Detect which compiler family an argv vector is invoking.
/// Returns `None` if no supported compiler matches — caller should fall
/// through to direct execution.
pub fn detect_compiler(args: &[String]) -> Option<CompilerKind> {
    if args.is_empty() {
        return None;
    }
    if rustc::looks_like_rustc(&args[0]) {
        return Some(CompilerKind::Rustc);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn detect_compiler_returns_none_for_empty_argv() {
        assert_eq!(detect_compiler(&[]), None);
    }

    #[test]
    fn detect_compiler_recognizes_rustc_paths() {
        assert_eq!(detect_compiler(&s(&["rustc"])), Some(CompilerKind::Rustc));
        assert_eq!(
            detect_compiler(&s(&["/usr/bin/rustc", "src/lib.rs"])),
            Some(CompilerKind::Rustc)
        );
        assert_eq!(
            detect_compiler(&s(&["clippy-driver"])),
            Some(CompilerKind::Rustc)
        );
    }

    #[test]
    fn detect_compiler_returns_none_for_non_rustc() {
        assert_eq!(detect_compiler(&s(&["gcc"])), None);
        assert_eq!(detect_compiler(&s(&["cargo", "build"])), None);
        assert_eq!(detect_compiler(&s(&["--crate-name"])), None);
    }

    #[test]
    fn plan_post_restore_dep_info_expands_paths() {
        assert_eq!(
            plan_post_restore(ArtifactKind::DepInfo),
            vec![PostRestoreAction::ExpandDepInfoPaths]
        );
    }

    #[test]
    fn plan_post_restore_executable_signs_for_os_loading() {
        assert_eq!(
            plan_post_restore(ArtifactKind::Executable),
            vec![PostRestoreAction::Sign(SigningPurpose::OsLoading)]
        );
    }

    #[test]
    fn plan_post_restore_dynamic_library_signs_for_os_loading() {
        // Same plan as Executable: dylibs are loaded by the dynamic linker
        // and need an OS-acceptable signature on macOS arm64. Encoded as a
        // single condition in `plan_post_restore` so adding a third
        // OS-loaded kind requires changing one place.
        assert_eq!(
            plan_post_restore(ArtifactKind::DynamicLibrary),
            vec![PostRestoreAction::Sign(SigningPurpose::OsLoading)]
        );
    }

    #[test]
    fn plan_post_restore_object_is_empty() {
        // Regression guard: `.o` / `.rcgu.o` files must not pick up any
        // post-restore action — in particular not codesign (kache-fork
        // bug 572f321).
        assert!(plan_post_restore(ArtifactKind::Object).is_empty());
    }

    #[test]
    fn plan_post_restore_passive_kinds_are_empty() {
        for kind in [
            ArtifactKind::Library,
            ArtifactKind::Metadata,
            ArtifactKind::DebugSidecar,
            ArtifactKind::Other("test"),
        ] {
            assert!(
                plan_post_restore(kind).is_empty(),
                "{kind:?} should have no post-restore actions"
            );
        }
    }
}
