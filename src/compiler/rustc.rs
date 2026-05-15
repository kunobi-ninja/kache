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

use super::{
    ArtifactKind, CompileResult, Compiler, CompilerKind, KeyCtx, RefuseReason, classify_by_filename,
};

/// Map a rustc `--crate-type` to the [`ArtifactKind`] of the artifact it
/// produces.
///
/// Single source of truth that both [`Compiler::classify_output`] (for
/// extensionless outputs) and [`crate::args::RustcArgs::is_executable_output`]
/// consult. Adding a new crate-type to rustc means adding one arm here;
/// every predicate in the codebase that asks "does this build produce
/// something the OS loads?" then picks up the right answer automatically
/// (via `link_strategy() == Copy`).
///
/// Returns [`ArtifactKind::Other`] for unknown crate-types — callers fall
/// back to safe defaults (immutable handling, no codesign).
pub fn classify_crate_type(crate_type: &str) -> ArtifactKind {
    match crate_type {
        "bin" => ArtifactKind::Executable,
        "dylib" | "cdylib" | "proc-macro" => ArtifactKind::DynamicLibrary,
        "lib" | "rlib" | "staticlib" => ArtifactKind::Library,
        _ => ArtifactKind::Other("unknown-crate-type"),
    }
}

#[derive(Default)]
pub struct RustcCompiler;

impl RustcCompiler {
    pub fn new() -> Self {
        Self
    }

    /// Does this argv invoke rustc (or clippy-driver, which wraps it)?
    ///
    /// Owns its own detection rule — `super::detect_compiler` delegates
    /// here so a future Msvc / etc. compiler doesn't require editing a
    /// central registry: it just adds its own `recognizes` and one new
    /// arm in `detect_compiler`. The arm is forced by the compile
    /// error on adding a `CompilerKind` variant, not by remembering to
    /// update an opaque list.
    ///
    /// Inspects only `argv[0]`. Path-prefixed forms (`/usr/bin/rustc`)
    /// work via [`Path::file_name`].
    pub fn recognizes(args: &[String]) -> bool {
        let Some(arg0) = args.first() else {
            return false;
        };
        let path = Path::new(arg0);
        match path.file_name() {
            Some(name) => {
                let name = name.to_string_lossy();
                name == "rustc" || name.starts_with("rustc") || name == "clippy-driver"
            }
            None => false,
        }
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

    fn classify_output(&self, parsed: &RustcArgs, name: &str) -> ArtifactKind {
        // Delegate to the filename-based classifier for known extensions.
        // Only the extensionless / unrecognized cases need invocation
        // context (to distinguish a bin's primary output from random
        // unrelated files).
        match classify_by_filename(name) {
            ArtifactKind::Other("extensionless") => {
                // No extension: the rustc convention for bin output on
                // Unix. Confirm via crate_types (or `--test`).
                let any_executable_crate_type = parsed
                    .crate_types
                    .iter()
                    .any(|t| matches!(classify_crate_type(t), ArtifactKind::Executable));
                if parsed.is_test || any_executable_crate_type {
                    ArtifactKind::Executable
                } else {
                    ArtifactKind::Other("rustc:unknown")
                }
            }
            ArtifactKind::Other(_) => ArtifactKind::Other("rustc:unknown"),
            kind => kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    #[test]
    fn recognizes_rustc_and_clippy_driver() {
        assert!(RustcCompiler::recognizes(&s(&["rustc"])));
        assert!(RustcCompiler::recognizes(&s(&["/usr/bin/rustc"])));
        assert!(RustcCompiler::recognizes(&s(&[
            "/home/user/.rustup/toolchains/stable/bin/rustc"
        ])));
        assert!(RustcCompiler::recognizes(&s(&["clippy-driver"])));
        assert!(RustcCompiler::recognizes(&s(&[
            "/path/to/bin/clippy-driver"
        ])));
        assert!(!RustcCompiler::recognizes(&s(&["gcc"])));
        assert!(!RustcCompiler::recognizes(&s(&["--crate-name"])));
        // Empty argv: there is nothing to recognize.
        assert!(!RustcCompiler::recognizes(&[]));
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

    fn lib_args() -> RustcArgs {
        RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "src/lib.rs",
                "--crate-name",
                "foo",
                "--crate-type",
                "lib",
            ]))
            .unwrap()
    }

    fn bin_args() -> RustcArgs {
        RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "src/main.rs",
                "--crate-name",
                "foo",
                "--crate-type",
                "bin",
            ]))
            .unwrap()
    }

    #[test]
    fn classify_output_recognizes_rust_library_artifacts() {
        let c = RustcCompiler::new();
        let args = lib_args();
        assert_eq!(
            c.classify_output(&args, "libfoo-abc123.rlib"),
            ArtifactKind::Library
        );
        assert_eq!(
            c.classify_output(&args, "libfoo-abc123.rmeta"),
            ArtifactKind::Metadata
        );
        assert_eq!(
            c.classify_output(&args, "foo-abc123.d"),
            ArtifactKind::DepInfo
        );
    }

    #[test]
    fn classify_output_recognizes_object_files_including_rcgu() {
        // Regression guard: `.rcgu.o` files must classify as Object so the
        // restore loop never sends them to codesign (kache-fork bug 572f321).
        let c = RustcCompiler::new();
        let args = bin_args();
        assert_eq!(
            c.classify_output(&args, "foo-abc.123.rcgu.o"),
            ArtifactKind::Object
        );
        assert_eq!(c.classify_output(&args, "foo.o"), ArtifactKind::Object);
    }

    #[test]
    fn classify_output_recognizes_dynamic_libraries_per_platform() {
        let c = RustcCompiler::new();
        let args = lib_args();
        assert_eq!(
            c.classify_output(&args, "libfoo.dylib"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            c.classify_output(&args, "libfoo.so"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            c.classify_output(&args, "foo.dll"),
            ArtifactKind::DynamicLibrary
        );
    }

    #[test]
    fn classify_output_recognizes_debug_sidecars() {
        let c = RustcCompiler::new();
        let args = bin_args();
        assert_eq!(
            c.classify_output(&args, "foo-abc.dwo"),
            ArtifactKind::DebugSidecar
        );
        assert_eq!(
            c.classify_output(&args, "foo.pdb"),
            ArtifactKind::DebugSidecar
        );
    }

    #[test]
    fn classify_output_treats_extensionless_bin_outputs_as_executable() {
        // A bin crate emits a no-extension file (`my_bin-abc123`); the
        // classifier needs invocation context to recognize it.
        let c = RustcCompiler::new();
        let args = bin_args();
        assert_eq!(
            c.classify_output(&args, "foo-abc123"),
            ArtifactKind::Executable
        );
        assert_eq!(
            c.classify_output(&args, "foo.exe"),
            ArtifactKind::Executable
        );
    }

    #[test]
    fn classify_output_falls_back_to_other_for_unrecognized_in_lib_build() {
        // Same extensionless name in a lib build: no executable context, so
        // we don't blindly call it Executable. Other("...") makes the
        // wrapper take the safe default (Hardlink, no post-processing).
        let c = RustcCompiler::new();
        let args = lib_args();
        match c.classify_output(&args, "mystery-file") {
            ArtifactKind::Other(_) => {}
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[test]
    fn classify_crate_type_maps_known_rustc_types() {
        // Single source of truth for crate-type → artifact kind. Any
        // predicate in the codebase that asks "does this build produce
        // something the OS loads at runtime?" derives its answer from this
        // mapping (via `link_strategy() == Copy`). Locking the contract.
        assert_eq!(classify_crate_type("bin"), ArtifactKind::Executable);
        assert_eq!(classify_crate_type("dylib"), ArtifactKind::DynamicLibrary);
        assert_eq!(classify_crate_type("cdylib"), ArtifactKind::DynamicLibrary);
        assert_eq!(
            classify_crate_type("proc-macro"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(classify_crate_type("lib"), ArtifactKind::Library);
        assert_eq!(classify_crate_type("rlib"), ArtifactKind::Library);
        // staticlib produces .a — a static library, NOT loaded by the OS.
        assert_eq!(classify_crate_type("staticlib"), ArtifactKind::Library);
        // Unknown crate-types fall back to Other, which has Hardlink
        // strategy and is_executable_output() returns false. Conservative
        // default for new rustc crate-types we haven't accounted for yet.
        match classify_crate_type("future-rustc-type-2030") {
            ArtifactKind::Other(_) => {}
            other => panic!("expected Other, got {other:?}"),
        }
    }

    #[test]
    fn classify_crate_type_link_strategy_matches_is_executable_output() {
        // Regression guard for the centralization: every crate-type in the
        // is_executable_output set (bin/dylib/cdylib/proc-macro/+test) maps
        // to a kind whose link_strategy is Copy; everything else maps to
        // Hardlink. Adding a new crate-type to classify_crate_type
        // automatically threads through is_executable_output and every
        // caller of it.
        use crate::link::LinkStrategy;
        let executable_types = ["bin", "dylib", "cdylib", "proc-macro"];
        for t in executable_types {
            assert_eq!(
                classify_crate_type(t).link_strategy(),
                LinkStrategy::Copy,
                "{t} should be Copy strategy"
            );
        }
        let library_types = ["lib", "rlib", "staticlib"];
        for t in library_types {
            assert_eq!(
                classify_crate_type(t).link_strategy(),
                LinkStrategy::Hardlink,
                "{t} should be Hardlink strategy"
            );
        }
    }
}
