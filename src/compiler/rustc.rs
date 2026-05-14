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

use super::{ArtifactKind, CompileResult, Compiler, CompilerKind, KeyCtx, RefuseReason};

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

    fn classify_output(&self, parsed: &RustcArgs, name: &str) -> ArtifactKind {
        if name.ends_with(".rlib") {
            ArtifactKind::Library
        } else if name.ends_with(".rmeta") {
            ArtifactKind::Metadata
        } else if name.ends_with(".d") {
            ArtifactKind::DepInfo
        } else if name.ends_with(".o") {
            // Covers `.o` and compound `.rcgu.o` (per-codegen-unit objects).
            ArtifactKind::Object
        } else if name.ends_with(".dylib") || name.ends_with(".so") || name.ends_with(".dll") {
            ArtifactKind::DynamicLibrary
        } else if name.ends_with(".dwo") || name.ends_with(".pdb") || name.ends_with(".dSYM") {
            ArtifactKind::DebugSidecar
        } else if name.ends_with(".exe") || parsed.is_executable_output() {
            // No recognized extension, but the invocation produces an
            // executable (bin / test / proc-macro / dylib): treat as the
            // primary executable. Note proc-macro/dylib normally match the
            // dynamic-library arm above on Unix; reaching here means a
            // Unix-style binary with no extension.
            ArtifactKind::Executable
        } else {
            // Unrecognized — wrapper falls back to safe defaults (Hardlink,
            // no post-processing).
            ArtifactKind::Other("rustc:unknown")
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
        assert_eq!(c.classify_output(&args, "foo.exe"), ArtifactKind::Executable);
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
}
