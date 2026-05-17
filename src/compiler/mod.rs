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
use std::path::{Path, PathBuf};

use crate::link::LinkStrategy;

pub mod cc;
pub mod platform;
pub mod rustc;

pub use platform::Platform;

pub use crate::compile::CompileResult;

/// Identifies a compiler family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilerKind {
    Rustc,
    /// C-family compiler (cc, gcc, g++, clang, clang++, c++ and version
    /// suffixes). Single variant for now — sub-distinctions (real gcc vs
    /// clang vs apple-clang vs MSVC) become relevant when arg parsing
    /// lands and matter per-impl, not at the dispatch layer.
    Cc,
    // Future: Msvc (different argv shape, separate variant).
    //
    // Compiler-family *probes* (e.g. `kache -E <file>`) are NOT a
    // compiler kind — they're a non-compiler invocation pattern that
    // happens to need passthrough. Detected separately via
    // `CcCompiler::recognizes_family_probe` and dispatched in
    // `run_wrapper_mode` before the compiler match.
}

/// Reason an invocation cannot be cached. Empty list = cacheable.
#[derive(Debug, Clone)]
pub enum RefuseReason {
    /// Not a primary compilation (e.g. `--print`, `-vV`, query mode).
    NotPrimary,
    /// Compiler-specific feature not yet supported by kache. Static string
    /// is a stable identifier suitable for logging / metrics. Used today
    /// by the C-family skeleton (refuses everything until real caching
    /// lands) and reserved for future per-compiler "we know this flag
    /// exists but can't safely cache it" cases.
    Unsupported(&'static str),
}

impl RefuseReason {
    /// Stable, human-readable description of why caching was refused.
    /// Used by the wrapper for diagnostic logging and (future) metrics.
    /// The string is a contract — changing it is observable.
    pub fn description(&self) -> &'static str {
        match self {
            RefuseReason::NotPrimary => "not a primary compilation",
            RefuseReason::Unsupported(detail) => detail,
        }
    }
}

/// Compiler-agnostic context passed to [`Compiler::cache_key`].
pub struct KeyCtx<'a> {
    pub file_hasher: &'a crate::cache_key::FileHasher,
    /// Strips machine-local path prefixes from key inputs so the same
    /// source produces the same key across hosts and worktrees. Lives
    /// in the context (not as a free function) so future per-compiler
    /// impls can pass a normalizer with extra rules (e.g. cc-family
    /// might know about `$SDKROOT`).
    pub path_normalizer: &'a crate::path_normalizer::PathNormalizer,
    /// kache's cache directory. Compiler-probe results (e.g. the cc
    /// `--version` identity line) are memoized under here so a probe
    /// runs once per build instead of once per translation unit — see
    /// [`crate::probe`].
    pub cache_dir: &'a Path,
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

/// Best-guess classification from filename alone, no compile-context.
///
/// Used by callers that scan a directory of artifacts (e.g. analyzing
/// `target/` from the CLI) where there's no parsed [`Compiler::Parsed`]
/// to disambiguate. Extensionless files return
/// [`ArtifactKind::Other`]`("extensionless")` — callers in target-scan
/// contexts should treat that as `Executable` (the rustc convention for
/// bin output on Unix); callers without that context should fall back
/// to the safe default (immutable, no post-processing).
///
/// This is the single source of truth for "filename → artifact kind"
/// across kache: [`Compiler::classify_output`] implementations delegate
/// to it for the known-extension cases. Adding a new artifact extension
/// happens here, not at every call site that does suffix matching.
pub fn classify_by_filename(name: &str) -> ArtifactKind {
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    match ext {
        "rlib" => ArtifactKind::Library,
        "rmeta" => ArtifactKind::Metadata,
        "d" => ArtifactKind::DepInfo,
        // Covers `.o` and compound `.rcgu.o` (Path::extension takes the
        // shortest tail, which is "o" for both).
        "o" | "obj" => ArtifactKind::Object,
        "dylib" | "so" | "dll" => ArtifactKind::DynamicLibrary,
        "dwo" | "pdb" | "dSYM" => ArtifactKind::DebugSidecar,
        "exe" => ArtifactKind::Executable,
        "" => ArtifactKind::Other("extensionless"),
        _ => ArtifactKind::Other("unknown-ext"),
    }
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
    ///
    /// `platform` is the host abstraction for OS-specific concerns
    /// (codesigning today; debug-path rewriting and similar concerns
    /// later). Passing it explicitly — rather than calling
    /// `platform::current()` here — keeps tests deterministic: a unit
    /// test can inject a counting / failing / no-op platform without
    /// needing the real host to behave a particular way.
    pub fn apply(&self, path: &std::path::Path, platform: &dyn Platform) -> Result<()> {
        match self {
            PostRestoreAction::ExpandDepInfoPaths => {
                if let Ok(pwd) = std::env::current_dir() {
                    let _ =
                        crate::link::rewrite_depinfo(path, &pwd, crate::link::DepInfoMode::Expand);
                }
                Ok(())
            }
            PostRestoreAction::Sign(SigningPurpose::OsLoading) => {
                // Verify-then-sign lives inside the platform impl so
                // the kache-fork bug 59866c0 (mutating already-valid
                // signatures) can't be reintroduced from this site.
                platform.ensure_binary_loadable(path)
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
///
/// Each compiler impl owns its own `recognizes` rule; this function is
/// just the dispatch table. Adding a new compiler family means adding
/// a new `CompilerKind` variant + its impl + one arm here — no central
/// "registry of recognizers" to keep in sync.
///
/// Returns `None` if no supported compiler matches — caller should
/// fall through to direct execution (or to compiler-family probe
/// handling via [`cc::CcCompiler::recognizes_family_probe`], which is
/// its own concern, not a compiler kind).
pub fn detect_compiler(args: &[String]) -> Option<CompilerKind> {
    if rustc::RustcCompiler::recognizes(args) {
        return Some(CompilerKind::Rustc);
    }
    if cc::CcCompiler::recognizes(args) {
        return Some(CompilerKind::Cc);
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
    fn detect_compiler_recognizes_cc_paths() {
        assert_eq!(detect_compiler(&s(&["cc"])), Some(CompilerKind::Cc));
        assert_eq!(detect_compiler(&s(&["gcc"])), Some(CompilerKind::Cc));
        assert_eq!(detect_compiler(&s(&["clang++"])), Some(CompilerKind::Cc));
        assert_eq!(
            detect_compiler(&s(&["/usr/bin/cc", "-c", "foo.c"])),
            Some(CompilerKind::Cc)
        );
    }

    #[test]
    fn detect_compiler_returns_none_for_cc_probe_shape() {
        // The cc-crate compiler-family probe (`kache -E <file>`) is
        // intentionally NOT a CompilerKind — it's a non-compiler
        // invocation pattern handled separately in run_wrapper_mode
        // via `CcCompiler::recognizes_family_probe`. Asserting None
        // here pins that boundary: detect_compiler must not grow into
        // a grab-bag of "anything kache should passthrough".
        assert_eq!(detect_compiler(&s(&["-E", "/tmp/probe.c"])), None);
        assert_eq!(
            detect_compiler(&s(&["-E", "/tmp/detect_compiler_family.c"])),
            None
        );
    }

    #[test]
    fn detect_compiler_returns_none_for_unrelated_argv() {
        assert_eq!(detect_compiler(&s(&["cargo", "build"])), None);
        assert_eq!(detect_compiler(&s(&["make"])), None);
        assert_eq!(detect_compiler(&s(&["ld"])), None);
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

    // ── apply() ──────────────────────────────────────────────────
    //
    // Coverage for the action executor: ExpandDepInfoPaths uses
    // link::rewrite_depinfo internally; Sign(OsLoading) routes through
    // the injected Platform. Both must be safe on arbitrary inputs and
    // must not panic.

    /// Convenience for tests that don't care which platform impl runs;
    /// the actual codesign behavior is exercised in
    /// `compiler::platform::tests` and on macOS arm64 in CI/locally.
    fn test_platform() -> Box<dyn Platform> {
        Box::new(crate::compiler::platform::LinuxPlatform)
    }

    #[test]
    fn apply_expand_dep_info_paths_rewrites_relative_paths_to_absolute() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let depfile = dir.path().join("test.d");
        {
            let mut f = std::fs::File::create(&depfile).unwrap();
            // The relative-paths shape that link::rewrite_depinfo's
            // Relativize mode produces; Expand should reverse it.
            write!(f, "./target/debug/foo: ./src/lib.rs").unwrap();
        }

        PostRestoreAction::ExpandDepInfoPaths
            .apply(&depfile, &*test_platform())
            .unwrap();

        let content = std::fs::read_to_string(&depfile).unwrap();
        let pwd = std::env::current_dir().unwrap();
        let pwd_str = pwd.display().to_string();
        // After Expand: the leading "./" is replaced with "<pwd>/" everywhere.
        assert!(
            content.contains(&format!("{pwd_str}/target/debug/foo")),
            "expected absolute target path, got: {content}"
        );
        assert!(
            content.contains(&format!("{pwd_str}/src/lib.rs")),
            "expected absolute source path, got: {content}"
        );
        assert!(
            !content.contains("./"),
            "no relative `./` markers should remain, got: {content}"
        );
    }

    #[test]
    fn apply_expand_dep_info_paths_is_silent_on_missing_file() {
        // The current implementation deliberately swallows rewrite_depinfo
        // errors with `let _` — a missing file shouldn't take down the
        // wrapper's restore loop. Lock that contract in.
        let dir = tempfile::tempdir().unwrap();
        let nonexistent = dir.path().join("does-not-exist.d");
        PostRestoreAction::ExpandDepInfoPaths
            .apply(&nonexistent, &*test_platform())
            .expect("apply must not error on a missing dep-info file");
    }

    #[test]
    fn apply_sign_os_loading_routes_through_platform() {
        // The dispatch contract: Sign(OsLoading) must hand off to the
        // platform's ensure_binary_loadable, not re-implement codesign
        // logic in-line. CountingPlatform proves the call happened
        // exactly once per apply().
        use crate::compiler::platform::tests::CountingPlatform;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not-actually-a-binary");
        std::fs::write(&path, b"definitely not Mach-O").unwrap();

        let platform = CountingPlatform::new();
        PostRestoreAction::Sign(SigningPurpose::OsLoading)
            .apply(&path, &platform)
            .expect("apply must not error even when the platform impl is a no-op");
        assert_eq!(
            platform.ensure_calls(),
            1,
            "Sign(OsLoading) must dispatch to platform.ensure_binary_loadable exactly once"
        );
    }

    #[test]
    fn apply_expand_dep_info_paths_does_not_call_platform() {
        // Confirms ExpandDepInfoPaths is purely filesystem work and
        // doesn't accidentally route through the platform layer (which
        // would be a code smell — depinfo rewriting is OS-agnostic).
        use crate::compiler::platform::tests::CountingPlatform;
        let dir = tempfile::tempdir().unwrap();
        let depfile = dir.path().join("nope.d");

        let platform = CountingPlatform::new();
        let _ = PostRestoreAction::ExpandDepInfoPaths.apply(&depfile, &platform);
        assert_eq!(platform.ensure_calls(), 0);
    }

    // ── classify → plan integration ──────────────────────────────
    //
    // The wrapper does `compiler.classify_output(...) → plan_post_restore(...)`
    // per cached file. These tests exercise that chain end-to-end so a
    // mistake in either side (e.g. `.rcgu.o` getting classified as
    // Executable, or a kind silently picking up the wrong actions) is
    // caught here without needing wrapper-level integration plumbing.

    #[test]
    fn rustc_classify_to_plan_chain_for_typical_lib_build() {
        use crate::compiler::rustc::RustcCompiler;
        let compiler = RustcCompiler::new();
        let lib_args = compiler
            .parse(&[
                "rustc".into(),
                "src/lib.rs".into(),
                "--crate-name".into(),
                "foo".into(),
                "--crate-type".into(),
                "lib".into(),
            ])
            .unwrap();

        let cases: &[(&str, Vec<PostRestoreAction>)] = &[
            ("libfoo-abc.rlib", vec![]),
            ("libfoo-abc.rmeta", vec![]),
            ("foo-abc.d", vec![PostRestoreAction::ExpandDepInfoPaths]),
            ("foo-abc.rcgu.o", vec![]),
            ("foo-abc.dwo", vec![]),
        ];

        for (name, expected) in cases {
            let kind = compiler.classify_output(&lib_args, name);
            assert_eq!(
                &plan_post_restore(kind),
                expected,
                "for {name}: kind = {kind:?}"
            );
        }
    }

    #[test]
    fn classify_by_filename_recognizes_known_extensions() {
        // Single source of truth — every caller in the codebase that does
        // suffix matching should delegate here. Locking the mapping in.
        assert_eq!(
            classify_by_filename("libfoo-abc.rlib"),
            ArtifactKind::Library
        );
        assert_eq!(
            classify_by_filename("libfoo-abc.rmeta"),
            ArtifactKind::Metadata
        );
        assert_eq!(classify_by_filename("foo-abc.d"), ArtifactKind::DepInfo);
        assert_eq!(classify_by_filename("foo.o"), ArtifactKind::Object);
        assert_eq!(
            classify_by_filename("foo-abc.123.rcgu.o"),
            ArtifactKind::Object
        );
        assert_eq!(classify_by_filename("foo.obj"), ArtifactKind::Object);
        assert_eq!(
            classify_by_filename("libfoo.dylib"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            classify_by_filename("libfoo.so"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            classify_by_filename("foo.dll"),
            ArtifactKind::DynamicLibrary
        );
        assert_eq!(
            classify_by_filename("foo-abc.dwo"),
            ArtifactKind::DebugSidecar
        );
        assert_eq!(classify_by_filename("foo.pdb"), ArtifactKind::DebugSidecar);
        assert_eq!(classify_by_filename("foo.exe"), ArtifactKind::Executable);
    }

    #[test]
    fn classify_by_filename_distinguishes_extensionless_from_unknown() {
        // Two distinct "Other" tags so callers can choose what convention
        // to apply: target/-scan callers treat extensionless as bin output;
        // others fall back to safe defaults.
        match classify_by_filename("my_bin-abc123") {
            ArtifactKind::Other("extensionless") => {}
            other => panic!("expected Other(extensionless), got {other:?}"),
        }
        match classify_by_filename("foo.lock") {
            ArtifactKind::Other("unknown-ext") => {}
            other => panic!("expected Other(unknown-ext), got {other:?}"),
        }
    }

    #[test]
    fn rustc_classify_to_plan_chain_for_typical_bin_build() {
        use crate::compiler::rustc::RustcCompiler;
        let compiler = RustcCompiler::new();
        let bin_args = compiler
            .parse(&[
                "rustc".into(),
                "src/main.rs".into(),
                "--crate-name".into(),
                "foo".into(),
                "--crate-type".into(),
                "bin".into(),
            ])
            .unwrap();

        let cases: &[(&str, Vec<PostRestoreAction>)] = &[
            // Extensionless binary on Unix → Executable → must sign.
            (
                "foo-abc",
                vec![PostRestoreAction::Sign(SigningPurpose::OsLoading)],
            ),
            // Dep-info still rewrites paths even in a bin build.
            ("foo-abc.d", vec![PostRestoreAction::ExpandDepInfoPaths]),
            // Per-codegen-unit object files must NEVER pick up codesign
            // (kache-fork bug 572f321). This case is the regression guard
            // for the whole bug class.
            ("foo-abc.rcgu.o", vec![]),
            // Debug sidecars are passive too.
            ("foo-abc.dwo", vec![]),
        ];

        for (name, expected) in cases {
            let kind = compiler.classify_output(&bin_args, name);
            assert_eq!(
                &plan_post_restore(kind),
                expected,
                "for {name}: kind = {kind:?}"
            );
        }
    }
}
