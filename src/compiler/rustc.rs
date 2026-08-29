//! Rustc implementation of the [`Compiler`] trait.
//!
//! Phase 0: a thin facade over the existing free functions in
//! [`crate::args`], [`crate::cache_key`], and [`crate::compile`]. Those
//! functions remain the canonical implementations; the trait simply gives
//! callers a stable shape that other compiler adapters can match.

use anyhow::Result;
use std::borrow::Cow;
#[cfg(test)]
use std::path::Path;

use crate::args::RustcArgs;
use crate::cache_key::compute_cache_key;
use crate::compile;

use super::{
    ArtifactKind, CompileResult, Compiler, CompilerAdapter, CompilerId, KeyCtx, RefuseReason,
    classify_by_filename,
};

pub const RUSTC_ID: CompilerId = CompilerId::new("rustc");
pub const ADAPTER: CompilerAdapter =
    CompilerAdapter::new(RUSTC_ID, "Rust compiler", RustcCompiler::recognizes);

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

/// Does a crate of this type have Rust metadata to emit?
///
/// rustc writes a real `.rmeta` only for the crate types a downstream crate can
/// link against as a Rust library (`lib` / `rlib` / `dylib` / `proc-macro`). For
/// `bin`, `cdylib`, `staticlib` — and for a `--test` unit, which passes no
/// `--crate-type` at all — an `--emit=metadata` compile still *creates* the
/// `.rmeta` file (cargo expects it) but leaves it **zero bytes**. Verified
/// against rustc 1.97 for every arm below.
///
/// Only used to decide whether an empty `.rmeta` is a legitimate output or a
/// truncated write (`store::zero_byte_is_valid_output`, kunobi-ninja/kache#624),
/// so an unknown crate-type answers `true`: keep the corruption guard rather
/// than exempt something we can't vouch for.
pub fn crate_type_produces_metadata(crate_type: &str) -> bool {
    !matches!(crate_type, "bin" | "cdylib" | "staticlib")
}

#[derive(Default)]
pub struct RustcCompiler {
    base_dirs: Vec<String>,
}

impl RustcCompiler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_base_dirs(mut self, base_dirs: Vec<String>) -> Self {
        self.base_dirs = base_dirs;
        self
    }

    /// Does this argv invoke rustc (or clippy-driver, which wraps it)?
    ///
    /// Owns its own detection rule; `super::detect_compiler` reaches it
    /// through this module's [`ADAPTER`] descriptor.
    ///
    /// Inspects only `argv[0]`. Path-prefixed forms (`/usr/bin/rustc`,
    /// `C:\…\bin\rustc.exe`) and Windows `.exe` suffixes are accepted —
    /// cargo passes `clippy-driver.exe` here under `cargo clippy` on Windows
    /// (issue #287), which must be recognized as a rustc invocation. Basename
    /// extraction and `.exe` stripping are shared with the cc adapter so both
    /// stay consistent across host platforms.
    pub fn recognizes(args: &[String]) -> bool {
        let Some(arg0) = args.first() else {
            return false;
        };
        let Some(name) = super::command_basename(arg0) else {
            return false;
        };
        let name = super::strip_windows_exe_suffix(name);
        name == "rustc" || name.starts_with("rustc") || name == "clippy-driver"
    }

    /// Execute rustc with Kache-owned isolated incremental state while keeping
    /// every other normal compile behavior (path remapping, opcounts,
    /// heartbeat monitoring, diagnostics, and output discovery).
    pub(crate) fn execute_preserving_incremental(
        &self,
        parsed: &RustcArgs,
        isolated_args: &[String],
    ) -> Result<CompileResult> {
        self.execute_with_args(
            parsed,
            isolated_args,
            compile::IncrementalMode::PreserveIsolated,
            None,
        )
    }

    /// Preserve ordinary passthrough path semantics for an executable Kache
    /// had already excluded from artifact caching, while retaining compiler
    /// accounting and heartbeat monitoring.
    pub(crate) fn execute_passthrough_preserving_incremental(
        &self,
        parsed: &RustcArgs,
        isolated_args: &[String],
    ) -> Result<CompileResult> {
        self.execute_with_args(
            parsed,
            isolated_args,
            compile::IncrementalMode::PreserveIsolated,
            Some(true),
        )
    }

    fn execute_with_args(
        &self,
        parsed: &RustcArgs,
        all_args: &[String],
        incremental_mode: compile::IncrementalMode,
        skip_remap_override: Option<bool>,
    ) -> Result<CompileResult> {
        // The invocation and key must use the same path-normalization rules.
        let workspace_root = parsed.path_normalization_root();
        let path_normalizer = crate::path_normalizer::PathNormalizer::from_env(workspace_root)
            .with_target_dir(parsed.target_dir().as_deref())
            .with_base_dirs(&self.base_dirs)
            .with_rust_src_rule(
                crate::cache_key::get_rustc_sysroot(parsed).as_deref(),
                crate::cache_key::get_rustc_commit_hash(&parsed.rustc).as_deref(),
            );
        let skip_remap = skip_remap_override.unwrap_or_else(|| parsed.skip_path_remap());
        // Only the cache miss path injects `-oso_prefix`. A passthrough must
        // not change the binary relative to an unwrapped rustc. `None` here
        // is that cache path (`execute` / isolated incremental).
        let cache_this_compile = skip_remap_override.is_none();
        let compiler_args = match macos_oso_prefix_flag(parsed, all_args, cache_this_compile) {
            Some(flag) => {
                let mut extended = all_args.to_vec();
                extended.push(flag);
                Cow::Owned(extended)
            }
            None => Cow::Borrowed(all_args),
        };
        compile::run_rustc(
            &parsed.rustc,
            parsed.inner_rustc.as_deref(),
            compiler_args.as_ref(),
            parsed.has_expanded_argfiles(),
            parsed.output.as_deref(),
            parsed.out_dir.as_deref(),
            parsed.crate_name.as_deref(),
            parsed.extra_filename.as_deref(),
            &parsed.emit,
            skip_remap,
            &path_normalizer,
            incremental_mode,
        )
    }
}

impl Compiler for RustcCompiler {
    type Parsed = RustcArgs;

    fn id(&self) -> CompilerId {
        RUSTC_ID
    }

    fn parse(&self, args: &[String]) -> Result<RustcArgs> {
        RustcArgs::parse(args)
    }

    fn refuse_reasons(&self, parsed: &RustcArgs) -> Vec<RefuseReason> {
        let build_script_out_dir = std::env::var_os("OUT_DIR").map(std::path::PathBuf::from);
        rustc_refuse_reasons(parsed, build_script_out_dir.as_deref())
    }

    fn cache_key(&self, parsed: &RustcArgs, ctx: &KeyCtx<'_, '_>) -> Result<String> {
        let crate_name = parsed.crate_name.as_deref().unwrap_or("unknown");
        let key = compute_cache_key(parsed, ctx.file_hasher, ctx.path_normalizer)?;
        let key = match ctx.extra_inputs_digest {
            Some(digest) => crate::cache_key::fold_labeled(key, "extra_inputs", digest),
            None => key,
        };
        let key = crate::cache_key::apply_key_env_vars(key, ctx.key_env_vars, crate_name);
        Ok(crate::cache_key::apply_key_salt(
            key,
            ctx.key_salt,
            crate_name,
        ))
    }

    fn execute(&self, parsed: &RustcArgs) -> Result<CompileResult> {
        // Construct the same PathNormalizer that the cache key was
        // built with — derived from `--out-dir` so workspace_root
        // matches across the two consumers (cache_key.rs and the
        // `--remap-path-prefix` injection here). If they diverged,
        // the key would represent one set of remap rules and the
        // output binary would have been compiled with a different
        // set, breaking the byte-for-byte invariant.
        self.execute_with_args(
            parsed,
            &parsed.all_args,
            compile::IncrementalMode::Strip,
            None,
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

fn rustc_refuse_reasons(
    parsed: &RustcArgs,
    build_script_out_dir: Option<&std::path::Path>,
) -> Vec<RefuseReason> {
    let mut reasons = Vec::new();
    if !parsed.is_primary {
        reasons.push(RefuseReason::NotPrimary);
    }
    if parsed.is_build_script_probe(build_script_out_dir) {
        reasons.push(RefuseReason::Unsupported(
            "rustc build-script probe — not yet",
        ));
    }
    // Expansion is atomic. A missing/non-UTF-8 response file or the unstable
    // shell-style subset keeps its original compact argv and passes through to
    // rustc, which remains the authority for diagnostics and exit status.
    if parsed.argfile_expansion_failed() {
        reasons.push(RefuseReason::Unsupported(
            "rustc response file could not be safely expanded — not yet supported",
        ));
    }
    if parsed.dep_info_output.is_some() {
        reasons.push(RefuseReason::Unsupported(
            "rustc explicit --emit=dep-info=<path> output — not cacheable",
        ));
    }
    // `--pretty`/`--unpretty` (and the `-Z unpretty=…` form) make rustc dump
    // (un)formatted source to stdout *instead* of producing the normal
    // artifacts. The key never reflected these flags, so against a warm cache
    // kache would replay a prior compile's artifacts and skip the requested
    // source dump entirely — the caller gets a binary where it asked for
    // expanded source. Neither cacheable nor keyable; pass through to rustc.
    let wants_source_dump = parsed.all_args.iter().any(|a| {
        a == "--pretty"
            || a == "--unpretty"
            || a.starts_with("--pretty=")
            || a.starts_with("--unpretty=")
    }) || parsed
        .unstable_flags
        .iter()
        .any(|z| z == "unpretty" || z.starts_with("unpretty="));
    if wants_source_dump {
        reasons.push(RefuseReason::Unsupported(
            "rustc --pretty/--unpretty source dump — not cacheable",
        ));
    }
    if let Some(reason) = wasm_link_refusal(parsed) {
        reasons.push(RefuseReason::Unsupported(reason));
    }
    reasons
}

/// Built-in WebAssembly targets whose linker and CRT ship in rustc.
/// Custom target specs never enter this list.
const COMPILER_BUNDLED_WASM_TARGETS: &[&str] = &[
    "wasm32-unknown-unknown",
    "wasm32-wasi",
    "wasm32-wasip1",
    "wasm32-wasip1-threads",
    "wasm32-wasip2",
    "wasm32v1-none",
    "wasm64-unknown-unknown",
];

fn is_compiler_bundled_wasm_target(target: &str) -> bool {
    COMPILER_BUNDLED_WASM_TARGETS.contains(&target)
}

fn is_custom_target_spec(target: &str) -> bool {
    target.contains('/') || target.contains('\\') || target.ends_with(".json")
}

fn is_wasm_triple(target: &str) -> bool {
    target
        .split(['-', '.', '/', '\\'])
        .any(|component| component.starts_with("wasm"))
}

fn link_self_contained_disabled(parsed: &RustcArgs) -> bool {
    match parsed.get_codegen_opt("link-self-contained") {
        None => false,
        Some("y" | "yes" | "on" | "true") => false,
        Some(_) => true,
    }
}

/// Explicit admission for rustc-bundled self-contained WASM links.
///
/// Custom specs, emscripten, and `link-self-contained=no` stay passthrough:
/// those links depend on an external toolchain or CRT that kache does not
/// pin. rlibs and metadata-only emits are unaffected.
fn wasm_link_refusal(parsed: &RustcArgs) -> Option<&'static str> {
    if !parsed.is_executable_output() || !parsed.emits_link() {
        return None;
    }
    let target = parsed.target.as_deref()?;
    if is_custom_target_spec(target) && is_wasm_triple(target) {
        return Some("wasm custom target spec — not yet");
    }
    if is_compiler_bundled_wasm_target(target) {
        if link_self_contained_disabled(parsed) {
            return Some("wasm link-self-contained disabled — not yet");
        }
        return None;
    }
    if is_wasm_triple(target) {
        return Some("wasm target uses an external toolchain — not yet");
    }
    None
}

/// ld64 `-oso_prefix` for a cached macOS debug link.
///
/// A `-g` Mach-O records absolute object paths in `N_OSO`. Prefixing the
/// output directory makes those paths relative to `--out-dir`, so a restored
/// binary is not checkout-local. An invocation that already carries the flag
/// keeps the caller's spelling. Passthrough compiles skip injection so the
/// binary matches an unwrapped rustc.
#[cfg(target_os = "macos")]
fn macos_oso_prefix_flag(
    parsed: &RustcArgs,
    all_args: &[String],
    cache_this_compile: bool,
) -> Option<String> {
    macos_oso_prefix_flag_inner(parsed, all_args, cache_this_compile)
}

#[cfg(not(target_os = "macos"))]
fn macos_oso_prefix_flag(
    _parsed: &RustcArgs,
    _all_args: &[String],
    _cache_this_compile: bool,
) -> Option<String> {
    None
}

fn macos_oso_prefix_flag_inner(
    parsed: &RustcArgs,
    all_args: &[String],
    enabled: bool,
) -> Option<String> {
    if !enabled {
        return None;
    }
    if !parsed.debuginfo_enabled() || !parsed.is_executable_output() || !parsed.emits_link() {
        return None;
    }
    if all_args.iter().any(|arg| arg.contains("-oso_prefix,")) {
        return None;
    }
    if parsed
        .target
        .as_deref()
        .is_some_and(|target| !target.contains("-apple-darwin"))
    {
        return None;
    }
    let out_dir = parsed.out_dir.as_ref()?;
    if !out_dir.is_absolute() {
        return None;
    }
    let mut prefix = out_dir.display().to_string();
    if !prefix.ends_with(['/', '\\']) {
        prefix.push('/');
    }
    Some(format!("-Clink-arg=-Wl,-oso_prefix,{prefix}"))
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

        // Regression for issue #287: on Windows `cargo clippy` invokes the
        // wrapper as `kache <…>\clippy-driver.exe rustc -vV`. The `.exe`
        // suffix and backslash separators must not defeat detection — this
        // case ran as a clap subcommand before the fix ("unrecognized
        // subcommand"). These assertions hold on every host OS, so the
        // regression is caught without a Windows runner.
        assert!(RustcCompiler::recognizes(&s(&["rustc.exe"])));
        assert!(RustcCompiler::recognizes(&s(&["clippy-driver.exe"])));
        assert!(RustcCompiler::recognizes(&s(&[
            r"C:\Program Files\Rust\bin\rustc.exe"
        ])));
        assert!(RustcCompiler::recognizes(&s(&[
            r"G:\.rustup\toolchains\nightly-x86_64-pc-windows-msvc\bin\clippy-driver.exe"
        ])));
        // Detection is architecture-independent: the binary is named
        // `clippy-driver.exe` / `rustc.exe` identically on ARM64 (aarch64),
        // i686, and the gnu toolchain. The arch substring in the toolchain
        // dir is incidental — only the `.exe` basename matters.
        assert!(RustcCompiler::recognizes(&s(&[
            r"C:\Users\dev\.rustup\toolchains\stable-aarch64-pc-windows-msvc\bin\clippy-driver.exe"
        ])));
        assert!(RustcCompiler::recognizes(&s(&[
            r"C:\.rustup\toolchains\stable-x86_64-pc-windows-gnu\bin\rustc.exe"
        ])));
        // `.exe` matching is case-insensitive (Windows filesystems).
        assert!(RustcCompiler::recognizes(&s(&["clippy-driver.EXE"])));

        assert!(!RustcCompiler::recognizes(&s(&["gcc"])));
        assert!(!RustcCompiler::recognizes(&s(&["--crate-name"])));
        // C-family compilers (incl. their Windows `.exe` forms) belong to the
        // cc adapter, not rustc.
        assert!(!RustcCompiler::recognizes(&s(&["gcc.exe"])));
        assert!(!RustcCompiler::recognizes(&s(&[
            r"C:\msys64\bin\clang.exe"
        ])));
        // Empty argv: there is nothing to recognize.
        assert!(!RustcCompiler::recognizes(&[]));
    }

    #[test]
    fn id_is_rustc() {
        assert_eq!(RustcCompiler::new().id(), RUSTC_ID);
    }

    #[test]
    fn adapter_descriptor_uses_rustc_recognizer() {
        assert_eq!(ADAPTER.id(), RUSTC_ID);
        assert!(ADAPTER.recognizes(&s(&["rustc"])));
        assert!(!ADAPTER.recognizes(&s(&["cc"])));
    }

    #[test]
    fn refuse_reasons_returns_not_primary_for_query_invocations() {
        // `rustc -vV` is a version query, not a primary compilation
        let parsed = RustcCompiler::new().parse(&s(&["rustc", "-vV"])).unwrap();
        let reasons = RustcCompiler::new().refuse_reasons(&parsed);
        assert!(matches!(reasons.as_slice(), [RefuseReason::NotPrimary]));
    }

    #[test]
    fn refuse_reasons_returns_not_primary_for_source_bearing_print_query() {
        // substrate-wasm-builder probes the wasm artifact name with a real
        // crate/source pair plus `--print=file-names`. It is still a query: rustc
        // prints the name and deliberately emits no dep-info or compile output.
        for print in [
            ["--print", "file-names"].as_slice(),
            ["--print=file-names"].as_slice(),
        ] {
            let mut argv = s(&[
                "rustc",
                "src/lib.rs",
                "--crate-name",
                "dummy_crate",
                "--crate-type",
                "rlib",
            ]);
            argv.extend(print.iter().map(|arg| (*arg).to_string()));
            let parsed = RustcCompiler::new().parse(&argv).unwrap();
            assert!(
                parsed.residual_args.is_empty(),
                "query value leaked into residual args: {print:?} -> {:?}",
                parsed.residual_args
            );
            let reasons = RustcCompiler::new().refuse_reasons(&parsed);
            assert!(
                matches!(reasons.as_slice(), [RefuseReason::NotPrimary]),
                "source-bearing print query was treated as a compile: {print:?} -> {reasons:?}"
            );
        }
    }

    #[test]
    fn refuse_reasons_returns_not_primary_for_source_bearing_info_query() {
        for query in ["-V", "--version", "-h", "--help", "-vV"] {
            let parsed = RustcCompiler::new()
                .parse(&s(&[
                    "rustc",
                    "src/lib.rs",
                    "--crate-name",
                    "dummy_crate",
                    query,
                ]))
                .unwrap();
            let reasons = RustcCompiler::new().refuse_reasons(&parsed);
            assert!(
                matches!(reasons.as_slice(), [RefuseReason::NotPrimary]),
                "source-bearing info query was treated as a compile: {query} -> {reasons:?}"
            );
        }
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

    #[test]
    fn refuse_reasons_accepts_expanded_response_file() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, "pub fn answer() -> u32 { 42 }\n").unwrap();
        let response = dir.path().join("rustc.args");
        std::fs::write(
            &response,
            format!(
                "--crate-name\nfoo\n{}\n--crate-type\nlib\n",
                source.display()
            ),
        )
        .unwrap();

        let parsed = RustcCompiler::new()
            .parse(&["rustc".to_string(), format!("@{}", response.display())])
            .unwrap();
        assert!(parsed.is_primary);
        let reasons = RustcCompiler::new().refuse_reasons(&parsed);
        assert!(reasons.is_empty(), "unexpected refusal: {reasons:?}");
    }

    #[test]
    fn refuse_reasons_refuses_unreadable_response_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.args");
        let parsed = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "--crate-name",
                "foo",
                "src/lib.rs",
                &format!("@{}", missing.display()),
            ]))
            .unwrap();
        let reasons = RustcCompiler::new().refuse_reasons(&parsed);
        assert!(
            reasons.iter().any(|r| matches!(
                r,
                RefuseReason::Unsupported(d) if d.contains("response file")
            )),
            "expected a response-file refusal, got {reasons:?}"
        );
    }

    #[test]
    fn refuse_reasons_refuses_explicit_dep_info_output_path() {
        let parsed = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "src/lib.rs",
                "--crate-name",
                "foo",
                "--emit=metadata,dep-info=custom/deps.mk",
            ]))
            .unwrap();
        let reasons = RustcCompiler::new().refuse_reasons(&parsed);
        assert!(reasons.iter().any(|reason| matches!(
            reason,
            RefuseReason::Unsupported(detail) if detail.contains("explicit --emit=dep-info")
        )));
    }

    #[test]
    fn refuse_reasons_refuses_unpretty_source_dump() {
        // `--unpretty` / `--pretty` / `-Z unpretty=…` make rustc emit source to
        // stdout instead of artifacts. None of these are folded into the key, so
        // a warm cache would replay the wrong (artifact) output. Must pass through.
        for args in [
            vec![
                "rustc",
                "--crate-name",
                "foo",
                "src/lib.rs",
                "--unpretty=expanded",
            ],
            vec![
                "rustc",
                "--crate-name",
                "foo",
                "src/lib.rs",
                "--pretty",
                "normal",
            ],
            vec![
                "rustc",
                "--crate-name",
                "foo",
                "src/lib.rs",
                "-Z",
                "unpretty=hir",
            ],
        ] {
            let parsed = RustcCompiler::new().parse(&s(&args)).unwrap();
            let reasons = RustcCompiler::new().refuse_reasons(&parsed);
            assert!(
                reasons.iter().any(|r| matches!(
                    r,
                    RefuseReason::Unsupported(d) if d.contains("unpretty")
                )),
                "expected an unpretty refusal for {args:?}, got {reasons:?}"
            );
        }
    }

    #[test]
    fn refuse_reasons_identifies_build_script_probe() {
        let parsed = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "--edition=2018",
                "--crate-name=thiserror",
                "--crate-type=lib",
                "--emit=dep-info,metadata",
                "--out-dir",
                "/work/proj/target/release/build/thiserror-abc/out/probe",
                "build/probe.rs",
            ]))
            .unwrap();

        let reasons = rustc_refuse_reasons(
            &parsed,
            Some(std::path::Path::new(
                "/work/proj/target/release/build/thiserror-abc/out",
            )),
        );
        assert_eq!(reasons.len(), 1);
        assert_eq!(
            reasons[0].description(),
            "rustc build-script probe — not yet"
        );
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
    fn crate_type_produces_metadata_splits_rmeta_emitters() {
        // Measured against rustc 1.97: `--emit=metadata` writes a real `.rmeta`
        // for these, so an empty one means a truncated write (#624).
        for ct in ["lib", "rlib", "dylib", "proc-macro"] {
            assert!(crate_type_produces_metadata(ct), "{ct} emits metadata");
        }
        // These create the `.rmeta` cargo expects but leave it zero bytes.
        for ct in ["bin", "cdylib", "staticlib"] {
            assert!(!crate_type_produces_metadata(ct), "{ct} emits no metadata");
        }
        // Unknown crate-type: keep the zero-byte corruption guard.
        assert!(crate_type_produces_metadata("future-rustc-type-2030"));
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

    fn linked_wasm(target: &str, extra: &[&str]) -> RustcArgs {
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "widget".to_string(),
            "--crate-type".to_string(),
            "bin".to_string(),
            "--emit".to_string(),
            "link".to_string(),
            "--target".to_string(),
            target.to_string(),
            "src/main.rs".to_string(),
        ];
        argv.extend(extra.iter().map(|s| s.to_string()));
        RustcCompiler::new().parse(&argv).unwrap()
    }

    #[test]
    fn refuse_reasons_admits_native_host_bins() {
        let parsed = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "--crate-name",
                "app",
                "--crate-type",
                "bin",
                "--emit",
                "link",
                "--target",
                "x86_64-unknown-linux-gnu",
                "src/main.rs",
            ]))
            .unwrap();
        assert!(
            RustcCompiler::new().refuse_reasons(&parsed).is_empty(),
            "a native GNU bin is not a WASM link: {:?}",
            RustcCompiler::new().refuse_reasons(&parsed)
        );
    }

    #[test]
    fn refuse_reasons_treats_path_only_wasm_target_as_custom_spec() {
        let parsed = linked_wasm("/tmp/wasm32-unknown-unknown", &[]);
        let reasons = RustcCompiler::new().refuse_reasons(&parsed);
        assert!(
            reasons.iter().any(|r| matches!(
                r,
                RefuseReason::Unsupported(d) if d.contains("custom target spec")
            )),
            "a path-shaped wasm target must pass through, got {reasons:?}"
        );
    }

    #[test]
    fn refuse_reasons_admits_compiler_bundled_wasm_links() {
        for target in COMPILER_BUNDLED_WASM_TARGETS {
            let parsed = linked_wasm(target, &[]);
            let reasons = RustcCompiler::new().refuse_reasons(&parsed);
            assert!(
                reasons.is_empty(),
                "{target} should use rustc's bundled linker, got {reasons:?}"
            );
        }

        let yes = linked_wasm("wasm32-wasip1", &["-Clink-self-contained=yes"]);
        assert!(RustcCompiler::new().refuse_reasons(&yes).is_empty());
    }

    #[test]
    fn refuse_reasons_passthroughs_external_wasm_toolchains() {
        for target in ["wasm32-unknown-emscripten", "wasm32-wali-linux-musl"] {
            let parsed = linked_wasm(target, &[]);
            let reasons = RustcCompiler::new().refuse_reasons(&parsed);
            assert!(
                reasons.iter().any(|r| matches!(
                    r,
                    RefuseReason::Unsupported(d) if d.contains("external toolchain")
                )),
                "{target} should pass through, got {reasons:?}"
            );
        }

        let custom = linked_wasm("/tmp/wasm32-unknown-unknown.json", &[]);
        let reasons = RustcCompiler::new().refuse_reasons(&custom);
        assert!(
            reasons.iter().any(|r| matches!(
                r,
                RefuseReason::Unsupported(d) if d.contains("custom target spec")
            )),
            "custom wasm spec should pass through, got {reasons:?}"
        );

        let disabled = linked_wasm("wasm32-unknown-unknown", &["-Clink-self-contained=no"]);
        let reasons = RustcCompiler::new().refuse_reasons(&disabled);
        assert!(
            reasons.iter().any(|r| matches!(
                r,
                RefuseReason::Unsupported(d) if d.contains("link-self-contained")
            )),
            "link-self-contained=no should pass through, got {reasons:?}"
        );
    }

    #[test]
    fn refuse_reasons_does_not_refuse_wasm_rlibs() {
        let parsed = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "--crate-name",
                "widget",
                "--crate-type",
                "rlib",
                "--target",
                "wasm32-unknown-emscripten",
                "src/lib.rs",
            ]))
            .unwrap();
        assert!(RustcCompiler::new().refuse_reasons(&parsed).is_empty());
    }

    fn debug_bin_args(out_dir: &Path, extra: &[&str]) -> (RustcArgs, Vec<String>) {
        let mut argv = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "foo".to_string(),
            "--crate-type".to_string(),
            "bin".to_string(),
            "-g".to_string(),
            "--out-dir".to_string(),
            out_dir.display().to_string(),
            "src/main.rs".to_string(),
        ];
        argv.extend(extra.iter().map(|s| s.to_string()));
        let parsed = RustcCompiler::new().parse(&argv).unwrap();
        (parsed, argv)
    }

    #[test]
    fn oso_prefix_is_injected_for_cached_macos_debug_links() {
        let dir = tempfile::tempdir().unwrap();
        let out_dir = dir.path().join("deps");
        std::fs::create_dir_all(&out_dir).unwrap();

        let (parsed, argv) = debug_bin_args(&out_dir, &[]);
        let flag = macos_oso_prefix_flag_inner(&parsed, &argv, true).unwrap();
        let mut prefix = out_dir.display().to_string();
        if !prefix.ends_with('/') && !prefix.ends_with('\\') {
            prefix.push('/');
        }
        assert_eq!(flag, format!("-Clink-arg=-Wl,-oso_prefix,{prefix}"));

        let already = debug_bin_args(&out_dir, &["-Clink-arg=-Wl,-oso_prefix,/elsewhere/"]);
        assert!(macos_oso_prefix_flag_inner(&already.0, &already.1, true).is_none());

        let wasm = debug_bin_args(&out_dir, &["--target", "wasm32-unknown-unknown"]);
        assert!(macos_oso_prefix_flag_inner(&wasm.0, &wasm.1, true).is_none());

        let release = RustcCompiler::new()
            .parse(&s(&[
                "rustc",
                "--crate-name",
                "foo",
                "--crate-type",
                "bin",
                "--out-dir",
                out_dir.to_str().unwrap(),
                "src/main.rs",
            ]))
            .unwrap();
        assert!(
            macos_oso_prefix_flag_inner(&release, &release.all_args, true).is_none(),
            "no debuginfo means no oso_prefix"
        );

        let relative = debug_bin_args(Path::new("target/debug/deps"), &[]);
        assert!(macos_oso_prefix_flag_inner(&relative.0, &relative.1, true).is_none());

        let (parsed, argv) = debug_bin_args(&out_dir, &[]);
        assert!(
            macos_oso_prefix_flag_inner(&parsed, &argv, false).is_none(),
            "passthrough must not rewrite the link"
        );
    }
}
