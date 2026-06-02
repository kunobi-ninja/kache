use anyhow::{Result, bail};
use std::path::{Path, PathBuf};

use crate::compiler::rustc::RustcCompiler;

/// Parsed rustc invocation arguments relevant to caching.
#[derive(Debug, Clone, Default)]
pub struct RustcArgs {
    /// Path to the rustc binary (first arg from cargo when using RUSTC_WRAPPER)
    pub rustc: PathBuf,
    /// Crate name (--crate-name)
    pub crate_name: Option<String>,
    /// Crate type (--crate-type): lib, rlib, proc-macro, bin, dylib, cdylib, etc.
    pub crate_types: Vec<String>,
    /// Output path (-o)
    pub output: Option<PathBuf>,
    /// Output directory (--out-dir)
    pub out_dir: Option<PathBuf>,
    /// Emit types (--emit): dep-info, metadata, link, etc.
    pub emit: Vec<String>,
    /// Source file (positional argument, typically the .rs file)
    pub source_file: Option<PathBuf>,
    /// Extern dependencies (--extern name=path)
    pub externs: Vec<ExternDep>,
    /// Target triple (--target)
    pub target: Option<String>,
    /// Edition (--edition)
    pub edition: Option<String>,
    /// Codegen options (-C key=value)
    pub codegen_opts: Vec<(String, Option<String>)>,
    /// Feature cfg flags (--cfg 'feature="name"')
    pub features: Vec<String>,
    /// All cfg flags (--cfg)
    pub cfgs: Vec<String>,
    /// Extra output file path (--extra-filename)
    pub extra_filename: Option<String>,
    /// Whether incremental compilation is enabled (-C incremental=...)
    pub incremental: Option<PathBuf>,
    /// Sysroot override (`--sysroot <path>`). Selects which std/core/
    /// proc-macro libs rustc links against, so it is codegen-relevant
    /// and must be part of the key (normalized at key time).
    pub sysroot: Option<PathBuf>,
    /// Native library search paths (`-L [KIND=]PATH`). Stored raw (kind
    /// prefix preserved); `compute_cache_key` path-normalizes the path
    /// and skips cargo's redundant `dependency=`/`crate=` entries.
    pub link_search: Vec<String>,
    /// Native libraries to link (`-l [KIND[:MODIFIERS]=]NAME`). Build
    /// scripts emit these via `cargo:rustc-link-lib`; they change a
    /// linked artifact without going through RUSTFLAGS, so they must be
    /// keyed. Machine-independent — hashed raw.
    pub link_libs: Vec<String>,
    /// Unstable `-Z` flags. Can change codegen (e.g. `-Zsanitizer`,
    /// `-Zshare-generics`) and arrive on argv outside RUSTFLAGS.
    pub unstable_flags: Vec<String>,
    /// Inner rustc path for double-wrapper case (RUSTC_WRAPPER + RUSTC_WORKSPACE_WRAPPER).
    /// When both wrappers are active, cargo passes: wrapper workspace_wrapper rustc <args>.
    /// This field holds the rustc path that the workspace wrapper expects as its first arg.
    pub inner_rustc: Option<PathBuf>,
    /// All original arguments (everything after the rustc path)
    pub all_args: Vec<String>,
    /// Whether this is a `--test` compilation (test harness binary)
    pub is_test: bool,
    /// Whether this looks like a primary compilation (has source file + crate name)
    pub is_primary: bool,
}

#[derive(Debug, Clone)]
pub struct ExternDep {
    pub name: String,
    pub path: Option<PathBuf>,
}

impl RustcArgs {
    /// Parse RUSTC_WRAPPER-style arguments.
    /// In RUSTC_WRAPPER mode, argv[0] = kache, argv[1] = rustc path, argv[2..] = rustc args.
    pub fn parse(args: &[String]) -> Result<Self> {
        if args.len() < 2 {
            bail!("expected at least rustc path as first argument");
        }

        let rustc = PathBuf::from(&args[0]);

        // Detect double-wrapper: if args[1] also looks like a compiler, this is
        // RUSTC_WRAPPER + RUSTC_WORKSPACE_WRAPPER. The inner path is the actual
        // rustc that the workspace wrapper (args[0]) expects as its first arg.
        let (inner_rustc, rustc_args) = if args.len() >= 3 && RustcCompiler::recognizes(&args[1..])
        {
            (Some(PathBuf::from(&args[1])), &args[2..])
        } else {
            (None, &args[1..])
        };

        let mut parsed = RustcArgs {
            rustc,
            crate_name: None,
            crate_types: Vec::new(),
            output: None,
            out_dir: None,
            emit: Vec::new(),
            source_file: None,
            externs: Vec::new(),
            target: None,
            edition: None,
            codegen_opts: Vec::new(),
            features: Vec::new(),
            cfgs: Vec::new(),
            extra_filename: None,
            incremental: None,
            sysroot: None,
            link_search: Vec::new(),
            link_libs: Vec::new(),
            unstable_flags: Vec::new(),
            inner_rustc,
            all_args: rustc_args.to_vec(),
            is_test: false,
            is_primary: false,
        };

        let mut i = 0;
        while i < rustc_args.len() {
            let arg = &rustc_args[i];

            match arg.as_str() {
                "--crate-name" => {
                    i += 1;
                    parsed.crate_name = rustc_args.get(i).cloned();
                }
                "--crate-type" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        parsed.crate_types.push(val.clone());
                    }
                }
                "-o" => {
                    i += 1;
                    parsed.output = rustc_args.get(i).map(PathBuf::from);
                }
                "--out-dir" => {
                    i += 1;
                    parsed.out_dir = rustc_args.get(i).map(PathBuf::from);
                }
                "--emit" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        for part in val.split(',') {
                            // emit can be "dep-info=path" or just "metadata"
                            let kind = part.split('=').next().unwrap_or(part);
                            parsed.emit.push(kind.to_string());
                        }
                    }
                }
                "--target" => {
                    i += 1;
                    parsed.target = rustc_args.get(i).cloned();
                }
                "--edition" => {
                    i += 1;
                    parsed.edition = rustc_args.get(i).cloned();
                }
                "--extern" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        parsed.externs.push(parse_extern(val));
                    }
                }
                "--cfg" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        parsed.cfgs.push(val.clone());
                        if let Some(feat) = parse_feature_cfg(val) {
                            parsed.features.push(feat);
                        }
                    }
                }
                "--extra-filename" if false => {
                    // --extra-filename is actually passed via -C extra-filename=...
                }
                _ if arg.starts_with("--emit=") => {
                    let val = &arg["--emit=".len()..];
                    for part in val.split(',') {
                        let kind = part.split('=').next().unwrap_or(part);
                        parsed.emit.push(kind.to_string());
                    }
                }
                "--test" => {
                    parsed.is_test = true;
                }
                _ if arg.starts_with("--crate-type=") => {
                    let val = &arg["--crate-type=".len()..];
                    parsed.crate_types.push(val.to_string());
                }
                _ if arg.starts_with("--crate-name=") => {
                    parsed.crate_name = Some(arg["--crate-name=".len()..].to_string());
                }
                _ if arg.starts_with("--target=") => {
                    parsed.target = Some(arg["--target=".len()..].to_string());
                }
                _ if arg.starts_with("--edition=") => {
                    parsed.edition = Some(arg["--edition=".len()..].to_string());
                }
                _ if arg.starts_with("--extern=") => {
                    parsed.externs.push(parse_extern(&arg["--extern=".len()..]));
                }
                _ if arg.starts_with("--cfg=") => {
                    let val = &arg["--cfg=".len()..];
                    parsed.cfgs.push(val.to_string());
                    if let Some(feat) = parse_feature_cfg(val) {
                        parsed.features.push(feat);
                    }
                }
                "-C" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        let (key, value) = parse_codegen_opt(val);
                        if key == "extra-filename" {
                            parsed.extra_filename = value.clone();
                        }
                        if key == "incremental" {
                            parsed.incremental = value.as_ref().map(PathBuf::from);
                        }
                        parsed.codegen_opts.push((key, value));
                    }
                }
                _ if arg.starts_with("-C") && arg.len() > 2 => {
                    let val = &arg[2..];
                    let (key, value) = parse_codegen_opt(val);
                    if key == "extra-filename" {
                        parsed.extra_filename = value.clone();
                    }
                    if key == "incremental" {
                        parsed.incremental = value.as_ref().map(PathBuf::from);
                    }
                    parsed.codegen_opts.push((key, value));
                }
                "--sysroot" => {
                    i += 1;
                    parsed.sysroot = rustc_args.get(i).map(PathBuf::from);
                }
                _ if arg.starts_with("--sysroot=") => {
                    parsed.sysroot = Some(PathBuf::from(&arg["--sysroot=".len()..]));
                }
                // Native link search paths / libraries. cargo passes
                // `-L dependency=…` / `--extern` for rlib resolution (the
                // latter already content-hashed); build scripts add
                // `-L native=…` / `-l name` that change a linked artifact.
                // Both separate (`-L val`) and attached (`-Lval`) forms.
                "-L" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        parsed.link_search.push(val.clone());
                    }
                }
                _ if arg.starts_with("-L") && arg.len() > 2 => {
                    parsed.link_search.push(arg["-L".len()..].to_string());
                }
                "-l" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        parsed.link_libs.push(val.clone());
                    }
                }
                _ if arg.starts_with("-l") && arg.len() > 2 => {
                    parsed.link_libs.push(arg["-l".len()..].to_string());
                }
                "-Z" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        parsed.unstable_flags.push(val.clone());
                    }
                }
                _ if arg.starts_with("-Z") && arg.len() > 2 => {
                    parsed.unstable_flags.push(arg["-Z".len()..].to_string());
                }
                // Positional argument: source file (doesn't start with -)
                _ if !arg.starts_with('-')
                    && parsed.source_file.is_none()
                    && (arg.ends_with(".rs") || std::path::Path::new(arg).exists()) =>
                {
                    parsed.source_file = Some(PathBuf::from(arg));
                }
                _ => {}
            }
            i += 1;
        }

        parsed.features.sort();
        parsed.is_primary = parsed.crate_name.is_some() && parsed.source_file.is_some();

        Ok(parsed)
    }

    /// Whether this invocation produces an artifact the OS loads at runtime
    /// (executable, dylib, cdylib, proc-macro, or a `--test` harness binary).
    ///
    /// Derived from [`crate::compiler::rustc::classify_crate_type`] +
    /// [`crate::compiler::ArtifactKind::link_strategy`] — single source of
    /// truth shared with the per-file classifier in
    /// [`crate::compiler::Compiler::classify_output`]. Adding a new
    /// rustc crate-type to that mapping automatically updates this
    /// predicate (and every caller of it: cache_key linker hash,
    /// wrapper cache_executables gating, etc.).
    pub fn is_executable_output(&self) -> bool {
        use crate::compiler::rustc::classify_crate_type;
        use crate::link::LinkStrategy;
        self.is_test
            || self
                .crate_types
                .iter()
                .any(|t| classify_crate_type(t).link_strategy() == LinkStrategy::Copy)
    }

    /// Whether this compilation produces an artifact the user
    /// directly consumes (a `bin` they run, a `--test` they invoke).
    ///
    /// Distinct from [`Self::is_executable_output`]: that predicate
    /// is broader, covering every artifact whose link strategy is
    /// `Copy` — which includes `dylib` / `cdylib` / `proc-macro`.
    /// The wrapper uses this narrower check to gate the
    /// skip-cache-for-executables behavior, because proc-macros and
    /// dylibs are build-time concerns (rustc loads them, not the
    /// user) and ARE safely cacheable: PR #72's verify-then-sign
    /// handles macOS dyld signature checks on restore, so a cached
    /// proc-macro `.dylib` doesn't risk loading a stale or unsigned
    /// blob.
    ///
    /// Without this split, proc-macro deps recompile every build →
    /// non-byte-identical `.dylib` outputs → downstream crates that
    /// `--extern` them get unstable cache keys (the e422e55 relocate
    /// failure mode).
    pub fn is_user_facing_executable(&self) -> bool {
        self.is_test || self.crate_types.iter().any(|t| t == "bin")
    }

    /// Derive the workspace root from `--out-dir`. Cargo invokes
    /// rustc with `--out-dir <workspace>/target/<profile>/deps`, so
    /// three `parent()` steps land on the workspace root.
    ///
    /// Returns `None` if `--out-dir` wasn't set or doesn't have the
    /// expected three-level shape — defensive, but cargo always sets
    /// it for cacheable invocations.
    ///
    /// Centralized here so both the cache_key construction (in
    /// `wrapper::run`) and the rustc invocation construction (in
    /// `RustcCompiler::execute`) derive the workspace from the same
    /// source. Otherwise PathNormalizer would compute different
    /// rules for the two consumers and the cache key wouldn't reflect
    /// the actual remap injection.
    pub fn workspace_root(&self) -> Option<PathBuf> {
        self.out_dir
            .as_ref()
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .and_then(|p| p.parent())
            .map(std::path::Path::to_path_buf)
    }

    /// Derive the cargo target directory (e.g. `<workspace>/target`) from
    /// the rustc args.
    ///
    /// This is the anchor for dep-info (`.d`) path rewriting. Cargo invokes
    /// rustc with cwd = the package source dir — *not* the target dir — so
    /// `std::env::current_dir()` cannot be used. Cargo's output layout is
    /// stable enough to infer the target dir from the args instead:
    ///
    /// - `--out-dir` is `<target>/<profile>/deps` for libs/bins → walk up 2.
    /// - `-o` for a build script is
    ///   `<target>/<profile>/build/<pkg>/build_script_build-<hash>`; walk up
    ///   to the ancestor named `deps` or `build`, then take its grandparent.
    ///
    /// Store and restore must agree on this anchor: the store side
    /// relativizes the `.d` against it (`<target>/...` → kache's dep-info
    /// sentinel) and the restore side expands that sentinel back against
    /// *this* invocation's target dir. Because the `.d`'s paths are all
    /// rooted under `<target>`, the relativize→expand round-trip yields paths
    /// valid at whatever location the restoring build runs from.
    ///
    /// Returns `None` for invocations outside cargo's layout (e.g. ad-hoc
    /// `rustc -o /tmp/prog`), so dep-info rewriting is skipped rather than
    /// anchored to a wrong directory.
    pub fn target_dir(&self) -> Option<PathBuf> {
        if let Some(od) = &self.out_dir {
            return od.parent()?.parent().map(Path::to_path_buf);
        }
        let out = self.output.as_deref()?;
        let mut cursor = out.parent();
        while let Some(dir) = cursor {
            if let Some(name) = dir.file_name()
                && (name == "deps" || name == "build")
            {
                return dir.parent()?.parent().map(Path::to_path_buf);
            }
            cursor = dir.parent();
        }
        None
    }

    /// Whether this rustc invocation looks like a build-script feature probe.
    ///
    /// Crates such as `proc-macro2`, `thiserror`, and `anyhow` run small rustc
    /// probes from their build scripts to detect compiler features. Those
    /// commands intentionally may fail, usually emit metadata only, and write
    /// under the build script's `OUT_DIR`. They are not useful cache entries:
    /// pass them through so expected probe failures do not appear as kache
    /// cache errors.
    pub fn is_build_script_probe(&self, build_script_out_dir: Option<&Path>) -> bool {
        let Some(build_script_out_dir) = build_script_out_dir else {
            return false;
        };
        let metadata_only = !self.emit.is_empty() && !self.emit.iter().any(|e| e == "link");
        if !metadata_only {
            return false;
        }

        self.out_dir
            .as_deref()
            .is_some_and(|out_dir| out_dir.starts_with(build_script_out_dir))
            || self
                .source_file
                .as_deref()
                .is_some_and(|source| source.starts_with(build_script_out_dir))
    }

    /// Get the output filename stem (crate name + extra filename).
    #[allow(dead_code)]
    pub fn output_stem(&self) -> Option<String> {
        let name = self.crate_name.as_ref()?;
        let extra = self.extra_filename.as_deref().unwrap_or("");
        Some(format!("{name}{extra}"))
    }

    /// Whether this compilation has coverage instrumentation enabled (-C instrument-coverage).
    /// When active, path remapping must be skipped so coverage tools (tarpaulin, llvm-cov)
    /// can map profraw data back to source files.
    pub fn has_coverage_instrumentation(&self) -> bool {
        self.codegen_opts
            .iter()
            .any(|(k, _)| k == "instrument-coverage")
    }

    /// Get a codegen option value by key.
    pub fn get_codegen_opt(&self, key: &str) -> Option<&str> {
        self.codegen_opts
            .iter()
            .find(|(k, _)| k == key)
            .and_then(|(_, v)| v.as_deref())
    }
}

fn parse_extern(s: &str) -> ExternDep {
    // Format: name=path or just name
    // Can also be: priv:name=path or noprelude:name=path
    let s = s
        .strip_prefix("priv:")
        .or_else(|| s.strip_prefix("noprelude:"))
        .unwrap_or(s);

    if let Some((name, path)) = s.split_once('=') {
        ExternDep {
            name: name.to_string(),
            path: Some(PathBuf::from(path)),
        }
    } else {
        ExternDep {
            name: s.to_string(),
            path: None,
        }
    }
}

fn parse_feature_cfg(s: &str) -> Option<String> {
    // --cfg 'feature="derive"' -> "derive"
    let s = s.strip_prefix("feature=\"")?.strip_suffix('"')?;
    Some(s.to_string())
}

fn parse_codegen_opt(s: &str) -> (String, Option<String>) {
    if let Some((key, value)) = s.split_once('=') {
        (key.to_string(), Some(value.to_string()))
    } else {
        (s.to_string(), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_lib() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "serde",
            "--edition=2021",
            "src/lib.rs",
            "--crate-type",
            "lib",
            "--emit=dep-info,metadata,link",
            "-C",
            "opt-level=3",
            "-C",
            "extra-filename=-d44c553",
            "--extern",
            "serde_derive=/path/to/libserde_derive.so",
            "-o",
            "/project/target/debug/deps/libserde-d44c553.rlib",
            "--cfg",
            "feature=\"derive\"",
            "--cfg",
            "feature=\"std\"",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.crate_name.as_deref(), Some("serde"));
        assert_eq!(parsed.crate_types, vec!["lib"]);
        assert_eq!(parsed.edition.as_deref(), Some("2021"));
        assert_eq!(parsed.emit, vec!["dep-info", "metadata", "link"]);
        assert_eq!(parsed.extra_filename.as_deref(), Some("-d44c553"));
        assert!(parsed.source_file.is_some());
        assert_eq!(parsed.externs.len(), 1);
        assert_eq!(parsed.externs[0].name, "serde_derive");
        assert_eq!(parsed.features, vec!["derive", "std"]);
        assert_eq!(
            parsed.output.as_ref().unwrap().to_string_lossy(),
            "/project/target/debug/deps/libserde-d44c553.rlib"
        );
        assert!(!parsed.is_executable_output());
        assert!(parsed.is_primary);
    }

    #[test]
    fn test_parse_bin_crate() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "myapp",
            "src/main.rs",
            "--crate-type",
            "bin",
            "-o",
            "/project/target/debug/myapp",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(parsed.is_executable_output());
    }

    #[test]
    fn test_parse_extern_with_prefix() {
        let dep = parse_extern("priv:core=/path/to/libcore.rlib");
        assert_eq!(dep.name, "core");
        assert!(dep.path.is_some());
    }

    #[test]
    fn test_feature_cfg_parsing() {
        assert_eq!(
            parse_feature_cfg("feature=\"derive\""),
            Some("derive".to_string())
        );
        assert_eq!(parse_feature_cfg("unix"), None);
    }

    #[test]
    fn test_parse_too_few_args() {
        let args: Vec<String> = vec!["rustc".into()];
        assert!(RustcArgs::parse(&args).is_err());
    }

    #[test]
    fn test_parse_empty_args() {
        let args: Vec<String> = vec![];
        assert!(RustcArgs::parse(&args).is_err());
    }

    #[test]
    fn test_parse_non_primary_no_source() {
        let args: Vec<String> = vec!["rustc", "--crate-name", "foo", "-C", "opt-level=3"]
            .into_iter()
            .map(String::from)
            .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(!parsed.is_primary);
    }

    #[test]
    fn test_parse_codegen_opt_lookup() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "-C",
            "opt-level=3",
            "-Cmetadata=abc123",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.get_codegen_opt("opt-level"), Some("3"));
        assert_eq!(parsed.get_codegen_opt("metadata"), Some("abc123"));
        assert_eq!(parsed.get_codegen_opt("nonexistent"), None);
    }

    #[test]
    fn test_is_executable_output_variants() {
        for crate_type in ["bin", "dylib", "cdylib", "proc-macro"] {
            let args: Vec<String> = vec!["rustc", "--crate-type", crate_type, "src/lib.rs"]
                .into_iter()
                .map(String::from)
                .collect();
            let parsed = RustcArgs::parse(&args).unwrap();
            assert!(
                parsed.is_executable_output(),
                "{crate_type} should be executable"
            );
        }
        for crate_type in ["lib", "rlib", "staticlib"] {
            let args: Vec<String> = vec!["rustc", "--crate-type", crate_type, "src/lib.rs"]
                .into_iter()
                .map(String::from)
                .collect();
            let parsed = RustcArgs::parse(&args).unwrap();
            assert!(
                !parsed.is_executable_output(),
                "{crate_type} should not be executable"
            );
        }

        // --test flag makes output executable regardless of crate type
        let args: Vec<String> = vec!["rustc", "--crate-type", "lib", "--test", "src/lib.rs"]
            .into_iter()
            .map(String::from)
            .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(parsed.is_test, "--test should set is_test");
        assert!(parsed.is_executable_output(), "--test should be executable");
    }

    #[test]
    fn test_is_user_facing_executable_excludes_proc_macro_and_dylib() {
        // The narrower predicate: only `bin` + `--test` count.
        // proc-macro / dylib / cdylib are build-time artifacts that
        // should be cacheable, not skipped via the
        // cache_executables gate. This is the contract that lets
        // multi-dep's relocate phase get to zero misses — a
        // recompiled-every-build proc-macro produces non-byte-
        // identical output that breaks downstream `extern:` keys.
        for (crate_type, expected) in [
            ("bin", true),
            ("lib", false),
            ("rlib", false),
            ("staticlib", false),
            ("dylib", false),
            ("cdylib", false),
            ("proc-macro", false),
        ] {
            let args: Vec<String> = vec!["rustc", "--crate-type", crate_type, "src/lib.rs"]
                .into_iter()
                .map(String::from)
                .collect();
            let parsed = RustcArgs::parse(&args).unwrap();
            assert_eq!(
                parsed.is_user_facing_executable(),
                expected,
                "{crate_type}: is_user_facing_executable mismatch"
            );
        }

        // --test makes any compilation user-facing (test harness).
        let args: Vec<String> = vec!["rustc", "--crate-type", "lib", "--test", "src/lib.rs"]
            .into_iter()
            .map(String::from)
            .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(
            parsed.is_user_facing_executable(),
            "--test must count as user-facing"
        );
    }

    #[test]
    fn test_output_stem() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "mylib",
            "src/lib.rs",
            "-C",
            "extra-filename=-abc123",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.output_stem(), Some("mylib-abc123".to_string()));
    }

    #[test]
    fn test_output_stem_no_extra() {
        let args: Vec<String> = vec!["rustc", "--crate-name", "mylib", "src/lib.rs"]
            .into_iter()
            .map(String::from)
            .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.output_stem(), Some("mylib".to_string()));
    }

    #[test]
    fn test_output_stem_no_name() {
        let args: Vec<String> = vec!["rustc", "src/lib.rs"]
            .into_iter()
            .map(String::from)
            .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.output_stem(), None);
    }

    #[test]
    fn test_parse_extern_name_only() {
        let dep = parse_extern("core");
        assert_eq!(dep.name, "core");
        assert!(dep.path.is_none());
    }

    #[test]
    fn test_parse_extern_noprelude() {
        let dep = parse_extern("noprelude:std=/path/to/libstd.rlib");
        assert_eq!(dep.name, "std");
        assert!(dep.path.is_some());
    }

    #[test]
    fn test_parse_codegen_opt_no_value() {
        let (key, value) = parse_codegen_opt("debuginfo");
        assert_eq!(key, "debuginfo");
        assert!(value.is_none());
    }

    #[test]
    fn test_parse_codegen_opt_with_value() {
        let (key, value) = parse_codegen_opt("opt-level=3");
        assert_eq!(key, "opt-level");
        assert_eq!(value, Some("3".to_string()));
    }

    #[test]
    fn test_parse_incremental_flag() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "-C",
            "incremental=/tmp/incr",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.incremental, Some(PathBuf::from("/tmp/incr")));
    }

    #[test]
    fn test_parse_target_and_out_dir() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "foo",
            "--target",
            "aarch64-apple-darwin",
            "--out-dir",
            "/project/target/debug/deps",
            "src/lib.rs",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.target.as_deref(), Some("aarch64-apple-darwin"));
        assert_eq!(
            parsed.out_dir,
            Some(PathBuf::from("/project/target/debug/deps"))
        );
    }

    #[test]
    fn test_parse_equals_form_args() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name=mylib",
            "--crate-type=rlib",
            "--target=x86_64-unknown-linux-gnu",
            "--edition=2021",
            "--cfg=unix",
            "--extern=serde=/path/lib.rlib",
            "src/lib.rs",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.crate_name.as_deref(), Some("mylib"));
        assert_eq!(parsed.crate_types, vec!["rlib"]);
        assert_eq!(parsed.target.as_deref(), Some("x86_64-unknown-linux-gnu"));
        assert_eq!(parsed.edition.as_deref(), Some("2021"));
        assert!(parsed.cfgs.contains(&"unix".to_string()));
        assert_eq!(parsed.externs[0].name, "serde");
    }

    #[test]
    fn test_parse_double_wrapper() {
        // Simulates: kache clippy-driver /path/to/rustc --crate-name foo src/lib.rs --crate-type lib
        // After main.rs strips argv[0], parse receives: [clippy-driver, /path/to/rustc, ...]
        let args: Vec<String> = vec![
            "clippy-driver",
            "/home/user/.rustup/toolchains/stable/bin/rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "--crate-type",
            "lib",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.rustc, PathBuf::from("clippy-driver"));
        assert_eq!(
            parsed.inner_rustc,
            Some(PathBuf::from(
                "/home/user/.rustup/toolchains/stable/bin/rustc"
            ))
        );
        assert_eq!(parsed.crate_name.as_deref(), Some("foo"));
        // inner rustc path should NOT appear in all_args
        assert!(!parsed.all_args.iter().any(|a| a.contains("rustc")));
        // inner rustc should NOT be picked up as the source file
        assert!(parsed.inner_rustc.is_some());
    }

    #[test]
    fn test_parse_single_wrapper_unchanged() {
        // Normal case: kache /path/to/rustc --crate-name foo src/lib.rs
        // After main.rs strips argv[0], parse receives: [/path/to/rustc, ...]
        let args: Vec<String> = vec![
            "/home/user/.rustup/toolchains/stable/bin/rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "--crate-type",
            "lib",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(
            parsed.rustc,
            PathBuf::from("/home/user/.rustup/toolchains/stable/bin/rustc")
        );
        assert!(parsed.inner_rustc.is_none());
        assert_eq!(parsed.crate_name.as_deref(), Some("foo"));
    }

    #[test]
    fn test_has_coverage_instrumentation_joined() {
        // -Cinstrument-coverage (joined form, used by tarpaulin via RUSTFLAGS)
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "-Cinstrument-coverage",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(parsed.has_coverage_instrumentation());
    }

    #[test]
    fn test_has_coverage_instrumentation_two_arg() {
        // -C instrument-coverage (two-arg form)
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "-C",
            "instrument-coverage",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(parsed.has_coverage_instrumentation());
    }

    #[test]
    fn test_no_coverage_instrumentation() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "-Copt-level=3",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert!(!parsed.has_coverage_instrumentation());
    }

    #[test]
    fn test_target_dir_from_out_dir() {
        // Lib/bin compiles: --out-dir is `<target>/<profile>/deps`.
        let args = RustcArgs {
            out_dir: Some(PathBuf::from("/work/proj/target/debug/deps")),
            ..Default::default()
        };
        assert_eq!(args.target_dir(), Some(PathBuf::from("/work/proj/target")));
    }

    #[test]
    fn test_target_dir_from_build_script_output() {
        // Build scripts: -o is
        // `<target>/<profile>/build/<pkg>/build_script_build-<hash>`.
        let args = RustcArgs {
            output: Some(PathBuf::from(
                "/work/proj/target/debug/build/serde-abc123/build_script_build-abc123",
            )),
            ..Default::default()
        };
        assert_eq!(args.target_dir(), Some(PathBuf::from("/work/proj/target")));
    }

    #[test]
    fn test_target_dir_prefers_out_dir_over_output() {
        // Cargo passes both --out-dir and -o for a lib/bin; --out-dir is
        // the reliable `<target>/<profile>/deps` shape, so it wins.
        let args = RustcArgs {
            out_dir: Some(PathBuf::from("/work/proj/target/release/deps")),
            output: Some(PathBuf::from(
                "/work/proj/target/release/deps/libfoo-abc.rlib",
            )),
            ..Default::default()
        };
        assert_eq!(args.target_dir(), Some(PathBuf::from("/work/proj/target")));
    }

    #[test]
    fn test_target_dir_returns_none_for_ad_hoc_rustc() {
        // An ad-hoc `rustc -o /tmp/prog` has no cargo layout to anchor
        // to — return None so dep-info rewriting is skipped.
        let args = RustcArgs {
            output: Some(PathBuf::from("/tmp/somewhere/myprog")),
            ..Default::default()
        };
        assert_eq!(args.target_dir(), None);
    }

    #[test]
    fn test_target_dir_none_when_no_paths() {
        assert_eq!(RustcArgs::default().target_dir(), None);
    }

    #[test]
    fn test_build_script_probe_detected_from_probe_out_dir() {
        let args: Vec<String> = vec![
            "rustc",
            "--edition=2021",
            "--crate-name=proc_macro2",
            "--crate-type=lib",
            "--emit=dep-info,metadata",
            "--out-dir",
            "/work/proj/target/release/build/proc-macro2-abc/out/probe",
            "src/probe/proc_macro_span.rs",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();

        assert!(parsed.is_build_script_probe(Some(Path::new(
            "/work/proj/target/release/build/proc-macro2-abc/out"
        ))));
    }

    #[test]
    fn test_build_script_probe_detected_from_source_in_out_dir() {
        let args: Vec<String> = vec![
            "rustc",
            "--edition=2018",
            "--crate-name=anyhow_build",
            "--crate-type=lib",
            "--emit=metadata",
            "--out-dir",
            "/work/proj/target/release/build/anyhow-abc/out",
            "/work/proj/target/release/build/anyhow-abc/out/probe.rs",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();

        assert!(parsed.is_build_script_probe(Some(Path::new(
            "/work/proj/target/release/build/anyhow-abc/out"
        ))));
    }

    #[test]
    fn test_normal_cargo_compile_is_not_build_script_probe() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name=foo",
            "--crate-type=lib",
            "--emit=dep-info,metadata,link",
            "--out-dir",
            "/work/proj/target/release/deps",
            "src/lib.rs",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();

        assert!(!parsed.is_build_script_probe(Some(Path::new(
            "/work/proj/target/release/build/foo-abc/out"
        ))));
    }

    #[test]
    fn test_features_are_sorted() {
        let args: Vec<String> = vec![
            "rustc",
            "--crate-name",
            "foo",
            "src/lib.rs",
            "--cfg",
            "feature=\"std\"",
            "--cfg",
            "feature=\"alloc\"",
            "--cfg",
            "feature=\"derive\"",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.features, vec!["alloc", "derive", "std"]);
    }
}
