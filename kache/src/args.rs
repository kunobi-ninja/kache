use anyhow::{Result, bail};
use std::path::{Path, PathBuf};

/// Check if a string looks like a path to rustc (or clippy-driver, which wraps rustc).
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

/// Parsed rustc invocation arguments relevant to caching.
#[derive(Debug, Clone)]
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
    /// Inner rustc path for double-wrapper case (RUSTC_WRAPPER + RUSTC_WORKSPACE_WRAPPER).
    /// When both wrappers are active, cargo passes: wrapper workspace_wrapper rustc <args>.
    /// This field holds the rustc path that the workspace wrapper expects as its first arg.
    pub inner_rustc: Option<PathBuf>,
    /// All original arguments (everything after the rustc path)
    pub all_args: Vec<String>,
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
        let (inner_rustc, rustc_args) = if args.len() >= 3 && looks_like_rustc(&args[1]) {
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
            inner_rustc,
            all_args: rustc_args.to_vec(),
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
                // Positional argument: source file (doesn't start with -)
                _ if !arg.starts_with('-') && parsed.source_file.is_none() => {
                    // Could be a source file if it looks like a .rs file or path
                    if arg.ends_with(".rs") || std::path::Path::new(arg).exists() {
                        parsed.source_file = Some(PathBuf::from(arg));
                    }
                }
                _ => {}
            }
            i += 1;
        }

        parsed.features.sort();
        parsed.is_primary = parsed.crate_name.is_some() && parsed.source_file.is_some();

        Ok(parsed)
    }

    /// Whether this invocation produces a binary, dylib, or proc-macro.
    pub fn is_executable_output(&self) -> bool {
        self.crate_types
            .iter()
            .any(|t| matches!(t.as_str(), "bin" | "dylib" | "cdylib" | "proc-macro"))
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
