//! CUDA `nvcc` compiler adapter.
//!
//! Phase 1 (kunobi-ninja/kache#1024): argv recognition, argument parsing,
//! and refuse-to-cache classification. There is deliberately no caching
//! yet — [`run_nvcc`](crate::wrapper::run_nvcc) parses every invocation,
//! reports *why* it is not cached, and passes through to the real `nvcc`.
//!
//! ## Why the parser is strict (fail-closed)
//!
//! An unrecognized flag must refuse, never assume cacheable. `nvcc` is a
//! *driver*: one `-c` line fans out to `cudafe++`, one `cicc` + `ptxas`
//! pair per GPU architecture, `fatbinary`, and the host C++ compiler.
//! A flag the parser does not model could change any of those stages'
//! outputs, so [`NvccArgs::refuse_reasons`] reports it as
//! [`RefuseReason::Unsupported`] until phase 2 classifies it into the key
//! (the same per-flag road the cc adapter walked).
//!
//! ## Why `-E` can never key (and `-M` can)
//!
//! `nvcc -E` always defines `__CUDA_ARCH__`, so host-only code guarded by
//! `#ifndef __CUDA_ARCH__` is invisible in preprocessed output — two
//! different sources could preprocess identically. Phase 2 therefore keys
//! raw source + header *contents* via the `nvcc -M` dependency closure,
//! never preprocessed output.

use anyhow::{Context, Result};
use std::path::PathBuf;

use super::{Compiler, CompilerAdapter, CompilerId, RefuseReason};

pub const NVCC_ID: CompilerId = CompilerId::new("nvcc");
pub const ADAPTER: CompilerAdapter =
    CompilerAdapter::new(NVCC_ID, "nvcc", NvccCompiler::recognizes);

/// What an `nvcc` invocation does. Only [`NvccMode::Compile`] is a future
/// cache candidate; every other mode refuses in [`NvccArgs::refuse_reasons`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NvccMode {
    /// `nvcc -c a.cu -o a.o`: single whole-invocation device compile.
    Compile,
    /// `nvcc -dlink`: device-link step combining relocatable objects.
    DeviceLink,
    /// No `-c`: host/device link producing an executable or shared object.
    Link,
    /// `nvcc --lib`: library archiving mode.
    Lib,
    /// `nvcc -E`: preprocessor output.
    Preprocess,
    /// Standalone `-ptx` / `-cubin` / `-fatbin` / `--optix-ir` emission.
    EmitPtx,
    EmitCubin,
    EmitFatbin,
    EmitOptixIr,
    /// Version / help / dry-run queries: informational, nothing to cache.
    Query,
}

/// Parsed `nvcc` invocation.
#[derive(Debug, Clone)]
pub struct NvccArgs {
    /// `argv[0]` as given (bare `nvcc`, path, `nvcc.exe`).
    pub program: String,
    /// `argv[1..]` verbatim, for byte-for-byte passthrough execution.
    pub rest: Vec<String>,
    /// What the invocation does.
    pub mode: NvccMode,
    /// Source files (`.cu` and accepted host extensions, see
    /// [`is_nvcc_source`]). Linker inputs (`.o`, `.a`, …) are *not*
    /// collected — link modes refuse before sources matter.
    pub sources: Vec<PathBuf>,
    /// `-o <file>`.
    pub output: Option<PathBuf>,
    /// `-dc` or `-rdc=true`: relocatable / separable device code.
    pub separate_device_code: bool,
    /// `-G`: device-debug info.
    pub device_debug: bool,
    /// `-keep` / `--save-temps`: intermediate files kept.
    pub keep_temps: bool,
    /// `@file`: response file (never expanded — refuse).
    pub response_file: bool,
    /// Flags the phase-1 table knows but phase 2 has not classified into
    /// the key yet (include dirs, defines, `-O`, `-gencode`, `-Xcompiler`,
    /// …). Known-but-unmodeled still refuses; the table exists so phase 2
    /// promotes entries instead of discovering flags from scratch.
    pub deferred_flags: Vec<String>,
    /// Flags matching nothing in the table. Always refuse; the
    /// user-declared allow-list ([`NvccCompiler::extra_allowlist_flags`])
    /// is the only escape hatch.
    pub unknown_flags: Vec<String>,
}

/// Source extensions `nvcc` accepts on a compile line. `.cu` is the CUDA
/// language; the host extensions let `nvcc` drive plain C/C++ files too
/// (common via `CUDACXX` wrappers covering a whole project).
fn is_nvcc_source(name: &str) -> bool {
    let ext = std::path::Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    matches!(
        ext,
        "cu" | "cuh" | "c" | "cc" | "cpp" | "cxx" | "C" | "h" | "hpp" | "hxx"
    )
}

impl NvccArgs {
    pub fn parse(args: &[String]) -> Result<Self> {
        let Some(program) = args.first().cloned() else {
            anyhow::bail!("nvcc: empty argv");
        };
        let rest = args[1..].to_vec();
        let mut parsed = NvccArgs {
            program,
            rest,
            mode: NvccMode::Link,
            sources: Vec::new(),
            output: None,
            separate_device_code: false,
            device_debug: false,
            keep_temps: false,
            response_file: false,
            deferred_flags: Vec::new(),
            unknown_flags: Vec::new(),
        };

        // Mode votes: emit/query modes win over `-c` when combined;
        // `--lib` / `-dlink` win over plain link. Last vote of the
        // highest-precedence class wins (matches driver behavior closely
        // enough for refuse classification — never for keying).
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum Vote {
            Link,
            Compile,
            Lib,
            DeviceLink,
            Emit,
            Preprocess,
            Query,
        }
        let mut vote = Vote::Link;
        let mut set_vote = |v: Vote, mode: NvccMode, parsed: &mut NvccArgs| {
            if v >= vote {
                vote = v;
                parsed.mode = mode;
            }
        };

        let argv = parsed.rest.clone();
        let mut i = 0;
        while i < argv.len() {
            let arg = argv[i].as_str();
            // Value-taking flags in `--flag value` form consume argv[i+1].
            // `take_value` bails on a missing value — a truncated line is a
            // parse error, and the wrapper surfaces the real compiler's
            // diagnostic via passthrough (fail-closed, never miskey).
            let take_value = |i: &mut usize, flag: &str| -> Result<String> {
                *i += 1;
                argv.get(*i)
                    .cloned()
                    .with_context(|| format!("nvcc: {flag} missing value"))
            };
            match arg {
                "-c" | "--compile" => set_vote(Vote::Compile, NvccMode::Compile, &mut parsed),
                "-dc" | "--device-c" => {
                    parsed.separate_device_code = true;
                    set_vote(Vote::Compile, NvccMode::Compile, &mut parsed);
                }
                "-dlink" | "--device-link" => {
                    set_vote(Vote::DeviceLink, NvccMode::DeviceLink, &mut parsed);
                }
                "--lib" | "-lib" => set_vote(Vote::Lib, NvccMode::Lib, &mut parsed),
                "-E" | "--preprocess" => {
                    set_vote(Vote::Preprocess, NvccMode::Preprocess, &mut parsed);
                }
                "-ptx" | "--ptx" => set_vote(Vote::Emit, NvccMode::EmitPtx, &mut parsed),
                "-cubin" | "--cubin" => {
                    set_vote(Vote::Emit, NvccMode::EmitCubin, &mut parsed);
                }
                "-fatbin" | "--fatbin" => {
                    set_vote(Vote::Emit, NvccMode::EmitFatbin, &mut parsed);
                }
                "--optix-ir" => set_vote(Vote::Emit, NvccMode::EmitOptixIr, &mut parsed),
                "-G" | "--device-debug" => parsed.device_debug = true,
                "-keep" | "--keep" | "--save-temps" | "-save-temps" => parsed.keep_temps = true,
                "--version" | "-V" | "--help" | "-h" | "--dryrun" => {
                    set_vote(Vote::Query, NvccMode::Query, &mut parsed);
                }
                "--time" => parsed.unknown_flags.push(arg.to_string()),
                "-o" => {
                    let value = take_value(&mut i, "-o")?;
                    parsed.output = Some(PathBuf::from(value));
                }
                _ if arg.starts_with("-o") && arg.len() > 2 && !arg.starts_with("-O") => {
                    parsed.output = Some(PathBuf::from(&arg[2..]));
                }
                _ if arg == "-rdc" => parsed.separate_device_code = true,
                _ if arg.starts_with("-rdc=") || arg.starts_with("--relocatable-device-code=") => {
                    let value = arg.rsplit('=').next().unwrap_or("");
                    parsed.separate_device_code = matches!(value, "true" | "1");
                }
                // Dependency-output flags: recorded so phase 2 can find the
                // `.d` sidecar; the `-M` *listing* mode still compiles, so
                // no mode vote.
                "-M" | "-MM" | "-MD" | "-MMD" | "-MG" | "-MP" | "--generate-dependencies" => {
                    parsed.deferred_flags.push(arg.to_string());
                }
                "-MF" | "-MT" | "-MQ" => {
                    let value = take_value(&mut i, arg)?;
                    parsed.deferred_flags.push(format!("{arg} {value}"));
                }
                _ if arg.starts_with("-MF") || arg.starts_with("-MT") || arg.starts_with("-MQ") => {
                    parsed.deferred_flags.push(arg.to_string());
                }
                // Verbatim-forwarded / codegen-defining flags: known,
                // keyed in phase 2, refused until then.
                _ if arg == "-Xcompiler"
                    || arg == "-Xlinker"
                    || arg == "-Xptxas"
                    || arg == "-Xnvlink"
                    || arg == "--compiler-options"
                    || arg == "-gencode"
                    || arg == "-arch"
                    || arg == "-code" =>
                {
                    let value = take_value(&mut i, arg)?;
                    parsed.deferred_flags.push(format!("{arg} {value}"));
                }
                _ if arg.starts_with("-Xcompiler")
                    || arg.starts_with("-Xlinker")
                    || arg.starts_with("-Xptxas")
                    || arg.starts_with("-Xnvlink")
                    || arg.starts_with("-gencode")
                    || arg.starts_with("-arch=")
                    || arg.starts_with("-code=")
                    || arg.starts_with("--gpu-architecture=")
                    || arg.starts_with("--gpu-code=") =>
                {
                    parsed.deferred_flags.push(arg.to_string());
                }
                _ if arg == "-D"
                    || arg == "-U"
                    || arg == "-I"
                    || arg == "-isystem"
                    || arg == "-iquote"
                    || arg == "-include"
                    || arg == "-O"
                    || arg == "-std"
                    || arg == "--std"
                    || arg == "-x"
                    || arg == "-L"
                    || arg == "-l" =>
                {
                    let value = take_value(&mut i, arg)?;
                    parsed.deferred_flags.push(format!("{arg} {value}"));
                }
                _ if arg.starts_with("-D")
                    || arg.starts_with("-U")
                    || arg.starts_with("-I")
                    || arg.starts_with("-O")
                    || arg.starts_with("-std=")
                    || arg.starts_with("--std=")
                    || arg.starts_with("-x")
                    || arg.starts_with("-g")
                    || arg.starts_with("-m")
                    || arg.starts_with("--expt-")
                    || arg.starts_with("-Xfatbin") =>
                {
                    parsed.deferred_flags.push(arg.to_string());
                }
                _ if arg == "-shared"
                    || arg == "--shared"
                    || arg == "-static"
                    || arg == "-v"
                    || arg == "--verbose"
                    || arg == "-w"
                    || arg == "-Wall"
                    || arg == "-W" =>
                {
                    parsed.deferred_flags.push(arg.to_string());
                }
                _ if arg.starts_with("--W") || arg.starts_with("-W") => {
                    parsed.deferred_flags.push(arg.to_string());
                }
                _ if arg.starts_with('@') => parsed.response_file = true,
                _ if arg.starts_with('-') => parsed.unknown_flags.push(arg.to_string()),
                _ => {
                    if is_nvcc_source(arg) {
                        parsed.sources.push(PathBuf::from(arg));
                    } else {
                        // Linker inputs (`.o`, `.a`, …) and anything else
                        // positional: not a compilable source line.
                        parsed.unknown_flags.push(arg.to_string());
                    }
                }
            }
            i += 1;
        }
        Ok(parsed)
    }

    /// Reasons this invocation must bypass the cache. Empty = cacheable
    /// (once phase 2 wires the store path — phase 1 passes through anyway).
    pub fn refuse_reasons(&self, extra_allowlist_flags: &[String]) -> Vec<RefuseReason> {
        match self.mode {
            NvccMode::Query => return vec![RefuseReason::NotPrimary],
            NvccMode::Preprocess => {
                return vec![RefuseReason::Unsupported(
                    "nvcc preprocessor mode (-E) — not yet supported",
                )];
            }
            NvccMode::EmitPtx => {
                return vec![RefuseReason::Unsupported(
                    "nvcc standalone -ptx emission — not yet supported",
                )];
            }
            NvccMode::EmitCubin => {
                return vec![RefuseReason::Unsupported(
                    "nvcc standalone -cubin emission — not yet supported",
                )];
            }
            NvccMode::EmitFatbin => {
                return vec![RefuseReason::Unsupported(
                    "nvcc standalone -fatbin emission — not yet supported",
                )];
            }
            NvccMode::EmitOptixIr => {
                return vec![RefuseReason::Unsupported(
                    "nvcc standalone --optix-ir emission — not yet supported",
                )];
            }
            NvccMode::Lib => {
                return vec![RefuseReason::Unsupported(
                    "nvcc library mode (--lib) — not yet supported",
                )];
            }
            NvccMode::DeviceLink => {
                return vec![RefuseReason::Unsupported(
                    "nvcc device-link (-dlink) mode — not yet supported",
                )];
            }
            NvccMode::Link => {
                return vec![RefuseReason::Unsupported(
                    "nvcc link mode — not yet supported",
                )];
            }
            NvccMode::Compile => {}
        }

        let mut reasons = Vec::new();
        if self.sources.len() != 1 {
            reasons.push(RefuseReason::Unsupported(
                "nvcc multi-source or source-less compile — not yet supported",
            ));
        }
        if self.separate_device_code {
            reasons.push(RefuseReason::Unsupported(
                "nvcc separable device code (-dc/-rdc) — not yet supported (kunobi-ninja/kache#1024)",
            ));
        }
        if self.device_debug {
            reasons.push(RefuseReason::Unsupported(
                "nvcc device debug (-G) — not yet supported",
            ));
        }
        if self.keep_temps {
            reasons.push(RefuseReason::Unsupported(
                "nvcc kept intermediates (-keep/--save-temps) — not yet supported",
            ));
        }
        if self.response_file {
            reasons.push(RefuseReason::Unsupported(
                "nvcc response file (@file) — not yet supported",
            ));
        }
        // Deferred = known but unkeyed (phase 2 promotes these); unknown =
        // unrecognized, fail-closed. The allow-list covers both: a user who
        // has audited a flag opts it in explicitly.
        let unmodeled: Vec<&String> = self
            .deferred_flags
            .iter()
            .chain(self.unknown_flags.iter())
            .filter(|f| !extra_allowlist_flags.iter().any(|a| flag_matches(a, f)))
            .collect();
        if !unmodeled.is_empty() {
            let detail = format!(
                "unrecognized nvcc flag(s) {} — not yet supported",
                unmodeled
                    .iter()
                    .map(|f| f.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            // `RefuseReason::Unsupported` carries `&'static str`; the
            // dynamic list is debug-logged by the caller instead. The
            // static detail names the class; `explain-miss` shows flags.
            tracing::debug!("nvcc unmodeled flags refused: {detail}");
            reasons.push(RefuseReason::Unsupported(
                "unrecognized nvcc flag(s) — not yet supported",
            ));
        }
        reasons
    }

    /// `-o` output, if any. Unused until the phase-2 store path reads it.
    #[allow(dead_code)] // used in tests; wired into run_nvcc in phase 2
    pub fn object_output_path(&self) -> Option<PathBuf> {
        self.output.clone()
    }
}

/// Allow-list entry matching: exact flag, or the flag's head before its
/// first value separator (`-O2` covered by `-O`, `-gencode arch=…` by
/// `-gencode`). Deliberately prefix-simple — auditing stays explicit.
fn flag_matches(allow: &str, flag: &str) -> bool {
    flag == allow
        || flag
            .split([' ', '='])
            .next()
            .is_some_and(|head| head == allow)
}

/// The `nvcc` compiler: recognition today, full [`Compiler`] in phase 2.
pub struct NvccCompiler {
    /// User-declared flags the built-in table doesn't model but the user
    /// opted into caching. Shared with the cc knob for now
    /// (`KACHE_CC_EXTRA_ALLOWLIST_FLAGS`); a dedicated `[nvcc]` knob is a
    /// follow-up once the flag set deserves its own namespace.
    extra_allowlist_flags: Vec<String>,
}

impl NvccCompiler {
    pub fn with_extra_allowlist_flags(extra_allowlist_flags: Vec<String>) -> Self {
        Self {
            extra_allowlist_flags,
        }
    }

    /// Does this argv invoke `nvcc`? Basename match, case-insensitive,
    /// `.exe`-tolerant: `nvcc`, `/usr/local/cuda/bin/nvcc`,
    /// `C:\CUDA\bin\nvcc.exe`.
    pub fn recognizes(args: &[String]) -> bool {
        let Some(program) = args.first() else {
            return false;
        };
        super::command_basename(program)
            .map(super::strip_windows_exe_suffix)
            .is_some_and(|name| name.eq_ignore_ascii_case("nvcc"))
    }
}

impl Compiler for NvccCompiler {
    type Parsed = NvccArgs;

    fn id(&self) -> CompilerId {
        NVCC_ID
    }

    fn parse(&self, args: &[String]) -> Result<NvccArgs> {
        NvccArgs::parse(args)
    }

    fn refuse_reasons(&self, parsed: &NvccArgs) -> Vec<RefuseReason> {
        parsed.refuse_reasons(&self.extra_allowlist_flags)
    }

    fn cache_key(&self, _parsed: &NvccArgs, _ctx: &super::KeyCtx<'_, '_>) -> Result<String> {
        anyhow::bail!("nvcc cache key not yet implemented (phase 2, kunobi-ninja/kache#1024)")
    }

    fn execute(&self, _parsed: &NvccArgs) -> Result<super::CompileResult> {
        anyhow::bail!("nvcc execution not yet implemented (phase 2, kunobi-ninja/kache#1024)")
    }

    fn classify_output(&self, _parsed: &NvccArgs, name: &str) -> super::ArtifactKind {
        super::classify_by_filename(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(args: &[&str]) -> Vec<String> {
        args.iter().map(|a| a.to_string()).collect()
    }

    fn parse_ok(args: &[&str]) -> NvccArgs {
        NvccArgs::parse(&s(args)).expect("parse must succeed")
    }

    #[test]
    fn recognizes_nvcc_spellings() {
        assert!(NvccCompiler::recognizes(&s(&["nvcc", "-c", "a.cu"])));
        assert!(NvccCompiler::recognizes(&s(&[
            "/usr/local/cuda/bin/nvcc",
            "-c",
            "a.cu"
        ])));
        assert!(NvccCompiler::recognizes(&s(&[
            r"C:\CUDA\bin\nvcc.exe",
            "-c",
            "kernel.cu"
        ])));
        assert!(NvccCompiler::recognizes(&s(&["NVCC", "--version"])));
        assert!(!NvccCompiler::recognizes(&s(&["cc", "-c", "a.c"])));
        assert!(!NvccCompiler::recognizes(&s(&["gcc", "-c", "a.c"])));
        assert!(!NvccCompiler::recognizes(&s(&[
            "nvcc-wrapper",
            "-c",
            "a.cu"
        ])));
        assert!(!NvccCompiler::recognizes(&[]));
    }

    #[test]
    fn parses_single_source_compile() {
        let parsed = parse_ok(&["nvcc", "-c", "src/kernel.cu", "-o", "build/kernel.o"]);
        assert_eq!(parsed.mode, NvccMode::Compile);
        assert_eq!(parsed.sources, vec![PathBuf::from("src/kernel.cu")]);
        assert_eq!(
            parsed.object_output_path(),
            Some(PathBuf::from("build/kernel.o"))
        );
        assert!(!parsed.separate_device_code);
        assert!(!parsed.device_debug);
    }

    #[test]
    fn parses_joined_output_and_arch_flags() {
        let parsed = parse_ok(&[
            "nvcc",
            "-c",
            "k.cu",
            "-obuild/k.o",
            "-gencode",
            "arch=compute_80,code=sm_80",
            "-Xcompiler",
            "-fPIC",
            "-DUSE_CUDA",
            "-O2",
        ]);
        assert_eq!(parsed.mode, NvccMode::Compile);
        assert_eq!(parsed.output, Some(PathBuf::from("build/k.o")));
        assert!(parsed.unknown_flags.is_empty());
        assert_eq!(parsed.deferred_flags.len(), 4);
    }

    #[test]
    fn query_is_not_primary() {
        for query in [["nvcc", "--version"], ["nvcc", "-V"], ["nvcc", "--dryrun"]] {
            let parsed = parse_ok(&query);
            assert_eq!(parsed.mode, NvccMode::Query);
            let reasons = parsed.refuse_reasons(&[]);
            assert_eq!(reasons.len(), 1);
            assert!(matches!(reasons[0], RefuseReason::NotPrimary));
        }
    }

    #[test]
    fn link_and_device_link_refuse() {
        let parsed = parse_ok(&["nvcc", "a.o", "b.o", "-o", "app"]);
        assert_eq!(parsed.mode, NvccMode::Link);
        let reasons = parsed.refuse_reasons(&[]);
        assert!(
            reasons
                .iter()
                .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("link mode")) })
        );

        let parsed = parse_ok(&["nvcc", "-dlink", "a.o", "-o", "dlink.o"]);
        assert_eq!(parsed.mode, NvccMode::DeviceLink);
        assert!(!parsed.refuse_reasons(&[]).is_empty());
    }

    #[test]
    fn separable_and_debug_refuse_with_named_reasons() {
        let parsed = parse_ok(&["nvcc", "-c", "-dc", "k.cu", "-o", "k.o"]);
        assert!(parsed.separate_device_code);
        let reasons = parsed.refuse_reasons(&[]);
        assert!(
            reasons
                .iter()
                .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("-dc")) })
        );

        let parsed = parse_ok(&["nvcc", "-c", "-rdc=true", "k.cu", "-o", "k.o"]);
        assert!(parsed.separate_device_code);

        let parsed = parse_ok(&["nvcc", "-c", "-rdc=false", "k.cu", "-o", "k.o"]);
        assert!(!parsed.separate_device_code);

        let parsed = parse_ok(&["nvcc", "-c", "-G", "k.cu", "-o", "k.o"]);
        assert!(parsed.device_debug);
        let reasons = parsed.refuse_reasons(&[]);
        assert!(
            reasons
                .iter()
                .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("-G")) })
        );
    }

    #[test]
    fn multi_source_refuses() {
        let parsed = parse_ok(&["nvcc", "-c", "a.cu", "b.cu", "-o", "out.o"]);
        assert!(
            parsed.refuse_reasons(&[]).iter().any(|r| {
                matches!(r, RefuseReason::Unsupported(d) if d.contains("multi-source"))
            })
        );
    }

    #[test]
    fn unknown_flags_refuse_unless_allowlisted() {
        let parsed = parse_ok(&["nvcc", "-c", "k.cu", "--fancy-new-flag", "-o", "k.o"]);
        assert_eq!(parsed.unknown_flags, vec!["--fancy-new-flag".to_string()]);
        assert!(!parsed.refuse_reasons(&[]).is_empty());
        // Allow-listed by head: caching proceeds past the flag check.
        let reasons = parsed.refuse_reasons(&["--fancy-new-flag".to_string()]);
        assert!(reasons.is_empty(), "unexpected refusals: {reasons:?}");
    }

    #[test]
    fn response_file_and_keep_refuse() {
        let parsed = parse_ok(&["nvcc", "-c", "@args.rsp", "-o", "k.o"]);
        assert!(parsed.response_file);

        let parsed = parse_ok(&["nvcc", "-c", "-keep", "k.cu", "-o", "k.o"]);
        assert!(parsed.keep_temps);
        assert!(!parsed.refuse_reasons(&[]).is_empty());
    }

    #[test]
    fn missing_value_is_parse_error() {
        assert!(NvccArgs::parse(&s(&["nvcc", "-c", "k.cu", "-o"])).is_err());
        assert!(NvccArgs::parse(&s(&["nvcc", "-c", "k.cu", "-gencode"])).is_err());
        assert!(NvccArgs::parse(&[]).is_err());
    }
}
