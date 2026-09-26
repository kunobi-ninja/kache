//! CUDA `nvcc` compiler adapter.
//!
//! Caches single-source object compiles, including relocatable device code.
//! Other invocation modes pass through to the real `nvcc`.
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
//! different sources could preprocess identically. The adapter keys
//! raw source + header *contents* via the `nvcc -M` dependency closure,
//! never preprocessed output.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use super::{Compiler, CompilerAdapter, CompilerId, RefuseReason};

pub const NVCC_ID: CompilerId = CompilerId::new("nvcc");
pub const ADAPTER: CompilerAdapter =
    CompilerAdapter::new(NVCC_ID, "nvcc", NvccCompiler::recognizes);

/// What an `nvcc` invocation does. Only [`NvccMode::Compile`] is cacheable;
/// every other mode refuses in [`NvccArgs::refuse_reasons`].
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
    /// `-MF <file>` / `-MF<file>`: explicit dep-info sidecar. Cached
    /// together with `-MD` / `-MMD`, which are the only generation modes
    /// v1 accepts: per the driver docs the `-MF` path is only honored
    /// with a generate-dependencies mode, and a bare `-MD` would leave
    /// the sidecar name to an unvalidated derivation rule.
    pub depfile: Option<PathBuf>,
    /// `-MD` / `-MMD`: generate dependencies *and* compile. Allowed only
    /// together with an explicit `-MF` path (see `depfile`); on its own
    /// the sidecar name is derived and v1 refuses to guess it.
    pub depgen_with_compile: bool,
    /// Bare `-M`/`-MM`/`-MG`/`-MP`/`--generate-dependencies`: always
    /// refused (see `depfile`). `-M`/`-MM` also skip the compile, so
    /// there is no object to cache.
    pub implicit_depfile: bool,
    /// `-dc` or `-rdc=true`: relocatable / separable device code.
    pub separate_device_code: bool,
    /// `-G`: device-debug info.
    pub device_debug: bool,
    /// `-keep` / `--save-temps`: intermediate files kept.
    pub keep_temps: bool,
    /// `@file`: response file (never expanded — refuse).
    pub response_file: bool,
    /// Modeled flags keyed verbatim into the cache key (see `cache_key`).
    pub deferred_flags: Vec<String>,
    /// Flags matching nothing in the table. Always refuse; the
    /// user-declared allow-list ([`NvccCompiler::extra_allowlist_flags`])
    /// is the only escape hatch.
    pub unknown_flags: Vec<String>,
    /// Structured `-Xcompiler` / `--compiler-options` values (the flag
    /// name stripped). The verbatim entry stays in `deferred_flags` for
    /// keying; this list exists so the preprocessor-smuggling check and
    /// phase-3 classification read values without re-splitting strings.
    pub xcompiler_values: Vec<String>,
}

/// Flags taking a separate value (`-D FOO`, `-gencode arch=..,code=..`).
/// Checked before [`JOINED_PREFIXES`], so `-MF` takes a value while
/// `-MFout.d` matches the joined form.
const VALUE_FLAGS: &[&str] = &[
    "-D",
    "-U",
    "-I",
    "-isystem",
    "-iquote",
    "-include",
    "-O",
    "-std",
    "--std",
    "-x",
    "-L",
    "-l",
    "-MT",
    "-MQ",
    "-Xlinker",
    "-Xptxas",
    "-Xnvlink",
    "-gencode",
    "-arch",
    "-code",
    "--gpu-architecture",
    "--gpu-code",
    "-march",
    "-mcpu",
    "-mtune",
];

/// Joined prefixes (`-O2`, `-DFOO`, `-arch=sm_80`, `-Werror`). Checked
/// after [`VALUE_FLAGS`].
const JOINED_PREFIXES: &[&str] = &[
    "-D",
    "-U",
    "-I",
    "-O",
    "-std=",
    "--std=",
    "-x",
    "-g",
    "-m",
    "--expt-",
    "-Xfatbin",
    "-MT",
    "-MQ",
    "-Xlinker",
    "-Xptxas",
    "-Xnvlink",
    "-gencode",
    "-arch=",
    "-code=",
    "--gpu-architecture=",
    "--gpu-code=",
    "--W",
    "-W",
];

/// Bare flags with no value (`-shared`, `-v`). Recorded deferred
/// like the tables above. Kept as data, not match arms, so extending
/// coverage is adding a string, not a branch.
const BARE_DEFERRED_FLAGS: &[&str] = &[
    "-shared",
    "--shared",
    "-static",
    "-v",
    "--verbose",
    "-w",
    "-Wall",
    "-W",
];

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

fn parse_rdc_value(value: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => anyhow::bail!("nvcc: invalid relocatable device code value {value:?}"),
    }
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
            depfile: None,
            depgen_with_compile: false,
            implicit_depfile: false,
            separate_device_code: false,
            device_debug: false,
            keep_temps: false,
            response_file: false,
            deferred_flags: Vec::new(),
            unknown_flags: Vec::new(),
            xcompiler_values: Vec::new(),
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

        // Iterator, not an index: there is no counter to mutate, so the
        // loop cannot hang on a bad increment — a truncated line ends the
        // iteration and `take_value` bails fail-closed (never miskeyed).
        let mut argv = parsed.rest.clone().into_iter();
        while let Some(arg) = argv.next() {
            let arg = arg.as_str();
            // Value-taking flags in `--flag value` form consume the next
            // item. A missing value bails: truncated line, parse error, and
            // the wrapper surfaces the real compiler's diagnostic via
            // passthrough.
            let take_value =
                |argv: &mut std::vec::IntoIter<String>, flag: &str| -> Result<String> {
                    argv.next()
                        .with_context(|| format!("nvcc: {flag} missing value"))
                };
            match arg {
                "-c" | "--compile" => set_vote(Vote::Compile, NvccMode::Compile, &mut parsed),
                "-dc" | "--device-c" => {
                    parsed.separate_device_code = true;
                    parsed.deferred_flags.push(arg.to_string());
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
                "-o" => {
                    let value = take_value(&mut argv, "-o")?;
                    parsed.output = Some(PathBuf::from(value));
                }
                // Joined `-oout.o`. (No length check: the exact `"-o"` arm
                // above already claims the bare spelling, so anything
                // reaching here carries a value.) `-O*` excluded —
                // optimization levels are deferred flags, not outputs.
                _ if arg.starts_with("-o") && !arg.starts_with("-O") => {
                    parsed.output = Some(PathBuf::from(&arg[2..]));
                }
                // Explicit dep-info sidecar: the path is unambiguous, so
                // phase 2 stores and restores it. (Also recorded deferred:
                // the flag itself is keyed verbatim like every table row.)
                "-MF" => {
                    let value = take_value(&mut argv, "-MF")?;
                    parsed.depfile = Some(PathBuf::from(&value));
                    parsed.deferred_flags.push(format!("-MF {value}"));
                }
                _ if arg.starts_with("-MF") => {
                    parsed.depfile = Some(PathBuf::from(&arg["-MF".len()..]));
                    parsed.deferred_flags.push(arg.to_string());
                }
                // Host-compiler forwarding, recorded twice: verbatim in
                // `deferred_flags` for keying, structured in
                // `xcompiler_values` for the preprocessor-smuggling check
                // (a `-Xcompiler -I…` would hide header inputs from the
                // `-M` closure).
                "-Xcompiler" | "--compiler-options" => {
                    let value = take_value(&mut argv, arg)?;
                    parsed.xcompiler_values.push(value.clone());
                    parsed.deferred_flags.push(format!("{arg} {value}"));
                }
                _ if arg.starts_with("-Xcompiler") || arg.starts_with("--compiler-options") => {
                    let flag = if arg.starts_with("-Xcompiler") {
                        "-Xcompiler"
                    } else {
                        "--compiler-options"
                    };
                    let mut value = &arg[flag.len()..];
                    value = value.strip_prefix('=').unwrap_or(value);
                    parsed.xcompiler_values.push(value.to_string());
                    parsed.deferred_flags.push(arg.to_string());
                }
                // Bare `-M` family without compile: nvcc emits no object
                // (and the sidecar name would be derived), so v1 refuses.
                // `-MD`/`-MMD` are handled below: they compile, and are
                // fine with an explicit `-MF` path.
                "-M" | "-MM" | "-MG" | "-MP" | "--generate-dependencies" => {
                    parsed.implicit_depfile = true;
                }
                // `-MD`/`-MMD` generate dependencies *and* compile. Keyed
                // verbatim (a different dep mode is a different build);
                // the refuse check below requires an explicit `-MF` path.
                "-MD" | "-MMD" => {
                    parsed.depgen_with_compile = true;
                    parsed.deferred_flags.push(arg.to_string());
                }
                "-rdc" | "--relocatable-device-code" => {
                    let value = take_value(&mut argv, arg)?;
                    parsed.separate_device_code = parse_rdc_value(&value)?;
                    parsed.deferred_flags.push(format!("{arg} {value}"));
                }
                _ if arg.starts_with("-rdc=") || arg.starts_with("--relocatable-device-code=") => {
                    let value = arg.rsplit('=').next().unwrap_or("");
                    parsed.separate_device_code = parse_rdc_value(value)?;
                    parsed.deferred_flags.push(arg.to_string());
                }
                // Known-but-unmodeled flags (see the tables): recorded so
                // phase 2 promotes entries instead of discovering flags
                // from scratch. Table lookup, not `||` chains, so adding
                // coverage is adding a string.
                _ if BARE_DEFERRED_FLAGS.contains(&arg) => {
                    parsed.deferred_flags.push(arg.to_string());
                }
                _ if VALUE_FLAGS.contains(&arg) => {
                    let value = take_value(&mut argv, arg)?;
                    parsed.deferred_flags.push(format!("{arg} {value}"));
                }
                _ if JOINED_PREFIXES.iter().any(|p| arg.starts_with(p)) => {
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
        if self.output.is_none() {
            reasons.push(RefuseReason::Unsupported(
                "nvcc default output naming (missing -o) — not yet supported",
            ));
        }
        if self.implicit_depfile {
            reasons.push(RefuseReason::Unsupported(
                "nvcc dependency-only mode (-M/-MM) — not yet supported",
            ));
        }
        if self.depgen_with_compile && self.depfile.is_none() {
            reasons.push(RefuseReason::Unsupported(
                "nvcc implicit depfile (-MD/-MMD without -MF) — pass -MF <file> (not yet supported)",
            ));
        }
        if nvcc_xcompiler_smuggles_pp(&self.xcompiler_values) {
            reasons.push(RefuseReason::Unsupported(
                "nvcc -Xcompiler hides preprocessor flags (-I/-D/…) from dependency tracking — not yet supported",
            ));
        }
        if nvcc_has_native_resolution(&self.deferred_flags, &self.unknown_flags) {
            reasons.push(RefuseReason::Unsupported(
                "nvcc host- or device-resolved value (native) — not yet supported",
            ));
        }
        // Deferred flags are keyed verbatim (see `cache_key`); only truly
        // unrecognized flags refuse. The allow-list covers those: a user
        // who has audited a flag opts it in explicitly.
        let unmatched: Vec<&String> = self
            .unknown_flags
            .iter()
            .filter(|f| !extra_allowlist_flags.iter().any(|a| flag_matches(a, f)))
            .collect();
        if !unmatched.is_empty() {
            let detail = format!(
                "unrecognized nvcc flag(s) {} — not yet supported",
                unmatched
                    .iter()
                    .map(|f| f.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            // `RefuseReason::Unsupported` carries `&'static str`; the
            // dynamic list is debug-logged by the caller instead. The
            // static detail names the class; `explain-miss` shows flags.
            tracing::debug!("nvcc unrecognized flags refused: {detail}");
            reasons.push(RefuseReason::Unsupported(
                "unrecognized nvcc flag(s) — not yet supported",
            ));
        }
        reasons
    }

    /// `-o` output, if any.
    pub fn object_output_path(&self) -> Option<PathBuf> {
        self.output.clone()
    }

    /// `-MF` dep-info sidecar, if requested.
    pub fn depinfo_output_path(&self) -> Option<PathBuf> {
        self.depfile.clone()
    }
}

/// Allow-list entry matching: the flag verbatim (`-DFOO` covers the
/// deferred entry `"-D FOO"` only when listed whole), or the entry head
/// before its first value separator (`-O` covers `-O 2`, `-gencode`
/// covers `-gencode arch=..`). Deliberately explicit — auditing stays a
/// conscious act, and the boundary form keeps `-O2` from smuggling in an
/// unrelated `-Ox` spelling.
fn flag_matches(allow: &str, flag: &str) -> bool {
    flag == allow || flag.starts_with(allow) && flag[allow.len()..].starts_with([' ', '='])
}

/// Flag substrings whose value the driver resolves per machine: host
/// `-march`/`-mcpu`/`-mtune`, CUDA `-arch`/`-gencode`/`-code`. Keying
/// them verbatim would share one key across different objects — a
/// stale hit, the fatal direction. This is a soundness veto, not a
/// modeling gap: it fires even on allow-listed flags. Substring match
/// so `-Xcompiler -march=native` (value riding inside the entry) and
/// `-gencode arch=native,…` are caught with the direct spellings; a
/// user `-DARCH=native` define matches nothing (no leading dash before
/// `arch`) and stays keyed, correctly.
const NVCC_NATIVE_MARKERS: &[&str] = &[
    "-march=native",
    "-mcpu=native",
    "-mtune=native",
    "-march native",
    "-mcpu native",
    "-mtune native",
    "-arch=native",
    "-arch native",
    "--gpu-architecture=native",
    "--gpu-architecture native",
    "-code=native",
    "--gpu-code=native",
    "arch=native",
];

/// Does any modeled or unmodeled flag carry a machine-resolved value?
fn nvcc_has_native_resolution(deferred: &[String], unknown: &[String]) -> bool {
    deferred
        .iter()
        .chain(unknown.iter())
        .any(|entry| NVCC_NATIVE_MARKERS.iter().any(|m| entry.contains(m)))
}

/// Preprocessor-affecting tokens: if one hides inside an `-Xcompiler`
/// value (or an `NVCC_PREPEND/APPEND_FLAGS` value), header inputs
/// escape the `-M` closure. Substring-conservative on purpose — an
/// over-refusal is a passthrough, an under-refusal is a stale hit.
const NVCC_PP_TOKENS: &[&str] = &["-I", "-D", "-U", "-isystem", "-iquote", "-include"];

/// Does one whitespace/comma-separated token smuggle a preprocessor
/// flag? Shared by the `-Xcompiler` check and the driver-env check so
/// the two cannot drift apart.
fn nvcc_pp_token(token: &str) -> bool {
    NVCC_PP_TOKENS
        .iter()
        .any(|f| token == *f || token.starts_with(f))
}

/// Does any `-Xcompiler` / `--compiler-options` value smuggle a
/// preprocessor flag past dependency tracking?
fn nvcc_xcompiler_smuggles_pp(values: &[String]) -> bool {
    values
        .iter()
        .any(|value| value.split([',', ' ', '\t']).any(nvcc_pp_token))
}

/// Mode-altering flags that must never arrive via `NVCC_PREPEND_FLAGS`
/// / `NVCC_APPEND_FLAGS`: the adapter keys and restores a `-c` object
/// compile, and these would silently change what is built. Exact
/// tokens plus `=`-joined relocatable-code spellings.
const NVCC_ENV_BLOCKED_EXACT: &[&str] = &[
    "-G",
    "--device-debug",
    "-dc",
    "--device-c",
    "-rdc",
    "-dlink",
    "--device-link",
    "--lib",
    "-ptx",
    "--ptx",
    "-cubin",
    "--cubin",
    "-fatbin",
    "--fatbin",
    "--optix-ir",
    "-E",
    "--preprocess",
    "-keep",
    "--save-temps",
    "--time",
];
const NVCC_ENV_BLOCKED_PREFIX: &[&str] = &["-rdc=", "--relocatable-device-code="];

/// Does a driver-env value alter the compilation mode (or smuggle
/// preprocessor inputs)? Pure and directly unit-tested; the
/// environment read lives in [`nvcc_driver_env_flags`].
fn nvcc_env_value_blocked(value: &str) -> bool {
    // Split like `-Xcompiler` values: comma-separated forwarding is
    // real there, and treating commas as separators here only ever
    // over-refuses (safe direction).
    value.split([',', ' ', '\t']).any(|token| {
        NVCC_ENV_BLOCKED_EXACT.contains(&token)
            || NVCC_ENV_BLOCKED_PREFIX.iter().any(|p| token.starts_with(p))
            || nvcc_pp_token(token)
    })
}

/// Validate one driver-env value: no mode alteration, no
/// machine-resolved `native`, both of which the key cannot see.
/// Called from [`nvcc_driver_env_flags`], so a violation bails the key
/// into passthrough instead of miscaching.
fn nvcc_check_env_value(var: &str, value: &str) -> Result<()> {
    if nvcc_env_value_blocked(value) {
        anyhow::bail!(
            "nvcc: {var} alters the compilation mode or hides preprocessor inputs; only codegen tuning is supported (not yet supported)"
        );
    }
    if NVCC_NATIVE_MARKERS.iter().any(|m| value.contains(m)) {
        anyhow::bail!("nvcc: {var} carries a machine-resolved (native) value — not yet supported");
    }
    Ok(())
}

/// Invisible driver inputs: `NVCC_PREPEND_FLAGS` /
/// `NVCC_APPEND_FLAGS` inject flags outside argv, so the key must see
/// them. Returns `(VAR, value)` pairs for key folding (verbatim — the
/// same build replays the same environment through the same process).
/// Empty/unset vars contribute nothing (byte-identical key).
/// Non-UTF-8 values bail: lossy matching could alias two different
/// environments under one key.
fn nvcc_driver_env_flags() -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    for var in ["NVCC_PREPEND_FLAGS", "NVCC_APPEND_FLAGS"] {
        let Some(os) = std::env::var_os(var) else {
            continue;
        };
        let value = os
            .into_string()
            .map_err(|_| anyhow::anyhow!("nvcc: {var} is not valid UTF-8"))?;
        if value.trim().is_empty() {
            continue;
        }
        nvcc_check_env_value(var, &value)?;
        out.push((var.to_string(), value));
    }
    Ok(out)
}

/// Deferred entries forwarded to `nvcc -M`: only flags that affect
/// header resolution. Everything else (`-gencode`, `-O`, `-Xcompiler`,
/// …) is meaningless to dependency listing and might error there —
/// never forward blindly. `-m*` rides along: multilib search paths
/// change which headers resolve, and `-M` accepts or rejects it
/// (a rejection is a passthrough, never a miskey).
const NVCC_DEP_FORWARD_FLAGS: &[&str] = &[
    "-I", "-isystem", "-iquote", "-D", "-U", "-std", "--std", "-x", "-include", "-m", "-O",
];

/// Split deferred entries back into `nvcc -M` arguments: separate form
/// (`-D FOO` → `-D`, `FOO`) and glued form (`-I/usr` as one word).
/// Non-forward flags are dropped, never erroring.
fn nvcc_dep_forward_args(deferred: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for entry in deferred {
        let mut words = entry.splitn(2, ' ');
        let head = words.next().unwrap_or("");
        if NVCC_DEP_FORWARD_FLAGS.contains(&head) {
            out.push(head.to_string());
            if let Some(value) = words.next() {
                out.push(value.to_string());
            }
            continue;
        }
        // Glued form: short flags take any tail (`-DUSE_CUDA`); long
        // flags only path/value tails, so `-includefoo` (some other
        // flag) is not mistaken for `-include`.
        if NVCC_DEP_FORWARD_FLAGS.iter().any(|f| {
            entry.starts_with(f) && (f.len() == 2 || entry[f.len()..].starts_with(['/', '=']))
        }) {
            out.push(entry.clone());
        }
    }
    out
}

/// Parse `nvcc -M` (make-style) output into dependency paths.
///
/// Fail-closed: any construct this parser does not confidently model
/// bails to passthrough. A dropped header would under-key the entry —
/// the fatal direction — so doubt means refusal, never a guess.
/// Conversely, unknown tokens never silently vanish: every parsed token
/// becomes a hashed input, so trailing garbage fails the hash (missing
/// file) instead of corrupting the key.
/// Byte offset where the make rule starts: skip leading lines that
/// cannot start one. Driver prologues (`nvcc warning : ...` on
/// stdout) contain a colon but are not rules; without this skip the
/// separator below would land inside the prologue and every word of
/// it would become a (missing, bailing) dependency — correct but
/// permanently uncached on toolkits that warn. A skipped-too-much
/// mistake is equally safe: no rule found means bail, never a guess.
fn leading_rule_offset(text: &str) -> usize {
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if line_has_rule_target(line) {
            break;
        }
        offset += line.len();
    }
    offset
}

/// Could `line` start a make rule? The text before the first colon
/// must be whitespace-free once backslash escapes are removed, so
/// `kernel.o :`, `C:\x:` and `my\ dir/f.o:` qualify while `nvcc
/// warning : ...` does not. Over-accepting is safe (unknown tokens
/// fail the hash); under-accepting just bails.
fn line_has_rule_target(line: &str) -> bool {
    let mut unescaped = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            unescaped.push(c);
        } else {
            chars.next();
        }
    }
    match unescaped.find(':') {
        Some(colon) => {
            let target = unescaped[..colon].trim();
            !target.is_empty() && !target.contains([' ', '\t'])
        }
        None => false,
    }
}

fn parse_nvcc_make_deps(text: &str) -> Result<Vec<PathBuf>> {
    // Skip leading non-rule lines. Driver prologues (`nvcc warning :
    // ...` on stdout) contain a colon but cannot start a make rule: a
    // rule target has no unescaped whitespace before its colon (see
    // `rule_separator_colon`). Without this, the separator below would
    // land inside the prologue and every word of it would become a
    // (missing, bailing) dependency — correct but permanently uncached
    // on toolkits that warn.
    let text = &text[leading_rule_offset(text)..];
    // Escaped spaces must survive the whitespace split below, so they
    // become a private-use placeholder first (restored after splitting).
    // Only two backslash sequences are escapes: a line continuation
    // (`\` + newline) and an escaped space. Anything else — notably
    // Windows separators (`C:\temp\x.h`) — is a literal backslash:
    // misreading those as escapes would corrupt every Windows depfile.
    const ESCAPED_SPACE: char = '\u{E000}';
    // Join backslash-newline continuations first.
    let mut joined = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\n') => continue,
                Some(' ') => joined.push(ESCAPED_SPACE),
                Some(other) => {
                    joined.push('\\');
                    joined.push(other);
                }
                None => joined.push('\\'),
            }
        } else {
            joined.push(c);
        }
    }
    // Split target from deps at the first colon that is not a Windows
    // drive-letter colon (`C:\…` / `C:/…`). No manual index arithmetic:
    // an off-by-sign mutation here would loop forever instead of
    // failing loudly.
    let separator = joined
        .match_indices(':')
        .map(|(index, _)| index)
        .find(|&index| {
            !(index == 1
                && joined
                    .as_bytes()
                    .first()
                    .is_some_and(|b| b.is_ascii_alphabetic()))
        })
        .with_context(|| "nvcc -M output has no target separator")?;
    let deps = joined[separator + 1..]
        .split_whitespace()
        .map(|dep| PathBuf::from(dep.replace(ESCAPED_SPACE, " ")))
        .collect();
    Ok(deps)
}

/// Run `nvcc -M` over the source with the header-resolution flags and
/// return the dependency paths (headers only — the caller keys the
/// source itself). Any failure (missing binary, bad exit, unparseable
/// output) bails: the compile then runs uncached via passthrough.
pub(crate) fn nvcc_dependency_closure(parsed: &NvccArgs) -> Result<Vec<PathBuf>> {
    let cmd_args = nvcc_dependency_query_args(parsed)?;
    let output = std::process::Command::new(&parsed.program)
        .env("LC_ALL", "C")
        .args(&cmd_args)
        .output()
        .with_context(|| format!("running `{} -M`", parsed.program))?;
    if !output.status.success() {
        anyhow::bail!("`{} -M` exited {}", parsed.program, output.status);
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let cwd = std::env::current_dir().context("nvcc -M: no current directory")?;
    let mut deps: Vec<PathBuf> = parse_nvcc_make_deps(&text)?
        .into_iter()
        .map(|dep| {
            if dep.is_absolute() {
                dep
            } else {
                cwd.join(dep)
            }
        })
        .collect();
    deps.sort();
    deps.dedup();
    Ok(deps)
}

fn nvcc_dependency_query_args(parsed: &NvccArgs) -> Result<Vec<String>> {
    let source = parsed
        .sources
        .first()
        .context("nvcc -M with no source file")?;
    let mut args = vec!["-M".to_string(), source.to_string_lossy().into_owned()];
    args.extend(nvcc_dep_forward_args(&parsed.deferred_flags));
    // nvcc defines __CUDACC_RDC__ only in relocatable mode. A header
    // included under that macro must appear in the -M dependency closure.
    if parsed.separate_device_code {
        args.push("-rdc=true".to_string());
    }
    Ok(args)
}

/// A machine-local path prefix and the portable sentinel it maps to.
/// Same shape as cc's prefix maps, nvcc's own sentinel namespace (keys
/// never cross compiler families).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NvccPrefixMap {
    pub from: String,
    pub to: String,
}

pub(crate) const NVCC_ROOT_SENTINEL: &str = "/kache/nvcc-root";

/// `KACHE_NVCC_PATH_NORMALIZE=0` disables nvcc path normalization
/// entirely: no maps → path-literal keys (no cross-machine sharing,
/// zero normalization miscache risk) and no `-ffile-prefix-map`
/// injection. Default on.
fn nvcc_path_normalize_enabled() -> bool {
    parse_nvcc_normalize_toggle(std::env::var("KACHE_NVCC_PATH_NORMALIZE").ok().as_deref())
}

/// Pure toggle behind [`nvcc_path_normalize_enabled`] — unit-testable
/// without touching the process environment.
fn parse_nvcc_normalize_toggle(value: Option<&str>) -> bool {
    value.map(|v| v != "0").unwrap_or(true)
}

fn nvcc_absolutize(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn nvcc_canonicalize_or_self(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Prefix maps for one invocation: configured `[paths].base_dirs`
/// roots (explicit wins ties) plus derived roots (cwd, source and
/// output parents, absolute and canonicalized), longest first so the
/// first prefix hit wins in [`nvcc_normalize_path`].
pub(crate) fn nvcc_prefix_maps(
    source: &Path,
    output: Option<&Path>,
    configured_base_dirs: &[String],
) -> Vec<NvccPrefixMap> {
    let mut maps: Vec<NvccPrefixMap> = Vec::new();
    if !nvcc_path_normalize_enabled() {
        return maps;
    }
    let Ok(cwd) = std::env::current_dir() else {
        return maps;
    };
    for (from, to) in crate::path_normalizer::configured_base_dir_prefix_maps(configured_base_dirs)
    {
        nvcc_push_unique_map(&mut maps, from, to);
    }
    let mut roots = vec![cwd.clone()];
    if let Some(parent) = nvcc_absolutize(source, &cwd).parent() {
        roots.push(parent.to_path_buf());
    }
    if let Some(output) = output
        && let Some(parent) = nvcc_absolutize(output, &cwd).parent()
    {
        roots.push(parent.to_path_buf());
    }
    for root in roots {
        for candidate in [root.clone(), nvcc_canonicalize_or_self(&root)] {
            nvcc_push_unique_map(
                &mut maps,
                candidate.to_string_lossy().into_owned(),
                NVCC_ROOT_SENTINEL.to_string(),
            );
        }
    }
    maps.sort_by(|a, b| {
        b.from
            .len()
            .cmp(&a.from.len())
            .then_with(|| a.from.cmp(&b.from))
    });
    maps
}

fn nvcc_push_unique_map(maps: &mut Vec<NvccPrefixMap>, from: String, to: String) {
    if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
        maps.push(NvccPrefixMap { from, to });
    }
}

/// Replace a machine-local prefix with its sentinel. The match must end
/// on a path separator (or the whole string): without the boundary,
/// `/a/proj` would also rewrite `/a/proj2` into nonsense.
pub(crate) fn nvcc_normalize_path(path: &str, maps: &[NvccPrefixMap]) -> String {
    for map in maps {
        if let Some(rest) = path.strip_prefix(map.from.as_str())
            && (rest.is_empty() || rest.starts_with('/') || rest.starts_with('\\'))
        {
            return format!("{}{}", map.to, rest);
        }
    }
    path.to_string()
}

/// `-Xcompiler -ffile-prefix-map=<from>=<to>` pairs (appended last so
/// they win over user maps). A side carrying a comma, space or quote
/// cannot survive nvcc's `-Xcompiler` splitting — bail instead of
/// forwarding a corrupted flag.
pub(crate) fn nvcc_prefix_map_args(maps: &[NvccPrefixMap]) -> Result<Vec<String>> {
    let mut args = Vec::new();
    for map in maps {
        if map.from.is_empty()
            || map.to.is_empty()
            || map.from.contains([',', ' ', '\t', '"', '\''])
            || map.to.contains([',', ' ', '\t', '"', '\''])
        {
            anyhow::bail!("nvcc: prefix map {:?} is not -Xcompiler-safe", map.from);
        }
        args.push("-Xcompiler".to_string());
        args.push(format!("-ffile-prefix-map={}={}", map.from, map.to));
    }
    Ok(args)
}

/// Effective `SOURCE_DATE_EPOCH` for the cache-miss execution: the
/// build's own value honored verbatim, else kache's `0` pin (stable
/// `__DATE__`/`__TIME__` across rebuilds), unless opted out via
/// `KACHE_NVCC_SOURCE_DATE_EPOCH=passthrough|wallclock|off`.
fn nvcc_effective_source_date_epoch() -> Option<std::ffi::OsString> {
    super::cc::resolve_source_date_epoch(
        std::env::var_os("SOURCE_DATE_EPOCH"),
        nvcc_source_date_epoch_passthrough(),
    )
}

fn nvcc_source_date_epoch_passthrough() -> bool {
    std::env::var("KACHE_NVCC_SOURCE_DATE_EPOCH")
        .ok()
        .map(|v| nvcc_epoch_opt_out(&v))
        .unwrap_or(false)
}

/// Pure opt-out predicate behind [`nvcc_source_date_epoch_passthrough`].
fn nvcc_epoch_opt_out(value: &str) -> bool {
    let v = value.trim().to_ascii_lowercase();
    v == "passthrough" || v == "wallclock" || v == "off"
}

/// Does the object at `path` embed a raw mapped root (a prefix map the
/// compiler did not honor)? Byte scan — the #1004-class safety net:
/// only a checkout-bound key may hold an object that names its
/// checkout, so a hit here drops the artifacts instead of storing.
fn nvcc_object_embeds_mapped_root(path: &Path, maps: &[NvccPrefixMap]) -> std::io::Result<bool> {
    let bytes = std::fs::read(path)?;
    Ok(maps
        .iter()
        .map(|map| map.from.as_bytes())
        .filter(|from| !from.is_empty())
        .any(|from| {
            bytes
                .iter()
                .enumerate()
                .filter(|(_, byte)| **byte == from[0])
                .any(|(start, _)| bytes[start..].starts_with(from))
        }))
}

/// Store-metadata target label: the requested GPU architectures, or
/// `generic` when none is named. Informational only (never keyed).
pub(crate) fn nvcc_target_label(deferred: &[String]) -> String {
    let mut archs: Vec<&str> = deferred
        .iter()
        .filter(|f| {
            f.starts_with("-arch")
                || f.starts_with("-gencode")
                || f.starts_with("--gpu-architecture")
                || f.starts_with("-code")
                || f.starts_with("--gpu-code")
        })
        .map(String::as_str)
        .collect();
    archs.sort_unstable();
    archs.dedup();
    if archs.is_empty() {
        "generic".to_string()
    } else {
        archs.join("+")
    }
}

/// The `nvcc` compiler.
pub struct NvccCompiler {
    /// User-declared flags the built-in table doesn't model but the user
    /// opted into caching. Shared with the cc knob for now
    /// (`KACHE_CC_EXTRA_ALLOWLIST_FLAGS`); a dedicated `[nvcc]` knob is a
    /// follow-up once the flag set deserves its own namespace.
    extra_allowlist_flags: Vec<String>,
    /// Deterministically ordered `[paths].base_dirs` roots applied to both
    /// the nvcc key probes and the real compiler invocation.
    base_dirs: Vec<String>,
}

impl NvccCompiler {
    pub fn with_extra_allowlist_flags(extra_allowlist_flags: Vec<String>) -> Self {
        Self {
            extra_allowlist_flags,
            base_dirs: Vec::new(),
        }
    }

    pub fn with_base_dirs(mut self, base_dirs: Vec<String>) -> Self {
        self.base_dirs = base_dirs;
        self.base_dirs.sort();
        self.base_dirs.dedup();
        self
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

    fn cache_key(&self, parsed: &NvccArgs, ctx: &super::KeyCtx<'_, '_>) -> Result<String> {
        // Preconditions (the wrapper checks refuse_reasons first): `-c`
        // mode, exactly one source, explicit `-o`, `-MF`-only dep-info.
        let source = parsed
            .sources
            .first()
            .context("nvcc cache key with no source file")?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nvcc_key_version:");
        hasher.update(crate::cache_key::CACHE_KEY_VERSION.to_string().as_bytes());
        hasher.update(b"\n");

        // Prefix-map sentinels (the TO set only — the FROM paths are
        // machine-local). Same checkout at another path normalizes
        // identically; a different map set keys apart.
        let maps = nvcc_prefix_maps(source, parsed.output.as_deref(), &self.base_dirs);
        // Validated here (not just at execute): an unforwardable map
        // means unportable objects, which must never be keyed shared.
        nvcc_prefix_map_args(&maps)?;
        let mut sentinels: Vec<&str> = maps.iter().map(|m| m.to.as_str()).collect();
        sentinels.sort_unstable();
        sentinels.dedup();
        hasher.update(b"prefix_maps:");
        for sentinel in sentinels {
            hasher.update(sentinel.as_bytes());
            hasher.update(b"\x1f");
        }
        hasher.update(b"\n");

        // Compiler identity: driver name + both probed versions. The
        // probe record is memoized per flag set; every other TU of the
        // build reads it instead of reforking.
        let program_name = std::path::Path::new(&parsed.program)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(parsed.program.as_str());
        hasher.update(b"compiler:");
        hasher.update(program_name.as_bytes());
        hasher.update(b"\n");
        let mut config_args = vec!["-c".to_string()];
        config_args.extend(parsed.deferred_flags.iter().cloned());
        // Invisible driver inputs join the probe sharing key: a `-ccbin`
        // smuggled through the environment changes the discovered host,
        // so environments must not share probe records. Validated here
        // (mode-altering or native values bail into passthrough below).
        let env_flags = nvcc_driver_env_flags()?;
        config_args.extend(
            env_flags
                .iter()
                .map(|(var, value)| format!("{var}={value}")),
        );
        let resolved = crate::probe::probe(
            ctx.cache_dir,
            &crate::probe::NvccProber,
            &crate::probe::ProbeRequest {
                compiler: &parsed.program,
                args: &parsed.rest,
                key_args: &config_args,
                // Unused by NvccProber (no -### tokens are hashed — see
                // the prober docs), filled for API honesty.
                per_tu_paths: &[],
                // nvcc objects keep raw paths unless our prefix maps
                // rewrite them, and MSVC-host objects keep them always:
                // never sentinel Windows paths in shared records.
                windows_aware: false,
            },
        )?;
        hasher.update(b"prober:nvcc\n");
        hasher.update(b"compiler_version:");
        hasher.update(resolved.version_line.as_bytes());
        hasher.update(b"\n");
        hasher.update(b"host_version:");
        hasher.update(
            resolved
                .host_version_line
                .as_deref()
                .context("nvcc probe has no host version")?
                .as_bytes(),
        );
        hasher.update(b"\n");

        // Modeled flags, verbatim and in invocation order (`-I` order is
        // search order — significant). Plus allow-listed unknowns, same
        // verbatim fold as cc: opting in is keying. Plus the validated
        // driver-env flags (same strings the probe sharing key saw).
        hasher.update(b"flags:");
        for flag in parsed
            .deferred_flags
            .iter()
            .chain(parsed.unknown_flags.iter().filter(|f| {
                self.extra_allowlist_flags
                    .iter()
                    .any(|a| flag_matches(a, f))
            }))
        {
            hasher.update(flag.as_bytes());
            hasher.update(b"\x1f");
        }
        for (var, value) in &env_flags {
            hasher.update(b"env:");
            hasher.update(var.as_bytes());
            hasher.update(b"=");
            hasher.update(value.as_bytes());
            hasher.update(b"\n");
        }
        hasher.update(b"\n");

        // Dependency closure: raw contents, never preprocessed output
        // (`-E` is blind to `#ifndef __CUDA_ARCH__` host-only code).
        // Paths are sentinel-normalized; contents are hashed raw. A
        // header the `-M` listing misses is a stale hit — the closure
        // bails on any listing or hashing failure instead.
        let closure = nvcc_dependency_closure(parsed)?;
        hasher.update(b"inputs:");
        let mut inputs: Vec<(&Path, String)> = Vec::with_capacity(closure.len() + 1);
        inputs.push((source, ctx.file_hasher.hash(source)?));
        for dep in &closure {
            inputs.push((dep, ctx.file_hasher.hash(dep)?));
        }
        inputs.sort_by(|a, b| a.0.cmp(b.0));
        for (path, content_hash) in &inputs {
            let display = nvcc_normalize_path(&path.to_string_lossy(), &maps);
            hasher.update(display.as_bytes());
            hasher.update(b"=");
            hasher.update(content_hash.as_bytes());
            hasher.update(b"\x1f");
        }
        hasher.update(b"\n");

        let key = hasher.finalize().to_hex().to_string();
        let key = crate::cache_key::apply_key_salt(key, ctx.key_salt, "nvcc");
        Ok(crate::cache_key::apply_key_env_vars(
            key,
            ctx.key_env_vars,
            "nvcc",
        ))
    }

    fn execute(&self, parsed: &NvccArgs) -> Result<super::CompileResult> {
        // The original argv plus `-ffile-prefix-map` rules so the object
        // does not embed clone-local roots. Appended last so they win
        // over user maps for the same prefix (same stance as cc).
        crate::opcounts::record_compiler_run();
        let source = parsed
            .sources
            .first()
            .context("nvcc execute with no source file")?;
        let maps = nvcc_prefix_maps(source, parsed.output.as_deref(), &self.base_dirs);
        let injection = nvcc_prefix_map_args(&maps)?;
        let mut command = std::process::Command::new(&parsed.program);
        command.args(&parsed.rest);
        command.args(&injection);
        // Pin the effective SOURCE_DATE_EPOCH so `__DATE__`/`__TIME__`
        // bake stably (mirrors the cc stance; opt out per knob docs).
        if let Some(epoch) = nvcc_effective_source_date_epoch() {
            command.env("SOURCE_DATE_EPOCH", epoch);
        }
        let output = command
            .output()
            .with_context(|| format!("executing {}", parsed.program))?;
        let exit_code = output.status.code().unwrap_or(1);

        // Output discovery: on success the named object is the artifact,
        // plus the `-MF` dep-info sidecar when requested. Anything else
        // (failure, or a mode that never reaches here) stores nothing.
        let mut artifacts = super::ArtifactSet::empty();
        if exit_code == 0 {
            // The named object gates everything: a successful compile
            // that left no object behind is not a cacheable state (and
            // a dep-info without its object is useless).
            let object = parsed.object_output_path().filter(|p| p.is_file());
            if let Some(object) = object {
                let store_name = object
                    .file_name()
                    .and_then(|n| n.to_str())
                    .context("nvcc object has no file name")?
                    .to_string();
                let mut found = vec![super::Artifact {
                    path: object.clone(),
                    store_name,
                    kind: super::ArtifactKind::Object,
                    required: true,
                }];
                if let Some(depfile) = parsed.depinfo_output_path()
                    && depfile.is_file()
                {
                    // `-MF` permits arbitrary suffixes; restore takes the
                    // real destination from the current invocation (same
                    // contract as cc, kunobi-ninja/kache#655).
                    let store_name = depfile
                        .file_name()
                        .and_then(|n| n.to_str())
                        .context("nvcc dep-info has no file name")?
                        .to_string();
                    found.push(super::Artifact {
                        path: depfile,
                        store_name,
                        kind: super::ArtifactKind::DepInfo,
                        required: true,
                    });
                }
                artifacts = super::ArtifactSet::new(found);
            }
            // #1004-class safety net: a mapped root that reaches the
            // object raw must not be stored under a shared key. Keep the
            // object for this build, store nothing.
            if !artifacts.is_empty()
                && let Some(object) = parsed.object_output_path()
                && nvcc_object_embeds_mapped_root(&object, &maps).unwrap_or(true)
            {
                tracing::warn!("nvcc: object embeds a raw mapped root; not caching it");
                artifacts = super::ArtifactSet::empty();
            }
        }

        Ok(super::CompileResult {
            exit_code,
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            pending_stderr: None,
            artifacts,
            keepalive: Vec::new(),
        })
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
    fn separable_modes_are_cacheable_and_keyed() {
        let parsed = parse_ok(&["nvcc", "-c", "-dc", "k.cu", "-o", "k.o"]);
        assert!(parsed.separate_device_code);
        assert!(parsed.refuse_reasons(&[]).is_empty());
        assert_eq!(parsed.deferred_flags, ["-dc"]);

        let parsed = parse_ok(&["nvcc", "-c", "-rdc=true", "k.cu", "-o", "k.o"]);
        assert!(parsed.separate_device_code);
        assert!(parsed.refuse_reasons(&[]).is_empty());
        assert_eq!(parsed.deferred_flags, ["-rdc=true"]);

        let parsed = parse_ok(&["nvcc", "-c", "-rdc=false", "k.cu", "-o", "k.o"]);
        assert!(!parsed.separate_device_code);
        assert!(parsed.refuse_reasons(&[]).is_empty());
        assert_eq!(parsed.deferred_flags, ["-rdc=false"]);

        // The long spelling feeds the same flag.
        let parsed = parse_ok(&[
            "nvcc",
            "-c",
            "--relocatable-device-code=true",
            "k.cu",
            "-o",
            "k.o",
        ]);
        assert!(parsed.separate_device_code);
        assert!(parsed.refuse_reasons(&[]).is_empty());
        assert_eq!(parsed.deferred_flags, ["--relocatable-device-code=true"]);

        let parsed = parse_ok(&["nvcc", "--device-c", "k.cu", "-o", "k.o"]);
        assert!(parsed.separate_device_code);
        assert!(parsed.refuse_reasons(&[]).is_empty());
        assert_eq!(parsed.deferred_flags, ["--device-c"]);
    }

    #[test]
    fn device_debug_still_refuses() {
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
        // Allow-listed verbatim: caching proceeds past the flag check.
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

    /// Every non-compile mode pins its refusal: deleting a mode arm must
    /// change the mode (caught here), not just shuffle the reason.
    #[test]
    fn every_emit_mode_names_its_refusal() {
        let cases: &[(&[&str], NvccMode, &str)] = &[
            (
                &["nvcc", "-E", "k.cu"],
                NvccMode::Preprocess,
                "preprocessor",
            ),
            (
                &["nvcc", "--lib", "a.o", "-o", "lib.a"],
                NvccMode::Lib,
                "--lib",
            ),
            (&["nvcc", "-ptx", "k.cu"], NvccMode::EmitPtx, "-ptx"),
            (&["nvcc", "-cubin", "k.cu"], NvccMode::EmitCubin, "-cubin"),
            (
                &["nvcc", "-fatbin", "k.cu"],
                NvccMode::EmitFatbin,
                "-fatbin",
            ),
            (
                &["nvcc", "--optix-ir", "k.cu"],
                NvccMode::EmitOptixIr,
                "--optix-ir",
            ),
        ];
        for (argv, mode, reason_part) in cases {
            let parsed = parse_ok(argv);
            assert_eq!(parsed.mode, *mode, "wrong mode for {argv:?}");
            assert!(
                parsed.refuse_reasons(&[]).iter().any(|r| {
                    matches!(r, RefuseReason::Unsupported(d) if d.contains(reason_part))
                }),
                "missing {reason_part:?} refusal for {argv:?}"
            );
        }
    }

    /// Each flag table contributes: a bare flag, a separate-value flag,
    /// and joined forms must all land deferred (never unknown).
    #[test]
    fn every_flag_table_lands_deferred() {
        let parsed = parse_ok(&[
            "nvcc",
            "-c",
            "k.cu",
            "-o",
            "k.o",
            "-MF",
            "deps.d",
            "-MFdeps2.d",
            "-shared",
            "-v",
            "-D",
            "FOO=1",
            "-I/usr/include",
            "-arch=sm_80",
            "--gpu-architecture=compute_80",
            "-Werror",
            "-m64",
        ]);
        assert_eq!(parsed.mode, NvccMode::Compile);
        assert!(
            parsed.unknown_flags.is_empty(),
            "unexpected unknowns: {:?}",
            parsed.unknown_flags
        );
        for expected in [
            "-MF deps.d",
            "-MFdeps2.d",
            "-shared",
            "-v",
            "-D FOO=1",
            "-I/usr/include",
            "-arch=sm_80",
            "--gpu-architecture=compute_80",
            "-Werror",
            "-m64",
        ] {
            assert!(
                parsed.deferred_flags.iter().any(|f| f == expected),
                "missing deferred {expected:?} in {:?}",
                parsed.deferred_flags
            );
        }
        assert_eq!(
            parsed.depinfo_output_path(),
            Some(PathBuf::from("deps2.d")),
            "last -MF wins, like the driver"
        );
    }

    #[test]
    fn bare_depinfo_flags_set_implicit_depfile() {
        for flag in ["-M", "-MM", "-MG", "-MP", "--generate-dependencies"] {
            let parsed = parse_ok(&["nvcc", "-c", "k.cu", "-o", "k.o", flag]);
            assert!(parsed.implicit_depfile, "{flag} must set implicit_depfile");
            assert!(
                parsed.refuse_reasons(&[]).iter().any(|r| {
                    matches!(r, RefuseReason::Unsupported(d) if d.contains("dependency-only"))
                }),
                "{flag} must refuse with the depfile reason"
            );
        }
    }

    #[test]
    fn depgen_with_compile_needs_explicit_mf() {
        // `-MD -MF k.d` / `-MMD -MF k.d` is the supported depinfo shape:
        // generation mode keyed verbatim, sidecar path explicit.
        for flag in ["-MD", "-MMD"] {
            let parsed = parse_ok(&["nvcc", "-c", "k.cu", "-o", "k.o", flag, "-MF", "k.d"]);
            assert!(
                parsed.depgen_with_compile,
                "{flag} must set depgen_with_compile"
            );
            assert!(!parsed.implicit_depfile);
            assert_eq!(parsed.depinfo_output_path(), Some(PathBuf::from("k.d")));
            assert!(
                parsed.deferred_flags.iter().any(|f| f == flag),
                "{flag} must be keyed, got {:?}",
                parsed.deferred_flags
            );
            assert!(
                parsed.refuse_reasons(&[]).is_empty(),
                "{flag} with -MF must be cacheable, got {:?}",
                parsed.refuse_reasons(&[])
            );
        }
        // Without `-MF` the sidecar name would be derived: refuse.
        for flag in ["-MD", "-MMD"] {
            let parsed = parse_ok(&["nvcc", "-c", "k.cu", "-o", "k.o", flag]);
            assert!(
                parsed.refuse_reasons(&[]).iter().any(|r| {
                    matches!(r, RefuseReason::Unsupported(d) if d.contains("-MF <file>"))
                }),
                "{flag} without -MF must refuse with the -MF reason"
            );
        }
    }

    #[test]
    fn missing_output_refuses() {
        let parsed = parse_ok(&["nvcc", "-c", "k.cu"]);
        assert!(parsed.output.is_none());
        assert!(
            parsed
                .refuse_reasons(&[])
                .iter()
                .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("missing -o")) })
        );
    }

    /// The `-M` closure through a fake nvcc: exact dep list on success,
    /// fail-closed on bad exit or garbage output. Needs no real CUDA.
    /// Env is saved/restored manually (this module owns no guard type)
    /// under the shared process-state lock.
    #[cfg(unix)]
    #[test]
    fn dependency_closure_lists_and_bails() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        let fake = dir.path().join("nvcc");
        kache_fs::testutil::write_executable(
            &fake,
            format!(
                "#!{}\nprintf '%s' \"$NVCC_FAKE_M_OUT\"\nexit \"${{NVCC_FAKE_M_EXIT:-0}}\"\n",
                shell.display()
            ),
        );
        let previous_out = std::env::var_os("NVCC_FAKE_M_OUT");
        let previous_exit = std::env::var_os("NVCC_FAKE_M_EXIT");
        unsafe {
            std::env::set_var("NVCC_FAKE_M_OUT", "k.o: /a/k.cu /a/h.h /a/k.cu\n");
            std::env::remove_var("NVCC_FAKE_M_EXIT");
        }

        let parsed = NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .parse(&[
                fake.to_string_lossy().into_owned(),
                "-c".to_string(),
                "/a/k.cu".to_string(),
                "-o".to_string(),
                "/a/k.o".to_string(),
                "-I/a".to_string(),
            ])
            .unwrap();
        let closure = nvcc_dependency_closure(&parsed).unwrap();
        assert_eq!(
            closure,
            vec![PathBuf::from("/a/h.h"), PathBuf::from("/a/k.cu")]
        );

        unsafe {
            std::env::set_var("NVCC_FAKE_M_EXIT", "1");
        }
        assert!(nvcc_dependency_closure(&parsed).is_err());
        unsafe {
            std::env::set_var("NVCC_FAKE_M_OUT", "garbage without separator");
            std::env::remove_var("NVCC_FAKE_M_EXIT");
        }
        assert!(nvcc_dependency_closure(&parsed).is_err());

        match previous_out {
            Some(value) => unsafe {
                std::env::set_var("NVCC_FAKE_M_OUT", value);
            },
            None => unsafe {
                std::env::remove_var("NVCC_FAKE_M_OUT");
            },
        }
        match previous_exit {
            Some(value) => unsafe {
                std::env::set_var("NVCC_FAKE_M_EXIT", value);
            },
            None => unsafe {
                std::env::remove_var("NVCC_FAKE_M_EXIT");
            },
        }
    }

    #[test]
    fn separate_rdc_value_is_keyed() {
        let parsed = parse_ok(&["nvcc", "-c", "-rdc", "true", "k.cu", "-o", "k.o"]);
        assert!(parsed.separate_device_code);
        assert!(parsed.refuse_reasons(&[]).is_empty());
        assert_eq!(parsed.deferred_flags, ["-rdc true"]);

        let parsed = parse_ok(&[
            "nvcc",
            "-c",
            "--relocatable-device-code",
            "false",
            "k.cu",
            "-o",
            "k.o",
        ]);
        assert!(!parsed.separate_device_code);
        assert_eq!(parsed.deferred_flags, ["--relocatable-device-code false"]);

        assert!(NvccArgs::parse(&s(&["nvcc", "-c", "-rdc=maybe", "k.cu"])).is_err());
        assert!(NvccArgs::parse(&s(&["nvcc", "-c", "-rdc"])).is_err());
    }

    #[test]
    fn dependency_query_uses_effective_rdc_mode() {
        let parsed = parse_ok(&["nvcc", "-dc", "k.cu", "-o", "k.o"]);
        assert_eq!(
            nvcc_dependency_query_args(&parsed).unwrap(),
            ["-M", "k.cu", "-rdc=true"]
        );

        let parsed = parse_ok(&["nvcc", "-c", "-rdc=true", "k.cu", "-o", "k.o"]);
        assert_eq!(
            nvcc_dependency_query_args(&parsed).unwrap(),
            ["-M", "k.cu", "-rdc=true"]
        );

        let parsed = parse_ok(&["nvcc", "-dc", "-rdc=false", "k.cu", "-o", "k.o"]);
        assert_eq!(nvcc_dependency_query_args(&parsed).unwrap(), ["-M", "k.cu"]);
    }

    /// Linker inputs are positional unknowns, never sources: a `-c` line
    /// over `.o` files has no source to key.
    #[test]
    fn linker_inputs_are_not_sources() {
        let parsed = parse_ok(&["nvcc", "-c", "a.o", "-o", "app"]);
        assert!(parsed.sources.is_empty());
        assert_eq!(parsed.unknown_flags, vec!["a.o".to_string()]);
        assert!(
            parsed.refuse_reasons(&[]).iter().any(|r| {
                matches!(r, RefuseReason::Unsupported(d) if d.contains("source-less"))
            })
        );
    }

    /// Dash-prefixed positionals are flags (unknown), never sources — even
    /// when the tail looks like a source extension.
    #[test]
    fn dash_prefixed_positionals_are_never_sources() {
        let parsed = parse_ok(&["nvcc", "-c", "-q.cu", "-o", "k.o"]);
        assert!(parsed.sources.is_empty());
        assert_eq!(parsed.unknown_flags, vec!["-q.cu".to_string()]);
    }

    /// `--time` has no dedicated arm: it falls through to the unknown
    /// bucket, which refuses identically. Pinned so a future arm addition
    /// is a conscious change.
    #[test]
    fn time_falls_through_to_unknown() {
        let parsed = parse_ok(&["nvcc", "-c", "k.cu", "--time", "-o", "k.o"]);
        assert_eq!(parsed.unknown_flags, vec!["--time".to_string()]);
        assert!(!parsed.refuse_reasons(&[]).is_empty());
    }

    #[test]
    fn flag_matches_boundary_rules() {
        assert!(flag_matches("--fancy-new-flag", "--fancy-new-flag"));
        assert!(flag_matches("-O", "-O 2"));
        assert!(flag_matches("-gencode", "-gencode=arch"));
        // No smuggling: `-O` must not cover `-O2`, and an unrelated flag
        // or a longer allow entry never matches.
        assert!(!flag_matches("-O", "-O2"));
        assert!(!flag_matches("-O", "--fancy"));
        assert!(!flag_matches("--long-flag", "-D"));
    }

    #[test]
    fn deferred_flags_need_no_allowlist() {
        // Deferred flags are keyed, not refused: `-O2` needs no allow-list…
        let parsed = parse_ok(&["nvcc", "-c", "k.cu", "-O2", "-o", "k.o"]);
        assert!(parsed.refuse_reasons(&[]).is_empty());
        assert!(parsed.refuse_reasons(&["-O".to_string()]).is_empty());
        // …while a truly unknown flag still does, and the verbatim entry
        // still opts it in.
        let parsed = parse_ok(&["nvcc", "-c", "k.cu", "--fancy-9", "-o", "k.o"]);
        assert!(!parsed.refuse_reasons(&["-O".to_string()]).is_empty());
        assert!(parsed.refuse_reasons(&["--fancy-9".to_string()]).is_empty());
    }

    /// The trait surface refusal delegates to the parsed shape; the key
    /// and execute paths are covered by the fake-toolchain tests below.
    #[test]
    fn compiler_trait_refuse_delegates() {
        let compiler = NvccCompiler::with_extra_allowlist_flags(Vec::new());
        let link = parse_ok(&["nvcc", "a.o", "b.o", "-o", "app"]);
        assert!(!compiler.refuse_reasons(&link).is_empty());
        let compile = parse_ok(&["nvcc", "-c", "k.cu", "-O2", "-o", "k.o"]);
        assert!(compiler.refuse_reasons(&compile).is_empty());
    }

    #[test]
    fn xcompiler_values_are_structured() {
        let parsed = parse_ok(&[
            "nvcc",
            "-c",
            "k.cu",
            "-o",
            "k.o",
            "-Xcompiler",
            "-fPIC",
            "--compiler-options=-Wall",
            "-Xptxas",
            "-O3",
        ]);
        assert_eq!(parsed.xcompiler_values, vec!["-fPIC", "-Wall"]);
        // -Xptxas is not a preprocessor path: unstructured, still deferred.
        assert!(parsed.deferred_flags.iter().any(|f| f == "-Xptxas -O3"));
    }

    #[test]
    fn xcompiler_smuggling_refuses() {
        for smuggled in [
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-Xcompiler", "-I/opt/x"],
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-Xcompiler", "-DFOO"],
            vec![
                "nvcc",
                "-c",
                "k.cu",
                "-o",
                "k.o",
                "-Xcompiler",
                "-O2,-isystem/x",
            ],
            vec![
                "nvcc",
                "-c",
                "k.cu",
                "-o",
                "k.o",
                "--compiler-options",
                "-UFOO",
            ],
        ] {
            let parsed = parse_ok(&smuggled);
            assert!(
                parsed.refuse_reasons(&[]).iter().any(|r| {
                    matches!(r, RefuseReason::Unsupported(d) if d.contains("-Xcompiler"))
                }),
                "{smuggled:?} must refuse"
            );
        }
        // Mixed values: one smuggled entry poisons the whole line.
        let parsed = parse_ok(&[
            "nvcc",
            "-c",
            "k.cu",
            "-o",
            "k.o",
            "-Xcompiler",
            "-O2",
            "-Xcompiler",
            "-DFOO",
        ]);
        assert_eq!(parsed.xcompiler_values, vec!["-O2", "-DFOO"]);
        assert!(
            parsed
                .refuse_reasons(&[])
                .iter()
                .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("-Xcompiler")) })
        );
        // Clean forwarding stays cacheable-shaped.
        let parsed = parse_ok(&[
            "nvcc",
            "-c",
            "k.cu",
            "-o",
            "k.o",
            "-Xcompiler",
            "-O2",
            "-Xcompiler",
            "-fPIC",
        ]);
        assert!(parsed.refuse_reasons(&[]).is_empty());
    }

    /// Machine-resolved values refuse with the native reason, in every
    /// spelling: direct, separate-value, smuggled through `-Xcompiler`,
    /// and embedded in `-gencode`.
    #[test]
    fn native_resolution_refuses() {
        for native in [
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-march=native"],
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-march", "native"],
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-mcpu=native"],
            vec![
                "nvcc",
                "-c",
                "k.cu",
                "-o",
                "k.o",
                "-Xcompiler",
                "-march=native",
            ],
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-arch=native"],
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-arch", "native"],
            vec![
                "nvcc",
                "-c",
                "k.cu",
                "-o",
                "k.o",
                "-gencode",
                "arch=native,code=sm_90",
            ],
            vec![
                "nvcc",
                "-c",
                "k.cu",
                "-o",
                "k.o",
                "--gpu-architecture=native",
            ],
        ] {
            let parsed = parse_ok(&native);
            assert!(
                parsed
                    .refuse_reasons(&[])
                    .iter()
                    .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("native")) }),
                "{native:?} must refuse"
            );
        }
        // The veto survives the allow-list: auditing a flag in cannot
        // bless a per-machine value.
        let parsed = parse_ok(&["nvcc", "-c", "k.cu", "-o", "k.o", "--march=native"]);
        assert!(
            parsed
                .refuse_reasons(&["--march=native".to_string()])
                .iter()
                .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("native")) })
        );
        // One native entry among clean ones still vetoes the line.
        let parsed = parse_ok(&["nvcc", "-c", "k.cu", "-o", "k.o", "-O2", "-march=native"]);
        assert!(
            parsed
                .refuse_reasons(&[])
                .iter()
                .any(|r| { matches!(r, RefuseReason::Unsupported(d) if d.contains("native")) })
        );
        // Near-misses stay keyed: concrete arches and defines that merely
        // mention native.
        for fine in [
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-arch=sm_80"],
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-DARCH=native"],
            vec!["nvcc", "-c", "k.cu", "-o", "k.o", "-I/opt/native/include"],
        ] {
            let parsed = parse_ok(&fine);
            assert!(
                parsed.refuse_reasons(&[]).is_empty(),
                "{fine:?} must stay cacheable"
            );
        }
    }

    /// Driver-env values: mode-altering and smuggled-preprocessor
    /// inputs refuse; plain codegen tuning passes through to keying.
    #[test]
    fn env_value_blocked_classification() {
        for blocked in [
            "-G",
            "--device-debug -O2",
            "-dc",
            "-rdc=true",
            "--relocatable-device-code=true",
            "-dlink",
            "--lib",
            "-ptx",
            "-E",
            "-DFOO",
            "-I/opt/x",
            "-O2,-isystem/x",
        ] {
            assert!(
                nvcc_env_value_blocked(blocked),
                "{blocked:?} must be blocked"
            );
        }
        for clean in [
            "",
            "-O2",
            "-gencode arch=sm_80,code=sm_80",
            "-fPIC",
            "--expt-relaxed-constexpr",
        ] {
            assert!(
                !nvcc_env_value_blocked(clean),
                "{clean:?} must pass to keying"
            );
        }
    }

    #[test]
    fn env_value_native_veto() {
        assert!(nvcc_check_env_value("NVCC_PREPEND_FLAGS", "-march=native").is_err());
        assert!(nvcc_check_env_value("NVCC_APPEND_FLAGS", "-O2").is_ok());
        assert!(nvcc_check_env_value("NVCC_PREPEND_FLAGS", "").is_ok());
    }

    #[test]
    fn dep_forward_args_cover_resolution_inputs() {
        let forwarded = nvcc_dep_forward_args(
            &[
                "-D FOO=1",
                "-DUSE_CUDA",
                "-I/usr/include",
                "-I /opt/include",
                "-isystem/sys",
                "-std=c++17",
                "-x cu",
                "-O2",
                "-gencode",
                "arch=compute_80,code=sm_80",
                "-Xcompiler",
                "-fPIC",
                "-Werror",
                "-m64",
            ]
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>(),
        );
        for expected in [
            "-D",
            "FOO=1",
            "-DUSE_CUDA",
            "-I/usr/include",
            "-I",
            "/opt/include",
            "-isystem/sys",
            "-std=c++17",
            "-x",
            "cu",
            "-O2",
            "-m64",
        ] {
            assert!(
                forwarded.iter().any(|f| f == expected),
                "missing forwarded {expected:?} in {forwarded:?}"
            );
        }
        for excluded in ["-gencode", "-Xcompiler", "-fPIC", "-Werror"] {
            assert!(
                !forwarded.iter().any(|f| f == excluded),
                "must not forward {excluded:?}: {forwarded:?}"
            );
        }
    }

    #[test]
    fn line_rule_target_shapes() {
        // Real rule lines qualify in every spelling.
        for rule in [
            "kernel.o: kernel.cu\n",
            "kernel.o : kernel.cu\n",
            "C:\\b\\k.obj: C:\\s\\k.cu\n",
            "C:/b/k.obj: C:/s/k.cu\n",
            "my\\ dir/k.o: k.cu\n",
            "   spaced.o: k.cu\n",
        ] {
            assert!(line_has_rule_target(rule), "{rule:?} must qualify");
        }
        // Prose, blanks, and colon-less lines never do — notably the
        // `nvcc warning : ...` prologue some toolkits print on stdout.
        // (A `word: ...` line WOULD qualify as a degenerate rule; that
        // is safe — its tokens fail the hash and bail.)
        for prose in [
            "nvcc warning : Support for offline compilation\n",
            "\n",
            "no colon here\n",
            ": leading colon\n",
        ] {
            assert!(!line_has_rule_target(prose), "{prose:?} must not qualify");
        }
    }

    #[test]
    fn make_deps_skips_driver_prologue() {
        // Real CUDA 12.8 shape: a warning prologue on stdout, then the
        // rule. Without the skip, the separator would land inside the
        // prologue and every word of it would fail the hash.
        let parsed = parse_nvcc_make_deps(
            "nvcc warning : Support for offline compilation for architectures prior to '<compute/sm/lto>_75' will be removed\nkernel.o : kernel.cu \\\n inc/k.h \\\n /usr/include/stdc-predef.h\n",
        )
        .unwrap();
        assert_eq!(
            parsed,
            vec![
                PathBuf::from("kernel.cu"),
                PathBuf::from("inc/k.h"),
                PathBuf::from("/usr/include/stdc-predef.h"),
            ]
        );
    }

    #[test]
    fn make_deps_parse_handles_continuations_and_drives() {
        let parsed = parse_nvcc_make_deps(
            "build/kernel.o: src/kernel.cu \\\n src/kernel.h /usr/local/cuda/include/cuda_runtime.h \\\n",
        )
        .unwrap();
        assert_eq!(
            parsed,
            vec![
                PathBuf::from("src/kernel.cu"),
                PathBuf::from("src/kernel.h"),
                PathBuf::from("/usr/local/cuda/include/cuda_runtime.h"),
            ]
        );
        // Backslash-space escapes survive; other backslashes (notably
        // Windows separators) are literal.
        let parsed = parse_nvcc_make_deps("k.o: dir/my\\ header.h dir/ok.h\n").unwrap();
        assert_eq!(
            parsed,
            vec![PathBuf::from("dir/my header.h"), PathBuf::from("dir/ok.h"),]
        );
        let parsed = parse_nvcc_make_deps("k.o: C:\\temp\\a.h\n").unwrap();
        assert_eq!(parsed, vec![PathBuf::from("C:\\temp\\a.h")]);
        assert!(parse_nvcc_make_deps("no separator here\n").is_err());
        // Windows drive-letter target: the first colon is the drive.
        let parsed = parse_nvcc_make_deps("C:\\b\\k.obj: C:\\s\\k.cu C:\\s\\k.h\n").unwrap();
        assert_eq!(
            parsed,
            vec![PathBuf::from("C:\\s\\k.cu"), PathBuf::from("C:\\s\\k.h")]
        );
    }

    #[test]
    fn absolutize_joins_relative_and_passes_absolute() {
        let cwd = Path::new("/work/tree");
        assert_eq!(
            nvcc_absolutize(Path::new("src/k.cu"), cwd),
            PathBuf::from("/work/tree/src/k.cu")
        );
        assert_eq!(
            nvcc_absolutize(Path::new("/elsewhere/k.cu"), cwd),
            PathBuf::from("/elsewhere/k.cu")
        );
    }

    #[test]
    fn canonicalize_falls_back_to_self() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            nvcc_canonicalize_or_self(dir.path()),
            std::fs::canonicalize(dir.path()).unwrap()
        );
        let missing = dir.path().join("nope");
        assert_eq!(nvcc_canonicalize_or_self(&missing), missing);
    }

    #[test]
    fn normalize_disabled_by_knob() {
        let _lock = crate::test_support::process_state_test_lock();
        let previous = std::env::var_os("KACHE_NVCC_PATH_NORMALIZE");
        unsafe {
            std::env::set_var("KACHE_NVCC_PATH_NORMALIZE", "0");
        }
        let maps = nvcc_prefix_maps(Path::new("k.cu"), None, &[]);
        match previous {
            Some(value) => unsafe {
                std::env::set_var("KACHE_NVCC_PATH_NORMALIZE", value);
            },
            None => unsafe {
                std::env::remove_var("KACHE_NVCC_PATH_NORMALIZE");
            },
        }
        assert!(maps.is_empty());
    }

    #[test]
    fn epoch_passthrough_knob_disables_the_pin() {
        let _lock = crate::test_support::process_state_test_lock();
        let previous_epoch = std::env::var_os("SOURCE_DATE_EPOCH");
        let previous_knob = std::env::var_os("KACHE_NVCC_SOURCE_DATE_EPOCH");
        unsafe {
            std::env::remove_var("SOURCE_DATE_EPOCH");
            std::env::set_var("KACHE_NVCC_SOURCE_DATE_EPOCH", "wallclock");
        }
        let epoch = nvcc_effective_source_date_epoch();
        unsafe { std::env::remove_var("KACHE_NVCC_SOURCE_DATE_EPOCH") };
        let pinned = nvcc_effective_source_date_epoch();
        match previous_epoch {
            Some(value) => unsafe {
                std::env::set_var("SOURCE_DATE_EPOCH", value);
            },
            None => unsafe {
                std::env::remove_var("SOURCE_DATE_EPOCH");
            },
        }
        match previous_knob {
            Some(value) => unsafe {
                std::env::set_var("KACHE_NVCC_SOURCE_DATE_EPOCH", value);
            },
            None => unsafe {
                std::env::remove_var("KACHE_NVCC_SOURCE_DATE_EPOCH");
            },
        }
        assert_eq!(epoch, None);
        assert_eq!(pinned, Some(std::ffi::OsString::from("0")));
    }

    #[test]
    fn prefix_maps_cover_cwd_and_source() {
        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("src").join("k.cu");
        let maps = nvcc_prefix_maps(&source, None, &[]);
        assert!(
            maps.iter().any(|m| m.to == NVCC_ROOT_SENTINEL),
            "derived roots must map: {maps:?}"
        );
        // Longest first: the source dir precedes its parents.
        let positions: Vec<usize> = maps.iter().map(|m| m.from.len()).collect();
        let mut sorted = positions.clone();
        sorted.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(positions, sorted);
    }

    #[test]
    fn prefix_map_push_skips_empty_and_duplicate_from() {
        let mut maps = Vec::new();
        nvcc_push_unique_map(&mut maps, String::new(), NVCC_ROOT_SENTINEL.to_string());
        assert!(maps.is_empty(), "empty from must not become a prefix map");
        nvcc_push_unique_map(
            &mut maps,
            "/a/proj".to_string(),
            NVCC_ROOT_SENTINEL.to_string(),
        );
        nvcc_push_unique_map(
            &mut maps,
            "/a/proj".to_string(),
            "/kache/base-dir-0".to_string(),
        );
        assert_eq!(maps.len(), 1);
        assert_eq!(maps[0].to, NVCC_ROOT_SENTINEL);
    }

    #[cfg(unix)]
    #[test]
    fn execute_drops_artifacts_when_object_embeds_mapped_root() {
        let dir = tempfile::tempdir().unwrap();
        let nvcc = dir.path().join("nvcc");
        let src = dir.path().join("k.cu");
        let obj = dir.path().join("k.o");
        std::fs::write(&src, "void k(void) {}\n").unwrap();
        let payload = dir.path().join("payload");
        std::fs::write(&payload, format!("ELF{}", dir.path().display())).unwrap();
        kache_fs::testutil::write_executable(
            &nvcc,
            format!(
                "#!/bin/sh\nout=\nprev=\nfor a in \"$@\"; do\n  if [ \"$prev\" = \"-o\" ]; then out=$a; fi\n  prev=$a\ndone\ncp '{}' \"$out\"\n",
                payload.display()
            ),
        );

        let parsed = parse_ok(&[
            nvcc.to_str().unwrap(),
            "-c",
            src.to_str().unwrap(),
            "-o",
            obj.to_str().unwrap(),
        ]);
        let result = NvccCompiler::with_extra_allowlist_flags(Vec::new())
            .execute(&parsed)
            .unwrap();
        assert_eq!(result.exit_code, 0, "stderr={}", result.stderr);
        assert!(obj.is_file(), "the compile must still write the object");
        assert!(
            result.artifacts.is_empty(),
            "an object that embeds a mapped root must not be cached, got {:?}",
            result
                .artifacts
                .outputs()
                .iter()
                .map(|a| &a.path)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn normalize_toggle_defaults_on_opts_out_on_zero() {
        assert!(parse_nvcc_normalize_toggle(None));
        assert!(parse_nvcc_normalize_toggle(Some("1")));
        assert!(!parse_nvcc_normalize_toggle(Some("0")));
    }

    #[test]
    fn normalize_path_needs_separator_boundary() {
        let maps = vec![NvccPrefixMap {
            from: "/a/proj".to_string(),
            to: NVCC_ROOT_SENTINEL.to_string(),
        }];
        assert_eq!(
            nvcc_normalize_path("/a/proj/src/k.cu", &maps),
            "/kache/nvcc-root/src/k.cu"
        );
        assert_eq!(nvcc_normalize_path("/a/proj", &maps), "/kache/nvcc-root");
        // No boundary, no rewrite: /a/proj2 is a different tree.
        assert_eq!(nvcc_normalize_path("/a/proj2/k.cu", &maps), "/a/proj2/k.cu");
        assert_eq!(nvcc_normalize_path("/other/k.cu", &maps), "/other/k.cu");
        // Backslash boundaries behave the same (Windows spellings).
        let win_maps = vec![NvccPrefixMap {
            from: "C:\\proj".to_string(),
            to: NVCC_ROOT_SENTINEL.to_string(),
        }];
        assert_eq!(
            nvcc_normalize_path("C:\\proj\\k.cu", &win_maps),
            "/kache/nvcc-root\\k.cu"
        );
        assert_eq!(
            nvcc_normalize_path("C:\\proj2\\k.cu", &win_maps),
            "C:\\proj2\\k.cu"
        );
    }

    #[test]
    fn prefix_map_args_reject_unsafe_sides() {
        let ok = nvcc_prefix_map_args(&[NvccPrefixMap {
            from: "/a/proj".to_string(),
            to: NVCC_ROOT_SENTINEL.to_string(),
        }])
        .unwrap();
        assert_eq!(
            ok,
            vec![
                "-Xcompiler".to_string(),
                "-ffile-prefix-map=/a/proj=/kache/nvcc-root".to_string()
            ]
        );
        for bad in ["/a,proj", "/a proj", "", "/a\"proj"] {
            assert!(
                nvcc_prefix_map_args(&[NvccPrefixMap {
                    from: bad.to_string(),
                    to: NVCC_ROOT_SENTINEL.to_string(),
                }])
                .is_err(),
                "{bad:?} must not be forwarded"
            );
        }
        // The check covers both sides: an unsafe target refuses too.
        assert!(
            nvcc_prefix_map_args(&[NvccPrefixMap {
                from: "/a/proj".to_string(),
                to: "/kache,nvcc-root".to_string(),
            }])
            .is_err()
        );
    }

    #[test]
    fn epoch_opt_out_names() {
        for enabled in ["passthrough", "wallclock", "off", "PASSTHROUGH", " Off "] {
            assert!(nvcc_epoch_opt_out(enabled), "{enabled:?} must opt out");
        }
        for other in ["", "yes", "0", "1", "passthroughx"] {
            assert!(!nvcc_epoch_opt_out(other), "{other:?} must pin");
        }
    }

    #[test]
    fn object_scan_finds_raw_roots() {
        let dir = tempfile::tempdir().unwrap();
        let object = dir.path().join("k.o");
        std::fs::write(&object, b"\x7fELF__FILE__=/a/proj/src/k.cu\x00rest").unwrap();
        let maps = vec![
            NvccPrefixMap {
                from: "/unrelated".to_string(),
                to: NVCC_ROOT_SENTINEL.to_string(),
            },
            NvccPrefixMap {
                from: "/a/proj".to_string(),
                to: NVCC_ROOT_SENTINEL.to_string(),
            },
        ];
        assert!(nvcc_object_embeds_mapped_root(&object, &maps).unwrap());
        std::fs::write(&object, b"\x7fELF__FILE__=/kache/nvcc-root/src/k.cu\x00").unwrap();
        assert!(!nvcc_object_embeds_mapped_root(&object, &maps).unwrap());
        // An empty map side never matches (and never panics the scan).
        let empty_maps = vec![NvccPrefixMap {
            from: String::new(),
            to: NVCC_ROOT_SENTINEL.to_string(),
        }];
        assert!(!nvcc_object_embeds_mapped_root(&object, &empty_maps).unwrap());
        assert!(nvcc_object_embeds_mapped_root(&dir.path().join("missing.o"), &maps).is_err());
    }

    #[test]
    fn target_label_names_arches() {
        assert_eq!(nvcc_target_label(&[]), "generic");
        assert_eq!(
            nvcc_target_label(&[
                "-arch=sm_80".to_string(),
                "-gencode arch=compute_90,code=sm_90".to_string(),
                "--gpu-architecture=compute_90".to_string(),
                "-code=sm_90".to_string(),
                "--gpu-code=sm_90".to_string(),
                "-O2".to_string(),
            ]),
            "--gpu-architecture=compute_90+--gpu-code=sm_90+-arch=sm_80+-code=sm_90+-gencode arch=compute_90,code=sm_90"
        );
    }

    #[test]
    fn base_dirs_ordering_is_canonical() {
        let _lock = crate::test_support::process_state_test_lock();
        // `with_base_dirs` sorts and dedups so flag order on the command
        // line cannot perturb map derivation downstream (configured roots
        // take index-based sentinels).
        let sorted = NvccCompiler::with_extra_allowlist_flags(Vec::new()).with_base_dirs(vec![
            "/b".to_string(),
            "/a".to_string(),
            "/b".to_string(),
        ]);
        let maps_a = nvcc_prefix_maps(Path::new("k.cu"), None, &sorted.base_dirs);
        let maps_b = nvcc_prefix_maps(
            Path::new("k.cu"),
            None,
            &["/a".to_string(), "/b".to_string()],
        );
        assert_eq!(maps_a, maps_b);
    }
}
