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
use std::path::{Path, PathBuf};

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
    /// `-MF <file>` / `-MF<file>`: explicit dep-info sidecar. The only
    /// dep-info shape v1 caches — bare `-M`/`-MD`/`-MMD` name the sidecar
    /// implicitly (derived from the output), and an unvalidated guess at
    /// nvcc's derivation is a misplaced-restore waiting to happen.
    pub depfile: Option<PathBuf>,
    /// Bare `-M`/`-MM`/`-MD`/`-MMD`/`-MG`/`-MP`/`--generate-dependencies`:
    /// refused (see `depfile`).
    pub implicit_depfile: bool,
    /// `-dc` or `-rdc=true`: relocatable / separable device code.
    pub separate_device_code: bool,
    /// `-G`: device-debug info.
    pub device_debug: bool,
    /// `-keep` / `--save-temps`: intermediate files kept.
    pub keep_temps: bool,
    /// `@file`: response file (never expanded — refuse).
    pub response_file: bool,
    /// Flags the phase-1 table knows, keyed verbatim into the cache key
    /// (see `cache_key`). The table exists so phase 3 promotes entries to
    /// resolved/probed classification instead of discovering flags from
    /// scratch.
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
/// `-MFout.d` matches the joined form. Every entry is deferred (known but
/// unkeyed) until phase 2 promotes it into the cache key.
const VALUE_FLAGS: &[&str] = &[
    "-D", "-U", "-I", "-isystem", "-iquote", "-include", "-O", "-std", "--std", "-x", "-L", "-l",
    "-MT", "-MQ", "-Xlinker", "-Xptxas", "-Xnvlink", "-gencode", "-arch", "-code",
];

/// Joined prefixes (`-O2`, `-DFOO`, `-arch=sm_80`, `-Werror`). Checked
/// after [`VALUE_FLAGS`].
///
/// NOTE for phase 2: `-march=native` (and any host-resolved value) must
/// refuse, never key — a resolved-through-probe classification like cc's
/// `CapturedByProbe`, not a verbatim accept.
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
                // Bare `-M` family: nvcc derives the sidecar name from the
                // output, and v1 does not guess at that derivation.
                "-M" | "-MM" | "-MD" | "-MMD" | "-MG" | "-MP" | "--generate-dependencies" => {
                    parsed.implicit_depfile = true;
                }
                _ if arg == "-rdc" => parsed.separate_device_code = true,
                _ if arg.starts_with("-rdc=") || arg.starts_with("--relocatable-device-code=") => {
                    let value = arg.rsplit('=').next().unwrap_or("");
                    parsed.separate_device_code = matches!(value, "true" | "1");
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
        if self.output.is_none() {
            reasons.push(RefuseReason::Unsupported(
                "nvcc default output naming (missing -o) — not yet supported",
            ));
        }
        if self.implicit_depfile {
            reasons.push(RefuseReason::Unsupported(
                "nvcc implicit depfile (-M/-MD/-MMD) — pass -MF <file> (not yet supported)",
            ));
        }
        if nvcc_xcompiler_smuggles_pp(&self.xcompiler_values) {
            reasons.push(RefuseReason::Unsupported(
                "nvcc -Xcompiler hides preprocessor flags (-I/-D/…) from dependency tracking — not yet supported",
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

/// Preprocessor-affecting tokens: if one hides inside an `-Xcompiler`
/// value, header inputs escape the `-M` closure. Substring-conservative
/// on purpose — an over-refusal is a passthrough, an under-refusal is a
/// stale hit.
const NVCC_PP_TOKENS: &[&str] = &["-I", "-D", "-U", "-isystem", "-iquote", "-include"];

/// Does any `-Xcompiler` / `--compiler-options` value smuggle a
/// preprocessor flag past dependency tracking?
fn nvcc_xcompiler_smuggles_pp(values: &[String]) -> bool {
    values.iter().any(|value| {
        value.split([',', ' ', '\t']).any(|token| {
            NVCC_PP_TOKENS
                .iter()
                .any(|f| token == *f || token.starts_with(f))
        })
    })
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
/// (unhandled escapes, missing colon, Windows drive-letter targets are
/// handled; anything else) bails to passthrough. A dropped header
/// would under-key the entry — the fatal direction — so doubt means
/// refusal, never a guess.
fn parse_nvcc_make_deps(text: &str) -> Result<Vec<PathBuf>> {
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
    // drive-letter colon (`C:\…` / `C:/…`).
    let mut search_from = 0;
    let separator = loop {
        let Some(relative) = joined[search_from..].find(':') else {
            anyhow::bail!("nvcc -M output has no target separator");
        };
        let index = search_from + relative;
        let bytes = joined.as_bytes();
        if index == 1 && bytes[0].is_ascii_alphabetic() {
            search_from = index + 1;
            continue;
        }
        break index;
    };
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
    let source = parsed
        .sources
        .first()
        .context("nvcc -M with no source file")?;
    let mut cmd_args = vec!["-M".to_string(), source.to_string_lossy().into_owned()];
    cmd_args.extend(nvcc_dep_forward_args(&parsed.deferred_flags));
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
        if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
            maps.push(NvccPrefixMap { from, to });
        }
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
            let from = candidate.to_string_lossy().to_string();
            if !from.is_empty() && !maps.iter().any(|m| m.from == from) {
                maps.push(NvccPrefixMap {
                    from,
                    to: NVCC_ROOT_SENTINEL.to_string(),
                });
            }
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
    resolve_nvcc_source_date_epoch(
        std::env::var_os("SOURCE_DATE_EPOCH"),
        nvcc_source_date_epoch_passthrough(),
    )
}

/// Pure core of [`nvcc_effective_source_date_epoch`] — unit-testable
/// without touching the process environment.
fn resolve_nvcc_source_date_epoch(
    build_value: Option<std::ffi::OsString>,
    passthrough: bool,
) -> Option<std::ffi::OsString> {
    match build_value {
        Some(v) => Some(v),
        None if passthrough => None,
        None => Some(std::ffi::OsString::from("0")),
    }
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
        // verbatim fold as cc: opting in is keying.
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
            artifacts,
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

        // The long spelling feeds the same flag.
        let parsed = parse_ok(&["nvcc", "-c", "--relocatable-device-code=true", "k.cu"]);
        assert!(parsed.separate_device_code);

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
        for flag in [
            "-M",
            "-MM",
            "-MD",
            "-MMD",
            "-MG",
            "-MP",
            "--generate-dependencies",
        ] {
            let parsed = parse_ok(&["nvcc", "-c", "k.cu", "-o", "k.o", flag]);
            assert!(parsed.implicit_depfile, "{flag} must set implicit_depfile");
            assert!(
                parsed.refuse_reasons(&[]).iter().any(|r| {
                    matches!(r, RefuseReason::Unsupported(d) if d.contains("implicit depfile"))
                }),
                "{flag} must refuse with the depfile reason"
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
        use std::os::unix::fs::PermissionsExt;

        let _lock = crate::test_support::process_state_test_lock();
        let dir = tempfile::tempdir().unwrap();
        let shell =
            crate::compiler::resolve_program_on_path("sh").expect("sh must be available on PATH");
        let fake = dir.path().join("nvcc");
        std::fs::write(
            &fake,
            format!(
                "#!{}\nprintf '%s' \"$NVCC_FAKE_M_OUT\"\nexit \"${{NVCC_FAKE_M_EXIT:-0}}\"\n",
                shell.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
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
    fn bare_rdc_sets_separable() {
        let parsed = parse_ok(&["nvcc", "-c", "-rdc", "k.cu", "-o", "k.o"]);
        assert!(parsed.separate_device_code);
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
    fn prefix_maps_cover_cwd_and_source() {
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
    fn epoch_resolution_prefers_build_pin_then_opt_out() {
        use std::ffi::OsString;
        assert_eq!(
            resolve_nvcc_source_date_epoch(Some(OsString::from("12345")), false),
            Some(OsString::from("12345"))
        );
        assert_eq!(
            resolve_nvcc_source_date_epoch(Some(OsString::from("12345")), true),
            Some(OsString::from("12345"))
        );
        assert_eq!(resolve_nvcc_source_date_epoch(None, true), None);
        assert_eq!(
            resolve_nvcc_source_date_epoch(None, false),
            Some(OsString::from("0"))
        );
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
