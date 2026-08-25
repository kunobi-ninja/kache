use anyhow::{Context, Result, bail};
use std::borrow::Cow;
use std::path::{Path, PathBuf};

use crate::compiler::rustc::RustcCompiler;

/// rustc flags that affect only diagnostics formatting or queries — never
/// the emitted artifact bytes. Their separated value (`--flag value`) is
/// skipped during parsing so neither the flag nor its value reaches the
/// `residual_args` catch-all and over-keys the result (kunobi-ninja/kache#324).
///
/// Lint configuration is deliberately NOT listed here even when it cannot
/// change a successful compile's object bytes: lint levels and `--check-cfg`
/// can change whether compilation fails, and a cache hit replays success — see
/// [`OUTCOME_AFFECTING_VALUE_FLAGS`].
const IGNORED_VALUE_FLAGS: &[&str] = &[
    "--error-format",
    "--json",
    "--color",
    "--diagnostic-width",
    "--print",
    "--explain",
];

/// Attached (`--flag=value`) forms of the diagnostics/query flags above.
const IGNORED_ATTACHED_PREFIXES: &[&str] = &[
    "--error-format=",
    "--json=",
    "--color=",
    "--diagnostic-width=",
    "--print=",
    "--explain=",
];

/// rustc flags that can flip the compile's *outcome* between failure and success
/// without changing the object bytes of the successful compile. A cache hit
/// replays success, so two invocations differing only here MUST NOT share a
/// key. Every lint level is included: `-A dead_code` can override
/// `-D warnings`, while `-W missing_docs` can become fatal under that same
/// group gate. `--check-cfg` controls which cfg names/values trigger the
/// `unexpected_cfgs` lint. Each flag is
/// captured with its value into [`RustcArgs::outcome_lint_flags`] and folded
/// into the cache key.
const OUTCOME_AFFECTING_VALUE_FLAGS: &[&str] = &[
    "-W",           // warn: can become fatal under a deny-level group
    "--warn",       // long form of -W
    "-A",           // allow: can relax an otherwise fatal lint
    "--allow",      // long form of -A
    "-D",           // deny: warnings of the named lint become hard errors
    "--deny",       // long form of -D
    "-F",           // forbid: like deny, cannot be re-allowed downstream
    "--forbid",     // long form of -F
    "--force-warn", // forces warn level; overrides attribute-level deny/allow
    "--cap-lints", // caps every lint level; changes effective levels (cargo passes --cap-lints allow)
    "--check-cfg", // changes which cfg names/values unexpected_cfgs accepts
];

/// Attached forms (`--deny=warnings`, `-Dwarnings`, `--check-cfg=cfg(...)`, …) of
/// [`OUTCOME_AFFECTING_VALUE_FLAGS`]. Bare `-D` / `-F` prefixes match any
/// attached value because rustc defines no other flag beginning with those
/// spellings; the same applies to `-W` / `-A`.
const OUTCOME_LINT_ATTACHED_PREFIXES: &[&str] = &[
    "--warn=",
    "--allow=",
    "--deny=",
    "--forbid=",
    "--force-warn=",
    "--cap-lints=",
    "--check-cfg=",
    "-W",
    "-A",
    "-D",
    "-F",
];

/// Boolean diagnostics / query flags (no value) that must not reach the key.
const IGNORED_BOOL_FLAGS: &[&str] = &["-v", "--verbose", "-V", "--version", "-h", "--help"];

/// Cargo/rustc artifact basename stem: `{crate_name}{extra_filename}`.
pub fn format_crate_output_stem(crate_name: &str, extra_filename: &str) -> String {
    format!("{crate_name}{extra_filename}")
}

/// Compilation-unit identity for diagnostics: cargo's `-C extra-filename` hash,
/// without its leading dash (kunobi-ninja/kache#627).
///
/// `crate_name` is not a unit identity — two versions of a package, a host and
/// a target build of the same crate, and different feature sets all collapse
/// onto one name. Cargo's `extra-filename` is exactly the disambiguator that
/// keeps those units' artifacts from colliding in one `deps/` directory, so
/// within a build tree it identifies the unit precisely.
///
/// Deliberately NOT `-C metadata`: cargo sets the two to different hashes
/// (`-C metadata=04bad873faff484a -C extra-filename=-843f02d6a46ebef1` in one
/// observed invocation), and it is `extra-filename` that appears in the
/// artifact filename a consumer sees on its `--extern` path. Matching the two
/// sides needs the one that is visible from both.
///
/// This is recorded on events, never folded into a cache key: keying on it
/// would tie the key to cargo's unit hashing and break cross-machine sharing.
pub fn unit_id_from_extra_filename(extra_filename: &str) -> Option<String> {
    let id = extra_filename.strip_prefix('-').unwrap_or(extra_filename);
    (!id.is_empty()).then(|| id.to_string())
}

/// The producing unit's identity, recovered from an `--extern` artifact path.
///
/// Cargo names dependency artifacts `lib{crate_name}-{hash}.rlib` (`.rmeta`,
/// `.dylib`, `.so`, `.dll`), where `-{hash}` is the producer's
/// `-C extra-filename`. Taking the tail after the LAST dash is safe because a
/// rustc crate name cannot contain one.
///
/// Returns `None` for anything not carrying that suffix — a sysroot crate, a
/// hand-rolled rustc invocation, an artifact built without `extra-filename` —
/// so callers fall back to name matching rather than inventing an identity.
pub fn unit_id_from_artifact_path(path: &Path) -> Option<String> {
    let stem = path.file_stem()?.to_str()?;
    let (_, suffix) = stem.rsplit_once('-')?;
    // Cargo's hash is lowercase hex. Requiring that shape keeps a crate whose
    // *file* name happens to carry a dash (`libfoo-bar.rlib`, built outside
    // cargo) from being read as a unit id.
    let hex = suffix.len() >= 8 && suffix.bytes().all(|b| b.is_ascii_hexdigit());
    hex.then(|| suffix.to_string())
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum RustcArgfileState {
    #[default]
    None,
    Expanded,
    Unsupported,
}

#[derive(Default)]
struct RustcArgfileExpander {
    shell_argfiles: bool,
    next_is_unstable_option: bool,
    expanded: Vec<String>,
}

impl RustcArgfileExpander {
    fn is_shell_argfiles_option(option: &str) -> bool {
        option == "shell-argfiles" || option.starts_with("shell-argfiles=")
    }

    fn expand_arg(&mut self, arg: &str) -> Result<()> {
        let Some(path) = arg.strip_prefix('@') else {
            self.push(arg.to_string());
            return Ok(());
        };

        if self.shell_argfiles && path.starts_with("shell:") {
            bail!("rustc shell-style response files are not yet supported");
        }

        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("reading rustc response file `{path}`"))?;
        // Match rustc exactly: each line is one argument, whitespace and blank
        // lines are preserved, and lines originating in a response file are
        // pushed verbatim rather than recursively expanded.
        for line in contents.lines() {
            self.push(line.to_string());
        }
        Ok(())
    }

    fn push(&mut self, arg: String) {
        // rustc inspects -Z options while expanding because
        // `-Zshell-argfiles` changes the meaning of a later top-level
        // `@shell:path`. We track that state only to fail closed on the
        // unsupported shell-style subset; ordinary response files still work.
        if self.next_is_unstable_option {
            if Self::is_shell_argfiles_option(&arg) {
                self.shell_argfiles = true;
            }
            self.next_is_unstable_option = false;
        } else if let Some(option) = arg.strip_prefix("-Z") {
            if option.is_empty() {
                self.next_is_unstable_option = true;
            } else if Self::is_shell_argfiles_option(option) {
                self.shell_argfiles = true;
            }
        }
        self.expanded.push(arg);
    }
}

/// Expand rustc's standard UTF-8, one-argument-per-line response files.
///
/// Returns `None` without allocating when the invocation contains no response
/// files. Expansion is atomic: any read/UTF-8/shell-style/lossless-transport
/// failure makes the whole invocation unsupported so the wrapper can pass the
/// original argv to rustc for its authoritative diagnostic.
fn expand_rustc_argfiles(args: &[String]) -> Result<Option<Vec<String>>> {
    if !args.iter().any(|arg| arg.starts_with('@')) {
        return Ok(None);
    }

    let mut expander = RustcArgfileExpander::default();
    for arg in args {
        expander.expand_arg(arg)?;
    }

    // Kache snapshots the effective argv into its own standard response file
    // before invoking rustc. Newlines and carriage returns cannot be encoded
    // losslessly in that format, so leave these rare invocations uncached.
    if expander
        .expanded
        .iter()
        .any(|arg| arg.contains('\n') || arg.contains('\r'))
    {
        bail!("rustc response file expands to an argument containing a line break");
    }

    Ok(Some(expander.expanded))
}

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
    /// Explicit output path from `--emit=dep-info=<path>`, when present.
    /// Cargo normally leaves this implicit; direct-rustc layouts are retained
    /// so cache admission can force a safe passthrough.
    pub dep_info_output: Option<PathBuf>,
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
    /// Direct rustc `--remap-path-prefix FROM=TO` values in argv order.
    /// The key normalizes known machine-local FROM prefixes while retaining
    /// unrelated FROM values, TO values, and occurrence order.
    pub remap_path_prefixes: Vec<String>,
    /// Inner rustc path for double-wrapper case (RUSTC_WRAPPER + RUSTC_WORKSPACE_WRAPPER).
    /// When both wrappers are active, cargo passes: wrapper workspace_wrapper rustc <args>.
    /// This field holds the rustc path that the workspace wrapper expects as its first arg.
    pub inner_rustc: Option<PathBuf>,
    /// Effective rustc arguments after standard response-file expansion.
    /// Identical to the original arguments when no response file was used.
    pub all_args: Vec<String>,
    /// Original compact argv. Safe to reuse after a response transport
    /// failure only when Kache did not rewrite any effective argument.
    raw_args: Option<Vec<String>>,
    argfile_state: RustcArgfileState,
    /// Argv tokens not matched by any modeled flag above, excluding the
    /// diagnostics / lint / query / already-keyed path flags that parsing
    /// explicitly drops. These can still affect codegen (e.g. `-O`, `-g`, or a
    /// future rustc flag) yet were previously invisible to the cache key, so
    /// they are folded in under a versioned tag (kunobi-ninja/kache#324).
    pub residual_args: Vec<String>,
    /// Outcome-affecting lint configuration (`-A`/`-W`/`-D`/`-F`, their long
    /// forms, `--force-warn`, `--cap-lints`, and `--check-cfg`) captured as
    /// flag+value token pairs in argv order, including attached spellings.
    /// These cannot change successful object bytes but DO change whether the
    /// compile fails, and hits replay success — so they are folded into the
    /// cache key.
    pub outcome_lint_flags: Vec<String>,
    /// Whether this is a `--test` compilation (test harness binary)
    pub is_test: bool,
    /// Whether this looks like a primary compilation (has source file + crate name)
    pub is_primary: bool,
    /// Snapshot of the `KACHE_RUSTC_PATH_NORMALIZE` opt-out, read once at parse
    /// time. Both the cache-key `remap:` fold ([`crate::cache_key`]) and the
    /// rustc invocation ([`crate::compiler::rustc`]) consult this ONE snapshot
    /// via [`RustcArgs::skip_path_remap`], so they can never observe different
    /// process-global env values and desync the key from the artifact.
    pub path_normalize_disabled: bool,
    /// Path-normalization root selected once at parse time. Key construction,
    /// rustc injection, and dep-info transport all reuse this frozen value so
    /// filesystem drift cannot make them observe different remap rules.
    path_normalization_root: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct ExternDep {
    pub name: String,
    pub path: Option<PathBuf>,
}

fn parse_emit_value(value: &str, kinds: &mut Vec<String>, dep_info_output: &mut Option<PathBuf>) {
    for part in value.split(',') {
        let (kind, output) = part.split_once('=').unwrap_or((part, ""));
        kinds.push(kind.to_string());
        if kind == "dep-info" && !output.is_empty() {
            *dep_info_output = Some(PathBuf::from(output));
        }
    }
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

        let (parse_args, raw_args, argfile_state) = match expand_rustc_argfiles(rustc_args) {
            Ok(Some(expanded)) => (
                Cow::Owned(expanded),
                Some(rustc_args.to_vec()),
                RustcArgfileState::Expanded,
            ),
            Ok(None) => (Cow::Borrowed(rustc_args), None, RustcArgfileState::None),
            Err(error) => {
                tracing::debug!(
                    "rustc response-file expansion failed; passing through uncached: {error:#}"
                );
                (
                    Cow::Borrowed(rustc_args),
                    None,
                    RustcArgfileState::Unsupported,
                )
            }
        };
        let rustc_args = parse_args.as_ref();

        let mut parsed = RustcArgs {
            rustc,
            crate_name: None,
            crate_types: Vec::new(),
            output: None,
            out_dir: None,
            emit: Vec::new(),
            dep_info_output: None,
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
            remap_path_prefixes: Vec::new(),
            inner_rustc,
            all_args: rustc_args.to_vec(),
            raw_args,
            argfile_state,
            residual_args: Vec::new(),
            outcome_lint_flags: Vec::new(),
            is_test: false,
            is_primary: false,
            path_normalize_disabled: !crate::path_normalizer::rustc_path_normalize_enabled(),
            path_normalization_root: None,
        };
        // Some rustc queries accept a crate name and source path but only print
        // information to stdout; they deliberately emit no cacheable artifacts.
        // Keep this separate from ignored-key flags so source-bearing queries
        // cannot be mistaken for primary compilations.
        let mut is_query = false;

        let mut i = 0;
        while i < rustc_args.len() {
            let i_before = i;
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
                        parse_emit_value(val, &mut parsed.emit, &mut parsed.dep_info_output);
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
                    parse_emit_value(val, &mut parsed.emit, &mut parsed.dep_info_output);
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
                "-C" | "--codegen" => {
                    i += 1;
                    if let Some(val) = rustc_args.get(i) {
                        record_codegen_opt(&mut parsed, val);
                    }
                }
                _ if arg.starts_with("-C") && arg.len() > 2 => {
                    record_codegen_opt(&mut parsed, &arg[2..]);
                }
                _ if arg.starts_with("--codegen=") => {
                    record_codegen_opt(&mut parsed, &arg["--codegen=".len()..]);
                }
                // rustc defines these shorthands as exact -C aliases. Model
                // them in the same ordered bucket so last-wins combinations
                // such as `-O -Copt-level=0` cannot collide with the reverse.
                "-O" => {
                    parsed
                        .codegen_opts
                        .push(("opt-level".to_string(), Some("3".to_string())));
                }
                "-g" => {
                    parsed
                        .codegen_opts
                        .push(("debuginfo".to_string(), Some("2".to_string())));
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
                "--remap-path-prefix" => {
                    i += 1;
                    if let Some(value) = rustc_args.get(i) {
                        parsed.remap_path_prefixes.push(value.clone());
                    }
                }
                _ if arg.starts_with("--remap-path-prefix=") => {
                    parsed
                        .remap_path_prefixes
                        .push(arg["--remap-path-prefix=".len()..].to_string());
                }
                "--print" | "--explain" => {
                    is_query = true;
                    i = i.saturating_add(1); // skip the value argument
                }
                _ if arg.starts_with("--print=") || arg.starts_with("--explain=") => {
                    is_query = true;
                }
                "-V" | "--version" | "-h" | "--help" | "-vV" => {
                    is_query = true;
                }
                // Outcome-affecting lint configuration: capture flag + value for the
                // cache key before the generic diagnostics drop below. Must
                // precede the IGNORED_* arms — classification is first-match
                // (review finding #2).
                _ if OUTCOME_AFFECTING_VALUE_FLAGS.contains(&arg.as_str()) => {
                    parsed.outcome_lint_flags.push(arg.clone());
                    if let Some(value) = rustc_args.get(i + 1) {
                        parsed.outcome_lint_flags.push(value.clone());
                    }
                    i = i.saturating_add(1); // skip the value argument
                }
                _ if OUTCOME_LINT_ATTACHED_PREFIXES
                    .iter()
                    .any(|p| arg.starts_with(p)) =>
                {
                    parsed.outcome_lint_flags.push(arg.clone());
                }
                // Diagnostics / lint / query flags: never change the artifact,
                // so drop them (and their separated value) before the residual
                // catch-all (kunobi-ninja/kache#324).
                _ if IGNORED_VALUE_FLAGS.contains(&arg.as_str()) => {
                    i += 1; // skip the value argument
                }
                _ if IGNORED_BOOL_FLAGS.contains(&arg.as_str()) => {}
                _ if IGNORED_ATTACHED_PREFIXES.iter().any(|p| arg.starts_with(p)) => {}
                // Positional argument: source file (doesn't start with -)
                _ if !arg.starts_with('-')
                    && parsed.source_file.is_none()
                    && (arg.ends_with(".rs") || std::path::Path::new(arg).exists()) =>
                {
                    parsed.source_file = Some(PathBuf::from(arg));
                }
                // Anything else is an argv token kache does not model. It may
                // affect codegen, so keep it for the cache key (folded
                // normalized + sorted in `cache_key.rs`) rather than dropping
                // it silently (kunobi-ninja/kache#324).
                _ => {
                    parsed.residual_args.push(arg.clone());
                }
            }
            i += 1;
            // Every iteration must consume at least the token it just
            // classified. An arm that moves `i` backwards instead of forward
            // would spin here forever, growing whatever vector it pushes to
            // until the machine runs out of memory — a failure mode no test
            // can catch, since there is nothing to fail. Debug-only, so the
            // release parse is unchanged.
            debug_assert!(
                i > i_before,
                "argv parse must advance past index {i_before}"
            );
        }

        parsed.features.sort();
        parsed.is_primary =
            !is_query && parsed.crate_name.is_some() && parsed.source_file.is_some();
        parsed.path_normalization_root = std::env::current_dir()
            .ok()
            .map(|cwd| parsed.select_path_normalization_root(&cwd));

        Ok(parsed)
    }

    /// Original compact argv. This differs from [`Self::all_args`] only after
    /// a response file was expanded successfully.
    pub(crate) fn raw_args(&self) -> &[String] {
        self.raw_args.as_deref().unwrap_or(&self.all_args)
    }

    /// Whether cached child processes should receive the effective snapshot
    /// through a Kache-owned response file.
    pub(crate) fn has_expanded_argfiles(&self) -> bool {
        self.argfile_state == RustcArgfileState::Expanded
    }

    /// Whether expansion failed and the invocation must remain uncached.
    pub(crate) fn argfile_expansion_failed(&self) -> bool {
        self.argfile_state == RustcArgfileState::Unsupported
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
        self.target_dir()
            .and_then(|t| t.parent().map(Path::to_path_buf))
    }

    /// Return the target-derived workspace root only when it is consistent
    /// with Cargo's compiler working directory.
    ///
    /// An external `CARGO_TARGET_DIR` makes [`Self::workspace_root`] point at
    /// the target's parent rather than the source workspace. Treating that as
    /// a relocatable source root can alias unrelated files. Requiring a
    /// manifest at the candidate and the compiler cwd beneath it fails closed
    /// for external/shared targets while retaining normal workspace layouts.
    pub fn verified_workspace_root(&self, cwd: &Path) -> Option<PathBuf> {
        let candidate = self.workspace_root()?;
        if !candidate.join("Cargo.toml").is_file() {
            return None;
        }
        let canonical_candidate = candidate.canonicalize().ok()?;
        let canonical_cwd = cwd.canonicalize().ok()?;
        canonical_cwd
            .starts_with(&canonical_candidate)
            .then(|| std::path::absolute(&candidate).unwrap_or(candidate))
    }

    /// Workspace anchor shared by cache-key construction and rustc remapping.
    ///
    /// Cargo's output-derived candidate is valid for an in-workspace target,
    /// but an external `CARGO_TARGET_DIR` makes it point at the target's parent.
    /// Fall back to the compiler working directory in that case so key and
    /// artifact remapping never treat an unrelated target parent as sources.
    fn select_path_normalization_root(&self, cwd: &Path) -> PathBuf {
        self.verified_workspace_root(cwd)
            .unwrap_or_else(|| cwd.to_path_buf())
    }

    /// Frozen root shared by the key, rustc invocation, and dep-info rewrite.
    pub fn path_normalization_root(&self) -> Option<&Path> {
        self.path_normalization_root.as_deref()
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
        let is_cross = self.target.is_some();
        if let Some(od) = &self.out_dir {
            let mut p = od.parent()?;
            p = p.parent()?;
            if is_cross {
                p = p.parent()?;
            }
            return Some(p.to_path_buf());
        }
        let out = self.output.as_deref()?;
        let mut cursor = out.parent();
        while let Some(dir) = cursor {
            if let Some(name) = dir.file_name()
                && (name == "deps" || name == "build")
            {
                let mut p = dir.parent()?;
                p = p.parent()?;
                if is_cross {
                    p = p.parent()?;
                }
                return Some(p.to_path_buf());
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

    /// Output filename stem (`crate_name` + optional `extra_filename`).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn output_stem(&self) -> Option<String> {
        Some(format_crate_output_stem(
            self.crate_name.as_ref()?,
            self.extra_filename.as_deref().unwrap_or(""),
        ))
    }

    /// Path of the Cargo-facing rustc dep-info output for this invocation.
    ///
    /// Cargo's normal invocation uses `<out-dir>/<crate><extra>.d`. Explicit
    /// `--emit=dep-info=<path>` and `-o` forms are direct-rustc layouts that the
    /// cache restore model does not preserve; those invocations pass through
    /// and retain their caller-owned freshness.
    pub fn dep_info_path(&self) -> Option<PathBuf> {
        if !self.emit.iter().any(|kind| kind == "dep-info") {
            return None;
        }
        if self.dep_info_output.is_some() {
            return None;
        }
        if self.output.is_some() {
            return None;
        }
        let name = self.crate_name.as_deref()?;
        let file_name = format!(
            "{}.d",
            format_crate_output_stem(name, self.extra_filename.as_deref().unwrap_or(""))
        );
        self.out_dir.as_ref().map(|dir| dir.join(file_name))
    }

    /// Cargo checksum freshness asks rustc to annotate every dep-info input.
    /// Directories cannot carry those annotations, so extra-input directory
    /// watches must currently reject this mode instead of staying perpetually
    /// dirty or silently missing additions.
    pub fn checksum_freshness_enabled(&self) -> bool {
        self.unstable_flags
            .iter()
            .any(|flag| flag.starts_with("checksum-hash-algorithm"))
    }

    /// This compile's own unit identity, for diagnostics only
    /// (kunobi-ninja/kache#627). `None` when cargo passed no `-C extra-filename`,
    /// which is the same case [`unit_id_from_artifact_path`] cannot resolve from
    /// the consumer side — so the two sides go unidentified together, and
    /// `why-miss` falls back to matching by crate name.
    pub fn unit_id(&self) -> Option<String> {
        unit_id_from_extra_filename(self.extra_filename.as_deref()?)
    }

    /// Whether this compilation has coverage instrumentation enabled (-C instrument-coverage).
    /// When active, path remapping must be skipped so coverage tools (tarpaulin, llvm-cov)
    /// can map profraw data back to source files.
    pub fn has_coverage_instrumentation(&self) -> bool {
        self.codegen_opts
            .iter()
            .any(|(k, _)| k == "instrument-coverage")
    }

    /// Whether kache should skip injecting its own `--remap-path-prefix` flags
    /// for this compile — either because coverage instrumentation needs real
    /// paths in the profraw, or because the user opted out via
    /// `KACHE_RUSTC_PATH_NORMALIZE=0` (kunobi-ninja/kache#480).
    ///
    /// Single source of truth for the injection decision
    /// ([`crate::compiler::rustc`]) and the cache-key `remap:` fold
    /// ([`crate::cache_key`]) — both MUST agree, or the key would claim one
    /// remap state while the binary was built with the other.
    pub fn skip_path_remap(&self) -> bool {
        self.has_coverage_instrumentation() || self.path_normalize_disabled
    }

    /// Get a codegen option value by key.
    pub fn get_codegen_opt(&self, key: &str) -> Option<&str> {
        self.codegen_opts
            .iter()
            .rev()
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

fn record_codegen_opt(parsed: &mut RustcArgs, value: &str) {
    let (key, value) = parse_codegen_opt(value);
    if key == "extra-filename" {
        parsed.extra_filename = value.clone();
    }
    if key == "incremental" {
        parsed.incremental = value.as_ref().map(PathBuf::from);
    }
    parsed.codegen_opts.push((key, value));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Outcome-affecting lint configuration must be captured exactly — flag AND
    /// value, separated and attached spellings — and must not leak into the
    /// residual catch-all. Asserting the captured tokens directly (not just
    /// "the key changed") pins the parse indices: a wrong skip or a wrong
    /// `get(i ± 1)` would capture the wrong token pair.
    #[test]
    fn outcome_lint_gates_capture_flag_and_value() {
        let parse = |extra: &[&str]| {
            let mut argv = vec![
                "rustc".to_string(),
                "--crate-name".to_string(),
                "m".to_string(),
            ];
            argv.extend(extra.iter().map(|s| s.to_string()));
            RustcArgs::parse(&argv).unwrap()
        };

        let separated = parse(&["-D", "warnings"]);
        assert_eq!(
            separated.outcome_lint_flags,
            ["-D", "warnings"],
            "separated gate must capture flag then value"
        );
        assert!(
            separated.residual_args.is_empty(),
            "gate tokens must not leak into residual: {:?}",
            separated.residual_args
        );

        let attached = parse(&["-Dwarnings"]);
        assert_eq!(attached.outcome_lint_flags, ["-Dwarnings"]);

        let long = parse(&["--cap-lints", "allow"]);
        assert_eq!(long.outcome_lint_flags, ["--cap-lints", "allow"]);
        assert!(long.residual_args.is_empty());

        let long_attached = parse(&["--forbid=unused"]);
        assert_eq!(long_attached.outcome_lint_flags, ["--forbid=unused"]);

        let remaining_forms: &[(&[&str], &[&str])] = &[
            (&["-W", "unused"], &["-W", "unused"]),
            (&["-Wunused"], &["-Wunused"]),
            (&["--warn", "unused"], &["--warn", "unused"]),
            (&["--warn=unused"], &["--warn=unused"]),
            (&["-A", "dead_code"], &["-A", "dead_code"]),
            (&["-Adead_code"], &["-Adead_code"]),
            (&["--allow", "dead_code"], &["--allow", "dead_code"]),
            (&["--allow=dead_code"], &["--allow=dead_code"]),
            (
                &["--check-cfg", "cfg(feature, values(\"extra\"))"],
                &["--check-cfg", "cfg(feature, values(\"extra\"))"],
            ),
            (&["--check-cfg=cfg(test)"], &["--check-cfg=cfg(test)"]),
        ];
        for (argv, expected) in remaining_forms {
            let parsed = parse(argv);
            assert_eq!(
                parsed.outcome_lint_flags, *expected,
                "wrong outcome capture for {argv:?}"
            );
            assert!(
                parsed.residual_args.is_empty(),
                "outcome flags leaked into residual for {argv:?}: {:?}",
                parsed.residual_args
            );
        }
    }

    /// The two sides of the #627 join have to agree: what a producer records
    /// for itself (`-C extra-filename`) must equal what a consumer recovers
    /// from the artifact filename cargo built with that flag.
    #[test]
    fn unit_id_round_trips_between_the_producer_and_its_artifact() {
        let producer = unit_id_from_extra_filename("-843f02d6a46ebef1").unwrap();
        for artifact in [
            "/w/target/debug/deps/librust_check-843f02d6a46ebef1.rmeta",
            "/w/target/debug/deps/librust_check-843f02d6a46ebef1.rlib",
            "/w/target/debug/deps/librust_check-843f02d6a46ebef1.dylib",
            r"C:\w\target\debug\deps\librust_check-843f02d6a46ebef1.rlib",
        ] {
            assert_eq!(
                unit_id_from_artifact_path(Path::new(artifact)).as_deref(),
                Some(producer.as_str()),
                "{artifact}"
            );
        }
    }

    #[test]
    fn unit_id_declines_paths_without_a_cargo_hash_suffix() {
        // A sysroot crate, and a crate whose file name merely contains a dash:
        // inventing an identity for either would be worse than falling back to
        // matching by name.
        for artifact in [
            "/toolchain/lib/rustlib/x86_64/lib/libstd.rlib",
            "/w/target/debug/deps/libfoo-bar.rlib",
            "/w/target/debug/deps/libfoo-123.rlib",
        ] {
            assert_eq!(
                unit_id_from_artifact_path(Path::new(artifact)),
                None,
                "{artifact}"
            );
        }
        assert_eq!(unit_id_from_extra_filename(""), None);
        assert_eq!(unit_id_from_extra_filename("-"), None);
    }

    #[test]
    fn unit_id_reads_the_parsed_extra_filename() {
        let args: Vec<String> = ["rustc", "rustc", "--crate-name", "foo", "src/lib.rs"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let mut parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(parsed.unit_id(), None, "no -C extra-filename, no identity");

        parsed.extra_filename = Some("-d44c553abc12".to_string());
        assert_eq!(parsed.unit_id().as_deref(), Some("d44c553abc12"));
    }

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
    fn rustc_response_file_is_expanded_before_parsing() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("lib.rs");
        std::fs::write(&source, "pub fn answer() -> u32 { 42 }\n").unwrap();
        let response = dir.path().join("rustc.args");
        std::fs::write(
            &response,
            format!(
                "--crate-name\nresponse_file\n{}\n--crate-type\nlib\n-C\nopt-level=2\n",
                source.display()
            ),
        )
        .unwrap();

        let parsed =
            RustcArgs::parse(&["rustc".to_string(), format!("@{}", response.display())]).unwrap();

        assert!(parsed.is_primary);
        assert_eq!(parsed.crate_name.as_deref(), Some("response_file"));
        assert_eq!(parsed.source_file.as_deref(), Some(source.as_path()));
        assert_eq!(parsed.get_codegen_opt("opt-level"), Some("2"));
        assert!(parsed.has_expanded_argfiles());
        assert_eq!(parsed.raw_args(), &[format!("@{}", response.display())]);
        assert!(
            !parsed
                .all_args
                .iter()
                .any(|arg| arg == &format!("@{}", response.display()))
        );
    }

    #[test]
    fn rustc_response_file_line_semantics_match_rustc() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("nested.args");
        std::fs::write(&nested, "must-not-be-expanded\n").unwrap();
        let response = dir.path().join("rustc.args");
        std::fs::write(
            &response,
            format!(
                "--crate-name\r\nfoo\r\n\r\n  spaced  \r\n@{}\r\n",
                nested.display()
            ),
        )
        .unwrap();

        let parsed =
            RustcArgs::parse(&["rustc".to_string(), format!("@{}", response.display())]).unwrap();

        assert_eq!(
            parsed.all_args,
            vec![
                "--crate-name".to_string(),
                "foo".to_string(),
                String::new(),
                "  spaced  ".to_string(),
                format!("@{}", nested.display()),
            ]
        );
    }

    #[test]
    fn invalid_utf8_response_file_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let response = dir.path().join("invalid.args");
        std::fs::write(&response, [0xff, 0xfe]).unwrap();
        let raw = vec![
            "rustc".to_string(),
            "--crate-name".to_string(),
            "foo".to_string(),
            "src/lib.rs".to_string(),
            format!("@{}", response.display()),
        ];

        let parsed = RustcArgs::parse(&raw).unwrap();
        assert!(parsed.argfile_expansion_failed());
        assert!(!parsed.has_expanded_argfiles());
        assert_eq!(parsed.all_args, raw[1..]);
        assert_eq!(parsed.raw_args(), &raw[1..]);
    }

    #[test]
    fn shell_style_response_file_fails_closed() {
        for option in ["-Zshell-argfiles", "-Zshell-argfiles=yes"] {
            let raw = vec![
                "rustc".to_string(),
                option.to_string(),
                "@shell:rustc.args".to_string(),
            ];

            let parsed = RustcArgs::parse(&raw).unwrap();
            assert!(parsed.argfile_expansion_failed(), "option: {option}");
            assert_eq!(parsed.all_args, raw[1..], "option: {option}");
        }
    }

    #[test]
    fn shell_argfiles_option_recognition_is_precise() {
        assert!(RustcArgfileExpander::is_shell_argfiles_option(
            "shell-argfiles"
        ));
        assert!(RustcArgfileExpander::is_shell_argfiles_option(
            "shell-argfiles=yes"
        ));
        assert!(!RustcArgfileExpander::is_shell_argfiles_option(
            "other-option"
        ));
        assert!(!RustcArgfileExpander::is_shell_argfiles_option(
            "shell-argfiles-extra"
        ));
    }

    #[test]
    fn regular_response_file_expands_with_shell_mode_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let response = dir.path().join("rustc.args");
        std::fs::write(&response, "--crate-name\nfoo\nsrc/lib.rs\n").unwrap();
        let raw = vec![
            "rustc".to_string(),
            "-Zshell-argfiles".to_string(),
            format!("@{}", response.display()),
        ];

        let parsed = RustcArgs::parse(&raw).unwrap();
        assert!(parsed.has_expanded_argfiles());
        assert!(!parsed.argfile_expansion_failed());
        assert_eq!(parsed.crate_name.as_deref(), Some("foo"));
        assert_eq!(parsed.source_file.as_deref(), Some(Path::new("src/lib.rs")));
    }

    #[test]
    fn response_file_with_unrepresentable_direct_arg_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let response = dir.path().join("rustc.args");
        std::fs::write(&response, "--crate-name\nfoo\n").unwrap();

        for argument in ["line\nbreak", "carriage\rreturn"] {
            let raw = vec![
                "rustc".to_string(),
                argument.to_string(),
                format!("@{}", response.display()),
            ];

            let parsed = RustcArgs::parse(&raw).unwrap();
            assert!(parsed.argfile_expansion_failed(), "argument: {argument:?}");
            assert_eq!(parsed.all_args, raw[1..], "argument: {argument:?}");
        }
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
    fn test_parse_codegen_shorthands_preserves_override_order() {
        let args: Vec<String> = vec![
            "rustc",
            "src/lib.rs",
            "-O",
            "--codegen=opt-level=0",
            "--codegen",
            "debuginfo=0",
            "-g",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();

        assert_eq!(
            parsed.codegen_opts,
            vec![
                ("opt-level".to_string(), Some("3".to_string())),
                ("opt-level".to_string(), Some("0".to_string())),
                ("debuginfo".to_string(), Some("0".to_string())),
                ("debuginfo".to_string(), Some("2".to_string())),
            ]
        );
        assert_eq!(parsed.get_codegen_opt("opt-level"), Some("0"));
        assert_eq!(parsed.get_codegen_opt("debuginfo"), Some("2"));
        assert!(parsed.residual_args.is_empty());
    }

    #[test]
    fn test_parse_direct_remap_path_prefixes() {
        let args: Vec<String> = vec![
            "rustc",
            "src/lib.rs",
            "--remap-path-prefix",
            "/work/a=/src",
            "--remap-path-prefix=/work/b=/generated",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();

        assert_eq!(
            parsed.remap_path_prefixes,
            vec!["/work/a=/src".to_string(), "/work/b=/generated".to_string()]
        );
        assert!(parsed.residual_args.is_empty());
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
    fn test_format_crate_output_stem() {
        assert_eq!(format_crate_output_stem("serde", "-9f2a1b"), "serde-9f2a1b");
        assert_eq!(format_crate_output_stem("serde", ""), "serde");
        assert_eq!(format_crate_output_stem("app", "-abc"), "app-abc");
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
    fn test_parse_double_wrapper_windows_exe() {
        // Regression for issue #287: the double-wrapper split keys off
        // `RustcCompiler::recognizes(&args[1..])`, so it must also fire when
        // the inner rustc is a Windows `.exe` path. Before the `.exe`/backslash
        // fix, `clippy-driver.exe` was not recognized at all — and here the
        // inner `rustc.exe` would likewise have been missed, mis-parsing the
        // inner compiler as a positional source file. Holds on every host OS.
        let args: Vec<String> = vec![
            r"G:\.rustup\toolchains\nightly-x86_64-pc-windows-msvc\bin\clippy-driver.exe",
            r"C:\Program Files\Rust\bin\rustc.exe",
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
            PathBuf::from(
                r"G:\.rustup\toolchains\nightly-x86_64-pc-windows-msvc\bin\clippy-driver.exe"
            )
        );
        assert_eq!(
            parsed.inner_rustc,
            Some(PathBuf::from(r"C:\Program Files\Rust\bin\rustc.exe"))
        );
        assert_eq!(parsed.crate_name.as_deref(), Some("foo"));
        // The inner rustc.exe must be consumed by the split, not left in
        // all_args where it could be mistaken for a source positional.
        assert!(!parsed.all_args.iter().any(|a| a.contains("rustc.exe")));
    }

    #[test]
    fn test_parse_double_wrapper_unrecognized_driver() {
        // Issue #505: dylint-driver (or any future RUSTC_WORKSPACE_WRAPPER
        // tool) in the double-wrapper chain. The split keys off the inner
        // rustc, so an unrecognized workspace wrapper is forwarded correctly.
        let args: Vec<String> = vec![
            "/Users/dev/.dylint_drivers/nightly/dylint-driver",
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
            PathBuf::from("/Users/dev/.dylint_drivers/nightly/dylint-driver")
        );
        assert_eq!(
            parsed.inner_rustc,
            Some(PathBuf::from(
                "/home/user/.rustup/toolchains/stable/bin/rustc"
            ))
        );
        assert_eq!(parsed.crate_name.as_deref(), Some("foo"));
        assert!(!parsed.all_args.iter().any(|a| a.contains("rustc")));
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
    fn dep_info_path_uses_cargo_output_stem() {
        let args = RustcArgs {
            crate_name: Some("serde".to_string()),
            extra_filename: Some("-abc123".to_string()),
            out_dir: Some(PathBuf::from("/work/proj/target/debug/deps")),
            emit: vec!["dep-info".to_string(), "metadata".to_string()],
            ..Default::default()
        };
        assert_eq!(
            args.dep_info_path(),
            Some(PathBuf::from("/work/proj/target/debug/deps/serde-abc123.d"))
        );
    }

    #[test]
    fn dep_info_path_skips_explicit_emit_path() {
        let args: Vec<String> = [
            "rustc",
            "--crate-name",
            "x",
            "src/lib.rs",
            "--emit=metadata,dep-info=custom/deps.mk",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let parsed = RustcArgs::parse(&args).unwrap();
        assert_eq!(
            parsed.dep_info_output,
            Some(PathBuf::from("custom/deps.mk"))
        );
        assert_eq!(parsed.dep_info_path(), None);
    }

    #[test]
    fn dep_info_path_is_none_when_not_requested() {
        let args = RustcArgs {
            crate_name: Some("x".to_string()),
            out_dir: Some(PathBuf::from("/tmp/out")),
            emit: vec!["metadata".to_string()],
            ..Default::default()
        };
        assert_eq!(args.dep_info_path(), None);
    }

    #[test]
    fn dep_info_path_does_not_guess_direct_rustc_o_naming() {
        for emit in [vec!["dep-info"], vec!["dep-info", "link"]] {
            let args = RustcArgs {
                crate_name: Some("x".to_string()),
                output: Some(PathBuf::from("named.bin")),
                emit: emit.into_iter().map(str::to_string).collect(),
                ..Default::default()
            };
            assert_eq!(args.dep_info_path(), None);
        }
    }

    #[test]
    fn dep_info_path_skips_stdout_and_out_dir_plus_o_forms() {
        let stdout = RustcArgs {
            emit: vec!["dep-info".to_string()],
            dep_info_output: Some(PathBuf::from("-")),
            ..Default::default()
        };
        assert_eq!(stdout.dep_info_path(), None);

        let overridden = RustcArgs {
            crate_name: Some("x".to_string()),
            output: Some(PathBuf::from("named.bin")),
            out_dir: Some(PathBuf::from("out")),
            emit: vec!["dep-info".to_string(), "link".to_string()],
            ..Default::default()
        };
        assert_eq!(overridden.dep_info_path(), None);
    }

    #[test]
    fn parses_checksum_freshness_rustc_flag() {
        let args: Vec<String> = [
            "rustc",
            "src/lib.rs",
            "-Z",
            "checksum-hash-algorithm=blake3",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert!(
            RustcArgs::parse(&args)
                .unwrap()
                .checksum_freshness_enabled()
        );
    }

    #[test]
    fn test_target_dir_cross_compiling() {
        let args = RustcArgs {
            target: Some("aarch64-unknown-linux-gnu".to_string()),
            out_dir: Some(PathBuf::from(
                "/work/proj/target/aarch64-unknown-linux-gnu/debug/deps",
            )),
            ..Default::default()
        };
        assert_eq!(args.target_dir(), Some(PathBuf::from("/work/proj/target")));
    }

    #[test]
    fn test_workspace_root_cross_compiling() {
        let args = RustcArgs {
            target: Some("aarch64-unknown-linux-gnu".to_string()),
            out_dir: Some(PathBuf::from(
                "/work/proj/target/aarch64-unknown-linux-gnu/debug/deps",
            )),
            ..Default::default()
        };
        assert_eq!(args.workspace_root(), Some(PathBuf::from("/work/proj")));
    }

    #[test]
    fn verified_workspace_root_rejects_external_target_parents() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let member = workspace.join("member");
        std::fs::create_dir_all(&member).unwrap();
        std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n").unwrap();

        let local = RustcArgs {
            out_dir: Some(workspace.join("target/debug/deps")),
            ..Default::default()
        };
        assert_eq!(
            local.verified_workspace_root(&member),
            Some(workspace.clone())
        );
        assert_eq!(local.select_path_normalization_root(&member), workspace);

        let external = RustcArgs {
            out_dir: Some(dir.path().join("external/shared-target/debug/deps")),
            ..Default::default()
        };
        assert_eq!(external.verified_workspace_root(&member), None);
        assert_eq!(external.select_path_normalization_root(&member), member);
    }

    #[test]
    fn path_normalization_root_exposes_the_frozen_root() {
        // The key, the rustc invocation, and the dep-info rewrite all read this
        // one accessor. If it stops reporting the frozen root they silently
        // disagree about which anchor a source path is relative to, so a
        // relocated hit rewrites dep-info against the wrong tree.
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let member = workspace.join("member");
        std::fs::create_dir_all(&member).unwrap();
        std::fs::write(workspace.join("Cargo.toml"), "[workspace]\n").unwrap();

        let mut args = RustcArgs {
            out_dir: Some(workspace.join("target/debug/deps")),
            ..Default::default()
        };
        assert_eq!(
            args.path_normalization_root(),
            None,
            "unset until the parse freezes it"
        );

        args.path_normalization_root = Some(args.select_path_normalization_root(&member));
        assert_eq!(
            args.path_normalization_root(),
            Some(workspace.as_path()),
            "the frozen workspace anchor must be readable back"
        );
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
