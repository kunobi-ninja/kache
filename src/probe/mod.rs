//! Compiler probe memoization.
//!
//! A "probe" is the act of asking a compiler about itself — today,
//! running `<cc> --version` to capture its version-stamped identity
//! line for the cache key. A probe's result depends only on the
//! compiler *binary*, so it is identical for every translation unit in
//! a build.
//!
//! kache runs as a fresh process per compile line (`CC=kache cc ...`),
//! so without memoization a 2000-file build would fork `cc --version`
//! 2000 times for 2000 identical answers. This module turns that into
//! one probe per build: the first process to need a compiler's config
//! runs the probe and writes a content-addressed record under the
//! cache dir; every later process reads that record instead.
//!
//! ## Why a file, not a daemon round-trip
//!
//! The record is a small JSON file. After the first write the kernel
//! page cache holds it in RAM, so every subsequent read is a
//! RAM-speed `read()` with no IPC and no dependency on the daemon
//! being alive. A regular file *is* the shared-memory area across the
//! build's processes — the kernel deduplicates it.
//!
//! ## Correctness
//!
//! A record is bound to the exact compiler binary via a `stat`
//! fingerprint (path + size + mtime, plus ctime + inode on Unix). Any
//! compiler change — an upgrade, or even a mtime-preserving `cp -p`
//! swap, which still bumps ctime — changes the key, so a stale record
//! is simply never looked up. [`ResolvedConfig::schema_version`] guards
//! against a record written by a different kache version being
//! mis-read.
//!
//! A probe-cache fault is never a compile fault: if the cache cannot be
//! keyed, read, or written, [`probe`] just runs the probe directly.
//!
//! ## Plugin seam
//!
//! [`Prober`] is the extension point. [`CcProber`] handles the
//! C-family compilers today; a `RustcProber`, or compiler-specific
//! probers that also capture the resolved `cc -###` invocation, slot
//! in behind the same trait without touching callers.

mod cache;
mod resolve;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Command;

/// Schema version of a [`ResolvedConfig`] record. Bump whenever the
/// struct's shape or the probe logic changes in a way that would make
/// an old on-disk record wrong: a mismatch turns the record into a
/// cache miss (re-probe), never a wrong hit.
pub const PROBE_SCHEMA_VERSION: u32 = 5;

/// The memoized result of probing a compiler.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedConfig {
    /// Schema of this record — see [`PROBE_SCHEMA_VERSION`].
    pub schema_version: u32,
    /// Id of the [`Prober`] that produced this record (`"cc"`). Lets
    /// one cache dir hold records from multiple probers safely.
    pub prober: String,
    /// `file_name` of the compiler executable, e.g. `clang`.
    pub compiler_name: String,
    /// First line of `<cc> --version` — the version-stamped identity
    /// string. gcc, clang and Apple clang each emit a distinct line.
    pub version_line: String,
    /// Codegen-semantic tokens of the resolved `cc -###` invocation —
    /// the driver's fully-expanded `-cc1` line with host-local paths
    /// sentinelled (see [`resolve`]). `None` when `-###` produced no
    /// resolvable compile line.
    pub resolved_tokens: Option<Vec<String>>,
    /// First line of the **host** compiler's `--version` output — the
    /// C++ compiler a driver compiler (today: `nvcc`) delegates code
    /// generation to. A host upgrade changes emitted objects with the
    /// driver itself unchanged, so drivers that need it fold this into
    /// the cache key alongside [`Self::version_line`]. `None` for
    /// probers without a host compiler (cc probes the compiler itself).
    pub host_version_line: Option<String>,
}

/// What to probe.
pub struct ProbeRequest<'a> {
    /// The compiler as named on the command line: `cc`, `clang-17`, or
    /// a path like `/usr/bin/gcc`.
    pub compiler: &'a str,
    /// Full compile arguments. `cc -###` is run with these so the
    /// driver resolves exactly what the real compile would.
    pub args: &'a [String],
    /// The configuration-identifying subset of `args` — per-TU noise
    /// (source files, `-o`, dep-file flags) removed. The probe cache
    /// is keyed on this, so every TU of a build that shares a flag set
    /// shares one resolved-invocation record.
    pub key_args: &'a [String],
    /// Per-TU path strings (this invocation's source, output, dep-file
    /// paths) to blank out of the resolved tokens. Because the record is
    /// SHARED across the build's TUs (keyed by `key_args`), a per-TU path
    /// left in the tokens would make the record TU-specific — and under
    /// `make -j` the TUs race over whose paths the first-probing TU stored,
    /// corrupting other TUs' cache keys. Blanking them keeps the record
    /// invariant. Empty for callers that have no per-TU paths to hide.
    pub per_tu_paths: &'a [String],
    /// Whether the resolved-invocation path sentinel should recognise
    /// absolute Windows paths (drive / UNC). True for gnu/clang (their
    /// objects are remapped via `-ffile-prefix-map`, so blanking host
    /// paths in the key is portable); **false for clang-cl**, whose
    /// objects keep raw native paths, so its key stays path-literal /
    /// machine-local (#299/#312). POSIX `/…` is always sentinelled.
    pub windows_aware: bool,
}

/// A compiler-family-specific probe strategy — the plugin seam.
pub trait Prober {
    /// Short, stable identifier, stored in the record and mixed into
    /// the cache key so different probers never collide.
    fn id(&self) -> &'static str;

    /// Run the probe. This forks the compiler; [`probe`] calls it at
    /// most once per compiler binary per build.
    fn probe(&self, req: &ProbeRequest<'_>) -> Result<ResolvedConfig>;
}

/// Prober for the C-family compilers (`cc`, `gcc`, `clang`, …).
pub struct CcProber;

impl Prober for CcProber {
    fn id(&self) -> &'static str {
        "cc"
    }

    fn probe(&self, req: &ProbeRequest<'_>) -> Result<ResolvedConfig> {
        // Compiler identity — `cc --version`.
        let output = Command::new(req.compiler)
            .env("LC_ALL", "C")
            .arg("--version")
            .output()
            .with_context(|| format!("running `{} --version`", req.compiler))?;
        if !output.status.success() {
            anyhow::bail!("`{} --version` exited {}", req.compiler, output.status);
        }
        let version_line = String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or("unknown")
            .to_string();
        let compiler_name = Path::new(req.compiler)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(req.compiler)
            .to_string();

        Ok(ResolvedConfig {
            schema_version: PROBE_SCHEMA_VERSION,
            prober: self.id().to_string(),
            compiler_name,
            version_line,
            resolved_tokens: resolve_invocation(
                req.compiler,
                req.args,
                req.windows_aware,
                req.per_tu_paths,
            ),
            host_version_line: None,
        })
    }
}

/// Prober for the CUDA `nvcc` driver (kunobi-ninja/kache#1024).
///
/// Captures the two version identities an nvcc cache key needs, both of
/// which depend only on installed toolchains (never on the TU), so the
/// record stays shareable across a build's translation units:
///
/// 1. nvcc's own version (`nvcc --version`).
/// 2. the **host** compiler's version — nvcc drives gcc/clang/cl for
///    host code, and a host upgrade changes emitted objects with nvcc
///    itself unchanged. The host binary is discovered by scanning
///    `nvcc --dryrun` output for the first known host-compiler command
///    (any subcommand line names the same driver binary, so no `-c`
///    requirement); its version is the first combined-output line of
///    `<host> --version` (exit status ignored — `cl` banners to stderr
///    with a nonzero status).
///
/// Deliberately NOT hashed: the `--dryrun` token stream itself. Its
/// install- and temp-path scrubbing is unvalidated against real nvcc,
/// and a missed temp path would make keys unstable (zero hits, silently)
/// while versions + flags + the `-M` content closure already cover the
/// dispatch space (same driver version + same flags = same plan).
/// Revisit with real-nvcc fixtures before hashing any of it.
///
/// Any failure — missing binary, failing `--version`/`--dryrun`, no
/// recognizable host, empty host banner — fails the probe, and the
/// caller passes the compile through uncached. A probe fault is never
/// a compile fault, and never a guessed key.
pub struct NvccProber;

impl Prober for NvccProber {
    fn id(&self) -> &'static str {
        "nvcc"
    }

    fn probe(&self, req: &ProbeRequest<'_>) -> Result<ResolvedConfig> {
        let version_line = nvcc_version_line(req.compiler)?;
        // Mirror the real compile's argv so dispatch resolves exactly as
        // it would for the TU. Nothing from this output is hashed (see
        // above) — only the host binary path is read out of it.
        let mut dryrun_args = vec!["--dryrun".to_string()];
        dryrun_args.extend(req.args.iter().cloned());
        let dryrun = Command::new(req.compiler)
            .env("LC_ALL", "C")
            .args(&dryrun_args)
            .output()
            .with_context(|| format!("running `{} --dryrun`", req.compiler))?;
        if !dryrun.status.success() {
            anyhow::bail!("`{} --dryrun` exited {}", req.compiler, dryrun.status);
        }
        // The plan goes to stderr on real toolkits (stdout carries only
        // the banner); scan both, stdout first. Only the host binary
        // path is read out — nothing here is hashed (see above).
        let dryrun_text = format!(
            "{}\n{}",
            String::from_utf8_lossy(&dryrun.stdout),
            String::from_utf8_lossy(&dryrun.stderr)
        );
        let host = find_nvcc_host_compiler(&dryrun_text).with_context(|| {
            // Include the raw output (truncated): host discovery runs
            // against real driver text exactly once per toolchain, and a
            // format drift is otherwise undebuggable from the reason alone.
            let mut shown: String =
                dryrun_text.chars().take(1200).collect();
            if dryrun_text.len() > shown.len() {
                shown.push_str("…[truncated]");
            }
            format!(
                "`{} --dryrun` names no recognizable host compiler\n--- combined output ---\n{shown}",
                req.compiler
            )
        })?;
        let host_version_line = host_version_line(&host)?;
        let compiler_name = Path::new(req.compiler)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(req.compiler)
            .to_string();

        Ok(ResolvedConfig {
            schema_version: PROBE_SCHEMA_VERSION,
            prober: self.id().to_string(),
            compiler_name,
            version_line,
            resolved_tokens: None,
            host_version_line: Some(host_version_line),
        })
    }
}

/// First line of `nvcc --version` (stdout). Mirrors [`CcProber`]: the
/// command must succeed and name a version.
fn nvcc_version_line(nvcc: &str) -> Result<String> {
    let output = Command::new(nvcc)
        .env("LC_ALL", "C")
        .arg("--version")
        .output()
        .with_context(|| format!("running `{nvcc} --version`"))?;
    if !output.status.success() {
        anyhow::bail!("`{nvcc} --version` exited {}", output.status);
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .next()
        .unwrap_or("unknown")
        .to_string())
}

/// Host-compiler driver names nvcc may dispatch to, matched against the
/// command basename (directories and a `.exe` suffix stripped,
/// case-insensitive). Deliberately bare names: version-suffixed and
/// target-prefixed spellings resolve through [`is_nvcc_host_command`].
const NVCC_HOST_COMPILERS: &[&str] = &["gcc", "g++", "clang", "clang++", "cc", "c++", "cl"];

/// Numeric `MAJOR[.MINOR...]` version suffix (`gcc-13`, `clang++-17`).
fn strip_nvcc_host_version(name: &str) -> &str {
    match name.rsplit_once('-') {
        Some((head, suffix))
            if !suffix.is_empty()
                && suffix
                    .split('.')
                    .all(|c| !c.is_empty() && c.bytes().all(|b| b.is_ascii_digit())) =>
        {
            head
        }
        _ => name,
    }
}

/// Is this `nvcc --dryrun` command word a known host-compiler driver?
/// Bare and versioned names match directly; target-prefixed cross
/// toolchains (`aarch64-linux-gnu-gcc`) match on the trailing driver
/// name. Anything else — nvcc itself, `cudafe++`, `cicc`, `ptxas`,
/// `fatbinary`, wrappers — is not a host compiler.
fn is_nvcc_host_command(command: &str) -> bool {
    let Some(base) = command.rsplit(['/', '\\']).next().filter(|n| !n.is_empty()) else {
        return false;
    };
    let base = base
        .strip_suffix(".exe")
        .or_else(|| base.strip_suffix(".EXE"))
        .unwrap_or(base);
    let bare = strip_nvcc_host_version(base);
    if NVCC_HOST_COMPILERS
        .iter()
        .any(|known| bare.eq_ignore_ascii_case(known))
    {
        return true;
    }
    bare.rsplit('-').next().is_some_and(|tail| {
        tail != bare
            && NVCC_HOST_COMPILERS
                .iter()
                .any(|k| tail.eq_ignore_ascii_case(k))
    })
}

/// Find the host compiler binary in `nvcc --dryrun` output: the first
/// output line whose command basename is a known host driver. Returns
/// the command exactly as nvcc spelled it (path included) so the
/// version probe runs that same binary.
fn find_nvcc_host_compiler(dryrun: &str) -> Option<String> {
    dryrun.lines().find_map(|line| {
        let line = line.trim();
        let line = line.strip_prefix("#$").unwrap_or(line).trim();
        let command = line.split_whitespace().next()?;
        if is_nvcc_host_command(command) {
            Some(command.to_string())
        } else {
            None
        }
    })
}

/// First non-empty combined-output line of `<host> --version`. Exit
/// status is ignored: `cl` banners to stderr and exits nonzero, which
/// still identifies it. Empty output means unidentifiable — bail.
fn host_version_line(host: &str) -> Result<String> {
    let output = Command::new(host)
        .env("LC_ALL", "C")
        .arg("--version")
        .output()
        .with_context(|| format!("running `{host} --version`"))?;
    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    combined
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_string)
        .with_context(|| format!("`{host} --version` produced no version line"))
}

/// Compiler family detected via `-E` preprocessing probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbedFamily {
    Gnu,
    Clang,
}

/// Probe an unknown binary via `-E -P -x c <source-file>` to detect its
/// compiler family.
///
/// Writes a small C snippet containing `#if defined(__clang__)` /
/// `#elif defined(__GNUC__)` markers to a temporary source file and scans
/// the preprocessor output.
///
/// Results are memoized in the existing probe cache under prober id
/// `"cc-family"`. No changes to `ResolvedConfig` — the family string
/// is stored in the `version_line` field of the existing record format.
///
/// Cache directory for the family probe, straight from the environment.
///
/// Avoids parsing the full TOML config just to get the cache directory on
/// the fast path — but expands a tilde exactly like `Config::load` would.
/// Taking the env value verbatim wrote probe records under a literal
/// `./~/probes/` for values the shell never expanded (systemd Environment=
/// files, and the tilde-expansion tests' env windows in CI) (#673).
fn family_probe_cache_dir() -> std::path::PathBuf {
    std::env::var_os("KACHE_CACHE_DIR")
        .map(|s| crate::config::shellexpand(&s.to_string_lossy()))
        .unwrap_or_else(crate::config::default_cache_dir)
}

/// Returns `None` if the binary isn't a recognized C compiler.
pub fn probe_compiler_family(program: &str) -> Option<ProbedFamily> {
    let cache_dir = family_probe_cache_dir();

    let key = cache::probe_key_isolated("cc-family", program);

    // Cache hit: read family from version_line.
    if let Some(ref k) = key
        && let Some(hit) = cache::load(&cache_dir, k)
    {
        match hit.version_line.as_str() {
            "clang" => return Some(ProbedFamily::Clang),
            "gnu" => return Some(ProbedFamily::Gnu),
            "none" => return None, // Cached negative!
            _ => {}                // Invalid/corrupted, treat as miss and re-probe
        }
    }

    // Miss: run the probe.
    let family = run_family_probe(program);
    let family_str = match family {
        Ok(Some(ProbedFamily::Clang)) => "clang",
        Ok(Some(ProbedFamily::Gnu)) => "gnu",
        Ok(None) => "none",
        Err(_) => return None, // Do not cache transient failures
    };

    // Store in the existing probe cache. Family (or "none") is encoded in
    // version_line — no ResolvedConfig changes needed.
    if let Some(ref k) = key {
        cache::store(
            &cache_dir,
            k,
            &ResolvedConfig {
                schema_version: PROBE_SCHEMA_VERSION,
                prober: "cc-family".to_string(),
                host_version_line: None,
                compiler_name: std::path::Path::new(program)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or(program)
                    .to_string(),
                version_line: family_str.to_string(),
                resolved_tokens: None,
            },
        );
    }

    match family {
        Ok(Some(f)) => Some(f),
        _ => None,
    }
}

const FAMILY_PROBE_SOURCE: &[u8] = b"\
#if defined(__clang__)\n\
KACHE_PROBE_CLANG\n\
#elif defined(__GNUC__)\n\
KACHE_PROBE_GNU\n\
#endif\n";

fn run_family_probe(program: &str) -> Result<Option<ProbedFamily>, ()> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    // Use a file rather than stdin: Windows batch wrappers pass ordinary
    // arguments through reliably, but stdin is consumed by cmd.exe instead
    // of reaching the compiler invoked by the wrapper.
    let source_file = tempfile::NamedTempFile::new().map_err(|_| ())?;
    std::fs::write(source_file.path(), FAMILY_PROBE_SOURCE).map_err(|_| ())?;

    let mut child_cmd = Command::new(program);
    child_cmd
        .args(["-E", "-P", "-x", "c"])
        .arg(source_file.path())
        .env("LC_ALL", "C")
        .env("KACHE_FAMILY_PROBE_ACTIVE", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    crate::platform::configure_process_group(&mut child_cmd);
    let mut child = match child_cmd.spawn() {
        Ok(c) => c,
        Err(_) => return Err(()),
    };
    let pid = child.id();

    let mut stdout_handle = match child.stdout.take() {
        Some(s) => s,
        None => return Err(()),
    };
    let (tx, rx) = std::sync::mpsc::channel();

    let tx_read = tx.clone();
    std::thread::spawn(move || {
        // Drain the child's stdout to EOF, keeping up to MAX_PROBE_OUTPUT for
        // marker scanning. Draining (rather than stopping at a fixed prefix)
        // matters twofold: the family marker can appear anywhere in the output —
        // shell stdio buffering can reorder a builtin `echo` marker behind a
        // child pipeline's bytes — and a child that keeps writing into a pipe we
        // stopped reading would block or die on SIGPIPE mid-write. The cap bounds
        // memory against a pathological program; past it we stop and let the
        // family stay undetected (the probe then falls back to a normal compile).
        const MAX_PROBE_OUTPUT: usize = 1 << 20; // 1 MiB
        let mut buf = Vec::with_capacity(8192);
        let mut chunk = [0u8; 8192];
        loop {
            match stdout_handle.read(&mut chunk) {
                Ok(0) => break,
                // Keep reading to EOF even past the cap (retaining only the
                // first MAX_PROBE_OUTPUT for marker scanning): stopping mid-write
                // would leave the child to die on SIGPIPE, whose non-success exit
                // would then discard an already-captured family marker.
                Ok(n) => {
                    let retain = n.min(MAX_PROBE_OUTPUT.saturating_sub(buf.len()));
                    buf.extend_from_slice(&chunk[..retain]);
                }
                Err(_) => break,
            }
        }
        let _ = tx_read.send(Ok(buf));
    });

    let tx_wait = tx.clone();
    std::thread::spawn(move || {
        let status = child.wait().ok();
        let _ = tx_wait.send(Err(status));
    });

    let mut output = None;
    let mut exit_status = None;
    let start = Instant::now();
    let timeout = Duration::from_secs(5);

    loop {
        if matches!((output.is_some(), exit_status.is_some()), (true, true)) {
            break;
        }
        if start.elapsed() >= timeout {
            break;
        }
        let remaining = timeout.saturating_sub(start.elapsed());
        match rx.recv_timeout(remaining) {
            Ok(Ok(buf)) => output = Some(buf),
            Ok(Err(status)) => exit_status = Some(status),
            Err(_) => break,
        }
    }

    let Some(output_buf) = output.as_ref() else {
        crate::platform::kill_process_group(pid);
        return Err(());
    };
    let Some(Some(status)) = exit_status.as_ref() else {
        crate::platform::kill_process_group(pid);
        return Err(());
    };
    if !status.success() {
        return Ok(None);
    }

    let stdout_str = String::from_utf8_lossy(output_buf);
    let clang = stdout_str.contains("KACHE_PROBE_CLANG");
    let gnu = stdout_str.contains("KACHE_PROBE_GNU");
    match (clang, gnu) {
        (true, false) => Ok(Some(ProbedFamily::Clang)),
        (false, true) => Ok(Some(ProbedFamily::Gnu)),
        _ => Ok(None),
    }
}

/// Run `cc -### <args>` and reduce the resolved `-cc1` invocation to
/// its codegen-semantic token list.
///
/// `-###` prints the fully-resolved command lines to stderr without
/// compiling. Returns `None` on any failure — a missing compiler, a
/// non-zero exit (bad flags), or output with no `-cc1` line. The probe
/// degrades to "no resolved invocation"; it never turns a `-###`
/// hiccup into a hard error.
fn resolve_invocation(
    compiler: &str,
    args: &[String],
    windows_aware: bool,
    per_tu_paths: &[String],
) -> Option<Vec<String>> {
    let output = Command::new(compiler)
        .env("LC_ALL", "C")
        .arg("-###")
        .args(args)
        .output()
        .ok()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    let resolved = resolve::resolved_semantic_tokens(&stderr, windows_aware, per_tu_paths);
    if resolved.is_none() {
        // Every unresolvable probe looks identical from the outside: the
        // caller refuses with "resolved invocation unavailable" and the one
        // fact that would explain it — what shape `-###` actually printed —
        // was discarded here. That cost four CI rounds on #580's Windows
        // failure, where gcc quoted the `cc1.exe` path and neither extractor
        // matched. `stdout_lines` is worth recording too: a driver shim that
        // prints the resolved command to stdout leaves stderr empty, which is
        // otherwise indistinguishable from an unrecognised shape.
        tracing::debug!(
            compiler,
            exit_code = ?output.status.code(),
            stderr_lines = stderr.lines().count(),
            stdout_lines = output.stdout.iter().filter(|b| **b == b'\n').count(),
            "cc -### resolved no cc1 line; probe-captured flags will refuse. head:\n{}",
            probe_stderr_head(&stderr)
        );
    }
    resolved
}

/// A bounded, log-safe head of `-###` stderr.
///
/// `-###` output is unbounded (gcc's `Configured with:` line alone runs to
/// several KB), so the head is clipped on both axes before it reaches a log
/// line. Clipping is on char boundaries, since the output carries filesystem
/// paths that need not be ASCII.
fn probe_stderr_head(stderr: &str) -> String {
    const MAX_LINES: usize = 12;
    const MAX_CHARS: usize = 300;
    stderr
        .lines()
        .take(MAX_LINES)
        .map(|line| {
            let line = line.trim();
            match line.char_indices().nth(MAX_CHARS) {
                Some((cut, _)) => format!("{}…", &line[..cut]),
                None => line.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// What running the compiler probe against the live toolchain found.
///
/// Distinguishes "no compiler" (nothing to diagnose) from "compiler present
/// but `-###` resolves no compile line". The latter makes every probe-keyed
/// C/C++ flag refuse to cache — correct but silent, which is how #607's
/// Windows regressions shipped unnoticed (#626).
#[derive(Debug)]
pub enum LiveProbeDiagnostic {
    /// `cc` is missing from PATH; there is no system C toolchain to diagnose.
    NoCompiler,
    /// `cc` exists, but the diagnostic itself could not run reliably. Kept
    /// distinct from [`Self::NoCompiler`]: only genuine absence is
    /// informational, everything else can hide exactly the silent caching
    /// loss this check exists to expose (#626).
    ProbeError { detail: String },
    /// `cc -###` resolved a compile line: probe-keyed flags can cache.
    Resolved { version_line: String },
    /// The compiler runs but its `-###` output yielded no compile line:
    /// probe-keyed flags will refuse to cache on this toolchain.
    Unresolved {
        version_line: String,
        stderr_head: String,
    },
}

/// Run the compiler probe against the live toolchain, for `kache doctor`.
///
/// Calls [`CcProber`] directly rather than through [`probe`]: the on-disk
/// record may hold `resolved_tokens: None` from before a toolchain change,
/// and doctor's job is to report what the compiler does NOW (#626).
pub fn live_probe_diagnostic() -> LiveProbeDiagnostic {
    // A `cc` shim may route `-###` through Kache's fallback wrapper (such as
    // sccache), which expects an object that the dry run never creates. Probe
    // the compiler Kache actually invokes behind the shim.
    match crate::compiler::shim::resolve_real_compiler_from_env("cc") {
        Some(compiler) => live_probe_diagnostic_for(&compiler.to_string_lossy()),
        None => live_probe_diagnostic_for("cc"),
    }
}

/// [`live_probe_diagnostic`] against an explicit compiler, so the
/// absent / broken / unresolvable classifications are testable with
/// stand-in compilers.
fn live_probe_diagnostic_for(compiler: &str) -> LiveProbeDiagnostic {
    // Absence is the one informational state; every other failure below is a
    // diagnostic that could not run and must surface as such.
    match Command::new(compiler)
        .env("LC_ALL", "C")
        .arg("--version")
        .output()
    {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return LiveProbeDiagnostic::NoCompiler;
        }
        Err(error) => {
            return LiveProbeDiagnostic::ProbeError {
                detail: format!("could not run `cc --version`: {error}"),
            };
        }
        Ok(output) if !output.status.success() => {
            return LiveProbeDiagnostic::ProbeError {
                detail: format!("`{compiler} --version` exited {}", output.status),
            };
        }
        Ok(_) => {}
    }
    let dir = match tempfile::tempdir() {
        Ok(dir) => dir,
        Err(error) => {
            return LiveProbeDiagnostic::ProbeError {
                detail: format!("could not create compiler-probe directory: {error}"),
            };
        }
    };
    let source = dir.path().join("kache-doctor-probe.c");
    if let Err(error) = std::fs::write(&source, "int kache_doctor_probe(void) { return 0; }\n") {
        return LiveProbeDiagnostic::ProbeError {
            detail: format!("could not write compiler-probe source: {error}"),
        };
    }
    let args: Vec<String> = ["-O2", "-x", "c", "-c"]
        .iter()
        .map(|s| s.to_string())
        .chain([source.to_string_lossy().into_owned()])
        .collect();
    let req = ProbeRequest {
        compiler,
        args: &args,
        key_args: &args,
        per_tu_paths: &[],
        windows_aware: true,
    };
    let config = match CcProber.probe(&req) {
        Ok(config) => config,
        Err(error) => {
            return LiveProbeDiagnostic::ProbeError {
                detail: format!("compiler probe failed: {error:#}"),
            };
        }
    };
    if config.resolved_tokens.is_some() {
        return LiveProbeDiagnostic::Resolved {
            version_line: config.version_line,
        };
    }
    // The prober discards the raw `-###` output; re-run it for the head,
    // which is the one fact an "unresolvable probe" report needs.
    let stderr_head = Command::new(compiler)
        .env("LC_ALL", "C")
        .arg("-###")
        .args(&args)
        .output()
        .map(|o| probe_stderr_head(&String::from_utf8_lossy(&o.stderr)))
        .unwrap_or_default();
    LiveProbeDiagnostic::Unresolved {
        version_line: config.version_line,
        stderr_head,
    }
}

/// Probe a compiler, memoized through an on-disk cache under
/// `cache_dir`.
///
/// The first call for a given compiler binary runs `prober` and writes
/// a content-addressed record; later calls — this process or any
/// other — read the record. Resilient: if the cache cannot be keyed or
/// read the probe simply runs directly. A probe-cache fault never
/// fails a compile.
pub fn probe(
    cache_dir: &Path,
    prober: &dyn Prober,
    req: &ProbeRequest<'_>,
) -> Result<ResolvedConfig> {
    let key = cache::probe_key(prober.id(), req);

    if let Some(key) = &key
        && let Some(hit) = cache::load(cache_dir, key)
    {
        return Ok(hit);
    }

    // Miss, or the probe could not be keyed: run the real probe.
    crate::opcounts::record_probe_run();
    let config = prober.probe(req)?;

    if let Some(key) = &key {
        cache::store(cache_dir, key, &config);
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_family(s: &str) -> Option<ProbedFamily> {
        match s {
            "clang" => Some(ProbedFamily::Clang),
            "gnu" => Some(ProbedFamily::Gnu),
            _ => None,
        }
    }
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::{NamedTempFile, TempDir};

    /// A `Prober` that records how many times it actually ran — lets a
    /// test prove memoization without forking a real compiler.
    #[derive(Default)]
    struct CountingProber {
        runs: AtomicUsize,
    }

    impl Prober for CountingProber {
        fn id(&self) -> &'static str {
            "test"
        }

        fn probe(&self, _req: &ProbeRequest<'_>) -> Result<ResolvedConfig> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(ResolvedConfig {
                schema_version: PROBE_SCHEMA_VERSION,
                prober: "test".to_string(),
                compiler_name: "fake".to_string(),
                version_line: "fake 1.0".to_string(),
                resolved_tokens: None,
                host_version_line: None,
            })
        }
    }

    /// A `ProbeRequest` for a compiler with no extra arguments.
    fn req(compiler: &str) -> ProbeRequest<'_> {
        ProbeRequest {
            compiler,
            args: &[],
            key_args: &[],
            per_tu_paths: &[],
            windows_aware: true,
        }
    }

    #[test]
    fn probe_runs_prober_once_then_serves_from_cache() {
        let _lock = crate::config::config_path_lock();
        let cache = TempDir::new().unwrap();
        // A real, stat-able file stands in for the compiler binary —
        // the CountingProber never actually execs it.
        let compiler = NamedTempFile::new().unwrap();
        let prober = CountingProber::default();
        let req = req(compiler.path().to_str().unwrap());

        let first = probe(cache.path(), &prober, &req).unwrap();
        let second = probe(cache.path(), &prober, &req).unwrap();

        assert_eq!(first, second, "memoized result must match the original");
        assert_eq!(
            prober.runs.load(Ordering::SeqCst),
            1,
            "second probe must be served from the on-disk cache"
        );
    }

    #[test]
    fn probe_falls_back_to_running_when_compiler_is_unresolvable() {
        let _lock = crate::config::config_path_lock();
        // A path that doesn't exist cannot be keyed, so every call
        // re-probes — but each call still succeeds. Correctness is
        // never sacrificed for memoization.
        let cache = TempDir::new().unwrap();
        let prober = CountingProber::default();
        let req = req("/nonexistent/kache-probe-test-cc");

        let _ = probe(cache.path(), &prober, &req).unwrap();
        let _ = probe(cache.path(), &prober, &req).unwrap();

        assert_eq!(
            prober.runs.load(Ordering::SeqCst),
            2,
            "an unkeyable probe is not memoized — both calls run"
        );
    }

    #[test]
    fn cc_prober_has_stable_id() {
        assert_eq!(CcProber.id(), "cc");
    }

    #[test]
    fn cc_prober_reads_a_real_compiler_version() {
        // Forks `cc --version`. Every dev box and CI runner that builds
        // kache has a C compiler; if `cc` is somehow absent, skip
        // rather than fail.
        let Ok(config) = CcProber.probe(&req("cc")) else {
            return;
        };
        assert!(
            !config.version_line.is_empty(),
            "version line should be populated"
        );
        assert_eq!(config.prober, "cc");
        assert_eq!(config.schema_version, PROBE_SCHEMA_VERSION);
    }

    /// The doctor diagnostic's classification boundaries (#626): only a
    /// genuinely absent compiler is `NoCompiler`; a present-but-broken one is
    /// a `ProbeError` that doctor must flag, never silently downgrade.
    #[test]
    fn live_probe_diagnostic_classifies_an_absent_compiler() {
        match live_probe_diagnostic_for("kache-test-definitely-not-a-compiler") {
            LiveProbeDiagnostic::NoCompiler => {}
            other => panic!("absent compiler must classify NoCompiler, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn live_probe_diagnostic_flags_a_broken_compiler_as_probe_error() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();

        // Present but not executable: spawn fails with a non-NotFound error.
        let unexecutable = dir.path().join("cc-unexecutable");
        kache_fs::testutil::write_executable(&unexecutable, "#!/bin/sh\nexit 0\n");
        std::fs::set_permissions(&unexecutable, std::fs::Permissions::from_mode(0o644)).unwrap();
        match live_probe_diagnostic_for(&unexecutable.to_string_lossy()) {
            LiveProbeDiagnostic::ProbeError { detail } => {
                assert!(
                    detail.contains("could not run"),
                    "unexpected detail: {detail}"
                );
            }
            other => panic!("unexecutable compiler must be ProbeError, got {other:?}"),
        }

        // Present and executable but --version fails.
        let failing = dir.path().join("cc-version-fails");
        kache_fs::testutil::write_executable(&failing, "#!/bin/sh\nexit 1\n");
        match live_probe_diagnostic_for(&failing.to_string_lossy()) {
            LiveProbeDiagnostic::ProbeError { detail } => {
                // Specifically the PREFLIGHT's report, not the prober's later
                // "compiler probe failed" fallback: skipping the preflight
                // reaches the same variant with a different story, and the
                // preflight is what keeps the failure attributable.
                assert!(
                    detail.contains("--version` exited")
                        && !detail.contains("compiler probe failed"),
                    "expected the --version preflight to report the failure, got: {detail}"
                );
            }
            other => panic!("--version failure must be ProbeError, got {other:?}"),
        }
    }

    /// A fake compiler whose `--version` succeeds but whose `-###` resolves
    /// nothing classifies `Unresolved` — the state #626 exists to surface —
    /// and a real gcc/clang-family `cc` never lands in the error states.
    #[cfg(unix)]
    #[test]
    fn live_probe_diagnostic_classifies_unresolvable_and_real_compilers() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("cc-resolves-nothing");
        kache_fs::testutil::write_executable(&fake, "#!/bin/sh\necho fake-cc 1.0\nexit 0\n");
        match live_probe_diagnostic_for(&fake.to_string_lossy()) {
            LiveProbeDiagnostic::Unresolved { version_line, .. } => {
                assert_eq!(version_line, "fake-cc 1.0");
            }
            other => panic!("resolving nothing must be Unresolved, got {other:?}"),
        }

        // The real host compiler, when present, must reach a live verdict —
        // never NoCompiler/ProbeError (kills the inverted-success mutants).
        match live_probe_diagnostic_for("cc") {
            LiveProbeDiagnostic::Resolved { .. } | LiveProbeDiagnostic::Unresolved { .. } => {}
            LiveProbeDiagnostic::NoCompiler => eprintln!("skipping: no `cc` on PATH"),
            LiveProbeDiagnostic::ProbeError { detail } => {
                panic!("a working host `cc` must not classify as ProbeError: {detail}")
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn live_doctor_probe_skips_a_kache_cc_shim() {
        use std::os::unix::fs::symlink;

        if let Some(expected) = std::env::var_os("KACHE_DOCTOR_SHIM_TEST_CHILD") {
            match live_probe_diagnostic() {
                LiveProbeDiagnostic::Resolved { version_line } => {
                    assert_eq!(version_line, expected.to_string_lossy())
                }
                other => panic!("doctor must probe the compiler behind the shim: {other:?}"),
            }
            return;
        }

        let _lock = crate::config::config_path_lock();
        // Leave out PATH directories whose `cc` is a kache shim installed on
        // this machine. That kache is a different binary from the one under
        // test, so it does not recognize this test's shim as kache and would
        // run it as the compiler: the test would then measure the installed
        // kache, not the probe.
        let original_path = std::env::var_os("PATH").unwrap();
        let host_dirs: Vec<std::path::PathBuf> = std::env::split_paths(&original_path)
            .filter(|dir| !cc_is_an_installed_kache_shim(dir))
            .collect();
        let exe = std::env::current_exe().unwrap();
        let Some(real_cc) = crate::compiler::shim::resolve_real_compiler(
            "cc",
            &host_dirs,
            Some(&exe),
            &|candidate| is_executable_file(candidate),
            &|path| std::fs::canonicalize(path).ok(),
            &|dir| crate::compiler::shim::has_shim_marker(dir),
        ) else {
            eprintln!("skipping: no real `cc` on PATH");
            return;
        };
        let LiveProbeDiagnostic::Resolved { version_line } =
            live_probe_diagnostic_for(&real_cc.to_string_lossy())
        else {
            eprintln!("skipping: host `cc` has no resolved compile line");
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        symlink(&exe, dir.path().join("cc")).unwrap();
        let mut dirs = vec![dir.path().to_path_buf()];
        dirs.extend(host_dirs);
        let output = Command::new(exe)
            .args([
                "--exact",
                "probe::tests::live_doctor_probe_skips_a_kache_cc_shim",
            ])
            .env("PATH", std::env::join_paths(dirs).unwrap())
            .env("KACHE_DOCTOR_SHIM_TEST_CHILD", version_line)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child probe failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// Whether `dir/cc` resolves to a kache binary: a shim farm left by
    /// `kache install-shims` or a distro package.
    #[cfg(unix)]
    fn cc_is_an_installed_kache_shim(dir: &std::path::Path) -> bool {
        std::fs::canonicalize(dir.join("cc"))
            .ok()
            .and_then(|real| {
                real.file_name()
                    .map(|name| name.to_string_lossy().starts_with("kache"))
            })
            .unwrap_or(false)
    }

    #[cfg(unix)]
    fn is_executable_file(path: &std::path::Path) -> bool {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
    }

    #[cfg(unix)]
    #[test]
    fn an_installed_kache_shim_is_recognized_and_a_real_compiler_is_not() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let kache = dir.path().join("kache");
        std::fs::write(&kache, b"").unwrap();
        let shims = dir.path().join("shims");
        let toolchain = dir.path().join("toolchain");
        std::fs::create_dir_all(&shims).unwrap();
        std::fs::create_dir_all(&toolchain).unwrap();
        symlink(&kache, shims.join("cc")).unwrap();
        std::fs::write(toolchain.join("cc"), b"").unwrap();

        assert!(cc_is_an_installed_kache_shim(&shims));
        assert!(!cc_is_an_installed_kache_shim(&toolchain));
        assert!(!cc_is_an_installed_kache_shim(&dir.path().join("absent")));
    }

    #[test]
    fn cc_prober_resolves_the_invocation_with_flags() {
        // Forks `cc -### -O2 -x c -c <file>` against the LIVE host compiler.
        // Both gcc- and clang-family drivers support `-###` and both have an
        // extractor in `resolve`, so on a family driver `resolved_tokens ==
        // None` is a FAILURE, never a skip: #607 shipped with the extractors
        // resolving nothing on Windows, and this test's old "assert only if
        // Some" shape waved it through (#626). Only a missing `cc` or a
        // non-gnu/clang driver skips.
        let src = NamedTempFile::new().unwrap();
        let args: Vec<String> = ["-O2", "-x", "c", "-c", src.path().to_str().unwrap()]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let request = ProbeRequest {
            compiler: "cc",
            args: &args,
            key_args: &args,
            per_tu_paths: &[],
            windows_aware: true,
        };
        let Ok(config) = CcProber.probe(&request) else {
            eprintln!("skipping: no `cc` on PATH");
            return;
        };
        // Family via the live `-E` probe, not the disk cache: the test's
        // subject is the host compiler, not a stored record.
        let family = match run_family_probe("cc") {
            Ok(Some(f)) => f,
            _ => {
                eprintln!("skipping: `cc` is not a gcc/clang-family driver");
                return;
            }
        };
        let Some(tokens) = config.resolved_tokens else {
            let head = Command::new("cc")
                .env("LC_ALL", "C")
                .arg("-###")
                .args(&args)
                .output()
                .map(|o| probe_stderr_head(&String::from_utf8_lossy(&o.stderr)))
                .unwrap_or_else(|e| format!("(could not re-run cc -###: {e})"));
            panic!(
                "`cc -###` resolved no compile line on a {family:?}-family driver \
                 ({}); every probe-keyed flag would silently refuse to cache (#626).\n\
                 -### head:\n{head}",
                config.version_line
            );
        };
        assert!(
            tokens.iter().any(|t| t == "-O2"),
            "resolved tokens should carry -O2: {tokens:?}"
        );
    }

    /// The head is what a future "unresolvable probe" investigation reads, so
    /// it has to stay bounded on both axes: gcc's `Configured with:` line
    /// alone runs to several KB, and `-###` output is unbounded in length.
    #[test]
    fn probe_stderr_head_is_bounded_on_lines_and_chars() {
        let long_line = "x".repeat(1000);
        let many = (0..50)
            .map(|i| format!("line{i} {long_line}"))
            .collect::<Vec<_>>()
            .join("\n");
        let head = super::probe_stderr_head(&many);

        assert_eq!(head.lines().count(), 12, "line budget must be enforced");
        for line in head.lines() {
            assert!(
                line.chars().count() <= 301,
                "char budget must be enforced (300 + ellipsis): {}",
                line.chars().count()
            );
            assert!(line.ends_with('\u{2026}'), "a clipped line must say so");
        }
    }

    /// Multi-byte paths must not panic the clip. Slicing on a byte offset that
    /// is not a char boundary would.
    #[test]
    fn probe_stderr_head_clips_on_char_boundaries() {
        let wide = "é".repeat(400);
        let head = super::probe_stderr_head(&wide);
        assert!(head.chars().count() <= 301);
        assert!(head.starts_with('é'));
    }

    /// Short output passes through untouched — no ellipsis, no reflow.
    #[test]
    fn probe_stderr_head_leaves_short_output_alone() {
        let head = super::probe_stderr_head("clang version 19\nTarget: x86_64\n");
        assert_eq!(head, "clang version 19\nTarget: x86_64");
    }

    /// kunobi-ninja/kache#673: the family probe reads `KACHE_CACHE_DIR`
    /// straight from the env; a value the shell never expanded must be
    /// tilde-expanded like `Config::load` does, or probe records land in a
    /// literal `./~/probes/` directory relative to the build's cwd.
    #[test]
    fn family_probe_cache_dir_expands_tilde() {
        let Some(home) = dirs::home_dir() else {
            eprintln!("skipping: no home dir");
            return;
        };
        let lock = crate::config::config_path_lock();
        let previous = std::env::var_os("KACHE_CACHE_DIR");

        unsafe { std::env::set_var("KACHE_CACHE_DIR", "~") };
        let bare = family_probe_cache_dir();
        unsafe { std::env::set_var("KACHE_CACHE_DIR", "~/kache-cache") };
        let nested = family_probe_cache_dir();
        unsafe { std::env::set_var("KACHE_CACHE_DIR", "/abs/kache-cache") };
        let absolute = family_probe_cache_dir();

        unsafe {
            match previous.as_ref() {
                Some(prev) => std::env::set_var("KACHE_CACHE_DIR", prev),
                None => std::env::remove_var("KACHE_CACHE_DIR"),
            }
        }
        drop(lock);

        assert_eq!(bare, home, "bare tilde must expand to the home dir");
        assert_eq!(nested, home.join("kache-cache"));
        assert_eq!(absolute, std::path::PathBuf::from("/abs/kache-cache"));
    }

    struct TestCacheDirGuard {
        _lock: crate::test_support::ProcessStateTestGuard,
        previous: Option<std::ffi::OsString>,
    }

    impl Drop for TestCacheDirGuard {
        fn drop(&mut self) {
            unsafe {
                match self.previous.as_ref() {
                    Some(prev) => std::env::set_var("KACHE_CACHE_DIR", prev),
                    None => std::env::remove_var("KACHE_CACHE_DIR"),
                }
            }
        }
    }

    fn set_test_cache_dir(path: &std::path::Path) -> TestCacheDirGuard {
        let lock = crate::config::config_path_lock();
        let previous = std::env::var_os("KACHE_CACHE_DIR");
        unsafe {
            std::env::set_var("KACHE_CACHE_DIR", path);
        }
        TestCacheDirGuard {
            _lock: lock,
            previous,
        }
    }

    #[test]
    fn parse_family_handles_valid_and_invalid_inputs() {
        assert_eq!(parse_family("clang"), Some(ProbedFamily::Clang));
        assert_eq!(parse_family("gnu"), Some(ProbedFamily::Gnu));
        assert_eq!(parse_family("invalid"), None);
        assert_eq!(parse_family(""), None);
    }

    #[test]
    fn family_probe_detects_system_cc() {
        let temp = TempDir::new().unwrap();
        let _guard = set_test_cache_dir(temp.path());
        let res = probe_compiler_family("cc");
        if res.is_none() {
            return;
        }
        assert!(matches!(
            res,
            Some(ProbedFamily::Clang) | Some(ProbedFamily::Gnu)
        ));
    }

    #[test]
    fn family_probe_returns_none_for_non_compiler() {
        let temp = TempDir::new().unwrap();
        let _guard = set_test_cache_dir(temp.path());
        let res = probe_compiler_family("cargo");
        assert_eq!(res, None);
    }

    #[test]
    fn family_probe_cached_result_roundtrips() {
        let temp = TempDir::new().unwrap();
        let _guard = set_test_cache_dir(temp.path());
        let compiler =
            create_mock_probe_script(temp.path(), "mock_family_roundtrip", "echo KACHE_PROBE_GNU");
        let program = compiler.to_str().unwrap();
        // The first call executes the freshly written script; the second must read the cache.
        let res1 = probe_family_retrying(program).unwrap();
        let key = cache::probe_key_isolated("cc-family", program).unwrap();
        let mut hit = cache::load(temp.path(), &key).expect("probe result must be persisted");

        // Invert the family in the cached record.
        let original_family = hit.version_line.clone();
        let inverted_family = if original_family == "clang" {
            "gnu"
        } else {
            "clang"
        };
        hit.version_line = inverted_family.to_string();

        cache::store(temp.path(), &key, &hit);

        // Call the probe again. It should return the inverted family from the cache hit!
        let res2 = probe_compiler_family(program).unwrap();
        assert_ne!(res1, res2);
        assert_eq!(res2, parse_family(inverted_family).unwrap());
    }

    #[test]
    fn family_probe_reads_cached_gnu_clang_none_and_corrupt() {
        let temp = TempDir::new().unwrap();
        let _guard = set_test_cache_dir(temp.path());

        let compiler =
            create_mock_probe_script(temp.path(), "mock_cached_none", "echo KACHE_PROBE_GNU");
        let prog = compiler.to_str().unwrap();
        let key = cache::probe_key_isolated("cc-family", prog).unwrap();

        // 1. Cached "gnu"
        cache::store(
            temp.path(),
            &key,
            &ResolvedConfig {
                schema_version: PROBE_SCHEMA_VERSION,
                prober: "cc-family".to_string(),
                compiler_name: "dummy".to_string(),
                version_line: "gnu".to_string(),
                resolved_tokens: None,
                host_version_line: None,
            },
        );
        assert_eq!(probe_compiler_family(prog), Some(ProbedFamily::Gnu));

        // 2. Cached "clang"
        cache::store(
            temp.path(),
            &key,
            &ResolvedConfig {
                schema_version: PROBE_SCHEMA_VERSION,
                prober: "cc-family".to_string(),
                compiler_name: "dummy".to_string(),
                version_line: "clang".to_string(),
                resolved_tokens: None,
                host_version_line: None,
            },
        );
        assert_eq!(probe_compiler_family(prog), Some(ProbedFamily::Clang));

        // 3. Cached "none" (negative hit)
        cache::store(
            temp.path(),
            &key,
            &ResolvedConfig {
                schema_version: PROBE_SCHEMA_VERSION,
                prober: "cc-family".to_string(),
                compiler_name: "dummy".to_string(),
                version_line: "none".to_string(),
                resolved_tokens: None,
                host_version_line: None,
            },
        );
        assert_eq!(probe_compiler_family(prog), None);
    }

    fn create_mock_probe_script(
        dir: &std::path::Path,
        name: &str,
        body: &str,
    ) -> std::path::PathBuf {
        #[cfg(unix)]
        {
            let path = dir.join(name);
            kache_fs::testutil::write_executable(&path, format!("#!/bin/sh\n{body}\n"));
            path
        }
        #[cfg(windows)]
        {
            let path = dir.join(format!("{name}.bat"));
            std::fs::write(&path, format!("@echo off\r\n{body}\r\n")).unwrap();
            path
        }
    }

    /// `probe_compiler_family` with a brief retry (kunobi-ninja/kache#673):
    /// tests spawn a script written moments ago, and a concurrent test's
    /// fork can still hold the script's write fd open at exec time
    /// (ETXTBSY). Spawn failures are deliberately not negative-cached, so a
    /// retry re-probes. Only for call sites that EXPECT a family — a genuine
    /// misparse still fails after the retries.
    fn probe_family_retrying(program: &str) -> Option<ProbedFamily> {
        for _ in 0..10 {
            if let Some(family) = probe_compiler_family(program) {
                return Some(family);
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        probe_compiler_family(program)
    }

    #[test]
    fn family_probe_executes_scripts_and_parses_outputs() {
        let temp = TempDir::new().unwrap();
        let _guard = set_test_cache_dir(temp.path());

        // 1. Script emitting GNU marker
        let gnu_script = create_mock_probe_script(temp.path(), "mock_gnu", "echo KACHE_PROBE_GNU");
        let gnu_str = gnu_script.to_str().unwrap();
        assert_eq!(probe_family_retrying(gnu_str), Some(ProbedFamily::Gnu));
        assert_eq!(probe_compiler_family(gnu_str), Some(ProbedFamily::Gnu));

        // 2. Script emitting Clang marker
        let clang_script =
            create_mock_probe_script(temp.path(), "mock_clang", "echo KACHE_PROBE_CLANG");
        let clang_str = clang_script.to_str().unwrap();
        assert_eq!(probe_family_retrying(clang_str), Some(ProbedFamily::Clang));
        assert_eq!(probe_compiler_family(clang_str), Some(ProbedFamily::Clang));

        // 3. Script emitting BOTH markers (ambiguous)
        let both_script = create_mock_probe_script(
            temp.path(),
            "mock_both",
            if cfg!(windows) {
                "echo KACHE_PROBE_CLANG\r\necho KACHE_PROBE_GNU"
            } else {
                "echo KACHE_PROBE_CLANG\necho KACHE_PROBE_GNU"
            },
        );
        let both_str = both_script.to_str().unwrap();
        assert_eq!(probe_compiler_family(both_str), None);

        // 4. Script emitting NEITHER marker
        let unk_script = create_mock_probe_script(temp.path(), "mock_unk", "echo UNKNOWN_COMPILER");
        let unk_str = unk_script.to_str().unwrap();
        assert_eq!(probe_compiler_family(unk_str), None);

        // 5. Script exiting with non-zero status
        let fail_script = create_mock_probe_script(
            temp.path(),
            "mock_fail",
            if cfg!(windows) { "exit /b 1" } else { "exit 1" },
        );
        let fail_str = fail_script.to_str().unwrap();
        assert_eq!(probe_compiler_family(fail_str), None);
    }

    #[test]
    fn run_family_probe_handles_large_output() {
        let temp = TempDir::new().unwrap();
        // Emit more than the old 8 KiB read limit before the family marker so
        // the regression is deterministic: `run_family_probe` must drain the
        // pipe and scan the whole bounded output. Its own timeout already
        // bounds hangs without relying on CI wall-clock scheduling.
        let large_body = if cfg!(windows) {
            "for /L %%i in (1,1,200) do echo 01234567890123456789012345678901234567890123456789\r\necho KACHE_PROBE_GNU"
        } else {
            "yes '0123456789012345678901234567890123456789' | head -n 300\necho KACHE_PROBE_GNU"
        };
        let script = create_mock_probe_script(temp.path(), "mock_large", large_body);
        // Brief retry on the transient spawn-failure Err (ETXTBSY, #673);
        // a wrong Ok value fails immediately.
        let mut res = run_family_probe(script.to_str().unwrap());
        for _ in 0..10 {
            if res.is_ok() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
            res = run_family_probe(script.to_str().unwrap());
        }
        assert_eq!(res, Ok(Some(ProbedFamily::Gnu)));
    }

    #[cfg(unix)]
    #[test]
    fn probe_spawns_pin_lc_all_c() {
        let temp = TempDir::new().unwrap();
        let _cache_guard = set_test_cache_dir(temp.path());
        // This fixture is checked in, not written immediately before exec.
        // Runtime-created scripts can race another test's fork and fail with
        // ETXTBSY under coverage even after the writer itself is closed.
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/mock_cc_lc_all.sh");
        let compiler = script.to_str().unwrap();

        let req = ProbeRequest {
            compiler,
            args: &["-O2".to_string()],
            key_args: &["-O2".to_string()],
            per_tu_paths: &[],
            windows_aware: false,
        };
        let config = CcProber.probe(&req).expect("probe succeeds");
        assert_eq!(config.version_line, "mock-cc 1.0");
        assert!(config.resolved_tokens.is_some());

        let family = run_family_probe(compiler).expect("family probe succeeds");
        assert_eq!(family, Some(ProbedFamily::Gnu));

        let diag = live_probe_diagnostic_for(compiler);
        match diag {
            LiveProbeDiagnostic::Resolved { version_line } => {
                assert_eq!(version_line, "mock-cc 1.0");
            }
            other => panic!("expected Resolved diagnostic, got: {other:?}"),
        }

        // The fixture exits non-zero unless every spawn pins LC_ALL=C, so
        // reaching each successful assertion above proves the environment.
    }

    #[test]
    fn nvcc_host_command_spellings() {
        for host in [
            "gcc",
            "/usr/bin/gcc",
            "g++",
            "clang++-17",
            "gcc-13",
            "aarch64-linux-gnu-gcc",
            r"C:\VS\bin\cl.exe",
            "CL.EXE",
            "/usr/bin/cc",
        ] {
            assert!(is_nvcc_host_command(host), "{host} must match");
        }
        for other in [
            "",
            "nvcc",
            "/usr/local/cuda/bin/nvcc",
            "cudafe++",
            "cicc",
            "ptxas",
            "fatbinary",
            "my-gcc-wrapper",
            "gcc-",
            "clang-1a2",
            "ld",
            "ar",
        ] {
            assert!(!is_nvcc_host_command(other), "{other} must not match");
        }
    }

    #[test]
    fn nvcc_host_found_in_dryrun_plan() {
        let dryrun = "#$ _SPACE_= \n\
            #$ _CUDART_=cudart\n\
            #$ _HERE_=/usr/local/cuda/bin\n\
            /usr/local/cuda/bin/cudafe++ --m64 --diag_error=host --stub_file_name /tmp/tmpxft_1.cu.cpp --gen_c_file_name /tmp/tmpxft_1.cudafe1.c kernel.cu\n\
            #$ gcc -D__CUDA_ARCH__=800 -c -x c++ -o /tmp/tmpxft_1.o /tmp/tmpxft_1.cudafe1.cpp\n\
            /usr/bin/gcc -D__CUDA_ARCH__=800 -c -x c++ -o /tmp/tmpxft_1.o /tmp/tmpxft_1.cudafe1.cpp\n";
        assert_eq!(find_nvcc_host_compiler(dryrun), Some("gcc".to_string()));
        // Without the `#$` line, the full command path is preserved for
        // the version probe.
        let dryrun = dryrun
            .lines()
            .filter(|l| !l.trim_start_matches("#$ ").starts_with("gcc"))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(
            find_nvcc_host_compiler(&dryrun),
            Some("/usr/bin/gcc".to_string())
        );
        // cudafe++ lines never match, even first.
        assert_eq!(
            find_nvcc_host_compiler("/usr/local/cuda/bin/cudafe++ --m64 x.cu\n"),
            None
        );
        assert_eq!(find_nvcc_host_compiler(""), None);
        assert_eq!(find_nvcc_host_compiler("#$ _SPACE_= \n"), None);
    }

    /// Write an executable shell script. Tests that spawn it retry a
    /// transient ETXTBSY through [`retry_nvcc_probe_spawn`] instead.
    #[cfg(unix)]
    fn write_nvcc_fixture(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        kache_fs::testutil::write_executable(&path, format!("#!/bin/sh\n{body}\n"));
        path
    }

    /// ETXTBSY: a concurrent test's fork can still hold a just-written
    /// script's fd at exec time. Only spawn failures (io::Error in the
    /// chain) retry — a genuine probe refusal fails immediately.
    #[cfg(unix)]
    fn is_nvcc_spawn_busy(err: &anyhow::Error) -> bool {
        err.chain()
            .filter_map(|c| c.downcast_ref::<std::io::Error>())
            .any(|e| e.raw_os_error() == Some(libc::ETXTBSY))
    }

    #[cfg(unix)]
    fn retry_nvcc_probe_spawn(req: &ProbeRequest<'_>) -> Result<ResolvedConfig> {
        let mut result = NvccProber.probe(req);
        for _ in 0..10 {
            match &result {
                Err(error) if is_nvcc_spawn_busy(error) => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    result = NvccProber.probe(req);
                }
                _ => break,
            }
        }
        result
    }

    #[cfg(unix)]
    #[test]
    fn nvcc_probe_captures_both_versions() {
        let temp = TempDir::new().unwrap();
        let host = write_nvcc_fixture(temp.path(), "host-gcc", "printf '%s\\n' 'gcc (GCC) 13.2.0'");
        let host_str = host.to_string_lossy().into_owned();
        let nvcc = write_nvcc_fixture(
            temp.path(),
            "nvcc",
            &format!(
                "if [ \"$1\" = \"--version\" ]; then\n\
                 printf '%s\\n' 'nvcc: NVIDIA (R) Cuda compiler driver' 'Copyright (c) 2005-2024 NVIDIA Corporation' 'Cuda compilation tools, release 12.6, V12.6.77'\n\
                 elif [ \"$1\" = \"--dryrun\" ]; then\n\
                 printf '%s\\n' '#$ _HERE_=/usr/local/cuda/bin' '{host_str} -D__CUDA_ARCH__=800 -c -x c++ -o /tmp/x.o /tmp/x.cpp'\n\
                 else exit 99\nfi"
            ),
        );

        let req = ProbeRequest {
            compiler: nvcc.to_str().unwrap(),
            args: &["-c".to_string(), "k.cu".to_string()],
            key_args: &["-c".to_string()],
            per_tu_paths: &["k.cu".to_string()],
            windows_aware: false,
        };
        let config = retry_nvcc_probe_spawn(&req).expect("probe succeeds");
        assert_eq!(config.prober, "nvcc");
        assert_eq!(config.version_line, "nvcc: NVIDIA (R) Cuda compiler driver");
        assert_eq!(
            config.host_version_line,
            Some("gcc (GCC) 13.2.0".to_string())
        );
        assert_eq!(config.resolved_tokens, None);
        assert_eq!(config.schema_version, PROBE_SCHEMA_VERSION);
    }

    /// An empty-but-successful `--version` still identifies (as
    /// unknown) instead of failing: the probe needs *a* stable string,
    /// and bailing here would only cost a cacheable compile.
    #[cfg(unix)]
    #[test]
    fn nvcc_empty_version_line_is_unknown() {
        let temp = TempDir::new().unwrap();
        let quiet = write_nvcc_fixture(temp.path(), "nvcc-quiet", "exit 0");
        assert_eq!(
            nvcc_version_line(quiet.to_str().unwrap()).unwrap(),
            "unknown"
        );
    }

    #[cfg(unix)]
    #[test]
    fn nvcc_probe_fails_closed() {
        let temp = TempDir::new().unwrap();
        // --version exits nonzero.
        let bad_version = write_nvcc_fixture(temp.path(), "nvcc-badver", "exit 3");
        // --dryrun exits nonzero.
        let bad_dryrun = write_nvcc_fixture(
            temp.path(),
            "nvcc-baddry",
            "if [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'nvcc mock'; else exit 4; fi",
        );
        // --dryrun names no host compiler.
        let no_host = write_nvcc_fixture(
            temp.path(),
            "nvcc-nohost",
            "if [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'nvcc mock'; else printf '%s\\n' '#$ nothing here'; fi",
        );
        // Host banner is empty.
        let empty_host = write_nvcc_fixture(temp.path(), "host-empty", "exit 0");
        let empty_host_nvcc = write_nvcc_fixture(
            temp.path(),
            "nvcc-emptyhost",
            &format!(
                "if [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'nvcc mock'; else printf '%s\\n' '{host} -c x.o'; fi",
                host = empty_host.display()
            ),
        );
        for nvcc in [&bad_version, &bad_dryrun, &no_host, &empty_host_nvcc] {
            let req = ProbeRequest {
                compiler: nvcc.to_str().unwrap(),
                args: &[],
                key_args: &[],
                per_tu_paths: &[],
                windows_aware: false,
            };
            assert!(
                NvccProber.probe(&req).is_err(),
                "{} must fail the probe",
                nvcc.display()
            );
        }
    }

    /// The host-discovery failure carries the raw output for debugging:
    /// long output gets a truncation marker, short output does not.
    #[cfg(unix)]
    #[test]
    fn nvcc_probe_failure_marks_truncation() {
        let temp = TempDir::new().unwrap();
        // 2000 chars of host-less output: marker expected.
        let long_garbage = "x".repeat(2000);
        let long_nvcc = write_nvcc_fixture(
            temp.path(),
            "nvcc-long",
            &format!(
                "if [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'nvcc mock'; else printf '%s' '{long_garbage}'; fi"
            ),
        );
        let req = ProbeRequest {
            compiler: long_nvcc.to_str().unwrap(),
            args: &[],
            key_args: &[],
            per_tu_paths: &[],
            windows_aware: false,
        };
        let err = retry_nvcc_probe_spawn(&req).expect_err("must fail the probe");
        assert!(
            format!("{err:#}").contains("[truncated]"),
            "long output must be marked: {err:#}"
        );
        // Short output: no marker.
        let short_nvcc = write_nvcc_fixture(
            temp.path(),
            "nvcc-short",
            "if [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'nvcc mock'; else printf '%s\\n' '#$ nothing here'; fi",
        );
        let req = ProbeRequest {
            compiler: short_nvcc.to_str().unwrap(),
            args: &[],
            key_args: &[],
            per_tu_paths: &[],
            windows_aware: false,
        };
        let err = retry_nvcc_probe_spawn(&req).expect_err("must fail the probe");
        assert!(
            format!("{err:#}").contains("names no recognizable host compiler"),
            "expected host discovery failure: {err:#}"
        );
        assert!(
            !format!("{err:#}").contains("[truncated]"),
            "short output must not be marked: {err:#}"
        );
    }
}
