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
pub const PROBE_SCHEMA_VERSION: u32 = 4;

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
        })
    }
}

/// Compiler family detected via `-E` preprocessing probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbedFamily {
    Gnu,
    Clang,
}

/// Probe an unknown binary via `-E -P -x c -` to detect its compiler family.
///
/// Pipes a small C snippet to stdin containing `#if defined(__clang__)`
/// / `#elif defined(__GNUC__)` markers and scans the preprocessor output.
///
/// Results are memoized in the existing probe cache under prober id
/// `"cc-family"`. No changes to `ResolvedConfig` — the family string
/// is stored in the `version_line` field of the existing record format.
///
/// Returns `None` if the binary isn't a recognized C compiler.
pub fn probe_compiler_family(program: &str) -> Option<ProbedFamily> {
    // Avoid parsing the full TOML config just to get the cache directory on the fast path.
    let cache_dir = std::env::var_os("KACHE_CACHE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(crate::config::default_cache_dir);

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
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let mut child_cmd = Command::new(program);
    #[cfg(windows)]
    {
        // `CreateProcess` cannot launch batch files directly.  CC/CXX are
        // commonly configured with `.cmd`/`.bat` wrappers on Windows, so
        // invoke those through cmd.exe while preserving the probe arguments.
        let is_batch = std::path::Path::new(program)
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("cmd") || ext.eq_ignore_ascii_case("bat"));
        if is_batch {
            child_cmd = Command::new("cmd.exe");
            child_cmd.args(["/D", "/C"]);
            child_cmd.arg(format!("\"{program}\" -E -P -x c -"));
        } else {
            child_cmd.args(["-E", "-P", "-x", "c", "-"]);
        }
    }
    #[cfg(not(windows))]
    child_cmd.args(["-E", "-P", "-x", "c", "-"]);
    child_cmd
        .env("KACHE_FAMILY_PROBE_ACTIVE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    crate::platform::configure_detached_process(&mut child_cmd);
    let mut child = match child_cmd.spawn() {
        Ok(c) => c,
        Err(_) => return Err(()),
    };
    let pid = child.id();

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(FAMILY_PROBE_SOURCE);
        drop(stdin);
    }

    let mut stdout_handle = match child.stdout.take() {
        Some(s) => s,
        None => return Err(()),
    };
    let (tx, rx) = std::sync::mpsc::channel();

    let tx_read = tx.clone();
    std::thread::spawn(move || {
        let mut buf = vec![0u8; 8192];
        let mut nread = 0;
        loop {
            if nread == buf.len() {
                break;
            }
            match stdout_handle.read(&mut buf[nread..]) {
                Ok(0) => break,
                Ok(n) => nread += n,
                Err(_) => break,
            }
        }
        buf.truncate(nread);
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
        match (output.is_some(), exit_status.is_some()) {
            (true, true) => break,
            _ if start.elapsed() >= timeout => break,
            _ => {}
        }
        let remaining = timeout.saturating_sub(start.elapsed());
        match rx.recv_timeout(remaining) {
            Ok(Ok(buf)) => output = Some(buf),
            Ok(Err(status)) => exit_status = Some(status),
            Err(_) => break,
        }
    }

    if output.is_none() || exit_status.is_none() || exit_status.unwrap().is_none() {
        crate::platform::kill_process_group(pid);
        return Err(());
    }

    let status = exit_status.unwrap().unwrap();
    if !status.success() {
        return Ok(None);
    }

    let output_buf = output.unwrap();
    let stdout_str = String::from_utf8_lossy(&output_buf);
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

    #[test]
    fn cc_prober_resolves_the_invocation_with_flags() {
        // Forks `cc -### -O2 -x c -c <file>`. On clang this resolves a
        // `-cc1` line; on gcc the resolved-line shape differs and
        // `resolved_tokens` is `None` until the gcc prober lands — so
        // the token assertion only runs when resolution succeeded.
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
            return;
        };
        if let Some(tokens) = config.resolved_tokens {
            assert!(
                tokens.iter().any(|t| t == "-O2"),
                "resolved `-cc1` tokens should carry -O2: {tokens:?}"
            );
        }
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

    struct TestCacheDirGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
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

        let res1 = probe_compiler_family("cc");
        if res1.is_none() {
            return;
        }

        // Locate the cached file on disk.
        let files: Vec<_> = std::fs::read_dir(temp.path().join("probes"))
            .unwrap()
            .map(|r| r.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        let cached_path = &files[0];

        // Read the file, modify the family, and write it back.
        let bytes = std::fs::read(cached_path).unwrap();
        let mut hit: ResolvedConfig = serde_json::from_slice(&bytes).unwrap();

        // Invert the family in the cached record.
        let original_family = hit.version_line.clone();
        let inverted_family = if original_family == "clang" {
            "gnu"
        } else {
            "clang"
        };
        hit.version_line = inverted_family.to_string();

        std::fs::write(cached_path, serde_json::to_vec(&hit).unwrap()).unwrap();

        // Call the probe again. It should return the inverted family from the cache hit!
        let res2 = probe_compiler_family("cc").unwrap();
        assert_ne!(res1.unwrap(), res2);
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
            std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path
        }
        #[cfg(windows)]
        {
            let path = dir.join(format!("{name}.bat"));
            std::fs::write(&path, format!("@echo off\r\n{body}\r\n")).unwrap();
            path
        }
    }

    #[test]
    fn family_probe_executes_scripts_and_parses_outputs() {
        let temp = TempDir::new().unwrap();
        let _guard = set_test_cache_dir(temp.path());

        // 1. Script emitting GNU marker
        let gnu_script = create_mock_probe_script(temp.path(), "mock_gnu", "echo KACHE_PROBE_GNU");
        let gnu_str = gnu_script.to_str().unwrap();
        assert_eq!(probe_compiler_family(gnu_str), Some(ProbedFamily::Gnu));
        assert_eq!(probe_compiler_family(gnu_str), Some(ProbedFamily::Gnu));

        // 2. Script emitting Clang marker
        let clang_script =
            create_mock_probe_script(temp.path(), "mock_clang", "echo KACHE_PROBE_CLANG");
        let clang_str = clang_script.to_str().unwrap();
        assert_eq!(probe_compiler_family(clang_str), Some(ProbedFamily::Clang));
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
        let large_body = if cfg!(windows) {
            "echo KACHE_PROBE_GNU\r\nfor /L %%i in (1,1,200) do echo 01234567890123456789012345678901234567890123456789"
        } else {
            "echo KACHE_PROBE_GNU\nyes '0123456789012345678901234567890123456789' | head -n 300"
        };
        let script = create_mock_probe_script(temp.path(), "mock_large", large_body);
        let res = run_family_probe(script.to_str().unwrap());
        assert_eq!(res, Ok(Some(ProbedFamily::Gnu)));
    }
}
