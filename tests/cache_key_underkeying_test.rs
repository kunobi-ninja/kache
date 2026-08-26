//! End-to-end regression tests for cache-key under-keying.
//!
//! Each test drives the real `kache` binary as a `RUSTC_WRAPPER` on a
//! controlled `rustc` invocation (a trivial rlib compile), varying ONE
//! codegen-affecting input that was previously absent from the cache key,
//! and asserts via `kache report` that:
//!
//!   - an identical re-invocation HITS (the invocation is cacheable and the
//!     key is deterministic — so the test can't pass vacuously), and
//!   - changing the input MISSES (the key diverged — no false hit).
//!
//! Before the fixes these guard, the changed-input build would have HIT the
//! original entry and silently restored a wrong artifact:
//!   - `-l` / `-L native=` — build-script `cargo:rustc-link-lib` /
//!     `cargo:rustc-link-search` reach rustc on argv, not via RUSTFLAGS.
//!   - `--sysroot` — selects which std rustc links against.
//!   - `-O` / `-g` — shorthand codegen flags whose order relative to explicit
//!     `-C` overrides is last-wins.
//!   - direct `--remap-path-prefix` — changes embedded source/debug paths.
//!   - lint configuration / `--check-cfg` — can change a compile from success
//!     to failure, which a cache hit must never replay as success.
//!
//! `-L dependency=` (cargo's rlib search, redundant with the content-hashed
//! `--extern`) must NOT affect the key, or every target-dir move would bust
//! the cache; that invariant is asserted too.
//!
//! `-Z` flags and custom `--target` JSON specs are also keyed (see the
//! `cache_key` unit tests `unstable_flag_changes_key` /
//! `target_spec_contents_change_key`); they are not exercised here because a
//! faithful e2e needs a nightly toolchain / `-Zbuild-std`.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

fn rustc_path() -> String {
    // cargo sets RUSTC to the absolute path when running tests; fall back
    // to PATH resolution otherwise.
    std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string())
}

fn rustc_sysroot() -> String {
    let out = std::process::Command::new(rustc_path())
        .args(["--print", "sysroot"])
        .output()
        .expect("run rustc --print sysroot");
    assert!(out.status.success(), "rustc --print sysroot failed");
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

fn toml_path(path: &Path) -> String {
    toml::Value::String(path.to_string_lossy().into_owned()).to_string()
}

/// Compile a trivial rlib through kache-as-RUSTC_WRAPPER with `extra` flags
/// appended to the rustc argv. Asserts the compile succeeds.
fn run_kache_rustc(cache_dir: &Path, out_dir: &Path, src: &Path, extra: &[&str]) {
    run_kache_rustc_from(cache_dir, out_dir, src, extra, None);
}

fn run_kache_rustc_from(
    cache_dir: &Path,
    out_dir: &Path,
    src: &Path,
    extra: &[&str],
    cwd: Option<&Path>,
) {
    let (args, output) = kache_rustc_output_from(cache_dir, out_dir, src, extra, cwd);

    assert!(
        output.status.success(),
        "kache rustc failed.\nargs: {args:?}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

fn kache_rustc_output_from(
    cache_dir: &Path,
    out_dir: &Path,
    src: &Path,
    extra: &[&str],
    cwd: Option<&Path>,
) -> (Vec<String>, std::process::Output) {
    let mut args: Vec<String> = vec![
        rustc_path(),
        "--crate-name".into(),
        "kt".into(),
        "--crate-type".into(),
        "lib".into(),
        "--edition".into(),
        "2021".into(),
        "--emit=link".into(),
        "--out-dir".into(),
        out_dir.display().to_string(),
        src.display().to_string(),
    ];
    args.extend(extra.iter().map(|s| s.to_string()));

    let config_path = isolated_config_path(cache_dir);
    std::fs::write(
        &config_path,
        format!(
            "[cache]\nlocal_only = true\nignore_env = true\nlocal_store = {}\nruntime_dir = {}\n",
            toml_path(cache_dir),
            toml_path(cache_dir)
        ),
    )
    .unwrap();
    let mut command = std::process::Command::new(kache_binary());
    command
        .args(&args)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", config_path)
        .env("KACHE_LOG", "kache=info")
        .env_remove("KACHE_DISABLED")
        .env_remove("KACHE_NAMESPACE")
        .env_remove("KACHE_BASE_DIR")
        .env_remove("KACHE_SOCKET_PATH")
        .env_remove("KACHE_ACTIVE")
        .env_remove("KACHE_FAMILY_PROBE_ACTIVE")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER");
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command.output().expect("failed to run kache rustc");
    (args, output)
}

/// `(compiled, local_hits)` from `kache report` over this isolated cache dir.
///
/// `dup` is still a compiler run: the entry missed, but the output blob was
/// already present. These tests are about key divergence, so they count
/// `dups + misses`.
fn compiled_hit_counts(cache_dir: &Path) -> (u64, u64) {
    let output = std::process::Command::new(kache_binary())
        .args(["report", "--format", "json", "--since", "1h"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env_remove("KACHE_DISABLED")
        .env_remove("KACHE_NAMESPACE")
        .env_remove("KACHE_BASE_DIR")
        .env_remove("KACHE_SOCKET_PATH")
        .env_remove("KACHE_ACTIVE")
        .env_remove("KACHE_FAMILY_PROBE_ACTIVE")
        .output()
        .expect("failed to run kache report");
    assert!(output.status.success(), "kache report failed");
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("report should be valid json");
    let s = &report["summary"];
    let compiled = s["dups"].as_u64().unwrap_or(0) + s["misses"].as_u64().unwrap_or(0);
    (compiled, s["local_hits"].as_u64().unwrap_or(0))
}

fn fresh_src() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("lib.rs");
    std::fs::write(&src, b"pub fn f() -> u32 { 42 }\n").unwrap();
    (dir, src)
}

#[derive(Debug, Clone, Copy)]
enum OracleCfgTransport {
    Inline,
    ResponseFile,
}

#[derive(Debug, Clone, Copy)]
enum OracleDebugOrder {
    EndsOff,
    EndsOn,
}

#[derive(Debug, Clone, Copy)]
struct OracleState {
    cfg: &'static str,
    cfg_transport: OracleCfgTransport,
    debug_order: OracleDebugOrder,
    env_value: &'static str,
    include_value: &'static str,
    module_a: u8,
    module_b: u8,
}

#[derive(Debug, Clone, Copy)]
struct OracleCase {
    name: &'static str,
    before: OracleState,
    after: OracleState,
}

#[derive(Debug, PartialEq, Eq)]
struct CompileObservation {
    status: Option<i32>,
    stdout: Vec<u8>,
}

const ORACLE_BASE: OracleState = OracleState {
    cfg: "oracle_a",
    cfg_transport: OracleCfgTransport::Inline,
    debug_order: OracleDebugOrder::EndsOff,
    env_value: "env-a",
    include_value: "include-a",
    module_a: 1,
    module_b: 2,
};

fn oracle_binary(out_dir: &Path) -> PathBuf {
    let mut binary = out_dir.join("kache-semantic-oracle");
    if cfg!(windows) {
        binary.set_extension("exe");
    }
    binary
}

/// Materialize one generated compiler-input state and return its effective
/// rustc arguments. Each matrix row below changes exactly one input dimension.
fn apply_oracle_state(root: &Path, state: OracleState) -> Vec<String> {
    std::fs::write(
        root.join("main.rs"),
        r#"
mod module_a;
mod module_b;

#[cfg(oracle_a)]
const CFG_VALUE: &str = "cfg-a";
#[cfg(oracle_b)]
const CFG_VALUE: &str = "cfg-b";

fn main() {
    print!(
        "{}|{}|{}|{}|{}:{}",
        CFG_VALUE,
        if cfg!(debug_assertions) { "debug-on" } else { "debug-off" },
        env!("KACHE_ORACLE_ENV"),
        include_str!("oracle-input.txt"),
        module_a::VALUE,
        module_b::VALUE,
    );
}
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("module_a.rs"),
        format!("pub const VALUE: u8 = {};\n", state.module_a),
    )
    .unwrap();
    std::fs::write(
        root.join("module_b.rs"),
        format!("pub const VALUE: u8 = {};\n", state.module_b),
    )
    .unwrap();
    std::fs::write(root.join("oracle-input.txt"), state.include_value).unwrap();

    let mut args = match state.cfg_transport {
        OracleCfgTransport::Inline => vec!["--cfg".to_string(), state.cfg.to_string()],
        OracleCfgTransport::ResponseFile => {
            let response = root.join("oracle.rsp");
            std::fs::write(&response, format!("--cfg\n{}\n", state.cfg)).unwrap();
            vec![format!("@{}", response.display())]
        }
    };
    match state.debug_order {
        OracleDebugOrder::EndsOff => args.extend([
            "-C".to_string(),
            "debug-assertions=on".to_string(),
            "-C".to_string(),
            "debug-assertions=off".to_string(),
        ]),
        OracleDebugOrder::EndsOn => args.extend([
            "-C".to_string(),
            "debug-assertions=off".to_string(),
            "-C".to_string(),
            "debug-assertions=on".to_string(),
        ]),
    }
    args
}

fn oracle_rustc_args(root: &Path, out_dir: &Path, extra: &[String]) -> Vec<String> {
    let mut args = vec![
        "--crate-name".to_string(),
        "kache_semantic_oracle".to_string(),
        "--crate-type".to_string(),
        "bin".to_string(),
        "--edition".to_string(),
        "2021".to_string(),
        "--emit=link".to_string(),
        "-o".to_string(),
        oracle_binary(out_dir).display().to_string(),
        root.join("main.rs").display().to_string(),
    ];
    args.extend(extra.iter().cloned());
    args
}

fn observe_oracle_compile(
    root: &Path,
    out_dir: &Path,
    state: OracleState,
    cache_dir: Option<&Path>,
) -> CompileObservation {
    std::fs::create_dir_all(out_dir).unwrap();
    let binary = oracle_binary(out_dir);
    if binary.exists() {
        std::fs::remove_file(&binary).unwrap();
    }

    let extra = apply_oracle_state(root, state);
    let rustc_args = oracle_rustc_args(root, out_dir, &extra);
    let mut command = if let Some(cache_dir) = cache_dir {
        let config_path = isolated_config_path(cache_dir);
        std::fs::write(
            &config_path,
            format!(
                "[cache]\nlocal_only = true\nignore_env = true\ncache_executables = true\nlocal_store = {}\nruntime_dir = {}\n",
                toml_path(cache_dir),
                toml_path(cache_dir)
            ),
        )
        .unwrap();
        let mut command = std::process::Command::new(kache_binary());
        command
            .arg(rustc_path())
            .args(&rustc_args)
            .env("KACHE_CACHE_DIR", cache_dir)
            .env("KACHE_CONFIG", config_path)
            .env("KACHE_LOG", "off")
            .env_remove("KACHE_DISABLED")
            .env_remove("KACHE_NAMESPACE")
            .env_remove("KACHE_BASE_DIR")
            .env_remove("KACHE_SOCKET_PATH")
            .env_remove("KACHE_ACTIVE")
            .env_remove("KACHE_FAMILY_PROBE_ACTIVE");
        command
    } else {
        let mut command = std::process::Command::new(rustc_path());
        command.args(&rustc_args);
        command
    };
    command
        .current_dir(root)
        .env("KACHE_ORACLE_ENV", state.env_value)
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER");

    let output = command.output().unwrap_or_else(|error| {
        panic!(
            "failed to run {} oracle compile: {error}",
            if cache_dir.is_some() {
                "Kache"
            } else {
                "bare rustc"
            }
        )
    });
    assert!(
        output.status.success(),
        "{} oracle compile failed.\nargs: {rustc_args:?}\nstdout: {}\nstderr: {}",
        if cache_dir.is_some() {
            "Kache"
        } else {
            "bare rustc"
        },
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let run = std::process::Command::new(&binary)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {}: {error}", binary.display()));
    assert!(
        run.status.success(),
        "oracle binary failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr),
    );
    CompileObservation {
        status: output.status.code(),
        stdout: run.stdout,
    }
}

/// A cache-key test can pass while asserting only hit/miss counters even if a
/// different bug corrupts the materialized artifact. This matrix uses bare
/// rustc as the semantic authority, seeds Kache with the before state, and
/// requires the after state to both miss and behave exactly like rustc.
#[test]
fn generated_semantic_cache_oracle_matches_bare_rustc() {
    build_kache();
    let cases = [
        OracleCase {
            name: "cfg value",
            before: ORACLE_BASE,
            after: OracleState {
                cfg: "oracle_b",
                ..ORACLE_BASE
            },
        },
        OracleCase {
            name: "last-wins codegen order",
            before: ORACLE_BASE,
            after: OracleState {
                debug_order: OracleDebugOrder::EndsOn,
                ..ORACLE_BASE
            },
        },
        OracleCase {
            name: "same-path response contents",
            before: OracleState {
                cfg_transport: OracleCfgTransport::ResponseFile,
                ..ORACLE_BASE
            },
            after: OracleState {
                cfg: "oracle_b",
                cfg_transport: OracleCfgTransport::ResponseFile,
                ..ORACLE_BASE
            },
        },
        OracleCase {
            name: "env! value",
            before: ORACLE_BASE,
            after: OracleState {
                env_value: "env-b",
                ..ORACLE_BASE
            },
        },
        OracleCase {
            name: "include_str! contents",
            before: ORACLE_BASE,
            after: OracleState {
                include_value: "include-b",
                ..ORACLE_BASE
            },
        },
        OracleCase {
            name: "module path-to-content mapping",
            before: ORACLE_BASE,
            after: OracleState {
                module_a: ORACLE_BASE.module_b,
                module_b: ORACLE_BASE.module_a,
                ..ORACLE_BASE
            },
        },
    ];

    for case in cases {
        let workspace = TempDir::new().unwrap();
        let plain_out = TempDir::new().unwrap();
        let kache_out = TempDir::new().unwrap();
        let cache_dir = TempDir::new().unwrap();

        let plain_before =
            observe_oracle_compile(workspace.path(), plain_out.path(), case.before, None);
        let plain_after =
            observe_oracle_compile(workspace.path(), plain_out.path(), case.after, None);
        assert_ne!(
            plain_before, plain_after,
            "{}: bare rustc must prove the generated perturbation is semantic",
            case.name
        );

        let kache_before = observe_oracle_compile(
            workspace.path(),
            kache_out.path(),
            case.before,
            Some(cache_dir.path()),
        );
        assert_eq!(
            kache_before, plain_before,
            "{}: cold Kache compile differs from bare rustc",
            case.name
        );
        let kache_before_hit = observe_oracle_compile(
            workspace.path(),
            kache_out.path(),
            case.before,
            Some(cache_dir.path()),
        );
        assert_eq!(
            kache_before_hit, plain_before,
            "{}: warm Kache result differs from bare rustc",
            case.name
        );
        assert_eq!(
            compiled_hit_counts(cache_dir.path()),
            (1, 1),
            "{}: the before state must prove a real cache hit",
            case.name
        );

        let kache_after = observe_oracle_compile(
            workspace.path(),
            kache_out.path(),
            case.after,
            Some(cache_dir.path()),
        );
        assert_eq!(
            kache_after, plain_after,
            "{}: Kache returned semantics different from bare rustc after the perturbation",
            case.name
        );
        assert_eq!(
            compiled_hit_counts(cache_dir.path()),
            (2, 1),
            "{}: a semantic change must compile instead of hitting the before entry",
            case.name
        );

        let kache_after_hit = observe_oracle_compile(
            workspace.path(),
            kache_out.path(),
            case.after,
            Some(cache_dir.path()),
        );
        assert_eq!(
            kache_after_hit, plain_after,
            "{}: repeated after state differs from bare rustc",
            case.name
        );
        assert_eq!(
            compiled_hit_counts(cache_dir.path()),
            (2, 2),
            "{}: the changed state must itself become a deterministic hit",
            case.name
        );
    }
}

/// `--check-cfg` defines the accepted cfg names and values. Tightening that
/// set under `-D unexpected_cfgs` must run rustc and fail, never replay the
/// success cached under the earlier accepted set.
#[test]
fn check_cfg_change_cannot_replay_cached_success() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let src_dir = TempDir::new().unwrap();
    let src = src_dir.path().join("lib.rs");
    std::fs::write(
        &src,
        b"#[cfg(accepted)]\npub fn accepted() -> bool { true }\n",
    )
    .unwrap();

    let accepted = ["-D", "unexpected_cfgs", "--check-cfg", "cfg(accepted)"];
    run_kache_rustc(cache_dir.path(), out.path(), &src, &accepted); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &accepted); // hit
    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (1, 1),
        "the accepted invocation must prove a real cache hit before the negative control"
    );

    let rejected = ["-D", "unexpected_cfgs", "--check-cfg", "cfg(other)"];
    let (args, output) =
        kache_rustc_output_from(cache_dir.path(), out.path(), &src, &rejected, None);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success()
            && stderr.contains("unexpected `cfg` condition name")
            && stderr.contains("accepted"),
        "tightened --check-cfg must execute rustc and reject the source, not replay success.\n\
         args: {args:?}\nstderr: {stderr}"
    );
}

/// Allow/warn levels are outcome-affecting when they interact with deny
/// groups. Removing an allow that made a compile green must not hit that entry.
#[test]
fn allow_override_change_cannot_replay_cached_success() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let src_dir = TempDir::new().unwrap();
    let src = src_dir.path().join("lib.rs");
    std::fs::write(&src, b"fn intentionally_unused() {}\n").unwrap();

    let allowed = ["-D", "warnings", "-A", "dead_code"];
    run_kache_rustc(cache_dir.path(), out.path(), &src, &allowed); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &allowed); // hit
    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (1, 1),
        "the allowed invocation must prove a real cache hit before the negative control"
    );

    let denied = ["-D", "warnings"];
    let (args, output) = kache_rustc_output_from(cache_dir.path(), out.path(), &src, &denied, None);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success()
            && stderr.contains("intentionally_unused")
            && stderr.contains("never used"),
        "removing -A dead_code must execute rustc and fail, not replay success.\n\
         args: {args:?}\nstderr: {stderr}"
    );
}

/// Rustc applies `-O` and `-Copt-level` in argv order. Reversing them must
/// miss rather than restore the artifact compiled under the opposite winner.
#[test]
fn codegen_shorthand_override_order_changes_cache_key() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let (_src_dir, src) = fresh_src();

    run_kache_rustc(
        cache_dir.path(),
        out.path(),
        &src,
        &["-O", "--codegen=opt-level=0"],
    ); // miss
    run_kache_rustc(
        cache_dir.path(),
        out.path(),
        &src,
        &["-O", "--codegen=opt-level=0"],
    ); // hit
    run_kache_rustc(
        cache_dir.path(),
        out.path(),
        &src,
        &["--codegen", "opt-level=0", "-O"],
    ); // miss

    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (2, 1),
        "reversing last-wins optimization flags must produce a new cache key"
    );
}

/// #647: response-file contents are effective rustc arguments, not merely a
/// path token. An unchanged file must hit, while changing a cfg at the same
/// path must miss rather than restore the artifact built under the old cfg.
#[test]
fn rustc_response_file_content_changes_cache_key() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let src_dir = TempDir::new().unwrap();
    let src = src_dir.path().join("lib.rs");
    std::fs::write(
        &src,
        b"#[cfg(response_v1)]\npub fn f() -> u32 { 1 }\n\
          #[cfg(response_v2)]\npub fn f() -> u32 { 2 }\n",
    )
    .unwrap();
    let response = src_dir.path().join("rustc.args");
    let at_response = format!("@{}", response.display());

    std::fs::write(&response, b"--cfg\nresponse_v1\n").unwrap();
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[&at_response]); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[&at_response]); // hit
    std::fs::write(&response, b"--cfg\nresponse_v2\n").unwrap();
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[&at_response]); // miss

    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (2, 1),
        "expected response v1 -> miss, unchanged v1 -> hit, rewritten v2 -> miss"
    );
}

/// Direct remaps alter `file!()` and debug paths. Equivalent spelling and a
/// relocated matching FROM must hit; a non-matching FROM or changed TO misses.
#[test]
fn direct_remap_path_prefix_changes_cache_key() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let workspace_a = TempDir::new().unwrap();
    let workspace_b = TempDir::new().unwrap();
    let root_a = workspace_a.path().canonicalize().unwrap();
    let root_b = workspace_b.path().canonicalize().unwrap();
    let layout = |root: &Path| {
        let src = root.join("src/lib.rs");
        let out = root.join("target/debug/deps");
        std::fs::create_dir_all(src.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(&src, b"pub const SOURCE: &str = file!();\n").unwrap();
        (src, out)
    };
    let (src_a, out_a) = layout(&root_a);
    let (src_b, out_b) = layout(&root_b);

    let map_a = format!("{}=/virtual/a", root_a.display());
    let attached_a = format!("--remap-path-prefix={map_a}");
    let nonmatching_a = format!(
        "--remap-path-prefix={}=/virtual/a",
        root_a.join("not-the-source-prefix").display()
    );
    let portable_b = format!("--remap-path-prefix={}=/virtual/a", root_b.display());
    let different_target_b = format!("--remap-path-prefix={}=/virtual/b", root_b.display());

    run_kache_rustc_from(
        cache_dir.path(),
        &out_a,
        &src_a,
        &["--remap-path-prefix", &map_a],
        Some(&root_a),
    ); // miss
    run_kache_rustc_from(
        cache_dir.path(),
        &out_a,
        &src_a,
        &[&attached_a],
        Some(&root_a),
    ); // hit
    run_kache_rustc_from(
        cache_dir.path(),
        &out_a,
        &src_a,
        &[&nonmatching_a],
        Some(&root_a),
    ); // miss
    run_kache_rustc_from(
        cache_dir.path(),
        &out_b,
        &src_b,
        &[&portable_b],
        Some(&root_b),
    ); // hit
    run_kache_rustc_from(
        cache_dir.path(),
        &out_b,
        &src_b,
        &[&different_target_b],
        Some(&root_b),
    ); // miss

    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (3, 2),
        "spelling and relocated FROM must hit; non-matching FROM or changed TO must miss"
    );
}

/// H1: a build-script `-l <lib>` reaches rustc on argv. A different native
/// lib must diverge the key; an identical one must hit.
#[test]
fn link_lib_value_changes_cache_key() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let (_src_dir, src) = fresh_src();

    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-l", "ssl"]); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-l", "ssl"]); // hit (same key)
    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-l", "crypto"]); // miss (diverged)

    let (compiled, hits) = compiled_hit_counts(cache_dir.path());
    assert_eq!(
        (compiled, hits),
        (2, 1),
        "expected -l ssl→miss, -l ssl→hit, -l crypto→miss; a false hit on the \
         differing -l would show compiled=1, hits=2"
    );
}

/// H1: a build-script `-L native=<path>` must diverge the key, but cargo's
/// redundant `-L dependency=<path>` (covered by the content-hashed
/// `--extern`) must NOT — otherwise every target-dir move busts the cache.
#[test]
fn link_search_native_keys_but_dependency_is_skipped() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let (_src_dir, src) = fresh_src();

    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-L", "native=/opt/a"]); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-L", "native=/opt/a"]); // hit
    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-L", "native=/opt/b"]); // miss (diverged)
    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-L", "dependency=/x"]); // miss (new)
    run_kache_rustc(cache_dir.path(), out.path(), &src, &["-L", "dependency=/y"]); // HIT (dependency= skipped)

    let (compiled, hits) = compiled_hit_counts(cache_dir.path());
    assert_eq!(
        (compiled, hits),
        (3, 2),
        "native= must diverge (native=/b→miss) while dependency= must be \
         ignored (dependency=/y→hit). A regression keying dependency= would \
         show compiled=4; one not keying native= would show compiled=2"
    );
}

/// H2: `--sysroot` selects which std rustc links against. Adding it (or
/// changing it) must diverge the key — before the fix it was ignored, so an
/// explicit `--sysroot` would have falsely hit the no-sysroot entry.
#[test]
fn sysroot_changes_cache_key() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let (_src_dir, src) = fresh_src();
    let sysroot = rustc_sysroot();

    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // hit
    run_kache_rustc(cache_dir.path(), out.path(), &src, &["--sysroot", &sysroot]); // miss (diverged)

    let (compiled, hits) = compiled_hit_counts(cache_dir.path());
    assert_eq!(
        (compiled, hits),
        (2, 1),
        "adding --sysroot must diverge the key; an ignored --sysroot (the bug) \
         would falsely hit the no-sysroot entry → compiled=1, hits=2"
    );
}

/// #220: a co-located `kache.toml` declares an out-of-band compile-time input
/// (the sqlx offline cache) that rustc's dep-info never reports. Editing that
/// file must diverge the key — before the feature it would falsely hit the
/// original entry and restore a stale artifact.
#[test]
fn colocated_extra_input_changes_cache_key() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();

    // A crate dir holding the source, its `Cargo.toml`, the co-located
    // `kache.toml`, and the declared `.sqlx/` tree. crate-dir resolution
    // walks up from the source file to the `Cargo.toml`.
    let crate_dir = TempDir::new().unwrap();
    std::fs::write(
        crate_dir.path().join("Cargo.toml"),
        "[package]\nname = \"kt\"\n",
    )
    .unwrap();
    std::fs::write(
        crate_dir.path().join("kache.toml"),
        "extra_inputs = [\".sqlx/**/*.json\"]\n",
    )
    .unwrap();
    std::fs::create_dir_all(crate_dir.path().join(".sqlx")).unwrap();
    let query = crate_dir.path().join(".sqlx/query.json");
    std::fs::write(&query, "v1").unwrap();
    let src = crate_dir.path().join("lib.rs");
    std::fs::write(&src, b"pub fn f() -> u32 { 42 }\n").unwrap();

    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // hit (declared input unchanged)
    std::fs::write(&query, "v2").unwrap();
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // miss (declared input changed)

    let (compiled, hits) = compiled_hit_counts(cache_dir.path());
    assert_eq!(
        (compiled, hits),
        (2, 1),
        "editing a declared extra input must diverge the key; the pre-feature \
         behavior (ignoring it) would falsely hit and show compiled=1, hits=2"
    );
}

/// #760: source identity is the path-to-content mapping, not merely the
/// unordered multiset of source bytes. Swapping two module bodies keeps the
/// byte multiset unchanged but changes which module owns each definition, so
/// the third invocation must compile instead of restoring the first artifact.
#[test]
fn swapping_module_contents_changes_cache_key() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let crate_dir = TempDir::new().unwrap();
    let src = crate_dir.path().join("lib.rs");
    let module_a = crate_dir.path().join("a.rs");
    let module_b = crate_dir.path().join("b.rs");
    let body_a = b"pub const VALUE: u8 = 1;\n";
    let body_b = b"pub const VALUE: u8 = 2;\n";

    std::fs::write(
        &src,
        b"mod a;\nmod b;\npub fn values() -> (u8, u8) { (a::VALUE, b::VALUE) }\n",
    )
    .unwrap();
    std::fs::write(&module_a, body_a).unwrap();
    std::fs::write(&module_b, body_b).unwrap();

    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // miss
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // hit

    std::fs::write(&module_a, body_b).unwrap();
    std::fs::write(&module_b, body_a).unwrap();
    run_kache_rustc(cache_dir.path(), out.path(), &src, &[]); // must miss

    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (2, 1),
        "swapping equal-multiset module contents must not restore the artifact for the old path mapping"
    );
}

/// Linux filesystems can distinguish canonically equivalent Unicode names.
/// Normalizing path identity to NFC would merge those names, so swapping their
/// contents would preserve the sorted source records and recreate #760.
#[test]
#[cfg(target_os = "linux")]
fn unicode_distinct_source_names_remain_distinct() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let crate_dir = TempDir::new().unwrap();
    let src = crate_dir.path().join("lib.rs");
    let nfc = "\u{e9}.txt";
    let nfd = "e\u{301}.txt";
    let body_a = b"alpha";
    let body_b = b"bravo";

    std::fs::write(
        &src,
        format!(
            "pub static A: &[u8] = include_bytes!({nfc:?});\n\
             pub static B: &[u8] = include_bytes!({nfd:?});\n"
        ),
    )
    .unwrap();
    std::fs::write(crate_dir.path().join(nfc), body_a).unwrap();
    std::fs::write(crate_dir.path().join(nfd), body_b).unwrap();

    run_kache_rustc_from(
        cache_dir.path(),
        out.path(),
        &src,
        &[],
        Some(crate_dir.path()),
    );
    run_kache_rustc_from(
        cache_dir.path(),
        out.path(),
        &src,
        &[],
        Some(crate_dir.path()),
    );

    std::fs::write(crate_dir.path().join(nfc), body_b).unwrap();
    std::fs::write(crate_dir.path().join(nfd), body_a).unwrap();
    run_kache_rustc_from(
        cache_dir.path(),
        out.path(),
        &src,
        &[],
        Some(crate_dir.path()),
    );

    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (2, 1),
        "filesystem-distinct Unicode source names must remain distinct key identities"
    );
}
