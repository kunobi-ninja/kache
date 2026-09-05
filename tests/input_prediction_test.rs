//! End-to-end tests for rustc input-set predictions.
//!
//! Each test drives the real `kache` binary as a `RUSTC_WRAPPER` on a
//! controlled `rustc` invocation and asserts via `kache report` that:
//!
//!   - the cold build runs the dep-info pre-pass and records the closure,
//!   - the warm rebuild derives the identical key with zero pre-pass spawns,
//!   - the same key comes out with predictions off (derivation changes
//!     discovery, never the digest),
//!   - a grown closure misses (never wrong-hits), recompiles through the
//!     pre-pass, and stores under the true key — so a predictions-off
//!     rebuild hits it (the rule-11 pin: never store under a derived key),
//!   - a shadowing sibling that rustc would reject never restores.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

fn rustc_path() -> String {
    std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string())
}

fn toml_path(path: &Path) -> String {
    toml::Value::String(path.to_string_lossy().into_owned()).to_string()
}

/// Hermetic config for exactly one flag setting. The flag travels in the
/// file because `ignore_env` neutralizes `KACHE_*` overrides by design.
fn write_test_config(cache_dir: &Path, predictions: bool) -> PathBuf {
    let config_path = isolated_config_path(cache_dir);
    std::fs::write(
        &config_path,
        format!(
            "[cache]\nlocal_only = true\nignore_env = true\ninput_predictions = {predictions}\nlocal_store = {}\nruntime_dir = {}\n",
            toml_path(cache_dir),
            toml_path(cache_dir)
        ),
    )
    .unwrap();
    config_path
}
fn run_kache_rustc_predict(
    cache_dir: &Path,
    out_dir: &Path,
    src: &Path,
    predictions: bool,
) -> std::process::Output {
    let args: Vec<String> = vec![
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
    let config_path = write_test_config(cache_dir, predictions);
    let output = std::process::Command::new(kache_binary())
        .args(&args)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", config_path)
        .env("KACHE_INT_SET", "x")
        .env_remove("KACHE_INT_UNSET")
        .env_remove("KACHE_DISABLED")
        .env_remove("KACHE_NAMESPACE")
        .env_remove("KACHE_BASE_DIR")
        .env_remove("KACHE_SOCKET_PATH")
        .env_remove("KACHE_ACTIVE")
        .env_remove("KACHE_FAMILY_PROBE_ACTIVE")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .output()
        .expect("failed to run kache rustc");
    assert!(
        output.status.success(),
        "kache rustc failed.\nargs: {args:?}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    output
}

struct LastEvent {
    result: String,
    cache_key: String,
    dep_info_runs: u64,
}

/// `local_hits` from the report summary.
fn summary_local_hits(cache_dir: &Path) -> u64 {
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
    report["summary"]["local_hits"].as_u64().unwrap_or(u64::MAX)
}

/// The most recent `kt` event from `kache report`.
fn last_event(cache_dir: &Path) -> LastEvent {
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
    let event = report["all_events"]
        .as_array()
        .expect("report should include all_events")
        .iter()
        .rev()
        .find(|e| e["crate_name"].as_str() == Some("kt"))
        .expect("report should include a kt event");
    LastEvent {
        result: event["result"].as_str().unwrap_or("").to_string(),
        cache_key: event["cache_key"].as_str().unwrap_or("").to_string(),
        dep_info_runs: event["dep_info_runs"].as_u64().unwrap_or(u64::MAX),
    }
}

fn fixture() -> (TempDir, TempDir, TempDir, PathBuf) {
    let work = TempDir::new().unwrap();
    let cache = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();
    let src = work.path().join("lib.rs");
    std::fs::write(
        &src,
        b"mod a;\npub fn f() -> u32 {\n    let _ = include_str!(\"data.txt\");\n    let _ = env!(\"KACHE_INT_SET\");\n    let _ = option_env!(\"KACHE_INT_UNSET\");\n    a::g() + 1\n}\n",
    )
    .unwrap();
    std::fs::write(work.path().join("a.rs"), b"pub fn g() -> u32 { 41 }\n").unwrap();
    std::fs::write(work.path().join("data.txt"), b"data\n").unwrap();
    (work, cache, out, src)
}

#[test]
fn predictions_cold_warm_off_and_stale_closure() {
    build_kache();
    let (work, cache, out, src) = fixture();
    let cache_dir = cache.path();
    let out_dir = out.path();

    // Cold with predictions on: miss, one pre-pass, key K1.
    run_kache_rustc_predict(cache_dir, out_dir, &src, true);
    let cold = last_event(cache_dir);
    assert_eq!(cold.result, "miss");
    assert_eq!(cold.dep_info_runs, 1);

    // Warm with predictions on: hit, zero pre-pass spawns, identical key.
    run_kache_rustc_predict(cache_dir, out_dir, &src, true);
    let warm = last_event(cache_dir);
    assert_eq!(warm.result, "local_hit");
    assert_eq!(warm.dep_info_runs, 0);
    assert_eq!(warm.cache_key, cold.cache_key);

    // Predictions off: hit via the pre-pass, identical key — derivation
    // changes discovery, never the digest.
    run_kache_rustc_predict(cache_dir, out_dir, &src, false);
    let off = last_event(cache_dir);
    assert_eq!(off.result, "local_hit");
    assert_eq!(off.dep_info_runs, 1);
    assert_eq!(off.cache_key, cold.cache_key);

    // Grow the closure: the stale record must miss (never wrong-hit),
    // re-derive through the pre-pass, and store under the true key.
    std::fs::write(
        &src,
        b"mod a;\nmod b;\npub fn f() -> u32 {\n    let _ = include_str!(\"data.txt\");\n    let _ = env!(\"KACHE_INT_SET\");\n    let _ = option_env!(\"KACHE_INT_UNSET\");\n    a::g() + b::h()\n}\n",
    )
    .unwrap();
    std::fs::write(work.path().join("b.rs"), b"pub fn h() -> u32 { 1 }\n").unwrap();
    run_kache_rustc_predict(cache_dir, out_dir, &src, true);
    let grown = last_event(cache_dir);
    assert_eq!(grown.result, "miss");
    assert_eq!(grown.dep_info_runs, 1);
    assert_ne!(grown.cache_key, cold.cache_key);

    // The true key was stored: predictions off hits it. Without the
    // miss-recomputation this misses — the entry went under a stale key.
    run_kache_rustc_predict(cache_dir, out_dir, &src, false);
    let regrown = last_event(cache_dir);
    assert_eq!(regrown.result, "local_hit");
    assert_eq!(regrown.cache_key, grown.cache_key);
}

/// A shadowing sibling that rustc rejects must never restore: the
/// prediction fails closed, the pre-pass runs, and the real compiler error
/// passes through instead of a stale hit.
#[test]
fn predictions_sibling_shadow_fails_closed() {
    build_kache();
    let (work, cache, out, src) = fixture();
    let cache_dir = cache.path();
    let out_dir = out.path();

    run_kache_rustc_predict(cache_dir, out_dir, &src, true);
    assert_eq!(last_event(cache_dir).result, "miss");
    run_kache_rustc_predict(cache_dir, out_dir, &src, true);
    assert_eq!(last_event(cache_dir).result, "local_hit");

    // `a/mod.rs` beside the recorded `a.rs`: rustc would fail with E0761.
    std::fs::create_dir_all(work.path().join("a")).unwrap();
    std::fs::write(
        work.path().join("a").join("mod.rs"),
        b"pub fn h() -> u32 { 1 }\n",
    )
    .unwrap();
    let args: Vec<String> = vec![
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
    let config_path = write_test_config(cache_dir, true);
    let output = std::process::Command::new(kache_binary())
        .args(&args)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", config_path)
        .env("KACHE_INT_SET", "x")
        .env_remove("KACHE_INT_UNSET")
        .env_remove("KACHE_DISABLED")
        .env_remove("KACHE_NAMESPACE")
        .env_remove("KACHE_BASE_DIR")
        .env_remove("KACHE_SOCKET_PATH")
        .env_remove("KACHE_ACTIVE")
        .env_remove("KACHE_FAMILY_PROBE_ACTIVE")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .output()
        .expect("failed to run kache rustc");
    assert!(
        !output.status.success(),
        "the real E0761 must surface, not a stale hit"
    );
    // Key computation fails on the shadowed tree, which logs no per-crate
    // event: assert on the summary instead — no hit was served.
    assert_eq!(
        summary_local_hits(cache_dir),
        1,
        "exactly the one warm hit from before may exist"
    );
}
