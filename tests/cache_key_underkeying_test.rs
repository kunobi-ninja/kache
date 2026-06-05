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

fn kache_binary() -> PathBuf {
    let mut path = std::env::current_exe().unwrap();
    path.pop(); // test binary name
    path.pop(); // deps/
    path.push("kache");
    path
}

fn build_kache() {
    // Bootstrap the binary under test with no configured wrapper; the
    // wrapper behavior is exercised by invoking it directly below.
    let status = std::process::Command::new("cargo")
        .args(["build", "--config", "build.rustc-wrapper=\"\""])
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .status()
        .expect("failed to build kache");
    assert!(status.success(), "kache build failed");
}

fn isolated_config_path(cache_dir: &Path) -> PathBuf {
    cache_dir.join("config.toml")
}

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

/// Compile a trivial rlib through kache-as-RUSTC_WRAPPER with `extra` flags
/// appended to the rustc argv. Asserts the compile succeeds.
fn run_kache_rustc(cache_dir: &Path, out_dir: &Path, src: &Path, extra: &[&str]) {
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

    let output = std::process::Command::new(kache_binary())
        .args(&args)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("KACHE_LOG", "kache=info")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .output()
        .expect("failed to run kache rustc");

    assert!(
        output.status.success(),
        "kache rustc failed.\nargs: {args:?}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

/// `(misses, local_hits)` from `kache report` over this isolated cache dir.
fn miss_hit_counts(cache_dir: &Path) -> (u64, u64) {
    let output = std::process::Command::new(kache_binary())
        .args(["report", "--format", "json", "--since", "1h"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .output()
        .expect("failed to run kache report");
    assert!(output.status.success(), "kache report failed");
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("report should be valid json");
    let s = &report["summary"];
    (
        s["misses"].as_u64().unwrap_or(0),
        s["local_hits"].as_u64().unwrap_or(0),
    )
}

fn fresh_src() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("lib.rs");
    std::fs::write(&src, b"pub fn f() -> u32 { 42 }\n").unwrap();
    (dir, src)
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

    let (misses, hits) = miss_hit_counts(cache_dir.path());
    assert_eq!(
        (misses, hits),
        (2, 1),
        "expected -l ssl→miss, -l ssl→hit, -l crypto→miss; a false hit on the \
         differing -l would show misses=1, hits=2"
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

    let (misses, hits) = miss_hit_counts(cache_dir.path());
    assert_eq!(
        (misses, hits),
        (3, 2),
        "native= must diverge (native=/b→miss) while dependency= must be \
         ignored (dependency=/y→hit). A regression keying dependency= would \
         show misses=4; one not keying native= would show misses=2"
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

    let (misses, hits) = miss_hit_counts(cache_dir.path());
    assert_eq!(
        (misses, hits),
        (2, 1),
        "adding --sysroot must diverge the key; an ignored --sysroot (the bug) \
         would falsely hit the no-sysroot entry → misses=1, hits=2"
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

    let (misses, hits) = miss_hit_counts(cache_dir.path());
    assert_eq!(
        (misses, hits),
        (2, 1),
        "editing a declared extra input must diverge the key; the pre-feature \
         behavior (ignoring it) would falsely hit and show misses=1, hits=2"
    );
}
