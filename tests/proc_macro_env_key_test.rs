//! End-to-end regression test for kunobi-ninja/kache#635: a proc macro that
//! branches on an environment variable at expansion time.
//!
//! rustc records an env var in dep-info only when a crate reads it through
//! `env!`/`option_env!`. A proc macro calling `std::env::var` while expanding
//! leaves no trace: the rustc command line, the source hashes, and the
//! `--extern` set are byte-identical whether or not the var is set, yet the
//! two compiles emit different artifacts. Reported against boltffi, whose
//! `#[export]` macro strips itself from dependency crates under
//! `BOLTFFI_BINDING_EXPANSION`; the stripped 269,858-byte artifact was served
//! to a normal build that needed the 635,006-byte one, and the build failed
//! with a missing trait impl.
//!
//! `[cache] key_env_vars` declares the vars that steer expansion so the two
//! modes get distinct keys. This test builds the smallest faithful version of
//! that shape — a real proc-macro dylib whose expansion depends on
//! `KACHE_TEST_PM_MODE` — and asserts that with the var declared:
//!
//!   - repeating a compile with the same value HITS (the key is deterministic,
//!     so the test cannot pass vacuously), and
//!   - flipping the var MISSES, and the restored constant matches the mode the
//!     build actually asked for.
//!
//! The value assertion is the one that matters: hit/miss counts prove the key
//! moved, but only reading the compiled artifact proves kache stopped serving
//! the wrong expansion.

use std::path::{Path, PathBuf};
use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

/// The env var the test proc macro branches on. Named `KACHE_TEST_*` so it is
/// obvious in a process listing that it belongs to this suite.
const MODE_VAR: &str = "KACHE_TEST_PM_MODE";

fn rustc_path() -> String {
    std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string())
}

/// Build the proc-macro dylib with plain rustc (never through kache): it is
/// fixed scaffolding for the test, and caching it would only add noise to the
/// hit/miss counts being asserted.
fn build_proc_macro(root: &Path) -> PathBuf {
    let src = root.join("pm.rs");
    std::fs::write(
        &src,
        r#"
extern crate proc_macro;
use proc_macro::TokenStream;

/// Expands to a different constant depending on the environment, with no
/// `env!` anywhere — so rustc reports nothing and the invocation kache sees
/// is identical in both modes.
///
/// The marker is emitted by the macro rather than selected in the consumer:
/// a `const` that picks between two literals leaves both in the rlib, so the
/// artifact could no longer be told apart.
#[proc_macro]
pub fn emit(_input: TokenStream) -> TokenStream {
    let value = match std::env::var("KACHE_TEST_PM_MODE") {
        Ok(v) if v == "expansion" => 1u32,
        _ => 2u32,
    };
    format!("pub const MARK: &str = \"KACHE_TEST_MARK_{value}\";")
        .parse()
        .unwrap()
}
"#,
    )
    .unwrap();

    let out = root.join("pm-out");
    std::fs::create_dir_all(&out).unwrap();
    let status = std::process::Command::new(rustc_path())
        .args([
            "--crate-name",
            "pmenv",
            "--crate-type",
            "proc-macro",
            "--edition",
            "2021",
            "--out-dir",
            out.to_str().unwrap(),
            src.to_str().unwrap(),
        ])
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove(MODE_VAR)
        .status()
        .expect("run rustc to build the test proc macro");
    assert!(status.success(), "building the test proc macro failed");

    // The dylib extension is platform-specific; find whatever rustc emitted.
    std::fs::read_dir(&out)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.contains("pmenv"))
                && p.extension().is_some_and(|e| e != "d")
        })
        .expect("proc-macro dylib should exist")
}

/// Compile the consumer crate through kache-as-`RUSTC_WRAPPER` with `mode`
/// either set or absent, and return the path to the emitted rlib.
fn compile_consumer(
    cache_dir: &Path,
    consumer_src: &Path,
    pm_dylib: &Path,
    out_dir: &Path,
    mode: Option<&str>,
) -> PathBuf {
    let mut command = std::process::Command::new(kache_binary());
    command
        .args([
            rustc_path().as_str(),
            "--crate-name",
            "consumer",
            "--crate-type",
            "lib",
            "--edition",
            "2021",
            "--emit=link",
            "--out-dir",
            out_dir.to_str().unwrap(),
            "--extern",
            &format!("pmenv={}", pm_dylib.display()),
            consumer_src.to_str().unwrap(),
        ])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("KACHE_LOG", "kache=info")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER");
    match mode {
        Some(value) => command.env(MODE_VAR, value),
        None => command.env_remove(MODE_VAR),
    };

    let output = command.output().expect("failed to run kache rustc");
    assert!(
        output.status.success(),
        "kache rustc failed (mode {mode:?}).\nstderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );

    out_dir.join("libconsumer.rlib")
}

/// `(compiled, local_hits)` from `kache report` over this isolated cache dir.
/// A `dup` is still a compiler run, so it counts as compiled.
fn compiled_hit_counts(cache_dir: &Path) -> (u64, u64) {
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
    let compiled = s["dups"].as_u64().unwrap_or(0) + s["misses"].as_u64().unwrap_or(0);
    (compiled, s["local_hits"].as_u64().unwrap_or(0))
}

/// Which expansion is baked into the rlib. The constant is the only thing that
/// differs between the two modes, so scanning for its bytes is enough to tell
/// a correct restore from a stale one.
fn baked_value(rlib: &Path) -> u32 {
    let bytes = std::fs::read(rlib).expect("read compiled rlib");
    let has_one = contains(&bytes, b"KACHE_TEST_MARK_1");
    let has_two = contains(&bytes, b"KACHE_TEST_MARK_2");
    assert!(
        has_one != has_two,
        "rlib should carry exactly one mode marker (one={has_one}, two={has_two})"
    );
    if has_one { 1 } else { 2 }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn declared_env_var_separates_proc_macro_expansions() {
    build_kache();
    let workspace = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let out = TempDir::new().unwrap();

    // Declare the var that steers expansion. Without this the two compiles are
    // indistinguishable to kache and the second one restores the first's
    // artifact — the #635 failure.
    std::fs::write(
        isolated_config_path(cache_dir.path()),
        format!("[cache]\nkey_env_vars = [\"{MODE_VAR}\"]\n"),
    )
    .unwrap();

    let pm_dylib = build_proc_macro(workspace.path());

    let consumer_src = workspace.path().join("consumer.rs");
    std::fs::write(&consumer_src, "pmenv::emit!();\n").unwrap();

    let compile = |mode: Option<&str>| {
        compile_consumer(cache_dir.path(), &consumer_src, &pm_dylib, out.path(), mode)
    };

    // 1. expansion mode — miss, compiles the stripped-equivalent artifact.
    assert_eq!(baked_value(&compile(Some("expansion"))), 1);
    // 2. same mode again — must hit, or the key is non-deterministic and the
    //    miss in step 3 would prove nothing.
    assert_eq!(baked_value(&compile(Some("expansion"))), 1);
    // 3. normal build — this is the reported bug. Before #635 the key was
    //    identical to step 1 and kache served the expansion-mode artifact.
    assert_eq!(
        baked_value(&compile(None)),
        2,
        "normal build must not restore the expansion-mode artifact"
    );
    // 4. normal build again — hits its own entry, not the expansion one.
    assert_eq!(baked_value(&compile(None)), 2);

    assert_eq!(
        compiled_hit_counts(cache_dir.path()),
        (2, 2),
        "each mode should compile once and hit once"
    );
}
