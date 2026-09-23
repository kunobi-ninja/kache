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
//!   - a shadowing sibling that rustc would reject never restores,
//!   - a registry unit that includes a file from its `OUT_DIR` uses, in a
//!     second target directory, the record the first one made, and only
//!     while that `OUT_DIR` holds the same files.

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
    run_kache_rustc_verify(cache_dir, out_dir, src, predictions, None)
}

/// The same invocation with `KACHE_VERIFY_INPUT_PREDICTIONS` set, which makes
/// key computation run the pre-pass alongside a prediction and compare the two
/// closures. The pre-pass answer always wins, so the only thing the mode
/// changes is whether a disagreement gets counted.
fn run_kache_rustc_verify(
    cache_dir: &Path,
    out_dir: &Path,
    src: &Path,
    predictions: bool,
    verify: Option<&str>,
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
        .envs(verify.map(|mode| ("KACHE_VERIFY_INPUT_PREDICTIONS", mode)))
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
    compiler_runs: u64,
    prediction_mismatches: u64,
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
        compiler_runs: event["compiler_runs"].as_u64().unwrap_or(u64::MAX),
        prediction_mismatches: event["prediction_mismatches"].as_u64().unwrap_or(u64::MAX),
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
/// The number that decides whether predictions can be trusted more widely.
///
/// With verification on, every prediction is checked against the pre-pass that
/// would have replaced it. A mismatch means the soundness argument has a hole
/// on this code, and it has to be a number a benchmark can assert on rather
/// than a warning someone might read.
#[test]
fn verified_predictions_agree_with_the_pre_pass_and_count_it() {
    build_kache();
    let (_work, cache, out, src) = fixture();
    let cache_dir = cache.path();
    let out_dir = out.path();

    // Cold records the closure. Verification has nothing to compare yet.
    run_kache_rustc_verify(cache_dir, out_dir, &src, true, Some("always"));
    let cold = last_event(cache_dir);
    assert_eq!(cold.result, "miss");
    assert_eq!(cold.prediction_mismatches, 0);

    // Warm uses the record AND runs the pre-pass to check it. The pre-pass
    // answer is the one that keys, so the key still matches cold's.
    run_kache_rustc_verify(cache_dir, out_dir, &src, true, Some("always"));
    let verified = last_event(cache_dir);
    assert_eq!(verified.result, "local_hit");
    assert_eq!(verified.cache_key, cold.cache_key);
    assert_eq!(
        verified.prediction_mismatches, 0,
        "the prediction must reproduce exactly what the pre-pass discovered"
    );
    assert_eq!(
        verified.dep_info_runs, 1,
        "verification pays for the pre-pass it was checking against"
    );

    // And with verification off the same warm build skips the pre-pass
    // entirely, which is the whole point of the record.
    run_kache_rustc_predict(cache_dir, out_dir, &src, true);
    let unverified = last_event(cache_dir);
    assert_eq!(unverified.result, "local_hit");
    assert_eq!(unverified.cache_key, cold.cache_key);
    assert_eq!(unverified.dep_info_runs, 0);
    assert_eq!(unverified.prediction_mismatches, 0);
}

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

/// Where Cargo extracts a registry package, relative to the fixture root.
const REGISTRY_PACKAGE: &str = "home/registry/src/index-test/kt-1.0.0";
/// Cargo's build-script unit for the package, the same in every checkout.
const BUILD_UNIT: &str = "kt-0123456789abcdef";
const GENERATED: &str = "pub fn generated() -> u32 { 7 }\n";

/// The unit's build-script output directory inside `target`.
fn out_dir_in(target: &Path) -> PathBuf {
    target.join("debug/build").join(BUILD_UNIT).join("out")
}

/// A package that includes a generated file from `OUT_DIR`, built from two
/// target directories that share one cache, as two checkouts of a project
/// share `CARGO_HOME` and the store.
struct OutDirUnit {
    root: TempDir,
    cache: PathBuf,
    package: PathBuf,
    proc_macro: Option<PathBuf>,
}

impl OutDirUnit {
    /// `manifest_dir` is relative to the fixture root. With `with_macro` the
    /// crate also expands a real proc macro, as serde does; without it, it
    /// has serde_core's shape.
    fn new(manifest_dir: &str, with_macro: bool) -> Self {
        let root = TempDir::new().unwrap();
        let package = root.path().join(manifest_dir);
        std::fs::create_dir_all(package.join("src")).unwrap();
        std::fs::write(package.join("Cargo.toml"), "[package]\nname = \"kt\"\n").unwrap();
        let mut lib = String::new();
        if with_macro {
            lib.push_str("pm::answer!();\n");
        }
        lib.push_str("include!(concat!(env!(\"OUT_DIR\"), \"/gen.rs\"));\n");
        std::fs::write(package.join("src/lib.rs"), lib).unwrap();
        let proc_macro = with_macro.then(|| build_proc_macro(root.path()));
        let cache = root.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        Self {
            root,
            cache,
            package,
            proc_macro,
        }
    }

    /// The unit with `content` at `path`, relative to the package.
    fn with_file(self, path: &str, content: &str) -> Self {
        let path = self.package.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
        self
    }

    /// `<root>/<checkout>/target` with the generated file in its `OUT_DIR`
    /// and the proc macro copied byte for byte into its `deps`.
    fn target(&self, checkout: &str) -> PathBuf {
        let target = self.root.path().join(checkout).join("target");
        std::fs::create_dir_all(out_dir_in(&target)).unwrap();
        std::fs::create_dir_all(target.join("debug/deps")).unwrap();
        std::fs::write(out_dir_in(&target).join("gen.rs"), GENERATED).unwrap();
        if let Some(proc_macro) = &self.proc_macro {
            std::fs::copy(proc_macro, self.macro_in(&target)).unwrap();
        }
        target
    }

    fn macro_in(&self, target: &Path) -> PathBuf {
        let name = self.proc_macro.as_ref().unwrap().file_name().unwrap();
        target.join("debug/deps").join(name)
    }

    /// Compile the unit into `target` the way Cargo would, from the package
    /// root, and return its event.
    fn build(&self, target: &Path, predictions: bool, verify: Option<&str>) -> LastEvent {
        let deps = target.join("debug/deps");
        let mut args: Vec<String> = vec![
            rustc_path(),
            "--crate-name".into(),
            "kt".into(),
            "--edition=2021".into(),
            self.package.join("src/lib.rs").display().to_string(),
            "--crate-type".into(),
            "lib".into(),
            "--emit=dep-info,metadata,link".into(),
            "-C".into(),
            "metadata=fedcba9876543210".into(),
            "-C".into(),
            "extra-filename=-fedcba9876543210".into(),
            "--out-dir".into(),
            deps.display().to_string(),
            "-L".into(),
            format!("dependency={}", deps.display()),
        ];
        if self.proc_macro.is_some() {
            args.push("--extern".into());
            args.push(format!("pm={}", self.macro_in(target).display()));
        }
        let config_path = write_test_config(&self.cache, predictions);
        let mut command = std::process::Command::new(kache_binary());
        command
            .args(&args)
            .current_dir(&self.package)
            .env("KACHE_CACHE_DIR", &self.cache)
            .env("KACHE_CONFIG", config_path)
            .env("CARGO_MANIFEST_DIR", &self.package)
            .env("OUT_DIR", out_dir_in(target))
            .env_remove("KACHE_DISABLED")
            .env_remove("KACHE_NAMESPACE")
            .env_remove("KACHE_BASE_DIR")
            .env_remove("KACHE_SOCKET_PATH")
            .env_remove("KACHE_ACTIVE")
            .env_remove("KACHE_FAMILY_PROBE_ACTIVE")
            .env_remove("RUSTC_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WRAPPER");
        match verify {
            Some(mode) => command.env("KACHE_VERIFY_INPUT_PREDICTIONS", mode),
            None => command.env_remove("KACHE_VERIFY_INPUT_PREDICTIONS"),
        };
        let output = command.output().expect("failed to run kache rustc");
        assert!(
            output.status.success(),
            "kache rustc failed.\nargs: {args:?}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr),
        );
        last_event(&self.cache)
    }
}

/// A proc macro built with plain rustc, never through kache: it is fixed
/// scaffolding, and caching it would add noise to the counts under test.
fn build_proc_macro(root: &Path) -> PathBuf {
    let src = root.join("pm.rs");
    std::fs::write(
        &src,
        "extern crate proc_macro;\n\
         #[proc_macro]\n\
         pub fn answer(_input: proc_macro::TokenStream) -> proc_macro::TokenStream {\n\
         \x20   \"pub fn answer() -> u32 { 42 }\".parse().unwrap()\n\
         }\n",
    )
    .unwrap();
    let out = root.join("pm-out");
    std::fs::create_dir_all(&out).unwrap();
    let status = std::process::Command::new(rustc_path())
        .args(["--crate-name", "pm", "--crate-type", "proc-macro"])
        .args(["--edition", "2021", "--out-dir"])
        .arg(&out)
        .arg(&src)
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .status()
        .expect("run rustc to build the test proc macro");
    assert!(status.success(), "building the test proc macro failed");
    let extension = if cfg!(windows) {
        "dll"
    } else if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    std::fs::read_dir(&out)
        .unwrap()
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| path.extension().is_some_and(|e| e == extension))
        .unwrap_or_else(|| panic!("no proc-macro {extension} in {}", out.display()))
}

/// A registry unit built in a second target directory derives its closure
/// from the first one's record, and gets the key the pre-pass would give.
fn predicts_in_another_target(unit: OutDirUnit) {
    build_kache();
    let a = unit.target("a");
    let b = unit.target("b");

    let cold = unit.build(&a, true, None);
    assert_eq!(cold.result, "miss");

    let warm = unit.build(&b, true, None);
    assert_eq!(warm.result, "local_hit");
    assert_eq!(warm.dep_info_runs, 0, "B uses the record A made");
    assert_eq!(warm.compiler_runs, 0);

    let off = unit.build(&b, false, None);
    assert_eq!(off.result, "local_hit");
    assert_eq!(off.dep_info_runs, 1);
    assert_eq!(warm.cache_key, off.cache_key);

    let verified = unit.build(&b, true, Some("always"));
    assert_eq!(verified.result, "local_hit");
    assert_eq!(verified.dep_info_runs, 1);
    assert_eq!(verified.prediction_mismatches, 0);
    assert_eq!(verified.cache_key, off.cache_key);
}

#[test]
fn a_registry_unit_predicts_in_another_target() {
    predicts_in_another_target(OutDirUnit::new(REGISTRY_PACKAGE, true));
}

#[test]
fn a_registry_unit_without_a_macro_predicts_in_another_target() {
    predicts_in_another_target(OutDirUnit::new(REGISTRY_PACKAGE, false));
}

const README_DOC: &str = "#![doc = include_str!(\"../README.md\")]\n";

/// rustc reports `include_str!("../README.md")` as `src/../README.md`,
/// without normalizing it. That path stays in the package, so the record
/// still serves another checkout: the shared row when the unit reads nothing
/// from `OUT_DIR`, the relocated row when it does.
#[test]
fn a_registry_unit_reading_its_readme_predicts_in_another_target() {
    for lib in [
        format!("{README_DOC}pub fn f() {{}}\n"),
        format!("{README_DOC}include!(concat!(env!(\"OUT_DIR\"), \"/gen.rs\"));\n"),
    ] {
        let unit = OutDirUnit::new(REGISTRY_PACKAGE, false)
            .with_file("README.md", "Docs.\n")
            .with_file("src/lib.rs", &lib);
        predicts_in_another_target(unit);
    }
}

/// Build in A, then in B, and return B's event. `prepare` edits the two
/// target directories in between.
fn build_a_then_b(unit: &OutDirUnit, prepare: impl FnOnce(&Path, &Path)) -> LastEvent {
    build_kache();
    let a = unit.target("a");
    let b = unit.target("b");
    prepare(&a, &b);
    assert_eq!(unit.build(&a, true, None).result, "miss");
    unit.build(&b, true, None)
}

/// A generated file the closure does not name still changes what a macro or
/// `include!` could read, so B's extra file keeps A's record out.
#[test]
fn a_relocated_record_needs_the_same_out_dir() {
    let unit = OutDirUnit::new(REGISTRY_PACKAGE, false);
    let warm = build_a_then_b(&unit, |_, b| {
        std::fs::write(out_dir_in(b).join("extra.rs"), "// not included\n").unwrap();
    });
    assert_eq!(warm.dep_info_runs, 1);
    assert_eq!(
        warm.result, "local_hit",
        "the closure itself did not change"
    );
}

/// A generated file that names A's target could send rustc to a file the
/// relocation does not move, so A keeps its record to itself.
#[test]
fn a_generated_file_naming_the_target_keeps_the_record_local() {
    let unit = OutDirUnit::new(REGISTRY_PACKAGE, false);
    let warm = build_a_then_b(&unit, |a, b| {
        let generated = format!("// generated in {}\n{GENERATED}", a.display());
        for target in [a, b] {
            std::fs::write(out_dir_in(target).join("gen.rs"), &generated).unwrap();
        }
    });
    assert_eq!(warm.dep_info_runs, 1);
    assert_eq!(warm.result, "local_hit");
}

/// A `..` may climb back up inside the package, not out of it, even into
/// another registry package, so this record stays with the checkout that
/// made it.
#[test]
fn a_registry_unit_reading_past_its_package_keeps_the_record_local() {
    let other = "pub const OTHER: &str = include_str!(\"../../other-1.0.0/lib.rs\");\n";
    for lib in [
        other.to_string(),
        format!("{other}include!(concat!(env!(\"OUT_DIR\"), \"/gen.rs\"));\n"),
    ] {
        let unit = OutDirUnit::new(REGISTRY_PACKAGE, false)
            .with_file("../other-1.0.0/lib.rs", "other\n")
            .with_file("src/lib.rs", &lib);
        let warm = build_a_then_b(&unit, |_, _| {});
        assert_eq!(warm.dep_info_runs, 1, "{lib}");
        assert_eq!(warm.result, "local_hit");
    }
}

/// Only a registry package is the same files in every checkout. The writer
/// checks that twice, in the relocated identity and again when it looks for
/// the registry root, so this test fails only when both checks go. A unit
/// test pins the reader's check on its own.
#[test]
fn a_workspace_unit_keeps_its_out_dir_record_local() {
    let unit = OutDirUnit::new("ws/kt", false);
    let warm = build_a_then_b(&unit, |_, _| {});
    assert_eq!(warm.dep_info_runs, 1);
    assert_eq!(warm.result, "local_hit");
}
