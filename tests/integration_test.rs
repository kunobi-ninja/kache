use assert_cmd::Command;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary};

fn run_kache_cc(project: &Path, cache_dir: &Path, args: &[&str]) {
    run_kache_cc_from(project, cache_dir, args);
}

fn run_kache_cc_from(cwd: &Path, cache_dir: &Path, args: &[&str]) {
    let output = std::process::Command::new(kache_binary())
        .args(args)
        .current_dir(cwd)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run kache cc");

    assert!(
        output.status.success(),
        "kache cc failed.\nargs: {args:?}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_cargo_build_with_kache(project: &Path, cache_dir: &Path, target_dir: &Path) {
    let output = std::process::Command::new("cargo")
        .args(["build", "--lib"])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run cargo build with kache");

    assert!(
        output.status.success(),
        "cargo build failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_cargo_test_with_kache(project: &Path, cache_dir: &Path, target_dir: &Path, package: &str) {
    let output = std::process::Command::new("cargo")
        .args(["test", "-q", "-p", package])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run cargo test with kache");

    assert!(
        output.status.success(),
        "cargo test failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn kache_report(cache_dir: &Path) -> serde_json::Value {
    let output = std::process::Command::new(kache_binary())
        .args(["report", "--format", "json", "--since", "1h"])
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .output()
        .expect("failed to run kache report");

    assert!(
        output.status.success(),
        "kache report failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).expect("report should be valid json")
}

fn kache_perfetto_report(cache_dir: &Path, root: &Path) -> serde_json::Value {
    let output = std::process::Command::new(kache_binary())
        .arg("report")
        .args(["--format", "perfetto", "--since", "1h", "--root"])
        .arg(root)
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .output()
        .expect("failed to run kache perfetto report");

    assert!(
        output.status.success(),
        "kache perfetto report failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).expect("perfetto report should be valid json")
}

fn assert_cc_report_counts(report: &serde_json::Value, expected_misses: u64, expected_hits: u64) {
    let summary = &report["summary"];
    assert_eq!(summary["misses"].as_u64(), Some(expected_misses));
    assert_eq!(summary["local_hits"].as_u64(), Some(expected_hits));
}

fn assert_last_cc_event(report: &serde_json::Value, result: &str, compiler_runs: u64) {
    let events = report["all_events"]
        .as_array()
        .expect("report should include all_events");
    let last = events
        .last()
        .expect("report should include at least one event");
    assert_eq!(last["crate_name"].as_str(), Some("foo.c"));
    assert_eq!(last["result"].as_str(), Some(result));
    assert_eq!(last["compiler_runs"].as_u64(), Some(compiler_runs));
}

fn find_depinfo_containing(root: &Path, needle: &str) -> Option<(PathBuf, String)> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("d") {
                continue;
            }
            let Ok(content) = std::fs::read_to_string(&path) else {
                continue;
            };
            if content.contains(needle) {
                return Some((path, content));
            }
        }
    }
    None
}

fn write_manifest_dir_workspace(root: &Path) {
    std::fs::create_dir_all(root.join("helper/src")).unwrap();
    std::fs::create_dir_all(root.join("consumer/src")).unwrap();
    std::fs::create_dir_all(root.join("consumer/tests")).unwrap();

    std::fs::write(
        root.join("Cargo.toml"),
        r#"[workspace]
members = ["helper", "consumer"]
resolver = "3"
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("helper/Cargo.toml"),
        r#"[package]
name = "helper"
version = "0.1.0"
edition = "2024"

[lib]
path = "src/lib.rs"
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("helper/src/lib.rs"),
        r#"#[inline(never)]
pub fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("consumer/Cargo.toml"),
        r#"[package]
name = "consumer"
version = "0.1.0"
edition = "2024"

[dependencies]
helper = { path = "../helper" }
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("consumer/src/lib.rs"),
        r#"pub fn helper_manifest_dir() -> &'static str {
    helper::manifest_dir()
}
"#,
    )
    .unwrap();
    std::fs::write(
        root.join("consumer/tests/manifest.rs"),
        r#"use std::path::Path;

#[test]
fn helper_manifest_dir_matches_this_checkout() {
    let embedded = Path::new(consumer::helper_manifest_dir()).canonicalize().unwrap();
    let expected = std::env::current_dir()
        .unwrap()
        .parent()
        .unwrap()
        .join("helper")
        .canonicalize()
        .unwrap();
    assert_eq!(embedded, expected);
}
"#,
    )
    .unwrap();
}

#[test]
fn test_cli_version_matches_package_version() {
    build_kache();
    assert_ne!(
        env!("CARGO_PKG_VERSION"),
        "0.0.0",
        "release builds must not use the placeholder package version"
    );

    Command::new(kache_binary())
        .arg("--version")
        .assert()
        .success()
        .stdout(predicates::str::contains(format!(
            "kache {}",
            env!("CARGO_PKG_VERSION")
        )));
}

#[test]
fn test_cli_help() {
    build_kache();
    Command::new(kache_binary())
        .arg("--help")
        .assert()
        .success()
        .stdout(predicates::str::contains("kache"));
}

#[test]
fn test_cli_list_empty() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();

    Command::new(kache_binary())
        .arg("list")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .assert()
        .success()
        .stdout(predicates::str::contains("No cached entries"));
}

#[test]
fn test_cli_purge_empty() {
    build_kache();
    let cache_dir = TempDir::new().unwrap();

    Command::new(kache_binary())
        .arg("purge")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .assert()
        .success()
        .stdout(predicates::str::contains("Cleared"));
}

#[test]
fn test_disabled_passthrough() {
    build_kache();

    let test_project = Path::new(env!("CARGO_MANIFEST_DIR")).join("test-projects/hello-world");
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // Build with kache disabled — should work just like normal cargo
    let status = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&test_project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_DISABLED", "1")
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status()
        .expect("failed to run cargo build with kache disabled");

    assert!(
        status.success(),
        "cargo build with KACHE_DISABLED should succeed"
    );
}

#[test]
fn test_wrapper_hello_world() {
    build_kache();

    let test_project = Path::new(env!("CARGO_MANIFEST_DIR")).join("test-projects/hello-world");
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    // First build (should be all cache misses)
    let status = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&test_project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .env("KACHE_EVENT_ROOT", &test_project)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("KACHE_LOG", "kache=debug")
        .status()
        .expect("failed to run cargo build with kache");

    assert!(status.success(), "first build with kache should succeed");

    // Verify the binary was produced (`.exe` on Windows).
    assert!(
        target_dir
            .path()
            .join(format!("debug/hello-world{}", std::env::consts::EXE_SUFFIX))
            .exists(),
        "binary should be produced"
    );

    // Check that the store has entries
    let store_dir = cache_dir.path().join("store");
    if store_dir.exists() {
        let entries: Vec<_> = std::fs::read_dir(&store_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        // Should have at least one cached entry (the hello-world lib)
        println!("Store entries after first build: {}", entries.len());
    }

    // Clean and rebuild (should be cache hits)
    let _ = std::process::Command::new("cargo")
        .args(["clean"])
        .current_dir(&test_project)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .status();

    let status = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&test_project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
        .env("KACHE_EVENT_ROOT", &test_project)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("KACHE_LOG", "kache=debug")
        .status()
        .expect("failed to run second cargo build with kache");

    assert!(status.success(), "second build (cache hit) should succeed");

    let trace = kache_perfetto_report(cache_dir.path(), &test_project);
    let trace_obj = trace.as_object().expect("trace report should be an object");
    assert_eq!(trace_obj.len(), 2);
    assert_eq!(trace["displayTimeUnit"].as_str(), Some("ms"));
    let trace_events = trace["traceEvents"]
        .as_array()
        .expect("trace report should include traceEvents");
    assert!(!trace_events.is_empty());
    // The trace leads with metadata events (ph "M": process_name / thread_name)
    // that name the kache process and worker lanes (#456); the compile slices
    // are ph "X". Assert on the first slice, not blindly on index 0.
    let first_slice = trace_events
        .iter()
        .find(|e| e["ph"].as_str() == Some("X"))
        .expect("trace report should include at least one X slice");
    assert_eq!(
        first_slice["args"]["root"].as_str(),
        Some(
            test_project
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        )
    );
}

#[test]
fn test_manifest_dir_env_dep_does_not_restore_stale_rlib_across_worktrees() {
    build_kache();

    let root = TempDir::new().unwrap();
    let workspace_a = root.path().join("checkout-a");
    let workspace_b = root.path().join("checkout-b");
    write_manifest_dir_workspace(&workspace_a);
    write_manifest_dir_workspace(&workspace_b);

    let cache_dir = TempDir::new().unwrap();
    let target_a = TempDir::new().unwrap();
    let target_b = TempDir::new().unwrap();

    run_cargo_test_with_kache(&workspace_a, cache_dir.path(), target_a.path(), "consumer");
    let events_after_a = kache_report(cache_dir.path())["all_events"]
        .as_array()
        .expect("report should include all_events")
        .len();

    run_cargo_test_with_kache(&workspace_b, cache_dir.path(), target_b.path(), "consumer");
    let report = kache_report(cache_dir.path());
    let all_events = report["all_events"]
        .as_array()
        .expect("report should include all_events");
    let checkout_b_events = &all_events[events_after_a..];

    assert!(
        checkout_b_events
            .iter()
            .any(|event| event["crate_name"].as_str() == Some("helper")
                && event["result"].as_str() == Some("miss")),
        "helper embeds CARGO_MANIFEST_DIR and must miss in checkout B, not restore checkout A's rlib: {checkout_b_events:?}"
    );
}

#[test]
fn test_rust_depinfo_restore_preserves_include_str_parent_relative_path() {
    build_kache();

    let project = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();
    std::fs::create_dir_all(project.path().join("src")).unwrap();
    std::fs::write(
        project.path().join("Cargo.toml"),
        r#"[package]
name = "kache-depinfo-repro"
version = "0.1.0"
edition = "2024"

[lib]
path = "src/lib.rs"

# Standalone workspace root so cargo never walks up into an ancestor workspace
# (e.g. when the system temp dir lives under one — common on Windows, where
# %TEMP% is under the user profile).
[workspace]
"#,
    )
    .unwrap();
    std::fs::write(
        project.path().join("README.md"),
        "included by the crate root\n",
    )
    .unwrap();
    std::fs::write(
        project.path().join("src/lib.rs"),
        r#"#![doc = include_str!("../README.md")]

pub fn value() -> u8 {
    1
}
"#,
    )
    .unwrap();

    run_cargo_build_with_kache(project.path(), cache_dir.path(), target_dir.path());
    std::fs::remove_dir_all(target_dir.path()).unwrap();
    run_cargo_build_with_kache(project.path(), cache_dir.path(), target_dir.path());

    // rustc joins the package-relative dir with the OS separator but keeps the
    // `include_str!` literal's own slashes verbatim, so the recorded path is
    // "src/../README.md" on Unix and "src\../README.md" on Windows.
    let sep = std::path::MAIN_SEPARATOR;
    let parent_rel = format!("src{sep}../README.md");
    let (depinfo_path, depinfo) = find_depinfo_containing(target_dir.path(), &parent_rel)
        .expect("restored target dir should contain rustc's parent-relative README.md dep-info");
    assert!(
        depinfo.contains(&parent_rel),
        "restored dep-info should preserve rustc's parent-relative include_str path in {}:\n{}",
        depinfo_path.display(),
        depinfo
    );
    assert!(
        !depinfo.contains(&format!("src{sep}./")),
        "restore must not inject the target dir into a parent-relative source path in {}:\n{}",
        depinfo_path.display(),
        depinfo
    );
    assert!(
        // Match either separator so a leaked sentinel can't slip through on
        // Windows (`__kache_root__\`).
        !depinfo.contains("__kache_root__"),
        "restored-facing dep-info must not expose kache sentinels in {}:\n{}",
        depinfo_path.display(),
        depinfo
    );

    let report = kache_report(cache_dir.path());
    assert!(
        report["summary"]["local_hits"].as_u64().unwrap_or(0) >= 1,
        "second build should restore at least one artifact from the local cache: {report}"
    );
}

/// True if `cc` on PATH accepts the GNU-dialect flag set the aws-lc-sys test
/// below drives, checked by actually compiling with it.
///
/// The flags are the ones aws-lc-sys passes to a non-cl-like driver; on a
/// cl-like driver it uses `/FI` instead and this shape never arises, so a
/// compiler that rejects them has nothing to say about the fix. Probing beats
/// guessing from the platform: the runner's `cc` may be MSVC, clang-cl, clang
/// or gcc, and only the driver itself knows which spellings it takes.
fn cc_accepts_gnu_forced_include(dir: &Path) -> bool {
    let header = dir.join("probe.h");
    let source = dir.join("probe.c");
    if std::fs::write(&header, "#define PROBE 1\n").is_err()
        || std::fs::write(&source, "int probe(void) { return PROBE; }\n").is_err()
    {
        return false;
    }
    std::process::Command::new("cc")
        .arg("-c")
        .arg(&source)
        .arg("-o")
        .arg(dir.join("probe.o"))
        .arg(format!("--include={}", header.display()))
        .args(["-fwrapv", "--param", "ssp-buffer-size=4", "-O0"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// aws-lc-sys drives BoringSSL symbol prefixing with
/// `--include=<generated-include>/boringssl_prefix_symbols*.h` and compiles
/// jitterentropy with `-fwrapv --param ssp-buffer-size=4`. Unclassified,
/// those flags sent ~62 TUs through as passthrough, so the archive they land
/// in differed per checkout and the `extern:` content hash re-keyed the whole
/// rustls/TLS subtree above it (#580).
///
/// Two project dirs share one cache and one forced-include header — the
/// registry-path shape, where the header is stable across checkouts but the
/// source dir is not. The second dir must hit the first dir's entry.
#[test]
fn test_cc_forced_include_and_param_converge_across_clones_issue_580() {
    let probe_dir = TempDir::new().unwrap();
    if !cc_accepts_gnu_forced_include(probe_dir.path()) {
        eprintln!(
            "skipping: `cc` does not accept --include=/-fwrapv/--param \
             (cl-like driver, or no cc on PATH)"
        );
        return;
    }
    build_kache();

    let cache_dir = TempDir::new().unwrap();
    // Stands in for `$CARGO_HOME/registry/src/…/generated-include` — one
    // absolute path both clones pass verbatim.
    let shared = TempDir::new().unwrap();
    let prefix_header = shared.path().join("boringssl_prefix_symbols.h");
    std::fs::write(&prefix_header, "#define AWS_LC_PFX(name) pfx_##name\n").unwrap();

    let source = "#if !defined(AWS_LC_PFX)\n#error \"forced include did not apply\"\n#endif\n\
         int AWS_LC_PFX(add_one)(int a) { return a + 1; }\n";

    let clone_a = TempDir::new().unwrap();
    let clone_b = TempDir::new().unwrap();
    for clone in [&clone_a, &clone_b] {
        std::fs::write(clone.path().join("bcm.c"), source).unwrap();
    }

    let forced_include = format!("--include={}", prefix_header.display());
    let args = [
        "cc",
        "-c",
        "bcm.c",
        "-o",
        "bcm.o",
        &forced_include,
        "-fwrapv",
        "--param",
        "ssp-buffer-size=4",
        "-O0",
        "-g0",
    ];

    // Cold: compiles and stores. A passthrough would record neither, so the
    // miss count is what proves the flags classified at all.
    run_kache_cc(clone_a.path(), cache_dir.path(), &args);
    assert!(clone_a.path().join("bcm.o").exists());
    let report = kache_report(cache_dir.path());
    // Named assertion with the raw event log attached: "left: Some(0), right:
    // Some(1)" says nothing about WHY kache declined, and the reason (a
    // passthrough reason string, an unresolvable `-###` probe) is the whole
    // content of a failure here.
    assert_eq!(
        report["summary"]["misses"].as_u64(),
        Some(1),
        "cold compile must be cached, not passed through.\nevents.jsonl:\n{}",
        std::fs::read_to_string(cache_dir.path().join("events.jsonl"))
            .unwrap_or_else(|e| format!("(unreadable: {e})"))
    );
    assert_cc_report_counts(&report, 1, 0);

    // Different source dir, same content and same forced-include path.
    run_kache_cc(clone_b.path(), cache_dir.path(), &args);
    assert!(clone_b.path().join("bcm.o").exists());
    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 1);

    // The opposite `-fwrapv` polarity must NOT hit — both clang and gcc
    // resolve the two spellings to different cc1 token streams, so this
    // guards the missed-polarity class of #422/#426 on the flag added here.
    //
    // Asserted as "local_hits did not grow", not "misses grew": a distinct
    // key whose object comes out byte-identical (as it does for this TU under
    // clang, where wrapping semantics change nothing about `a + 1`) is
    // recorded as a `dup`, not a miss. Either bucket is correct — a false HIT
    // is the only wrong answer, so that is what the assertion pins.
    //
    // Note what is deliberately NOT asserted: that a changed `--param` VALUE
    // misses. gcc forwards `--param=<name>=<value>` to cc1 (so it does miss
    // there), but clang drops the option before cc1 — and also ignores it, so
    // the objects are identical and the hit is correct rather than lossy. The
    // gcc side of that premise is pinned on a frozen `-###` fixture in
    // `probe::resolve` instead of on whichever compiler runs this test.
    let no_wrapv = [
        "cc",
        "-c",
        "bcm.c",
        "-o",
        "bcm.o",
        &forced_include,
        "-fno-wrapv",
        "--param",
        "ssp-buffer-size=4",
        "-O0",
        "-g0",
    ];
    run_kache_cc(clone_a.path(), cache_dir.path(), &no_wrapv);
    let report = kache_report(cache_dir.path());
    let summary = &report["summary"];
    assert_eq!(
        summary["local_hits"].as_u64(),
        Some(1),
        "-fno-wrapv must not reuse the -fwrapv entry: {report}"
    );
    assert_eq!(
        summary["misses"].as_u64().unwrap_or(0) + summary["dups"].as_u64().unwrap_or(0),
        2,
        "-fno-wrapv should have compiled under its own key: {report}"
    );
}

#[test]
fn test_cc_depinfo_sidecar_restores_on_hit_and_new_mf_path() {
    build_kache();

    let project = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    std::fs::create_dir_all(project.path().join("src")).unwrap();
    std::fs::create_dir_all(project.path().join("build")).unwrap();
    std::fs::write(
        project.path().join("src/bar.h"),
        "#define BAR_GREETING \"hello depinfo\"\n",
    )
    .unwrap();
    std::fs::write(
        project.path().join("src/foo.c"),
        "#include \"bar.h\"\nconst char *greeting(void) { return BAR_GREETING; }\n",
    )
    .unwrap();

    let base_args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-Isrc",
        "-c",
        "src/foo.c",
        "-o",
        "build/foo.o",
    ];

    run_kache_cc(project.path(), cache_dir.path(), &base_args);
    let cold_depinfo = std::fs::read_to_string(project.path().join("build/foo.d")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert!(cold_depinfo.contains("build/foo.o"));
    assert!(cold_depinfo.contains("src/foo.c"));
    assert!(cold_depinfo.contains("src/bar.h"));
    assert!(!cold_depinfo.contains("./foo.o"));
    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 0);
    assert_last_cc_event(&report, "miss", 1);

    std::fs::remove_dir_all(project.path().join("build")).unwrap();
    run_kache_cc(project.path(), cache_dir.path(), &base_args);
    let warm_depinfo = std::fs::read_to_string(project.path().join("build/foo.d")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert_eq!(warm_depinfo, cold_depinfo);
    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 1);
    assert_last_cc_event(&report, "local_hit", 0);

    std::fs::remove_dir_all(project.path().join("build")).unwrap();
    let mf_args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-MF",
        "deps/custom.d",
        "-Isrc",
        "-c",
        "src/foo.c",
        "-o",
        "build/foo.o",
    ];
    run_kache_cc(project.path(), cache_dir.path(), &mf_args);
    let mf_depinfo = std::fs::read_to_string(project.path().join("deps/custom.d")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert!(!project.path().join("build/foo.d").exists());
    assert_eq!(mf_depinfo, cold_depinfo);
    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 2);
    assert_last_cc_event(&report, "local_hit", 0);

    let pp_cache_dir = TempDir::new().unwrap();
    let _ = std::fs::remove_dir_all(project.path().join("build"));
    let _ = std::fs::remove_dir_all(project.path().join("deps"));
    std::fs::create_dir_all(project.path().join("build")).unwrap();
    std::fs::create_dir_all(project.path().join("deps")).unwrap();
    let pp_args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-MF",
        "deps/custom.pp",
        "-Isrc",
        "-c",
        "src/foo.c",
        "-o",
        "build/foo.o",
    ];
    run_kache_cc(project.path(), pp_cache_dir.path(), &pp_args);
    let cold_pp_depinfo = std::fs::read_to_string(project.path().join("deps/custom.pp")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert!(cold_pp_depinfo.contains("build/foo.o"));
    assert!(cold_pp_depinfo.contains("src/foo.c"));
    assert!(cold_pp_depinfo.contains("src/bar.h"));
    let report = kache_report(pp_cache_dir.path());
    assert_cc_report_counts(&report, 1, 0);
    assert_last_cc_event(&report, "miss", 1);

    std::fs::remove_dir_all(project.path().join("build")).unwrap();
    std::fs::remove_dir_all(project.path().join("deps")).unwrap();
    run_kache_cc(project.path(), pp_cache_dir.path(), &pp_args);
    let warm_pp_depinfo = std::fs::read_to_string(project.path().join("deps/custom.pp")).unwrap();
    assert!(project.path().join("build/foo.o").exists());
    assert_eq!(warm_pp_depinfo, cold_pp_depinfo);
    let report = kache_report(pp_cache_dir.path());
    assert_cc_report_counts(&report, 1, 1);
    assert_last_cc_event(&report, "local_hit", 0);
}

#[test]
fn test_cc_depinfo_restore_preserves_parent_relative_deps() {
    build_kache();

    let project = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let source_dir = project.path().join("src");
    let object_dir = project.path().join("obj/a/b/c");
    std::fs::create_dir_all(&source_dir).unwrap();
    std::fs::create_dir_all(object_dir.join(".deps")).unwrap();
    std::fs::write(source_dir.join("bar.h"), "#define VALUE 42\n").unwrap();
    std::fs::write(
        source_dir.join("foo.c"),
        "#include \"bar.h\"\nint answer(void) { return VALUE; }\n",
    )
    .unwrap();

    let args = [
        "cc",
        "-O0",
        "-g0",
        "-MMD",
        "-MP",
        "-MF",
        ".deps/foo.o.pp",
        "-I../../../../src",
        "-c",
        "../../../../src/foo.c",
        "-o",
        "foo.o",
    ];

    run_kache_cc_from(&object_dir, cache_dir.path(), &args);
    let cold_depinfo = std::fs::read_to_string(object_dir.join(".deps/foo.o.pp")).unwrap();
    assert!(object_dir.join("foo.o").exists());
    assert!(
        cold_depinfo.contains("../../../../src/foo.c"),
        "cold depfile should preserve compiler parent-relative source path: {cold_depinfo}"
    );
    assert!(
        cold_depinfo.contains("../../../../src/bar.h"),
        "cold depfile should preserve compiler parent-relative header path: {cold_depinfo}"
    );
    assert!(
        !cold_depinfo.contains("__kache_root__/"),
        "restored-facing depfiles must not expose kache sentinels: {cold_depinfo}"
    );

    std::fs::remove_file(object_dir.join("foo.o")).unwrap();
    std::fs::remove_dir_all(object_dir.join(".deps")).unwrap();
    std::fs::create_dir_all(object_dir.join(".deps")).unwrap();

    run_kache_cc_from(&object_dir, cache_dir.path(), &args);
    let warm_depinfo = std::fs::read_to_string(object_dir.join(".deps/foo.o.pp")).unwrap();
    assert!(object_dir.join("foo.o").exists());
    assert_eq!(
        warm_depinfo, cold_depinfo,
        "cache-hit restore must reproduce parent-relative depfiles byte-for-byte"
    );
    assert!(
        !warm_depinfo.contains(&object_dir.to_string_lossy().to_string()),
        "restore must not inject the object dir into parent-relative paths: {warm_depinfo}"
    );

    let report = kache_report(cache_dir.path());
    assert_cc_report_counts(&report, 1, 1);
}

#[test]
fn test_auto_gc_bounds_store_size() {
    build_kache();

    let test_project = TempDir::new().unwrap();
    let cache_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();
    let src_dir = test_project.path().join("src");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::write(
        test_project.path().join("Cargo.toml"),
        r#"[package]
name = "hello-world"
version = "0.1.0"
edition = "2021"

[workspace]
"#,
    )
    .unwrap();
    std::fs::write(
        src_dir.join("main.rs"),
        r#"fn main() {
    println!("Hello from kache test project v1!");
}
"#,
    )
    .unwrap();

    // First populate one entry with auto-GC disabled. The budget used for the
    // second build is derived from this entry's actual size, which varies
    // substantially across platforms/toolchains.
    let config_content = r#"[cache]
local_only = true
local_max_size = "10GiB"
auto_gc = false
cache_executables = true
"#;
    let config_path = isolated_config_path(cache_dir.path());
    std::fs::write(&config_path, config_content).unwrap();

    // First build (populates the cache past the size budget)
    let output = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(&test_project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", &config_path)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run cargo build");
    assert!(
        output.status.success(),
        "first cargo build failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    println!("STDOUT:\n{}", String::from_utf8_lossy(&output.stdout));
    println!("STDERR:\n{}", String::from_utf8_lossy(&output.stderr));

    // Connect to SQLite to check initial size and age the entries
    let db_path = cache_dir.path().join("index.db");
    assert!(db_path.exists(), "index.db should exist");

    let total_store_size = || -> i64 {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "busy_timeout", "5000").unwrap();
        conn.query_row("SELECT COALESCE(SUM(size), 0) FROM entries", [], |row| {
            row.get(0)
        })
        .unwrap()
    };

    let age_entries = || {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "busy_timeout", "5000").unwrap();
        conn.execute(
            "UPDATE entries SET last_accessed = datetime('now', '-300 seconds'), created_at = datetime('now', '-300 seconds')",
            [],
        )
        .unwrap();
    };

    let size_before = total_store_size();

    let entry_count_before: i64 = {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.pragma_update(None, "busy_timeout", "5000").unwrap();
        conn.query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
            .unwrap()
    };
    assert_eq!(
        entry_count_before, 1,
        "first build should populate exactly one cache entry"
    );

    println!("Store size before GC: {size_before} bytes");
    assert!(size_before > 0, "store should be populated");

    // Age the first entry so the background worker's first sweep can evict it
    // immediately. This avoids depending on the retry-delay env var surviving
    // Cargo's rustc-wrapper environment on every platform.
    age_entries();

    // Set the budget so one entry fits but two entries exceed max+slack. The
    // second build below changes the source to force a second cache entry; GC
    // should evict the aged first entry and leave the fresh entry intact.
    let gc_budget = ((size_before as u64) * 3 / 2).max(size_before as u64 + 1);
    let config_content = format!(
        r#"[cache]
local_only = true
local_max_size = "{gc_budget}B"
auto_gc = true
cache_executables = true
"#
    );
    std::fs::write(&config_path, config_content).unwrap();

    std::fs::write(
        src_dir.join("main.rs"),
        r#"fn main() {
    println!("Hello from kache test project v2!");
}
"#,
    )
    .unwrap();

    // Second build creates a second entry and triggers put() -> maybe_spawn_auto_gc
    let output = std::process::Command::new("cargo")
        .args(["build"])
        .current_dir(test_project.path())
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir.path())
        .env("KACHE_CONFIG", &config_path)
        .env("CARGO_TARGET_DIR", target_dir.path())
        .env("CARGO_INCREMENTAL", "0")
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run cargo build");
    assert!(
        output.status.success(),
        "second cargo build failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("auto-gc: spawned background `kache gc`"),
        "second build should trigger background auto-GC.\nstderr: {stderr}"
    );

    // Poll fresh SQLite connections until the aged first entry is evicted.
    let mut size_after = total_store_size();
    let start = std::time::Instant::now();
    while start.elapsed() < std::time::Duration::from_secs(10) {
        size_after = total_store_size();
        if size_after <= gc_budget as i64 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    println!("Store size after GC: {size_after} bytes");
    if size_after > gc_budget as i64 {
        let log_path = cache_dir.path().join("auto-gc.log");
        if log_path.exists()
            && let Ok(log_content) = std::fs::read_to_string(&log_path)
        {
            println!("--- AUTO-GC.LOG CONTENT ---");
            println!("{}", log_content);
            println!("----------------------------");
        }
    }
    assert!(
        size_after <= gc_budget as i64,
        "auto-GC failed to evict store below budget {gc_budget}, current size: {size_after}"
    );
}

/// Issue #505: unrecognized RUSTC_WORKSPACE_WRAPPER tools must pass through
/// uncached — the wrapper must execute on every build, not just the first.
/// If someone routes unknown wrappers back through the cache, the second
/// invocation would be a cache hit and the wrapper would NOT execute,
/// failing the marker-count assertion.
#[cfg(unix)]
#[test]
fn workspace_wrapper_passthrough_executes_every_time() {
    use std::os::unix::fs::PermissionsExt;

    build_kache();
    let cache_dir = TempDir::new().unwrap();
    let marker = cache_dir.path().join("wrapper-runs.log");

    // Fake workspace wrapper: records each execution, then forwards to rustc.
    let wrapper = cache_dir.path().join("fake-driver");
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\necho ran >> {}\nexec \"$@\"\n", marker.display()),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let src = cache_dir.path().join("lib.rs");
    std::fs::write(&src, "pub fn foo() -> u32 { 42 }\n").unwrap();
    let out = cache_dir.path().join("out.rlib");

    let run = || {
        std::process::Command::new(kache_binary())
            .arg(&wrapper)
            .arg("rustc")
            .args([
                "--edition",
                "2024",
                "--crate-type",
                "lib",
                "--crate-name",
                "fakedriver",
                "-o",
                out.to_str().unwrap(),
                src.to_str().unwrap(),
            ])
            .env("KACHE_CACHE_DIR", cache_dir.path())
            .env("KACHE_CONFIG", isolated_config_path(cache_dir.path()))
            .env_remove("RUSTC_WRAPPER")
            .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
            .output()
            .expect("failed to run kache")
    };

    let out1 = run();
    assert!(
        out1.status.success(),
        "first build failed\nstderr: {}",
        String::from_utf8_lossy(&out1.stderr)
    );
    let count1 = std::fs::read_to_string(&marker).unwrap().lines().count();
    assert_eq!(count1, 1);

    let out2 = run();
    assert!(
        out2.status.success(),
        "second build failed\nstderr: {}",
        String::from_utf8_lossy(&out2.stderr)
    );
    let count2 = std::fs::read_to_string(&marker).unwrap().lines().count();
    assert_eq!(
        count2, 2,
        "wrapper must execute on every build (uncached passthrough)"
    );
}
