//! End-to-end guard for restoring `[[test]] harness = false` binaries.
//!
//! A custom-harness test target supplies its own `main`, so cargo compiles it
//! with neither `--test` nor `--crate-type`. Its extensionless output therefore
//! classifies as `Other("rustc:unknown")`, whose link strategy is `Hardlink` —
//! and the restore path used to derive the output's permissions from that
//! strategy alone. The restored binary came back without its executable bit and
//! cargo failed the run with "Permission denied (os error 13)".
//!
//! The build here is a real one through the real wrapper: warm the cache, throw
//! the target directory away so every artifact has to come back from the cache,
//! then let cargo execute the restored binary.
//!
//! Two things keep that from passing for the wrong reason:
//!
//! - The warm run must record a `local_hit` for `harness` with zero compiler
//!   runs. A warm *miss* would relink the binary itself and satisfy the mode
//!   check without ever entering the restore path.
//! - The store blobs are stripped to `0o444` first. Otherwise some
//!   materialization paths reproduce `+x` for free — a copy-ingested blob keeps
//!   `0o555` (`set_blob_readonly` only drops the write bits), so a
//!   same-filesystem hardlink shares an executable inode, and macOS
//!   `clonefile` copies the source mode the same way. With the bit gone from
//!   the blob, reflink, hardlink and copy all yield a non-executable output,
//!   and `0o755` can only come from restore setting it deliberately.
//!
//! The test still runs on the repository's filesystem rather than `TMPDIR`:
//! that keeps it on the reflink/hardlink paths a real build takes, instead of
//! tmpfs's `fs::copy` fallback. `wrapper::tests` asserts the same contract
//! directly, without depending on any of it.

use std::path::Path;

use tempfile::TempDir;

mod common;
use common::{build_kache, isolated_config_path, kache_binary, scratch_dir};

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let dest_path = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest_path);
        } else {
            std::fs::copy(entry.path(), &dest_path).unwrap();
        }
    }
}

fn copy_fixture() -> TempDir {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("test-projects/custom-harness");
    let tmp = TempDir::new_in(scratch_dir()).unwrap();
    copy_dir(&fixture, tmp.path());
    tmp
}

/// Runs `cargo test --test harness` through the kache wrapper.
fn cargo_test(project: &Path, cache_dir: &Path, target_dir: &Path) -> std::process::Output {
    std::process::Command::new("cargo")
        .args(["test", "--test", "harness"])
        .current_dir(project)
        .env("RUSTC_WRAPPER", kache_binary())
        .env("KACHE_CACHE_DIR", cache_dir)
        .env("KACHE_CONFIG", isolated_config_path(cache_dir))
        .env("CARGO_TARGET_DIR", target_dir)
        .env("CARGO_INCREMENTAL", "0")
        .output()
        .expect("failed to run cargo test")
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

/// The most recent event for `crate_name`. `all_events` is in event-log order,
/// so after the warm run the last `harness` entry is the warm one.
fn last_event_for<'a>(report: &'a serde_json::Value, crate_name: &str) -> &'a serde_json::Value {
    report["all_events"]
        .as_array()
        .expect("report should include all_events")
        .iter()
        .rfind(|event| event["crate_name"].as_str() == Some(crate_name))
        .unwrap_or_else(|| {
            panic!(
                "no `{crate_name}` event in report: {}",
                serde_json::to_string_pretty(report).unwrap()
            )
        })
}

/// Drop the executable bit from every file in the store, so no restore path can
/// reproduce it by accident. Returns how many files were touched.
///
/// `0o444` rather than a bare `& !0o111`: blobs are stored read-only, and this
/// keeps that invariant while removing the bit that makes a hardlinked or
/// `clonefile`d output runnable for free.
#[cfg(unix)]
fn strip_executable_bits(dir: &Path) -> usize {
    use std::os::unix::fs::PermissionsExt;

    let mut stripped = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current).expect("reading store dir") {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                stack.push(path);
            } else {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))
                    .expect("chmod store blob");
                stripped += 1;
            }
        }
    }
    stripped
}

/// The restored test binary must still be runnable.
#[test]
fn custom_harness_test_binary_is_executable_after_restore() {
    build_kache();
    let project = copy_fixture();
    let cache = TempDir::new_in(scratch_dir()).unwrap();
    let target = TempDir::new_in(scratch_dir()).unwrap();

    let cold = cargo_test(project.path(), cache.path(), target.path());
    assert!(
        cold.status.success(),
        "cold build failed.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&cold.stdout),
        String::from_utf8_lossy(&cold.stderr),
    );

    // The cold run has to have compiled the harness target for the warm
    // assertion below to mean anything.
    let report = kache_report(cache.path());
    let cold_event = last_event_for(&report, "harness");
    assert_eq!(
        cold_event["compiler_runs"].as_u64(),
        Some(1),
        "cold run should have compiled `harness`: {cold_event}"
    );

    // Drop every build output so the second run has to restore the test binary
    // from the cache rather than reuse the one the linker just produced.
    std::fs::remove_dir_all(target.path()).unwrap();
    std::fs::create_dir_all(target.path()).unwrap();

    #[cfg(unix)]
    {
        let store = cache.path().join("store");
        let stripped = strip_executable_bits(&store);
        assert!(
            stripped > 0,
            "no files under {} — store layout changed?",
            store.display()
        );
    }

    let warm = cargo_test(project.path(), cache.path(), target.path());
    assert!(
        warm.status.success(),
        "restored harness=false test binary could not be run.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&warm.stdout),
        String::from_utf8_lossy(&warm.stderr),
    );

    // A warm miss would relink the binary and pass the mode check below without
    // exercising restore at all.
    let report = kache_report(cache.path());
    let warm_event = last_event_for(&report, "harness");
    assert_eq!(
        warm_event["result"].as_str(),
        Some("local_hit"),
        "warm run should have restored `harness` from the cache: {warm_event}"
    );
    assert_eq!(
        warm_event["compiler_runs"].as_u64(),
        Some(0),
        "warm run should not have spawned the compiler: {warm_event}"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let binary = std::fs::read_dir(target.path().join("debug").join("deps"))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.extension().is_none()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with("harness-"))
            })
            .expect("restored harness test binary");

        let mode = std::fs::metadata(&binary).unwrap().permissions().mode();
        assert_ne!(
            mode & 0o111,
            0,
            "{} was restored without its executable bit (mode {mode:o})",
            binary.display()
        );
    }
}
