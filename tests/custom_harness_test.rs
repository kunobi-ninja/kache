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
//! The restored mode depends on which materialization path runs, and only one
//! of them preserves the bit by accident:
//!
//! | path | when | mode |
//! |---|---|---|
//! | reflink | cache and target on one CoW filesystem | fresh file at the umask |
//! | hardlink | one filesystem, blob copy-ingested | blob's `0o555` — survives |
//! | hardlink | one filesystem, blob reflink-ingested | blob's `0o444` |
//! | copy | cache and target on different filesystems | `0o644`, hardcoded |
//!
//! So the only configuration that accidentally works is a same-filesystem
//! hardlink from a copy-ingested blob — which is what a plain ext4 checkout
//! with `TMPDIR` alongside it produces, and why this went unnoticed.
//!
//! Running under `TMPDIR` would land in exactly that blind spot, so this test
//! uses the repository's filesystem instead. `wrapper::tests` asserts the same
//! contract directly, without depending on any of it.

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

    // Drop every build output so the second run has to restore the test binary
    // from the cache rather than reuse the one the linker just produced.
    std::fs::remove_dir_all(target.path()).unwrap();
    std::fs::create_dir_all(target.path()).unwrap();

    let warm = cargo_test(project.path(), cache.path(), target.path());
    assert!(
        warm.status.success(),
        "restored harness=false test binary could not be run.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&warm.stdout),
        String::from_utf8_lossy(&warm.stderr),
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
