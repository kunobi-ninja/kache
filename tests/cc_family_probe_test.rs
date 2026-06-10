//! End-to-end regression test for the `cc`-crate compiler-family probe
//! (issue #286: "Firefox fails to build swgl").
//!
//! When `CC="kache <compiler>"`, the `cc` crate mis-parses it (kache is
//! not in the crate's known-wrapper allowlist), treats kache as the
//! compiler, and runs the family probe as `kache -E <file>` — dropping
//! `<compiler>`. kache must recover the real compiler from the
//! environment and forward the probe to it. The old code hard-coded a
//! spawn of the system `cc`, which does not exist on Windows MSVC
//! toolchains, so the probe failed and the cc crate fell back to an
//! unsupported GNU family, aborting the whole build.
//!
//! This drives the real `kache` binary as the probe with `CC` pointing
//! at `kache <fake-compiler>`, and asserts the probe is forwarded to the
//! fake compiler (whose marker shows up on stdout) rather than the
//! system `cc`. Unix-only: it relies on a shell-script stand-in for the
//! compiler. The Windows shapes are covered by the `resolve_probe_compiler`
//! unit tests in `src/compiler/cc.rs`.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::Command;

/// Path to the kache binary under test (Cargo sets this for the
/// integration-test harness).
fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

/// The marker the fake compiler prints — proof the probe reached *it*
/// and not the system `cc`.
const FAKE_MARKER: &str = "__KACHE_FAKE_COMPILER_FAMILY_PROBE__";

#[test]
fn family_probe_forwards_to_real_compiler_recovered_from_cc_env() {
    let dir = tempfile::tempdir().unwrap();

    // A fake compiler: ignores its args, prints a unique marker. Stands
    // in for clang-cl on a host where `cc` may or may not exist.
    let fake = dir.path().join("fake-cc.sh");
    fs::write(&fake, format!("#!/bin/sh\necho '{FAKE_MARKER}'\nexit 0\n")).unwrap();
    fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();

    // The probe file the cc crate would have created (contents irrelevant
    // — the fake compiler ignores it).
    let probe_file = dir.path().join("detect_compiler_family.c");
    fs::write(&probe_file, b"int main(void){return 0;}\n").unwrap();

    // Hermetic cache/config so the probe run touches nothing real.
    let cache_dir = dir.path().join("cache");
    let config = dir.path().join("kache.toml");

    // Reproduce the cc crate's mis-parse: CC = "<kache> <real-compiler>".
    // The cc crate would invoke `kache -E <probe_file>`, dropping the
    // real compiler from argv — so kache must recover it from CC here.
    let cc_value = format!("{} {}", kache_binary(), fake.display());

    let output = Command::new(kache_binary())
        .args(["-E", probe_file.to_str().unwrap()])
        .env("CC", &cc_value)
        .env("KACHE_CACHE_DIR", &cache_dir)
        .env("KACHE_CONFIG", &config)
        // Make sure a stray `cc` on PATH can't accidentally satisfy the
        // probe and mask a regression: an empty PATH means the old
        // hard-coded `cc` spawn fails outright.
        .env("PATH", "")
        .output()
        .expect("failed to run kache as a cc-family probe");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "probe should succeed by forwarding to the recovered compiler; \
         status={:?}\nstdout={stdout}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains(FAKE_MARKER),
        "probe must forward to the CC-recovered compiler (marker missing). \
         Got stdout: {stdout:?}"
    );
}

/// Sanity: when no kache-wrapped CC variable exists, the probe falls
/// back to the system `cc` (preserving the original unix behaviour).
/// We only assert it does NOT pick up the fake compiler — proving the
/// fallback path is distinct from the recovery path.
#[test]
fn family_probe_without_kache_cc_does_not_use_fake_compiler() {
    let dir = tempfile::tempdir().unwrap();
    let probe_file = dir.path().join("detect_compiler_family.c");
    fs::write(&probe_file, b"int main(void){return 0;}\n").unwrap();

    let _ = PathBuf::from(kache_binary());
    let output = Command::new(kache_binary())
        .args(["-E", probe_file.to_str().unwrap()])
        // No CC var → nothing to recover → fall back to system `cc`.
        .env_remove("CC")
        .env_remove("CXX")
        .env("KACHE_CACHE_DIR", dir.path().join("cache"))
        .env("KACHE_CONFIG", dir.path().join("kache.toml"))
        .output()
        .expect("failed to run kache as a cc-family probe");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        !stdout.contains(FAKE_MARKER),
        "fallback path must not invent the fake compiler"
    );
}
