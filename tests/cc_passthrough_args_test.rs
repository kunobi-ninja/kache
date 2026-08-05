//! End-to-end regression test for exact C/C++ passthrough arguments.
//!
//! A refused invocation must reach the selected compiler byte-for-byte at the
//! argument boundary: no cache-only prefix-map flags may be injected. The
//! `compose_cc_args` unit tests separately cover ordering on cacheable misses.
//!
//! Unix-only: it relies on a shell-script stand-in for the compiler. The
//! splice logic itself is also covered by the `compose_cc_args` unit
//! tests in `src/compiler/cc.rs`.

#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

fn kache_binary() -> &'static str {
    env!("CARGO_BIN_EXE_kache")
}

#[test]
fn refused_double_dash_invocation_is_exact_passthrough() {
    let dir = tempfile::tempdir().unwrap();

    // Fake compiler: dump every argument it receives, one per line, then
    // succeed. A real clang-cl would *fail* on this argv if a flag landed
    // after `--`; we assert the ordering directly instead of depending on
    // a real clang-cl being installed.
    // Named `cc` (no extension) so kache's wrapper recognizer accepts it
    // as a C compiler and a GNU dialect is inferred — the dialect that
    // injects `-ffile-prefix-map` today (clang-cl injection is gated by
    // #285).
    let argv_dump = dir.path().join("argv.txt");
    let fake = dir.path().join("cc");
    fs::write(
        &fake,
        format!(
            "#!/bin/sh\n: > '{dump}'\nfor a in \"$@\"; do printf '%s\\n' \"$a\" >> '{dump}'; done\nexit 0\n",
            dump = argv_dump.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&fake, fs::Permissions::from_mode(0o755)).unwrap();

    // A real source keeps the invocation representative of cc-rs.
    let source = dir.path().join("windows.c");
    fs::write(&source, b"int main(void){return 0;}\n").unwrap();

    let cache_dir = dir.path().join("cache");
    let config = dir.path().join("kache.toml");

    // Mirror the cc-rs clang-cl shape: flags, an object output, then the
    // source behind a `--` separator. A GNU-dialect compiler name keeps
    // prefix-map injection enabled (clang-cl injection is gated by #285),
    // so this guards the argv ordering on the path that injects today.
    let output = Command::new(kache_binary())
        .args([
            fake.to_str().unwrap(),
            "-c",
            "-o",
            "windows.o",
            "--",
            source.to_str().unwrap(),
        ])
        .current_dir(dir.path())
        .env("KACHE_CACHE_DIR", &cache_dir)
        .env("KACHE_CONFIG", &config)
        .env("KACHE_BASE_DIR", dir.path())
        .env("KACHE_LOG", "kache=debug")
        .output()
        .expect("failed to run kache as a cc wrapper");

    assert!(
        output.status.success(),
        "kache cc passthrough should succeed; status={:?}\nstderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );

    let recorded = fs::read_to_string(&argv_dump).expect("fake compiler did not record argv");
    let args: Vec<&str> = recorded.lines().collect();

    assert_eq!(
        args,
        ["-c", "-o", "windows.o", "--", source.to_str().unwrap()],
        "refused passthrough must preserve the original argv without cache-only flags"
    );
}
