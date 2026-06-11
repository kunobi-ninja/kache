//! End-to-end regression test for issue #300 ("Firefox fails to build
//! whatsys on Windows").
//!
//! cc-rs emits a `--` end-of-options separator before the source file on
//! clang-cl invocations, and the clang/clang-cl driver treats every token
//! *after* `--` as an input file. kache injects `-ffile-prefix-map=…`
//! path-portability rules when it runs the real compiler; the old code
//! appended them to the very end of the argv, so they landed after `--`
//! and the driver parsed them as extra source files:
//!
//! ```text
//! clang-cl: error: cannot specify '-Fo…' when compiling multiple source files
//! clang-cl: warning: -ffile-prefix-map=…: 'linker' input unused
//! ```
//!
//! This drives the real `kache` binary as a `cc` wrapper around a fake
//! compiler that records the argv it actually receives, then asserts the
//! injected `-ffile-prefix-map` flag arrives *before* the `--` separator
//! — i.e. classified as an option, not an input. It exercises the full
//! path (wrapper → passthrough → `execute` → `compose_cc_args` → spawned
//! command), so a regression in the `execute` wiring — not just the
//! `compose_cc_args` helper — fails the test.
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
fn injected_prefix_map_lands_before_the_double_dash_separator() {
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

    // A real source so the prefix-map derivation has something to anchor
    // on; `KACHE_BASE_DIR` guarantees at least one map is injected
    // regardless of the test process's cwd.
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

    let sep = args.iter().position(|a| *a == "--");
    let map = args
        .iter()
        .position(|a| a.starts_with("-ffile-prefix-map="));

    assert!(
        map.is_some(),
        "kache should have injected a -ffile-prefix-map flag; recorded argv: {args:?}"
    );
    assert!(
        sep.is_some(),
        "the `--` separator should be preserved in the spawned argv: {args:?}"
    );
    assert!(
        map < sep,
        "injected -ffile-prefix-map (idx {map:?}) must precede `--` (idx {sep:?}), \
         else the driver treats it as an input file (#300); recorded argv: {args:?}"
    );
}
