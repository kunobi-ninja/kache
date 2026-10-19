//! The launcher defines the personality routine under every name a precompiled
//! `core` may reference.
//!
//! The link itself already covers `rust_eh_personality`: the crate's own build
//! compiles this launcher and the pinned toolchain's `core` references that
//! name, so dropping it breaks the build. The v0-mangled name has no such
//! cover on a toolchain whose `core` predates the rename, which is why this
//! test compiles the launcher and looks for the symbol itself.

#![cfg(unix)]

use std::path::Path;
use std::process::Command;

/// `__rustc::rust_eh_personality` as the v0 mangling spells it.
const V0_PERSONALITY: &str = "_RNvCs6EUDi8WbMwz_7___rustc19rust_eh_personality";

/// Compile the launcher the way `build.rs` does, without the flags that would
/// drop the symbol table this test reads.
fn compile(source: &Path, out: &Path) -> std::process::Output {
    let mut command = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()));
    command
        .args(["--edition", "2024", "--crate-type", "bin"])
        .args(["--crate-name", "kache_build_script_launcher"])
        .args([
            "-C",
            "opt-level=s",
            "-C",
            "panic=abort",
            "-C",
            "debuginfo=0",
        ])
        .arg("-o")
        .arg(out)
        .arg(source);
    // The launcher's own `link(name = "c")` puts libc before the rlibs, which
    // nixpkgs' stack-protectored compiler_builtins needs repeated. See build.rs.
    if cfg!(all(target_os = "linux", target_env = "gnu")) {
        command.args(["-C", "link-arg=-lc"]);
    }
    command.output().expect("running rustc on the launcher")
}

#[test]
fn the_launcher_defines_the_personality_under_the_v0_mangled_name() {
    let fixture = tempfile::tempdir().unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("launcher/build_script.rs");
    let out = fixture.path().join("launcher");
    let output = compile(&source, &out);
    assert!(output.status.success(), "{output:?}");

    // The name reaches the binary as a symbol-table entry. No ELF parser is a
    // dependency of this crate, and a name this specific appears nowhere else
    // in the image.
    let image = std::fs::read(&out).unwrap();
    assert!(
        contains(&image, V0_PERSONALITY.as_bytes()),
        "the launcher defines no `{V0_PERSONALITY}`; a `core` newer than 2026-09-07 \
         cannot link it"
    );
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
