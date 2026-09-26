//! Compiles the build-script launcher (`launcher/build_script.rs`) for the
//! target kache is being built for. `src/build_script.rs` embeds the binary and
//! installs a copy wherever Cargo expects a build script it caches.

use std::path::PathBuf;
use std::process::Command;

const SOURCE: &str = "launcher/build_script.rs";

fn main() {
    println!("cargo:rerun-if-changed={SOURCE}");
    let out =
        PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR")).join("build-script-launcher");
    // Launchers are installed on Unix only; elsewhere nothing reads the bytes.
    if std::env::var_os("CARGO_CFG_UNIX").is_none() {
        std::fs::write(&out, b"").expect("writing the empty launcher");
        return;
    }

    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let target = std::env::var("TARGET").expect("TARGET");
    let mut command = Command::new(rustc);
    command
        .args(["--edition", "2024", "--crate-type", "bin"])
        .args(["--crate-name", "kache_build_script_launcher"])
        .args(["--target", &target])
        .args([
            "-C",
            "opt-level=s",
            "-C",
            "panic=abort",
            "-C",
            "codegen-units=1",
        ])
        .args(["-C", "debuginfo=0", "-C", "strip=symbols"])
        .arg("-o")
        .arg(&out)
        .arg(SOURCE);
    // Link the launcher the way Cargo links kache for this target.
    if let Some(linker) = std::env::var_os("RUSTC_LINKER") {
        let mut flag = std::ffi::OsString::from("linker=");
        flag.push(linker);
        command.arg("-C").arg(flag);
    }
    let encoded = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
    command.args(link_flags(
        encoded.split('\x1f').filter(|flag| !flag.is_empty()),
    ));
    // The launcher's own `link(name = "c")` puts libc before the standard
    // rlibs on the link line. nixpkgs builds compiler_builtins' C code with a
    // stack protector, and on aarch64 glibc exports `__stack_chk_guard` from
    // ld.so, which the linker then cannot reach (NixOS/nixpkgs#561411). A
    // std binary gets libc again after its rlibs; this one needs it asked for.
    if target_is_glibc() {
        command.args(["-C", "link-arg=-lc"]);
    }

    let status = command
        .status()
        .expect("running rustc for the build-script launcher");
    assert!(
        status.success(),
        "compiling {SOURCE} for {target} failed: {status}"
    );
}

/// Whether Cargo is building for a glibc Linux target.
fn target_is_glibc() -> bool {
    let cfg = |name: &str| std::env::var(name).unwrap_or_default();
    cfg("CARGO_CFG_TARGET_OS") == "linux" && cfg("CARGO_CFG_TARGET_ENV") == "gnu"
}

/// The `-C` options from the target's rustflags that decide how a binary
/// links. Everything else (coverage instrumentation, lints, cfgs) is kache's
/// own business, not the launcher's.
fn link_flags<'a>(mut flags: impl Iterator<Item = &'a str>) -> Vec<String> {
    const LINK_OPTIONS: &[&str] = &[
        "linker",
        "linker-flavor",
        "link-arg",
        "link-args",
        "link-self-contained",
        "target-feature",
    ];
    let mut kept = Vec::new();
    while let Some(flag) = flags.next() {
        let option = match flag.strip_prefix("-C") {
            Some("") => match flags.next() {
                Some(option) => option,
                None => break,
            },
            Some(option) => option,
            None => continue,
        };
        let name = option.split_once('=').map_or(option, |(name, _)| name);
        if LINK_OPTIONS.contains(&name) {
            kept.push(format!("-C{option}"));
        }
    }
    kept
}
