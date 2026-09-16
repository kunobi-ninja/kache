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

    let status = command
        .status()
        .expect("running rustc for the build-script launcher");
    assert!(
        status.success(),
        "compiling {SOURCE} for {target} failed: {status}"
    );
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
