//! Link and run the launcher against the selected toolchain's real core.

#![cfg(unix)]

use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

#[path = "../launcher/compile.rs"]
mod launcher;

#[test]
fn the_launcher_links_with_current_core_and_runs_the_preserved_script() {
    let fixture = tempfile::tempdir().unwrap();
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let version = Command::new(&rustc).arg("-vV").output().unwrap();
    assert!(version.status.success(), "{version:?}");
    let version = String::from_utf8(version.stdout).unwrap();
    let host = version
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("launcher/build_script.rs");
    let out = fixture.path().join("launcher");
    let mut flags: Vec<OsString> = Vec::new();
    if let Some(linker) = std::env::var_os("RUSTC_LINKER") {
        let mut flag = OsString::from("linker=");
        flag.push(linker);
        flags.extend(["-C".into(), flag]);
    }
    if cfg!(all(target_os = "linux", target_env = "gnu")) {
        flags.extend(["-C".into(), "link-arg=-lc".into()]);
    }
    let output = launcher::build(&rustc, host, &source, &out, &flags).unwrap();
    assert!(output.status.success(), "{output:?}");

    // A successful link is the symbol oracle. Executing the fallback also
    // proves that the generated launcher remains usable, rather than merely
    // carrying a chosen string somewhere in its image.
    let preserved = fixture.path().join("preserved");
    std::fs::write(&preserved, "#!/bin/sh\nexit 23\n").unwrap();
    std::fs::set_permissions(&preserved, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(fixture.path().join(".kache-launch"), b"preserved\0\0./\0").unwrap();
    let output = Command::new(&out).env_clear().output().unwrap();
    assert_eq!(output.status.code(), Some(23), "{output:?}");
}
