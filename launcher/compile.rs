//! Build the launcher against the personality name emitted by this compiler.

use std::ffi::{OsStr, OsString};
use std::io;
use std::path::Path;
use std::process::{Command, Output};

const PROBE: &str = r#"#![no_std]
#[unsafe(no_mangle)]
pub extern "C" fn kache_personality_probe(callback: extern "C-unwind" fn()) {
    callback();
}
"#;

/// Compile for the same target and link settings as the containing Kache.
pub(crate) fn build(
    rustc: &OsStr,
    target: &str,
    source: &Path,
    out: &Path,
    link_options: &[OsString],
) -> io::Result<Output> {
    let directory = out.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "launcher output has no parent")
    })?;
    let probe = directory.join("launcher-personality-probe.rs");
    let ir = directory.join("launcher-personality-probe.ll");
    std::fs::write(&probe, PROBE)?;
    // An unwind-capable callback crossing a non-unwinding C boundary makes
    // rustc name its personality in the IR. No probe executable is linked or
    // run, so this also works when HOST and TARGET differ.
    let output = Command::new(rustc)
        .args(["--edition", "2024", "--crate-type", "lib"])
        .args(["--crate-name", "kache_personality_probe"])
        .args(["--target", target, "--emit=llvm-ir", "-Cpanic=unwind"])
        .args(link_options)
        .arg("-o")
        .arg(&ir)
        .arg(&probe)
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "probing the launcher personality for {target}: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    let ir = std::fs::read_to_string(ir)?;
    let symbol = personality_symbol(&ir).map_err(io::Error::other)?;
    let mut command = Command::new(rustc);
    command
        .args(["--edition", "2024", "--crate-type", "bin"])
        .args(["--crate-name", "kache_build_script_launcher"])
        .args(["--target", target])
        .args(["-Copt-level=s", "-Cpanic=abort", "-Ccodegen-units=1"])
        .args(["-Cdebuginfo=0", "-Cstrip=symbols"])
        .arg("--check-cfg=cfg(kache_mangled_personality)")
        .env("KACHE_LAUNCHER_PERSONALITY", symbol)
        .args(link_options)
        .arg("-o")
        .arg(out)
        .arg(source);
    if symbol != "rust_eh_personality" {
        command.arg("--cfg=kache_mangled_personality");
    }
    command.output()
}

/// Read the actual function personality, ignoring declarations and strings.
/// The name is passed through without decoding or reproducing rustc's hash.
fn personality_symbol(ir: &str) -> Result<&str, &'static str> {
    let mut found = None;
    for line in ir.lines().filter(|line| line.starts_with("define ")) {
        let Some((_, rest)) = line.split_once(" personality ptr @") else {
            continue;
        };
        let symbol = rest.split_ascii_whitespace().next().unwrap_or_default();
        if !symbol.ends_with("rust_eh_personality")
            || !symbol
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err("rustc emitted an unsupported launcher personality");
        }
        if found.is_some_and(|previous| previous != symbol) {
            return Err("rustc emitted conflicting launcher personalities");
        }
        found = Some(symbol);
    }
    found.ok_or("rustc emitted no launcher personality; cannot select a safe export")
}

#[cfg(test)]
mod tests {
    use super::personality_symbol;

    #[test]
    fn reads_legacy_and_distinct_compiler_names() {
        for symbol in [
            "rust_eh_personality",
            "_RNvCsFirstCompiler_7___rustc19rust_eh_personality",
            "_RNvCsAnotherCompiler_7___rustc19rust_eh_personality",
        ] {
            let ir = format!("define void @probe() personality ptr @{symbol} {{\n}}");
            assert_eq!(personality_symbol(&ir).unwrap(), symbol);
        }
    }

    #[test]
    fn declarations_do_not_choose_the_export() {
        assert!(personality_symbol("declare i32 @rust_eh_personality(...)").is_err());
        let ir = "declare i32 @wrong_rust_eh_personality(...)\n\
                  define void @probe() personality ptr @rust_eh_personality {\n}";
        assert_eq!(personality_symbol(ir).unwrap(), "rust_eh_personality");
    }

    #[test]
    fn refuses_missing_unsupported_or_conflicting_personalities() {
        for ir in [
            "define void @probe() {\n}",
            "define void @probe() personality ptr @other_personality {\n}",
            "define void @probe() personality ptr @\"quoted_rust_eh_personality\" {\n}",
            "define void @a() personality ptr @rust_eh_personality {\n}\n\
             define void @b() personality ptr @different_rust_eh_personality {\n}",
        ] {
            assert!(personality_symbol(ir).is_err(), "{ir}");
        }
    }
}
