//! External C/C++ reports exercised through real clang and Make.
#![cfg(unix)]

use std::path::Path;
use std::process::Command;

#[allow(dead_code)]
mod common;

fn real_cc_regression(issue: &str) {
    for tool in ["clang", "make"] {
        if !Command::new(tool)
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
        {
            assert!(
                !cfg!(target_os = "linux") || std::env::var_os("CI").is_none(),
                "Linux CI must provide {tool} for C/C++ release regressions"
            );
            eprintln!("skipping issue {issue}: {tool} unavailable");
            return;
        }
    }
    let scratch = tempfile::tempdir().unwrap();
    let mut command = common::hermetic_command(
        "bash",
        &scratch.path().join("cache"),
        Some(&scratch.path().join("config.toml")),
    );
    let output = command
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/cc_release_regressions.sh"))
        .args([env!("CARGO_BIN_EXE_kache"), "clang", issue])
        .arg(scratch.path())
        .output()
        .expect("run real clang/Make regression");
    assert!(
        output.status.success(),
        "issue {issue}: {}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn restored_cc_object_relinks_the_cached_code() {
    real_cc_regression("1459");
}

#[test]
fn generated_cc_source_watches_the_consumers_headers() {
    real_cc_regression("1460");
}

#[test]
fn clang_diagnostic_formatting_reuses_the_first_object() {
    real_cc_regression("1461");
}

#[test]
fn clang_wasm_named_compile_output_hits_the_cache() {
    real_cc_regression("1462");
}
