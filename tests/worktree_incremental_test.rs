//! Real Cargo/rustc coverage for the explicit worktree seed experiment.
#![cfg(unix)]

use std::{fs, path::Path, process::Output};

#[allow(dead_code)]
mod common;

fn project(root: &Path, answer: u64) {
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname='worktree-probe'\nversion='0.1.0'\nedition='2024'\n[workspace]\n",
    )
    .unwrap();
    fs::write(root.join("src/main.rs"), format!(
        "#[inline(never)] fn answer() -> u64 {{ {answer} }}\nfn main() {{ println!(\"{{}}|{{}}\", answer(), env!(\"CARGO_MANIFEST_DIR\")); }}\n"
    )).unwrap();
}

fn build(root: &Path, donor: Option<&Path>, extra_flags: &str) -> Output {
    let cache = root.join("cache");
    let mut command = common::hermetic_command("cargo", &cache, Some(&root.join("absent.toml")));
    command
        .current_dir(root)
        .args(["build", "--offline", "--quiet"])
        .env("RUSTC_WRAPPER", env!("CARGO_BIN_EXE_kache"))
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env("CARGO_INCREMENTAL", "1")
        .env("KACHE_PRESERVE_INCREMENTAL", "0")
        .env("KACHE_INCREMENTAL_CRATES", "worktree_probe")
        .env("KACHE_UNSTABLE", "true")
        .env("KACHE_ADAPTIVE_INCREMENTAL", "0")
        // Only this test's compiler children opt into unstable flags.
        .env("RUSTC_BOOTSTRAP", "1")
        .env(
            "RUSTFLAGS",
            format!("-Zremap-cwd-prefix=/src -Zincremental-info {extra_flags}"),
        )
        .env_remove("RUSTC_WORKSPACE_WRAPPER")
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    if let Some(donor) = donor {
        command.env("KACHE_INCREMENTAL_SEED_FROM", donor);
    } else {
        command.env_remove("KACHE_INCREMENTAL_SEED_FROM");
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn check_binary(root: &Path, answer: u64) {
    let output = std::process::Command::new(root.join("target/debug/worktree-probe"))
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("{answer}|{}", root.display())
    );
}

fn reused(output: &Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("completely ignoring cache"), "{stderr}");
    stderr.lines().any(|line| {
        line.contains("session directory:")
            && line.contains("files hard-linked")
            && !line.contains(": 0 files")
    })
}

fn supports_cwd_remap() -> bool {
    // Older toolchains cannot take part in this experiment. Keep the ordinary
    // test suite usable when the selected compiler lacks the required flag.
    let help = std::process::Command::new("rustc")
        .args(["-Z", "help"])
        .env("RUSTC_BOOTSTRAP", "1")
        .output()
        .unwrap();
    String::from_utf8_lossy(&help.stdout).contains("remap-cwd-prefix")
}

#[test]
fn imports_across_checkout_paths_and_rechecks_sources_and_compile_time_env() {
    if !supports_cwd_remap() {
        eprintln!("skipping worktree seed test: rustc lacks remap-cwd-prefix");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let donor = root.join("donor");
    let receiver = root.join("deeper/receiver");
    let incompatible = root.join("incompatible");
    let cold = root.join("cold");
    project(&donor, 1);
    project(&receiver, 2);
    project(&incompatible, 3);
    project(&cold, 4);
    let profile = donor.join("target/debug");
    assert!(!reused(&build(&donor, Some(&profile), "")));
    check_binary(&donor, 1);
    let seeded = build(&receiver, Some(&profile), "");
    assert!(
        reused(&seeded),
        "{}",
        String::from_utf8_lossy(&seeded.stderr)
    );
    check_binary(&receiver, 2);
    assert!(!reused(&build(
        &incompatible,
        Some(&profile),
        "-Copt-level=1"
    )));
    check_binary(&incompatible, 3);
    assert!(!reused(&build(&cold, None, "")));
    check_binary(&cold, 4);
}

#[test]
fn absolute_source_keeps_file_macro_semantics_without_publishing_a_seed() {
    if !supports_cwd_remap() {
        eprintln!("skipping worktree seed test: rustc lacks remap-cwd-prefix");
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    let source = root.join("src/main.rs");
    fs::create_dir(root.join("src")).unwrap();
    fs::write(&source, "fn main() { println!(\"{}\", file!()); }").unwrap();
    let profile = root.join("target/debug");
    let deps = profile.join("deps");
    fs::create_dir_all(&deps).unwrap();
    let cache = root.join("cache");
    let output = common::hermetic_command(
        env!("CARGO_BIN_EXE_kache"),
        &cache,
        Some(&root.join("absent.toml")),
    )
    .current_dir(&root)
    .args([
        "rustc",
        "--crate-name",
        "absolute_probe",
        "--crate-type=bin",
        "-Cextra-filename=-1234abcd",
        "-Zremap-cwd-prefix=/src",
    ])
    .arg(&source)
    .arg("--out-dir")
    .arg(&deps)
    .arg(format!(
        "-Cincremental={}",
        profile.join("incremental").display()
    ))
    .env("KACHE_INCREMENTAL_CRATES", "absolute_probe")
    .env("KACHE_UNSTABLE", "true")
    .env("KACHE_PRESERVE_INCREMENTAL", "0")
    .env("KACHE_ADAPTIVE_INCREMENTAL", "0")
    .env("KACHE_INCREMENTAL_SEED_FROM", &profile)
    .env("RUSTC_BOOTSTRAP", "1")
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result = std::process::Command::new(deps.join("absolute_probe-1234abcd"))
        .output()
        .unwrap();
    assert!(result.status.success());
    assert_eq!(
        String::from_utf8(result.stdout).unwrap().trim(),
        "/src/src/main.rs"
    );
    for unit in fs::read_dir(profile.join("incremental.kache-auto/v1")).unwrap() {
        assert!(!unit.unwrap().path().join("worktree-seed-v1").exists());
    }
}

fn automatic_project(root: &Path) {
    project(root, 1);
    fs::write(root.join("build.rs"), r#"fn main() {
        println!("cargo:rustc-env=BUILD_BOOTSTRAP={}", option_env!("RUSTC_BOOTSTRAP").unwrap_or("unset"));
    }"#).unwrap();
    fs::write(root.join("src/main.rs"), r#"fn main() {
        println!("{}|{}", option_env!("RUSTC_BOOTSTRAP").unwrap_or("unset"), env!("BUILD_BOOTSTRAP"));
    }"#).unwrap();
}

fn automatic_command(
    root: &Path,
    unstable: &str,
    bootstrap: Option<&str>,
) -> std::process::Command {
    let mut command = common::hermetic_command(
        "cargo",
        &root.join("cache"),
        Some(&root.join("absent.toml")),
    );
    command
        .current_dir(root)
        .args(["build", "--offline", "--quiet"])
        .env("RUSTC_WRAPPER", env!("CARGO_BIN_EXE_kache"))
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env("CARGO_INCREMENTAL", "1")
        .env("KACHE_PRESERVE_INCREMENTAL", "0")
        .env("KACHE_INCREMENTAL_CRATES", "worktree_probe")
        .env("KACHE_ADAPTIVE_INCREMENTAL", "0")
        .env("KACHE_UNSTABLE", unstable)
        .env("KACHE_INCREMENTAL_SEED_FROM", root.join("target/debug"))
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_RUSTFLAGS")
        .env_remove("RUSTC_WORKSPACE_WRAPPER");
    if let Some(value) = bootstrap {
        command.env("RUSTC_BOOTSTRAP", value);
    } else {
        command.env_remove("RUSTC_BOOTSTRAP");
    }
    command
}

fn marker_count(root: &Path) -> usize {
    fs::read_dir(root.join("target/debug/incremental.kache-auto/v1"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|unit| unit.path().join("worktree-seed-v1").is_file())
        .count()
}

fn automatic_output(root: &Path) -> String {
    let result = std::process::Command::new(root.join("target/debug/worktree-probe"))
        .output()
        .unwrap();
    assert!(result.status.success());
    String::from_utf8(result.stdout).unwrap().trim().to_owned()
}

#[test]
fn automatic_mode_scopes_bootstrap_and_preserves_explicit_selections() {
    if !supports_cwd_remap() {
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    for (name, unstable, bootstrap, expected, markers) in [
        ("off", "0", None, "unset|unset", 0),
        (
            "off-with-existing-permission",
            "false",
            Some("worktree_probe"),
            "worktree_probe|worktree_probe",
            0,
        ),
        ("explicit-denial", "true", Some("-1"), "-1|-1", 0),
        (
            "existing-selection",
            "true",
            Some("worktree_probe"),
            "worktree_probe|worktree_probe",
            1,
        ),
    ] {
        let project = root.join(name);
        automatic_project(&project);
        let output = automatic_command(&project, unstable, bootstrap)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(automatic_output(&project), expected);
        assert_eq!(marker_count(&project), markers);
    }
    // Exercise the override only when the selected compiler is stable.
    let vv = std::process::Command::new("rustc")
        .arg("-vV")
        .output()
        .unwrap();
    if String::from_utf8_lossy(&vv.stdout).contains("nightly")
        || String::from_utf8_lossy(&vv.stdout).contains("-dev")
    {
        return;
    }
    let project = root.join("scoped");
    automatic_project(&project);
    let output = automatic_command(&project, "true", None)
        .env_remove("KACHE_INCREMENTAL_SEED_FROM")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        automatic_output(&project),
        "1|unset",
        "Cargo and build scripts must not receive Kache's override"
    );
    assert_eq!(marker_count(&project), 1);
    let project = root.join("source-features");
    automatic_project(&project);
    fs::write(
        project.join("src/main.rs"),
        "#![feature(test)]\nfn main() {}\n",
    )
    .unwrap();
    let output = automatic_command(&project, "true", None).output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("error[E0725]"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(marker_count(&project), 0);
}

#[test]
fn unsupported_compiler_compiles_normally_and_memoizes_negative_probes() {
    let temp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(temp.path()).unwrap();
    automatic_project(&root);
    let shim = root.join("rustc-unsupported");
    kache_fs::testutil::write_executable(
        &shim,
        r#"#!/bin/sh
for flag in "$@"; do
    case "$flag" in -Zremap-cwd-prefix=*) printf '%s\n' "${RUSTC_BOOTSTRAP-unset}" >> "$PROBE_COUNTER"; exit 1;; esac
done
exec "$REAL_RUSTC" "$@"
"#,
    );
    let sysroot = std::process::Command::new("rustc")
        .args(["--print", "sysroot"])
        .output()
        .unwrap();
    let real = Path::new(String::from_utf8_lossy(&sysroot.stdout).trim()).join("bin/rustc");
    let count = root.join("probe-counter");
    for edit in 0..2 {
        fs::write(root.join("src/main.rs"), format!("fn main() {{ println!(\"{edit}:{{}}\", option_env!(\"RUSTC_BOOTSTRAP\").unwrap_or(\"unset\")); }}")).unwrap();
        let output = automatic_command(&root, "true", None)
            .env("RUSTC", &shim)
            .env("REAL_RUSTC", &real)
            .env("PROBE_COUNTER", &count)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(automatic_output(&root), format!("{edit}:unset"));
        assert_eq!(marker_count(&root), 0);
    }
    assert_eq!(
        fs::read_to_string(count)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        ["unset", "1"]
    );
}
