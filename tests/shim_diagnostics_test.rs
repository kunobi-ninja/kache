//! First-party version-manager fallback fixtures; no external scripts.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

#[allow(dead_code)]
mod common;

fn executable(path: &Path, source: &str) {
    std::fs::write(path, source).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn init_detects_projected_fallback_loops_and_real_overrides_repair_both_tools() {
    let scratch = tempfile::tempdir().unwrap();
    let root = scratch.path().canonicalize().unwrap();
    let farm = root.join("custom farm");
    let manager = root.join("version manager");
    let real = root.join("real tools");
    std::fs::create_dir(&manager).unwrap();
    std::fs::create_dir(&real).unwrap();
    for name in ["cargo", "rustdoc"] {
        executable(
            &manager.join(name),
            &format!(
                "#!/bin/sh\nif [ -n \"${{KACHE_TEST_REAL_TOOLS-}}\" ]; then exec \"$KACHE_TEST_REAL_TOOLS/{name}\" \"$@\"; fi\nexec \"$KACHE_TEST_FARM/{name}\" \"$@\"\n"
            ),
        );
        executable(
            &real.join(name),
            &format!("#!/bin/sh\nprintf '{name} 1.99.0\\n'\n"),
        );
    }
    let command = || {
        let mut command = common::hermetic_command(
            env!("CARGO_BIN_EXE_kache"),
            &root.join("cache"),
            Some(&root.join("config.toml")),
        );
        command
            .current_dir(&root)
            .env("HOME", &root)
            .env("CARGO_HOME", root.join(".cargo"))
            .env("PATH", &manager)
            .env("KACHE_TEST_FARM", &farm)
            .env_remove("KACHE_REAL_CARGO")
            .env_remove("KACHE_REAL_RUSTDOC")
            .env_remove("KACHE_TEST_REAL_TOOLS")
            .env_remove("KACHE_BLAZE_REAL_CARGO")
            .args(["init", "--shims"])
            .arg(&farm)
            .arg("--canonical-only");
        assert_cmd::Command::from_std(command)
    };
    let failed = command().timeout(Duration::from_secs(15)).output().unwrap();
    assert!(!failed.status.success());
    let error = String::from_utf8_lossy(&failed.stderr);
    for expected in [
        "Cargo shim chain",
        "rustdoc shim chain",
        "forwards back",
        "KACHE_REAL_CARGO",
        "KACHE_REAL_RUSTDOC",
    ] {
        assert!(error.contains(expected), "missing {expected}: {error}");
    }
    // The interactive shell can put the manager before Kache. Doctor must
    // still discover and diagnose the Kache farm later in that PATH.
    let mut doctor = common::hermetic_command(
        env!("CARGO_BIN_EXE_kache"),
        &root.join("cache"),
        Some(&root.join("config.toml")),
    );
    doctor
        .current_dir(&root)
        .env("HOME", &root)
        .env("CARGO_HOME", root.join(".cargo"))
        .env("PATH", std::env::join_paths([&manager, &farm]).unwrap())
        .env("KACHE_TEST_FARM", &farm)
        .env_remove("KACHE_REAL_CARGO")
        .env_remove("KACHE_REAL_RUSTDOC")
        .env_remove("KACHE_TEST_REAL_TOOLS")
        .env_remove("KACHE_BLAZE_REAL_CARGO")
        .args(["--json", "doctor"]);
    let output = assert_cmd::Command::from_std(doctor)
        .timeout(Duration::from_secs(30))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    for label in ["Cargo shim chain", "rustdoc shim chain"] {
        let check = report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["label"] == label)
            .unwrap();
        assert_eq!(check["pass"], false, "{check}");
        assert!(
            check["detail"].as_str().unwrap().contains("forwards back"),
            "{check}"
        );
    }
    // An activated version manager is also a valid route, without overrides.
    command()
        .env("KACHE_TEST_REAL_TOOLS", &real)
        .timeout(Duration::from_secs(15))
        .assert()
        .success();
    command()
        .env("KACHE_REAL_CARGO", real.join("cargo"))
        .env("KACHE_REAL_RUSTDOC", real.join("rustdoc"))
        .timeout(Duration::from_secs(15))
        .assert()
        .success();
    let mut rustdoc = common::hermetic_command(
        farm.join("rustdoc"),
        &root.join("cache"),
        Some(&root.join("config.toml")),
    );
    rustdoc
        .current_dir(&root)
        .env("PATH", &manager)
        .env("KACHE_REAL_RUSTDOC", real.join("rustdoc"))
        .arg("--version");
    let output = assert_cmd::Command::from_std(rustdoc)
        .timeout(Duration::from_secs(15))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("rustdoc 1.99.0"));
}

#[test]
fn clang_through_mise_uses_the_active_tool_or_the_system_fallback() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path();
    for dir in ["farm", "manager", "system", "pinned"] {
        std::fs::create_dir(base.join(dir)).unwrap();
    }
    let kache = env!("CARGO_BIN_EXE_kache");
    let cache = base.join("cache");
    let _daemon = common::DaemonGuard::new(kache, &cache);
    std::fs::write(base.join("farm/.kache-shims"), "").unwrap();
    std::os::unix::fs::symlink(kache, base.join("farm/clang")).unwrap();
    std::os::unix::fs::symlink("../mise", base.join("manager/clang")).unwrap();
    executable(
        &base.join("mise"),
        r#"#!/bin/sh
if [ -f selected ]; then exec ./pinned/clang "$@"; fi
IFS=:
for dir in $PATH; do
    if [ "$dir" != "$SHIM_DIR" ] && [ -x "$dir/clang" ]; then
        exec "$dir/clang" "$@"
    fi
done
exit 127
"#,
    );
    for dir in ["system", "pinned"] {
        executable(
            &base.join(dir).join("clang"),
            &format!(
                "#!/bin/sh\nif [ \"$1\" = -c ]; then echo {dir} > \"$4\"; else echo {dir} \"$@\"; fi\n"
            ),
        );
    }
    std::fs::write(base.join("unit.c"), "int unit;").unwrap();
    let path =
        std::env::join_paths(["farm", "manager", "system"].map(|dir| base.join(dir))).unwrap();
    for expected in ["system", "pinned"] {
        if expected == "pinned" {
            std::fs::write(base.join("selected"), "").unwrap();
        }
        for args in [&["--version"][..], &["-c", "unit.c", "-o", "unit.o"][..]] {
            let output = common::hermetic_command(base.join("farm/clang"), &cache, None)
                .args(args)
                .current_dir(base)
                .env("PATH", &path)
                .env("SHIM_DIR", base.join("manager"))
                .env_remove("CARGO_PKG_NAME")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            if args == ["--version"] {
                assert_eq!(
                    String::from_utf8(output.stdout).unwrap().trim(),
                    format!("{expected} --version")
                );
            } else {
                assert_eq!(
                    std::fs::read_to_string(base.join("unit.o")).unwrap().trim(),
                    expected
                );
            }
        }
    }
    let log = std::fs::read_to_string(cache.join("events.jsonl")).unwrap();
    let compiles: Vec<serde_json::Value> = log
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| event["crate_name"] == "unit.c")
        .collect();
    assert_eq!(compiles.len(), 2);
    for event in compiles {
        assert_eq!(event["result"], "passthrough");
        assert_eq!(
            event["passthrough_reason"],
            "unsupported|mise selects the compiler at runtime"
        );
    }
}
