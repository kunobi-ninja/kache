//! A rustc compile that loads its codegen backend from a dylib runs directly:
//! no cache and no configured fallback, unless the backend is trusted.
#![cfg(unix)]
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;

fn script(path: &Path, body: &str) {
    fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn untrusted_codegen_backend_runs_rustc_directly_every_time() {
    let dir = tempfile::tempdir().unwrap();
    let rustc = dir.path().join("rustc");
    let fallback = dir.path().join("fallback");
    let calls = dir.path().join("calls");
    // The fake backend writes a device artifact beside the source, which is
    // what a cache hit would fail to restore.
    script(
        &rustc,
        r#"
for arg in "$@"; do
  if [ "$arg" = "-vV" ]; then
    printf 'rustc 1.98.0\nbinary: rustc\ncommit-hash: fake\ncommit-date: 2026-09-01\nhost: x86_64-unknown-linux-gnu\nrelease: 1.98.0\nLLVM version: 22.1.0\n'
    exit 0
  fi
done
printf 'direct\n' >> "$TEST_CALLS"
printf 'device' > kernel.ptx
printf 'binary' > app
"#,
    );
    script(&fallback, r#"printf 'fallback\n' >> "$TEST_CALLS"; exit 0"#);
    fs::write(dir.path().join("main.rs"), "fn main() {}\n").unwrap();
    fs::write(dir.path().join("backend.so"), "backend").unwrap();

    for _ in 0..2 {
        fs::remove_file(dir.path().join("kernel.ptx")).ok();
        let output = Command::new(env!("CARGO_BIN_EXE_kache"))
            .arg(&rustc)
            .args([
                "--crate-name",
                "app",
                "main.rs",
                "--crate-type",
                "bin",
                "--emit=link",
                "-o",
                "app",
            ])
            .arg(format!(
                "-Zcodegen-backend={}",
                dir.path().join("backend.so").display()
            ))
            .current_dir(dir.path())
            .env("KACHE_CONFIG", dir.path().join("absent.toml"))
            .env("KACHE_HOST_CONFIG", "")
            .env("KACHE_CACHE_DIR", dir.path().join("cache"))
            .env("KACHE_FALLBACK", &fallback)
            .env("KACHE_CACHE_EXECUTABLES", "true")
            .env("KACHE_REMOTE", "")
            .env_remove("KACHE_TRUST_CODEGEN_BACKENDS")
            .env_remove("KACHE_DISABLED")
            .env("TEST_CALLS", &calls)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            dir.path().join("kernel.ptx").exists(),
            "the backend's side file must be produced on every build"
        );
    }

    assert_eq!(fs::read_to_string(&calls).unwrap(), "direct\ndirect\n");
    let events = fs::read_to_string(dir.path().join("cache/events.jsonl")).unwrap();
    let reasons: Vec<String> = events
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| event["result"] == "passthrough")
        .map(|event| event.to_string())
        .collect();
    assert_eq!(reasons.len(), 2, "{events}");
    assert!(
        reasons
            .iter()
            .all(|event| event.contains("cache.trust_codegen_backends")),
        "{events}"
    );
}
