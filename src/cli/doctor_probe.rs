use super::Check;
use crate::invocation::process::CommandExt;
use anyhow::{Context, Result, bail, ensure};
use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub(super) fn check() -> Check {
    let result = probe();
    let pass = result.is_ok();
    Check {
        label: "Cache probe",
        pass,
        detail: match result {
            Ok(()) => "Rust compile, store and restore produced a local hit with identical bytes (isolated temporary cache; no remote access)".into(),
            Err(error) => format!("{error:#}"),
        },
        fix: (!pass).then(|| "check the Rust toolchain on PATH and the failure above; this probe uses an isolated configuration, so inspect the other checks for problems with the configured store".into()),
    }
}

fn probe() -> Result<()> {
    let fixture = tempfile::Builder::new().prefix("kache-doctor-").tempdir()?;
    let root = fixture.path();
    let cache = root.join("cache");
    let output = root.join("target/debug/deps");
    fs::create_dir_all(&output)?;
    let config = root.join("config.toml");
    fs::write(
        &config,
        toml::to_string(&serde_json::json!({"cache": {
            "ignore_env": true,
            "local_only": true,
            "local_store": cache.to_str().context("temporary cache path is not UTF-8")?,
            "runtime_dir": cache.to_str().context("temporary runtime path is not UTF-8")?,
            "min_store_compile_ms": 0,
            "auto_recover_min_free_bytes": 0,
        }}))?,
    )?;
    let source = root.join("lib.rs");
    fs::write(&source, "pub fn answer() -> u32 { 42 }\n")?;
    let exe = crate::platform::frontend_executable().context("locating the running frontend")?;
    let mut command = crate::platform::self_command(&exe, "rustc");
    command
        .args([
            "--crate-name",
            "kache_doctor_probe",
            "--crate-type",
            "rlib",
            "--edition=2021",
            "--emit=link,dep-info",
            "--out-dir",
        ])
        .arg(&output)
        .arg(&source)
        .current_dir(root)
        .env("KACHE_CONFIG", &config)
        .env("KACHE_HOST_CONFIG", "")
        .env("KACHE_LOG", "off")
        .env("KACHE_LOG_FILE_PATH", root.join("probe.log"))
        .env("TMPDIR", root)
        .env("TMP", root)
        .env("TEMP", root)
        .env_remove("KACHE_LOG_FILE")
        .env_remove("KACHE_ACTIVE")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("CARGO_BUILD_RUSTC_WRAPPER")
        .env_remove("OUT_DIR")
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env("CARGO_PRIMARY_PACKAGE", "1")
        .env("CARGO_PKG_NAME", "kache_doctor_probe")
        .env("CARGO_MANIFEST_DIR", root);
    let log = root.join("compiler.log");
    run(&mut command, &log, Duration::from_secs(30)).context("first Rust compilation")?;
    let events = crate::events::read_events(&cache.join("events.jsonl"))?;
    let first = events
        .last()
        .context("first compilation recorded no cache event")?;
    ensure!(
        matches!(
            first.result,
            crate::events::EventResult::Miss | crate::events::EventResult::Dup
        ),
        "first compilation was not stored: {}",
        crate::report::uncached_compile_words(first)
    );
    ensure!(
        first.store_error.is_empty(),
        "first compilation could not be stored: {}",
        first.store_error
    );
    let expected = fs::read(output.join("libkache_doctor_probe.rlib"))?;
    fs::remove_dir_all(&output)?;
    fs::create_dir_all(&output)?;
    run(&mut command, &log, Duration::from_secs(30)).context("second Rust compilation")?;
    let events = crate::events::read_events(&cache.join("events.jsonl"))?;
    let second = events
        .last()
        .context("second compilation recorded no cache event")?;
    ensure!(
        second.result == crate::events::EventResult::LocalHit,
        "second compilation was {}, expected a local hit",
        second.result
    );
    ensure!(
        second.compiler_runs == 0,
        "cache hit unexpectedly ran the compiler"
    );
    ensure!(
        first.cache_key == second.cache_key,
        "the two identical compilations produced different cache keys"
    );
    ensure!(
        expected == fs::read(output.join("libkache_doctor_probe.rlib"))?,
        "restored Rust library differs from the first compilation"
    );
    Ok(())
}

/// File-backed output avoids blocked pipes; the managed child owns its process
/// tree so a deadline also stops compiler descendants.
fn run(command: &mut Command, log: &Path, timeout: Duration) -> Result<()> {
    let file = fs::File::create(log)?;
    command
        .stdin(Stdio::null())
        .stdout(file.try_clone()?)
        .stderr(file);
    let mut child = command
        .invocation_spawn_in_group()
        .context("starting probe compiler")?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            if status.success() {
                return Ok(());
            }
            let mut output = Vec::new();
            fs::File::open(log)?.take(8192).read_to_end(&mut output)?;
            bail!(
                "compiler exited with {status}: {}",
                String::from_utf8_lossy(&output).trim()
            );
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            bail!(
                "probe compiler did not finish within {} ms",
                timeout.as_millis()
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn failed_and_stalled_compilers_return_details_and_finish() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("compiler.log");
        let mut failed = Command::new("/bin/sh");
        failed.args(["-c", "echo probe-failure >&2; exit 7"]);
        let error = run(&mut failed, &log, Duration::from_secs(2)).unwrap_err();
        assert!(error.to_string().contains("probe-failure"));
        assert!(error.to_string().contains('7'));
        let mut stalled = Command::new("/bin/sh");
        let survivor = dir.path().join("surviving-descendant");
        stalled
            .args([
                "-c",
                "(sleep 0.2; printf survived > \"$1\") & wait",
                "doctor-probe",
            ])
            .arg(&survivor);
        let start = Instant::now();
        let error = run(&mut stalled, &log, Duration::from_millis(30)).unwrap_err();
        assert!(error.to_string().contains("did not finish"));
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            !survivor.exists(),
            "the timeout must terminate compiler descendants too"
        );
    }
}
