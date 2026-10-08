//! Identify the compiler selected for an explicit warm-set replay.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use tokio::io::AsyncReadExt;

const PROBE_BUDGET: Duration = Duration::from_secs(3);
const OUTPUT_CAP: usize = 1 << 16;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Toolchain {
    pub hash: String,
    pub host: String,
}

pub(crate) fn probe(rustc: &Path) -> Result<Toolchain> {
    probe_with_limits(rustc, PROBE_BUDGET, OUTPUT_CAP)
}

fn probe_with_limits(rustc: &Path, budget: Duration, cap: usize) -> Result<Toolchain> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("creating rustc probe runtime")?;
    runtime.block_on(async {
        let mut command = std::process::Command::new(rustc);
        command
            .arg("-vV")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        crate::platform::configure_process_group(&mut command);
        let mut command = tokio::process::Command::from(command);
        command.kill_on_drop(true);
        let mut child = command.spawn().context("starting rustc -vV")?;
        let pid = child.id().context("rustc probe has no process id")?;
        let stdout = child.stdout.take().context("opening rustc probe stdout")?;
        let result = tokio::time::timeout(budget, async {
            tokio::try_join!(read_bounded(stdout, cap), async {
                child.wait().await.context("waiting for rustc probe")
            })
        })
        .await
        .context("rustc -vV exceeded its time budget")
        .and_then(|result| result);
        match result {
            Ok((stdout, status)) => {
                ensure!(
                    status.success(),
                    "rustc -vV exited unsuccessfully: {status}"
                );
                parse_version(&stdout)
            }
            Err(error) => {
                crate::platform::kill_process_group(pid);
                // The group kill reaches descendants holding stdout open;
                // kill/wait also reap the direct child on every platform.
                let _ = child.kill().await;
                let _ = child.wait().await;
                Err(error)
            }
        }
    })
}

async fn read_bounded(mut stdout: tokio::process::ChildStdout, cap: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = stdout
            .read(&mut chunk)
            .await
            .context("reading rustc probe stdout")?;
        if count == 0 {
            return Ok(output);
        }
        ensure!(
            output.len().saturating_add(count) <= cap,
            "rustc -vV output exceeds its byte budget"
        );
        output.extend_from_slice(&chunk[..count]);
    }
}

fn parse_version(stdout: &[u8]) -> Result<Toolchain> {
    let version = std::str::from_utf8(stdout)
        .context("rustc -vV output is not UTF-8")?
        .trim();
    ensure!(
        !version.contains('\u{fffd}'),
        "rustc -vV output contains a replacement character"
    );
    ensure!(
        version
            .lines()
            .next()
            .and_then(|line| line.strip_prefix("rustc "))
            .is_some_and(|value| !value.trim().is_empty()),
        "rustc -vV did not report a compiler version"
    );
    let host = crate::cache_key::rustc_host_triple(version)
        .context("rustc -vV did not report a host target")?;
    Ok(Toolchain {
        // The wrapper's cached compiler-version helper trims the same text.
        hash: blake3::hash(version.as_bytes()).to_hex().to_string(),
        host: host.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const VERSION: &str = "rustc 1.99.0 (abc 2026-10-01)\nbinary: rustc\nhost: x86_64-unknown-linux-gnu\nrelease: 1.99.0\n";

    #[test]
    fn public_probe_limits_are_three_seconds_and_sixty_four_kibibytes() {
        assert_eq!(PROBE_BUDGET, Duration::from_millis(3000));
        assert_eq!(OUTPUT_CAP, 65_536);
    }

    #[test]
    fn version_hash_matches_the_wrappers_trimmed_full_text() {
        let parsed = parse_version(format!("\n {VERSION}\n").as_bytes()).unwrap();
        assert_eq!(parsed.host, "x86_64-unknown-linux-gnu");
        assert_eq!(
            parsed.hash,
            blake3::hash(VERSION.trim().as_bytes()).to_hex().to_string()
        );
        let changed = VERSION.replace("abc", "different-compiler");
        assert_ne!(parsed.hash, parse_version(changed.as_bytes()).unwrap().hash);
    }

    #[test]
    fn unknown_or_invalid_compiler_context_is_rejected() {
        for value in [
            "",
            "host: x86_64-unknown-linux-gnu\n",
            "rustc \nhost: x86_64-unknown-linux-gnu\n",
            "other 1.99\nhost: x86_64-unknown-linux-gnu\n",
            "rustc 1.99\n",
            "rustc 1.99\nhost: \n",
            "rustc 1.99\nhost: x\n\u{fffd}",
        ] {
            assert!(parse_version(value.as_bytes()).is_err(), "{value:?}");
        }
        assert!(parse_version(b"rustc 1.99\nhost: x\n\xff").is_err());
    }

    #[cfg(unix)]
    fn fake_rustc(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compiler with spaces");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        (dir, path)
    }

    #[cfg(unix)]
    #[test]
    fn a_successful_probe_preserves_the_full_identity_and_argument() {
        let (_dir, rustc) = fake_rustc(&format!(
            "[ \"$#\" = 1 ] && [ \"$1\" = -vV ] || exit 4\nprintf '%s' '{VERSION}'"
        ));
        assert_eq!(
            probe(&rustc).unwrap(),
            parse_version(VERSION.as_bytes()).unwrap()
        );
        let (_dir, rustc) = fake_rustc("printf 'rustc 1.99\\nhost: x\\n'; exit 7");
        assert!(
            probe(&rustc)
                .unwrap_err()
                .to_string()
                .contains("unsuccessfully")
        );
        let (_dir, rustc) = fake_rustc("printf 'rustc 1.99\\n'");
        assert!(
            probe(&rustc)
                .unwrap_err()
                .to_string()
                .contains("host target")
        );
    }

    #[test]
    fn a_missing_compiler_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(probe(&dir.path().join("missing-rustc")).is_err());
    }

    #[cfg(unix)]
    fn assert_reaped(rustc: &Path) {
        let pid: libc::pid_t = std::fs::read_to_string(rustc.with_extension("pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let mut status = 0;
        assert_eq!(
            unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ECHILD)
        );
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[cfg(unix)]
    #[test]
    fn timeout_kills_and_reaps_the_probe() {
        let (_dir, rustc) = fake_rustc("echo $$ > \"$0.pid\"\nexec sleep 30");
        let started = std::time::Instant::now();
        let error = probe_with_limits(&rustc, Duration::from_millis(400), 64 << 10).unwrap_err();
        assert!(error.to_string().contains("time budget"));
        assert!(started.elapsed() < Duration::from_secs(2));
        // The fixture appends .pid; the executable has no extension.
        assert_reaped(&rustc);
    }

    #[cfg(unix)]
    #[test]
    fn an_unbounded_writer_is_killed_and_reaped_before_the_deadline() {
        let (_dir, rustc) = fake_rustc(
            "echo $$ > \"$0.pid\"\nwhile :; do printf 'abcdefghijklmnopqrstuvwxyz'; done",
        );
        let started = std::time::Instant::now();
        let error = probe_with_limits(&rustc, Duration::from_secs(5), 1024).unwrap_err();
        assert!(error.to_string().contains("byte budget"));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_reaped(&rustc);
    }

    #[cfg(unix)]
    #[test]
    fn the_output_cap_includes_its_boundary() {
        let (_dir, rustc) = fake_rustc(&format!("printf '%s' '{VERSION}'"));
        let parsed = probe_with_limits(&rustc, PROBE_BUDGET, VERSION.len()).unwrap();
        assert_eq!(parsed, parse_version(VERSION.as_bytes()).unwrap());
        let error = probe_with_limits(&rustc, PROBE_BUDGET, VERSION.len() - 1).unwrap_err();
        assert!(error.to_string().contains("byte budget"));
    }
}
