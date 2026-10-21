use anyhow::{Context, Result};
use std::ffi::OsString;
use std::process::Command;

#[cfg(windows)]
#[path = "frontend_windows.rs"]
mod windows;

/// Run the thin compiler client, or replace it with the companion CLI engine.
pub fn run(version: &str) -> Result<i32> {
    let started_at = std::time::Instant::now();
    let args = std::env::args_os().collect::<Vec<_>>();
    anyhow::ensure!(
        args.get(1)
            .is_none_or(|arg| arg != crate::FORWARDED_ARGV_FLAG),
        "kache-engine points to the client; reinstall both Kache binaries together"
    );
    let executable = std::env::current_exe().context("locating the Kache client")?;
    let executable = if executable.is_absolute() {
        executable
    } else {
        std::env::current_dir()?.join(executable)
    };
    if engine_only(&args)
        || std::env::var_os("KACHE_FAMILY_PROBE_ACTIVE").is_some()
        || std::env::var_os("KACHE_BUILD_SCRIPT_PATH").is_some()
    {
        return forward_engine(&executable, version, &args, started_at);
    }
    #[cfg(any(unix, windows))]
    {
        let env = kache_config::InvocationEnv::capture();
        if env
            .var_os("KACHE_ACTIVE")
            .is_none_or(|value| value.is_empty())
        {
            let prepared = (|| -> Result<_> {
                let config = kache_config::Config::load_for(&env)?;
                #[cfg(unix)]
                let (files, terminal) = crate::Stdio::capture()?.into_files();
                #[cfg(windows)]
                let (files, terminal) = (Vec::new(), false);
                Ok((config, files, terminal))
            })();
            if let Ok((config, files, terminal)) = prepared {
                let execution = crate::Execution {
                    entry: kache_protocol::execution::ExecutionEntry::Original,
                    args: &args,
                    env: &env,
                    started_at,
                    executable: &executable,
                    files: &files,
                    terminal,
                    client_version: version,
                };
                let socket = config.socket_path();
                if let Some(exit) = execution.try_execute(&socket)? {
                    return Ok(exit);
                }
                #[cfg(windows)]
                if let Some(exit) = windows::try_bootstrap(&execution, &socket, config.disabled)? {
                    return Ok(exit);
                }
            }
        }
    }
    forward_engine(&executable, version, &args, started_at)
}

fn forward_engine(
    executable: &std::path::Path,
    version: &str,
    args: &[OsString],
    started_at: std::time::Instant,
) -> Result<i32> {
    let mut command = engine_command(executable, version, args, started_at)?;
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        Err(command.exec()).context("starting the Kache engine")
    }
    #[cfg(not(unix))]
    {
        let status = command.status().context("starting the Kache engine")?;
        Ok(status.code().unwrap_or(1))
    }
}

fn engine_command(
    executable: &std::path::Path,
    version: &str,
    args: &[OsString],
    started_at: std::time::Instant,
) -> Result<Command> {
    let engine = kache_shims::engine_for(executable)
        .context("locating kache-engine; reinstall both Kache binaries together")?;
    let mut command = Command::new(engine);
    command
        .arg(crate::FORWARDED_ARGV_FLAG)
        .arg(version)
        .arg(
            started_at
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX))
                .to_string(),
        )
        .arg(executable)
        .arg(
            kache_shims::running_identity()
                .map(|identity| format!("{}:{}", identity.dev, identity.ino))
                .unwrap_or_else(|| "-".to_owned()),
        )
        .args(args);
    Ok(command)
}

fn engine_only(args: &[OsString]) -> bool {
    let arg0 = args
        .first()
        .map(|arg| arg.to_string_lossy())
        .unwrap_or_default();
    if kache_shims::is_compiler_shim_name(&arg0) {
        return false;
    }
    let name = std::path::Path::new(arg0.as_ref())
        .file_stem()
        .and_then(|name| name.to_str());
    if name == Some("cargo") {
        return true;
    }
    let Some(first) = args.get(1) else {
        return true;
    };
    let Some(first) = first.to_str() else {
        return false;
    };
    crate::engine_command_names().contains(&first)
        || (first.starts_with('-') && !(first == "-E" && args.len() >= 3))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_flags_and_cargo_go_to_the_engine_before_configuration() {
        for args in [
            vec!["kache", "init"],
            vec!["kache", "help"],
            vec!["kache", "-E"],
            vec!["kache", "status"],
            vec!["renamed-kache", "--help"],
            vec!["cargo", "build"],
            vec!["kache"],
        ] {
            assert!(engine_only(
                &args.into_iter().map(OsString::from).collect::<Vec<_>>()
            ));
        }
        for args in [
            vec!["cc", "init"],
            vec!["x86_64-linux-gnu-gcc-13", "--help"],
            vec!["kache", "-E", "probe.c"],
            vec!["kache", "rustc", "lib.rs"],
        ] {
            assert!(!engine_only(
                &args.into_iter().map(OsString::from).collect::<Vec<_>>()
            ));
        }
    }
}

#[cfg(all(test, unix))]
#[path = "frontend/tests.rs"]
mod boundary_tests;
