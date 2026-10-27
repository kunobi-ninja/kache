//! Optional Cargo-level routing. Compiler invocations never make this decision.

use crate::config::Config;
use crate::invocation::InvocationEnv;
use anyhow::{Context, Result};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::Instant;

const ROUTED: &str = "KACHE_BLAZE_ROUTED";
const RETURN_CARGO: &str = "KACHE_BLAZE_REAL_CARGO";
const RETURN_SHIM: &str = "KACHE_BLAZE_CARGO_SHIM";

pub(crate) fn eligible(args: &[OsString], env: &InvocationEnv) -> bool {
    if env.var_os(ROUTED).is_some()
        || kache_config::policy::running_in_ci_with(|name| env.var(name).ok())
    {
        return false;
    }
    let Some(command) = args.first().and_then(|arg| arg.to_str()) else {
        return false;
    };
    if !matches!(
        command,
        "build" | "b" | "run" | "r" | "test" | "t" | "check" | "c"
    ) {
        return false;
    }
    !args[1..]
        .iter()
        .take_while(|arg| *arg != "--")
        .any(|arg| explicit_profile(arg))
}

fn explicit_profile(arg: &OsStr) -> bool {
    let Some(arg) = arg.to_str() else {
        return false;
    };
    if arg == "--release" || arg == "--profile" || arg.starts_with("--profile=") {
        return true;
    }
    if let Some(flags) = arg.strip_prefix('-') {
        for flag in flags.chars() {
            match flag {
                'r' => return true,
                'q' | 'v' => {}
                // Stop before values such as the crate name in -pcrate.
                _ => break,
            }
        }
    }
    false
}

pub(crate) fn installed(env: &InvocationEnv) -> Option<PathBuf> {
    if !(cfg!(target_os = "linux") || cfg!(all(target_os = "macos", target_arch = "aarch64"))) {
        return None;
    }
    // The engine name is stable across the ra-jit -> Blaze rename. Look it
    // up for each eligible command so installing it later needs no new init.
    env.resolve_program(Path::new("ra-jit"))
}

pub(crate) fn returning_cargo(env: &InvocationEnv) -> Option<OsString> {
    env.var_os(ROUTED)?;
    let shim = env.var_os(RETURN_SHIM)?;
    if env.var_os("CARGO").as_deref() != Some(shim.as_os_str()) {
        return None;
    }
    env.var_os(RETURN_CARGO)
}

/// A started Blaze owns its exit status, including build errors and signals.
/// Only a failure to start falls back to the ordinary Cargo path.
pub(crate) fn try_run(
    config: &Config,
    env: &InvocationEnv,
    shim: &OsStr,
    cargo: &Path,
    args: &[OsString],
) -> Result<Option<ExitStatus>> {
    if !config.blaze || config.disabled || !eligible(args, env) {
        return Ok(None);
    }
    let Some(engine) = installed(env) else {
        eprintln!(
            "kache: Blaze is enabled but ra-jit is unavailable on this platform or PATH; using Cargo"
        );
        return Ok(None);
    };
    let shim = env
        .resolve_program(Path::new(shim))
        .context("locating the Cargo shim for Blaze")?;
    let mut command = env.command(&engine)?;
    command
        .arg("cargo")
        .args(args)
        .env("CARGO", &shim)
        .env(RETURN_CARGO, cargo)
        .env(RETURN_SHIM, &shim)
        .env(ROUTED, "1");
    let started = Instant::now();
    let mut child = match crate::platform::host_spawn(&mut command) {
        Ok(child) => child,
        Err(error) => {
            eprintln!("kache: could not start Blaze ({error}); using Cargo");
            return Ok(None);
        }
    };
    crate::test_runner::signals::forward_to(child.id());
    let status = child.wait().context("waiting for Blaze")?;
    if let Some(root) = event_root(env, args) {
        let root = root.to_string_lossy();
        crate::wrapper::log_event(
            config,
            crate::wrapper::EventInputs::new(
                &root,
                "blaze",
                crate::events::EventResult::Passthrough,
                started.elapsed().as_millis() as u64,
            )
            .passthrough_reason(
                "blaze-command|Cargo command handled by Blaze; uses Blaze's own cache".into(),
            )
            .exit_code(status.code().unwrap_or(1)),
        );
    }
    Ok(Some(status))
}

fn event_root(env: &InvocationEnv, args: &[OsString]) -> Option<PathBuf> {
    if let Some(root) = env.var_os("KACHE_EVENT_ROOT") {
        return env.resolve(Path::new(&root));
    }
    let mut location = env.cwd()?.to_path_buf();
    let mut args = args.iter().take_while(|arg| *arg != "--");
    while let Some(arg) = args.next() {
        let manifest = if arg == "--manifest-path" {
            args.next().map(PathBuf::from)
        } else {
            arg.to_str()
                .and_then(|arg| arg.strip_prefix("--manifest-path="))
                .map(PathBuf::from)
        };
        if let Some(manifest) = manifest {
            location = env.resolve(&manifest)?.parent()?.to_path_buf();
        }
    }
    crate::cli::cargo_workspace_root(&location)
}

pub(crate) fn workspace_wrapper(program: &str) -> bool {
    matches!(
        crate::compiler::command_basename(program),
        Some("ra-jit-rustc" | "ra-jit-rustc-first")
    )
}

/// A wrapper may satisfy Cargo from existing files, so neither pre-clean its
/// outputs nor rewrite its incremental flags as though Kache owned the compile.
pub(crate) fn run_workspace_wrapper(
    config: &Config,
    args: &[String],
    parsed: &crate::args::RustcArgs,
    env: &InvocationEnv,
) -> Result<i32> {
    let started = Instant::now();
    let status = crate::invocation::io::status(env.command(&args[0])?.args(&args[1..]))?;
    let exit = status.code().unwrap_or(1);
    if parsed.is_primary
        && let Some(crate_name) = parsed.crate_name.as_deref()
    {
        let root = crate::wrapper::rustc_event_root(parsed);
        crate::wrapper::log_event(
            config,
            crate::wrapper::EventInputs::new(
                &root,
                crate_name,
                crate::events::EventResult::Passthrough,
                started.elapsed().as_millis() as u64,
            )
            .passthrough_reason(
                "blaze|workspace compilation handled by Blaze; not cached by Kache".into(),
            )
            .exit_code(exit),
        );
    }
    Ok(exit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(root: &Path, vars: &[(&str, &str)]) -> InvocationEnv {
        InvocationEnv::from_parts(vars.iter().copied(), Some(root.into()), None, root.into())
    }

    #[test]
    fn command_selection_keeps_profiles_toolchains_and_program_arguments_separate() {
        let root = tempfile::tempdir().unwrap();
        let env = snapshot(root.path(), &[]);
        let args = |line: &str| {
            line.split_whitespace()
                .map(OsString::from)
                .collect::<Vec<_>>()
        };
        for line in [
            "build",
            "b",
            "run",
            "r",
            "test",
            "t",
            "check",
            "c",
            "run -- --release",
            "test -- --profile=ci",
            "build -pcrate -Ffeature",
        ] {
            assert!(eligible(&args(line), &env), "{line}");
        }
        for line in [
            "",
            "build --release",
            "build -r",
            "build -rq",
            "build -vqr",
            "run --profile release",
            "check --profile=dev",
            "+nightly build",
            "--locked build",
            "clippy",
            "bench",
            "install",
        ] {
            assert!(!eligible(&args(line), &env), "{line}");
        }
        for var in ["CI", "GITHUB_ACTIONS", "GITLAB_CI", ROUTED] {
            let env = snapshot(root.path(), &[(var, "1")]);
            assert!(!eligible(&args("build"), &env), "{var}");
        }
        assert!(eligible(
            &args("build"),
            &snapshot(root.path(), &[("CI", "false")])
        ));
    }

    #[test]
    fn returning_calls_keep_the_selected_cargo_only_while_cargo_names_our_shim() {
        let root = tempfile::tempdir().unwrap();
        let env = snapshot(
            root.path(),
            &[
                (ROUTED, "1"),
                ("CARGO", "/shim/cargo"),
                (RETURN_SHIM, "/shim/cargo"),
                (RETURN_CARGO, "/tools/cargo"),
            ],
        );
        assert_eq!(returning_cargo(&env), Some("/tools/cargo".into()));
        let mut other = env.clone();
        other.set("CARGO", "/other/cargo");
        assert_eq!(returning_cargo(&other), None);
        assert_eq!(returning_cargo(&snapshot(root.path(), &[])), None);
        assert_eq!(
            returning_cargo(&snapshot(
                root.path(),
                &[(ROUTED, "1"), (RETURN_CARGO, "/tools/cargo")]
            )),
            None
        );
    }

    #[test]
    fn only_known_blaze_wrappers_keep_their_outputs() {
        for program in ["/bin/ra-jit-rustc", "/bin/ra-jit-rustc-first"] {
            assert!(workspace_wrapper(program));
        }
        for program in ["rustc", "ra-jit", "clippy-driver", "other-wrapper"] {
            assert!(!workspace_wrapper(program));
        }
    }

    #[cfg(unix)]
    #[test]
    fn installation_is_checked_again_and_nonexecutables_are_ignored() {
        use std::os::unix::fs::PermissionsExt;
        if !(cfg!(target_os = "linux") || cfg!(target_arch = "aarch64")) {
            return;
        }
        let root = tempfile::tempdir().unwrap();
        let env = snapshot(root.path(), &[("PATH", root.path().to_str().unwrap())]);
        assert_eq!(installed(&env), None);
        let engine = root.path().join("ra-jit");
        std::fs::write(&engine, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(installed(&env), None);
        std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(installed(&env), Some(engine));
    }
}
