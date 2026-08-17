//! Guarded Cargo front-end for canonical duplicate config files (#766).
//!
//! Cargo merges array-valued configuration every time a config path is
//! discovered.  An ancestor `.cargo` symlink to `$CARGO_HOME` therefore reads
//! one physical `build.rustflags` array twice.  `RUSTC_WRAPPER` runs too late to
//! repair Cargo's unit identities, so `kache cargo -- ...` removes only that
//! duplicate source before Cargo computes them.

use anyhow::{Context, Result, bail};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;

const ENCODED_SEPARATOR: char = '\x1f';

#[derive(Debug, Clone)]
struct ConfigSource {
    logical_path: PathBuf,
    canonical_path: PathBuf,
    content_hash: blake3::Hash,
    value: toml::Value,
    cargo_home: bool,
}

#[derive(Debug)]
struct NormalizationPlan {
    encoded_rustflags: String,
    snapshots: Vec<ConfigSource>,
    cwd: PathBuf,
    cargo_home: PathBuf,
    candidate_paths: Vec<PathBuf>,
    duplicate_paths: Vec<PathBuf>,
}

#[derive(Debug)]
enum PlanDecision {
    Apply(NormalizationPlan),
    Passthrough,
    Refused(String),
}

/// Run Cargo with canonical duplicate `$CARGO_HOME` rustflags collapsed once.
///
/// Every ambiguous case fails closed by launching Cargo unchanged. That keeps
/// this command a conservative convenience rather than a second, incomplete
/// Cargo config implementation.
pub(crate) fn run(cargo_args: Vec<OsString>) -> Result<()> {
    let cwd = std::env::current_dir().context("resolving the Cargo working directory")?;
    let cargo = real_cargo_program()?;
    let mut command = Command::new(&cargo);
    command.args(&cargo_args).current_dir(&cwd);

    match normalization_plan(&cwd, &cargo_args) {
        PlanDecision::Apply(plan) => {
            if !plan_is_current(&plan) {
                eprintln!(
                    "kache: Cargo config changed while it was being inspected; \
                     running Cargo unchanged"
                );
            } else {
                tracing::info!(
                    aliases = ?plan.duplicate_paths,
                    "collapsing canonical duplicate Cargo rustflags source"
                );
                command.env("CARGO_ENCODED_RUSTFLAGS", &plan.encoded_rustflags);
            }
        }
        PlanDecision::Refused(reason) => {
            eprintln!(
                "kache: safe Cargo config normalization is unavailable: \
                 {reason}; running Cargo unchanged"
            );
        }
        PlanDecision::Passthrough => {}
    }

    #[cfg(unix)]
    {
        exec_cargo_unix(command, &cargo)
    }
    #[cfg(not(unix))]
    {
        run_cargo_non_unix(command, &cargo)
    }
}

fn real_cargo_program() -> Result<PathBuf> {
    let program = std::env::var_os("KACHE_REAL_CARGO")
        .or_else(|| std::env::var_os("CARGO"))
        .unwrap_or_else(|| OsString::from("cargo"));
    let cwd = std::env::current_dir().context("resolving the Cargo working directory")?;
    resolve_cargo_program(&program, &cwd)
}

fn resolve_cargo_program(program: &OsStr, cwd: &Path) -> Result<PathBuf> {
    let path = Path::new(program);
    let executable = match program.to_str() {
        Some(program) => crate::compiler::resolve_program_on_path(program),
        None => Some(path.to_path_buf()),
    }
    .with_context(|| format!("resolving Cargo program {program:?}"))?;
    let executable = if executable.is_absolute() {
        executable
    } else {
        cwd.join(executable)
    };
    let resolved_identity = executable
        .canonicalize()
        .with_context(|| format!("resolving Cargo program {program:?}"))?;
    let current = std::env::current_exe()
        .ok()
        .and_then(|path| path.canonicalize().ok());
    if Some(&resolved_identity) == current.as_ref() {
        bail!(
            "resolved Cargo program {:?} points back to kache; set KACHE_REAL_CARGO to the real Cargo binary",
            program
        );
    }
    // Preserve the launcher path. In a rustup installation `cargo` is a symlink
    // to the shared rustup binary, whose behavior depends on argv[0]. Executing
    // its canonical target would launch `rustup`, not the Cargo proxy.
    Ok(executable)
}

#[cfg(unix)]
fn exec_cargo_unix(mut command: Command, cargo: &Path) -> Result<()> {
    use std::os::unix::process::CommandExt;

    let error = command.exec();
    Err(error).with_context(|| format!("executing Cargo program {cargo:?}"))
}

#[cfg(not(unix))]
fn run_cargo_non_unix(mut command: Command, cargo: &Path) -> Result<()> {
    let status = command
        .status()
        .with_context(|| format!("running Cargo program {cargo:?}"))?;
    std::process::exit(status.code().unwrap_or(1));
}

fn normalization_plan(cwd: &Path, cargo_args: &[OsString]) -> PlanDecision {
    if rustflags_environment_is_explicit() {
        return PlanDecision::Passthrough;
    }
    if supported_cargo_command(cargo_args).is_err() {
        return PlanDecision::Passthrough;
    }

    let cargo_home = match cargo_home(cwd) {
        Ok(cargo_home) => cargo_home,
        Err(reason) => return PlanDecision::Refused(reason),
    };
    let candidates = cargo_config_candidates(cwd, &cargo_home);
    let mut sources = Vec::with_capacity(candidates.len());
    let candidate_paths = candidates.iter().map(|(path, _)| path.clone()).collect();
    for (logical_path, cargo_home) in candidates {
        match read_source(logical_path, cargo_home) {
            Ok(source) => sources.push(source),
            Err(reason) => return PlanDecision::Refused(reason),
        }
    }

    let Some(home_source) = sources.iter().find(|source| source.cargo_home) else {
        return PlanDecision::Passthrough;
    };
    let home_canonical = home_source.canonical_path.clone();
    let duplicate_paths: Vec<PathBuf> = sources
        .iter()
        .filter(|source| is_cargo_home_alias(source, &home_canonical))
        .map(|source| source.logical_path.clone())
        .collect();
    if duplicate_paths.is_empty() {
        return PlanDecision::Passthrough;
    }

    let mut rustflags = Vec::new();
    for source in &sources {
        if is_cargo_home_alias(source, &home_canonical) {
            continue;
        }
        match source_rustflags(&source.value, &source.logical_path) {
            Ok(Some(flags)) => rustflags.extend(flags),
            Ok(None) => {}
            Err(reason) => return PlanDecision::Refused(reason),
        }
    }
    if rustflags.is_empty() {
        return PlanDecision::Refused(
            "the canonical duplicate has no array-valued build.rustflags".into(),
        );
    }
    let encoded_rustflags = rustflags.join("\x1f");
    PlanDecision::Apply(NormalizationPlan {
        encoded_rustflags,
        snapshots: sources,
        cwd: cwd.to_path_buf(),
        cargo_home,
        candidate_paths,
        duplicate_paths,
    })
}

fn is_cargo_home_alias(source: &ConfigSource, home_canonical: &Path) -> bool {
    !source.cargo_home && source.canonical_path == home_canonical
}

fn rustflags_environment_is_explicit() -> bool {
    std::env::vars_os().any(|(key, _)| {
        let Some(key) = key.to_str() else {
            return false;
        };
        rustflags_env_name(key, cfg!(windows))
    })
}

fn rustflags_env_name(key: &str, case_insensitive: bool) -> bool {
    let key = if case_insensitive {
        key.to_ascii_uppercase()
    } else {
        key.to_string()
    };
    key == "RUSTFLAGS"
        || key == "CARGO_ENCODED_RUSTFLAGS"
        || (key.starts_with("CARGO_") && key.ends_with("RUSTFLAGS"))
}

fn supported_cargo_command(args: &[OsString]) -> std::result::Result<(), String> {
    let Some((first, remaining)) = args.split_first() else {
        return Err("no Cargo build/check command was provided".into());
    };
    let (command, trailing) = if first
        .to_str()
        .is_some_and(|argument| argument.starts_with('+'))
    {
        remaining
            .split_first()
            .ok_or_else(|| "no Cargo build/check command was provided".to_string())?
    } else {
        (first, remaining)
    };
    let Some(command) = command.to_str() else {
        return Err("the Cargo command is not UTF-8".into());
    };
    if !matches!(command, "build" | "check") {
        return Err("only Cargo's built-in build/check commands are normalized".into());
    }
    if trailing
        .iter()
        .any(|arg| arg.to_str().is_none_or(cargo_arg_may_change_config))
    {
        return Err("Cargo -C/-Z/--config arguments are not normalized".into());
    }
    Ok(())
}

fn cargo_arg_may_change_config(arg: &str) -> bool {
    arg == "--config"
        || arg.starts_with("--config=")
        || arg.starts_with("-C")
        || arg.starts_with("-Z")
}

fn cargo_config_candidates(cwd: &Path, cargo_home: &Path) -> Vec<(PathBuf, bool)> {
    let mut candidates = Vec::new();
    if let Some(path) = selected_config(cargo_home) {
        candidates.push((path, true));
    }

    let mut ancestors: Vec<&Path> = cwd.ancestors().collect();
    ancestors.reverse();
    for ancestor in ancestors {
        let Some(path) = selected_config(&ancestor.join(".cargo")) else {
            continue;
        };
        if !candidates.iter().any(|(candidate, _)| candidate == &path) {
            candidates.push((path, false));
        }
    }
    candidates
}

fn cargo_home(cwd: &Path) -> std::result::Result<PathBuf, String> {
    home::cargo_home_with_cwd(cwd)
        .map_err(|error| format!("cannot resolve Cargo's home directory: {error}"))
}

/// Cargo retains the extensionless file for backwards compatibility when both
/// names exist (and emits its own warning).
fn selected_config(config_dir: &Path) -> Option<PathBuf> {
    let legacy = config_dir.join("config");
    if legacy.is_file() {
        return Some(legacy);
    }
    let modern = config_dir.join("config.toml");
    modern.is_file().then_some(modern)
}

fn read_source(path: PathBuf, cargo_home: bool) -> std::result::Result<ConfigSource, String> {
    let canonical_path = path
        .canonicalize()
        .map_err(|error| format!("cannot canonicalize {}: {error}", path.display()))?;
    let bytes =
        std::fs::read(&path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|error| format!("{} is not UTF-8: {error}", path.display()))?;
    let value = toml::from_str(text)
        .map_err(|error| format!("cannot parse {}: {error}", path.display()))?;
    Ok(ConfigSource {
        logical_path: path,
        canonical_path,
        content_hash: blake3::hash(&bytes),
        value,
        cargo_home,
    })
}

fn source_rustflags(
    value: &toml::Value,
    path: &Path,
) -> std::result::Result<Option<Vec<String>>, String> {
    let table = value
        .as_table()
        .ok_or_else(|| format!("{} is not a TOML table", path.display()))?;
    if table.contains_key("include") {
        return Err(format!("{} uses Cargo config include", path.display()));
    }
    if target_rustflags_present(table) {
        return Err(format!(
            "{} defines target-specific rustflags",
            path.display()
        ));
    }
    if config_env_rustflags_present(table) {
        return Err(format!(
            "{} defines rustflags through [env]",
            path.display()
        ));
    }

    let Some(build) = table.get("build") else {
        return Ok(None);
    };
    let build = build
        .as_table()
        .ok_or_else(|| format!("{}.build is not a table", path.display()))?;
    let Some(rustflags) = build.get("rustflags") else {
        return Ok(None);
    };
    let array = rustflags.as_array().ok_or_else(|| {
        format!(
            "{}.build.rustflags uses string form instead of an argument array",
            path.display()
        )
    })?;
    let mut flags = Vec::with_capacity(array.len());
    for flag in array {
        let Some(flag) = flag.as_str() else {
            return Err(format!(
                "{}.build.rustflags contains a non-string value",
                path.display()
            ));
        };
        if flag.is_empty() || flag.contains(ENCODED_SEPARATOR) {
            return Err(format!(
                "{}.build.rustflags contains an empty argument or Cargo's encoded-argument separator",
                path.display()
            ));
        }
        flags.push(flag.to_string());
    }
    Ok(Some(flags))
}

fn target_rustflags_present(table: &toml::map::Map<String, toml::Value>) -> bool {
    table
        .get("target")
        .and_then(toml::Value::as_table)
        .is_some_and(|targets| {
            targets.values().any(|target| {
                target
                    .as_table()
                    .is_some_and(|target| target.contains_key("rustflags"))
            })
        })
}

fn config_env_rustflags_present(table: &toml::map::Map<String, toml::Value>) -> bool {
    table
        .get("env")
        .and_then(toml::Value::as_table)
        .is_some_and(|env| env.keys().any(|key| rustflags_env_name(key, cfg!(windows))))
}

fn plan_is_current(plan: &NormalizationPlan) -> bool {
    let rescanned: Vec<PathBuf> = cargo_config_candidates(&plan.cwd, &plan.cargo_home)
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    if rescanned != plan.candidate_paths {
        return false;
    }
    revalidate_sources(&plan.snapshots).is_ok()
}

fn revalidate_sources(sources: &[ConfigSource]) -> std::result::Result<(), String> {
    for source in sources {
        let canonical = source.logical_path.canonicalize().map_err(|error| {
            format!(
                "cannot re-canonicalize {}: {error}",
                source.logical_path.display()
            )
        })?;
        let bytes = std::fs::read(&source.logical_path).map_err(|error| {
            format!("cannot re-read {}: {error}", source.logical_path.display())
        })?;
        if canonical != source.canonical_path || blake3::hash(&bytes) != source.content_hash {
            return Err(format!("{} changed", source.logical_path.display()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_gate_accepts_only_unambiguous_builtin_build_and_check() {
        for args in [
            vec!["build"],
            vec!["build", "--workspace"],
            vec!["+nightly", "check", "--locked"],
            vec!["check", "--color=always"],
        ] {
            let args: Vec<OsString> = args.into_iter().map(OsString::from).collect();
            assert!(supported_cargo_command(&args).is_ok(), "args: {args:?}");
        }

        for args in [
            vec!["install", "demo"],
            vec!["xtask", "check"],
            vec!["b"],
            vec!["--locked", "check"],
            vec!["-C", "elsewhere", "check"],
            vec!["check", "--config", "build.rustflags=[]"],
            vec!["check", "--config=build.rustflags=[]"],
            vec!["check", "-Celsewhere"],
            vec!["-Zunstable-options", "check"],
            vec!["check", "-Zunstable-options"],
        ] {
            let args: Vec<OsString> = args.into_iter().map(OsString::from).collect();
            assert!(supported_cargo_command(&args).is_err(), "args: {args:?}");
        }
        assert!(supported_cargo_command(&[]).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let args = [OsString::from("check"), OsString::from_vec(vec![0x80])];
            assert!(supported_cargo_command(&args).is_err());
        }
    }

    #[test]
    fn only_config_affecting_cargo_arguments_are_ambiguous() {
        for arg in [
            "--config",
            "--config=build.rustflags=[]",
            "-C",
            "-Celsewhere",
            "-Zunstable-options",
        ] {
            assert!(cargo_arg_may_change_config(arg), "arg: {arg}");
        }
        for arg in ["--color=always", "--target", "--locked"] {
            assert!(!cargo_arg_may_change_config(arg), "arg: {arg}");
        }
    }

    #[test]
    fn windows_rustflags_environment_names_are_case_insensitive() {
        assert!(rustflags_env_name("RUSTFLAGS", false));
        assert!(!rustflags_env_name("rustflags", false));
        assert!(rustflags_env_name("rustflags", true));
        assert!(rustflags_env_name(
            "cargo_target_x86_64_unknown_linux_gnu_rustflags",
            true
        ));
        assert!(!rustflags_env_name("CARGO_BUILD_TARGET", false));
        assert!(!rustflags_env_name("MY_RUSTFLAGS", false));
        assert!(!rustflags_env_name("CARGO_RUSTFLAGS_EXTRA", false));
    }

    #[test]
    fn source_flags_preserve_repeated_additive_arguments() {
        let value: toml::Value =
            toml::from_str("[build]\nrustflags = [\"-Clink-arg=-lfoo\", \"-Clink-arg=-lfoo\"]\n")
                .unwrap();
        assert_eq!(
            source_rustflags(&value, Path::new("config.toml")).unwrap(),
            Some(vec!["-Clink-arg=-lfoo".into(), "-Clink-arg=-lfoo".into()])
        );
    }

    #[test]
    fn only_non_home_sources_with_the_same_identity_are_aliases() {
        let canonical = PathBuf::from("/physical/config.toml");
        let mut source = ConfigSource {
            logical_path: PathBuf::from("/cargo-home/config.toml"),
            canonical_path: canonical.clone(),
            content_hash: blake3::hash(b""),
            value: toml::Value::Table(Default::default()),
            cargo_home: true,
        };
        assert!(!is_cargo_home_alias(&source, &canonical));

        source.cargo_home = false;
        assert!(is_cargo_home_alias(&source, &canonical));

        source.canonical_path = PathBuf::from("/different/config.toml");
        assert!(!is_cargo_home_alias(&source, &canonical));
    }

    #[test]
    fn target_include_string_empty_and_env_forms_fail_closed() {
        for source in [
            "include = \"other.toml\"\n[build]\nrustflags = [\"-Copt-level=2\"]\n",
            "[build]\nrustflags = \"-Copt-level=2\"\n",
            "[build]\nrustflags = [\"\"]\n",
            "[build]\nrustflags = [\"\\u001f\"]\n",
            "[target.x86_64-unknown-linux-gnu]\nrustflags = [\"-Copt-level=2\"]\n",
            "[env]\nRUSTFLAGS = \"-Copt-level=2\"\n",
        ] {
            let value: toml::Value = toml::from_str(source).unwrap();
            assert!(source_rustflags(&value, Path::new("config.toml")).is_err());
        }
    }

    #[test]
    fn extensionless_config_has_cargo_compatibility_precedence() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config.toml"), "[build]\n").unwrap();
        std::fs::write(dir.path().join("config"), "[build]\n").unwrap();
        assert_eq!(selected_config(dir.path()), Some(dir.path().join("config")));
    }

    #[cfg(unix)]
    #[test]
    fn cargo_resolution_preserves_a_launcher_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("rustup");
        let launcher = dir.path().join("cargo");
        std::fs::write(&target, "#!/bin/sh\nexit 0\n").unwrap();
        std::os::unix::fs::symlink(&target, &launcher).unwrap();

        assert_eq!(
            resolve_cargo_program(launcher.as_os_str(), dir.path()).unwrap(),
            launcher
        );
    }

    #[test]
    fn bare_cargo_program_resolves_through_path() {
        let resolved = resolve_cargo_program(OsStr::new("cargo"), Path::new(".")).unwrap();
        assert!(resolved.is_absolute(), "resolved Cargo path: {resolved:?}");
        assert!(resolved.is_file(), "resolved Cargo path: {resolved:?}");
    }

    #[test]
    fn revalidation_rejects_changed_config_content() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("config.toml");
        std::fs::write(&config, "[build]\nrustflags = [\"--cfg=a\"]\n").unwrap();
        let source = read_source(config.clone(), true).unwrap();
        std::fs::write(&config, "[build]\nrustflags = [\"--cfg=b\"]\n").unwrap();
        assert!(revalidate_sources(&[source]).is_err());
    }

    #[test]
    fn revalidation_rejects_a_changed_candidate_set() {
        let dir = tempfile::tempdir().unwrap();
        let cargo_home = dir.path().join("home/.cargo");
        let cwd = dir.path().join("work/project");
        std::fs::create_dir_all(&cargo_home).unwrap();
        std::fs::create_dir_all(&cwd).unwrap();
        let home_config = cargo_home.join("config.toml");
        std::fs::write(&home_config, "[build]\nrustflags = [\"--cfg=home\"]\n").unwrap();
        let candidate_paths = cargo_config_candidates(&cwd, &cargo_home)
            .into_iter()
            .map(|(path, _)| path)
            .collect();
        let plan = NormalizationPlan {
            encoded_rustflags: "--cfg=home".into(),
            snapshots: vec![read_source(home_config, true).unwrap()],
            cwd: cwd.clone(),
            cargo_home,
            candidate_paths,
            duplicate_paths: Vec::new(),
        };
        assert!(plan_is_current(&plan));

        std::fs::create_dir_all(dir.path().join("work/.cargo")).unwrap();
        std::fs::write(
            dir.path().join("work/.cargo/config.toml"),
            "[build]\nrustflags = [\"--cfg=project\"]\n",
        )
        .unwrap();
        assert!(!plan_is_current(&plan));
    }

    #[cfg(unix)]
    #[test]
    fn revalidation_rejects_retargeted_symlink_even_with_same_content() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first.toml");
        let second = dir.path().join("second.toml");
        let alias = dir.path().join("config.toml");
        let content = "[build]\nrustflags = [\"--cfg=same\"]\n";
        std::fs::write(&first, content).unwrap();
        std::fs::write(&second, content).unwrap();
        std::os::unix::fs::symlink(&first, &alias).unwrap();
        let source = read_source(alias.clone(), false).unwrap();
        std::fs::remove_file(&alias).unwrap();
        std::os::unix::fs::symlink(&second, &alias).unwrap();
        assert!(revalidate_sources(&[source]).is_err());
    }
}
