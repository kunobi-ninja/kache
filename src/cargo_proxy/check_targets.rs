//! Keep Cargo checks from taking a build's intermediate-directory lock.

use super::*;

pub(super) struct Plan {
    target: PathBuf,
    config: BuildDirIsolationPlan,
}

/// Configure the ordinary Cargo shim, after any alternate build engine declined.
pub(super) fn configure(
    command: &mut Command,
    cargo: &Path,
    config: &crate::config::Config,
    env: &crate::invocation::InvocationEnv,
) {
    let Some(cwd) = env.cwd() else { return };
    let args: Vec<_> = command.get_args().map(OsStr::to_os_string).collect();
    if let Some(plan) = plan(cwd, &args, cargo, config, env) {
        apply(command, plan);
    }
}

pub(super) fn plan(
    cwd: &Path,
    args: &[OsString],
    cargo: &Path,
    config: &crate::config::Config,
    env: &crate::invocation::InvocationEnv,
) -> Option<Plan> {
    if config.disabled || !config.separate_check_targets {
        return None;
    }
    let manifest = eligible(args)?;
    for name in [
        "CARGO_TARGET_DIR",
        "CARGO_BUILD_TARGET_DIR",
        "CARGO_BUILD_BUILD_DIR",
    ] {
        if env.var_os(name).is_some() {
            return None;
        }
    }
    let home = match env.var_os("CARGO_HOME").filter(|home| !home.is_empty()) {
        Some(home) => cwd.join(home),
        None => cargo_home(cwd).ok()?,
    };
    let snapshots = build_dir_isolation_plan_from_sources(cwd, home).ok()??;
    if configured_target(&snapshots.snapshots) {
        return None;
    }
    // locate-project reads manifests without resolving dependencies or running
    // build scripts. Cargo, rather than an ancestor-directory heuristic, owns
    // workspace discovery, including members with an explicit workspace path.
    let mut locate = env.command(cargo).ok()?;
    locate
        .current_dir(cwd)
        .args(split_toolchain_selector(args).0)
        .args(["locate-project", "--workspace", "--message-format", "plain"]);
    if let Some(manifest) = manifest {
        locate.arg("--manifest-path").arg(manifest);
    }
    let output = locate.output().ok()?;
    if !output.status.success() {
        return None;
    }
    let manifest = String::from_utf8(output.stdout).ok()?;
    let root = Path::new(manifest.trim()).parent()?;
    if !root.is_absolute() {
        return None;
    }
    let target = root.join("target");
    let check = target.join("check");
    if [&target, &check]
        .into_iter()
        .any(|path| std::fs::symlink_metadata(path).is_ok_and(|meta| !meta.is_dir()))
    {
        return None;
    }
    Some(Plan {
        target: check,
        config: snapshots,
    })
}

pub(super) fn apply(command: &mut Command, plan: Plan) {
    if build_dir_plan_is_current(&plan.config) {
        command
            .env("CARGO_TARGET_DIR", &plan.target)
            .env("CARGO_BUILD_BUILD_DIR", &plan.target);
    }
}

fn configured_target(sources: &[ConfigSource]) -> bool {
    sources.iter().any(|source| {
        source
            .value
            .get("build")
            .and_then(toml::Value::as_table)
            .is_some_and(|build| build.contains_key("target-dir"))
            || source
                .value
                .get("env")
                .and_then(toml::Value::as_table)
                .is_some_and(|table| {
                    table.keys().any(|name| {
                        name.eq_ignore_ascii_case("CARGO_TARGET_DIR")
                            || name.eq_ignore_ascii_case("CARGO_BUILD_TARGET_DIR")
                    })
                })
    })
}

/// Return the optional manifest path only for direct check/Clippy commands.
/// Unknown aliases, config overrides and directory changes keep Cargo's layout.
fn eligible(args: &[OsString]) -> Option<Option<&OsStr>> {
    let (_, args) = split_toolchain_selector(args);
    let mut command = false;
    let mut manifest = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        if arg == "--" {
            break;
        }
        let text = arg.to_str()?;
        if cargo_arg_may_change_config(text)
            || text == "--help"
            || text == "-h"
            || text == "--version"
            || text == "-V"
            || text.starts_with("--target-dir")
            || text.starts_with("--build-dir")
        {
            return None;
        }
        if !command {
            match text {
                "check" | "c" | "clippy" => command = true,
                "--color" => {
                    args.next()?;
                }
                "--offline" | "--locked" | "--frozen" | "--quiet" | "-q" | "-v" | "-vv" => {}
                _ if text.starts_with("--color=") => {}
                _ => return None,
            }
        } else if text == "--manifest-path" {
            manifest = Some(args.next()?.as_os_str());
        } else if let Some(path) = text.strip_prefix("--manifest-path=") {
            manifest = Some(OsStr::new(path));
        }
    }
    command.then_some(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn separates_direct_checks_and_preserves_explicit_or_unknown_commands() {
        for values in [
            vec!["check"],
            vec![
                "+nightly", "--color", "always", "clippy", "--", "-D", "warnings",
            ],
            vec!["c", "--release"],
            vec!["clippy", "--", "-Copt-level=2"],
            vec!["clippy", "--", "--help"],
        ] {
            assert_eq!(eligible(&args(&values)), Some(None), "{values:?}");
        }
        for values in [
            vec![],
            vec!["build"],
            vec!["test"],
            vec!["alias"],
            vec!["check", "--target-dir", "elsewhere"],
            vec!["check", "--target-dir=elsewhere"],
            vec!["check", "--config=x=1"],
            vec!["check", "-Celsewhere"],
            vec!["clippy", "-Zunstable-options"],
            vec!["check", "--help"],
            vec!["--color"],
            vec!["check", "--manifest-path"],
        ] {
            assert!(eligible(&args(&values)).is_none(), "{values:?}");
        }
        assert_eq!(
            eligible(&args(&["check", "--manifest-path", "member/Cargo.toml"])),
            Some(Some(OsStr::new("member/Cargo.toml")))
        );
        assert_eq!(
            eligible(&args(&["clippy", "--manifest-path=member/Cargo.toml"])),
            Some(Some(OsStr::new("member/Cargo.toml")))
        );
    }

    #[cfg(unix)]
    #[test]
    fn check_target_plans_respect_environment_configuration_and_changed_snapshots() {
        use crate::invocation::InvocationEnv;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let project = root.join("workspace");
        let home = root.join("cargo-home");
        std::fs::create_dir_all(project.join(".cargo")).unwrap();
        std::fs::create_dir_all(&home).unwrap();
        let cargo = root.join("cargo");
        kache_fs::testutil::write_executable(&cargo, "#!/bin/sh\nprintf '%s\\n' \"$MANIFEST\"\n");
        let config = crate::test_support::test_config(root.join("cache"));
        let environment = |extra: Option<(&str, &str)>, manifest: &str| {
            let mut vars = vec![
                (OsString::from("CARGO_HOME"), home.clone().into_os_string()),
                (OsString::from("MANIFEST"), OsString::from(manifest)),
            ];
            if let Some((name, value)) = extra {
                vars.push((name.into(), value.into()));
            }
            InvocationEnv::from_parts(vars, Some(project.clone()), None, root.clone())
        };
        let manifest = project.join("Cargo.toml").display().to_string();
        let env = environment(None, &manifest);
        let get = |config: &crate::config::Config, env: &InvocationEnv| {
            plan(&project, &args(&["check"]), &cargo, config, env)
        };
        assert_eq!(
            get(&config, &env).unwrap().target,
            project.join("target/check")
        );
        let home_config = home.join("config.toml");
        std::fs::write(&home_config, "[build]\ntarget-dir = 'from-captured-home'").unwrap();
        assert!(
            get(&config, &env).is_none(),
            "the captured Cargo home takes precedence over this process's home"
        );
        std::fs::remove_file(home_config).unwrap();
        let mut disabled = config.clone();
        disabled.disabled = true;
        assert!(get(&disabled, &env).is_none());
        disabled.disabled = false;
        disabled.separate_check_targets = false;
        assert!(get(&disabled, &env).is_none());
        for name in [
            "CARGO_TARGET_DIR",
            "CARGO_BUILD_TARGET_DIR",
            "CARGO_BUILD_BUILD_DIR",
        ] {
            assert!(
                get(&config, &environment(Some((name, "explicit")), &manifest)).is_none(),
                "{name}"
            );
        }
        assert!(get(&config, &environment(None, "relative/Cargo.toml")).is_none());
        let file = project.join(".cargo/config.toml");
        for text in [
            "[build]\ntarget-dir = 'custom'",
            "[build]\nbuild-dir = 'custom'",
            "[env]\nCARGO_TARGET_DIR = 'custom'",
            "[env]\nCARGO_BUILD_TARGET_DIR = 'custom'",
            "[env]\nCARGO_BUILD_BUILD_DIR = 'custom'",
            "include = 'other.toml'",
            "invalid toml [",
        ] {
            std::fs::write(&file, text).unwrap();
            assert!(get(&config, &env).is_none(), "{text}");
        }
        std::fs::write(&file, "[net]\noffline = true").unwrap();
        let stale = get(&config, &env).unwrap();
        std::fs::write(&file, "[net]\noffline = false").unwrap();
        let mut command = Command::new(&cargo);
        apply(&mut command, stale);
        assert_eq!(
            command.get_envs().count(),
            0,
            "a changed configuration keeps Cargo's layout"
        );
        command.arg("clippy");
        configure(&mut command, &cargo, &config, &env);
        let configured: Vec<_> = command.get_envs().collect();
        assert_eq!(configured.len(), 2);
        assert!(
            configured
                .iter()
                .all(|(_, value)| *value == Some(project.join("target/check").as_os_str()))
        );
        let outside = root.join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, project.join("target")).unwrap();
        assert!(
            get(&config, &env).is_none(),
            "an explicit target symlink keeps its layout"
        );
        std::fs::remove_file(project.join("target")).unwrap();
        std::fs::create_dir(project.join("target")).unwrap();
        assert!(get(&config, &env).is_some());
        let destination = project.join("target/check");
        std::fs::write(&destination, "user-owned file").unwrap();
        assert!(
            get(&config, &env).is_none(),
            "an existing file cannot become a check target"
        );
        assert_eq!(
            std::fs::read_to_string(&destination).unwrap(),
            "user-owned file"
        );
        std::fs::remove_file(&destination).unwrap();
        std::os::unix::fs::symlink(&outside, &destination).unwrap();
        assert!(
            get(&config, &env).is_none(),
            "an explicit check-target symlink keeps its layout"
        );
    }
}
