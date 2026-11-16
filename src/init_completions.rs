//! Generate and activate completions as part of the shell setup.
#![cfg(unix)]

use anyhow::{Context, Result, ensure};
use std::path::{Path, PathBuf};

use crate::init_shell::{COMPLETIONS_BLOCK, Edit, Shell};

pub(crate) struct Plan {
    pub script: PathBuf,
    pub edits: Vec<Edit>,
}

pub(crate) fn plan(
    shell: Shell,
    startup_files: Vec<PathBuf>,
    home: &Path,
    config_home: Option<&Path>,
    data_home: Option<&Path>,
) -> Result<Plan> {
    let data = data_home
        .map(Path::to_path_buf)
        .unwrap_or_else(|| home.join(".local/share"))
        .join("kache/completions");
    let (generator, script) = match shell {
        Shell::Zsh => (clap_complete::Shell::Zsh, data.join("kache.zsh")),
        Shell::Bash => (clap_complete::Shell::Bash, data.join("kache.bash")),
        Shell::Fish => (
            clap_complete::Shell::Fish,
            config_home
                .map(Path::to_path_buf)
                .unwrap_or_else(|| home.join(".config"))
                .join("fish/completions/kache.fish"),
        ),
    };
    ensure!(
        script.is_absolute(),
        "completion directory must be absolute"
    );
    let mut generated = Vec::new();
    crate::generate_completions(generator, &mut generated);
    let generated = String::from_utf8(generated).context("completion script must be UTF-8")?;
    let mut edits = vec![Edit::plan_file(script.clone(), generated)?];
    if let Some(activation) = activation(shell, &script)? {
        for path in startup_files {
            edits.push(Edit::plan_block(path, COMPLETIONS_BLOCK, &activation)?);
        }
    }
    Ok(Plan { script, edits })
}

fn activation(shell: Shell, script: &Path) -> Result<Option<String>> {
    if shell == Shell::Fish {
        return Ok(None);
    }
    let quoted = shell.quote_path(script)?;
    Ok(Some(if shell == Shell::Zsh {
        format!(
            "if [[ -o interactive && -r {quoted} ]]; then\n    if (( ! $+functions[compdef] )); then\n        autoload -Uz compinit\n        compinit\n    fi\n    source {quoted}\nfi"
        )
    } else {
        // POSIX syntax: the bash login file can be `~/.profile`, which `sh`
        // login shells such as dash also read (kunobi-ninja/kache#1923).
        format!(
            "case $- in\n    *i*) if [ -n \"${{BASH_VERSION-}}\" ] && [ -r {quoted} ]; then\n        . {quoted}\n    fi ;;\nesac"
        )
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn respects_custom_completion_directories_and_protects_managed_files() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let config = temp.path().join("config");
        let data = temp.path().join("data");
        for (shell, relative) in [
            (Shell::Zsh, "kache/completions/kache.zsh"),
            (Shell::Bash, "kache/completions/kache.bash"),
        ] {
            let planned = plan(
                shell,
                vec![home.join("rc")],
                &home,
                Some(&config),
                Some(&data),
            )
            .unwrap();
            assert_eq!(planned.script, data.join(relative));
        }
        let fish = plan(Shell::Fish, Vec::new(), &home, Some(&config), Some(&data)).unwrap();
        assert_eq!(fish.script, config.join("fish/completions/kache.fish"));
        assert_eq!(fish.edits.len(), 1, "Fish uses its autoload directory");
        assert!(
            plan(
                Shell::Zsh,
                Vec::new(),
                &home,
                None,
                Some(Path::new("relative"))
            )
            .is_err()
        );
        assert!(
            plan(
                Shell::Fish,
                Vec::new(),
                &home,
                Some(Path::new("relative")),
                None
            )
            .is_err()
        );

        let target = temp.path().join("user-file");
        std::fs::write(&target, "keep this\n").unwrap();
        let script = home.join(".local/share/kache/completions/kache.zsh");
        std::fs::create_dir_all(script.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&target, &script).unwrap();
        assert!(plan(Shell::Zsh, vec![home.join("rc")], &home, None, None).is_err());
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "keep this\n");
        std::fs::remove_file(&script).unwrap();
        std::fs::hard_link(&target, &script).unwrap();
        assert!(plan(Shell::Zsh, vec![home.join("rc")], &home, None, None).is_err());
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "keep this\n");
        std::fs::remove_file(&script).unwrap();
        std::os::unix::fs::symlink(&target, home.join("rc")).unwrap();
        assert!(plan(Shell::Zsh, vec![home.join("rc")], &home, None, None).is_err());
        assert!(
            !script.exists(),
            "planning must not partially install a script"
        );
    }

    #[test]
    fn shell_activation_loads_completions_without_relying_on_a_cached_fpath() {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home with ' quote and $(touch injected)");
        std::fs::create_dir(&home).unwrap();
        for (shell, program, check) in [
            (
                Shell::Zsh,
                "zsh",
                "(( $+functions[_kache] )) && [[ ${_comps[kache]} == _kache ]]",
            ),
            (
                Shell::Bash,
                "bash",
                "declare -F _kache >/dev/null && [[ $(complete -p kache) == *_kache* ]] && COMP_WORDS=(kache mon) && COMP_CWORD=1 && _kache kache mon kache && [[ ${COMPREPLY[0]} == monitor ]]",
            ),
        ] {
            if std::process::Command::new(program)
                .arg("--version")
                .output()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
            {
                continue;
            }
            let planned = plan(shell, Vec::new(), &home, None, None).unwrap();
            for edit in &planned.edits {
                edit.apply_keeping(false).unwrap();
            }
            let body = activation(shell, &planned.script).unwrap().unwrap();
            let run = |prefix: &str, assertion: &str| {
                let mut command = std::process::Command::new(program);
                match shell {
                    Shell::Zsh => {
                        command.args(["-f", "-i", "-c"]);
                    }
                    Shell::Bash => {
                        command.args(["--noprofile", "--norc", "-i", "-c"]);
                    }
                    Shell::Fish => unreachable!(),
                }
                command
                    .arg(format!("{prefix}\n{body}\n{assertion}"))
                    .env("HOME", &home)
                    .env("ZDOTDIR", &home)
                    .current_dir(temp.path())
                    .output()
                    .unwrap()
            };
            let output = run("", check);
            assert!(
                output.status.success(),
                "{program}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                !temp.path().join("injected").exists(),
                "shell paths must stay quoted"
            );
            if shell == Shell::Zsh {
                let output = run(
                    "autoload -Uz compinit; compinit; compinit() { return 91; }",
                    check,
                );
                assert!(
                    output.status.success(),
                    "existing compinit must be reused: {output:?}"
                );
            }
            std::fs::remove_file(&planned.script).unwrap();
            let output = run("", ":");
            assert!(
                output.status.success(),
                "a missing script must not break startup"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn the_bash_block_parses_in_posix_sh_and_sources_only_under_bash() {
        let temp = tempfile::tempdir().unwrap();
        let script = temp.path().join("it's completions.bash");
        std::fs::write(&script, "echo sourced\n").unwrap();
        let block = activation(Shell::Bash, &script).unwrap().unwrap();
        assert!(!block.contains("[["), "{block}");
        let profile = temp.path().join("profile");
        std::fs::write(&profile, format!("{block}\necho done\n")).unwrap();
        let run = |shell: &str, interactive: bool| {
            let mut command = std::process::Command::new(shell);
            if interactive {
                command.arg("-i");
            }
            let output = command
                .arg("-c")
                .arg(format!(". '{}'", profile.display()))
                .env_remove("BASH_VERSION")
                .env("HOME", temp.path())
                .output()
                .unwrap();
            (
                output.status.success(),
                String::from_utf8_lossy(&output.stdout).into_owned(),
            )
        };
        let (ok, out) = run("sh", false);
        assert!(ok, "{out}");
        assert_eq!(out, "done\n");
        let (ok, out) = run("bash", true);
        assert!(ok, "{out}");
        assert_eq!(out, "sourced\ndone\n");
        let (ok, out) = run("bash", false);
        assert!(ok, "{out}");
        assert_eq!(out, "done\n");
    }
}
