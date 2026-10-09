//! The `rerun-if-changed` and `rerun-if-env-changed` lines a build script
//! prints, and what the run cache makes of them.

use super::Environment;

/// One `rerun-if-*` directive, with the path or variable name as printed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rerun<'a> {
    Changed(&'a str),
    EnvChanged(&'a str),
}

/// The `rerun-if-*` directive on `line`, in the `cargo::` or the `cargo:`
/// form.
pub(crate) fn rerun_directive(line: &str) -> Option<Rerun<'_>> {
    let directive = line
        .strip_prefix("cargo::")
        .or_else(|| line.strip_prefix("cargo:"))?;
    if let Some(path) = directive.strip_prefix("rerun-if-changed=") {
        return Some(Rerun::Changed(path));
    }
    directive
        .strip_prefix("rerun-if-env-changed=")
        .map(Rerun::EnvChanged)
}

/// What a build script declared, as printed and in order.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Declared {
    pub(crate) paths: Vec<String>,
    pub(crate) env: Vec<String>,
}

/// The declarations in a script's recorded stdout, read by Cargo's rules:
/// lines split at `\n`, a line that is not UTF-8 skipped, each line trimmed.
/// `relocated` is the `OUT_DIR` Cargo recorded for the run and the current
/// one; Cargo replaces the first with the second in every value when the
/// target directory moved.
pub(crate) fn cargo_declarations(stdout: &[u8], relocated: Option<(&str, &str)>) -> Declared {
    let relocate = |value: &str| match relocated {
        Some((recorded, current)) if !recorded.is_empty() => value.replace(recorded, current),
        _ => value.to_string(),
    };
    let mut declared = Declared::default();
    for line in stdout.split(|byte| *byte == b'\n') {
        let Ok(line) = std::str::from_utf8(line) else {
            continue;
        };
        match rerun_directive(line.trim()) {
            Some(Rerun::Changed(path)) => declared.paths.push(relocate(path)),
            Some(Rerun::EnvChanged(name)) => declared.env.push(relocate(name)),
            None => {}
        }
    }
    declared
}

/// The `rerun-if-*` declarations in a script's stdout. `None` when the script
/// asked to run every time, which no finite key can honour.
pub(super) fn parse_declarations(
    stdout: &str,
    environment: &Environment,
) -> Option<(Vec<String>, Vec<String>, bool)> {
    let mut inputs = std::collections::BTreeSet::new();
    let mut env = std::collections::BTreeSet::new();
    for line in stdout.lines() {
        match rerun_directive(line) {
            Some(Rerun::Changed(path)) => {
                // Cargo treats an empty path as "always rerun".
                if path.is_empty() {
                    return None;
                }
                let resolved = environment.manifest_dir.join(path);
                // An input under OUT_DIR is one this run produced; scripts use
                // that shape to force a rerun every time.
                if resolved.starts_with(&environment.out_dir) {
                    return None;
                }
                inputs.insert(environment.normalize_str(&resolved.to_string_lossy()));
            }
            Some(Rerun::EnvChanged(name)) if !name.is_empty() => {
                env.insert(name.to_string());
            }
            _ => {}
        }
    }
    let default_package = inputs.is_empty() && env.is_empty();
    if default_package {
        inputs.insert("${KACHE_MANIFEST_DIR}".to_string());
    }
    Some((
        inputs.into_iter().collect(),
        env.into_iter().collect(),
        default_package,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cargo_declarations_read_a_recorded_stdout_as_cargo_does() {
        let stdout = b"cargo:rerun-if-changed=build.rs\n\
            cargo::rerun-if-env-changed=APP_MODE\n\
            cargo:rerun-if-changed=a=b.txt\n  \
            cargo:rerun-if-changed=padded.txt \t\r\n\
            cargo:rerun-if-changed=crlf.txt\r\n\
            cargo:rerun-if-changed=\n\
            cargo:rustc-cfg=x\n\
            cargo:rerun-if-changedx=no.txt\n\
            rerun-if-changed=plain.txt\n\
            cargo:rerun-if-changed=\xff.txt\n\
            cargo:rerun-if-env-changed=\n\
            cargo:rerun-if-changed=last.txt";
        let declared = cargo_declarations(stdout, None);
        assert_eq!(
            declared.paths,
            [
                "build.rs",
                "a=b.txt",
                "padded.txt",
                "crlf.txt",
                "",
                "last.txt"
            ]
        );
        assert_eq!(declared.env, ["APP_MODE", ""]);
    }

    #[test]
    fn cargo_declarations_move_the_recorded_out_dir_to_the_current_one() {
        let stdout = b"cargo:rerun-if-changed=/old/out/gen.rs\n\
            cargo:rerun-if-changed=/other/x\n\
            cargo:rerun-if-env-changed=/old/out\n";
        let declared = cargo_declarations(stdout, Some(("/old/out", "/new/out")));
        assert_eq!(declared.paths, ["/new/out/gen.rs", "/other/x"]);
        assert_eq!(declared.env, ["/new/out"]);
        assert_eq!(
            cargo_declarations(b"cargo:rerun-if-changed=x\n", Some(("", "/new"))).paths,
            ["x"],
            "an empty record moves nothing"
        );
    }

    #[test]
    fn a_rerun_directive_is_read_in_either_form() {
        assert_eq!(
            rerun_directive("cargo:rerun-if-changed=a"),
            Some(Rerun::Changed("a"))
        );
        assert_eq!(
            rerun_directive("cargo::rerun-if-changed=a"),
            Some(Rerun::Changed("a"))
        );
        assert_eq!(
            rerun_directive("cargo::rerun-if-env-changed=A"),
            Some(Rerun::EnvChanged("A"))
        );
        assert_eq!(rerun_directive("cargo:rustc-cfg=x"), None);
        assert_eq!(rerun_directive(" cargo:rerun-if-changed=a"), None);
    }
}
