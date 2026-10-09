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
