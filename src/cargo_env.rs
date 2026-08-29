//! Cargo `[env]` keys `kache init` may add for host C/C++ wrapping.
//!
//! Cargo has no `CC_WRAPPER`. Setting `CC` / `CXX` makes `cargo build --target`
//! use the host shim as the cross compiler. `HOST_CC` / `HOST_CXX` are what the
//! `cc` crate consults for host compiles only.
//!
//! Existing `[env]` keys are never overwritten. Cargo itself also leaves a
//! variable already present in the process environment alone unless `force` is
//! set; we never set `force`.

use anyhow::{Context, Result};
use std::path::Path;

/// One `[env]` assignment `kache init` is willing to add.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CargoEnvAssignment {
    pub name: &'static str,
    pub value: &'static str,
}

/// Keys init may add. Unix gets host compiler wrappers; Windows only tells the
/// `cc` crate that `kache` is a known wrapper, because choosing clang-cl vs
/// MSVC is the user's call.
pub(crate) fn desired_assignments() -> &'static [CargoEnvAssignment] {
    #[cfg(unix)]
    {
        &[
            CargoEnvAssignment {
                name: "HOST_CC",
                value: "kache cc",
            },
            CargoEnvAssignment {
                name: "HOST_CXX",
                value: "kache c++",
            },
            CargoEnvAssignment {
                name: "CC_KNOWN_WRAPPER_CUSTOM",
                value: "kache",
            },
        ]
    }
    #[cfg(windows)]
    {
        &[CargoEnvAssignment {
            name: "CC_KNOWN_WRAPPER_CUSTOM",
            value: "kache",
        }]
    }
}

/// Assignments not already present in `content`. A key with any value, including
/// a different one, is left alone.
pub(crate) fn missing_assignments(content: &str) -> Result<Vec<CargoEnvAssignment>> {
    let parsed: toml::Value = if content.trim().is_empty() {
        toml::Value::Table(toml::map::Map::new())
    } else {
        toml::from_str(content).context("parsing cargo config")?
    };
    let env_table = parsed.get("env");
    Ok(desired_assignments()
        .iter()
        .copied()
        .filter(|assignment| {
            env_table
                .and_then(|table| table.get(assignment.name))
                .is_none()
        })
        .collect())
}

pub(crate) fn missing_assignments_from_path(path: &Path) -> Result<Vec<CargoEnvAssignment>> {
    if !path.exists() {
        return Ok(desired_assignments().to_vec());
    }
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    missing_assignments(&content)
}

/// Insert missing keys under `[env]`, creating the table if needed. Never
/// writes `CC` or `CXX`.
pub(crate) fn apply_cargo_env_edit(existing: &str, additions: &[CargoEnvAssignment]) -> String {
    if additions.is_empty() {
        return existing.to_string();
    }

    let mut block = String::new();
    for assignment in additions {
        block.push_str(&format!("{} = \"{}\"\n", assignment.name, assignment.value));
    }

    if cargo_config_has_env_table(existing) {
        let mut out = String::with_capacity(existing.len() + block.len());
        let mut inserted = false;
        for line in existing.lines() {
            out.push_str(line);
            out.push('\n');
            if !inserted && is_env_table_header(line) {
                out.push_str(&block);
                inserted = true;
            }
        }
        if !inserted {
            if !out.ends_with('\n') {
                out.push('\n');
            }
            out.push_str("\n[env]\n");
            out.push_str(&block);
        }
        return out;
    }

    let mut out = existing.to_string();
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str("[env]\n");
    out.push_str(&block);
    out
}

fn cargo_config_has_env_table(content: &str) -> bool {
    content.lines().any(is_env_table_header)
}

fn is_env_table_header(line: &str) -> bool {
    matches!(line.trim(), "[env]" | "[env]\r")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_content_needs_every_desired_key() {
        let missing = missing_assignments("").unwrap();
        assert_eq!(missing, desired_assignments());
    }

    #[test]
    fn existing_key_is_not_replaced() {
        let content = "[env]\nHOST_CC = \"gcc\"\nCC_KNOWN_WRAPPER_CUSTOM = \"kache\"\n";
        let missing = missing_assignments(content).unwrap();
        assert!(
            !missing.iter().any(|a| a.name == "HOST_CC"),
            "user HOST_CC must win, got {missing:?}"
        );
        assert!(
            !missing.iter().any(|a| a.name == "CC_KNOWN_WRAPPER_CUSTOM"),
            "matching CC_KNOWN_WRAPPER_CUSTOM must not be re-added"
        );
    }

    #[test]
    fn apply_never_writes_cc_or_cxx() {
        let out = apply_cargo_env_edit("", desired_assignments());
        for line in out.lines() {
            let trimmed = line.trim();
            assert!(
                !trimmed.starts_with("CC ") && !trimmed.starts_with("CC=") && trimmed != "CC",
                "must not set CC: {trimmed}"
            );
            assert!(
                !trimmed.starts_with("CXX ") && !trimmed.starts_with("CXX="),
                "must not set CXX: {trimmed}"
            );
        }
        assert!(out.contains("[env]"));
        assert!(out.contains("CC_KNOWN_WRAPPER_CUSTOM = \"kache\""));
    }

    #[test]
    fn apply_appends_env_table_after_build() {
        let existing = "[build]\nrustc-wrapper = \"kache\"\n";
        let out = apply_cargo_env_edit(existing, desired_assignments());
        assert!(out.contains("[build]"));
        assert!(out.contains("rustc-wrapper = \"kache\""));
        assert!(out.contains("[env]"));
        assert!(out.contains("CC_KNOWN_WRAPPER_CUSTOM = \"kache\""));
    }

    #[test]
    fn apply_inserts_under_existing_env_table() {
        let existing = "# keep me first\n[env]\nRUST_BACKTRACE = \"1\"\n";
        let additions = [CargoEnvAssignment {
            name: "CC_KNOWN_WRAPPER_CUSTOM",
            value: "kache",
        }];
        let out = apply_cargo_env_edit(existing, &additions);
        assert!(out.contains("RUST_BACKTRACE = \"1\""));
        assert!(out.contains("CC_KNOWN_WRAPPER_CUSTOM = \"kache\""));
        assert_eq!(out.matches("[env]").count(), 1);
        let preamble = out.split("[env]").next().expect("env table");
        assert!(
            preamble.contains("# keep me first"),
            "comment before [env] must stay before it"
        );
        assert!(
            !preamble.contains("CC_KNOWN_WRAPPER_CUSTOM"),
            "keys must be inserted under [env], not after the first line: {out}"
        );
    }

    #[test]
    fn apply_puts_a_blank_line_before_a_new_env_table() {
        let existing = "[build]\nrustc-wrapper = \"kache\"";
        let additions = [CargoEnvAssignment {
            name: "CC_KNOWN_WRAPPER_CUSTOM",
            value: "kache",
        }];
        let out = apply_cargo_env_edit(existing, &additions);
        assert!(
            out.contains(
                "[build]\nrustc-wrapper = \"kache\"\n\n[env]\nCC_KNOWN_WRAPPER_CUSTOM = \"kache\"\n"
            ),
            "expected a blank line between [build] and a newly created [env], got:\n{out}"
        );
    }

    #[test]
    fn missing_assignments_from_absent_file_are_all_desired() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let missing = missing_assignments_from_path(&path).unwrap();
        assert_eq!(missing, desired_assignments());
    }

    #[cfg(unix)]
    #[test]
    fn unix_assignments_include_host_compilers() {
        let names: Vec<_> = desired_assignments().iter().map(|a| a.name).collect();
        assert!(names.contains(&"HOST_CC"));
        assert!(names.contains(&"HOST_CXX"));
        let out = apply_cargo_env_edit("", desired_assignments());
        assert!(out.contains("HOST_CC = \"kache cc\""));
        assert!(out.contains("HOST_CXX = \"kache c++\""));
    }

    #[cfg(windows)]
    #[test]
    fn windows_assignments_do_not_pick_a_c_compiler() {
        let names: Vec<_> = desired_assignments().iter().map(|a| a.name).collect();
        assert!(!names.contains(&"HOST_CC"));
        assert!(!names.contains(&"HOST_CXX"));
    }
}
