use serde::Deserialize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

const CARGO_CONFIG_NAMES: [&str; 2] = ["config.toml", "config"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WrapperSetting {
    Environment { value: String },
    CargoConfig { value: String, path: PathBuf },
}

#[derive(Debug, Deserialize)]
struct CargoConfigFile {
    build: Option<CargoBuildConfig>,
}

#[derive(Debug, Deserialize)]
struct CargoBuildConfig {
    #[serde(rename = "rustc-wrapper")]
    rustc_wrapper: Option<String>,
}

pub(crate) fn resolve_wrapper_setting() -> Option<WrapperSetting> {
    resolve_wrapper_setting_from(
        std::env::var_os("RUSTC_WRAPPER"),
        std::env::current_dir().ok().as_deref(),
        std::env::var_os("CARGO_HOME").map(PathBuf::from),
        dirs::home_dir(),
    )
}

pub(crate) fn cargo_wrapper_setting() -> Option<(String, PathBuf)> {
    cargo_wrapper_setting_from(
        std::env::current_dir().ok().as_deref(),
        std::env::var_os("CARGO_HOME").map(PathBuf::from),
        dirs::home_dir(),
    )
}

pub(crate) fn wrapper_status_line() -> String {
    match resolve_wrapper_setting() {
        Some(WrapperSetting::Environment { value }) if value.contains("kache") => {
            "RUSTC_WRAPPER=kache ✓".to_string()
        }
        Some(WrapperSetting::Environment { value }) => {
            format!("RUSTC_WRAPPER={value} (not kache!)")
        }
        Some(WrapperSetting::CargoConfig { value, path }) if value.contains("kache") => {
            format!("rustc-wrapper=kache via {} ✓", display_path(&path))
        }
        Some(WrapperSetting::CargoConfig { value, path }) => {
            format!(
                "rustc-wrapper={value} in {} (not kache!)",
                display_path(&path)
            )
        }
        None => "rustc-wrapper not configured ✗".to_string(),
    }
}

pub(crate) fn display_path(path: &Path) -> String {
    if let Some(home) = dirs::home_dir()
        && let Ok(stripped) = path.strip_prefix(&home)
    {
        return format!("~/{}", stripped.display());
    }
    path.display().to_string()
}

fn resolve_wrapper_setting_from(
    rustc_wrapper_env: Option<OsString>,
    current_dir: Option<&Path>,
    cargo_home: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Option<WrapperSetting> {
    if let Some(value) = rustc_wrapper_env {
        return Some(WrapperSetting::Environment {
            value: value.to_string_lossy().into_owned(),
        });
    }

    cargo_wrapper_setting_from(current_dir, cargo_home, home)
        .map(|(value, path)| WrapperSetting::CargoConfig { value, path })
}

fn cargo_wrapper_setting_from(
    current_dir: Option<&Path>,
    cargo_home: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Option<(String, PathBuf)> {
    let mut resolved = None;
    for path in cargo_config_candidates(current_dir, cargo_home, home) {
        if let Some(value) = read_rustc_wrapper_from_config(&path) {
            resolved = Some((value, path));
        }
    }
    resolved
}

fn cargo_config_candidates(
    current_dir: Option<&Path>,
    cargo_home: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Vec<PathBuf> {
    let mut candidates = Vec::new();

    if let Some(cargo_home) = cargo_home.or_else(|| home.clone().map(|home| home.join(".cargo"))) {
        push_config_candidates(&cargo_home, &mut candidates);
    }

    if let Some(current_dir) = current_dir {
        let mut ancestors: Vec<&Path> = current_dir.ancestors().collect();
        ancestors.reverse();
        for ancestor in ancestors {
            push_config_candidates(&ancestor.join(".cargo"), &mut candidates);
        }
    }

    candidates
}

fn push_config_candidates(config_dir: &Path, candidates: &mut Vec<PathBuf>) {
    for name in CARGO_CONFIG_NAMES {
        let path = config_dir.join(name);
        if path.exists() && !candidates.iter().any(|candidate| candidate == &path) {
            candidates.push(path);
        }
    }
}

fn read_rustc_wrapper_from_config(path: &Path) -> Option<String> {
    let content = std::fs::read_to_string(path).ok()?;
    let parsed: CargoConfigFile = toml::from_str(&content).ok()?;
    parsed.build?.rustc_wrapper
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_wrapper_takes_precedence_over_cargo_config() {
        let dir = tempfile::tempdir().unwrap();
        let cargo_home = dir.path().join(".cargo");
        std::fs::create_dir_all(&cargo_home).unwrap();
        std::fs::write(
            cargo_home.join("config.toml"),
            "[build]\nrustc-wrapper = \"kache\"\n",
        )
        .unwrap();

        let setting = resolve_wrapper_setting_from(
            Some(OsString::from("sccache")),
            Some(dir.path()),
            Some(cargo_home),
            Some(dir.path().to_path_buf()),
        )
        .unwrap();

        assert_eq!(
            setting,
            WrapperSetting::Environment {
                value: "sccache".into()
            }
        );
    }

    #[test]
    fn cargo_config_falls_back_to_home_config() {
        let dir = tempfile::tempdir().unwrap();
        let cargo_home = dir.path().join(".cargo");
        std::fs::create_dir_all(&cargo_home).unwrap();
        std::fs::write(
            cargo_home.join("config.toml"),
            "[build]\nrustc-wrapper = \"kache\"\n",
        )
        .unwrap();

        let setting = resolve_wrapper_setting_from(
            None,
            Some(dir.path()),
            Some(cargo_home.clone()),
            Some(dir.path().to_path_buf()),
        )
        .unwrap();

        assert_eq!(
            setting,
            WrapperSetting::CargoConfig {
                value: "kache".into(),
                path: cargo_home.join("config.toml"),
            }
        );
    }

    #[test]
    fn nearest_project_cargo_config_overrides_home_config() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("home");
        let project = dir.path().join("workspace/project");
        std::fs::create_dir_all(home.join(".cargo")).unwrap();
        std::fs::create_dir_all(project.join(".cargo")).unwrap();
        std::fs::write(
            home.join(".cargo/config.toml"),
            "[build]\nrustc-wrapper = \"sccache\"\n",
        )
        .unwrap();
        std::fs::write(
            project.join(".cargo/config.toml"),
            "[build]\nrustc-wrapper = \"kache\"\n",
        )
        .unwrap();

        let setting = cargo_wrapper_setting_from(None, None, Some(home.clone()));
        assert_eq!(
            setting,
            Some(("sccache".into(), home.join(".cargo/config.toml")))
        );

        let setting = cargo_wrapper_setting_from(Some(&project), None, Some(home));
        assert_eq!(
            setting,
            Some(("kache".into(), project.join(".cargo/config.toml")))
        );
    }

    #[test]
    fn status_line_reports_cargo_config_source() {
        let dir = tempfile::tempdir().unwrap();
        let cargo_home = dir.path().join(".cargo");
        std::fs::create_dir_all(&cargo_home).unwrap();
        let config_path = cargo_home.join("config.toml");
        std::fs::write(&config_path, "[build]\nrustc-wrapper = \"kache\"\n").unwrap();

        let setting = cargo_wrapper_setting_from(
            Some(dir.path()),
            Some(cargo_home),
            Some(dir.path().to_path_buf()),
        )
        .unwrap();

        assert_eq!(setting.0, "kache");
        assert!(display_path(&config_path).ends_with(".cargo/config.toml"));
    }
}
