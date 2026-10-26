//! File-backed configuration commands. Environment and host overrides stay separate.

use anyhow::{Context, Result, bail};
use clap::{Args, Subcommand};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

#[derive(Args, Debug, Default)]
pub(crate) struct Scope {
    /// Use the nearest project config, or create .kache.toml in this directory
    #[arg(long, conflicts_with_all = ["global", "file"])]
    project: bool,
    /// Use the user config, ignoring KACHE_CONFIG and project files
    #[arg(long, conflicts_with = "file")]
    global: bool,
    /// Use this configuration file
    #[arg(long, value_name = "PATH")]
    file: Option<PathBuf>,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Command {
    /// Read a value explicitly set in the selected file
    Get {
        #[command(flatten)]
        scope: Scope,
        key: String,
    },
    /// List values explicitly set in the selected file
    List {
        #[command(flatten)]
        scope: Scope,
    },
    /// Set a known configuration key; accepts TOML values or an unquoted string
    Set {
        #[command(flatten)]
        scope: Scope,
        key: String,
        #[arg(allow_hyphen_values = true)]
        value: String,
    },
    /// Remove a value from the selected file
    Unset {
        #[command(flatten)]
        scope: Scope,
        key: String,
    },
    /// Print the selected file's path, even if it does not exist
    Path {
        #[command(flatten)]
        scope: Scope,
    },
}

impl Scope {
    fn path(&self) -> Result<PathBuf> {
        let cwd = std::env::current_dir().context("resolving configuration directory")?;
        let path = if let Some(path) = &self.file {
            path.clone()
        } else if self.global {
            crate::config::config_file_path()
        } else if self.project {
            cwd.ancestors()
                .map(|parent| parent.join(".kache.toml"))
                .find(|path| path.is_file())
                .unwrap_or_else(|| cwd.join(".kache.toml"))
        } else {
            crate::config::resolve_config_path()
        };
        Ok(cwd.join(path))
    }
}

pub(crate) fn run(command: &Command, json: bool) -> Result<()> {
    let scope = match command {
        Command::Get { scope, .. }
        | Command::List { scope }
        | Command::Set { scope, .. }
        | Command::Unset { scope, .. }
        | Command::Path { scope } => scope,
    };
    let path = scope.path()?;
    let body = match command {
        Command::Path { .. } => {
            if !json {
                println!("{}", path.display());
            }
            serde_json::json!({"path": path.display().to_string()})
        }
        Command::Get { key, .. } => {
            let keys = key_parts(key)?;
            let document = read_document(&path)?;
            let values: toml::Value = toml::from_str(&document.to_string())?;
            let value = lookup(&values, &keys)
                .with_context(|| format!("{key} is not set in {}", path.display()))?;
            if !json {
                match value.as_str() {
                    Some(text) => println!("{text}"),
                    None => println!("{value}"),
                }
            }
            serde_json::json!({"path": path.display().to_string(), "key": key, "value": value})
        }
        Command::List { .. } => {
            let document = read_document(&path)?;
            let values: toml::Value = toml::from_str(&document.to_string())?;
            if !json {
                print_values("", &values);
            }
            serde_json::json!({"path": path.display().to_string(), "values": values})
        }
        Command::Set { key, value, .. } => {
            edit(&path, key, Some(value))?;
            if !json {
                println!("Set {key} in {}", path.display());
            }
            serde_json::json!({"path": path.display().to_string(), "key": key, "action": "set"})
        }
        Command::Unset { key, .. } => {
            edit(&path, key, None)?;
            if !json {
                println!("Unset {key} in {}", path.display());
            }
            serde_json::json!({"path": path.display().to_string(), "key": key, "action": "unset"})
        }
    };
    if json {
        crate::machine::emit("config", body, Vec::new())?;
    }
    Ok(())
}

fn key_parts(key: &str) -> Result<Vec<&str>> {
    let keys: Vec<_> = key.split('.').collect();
    if keys.iter().any(|part| {
        part.is_empty()
            || !part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    }) {
        bail!("use a dotted key such as cache.local_only");
    }
    Ok(keys)
}

fn lookup<'a>(value: &'a toml::Value, keys: &[&str]) -> Option<&'a toml::Value> {
    keys.iter().try_fold(value, |value, key| value.get(*key))
}

fn read_document(path: &Path) -> Result<toml_edit::DocumentMut> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(error).with_context(|| format!("reading {}", path.display())),
    };
    text.parse()
        .with_context(|| format!("{} is not valid TOML", path.display()))
}

fn print_values(prefix: &str, value: &toml::Value) {
    if let Some(table) = value.as_table() {
        for (key, value) in table {
            let key = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            print_values(&key, value);
        }
    } else {
        println!("{prefix} = {value}");
    }
}

fn edit(path: &Path, key: &str, value: Option<&str>) -> Result<()> {
    let keys = key_parts(key)?;
    // Resolve existing symlinks before choosing a stable lock or replacing the file.
    let path = if fs::symlink_metadata(path).is_ok() {
        path.canonicalize()
            .context("resolving configuration file")?
    } else {
        path.to_owned()
    };
    let parent = path
        .parent()
        .context("configuration file needs a parent directory")?;
    fs::create_dir_all(parent).context("creating configuration directory")?;
    let mut lock_name = OsString::from(".");
    lock_name.push(
        path.file_name()
            .context("configuration file needs a name")?,
    );
    lock_name.push(".lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let lock = options
        .open(parent.join(lock_name))
        .context("opening configuration lock")?;
    lock.lock().context("locking configuration file")?;
    let mut document = read_document(&path)?;
    change(&mut document, &keys, value)?;
    let contents = document.to_string();
    crate::atomic::atomic_write_and_replace(&path, false, |temp| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        use std::io::Write;
        options.open(temp)?.write_all(contents.as_bytes())?;
        Ok(())
    })?;
    Ok(())
}

fn change(document: &mut toml_edit::DocumentMut, keys: &[&str], raw: Option<&str>) -> Result<()> {
    if raw.is_some() && keys[0] == "workspace" {
        bail!("edit workspace declarations directly in TOML");
    }
    let mut replacement = raw.map(|raw| {
        raw.parse::<toml_edit::Value>()
            .unwrap_or_else(|_| toml_edit::Value::from(raw))
    });
    if replacement
        .as_ref()
        .is_some_and(toml_edit::Value::is_inline_table)
    {
        bail!("set a scalar or array key; edit configuration tables in TOML");
    }
    let mut table: &mut dyn toml_edit::TableLike = document.as_table_mut();
    for part in &keys[..keys.len() - 1] {
        if !table.contains_key(part) {
            if raw.is_none() {
                bail!("{} is not set", keys.join("."));
            }
            table.insert(part, toml_edit::Item::Table(toml_edit::Table::new()));
        }
        table = table
            .get_mut(part)
            .and_then(toml_edit::Item::as_table_like_mut)
            .context("a parent key is not a configuration table")?;
    }
    let key = keys[keys.len() - 1];
    if let Some(mut value) = replacement.take() {
        if let Some(old) = table.get(key).and_then(toml_edit::Item::as_value) {
            *value.decor_mut() = old.decor().clone();
        }
        table.insert(key, toml_edit::Item::Value(value));
        let typed: crate::config::FileConfig =
            toml::from_str(&document.to_string()).context("invalid configuration value")?;
        let known = toml::Value::try_from(typed)?;
        if lookup(&known, keys).is_none() {
            bail!("unknown configuration key {}", keys.join("."));
        }
    } else if table.remove(key).is_none() {
        bail!("{} is not set", keys.join("."));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edits_preserve_comments_unknown_keys_and_value_types() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.toml");
        fs::write(
            &path,
            "# Personal settings\n[cache]\nlocal_only = false # Keep offline\nfuture_key = 9\n[workspace]\nextra_inputs = []\n",
        )
        .unwrap();
        edit(&path, "cache.local_only", Some("true")).unwrap();
        edit(&path, "cache.local_max_size", Some("50GiB")).unwrap();
        edit(&path, "paths.base_dirs", Some("['/a', '/b']")).unwrap();
        let text = fs::read_to_string(&path).unwrap();
        assert!(text.contains("# Personal settings"));
        assert!(text.contains("true # Keep offline"));
        assert!(text.contains("future_key = 9"));
        let values: toml::Value = toml::from_str(&text).unwrap();
        assert_eq!(values["cache"]["local_only"].as_bool(), Some(true));
        assert_eq!(values["cache"]["local_max_size"].as_str(), Some("50GiB"));
        assert_eq!(values["paths"]["base_dirs"][1].as_str(), Some("/b"));
        edit(&path, "cache.future_key", None).unwrap();
        assert!(!fs::read_to_string(&path).unwrap().contains("future_key"));
        edit(&path, "workspace.extra_inputs", None).unwrap();
        assert!(!fs::read_to_string(&path).unwrap().contains("extra_inputs"));
    }

    #[test]
    fn rejected_edits_leave_the_file_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.toml");
        let original = "[cache]\nlocal_only = true\n";
        fs::write(&path, original).unwrap();
        for (key, value) in [
            ("cache.typo", Some("true")),
            ("workspace.typo", Some("true")),
            ("cache.local_only", Some("not-a-boolean")),
            ("cache.remote.typo", Some("1")),
            ("cache", Some("{local_only = false}")),
            ("cache.local_only.child", Some("false")),
            ("cache..local_only", Some("false")),
            ("", Some("false")),
            ("cache.local_only\n", Some("false")),
            ("cache.absent", None),
            ("missing.value", None),
        ] {
            assert!(edit(&path, key, value).is_err(), "{key}");
            assert_eq!(fs::read_to_string(&path).unwrap(), original, "{key}");
        }
        fs::write(&path, "not valid TOML").unwrap();
        assert!(edit(&path, "cache.local_only", Some("true")).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap(), "not valid TOML");
    }

    #[test]
    fn set_creates_a_config_and_unset_restores_absence() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("nested/config.toml");
        assert!(edit(&path, "cache.local_only", None).is_err());
        assert!(!path.exists());
        edit(&path, "cache.local_only", Some("true")).unwrap();
        edit(&path, "cache.local_only", None).unwrap();
        let values: toml::Value = toml::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert!(lookup(&values, &["cache", "local_only"]).is_none());
    }

    #[test]
    fn concurrent_edits_keep_both_changes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("config.toml");
        for _ in 0..8 {
            fs::write(&path, "[cache]\nlocal_only = false\nexplain_miss = false\n").unwrap();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                for key in ["cache.local_only", "cache.explain_miss"] {
                    let barrier = &barrier;
                    let path = &path;
                    scope.spawn(move || {
                        barrier.wait();
                        edit(path, key, Some("true")).unwrap();
                    });
                }
            });
            let values: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
            assert_eq!(values["cache"]["local_only"].as_bool(), Some(true));
            assert_eq!(values["cache"]["explain_miss"].as_bool(), Some(true));
        }
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_and_private_permissions_survive_an_edit() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("dotfiles.toml");
        let link = root.path().join("config.toml");
        edit(&target, "cache.local_only", Some("false")).unwrap();
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(&target, fs::Permissions::from_mode(0o640)).unwrap();
        symlink(&target, &link).unwrap();
        edit(&link, "cache.local_only", Some("true")).unwrap();
        assert!(fs::symlink_metadata(&link).unwrap().is_symlink());
        assert!(fs::read_to_string(&target).unwrap().contains("true"));
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o640
        );
        fs::remove_file(target).unwrap();
        assert!(edit(&link, "cache.local_only", Some("false")).is_err());
    }
}
