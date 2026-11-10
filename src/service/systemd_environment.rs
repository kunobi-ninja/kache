//! Resolve only a verified Kache user unit. No manager writes occur.
//!
//! The instance a unit's daemon serves follows from its environment and
//! working directory. A unit may carry other settings, in its file or in
//! drop-ins (`UMask=`, `Nice=`, `TimeoutStopSec=`), and a drop-in may wrap
//! `kache daemon run` (`setpriv`). Such a unit is still the login service:
//! declining it would start a second daemon outside the manager. A wrapper is
//! trusted to keep the manager's environment; set variables with
//! `Environment=`, which is verified.

use std::collections::BTreeMap;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;

pub(super) fn socket(unit: &Path, deadline: Instant) -> Option<PathBuf> {
    let executable = super::parse_exe_from_service_file(unit)?;
    let properties = query(
        &[
            "--user",
            "--no-pager",
            "show",
            super::UNIT_NAME,
            "--all",
            "--property=FragmentPath,NeedDaemonReload,Environment,EnvironmentFiles,UnsetEnvironment,WorkingDirectory,ExecStart",
        ],
        deadline,
    )?;
    let environment = query(&["--user", "--no-pager", "show-environment"], deadline)?;
    let (variables, home) = manager_environment(&environment)?;
    if !verified_loaded_unit(unit, &executable, &properties, &home) {
        return None;
    }
    socket_for(variables, home)
}

// Anonymous files avoid pipe backpressure and never persist an environment dump.
// Readback is bounded. Oversized producers are stopped at the next wait poll;
// neither their output nor their environment is logged or returned as an error.
fn query(arguments: &[&str], deadline: Instant) -> Option<String> {
    if Instant::now() >= deadline {
        return None;
    }
    let mut output = tempfile::tempfile().ok()?;
    let child = Command::new("systemctl")
        .args(arguments)
        .stdin(Stdio::null())
        .stdout(output.try_clone().ok()?)
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let mut child = kunobi_daemon::Candidate::new(child);
    let status = kunobi_daemon::readiness::wait_until(deadline, |_| {
        if output_exceeds_limit(output.metadata()?.len()) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "service query output exceeds limit",
            ));
        }
        child.try_wait()
    })
    .ok()??;
    if !status.success() {
        return None;
    }
    output.rewind().ok()?;
    let mut bytes = Vec::new();
    output.take(65_537).read_to_end(&mut bytes).ok()?;
    if output_exceeds_limit(bytes.len() as u64) {
        return None;
    }
    String::from_utf8(bytes).ok()
}

fn output_exceeds_limit(byte_count: u64) -> bool {
    byte_count > 65_536
}

// `home` is the manager's HOME: a user manager runs a unit without its own
// WorkingDirectory= in that home and reports it as `!<home>`.
fn verified_loaded_unit(unit: &Path, executable: &Path, text: &str, home: &Path) -> bool {
    let Some(properties) = assignments(text) else {
        return false;
    };
    let empty = ["EnvironmentFiles", "UnsetEnvironment"];
    if !executable.is_absolute()
        || !empty
            .iter()
            .all(|key| properties.get(*key).is_some_and(String::is_empty))
        || !properties
            .get("WorkingDirectory")
            .is_some_and(|v| v.is_empty() || v.strip_prefix('!').map(Path::new) == Some(home))
        || properties.get("NeedDaemonReload").map(String::as_str) != Some("no")
        || !properties
            .get("Environment")
            .is_some_and(|v| v.is_empty() || v == "KACHE_LOG=kache=info")
    {
        return false;
    }
    let Some(fragment) = properties.get("FragmentPath") else {
        return false;
    };
    if super::canonical_or_original(Path::new(fragment)) != super::canonical_or_original(unit) {
        return false;
    }
    let Some(start) = properties.get("ExecStart") else {
        return false;
    };
    // `{ path=<program> ; argv[]=<words> ; ... }`: the words end in Kache's
    // own command, alone or after a wrapper.
    let command = format!("{} daemon run", executable.display());
    let Some(argv) = start
        .strip_prefix("{ path=")
        .and_then(|rest| rest.split_once(" ; argv[]="))
        .and_then(|(_, rest)| rest.split_once(" ; "))
        .map(|(argv, _)| argv)
    else {
        return false;
    };
    (argv == command || argv.ends_with(&format!(" {command}")))
        && start.ends_with(" }")
        && start.matches("argv[]=").count() == 1
}

fn assignments(text: &str) -> Option<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for line in text.lines() {
        let (key, value) = line.split_once('=')?;
        if result.insert(key.to_owned(), value.to_owned()).is_some() {
            return None;
        }
    }
    Some(result)
}

#[cfg(test)]
fn configured_socket(text: &str) -> Option<PathBuf> {
    let (variables, home) = manager_environment(text)?;
    socket_for(variables, home)
}

fn manager_environment(text: &str) -> Option<(BTreeMap<String, String>, PathBuf)> {
    let mut variables = BTreeMap::new();
    for line in text.lines() {
        let word = shell_word(line)?;
        let (key, value) = word.split_once('=')?;
        if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return None;
        }
        if variables.insert(key.to_owned(), value.to_owned()).is_some() {
            return None;
        }
    }
    let home = PathBuf::from(variables.get("HOME")?);
    if !home.is_absolute() {
        return None;
    }
    Some((variables, home))
}

fn socket_for(variables: BTreeMap<String, String>, home: PathBuf) -> Option<PathBuf> {
    let environment = kache_config::InvocationEnv::from_parts(
        variables,
        Some(home.clone()),
        Some(home),
        PathBuf::from("/tmp"),
    );
    crate::config::Config::load_for(&environment)
        .ok()
        .map(|config| config.socket_path())
}

// Decode one shell-quoted assignment without evaluating substitutions. Unknown
// shell syntax makes ownership unproven instead of selecting a guessed path.
fn shell_word(text: &str) -> Option<String> {
    let mut result = String::new();
    let mut quote = None;
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        match (quote, character) {
            (Some('\''), '\'') | (Some('"'), '"') => quote = None,
            (Some('\''), c) => result.push(c),
            (Some('"'), '\\') => {
                let escaped = characters.next()?;
                if !"$`\"\\".contains(escaped) {
                    result.push('\\');
                }
                result.push(escaped);
            }
            (None, '\\') => result.push(characters.next()?),
            (None, '\'' | '"') => quote = Some(character),
            (_, '$' | '`') => return None,
            (None, c) if c.is_whitespace() || ";&|<>()".contains(c) => return None,
            (_, c) => result.push(c),
        }
    }
    quote.is_none().then_some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_output_accepts_exactly_64_kib_and_refuses_more() {
        assert!(!output_exceeds_limit(0));
        assert!(!output_exceeds_limit(65_535));
        assert!(!output_exceeds_limit(65_536));
        assert!(output_exceeds_limit(65_537));
    }

    fn manager(home: &Path, extra: &str) -> String {
        format!("HOME={}\nKACHE_HOST_CONFIG=\n{extra}", home.display())
    }

    #[test]
    fn manager_xdg_cache_is_used_instead_of_the_builds_home_cache() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path();
        let cache = home.join("manager-cache");
        let text = manager(home, &format!("XDG_CACHE_HOME={}\n", cache.display()));
        let socket = configured_socket(&text).unwrap();
        assert_eq!(socket, cache.join("kache/daemon.sock"));
        assert_ne!(socket, home.join(".cache/kache/daemon.sock"));
        assert_eq!(
            configured_socket(&manager(home, "")).unwrap(),
            home.join(".cache/kache/daemon.sock")
        );
    }

    #[test]
    fn a_private_xdg_file_is_used_only_when_it_belongs_to_the_manager() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path();
        let xdg = home.join("manager-config");
        let file = xdg.join("kache/config.toml");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        let runtime = home.join("private-runtime");
        std::fs::write(&file, format!("[cache]\nruntime_dir = {:?}\n", runtime)).unwrap();
        let private = runtime.join("daemon.sock");
        assert_ne!(configured_socket(&manager(home, "")).unwrap(), private);
        let text = manager(home, &format!("XDG_CONFIG_HOME={}\n", xdg.display()));
        assert_eq!(configured_socket(&text).unwrap(), private);
    }

    #[test]
    fn the_manager_snapshot_supplies_config_overrides_and_home_expansion() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path();
        let file = home.join("chosen.toml");
        std::fs::write(
            &file,
            "[cache]\nruntime_dir = '~/manager-instance/runtime'\n",
        )
        .unwrap();
        let text = manager(home, "KACHE_CONFIG=~/chosen.toml\n");
        assert_eq!(
            configured_socket(&text).unwrap(),
            home.join("manager-instance/runtime/daemon.sock")
        );
    }

    #[test]
    fn unsupported_or_ambiguous_environment_is_not_guessed() {
        for text in [
            "",
            "HOME=relative\n",
            "HOME=/a\nHOME=/b\n",
            "HOME=$(command)\n",
            "HOME='unterminated\n",
        ] {
            assert!(configured_socket(text).is_none(), "{text}");
        }
        assert_eq!(shell_word("X='a b$literal'"), Some("X=a b$literal".into()));
        assert_eq!(shell_word(r#"X="a\\b\"c""#), Some("X=a\\b\"c".into()));
        assert_eq!(shell_word(r#"X="a\q""#), Some(r"X=a\q".into()));
        assert_eq!(shell_word(r"X=a\ b"), Some("X=a b".into()));
        for text in [
            "X=a b",
            "X=a;command",
            "X=`command`",
            "X=$'ansi'",
            "X=trailing\\",
        ] {
            assert!(shell_word(text).is_none(), "{text}");
        }
    }

    #[test]
    fn empty_and_invalid_environment_names_do_not_prove_ownership() {
        let root = tempfile::tempdir().unwrap();
        for invalid in ["=value\n", "BAD-NAME=value\n"] {
            assert!(configured_socket(&manager(root.path(), invalid)).is_none());
        }
    }

    fn properties(unit: &Path, exe: &Path) -> String {
        format!(
            "FragmentPath={}\nNeedDaemonReload=no\nEnvironment=KACHE_LOG=kache=info\nEnvironmentFiles=\nUnsetEnvironment=\nWorkingDirectory=\nExecStart={{ path={} ; argv[]={} daemon run ; ignore_errors=no ; }}\n",
            unit.display(),
            exe.display(),
            exe.display()
        )
    }

    #[test]
    fn only_a_matching_known_loaded_unit_proves_ownership() {
        let root = tempfile::tempdir().unwrap();
        let unit = root.path().join("kache.service");
        let exe = Path::new("/opt/kache");
        std::fs::write(&unit, "[Service]\nExecStart=/opt/kache daemon run\n").unwrap();
        let home = Path::new("/home/someone");
        let valid = properties(&unit, exe);
        assert!(verified_loaded_unit(&unit, exe, &valid, home));
        assert!(!verified_loaded_unit(
            &unit,
            Path::new("relative"),
            &valid,
            home
        ));
        for (old, new) in [
            // A user manager reports its default working directory, the home.
            ("WorkingDirectory=\n", "WorkingDirectory=!/home/someone\n"),
            // Drop-ins and a wrapper keep the manager's environment.
            (
                "{ path=/opt/kache ; argv[]=/opt/kache",
                "{ path=/usr/bin/setpriv ; argv[]=/usr/bin/setpriv --inh-caps=-all /opt/kache",
            ),
        ] {
            assert!(
                verified_loaded_unit(&unit, exe, &valid.replace(old, new), home),
                "{new}"
            );
        }
        for (old, new) in [
            ("NeedDaemonReload=no", "NeedDaemonReload=yes"),
            (
                "Environment=KACHE_LOG=kache=info",
                "Environment=KACHE_CONFIG=/other",
            ),
            ("EnvironmentFiles=\n", "EnvironmentFiles=/other\n"),
            ("UnsetEnvironment=\n", "UnsetEnvironment=HOME\n"),
            ("WorkingDirectory=\n", "WorkingDirectory=/other\n"),
            ("WorkingDirectory=\n", "WorkingDirectory=!/home/other\n"),
            ("argv[]=/opt/kache daemon run", "argv[]=/bin/sh -c command"),
            (
                "argv[]=/opt/kache daemon run",
                "argv[]=/opt/kache other run",
            ),
            (
                "argv[]=/opt/kache daemon run",
                "argv[]=/other/opt/kache daemon run",
            ),
        ] {
            assert!(
                !verified_loaded_unit(&unit, exe, &valid.replace(old, new), home),
                "{new}"
            );
        }
        assert!(!verified_loaded_unit(
            &root.path().join("other"),
            exe,
            &valid,
            home
        ));
        assert!(!verified_loaded_unit(&unit, exe, "", home));
        assert!(assignments("X=a\nX=b\n").is_none());
    }
}
