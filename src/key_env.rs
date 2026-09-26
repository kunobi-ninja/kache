//! The environment variables and working directory a rustc cache key reads.
//!
//! [`crate::cache_key::compute_cache_key`] reads them from a [`KeyEnv`], never
//! from the process. The wrapper captures one per invocation; a caller that
//! computes keys for another process builds one from that process's parts.

use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

/// Every variable key computation reads by name. A read of a name missing
/// here fails in debug builds (see [`KeyEnv::var_os`]), so adding a read means
/// adding the name.
pub(crate) const KEY_ENV_VARS: &[&str] = &[
    "CARGO_ENCODED_RUSTFLAGS",
    "CARGO_MANIFEST_DIR",
    "CARGO_PKG_NAME",
    "CARGO_PKG_VERSION",
    "CLIPPY_ARGS",
    "CLIPPY_CONF_DIR",
    "CLIPPY_DISABLE_DOCS_LINKS",
    "KACHE_VERIFY_INPUT_PREDICTIONS",
    "LIBRARY_PATH",
    "MACOSX_DEPLOYMENT_TARGET",
    "OUT_DIR",
    "RUSTC_BOOTSTRAP",
    "RUSTFLAGS",
    "SDKROOT",
];

/// The one family the key reads by prefix: Cargo's `CARGO_CFG_*` variables.
const CARGO_CFG_PREFIX: &str = "CARGO_CFG_";

/// A snapshot of the variables in [`KEY_ENV_VARS`], the `CARGO_CFG_*` family,
/// and the working directory.
#[derive(Debug, Default)]
pub(crate) struct KeyEnv {
    vars: Vec<(&'static str, OsString)>,
    cargo_cfgs: Vec<(OsString, OsString)>,
    cwd: Option<PathBuf>,
}

impl KeyEnv {
    /// Read the snapshot from this process.
    pub(crate) fn capture() -> Self {
        Self::from_parts(std::env::vars_os(), std::env::current_dir().ok())
    }

    /// Build the snapshot from another process's environment and working
    /// directory. Variables the key does not read are dropped.
    pub(crate) fn from_parts<N, V>(
        vars: impl IntoIterator<Item = (N, V)>,
        cwd: Option<PathBuf>,
    ) -> Self
    where
        N: Into<OsString>,
        V: Into<OsString>,
    {
        let mut env = Self {
            cwd,
            ..Self::default()
        };
        for (name, value) in vars {
            let name = name.into();
            if let Some(declared) = declared_name(&name) {
                env.vars.push((declared, value.into()));
            } else if name.to_string_lossy().starts_with(CARGO_CFG_PREFIX) {
                env.cargo_cfgs.push((name, value.into()));
            }
        }
        env
    }

    /// The value of `name`, which must be listed in [`KEY_ENV_VARS`].
    pub(crate) fn var_os(&self, name: &str) -> Option<OsString> {
        debug_assert!(
            KEY_ENV_VARS.contains(&name),
            "{name} is read by key computation but missing from KEY_ENV_VARS"
        );
        self.vars
            .iter()
            .find(|(declared, _)| *declared == name)
            .map(|(_, value)| value.clone())
    }

    /// [`Self::var_os`] as UTF-8, `None` when unset or not Unicode, like
    /// `std::env::var(name).ok()`.
    pub(crate) fn var(&self, name: &str) -> Option<String> {
        self.var_os(name)?.into_string().ok()
    }

    /// Every `CARGO_CFG_*` variable, in no particular order.
    pub(crate) fn cargo_cfgs(&self) -> impl Iterator<Item = (OsString, OsString)> + '_ {
        self.cargo_cfgs.iter().cloned()
    }

    /// The working directory, `None` when it could not be read.
    pub(crate) fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }
}

/// The [`KEY_ENV_VARS`] entry `name` refers to. Windows variable names are
/// case-insensitive, as `std::env::var_os` treats them there.
fn declared_name(name: &OsStr) -> Option<&'static str> {
    let name = name.to_str()?;
    KEY_ENV_VARS.iter().copied().find(|declared| {
        if cfg!(windows) {
            declared.eq_ignore_ascii_case(name)
        } else {
            *declared == name
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_declared_variables_and_the_cargo_cfg_family() {
        let env = KeyEnv::from_parts(
            [
                ("OUT_DIR", "/t/out"),
                ("PATH", "/usr/bin"),
                ("CARGO_CFG_UNIX", ""),
                ("CARGO_PKG_AUTHORS", "someone"),
            ],
            Some(PathBuf::from("/work")),
        );
        assert_eq!(env.var_os("OUT_DIR"), Some(OsString::from("/t/out")));
        assert_eq!(env.var("OUT_DIR").as_deref(), Some("/t/out"));
        assert_eq!(env.var_os("RUSTFLAGS"), None);
        assert_eq!(
            env.cargo_cfgs().collect::<Vec<_>>(),
            vec![(OsString::from("CARGO_CFG_UNIX"), OsString::new())]
        );
        assert_eq!(env.vars.len(), 1, "PATH and CARGO_PKG_AUTHORS are dropped");
        assert_eq!(env.cwd(), Some(Path::new("/work")));
    }

    #[test]
    fn a_variable_reads_its_own_value() {
        let env = KeyEnv::from_parts([("RUSTFLAGS", "-a"), ("SDKROOT", "/sdk")], None);
        assert_eq!(env.var("RUSTFLAGS").as_deref(), Some("-a"));
        assert_eq!(env.var("SDKROOT").as_deref(), Some("/sdk"));
        assert_eq!(env.cwd(), None);
    }

    #[cfg(unix)]
    #[test]
    fn a_non_unicode_value_is_unset_as_text() {
        use std::os::unix::ffi::OsStringExt;
        let raw = OsString::from_vec(vec![b'a', 0xff]);
        let env = KeyEnv::from_parts([(OsString::from("RUSTFLAGS"), raw.clone())], None);
        assert_eq!(env.var_os("RUSTFLAGS"), Some(raw));
        assert_eq!(env.var("RUSTFLAGS"), None);
    }

    #[cfg(windows)]
    #[test]
    fn windows_names_match_without_case() {
        let env = KeyEnv::from_parts([("RustFlags", "-a")], None);
        assert_eq!(env.var("RUSTFLAGS").as_deref(), Some("-a"));
    }

    #[cfg(not(windows))]
    #[test]
    fn unix_names_match_with_case() {
        let env = KeyEnv::from_parts([("RustFlags", "-a")], None);
        assert_eq!(env.var("RUSTFLAGS"), None);
    }

    #[test]
    fn capture_reads_this_process() {
        let _lock = crate::test_support::process_state_test_lock();
        let _declared = crate::config::tests::set_env_for_test(
            "KACHE_VERIFY_INPUT_PREDICTIONS",
            Some(OsStr::new("captured")),
        );
        let _cfg = crate::config::tests::set_env_for_test(
            "CARGO_CFG_KACHE_CAPTURE_TEST",
            Some(OsStr::new("1")),
        );
        let env = KeyEnv::capture();
        assert_eq!(
            env.var("KACHE_VERIFY_INPUT_PREDICTIONS").as_deref(),
            Some("captured")
        );
        assert!(
            env.cargo_cfgs()
                .any(|(name, value)| name == "CARGO_CFG_KACHE_CAPTURE_TEST" && value == "1")
        );
        assert_eq!(env.cwd(), std::env::current_dir().ok().as_deref());
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "missing from KEY_ENV_VARS")]
    fn an_undeclared_read_fails_in_debug_builds() {
        KeyEnv::default().var_os("PATH");
    }
}
