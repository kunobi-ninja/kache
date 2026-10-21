//! Inputs owned by one compiler invocation.
//!
//! Capture at the process boundary. Code serving another client reads this
//! snapshot and configures compiler children from it, without changing the
//! server's environment or working directory.
use std::env::VarError;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Clone, Default)]
pub struct InvocationEnv {
    vars: std::sync::Arc<Vec<(OsString, OsString)>>,
    cwd: Option<PathBuf>,
    home: Option<PathBuf>,
    temp: PathBuf,
    cache: Option<PathBuf>,
}

impl std::fmt::Debug for InvocationEnv {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InvocationEnv")
            .field("variable_count", &self.vars.len())
            .field("cwd", &self.cwd)
            .finish_non_exhaustive()
    }
}

impl InvocationEnv {
    pub fn capture() -> Self {
        Self::from_parts(
            std::env::vars_os(),
            std::env::current_dir().ok(),
            dirs::home_dir(),
            std::env::temp_dir(),
        )
        .with_cache_dir(dirs::cache_dir())
    }

    pub fn with_cache_dir(mut self, cache: Option<PathBuf>) -> Self {
        self.cache = cache;
        self
    }

    pub fn from_parts(
        vars: impl IntoIterator<Item = (impl Into<OsString>, impl Into<OsString>)>,
        cwd: Option<PathBuf>,
        home: Option<PathBuf>,
        temp: PathBuf,
    ) -> Self {
        let mut invocation = Self {
            vars: std::sync::Arc::new(
                vars.into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
            ),
            cwd,
            home,
            temp,
            cache: None,
        };
        if let Some(temp) = invocation.resolve(&invocation.temp) {
            invocation.temp = temp;
        }
        #[cfg(target_os = "macos")]
        {
            invocation.cache = invocation
                .home_dir()
                .map(|home| home.join("Library/Caches"));
        }
        #[cfg(windows)]
        {
            invocation.cache = invocation
                .var_os("LOCALAPPDATA")
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
                .or_else(|| invocation.home_dir().map(|home| home.join("AppData/Local")));
        }
        #[cfg(not(any(target_os = "macos", windows)))]
        {
            invocation.cache = invocation
                .var_os("XDG_CACHE_HOME")
                .map(PathBuf::from)
                .filter(|path| path.is_absolute())
                .or_else(|| invocation.home_dir().map(|home| home.join(".cache")));
        }
        invocation
    }

    pub fn validate(&self) -> std::io::Result<()> {
        if self.cwd().is_none_or(|cwd| !cwd.is_absolute()) || !self.temp.is_absolute() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "compiler invocation requires absolute working and temporary directories",
            ));
        }
        Ok(())
    }

    pub fn var_os(&self, name: &str) -> Option<OsString> {
        self.vars
            .iter()
            .rev()
            .find(|(key, _)| name_matches(key, name))
            .map(|(_, value)| value.clone())
    }

    pub fn var(&self, name: &str) -> Result<String, VarError> {
        self.var_os(name)
            .ok_or(VarError::NotPresent)?
            .into_string()
            .map_err(VarError::NotUnicode)
    }

    pub fn vars_os(&self) -> impl Iterator<Item = (OsString, OsString)> + '_ {
        self.vars.iter().cloned()
    }

    pub fn cwd(&self) -> Option<&Path> {
        self.cwd.as_deref()
    }

    pub fn home_dir(&self) -> Option<&Path> {
        self.home.as_deref()
    }
    pub fn temp_dir(&self) -> &Path {
        &self.temp
    }

    /// Native user cache location, including redirected Windows Known Folders.
    pub fn cache_dir(&self) -> Option<&Path> {
        self.cache.as_deref()
    }

    pub fn resolve(&self, path: &Path) -> Option<PathBuf> {
        if path.is_absolute() {
            Some(path.to_owned())
        } else {
            let cwd = self.cwd().filter(|cwd| cwd.is_absolute())?;
            #[cfg(windows)]
            if let Some(std::path::Component::Prefix(prefix)) = path.components().next() {
                return self.resolve_drive_relative(path, cwd, prefix);
            }
            let resolved = cwd.join(path);
            resolved.is_absolute().then_some(resolved)
        }
    }

    #[cfg(windows)]
    fn resolve_drive_relative(
        &self,
        path: &Path,
        cwd: &Path,
        prefix: std::path::PrefixComponent<'_>,
    ) -> Option<PathBuf> {
        // A drive-relative path needs that drive's client directory.
        // Never let a server's per-drive state resolve it implicitly.
        use std::path::{Component, Prefix};
        let drive = match prefix.kind() {
            Prefix::Disk(drive) => drive,
            _ => return None,
        };
        let same_drive = matches!(cwd.components().next(),
            Some(Component::Prefix(cwd_prefix))
            if matches!(cwd_prefix.kind(), Prefix::Disk(cwd_drive) | Prefix::VerbatimDisk(cwd_drive)
                if cwd_drive.eq_ignore_ascii_case(&drive)));
        let base = if same_drive {
            cwd.to_owned()
        } else {
            let base = PathBuf::from(self.var_os(&format!("={}:", char::from(drive)))?);
            if !base.is_absolute() {
                return None;
            }
            match base.components().next()? {
                Component::Prefix(base_prefix)
                    if matches!(base_prefix.kind(), Prefix::Disk(base_drive)
                        if base_drive.eq_ignore_ascii_case(&drive)) =>
                {
                    base
                }
                _ => return None,
            }
        };
        Some(base.join(path.strip_prefix(prefix.as_os_str()).ok()?))
    }

    /// Apply an invocation-local rewrite, such as an OUT_DIR alias.
    pub fn set(&mut self, name: &str, value: impl Into<OsString>) {
        self.set_os(OsStr::new(name), value.into());
    }

    pub fn set_os(&mut self, name: &OsStr, value: OsString) {
        let vars = std::sync::Arc::make_mut(&mut self.vars);
        vars.retain(|(key, _)| !name_matches_os(key, name));
        vars.push((name.to_owned(), value));
    }

    /// Search the client's PATH after rooting relative entries in its cwd.
    pub fn resolve_program(&self, program: &Path) -> Option<PathBuf> {
        if program.is_absolute() || program.components().count() > 1 {
            return self.resolve(program);
        }
        let paths = self.var_os("PATH")?;
        let rooted = std::env::split_paths(&paths).filter_map(|path| self.resolve(&path));
        let path = std::env::join_paths(rooted).ok()?;
        kache_shims::resolve_program_on_path_with(
            program.to_str()?,
            Some(&path),
            self.var_os("PATHEXT").as_deref(),
        )
    }

    pub fn command(&self, program: impl AsRef<OsStr>) -> std::io::Result<Command> {
        let program_path = Path::new(program.as_ref());
        let resolved = self.resolve_program(program_path);
        if resolved.is_none() && (cfg!(windows) || program_path.components().count() > 1) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "compiler program cannot be resolved in the invocation context",
            ));
        }
        let mut command = Command::new(
            resolved
                .as_deref()
                .map(Path::as_os_str)
                .unwrap_or(program.as_ref()),
        );
        command
            .env_clear()
            .envs(self.vars_os())
            .current_dir(self.cwd().ok_or_else(|| {
                std::io::Error::other("compiler invocation has no working directory")
            })?);
        Ok(command)
    }
}

fn name_matches(key: &OsStr, name: &str) -> bool {
    name_matches_os(key, OsStr::new(name))
}

fn name_matches_os(key: &OsStr, name: &OsStr) -> bool {
    if cfg!(windows) {
        key.to_str()
            .zip(name.to_str())
            .is_some_and(|(key, name)| key.eq_ignore_ascii_case(name))
    } else {
        key == name
    }
}

/// Identify Cargo manifest-directory keys before normalizing environment values.
pub fn is_manifest_dir_var(var: &str) -> bool {
    var.eq_ignore_ascii_case("CARGO_MANIFEST_DIR")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_paths_are_rooted_in_the_client_directory() {
        let root = tempfile::tempdir().unwrap();
        let env = InvocationEnv::from_parts(
            Vec::<(OsString, OsString)>::new(),
            Some(root.path().to_owned()),
            None,
            "client-temp".into(),
        );
        assert_eq!(env.temp_dir(), root.path().join("client-temp"));
        env.validate().unwrap();
        let missing = InvocationEnv::from_parts(
            Vec::<(OsString, OsString)>::new(),
            None,
            None,
            "client-temp".into(),
        );
        assert_eq!(
            missing.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn separate_invocations_keep_environment_and_paths_separate() {
        let root = tempfile::tempdir().unwrap();
        let one = root.path().join("one");
        let two = root.path().join("two");
        let mut first = InvocationEnv::from_parts(
            [("OUT_DIR", "/one/out"), ("EMPTY", "")],
            Some(one.clone()),
            None,
            "/tmp".into(),
        );
        let second = InvocationEnv::from_parts(
            [("OUT_DIR", "/two/out")],
            Some(two.clone()),
            None,
            "/tmp".into(),
        );
        first.set("OUT_DIR", "/one/alias");
        assert_eq!(first.var("OUT_DIR").unwrap(), "/one/alias");
        assert_eq!(second.var("OUT_DIR").unwrap(), "/two/out");
        assert_eq!(first.var("EMPTY").unwrap(), "");
        assert_eq!(second.var("EMPTY"), Err(VarError::NotPresent));
        assert_eq!(
            first.resolve(Path::new("src/lib.rs")),
            Some(one.join("src/lib.rs"))
        );
        assert_eq!(
            second.resolve(Path::new("src/lib.rs")),
            Some(two.join("src/lib.rs"))
        );
    }

    #[test]
    fn debug_output_excludes_environment_values() {
        let env = InvocationEnv::from_parts(
            [("PRIVATE_TOKEN", "a-secret-value")],
            None,
            None,
            PathBuf::new(),
        );
        let debug = format!("{env:?}");
        assert!(debug.starts_with("InvocationEnv"));
        assert!(debug.contains("variable_count: 1"));
        assert!(!debug.contains("PRIVATE_TOKEN"));
        assert!(!debug.contains("a-secret-value"));
    }

    #[test]
    fn validation_rejects_an_invalid_directory_even_when_the_other_is_absolute() {
        let root = tempfile::tempdir().unwrap();
        let mut env = InvocationEnv::from_parts(
            Vec::<(OsString, OsString)>::new(),
            Some(root.path().to_owned()),
            None,
            root.path().to_owned(),
        );
        env.validate().unwrap();
        env.temp = "relative-temp".into();
        assert_eq!(
            env.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        env.temp = root.path().to_owned();
        env.cwd = Some("relative-cwd".into());
        assert_eq!(
            env.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn explicit_program_paths_and_missing_search_paths_keep_their_native_meaning() {
        let root = tempfile::tempdir().unwrap();
        let env = InvocationEnv::from_parts(
            Vec::<(OsString, OsString)>::new(),
            Some(root.path().to_owned()),
            None,
            root.path().to_owned(),
        );
        assert_eq!(
            env.resolve_program(Path::new("relative/compiler")),
            Some(root.path().join("relative/compiler"))
        );
        assert_eq!(
            env.resolve_program(root.path()),
            Some(root.path().to_owned())
        );
        assert_eq!(env.resolve_program(Path::new("missing-compiler")), None);
        #[cfg(unix)]
        {
            assert_eq!(
                env.resolve_program(Path::new("/")),
                Some(PathBuf::from("/"))
            );
            assert_eq!(
                env.command("missing-compiler").unwrap().get_program(),
                "missing-compiler"
            );
        }
        #[cfg(windows)]
        assert_eq!(
            env.command("missing-compiler").unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        let mut missing_cwd = env;
        missing_cwd.cwd = None;
        assert_eq!(
            missing_cwd.command("relative/compiler").unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_relative_paths_use_only_the_clients_drive_directories() {
        let env = InvocationEnv::from_parts(
            [("=D:", r"D:\other-client")],
            Some(PathBuf::from(r"C:\client")),
            None,
            PathBuf::from(r"C:\temp"),
        );
        assert_eq!(
            env.resolve(Path::new(r"C:out")),
            Some(PathBuf::from(r"C:\client\out"))
        );
        assert_eq!(
            env.resolve(Path::new(r"D:out")),
            Some(PathBuf::from(r"D:\other-client\out"))
        );
        assert_eq!(
            env.resolve(Path::new(r"\out")),
            Some(PathBuf::from(r"C:\out"))
        );
        assert_eq!(env.resolve(Path::new(r"E:out")), None);
        assert!(env.command(r"E:compiler.exe").is_err());
    }

    #[cfg(windows)]
    #[test]
    fn windows_drive_directories_must_be_absolute_and_match_the_requested_drive() {
        let mut env = InvocationEnv::from_parts(
            [("=D:", r"D:\other-client")],
            Some(PathBuf::from(r"C:\client")),
            None,
            PathBuf::from(r"C:\temp"),
        );
        for invalid in [
            r"relative",
            r"D:relative",
            r"E:\wrong-drive",
            r"\\server\share",
        ] {
            env.set("=D:", invalid);
            assert_eq!(env.resolve(Path::new(r"D:out")), None, "{invalid}");
        }
        env.set("=D:", r"d:\lowercase");
        assert_eq!(
            env.resolve(Path::new(r"D:out")),
            Some(PathBuf::from(r"d:\lowercase\out"))
        );
        env.cwd = Some(PathBuf::from(r"\\?\C:\client"));
        assert_eq!(
            env.resolve(Path::new(r"c:out")),
            Some(PathBuf::from(r"\\?\C:\client\out"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn children_receive_only_their_invocations_environment_and_directory() {
        let root = tempfile::tempdir().unwrap();
        let left = root.path().join("left");
        let right = root.path().join("right");
        std::fs::create_dir_all(&left).unwrap();
        std::fs::create_dir_all(&right).unwrap();
        let spawn = |cwd: &Path, token: &str| {
            InvocationEnv::from_parts(
                [("INVOCATION_TOKEN", token)],
                Some(cwd.to_owned()),
                None,
                "/tmp".into(),
            )
            .command("/bin/sh")
            .unwrap()
            .args([
                "-c",
                "printf '%s\\n%s\\n' \"$INVOCATION_TOKEN\" \"$PWD\"; test -z \"${HOME+x}\"",
            ])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap()
        };
        // Spawn both before waiting: a process-wide chdir or environment
        // rewrite cannot provide these independent command configurations.
        let first = spawn(&left, "left");
        let second = spawn(&right, "right");
        for (child, token, cwd) in [(first, "left", &left), (second, "right", &right)] {
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("{token}\n{}\n", cwd.canonicalize().unwrap().display())
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_environment_values_remain_bytes() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let value = OsString::from_vec(vec![0xff, b'x']);
        let env = InvocationEnv::from_parts(
            [(OsString::from("VALUE"), value.clone())],
            Some("/work".into()),
            None,
            "/tmp".into(),
        );
        assert_eq!(env.var_os("VALUE").unwrap().as_bytes(), &[0xff, b'x']);
        assert_eq!(env.var("VALUE"), Err(VarError::NotUnicode(value)));
    }
}
