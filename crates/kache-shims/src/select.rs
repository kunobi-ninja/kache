//! Which path to record for the running binary.
//!
//! Shims and service files outlive the binary that wrote them. Recording the
//! resolved path of a versioned install (a Homebrew keg, a Nix store path, a
//! mise or asdf version directory) breaks them when that version is removed.
//! [`select`] looks for a path that reaches the same file today and that the
//! installer keeps pointing at the current version.
//!
//! Rules, first match wins:
//!
//! 1. An installer alias: Homebrew's `opt/<formula>` link, a Nix profile, or
//!    mise's `installs/<tool>/latest`.
//! 2. The first entry on `PATH` that reaches the running binary, skipping
//!    relative entries, versioned install directories, mise and asdf shim
//!    dispatchers, and directories marked as compiler-shim farms.
//! 3. The running binary's own resolved path.
//!
//! "Reaches the running binary" compares device and inode, so a hardlink or a
//! bind mount of the same file counts. It is a check of the present: a later
//! upgrade decides what the recorded path reaches then.

use crate::fs::{Fs, RealFs};
use kache_fs::InodeId;
use std::fmt;
use std::path::{Component, Path, PathBuf};

/// The installer that placed the running binary, judged from its resolved
/// path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Homebrew,
    Nix,
    Mise,
    Asdf,
    /// No installer layout recognized: `cargo install`, a distro package or a
    /// copied binary.
    Other,
}

impl Kind {
    pub fn label(self) -> &'static str {
        match self {
            Kind::Homebrew => "Homebrew",
            Kind::Nix => "Nix",
            Kind::Mise => "mise",
            Kind::Asdf => "asdf",
            Kind::Other => "standalone",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Whether the selected path is expected to keep reaching kache after an
/// upgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stability {
    /// An alias the installer repoints at the new version on every upgrade.
    InstallerManaged,
    /// A `PATH` entry outside any versioned directory. It keeps working until
    /// someone replaces or removes that file.
    UserManaged,
    /// The binary's own path inside a version directory. Removing that
    /// version breaks whatever recorded it.
    Versioned,
    /// Running as another user (sudo), so `HOME` and `PATH` may not be the
    /// ones the recorded path is used with.
    Unverified,
}

impl Stability {
    pub fn label(self) -> &'static str {
        match self {
            Stability::InstallerManaged => "installer-managed",
            Stability::UserManaged => "user-managed",
            Stability::Versioned => "versioned",
            Stability::Unverified => "unverified",
        }
    }

    /// Whether an upgrade is expected to leave the path working.
    pub fn survives_upgrade(self) -> bool {
        matches!(self, Stability::InstallerManaged | Stability::UserManaged)
    }
}

impl fmt::Display for Stability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// The path to record, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub path: PathBuf,
    pub kind: Kind,
    pub stability: Stability,
    /// One sentence a person can read in `kache doctor`.
    pub reason: String,
}

/// Why no path could be selected.
#[derive(Debug)]
pub enum Error {
    /// The OS would not say where the running binary is.
    CurrentExe(std::io::Error),
    /// The running binary was replaced on disk while it ran. Linux reports
    /// its old path with a ` (deleted)` suffix.
    Replaced(PathBuf),
    /// The running binary's path reaches no file.
    Unreachable(PathBuf),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::CurrentExe(error) => write!(f, "cannot locate the running binary: {error}"),
            Error::Replaced(path) => write!(
                f,
                "{} was replaced while this process ran; run the new binary instead",
                path.display()
            ),
            Error::Unreachable(path) => write!(f, "{} no longer exists", path.display()),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::CurrentExe(error) => Some(error),
            _ => None,
        }
    }
}

/// Running with someone else's identity, so `HOME` and `PATH` may describe
/// another account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Elevation {
    /// Root through sudo, invoked by `user`.
    Sudo { user: String },
    /// `HOME` is owned by a different user.
    ForeignHome { home: PathBuf },
}

/// Everything selection reads from the process, passed in so each rule can
/// be tested with fake values.
#[derive(Debug, Clone, Default)]
pub struct Env {
    /// The running binary as the OS reports it, unresolved.
    pub exe: PathBuf,
    /// `PATH`, split.
    pub path: Vec<PathBuf>,
    pub home: Option<PathBuf>,
    pub user: Option<String>,
    pub xdg_state_home: Option<PathBuf>,
    pub xdg_data_home: Option<PathBuf>,
    pub mise_data_dir: Option<PathBuf>,
    pub asdf_data_dir: Option<PathBuf>,
    pub elevation: Option<Elevation>,
}

impl Env {
    /// Read the running process.
    pub fn from_process() -> std::io::Result<Self> {
        let var = |name: &str| std::env::var_os(name).filter(|value| !value.is_empty());
        let dir = |name: &str| var(name).map(PathBuf::from);
        let home = dir("HOME").or_else(|| dir("USERPROFILE"));
        let path = var("PATH")
            .map(|path| std::env::split_paths(&path).collect())
            .unwrap_or_default();
        let user = var("USER")
            .or_else(|| var("LOGNAME"))
            .and_then(|user| user.into_string().ok());
        let elevation = process_elevation(home.as_deref());
        Ok(Self {
            exe: std::env::current_exe()?,
            path,
            home,
            user,
            xdg_state_home: dir("XDG_STATE_HOME"),
            xdg_data_home: dir("XDG_DATA_HOME"),
            mise_data_dir: dir("MISE_DATA_DIR"),
            asdf_data_dir: dir("ASDF_DATA_DIR"),
            elevation,
        })
    }
}

fn process_elevation(home: Option<&Path>) -> Option<Elevation> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions and cannot fail.
        let euid = unsafe { libc::geteuid() };
        let sudo_user = std::env::var("SUDO_USER").ok();
        let home_owner = home.and_then(|home| std::fs::metadata(home).ok().map(|m| m.uid()));
        elevation(euid, sudo_user, home, home_owner)
    }
    #[cfg(not(unix))]
    {
        let _ = home;
        None
    }
}

/// Root with `SUDO_USER` set is sudo; otherwise a `HOME` owned by another
/// user means the environment belongs to someone else.
#[cfg_attr(not(unix), allow(dead_code))]
fn elevation(
    euid: u32,
    sudo_user: Option<String>,
    home: Option<&Path>,
    home_owner: Option<u32>,
) -> Option<Elevation> {
    if let Some(user) = sudo_user.filter(|user| euid == 0 && !user.is_empty()) {
        return Some(Elevation::Sudo { user });
    }
    match (home, home_owner) {
        (Some(home), Some(owner)) if owner != euid => Some(Elevation::ForeignHome {
            home: home.to_path_buf(),
        }),
        _ => None,
    }
}

/// Where the version managers keep their installs and dispatchers.
#[derive(Debug, Clone, Default)]
pub struct Layout {
    mise: Vec<PathBuf>,
    asdf: Vec<PathBuf>,
}

impl Layout {
    pub fn new(env: &Env, fs: &dyn Fs) -> Self {
        let home = env.home.as_deref();
        let mise = env
            .mise_data_dir
            .clone()
            .or_else(|| env.xdg_data_home.as_ref().map(|data| data.join("mise")))
            .or_else(|| home.map(|home| home.join(".local/share/mise")));
        let asdf = env
            .asdf_data_dir
            .clone()
            .or_else(|| home.map(|home| home.join(".asdf")));
        Self {
            mise: with_resolved(mise, fs),
            asdf: with_resolved(asdf, fs),
        }
    }

    /// [`Layout::new`] for the running process.
    pub fn from_process() -> Self {
        Env::from_process()
            .map(|env| Self::new(&env, &RealFs))
            .unwrap_or_default()
    }

    /// The installer whose version directory holds `path`, if any. A path
    /// through mise's `latest` alias is not versioned.
    pub fn versioned(&self, path: &Path) -> Option<Kind> {
        if path.starts_with("/nix/store") {
            return Some(Kind::Nix);
        }
        if homebrew_keg(path).is_some() {
            return Some(Kind::Homebrew);
        }
        if install_parts(path, &self.mise).is_some_and(|parts| parts.version != "latest") {
            return Some(Kind::Mise);
        }
        if install_parts(path, &self.asdf).is_some() {
            return Some(Kind::Asdf);
        }
        None
    }

    /// Whether `dir` holds mise or asdf dispatchers, which run whatever
    /// version the current directory selects.
    fn is_dispatcher(&self, dir: &Path) -> bool {
        self.mise
            .iter()
            .chain(&self.asdf)
            .any(|root| dir == root.join("shims"))
    }
}

/// `dir` and, when it resolves, its real path. The two may be equal.
fn with_resolved(dir: Option<PathBuf>, fs: &dyn Fs) -> Vec<PathBuf> {
    let Some(dir) = dir else {
        return Vec::new();
    };
    let real = fs.resolve(&dir);
    std::iter::once(dir).chain(real).collect()
}

/// `<prefix>/Cellar/<formula>/<version>/<rest>` split into the `opt` alias
/// `<prefix>/opt/<formula>/<rest>` and the formula name.
fn homebrew_keg(path: &Path) -> Option<(PathBuf, String)> {
    let components: Vec<Component<'_>> = path.components().collect();
    let cellar = components.iter().position(|c| c.as_os_str() == "Cellar")?;
    let formula = components.get(cellar + 1)?.as_os_str();
    let rest = components
        .get(cellar + 3..)
        .filter(|rest| !rest.is_empty())?;
    let mut opt: PathBuf = components[..cellar].iter().collect();
    opt.push("opt");
    opt.push(formula);
    opt.extend(rest);
    Some((opt, formula.to_string_lossy().into_owned()))
}

struct InstallParts {
    root: PathBuf,
    tool: String,
    version: String,
    rest: PathBuf,
}

/// `<root>/installs/<tool>/<version>/<rest>` for one of `roots`.
fn install_parts(path: &Path, roots: &[PathBuf]) -> Option<InstallParts> {
    roots.iter().find_map(|root| {
        let inside = path.strip_prefix(root.join("installs")).ok()?;
        let mut components = inside.components();
        let tool = components
            .next()?
            .as_os_str()
            .to_string_lossy()
            .into_owned();
        let version = components
            .next()?
            .as_os_str()
            .to_string_lossy()
            .into_owned();
        Some(InstallParts {
            root: root.clone(),
            tool,
            version,
            rest: components.as_path().to_path_buf(),
        })
    })
}

/// Nix profile `bin` directories, most specific first.
fn nix_profile_bins(env: &Env) -> Vec<PathBuf> {
    let mut bins = Vec::new();
    if let Some(home) = &env.home {
        bins.push(home.join(".nix-profile/bin"));
    }
    let state = env
        .xdg_state_home
        .clone()
        .or_else(|| env.home.as_ref().map(|home| home.join(".local/state")));
    if let Some(state) = state {
        bins.push(state.join("nix/profile/bin"));
    }
    if let Some(user) = &env.user {
        bins.push(Path::new("/etc/profiles/per-user").join(user).join("bin"));
    }
    bins.push(PathBuf::from("/run/current-system/sw/bin"));
    bins.push(PathBuf::from("/nix/var/nix/profiles/default/bin"));
    bins
}

/// Whether rule 2 ignores `dir` on `PATH`.
fn skip_path_dir(dir: &Path, layout: &Layout, fs: &dyn Fs) -> bool {
    !dir.has_root()
        || layout.versioned(dir).is_some()
        || layout.is_dispatcher(dir)
        || fs.is_file(&dir.join(crate::farm::MARKER))
}

/// Select the path to record for the binary `env.exe`.
pub fn select(env: &Env, fs: &dyn Fs) -> Result<Selection, Error> {
    let own = fs.identity(&env.exe).ok_or_else(|| missing_exe(&env.exe))?;
    let resolved = fs.resolve(&env.exe).unwrap_or_else(|| env.exe.clone());
    let layout = Layout::new(env, fs);
    let kind = layout.versioned(&resolved).unwrap_or(Kind::Other);
    let name = env
        .exe
        .file_name()
        .or_else(|| resolved.file_name())
        .map(PathBuf::from)
        .unwrap_or_default();
    let reaches = |candidate: &Path| reaches(candidate, own, fs);
    let selection = installer_alias(&resolved, kind, &name, env, &layout, &reaches)
        .or_else(|| path_entry(&name, kind, env, &layout, fs, &reaches))
        .unwrap_or_else(|| fallback(resolved, kind));
    Ok(with_elevation(selection, env.elevation.as_ref()))
}

/// [`select`] for the running process.
pub fn detect() -> Result<Selection, Error> {
    let env = Env::from_process().map_err(Error::CurrentExe)?;
    select(&env, &RealFs)
}

fn missing_exe(exe: &Path) -> Error {
    let text = exe.to_string_lossy();
    match text.strip_suffix(" (deleted)") {
        Some(original) => Error::Replaced(PathBuf::from(original)),
        None => Error::Unreachable(exe.to_path_buf()),
    }
}

fn reaches(candidate: &Path, own: InodeId, fs: &dyn Fs) -> bool {
    fs.is_executable_file(candidate) && fs.identity(candidate) == Some(own)
}

fn installer_alias(
    resolved: &Path,
    kind: Kind,
    name: &Path,
    env: &Env,
    layout: &Layout,
    reaches: &dyn Fn(&Path) -> bool,
) -> Option<Selection> {
    let managed = |path: PathBuf, reason: String| Selection {
        path,
        kind,
        stability: Stability::InstallerManaged,
        reason,
    };
    match kind {
        Kind::Homebrew => {
            let (opt, formula) = homebrew_keg(resolved)?;
            reaches(&opt).then(|| {
                managed(
                    opt,
                    format!("Homebrew's opt link for {formula}, which each upgrade repoints"),
                )
            })
        }
        Kind::Nix => nix_profile_bins(env)
            .into_iter()
            .map(|bin| bin.join(name))
            .find(|candidate| reaches(candidate))
            .map(|profile| {
                let dir = profile.parent().unwrap_or(&profile).display().to_string();
                managed(
                    profile,
                    format!("the Nix profile in {dir}, which each upgrade or rollback repoints"),
                )
            }),
        Kind::Mise => {
            let parts = install_parts(resolved, &layout.mise)?;
            let latest = parts
                .root
                .join("installs")
                .join(&parts.tool)
                .join("latest")
                .join(&parts.rest);
            reaches(&latest).then(|| {
                managed(
                    latest,
                    format!(
                        "mise's latest alias for {}, which moves to the newest installed version",
                        parts.tool
                    ),
                )
            })
        }
        Kind::Asdf | Kind::Other => None,
    }
}

fn path_entry(
    name: &Path,
    kind: Kind,
    env: &Env,
    layout: &Layout,
    fs: &dyn Fs,
    reaches: &dyn Fn(&Path) -> bool,
) -> Option<Selection> {
    env.path
        .iter()
        .filter(|dir| !skip_path_dir(dir, layout, fs))
        .map(|dir| dir.join(name))
        .find(|candidate| reaches(candidate))
        .map(|path| {
            let reason = format!(
                "the first {} on PATH, which keeps working until that file is replaced or removed",
                name.display()
            );
            Selection {
                path,
                kind,
                stability: Stability::UserManaged,
                reason,
            }
        })
}

fn fallback(resolved: PathBuf, kind: Kind) -> Selection {
    let reason = match kind {
        Kind::Homebrew => {
            "Homebrew's opt link does not reach this binary, so this keg path breaks when Homebrew removes this version"
        }
        Kind::Nix => {
            "no Nix profile reaches this binary, so this store path breaks when the Nix store is garbage collected"
        }
        Kind::Mise => {
            "mise's latest alias does not reach this version, so this path breaks when mise removes it"
        }
        Kind::Asdf => {
            "asdf keeps no alias for an installed version, so this path breaks when asdf uninstalls it"
        }
        Kind::Other => {
            "no installer alias or PATH entry reaches this binary, so this path breaks if the binary moves"
        }
    };
    Selection {
        path: resolved,
        kind,
        stability: Stability::Versioned,
        reason: reason.to_string(),
    }
}

fn with_elevation(mut selection: Selection, elevation: Option<&Elevation>) -> Selection {
    let Some(elevation) = elevation else {
        return selection;
    };
    let why = match elevation {
        Elevation::Sudo { user } => {
            format!("running as root through sudo for {user}, so HOME and PATH may be root's")
        }
        Elevation::ForeignHome { home } => {
            format!("HOME ({}) belongs to another user", home.display())
        }
    };
    if selection.stability.survives_upgrade() {
        selection.stability = Stability::Unverified;
    }
    selection.reason = format!("{why}; {}", selection.reason);
    selection
}

#[cfg(test)]
#[cfg(unix)]
mod tests;
