//! Which path to record for the running binary so shims and service files
//! survive an upgrade. Rules, first match wins: an installer alias, a stable
//! `PATH` entry, the binary's own path. A candidate counts only if it has the
//! running binary's file identity (device and inode), checked now.

use crate::fs::{Fs, RealFs};
use kache_fs::InodeId;
use std::fmt;
use std::path::{Component, Path, PathBuf};

/// The installer that placed the running binary, judged from its resolved path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Homebrew,
    Nix,
    Mise,
    Asdf,
    /// `cargo install`, a distro package, a copied binary.
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

/// Whether the selected path is expected to keep working after an upgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stability {
    /// An alias the installer repoints on every upgrade.
    InstallerManaged,
    /// A `PATH` entry outside version directories; valid until replaced.
    UserManaged,
    /// The binary's own path; removing this version breaks it.
    Versioned,
    /// Running under sudo or a foreign `HOME`, so no claim is made.
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
    /// One sentence for `kache doctor`.
    pub reason: String,
}

/// Why no path could be selected.
#[derive(Debug)]
pub enum Error {
    CurrentExe(std::io::Error),
    /// Replaced on disk while running (Linux reports `<path> (deleted)`).
    Replaced(PathBuf),
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

/// `HOME` and `PATH` may belong to another account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Elevation {
    Sudo { user: String },
    ForeignHome { home: PathBuf },
}

/// Everything selection reads from the process.
#[derive(Debug, Clone, Default)]
pub struct Env {
    /// `current_exe()`, unresolved.
    pub exe: PathBuf,
    /// Identity of the running image (`/proc/self/exe` on Linux). When set,
    /// `exe` must still name this file, or the binary was replaced.
    pub exe_identity: Option<InodeId>,
    pub path: Vec<PathBuf>,
    pub home: Option<PathBuf>,
    pub user: Option<String>,
    pub xdg_state_home: Option<PathBuf>,
    pub xdg_data_home: Option<PathBuf>,
    pub mise_data_dir: Option<PathBuf>,
    pub mise_installs_dir: Option<PathBuf>,
    pub asdf_data_dir: Option<PathBuf>,
    /// asdf's own checkout, its data dir when `ASDF_DATA_DIR` is unset and
    /// `~/.asdf` does not exist.
    pub asdf_dir: Option<PathBuf>,
    pub elevation: Option<Elevation>,
}

impl Env {
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
        let elevation = process_elevation(home.as_deref(), user.as_deref());
        Ok(Self {
            exe: std::env::current_exe()?,
            exe_identity: running_identity(),
            path,
            home,
            user,
            xdg_state_home: dir("XDG_STATE_HOME"),
            xdg_data_home: dir("XDG_DATA_HOME"),
            mise_data_dir: dir("MISE_DATA_DIR"),
            mise_installs_dir: dir("MISE_INSTALLS_DIR"),
            asdf_data_dir: dir("ASDF_DATA_DIR"),
            asdf_dir: dir("ASDF_DIR"),
            elevation,
        })
    }
}

/// Identity of the running image. On Linux `/proc/self/exe` reaches it even
/// after the path was replaced; elsewhere this re-reads `current_exe()`, a
/// present-time check.
pub fn running_identity() -> Option<InodeId> {
    #[cfg(target_os = "linux")]
    {
        kache_fs::file_identity(Path::new("/proc/self/exe")).ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::env::current_exe()
            .ok()
            .and_then(|exe| kache_fs::file_identity(&exe).ok())
    }
}

fn process_elevation(home: Option<&Path>, user: Option<&str>) -> Option<Elevation> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions.
        let euid = unsafe { libc::geteuid() };
        let sudo_user = std::env::var("SUDO_USER").ok();
        let home_owner = home.and_then(|home| std::fs::metadata(home).ok().map(|m| m.uid()));
        elevation(euid, sudo_user, user, home, home_owner)
    }
    #[cfg(not(unix))]
    {
        let _ = (home, user);
        None
    }
}

#[cfg_attr(not(unix), allow(dead_code))]
/// sudo to root, or `sudo -u <other>`; a `SUDO_USER` equal to the current
/// user is a leftover.
fn elevation(
    euid: u32,
    sudo_user: Option<String>,
    user: Option<&str>,
    home: Option<&Path>,
    home_owner: Option<u32>,
) -> Option<Elevation> {
    let sudo = |by: &String| !by.is_empty() && (euid == 0 || user != Some(by.as_str()));
    if let Some(user) = sudo_user.filter(sudo) {
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
    mise_installs: Vec<PathBuf>,
    asdf_installs: Vec<PathBuf>,
    dispatchers: Vec<PathBuf>,
}

impl Layout {
    pub fn new(env: &Env, fs: &dyn Fs) -> Self {
        let home = env.home.as_deref();
        let mise = env
            .mise_data_dir
            .clone()
            .or_else(|| env.xdg_data_home.as_ref().map(|data| data.join("mise")))
            .or_else(|| home.map(|home| home.join(".local/share/mise")));
        let mise_installs = env
            .mise_installs_dir
            .clone()
            .or_else(|| mise.as_ref().map(|mise| mise.join("installs")));
        let home_asdf = home
            .map(|home| home.join(".asdf"))
            .filter(|dir| fs.resolve(dir).is_some());
        let asdf = env
            .asdf_data_dir
            .clone()
            .or(home_asdf)
            .or_else(|| env.asdf_dir.clone());
        let shims = |root: &Option<PathBuf>| root.as_ref().map(|root| root.join("shims"));
        Self {
            mise_installs: with_resolved(mise_installs, fs),
            asdf_installs: with_resolved(asdf.as_ref().map(|asdf| asdf.join("installs")), fs),
            dispatchers: [shims(&mise), shims(&asdf)]
                .into_iter()
                .flat_map(|dir| with_resolved(dir, fs))
                .collect(),
        }
    }

    pub fn from_process() -> Self {
        Env::from_process()
            .map(|env| Self::new(&env, &RealFs))
            .unwrap_or_default()
    }

    /// The installer whose version directory holds `path`. mise's `latest`
    /// alias is not a version directory.
    pub fn versioned(&self, path: &Path) -> Option<Kind> {
        if path.starts_with("/nix/store") || path.components().any(is_nix_generation) {
            return Some(Kind::Nix);
        }
        if homebrew_keg(path).is_some() {
            return Some(Kind::Homebrew);
        }
        if install_parts(path, &self.mise_installs).is_some_and(|parts| parts.version != "latest") {
            return Some(Kind::Mise);
        }
        if install_parts(path, &self.asdf_installs).is_some() {
            return Some(Kind::Asdf);
        }
        None
    }

    /// mise and asdf shims run whatever version the current directory selects.
    fn is_dispatcher(&self, dir: &Path) -> bool {
        self.dispatchers.iter().any(|shims| dir == shims)
    }
}

/// `current_exe()` comes back resolved, so match data dirs by their real path too.
fn with_resolved(dir: Option<PathBuf>, fs: &dyn Fs) -> Vec<PathBuf> {
    let Some(dir) = dir else {
        return Vec::new();
    };
    let real = fs.resolve(&dir);
    std::iter::once(dir).chain(real).collect()
}

/// `<prefix>/Cellar/<formula>/<version>/<rest>` -> (`<prefix>/opt/<formula>`, rest, formula).
fn homebrew_keg(path: &Path) -> Option<(PathBuf, PathBuf, String)> {
    let components: Vec<Component<'_>> = path.components().collect();
    let cellar = components.iter().position(|c| c.as_os_str() == "Cellar")?;
    let formula = components.get(cellar + 1)?.as_os_str();
    let rest = components
        .get(cellar + 3..)
        .filter(|rest| !rest.is_empty())?;
    let mut link: PathBuf = components[..cellar].iter().collect();
    link.push("opt");
    link.push(formula);
    let rest = rest.iter().collect();
    Some((link, rest, formula.to_string_lossy().into_owned()))
}

struct InstallParts {
    root: PathBuf,
    tool: String,
    version: String,
    rest: PathBuf,
}

/// A pinned Nix generation such as `profile-12-link`.
fn is_nix_generation(component: Component<'_>) -> bool {
    let name = component.as_os_str().to_string_lossy();
    name.strip_suffix("-link")
        .and_then(|rest| rest.rsplit_once('-'))
        .is_some_and(|(profile, number)| {
            !profile.is_empty() && !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())
        })
}

/// `<installs>/<tool>/<version>/<rest>` for one of `roots`.
fn install_parts(path: &Path, roots: &[PathBuf]) -> Option<InstallParts> {
    roots.iter().find_map(|root| {
        let inside = path.strip_prefix(root).ok()?;
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
        let per_user = Path::new("/nix/var/nix/profiles/per-user").join(user);
        bins.push(per_user.join("profile/bin"));
    }
    bins.push(PathBuf::from("/run/current-system/sw/bin"));
    bins.push(PathBuf::from("/nix/var/nix/profiles/default/bin"));
    bins.push(PathBuf::from(
        "/nix/var/nix/profiles/per-user/root/profile/bin",
    ));
    bins
}

fn skip_path_dir(dir: &Path, layout: &Layout, fs: &dyn Fs) -> bool {
    !dir.is_absolute()
        || layout.versioned(dir).is_some()
        || layout.is_dispatcher(dir)
        || fs.is_file(&dir.join(crate::farm::MARKER))
}

/// The path to record for `env.exe`.
pub fn select(env: &Env, fs: &dyn Fs) -> Result<Selection, Error> {
    let own = own_identity(env, fs)?;
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
    let selection = installer_alias(&resolved, kind, &name, env, &layout, fs, &reaches)
        .or_else(|| path_entry(&name, kind, env, &layout, fs, &reaches))
        .unwrap_or_else(|| fallback(resolved, kind));
    Ok(with_elevation(selection, env.elevation.as_ref()))
}

/// [`select`] for the running process.
pub fn detect() -> Result<Selection, Error> {
    let env = Env::from_process().map_err(Error::CurrentExe)?;
    select(&env, &RealFs)
}

/// The path must still name the running image; otherwise it was replaced.
fn own_identity(env: &Env, fs: &dyn Fs) -> Result<InodeId, Error> {
    let at_path = fs.identity(&env.exe).ok_or_else(|| missing_exe(&env.exe))?;
    match env.exe_identity {
        Some(running) if running != at_path => Err(Error::Replaced(env.exe.clone())),
        _ => Ok(at_path),
    }
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

/// An alias counts only if it is a link the installer maintains; a copy with
/// the same identity is not.
fn installer_alias(
    resolved: &Path,
    kind: Kind,
    name: &Path,
    env: &Env,
    layout: &Layout,
    fs: &dyn Fs,
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
            let (link, rest, formula) = homebrew_keg(resolved)?;
            let opt = link.join(rest);
            (fs.read_link(&link).is_some() && reaches(&opt)).then(|| {
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
            let parts = install_parts(resolved, &layout.mise_installs)?;
            let alias = parts.root.join(&parts.tool).join("latest");
            let latest = alias.join(&parts.rest);
            (fs.read_link(&alias).is_some() && reaches(&latest)).then(|| {
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
    // A link into a version or store directory is as pinned as its target.
    let resolves_outside = |candidate: &Path| {
        fs.resolve(candidate)
            .and_then(|real| real.parent().map(Path::to_path_buf))
            .is_some_and(|dir| !skip_path_dir(&dir, layout, fs))
    };
    env.path
        .iter()
        .filter(|dir| !skip_path_dir(dir, layout, fs))
        .map(|dir| dir.join(name))
        .find(|candidate| reaches(candidate) && resolves_outside(candidate))
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
