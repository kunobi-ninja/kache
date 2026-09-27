//! Compiler-name shim farms: `cc`, `gcc`, `clang`, ... symlinks to kache,
//! put ahead of the real toolchain on `PATH`.

use crate::fs::Fs;
#[cfg(unix)]
use crate::select::Layout;
use std::fmt;
use std::io;
#[cfg(unix)]
use std::path::Component;
use std::path::{Path, PathBuf};

/// The canonical drivers. Versioned and target-prefixed names are opt-in.
pub const SHIM_NAMES: &[&str] = &["cc", "c++", "gcc", "g++", "clang", "clang++"];

/// Marks a directory of kache shims. Every kache skips every entry in it when
/// looking for the real compiler, so it goes only on shim-only directories.
pub const MARKER: &str = ".kache-shims";

/// The farm `kache install-shims` fills by default.
pub fn default_dir(home: &Path) -> PathBuf {
    home.join(".local/lib/kache/shims")
}

/// The farm the distro packages ship.
pub fn system_dir() -> PathBuf {
    PathBuf::from("/usr/lib/kache")
}

pub fn has_marker(dir: &Path) -> bool {
    dir.join(MARKER).is_file()
}

pub fn write_marker(dir: &Path) -> io::Result<()> {
    std::fs::write(
        dir.join(MARKER),
        "kache compiler shims. kache skips this directory when it looks for \
         the real compiler, so keep only shims here.\n",
    )
}

/// Catches shims of another kache install, including farms older than the marker.
pub fn is_kache_binary(path: &Path) -> bool {
    path.file_name().is_some_and(|name| {
        name.eq_ignore_ascii_case("kache") || name.eq_ignore_ascii_case("kache.exe")
    })
}

pub fn resolves_to_kache(
    candidate: &Path,
    self_real: Option<&Path>,
    resolve: &dyn Fn(&Path) -> Option<PathBuf>,
) -> bool {
    resolve(candidate)
        .is_some_and(|real| self_real == Some(real.as_path()) || is_kache_binary(&real))
}

/// The link-text check covers a dangling shim, which resolves to nothing.
fn is_shim_entry(path: &Path, self_real: Option<&Path>, fs: &dyn Fs) -> bool {
    resolves_to_kache(path, self_real, &|path| fs.resolve(path))
        || fs
            .read_link(path)
            .is_some_and(|text| is_kache_binary(&text))
}

/// Whether `dir` may be marked: everything in it besides the marker is a kache shim.
pub fn holds_only_shims(dir: &Path, self_exe: Option<&Path>, fs: &dyn Fs) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    let self_real = self_exe.and_then(|exe| fs.resolve(exe));
    entries.into_iter().all(|entry| {
        entry.is_ok_and(|entry| {
            entry.file_name() == MARKER || is_shim_entry(&entry.path(), self_real.as_deref(), fs)
        })
    })
}

/// Every canonical shim in `dir` is a working link whose text is exactly `target`.
pub fn is_ready(dir: &Path, target: &Path, fs: &dyn Fs) -> bool {
    SHIM_NAMES.iter().all(|name| {
        let link = dir.join(name);
        fs.read_link(&link).as_deref() == Some(target) && fs.is_executable_file(&link)
    })
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    Empty,
    Current,
    /// Target missing or not executable.
    Broken,
    /// Works, but points into a version directory while the target does not.
    Versioned,
    Other,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    Create,
    Keep,
    Skip,
    Replace(Why),
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Why {
    Forced,
    Repair,
    Refresh,
}

#[cfg(unix)]
/// `link` is the symlink text made absolute; `None` for anything else.
fn classify(
    occupied: bool,
    link: Option<&Path>,
    reaches_executable: bool,
    target: &Path,
    versioned: &dyn Fn(&Path) -> bool,
) -> Slot {
    if !occupied {
        return Slot::Empty;
    }
    let Some(link) = link else {
        return Slot::Other;
    };
    if !reaches_executable {
        return Slot::Broken;
    }
    if link == target {
        return Slot::Current;
    }
    if versioned(link) && !versioned(target) {
        return Slot::Versioned;
    }
    Slot::Other
}

#[cfg(unix)]
/// Without `force`, replace only what serves no one (broken) or an upgrade
/// will break (versioned), and only in a directory kache owns.
fn decide(slot: Slot, force: bool, owned: bool) -> Action {
    match slot {
        Slot::Empty => Action::Create,
        Slot::Current => Action::Keep,
        _ if force => Action::Replace(Why::Forced),
        Slot::Broken if owned => Action::Replace(Why::Repair),
        Slot::Versioned if owned => Action::Replace(Why::Refresh),
        _ => Action::Skip,
    }
}

#[cfg(unix)]
fn owns(created: bool, marked: bool, only_shims: bool) -> bool {
    created || marked || only_shims
}

#[cfg(unix)]
/// Lexical, so a relative link compares equal to an absolute target.
fn link_path(dir: &Path, text: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in dir.join(text).components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other),
        }
    }
    out
}

/// What [`install`] did, name by name.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Installed {
    pub created: Vec<String>,
    /// Broken links replaced.
    pub repaired: Vec<String>,
    /// Versioned links moved to the target.
    pub refreshed: Vec<String>,
    /// Replaced because of `force`.
    pub replaced: Vec<String>,
    pub current: Vec<String>,
    pub skipped: Vec<String>,
    pub marked: bool,
}

#[cfg(unix)]
impl Installed {
    fn record(&mut self, name: String, action: Action) {
        let list = match action {
            Action::Create => &mut self.created,
            Action::Keep => &mut self.current,
            Action::Skip => &mut self.skipped,
            Action::Replace(Why::Forced) => &mut self.replaced,
            Action::Replace(Why::Repair) => &mut self.repaired,
            Action::Replace(Why::Refresh) => &mut self.refreshed,
        };
        list.push(name);
    }
}

/// An [`install`] step that failed.
#[derive(Debug)]
pub struct InstallError {
    what: String,
    source: io::Error,
}

#[cfg(unix)]
impl InstallError {
    fn new(what: impl fmt::Display, path: &Path, source: io::Error) -> Self {
        Self {
            what: format!("{what} {}", path.display()),
            source,
        }
    }
}

impl fmt::Display for InstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.what)
    }
}

impl std::error::Error for InstallError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// Link each of `names` in `dir` to `target`. Existing entries stay unless
/// `force`, except broken or versioned links in a directory kache owns
/// (created now, marked, or holding only kache shims). Marks the directory
/// when it ends up holding only shims.
#[cfg(unix)]
pub fn install(
    dir: &Path,
    target: &Path,
    names: &[String],
    force: bool,
    layout: &Layout,
) -> Result<Installed, InstallError> {
    let fs = crate::fs::RealFs;
    let created = std::fs::symlink_metadata(dir).is_err();
    std::fs::create_dir_all(dir)
        .map_err(|error| InstallError::new("creating shim directory", dir, error))?;
    let owned = owns(
        created,
        has_marker(dir),
        holds_only_shims(dir, Some(target), &fs),
    );
    let versioned = |path: &Path| layout.versioned(path).is_some();

    let mut report = Installed::default();
    for name in names {
        let link = dir.join(name);
        let (occupied, text) = match std::fs::symlink_metadata(&link) {
            // read_link fails for anything that is not a symlink.
            Ok(_) => (
                true,
                std::fs::read_link(&link)
                    .ok()
                    .map(|text| link_path(dir, &text)),
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => (false, None),
            Err(error) => return Err(InstallError::new("inspecting", &link, error)),
        };
        let reaches = crate::fs::is_executable_file(&link);
        let slot = classify(occupied, text.as_deref(), reaches, target, &versioned);
        let action = decide(slot, force, owned);
        if let Action::Replace(_) = action {
            std::fs::remove_file(&link)
                .map_err(|error| InstallError::new("replacing", &link, error))?;
        }
        if matches!(action, Action::Create | Action::Replace(_)) {
            std::os::unix::fs::symlink(target, &link)
                .map_err(|error| InstallError::new("creating shim", &link, error))?;
        }
        report.record(name.clone(), action);
    }

    report.marked = holds_only_shims(dir, Some(target), &fs);
    if report.marked {
        write_marker(dir)
            .map_err(|error| InstallError::new("marking shim directory", dir, error))?;
    }
    Ok(report)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    Active {
        name: String,
        dir: PathBuf,
    },
    /// `PATH` lookup skips a dangling link, so builds silently run uncached.
    Broken {
        dir: PathBuf,
        names: Vec<String>,
        target: PathBuf,
    },
    NotFirst {
        dir: PathBuf,
    },
    NotInstalled,
}

impl Status {
    pub fn is_active(&self) -> bool {
        matches!(self, Status::Active { .. })
    }

    pub fn detail(&self) -> String {
        match self {
            Status::Active { name, dir } => {
                format!("{name} on PATH is a kache shim ({})", dir.display())
            }
            Status::Broken { dir, names, target } => format!(
                "broken: {} in {} point at {}, which is missing or not executable",
                names.join(", "),
                dir.display(),
                target.display()
            ),
            Status::NotFirst { dir } => {
                format!("installed at {}, not first on PATH", dir.display())
            }
            Status::NotInstalled => "not installed".into(),
        }
    }

    /// The command that fixes this state.
    pub fn fix(&self, default_dir: &Path) -> Option<String> {
        match self {
            Status::Active { .. } => None,
            Status::Broken { dir, .. } if dir == default_dir => Some("kache install-shims".into()),
            Status::Broken { dir, .. } => Some(format!("kache install-shims {}", dir.display())),
            Status::NotFirst { dir } => Some(format!("export PATH=\"{}:$PATH\"", dir.display())),
            Status::NotInstalled => Some(format!(
                "kache install-shims && export PATH=\"{}:$PATH\"",
                default_dir.display()
            )),
        }
    }
}

fn broken_shims(dir: &Path, fs: &dyn Fs) -> Option<(Vec<String>, PathBuf)> {
    if !dir.has_root() {
        return None;
    }
    let marked = fs.is_file(&dir.join(MARKER));
    let mut names = Vec::new();
    let mut first = None;
    for name in SHIM_NAMES {
        let link = dir.join(name);
        let Some(text) = fs.read_link(&link) else {
            continue;
        };
        if (marked || is_kache_binary(&text)) && !fs.is_executable_file(&link) {
            names.push((*name).to_string());
            first.get_or_insert(text);
        }
    }
    first.map(|target| (names, target))
}

fn active_shim(
    path_dirs: &[PathBuf],
    own: Option<kache_fs::InodeId>,
    fs: &dyn Fs,
) -> Option<Status> {
    let own = own?;
    SHIM_NAMES.iter().find_map(|name| {
        let dir = path_dirs
            .iter()
            .find(|dir| fs.is_executable_file(&dir.join(name)))?;
        (fs.identity(&dir.join(name)) == Some(own)).then(|| Status::Active {
            name: (*name).to_string(),
            dir: dir.clone(),
        })
    })
}

/// `known_dirs` are the default and system farms. A broken farm is reported
/// first: its fix is the one that restores caching.
pub fn status(
    path_dirs: &[PathBuf],
    self_exe: Option<&Path>,
    known_dirs: &[PathBuf],
    fs: &dyn Fs,
) -> Status {
    let broken = known_dirs
        .iter()
        .chain(path_dirs)
        .find_map(|dir| broken_shims(dir, fs).map(|found| (dir, found)));
    if let Some((dir, (names, target))) = broken {
        return Status::Broken {
            dir: dir.clone(),
            names,
            target,
        };
    }
    let own = self_exe.and_then(|exe| fs.identity(exe));
    if let Some(active) = active_shim(path_dirs, own, fs) {
        return active;
    }
    let installed = known_dirs.iter().find(|dir| {
        own.is_some()
            && SHIM_NAMES
                .iter()
                .any(|name| fs.identity(&dir.join(name)) == own)
    });
    match installed {
        Some(dir) => Status::NotFirst { dir: dir.clone() },
        None => Status::NotInstalled,
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests;
