//! Toolchain shims (`cargo`, `cc`, `gcc`, `clang`, ...) ahead of the real
//! programs on `PATH`.

use crate::fs::Fs;
#[cfg(unix)]
use crate::select::Layout;
use std::fmt;
use std::io;
#[cfg(unix)]
use std::path::Component;
use std::path::{Path, PathBuf};

/// The canonical drivers. Versioned drivers ([`is_versioned_driver`]) are
/// linked when found on `PATH`; target-prefixed compiler names are opt-in.
pub const SHIM_NAMES: &[&str] = &[
    "cc", "c++", "gcc", "g++", "clang", "clang++", "cargo", "rustdoc",
];

/// A C or C++ driver from [`SHIM_NAMES`] with a version suffix, as Debian,
/// Ubuntu and Homebrew install releases side by side: `clang-19`,
/// `clang++-19`, `gcc-13`, `g++-14`. Decided by name alone, so finding them
/// on `PATH` runs nothing.
pub fn is_versioned_driver(name: &str) -> bool {
    name.rsplit_once('-').is_some_and(|(driver, version)| {
        matches!(driver, "cc" | "c++" | "gcc" | "g++" | "clang" | "clang++")
            && version
                .split('.')
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
    })
}

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

/// Writes the marker unless something is already there. `Ok(false)` when the
/// name is taken by anything but a regular file: never write through a link.
pub fn write_marker(dir: &Path) -> io::Result<bool> {
    use std::io::Write;
    let path = dir.join(MARKER);
    if let Ok(metadata) = std::fs::symlink_metadata(&path) {
        return Ok(metadata.is_file());
    }
    // create_new fails on any existing entry, a link included, and reports
    // whatever else kept the lookup from answering.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(
        b"kache toolchain shims. kache skips this directory when it looks for \
          the real toolchain, so keep only shims here.\n",
    )?;
    Ok(true)
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

/// Each of `names` in `dir` is a working link whose text is exactly `target`.
pub fn is_ready(dir: &Path, target: &Path, names: &[String], fs: &dyn Fs) -> bool {
    names.iter().all(|name| {
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
/// Without `force`, replace only a kache shim that serves no one (broken) or
/// that an upgrade will break (versioned), in a directory kache owns.
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

#[cfg(unix)]
/// A single plain file name: no separators, no `.` or `..`.
fn valid_name(name: &str) -> bool {
    let mut components = Path::new(name).components();
    matches!(components.next(), Some(Component::Normal(first)) if first == name)
        && components.next().is_none()
        && !name.contains(['/', '\\'])
}

#[cfg(unix)]
/// Point `link` at `target` through a temporary name and a rename, so the
/// name is never missing and a failure leaves the old entry in place.
fn replace_link(dir: &Path, name: &str, target: &Path) -> Result<(), InstallError> {
    let link = dir.join(name);
    let temporary = dir.join(format!(".{name}.kache-{}", std::process::id()));
    match std::fs::remove_file(&temporary) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => {
            return Err(InstallError::new("clearing", &temporary, error));
        }
        _ => {}
    }
    std::os::unix::fs::symlink(target, &temporary)
        .map_err(|error| InstallError::new("creating shim", &temporary, error))?;
    std::fs::rename(&temporary, &link).map_err(|error| {
        let _ = std::fs::remove_file(&temporary);
        InstallError::new("replacing", &link, error)
    })
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
/// `force`, except broken or versioned kache shims in a directory kache owns
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
    if let Some(name) = names.iter().find(|name| !valid_name(name)) {
        let error = io::Error::new(io::ErrorKind::InvalidInput, "not a plain file name");
        return Err(InstallError::new(
            "refusing shim name",
            Path::new(name),
            error,
        ));
    }
    let created = std::fs::symlink_metadata(dir).is_err();
    std::fs::create_dir_all(dir)
        .map_err(|error| InstallError::new("creating shim directory", dir, error))?;
    let owned = owns(
        created,
        has_marker(dir),
        holds_only_shims(dir, Some(target), &fs),
    );
    let versioned = |path: &Path| layout.versioned(path).is_some();
    let target_real = fs.resolve(target);

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
        let kache_entry = is_shim_entry(&link, target_real.as_deref(), &fs);
        let action = decide(slot, force, owned && kache_entry);
        match action {
            Action::Create => std::os::unix::fs::symlink(target, &link)
                .map_err(|error| InstallError::new("creating shim", &link, error))?,
            Action::Replace(_) => replace_link(dir, name, target)?,
            Action::Keep | Action::Skip => {}
        }
        report.record(name.clone(), action);
    }

    if holds_only_shims(dir, Some(target), &fs) {
        report.marked = write_marker(dir)
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

    /// The command that fixes this state. `default_dir` is `None` without a
    /// home directory.
    pub fn fix(&self, default_dir: Option<&Path>) -> Option<String> {
        match self {
            Status::Active { .. } => None,
            Status::Broken { dir, .. } if Some(dir.as_path()) == default_dir => {
                Some("kache install-shims".into())
            }
            Status::Broken { dir, .. } => Some(format!("kache install-shims {}", dir.display())),
            Status::NotFirst { dir } => Some(format!("export PATH=\"{}:$PATH\"", dir.display())),
            Status::NotInstalled => Some(match default_dir {
                Some(dir) => format!(
                    "kache install-shims && export PATH=\"{}:$PATH\"",
                    dir.display()
                ),
                None => "no home directory: run kache install-shims DIR and put DIR first on PATH"
                    .into(),
            }),
        }
    }
}

fn broken_shims(dir: &Path, fs: &dyn Fs) -> Option<(Vec<String>, PathBuf)> {
    if !dir.is_absolute() {
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

/// `known_dirs` are the default and system farms. A working farm first on
/// `PATH` wins; otherwise a broken farm comes next, since its fix is the one
/// that restores caching.
pub fn status(
    path_dirs: &[PathBuf],
    self_exe: Option<&Path>,
    known_dirs: &[PathBuf],
    fs: &dyn Fs,
) -> Status {
    let own = self_exe.and_then(|exe| fs.identity(exe));
    if let Some(active) = active_shim(path_dirs, own, fs) {
        return active;
    }
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
