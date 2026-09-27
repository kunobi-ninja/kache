//! Compiler-name shim farms: directories of `cc`, `gcc`, `clang`, ...
//! symlinks to kache, put ahead of the real toolchain on `PATH`.

use crate::fs::Fs;
#[cfg(unix)]
use crate::select::Layout;
use std::fmt;
use std::io;
#[cfg(unix)]
use std::path::Component;
use std::path::{Path, PathBuf};

/// The compiler names a farm holds: the canonical drivers only. A shim is a
/// `PATH` ambush, so it covers what builds invoke rather than every name
/// kache recognizes. Versioned and target-prefixed names are added on
/// request.
pub const SHIM_NAMES: &[&str] = &["cc", "c++", "gcc", "g++", "clang", "clang++"];

/// File that marks a directory of kache shims. Packages that build a farm
/// write it, and [`install`] does when the directory holds nothing else.
/// Every entry in a marked directory is taken to be a shim, whichever kache
/// it belongs to.
pub const MARKER: &str = ".kache-shims";

/// The farm `kache install-shims` fills when given no directory.
pub fn default_dir(home: &Path) -> PathBuf {
    home.join(".local/lib/kache/shims")
}

/// The farm the distro packages install.
pub fn system_dir() -> PathBuf {
    PathBuf::from("/usr/lib/kache")
}

/// Whether `dir` holds [`MARKER`].
pub fn has_marker(dir: &Path) -> bool {
    dir.join(MARKER).is_file()
}

/// Mark `dir` as a shim directory.
pub fn write_marker(dir: &Path) -> io::Result<()> {
    std::fs::write(
        dir.join(MARKER),
        "kache compiler shims. kache skips this directory when it looks for \
         the real compiler, so keep only shims here.\n",
    )
}

/// Whether `path` names a kache binary by its file name. This catches the
/// shims of another kache install, including farms made before the marker
/// existed.
pub fn is_kache_binary(path: &Path) -> bool {
    path.file_name().is_some_and(|name| {
        name.eq_ignore_ascii_case("kache") || name.eq_ignore_ascii_case("kache.exe")
    })
}

/// Whether `candidate` resolves to `self_real` or to any binary named
/// `kache`.
pub fn resolves_to_kache(
    candidate: &Path,
    self_real: Option<&Path>,
    resolve: &dyn Fn(&Path) -> Option<PathBuf>,
) -> bool {
    resolve(candidate)
        .is_some_and(|real| self_real == Some(real.as_path()) || is_kache_binary(&real))
}

/// Whether `path` is a kache shim: it resolves to `self_real` or a binary
/// named `kache`, or it is a symlink whose text names one. The last case
/// covers a dangling shim, which resolves to nothing.
fn is_shim_entry(path: &Path, self_real: Option<&Path>, fs: &dyn Fs) -> bool {
    resolves_to_kache(path, self_real, &|path| fs.resolve(path))
        || fs
            .read_link(path)
            .is_some_and(|text| is_kache_binary(&text))
}

/// Whether `dir` may be marked: every entry other than the marker is a kache
/// shim. The marker hides every entry from every kache, so a real compiler,
/// or any other file, keeps the directory unmarked.
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

/// Whether every canonical shim in `dir` is a link to exactly `target` that
/// reaches an executable.
pub fn is_ready(dir: &Path, target: &Path, fs: &dyn Fs) -> bool {
    SHIM_NAMES.iter().all(|name| {
        let link = dir.join(name);
        fs.read_link(&link).as_deref() == Some(target) && fs.is_executable_file(&link)
    })
}

#[cfg(unix)]
/// What one name in the farm holds before [`install`] touches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// Nothing.
    Empty,
    /// A link to the target that reaches an executable.
    Current,
    /// A link whose target is missing or not executable.
    Broken,
    /// A working link into a versioned install directory while the target
    /// is not in one.
    Versioned,
    /// Anything else: a file, a directory, a link elsewhere.
    Other,
}

#[cfg(unix)]
/// What [`install`] does with one name.
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
/// Classify one entry. `link` is the symlink's text made absolute,
/// `reaches_executable` whether following it ends at an executable file.
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
/// Replace only what cannot be serving anyone (a broken link) or what an
/// upgrade will break (a versioned link), and only in a directory kache
/// owns. `force` replaces anything but a current link.
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
/// A directory kache owns: created by this run, marked, or holding nothing
/// but kache shims.
fn owns(created: bool, marked: bool, only_shims: bool) -> bool {
    created || marked || only_shims
}

#[cfg(unix)]
/// Where the link text `text` in `dir` points, with `.` and `..` removed
/// lexically, so a relative link compares equal to an absolute target.
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
    /// New links.
    pub created: Vec<String>,
    /// Links whose target was missing or not executable, replaced.
    pub repaired: Vec<String>,
    /// Links into a versioned install directory, moved to the target.
    pub refreshed: Vec<String>,
    /// Entries replaced because the caller forced it.
    pub replaced: Vec<String>,
    /// Links that already pointed at the target.
    pub current: Vec<String>,
    /// Entries left alone.
    pub skipped: Vec<String>,
    /// Whether the directory now carries [`MARKER`].
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

/// Fill `dir` with symlinks named `names`, each pointing at `target`.
///
/// An existing entry is kept unless `force` is set, with two exceptions in a
/// directory kache owns: a link whose target is missing or not executable is
/// replaced, and so is a working link into a versioned install directory
/// when `target` is not in one. When the directory ends up holding only
/// kache shims it is marked with [`MARKER`].
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

/// Where the compiler shims stand for one `PATH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// The first `name` on `PATH` is a shim of the running kache.
    Active {
        name: String,
        dir: PathBuf,
    },
    /// A farm holds shims whose target is missing or not executable. A
    /// shell's `PATH` lookup skips them, so builds run the compiler
    /// uncached.
    Broken {
        dir: PathBuf,
        names: Vec<String>,
        target: PathBuf,
    },
    /// A working farm that is not first on `PATH`.
    NotFirst {
        dir: PathBuf,
    },
    NotInstalled,
}

impl Status {
    pub fn is_active(&self) -> bool {
        matches!(self, Status::Active { .. })
    }

    /// One line for a status report.
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

    /// The command that fixes this state. `default_dir` is the farm
    /// `kache install-shims` fills with no argument.
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

/// Shims in `dir` that are kache's and no longer reach an executable, with
/// the text of the first one's link.
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

/// The first canonical name whose first `PATH` hit is the running kache.
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

/// Report the shims for `path_dirs`, checking `known_dirs` (the default and
/// system farms) for one that is installed but not on `PATH`. A broken farm
/// in either list comes first: its fix is the one that restores caching.
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
