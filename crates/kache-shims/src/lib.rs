//! Upgrade-safe paths to an installed binary ([`select`], [`detect`]) and the
//! compiler-name shim farms that point at it ([`farm`]).

pub mod farm;
mod fs;
mod select;

#[cfg(unix)]
pub use farm::install;
pub use farm::{InstallError, Installed, Status};
pub use fs::{Fs, RealFs, is_executable_file};
pub use kache_fs::InodeId;
pub use select::{
    Elevation, Env, Error, Kind, Layout, Selection, Stability, detect, running_identity, select,
};
