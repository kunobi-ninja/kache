//! Upgrade-safe paths to an installed binary, and the compiler-name shim
//! farms that point at it.
//!
//! [`select`] (or [`detect`] for the running process) picks the path kache
//! records in shims and service files. The [`farm`] module creates, repairs
//! and reports on the shim directories.

pub mod farm;
mod fs;
mod select;

#[cfg(unix)]
pub use farm::install;
pub use farm::{InstallError, Installed, Status};
pub use fs::{Fs, RealFs, is_executable_file};
pub use select::{Elevation, Env, Error, Kind, Layout, Selection, Stability, detect, select};
