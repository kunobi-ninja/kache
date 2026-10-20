//! Upgrade-safe paths to an installed binary ([`select`], [`detect`]) and the
//! compiler-name shim farms that point at it ([`farm`]).

mod companion;
pub mod farm;
pub use companion::{FrontendImage, engine_beside, engine_for, frontend_for};
mod fs;
mod program;
mod select;
pub use program::resolve_program_on_path_with;

#[cfg(unix)]
pub use farm::install;
pub use farm::{InstallError, Installed, Status};
pub use fs::{Fs, RealFs, is_executable_file};
pub use kache_fs::InodeId;
pub use select::{
    Elevation, Env, Error, Kind, Layout, Selection, Stability, detect, detect_frontend,
    running_identity, select, select_frontend,
};

mod compiler_name;
pub use compiler_name::{compiler_driver, is_cc_compiler_name, is_compiler_shim_name};
