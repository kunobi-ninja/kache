//! Compiler invocation capture and synchronous handoff to a running Kache daemon.

#[cfg(any(unix, windows))]
mod execution;
#[cfg(any(unix, windows))]
pub use execution::Execution;

#[cfg(any(unix, windows))]
mod stdio;
#[cfg(any(unix, windows))]
pub use stdio::Stdio;
#[cfg(unix)]
mod creation_mask;
#[cfg(unix)]
pub use creation_mask::creation_mask;
mod frontend;
pub use frontend::run;

/// Internal argv envelope passed from the frontend to its companion engine.
pub const FORWARDED_ARGV_FLAG: &str = "--kache-forwarded-argv-v1";

mod entry;
pub use entry::{EngineEntry, engine_entry};

include!(concat!(env!("OUT_DIR"), "/commands.rs"));
