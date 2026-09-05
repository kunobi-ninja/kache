//! Local artifact storage, independent of compiler parsing and remote transports.

pub mod atomic;
pub mod config;
pub mod eviction;
pub mod file_hash;
pub mod filesystem;
pub mod link;
pub mod markers;
pub mod opcounts;
pub mod sharing;
mod store;
#[cfg(test)]
mod test_support;

pub use store::*;

/// Compiler-owned rules applied when publishing an entry. Implementations must
/// permit shared inodes only for outputs that remain immutable after publication.
pub trait ArtifactPolicy {
    fn allow_hardlink(name: &str) -> bool;
    fn allow_empty(name: &str, output_types: &[String]) -> bool;
    fn emit_kind(name: &str) -> Option<&'static str>;
    fn stable_after_store(name: &str) -> bool;
}
