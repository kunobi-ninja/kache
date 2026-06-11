//! Trivial library so the integration test under `tests/` has a crate to
//! link against. The point of this fixture is the `--test` binary that
//! cargo builds for `tests/e2e.rs`, not this code.
pub fn answer() -> u32 {
    42
}
