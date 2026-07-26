//! Trivial library. The point of this fixture is the `--test` units that
//! `cargo check --all-targets` compiles with `--emit=metadata`: rustc writes
//! a zero-byte `.rmeta` for them, and those entries must still cache (#624).
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}
