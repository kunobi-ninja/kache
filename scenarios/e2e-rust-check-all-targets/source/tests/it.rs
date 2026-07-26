//! Integration test. `cargo check --all-targets` type-checks it as a `--test`
//! unit with `--emit=metadata`, which is where the zero-byte `.rmeta` comes
//! from — the test body is never run by this fixture.
#[test]
fn adds() {
    assert_eq!(rust_check_all_targets::add(1, 1), 2);
}
