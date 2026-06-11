//! Integration test compiled by cargo into `target/debug/deps/e2e-<hash>`,
//! a user-facing `--test` executable. With `KACHE_CACHE_EXECUTABLES=1`
//! kache caches and restores it; the warm phase then has cargo EXECUTE the
//! restored binary. A restore that drops the execute bit reproduces #298's
//! `Permission denied (os error 13)` and fails this fixture's build step.
#[test]
fn test_binary_executes() {
    assert_eq!(rust_test_exec::answer(), 42);
}
