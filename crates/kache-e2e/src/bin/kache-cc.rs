//! `kache-cc` — single-token `CC` shim that forwards to `kache cc`.
//!
//! Lets a `cc`-crate (`build.rs`) C compile route through kache on Windows,
//! where a multi-token `CC="<path>\kache.exe cc"` is mangled by cc-rs's
//! `check_exe`. See [`kache_e2e::run_kache_compiler_shim`] for the full
//! rationale.

fn main() {
    kache_e2e::run_kache_compiler_shim("cc");
}
