//! A library crate so there is a cacheable rlib whose key carries RUSTFLAGS
//! (a bare bin is passthrough and caches nothing). The `relocate` phase then
//! exercises whether the build-injected `--remap-path-prefix` leaks the
//! checkout path into this crate's key.
pub fn greeting() -> &'static str {
    "kache remap-path-prefix fixture"
}
