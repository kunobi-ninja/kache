//! `kache-cxx` — single-token `CXX` shim that forwards to `kache c++`.
//!
//! The C++ companion to `kache-cc`; see [`kache_e2e::run_kache_compiler_shim`].

fn main() {
    kache_e2e::run_kache_compiler_shim("c++");
}
