// Debug-profile binary that calls into the sibling library crate (which pulls
// in the itoa registry dependency). The library's debug info is remapped and
// linked into this executable, so the harness can assert the resolvable remap
// targets landed: the workspace comp_dir (/proc/self/cwd on Linux) and the
// dependency comp_dir (/kache/.cargo/...) — see scenario.toml, #480/#485.

fn main() {
    let n: u64 = std::env::args().count() as u64;
    println!("rust-debug-paths: {}", rust_debug_paths::format_bumped(n));
}
