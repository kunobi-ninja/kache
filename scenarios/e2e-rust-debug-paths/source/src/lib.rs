// A library crate in the workspace. Its debug info is compiled by kache and
// linked into the binary, so the executable carries this crate's remapped
// `comp_dir` (the workspace target: /proc/self/cwd on Linux, /kache/workspace
// elsewhere) alongside the itoa dependency's /kache/.cargo comp_dir. The e2e
// harness asserts both landed (kunobi-ninja/kache#480, #485). Unlike the final
// binary compile (which kache does not remap), a library crate's DWARF IS
// remapped and IS embedded into the executable on Linux.

/// Format `n + 41` via the itoa dependency, exercising both this crate's own
/// debug info and the dependency's.
pub fn format_bumped(n: u64) -> String {
    let mut buf = itoa::Buffer::new();
    buf.format(n + 41).to_string()
}
