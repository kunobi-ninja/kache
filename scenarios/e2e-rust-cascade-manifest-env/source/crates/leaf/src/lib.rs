/// Cascade root via `env!`. `env!("CARGO_MANIFEST_DIR")` bakes the crate's
/// ABSOLUTE manifest path into the compiled rlib/rmeta as a `&str` constant.
/// Crucially, `--remap-path-prefix` does NOT rewrite `env!` string values
/// (it only remaps span/debuginfo source paths), so this absolute path lands
/// in `leaf`'s output verbatim and differs between two clones at different
/// paths. kache hashes `leaf`'s rmeta BYTES into `mid`'s key, so the leak
/// cascades to `mid` and `app`. This mirrors substrate crates that embed
/// build paths via env.
pub const MANIFEST_DIR: &str = env!("CARGO_MANIFEST_DIR");

pub fn base() -> u32 {
    // Reference the const so it cannot be optimized out of the rlib.
    (MANIFEST_DIR.len() as u32).min(0) + 40
}
