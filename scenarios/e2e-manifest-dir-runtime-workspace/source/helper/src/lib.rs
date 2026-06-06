#[inline(never)]
pub fn manifest_dir() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}
