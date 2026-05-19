/// Uses the dependency so `cargo check` actually type-checks against
/// it — exercising metadata-only (`--emit=metadata`) caching.
pub fn answer() -> String {
    let mut buf = itoa::Buffer::new();
    buf.format(42u32).to_string()
}
