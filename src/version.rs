/// Build version: CI sets KACHE_VERSION from the git tag, local builds use
/// Cargo.toml. Only the leading 'v' is stripped, so a prerelease suffix flows
/// through verbatim (release-candidates publish to crates.io, so the tag — and
/// thus --version — carries the full version, e.g. "v0.5.0-rc.4" -> "0.5.0-rc.4").
pub const VERSION: &str = {
    const RAW: &str = match option_env!("KACHE_VERSION") {
        Some(v) => v,
        None => env!("CARGO_PKG_VERSION"),
    };
    let b = RAW.as_bytes();
    if b.len() > 1 && b[0] == b'v' {
        // SAFETY: removing a leading ASCII 'v' preserves UTF-8 validity
        unsafe { core::str::from_utf8_unchecked(b.split_at(1).1) }
    } else {
        RAW
    }
};
