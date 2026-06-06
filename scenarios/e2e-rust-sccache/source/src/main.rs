//! e2e fixture: proves kache's `KACHE_FALLBACK` delegation composes
//! with a *real* sccache.
//!
//! kache never caches a user-facing executable, so building this
//! `bin` always takes the passthrough path — and with `KACHE_FALLBACK`
//! pointing at `fb-sccache.sh`, passthrough delegates the compile to
//! `sccache rustc <args>` (sccache's own wrapper invocation form).
//!
//! `fb-sccache.sh` appends `--cfg via_sccache` to that compile, so the
//! `cfg!` below records — at compile time — whether the build really
//! travelled kache → fallback → sccache → rustc. If the delegation
//! breaks, rustc runs without the flag and the binary prints the
//! other line, making this a falsifiable end-to-end check.
fn main() {
    if cfg!(via_sccache) {
        println!("rust-sccache: via sccache");
    } else {
        println!("rust-sccache: no sccache");
    }
}
