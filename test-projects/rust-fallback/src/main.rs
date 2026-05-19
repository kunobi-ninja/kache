//! e2e fixture: proves kache's configured fallback wrapper actually
//! runs.
//!
//! kache never caches a user-facing executable, so building this
//! `bin` always takes the passthrough path — and with `KACHE_FALLBACK`
//! set, passthrough delegates to the fallback wrapper (`fb.sh`)
//! instead of running rustc directly.
//!
//! `fb.sh` appends `--cfg fallback_was_used` to the rustc command it
//! delegates to, so the `cfg!` below records — at compile time —
//! whether the fallback ran. If the fallback wiring breaks, rustc is
//! invoked without the flag and the binary prints the other line,
//! making this a falsifiable end-to-end check.
fn main() {
    if cfg!(fallback_was_used) {
        println!("rust-fallback: via fallback");
    } else {
        println!("rust-fallback: no fallback");
    }
}
