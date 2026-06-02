//! `e2e-fallback` — cross-platform `KACHE_FALLBACK` wrapper for the harness.
//!
//! kache invokes a configured fallback as `<fallback> <compiler> <args...>`
//! whenever it declines to cache a compile. The fixtures used to point
//! `KACHE_FALLBACK` at `#!/bin/sh` scripts (`fb.sh`, `fb-sccache.sh`), but a
//! `.sh` can't be exec'd on Windows (`CreateProcess` → os error 193), and
//! `.bat`/`.cmd` are not a safe substitute. This one binary replaces both
//! scripts on every platform; the harness exposes its path as `$FALLBACK`.
//!
//! Behaviour is driven by env vars set in the fixture's `[env]` (mirroring what
//! the shell wrappers did):
//!   * `E2E_FALLBACK_DELEGATE` — optional program to run *instead of* the
//!     compiler (e.g. `sccache`, replicating `exec sccache "$@"`); unset → run
//!     the compiler directly (replicating `exec "$@"`).
//!   * `E2E_FALLBACK_CRATE` — the `--crate-name` value that triggers the marker
//!     cfg; only that crate's compile gets it (probe/`-vV` invocations don't).
//!   * `E2E_FALLBACK_CFG` — cfg appended as `--cfg <value>` for that crate, so
//!     the built binary's own output proves the compile travelled through the
//!     fallback.

use std::process::{Command, exit};

fn main() {
    // argv after argv0: `<compiler> <args...>`.
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("e2e-fallback: missing compiler argument");
        exit(2);
    }

    // Append `--cfg <CFG>` only when this invocation compiles the marked crate —
    // the crate name appears as a bare arg (the `--crate-name <name>` value),
    // exactly what the old `for arg in "$@"; do [ "$arg" = ... ]` loop matched.
    let krate = std::env::var("E2E_FALLBACK_CRATE").unwrap_or_default();
    let cfg = std::env::var("E2E_FALLBACK_CFG").unwrap_or_default();
    if !krate.is_empty() && !cfg.is_empty() && args.iter().any(|a| a == &krate) {
        args.push("--cfg".into());
        args.push(cfg);
    }

    // Either delegate the whole `<compiler> <args>` line to another wrapper
    // (sccache's own `<wrapper> <compiler>` form), or run the compiler directly.
    let (program, rest) = match std::env::var("E2E_FALLBACK_DELEGATE") {
        Ok(delegate) if !delegate.is_empty() => (delegate, args),
        _ => {
            let compiler = args.remove(0);
            (compiler, args)
        }
    };

    let status = Command::new(&program).args(&rest).status();
    match status {
        Ok(status) => exit(status.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("e2e-fallback: failed to run `{program}`: {e}");
            exit(1);
        }
    }
}
