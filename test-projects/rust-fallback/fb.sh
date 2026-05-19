#!/bin/sh
# kache-e2e fallback wrapper for the `rust-fallback` fixture.
#
# kache invokes a configured fallback as `<this> <compiler> <args...>`
# whenever it declines to cache a compile (here: the user-facing
# executable). We make that delegation observable by appending
# `--cfg fallback_was_used` to the real crate compile — `src/main.rs`
# reads it with `cfg!`, so the built binary's own output reveals
# whether the fallback ran.
#
# The `--cfg` is added ONLY to the fixture crate's own compile
# (`--crate-name rust_fallback`); rustc `--print` / `-vV` capability
# probes are exec'd untouched, so cargo's view of the toolchain — and
# its fingerprint — stay byte-identical to a normal build.
for arg in "$@"; do
    if [ "$arg" = "rust_fallback" ]; then
        exec "$@" --cfg fallback_was_used
    fi
done
exec "$@"
