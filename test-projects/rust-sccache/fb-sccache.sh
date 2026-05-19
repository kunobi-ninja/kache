#!/bin/sh
# kache-e2e fallback wrapper for the `rust-sccache` fixture.
#
# kache invokes a configured fallback as `<this> <compiler> <args...>`
# whenever it declines to cache a compile (here: the user-facing
# executable). We hand that compile to the REAL sccache: `exec sccache
# <compiler> <args>` is sccache's own `<wrapper> <compiler>` form —
# byte-identical to running with `RUSTC_WRAPPER=sccache`.
#
# `--cfg via_sccache` is appended ONLY to the fixture crate's own
# compile (`--crate-name rust_sccache`), so `[verify]` can confirm the
# build genuinely travelled kache -> fallback -> sccache. rustc
# `--print` / `-vV` capability probes are handed to sccache untouched
# (sccache execs them through transparently).
for arg in "$@"; do
    if [ "$arg" = "rust_sccache" ]; then
        exec sccache "$@" --cfg via_sccache
    fi
done
exec sccache "$@"
