#!/usr/bin/env bash
# Proves the `KACHE_FALLBACK` wrapper genuinely delegates to — and is
# served by — a real sccache.
#
# The `rust-sccache` e2e fixture proves the *executable* passthrough
# composes with sccache. But sccache does not cache `--crate-type bin`
# compiles, so that fixture alone cannot show the fallback adding any
# caching. This check covers the path that does: a *library* (`rlib`)
# compile that kache passes through. With `KACHE_FALLBACK=sccache`,
# kache hands the excluded rlib to sccache, which caches it — so a
# clean rebuild is an sccache cache hit.
#
# Usage: scripts/sccache-fallback-check.sh <path-to-kache-binary>
#
# Exit 0 = pass, OR skipped (sccache not installed).
# Exit 1 = the fallback did not reach sccache, or sccache did not
#          serve the rlib from cache on the rebuild.
set -euo pipefail

kache="${1:?usage: sccache-fallback-check.sh <path-to-kache>}"
if ! command -v sccache >/dev/null 2>&1; then
    echo "sccache-check: sccache not on PATH — skipping"
    exit 0
fi
# Absolutize: the build runs from a tempdir, so a relative kache path
# (e.g. ./target/release/kache) would no longer resolve.
kache="$(cd "$(dirname "$kache")" && pwd)/$(basename "$kache")"

work="$(mktemp -d)"
cleanup() {
    "$kache" daemon stop >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

# A library crate whose source is unique to this run, so build #1 is a
# guaranteed sccache miss regardless of any pre-warmed sccache cache.
marker="kache_sccache_probe_$(date +%s)_${RANDOM}"
mkdir -p "$work/src"
cat > "$work/Cargo.toml" <<EOF
[package]
name = "sccache_fallback_probe"
version = "0.1.0"
edition = "2021"
[workspace]
[lib]
EOF
printf 'pub fn %s() -> u32 { 42 }\n' "$marker" > "$work/src/lib.rs"
# kache excludes this source, so the rlib compile is passed through to
# the configured fallback (sccache) instead of cached by kache itself.
cat > "$work/.kache.toml" <<EOF
[cache]
exclude = ["src/lib.rs"]
EOF

cd "$work"
export RUSTC_WRAPPER="$kache"
export KACHE_FALLBACK="sccache"
export KACHE_CACHE_DIR="$work/.kache-cache"
export CARGO_INCREMENTAL=0

sccache --zero-stats >/dev/null

echo "sccache-check: build #1 (expect an sccache miss)"
cargo build --quiet

echo "sccache-check: build #2 after clean (expect an sccache hit)"
rm -rf target
cargo build --quiet

stats="$(sccache --show-stats --stats-format json)"
read_count() {  # $1 = cache_hits | cache_misses
    printf '%s' "$stats" | python3 -c \
        "import json,sys; print(json.load(sys.stdin)['stats']['$1']['counts'].get('Rust',0))"
}
misses="$(read_count cache_misses)"
hits="$(read_count cache_hits)"
echo "sccache-check: sccache Rust cache — misses=$misses hits=$hits"

if [ "$misses" -lt 1 ]; then
    echo "sccache-check: FAIL — build #1 never reached sccache as a cacheable"
    echo "  compile. kache should pass the excluded rlib through to KACHE_FALLBACK."
    exit 1
fi
if [ "$hits" -lt 1 ]; then
    echo "sccache-check: FAIL — build #2 was not served from sccache's cache."
    echo "  The fallback ran but sccache did not cache/restore the rlib."
    exit 1
fi
echo "sccache-check: PASS — kache delegated the rlib to sccache and the rebuild hit"
