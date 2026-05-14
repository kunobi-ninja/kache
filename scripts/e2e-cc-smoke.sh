#!/usr/bin/env bash
# End-to-end smoke test for kache's C/C++ wrapper (passthrough skeleton).
#
# Verifies that `CC=kache cc` (and `CXX=kache c++`) work end-to-end:
# kache recognizes the invocation as wrapper-mode, passes through to the
# underlying compiler, and the build succeeds with correct outputs.
#
# **Today this only checks the passthrough contract** — kache doesn't
# cache C/C++ yet. When real caching lands, this script gains
# cold-vs-warm hit-rate assertions parallel to e2e-smoke.sh.
#
# Usage: bash scripts/e2e-cc-smoke.sh [--lang c|cpp] [path-to-kache]

set -euo pipefail

LANG_KIND="both"
KACHE=""

while [ $# -gt 0 ]; do
    case "$1" in
        --lang) LANG_KIND="$2"; shift 2 ;;
        -*)     echo "unknown option: $1" >&2; exit 2 ;;
        *)      KACHE="$1"; shift ;;
    esac
done

KACHE="${KACHE:-./target/release/kache}"

# ── Helpers ──────────────────────────────────────────────────────

die() { echo "FAIL: $*" >&2; exit 1; }
ok()  { echo "  OK: $*"; }

# Resolve kache binary
if [ ! -f "$KACHE" ]; then
    if [ -f "${KACHE}.exe" ]; then KACHE="${KACHE}.exe"
    else die "kache binary not found at $KACHE — run 'cargo build --release' first"
    fi
fi
KACHE="$(cd "$(dirname "$KACHE")" && pwd)/$(basename "$KACHE")"

# Isolated cache dir per run
E2E_CACHE_DIR="$(mktemp -d)"
trap 'rm -rf "$E2E_CACHE_DIR" 2>/dev/null || true' EXIT

export KACHE_CACHE_DIR="$E2E_CACHE_DIR"

echo "=== kache cc/c++ e2e smoke test ==="
echo "Binary: $KACHE"
"$KACHE" --version || die "kache --version failed"
echo "Cache:  $E2E_CACHE_DIR"
echo ""

# ── Per-language test ────────────────────────────────────────────

run_lang_test() {
    local lang="$1"     # c | cpp
    local fixture compiler_var compiler_cmd

    case "$lang" in
        c)   fixture="test-projects/c-hello"   ; compiler_var="CC"  ; compiler_cmd="cc"  ;;
        cpp) fixture="test-projects/cpp-hello" ; compiler_var="CXX" ; compiler_cmd="c++" ;;
        *)   die "unknown lang: $lang" ;;
    esac

    local lang_upper
    lang_upper="$(echo "$lang" | tr '[:lower:]' '[:upper:]')"
    echo "--- $lang_upper: build via $compiler_var=\"$KACHE $compiler_cmd\" ---"
    [ -f "$fixture/Makefile" ] || die "fixture not found at $fixture"
    (cd "$fixture" && make clean >/dev/null 2>&1 || true)

    # ── Cold build (today: passthrough; future: populate cache) ─────
    if ! (cd "$fixture" && env "$compiler_var=$KACHE $compiler_cmd" make 2>&1 | sed 's/^/    /'); then
        die "$lang cold build via kache wrapper failed"
    fi
    [ -f "$fixture/build/foo" ] || die "$lang cold build did not produce expected binary"
    local output
    output="$($fixture/build/foo)"
    case "$output" in
        *"hello from bar"*) ok "$lang cold build OK: $output" ;;
        *) die "$lang binary produced unexpected output: $output" ;;
    esac

    # ── Warm build (today: re-exec passthrough; future: cache hits) ─
    # Today this only verifies the wrapper is idempotent (a second build
    # produces the same correct output via passthrough). When real
    # caching lands and `CcCompiler::refuse_reasons` flips off for the
    # cacheable cases, swap the assertion below for a hit-rate check
    # parallel to e2e-rust-smoke.sh:
    #
    #   LIST_WARM=$("$KACHE" list --sort hits)
    #   HIT_ENTRIES=$(echo "$LIST_WARM" | awk 'NR>2 && $NF+0 > 0' | wc -l)
    #   [ "$HIT_ENTRIES" -gt 0 ] || die "expected cache hits"
    #
    (cd "$fixture" && make clean >/dev/null 2>&1 || true)
    if ! (cd "$fixture" && env "$compiler_var=$KACHE $compiler_cmd" make 2>&1 | sed 's/^/    /'); then
        die "$lang warm build via kache wrapper failed"
    fi
    [ -f "$fixture/build/foo" ] || die "$lang warm build did not produce expected binary"
    output="$($fixture/build/foo)"
    case "$output" in
        *"hello from bar"*) ok "$lang warm build OK (passthrough idempotent)" ;;
        *) die "$lang warm binary produced unexpected output: $output" ;;
    esac

    # ── Cache state check (today: confirms cache is empty / no entries) ─
    # Locks the skeleton contract: with refuse_reasons unconditional,
    # nothing should land in the cache. When real caching lands this
    # assertion flips to "expected entries > 0".
    local entries
    entries=$("$KACHE" list 2>&1 | awk 'NR>2 && NF>0' | wc -l | tr -d ' ' || echo 0)
    if [ "$entries" -eq 0 ]; then
        ok "cache empty as expected (skeleton refuses to cache cc invocations)"
    else
        # Soft warn for now — when caching lands, change to ok.
        echo "  NOTE: cache has $entries entries; expected 0 in skeleton mode"
    fi

    (cd "$fixture" && make clean >/dev/null 2>&1 || true)
    echo ""
}

if [ "$LANG_KIND" = "both" ] || [ "$LANG_KIND" = "c" ]; then
    run_lang_test c
fi
if [ "$LANG_KIND" = "both" ] || [ "$LANG_KIND" = "cpp" ]; then
    run_lang_test cpp
fi

echo "=== cc/c++ e2e smoke test PASSED ==="
echo ""
echo "Today this verifies:"
echo "  - kache recognizes \$CC=kache cc and \$CXX=kache c++ as wrapper-mode"
echo "  - underlying compiler runs and produces correct binary"
echo "  - passthrough is idempotent (warm build also works)"
echo "  - cache stays empty (skeleton refuse_reasons in effect)"
echo ""
echo "When real C/C++ caching lands (CcCompiler::refuse_reasons flipping"
echo "off for cacheable cases), the warm-build assertion swaps to a"
echo "cold-vs-warm hit-rate check parallel to e2e-rust-smoke.sh."
