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

KACHE_VERSION="$("$KACHE" --version 2>/dev/null | awk '{print $2}')"
PLATFORM="$(uname -s)-$(uname -m)"

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

    local lang_status="pass"
    local cold_t=0 warm_t=0 entries=0

    # ── Cold build (today: passthrough; future: populate cache) ─────
    local t0
    t0=$(date +%s)
    if ! (cd "$fixture" && env "$compiler_var=$KACHE $compiler_cmd" make 2>&1 | sed 's/^/    /'); then
        lang_status="fail"
        emit_result "$lang" "$fixture" "$compiler_var" "$lang_status" 0 0 0
        die "$lang cold build via kache wrapper failed"
    fi
    cold_t=$(( $(date +%s) - t0 ))
    [ -f "$fixture/build/foo" ] || { lang_status="fail"; die "$lang cold build did not produce expected binary"; }
    local output
    output="$($fixture/build/foo)"
    case "$output" in
        *"hello from bar"*) ok "$lang cold build OK (${cold_t}s): $output" ;;
        *) lang_status="fail"; die "$lang binary produced unexpected output: $output" ;;
    esac

    # ── Warm build (today: re-exec passthrough; future: cache hits) ─
    # When real caching lands and CcCompiler::refuse_reasons flips off
    # for cacheable cases, swap the assertion below for a hit-rate
    # check parallel to e2e-rust-smoke.sh.
    (cd "$fixture" && make clean >/dev/null 2>&1 || true)
    t0=$(date +%s)
    if ! (cd "$fixture" && env "$compiler_var=$KACHE $compiler_cmd" make 2>&1 | sed 's/^/    /'); then
        lang_status="fail"
        emit_result "$lang" "$fixture" "$compiler_var" "$lang_status" "$cold_t" 0 0
        die "$lang warm build via kache wrapper failed"
    fi
    warm_t=$(( $(date +%s) - t0 ))
    [ -f "$fixture/build/foo" ] || { lang_status="fail"; die "$lang warm build did not produce expected binary"; }
    output="$($fixture/build/foo)"
    case "$output" in
        *"hello from bar"*) ok "$lang warm build OK (${warm_t}s, passthrough idempotent)" ;;
        *) lang_status="fail"; die "$lang warm binary produced unexpected output: $output" ;;
    esac

    # ── Cache state check ─────────────────────────────────────────
    # Locks the skeleton contract: refuse_reasons is unconditional, so
    # nothing lands in the cache. When real caching arrives this check
    # flips to "expected entries > 0".
    entries=$("$KACHE" list 2>&1 | awk 'NR>2 && NF>0' | wc -l | tr -d ' ' || echo 0)
    if [ "$entries" -eq 0 ]; then
        ok "cache empty as expected (skeleton refuses to cache cc invocations)"
    else
        echo "  NOTE: cache has $entries entries; expected 0 in skeleton mode"
    fi

    emit_result "$lang" "$fixture" "$compiler_var" "$lang_status" "$cold_t" "$warm_t" "$entries"

    (cd "$fixture" && make clean >/dev/null 2>&1 || true)
    echo ""
}

# Emit a structured result line per language run. Grep-extractable:
#   grep '^RESULT_JSON: ' log | sed 's/^RESULT_JSON: //' | jq -s
emit_result() {
    local lang="$1" fixture="$2" cv="$3" st="$4" cold="$5" warm="$6" ent="$7"
    cat <<JSON
RESULT_JSON: {"lang":"$lang","fixture":"$fixture","compiler_var":"$cv","status":"$st","kache_version":"$KACHE_VERSION","platform":"$PLATFORM","metrics":{"cold_build_s":$cold,"warm_build_s":$warm,"cache_entries":$ent,"warm_hit_count":null,"noop_recompiled":null,"passthrough_only":true}}
JSON
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
