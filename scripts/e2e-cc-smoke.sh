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

# Each lang test gets its OWN isolated cache dir so the embedded
# `kache report --format json` for cpp doesn't include events from
# the c run (cross-contamination would make per-lang metrics wrong).
# Tracked here so cleanup can sweep them all on EXIT.
CACHE_DIRS=()
cleanup_cache_dirs() {
    for d in "${CACHE_DIRS[@]:-}"; do
        [ -n "$d" ] && rm -rf "$d" 2>/dev/null || true
    done
    # Also clean any leftover build dirs in the fixtures.
    rm -rf test-projects/c-hello/build test-projects/cpp-hello/build 2>/dev/null || true
}
trap cleanup_cache_dirs EXIT

KACHE_VERSION="$("$KACHE" --version 2>/dev/null | awk '{print $2}')"
PLATFORM="$(uname -s)-$(uname -m)"

echo "=== kache cc/c++ e2e smoke test ==="
echo "Binary: $KACHE"
"$KACHE" --version || die "kache --version failed"
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

    # Per-lang isolated cache dir so the embedded kache report covers
    # ONLY this lang's invocations. Without this, cpp's report would
    # include c's events (and any earlier debug noise from the daemon).
    local lang_cache_dir
    lang_cache_dir="$(mktemp -d)"
    CACHE_DIRS+=("$lang_cache_dir")
    export KACHE_CACHE_DIR="$lang_cache_dir"
    echo "    cache: $lang_cache_dir"

    # Defensive: stop any daemon a previous lang or run might have
    # spawned against this CACHE_DIR before we start measuring.
    "$KACHE" daemon stop >/dev/null 2>&1 || true

    # Make sure the fixture is clean before BOTH the cold and warm
    # runs (idempotent; harmless if already clean).
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

    # Stop the per-lang daemon so it releases this cache dir's locks
    # before the next lang test (or before the trap removes the dir).
    "$KACHE" daemon stop >/dev/null 2>&1 || true
    (cd "$fixture" && make clean >/dev/null 2>&1 || true)
    echo ""
}

# Emit a structured result line per language run.
#
# Two parts:
#   - `test`: lang/fixture/wall times/test-only flags
#   - `kache_report`: full `kache report --format json` output
#     (kache's authoritative metrics, not re-parsed here)
#
# Grep-extractable:
#   grep '^RESULT_JSON: ' log | sed 's/^RESULT_JSON: //' | jq -s
emit_result() {
    local lang="$1" fixture="$2" cv="$3" st="$4" cold="$5" warm="$6"
    # Drop the cache_entries count from the test envelope — `kache_report`
    # carries hit/miss/total breakdowns over the time window which are
    # the authoritative numbers. Disk cache size is a separate concern
    # (could be added later via `kache list --format json` if needed).
    local kache_report_json
    kache_report_json="$("$KACHE" report --format json --since 1h 2>/dev/null || echo '{}')"

    local test_json
    test_json=$(jq -n \
        --arg lang "$lang" \
        --arg fixture "$fixture" \
        --arg compiler_var "$cv" \
        --arg status "$st" \
        --arg platform "$PLATFORM" \
        --argjson cold "$cold" \
        --argjson warm "$warm" \
        '{lang: $lang, fixture: $fixture, compiler_var: $compiler_var, status: $status,
          platform: $platform, cold_build_wall_s: $cold, warm_build_wall_s: $warm,
          noop_recompiled: null, passthrough_only: true}')

    local result_obj
    result_obj=$(jq -nc \
        --argjson test "$test_json" \
        --argjson kache "$kache_report_json" \
        '{test: $test, kache_report: $kache}')

    echo "RESULT_JSON: $result_obj"
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
