#!/usr/bin/env bash
# End-to-end smoke test for kache's rustc wrapper.
#
# Verifies the full lifecycle per fixture: cold-build (populate cache),
# clean, warm-build (cache hits), no-op build (nothing to recompile).
# Each fixture runs in its own isolated cache dir so the embedded
# `kache report --format json` covers ONLY that fixture's events.
#
# Companion: scripts/e2e-cc-smoke.sh covers the C/C++ wrapper path
# (passthrough only today; cold-vs-warm assertions land when real
# C/C++ caching arrives).
#
# Runs on Linux, macOS, and Windows (Git Bash / MSYS2).
# Usage: bash scripts/e2e-rust-smoke.sh [path-to-kache-binary]

set -euo pipefail

KACHE="${1:-./target/release/kache}"

# ── Helpers ──────────────────────────────────────────────────────

die() { echo "FAIL: $*" >&2; exit 1; }
ok()  { echo "  OK: $*"; }

# Resolve kache binary to absolute path
if [ ! -f "$KACHE" ]; then
    if [ -f "${KACHE}.exe" ]; then KACHE="${KACHE}.exe"
    else die "kache binary not found at $KACHE — run 'cargo build --release' first"
    fi
fi
KACHE="$(cd "$(dirname "$KACHE")" && pwd)/$(basename "$KACHE")"

# Each fixture mints its own KACHE_CACHE_DIR so the embedded kache
# report covers only its events. CACHE_DIRS tracks them all so the
# trap can sweep them on EXIT.
CACHE_DIRS=()
FIXTURE_DIRS_TO_CLEAN=()
cleanup_all() {
    # Stop any per-fixture daemon FIRST (releases DB locks) before the
    # mktemp dirs get removed.
    for d in "${CACHE_DIRS[@]:-}"; do
        [ -n "$d" ] && KACHE_CACHE_DIR="$d" "$KACHE" daemon stop >/dev/null 2>&1 || true
    done
    sleep 1
    for d in "${CACHE_DIRS[@]:-}"; do
        [ -n "$d" ] && rm -rf "$d" 2>/dev/null || true
    done
    for f in "${FIXTURE_DIRS_TO_CLEAN[@]:-}"; do
        [ -n "$f" ] && rm -rf "$f/target" 2>/dev/null || true
    done
}
trap cleanup_all EXIT

PLATFORM="$(uname -s)-$(uname -m)"

echo "=== kache rustc e2e smoke test ==="
echo "Binary: $KACHE"
"$KACHE" --version || die "kache --version failed"
echo ""

# ── Per-fixture test ────────────────────────────────────────────

run_fixture_test() {
    local fixture="$1"

    echo "--- $fixture: full lifecycle (cold → warm → no-op) ---"
    [ -f "$fixture/Cargo.toml" ] || die "fixture not found at $fixture"

    # Per-fixture isolated cache dir.
    local cache_dir
    cache_dir="$(mktemp -d)"
    CACHE_DIRS+=("$cache_dir")
    FIXTURE_DIRS_TO_CLEAN+=("$fixture")
    export KACHE_CACHE_DIR="$cache_dir"
    export RUSTC_WRAPPER="$KACHE"
    # Also wire CC / CXX through kache so any C/C++ invocations a Rust
    # crate triggers via build.rs (e.g. -sys crates, the `cc` crate)
    # flow through the cc-family wrapper. Pure-rust fixtures simply
    # never invoke them — harmless.
    export CC="$KACHE cc"
    export CXX="$KACHE c++"
    echo "    cache: $cache_dir"

    # Defensive: stop any inherited daemon before starting.
    "$KACHE" daemon stop >/dev/null 2>&1 || true
    rm -rf "$fixture/target"

    local status="pass"
    local cold_t=0 warm_t=0
    local noop_recompiled="false"
    local entry_count=0 hit_entries=0

    # ── 1. Doctor baseline ───────────────────────────────────────
    "$KACHE" doctor 2>&1 || true
    ok "doctor ran"

    # ── 2. Cold build (populate cache) ───────────────────────────
    local t0
    t0=$(date +%s)
    if ! (cd "$fixture" && cargo build --release 2>&1); then
        status="fail"
        emit_result "$fixture" "$status" "$cold_t" "$warm_t" "$noop_recompiled"
        die "cold build failed"
    fi
    cold_t=$(( $(date +%s) - t0 ))
    ok "cold build succeeded (${cold_t}s)"

    # ── 3. Verify cache populated (assertion only — kache_report
    #       carries the authoritative count per its time window) ───
    local list_cold
    list_cold="$("$KACHE" list 2>&1)" || true
    entry_count=$(echo "$list_cold" | grep -cE '^\S' || echo "0")
    entry_count=$((entry_count > 2 ? entry_count - 2 : 0))
    [ "$entry_count" -gt 0 ] || { status="fail"; die "expected cached entries after cold build, got 0"; }
    ok "$entry_count cache entries after cold build"

    # ── 4. Clean + warm build (cache hits) ───────────────────────
    rm -rf "$fixture/target"
    t0=$(date +%s)
    if ! (cd "$fixture" && cargo build --release 2>&1); then
        status="fail"
        emit_result "$fixture" "$status" "$cold_t" "$warm_t" "$noop_recompiled"
        die "warm build failed"
    fi
    warm_t=$(( $(date +%s) - t0 ))
    ok "warm build succeeded (${warm_t}s)"

    # ── 5. Verify cache hits (assertion only) ────────────────────
    local list_warm
    list_warm="$("$KACHE" list --sort hits 2>&1)" || true
    hit_entries=$(echo "$list_warm" | awk 'NR>2 && $NF+0 > 0' | wc -l | tr -d ' ' || echo "0")
    if [ "$hit_entries" -gt 0 ]; then
        ok "$hit_entries entries have cache hits"
    else
        status="fail"
        emit_result "$fixture" "$status" "$cold_t" "$warm_t" "$noop_recompiled"
        die "expected cache hits on warm build, got 0 — cache restore is not working"
    fi

    # ── 6. No-op build (nothing to recompile) ────────────────────
    local build3_out
    build3_out="$(cd "$fixture" && cargo build --release 2>&1)"
    if echo "$build3_out" | grep -q "Compiling"; then
        noop_recompiled="true"
        status="fail"
        emit_result "$fixture" "$status" "$cold_t" "$warm_t" "$noop_recompiled"
        die "third build recompiled crates — expected no-op"
    fi
    ok "no recompilation on third build"

    emit_result "$fixture" "$status" "$cold_t" "$warm_t" "$noop_recompiled"

    # Stop the per-fixture daemon so it releases this cache dir's
    # locks before the next fixture (or the trap on EXIT).
    "$KACHE" daemon stop >/dev/null 2>&1 || true
    rm -rf "$fixture/target"
    echo ""
}

# Emit a structured result line per fixture run.
#
# Two parts:
#   - `test`: lang/fixture/wall times/test-only flags (this script's view)
#   - `kache_report`: full `kache report --format json` (authoritative)
#
# Grep-extractable:
#   grep '^RESULT_JSON: ' log | sed 's/^RESULT_JSON: //' | jq -s
emit_result() {
    local fixture="$1" st="$2" cold="$3" warm="$4" noop="$5"

    local kache_report_json
    kache_report_json="$("$KACHE" report --format json --since 1h 2>/dev/null || echo '{}')"

    local test_json
    test_json=$(jq -n \
        --arg lang "rust" \
        --arg fixture "$fixture" \
        --arg compiler_var "RUSTC_WRAPPER" \
        --arg status "$st" \
        --arg platform "$PLATFORM" \
        --argjson cold "$cold" \
        --argjson warm "$warm" \
        --argjson noop "$noop" \
        '{lang: $lang, fixture: $fixture, compiler_var: $compiler_var, status: $status,
          platform: $platform, cold_build_wall_s: $cold, warm_build_wall_s: $warm,
          noop_recompiled: $noop, passthrough_only: false}')

    local result_obj
    result_obj=$(jq -nc \
        --argjson test "$test_json" \
        --argjson kache "$kache_report_json" \
        '{test: $test, kache_report: $kache}')

    echo "RESULT_JSON: $result_obj"
}

# ── Run fixtures ────────────────────────────────────────────────
# Add more fixtures here as they're created (each gets its own
# isolated cache dir, daemon, and kache_report).
#
# multi-dep:    pure rustc — typical Cargo workspace with serde deps.
# rust-c-ffi:   rust crate with build.rs invoking the `cc` crate;
#               exercises BOTH wrappers (RUSTC_WRAPPER + CC) in a
#               single build, the realistic mixed-language scenario.
run_fixture_test "test-projects/multi-dep"
run_fixture_test "test-projects/rust-c-ffi"

echo "=== rustc e2e smoke test PASSED ==="
