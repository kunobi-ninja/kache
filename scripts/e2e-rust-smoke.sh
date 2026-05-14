#!/usr/bin/env bash
# End-to-end smoke test for kache's rustc wrapper.
#
# Verifies the full lifecycle: build kache, configure as RUSTC_WRAPPER,
# cold-build a fixture project (populate cache), clean, warm-build
# (cache hits), validate hit count > 0, third build is no-op.
#
# Companion: scripts/e2e-cc-smoke.sh covers the C/C++ wrapper path
# (passthrough only today; cold-vs-warm assertions land when real
# C/C++ caching arrives).
#
# Runs on Linux, macOS, and Windows (Git Bash / MSYS2).
# Usage: bash scripts/e2e-rust-smoke.sh [path-to-kache-binary]

set -euo pipefail

KACHE="${1:-./target/release/kache}"
FIXTURE_DIR="test-projects/multi-dep"
E2E_CACHE_DIR=""

# ── Helpers ──────────────────────────────────────────────────────

die() { echo "FAIL: $*" >&2; exit 1; }
ok()  { echo "  OK: $*"; }

cleanup() {
    # Stop daemon FIRST (releases DB lock) before removing cache dir
    if [ -n "${E2E_CACHE_DIR:-}" ] && [ -n "${KACHE:-}" ]; then
        KACHE_CACHE_DIR="$E2E_CACHE_DIR" "$KACHE" daemon stop 2>/dev/null || true
        sleep 1
    fi
    rm -rf "$FIXTURE_DIR/target" 2>/dev/null || true
    rm -rf "${E2E_CACHE_DIR:-}" 2>/dev/null || true
}
trap cleanup EXIT

# ── Setup ────────────────────────────────────────────────────────

echo "=== kache e2e smoke test ==="
echo ""

# Resolve kache binary to absolute path
if [ ! -f "$KACHE" ]; then
    if [ -f "${KACHE}.exe" ]; then
        KACHE="${KACHE}.exe"
    else
        die "kache binary not found at $KACHE — run 'cargo build --release' first"
    fi
fi
KACHE="$(cd "$(dirname "$KACHE")" && pwd)/$(basename "$KACHE")"

echo "Binary: $KACHE"
"$KACHE" --version || die "kache --version failed"

# Isolated cache directory
E2E_CACHE_DIR="$(mktemp -d)"
export KACHE_CACHE_DIR="$E2E_CACHE_DIR"
export RUSTC_WRAPPER="$KACHE"

echo "Cache:  $E2E_CACHE_DIR"

[ -f "$FIXTURE_DIR/Cargo.toml" ] || die "fixture not found at $FIXTURE_DIR"
rm -rf "$FIXTURE_DIR/target"
echo ""

# ── Test-envelope state (populated as we go) ─────────────────────
# Wall-clock times + test status are owned by this script. Cache
# state on disk (kache list) is checked for assertions but NOT
# emitted in the JSON envelope — kache report is the authoritative
# source for hit rate / timing / cache activity.
PLATFORM="$(uname -s)-$(uname -m)"
STATUS="pass"
COLD_T=0
WARM_T=0
NOOP_RECOMPILED="false"
ENTRY_COUNT=0   # for assertion only
HIT_ENTRIES=0   # for assertion only

# ── 1. Doctor baseline ──────────────────────────────────────────

echo "--- Step 1: doctor (baseline, no daemon) ---"
"$KACHE" doctor 2>&1 || true
ok "doctor ran"
echo ""

# ── 2. Cold build (populates cache) ─────────────────────────────

echo "--- Step 2: cold build (populate cache) ---"
T0=$(date +%s)
(cd "$FIXTURE_DIR" && cargo build --release 2>&1) || { STATUS="fail"; die "cold build failed"; }
COLD_T=$(( $(date +%s) - T0 ))
ok "cold build succeeded (${COLD_T}s)"
echo ""

# ── 3. Verify cache populated ───────────────────────────────────

echo "--- Step 3: verify cache populated ---"
LIST_COLD=$("$KACHE" list 2>&1) || true
echo "$LIST_COLD" | head -5

# Count data rows (lines with content after the header separator)
ENTRY_COUNT=$(echo "$LIST_COLD" | grep -cE '^\S' | tail -1 || echo "0")
# Subtract header lines (title + separator = 2)
ENTRY_COUNT=$((ENTRY_COUNT > 2 ? ENTRY_COUNT - 2 : 0))

if [ "$ENTRY_COUNT" -eq 0 ]; then
    die "expected cached entries after cold build, got 0"
fi
ok "$ENTRY_COUNT cache entries after cold build"
echo ""

# ── 4. Clean + warm build (cache hits) ──────────────────────────

echo "--- Step 4: clean + warm build (cache hits) ---"
rm -rf "$FIXTURE_DIR/target"
T0=$(date +%s)
WARM_OUTPUT=$(cd "$FIXTURE_DIR" && cargo build --release 2>&1) || { STATUS="fail"; die "warm build failed"; }
WARM_T=$(( $(date +%s) - T0 ))
echo "$WARM_OUTPUT"
ok "warm build succeeded (${WARM_T}s)"
echo ""

# ── 5. Verify cache hits ────────────────────────────────────────

echo "--- Step 5: verify cache hits ---"
LIST_WARM=$("$KACHE" list --sort hits 2>&1) || true
echo "$LIST_WARM" | head -15

# Count entries with hits > 0 (lines containing a number > 0 in the Hits column)
HIT_ENTRIES=$(echo "$LIST_WARM" | awk 'NR>2 && $NF+0 > 0' | wc -l || echo "0")
HIT_ENTRIES=$(echo "$HIT_ENTRIES" | tr -d ' ')

if [ "$HIT_ENTRIES" -gt 0 ]; then
    ok "$HIT_ENTRIES entries have cache hits"
else
    die "expected cache hits on warm build, got 0 — cache restore is not working"
fi
echo ""

# ── 6. No-op build (nothing to recompile) ───────────────────────

echo "--- Step 6: no-op build (nothing to recompile) ---"
BUILD3_OUT=$(cd "$FIXTURE_DIR" && cargo build --release 2>&1)
echo "$BUILD3_OUT"
if echo "$BUILD3_OUT" | grep -q "Compiling"; then
    NOOP_RECOMPILED="true"
    STATUS="fail"
    die "third build recompiled crates — expected no-op"
fi
ok "no recompilation on third build"
echo ""

# ── 7. Doctor with daemon ───────────────────────────────────────

echo "--- Step 7: doctor (final) ---"
"$KACHE" doctor 2>&1 || true
ok "doctor ran"
echo ""

# ── Structured result line ───────────────────────────────────────
#
# Two parts per result:
#   - `test`: fields owned by this script (lang/fixture/wall times/etc).
#     kache doesn't know what fixture we're building or whether the test
#     considers itself passing.
#   - `kache_report`: the full output of `kache report --format json` —
#     the authoritative source for hit-rate, timing breakdowns, transfer
#     counts, etc. Single source of truth, maintained alongside the
#     report module instead of re-parsed here.
#
# One `RESULT_JSON: {...}` line per test run, grep-extractable:
#   grep '^RESULT_JSON: ' log | sed 's/^RESULT_JSON: //' | jq -s

KACHE_REPORT_JSON="$("$KACHE" report --format json --since 1h 2>/dev/null || echo '{}')"
TEST_ENVELOPE_JSON=$(jq -n \
    --arg lang "rust" \
    --arg fixture "$FIXTURE_DIR" \
    --arg compiler_var "RUSTC_WRAPPER" \
    --arg status "$STATUS" \
    --arg platform "$PLATFORM" \
    --argjson cold "$COLD_T" \
    --argjson warm "$WARM_T" \
    --argjson noop "$NOOP_RECOMPILED" \
    '{lang: $lang, fixture: $fixture, compiler_var: $compiler_var, status: $status,
      platform: $platform, cold_build_wall_s: $cold, warm_build_wall_s: $warm,
      noop_recompiled: $noop, passthrough_only: false}')

RESULT_JSON_OBJ=$(jq -nc \
    --argjson test "$TEST_ENVELOPE_JSON" \
    --argjson kache "$KACHE_REPORT_JSON" \
    '{test: $test, kache_report: $kache}')

echo "RESULT_JSON: $RESULT_JSON_OBJ"

# ── Done ─────────────────────────────────────────────────────────

echo "=== e2e smoke test PASSED ==="
