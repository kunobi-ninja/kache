#!/usr/bin/env bash
# End-to-end smoke test for kache.
#
# Verifies the full lifecycle: build kache, configure as RUSTC_WRAPPER,
# cold-build a fixture project (populate cache), clean, warm-build
# (cache hits), and validate stats show the expected behavior.
#
# Runs on Linux, macOS, and Windows (Git Bash / MSYS2).
# Usage: bash scripts/e2e-smoke.sh [path-to-kache-binary]

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

# ── 1. Doctor baseline ──────────────────────────────────────────

echo "--- Step 1: doctor (baseline, no daemon) ---"
"$KACHE" doctor 2>&1 || true
ok "doctor ran"
echo ""

# ── 2. Cold build (populates cache) ─────────────────────────────

echo "--- Step 2: cold build (populate cache) ---"
(cd "$FIXTURE_DIR" && cargo build --release 2>&1) || die "cold build failed"
ok "cold build succeeded"
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
WARM_OUTPUT=$(cd "$FIXTURE_DIR" && cargo build --release 2>&1) || die "warm build failed"
echo "$WARM_OUTPUT"
ok "warm build succeeded"
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
    die "third build recompiled crates — expected no-op"
fi
ok "no recompilation on third build"
echo ""

# ── 7. Doctor with daemon ───────────────────────────────────────

echo "--- Step 7: doctor (final) ---"
"$KACHE" doctor 2>&1 || true
ok "doctor ran"
echo ""

# ── Done ─────────────────────────────────────────────────────────

echo "=== e2e smoke test PASSED ==="
