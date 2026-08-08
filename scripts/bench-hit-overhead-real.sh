#!/usr/bin/env bash
# Real-workload companion to bench-hit-overhead.sh: build an existing cargo
# project (its full dependency graph, proc macros included) through the kache
# wrapper with a scratch store, then compare warm-hit key overhead with the
# dep-info memo OFF vs ON. Same binary, back-to-back, per-phase event slices.
#
# Usage: scripts/bench-hit-overhead-real.sh <kache-binary> <project-dir> [out-dir]
set -euo pipefail

KACHE_BIN=${1:?usage: bench-hit-overhead-real.sh <kache-binary> <project-dir> [out-dir]}
PROJECT=${2:?usage: bench-hit-overhead-real.sh <kache-binary> <project-dir> [out-dir]}
OUT=${3:-/tmp/kache-hit-bench-real}
CACHE="$OUT/cache"
TARGET="$OUT/target"

rm -rf "$CACHE" "$TARGET"
mkdir -p "$OUT" "$CACHE"

export RUSTC_WRAPPER="$KACHE_BIN"
export KACHE_CACHE_DIR="$CACHE"
export CARGO_TARGET_DIR="$TARGET"

cd "$PROJECT"
snapshot() { cp "$CACHE/events.jsonl" "$OUT/events-$1.jsonl" 2>/dev/null || : > "$OUT/events-$1.jsonl"; }

echo "── cold build (populate store + memo) ──"
cargo build 2> "$OUT/cold.log" || { tail -5 "$OUT/cold.log"; exit 1; }
snapshot cold

echo "── warm build, memo OFF ──"
rm -rf "$TARGET"
KACHE_DEP_INFO_MEMO=0 cargo build 2> "$OUT/warm-off.log" || { tail -5 "$OUT/warm-off.log"; exit 1; }
snapshot warm-off

echo "── warm build, memo ON ──"
rm -rf "$TARGET"
cargo build 2> "$OUT/warm-on.log" || { tail -5 "$OUT/warm-on.log"; exit 1; }
snapshot warm-on

python3 - "$OUT" <<'PY'
import json, statistics, sys
out = sys.argv[1]

def load(phase):
    return [json.loads(l) for l in open(f"{out}/events-{phase}.jsonl") if l.strip()]

cold = load("cold")
off = load("warm-off")[len(cold):]
on = load("warm-on")[len(cold) + len(off):]

def stats(events, label):
    hits = [e for e in events if e.get("result") in ("hit", "local_hit")]
    misses = [e for e in events if e.get("result") in ("miss", "dup")]
    vals = [e.get("key_ms", 0) for e in hits]
    if not vals:
        print(f"{label}: no hits")
        return None
    print(f"{label}: hits={len(hits)} misses={len(misses)} "
          f"avg_key_ms={statistics.mean(vals):.1f} p50={statistics.median(vals):.1f} "
          f"p90={sorted(vals)[int(len(vals) * 0.9)]} max={max(vals)} "
          f"total_key_s={sum(vals) / 1000:.1f}")
    return statistics.mean(vals)

print("── warm-hit key_ms, real dependency graph ──")
a = stats(off, "memo OFF")
b = stats(on, "memo ON ")
if a and b:
    print(f"ratio: {a / b:.1f}x  ({(1 - b / a) * 100:.0f}% reduction)")
PY
