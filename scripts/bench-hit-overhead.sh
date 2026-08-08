#!/usr/bin/env bash
# Benchmark kache per-hit key-computation overhead on a synthetic workspace,
# with the dep-info memo OFF vs ON (same binary, back-to-back).
#
# Layout: one `bigdep` crate producing a multi-MB rlib + N leaf crates that
# each depend on it. A warm restore of the leaves exercises the per-hit key
# path (dep-info pre-pass + source hashing + extern rlib hashing) N times,
# which is exactly the shape of the reported overhead (kache report shows
# "Hit overhead: avg <big> key + ~0ms lookup + ~3ms restore").
#
# Phases:
#   cold     — populate the store (and the dep-info memo, when enabled)
#   warm-off — rm -rf target; KACHE_DEP_INFO_MEMO=0 (memo bypassed)
#   warm-on  — rm -rf target; memo enabled (default)
#
# Usage: scripts/bench-hit-overhead.sh <kache-binary> [n-leaves] [out-dir]
# Emits per-phase events copies (<out>/events-<phase>.jsonl), a kache report
# JSON over the whole run, and a key_ms + phase-attribution summary from the
# `[key-timing:...]` debug lines (requires KACHE_LOG=kache=debug, set below).
set -euo pipefail

KACHE_BIN=${1:?usage: bench-hit-overhead.sh <kache-binary> [n-leaves] [out-dir]}
N=${2:-100}
OUT=${3:-/tmp/kache-hit-bench}
WS="$OUT/ws"
CACHE="$OUT/cache"

rm -rf "$WS" "$CACHE"
mkdir -p "$WS" "$CACHE" "$OUT"

# ── bigdep: multi-MB rlib (metadata-heavy: many pub fns with long const strings) ──
mkdir -p "$WS/bigdep/src"
cat > "$WS/bigdep/Cargo.toml" <<EOF
[package]
name = "bigdep"
version = "0.1.0"
edition = "2021"
EOF
{
  echo "// generated: metadata-heavy crate to fatten the rlib"
  for i in $(seq 0 2999); do
    printf 'pub fn f_%d() -> &%s str { "%s" }\n' "$i" "'static" \
      "$(printf 'x%.0s' $(seq 1 400))_$i"
  done
} > "$WS/bigdep/src/lib.rs"

# ── N leaf crates, each depending on bigdep ──
MEMBERS='"bigdep"'
for i in $(seq -w 0 $((N - 1))); do
  d="$WS/leaf$i"
  mkdir -p "$d/src"
  cat > "$d/Cargo.toml" <<EOF
[package]
name = "leaf$i"
version = "0.1.0"
edition = "2021"

[dependencies]
bigdep = { path = "../bigdep" }
EOF
  cat > "$d/src/lib.rs" <<EOF
pub fn go() -> &'static str {
    bigdep::f_$((10#$i))()
}
EOF
  MEMBERS="$MEMBERS, \"leaf$i\""
done
cat > "$WS/Cargo.toml" <<EOF
[workspace]
members = [$MEMBERS]
resolver = "2"
EOF

export RUSTC_WRAPPER="$KACHE_BIN"
export KACHE_CACHE_DIR="$CACHE"
export KACHE_LOG="kache=debug"

cd "$WS"
snapshot() { cp "$CACHE/events.jsonl" "$OUT/events-$1.jsonl" 2>/dev/null || : > "$OUT/events-$1.jsonl"; }

echo "── cold build (populate store + memo) ──"
cargo build 2> "$OUT/cold.log"
ls -l target/debug/libbigdep*.rlib | awk '{print "bigdep rlib size:", $5}'
snapshot cold

echo "── warm build, memo OFF ──"
rm -rf target
KACHE_DEP_INFO_MEMO=0 cargo build 2> "$OUT/warm-off.log"
snapshot warm-off

echo "── warm build, memo ON ──"
rm -rf target
cargo build 2> "$OUT/warm-on.log"
snapshot warm-on

"$KACHE_BIN" report --format json --since 15m -o "$OUT/report.json" >/dev/null || true

python3 - "$OUT" <<'PY'
import json, re, statistics, sys
out = sys.argv[1]

def load(phase):
    return [json.loads(l) for l in open(f"{out}/events-{phase}.jsonl") if l.strip()]

cold = load("cold")
off = load("warm-off")[len(cold):]
on = load("warm-on")[len(cold) + len(off):]

def key_stats(events, label):
    hits = [e for e in events if e.get("result") in ("hit", "local_hit")
            and e.get("crate_name", "").startswith("leaf")]
    vals = [e.get("key_ms", 0) for e in hits]
    if vals:
        print(f"{label}: n={len(vals)} avg_key_ms={statistics.mean(vals):.1f} "
              f"p50={statistics.median(vals):.1f} max={max(vals)}")
    return statistics.mean(vals) if vals else None

print("── per-event key_ms on warm leaf hits ──")
a = key_stats(off, "memo OFF")
b = key_stats(on, "memo ON ")
if a and b:
    print(f"ratio: {a / b:.1f}x  ({(1 - b / a) * 100:.0f}% reduction)")

pat = re.compile(
    r"\[key-timing:(\S+)\] dep_info_ms=(\d+) source_hash_ms=(\d+) n_sources=(\d+) "
    r"extern_hash_ms=(\d+) n_externs=(\d+) memo=(\w+)")

def phases(log, label):
    rows = [m for m in (pat.search(l) for l in open(f"{out}/{log}", errors="replace"))
            if m and m.group(1).startswith("leaf")]
    if not rows:
        print(f"{label}: no [key-timing:] lines (KACHE_LOG filtered?)")
        return
    dep = [int(m.group(2)) for m in rows]
    memo = {}
    for m in rows:
        memo[m.group(7)] = memo.get(m.group(7), 0) + 1
    print(f"{label}: dep_info_ms avg={statistics.mean(dep):.1f} p50={statistics.median(dep):.1f} "
          f"max={max(dep)} memo_states={memo}")

print("── phase attribution ──")
phases("warm-off.log", "memo OFF")
phases("warm-on.log", "memo ON ")
PY
