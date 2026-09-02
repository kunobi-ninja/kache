#!/usr/bin/env bash
# Executable tests for perf-gate-compare.sh.
#
# The comparison script decides the `perf-gate/warm` commit status on every
# pull request, and its first line is parsed by the workflow into that status.
# It runs on two JSON files, so it can be tested on two JSON files: each case
# below writes a merge-base and a PR-head result in the shape the benchmark
# engine emits, runs the script, and checks the exit code, the headline, and
# the lines a reviewer reads.
#
# Usage: scripts/test-perf-gate-compare.sh    (exit 0 = all cases pass)
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/perf-gate-compare.sh"
pass=0
fail=0

ok() { printf '  ok   %s\n' "$1"; pass=$((pass + 1)); }
no() { printf '  FAIL %s\n     %s\n' "$1" "$2"; fail=$((fail + 1)); }

# A result JSON as `kache-scenario --warm-same-tree` writes it, reduced to the
# fields the comparison reads. Each phase carries `wall_ms` and its derived
# whole-second `wall_s`, exactly as the engine records them.
#
#   result <path> <cold_ms> <warm_same_tree_ms> <cross_ms> [hits] [restored_bytes]
result() {
    local path="$1" cold="$2" warm="$3" cross="$4" hits="${5:-783}" restored="${6:-2000000000}"
    cat >"$path" <<EOF
{
  "project": "bench-pr-cargo",
  "git_ref": "797e8a9",
  "cold": { "wall_s": $((cold / 1000)), "wall_ms": $cold, "hits": 0,
            "storage": { "restored_bytes": 0 } },
  "warm_same_tree": { "wall_s": $((warm / 1000)), "wall_ms": $warm, "hits": $hits,
                      "storage": { "restored_bytes": $restored } },
  "warm": { "wall_s": $((cross / 1000)), "wall_ms": $cross, "hits": $hits,
            "storage": { "restored_bytes": $restored } },
  "verdict": { "ok": true },
  "warm_same_tree_verdict": { "ok": true }
}
EOF
}

# Run the script on two results; sets $status, $out (stdout+stderr), $md.
run_gate() {
    local base="$1" head="$2"
    md="$(mktemp)"
    out="$("$script" "$base" "$head" "$md" 2>&1)"
    status=$?
}

expect_status() {
    local name="$1" want="$2"
    if [ "$status" -eq "$want" ]; then ok "$name"; else no "$name" "exit $status, want $want: $out"; fi
}

expect_line() {
    local name="$1" substr="$2"
    if [[ "$out" == *"$substr"* ]]; then ok "$name"; else no "$name" "output lacked '$substr': $out"; fi
}

expect_headline() {
    local name="$1" want="$2" got
    got="$(head -n 1 "$md")"
    if [ "$got" = "$want" ]; then ok "$name"; else no "$name" "headline '$got', want '$want'"; fi
}

echo "perf-gate-compare.sh"
work="$(mktemp -d)"

# --- the numbers from the gate's first live run ------------------------------
#
# 14.6s against 15.1s is +3.4%: a delta whole seconds could never resolve, and
# one the gate must now report as a pass rather than refuse.

result "$work/base.json" 112400 14600 17000
result "$work/head.json" 108100 15100 16200
run_gate "$work/base.json" "$work/head.json"
expect_status "a +3.4% warm build passes" 0
expect_headline "pass headline carries the warm delta and the limit" \
    "## Perf gate: pass — warm build +3.4% (limit +5.0%)"
expect_line "the table shows seconds with one decimal" "| 14.6s | 15.1s | **+3.4%** |"
expect_line "the cold row is reported, not gated" "| 112.4s | 108.1s | -3.8% |"
expect_line "the milliseconds behind the percentages are printed" \
    "Milliseconds (merge base / PR head): cold 112400 / 108100, warm 14600 / 15100, cross-worktree 17000 / 16200."

# The workflow's report step turns the first line into the commit status
# description with exactly this sed (see .github/workflows/perf-gate.yml).
parsed="$(head -n 1 "$md" | sed -e 's/^#\{1,\} *//' -e 's/^Perf gate: *//')"
if [ "$parsed" = "pass — warm build +3.4% (limit +5.0%)" ]; then
    ok "the headline parses into the commit-status description the workflow expects"
else
    no "the headline parses into the commit-status description the workflow expects" "got '$parsed'"
fi

# --- the threshold ------------------------------------------------------------

result "$work/head.json" 108100 16000 16200
run_gate "$work/base.json" "$work/head.json"
expect_status "a +9.6% warm build fails" 1
expect_headline "FAIL headline carries the warm delta and the limit" \
    "## Perf gate: FAIL — warm build +9.6% (limit +5.0%)"
expect_line "the failure is annotated with the milliseconds it was decided on" \
    "::error::perf gate: warm wall-clock regressed +9.6% (merge base 14600 ms, PR head 16000 ms; limit +5.0%)"

# Exactly the limit is within budget; the first tenth past it is not.
result "$work/head.json" 108100 15330 16200
run_gate "$work/base.json" "$work/head.json"
expect_status "+5.0% is within budget" 0
result "$work/head.json" 108100 15345 16200
run_gate "$work/base.json" "$work/head.json"
expect_status "+5.1% is a regression" 1

# The decision is taken on the milliseconds, not on the rounded percent the
# headline prints: +5.04% rounds to +5.0% for display and is still over.
result "$work/head.json" 108100 15336 16200
run_gate "$work/base.json" "$work/head.json"
expect_status "+5.04% is a regression even though it prints as +5.0%" 1
expect_headline "the headline still shows the rounded delta" \
    "## Perf gate: FAIL — warm build +5.0% (limit +5.0%)"

# A faster head is never a failure.
result "$work/head.json" 108100 13000 16200
run_gate "$work/base.json" "$work/head.json"
expect_status "a faster warm build passes" 0
expect_headline "a speed-up is reported with its sign" \
    "## Perf gate: pass — warm build -11.0% (limit +5.0%)"

# --- the floor ------------------------------------------------------------------

result "$work/base.json" 20000 4900 6000
result "$work/head.json" 20000 4900 6000
run_gate "$work/base.json" "$work/head.json"
expect_status "a warm build under the floor is not compared" 1
expect_headline "an uncompared run says so in the headline" "## Perf gate: INVALID MEASUREMENT"
expect_line "the floor message names both figures in milliseconds" \
    "the warm build took 4900 ms (4.9s), below the 5000 ms floor"

result "$work/base.json" 20000 5000 6000
result "$work/head.json" 20000 5000 6000
run_gate "$work/base.json" "$work/head.json"
expect_status "a warm build exactly at the floor is compared" 0

# --- runs that measured nothing ------------------------------------------------

result "$work/base.json" 20000 20000 6000
result "$work/head.json" 20000 15000 6000
run_gate "$work/base.json" "$work/head.json"
expect_status "a warm build that did not beat its cold build is rejected" 1
expect_line "the rejection names the side and both builds" \
    "merge base: the warm build (20000 ms) did not beat its own cold build (20000 ms)"

result "$work/base.json" 112400 14600 17000 0 0
result "$work/head.json" 108100 15100 16200
run_gate "$work/base.json" "$work/head.json"
expect_status "zero hits is rejected" 1
expect_line "zero hits is named" "merge base: the warm build reported 0 cache hits"
expect_line "zero restored bytes is named" "merge base: the warm build restored 0 bytes"

# A result written by an engine that recorded whole seconds only. It has the
# phase, but no milliseconds to compare on.
result "$work/base.json" 112400 14600 17000
result "$work/head.json" 108100 15100 16200
jq 'del(.cold.wall_ms, .warm_same_tree.wall_ms, .warm.wall_ms)' "$work/head.json" >"$work/old.json"
run_gate "$work/base.json" "$work/old.json"
expect_status "a result without wall_ms is rejected" 1
expect_line "the missing field is named" "PR head: the result JSON carries no wall_ms"

jq 'del(.warm_same_tree, .warm_same_tree_verdict)' "$work/head.json" >"$work/nophase.json"
run_gate "$work/base.json" "$work/nophase.json"
expect_status "a result without the same-tree phase is rejected" 1
expect_line "the missing phase is named" "PR head: no warm-same-tree phase in the result JSON"

# The same phase, present as a literal null rather than absent. Not a shape the
# engine writes, but the diagnosis must still be "no phase", not "no wall_ms".
jq '.warm_same_tree = null | .warm_same_tree_verdict = null' "$work/head.json" >"$work/nullphase.json"
run_gate "$work/base.json" "$work/nullphase.json"
expect_status "a null same-tree phase is rejected" 1
expect_line "a null phase is reported as missing, not as lacking wall_ms" "PR head: no warm-same-tree phase in the result JSON"

run_gate "$work/base.json" "$work/does-not-exist.json"
expect_status "a missing input is an error" 1
expect_line "the missing input is named" "missing benchmark result"

rm -rf "$work"
echo
echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
