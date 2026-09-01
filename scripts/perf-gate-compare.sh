#!/usr/bin/env bash
# Compare two `bench-pr-cargo` runs — the PR head and its merge base, measured
# back to back on the same machine — and decide whether the head regressed.
#
# Usage: scripts/perf-gate-compare.sh <base.json> <head.json> <markdown-out>
#
# Both inputs are the result JSON the clone benchmark engine writes
# (`tmp/bench/<scenario>/<scenario>.json`), produced with `--warm-same-tree` so
# each side carries three wall-clocks:
#
#   cold            empty store, fresh objdir               (baseline)
#   warm-same-tree  warm store, fresh objdir, SAME path     (the gated number)
#   warm            warm store, fresh objdir, other path    (cross-worktree)
#
# Writes a markdown report to <markdown-out> for the step summary and the PR
# comment, prints the same table to stdout, and exits non-zero when the gate
# fails.
#
# Exit 0 = within budget.  Exit 1 = regression, or the run did not validly
#                                  measure anything.
set -euo pipefail

# ── The threshold ────────────────────────────────────────────────────────────
# A head warm build slower than the merge base's by more than this fails the
# gate. 5% sits above the run-to-run noise of a minutes-long build on the ARC
# runners while still catching the kind of drift that accumulated unnoticed
# before this gate existed. It is the ONLY blocking number: the cold and
# cross-worktree deltas are reported so a reviewer sees them, but they do not
# fail the run until there is enough history to know their noise floor.
readonly WARM_REGRESSION_LIMIT_PCT=5.0

# The engine times phases to whole seconds. On a warm build this short, a single
# one-second tick is already worth more than the threshold above, so a "delta"
# would be reporting quantization rather than performance. The real subject's
# warm phase runs well past this; tripping it means the subject shrank, the
# build did nothing, or the scenario is pointed somewhere unexpected.
readonly MIN_MEASURABLE_WARM_S=30

base_json="${1:?usage: perf-gate-compare.sh <base.json> <head.json> <markdown-out>}"
head_json="${2:?usage: perf-gate-compare.sh <base.json> <head.json> <markdown-out>}"
md_out="${3:?usage: perf-gate-compare.sh <base.json> <head.json> <markdown-out>}"

for f in "$base_json" "$head_json"; do
    [ -f "$f" ] || { echo "::error::perf gate: missing benchmark result $f"; exit 1; }
done
command -v jq >/dev/null 2>&1 || { echo "::error::perf gate: jq is required"; exit 1; }

# `// empty` turns a missing field into the empty string rather than the literal
# "null", so the validity checks below can reject it by name.
field() { jq -r "${2} // empty" "$1"; }

base_cold="$(field "$base_json" .cold.wall_s)"
base_warm="$(field "$base_json" .warm_same_tree.wall_s)"
base_cross="$(field "$base_json" .warm.wall_s)"
base_hits="$(field "$base_json" .warm_same_tree.hits)"
base_restored="$(field "$base_json" .warm_same_tree.storage.restored_bytes)"
base_verdict="$(field "$base_json" .verdict.ok)"
base_st_verdict="$(field "$base_json" .warm_same_tree_verdict.ok)"

head_cold="$(field "$head_json" .cold.wall_s)"
head_warm="$(field "$head_json" .warm_same_tree.wall_s)"
head_cross="$(field "$head_json" .warm.wall_s)"
head_hits="$(field "$head_json" .warm_same_tree.hits)"
head_restored="$(field "$head_json" .warm_same_tree.storage.restored_bytes)"
head_verdict="$(field "$head_json" .verdict.ok)"
head_st_verdict="$(field "$head_json" .warm_same_tree_verdict.ok)"
subject_ref="$(field "$head_json" .git_ref)"

# ── Validity gates ───────────────────────────────────────────────────────────
# A benchmark that measured nothing must fail rather than report a flattering
# number. The engine already enforces the per-phase floors each scenario
# declares in `[checks.assert.*]` — cache hits, restored bytes, passthrough
# share, cross-worktree key stability — and exits non-zero when they trip. What
# follows re-reads the two that matter most for a TIMING comparison (so a
# result JSON handed to this script out of band cannot dodge them) and adds the
# one relation the engine cannot check, because it spans two phases: a warm
# build that did not beat its own cold build was not served by the cache,
# whatever its counters say.
problems=()

check_side() {
    local side="$1" cold="$2" warm="$3" hits="$4" restored="$5" verdict="$6" st_verdict="$7"

    if [ -z "$warm" ]; then
        problems+=("$side: no warm-same-tree phase in the result JSON — that run was not invoked with --warm-same-tree")
        return
    fi
    [ "$verdict" = "true" ] ||
        problems+=("$side: the engine's cross-worktree verdict is not ok")
    [ "$st_verdict" = "true" ] ||
        problems+=("$side: the engine's warm-same-tree verdict is not ok")
    [ "${hits:-0}" -gt 0 ] ||
        problems+=("$side: the warm build reported 0 cache hits — it recompiled everything, so its wall-clock is not a cache measurement")
    [ "${restored:-0}" -gt 0 ] ||
        problems+=("$side: the warm build restored 0 bytes — nothing came out of the store into the build tree")
    [ "${cold:-0}" -gt 0 ] ||
        problems+=("$side: cold wall-clock is 0s")
    [ "${warm:-0}" -ge "$MIN_MEASURABLE_WARM_S" ] ||
        problems+=("$side: the warm build took ${warm}s, below the ${MIN_MEASURABLE_WARM_S}s floor — at that length a one-second timing tick outstrips the ${WARM_REGRESSION_LIMIT_PCT}% threshold, so any delta would be quantization noise")
    if [ "${warm:-0}" -gt 0 ] && [ "${cold:-0}" -gt 0 ] && [ "$warm" -ge "$cold" ]; then
        problems+=("$side: the warm build (${warm}s) did not beat its own cold build (${cold}s) — the cache bought nothing, so a delta against it is meaningless")
    fi
}

check_side "merge base" \
    "$base_cold" "$base_warm" "$base_hits" "$base_restored" "$base_verdict" "$base_st_verdict"
check_side "PR head" \
    "$head_cold" "$head_warm" "$head_hits" "$head_restored" "$head_verdict" "$head_st_verdict"

if [ "${#problems[@]}" -gt 0 ]; then
    {
        echo "## Perf gate: INVALID MEASUREMENT"
        echo
        echo "The benchmark did not validly exercise kache, so no delta is reported."
        echo
        for p in "${problems[@]}"; do echo "- $p"; done
    } | tee "$md_out"
    for p in "${problems[@]}"; do echo "::error::perf gate: $p"; done
    exit 1
fi

# ── Deltas ───────────────────────────────────────────────────────────────────
# Positive percent means the head is SLOWER than the merge base.
pct() {
    awk -v b="$1" -v h="$2" \
        'BEGIN { if (b + 0 <= 0) print "n/a"; else printf "%+.1f", (h - b) * 100.0 / b }'
}
cold_pct="$(pct "$base_cold" "$head_cold")"
warm_pct="$(pct "$base_warm" "$head_warm")"
cross_pct="$(pct "$base_cross" "$head_cross")"

regressed="$(awk -v d="$warm_pct" -v lim="$WARM_REGRESSION_LIMIT_PCT" \
    'BEGIN { print (d != "n/a" && d + 0 > lim + 0) ? "yes" : "no" }')"

if [ "$regressed" = "yes" ]; then
    headline="## Perf gate: FAIL — warm build ${warm_pct}% (limit +${WARM_REGRESSION_LIMIT_PCT}%)"
else
    headline="## Perf gate: pass — warm build ${warm_pct}% (limit +${WARM_REGRESSION_LIMIT_PCT}%)"
fi

{
    echo "$headline"
    echo
    echo "Subject \`bench-pr-cargo\` @ \`${subject_ref}\`, both sides measured back to back on one runner."
    echo
    echo "| phase | merge base | PR head | delta |"
    echo "| --- | ---: | ---: | ---: |"
    echo "| cold (empty store) | ${base_cold}s | ${head_cold}s | ${cold_pct}% |"
    echo "| **warm** (warm store, fresh objdir) | ${base_warm}s | ${head_warm}s | **${warm_pct}%** |"
    echo "| cross-worktree (warm store, other path) | ${base_cross}s | ${head_cross}s | ${cross_pct}% |"
    echo
    echo "Positive means slower. Only the warm row is blocking; cold and cross-worktree"
    echo "are reported for context while their noise floor is still being established."
} | tee "$md_out"

if [ "$regressed" = "yes" ]; then
    echo "::error::perf gate: warm wall-clock regressed ${warm_pct}% (limit +${WARM_REGRESSION_LIMIT_PCT}%)"
    exit 1
fi
