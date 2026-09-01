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
# Every phase records its wall-clock twice: `wall_ms`, which this script
# compares, and `wall_s`, the whole-second view the nightly reports and their
# telemetry carry.
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
# gate. 5% sits above the run-to-run noise of a build measured back to back on
# one runner while still catching the kind of drift that accumulated unnoticed
# before this gate existed. It is the ONLY blocking number: the cold and
# cross-worktree deltas are reported so a reviewer sees them, but they do not
# fail the run until there is enough history to know their noise floor.
readonly WARM_REGRESSION_LIMIT_PCT=5.0

# ── The floor ────────────────────────────────────────────────────────────────
# The gate's first live run measured the subject's warm build at 14-15s. The
# engine recorded whole seconds then, so one timing tick was 7% of the build,
# more than the threshold above, and the old 30s floor rightly refused to
# compare. The engine now records milliseconds, which puts quantization three
# orders of magnitude below the threshold at that length, so the floor no
# longer guards against the timer.
#
# What it guards against is a build so short that the threshold falls inside
# the fixed costs that do not scale with the build: spawning the shell and
# cargo, cargo's own resolve and fingerprint pass, scheduler jitter on a shared
# runner. Those are tens of milliseconds each. At 5000 ms the threshold is
# 250 ms, an order of magnitude above them, so a delta at the limit reflects
# the build changing rather than the runner breathing. The subject sits about
# three times above this floor; tripping it means the subject shrank, the build
# did nothing, or the scenario is pointed somewhere unexpected.
readonly MIN_MEASURABLE_WARM_MS=5000

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

# The same-tree phase is present or it is not: the engine omits the key when
# the run had no `--warm-same-tree`, and a hand-edited `null` means the same.
# Testing the value rather than the key keeps both from being misreported as a
# phase that ran but carries no `wall_ms`.
has_phase() { jq -r "${2} != null" "$1"; }

base_has_warm="$(has_phase "$base_json" .warm_same_tree)"
base_cold="$(field "$base_json" .cold.wall_ms)"
base_warm="$(field "$base_json" .warm_same_tree.wall_ms)"
base_cross="$(field "$base_json" .warm.wall_ms)"
base_hits="$(field "$base_json" .warm_same_tree.hits)"
base_restored="$(field "$base_json" .warm_same_tree.storage.restored_bytes)"
base_verdict="$(field "$base_json" .verdict.ok)"
base_st_verdict="$(field "$base_json" .warm_same_tree_verdict.ok)"

head_has_warm="$(has_phase "$head_json" .warm_same_tree)"
head_cold="$(field "$head_json" .cold.wall_ms)"
head_warm="$(field "$head_json" .warm_same_tree.wall_ms)"
head_cross="$(field "$head_json" .warm.wall_ms)"
head_hits="$(field "$head_json" .warm_same_tree.hits)"
head_restored="$(field "$head_json" .warm_same_tree.storage.restored_bytes)"
head_verdict="$(field "$head_json" .verdict.ok)"
head_st_verdict="$(field "$head_json" .warm_same_tree_verdict.ok)"
subject_ref="$(field "$head_json" .git_ref)"

# Seconds with one decimal for a human, truncated rather than rounded so the
# figure agrees with the whole-second `wall_s` the same JSON carries.
secs() {
    awk -v ms="$1" 'BEGIN { printf "%d.%d", ms / 1000, (ms % 1000) / 100 }'
}

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
    local side="$1" has_warm="$2" cold="$3" warm="$4" cross="$5" hits="$6" restored="$7" verdict="$8" st_verdict="$9"

    if [ "$has_warm" != "true" ]; then
        problems+=("$side: no warm-same-tree phase in the result JSON — that run was not invoked with --warm-same-tree")
        return
    fi
    if [ -z "$cold" ] || [ -z "$warm" ] || [ -z "$cross" ]; then
        problems+=("$side: the result JSON carries no wall_ms — it was written by an engine that timed whole seconds only; rebuild kache-scenario from a current checkout and measure again")
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
    [ "$cold" -gt 0 ] ||
        problems+=("$side: cold wall-clock is 0 ms")
    [ "$warm" -ge "$MIN_MEASURABLE_WARM_MS" ] ||
        problems+=("$side: the warm build took ${warm} ms ($(secs "$warm")s), below the ${MIN_MEASURABLE_WARM_MS} ms floor — that short, the ${WARM_REGRESSION_LIMIT_PCT}% threshold sits inside the runner's fixed per-build overhead, so a delta would measure the runner rather than kache")
    if [ "$warm" -gt 0 ] && [ "$cold" -gt 0 ] && [ "$warm" -ge "$cold" ]; then
        problems+=("$side: the warm build (${warm} ms) did not beat its own cold build (${cold} ms) — the cache bought nothing, so a delta against it is meaningless")
    fi
}

check_side "merge base" "$base_has_warm" \
    "$base_cold" "$base_warm" "$base_cross" "$base_hits" "$base_restored" "$base_verdict" "$base_st_verdict"
check_side "PR head" "$head_has_warm" \
    "$head_cold" "$head_warm" "$head_cross" "$head_hits" "$head_restored" "$head_verdict" "$head_st_verdict"

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
# Positive percent means the head is SLOWER than the merge base. Computed on
# milliseconds; the table below shows seconds for reading and repeats the
# milliseconds so the percent can be checked by hand.
pct() {
    awk -v b="$1" -v h="$2" \
        'BEGIN { if (b + 0 <= 0) print "n/a"; else printf "%+.1f", (h - b) * 100.0 / b }'
}
cold_pct="$(pct "$base_cold" "$head_cold")"
warm_pct="$(pct "$base_warm" "$head_warm")"
cross_pct="$(pct "$base_cross" "$head_cross")"

regressed="$(awk -v d="$warm_pct" -v lim="$WARM_REGRESSION_LIMIT_PCT" \
    'BEGIN { print (d != "n/a" && d + 0 > lim + 0) ? "yes" : "no" }')"

# The workflow's report step parses this first line into the commit status:
# it strips the leading `## Perf gate: ` and keeps the rest. Keep the grammar.
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
    echo "| cold (empty store) | $(secs "$base_cold")s | $(secs "$head_cold")s | ${cold_pct}% |"
    echo "| **warm** (warm store, fresh objdir) | $(secs "$base_warm")s | $(secs "$head_warm")s | **${warm_pct}%** |"
    echo "| cross-worktree (warm store, other path) | $(secs "$base_cross")s | $(secs "$head_cross")s | ${cross_pct}% |"
    echo
    echo "Positive means slower. Only the warm row is blocking; cold and cross-worktree"
    echo "are reported for context while their noise floor is still being established."
    echo
    echo "Milliseconds (merge base / PR head): cold ${base_cold} / ${head_cold}, warm ${base_warm} / ${head_warm}, cross-worktree ${base_cross} / ${head_cross}."
} | tee "$md_out"

if [ "$regressed" = "yes" ]; then
    echo "::error::perf gate: warm wall-clock regressed ${warm_pct}% (merge base ${base_warm} ms, PR head ${head_warm} ms; limit +${WARM_REGRESSION_LIMIT_PCT}%)"
    exit 1
fi
