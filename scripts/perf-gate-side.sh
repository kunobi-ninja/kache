#!/usr/bin/env bash
# Measure one side of the per-PR perf gate: run the benchmark engine with that
# side's kache binary, keep its result JSON and logs, then free its scratch so
# the other side starts on the same disk budget.
#
# Usage: perf-gate-side.sh <base|head>
#
# Expects the layout .github/workflows/perf-gate.yml stages under
# $RUNNER_TEMP: the instrument in instrument/ and the binaries under test as
# kache-base and kache-head. BENCH_PROFILE and BENCH_SCENARIO come from the
# workflow env. Writes $RUNNER_TEMP/<side>.json and $RUNNER_TEMP/logs/<side>/.
set -euo pipefail

side="${1:?usage: perf-gate-side.sh <base|head>}"
case "$side" in
    base | head) ;;
    *)
        echo "::error::perf gate: unknown side '$side' (want base or head)"
        exit 1
        ;;
esac
: "${RUNNER_TEMP:?}" "${BENCH_PROFILE:?}" "${BENCH_SCENARIO:?}"

work="tmp/perf-gate/$side"
"$RUNNER_TEMP/instrument/kache-scenario" \
    --kache "$RUNNER_TEMP/kache-$side" \
    --scenarios "$RUNNER_TEMP/instrument/scenarios" \
    --select suite:bench --select backend:kache \
    --profile "$BENCH_PROFILE" \
    --warm-same-tree \
    --work-dir "$work"
cp "$work/$BENCH_SCENARIO.json" "$RUNNER_TEMP/$side.json"
mkdir -p "$RUNNER_TEMP/logs/$side"
cp "$work"/*.log "$RUNNER_TEMP/logs/$side/" || true
# Free the objdirs, worktrees and store before the other side runs.
# `clone-ref` is a SIBLING of the work dir, so it needs its own rm.
rm -rf "$work" "$work-clone-ref"
