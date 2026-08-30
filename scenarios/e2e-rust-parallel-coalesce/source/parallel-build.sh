#!/bin/sh
# In-flight build coalescing across parallel cargo invocations (PR #646).
#
# The use case: CI runs SEVERAL cargo processes at once, each with its own
# CARGO_TARGET_DIR, building overlapping dependencies with the same
# features. Without coalescing every process compiles the shared deps
# itself and any dedup happens after the fact. kache's contract is
# stronger: the first wrapper to miss a unit joins a flight and claims
# its key; concurrent wrappers wait on that flight and restore the
# winner's artifact — one compile per unit ACROSS ALL processes, even
# on a stone-cold cache. Distinct CARGO_TARGET_DIR values remain
# required; Kache does not wrap Cargo.
#
# This script is the fixture's `build` command: two cargo builds of the
# same workspace, launched simultaneously against distinct target dirs.
# The scenario.toml cold assertions (max_compiler_runs = one per unit,
# min_hits = the coalesced restores) pin the contract; without the claim
# both processes would miss and compile every unit, doubling
# compiler_runs.
set -e

# Uncolored, format-stable cargo output for the harness log.
CARGO_TERM_COLOR=never
export CARGO_TERM_COLOR

# The heavy members are generated, not committed: ~10k trivial functions
# each buy a compile long enough (seconds) that the second process
# reliably arrives while the first is still compiling — the contended
# path, not just a late plain hit. Generation is deterministic (the seed
# is the only variation) and guarded by existence so reruns keep source
# mtimes stable: the noop phase must stay a true zero-dispatch no-op.
gen_heavy() {
    dir=$1
    seed=$2
    out="$dir/src/lib.rs"
    [ -f "$out" ] && return 0
    mkdir -p "$dir/src"
    # Generate to a temp name and rename: an interrupted awk must not leave
    # a partial lib.rs that the existence guard would accept on retry.
    awk -v seed="$seed" 'BEGIN {
        n = 10000
        for (i = 0; i < n; i++)
            printf "pub fn f%d(x: u64) -> u64 { let mut a = x.wrapping_mul(%d); let mut j = 0u64; while j < 5 { a = a.rotate_left(7).wrapping_add(j) ^ %d; j += 1; } a }\n", i, i + 1, i + seed
        printf "pub fn sum(x: u64) -> u64 { let mut a = 0u64; "
        for (i = 0; i < n; i += 100) printf "a ^= f%d(x); ", i
        printf "a }\n"
    }' > "$out.tmp"
    mv "$out.tmp" "$out"
}
gen_heavy heavy1 100000
gen_heavy heavy2 200000
gen_heavy heavy3 300000

# Two simultaneous builds, distinct target dirs, one shared cache. Cargo
# serializes concurrent access to a SHARED target dir with its build-dir
# lock, so distinct dirs are exactly how real CI gets parallelism — and
# exactly where duplicated dep compiles would come from.
CARGO_TARGET_DIR=t1 cargo build --release > t1-build.log 2>&1 &
p1=$!
CARGO_TARGET_DIR=t2 cargo build --release > t2-build.log 2>&1 &
p2=$!

s1=0
s2=0
wait "$p1" || s1=$?
wait "$p2" || s2=$?

# Full transcripts to stderr for the harness log, then drop the temp
# files so they never ride along into the relocated tree.
sed 's/^/[t1] /' t1-build.log >&2
sed 's/^/[t2] /' t2-build.log >&2
rm -f t1-build.log t2-build.log

# A FAILED build must abort loudly, never read as a cheap phase — the
# assertion bug class #135/#136 exists to prevent.
if [ "$s1" != "0" ] || [ "$s2" != "0" ]; then
    echo "parallel-coalesce: build failed (t1=$s1 t2=$s2)" >&2
    exit 1
fi
