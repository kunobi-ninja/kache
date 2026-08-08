#!/bin/sh
# Cross-tree convergence contract (kunobi-ninja/kache#680).
#
# With the hardlink restore strategy, the write-clock stamp applied at
# restore time lands on the store blob's inode, shared by every tree that
# restored the same blob. A tree whose fingerprints predate the stamp
# re-dispatches its downstream units on its next build. The accepted
# semantics (#680) are: that perturbation must be a BOUNDED transient, and
# steady-state alternation between trees must stay at zero dispatches.
# This script asserts exactly that contract, self-contained and idempotent,
# so every harness phase (cold / warm / noop / relocate) can run it as the
# fixture's `build` command.
#
# On filesystems where reflink wins (APFS), restores do not share inodes
# and the contract holds trivially; the hardlink case engages on ext4
# (e2e-docker, Linux CI), which is the substrate this fixture exists for.
set -e

# Build the workspace against target dir $1; print the number of units
# cargo dispatched (its `Compiling` announcements go to stderr). A FAILED
# build must abort the contract, never read as zero dispatches — counting
# silence as convergence is exactly the assertion bug class this fixture
# exists to prevent (see #135/#136). The `exit 1` fires inside a command
# substitution subshell at the call sites, which `set -e` then propagates.
run() {
    out=$(CARGO_TARGET_DIR="$1" cargo build --release 2>&1) || {
        echo "cross-tree: build failed in $1:" >&2
        printf '%s\n' "$out" | tail -5 >&2
        exit 1
    }
    printf '%s\n' "$out" | grep -cE "^ +Compiling" || true
}

# Contract 0 (the single-tree #677 regression, on this substrate): after
# one build of a tree — a cold compile or a warm restore — the very next
# build must already be a zero-dispatch no-op. The precise-clock stamp
# failed exactly here after a warm restore.
run ta >/dev/null
n=$(run ta)
if [ "$n" != "0" ]; then
    echo "cross-tree: tree A did not converge after one build ($n dispatches)"
    exit 1
fi

# Tree B warms from the same cache: its restores re-date the shared blobs.
# B itself must converge immediately, same contract.
run tb >/dev/null
n=$(run tb)
if [ "$n" != "0" ]; then
    echo "cross-tree: tree B did not converge after its warmup ($n dispatches)"
    exit 1
fi

# Contract 1: tree A reconverges within three rebuilds of B's warmup.
i=0
n=1
until [ "$i" -ge 3 ]; do
    n=$(run ta)
    if [ "$n" = "0" ]; then
        break
    fi
    i=$((i + 1))
done
if [ "$n" != "0" ]; then
    echo "cross-tree: tree A failed to reconverge after 3 rebuilds (still $n dispatches)"
    exit 1
fi

# Contract 2: steady-state alternation stays at zero dispatches.
for t in tb ta tb ta; do
    n=$(run "$t")
    if [ "$n" != "0" ]; then
        echo "cross-tree: steady-state dispatch in $t ($n)"
        exit 1
    fi
done

echo "cross-tree contract ok"
