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

# Uncolored, format-stable cargo output for the `Compiling` count below.
CARGO_TERM_COLOR=never
export CARGO_TERM_COLOR

# Build the workspace against target dir $1; print the number of units
# cargo dispatched (its `Compiling` announcements go to stderr). A FAILED
# build must abort the contract, never read as zero dispatches — counting
# silence as convergence is exactly the assertion bug class this fixture
# exists to prevent (see #135/#136). On a direct `run x` call the `exit 1`
# fires in the main shell; inside `n=$(run x)` it exits the substitution
# subshell and the failed assignment trips the outer `set -e`. Successful
# output is re-emitted on stderr so the harness log keeps the full cargo
# transcript.
run() {
    out=$(CARGO_TARGET_DIR="$1" cargo build --release 2>&1) || {
        echo "cross-tree: build failed in $1:" >&2
        printf '%s\n' "$out" | tail -n 5 >&2
        exit 1
    }
    printf '%s\n' "$out" >&2
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

# Substrate proof (Linux only): the contract is about SHARED INODES, and a
# copy/reflink fallback would make everything below pass trivially. Tree B
# is always restored from cache (in every phase — cold populates it from
# tree A first), so on the Linux legs (ubuntu CI, e2e-docker's ext4
# volume) at least one of its restored rlibs must share its inode with the
# store (nlink >= 2). The `util` member exists exactly to be this hardlink
# carrier — proc-macro dylibs and bins may restore via copy. Skipped
# elsewhere: APFS restores reflink by design. If a Linux host legitimately
# reflinks (btrfs/XFS), set KACHE_E2E_SKIP_HARDLINK_CHECK=1 rather than
# deleting the check.
if [ "$(uname -s)" = "Linux" ] && [ -z "${KACHE_E2E_SKIP_HARDLINK_CHECK:-}" ]; then
    linked=0
    for f in tb/release/deps/*.rlib; do
        [ -e "$f" ] || continue
        if [ "$(stat -c %h "$f")" -ge 2 ]; then
            linked=1
            break
        fi
    done
    if [ "$linked" != "1" ]; then
        echo "cross-tree: no restored rlib is hardlinked to the store (nlink < 2) — the cross-tree contract would be vacuous on this substrate"
        exit 1
    fi
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
