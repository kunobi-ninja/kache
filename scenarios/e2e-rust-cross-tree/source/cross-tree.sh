#!/bin/sh
# Cross-tree active-reader isolation contract (kunobi-ninja/kache#794).
#
# On a non-CoW filesystem the first target may share an immutable artifact
# with its store blob. A later restore must get a private inode before its
# write-clock stamp: otherwise the stamp changes the first target while a
# linker such as Wild is reading it. This script holds tree A's rlib open
# while tree B restores and asserts both metadata stability and immediate
# Cargo convergence. It is self-contained and idempotent so every harness
# phase (cold / warm / noop / relocate) can run it as `build`.
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

# The harness does not clean before its noop phase. Only require a phase-local
# util hit when tree B is absent and the build below must actually restore it.
tree_b_needs_restore=0
if [ ! -d tb ]; then
    tree_b_needs_restore=1
fi

# Substrate proof (Linux only): tree A is the initial hardlink carrier. Prove
# that it shares its inode with an actual content blob (nlink alone can also
# come from Cargo's profile-level uplift), then hold that exact inode open
# across tree B's restore just as Wild does. Linux hosts with reflink may set
# KACHE_E2E_SKIP_HARDLINK_CHECK=1.
shares_store_blob() {
    probe_artifact_id=$(stat -Lc '%d:%i' "$1")
    for probe_blob in "$KACHE_CACHE_DIR"/store/blobs/*/*; do
        [ -f "$probe_blob" ] || continue
        if [ "$(stat -Lc '%d:%i' "$probe_blob")" = "$probe_artifact_id" ]; then
            return 0
        fi
    done
    return 1
}

carrier=
if [ "$(uname -s)" = "Linux" ] && [ -z "${KACHE_E2E_SKIP_HARDLINK_CHECK:-}" ]; then
    for f in ta/release/deps/libutil-*.rlib; do
        [ -e "$f" ] || continue
        if shares_store_blob "$f"; then
            carrier=$f
            break
        fi
    done
    if [ -z "$carrier" ]; then
        echo "cross-tree: tree A has no hardlinked util rlib — the active-reader contract would be vacuous on this substrate"
        exit 1
    fi
    exec 3<"$carrier"
    held_before=$(stat -Lc '%s:%y' "/proc/$$/fd/3")
    event_log="$KACHE_CACHE_DIR/events.jsonl"
    event_lines_before=$(wc -l < "$event_log")
    # Cross even coarse one-second filesystem timestamp resolution.
    sleep 1
fi

# Tree B warms from the same cache while A's carrier is held open. Cargo must
# dispatch its units once to populate a fresh target directory; the Linux check
# below requires a phase-local util hit rather than relying on aggregate hits.
run tb >/dev/null

if [ -n "$carrier" ]; then
    if [ "$tree_b_needs_restore" = "1" ] &&
        ! tail -n "+$((event_lines_before + 1))" "$event_log" |
        grep -q '"crate_name":"util".*"result":"local_hit"'; then
        echo "cross-tree: tree B did not restore util from the local cache"
        exit 1
    fi

    held_after=$(stat -Lc '%s:%y' "/proc/$$/fd/3")
    if [ "$held_after" != "$held_before" ]; then
        echo "cross-tree: tree B changed tree A's open rlib metadata ($held_before -> $held_after)"
        exit 1
    fi

    carrier_name=$(basename "$carrier")
    tree_b_carrier="tb/release/deps/$carrier_name"
    if [ ! -e "$tree_b_carrier" ]; then
        echo "cross-tree: tree B did not restore matching carrier $carrier_name"
        exit 1
    fi
    if [ "$(stat -Lc '%d:%i' "/proc/$$/fd/3")" = "$(stat -Lc '%d:%i' "$tree_b_carrier")" ]; then
        echo "cross-tree: trees A and B share the active rlib inode"
        exit 1
    fi
    exec 3<&-
fi

# Both the restored tree and the previously-active tree must be immediate
# no-ops. A bounded rebuild transient is no longer acceptable: it can abort
# an in-flight linker (#794).
n=$(run tb)
if [ "$n" != "0" ]; then
    echo "cross-tree: tree B did not converge after its warmup ($n dispatches)"
    exit 1
fi
n=$(run ta)
if [ "$n" != "0" ]; then
    echo "cross-tree: tree B invalidated tree A ($n dispatches)"
    exit 1
fi

# Steady-state alternation stays at zero dispatches.
for t in tb ta tb ta; do
    n=$(run "$t")
    if [ "$n" != "0" ]; then
        echo "cross-tree: steady-state dispatch in $t ($n)"
        exit 1
    fi
done

echo "cross-tree contract ok"
