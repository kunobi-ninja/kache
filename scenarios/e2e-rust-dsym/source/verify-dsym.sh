#!/bin/sh
# Verify contract for the e2e-rust-dsym fixture (kunobi-ninja/kache#319).
#
# Runs after EVERY successful build phase, so it must hold on the cold
# build too — which it does, because kache's store path leaves the baked
# `.dSYM` next to the binary it packages (cold/warm parity), and the
# restore path re-creates the same shape by unpacking the cached
# `<bin>.dsym.tar`.
#
# For every bundle kache baked (the bin and its unit-test harness and the
# integration test in deps/, the example in examples/; from Cargo 1.100 each
# in its own build/<pkg>/<hash>/out/):
#   1. its DWARF UUID equals the binary's — UUID identity is the exact
#      criterion lldb uses to adopt an adjacent bundle over the binary's
#      stale N_OSO debug map, so a mismatch means broken debugging;
#   2. it holds the target's own compile unit AND itoa's. dsymutil still
#      writes a UUID-matched bundle when it cannot open a single object,
#      so the UUID alone passed while every bundle was empty
#      (kunobi-ninja/kache#1161). itoa's objects are rlib members in deps/
#      (or itoa's own unit directory), which is where that bug looked in the
#      wrong directory.
set -eu

./target/debug/rust-dsym

count=0
for bundle in target/debug/deps/*.dSYM target/debug/examples/*.dSYM \
    target/debug/build/*/*/out/*.dSYM; do
    [ -d "$bundle" ] || continue
    count=$((count + 1))
    binary="${bundle%.dSYM}"
    binary_uuid=$(dwarfdump --uuid "$binary" | awk 'NR==1 {print $2}')
    bundle_uuid=$(dwarfdump --uuid "$bundle" | awk 'NR==1 {print $2}')
    [ -n "$binary_uuid" ] || { echo "dwarfdump produced no UUID for $binary"; exit 1; }
    if [ "$binary_uuid" != "$bundle_uuid" ]; then
        echo "UUID mismatch: binary $binary_uuid vs bundle $bundle_uuid"
        exit 1
    fi
    info=$(dwarfdump --debug-info "$bundle")
    echo "$info" | grep -qE 'DW_AT_name[[:space:]]+\("(src/main|examples/demo|tests/it)\.rs/@/' ||
        { echo "no compile unit for the target's own source in $bundle"; exit 1; }
    echo "$info" | grep -qE 'DW_AT_name[[:space:]]+\(".*/itoa-[^/]+/src/lib\.rs/@/' ||
        { echo "no itoa compile unit in $bundle"; exit 1; }
done
# bin + unit-test harness + integration test + example (cargo may also
# uplift a copy of the example's bundle).
[ "$count" -ge 4 ] || { echo "expected at least 4 .dSYM bundles, found $count"; exit 1; }
echo "DSYM-DEBUG-INFO-OK $count"
