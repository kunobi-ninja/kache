#!/bin/sh
# Verify contract for the e2e-rust-dsym fixture (kunobi-ninja/kache#319).
#
# Runs after EVERY successful build phase, so it must hold on the cold
# build too — which it does, because kache's store path leaves the baked
# `.dSYM` next to the binary it packages (cold/warm parity), and the
# restore path re-creates the same shape by unpacking the cached
# `<bin>.dsym.tar`.
#
# Three assertions:
#   1. the restored binary still runs (stdout contract),
#   2. exactly one `.dSYM` bundle exists next to the deps binary,
#   3. its DWARF UUID equals the binary's — UUID identity is the exact
#      criterion lldb uses to adopt an adjacent bundle over the binary's
#      stale N_OSO debug map, so a mismatch means broken debugging.
set -eu

./target/debug/rust-dsym

bundle=""
for candidate in target/debug/deps/rust_dsym-*.dSYM; do
    [ -d "$candidate" ] || { echo "no .dSYM bundle in target/debug/deps"; exit 1; }
    [ -z "$bundle" ] || { echo "more than one .dSYM bundle: $bundle and $candidate"; exit 1; }
    bundle="$candidate"
done

binary="${bundle%.dSYM}"
binary_uuid=$(dwarfdump --uuid "$binary" | awk 'NR==1 {print $2}')
bundle_uuid=$(dwarfdump --uuid "$bundle" | awk 'NR==1 {print $2}')
[ -n "$binary_uuid" ] || { echo "dwarfdump produced no UUID for $binary"; exit 1; }
if [ "$binary_uuid" != "$bundle_uuid" ]; then
    echo "UUID mismatch: binary $binary_uuid vs bundle $bundle_uuid"
    exit 1
fi
echo "DSYM-UUID-MATCH $bundle_uuid"
dwarfdump --uuid "$bundle"
