#!/usr/bin/env bash
# Exercise daemon admission when Linux cannot expose process security labels.
set -euo pipefail

if [ "$(uname -s)" != Linux ]; then
  echo 'This regression requires Linux /proc and LD_PRELOAD.' >&2
  exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir=$(mktemp -d)
trap 'rm -rf "$fixture_dir"' EXIT

"${CC:-cc}" -std=c11 -Wall -Wextra -Werror -shared -fPIC \
  "$root/tests/fixtures/proc-attr-unavailable.c" \
  -o "$fixture_dir/proc-attr-unavailable.so"
preload="$fixture_dir/proc-attr-unavailable.so${LD_PRELOAD:+:$LD_PRELOAD}"

if LD_PRELOAD="$preload" cat /proc/self/attr/current > /dev/null 2>&1; then
  echo 'The security-label fault was not injected.' >&2
  exit 1
fi

cd "$root"
probe=daemon::execution::tests::fixture_unavailable_security_labels
selected=$(RUSTC_WRAPPER= cargo test --locked -p kache --lib -- --list --exact "$probe")
if ! grep -q ': test$' <<< "$selected"; then
  echo "The unavailable-label fixture was not selected: $probe" >&2
  exit 1
fi
LD_PRELOAD="$preload" RUSTC_WRAPPER= cargo test --locked -p kache --lib -- \
  --ignored --exact "$probe"
filters=(
  daemon::execution::tests::execution_probe_authenticates_the_peer_and_reserves_no_worker
  daemon::execution::tests::filtered_execution_requires_both_peers_to_trust_the_pipeline
  daemon::execution::tests::trusted_linux_clients_stream_concurrently_and_release_worker_permits
)
selected=$(RUSTC_WRAPPER= cargo test --locked -p kache --lib -- --list "${filters[@]}")
for test in "${filters[@]}"; do
  if ! grep -Fxq "$test: test" <<< "$selected"; then
    echo "No daemon test selected by $test" >&2
    exit 1
  fi
done
LD_PRELOAD="$preload" RUSTC_WRAPPER= cargo test --locked -p kache --lib -- "${filters[@]}"
