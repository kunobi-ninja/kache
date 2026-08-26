#!/usr/bin/env bash
# Deterministic checks for with-test-resources.sh. Every limit change happens
# in a subshell, so the caller's soft/hard limits are never modified.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
wrapper="$script_dir/with-test-resources.sh"

case "$(uname -s 2>/dev/null || true)" in
  CYGWIN* | MINGW* | MSYS*)
    echo "test-resource guard: Windows uses native process resources; skipped"
    exit 0
    ;;
esac

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

hard_nofile="$(ulimit -Hn)"
if [ "$hard_nofile" = "unlimited" ] || {
  is_positive_integer "$hard_nofile" && ((10#$hard_nofile >= 4096))
}; then
  (
    unset RUST_TEST_THREADS
    ulimit -Sn 64
    # shellcheck disable=SC2016 # Evaluated by the child Bash.
    "$wrapper" bash -c '
      soft="$(ulimit -Sn)"
      if [ "$soft" != unlimited ] && [ "$soft" -lt 4096 ]; then
        echo "soft nofile was not raised: $soft" >&2
        exit 1
      fi
      [ "${RUST_TEST_THREADS+x}" != x ]
    '
  )
else
  echo "test-resource guard: hard nofile below 4096; raise-path check skipped"
fi

# Make 4096 unreachable in this subshell and prove an existing lower setting
# is preserved rather than increased.
(
  ulimit -Sn 64
  ulimit -Hn 64
  # shellcheck disable=SC2016 # Evaluated by the child Bash.
  RUST_TEST_THREADS=1 "$wrapper" bash -c '
    [ "$(ulimit -Sn)" -eq 64 ]
    [ "$RUST_TEST_THREADS" -eq 1 ]
  '
)

# With no explicit setting, the fallback must be positive and no greater than
# either its conservative cap or the host concurrency visible to the child.
(
  ulimit -Sn 64
  ulimit -Hn 64
  unset RUST_TEST_THREADS
  # shellcheck disable=SC2016 # Evaluated by the child Bash.
  "$wrapper" bash -c '
    case "$RUST_TEST_THREADS" in
      "" | *[!0-9]* | 0) exit 1 ;;
    esac
    [ "$RUST_TEST_THREADS" -le 8 ]
    detected="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    case "$detected" in
      "" | *[!0-9]* | 0) detected=1 ;;
    esac
    [ "$RUST_TEST_THREADS" -le "$detected" ]
  '
)

set +e
"$wrapper" >/dev/null 2>&1
usage_status=$?
set -e
if [ "$usage_status" -ne 64 ]; then
  echo "wrapper without a command returned $usage_status, expected 64" >&2
  exit 1
fi

echo "test-resource guard: all checks passed"
