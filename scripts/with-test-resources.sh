#!/usr/bin/env bash
# Run a test-bearing command with enough file descriptors for Kache's parallel
# suite. Low Unix soft limits can otherwise surface unrelated policy failures
# when filesystem operations fail closed under EMFILE (#756).
set -euo pipefail

target_nofile=4096
fallback_max_threads=8

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <command> [args...]" >&2
  exit 64
fi

is_positive_integer() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

limit_is_sufficient() {
  local limit="$1"
  if [ "$limit" = "unlimited" ]; then
    return 0
  fi
  is_positive_integer "$limit" && ((10#$limit >= target_nofile))
}

available_test_threads() {
  local detected
  detected="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
  if is_positive_integer "$detected"; then
    printf '%s\n' "$detected"
  else
    # Unknown is not permission to increase concurrency. One is the only
    # conservative fallback when the host cannot report its CPU allowance.
    printf '1\n'
  fi
}

# Windows has no POSIX RLIMIT_NOFILE. Git Bash can execute this wrapper for
# local Just recipes, but the command itself should remain native there.
case "$(uname -s 2>/dev/null || true)" in
  CYGWIN* | MINGW* | MSYS*) exec "$@" ;;
esac

soft_nofile="$(ulimit -Sn 2>/dev/null || true)"
if limit_is_sufficient "$soft_nofile"; then
  exec "$@"
fi

if ulimit -Sn "$target_nofile" 2>/dev/null; then
  raised_nofile="$(ulimit -Sn 2>/dev/null || true)"
  if limit_is_sufficient "$raised_nofile"; then
    exec "$@"
  fi
fi

# A builder may have a hard limit below 4096. Bound libtest only in that case;
# never raise an explicit setting or the host's available parallelism.
available_threads="$(available_test_threads)"
fallback_threads="$available_threads"
if [ "${RUST_TEST_THREADS+x}" = "x" ]; then
  if ! is_positive_integer "$RUST_TEST_THREADS"; then
    echo "invalid RUST_TEST_THREADS=$RUST_TEST_THREADS: expected a positive integer" >&2
    exit 64
  fi
  fallback_threads="$RUST_TEST_THREADS"
fi
if ((10#$fallback_threads > 10#$available_threads)); then
  fallback_threads="$available_threads"
fi
if ((10#$fallback_threads > fallback_max_threads)); then
  fallback_threads="$fallback_max_threads"
fi
export RUST_TEST_THREADS="$fallback_threads"

printf \
  'warning: could not raise soft nofile limit %s to %s; capping Rust test threads at %s\n' \
  "${soft_nofile:-unknown}" "$target_nofile" "$fallback_threads" >&2
exec "$@"
