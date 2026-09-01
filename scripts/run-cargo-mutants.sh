#!/usr/bin/env bash
# Keep mutation testing independent from the Kache version under test. On
# macOS, bound both cargo-mutants workers and libtest concurrency: process-heavy
# tests otherwise make the OS assess many short-lived binaries at once.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export RUSTC_WRAPPER=""

if [ "$(uname -s 2>/dev/null || true)" = "Darwin" ]; then
  export CARGO_MUTANTS_JOBS="${CARGO_MUTANTS_JOBS:-1}"
  export RUST_TEST_THREADS="${RUST_TEST_THREADS:-2}"
  printf \
    'macOS mutation limits: cargo-mutants jobs=%s, Rust test threads=%s\n' \
    "$CARGO_MUTANTS_JOBS" "$RUST_TEST_THREADS" >&2
fi

exec "$script_dir/with-test-resources.sh" cargo mutants "$@"
