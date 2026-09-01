#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
runner="$script_dir/run-cargo-mutants.sh"
fake_bin="$(mktemp -d "${TMPDIR:-/tmp}/kache-mutants-test.XXXXXX")"
trap 'rm -rf "$fake_bin"' EXIT

cat >"$fake_bin/uname" <<'EOF'
#!/bin/sh
printf '%s\n' "${FAKE_UNAME:-Darwin}"
EOF

cat >"$fake_bin/cargo" <<'EOF'
#!/bin/sh
printf 'wrapper=%s\n' "${RUSTC_WRAPPER-<unset>}"
printf 'jobs=%s\n' "${CARGO_MUTANTS_JOBS-<unset>}"
printf 'threads=%s\n' "${RUST_TEST_THREADS-<unset>}"
printf 'args='
printf '<%s>' "$@"
printf '\n'
EOF

chmod +x "$fake_bin/uname" "$fake_bin/cargo"

assert_line() {
  local output="$1"
  local expected="$2"
  if ! grep -Fqx "$expected" <<<"$output"; then
    printf 'missing expected line %q in:\n%s\n' "$expected" "$output" >&2
    exit 1
  fi
}

darwin_output="$(
  env -u CARGO_MUTANTS_JOBS -u RUST_TEST_THREADS \
    PATH="$fake_bin:$PATH" FAKE_UNAME=Darwin RUSTC_WRAPPER=kache \
    "$runner" --list
)"
assert_line "$darwin_output" 'wrapper='
assert_line "$darwin_output" 'jobs=1'
assert_line "$darwin_output" 'threads=2'
assert_line "$darwin_output" 'args=<mutants><--list>'

override_output="$(
  PATH="$fake_bin:$PATH" FAKE_UNAME=Darwin RUSTC_WRAPPER=kache \
    CARGO_MUTANTS_JOBS=3 RUST_TEST_THREADS=4 "$runner" --check
)"
assert_line "$override_output" 'wrapper='
assert_line "$override_output" 'jobs=3'
assert_line "$override_output" 'threads=4'

linux_output="$(
  env -u CARGO_MUTANTS_JOBS \
    PATH="$fake_bin:$PATH" FAKE_UNAME=Linux RUSTC_WRAPPER=kache \
    RUST_TEST_THREADS=5 \
    "$runner" --list
)"
assert_line "$linux_output" 'wrapper='
assert_line "$linux_output" 'jobs=<unset>'
assert_line "$linux_output" 'threads=5'

echo 'mutation runner: all checks passed'
