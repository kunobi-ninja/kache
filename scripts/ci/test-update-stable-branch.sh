#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
updater="$root/scripts/ci/update-stable-branch.sh"
tmp="$(mktemp -d "${TMPDIR:-/tmp}/kache-stable-branch-test.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

remote="$tmp/remote.git"
repo="$tmp/repo"
git init --bare --quiet "$remote"
git init --quiet --initial-branch=main "$repo"
git -C "$repo" config user.name "kache stable branch test"
git -C "$repo" config user.email "stable-branch-test@example.invalid"
git -C "$repo" config commit.gpgSign false
git -C "$repo" config tag.gpgSign false
git -C "$repo" remote add origin "$remote"

printf '1.0.0\n' >"$repo/version"
git -C "$repo" add version
git -C "$repo" commit --quiet -m "release 1.0.0"
git -C "$repo" tag v1.0.0
v1="$(git -C "$repo" rev-parse HEAD)"

printf '1.1.0-rc.1\n' >"$repo/version"
git -C "$repo" commit --quiet -am "release 1.1.0-rc.1"
git -C "$repo" tag v1.1.0-rc.1

printf '1.1.0\n' >"$repo/version"
git -C "$repo" commit --quiet -am "release 1.1.0"
git -C "$repo" tag v1.1.0
v2="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" push --quiet origin main --tags

stable_sha() {
  git ls-remote --refs "$remote" refs/heads/stable | awk 'NR == 1 { print $1 }'
}

run_updater() {
  (cd "$repo" && STABLE_BRANCH_REMOTE="$remote" bash "$updater" "$@")
}

# Prereleases are rejected even if a caller incorrectly labels one "latest".
if run_updater v1.1.0-rc.1 v1.1.0-rc.1 >/dev/null 2>&1; then
  echo "prerelease unexpectedly updated stable" >&2
  exit 1
fi
[ -z "$(stable_sha)" ]

# The first stable release creates the branch.
run_updater v1.0.0 v1.0.0 >/dev/null
[ "$(stable_sha)" = "$v1" ]

# A delayed older event is a successful no-op, never a rewind.
run_updater v1.0.0 v1.1.0 >/dev/null
[ "$(stable_sha)" = "$v1" ]

# A newer GA release advances the branch; repeating it is idempotent.
run_updater v1.1.0 v1.1.0 >/dev/null
[ "$(stable_sha)" = "$v2" ]
run_updater v1.1.0 v1.1.0 >/dev/null
[ "$(stable_sha)" = "$v2" ]

# A divergent manual branch move is not overwritten, even when the requested
# tag is still GitHub's latest release.
git -C "$repo" switch --quiet --detach "$v1"
printf 'diverged\n' >"$repo/version"
git -C "$repo" commit --quiet -am "divergent stable branch"
diverged="$(git -C "$repo" rev-parse HEAD)"
git -C "$repo" push --quiet --force origin "$diverged:refs/heads/stable"
if run_updater v1.1.0 v1.1.0 >/dev/null 2>&1; then
  echo "divergent stable branch was unexpectedly overwritten" >&2
  exit 1
fi
[ "$(stable_sha)" = "$diverged" ]

echo "stable branch update tests passed"
