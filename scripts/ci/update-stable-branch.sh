#!/usr/bin/env bash
# Move the Nix-facing `stable` branch to the latest stable release tag.
#
# The caller is responsible for resolving the authoritative latest GitHub
# release and for checking its release CI. Keeping the ref update here makes
# the destructive part small, testable, and race-safe.
#
# Usage: update-stable-branch.sh <release-tag> <latest-stable-tag>
# Env:   STABLE_BRANCH_REMOTE (default: origin; overridden by the local test)
set -euo pipefail

tag="${1:-}"
latest_tag="${2:-}"
[ -n "$tag" ] && [ -n "$latest_tag" ] || {
  echo "usage: update-stable-branch.sh <release-tag> <latest-stable-tag>" >&2
  exit 2
}

stable_tag_pattern='^v[0-9]+\.[0-9]+\.[0-9]+$'
if [[ ! "$tag" =~ $stable_tag_pattern ]]; then
  echo "refusing non-stable release tag: $tag" >&2
  exit 1
fi
if [[ ! "$latest_tag" =~ $stable_tag_pattern ]]; then
  echo "refusing invalid latest stable tag: $latest_tag" >&2
  exit 1
fi

# A delayed/re-run release event must never rewind the branch.
if [ "$tag" != "$latest_tag" ]; then
  echo "$tag is not the latest stable release ($latest_tag); leaving stable unchanged"
  exit 0
fi

remote="${STABLE_BRANCH_REMOTE:-origin}"
target="$(git rev-parse --verify "refs/tags/$tag^{commit}")"
current="$(git ls-remote --refs "$remote" refs/heads/stable | awk 'NR == 1 { print $1 }')"

if [ "$current" = "$target" ]; then
  echo "stable already points to $tag ($target)"
  exit 0
fi

if [ -z "$current" ]; then
  # Creation is non-forced, so a concurrent creator wins instead of being
  # overwritten.
  git push "$remote" "$target:refs/heads/stable"
else
  # Releases are cut from main, so every GA update must be a fast-forward.
  # Fetch the observed object explicitly: ls-remote tells us its identity but
  # does not guarantee that the commit exists in this checkout.
  git fetch --quiet "$remote" refs/heads/stable
  if ! git merge-base --is-ancestor "$current" "$target"; then
    echo "refusing non-fast-forward stable move: $current -> $target" >&2
    exit 1
  fi

  # Pin the value observed above. A human or another workflow moving the ref
  # between read and write makes this fail closed rather than clobbering it.
  git push "$remote" \
    "--force-with-lease=refs/heads/stable:$current" \
    "$target:refs/heads/stable"
fi

published="$(git ls-remote --refs "$remote" refs/heads/stable | awk 'NR == 1 { print $1 }')"
if [ "$published" != "$target" ]; then
  echo "stable verification failed: expected $target, found ${published:-<missing>}" >&2
  exit 1
fi

echo "stable now points to $tag ($target)"
