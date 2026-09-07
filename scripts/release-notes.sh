#!/usr/bin/env bash
# Print GitHub Release notes for a tag, grouped from conventional commits.
#
# Usage:
#   scripts/release-notes.sh            # latest v* tag
#   scripts/release-notes.sh v0.17.0
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

if ! command -v git-cliff >/dev/null 2>&1; then
  echo "git-cliff not found — install with: cargo binstall -y git-cliff" >&2
  exit 1
fi

tag="${1:-}"
if [ -z "$tag" ]; then
  tag="$(git describe --tags --abbrev=0 --match 'v*')"
fi
case "$tag" in
  v*) ;;
  *) echo "tag must look like vX.Y.Z, got: $tag" >&2; exit 1 ;;
esac
git rev-parse -q --verify "refs/tags/$tag" >/dev/null \
  || { echo "tag $tag is not in this clone" >&2; exit 1; }

prev="$(git describe --tags --abbrev=0 --match 'v*' "$tag^" 2>/dev/null || true)"
version="${tag#v}"

echo "Install with \`cargo install kache --locked\` or \`mise use -g github:kunobi-ninja/kache@${version}\`."
echo
git-cliff --config "$root/cliff.toml" "${prev:+$prev..}""$tag" 2>/dev/null
if [ -n "$prev" ]; then
  echo
  echo "**Full changelog:** https://github.com/kunobi-ninja/kache/compare/${prev}...${tag}"
fi
