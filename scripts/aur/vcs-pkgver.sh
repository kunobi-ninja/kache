#!/usr/bin/env bash
# Compute the pkgver for a VCS (-git) AUR package.
#
# Emits `<version>.r<count>.g<sha>`, byte-identical to what the pkgver()
# function inside aur/*-git/PKGBUILD produces at build time. The two must
# agree: the AUR advertises this value as metadata, makepkg recomputes it on
# the user's machine, and if the formats diverge then version comparison
# between "what the AUR says" and "what got installed" stops meaning anything.
#
# Usage: scripts/aur/vcs-pkgver.sh [repo-dir]     (default: cwd)
set -uo pipefail

repo="${1:-.}"

if [ ! -e "$repo" ]; then
  echo "vcs-pkgver: '$repo' does not exist" >&2
  exit 1
fi

if ! git -C "$repo" rev-parse --git-dir >/dev/null 2>&1; then
  echo "vcs-pkgver: '$repo' is not a git repository" >&2
  exit 1
fi

# A shallow clone silently breaks this. `git rev-list --count HEAD` counts only
# the commits it actually has, so a depth-1 checkout reports 1 regardless of
# real history — and actions/checkout defaults to depth 1. That would publish a
# pkgver LOWER than the previous release's, which pacman reads as a downgrade
# and which no build failure would ever surface. Refuse instead of guessing.
if [ "$(git -C "$repo" rev-parse --is-shallow-repository 2>/dev/null)" = "true" ]; then
  echo "vcs-pkgver: '$repo' is a shallow clone — the commit count would be wrong." >&2
  echo "            Fetch full history first (actions/checkout: fetch-depth: 0)." >&2
  exit 1
fi

if ! git -C "$repo" rev-parse HEAD >/dev/null 2>&1; then
  echo "vcs-pkgver: '$repo' has no commits" >&2
  exit 1
fi

# Mirrors aur/*-git/PKGBUILD's pkgver() exactly, including the 0.0.0 fallback
# for a repo with no tags yet.
version="$(git -C "$repo" describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')"
[ -n "$version" ] || version="0.0.0"
count="$(git -C "$repo" rev-list --count HEAD)"
sha="$(git -C "$repo" rev-parse --short=7 HEAD)"

printf '%s.r%s.g%s\n' "$version" "$count" "$sha"
