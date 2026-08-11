#!/usr/bin/env bash
# Executable tests for vcs-pkgver.sh.
#
# vcs-pkgver.sh runs once per release, inside CI, and its output is committed
# to the AUR as package metadata. A wrong value there is not a build failure —
# it publishes cleanly and is only visible on a web page nobody on the team
# reads. So the failure modes have to be caught here.
#
# The one that matters is the shallow clone: actions/checkout defaults to
# fetch-depth 1, which makes `git rev-list --count HEAD` return 1 no matter how
# many commits exist. That silently produces a pkgver LOWER than the previous
# release's, which is the one thing a version string must never do.
#
# Usage: scripts/aur/test-vcs-pkgver.sh    (exit 0 = all cases pass)
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
script="$here/vcs-pkgver.sh"
repo_root="$(cd "$here/../.." && pwd)"
pass=0
fail=0

# A throwaway git repo with $1 commits, optionally tagged $2 on the last one.
make_repo() {
  local commits="$1" tag="${2:-}" root
  root="$(mktemp -d)"
  git -C "$root" init -q -b main
  git -C "$root" config user.email t@example.com
  git -C "$root" config user.name t
  for i in $(seq 1 "$commits"); do
    echo "$i" >"$root/f"
    git -C "$root" add f
    git -C "$root" commit -qm "c$i"
  done
  [ -n "$tag" ] && git -C "$root" tag "$tag"
  echo "$root"
}

ok() { printf '  ok   %s\n' "$1"; pass=$((pass + 1)); }
no() { printf '  FAIL %s\n     %s\n' "$1" "$2"; fail=$((fail + 1)); }

# Assert the script succeeds and prints exactly $2.
expect_output() {
  local name="$1" want="$2" repo="$3" got status
  got="$("$script" "$repo" 2>&1)"
  status=$?
  if [ "$status" -ne 0 ]; then
    no "$name" "expected success, exited $status: $got"
  elif [ "$got" != "$want" ]; then
    no "$name" "want '$want', got '$got'"
  else
    ok "$name"
  fi
}

# Assert the script fails and says why (a bare non-zero could be any bug).
expect_failure() {
  local name="$1" substr="$2" repo="$3" got status
  got="$("$script" "$repo" 2>&1)"
  status=$?
  if [ "$status" -eq 0 ]; then
    no "$name" "expected failure, but it succeeded with: $got"
  elif [[ "$got" != *"$substr"* ]]; then
    no "$name" "failed as expected, but message lacked '$substr': $got"
  else
    ok "$name"
  fi
}

echo "vcs-pkgver.sh"

# --- format ---------------------------------------------------------------

r="$(make_repo 3 v1.2.3)"
sha="$(git -C "$r" rev-parse --short=7 HEAD)"
expect_output "tagged repo yields <version>.r<count>.g<sha>" "1.2.3.r3.g$sha" "$r"

r="$(make_repo 1 v0.38.0)"
sha="$(git -C "$r" rev-parse --short=7 HEAD)"
expect_output "leading v is stripped from the tag" "0.38.0.r1.g$sha" "$r"

r="$(make_repo 2)"
sha="$(git -C "$r" rev-parse --short=7 HEAD)"
expect_output "untagged repo falls back to 0.0.0" "0.0.0.r2.g$sha" "$r"

# The AUR sorts pkgver with the same rules pacman uses, so the r<count> field
# is what orders two builds of the same tag. It must track real depth.
r="$(make_repo 7 v1.0.0)"
sha="$(git -C "$r" rev-parse --short=7 HEAD)"
expect_output "commit count reflects full history" "1.0.0.r7.g$sha" "$r"

# --- the shallow-clone trap ----------------------------------------------

r="$(make_repo 5 v1.0.0)"
shallow="$(mktemp -d)/clone"
git clone -q --depth 1 "file://$r" "$shallow" 2>/dev/null
expect_failure "shallow clone is refused, not silently counted as r1" \
  "shallow" "$shallow"

# --- misuse ---------------------------------------------------------------

expect_failure "a non-repo path is refused" "not a git repository" "$(mktemp -d)"
expect_failure "a missing path is refused" "does not exist" "/nonexistent/nope"

# --- drift guard ----------------------------------------------------------
#
# The PKGBUILD computes its own pkgver at build time on the user's machine.
# If these two ever disagree on format, the AUR advertises one shape and
# installs another, and version comparison stops meaning anything. Run the
# real pkgver() from the shipped PKGBUILD against the same repo and demand
# byte-identical output.
pkgbuild="$(echo "$repo_root"/aur/*-git/PKGBUILD)"
if [ -f "$pkgbuild" ]; then
  # pkgver() does `cd "$srcdir/<name>"`, where <name> comes from the PKGBUILD's
  # own source= line. Derive it rather than hardcoding, so this file stays
  # identical across repos that vendor it.
  srcname="$(sed -n 's/^source=("\([^:]*\)::git+.*/\1/p' "$pkgbuild" | head -1)"
  if [ -z "$srcname" ]; then
    no "source= names a git checkout directory" "could not parse it from $pkgbuild"
  else
    r="$(make_repo 4 v2.5.1)"
    holder="$(mktemp -d)"
    cp -R "$r" "$holder/$srcname"
    from_pkgbuild="$(
      srcdir="$holder"
      export srcdir
      eval "$(sed -n '/^pkgver() {/,/^}/p' "$pkgbuild")"
      pkgver 2>/dev/null
    )"
    from_script="$("$script" "$holder/$srcname" 2>&1)"
    if [ "$from_pkgbuild" = "$from_script" ] && [ -n "$from_script" ]; then
      ok "output matches the PKGBUILD's own pkgver() byte for byte"
    else
      no "output matches the PKGBUILD's own pkgver() byte for byte" \
        "PKGBUILD gave '$from_pkgbuild', script gave '$from_script'"
    fi
  fi
else
  no "an aur/*-git/PKGBUILD is present" "not found under $repo_root/aur"
fi

echo
echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
