#!/usr/bin/env bash
set -euo pipefail
# curl, not `gh`: the ARC runner image ships no GitHub CLI.
# /releases/latest redirects to the tag, so the newest release is
# one HEAD request and needs no token or JSON parsing.
repo=https://github.com/jdx/mr-boxington
archive=mbx-x86_64-unknown-linux-gnu.tar.gz
dir="$RUNNER_TEMP/mbx"
mkdir -p "$dir"
tag="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "$repo/releases/latest" | sed 's#.*/tag/##')"
case "$tag" in
  v[0-9]*) ;;
  *) echo "::error::could not resolve the latest release tag (got '$tag')"; exit 1 ;;
esac
curl -fsSL -o "$dir/$archive" "$repo/releases/download/$tag/$archive"
curl -fsSL -o "$dir/SHA256SUMS" "$repo/releases/download/$tag/SHA256SUMS"
(cd "$dir" && grep "  $archive$" SHA256SUMS | sha256sum --check --strict -)
tar -xzf "$dir/$archive" -C "$dir"
echo "$dir" >> "$GITHUB_PATH"
echo "mbx release $tag"
"$dir/mbx" --version

      # The harness clones third-party subjects over https. Anonymous
      # requests share this cluster's egress quota, and when it is exhausted
      # GitHub answers 401, which git reports as "could not read Username"
      # and the bench dies before it builds anything. Authenticating with the
      # job's own read-only token moves those fetches onto the much larger
      # per-token limit. Ephemeral pod, so the config dies with the job.
