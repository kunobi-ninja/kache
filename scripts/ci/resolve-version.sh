#!/usr/bin/env bash
# Resolve release version and channel for the kache publish workflow.
#
# Inputs (env vars):
#   VERSION            – version string (with or without leading 'v') or "latest"
#   CHANNEL            – "stable", "unstable", "auto", or "" (default: "auto")
#   GITHUB_REPOSITORY  – owner/repo for gh release list (required)
#   GH_TOKEN           – GitHub token (required)
#
# Outputs:
#   stdout:        version=X.Y.Z and channel=stable|unstable (one per line)
#   GITHUB_OUTPUT: same key=value pairs appended (when set)
#
# Channel inference (kache semver):
#   vX.Y.Z            -> stable
#   vX.Y.Z-rc.N       -> unstable
#   vX.Y.Z-beta.N     -> unstable

set -euo pipefail

VERSION="${VERSION:-latest}"
CHANNEL="${CHANNEL:-auto}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${GH_TOKEN:?GH_TOKEN is required}"

# Normalize a possibly-empty / "auto" channel coming from workflow_dispatch.
[[ -z "$CHANNEL" ]] && CHANNEL="auto"

case "$CHANNEL" in
  auto|stable|unstable) ;;
  *)
    echo "Error: Invalid CHANNEL '$CHANNEL' (must be one of: auto stable unstable)" >&2
    exit 1
    ;;
esac

# Strip a leading 'v' from explicit versions so the rest of the script works
# on bare semver.
VERSION="${VERSION#v}"

# ---------------------------------------------------------------------------
# Resolve version ("latest" → most recent release, filtered by channel)
# ---------------------------------------------------------------------------
if [[ -z "$VERSION" || "$VERSION" == "latest" ]]; then
  if [[ "$CHANNEL" == "stable" ]]; then
    JQ_FILTER='[.[].tagName | select(test("^v[0-9]+\\.[0-9]+\\.[0-9]+$"))] | first // "" | ltrimstr("v")'
  elif [[ "$CHANNEL" == "unstable" ]]; then
    JQ_FILTER='[.[].tagName | select(test("^v[0-9]+\\.[0-9]+\\.[0-9]+-(rc|beta|alpha)"))] | first // "" | ltrimstr("v")'
  else
    # Any release (stable or pre-release)
    JQ_FILTER='[.[].tagName | select(test("^v[0-9]+\\.[0-9]+\\.[0-9]+(-(rc|beta|alpha).*)?$"))] | first // "" | ltrimstr("v")'
  fi

  VERSION=$(gh release list --repo "$GITHUB_REPOSITORY" --limit 50 --json tagName --jq "$JQ_FILTER")

  if [[ -z "$VERSION" ]]; then
    echo "Error: No matching release found in $GITHUB_REPOSITORY (channel=$CHANNEL)" >&2
    exit 1
  fi
else
  # Validate the explicit version exists as a release.
  if ! gh release view "v${VERSION}" --repo "$GITHUB_REPOSITORY" --json tagName &>/dev/null; then
    echo "Error: No matching release found in $GITHUB_REPOSITORY (version=v${VERSION})" >&2
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Resolve channel (infer from version if empty or "auto")
# ---------------------------------------------------------------------------
if [[ -z "$CHANNEL" || "$CHANNEL" == "auto" ]]; then
  if [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    CHANNEL="stable"
  elif [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+-(rc|beta|alpha) ]]; then
    CHANNEL="unstable"
  else
    echo "Error: Cannot infer channel from version '$VERSION'" >&2
    exit 1
  fi
fi

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
echo "Resolved version: $VERSION" >&2
echo "Resolved channel: $CHANNEL" >&2

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "version=$VERSION" >> "$GITHUB_OUTPUT"
  echo "channel=$CHANNEL" >> "$GITHUB_OUTPUT"
fi

echo "version=$VERSION"
echo "channel=$CHANNEL"
