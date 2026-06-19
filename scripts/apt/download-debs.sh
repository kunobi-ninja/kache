#!/usr/bin/env bash
set -euo pipefail

# Downloads .deb packages from R2 — FALLBACK path only.
#
# The package-publish.yml workflow's PRIMARY path is `gh release download`,
# pulling the .deb assets straight off the GitHub Release. This script exists
# for manual/local runs where the .debs live on R2 instead (e.g. re-publishing
# from object storage without a fresh release event).
#
# Required env vars:
#   VERSION   - package version (e.g. 0.7.0-rc.1)
#   R2_URL    - R2 base URL (e.g. https://r2.kunobi.com)
#   ARCHS     - space-separated Debian architectures (e.g. "amd64 arm64")
#
# Optional env vars:
#   DEBS_DIR  - output directory (default: /tmp/debs)

: "${VERSION:?VERSION is required}"
: "${R2_URL:?R2_URL is required}"
: "${ARCHS:?ARCHS is required}"

DEBS_DIR="${DEBS_DIR:-/tmp/debs}"
mkdir -p "${DEBS_DIR}"

for arch in ${ARCHS}; do
  # Mirrors the GitHub Release asset naming: kache_<version>_<arch>.deb
  url="${R2_URL}/kache/releases/v${VERSION}/kache_${VERSION}_${arch}.deb"
  dest="${DEBS_DIR}/kache_${VERSION}_${arch}.deb"
  echo "Downloading ${url}..."
  curl -sfL -o "${dest}" "${url}"
done

echo "Downloaded .deb files:"
ls -la "${DEBS_DIR}/"
