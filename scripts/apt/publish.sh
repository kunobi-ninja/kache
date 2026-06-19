#!/usr/bin/env bash
set -euo pipefail

# Publishes the kache APT repository to R2 (prefix kache/apt/) using surgical
# per-suite uploads. Only uploads dists/ for suites that were modified,
# preserving the other channel's metadata on R2. Pool uploads are additive.
#
# Served at: https://r2.kunobi.com/kache/apt/
#
# Required env vars:
#   R2_ACCESS_KEY_ID      - Cloudflare R2 access key
#   R2_SECRET_ACCESS_KEY  - Cloudflare R2 secret key
#   R2_ACCOUNT_ID         - Cloudflare account ID
#   R2_BUCKET             - R2 bucket name
#   APT_CHANNEL           - release channel ("stable" or "unstable")
#
# Optional env vars:
#   APT_REPO_DIR  - repo directory to publish (default: /tmp/apt-repo)

: "${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID is required}"
: "${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY is required}"
: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID is required}"
: "${R2_BUCKET:?R2_BUCKET is required}"
: "${APT_CHANNEL:?APT_CHANNEL is required (stable or unstable)}"

APT_REPO_DIR="${APT_REPO_DIR:-/tmp/apt-repo}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/apt/configure-rclone.sh
source "${SCRIPT_DIR}/configure-rclone.sh"

if [[ "${APT_CHANNEL}" == "stable" ]]; then
  DISTS=("stable" "unstable")
elif [[ "${APT_CHANNEL}" == "unstable" ]]; then
  DISTS=("unstable")
else
  echo "Error: APT_CHANNEL must be 'stable' or 'unstable', got '${APT_CHANNEL}'" >&2
  exit 1
fi

# Upload dists/ per suite (sync replaces metadata for that codename only).
for dist in "${DISTS[@]}"; do
  if [ -d "${APT_REPO_DIR}/dists/${dist}" ]; then
    echo "Syncing dists/${dist}/ to R2..."
    rclone sync \
      "${APT_REPO_DIR}/dists/${dist}/" \
      "r2:${R2_BUCKET}/kache/apt/dists/${dist}/" \
      --progress \
      --checksum
  fi
done

# Upload pool (additive — never deletes old .debs from other releases).
if [ -d "${APT_REPO_DIR}/pool" ]; then
  echo "Copying pool/ to R2 (additive)..."
  rclone copy \
    "${APT_REPO_DIR}/pool/" \
    "r2:${R2_BUCKET}/kache/apt/pool/" \
    --progress \
    --checksum
fi

# Upload GPG public key.
if [ -f "${APT_REPO_DIR}/gpg.key" ]; then
  echo "Uploading gpg.key..."
  rclone copyto \
    "${APT_REPO_DIR}/gpg.key" \
    "r2:${R2_BUCKET}/kache/apt/gpg.key" \
    --checksum
fi

# Persist updated reprepro db/ to R2 LAST — only after public files are confirmed,
# so db/ is never ahead of the actual published state.
if [ -d "${APT_REPO_DIR}/db" ]; then
  echo "Persisting reprepro db/ to R2..."
  rclone sync \
    "${APT_REPO_DIR}/db/" \
    "r2:${R2_BUCKET}/kache/apt-state/db/" \
    --checksum
fi

echo "kache APT repository published to R2 (channel: ${APT_CHANNEL}, suites: ${DISTS[*]})"
