#!/usr/bin/env bash
set -euo pipefail

# Generates the kache APT repository structure using reprepro.
# Supports multi-channel: stable releases go to both the stable and unstable
# suites; unstable releases go to unstable only.
#
# Required env vars:
#   KMS_KEY       - Full GCP KMS key version path for kunobi-pgp-kms
#   APT_CHANNEL   - release channel ("stable" or "unstable")
#   CERT_PATH     - path to the materialized OpenPGP cert (from setup-pgp-kms-signing)
#
# Optional env vars:
#   DEBS_DIR      - directory containing .deb files (default: /tmp/debs)
#   APT_REPO_DIR  - output repo directory (default: /tmp/apt-repo)
#   CONF_DIR      - directory containing distributions config (default: ./apt_config)
#   APT_STATE_DIR - directory for persistent db (local testing only)
#
# R2 db persistence (download only):
#   If R2 credentials are set (R2_ACCESS_KEY_ID, R2_BUCKET, etc.), the script
#   downloads the existing reprepro db/ from R2 (prefix kache/apt-state/db/)
#   before generating. publish.sh uploads it back AFTER public files are
#   confirmed, so db/ is never ahead of the published state.

: "${KMS_KEY:?KMS_KEY is required}"
: "${APT_CHANNEL:?APT_CHANNEL is required (stable or unstable)}"
: "${CERT_PATH:?CERT_PATH is required (path to materialized OpenPGP cert)}"

DEBS_DIR="${DEBS_DIR:-/tmp/debs}"
APT_REPO_DIR="${APT_REPO_DIR:-/tmp/apt-repo}"
CONF_DIR="${CONF_DIR:-./apt_config}"

export REPREPRO_BASE_DIR="${APT_REPO_DIR}"
mkdir -p "${APT_REPO_DIR}/conf"

# ── Restore persistent db from R2 (if credentials available) ──
# First run: no db/ on R2 yet, reprepro bootstraps a fresh one.
if [[ -n "${R2_ACCESS_KEY_ID:-}" && -n "${R2_BUCKET:-}" ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  # shellcheck source=scripts/apt/configure-rclone.sh
  source "${SCRIPT_DIR}/configure-rclone.sh"
  echo "Downloading reprepro db/ from R2 (kache/apt-state/db/)..."
  rclone copy "r2:${R2_BUCKET}/kache/apt-state/db/" "${APT_REPO_DIR}/db/" \
    --quiet 2>/dev/null || echo "No existing db/ on R2 (first run — bootstrapping)"
fi

# ── Restore from local state dir (for local testing) ──
if [[ -n "${APT_STATE_DIR:-}" && -d "${APT_STATE_DIR}/db" ]]; then
  echo "Restoring reprepro db/ from ${APT_STATE_DIR}..."
  cp -r "${APT_STATE_DIR}/db" "${APT_REPO_DIR}/db"
fi

# ── Install distributions config verbatim (no SignWith — we sign manually) ──
cp "${CONF_DIR}/distributions" "${APT_REPO_DIR}/conf/distributions"

# ── Select target suites ──
# Stable releases land in BOTH suites (unstable users also get stable updates).
# Unstable releases land in unstable only.
if [[ "${APT_CHANNEL}" == "stable" ]]; then
  DISTS=("stable" "unstable")
elif [[ "${APT_CHANNEL}" == "unstable" ]]; then
  DISTS=("unstable")
else
  echo "Error: APT_CHANNEL must be 'stable' or 'unstable', got '${APT_CHANNEL}'" >&2
  exit 1
fi

shopt -s nullglob
debs=("${DEBS_DIR}"/*.deb)
shopt -u nullglob

if [[ ${#debs[@]} -eq 0 ]]; then
  echo "Error: No .deb files found in ${DEBS_DIR}" >&2
  exit 1
fi

# ── Debian version ──
# No normalization needed: cargo-deb already produces a Debian-correct version.
# It maps the crate's semver prerelease (e.g. 0.7.0-rc.3) to "0.7.0~rc.3-1" —
# the '~' sorts BELOW the eventual GA "0.7.0" (so apt still offers GA to
# pre-release users) and "-1" is the Debian revision.
#
# The previous repack here was both redundant and broken on cargo-deb's output:
# `${current/-/~}` targeted the "-1" revision separator (not a prerelease
# hyphen), and the replacement '~' underwent tilde expansion → $HOME leaked in,
# yielding an invalid Version like "0.7.0~rc.3/home/runner1".

# ── Add packages to suites ──
for deb in "${debs[@]}"; do
  for dist in "${DISTS[@]}"; do
    echo "Adding ${deb} to ${dist}..."
    reprepro includedeb "${dist}" "${deb}"
  done
done

# ── Sign Release files with kunobi-pgp-kms ──
ARGS_BASE=(--kms-key "${KMS_KEY}" --cert "${CERT_PATH}")

for dist in "${DISTS[@]}"; do
  SUITE="${dist}"
  echo "Signing APT Release for ${SUITE}..."
  kunobi-pgp-kms clearsign   "${ARGS_BASE[@]}" \
    --in  "${APT_REPO_DIR}/dists/${SUITE}/Release" \
    --out "${APT_REPO_DIR}/dists/${SUITE}/InRelease"
  kunobi-pgp-kms detach-sign "${ARGS_BASE[@]}" \
    --in  "${APT_REPO_DIR}/dists/${SUITE}/Release" \
    --out "${APT_REPO_DIR}/dists/${SUITE}/Release.gpg"
done

# ── Publish gpg.key (the KMS-rooted cert — the single trust root) ──
cat "${CERT_PATH}" > "${APT_REPO_DIR}/gpg.key"

echo "Generated repository structure:"
find "${APT_REPO_DIR}" -not -path '*/db/*' -not -path '*/conf/*' | sort
