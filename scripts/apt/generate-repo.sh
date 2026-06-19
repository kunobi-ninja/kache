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

# ── Normalize Debian pre-release version (repack) ──
# cargo-deb stamps the crate version verbatim, so a pre-release tag produces a
# control Version like "0.7.0-rc.1". In Debian, '-' separates upstream from the
# Debian revision and "0.7.0-rc.1" sorts ABOVE "0.7.0" — apt would never offer
# the GA to a pre-release user. Rewrite the FIRST '-' to '~' (tilde sorts below
# anything, including GA) inside the control file, keeping the .deb FILENAME's
# hyphen untouched. Idempotent: skips .debs whose Version has no '-'.
for deb in "${debs[@]}"; do
  current=$(dpkg-deb -f "${deb}" Version)
  case "${current}" in
    *-*)
      desired="${current/-/~}"
      echo "Normalizing ${deb##*/}: Version ${current} -> ${desired}"
      tmp="$(mktemp -d)"
      dpkg-deb -R "${deb}" "${tmp}"
      sed -i.bak "s|^Version: .*|Version: ${desired}|" "${tmp}/DEBIAN/control"
      rm "${tmp}/DEBIAN/control.bak"
      # --root-owner-group: keep files owned by root:root (else repack stamps
      # the runner's uid/gid onto installed files on user machines).
      dpkg-deb --root-owner-group -b "${tmp}" "${deb}" >/dev/null
      rm -rf "${tmp}"
      ;;
    *)
      echo "Version ${current} for ${deb##*/} needs no normalization"
      ;;
  esac
done

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
