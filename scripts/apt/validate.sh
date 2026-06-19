#!/usr/bin/env bash
set -euo pipefail

# Validates the generated kache APT repository.
# Checks: GPG signatures, Packages indices (amd64 + arm64), Release hashes.
# Validates all suites that were published based on APT_CHANNEL.
#
# Required env vars:
#   APT_CHANNEL   - release channel ("stable" or "unstable")
#   CERT_PATH     - path to OpenPGP cert for the KMS-rooted signing key
#
# Optional env vars:
#   APT_REPO_DIR  - repo directory to validate (default: /tmp/apt-repo)

: "${APT_CHANNEL:?APT_CHANNEL is required (stable or unstable)}"
: "${CERT_PATH:?CERT_PATH is required (OpenPGP cert for the KMS signing key)}"

APT_REPO_DIR="${APT_REPO_DIR:-/tmp/apt-repo}"
ARCHES=(amd64 arm64)

# Import the signing public key into a scratch keyring so gpg --verify can
# resolve the kunobi-pgp-kms signatures.
GNUPGHOME=$(mktemp -d)
export GNUPGHOME
chmod 700 "${GNUPGHOME}"
trap 'rm -rf "${GNUPGHOME}"' EXIT

gpg --batch --import "${CERT_PATH}"

if [[ "${APT_CHANNEL}" == "stable" ]]; then
  DISTS=("stable" "unstable")
elif [[ "${APT_CHANNEL}" == "unstable" ]]; then
  DISTS=("unstable")
else
  echo "Error: APT_CHANNEL must be 'stable' or 'unstable', got '${APT_CHANNEL}'" >&2
  exit 1
fi

for dist in "${DISTS[@]}"; do
  DIST_DIR="${APT_REPO_DIR}/dists/${dist}"

  if [ ! -d "${DIST_DIR}" ]; then
    echo "Error: dists/${dist}/ does not exist" >&2
    exit 1
  fi

  echo "==> Validating suite: ${dist}"

  # --- GPG signatures ---
  echo "  Validating GPG signatures..."
  gpg --verify "${DIST_DIR}/Release.gpg" "${DIST_DIR}/Release"
  gpg --verify "${DIST_DIR}/InRelease"
  echo "  GPG signatures valid"

  # --- Packages indices (amd64 + arm64) ---
  for arch in "${ARCHES[@]}"; do
    echo "  Validating ${arch} Packages index..."
    if [ -f "${DIST_DIR}/main/binary-${arch}/Packages.gz" ]; then
      zcat "${DIST_DIR}/main/binary-${arch}/Packages.gz" | grep -q "^Package: kache"
      echo "  ${arch} Packages index contains kache"
    else
      echo "  Error: ${arch} Packages index not found in ${dist}" >&2
      exit 1
    fi
  done

  # --- Release hashes ---
  echo "  Validating Release hashes..."
  (
    cd "${DIST_DIR}"
    awk '/^SHA256:/{found=1; next} /^[^ ]/{found=0} found{print $1 "  " $3}' Release \
      | while read -r hash file; do
          if [ -f "${file}" ]; then
            echo "${hash}  ${file}" | sha256sum -c -
          else
            echo "Error: Release lists '${file}' but file is missing" >&2
            exit 1
          fi
        done
  )
  echo "  All Release hashes verified for ${dist}"
  echo ""
done

echo "All validations passed (suites: ${DISTS[*]})"
