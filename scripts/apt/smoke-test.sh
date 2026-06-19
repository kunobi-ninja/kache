#!/usr/bin/env bash
set -euo pipefail

# Tests live kache APT repository URLs after publishing.
# Only tests suites that were published based on APT_CHANNEL.
#
# Required env vars:
#   APT_CHANNEL   - release channel ("stable" or "unstable")
#
# Optional env vars:
#   BASE_URL    - APT repo base URL (default: https://r2.kunobi.com/kache/apt)
#   CERT_PATH   - materialized cert; when set, asserts its fingerprint is in
#                 the served gpg.key

: "${APT_CHANNEL:?APT_CHANNEL is required (stable or unstable)}"

BASE_URL="${BASE_URL:-https://r2.kunobi.com/kache/apt}"
REPO_URL="${BASE_URL}"
ARCHES=(amd64 arm64)

GNUPGHOME=$(mktemp -d)
export GNUPGHOME
chmod 700 "${GNUPGHOME}"
trap 'rm -rf "${GNUPGHOME}"' EXIT

if [[ "${APT_CHANNEL}" == "stable" ]]; then
  DISTS=("stable" "unstable")
elif [[ "${APT_CHANNEL}" == "unstable" ]]; then
  DISTS=("unstable")
else
  echo "Error: APT_CHANNEL must be 'stable' or 'unstable', got '${APT_CHANNEL}'" >&2
  exit 1
fi

# Download + import the published gpg.key the way real users do.
echo "Testing gpg.key at ${BASE_URL}/gpg.key..."
GPG_KEY_FILE=$(mktemp)
curl -sf "${BASE_URL}/gpg.key" -o "${GPG_KEY_FILE}"
gpg --with-colons --import-options show-only --import "${GPG_KEY_FILE}" | grep -q "pub"
gpg --batch --import "${GPG_KEY_FILE}"
rm -f "${GPG_KEY_FILE}"
echo "GPG key valid"

for dist in "${DISTS[@]}"; do
  echo ""
  echo "==> Testing suite: ${dist}"

  echo "  Testing InRelease at ${BASE_URL}/dists/${dist}/InRelease..."
  tmp_inrelease=$(mktemp)
  curl -sf "${BASE_URL}/dists/${dist}/InRelease" -o "${tmp_inrelease}"
  gpg --verify "${tmp_inrelease}"
  rm -f "${tmp_inrelease}"

  for arch in "${ARCHES[@]}"; do
    echo "  Testing Packages.gz at ${BASE_URL}/dists/${dist}/main/binary-${arch}/Packages.gz..."
    curl -sf "${BASE_URL}/dists/${dist}/main/binary-${arch}/Packages.gz" \
      | zcat | grep -q "^Package: kache"
  done
  echo "  ${dist} smoke tests passed"
done

# Assert the KMS-rooted key fingerprint is present in the served gpg.key.
if [ -n "${CERT_PATH:-}" ] && [ -f "${CERT_PATH}" ]; then
  NEW_FPR=$(gpg --with-colons --show-keys "${CERT_PATH}" \
            | awk -F: '/^fpr/{print $10; exit}')
  if [ -n "$NEW_FPR" ]; then
    PUBLISHED_KEY=$(mktemp)
    curl -sf "${REPO_URL}/gpg.key" -o "${PUBLISHED_KEY}"
    if ! gpg --with-colons --show-keys "${PUBLISHED_KEY}" \
         | awk -F: '/^fpr/{print $10}' | grep -q "$NEW_FPR"; then
      rm -f "${PUBLISHED_KEY}"
      echo "FAIL: key fingerprint $NEW_FPR not in published gpg.key"
      exit 1
    fi
    rm -f "${PUBLISHED_KEY}"
    echo "KMS key fingerprint $NEW_FPR verified in published gpg.key"
  fi
fi

echo ""
echo "All smoke tests passed (suites: ${DISTS[*]})"
