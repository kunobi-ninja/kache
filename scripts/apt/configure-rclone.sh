#!/usr/bin/env bash
set -euo pipefail

# Configures the rclone r2: remote for Cloudflare R2 access.
# Sourced by generate-repo.sh and publish.sh.
#
# Required env vars:
#   R2_ACCESS_KEY_ID      - Cloudflare R2 access key
#   R2_SECRET_ACCESS_KEY  - Cloudflare R2 secret key
#   R2_ACCOUNT_ID         - Cloudflare account ID

: "${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID is required}"
: "${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY is required}"
: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID is required}"

if [ ! -f ~/.config/rclone/rclone.conf ]; then
  mkdir -p ~/.config/rclone
  cat > ~/.config/rclone/rclone.conf <<EOF
[r2]
type = s3
provider = Cloudflare
access_key_id = ${R2_ACCESS_KEY_ID}
secret_access_key = ${R2_SECRET_ACCESS_KEY}
endpoint = https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com
acl = private
no_check_bucket = true
EOF
fi
