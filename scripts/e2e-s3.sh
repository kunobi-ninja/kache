#!/usr/bin/env bash
# Run the S3 remote tests against a throwaway local RustFS.
#
# Starts RustFS through scripts/rustfs-start.sh, runs the tests whose names
# match the filter (default `e2e_s3`), then stops the server and deletes its
# data.
#
# Usage: scripts/e2e-s3.sh [test filter]
set -euo pipefail

filter=${1:-e2e_s3}

bucket=kache-e2e
rustfs=$(scripts/rustfs-start.sh "$bucket")
eval "$rustfs"
endpoint=$RUSTFS_ENDPOINT
access_key=$RUSTFS_ACCESS_KEY
secret_key=$RUSTFS_SECRET_KEY
cleanup() {
  kill "$RUSTFS_PID" 2>/dev/null || true
  # RustFS is not this shell's child, so `wait` cannot see it exit. Removing
  # its data while it still writes on the way down fails the run.
  for _ in $(seq 100); do
    kill -0 "$RUSTFS_PID" 2>/dev/null || break
    sleep 0.1
  done
  rm -rf "$RUSTFS_DATA" "$RUSTFS_LOG"
}
trap cleanup EXIT

# A second bucket that anonymous readers may only GET from: a reader without
# s3:ListBucket, as a GetObject-only CI role would be.
read_only_bucket=kache-e2e-read-only
policy=$(printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}' "$read_only_bucket")
curl -fsS -o /dev/null -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$access_key:$secret_key" "$endpoint/$read_only_bucket"
curl -fsS -o /dev/null -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$access_key:$secret_key" --data "$policy" "$endpoint/$read_only_bucket?policy"

output=$(mktemp)
KACHE_E2E_S3_ENDPOINT=$endpoint \
  KACHE_E2E_S3_BUCKET=$bucket \
  KACHE_E2E_S3_READ_ONLY_BUCKET=$read_only_bucket \
  KACHE_S3_ACCESS_KEY=$access_key \
  KACHE_S3_SECRET_KEY=$secret_key \
  RUSTC_WRAPPER="" \
  cargo test -p kache --bin kache -- "$filter" --nocapture 2>&1 | tee "$output"

# The tests return early without the store; make sure they did not.
ran=$(grep -c "^test .*$filter.* \.\.\. ok$" "$output" || true)
rm -f "$output"
if [[ $ran -eq 0 ]]; then
  echo "no test matching '$filter' ran against RustFS" >&2
  exit 1
fi
echo "$ran test(s) passed against RustFS"
