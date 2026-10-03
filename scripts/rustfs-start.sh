#!/usr/bin/env bash
# Start a throwaway RustFS on a loopback port and create the named buckets.
#
# Downloads a pinned RustFS release (checksum-verified, cached under
# $KACHE_E2E_CACHE) and starts it in the background with an empty data
# directory. Prints shell assignments for the caller to eval: RUSTFS_PID,
# RUSTFS_DATA, RUSTFS_LOG, RUSTFS_ENDPOINT, RUSTFS_ACCESS_KEY and
# RUSTFS_SECRET_KEY. The caller stops the server and removes its data. The
# server is not the caller's child: wait for it with `kill -0`, not `wait`.
#
# Usage: rustfs=$(scripts/rustfs-start.sh BUCKET...) && eval "$rustfs"
set -euo pipefail

RUSTFS_VERSION=1.0.0
cache=${KACHE_E2E_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/kache-e2e}

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    asset=rustfs-linux-x86_64-gnu-v$RUSTFS_VERSION.zip
    sha256=2d5059501745682664c3d345b22274b66079c952fbec7e1ce66980ef4515cd42
    ;;
  Linux-aarch64)
    asset=rustfs-linux-aarch64-gnu-v$RUSTFS_VERSION.zip
    sha256=780e832d68e0148dc042f05647796056fe014e7cf1a8f195e3e83b22a3bb988f
    ;;
  Darwin-arm64)
    asset=rustfs-macos-aarch64-v$RUSTFS_VERSION.zip
    sha256=06e32a681c16930fb5414df64c96151fe3370321fab0403a83a83a015874c39a
    ;;
  *)
    echo "no RustFS $RUSTFS_VERSION build for $(uname -s)-$(uname -m)" >&2
    exit 1
    ;;
esac

sha256_of() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

bin_dir=$cache/rustfs-$RUSTFS_VERSION
rustfs=$bin_dir/rustfs
if [[ ! -x $rustfs ]]; then
  mkdir -p "$bin_dir"
  zip=$bin_dir/$asset
  curl -fsSL -o "$zip" \
    "https://github.com/rustfs/rustfs/releases/download/$RUSTFS_VERSION/$asset"
  actual=$(sha256_of "$zip")
  if [[ $actual != "$sha256" ]]; then
    echo "$asset: SHA-256 $actual, expected $sha256" >&2
    rm -f "$zip"
    exit 1
  fi
  unzip -oq "$zip" -d "$bin_dir"
  rm -f "$zip"
  chmod +x "$rustfs"
fi

data=$(mktemp -d)
log=$data.log
port=$((20000 + RANDOM % 20000))
access_key=kache-e2e
secret_key=kache-e2e-secret
"$rustfs" server "$data" --address "127.0.0.1:$port" \
  --access-key "$access_key" --secret-key "$secret_key" >"$log" 2>&1 </dev/null &
server=$!

fail() {
  echo "$1" >&2
  cat "$log" >&2
  kill "$server" 2>/dev/null || true
  rm -rf "$data" "$log"
  exit 1
}

endpoint=http://127.0.0.1:$port
for _ in $(seq 60); do
  curl -fs -o /dev/null "$endpoint/health" && break
  kill -0 "$server" 2>/dev/null || fail "RustFS exited during startup"
  sleep 0.5
done
curl -fs -o /dev/null "$endpoint/health" || fail "RustFS never became healthy"

# Health can report ready before the object layer is; retry the first write.
for bucket in "$@"; do
  created=
  for _ in $(seq 60); do
    if curl -fs -o /dev/null -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$access_key:$secret_key" "$endpoint/$bucket"; then
      created=1
      break
    fi
    sleep 0.5
  done
  [[ -n $created ]] || fail "RustFS never accepted creating bucket $bucket"
done

printf 'RUSTFS_PID=%q\n' "$server"
printf 'RUSTFS_DATA=%q\n' "$data"
printf 'RUSTFS_LOG=%q\n' "$log"
printf 'RUSTFS_ENDPOINT=%q\n' "$endpoint"
printf 'RUSTFS_ACCESS_KEY=%q\n' "$access_key"
printf 'RUSTFS_SECRET_KEY=%q\n' "$secret_key"
