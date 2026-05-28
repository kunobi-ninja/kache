#!/usr/bin/env bash
set -euo pipefail

tag="${1:-}"

if [[ -z "$tag" ]]; then
  echo "usage: $0 vX.Y.Z" >&2
  exit 2
fi

if [[ "$tag" != v* || "$tag" == "v" ]]; then
  echo "release tag must look like vX.Y.Z, got: $tag" >&2
  exit 1
fi

version="${tag#v}"
metadata="$(cargo metadata --no-deps --format-version 1)"

package_version() {
  local package="$1"
  METADATA="$metadata" python3 - "$package" <<'PY'
import json
import os
import sys

package = sys.argv[1]
metadata = json.loads(os.environ["METADATA"])
versions = [p["version"] for p in metadata["packages"] if p["name"] == package]
if len(versions) != 1:
    raise SystemExit(f"expected exactly one package named {package}, found {len(versions)}")
print(versions[0])
PY
}

root_version="$(package_version kache)"
core_version="$(package_version kache-core)"

if [[ "$root_version" != "$version" ]]; then
  echo "kache version $root_version does not match release tag $tag" >&2
  exit 1
fi

if [[ "$core_version" != "$version" ]]; then
  echo "kache-core version $core_version does not match release tag $tag" >&2
  exit 1
fi

echo "release tag $tag matches kache and kache-core version $version"
