#!/usr/bin/env bash
# Native build dependencies must exist before mise installs tools from source.
set -euo pipefail

missing=()
for tool in cc c++ make clang cmake ninja pkg-config nasm wget; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
done
if ((${#missing[@]})); then
    printf 'Installing native build tools; missing: %s\n' "${missing[*]}"
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends \
        build-essential clang cmake ninja-build pkg-config libssl-dev nasm wget
fi

for tool in cc c++ make clang cmake ninja pkg-config nasm wget; do
    command -v "$tool"
done
