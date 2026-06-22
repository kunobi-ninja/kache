# Toolchain image for running the kache e2e harness from a non-Linux host
# (e.g. macOS), so a dev gets CI-equivalent results locally — same Linux
# clang/gcc the `E2E smoke (Linux)` job uses. Driven by `just e2e-docker`.
#
# The harness runs against a copy of the tree on a CONTAINER-NATIVE volume,
# NOT a bind mount: kache restores cache hits via hardlink and the harness
# relocates source trees, both of which fail on Docker Desktop's macOS
# bind-mount filesystem (cross-device hardlinks; virtiofs permission quirks
# on copy). A native volume behaves like CI's real Linux fs, and keeps the
# multi-GB cargo target OFF the host disk — only the small results.json is
# copied back to the host. (`just e2e-docker` wires this up; `rsync` syncs
# the source in, excluding .git/target/tmp.)
#
# Rust pin matches docker/service.Dockerfile. build-essential gives
# cc/c++/make for the gnu C fixtures; clang adds the clang / clang-cl
# (`--driver-mode=cl`) path the Firefox-flag fixture (issue #411) exercises.
FROM rust:1.95-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential clang make rsync ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /work
