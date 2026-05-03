# syntax=docker/dockerfile:1.7

# Stage 1 — build environment + record GIF
FROM ghcr.io/charmbracelet/vhs AS builder

ARG TARGETARCH
ARG KACHE_VERSION=v0.1.1

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        curl ca-certificates build-essential pkg-config git \
 && rm -rf /var/lib/apt/lists/*

# Rust toolchain (matches kache MSRV)
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --default-toolchain 1.95.0 --profile minimal --no-modify-path

# kache from release (musl static binary)
RUN set -eux; \
    case "$TARGETARCH" in \
        amd64) ARCH=x86_64 ;; \
        arm64) ARCH=aarch64 ;; \
        *) echo "unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac; \
    curl -sSL "https://github.com/kunobi-ninja/kache/releases/download/${KACHE_VERSION}/kache-${ARCH}-unknown-linux-musl.tar.gz" \
        -o /tmp/kache.tar.gz; \
    tar -xzf /tmp/kache.tar.gz -C /tmp; \
    install -m 0755 "$(find /tmp -type f -name kache | head -1)" /usr/local/bin/kache; \
    rm -rf /tmp/kache*

ENV RUSTC_WRAPPER=kache

# Demo workload: a tiny synthetic Rust project. Sized to fit comfortably
# inside Docker's default CPU/RAM allocation while still showing real
# kache behaviour (cold compile populates the store; clean+rebuild
# restores everything via hardlinks).
COPY fixtures /workspace
WORKDIR /workspace/project-a
RUN cargo fetch

COPY demo.tape /workspace/project-a/demo.tape
COPY monitor.tape /workspace/project-a/monitor.tape
COPY clean.tape /workspace/project-a/clean.tape

# Record. demo.tape populates the kache cache (cold + warm).
# monitor.tape runs `kache monitor` against that populated cache.
RUN vhs demo.tape
RUN vhs monitor.tape

# Set up sibling project copies so `kache clean` has multiple target/
# dirs to list. `cp -a` preserves hardlinks to kache's store, so each
# copy reports a realistic cached percentage instead of a fresh tree.
RUN cd /workspace \
 && cp -a project-a project-b \
 && cp -a project-a project-c \
 && cp -a project-a project-d

RUN vhs clean.tape

# Stage 2 — only the GIFs, for `docker build --output`
FROM scratch AS export
COPY --from=builder /workspace/project-a/demo.gif /demo.gif
COPY --from=builder /workspace/project-a/monitor.gif /monitor.gif
COPY --from=builder /workspace/project-a/clean.gif /clean.gif
