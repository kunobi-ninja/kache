FROM rust:1.93-bookworm AS builder

WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p kache-service

FROM gcr.io/distroless/cc-debian12

ARG BUILD_VERSION=dev
ARG BUILD_COMMIT=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="kache-service"
LABEL org.opencontainers.image.description="Remote service shell for kache planner endpoints"
LABEL org.opencontainers.image.version="${BUILD_VERSION}"
LABEL org.opencontainers.image.revision="${BUILD_COMMIT}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.source="https://github.com/kunobi-ninja/kache"

COPY --from=builder /app/target/release/kache-service /kache-service

EXPOSE 8080

ENTRYPOINT ["/kache-service"]
