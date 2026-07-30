# Multi-stage build for the ravel-server container image (ADR-0034, decision 5).
#
# One multi-mode server image: ravel-server runs `--mode all|gateway|query|
# maintain`, selected at runtime by the operator via the CRD-driven Deployment
# spec, so no mode is baked in here. ravel-cli ships in the same image for
# one-shot maintain and inspection use.
#
# This Dockerfile is designed to grow a second final stage (the operator image)
# in a later task of the epic; today it has a single runtime target, `server`.
# Stages are named so `--target server` builds only the server image.

# ---- Builder ----------------------------------------------------------------
# Pinned to the workspace toolchain (rust-toolchain.toml channel = 1.97.1) so
# the image build uses the same compiler CI and local development pin to.
FROM rust:1.97.1-bookworm AS builder

WORKDIR /app

# All proto compilation in this workspace uses protox (pure Rust); no protoc,
# no network, and no extra apt packages are needed in the builder.
#
# The whole workspace is copied in one layer rather than doing a manifest-only
# dependency pre-build. That optimization only speeds up incremental rebuilds,
# and it is fragile here: every internal crate is a path dependency, so a
# manifest-only cache layer is invalidated by any crate source change anyway.
# For a from-scratch image build (the CI and release case) it buys nothing, so
# it is left out deliberately to keep the file simple and correct. .dockerignore
# keeps the build context small.
COPY . .

# ravel-server with the `sql` feature (POST /api/v1/sql). `flight-sql` stays
# off while unimplemented (ADR-0034 decision 5). ravel-cli has no feature flags
# and is built plain. --locked builds against the committed Cargo.lock.
RUN cargo build --release --locked -p ravel-server --features sql \
    && cargo build --release --locked -p ravel-cli

# ---- Runtime: server image --------------------------------------------------
# distroless/cc: glibc (no untested musl), ships ca-certificates for
# object_store's TLS path against real AWS S3, and carries no shell or package
# manager. The :nonroot tag runs as an unprivileged user by default.
FROM gcr.io/distroless/cc-debian12:nonroot AS server

COPY --from=builder /app/target/release/ravel-server /usr/local/bin/ravel-server
COPY --from=builder /app/target/release/ravel-cli /usr/local/bin/ravel-cli

# Documentation only: HTTP (OTLP/HTTP, health) and gRPC (OTLP/gRPC) ports.
EXPOSE 4318 4317

# No default CMD: the operator supplies every argument (--mode, --store, listen
# addresses, tenant tokens, ...) from the CRD, so baking defaults in would only
# create a second source of truth. A bare `docker run <image>` therefore runs
# `ravel-server` with no args, which prints usage and exits; `docker run <image>
# --help` is the runtime smoke test.
ENTRYPOINT ["/usr/local/bin/ravel-server"]
