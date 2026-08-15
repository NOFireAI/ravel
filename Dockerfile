# Multi-stage build for the ravel-server container image (ADR-0034, decision 5).
#
# One multi-mode server image: ravel-server runs `--mode all|gateway|query|
# maintain`, selected at runtime by the operator via the CRD-driven Deployment
# spec, so no mode is baked in here. ravel-cli ships in the same image for
# one-shot maintain and inspection use.
#
# This Dockerfile has two final runtime targets: `server` (ravel-server plus
# ravel-cli) and `operator` (the Kubernetes operator). Stages are named so
# `--target server` or `--target operator` builds only that one image.

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
#
# CARGO_BUILD_JOBS is capped, not left at cargo's default (one job per host
# core): the root Cargo.toml's release profile (lto = "thin", codegen-units =
# 1, debug = 1) gives each rustc a high peak RSS, and datafusion's crates are
# the worst of them. Measured on an 8 GiB / 8-core Docker host: the default
# (8 parallel jobs) reliably OOM-kills the build partway through
# datafusion-functions-aggregate (docker stats showed 7+ GiB used just before
# the kill); capping to 2 jobs, everything else identical, builds clean. 2 is
# deliberately conservative rather than tuned to one specific host's RAM,
# since this Dockerfile has to build on whatever the CI runner and every
# developer's machine actually provide, not just the box this was measured on.
ENV CARGO_BUILD_JOBS=2
RUN cargo build --release --locked -p ravel-server --features sql \
    && cargo build --release --locked -p ravel-cli \
    && cargo build --release --locked -p ravel-operator

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
# create a second source of truth. Every ravel-server flag defaults to
# something runnable (--mode all, --store memory, --listen-http
# 127.0.0.1:4318), so a bare `docker run <image>` does NOT print usage and
# exit -- it starts a real server against an in-memory store and blocks.
# `docker run <image> --help` is the runtime smoke test; a bare `docker run`
# is a real (if minimally configured) server process, not a no-op.
ENTRYPOINT ["/usr/local/bin/ravel-server"]

# ---- Runtime: operator image ------------------------------------------------
# The Kubernetes operator (ADR-0034 decision 5): same distroless/cc:nonroot
# base and the same CARGO_BUILD_JOBS=2 builder stage as the server image, so it
# inherits the OOM fix without a second build configuration.
# `--target operator` builds only this image.
FROM gcr.io/distroless/cc-debian12:nonroot AS operator

COPY --from=builder /app/target/release/ravel-operator /usr/local/bin/ravel-operator

# No ports: the operator makes outbound calls to the Kubernetes API server and
# serves nothing itself.

# The operator takes its configuration from the ambient Kubernetes environment
# (in-cluster service account or kubeconfig). `--print-crd` emits the
# CustomResourceDefinition and exits, which is how deploy/k8s/operator/crd.yaml
# is regenerated.
ENTRYPOINT ["/usr/local/bin/ravel-operator"]
