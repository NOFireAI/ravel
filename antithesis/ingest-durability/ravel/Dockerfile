# Instrumented ravel-server and ravel-cli for the Antithesis ingest-durability
# workload.
# Build context: the repository root (scripts/build.sh passes it).
#
# ravel-server is built with the `sql` feature (the driver reads rows through
# POST /api/v1/sql) and the `antithesis` feature (SDK assertions and the
# instrumentation runtime shim), with Antithesis's compile-time coverage flags.
# ravel-cli is built the same way with its own `antithesis` feature, so the
# ADR-4326 assertion sites in the crates it links report from it too.
# Those flags do not survive link-time optimization, so this build turns the
# release profile's thin LTO off; codegen-units stays at 1. The published
# image is unaffected: only this Dockerfile passes these flags.
#
# SERVER_FEATURES is ravel-server's feature list. scripts/build.sh overrides
# it, with sql,antithesis,antithesis-mutants, only for the
# ingest-durability-mutants tag (ADR-4326 decision 3); every other build keeps
# the default. ravel-cli never builds the mutants.

FROM rust:1.97.1-trixie AS server
ARG SERVER_FEATURES=sql,antithesis
WORKDIR /src
# The two build stages run in parallel; each takes half of a 16-core box.
ENV CARGO_BUILD_JOBS=8
COPY . .
RUN cargo build --release --locked -p ravel-server --features "$SERVER_FEATURES" \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.lto = false' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Cllvm-args=-sanitizer-coverage-pc-table", "-Clink-args=-Wl,--build-id"]' \
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/ravel-server /out/ \
 && cp /out/ravel-server /symbols/

# ravel-cli qualifies the bucket in the init container. Its `antithesis`
# feature links the runtime shim, without which an instrumented binary would
# not link at all.
FROM rust:1.97.1-trixie AS cli
WORKDIR /src
ENV CARGO_BUILD_JOBS=8
COPY . .
RUN cargo build --release --locked -p ravel-cli --features antithesis \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.lto = false' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Cllvm-args=-sanitizer-coverage-pc-table", "-Clink-args=-Wl,--build-id"]' \
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/ravel-cli /out/ \
 && cp /out/ravel-cli /symbols/

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl jq procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=server /out/ravel-server /usr/local/bin/ravel-server
COPY --from=server /symbols /symbols
COPY --from=cli /symbols/ravel-cli /symbols/ravel-cli
COPY --from=cli /out/ravel-cli /usr/local/bin/ravel-cli
COPY antithesis/ingest-durability/ravel/init.sh /usr/local/bin/ravel-antithesis-init
RUN chmod 755 /usr/local/bin/ravel-antithesis-init
EXPOSE 4318 4317
ENTRYPOINT ["/usr/local/bin/ravel-server"]
