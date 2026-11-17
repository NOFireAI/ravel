# The workload driver. Build context: antithesis/ (the driver depends on the
# ingest-durability driver's library by path). scripts/build.sh passes
# SERVER_IMAGE, the server image it built first, which holds the ravel-cli
# binary the driver runs.
# Built with Antithesis's compile-time coverage flags (the driver has no LTO).

ARG SERVER_IMAGE=ravel-antithesis-server:catalog-maintenance
FROM ${SERVER_IMAGE} AS server

FROM rust:1.97.1-trixie AS build
WORKDIR /src
COPY ingest-durability/driver/Cargo.toml ingest-durability/driver/Cargo.lock ./ingest-durability/driver/
COPY ingest-durability/driver/src ./ingest-durability/driver/src
COPY catalog-maintenance/driver/Cargo.toml catalog-maintenance/driver/Cargo.lock ./catalog-maintenance/driver/
COPY catalog-maintenance/driver/src ./catalog-maintenance/driver/src
WORKDIR /src/catalog-maintenance/driver
RUN cargo build --release --locked \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Cllvm-args=-sanitizer-coverage-pc-table", "-Clink-args=-Wl,--build-id"]' \
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/driver /out/ \
 && cp /out/driver /symbols/

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl jq procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/driver /usr/local/bin/driver
COPY --from=build /symbols /symbols
COPY --from=server /usr/local/bin/ravel-cli /usr/local/bin/ravel-cli
COPY catalog-maintenance/driver/first_start_workload /opt/antithesis/test/v1/main/first_start_workload
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]
