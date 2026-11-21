# The lifecycle-gc workload driver. Build context: antithesis/lifecycle-gc/.
# scripts/build.sh passes SERVER_IMAGE, the server image the compose file
# runs, which holds the ravel-cli binary the driver submits erasures and
# holds with. Built with Antithesis's compile-time coverage flags.

ARG SERVER_IMAGE=ravel-antithesis-server:lifecycle-gc
FROM ${SERVER_IMAGE} AS server

FROM rust:1.100.0-trixie AS build
WORKDIR /src
COPY driver/Cargo.toml driver/Cargo.lock ./
COPY driver/src ./src
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
COPY driver/first_start_workload /opt/antithesis/test/v1/main/first_start_workload
RUN chmod 755 /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]
