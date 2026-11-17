# The store-contract workload driver. Build context: the repository root
# (scripts/build.sh passes it), because the driver depends on
# crates/ravel-object-store by path. Dockerfile.dockerignore beside this file
# narrows that context to the files the build reads.
# Built with Antithesis's compile-time coverage flags (the driver has no LTO).

FROM rust:1.97.1-trixie AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked \
      --manifest-path antithesis/store-contract/driver/Cargo.toml \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Cllvm-args=-sanitizer-coverage-pc-table", "-Clink-args=-Wl,--build-id"]' \
 && mkdir -p /out /symbols \
 && cp antithesis/store-contract/driver/target/x86_64-unknown-linux-gnu/release/driver /out/ \
 && cp /out/driver /symbols/

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl jq procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/driver /usr/local/bin/driver
COPY --from=build /symbols /symbols
COPY antithesis/store-contract/driver/init.sh /usr/local/bin/store-contract-init
COPY antithesis/store-contract/driver/first_start_workload /opt/antithesis/test/v1/main/first_start_workload
RUN chmod 755 /usr/local/bin/store-contract-init /opt/antithesis/test/v1/main/first_start_workload
CMD ["/usr/local/bin/driver"]
