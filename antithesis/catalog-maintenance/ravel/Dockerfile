# Instrumented ravel-server for the Antithesis catalog-maintenance workload.
# Build context: the repository root (scripts/build.sh passes it).
#
# The two build stages are the ingest-durability image's (see
# antithesis/ingest-durability/ravel/Dockerfile for why the flags are what
# they are). This image adds the seed-marker entrypoint, the maintain key,
# and libfaketime for the clock spike (local/docker-compose.faketime.yaml);
# libfaketime does nothing unless LD_PRELOAD names it.

FROM rust:1.97.1-trixie AS server
WORKDIR /src
ENV CARGO_BUILD_JOBS=8
COPY . .
RUN cargo build --release --locked -p ravel-server --features sql,antithesis \
      --config 'build.target = "x86_64-unknown-linux-gnu"' \
      --config 'profile.release.lto = false' \
      --config 'target.x86_64-unknown-linux-gnu.rustflags = ["-Ccodegen-units=1", "-Cpasses=sancov-module", "-Cllvm-args=-sanitizer-coverage-level=3", "-Cllvm-args=-sanitizer-coverage-trace-pc-guard", "-Cllvm-args=-sanitizer-coverage-pc-table", "-Clink-args=-Wl,--build-id"]' \
 && mkdir -p /out /symbols \
 && cp target/x86_64-unknown-linux-gnu/release/ravel-server /out/ \
 && cp /out/ravel-server /symbols/

# ravel-cli qualifies the bucket here, and the driver image copies it from
# this image to seed, fold, compact, sweep and decode. It is not under test.
FROM rust:1.97.1-trixie AS cli
WORKDIR /src
ENV CARGO_BUILD_JOBS=8
COPY . .
RUN cargo build --release --locked -p ravel-cli \
 && mkdir -p /out \
 && cp target/release/ravel-cli /out/

FROM debian:trixie-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl jq procps libfaketime \
 && rm -rf /var/lib/apt/lists/*
COPY --from=server /out/ravel-server /usr/local/bin/ravel-server
COPY --from=server /symbols /symbols
COPY --from=cli /out/ravel-cli /usr/local/bin/ravel-cli
COPY antithesis/catalog-maintenance/ravel/init.sh /usr/local/bin/ravel-antithesis-init
COPY antithesis/catalog-maintenance/ravel/entrypoint.sh /usr/local/bin/ravel-antithesis-entrypoint
# The maintain workers' registry key (ADR-3130), generated per image so no key
# is checked in. Both maintain containers run this one image, so they share it.
RUN chmod 755 /usr/local/bin/ravel-antithesis-init /usr/local/bin/ravel-antithesis-entrypoint \
 && mkdir -p /etc/ravel \
 && head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > /etc/ravel/maintain.key \
 && chmod 600 /etc/ravel/maintain.key \
 && test "$(wc -c < /etc/ravel/maintain.key)" -eq 64 \
 && test -e /usr/lib/x86_64-linux-gnu/faketime/libfaketimeMT.so.1
EXPOSE 4318 4317
ENTRYPOINT ["/usr/local/bin/ravel-antithesis-entrypoint"]
