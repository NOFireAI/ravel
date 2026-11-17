# Instrumented ravel-server for the Antithesis catalog-maintenance workload.
# Build context: the repository root (scripts/build.sh passes it).
#
# The two build stages are the ingest-durability image's (see
# antithesis/ingest-durability/ravel/Dockerfile for why the flags are what
# they are). This image adds the seed-marker entrypoint, the maintain key,
# and libfaketime with its setup and run scripts for the clock jump
# (config/docker-compose.yaml, service `faketime`); libfaketime does nothing
# unless LD_PRELOAD names it.

# libfaketime from a pinned release tarball, checked by digest. Every
# container loads this one build from the shared volume, the glibc rustfs
# image included (Ubuntu, whose glibc is not older than trixie's).
FROM debian:trixie-slim AS faketime
ARG FAKETIME_VERSION=0.9.13
ARG FAKETIME_SHA256=8e56deeb805682b025107e095f1d94a6ea677472f05824cd475f5c5e6e1a5ddf
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl gcc libc6-dev make \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /build
RUN curl -fsSL --max-time 300 -o libfaketime.tar.gz \
      "https://github.com/wolfcw/libfaketime/archive/refs/tags/v${FAKETIME_VERSION}.tar.gz" \
 && echo "${FAKETIME_SHA256}  libfaketime.tar.gz" | sha256sum -c - \
 && tar xzf libfaketime.tar.gz \
 && cd "libfaketime-${FAKETIME_VERSION}" \
 && sed -i 's/-Werror//' src/Makefile \
 && make PREFIX=/usr \
 && mkdir -p /out \
 && cp src/libfaketimeMT.so.1 /out/

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
 && apt-get install -y --no-install-recommends ca-certificates curl jq procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=server /out/ravel-server /usr/local/bin/ravel-server
COPY --from=server /symbols /symbols
COPY --from=cli /out/ravel-cli /usr/local/bin/ravel-cli
COPY --from=faketime /out/libfaketimeMT.so.1 /usr/local/lib/faketime/libfaketimeMT.so.1
COPY antithesis/catalog-maintenance/ravel/init.sh /usr/local/bin/ravel-antithesis-init
COPY antithesis/catalog-maintenance/ravel/entrypoint.sh /usr/local/bin/ravel-antithesis-entrypoint
COPY antithesis/catalog-maintenance/ravel/faketime-setup.sh /usr/local/bin/ravel-antithesis-faketime-setup
COPY antithesis/catalog-maintenance/ravel/faketime-run.sh /usr/local/bin/ravel-antithesis-faketime-run
# The maintain workers' registry key (ADR-3130), generated per image so no key
# is checked in. Both maintain containers run this one image, so they share it.
RUN chmod 755 /usr/local/bin/ravel-antithesis-init /usr/local/bin/ravel-antithesis-entrypoint \
      /usr/local/bin/ravel-antithesis-faketime-setup /usr/local/bin/ravel-antithesis-faketime-run \
 && mkdir -p /etc/ravel \
 && head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > /etc/ravel/maintain.key \
 && chmod 600 /etc/ravel/maintain.key \
 && test "$(wc -c < /etc/ravel/maintain.key)" -eq 64 \
 && test "$(FAKETIME=+1d LD_PRELOAD=/usr/local/lib/faketime/libfaketimeMT.so.1 date +%s)" -gt "$(($(date +%s) + 86000))"
EXPOSE 4318 4317
ENTRYPOINT ["/usr/local/bin/ravel-antithesis-entrypoint"]
