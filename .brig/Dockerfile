# syntax=docker/dockerfile:1
#
# The guest image of the Brig dev target that #4405 adds: a development
# environment for a Claude Code session inside a Brig sandbox, not a
# deployable Ravel artifact.
# CI publishes it as ghcr.io/nofireai/ravel-dev (.github/workflows/brig-image.yml).
#
# On top of Brig's stock Claude Code image (Ubuntu 24.04, root, git, gh, jq,
# python3, Node 22, a C toolchain, rustup and claude) it adds:
#
#   - the Rust toolchain rust-toolchain.toml names, with its components,
#     as the default toolchain;
#   - cargo-nextest and cargo-deny;
#   - the crates.io registry filled for Cargo.lock, so a build needs no fetch;
#   - an npm cache filled for sdk/typescript/package-lock.json.
#
# Build it from the repository root:
#   docker build -f .brig/Dockerfile -t ravel-dev .
#
# Brig mounts the guest home over /root, so nothing here lives under it. The
# stock image keeps RUSTUP_HOME and CARGO_HOME under /usr/local.

# The root variant of the stock image, by digest. Bump it deliberately:
#   docker buildx imagetools inspect ghcr.io/brig-sh/claude-code-stock:root
ARG GUEST_IMAGE=ghcr.io/brig-sh/claude-code-stock:root@sha256:f6bfd5213245c86ebb4165a4267b45d6b55727e11ae44cb3db311a3397d39b60

# The same builder image the root Dockerfile pins.
ARG FETCH_IMAGE=rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea

ARG NEXTEST_VERSION=0.9.152
ARG CARGO_DENY_VERSION=0.20.2

# ========== Release binaries ==========
# Each archive is checked against the sha256 below. A version with no sha256
# here fails the build: add the two lines for it.
FROM --platform=$BUILDPLATFORM ${FETCH_IMAGE} AS tools
ARG TARGETARCH
ARG NEXTEST_VERSION
ARG CARGO_DENY_VERSION
WORKDIR /out
RUN set -eu; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64 ;; \
      arm64) triple=aarch64 ;; \
      *) echo "tools: unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    case "$NEXTEST_VERSION-$TARGETARCH" in \
      0.9.152-amd64) nextest_sha=d039b43c3e0958441b6ad0a39c8884d018b3d2e8fb9928f209074b1100b5e17a ;; \
      0.9.152-arm64) nextest_sha=ca1f81a5b86533eb45ab97e0064aa88e925e53aab9a698ca6331a7cad1f85f07 ;; \
      *) echo "tools: no sha256 for cargo-nextest $NEXTEST_VERSION on $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    case "$CARGO_DENY_VERSION-$TARGETARCH" in \
      0.20.2-amd64) deny_sha=9f12ed4c49936e09b48bf862b595cde2fe64fcbd9d74dfacac6131ca824c8d5f ;; \
      0.20.2-arm64) deny_sha=995c82be0defc7a025cae49a2aa2644ce8245c9a3318fc4103907c6a285e8c7d ;; \
      *) echo "tools: no sha256 for cargo-deny $CARGO_DENY_VERSION on $TARGETARCH" >&2; exit 1 ;; \
    esac; \
    f="cargo-nextest-$NEXTEST_VERSION-$triple-unknown-linux-gnu.tar.gz"; \
    curl -fsSLo "$f" "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-$NEXTEST_VERSION/$f"; \
    echo "$nextest_sha  $f" | sha256sum -c -; \
    tar -xzf "$f" -C /out cargo-nextest; \
    rm "$f"; \
    d="cargo-deny-$CARGO_DENY_VERSION-$triple-unknown-linux-musl"; \
    curl -fsSLo "$d.tar.gz" "https://github.com/EmbarkStudios/cargo-deny/releases/download/$CARGO_DENY_VERSION/$d.tar.gz"; \
    echo "$deny_sha  $d.tar.gz" | sha256sum -c -; \
    tar -xzf "$d.tar.gz" --strip-components=1 -C /out "$d/cargo-deny"; \
    rm "$d.tar.gz"

# ========== crates.io registry ==========
# The registry holds source, not binaries, so one fetch on the build platform
# feeds both target images. cargo fetch reads every workspace manifest, so the
# stage takes the whole build context; only the registry leaves it.
FROM --platform=$BUILDPLATFORM ${FETCH_IMAGE} AS registry
WORKDIR /src
COPY . .
RUN cargo fetch --locked

# ========== Final: the guest image ==========
FROM ${GUEST_IMAGE}

LABEL org.opencontainers.image.source=https://github.com/NOFireAI/ravel
LABEL org.opencontainers.image.description="Ravel development guest for Brig"

# The toolchain rust-toolchain.toml names, with its components, as the
# default. Any other toolchain the stock image carries is removed.
COPY rust-toolchain.toml /opt/ravel/rust-toolchain.toml
RUN set -eu; \
    channel=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' /opt/ravel/rust-toolchain.toml); \
    components=$(sed -n 's/^components *= *\[\(.*\)\]/\1/p' /opt/ravel/rust-toolchain.toml | tr -d '" ' | tr ',' ' '); \
    [ -n "$channel" ] || { echo "no channel in rust-toolchain.toml" >&2; exit 1; }; \
    set --; for c in $components; do set -- "$@" -c "$c"; done; \
    rustup toolchain install "$channel" --profile minimal "$@"; \
    rustup default "$channel"; \
    for t in $(rustup toolchain list | awk '{print $1}'); do \
      case "$t" in "$channel"-*) ;; *) rustup toolchain uninstall "$t" ;; esac; \
    done; \
    rustc --version; cargo clippy --version; cargo fmt --version

COPY --from=tools /out/cargo-nextest /out/cargo-deny /usr/local/cargo/bin/
COPY --from=registry /usr/local/cargo/registry /usr/local/cargo/registry

# The npm cache for the TypeScript SDK's lockfile, filled on the target
# platform so it holds that platform's optional packages. The SDK is an npm
# workspace, so npm ci needs every member's package.json; the stage copies the
# SDK tree and keeps only the cache.
ENV npm_config_cache=/opt/npm-cache
COPY sdk/typescript /tmp/ravel-sdk
RUN cd /tmp/ravel-sdk \
 && npm ci --ignore-scripts --no-audit --no-fund \
 && cd / && rm -rf /tmp/ravel-sdk

WORKDIR /work
