#!/usr/bin/env bash
# Build the release binaries that Dockerfile.prebuilt packages, inside the root
# Dockerfile's builder image rather than on the host.
#
# Dockerfile.prebuilt runs these binaries on the shipping distroless base
# (Debian 12, glibc 2.36). A binary links against the glibc of the machine
# that built it, so one built on a CI runner (Ubuntu 24.04: 2.39, Ubuntu 26.04:
# 2.43) fails to exec there with "GLIBC_x.y not found". Building inside the
# image the root Dockerfile's builder stage uses gives the same glibc as the
# shipping build, whatever runner the job lands on (#3146).
#
# The image is read from the root Dockerfile's `AS builder` stage, so the two
# cannot drift. The workspace and the host's cargo registry and git caches are
# mounted, so target/ and the caches the CI job restores are the ones the
# container builds into. The container runs as the calling user, with
# CARGO_HOME and HOME under /tmp, so nothing it writes is root-owned even if
# it is killed mid-build; --init forwards Ctrl-C to the build.
#
# The builder stage's CARGO_BUILD_JOBS=2 cap is deliberately not inherited:
# CI builds at cargo's default against a 55-minute budget. On a small host
# (the 8 GiB arm64 box the Dockerfile measured), set CARGO_BUILD_JOBS=2 in the
# environment; the script passes it into the container.
#
# Usage: scripts/build-prebuilt-binaries.sh
# Output: target/release/{ravel-server,ravel-cli,ravel-operator}
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "${root}"

image="$(awk '$1 == "FROM" && $NF == "builder" { print $2; exit }' Dockerfile)"
if [[ -z "${image}" ]]; then
  echo "build-prebuilt-binaries: no 'FROM <image> AS builder' stage in Dockerfile" >&2
  exit 2
fi

# rust-toolchain.toml also lists components (clippy, rustfmt, llvm-tools) the
# image does not ship; naming the channel directly keeps rustup from trying to
# install them into the image's root-owned RUSTUP_HOME as a non-root user.
channel="$(awk -F'"' '/^channel/ { print $2; exit }' rust-toolchain.toml)"
if [[ -z "${channel}" ]]; then
  echo "build-prebuilt-binaries: no channel in rust-toolchain.toml" >&2
  exit 2
fi

cargo_home="${CARGO_HOME:-${HOME}/.cargo}"
mkdir -p "${cargo_home}/registry" "${cargo_home}/git" target

echo "build-prebuilt-binaries: building in ${image}"
docker run --rm --init \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e CARGO_HOME=/tmp/cargo-home \
  -e RUSTUP_TOOLCHAIN="${channel}" \
  -v "${root}:/work" -w /work \
  -v "${cargo_home}/registry:/tmp/cargo-home/registry" \
  -v "${cargo_home}/git:/tmp/cargo-home/git" \
  -e CARGO_INCREMENTAL=0 \
  ${CARGO_BUILD_JOBS:+-e CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS}"} \
  "${image}" \
  bash -c '
    set -euo pipefail
    cargo build --release --locked -p ravel-server --features sql,flight-sql,otap
    cargo build --release --locked -p ravel-cli
    cargo build --release --locked -p ravel-operator
  '
