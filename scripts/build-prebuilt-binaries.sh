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
# container builds into. The container runs as root; it hands everything it
# wrote back to the calling user before it exits.
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

cargo_home="${CARGO_HOME:-${HOME}/.cargo}"
mkdir -p "${cargo_home}/registry" "${cargo_home}/git" target

echo "build-prebuilt-binaries: building in ${image}"
docker run --rm \
  -v "${root}:/work" -w /work \
  -v "${cargo_home}/registry:/usr/local/cargo/registry" \
  -v "${cargo_home}/git:/usr/local/cargo/git" \
  -e CARGO_INCREMENTAL=0 \
  ${CARGO_BUILD_JOBS:+-e CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS}"} \
  -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
  "${image}" \
  bash -c '
    set -euo pipefail
    rc=0
    cargo build --release --locked -p ravel-server --features sql,flight-sql,otap || rc=$?
    if [[ "${rc}" == 0 ]]; then cargo build --release --locked -p ravel-cli || rc=$?; fi
    if [[ "${rc}" == 0 ]]; then cargo build --release --locked -p ravel-operator || rc=$?; fi
    chown -R "${HOST_UID}:${HOST_GID}" /work/target /usr/local/cargo/registry /usr/local/cargo/git
    exit "${rc}"
  '
