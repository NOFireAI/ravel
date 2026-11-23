#!/usr/bin/env bash
# Cases for check-rust-image-versions.sh. Add a case here before changing the
# rule.
#
# Each case builds a throwaway git repository under $TMPDIR and passes it to
# the guard as its repo-root, so nothing here touches the real checkout.
# RUST_IMAGE_GUARD picks the script under test (default: the sibling guard),
# so a deliberately broken copy can be shown failing these cases.
#
# Run: bash scripts/guards/check-rust-image-versions.test.sh
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${RUST_IMAGE_GUARD:-${HERE}/check-rust-image-versions.sh}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-rust-image-versions-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name> [channel]: an empty scratch repo with rust-toolchain.toml
# pinning <channel> (default 1.100.0). "none" writes no toolchain file.
new_repo() {
  local dir="${TMP}/$1" channel="${2:-1.100.0}"
  mkdir -p "${dir}"
  git -C "${dir}" init -q
  if [[ "${channel}" != "none" ]]; then
    printf '[toolchain]\nchannel = "%s"\ncomponents = ["rustfmt", "clippy"]\n' \
      "${channel}" >"${dir}/rust-toolchain.toml"
  fi
  printf '%s' "${dir}"
}

# put <repo> <path>: writes stdin to <path> in <repo> and tracks it.
put() {
  local dir="$1" rel="$2"
  mkdir -p "$(dirname "${dir}/${rel}")"
  cat >"${dir}/${rel}"
  git -C "${dir}" add -- "${rel}"
}

# expect <name> <want-exit> <repo> [needle ...]: runs the guard on <repo>,
# asserts the exit code and that every needle appears in its output.
expect() {
  local name="$1" want="$2" dir="$3" out code=0 needle ok=1
  shift 3
  out="$(bash "${GUARD}" "${dir}" 2>&1)" || code=$?
  if [[ "${code}" -ne "${want}" ]]; then
    ok=0
  fi
  for needle in "$@"; do
    if [[ "${out}" != *"${needle}"* ]]; then
      ok=0
      printf '  missing in output: %s\n' "${needle}"
    fi
  done
  if [[ "${ok}" -eq 1 ]]; then
    passes=$((passes + 1))
    printf 'ok   %s\n' "${name}"
  else
    fails=$((fails + 1))
    printf 'FAIL %s: want exit %s, got %s\n%s\n' "${name}" "${want}" "${code}" "${out}"
  fi
}

# --- clean ---------------------------------------------------------------

d="$(new_repo all-match)"
put "${d}" Dockerfile <<'EOF'
# syntax=docker/dockerfile:1
FROM rust:1.100.0-bookworm AS builder
FROM rust:1.100.0 AS plain
FROM rust:1.100.0-slim-bookworm AS slim
FROM docker.io/library/rust:1.100.0-trixie AS hub
ARG FETCH_IMAGE=rust:1.100.0-bookworm
FROM ${FETCH_IMAGE} AS fetch
FROM debian:bookworm-slim AS runtime
EOF
expect "all matching lines pass, suffixes compared by version prefix" 0 "${d}" \
  "5 rust image line(s) checked in 1 Dockerfile(s), 0 exempt, 0 finding(s); want 1.100.0"

d="$(new_repo digest-match)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea AS builder
ARG FETCH_IMAGE=rust:1.100.0-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea
EOF
expect "digest-pinned matching lines pass" 0 "${d}" "2 rust image line(s) checked"

# --- findings ------------------------------------------------------------

d="$(new_repo one-mismatch)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm AS builder
RUN true
FROM rust:1.99.0-trixie AS server
EOF
expect "one mismatched tag names file, line and both versions" 1 "${d}" \
  "Dockerfile:3: rust:1.99.0-trixie has version 1.99.0, want 1.100.0" \
  "1 finding(s)"

d="$(new_repo patch-mismatch)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.1-bookworm AS builder
EOF
expect "a version that only shares a prefix with the channel is a finding" 1 "${d}" \
  "Dockerfile:1: rust:1.100.1-bookworm has version 1.100.1, want 1.100.0"

d="$(new_repo digest-mismatch)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.97.1-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea AS builder
EOF
expect "digest-pinned mismatch is a finding" 1 "${d}" "has version 1.97.1, want 1.100.0"

d="$(new_repo arg-mismatch)"
put "${d}" .brig/Dockerfile <<'EOF'
FROM debian:bookworm-slim
ARG FETCH_IMAGE=rust:1.97.1-bookworm@sha256:ade39a710ef85f3506cffcfec3e8a24ba090badef1dff2fb798f71d878833fea
EOF
expect "ARG default mismatch is a finding" 1 "${d}" \
  ".brig/Dockerfile:2: rust:1.97.1-bookworm@sha256:" "has version 1.97.1, want 1.100.0"

d="$(new_repo arg-quoted-mismatch)"
put "${d}" Dockerfile <<'EOF'
arg IMAGE="docker.io/library/rust:1.97.1"
EOF
expect "quoted lower-case ARG default with the docker.io prefix is checked" 1 "${d}" \
  "Dockerfile:1: docker.io/library/rust:1.97.1 has version 1.97.1"

d="$(new_repo platform-mismatch)"
put "${d}" Dockerfile <<'EOF'
FROM --platform=$BUILDPLATFORM rust:1.97.1-trixie AS build
EOF
expect "--platform FROM mismatch is a finding" 1 "${d}" \
  "Dockerfile:1: rust:1.97.1-trixie has version 1.97.1, want 1.100.0"

d="$(new_repo lower-from)"
put "${d}" Dockerfile <<'EOF'
from rust:1.97.1-trixie as build
EOF
expect "lower-case from mismatch is a finding" 1 "${d}" \
  "Dockerfile:1: rust:1.97.1-trixie has version 1.97.1"

d="$(new_repo mixed-from)"
put "${d}" Dockerfile <<'EOF'
  From rust:1.97.1-trixie
EOF
expect "mixed-case indented From mismatch is a finding" 1 "${d}" "Dockerfile:1:"

d="$(new_repo hub-mismatch)"
put "${d}" Dockerfile <<'EOF'
FROM docker.io/library/rust:1.97.1-trixie AS build
EOF
expect "docker.io/library/rust mismatch is a finding" 1 "${d}" \
  "docker.io/library/rust:1.97.1-trixie has version 1.97.1"

for tag in "rust:latest" "rust:1" "rust:slim" "rust" "rust@sha256:00ff" 'rust:${RUST_VERSION}'; do
  d="$(new_repo "unpinned-${#tag}-${tag//[^a-z0-9]/}")"
  printf 'FROM %s AS build\n' "${tag}" | put "${d}" Dockerfile
  expect "${tag} pins no x.y.z version and is a finding" 1 "${d}" \
    "Dockerfile:1: ${tag} has no x.y.z version, want 1.100.0"
done

# --- marker --------------------------------------------------------------

d="$(new_repo marker-reason)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm AS builder
# rust-image-allow: the nightly fuzzer needs its own toolchain
FROM rust:1.97.1-trixie AS fuzz
EOF
expect "a mismatch under a marker with a reason passes" 0 "${d}" \
  "2 rust image line(s) checked in 1 Dockerfile(s), 1 exempt, 0 finding(s)"

d="$(new_repo marker-empty)"
put "${d}" Dockerfile <<'EOF'
# rust-image-allow:
FROM rust:1.97.1-trixie AS fuzz
EOF
expect "a marker with an empty reason does not suppress" 1 "${d}" \
  "Dockerfile:2: rust-image-allow marker above has no reason" \
  "Dockerfile:2: rust:1.97.1-trixie has version 1.97.1"

d="$(new_repo marker-blank-reason)"
printf '# rust-image-allow:   \t \nFROM rust:1.97.1-trixie AS fuzz\n' | put "${d}" Dockerfile
expect "a marker with a whitespace-only reason does not suppress" 1 "${d}" \
  "has no reason"

d="$(new_repo marker-two-above)"
put "${d}" Dockerfile <<'EOF'
# rust-image-allow: the nightly fuzzer needs its own toolchain

FROM rust:1.97.1-trixie AS fuzz
EOF
expect "a marker two lines above does not suppress" 1 "${d}" "Dockerfile:3:"

d="$(new_repo marker-not-comment)"
put "${d}" Dockerfile <<'EOF'
LABEL note="rust-image-allow: not a comment"
FROM rust:1.97.1-trixie AS fuzz
EOF
expect "a marker outside a comment does not suppress" 1 "${d}" "Dockerfile:2:"

# --- what is not matched -------------------------------------------------

d="$(new_repo lookalikes)"
put "${d}" Dockerfile <<'EOF'
FROM rustlang/rust:nightly AS nightly
FROM rust-musl-builder:1.0 AS musl
FROM ghcr.io/example/rust:1.50.0 AS mirror
ARG OTHER=rustlang/rust:nightly
FROM rust:1.100.0-bookworm AS builder
RUN echo "FROM rust:1.0.0"
EOF
expect "images that only start with rust are not matched" 0 "${d}" \
  "1 rust image line(s) checked"

d="$(new_repo docs-out-of-scope)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm AS builder
EOF
put "${d}" docs/guide.md <<'EOF'
```dockerfile
FROM rust:1.97.1-bookworm AS builder
```
EOF
put "${d}" scripts/build.sh <<'EOF'
FROM rust:1.97.1-bookworm
EOF
expect "Markdown and other non-Dockerfile files are not scanned" 0 "${d}" \
  "1 rust image line(s) checked in 1 Dockerfile(s)"

d="$(new_repo file-rule)"
put "${d}" Dockerfile.prebuilt <<'EOF'
FROM rust:1.97.1-bookworm AS a
EOF
put "${d}" deploy/tools.Dockerfile <<'EOF'
FROM rust:1.97.2-bookworm AS b
EOF
put "${d}" antithesis/x/driver/Dockerfile <<'EOF'
FROM rust:1.97.3-trixie AS c
EOF
put "${d}" antithesis/x/driver/Dockerfile.dockerignore <<'EOF'
FROM rust:1.97.4-trixie AS d
EOF
expect "Dockerfile.<suffix>, <prefix>.Dockerfile and nested Dockerfile are scanned, .dockerignore is not" 1 "${d}" \
  "Dockerfile.prebuilt:1:" "deploy/tools.Dockerfile:1:" "antithesis/x/driver/Dockerfile:1:" \
  "3 rust image line(s) checked in 3 Dockerfile(s), 0 exempt, 3 finding(s)"

d="$(new_repo untracked)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm AS builder
EOF
printf 'FROM rust:1.97.1-bookworm\n' >"${d}/other.Dockerfile"
expect "an untracked Dockerfile is not scanned" 0 "${d}" "1 rust image line(s) checked"

# --- could not check -----------------------------------------------------

d="$(new_repo no-toolchain none)"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm AS builder
EOF
expect "no rust-toolchain.toml cannot be checked" 2 "${d}" \
  "cannot check: rust-toolchain.toml does not exist"

d="$(new_repo no-channel none)"
printf '[toolchain]\ncomponents = ["rustfmt"]\n' >"${d}/rust-toolchain.toml"
put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm AS builder
EOF
expect "a toolchain file with no channel line cannot be checked" 2 "${d}" \
  "rust-toolchain.toml has no channel line"

for ch in stable 1.100 nightly-2026-01-01 1.100.0-beta; do
  d="$(new_repo "channel-${ch}" "${ch}")"
  put "${d}" Dockerfile <<'EOF'
FROM rust:1.100.0-bookworm AS builder
EOF
  expect "channel ${ch} is not x.y.z and cannot be checked" 2 "${d}" \
    "channel '${ch}' is not a plain x.y.z version"
done

d="$(new_repo no-lines)"
put "${d}" Dockerfile <<'EOF'
FROM debian:bookworm-slim
EOF
expect "no FROM rust lines cannot be checked" 2 "${d}" \
  "no FROM or ARG line names a rust image in 1 Dockerfile(s)"

d="$(new_repo no-dockerfiles)"
put "${d}" README.md <<'EOF'
FROM rust:1.100.0-bookworm
EOF
expect "no Dockerfile at all cannot be checked" 2 "${d}" \
  "no FROM or ARG line names a rust image in 0 Dockerfile(s)"

d="${TMP}/not-a-repo"
mkdir -p "${d}"
printf '[toolchain]\nchannel = "1.100.0"\n' >"${d}/rust-toolchain.toml"
printf 'FROM rust:1.100.0-bookworm\n' >"${d}/Dockerfile"
expect "a directory git cannot list cannot be checked" 2 "${d}" "git ls-files failed"

# --- usage ---------------------------------------------------------------

code=0
bash "${GUARD}" "${TMP}" extra >/dev/null 2>&1 || code=$?
if [[ "${code}" -eq 64 ]]; then
  passes=$((passes + 1)); printf 'ok   two arguments is a usage error\n'
else
  fails=$((fails + 1)); printf 'FAIL two arguments: want exit 64, got %s\n' "${code}"
fi
code=0
bash "${GUARD}" --bogus >/dev/null 2>&1 || code=$?
if [[ "${code}" -eq 64 ]]; then
  passes=$((passes + 1)); printf 'ok   an unknown flag is a usage error\n'
else
  fails=$((fails + 1)); printf 'FAIL unknown flag: want exit 64, got %s\n' "${code}"
fi
code=0
bash "${GUARD}" "${TMP}/does-not-exist" >/dev/null 2>&1 || code=$?
if [[ "${code}" -eq 64 ]]; then
  passes=$((passes + 1)); printf 'ok   a repo-root that is not a directory is a usage error\n'
else
  fails=$((fails + 1)); printf 'FAIL missing repo-root: want exit 64, got %s\n' "${code}"
fi

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]] || exit 1
