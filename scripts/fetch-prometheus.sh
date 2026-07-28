#!/usr/bin/env bash
# Downloads and verifies the pinned Prometheus release binary used by the
# PromQL differential test harness (ADR-0021 decision 4,
# docs/promql-evaluator-plan.md section 5.1, crates/ravel-promql-difftest).
#
# The version is pinned to the same tag this repo already vendors the
# Remote Write proto from (see proto/prometheus/prompb/*.proto: "Vendored
# from github.com/prometheus/prometheus, tag v3.13.1"), so both the wire
# format and the query-serving binary this harness runs against come from
# one consistent upstream tag.
#
# IMPORTANT: the sha256 checksums below are placeholders, not real
# published hashes. This environment has no network egress, so they could
# not be fetched from upstream (Prometheus' GitHub release page /
# sha256sums.txt) and verified here. The script refuses to run past the
# verification step while a checksum still carries the UNVERIFIED-PLACEHOLDER
# marker: silently trusting an unverified binary would defeat the point of
# pinning one. Populate PROM_SHA256_LINUX_AMD64 / PROM_SHA256_DARWIN_ARM64
# (or add a platform) with the real published digest before first use.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROM_VERSION="3.13.1"
CACHE_DIR="${RAVEL_DIFFTEST_PROM_CACHE:-$ROOT_DIR/.cache/prometheus}"

# UNVERIFIED-PLACEHOLDER: replace with the real sha256 from Prometheus'
# published `sha256sums.txt` for v${PROM_VERSION} before relying on this
# script outside a network-isolated sandbox.
PROM_SHA256_LINUX_AMD64="UNVERIFIED-PLACEHOLDER-linux-amd64"
PROM_SHA256_LINUX_ARM64="UNVERIFIED-PLACEHOLDER-linux-arm64"
PROM_SHA256_DARWIN_AMD64="UNVERIFIED-PLACEHOLDER-darwin-amd64"
PROM_SHA256_DARWIN_ARM64="UNVERIFIED-PLACEHOLDER-darwin-arm64"

log() {
  echo "[fetch-prometheus] $*" >&2
}

detect_platform() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os" in
    Linux) os="linux" ;;
    Darwin) os="darwin" ;;
    *)
      log "unsupported OS: $os"
      exit 1
      ;;
  esac
  case "$arch" in
    x86_64 | amd64) arch="amd64" ;;
    aarch64 | arm64) arch="arm64" ;;
    *)
      log "unsupported architecture: $arch"
      exit 1
      ;;
  esac
  echo "${os}-${arch}"
}

checksum_for() {
  case "$1" in
    linux-amd64) echo "$PROM_SHA256_LINUX_AMD64" ;;
    linux-arm64) echo "$PROM_SHA256_LINUX_ARM64" ;;
    darwin-amd64) echo "$PROM_SHA256_DARWIN_AMD64" ;;
    darwin-arm64) echo "$PROM_SHA256_DARWIN_ARM64" ;;
    *)
      log "no pinned checksum for platform: $1"
      exit 1
      ;;
  esac
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

platform="$(detect_platform)"
expected_sha256="$(checksum_for "$platform")"
archive_name="prometheus-${PROM_VERSION}.${platform}.tar.gz"
archive_url="https://github.com/prometheus/prometheus/releases/download/v${PROM_VERSION}/${archive_name}"
extract_dir="$CACHE_DIR/prometheus-${PROM_VERSION}.${platform}"
bin_path="$extract_dir/prometheus"

if [[ "$expected_sha256" == UNVERIFIED-PLACEHOLDER* ]]; then
  log "refusing to fetch: no verified sha256 pinned for platform '$platform'."
  log "populate PROM_SHA256_$(echo "$platform" | tr 'a-z-' 'A-Z_') in this script"
  log "with the real digest from https://github.com/prometheus/prometheus/releases/tag/v${PROM_VERSION}"
  log "(sha256sums.txt), then re-run. Refusing rather than trusting an unverified binary."
  exit 1
fi

if [[ -x "$bin_path" ]]; then
  log "already present and executable: $bin_path"
  echo "$bin_path"
  exit 0
fi

mkdir -p "$CACHE_DIR"
archive_path="$CACHE_DIR/$archive_name"

log "downloading $archive_url"
curl --fail --silent --show-error --location --output "$archive_path" "$archive_url"

log "verifying sha256"
actual_sha256="$(sha256_of "$archive_path")"
if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  log "checksum mismatch: expected $expected_sha256, got $actual_sha256"
  rm -f "$archive_path"
  exit 1
fi

log "extracting to $extract_dir"
mkdir -p "$extract_dir"
tar --extract --gzip --file "$archive_path" --directory "$extract_dir" --strip-components=1

if [[ ! -x "$bin_path" ]]; then
  log "extracted archive did not produce an executable at $bin_path"
  exit 1
fi

log "ready: $bin_path"
echo "$bin_path"
