#!/usr/bin/env bash
# Checks a built ravel-dev guest image (.brig/Dockerfile) against this
# checkout. The brig-image workflow runs it before a pull request passes and
# before main publishes.
#
#   .brig/check-image.sh <image>
#
# Run from the repository root. It fails when the image's Rust toolchain is
# not the one rust-toolchain.toml names, a component or gate tool is missing,
# Node's major version is not sdk/typescript/.node-version, or the image's
# registry cannot serve Cargo.lock offline.
set -euo pipefail

image=${1:?usage: .brig/check-image.sh <image>}
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

channel=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' rust-toolchain.toml)
node_major=$(tr -d ' \r\n' < sdk/typescript/.node-version)
node_major=${node_major#v}
node_major=${node_major%%.*}
[ -n "$channel" ] || { echo "check-image: no channel in rust-toolchain.toml" >&2; exit 1; }
[ -n "$node_major" ] || { echo "check-image: sdk/typescript/.node-version is empty" >&2; exit 1; }

# Brig mounts the guest home over /root, so the checks run with an empty one
# to prove nothing the image needs lives there.
run() {
  docker run --rm --entrypoint bash --tmpfs /root -e HOME=/root "$image" -c "$1"
}

fail=0
check() {
  local name=$1 script=$2 out
  if out=$(run "$script" 2>&1); then
    echo "ok    $name: $out"
  else
    echo "FAIL  $name: $out" >&2
    fail=1
  fi
}

check rustc "rustc --version | grep -F 'rustc $channel ' "
check default-toolchain "rustup default | grep -F '$channel-'"
check one-toolchain "test \$(rustup toolchain list | wc -l) -eq 1 && rustup toolchain list"
check components "rustup component list --installed | grep -E '^(rustfmt|clippy|llvm-tools)' | sort | tr '\n' ' ' | grep -E 'clippy.*llvm-tools.*rustfmt'"
check nextest "cargo nextest --version | head -n 1"
check cargo-deny "cargo deny --version"
check node "node --version | grep -E '^v$node_major\\.'"
check npm-cache "test -d \"\$npm_config_cache/_cacache\" && echo \$npm_config_cache"
for tool in git gh jq python3 curl claude cc; do
  check "$tool" "command -v $tool"
done

# The registry serves Cargo.lock with no network: the checkout is mounted
# read-only and cargo fetch runs offline against the image's registry.
if out=$(docker run --rm --entrypoint bash --network none --tmpfs /root -e HOME=/root \
  -v "$root:/src:ro" -w /src "$image" -c 'cargo fetch --locked --offline' 2>&1); then
  echo "ok    registry: cargo fetch --locked --offline"
else
  echo "FAIL  registry: $out" >&2
  fail=1
fi

exit "$fail"
