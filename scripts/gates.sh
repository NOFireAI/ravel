#!/usr/bin/env bash
# Run Ravel's merge gates (CLAUDE.md "Gates"): fmt, clippy, tests, doctests.
#
# Usage:
#   scripts/gates.sh                 # workspace-wide -- run this once,
#                                     # right before a commit that touches
#                                     # more than one crate
#   scripts/gates.sh -p CRATE ...     # scope clippy/test to one or more
#                                     # crates -- fast local iteration
set -euo pipefail

crate_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p)
      shift
      if [[ $# -eq 0 ]]; then
        echo "gates.sh: -p requires a crate name" >&2
        exit 64
      fi
      crate_args+=("-p" "$1")
      shift
      ;;
    *)
      echo "gates.sh: unknown argument: $1" >&2
      exit 64
      ;;
  esac
done

# --locked everywhere so a local green gate warms the exact artifacts CI's
# `check` job reuses (and vice versa): CI runs against a committed
# Cargo.lock, so a divergent local resolve would recompile from scratch on
# CI and mask lockfile drift here. `cargo fmt` (rustfmt) does not accept
# --locked after the subcommand, so it takes the global-flag position.
echo "==> cargo --locked fmt --all --check"
cargo --locked fmt --all --check

# Match CI's `check` job: it runs `cargo nextest run --workspace
# --cargo-profile ci`. Use nextest when it is installed so a local run
# warms the same `ci`-profile artifacts CI reuses; fall back to `cargo
# test` (which every dev machine has) otherwise. nextest cannot run
# doctests, so those always go through `cargo test --doc`, pinned to the
# `ci` profile for consistency. `command -v` in an `if` condition does not
# trip `set -e` when nextest is absent.
if command -v cargo-nextest >/dev/null 2>&1; then
  have_nextest=1
else
  have_nextest=0
fi

if [[ ${#crate_args[@]} -eq 0 ]]; then
  echo "==> cargo clippy --locked --workspace --all-targets -- -D warnings"
  cargo clippy --locked --workspace --all-targets -- -D warnings
  if [[ ${have_nextest} -eq 1 ]]; then
    echo "==> cargo nextest run --locked --workspace --cargo-profile ci"
    cargo nextest run --locked --workspace --cargo-profile ci
  else
    echo "==> cargo test --locked --workspace"
    cargo test --locked --workspace
  fi
  echo "==> cargo test --locked --doc --profile ci --workspace"
  cargo test --locked --doc --profile ci --workspace
else
  echo "==> cargo clippy --locked ${crate_args[*]} --all-targets -- -D warnings"
  cargo clippy --locked "${crate_args[@]}" --all-targets -- -D warnings
  if [[ ${have_nextest} -eq 1 ]]; then
    echo "==> cargo nextest run --locked ${crate_args[*]} --cargo-profile ci"
    cargo nextest run --locked "${crate_args[@]}" --cargo-profile ci
  else
    echo "==> cargo test --locked ${crate_args[*]}"
    cargo test --locked "${crate_args[@]}"
  fi
  echo "==> cargo test --locked --doc --profile ci ${crate_args[*]}"
  cargo test --locked --doc --profile ci "${crate_args[@]}"
fi

echo "All gates passed."
