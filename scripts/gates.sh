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

echo "==> cargo fmt --all --check"
cargo fmt --all --check

if [[ ${#crate_args[@]} -eq 0 ]]; then
  echo "==> cargo clippy --workspace --all-targets -- -D warnings"
  cargo clippy --workspace --all-targets -- -D warnings
  echo "==> cargo test --workspace"
  cargo test --workspace
  echo "==> cargo test --doc"
  cargo test --doc
else
  echo "==> cargo clippy ${crate_args[*]} --all-targets -- -D warnings"
  cargo clippy "${crate_args[@]}" --all-targets -- -D warnings
  echo "==> cargo test ${crate_args[*]}"
  cargo test "${crate_args[@]}"
  echo "==> cargo test --doc ${crate_args[*]}"
  cargo test --doc "${crate_args[@]}"
fi

echo "All gates passed."
