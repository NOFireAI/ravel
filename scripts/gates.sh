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

# Linker OOM guard. On low-memory hosts (fleet executors run with 8 GB)
# the default parallelism links several multi-GB test binaries at once and
# the kernel kills ld ("signal 9"). That is a resource failure, not a code
# failure, and it wastes a full gate cycle. Cap build jobs before it
# happens. An explicit CARGO_BUILD_JOBS in the environment wins.
if [[ -z "${CARGO_BUILD_JOBS:-}" ]]; then
  mem_gb=""
  if [[ "$(uname)" == "Darwin" ]]; then
    mem_gb=$(( $(sysctl -n hw.memsize) / 1073741824 ))
  elif [[ -r /proc/meminfo ]]; then
    mem_gb=$(( $(awk '/^MemTotal/{print $2}' /proc/meminfo) / 1048576 ))
  fi
  if [[ -n "${mem_gb}" && "${mem_gb}" -le 11 ]]; then
    export CARGO_BUILD_JOBS=2
    echo "gates.sh: ${mem_gb} GB RAM; capping cargo build jobs at 2"
  fi
fi

# --locked everywhere so a local green gate warms the exact artifacts CI's
# `check` job reuses (and vice versa): CI runs against a committed
# Cargo.lock, so a divergent local resolve would recompile from scratch on
# CI and mask lockfile drift here. `cargo fmt` (rustfmt) does not accept
# --locked after the subcommand, so it takes the global-flag position.
echo "==> cargo --locked fmt --all --check"
cargo --locked fmt --all --check

# The test-hygiene shapes CLAUDE.md names under "Testing patterns": a
# wall-clock assertion, an unseeded rng, an untracked proptest seed. Each
# costs a gate rerun after the rule was already written down, so it is a
# check rather than another paragraph. A source scan, no build, so it runs
# ahead of the expensive lanes and fails at authoring time.
echo "==> scripts/guards/check-test-hygiene.sh"
"$(dirname "$0")/guards/check-test-hygiene.sh"

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

# Feature-gated surfaces (issues #609, #616, #714, #732). ravel-server's `sql`
# and `flight-sql` features, and ravel-bench's bench-lane features, are off by
# default, so nothing above compiles the SQL handler, the Flight SQL service,
# the bench lanes, or their tests. Without these lanes a local run prints "All
# gates passed" on a tree CI rejects. That happened while rebasing #511: the
# workspace gate was green and `--features sql` failed with E0061 on a call
# site that had gone stale under a textually clean merge. It happened again for
# ravel-bench: #712 merged on a receipt but went red in CI's features job
# because gates.sh never built ravel-bench's sql-latency tests, and #724's
# `flight-lane` feature left main uncompilable under
# `--features sql-latency,profiling,flight-lane` because nothing gated it.
#
# Mirrors CI's `lint`, `flight-sql`, and `features` jobs. In scoped mode these
# run only when the affected crates are named, so `-p ravel-logseg` does not
# pay for a ravel-server build it did not ask for.
run_feature_lane() {
  local feature="$1"
  shift
  echo "==> cargo clippy --locked $* --features ${feature} --all-targets -- -D warnings"
  cargo clippy --locked "$@" --features "${feature}" --all-targets -- -D warnings
  if [[ ${have_nextest} -eq 1 ]]; then
    echo "==> cargo nextest run --locked $* --features ${feature}"
    cargo nextest run --locked "$@" --features "${feature}"
  else
    echo "==> cargo test --locked $* --features ${feature}"
    cargo test --locked "$@" --features "${feature}"
  fi
}

want_features=0
want_bench=0
if [[ ${#crate_args[@]} -eq 0 ]]; then
  want_features=1
  want_bench=1
else
  for arg in "${crate_args[@]}"; do
    case "${arg}" in
      ravel-server | ravel-sql) want_features=1 ;;
    esac
    # ravel-bench's feature tests exercise these crates, so any of them in
    # scope should pay for the bench lanes.
    case "${arg}" in
      ravel-bench | ravel-sql | ravel-query | ravel-ingest) want_bench=1 ;;
    esac
  done
fi

if [[ ${want_features} -eq 1 ]]; then
  run_feature_lane sql -p ravel-server
  run_feature_lane flight-sql -p ravel-server -p ravel-sql
fi

# ravel-bench bench lanes. The ClickBench box builds
# `--features sql-latency,profiling` and `--features
# sql-latency,profiling,flight-lane`; gate the widest combination so a break in
# any of the three features (SQL corpus, profiling, Flight SQL bench) is caught
# locally. stage-timing is a separate lane CI checks and runs, kept at parity.
if [[ ${want_bench} -eq 1 ]]; then
  run_feature_lane sql-latency,profiling,flight-lane -p ravel-bench
  echo "==> cargo check --locked -p ravel-bench --features stage-timing --all-targets"
  cargo check --locked -p ravel-bench --features stage-timing --all-targets
  echo "==> cargo test --locked -p ravel-bench --features stage-timing"
  cargo test --locked -p ravel-bench --features stage-timing
fi

# --- Gates-pass receipt ---------------------------------------------------
# Record the tree this run passed. fleet-result-merge.sh honors
# FLEET_MERGE_SKIP_GATES=1 only for a tree with a receipt here. Keyed by
# TREE hash because the merge path's authorship/squash rewrite changes
# commit ids but not content. Receipts live in the shared common dir (one
# file per tree) so worktrees see each other's runs and concurrent
# sessions never clobber each other. Written only for a full (unscoped)
# run on a clean tree; a scoped or dirty run does not prove the tree.
if [[ ${#crate_args[@]} -eq 0 && -z "$(git status --porcelain --untracked-files=no 2>/dev/null)" ]]; then
  receipt_dir="$(cd "$(git rev-parse --git-common-dir)" && pwd)/gates-pass"
  mkdir -p "${receipt_dir}"
  gated_tree="$(git rev-parse 'HEAD^{tree}')"
  date -u +%Y-%m-%dT%H:%M:%SZ >"${receipt_dir}/${gated_tree}"
  echo "gates.sh: receipt written for tree ${gated_tree}"
fi

echo "All gates passed."
