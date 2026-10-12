#!/usr/bin/env bash
# Run tests for the named crates plus every workspace crate that depends on
# them (transitively), skipping the rest. This is the executor-side test
# gate: full-workspace verification still happens at merge time
# (verify-dispatch-gates.sh cold run and PR CI), so executors only need the
# blast radius of their own change.
#
# Usage: affected-tests.sh [-n] -p CRATE [-p CRATE ...]
#   -n  print the affected crate list and exit without running anything
#
# Uses the `ci` cargo build profile (no debug info: faster codegen, smaller
# artifacts) and cargo-nextest when installed, plain `cargo test` otherwise.
# Doctests run separately: nextest does not execute them.
#
# ravel-server's `sql` and `flight-sql` surfaces compile only under their
# features, which are off by default, so the plain closure run does not test
# them with the feature selected on its own. When ravel-server is in the
# closure this also runs
# `-p ravel-server --features sql`, and when ravel-sql or ravel-server is, `-p
# ravel-server -p ravel-sql --features flight-sql`, both on the `ci` profile.
# The exit code is the first failing run's, after every run has finished.
#
# scripts/gates.sh reads its scoped closure from `-n`, so `-n` prints the
# `affected crates:` line and nothing else.
set -u

dry=0
seeds=""
while getopts "np:" opt; do
  case "$opt" in
    n) dry=1 ;;
    p) seeds="$seeds $OPTARG" ;;
    *) echo "usage: $0 [-n] -p CRATE [-p CRATE ...]" >&2; exit 2 ;;
  esac
done
if [ -z "$seeds" ]; then
  echo "usage: $0 [-n] -p CRATE [-p CRATE ...]" >&2
  exit 2
fi

command -v jq >/dev/null 2>&1 || {
  echo "$0: jq is required to compute the reverse-dependency closure" >&2
  exit 2
}
meta=$(cargo metadata --format-version 1 --no-deps) || exit 1
members=$(printf '%s' "$meta" | jq -r '.packages[].name')

# "<pkg> <dep>" pairs, restricted to workspace-internal dependencies
# (dev and build deps included: a dev-dependency edge still means the
# dependent's tests exercise the changed crate).
pairs=$(printf '%s' "$meta" | jq -r '
  [.packages[].name] as $m
  | .packages[]
  | .name as $n
  | .dependencies[]
  | select(.name as $d | $m | index($d))
  | "\(.name) \($n)"')

for s in $seeds; do
  if ! printf '%s\n' "$members" | grep -qx "$s"; then
    echo "error: $s is not a workspace member" >&2
    exit 2
  fi
done

# Fixed-point reverse-dependency closure. Plain string sets: macOS ships
# bash 3.2, which has no associative arrays.
# shellcheck disable=SC2086
affected=" $(printf '%s ' $seeds)"
grew=1
while [ "$grew" -eq 1 ]; do
  grew=0
  while read -r dep pkg; do
    [ -z "$dep" ] && continue
    case "$affected" in
      *" $dep "*)
        case "$affected" in
          *" $pkg "*) ;;
          *) affected="$affected$pkg "; grew=1 ;;
        esac
        ;;
    esac
  done <<EOF
$pairs
EOF
done

crate_args=""
for c in $affected; do
  crate_args="$crate_args -p $c"
done

echo "affected crates:$affected"
if [ "$dry" -eq 1 ]; then
  exit 0
fi

if command -v cargo-nextest >/dev/null 2>&1; then
  have_nextest=1
else
  have_nextest=0
fi

# run_tests <extra cargo args...>: the closure's test runner with the given
# selection, on the `ci` profile.
run_tests() {
  if [ "$have_nextest" -eq 1 ]; then
    cargo nextest run --cargo-profile ci "$@"
  else
    cargo test --profile ci "$@"
  fi
}

rc=0
# shellcheck disable=SC2086
run_tests $crate_args || rc=$?

doc_rc=0
# shellcheck disable=SC2086
cargo test --doc --profile ci $crate_args || doc_rc=$?

sql_rc=0
case "$affected" in
  *" ravel-server "*)
    run_tests -p ravel-server --features sql || sql_rc=$?
    ;;
esac

flight_rc=0
case "$affected" in
  *" ravel-server "* | *" ravel-sql "*)
    run_tests -p ravel-server -p ravel-sql --features flight-sql || flight_rc=$?
    ;;
esac

for code in "$rc" "$doc_rc" "$sql_rc" "$flight_rc"; do
  if [ "$code" -ne 0 ]; then
    exit "$code"
  fi
done
exit 0
