#!/usr/bin/env bash
# Writes the ClickBench Parquet lane's reference outputs (ADR-2040 D7):
# datafusion-cli 54.1.0 runs upstream create.sql, with its LOCATION pointed at
# a local copy of the single-file hits.parquet (arm B's definition of hits),
# then each statement of queries.sql, and statement N's result is written as
# --format json to qNN.json. clickbench_parquet_bench compares Ravel's answers
# against these files.
#
# Usage:
#   make-reference.sh <path/to/hits.parquet> [out-dir]
#
# out-dir defaults to ref/ beside this script. Besides q01.json..q43.json it
# gets describe.json (DESCRIBE hits, for the column-type check in
# docs/internal/clickbench.md) and, only once every statement has written its
# file, VERSION holding `datafusion-cli --version`. The bench refuses a
# directory without that VERSION, so a partial run cannot be mistaken for a
# reference.
#
# Exit 0 when every file was written; 1 when datafusion-cli is not 54.1.0
# (nothing is written), when create.sql's LOCATION cannot be edited, or when
# any statement failed (each is named on stderr); 64 on bad usage.
set -euo pipefail

want="datafusion-cli 54.1.0"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  echo "usage: make-reference.sh <path/to/hits.parquet> [out-dir]" >&2
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 64
fi
hits="$1"
out="${2:-$here/ref}"
if [[ ! -f "$hits" ]]; then
  echo "make-reference.sh: no such file: $hits" >&2
  exit 64
fi
if [[ "$hits" == *"'"* ]]; then
  echo "make-reference.sh: the hits.parquet path may not contain a quote: $hits" >&2
  exit 64
fi
hits_dir="$(cd "$(dirname "$hits")" && pwd)"
hits_abs="$hits_dir/$(basename "$hits")"

code=0
version="$(datafusion-cli --version)" || code=$?
if [[ $code -ne 0 ]]; then
  echo "make-reference.sh: datafusion-cli --version failed (exit $code)" >&2
  exit 1
fi
if [[ "$version" != "$want" ]]; then
  echo "make-reference.sh: refused: datafusion-cli reports '$version'; D7's reference is '$want'" >&2
  exit 1
fi

create="$(<"$here/create.sql")"
needle="LOCATION 'hits.parquet'"
after_first="${create#*"$needle"}"
if [[ "$after_first" == "$create" || "$after_first" == *"$needle"* ]]; then
  echo "make-reference.sh: create.sql must contain $needle exactly once" >&2
  exit 1
fi
edited="${create/"$needle"/"LOCATION '$hits_abs'"}"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$out"
rm -f "$out"/q[0-9][0-9].json "$out/describe.json" "$out/VERSION"

failed=()
# run_one <name> <statement>: writes <name>.json into $out, or records why not.
run_one() {
  local name="$1" statement="$2" rc=0
  printf '%s\n%s\n' "$edited" "$statement" >"$work/$name.sql"
  datafusion-cli -q --maxrows inf --format json -f "$work/$name.sql" \
    >"$work/$name.json" 2>"$work/$name.err" || rc=$?
  if [[ $rc -ne 0 || -s "$work/$name.err" ]]; then
    failed+=("$name (exit $rc): $(<"$work/$name.err")")
    return 0
  fi
  mv "$work/$name.json" "$out/$name.json"
}

n=0
while IFS= read -r statement || [[ -n "$statement" ]]; do
  if [[ -z "$statement" ]]; then
    continue
  fi
  n=$((n + 1))
  run_one "$(printf 'q%02d' "$n")" "$statement"
done <"$here/queries.sql"
run_one describe "DESCRIBE hits;"

if [[ $n -ne 43 ]]; then
  failed+=("queries.sql holds $n statements, not 43")
fi
if [[ ${#failed[@]} -gt 0 ]]; then
  for f in "${failed[@]}"; do
    echo "make-reference.sh: failed: $f" >&2
  done
  echo "make-reference.sh: ${#failed[@]} failures; VERSION not written" >&2
  exit 1
fi
printf '%s\n' "$version" >"$out/VERSION"
echo "make-reference.sh: wrote $n statements, describe.json and VERSION to $out" >&2
