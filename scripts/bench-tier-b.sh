#!/usr/bin/env bash
# ADR-0070 tier B: run the pure-CPU criterion smoke set, collect its point
# estimates, and either record a baseline or compare against one.
#
# The bench set is exactly the one ADR-0070 decision 3 names: segment_encode
# groups, series_id_hash, merge_kway_vs_materialized, bytes_slice_vs_copy,
# logseg_encode, logseg_scan, and otap decode. These are pure-CPU and
# store-independent, which is why they can carry a timing comparison at all;
# anything touching object storage stays in the exact byte/request gates
# (tier A), not here.
#
# Usage:
#   scripts/bench-tier-b.sh record  <out.json> <label>
#   scripts/bench-tier-b.sh compare <baseline.json> [--enforce]
#
# record   runs the set and writes a baseline JSON tagged with <label> (use the
#          label to record the environment; a baseline whose provenance is not
#          on its face is unusable later).
# compare  runs the set and diffs it against <baseline.json>. Default advisory
#          (never fails); --enforce exits non-zero on a regression past the
#          threshold, which is the "advisory-that-can-block" mode. Advisory is
#          the ADR-0070 default; enforce is gated on the probation window.
#
# Env knobs (all have quick-CI defaults; a real reference-runner baseline should
# raise the sampling):
#   CARGO_BUILD_JOBS      cargo -j (default 4)
#   BENCH_SAMPLE_SIZE     criterion --sample-size (default 10, the floor)
#   BENCH_WARMUP          criterion --warm-up-time seconds (default 1)
#   BENCH_MEASURE         criterion --measurement-time seconds (default 3)
#   BENCH_THRESHOLD       compare threshold percent (default 15, the ADR value)
#   RAVEL_BENCH_MAX_SERIES  segment_encode cardinality cap (default 2000, small
#                           for quick runs; raise for a real baseline)
#   KEEP_CRITERION=1      do not wipe target/criterion before the run
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$PWD"

CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-4}"
BENCH_SAMPLE_SIZE="${BENCH_SAMPLE_SIZE:-10}"
BENCH_WARMUP="${BENCH_WARMUP:-1}"
BENCH_MEASURE="${BENCH_MEASURE:-3}"
BENCH_THRESHOLD="${BENCH_THRESHOLD:-15}"
RAVEL_BENCH_MAX_SERIES="${RAVEL_BENCH_MAX_SERIES:-2000}"
export CARGO_BUILD_JOBS RAVEL_BENCH_MAX_SERIES
# Never let ANSI codes into anything downstream reads. We parse JSON files, not
# criterion stdout, but keep the invariant explicit.
export CARGO_TERM_COLOR=never

# Criterion writes under the cargo target directory, which is not always
# ./target: this repo's executors redirect it via CARGO_TARGET_DIR. Honor that,
# falling back to ./target when it is unset.
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"
CRITERION_DIR="$TARGET_DIR/criterion"

# The named set: "cargo -p CRATE --bench NAME".
BENCHES=(
  "ravel-bench:segment_encode"
  "ravel-bench:series_id_hash"
  "ravel-query:merge_kway_vs_materialized"
  "ravel-query:bytes_slice_vs_copy"
  "ravel-logseg:logseg_encode"
  "ravel-logseg:logseg_scan"
  "ravel-otap:decode"
)

run_set() {
  if [ -z "${KEEP_CRITERION:-}" ]; then
    rm -rf "$CRITERION_DIR"
  fi
  local entry crate bench
  for entry in "${BENCHES[@]}"; do
    crate="${entry%%:*}"
    bench="${entry##*:}"
    echo ">>> $crate :: $bench" >&2
    cargo bench --locked -p "$crate" --bench "$bench" -- \
      --sample-size "$BENCH_SAMPLE_SIZE" \
      --warm-up-time "$BENCH_WARMUP" \
      --measurement-time "$BENCH_MEASURE"
  done
}

cmd="${1:-}"
case "$cmd" in
  record)
    out="${2:?usage: bench-tier-b.sh record <out.json> <label>}"
    label="${3:?usage: bench-tier-b.sh record <out.json> <label>}"
    run_set
    python3 "$ROOT/scripts/bench-compare.py" collect \
      --criterion-dir "$CRITERION_DIR" --out "$out" --label "$label"
    echo "bench-tier-b: recorded baseline at $out" >&2
    ;;
  compare)
    baseline="${2:?usage: bench-tier-b.sh compare <baseline.json> [--enforce]}"
    enforce=""
    if [ "${3:-}" = "--enforce" ]; then
      enforce="--enforce"
    fi
    current="$(mktemp)"
    trap 'rm -f "$current"' EXIT
    run_set
    python3 "$ROOT/scripts/bench-compare.py" collect \
      --criterion-dir "$CRITERION_DIR" --out "$current" \
      --label "current run ($(uname -m), $(nproc) cores)"
    # .gate-logs/ is gitignored and created by gates.sh, so a fresh CI
    # checkout does not have it. bench-compare.py opens --out-md for write
    # with no fallback, and this script runs under set -euo pipefail.
    mkdir -p "$ROOT/.gate-logs"
    python3 "$ROOT/scripts/bench-compare.py" compare \
      --baseline "$baseline" --current "$current" \
      --threshold "$BENCH_THRESHOLD" ${enforce:+$enforce} \
      --out-md "$ROOT/.gate-logs/bench-compare.md"
    ;;
  *)
    echo "usage: bench-tier-b.sh record <out.json> <label>" >&2
    echo "       bench-tier-b.sh compare <baseline.json> [--enforce]" >&2
    exit 2
    ;;
esac
