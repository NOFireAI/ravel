#!/usr/bin/env bash
# Collect a ClickBench kit run and judge arm B against arm A (ADR-2867
# change 2).
#
# Polls s3://<bucket>/<prefix>/runs/<run-id>/out/A/ and out/B/ until each holds
# DONE or FAILED, every --interval seconds (default 60), with no single aws
# call allowed more than 90 s. It then downloads both arms' results.json and
# evidence.tgz, and in/kit.tgz for the band files the run actually used,
# under --out (default ./clickbench-runs/<run-id>/), and prints each arm's
# absolute band table followed by every lane's relative bands, arm B against
# arm A, when run.toml's band_sections selected them:
#
#   pooled error rate   at most A's + relative.error_pooled_max_delta; a
#                       wrong-answer call counts as an error
#   QPS                 at least relative.qps_min_ratio x A's
#   each statement hot  at most max(relative.hot_statement_max_ratio x A's,
#                       relative.hot_statement_floor_s)
#   hot sum, cold sum   each at most relative.hot_sum_max_ratio /
#                       relative.cold_sum_max_ratio x A's, both summed over
#                       the statements both arms answered
#   each statement      a statement A answered and B did not is a miss,
#                       named by id
#
# A band that cannot be judged (QPS with no concurrency phase, a statement
# arm A failed) is printed INCONCLUSIVE and is not a miss.
#
# Written for bash 3.2 (macOS /bin/bash).
#
# Exit codes:
#   0   both arms DONE and every band passed or was inconclusive
#   1   a band missed, absolute on either arm or relative
#   2   an arm wrote FAILED (its content is printed), or a results.json is
#       missing or malformed
#   3   --timeout elapsed before both arms wrote a marker
#   4   S3 unreachable: --unreachable-tries consecutive polls failed (default
#       3), or a download failed with an error other than a missing object
#       (a 403 included)
#   64  bad usage
set -uo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/clickbench/kit/fetch.sh [--run-toml PATH] [--out DIR]
         [--interval S] [--timeout S] [--unreachable-tries N] RUN_ID

  --run-toml           the run.toml the run was launched with (default ./run.toml)
  --out                where to put the downloads (default ./clickbench-runs/RUN_ID)
  --interval           seconds between polls (default 60)
  --timeout            seconds to wait for both markers (default 39600, eleven
                       hours: the boxes terminate themselves after ten)
  --unreachable-tries  consecutive failed polls before exit 4 (default 3)
EOF
}

die64() {
  echo "fetch: $*" >&2
  exit 64
}

KIT_DIR="$(cd "$(dirname "$0")" && pwd)"
CALL_LIMIT="${KIT_FETCH_CALL_LIMIT:-90}"
RUN_TOML="run.toml"
OUT_DIR=""
INTERVAL=60
TIMEOUT=39600
UNREACHABLE_TRIES=3
RUN_ID=""
while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h) usage; exit 0 ;;
    --run-toml|--out|--interval|--timeout|--unreachable-tries)
      [ $# -ge 2 ] || die64 "$1 needs a value"
      case "$1" in
        --run-toml) RUN_TOML="$2" ;;
        --out) OUT_DIR="$2" ;;
        --interval) INTERVAL="$2" ;;
        --timeout) TIMEOUT="$2" ;;
        --unreachable-tries) UNREACHABLE_TRIES="$2" ;;
      esac
      shift 2; continue ;;
    -*) usage >&2; die64 "unknown argument: $1" ;;
    *) [ -z "$RUN_ID" ] || die64 "one run id only"; RUN_ID="$1" ;;
  esac
  shift
done
[ -n "$RUN_ID" ] || { usage >&2; die64 "missing RUN_ID"; }
[[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]] || die64 "run id '$RUN_ID' is not a run id"
for n in "$INTERVAL" "$TIMEOUT" "$UNREACHABLE_TRIES" "$CALL_LIMIT"; do
  [[ "$n" =~ ^[0-9]+$ ]] && [ "$n" -gt 0 ] || die64 "'$n' is not a whole number above 0"
done
[ "$CALL_LIMIT" -le 90 ] || die64 "KIT_FETCH_CALL_LIMIT may not exceed 90 s"
[ -f "$RUN_TOML" ] || die64 "no run.toml at $RUN_TOML (pass --run-toml)"
[ -n "$OUT_DIR" ] || OUT_DIR="clickbench-runs/$RUN_ID"

kit_py() {
  python3 "$KIT_DIR/lib/kit.py" "$@"
}

REGION="$(kit_py get "$RUN_TOML" region)" || die64 "run.toml has no region"
BUCKET="$(kit_py get "$RUN_TOML" bucket)" || die64 "run.toml has no bucket"
PREFIX="$(kit_py get "$RUN_TOML" prefix)" || die64 "run.toml has no prefix"
RUN_URI="s3://$BUCKET/${PREFIX%/}/runs/$RUN_ID"

# bounded CMD...: CMD, killed once it has run CALL_LIMIT seconds (exit 124).
# The watcher's sleep holds none of the caller's descriptors: it outlives the
# watcher, and a captured stderr would stay open until it ended.
bounded() {
  local pid watcher code=0
  "$@" &
  pid=$!
  (
    sleep "$CALL_LIMIT" </dev/null >/dev/null 2>&1
    kill -TERM "$pid" 2>/dev/null && echo "fetch: killed an aws call after ${CALL_LIMIT} s" >&2
  ) </dev/null >/dev/null &
  watcher=$!
  wait "$pid" || code=$?
  kill "$watcher" 2>/dev/null
  wait "$watcher" 2>/dev/null
  [ "$code" -ne 143 ] || code=124
  return "$code"
}

# marker ARM: print DONE, FAILED or nothing; fail when S3 could not be asked.
# `aws s3 ls` exits 1 for an empty prefix, which is an answer.
marker() {
  local listing code=0
  listing="$(bounded aws s3 ls --region "$REGION" "$RUN_URI/out/$1/")" || code=$?
  [ "$code" -le 1 ] || return 1
  case "$listing" in
    *" FAILED"*) echo FAILED ;;
    *" DONE"*) echo DONE ;;
  esac
}

# download KEY DEST: 0 fetched, 2 not there, 4 any other failure. `aws s3 cp`
# exits 1 for a 403 as well as a 404, so only a 404 reads as not there.
download() {
  local code=0 msg
  mkdir -p "$(dirname "$2")"
  msg="$(bounded aws s3 cp --region "$REGION" --only-show-errors "$RUN_URI/$1" "$2" 2>&1)" || code=$?
  [ -z "$msg" ] || printf '%s\n' "$msg" >&2
  case "$code" in
    0) return 0 ;;
    1)
      case "$msg" in
        *"(404)"*|*NoSuchKey*|*"does not exist"*)
          echo "fetch: $RUN_URI/$1 is not there" >&2; return 2 ;;
      esac ;;
  esac
  echo "fetch: download of $RUN_URI/$1 failed (aws exit $code)" >&2
  return 4
}

START="$(date +%s)"
FAILS=0
MARK_A=""
MARK_B=""
while :; do
  ok=1
  [ -n "$MARK_A" ] || MARK_A="$(marker A)" || ok=0
  [ "$ok" = 0 ] || [ -n "$MARK_B" ] || MARK_B="$(marker B)" || ok=0
  if [ "$ok" = 0 ]; then
    FAILS=$((FAILS + 1))
    echo "fetch: S3 poll failed ($FAILS of $UNREACHABLE_TRIES)" >&2
    [ "$FAILS" -lt "$UNREACHABLE_TRIES" ] || { echo "fetch: S3 unreachable at $RUN_URI" >&2; exit 4; }
  else
    FAILS=0
    echo "fetch: $(date -u +%Y-%m-%dT%H:%M:%SZ) arm A ${MARK_A:-running}, arm B ${MARK_B:-running}" >&2
    if [ -n "$MARK_A" ] && [ -n "$MARK_B" ]; then
      break
    fi
    case "$MARK_A$MARK_B" in
      *FAILED*) break ;;
    esac
  fi
  if [ $(($(date +%s) - START + INTERVAL)) -gt "$TIMEOUT" ]; then
    echo "fetch: no marker from both arms within $TIMEOUT s (arm A ${MARK_A:-none}, arm B ${MARK_B:-none})" >&2
    exit 3
  fi
  sleep "$INTERVAL"
done

worst=0
for arm in A B; do
  m="$MARK_A"
  [ "$arm" = A ] || m="$MARK_B"
  if [ "$m" = FAILED ]; then
    code=0
    download "out/$arm/FAILED" "$OUT_DIR/$arm/FAILED" || code=$?
    [ "$code" -ne 4 ] || exit 4
    echo "fetch: arm $arm FAILED:" >&2
    cat "$OUT_DIR/$arm/FAILED" >&2 2>/dev/null || true
    download "out/$arm/progress.log" "$OUT_DIR/$arm/progress.log" || true
    download "out/$arm/evidence.tgz" "$OUT_DIR/$arm/evidence.tgz" || true
    worst=2
  fi
done
[ "$worst" = 0 ] || exit 2

for arm in A B; do
  for f in results.json evidence.tgz progress.log DONE; do
    code=0
    download "out/$arm/$f" "$OUT_DIR/$arm/$f" || code=$?
    [ "$code" -ne 4 ] || exit 4
    if [ "$code" -ne 0 ] && [ "$f" = results.json ]; then exit 2; fi
  done
  kit_py validate "$OUT_DIR/$arm/results.json" || { echo "fetch: arm $arm results.json is malformed" >&2; exit 2; }
done

code=0
download in/kit.tgz "$OUT_DIR/kit.tgz" || code=$?
[ "$code" -ne 4 ] || exit 4
[ "$code" -eq 0 ] || exit 2
rm -rf "$OUT_DIR/kit"
tar -xzf "$OUT_DIR/kit.tgz" -C "$OUT_DIR" || { echo "fetch: in/kit.tgz does not unpack" >&2; exit 2; }

miss=0
echo "VERDICT      ARM      BAND                                           VALUE          BOUND"
for arm in A B; do
  code=0
  kit_py absolute "$arm" "$OUT_DIR/$arm/results.json" || code=$?
  case "$code" in
    0) ;;
    1) miss=1 ;;
    *) exit 2 ;;
  esac
done
for band in "$OUT_DIR"/kit/bands/*.toml; do
  [ -f "$band" ] || continue
  code=0
  kit_py relative "$band" "$OUT_DIR/A/results.json" "$OUT_DIR/B/results.json" || code=$?
  case "$code" in
    0) ;;
    1) miss=1 ;;
    *) exit 2 ;;
  esac
done
if [ "$miss" = 1 ]; then
  echo "fetch: at least one band MISSED; results under $OUT_DIR" >&2
  exit 1
fi
echo "fetch: every band passed or was inconclusive; results under $OUT_DIR" >&2
