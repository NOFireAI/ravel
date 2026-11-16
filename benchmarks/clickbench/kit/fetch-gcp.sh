#!/usr/bin/env bash
# Collect a GCP ClickBench kit run (launch-gcp.sh) and judge it against its
# pre-registration.
#
# Polls gs://<bucket>/<prefix>/runs/<run-id>/out/A/ until it holds DONE or
# FAILED, every --interval seconds (default 60), with no single gcloud call
# allowed more than 90 s. Each poll also lists the run's instances by label:
# no marker and no instance for two polls in a row means the box is gone
# without a word, which exits 6. On DONE it downloads results.json,
# evidence.tgz, progress.log and DONE under --out (default
# ./clickbench-runs/<run-id>/), validates results.json, writes the run as one
# ClickBench results file, <machine>.json (kit.py upstream-result), and
# judges it with kit.py prereg against the prereg.toml launch-gcp.sh shipped
# in the run's in/kit.tgz.
#
# Written for bash 3.2 (macOS /bin/bash).
#
# Exit codes:
#   0   DONE and every pre-registered figure inside its band
#   1   a figure missed its band
#   2   the box wrote FAILED (its content is printed), a download or
#       results.json is missing or malformed, or a pre-registered figure is
#       missing or emitted twice
#   3   --timeout elapsed before a marker
#   4   GCS unreachable: --unreachable-tries consecutive polls failed (default
#       3), or a download failed with an error other than a missing object
#   6   the instance is gone and wrote no marker
#   64  bad usage
set -uo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/clickbench/kit/fetch-gcp.sh --run-toml PATH [--out DIR]
         [--interval S] [--timeout S] [--unreachable-tries N] RUN_ID

  --run-toml           the run.toml the run was launched with (required)
  --out                where to put the downloads (default ./clickbench-runs/RUN_ID)
  --interval           seconds between polls (default 60)
  --timeout            seconds to wait for a marker (default 39600, eleven
                       hours: the box is deleted within ten)
  --unreachable-tries  consecutive failed polls before exit 4 (default 3)
EOF
}

die64() {
  echo "fetch-gcp: $*" >&2
  exit 64
}

KIT_DIR="$(cd "$(dirname "$0")" && pwd)"
CALL_LIMIT="${KIT_FETCH_CALL_LIMIT:-90}"
RUN_TOML=""
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
[ -n "$RUN_TOML" ] && [ -f "$RUN_TOML" ] || die64 "no run.toml at '$RUN_TOML' (pass --run-toml)"
[ -n "$OUT_DIR" ] || OUT_DIR="clickbench-runs/$RUN_ID"

kit_py() {
  python3 "$KIT_DIR/lib/kit.py" "$@"
}

[ "$(kit_py get "$RUN_TOML" cloud 2>/dev/null)" = gcp ] || die64 "$RUN_TOML is not a cloud = \"gcp\" run.toml"
PROJECT="$(kit_py get "$RUN_TOML" project)" || die64 "run.toml has no project"
BUCKET="$(kit_py get "$RUN_TOML" bucket)" || die64 "run.toml has no bucket"
PREFIX="$(kit_py get "$RUN_TOML" prefix)" || die64 "run.toml has no prefix"
STORE_PREFIX="$(kit_py get "$RUN_TOML" store_bucket_prefix)" || die64 "run.toml has no store_bucket_prefix"
NAMES="$(kit_py gcp-names "$RUN_ID" "$STORE_PREFIX")" || die64 "run id $RUN_ID names no GCP run"
STORE_BUCKET="$(printf '%s\n' "$NAMES" | sed -n 2p)"
LABEL="$(printf '%s\n' "$NAMES" | sed -n 3p)"
RUN_URI="gs://$BUCKET/${PREFIX%/}/runs/$RUN_ID"

# bounded CMD...: CMD, killed once it has run CALL_LIMIT seconds (exit 124).
bounded() {
  local pid watcher code=0
  "$@" &
  pid=$!
  (
    sleep "$CALL_LIMIT" </dev/null >/dev/null 2>&1
    kill -TERM "$pid" 2>/dev/null && echo "fetch-gcp: killed a gcloud call after ${CALL_LIMIT} s" >&2
  ) </dev/null >/dev/null &
  watcher=$!
  wait "$pid" || code=$?
  kill "$watcher" 2>/dev/null
  wait "$watcher" 2>/dev/null
  [ "$code" -ne 143 ] || code=124
  return "$code"
}

# marker: print DONE, FAILED or nothing; fail when GCS could not be asked.
# `gcloud storage ls` exits 1 saying "matched no objects" for an empty
# prefix, which is an answer.
marker() {
  local listing code=0
  listing="$(bounded gcloud storage ls --project "$PROJECT" "$RUN_URI/out/A/" 2>&1)" || code=$?
  if [ "$code" -ne 0 ]; then
    case "$listing" in
      *"matched no objects"*) return 0 ;;
    esac
    printf '%s\n' "$listing" >&2
    return 1
  fi
  case "$listing" in
    */out/A/FAILED*) echo FAILED ;;
    */out/A/DONE*) echo DONE ;;
  esac
}

# instances: the number of instances carrying the run's label, in any status;
# fails when Compute could not be asked.
instances() {
  local listing
  listing="$(bounded gcloud compute instances list --project "$PROJECT" \
    --filter "labels.ravel-clickbench-run=$LABEL" --format='value(name)')" || return 1
  printf '%s\n' "$listing" | grep -c . || true
}

# download NAME DEST: 0 fetched, 2 not there, 4 any other failure.
download() {
  local code=0 msg
  mkdir -p "$(dirname "$2")"
  msg="$(bounded gcloud storage cp --project "$PROJECT" --quiet "$RUN_URI/$1" "$2" 2>&1)" || code=$?
  [ "$code" = 0 ] && return 0
  printf '%s\n' "$msg" >&2
  case "$msg" in
    *"matched no objects"*|*"No URLs matched"*|*"404"*)
      echo "fetch-gcp: $RUN_URI/$1 is not there" >&2; return 2 ;;
  esac
  echo "fetch-gcp: download of $RUN_URI/$1 failed (gcloud exit $code)" >&2
  return 4
}

START="$(date +%s)"
FAILS=0
GONE=0
MARK=""
while :; do
  ok=1
  MARK="$(marker)" || ok=0
  n=""
  if [ "$ok" = 1 ] && [ -z "$MARK" ]; then
    n="$(instances)" || ok=0
  fi
  if [ "$ok" = 0 ]; then
    FAILS=$((FAILS + 1))
    echo "fetch-gcp: poll failed ($FAILS of $UNREACHABLE_TRIES)" >&2
    [ "$FAILS" -lt "$UNREACHABLE_TRIES" ] || { echo "fetch-gcp: GCS or Compute unreachable for $RUN_URI" >&2; exit 4; }
  else
    FAILS=0
    echo "fetch-gcp: $(date -u +%Y-%m-%dT%H:%M:%SZ) arm A ${MARK:-running}${n:+, $n instance(s)}" >&2
    [ -z "$MARK" ] || break
    if [ "$n" = 0 ]; then
      GONE=$((GONE + 1))
      [ "$GONE" -lt 2 ] || { echo "fetch-gcp: no instance labelled $LABEL and no marker under $RUN_URI/out/A/: the box is gone without a word" >&2; exit 6; }
    else
      GONE=0
    fi
  fi
  if [ $(($(date +%s) - START + INTERVAL)) -gt "$TIMEOUT" ]; then
    echo "fetch-gcp: no marker within $TIMEOUT s" >&2
    exit 3
  fi
  sleep "$INTERVAL"
done

if [ "$MARK" = FAILED ]; then
  code=0
  download out/A/FAILED "$OUT_DIR/A/FAILED" || code=$?
  [ "$code" -ne 4 ] || exit 4
  echo "fetch-gcp: arm A FAILED:" >&2
  cat "$OUT_DIR/A/FAILED" >&2 2>/dev/null || true
  download out/A/progress.log "$OUT_DIR/A/progress.log" || true
  download out/A/evidence.tgz "$OUT_DIR/A/evidence.tgz" || true
  exit 2
fi

for f in results.json evidence.tgz progress.log DONE; do
  code=0
  download "out/A/$f" "$OUT_DIR/A/$f" || code=$?
  [ "$code" -ne 4 ] || exit 4
  [ "$code" -eq 0 ] || exit 2
done
code=0
download in/run.toml "$OUT_DIR/run.toml" || code=$?
[ "$code" -ne 4 ] || exit 4
[ "$code" -eq 0 ] || exit 2
kit_py validate "$OUT_DIR/A/results.json" || { echo "fetch-gcp: results.json is malformed" >&2; exit 2; }

# The pre-registration the run was launched with, from its own kit.tgz, not
# whatever this checkout holds now.
code=0
download in/kit.tgz "$OUT_DIR/kit.tgz" || code=$?
[ "$code" -ne 4 ] || exit 4
[ "$code" -eq 0 ] || exit 2
rm -rf "$OUT_DIR/kit"
tar -xzf "$OUT_DIR/kit.tgz" -C "$OUT_DIR" || { echo "fetch-gcp: in/kit.tgz does not unpack" >&2; exit 2; }
PREREG="$OUT_DIR/kit/prereg.toml"
[ -f "$PREREG" ] || { echo "fetch-gcp: the run's kit.tgz holds no prereg.toml" >&2; exit 2; }
MACHINE="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["provenance"]["hardware"]["instance_type"])' \
  "$OUT_DIR/A/results.json")" || { echo "fetch-gcp: results.json names no machine" >&2; exit 2; }
[[ "$MACHINE" =~ ^[a-z0-9][a-z0-9.-]*$ ]] || { echo "fetch-gcp: machine '$MACHINE' is not a machine name" >&2; exit 2; }
kit_py upstream-result "$OUT_DIR/A/results.json" "$OUT_DIR/A/progress.log" >"$OUT_DIR/$MACHINE.json.tmp" \
  || { echo "fetch-gcp: cannot write the ClickBench results file" >&2; exit 2; }
mv "$OUT_DIR/$MACHINE.json.tmp" "$OUT_DIR/$MACHINE.json"
echo "fetch-gcp: ClickBench results file: $OUT_DIR/$MACHINE.json" >&2

echo "VERDICT      ARM      FIGURE                                         VALUE          BOUND"
code=0
kit_py prereg "$PREREG" "$OUT_DIR/A/results.json" "$OUT_DIR/A/progress.log" "$OUT_DIR/$MACHINE.json" \
  "$STORE_BUCKET" || code=$?
case "$code" in
  0) echo "fetch-gcp: every pre-registered figure is inside its band; results under $OUT_DIR" >&2 ;;
  1) echo "fetch-gcp: at least one figure MISSED its band; results under $OUT_DIR" >&2 ;;
  *) echo "fetch-gcp: the pre-registration could not be judged (exit $code); results under $OUT_DIR" >&2; code=2 ;;
esac
exit "$code"
