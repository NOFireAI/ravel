#!/usr/bin/env bash
# box/fetch.sh: collect the s3 lane of a selective-investigations run that
# box/launch.sh started (issue #3699).
#
# Polls s3://<bucket>/<prefix>/runs/<run-id>/out/ for DONE or FAILED every
# --interval seconds (default 60), with no single aws call allowed more than
# 90 s, and gives up box.max_minutes + 30 minutes after it started (the
# instance has shut itself down by then). It then downloads out/ under --out
# (default ./investigations-runs/<run-id>/) and prints summary.s3.txt, or the
# FAILED marker. run.sh puts the marker only after the rest of out/ uploaded,
# so a prefix with no marker holds an incomplete run: fetch.sh refuses to
# download it unless --partial is given, which checks for a marker once and,
# with none, downloads what is there and exits 3. It then names the failed
# commands' logs run.sh kept under out/logs/, and the states run.sh recorded
# failed (stamps/failed_states.jsonl).
#
# --dry-run prints the aws commands one poll would run, and exits 0.
#
# Exit codes:
#   0   DONE and the verdict is pass
#   1   DONE and the verdict is fail, or FAILED
#   3   no DONE or FAILED within the bound (nothing downloaded), or
#       --partial found none (the incomplete out/ downloaded)
#   64  bad usage
#   69  a marker check could not be answered (aws exited other than 0 or 1)
set -uo pipefail

usage() {
  echo "usage: benchmarks/investigations/box/fetch.sh [--dry-run] [--partial] [--interval S] [--out DIR] RUN_TOML RUN_ID" >&2
  exit 64
}

BOX_DIR="$(cd "$(dirname "$0")" && pwd)"
DRY_RUN=0
PARTIAL=0
INTERVAL=60
DEST=""
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --partial) PARTIAL=1; shift ;;
    --interval) INTERVAL="${2:-}"; shift 2 ;;
    --out) DEST="${2:-}"; shift 2 ;;
    -*) usage ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
[ "${#ARGS[@]}" = 2 ] || usage
RUN_TOML="${ARGS[0]}"
RUN_ID="${ARGS[1]}"
[ -f "$RUN_TOML" ] || usage
[[ "$INTERVAL" =~ ^[0-9]+$ ]] && [ "$INTERVAL" -ge 1 ] || usage
[[ "$RUN_ID" =~ ^investigations-[0-9A-Za-z-]+$ ]] || { echo "fetch: '$RUN_ID' is not a launch.sh run id" >&2; exit 64; }

toml() { python3 "$BOX_DIR/lib/inv.py" get "$RUN_TOML" "$@"; }
REGION="$(toml box.region)" || exit 64
BUCKET="$(toml box.bucket)" || exit 64
PREFIX="$(toml box.prefix --default investigations)"
MAX_MINUTES="$(toml box.max_minutes --default 600)"
URI="s3://$BUCKET/${PREFIX%/}/runs/$RUN_ID/out"
DEST="${DEST:-./investigations-runs/$RUN_ID}"

aws_call() { timeout 90 aws --region "$REGION" "$@"; }

if [ "$DRY_RUN" = 1 ]; then
  printf 'DRY-RUN: timeout 90 aws --region %s s3 ls %s/DONE\n' "$REGION" "$URI"
  printf 'DRY-RUN: timeout 90 aws --region %s s3 ls %s/FAILED\n' "$REGION" "$URI"
  if [ "$PARTIAL" = 1 ]; then
    printf 'DRY-RUN: no marker: aws s3 cp --recursive %s/ %s/ and exit 3 (partial)\n' "$URI" "$DEST"
  else
    printf 'DRY-RUN: poll every %s s for at most %s minutes, then aws s3 cp --recursive %s/ %s/\n' \
      "$INTERVAL" "$((MAX_MINUTES + 30))" "$URI" "$DEST"
    printf 'DRY-RUN: no marker within the bound: refuse the download, exit 3 (--partial fetches it)\n'
  fi
  exit 0
fi

deadline=$(($(date +%s) + (MAX_MINUTES + 30) * 60))
[ "$PARTIAL" = 0 ] || deadline=0
# aws s3 ls exits 1 for a key that is not there; anything else (255 for a
# credential or endpoint error, 124 from timeout) means the question was
# never answered, which must not read as "no marker yet".
has_marker() {
  local code=0
  aws_call s3 ls "$URI/$1" >/dev/null 2>"$ERR_LOG" || code=$?
  case "$code" in
    0) return 0 ;;
    1) return 1 ;;
    *) echo "fetch: could not ask for $URI/$1 (aws exit $code):" >&2; cat "$ERR_LOG" >&2; exit 69 ;;
  esac
}
ERR_LOG="$(mktemp)"
trap 'rm -f "$ERR_LOG"' EXIT
marker=""
while :; do
  if has_marker DONE; then
    marker=DONE
    break
  fi
  if has_marker FAILED; then
    marker=FAILED
    break
  fi
  [ "$(date +%s)" -lt "$deadline" ] || break
  sleep "$INTERVAL"
done
if [ -z "$marker" ] && [ "$PARTIAL" = 1 ]; then
  mkdir -p "$DEST"
  aws_call s3 cp --only-show-errors --recursive "$URI/" "$DEST/" || { echo "fetch: download of $URI failed" >&2; exit 1; }
  echo "fetch: PARTIAL: no DONE or FAILED under $URI; downloaded what is there to $DEST" >&2
  exit 3
fi
if [ -z "$marker" ]; then
  echo "fetch: no DONE or FAILED under $URI after $((MAX_MINUTES + 30)) minutes; refusing an incomplete prefix (--partial downloads it)" >&2
  exit 3
fi

mkdir -p "$DEST"
aws_call s3 cp --only-show-errors --recursive "$URI/" "$DEST/" || { echo "fetch: download of $URI failed" >&2; exit 1; }
if [ -s "$DEST/stamps/failed_states.jsonl" ]; then
  echo "fetch: failed states (stamps/failed_states.jsonl):" >&2
  cat "$DEST/stamps/failed_states.jsonl" >&2
fi
if [ -d "$DEST/logs" ]; then
  echo "fetch: failed commands' logs under $DEST/logs/:" >&2
  ls "$DEST/logs" >&2
fi
if [ "$marker" = FAILED ]; then
  cat "$DEST/FAILED" >&2
  exit 1
fi
[ -f "$DEST/summary.s3.txt" ] && cat "$DEST/summary.s3.txt"
cat "$DEST/DONE"
grep -q '^DONE verdict=pass ' "$DEST/DONE"
