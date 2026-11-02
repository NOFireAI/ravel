#!/usr/bin/env bash
# box/fetch.sh -- poll the s3 lane's out/<run_id>/ prefix for DONE/FAILED
# and download results.<lane>.json + summary.txt + calls.jsonl once
# present. Mirrors benchmarks/clickbench/kit/fetch.sh's polling shape.
#
# Usage: fetch.sh <run.toml> <dest-dir> [--dry-run] [--poll-interval-seconds N]
set -u

RUN_TOML="${1:-}"
DEST="${2:-}"
DRY_RUN=0
POLL_S=30
shift 2 2>/dev/null || true
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --poll-interval-seconds) shift; POLL_S="$1" ;;
  esac
  shift
done
[ -n "$RUN_TOML" ] && [ -n "$DEST" ] || { echo "usage: fetch.sh <run.toml> <dest-dir> [--dry-run]" >&2; exit 64; }

run() {
  if [ "$DRY_RUN" = 1 ]; then
    { printf 'DRY-RUN:'; printf ' %q' "$@"; printf '\n'; }
    return 0
  fi
  "$@"
}

read_toml_key() {
  python3 - "$RUN_TOML" "$1" <<'EOF'
import sys, tomllib
with open(sys.argv[1], "rb") as fh:
    cfg = tomllib.load(fh)
key = sys.argv[2]
val = cfg
for part in key.split("."):
    if not isinstance(val, dict) or part not in val:
        val = None
        break
    val = val[part]
print("" if val is None else val)
EOF
}

main() {
  local bucket prefix run_id uri
  bucket="$(read_toml_key bucket)"
  prefix="$(read_toml_key prefix)"
  run_id="$(read_toml_key run_id)"
  uri="s3://$bucket/$prefix/out/$run_id"

  if [ "$DRY_RUN" = 1 ]; then
    run aws s3 ls "$uri/DONE"
    run aws s3 cp --recursive "$uri/" "$DEST/"
    return 0
  fi

  while true; do
    if aws s3 ls "$uri/DONE" >/dev/null 2>&1; then
      echo "DONE: $uri"
      run mkdir -p "$DEST"
      run aws s3 cp --recursive "$uri/" "$DEST/"
      return 0
    fi
    if aws s3 ls "$uri/FAILED" >/dev/null 2>&1; then
      echo "FAILED: $uri" >&2
      run mkdir -p "$DEST"
      run aws s3 cp --recursive "$uri/" "$DEST/"
      return 1
    fi
    sleep "$POLL_S"
  done
}

main
