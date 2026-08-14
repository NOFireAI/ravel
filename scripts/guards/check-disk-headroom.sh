#!/bin/sh
# check-disk-headroom.sh [dir] [min_gb] - fail fast before a cold cargo build
# when the volume backing <dir> has less than <min_gb> free (default 20).
#
# ENOSPC mid-link surfaces as a FAKE compiler error (`ld: write() failed,
# errno=28`, `linking with cc failed`, `couldn't create a temp dir`) and gets
# misdiagnosed as a code bug; a full disk can also break the harness's own
# stdout-capture so no command runs at all. Call this before any
# --all-targets / --workspace / feature-lane build or verify-dispatch gate.
#
# Set FLEET_DISK_REAP=1 to auto-run scripts/disk-reap.sh -y when below floor.
set -u

dir=${1:-.}
min_gb=${2:-20}

if [ ! -e "$dir" ]; then
  echo "guard: path does not exist: $dir" >&2
  exit 1
fi

df_line=''
df_line=$(df -Pk "$dir" 2>/dev/null | awk 'NR==2{print; exit}')
if [ -z "$df_line" ]; then
  echo "guard: could not read df for $dir" >&2
  exit 1
fi

avail_kb=$(printf '%s\n' "$df_line" | awk '{print $4}')
case "$avail_kb" in
  ''|*[!0-9]*)
    echo "guard: could not parse available KB from: $df_line" >&2
    exit 1
    ;;
esac

avail_gb=$(( avail_kb / 1024 / 1024 ))

if [ "$avail_gb" -ge "$min_gb" ]; then
  exit 0
fi

echo "guard: LOW DISK - ${avail_gb}GB free on the volume for $dir, need >=${min_gb}GB." >&2
echo "guard: A cold cargo build here will die mid-link with ENOSPC (errno=28)," >&2
echo "guard: which prints as a fake 'linking with cc failed' - NOT a code bug." >&2

reap="$(dirname "$0")/../disk-reap.sh"
if [ "${FLEET_DISK_REAP:-0}" = "1" ] && [ -x "$reap" ]; then
  echo "guard: FLEET_DISK_REAP=1 - running disk-reap.sh -y" >&2
  "$reap" -y || true
  avail_kb=$(df -Pk "$dir" 2>/dev/null | awk 'NR==2{print $4}')
  avail_kb=${avail_kb:-0}
  case "$avail_kb" in ''|*[!0-9]*) avail_kb=0 ;; esac
  avail_gb=$(( avail_kb / 1024 / 1024 ))
  if [ "$avail_gb" -ge "$min_gb" ]; then
    echo "guard: reaped to ${avail_gb}GB free." >&2
    exit 0
  fi
  echo "guard: still ${avail_gb}GB after reap - free space manually before building." >&2
fi

exit 1
