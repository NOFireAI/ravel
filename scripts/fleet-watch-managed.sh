#!/usr/bin/env bash
# Poll a fleet_dispatch watch endpoint in bounded runs.
#
# The harness kills background processes after about 10 minutes, so the
# unbounded fleet-watch.sh dies unnoticed. This variant polls for at most
# BUDGET seconds and then exits 75 so the caller relaunches it. Arm it
# under a Monitor until-loop that relaunches on exit 75. Exit 0 prints
# the terminal event. Any other exit code is an error; do not relaunch.
#
# Usage: fleet-watch-managed.sh <watch-url> [budget-seconds=480] [poll-seconds=20]
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <watch-url> [budget-seconds] [poll-seconds]" >&2
  exit 64
fi

watch_url="$1"
budget="${2:-480}"
poll="${3:-20}"

deadline=$(( $(date +%s) + budget ))

while (( $(date +%s) < deadline )); do
  # The SSE stream drops almost immediately (see fleet-watch.sh). Take the
  # first data: line of a short-lived connection if one arrives.
  event_line="$(curl -s --max-time 25 "${watch_url}" 2>/dev/null | grep '^data:' | head -1 || true)"
  if [[ -n "${event_line}" ]]; then
    echo "${event_line}"
    exit 0
  fi
  sleep "${poll}"
done

echo "fleet-watch-managed.sh: budget of ${budget}s elapsed, no terminal event; relaunch me." >&2
exit 75
