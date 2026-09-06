#!/usr/bin/env bash
# Watch a fleet task to its terminal event, relaunching the bounded watcher.
#
# fleet-watch-managed.sh exits 75 when its budget elapses, expecting the
# caller to relaunch it. Hand-writing that loop in each Monitor command is
# where it goes wrong: a command that merely reports the 75 ends the watch,
# and every later terminal event is lost with nothing to say the watch died.
# Arm this instead, and the only notifications are the ones worth reading.
#
# One stdout line per event, so it suits a Monitor directly:
#   TERMINAL <data: line>   the task reached a terminal state (exit 0)
#   WATCH-ERROR code=N      the watcher failed; the watch is over, act on it
#   WATCH-GAVE-UP after Ns  the wall-clock cap elapsed with no terminal event
#
# Usage: fleet-watch-loop.sh <watch-url> [budget-seconds=540] [poll-seconds=60]
# Environment: FLEET_WATCH_MAX_SECONDS caps total wall clock (default 18000,
# comfortably past the fleet's own ~4h runtime ceiling).
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <watch-url> [budget-seconds] [poll-seconds]" >&2
  exit 64
fi

watch_url="$1"
budget="${2:-540}"
poll="${3:-60}"
max_seconds="${FLEET_WATCH_MAX_SECONDS:-18000}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
managed="${here}/fleet-watch-managed.sh"

if [[ ! -x "${managed}" ]]; then
  echo "WATCH-ERROR code=64 (${managed} not executable)"
  exit 64
fi

give_up_at=$(( $(date +%s) + max_seconds ))

while (( $(date +%s) < give_up_at )); do
  event="$("${managed}" "${watch_url}" "${budget}" "${poll}")" && code=0 || code=$?
  if (( code == 0 )); then
    echo "TERMINAL ${event}"
    exit 0
  fi
  if (( code != 75 )); then
    echo "WATCH-ERROR code=${code}"
    exit "${code}"
  fi
done

echo "WATCH-GAVE-UP after ${max_seconds}s with no terminal event"
exit 75
