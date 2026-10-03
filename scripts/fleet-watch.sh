#!/usr/bin/env bash
# Poll a fleet_dispatch watch endpoint until a terminal event arrives.
#
# The SSE stream this URL serves can drop the connection almost immediately,
# so a single long-lived `curl -N | grep` never sees the terminal event. This
# retries the connection in a loop instead of trusting one that stays open.
#
# Usage: fleet-watch.sh <watch-url> [poll-interval-seconds]
#
# <watch-url> is the URL fleet_dispatch's response prints after "watch for
# completion instantly with:". Prints the first `data:` line it receives
# (the terminal status/result JSON) and exits 0.
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <watch-url> [poll-interval-seconds]" >&2
  exit 64
fi

watch_url="$1"
poll_interval="${2:-60}"

while true; do
  event_line="$(curl -sN "${watch_url}" 2>/dev/null | grep '^data:' | head -1 || true)"
  if [[ -n "${event_line}" ]]; then
    echo "${event_line}"
    exit 0
  fi
  sleep "${poll_interval}"
done
