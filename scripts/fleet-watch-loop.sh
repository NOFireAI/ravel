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

# Reject a bad duration before the loop rather than inside it. A budget of 0 or
# a non-number makes the managed watcher exit 75 immediately, and this loop
# treats 75 as "relaunch", so an unchecked value spins as fast as the shell can
# fork until the wall-clock cap.
require_positive_int() {
  local name="$1" value="$2"
  if [[ ! ${value} =~ ^[0-9]+$ ]] || (( value <= 0 )); then
    echo "WATCH-ERROR code=64 (${name} must be a positive integer, got '${value}')"
    exit 64
  fi
}

if [[ -z ${watch_url} ]]; then
  echo "WATCH-ERROR code=64 (watch-url is empty)"
  exit 64
fi
require_positive_int budget-seconds "${budget}"
require_positive_int poll-seconds "${poll}"
require_positive_int FLEET_WATCH_MAX_SECONDS "${max_seconds}"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
managed="${here}/fleet-watch-managed.sh"

if [[ ! -x "${managed}" ]]; then
  echo "WATCH-ERROR code=64 (${managed} not executable)"
  exit 64
fi

give_up_at=$(( $(date +%s) + max_seconds ))

give_up() {
  echo "WATCH-GAVE-UP after ${max_seconds}s with no terminal event"
  exit 75
}

# Run the managed watcher and kill it if it is still going at `give_up_at`.
#
# Shrinking the budget argument is not enough on its own. The managed watcher
# checks its own deadline between polls, so after that check it can still spend
# a `curl --max-time 25` and then a full `sleep ${poll}`, overrunning by most of
# a poll interval; with a 60 s poll that is a minute past a cap the caller set.
# Worse, an event arriving in that overrun would be reported as TERMINAL after
# the watch was supposed to be over.
#
# Killing the process group rather than the pid matters: the watcher's own time
# is spent inside `curl` and `sleep` children, and signalling only the wrapper
# leaves those running. `setsid` is not on macOS, so the child is put in its own
# group with `set -m` and signalled by negated pgid.
run_bounded() {
  local budget_arg="$1" out_file="$2" child code=0
  set -m
  "${managed}" "${watch_url}" "${budget_arg}" "${poll}" > "${out_file}" &
  child=$!
  set +m
  while kill -0 "${child}" 2>/dev/null; do
    if (( $(date +%s) >= give_up_at )); then
      kill -TERM -"${child}" 2>/dev/null || kill -TERM "${child}" 2>/dev/null || true
      wait "${child}" 2>/dev/null || true
      return 75
    fi
    sleep 1
  done
  wait "${child}" || code=$?
  return "${code}"
}

# An explicit XXXXXX template rather than `mktemp -t fleet-watch-loop`: BSD
# mktemp treats the argument as a prefix, GNU mktemp requires the Xs and fails
# with "too few X's in template", which under `set -e` exits before the loop
# ever starts. The bug is invisible on macOS and fatal on a Linux runner.
out_file="$(mktemp "${TMPDIR:-/tmp}/fleet-watch-loop.XXXXXX")"
trap 'rm -f "${out_file}"' EXIT

while (( $(date +%s) < give_up_at )); do
  remaining=$(( give_up_at - $(date +%s) ))
  run_budget=$(( budget < remaining ? budget : remaining ))
  run_bounded "${run_budget}" "${out_file}" && code=0 || code=$?
  if (( code == 0 )); then
    # A terminal event, but only one that arrived inside the cap: run_bounded
    # returns 75 rather than 0 when it had to kill the child.
    echo "TERMINAL $(cat "${out_file}")"
    exit 0
  fi
  if (( code != 75 )); then
    echo "WATCH-ERROR code=${code}"
    exit "${code}"
  fi
done

give_up
