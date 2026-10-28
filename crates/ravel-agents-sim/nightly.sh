#!/usr/bin/env bash
# Run every ravel-agents-sim seed batch over RAVEL_SIM_SEED (first seed,
# decimal) and RAVEL_SIM_SEEDS (seed count), and print one line per schedule.
#
# Exits non-zero when the crate's tests fail (an invariant broke on some seed,
# or a pinned total moved on the default batch), or when any schedule below
# printed its line other than exactly once. Both variables unset is the
# default batch, the one the pinned totals were measured on; any other batch
# prints "unpinned" and is checked by the per-seed invariants alone.
#
# Usage: crates/ravel-agents-sim/nightly.sh [log-file]
# With GITHUB_STEP_SUMMARY set, the lines are also appended there.
set -uo pipefail

SCHEDULES=(
  enqueue_racing_completion
  cancel_racing_completion
  contended_refusals
  competing_senders
  duplicate_and_conflict
  lost_cas_response
  worker_kills
  suspended_worker
  cold_restart
  provider_idempotency_kills
  at_least_once_kills
  manual_kills
  cancels
  conflicting_reporters
  handler_version_changes
  fire_cancel_race
  duplicate_hint_delivery
  lost_hint_puts
  crashes_around_the_hint_write
  recovery_storms
)

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
log="${1:-$root/.gate-logs/sim-nightly.log}"
mkdir -p "$(dirname "$log")"

echo "seeds: RAVEL_SIM_SEED=${RAVEL_SIM_SEED:-<default>} RAVEL_SIM_SEEDS=${RAVEL_SIM_SEEDS:-<default>}"
code=0
(cd "$root" && cargo test --locked -p ravel-agents-sim -- --nocapture) >"$log" 2>&1 || code=$?

lines="$(grep -E '^sim-batch ' "$log" || true)"
missing=0
for s in "${SCHEDULES[@]}"; do
  n="$(grep -cE "^sim-batch $s " <<<"$lines" || true)"
  if [[ "$n" != 1 ]]; then
    echo "MISSING: schedule $s printed $n lines, expected exactly 1" >&2
    missing=1
  fi
done
extra=0
if [[ -n "$lines" ]]; then
  extra="$(grep -vcE "^sim-batch ($(IFS='|'; echo "${SCHEDULES[*]}")) " <<<"$lines" || true)"
fi
if [[ "$extra" != 0 ]]; then
  echo "UNKNOWN: $extra sim-batch lines name no listed schedule" >&2
  missing=1
fi

echo "$lines"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### ravel-agents-sim seed batches"
    echo
    echo "RAVEL_SIM_SEED=${RAVEL_SIM_SEED:-default}, RAVEL_SIM_SEEDS=${RAVEL_SIM_SEEDS:-default}, cargo test exit $code"
    echo
    echo '```'
    echo "$lines"
    echo '```'
  } >>"$GITHUB_STEP_SUMMARY"
fi

if [[ "$code" != 0 ]]; then
  echo "FAILED: cargo test exited $code; see $log" >&2
  grep -E "^(---- |thread '|replay: )" "$log" >&2 || true
  exit 1
fi
if [[ "$missing" != 0 ]]; then
  exit 1
fi
echo "all ${#SCHEDULES[@]} schedules ran once each"
