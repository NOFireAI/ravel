#!/usr/bin/env bash
# Cases the epic-status nudge must get right. Run:
#   bash .claude/guards/epic-status-nudge.test.sh
set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/epic-status-nudge.mjs"
pass=0
fail=0

tmpdir="$(mktemp -d)" || { echo "FAIL  could not create a temp dir for test state" >&2; exit 1; }
trap 'rm -rf "$tmpdir"' EXIT
tmpstate="$tmpdir/state.json"
export EPIC_STATUS_NUDGE_STATE="$tmpstate"

bash_payload() {
  node -e 'process.stdout.write(JSON.stringify({tool_name:"Bash",tool_input:{command:process.argv[1]}}))' "$1"
}

track() {
  printf '%s' "$(bash_payload "$1")" | node "$SCRIPT" track
}

nudge_output() {
  node "$SCRIPT" nudge
}

count_in_state() {
  node -e 'try{console.log(JSON.parse(require("fs").readFileSync(process.argv[1],"utf8")).count)}catch{console.log("MISSING")}' "$tmpstate"
}

check_count() {
  local want="$1" label="$2" got
  got="$(count_in_state)"
  if [ "$got" = "$want" ]; then
    pass=$((pass + 1))
  else
    printf 'FAIL  %-58s want count=%s, got %s\n' "$label" "$want" "$got"
    fail=$((fail + 1))
  fi
}

check_nudge() {
  local want="$1" label="$2" out
  out="$(nudge_output)"
  if [ "$want" = "silent" ]; then
    if [ -z "$out" ]; then
      pass=$((pass + 1))
    else
      printf 'FAIL  %-58s want silent, got: %s\n' "$label" "$out"
      fail=$((fail + 1))
    fi
  else
    if printf '%s' "$out" | grep -q "epic-status.sh"; then
      pass=$((pass + 1))
    else
      printf 'FAIL  %-58s want a reminder, got: %s\n' "$label" "$out"
      fail=$((fail + 1))
    fi
  fi
}

# --- fresh state ----------------------------------------------------------
rm -f "$tmpstate"
check_nudge silent "no state file yet"

# --- tracking counts only reconciliation-style gh reads --------------------
track 'gh issue view 519 --repo NOFireAI/ravel'
check_count 1 "gh issue view increments"
track 'gh pr view 583 --repo NOFireAI/ravel'
check_count 2 "gh pr view increments"
track 'gh issue list --repo NOFireAI/ravel --state open'
check_count 3 "gh issue list increments"
track 'gh issue create --repo NOFireAI/ravel --title x --body y'
check_count 3 "gh issue create does not count as reconciliation"
track 'gh pr merge 583 --repo NOFireAI/ravel --auto'
check_count 3 "gh pr merge does not count as reconciliation"
track 'cargo test -p ravel-cli'
check_count 3 "unrelated command does not count"

# --- matching is anchored to a command position, not a substring search ----
track 'git commit -m "explain why scripts/epic-status.sh is broken"'
check_count 3 "a commit message mentioning epic-status.sh does not reset"
track 'echo "run gh issue view instead"'
check_count 3 "a string merely mentioning gh issue view does not increment"
track 'cd /tmp && gh issue view 42 --repo NOFireAI/ravel'
check_count 4 "a gh call after a harmless cd prefix still counts"
track 'GH_TOKEN=x gh issue view 42 --repo NOFireAI/ravel'
check_count 5 "a gh call after an env-var prefix still counts"

# --- below threshold: silent -----------------------------------------------
check_nudge silent "count 5 is below threshold"

# --- cross threshold --------------------------------------------------------
for _ in 1 2 3; do track 'gh issue view 1 --repo NOFireAI/ravel'; done
check_count 8 "count reaches threshold"
check_nudge remind "count 8 at threshold reminds"
check_nudge silent "does not repeat the same reminder on the very next prompt"

# --- climbing further past a nudge reminds again ----------------------------
for _ in 1 2 3 4 5 6 7 8; do track 'gh issue view 1 --repo NOFireAI/ravel'; done
check_count 16 "count climbs past the first nudge"
check_nudge remind "reminds again after another threshold's worth of calls"

# --- epic-status.sh resets --------------------------------------------------
track 'scripts/epic-status.sh 597'
check_count 0 "epic-status.sh run resets the counter"
check_nudge silent "silent again right after a reset"

# --- epic-status.sh only counts as a real command, not a mention -----------
for _ in 1 2 3 4 5 6 7 8; do track 'gh issue view 1 --repo NOFireAI/ravel'; done
check_count 8 "count reaches threshold again after the reset"
check_nudge remind "reminds again after the reset and a fresh climb"
track './scripts/epic-status.sh 597'
check_count 0 "a ./-prefixed epic-status.sh run also resets"

# --- malformed input never breaks anything ----------------------------------
printf '' | node "$SCRIPT" track
printf 'not json' | node "$SCRIPT" track
printf '{"tool_name":"Bash","tool_input":{}}' | node "$SCRIPT" track
check_count 0 "malformed/empty track calls leave the counter untouched"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
[ "$fail" -eq 0 ]
