#!/usr/bin/env bash
# Cases for check-disk-headroom.sh. Run: bash scripts/guards/check-disk-headroom.test.sh
set -uo pipefail

# Every case here pins an unreachable floor, so every case takes the
# below-floor branch, and that branch runs `disk-reap.sh -y` FOR REAL when
# FLEET_DISK_REAP=1 is set in the environment. A session with that variable
# exported reaps its peers' worktrees by running the tests, silently: the
# guard's message goes to stderr inside a command substitution and the suite
# still reports green. Neutralised for the whole file.
FLEET_DISK_REAP=0
export FLEET_DISK_REAP

GUARD="$(cd "$(dirname "$0")" && pwd)/check-disk-headroom.sh"
passes=0
fails=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${label}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %-52s want %s, got %s\n' "${label}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

check_contains() {
  local label="$1" needle="$2" hay="$3"
  if [[ "${hay}" == *"${needle}"* ]]; then
    printf 'ok    %s\n' "${label}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %-52s missing %q in %q\n' "${label}" "${needle}" "${hay}"
    fails=$((fails + 1))
  fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

# --- an empty argument is a caller's variable that did not survive ---------
#
# `${1:-.}` substitutes the default for an argument that is present and
# empty as readily as for one that is absent, so the guard answered about
# the current directory and exited 0 about a volume the caller was never
# going to write to. The shape that produced it is a shell variable set in
# one tool call and read in the next, where each call is its own shell.
out="$("${GUARD}" "" 5 2>&1)"; rc=$?
check_eq "an empty directory argument is refused" "2" "${rc}"
check_contains "it says the argument was empty" "empty directory argument" "${out}"

# The default still applies when no argument is passed at all, which is a
# different thing and has to keep working.
# Differenced against an explicit `.` rather than asserting a bare exit code:
# free space on the host is unknown, so the old form collapsed everything that
# was not 2 into 0 and asserted only "not refused". Deleting the default
# outright left it green.
out_default="$("${GUARD}" 2>&1)"; rc_default=$?
out_explicit="$("${GUARD}" . 20 2>&1)"; rc_explicit=$?
check_eq "no argument matches an explicit ." "${rc_explicit}" "${rc_default}"
check_eq "...and reports the same thing" "${out_explicit}" "${out_default}"

# --- an empty min_gb is refused too ---------------------------------------
#
# The directory argument was fixed and the floor was left with the identical
# hole, which is half a defect class. `${2:-20}` cannot tell an absent floor
# from a caller whose variable did not survive.
out="$("${GUARD}" . "" 2>&1)"; rc=$?
check_eq "an empty min_gb is refused" "2" "${rc}"
check_contains "it says which argument was empty" "min_gb" "${out}"

# --- a path that does not exist is refused --------------------------------
out="$("${GUARD}" "${TMP}/nope" 5 2>&1)"; rc=$?
check_eq "a missing path is refused" "1" "${rc}"
check_contains "it names the path it could not find" "does not exist" "${out}"

# --- a real directory below the floor fails, above it passes --------------
#
# The floor is compared against real df output, so pin both directions with
# floors that cannot be anything else on any host: nothing has 0 GB free by
# this guard's own reading, and nothing has 10^9 GB.
"${GUARD}" "${TMP}" 0 >/dev/null 2>&1
check_eq "a readable volume reaches the floor comparison" "0" "$?"

out="$("${GUARD}" "${TMP}" 1000000000 2>&1)"; rc=$?
check_eq "an unreachable floor fails" "1" "${rc}"
check_contains "it reports the shortfall" "free" "${out}"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
