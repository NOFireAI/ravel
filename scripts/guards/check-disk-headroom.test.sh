#!/usr/bin/env bash
# Cases for check-disk-headroom.sh. Run: bash scripts/guards/check-disk-headroom.test.sh
set -uo pipefail

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
"${GUARD}" >/dev/null 2>&1; rc=$?
check_eq "no argument at all still uses the default" "0" "$((rc == 2 ? 2 : 0))"

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
check_eq "a healthy volume passes a floor of zero" "0" "$?"

out="$("${GUARD}" "${TMP}" 1000000000 2>&1)"; rc=$?
check_eq "an unreachable floor fails" "1" "${rc}"
check_contains "it reports the shortfall" "free" "${out}"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
