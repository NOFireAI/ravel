#!/usr/bin/env bash
# Cases for scripts/guards/disk-watchdog.sh.
#
# Every case that involves killing runs against a process this file started
# itself, never against whatever happens to be building on the box: testing
# the kill path for real against live processes is the mistake that produced
# this script's scoping rules in the first place.
#
# Exit 0 all pass, 1 on a failure.
set -uo pipefail

WATCHDOG="$(cd "$(dirname "$0")" && pwd)/disk-watchdog.sh"
[[ -x "${WATCHDOG}" ]] || { echo "missing or not executable: ${WATCHDOG}" >&2; exit 64; }

passes=0
fails=0

check_eq() {
  local name="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${name}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %s:\n  want: %s\n  got:  %s\n' "${name}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

check_contains() {
  local name="$1" needle="$2" hay="$3"
  if [[ "${hay}" == *"${needle}"* ]]; then
    printf 'ok    %s\n' "${name}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %s:\n  expected to contain: %s\n  got: %s\n' "${name}" "${needle}" "${hay}"
    fails=$((fails + 1))
  fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

# A stand-in for a build: a process named `cargo` whose cwd is inside the
# scope directory. The name has to be the basename of an executable, since
# the watchdog reads `ps comm`, and the cwd has to be real, since it reads
# lsof. A SYMLINK to /bin/sh named cargo satisfies both: `ps comm` reports
# the symlink's own path, so the basename is `cargo`. A COPY of /bin/sh does
# not work on macOS, where the copy fails code-signing checks and dies
# silently the moment it is executed.
SCOPE="${TMP}/scope"
mkdir -p "${SCOPE}/bin"
ln -s /bin/sh "${SCOPE}/bin/cargo"
# stdout and stderr are redirected, not inherited. A background process that
# keeps the command substitution's pipe open makes `$(start_fake_build)` block
# until that process exits, which turns every call into a hang rather than a
# pid.
start_fake_build() {
  ( cd "${SCOPE}" && exec "${SCOPE}/bin/cargo" -c 'while :; do sleep 1; done' ) \
    >/dev/null 2>&1 &
  echo $!
}

# --- the scope argument is mandatory -------------------------------------
#
# A watchdog invoked with no scope must refuse rather than default to "every
# build on this machine", which is the unscoped version this script exists to
# replace.
out="$("${WATCHDOG}" 2>&1)"; rc=$?
check_eq "no scope argument: refuses" "1" "$((rc == 0 ? 0 : 1))"
check_contains "no scope argument: says what it wanted" "usage" "${out}"

# --- out of scope is left alone ------------------------------------------
#
# The floor is impossible, so the watchdog is firing; the scope names a
# directory with no builds under it. It must report finding nothing rather
# than fall back to matching by name.
pid_a="$(start_fake_build)"
sleep 1
mkdir -p "${TMP}/no-builds-here"
out="$(WATCHDOG_DRY_RUN=1 "${WATCHDOG}" "${TMP}/no-builds-here" 999999 1000000 1 2>&1)"
check_contains "out of scope: reports no build under that path" "no cargo/rustc under" "${out}"
check_eq "out of scope: the process is still alive" "alive" \
  "$(kill -0 "${pid_a}" 2>/dev/null && echo alive || echo dead)"

# --- a scope that does not exist is refused -------------------------------
#
# Not cosmetic. `lsof` reports a cwd with symlinks resolved, so a scope that
# cannot be resolved to a physical path would prefix-match nothing and the
# watchdog would sample forever, killing nothing, while looking armed.
out="$("${WATCHDOG}" "${TMP}/never-created" 999999 1000000 1 2>&1)"; rc=$?
check_eq "missing scope: refuses rather than matching nothing" "2" "${rc}"
check_contains "missing scope: names the path it could not resolve" "does not exist" "${out}"

# --- a scope given through a symlinked path still matches -----------------
#
# The defect this pins: on macOS /tmp and /var are symlinks, `mktemp -d`
# hands back the symlinked form, and lsof answers with the physical one. A
# watchdog comparing those two directly matches nothing, which is silent and
# looks exactly like a healthy idle watchdog.
sym_out="$(WATCHDOG_DRY_RUN=1 "${WATCHDOG}" "${SCOPE}" 999999 1000000 1 2>&1)"
check_contains "symlinked scope path: still finds the build under it" "would kill" "${sym_out}"

# --- dry run names the pid and kills nothing ------------------------------
out="$(WATCHDOG_DRY_RUN=1 "${WATCHDOG}" "${SCOPE}" 999999 1000000 1 2>&1)"
check_contains "dry run: names the scoped pid" "${pid_a}" "${out}"
check_contains "dry run: says it is a dry run" "DRY RUN" "${out}"
check_eq "dry run: the process is still alive" "alive" \
  "$(kill -0 "${pid_a}" 2>/dev/null && echo alive || echo dead)"
check_eq "dry run: writes no marker" "absent" \
  "$([[ -e "${SCOPE}/.disk-watchdog-fired" ]] && echo present || echo absent)"

# --- a stale marker is cleared when the watchdog arms ---------------------
#
# A marker left by an earlier firing would label a later, valid gate as
# invalid. Ignored markers are worse than no markers, so arming truncates.
echo "fired_at=1999-01-01T00:00:00Z" > "${SCOPE}/.disk-watchdog-fired"
WATCHDOG_DRY_RUN=1 "${WATCHDOG}" "${SCOPE}" 999999 1000000 1 >/dev/null 2>&1
check_eq "arming clears a marker from an earlier firing" "absent" \
  "$([[ -e "${SCOPE}/.disk-watchdog-fired" ]] && echo present || echo absent)"

# --- firing kills the scoped build and leaves the marker ------------------
out="$("${WATCHDOG}" "${SCOPE}" 999999 1000000 1 2>&1)"
sleep 1
check_eq "firing: the scoped process is dead" "dead" \
  "$(kill -0 "${pid_a}" 2>/dev/null && echo alive || echo dead)"
check_eq "firing: writes the marker" "present" \
  "$([[ -e "${SCOPE}/.disk-watchdog-fired" ]] && echo present || echo absent)"

marker="$(cat "${SCOPE}/.disk-watchdog-fired" 2>/dev/null)"
check_contains "marker: names the pid it killed" "${pid_a}" "${marker}"
check_contains "marker: carries the firing time" "fired_at=" "${marker}"
check_contains "marker: carries the free-space figure" "free_gb=" "${marker}"
# The reason the marker exists at all: a runner reports a SIGTERM'd test as a
# failure, and the next reader cannot tell that from a real one.
check_contains "marker: says an overlapping gate is invalid, not red" "INVALID" "${marker}"

# --- a build outside the scope survives a real firing ---------------------
#
# The case the scoping exists for: another session's build, running while
# this watchdog fires on its own scope, must be untouched.
OTHER="${TMP}/other-session"
mkdir -p "${OTHER}/bin"
ln -s /bin/sh "${OTHER}/bin/cargo"
( cd "${OTHER}" && exec "${OTHER}/bin/cargo" -c 'while :; do sleep 1; done' ) \
  >/dev/null 2>&1 &
pid_other=$!
pid_b="$(start_fake_build)"
sleep 1
"${WATCHDOG}" "${SCOPE}" 999999 1000000 1 >/dev/null 2>&1
sleep 1
check_eq "firing: kills the build inside the scope" "dead" \
  "$(kill -0 "${pid_b}" 2>/dev/null && echo alive || echo dead)"
check_eq "firing: leaves another session's build alone" "alive" \
  "$(kill -0 "${pid_other}" 2>/dev/null && echo alive || echo dead)"
kill -KILL "${pid_other}" 2>/dev/null || true

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
