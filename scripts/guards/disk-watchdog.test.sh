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
# Backgrounded, because below the floor with nothing of ours running the
# watchdog keeps sampling rather than exiting; see the no-disarm case below
# for why. A foreground run here would hang the suite.
"${WATCHDOG}" "${TMP}/no-builds-here" 999999 1000000 1 >"${TMP}/oos.out" 2>&1 &
pid_oos=$!
sleep 3
kill -KILL "${pid_oos}" 2>/dev/null || true
check_contains "out of scope: reports no build under that path" "no cargo/rustc under" \
  "$(cat "${TMP}/oos.out" 2>/dev/null)"
check_eq "out of scope: the build outside it is untouched" "alive" \
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
# Armed for real, with a floor of 0 so it watches without ever firing: an
# earlier version of this proved the clearing with a DRY RUN, which asserted
# the behaviour the dry run must not have.
echo "fired_at=1999-01-01T00:00:00Z" > "${SCOPE}/.disk-watchdog-fired"
"${WATCHDOG}" "${SCOPE}" 0 0 1 >/dev/null 2>&1 &
arm_pid=$!
sleep 1
check_eq "arming clears a marker from an earlier firing" "absent" \
  "$([[ -e "${SCOPE}/.disk-watchdog-fired" ]] && echo present || echo absent)"
kill "${arm_pid}" 2>/dev/null || true
wait "${arm_pid}" 2>/dev/null || true

# --- a DRY RUN preserves a marker it did not write ------------------------
#
# The dry run is the documented way to check the matcher, and the situation
# that prompts it is a gate that has just come back red. Clearing the marker
# there answers the question by destroying the evidence: the run that fired is
# gone, and the gate reads as a genuine failure.
echo "fired_at=1999-01-01T00:00:00Z" > "${SCOPE}/.disk-watchdog-fired"
WATCHDOG_DRY_RUN=1 "${WATCHDOG}" "${SCOPE}" 999999 1000000 1 >/dev/null 2>&1
check_eq "a dry run leaves an existing marker in place" "present" \
  "$([[ -e "${SCOPE}/.disk-watchdog-fired" ]] && echo present || echo absent)"
rm -f "${SCOPE}/.disk-watchdog-fired"

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

# --- a sibling whose name starts with the scope is NOT in scope -----------
#
# The defect this pins, reproduced on the first version of this script: a
# bare "${SCOPE}"* prefix match also matches a sibling directory whose name
# merely begins with the same string, so a scope of .../wt named a build
# running in .../wt2 for killing. That is another session's work, and it is
# precisely what the scoping exists to prevent. Every other case here puts
# its build at the scope root or under it, so none of them can see this.
SIB="${TMP}/scope2"
mkdir -p "${SIB}/bin"
ln -s /bin/sh "${SIB}/bin/cargo"
( cd "${SIB}" && exec "${SIB}/bin/cargo" -c 'while :; do sleep 1; done' ) \
  >/dev/null 2>&1 &
pid_sibling=$!
sleep 1
out="$(WATCHDOG_DRY_RUN=1 "${WATCHDOG}" "${SCOPE}" 999999 1000000 1 2>&1)"
check_eq "a name-prefix sibling of the scope is not matched" "0" \
  "$(printf '%s' "${out}" | grep -c "${pid_sibling}")"
kill -KILL "${pid_sibling}" 2>/dev/null || true

# --- a build in a SUBDIRECTORY of the scope IS in scope --------------------
#
# The obvious over-correction to the case above is an exact match, which
# passes every other case here while silently narrowing the watchdog to
# processes sitting at the worktree root. A real build's cwd is a crate
# directory, so this is the normal case, not the exotic one.
SUB="${SCOPE}/crates/ravel-sql"
mkdir -p "${SUB}"
( cd "${SUB}" && exec "${SCOPE}/bin/cargo" -c 'while :; do sleep 1; done' ) \
  >/dev/null 2>&1 &
pid_sub=$!
sleep 1
out="$(WATCHDOG_DRY_RUN=1 "${WATCHDOG}" "${SCOPE}" 999999 1000000 1 2>&1)"
check_eq "a build in a subdirectory of the scope is matched" "1" \
  "$(printf '%s' "${out}" | grep -c "${pid_sub}")"
kill -KILL "${pid_sub}" 2>/dev/null || true

# --- below the floor with no build yet: keep watching, do not disarm -------
#
# Arming happens alongside a gate that has not spawned cargo yet, and
# gates.sh runs its lanes as separate processes, so a sample can legitimately
# find nothing of ours while the volume is already low. Exiting there would
# disarm the watchdog at the moment it is most needed, returning the same
# status a successful firing returns.
mkdir -p "${TMP}/empty-scope"
"${WATCHDOG}" "${TMP}/empty-scope" 999999 1000000 1 >"${TMP}/starve.out" 2>&1 &
pid_wd=$!
sleep 3
check_eq "below the floor with nothing of ours: still running" "alive" \
  "$(kill -0 "${pid_wd}" 2>/dev/null && echo alive || echo dead)"
check_contains "below the floor with nothing of ours: says it is still watching" \
  "still watching" "$(cat "${TMP}/starve.out" 2>/dev/null)"
kill -KILL "${pid_wd}" 2>/dev/null || true

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

# --- the dry run works ABOVE the floor, which is the only time it is used ---
#
# The dry run is the safe check CLAUDE.md tells sessions to use instead of
# testing the kill path against live processes, so it is run on a healthy
# volume. Shipped inside the floor branch it produced no output and never
# returned, and no case saw it because every dry-run case above passes an
# impossible floor. Default floor here, real free space.
pid_dry="$(start_fake_build)"
sleep 1
dry_out=""
if dry_out="$(WATCHDOG_DRY_RUN=1 timeout 10 "${WATCHDOG}" "${SCOPE}" 2>&1)"; then dry_rc=0; else dry_rc=$?; fi
check_eq "dry run above the floor returns instead of looping" "0" "${dry_rc}"
check_contains "dry run above the floor names the build it would kill" "${pid_dry}" "${dry_out}"
check_eq "dry run above the floor kills nothing" "alive" \
  "$(kill -0 "${pid_dry}" 2>/dev/null && echo alive || echo dead)"
kill -KILL "${pid_dry}" 2>/dev/null || true

# --- the free-space reader is real, not a constant ------------------------
#
# Nothing asserted on the parsed figure, so replacing free_gb's body with a
# constant left every case green while turning the shipped watchdog into an
# unconditional killer at the default floor. Pin the number against df.
want_gb="$(df -Pk "${SCOPE}" | awk 'NR==2 {print int($4/1048576)}')"
got_line="$(WATCHDOG_DRY_RUN=1 timeout 10 "${WATCHDOG}" "${SCOPE}" 2>&1)"
check_contains "the reported free space matches df on the scope" "${want_gb} GB left" "${got_line}"

# --- the marker is written BEFORE the kill --------------------------------
#
# Both the commit message and CLAUDE.md assert this ordering, and every other
# case observes the marker only after the target is already dead, so moving
# the marker below the SIGKILL left them all green. Here the victim ignores
# SIGTERM, so it survives into the five-second window between the TERM and the
# KILL, and records whether the marker exists while it is still alive. With
# the marker written first it sees it; with the marker written after the kill
# it is dead before the marker exists and never can.
ORD="${TMP}/ordering"
mkdir -p "${ORD}/bin"
ln -s /bin/sh "${ORD}/bin/cargo"
ORD_MARKER="${ORD}/.disk-watchdog-fired"
ORD_SEEN="${ORD}/seen"
: > "${ORD_SEEN}"
( cd "${ORD}" && exec "${ORD}/bin/cargo" -c "trap '' TERM; while :; do if [ -e '${ORD_MARKER}' ]; then echo yes >> '${ORD_SEEN}'; fi; sleep 1; done" ) \
  >/dev/null 2>&1 &
pid_ord=$!
sleep 1
"${WATCHDOG}" "${ORD}" 999999 1000000 1 >/dev/null 2>&1
check_eq "the marker exists while the build is still alive" "yes" \
  "$(head -1 "${ORD_SEEN}" 2>/dev/null)"
kill -KILL "${pid_ord}" 2>/dev/null || true

# --- the linker children are really in the match set -----------------------
#
# The matcher names cc, ld, collect2, rust-lld, clang and clang++ alongside
# cargo and rustc, because SIGKILL to rustc leaves the linker it spawned
# running and the link is the phase writing the largest artifacts. Every case
# above builds its fixture as `cargo`, so deleting all six linker names from
# the matcher left the whole suite green and the shipped watchdog reporting
# success while the volume kept draining. One fixture per name.
LINKERS="${TMP}/linkers"
mkdir -p "${LINKERS}/bin"
for tool in cc ld collect2 rust-lld clang clang++; do
  ln -s /bin/sh "${LINKERS}/bin/${tool}"
  ( cd "${LINKERS}" && exec "${LINKERS}/bin/${tool}" -c 'while :; do sleep 1; done' ) \
    >/dev/null 2>&1 &
  eval "pid_${tool//[!a-z0-9]/_}=$!"
done
sleep 1
link_out="$(WATCHDOG_DRY_RUN=1 timeout 10 "${WATCHDOG}" "${LINKERS}" 2>&1)"
for tool in cc ld collect2 rust-lld clang clang++; do
  eval "tool_pid=\${pid_${tool//[!a-z0-9]/_}}"
  check_contains "a ${tool} under the scope is in the match set" "${tool_pid}" "${link_out}"
  kill -KILL "${tool_pid}" 2>/dev/null || true
done

# --- the post-kill re-scan catches a child the kill left behind ------------
#
# A process can be spawned between the scan and the kill, and a linker child
# outlives the rustc that was matched, so the watchdog re-scans and kills
# again until the scoped set is empty. Nothing asserted that: deleting the
# whole re-scan loop left every case green, because in all of them the single
# fixture dies to the first SIGKILL. Here the victim spawns a cc child from
# its TERM handler and then exits, so the child exists only AFTER the first
# scan and only the re-scan can find it.
RESCAN="${TMP}/rescan"
mkdir -p "${RESCAN}/bin"
ln -s /bin/sh "${RESCAN}/bin/cargo"
ln -s /bin/sh "${RESCAN}/bin/cc"
cat > "${RESCAN}/spawn-on-term.sh" <<'EOF'
on_term() {
  "${RESCAN_BIN}/cc" -c 'while :; do sleep 1; done' >/dev/null 2>&1 &
  echo "$!" > "${RESCAN_CHILD}"
  exit 0
}
trap on_term TERM
while :; do sleep 1; done
EOF
RESCAN_CHILD="${RESCAN}/child.pid"
: > "${RESCAN_CHILD}"
( cd "${RESCAN}" && RESCAN_BIN="${RESCAN}/bin" RESCAN_CHILD="${RESCAN_CHILD}" \
    exec "${RESCAN}/bin/cargo" "${RESCAN}/spawn-on-term.sh" ) >/dev/null 2>&1 &
pid_rescan=$!
sleep 1
rescan_out="$(timeout 60 "${WATCHDOG}" "${RESCAN}" 999999 1000000 1 2>&1)"; rescan_rc=$?
child_pid="$(cat "${RESCAN_CHILD}" 2>/dev/null)"
check_contains "the re-scan reports the child left behind" "still running under" "${rescan_out}"
check_contains "the re-scan names the child's pid" "${child_pid}" "${rescan_out}"
check_eq "the orphaned linker child is dead" "dead" \
  "$(kill -0 "${child_pid}" 2>/dev/null && echo alive || echo dead)"
check_eq "the watchdog still succeeds once the set is empty" "0" "${rescan_rc}"
kill -KILL "${pid_rescan}" "${child_pid}" 2>/dev/null || true

# --- giving up is reported, not reported as success ------------------------
#
# After five rounds the watchdog stops and exits 1 rather than claiming a
# volume it did not free. CLAUDE.md states that; nothing reached it, so
# deleting the give-up branch and letting the loop fall through to the success
# message left every case green. A keeper OUTSIDE the scope respawns a `cc`
# inside it whenever one dies, so the scoped set is never empty. The keeper is
# named `sh`, which the matcher ignores, and it is bounded to 20 spawns so a
# failing test cannot leave a respawn loop running on a shared box.
GIVEUP="${TMP}/giveup"
mkdir -p "${GIVEUP}/bin"
ln -s /bin/sh "${GIVEUP}/bin/cc"
cat > "${TMP}/keeper.sh" <<'EOF'
n=0
while [ "${n}" -lt 20 ]; do
  ( cd "${KEEP_DIR}" && exec "${KEEP_DIR}/bin/cc" -c 'while :; do sleep 1; done' ) \
    >/dev/null 2>&1
  n=$((n + 1))
done
EOF
KEEP_DIR="${GIVEUP}" sh "${TMP}/keeper.sh" >/dev/null 2>&1 &
keeper_pid=$!
sleep 1
giveup_out="$(timeout 90 "${WATCHDOG}" "${GIVEUP}" 999999 1000000 1 2>&1)"; giveup_rc=$?
kill -KILL "${keeper_pid}" 2>/dev/null || true
pkill -KILL -f "${GIVEUP}/bin/cc" 2>/dev/null || true
check_eq "giving up exits non-zero" "1" "${giveup_rc}"
check_contains "giving up says what is still running" "gave up" "${giveup_out}"
check_eq "giving up does not print the success line" "absent" \
  "$([[ "${giveup_out}" == *"killed; marker at"* ]] && echo present || echo absent)"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
