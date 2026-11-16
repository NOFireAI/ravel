#!/usr/bin/env bash
# Cases for strip-attribution-trailers.sh. Run:
#   bash scripts/guards/strip-attribution-trailers.test.sh
#
# The cases that matter build a throwaway repository, commit a message that
# went through the script, and run assert-clean-authorship.sh over it: the
# script exists so that the guard passes, so the guard is the oracle. No
# network access needed.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STRIP="${HERE}/strip-attribution-trailers.sh"
GUARD="${HERE}/assert-clean-authorship.sh"
MERGE="${HERE}/../fleet-result-merge.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/strip-attribution-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

passes=0
fails=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${label}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %-56s want %q, got %q\n' "${label}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

EMAIL="author@example.test"
NAME="Test Author"

# A repository with origin/main and one feature commit carrying <message>,
# authored and signed off by the expected identity. Prints the repo path.
repo_with_message() {
  local dir="$1" message="$2"
  mkdir -p "${dir}"
  (
    cd "${dir}" || exit 1
    git init -q -b main .
    git config user.email "${EMAIL}"
    git config user.name "${NAME}"
    git commit -q --allow-empty -s -m "chore: base"
    git update-ref refs/remotes/origin/main HEAD
    printf 'x\n' > f
    git add f
    printf '%s\n' "${message}" > .msg
    git commit -q -s -F .msg
  ) >/dev/null 2>&1
  printf '%s\n' "${dir}"
}

guard_exit() {
  local dir="$1" rc=0
  (cd "${dir}" && SKIP_FETCH=1 bash "${GUARD}" HEAD "${EMAIL}") >/dev/null 2>&1 || rc=$?
  printf '%s\n' "${rc}"
}

BODY='fix(x): do the thing

It was broken, and now it is not.

Refs: #1'

# The four lines the guard refuses, each as an executor writes it.
declare -a REFUSED=(
  'Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>'
  'co-authored-by: Someone <bot@anthropic.com>'
  '🤖 Generated with [Claude Code](https://claude.com/claude-code)'
  'Generated with a tool'
  'Claude-Session: https://example.test/session_0123'
)

# 1. The oracle really refuses each line, so a pass below means something.
i=0
for line in "${REFUSED[@]}"; do
  i=$((i + 1))
  d="$(repo_with_message "${TMP}/raw-${i}" "${BODY}"$'\n\n'"${line}")"
  check_eq "guard refuses unstripped line ${i}" "1" "$(guard_exit "${d}")"
done

# 2. After the script, the guard passes and the rest of the message is intact.
i=0
for line in "${REFUSED[@]}"; do
  i=$((i + 1))
  stripped="$(printf '%s\n\n%s\n' "${BODY}" "${line}" | bash "${STRIP}")"
  d="$(repo_with_message "${TMP}/stripped-${i}" "${stripped}")"
  check_eq "guard passes stripped line ${i}" "0" "$(guard_exit "${d}")"
  got="$(cd "${d}" && git log -1 --format=%B | grep -c -F 'Refs: #1')"
  check_eq "Refs trailer survives line ${i}" "1" "${got}"
  got="$(cd "${d}" && git log -1 --format=%s)"
  check_eq "subject survives line ${i}" "fix(x): do the thing" "${got}"
done

# 3. Several refused lines at once, mixed with a kept trailer.
all="$(printf '%s\n' "${REFUSED[@]}")"
stripped="$(printf '%s\n\n%s\n' "${BODY}" "${all}" | bash "${STRIP}")"
d="$(repo_with_message "${TMP}/stripped-all" "${stripped}")"
check_eq "guard passes with every refused line stripped" "0" "$(guard_exit "${d}")"

# 4. A human co-author is not attribution and is kept.
human='Co-Authored-By: Ada Lovelace <ada@example.test>'
stripped="$(printf '%s\n%s\n' "${BODY}" "${human}" | bash "${STRIP}")"
check_eq "human co-author is kept" "1" "$(printf '%s\n' "${stripped}" | grep -c -F "${human}")"

# 5. A message with nothing to strip comes back byte for byte.
check_eq "clean message unchanged" "${BODY}" "$(printf '%s\n' "${BODY}" | bash "${STRIP}")"

# 6. Prose that mentions the words mid-line is not a trailer and is kept.
prose='The helper is not generated with a script; see Claude-Session notes.'
check_eq "mid-line mention is kept" "1" \
  "$(printf '%s\n\n%s\n' "${BODY}" "${prose}" | bash "${STRIP}" | grep -c -F "${prose}")"

# 7. Empty input: exit 0 and no output.
rc=0
out="$(printf '' | bash "${STRIP}")" || rc=$?
check_eq "empty input exits 0" "0" "${rc}"
check_eq "empty input prints nothing" "" "${out}"

# 8. The two patterns are the same text, so a line added to one is added to
#    the other.
guard_pattern="$(grep -o "grep -iE '[^']*'" "${GUARD}" | head -1 | sed "s/^grep -iE '//; s/'\$//")"
strip_pattern="$(grep -E "^PATTERN='" "${STRIP}" | sed "s/^PATTERN='//; s/'\$//")"
check_eq "guard and script share one pattern" "${guard_pattern}" "${strip_pattern}"

# 9. The merge script calls the script in its re-authoring step. Without the
#    call, a result branch with an attribution line fails at the guard after
#    all of the rewriting is done.
#    It reads the script once, up front, and pipes each message through that
#    copy: the pass runs with a result branch's base checked out, which may
#    predate the file.
check_eq "fleet-result-merge.sh loads the script before rewriting" "1" \
  "$(grep -c '^strip_attribution_src="$(cat "${script_dir}/guards/strip-attribution-trailers.sh")"$' "${MERGE}")"
check_eq "fleet-result-merge.sh pipes each message through it" "1" \
  "$(grep -c '^    | bash -c "${strip_attribution_src}" \\$' "${MERGE}")"

# 10. That pipeline, as the merge script runs it, on a message with an
#     executor sign-off and an attribution line: both go, Refs stays, and the
#     guard passes once the merger's sign-off is added.
executor_msg="${BODY}"$'\n\n''Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>'$'\n''Signed-off-by: Ravel Fleet Executor <fleet-executor@nofire.ai>'
src="$(cat "${STRIP}")"
rewritten="$(printf '%s\n' "${executor_msg}" \
  | bash -c "${src}" \
  | { grep -viE '^Signed-off-by:[[:space:]]+.*<fleet-executor@nofire\.ai>[[:space:]]*$' || true; })"
d="$(repo_with_message "${TMP}/pipeline" "${rewritten}")"
check_eq "merge pipeline output passes the guard" "0" "$(guard_exit "${d}")"
check_eq "merge pipeline drops the attribution line" "0" \
  "$(cd "${d}" && git log -1 --format=%B | grep -c -i 'co-authored-by')"
check_eq "merge pipeline drops the executor sign-off" "0" \
  "$(cd "${d}" && git log -1 --format=%B | grep -c -F 'fleet-executor@nofire.ai')"
check_eq "merge pipeline keeps Refs as a trailer" "#1" \
  "$(cd "${d}" && git log -1 --format='%(trailers:key=Refs,valueonly)' | tr -d '\n')"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ ${fails} -eq 0 ]]
