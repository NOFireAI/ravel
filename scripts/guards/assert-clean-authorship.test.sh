#!/usr/bin/env bash
# Cases for assert-clean-authorship.sh. Run:
#   bash scripts/guards/assert-clean-authorship.test.sh
#
# Each case builds a throwaway repository under $TMPDIR with origin/main and
# feature commits to test identity checks, sign-off enforcement, and AI trailer
# detection. No network access needed.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/assert-clean-authorship.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/assert-clean-authorship-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

passes=0
fails=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${label}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %-56s want %s, got %s\n' "${label}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

check_contains() {
  local label="$1" needle="$2" hay="$3"
  if [[ "${hay}" == *"${needle}"* ]]; then
    printf 'ok    %s\n' "${label}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %-56s missing %q in %q\n' "${label}" "${needle}" "${hay}"
    fails=$((fails + 1))
  fi
}

TEST_EMAIL="author@example.test"
TEST_NAME="Author Name"

new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}"
  git -C "${dir}" init -q -b main
  git -C "${dir}" config user.name "${TEST_NAME}"
  git -C "${dir}" config user.email "${TEST_EMAIL}"
  git -C "${dir}" config commit.gpgsign false

  printf 'seed\n' >"${dir}/README.md"
  git -C "${dir}" add README.md
  git -C "${dir}" commit -q -s -m "chore: seed repository"
  git -C "${dir}" update-ref refs/remotes/origin/main HEAD
  printf '%s\n' "${dir}"
}

# --- usage and configuration errors -------------------------------------------

out="$("${GUARD}" 2>&1)"; rc=$?
check_eq "no ref passed: exits 1" "1" "${rc}"
check_contains "no ref passed: prints usage" "usage:" "${out}"

d="$(new_repo no-email)"
git -C "${d}" checkout -q -b feat
git -C "${d}" config --unset user.email
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -m "feat: change" -s --author="${TEST_NAME} <${TEST_EMAIL}>"
mkdir -p "${TMP}/emptyhome"
out="$(cd "${d}" && HOME="${TMP}/emptyhome" GIT_CONFIG_GLOBAL=/dev/null "${GUARD}" feat 2>&1)"; rc=$?
check_eq "no expected email available: exits 1" "1" "${rc}"
check_contains "no expected email available: error message" "no expected email" "${out}"

# --- clean commits pass -------------------------------------------------------

d="$(new_repo clean)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -s -m "feat: clean commit"
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "clean commit passes" "0" "${rc}"
check_contains "clean commit reports clean count" "authorship clean on 1 commit(s)" "${out}"

# --- commit body prose with 'regenerated without' passes ----------------------
#
# Regression test: `Generated with` was unanchored and case-insensitive, so
# prose such as "the committed block is regenerated without Prometheus in the loop"
# matched `reGenerated witho...` as an AI attribution trailer.

d="$(new_repo prose-regenerated-without)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -s -m "fix: update metrics pipeline" \
  -m "the committed block is regenerated without Prometheus in the loop"
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "body with 'regenerated without' passes" "0" "${rc}"
check_contains "body with 'regenerated without' reports clean" "authorship clean on 1 commit(s)" "${out}"

d="$(new_repo prose-regenerated-with-flag)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -s -m "docs: regenerate schemas" \
  -m "Regenerated with the new flag after format version bump."
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "body with 'Regenerated with the new flag' passes" "0" "${rc}"

d="$(new_repo prose-generated-with-derive)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -s -m "refactor: traits" \
  -m "These structs are generated with the derive macro."
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "body with 'generated with the derive macro' passes" "0" "${rc}"

d="$(new_repo legitimate-co-author)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -s -m "feat: collaborate" \
  -m "Co-Authored-By: Colleague <colleague@example.test>"
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "human Co-Authored-By trailer passes" "0" "${rc}"

# --- AI attribution trailers are rejected ------------------------------------

test_ai_trailer() {
  local label="$1" trailer="$2"
  local d
  d="$(new_repo "ai-${label}")"
  git -C "${d}" checkout -q -b feat
  printf 'change\n' >>"${d}/README.md"
  git -C "${d}" add README.md
  git -C "${d}" commit -q -s -m "feat: ai assisted change" -m "${trailer}"
  local out rc=0
  out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)" || rc=$?
  check_eq "AI trailer [${label}] rejected: exit 1" "1" "${rc}"
  check_contains "AI trailer [${label}] reports attribution trailer" "carries an AI attribution trailer" "${out}"
}

test_ai_trailer "generated-with-claude" "Generated with Claude Code"
test_ai_trailer "generated-with-lower" "generated with claude code"
test_ai_trailer "robot-generated-with" "🤖 Generated with Claude Code"
test_ai_trailer "co-authored-claude" "Co-Authored-By: Claude <noreply@anthropic.com>"
test_ai_trailer "co-authored-anthropic" "Co-Authored-By: Anthropic <noreply@anthropic.com>"
test_ai_trailer "claude-session" "Claude-Session: 550e8400-e29b-41d4-a716-446655440000"

# --- identity and sign-off checks ---------------------------------------------

d="$(new_repo missing-signoff)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -m "feat: commit without signoff"
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "missing Signed-off-by: exits 1" "1" "${rc}"
check_contains "missing Signed-off-by error" "missing Signed-off-by" "${out}"

d="$(new_repo foreign-signoff)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -s -m "feat: foreign signoff" \
  -m "Signed-off-by: Foreign Identity <foreign@example.test>"
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "foreign Signed-off-by: exits 1" "1" "${rc}"
check_contains "foreign Signed-off-by error" "carries a foreign sign-off" "${out}"

d="$(new_repo wrong-author)"
git -C "${d}" checkout -q -b feat
printf 'change\n' >>"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -s --author="Other Author <other@example.test>" -m "feat: wrong author"
out="$(cd "${d}" && "${GUARD}" feat "${TEST_EMAIL}" 2>&1)"; rc=$?
check_eq "wrong author email: exits 1" "1" "${rc}"
check_contains "wrong author email error" "wrong author/committer" "${out}"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
