#!/usr/bin/env bash
# Cases for check-changelog-touched.sh, in the pattern of
# check-workflow-permissions.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway git repo under $TMPDIR, commits a base state,
# then one or more commits forming the range under test, and runs the guard
# directly against that repo (the guard itself needs no repo_root cd trick
# beyond `cd` to the fixture, since it only ever reads git history).
#
# Run: bash scripts/guards/check-changelog-touched.test.sh
set -uo pipefail

export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-changelog-touched.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-changelog-touched-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: an initialized repo with a base commit (CHANGELOG.md and
# an unrelated file already present) and the guard copied into
# scripts/guards/. Prints its path.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/crates/c" "${dir}/services/s"
  cp "${GUARD}" "${dir}/scripts/guards/check-changelog-touched.sh"
  git -C "${dir}" init -q -b main 2>/dev/null
  git -C "${dir}" config user.email t@example.com
  git -C "${dir}" config user.name Test
  printf '# Changelog\n\n## [Unreleased]\n' >"${dir}/CHANGELOG.md"
  printf 'seed\n' >"${dir}/README.md"
  git -C "${dir}" add -A
  git -C "${dir}" commit -q -m "chore: seed"
  printf '%s\n' "${dir}"
}

# check <name> <repo> <base> <head> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" base="$3" head="$4" want_rc="$5" want_sub="${6:-}"
  local out rc=0
  out="$(cd "${dir}" && bash scripts/guards/check-changelog-touched.sh "${base}" "${head}" 2>&1)" || rc=$?
  if [[ "${rc}" != "${want_rc}" ]]; then
    printf 'FAIL  %s: exit %s, wanted %s\n' "${name}" "${rc}" "${want_rc}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  if [[ -n "${want_sub}" && "${out}" != *"${want_sub}"* ]]; then
    printf 'FAIL  %s: output missing %s\n' "${name}" "${want_sub}"
    printf '%s\n' "${out}" | sed 's/^/      /'
    fails=$((fails + 1))
    return
  fi
  printf 'ok    %s\n' "${name}"
  passes=$((passes + 1))
}

# --- feat_commit_touching_crates_without_changelog_fails --------------------

d="$(new_repo feat-no-changelog)"
base="$(git -C "${d}" rev-parse HEAD)"
echo 'fn feature() {}' >"${d}/crates/c/feature.rs"
git -C "${d}" add crates/c/feature.rs
git -C "${d}" commit -q -m "feat(c): add feature"
head="$(git -C "${d}" rev-parse HEAD)"
check "feat_commit_touching_crates_without_changelog_fails" "${d}" "${base}" "${head}" 1 \
  "CHANGELOG.md is untouched"

# --- docs_only_fixture_passes ------------------------------------------------
# A docs-typed commit that happens to touch crates/ does not qualify: only
# feat/fix commit types are gated.

d="$(new_repo docs-only)"
base="$(git -C "${d}" rev-parse HEAD)"
echo 'fn doc_example() {}' >"${d}/crates/c/example.rs"
git -C "${d}" add crates/c/example.rs
git -C "${d}" commit -q -m "docs(c): document example"
head="$(git -C "${d}" rev-parse HEAD)"
check "docs_only_fixture_passes" "${d}" "${base}" "${head}" 0 "clean"

# --- changelog_none_trailer_passes -------------------------------------------

d="$(new_repo trailer)"
base="$(git -C "${d}" rev-parse HEAD)"
echo 'fn feature() {}' >"${d}/services/s/feature.rs"
git -C "${d}" add services/s/feature.rs
git -C "${d}" commit -q -m "fix(s): correct feature

Changelog: none"
head="$(git -C "${d}" rev-parse HEAD)"
check "changelog_none_trailer_passes" "${d}" "${base}" "${head}" 0 "clean"

# --- missing_base_ref_exits_2 ------------------------------------------------

d="$(new_repo missing-base)"
head="$(git -C "${d}" rev-parse HEAD)"
check "missing_base_ref_exits_2" "${d}" "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef" "${head}" 2 \
  "cannot resolve base ref"

# --- bonus: CHANGELOG.md touched in range also satisfies the rule -----------

d="$(new_repo changelog-touched)"
base="$(git -C "${d}" rev-parse HEAD)"
echo 'fn feature() {}' >"${d}/crates/c/feature.rs"
printf '\n- new feature entry\n' >>"${d}/CHANGELOG.md"
git -C "${d}" add crates/c/feature.rs CHANGELOG.md
git -C "${d}" commit -q -m "feat(c): add feature with changelog"
head="$(git -C "${d}" rev-parse HEAD)"
check "changelog_touched_in_range_passes" "${d}" "${base}" "${head}" 0 "clean"

# --- missing argument entirely -----------------------------------------------

d="$(new_repo no-args)"
out="$(cd "${d}" && bash scripts/guards/check-changelog-touched.sh 2>&1)"
rc=$?
if [[ "${rc}" == "2" && "${out}" == *"missing <base-ref>"* ]]; then
  printf 'ok    no arguments at all exits 2\n'
  passes=$((passes + 1))
else
  printf 'FAIL  no arguments at all exits 2: got %s / %s\n' "${rc}" "${out}"
  fails=$((fails + 1))
fi

# --- a diverged base does not borrow the base branch's changelog edit --------
#
# The range must be taken from the merge base. With a two-dot range, a
# CHANGELOG.md edit that landed on main AFTER the fork point reads as part of
# this range and exempts a pull request that touched no changelog. Because this
# guard makes nearly every merged pull request touch CHANGELOG.md, a base and
# head that diverge over a changelog edit is the common case.

d="$(new_repo diverged-base)"
git -C "${d}" checkout -q -b feature
mkdir -p "${d}/crates/c"
printf 'pub fn f() {}\n' >"${d}/crates/c/feature.rs"
git -C "${d}" add crates/c/feature.rs
git -C "${d}" commit -q -m "feat(c): add feature with no changelog entry"
head="$(git -C "${d}" rev-parse HEAD)"
# Meanwhile main gains a changelog edit of its own, after the fork point.
git -C "${d}" checkout -q main
printf '\n- someone else\n' >>"${d}/CHANGELOG.md"
git -C "${d}" add CHANGELOG.md
git -C "${d}" commit -q -m "docs: unrelated changelog entry on main"
base="$(git -C "${d}" rev-parse HEAD)"
check "a changelog edit on the base branch does not exempt this range" \
  "${d}" "${base}" "${head}" 1 "CHANGELOG.md"

# A feat commit on the base branch must not be reported as this range's.
#
# Unlike the case above, this one does NOT fail under the two-dot range: the
# review suggested the commit walk had the mirror of the diff bug, but
# `git rev-list base..head` excludes everything reachable from base, so a
# commit on the base branch is never walked either way. The case is kept as a
# property worth pinning, not as a regression test for the merge-base fix; only
# the case above discriminates, and it is the one that was silently passing.
d="$(new_repo diverged-base-foreign-feat)"
git -C "${d}" checkout -q -b docs-only
printf 'text\n' >"${d}/README.md"
git -C "${d}" add README.md
git -C "${d}" commit -q -m "docs: touch nothing that qualifies"
head="$(git -C "${d}" rev-parse HEAD)"
git -C "${d}" checkout -q main
mkdir -p "${d}/services/s"
printf 'pub fn g() {}\n' >"${d}/services/s/other.rs"
git -C "${d}" add services/s/other.rs
git -C "${d}" commit -q -m "feat(s): a neighbour feature with no changelog"
base="$(git -C "${d}" rev-parse HEAD)"
check "a feat commit on the base branch is not this range's" \
  "${d}" "${base}" "${head}" 0 "clean"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
