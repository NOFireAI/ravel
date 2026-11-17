#!/usr/bin/env bash
# Cases for check-compat-corpus-append-only.sh, in the pattern of
# check-changelog-touched.test.sh: add a case here before changing a rule.
#
# Each case builds a throwaway git repo under $TMPDIR, commits a base state
# that already holds a published corpus, then one or more commits forming the
# range under test, and runs the guard against that repo. The guard only reads
# git history, so `cd` into the fixture is all it needs.
#
# Run: bash scripts/guards/check-compat-corpus-append-only.test.sh
set -uo pipefail

export GIT_CONFIG_NOSYSTEM=1
export GIT_CONFIG_GLOBAL=/dev/null

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/check-compat-corpus-append-only.sh"
CORPUS="crates/ravel-compat-tests/corpus"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/check-compat-corpus-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT

fails=0
passes=0

# new_repo <name>: an initialized repo whose base commit already holds a
# published corpus directory crates/ravel-compat-tests/corpus/v0.34.0/ with one
# fixture object and its expected-results file, plus the guard installed and an
# unrelated source file. Prints its path.
new_repo() {
  local dir="${TMP}/$1"
  mkdir -p "${dir}/scripts/guards" "${dir}/${CORPUS}/v0.34.0" \
    "${dir}/crates/ravel-compat-tests/tests"
  cp "${GUARD}" "${dir}/scripts/guards/check-compat-corpus-append-only.sh"
  git -C "${dir}" init -q -b main 2>/dev/null
  git -C "${dir}" config user.email t@example.com
  git -C "${dir}" config user.name Test
  printf 'RSG1-fixture-bytes\n' >"${dir}/${CORPUS}/v0.34.0/metrics.rseg"
  printf 'expected: 1.0\n' >"${dir}/${CORPUS}/v0.34.0/expected.json"
  printf 'fn t() {}\n' >"${dir}/crates/ravel-compat-tests/tests/matrix.rs"
  git -C "${dir}" add -A
  git -C "${dir}" commit -q -m "chore: seed with a published corpus"
  printf '%s\n' "${dir}"
}

# check <name> <repo> <base> <head> <want-exit> <want-substring-or-empty>
check() {
  local name="$1" dir="$2" base="$3" head="$4" want_rc="$5" want_sub="${6:-}"
  local out rc=0
  out="$(cd "${dir}" && "${BASH}" scripts/guards/check-compat-corpus-append-only.sh "${base}" "${head}" 2>&1)" || rc=$?
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

# --- no_corpus_change_passes -------------------------------------------------
d="$(new_repo no-change)"
base="$(git -C "${d}" rev-parse HEAD)"
echo 'fn more() {}' >"${d}/crates/ravel-compat-tests/tests/matrix.rs"
git -C "${d}" add -A
git -C "${d}" commit -q -m "test: extend matrix (no corpus change)"
head="$(git -C "${d}" rev-parse HEAD)"
check "no_corpus_change_passes" "${d}" "${base}" "${head}" 0 "clean"

# --- added_fixture_passes ----------------------------------------------------
d="$(new_repo add-fixture)"
base="$(git -C "${d}" rev-parse HEAD)"
printf 'RLG1-fixture\n' >"${d}/${CORPUS}/v0.34.0/logs.rlog"
git -C "${d}" add -A
git -C "${d}" commit -q -m "test: add a logs fixture to the corpus"
head="$(git -C "${d}" rev-parse HEAD)"
check "added_fixture_passes" "${d}" "${base}" "${head}" 0 "clean"

# --- added_new_release_dir_passes --------------------------------------------
d="$(new_repo add-release)"
base="$(git -C "${d}" rev-parse HEAD)"
mkdir -p "${d}/${CORPUS}/v0.35.0"
printf 'RSG1-next\n' >"${d}/${CORPUS}/v0.35.0/metrics.rseg"
git -C "${d}" add -A
git -C "${d}" commit -q -m "test: add v0.35.0 corpus"
head="$(git -C "${d}" rev-parse HEAD)"
check "added_new_release_dir_passes" "${d}" "${base}" "${head}" 0 "clean"

# --- modified_fixture_fails --------------------------------------------------
d="$(new_repo modify-fixture)"
base="$(git -C "${d}" rev-parse HEAD)"
printf 'RSG1-fixture-bytes-TAMPERED\n' >"${d}/${CORPUS}/v0.34.0/metrics.rseg"
git -C "${d}" add -A
git -C "${d}" commit -q -m "test: tamper with a published fixture"
head="$(git -C "${d}" rev-parse HEAD)"
check "modified_fixture_fails" "${d}" "${base}" "${head}" 1 \
  "MODIFIED published corpus file: ${CORPUS}/v0.34.0/metrics.rseg"

# --- modified_expected_results_fails -----------------------------------------
# The recorded expected-results file lives inside corpus/, so editing it is as
# much a tamper as editing the data object.
d="$(new_repo modify-expected)"
base="$(git -C "${d}" rev-parse HEAD)"
printf 'expected: 999.0\n' >"${d}/${CORPUS}/v0.34.0/expected.json"
git -C "${d}" add -A
git -C "${d}" commit -q -m "test: change a recorded expectation"
head="$(git -C "${d}" rev-parse HEAD)"
check "modified_expected_results_fails" "${d}" "${base}" "${head}" 1 \
  "MODIFIED published corpus file: ${CORPUS}/v0.34.0/expected.json"

# --- deleted_fixture_fails ---------------------------------------------------
d="$(new_repo delete-fixture)"
base="$(git -C "${d}" rev-parse HEAD)"
git -C "${d}" rm -q "${CORPUS}/v0.34.0/metrics.rseg"
git -C "${d}" commit -q -m "test: delete a published fixture"
head="$(git -C "${d}" rev-parse HEAD)"
check "deleted_fixture_fails" "${d}" "${base}" "${head}" 1 \
  "DELETED published corpus file: ${CORPUS}/v0.34.0/metrics.rseg"

# --- renamed_fixture_fails ---------------------------------------------------
# A rename removes the old published path. With --no-renames the guard sees a
# delete of the old name, which fails, and an add of the new name, which is
# fine.
d="$(new_repo rename-fixture)"
base="$(git -C "${d}" rev-parse HEAD)"
git -C "${d}" mv "${CORPUS}/v0.34.0/metrics.rseg" "${CORPUS}/v0.34.0/metrics-renamed.rseg"
git -C "${d}" commit -q -m "test: rename a published fixture"
head="$(git -C "${d}" rev-parse HEAD)"
check "renamed_fixture_fails" "${d}" "${base}" "${head}" 1 \
  "DELETED published corpus file: ${CORPUS}/v0.34.0/metrics.rseg"

# --- non_corpus_change_under_crate_passes ------------------------------------
# Only corpus/ is frozen; the Rust tests and Cargo.toml are free to change.
d="$(new_repo non-corpus)"
base="$(git -C "${d}" rev-parse HEAD)"
printf '[package]\nname = "ravel-compat-tests"\n' >"${d}/crates/ravel-compat-tests/Cargo.toml"
git -C "${d}" add -A
git -C "${d}" commit -q -m "chore: edit Cargo.toml (outside corpus)"
head="$(git -C "${d}" rev-parse HEAD)"
check "non_corpus_change_under_crate_passes" "${d}" "${base}" "${head}" 0 "clean"

# --- merge_base_ignores_base_branch_edit -------------------------------------
# A corpus edit pushed to the base branch after the fork point must not be
# attributed to this range: the guard ranges from the merge base.
d="$(new_repo merge-base)"
fork="$(git -C "${d}" rev-parse HEAD)"
# base branch moves ahead with a tamper of its own
printf 'RSG1-base-branch-edit\n' >"${d}/${CORPUS}/v0.34.0/metrics.rseg"
git -C "${d}" add -A
git -C "${d}" commit -q -m "test: base-branch edit"
base="$(git -C "${d}" rev-parse HEAD)"
# our head branches from the fork and adds a fixture only
git -C "${d}" checkout -q -b feature "${fork}"
printf 'RLG1-ours\n' >"${d}/${CORPUS}/v0.34.0/logs.rlog"
git -C "${d}" add -A
git -C "${d}" commit -q -m "test: our added fixture"
head="$(git -C "${d}" rev-parse HEAD)"
check "merge_base_ignores_base_branch_edit" "${d}" "${base}" "${head}" 0 "clean"

# --- missing_base_ref_is_could_not_check -------------------------------------
d="$(new_repo missing-arg)"
check "missing_base_ref_is_could_not_check" "${d}" "" HEAD 2 "missing <base-ref>"

# --- unresolvable_base_ref_is_could_not_check --------------------------------
d="$(new_repo bad-ref)"
head="$(git -C "${d}" rev-parse HEAD)"
check "unresolvable_base_ref_is_could_not_check" "${d}" "deadbeefdeadbeef" "${head}" 2 \
  "cannot resolve base ref"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
