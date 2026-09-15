#!/usr/bin/env bash
# Cases for scripts/guards/check-test-suites-run.sh.
#
# The guard asserts that every shell test suite is named in some workflow. It
# therefore has to fail on an orphan and pass on a wired suite, and it must
# not report clean when it could not look -- a guard that says "all run" when
# it found nothing to check is worse than no guard, because it retires the
# suspicion that would have caught the orphan.
#
# Each case builds a throwaway repo with its own workflows dir; the real
# repository is never consulted.
#
# Run by hand:   bash scripts/tests/check-test-suites-run.test.sh
# Wired into CI: the doc-scripts job in .github/workflows/ci.yml.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
GUARD="${GUARD:-${SCRIPT_DIR}/guards/check-test-suites-run.sh}"

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

check_contains() {
  local label="$1" needle="$2" haystack="$3"
  if [[ "${haystack}" == *"${needle}"* ]]; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n  want substring: %s\n  got: %s\n' "${label}" "${needle}" "${haystack}"
  fi
}

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# new_repo <name>: a repo with a workflows dir and the guard copied in at the
# path it expects relative to itself.
new_repo() {
  local dir="${work}/$1"
  mkdir -p "${dir}/.github/workflows" "${dir}/scripts/guards" "${dir}/scripts/tests"
  git -C "${dir}" init -q -b main
  git -C "${dir}" config user.email t@example.test
  git -C "${dir}" config user.name t
  cp "${GUARD}" "${dir}/scripts/guards/check-test-suites-run.sh"
  chmod +x "${dir}/scripts/guards/check-test-suites-run.sh"
  printf 'name: ci\non: [push]\njobs:\n  x:\n    runs-on: ubuntu-latest\n    steps:\n' \
    >"${dir}/.github/workflows/ci.yml"
  printf '%s\n' "${dir}"
}

# add_suite <repo> <path> [wired]
add_suite() {
  local dir="$1" path="$2" wired="${3:-no}"
  mkdir -p "${dir}/$(dirname "${path}")"
  printf '#!/usr/bin/env bash\nexit 0\n' >"${dir}/${path}"
  chmod +x "${dir}/${path}"
  if [[ "${wired}" == "wired" ]]; then
    printf '      - run: bash %s\n' "${path}" >>"${dir}/.github/workflows/ci.yml"
  fi
}

commit_all() {
  git -C "$1" add -A
  git -C "$1" commit -q -m "seed"
}

run_guard() { ( cd "$1" && ./scripts/guards/check-test-suites-run.sh ) 2>&1; }

# --- a wired suite passes ----------------------------------------------
d="$(new_repo wired)"
add_suite "${d}" "scripts/tests/alpha.test.sh" wired
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "a suite named in a workflow passes (0)" "0" "${rc}"
check_contains "and says how many it checked" "shell test suite(s), all run" "${out}"

# --- an orphan fails ----------------------------------------------------
# Mutation: make the guard exit 0 regardless, or drop the grep; this case is
# the whole point of the file.
d="$(new_repo orphan)"
add_suite "${d}" "scripts/tests/alpha.test.sh" wired
add_suite "${d}" "scripts/tests/beta.test.sh"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "an orphan suite fails (1)" "1" "${rc}"
check_contains "and names it" "beta.test.sh" "${out}"
check_contains "and does not name the wired one as an orphan" "ORPHAN    scripts/tests/beta.test.sh" "${out}"

# --- a suite wired in a NON-ci workflow still counts --------------------
# A nightly is a workflow. Requiring ci.yml specifically would push slow
# suites toward being deleted rather than scheduled.
d="$(new_repo nightly)"
add_suite "${d}" "scripts/tests/slow.test.sh"
printf 'name: nightly\non:\n  schedule:\n    - cron: "0 3 * * *"\njobs:\n  y:\n    runs-on: ubuntu-latest\n    steps:\n      - run: bash scripts/tests/slow.test.sh\n' \
  >"${d}/.github/workflows/nightly.yml"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "a suite run only by a nightly workflow passes (0)" "0" "${rc}"

# --- untracked suites are not this check's business ---------------------
# A scratch suite in someone's worktree would otherwise fail the guard on
# that machine and nowhere else.
d="$(new_repo untracked)"
add_suite "${d}" "scripts/tests/alpha.test.sh" wired
commit_all "${d}"
add_suite "${d}" "scripts/tests/scratch.test.sh"   # created, never committed
out="$(run_guard "${d}")"; rc=$?
check_eq "an untracked suite is ignored (0)" "0" "${rc}"

# --- could not look is not clean ----------------------------------------
# Zero suites means the glob changed or this ran somewhere unexpected. Either
# way the check did not check, and reporting a pass would retire exactly the
# suspicion that finds the next orphan.
# Mutation: return 0 on an empty list.
d="$(new_repo empty)"
printf 'seed\n' >"${d}/README.md"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "finding no suites at all exits 2, not 0" "2" "${rc}"
check_contains "and says it refuses to report clean" "refusing to report clean" "${out}"

# A missing workflows dir is the same shape.
d="$(new_repo noworkflows)"
add_suite "${d}" "scripts/tests/alpha.test.sh"
rm -rf "${d}/.github"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "no workflows dir exits 2, not 0" "2" "${rc}"

# --- --list reports the wired ones too ----------------------------------
d="$(new_repo listmode)"
add_suite "${d}" "scripts/tests/alpha.test.sh" wired
commit_all "${d}"
out="$( cd "${d}" && ./scripts/guards/check-test-suites-run.sh --list 2>&1 )"; rc=$?
check_eq "--list exits 0 on a clean tree" "0" "${rc}"
check_contains "--list names the wired suite" "run       scripts/tests/alpha.test.sh" "${out}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
