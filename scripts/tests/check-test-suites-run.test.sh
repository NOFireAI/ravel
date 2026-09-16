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

# An unknown helper is a silent pass here: this file runs under `set -uo
# pipefail` without `-e`, so a call to a function that does not exist prints
# "command not found", increments nothing, and leaves the summary reading
# green. That happened to `check_true`, copied in from a sibling suite that
# defines it -- the assertion it guarded never ran at all.
check_true() {
  local label="$1" cond="$2"
  if [[ "${cond}" == "1" ]]; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n' "${label}"
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
# Asserts the WIRED suite is absent from the output. The previous version
# asserted the ORPHAN line for beta -- the orphan -- which is line above's
# assertion with a prefix, and never mentioned alpha at all. A guard patched
# to libel every wired suite as an orphan passed the whole file unchanged.
check_true "and does not name the wired one as an orphan" \
  "$([[ "${out}" != *"alpha.test.sh"* ]] && echo 1 || echo 0)"

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

# --- a suite named only in a YAML comment is NOT run --------------------
# A comment is not something running a suite. Matching the raw files made the
# guard report a suite as covered when a workflow merely talked about it, and
# the change that added this guard put two real suites into that state.
# Mutation: match the files directly instead of the comment-stripped text.
d="$(new_repo commentonly)"
add_suite "${d}" "scripts/tests/alpha.test.sh" wired
add_suite "${d}" "scripts/tests/beta.test.sh"
printf '      # we used to run scripts/tests/beta.test.sh here; removed for speed\n' \
  >>"${d}/.github/workflows/ci.yml"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "a suite named only in a comment is an orphan (1)" "1" "${rc}"
check_contains "and is named as one" "ORPHAN    scripts/tests/beta.test.sh" "${out}"

# A trailing comment on a real step does not disqualify that step.
d="$(new_repo trailingcomment)"
add_suite "${d}" "scripts/tests/alpha.test.sh"
printf '      - run: bash scripts/tests/alpha.test.sh  # the important one\n' \
  >>"${d}/.github/workflows/ci.yml"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "a step with a trailing comment still counts (0)" "0" "${rc}"

# --- the exception branch ----------------------------------------------
# Reachable only because the guard reads extra entries from the environment.
# With the array literal as the only source, is_excepted and excepted_reason
# were unreachable from here and a typo in the field split would ship green.
# Mutation: change `${entry%%|*}` to `${entry%|*}` and the reason leaks into
# the path comparison, failing the first case below.
d="$(new_repo excepted)"
add_suite "${d}" "scripts/tests/alpha.test.sh" wired
add_suite "${d}" "scripts/tests/slow.test.sh"
commit_all "${d}"
# The reason carries its own `|`, which is what makes the field split
# observable: with a single separator `${entry%%|*}` and `${entry%|*}` return
# the same string, so a case using a pipe-free reason cannot tell a greedy
# split from a lazy one.
out="$( cd "${d}" && CHECK_TEST_SUITES_EXCEPTED='scripts/tests/slow.test.sh|needs a GPU | see #1234' \
        ./scripts/guards/check-test-suites-run.sh 2>&1 )"; rc=$?
check_eq "an excepted suite does not fail the guard (0)" "0" "${rc}"
check_contains "and its whole reason is printed" "needs a GPU | see #1234" "${out}"
# An exception must be visible without --list: the summary line is what a
# reader uses to decide nothing is excluded.
check_contains "and the summary says how many were excepted" "1 excepted above" "${out}"
check_true "and the summary does not claim all are run" \
  "$([[ "${out}" != *"all run by a workflow"* ]] && echo 1 || echo 0)"

# An exception for a DIFFERENT suite does not cover this one.
d="$(new_repo excepted_other)"
add_suite "${d}" "scripts/tests/alpha.test.sh" wired
add_suite "${d}" "scripts/tests/slow.test.sh"
commit_all "${d}"
out="$( cd "${d}" && CHECK_TEST_SUITES_EXCEPTED='scripts/tests/other.test.sh|unrelated' \
        ./scripts/guards/check-test-suites-run.sh 2>&1 )"; rc=$?
check_eq "an unrelated exception leaves the orphan failing (1)" "1" "${rc}"

# --- one suite's name must not be covered by another's line -------------
# A plain substring match lets `prefix-foo.test.sh` cover `foo.test.sh`, so
# an orphan reads as run. That is the guard's own worst failure and a silent
# one. No such pair exists among the real suites, which is why the match has
# to be right before one appears.
# Mutation: drop the boundaries (plain grep -qF on the basename).
d="$(new_repo substring)"
add_suite "${d}" "scripts/tests/prefix-foo.test.sh" wired
add_suite "${d}" "scripts/tests/foo.test.sh"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "a name covered only as another's substring is an orphan (1)" "1" "${rc}"
check_contains "and is named" "ORPHAN    scripts/tests/foo.test.sh" "${out}"
check_true "and the wired longer name is not called an orphan" \
  "$([[ "${out}" != *"ORPHAN    scripts/tests/prefix-foo.test.sh"* ]] && echo 1 || echo 0)"

# The reverse direction: the longer name is genuinely wired, and a name that
# merely SHARES a prefix must not steal its coverage either.
d="$(new_repo substring_rev)"
add_suite "${d}" "scripts/tests/foo.test.sh" wired
add_suite "${d}" "scripts/tests/foo.test.sh.bak.test.sh"
commit_all "${d}"
out="$(run_guard "${d}")"; rc=$?
check_eq "a longer name is not covered by a shorter wired one (1)" "1" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
