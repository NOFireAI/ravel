#!/usr/bin/env bash
# Every shell test suite must be named in a workflow, or be listed here as a
# deliberate exception with a reason.
#
# The two sides of this repository's test tree fail differently, and only one
# of them can develop an orphan:
#
#   Python:  `make test-python` runs `unittest discover -p 'test_*.py'`.
#            Discovery is by PATTERN, so adding a matching file enrols it. A
#            Python suite cannot be orphaned, and grepping a workflow for its
#            filename finds nothing and proves nothing.
#   Shell:   every suite is enumerated by hand in a workflow step. A new one
#            runs nowhere until somebody remembers, and nothing says so.
#
# That has now bitten three times, each time the same way: a suite that was
# green locally, had never executed in CI, and sat beside a real defect. The
# PreToolUse guard's 107 cases ran in no job; so did
# verify-dispatch-gates-with-gates.test.sh, next to a worktree-deletion bug;
# issue #1834 found five more. CLAUDE.md's own rule is that a test-hygiene
# rule which bites twice becomes a check rather than another paragraph.
#
# This is the enumeration side made visible. It does not run the suites; it
# asserts that something does.
#
# Usage: check-test-suites-run.sh [--list]
#   --list   print every suite and its state, not only the failures
#
# Exit codes:
#   0   every suite is run by a workflow, or knowingly excepted
#   1   at least one suite runs nowhere
#   2   the check could not be performed (no workflows dir, git failed)
set -uo pipefail

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "check-test-suites-run.sh: not inside a git repository" >&2
  exit 2
}
cd "${repo_root}" || exit 2

workflow_dir=".github/workflows"
if [[ ! -d "${workflow_dir}" ]]; then
  echo "check-test-suites-run.sh: no ${workflow_dir}; cannot tell what CI runs" >&2
  exit 2
fi

list_all=0
[[ "${1:-}" == "--list" ]] && list_all=1

# Suites deliberately not run by any workflow. Each entry carries the reason
# inline, because an allowlist without one becomes a place to make failures
# quiet. Add to this only when a suite genuinely cannot run on a runner; a
# slow suite belongs in a nightly workflow instead, which this check counts.
#
# Format: "<path>|<reason>". Keep the path repo-relative, as git reports it.
excepted=(
  # (none today; the five from #1834 are wired into doc-scripts by the same
  # change that added this guard)
)

# Extra entries from the environment, newline-separated, same "<path>|<reason>"
# shape. This exists so the exception branch is reachable from a test: with the
# array literal as the only source, `is_excepted` and `excepted_reason` shipped
# untested and a typo in the field split would have been green.
if [[ -n "${CHECK_TEST_SUITES_EXCEPTED:-}" ]]; then
  while IFS= read -r extra; do
    [[ -z "${extra}" ]] && continue
    # The reason is the whole point of the list, so it is required rather
    # than requested. Without this an entry of just the path printed the path
    # AS its own reason and exited 0, which is the quiet exclusion the comment
    # above says the format exists to prevent.
    if [[ "${extra}" != *"|"* || -z "${extra#*|}" ]]; then
      echo "check-test-suites-run.sh: exception '${extra}' has no reason; use '<path>|<reason>'" >&2
      exit 2
    fi
    excepted+=("${extra}")
  done <<<"${CHECK_TEST_SUITES_EXCEPTED}"
fi

is_excepted() {
  local want="$1" entry
  for entry in ${excepted[@]+"${excepted[@]}"}; do
    [[ "${entry%%|*}" == "${want}" ]] && return 0
  done
  return 1
}

excepted_reason() {
  local want="$1" entry
  for entry in ${excepted[@]+"${excepted[@]}"}; do
    if [[ "${entry%%|*}" == "${want}" ]]; then
      printf '%s' "${entry#*|}"
      return 0
    fi
  done
  return 1
}

# Tracked files only: an untracked scratch suite in someone's worktree is not
# this check's business, and would fail it on every machine differently.
#
# SCOPE: the `*.test.sh` suffix is itself a hand-maintained convention, which
# is this guard's own problem one level up. A suite named another way is
# invisible here, and one exists: `deploy/metricsbench/tests/*.sh` is a
# `#!/bin/sh` acceptance suite wired by hand in ci.yml. It is not an orphan
# today, checked rather than assumed. The summary line below says "shell test
# suite(s)" and means "matching this key", not "every shell suite in the
# repository".
suites="$(git ls-files -- '*.test.sh' 2>/dev/null)" || {
  echo "check-test-suites-run.sh: git ls-files failed" >&2
  exit 2
}

if [[ -z "${suites}" ]]; then
  # Zero suites is not a pass. The glob changed, the tree moved, or this ran
  # somewhere unexpected -- all of which mean the check did not check.
  echo "check-test-suites-run.sh: found no *.test.sh files at all; refusing to report clean" >&2
  exit 2
fi

# Workflow text with comment lines removed, gathered ONCE.
#
# A YAML comment is not something running a suite, and matching the raw files
# made the guard report a suite as run when a workflow only talked about it.
# That was not hypothetical: the change adding this guard put two suites into
# exactly that state, `flag-doc-guard.test.sh` through a comment in its own
# step's explanation and `check-tla.test.sh` through two in tla-nightly.yml.
# Deleting either `run:` line left the guard green, so the two suites carrying
# the most explanatory prose were the two it had stopped protecting.
#
# Gathered into a variable rather than piped per suite: a `grep | grep -v`
# pipeline reports the LAST stage's status, and this decides whether a suite
# counts as covered.
#
# A trailing comment on a real step (`run: bash x.test.sh  # why`) keeps its
# line, because the line is not a comment line.
# Comment lines AND `name:` values are dropped. A step name is prose the same
# way a comment is -- `- name: we should run x.test.sh one day` counted as
# running it, the same false clean one YAML key over. The `run:` line that
# actually invokes the suite is untouched.
workflow_text="$(grep -rhvE '^[[:space:]]*#|^[[:space:]]*-?[[:space:]]*name:' -- "${workflow_dir}" 2>/dev/null)"
if [[ -z "${workflow_text}" ]]; then
  echo "check-test-suites-run.sh: ${workflow_dir} has no non-comment content; cannot tell what CI runs" >&2
  exit 2
fi

orphans=0
excepted_count=0
total=0
while IFS= read -r suite; do
  [[ -z "${suite}" ]] && continue
  total=$((total + 1))
  base="$(basename "${suite}")"
  # Named on a non-comment line. A basename is the right key: a step may
  # invoke it via `bash scripts/tests/x.test.sh` or a variable path, and
  # matching the full repo-relative path would miss the latter.
  #
  # Bounded on both sides, because a plain substring match lets one suite's
  # line cover another's name: with `prefix-foo.test.sh` wired, a search for
  # `foo.test.sh` hits that same line and reports an orphan as run. No such
  # pair exists among the suites today, which is exactly why the match has to
  # be right before one does -- a false "run" is the failure this guard exists
  # to prevent, and it would be silent.
  #
  # The boundary class is the set of characters a filename can contain, so a
  # name touching `/`, whitespace or a quote still matches.
  # Every character outside [A-Za-z0-9_-] is escaped, not just the dot. A
  # basename carrying a regex metacharacter otherwise becomes a PATTERN:
  # `a+b.test.sh` matched the wired `ab.test.sh` line and was reported run,
  # which is the silent false-run this guard exists to prevent. Escaping by
  # enumeration rather than by assuming which characters appear, because the
  # assumption is what failed.
  #
  # Built in the shell rather than by a sed subshell: a sed that errors
  # returns empty, and an empty key matches EVERY line, so the guard would
  # report every suite as run while printing the error above its own summary.
  base_re=""
  for (( _i = 0; _i < ${#base}; _i++ )); do
    _c="${base:_i:1}"
    case "${_c}" in
      [A-Za-z0-9_-]) base_re+="${_c}" ;;
      *) base_re+="\\${_c}" ;;
    esac
  done
  if [[ -z "${base_re}" ]]; then
    echo "check-test-suites-run.sh: empty match key for '${suite}'; refusing to judge it" >&2
    exit 2
  fi
  if grep -qE -- "(^|[^A-Za-z0-9._-])${base_re}([^A-Za-z0-9._-]|\$)" <<<"${workflow_text}"; then
    ((list_all == 1)) && printf 'run       %s\n' "${suite}"
    continue
  fi
  if is_excepted "${suite}"; then
    excepted_count=$((excepted_count + 1))
    # Printed whatever the mode: the summary line is what a reader uses to
    # decide nothing is excluded, so an exception must not be visible only
    # under --list.
    printf 'excepted  %s (%s)\n' "${suite}" "$(excepted_reason "${suite}")"
    continue
  fi
  printf 'ORPHAN    %s runs in no workflow\n' "${suite}"
  orphans=$((orphans + 1))
done <<<"${suites}"

if ((orphans > 0)); then
  echo
  echo "${orphans} of ${total} shell test suite(s) run in no CI job." >&2
  echo "A suite that runs nowhere is green from the day it lands and proves nothing." >&2
  echo "Add a step naming it (doc-scripts in ci.yml is where the bash-only suites" >&2
  echo "live), or add it to this script's 'excepted' list with a reason." >&2
  exit 1
fi

if ((excepted_count > 0)); then
  echo "check-test-suites-run.sh: ${total} shell test suite(s); $((total - excepted_count)) run by a workflow, ${excepted_count} excepted above."
else
  echo "check-test-suites-run.sh: ${total} shell test suite(s), all run by a workflow."
fi
