#!/usr/bin/env bash
# Cases for assert-worktree.sh. Run:
#   bash scripts/guards/assert-worktree.test.sh
#
# Builds throwaway repositories under $TMPDIR: a primary checkout with a
# linked worktree, and a standalone clone marked as a Brig sandbox clone.
# git only, no network.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
GUARD="${HERE}/assert-worktree.sh"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/assert-worktree-test.XXXXXX")"
trap 'rm -rf "${TMP}"' EXIT
TMP="$(cd "${TMP}" && pwd -P)"

passes=0
fails=0

# Runs the guard in a directory with the given BRIG_PROJECT ('' for unset)
# and checks its exit code.
case_exit() {
  local label="$1" want="$2" dir="$3" project="$4" got
  if [[ -n "${project}" ]]; then
    (cd "${dir}" && BRIG_PROJECT="${project}" sh "${GUARD}") >/dev/null 2>&1
  else
    (cd "${dir}" && env -u BRIG_PROJECT sh "${GUARD}") >/dev/null 2>&1
  fi
  got=$?
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${label}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %-56s want %s, got %s\n' "${label}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

git_q() { git -c init.defaultBranch=main -c user.email=t@example.test -c user.name=t "$@" >/dev/null 2>&1; }

primary="${TMP}/primary"
git_q init "${primary}"
git_q -C "${primary}" commit --allow-empty -m init
git_q -C "${primary}" worktree add -b wt "${TMP}/wt"

sandbox="${TMP}/sandbox"
git_q clone "${primary}" "${sandbox}"
git -C "${sandbox}" config ravel.sandboxCheckout true
mkdir -p "${sandbox}/sub"

unmarked="${TMP}/unmarked"
git_q clone "${primary}" "${unmarked}"

mkdir -p "${TMP}/plain"

case_exit "linked worktree passes"                       0 "${TMP}/wt"   ''
case_exit "primary checkout is refused"                  1 "${primary}"  ''
case_exit "not a git repository is refused"              1 "${TMP}/plain" ''
case_exit "sandbox clone in its guest passes"            0 "${sandbox}"  "${sandbox}"
case_exit "sandbox clone from a subdirectory passes"     0 "${sandbox}/sub" "${sandbox}"
case_exit "sandbox clone on the host is refused"         1 "${sandbox}"  ''
case_exit "sandbox clone under another project refused"  1 "${sandbox}"  "${unmarked}"
case_exit "unmarked clone in a guest is refused"         1 "${unmarked}" "${unmarked}"
case_exit "primary with BRIG_PROJECT but no mark refused" 1 "${primary}" "${primary}"

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
