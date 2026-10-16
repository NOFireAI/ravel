#!/usr/bin/env bash
# Cases for .githooks/pre-push, the destructive-push guard.
#
# These drive REAL pushes between real local repositories, because the
# property under test is structural: the hook never sees `--force`, it sees
# four fields on stdin and has to work out from them that commits would
# disappear. A test that called the hook with hand-written stdin would prove
# nothing about whether git's own invocation matches that shape.
#
# `gh` is stubbed so the open-pull-request lookup is deterministic, and one
# case removes it entirely to prove that "could not ask" refuses rather than
# passing for "no pull request open".
#
# Run by hand:   bash scripts/tests/pre-push-destructive.test.sh
# Wired into CI: the doc-scripts job in .github/workflows/ci.yml.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
HOOK="${HOOK:-${SCRIPT_DIR}/../.githooks/pre-push}"

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

git_q() { /usr/bin/git "$@" >/dev/null 2>&1; }

# A fresh remote + clone with the hook installed. `pr_answer`:
#   number -> that pull request is open for the branch
#   empty  -> asked, nothing open
#   absent -> gh itself fails (could not ask)
new_repo() {
  local name="$1" pr_answer="${2-}"
  local root="${work}/${name}"
  mkdir -p "${root}/bin" "${root}/hooks"
  cp "${HOOK}" "${root}/hooks/pre-push"
  chmod +x "${root}/hooks/pre-push"

  if [[ "${pr_answer}" != "ABSENT" ]]; then
    cat >"${root}/bin/gh" <<STUB
#!/usr/bin/env bash
printf '%s' "${pr_answer}"
exit 0
STUB
    chmod +x "${root}/bin/gh"
  fi

  git_q init --bare -b main "${root}/origin.git"
  git_q clone "${root}/origin.git" "${root}/work"
  ( cd "${root}/work"
    /usr/bin/git config user.email t@example.com
    /usr/bin/git config user.name t
    /usr/bin/git config core.hooksPath "${root}/hooks"
    echo one >f && /usr/bin/git add f && /usr/bin/git commit -qm one
    echo two >>f && /usr/bin/git add f && /usr/bin/git commit -qm two
    /usr/bin/git push -q origin main ) >/dev/null 2>&1
  printf '%s\n' "${root}"
}

# Runs a push with a PATH that contains only the stub bin plus the real
# system directories, so `gh` resolves to the stub (or to nothing).
push_in() {
  local root="$1"; shift
  ( cd "${root}/work"
    export PATH="${root}/bin:/usr/bin:/bin:/usr/sbin:/sbin"
    /usr/bin/git "$@" ) 2>&1
}

# --- a fast-forward push is untouched ----------------------------------
r="$(new_repo ff 123)"
( cd "${r}/work" && echo three >>f && /usr/bin/git add f && /usr/bin/git commit -qm three ) >/dev/null
out="$(push_in "${r}" push origin main)"; rc=$?
check_eq "a fast-forward push to main is allowed" "0" "${rc}"

# --- dropping commits from main is refused -----------------------------
# Mutation: compare the flag instead of ancestry (the hook never sees it),
# or drop the merge-base test.
r="$(new_repo force_main 123)"
( cd "${r}/work" && /usr/bin/git reset -q --hard HEAD~1 ) >/dev/null
out="$(push_in "${r}" push --force origin main)"; rc=$?
check_eq "a history-dropping push to main is refused" "1" "${rc}"
check_contains "and says what it refused" "REFUSED" "${out}"
check_contains "and lists the commit that would vanish" "two" "${out}"
remote_head="$(/usr/bin/git -C "${r}/origin.git" rev-parse main)"
local_head="$(/usr/bin/git -C "${r}/work" rev-parse HEAD)"
check_eq "and the remote still has its commit" "no" \
  "$([[ "${remote_head}" == "${local_head}" ]] && echo yes || echo no)"

# --force-with-lease and a +refspec are the same operation wearing a
# different flag, and the hook sees neither spelling.
r="$(new_repo lease 123)"
( cd "${r}/work" && /usr/bin/git reset -q --hard HEAD~1 ) >/dev/null
out="$(push_in "${r}" push --force-with-lease origin main)"; rc=$?
check_eq "--force-with-lease to main is refused too" "1" "${rc}"

r="$(new_repo refspec 123)"
( cd "${r}/work" && /usr/bin/git reset -q --hard HEAD~1 ) >/dev/null
out="$(push_in "${r}" push origin +main:main)"; rc=$?
check_eq "a +refspec push to main is refused too" "1" "${rc}"

# --- the escape hatch --------------------------------------------------
r="$(new_repo allowed 123)"
( cd "${r}/work" && /usr/bin/git reset -q --hard HEAD~1 ) >/dev/null
out="$( cd "${r}/work"
        export PATH="${r}/bin:/usr/bin:/bin:/usr/sbin:/sbin" ALLOW_DESTRUCTIVE=1
        /usr/bin/git push --force origin main 2>&1 )"; rc=$?
check_eq "ALLOW_DESTRUCTIVE=1 lets the push through" "0" "${rc}"
check_contains "and still prints the loss" "ALLOW_DESTRUCTIVE=1: allowing" "${out}"

# --- a branch with an open pull request --------------------------------
r="$(new_repo pr_open 456)"
( cd "${r}/work"
  /usr/bin/git checkout -qb feature
  echo x >>f && /usr/bin/git add f && /usr/bin/git commit -qm feat
  /usr/bin/git push -q origin feature
  /usr/bin/git reset -q --hard HEAD~1 ) >/dev/null 2>&1
out="$(push_in "${r}" push --force origin feature)"; rc=$?
check_eq "a history-dropping push to a branch with an open PR is refused" "1" "${rc}"
check_contains "and names the pull request" "#456" "${out}"

# --- a branch with no open pull request is ordinary work ---------------
# Mutation: block every non-fast-forward push, and every fix round on a
# private branch stops.
r="$(new_repo pr_none "")"
( cd "${r}/work"
  /usr/bin/git checkout -qb scratch
  echo x >>f && /usr/bin/git add f && /usr/bin/git commit -qm wip
  /usr/bin/git push -q origin scratch
  /usr/bin/git reset -q --hard HEAD~1 ) >/dev/null 2>&1
out="$(push_in "${r}" push --force origin scratch)"; rc=$?
check_eq "a force-push to a branch with no open PR is allowed" "0" "${rc}"
check_contains "and is still reported" "non-fast-forward" "${out}"

# --- could not ask is not permission -----------------------------------
# Mutation: treat a failed gh lookup as "no pull request open".
r="$(new_repo gh_absent ABSENT)"
( cd "${r}/work"
  /usr/bin/git checkout -qb quiet
  echo x >>f && /usr/bin/git add f && /usr/bin/git commit -qm wip
  /usr/bin/git push -q origin quiet
  /usr/bin/git reset -q --hard HEAD~1 ) >/dev/null 2>&1
out="$(push_in "${r}" push --force origin quiet)"; rc=$?
check_eq "an unanswerable pull-request lookup refuses (1)" "1" "${rc}"
check_contains "and says why" "could not be read" "${out}"

# --- deleting a branch --------------------------------------------------
r="$(new_repo delete_main 123)"
out="$(push_in "${r}" push origin :main)"; rc=$?
check_eq "deleting main is refused" "1" "${rc}"
check_contains "and says it is a delete" "DELETING" "${out}"

# --- a brand-new remote branch removes nothing --------------------------
r="$(new_repo new_branch "")"
( cd "${r}/work"
  /usr/bin/git checkout -qb brand-new
  echo y >>f && /usr/bin/git add f && /usr/bin/git commit -qm new ) >/dev/null 2>&1
out="$(push_in "${r}" push origin brand-new)"; rc=$?
check_eq "pushing a new branch is allowed" "0" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ ${fail} -eq 0 ]]
