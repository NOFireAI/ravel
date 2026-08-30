#!/usr/bin/env bash
# What scripts/disk-reap.sh must decide about a DETACHED worktree. Run:
#   bash scripts/tests/disk-reap-detached.test.sh
#
# Every worktree this repo's workflow creates is on a detached HEAD, and
# `main` is rebase-merged, so a landed worktree's HEAD is never an ancestor
# of origin/main. Both facts together are issue #941: the script's only
# detached-HEAD path was an early `skip` that never reached any merge check,
# so a dry run over 69 worktrees reclaimed almost nothing while reporting
# what looked like healthy skips.
#
# Each case builds a throwaway repo (a bare origin plus a working clone plus
# worktrees) under $TMPDIR and runs disk-reap.sh against it in DRY RUN, with
# a fake `gh` on PATH. Nothing on the host is fetched, pushed, or removed.
set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/disk-reap.sh"
# Sourced for the helper-level cases; the file returns before its main body.
# shellcheck source=../disk-reap.sh
source "${SCRIPT}"

tmproot="$(mktemp -d "${TMPDIR:-/tmp}/disk-reap-detached.XXXXXX")"
if [[ ! -d "${tmproot}" ]]; then
  echo "FAIL  could not create a temp dir for the scratch repos" >&2
  exit 1
fi
trap 'rm -rf "${tmproot}"' EXIT

pass=0
fail=0

check_contains() {
  local label="$1" needle="$2" haystack="$3"
  if [[ "${haystack}" == *"${needle}"* ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  wanted a line containing: %s\n  got:\n%s\n' \
      "${label}" "${needle}" "${haystack}"
  fi
}

check_absent() {
  local label="$1" needle="$2" haystack="$3"
  if [[ "${haystack}" != *"${needle}"* ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  did not want: %s\n  got:\n%s\n' \
      "${label}" "${needle}" "${haystack}"
  fi
}

# --- fake gh -------------------------------------------------------------
# Answers only the two calls disk-reap.sh makes, from files in $GH_FAKE_DIR:
#   sha-<sha>      the `<number>\t<head sha>` line for that commit's merged PR
#   branch-<name>  the same line for a branch-keyed lookup
# A missing file means "no merged PR" and exits 0; GH_FAKE_MODE=down makes
# every call fail, standing in for an unreachable or unauthenticated API.
# It ignores --jq and prints the pre-rendered line, so it stands in for the
# API's shape, not for jq.
fakebin="${tmproot}/bin"
mkdir -p "${fakebin}"
cat >"${fakebin}/gh" <<'FAKEGH'
#!/usr/bin/env bash
if [[ "${GH_FAKE_MODE:-}" == "down" ]]; then
  echo "fake gh: could not connect to api.github.com" >&2
  exit 1
fi
sub="${1:-}"
case "${sub}" in
  api)
    endpoint="${2:-}"
    sha="${endpoint#*/commits/}"
    sha="${sha%/pulls}"
    if [[ -f "${GH_FAKE_DIR:-}/sha-${sha}" ]]; then
      cat "${GH_FAKE_DIR}/sha-${sha}"
    fi
    exit 0
    ;;
  pr)
    branch=""
    while [[ $# -gt 0 ]]; do
      if [[ "$1" == "--head" ]]; then
        branch="${2:-}"
      fi
      shift
    done
    if [[ -n "${branch}" && -f "${GH_FAKE_DIR:-}/branch-${branch}" ]]; then
      cat "${GH_FAKE_DIR}/branch-${branch}"
    fi
    exit 0
    ;;
esac
exit 0
FAKEGH
chmod +x "${fakebin}/gh"
PATH="${fakebin}:${PATH}"
export PATH

# --- scratch repo builders ----------------------------------------------

# new_scratch <name>: a bare origin with a `main` branch plus a clone that
# tracks it. Prints nothing; paths are ${tmproot}/<name>/{origin.git,repo}.
new_scratch() {
  local name="$1" root="${tmproot}/${name}"
  mkdir -p "${root}"
  git init -q --bare -b main "${root}/origin.git"
  git init -q -b main "${root}/repo"
  git -C "${root}/repo" config user.email "reaper@example.test"
  git -C "${root}/repo" config user.name "Scratch Reaper"
  git -C "${root}/repo" config commit.gpgsign false
  printf 'seed\n' >"${root}/repo/README.md"
  git -C "${root}/repo" add README.md
  git -C "${root}/repo" commit -q -m "chore: seed the scratch repo"
  git -C "${root}/repo" remote add origin "${root}/origin.git"
  git -C "${root}/repo" push -q origin main
  git -C "${root}/repo" fetch -q origin main
}

# add_detached_wt <root> <wt-name> <file> <content> <subject>: a worktree at a
# DETACHED HEAD carrying one commit that is not on origin/main. Prints the
# worktree's HEAD sha.
add_detached_wt() {
  local root="$1" name="$2" file="$3" content="$4" subject="$5"
  local wt="${root}/${name}"
  git -C "${root}/repo" worktree add -q --detach "${wt}" main
  printf '%s\n' "${content}" >"${wt}/${file}"
  git -C "${wt}" add -A
  git -C "${wt}" commit -q -m "${subject}"
  git -C "${wt}" rev-parse HEAD
}

# advance_main <root> <file> <content> <subject>: land an unrelated commit on
# origin/main, so worktree HEADs stop being ancestors of it.
advance_main() {
  local root="$1" file="$2" content="$3" subject="$4"
  printf '%s\n' "${content}" >"${root}/repo/${file}"
  git -C "${root}/repo" add -A
  git -C "${root}/repo" commit -q -m "${subject}"
  git -C "${root}/repo" push -q origin main
  git -C "${root}/repo" fetch -q origin main
}

# rebase_merge_onto_main <root> <sha>: replay <sha>'s patch onto main as a new
# commit and push it, which is what this repo's rebase-only merge does. The
# original <sha> stays reachable in the worktree and is never an ancestor of
# the new origin/main.
rebase_merge_onto_main() {
  local root="$1" sha="$2"
  git -C "${root}/repo" cherry-pick -q "${sha}" >/dev/null 2>&1
  git -C "${root}/repo" push -q origin main
  git -C "${root}/repo" fetch -q origin main
}

# reap_dry <root>: run the script in dry run against <root>/repo, merging
# stderr in, and print its output. The caller reads the exit code separately.
reap_dry() {
  local root="$1"
  RAVEL_DISK_REAP_REPO="${root}/repo" bash "${SCRIPT}" 2>&1
}

# --- (a) detached HEAD whose commit is a merged PR: REAPED ---------------
# The patch is deliberately absent from origin/main, so only the SHA-keyed PR
# lookup can produce this verdict. This is the case that fails outright
# against the pre-#941 script, which printed
#   skip (detached, not an ancestor of main): <wt>
# before any PR lookup ran.
root="${tmproot}/merged"
new_scratch "merged"
sha_merged="$(add_detached_wt "${root}" "wt-merged" "feature.txt" "landed work" "feat(scratch): landed work")"
advance_main "${root}" "other.txt" "unrelated" "feat(scratch): an unrelated commit on main"
export GH_FAKE_DIR="${tmproot}/ghfixtures-merged"
mkdir -p "${GH_FAKE_DIR}"
printf '941\t%s\n' "${sha_merged}" >"${GH_FAKE_DIR}/sha-${sha_merged}"
unset GH_FAKE_MODE
out="$(reap_dry "${root}")"; code=$?
check_contains "merged PR: exit 0" "0" "${code}"
check_contains "merged PR: detached worktree is proposed for removal" \
  "reap (PR #941 merged with this exact head" "${out}"
check_contains "merged PR: removal is a dry run, not applied" \
  "DRY-RUN: git -C ${root}/repo worktree remove --force ${root}/wt-merged" "${out}"
check_contains "merged PR: worktree still on disk after a dry run" "yes" \
  "$([[ -d "${root}/wt-merged" ]] && echo yes || echo no)"
check_absent "merged PR: no bare 'detached' skip survives" \
  "skip (detached" "${out}"

# --- (b) detached HEAD with no merged PR: KEPT --------------------------
root="${tmproot}/open"
new_scratch "open"
sha_open="$(add_detached_wt "${root}" "wt-open" "wip.txt" "in progress" "feat(scratch): work in progress")"
advance_main "${root}" "other.txt" "unrelated" "feat(scratch): an unrelated commit on main"
export GH_FAKE_DIR="${tmproot}/ghfixtures-open"
mkdir -p "${GH_FAKE_DIR}"  # no fixture: the API answers "no merged PR"
unset GH_FAKE_MODE
out="$(reap_dry "${root}")"; code=$?
check_contains "no merged PR: exit 0" "0" "${code}"
check_contains "no merged PR: worktree is kept, and says the work is open" \
  "keep (not landed:" "${out}"
check_contains "no merged PR: the reason names the detached head" \
  "detached HEAD ${sha_open:0:12}" "${out}"
check_absent "no merged PR: nothing is reaped" "reap (" "${out}"
check_absent "no merged PR: nothing is removed" "worktree remove" "${out}"

# --- (c) a dirty worktree is kept, whatever its merge state -------------
# Its HEAD is a merged PR, so only the dirty check can save it.
root="${tmproot}/dirty"
new_scratch "dirty"
sha_dirty="$(add_detached_wt "${root}" "wt-dirty" "feature.txt" "landed work" "feat(scratch): landed work")"
advance_main "${root}" "other.txt" "unrelated" "feat(scratch): an unrelated commit on main"
printf 'uncommitted edit\n' >>"${root}/wt-dirty/feature.txt"
export GH_FAKE_DIR="${tmproot}/ghfixtures-dirty"
mkdir -p "${GH_FAKE_DIR}"
printf '941\t%s\n' "${sha_dirty}" >"${GH_FAKE_DIR}/sha-${sha_dirty}"
unset GH_FAKE_MODE
out="$(reap_dry "${root}")"; code=$?
check_contains "dirty: exit 0" "0" "${code}"
check_contains "dirty: worktree is kept" "keep (dirty:" "${out}"
check_absent "dirty: nothing is reaped" "reap (" "${out}"
check_absent "dirty: nothing is removed" "worktree remove" "${out}"

# --- (d) an unreachable PR API keeps everything ------------------------
# Same shape as (a) -- a landed worktree with a merged PR -- but the API
# fails and the patch is not on main, so there is no evidence at all. The
# script must keep it and say it could not tell, not that it is in use.
root="${tmproot}/apidown"
new_scratch "apidown"
sha_down="$(add_detached_wt "${root}" "wt-down" "feature.txt" "landed work" "feat(scratch): landed work")"
advance_main "${root}" "other.txt" "unrelated" "feat(scratch): an unrelated commit on main"
export GH_FAKE_DIR="${tmproot}/ghfixtures-down"
mkdir -p "${GH_FAKE_DIR}"
printf '941\t%s\n' "${sha_down}" >"${GH_FAKE_DIR}/sha-${sha_down}"
export GH_FAKE_MODE=down
out="$(reap_dry "${root}")"; code=$?
unset GH_FAKE_MODE
check_contains "API down: exit 0" "0" "${code}"
check_contains "API down: worktree is reported undetermined, not in use" \
  "undetermined (PR API unreachable" "${out}"
check_contains "API down: the summary says undetermined worktrees were kept" \
  "undetermined worktrees were KEPT" "${out}"
check_absent "API down: nothing is reaped" "reap (" "${out}"
check_absent "API down: nothing is removed" "worktree remove" "${out}"

# --- (e) offline fallback: the rebase-written commit is found by patch-id
# The API is down, but the worktree's patch was replayed onto main by the
# rebase merge. That is proof it landed, so it is reapable without gh.
root="${tmproot}/patchid"
new_scratch "patchid"
sha_pid="$(add_detached_wt "${root}" "wt-pid" "feature.txt" "landed work" "feat(scratch): landed work")"
rebase_merge_onto_main "${root}" "${sha_pid}"
advance_main "${root}" "other.txt" "unrelated" "feat(scratch): an unrelated commit on main"
export GH_FAKE_MODE=down
out="$(reap_dry "${root}")"; code=$?
unset GH_FAKE_MODE
check_contains "patch-id: exit 0" "0" "${code}"
check_contains "patch-id: the rebase-written commit is recognized" \
  "matches a rewritten commit on origin/main by patch-id" "${out}"
check_contains "patch-id: removal is a dry run" \
  "DRY-RUN: git -C ${root}/repo worktree remove --force ${root}/wt-pid" "${out}"

# --- (f) gh not installed at all: undetermined, and it says so ----------
# PATH is reduced to symlinks of the utilities the classifier itself needs,
# so `command -v gh` genuinely fails even on a host that has gh.
root="${tmproot}/nogh"
new_scratch "nogh"
sha_nogh="$(add_detached_wt "${root}" "wt-nogh" "feature.txt" "landed work" "feat(scratch): landed work")"
advance_main "${root}" "other.txt" "unrelated" "feat(scratch): an unrelated commit on main"
minbin="${tmproot}/minbin"
mkdir -p "${minbin}"
for tool in git awk grep sed; do
  tool_path="$(command -v "${tool}" || true)"
  if [[ -n "${tool_path}" ]]; then
    ln -sf "${tool_path}" "${minbin}/${tool}"
  fi
done
main_sha_nogh="$(git -C "${root}/repo" rev-parse origin/main)"
line="$(PATH="${minbin}" dr_classify_worktree "${root}/repo" "${root}/wt-nogh" "${main_sha_nogh}" "owner/repo")"
check_contains "gh absent: verdict is UNDETERMINED" "UNDETERMINED" "${line}"
check_contains "gh absent: the reason names the missing tool" \
  "gh is not installed" "${line}"
check_contains "gh absent: the reason names the detached head" \
  "detached HEAD ${sha_nogh:0:12}" "${line}"

# --- (g) the primary checkout is never a candidate ----------------------
# It is clean and its HEAD is on origin/main, which would otherwise make it
# the most reapable thing in the list.
root="${tmproot}/primary"
new_scratch "primary"
add_detached_wt "${root}" "wt-x" "x.txt" "x" "feat(scratch): x" >/dev/null
export GH_FAKE_DIR="${tmproot}/ghfixtures-primary"
mkdir -p "${GH_FAKE_DIR}"
unset GH_FAKE_MODE
out="$(reap_dry "${root}")"; code=$?
check_contains "primary: exit 0" "0" "${code}"
check_absent "primary: the checkout itself is never named for removal" \
  "worktree remove --force ${root}/repo" "${out}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
if [[ ${fail} -ne 0 ]]; then
  exit 1
fi
