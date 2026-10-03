#!/usr/bin/env bash
# History shapes scripts/fleet-result-merge.sh's rewrite must produce. Run:
#   bash scripts/tests/fleet-result-merge-squash.test.sh
#
# The merge script is sourced, not executed, so only its rewrite helpers run:
# every case builds a throwaway repo under $TMPDIR and rewrites it there.
# Nothing is fetched, pushed, or merged.
set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")/.." && pwd)/fleet-result-merge.sh"
# shellcheck source=../fleet-result-merge.sh
source "${SCRIPT}"
# The merge script turns on errexit for its own main body. Cases below capture
# exit codes explicitly instead, and re-enable errexit inside their subshell.
set +e

tmproot="$(mktemp -d "${TMPDIR:-/tmp}/fleet-merge-squash.XXXXXX")"
if [[ ! -d "${tmproot}" ]]; then
  echo "FAIL  could not create a temp dir for the scratch repos" >&2
  exit 1
fi
trap 'rm -rf "${tmproot}"' EXIT

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want:\n%s\n  got:\n%s\n' "${label}" "${want}" "${got}"
  fi
}

# new_repo <dir>: a scratch repo with one seed commit. Prints nothing; the
# seed commit is the rewrite base.
new_repo() {
  local dir="$1"
  mkdir -p "${dir}"
  git -C "${dir}" init -q -b main
  git -C "${dir}" config user.email "executor@example.test"
  git -C "${dir}" config user.name "Scratch Executor"
  git -C "${dir}" config commit.gpgsign false
  printf 'seed\n' >"${dir}/README.md"
  git -C "${dir}" add README.md
  git -C "${dir}" commit -q -s -m "chore: seed the scratch repo"
}

# commit_in <dir> <file> <subject> <body> <author>: one commit adding <file>,
# signed off by the repo identity so trailer handling is exercised.
commit_in() {
  local dir="$1" file="$2" subject="$3" body="$4" author="$5"
  printf '%s\n' "${file}" >"${dir}/${file}"
  git -C "${dir}" add -A
  if [[ -n "${body}" ]]; then
    git -C "${dir}" commit -q -s --author="${author}" -m "${subject}" -m "${body}"
  else
    git -C "${dir}" commit -q -s --author="${author}" -m "${subject}"
  fi
}

# rewrite <dir> <base>: run the rewrite over <base>..HEAD in <dir> and print
# the resulting sha. Progress and git chatter go to <dir>/rewrite.log.
rewrite() {
  local dir="$1" base="$2"
  (
    cd "${dir}" || exit 1
    set -e
    fleet_rewrite_history "${base}" "$(git rev-parse HEAD)" "_rewrite_test" "task-1234"
  ) 2>"${dir}/rewrite.log"
}

# The three assertions each case makes: the commit shape (subject + author,
# oldest first), the trailers of the final commit, and the files the rewrite
# carried into the tree.
shape_of() { git -C "$1" log --reverse --format='%s | %an <%ae>' "$2..$3"; }
trailers_of() { git -C "$1" log -1 --format=%B "$2" | git interpret-trailers --parse; }
files_of() { git -C "$1" ls-tree -r --name-only "$2"; }

WIP_AUTHOR="Wip Author <wip@example.test>"
DELIVERABLE_AUTHOR="Deliverable Author <deliverable@example.test>"
SIGNOFF="Signed-off-by: Scratch Executor <executor@example.test>"

run_case() {
  local label="$1" dir="$2" want_shape="$3" want_trailers="$4" want_files="$5"
  local base clean code=0
  base="$(git -C "${dir}" rev-parse "HEAD~$6")"
  clean="$(rewrite "${dir}" "${base}")" || code=$?
  if [[ ${code} -ne 0 || -z "${clean}" ]]; then
    fail=$((fail + 1))
    printf 'FAIL  %s: rewrite exited %s\n' "${label}" "${code}"
    cat "${dir}/rewrite.log"
    return
  fi
  printf '\n--- %s ---\n' "${label}"
  git -C "${dir}" log --reverse --format='commit %h  %an <%ae>%n%B----' "${base}..${clean}"
  check_eq "${label}: commit shape" "${want_shape}" "$(shape_of "${dir}" "${base}" "${clean}")"
  check_eq "${label}: trailers" "${want_trailers}" "$(trailers_of "${dir}" "${clean}")"
  check_eq "${label}: files" "${want_files}" "$(files_of "${dir}" "${clean}")"
}

# (a) A leading wip: commit folds forwards into the deliverable that follows.
# The deliverable's subject and author win; the wip's content and its
# wip-only Refs: trailer ride along. This is issue #659: the old rewrite
# reworded the wip to `chore:` and left it as a separate commit ahead of the
# deliverable (PR #658, b6181104 on #602).
dir="${tmproot}/wip-first"
new_repo "${dir}"
commit_in "${dir}" notes.txt "wip: doc comment for the new module" "Refs: #659" "${WIP_AUTHOR}"
commit_in "${dir}" deliverable.txt "feat(scratch): add the deliverable" "The real change." "${DELIVERABLE_AUTHOR}"
run_case "wip-first + feat" "${dir}" \
  "feat(scratch): add the deliverable | ${DELIVERABLE_AUTHOR}" \
  "${SIGNOFF}
Refs: #659" \
  "README.md
deliverable.txt
notes.txt" 2

# (b) A trailing wip: fixup still folds backwards into the deliverable it
# fixes up (the behavior that already worked; this pins it).
dir="${tmproot}/wip-last"
new_repo "${dir}"
commit_in "${dir}" deliverable.txt "feat(scratch): add the deliverable" "The real change." "${DELIVERABLE_AUTHOR}"
commit_in "${dir}" typo.txt "wip: fix a typo" "Refs: #659" "${WIP_AUTHOR}"
run_case "feat + wip-fixup" "${dir}" \
  "feat(scratch): add the deliverable | ${DELIVERABLE_AUTHOR}" \
  "${SIGNOFF}
Refs: #659" \
  "README.md
deliverable.txt
typo.txt" 2

# (c) A branch that is only wip: commits has nothing to fold forwards into,
# so it keeps the rename behavior: one commit, `wip:` stripped, `chore:`
# prepended, the first wip commit's author kept.
dir="${tmproot}/only-wip"
new_repo "${dir}"
commit_in "${dir}" notes.txt "wip: doc comment for the new module" "Refs: #659" "${WIP_AUTHOR}"
commit_in "${dir}" more.txt "wip: more notes" "" "${WIP_AUTHOR}"
run_case "only wip" "${dir}" \
  "chore: doc comment for the new module | ${WIP_AUTHOR}" \
  "Refs: #659
${SIGNOFF}" \
  "README.md
more.txt
notes.txt" 2

# (d) The forward fold consumes the FIRST deliverable only: a second
# substantive commit stays its own commit.
dir="${tmproot}/wip-first-two-feats"
new_repo "${dir}"
commit_in "${dir}" notes.txt "wip: doc comment for the new module" "Refs: #659" "${WIP_AUTHOR}"
commit_in "${dir}" deliverable.txt "feat(scratch): add the deliverable" "The real change." "${DELIVERABLE_AUTHOR}"
commit_in "${dir}" followup.txt "feat(scratch): add a follow-up" "More." "${DELIVERABLE_AUTHOR}"
run_case "wip-first + feat + feat" "${dir}" \
  "feat(scratch): add the deliverable | ${DELIVERABLE_AUTHOR}
feat(scratch): add a follow-up | ${DELIVERABLE_AUTHOR}" \
  "${SIGNOFF}" \
  "README.md
deliverable.txt
followup.txt
notes.txt" 3

# (e) A whitespace-only fmt fixup still squashes away (unchanged behavior; the
# fixup!-fold work must not disturb it). A `cargo fmt` reindent whose diff is
# empty under `git diff -w` is dropped, folded into the deliverable before it.
dir="${tmproot}/fmt-fixup"
new_repo "${dir}"
commit_in "${dir}" deliverable.txt "feat(scratch): add the deliverable" "The real change." "${DELIVERABLE_AUTHOR}"
# A reindent-only change to the same file: content identical under `git diff -w`.
printf 'deliverable.txt\n' >"${dir}/deliverable.txt"
printf '    deliverable.txt\n' >"${dir}/deliverable.txt"  # leading whitespace only
git -C "${dir}" add -A
git -C "${dir}" commit -q -s --author="${DELIVERABLE_AUTHOR}" -m "style: cargo fmt"
run_case "feat + fmt-fixup" "${dir}" \
  "feat(scratch): add the deliverable | ${DELIVERABLE_AUTHOR}" \
  "${SIGNOFF}" \
  "README.md
deliverable.txt" 2

# --- fixup!/squash! autosquash fold (issue #931) -------------------------
# fold_fixup_commits folds each fixup!/squash! commit into the commit it names,
# using git's autosquash, and fails loudly on an orphan whose target is absent.

# fold_at <dir> <base>: run fold_fixup_commits over <base>..HEAD in <dir>,
# print the resulting sha, git chatter + errors to <dir>/fold.log.
fold_at() {
  local dir="$1" base="$2"
  (
    cd "${dir}" || exit 1
    set -e
    fold_fixup_commits "${base}" "$(git rev-parse HEAD)" "_fold_test"
  ) 2>"${dir}/fold.log"
}

subjects_of() { git -C "$1" log --reverse --format='%s' "$2..$3"; }

# run_fold_case <label> <dir> <want-subjects> <want-files> <depth>: assert a
# successful fold's resulting subjects (oldest first) and tree files.
run_fold_case() {
  local label="$1" dir="$2" want_subjects="$3" want_files="$4" depth="$5"
  local base clean code=0
  base="$(git -C "${dir}" rev-parse "HEAD~${depth}")"
  clean="$(fold_at "${dir}" "${base}")" || code=$?
  if [[ ${code} -ne 0 || -z "${clean}" ]]; then
    fail=$((fail + 1))
    printf 'FAIL  %s: fold exited %s\n' "${label}" "${code}"
    cat "${dir}/fold.log"
    return
  fi
  printf '\n--- %s ---\n' "${label}"
  git -C "${dir}" log --reverse --format='commit %h  %s' "${base}..${clean}"
  check_eq "${label}: subjects" "${want_subjects}" "$(subjects_of "${dir}" "${base}" "${clean}")"
  check_eq "${label}: files" "${want_files}" "$(files_of "${dir}" "${clean}")"
}

# (f) A fixup! naming an earlier commit on the same branch folds its content in;
# no fixup! subject survives.
dir="${tmproot}/fixup-adjacent"
new_repo "${dir}"
printf 'v1\n' >"${dir}/feature.txt"; git -C "${dir}" add -A
git -C "${dir}" commit -q -s --author="${DELIVERABLE_AUTHOR}" -m "feat(scratch): add the feature"
printf 'v1\nv2\n' >"${dir}/feature.txt"; git -C "${dir}" add -A
git -C "${dir}" commit -q -s --author="${DELIVERABLE_AUTHOR}" -m "fixup! feat(scratch): add the feature"
run_fold_case "fixup adjacent folds in" "${dir}" \
  "feat(scratch): add the feature" \
  "README.md
feature.txt" 2
check_eq "fixup adjacent: content folded" "v1
v2" "$(git -C "${dir}" show HEAD:feature.txt 2>/dev/null || cat "${dir}/feature.txt")"

# (g) A squash! naming an earlier commit folds the same way.
dir="${tmproot}/squash-adjacent"
new_repo "${dir}"
printf 'a\n' >"${dir}/s.txt"; git -C "${dir}" add -A
git -C "${dir}" commit -q -s --author="${DELIVERABLE_AUTHOR}" -m "feat(scratch): add s"
printf 'a\nb\n' >"${dir}/s.txt"; git -C "${dir}" add -A
git -C "${dir}" commit -q -s --author="${DELIVERABLE_AUTHOR}" -m "squash! feat(scratch): add s"
run_fold_case "squash adjacent folds in" "${dir}" \
  "feat(scratch): add s" \
  "README.md
s.txt" 2

# (h) THE #929 SHAPE: fixup! as the LAST commit, naming the FIRST, with two
# substantive commits in between. Autosquash reorders it next to its target.
# A test that only covered adjacent commits would pass while this one failed.
dir="${tmproot}/fixup-pr929"
new_repo "${dir}"
printf 'base\n' >"${dir}/stats.txt"; git -C "${dir}" add -A
git -C "${dir}" commit -q -s --author="${DELIVERABLE_AUTHOR}" \
  -m "feat(catalog): carry an exact per-object integer sum in column stats"
commit_in "${dir}" two.txt "feat(scratch): second commit" "" "${DELIVERABLE_AUTHOR}"
commit_in "${dir}" three.txt "feat(scratch): third commit" "" "${DELIVERABLE_AUTHOR}"
printf 'base\nfolded\n' >"${dir}/stats.txt"; git -C "${dir}" add -A
git -C "${dir}" commit -q -s --author="${DELIVERABLE_AUTHOR}" \
  -m "fixup! feat(catalog): carry an exact per-object integer sum in column stats"
run_fold_case "#929 fixup last names first" "${dir}" \
  "feat(catalog): carry an exact per-object integer sum in column stats
feat(scratch): second commit
feat(scratch): third commit" \
  "README.md
stats.txt
three.txt
two.txt" 4
check_eq "#929: fixup content folded into first commit" "base
folded" "$(git -C "${dir}" show HEAD:stats.txt 2>/dev/null || true)"

# (i) An ORPHAN fixup! (target not on the branch) must FAIL, naming the commit.
dir="${tmproot}/fixup-orphan"
new_repo "${dir}"
commit_in "${dir}" real.txt "feat(scratch): a real commit" "" "${DELIVERABLE_AUTHOR}"
commit_in "${dir}" orphan.txt "fixup! feat(scratch): a commit that is not on this branch" "" "${DELIVERABLE_AUTHOR}"
orphan_sha="$(git -C "${dir}" rev-parse HEAD)"
base="$(git -C "${dir}" rev-parse HEAD~2)"
orphan_out="$(fold_at "${dir}" "${base}")"; orphan_rc=$?
if [[ ${orphan_rc} -ne 0 ]]; then
  pass=$((pass + 1)); printf 'ok    orphan fixup: fold failed (rc=%s)\n' "${orphan_rc}"
else
  fail=$((fail + 1)); printf 'FAIL  orphan fixup: fold succeeded, want failure (sha=%s)\n' "${orphan_out}"
fi
if grep -q "${orphan_sha:0:12}" "${dir}/fold.log" \
   && grep -qi 'name no commit on this branch' "${dir}/fold.log"; then
  pass=$((pass + 1)); printf 'ok    orphan fixup: error names the commit\n'
else
  fail=$((fail + 1)); printf 'FAIL  orphan fixup: error does not name the commit\n'
  cat "${dir}/fold.log"
fi

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
if [[ ${fail} -ne 0 ]]; then
  exit 1
fi
