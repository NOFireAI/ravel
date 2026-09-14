#!/usr/bin/env bash
# Cases for scripts/guards/check-duplicate-work.sh.
#
# The guard's whole value is the IDENTICAL branch, and its only real-world
# case (#1786 and #1788, two sessions bumping rustls for the same advisory
# twenty minutes apart) was closed as soon as it was found. So that branch
# cannot be exercised against live data, and a guard whose important path is
# never run is a guard nobody knows works. These cases build the collision
# synthetically instead.
#
# `gh` and the network are stubbed; the git side is real, against a scratch
# repository built per case, because patch-id over real commits is the thing
# under test and stubbing it would test nothing.
#
# Run by hand:   bash scripts/guards/check-duplicate-work.test.sh
set -euo pipefail

SCRIPT="${SCRIPT:-$(cd "$(dirname "$0")" && pwd)/check-duplicate-work.sh}"
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
  local label="$1" needle="$2" hay="$3"
  if printf '%s' "${hay}" | grep -qF -- "${needle}"; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n  missing: %s\n  in: %s\n' "${label}" "${needle}" "${hay}"
  fi
}

check_absent() {
  local label="$1" needle="$2" hay="$3"
  if printf '%s' "${hay}" | grep -qF -- "${needle}"; then
    fail=$((fail + 1)); printf 'FAIL  %s\n  unexpectedly present: %s\n' "${label}" "${needle}"
  else
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  fi
}

# Build a scratch repo with a main branch and two PR branches. `same` decides
# whether branch B makes byte-identical changes to branch A (the duplicate
# case) or a different change to the same file (the overlap case).
#
# The two branches always differ in commit message, author and branch name, so
# a passing IDENTICAL case proves patch-id is matching CONTENT and not any of
# the metadata a title-based check would have keyed on.
build_repo() {
  local dir="$1" same="$2"
  mkdir -p "${dir}"
  git -C "${dir}" init --quiet -b main
  git -C "${dir}" config user.email t@example.com
  git -C "${dir}" config user.name  T
  printf 'base\n' > "${dir}/shared.txt"
  printf 'other\n' > "${dir}/only-a.txt"
  git -C "${dir}" add -A
  git -C "${dir}" commit -qm 'base'

  git -C "${dir}" checkout -q -b pr-a main
  printf 'bumped\n' > "${dir}/shared.txt"
  git -C "${dir}" add -A
  git -C "${dir}" commit -qm 'fix(deps): bump the thing'

  git -C "${dir}" checkout -q -b pr-b main
  if [[ "${same}" == "same" ]]; then
    printf 'bumped\n' > "${dir}/shared.txt"
  else
    printf 'bumped differently\n' > "${dir}/shared.txt"
  fi
  git -C "${dir}" add -A
  git -C "${dir}" -c user.email=u@example.com -c user.name=U \
    commit -qm 'chore(deps): take the thing'

  git -C "${dir}" checkout -q main
  # The guard fetches `origin`; point it at itself so the fetch is a no-op
  # that succeeds, and pre-create the remote-tracking refs it resolves.
  git -C "${dir}" remote add origin "${dir}"
  git -C "${dir}" update-ref refs/remotes/origin/main main
  git -C "${dir}" fetch origin --quiet 2>/dev/null || true
}

# A `gh` stub: branch names and file lists per PR number, and a fixed list of
# open PRs.
write_gh_stub() {
  local bin="$1" files_b="$2"
  mkdir -p "${bin}"
  cat > "${bin}/gh" <<STUB
#!/usr/bin/env bash
# gh api repos/<o>/<r>/pulls/<n>/files --paginate --jq .[].path
# gh pr view <n> --json <fields> --jq <expr>   |   gh pr list ...
if [[ "\$1" == "api" ]]; then
  n="\$(printf '%s' "\$2" | sed 's#.*/pulls/##; s#/files\$##')"
  case "\${n}" in
    101) printf 'shared.txt\n' ;;
    102) printf '${files_b}\n' ;;
    *) exit 1 ;;
  esac
  exit 0
fi
if [[ "\$1" == "pr" && "\$2" == "list" ]]; then
  echo 101
  echo 102
  exit 0
fi
if [[ "\$1" == "pr" && "\$2" == "view" ]]; then
  n="\$3"
  for a in "\$@"; do case "\$a" in headRefName) want=branch;; files) want=files;; title) want=title;; esac; done
  case "\${want}:\${n}" in
    branch:101) echo pr-a ;;
    branch:102) echo pr-b ;;
    files:101)  printf 'shared.txt\n' ;;
    files:102)  printf '${files_b}\n' ;;
    title:101)  echo 'fix(deps): bump the thing' ;;
    title:102)  echo 'chore(deps): take the thing' ;;
    *) exit 1 ;;
  esac
  exit 0
fi
exit 1
STUB
  chmod +x "${bin}/gh"
}

run_case() {
  local same="$1" files_b="$2"
  local root; root="$(mktemp -d)"
  build_repo "${root}/repo" "${same}"
  write_gh_stub "${root}/bin" "${files_b}"
  set +e
  ( cd "${root}/repo" && PATH="${root}/bin:${PATH}" sh "${SCRIPT}" 101 origin 2>&1 )
  local rc=$?
  set -e
  rm -rf "${root}"
  return ${rc}
}

# --- identical change, different title/branch/author -> IDENTICAL, exit 1 ---
out="$(run_case same shared.txt || true)"
set +e
( run_case same shared.txt >/dev/null 2>&1 ); rc=$?
set -e
check_eq       "identical: exits 1"            "1" "${rc}"
check_contains "identical: reports IDENTICAL"  "IDENTICAL: #101 and #102" "${out}"
check_absent   "identical: not merely OVERLAP" "OVERLAP: #101 and #102"   "${out}"

# --- different change to the same file -> OVERLAP, not IDENTICAL ---
out="$(run_case diff shared.txt || true)"
check_contains "overlap: reports OVERLAP"      "OVERLAP: #101 and #102"   "${out}"
# Match the FINDING line, not the bare word: the guard's closing advice
# explains what IDENTICAL means, so a bare-word absence check matches the
# explanation and fails on a correct run. Caught by this case doing exactly
# that on its first run.
check_absent   "overlap: no IDENTICAL finding" "IDENTICAL: #101"          "${out}"
check_contains "overlap: names the file"       "shared.txt"               "${out}"
# Pin the exact count, not that some number appeared: the fixture shares
# exactly one file, and a count computed from a mis-parsed value would still
# have rendered something here.
check_contains "overlap: counts exactly one"   "share 1 file(s)"          "${out}"

# --- different change, no shared file -> clean, exit 0 ---
set +e
( run_case diff only-b.txt >/dev/null 2>&1 ); rc=$?
set -e
out="$(run_case diff only-b.txt || true)"
check_eq       "disjoint: exits 0"             "0" "${rc}"
check_contains "disjoint: says so"             "overlaps no other open pull request" "${out}"
# Both checks above passed against a guard that printed
# "[: Illegal number: 0\n0" to stderr once per non-overlapping PR: the exit
# code and the message were right, and only the noise was wrong, so nothing
# here noticed. run_case folds stderr into the output, so assert it is clean.
check_absent   "disjoint: no shell error noise" "Illegal number"     "${out}"
check_absent   "disjoint: no integer error"     "integer expression" "${out}"

# --- other side's diff unfetchable (fork PR, deleted branch) -> say so ---
# The IDENTICAL branch needs the other PR's head under refs/heads on this
# remote. A fork PR's head is not, and a deleted branch is gone, so patch_id_of
# returns empty. That is "could not compute", not "did not match": staying
# quiet would downgrade a real duplicate to the advisory OVERLAP signal with
# nothing saying why. Build exactly that case by deleting pr-b from the remote
# while the gh stub still reports its files.
root="$(mktemp -d)"
build_repo "${root}/repo" same
write_gh_stub "${root}/bin" shared.txt
git -C "${root}/repo" branch -D pr-b --quiet
set +e
out="$( cd "${root}/repo" && PATH="${root}/bin:${PATH}" sh "${SCRIPT}" 101 origin 2>&1 )"
rc=$?
set -e
rm -rf "${root}"
check_contains "unfetchable other: says it could not resolve" "could not resolve #102's diff" "${out}"
check_contains "unfetchable other: still reports the overlap" "OVERLAP: #101 and #102" "${out}"
check_absent   "unfetchable other: claims no IDENTICAL"       "IDENTICAL: #101" "${out}"
check_eq       "unfetchable other: still exits 1"             "1" "${rc}"

# --- a missing pull request is 'could not tell' (2), never 'clean' (0) ---
root="$(mktemp -d)"
build_repo "${root}/repo" diff
write_gh_stub "${root}/bin" shared.txt
cat > "${root}/bin/gh" <<'STUB'
#!/usr/bin/env bash
exit 1
STUB
chmod +x "${root}/bin/gh"
set +e
( cd "${root}/repo" && PATH="${root}/bin:${PATH}" sh "${SCRIPT}" 999 origin >/dev/null 2>&1 ); rc=$?
set -e
rm -rf "${root}"
check_eq "unresolvable pull request exits 2, not 0" "2" "${rc}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ "${fail}" -eq 0 ]]
