#!/bin/sh
# check-duplicate-work.sh <pr-number> [remote]
# Report open pull requests whose work overlaps <pr-number>'s, before you start
# on it. Exit 0 when nothing overlaps; exit 1 when something does; exit 2 when
# the check could not run (bad argument, no such pull request, gh or git
# failed).
#
# Several Claude sessions work this repository at once through one shared `gh`
# account, so a pull request's author never says which session owns it, and a
# branch sitting idle for hours says nothing about whether its session is alive.
# Two collisions on 2026-09-14 came from guessing at both:
#
#   #1786 and #1788 both bumped rustls for RUSTSEC-2026-0285, opened twenty
#   minutes apart by two sessions, and sat in the merge queue together. Their
#   titles differ ("fix(deps): bump rustls ..." and "chore(deps): take rustls
#   ..."), so nothing short of comparing the diffs would have matched them.
#   Under the queue's ALLGREEN batching the second would have rebased to an
#   empty diff, which is an expensive thing to diagnose from a merge_group run.
#
#   Five fix rounds were dispatched against pull requests whose session was
#   awake and already fixing the same review findings. The rounds were built on
#   heads that moved minutes later, so landing one would have reverted the
#   other session's work while looking like an ordinary landing.
#
# Two signals, because they catch different things:
#
#   IDENTICAL: `git patch-id --stable` over each pull request's diff against
#   the merge base. Two branches that make the same change hash the same
#   regardless of title, branch name, commit message or author. This is the
#   duplicate-work signal.
#
#   OVERLAP: a shared file. Not a problem by itself -- concurrent work touches
#   common files all the time, and the merge queue rebases and re-tests
#   whichever lands second -- but it tells you whose toes you are near, and a
#   large overlap is worth a message before a merge rather than after. #1778
#   and #1436 share 42 files because each adds one ServerConfig field and a
#   field addition touches every literal construction site.
#
# This guard reports; it does not refuse. A duplicate is a conversation with
# another session, not a mechanical failure, and the right resolution (close
# one, split the work, merge order) depends on facts the script cannot see.
set -eu

pr="${1:-}"
remote="${2:-origin}"
if [ -z "${pr}" ]; then
    echo "usage: check-duplicate-work.sh <pr-number> [remote]" >&2
    exit 2
fi

repo="NOFireAI/ravel"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT INT TERM
tmp_mine="${tmp_dir}/mine"
tmp_other="${tmp_dir}/other"
tmp_shared="${tmp_dir}/shared"

# Fetch in THIS invocation: a base resolved from a stale local ref compares the
# wrong trees, and the whole point here is what is true right now.
git fetch "${remote}" main --quiet || {
    echo "check-duplicate-work: could not fetch ${remote}/main" >&2
    exit 2
}

pr_branch() {
    gh pr view "$1" --repo "${repo}" --json headRefName --jq .headRefName 2>/dev/null
}

# A pull request's diff, as a patch-id. Empty output means it could not be
# computed, which the caller must not read as "no match".
patch_id_of() {
    _br="$(pr_branch "$1")" || return 1
    [ -n "${_br}" ] || return 1
    git fetch "${remote}" "refs/heads/${_br}:refs/remotes/${remote}/_dupchk_$1" \
        --quiet --force 2>/dev/null || return 1
    _base="$(git merge-base "${remote}/main" "${remote}/_dupchk_$1" 2>/dev/null)" || return 1
    git diff "${_base}" "${remote}/_dupchk_$1" 2>/dev/null | git patch-id --stable | cut -d' ' -f1
}

files_of() {
    gh pr view "$1" --repo "${repo}" --json files --jq '[.files[].path] | sort | .[]' 2>/dev/null
}

mine_id="$(patch_id_of "${pr}" || true)"
if [ -z "${mine_id}" ]; then
    echo "check-duplicate-work: could not resolve a diff for #${pr}" >&2
    exit 2
fi
mine_files="$(files_of "${pr}" || true)"
if [ -z "${mine_files}" ]; then
    echo "check-duplicate-work: could not list files for #${pr}" >&2
    exit 2
fi

others="$(gh pr list --repo "${repo}" --state open --limit 100 --json number --jq '.[].number' 2>/dev/null)" || {
    echo "check-duplicate-work: could not list open pull requests" >&2
    exit 2
}

found=0
for other in ${others}; do
    [ "${other}" = "${pr}" ] && continue

    other_id="$(patch_id_of "${other}" || true)"
    if [ -n "${other_id}" ] && [ "${other_id}" = "${mine_id}" ]; then
        echo "IDENTICAL: #${pr} and #${other} make the same change (patch-id ${mine_id})"
        echo "    $(gh pr view "${other}" --repo "${repo}" --json title --jq .title 2>/dev/null)"
        echo "    One of them should close. Prefer the one opened first unless its"
        echo "    description or history is worse; say which and why on the one you close."
        found=1
        continue
    fi

    other_files="$(files_of "${other}" || true)"
    [ -n "${other_files}" ] || continue
    # Temp files rather than process substitution: this runs under /bin/sh,
    # where `<(...)` is a syntax error rather than a portability nicety.
    printf '%s\n' "${mine_files}" > "${tmp_mine}"
    printf '%s\n' "${other_files}" > "${tmp_other}"
    comm -12 "${tmp_mine}" "${tmp_other}" > "${tmp_shared}" 2>/dev/null || true
    shared="$(grep -c . "${tmp_shared}" 2>/dev/null || echo 0)"
    if [ "${shared}" -gt 0 ]; then
        echo "OVERLAP: #${pr} and #${other} share ${shared} file(s)"
        head -5 "${tmp_shared}" | sed 's/^/        /'
        echo "    $(gh pr view "${other}" --repo "${repo}" --json title --jq .title 2>/dev/null)"
        found=1
    fi
done

if [ "${found}" -eq 0 ]; then
    echo "check-duplicate-work: #${pr} overlaps no other open pull request"
    exit 0
fi

echo ""
echo "Overlap is not a blocker: the merge queue rebases and re-tests whichever"
echo "lands second. IDENTICAL is a blocker, and a large overlap is worth a word"
echo "with the other session before either merges."
exit 1
