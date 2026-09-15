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
# The per-PR fetches below write refs/remotes/<remote>/_dupchk_<n>. Drop them on
# the way out: --force keeps any one PR number fresh, but a run over the whole
# backlog leaves one ref per PR behind in the real checkout's namespace.
cleanup() {
    rm -rf "${tmp_dir}"
    git for-each-ref --format='%(refname)' "refs/remotes/${remote}/_dupchk_*" 2>/dev/null |
        while read -r _stale; do
            git update-ref -d "${_stale}" 2>/dev/null || true
        done
}
trap cleanup EXIT INT TERM
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

# The REST files endpoint with --paginate rather than `gh pr view --json files`:
# that field caps at about 100 entries, so a wide PR would silently drop paths
# and undercount the overlap, or miss a shared file sitting past the cap on
# either side. Same silent-cap class the scan note above exists for, and a
# field-addition PR reaches 100 files easily.
files_of() {
    # `.filename`, NOT `.path`: this endpoint and `gh pr view --json files` name
    # the same thing differently. Switching to REST for its pagination while
    # keeping the old field name yields empty for every pull request, which
    # turns every comparison into "could not list files" and disables OVERLAP
    # entirely.
    _files="$(gh api "repos/${repo}/pulls/$1/files" --paginate --jq '.[].filename' 2>/dev/null | sort)"
    if [ -n "${_files}" ]; then
        _count="$(printf '%s\n' "${_files}" | wc -l | tr -d '[:space:]')"
        # The endpoint itself stops at 3000 files and --paginate cannot follow
        # past that, so at the ceiling the list is truncated rather than
        # complete. Implausible for a real PR, but an OVERLAP miss that reports
        # two branches as disjoint is the one outcome worth never doing
        # silently, however unlikely the input.
        if [ "${_count}" -ge 3000 ]; then
            echo "check-duplicate-work: NOTE: #$1 returned ${_count} files, the REST" >&2
            echo "    endpoint's ceiling. Its file list is truncated, so an OVERLAP" >&2
            echo "    against it may be undercounted or missed entirely." >&2
        fi
    fi
    printf '%s\n' "${_files}"
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

scan_limit=300
others="$(gh pr list --repo "${repo}" --state open --limit "${scan_limit}" --json number --jq '.[].number' 2>/dev/null)" || {
    echo "check-duplicate-work: could not list open pull requests" >&2
    exit 2
}
# Say so when the scan is capped rather than reporting a clean result that only
# means "no duplicate among the first N": a silent cap turns "I did not find
# one" into "there is not one", which is the claim this guard must never make.
# `printf '%s\n' ""` still emits one newline, so an empty list would count as 1.
if [ -z "${others}" ]; then
    scanned=0
else
    scanned="$(printf '%s\n' "${others}" | wc -l | tr -d '[:space:]')"
    [ -n "${scanned}" ] || scanned=0
fi
if [ "${scanned}" -ge "${scan_limit}" ]; then
    echo "check-duplicate-work: NOTE: compared only the first ${scan_limit} open" >&2
    echo "    pull requests; more are open and were NOT compared, so a clean" >&2
    echo "    result here does not rule out a duplicate past that cap." >&2
fi

found=0
unread=0
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
    # A pull request whose file list could not be read is one this run did NOT
    # compare, so skipping it quietly would let the clean message below claim a
    # comparison that never happened. Count it and say so; the endpoint returns
    # nothing on a transient 5xx as readily as on a genuinely empty diff.
    if [ -z "${other_files}" ]; then
        unread=$((unread + 1))
        echo "check-duplicate-work: NOTE: could not read #${other}'s file list, so" >&2
        echo "    it was NOT compared. Any clean result below excludes it." >&2
        continue
    fi
    # An empty other_id means the diff could not be COMPUTED, not that it did
    # not match: a fork PR's head is not under refs/heads on this remote, and a
    # merged PR's branch may be gone. Staying quiet would downgrade a true
    # duplicate from the blocker signal to the advisory one with nothing to say
    # why, which is the same could-not-ask/asked-and-got-nothing collapse the
    # mine_id path above refuses to make.
    if [ -z "${other_id}" ]; then
        echo "check-duplicate-work: NOTE: could not resolve #${other}'s diff (a fork" >&2
        echo "    PR's head, or a deleted branch), so it was compared on file paths" >&2
        echo "    only. A true duplicate would read as OVERLAP here, not IDENTICAL." >&2
    fi
    # Temp files rather than process substitution: this runs under /bin/sh,
    # where `<(...)` is a syntax error rather than a portability nicety.
    printf '%s\n' "${mine_files}" > "${tmp_mine}"
    printf '%s\n' "${other_files}" > "${tmp_other}"
    comm -12 "${tmp_mine}" "${tmp_other}" > "${tmp_shared}" 2>/dev/null || true
    # `grep -c` prints 0 AND exits 1 when it matches nothing, so the obvious
    # `$(grep -c . f || echo 0)` yields "0\n0" and the -gt below dies with
    # "Illegal number" on every PR that shares no file. wc emits one number.
    shared="$(wc -l < "${tmp_shared}" 2>/dev/null | tr -d '[:space:]')"
    [ -n "${shared}" ] || shared=0
    if [ "${shared}" -gt 0 ]; then
        echo "OVERLAP: #${pr} and #${other} share ${shared} file(s)"
        head -5 "${tmp_shared}" | sed 's/^/        /'
        echo "    $(gh pr view "${other}" --repo "${repo}" --json title --jq .title 2>/dev/null)"
        found=1
    fi
done

if [ "${found}" -eq 0 ]; then
    if [ "${unread}" -gt 0 ]; then
        # Qualify rather than claim. "Overlaps nothing" is a statement about
        # every open pull request; what this run actually established is a
        # statement about the ones it could read.
        echo "check-duplicate-work: #${pr} overlaps none of the open pull requests"
        echo "    this run could read; ${unread} could NOT be read and were skipped"
        echo "    (see the NOTEs above). This is not a clean bill for those."
        exit 2
    fi
    echo "check-duplicate-work: #${pr} overlaps no other open pull request"
    exit 0
fi

echo ""
echo "Overlap is not a blocker: the merge queue rebases and re-tests whichever"
echo "lands second. IDENTICAL is a blocker, and a large overlap is worth a word"
echo "with the other session before either merges."
exit 1
