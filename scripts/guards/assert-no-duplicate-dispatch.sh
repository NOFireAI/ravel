#!/usr/bin/env bash
# Refuse to dispatch work that someone is already doing.
#
# This asks the question BEFORE a branch exists: given an issue number and
# the files a task is predicted to touch, is there already a pull request
# that addresses that issue, or one in flight over those files? It is the
# pre-dispatch half of the duplicate-work problem. The post-hoc half, two
# pull requests that already exist and turn out to make the same change,
# is scripts/guards/check-duplicate-work.sh, which compares patch-ids;
# neither can answer the other's question, because a task about to be
# dispatched has no diff to hash yet.
#
# The file list per candidate comes from the paginated pulls/<n>/files
# endpoint rather than `gh pr list --json files`, which caps at 100 entries
# and would report a wide pull request as touching none of the paths asked
# about: a false clean, which is the only wrong answer that costs anything
# here.
#
# Usage:
#   assert-no-duplicate-dispatch.sh --issue <n> [--paths a,b,c] [--paths d]
#                                   [--closed-days N] [--repo owner/name]
#
# Exit codes:
#   0   nothing overlapping; dispatch
#   64  usage
#   65  an existing pull request already addresses the issue; skip and log
#   66  an OPEN pull request is already touching the predicted files
#   69  the question could not be asked (gh failed); not the same as a clean
#       answer, and never report it as one
set -euo pipefail

issue=""
paths=()
closed_days=14
repo=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --issue) issue="${2:-}"; shift 2 ;;
    --paths)
      [[ -n "${2:-}" ]] || { echo "assert-no-duplicate-dispatch.sh: --paths needs a value" >&2; exit 64; }
      IFS=',' read -r -a chunk <<<"$2"
      paths+=("${chunk[@]}")
      shift 2
      ;;
    --closed-days) closed_days="${2:-}"; shift 2 ;;
    --repo) repo="${2:-}"; shift 2 ;;
    *) echo "assert-no-duplicate-dispatch.sh: unexpected argument '$1'" >&2; exit 64 ;;
  esac
done

if [[ -z "${issue}" ]]; then
  echo "usage: assert-no-duplicate-dispatch.sh --issue <n> [--paths a,b] [--closed-days N]" >&2
  exit 64
fi
[[ "${issue}" =~ ^[0-9]+$ ]] || { echo "assert-no-duplicate-dispatch.sh: --issue must be a number" >&2; exit 64; }
[[ "${closed_days}" =~ ^[0-9]+$ ]] || { echo "assert-no-duplicate-dispatch.sh: --closed-days must be a number" >&2; exit 64; }

if [[ -z "${repo}" ]]; then
  repo="$(gh repo view --json nameWithOwner --jq .nameWithOwner 2>/dev/null || true)"
  [[ -n "${repo}" ]] || { echo "assert-no-duplicate-dispatch.sh: could not resolve the repository" >&2; exit 69; }
fi

# A failed query must not read as "nothing found". Every gh call below
# either produces data or exits 69.
ask() {
  local out
  if ! out="$("$@" 2>/dev/null)"; then
    echo "assert-no-duplicate-dispatch.sh: '$*' failed; refusing to report a clean answer" >&2
    exit 69
  fi
  printf '%s' "${out}"
}

issue_json="$(ask gh issue view "${issue}" --repo "${repo}" --json number,state,title,stateReason)"
issue_state="$(jq -r '.state' <<<"${issue_json}")"
issue_title="$(jq -r '.title' <<<"${issue_json}")"

if [[ "${issue_state}" == "CLOSED" ]]; then
  echo "SKIP  issue #${issue} is already CLOSED (${issue_title})"
  exit 65
fi

cutoff_epoch=$(( $(date +%s) - closed_days * 86400 ))

prs_json="$(ask gh pr list --repo "${repo}" --state all --limit 200 \
  --json number,title,state,body,headRefName,updatedAt,mergedAt,closedAt)"

# Candidates: every open pull request, plus anything closed or merged inside
# the window. A merged pull request from last week is exactly the thing that
# already did the work.
candidates="$(jq -r --argjson cutoff "${cutoff_epoch}" '
  [ .[] | select(
      .state == "OPEN"
      or ((.mergedAt // .closedAt // "") != ""
          and ((.mergedAt // .closedAt) | fromdateiso8601) >= $cutoff)
    ) ]
  | .[] | "\(.number)\t\(.state)\t\(.title)"' <<<"${prs_json}")"

# A pull request that CLOSES the issue, not one that merely cites it.
#
# This repository's commit convention makes the distinction load-bearing:
# `Fixes: #N` resolves the issue, `Refs: #N` says related and explicitly does
# not close it. Treating both as "already addressed" refuses genuine first
# dispatches -- measured on issue #1790, a coordination issue that two pull
# requests cite with `Refs:` and neither resolves; the guard refused it and
# pushed the operator toward DISPATCH_SKIP_DUPLICATE_CHECK=1, which trains
# exactly the reflex that flag exists to avoid. A guard worked around by
# habit has stopped being a guard.
#
# The keyword list is GitHub's own closing set, so what refuses here is what
# GitHub would actually close on merge.
closing_kw='([Cc]los(e[sd]?|ing)|[Ff]ix(e[sd]|ing)?|[Rr]esolv(e[sd]?|ing))'
addressing="$(jq -r --arg needle "#${issue}" --arg kw "${closing_kw}" \
  --argjson cutoff "${cutoff_epoch}" '
  [ .[] | select(
      .state == "OPEN"
      or ((.mergedAt // .closedAt // "") != ""
          and ((.mergedAt // .closedAt) | fromdateiso8601) >= $cutoff)
    )
    | select(((.body // "") + " " + (.title // ""))
        | test($kw + "[:]?[[:space:]]+" + $needle + "([^0-9]|$)"))
  ] | .[] | "\(.number)\t\(.state)\t\(.title)"' <<<"${prs_json}")"

# Cited but not closed. Worth knowing, never a refusal.
mentioning="$(jq -r --arg needle "#${issue}" --arg kw "${closing_kw}" \
  --argjson cutoff "${cutoff_epoch}" '
  [ .[] | select(
      .state == "OPEN"
      or ((.mergedAt // .closedAt // "") != ""
          and ((.mergedAt // .closedAt) | fromdateiso8601) >= $cutoff)
    )
    | select(((.body // "") + " " + (.title // ""))
        | test("(^|[^0-9A-Za-z])" + $needle + "([^0-9]|$)"))
    | select((((.body // "") + " " + (.title // ""))
        | test($kw + "[:]?[[:space:]]+" + $needle + "([^0-9]|$)")) | not)
  ] | .[] | "\(.number)\t\(.state)\t\(.title)"' <<<"${prs_json}")"

if [[ -n "${addressing}" ]]; then
  echo "SKIP  issue #${issue} is already addressed:"
  while IFS=$'\t' read -r num state title; do
    [[ -z "${num}" ]] && continue
    echo "      #${num} [${state}] ${title}"
  done <<<"${addressing}"
  echo "      Do not dispatch a second task for it. Land, review or close that pull request instead."
  exit 65
fi

if [[ -n "${mentioning}" ]]; then
  echo "NOTE  issue #${issue} is cited by pull request(s) that do not close it:"
  while IFS=$'\t' read -r num state title; do
    [[ -z "${num}" ]] && continue
    echo "      #${num} [${state}] ${title}"
  done <<<"${mentioning}"
  echo "      Not a blocker. Read them before dispatching so the work is not redone."
fi

if ((${#paths[@]} == 0)); then
  echo "OK    no pull request closes issue #${issue}; no paths given, so no overlap check ran"
  exit 0
fi

collision=0
overlap_report=""
while IFS=$'\t' read -r num state title; do
  [[ -z "${num}" ]] && continue
  files="$(ask gh api "repos/${repo}/pulls/${num}/files" --paginate --jq '.[].filename')"
  shared=""
  for want in "${paths[@]}"; do
    [[ -z "${want}" ]] && continue
    while IFS= read -r have; do
      [[ -z "${have}" ]] && continue
      if [[ "${have}" == "${want}" ]]; then
        shared+="${have} "
      fi
    done <<<"${files}"
  done
  if [[ -n "${shared}" ]]; then
    overlap_report+="      #${num} [${state}] ${title}"$'\n'"        shared: ${shared% }"$'\n'
    [[ "${state}" == "OPEN" ]] && collision=1
  fi
done <<<"${candidates}"

if [[ -z "${overlap_report}" ]]; then
  echo "OK    issue #${issue}: no referencing pull request, no file overlap across ${#paths[@]} predicted path(s)"
  exit 0
fi

if ((collision == 1)); then
  echo "COLLIDE  an OPEN pull request is already touching these files:"
  printf '%s' "${overlap_report}"
  echo "         Dispatching over them produces two divergent rewrites of the same code."
  echo "         Wait for it to land, or fold the work into that pull request."
  exit 66
fi

echo "NOTE  recently closed pull requests touched these files; nothing open:"
printf '%s' "${overlap_report}"
echo "      Not a blocker. Read them before dispatching so the work is not redone."
exit 0
