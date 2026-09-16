#!/usr/bin/env bash
# Intent-first dispatch bookkeeping on the epic ledger.
#
# The dispatch itself goes through the fleet_dispatch MCP tool; this
# script enforces the ordering around it. Record intent before the
# dispatch, refuse a new intent while one for the same ticket is
# unresolved, and record the outcome after. A start push that dies on a
# control-plane 5xx then leaves a record instead of a ghost task, and a
# retry cannot double-dispatch.
#
# Usage:
#   fleet-dispatch-intent.sh intent <epic-issue> <ticket> <ref-sha>
#       Refuses on a dangling intent (exit 65) or on an unreadable intent
#       history (exit 69: UNKNOWN is not clean), runs the fresh-ref guard
#       and the duplicate-work guard, posts a dispatch-intent comment,
#       prints the nonce.
#       Env: DISPATCH_PATHS="a,b" gives the duplicate-work guard the files
#       the task is predicted to touch, so an open pull request already on
#       them refuses (66) instead of two divergent rewrites of one file.
#       DISPATCH_SKIP_DUPLICATE_CHECK=1 for a deliberate second dispatch.
#   fleet-dispatch-intent.sh record <epic-issue> <nonce> <task-id>
#   fleet-dispatch-intent.sh failed <epic-issue> <nonce> [reason...]
#
# This script writes COMMENTS. epic-status.sh reads the issue BODY and
# nothing else, so a task recorded only here is invisible to the
# reconciliation that exists to catch a silently dead task: it reports
# nothing wrong because it never saw the task at all. Step 5 below is what
# closes that, and it is not optional if you rely on epic-status.sh.
#
# Flow in the orchestrator turn:
#   1. sha=$(git fetch origin main -q && git rev-parse origin/main)
#   2. nonce=$(scripts/fleet-dispatch-intent.sh intent <epic> <ticket> "$sha")
#   3. call fleet_dispatch with ref=$sha
#   4a. scripts/fleet-dispatch-intent.sh record <epic> "$nonce" <task-id>
#   4b. on error: scripts/fleet-dispatch-intent.sh failed <epic> "$nonce" <why>
#   5. append the task id to the epic BODY's ledger, in the form
#      `- #<ticket> task=<uuid> <status>`, then re-run
#      `scripts/epic-status.sh <epic> --fresh` and confirm the id appears
#      (an in-flight task reads `start=yes result=no`). Any body line
#      containing the UUID works; epic-status.sh scans the whole body, so a
#      dedicated section is a convenience, not a requirement.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $# -lt 3 ]]; then
  echo "usage: $0 intent <epic-issue> <ticket> <ref-sha>" >&2
  echo "       $0 record <epic-issue> <nonce> <task-id>" >&2
  echo "       $0 failed <epic-issue> <nonce> [reason...]" >&2
  exit 64
fi

mode="$1"
epic="$2"
shift 2

case "${mode}" in
  intent)
    if [[ $# -lt 2 ]]; then
      echo "fleet-dispatch-intent.sh: intent needs <ticket> <ref-sha>" >&2
      exit 64
    fi
    ticket="$1"
    ref_sha="$2"

    # A dangling intent = an intent comment for this ticket with no
    # matching record/failed comment. Scan the most recent 100 comments.
    #
    # A failed read is UNKNOWN, not "no dangling intent". With `|| true` here
    # an API failure produced an empty history, every intent looked clean,
    # and the guard that exists to stop a double dispatch waved it through
    # at exactly the moment GitHub was unreliable, which is when a dispatch
    # is most likely to be a retry of one that already started.
    comments_rc=0
    comments=$(gh issue view "${epic}" --json comments \
      --jq '.comments[-100:][].body' 2>/dev/null) || comments_rc=$?
    if [[ ${comments_rc} -ne 0 ]]; then
      echo "fleet-dispatch-intent.sh: could not read comments on epic #${epic} (gh exit ${comments_rc})." >&2
      echo "  Intent history is UNKNOWN, which is not the same as clean. Refusing to dispatch;" >&2
      echo "  re-run once gh works, or reconcile with scripts/epic-status.sh ${epic} --fresh." >&2
      exit 69
    fi
    dangling=""
    while IFS= read -r nonce_line; do
      n="${nonce_line#dispatch-intent nonce=}"
      n="${n%% *}"
      [[ -z "${n}" ]] && continue
      if ! grep -qE "^dispatch-(record|failed) nonce=${n}( |$)" <<<"${comments}"; then
        dangling="${n}"
      fi
    done < <(grep -E "^dispatch-intent nonce=[a-z0-9-]+ ticket=${ticket}( |$)" <<<"${comments}" || true)

    if [[ -n "${dangling}" ]]; then
      echo "fleet-dispatch-intent.sh: dangling intent ${dangling} for ${ticket} on epic #${epic}." >&2
      echo "  A previous dispatch of this ticket has no recorded outcome. Reconcile" >&2
      echo "  first: check fleet_status / scripts/epic-status.sh ${epic}, then mark it" >&2
      echo "  with 'record <task-id>' or 'failed' before dispatching again." >&2
      exit 65
    fi

    "${script_dir}/guards/assert-fresh-dispatch-ref.sh" "${ref_sha}" >&2

    # Is someone already doing this? Asked HERE because this is the one
    # chokepoint every dispatch passes through; a rule that lives only in
    # prose is a rule that holds until the first hurried session. A ticket
    # that is not a plain issue number (a task label, a free-form string)
    # is not something the guard can look up, so it is skipped rather than
    # guessed at.
    #
    # Exit 65 (a pull request already addresses the issue) and 66 (an open
    # pull request is already touching the predicted files) both refuse.
    # 69 is "could not ask", which refuses too: the moment GitHub is
    # unreadable is the moment a dispatch is most likely to be a retry of
    # one that already started, and this script already takes that line on
    # its own history read above.
    #
    # DISPATCH_SKIP_DUPLICATE_CHECK=1 proceeds anyway, for a deliberate
    # second task on one ticket (a fix round, a continuation after a
    # ceiling kill). Say so in the spec when you use it.
    if [[ "${ticket}" =~ ^#?[0-9]+$ && "${DISPATCH_SKIP_DUPLICATE_CHECK:-0}" != "1" ]]; then
      dup_rc=0
      # An array rather than `${VAR:+--paths "${VAR}"}`. Both are correct under
      # bash, which is what runs this file: the inner quotes group, so a value
      # containing a space stays one argument. They are NOT equivalent under
      # zsh, where the whole alternate collapses into a single `--paths value`
      # argument that the guard rejects with 64. The array behaves the same in
      # both, and reading it needs no knowledge of which shell got here.
      dup_args=(--issue "${ticket#\#}")
      [[ -n "${DISPATCH_PATHS:-}" ]] && dup_args+=(--paths "${DISPATCH_PATHS}")
      "${script_dir}/guards/assert-no-duplicate-dispatch.sh" "${dup_args[@]}" >&2 || dup_rc=$?
      if [[ ${dup_rc} -ne 0 ]]; then
        echo "fleet-dispatch-intent.sh: duplicate-work guard exited ${dup_rc}; refusing to dispatch." >&2
        echo "  65 = a pull request already addresses #${ticket#\#}; 66 = an open pull request is on" >&2
        echo "  those files; 69 = the question could not be asked, which is not a clean answer." >&2
        echo "  Deliberate second dispatch on this ticket: DISPATCH_SKIP_DUPLICATE_CHECK=1." >&2
        exit "${dup_rc}"
      fi
    fi

    nonce="$(date +%s)-$$"
    gh issue comment "${epic}" --body \
      "dispatch-intent nonce=${nonce} ticket=${ticket} ref=${ref_sha}" >/dev/null
    echo "${nonce}"
    ;;

  record)
    if [[ $# -lt 2 ]]; then
      echo "fleet-dispatch-intent.sh: record needs <nonce> <task-id>" >&2
      exit 64
    fi
    gh issue comment "${epic}" --body \
      "dispatch-record nonce=$1 task=$2" >/dev/null
    echo "recorded task $2 against intent $1 on epic #${epic}"
    ;;

  failed)
    if [[ $# -lt 1 ]]; then
      echo "fleet-dispatch-intent.sh: failed needs <nonce> [reason...]" >&2
      exit 64
    fi
    nonce="$1"
    shift
    reason="${*:-unspecified}"
    gh issue comment "${epic}" --body \
      "dispatch-failed nonce=${nonce} reason: ${reason}" >/dev/null
    echo "marked intent ${nonce} failed on epic #${epic}"
    ;;

  *)
    echo "fleet-dispatch-intent.sh: unknown mode '${mode}' (intent|record|failed)" >&2
    exit 64
    ;;
esac
