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
#       Refuses on a dangling intent (exit 65), runs the fresh-ref guard,
#       posts a dispatch-intent comment, prints the nonce.
#   fleet-dispatch-intent.sh record <epic-issue> <nonce> <task-id>
#   fleet-dispatch-intent.sh failed <epic-issue> <nonce> [reason...]
#
# Flow in the orchestrator turn:
#   1. sha=$(git fetch origin main -q && git rev-parse origin/main)
#   2. nonce=$(scripts/fleet-dispatch-intent.sh intent <epic> <ticket> "$sha")
#   3. call fleet_dispatch with ref=$sha
#   4a. scripts/fleet-dispatch-intent.sh record <epic> "$nonce" <task-id>
#   4b. on error: scripts/fleet-dispatch-intent.sh failed <epic> "$nonce" <why>
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

    # The ticket must be claimed (assigned) before any work is dispatched on
    # it. Runs before the marker comment is posted, so a failed claim check
    # leaves no dangling intent behind: a caller can fix the claim and retry
    # without tripping the dangling-intent refusal below. set -e propagates the
    # guard's own non-zero exit and its stderr message.
    "${script_dir}/guards/assert-issue-claimed.sh" "${ticket}" >&2

    # A dangling intent = an intent comment for this ticket with no
    # matching record/failed comment. Scan the most recent 100 comments.
    comments=$(gh issue view "${epic}" --json comments \
      --jq '.comments[-100:][].body' 2>/dev/null || true)
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
