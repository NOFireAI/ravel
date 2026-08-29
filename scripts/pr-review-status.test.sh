#!/usr/bin/env bash
# Cases for scripts/lib/coderabbit-verdict.jq, the filter that decides whether
# CodeRabbit has actually reviewed a PR's current head.
#
# Fixtures only: no network, no gh. Each case is a comment shape observed on a
# real PR, named with the PR it came from.
#
# Exit 0 all pass, 1 on a failure.
set -uo pipefail

FILTER="$(dirname "$0")/lib/coderabbit-verdict.jq"
[[ -r "${FILTER}" ]] || { echo "missing ${FILTER}" >&2; exit 64; }

SHA="0c65a3d3983384d663189257a25cc2d40ca95d32"
OTHER="1111111111111111111111111111111111111111"
passes=0
fails=0

check() {
  local name="$1" want="$2" json="$3" got
  got="$(printf '%s' "${json}" | jq --arg sha "${SHA}" -f "${FILTER}")" || got="ERROR"
  if [[ "${got}" == "${want}" ]]; then
    printf 'ok    %s\n' "${name}"
    passes=$((passes + 1))
  else
    printf 'FAIL  %s: want %s, got %s\n' "${name}" "${want}" "${got}"
    fails=$((fails + 1))
  fi
}

# The clean case (#893): zero findings, reported as an issue comment, no formal
# review filed at all. This is what used to be missed, holding a reviewed PR
# forever.
check "zero-finding walkthrough at head counts" 1 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"Reviewing files that changed between abc and ${SHA}.\n\nNo actionable comments were generated in the recent review."}]
EOF
)"

# A review that DID find things still counts as a review; its findings are
# gated separately by the inline-comment check.
check "walkthrough reporting findings counts" 1 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"Actionable comments posted: 6\n\nReviewed up to ${SHA}."}]
EOF
)"

# The regression this file exists for (#788): a rate-limit notice quotes the
# commit but is not a review. Keying on the sha alone counted it and cleared
# the review gate for a PR the bot never reviewed -- the false-CLEAR direction.
check "rate-limit notice quoting the sha does NOT count" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"⚠️ Rate limit exceeded\n\n@user has exceeded the limit. Please wait before requesting another review.\n\nCommits: reviewed up to ${SHA}."}]
EOF
)"

# A verdict for a DIFFERENT commit is a stale review of code that is no longer
# the head, exactly as a stale formal review would be.
check "verdict naming another commit does NOT count" 0 "$(cat <<EOF
[{"user":{"login":"coderabbitai[bot]"},
  "body":"Reviewed up to ${OTHER}.\n\nNo actionable comments were generated in the recent review."}]
EOF
)"

# Only the bot's own verdicts count; a human quoting the marker does not.
check "human comment quoting the marker does NOT count" 0 "$(cat <<EOF
[{"user":{"login":"pmoust"},
  "body":"CodeRabbit said: No actionable comments were generated, at ${SHA}."}]
EOF
)"

# A body-less comment must not blow up the filter.
check "null body is tolerated" 0 '[{"user":{"login":"coderabbitai[bot]"},"body":null}]'

check "no comments at all" 0 '[]'

printf '\n%d passed, %d failed\n' "${passes}" "${fails}"
[[ "${fails}" -eq 0 ]]
