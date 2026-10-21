# Classify a PR's statusCheckRollup into one bucket per check.
#
# Input: the object `gh pr view --json statusCheckRollup` returns.
# Output: [{name, class}], class in success/skipped/pending/failing/other.
#
# `statusCheckRollup` can mix two shapes: a `CheckRun` (GitHub Actions and
# most modern integrations -- `status`/`conclusion`, name in `.name`) and a
# legacy `StatusContext` (the older commit-status API some third-party
# integrations still use -- `state` only, name in `.context`). Classify each
# entry once, by shape, into one bucket, so neither shape nor an unrecognized
# value inside a recognized shape can silently vanish from every count.
#
# A skipped check is not a failure and does not block a merge -- GitHub's own
# ruleset treats a path-filtered required check as satisfied -- but it is not
# a pass either, so it gets its own class and a caller that folds it into the
# pass count is making that choice visibly.
#
# A check that ran more than once on the same head (a re-run, or a fresh
# suite after the pull request was closed and reopened) appears once per run.
# Only the newest run of each check counts, as it does for GitHub's own merge
# rules: an old cancelled run does not hold back a newer success, and a newer
# failure is not hidden by an older success. A check run is keyed by workflow
# and job name, so same-named jobs in different workflows stay separate; a
# status context by its context. A run that has not settled (not completed,
# or no `startedAt` yet, as a queued re-run reports) is taken as the newest,
# so a re-run in flight reads as pending rather than letting an older
# completed run stand. Among settled runs the newest is the latest
# `startedAt`, with input order breaking ties.
#
# Shared by scripts/pr-review-status.sh and scripts/guards/assert-green-head.sh
# so the two can never disagree about whether a pull request is green.
[.statusCheckRollup[]?]
| to_entries
| map(.value + {
    _key: (if (.value | has("state")) then "status:" + (.value.context // .value.name // "unknown")
           else "check:" + (.value.workflowName // "") + "/" + (.value.name // "unknown") end),
    _unsettled: (
      (if (.value | has("state")) then (.value.state == "PENDING" or .value.state == "EXPECTED")
       else (.value.status != "COMPLETED") end)
      or (.value.startedAt == null)
    ),
    _ts: (.value.startedAt // ""),
    _ord: .key
  })
| group_by(._key)
| map(sort_by([._unsettled, ._ts, ._ord]) | last)
| sort_by(._ord)
| [.[] | {
  name: (.name // .context // "unknown"),
  class: (
    if has("state") then
      (if .state == "SUCCESS" then "success"
       elif (.state == "PENDING" or .state == "EXPECTED") then "pending"
       elif (.state == "FAILURE" or .state == "ERROR") then "failing"
       else "other" end)
    elif has("status") then
      (if .status != "COMPLETED" then "pending"
       elif .conclusion == "SKIPPED" then "skipped"
       elif (.conclusion == "SUCCESS" or .conclusion == "NEUTRAL") then "success"
       elif (.conclusion == "FAILURE" or .conclusion == "CANCELLED" or .conclusion == "TIMED_OUT") then "failing"
       else "other" end)
    else "other" end
  ),
  conclusion: (.conclusion // .state // "")
}]
