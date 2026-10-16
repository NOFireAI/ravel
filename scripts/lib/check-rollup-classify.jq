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
# Shared by scripts/pr-review-status.sh and scripts/guards/assert-green-head.sh
# so the two can never disagree about whether a pull request is green.
[.statusCheckRollup[]? | {
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
