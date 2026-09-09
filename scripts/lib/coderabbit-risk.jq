# Extracts CodeRabbit's freshness signal from the issue-comments array of a PR
# (`gh api repos/<repo>/issues/<pr>/comments`). Shared by pr-review-status.sh
# and its test so the two cannot drift apart.
#
# Why this exists (issue #950): CodeRabbit does not file a new review object
# per re-review. It EDITS one walkthrough issue-comment in place, so a review
# count keyed on the head commit is wrong in both directions -- zero reviews at
# head for a PR that was just re-reviewed (false "not reviewed"), and a review
# object left at head by an earlier push while the assessment inside the
# walkthrough predates it (false "reviewed"). Both have decided a merge.
#
# The walkthrough's own risk line names the commit the assessment covers:
#
#   Merge Risk: Moderate . up to `6f458`
#
# The sha is a short prefix, so the caller compares it against the head sha by
# prefix, not by equality.
#
# Output is a two-field TSV line: the risk sha ("" when no risk line exists at
# all) and whether the newest bot comment says reviews are paused.
#
# Selection rules:
#
#   only comments authored by coderabbitai[bot] -- a human quoting a risk line
#   is not an assessment;
#
#   ordered by created_at and the NEWEST comment carrying a risk line wins. A
#   PR can accumulate several walkthrough comments (the bot posts a fresh one
#   after a force-push, or after being re-summoned), and an older one still
#   carries its own risk line for a commit that is no longer the head;
#
#   the LAST backticked hex token on the risk line is the sha. The greedy
#   `[^\n]*` before it is bounded to one line, so a body with a risk line and
#   an unrelated backticked token further down cannot be mixed up, and the
#   "up to `<sha>`" token wins over anything backticked earlier on the line.
def risk_sha:
  [ match("Merge Risk:[^\\n]*`(?<s>[0-9a-fA-F]{4,40})`"; "g") | .captures[0].string ]
  | last // "";

[ .[]? | select(.user.login == "coderabbitai[bot]") ]
| sort_by(.created_at // "")
| (([ .[] | select((.body // "") | test("Merge Risk:")) ] | last) // null) as $newest_risk
| ((. | last) // null) as $newest_any
| [
    (if $newest_risk == null then "" else (($newest_risk.body // "") | risk_sha) end),
    (if (($newest_any.body // "") | test("Reviews paused"; "i")) then "paused" else "active" end)
  ]
| @tsv
