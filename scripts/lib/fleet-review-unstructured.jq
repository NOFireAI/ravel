# Says whether the fleet review bot's last review for the commit in $sha is one
# it could not parse: "true" or "false". Input is the array from
# `gh api repos/<repo>/pulls/<pr>/reviews`. Shared by pr-review-status.sh and
# its test so the two cannot drift apart.
#
# When a review task ends without a structured result, the bot posts the
# agent's raw tail as the review body under this first line:
#
#   Review task finished without a structured result (no ```json block in
#   final message). Raw tail follows.
#
# That review has no findings list and no inline comments, so every count of
# findings reads zero, whether the tail holds a complete review or one stale
# line. Zero findings there means nothing was reported, not that nothing was
# found.
#
# Only the last bot review at the commit decides: a later structured review of
# the same commit supersedes the unparsed one.
[
  .[]?
  | select(.user.login == $bot and .commit_id == $sha)
  | (.body // "")
]
| if length == 0 then false
  else (.[-1] | test("^Review task finished without a structured result"))
  end
