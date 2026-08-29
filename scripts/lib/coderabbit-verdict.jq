# Counts CodeRabbit issue comments that constitute a completed review of the
# commit in $sha. Shared by pr-review-status.sh and its test so the two cannot
# drift apart.
#
# Two conditions, both required:
#
#   the head sha appears in the body -- the walkthrough names the exact commit
#   range it reviewed, so this is as strict as matching a review's commit_id,
#   and a walkthrough from before the last push does not count;
#
#   a verdict marker appears -- one of the two lines the bot emits only when a
#   review actually completed. Without this, any bot comment quoting a commit
#   counts, and a rate-limit notice is not a review. On #788 those notices
#   carry two 40-hex shas and no verdict marker, and would otherwise clear the
#   review gate for a PR the bot never reviewed.
[
  .[]
  | select(
      .user.login == "coderabbitai[bot]"
      and ((.body // "") | contains($sha))
      and ((.body // "")
           | (contains("No actionable comments were generated")
              or contains("Actionable comments posted")))
    )
]
| length
