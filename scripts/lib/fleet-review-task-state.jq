# Classifies the fleet review bot's task comment for the commit in $sha into
# one word. Input is the array from `gh api repos/<repo>/issues/<pr>/comments`.
# Shared by pr-review-status.sh and its test so the two cannot drift apart.
#
# The bot posts ONE task comment per review task and edits it in place as the
# task progresses (reviewbot/format.go, InfoComment):
#
#   Review task queued.
#
#   Task: 3d030033-05ed-48d5-98f1-b086b1684655
#   Commit: a883f54edc1d6a1faac8e3e3c99f3facda837d27
#   Model: executor default (effort high)
#   Executor: pimox5
#   Status: failed (setup). No review posted.
#
# The trailing Status line is absent while the task runs and is one of a fixed
# set once it is terminal. Telling those apart is the whole point of this
# filter: a task that will never post a review must not read as one still
# working, or a merge check waits forever; and it must not read as done either,
# or the merge clears with nothing reviewed.
#
# Output, one of:
#   none     no task comment names $sha -- nobody asked for a review of it
#   running  queued or running, no Status line yet
#   done     terminal, a review was posted (including the unstructured form)
#   dead     terminal, no review will arrive: re-trigger, do not wait
#   unknown  a Status line this filter does not recognize -- read it by hand
#
# `unknown` exists so a new status string upstream surfaces as "verify by hand"
# rather than being folded into whichever branch happens to be checked last.
def classify:
  if test("(?m)^Status: done\\. Review posted\\.")
     or test("(?m)^Status: done \\(unstructured\\)\\.") then "done"
  # Every failure path the bot writes says so in the same words, and the two
  # that do not ("abandoned", "done, but posting the review failed") are named
  # here. Order matters: the done markers are matched above first, because
  # "done, but posting the review failed" contains the word done.
  elif test("No review posted\\.")
       or test("(?m)^Status: abandoned")
       or test("(?m)^Status: done, but posting the review failed") then "dead"
  elif test("(?m)^Status:") then "unknown"
  else "running"
  end;

[
  .[]?
  | select(
      .user.login == $bot
      # The literal first line the bot writes, so a human comment quoting a
      # Commit: line cannot be read as a task comment.
      and ((.body // "") | test("^Review task queued\\."))
      and ((.body // "") | test("(?m)^Commit: " + $sha + "$"))
    )
  | (.body // "")
]
| if length == 0 then "none"
  # A re-trigger on the same commit is refused upstream ("a task already runs
  # for this commit"), so more than one comment for one sha means an earlier
  # task for it went terminal and a later one exists. Read the last.
  else (.[-1] | classify)
  end
