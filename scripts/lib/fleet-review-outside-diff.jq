# Counts the findings the fleet review bot reports in a REVIEW BODY rather than
# as an inline comment, for the commit in $sha. Input is the array from
# `gh api repos/<repo>/pulls/<pr>/reviews`. Shared by pr-review-status.sh and
# its test so the two cannot drift apart.
#
# A finding whose line GitHub will not accept an inline comment on is folded
# into the review body under a heading of its own:
#
#   Findings outside the diff:
#   - crates/ravel-sql/src/executor.rs:120 (major): the retry drops the error
#   - docs/ingest.md: the flush cadence paragraph contradicts the code
#
# Nothing in the inline-comment count sees those. The bot writes that heading
# on a line of its own and one `- ` bullet per finding (reviewbot/result.go),
# and it appends a SECOND such block when GitHub answers 422 to a line it will
# not take, so a body can carry more than one heading and both must count.
#
# Two conditions, both required:
#
#   the review's own commit_id equals $sha -- the same head discipline the
#   review count uses, and stricter than a body-text sha match, since a body
#   on a superseded commit describes code that no longer exists;
#
#   the heading on a line of its own. Counting bullets only while inside such a
#   block is what keeps the "Not checked:" list, which uses the same bullet
#   syntax earlier in the same body, out of the count.
#
# KNOWN LIMIT, accepted deliberately: a body that quotes a complete block
# verbatim (a review discussing this very mechanism, say) counts those bullets.
# That is a false BLOCK, which costs one read of the body, rather than a false
# CLEAR, which ships unfixed findings. The asymmetry decides it.
def outside_diff_count:
  # Line-state walk rather than a regex over the whole body: the heading opens
  # a block, any non-bullet line closes it. A regex spanning lines would have
  # to guess where the block ends, and the bullet syntax is not unique to it.
  ( split("\n")
  | reduce .[] as $line ({inblock: false, n: 0};
      if ($line | sub("[[:space:]]+$"; "")) == "Findings outside the diff:" then
        .inblock = true
      elif .inblock and ($line | test("^- ")) then
        .n += 1
      else
        .inblock = false
      end)
  | .n
  );

[
  .[]?
  | select(
      .user.login == $bot
      and .commit_id == $sha
    )
  | (.body // "")
  | outside_diff_count
]
| add // 0
