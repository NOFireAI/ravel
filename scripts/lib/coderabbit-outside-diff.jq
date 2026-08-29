# Counts the findings CodeRabbit reports in a REVIEW BODY rather than as an
# inline comment, for the commit in $sha. Input is the array from
# `gh api repos/<repo>/pulls/<pr>/reviews`. Shared by pr-review-status.sh and
# its test so the two cannot drift apart.
#
# When a finding falls outside the diff range GitHub will accept an inline
# comment on, the bot cannot post it inline, so it folds it into the review
# body under a heading like:
#
#   > <details>
#   > <summary>⚠️ Outside diff range comments (1)</summary>
#
# Nothing in the inline-comment count sees those.
#
# Two conditions, both required:
#
#   the review's own commit_id equals $sha -- the same head discipline the
#   review count uses, and stricter than a body-text sha match, since a body
#   on a superseded commit describes code that no longer exists;
#
#   the literal heading text appears in the body. The pattern is the heading
#   phrase and nothing looser on purpose: the verdict filter beside this one
#   had to be tightened after keying on a loose signal (a bare sha) produced a
#   false CLEAR on #788, and the same direction is the danger here. Keying on
#   "the body is non-empty" would count every summary and walkthrough as a
#   finding, which trains an operator to pass --confirm-addressed reflexively
#   and puts us back where #908 started.
#
# The pattern requires the emitted <summary> ELEMENT, not the phrase. The bot
# writes the heading as
#
#   > <summary>⚠️ Outside diff range comments (1)</summary>
#
# so the match is anchored on <summary> ... </summary> with "Outside diff
# range" and "comment"/"comments" inside it, on one line. Matching the phrase
# alone was wrong in a way that is easy to reproduce: a review that QUOTES the
# heading in ordinary prose -- discussing this very mechanism, for instance --
# contains the phrase and the word on one line and would be counted as a
# finding, blocking a clean PR. That is a false block rather than the false
# clear #908 was about, but a checker that cries wolf while discussing itself
# trains the operator to pass --confirm-addressed reflexively, which lands back
# at the same place.
#
# The older "Outside diff range and nitpick comments (N)" variant is emitted in
# the same element and is still matched.
#
# KNOWN LIMIT, accepted deliberately: a body that quotes the COMPLETE element
# verbatim still counts. Tightening further (requiring the "> " blockquote
# prefix the bot currently emits, say) would exclude that case, but it couples
# this check to a formatting detail of the bot's output -- and if that detail
# ever changes, real findings stop being counted and the gate goes quiet. That
# is a false CLEAR, the failure this whole check exists to prevent, traded for
# a false BLOCK that costs one read of the body. The asymmetry decides it: an
# over-count makes a human look, an under-count ships unfixed findings. Verified
# not to fire in practice -- run against the live review bodies on PR #915 at
# head 88f60210, including the review that raised this exact concern, the count
# is 0.
#
# Counting rule: the heading carries the number of findings in the block in
# parentheses, so a heading with a count contributes that count, and a heading
# without one contributes 1. Multiple headings in one body, and multiple
# reviews at the same head, sum.
def outside_diff_count:
  [
    match("<summary>[^\\n]*Outside diff range[^\\n]*comments?[^\\n]*</summary>"; "g")
    | .string
    | ((capture("\\((?<n>[0-9]+)\\)") | .n | tonumber) // 1)
  ]
  | add // 0;

[
  .[]?
  | select(
      .user.login == "coderabbitai[bot]"
      and .commit_id == $sha
    )
  | (.body // "")
  | outside_diff_count
]
| add // 0
