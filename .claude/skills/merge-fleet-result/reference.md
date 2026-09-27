# merge-fleet-result: details

Background for SKILL.md.

## How fleet-result-merge.sh cleans history

Before pushing, the script scans the result branch's own commits (from
its merge base with `origin/main` to its tip) for two classes that should
never reach main: `wip:` snapshot headers, and formatting-only fixups
(executors that gate first and format second leave them behind). It
rebuilds the branch linearly with `cherry-pick` on a throwaway
`_fleet_rewrite_<id>` branch; a merge commit anywhere in the range aborts
the run, because cherry-pick cannot replay one.

Each flagged commit folds into the previous retained commit, carrying its
`Refs:`/`Fixes:`/`Signed-off-by:` trailers over first
(`git interpret-trailers --if-exists addIfDifferent`), so a trailer that
lived only on a `wip:` snapshot is kept. A flagged first commit has
nothing to fold into, so it is reworded: the `wip:` prefix goes, and
`chore:` is prepended if no Conventional Commits type remains. The
formatting detector fires only when the subject mentions `fmt` or `style
fix` and the diff is empty under `git diff -w`, so a commit that reformats
and changes content is left alone. A branch with nothing flagged skips
the fold, but a separate authorship pass always runs: it re-authors every
commit to the merging identity, drops executor sign-offs and adds the
merger's, so a fleet-executor identity never reaches main. The
rebase-merge keeps each commit's message, so per-commit trailers still
close their issues.

## Why the PR opens without auto-merge

The fleet review (ADR-1586) posts as a review comment, not a required
status check, so `--auto` merges before the review lands and its findings
go unaddressed. The trigger is a comment whose whole body is
`@claude-fleet review`; anything after `review` is parsed as arguments,
and an unrecognized word gets a confused reaction and no review. The bot
reports nits on almost every PR, which is why an all-nit review counts as
clean: waiting for zero findings never ends, and a push for nits moves
the head and needs a fresh review.

## Review states in pr-review-status.sh

The review state and inline-comment count cover reviews from any author,
so a maintainer's `CHANGES_REQUESTED` at head blocks even though
`protect-main` requires no approvals; the check that a review happened at
all is scoped to the bot. With no review at head, the script names one of
five states: nobody asked, a malformed trigger, a task queued or running,
a task that went terminal with no review (nothing retries it), or a
review at an older commit. The REST API never removes a comment when the
code it flagged changes, so the count only ever grows; a review with zero
comments from the start is the only one clean by count.

## Why the task refs stay until merged

Opening a PR is not landing. Deleting `task/<id>/result` and
`task/<id>/start` before the checks and review finish would leave a PR
with a failed check or an unresolved finding and no way to recover the
original result branch.
