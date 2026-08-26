---
name: merge-fleet-result
description: Use when a fleet task finishes - fetch its result branch, re-run gates locally, merge to main, push, and clean up; never trust executor-claimed green
---

# Merging a fleet result branch

Executor gate claims are not the gate. An executor can report fmt/clippy/
test clean while its branch does not compile from a cold build, because an
incremental build cache can mask an error (a stale-cache lifetime error is
the known shape). Local gates on the merged tree are the only acceptance.

**Run the verify-dispatch skill on the result branch before merging.** It
runs the same gates this skill's procedure below runs, but in an isolated
worktree with a cold `CARGO_TARGET_DIR` (defeating exactly that
incremental-cache masking) instead of on the primary checkout after the
merge has already landed there, plus a set of narrow adversarial checks
for defect classes this repo has shipped before. On a tier-1 FAIL,
verify-dispatch's own procedure covers filing an issue and re-dispatching
a fix; don't duplicate that here. Once verify-dispatch reports tier-1
PASS, continue with the procedure below.

## main is landed through a PR, never a direct push

`main` has required branch protection: a PR is mandatory, required status
checks must pass, and history is linear (rebase-merge only). No process,
`fleet-result-merge.sh` included, pushes `main` directly. The script
lands a result branch by cleaning its history, pushing that cleaned history
to a `task/<id>/merge` head branch, and opening a PR against `main`. The
rebase-merge keeps each commit's own message, so per-commit `Fixes:`/
`Refs:` trailers still close their issues when they land.

**The PR opens WITHOUT auto-merge by default (standing rule, 2026-08-26).**
CodeRabbit's GitHub App reviews every PR but posts as a review comment, not
a required status check, so `--auto` merges before that review lands --
#749/#750 landed with 6 real CodeRabbit findings unaddressed this way. Wait
for the `coderabbitai[bot]` review, fix or explicitly answer every
actionable finding (a walkthrough-only comment with zero findings counts
as clean), then merge by hand once CI is green:

```sh
scripts/pr-review-status.sh <pr-number>   # one-line CI + CodeRabbit status
gh pr merge <pr-number> --rebase
```

`FLEET_MERGE_AUTO=1` restores the old `gh pr merge --auto --rebase`
behavior for the rare case that genuinely does not need a CodeRabbit wait;
do not set it out of impatience.

## Result-branch history is cleaned before the PR is opened

Before it pushes anything, `fleet-result-merge.sh` scans the result
branch's own commits (everything between its merge base against
`origin/main` and its tip) and rewrites two classes of commit out so they
never reach main:

- `wip:` headers. Work-in-progress snapshot commits have reached
  protected main when the merge path carried a result branch through
  verbatim.
- Pure formatting/style fixups. When an executor gates first and formats
  second, a formatting-only commit rides along on top of the real work
  and ends up on main as noise.

The rewrite rebuilds the branch linearly with `cherry-pick` onto a
throwaway `_fleet_rewrite_<id>` branch (a merge commit anywhere in the
range aborts the whole run with a clear message, because cherry-pick
cannot replay a merge). Each flagged commit is folded into the previous *retained*
commit, but its own `Refs:`/`Fixes:`/`Signed-off-by:` trailers are carried
into that commit's message first (via `git interpret-trailers
--if-exists addIfDifferent`), so a required trailer that lived only on a
`wip:` snapshot is never dropped. A flagged commit that is the *first*
retained commit has nothing to fold into, so it is reworded instead: the
`wip:` prefix is stripped and, if what remains has no Conventional Commits
type, a `chore:` type is prepended so the subject stays a valid header. The
formatting-fixup detector is deliberately conservative: it fires only when
the subject mentions `fmt`/`style fix` AND the diff is empty under
`git diff -w`, so a commit that reformats and also changes real content is
left alone. A branch with no flagged commits is left byte-for-byte
untouched and pushed as-is. The cleaned history is what becomes the PR
head, so the PR only ever contains clean commits. The fleet-task-spec skill
stops these commits being created in the first place (specs tell executors
to format before the gated commit); this step is the backstop for branches
written before that discipline, or that slip.

## Procedure

`fleet-result-merge.sh` is the path: it enforces the pre-flight guard
(refuses unless HEAD is a clean `main`/`origin/main`, so a stale HEAD can
never produce a too-old merge base), does the history clean above, runs the
local pre-flight gates on the cleaned tree, and only then pushes the PR
head and opens the PR (without auto-merge; see above).

```sh
TASK=<task-id>
git fetch origin refs/heads/task/$TASK/result
git log --oneline origin/main..FETCH_HEAD   # exactly the expected commits?
git diff --stat origin/main...FETCH_HEAD    # scope: only the task's dirs?

# Write the PR message: first line is the PR title, everything after the
# blank line is the PR body (put Fixes: #<issue> / Refs: #<issue> here).
scripts/fleet-result-merge.sh $TASK message.txt   # add -p CRATE to scope local gates
```

- Scope creep in the diff (files outside the task's stated dirs): stop and
  review those hunks before running the script; do not open the PR if they
  are wrong.
- The PR's required status checks are the real gate. The script's local
  pre-flight gate run only catches obvious breakage before a PR is opened;
  a failure there means fix the branch (or re-dispatch) before retrying,
  not push anything. Never try to bypass the required checks.
- When the exact tree being merged already passed the full gates locally
  (the orchestrator gated the result branch before or after a local fix
  commit), `FLEET_MERGE_SKIP_GATES=1` skips the script's repeat run and
  lets the PR checks carry it. Not for conflict resolutions or any tree
  that differs from what was gated.
- Commit header not in repo convention: amend on the branch before running
  the script (the script only rewrites `wip:`/fixup subjects, not arbitrary
  non-conforming ones).

## Recurring mechanical conflicts and gotchas

- Run the script from a clean `main` checkout (or a fresh worktree of
  `origin/main`), not from the worktree you reviewed or fixed the branch
  in. The script's own guard exists because that mistake recurs; it
  refuses any other HEAD.
- Append-heavy index files (`docs/adrs/README.md` is the usual one)
  conflict on almost every landing, because `main` moves with unrelated
  entries. This is not a premise conflict. Mechanical resolve: keep both
  sides' entries, delete the duplicate of your own entry, keep the file's
  existing order, re-run the gates.
- Before running cargo on any hand-combined tree (a rebase, a multi-branch
  land), do a textual pass first: for every struct whose field list the
  combined diff changes, `grep -rn '<StructName> {'` across the whole
  workspace and fix every literal. Otherwise a changed field list surfaces
  as a long series of `E0063` (missing field) errors, one full cargo cycle
  at a time; the grep finds the whole class at once.
- `gh api` sends every `-f` value as a string. A boolean or number field
  needs `-F` (`-F strict=false`), or the API answers with
  `"false" is not a boolean`.
- `gh pr merge --auto --rebase` (only under `FLEET_MERGE_AUTO=1`, or the
  final by-hand merge once CodeRabbit is clean) can fail once with a
  GraphQL error naming a merge method you never requested ("squash merging
  is not allowed"). That is API flakiness around enabling auto-merge:
  retry once before investigating repo settings.
- Disabling auto-merge on a PR that is already mid-merge fails silently:
  GitHub can complete a merge within seconds of a required check going
  green, and a `gh pr merge --disable-auto` call that loses that race just
  finds the PR already merged. This is why the default is to never enable
  auto-merge in the first place, not to enable-then-disable it.

## After the PR is open

Poll `scripts/pr-review-status.sh <number>` until it reports clean (CI
green, CodeRabbit reviewed, no open inline comments), fixing or answering
every finding along the way, then land it by hand:

```sh
gh pr merge <number> --rebase --delete-branch
```

This removes the `task/$TASK/merge` head once it merges. The script
deliberately leaves `task/$TASK/result` and `task/$TASK/start` in place
regardless of merge mode: opening a PR is not landing, and deleting them
before the checks (and the CodeRabbit wait) finish would mean a failed
check or an unresolved finding leaves the PR open with no way to recover
the original result branch.

Watch the PR (`gh pr view <number> --json state,mergedAt`) until it reports
merged. Once merged, delete the task refs yourself:

```sh
git push origin --delete task/$TASK/result task/$TASK/start
```

If a required check fails instead, the PR stays open with the task refs
still intact; fix the branch (or re-dispatch) and retry, do not delete
anything.

Close the issue if no landed commit trailer did (`Fixes:` closes it,
`Refs:` does not). Update the in-flight task ledger. If the agent's report
listed deviations, ambiguities, or discovered bugs, file follow-up issues
now, before the context is lost.
