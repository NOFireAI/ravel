---
name: merge-fleet-result
description: Use when a fleet task finishes - inspect its result branch, open the PR through fleet-result-merge.sh, wait for the fleet review and CI, merge through the queue, and clean up the task refs; never trust executor-claimed green
---

# Merging a fleet result branch

An executor's "gates green" is not the gate: an incremental build cache
can mask an error that a cold build shows. Acceptance is a cold-cache gate
run on the result branch (the verify-dispatch skill) plus the PR's
required checks, which the merge queue runs again on the branch rebased
onto current main. `main` is protected (PR required, required checks,
rebase-merge only), so nothing pushes it directly. `reference.md` beside
this file has the details of the history clean and the review states.

## 1. Verify and inspect

Run the verify-dispatch skill on the result branch first. On a tier-1
FAIL, follow its procedure (issue plus fix dispatch) and stop here.

```sh
TASK=<task-id>
scripts/fleet-result-inspect.sh $TASK   # expected commits? only the task's dirs?
```

Files outside the task's stated dirs: review those hunks before going on,
and do not open the PR if they are wrong. A commit header outside the
repo convention: amend it on the branch first (the script rewrites only
`wip:` and formatting-fixup subjects).

## 2. Open the PR

From a fresh worktree detached at `origin/main` (`git worktree add
--detach <path> origin/main`), never the primary checkout or a worktree
you reviewed or fixed the branch in: the script checks out the cleaned
branch in its current directory to run pre-flight gates, and its guard
refuses any HEAD other than a clean `main` or `origin/main`.

Write the PR message file (line 1 is the title; the body after the blank
line carries `Fixes: #<issue>` or `Refs: #<issue>`), then:

```sh
scripts/fleet-result-merge.sh $TASK message.txt   # -p CRATE scopes the local gates
```

It folds `wip:` and formatting-only commits into their neighbours (keeping
their trailers), rewrites authorship, runs the local pre-flight gates,
pushes `task/<id>/merge`, opens the PR without auto-merge, and posts
`@claude-fleet review`.

- A pre-flight gate failure means fix the branch or re-dispatch, then
  retry. Never bypass the required checks.
- `FLEET_MERGE_SKIP_GATES=1` skips the repeat local run only when this
  exact tree already passed the full gates (for example a
  `verify-dispatch-gates.sh --with-gates` receipt). Not after a conflict
  resolution or any edit.
- `FLEET_MERGE_AUTO=1` enables auto-merge for the rare PR that needs no
  review wait. The review still arrives after the merge; sweep it.

## 3. Answer the review

Poll `scripts/pr-review-status.sh <pr-number>` until CI is green,
`mergeStateStatus` is `CLEAN` or `UNSTABLE`, and a review exists at the
current head in state `COMMENTED` or `APPROVED`. `DIRTY`, `DRAFT` and
`BEHIND` are not mergeable whatever CI or the review says: resolve them
first. `PENDING`, `DISMISSED` and `CHANGES_REQUESTED` are not clean. The
bot only ever posts `COMMENTED`; a `CHANGES_REQUESTED` from anyone at
head blocks. When there
is no review at head, act on which of the five states the script names;
a task that went terminal with no review needs the trigger posted again,
and a push never starts a review by itself.

Fix or explicitly answer (thread reply, or a commit message saying why it
does not apply) every finding not marked `nit`; an inline comment with no
severity counts as actionable. Read the "Findings outside the diff:"
section of the review body too. The inline-comment count never drops, so
clean means every comment accounted for, not zero. A review whose
findings are all nits is clean: do not push nit fixes or ask for another
round. After 3 review rounds on one PR, stop and hand it to a person.

## 4. Merge

Run the merge command `pr-review-status.sh` prints, exactly:

```sh
ALLOW_LITERAL_SHA=1 gh pr merge <number> --rebase --match-head-commit <sha>
```

The pinned SHA is the check: a head resolved at merge time
(`gh pr view --json headRefOid`) would merge a push that landed after the
review. The command enqueues the PR in the merge queue (`REBASE`,
`ALLGREEN`, batches up to 5), which tests it on top of current main.

- Do not hand-rebase because main moved; it costs a CI cycle and
  invalidates the review at head. A behind-ness count is not a reason to
  act; `assert-fresh-merge-base.sh` is for a merge that bypasses the
  queue.
- Do not add `--delete-branch`: `gh` refuses it while a queue is enabled,
  and `delete_branch_on_merge` removes the merge head anyway. "The merge
  strategy for main is set by the merge queue" is informational.
- Poll `gh pr view <number> --json state,mergedAt` until it reports
  merged. A PR still open ten minutes later may have been ejected (a red
  batch, or a conflict after the rebase): look, do not wait it out. For a
  failed batch, read which check went red on the `merge_group` run.
- Under `FLEET_MERGE_AUTO=1`, only poll; never run a second `gh pr merge`.

## 5. Clean up

Only once the PR reports merged:

```sh
git push origin --delete task/$TASK/result task/$TASK/start
```

If a required check fails instead, keep the task refs, fix or
re-dispatch, and retry. Close the issue if no landed trailer did
(`Fixes:` closes, `Refs:` does not), update the task ledger, and file
follow-up issues for any deviations or bugs the executor reported.

## Gotchas

- Append-heavy index files (`docs/adrs/README.md`) conflict on almost
  every landing. Not a premise conflict: keep both sides' entries, drop
  the duplicate of your own, keep the file's order, re-run the gates.
- Before running cargo on a hand-combined tree (a rebase, a multi-branch
  land), `grep -rn '<StructName> {'` workspace-wide for every struct whose
  fields the diff changes and fix every literal; otherwise `E0063` errors
  surface one cargo cycle at a time.
- `gh api -f` sends strings; a boolean or number needs `-F`
  (`-F strict=false`), or the API answers `"false" is not a boolean`.
- `gh pr merge` can fail once with a GraphQL error naming a merge method
  you never asked for ("squash merging is not allowed"). Retry once
  before investigating repo settings.
- Never enable auto-merge planning to disable it later:
  `gh pr merge --disable-auto` fails silently when GitHub has already
  merged the PR seconds after a check went green.
