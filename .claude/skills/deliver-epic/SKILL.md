---
name: deliver-epic
description: Use when the user gives a paragraph of feature intent and wants the whole epic delivered - ADR through fleet execution to merged main - with one approval gate. Triggers: "deliver this epic", "take this feature end to end", "run deliver-epic on ...".
---

# Delivering an epic end to end

One paragraph of intent in, merged epic out. Five stages; exactly one
human gate (ADR approval, end of stage 1). Every other decision is yours.
The ledger in the epic issue is the source of truth for resume: write it
before acting, not after, so a dropped session loses nothing.

This skill orchestrates; it does not restate. Specs come from the
fleet-task-spec skill, merges from merge-fleet-result, frozen formats from
format-change. `reference.md` beside this file holds the incidents behind
the rules below.

| Stage | Output | Gate |
|---|---|---|
| 0 Measure | Profiled baseline on the real workload | a number, not a hypothesis |
| 1 Design | ADR + epic issue | HUMAN APPROVAL (the only one) |
| 2 Decompose | Task table, DAG, waves, sub-issues | waves have zero file overlap |
| 3 Execute | Fleet dispatch per wave, ledger entries | all tasks terminal |
| 4 Checkpoint | Adversarial review of the wave diff | review passes |
| 5 Land | Merged main, closed sub-issues, ledger | required checks green |

Stages 3-5 loop per wave. Wave N+1 is not dispatched until wave N has
landed on main.

## Stage 0 - Measure

A performance epic aimed by a report instead of a profile spends its
budget on the wrong half of the system. Run the real workload end to end
with the profile-hotspot skill (the CLI and bench crates carry
stage-timing and flamegraph lanes), and write the total, the dominant
phase and the measurement command into the epic body. The Decision must
name the measured bottleneck and the number it moves. Skip this stage
only for an epic with no performance claim, and say so in the epic body.

## Stage 1 - Design

1. Research the crates the intent touches and their normative docs (the
   CLAUDE.md doc map) with Explore subagents. Never read vendored
   dependency sources.
2. Open the epic issue first:
   `gh issue create --title "Epic: <feature>" --body-file <body>`, with the
   intent, the Stage 0 numbers, an empty `## Tasks` checklist and an empty
   `## Ledger`. Assign it to yourself (`gh issue edit <n> --add-assignee
   @me`): the assignee is the claim another session sees. Then
   `scripts/epic-orchestrator.sh init <epic> --title "<feature>"`.
3. The ADR number is the epic issue number, zero-padded:
   `docs/adrs/NNNN-<slug>.md`. Sections: Context, Decision, Rejected
   alternatives (each with the concrete reason it lost), Consequences, and
   at least one Mermaid diagram (component or data flow; trust boundary
   for anything security-shaped). If a frozen format is touched, follow
   format-change first.
4. STOP. Present the ADR summary, rejected alternatives and issue number.
   On approval, commit the ADR (`docs: add ADR-NNNN <title>`, trailer
   `Refs: #<epic>`), push, and proceed without further confirmation.

## Stage 2 - Decompose

For each task emit a row:

```
ID | title | crates | predicted files | deps | acceptance test | risk
```

- Acceptance test: a named test (`crate::module::test_name`) that must
  exist and pass in the result. It goes verbatim into the spec's Tests
  section and is re-run at checkpoint.
- Risk: low / medium / high. High touches durability, the commit protocol,
  or a persistent format boundary.
- At least one task's acceptance test drives the delivered capability
  through a real entry point in the shipping binary (a service handler, a
  query path, a startup wire-up), so a green result means it is usable,
  not just present. Point the executor at an existing reachability test
  as the pattern.

Build the DAG from deps, then cut waves:

- A wave is a set of ready tasks with zero overlap in predicted files and
  no two tasks in the same crate (same-crate tasks collide on `lib.rs`,
  `Cargo.toml` and signatures even when their file lists look disjoint).
  When one task changes a public signature, grep the other tasks'
  predicted files for that name.
- Two tasks predicted to touch the same file are one fleet task with
  combined deliverables, not two serialized ones.
- High-risk tasks ride solo in their wave and get `effort: high`.
- Predict conservatively: unsure whether a task touches a file, assume it
  does.

Create one sub-issue per task (scope, acceptance test, risk), tick-list
them in `## Tasks`, and write the wave plan as the first ledger entry.

## Stage 3 - Execute (per wave)

1. Write each spec with the fleet-task-spec skill; its Tests section names
   the acceptance test.
2. Dispatch through the intent script, never by calling `fleet_dispatch`
   directly: it is where the stale-ref, dangling-intent and duplicate-work
   guards run.

   ```sh
   sha=$(git fetch origin main -q && git rev-parse origin/main)
   # Predicted files from the Stage 2 table: an open PR on them refuses.
   export DISPATCH_PATHS="crates/x/src/a.rs,crates/x/src/b.rs"
   nonce=$(scripts/fleet-dispatch-intent.sh intent <epic> <sub-issue> "$sha")
   ```

   A refusal ends that task for this tick. 65: a pull request already
   addresses the ticket, or a previous intent has no recorded outcome
   (reconcile it with `record` or `failed`). 66: an open pull request
   touches those files. 69: GitHub could not be asked, which is not a
   clean answer. Any other non-zero exit, including the fresh-ref
   guard's, is a refusal too. Resolve what it names;
   `DISPATCH_SKIP_DUPLICATE_CHECK=1` is only for a deliberate second task
   on one ticket (a fix round, a continuation after a ceiling kill).

3. `fleet_dispatch` with `ref=$sha`, then record immediately, before
   watching anything:

   ```sh
   scripts/fleet-dispatch-intent.sh record <epic> "$nonce" <task_id>
   scripts/epic-orchestrator.sh record <epic> task-dispatched \
     ticket=<sub-issue> task=<task_id> ref="$sha"
   ```

   If the dispatch errored, close the intent instead with
   `scripts/fleet-dispatch-intent.sh failed <epic> "$nonce" <reason>`.

   Both, not either. The intent script writes comments, which close the
   dangling-intent check; `epic-orchestrator.sh record` writes the task
   line into the issue body, reads it back (exit 70 if another session's
   edit overwrote it) and indexes it locally. `epic-status.sh` reads only
   the body. Hand-editing the body instead leaves the index empty, and
   resume then fails with exit 66.
4. Watch each task with `scripts/fleet-watch-loop.sh <watch-url>` as the
   whole command of a Monitor, one per task (CLAUDE.md, "Waiting on fleet
   tasks and PRs"). Never run a watcher with `run_in_background` or
   `nohup ... &` (the harness kills it after about 10 minutes), never poll
   `fleet_status` in a foreground loop, and never rely on one SSE
   connection.

Failure playbook, in order of check:

| Signal | Action |
|---|---|
| done, but `git ls-remote origin refs/heads/task/<id>/result` empty | Work lost. Re-dispatch the same spec once. |
| rate limit / auth / transient executor error | Retry once as-is. Second failure: re-dispatch with `label_selector` naming a different executor. |
| runtime ceiling (~4h kill) | Fetch the recovered ref and re-dispatch a continuation from it (`ref:`, spec says what remains). Never restart from scratch. |
| second re-dispatch also fails | Stop the wave, ledger `blocked`, report to the user. Never drop the task silently. |

Every dispatch, retry and terminal event gets a ledger line when it
happens.

## Stage 4 - Checkpoint (per wave)

After every wave task is terminal with a verified result ref, and before
any PR is opened:

1. Fetch the result branches and build the combined diff against current
   `origin/main`, not the dispatch-time HEAD.
2. Spawn one adversarial reviewer: Agent tool, general-purpose,
   `model: opus`, `isolation: "worktree"` (it runs real git commands, and
   without isolation it can revert uncommitted edits in your checkout).
   It hunts only correctness bugs and invariant violations, not style:
   - durability depending on local disk; recovery reading another
     process's local state
   - mutation of data objects, commit records, manifests, index objects
   - in-place edits to frozen formats (RSEG, proto/, series identity,
     commit tokens, key layout) without ADR and version bump
   - unwrap/expect on production paths; `unsafe`
   - silent approximation; placeholder implementations on critical paths
   - acceptance tests that do not assert the claimed behavior
   - a sentence in a doc, HELP string, doc comment or ADR asserting a
     property the code lacks ("never"/"always" true on one path only, a
     number that no longer matches the tree)
   - a pattern fixed at one site and left at another: grep for the
     instance the change fixes and check every other occurrence
   - a metric family, label, flag or report field a doc names and nothing
     renders

   Findings as `file:line - claim - why it's wrong`, and a verdict: pass
   or block.
3. Block: fix before merging anything, locally in a worktree if small and
   mechanical, otherwise as a fleet fix task. Re-review the changed area.
   Wave N+1 does not dispatch until the verdict is pass.
4. Ledger the verdict, finding count and fix commits.
5. Remove the reviewer's worktree (`git worktree remove <path>`, from
   outside it). `isolation: "worktree"` removes it only when the agent
   changed nothing, and each one left behind can hold tens of gigabytes
   of build cache.

## Stage 5 - Land (per wave)

Work in a fresh worktree of `origin/main`, per CLAUDE.md.

1. Per task, in DAG order: `scripts/fleet-result-inspect.sh <task-id>`,
   write the PR message file (line 1 is the title; the body carries
   `Fixes: #<sub-issue>`), then `scripts/fleet-result-merge.sh <task-id>
   <message-file> -p <crates>`. It opens a PR without auto-merge and
   posts `@claude-fleet review`; follow the merge-fleet-result skill to
   answer the review and merge. Before merging, confirm CI on the
   reviewed head:

   ```sh
   scripts/guards/assert-green-head.sh <pr> --epic <epic>
   ```

   0 merges; 2 wait; 3 rerun once; 1 is the same failure twice (real,
   escalate); 4 a flake with the rerun budget spent; 5 no verdict, never
   a green; 6 a cancelled or timed-out check for `ci-sweep-cancelled.sh`.
   Then `scripts/epic-orchestrator.sh record <epic> merged pr=<pr>
   sha=<merge-sha>`.
2. Real merge conflict: stop and read the conflicting main commits
   (`git log <merge-base>..origin/main -- <paths>`, full bodies). If a
   structural decision killed the task's premise (an ADR, a format
   version change, a rewrite), preserve the branch, point to it from the
   sub-issue, ledger it and move on. Resolve only overlapping edits
   textually.
3. Verify each sub-issue closed (`Fixes:` closes, `Refs:` does not);
   close stragglers with a comment linking the merge.
4. Ledger the wave: merge SHAs, new main SHA, closed issues. Tick the
   checklist.
5. Remove this wave's worktree before starting wave N+1; the next wave
   lands against a moved main in its own fresh worktree.
6. Last wave only: update README and docs per CLAUDE.md, write the final
   ledger entry, close the epic.

## Ledger format

One `### Wave N` block in the epic's `## Ledger`, edited in place
(`gh issue edit --body-file`, or a comment per wave if body edits race):

```
### Wave N - <planned|dispatched|review|blocked|landed>
plan: T4 #103, T7 #106 (from main <sha>)
T4 #103 task=<task_id> done result=<sha>
T7 #106 task=<task_id> ceiling-killed; continued as task=<task_id2> from <recovered-sha>
review: pass (2 findings fixed in <sha>)
landed: <merge-sha> <merge-sha2>; main=<sha>; closed #103 #106
```

## Resume after a dropped session

0. `scripts/epic-orchestrator.sh reconcile <epic>` first. It rewrites
   `.claude/epic-state/<epic>.json` from live GitHub state and exits 65 on
   drift that must be fixed before the next dispatch, including a task
   the local index knows about that never reached the issue body. A task
   with a start ref and no result ref is settled by asking the fleet control
   plane (`GET /v1/tasks/<id>`). RUNNING does not block. DEAD blocks until
   its ticket is re-dispatched, unless another task on the same ticket
   landed, completed, or is running, which makes it SUPERSEDED. If the
   control plane cannot be asked, the task stays UNRESOLVED and blocks:
   guessing "running" is how a dead task's ticket sits unfixed for the rest
   of the session. Set `FLEET_CP_URL` when `~/.fleet/cp.env` names a stale
   host.
   `resume-get <epic>` says whether an interruption is parked and how long
   the backoff has to run; a rate limit, a 5xx or a session limit is parked
   with `resume-set --reason`, which exits 69 with the delay to pass to
   ScheduleWakeup, or 75 when the error is fatal or the budget is spent.
1. Read the ledger's last wave block and its status.
2. Any task without a terminal ledger line: `fleet_status` it, verify its
   result ref with `git ls-remote`, and resume that stage.
3. `planned`: dispatch. `dispatched`: re-arm the watches. `review`: rerun
   the checkpoint. `blocked`: resolve the reason in the ledger. `landed`:
   next wave.

Never re-dispatch a task whose result ref exists; merge it.

## Red flags

- "Executor reported gates green": the cold verify-dispatch run and the
  PR's required checks are the gate.
- "Conflict, I'll just resolve it": premise check first (Stage 5.2).
- "Same file but different functions" or "same crate, disjoint files":
  one task, or different waves.
- "Diff is small, skip the checkpoint": it runs every wave.
- "I'll ask the user before wave 2": the only gate is ADR approval.
- "I'll record the task ids after they finish": record before watching.
- "I'll call `fleet_dispatch` directly, I already checked", or "the
  duplicate guard refused, I'll set the skip flag": the intent script's
  guards are the check; read what a refusal names.
- "I'll clean up worktrees at the end": remove each at the end of its own
  wave, reviewer worktrees included.
