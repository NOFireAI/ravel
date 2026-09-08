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
fleet-task-spec skill, merges from the merge-fleet-result skill, frozen
formats from the format-change skill. Read those when you reach their
stage.

## Quick reference

| Stage | Output | Gate |
|---|---|---|
| 0 Measure | Profiled baseline on the real workload | a number, not a hypothesis |
| 1 Design | ADR + epic issue | HUMAN APPROVAL (only one) |
| 2 Decompose | Task table, DAG, waves, sub-issues | waves have zero file overlap |
| 3 Execute | Fleet dispatch per wave, ledger entries | all tasks terminal |
| 4 Checkpoint | Opus adversarial review of wave diff | review passes |
| 5 Land | Merged main, closed sub-issues, ledger | gates green in worktree |

Stages 3-5 loop per wave. Wave N+1 is not dispatched until wave N has
landed on main.

## Stage 0 - Measure

A performance epic aimed by a report instead of a profile spends its
whole budget on the wrong half of the system. Before Stage 1:

1. Run the real target workload end to end and record where the time
   goes, per phase. Use the profile-hotspot skill; the CLI and bench
   crates already carry stage-timing and flamegraph lanes.
2. Write the numbers into the epic issue body: total, the phase that
   dominates, and the measurement command so anyone can re-run it.
3. The epic's Decision must name the measured bottleneck it attacks and
   the number it moves. "The audit suggests X is slow" is not a
   bottleneck; a stage timing is.

Skip Stage 0 only for an epic with no performance claim at all. If you
skip it, say so in the epic body and say why.

## Stage 1 - Design

1. Research against the actual codebase: Explore subagents over the
   crates the intent touches, plus their normative docs from the
   CLAUDE.md doc map. Never read vendored dependency sources.
2. Open the epic issue FIRST:
   `gh issue create --title "Epic: <feature>" --body-file <body>`
   Body: intent paragraph, Stage 0 numbers, empty `## Tasks` checklist,
   empty `## Ledger` section. Then assign it to yourself
   (`gh issue edit <n> --add-assignee @me`). The assignee is the claim:
   ownership that lives only in a session's head collides the moment a
   second session picks "the next obvious thing".
3. **The ADR number is the epic issue number.** GitHub allocates issue
   numbers atomically, so two parallel epics cannot collide, and no
   reservation step is needed. Write the full ADR at
   `docs/adrs/NNNN-<slug>.md` where NNNN is the zero-padded issue
   number. Sections: Context, Decision, Rejected alternatives (each with
   the concrete reason it lost), Consequences. Include at least one
   diagram (Mermaid: component or data-flow; trust-boundary for anything
   security-shaped) - a prose-only ADR has cost an extra approval round
   trip. If any frozen format is touched, follow the format-change skill
   before writing the full ADR.
4. STOP. Present ADR summary, rejected alternatives, and issue number.
   Nothing is committed yet: there is no stub to land, because the issue
   number already reserves the ADR number. This is the only mandatory
   human gate; on approval, commit the ADR
   (`docs: add ADR-NNNN <title>`, trailer `Refs: #<epic>`), push, and
   proceed without further confirmation.

## Stage 2 - Decompose

For each task emit a row:

```
ID | title | crates | predicted files | deps | acceptance test | risk
```

- Acceptance test: a named test (`crate::module::test_name`) that must
  exist and pass in the task's result. It goes verbatim into the spec's
  Tests section and is re-run at checkpoint.
- Risk tier: low / medium / high. High = touches durability, commit
  protocol, or a persistent format boundary.
- End-to-end reachability: at least one task in the table must carry an
  acceptance test that proves the epic's delivered capability is actually
  constructed and reachable from a real caller in the shipping binary,
  not merely unit- or crate-tested in isolation. Crate-level tests pass
  against code no production path ever builds: a merged, crate-tested
  cache has shipped that no caller constructed. The reachability
  test drives the capability through a real entry point (a service
  handler, a query path, a startup wire-up), so a green result means the
  feature is truly usable, not just present. Point the executor at an
  existing reachability test in the tree (one that attaches the feature
  through a real entry point) as the pattern to follow, without
  repeating its diff.

Build the DAG from deps, then cut into waves:

- A wave is a set of ready tasks (all deps landed) with **zero overlap
  in predicted files, and no two tasks in the same crate** - same-crate
  tasks collide on lib.rs mod declarations and Cargo.toml even when
  their file lists look disjoint.
- Two tasks predicted to touch the same file are not "serialized", they
  are ONE fleet task with combined deliverables. Splitting same-file
  work across dispatches produces two divergent rewrites of the same
  code that then need manual reconciliation.
- High-risk tasks ride solo in their wave (isolates the checkpoint
  review) and get `effort: high` on dispatch.
- Prediction is conservative: unsure whether a task touches a file,
  assume it does.

Create one sub-issue per task (body: scope, acceptance test, risk),
tick-list them in the epic's `## Tasks`, and write the full wave plan as
the first ledger entry.

## Stage 3 - Execute (per wave)

1. Confirm local main == origin/main and clean; the wave dispatches from
   this HEAD.
2. Write each spec with the fleet-task-spec skill. The harness-override
   and UNATTENDED paragraphs are not optional. Spec's Tests section
   names the acceptance test.
3. Dispatch the whole wave in parallel `fleet_dispatch` calls. Record
   every task_id in the ledger IMMEDIATELY, before watching anything - a
   dropped session with unrecorded task_ids orphans running work.
4. Watch with `scripts/fleet-watch.sh <watch-url> <interval>` in
   background, one per task. Pass the bare command straight to the
   background-execution tool with its own backgrounding flag (e.g. the
   Bash tool's `run_in_background: true`) - do not wrap it in `nohup
   ... &` yourself first. Backgrounding it manually returns control to
   the shell the instant the `&` is issued, so the tool marks that call
   "complete" right then, before the watch loop has run at all; every
   later terminal-event notification is then silently lost, and nothing
   tells you the wave finished. Watchers launched with
   `nohup fleet-watch.sh ... & echo $!` show "completed" within a
   second while `fleet_status` still reports
   `running` for every task. Interval: 120s for tasks expected under an
   hour, 300s otherwise. Never poll `fleet_status` in a foreground loop
   and never rely on a single SSE connection - it drops within seconds
   in this environment.

Failure playbook (in order of check):

| Signal | Action |
|---|---|
| done, but `git ls-remote origin refs/heads/task/<id>/result` empty | Work lost. Re-dispatch same spec once. |
| rate limit / auth / transient executor error | Retry once as-is. Second failure: re-dispatch with `label_selector` naming a different executor. |
| runtime ceiling (~4h kill) | Fleet recovers committed work. Fetch the recovered ref, re-dispatch as a CONTINUATION from that ref (`ref:` param, spec says what remains). Never restart from scratch. |
| second re-dispatch also fails | Stop the wave, ledger `blocked`, report to user. Do not silently drop the task from the epic. |

Every dispatch, retry, and terminal event gets a ledger line when it
happens.

## Stage 4 - Checkpoint (per wave)

Runs after every wave task is terminal with a verified result ref, and
BEFORE any merge.

1. Fetch all result branches. Build the combined diff against **current
   origin/main**, not the dispatch-time HEAD - main moves under you.
2. Spawn one adversarial review subagent: Agent tool, general-purpose,
   `model: opus`, `isolation: "worktree"`. The reviewer runs real git
   commands (checkouts, scratch clones, rebase drills against edge
   cases) in the course of verifying scripts; without worktree isolation
   it operates on the dispatching session's own shared checkout and can
   silently discard uncommitted local edits as collateral: a reviewer
   dispatched without isolation has reverted a hand-authored,
   uncommitted fix to its committed state,
   with no stash and no reflog entry, the exact corruption CLAUDE.md's
   workspace-isolation section warns about. Prompt it to hunt ONLY
   correctness bugs and invariant violations - not style, not naming,
   not structure:
   - durability depending on local disk; recovery reading another
     process's local state
   - mutation of data objects, commit records, manifests, index objects
   - in-place edits to frozen formats (RSEG, proto/, series identity,
     commit tokens, key layout) without ADR + version bump
   - unwrap/expect on production paths; `unsafe`
   - silent approximation; placeholder implementations on critical paths
   - acceptance tests that don't genuinely assert the claimed behavior
   Require findings as `file:line - claim - why it's wrong`, and a
   verdict: pass or block.
3. Block verdict: fix before merging anything. Small and mechanical fix
   locally (in a worktree); otherwise a fix task to the fleet. Re-review
   the changed area. Wave N+1 does not dispatch until pass.
4. Ledger the verdict, findings count, and fix commits.
5. Remove the reviewer's worktree. `isolation: "worktree"` only
   auto-removes it when the agent made no changes; a reviewer that ran
   scratch clones, rebase drills, or a block-verdict fix leaves it behind
   and returns its path in the result instead. Once its verdict and any
   fix commits are captured in the ledger (step 4), nothing reads that
   worktree again: `git worktree remove <path>` from outside it. Same
   failure shape as Stage 5's: an instruction living only in the tool
   description, not a numbered step here, is an instruction that gets
   skipped, and each skipped removal leaves a worktree carrying up to
   tens of gigabytes of build cache.

## Stage 5 - Land (per wave)

Work in a dedicated worktree of main (`git worktree add`), per CLAUDE.md.

1. Per task, in DAG order: `scripts/fleet-result-inspect.sh <task-id>`,
   write the PR message file (first line = PR title, body after the blank
   line carries `Fixes: #<sub-issue>`), then
   `scripts/fleet-result-merge.sh <task-id> <message-file> -p <crates>`.
   `main` is protected, so the script never pushes it: it cleans the
   result branch's history, runs local pre-flight gates, opens a PR, and
   enables auto-merge (`--rebase`) so GitHub lands it once the required
   checks pass. The merge-fleet-result skill covers gate failures, scope
   creep, and confirming the PR actually merged.
2. Real merge conflict: STOP and read the conflicting main commits first
   (`git log <merge-base>..origin/main -- <paths>`, full bodies). If a
   structural decision killed the task's premise (an ADR, a format
   version change, a rewrite), do not force it through: preserve the
   branch, comment on the sub-issue with the pointer, ledger it, move
   on. Only overlapping-edit conflicts get resolved textually.
3. Verify each sub-issue actually closed (`Fixes:` closes, `Refs:` does
   not); close stragglers with a comment linking the merge.
4. Ledger the wave: merge SHAs, new main SHA, closed issues. Tick the
   epic checklist.
5. Remove this wave's worktree now (`git worktree remove <path>`), from
   outside it: every task's PR is open (or landed) and wave N+1 lands
   against a moved main anyway, so it creates its own fresh worktree in
   step 1 rather than reusing this one. Do this before starting wave N+1,
   not batched at epic end: a multi-wave epic that waits lets these
   pile up (each wave's worktree carrying its own build-cache
   target dir) instead of shedding them wave by wave.
6. Last wave only: update README and docs per CLAUDE.md's
   doc-currency rule, final ledger entry, close the epic.

## Ledger format

One `### Wave N` block in the epic issue's `## Ledger` section, edited
in place (`gh issue edit --body-file`, or a comment per wave if body
edits race):

```
### Wave N - <planned|dispatched|review|blocked|landed>
plan: T4 #103, T7 #106 (from main <sha>)
T4 #103 task=<task_id> done result=<sha>
T7 #106 task=<task_id> ceiling-killed; continued as task=<task_id2> from <recovered-sha>
review: pass (opus, 2 findings fixed in <sha>)
landed: <merge-sha> <merge-sha2>; main=<sha>; closed #103 #106
```

## Resume after a dropped session

1. Read the epic issue ledger; find the last wave block and its status.
2. Any task_id without a terminal ledger line: `fleet_status` it, verify
   its result ref with `git ls-remote`, resume that stage's procedure.
3. `planned` -> dispatch it; `dispatched` -> re-arm watches;
   `review` -> rerun checkpoint (results are fetched, diff is cheap);
   `blocked` -> the block reason is in the ledger, resolve it;
   `landed` -> next wave.
Never re-dispatch a task whose result ref exists; merge it.

## Red flags - stop and reread the relevant stage

- "Executor reported gates green" - not the gate; local gates on the
  merged tree are.
- "Conflict, I'll just resolve it" - premise check first (stage 5.2).
- "These two tasks touch the same file but different functions" - one
  task or different waves. No exceptions.
- "Same crate, but their file lists are disjoint" - still one wave apart.
  Two tasks dispatched concurrently into one crate on this reasoning
  have collided: one added a parameter to a function the other grew a new
  call site for, the merge was textually clean, and the build broke on
  `E0061`. When one task changes a public signature, grep the other
  task's predicted files for that name before calling the waves disjoint.
- "Diff is small, skip the checkpoint" - checkpoint runs every wave.
- "I'll ask the user before wave 2" - the only gate is ADR approval.
  Asking mid-flight strands the epic (unattended rule).
- "I'll record the task_ids after they finish" - ledger before watch,
  always.
- "I'll clean up all the wave worktrees at the end" - remove each one at
  the end of its own wave (Stage 5 step 5); waiting compounds disk use
  across every wave still to come.
- "The reviewer's worktree is the harness's problem" - it's only
  auto-removed when the agent made no changes; a review that ran scratch
  git commands or applied a fix leaves it behind (Stage 4 step 5).
