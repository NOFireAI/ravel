---
name: fleet-task-spec
description: Use when writing a fleet_dispatch spec for this repo - templates the unattended rules, sizing, and scoping so specs stay short and tasks stay alive
---

# Writing a fleet task spec for Ravel

Fleet executors are unattended and context-limited. Two real failures shape
this skill: an agent finished hours of work, asked "want me to commit?",
and died unpushed; another burned its entire context window grepping
arrow-rs sources.

The repo CLAUDE.md already tells executors the gates, commit conventions,
invariants, testing patterns, and context discipline. Do not restate those
in specs. A spec carries only what is task-specific.

CLAUDE.md's "Workspace isolation" section tells every agent working in
this repo to commit inside a dedicated git worktree, never directly on the
primary checkout. That rule targets local subagents sharing one session's
tree. A fleet executor's dispatched checkout already is its own isolated
workspace; if it also obeys the literal instruction and creates a *second*
worktree/branch inside that checkout, the harness, which only collects
what ends up on the checkout's own HEAD, never sees those commits.
`fleet_status` will still report `done` with what looks like a result ref,
but the ref was never pushed and the workdir is gone: the work is silently
lost. This has destroyed finished work in practice. Every spec for this
repo must therefore open with the harness-override paragraph below, not
just the unattended one.

## Template

```
HARNESS REQUIREMENT (overrides CLAUDE.md's workspace-isolation section for
you specifically -- you are a fleet executor, not a local subagent sharing
someone else's session tree): commit directly on this dispatched checkout's
HEAD. Detached HEAD is fine. Do not create a separate git worktree or a
side branch for your own commits -- the harness only collects what is on
this checkout's HEAD; anything committed elsewhere is invisible to it and
lost when your workdir is torn down.

UNATTENDED TASK: never ask for confirmation or approval; when your work
passes the gates, commit it and end with a report. Committing
(git commit -s) is part of the deliverable.

Implement <issue ref> for the Ravel project: <one sentence>. Work ONLY
inside <crates/dirs>.

Read first: <the minimal normative docs, with section hints>.
Already on main: <the building blocks the task consumes, one line each>.

Deliverables:
1..n. <numbered, concrete, with file paths and API shapes>

Reachability: <the caller that will exercise this, named. If the task adds
a capability no existing caller reaches, say so here and say which ticket
wires it.>

Tests: <the specific behaviors to prove, including failure paths>.

Gates: format and lint IN PLACE before the commit you will gate -- run
`cargo fmt --all` (not just --check) and, where it applies, scoped
`cargo clippy --fix -p <crate>` -- then verify with `cargo fmt --all
--check`; cargo clippy --workspace --all-targets -- -D warnings;
scripts/affected-tests.sh -p <crate> [-p <crate2>]. Do NOT run
`cargo test --workspace`: full-workspace tests are verified at merge
time (verify-dispatch cold gate and PR CI); your job is the blast
radius of your own change, and affected-tests.sh computes it (the
named crates plus every crate that depends on them). The commit that
gets gated must already be formatted; never append a formatting-only
fixup commit after a failed --check.
Commit with trailer "Refs: #N".
Report: <what the orchestrator needs to merge: deviations, counts,
ambiguities found>.
```

## Executor test scope

The Gates template scopes executor tests with affected-tests.sh on
purpose. A fleet task used to end with `cargo test --workspace` on an
8 GB 4-core host: 1-2 hours of cold compile and test time per task,
almost all of it re-verifying crates the change cannot affect, and all
of it re-verified anyway at merge (the orchestrator's cold
verify-dispatch run and the PR's required CI checks are the trust
boundary; the executor's own green is never trusted). The executor-side
run exists for fast self-feedback, so it covers exactly the changed
crates and their reverse dependencies. Workspace clippy stays: it is
check-mode (no codegen or link) and is the cheap whole-workspace
compile-break detector.

## Format before you commit, not after

The Gates line above says to run `cargo fmt --all` (write mode, not
`--check`) before the commit for a reason. Executors that gate first and
format second land a second, formatting-only fixup commit on the result
branch when the check fails, and those fixups ride the result branch all
the way toward main. The merge path squashes such commits (see the
merge-fleet-result skill), but the spec is the right place to stop them
being created: a result branch should never contain a commit whose only
content is a formatting fix. Formatting is not a gate you react to; it is
a step you run before the commit exists.

## Reachability

Every spec names the caller. Tasks have delivered correct, tested code
that no user could reach: a merged, crate-tested cache that no caller
constructed; a normalize entry point nothing invoked; an attribute-postings
index that shipped with nothing in production building an attribute
predicate; a prune channel whose intended caller still used the old scan
path. Each passed its own gates. Each looked done on the ticket.

The question is not "does it compile and test". It is "which existing call
site changes behaviour when this lands". If the honest answer is none, the
spec says so and names the follow-up, and the orchestrator does not record
the epic as having closed the gap. A capability with no caller is a
half-finished feature that reads as a finished one.

## Soundness claims need a failing test

When a spec asks for a prune, a pushdown, a cache, or any other
optimization, require the executor to prove the sound case with a test that
FAILS against the unsound implementation, and to say in its report which
line it flipped to watch it fail.

"I reasoned it is sound" and "I proved it is unsound" get very different
scrutiny, and the first is where the defects live. Results that rest on the
executor's own soundness reasoning have been right, partly right, and wrong;
the wrong one silently dropped half the rows of a query and was described in
its own report as unreachable. It was reachable, and a fifteen-line test
showed it.

The same applies to a test that claims to pin a fix. A tie-break test built
on two elements passed against the unfixed code, because the standard
library's unstable sort preserves order on short inputs. Require the test to
be demonstrated failing, not merely written. The prove-the-test skill (in
this repo, so executors have it too) lists the known vacuity shapes; point
the spec's Tests section at it when the task is fix-shaped.

## Sizing rules

- One task must fit one context window: one crate, or one module cluster
  within a crate. If deliverables exceed roughly five numbered items or
  two modules plus tests, split into sequential tasks and dispatch part 2
  after part 1 merges to main.
- Tasks that sit near heavy dependencies (arrow, datafusion, tonic
  internals) get an explicit context-discipline paragraph even though
  CLAUDE.md covers it: name the dependency and forbid reading its sources.
- Parallel tasks must have disjoint file scopes. If two tasks need a
  shared artifact (filenames, trait signatures), fix the names in both
  specs so merge order does not matter.
- Split a ticket along the compile/no-compile seam when it bundles a
  cargo-loop half (code, tests) with a doc/diagram half (spec rewrite,
  SVG redraw) that needs no cargo at all. Bundled into one task, the doc
  half serializes behind the build loop for no reason, and one executor
  holds both for the full duration. Dispatch the doc task in parallel,
  dependent only on the design decision (the ADR), not on the code
  landing. Merge order handles any cross-references.

## Before dispatch: two mechanical preflights

Both failure modes below have burned real dispatches. Run both checks in
the same turn as the `fleet_dispatch` call, every time.

1. **Resolve the ref with git, never from memory.** For the common case:
   `git fetch origin main --quiet && git rev-parse origin/main`, and pipe
   that output straight into the `ref` parameter. Never type a 40-char
   SHA by hand and never complete a short SHA from memory: a dispatch has
   carried a SHA whose first 8 hex digits were real and whose remaining
   32 were invented, and the task expired unclaimed. For any other ref
   shape (a short SHA, a remote-only branch name, another task's result
   ref) run `git fetch origin <ref> --quiet && git rev-parse FETCH_HEAD`
   first; dispatch only pushes objects the local repo already has.
2. **Check `git status --short` on the primary checkout.** The dispatch
   push refuses to run on ANY uncommitted or untracked file, including a
   stray build artifact (a leftover `__pycache__/` has blocked dispatch
   after dispatch before anyone looked). Remove a file only when you can
   attribute it to a command you ran yourself (a `__pycache__/` from your
   own compile check). Any file you cannot attribute to yourself is
   another session's: wait 30 s and retry, up to 3 times, since it
   reliably self-clears, and never touch it and never ask the user.

## After dispatch

Record the returned task_id, arm the watch command from the dispatch
response as a persistent Monitor, and merge with the merge-fleet-result
skill when it lands. `fleet_status` needs the full task UUID; an 8-char
short form returns "not found". Result branches appear at
refs/heads/task/<task-id>/result; a done status with no result ref means
the agent never committed, and the workdir is already gone. Re-dispatch;
do not try to recover.

Do not trust `fleet_status`'s text at face value: it has printed a
`result at refs/heads/task/<id>/result` line even when that ref was never
pushed (the executor committed to its own side worktree/branch instead of
the checkout's HEAD; see the harness-override paragraph above). Verify
with `git ls-remote origin refs/heads/task/<task-id>/result` before
fetching; if it's empty, the work is gone and the only path forward is
re-dispatching the same ticket with the harness-override paragraph in
place.

Two more verification rules, both learned the expensive way:

- Always use `git ls-remote` for result refs, never
  `gh api repos/.../branches/<name>`: the REST branch listing lags the
  git protocol by minutes and has returned 404 on a ref that `ls-remote`
  showed correctly the whole time.
- When an executor's report says its final push failed (control-plane
  502s do this), check `git ls-remote` for the result ref BEFORE
  re-dispatching: a retried push may have landed after the report was
  written. Re-dispatching on an assumed loss destroys completed,
  gate-green work, so confirm loss, never assume it. Repeated 502s from
  the control plane also warrant a cooldown (minutes, not seconds)
  before the next dispatch attempt.
