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
EXPECTS_REF: yes

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

DEGRADED-BOX TRIPWIRE, your very first command, before reading anything:

    time git config user.email "fleet-executor@nofire.ai"
    time git config user.name "Ravel Fleet Executor"

You need both of these before your first commit anyway. Read the elapsed
time on the FIRST one before running the second: if it took more than 30
seconds, stop there. Waiting for both doubles the worst case, since the
box this exists for takes 90 to 120 seconds per command, and two of them
is three to four minutes rather than the two this paragraph used to
claim.

On a breach, STOP: do not read a file, do not start the work. Report

    DEGRADED EXECUTOR: git config took <N>s
    <the output of `uname -a`>
    <the output of `nproc`>

and end the task. The two extra lines are what make the report
actionable: one lost task tells the orchestrator the pool has a bad box,
and the box's identity is what lets it be quarantined instead of
rediscovered next week by a different task.

The four-hour ceiling arrives on that box before a first commit does, so
COMMIT EARLY below cannot save you. Ending in two minutes with a report
costs a redispatch; carrying on costs the whole task, and 2h50m and
3h29m have each been lost that way.

COMMIT EARLY: `git commit -s` the first state that compiles, before you
go on to the rest. Never put any command in the background and never end
your turn waiting for one. Nothing will wake you -- your turn ending ends
the task, and anything uncommitted is lost with no result ref. A slow cold
build here is expected; wait for it in the foreground.

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
fixup commit after a failed --check. Run every gate command UNPIPED and
read its own real exit code (`cmd; code=$?`), never `| tail` / `| head` /
`| grep` to keep the output small -- the pipeline's exit code is the last
stage's, not the gate's, so a real failure buried in a `tail`-truncated
tool-result can report a false "affected-tests passed" while a test
actually failed. If the output is long, redirect it to a file and grep or
read the file separately; the exit code check and the output-size problem
are independent, solve them independently.
Where that file goes is itself a rule, because both wrong answers have
already cost a task. Run this first, as ONE command, substituting
nothing:

    LOGDIR="$HOME/gate-logs/$(basename "$PWD")" && mkdir -p "$LOGDIR" &&
      scripts/guards/check-disk-headroom.sh "$LOGDIR" 5 && df -h /tmp "$LOGDIR"

One command because each tool call is its own shell: `LOGDIR` set in one
call is empty in the next, and the guard was reached with an empty first
argument, which it read as "no argument" and answered about the current
directory instead. It now refuses an empty argument, so the split
version fails loudly rather than passing about the wrong volume, but the
single command is what you run.

If the guard exits non-zero, say so in your report and stop rather than
picking another directory: a host without 5 GB for a log has no room for
the gate either, and the run would die mid-link with a fake compiler
error. Then redirect every long gate to
`"$HOME/gate-logs/$(basename "$PWD")/<step>.log"`, spelled out in full
each time for the same reason: nothing carries over between calls.
The two constraints that path satisfies, both of which have cost a task:
it is outside the git checkout, and it is not under `/tmp`. Inside the
checkout, the harness's commit-on-death runs `git add -A`, so a killed
task sweeps the log into a wip commit and the merge script folds it
forward into the PR; `.gitignore` carries no `*.log`, and the executor's
own stray-files self-check below cannot see a commit the harness makes.
It is also per-task by construction, since the basename of the checkout
carries the task id, which matters because several tasks can share a
host. Under `/tmp`, the evidence says the harness's capture filesystem
lives there rather than on the host disk: issue #1526 records the error
(`the temp filesystem at /tmp/claude-996/... is full (0MB free)`) and
`df -h /` reporting 56 GB available at the start of that same run. Once
it fills, every Bash call fails with ENOSPC, including `true` and `df`,
so the task cannot run a command to diagnose itself while the host's own
disk figures look healthy. That is why `df -h /tmp` is in the block
above: no run has yet captured it on an executor, and the next incident
needs the datum this one lacked. Quote both `df` lines in your report.
`CLAUDE_CODE_TMPDIR` is NOT the executor's lever: the harness reads it
when it creates the per-call capture directory, before the task's first
Bash call, so exporting it from inside a task changes nothing. Setting it
on the executor image is the real fix and is tracked on #1526.
Commit with trailer "Refs: #N".
Self-check before the commit (see the checklist below): no tool-call
artifacts in files, no debug_assert-only guards, generated docs
regenerated, tests demonstrated failing, no stray files staged.
Report: <what the orchestrator needs to merge: deviations, counts,
ambiguities found>.
```

## Self-check before the commit

Adversarial checkpoint review runs on every result branch before merge,
and the same defect classes keep coming back as extra review/fix rounds.
Each item below has blocked a real result branch. Put the self-check line
in every spec; the executor runs it against its own diff right before the
commit:

- **Tool-call artifacts in files**: a pasted tool result, a stray
  transcript fragment, an editor conflict marker, or a placeholder left
  in committed content. `git diff --staged` and read your own hunks.
- **`debug_assert`-only guards**: a safety check that compiles out of
  release builds is not a guard. If the condition matters in production,
  it is a runtime check with a typed error.
- **Generated docs**: if the change touches anything a doc generator
  derives (counts, tables, indexes), regenerate against YOUR tree and
  commit the output in the same commit. A hand-edit of generated output
  is undone at the next regeneration.
- **Vacuous tests**: the prove-the-test skill's rule applies to you -
  demonstrate the new test failing against the pre-fix code and name the
  flipped line in your report. A test that cannot fail proves nothing.
- **Stray files**: nothing staged that the deliverables do not name
  (scratch scripts, logs, `__pycache__/`, editor droppings).

## Commit before the slow gates, not after

A task is killed mid-run more often than it fails: over one four-day
window, 27 of 165 dispatched tasks died, and 13 of those were account
rate limits that kill the agent at provisioning or mid-stream with no
warning and no result ref. Only committed work survives. So the spec
orders the work commit-first:

1. Format in place, then make the change compile and pass the scoped
   tests.
2. `git commit -s` the working state.
3. Run the full gate list. Amend or add a fixup commit if a gate fails.

A kill during step 3 then costs a re-run of the gates, not a re-run of
the whole task. Two tasks in that window were lost the other way round.

Every spec must forbid the executor from backgrounding ANY command and
must name the consequence, because an executor that only reads "never
background a gate" will reason its way around it correctly and still die.

An earlier version of this section said exactly that, and a task was lost
to the gap: 3h29m, 205 turns and $18.29 on a Pi-class ARM host, with zero
`git commit` calls anywhere in its transcript. It backgrounded a plain
`cargo check` as a diagnostic, not one of the four named gates, wrote
"letting it run in the background, waiting for that now instead of polling
further", and ended its turn. Nothing resumed it: the fleet harness has no
notification that wakes an executor's own turn the way it wakes an
orchestrator's. It sat until something outside killed it at the 3.5h mark.
`git ls-remote` showed only the `start` ref -- no checkpoint, no result, no
rescue bundle, nothing to recover.

So state it as an absolute and give the reason, in the spec itself:

> Never put any command in the background and never end your turn waiting
> for one. Nothing will wake you: your turn ending ends the task, and
> anything not committed is lost with no result ref. A slow cold build on a
> Pi-class executor is expected -- wait for it in the foreground.

Pair it with an explicit COMMIT EARLY instruction rather than relying on
the ordering above being followed: the first state that compiles gets
committed immediately, before any further deliverable. On a host where a
single cold `cargo check` can run for hours, "commit after step 1" and
"commit the moment anything works" are different instructions, and only
the second survives a kill mid-build.

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

## Numbers need magnitudes, not `> 0`

When a task reports a count, a size, or any other measured quantity, the
spec must forbid asserting only that it is non-zero or non-empty. A `> 0`
assertion holds just as well when a figure is a fraction of the truth,
which is how an accounting bug survives a green suite: a memory-pool charge
shipped reporting roughly a quarter of its real resident footprint under a
test that checked exactly that plus return-to-zero. Return-to-zero proves
you released what you reserved, not that you reserved the right amount.

- Pin an exact value where one exists. Generated 60 records, count 60.
- Where the exact value moves with encoding or compression, bound it
  **proportionally to something known** — per object, per row, per shard.
  A flat floor is the trap: a floor low enough to be safe for one object is
  also cleared by a figure that counts one object out of three. That exact
  substitution passed a flat 1 KiB floor and failed a per-object band.
- Require the magnitude assertion itself to be demonstrated failing, by
  under-counting the source deliberately. An assertion nobody watched fail
  is not evidence that it pins anything.

This is also a REVIEWER's instruction, not only the executor's. The
undercharge above survived its first adversarial review and was caught by a
second one whose prompt asked, in as many words, whether each assertion
actually pins a magnitude. Put that question in the review spec: for every
number the change reports, does a test fail if the number is wrong, or only
if it is missing?

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
2. **A dirty tree does not block the push; it matters only when the
   dispatch takes local HEAD.** The dispatch push is ref-based: it pushes
   the objects behind the `ref` resolved in step 1, and it does not refuse
   on uncommitted or untracked files. Observed 2026-08-25 (#687): five
   dispatches pushed fine with three untracked, non-ignored files present
   the whole time. So do not clean up, wait, or retry over a dirty
   `git status --short`, and never touch a file you cannot attribute to a
   command you ran yourself: it is another session's. The caveat that does
   survive is the HEAD-implicit dispatch: when a dispatch uses local HEAD
   instead of an explicit ref, uncommitted work decides what gets built,
   so commit it (or pass the step-1 ref explicitly) first.

## After dispatch

Record the returned task_id, arm the watch command from the dispatch
response as a persistent Monitor, and merge with the merge-fleet-result
skill when it lands.

**Watch the transcript size, not just the status, and read the HTTP code
beside it.** A task on a degraded box reports `running` for its whole
life, then dies at the ceiling with an empty `result_ref` and only a
start ref. An authorized, empty transcript separates it from a hard task
within minutes:

    curl -s -o /dev/null -w "http=%{http_code} bytes=%{size_download}\n" \
      "$FLEET_CP/v1/tasks/<id>/transcript?token=<per-task-jwt>"

Save the per-task JWT from the dispatch response; without it this check
cannot be made. Healthy live task: `http=200 bytes=784005`. Dead box:
`http=200 bytes=0`. Sample every few minutes for the first quarter hour
and cancel-and-redispatch on an authorized zero rather than waiting for
the ceiling.

Read the status code, and do not use the size alone. Fetched with the
operator token rather than the task's own JWT, the endpoint refuses, and
the refusal body is the 13-byte string `unauthorized`. Through `wc -c`
that is a plausible small measurement rather than an error, so a probe
built on the size alarms on its first sample against every task, healthy
or dead. It was caught only because a task that had finished SUCCESSFULLY
also read 13 bytes; two tasks in opposite states with identical readings
is not a measurement. The status code is what separates a refusal from an
authorized empty transcript, which is why the command above prints it
first. The MCP `fleet_transcript` returning nothing carries the same
ambiguity with no code to inspect, so prefer the HTTP form.

A start-only ref after hours is suggestive, not conclusive: an executor
commits locally and pushes once at the end, so it can mean a lost final
push rather than no work. Confirm with the authorized-empty transcript
before cancelling, because cancelling on the weaker signal destroys
committed work.

Placement cannot be pre-checked: `GET /v1/tasks/<id>` carries no executor
field while a task runs, and the executor name appears only in the
terminal event's `results.executor`. That is why the tripwire in the spec
and this probe both exist, rather than a label selector. `fleet_status` needs the full task UUID; an 8-char
short form returns "not found". Result branches appear at
refs/heads/task/<task-id>/result.

**Every spec declares, in its first line, whether it produces a branch:**
`EXPECTS_REF: yes` for an implementation task, `EXPECTS_REF: no` for a
read-only review, audit, or research task. Without that declaration a
missing result ref is ambiguous, and the ambiguity is not rare: over one
four-day window, 30 of the 33 no-ref tasks were reviews that produce no
branch by design, so a blanket "no ref means lost" rule generates 30
false alarms and trains the orchestrator to ignore the real ones. With
the declaration:

- `EXPECTS_REF: yes` and no ref = the agent never committed and the
  workdir is gone. Re-dispatch; do not try to recover.
- `EXPECTS_REF: no` and no ref = expected. The deliverable is the report.

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
