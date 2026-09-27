---
name: fleet-task-spec
description: Use when writing a fleet_dispatch spec for this repo - templates the unattended rules, sizing, and scoping so specs stay short and tasks stay alive
---

# Writing a fleet task spec for Ravel

Fleet executors are unattended, context-limited, and cannot be woken once
their turn ends. CLAUDE.md already gives them the gates, commit
conventions, invariants, testing patterns and context discipline, so a
spec carries only what is task-specific plus the template's executor-only
paragraphs. `reference.md` beside this file holds the evidence behind each
rule; read it when a rule looks wrong for your case.

## Template

Copy it whole. The HARNESS REQUIREMENT paragraph is not optional: an
executor that obeys CLAUDE.md's worktree rule literally commits to a side
branch, `fleet_status` still reports `done`, and the work is lost with the
workdir. The first line declares `EXPECTS_REF: yes` for an implementation
task or `EXPECTS_REF: no` for a review, audit or research task.

```
EXPECTS_REF: yes

HARNESS REQUIREMENT (overrides CLAUDE.md's workspace-isolation section for
you: you are a fleet executor, not a local subagent): commit directly on
this dispatched checkout's HEAD. Detached HEAD is fine. Do not create a
separate git worktree or side branch: the harness collects only this
checkout's HEAD, and anything committed elsewhere is lost with the workdir.

UNATTENDED TASK: never ask for confirmation or approval; when your work
passes the gates, commit it (git commit -s) and end with a report.

DEGRADED-BOX TRIPWIRE, your very first command, before reading anything:

    time git config user.email "fleet-executor@nofire.ai"

If it took more than 30 seconds, STOP: do not read a file or start the
work. Report

    DEGRADED EXECUTOR: git config took <N>s
    <the output of `uname -a`>
    <the output of `nproc`>

and end the task. That box reaches the four-hour ceiling before a first
commit, so ending now costs only a redispatch, and the extra lines let the
orchestrator quarantine it. Otherwise run
`git config user.name "Ravel Fleet Executor"` and continue.

COMMIT EARLY: `git commit -s` the first state that compiles, before the
rest of the work; if a later gate fails, fix it and amend or add a
commit. Never put any command in the background and never end
your turn waiting for one. Nothing will wake you: your turn ending ends
the task, and anything uncommitted is lost with no result ref. A slow cold
build is expected; wait for it in the foreground.

Implement <issue ref> for the Ravel project: <one sentence>. Work ONLY
inside <crates/dirs>.

Read first: <the minimal normative docs, with section hints>.
Already on main: <the building blocks the task consumes, one line each>.

Deliverables:
1..n. <numbered, concrete, with file paths and API shapes>

Reachability: <the caller that will exercise this, named. If none does
yet, say so and name the ticket that wires it.>

Tests: <the specific behaviors to prove, including failure paths>.

Gates, in this order:
1. Format and lint in place: `cargo fmt --all` and, where it applies,
   `cargo clippy --fix -p <crate>`. The gated commit must already be
   formatted; never add a formatting-only fixup commit.
2. `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --
   -D warnings`; `scripts/affected-tests.sh -p <crate> [-p <crate2>]`.
   Do not run `cargo test --workspace`: merge time runs the full suite,
   and affected-tests.sh covers your crates plus every crate that
   depends on them.

Prefix every cargo command, and every script that runs cargo, with
`CARGO_BUILD_JOBS=<n>` (affected-tests.sh has no `--jobs` flag). n is
`min(4, max(2, nproc / 4))`, or 2 when total memory (`/proc/meminfo`
MemTotal, or `sysctl -n hw.memsize`) is 11 GB or less: a higher value gets
`ld` killed with signal 9 mid-link, which reads as a compiler error. The
value also overrides the cap inside `scripts/gates.sh`. Report `uname -m`,
`nproc`, the memory, n, and which bound decided it.

Run every gate unpiped and read its own exit code (`cmd; code=$?`),
never `| tail` / `| head` / `| grep`: a pipeline reports the last stage's
exit code, so a failed test can read as passed. Before the first gate,
run this as ONE command, substituting nothing:

    mkdir -p .gate-logs && git check-ignore -q .gate-logs && scripts/guards/check-disk-headroom.sh .gate-logs 5 && df -h /tmp . "$HOME"

If it fails, report that and stop rather than picking another directory:
a host without 5 GB for logs has no room for the gate either. Otherwise
redirect every long gate to `.gate-logs/<step>.log` and read the file.
Keep logs inside the checkout: HOME on the amd64 class is a 1 GB tmpfs,
`/tmp` is the harness's capture filesystem and filling it breaks every
later Bash call, and exporting `CLAUDE_CODE_TMPDIR` from inside the task
changes nothing. `.gate-logs/` and `.dd-tools/` (for any `cargo install
--root "$PWD/.dd-tools"` tree) are gitignored; never force-add them. Quote
the three `df` lines in your report. Commit with trailer "Refs: #N".

CLAIMS AUDIT, before the commit you gate, reported afterwards: list every
sentence you wrote or edited in docs, HELP text, doc comments or an ADR
that asserts a property of the system, and name the test or exact code
line that makes it true. Delete or qualify any you cannot pair with one.
Do not write "never" or "always" about behaviour you have not checked on
every path, and recompute every number you state against the tree you
are committing.

DISTINGUISHING TESTS, when this task delivers an acceptance test for a
behaviour change: name at least TWO plausible WRONG implementations the
test rules out, and show it failing against each, not only against
deleted code. <When the shape is known, name them here: e.g. "one that
counts per unit instead of per signal" and "one that hardcodes 1".>

CLASS CLOSURE: <when this change is an instance of a pattern, name the
grep> -- list every other site, and say fixed, out of scope with the
reason, or already handled and where.

OBSERVABILITY, when you add or document a metric family, label, flag or
report field: a test asserts it appears on the real surface a reader is
sent to. Name that test in your report.

Self-check before the commit: read your own staged hunks
(`git diff --staged`) for pasted tool output, conflict markers or
placeholders; no debug_assert-only guard where the condition matters in
production (make it a runtime check with a typed error); anything a doc
generator derives regenerated against your tree and committed in the
same commit; every new test shown failing against the pre-fix code,
with the flipped line named; nothing staged that the deliverables do not
name.

Report: <what the orchestrator needs to merge: deviations, counts,
ambiguities found>.
```

## Filling the template

- **Reachability.** Name the existing call site whose behaviour changes
  when this lands. If there is none, say so and name the follow-up
  ticket; the epic has not closed the gap until a real caller reaches it.
- **Tests.** For a fix, point at the prove-the-test skill (executors have
  it too) and require the report to name the flipped line. For a prune,
  pushdown, cache or other optimization, require a test that fails
  against the unsound implementation, and the flipped line in the report.
- **Distinguishing tests.** When you know the bug's shape, write the two
  wrong implementations in yourself ("a decoder that enforces the byte
  cap but not the frame cap"); that beats "demonstrate the test failing".
- **Class closure.** When the pattern is known, write the grep ("every
  construction of S3Config"). An executor shown one call site fixes one.
- **Numbers.** When the task reports a count or size, forbid `> 0` and
  non-empty assertions: pin an exact value where one exists, otherwise
  bound it proportionally (per object, row or shard, never a flat
  floor), and require the magnitude assertion itself to be shown failing
  by under-counting the source. Review specs ask the same question: does
  a test fail if the number is wrong, or only if it is missing?
- **Context discipline.** A task near heavy dependencies (arrow,
  datafusion, tonic internals) names the dependency and forbids reading
  its sources, even though CLAUDE.md covers it.
- **No shell substitutions.** fleet-cp rejects a spec containing `$(...)`
  or a backtick substitution (`400 bad request: spec contains an
  unexpanded shell substitution`), so every path in a spec is fixed text.

## Sizing rules

- One task fits one context window: one crate, or one module cluster.
  Past roughly five deliverables or two modules plus tests, split into
  sequential tasks and dispatch part 2 after part 1 merges.
- Parallel tasks have disjoint file scopes. A shared artifact (filenames,
  trait signatures) gets fixed names in both specs so merge order does
  not matter.
- Split a ticket along the compile/no-compile seam: a doc or diagram half
  that needs no cargo is its own task, dispatched in parallel and
  dependent only on the ADR.
- A spec must survive any hardware. `label_selector {"arch":"amd64"}` is a
  preference, and the scheduler falls back to other boxes. Do not add a
  stop on an unexpected architecture (a healthy Pi is slow, not broken);
  the DEGRADED-BOX tripwire is the check to keep, and the job cap stays
  derived rather than a list of machine shapes.

## Before dispatch

Run both in the same turn as the `fleet_dispatch` call.

1. **Resolve the ref with git, never from memory:** `git fetch origin
   main --quiet && git rev-parse origin/main`, straight into `ref`. Never
   type or complete a SHA by hand. For another ref shape (a short SHA, a
   remote-only branch, a result ref) run `git fetch origin <ref> --quiet
   && git rev-parse FETCH_HEAD` first; dispatch only pushes objects the
   local repo has.
2. **A dirty tree does not block the push.** The push is ref-based. Do
   not clean up, wait or retry over `git status --short`, and never touch
   a file you did not create: it is another session's. Only a dispatch
   that takes local HEAD instead of an explicit ref builds uncommitted
   state, so pass the ref.

## After dispatch

Record the task_id, arm the watch from the dispatch response under a
persistent Monitor, and land the result with the merge-fleet-result skill.

- **Probe the transcript early.** A task on a degraded box reports
  `running` until the ceiling kills it. Save the per-task JWT from the
  dispatch response and sample every few minutes for the first quarter
  hour:

      curl -s -o /dev/null -w "http=%{http_code} bytes=%{size_download}\n" \
        "$FLEET_CP/v1/tasks/<id>/transcript?token=<per-task-jwt>"

  `http=200 bytes=0` is a dead box: cancel and redispatch. A non-200 is a
  refused probe (the operator token instead of the task JWT returns the
  13-byte body `unauthorized`), not a measurement. The MCP
  `fleet_transcript` has the same ambiguity with no status code, so
  prefer the HTTP form.
- **A start-only ref is not proof of loss.** Executors push once, at the
  end. Confirm with an authorized empty transcript before cancelling;
  cancelling on the weaker signal destroys committed work.
- **Missing result ref.** With `EXPECTS_REF: yes` the work is gone:
  re-dispatch rather than try to recover. With `EXPECTS_REF: no` it is
  expected, and the report is the deliverable.
- **Verify refs with `git ls-remote origin refs/heads/task/<task-id>/result`**,
  never with `fleet_status`'s text (it can name a ref that was never
  pushed) or `gh api .../branches/<name>` (it lags by minutes). When a
  report says the final push failed, check ls-remote before
  re-dispatching: a retried push may have landed. Repeated control-plane
  502s call for a cooldown of minutes before the next dispatch.
- `fleet_status` needs the full task UUID; the executor's name appears
  only in the terminal event's `results.executor`.
