# fleet-task-spec: why each rule exists

Background for SKILL.md. Read a section when the rule it explains looks
wrong for the task at hand; a spec never needs to copy this text.

## Harness override and EXPECTS_REF

CLAUDE.md tells every agent to work in a dedicated worktree. A fleet
executor's dispatched checkout already is one; an executor that also
creates a worktree or branch inside it commits where the harness never
looks. `fleet_status` has then printed a `result at
refs/heads/task/<id>/result` line for a ref that was never pushed, and
the work was gone with the workdir.

Without `EXPECTS_REF`, a missing result ref is ambiguous: over one
four-day window, 30 of the 33 no-ref tasks were reviews that produce no
branch by design, so a blanket "no ref means lost" rule raised 30 false
alarms and trained the orchestrator to ignore the real ones.

## Commit early, never background

Tasks die mid-run more often than they fail. Over one four-day window,
27 of 165 dispatched tasks died, 13 of them to account rate limits that
kill the agent at provisioning or mid-stream with no result ref. Only
committed work survives, so the first state that compiles is committed
before anything else. On a host where one cold `cargo check` runs for
hours, "commit after step 1" and "commit the moment anything works" are
different instructions, and only the second survives a kill mid-build.

The no-background rule names every command, not just gates, because an
executor told "never background a gate" backgrounded a plain `cargo check`
as a diagnostic and ended its turn waiting for it. The fleet harness has
no notification that wakes an executor's turn, so the task sat for 3.5
hours (205 turns) with zero commits and was killed; `git ls-remote`
showed only the start ref, with no checkpoint or rescue bundle.

The degraded-box tripwire exists because one pool box takes 90 to 120
seconds per `git config` call and reaches the four-hour ceiling before a
first commit; tasks lost hours there, with nothing committed, before the
check existed.

## Executor test scope and formatting

Executors used to end with `cargo test --workspace`. On the 4-core Pi
class that is 1-2 hours of cold compile and test per task, almost all of
it re-verifying crates the change cannot affect, and all of it re-run at
merge anyway (the cold verify-dispatch run and PR CI are the trust
boundary; an executor's own green is never trusted). affected-tests.sh
covers the changed crates and their reverse dependencies. Workspace
clippy stays because it is check-mode, with no codegen or link, and is
the cheap whole-workspace compile-break detector.

Executors that gate first and format second landed formatting-only fixup
commits on result branches. The merge script squashes them, but the spec
stops them being created: formatting is a step before the commit exists.

## Build-job cap

An 8-core x86_64 box with about 15 GB of RAM is in the pool alongside the
16 vCPU amd64 class and the 4-core Pi. `CARGO_BUILD_JOBS=4` gets `ld`
SIGKILLed there during a cold `--all-targets` link, and gates.sh's own
memory-based cap (2 at 11 GB or less) does not fire at 15 GB. An
executor reading a list of two machine shapes that did not cover its box
had no instruction, which is why the template derives the value.

A spec once stopped on an unexpected architecture: the task landed on a
healthy Pi, the timing tripwire cleared it, and the arch stanza killed it
24 seconds in, turning a slow build into a lost one.

## Logs, /tmp and HOME

HOME on the amd64 class is a 1 GB tmpfs, so a log directory there makes
the headroom guard report 0 GB and fail before any gate runs; three tasks
died that way. `/tmp` is the harness's capture filesystem: a run that
fills it fails every later Bash call, including `true` and `df`, while the
host's disk figures look healthy. `CLAUDE_CODE_TMPDIR` is read when the
harness creates the capture directory, before the first Bash call, so
only setting it on the executor image helps (issue #1526). The harness's
commit-on-death `git add -A` would sweep untracked logs into a wip commit
that the merge script folds forward, which is why `.gate-logs/` and
`.dd-tools/` are in the tracked `.gitignore`.

## Reachability

Tasks have delivered correct, tested code that no user could reach: a
crate-tested cache that no caller constructed, a normalize entry point
nothing invoked, an attribute-postings index with nothing in production
building an attribute predicate, a prune channel whose intended caller
still used the old scan path. Each passed its own gates.

## Claims audit

Across epic #1678's 13 pull requests (10 review rounds on one, 5 on two
more, about 60 should-fix findings), roughly half of all findings were a
sentence, not a code defect: a gauge documented to surface a dip on a
counter that the biggest-dropping paths never reach; "a worker refusal is
never a 503" when a fetch-memory refusal is one by design; "a worker's
configuration is always an upper bound" under routing that clamps each
slice independently; a help string pointing at a metric family nothing
rendered. A number is a claim too: one sentence multiplied an
instantaneous count by an accumulating one and read as an upper bound
while bounding nothing.

## Distinguishing tests and class closure

A test that fails against deleted code can still pass against every
plausible wrong implementation. An acceptance test for a per-signal,
per-process sum used one bucket with one record on one shard of one
tenant, so `+= count`, `+= 1` and `= count` all passed. A cache-capacity
derivation missing its signal multiplier passed a single-signal fixture.

Fixes that closed two of three sites were the most repeated cause of an
extra round: `allow_http` derived from endpoint presence was fixed in the
server, then found in ravel-cli, then in the operator's S3 client; a
"never a 503" carve-out reached three intra-cluster copies and missed
three cross-cluster ones.

## Observability

A `flush_trigger_deferred_total` that exists in the crate and reaches no
exposition is not shipped, and help text naming it sends an operator
looking for nothing. The same goes for a report row whose producer does
not exist.

## Soundness and magnitudes

Results resting on an executor's own soundness reasoning have been right,
partly right, and wrong; the wrong one silently dropped half the rows of a
query and its report called the path unreachable. A fifteen-line test
showed it was reachable. A tie-break test on two elements passed against
the unfixed code because an unstable sort keeps order on short inputs.

A memory-pool charge shipped reporting about a quarter of its real
footprint under a test that checked `> 0` and return-to-zero.
Return-to-zero proves you released what you reserved, not that you
reserved the right amount. A flat 1 KiB floor also passed a figure that
counted one object out of three; a per-object band caught it. The first
adversarial review missed the undercharge; the second caught it because
its prompt asked whether each assertion pins a magnitude.

## Transcript probe and refs

The transcript endpoint refuses the operator token with the 13-byte body
`unauthorized`. Through `wc -c` that reads as a small measurement, so a
size-only probe alarmed on every task; it was caught when a task that had
finished successfully also read 13 bytes. `GET /v1/tasks/<id>` carries no
executor field while a task runs, which is why placement cannot be
pre-checked and the tripwire and probe both exist.

A dispatch once carried a SHA whose first 8 hex digits were real and
whose other 32 were invented, and the task expired unclaimed. Five
dispatches pushed fine with three untracked, non-ignored files present
(#687), which is why a dirty tree is no reason to wait. The REST branch
listing has returned 404 on a result ref that `git ls-remote` showed the
whole time.
