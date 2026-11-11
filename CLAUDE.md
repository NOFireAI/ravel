# Ravel: agent instructions

Ravel is a multi-tenant telemetry database. S3-compatible object storage is
the only durable backend; every compute process is disposable. These rules
apply to every agent working in this repository, including unattended fleet
executors.

## Unattended behavior

- Never ask for confirmation or approval. When your work passes the gates,
  commit it and finish with a report. An unanswered question ends the task
  with the work lost.
- If you find a contradiction between a spec document and code, or a bug in
  a crate outside your task scope, report it in your final message. Do not
  silently fix or work around it.

## Workspace isolation

- Always work in a dedicated git worktree, never directly on the primary
  checkout. Create one (`git worktree add`) before making any change, and
  remove it once your work is merged. This applies to every agent,
  including a local subagent dispatched into this same repo: a subagent
  editing files directly in the dispatching session's working tree, or
  two subagents sharing one tree, corrupts both in-flight edits and any
  concurrent `cargo` build cache. One worktree per unit of work, always.
  This rule has no small-change exception. A doc-only edit and a one-file
  fixup follow it too: every bypass gets rationalized as "just a doc" or
  "just one file", and a concurrent session can sit on the primary
  checkout at any moment.
- Exception: fleet executors working in a dedicated clone. The clone is
  already the isolated workspace; commit directly on the dispatched
  checkout's HEAD (detached HEAD is fine). Do not create a side worktree
  or branch: the fleet harness collects only the dispatched checkout's
  HEAD as the result, and work committed anywhere else is lost when the
  workdir is destroyed.

## Merging fleet results

- A real (non-fast-forward-only) merge conflict between a fleet result and
  current `main` can mean two different things: overlapping edits (resolve
  textually), or a structural decision landed on `main` while the task was
  in flight and the task's whole premise is now stale (an ADR, a format
  version change, a crate rewritten from scratch). Before resolving, read
  the commit(s) on `main` that conflict: `git log --oneline
  <merge-base>..origin/main -- <conflicting paths>`, then the full commit
  body of whatever touched the same files. Forcing a stale-premise branch
  through reintroduces code or assumptions a deliberate decision already
  removed.
- The failure shape: a single-RSEG-version ADR (ADR-0027) landed mid-flight
  under two long-running tasks built on the multi-version model it deleted.
  One had a partial file-level collision (some files merged clean, one file
  conflicted because it had already been rewritten for the new reality);
  the other's whole dependency chain (a path dev-dependency on a crate
  independently rewritten from scratch) needed re-targeting, not just
  conflict resolution.
- If the underlying logic (not the version/format-specific plumbing) is
  still valuable once the premise moves, don't discard it and don't force
  it through: preserve the branch, comment on the relevant issue with a
  pointer to it as reference material, and let a follow-up port it onto
  the new reality deliberately.

## Invariants (violating these is never a valid trade-off)

- Object storage is the source of truth. No durability may depend on local
  disk, and no recovery path may read state another process wrote locally.
- Data objects, commit records, manifests, and index objects are immutable.
- Persistent formats are frozen contracts: the RSEG layout
  (docs/segment-format.md), the protobuf schemas under proto/, canonical
  series identity and commit tokens (crates/ravel-types), and the object
  key layout (docs/catalog-and-mvcc.md). Changing any of them requires an
  ADR and a version bump, never an in-place edit.
- `unsafe` is denied workspace-wide. No unwrap/expect in production code
  paths; test modules carry `#[allow(clippy::expect_used)]`.
- Exact semantics by default. Approximation is opt-in and visible.
- No placeholder implementations on critical paths; no TODO that changes
  durability or query correctness.

## Gates (run all before any commit; CI runs the same)

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p <your-crate>        # plus --workspace when your change is cross-crate
```

Those do not cover `ravel-server`'s SQL, Flight SQL and ES|QL surfaces: all
three sit behind cargo features that are off by default. `cargo test -p
ravel-server` compiles none of them. A `--workspace` run that builds test
targets (`cargo clippy --workspace --all-targets`, `cargo test --workspace`,
`cargo nextest run --workspace`) does compile the SQL surface, because
`ravel-cli` has a dev-dependency on `ravel-server` with the `sql` feature and
cargo unifies features across what it builds. A workspace run that builds no
test targets does not: `cargo check --workspace` and `cargo build --workspace`
leave dev-dependencies out, so they check `ravel-server` without `sql`. None
of the three commands above compiles Flight SQL, ES|QL or the ravel-bench
features, and none builds `ravel-server` with `sql` selected on its own, without the
dependency features the rest of the workspace adds. (CI's `all-features` job
type-checks every feature at once with clippy; it runs no tests and is not
one of the local gates.) When your change touches
`ravel-server`, `ravel-sql`, `ravel-esql`, or `ravel-query`, add:

```sh
cargo clippy -p ravel-server --features sql --all-targets -- -D warnings
cargo test   -p ravel-server --features sql
cargo clippy -p ravel-server -p ravel-sql --features flight-sql --all-targets -- -D warnings
cargo test   -p ravel-server -p ravel-sql --features flight-sql
cargo clippy -p ravel-server --features esql --all-targets -- -D warnings
cargo test   -p ravel-server --features esql
```

`scripts/gates.sh` runs these when the crates are in scope. In scoped mode
(`-p CRATE`) scope is the reverse-dependency closure of the named crates:
the `sql` lane runs when ravel-server is in it, the `flight-sql` lane
when ravel-sql or ravel-server is, and the `esql` lane when ravel-esql,
ravel-sql or ravel-server is, so `-p ravel-query` runs all three. Two more
ravel-server lanes run whenever ravel-server is in the closure: `agents`
(clippy, the agents_seam, agents_effects_e2e, agents_e2e,
agents_timers_e2e, agents_projection_e2e and agents_real_store binaries,
the page-size-2 runtime and sim run, and the lib tests, plus
ravel-catalog's sys_proto_format_version_classification test;
agents_real_store prints a skip line and passes unless
RAVEL_REAL_STORE_URL is set) and `mcp` (clippy, the
feature-gated binaries the lane lists, and the lib tests), so `-p ravel-query`
runs five lanes, and each of them builds into the same `target/` as the
other four (the disk figures below predate these two lanes). The agents
lane has one more run with its own trigger, ravel-agents-runtime or
ravel-agents-sim in the closure (so any agents crate, not ravel-server
alone): those two crates' tests again with `RAVEL_AGENTS_PAGE_ENTRIES=2`,
where every paged list seals after two entries and the sim batches assert
their size-2 pins; CI's features-agents job and the sim-nightly workflow
run it too. An unscoped
run also runs `scripts/gates-sdk.sh` (npm ci, lint, typecheck and test
under sdk/typescript), which needs node 22 and refuses with exit 64
without it; `RAVEL_GATES_SKIP_SDK=1` skips that step and leaves CI's
`sdk` job as the gate for the SDK. A fleet
executor's gate is `scripts/gates.sh --tier executor -p <crate> [-p
<crate2>]` (ADR-4034 D1), not a hand-listed workspace clippy, and the
PreToolUse hook refuses `cargo clippy --workspace` in a fleet clone. That
tier tests only the named crates, compiles the rest of their closure with
one `cargo check`, and runs a lane's clippy and tests only for a named
crate the lane covers; CI runs the dependents' tests and the lane suites.
The merge gate is `--tier merge`, the default, which runs every lane
this section names. A fleet spec declares `gate_tier:` and `disk_floor_gb:` lines
in its head; `bin/agent-cli dispatch` refuses a spec without them and
`bin/agent-cli spec-lint` checks the same. gates.sh also runs a
ravel-bench lane (`--features sql-latency,profiling,flight-lane` clippy and
tests, plus `--features stage-timing`) whenever ravel-bench, ravel-sql,
ravel-query, or ravel-ingest is in scope, and a `-p ravel-logseg -p
ravel-ingest --features stage-timing` lane whenever ravel-bench,
ravel-ingest, or ravel-logseg is in scope, matching CI's `features` job. The
second lane exists because the first cannot replace it: it enables
stage-timing in both crates transitively, but a `-p ravel-bench` selection
builds them as dependencies and cargo never compiles a dependency's
`#[cfg(test)]` module, so their own gated tests run in neither.
gates.sh also runs a `-p ravel-cli --features sql-explain` clippy and test
lane whenever ravel-cli is in scope, matching CI's `features` job. And
it runs `cargo clippy -p ravel-ingest --all-targets` on its own whenever
ravel-ingest is in scope, matching the `lint` job: a selection that also
holds ravel-bench turns ravel-ingest's `flush-input-bytes` feature on, so
the scoped and workspace clippy runs never lint ravel-ingest as it ships. Do
not skip them: a workspace gate can print "All gates passed" on a tree where
a feature lane fails. For `flight-sql`, `esql` and the ravel-bench features
that is because the default workspace gate never compiles them. For `sql` it is
because the
workspace run builds `ravel-server` with the dependency features the rest of
the workspace adds, so a break that only appears with `sql` selected on its
own passes there.

### Fast local iteration

While iterating, use `cargo check -p <crate>` for fast feedback (or
`cargo check --workspace` only when the change is genuinely cross-crate),
and scope clippy and tests to your crate with `-p`. Run the full gate list
above exactly once, immediately before the commit, not after every edit.
This is a local development-loop cadence only: it changes nothing about
what CI enforces on a pull request, which still runs the full fmt, clippy,
and test gates on every push. Where cargo-nextest is installed, `cargo
nextest run` is an accepted equivalent of `cargo test` (CI's check job
runs it with the `ci-opt` profile, which is `ci` with third-party
dependencies optimised; use `ci` locally, since a cold build pays for that
optimisation, and `cargo nextest run --locked --workspace --cargo-profile
ci-opt` to reproduce a failure that appears only in CI); doctests still need
`cargo test --doc`.

One narrowing for commits that can only land through a PR on protected
`main`: when the exact tree was already taken through the full gate list
once this session, later mechanical steps on that same tree (the merge
script's pre-flight, a re-push) may skip the repeat run and let the PR's
required checks enforce it; that is what `FLEET_MERGE_SKIP_GATES=1`
(described under Scripts below) is for. The full list still runs at least
once locally before the commit exists; protection makes the repeats
redundant, not the first run. This precondition is enforced, not
remembered: `gates.sh` writes a receipt for every full clean-tree run,
and the merge script refuses the skip without a matching receipt.

### Long commands and the Bash tool

The Bash tool stops a foreground command after 2 minutes by default. Its
`timeout` parameter accepts up to 600000 ms. Workspace clippy and test
runs, and the `sql`/`flight-sql` lanes, routinely run longer than 2
minutes: pass a long `timeout` on the call, or use `run_in_background`
and wait for the notification. Do not emulate waiting with repeated
`sleep N && tail` calls: a sleep of 120 s or more times out itself, and
each poll turn resends the full session context, so a long wait spent
polling can consume a large share of a session's turns.

On an 8 GB host, default cargo parallelism gets ld killed with signal 9;
`gates.sh` caps build jobs there automatically. If you invoke cargo
directly on such a host, pass `--jobs 2`.

### Waiting on fleet tasks and PRs

Waiting is event-driven, never babysat. The harness kills background
processes after about 10 minutes; an unbounded watcher dies unnoticed and
the fallback becomes no-op status polls that burn turns and tokens.

- Arm `scripts/fleet-watch-loop.sh <watch-url>` under a Monitor, as the
  whole command. It relaunches the bounded watcher across budget
  rollovers and emits one line only when the watch is actually over.
  Writing that loop by hand in the Monitor command is a step that has
  been got wrong twice: a command that prints the 75 and exits ends the
  watch silently. Never nohup a watcher and never poll one with `ps`.
- Idle fallback wakeups: 15 minutes or longer. Embed the current ledger
  state in every wakeup prompt so a compaction between wakeups costs
  nothing.
- If two consecutive polls on the same task return nothing new, stop
  polling: fetch the fleet transcript and diagnose, or escalate.
- "Where are we at?" has one answer: `scripts/epic-status.sh <epic>`.
  Run it instead of repeated `gh issue view` reads, and post a one-line
  status to the epic ledger at each state transition so the user can
  self-serve.
- Both of the above are standing behavior, not something to do only when
  asked (issue #613: over four days, `epic-status.sh` ran 15 times
  against 440 hand-rolled `gh issue view` calls, and every fleet task
  death in that window was found by someone remembering to reconcile,
  never by an error surfacing on its own). Concretely: run
  `scripts/epic-status.sh <epic>` for every epic you are actively
  driving before dispatching new work on it, and again at each idle
  wakeup while a wave is in flight -- not only in response to a status
  question. On a terminal fleet event (a dispatched task reaching
  done/failed), send a `PushNotification` as part of handling that
  event, not only after the user asks what happened.

### CI workflow changes

No local gate compiles `.github/workflows/`. A workflow change is done
only when the pushed Actions run is green. Two defect classes have
shipped from here: `taiki-e/install-action@sccache` lacks the Cache
Service v2 tokens (use `mozilla-actions/sccache-action`), and
`CARGO_TERM_COLOR: always` puts ANSI codes inside cargo/nextest output,
which breaks any grep guard over that output (`--color never`, or strip
the codes first).

## Scripts

Use these instead of retyping the same shell each time; the ad-hoc version
of each has broken in practice. This is the index. `scripts/README.md`
holds the full entry for each script and `scripts/guards/README.md` the
full entry for each guard: flags, exit codes, environment variables and
the failure each one exists for. Read the entry before you rely on an
exit code or an escape hatch, and before you change the script.

- `bin/agent-cli`: use it first, and prefer it over re-reading files or
  guessing commands. One subcommand per lookup, terse output, `--json`:
  `pr`, `pr-findings`, `pr-reply`, `pr-review`, `pr-wait`, `pr-landed`,
  `merge-line`, `fresh`; `issue`, `issue-tick`, `issue-note`, `epic`;
  `task`, `dispatch`, `spec-lint`; `docs-check`, `authorship`, `box`,
  `wt`, `main`, `adr`. `bin/agent-cli <command> --help` has the details.
- `scripts/gates.sh [--tier executor|merge] [-p CRATE ...] [--dry-run]`:
  the Gates list above. No args is the full workspace merge gate. `-p`
  scopes to the named crates and their reverse-dependency closure.
  `--tier executor -p CRATE` is the executor's gate in fleet specs. A
  scoped run writes no gates-pass receipt. An unscoped run in a fleet
  clone is refused with exit 64.
- `scripts/affected-tests.sh [-n] -p CRATE ...`: tests for the named
  crates and their dependents; `-n` prints the set without running.
- `scripts/disk-reap.sh [-y]`: reclaims merged worktrees and orphaned
  target dirs (dry run by default). Run it below about 20 GB free.
- `scripts/fleet-watch-loop.sh <watch-url>`: the only watcher to arm under
  a Monitor. `fleet-watch.sh` and `fleet-watch-managed.sh` are its parts.
- `scripts/epic-status.sh <epic> [--fresh]`: the one-call ledger
  reconciliation and the answer to "where are we at?".
- `scripts/fleet-dispatch-intent.sh intent|record|failed ...`: wrap every
  dispatch in it. `intent` runs the fresh-ref and duplicate-work guards;
  pass `DISPATCH_PATHS="a,b"` with the files the task will touch.
- `scripts/epic-orchestrator.sh <command> <epic>`: the per-epic state index
  for a resumable orchestration; `reconcile` blocks the next dispatch (65)
  on drift.
- `scripts/ci-sweep-cancelled.sh [-y]`: reruns cancelled CI runs on open
  pull request heads; refuses to rerun a timeout (exit 3).
- `scripts/fleet-result-inspect.sh <task-id>`: commits and diff scope of a
  result branch. Run it before merging; never trust an executor's own
  "gates green" claim.
- `scripts/fleet-result-merge.sh <task-id> <message-file> [-p CRATE ...]`:
  cleans the result branch, runs the gates and guards, and opens a pull
  request without auto-merge, then requests the fleet review. Wait for the
  `claude-fleet[bot]` review and fix or answer every finding not marked
  `nit`. A review whose findings are all nits is clean. After 3 review
  rounds on one pull request, stop and hand it to a person.
  `FLEET_MERGE_SKIP_GATES=1` needs a gates-pass receipt for the same tree,
  younger than 24 h.
- `scripts/pr-review-status.sh <pr-number>`: merge state, CI rollup and
  fleet review state in one line, then the exact merge command pinned to
  the head it checked. Run that command as printed.
- `scripts/verify-dispatch-gates.sh [--with-gates] <ref> <scratchpad-dir>`:
  the cold-cache workspace gate behind the `verify-dispatch` skill.

### Guard scripts (mechanical preconditions)

Fast checks that fail closed before a known-expensive mistake. Run the
relevant one as a precondition, not after the damage. Any non-zero exit
means no, including 126 and 127, which mean the guard never ran.

Enforced without being remembered:

- `.claude/guards/pretooluse.mjs` (PreToolUse hook) refuses: a gate, a
  guard or a `git push` piped into `tail`/`head`/`grep`/`rg`/`sed` or
  followed by `&& echo`; an assignment to zsh's reserved `status`, `path`,
  `argv` or `PWD`; a `ScheduleWakeup` under 900 s; an `Edit`/`Write` inside
  the primary checkout; a bare 40-character hex literal in a Bash command;
  `reset --hard/--soft` onto a remote ref, `filter-branch`, and a
  force-push naming `main`; and, in a fleet clone, `cargo clippy
  --workspace` or a bare `cargo clippy` with no `-p`. Escape hatches, written inline at the front of the
  command: `RAVEL_GUARD_ALLOW_PRIMARY=1`, `ALLOW_DESTRUCTIVE=1`,
  `ALLOW_LITERAL_SHA=1`, `RAVEL_GATES_ALLOW_UNSCOPED=1`. Keep a SHA copied
  from a check just run (a `--match-head-commit` value); resolve any other
  SHA in the same command instead of typing it. Add a case to
  `.claude/guards/pretooluse.test.sh` before changing a rule.
- `.githooks/pre-push` refuses a push that would drop commits from `main`
  or from a branch with an open pull request. Install it once per checkout
  with `scripts/guards/install-git-hooks.sh`.
- `.claude/settings.json` is the team's. Personal tooling goes in
  `.claude/settings.local.json` and `.claude/helpers/`, both gitignored.

Run by hand, at the step named:

- Before the first edit of a unit of work: `assert-worktree.sh`.
- Before a dispatch: `assert-fresh-dispatch-ref.sh <ref-sha>` and
  `assert-no-duplicate-dispatch.sh --issue <n> [--paths a,b]` (both run
  inside `fleet-dispatch-intent.sh intent`). Exit 65: a pull request
  already closes the issue. 66: an open non-draft pull request touches a
  predicted path. 69: could not ask.
- Before the first action on a pull request that is not demonstrably
  yours: `check-duplicate-work.sh <pr-number>`. Several sessions share one
  `gh` account, so author and idle time say nothing about ownership.
  Claim the pull request with a comment before reading its findings.
- Before a landing sequence: `assert-gh-auth.sh`. After any manual amend
  or script-bypassing merge: `assert-clean-authorship.sh <ref>`.
- Before a merge: `assert-green-head.sh <pr> [--rerun]` (1 is a real
  failure, 4 a flake, 6 cancelled or timed out) and
  `assert-struct-literals-current.sh <branch>` (run by
  `pr-review-status.sh` and `fleet-result-merge.sh`).
- `assert-fresh-merge-base.sh <pr-number>`: only for a merge that bypasses
  the merge queue, or to diagnose a branch that will not enter it. The
  queue rebases each entry onto current `main` and runs full CI, so do not
  hand-rebase a pull request merely because `main` moved.
- Before `ALLOW_DESTRUCTIVE=1`: `show-destructive-loss.sh <target-ref>`.
- Before a cold `--all-targets`, `--workspace` or feature-lane build:
  `check-disk-headroom.sh [dir] [min_gb]`, and for anything longer than a
  few minutes also arm `disk-watchdog.sh <your-worktree>`. The scope
  argument is required: it kills only build processes inside it. A gate
  whose run overlaps `<scope-dir>/.disk-watchdog-fired` is invalid, not
  red.

Disk facts that decide whether a gate may start:

- ENOSPC shows up mid-link as a fake `linking with cc failed` (errno 28).
  A volume at zero bytes free stops every Bash command in every session.
- This host is also a fleet executor. `~/.fleet/executor` grows without
  warning, so an idle box does not mean an idle volume.
- A cold gate needs 54-70 GB of `target/` per worktree with no feature
  lane, and from about 93 GB to more than 188 GB when it reaches the sql,
  flight-sql, esql and ravel-bench lanes. This machine runs one at a time.
  Sequence with other sessions, or push the gate to pull request CI with
  `FLEET_MERGE_SKIP_GATES=1`, which still needs a valid receipt.

Source and repository checks, wired into `gates.sh` and CI's `doc-scripts`
job unless noted. Each has a suppression marker that needs a reason, and
each treats "could not check" as a failure; the README has the marker
syntax and exit codes:

- `check-test-suites-run.sh`: every tracked `*.test.sh` is named in a
  workflow. A new shell suite runs nowhere until it is.
- `check-guarded-sql-parse.sh`, `check-guarded-esql-parse.sh`: caller text
  reaches a parser only through the guarded entry points.
- `check-promql-unreachable.sh`: every `unreachable!` in ravel-promql
  carries an `unreachable-allow` marker with a reason.
- `check-release-profile.sh`: published binaries are built optimised.
- `check-quick-xml-entry-points.sh`, and
  `check-quick-xml-shipped-reachability.sh` (supply-chain job only): the
  RUSTSEC-2026-0194/-0195 ignore names every parent and no release build
  reaches an affected version.
- `check-workflow-permissions.sh`: every workflow has a read-only
  top-level `permissions:` block and every checkout sets
  `persist-credentials: false`.
- `check-changelog-touched.sh <base-ref> [head-ref]`: a `feat`/`fix`
  commit touching `crates/` or `services/` needs a `CHANGELOG.md` change
  in the range or a `Changelog: none` trailer. CI's `doc-scripts` job
  only; `gates.sh` does not run it.
- `check-amendment-integrity.sh`, `check-adr-band-sources.sh`: an ADR
  amendment's marker matches the document, and every band table row has a
  source. `docs/adrs/README.md`, "Amending an ADR", has the marker syntax.
- `scripts/check-injected-clock-helpers.sh`: no wall-clock wait in an
  injected-clock test helper. Scope is `services/ravel-cli/src/load.rs`
  only; issue #1278 widens it.

### Writing gate and poll shell

Three shell bugs have each silently turned a failing gate or watch loop
into a false green. When you write or edit any such script:

- Capture an exit code as `cmd || code=$?` on the same line. `$?` read
  after an `if`/`fi` block reports the `if` construct, not the command
  (this exact bug made a draft of `verify-dispatch-gates.sh` report PASS
  on everything).
- `set -e` at the top of a Bash tool call does NOTHING. The block is
  `eval`'d (zsh's own `(eval):N:` error prefix gives it away), and
  ERREXIT does not apply to the enclosing eval'd context, so it is inert
  at every depth: top level, inside `( ... )`, and inside a function. A
  partial rebase was pushed under one, and the fix that re-pushed under
  another was equally ungated and merely had nothing left to catch.
  SCRIPT FILES ARE UNAFFECTED: everything under `scripts/` runs as its
  own shell and its `set -e` behaves normally. This is about inline shell
  only. What works inline is one explicit refusal per dangerous step,
  `cmd || { echo "refused: ..." >&2; exit 1; }` or
  `if ! cmd; then ... exit 1; fi`. `echo "EXIT=$?"` reports and
  continues; printing a number beside a dangerous action is not a gate,
  and it relies on your attention at the moment you are most hurried.
  Verifying this rule is itself the trap, and four attempts across two
  sessions all failed the same way, by measuring a neighbouring shell:
  `bash -c 'set -e; ...'` and `zsh -c '...'` spawn a clean shell where it
  works, running the probe from inside a `.sh` file does the same, and
  appending `|| echo aborted` to see whether something aborts puts it in
  a condition context, which suppresses ERREXIT and disables the very
  behaviour being measured. Probe it inline, unconditioned, and read `$?`
  on the following line.
- Never pass `--body` inline to `gh issue comment` or `gh pr comment`
  when the text contains backticks. The shell command-substitutes them,
  the identifier runs as a command, its empty output is substituted, and
  `gh` exits 0 and returns a URL: the published text is missing exactly
  the names the argument rested on. Two sessions hit this within an hour.
  Use `--body-file` or `-F body=@file` with text written by the Write
  tool or a quoted heredoc, then verify the artifact rather than the exit
  code. Save the body to a file first and refuse on a failed fetch,
  because piping `gh api` into `grep -cF` loses the API status to the
  pipeline and a failed call returns the same `0` as a response missing
  the identifier, which is the could-not-ask/asked-and-got-nothing
  collapse this file warns about elsewhere:

      gh api repos/<o>/<r>/issues/comments/<id> --jq .body > /tmp/posted.txt \
        || { echo "could not fetch the comment" >&2; exit 1; }
      grep -cF '<an identifier you expect>' /tmp/posted.txt   # want >= 1
      grep -cF '<a string you know is absent>' /tmp/posted.txt # want 0

  Do not reach for `PIPESTATUS` instead: it is a bashism and expands to
  the empty string in zsh, so it prints `EXIT=` and reads as success.
  Repair in place with
  `gh api -X PATCH .../issues/comments/<id> -F body=@file`.
- Never name a variable `status`, `path`, `argv`, or `PWD`: zsh reserves
  them, and assignment kills the loop with `read-only variable`.
- Never pipe a gate OR A GUARD through `grep`, `head`, or `tail`, and never
  append `&& echo MARKER`: the pipeline's exit code masks the real one. The
  rule reads as being about gates and is not. On 2026-09-09 two sessions
  broke it on `assert-fresh-merge-base.sh` within the same hour, both while
  shortening its output: `guard 1556 | head -3` reported exit 0 on a branch
  three commits behind, and `out=$(guard N 2>&1 | tail -1)` reported 0 for a
  guard that was not in that checkout and had really exited 127. A guard
  that reports fresh on a stale branch is worse than no guard, because it
  retires the suspicion that would have caught it. The PreToolUse hook now refuses both forms rather than
  leaving this to memory. When you genuinely want short output and the
  truth, redirect to a file and read it separately; if you must pipe, bash
  keeps every stage in `${PIPESTATUS[0]}` (zsh spells it `${pipestatus[1]}`,
  and every script under scripts/ is bash).
- A plain `out=$(cmd)` DOES propagate cmd's status, including when `$?` is
  read on the next line; measured, not recalled. The two forms that lose it
  are `local o=$(cmd)`, where `local`'s own success becomes the status, and
  any pipe. Do not restructure a working assignment believing otherwise: a
  session diagnosed its pipe bug as an assignment bug and wrote the wrong
  mechanism into its notes.

## Fleet executor environment

Facts about the dispatched clone that executors have re-derived by trial
and error, one wasted turn (or one lost result) at a time:

- Two executor classes exist; tell them apart with `uname -m` and
  `nproc`. The amd64 class (label `arch=amd64`, x86_64, 16 vCPU, 30 GB
  RAM, about 350 GB free on the root volume) has a 1 GB tmpfs as HOME:
  never write logs or `cargo install` output under HOME there; install
  tools with `--root "$PWD/.dd-tools"`. The arm64 Pi class (aarch64, 4
  cores, 8 GB RAM) needs `CARGO_BUILD_JOBS=2` on every cargo command and
  on every script that runs cargo (`scripts/affected-tests.sh` takes no
  jobs flag); use `CARGO_BUILD_JOBS=4` on amd64. See "Long commands and
  the Bash tool" above for the `timeout` consequences.
- Gate logs go to `.gate-logs/` inside the checkout. The tracked
  `.gitignore` covers `.gate-logs/` and `.dd-tools/` so the harness's
  commit-on-death cannot sweep them into a wip commit; the fleet-task-spec
  skill's setup command checks that with `git check-ignore` first.
- fleet-cp rejects any dispatch spec whose text contains a dollar-paren
  command substitution or a backtick substitution with `400 bad request`.
  Write every path in a spec as fixed text.
- Fresh clones may carry no git identity, and the first `git commit -s`
  fails with "unable to auto-detect email address". Before your first
  commit, run: `git config user.email "fleet-executor@nofire.ai" &&
  git config user.name "Ravel Fleet Executor"`.
- `CARGO_HOME` varies per clone and is never `~/.cargo`. To locate a
  dependency's source, ask cargo: `cargo metadata --format-version 1 |
  jq -r '.packages[] | select(.name=="<crate>") | .manifest_path'`.
  Never hunt with `find /`.
- A source-only `git fetch origin <ref>` populates only `FETCH_HEAD`. To
  diff another task's result branch, fetch with a destination:
  `git fetch origin '<ref>:refs/remotes/origin/<ref>'`.
- If the final result push (or a git-start call) fails with a 5xx from
  the control plane, wait 30 s and retry, and report the retries. Do NOT
  treat 5 retries as a bound: the fleet-cp git proxy returns intermittent
  502s on both an executor's final result push and on `fleet_dispatch`
  start pushes, and an outage can run far longer than five attempts, long
  enough that a bounded retry loop gives up and loses an already-completed,
  gate-green result. Keep the 30 s backoff-and-retry loop running until
  the proxy recovers or you escalate to the user. A 502 on the FINAL push
  discards finished work outright (no result ref exists to fetch), which
  is exactly why an executor MUST have committed its HEAD before the push
  step: a lost push then costs a redispatch of push time, not of the work.
  When you detect a lost final push, redispatch from current
  `origin/main`, not the stale dispatch-time ref.

## Fleet ledger reconciliation (orchestrator sessions)

The in-flight task ledger is NOT authoritative on its own: your
conversational context is compactable, and a task that died silently
simply stops appearing in the tracked list with no error and no retry.
Before you dispatch ANY new fleet task on a poll tick, you MUST reconcile
the ledger against ground truth. If you skip this, a dead task's ticket
sits unfixed for the rest of the session while stacked tickets are
processed as if it were done.

- For every ticket the ledger marks dispatched-but-not-done, call
  `fleet_status` on its task id AND `gh pr view` / `gh issue view` on its
  ticket. If a task is terminal with no result ref, or the ticket is
  already merged, STOP and fix the mismatch before any new dispatch this
  tick: redispatch a silently-dead task from freshly-fetched
  `origin/main`, and mark a merged ticket done.
- A task that died at provisioning (ENOSPC on the executor's home dir, or
  a fleet-cp 5xx on the final push) pushes ZERO commits and leaves no
  result ref. Treat "the task dropped off my list" as a lost task to
  re-verify, never as a completed one.
- Persist the ledger outside the compactable context (a checked-in file or
  the epic issue body), and regenerate any "Wave N landed" claim from a
  live `gh pr list --json number,state,mergedAt` query over that wave's PR
  numbers, never from memory of which MERGED notifications fired.
- `scripts/epic-status.sh <epic>` performs this reconciliation as one
  command; run it on every tick instead of hand-rolling the queries.
- Wrap every dispatch in `scripts/fleet-dispatch-intent.sh`: `intent`
  before the `fleet_dispatch` call, `record`/`failed` after.

## Commits

Conventional Commits: imperative header <=72 chars (feat/fix/docs/test/
chore, optional scope), body explains what and why in plain sentences,
wrap at 80. Sign off with `git commit -s`. Trailer `Refs: #<issue>` (or
`Fixes: #<issue>` when the commit fully resolves it). Plain language: no
em-dashes, no filler adjectives, no AI footers or self-references.

Pull request shape (ADR-2867 change 4):

- One behaviour change per pull request. Two in one share every review
  round, red check and revert, so a finding on either holds both and
  reverting one removes the other.
- A dependency upgrade pull request carries the upgrade and the test
  re-pins it forces, nothing else. A new mechanism the upgrade enables is
  stacked as its own pull request, so reverting a bad upgrade does not take
  the mechanism with it and a reviewer can tell a re-pin from a behaviour
  change.
- A pull request that other work depends on and that has been open for
  more than 24 hours is split or landed, not grown. Every commit added
  moves the base its dependents rebase onto: #2774, a long-lived pull
  request on hub files, blocked three workstreams for two days and needed
  a 41-commit rebase (ADR-2867, Context).

## Documentation stays current

Update documentation in the same commit as the behavior it describes, not
as a follow-up. A new endpoint or query capability updates README.md; a
format or protocol change updates its normative doc below. A stale doc is
a bug like any other, and the same "report, don't silently fix" rule
applies if you find one outside your task scope.

### Doc map (read the doc that governs your crate; skip the rest)

| Crate | Normative doc |
|---|---|
| ravel-types | docs/adrs/0005, 0010 |
| ravel-object-store | docs/object-store-contract.md |
| ravel-segment | docs/segment-format.md |
| ravel-logseg | docs/log-segment-format.md |
| ravel-commit, ravel-catalog | docs/catalog-and-mvcc.md |
| ravel-agents-core, ravel-agents-store | docs/agents.md, docs/adrs/3331 |
| ravel-ingest | docs/ingest.md, docs/consistency-model.md |
| ravel-otlp | docs/adrs/0005 (mapping note), crate module docs |
| ravel-otap | docs/otap-ingest.md, proto/otel-arrow/docs/ |
| ravel-promql, ravel-query | docs/query-engine.md, docs/adrs/0007 |
| ravel-analytics | docs/analytics.md, docs/adrs/0028 |
| services/* | docs/architecture.md |

docs/consistency-model.md is normative for acknowledgement, visibility,
and crash behavior everywhere. ADRs live in docs/adrs/, one decision per
file.

### Repo-wide docs (not crate-specific)

| What | Where |
|---|---|
| Project overview, quickstart, PromQL/SQL query examples | README.md |
| Index of every guide and spec | docs/README.md |
| Getting started, ingest, query, operations, inspecting data | docs/guides/ |

## Testing patterns

- `MemoryStore` (ravel-object-store) is the semantics oracle;
  `MemoryStore::with_page_size(2)` exercises listing pagination.
- `FaultStore` injects faults by operation kind, key substring, and Nth
  occurrence; use it for every failure-path test and assert its counters
  so tests prove the fault fired.
- Time is injected. No `SystemTime::now()` in library logic; take a
  `Clock` or a `now_ns` parameter so tests are deterministic.
- Float comparisons in storage and dedup paths use bit patterns
  (`f64::to_bits`), never `==`. NaN payloads and -0.0 are significant.
- Property tests (proptest) for every codec and parser; corrupt-input
  tests must produce typed errors, never panics or wrong data. Check in
  the regression seed file where proptest writes it (with no
  `failure_persistence` override that is `proptest-regressions/` under
  the crate root for a property test in `src/`, and
  `tests/<name>.proptest-regressions` for one under `tests/`; both forms
  exist in this repo): a catch at 20,000 cases that is not replayed by
  the default test command is found again at gate time.
- A test pins the claim its commit message makes, in the words the
  message uses. "Asserts the exact split" means the test asserts the
  exact split, not that both sides are non-empty. Reviewers ask, for
  every claim in the message, which assertion fails when the claim is
  false and whether it was shown failing.
- A test-hygiene rule that bites twice becomes a check, not a third
  paragraph. Wall-clock bands, unpinned random ids, and missing seed
  files each cost a full gate rerun after the rule was already written
  down; the durable form is a grep in the gate script or a lint, so the
  pattern fails at authoring time.

## Measurement and cost discipline

Performance work here runs on real object storage and real corpora, and
every miss in this area has had the same shape: the evidence was already
in output that nobody asserted on. These rules make the assertion
mechanical.

- A number you print and do not assert on is decoration. Every load,
  fold, bench, and gate script states the expected band for each figure
  it reports (rows written, entries folded, statements measured, bytes
  moved) and exits non-zero outside it. A figure that was expected and
  not emitted, or emitted more than once, fails the same way as one
  outside its band; the check is "present exactly once and inside the
  band", not "inside the band if present". "Exit 0" means the tool ran,
  not that the work happened; a pass that measured one statement of
  forty-three and exited zero is the failure this rule exists for.
- Pre-register before measuring. Before a load or a query pass, write
  down the expected figures and the band that would count as a miss, on
  the tracking issue. A result that only gets a prediction after the
  fact cannot surprise you, and surprise is the signal.
- Reproduce before you dispute, and quote the reproduction. A claim that
  a published figure is wrong, unreconstructable, or computed on the
  wrong basis is filed only after grepping the publishing document for
  its own stated basis and running `scripts/reproduce-figure.sh`, with
  that output quoted in the filing. Quoting is the mechanism, not the
  running: an omitted quote is visible on the page, an omitted mental
  step is not. The same applies to a mechanism claim, which carries its
  count-check inline beside it. A published headline was once disputed
  as unreconstructable while the epic body stated the rule outright and
  a single-statement drop reproduced the total to the cent; the
  correction moved a planning target twice. An overcorrection is a claim
  too, and needs the same evidence as the claim it replaces.
- A measurement has preconditions on state, not only on code: the
  catalog folded to the last write (a fold seals an ingest hour only
  `max_flush_lifetime + clock_skew_allowance + fold_safety_margin` after
  it ends, so a fold run right after a load seals nothing), declared
  columns present, the on-object format version audited, the cache in
  the state the report claims. Verify each before the first statement,
  re-verify after any step that can change state (a fold, a compaction,
  a cache flush, a reload), and stamp what was verified into the report
  next to the expected figures, their bands, and the link to the
  pre-registration comment, so a report entry can be checked without
  the issue open beside it. A report without the stamp is not a result,
  and a tenant you did not verify is a tenant you did not measure.
- When a measurement misses, check in this order and do not skip a step:
  is the measured state the pre-registered state; does the per-phase cost
  split point at one layer; only then hypothesize about code or format.
  The first experiment changes exactly one variable, and it is the
  variable the stamps say differs (same binary and box against two
  tenants; same tenant and box against two binaries). An unfolded
  catalog once presented as a format that read 2x slower, at equal CPU,
  because the pooled GET counter hid one extra round trip per object.
- Cost is a first-class output of every read path, split by phase. Any
  code that touches object storage reports its requests and bytes under
  the phase that issued them (resolve, plan, probe, scan, decode), never
  into one pooled counter, and every byte figure names which bytes it
  counts: wire bytes as transferred, bytes charged to a pool, or
  decompressed bytes, and whether retries and range reads are included.
  Two phases reporting "bytes" of different kinds cannot be compared or
  summed. This is what makes a slowdown attributable in one query
  instead of one afternoon, and the exact-figure fixture rule above
  applies to it: a test pins the count per phase.
- A default that selects which data a command touches (signal, tenant,
  shard set, format version) is never silent when the alternative is
  plausible on that target: refuse, or act and say what was chosen and
  what was found empty. A fold that defaults to metrics on a logs-only
  tenant succeeds three times and seals nothing.
- Shared measurement hosts keep one binary per main SHA at a path that
  names the SHA. Scripts refer to that path, never to whatever the last
  build left in `target/release`; two hosts with different binaries
  under the same name turn a missing flag into a wrong default.
- Plan shape is data; lint it. The explain output captured for every
  bench statement is a regression detector for whole classes of
  performance bugs once a few shape rules run over it, each stated
  against a budget the statement's shape defines rather than as a bare
  comparison: a TopK whose input scan decodes columns that only the
  final result needs (the sort and filter can run on the narrow set and
  the rest can be fetched for the surviving k rows), a selective
  statement moving more wire bytes than the tenant's objects hold, a
  full scan issuing more whole-object GETs than the tenant has objects
  or more requests than its per-phase budget allows for probes, ranges,
  and listing pages. A rule without its budget rejects valid plans, so
  write the budget first. Each of the shapes above was visible in
  captured plans for several passes before a benchmark made it a
  ticket.
- Read your own output before running the next command. A counter that
  moved without an explanation (a LIST count from 9 to 14, a fold that
  reported zero entries, a full pass that finished in thirty seconds) is
  the next thing to investigate, before anything is built on top of it.

## Dependencies and context

- Add dependencies to your crate's Cargo.toml only, using versions already
  present in the workspace `[workspace.dependencies]`. A genuinely new
  external dependency must be flagged in your final report.
- Never read vendored or registry dependency sources wholesale into your
  context. Rely on the compiler's error messages; if you must check an
  API signature, use a narrow grep piped through `head -5`.
- Stay inside the crates your task names. The workspace root Cargo.toml,
  CI config, and other crates are out of scope unless the task says
  otherwise.

## Editing and hygiene

- Edit repo files with the Edit/Write tools, never `cat <<EOF` or python
  heredocs run through Bash: heredoc writes bypass the post-edit hooks
  and leave no auditable diff in the session. Heredocs are fine for
  scratchpad files.
- Code comments are for the next reader, not the reviewer. Write one
  only when the code cannot show the constraint itself, keep it to a
  sentence or two, and write none when the code is self-explanatory.
  Do not narrate history ("this used to", "a session once", incident
  stories): the why belongs in the comment only as the live constraint,
  the story belongs in the commit message.
- Parallel agents (fleet or local fan-out): derive every scratch path
  from your own task or agent id (`<scratchpad>/<agent-id>/...`), never a
  bare shared filename. Agents that share a name overwrite each other
  mid-task.
- Never paste live credentials into the conversation, and flag it in your
  report if the user does: transcripts persist on disk and later agents
  read them. Point at a keychain entry or environment variable instead.
