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
  fixup follow it too: both commits that ever bypassed it were rationalized
  as "just a doc" and "just one file", and a concurrent session can sit on
  the primary checkout at any moment.
- Exception: fleet executors working in a dedicated clone. The clone is
  already the isolated workspace; commit directly on the dispatched
  checkout's HEAD (detached HEAD is fine). Do not create a side worktree
  or branch: the fleet harness collects only the dispatched checkout's
  HEAD as the result, and work committed anywhere else is lost when the
  workdir is destroyed (this happened; see the 2026-07-27 audit report,
  section 10).

## Merging fleet results

- A real (non-fast-forward-only) merge conflict between a fleet result and
  current `main` can mean two different things: overlapping edits (resolve
  textually), or a structural decision landed on `main` while the task was
  in flight and the task's whole premise is now stale (an ADR, a format
  version change, a crate rewritten from scratch). Before resolving, read
  the commit(s) on `main` that conflict — `git log --oneline
  <merge-base>..origin/main -- <conflicting paths>`, then the full commit
  body of whatever touched the same files. Forcing a stale-premise branch
  through reintroduces code or assumptions a deliberate decision already
  removed.
- This happened twice on 2026-07-28: ADR-0027 (single-RSEG-version
  pre-release) landed mid-flight under two long-running tickets built on
  the multi-version model it deleted. One had a partial file-level
  collision (some files merged clean, one file conflicted because it had
  already been rewritten for the new reality); the other's whole
  dependency chain (a path dev-dependency on a crate independently
  rewritten from scratch) needed re-targeting, not just conflict
  resolution.
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

None of those compile `ravel-server`'s SQL or Flight SQL surfaces: both sit
behind cargo features that are off by default. When your change touches
`ravel-server`, `ravel-sql`, or `ravel-query`, add:

```sh
cargo clippy -p ravel-server --features sql --all-targets -- -D warnings
cargo test   -p ravel-server --features sql
cargo clippy -p ravel-server -p ravel-sql --features flight-sql --all-targets -- -D warnings
cargo test   -p ravel-server -p ravel-sql --features flight-sql
```

`scripts/gates.sh` runs these when the crates are in scope. Do not skip them:
a workspace gate has printed "All gates passed" on a tree where
`--features sql` failed to compile, because the broken call site sat in a
target the default feature set never builds.

### Fast local iteration

While iterating, use `cargo check -p <crate>` for fast feedback (or
`cargo check --workspace` only when the change is genuinely cross-crate),
and scope clippy and tests to your crate with `-p`. Run the full gate list
above exactly once, immediately before the commit, not after every edit.
This is a local development-loop cadence only: it changes nothing about
what CI enforces on a pull request, which still runs the full fmt, clippy,
and test gates on every push. Where cargo-nextest is installed, `cargo
nextest run` is an accepted equivalent of `cargo test` (CI's check job
runs it with the `ci` profile); doctests still need `cargo test --doc`.

One narrowing for commits that can only land through a PR on protected
`main` (every commit since 2026-07-30): when the exact tree was already
taken through the full gate list once this session, later mechanical
steps on that same tree (the merge script's pre-flight, a re-push) may
skip the repeat run and let the PR's required checks enforce it —
that is what `FLEET_MERGE_SKIP_GATES=1` above is for. The full list
still runs at least once locally before the commit exists; protection
makes the repeats redundant, not the first run.

### Long commands and the Bash tool

The Bash tool stops a foreground command after 2 minutes by default. Its
`timeout` parameter accepts up to 600000 ms. Workspace clippy and test
runs, and the `sql`/`flight-sql` lanes, routinely run longer than 2
minutes: pass a long `timeout` on the call, or use `run_in_background`
and wait for the notification. Do not emulate waiting with repeated
`sleep N && tail` calls: a sleep of 120 s or more times out itself, and
each poll turn resends the full session context (one measured executor
session spent 39 of 205 turns on pure poll turns).

On an 8 GB host, default cargo parallelism gets ld killed with signal 9;
`gates.sh` caps build jobs there automatically. If you invoke cargo
directly on such a host, pass `--jobs 2`.

### CI workflow changes

No local gate compiles `.github/workflows/`. A workflow change is done
only when the pushed Actions run is green. Two defect classes have
shipped from here: `taiki-e/install-action@sccache` lacks the Cache
Service v2 tokens (use `mozilla-actions/sccache-action`), and
`CARGO_TERM_COLOR: always` puts ANSI codes inside cargo/nextest output,
which breaks any grep guard over that output (`--color never`, or strip
the codes first).

## Scripts

Use these instead of retyping the same shell each time; they exist
because the ad-hoc version of each has broken in practice (a stale SSE
connection, a pushed-but-broken main).

- `scripts/gates.sh [-p CRATE ...]` — the Gates list above. No args runs
  the full workspace gate; `-p CRATE` (repeatable) scopes clippy/test/doc
  to specific crates for fast iteration. It also runs the `sql` and
  `flight-sql` feature lanes, always in workspace mode and in scoped mode
  when `ravel-server` or `ravel-sql` is named.
- `scripts/disk-reap.sh [-y]` — reclaims disk from merged clean worktrees
  and orphaned cargo target dirs (dry run by default; `-y` applies). Run
  it when free space drops below ~20 GB, and after a land worktree's PR
  merges. Multi-session days have filled this volume to zero bytes free,
  at which point no Bash command can run at all and gates fail with fake
  errors; each manual cleanup used exactly the heuristics this script
  encodes.
- `scripts/fleet-watch.sh <watch-url> [poll-interval-seconds]` — waits on
  a `fleet_dispatch`/`fleet_status` task by polling its watch endpoint in
  a loop. The SSE stream it wraps drops the connection almost immediately
  in this environment, so a single long-lived `curl -N` never sees the
  terminal event; this retries instead. Prints the terminal event and
  exits 0 once one arrives.
- `scripts/fleet-result-inspect.sh <task-id>` — fetches a dispatched
  task's result branch and prints its commits and diff scope vs `main`,
  for review before merging. Never trust an executor's own "gates green"
  claim; look at what actually landed.
- `scripts/fleet-result-merge.sh <task-id> <message-file> [-p CRATE ...]`
  — cleans `wip:`/fixup commits out of the reviewed result branch, runs
  `gates.sh`, and opens a PR against `main` with auto-merge enabled
  (`main` is protected; the script never pushes or merges it directly).
  Task refs are left on origin until the PR is confirmed merged; see the
  merge-fleet-result skill for that step. Write the PR message to
  `<message-file>` first (trailers included; line 1 is the title, the
  body starts at line 3); this script does not construct one for you.
  Run `fleet-result-inspect.sh` first — this script does not pause for
  review, it assumes you already decided the scope is correct.
  `FLEET_MERGE_SKIP_GATES=1` skips the local `gates.sh` run and lets the
  PR's required checks be the gate. Use it only when the tree being
  merged is byte-identical to one that already passed the full gates
  this session (the common case: the orchestrator gated the result
  branch minutes earlier); the cost is learning about a red PR from CI
  instead of immediately. Never combine it with a conflict resolution
  or any manual edit to the branch.
- `scripts/verify-dispatch-gates.sh <ref> <scratchpad-dir>` — the tier-1
  gate check behind the `verify-dispatch` skill: an isolated worktree
  outside the repo, a cold `CARGO_TARGET_DIR`, and the full workspace
  gate list, regardless of which crate the branch touched. Run this (via
  the `verify-dispatch` skill, which adds narrow adversarial checks on
  top) before merging any fleet result — a crate-scoped or warm-cache
  gate run has let a broken branch through before.
- `scripts/affected-tests.sh [-n] -p CRATE [-p CRATE ...]` — runs tests
  for the named crates plus every workspace crate that depends on them
  (transitively), with the `ci` cargo profile; `-n` prints the affected
  set without running. This is the executor-side test gate in fleet
  specs: full-workspace tests still run at merge time
  (verify-dispatch-gates.sh and PR CI), so executors only pay for the
  blast radius of their change. Doctests included (nextest skips them,
  the script runs them separately).

### Writing gate and poll shell

Three shell bugs have each silently turned a failing gate or watch loop
into a false green. When you write or edit any such script:

- Capture an exit code as `cmd || code=$?` on the same line. `$?` read
  after an `if`/`fi` block reports the `if` construct, not the command
  (this exact bug made a draft of `verify-dispatch-gates.sh` report PASS
  on everything).
- Never name a variable `status`, `path`, `argv`, or `PWD`: zsh reserves
  them, and assignment kills the loop with `read-only variable` (killed
  the same Monitor poll loop twice).
- Never pipe a gate through `grep`, `head`, or `tail`, and never append
  `&& echo MARKER`: the pipeline's exit code masks the gate's.

## Fleet executor environment

Facts about the dispatched clone that executors have re-derived by trial
and error, one wasted turn (or one lost result) at a time:

- The host has 8 GB RAM and 4 cores. See "Long commands and the Bash
  tool" above for the `timeout` and `--jobs` consequences.
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
  the control plane, wait 30 s and retry, up to 5 times, and report the
  retries. Completed, gate-green work has been lost to a single
  unretried 502 at exactly this step.

## Commits

Conventional Commits: imperative header <=72 chars (feat/fix/docs/test/
chore, optional scope), body explains what and why in plain sentences,
wrap at 80. Sign off with `git commit -s`. Trailer `Refs: #<issue>` (or
`Fixes: #<issue>` when the commit fully resolves it). Plain language: no
em-dashes, no filler adjectives, no AI footers or self-references.

## Documentation stays current

Update documentation in the same commit as the behavior it describes, not
as a follow-up. A new endpoint or query capability updates README.md; a
status change updates PROGRESS.md; a format or protocol change updates its
normative doc below. A stale doc is a bug like any other, and the same
"report, don't silently fix" rule applies if you find one outside your
task scope.

### Doc map (read the doc that governs your crate; skip the rest)

| Crate | Normative doc |
|---|---|
| ravel-types | docs/adrs/0005, 0010 |
| ravel-object-store | docs/object-store-contract.md |
| ravel-segment | docs/segment-format.md |
| ravel-logseg | docs/log-segment-format.md |
| ravel-commit, ravel-catalog | docs/catalog-and-mvcc.md |
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
| Living log of what shipped, broke, and what's next | PROGRESS.md |
| Measured benchmark numbers, with the commands/environment that produced them | BENCHMARKS.md |

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
  tests must produce typed errors, never panics or wrong data.

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
