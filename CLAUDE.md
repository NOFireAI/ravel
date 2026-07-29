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

## Scripts

Use these instead of retyping the same shell each time; they exist
because the ad-hoc version of each has broken in practice (a stale SSE
connection, a pushed-but-broken main).

- `scripts/gates.sh [-p CRATE ...]` — the Gates list above. No args runs
  the full workspace gate; `-p CRATE` (repeatable) scopes clippy/test/doc
  to specific crates for fast iteration.
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
  — merges the reviewed result branch (`--no-ff`), runs `gates.sh`, and
  only pushes `main` and deletes the task's remote refs if gates pass.
  Write the merge commit message to `<message-file>` first (trailers
  included); this script does not construct one for you. Run
  `fleet-result-inspect.sh` first — this script does not pause for
  review, it assumes you already decided the scope is correct.

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
