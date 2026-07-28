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
| ravel-commit, ravel-catalog | docs/catalog-and-mvcc.md |
| ravel-ingest | docs/ingest.md, docs/consistency-model.md |
| ravel-otlp | docs/adrs/0005 (mapping note), crate module docs |
| ravel-otap | docs/otap-ingest.md, proto/otel-arrow/docs/ |
| ravel-promql, ravel-query | docs/query-engine.md, docs/adrs/0007 |
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
