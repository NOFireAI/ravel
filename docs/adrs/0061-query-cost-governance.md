# ADR-0061: query cost governance

Status: Accepted

## Context

ADR-0044 (epic #418, closed) gave every query a `QueryAccounting` handle
that measures S3 requests and bytes fetched per phase
(`crates/ravel-types/src/accounting.rs`), and a pre-execution
`CostEstimate`, but shipped enforcement out of scope on purpose. A
2026-08-05 amendment enforced only the catalog term — a resolve whose
worst-case LIST count is too high is refused before any GET
(`CatalogConfig::max_catalog_list_requests`). Everything past that point
is unenforced: `QueryAccounting::add_s3_bytes` accumulates a real number
that nothing ever compares to a ceiling.

`EngineConfig` (`crates/ravel-query/src/config.rs`) has real, enforced
count caps — `max_segments`, `max_series`, `max_samples` — checked
incrementally during fetch (`engine.rs:896-918,1190-1221,1570-1599`) and,
independently, inside SQL's own scan loop
(`RsegScanExec::prepare_partition`, proven by
`crate::ravel_sql::tests::scan_budgets::max_series_rejects_before_every_segment_is_fetched`).
None of these three fields is byte-shaped. A selector that matches a
handful of series but whose covering segments happen to be large L1 parts
passes every count cap while reading and paying for an unbounded number
of bytes — this is finding S4-06/S3-04: count caps do not bound bytes
scanned, and nothing cancels a query on cost.

Query concurrency has the same shape of gap one layer up. `fetch_concurrency`
(`EngineConfig`, default 8) bounds concurrent segment fetches *within one
query* via a `tokio::sync::Semaphore` (`fetcher.rs:310,328`). Nothing bounds
concurrent queries within one process, and nothing bounds them across the
fleet. ADR-0057 (epic #656, closed) solved exactly this shape of problem —
unbounded aggregate demand across independent processes — for ingest
admission, via a periodic self-owned-key reconciliation: each process
writes its own current usage to a key only it writes, reads its siblings'
non-stale snapshots, and computes a local threshold from the reconciled
total. That mechanism is wired entirely into `AdmissionController`
(`crates/ravel-ingest/src/admission.rs`), which is ingest-specific state
(active-series tracking, series-creation rate) with no query-path caller
anywhere in the workspace. The *pattern* transfers cleanly; the code does
not.

Name-postings pruning (`crates/ravel-catalog/src/snapshot_format/postings.rs`,
consulted via `Catalog::resolve_pruned_with_accounting`) already prunes
whole segments before any GET, and already covers both query languages —
issue #278 (landed 2026-08-01, before this epic was scoped) wired SQL's
`pushed_down_name_filter`/`equality_name_filter`
(`crates/ravel-sql/src/executor.rs:475-511,900`) onto the same catalog
call PromQL's `equality_name_filter` (`engine.rs:1023-1059`) already used.
Both paths, identically, bypass pruning on anything but a lone equality
`__name__` matcher — a regex `__name__` selector in either language falls
back to an unpruned resolve, silently. This ADR's finding S3-10 is
narrower than issue #456's original framing: the gap is regex support in
the postings layer, on both languages equally, not "SQL has no postings
pruning" (SQL's equality path already landed). The name dictionary
(`postings.rs`) is a sorted structure, so a literal-prefix-anchored regex
(`^foo.*$`) can resolve via a bounded range scan the same way equality
resolves via exact lookup; a fully general anchored regex (infix,
alternation) cannot without scanning every name, and this ADR does not
attempt that case.

## Decision

### 1. Per-tenant bytes-scanned budget, checked incrementally, enforced identically in both query languages

A new per-tenant limit, `QueryLimits { max_bytes_scanned: ByteLimit }`
(`ByteLimit::Bounded(u64) | Unlimited`), following `AdmissionLimits`'
exact shape and lifecycle (`crates/ravel-ingest/src/admission.rs:86-98`):
loaded once at startup from a `--query-limits-file` TOML with a
`[defaults]` table and per-tenant `[tenants.<id>]` overrides, held
per-process, changing a limit is a restart. This is deliberately the same
shape operators already learned for ingest limits, not a new pattern.

Checked incrementally in the same places the existing count caps are
checked, once per completed segment fetch, comparing
`accounting.snapshot().total_s3_bytes()` against the tenant's
`max_bytes_scanned`:
- PromQL: alongside the existing `max_series`/`max_samples` checks in
  `engine.rs`'s three merge functions.
- SQL: alongside the existing `max_series` check in
  `RsegScanExec::prepare_partition`.

This duplicates the check per language rather than sharing one function,
matching this codebase's existing precedent for language-specific
enforcement sites (the count caps themselves are already duplicated this
way, not centralized). A future refactor that unifies PromQL's and SQL's
fetch loops could collapse this; this ADR does not require or block that.

A tripped budget cancels the in-flight query and returns a typed,
distinguishable error (mirroring `byte_budget_exceeded_returns_typed_error`'s
existing pattern in `ravel-sql`'s decoded-memory budget, a different
resource but the same "typed cancellation, not a generic timeout" shape).
Cancellation must release everything a normal completion releases
(in-flight fetch tasks, any reserved decode memory) — the existing
`dropped_mid_scan_stream_releases_tenant_bytes` test already proves the
memory-pool half of this for SQL; this ADR's acceptance test proves the
new byte-budget half.

## Amendment (2026-08-07): PromQL's enforcement site cannot deliver mid-scan cancellation

Decision 1's PromQL location clause — "alongside the existing
`max_series`/`max_samples` checks in `engine.rs`'s three merge functions"
— is incompatible with the same paragraph's own requirement ("checked
incrementally... once per completed segment fetch"). EF-1 (#721)
implemented the location clause exactly as written and its Stage 4
checkpoint caught the contradiction: `engine.rs`'s merge functions
(`build_series_by_id`, `merge_soa_runs`, `merge_histogram_soa_runs`) run
only after `fetch_all_series` / `fetch_all_samples_and_histograms` have
already drained every segment's fetch via
`stream::iter(...).buffer_unordered(concurrency).collect().await`
(`engine.rs:824-912`). By the time a merge function's loop runs, every
segment's bytes are already paid for — a bounded budget still issues
every GET and holds the full decoded result before returning an error.
The check moves the point of failure, not the point of cost.

SQL does not share this defect. `RsegScanExec::prepare_partition`
(`crates/ravel-sql/src/scan.rs:328-411`) is already a sequential
`for seg in &segs` loop that checks `max_series` and grows a memory
reservation once per completed segment, before fetching the next one —
genuinely incremental. Decision 1's SQL location clause is unchanged by
this amendment.

`QueryAccounting::add_s3_bytes` (`crates/ravel-types/src/accounting.rs`)
is an `Arc<Inner>` of atomics: every clone handed into a segment's fetch
task observes and contributes to the same live counters, so the running
total genuinely grows as each segment completes inside the
`buffer_unordered` stream, concurrently with its siblings still in
flight. The fetch functions can therefore check the budget as each
segment finishes and cancel the rest, instead of waiting for all of them:

- PromQL's enforcement site moves to `fetch_all_series` and
  `fetch_all_samples_and_histograms` (`engine.rs:824-912`) themselves.
  Replace each function's `.buffer_unordered(concurrency).collect().await`
  with a short-circuiting consumption (e.g. a manual loop over
  `StreamExt::next()`, or `try_for_each`) that, after each segment's
  result arrives, checks `accounting.snapshot().total_s3_bytes()` against
  `max_bytes_scanned` and returns the typed byte-budget error immediately
  if tripped. Dropping the stream at that point stops polling the
  remaining segment futures, cancelling their in-flight fetches
  cooperatively — the same release shape the memory-pool budget already
  proves for SQL. The `max_series`/`max_samples` checks in the merge
  functions are unaffected and stay where they are; this amendment only
  relocates the new byte check.
- A single-selector query (`plans.len() == 1`, the common case and the
  one S4-06 describes) has no sibling plans still fetching once its own
  segments are exhausted, so threading a live `&QueryAccounting` into the
  merge functions instead of a frozen `u64` snapshot would not recover
  mid-scan cancellation for it — the fix must sit in the fetch stage
  itself, not the merge stage, regardless of how the value is threaded.
- The acceptance test must prove cancellation stops in-flight work, not
  only that a bounded run returns an error: assert, via a fault-injecting
  or counting store, that fewer GETs were issued than the snapshot has
  segments when the budget trips partway through a multi-segment fetch.
  A test that only compares a bounded run's result against an unbounded
  run's does not distinguish real cancellation from a check that fires
  after the fact.

### 2. Fleet-global query concurrency ceiling via ADR-0057's count-cap reconciliation pattern

A new, independent controller — not an addition to `AdmissionController`,
which is ingest-scoped state and gains nothing from carrying an unrelated
resource type — tracking concurrently in-flight queries per process and
reconciling fleet-wide the same way ADR-0057 reconciles `active_series`:
self-owned snapshot key, periodic read of non-stale siblings, additive-
headroom formula (`fleet_used = own_current_usage + sum(non-stale
sibling usage)`, `crates/ravel-ingest/src/admission.rs`'s count-cap
formula, ADR-0057 section 2). Concurrent query count is a stock (how many
queries are open right now), the same shape as `active_series`, not a
rate — so this reuses the additive-headroom formula ADR-0057 shipped
correct the first time, not the rate-cap formula a checkpoint review had
to fix mid-epic (ADR-0057's own Correction section).

This is a single fleet-global ceiling, not per-tenant: the finding is
aggregate fan-out across tenants overwhelming the fleet, not any one
tenant's own concurrency. Snapshot key is process-scoped with no tenant
dimension — a new keyspace shape, `admission/query/<process_id>.snapshot`
at the bucket root rather than under a `t/<tenant_hash>/` prefix, since
this resource has no tenant to scope under. This needs its own IAM grant:
every mode that serves queries (`Mode::Query`, `Mode::All`) needs
`s3:PutObject`/`s3:GetObject`/`s3:ListBucket` on `admission/query/*`,
amending ADR-0055's role table the same way ADR-0057's own ingest
addendum did for `t/*/*/admission/*`.

A query that would exceed the local threshold is rejected before it
starts (before any resolve, let alone any GET) — this is an admission
decision, not a cancellation, unlike the per-query bytes-scanned budget
in decision 1 which cancels mid-flight. Staleness handling matches
ADR-0057 section 3 exactly (fail closed on the cap, guess stale sibling =
zero, self-correcting within `2R`).

### 3. Extend name-postings pruning to literal-prefix-anchored regex, both languages

Extend `postings.rs`'s name-dictionary lookup with a bounded range-scan
path: a `__name__` regex matcher whose pattern is a literal prefix
followed by an unanchored suffix (`^foo.*$` and equivalent shapes; the
existing PromQL/SQL regex matchers are already always fully anchored,
`^(?:expr)$`) resolves via a range over the sorted name dictionary
instead of bypassing to an unpruned resolve. A regex that is not
prefix-shaped (infix, alternation, non-prefix-anchored) keeps the
existing conservative bypass — this ADR does not attempt general regex
postings pruning. Both `engine.rs`'s `equality_name_filter` and
`executor.rs`'s `equality_name_filter`/`pushed_down_name_filter` gain the
same prefix-detection and range-scan call against the same
`Catalog::resolve_pruned_with_accounting`, following the existing
duplication precedent from decision 1 and from #278's own SQL wiring.

### 4. Corrected epic framing carried into the sub-issue

Issue #456's original text ("name-postings pruning does not extend to...
the SQL path") is stale relative to main: SQL's equality postings pruning
landed under #278 on 2026-08-01, before this epic was scoped against an
older snapshot of the codebase. The epic's sub-issue for finding S3-10 is
scoped to decision 3 above (regex only, both languages) rather than
re-wiring SQL from scratch.

## Rejected alternatives

**Fold the fleet-global query concurrency ceiling into `AdmissionController`
as a fifth tracked resource.** Rejected: `AdmissionController` is
ingest-specific state (`crates/ravel-ingest`), constructed and owned by
the ingest path. Giving it a query-shaped resource couples two otherwise
independent subsystems for no benefit — the reconciliation *pattern* is
what's worth reusing, not the struct.

**A single global (not per-tenant) bytes-scanned budget instead of a
per-tenant one.** Rejected: issue #456's own framing and finding S4-06
are about one tenant's selector reading unbounded bytes on the fleet's
behalf; a global-only budget would let one tenant's runaway query consume
the entire fleet's allowance before a second tenant's much cheaper query
ever gets a fair chance at it. Per-tenant, following `AdmissionLimits`'
existing config shape, costs nothing extra to build since the shape
already exists as a precedent.

**General (non-prefix) regex postings pruning.** Rejected on the same
cost basis ADR-0059 rejected whole-object-every-tick scrubbing: a name
dictionary has no index structure that makes an arbitrary regex cheaper
than a linear scan of every name, so "pruning" a fully general regex
would cost as much as not pruning it. Prefix-anchored regex is the
genuinely cheap case the sorted structure already supports; scoping to
it is honest about what's actually free.

**Re-wire SQL's postings pruning from scratch, as issue #456 originally
scoped it.** Rejected: it already exists (#278, landed before this epic
was scoped). Redoing it would be pure duplication against already-tested,
already-shipped code.

## Consequences

- Closes S4-06/S3-04's cost-cancellation gap and S3-04's concurrency gap
  with two independently landable mechanisms; a query can now be rejected
  before it starts (fleet concurrency) or cancelled mid-flight (bytes
  budget), and an operator configures both per-tenant the same way they
  already configure ingest admission limits.
- The fleet-global concurrency ceiling introduces a second reconciliation
  loop alongside ADR-0057's ingest one, on the same proven pattern but a
  new keyspace (`admission/query/*`) and a new ADR-0055 IAM grant for
  every query-serving mode.
- Regex postings pruning stays partial by design (prefix-anchored only);
  a fully general anchored regex still bypasses pruning, exactly as
  today, and this ADR does not claim otherwise.
- Neither the byte budget nor the concurrency ceiling is exact: the byte
  budget is checked once per completed segment fetch, so a single very
  large segment can overshoot the budget by up to that segment's size
  before the next check fires (the same granularity every existing count
  cap already accepts); the concurrency ceiling inherits ADR-0057's
  bounded staleness window (`2R`).
