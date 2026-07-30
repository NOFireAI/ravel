# ADR-0033: SQL query over logs (log storage phase 3)

Status: Proposed. Builds on ADR-0013 (Arrow/DataFusion SQL integration),
ADR-0029 (RLOG v1 log segment), the approved log-storage design
(docs/superpowers/specs/2026-07-28-log-storage-design.md, "Query"
section, phase 3), and log storage phases 1-2 (merged: `ravel-logseg`,
OTLP log ingest over HTTP/gRPC, durable to the `l` keyspace).

## Context

Logs are durable end to end (phases 1-2) but unqueryable: nothing reads
them back except `ravel-cli`'s inspector. Two gaps block a `logs` SQL
table, researched against the current codebase before writing this ADR:

**Gap 1 — catalog fold never runs for `Signal::Logs`.**
`services/ravel-server/src/fold.rs::spawn()` hardcodes `Signal::Metrics`;
its own doc comment says "ravel-server ingests metrics only today... a
future signal would add another task per (tenant, signal) here, not
change this function's shape." So `catalog/l/HEAD` is never produced,
and `Catalog::resolve(_, Signal::Logs, ...)` returns nothing today even
though the resolve/cache/snapshot-part code underneath it is already
fully signal-generic (`crates/ravel-catalog/src/{cache,snapshot_resolve}.rs`
have no metrics-specific branching at all; `cache.rs` already tests
`Signal::Logs`). `SnapshotEntry` (catalog.proto) already carries
per-object `min_event_ts_ns`/`max_event_ts_ns`/`sample_count`/
`series_count`, and `ravel-ingest`'s log shard actor already populates
these with record count and distinct-stream count
(`crates/ravel-ingest/src/log_shard.rs`) — so once HEAD/part objects
exist for `Signal::Logs`, time-range snapshot resolution works for logs
with zero format changes.

The one part of `fold()` that is not signal-generic is the *name-postings
build* (`build_postings`/`fetch_entry_names`,
`crates/ravel-catalog/src/fold.rs`): it decodes RSEG segments via
`ravel_segment::open_from_full`/`decode_catalog_v5` and requires a
`__name__` label. This is the P5a/P5b metric-name index specifically,
layered on top of the base fold as an optional pruning optimization —
metrics itself shipped and worked without it before P5a/P5b existed.
Logs folding does not need to call this step at all.

**Gap 2 — no query path reads RLOG.** `ravel-query`'s `SegmentFetcher`
(the PromQL and SQL paths' shared segment-fetch abstraction) is RSEG-only:
every public method operates on `ravel_segment::{Footer, SeriesEntryV4,
RunEntry}` types. `ravel-sql`'s `RavelTableProvider`/`RsegScanExec`
pipeline (footer-first fetch → label-matcher pruning → k-way merge →
cross-segment dedup) is likewise built entirely around RSEG's shape.
Nothing wraps `ravel_logseg::RlogReader` for query use; it is used today
only by ingest, `ravel-maintain` (RLOG compaction, ADR-0032, a sibling,
independent epic — #229 — with no file overlap with this one), and
`ravel-cli`'s inspector.

## Decision

**Wire `Signal::Logs` into the fold task, without building postings for
it.** Generalize `services/ravel-server/src/fold.rs::spawn()` to fold a
set of signals per tenant (or call it once per signal — either shape is
acceptable; the task decomposition picks one). Logs fold calls
`Catalog::fold` the same way metrics does, and skips the postings-build
step entirely — logs get base HEAD/part resolution, no name-postings
object. `FoldTaskConfig`'s `fold_interval` stays shared across signals
for v1 (both currently want the same 5-minute cadence; a per-signal
interval is a config-shape follow-up if that changes).

**Build a sibling fetcher, not a shared abstraction.** A new
`LogSegmentFetcher` in `ravel-query` (or a new small module — task
decomposition decides), wrapping `ravel_logseg::RlogReader`. It fetches
each candidate object's footer + STREAM_DIR (both already whole-section,
single-GET reads per `ravel_logseg::read_section`, issue #221) to decide
per-object relevance — time range from the footer summary, stream
membership from STREAM_DIR — before any block-level GET, mirroring how
`SegmentFetcher` already decodes a segment's catalog section before
paying for page fetches. In-object pruning (skip index, token blooms)
is `RlogReader::scan`'s job already; the fetcher does not reimplement it.

**Build a `logs` table in `ravel-sql`, structurally mirroring
`RavelTableProvider`.** A `LogsTableProvider` implementing
`datafusion::catalog::TableProvider`, built around an already-resolved
`Snapshot` for `Signal::Logs` exactly like the metrics provider — same
`supports_filters_pushdown` = always `Inexact` discipline (pushdown
narrows fetch, DataFusion always re-applies every predicate; this is
where exactness comes from on the metrics side and carries over
unchanged). Its own pushdown extractor recognizes: `ts` range
comparisons (segment-level pruning, same shape as metrics' `ts_lo`/
`ts_hi`), stream-identifying attribute equality (resolved per-object
against STREAM_DIR contents at fetch time — see below, not at the
catalog level), and word/phrase text search via a `has_word(col,
'literal')` scalar UDF plus literal extraction from plain `LIKE
'%word%'` patterns, both feeding `ravel_logseg::Predicate::HasWord` for
bloom-accelerated pruning inside `RlogReader::scan`. Schema: fixed
columns (`ts`, `observed_ts`, `severity_num`, `severity_text`, `body`,
`trace_id`, `span_id`, `flags`) plus one `attrs` `Map(Utf8, Utf8)`
column for dynamic attributes — mirroring how the metrics table already
exposes `labels` as a map rather than per-key columns, for the same
reason: a per-tenant, per-key column schema is a v-next refinement, not
a v1 requirement.

**Stream-identifying predicates resolve per-object, not per-catalog.**
A SQL predicate like `attrs['service.name'] = 'api'` cannot become a
catalog-level filter without a stream-identity index (see Rejected
Alternatives, A) — it resolves per-object: the fetcher decodes each
candidate object's STREAM_DIR, matches attribute predicates against each
entry's canonical `stream_attrs` blob (`ravel_logseg::stream_attrs_bytes`'
own encoding, already self-consistent with `LogStreamId`, issue #225),
and builds a `Predicate::StreamIn` over the matching `stream_ref`s for
that object before calling `scan`. This costs one STREAM_DIR decode per
candidate object surviving time-range pruning — acceptable for v1, and
exactly the placeholder this ADR's Rejected Alternatives section below
argues should stay a placeholder until real query latency says
otherwise.

**One SQL endpoint, two tables.** No new HTTP endpoint. `SqlExecutor`'s
per-query `SessionContext` (`crates/ravel-sql/src/session.rs`) registers
`logs` alongside `samples`, resolving `Signal::Logs`'s snapshot only
when the query text references the `logs` table (avoid an unconditional
second catalog resolve on every metrics-only query). `POST /api/v1/sql`
is unchanged; DataFusion picks the table from the query's `FROM` clause.

**Correctness gate: exact equality, not bit-pattern, and scan-oracle
only for v1.** Mirror the *structure* of the existing two-layer
differential gate (docs/arrow-datafusion-plan.md), not its float-specific
comparator. Layer 1 (scan oracle): an independent reference that
resolves the same `Signal::Logs` snapshot, fetches every surviving
object, and evaluates the query's predicate by direct record iteration
(no skip index, no bloom, no DataFusion) — compared against
`LogsTableProvider`'s actual output by exact row/byte equality. Layer 2
(operator gate) is not built in v1: the log SQL subset has no
aggregation semantics yet (no `min`/`max`/`avg`-equivalent over text),
so there is no float-exactness class of bug for it to guard against;
add it when/if log aggregation ships.

## Rejected alternatives

**A. Build the stream-postings catalog index now**, per the original
design spec's Query section ("catalog snapshot parts for `l` carry... a
stream postings list"). Rejected for this epic: the existing postings
codec (`NamePostings`/RNP1, `crates/ravel-catalog/src/snapshot_format/postings.rs`)
is hard-typed to a UTF-8 string key (`decode_postings` explicitly
`str::from_utf8`s the name bytes) and cannot carry a 128-bit
`LogStreamId` without a new envelope and new, additively-numbered proto
messages in `proto/ravel/catalog.proto` — a frozen-contract change
requiring the full format-change procedure (ADR, explicit versioning,
dual-reader question, checksum-coverage review, corrupt/fuzz tests,
`ravel-cli` inspector support). It is also not required for correctness:
RLOG's own per-object STREAM_DIR + skip index + token blooms already
prune adequately once time-range resolution has narrowed the candidate
set, the same way metrics shipped and served real queries for phases
before name-postings (P5a/P5b) existed as a pruning optimization on top.
Revisit once real per-object STREAM_DIR-decode cost is measured against
actual query latency, as its own follow-up ADR.

**B. Generalize `SegmentFetcher` into a trait shared between RSEG and
RLOG.** Rejected: each format would still have exactly one concrete
implementation, and the two formats' pruning strategies differ enough
(label-matcher-driven catalog-section decode for RSEG vs.
footer/STREAM_DIR/bloom-driven for RLOG) that a shared trait would
either leak format-specific methods through the abstraction or reduce
to a near-empty pass-through. Matches this repo's own precedent: RLOG is
documented as "a sibling of RSEG... sharing conventions... none of the
bytes" (`ravel-logseg/src/lib.rs`), not a format unified with RSEG at
any layer.

**C. A single `telemetry` table spanning both signals.** Rejected:
metrics rows (numeric samples, fixed label set) and log rows (text
bodies, dynamic attributes, severity) have no common schema worth
forcing, and no query needs to scan or join both signals in v1. Two
tables, one session.

**D. A separate `POST /api/v1/logs-sql` endpoint.** Rejected: DataFusion
already resolves table names from query text inside one
`SessionContext`; registering both tables (resolving `logs`'s snapshot
lazily, only when referenced) reuses the existing endpoint, request
schema, auth, deadline, and error-mapping code unchanged, avoiding a
second endpoint's documentation and client surface for no behavioral
gain.

## Consequences

- No frozen-format changes in this epic. Confirmed: fold.rs signal
  wiring is ordinary `services/ravel-server` code; `SnapshotEntry`/HEAD/
  part formats are untouched and already sufficient; stream postings
  (the one piece that would need a format change) is explicitly
  deferred, not silently dropped.
- Catalog-level pruning for logs is time-range-only in v1. Stream/
  attribute filtering costs one STREAM_DIR decode per surviving object.
  This is a documented, revisit-able choice (Rejected Alternative A),
  not an oversight.
- This epic and the compaction epic (#229, ADR-0032) both read the
  commit-record/catalog layer but touch disjoint files (this epic:
  `services/ravel-server/src/fold.rs`, `ravel-query`, `ravel-sql`;
  #229: `ravel-maintain`, `ravel-logseg`'s footer) — no wave-planning
  conflict between them.
- Real new code volume: a fetcher and a `TableProvider`/scan-exec/
  pushdown module roughly proportional to what already exists for RSEG
  (`SegmentFetcher`, `RavelTableProvider`, `RsegScanExec`, `pushdown.rs`
  combined) — this is not a thin wrapper over existing metrics code.
- `has_word` join with the metrics UDAF registration precedent
  (`min`/`max`/`avg` in `crates/ravel-sql/src/session.rs`): one more
  function registered into the per-query `SessionContext`, no new
  isolation mechanism.

## Amendment (issue #239, 2026-07-30): the `attrs` column carries merged resource + scope + record attributes

The original decision above left one correctness gap unstated, found by an
adversarial review of the first `LogsTableProvider` implementation. It is a
data-loss bug, corrected here.

`supports_filters_pushdown` returns `Inexact` for every filter, so
DataFusion always re-applies the original predicate above the scan as a
residual `FilterExec`. A stream-identifying predicate written as
`attrs['service.name'] = 'api'` is thus evaluated twice: once by the scan
(resolved per-object against `STREAM_DIR` and re-verified against each
record's `stream_attrs` resource/scope blob, which is correct), and once by
the residual against the Arrow `attrs` column. The first implementation
populated that column from `LogRecord.attrs` alone (the per-record dynamic
attributes). Resource and scope attributes were therefore absent from it, so
the residual re-check of a resource attribute always failed, and a record
whose only match was a genuine resource attribute (the normal OTLP shape,
where `service.name` is a resource attribute) was silently dropped even
though the scan had verified its stream matched.

The fix: the `attrs` column carries each record's resource + scope
attributes (decoded from its `stream_attrs` blob) merged with its dynamic
per-record attributes, with the record's value winning on a key collision.
The data the residual checks and the data the scan's stream-identity check
verifies are then the same data, so the two agree and no row is lost. A
top-level resource or scope attribute whose value is itself a nested `Map`
or `List` is not projected into the map column (it is consumed but omitted),
matching the scan's existing rule that a nested pair never satisfies a
stream-attribute equality; a richer typed representation of nested values is
the same v-next refinement the per-key column schema is. Per-record dynamic
attributes with nested values are unaffected and still appear (rendered to
text).

This changes no persistent format and no query result that was previously
correct; it only stops dropping rows that should have been returned. It is
covered by an integration test that drives a real `TableProvider::scan`
inside a `SessionContext` so the residual `FilterExec` actually runs
(`crates/ravel-sql/tests/logs_provider.rs`).

Related, unchanged by this amendment but clarified here because the first
implementation's module docs misstated it: the `attrs['k']` subscript
*syntax* does not plan on this crate's DataFusion build. Lowering
`attrs['k']` to a `get_field(attrs, 'k')` scalar call needs a
nested-expression `ExprPlanner`, and this crate depends on `datafusion` with
`features = ["sql"]` only, registering no such planner. The subscript
therefore fails query planning with a hard error (loudly, never silently
mis-evaluated), for both the old record-only and the new merged-map `attrs`
column alike. Registering the planner in the query session/endpoint is an
explicit gate item for the session/endpoint wiring (issue #240); the
pushdown extractor already recognizes the lowered `get_field(attrs, 'k')`
shape so it works unchanged once that lands.
