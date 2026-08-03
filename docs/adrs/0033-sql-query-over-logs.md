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
column — mirroring how the metrics table already exposes `labels` as a
map rather than per-key columns, for the same reason: a per-tenant,
per-key column schema is a v-next refinement, not a v1 requirement.

**`attrs` carries resource, scope, and record attributes merged into
one map — not record attributes alone.** This is a correction of this
ADR's own first draft, which described `attrs` as holding only
per-record dynamic attributes while also treating `attrs['service.name']
= 'api'` as a stream-identifying predicate. Those two claims cannot both
hold under the `Inexact` discipline two paragraphs up: DataFusion's
residual re-applies the *original expression* against the *emitted
batch*, using only the columns in that batch. A predicate whose truth
depends on data absent from the batch — here, resource/scope attributes
that never appear in a record-only `attrs` column — is not sound to
mark `Inexact`, because the residual will drop rows the fetcher and
scan correctly matched. This is not a corner case: it silently loses
every row whose only match is a genuine resource attribute (the common
OTLP shape, e.g. `service.name` set once on the Resource and never
repeated per record).

The fix is architectural, not a scan-level patch: the column the
residual evaluates and the data the stream-identity check matches must
be the same data. `logs_scan.rs`'s `build_batch` populates `attrs` from
`LogRecord.attrs` (record) merged with the key-value pairs decoded from
`LogRecord.stream_attrs` (resource + scope, `ravel_logseg::
stream_attrs_bytes`'s own encoding). Precedence on key collision:
record wins over resource/scope. Log ingest (`ravel-otlp::logs_normalize`)
does not reject or deduplicate across these three attribute sources —
unlike the metrics path, which rejects a data-point attribute colliding
with a synthesized `job`/`instance` label (`DuplicateLabelName`) —  so
collisions are possible and this ADR fixes the resolution rule rather
than leaving it to whichever source happens to be applied last.
Record-wins matches the general OTel convention that the most specific
scope overrides the least specific. Scope name and scope version
(`stream_attrs_bytes`' first two fields) are positional, not key-value
entries, and are excluded from the merged map — no synthetic
`scope.name`/`scope.version` keys in v1; nothing in the query surface
needs them yet, and inventing keys for them now risks exactly the kind
of over-match the `Inexact` fix above is closing.

An alternative was to give stream-identifying predicates their own
syntax distinct from `attrs[...]` (e.g. a `stream_attr(key, value)`
scalar predicate), so the record-only `attrs` column and the
stream-identity check would never share a name. Rejected: it does not
solve the underlying `Inexact` problem, it moves it — the residual
would still need to re-evaluate `stream_attr(key, value)` against
*some* column in the emitted batch, and no such column would exist
under that design either. The one row-set columns can be evaluated
against is the one actually returned to DataFusion; merging is the only
way to give the residual real data to check.

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
otherwise. This per-object match is pruning only, same as the fetcher's
STREAM_DIR check and the scan's own re-verification of it — correctness
now comes from the residual re-applying the predicate against the fully
merged `attrs` column, not from any pre-DataFusion filtering step being
exact on its own.

**Amendment (issue #241): a stream-attribute predicate is not pushed as a
fetch prune at all.** The paragraph above is inconsistent with the merged-
`attrs` amendment two paragraphs up, and the inconsistency was a live data-
loss bug. It calls the per-object `StreamIn` match "pruning only," i.e. a
sound over-approximation that never drops a true result. Under the merged
`attrs` column with record-wins precedence, it is not: a `StreamIn` built
by matching a `(key, value)` against each stream's resource/scope
`stream_attrs` blob drops a record whose match for `attrs['k'] = 'v'` lives
only in its *per-record* dynamic attributes (resource `service.name =
worker`, record attribute `service.name = api`, query `= 'api'`). The
merged column resolves that key to the record's value and must keep the
row, but the record is never fetched, because its stream is absent from the
`StreamIn` set. That is a narrowing, not a widening — it violates the
pruning-soundness invariant, and no residual can recover a row the scan
never emitted. The same flaw applies to a scan-level re-check of
`stream_attrs`, which was the mirror-image bug (it dropped the record-
override case) originally masked by the record-only `attrs` column.

Because there is no *sound* stream-level prune for a merged-column
attribute predicate, `ravel-sql` no longer extracts `attrs['k'] = 'v'` into
a fetch prune at all (`crates/ravel-sql/src/logs_pushdown.rs`), and the
scan performs no per-record re-verification
(`crates/ravel-sql/src/logs_scan.rs`): it emits every fetched record and
lets DataFusion's mandatory `Inexact` residual evaluate the equality
against the merged `attrs` column — which is exactly what "correctness
comes from the residual, not any pre-DataFusion step being exact on its
own" already required. The fetch still prunes on the ts range (exact) and
content predicates (`has_word`, whose SQL semantics equal the reader's
exact filter). `LogSegmentFetcher::matching_streams` in `ravel-query` is
unchanged and remains available; `ravel-sql` simply no longer calls it,
because a stream-level match cannot soundly prune a merged-column
predicate. Restoring a sound stream-attribute prune would require a
record-attribute-aware index and is a deliberate follow-up (see Rejected
Alternative A), not a placeholder to re-enable in place.

**`attrs['k']` subscript lowering is a known gap, not fixed by this
ADR.** `ravel-sql`'s DataFusion dependency is configured with
`features = ["sql"]` only — no nested-expression `ExprPlanner`
(`GetFieldAccess`/`NamedStructField`) is registered, so
`attrs['service.name']` fails query planning outright today with a
loud `GetFieldAccess not supported` error, for both the record-only and
merged-map cases; this is unrelated to the merge fix above. Fail-loud is
acceptable for v1 under this repo's own posture (a hard planning error,
never a silently wrong answer) but the SQL surface is not usable end to
end until it is resolved. This is an explicit gate for the session/
endpoint wiring task (design spec phase 3's remaining task, T4): either
register the `ExprPlanner` (weighing it against the deliberate
no-nested-expressions dependency-surface stance) or document the
supported map-access syntax and have the planner reject the subscript
form with a clear error. `has_word(col, 'literal')` and `LIKE` do not
depend on this — both plan and push down today without it.

**Amended 2026-08-03: this gap is closed.** The gate above offered two
ways out; the first was taken. `crates/ravel-sql/src/map_field_planner.rs`
registers a hand-written `ExprPlanner` covering only the map-field case, so
`attrs['k'] = 'v'` plans and answers without enabling DataFusion's
`nested_expressions` feature. The no-nested-expressions dependency stance
this ADR weighed therefore survives intact: the subscript is supported and
the dependency surface did not grow.

The paragraph above is left as written because an ADR records what was
decided and why, and the reasoning that made fail-loud acceptable for v1 is
still the reasoning that governs any construct outside the subset. Only the
gap's status changed. The pruning gap described earlier in this section is a
different problem and is still open; ADR-0049 addresses it.

**One SQL endpoint, two tables, exactly one registered per session.** No
new HTTP endpoint; `POST /api/v1/sql` is unchanged. This paragraph's
first draft said the per-query `SessionContext` "registers `logs`
alongside `samples`" and that "DataFusion picks the table from the
query's `FROM` clause" — that is not what was built, and not what
should be built: registering both tables in one session would let
DataFusion itself plan a query that scans or joins both signals, which
Rejected Alternative C already says v1 has no use for and no operator
semantics to support. Instead, `SqlExecutor::target_signal`
(`crates/ravel-sql/src/executor.rs`) decides which single table a query
needs *before* any planning, by parsing the query's referenced base
tables with the same `sqlparser`/`DFParser` front end `crate::validate`
already uses (`referenced_base_tables`, `crates/ravel-sql/src/validate.rs`)
— never a raw-text scan, so a string literal or comment naming the other
table cannot change the decision. A query referencing only `samples` (or
neither table — a tableless constant query, preserving the exact
pre-ADR-0033 behavior of such a query) resolves `Signal::Metrics`; one
referencing only `logs` resolves `Signal::Logs`; one referencing both is
rejected outright, before any catalog resolve, with a dedicated
client-visible error (`SqlError::CrossSignalQuery`) rather than being
handed to DataFusion to fail on its own. `build_session` then registers
exactly the one table the query needs, plus that table's own scalar UDFs
(`label`/`label_match` for `samples`, `has_word` for `logs`) and no
others — the session exposes only what one query needs, the same
posture `crate::session`'s security-invariant-2 doc already states for
every other registration in that function. `Signal::Logs`'s snapshot is
therefore resolved only when a query actually references `logs`,
avoiding an unconditional second catalog resolve on every metrics-only
query, as the original paragraph intended — just enforced before
DataFusion is invoked, not by DataFusion itself.

A base-table-name parse has one known sharp edge: a `WITH <name> AS
(...)` common table expression named `logs` or `samples` shadows the
real table of that name for the query's own scope, but the visitor
above does not resolve CTE scoping — it collects every table-factor name
in the query tree, CTE names included. `crates/ravel-sql/src/executor.rs`
and `crates/ravel-sql/src/validate.rs` correct for this: CTE names
declared in a `WITH` clause are collected separately and excluded from
the signal decision, so `WITH logs AS (SELECT value FROM samples) SELECT
count(*) FROM logs` resolves `Signal::Metrics` (the only base table it
actually reads), not a false `CrossSignalQuery` rejection.

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
- The `samples`/`logs` table dispatch decided here, and the cross-signal
  rejection, are enumerated and scored in the SQL conformance table
  ([docs/sql-conformance.md](../sql-conformance.md), ADR-0035) alongside the
  aggregate allowlist and the read-only write rejections.
