# ADR-0041: RSPAN v1 span segment format and trace routing

## Context

Epic 1 (traces + cross-signal correlation, issue TBD, program #333).
`Signal::Spans` is already a reserved `ravel-types` enum variant with key
prefix `s` (docs/catalog-and-mvcc.md), but no span segment format,
ingest path, or query path exists (confirmed by codebase survey). This
epic follows the Logs signal's precedent closely: a new segment format
crate, an ingest router mirroring the existing shard-actor pattern, a
`SegmentCodec` compaction arm, and a `ravel-sql` table, in that order.
`opentelemetry-proto`'s `trace` feature is already enabled workspace-wide
(`Cargo.toml`), so `ExportTraceServiceRequest` types exist with no new
proto vendoring.

Two things about span data make it a genuine departure from the Logs
precedent rather than a mechanical copy, and this ADR exists to decide
them before implementation starts:

1. **Routing.** RLOG shards by a derived `stream_id` (resource+scope
   identity), which gives good locality for "all logs from service X"
   queries. A trace's spans usually cross multiple services (that is
   the entire value of a trace), so resource-based sharding would
   scatter one trace's spans across every shard, making trace-by-id
   lookup - the epic's headline capability - a full fan-out across every
   shard and hour bucket in the query window. That defeats the point.
2. **Time shape.** RLOG's skip index stores one min/max timestamp per
   block. A span has a start and an end; pruning must be an interval
   overlap test, not a point-containment test.

## Decision

1. **New crate `crates/ravel-rspan`**: RSPAN v1, the span-segment sibling
   of RLOG (`ravel-logseg`) and RSEG. Reuses RLOG's proven section
   conventions (suffix-GET reader protocol, crc32c discipline,
   protobuf footer, columnar row blocks) rather than inventing new
   mechanics; the concrete departures are sections 2 and 3 below.
2. **Shard and sort by `trace_id`, not by resource/service identity.**
   `shard_for_span` hashes the leading bytes of `trace_id` mod
   `shard_count` - the same style of routing math `shard_for`/
   `shard_for_log` already use, just keyed differently. Records within
   an object sort by `(trace_id, start_ts)`. This keeps one trace's
   spans confined to one shard (modulo flush-boundary splits across
   time, which trace-by-id lookup already has to scan multiple hour
   buckets for regardless), so trace-by-id assembly is a bounded scan
   of one shard's buckets across the query window, not a scatter-gather
   across every shard. Service-scoped span search (e.g. "spans for
   service X in the last hour") consequently does need cross-shard
   fan-out; that is the accepted cost of this tradeoff (section
   "Rejected alternatives").
3. **Interval-aware skip index.** Where RLOG's `SKIP_IDX` stores one
   `(min_ts, max_ts)` pair per block, RSPAN's stores `(min_start_ts,
   max_end_ts)`. A query window `[T1, T2]` prunes a block when
   `max_end_ts < T1 || min_start_ts > T2`, an interval-overlap test
   using the same per-block min/max mechanism, not new machinery.
4. **Record shape**: `trace_id` (16 bytes), `span_id` (8 bytes),
   `parent_span_id` (8 bytes, nullable - root spans have none),
   `name`, `start_ts`/`end_ts` (both Timestamp ns), `status` (OTLP's
   status code + message), and one `attrs` `Map<Utf8, Utf8>` merging
   resource + scope + span attributes, following the exact
   resource+scope-wins-over-record-then-merge convention
   `docs/log-segment-format.md` already documents for logs (reused, not
   redesigned - the ADR-0033 amendment that fixed the logs residual-scan
   data-loss bug applies here too, so the scan-side re-verification
   pattern from `ravel-sql`'s `logs_scan.rs` is reused verbatim for a new
   `spans_scan.rs`).
   Span events and links (OTLP's nested repeated fields) are out of this
   ADR's scope - stored as an opaque serialized blob attr for now if
   present, not decoded into first-class columns; a later epic can
   promote them if a real query need shows up (approximation opt-in and
   visible: the blob is there, unindexed, not silently dropped).
5. **Parent/child relationships are a plain column, not a materialized
   tree.** `parent_span_id` is stored like any other field; service-graph
   construction (which service called which) is a query-time
   aggregation over parent/child edges across the `spans` SQL table, not
   a storage-format concern. This keeps the format simple and defers
   graph semantics to the query layer, where they can evolve without a
   format version bump.
6. **Phase breakdown**, sized against the Logs precedent's actual phase
   sizes (RLOG format ~6980 lines across 17 files; logs ingest ~2700
   lines; logs compaction ~1140 lines; logs SQL ~1640 lines):
   - Phase 1: RSPAN v1 format crate (writer/reader/footer/skip_index/
     span_dir), ravel-cli `rspan inspect`.
   - Phase 2: OTLP trace ingest (`traces_normalize.rs`/`traces_limits.rs`
     in `ravel-otlp`; `SpanIngestRouter`/`SpanShardActor` in
     `ravel-ingest` mirroring `LogIngestRouter`/`LogShardActor`; server
     handlers for `POST /v1/traces` and the OTLP gRPC trace service).
   - Phase 3: compaction (`SpanCodec` behind the existing `SegmentCodec`
     trait, ADR-0032's seam) and the one-line `FOLD_SIGNALS` addition in
     `services/ravel-server/src/fold.rs` (currently hardcoded to
     `[Signal::Metrics, Signal::Logs]` - a gap the Logs epic hit too and
     fixed the same way).
   - Phase 4: sweeper/retention - expected near-zero new code, since
     both already operate signal-generically on keys and timestamps.
   - Phase 5: `ravel-sql` `spans` table (schema/provider/pushdown/scan/
     UDFs mirroring the `logs` table pattern).
   - **Explicitly out of this epic, deferred to follow-up epics**:
     trace-by-id lookup as a dedicated query endpoint, service-graph
     construction, and exemplar linkage from metrics to traces. Phase 5
     (SQL over `spans`, filterable by `trace_id`) already gives ad hoc
     trace-by-id query capability; a dedicated fast-path endpoint and
     graph/exemplar features are new, uncalibrated scope with no Logs
     precedent to size against, and belong in their own ADR once phase
     1-5 numbers exist to plan against.

## Rejected alternatives

- **Shard by resource/service identity (mirror Logs exactly).**
  Rejected: makes trace-by-id lookup a full cross-shard fan-out for
  every multi-service trace, which is most of them - directly
  undermines the epic's stated goal (metric spike -> exemplar -> trace
  -> logs pivot in one store).
  Instead, service-scoped span search pays the fan-out cost.
- **A separate cross-shard trace-id index (a name-postings-style side
  index, ADR-0020's precedent) instead of routing by trace_id.**
  Considered as an alternative to shard-by-trace_id that would preserve
  resource-based shard locality. Rejected for this ADR: it adds a whole
  second index structure and a consistency story (index built how far
  behind ingest?) for a problem shard-by-trace_id solves for free at the
  routing layer. Not ruled out forever - if service-scoped span search
  turns out to matter more than expected, a resource-name index can be
  added later as an additive optimization without a format change.
- **Materialize parent/child as an explicit tree structure in the
  segment format** (e.g. a dedicated SPAN_TREE section). Rejected:
  premature - no query has been specified that needs O(1) tree
  traversal at the storage layer rather than a query-time join/
  aggregation over the flat `parent_span_id` column, and it would be a
  new section kind with its own crc/versioning surface for
  unvalidated benefit.
- **Decode span events/links into first-class columns now.** Rejected by
  the same "exact semantics by default, approximation opt-in and
  visible" reasoning as the RLOG `attrs` design: no calibration data on
  what queries actually need from them yet; an opaque, visibly-present
  blob beats guessing a schema that has to be a frozen-format decision
  once chosen.

## Dual-reader question

Not applicable - `Signal::Spans` has never had any data written to it in
any format; there is no prior version to keep reading.

## Checksum coverage

Same per-section/per-block crc32c discipline as RLOG/RSEG; no new
checksum design needed since the format reuses RLOG's section
mechanics with different column content and skip-index semantics.

## Consequences

- `crates/ravel-rspan` is genuinely new code, not a generalization of
  `ravel-logseg` (RSPAN and RLOG are siblings sharing a design lineage,
  not one format branching on signal) - this ADR does not attempt to
  unify them into one crate, since their column shapes and skip-index
  semantics differ enough that a shared abstraction would be premature.
- `shard_for_span`'s trace_id-based routing is a persistent-format
  decision (it determines which shard's object keys a given trace's
  spans land under) and is therefore frozen once phase 2 ships; changing
  it later is itself a format-change-ADR event.
- Service-scoped span search is cross-shard by design; if that path
  turns out to be latency-critical, the name-postings-style index
  rejected above becomes the follow-up ticket, not a redesign.
- Trace-by-id fast lookup, service graph, and exemplar linkage remain
  open, unscoped work after this ADR's five phases land - tracked as
  follow-up issues, not silently assumed to be "done" once Phase 5
  ships ad hoc SQL filtering.
