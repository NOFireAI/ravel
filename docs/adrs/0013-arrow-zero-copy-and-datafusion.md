# ADR-0013: Targeted Arrow zero-copy, DataFusion for SQL and relational operators only

Status: Accepted (2026-07-27); amended the same day after the adversarial
review (docs/reviews/2026-07-27-arrow-datafusion-plan-review.md). The
decision stands; several mechanism claims below are corrected, and the
plan document carries the full finding-by-finding response.

## Context

ADR-0006 deferred the Arrow/DataFusion question to Phase 3. That
evaluation is now due: a SQL analytics surface and Flight SQL interop are
wanted, RavelQL will need relational operators (where/project/join/stats),
and reimplementing a relational optimizer and executor by hand is wasted
effort. Separately, BENCHMARKS.md names allocation churn as a measured
bottleneck, inviting zero-copy claims that must be checked against the
frozen RSEG v1 layout: TS pages are lz4-compressed, Gorilla and varint
encodings are transforms, page crcs force a full read of every payload
byte, and VAL_RAW_F64 payloads sit at unaligned offsets inside fetched
`Bytes`, while arrow `Float64Array` requires 8-byte alignment.

## Alternatives

1. Keep the custom engine only, add SQL by hand. Full control, no
   dependency weight, but we reinvent parsing, planning, optimization,
   and relational execution that DataFusion already does well, and Flight
   SQL interop never gets a standard implementation.
2. DataFusion everywhere, including PromQL evaluation lowered onto
   relational plans. Rejected: lookback, staleness, counter resets, and
   native histogram arithmetic do not map onto relational operators
   without semantic loss; ADR-0006/0007 already decided this and nothing
   has changed the facts.
3. DataFusion for SQL, RavelQL relational lowering, and Flight SQL only;
   custom evaluator and ingest untouched; zero-copy applied only where
   the format physically permits, with format-dependent alignment work
   deferred to an RSEG v2 decision.

## Decision

Option 3. Concretely:

- A new `ravel-sql` crate owns all datafusion usage: a `TableProvider`
  and `ExecutionPlan` pipeline over catalog snapshots and RSEG segments,
  reusing the existing fetcher and pruning (time bounds to
  `Catalog::resolve` under a widen-only soundness invariant, matchers to
  SERIES_TABLE), with cross-segment dedup as a dedicated streaming
  operator above a sort-preserving merge, never a per-partition scan
  property. Exposed as `POST /api/v1/sql` and later Flight SQL, both
  behind cargo features. ravel-query stays free of arrow and datafusion;
  ingest-critical crates keep the ADR-0011 isolation (arrow in
  ravel-otap only).
- Two security invariants with the same standing as ADR-0011's
  structural isolation: the SQL surface accepts exactly one read-only
  SELECT statement (DDL, DML, COPY, SET, multi-statement rejected at
  parse time, no object store registered in the SQL runtime), and every
  query runs in a fresh single-tenant `SessionContext` with no shared or
  cached runtime state. Relaxing either requires an ADR.
- Query budgets bridge to DataFusion through a budget-sized per-query
  memory pool and a tenant-delegating `MemoryPool` implementation;
  budget exhaustion is an error, never a partial result; spilling stays
  disabled.
- Exactness is enforced by a two-layer differential gate: the scan plus
  dedup output is compared against the PromQL merge path on the same
  snapshot (an independent implementation of the same dedup total order,
  including the value-bit-pattern tiebreak), and every
  DataFusion-executed operator is compared bit-exactly (f64::to_bits)
  against a reference executor fed from that independent path, on
  property-tested datasets. Aggregations run single-partitioned with
  deterministic input order, and avg stays out of the v1 SQL subset,
  until an ADR defines anything looser.
- Zero-copy work is limited to what RSEG v1 permits: refcounted
  `Bytes::slice` on the fetch path (today `FetchedRegions::slice` copies
  every sliced region via `Bytes::copy_from_slice`; the fix is Phase A
  work), prost Bytes fields and `arrow_ipc::reader::StreamDecoder` IPC
  decode in ravel-otap (zero-copy only when the external producer's IPC
  buffer alignment permits, measured by a copy-fallback counter), and
  SoA sample hand-off with lazy merge in ravel-segment/ravel-query,
  including a public SoA fetch surface the SQL scan consumes.
  Typed zero-copy views over raw-f64 pages are recorded as physically
  impossible in v1 (unaligned payloads, unaligned GET buffers); pursuing
  them is an RSEG v2 alignment decision through the format-change
  procedure, opened only after a measurement of raw-f64 page share.

Detail, phasing (A: in-crate zero-copy with measurements, B: TableProvider
and SQL endpoint, C: Flight SQL, D: RavelQL lowering), tickets, and risks:
docs/arrow-datafusion-plan.md.

## Consequences

- SQL lands on a maintained optimizer and executor instead of a homegrown
  one, and Flight SQL becomes implementable from a standard trait.
- PromQL results remain byte-identical to today; the evaluator and its
  differential-vs-Prometheus gate are untouched.
- The workspace gains its heaviest dependency, contained behind a crate
  boundary and off-by-default features; arrow and datafusion versions are
  pinned in lockstep and upgraded deliberately, gated on the differential
  suite.
- Two query surfaces share one visibility semantics: one normative dedup
  total order (docs/catalog-and-mvcc.md) with two independent
  implementations (the PromQL merge and the SQL dedup operator) held
  equal by a scan-level differential gate, plus cross-surface tests at
  the same commit token.
- ADR-0006 is completed rather than superseded: the Phase 3 evaluation it
  promised has happened, and its core holding (custom evaluator for
  PromQL) stands.
