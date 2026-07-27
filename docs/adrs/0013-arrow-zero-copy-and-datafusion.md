# ADR-0013: Targeted Arrow zero-copy, DataFusion for SQL and relational operators only

Status: Accepted (2026-07-27)

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
  and `ExecutionPlan` over catalog snapshots and RSEG segments, reusing
  the existing fetcher and pruning (time bounds to `Catalog::resolve`,
  matchers to SERIES_TABLE), exposed as `POST /api/v1/sql` and later
  Flight SQL, both behind cargo features. ravel-query stays free of
  arrow and datafusion; ingest-critical crates keep the ADR-0011
  isolation (arrow in ravel-otap only).
- Query budgets bridge to DataFusion through a budget-sized per-query
  memory pool and a tenant-delegating `MemoryPool` implementation;
  budget exhaustion is an error, never a partial result; spilling stays
  disabled.
- Exactness is enforced by a differential gate: every DataFusion-executed
  operator is compared bit-exactly (f64::to_bits) against a reference
  scalar executor on property-tested datasets; aggregations run
  single-partitioned with deterministic input order until an ADR defines
  anything looser.
- Zero-copy work is limited to what RSEG v1 permits: `bytes::Bytes`
  slicing on the fetch path (already true), prost Bytes fields and
  `arrow_ipc::reader::StreamDecoder` zero-copy IPC decode in ravel-otap,
  and SoA sample hand-off with lazy merge in ravel-segment/ravel-query.
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
- Two query surfaces share one visibility semantics (dedup and commit
  tokens implemented once in the scan), tested cross-surface.
- ADR-0006 is completed rather than superseded: the Phase 3 evaluation it
  promised has happened, and its core holding (custom evaluator for
  PromQL) stands.
