# ADR-0011: OTAP (OpenTelemetry Arrow) ingest, not generic Arrow Flight

Status: Accepted (2026-07-27)

## Context

Arrow-based ingest promises lower CPU per point than OTLP protobuf,
especially for high-cardinality repeated attributes, because attributes
arrive dictionary-encoded and the canonical series-identity hash can run
per distinct label set instead of per point. Two candidate surfaces:
the OpenTelemetry Arrow Protocol (OTAP) from open-telemetry/otel-arrow,
or a generic Arrow Flight DoPut endpoint.

## Alternatives

1. Arrow Flight DoPut with our own schema conventions: clean transport,
   but no client ecosystem; every producer would need custom code.
2. OTAP: the OTel Collector ships an `otelarrow` exporter; per-batch acks
   map exactly onto Ravel's strict commit acknowledgement; schema and
   dictionary deltas are connection-scoped soft state, which fits the
   stateless-compute invariant.
3. Both now: double surface area before either is measured.

## Decision

Option 2, feature-gated (`otap` cargo feature, `--otap` flag), in a new
`ravel-otap` crate. Vendored OTAP protos at a pinned release compiled with
protox (no git dependencies; no published otel-arrow Rust crate exists as
of this date), `arrow-ipc` 59.x for payload decode, zstd already in the
workspace. Commit tokens are returned in `BatchStatus` details so
read-your-write holds over OTAP. A columnar normalizer joins OTAP's
relational attribute tables back to points and computes one SeriesId per
distinct label set per batch. The OTLP-vs-OTAP differential gate (identical
SeriesIds, samples, and rejections for the same logical data) is the
correctness bar; the bench panel (CPU/point, allocs/point at high attribute
cardinality) is the performance bar before default-on.

Arrow Flight (DoPut ingest, Flight SQL queries) is deferred to the Phase 3
DataFusion decision and gets its own ADR if pursued.

## Consequences

- First arrow-rs dependency enters the workspace, isolated to ravel-otap
  behind a feature flag until measurements justify more.
- Per-stream schema/dictionary state is bounded and connection-scoped;
  losing it costs a stream reset, never durability.
- Full plan, safety caps, and phasing: docs/otap-ingest.md.
