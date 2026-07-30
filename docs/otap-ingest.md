# OTAP Ingest: OpenTelemetry Arrow over gRPC

Plan for receiving telemetry as Arrow record batches (feature-gated,
`otap`). ADR-0011 records the decisions; this is the implementer contract
and phasing.

## What we are implementing

The OpenTelemetry Arrow Protocol (OTAP) from open-telemetry/otel-arrow:
bidirectional gRPC streams (`ArrowMetricsService`, `ArrowLogsService`,
`ArrowTracesService`) carrying `BatchArrowRecords` messages. Each message
has a `batch_id`, and one `ArrowPayload` per payload type (metrics table,
resource attrs, scope attrs, data-point attrs, exemplars), each payload an
Arrow IPC stream fragment compressed with zstd. Schemas and dictionaries
arrive incrementally per stream; payloads reference them by `schema_id`.
The receiver replies per batch with `BatchStatus` (ack/nack + retry hint).

This is NOT generic Arrow Flight. Flight's `DoPut` could carry the same
batches, but the OTel collector ecosystem speaks OTAP (`otelarrow`
exporter), so OTAP is the interoperable surface. A Flight/Flight SQL
surface stays a query-side decision for Phase 3 (ADR-0006); if a Flight
ingest path is ever wanted, it reuses everything below except the stream
state machine.

## Why it fits Ravel unusually well

- OTAP acks are per `batch_id` on the stream. That is exactly our strict
  ack: reply `BatchStatus` only after the batch's flushes have data + commit
  objects durable. Commit tokens ride in the status details so
  read-your-write works over OTAP too.
- OTAP's value is dictionary-encoded repeated attributes. Our normalizer's
  hottest cost is canonical series identity; with Arrow dictionaries the
  BLAKE3 canonicalization runs once per distinct (resource, scope, attrs)
  combination per batch instead of once per point.
- Backpressure: `BatchStatus` carries retry hints; shard-actor channel
  fullness maps to it directly.

## Architecture

```
tonic OTAP stream
  -> per-stream state machine (schema store, dictionary deltas, zstd)
     bounded: max schemas, max dict bytes, max decompressed payload
  -> arrow-ipc StreamReader per payload -> RecordBatch set
  -> columnar normalizer (join attrs tables to points by parent_id,
     group by distinct label-set key, one SeriesId per group)
  -> Vec<NormalizedPoint> + Rejections (same admission limits as OTLP)
  -> IngestRouter::write (unchanged)
  -> BatchStatus ack with commit tokens after strict commit
```

New crate `ravel-otap` (protocol decode + normalizer). Gateway wiring in
`ravel-server` behind cargo feature `otap` and flag `--otap` is planned,
not present: as shipped, `services/` has no `ravel-otap` dependency and the
server ingest router exposes no OTAP surface. The decode/normalizer crate
stands alone; wiring it into the server is tracked in issue #12.

Dependency decisions:
- Vendored OTAP `.proto` files (Apache-2.0) compiled with protox, same as
  our own protos. No git dependencies (deny.toml forbids them; no
  published otel-arrow Rust crate exists as of 2026-07-27).
- `arrow` / `arrow-ipc` 59.x for IPC decode only; kept out of
  ingest-critical crates other than ravel-otap. This is also the first
  arrow-rs foothold that Phase 3's DataFusion evaluation will reuse.

Safety (spec §19 applies in full):
- Decompressed-size caps per payload and per stream before allocation
  (zstd bombs), max schemas and max dictionary memory per stream, bounded
  stream count per connection, hard row-count caps cross-checked against
  IPC metadata before materialization.
- Arrow IPC parsing treats all offsets/lengths as untrusted; fuzz targets
  over `BatchArrowRecords` bytes are part of the deliverable.
- A malformed batch nacks that `batch_id`; it never tears down the whole
  stream unless the stream state itself is corrupt.

## Id column transport encodings

otap-spec.md section 6.4 lets `id`/`parent_id` columns ride the wire DELTA-
or QUASI-DELTA-encoded instead of as literal values, declared via an
`encoding` Arrow field-metadata key (absent metadata means DELTA, the
spec's default). The normalizer (`ravel-otap::normalize`) decodes:

- `UNIVARIATE_METRICS.id` (section 5.3.1) and `id`/`parent_id` on
  `NUMBER_DATA_POINTS`, `HISTOGRAM_DATA_POINTS`, and `SUMMARY_DATA_POINTS`
  (sections 5.3.2-5.3.4): DELTA, applied whenever declared or when the
  `encoding` metadata is absent; PLAIN only when explicitly declared. A
  declared encoding that is neither `plain` nor `delta` is a typed
  rejection, not a guess. `quasidelta` is one such rejection on this core
  chain: it is a valid encoding only on the `*Attrs`/`*DpExemplars` columns
  below, never here.
- `parent_id` on the `*Attrs` tables (`RESOURCE_ATTRS`, `SCOPE_ATTRS`,
  `NUMBER_DP_ATTRS`, `HISTOGRAM_DP_ATTRS`, `SUMMARY_DP_ATTRS`) and on
  `HISTOGRAM_DP_EXEMPLARS`: QUASI-DELTA (section 6.4.3), applied whenever
  declared or when the `encoding` metadata is absent, since QUASI-DELTA is
  the spec default for these columns; PLAIN or DELTA only when explicitly
  declared. The equality columns that gate each run's delta-vs-absolute
  choice are the spec's: `type`, `key`, and the type's Active Field value
  for the `*Attrs` tables (Map/Slice types and null values are never delta,
  per section 6.4.3), and `int_value`/`double_value` for
  `*DpExemplars`. An unrecognized declaration is the same typed rejection
  as on the core chain, not a guess.

Not decoded:

- `EXP_HISTOGRAM_DATA_POINTS` is unaffected by any of the above: the
  normalizer already rejects that table outright as unsupported.

## Phasing

1. Spike (fleet task): vendor protos at a pinned otel-arrow release; build
   stream state machine skeleton; decode golden OTAP captures from the
   otel-arrow repo test data; measure decode cost vs the OTLP protobuf
   path on identical content. Exit criterion: measured decode numbers and
   a go/no-go on arrow-ipc fit.
2. `ravel-otap` crate: full decode + columnar normalizer + admission
   limits + property tests. Differential gate: the same logical batch
   ingested via OTLP and via OTAP must produce identical SeriesIds,
   samples, and rejection classes.
3. Gateway wiring behind the feature flag; strict-ack integration with
   commit tokens in BatchStatus; e2e test driving a pinned OTel Collector
   `otelarrowexporter` (docker) against ravel-server.
4. Bench panel: OTLP vs OTAP ingest on the same workloads (high attribute
   cardinality is OTAP's sweet spot); CPU/point, allocations/point, and
   ack latency recorded in BENCHMARKS.md. Target from the mission: higher
   ingest throughput per core than the OTLP path at high-cardinality
   attributes; report honestly if not met.
5. Later, separate ADR: Arrow Flight DoPut ingest and Flight SQL query
   surface, decided together with Phase 3 DataFusion work.

## Risks

- otel-arrow protocol still evolves; Rust reference implementation is not
  published to crates.io. Mitigation: pin a release tag for protos and
  golden captures; differential tests against the Go collector's exporter
  catch drift.
- Arrow decode pulls a large dependency tree into one ingest crate.
  Mitigation: isolate in ravel-otap behind the feature flag; audit via
  cargo-deny; the gateway builds without it by default until the bench
  panel justifies default-on.
