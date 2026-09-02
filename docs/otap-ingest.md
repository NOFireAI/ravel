# OTAP Ingest: OpenTelemetry Arrow over gRPC

Implementer contract for receiving metrics as Arrow record batches.
ADR-0011 records the decisions.

Enabling OTAP takes two things, not one: `ravel-server` must be built with
the `otap` cargo feature, which links the arrow decode stack, and the
process must be started with `--otap`, which registers the service. Either
alone serves nothing, the flag does not exist in a build without the
feature, and no published image builds it.

OTAP here is metrics only. The vendored protos declare `ArrowLogsService`
and `ArrowTracesService`, but the tree implements neither: `ravel-server`
registers `ArrowMetricsService` and nothing else, and logs and traces reach
Ravel over OTLP.

## What the protocol is

The OpenTelemetry Arrow Protocol (OTAP) from open-telemetry/otel-arrow:
bidirectional gRPC streams carrying `BatchArrowRecords` messages. Each message
has a `batch_id`, and one `ArrowPayload` per payload type (metrics table,
resource attrs, scope attrs, data-point attrs, exemplars), each payload an
Arrow IPC stream fragment compressed with zstd. Schemas and dictionaries
arrive incrementally per stream; payloads reference them by `schema_id`.
The receiver replies per batch with `BatchStatus` (ack/nack + retry hint).

This is NOT generic Arrow Flight. Flight's `DoPut` could carry the same
batches, but the OTel collector ecosystem speaks OTAP (`otelarrow`
exporter), so OTAP is the interoperable surface. Flight SQL is a query
surface and it ships, behind the `flight-sql` cargo feature that no
published image builds (ADR-0006, docs/query-engine.md). No Flight ingest
path exists; if one is ever wanted, it reuses everything below except the
stream state machine.

## Why it fits Ravel

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

`ravel-otap` holds the protocol decode and the normalizer. The gateway
wiring sits in `ravel-server` behind the `otap` cargo feature (default off):
with the feature on and `--otap` given, `ravel-server` links `ravel-otap`
and registers `ArrowMetricsService` on the same gRPC listener as the OTLP
services (`services/ravel-server/src/otap_grpc.rs`), driving the per-stream
`StreamState` and replying `BatchStatus` with commit tokens (see "Strict
ack" below). The default build links neither `ravel-otap` nor its arrow
dependency tree.

Dependency decisions:
- Vendored OTAP `.proto` files (Apache-2.0) compiled with protox, same as
  our own protos. No git dependencies (deny.toml forbids them, and no
  otel-arrow Rust crate is published on crates.io).
- `arrow` / `arrow-ipc` 59.x for IPC decode only; kept out of
  ingest-critical crates other than ravel-otap. It was also the first
  arrow-rs foothold in the tree, which the DataFusion query path now
  shares.

Safety (spec §19 applies in full):
- Decompressed-size caps per payload and per stream before allocation
  (zstd bombs), max schemas and max dictionary memory per stream, bounded
  stream count per connection, hard row-count caps cross-checked against
  IPC metadata before materialization.
- Arrow IPC parsing treats all offsets/lengths as untrusted. A byte-level
  mutation suite over `BatchArrowRecords` bytes
  (`crates/ravel-otap/tests/fuzz_mutation.rs`) runs in the ordinary test
  gate, alongside a decode-panic guard.
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
  `NUMBER_DP_ATTRS`, `HISTOGRAM_DP_ATTRS`, `SUMMARY_DP_ATTRS`,
  `NUMBER_DP_EXEMPLAR_ATTRS`, `HISTOGRAM_DP_EXEMPLAR_ATTRS`) and on
  `NUMBER_DP_EXEMPLARS`/`HISTOGRAM_DP_EXEMPLARS`: QUASI-DELTA (section
  6.4.3), applied whenever
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

## Exemplars

`NUMBER_DP_EXEMPLARS` and `HISTOGRAM_DP_EXEMPLARS` rows are carried, not
just counted (ADR-0047). The two tables have identical columns, so one
decoder and one admission path serve both. Each row's own columns
(`time_unix_nano`, `int_value`/`double_value`, `span_id`, `trace_id`) become
a `ravel_types::Exemplar`, its attributes are joined from the matching
`*_DP_EXEMPLAR_ATTRS` table by the exemplar's `id`, and it attaches to:

- for a histogram, the bucket series whose `le` bound is the smallest at or
  above its value;
- for a gauge or sum, the data point's own series. A number point has no
  buckets, so there is no `le` bound to resolve.

Both are the rule the OTLP path applies to the same input. Admission goes
through the caller's `ravel_types::ExemplarCap` (one exemplar per series per
window, a security control per ADR-0047 decision 2), so
`normalize_decoded_with_exemplars` takes that cap by `&mut` from whoever
owns the long-lived per-shard state; `normalize_decoded` keeps its old
signature and wraps it with a batch-scoped cap. One cap and one output
vector serve every exemplar source in a batch, so a gauge point and a
histogram bucket compete for the same per-series window.

A row too malformed to carry (no value column set, which OTLP itself calls
an invalid exemplar; or no timestamp, which leaves nothing to place it in a
window), a row the cap turns away, and a row whose `parent_id` matches no
data point in its table all increment the pre-existing
`HistogramExemplarsDropped` counter, so that counter now means "dropped by
the cap, malformed, or orphaned" rather than "every exemplar, always".
Despite its name it has always covered every metric type, not only
histograms; it is not renamed, because the name reaches no operator-facing
surface (rejections surface through `Display` and `rejected_count()`, and
its `Display` string names no metric type) while a rename would break every
crate that matches on the variant. A `trace_id` or `span_id` cell that is
null, absent, or in a column of the wrong fixed width reads back all-zero,
the layout's convention for "absent"; it never rejects the exemplar
carrying it.

Exemplars attached to a data point that was itself rejected are dropped
with that point and are not counted separately, which is what `ravel-otlp`
does: counting them would give the same logical input a rejection class the
OTLP path does not produce, and ADR-0011 requires the two to agree.

`EXP_HISTOGRAM_DP_EXEMPLARS` rows are counted as dropped, never carried.
The reason is structural, not a missing decode: `EXP_HISTOGRAM_DATA_POINTS`
is rejected as an unsupported metric type on this path (ADR-0017 is a
separate ticket), so the series an exemplar would attach to is never built.
When that changes, the attachment rule is the data point's own series, the
same as a gauge or sum: Ravel stores a native histogram as one series with
one native-histogram sample per timestamp rather than a set of exploded
`le`-bucket series, so resolving the exemplar's value to a bucket index
through the exponential schema's scale would name a series that does not
exist. This needs no ADR amendment: ADR-0047 decision 1 attaches an exemplar
to a `(series, ts_ns)`, and the explicit-bucket rule is a consequence of the
classic histogram exploding into several series, not a separate rule.

## What holds the contract

- **Differential gate** (`crates/ravel-otap/tests/differential.rs`). The
  same logical batch ingested via OTLP and via OTAP produces identical
  SeriesIds, samples, and rejection classes. A property-test regression seed
  file sits beside it. Rejection-class parity has its own suite
  (`otap_otlp_rejection_class_parity.rs`), because a class the OTAP path
  produces and the OTLP path does not is the failure ADR-0011 forbids.
- **Strict ack.** The gateway replies `BatchStatus` for a `batch_id` only
  after that batch's flushes have data and commit objects durable, with the
  commit tokens in the status details, so read-your-write holds over OTAP.
  `services/ravel-server/tests/otap_grpc.rs` drives that path in process
  using `ravel-otap`'s own encoder as the client.
- **Interop.** `services/ravel-server/tests/otap_collector_e2e.rs` runs a
  pinned OTel Collector Contrib in docker whose `otelarrow` exporter encodes
  with the upstream Go otel-arrow library, streams into an in-process
  `ravel-server` started with `--otap`, and reads the sample back through
  `/api/v1/query`. That substitutes an independent producer for the
  synthetic one, which is the interop bar ADR-0011 sets.
- **Bench panel.** `crates/ravel-otap/benches/ingest_panel.rs` measures OTLP
  against OTAP on the same workloads, where high attribute cardinality is
  OTAP's favourable case.

## Standing constraints

- The otel-arrow protocol still evolves and no Rust reference
  implementation is published to crates.io. The protos and golden captures
  are pinned to a release tag, and the differential and collector tests
  against the Go exporter are what catch drift.
- Arrow decode pulls a large dependency tree into one ingest crate. It is
  isolated in ravel-otap behind the feature flag and audited through
  cargo-deny; the gateway builds without it by default, and moving it to
  default-on needs a bench result that justifies the build cost.
- Arrow Flight `DoPut` ingest would need its own decision record. Flight SQL
  is the query-side surface and already ships behind `flight-sql`.
