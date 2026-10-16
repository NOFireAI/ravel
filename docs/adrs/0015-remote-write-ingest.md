# ADR-0015: Prometheus Remote Write 1.0/2.0 ingest surface

Status: Accepted

## Context

Ravel ingests metrics today only via OTLP (gauge and cumulative sum) and
OTAP. The largest installed base of metric producers speaks Prometheus
Remote Write: RW 1.0 (`prometheus.WriteRequest`: repeated TimeSeries with
inline string labels, snappy-compressed protobuf) and RW 2.0
(`io.prometheus.write.v2.Request`: a per-request string symbol table, all
labels/metadata referenced by uint32 index, plus per-series metadata and
created timestamp). Both versions carry plain `Sample{value,
timestamp_ms}`, optional native `Histogram` messages, optional
`Exemplar`s, and metric metadata.

Two properties make RW a low-friction fit:

- RW's acknowledgement contract (2xx only after a durable write,
  retryable 5xx/429 vs non-retryable 4xx) maps exactly onto Ravel's
  strict-mode ack (docs/consistency-model.md): acknowledge only after
  data object and commit record are durable. Nothing has to be faked.
- Classic histograms and summaries need no storage work at all: RW
  transmits them as ordinary float samples on N series
  (`_bucket{le="..."}`, `_sum`, `_count`, `{quantile="..."}`), which is
  precisely the shape Ravel's existing scalar sample path already
  stores. Only native (exponential) histograms carry structure that the
  current format cannot hold; that decision is ADR-0017, not this one.

The precedent for surface boundaries is ADR-0011: each wire protocol
gets its own ingest crate that decodes and normalizes into the shared
internal shape (`ravel_otlp::NormalizedPoint`, admission-limited), and
everything from `IngestRouter::write` down is protocol-blind.

## Alternatives

1. No RW support; require an OTel Collector in front translating RW to
   OTLP. Pushes an extra hop and a lossy translation onto every
   Prometheus-native user, and the collector's RW receiver applies its
   own semantics before Ravel ever sees the data. Rejected.
2. RW 1.0 only. Simplest decoder, but Prometheus 3.x defaults are moving
   senders to RW 2.0, and 2.0 is where native histograms, per-series
   metadata, and the stats-header contract are first-class. Supporting
   only 1.0 buys a second migration immediately. Rejected.
3. One combined decoder that maps RW1 messages into RW2 shapes (or vice
   versa) before normalization. Tempting because the sample semantics
   are identical, but the structural difference is real: RW2 requires
   resolving every label, help, and unit string through the symbol
   table (with index-validation failure modes RW1 cannot have) before
   any usable string exists, while RW1 has inline strings and a separate
   request-level metadata list. Forcing one message shape through the
   other's decode path smears validation across layers. Rejected in
   favor of two thin decoders that converge on one resolved
   intermediate.
4. Two decoders, one normalizer, one new crate `ravel-remote-write`
   (chosen). The decoders own wire concerns (snappy, protobuf,
   symbol-table resolution, per-version validation); the shared
   normalizer owns semantics (label validation, `__name__` extraction,
   admission limits, skew bounds, series identity) exactly once.

## Decision

Option 4. A new crate `crates/ravel-remote-write`, mirroring the
ravel-otlp/ravel-otap pattern:

- Vendored prompb protos (`prometheus.WriteRequest` and
  `io.prometheus.write.v2.Request`, Apache-2.0) at a pinned Prometheus
  release, compiled with protox like the OTAP protos. No git
  dependencies.
- Two decode modules (`rw1`, `rw2`) producing one resolved intermediate
  (owned label strings, samples, native-histogram messages, exemplar
  and metadata counts). RW2 decode validates every symbol reference
  (in-range, even-length label_refs, name/value pairing) before any
  string is used; a bad reference rejects the request as malformed
  (non-retryable 400), never panics.
- One normalizer producing `ravel_otlp::NormalizedPoint` under the same
  `IngestLimits` and skew bounds as OTLP (the skew bounds are what keep
  the catalog listing window sound; they are not optional per
  protocol). Timestamps convert ms to ns with overflow checks. Values
  pass through as raw f64 bit patterns; Prometheus stale markers are
  ordinary samples to storage.
- No name sanitization. RW payloads are already in the Prometheus data
  model; mutating names or labels would silently alias series relative
  to what the sender believes it wrote. Validation only: `__name__`
  present and non-empty, length limits, duplicate labels rejected,
  empty-value labels dropped per RW convention.
- Gateway wiring in ravel-server: `POST /api/v1/write`, version
  negotiation via `Content-Type`/`X-Prometheus-Remote-Write-Version`,
  snappy decompression with a decompressed-size cap before allocation,
  tenant auth as ADR-0009. Response semantics: 2xx only after strict
  ack (commit token also returned in `x-ravel-commit-token`), 429/5xx
  retryable with `Retry-After` mapped from shard backpressure, 400 for
  malformed or invalid-reference payloads, 415 for unknown content
  types; RW 2.0 responses carry the written-counts stats headers.
- Scope at first ship: samples only, full fidelity. Native `Histogram`
  messages are rejected at admission with a typed, counted rejection
  reported through the RW stats/error surface (never silently dropped)
  until ADR-0017's storage lands. Metadata (type/help/unit) and
  created timestamps are accepted and dropped with counters: Ravel has
  no metric-metadata store yet (unit and type are not identity,
  ADR-0005); the planned metric index picks this up later. Exemplars
  follow ADR-0017 (accept-and-drop, counted).

Buffered-mode acks are not offered on this surface: RW senders
interpret 2xx as durable and drop their WAL entries, so answering
before commit would convert Ravel's documented at-least-once into
silent loss on crash. RW is strict-mode only regardless of tenant
default.

## Consequences

- First consumer of the snappy codec: RW mandates snappy block
  compression, and `snap = "1.1"` is already declared in
  `[workspace.dependencies]` with no consumer today; ravel-remote-write
  references that existing workspace version, so no new external
  dependency enters the workspace.
- Duplicate and retry behavior needs no new machinery: RW retries after
  a lost ack re-ingest the batch, which is the documented at-least-once
  model; identical duplicates are harmless under the cross-segment
  dedup order (docs/catalog-and-mvcc.md).
- Classic histogram and summary series ingested via RW are immediately
  queryable through the existing scalar path, and they define the
  representation that OTLP-side explosion (ADR-0016) must match
  byte-for-byte in series identity.
- The differential-testing discipline from ADR-0011 extends: RW1 and
  RW2 encodings of the same logical data must produce identical
  SeriesIds, samples, and rejection classes, and a pinned real
  Prometheus (docker) drives the e2e gate.
- Until ADR-0017 lands storage, a mixed RW2 request from a
  native-histogram-enabled Prometheus has its float samples stored and
  its histogram samples visibly rejected in the stats headers. Senders
  do not retry those (the response is 2xx), so the gap is a documented,
  observable non-goal of this phase, not a latent surprise.
