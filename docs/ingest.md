# Ingest Pipeline Design (Phase 1)

Implementer contract for `ravel-ingest` and the ingest side of
`ravel-server`. See ADR-0001/0002 and docs/consistency-model.md for the
semantics this design must deliver.

## Structure

```
gateway handler (axum / tonic)
  -> auth + tenant resolve + admission limits (ravel-otlp::limits)
  -> normalize (ravel-otlp) -> Vec<NormalizedPoint> + rejects
  -> IngestRouter::write(tenant, points, mode) -> WriteReceipt
```

Remote Write (`POST /api/v1/write`, ADR-0015) is a second gateway handler
in front of the same router, decode/normalize swapped for the RW-specific
crate:

```
POST /api/v1/write (axum)
  -> auth + tenant resolve (shared TenantResolver)
  -> negotiate RW1 vs RW2 (Content-Type, then X-Prometheus-Remote-Write-Version; else 415)
  -> snappy-decompress (capped) + protobuf decode (ravel-remote-write) -> ResolvedRequest
  -> normalize_resolved (ravel-remote-write) -> Vec<NormalizedPoint> + rejects
  -> IngestRouter::write(tenant, points, mode=Strict) -> WriteReceipt
```

This surface always passes `mode=Strict`: it never reads
`x-ravel-ingest-mode`, since a Remote Write sender treats any 2xx as
durable and drops its WAL entry on that basis. A commit token still comes
back in `x-ravel-commit-token`; RW2 responses additionally carry
`X-Prometheus-Remote-Write-Samples-Written` (and the histogram- and
exemplar-written counterparts, currently always 0). Malformed input is
400; shard backpressure and retryable store failures are mapped to a
retryable 5xx with `Retry-After`.

IngestRouter owns `shard_count` shard handles. It groups points by
`shard_for(series_id, shard_count)` and sends one message per shard:

```
ShardMsg::Write { tenant, points: Vec<NormalizedPoint>, ack: Option<oneshot::Sender<Result<CommitToken, WriteError>>> }
```

Channel: `tokio::sync::mpsc` bounded (default 256 messages per shard).
`send` awaiting on a full channel IS the backpressure mechanism; the gateway
holds the request open and its own concurrency limits bound global memory.

## Shard actor

Single task per shard. No locks on the hot path; all state actor-local:

- `buf: HashMap<SeriesId, SeriesBuf { labels: LabelSet, samples: Vec<Sample> }>`
- `exemplars: Vec<IngestExemplar>` in arrival order, one per exemplar the wire
  admitted for a series routed to this shard (ADR-0047)
- `est_bytes`: running estimate (samples * 16 + label bytes on first sight,
  plus ~40 bytes and its attribute strings per buffered exemplar)
- `oldest_ns`: ingest-arrival time of the oldest buffered point
- `waiters: Vec<oneshot::Sender<...>>` for strict-mode acks in this flush window
- writer identity: (writer_id uuid, epoch, next_seq) owned by the process

Loop over `select!`:
- message received: merge points, push `ack` to waiters (strict) or reply
  immediately (buffered), flush if `est_bytes >= target_bytes` (default
  8 MiB). Before merging, each point's series_id is checked against the
  canonical label set that id already claims in the buffer; a mismatch
  (hash collision) rejects the point with a typed error and increments
  the series_id_collisions counter instead of silently merging
  (ADR-0005 fail-loud rule; issue #63).
- flush tick (interval default 200 ms): flush if `oldest_ns` older than an
  age threshold, and buffer non-empty. The threshold is `max_flush_delay`
  (default 500 ms) when the buffer has a strict-mode waiter or already holds
  at least `min_flush_bytes` (default 64 KiB); otherwise the buffer is idle
  and the threshold is `max_flush_delay_idle` (default 10 s) instead
  (ADR-0051 section 7). Strict-mode ack latency is unaffected, since a
  strict write always leaves a waiter in the buffer for its whole flush
  window; only a low-volume buffered-mode tenant's PUT cadence changes.
- channel closed (router dropped): flush the remaining buffer before
  exiting rather than discarding it; points that still fail to flush are
  counted, never silently lost (issue #64).

Shard-actor death is observable: the router marks a shard dead when its
channel closes or an ack receiver fails, routes subsequent points for
that shard to a typed shard-unavailable error, and increments a
shard_deaths counter. Surviving shards keep working (issue #65).

Flush (still inside the actor; ingest-ordering per shard is the point):
1. Build RSEG via `ravel-segment::SegmentWriter` (one segment per tenant in
   the buffer; Phase 1 actors buffer per tenant already, key the buf map by
   (tenant, series_id)).
2. blake3 -> data key -> data PUT (Overwrite) with bounded retries on
   retryable errors.
3. Build CommitRecord, `ravel-commit::publish` (CreateIfAbsent + idempotency
   check) -> CommitToken.
4. Send token to all waiters; clear buffer; seq += 1.
5. On permanent failure: error to all waiters (client retries; nothing was
   acknowledged), drop the buffer (documented at-least-once), count it.

Step 1 also decides which buffered exemplars the object carries (ADR-0047
decisions 1 and 2). An exemplar whose parent sample is not in this flush is
dropped first, before the cap, since the object carries no measurement for it
and the writer treats such a record as an error rather than a silent drop.
What survives is offered to an `ExemplarCap` built for this flush and dropped
with it (a shard-lived cap would hold an unbounded per-series map), newest
-first with a stable sort, because `ExemplarCap::admit` is first-wins and
never retracts. A flush with nothing admitted emits no EXEMPLARS section at
all. Both outcomes are counted (`exemplars_written_total`,
`exemplars_dropped_total`) so the drop stays visible.

The commit record's `ingest_hour_bucket` is derived from the flush-open
clock reading at step 1, before the segment is built. A non-positive or
non-representable reading fails the flush the same way a segment-build
error does (typed `SegmentBuild` error, every waiter acked with it, no
object written) rather than defaulting to bucket 0 (ADR-0051 section 7,
issue #494): a fallback bucket would make the data undiscoverable by hour
with no trace of the failure.

The PUTs run inline in the actor for Phase 1: simple, and per-shard flush
ordering falls out. If PUT latency limits per-shard throughput, Phase 2 can
pipeline flushes (seq allocated at flush start, commits may land out of
order; catalog tolerates seq gaps already).

## Log pipeline

Logs run a parallel pipeline, not a mode of the metrics one
(ADR-0029, docs/superpowers/specs/2026-07-28-log-storage-design.md):

```
POST /v1/logs (axum) | logs.v1.LogsService/Export (tonic)
  -> auth + tenant resolve (the same TenantResolver both metrics surfaces use)
  -> normalize_logs (ravel-otlp::logs_normalize) -> Vec<NormalizedLogRecord> + rejects
  -> LogIngestRouter::write(tenant, records, mode) -> LogWriteReceipt
```

`LogIngestRouter` and `LogShardActor` mirror `IngestRouter`/`ShardActor`
structurally (one bounded mpsc channel and one actor task per shard, the
same `IngestConfig` knobs, the same flush triggers, the same pinned
writer identity, the same commit sequence) and diverge in exactly four
places:

- Objects are RLOG, built with `ravel_logseg::RlogWriter`, not RSEG built
  with `SegmentWriter`. They land under the `l` keyspace
  (`t/<tenant>/l/l0/...`); commit records under `t/<tenant>/l/c/...`.
  `keys::data_key` and the commit protocol are already
  `Signal`-parameterized, so nothing in `ravel-commit` changed for logs.
- Routing is by log stream, `shard_for_log(stream_id, shard_count)`, with
  the identical leading-8-bytes-mod-shard_count math `shard_for` uses on a
  `SeriesId`. Unlike a `SeriesId`, a `LogStreamId` does not itself carry
  the tenant: routing is tenant-scoped because each shard's buffer is keyed
  by tenant, not because the id is.
- There is no series-value-kind concept, so no per-series kind check on
  merge. A log record's unit of identity is its `stream_id` plus the
  canonical `stream_attrs` bytes that id was hashed from.
- The fail-loud identity check does not live in the shard buffer. Where
  `TenantBuf::merge` checks an incoming point's `series_id` against the
  label set that id already claims (ADR-0005), `LogTenantBuf::merge` checks
  nothing: `RlogWriter::finish()` already compares every buffered record's
  `stream_attrs` for a shared `stream_id` and rejects the whole object with
  `LogSegError::InconsistentStreamAttrs` (issue #225). The flush step maps
  that one variant to `LogWriteError::StreamIdCollision` and counts it in
  `stream_id_collisions`; every other `LogSegError` becomes
  `LogWriteError::SegmentBuild`. Duplicating the check in the buffer would
  be dead code with a second chance to drift.

Commit-record fields the log flush fills differently: `sample_count` is the
log record count (a record is the RLOG analogue of a sample), `series_count`
is the number of distinct `stream_id`s in the batch (tracked in the actor,
since `finish()` does not report it back), and `segment_format_version` is
`LOG_SEGMENT_FORMAT_VERSION`, RLOG's own trailer version, not
`SEGMENT_FORMAT_VERSION`.

`LogIngestMetrics` mirrors `IngestMetrics` counter for counter under two
renames that follow the unit change: `buffered_records_total` for
`buffered_points_total`, `stream_id_collisions` for `series_id_collisions`.

Snapshot resolution for logs is not wired: `services/ravel-server/src/fold.rs`
still folds `Signal::Metrics` only, so `catalog/l/HEAD` is never produced
and there is no query path over log objects yet. Ingest durability does not
depend on it (a commit token resolves to its commit record directly), but a
catalog-based read does; that is the log query phase's prerequisite.

## Span pipeline

Spans run a third parallel pipeline on the same terms (ADR-0041):

```
POST /v1/traces (axum) | trace.v1.TraceService/Export (tonic)
  -> auth + tenant resolve (the same TenantResolver every ingest surface uses)
  -> normalize_traces (ravel-otlp::traces_normalize) -> Vec<NormalizedSpan> + rejects
  -> SpanIngestRouter::write(tenant, spans, mode) -> SpanWriteReceipt
```

`SpanIngestRouter` and `SpanShardActor` mirror `LogIngestRouter`/
`LogShardActor` structurally (one bounded mpsc channel and one actor task
per shard, the same `IngestConfig` knobs, the same flush triggers, the same
pinned writer identity, the same commit sequence) and diverge in these
places:

- Objects are RSPAN, built with `ravel_rspan::RspanWriter`. They land under
  the `s` keyspace (`t/<tenant>/s/l0/...`); commit records under
  `t/<tenant>/s/c/...`.
- Routing is by trace, `shard_for_span(trace_id, shard_count)`, with the
  identical leading-8-bytes-mod-shard_count math `shard_for` uses on a
  `SeriesId`. Keying on `trace_id` rather than on a resource-derived
  identity is ADR-0041 decision 2: it keeps one trace's spans confined to
  one shard, so trace-by-id assembly is a bounded scan instead of a
  fan-out across every shard. The tradeoff ADR-0041 accepts is that
  service-scoped span search is cross-shard.
  `shard_for_span` currently lives in `crates/ravel-ingest/src/span_router.rs`
  rather than beside `shard_for`/`shard_for_log` in `ravel-types`, because
  `ravel-types` was outside the scope of the change that added it. The
  placement is provisional; the routing rule itself is frozen, since it
  determines which shard's object keys a trace's spans land under.
- There is no derived identity, so no identity-collision check anywhere in
  the path: `trace_id` and `span_id` come from the sender verbatim, unlike
  a metric's `series_id` or a log's `stream_id`. `SpanWriteError`
  accordingly has no collision variant and `SpanIngestMetrics` no
  collision counter.
- Resource and scope attributes are merged into each span's single `attrs`
  map at normalization time (`ravel_rspan::merge_attrs`, resource beats
  scope beats span), not carried as a separate identity blob. Because they
  feed no identity, a resource attribute that cannot be converted is
  dropped and reported on its own rather than rejecting every span under
  the resource the way the log path must. Span kind, trace state, flags,
  events, and links have no RSPAN column and are stored under reserved
  underscore-prefixed `attrs` keys (`_kind`, `_trace_state`, `_flags`,
  `_events_raw`, `_links_raw`); events and links are opaque hex blobs in
  v1 (ADR-0041 decision 4).

Commit-record fields the span flush fills differently: `sample_count` is the
span count, `series_count` is the number of distinct `trace_id`s in the batch
(tracked in the actor), `segment_format_version` is
`SPAN_SEGMENT_FORMAT_VERSION`, and the event-time bounds are the batch's
interval — the minimum `start_ts_ns` and the maximum `end_ts_ns` — so a
commit record advertises the same interval RSPAN's skip index prunes with.

`SpanIngestMetrics` mirrors `LogIngestMetrics` counter for counter, minus
`stream_id_collisions` and with `buffered_spans_total` in place of
`buffered_records_total`.

Snapshot resolution for spans is not wired: `services/ravel-server/src/fold.rs`
folds metrics and logs only, so `catalog/s/HEAD` is never produced and there is
no query path over span objects yet (ADR-0041 phases 3 and 5). Ingest
durability does not depend on it.

## Admission control (ADR-0051)

`ravel-server` builds one `AdmissionController` (`crates/ravel-ingest/src/
admission.rs`) at startup from `--limits-file` (validated at parse time
regardless of mode; an unparseable file or unknown key fails startup) and
threads the same `Arc` into every ingest path: OTLP HTTP and gRPC
(metrics/logs/traces), OTAP, and Remote Write. `--limits-file` now has a
runtime effect, not just a startup validation pass.

Four layers, each enforced before the allocation it bounds, in this order:

1. **Body size.** `DefaultBodyLimit` 16 MiB on `/v1/metrics`, `/v1/logs`,
   `/v1/traces`, and `/api/v1/write`; `max_decoding_message_size` 16 MiB on
   every tonic *ingest* service, OTAP's `ArrowMetricsServiceServer` included.
   The Flight SQL query service (`services/ravel-server/src/flight.rs`) is
   not an ingest path and is out of scope here; it still runs on tonic's
   4 MiB default. Remote
   Write additionally caps the *compressed* body at 16 MiB ahead of its
   existing 64 MiB decompressed cap (`MAX_DECOMPRESSED_PAYLOAD_BYTES`).
   OTAP's Arrow-stream decompression cap
   (`StreamConfig::default().max_decompressed_payload_bytes == 16 MiB`) does
   *not* bound the whole wire message: it is checked per
   `ArrowPayload.record`, and a `BatchArrowRecords` message carries a vector
   of payloads, so the decompressed bound it provides is 16 MiB times the
   payload count, not a flat 16 MiB. The `max_decoding_message_size` cap on
   the tonic service is therefore the explicit per-message bound for OTAP
   too; tonic's own 4 MiB default is stricter than our 16 MiB cap today, but
   the cap is set explicitly rather than left to that upstream default.
2. **Byte rate.** `check_byte_rate`, on wire body bytes, after tenant
   resolution and before decode. HTTP and gRPC each decode independently
   before calling into the shared normalize/write path, so this check has
   no single shared insertion point: it runs once per transport handler
   (all three OTLP HTTP handlers, all three OTLP gRPC services, OTAP's
   per-batch handler, and Remote Write), eight call sites in total. A gRPC
   handler measures `request.get_ref().encoded_len()` since tonic has
   already decoded the message by the time the handler runs.
3. **Event-time skew.** Out of scope for this change; unchanged.
4. **Series/stream admission**, metrics and logs only (spans excluded).
   `check_series_creation_rate`/`check_stream_creation_rate` first (a
   breach rejects the whole request); then `admit_series`/`admit_streams`,
   which partially admits: rejected series/streams are dropped from the
   write and folded into the signal's existing partial-success reporting
   (`ExportMetricsPartialSuccess`/`ExportLogsPartialSuccess`).

Rejection is per signal and per layer:

| layer | HTTP | gRPC | Remote Write |
|---|---|---|---|
| body size | 413 | RESOURCE_EXHAUSTED | 413 |
| byte rate | 429 + `Retry-After` | RESOURCE_EXHAUSTED | 429 + `Retry-After` |
| series/stream creation rate | 429 + `Retry-After` | RESOURCE_EXHAUSTED | 429 + `Retry-After` |
| active series/stream cap | 200 + partial success | OK + partial success | 200, no partial-success message, reduced `X-Prometheus-Remote-Write-Samples-Written` |

Remote Write's partial-admission semantics are pinned and do not follow
the other two signals' shape: it never emits a partial-success message,
always answers 2xx with the true written count once body-size and rate
checks pass, and reserves 429 for the rate-limit rows only, never for an
active-series-cap breach.

## Modes

`mode=strict` (default): ack after step 3 for every flush the request's
points landed in (a request spanning shards awaits all of them; the receipt
carries max token per shard).
`mode=buffered`: ack at enqueue. Config per tenant; header override
`x-ravel-ingest-mode: buffered` allowed only when tenant config permits.

## Sizing defaults (config, all overridable)

| knob | default |
|---|---|
| shard_count | 4 (dev), scale with cores |
| channel depth | 256 msgs |
| target_bytes | 8 MiB |
| max_flush_delay | 500 ms |
| max_flush_delay_idle | 10 s |
| min_flush_bytes | 64 KiB |
| put retry budget | 4 attempts, 100ms..2s jittered backoff |
| max in-flight strict requests per conn | transport-level (tonic/axum concurrency limits) |

## Metrics (self-observability)

`IngestMetrics` (crates/ravel-ingest/src/metrics.rs) exposes process-global
`u64` counters through `IngestMetricsSnapshot`. They carry no per-shard and no
per-tenant dimension: one `IngestMetrics` is built by the router and shared by
every shard actor via `Arc`, so each value is the sum across all shards and all
tenants of the process.

Counters recorded today:

- `flushes_by_size`, `flushes_by_age`, `flushes_manual`: flush count by trigger.
  `flushes_manual` covers explicit `FlushNow`, the `Shutdown` drain, and the
  channel-close drop-path drain. These are **attempt-time**: incremented when a
  flush is opened, before the segment build or any PUT, so a later-abandoned
  flush is counted here as well as in an `abandoned_*` counter. Successful
  flushes = the three trigger counters minus the two `abandoned_*` counters.
- `abandoned_retry_exhausted`: flush abandoned because a PUT exhausted its retry
  budget or `max_flush_lifetime` elapsed (`WriteError::Abandoned`). Durability
  signal; retryable.
- `abandoned_input_rejected`: flush abandoned because the input could not be
  built into a durable object (`WriteError::SegmentBuild`). Client signal; not
  retryable. Split from `abandoned_retry_exhausted` so a store problem is
  distinguishable from a bad-input problem by counter alone.
- `put_retries`: retried PUT attempts across the data-object and commit-record
  paths (first attempt of each excluded).
- `buffered_bytes_total`, `buffered_points_total`: cumulative volume admitted
  into shard buffers at enqueue time.
- `acks_ok`, `acks_err`: strict-mode waiters acked (**success-time**, at the
  flush's terminal outcome). Zero for buffered-mode and for flushes with no
  strict waiter, so this is an ack-outcome counter, not a flush-outcome one.
- `series_id_collisions`: batches rejected fail-loud on an ADR-0005 series-id
  collision.
- `shard_deaths`: distinct shard actors observed dead by the router, counted
  once per shard.

Tracked future work (not yet implemented; own ticket, see a8-F05): a per-shard
and per-tenant dimensioned model — per-shard buffered bytes/points, flush
build/put/commit latency histograms, ack latency, and queue depth; per-tenant
accepted/rejected points and bytes. It requires a metrics backend that these
flat atomics do not provide and is out of scope for the counters above.
