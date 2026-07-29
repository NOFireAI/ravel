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
- `est_bytes`: running estimate (samples * 16 + label bytes on first sight)
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
- flush tick (interval default 200 ms): flush if `oldest_ns` older than
  `max_flush_delay` (default 500 ms) and buffer non-empty.
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
