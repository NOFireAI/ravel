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
  8 MiB).
- flush tick (interval default 200 ms): flush if `oldest_ns` older than
  `max_flush_delay` (default 500 ms) and buffer non-empty.

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

Per shard: buffered bytes/points, flush count by trigger (size|age), flush
build/put/commit latency histograms, retries, failures, ack latency, queue
depth. Per tenant: accepted/rejected points, bytes.
