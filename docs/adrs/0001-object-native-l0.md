# ADR-0001: Object-native L0, no local WAL

Status: Accepted

## Context

The system invariant is that S3-compatible object storage is the only durable
backend and any compute node may vanish at any instant. A classical design
buffers writes in a local WAL and ships them to object storage asynchronously;
that makes local disk a durability dependency and recovery requires reading
another node's disk or accepting loss.

## Alternatives

1. Local WAL + async upload: fast acks, but violates
   the no-local-durability invariant.
2. Distributed log (Kafka/Redpanda) in front: durable, but adds a stateful
   dependency Ravel's design explicitly forbids.
3. Object-native L0: the first durable artifact *is* an immutable, queryable
   object in the store; acknowledgement follows its successful PUT + commit
   record.

## Decision

Option 3. Ingest actors buffer in memory only (bounded, backpressured), then
serialize an L0 RSEG object and upload it. The L0 object is simultaneously the
durable ingest record, an immediately queryable segment, and compaction input.
Strict mode acks only after data PUT + commit PUT succeed. A separately named
"buffered" mode may ack earlier and must never be described as durable.

## Consequences

- Ack latency is bounded below by one S3 PUT round trip (~10–100 ms); adaptive
  batching amortizes it across requests sharing a flush.
- Unflushed in-memory batches are lost on crash. Permitted, because they were
  never acknowledged in strict mode.
- Freshness/throughput/object-size tension is managed by adaptive batch
  control (target size, max flush delay, PUT latency feedback).
- No recovery code path reads local disk; restart-from-empty is the only
  recovery path and is tested as such.
