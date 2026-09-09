# ADR-0067: Pipelined ingest flushes with adaptive flush delay

Status: Accepted

## Context

A shard actor today executes its flush inline: while the segment encode and
the two sequential PUTs (data, then commit record) are in flight, the actor
drains no messages (crates/ravel-ingest/src/shard.rs). Throughput per
(shard, tenant) is therefore bounded by `target_bytes / flush_round_trip`,
and every waiter queues behind the in-flight flush. This is the documented
Phase-1 simplification, and it is measured: the same ingest configuration
that accepts ~303k points/s against MemoryStore accepts ~1,285 points/s
against loopback MinIO (~236x collapse), with visibility lag at 57 s in the
saturated regime (measured on the S3/MinIO panel). Real S3 round
trips are 10-30x slower than loopback, so the production gap is larger.

Independently, the age trigger is a fixed 500 ms regardless of arrival
rate. A trickle tenant saturates it and pays ~16 PUT/s fleet-wide (about
$200/month in PUT requests alone at AWS pricing), and the resulting tiny
objects push write amplification above 1 (measured 1.74 on the MinIO
panel). ADR-0001 anticipated "adaptive batch control with PUT latency
feedback"; none was implemented.

The encode path also performs avoidable work per flush: page bytes are
materialized ~5 times, and `retrailer_v4_to_v6` copies the entire finished
object and computes blake3 a second time on every sub-4096-series flush
(crates/ravel-segment/src/writer.rs:684-722) — writer-internal plumbing,
not a format requirement. All of it runs on the actor's task.

What already makes pipelining safe: flush identity (writer_id, epoch, seq,
ingest hour, content hash) is pinned at flush open and retries reuse it
verbatim; commit records are keyed by that identity; the catalog tolerates
seq gaps per (writer, shard); acks are attached to their own flush's
waiters; and the crash matrix (docs/consistency-model.md) is defined per
flush, not per actor.

## Decision

1. **Split buffer ownership from flush execution.** The shard actor remains
   the single-threaded owner of buffered state. At flush trigger it pins the
   flush identity, moves the `TenantBuf` (with its waiters) into a flush
   stage, and continues draining its channel. The flush stage encodes the
   segment and runs the data PUT then commit PUT for that flush, then acks
   that flush's waiters. Ownership transfers by move; no shared mutable
   state is introduced.
2. **Bound in-flight flushes per shard** with a semaphore,
   `max_inflight_flushes` (default 1 at landing = today's behavior; flipped
   to 3 only after the ingest pipeline-depth panel validates throughput,
   memory, and ack-tail effects). When the bound is reached the actor's
   flush trigger blocks, and backpressure propagates through the bounded
   channel exactly as today.
3. **Adaptive flush delay per (shard, tenant).** The age trigger adapts to
   observed arrival rate within an explicit corridor:
   floor = current 500 ms behavior (bursty tenants keep today's latency);
   ceiling = strict-mode visibility p99 budget (1 s) minus 2x observed PUT
   RTT p99 minus retry headroom. The bound is derived from the contract in
   docs/consistency-model.md, never a free constant.
4. **Writer copy elimination rides this epic** (no format change): emit the
   v6 trailer directly in the core writer instead of retrailing, frame
   pages into section buffers through reused scratch (`encode_*_into` APIs
   already exist), and stop cloning histogram values per sample. Output is
   gated by bit-for-bit parity tests against the current writer.

```mermaid
flowchart LR
    subgraph shard actor (single owner)
        A[drain mpsc 256] --> B[TenantBuf merge]
        B -->|size 8 MiB or adaptive age| C[pin identity,\nmove TenantBuf]
    end
    C --> S{{semaphore\nmax_inflight_flushes}}
    S --> F1[flush task 1\nencode -> data PUT -> commit PUT -> ack]
    S --> F2[flush task 2]
    S --> F3[flush task 3]
    F1 --> OS[(object store)]
    F2 --> OS
    F3 --> OS
    A -. keeps draining while flushes fly .-> B
```

## Rejected alternatives

- **Cross-tenant PUT coalescing** (one object carrying many tenants):
  breaks the per-tenant key layout (frozen contract), per-tenant KMS write
  routing, and per-tenant retention/GC lifecycles, for a request saving
  that the adaptive delay achieves without touching any contract.
- **Delayed or relaxed commit acks**: violates the strict-mode contract;
  buffered mode already exists as the explicit opt-out.
- **WAL or local-disk staging to absorb PUT latency**: forfeits the
  disposability and recovery model (object storage as sole durable truth);
  rejected on the same grounds as the local-staging model in the
  performance investigation.
- **Unbounded pipelining**: memory grows with in-flight count and the
  ordering pressure on downstream consumers becomes implicit; a bounded
  semaphore keeps both explicit.
- **Parallelizing the two PUTs within one flush**: the commit record must
  not exist before the data object is durable (crash-matrix row 2 depends
  on the order); within-flush ordering is contract, not waste.

## Consequences

- Commit seqs within a shard may land out of order across in-flight
  flushes; the catalog already tolerates seq gaps, and read-your-write
  tokens are per-flush, unaffected.
- Memory per (shard, tenant) rises by up to (max_inflight_flushes - 1)
  flush windows; bounded and reported via a new in-flight gauge. Interacts
  with ADR-0069's global byte budget (charged until flush completion).
- The crash matrix is re-verified with FaultStore hold/release gates at
  depths 1 and 3 (interleaved flush failures, abandoned-flush interlock,
  duplicate-delivery on retry).
- The ingest panel gains a pipeline-depth x delay-policy sweep; the flip
  from default 1 to 3 is a measured decision.
- Encode leaves the actor task, so a slow encode no longer blocks channel
  drain even at depth 1.
