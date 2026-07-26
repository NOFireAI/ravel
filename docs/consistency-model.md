# Consistency, Durability, and Failure Semantics

Normative. Tests in tests/failure/ assert every claim here. If code and this
document disagree, one of them is a bug and the fix updates both.

## Acknowledgement semantics

Strict mode (default):
- An OTLP export is acknowledged only after every batch it contributed to has
  (a) its L0 data object durably stored and (b) its commit record created.
- The response carries a commit token (header `x-ravel-commit-token`; the
  max token across flushes the request participated in, per shard).
- After a strict ack, no crash of any Ravel process may lose that data.
  Object-store durability is the floor: data survives anything the object
  store survives.

Buffered mode (opt-in per tenant or per request, named "buffered"):
- Acknowledged after admission and enqueue to a shard actor. A crash between
  ack and flush loses the buffered window (bounded by max flush delay).
- Never described as durable. No commit token is returned.

Rejection: admission failures (limits, auth, quota) reject before buffering
in both modes. Partial success uses the OTLP partial-success message with
rejected point counts and reasons.

## Visibility semantics

- A batch becomes visible to queries when its commit record exists; commit
  record creation is atomic (create-if-absent), so visibility is atomic per
  L0 object.
- Visibility latency = flush delay + data PUT + commit PUT. The p99 target in
  strict mode under target load is < 1 s.
- There is no cross-shard ordering guarantee. A query snapshot may include
  commit N+1 of shard A and not commit M of shard B, regardless of wall-clock
  order. Per (writer, shard), commits are sequenced by `seq`.

## Read-your-write

- A caller holding commit token T sees the referenced commit by passing
  `min_commit_token=T` to query APIs. The catalog re-lists until the commit
  is included (bounded retries; then `unsatisfiable token` error rather than
  silently serving stale data).
- Without a token, queries see some recent consistent snapshot; freshness is
  bounded by listing behavior, not guaranteed.

## Snapshot isolation

- A query resolves one snapshot (a logical set of immutable segments) and
  uses it for its entire execution. Commits, compactions, and deletions that
  land mid-query do not affect it.
- Compaction transactions (Phase 2) atomically swap inputs for outputs in
  new snapshots; both sets remain physically present until GC clears the
  inputs after the protection horizon.

## Duplicates and idempotency

- Delivery model is at-least-once. A client retry after a lost ack re-ingests
  the batch; both copies are stored and queries see duplicate samples exactly
  as Prometheus would if scraped twice (PromQL takes the last value at a
  timestamp; identical duplicates are harmless, differing values at the same
  timestamp are last-write-wins per evaluation order and documented as such).
- Writer-side retries of the same flush are idempotent by construction:
  same commit key, content-hash-verified (ADR-0002).
- A client idempotency key window (S3-backed) is planned; until it exists,
  Ravel does not claim exactly-once ingestion. Tracked in issues.

## Late and skewed data

- Event time is never trusted for discovery. Commit records are bucketed by
  ingest hour; event-time bounds ride along for pruning. Late data is always
  discoverable; queries bound their listing by `max_ingest_lag` plus catalog
  snapshots (Phase 2) for anything older.

## Crash matrix (strict mode)

| Crash point | Data object | Commit record | Ack | Outcome |
|---|---|---|---|---|
| Before data PUT | absent | absent | no | client retries; nothing stored |
| After data PUT, before commit PUT | present (orphan) | absent | no | invisible; GC after grace; client retries |
| After commit PUT, before ack | present | present | no | visible; client retry stores a duplicate (see above) |
| After ack | present | present | yes | durable and visible |

## Deletion and GC

- Deletion is a durable tombstone transaction, then logical exclusion from
  new snapshots, then physical removal via GC.
- GC deletes an object only when all hold: unreachable from any snapshot
  within the protection horizon, not lease-protected, grace period expired.
- Orphan GC (data objects with no commit record) uses last_modified age >
  grace period (default 24 h) and verifies commit-record absence at delete
  time, in that order (list commits, then delete), relying on strongly
  consistent listing.
