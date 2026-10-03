# ADR-0003: Listing-based discovery first, immutable catalog snapshots second

Status: Accepted

## Context

Queries need the set of committed segments for (tenant, signal, shard, time
range): an MVCC snapshot. Commit records are already partitioned by shard and
ingest hour. A catalog service must be an optimization, never a durability
dependency.

## Alternatives

1. Catalog service as source of truth: forbidden (stateful dependency).
2. Always list commit prefixes: correct and simple; O(commits) listing cost
   grows unboundedly without compaction of the metadata view.
3. Immutable catalog snapshot objects + sharded HEAD pointer updated by CAS,
   with listing only for commits newer than the snapshot watermark.

## Decision

Phase 1 ships option 2 behind a `Catalog` API (`resolve_snapshot(tenant,
signal, shards, time_range, min_commit_token) -> Snapshot`), so callers never
see the discovery mechanism. Option 3 is layered in with compaction work: a
catalog worker periodically folds commit records into an immutable snapshot
object and CAS-updates `t/<tenant_hash>/catalog/HEAD/<shard-group>`; resolvers
then read HEAD → snapshot → list only the bounded suffix of newer commits.

A `Snapshot` is a stable logical segment set held for the whole query
(MVCC). `min_commit_token` forces refresh until the referenced commit is
included, giving read-your-write after an ack.

## Consequences

- Phase 1 queries pay one LIST per (shard, ingest-hour bucket) in range;
  acceptable for the vertical slice, and a known bottleneck gate before
  Phase 2 load tests.
- Catalog nodes cache decoded snapshots in memory only; losing them costs
  latency, never correctness.
- HEAD CAS requires etag/generation preconditions, a mandatory capability.
