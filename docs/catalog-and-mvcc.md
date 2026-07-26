# Commit Protocol, Catalog, and MVCC

Companion to ADR-0002 and ADR-0003. This is the implementer contract for
`ravel-commit` and `ravel-catalog`.

## Key layout (all under one bucket root)

```
t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash8>.rseg    data
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt  commit
t/<tenant_hash>/catalog/snap/<snapshot_id>.snap                        (later)
t/<tenant_hash>/catalog/HEAD                                           (later)
```

- `tenant_hash`: hex, 32 chars (ADR-0009).
- `m` = metrics signal. Logs `l`, spans `s`, profiles `p` reserved.
- `shard`: zero-padded 4-digit decimal.
- `ingest_hour`: `YYYYMMDDTHH` UTC derived from ingest time of the flush.
- `writer_id`: UUIDv4 assigned per process start. `epoch`: u64, one per
  process start (Phase 1: unix seconds at startup). `seq`: u64 monotonic per
  (writer, epoch), zero-padded 20 digits so lexicographic = numeric order.
- `hash8`: first 8 hex chars of the object's blake3, so retried uploads of
  different content never collide on a key.

## Commit sequence (strict mode)

1. Serialize RSEG object; compute blake3 (32 bytes).
2. PUT data object, `PutMode::Overwrite` (key is unique by construction).
3. PUT commit record, `PutMode::CreateIfAbsent`.
   - `AlreadyExists`: GET the record. Same `content_hash`: success (retry
     landed twice). Different: fatal invariant breach; crash loudly.
4. Ack all requests in the flush with the commit token.

Crash between 2 and 3 leaves an orphan data object: invisible, GC-eligible
after grace period (default 24 h, keyed on last_modified).

Commit record: `ravel.commit.v1.CommitRecord` protobuf (see
`proto/ravel/commit.proto`): tenant_hash, signal, shard, writer_id, epoch,
seq, object_key, object_size, content_hash (32B), sample_count, series_count,
min/max event ts, min/max ingest ts, format_version, created_unix_ns.

Commit token (returned to clients, opaque): base64url of
`v1:<shard>:<writer_id>:<epoch>:<seq>`.

## Snapshot resolution (Phase 1)

`Catalog::resolve(tenant, signal, time_range, min_token) -> Snapshot`

1. For each shard, for each ingest_hour bucket overlapping
   `[range.start - max_ingest_lag, now]` (max_ingest_lag default 2 h, config):
   LIST `t/<th>/m/c/<shard>/<hour>/`.
2. Decode commit records (cache by key; records are immutable so the cache
   never invalidates).
3. Filter: commit's [min_event_ts, max_event_ts] overlaps the query range.
4. Snapshot = the resulting segment set. It is pinned for the query lifetime;
   later commits or deletions do not affect a running query.
5. If `min_token` names a commit not in the set, re-list once (list is
   strongly consistent, so a successful ack implies visibility; the retry
   guards racing caches only). Still missing: error `unsatisfiable token`,
   surfaced 5xx.

Event-time pruning note: commit discovery is organized by ingest hour, and
event-time overlap filtering happens on the record. Late data lands in the
current ingest hour regardless of its event timestamps, so it is always
discoverable. `max_ingest_lag` bounds how far back a *reader* must list; data
older than the lag remains discoverable through catalog snapshots (Phase 2)
which index by event time.

## MVCC rules

- Snapshots are logical sets of immutable segments. Compaction (Phase 2)
  publishes a transaction adding outputs and removing inputs; running queries
  keep their pinned set.
- GC deletes an object only when: not referenced by any snapshot younger than
  the protection horizon, no active lease, grace period expired.
- Deletion (retention/tombstone) is a durable transaction that excludes
  segments from new snapshots first; physical removal follows via GC.
