# Commit Protocol, Catalog, and MVCC

Companion to ADR-0002, ADR-0003, and ADR-0010. This is the implementer
contract for `ravel-commit` and `ravel-catalog`.

## Key layout (all under one bucket root)

```
t/<tenant_hash>/m/l0/<shard>/<writer_id>.<epoch>.<seq>.<hash16>.rseg      data
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/<writer_id>.<epoch>.<seq>.cmt   commit
t/<tenant_hash>/m/l1/<shard>/<ingest_hour>/<input_set_hash16>.<part:04>.<hash16>.rseg   L1 part
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/l1.<input_set_hash16>.cmt       compaction record
t/<tenant_hash>/m/c/<shard>/<ingest_hour>/retire.tmb                      retention tombstone
t/<tenant_hash>/m/maint/<shard>/cursor                                    advisory scan cursor
t/<tenant_hash>/catalog/snap/<snapshot_id>.snap                           (later)
t/<tenant_hash>/catalog/HEAD                                              (later)
```

The four compaction/retention key shapes (ADR-0018, ADR-0019;
docs/compaction-retention-plan.md §3.1) are additive: existing keys and
their meaning are untouched.

- `input_set_hash16`: first 16 hex chars of the blake3 digest over the
  compaction record's sorted `inputs` list (canonical encoding, sorted by
  `(writer_id, writer_epoch, writer_seq)`). `hash16` on an L1 part is the
  part object's own blake3, same convention as an L0 data key. `part` is
  zero-padded 4 digits.
- Compaction records and the retention tombstone live in the same
  `c/<shard>/<ingest_hour>/` prefix as L0 commit records, so the existing
  one-LIST-per-bucket resolution path discovers all three shapes without
  a second LIST. Filenames are disjoint by construction:
  `<writer_id>.<epoch>.<seq>.cmt` (L0 commit), `l1.<input_set_hash16>.cmt`
  (compaction record), `retire.tmb` (tombstone, fixed name). A key in
  this prefix matching none of the three shapes is a fail-loud error
  (surfaced to metrics), never silently skipped: layout drift must be
  visible, not swallowed.
- The maint cursor (`m/maint/<shard>/cursor`) is advisory mutable state,
  updated by CAS, the same exemption from the immutability rule that the
  ADR-0003 HEAD pointer has. Losing or corrupting it costs a rescan, never
  correctness; it carries no durability role and is not a manifest.

- `tenant_hash`: hex, 32 chars (ADR-0009). Unkeyed BLAKE3 by default; a
  deployment-keyed variant is available via config (ADR-0010 §13).
- `m` = metrics signal. Logs `l`, spans `s`, profiles `p` reserved.
- `shard`: zero-padded 4-digit decimal. `shard_count` is immutable per
  (tenant, signal) in v1 (ADR-0010 §9); Phase 1 reads it from config and
  treats changing it as a data-loss operation (forbidden).
- `ingest_hour`: `YYYYMMDDTHH` UTC formatted from the pinned
  `ingest_hour_bucket` (unix hours) of the flush. Never recomputed on retry.
- `writer_id`: UUIDv4 assigned per process start. MUST be freshly random;
  MUST NOT be derived from hostname, pod name, shard index, or any config
  (ADR-0010 §3). `epoch`: u64, informational (unix seconds at startup).
  `seq`: u64 monotonic per (writer_id, epoch, shard), zero-padded 20 digits
  so lexicographic = numeric order. Gaps are permitted (abandoned flushes)
  and carry no meaning: never infer completeness from seq continuity.
- `hash16`: first 16 hex chars of the object's blake3.

## Pinned flush identity

At flush open the writer fixes, immutably, for the lifetime of the flush:

1. `seq` (allocated once, never reused for a different flush),
2. `ingest_hour_bucket` = ingest wall clock at flush open, unix hours,
3. the serialized segment bytes,
4. the blake3 content hash of those bytes.

Every retry of any step below reuses these verbatim. A retry MUST NOT
re-serialize, MUST NOT accrete newly arrived samples, and MUST NOT re-read
the clock. New samples always go to the next flush. A flush that cannot
complete within `max_flush_lifetime` (default 1 h) is abandoned: the writer
MUST NOT publish its commit record afterward (GC interlock, ADR-0010 §11);
its buffered points are reported as failed to any strict-mode waiters.

## Commit sequence (strict mode)

1. Pin the flush identity (above).
2. PUT data object with `PutMode::CreateIfAbsent` and an upload checksum.
   `AlreadyExists` is success: the key embeds the content hash, so the
   stored bytes are identical by construction.
3. PUT commit record with `PutMode::CreateIfAbsent`.
   - `AlreadyExists`: GET the record. Same `content_hash`: success (a
     previous attempt landed; ack path continues). Different: fatal
     split-brain; crash loudly. With identity pinned, this cannot fire on a
     benign retry.
4. Ack all requests in the flush with the commit token.

Crash between 2 and 3 leaves an orphan data object: invisible, GC-eligible
only after `grace + max_flush_lifetime` with a re-verify before delete
(ADR-0010 §11).

Commit record: `ravel.commit.v1.CommitRecord` protobuf: tenant_hash, signal,
shard, writer_id, epoch, seq, ingest_hour_bucket, object_key, object_size,
content_hash (32B), sample_count, series_count, min/max event ts, min/max
ingest ts, format_version, created_unix_ns.

`object_key` is informational. Readers MUST reconstruct the data key from
(tenant_hash, signal, shard, writer_id, epoch, seq, content_hash) and treat
any mismatch with the stored `object_key` as a fatal invariant breach
(ADR-0010 §7). After the suffix GET, the segment reader MUST verify footer
tenant_hash, shard, writer_id, epoch, seq against the commit record.

## Commit tokens

Token (opaque to clients): base64url of
`v2:<shard>:<writer_id>:<epoch>:<seq>:<ingest_hour_bucket>`.

A token fully determines its commit-record key. Ingest acks return one
token per shard the request's points flushed through; the HTTP/gRPC surface
carries them as a comma-separated list in `x-ravel-commit-token`.

## Snapshot resolution (Phase 1)

`Catalog::resolve(tenant, signal, range, min_tokens, now_ns) -> Snapshot`

1. For each shard 0..shard_count, for each ingest_hour bucket overlapping
   `[range.start_ns - max_ingest_lag, now_ns + clock_skew_allowance]`
   (max_ingest_lag default 2 h, clock_skew_allowance default 5 m, config):
   LIST `t/<th>/m/c/<shard>/<hour>/` (paginated; callers dedup keys).
2. Partition the listed keys by shape (L0 commit record, compaction
   record, tombstone; ADR-0018, ADR-0019, docs/compaction-retention-plan.md
   §3.5). A key matching none of the three shapes is a fail-loud error, not
   a skip. Decode all records. Cache decoded records keyed by FULL object
   key; validate tenant_hash/signal/shard fields against the expected
   values on every hit; bound the cache per tenant. Records are immutable
   and never invalidated, except: observing a tombstone for a bucket
   invalidates that bucket's cached commit and compaction records (the
   trigger ADR-0010 §10 promises).
3. Tombstone present: the bucket contributes nothing to the snapshot.
   Otherwise, for each compaction record present: include its parts as
   segment refs, filtered by per-part event bounds against the query
   range; exclude exactly the L0 records named in its input list; include
   any L0 record not named in an input list normally, and raise an
   interlock-violation metric if its created_unix_ns postdates the
   compaction record (it should have been sealed before compaction ran).
   Two compaction records in one bucket with different input_set_hash:
   include both parts sets and all L0s not covered by either (correct
   under overlap harmlessness; ADR-0018), and alarm loudly (§3.6 row 11).
4. Filter the remaining L0 commit records: [min_event_ts, max_event_ts]
   overlaps the query range.
5. For each `min_token`: reconstruct its commit key and GET it directly
   (never by re-listing). Present: ensure it is in the snapshot set (its
   event range might not overlap; include it anyway so read-your-write
   holds). Absent: fall back to GETting the bucket's compaction record(s)
   (cacheable, same as step 2) and check the token's writer identity
   against each record's input list. Found in an input list: satisfied via
   that record's parts. A tombstone present for the bucket: satisfied with
   zero segments (the data was retired, not lost). Neither found after one
   retry: error `unsatisfiable token`, surfaced as 5xx.
6. Snapshot = the resulting segment set, pinned for the query lifetime;
   later commits, compactions, or deletions do not affect a running query.
   A store NotFound on a pinned segment surfaces as SnapshotInvalidated;
   the frontend re-resolves and retries the query once (ADR-0010 §11).

`SegmentRef` carries a level discriminator. L0 refs keep the existing
commit-record provenance fields. L1 part refs carry (ingest_hour,
input_set_hash, part_index, content_hash, object_size, event bounds) and
reconstruct the part key from those fields rather than trusting any
stored string (ADR-0010 §7, same discipline as the L0 data key). Snapshot
ordering stays a deterministic total order across mixed levels: L1 parts
sort into the same (provenance, shard, writer_id) tiebreak chain as L0
segments (see "Cross-segment duplicate samples" below) using the
compaction record's created_unix_ns in place of a commit record's, and
input_set_hash plus part_index as the final tiebreaks in place of
writer_id/epoch/seq, since a part has no writer identity of its own.

The listing window is sound because admission bounds event-time skew
(ADR-0010 §8): points with `event_ts > ingest_ts + max_future_skew`
(default 10 m) or `event_ts < ingest_ts - max_ingest_lag` are rejected at
ingest. Late arrivals within bounds land in the current ingest hour and
stay discoverable via the `now`-anchored upper bound.

## MVCC rules

- Snapshots are logical sets of immutable segments. Compaction (Phase 2)
  publishes a transaction adding outputs and removing inputs; running
  queries keep their pinned set.
- GC deletes an object only when all hold: unreachable from any snapshot
  within the protection horizon (>= max_query_duration + grace), not
  lease-protected, and older than grace + max_flush_lifetime; commit-record
  absence (for orphans) is re-verified immediately before each delete.
- Deletion (retention/tombstone) is a durable transaction that excludes
  segments from new snapshots first; physical removal follows via GC.

## Cross-segment duplicate samples

Queries dedup by (series_id, ts) under the provenance order
(commit created_unix_ns, writer_epoch, writer_seq, in-page index); the
greatest wins. Values compare by f64 bit pattern (ADR-0010 §5).

That provenance order is not total across segments: two same-shard segments
from different writers can tie on (created_unix_ns, writer_epoch, writer_seq)
because seq is monotonic only per (writer_id, epoch, shard) (ADR-0010 §3). To
make the resolved snapshot's segment order a deterministic total order, the
catalog sort appends shard then writer_id as final tiebreaks after the
provenance components. writer_id is a per-segment identity component, so no
two distinct segments can tie on the full key; the order never depends on
arrival, insertion, or map iteration order.
