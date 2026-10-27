# ADR-0046: A content-addressed read cache at the read funnels, not a store decorator

Status: Accepted

## Context

S3 is Ravel's only durable backend, so every byte a query reads costs a
network round trip. `docs/query-engine.md` states the current position
plainly: "Phase 1 caches only decoded commit records and parsed footers,
both in-memory LRU. Anything else waits for measurements."

The measurements exist now. The metric-index phase 4 benchmark found
that for a folded, selective query, per-segment fetch cost is 97.6% of
requests. Those fetches are of immutable, content-addressed objects.

A survey of the codebase found five things that shape this decision.

**There are five caches and none of them counts anything.** All live in
`crates/ravel-catalog/src/cache.rs`: `RecordCache` (:46),
`CompactionRecordCache` (:119), `HeadCache` (:164), `PartCache` (:228),
and `PostingsCache` (:282). Every `get()` returns a bare `Option`, and
the file contains no counter of any kind. `HeadCache` has a TTL but no
capacity bound: it holds one entry per (tenant, signal) and is unbounded
in tenant count.

**There is exactly one place to insert a store decorator, and it is the
wrong place.** `build_store` (services/ravel-server/src/store.rs:143)
wraps the backend in `InstrumentedStore` and hands one
`Arc<dyn ObjectStoreBackend>` to ten consumers
(services/ravel-server/src/lib.rs:259-406). Three of them are ingest
routers that write, one is the compactor, one is the folder, and two of
those CAS-write mutable pointers. A cache inserted there would sit
underneath every writer in the system.

**The trait keys on strings, and some of those strings are mutable.**
`ObjectStoreBackend::get(&self, key: &str, range: GetRange)` cannot tell
an immutable content-addressed data object from the catalog HEAD pointer
(CAS-written at crates/ravel-catalog/src/fold.rs:761) or the maint cursor
(CAS-written at crates/ravel-maintain/src/scan.rs:494). A key-keyed cache
would have to maintain an invalidation protocol for those two, which is
exactly the class of bug this design should not have.

**The content hash is available before the fetch, and is never checked
after it.** `SegmentRef::content_hash` (crates/ravel-catalog/src/snapshot.rs:48)
carries the object's blake3, known at planning time. The segment read
path never recomputes it: identity checking is footer-field comparison
(`verify_l1_identity`, crates/ravel-query/src/fetcher.rs:1281). The only
blake3 verification on any read path is in the catalog snapshot code
(snapshot_resolve.rs:256, :412).

**No production code touches local disk.** Two exceptions, neither on a
data path: the alert rules file read once at startup
(services/ravel-server/src/alerting.rs:839) and a CLI convenience
(services/ravel-cli/src/store.rs:80). An on-disk cache tier would be the
first production file Ravel creates.

**There is no request coalescing anywhere.** Concurrency is bounded by
semaphores, which do not collapse duplicate work. Two concurrent queries
over the same segment issue two identical GETs today.

## Decision

### 1. A `ravel-cache` crate consulted at the read funnels, never a store decorator

The cache is not an `ObjectStoreBackend` implementation. It is a separate
type that the read paths consult before calling the store, at the funnels
that already exist:

- `SegmentFetcher::guarded_get` (crates/ravel-query/src/fetcher.rs:307)
- `Catalog::guarded_get` (crates/ravel-catalog/src/catalog.rs:96)
- `RlogFetcher::fetch` (crates/ravel-query/src/log_fetcher.rs:289)

Writers, the compactor, and the folder never see it. This is the whole
reason for not decorating the store: a cache that sits under every writer
is a cache that has to reason about writes.

### 2. Keyed by content hash, not by object key

The cache key is `(tenant_hash, content_hash, offset, len)`. The content
hash is the object's blake3, known from `SegmentRef` before the fetch is
planned.

This gives three properties an object-key cache cannot have. Two writers
that produce identical bytes share an entry. A re-PUT after a
`CreateIfAbsent` `AlreadyExists` cannot alias stale bytes. And no mutable
object is representable as a key at all, because HEAD and the maint
cursor have no content hash in any `SegmentRef`, so the type system keeps
them out rather than a rule keeping them out. (Parquet tables add a second
key kind for pinned external objects; see the pinned-key amendment
below.)

`tenant_hash` is in the key even though the content hash alone would be
unique. It is a defence-in-depth boundary: a hash collision or a
programming error cannot serve one tenant's bytes to another.

### 3. Two tiers

**RAM**, for decoded structures: parsed footers, `SERIES_IDX`,
`LABEL_DICT`, `SKIP_IDX`, `FIELD_DIR`, `STREAM_DIR`. The five existing
catalog caches stay where they are and gain hit, miss, and byte counters
feeding `QueryAccounting` (ADR-0044). `HeadCache` gains the capacity
bound it lacks.

**Local disk**, for raw compressed byte ranges of immutable objects.
This is the first production file Ravel writes, and it carries three
rules. The path is opt-in: with no `--cache-dir`, only the RAM tier
exists and behavior is exactly today's. Nothing durable is ever written
there. A cache directory that is missing, full, corrupt, or from a
previous release is discarded and rebuilt, never repaired.

### 4. Correctness never depends on the cache, and a test proves it

A hit re-runs the identical checksum verification a store read runs: the
footer, section, page, block, frame, and window crc32c functions listed
in the read-path survey are called on cached bytes exactly as on fetched
bytes. A corrupt entry is therefore indistinguishable from a corrupt S3
read and fails closed on the existing path.

**Amended, after a checkpoint review found the original
instruction unimplementable.** It said the blake3 is verified once, when
bytes are admitted to the disk tier. The cache crate cannot do that.
`CacheKey` is `(tenant_hash, content_hash, offset, len)` where
`content_hash` is the blake3 of the *whole object* and the entry is a byte
sub-range of it, and the key carries no object size, so the crate cannot
even identify the full-object case where `blake3(payload) ==
content_hash` would be checkable.

What actually protects a disk entry, stated so no later reader assumes
more:

- **Corruption after a successful write** is caught by a crc32c over the
  payload, recomputed on every hit. Present and proven.
- **A foreign or stale file at an entry's path** is caught by comparing
  all four key fields in the entry header against the requested key.
  Present and proven.
- **Bytes that were never the named range to begin with** are caught by
  nothing, here or anywhere else in the tree. Such bytes pass the cache's
  crc32c and pass every crc32c in the segment reader's
  footer/section/page/block hierarchy, and produce silently wrong query
  results.

The third case is a real gap and it is not the cache's to close. The
obligation belongs to the funnel that admits bytes: it holds both the
`SegmentRef` the range came from and the bytes themselves, and it must
not admit a payload under a key that does not describe it. The wiring
tasks own this, and the acceptance gate below is what proves it.

The acceptance gate for this epic is a test mode in which every cache hit
returns deliberately corrupted bytes, and the entire query test suite
must still either error with a typed error or return the identical
result. A cache that is load-bearing for correctness is the most likely
serious failure of this design, and this test is the thing that prevents
it.

### 5. Single-flight, hand-written

Concurrent identical fetches collapse into one store call. A dashboard
refresh landing fifty identical queries must produce one GET.

No coalescing primitive exists in the workspace, and `dashmap`, `moka`,
`lru`, and `foyer` are none of them dependencies. The implementation uses
`parking_lot::Mutex` and `tokio::sync::watch`, both already present, in
roughly a hundred lines. No new external dependency.

### 6. Scan-resistant eviction

The disk tier evicts with S3-FIFO. The compactor and the folder run
continuously over cold data in the same process as queries in every mode
except a dedicated maintain deployment; plain LRU or plain FIFO would let
one compaction pass evict the entire query working set. The existing
catalog caches keep their FIFO capacity caps, which are adequate for small
decoded structures.

Scan resistance matters **more** on disk than in RAM, not less, and this
is worth stating because the reasoning inverts easily. It is tempting to
argue that a disk miss is cheap because it only costs a round trip. That
round trip is a fetch from S3, which is the single most expensive thing a
query does and the entire reason this cache exists. The disk tier is also
the large one, so it is where the working set actually lives. A scan that
evicts it converts every subsequent query back into the cold path this
epic was written to remove.

How long S3-FIFO remembers a key evicted from probation is set by the ghost
window amendment below (issue #3270).

### 7. No encryption inside Ravel

The disk tier stores object bytes in plaintext. Deployments requiring
encryption at rest for the cache use an encrypted filesystem, which is
documented as a deployment step.

This follows ADR-0042's precedent exactly: that ADR rejected
Ravel-managed envelope encryption because it would make Ravel a key
management system, and chose to delegate to SSE-KMS. Adding a cipher here
would add a crypto dependency and a key lifecycle to solve a problem the
operating system already solves. The consequence is stated plainly rather
than hidden: with SSE-KMS configured, cached bytes on local disk are not
protected by that key.

## Rejected alternatives

1. **A `CachingStore` decorator implementing `ObjectStoreBackend`.** The
   obvious design, and wrong here. `build_store` is the only composition
   point and it is shared with three ingest routers, the compactor, and
   the folder, so the cache would sit under every writer. The trait keys
   on strings, so it could not distinguish an immutable data object from
   the CAS-written HEAD without an invalidation protocol. Rejected.

2. **Key on `(object_key, etag)`.** Works, and is weaker for no saving.
   It cannot share entries between identical content, it depends on the
   commit protocol's key-reuse rule being true rather than on the key
   being unforgeable, and it admits mutable keys.

3. **Add `moka` or `foyer`.** Both are good. Rejected for now: the cache
   must be consulted at three funnels with a key type Ravel defines, must
   feed `QueryAccounting`, and must support the corrupt-every-hit test
   mode. That is a thin layer over a map plus an eviction policy, and a
   general-purpose caching crate would still need all of it wrapped. If
   the hand-written eviction proves to be the bottleneck, adopting one is
   a small, well-scoped follow-up.

4. **Verify blake3 on every hit.** Rejected: it is a full pass over the
   cached bytes on the hot path to re-prove something admission already
   proved, when the crc32c hierarchy already covers every byte a reader
   interprets on its own access path.

5. **Cache decoded query results as the first tier.** Rejected as a
   starting point: the correctness reasoning for result caching is subtle
   above the fold watermark, and the byte cache benefits every query
   rather than only repeated ones. A watermark-bounded result cache is a
   later decision.

6. **Make the disk tier mandatory.** Rejected: it would make a query node
   unable to start without writable local storage, which contradicts
   "every compute process is disposable".

7. **Encrypt the disk tier inside Ravel.** Rejected, per decision 7.

## Consequences

- `ravel-cache` is a new crate. It depends on `ravel-types` for the
  accounting handle and on nothing else new.
- Ravel writes files in production for the first time. Every failure mode
  of that must degrade to a miss: no read, write, or eviction error may
  ever surface as a query error.
- The RAM tier changes the five existing catalog caches only by adding
  counters and one capacity bound. Their keys, values, and eviction stay
  as they are.
- Cache effectiveness becomes an SLI: byte hit rate, request hit rate,
  and single-flight collapse rate, under ADR-0044's label allowlist.
- Warm and cold query latency become different service levels and must be
  reported separately rather than averaged.
- With SSE-KMS configured, cached bytes on local disk are not protected by
  that key. This is a documented deployment consideration, not a silent
  gap.
- Nothing durable moves. No format, key layout, commit protocol, or
  consistency property changes. A node with its cache directory deleted
  mid-flight answers every query correctly and more slowly.

## Amendment (2026-09-05): startup warm-up measures "most recent" from each tenant's latest ingest hour, not wall-clock now

<!-- amendment-applies: none reason="no section above describes the startup warm-up pass; this records how cache_warm.rs picks its window and retires no earlier wording" -->

The startup cache warm-up pass (`services/ravel-server/src/cache_warm.rs`)
resolves each tenant's most recent parts to prime the RAM/disk tiers
before `/readyz` latches. It originally resolved a fixed `[now - 24h,
now]` window, so a tenant whose last ingest predated `now` by more than
24h got zero parts warmed on restart (issue #1233: such a tenant's first
real query after restart paid the full cold-fetch cost this ADR exists to
avoid). The pass now calls `Catalog::latest_ingest_hour` first and anchors
the warm window to that tenant's own latest ingest hour, so "most recent"
means the tenant's own most recent data, never data recent only relative
to wall-clock time.

This anchors to the ingest-hour bucket a part was filed under, not to the
part's own `max_event_ts_ns`. The new window is a superset of the old
`[now - 24h, now]` window only when every part's `max_event_ts_ns` stays
within `max_ingest_lag_ns` of its ingest-hour bucket's span, which is the
assumption this warm-up serves, not a guarantee `latest_ingest_hour`
enforces. A part filed further out of that bound can still miss the warm
pass.

`Catalog::latest_ingest_hour` itself discovers the latest hour with a
bounded, floored listing that doubles its lookback window backwards from
now (24h, 48h, ..., capped at 64 days across at most 7 probes per shard)
instead of listing a shard's entire commit history, so a tenant with a
long or gapless history no longer pages through it all before `/readyz`.
An "ingest hour" here means any hour bucket holding a commit-record-shaped
key of any kind -- a commit record, a compaction record, a rewrite record,
or a tombstone -- not only a live commit record; the shard loop reports
whichever of those it finds newest, and separately reports whether that
newest hour actually holds a live (non-tombstone) part.

A tenant whose last ingest predates the 64-day cap, or one that has never
ingested this signal at all, both resolve to `Ok(None)`. Exhausting the
probe sweep without finding anything is a normal, expected outcome for a
signal a tenant simply does not use, so `Catalog` itself never logs above
`debug!` for it; only `cache_warm`'s own `log_warm_result` decides between
`info!` and its "parts exist but none warmed" `warn!`, and it does so
using the found hour's live-part flag, not merely whether a hour was
found -- a tenant whose only bucket in range is tombstone-only resolves
`Ok(Some((hour, false)))` and is logged the same as "nothing to warm",
never as the warn-level "has parts but none warmed" case. The flag is
per hour, not per bucket: an hour holding both a live record and a
tombstone reports `true` while the resolve drops the tombstoned bucket,
so that warning can still fire for such an hour when the rest of the
window warms nothing; it is a hint, not a fault signal.

Residual clock-skew case: an event whose timestamp is up to
`max_ingest_lag_ns` behind its ingest-hour bucket, where that ingest hour
is itself not ahead of the wall clock, still resolves correctly -- the
probe's `max_hour` is computed from `now_ns + clock_skew_allowance_ns`,
not from the event timestamp, so a late-arriving event filed under an
hour at or before wall-clock `now` is always within the first probe's
upper bound.

## Amendment (2026-09-27): a pinned key for external Parquet objects (ADR-2040)

<!-- amendment-applies: sections="2. Keyed by content hash, not by object key" pointer="pinned-key amendment" -->

ADR-2040 reads Parquet files that Ravel did not write, in buckets an
operator granted a tenant. Their bytes have no BLAKE3 Ravel computed, and
their owner can overwrite them. They get a second key kind: the
`content_hash` part is a BLAKE3 over (credential profile, bucket, object
key, ETag, version or generation, size), built by a constructor separate
from the one decision 2 describes.

This key is sound only because every read of such an object carries the
recorded ETag and version as a precondition, and a store that fails the
precondition probe cannot back a grant. (The version is a selector, not a
precondition; see the selector correction below.) So a cached range under
this key is always a range of the bytes the precondition admits. It is
weaker than decision 2's key in one way, stated here rather than hidden: on
an unversioned S3 bucket (every S3 bucket for now; see the selector
correction below) a single-PUT ETag is an MD5, and someone who can
write the granted location could in principle forge an MD5 collision. The
harm stays inside the tenants granted that location, because `tenant_hash`
is still part of the key. Decision 2's key is unchanged for every object
Ravel writes.

### Correction (2026-09-28): the version is a selector (ADR-2040 pinning amendment)

<!-- amendment-applies: sections="Amendment (2026-09-27): a pinned key for external Parquet objects (ADR-2040)" pointer="selector correction" -->

The pinned-key amendment above says every read carries the recorded ETag
and version "as a precondition". ADR-2040's pinning amendment corrects
that: object stores treat a version as a selector that picks that version
of the object, and only `If-Match` on the ETag is a precondition. The key
stays sound for the same reason in both cases. A read admitted by the ETag
precondition, or served from the selected version, returns exactly the
bytes the key names, so a cached range under this key is a range of those
bytes.

The MD5 bound above also widens. It names "an unversioned S3 bucket", but
until `S3Store` surfaces the real `x-amz-version-id`, every S3 file is
pinned by ETag alone (ADR-2040's pinning amendment), so the bound applies
to every S3 bucket, versioned or not. GCS files, and Azure files on a
bucket with versioning on, are pinned by version and are outside it.

## Amendment (2026-10-26): the S3-FIFO ghost window covers 30% of the cache in bytes (issue #3270)

<!-- amendment-applies: sections="6. Scan-resistant eviction" pointer="ghost window amendment" -->

Decision 6 chose S3-FIFO and left its ghost queue unsized. The
implementation sized it at `max_bytes / max_entry_bytes` keys, the
smallest resident capacity the entry cap allows, so that a ghost could
never span more than the cache and readmit a completed scan straight to
main. At the server's 64 MiB entry cap that is about 100 keys at a 7 GB
cache. In bytes, it is as wide as the cache for 64 MiB entries and about
2% of it for the 1.3 MB ranges ClickBench reads, so a statement whose
bytes per call exceed the probation queue (10% of `max_bytes`) plus that
2% is never served from the cache, however often it repeats. The ClickBench
hot sweep measured q29 and q31 to q33 (1.3 to 2.3 GB per call against a
6.95 GB cache) fetched in full on every timed call.

The ghost now keeps a key while it is inside either window: the key window
above, or the most recent probation evictions whose sizes sum to at most
30% of `max_bytes`. The window is the wider of the two, so it is never
narrower than before for any entry size and never spans more than the
cache. The ghost lives outside `max_bytes` and outside `bookkeeping_bytes`,
which are unchanged; its key count is capped at one per mille of
`max_bytes` in ghost bookkeeping at 152 bytes a key (45,751 keys at
6,954,184,212 bytes), never below the key window and never above
`max_entries`. Which bound sets the ghost's reach depends on range size.
Above `ghost_window_bytes / ghost_max_keys` (0.3 times 1,000 times 152
bytes, about 45.6 KB, at any cache size where neither the key window nor
`max_entries` clamps the cap) the byte window binds, and a repeated read of
up to about 40% of the cache (probation plus the byte window) is promoted
on its second call, more where the key window is wider. Below it the key
cap binds, and the reach is probation plus 45,751 ranges: for 16 KiB ranges
at 6,954,184,212 bytes that is 749.6 MB of remembered evictions, about
10.8% of `max_bytes`, so a repeated read of up to about 21% of the cache is
promoted. The 152 bytes are one queue slot and one map
entry; the real worst case is about three times that budget. The queue
keeps up to twice the key cap plus 65 slots before it drops stale ones,
both containers round their capacity up as they grow, the map can double
once more to clear removed slots, and neither gives capacity back: at
6,954,184,212 bytes that is 131,072 queue slots of 72 bytes and 131,072 map
buckets of 81 bytes, about 20.1 MB against the 6.95 MB budget. That figure
is per `S3Fifo`: a server started with `--cache-dir` builds two with the same
limits, one for the RAM tier and one for the disk tier
(`services/ravel-server/src/store.rs`, `build_cache`), so about 40.2 MB in
all.
Both tiers use this policy (`DiskCache` builds the same `S3Fifo`, with the
same limits on the server). The ghost holds keys and sizes only, in
memory, so the disk tier's restart behaviour is unchanged. Access intents
(ADR-3072 decision 3b) and the bulk region are unchanged, and ADR-2677's
decision 3 deferral stands: this adds no loop policy.

The figures come from a deterministic simulation of the policy,
`crates/ravel-cache/src/s3fifo/ghost_window.rs`, whose tests pin them:

- W1, the hot sweep (6,954,184,212-byte cache, 43 statements, a warm call
  and three timed calls each): the key window served q31, q32 and q33
  0.000 on calls 2 and 3. The shipped window serves every statement 1.000
  on calls 2 and 3. The model serves q29 1.000 under both windows, where
  the sweep measured it fetched; the model's statement order and column
  sizes are an approximation of the sweep's.
- W2, decision 6 at the server geometry ADR-2677's deferral cites (an
  11.5 GB cache, a 33 MB hot set of 16 KiB ranges read once per thousand
  25 MB scan reads), against scans of 300, 440 and 600 looping objects and
  of unique objects, with the hot set read before the scan or first read
  during it: the hot set's hit rate equals the key window's in all eight
  cases (0.1413 against the 600-object loop with the hot set established).
  The scan's own rate is equal in seven and one re-read lower of 14,400 in
  the eighth. Against a repeated scan of 92, 115 or 161 looping objects
  (0.20, 0.25 and 0.35 of the cache), both sizings serve the hot set and
  the scan's re-reads 1.000 in all six cases, but those cases cannot tell
  ghost sizings apart: the loop and the hot set fit in the cache, nothing
  is evicted, and an unbounded ghost serves the same 1.000.
- W2b, the regime this amendment creates (the same 11.5 GB cache and hot
  set, against a repeated scan of 0.25 or 0.35 of the cache in 1.3 MB
  entries, entries small enough that the key window was the narrower
  bound, with one-off cold reads keeping the cache full: 79,190 to
  120,794 evictions per run): the repeated scan now reaches main, and
  its re-reads are served 0.9118 where the key window served them 0.0000.
  The hot set's hit rate is never below the key window's. It is 1.0000
  under both windows when the hot set was read before the traffic, and
  0.0000 under both when it was first read after a 25 GB interval, which
  outlasts probation and the byte window alike; only a much wider ghost
  serves that case: 0.7641 with a byte window of main's quota (0.9 of the
  cache) or more.
- W3, a loop of 0.5 to 3 times the cache in 1.3 MB or 25 MB entries, read
  five times, then a second such loop: no pass is served less than under
  the key window, and a first loop of 1.05 times the cache in 1.3 MB
  entries is served 0.857 on its third pass and 0.810 after, where it was
  served nothing.

The candidates that were not chosen, from the same simulation's
`ghost_window_report` (`cargo test -p ravel-cache --release --
--ignored --nocapture ghost_window_report`):

| Ghost window | W1 calls 2-3 short | W2 against the 600-object loop, hot set established (key window: hot 0.1413, scan 0.645) | W3 against the key window |
|---|---|---|---|
| 10% of `max_bytes` in bytes alone | q32, q33 | hot 1.0000, scan 0.000 | equal or better |
| 20% in bytes alone | q32 | hot 1.0000, scan 0.000 | equal or better |
| 25% in bytes alone | none | hot 1.0000, scan 0.000 | 1.3-times loop of 25 MB entries 0.695 to 0.000 on its third pass |
| 30% in bytes alone | none | hot 1.0000, scan 0.000 | equal or better |
| 50%, 90% or 100% in bytes, or unbounded | none | hot 0.1210, scan 0.545 | equal or better |

A byte window alone narrows the window for entries above 30% of
`max_entry_bytes` (about 20 MB at a 64 MiB cap), and at the server
geometry that stops promoting a cold loop of 25 MB objects just over the
cache, which keeps a hot set established before the scan (hot rate 1.0)
and serves the loop nothing. That trade is the one ADR-2677's deferral
left open, so the shipped window keeps the key window and changes neither
case. A byte window of 50% or more promotes more of that loop than the key
window does and lowers the hot set's rate to 0.1210; at 30% it stays under
the roughly 37% of the cache the key window spans in 25 MB objects, and
changes neither figure. 25% would leave q32 about 160 MB of margin; 30%
leaves about 510 MB.
