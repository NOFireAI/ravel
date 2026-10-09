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

A repeated scan larger than the cache is a case this decision did not
consider; the loop amendment below adds it for both tiers.

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

## Amendment (2026-10-09): a repeated scan larger than the cache serves a stable subset (ADR-2677)

<!-- amendment-applies: sections="6. Scan-resistant eviction" pointer="loop amendment" -->

Refs: #2681. Decision 6 guards a hot working set against a cold scan that
passes once. It did not consider a hot scan that repeats: a query run again
over the same N objects in the same order, over a cache that holds C < N
of them. Plain S3-FIFO serves that loop nothing. Each entry leaves
probation untouched, because its next touch is a whole pass away, and the
ghost window was sized from `max_bytes / max_entry_bytes`: 25 to 74 keys
for a 1.7 to 5.0 GB cache under the server's 64 MiB `max_entry_bytes`
(1.7e9 / 2^26 = 25.3, 5.0e9 / 2^26 = 74.5), against a 2,617-object loop of
3.8 MB objects. So no entry reached main, and a 1.7 to 5.0 GB cache served
0.01 GB per hot pass (#2615); on a 32 GB host whose derived cache fell
0.8 GB under the corpus, it served nothing (#2639 W2).

The rules in `crates/ravel-cache/src/s3fifo.rs` that close this count
distances in touches: a logical clock advances by one on every hit and
every admission. Every entry records its reuse distance, the longer of its
last two gaps between touches.

- **Fill main while it has room.** An entry leaving probation untouched
  moves into main, rather than out of the cache, while main has room inside
  its share of the bounds: `max_bytes` less the probation quota of one
  tenth, and `max_entries` less one tenth of it (at least one entry). This
  evicts nothing. Such an entry has no reuse distance yet and is never
  overdue (next rule).
- **A loop entry that misses its turn gives up its slot.** A main entry
  whose reuse distance is at least the resident capacity in entries is a
  loop entry: only a scan larger than the cache re-reads an entry that far
  apart. It is overdue once it has gone untouched for more than 9/8 of its
  reuse distance. When main is full, an entry leaving probation untouched
  takes the slot of the most overdue loop entry, if there is one, and is
  otherwise evicted to the ghost queue. Under a steady loop no entry is
  overdue, so main's set does not change. When the loop stops and another
  starts, the old loop's entries become overdue one by one and the new
  loop's entries take their slots during its first pass.
- **A ghost hit displaces only something stale.** A key returning from the
  ghost queue enters main if main has room, or by taking the slot of the
  most overdue loop entry, or of the main entry at the front if that entry
  has gone untouched for longer than the returning key's own reuse
  distance. Otherwise the front entry moves to the back and the returning
  key starts over in probation. Under a steady loop every main entry has
  been idle for less than a pass, so no returning key of the same loop
  displaces it, while a hot key with a short reuse distance still displaces
  a cold main entry.
- **The ghost remembers twice the resident capacity.** Resident capacity
  is estimated as `max_bytes` over the average resident entry size, capped
  at `max_entries` and never below `max_bytes / max_entry_bytes`. For the
  #2615 geometry that is 447 to 1,315 entries (1.7e9 / 3.8e6 = 447.4,
  5.0e9 / 3.8e6 = 1,315.8), so the ghost remembers 894 to 2,630 keys, and
  50 to 148 before the first entry is resident. The ghost is memory outside
  `max_bytes`: at the server's 1,000,000-entry cap it holds at most
  2,000,000 keys of under 500 bytes each, which it reaches only when the
  average resident entry is under a millionth of `max_bytes`.

Main's share is where untouched entries stop filling it, not a cap. A
promotion out of probation, or a displacement that admits a larger entry
than it evicts, can take main past its share; `max_bytes` and
`max_entries` hold for the whole cache.

What the crate tests pin, each at the #2615 geometry (C about N/2), the
#2639 W2 geometry (C about 0.9 N), and a mixed-size loop of 0.5 to 1.5
times 3.8 MB objects at half its bytes:

- `repeated_scan_larger_than_the_cache_serves_a_stable_subset`: a loop over
  an empty cache serves at least 0.8 x C/N of its bytes on passes 2 and 3,
  the same set on both (measured 0.45, 0.81, 0.45).
- `a_second_loop_after_the_cache_is_full_converges`: loops B and C, each
  starting after the loop before it filled the cache, meet the same bounds
  from their second pass.
- `a_loop_after_a_cold_scan_converges_from_its_third_pass`: after a one-pass
  cold scan of 3N, the loop serves nothing on pass 2, then at least the
  floor on passes 3 and 4, the same set on both.
- `a_loop_five_times_the_cache_after_a_cold_scan_is_not_served`: the same
  floor and stable set for a loop five times the cache over an empty cache
  (0.18), and the limit of the previous guarantee: that loop outruns the
  ghost, so after the cold scan it serves nothing in six passes.
- `background_cold_scan_does_not_evict_the_hot_working_set` and
  `hot_working_set_resident_first_survives_a_continuing_cold_scan`: decision
  6 in both orders. A hot working set that arrives while a cold scan has
  filled the cache takes residency and keeps it, and one resident before
  the scan loses no touch to it.

ADR-2677 decision 3 also asks that a loop after a cold scan meet the floor
from its second pass. No policy found meets that. On its first pass such a
loop cannot be told apart from the cold scan continuing, so pass 2 can be
served only if the scan's unproven main entries expire, and a loop's own
entries are unproven during its first pass too. An expiry after 1.2 to 4.0
times the resident capacity idle was tried. Every horizon in that range
stops a loop five times the cache from being served over an empty cache
(the first assertion of
`a_loop_five_times_the_cache_after_a_cold_scan_is_not_served`, 0.18 without
expiry), because its first-pass entries expire before pass 2 reads them.
Below 2.0 the horizon also breaks
`repeated_scan_larger_than_the_cache_serves_a_stable_subset` and
`a_second_loop_after_the_cache_is_full_converges`, and at no horizon does
the loop after a cold scan reach the floor on pass 2 at C about 0.9 N
(0.49 at 2.0, at most 0.42 above it). So
`a_loop_after_a_cold_scan_converges` conflicts with the first-loop
guarantee for any loop longer than the horizon; it keeps the decision's
bound and is ignored, and the policy here has no expiry.

Both tiers keep one policy. The disk tier and the catalog byte cache build
the same `S3Fifo`, so they get the loop behaviour too, and the decision 6
tests `s3_fifo_scan_resistance_keeps_working_set_resident`,
`s3_fifo_scan_resistance_survives_repeated_scans` and
`disk_tier_survives_repeated_scans_via_s3_fifo_eviction` pass unchanged. No
`CacheLimits` option was needed, and `max_entry_bytes` is unchanged.

ADR-2677's rejected alternatives cover the other two fixes: bypassing the
cache for one-pass scans refuses the hot run, and plain LRU serves nothing
on a loop larger than itself.
