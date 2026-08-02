# ADR-0046: A content-addressed read cache at the read funnels, not a store decorator

Status: Accepted (2026-08-02)

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
them out rather than a rule keeping them out.

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

**Amended 2026-08-02, after a checkpoint review found the original
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
