# Read cache

Ravel keeps every durable byte in object storage. Object storage is slower
than local memory. The read cache (ADR-0046) keeps recently read bytes
close to the query engine, so a repeat read of the same data does not go
back to object storage.

The cache is an optimization only. It never changes a query result. A
process with the cache off, or a process that just restarted with an empty
cache, answers every query correctly. It only answers more slowly. Warm
and cold queries are different speeds, not different correctness levels.

## What gets cached

The cache stores byte ranges read from two kinds of objects:

- **Metric segments (RSEG).** A metric query reads a segment's footer,
  then the catalog sections it needs, then only the page ranges that match
  the query. Each of these byte ranges is cached separately.
- **Log objects (RLOG).** Which unit is cached depends on the object's
  size, against `--logs-block-range-threshold` (512 KiB by default). A log
  query reads a smaller object whole, so the cached unit is the whole
  object. For a larger one it reads only the blocks time and predicate
  pruning kept: a probe of the object's tail, the directory sections it
  needs, and the candidate blocks (adjacent ones fetched together in one
  request). Each of those is cached separately, one entry per block, so a
  later query whose blocks partly overlap reuses what it can. Set the
  threshold to `18446744073709551615` to read every log object whole, as
  before this split existed.

  On default flags the whole-object read is what happens anyway:
  `--logs-fetch-policy` defaults to `cost-based`, and at the shipped
  reference cost profile (intra-region, where transferred bytes are free
  and the bill is requests) it resolves to reading every object whole in
  one GET. Set `--logs-fetch-policy byte-minimal` for the block-range
  shape, where the threshold above governs. See the flag table below.

Both PromQL queries and SQL queries over the `samples` table use the
metric path. SQL queries over the `logs` table use the log path.

Two query surfaces do not use the cache today:

- **Spans (traces).** The `spans` SQL table is queryable on
  `POST /api/v1/sql`, but its reads are uncached. The span scan fetches
  each RSPAN segment straight from the object store on every query: the
  fetch is accounted and tenant-checked, and the tenant identity the log
  path uses to key its cache entries serves only as that tenant check
  here. A repeated span query therefore re-reads the same objects. Wiring
  spans into this cache is still open work.
- **The `alerts` and `audit` SQL tables.** These tables exist in
  `ravel-sql`, but nothing in production reaches them through a real SQL
  query today, so they have no cached read path to speak of.

Each cache entry is keyed by tenant, the content hash of the object it
came from, and the byte offset and length. Content is immutable once
written, so a cache entry never goes stale: it is either the right bytes
or not present at all.

## Two tiers

The cache has a RAM tier and a local-disk tier. The RAM tier is always on
(unless `--disable-cache`). The disk tier is opt-in: `--cache-dir <path>`
attaches a local-disk tier at that directory to both the query fetcher cache
and the catalog byte cache, so a RAM eviction is served from local disk instead
of re-paying the S3 round trip. With no `--cache-dir`, only the RAM tier exists
and behavior is exactly as before.

The disk tier is disposable. Its directory is created lazily on first admission
and is never required to exist; a missing, full, or corrupt cache directory
degrades to a store read, never a query error, so a node whose cache directory
is deleted mid-flight answers every query correctly and only more slowly.
Nothing durable is ever written there, and a cache directory from a previous
release is discarded and rebuilt rather than repaired.

**Encryption at rest (ADR-0046 decision 7).** Bytes written to the cache
directory are **not** encrypted by Ravel, even with SSE-KMS configured for
object storage. SSE-KMS protects object bytes at rest in the store, not the
local cache. If you need bytes-at-rest encryption for the cache directory,
provide it at the filesystem/volume layer (an encrypted volume mounted at
`--cache-dir`).

## CLI flags

| Flag | Default | Meaning |
|---|---|---|
| `--cache-max-bytes <n>` | `268435456` (256 MiB) | Maximum bytes the RAM tier holds. Bounds **every** ADR-0046 cache in the process from one number: the fetcher cache and the catalog's byte cache both. Read once at startup; there is no live resize. |
| `--cache-dir <path>` | none | Directory for the local-disk tier. Set, both the fetcher cache and the catalog byte cache gain a disk tier at this path, each bounded by the same `--cache-max-bytes` number (there is no separate disk-tier capacity flag). Absent, only the RAM tier exists. Bytes written here are not SSE-KMS encrypted (see "Two tiers" above). |
| `--disable-cache` | off | Turns **every** ADR-0046 cache off: the fetcher cache and the catalog's byte cache both. No cache is constructed at all, so query *results* are byte-for-byte the same as a build with no cache code, and the process holds no read-cache memory. This is the flag to set in a memory-constrained container. |
| `--logs-block-range-threshold <bytes>` | `524288` (512 KiB) | Log-object size above which a `logs` query reads only the pruning-relevant blocks (a tail probe plus per-block ranges, cached per block) instead of the whole object. Set it to `18446744073709551615` to read every log object whole, the shape before this split existed; set it to `0` to use the block-range path for every object. Read once at startup. Under a resolved fetch policy that reads every object whole (`--logs-fetch-policy request-minimal`, or `cost-based` at a profile whose bytes are free) this flag is **overridden**, and startup logs a WARN naming the value it overrode. |
| `--logs-fetch-policy <policy>` | `cost-based` | The logs read shape (ADR-0996). `request-minimal` reads every object whole in one covering GET, with no tail probe and no ranged read: the cost-preferring shape where transfer is free and the object-store bill is requests. `byte-minimal` is the older behaviour, ranged reads wherever they save more bytes than a request costs, for egress-billed and network-constrained deployments. `cost-based` derives the choice from `--store-cost-profile`; at the shipped reference profile (intra-region, transfer free) that resolves to request-minimal behaviour, so **a deployment on default flags reads log objects whole**. Read once at startup; the running process never changes its own policy. The resolved policy, profile, and byte quantities are logged at startup on the `logs fetch policy resolved` line. |
| `--store-cost-profile <path>` | reference profile (`s3-intra-region-2026`) | TOML file carrying this deployment's object-store prices in integer nanodollars: `name`, `put_class_nanodollars`, `get_class_nanodollars`, `transfer_nanodollars_per_gib`, `retrieval_nanodollars_per_gib`, and optionally `delete_class_nanodollars`. Only `--logs-fetch-policy cost-based` reads it, to derive how many transferred bytes one saved request is worth; no price reaches the fetch layer. An unreadable file, invalid TOML, an unknown key, or a blank `name` fails startup rather than falling back to the reference prices. |
| `--logs-max-fetch-run-bytes <bytes>` | `67108864` (64 MiB) | The fetch bound: the maximum length of one covering GET on the log path, on every policy. An object at or under it is read in a single request; a larger one is read as sequential block-aligned covering sub-ranges of at most this many bytes each, so one oversized object cannot pull an unbounded response into memory. `0` is refused at startup. |

### Disk-tier max-age sweep

The disk tier bounds how long an entry's raw bytes may sit on local disk,
so bytes of a subject erased by ADR-0064's rewrite pass cannot outlive the
erasure sweep on a query node by more than a fixed window. Two knobs
(`CacheLimits`) govern it:

| Knob | Default | Meaning |
|---|---|---|
| `max_entry_age_ns` | `82800000000000` (23 h) | Maximum wall-clock age an entry is served at. A hit on an older entry is treated as a miss and its bytes dropped. |
| `sweep_interval_ns` | `3600000000000` (1 h) | Period of the background sweep that drops over-age idle entries nothing re-reads, so an entry that is never touched still ages out on its own. |

The worst-case residue of an idle entry is `max_entry_age_ns +
sweep_interval_ns`; the defaults (23 h + 1 h) are tuned so that sum meets
ADR-0064's 24 h bound exactly. These are configuration on the disk tier
(`CacheLimits`), not CLI flags: the disk tier is constructed with the shipped
defaults, and there is no flag to override the max-age sweep from the command
line today.

## Startup warmup

When the cache is on, `ravel-server` warms it before it reports ready
(`/readyz`). For each tenant storage holds data for, it reads a small,
bounded number of that tenant's most recent metric and log parts, so the
first real query after a restart is not the one paying full cold cost.

Warmup is best-effort:

- It has an overall time budget. If storage is slow or a tenant list is
  large, warmup stops early and the process still becomes ready; it just
  starts with a smaller warm set.
- A failure warming one tenant or one part is logged and skipped. It
  never fails startup.

## Metrics

When a cache is on, `GET /metrics` reports these counters, labeled by
`mode` and by `cache` (ADR-0044's label allowlist). The `cache` label names
which ADR-0046 cache the sample belongs to: `cache="fetch"` for the fetcher
cache, `cache="catalog"` for the catalog's byte cache. When a disk tier is
configured (`--cache-dir`), each cache's samples additionally carry a
`tier="ram"`/`tier="disk"` label so the two tiers' hit rates are reported
separately; with no `--cache-dir` no `tier=` label appears and the exposition
is byte-for-byte as before. Each cache renders its own series, so the hit-rate
formulas below can be computed per cache or summed across both:

- `ravel_cache_hits_total` / `ravel_cache_misses_total`: read outcomes.
- `ravel_cache_bytes_served_total`: bytes returned from the cache on a
  hit.
- `ravel_cache_bytes_admitted_total`: bytes written into the cache.
- `ravel_cache_evictions_total`: entries evicted to make room.
- `ravel_cache_disk_errors_degraded_to_misses_total`: a disk-tier read
  found a corrupt or unreadable entry and treated it as a miss instead of
  failing the query. Distinct from a normal miss (nothing was there);
  this counter means something was there and could not be trusted.

  It is nonzero only when a disk tier is configured (`--cache-dir`) and that
  tier is unhealthy: a `tier="disk"` sample above zero means the disk tier
  found entries it could not trust, not merely that it was cold. A process with
  no `--cache-dir` never emits it.
- `ravel_cache_disk_entries_expired_max_age_total`: disk-tier entries
  dropped because they aged past the per-entry max-age (ADR-0064). Counts
  every drop point: a hit that found an over-age entry, the startup scan, and
  the periodic background sweep that reaches idle entries nothing re-reads.
  Distinct from an eviction (which makes room under the byte or entry bound):
  this is a time bound, not a capacity bound.

  It is nonzero only when a disk tier is configured (`--cache-dir`); a process
  with no disk tier never emits it.

With every cache off (`--disable-cache`), none of these samples appear on
`/metrics` at all: neither `cache="fetch"` nor `cache="catalog"`.

These counters cover every ADR-0046 cache in the process. Request hit rate is
`hits / (hits + misses)`; byte hit rate is `bytes_served / (bytes_served +
bytes_admitted)`. Filter by the `cache` label for one cache's rate, or omit it
(let PromQL sum the series) for the whole process.

## Sizing for logs column-filtering waste

A logs (RLOG) query's `QueryAccounting` carries two decode-time byte counters
next to its wire-byte counters (ADR-0107 decision 4): `page_bytes_fetched`, the
stored bytes of every page present in the blocks the query decoded, and
`page_bytes_decoded`, the stored bytes of only the pages the query's column
projection kept. These are a decode-time measurement, not a wire measurement:
they count bytes a fetched block already holds, distinct from `s3_bytes` (the
actual bytes moved over the network). Column filtering today is decode-time only
-- a narrow projection skips decompressing the pages it does not need, but the
whole block still arrives on the wire, because RLOG has no per-page checksum to
verify a sub-block fetch against.

The ratio `page_bytes_decoded / page_bytes_fetched` is the interpretation lever.
When it is close to 1, the query decodes nearly everything its blocks contain and
a larger cache working set is the main way to make repeat runs cheaper. When it
is small -- most fetched page bytes are thrown away by column filtering, the
wide-schema, narrow-projection shape -- the same block is being fetched and cached
in full to serve a few columns. Two responses apply: narrow the projection
further where the query allows it (fewer columns decoded per block, though not
fewer bytes fetched until a future per-page-crc format change lands), and size the
cache to the *working set of whole blocks* the workload touches rather than to the
decoded byte volume, since the cache admits and holds whole blocks regardless of
how little of each a given query decodes. A small decoded fraction across a
tenant's queries is the signal that its cache should be sized against block
footprint, not against what its projections actually read.

A concrete measured ratio for a representative workload is not quoted here: no
`ravel-bench` logs run was executed in the environment this guidance was written
in, so a fabricated number is deliberately avoided. Neither counter is exposed
through the server's query-facing accounting today (`EXPLAIN ANALYZE` surfaces
only the sibling page-count fields, `pages_decoded`/`pages_skipped`, not their
byte-denominated equivalents), so to see the ratio for your own workload, drive
the query through `QueryAccounting` in-process -- an in-process `ravel-bench`
run against `crates/ravel-bench/src/logs_scan_scaling.rs` is the existing
example -- rather than reading it off a running server.

## Known gaps

- **Spans have no cache path**, because span reads are not wired into the
  cache layer yet: the `spans` SQL table's fetcher has no `with_cache`
  seam, unlike the RSEG/RLOG fetchers.
- **The `alerts` and `audit` SQL tables are not reachable in
  production**, so caching them is not meaningful yet.
