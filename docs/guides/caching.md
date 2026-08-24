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

The cache has a RAM tier and a local-disk tier. Only the RAM tier works
today. The `--cache-dir` flag for the disk tier exists in the CLI, but
setting it fails startup with an explicit error rather than silently
doing nothing: the query fetchers this process calls only know how to
attach a RAM cache. See "Known gaps" below.

## CLI flags

| Flag | Default | Meaning |
|---|---|---|
| `--cache-max-bytes <n>` | `268435456` (256 MiB) | Maximum bytes the RAM tier holds. Bounds **every** ADR-0046 cache in the process from one number: the fetcher cache and the catalog's byte cache both. Read once at startup; there is no live resize. |
| `--cache-dir <path>` | none | Reserved for the disk tier. Setting it fails startup today (see "Known gaps"). |
| `--disable-cache` | off | Turns **every** ADR-0046 cache off: the fetcher cache and the catalog's byte cache both. No cache is constructed at all, so query *results* are byte-for-byte the same as a build with no cache code, and the process holds no read-cache memory. This is the flag to set in a memory-constrained container. |
| `--logs-block-range-threshold <bytes>` | `524288` (512 KiB) | Log-object size above which a `logs` query reads only the pruning-relevant blocks (a tail probe plus per-block ranges, cached per block) instead of the whole object. Set it to `18446744073709551615` to read every log object whole, the shape before this split existed; set it to `0` to use the block-range path for every object. Read once at startup. |

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
ADR-0064's 24 h bound exactly. These are configuration on the disk tier,
not CLI flags yet: like `--cache-dir`, the disk tier is not wired into a
running process (see "Known gaps"), so there is no flag to set them from
today.

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
cache, `cache="catalog"` for the catalog's byte cache. Each cache renders its
own series, so the hit-rate formulas below can be computed per cache or summed
across both:

- `ravel_cache_hits_total` / `ravel_cache_misses_total`: read outcomes.
- `ravel_cache_bytes_served_total`: bytes returned from the cache on a
  hit.
- `ravel_cache_bytes_admitted_total`: bytes written into the cache.
- `ravel_cache_evictions_total`: entries evicted to make room.
- `ravel_cache_disk_errors_degraded_to_misses_total`: a disk-tier read
  found a corrupt or unreadable entry and treated it as a miss instead of
  failing the query. Distinct from a normal miss (nothing was there);
  this counter means something was there and could not be trusted.

  This counter is always 0 today. No disk tier is attached, because
  `--cache-dir` fails startup (see the known gaps below). Do not alert on
  it until a disk tier exists.
- `ravel_cache_disk_entries_expired_max_age_total`: disk-tier entries
  dropped because they aged past the per-entry max-age (ADR-0064). Counts
  every drop point: a hit that found an over-age entry, the startup scan, and
  the periodic background sweep that reaches idle entries nothing re-reads.
  Distinct from an eviction (which makes room under the byte or entry bound):
  this is a time bound, not a capacity bound.

  This counter is always 0 today, for the same reason as the one above: no
  disk tier is attached. Do not alert on it until a disk tier exists.

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
in, so a fabricated number is deliberately avoided. Read the two counters off a
real query's accounting (or `EXPLAIN ANALYZE`, which surfaces the sibling
`pages_decoded`/`pages_skipped` counts) to get the ratio for your own workload.

## Known gaps

- **`--cache-dir` is not wired up.** The disk tier (`DiskCache`) exists
  as a crate, but the query fetchers only accept a RAM cache today. Until
  that attachment point is added, `--cache-dir` fails startup instead of
  silently doing nothing.
- **Spans have no cache path**, because span reads are not wired into the
  cache layer yet: the `spans` SQL table's fetcher has no `with_cache`
  seam, unlike the RSEG/RLOG fetchers.
- **The `alerts` and `audit` SQL tables are not reachable in
  production**, so caching them is not meaningful yet.
