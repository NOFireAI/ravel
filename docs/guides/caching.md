# Read cache

Ravel keeps every durable byte in object storage. Object storage is slower
than local memory. The read cache keeps recently read bytes close to the
query engine, so a repeat read of the same data does not go back to object
storage.

The cache is an optimization only. It never changes a query result. A
process with the cache off, or a process that just restarted with an empty
cache, answers every query correctly. It only answers more slowly. Warm and
cold queries are different speeds, not different correctness levels.

## What gets cached

The cache stores byte ranges read from two kinds of objects:

- **Metric segments (RSEG).** A metric query reads a segment's footer, then
  the catalog sections it needs, then only the page ranges that match the
  query. Ravel caches each of these byte ranges separately.
- **Log objects (RLOG).** The cached unit depends on the object's size
  against `--logs-block-range-threshold` (512 KiB by default). A log query
  reads a smaller object whole, so the cached unit is the whole object. For a
  larger object it reads only the blocks that time and predicate pruning
  kept: a probe of the object's tail, the directory sections it needs, and
  the candidate blocks. Ravel caches each of those separately, one entry per
  block, so a later query whose blocks partly overlap reuses what it can. Set
  the threshold to `18446744073709551615` to read every log object whole.

Both PromQL queries and SQL queries over the `samples` table use the metric
path. SQL queries over the `logs` table use the log path.

Two query surfaces do not use the cache today:

- **Spans (traces).** The `spans` SQL table is queryable on
  `POST /api/v1/sql`, but its reads are uncached. The span scan fetches each
  RSPAN segment straight from the object store on every query. The fetch is
  accounted and tenant-checked. A repeated span query re-reads the same
  objects.
- **The `alerts` and `audit` SQL tables.** These tables exist in
  `ravel-sql`, but no production SQL query reaches them today, so they have
  no cached read path.

Ravel keys each cache entry by tenant, the content hash of the object it came
from, and the byte offset and length. Content is immutable once written, so a
cache entry never goes stale. It is either the right bytes or not present at
all.

## Two tiers

The cache has a RAM tier and a local-disk tier. The RAM tier is always on,
unless `--disable-cache`. The disk tier is opt-in. `--cache-dir <path>`
attaches a local-disk tier at that directory to both the query fetcher cache
and the catalog byte cache, so a RAM eviction is served from local disk
instead of re-paying the S3 round trip. With no `--cache-dir`, only the RAM
tier exists.

The disk tier is disposable. Ravel creates its directory lazily on first
admission and never requires it to exist. A missing, full, or corrupt cache
directory degrades to a store read, never a query error, so a node whose
cache directory is deleted mid-flight answers every query correctly and only
more slowly. Ravel never writes anything durable there, and it discards and
rebuilds a cache directory from a previous release rather than repairing it.

**Encryption at rest.** Ravel does not encrypt bytes written to the cache
directory, even with SSE-KMS configured for object storage. SSE-KMS protects
object bytes at rest in the store, not the local cache. If you need
bytes-at-rest encryption for the cache directory, provide it at the
filesystem or volume layer. Mount an encrypted volume at `--cache-dir`.

## CLI flags

| Flag | Default | Meaning |
|---|---|---|
| `--cache-max-bytes <n>` | `268435456` (256 MiB) | Maximum bytes the RAM tier holds. Bounds **every** read cache in the process from one number: the fetcher cache and the catalog's byte cache both. Read once at startup. There is no live resize. |
| `--cache-dir <path>` | none | Directory for the local-disk tier. When set, both the fetcher cache and the catalog byte cache gain a disk tier at this path, each bounded by the same `--cache-max-bytes` number. There is no separate disk-tier capacity flag. When absent, only the RAM tier exists. Ravel does not SSE-KMS-encrypt bytes written here (see "Two tiers" above). |
| `--disable-cache` | off | Turns **every** read cache off: the fetcher cache and the catalog's byte cache both. Ravel constructs no cache at all, so query *results* are byte-for-byte the same as a build with no cache code, and the process holds no read-cache memory. Set this flag in a memory-constrained container. |
| `--logs-block-range-threshold <bytes>` | `524288` (512 KiB) | Log-object size above which a `logs` query reads only the pruning-relevant blocks (a tail probe plus per-block ranges, cached per block) instead of the whole object. Set it to `18446744073709551615` to read every log object whole. Set it to `0` to use the block-range path for every object. Read once at startup. |

### Disk-tier max-age sweep

The disk tier bounds how long an entry's raw bytes can sit on local disk, so
bytes of an erased subject cannot outlive the erasure sweep on a query node
by more than a fixed window. Two knobs (`CacheLimits`) govern it:

| Knob | Default | Meaning |
|---|---|---|
| `max_entry_age_ns` | `82800000000000` (23 h) | Maximum wall-clock age an entry is served at. Ravel treats a hit on an older entry as a miss and drops its bytes. |
| `sweep_interval_ns` | `3600000000000` (1 h) | Period of the background sweep that drops over-age idle entries nothing re-reads, so an entry that is never touched still ages out on its own. |

The worst-case residue of an idle entry is `max_entry_age_ns +
sweep_interval_ns`. The defaults (23 h + 1 h) sum to the 24 h erasure bound
exactly. These are configuration on the disk tier (`CacheLimits`), not CLI
flags. Ravel constructs the disk tier with the shipped defaults, and no flag
overrides the max-age sweep from the command line today.

## Startup warmup

When the cache is on, `ravel-server` warms it before it reports ready
(`/readyz`). For each tenant storage holds data for, it reads a small,
bounded number of that tenant's most recent metric and log parts, so the
first real query after a restart does not pay full cold cost.

Warmup is best-effort:

- Warmup has an overall time budget. If storage is slow or a tenant list is
  large, warmup stops early and the process still becomes ready. It just
  starts with a smaller warm set.
- Ravel logs and skips a failure warming one tenant or one part. It never
  fails startup.

## Metrics

When a cache is on, `GET /metrics` reports these counters, labeled by `mode`
and by `cache`. The `cache` label names which cache the sample belongs to:
`cache="fetch"` for the fetcher cache, `cache="catalog"` for the catalog's
byte cache. When a disk tier is configured (`--cache-dir`), each cache's
samples also carry a `tier="ram"`/`tier="disk"` label, so the two tiers' hit
rates are reported separately. With no `--cache-dir`, no `tier=` label
appears. Each cache renders its own series, so you can compute the hit-rate
formulas below per cache or summed across both:

- `ravel_cache_hits_total` / `ravel_cache_misses_total`: read outcomes.
- `ravel_cache_bytes_served_total`: bytes returned from the cache on a hit.
- `ravel_cache_bytes_admitted_total`: bytes written into the cache.
- `ravel_cache_evictions_total`: entries evicted to make room.
- `ravel_cache_disk_errors_degraded_to_misses_total`: a disk-tier read found
  a corrupt or unreadable entry and treated it as a miss instead of failing
  the query. This is distinct from a normal miss, where nothing was there.
  This counter means something was there and could not be trusted.

  It is nonzero only when a disk tier is configured (`--cache-dir`) and that
  tier is unhealthy. A `tier="disk"` sample above zero means the disk tier
  found entries it could not trust, not merely that it was cold. A process
  with no `--cache-dir` never emits it.
- `ravel_cache_disk_entries_expired_max_age_total`: disk-tier entries dropped
  because they aged past the per-entry max-age. Counts every drop point: a
  hit that found an over-age entry, the startup scan, and the periodic
  background sweep that reaches idle entries nothing re-reads. This is
  distinct from an eviction, which makes room under the byte or entry bound.
  This is a time bound, not a capacity bound.

  It is nonzero only when a disk tier is configured (`--cache-dir`). A
  process with no disk tier never emits it.

With every cache off (`--disable-cache`), none of these samples appear on
`/metrics` at all: neither `cache="fetch"` nor `cache="catalog"`.

These counters cover every read cache in the process. Request hit rate is
`hits / (hits + misses)`. Byte hit rate is `bytes_served / (bytes_served +
bytes_admitted)`. Filter by the `cache` label for one cache's rate, or omit
it and let PromQL sum the series for the whole process.

## Sizing for logs column-filtering waste

A logs (RLOG) query's `QueryAccounting` carries two decode-time byte counters
next to its wire-byte counters: `page_bytes_fetched`, the stored bytes of
every page present in the blocks the query decoded, and `page_bytes_decoded`,
the stored bytes of only the pages the query's column projection kept. These
are a decode-time measurement, not a wire measurement. They count bytes a
fetched block already holds, distinct from `s3_bytes` (the actual bytes moved
over the network). Column filtering today is decode-time only. A narrow
projection skips decompressing the pages it does not need, but the whole
block still arrives on the wire, because RLOG has no per-page checksum to
verify a sub-block fetch against.

The ratio `page_bytes_decoded / page_bytes_fetched` is the interpretation
lever. When it is close to 1, the query decodes nearly everything its blocks
contain, and a larger cache working set is the main way to make repeat runs
cheaper. When it is small, most fetched page bytes are thrown away by column
filtering: the wide-schema, narrow-projection shape, where the same block is
fetched and cached in full to serve a few columns. Two responses apply.
Narrow the projection further where the query allows it, which decodes fewer
columns per block though it does not fetch fewer bytes. And size the cache to
the *working set of whole blocks* the workload touches rather than to the
decoded byte volume, since the cache admits and holds whole blocks regardless
of how little of each a given query decodes. A small decoded fraction across
a tenant's queries is the signal to size its cache against block footprint,
not against what its projections actually read.

Neither counter is exposed through the server's query-facing accounting today.
`EXPLAIN ANALYZE` surfaces the sibling page-count fields, `pages_decoded` and
`pages_skipped`, not their byte-denominated equivalents. To see the ratio for
your own workload, drive the query through `QueryAccounting` in-process. An
in-process `ravel-bench` run against
`crates/ravel-bench/src/logs_scan_scaling.rs` is the existing example.

## Known gaps

- **Spans have no cache path**, because span reads are not wired into the
  cache layer yet: the `spans` SQL table's fetcher has no `with_cache` seam,
  unlike the RSEG/RLOG fetchers.
- **The `alerts` and `audit` SQL tables are not reachable in production**, so
  caching them is not meaningful yet.
</content>
</invoke>
