# Read cache

Ravel keeps every durable byte in object storage. Object storage is slower
than local memory. The read cache keeps recently read bytes close to the query
engine, so a repeat read of the same data does not go back to object storage.

A process runs two of them, and every flag and metric on this page covers
both:

- the **fetcher cache**, holding byte ranges of the data objects a query
  scans, reported under `cache="fetch"`;
- the **catalog byte cache**, holding the catalog objects a resolve reads,
  reported under `cache="catalog"`.

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
  threshold to `18446744073709551615` to read every log object whole
  regardless of size.

  On default flags the whole-object read is what happens anyway:
  `--logs-fetch-policy` defaults to `cost-based`, and at the shipped
  reference cost profile (intra-region, where transferred bytes are free
  and the bill is requests) it resolves to reading every object whole in
  one GET. Set `--logs-fetch-policy byte-minimal` for the block-range
  shape, where the threshold above governs. See the flag table below.

Both PromQL queries and SQL queries over the `samples` table use the
metric path. SQL queries over the `logs` table use the log path.

Two things are not cached, for two different reasons:

- **Spans.** The `spans` SQL table is queryable on `POST /api/v1/sql`, but its
  reads are uncached. The span scan fetches each RSPAN segment straight from
  the object store on every query: the fetch is accounted and tenant-checked,
  and the tenant identity the log path uses to key its cache entries serves
  only as that tenant check here. A repeated span query therefore re-reads the
  same objects. This is the one genuine cache gap.
- **Alert transitions and audit records.** These are written durably, but
  the SQL session registers exactly three tables, `samples`, `logs`, and
  `spans`, and no query surface can read alert or audit data at all, so there
  is no read path to cache.

Each cache entry is keyed by tenant, the content hash of the object it
came from, and the byte offset and length. Content is immutable once
written, so a cache entry never goes stale: it is either the right bytes
or not present at all.

## Two tiers

The cache has a RAM tier and a local-disk tier. The RAM tier is always on
(unless `--disable-cache`). The disk tier is opt-in: `--cache-dir <path>`
attaches a local-disk tier at that directory to both the fetcher cache and the
catalog byte cache, so a RAM eviction is served from local disk instead of
re-paying the object-store round trip. With no `--cache-dir`, the process has
a RAM tier only.

The disk tier is disposable. Its directory is created lazily on first admission
and is never required to exist; a missing, full, or corrupt cache directory
degrades to a store read, never a query error, so a node whose cache directory
is deleted mid-flight answers every query correctly and only more slowly.
Nothing durable is ever written there, and a cache directory from a previous
release is discarded and rebuilt rather than repaired.

**Encryption at rest.** Bytes written to the cache
directory are **not** encrypted by Ravel, even with SSE-KMS configured for
object storage. SSE-KMS protects object bytes at rest in the store, not the
local cache. If you need bytes-at-rest encryption for the cache directory,
provide it at the filesystem/volume layer (an encrypted volume mounted at
`--cache-dir`).

## CLI flags

| Flag | Default | Meaning |
|---|---|---|
| `--cache-max-bytes <n>` | `268435456` (256 MiB) | Maximum bytes the RAM tier holds. Bounds **both** caches in the process from one number: the fetcher cache and the catalog byte cache. Read once at startup; there is no live resize. |
| `--cache-dir <path>` | none | Directory for the local-disk tier. Set, both the fetcher cache and the catalog byte cache gain a disk tier at this path, each bounded by the same `--cache-max-bytes` number (there is no separate disk-tier capacity flag). Absent, the process has a RAM tier only. Bytes written here are not SSE-KMS encrypted (see "Two tiers" above). |
| `--disable-cache` | off | Turns **both** caches off. No cache is constructed at all, so query *results* are byte-for-byte the same as a build with no cache code, and the process holds no read-cache memory. This is the flag to set in a memory-constrained container. |
| `--logs-block-range-threshold <bytes>` | `524288` (512 KiB) | Log-object size above which a `logs` query reads only the pruning-relevant blocks (a tail probe plus per-block ranges, cached per block) instead of the whole object. Set it to `18446744073709551615` to read every log object whole regardless of size; set it to `0` to use the block-range path for every object. Read once at startup. Under a resolved fetch policy that reads every object whole (`--logs-fetch-policy request-minimal`, or `cost-based` at a profile whose bytes are free) this flag is **overridden**, and startup logs a WARN naming the value it overrode. |
| `--logs-fetch-policy <policy>` | `cost-based` | The logs read shape. `request-minimal` reads every object whole in one covering GET, with no tail probe and no ranged read: the cost-preferring shape where transfer is free and the object-store bill is requests. `byte-minimal` uses ranged reads wherever they save more bytes than a request costs, for egress-billed and network-constrained deployments. `cost-based` derives the choice from `--store-cost-profile`; at the shipped reference profile (intra-region, transfer free) that resolves to request-minimal behaviour, so **a deployment on default flags reads log objects whole**. Read once at startup; the running process never changes its own policy. The resolved policy, profile, and byte quantities are logged at startup on the `logs fetch policy resolved` line. |
| `--store-cost-profile <path>` | reference profile (`s3-intra-region-2026`) | TOML file carrying this deployment's object-store prices in integer nanodollars: `name`, `put_class_nanodollars`, `get_class_nanodollars`, `transfer_nanodollars_per_gib`, `retrieval_nanodollars_per_gib`, and optionally `delete_class_nanodollars`. Only `--logs-fetch-policy cost-based` reads it, to derive how many transferred bytes one saved request is worth; no price reaches the fetch layer. An unreadable file, invalid TOML, an unknown key, or a blank `name` fails startup rather than falling back to the reference prices. |
| `--logs-max-fetch-run-bytes <bytes>` | `67108864` (64 MiB) | The fetch bound: the maximum length of one covering GET on the log path, on every policy. An object at or under it is read in a single request; a larger one is read as sequential block-aligned covering sub-ranges of at most this many bytes each, so one oversized object cannot pull an unbounded response into memory. `0` is refused at startup. |

### The max-age sweep

Both tiers bound how long an entry's raw bytes may persist, so bytes of an
erased subject cannot outlive the erasure pass on a query node by more than a
fixed window, in RAM as well as on local disk. Two knobs govern it:

| Knob | Default | Meaning |
|---|---|---|
| `max_entry_age_ns` | `82800000000000` (23 h) | Maximum wall-clock age an entry is served at. A hit on an older entry is treated as a miss and its bytes dropped. |
| `sweep_interval_ns` | `3600000000000` (1 h) | Period of the background sweep that drops over-age idle entries nothing re-reads, so an entry that is never touched still ages out on its own. |

The worst-case residue of an idle entry is `max_entry_age_ns +
sweep_interval_ns`. The defaults are set so that sum is 24 h exactly: the
max-age sits one sweep interval below the bound rather than at it, so the
default meets the bound instead of overshooting it by the sweep period.

Neither knob is a command-line flag. Both tiers are constructed with the
shipped defaults, and there is no way to override the max-age sweep from the
command line.

## Startup warmup

When the cache is on, `ravel-server` warms it before it reports ready
(`/readyz`). For each tenant storage holds data for, it reads a small,
bounded number of that tenant's most recent metric and log segments, so the
first real query after a restart is not the one paying full cold cost.

Warmup is best-effort:

- It has an overall time budget. If storage is slow or a tenant list is
  large, warmup stops early and the process still becomes ready; it just
  starts with a smaller warm set.
- A failure warming one tenant or one segment is logged and skipped. It
  never fails startup.

## Metrics

When a cache is on, `GET /metrics` reports these counters, labeled by
`mode` and by `cache`. The `cache` label names which of the two the sample
belongs to: `cache="fetch"` for the fetcher cache, `cache="catalog"` for the
catalog byte cache. When a disk tier is configured (`--cache-dir`), each
cache's samples additionally carry a `tier="ram"`/`tier="disk"` label so the
two tiers' hit rates are reported separately; with a RAM tier only, no `tier=`
label appears. Each cache renders its own series, so the hit-rate formulas
below can be computed per cache or summed across both:

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
  dropped because they aged past the per-entry max-age. Counts
  every drop point: a hit that found an over-age entry, the startup scan, and
  the periodic background sweep that reaches idle entries nothing re-reads.
  Distinct from an eviction (which makes room under the byte or entry bound):
  this is a time bound, not a capacity bound.

  It is nonzero only when a disk tier is configured (`--cache-dir`); a process
  with no disk tier never emits it. The RAM tier applies the same max-age but
  has no counter of its own: an over-age RAM entry is reported as an ordinary
  miss, so a workload whose entries routinely age out shows a hit rate lower
  than its access pattern would suggest and nothing else.

With both caches off (`--disable-cache`), none of these samples appear on
`/metrics` at all: neither `cache="fetch"` nor `cache="catalog"`.

Request hit rate is
`hits / (hits + misses)`; byte hit rate is `bytes_served / (bytes_served +
bytes_admitted)`. Filter by the `cache` label for one cache's rate, or omit it
(let PromQL sum the series) for the whole process.

## Sizing for logs column-filtering waste

A logs (RLOG) query's `QueryAccounting` carries two decode-time byte counters
next to its wire-byte counters: `page_bytes_fetched`, the
stored bytes of every page present in the blocks the query decoded, and
`page_bytes_decoded`, the stored bytes of only the pages the query's column
projection kept. These are a decode-time measurement, not a wire measurement:
they count bytes a fetched block already holds, distinct from `s3_bytes` (the
actual bytes moved over the network). On the whole-object read shape the whole
block arrives on the wire and a narrow projection skips decompressing the
pages it does not need; on the ranged read shape the page directory lets the
fetch pull only the projected columns' pages, so the projection narrows the
wire bytes as well.

The ratio `page_bytes_decoded / page_bytes_fetched` is the interpretation lever.
When it is close to 1, the query decodes nearly everything its blocks contain and
a larger cache working set is the main way to make repeat runs cheaper. When it
is small -- most fetched page bytes are thrown away by column filtering, the
wide-schema, narrow-projection shape -- a whole-object read is fetching and
caching the block in full to serve a few columns. Two responses apply: narrow
the projection further where the query allows it, and size the cache to the
*working set of whole blocks* the workload touches rather than to the decoded
byte volume, since on the whole-object shape the cache admits and holds whole
blocks regardless of how little of each a given query decodes. A small decoded
fraction across a
tenant's queries is the signal that its cache should be sized against block
footprint, not against what its projections actually read.

The ratio is workload-dependent: it is set by how wide the tenant's log schema
is against how narrow its queries' projections are, so there is no
representative figure to quote. Measure your own.

Neither counter reaches a running server's query-facing accounting.
`EXPLAIN ANALYZE` surfaces the sibling page-count fields,
`pages_decoded`/`pages_skipped`, not their byte-denominated equivalents, so
the ratio is available only to a caller that reads `QueryAccounting`
in-process, which is what the `ravel-bench` logs scan does.

## What is not cached

The one genuine gap is spans: RSPAN reads have no cache seam, so a repeated
span query re-reads the same objects from the store every time. Alert
transitions and audit records are not a cache gap, because they have no read
path at all: neither is a registered SQL table.

## Background

The read cache is [ADR-0046](../adrs/0046-read-cache-tier.md); the max-age
bound on cached bytes comes from the erasure guarantee in
[ADR-0064](../adrs/0064-selective-subject-erasure.md); the logs block-range
read shape is
[ADR-0107](../adrs/0107-pruning-proportional-logs-fetch.md), and the fetch
policy above it is
[ADR-0996](../adrs/0996-request-cost-aware-fetching.md).
