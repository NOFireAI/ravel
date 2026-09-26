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

  On default flags against a remote store, the whole-object read is what
  happens anyway: `--logs-fetch-policy` defaults to `cost-based`, and at the
  shipped reference cost profile (intra-region, where transferred bytes are
  free and the bill is requests) it resolves to reading every object whole in
  one GET. Against a loopback `--s3-endpoint` the unset default instead
  resolves to `byte-minimal` (ADR-2014), so the block-range shape below is
  what happens without any flag at all. Set `--logs-fetch-policy
  byte-minimal` explicitly to get that shape on a remote store too, where the
  threshold above governs. See the flag table below.

Both PromQL queries and SQL queries over the `samples` table use the
metric path. SQL queries over the `logs`, `alerts` and `audit` tables use the
log path.

Alert transitions and audit records are log objects on their own signal
prefixes, and the `alerts` and `audit` tables read them through that same log
fetcher, so their bytes are cached on the same terms as any other log object:
whole object or block ranges by the same size threshold and fetch policy, keyed
the same way, accounted through the same funnel, and held by whichever tiers
the process built: the RAM tier always, plus the local-disk tier when
`--cache-dir` is set, exactly as for `logs`. Nothing about the cache is
specific to those two signals.

One thing is not cached:

- **Spans.** The `spans` SQL table is queryable on `POST /api/v1/sql`, but its
  reads are uncached. The span scan fetches each RSPAN segment straight from
  the object store on every query: the fetch is accounted and tenant-checked,
  and the tenant identity the log path uses to key its cache entries serves
  only as that tenant check here. A repeated span query therefore re-reads the
  same objects. This is the one genuine cache gap.

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
`--cache-dir`). Ravel does restrict who can read those plaintext bytes
locally: on Unix it creates each cache entry file owner-read-write only
(`0600`) and each cache directory it creates owner-only (`0700`), whatever
the ambient umask is. That is filesystem permissions, not encryption: it stops
another local user reading the bytes, and it does nothing against anyone who
can read the volume itself. Ravel sets modes within its own namespace
subtree under the configured directory, and never on the configured
directory itself: a cache root you create yourself keeps the mode you gave
it, while a root that does not exist yet is created owner-only along with
any missing ancestor of it. On startup Ravel also narrows the directories
and entries beneath its namespace that an older build left at the ambient
umask, so upgrading a node in place closes the same gap on a cache tree that
already exists. A tree from before the per-instance namespace layout sits
beside the namespace rather than under it, and the startup narrowing does
not reach it: remove it with `ravel-cli cache reclaim-legacy --cache-dir
<dir> --apply` (without `--apply` the command only lists; see the
maintenance guide's section on reclaiming a pre-namespacing cache
directory).

## CLI flags

| Flag | Default | Meaning |
|---|---|---|
| `--cache-max-bytes <n>` | fetcher cache derived: 25% of the process memory budget; catalog byte cache derived: 5% of the same budget (each `268435456`, 256 MiB, when memory cannot be read) | Maximum bytes the RAM tier holds. **Set**, it bounds **both** caches at that one value. **Unset**, the two derive independently from the process memory budget (effective, cgroup-capped memory minus a fixed overhead reserve, not raw host memory): the fetcher cache at 25% (`7516192768` on the 30 GB reference host) and the catalog byte cache at 5% (`1503238553`), a smaller separate ceiling so the two LRU caches do not each claim a quarter of RAM. Startup refuses to start, rather than silently clamping, if an explicit value here pushes the two resolved caps above the process memory budget (not under `--disable-cache`, which builds neither cache, so neither cap claims anything). Read once at startup; there is no live resize. |
| `--cache-dir <path>` | none | Directory for the local-disk tier. Set, both the fetcher cache and the catalog byte cache gain a disk tier at this path, each bounded by its own resolved RAM ceiling: the fetcher cache's disk tier by `--cache-max-bytes` or its derived 25% share, the catalog byte cache's by its own resolved value (the derived 5% share, or `--cache-max-bytes` when that flag is set explicitly). There is no separate disk-tier capacity flag. Absent, the process has a RAM tier only. Bytes written here are not SSE-KMS encrypted, and on Unix Ravel keeps what it writes here readable by its own user only (see "Two tiers" above for both). |
| `--disable-cache` | off | Turns **both** caches off. No cache is constructed at all, so query *results* are byte-for-byte the same as a build with no cache code, and the process holds no read-cache memory. This is the flag to set in a memory-constrained container. It covers the caches of object bytes only: the catalog's two per-tenant record caches stay on, held at their 10,000-entry floor rather than the derived capacity, about 18 MB per actively-queried tenant. See [the catalog record caches](#the-catalog-record-caches-which-this-budget-does-not-cover). Because neither cache exists, neither ceiling is charged against the process memory budget: the whole budget goes to the shared SQL/fetch accountant, and the startup refusal above cannot fire, whatever `--cache-max-bytes` says. |
| `--logs-block-range-threshold <bytes>` | `524288` (512 KiB) | Log-object size above which a `logs` query reads only the pruning-relevant blocks (a tail probe plus per-block ranges, cached per block) instead of the whole object. Set it to `18446744073709551615` to read every log object whole regardless of size; set it to `0` to use the block-range path for every object. Read once at startup. Under a resolved fetch policy that reads every object whole (`--logs-fetch-policy request-minimal`, or `cost-based` at a profile whose bytes are free) this flag is **overridden**, and startup logs a WARN naming the value it overrode. |
| `--logs-fetch-policy <policy>` | `cost-based`, unless unset with `--store s3` against a loopback `--s3-endpoint`, where it derives `byte-minimal` (ADR-2014) | The logs read shape. `request-minimal` reads every object whole in one covering GET, with no tail probe and no ranged read: the cost-preferring shape where transfer is free and the object-store bill is requests. `byte-minimal` uses ranged reads wherever they save more bytes than a request costs, for egress-billed, network-constrained, or local-disk-bound deployments. `cost-based` derives the choice from `--store-cost-profile`; at the shipped reference profile (intra-region, transfer free) that resolves to request-minimal behaviour, so **a deployment on default flags against a remote store reads log objects whole**. `latency-first` resolves the same byte quantities as `byte-minimal`; it is an intent for deployments where cold wall-clock matters more than the request bill, and it pays off only once GET concurrency is also raised explicitly (it sets no concurrency default of its own), which carries a memory caveat -- see the operations guide before turning it on. An explicit flag always wins, including an explicit `cost-based` on a loopback endpoint. Read once at startup; the running process never changes its own policy. The resolved policy, its source (`flag`, `default`, or `derived-loopback-endpoint`), the profile, and the byte quantities are logged at startup on the `logs fetch policy resolved` line. |
| `--store-cost-profile <path>` | reference profile (`s3-intra-region-2026`) | TOML file carrying this deployment's object-store prices in integer nanodollars: `name`, `put_class_nanodollars`, `get_class_nanodollars`, `transfer_nanodollars_per_gib`, `retrieval_nanodollars_per_gib`, and optionally `delete_class_nanodollars`. Only `--logs-fetch-policy cost-based` reads it, to derive how many transferred bytes one saved request is worth; no price reaches the fetch layer. An unreadable file, invalid TOML, an unknown key, or a blank `name` fails startup rather than falling back to the reference prices. |
| `--logs-max-fetch-run-bytes <bytes>` | `67108864` (64 MiB) | The fetch bound: the maximum length of one covering GET on the log path, on every policy. An object at or under it is read in a single request; a larger one is read as sequential block-aligned covering sub-ranges of at most this many bytes each, so one oversized object cannot pull an unbounded response into memory. `0` is refused at startup. |

Both `--cache-max-bytes` figures are **LRU caps, not reservations**. Neither
cache pre-allocates its ceiling; each holds only the bytes it has actually
admitted and evicts least-recently-used entries once it reaches its cap. The
ceiling is an upper bound on resident cache bytes, not memory claimed at
startup. Because the two caches are independent and the SQL memory pools
(`--sql-max-query-bytes`, `--sql-tenant-max-bytes`) are bounded separately
again, the **sum of every ceiling can exceed physical RAM**: the fetcher cache
(25% of the process memory budget by default) plus the catalog byte cache
(5%) plus the SQL pools is more than 100% of raw host memory on the
reference host. That is deliberate.
The caches only fill under a working set that touches that many distinct bytes,
and the SQL pools abort a query rather than growing past their own ceiling, so
the peaks do not coincide the way the arithmetic sum suggests. Size a host
against its real working set, not against the sum of the caps. Set an explicit
`--cache-max-bytes` to bound both caches at one value on a memory-constrained
host, or `--disable-cache` to hold no read-cache memory at all and leave the
entire process memory budget to the query path. `--disable-cache` does not
reach the catalog record caches, which are not read caches of object bytes and
cost about 18 MB per actively-queried tenant under it; see
[the catalog record caches](#the-catalog-record-caches-which-this-budget-does-not-cover)
below.

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
"Most recent" is measured from each tenant's own latest ingest hour, not
from wall-clock time: a tenant that has not ingested in the
last day still gets its most recent parts warmed, not zero.

Warmup is best-effort:

- It has an overall time budget. If storage is slow or a tenant list is
  large, warmup stops early and the process still becomes ready; it just
  starts with a smaller warm set.
- A failure warming one tenant or one segment is logged and skipped. It
  never fails startup.

To tell a warmed restart from an unwarmed one, or to see what was warmed,
read the `cache_warm` module's log lines rather than guessing from query
latency. `cache_warm` alone decides the level of the per-(tenant, signal)
line -- `Catalog::latest_ingest_hour` never logs above `debug!`, so a
signal a tenant does not use never produces a `warn!` on its own.
The line is an `info!` (or `warn!` if the tenant has a live part for that
signal but none of them warmed -- a resolve or fetch failure ate the
whole signal) and carries `tenant_hash`, `signal`, `parts_warmed`, and
`latest_ingest_hour`, which takes one of three values:

- an hour number: the tenant's own latest ingest hour was found -- the
  newest hour bucket holding a commit-record-shaped key of any kind
  (commit, compaction, rewrite, or tombstone), not only a live commit
  record. `parts_warmed` counts what actually warmed from it, which is 0
  when that hour's only key was a tombstone; that case still logs at
  `info!`, since a tombstone-only hour has no live part to have failed to
  warm. An hour that holds both a live record and a tombstone counts as
  having a live part while the resolve drops the tombstoned bucket, so
  when nothing else in the window warms the `warn!` can fire for it;
  read that warning as a hint to look at the resolve, not as a fault.
- `"none"`: `Catalog::latest_ingest_hour` returned `Ok(None)` -- the
  tenant has no discoverable ingest for that signal, either because it
  never ingested it or because its last ingest predates the discovery
  lookback cap (see below). `parts_warmed` is always 0 for this value,
  and the `cache_warm` pass logs it at `info!`, not `warn!`: no parts to
  warm is a normal outcome, not a failure.
- `"error"`: the `Catalog::latest_ingest_hour` listing itself failed
  (an object store fault, or the per-shard LIST budget refused). This
  value appears on a separate `warn!` event, `cache warmup:
  latest_ingest_hour failed; skipping this tenant and signal`, which
  carries `tenant_hash`, `signal`, `error` and `latest_ingest_hour` and
  no `parts_warmed`; the pass emits no result line for that tenant and
  signal, so a filter on `parts_warmed` alone will not show it. It is
  distinct from the `"none"` case so a store fault is never mistaken for
  a tenant that has no data.

`Catalog::latest_ingest_hour` exhausting its lookback cap (64 days)
without finding anything logs at `debug!`, not `warn!`: it cannot itself
tell a tenant that never used a signal apart from one whose ingest is
genuinely stuck past the cap -- both produce the identical `Ok(None)` --
and neither case is a failure worth a warning on its own. Do not expect a
`warn!` from this path on a normal warmup pass; the only `warn!` a
tenant/signal combination can produce is the `"error"` case above or
`cache_warm`'s own "has parts but none warmed" line.

One final `info!` at the end of the pass
carries the total `parts_warmed` across every tenant and signal and the
pass's `elapsed_ms`. A restart whose log shows this final line warmed the cache; one that hit
the time budget above never reaches it, and logs the `warn!` naming the
deadline instead -- the per-(tenant, signal) lines up to that point are
whatever the pass warmed before it ran out of time.

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

## The catalog record caches, which this budget does not cover

Everything above is a read cache: a cache of object bytes whose ceiling is a
share of the process memory budget. The catalog also keeps two per-tenant
caches of decoded commit and compaction records, and neither takes a share of
that budget, so their memory comes out of what is left after the carved
shares, never out of them. Budget for them separately.

One capacity bounds both caches independently. The capacity is derived per
deployment from the shard count, the signal count and the configured max flush
delay, not from a flat constant:

```text
shards * 6 signals * ceil(3600 / max_flush_delay_seconds) * 3 unsealed hours
```

floored at 10,000 entries and capped at 25,000. Neither cache is denominated in
entries alone: each is ALSO held to a byte budget of `capacity x 900 bytes`,
22.5 MB per tenant at the cap, and evicts against whichever bound binds first.
Neither record type has a bounded size, which is why:

- The **commit-record cache** is bounded in bytes at
  `commit_cache_max_bytes_per_tenant`. `CommitRecord.declared_column_stats` is
  a repeated field with no cap in the proto, in validation, or in the
  tenant-config declared-column path, so a record declaring 200 typed
  attribute columns charges about 20 KB where an ordinary one charges 864
  bytes, more than twenty times the planning rate. The cache charges each
  entry an estimate of the live heap it holds and evicts least-recently-used
  until the charged total is back inside the budget.
- The **compaction-record cache** is bounded in bytes at the same share,
  `compaction_cache_max_bytes_per_tenant`. A compaction record carries one
  `CompactionInputIdentity` per L0 segment it merged and that list is capped
  neither by the format nor by validation. One L1 record over 1,800 L0
  segments charges about 137 KB, roughly 150 times the planning rate, so an
  entry count alone would have let a single tenant hold hundreds of times the
  figure below. The cache evicts oldest-first until its charged bytes are back
  inside the budget.

The 900 bytes is a PLANNING rate the capacity is derived against, not a
per-entry cap: an ordinary stats-free commit entry charges 864 bytes (a
119-byte commit key held twice, the decoded struct, and the record's own heap),
and what each cache enforces is the summed charge against its budget. So the
capacity is an entry cap rather than a guaranteed residency: a tenant whose
records carry typed attribute column statistics holds proportionally fewer than
`capacity` of them, and the memory stays inside the figure either way.

So the worst case is 45 MB per actively-queried tenant, both halves enforced in
bytes by their own eviction path rather than projected from a per-entry
estimate. That is what the cap holds constant across every deployment shape
(`--shards 64` derives 2,073,600 entries and 3.7 GB per tenant uncapped).
Budget it as 45 MB times the number of tenants queried concurrently: 100 of
them is 4.5 GB worst case,
and idle tenants are reclaimed by idle-tenant eviction. At the shipped
2-second cadence the cap decides the value for every shard count, so
`--shards` does not move it there.

The capacity covers a tenant's unsealed tail up to the cap, not the whole
tail. Three things put a real tail past it: the cap itself, since the estimate
at the shipped defaults is already 129,600 entries; the flush cadence term,
which counts the age trigger only while a shard also flushes as soon as its
estimated object bytes reach `target_bytes` (8 MiB by default); and the three
unsealed hours, which assume the default seal parameters. That last term is
the one an operator can move: `--gc-max-flush-lifetime 4h` puts the seal
margin at 4h20m, so the oldest unsealed hour can have started 5h20m ago and
the tail spans up to 5.34 hours, which three under-counts by about 1.8x. A
tenant over the bound pays a per-record GET on every resolve. That is not what
it paid before: the old cache held a flat 10,000 records whatever they cost, so
a tenant whose records carry declared-column statistics now holds fewer than it
did and pays GETs it did not pay before. The bound trades that hit rate for a
memory figure the eviction path enforces. The levers are a coarser
`--max-flush-delay`, a shorter `--gc-max-flush-lifetime`, or a lower shard
count, all of which shrink the tail itself.

`--disable-cache` does not turn these caches off, because a resolve with no
record cache re-reads every record from the store. It does hold the capacity
at the 10,000-entry floor rather than the derived value, so the flag costs
about 18 MB per actively-queried tenant: a 9 MB byte budget for each of the
two caches, both enforced.

Neither cache is exported. The resident-bytes gauge covers the object-byte
read caches only (`ravel_cache_resident_bytes` is emitted for the fetch
family's tiers, and `ravel_cache_max_bytes` for the fetch and catalog byte
ceilings), so there is no scrape-time figure for how many bytes a tenant's
record caches actually hold. The numbers above are bounds to budget against,
not something to read back off a running server.

## What is not cached

The one genuine gap is spans: RSPAN reads have no cache seam, so a repeated
span query re-reads the same objects from the store every time. Alert
transitions and audit records are not a gap: the `alerts` and `audit` tables
read through the log fetcher, so their bytes are cached exactly as log bytes
are.

## Background

The read cache is [ADR-0046](../adrs/0046-read-cache-tier.md); the max-age
bound on cached bytes comes from the erasure guarantee in
[ADR-0064](../adrs/0064-selective-subject-erasure.md); the logs block-range
read shape is
[ADR-0107](../adrs/0107-pruning-proportional-logs-fetch.md), and the fetch
policy above it is
[ADR-0996](../adrs/0996-request-cost-aware-fetching.md).
