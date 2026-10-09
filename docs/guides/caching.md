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

The cache is an optimization only. It never changes a query result. A process
with the cache off, or one that restarted with an empty cache, answers every
query correctly and only more slowly.

## What gets cached

The cache stores byte ranges read from two kinds of objects:

- **Metric segments (RSEG).** A metric query reads the footer of a segment,
  then the catalog sections that it needs, then only the page ranges that
  match the query. Each of these byte ranges is cached separately.
- **Log objects (RLOG).** The cached unit depends on the size of the object,
  against `--logs-block-range-threshold` (512 KiB by default):
  - A log query reads a smaller object whole, so the cached unit is the whole
    object.
  - For a larger object, the query reads only the blocks that time and
    predicate pruning kept: a probe of the tail of the object, the directory
    sections that it needs, and the candidate blocks. Adjacent blocks are
    fetched together in one request. Each of those is cached separately, one
    entry per block, so a later query whose blocks partly overlap reuses what
    it can.
  - Set the threshold to `18446744073709551615` to read every log object whole
    regardless of size.

Both PromQL queries and SQL queries over the `samples` table use the metric
path. SQL queries over the `logs`, `alerts` and `audit` tables use the log
path.

Each cache entry is keyed by tenant, the content hash of the object that it
came from, and the byte offset and length. Content is immutable once written,
so a cache entry never goes stale. It is the right bytes, or it is not
present.

### Log objects on default flags

On default flags, Ravel makes the choice per object against a projection
break-even. `--logs-fetch-policy` defaults to `cost-based`, and at the shipped
reference cost profile the break-even is 18,900,000 bytes.

- The blocks of an object of that size or less are read in one whole-object
  GET with no tail probe.
- A larger object takes the block-range shape when the bytes that its
  projection skips exceed the break-even. Otherwise it is read whole.
- To get the block-range shape for every object that the threshold routes
  there, set `--logs-fetch-policy byte-minimal` explicitly.

See the [flag table](#cli-flags), and
[the cost model](cost-model.md#the-derived-value) for the derivation.

### Alerts and audit records

Alert transitions and audit records are log objects on their own signal
prefixes. The `alerts` and `audit` tables read them through the same log
fetcher, so their bytes are cached on the same terms as any other log object:

- whole object or block ranges, by the same size threshold and fetch policy
- keyed the same way
- accounted through the same funnel
- held by whichever tiers the process built: the RAM tier whenever the fetcher
  cache is built, plus the local-disk tier when `--cache-dir` is set

### Spans are not cached

The `spans` SQL table is queryable on `POST /api/v1/sql`, but its reads are
uncached. This is the one cache gap.

- The span scan fetches each RSPAN segment straight from the object store on
  every query. RSPAN reads have no cache seam.
- The fetch is accounted and tenant-checked. The tenant identity that the log
  path uses to key its cache entries serves only as that tenant check here.
- A repeated span query therefore reads the same objects again.

## Two tiers

The cache has a RAM tier and a local-disk tier.

The RAM tier is on unless `--disable-cache` is set or the ceiling of the
fetcher cache is `0`:

- A `--cache-max-bytes` of `0` builds no fetcher cache.
- A gateway with neither cache flag set resolves both ceilings to `0`.

The disk tier is opt-in. `--cache-dir <path>` attaches a local-disk tier at
that directory to both the fetcher cache and the catalog byte cache. A RAM
eviction is then served from local disk and does not pay the object-store
round trip again. With no `--cache-dir`, the process has a RAM tier only.

The disk tier is disposable:

- Its directory is created lazily on first admission and is never required to
  exist.
- A missing, full, or corrupt cache directory degrades to a store read, never
  a query error. A node whose cache directory is deleted mid-flight answers
  every query correctly and only more slowly.
- Nothing durable is ever written there.
- A cache directory from a previous release is discarded and rebuilt, not
  repaired.

### Encryption at rest

Ravel does **not** encrypt the bytes that it writes to the cache directory,
even with SSE-KMS configured for object storage. SSE-KMS protects object bytes
at rest in the store, not the local cache. If you need bytes-at-rest
encryption for the cache directory, provide it at the filesystem/volume layer
(an encrypted volume mounted at `--cache-dir`).

Ravel does restrict who can read those plaintext bytes locally. On Unix it
creates each cache entry file owner-read-write only (`0600`) and each cache
directory that it creates owner-only (`0700`), whatever the ambient umask is.
That is filesystem permissions, not encryption. It stops another local user
reading the bytes, and it does nothing against anyone who can read the volume
itself.

Ravel sets modes within its own namespace subtree under the
configured directory, and never on the configured directory
itself: a cache root you create yourself keeps the mode you gave
it, while a root that does not exist yet is created owner-only along with
any missing ancestor of it. On startup Ravel also narrows the directories
and entries beneath its namespace that an older build left at the ambient
umask, so upgrading a node in place closes the same gap on a cache tree that
already exists.

A tree from before the per-instance namespace layout sits beside the
namespace, not under it, and the startup narrowing does not reach it. Remove
it with `ravel-cli cache reclaim-legacy --cache-dir <dir> --apply`. Without
`--apply` the command only lists. See the section of the maintenance guide on
reclaiming a pre-namespacing cache directory.

## CLI flags

| Flag | Default | Meaning |
|---|---|---|
| `--cache-max-bytes <n>` | derived: 25% of the process memory budget, or 40% against a loopback `--s3-endpoint` (`268435456`, 256 MiB, when memory cannot be read; `0` in `--mode gateway`) | Maximum bytes the fetcher cache's RAM tier holds. Bounds the fetcher cache only; the catalog byte cache has its own `--catalog-cache-max-bytes` below and this flag never reaches it. **Unset**, it derives from the process memory budget (effective, cgroup-capped memory minus an overhead reserve, not raw host memory -- fixed at 2 GiB from 8 GiB of memory up and scaled down below it at the default ingest ceiling, and raised above 2 GiB in `--mode all` by an ingest ceiling above 1.75 GiB, see the [configuration guide](operations/configuration.md)): 25% (`7516192768` on the 30 GiB reference host) normally, or a larger 40% share when the store is `s3` against a loopback endpoint, where a miss is served from the same local disk the store reads from and a cache that holds the whole working set serves repeated statements without going back to it. Startup refuses to start, rather than silently clamping, if an explicit value here together with the catalog byte cache's ceiling brings their sum to or above the process memory budget (not under `--disable-cache`, which builds neither cache, so neither cap claims anything, and not in `--mode gateway`, which serves no query and derives no memory budget). Read once at startup; there is no live resize. |
| `--catalog-cache-max-bytes <n>` | derived: 5% of the process memory budget (`268435456`, 256 MiB, when memory cannot be read; `0` in `--mode gateway`) | Maximum bytes the catalog byte cache's RAM tier holds. A separate ceiling from `--cache-max-bytes` above; unset, it derives its own 5% share of the process memory budget (`1503238553` on the 30 GiB reference host) regardless of `--cache-max-bytes` or the loopback share it may have taken. Counted in the same startup refusal as `--cache-max-bytes`. Read once at startup. |
| `--cache-dir <path>` | none | Directory for the local-disk tier. Set, both the fetcher cache and the catalog byte cache gain a disk tier at this path, each bounded by its own resolved RAM ceiling: the fetcher cache's disk tier by its resolved `--cache-max-bytes`, the catalog byte cache's by its resolved `--catalog-cache-max-bytes`. There is no separate disk-tier capacity flag. Absent, the process has a RAM tier only. Bytes written here are not SSE-KMS encrypted, and on Unix Ravel keeps what it writes here readable by its own user only (see "Two tiers" above for both). |
| `--disable-cache` | off | Turns **both** caches off. No cache is constructed at all, so query *results* are byte-for-byte the same as a build with no cache code, and the process holds no read-cache memory. This is the flag to set in a memory-constrained container. It covers the caches of object bytes only: the catalog's two per-tenant record caches stay on, held at their 10,000-entry floor rather than the derived capacity, about 18 MB per actively-queried tenant. See [the catalog record caches](#the-catalog-record-caches). Because neither cache exists, neither ceiling is charged against the process memory budget: the whole budget goes to the shared SQL/fetch accountant, and the startup refusal above cannot fire, whatever `--cache-max-bytes` says. |
| `--logs-block-range-threshold <bytes>` | `524288` (512 KiB) | Log-object size above which a `logs` query reads only the pruning-relevant blocks (a tail probe plus per-block ranges, cached per block) instead of the whole object. Set it to `18446744073709551615` to read every log object whole regardless of size; set it to `0` to use the block-range path for every object. Read once at startup. Under a resolved fetch policy that reads every object whole (`--logs-fetch-policy request-minimal`, or `cost-based` at a profile whose bytes are free and which records no request timings) this flag is **overridden**, and startup logs a WARN naming the value it overrode. Under `cost-based` with a rate derived from the profile, the shipped default, it is kept, and a narrow projection is read ranged only where it skips more than the projection break-even, the larger of this threshold and three request costs. |
| `--logs-fetch-policy <policy>` | `cost-based` | The logs read shape. `request-minimal` reads every object whole in one covering GET, with no tail probe and no ranged read: the cost-preferring shape where transfer is free and the object-store bill is requests. `byte-minimal` uses ranged reads wherever they save more bytes than a request costs, for egress-billed or network-constrained deployments. `cost-based` derives the choice from `--store-cost-profile`, as the larger of its price term and its time term; at the shipped reference profile (intra-region, transfer free) the rate is the time term, 6,300,000 bytes per request, so **a deployment on default flags, loopback `--s3-endpoint` or not, reads a log object whole unless the bytes a narrow projection skips exceed the 18,900,000-byte projection break-even**, which no object of that size or less can. `latency-first` resolves the same byte quantities as `byte-minimal`; it is an intent for deployments where cold wall-clock matters more than the request bill, and it pays off only once GET concurrency is also raised explicitly (it sets no concurrency default of its own), which carries a memory caveat -- see the operations guide before turning it on. An explicit flag always wins, including on a loopback endpoint. Read once at startup; the running process never changes its own policy. The resolved policy, its source (`flag` or `default`), the profile, and the byte quantities are logged at startup on the `logs fetch policy resolved` line. |
| `--store-cost-profile <path>` | reference profile (`s3-intra-region-2026`) | TOML file carrying this deployment's object-store prices in integer nanodollars: `name`, `put_class_nanodollars`, `get_class_nanodollars`, `transfer_nanodollars_per_gib`, `retrieval_nanodollars_per_gib`, and optionally `delete_class_nanodollars` and the measured request timings `request_latency_micros` and `per_connection_throughput_bytes_per_s` (both or neither), with `timings_measured` naming where they were measured. Only `--logs-fetch-policy cost-based` reads it, to derive how many transferred bytes one saved request is worth; no price reaches the fetch layer. An unreadable file, invalid TOML, an unknown key, one timing without the other, or a blank `name` fails startup rather than falling back to the reference prices. |
| `--logs-max-fetch-run-bytes <bytes>` | `67108864` (64 MiB) | The fetch bound: the maximum length of one covering GET on the log path, on every policy. An object at or under it is read in a single request; a larger one is read as sequential block-aligned covering sub-ranges of at most this many bytes each, so one oversized object cannot pull an unbounded response into memory. `0` is refused at startup. |

Both `--cache-max-bytes` and `--catalog-cache-max-bytes` are **caps, not
reservations**. Neither cache pre-allocates its ceiling. Each holds only the
bytes that it admitted and, once it reaches its cap, evicts with S3-FIFO, not
LRU: a new entry waits in a small probation queue, and only an entry that is
read again, or one that leaves probation while the main queue still has
room, joins the main queue. A one-pass scan, such as a compaction or a fold,
therefore passes through probation without evicting a working set that
queries keep reading. The ceiling is an upper bound on resident cache bytes,
not memory claimed at startup.

A repeated scan larger than the cache, such as the same query run again over
more data than the cache holds, is served in part rather than not at all. The
first run fills the cache, and the entries it admitted stay resident: later
runs of the same scan read those from the cache and the rest from storage.
For equal-sized objects twice the cache's size, at least 40% of the second
and third runs' reads come from the cache, from the same entries in both. A
working set that queries read again sooner than the scan comes back around
still takes residency from entries that are read less often.

The **sum of every ceiling can exceed physical RAM**. The two caches are
independent, and the SQL memory pools (`--sql-max-query-bytes`,
`--sql-tenant-max-bytes`) are bounded separately again. On the reference host,
the following sum is more than 100% of raw host memory:

- the fetcher cache (25% of the process memory budget by default, or 40%
  against a loopback store)
- the catalog byte cache (5%)
- the SQL pools

The peaks do not coincide the way the arithmetic sum suggests. The caches only
fill under a working set that touches that many distinct bytes, and the SQL
pools abort a query and do not grow past their own ceiling. Size a host
against its real working set, not against the sum of the caps.

On a memory-constrained host:

- To bound either cache, set an explicit `--cache-max-bytes`,
  `--catalog-cache-max-bytes`, or both.
- To hold no read-cache memory at all and leave the entire process memory
  budget to the query path, set `--disable-cache`. The flag does not reach
  [the catalog record caches](#the-catalog-record-caches), which are not read
  caches of object bytes and cost about 18 MB per actively-queried tenant
  under it.

### The max-age sweep

Both tiers bound how long the raw bytes of an entry can persist. Bytes of an
erased subject therefore cannot outlive the erasure pass on a query node by
more than a fixed window, in RAM as well as on local disk. Two knobs govern
it:

| Knob | Default | Meaning |
|---|---|---|
| `max_entry_age_ns` | `82800000000000` (23 h) | Maximum wall-clock age an entry is served at. A hit on an older entry is treated as a miss and its bytes dropped. |
| `sweep_interval_ns` | `3600000000000` (1 h) | Period of the background sweep that drops over-age idle entries nothing re-reads, so an entry that is never touched still ages out on its own. |

The worst-case residue of an idle entry is `max_entry_age_ns +
sweep_interval_ns`. At the defaults that sum is 24 h. The max-age is one sweep
interval below the bound, so the default meets the bound and does not
overshoot it by the sweep period.

Neither knob is a command-line flag. Both tiers are constructed with the
shipped defaults, and you cannot override the max-age sweep from the command
line.

## Startup warmup

When the cache is on, `ravel-server` warms it before it reports ready
(`/readyz`). For each tenant that storage holds data for, it reads a small,
bounded number of the most recent metric and log segments of that tenant. The
first real query after a restart then does not pay the full cold cost.

"Most recent" is measured from the latest ingest hour of each tenant, not from
wall-clock time. A tenant that has not ingested in the last day still gets its
most recent parts warmed, not zero.

Warmup is best-effort:

- It has an overall time budget. If storage is slow or a tenant list is large,
  warmup stops early and the process still becomes ready. It starts with a
  smaller warm set.
- A failure to warm one tenant or one segment is logged and skipped. It never
  fails startup.

### Warmup log lines

To tell a warmed restart from an unwarmed one, or to see what was warmed, read
the log lines of the `cache_warm` module. Do not guess from query latency.

`cache_warm` alone decides the level of the per-(tenant, signal) line.
`Catalog::latest_ingest_hour` never logs above `debug!`, so a signal that a
tenant does not use never produces a `warn!` by itself.

The per-(tenant, signal) line carries `tenant_hash`, `signal`, `parts_warmed`,
and `latest_ingest_hour`. Its level is:

- `info!` normally.
- `warn!` if the tenant has a live part for that signal but none of them
  warmed, which means that a resolve or fetch failure lost the whole signal.

`latest_ingest_hour` takes one of three values:

- **An hour number.** The latest ingest hour of the tenant was found. It is
  the newest hour bucket that holds a commit-record-shaped key of any kind
  (commit, compaction, rewrite, or tombstone), not only a live commit record.
  - `parts_warmed` counts what warmed from it. It is 0 when the only key of
    that hour was a tombstone. That case still logs at `info!`, because a
    tombstone-only hour has no live part that failed to warm.
  - An hour that holds both a live record and a tombstone counts as having a
    live part, while the resolve drops the tombstoned bucket. When nothing
    else in the window warms, the `warn!` can fire for it. Read that warning
    as a hint to look at the resolve, not as a fault.
- **`"none"`.** `Catalog::latest_ingest_hour` returned `Ok(None)`. The tenant
  has no discoverable ingest for that signal: it never ingested the signal, or
  its last ingest predates the discovery lookback cap (see below).
  `parts_warmed` is always 0 for this value. The `cache_warm` pass logs it at
  `info!`, not `warn!`, because no parts to warm is a normal outcome, not a
  failure.
- **`"error"`.** The `Catalog::latest_ingest_hour` listing itself failed: an
  object store fault, or the per-shard LIST budget refused.
  - This value appears on a separate `warn!` event, `cache warmup:
    latest_ingest_hour failed; skipping this tenant and signal`. The event
    carries `tenant_hash`, `signal`, `error` and `latest_ingest_hour` and no
    `parts_warmed`.
  - The pass emits no result line for that tenant and signal, so a filter on
    `parts_warmed` alone will not show it.
  - It is distinct from the `"none"` case, so a store fault is never mistaken
    for a tenant that has no data.

When `Catalog::latest_ingest_hour` exhausts its lookback cap (64 days) and
finds nothing, it logs at `debug!`, not `warn!`. It cannot tell a tenant that
never used a signal apart from one whose ingest is stuck past the cap. Both
produce the identical `Ok(None)`, and neither case alone is a failure worth a
warning.

Do not expect a `warn!` from this path on a normal warmup pass. A
tenant/signal combination can produce only two `warn!` lines: the `"error"`
case, and the "has parts but none warmed" line of `cache_warm`.

One final `info!` at the end of the pass carries the total `parts_warmed`
across every tenant and signal and the `elapsed_ms` of the pass.

- A restart whose log shows this final line warmed the cache.
- A restart that hit the time budget never reaches it. It logs the `warn!`
  that names the deadline instead. The per-(tenant, signal) lines up to that
  point are whatever the pass warmed before it ran out of time.

## Metrics

When a cache is on, `GET /metrics` reports these counters, labeled by `mode`
and by `cache`:

- The `cache` label names which of the two caches the sample belongs to:
  `cache="fetch"` for the fetcher cache, `cache="catalog"` for the catalog
  byte cache.
- When a disk tier is configured (`--cache-dir`), the samples of each cache
  also carry a `tier="ram"`/`tier="disk"` label, so the hit rates of the two
  tiers are reported separately. With a RAM tier only, no `tier=` label
  appears.
- Each cache renders its own series, so you can compute the hit-rate formulas
  below per cache or summed across both.

The counters are:

- `ravel_cache_hits_total` / `ravel_cache_misses_total`: read outcomes.
- `ravel_cache_bytes_served_total`: bytes returned from the cache on a
  hit.
- `ravel_cache_bytes_admitted_total`: bytes written into the cache.
- `ravel_cache_evictions_total`: entries evicted to make room.
- `ravel_cache_disk_errors_degraded_to_misses_total`: a disk-tier read
  found a corrupt or unreadable entry and treated it as a miss instead of
  failing the query. This is distinct from a normal miss, where nothing was
  there. This counter means that something was there and could not be trusted.

  It is nonzero only when a disk tier is configured (`--cache-dir`) and that
  tier is unhealthy. A `tier="disk"` sample above zero means that the disk
  tier found entries that it could not trust, not only that it was cold. A
  process with no `--cache-dir` never emits it.
- `ravel_cache_disk_entries_expired_max_age_total`: disk-tier entries
  dropped because they aged past the per-entry max-age. It counts every drop
  point: a hit that found an over-age entry, the startup scan, and the
  periodic background sweep that reaches idle entries nothing re-reads. This
  is distinct from an eviction, which makes room under the byte or entry
  bound. This is a time bound, not a capacity bound.

  It is nonzero only when a disk tier is configured (`--cache-dir`). A process
  with no disk tier never emits it.

The RAM tier applies the same max-age but has no counter for it. An over-age
RAM entry is reported as an ordinary miss. A workload whose entries routinely
age out therefore shows a hit rate lower than its access pattern suggests, and
nothing else.

With both caches off (`--disable-cache`), none of these samples appear on
`/metrics` at all: neither `cache="fetch"` nor `cache="catalog"`. A fetcher
cache whose ceiling is `0` is not built either, so `cache="fetch"` is absent
in that case too.

Request hit rate is `hits / (hits + misses)`. Byte hit rate is
`bytes_served / (bytes_served + bytes_admitted)`. Filter by the `cache` label
for the rate of one cache, or omit it (let PromQL sum the series) for the
whole process.

### Late serves

Two kinds of read are late serves:

- A read that waited on the in-flight GET of another read (a single-flight
  follower).
- A read whose peek missed while a fetch for the same range was in flight, and
  that reached the single flight only after that fetch finished. It is served
  from the RAM tier.

The second kind records nothing beyond the misses of its peek: a RAM miss,
plus a disk miss when a disk tier is configured. It records no hit, no
`bytes_served` and no `bytes_admitted`. When two such reads arrive together,
one leads the RAM recheck, and the other follows it and records a
single-flight collapse. This holds with a RAM tier only as well.

A Parquet read counts a late serve in its query accounting as one cache miss
with zero GETs and zero fetched bytes. So do the four read-through paths of
the query fetchers:

- a metrics (RSEG) read, ranged or whole-object (only a suffix read bypasses
  the cache)
- a log whole-object read
- a log extent read: the block-range probe, directory and page-range reads,
  and the covering read of an oversized object
- a span whole-object read

On those four paths:

- A single-flight follower is a late serve with or without `--cache-dir`.
- A read that the RAM recheck serves is a late serve only with a RAM tier
  alone. With `--cache-dir` those paths have no RAM recheck, so a read that
  arrives after the flight ends leads its own flight and counts the disk hit.
- The `page_fetch` and `segment_open` spans record no request for a late
  serve. A page-range read counts it in neither `block_range_gets` nor
  `block_cache_hits`.
- Only the read that ran the GET is charged it, and only a lookup that found
  the bytes is a hit.
- The cache hit and miss counts of the query
  (`ravel_query_cache_hits_total`) do not depend on which caller fetched. A
  late serve is a miss on these paths and is charged no GET.

A log block-range read looks a second time at the blocks of a coalesced run.
That second look records no hit or miss on either tier. A block that it finds
on the disk tier is still re-admitted to RAM, so `bytes_admitted` moves.

## Sizing for logs column-filtering waste

The `QueryAccounting` of a logs (RLOG) query carries two decode-time byte
counters next to its wire-byte counters:

- `page_bytes_fetched`: the stored bytes of every page present in the blocks
  that the query decoded.
- `page_bytes_decoded`: the stored bytes of only the pages that the column
  projection of the query kept.

These are a decode-time measurement, not a wire measurement. They count bytes
that a fetched block already holds. `s3_bytes` counts the bytes moved over the
network.

- On the whole-object read shape, the whole block arrives on the wire, and a
  narrow projection skips decompressing the pages that it does not need.
- On the ranged read shape, the page directory lets the fetch pull only the
  pages of the projected columns, so the projection narrows the wire bytes as
  well.

Read the ratio `page_bytes_decoded / page_bytes_fetched` as follows:

- **Close to 1.** The query decodes nearly everything that its blocks contain.
  A larger cache working set is the main way to make repeat runs cheaper.
- **Small.** Column filtering throws away most fetched page bytes: the
  wide-schema, narrow-projection shape. A whole-object read fetches and caches
  the block in full to serve a few columns. Two responses apply:
  - Narrow the projection further where the query allows it.
  - Size the cache to the *working set of whole blocks* that the workload
    touches, not to the decoded byte volume. On the whole-object shape the
    cache admits and holds whole blocks, regardless of how little of each a
    given query decodes.

A small decoded fraction across the queries of a tenant is the signal to size
its cache against block footprint, not against what its projections read.

The ratio is workload-dependent. It is set by how wide the log schema of the
tenant is against how narrow the projections of its queries are, so there is
no representative figure to quote. Measure your own.

Neither counter reaches the query-facing accounting of a running server.
`EXPLAIN ANALYZE` surfaces the sibling page-count fields,
`pages_decoded`/`pages_skipped`, not their byte-denominated equivalents. The
ratio is therefore available only to a caller that reads `QueryAccounting`
in-process, which is what the `ravel-bench` logs scan does.

## The catalog record caches

The sections above describe read caches: caches of object bytes whose ceiling
is a share of the process memory budget. The catalog also keeps two per-tenant
caches of decoded commit and compaction records. Neither takes a share of that
budget. Their memory comes out of what is left after the carved shares, never
out of them. Budget for them separately.

One capacity bounds both caches independently. The capacity is derived per
deployment from the shard count, the signal count and the configured max flush
delay, not from a flat constant:

```text
shards * 6 signals * ceil(3600 / max_flush_delay_seconds) * 3 unsealed hours
```

The result is floored at 10,000 entries and capped at 25,000. Each cache is
ALSO held to a byte budget of `capacity x 900 bytes`, 22.5 MB per tenant at
the cap, and evicts against whichever bound binds first. The byte budget
exists because neither record type has a bounded size:

- The **commit-record cache** is bounded in bytes at
  `commit_cache_max_bytes_per_tenant`. `CommitRecord.declared_column_stats` is
  a repeated field with no cap in the proto, in validation, or in the
  tenant-config path for typed attribute columns. A record that declares 200
  typed attribute columns charges about 20 KB where an ordinary one charges
  864 bytes, more than twenty times the planning rate. The cache charges each
  entry an estimate of the live heap that it holds. It evicts
  least-recently-used until the charged total is back inside the budget.
- The **compaction-record cache** is bounded in bytes at the same share,
  `compaction_cache_max_bytes_per_tenant`. A compaction record carries one
  `CompactionInputIdentity` per L0 segment that it merged. Neither the format
  nor validation caps that list. One L1 record over 1,800 L0 segments charges
  about 137 KB, roughly 150 times the planning rate. An entry count alone
  lets a single tenant hold hundreds of times the figure below. The cache
  evicts oldest-first until its charged bytes are back inside the budget.

The 900 bytes is a PLANNING rate the capacity is derived against, not a
per-entry cap: an ordinary stats-free commit entry charges 864 bytes (a
119-byte commit key held twice, the decoded struct, and the record's own heap),
and what each cache enforces is the summed charge against its budget. So the
capacity is an entry cap rather than a guaranteed residency: a tenant whose
records carry typed attribute column statistics holds proportionally fewer than
`capacity` of them, and the memory stays inside the figure either way.

### Budgeting the record caches

So the worst case is 45 MB per actively-queried tenant, both halves enforced in
bytes by their own eviction path rather than projected from a per-entry
estimate. That is what the cap holds constant across every deployment shape
(`--shards 64` derives 2,073,600 entries and 3.7 GB per tenant uncapped).
Budget it as 45 MB times the number of tenants queried concurrently: 100 of
them is 4.5 GB worst case,
and idle tenants are reclaimed by idle-tenant eviction. At the shipped
2-second cadence the cap decides the value for every shard count, so
`--shards` does not move it there.

The capacity covers the unsealed tail of a tenant up to the cap, not the whole
tail. Three things put a real tail past it:

- The cap itself. The estimate at the shipped defaults is already 129,600
  entries.
- The flush cadence term. It counts the age trigger only, while a shard also
  flushes as soon as its estimated object bytes reach `target_bytes` (8 MiB by
  default).
- The three unsealed hours, which assume the default seal parameters. An
  operator can move this term. `--gc-max-flush-lifetime 4h` puts the seal
  margin at 4h20m, so the oldest unsealed hour can have started 5h20m ago and
  the tail spans up to 5.34 hours. Three under-counts that by about 1.8x.

A tenant over the bound pays a per-record GET on every resolve. That is a
change: the old cache held a flat 10,000 records whatever they cost. A tenant
whose records carry typed attribute column statistics now holds fewer records
than the entry capacity, and pays GETs for the records that it cannot hold.
The bound trades that hit rate for a memory figure that the eviction path
enforces. The levers are a coarser
`--max-flush-delay`, a shorter `--gc-max-flush-lifetime`, or a lower shard
count. All of them shrink the tail itself.

`--disable-cache` does not turn these caches off, because a resolve with no
record cache re-reads every record from the store. It does hold the capacity
at the 10,000-entry floor rather than the derived value, so the flag costs
about 18 MB per actively-queried tenant: a 9 MB byte budget for each of the
two caches, both enforced.

Neither cache is exported. The resident-bytes gauge covers the object-byte
read caches only: `ravel_cache_resident_bytes` is emitted for the tiers of the
fetch family, and `ravel_cache_max_bytes` for the fetch and catalog byte
ceilings. No scrape-time figure shows how many bytes the record caches of a
tenant hold. The numbers above are bounds to budget against. You cannot read
them back off a running server.

## Background

The read cache is [ADR-0046](../adrs/0046-read-cache-tier.md); the max-age
bound on cached bytes comes from the erasure guarantee in
[ADR-0064](../adrs/0064-selective-subject-erasure.md); the logs block-range
read shape is
[ADR-0107](../adrs/0107-pruning-proportional-logs-fetch.md), and the fetch
policy above it is
[ADR-0996](../adrs/0996-request-cost-aware-fetching.md).
