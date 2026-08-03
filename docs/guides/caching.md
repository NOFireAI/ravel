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
- **Log objects (RLOG).** A log query reads a whole RLOG object at once,
  so the cached unit is the whole object.

Both PromQL queries and SQL queries over the `samples` table use the
metric path. SQL queries over the `logs` table use the log path.

Two query surfaces do not use the cache today:

- **Spans (traces).** There is no SQL query surface over spans yet (see
  the main [README](../../README.md#whats-next)). When that surface
  lands, it will need its own cache wiring; the trace fetcher has none
  yet.
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
| `--cache-max-bytes <n>` | `268435456` (256 MiB) | Maximum bytes the RAM tier holds. Read once at startup; there is no live resize. |
| `--cache-dir <path>` | none | Reserved for the disk tier. Setting it fails startup today (see "Known gaps"). |
| `--disable-cache` | off | Turns the fetcher cache off. Query *results* are then byte-for-byte the same as a build with no cache code at all. Memory is not: the catalog keeps a byte cache of its own that this flag does not reach (#553). |

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

When the cache is on, `GET /metrics` reports these counters, labeled only
by `mode` (ADR-0044's label allowlist):

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

With the cache off, none of these samples appear on `/metrics` at all.

These counters describe the fetcher cache only. The catalog keeps a byte
cache of its own, which these samples do not cover and `--cache-max-bytes`
does not bound. Issue #553 tracks that gap.

## Known gaps

- **`--cache-dir` is not wired up.** The disk tier (`DiskCache`) exists
  as a crate, but the query fetchers only accept a RAM cache today. Until
  that attachment point is added, `--cache-dir` fails startup instead of
  silently doing nothing.
- **Spans have no cache path**, because spans have no query surface yet.
- **The `alerts` and `audit` SQL tables are not reachable in
  production**, so caching them is not meaningful yet.
