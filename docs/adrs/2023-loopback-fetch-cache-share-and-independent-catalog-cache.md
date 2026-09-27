# ADR-2023: Whole-object fetching on a loopback store again, a larger fetch-cache share there, and a catalog cache sized on its own

- Status: Proposed
- Date: 2026-09-26
- Refs: #2023, #2014, #1170, #1463, #1506, ADR-1170, ADR-2014

## Context

ADR-2014 made `byte-minimal` the default fetch policy when the S3 endpoint is
loopback. On the ClickBench reference machine (c6a.4xlarge, RustFS 1.0.0 on a
500 GB gp2 volume) it cut the single-query sums, which is what it measured.
End to end on a fresh machine, cold fell from 1,741.9 s to 1,197.6 s and hot
from 274.5 s to 101.7 s. It did not measure concurrency. The ClickBench driver
also runs ten connections against one server for 600 s, and there the default
cut throughput from 0.50 to 0.12 queries per second, with the error ratio up
from 0.05 to 0.14.

On one instance, the concurrent phase alone, with one variable changed per arm
(#2014):

| arm | concurrent QPS | error ratio |
|---|---|---|
| `cost-based`, derived 7.69 GB fetch cache | 0.400 | 0.101 |
| `byte-minimal`, derived 7.69 GB fetch cache | 0.123 | 0.245 |
| `byte-minimal`, `--cache-max-bytes 12000000000` | 0.320 | 0.307 |

The ranged plan issues about 5.5 fixed GETs per object (the footer suffix,
directory sections and block stats), whatever `--logs-request-cost-bytes` is
set to. It caches column blocks rather than objects, and the store serves each
small ranged GET with block-granular reads of the same disk. Under ten
concurrent statements it moved twice the disk bytes of `cost-based` for a
third of the throughput.

The third arm looked like the fix and was not a single-variable change. It
ran directly after the `cost-based` arm on the same instance without dropping
the page cache, so it started with much of the store's data in memory. A
fetch-cache share sized to hold the corpus (40% of `memory_budget_bytes`,
12.3 GB on that machine) was then measured properly: the stock entry, built
from source, run end to end on a fresh bot-style machine (#2023 comment
5848140511).

| figure | v0.18.0 stock | `byte-minimal` + 40% share | bar set before the run |
|---|---|---|---|
| concurrent QPS | 0.123 | 0.170 | at or above 0.40 |
| concurrent error ratio | 0.140 | 0.089 | at or below 0.058 |
| cold / hot | 1,197.6 s / 101.7 s | 1,211.0 s / 95.5 s | within 10% |

A larger cache recovers part of the gap, not most of it. Under concurrency on
a single local disk, the ranged plan's extra requests cost more than the bytes
it saves.

Separately, `--cache-max-bytes` sizes both the fetch cache and the catalog
byte cache at the one value. The 12 GB arm above therefore also committed
12 GB to a catalog cache that served 0 hits in every window measured, and cut
the query remainder to 6.76 GB.

## Decision

1. **A loopback store is back on whole-object fetching by default.** With
   `--logs-fetch-policy` unset, every deployment resolves `cost-based`, as
   ADR-1196 decided. This supersedes ADR-2014's decision. An explicit
   `--logs-fetch-policy byte-minimal` still gives the ranged plan, where its
   single-query gain is the goal, for example a tuned benchmark entry.
2. **`--cache-max-bytes` bounds the fetch cache only.** The catalog byte cache
   derives at its own share (`CATALOG_CACHE_MEMORY_PERCENT`, 5%) whether or not
   `--cache-max-bytes` is set, and `--catalog-cache-max-bytes` sets it
   explicitly. Startup still refuses a combination whose two caps together
   exceed `memory_budget_bytes`, as ADR-1170 decision 3 requires.
3. **A loopback store derives a larger fetch-cache share.** When
   `--cache-max-bytes` is unset, `--store` is `s3` and the endpoint is loopback
   (the predicate `Cli::store_is_loopback`), the fetch cache takes
   `LOOPBACK_CACHE_MEMORY_PERCENT` (40%) of `memory_budget_bytes`: 12.3 GB on
   the ClickBench c6a.4xlarge (budget 30,756,311,040 bytes), above its
   11.24 GB corpus, and 12.03 GB on the repository's 30 GiB reference host.
   Every other deployment keeps 25%. The resolved line names the source
   `budget-carve-loopback`. With whole-object reads a cache that holds the
   corpus serves repeated statements without touching the store's disk.
4. **The combination is measured before it ships.** A fresh bot-style
   end-to-end run of the stock entry, built from the change, must reach:
   - concurrent throughput at or above 0.40 QPS;
   - a concurrent error ratio at or below 0.058, the ClickBench bot's own
     v0.17.0 run on the same machine type;
   - a cold sum within 10% of v0.17.0's 1,741.9 s, which the cache cannot
     change because the driver restarts the server before each cold try;
   - a hot sum at or below v0.17.0's 274.5 s.

## Rejected alternatives

**Keep `byte-minimal` on loopback with the larger share.** Measured above:
0.170 QPS against a 0.40 bar. The single-query gain stays available through
the explicit flag.

**Raise the fetch share everywhere.** #1170's cliff was measured against
remote S3, and nothing here re-measures it there. A remote deployment's miss
is a network GET and does not compete with the store for the same disk.

**Keep the cache coupling and document `--cache-max-bytes` for local
stores.** The coupling is what drove the error ratio up in the one run that
tried a larger cache, and the stock ClickBench entry passes no flags.

## Consequences

- A single-host deployment on a loopback store reads whole objects again: its
  single-query cold time returns to the pre-ADR-2014 figure, and its
  concurrent throughput with it. The ranged plan is an explicit opt-in.
- The larger loopback share is expected to cut hot times, since whole objects
  held in a corpus-sized cache serve every statement that touches them.
  Decision 4 is what checks that.
- The shared SQL/fetch remainder on a loopback store shrinks by the extra
  share: 15 points of the budget, about 4.6 GB on the ClickBench c6a.4xlarge.
  The error-ratio bound in decision 4 is what guards it.
- With `--cache-dir` set, the fetcher cache's disk tier is bounded by the same
  resolved ceiling as its RAM tier, so on a loopback store the disk tier also
  grows from 25% to 40% of the budget, on the same disk the store reads from.
- `--cache-max-bytes` no longer resizes the catalog cache. A deployment that
  relied on the flag to grow both sets `--catalog-cache-max-bytes` as well.
