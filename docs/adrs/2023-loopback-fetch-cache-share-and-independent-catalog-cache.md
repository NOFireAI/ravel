# ADR-2023: A larger fetch-cache share on a loopback store, and a catalog cache sized on its own

- Status: Proposed
- Date: 2026-09-26
- Refs: #2023, #2014, #1170, #1463, ADR-1170, ADR-2014

## Context

ADR-2014 made `byte-minimal` the default fetch policy when the S3 endpoint is
loopback. On the ClickBench reference machine (c6a.4xlarge, RustFS 1.0.0 on a
500 GB gp2 volume) it cut the single-query sums, which is what it measured:
cold from 1,741.9 s to 1,197.6 s and hot from 274.5 s to 101.7 s. It did not
measure concurrency. The ClickBench driver also runs ten connections against
one server for 600 s, and there the default cut throughput from about 0.4 to
0.12 queries per second.

Attribution on #2014 (comments 5846300474 and 5846371014), on one fresh
instance with one variable changed per arm:

| arm | concurrent QPS | error ratio |
|---|---|---|
| `cost-based`, derived 7.69 GB fetch cache | 0.400 | 0.101 |
| `byte-minimal`, derived 7.69 GB fetch cache | 0.123 | 0.245 |
| `byte-minimal`, `--cache-max-bytes 12000000000` | 0.320 | 0.307 |

The ranged plan caches column blocks, so with ten statements sharing a fetch
cache smaller than the 11.24 GB corpus they keep missing. Each miss is a small
ranged GET, which the store serves from the same disk with block-granular
reads. In that window `byte-minimal` moved twice the disk bytes of
`cost-based` for less than a third of the throughput. With a fetch cache that
holds the corpus it reached 80% of `cost-based`'s throughput (71% of the gap). Raising the per-request coalescing
(`--logs-request-cost-bytes` at 8 and 32 MiB) did not change the request count
and is not the lever.

That run's error ratio rose because `--cache-max-bytes` bounds both caches at
the one value. It committed another 12 GB to the catalog byte cache, which
served 0 hits in every window measured, and cut the shared SQL/fetch remainder
to 6.76 GB, so more statements were refused on memory.

ADR-1170 decision 3 set the fetch cache at 25% of `memory_budget_bytes` after
#1170 measured a 12 GiB cache failing at a 0.997 error ratio under ten
connections. That measurement was against remote S3, with the flag coupling
both caches, where the non-cache working set peaked at 17.2 to 20.8 GB. The
loopback run above held its RSS at 19 to 24.5 GB with the same 12 GB fetch
cache.

## Decision

1. **`--cache-max-bytes` bounds the fetch cache only.** The catalog byte cache
   derives at its own share (`CATALOG_CACHE_MEMORY_PERCENT`, 5%) whether or not
   `--cache-max-bytes` is set. A new `--catalog-cache-max-bytes` sets it
   explicitly. Startup still refuses a combination whose two caps together
   exceed `memory_budget_bytes`, as ADR-1170 decision 3 requires.
2. **A loopback store derives a larger fetch-cache share.** When
   `--cache-max-bytes` is unset, `--store` is `s3` and the endpoint is
   loopback, the same predicate ADR-2014 uses, the fetch cache takes
   `LOOPBACK_CACHE_MEMORY_PERCENT` of `memory_budget_bytes`. Every other
   deployment keeps 25%. The resolved `cache_max_bytes` line names the source
   `budget-carve-loopback`, so the choice is visible.
3. **The share is measured, not assumed.** It starts at 40%: 12.3 GB on the
   reference host, above the 11.24 GB corpus. It ships only if a fresh
   bot-style end-to-end run of the stock entry meets all of these:
   - concurrent throughput at or above 0.40 QPS, against v0.17.0's
     end-to-end 0.46 to 0.50;
   - a concurrent error ratio no worse than 0.058, the ClickBench bot's own
     v0.17.0 run on the same machine type;
   - cold and hot sums within 10% of v0.18.0's 1,197.6 s and 101.7 s.

   If 40% misses the error bound, the next run moves the share down, not the
   bar. The figures are recorded in the constant's doc comment, as
   `CACHE_MEMORY_PERCENT` records its sweep.

## Rejected alternatives

**Revert the loopback default to `cost-based`.** Restores concurrency and
gives back the whole single-query gain. The attribution shows the ranged plan
is not what fails under load; its cache is too small.

**Raise the fetch share everywhere.** #1170's cliff was measured against
remote S3, and nothing here re-measures it there. A remote deployment's miss
is a network GET and does not compete with the store for the same disk.

**Keep the coupling and document `--cache-max-bytes` for local stores.** The
coupling is what drove the error ratio up in the one run that tried it, and
the stock entry passes no flags.

## Consequences

- A single-host deployment on a loopback store keeps the ranged plan's cold
  and hot gains and, if the measurement holds, recovers its concurrent
  throughput.
- The shared SQL/fetch remainder on a loopback store shrinks by the extra
  share: 15 points of the budget, about 4.6 GB on the reference host. The
  error-ratio bound in decision 3 is what guards it.
- `--cache-max-bytes` no longer resizes the catalog cache. A deployment that
  relied on the flag to grow both sets `--catalog-cache-max-bytes` as well.
