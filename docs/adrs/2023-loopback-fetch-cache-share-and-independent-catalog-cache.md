# ADR-2023: Whole-object fetching on a loopback store again, a larger fetch-cache share there, and a catalog cache sized on its own

- Status: Accepted (decision 4's error-ratio bar missed by one query; see Acceptance)
- Date: 2026-09-26
- Refs: #2023, #2014, #2035, #1170, #1463, #1506, ADR-0088, ADR-0996, ADR-1170, ADR-1196, ADR-2014

## Context

ADR-2014 made `byte-minimal` the default fetch policy when the S3 endpoint is
loopback. On the ClickBench reference machine (c6a.4xlarge, RustFS 1.0.0 on a
500 GB gp2 volume) it cut the single-query sums, which is what it measured.
End to end on a fresh machine, cold fell from 1,741.9 s to 1,197.6 s and hot
from 274.5 s to 101.7 s. It did not measure concurrency. The ClickBench driver
also runs ten connections against one server for 600 s, and there the default
cut throughput from 0.50 to 0.12 queries per second, with the error ratio up
from 0.05 to 0.14.

On one long-lived instance, the concurrent phase alone, with one variable
changed per arm (#2014):

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
third of the throughput. `iostat` on the fresh machine read 3,220 reads per
second at a 50 KB average and 87% utilisation, against the gp2 volume's
3,000 IOPS burst: the ranged plan is bound by request count, which a larger
cache or query pool cannot buy back.

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

The two 0.123 figures come from different runs, the long-lived instance above
and this fresh machine; they agree on throughput and differ on error ratio
(0.245 against 0.140). A larger cache recovers part of the gap, not most of
it. Under concurrency on a single local disk, the ranged plan's extra requests
cost more than the bytes it saves.

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
   reach or exceed `memory_budget_bytes`, as ADR-1170 decision 3 requires.
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
   - a cold sum at or below 1,916.1 s, 10% above v0.17.0's 1,741.9 s, which
     the cache cannot change because the driver restarts the server before
     each cold try;
   - a hot sum at or below v0.17.0's 274.5 s.

   That run measures decisions 1 and 3 together, so a pass alone cannot credit
   the share. A control arm runs the same build on a second fresh machine with
   `--cache-max-bytes` pinned at the 25% share, so the share is the only
   variable between them. The share is credited only where the candidate beats
   the control by more than the 15% timing noise floor, on hot sum or QPS; a
   tie records decision 3 as unmeasured-neutral.

## Rejected alternatives

**Keep `byte-minimal` on loopback with the larger share.** Measured above:
0.170 QPS against a 0.40 bar. The single-query gain stays available through
the explicit flag.

**Keep `byte-minimal` and coalesce its structural reads (deferred, not
rejected).** One suffix read covering the footer, directory and block stats,
or those sections cached across statements, would bring the ranged plan's
per-object request count toward one, which is what the IOPS ceiling above
punishes. That is the engineering follow-up that could make the ranged plan
viable on loopback again, and it belongs with the tuned entry (#1506).

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
- The larger loopback share cuts hot times, since whole objects held in a
  corpus-sized cache serve every statement that touches them. The competing
  explanation, that the host page cache already holds the corpus for the store
  so a Ravel-side miss costs only a loopback round trip and a decode, predicted
  a tie; the acceptance run below measured a 67% cut against the control, so
  it does not hold.
- The shared SQL/fetch remainder on a loopback store shrinks by the extra
  share: 15 points of the budget, 4,613,446,656 bytes on the ClickBench
  c6a.4xlarge. Decision 4's error-ratio bound guards the SQL path only. Since
  PromQL fetches reserve from the same remainder (#1255) and fail with a 503
  when it is exhausted, a loopback metrics deployment with a wide PromQL fanout
  has 4.6 GB less headroom, and ClickBench, which is SQL only, cannot see that.
- With `--cache-dir` set, the fetcher cache's disk tier is bounded by the same
  resolved ceiling as its RAM tier, so on a loopback store the disk tier also
  grows from 25% to 40% of the budget, on the same disk the store reads from.
- `--cache-max-bytes` no longer resizes the catalog cache. A deployment that
  relied on the flag to grow both sets `--catalog-cache-max-bytes` as well.

## Acceptance

Decision 4 ran on fresh bot-style c6a.4xlarge machines, each building the
change from source with the stock entry. Stamps, bands and the reading rule
were registered before the runs (#2023 comments 5854876916 and
5855941650); the figures are on #2023 (comments 5855474385 and 5856580042).

| figure | candidate, 40% | control, 25% | candidate with audit retry, 40% | bar |
|---|---|---|---|---|
| cold sum | 1,711.8 s | 1,717.6 s | 1,712.2 s | at or below 1,916.1 s |
| hot sum | 89.9 s | 268.3 s | 89.2 s | at or below 274.5 s |
| concurrent QPS | 0.403 | 0.372 | 0.493 | at or above 0.40 |
| concurrent error ratio | 0.955 | 0.043 | 0.060 | at or below 0.058 |

Cold sums were within 0.4% of each other on every arm, as decision 4
expects. The first candidate's QPS of 0.403 was measured in the restart
window described below and is not read. In the rerun, the share beat the
control by 67% on hot sum and 33% on QPS, both past the noise floor, so the
share is credited on hot time and throughput.

The first candidate's error ratio was not the share. One object-store PUT
for an audit record timed out under the concurrent load. Both audit PUTs are
conditional creates, which the S3 client never retries, so the query waiting
on that record failed closed. That query was the driver's own `SELECT 1`
health probe, so the driver restarted a healthy server, and every attempt in
the restart window failed within milliseconds: about 5,100 failures against
about 242 successes. A 90 s reproduction on the same machine without the
event gave 55 successes and one failure. #2035 retries a transient audit PUT
before failing closed, and the third column is the candidate rerun with the
first version of that change. No retry fired in that run and the server was
not restarted, so it shows the timeout did not recur rather than that the
retry prevented it.

The rerun's concurrent throughput is back to the v0.17.0 bot's 0.50. The error
ratio missed the bar by 0.002. Every one of the 19 errors was a memory-budget
refusal (9 on the tenant limit, 7 on the per-query limit, 3 on a full pool
with spill off), and 18 would have read 0.057. That is the cost the
Consequences name: the SQL remainder is 4.6 GB smaller, and the control, with
21.5 GB of it, refused less (0.043). The bar itself came from one v0.17.0 bot
run whose run-to-run spread was never measured. The owner accepted the miss
and kept the 40% share; #2044 tracks bringing the refusals under the bar
without giving back the hot-time gain.