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
   The rate `cost-based` resolves on the intra-region profile and the
   break-even a narrow projection is routed by are changed by the ADR-2414
   amendment below. On a loopback S3 endpoint the unset default is
   `latency-first` since the issue #2683 amendment below.
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
   ADR-1170's available-memory amendment (issue #2367) derives a smaller
   budget on a host with co-resident processes. On the ClickBench c6a.4xlarge
   that puts this share at about 11.11 GB, below the 11.24 GB corpus cited
   here and above the 9.84 GB corpus v0.21.0 loads. The 12.03 GB figure for
   the 30 GiB reference host stands only where that host has no co-resident
   process. Otherwise it follows that host's `MemAvailable` reading, which
   this ADR does not record. The share is 24%, below the remote 25%, since
   the issue #2683 amendment below; the figures in this decision are the 40%
   ones.
4. **The combination is measured before it ships.** A fresh bot-style
   end-to-end run of the stock entry, built from the change, must reach:
   - concurrent throughput at or above 0.40 QPS;
   - a concurrent error ratio at or below 0.058, the ClickBench bot's own
     v0.17.0 run on the same machine type;
   - a cold sum at or below 1,916.1 s, 10% above v0.17.0's 1,741.9 s, which
     the cache cannot change because the driver restarts the server before
     each cold try;
   - a hot sum at or below v0.17.0's 274.5 s.

   The error-ratio bar was missed by one query and the miss was accepted;
   see Acceptance.

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
  The issue #2683 amendment below sets the loopback share to 24%, so neither
  the smaller remainder nor the larger disk tier holds there any more.
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
window described below and is not read. The share is credited on hot time:
the same-build candidate cut the hot sum 67% against the control, past the
noise floor, and the rerun repeated it. The rerun's QPS was 33% above the
control's, but the rerun also carries the first version of #2035, so that
comparison crosses builds; it is consistent with the share helping
throughput but does not isolate it.

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

## Amendment (2026-10-03, Refs: #2414): a time term in the cost-based rate and a projection break-even above the routing threshold

<!-- amendment-applies: sections="Decision" pointer="ADR-2414 amendment" -->

Decision 1 keeps `cost-based` as every deployment's default. ADR-2414
decision A3 changes what that policy resolves to: the store cost profile
carries a request latency and a per-connection throughput beside its
prices, the request cost in bytes is the larger of the price-derived rate
and `request_latency * per_connection_throughput`, and on the intra-region
profile, whose prices are zero, the rate is therefore finite instead of
`u64::MAX`. The routing threshold keeps its configured value, and the
projection break-even (`ranged_projection_pays`) takes the larger of that
threshold and five request costs (three since the issue #2555 amendment
below), so a 3 MB L0 object still reads whole and
a 35 MB compacted object at a narrow projection reads ranged. The loopback
measurement in decision 4 was reasoned about, not re-run, and the result is
this. Unless `--store-cost-profile` names another profile, a loopback
deployment resolves the reference profile and its 31,500,000-byte break-even
(18,900,000 bytes since the issue #2555 amendment below). Decision 4 states no object
size; ADR-2414 puts the L0 objects of that layout at about 3 MB each, so on
that figure each object decision 4 measured sits under the break-even on its
own (whether an object reads whole depends on its own size, not on the
corpus average). So on the reference profile those objects' blocks
are still read in one whole-object GET on the whole-segment fast path and on
the planned route alike. On the planned route (any statement the
whole-segment fast path refuses) an object at or below the break-even is read
whole with no tail probe, as decision 4 measured: the plan phase reads it in
one covering GET and hands those bytes to the scan, although the routing
threshold is back at its 512 KiB default. Above the break-even a statement on
the planned route probes the object's tail and directories, and a wide
projection whose surviving ranges cover at least 75% of the object then reads
it whole anyway (the coverage crossover), because the planned route does not
weigh the projected fraction. That probe is a request the saturated
threshold never issued; whether it moves decision 4's concurrent throughput on
a corpus with objects that large is unmeasured.

## Amendment (2026-10-04, Refs: #2555): the projection break-even is three request costs

<!-- amendment-applies: sections="Amendment (2026-10-03, Refs: #2414): a time term in the cost-based rate and a projection break-even above the routing threshold" pointer="issue #2555 amendment" -->

This is the issue #2555 amendment. ADR-2414's three-request-cost amendment
lowers the cost-based projection break-even from five request costs to
three, the larger of the routing threshold and 3 * request cost: 18,900,000
bytes on the reference profile instead of 31,500,000. The ADR-2414
amendment above now carries a pointer here at both figures.

Its conclusion for decision 4 holds. A 3 MB L0 object is under one request
cost, 6,300,000 bytes, so it is under the break-even at either figure, and on
the reference profile its blocks are still read in one whole-object GET on
the whole-segment fast path and on the planned route, with no tail probe on
the planned route. What moves is the band between 18,900,000 and 31,500,000
bytes: an object there that a narrow projection skips more than 18,900,000
bytes of reads ranged on the fast path, and the planned route probes every
object there before the coverage crossover decides.

## Amendment (2026-10-12, Refs: #2683): latency-first on a loopback store and a 24% fetch-cache share

<!-- amendment-applies: sections="Decision|Consequences" pointer="issue #2683 amendment" -->

This is the issue #2683 amendment, ADR-2677 decision 7 as re-scoped there.

Decision 1 changes for a loopback store only. With `--logs-fetch-policy`
unset, a `--store s3` deployment whose `--s3-endpoint` is loopback resolves
`latency-first`, and the startup line names the source `default-loopback`.
Every other deployment keeps `cost-based`, and an explicit flag wins on
loopback as anywhere else. `latency-first` runs at the core-derived
`--store-get-concurrency`, which this amendment does not change.

Decision 3's share drops from 40% to 24% (`LOOPBACK_CACHE_MEMORY_PERCENT`).
The source stays `budget-carve-loopback`; the loopback share is now one point
below the remote 25%, so the remainder the SQL pools and the PromQL fetches
share is one point larger there than on a remote store, not 15 points
smaller, and with `--cache-dir` set the disk tier is bounded at 24% too.

Both rest on ClickBench over a RustFS loopback endpoint on four c6a.4xlarge
boxes, one run per arm (issue #2044 comment 6115248576):

| arm | concurrent error ratio | concurrent QPS | cold sum | hot sum |
|---|---|---|---|---|
| stock, 40% share, `cost-based` | 0.099 | 0.574 | 478.1 s | 59.90 s |
| 7 GB cache, `cost-based` | 0.029 | 0.512 | 478.1 s | 59.92 s |
| 40% share, `latency-first` | 0.093 | 0.703 | 368.6 s | 60.03 s |
| 7 GB cache, `latency-first` | 0.040 | 0.678 | 370.2 s | 60.61 s |

A replicate of the stock arm read 0.092 and 0.612 QPS (issue #2720 comment
6115316539). The 7 GB cache is 23.8% of those boxes' roughly 29.4 GB budget,
hence 24%. The smaller cache cut the error ratio from 0.099 to 0.029 at the
same hot sum, so the hot-time gain decision 3 was credited with does not need
the 40% share on this corpus; 60% of the concurrent refusals on stock main
are the process budget filling (issue #2044 comment 6114049172), which is
what a smaller carve relieves. `latency-first` cut the cold sum by 23% and
raised QPS on both cache sizes. Decision 4's error-ratio bar, 0.058, is met
by the carve alone (0.029); the combined arm read 0.040, also under it.

The confirmation on a 16 GB box passed (#2683 comment 6116274802, requested
in comment 6115323679). That run set both by flag on the code before this
amendment: a 3.1 GB cache, about 24% of its 13.2 GB budget, and
`latency-first`. Against stock it read a pooled error ratio of 0.259
against 0.282, 0.313 QPS against 0.310, and a cold sum of 357.0 s against
453.1 s, and it refused no statement that stock answers. Its minimum
`MemAvailable` was 0.11 GB, and it spent several times stock's time below
the host memory gate's floor, so the margin at 16 GB is thinner than at
32 GB.

Decision 4 was not re-run for this amendment, and no stock run of the
shipped defaults exists yet. Its control-arm rule credits a share raised
above the remote 25%, on hot sum or QPS. This amendment lowers the share to
one point under 25%, and on those two metrics the carve alone tied on hot
sum (59.92 s against 59.90 s) and cost 11% of QPS (0.512 against 0.574): its
gain is the error ratio, which decision 4 does not credit. The next stock
release measurement is the run that checks the shipped defaults against
decision 4's four bars.

