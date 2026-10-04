# ADR-2414: narrow projections read ranged on S3, and a lone statement gets the tenant's SQL share

Status: Proposed. Issue #2414.
Amends ADR-2023 (the cost-based fetch policy's rate and the projection
break-even); it is Accepted and carries an amendment section pointing here,
added together with this document. ADR-1170 (the derived per-query SQL
share) is still Proposed, so task B1 edits its share in place and cites this
ADR there. ADR-0954 (spill) is not amended: see decision B2. No persistent
format changes.

## Context

The full reference-suite run on real S3 (issue #1248, main `affb0260e`, the
16-vCPU 32 GB reference box, the compacted tenant of 219 RLOG v5 objects,
7,741,962,796 bytes, 99,997,497 rows, 104 declared typed columns) measured
the read path with per-run accounting, cold (server restart and page-cache
drop before each statement). Two findings, both reproduced with one variable
changed at a time (Stage 0 comments on issues #862 and #837):

1. A one-column statement, `SELECT COUNT(*) FROM logs WHERE "AdvEngineID"
   <> 0`, costs the same under every shipped configuration, for different
   reasons:

   | configuration | wall | scan GETs | wire bytes | decompressed bytes |
   |---|---|---|---|---|
   | default (`cost-based`, 32 partitions) | 7,201 ms | 240 | 7,741,962,796 | 184,108,825 |
   | `latency-first`, 256 store GETs in flight, 8 partitions | 8,150 ms | 918 | 194,213,453 | 220,576,798 |
   | `latency-first`, 256 in flight, 219 partitions (one per segment) | 992 ms | 918 | 194,213,453 | 220,576,798 |
   | `latency-first` via `--fetch-concurrency 256` (256 partitions) | 7,809 ms | 699 | 137,029,034 | 4,656,911,808 |

   - The default policy reads every object whole: the `s3-intra-region-2026`
     store cost profile prices transfer at zero, `resolve_cost_based_rate`
     returns `u64::MAX`, and `LogSegmentFetcher::ranged_projection_pays`
     fails at its first comparison for any projection. The statement is
     network-bound at 1.07 GB/s.
   - The ranged read moves 2.5% of the bytes and decompresses within 20% of
     the whole-object figure, but the whole-segment fast path walks a
     partition's segments one at a time, so its wall time is the number of
     round trips per partition (918 over 8 partitions is about 115 each)
     times the request latency (about 70 ms measured from the box), not the
     bytes. One partition per segment is the only configuration that lets
     the fetch concurrency do its work.
   - When the partition count exceeds the segment count, the fast path
     refuses (`FewerSegmentsThanPartitions`) and `owned_work` deals single
     blocks to partitions, about 56 per object; every open decodes the
     directory sections again (SKIP_IDX, PAGE_DIR and FIELD_DIR in
     `fetch_object_v4`, all four in `RlogReader::from_source`, PAGE_DIR a
     third time for the read gate's `max_block_uncompressed_len`). About
     12,200 opens times three PAGE_DIR decodes is the 4.66 GB. This is also
     why the tuned arm lost on the compacted layout in #1248 after winning on
     the 2,617-object L0 layout.

2. The statement `SELECT "WatchID", "ClientIP", COUNT(*) AS c,
   SUM("IsRefresh"), AVG("ResolutionWidth") FROM logs GROUP BY "WatchID",
   "ClientIP" ORDER BY c DESC LIMIT 10` (q33) is refused under the stock
   configuration: it reserves 10,855,811,936 bytes at peak (about 100M
   groups at 108 bytes), the derived per-query pool is 25% of memory
   (8,225,948,672 on the reference box), and the per-tenant pool, 50%
   (16,451,897,344), would hold it. With a 16 GiB per-query pool it runs in
   10.6 s cold and 10.1 s warm. ADR-0954's spill would not apply either: its
   eligibility predicate (`plan_nodes_are_spill_classifiable`) does not
   admit a `Sort` node, which the statement's `ORDER BY ... LIMIT` carries.

## Decision

### Track A: narrow projections

A1. **Directories are decoded once per (query, segment) on every route.**
The striped path carries the decoded footer directories (STREAM_DIR,
FIELD_DIR, SKIP_IDX, PAGE_DIR) beside the footer in the per-segment plan
state, and both `fetch_object_v4` and `RlogReader::from_source` take them
from there; the read gate's job size comes from the already-decoded
`PageDir` rather than a third decode. Striping deals whole row groups, not
single blocks, so a row group's dictionaries are decoded once per partition
that owns it. Pinned by an accounting test on a multi-row-group fixture
scanned with more partitions than segments: for a 1-of-N projection the
plan phase charges each segment's four directories exactly once (their
`uncomp_len` read off the footer), the scan phase charges only the
projected pages (what a whole-object reader reports for the same
projection), and the two together equal one whole-object decode; today the
striped scan phase alone exceeds that by (blocks - 1) times the directory
total. The route also applies to every predicated statement the
whole-segment fast path refuses (block predicates, pending erasure, a
window the segment does not contain), not only when partitions exceed
segments.

A2. **A partition's ranged reads are pipelined across its segments.** The
whole-segment fast path prefetches the ranges of its next segments while the
current one decodes, bounded by its share of the store GET concurrency
(`store_get_concurrency / partitions`, at least 2), so a partition's wall
time is its bytes over its share of throughput, not its round trips times
the latency. Pinned by a `FaultStore::hold` test: with a partition owning
three segments and the first segment's range GET held, the second segment's
range GETs are issued before the hold releases; today they are not.
Within one statement a current open is reported refused by the fetch memory
budget only if the budget refused it twice, the second time after every
unconsumed prefetch of every partition of the statement had been dropped and
the statement's pipeline turned off. Across statements the budget stays
fail-fast: another statement's prefetches can hold the bytes, and that
refusal is typed and reported like any contended reservation.

A3. **The cost-based policy carries a time term, so a narrow projection on
S3 reads ranged.** A request's cost in bytes is the bytes one connection
transfers during one request's latency, `request_latency *
per_connection_throughput`, from the store cost profile beside its prices
(the reference profile records the figures measured from the reference box:
about 70 ms and about 90 MB/s, so about 6.3 MB per request). The resolved
rate under `cost-based` is the time-derived rate whenever the price-derived
one saturates (a profile with zero byte prices, which is where
`resolve_cost_based_rate` short-circuits to `u64::MAX` today), and the
larger of the two when both are finite; a saturated rate is therefore only
possible on a profile that records neither prices nor timings.
`resolve_cost_based_rate` thus returns a finite rate on the intra-region
profile, `saturates_routing` is false, and the routing threshold stays at
its configured value (512 KiB by default). The projection break-even is NOT
that threshold: today the engine always sets `logs_block_range_threshold`,
and `effective_whole_object_threshold` returns the configured value
verbatim, so a finite rate alone would make `ranged_projection_pays`
compare the saved bytes against 512 KiB and read a 3 MB L0 object ranged,
the shape ADR-2023 measured as a threefold concurrent throughput loss. So
under `cost-based`, and only there, `ranged_projection_pays` takes the
larger of the configured routing threshold and five request costs as its
break-even (three request costs, 18,900,000 bytes, since the three-request-cost amendment below):
a 35 MB object at a 3% projection saves 34 MB against a 31 MB
break-even and reads ranged; a 3 MB L0 object saves under 3 MB and reads
whole; an explicit `--logs-block-range-threshold` still bounds the
block-range routing it was written for. The break-even applies only when
`cost-based` took the rate from the profile (its price term or its time
term). An explicit `--logs-request-cost-bytes` replaces that rate and keeps
the configured routing threshold as the break-even, because ADR-0996
promises that a deployment setting the flag keeps exactly the routing it had
under ADR-0904; the end-to-end test
`an_explicit_request_cost_flag_keeps_its_deployment_on_the_ranged_route`
pins it. `byte-minimal` and `latency-first`
keep today's break-even (the configured threshold) and today's rate: they
exist to read ranged wherever bytes are saved, and this decision does not
touch them. The rate drives a third decision
too: the coalescing gap, `max(request_cost_bytes, DEFAULT_LOG_COALESCE_GAP)`,
which no caller pins, so on the reference profile it becomes about 6.3 MB
instead of the byte-minimal rate's 1.9 MB. That is the same trade stated
once (fetching up to one request cost of unwanted bytes to save one
request), so it stands, but it means the ranged bytes of a narrow
projection under the default will sit above the 2.5% measured under
`latency-first`; the A3 acceptance pins the coalesced byte count at the
reference profile's gap on a fixture with ranges spaced on both sides of
it, and the measurement wave reports the figure beside the wall time. The
startup line names which term produced the rate and the break-even in
force. The partition count is unchanged (derived from cores); with A2 that
is enough.

Expected on the reference box after A1 to A3 (pre-registered on #1248
before A3's run): the one-column statement under 2 s cold at 32 partitions
(see the three-request-cost amendment below for the read shape the five-request-cost
break-even gave that statement; it records no wall time),
the stock cold suite under 200 s over the same 42 statements the 241.4 s
baseline covers, with q33 reported beside it under its own band (B3), and
the tuned arm re-registered for 35 MB objects with partitions at most the
segment count.

### Track B: q33 in the stock configuration

B1. **The derived per-query SQL pool is the tenant's share.**
`SQL_QUERY_MEMORY_PERCENT` becomes 50, equal to `SQL_TENANT_MEMORY_PERCENT`
(whose doc comment, "twice the per-query share so the per-tenant ceiling
admits concurrent queries without the per-query clamp binding", is rewritten
in the same change, since it would then be false),
so a lone statement may reserve the tenant's whole SQL share. The tenant
total is unchanged and the per-statement cap is removed: the per-query pool
nests inside the per-tenant pool, which refuses the byte that would exceed
50%, so N statements share the same total they share today, but a second
concurrent statement no longer has a guaranteed quarter of memory; it gets
what the first left. The ten-connection envelope (#2367) does not move,
because the tenant pool bounds the sum. An explicit `--sql-max-query-bytes`
still wins: over an explicit `--sql-tenant-max-bytes` it is clamped to that
ceiling, and over a derived or fallback tenant ceiling it raises the ceiling
to match, the existing rule. On the reference box q33 then runs inside
16,451,897,344 against its 10.86 GB peak.

B2. **Spill eligibility for the `Sort` node is not changed here.** ADR-0954
excludes `Sort` for a recorded reason this ADR cannot answer: an external
merge sort returns the same rows, but there is no proof its tie order
equals the in-memory sort's, and row order is part of an `ORDER BY` result
(`executor.rs`, the classifier's own comment). q33's `ORDER BY c DESC
LIMIT 10` has ties by construction (most groups count 1), so admitting the
node on the aggregate's exactness alone would let a spilled run return a
different top ten. Admitting it needs a tie-order proof: a sort key the
plan proves total, or the group keys appended as a deterministic tiebreak
before the sort when spill is on. That belongs with the spill default work
on issue #2416, which this ADR leaves to its own decision. On hosts whose
tenant share is below q33's peak the statement therefore still refuses
after this ADR; the reference box is covered by B1.

B3. **q33 is a measured statement.** The reference-suite registration on
#1248 carries q33 in the stock arm with a pre-registered band (about 11 s
cold, 10 s warm from Stage 0); a null for it is a miss. The acceptance
script in `docs/internal/clickbench-aws-runbook.md` asserts the statement
set by identity before it reads any band (`EXPECTED_MEASURED = 42` and
`EXPECTED_FAILED_Q = {"q33"}`, written
for the budget B1 removes), so the measurement wave changes those to 43
measured and no expected failure in the same change that records the
figures, and the 42-statement bands keep their baseline for comparability
while q33 is reported as a 43rd row.

## Rejected alternatives

- **Enable spill by default.** Needs a scratch location and a scratch
  ceiling the server has no derivation for, and the statement fits in memory
  on the reference box once the per-query share is right. Spill stays the
  operator's lever for smaller hosts, and the `Sort` admission it needs for
  this statement is decided with it on issue #2416. (Superseded: see the
  issue #2416 spill amendment below, which turns spill on by default under
  `--cache-dir`.)
- **A streaming top-k for q33.** The running `COUNT` is a lower bound on a
  group's final count and not monotone with the final order, so no exact
  short-cut exists over an unsorted scan (noted on #837).
- **Raise only the tenant share.** The refusal comes from the per-query
  ceiling; 25% was chosen so two statements could sit at the ceiling at once
  inside the 50% tenant share and a second always had room, and the tenant
  pool already bounds the sum by refusing the byte over 50%.
- **Derive the partition count from the segment count per query.** It fixes
  the ranged fast path's latency bound for this corpus but not the general
  case (a query over 10,000 segments cannot run 10,000 partitions), and it
  leaves the striped path's amplification in place for any query whose
  segment count is below the partition count. A2 fixes the fast path at any
  partition count; A1 fixes the striped path.
- **Make `latency-first` the default on S3.** It is byte-minimal with an
  operator-chosen concurrency, so it would read 3 MB L0 objects ranged where
  the whole read is cheaper, and ADR-2023 measured a threefold concurrent
  throughput loss from a ranged default on a loopback store. The time term
  lets the existing break-even decide per object.

## Consequences

- Narrow projections on S3 move their columns' bytes and finish in the time
  those bytes take, on objects where the bytes they skip exceed the
  break-even (three request costs since the three-request-cost amendment below).
  On the whole-segment fast path wide statements are
  unchanged (they fail the break-even and read whole, as today). On the
  planned route an object at or below the break-even is read whole with no
  probe, but an object above it pays the tail probe and directory reads
  before the coverage crossover decides, so a wide statement there whose
  surviving ranges cover at least 75% of such an object reads it whole after
  the probe.
- A query whose partition count exceeds its segment count no longer pays a
  directory decode per block; the striped route costs what the fast path
  costs plus one fetch-side directory read per segment.
- A lone statement can use the tenant's whole SQL share; the operator who
  wants the old split sets `--sql-max-query-bytes` to half the tenant
  ceiling, 25% of memory.
- Spill eligibility is unchanged; a `Sort` over an aggregate still refuses
  spill until the tie-order question is answered on issue #2416. (Superseded:
  see the issue #2416 spill amendment below, which admits that `Sort` once
  its key is made total.)
- The store cost profile gains two measured constants per profile (request
  latency, per-connection throughput) with the date and host they were
  measured on; a profile without them keeps the price-only rate.

```mermaid
flowchart LR
    Q[statement, projection f] --> P{partitions vs segments}
    P -->|partitions <= segments| F[whole-segment fast path]
    P -->|partitions > segments| S[striped path]
    F --> R{ranged_projection_pays?<br/>saved bytes > max(routing threshold, 5 x request cost)<br/>3 x request cost since the three-request-cost amendment}
    R -->|yes, A3 time term| RG[ranged column reads<br/>A2: pipelined per partition]
    R -->|no| W[whole-object GET]
    S --> D[A1: directories decoded once per segment<br/>row groups dealt whole]
    RG --> X[decode projected pages, dictionaries once per row group]
    W --> X
    D --> X
```

## Amendment (2026-10-03): spill on by default under --cache-dir, and the Sort admitted (issue #2416)

<!-- amendment-applies: sections="Rejected alternatives|Consequences" pointer="issue #2416 spill amendment" -->

This is the issue #2416 spill amendment. Two statements above no longer
hold; both now carry a pointer here, and the rest of this ADR stands.

- The rejected alternative "Enable spill by default" is superseded. The
  server now derives both things that alternative lacked: with `--cache-dir`
  set and no spill environment, spill goes under
  `<cache-dir>/sql-spill/<instance-id>` with a ceiling derived from the
  volume's free bytes and the memory budget, and `--sql-spill off` turns it
  off. ADR-0954's cache-dir spill default amendment records the rules.
- The consequence "a `Sort` over an aggregate still refuses spill" is
  superseded. A `Sort` directly over a spill-exact aggregate, or over a
  projection directly over one, now has the aggregate's group keys appended
  as trailing tiebreak terms, which makes its key total, and is then
  spill-eligible. That is the tie-order proof decision B2 asked for. The
  terms are appended whatever the spill setting, so a statement of q33's
  shape returns the same top ten with spill forced and with spill off.

Decision B2 itself stands as a record of what this ADR did not change.

## Amendment (2026-10-04): the cost-based projection break-even is three request costs (issue #2555)

<!-- amendment-applies: sections="Track A: narrow projections|Consequences" pointer="three-request-cost amendment" -->
<!-- amendment-supersedes: phrase="five request costs" pointer="three-request-cost amendment" -->

This is the three-request-cost amendment. Decision A3 set the cost-based
projection break-even at five request costs; it is now three. A3, the first
Consequences bullet and the diagram carry a pointer here, and the rest of
this ADR stands.

**Why three.** A whole read costs one request and S bytes; a ranged read
costs k requests and b bytes. At a rate of r bytes per request the ranged
read pays when (k - 1) * r + b < S, that is when the bytes it skips, S - b,
exceed (k - 1) * r. Five request costs is that inequality with k = 6, taken
from the 5.46 GETs per object measured on q20 on an older layout, the figure
`WHOLE_OBJECT_REQUEST_MULTIPLE` records. On the reference tenant a
one-column projection measured 4.19 GETs per object under `latency-first`
(918 over 219 objects) and 4.40 under `cost-based` for the 107 objects that
read ranged, moving 0.89 to 0.95 MB per object. So k = 4
(`COST_BASED_RANGED_REQUESTS`), and the break-even is three request costs.

**What five request costs produced.** At 31,500,000 bytes on the reference
profile, the 112 of those 219 objects at or below about 32.4 MB (the size at
which a 3-of-114 projection skips 31,500,000 bytes) read whole: 583 GETs and
2,919,327,365 wire bytes, 37.7 percent of the 7,741,962,796-byte corpus,
where the ranged read of all 219 objects moves 194,213,453.

**The new break-even.** When `cost-based` takes its rate from the profile
(its price term or its time term), the break-even is
max(routing threshold, 3 * request cost): 18,900,000 bytes on the reference
profile, whose request cost is the time term's 6,300,000. The whole-segment
fast path compares it with the bytes it expects to skip, the object size less
its count-ratio estimate of the projected bytes (`ranged_projection_pays`).
The plan phase (`plan_whole_object_bound`) and the ranged fetch's size
crossover know no projection and compare it with the object size, so an
object of 18,900,000 bytes or less reads whole on both, and the planned route
probes an object above it before the coverage crossover decides.

**Not changed.** An explicit `--logs-request-cost-bytes` still derives no
break-even and keeps the routing threshold in its place, whatever its value.
`request-minimal`, `byte-minimal` and `latency-first` resolve as before. The
0.75 coverage crossover, the coalescing gap (one request cost, at least
64 KiB), and ADR-0904's derived crossover (`WHOLE_OBJECT_REQUEST_MULTIPLE`
request costs, still 5) are unchanged. A 3 MB L0 object still reads whole:
one request cost, 6,300,000 bytes, already exceeds it, so no projection of it
skips three.

**Known limit.** The fast path's projected fraction is a ratio of column
counts, not of bytes. A statement narrow by count and wide by bytes (a few
columns holding most of an object's bytes) is estimated to skip more than it
does and is routed ranged; if its pages then cover 75% of the object the
coverage crossover reads it whole after the probe and directory reads, and
below that it pays the ranged requests for a smaller saving than estimated.
Lowering the break-even from five request costs to three widens the band of
object sizes where that misroute can happen. This amendment records no wall
time for the one-column statement A3 expected under 2 s.

Pinned by `the_cost_based_break_even_is_three_request_costs` (ravel-query
config), `a_3mb_l0_object_reads_whole_on_the_reference_profile`,
`a_30mb_object_reads_ranged_narrow_and_whole_wide` and
`the_planned_route_reads_an_object_under_the_break_even_whole` (ravel-query
`log_fetch_bound`), the fetch half of
`a_30mb_object_reads_ranged_narrow_and_whole_wide` (ravel-sql
`logs_fast_path_projection_routing`), and
`an_explicit_request_cost_flag_keeps_its_deployment_on_the_ranged_route`
(ravel-server `logs_fetch_policy_e2e`).
