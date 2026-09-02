# Predicting the S3 request bill

Storage is a rounding error in Ravel's cost. Request charges dominate: at a
modeled 100-tenant, 1 TB/day workload, ADR-0076 estimated roughly $7-8k/month
of request fees against roughly $100-150/month of storage, and at a
single-tenant deployment request fees run over 98% of the total bill. This
guide gives an operator the formula and the levers to predict and control
that bill before deploying, not after the first invoice. The write path sets
most of it and comes first; the read path adds a request count the engine
itself chooses, and one flag moves it.

## The formula

```
PUTs/day = 2 x tenants x signals x shards x replicas x (86400 / age_threshold_s)
```

Every flush is a data-object PUT and a commit-record PUT (the two-object
commit protocol, unchanged by anything in this guide). Buffers are scoped
per `(tenant, signal, shard)` per ingest replica, so the PUT rate scales
linearly in all five terms. Two of them are workload facts an operator does
not choose (tenant count, signal mix); three are configuration this guide
covers, in the order that costs the least to use:

1. **Replica fan-out** ([ingest-affinity.md](ingest-affinity.md)): routing
   each `(tenant, signal)` to a stable subset of ingest replicas divides this
   term by (total replicas / subset size), at no cost in acknowledgement
   latency.
2. **Shard count** ([shard-overrides.md](shard-overrides.md)): linear in
   shards, and the primary per-tenant, operator-facing cost control. Cutting
   a tenant from 4 shards to 1 is a 4x reduction in that tenant's ingest PUTs
   and read-side LIST cost. A tenant's shard count no longer has to equal the
   deployment-wide default once it has a provisioning record: lowering the
   global default is a new-tenant-only change and does not require touching
   any already-onboarded tenant.
3. **Flush cadence** (`--max-flush-delay`, `--max-flush-delay-idle`,
   `--min-flush-bytes` on `ravel-server`; ADR-0076 decision 4): the shipped
   default is 2s / 40s / 256KiB, a 4x reduction from the pre-ADR-0076 500ms /
   10s / 64KiB defaults. This is the lever of last resort because, unlike the
   first two, it costs strict-mode acknowledgement latency directly: a
   strict export waits for the configured `max_flush_delay` plus two PUT
   round trips before its ack returns. The three knobs must move together
   (raising one alone does little) and are validated at startup against a
   derived ceiling: `max_flush_delay` plus the ingest pipeline's flush
   lifetime must stay under `FLUSH_BOUND_SLACK_HOURS` (the read-side scan
   slack budget), and the derived strict-visibility budget must stay under a
   client-timeout-derived ceiling (3s, from the smallest documented OTLP
   export timeout of 5s minus an assumed 2s PUT tail). A value that violates
   either is refused at startup, not silently accepted.

## Measured effect of the cadence default

Measured locally with `cargo run --release -p ravel-bench --bin ingest_bench
-- --store memory --shards 4 --target-series 50000 --points-per-sec 50000
--duration-secs 30 --batch-size 200 --max-inflight-flushes 1
--flush-delay-policy fixed`, comparing the pre-epic defaults (commit
`cc5ef36d`, 500ms/10s/64KiB) against the post-epic defaults (commit
`96158c02`, 2s/40s/256KiB), same workload, in-memory store (isolating the
ingest pipeline's own flush behavior from real object-store latency, which
is what this comparison needs to measure — a request-count reduction is a
property of flush cadence, not of the backing store):

| | Before (500ms) | After (2s) | Change |
|---|---|---|---|
| Flushes (30s window) | 200 | 60 | 3.33x fewer |
| Estimated PUTs | 400 | 120 | 3.33x fewer |
| Ack latency p99 | 616.7ms | 2174.4ms | 3.5x higher |
| Write amplification | 3.44x | 2.16x | lower (fewer, larger objects) |

The measured reduction (3.33x) is close to but under the ADR's steady-state
4x estimate: a fixed 30-second window ends mid-interval for the slower
cadence, undercounting its flush count relative to a true steady-state rate.
A longer-duration run converges closer to 4x. The ack-latency increase is
the direct, expected, and accepted cost of this decision (ADR-0076 decision
4's "Expected effect" and "Consequences" sections) — durability itself is
unchanged; a strict-mode ack still only returns once the commit PUT that
makes the write durable has landed.

The full committed S3 cost-per-request benchmark (dollar cost against real
S3/MinIO, not just PUT count) is tracked separately (#79): request-count
reduction is a property of the ingest pipeline alone and is measured here
without needing real object storage, but a real dollar-cost figure needs a
real backend and is out of this guide's scope.

## Predicting your bill

Given a workload, estimate PUTs/day from the formula above using your
`shards`, `replicas`, `max_flush_delay` (or `max_flush_delay_idle` for a low
per-tenant volume, since the idle tier's ceiling — not the strict tier's
floor — bounds a buffer that never crosses `min_flush_bytes` before its
strict waiter, if any, resolves), and multiply by your object-storage
provider's per-1k-request price. Keyed log and span requests add a
per-request idempotency-marker PUT and a dedup-window LIST beyond this
formula (docs/consistency-model.md); no lever in this guide touches that
cost, since it is per-request rather than per-flush.

Two ceilings this guide's levers do not remove:

- **Per-tenant caps** (200k active series/signal by default) mean a
  workload beyond that scales as more tenants, not one larger tenant — the
  formula's `tenants` term grows, not any single tenant's own shard/PUT
  count.
- **The read-side request budget** (`docs/adrs/0075`) is derived from shard
  count and flush cadence together (`derive_max_s3_requests`), so lowering
  shard count or raising flush delay also lowers the per-query request
  budget on the read side — cheaper writes and cheaper reads move together,
  not independently.

## The read side: buying bytes with requests

The formula above counts writes, where the request rate follows from
configuration an operator sets once. Reads are different: the engine decides,
per segment, whether to fetch an object in one whole-object GET or as a probe
plus a small number of ranged GETs that move fewer bytes. That decision is a
request count nobody configured directly, and on a request-billed backend it
is money.

One number arbitrates it:

```text
avoiding k requests to move b extra bytes is a win exactly when
b < k * request_cost
```

`--logs-request-cost-bytes <BYTES>` on `ravel-server` is that `request_cost`:
one saved object-store round trip is worth this many saved transfer bytes.
Unset, it takes `ravel_query::DEFAULT_LOG_REQUEST_COST_BYTES` (1,887,437 bytes,
about 1.8 MiB), so an unset deployment behaves exactly as it did before the
flag existed. The derivation of that number — a latency-bandwidth product
measured on one in-region S3 configuration, one instance type, one fetch-permit
count — lives in the constant's doc comment in
`crates/ravel-query/src/log_fetcher.rs`; read it there rather than assuming the
default was tuned for your deployment, because it was tuned for that one.

Raising the value makes the engine value a request more: fewer segment fetches
take the ranged route, more read whole objects, request count falls and bytes
rise. Lowering it does the reverse. The decision is per segment, so one
statement spanning many segments can take both routes within a single query --
the counters report opens by shape rather than statements by shape for exactly
that reason.

The setting never changes a query's answer. It selects which read path fetches
the bytes; the rows returned are identical at every value, and only request
counts, byte counts, and timing differ (ADR-0904 decision 4, pinned by a
whole-vs-ranged row-equality test).

### Why one knob and not three

The value drives three decisions in the logs fetch layer, all of them the same
question asked about different inputs:

1. **The coalescing gap**: two wanted extents separated by less than one
   request cost fuse into a single GET.
2. **The pre-probe whole-object crossover**: an object at or below five
   request costs is read whole, because the ranged protocol adds roughly that
   many round trips and cannot save enough bytes below that size to pay for
   them.
3. **Projection routing on the whole-segment fast path**: a predicate-free
   scan opens by column chunk only when the bytes its projection skips clear
   that same crossover.

Deriving all three from one field is what makes recalibrating the store
recalibrate the read path coherently, instead of leaving three thresholds to
disagree. Two floors bound the low end (a 64 KiB coalescing gap and a 512 KiB
whole-object crossover), so a very small value clamps rather than producing a
one-block GET storm.

Two properties follow from what the number is:

- It is a property of the **store and the instance** — round-trip latency and
  single-stream bandwidth at the fetch concurrency in use — not of the RLOG
  format. A different store, a cross-region bucket, or a different
  `--max-concurrent-gets` has a different right value, and changing the GET
  bound changes this break-even along with it.
- The `logs-` prefix is literal. Metric (RSEG) reads use fixed gap and
  crossover constants that are not request-cost-derived, and do not respond to
  this flag.

### What the trade costs, in modelled dollars

Every dollar figure in this section is list-price modelling, not a measured
bill: the amounts are computed from counted requests and counted bytes against
published us-east-1 list prices, never read off an invoice. The absolute
amounts for a single benchmark pass are small, so they matter as a rate at
production query volume, not as cents.

The measured example is the ClickBench corpus (42 statements, cold, reference
box; recorded on issue #680 and quoted in `docs/adrs/0904`, with the pass
procedure in [clickbench-aws-runbook.md](clickbench-aws-runbook.md)). Turning
on projection routing moved:

```text
cold requests   203,243 -> 751,409    (+270%)
cold bytes       403.97 -> 194.19 GB  (-52%)
cold time        324.79 -> 222.19 s   (-31.6%)
```

Half the bytes, 3.7x the requests, 31.6% faster. Modelled at us-east-1 list
prices that pass costs about 2.2x more in request charges than it did before,
and it is faster.

Both of those can be true at once because of an asymmetry in the price sheet:
same-region S3-to-EC2 transfer is not billed, so on that deployment shape
trading bytes for requests spends a billed resource to save a free one. Where
transfer is billed — cross-region, internet-facing, or an object store that
charges egress — the sign flips: at list egress near $0.09/GB against GETs near
$0.0004/1000, the dollar break-even lands around 4.4 KB per request, three
orders of magnitude below the 1.8 MiB latency break-even, so the shipped
default is already the cost-preferring setting there. The right value depends
on the deployment's billing shape, which is why this is a flag and not a
constant.

### Choosing a value, cheapest lever first

1. **Leave it unset** (prefer latency). Costs nothing and changes nothing. It
   is the right starting point for a latency-bound read path on a store whose
   round-trip latency and single-stream bandwidth resemble the configuration
   the default was measured on. A different store, region, or fetch
   concurrency has a different break-even, so treat unset as a default to
   measure against rather than one known to be correct everywhere.
2. **Leave it unset on an egress-billed backend** too. The default already
   sits far above that deployment's dollar break-even, so there is no lower
   value worth inventing.
3. **Raise it on a request-billed, transfer-free backend** (same-region S3).
   Set it at or above the largest segment object *any* tenant this process
   serves writes: the flag is process-wide, so a single tenant's largest object
   is the wrong unit, and any tenant holding bigger objects keeps routing
   ranged. There is no format-level object-size cap to read this from; object
   size comes from `--batch-rows` and `--target-bytes` at write time and is
   observable per tenant, so measure it and round up. Overshooting costs
   nothing, because all three decisions saturate once the value exceeds the
   largest object: every candidate segment becomes one GET. What it costs is
   the other column of the table above, roughly twice the bytes and the cold
   latency win given back.

The flag is read at startup only, like the other read-path knobs in
[query.md](query.md#operator-configurable-budgets-server-flags), and it sits
inside `--logs-block-range-threshold`, which selects which fetcher entry point
an object takes before this value governs how that fetch behaves.

To see which way your queries are actually routing, read the logs scan's
`fast_path_whole_object_segments` and `fast_path_ranged_segments` plan metrics
from an `EXPLAIN ANALYZE`, beside the per-operation request and byte counts. A
large ranged-open share on statements that move few bytes, on a backend that
bills requests, is the signal that this value is set for the wrong objective.

The cost-preferring setting has no published ClickBench triple yet: the figures
above measured the routing change, not this flag, and a high value also
collapses ranged reads on the predicate path that were active on both sides of
that comparison. Requests should land at or below 203,243 and bytes at or above
403.97 GB; the measurement, not this guide, gets to say where.
