# Predicting the S3 request bill

Storage is a rounding error in Ravel's cost. Request charges dominate: at a
modeled 100-tenant, 1 TB/day workload, request fees run roughly $7-8k/month
against roughly $100-150/month of storage, so request fees are over 97% of the
total bill. This guide gives an operator the formula and the levers to predict
and control that bill before deploying, not after the first invoice. The write
path sets most of it and comes first; the read path adds a request count the
engine itself chooses, and one flag moves it.

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
   and read-side LIST cost. A tenant's shard count does not have to equal the
   deployment-wide default once it has a provisioning record: lowering the
   global default is a new-tenant-only change and does not require touching
   any already-onboarded tenant.
3. **Flush cadence** (`--max-flush-delay`, `--max-flush-delay-idle`,
   `--min-flush-bytes` on `ravel-server`): the shipped default is
   2s / 40s / 256KiB. This is the lever of last resort because, unlike the first two,
   it costs strict-mode acknowledgement latency directly: a strict export
   waits for the configured `max_flush_delay` plus two PUT round trips before
   its ack returns. The three knobs must move together (raising one alone does
   little) and are validated at startup against a derived ceiling:
   `max_flush_delay` plus the ingest pipeline's flush lifetime must stay under
   the read-side scan slack budget, and the derived strict-visibility budget
   must stay under a client-timeout-derived ceiling (3s, from the smallest
   documented OTLP export timeout of 5s minus an assumed 2s PUT tail). A value
   that violates either is refused at startup, not silently accepted.

## What the cadence costs

Lengthening `max_flush_delay` by a factor divides the fast-tier PUT rate by
about that factor, because every flush is two PUTs and a buffer that reaches
neither `min_flush_bytes` nor a strict waiter flushes on age alone, and it
raises strict-mode acknowledgement latency by up to the added delay: a strict
export waits for the flush trigger plus two PUT round trips. Fewer, larger
objects also lower write amplification. Durability is unchanged at any
setting: a strict-mode ack still returns only once the commit PUT that makes
the write durable has landed. The request-count reduction is a property of
the ingest pipeline alone and holds on any backend; a dollar figure needs the
backend's own price sheet, which the read-side section below applies.

## Predicting your bill

Given a workload, estimate PUTs/day from the formula above using your
`shards`, `replicas`, `max_flush_delay` (or `max_flush_delay_idle` for a low
per-tenant volume, since the idle ceiling, not the strict floor, bounds a
buffer that never crosses `min_flush_bytes` before its strict waiter, if any,
resolves), and multiply by your object-storage provider's per-1k-request
price. Keyed log and span requests add a per-request idempotency-marker PUT
and a dedup-window LIST beyond this formula (see the
[consistency model](../consistency-model.md)); no lever in this guide touches
that cost, since it is per-request rather than per-flush.

Two ceilings this guide's levers do not remove:

- **Per-tenant caps** (200k active series/signal by default) mean a
  workload beyond that scales as more tenants, not one larger tenant: the
  formula's `tenants` term grows, not any single tenant's own shard/PUT
  count.
- **The read-side request budget** is derived from shard count and flush
  cadence together, so lowering shard count or raising flush delay also lowers
  the per-query request budget on the read side: cheaper writes and cheaper
  reads move together, not independently.

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
Unset, it takes the built-in default of 1,887,437 bytes (about 1.8 MiB). That
default is a latency-bandwidth product: the transfer volume whose time, at one
in-region S3 configuration's single-stream bandwidth, equals one request's
round-trip latency, measured on one instance type at one fetch-permit count.
It was tuned for that one configuration, so treat the default as a value to
measure against rather than one known to be correct for your deployment.

Raising the value makes the engine value a request more: fewer segment fetches
take the ranged route, more read whole objects, request count falls and bytes
rise. Lowering it does the reverse. The decision is per segment, so one
statement spanning many segments can take both routes within a single query.
The counters report opens by shape rather than statements by shape for exactly
that reason.

The setting never changes a query's answer. It selects which read path fetches
the bytes; the rows returned are identical at every value, and only request
counts, byte counts, and timing differ.

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

- It is a property of the **store and the instance**, that is round-trip
  latency and single-stream bandwidth at the fetch concurrency in use, not of
  the RLOG format. A different store, a cross-region bucket, or a different
  `--fetch-concurrency` has a different right value, and changing fetch
  concurrency changes this break-even along with it.
- The `logs-` prefix is literal. Metric (RSEG) reads use fixed gap and
  crossover constants that are not request-cost-derived, and do not respond to
  this flag.

### What the trade costs, in modeled dollars

Every dollar figure in this section is list-price modeling, not a measured
bill: the amounts are computed from counted requests and counted bytes against
published us-east-1 list prices, never read off an invoice. The absolute
amounts for a single benchmark pass are small, so they matter as a rate at
production query volume, not as cents.

The measured example is the ClickBench corpus (42 statements, cold, reference
box, with the pass procedure in
[clickbench-aws-runbook.md](../internal/clickbench-aws-runbook.md)). Reading
every segment whole against routing narrow projections to ranged reads:

```text
cold requests   203,243 whole    751,409 ranged    (+270%)
cold bytes       403.97 GB whole  194.19 GB ranged  (-52%)
```

Half the bytes and 3.7x the requests. The ranged pass finished faster despite
the extra requests, at 222.19 s cold. Modeled at us-east-1 list prices it
costs about 2.2x more in request charges than the whole-object pass, and it
is faster.

Both of those can be true at once because of an asymmetry in the price sheet:
same-region S3-to-EC2 transfer is not billed, so on that deployment shape
trading bytes for requests spends a billed resource to save a free one. Where
transfer is billed (cross-region, internet-facing, or an object store that
charges egress) the sign flips: at list egress near $0.09/GB against GETs near
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
   serves writes. The flag is process-wide, so a single tenant's largest
   object is the wrong unit, and any tenant holding bigger objects keeps
   routing ranged. There is no format-level object-size cap to read this from;
   object size comes from `--batch-rows` and `--target-bytes` at write time
   and is observable per tenant, so measure it and round up. Overshooting
   costs nothing, because all three decisions saturate once the value exceeds
   the largest object: every candidate segment becomes one GET. What it costs
   is the other column of the comparison above, roughly twice the bytes and
   the cold-latency win given back.

The flag is read at startup only, like the other read-path knobs in
[query.md](query.md#operator-configurable-budgets-server-flags), and it sits
inside `--logs-block-range-threshold`, which selects which fetcher entry point
an object takes before this value governs how that fetch behaves.

To see which way your queries are actually routing, read the logs scan's
`fast_path_whole_object_segments` and `fast_path_ranged_segments` plan metrics
from an `EXPLAIN ANALYZE`, beside the per-operation request and byte counts. A
large ranged-open share on statements that move few bytes, on a backend that
bills requests, is the signal that this value is set for the wrong objective.

No ClickBench pass under the cost-preferring setting is published: the figures
above compare the two read shapes, not this flag, and a high value also
collapses the ranged reads on the predicate path that were active on both
sides of that comparison. Such a pass lands at or below 203,243 requests and
at or above 403.97 GB; the measurement, not this guide, says where.

## Per-query cost accounting

Every accounted read reports what it actually spent on object storage, and
Ravel exports the running totals in the `ravel_query_*` metric family. The
[observability guide](observability.md#per-query-cost) catalogs that family and
shows the PromQL that reads it; this section is the accounting behind the
numbers.

Each accounted query folds one snapshot at completion: the object-store
requests it issued, the bytes it transferred, the in-process cache hits and
misses attributed to it, and the decompressed sample bytes it decoded. Beside
each actual sits a pre-execution estimate of the same quantity. The estimate is
an upper envelope, never a prediction: the planner takes the worst case
wherever it cannot bound a quantity, so a correct estimate lands at or above
the actual, never below it. Dividing an actual by its matching estimate gives a
ratio at or below 1 in the healthy case; a ratio above 1 means the actual
exceeded the envelope meant to bound it, which rules in either a cost-model gap
(the estimate omits a real source of spend) or a runaway query pattern the
model did not anticipate. Nothing rejects a query on that envelope. It is
measurement only.

Three gaps limit what the per-query cost family can show:

- A query that fails records no cost. A deadline breach, an admission
  rejection, and an execution error all return before the fold, and the error
  type carries no accounting snapshot. Read a sudden drop in the completed-query
  count against steady request logs as failures, not as idle capacity.
- A Flight SQL statement records two folds, one per RPC. The plan request
  records the first fold and the fetch request records the second, so the
  completed-query counter counts 2 for one logical query, and the two folds sum
  to one whole-query estimate beside the summed whole-query actual.
- A Flight fetch that a client abandons after one batch still records its
  partial cost. The stream ends when the client disconnects, so the bytes
  already spent are recorded and count as one query. An unusually low
  cost-per-query ratio on the Flight path can therefore mean early client
  disconnects, not cheap queries.

## A modeled cost is a model, not a bill

Every dollar figure in this guide is computed from counted requests and counted
bytes multiplied by a published list price. None of it is read off an invoice.
Two assumptions sit under any such figure, and either can move it:

- The **price sheet**. The figures here use us-east-1 list prices for PUT,
  GET, and transfer. A different region, a negotiated rate, a different
  provider's per-request price, or an egress charge the list model omits all
  change the total, sometimes enough to flip which lever is cheapest, as the
  same-region-versus-egress reversal above shows.
- The **workload**. Tenant count, signal mix, and query shape are inputs to
  the formula and the per-query accounting, not constants. A projection tuned
  for one corpus predicts a different bill on another.

The counted requests and bytes are real: they come from the object-store call
counters and the per-query fold, not from a guess. The dollars are a model laid
over those counts. Use a projection to compare levers and to size a deployment
before the first invoice, then reconcile it against a real bill once traffic is
flowing, and treat a persistent gap between the two as a signal that one of the
two assumptions above no longer holds.

## Background

The two-object commit protocol and request-cost reduction through flush
cadence and ingest affinity: ADR-0076. Per-query cost accounting and the
`/metrics` cost family: ADR-0044. The read-side request-cost knob and its
whole-versus-ranged routing: ADR-0904. The read-side request budget derived
from shard count and cadence: ADR-0075. Fetch concurrency: ADR-0088.
