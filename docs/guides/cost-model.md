# Predicting the S3 request bill

Request charges dominate the cost of Ravel, and storage is a small part of it.
In one illustrative scenario, request fees are roughly $7-8k/month against
roughly $100-150/month of storage. That puts request fees over 97% of the bill
of that scenario. The scenario is a modeled workload:

- 100 tenants and 1 TB/day
- three signals: metrics, logs, and spans. Audit runs its own separate
  maintenance path and is excluded here.
- the shipped `--shards` default of 4
- one ingest replica
- the shipped flush cadence (see [The formula](#the-formula))

Both figures are an illustration, not a derivation. They come from the
independent review reported by the request-cost decision record cited under
Background.

- The workload above fixes only the request side, through the formula.
- A storage amount also needs a retention window, the compressed bytes that
  those writes occupy on object storage, and a per-GB-month storage price. The
  values of that review for the three are recorded nowhere in this repository.
  You therefore cannot recompute the $100-150 or the 97% from this page.
- To get your own ratio, supply those three inputs, plus the per-1k-request
  prices of your provider.
- 97% is a short-retention figure. Storage and request cost approach parity at
  retention windows measured in hundreds of days. This is the one
  qualification that the decision records carry.
- A different shard count, replica count, or flush cadence moves the request
  side of the ratio. Put your own values into the formula. Do not trust these
  amounts at a different scale.

Use the formula and the levers in this guide to predict and control that bill
before you deploy. The write path sets most of the bill and comes first. The
read path adds a request count that the engine chooses, and one flag moves it.

## The formula

```
PUTs/day = 2 x tenants x signals x shards x replicas x (86400 / age_threshold_s)
```

`age_threshold_s` is decided per `(tenant, signal, shard)` buffer, per flush.
A buffer has priority in two cases, and either one is enough:

- A strict-mode export is waiting on its flush.
- Its estimated flush size has reached `min_flush_bytes` (256KiB default).

The two cases share one threshold. The bands are:

- **Idle clock**: `max_flush_delay_idle` (40s default). Applies to a buffer
  with no priority: no waiter, and not yet at `min_flush_bytes`.
- **Priority**: `max_flush_delay` (2s default). Applies to a buffer with a
  waiter, with enough bytes, or both.
- **Adaptive priority** (off by default, metrics only): with adaptive flush
  delay on, the metrics actor widens the priority threshold inside a corridor
  from `max_flush_delay` up to a ceiling.
  - The ceiling comes from the observed PUT round-trip time of the shard and
    the strict-visibility budget.
  - Where the threshold lands inside the corridor is per tenant, from the
    observed arrival gap of the tenant.
  - Log and span shards have no corridor and always use the fixed pair above.
- **Sub-floor hold** (off by default, all three signals): set
  `--idle-flush-byte-floor` to a non-zero byte count. A buffer whose estimated
  flush size has not reached that floor then waits for the sub-floor hold
  instead of the idle clock.
  - The hold is the flush lifetime of the pipeline (1 h) less one flush tick
    (200 ms). `age_threshold_s` for such a buffer is therefore 3,599.8 instead
    of 40, which is 24 flushes a day instead of 2,160.
  - The floor must be below `min_flush_bytes`. A server given a floor at or
    above it refuses to start.
  - This band is a durability trade, not only a cost trade. An acknowledged
    buffered-mode row in a buffer under the floor can sit in process memory
    for up to an hour before its flush opens. A crash in that window loses it.
  - For the same reason, the graceful-drain residue that a shutdown timeout
    cuts short can hold up to an hour of the rows of such a tenant.
  - Strict mode is unaffected, because a strict waiter keeps the priority
    threshold.
  - The flush deferral cap is derived from the slowest flush trigger. At the
    defaults that trigger is the 40 s idle clock, not this band.
  - `ravel_ingest_flushes_by_age_floor_total` counts the flushes that this
    band opened.

### Floor worked example

The estimated flush size of a buffer is measured in object bytes, not in the
memory that the buffer occupies. It is the sum of two terms:

- 32 bytes plus the name and value text of each series, counted once per
  series per object
- 16 bytes per scalar sample

Both terms matter here. The series term is charged again on every flush, and
it can dominate a wide, slow tenant. Take two tenants, each on one shard of
one replica, with about 60 bytes of label text per series, and a floor of
64 KiB:

| | series | one sample per series every | at the 40 s idle clock | floor off | floor at 64 KiB |
|---|---|---|---|---|---|
| Near-empty tenant | 2 | 10 s | 312 bytes per object | 2,160 flushes/day, 4,320 PUTs | 24 flushes/day, 48 PUTs |
| Small-but-steady tenant | 20 | 10 s | 3,120 bytes per object | 2,160 flushes/day, 4,320 PUTs | about 43 flushes/day, about 87 PUTs |

The first row is the case that the floor is for. Two series at one sample each
per 10 seconds accrue `2 x (32 + 60) = 184` bytes of series terms plus 3.2
bytes a second. After the full 3,599.8 s hold the object is about 11.7 KB,
still far under the 64 KiB floor. That buffer takes the hold every time, and
its flushes land in `ravel_ingest_flushes_by_age_floor_total`.

The second row shows that the floor is not a blanket 90x. The same arithmetic
with 20 series gives `20 x 92 = 1,840` bytes of series terms plus 32 bytes a
second, which reaches 64 KiB after about 1,990 seconds. At that point the
buffer leaves the sub-floor band. Its age is already well past 40 seconds, so
it flushes on the next tick. Its saving is real but smaller, and its flushes
count as ordinary idle-clock flushes, not floor ones.

A buffer crosses bands upward as rows arrive, so the floor never holds a
tenant that grew past it. The floor sets its boundary in bytes, and the rate
of each tenant decides which side the tenant is on.

### The size trigger

Age is not the only trigger. A buffer also flushes as soon as its estimated
object size reaches `target_bytes` (8MiB default), without waiting for any
clock. For a busy buffer that is the trigger that fires, so its PUT rate is
its byte rate divided by `target_bytes`, not `86400 / age_threshold_s`.

- Use the age formula for buffers that flush on a clock: the low-volume and
  strict-mode cases.
- Use the byte rate for buffers that reach the target size within their flush
  window.

### The levers

Every flush is a data-object PUT and a commit-record PUT (the two-object
commit protocol, unchanged by anything in this guide). Buffers are scoped
per `(tenant, signal, shard)` per ingest replica, so the PUT rate scales
linearly in all five terms. Two of them are workload facts that an operator
does not choose (tenant count, signal mix). Three are configuration that this
guide covers, in the order that costs the least to use:

1. **Replica fan-out** ([ingest-affinity.md](ingest-affinity.md)): route each
   `(tenant, signal)` to a stable subset of ingest replicas. This divides the
   term by (total replicas / subset size), at no cost in acknowledgement
   latency.
2. **Shard count** ([shard-overrides.md](shard-overrides.md)): the cost is
   linear in shards. This is the primary per-tenant, operator-facing cost
   control. A cut from 4 shards to 1 is a 4x reduction in the ingest PUTs and
   the read-side LIST cost of that tenant. After a tenant has a provisioning
   record, its shard count does not have to equal the deployment-wide default.
   A lower global default is a new-tenant-only change and does not require a
   change to any already-onboarded tenant.
3. **Flush cadence** (`--max-flush-delay`, `--max-flush-delay-idle`,
   `--min-flush-bytes` on `ravel-server`): the shipped default is
   2s / 40s / 256KiB. This is the lever of last resort. Unlike the first two,
   it costs strict-mode acknowledgement latency directly: a strict export
   waits for the configured `max_flush_delay` plus two PUT round trips before
   its ack returns. The three knobs must move together, because raising one
   alone does little. Startup validates them against a derived ceiling and
   refuses a value that violates either rule:
   - `max_flush_delay` plus the flush lifetime of the ingest pipeline must
     stay under the read-side scan slack budget.
   - The derived strict-visibility budget must stay under a
     client-timeout-derived ceiling (3s, from the smallest documented OTLP
     export timeout of 5s minus an assumed 2s PUT tail).
4. **The sub-floor hold** (`--idle-flush-byte-floor`, default 0 = off): the
   fourth band above. It moves separately from the three cadence knobs. It
   targets the near-empty tenant, the one shape that the first three levers
   cannot reach cheaply. It does not change what a tenant at moderate volume
   pays or what a strict-mode export waits for. It is listed last because it
   is the only lever here that widens a published durability window. See the
   fourth band for what an operator accepts when setting it.

## What the cadence costs

If you lengthen `max_flush_delay` by a factor, the fast-tier PUT rate divides
by about that factor. Every flush is two PUTs, and a buffer that reaches
neither `min_flush_bytes` nor a strict waiter flushes on age alone.

- Strict-mode acknowledgement latency rises by up to the added delay, because
  a strict export waits for the flush trigger plus two PUT round trips.
- Fewer, larger objects also lower write amplification.
- Durability is unchanged at any setting. A strict-mode ack still returns only
  after the commit PUT that makes the write durable has landed.

The request-count reduction is a property of the ingest pipeline alone and
holds on any backend. A dollar figure needs the price sheet of the backend,
which the read-side section below applies.

## Predicting your bill

1. Estimate PUTs/day from the formula with your `shards`, `replicas`, and
   `max_flush_delay`. For a low per-tenant volume, use `max_flush_delay_idle`
   instead. The idle ceiling, not the strict floor, bounds a buffer that never
   crosses `min_flush_bytes` before its strict waiter, if any, resolves.
2. Multiply by the per-1k-request price of your object-storage provider.

Keyed log and span requests add a per-request idempotency-marker PUT and a
dedup-window LIST beyond this formula (see the
[consistency model](../consistency-model.md)). No lever in this guide changes
that cost, because it is per-request, not per-flush.

The levers in this guide do not remove two ceilings:

- **Per-tenant caps** (200k active series/signal by default). A workload
  beyond that scales as more tenants, not one larger tenant. The `tenants`
  term of the formula grows, not the shard/PUT count of any single tenant.
- **The read-side request budget** is derived from shard count and flush
  cadence together. A lower shard count or a higher flush delay therefore also
  lowers the per-query request budget on the read side. Cheaper writes and
  cheaper reads move together, not independently.

## Read-side request cost

The formula counts writes, where the request rate follows from configuration
that an operator sets once. For reads, the engine decides per segment how to
fetch an object: in one whole-object GET, or as a probe plus a small number of
ranged GETs that move fewer bytes. That decision sets a request count that
nobody configured directly, and on a request-billed backend it is money.

One number decides it:

```text
avoiding k requests to move b extra bytes is a win exactly when
b < k * request_cost
```

`--logs-request-cost-bytes <BYTES>` on `ravel-server` is that `request_cost`:
one saved object-store round trip is worth this many saved transfer bytes.

Three separate values get called "the default" for this flag. Only one of them
is what a deployment that sets no flag runs with. If you confuse them, you can
make the read path more expensive.

### The fallback constant

**The built-in fallback constant** is 1,887,437 bytes (about 1.8 MiB). It is a
latency-bandwidth product: the transfer volume whose time equals the
round-trip latency of one request, at the single-stream bandwidth of one
in-region S3 configuration. It was measured on one instance type at one
fetch-permit count, and tuned for that one configuration. Treat it as a value
to measure against, not as a value known to be correct for your deployment.

The engine configuration carries the constant when nothing resolves over it.
`--logs-fetch-policy byte-minimal` and `latency-first` resolve to it while
this flag is unset. On the shipped flag set it is not the value in force.

### The derived value

**The value the shipped flag set resolves to** is derived, not that constant,
on every deployment, loopback `--s3-endpoint` or not. `--logs-fetch-policy`
defaults to `cost-based`. `cost-based` computes the rate from the active store
cost profile and does not fall back to the constant. The profile is
`--store-cost-profile`, which defaults to the reference `s3-intra-region-2026`
profile. The derivation takes the larger of a price term and a time term:

```text
price term = get_class_nanodollars x bytes_per_gib
             / (transfer_nanodollars_per_gib
                + retrieval_nanodollars_per_gib)
time term  = request_latency_micros x per_connection_throughput_bytes_per_s
             / 1,000,000
request_cost_bytes = the larger of the two
```

The result is floored at one byte. The time term needs the two optional
timings of the profile and is absent without them. A price term with free
bytes saturates and yields to the time term. Three cases matter:

- **The reference profile.** Transfer and retrieval are both free, so the
  price term saturates. The timings of the profile are 70,000 microseconds per
  request at 90,000,000 bytes per second per connection. They give a time term
  of 6,300,000 bytes per request: the bytes that one connection moves while it
  waits for one request.
  - The routing threshold keeps its configured value, 524,288 bytes by
    default. An explicit `--logs-block-range-threshold` is not overridden.
  - The projection break-even is the bytes that a projection must skip before
    an object is read ranged and not whole. It is the larger of that threshold
    and three request costs: 18,900,000 bytes. Three, because a ranged read of
    a narrow projection issues about four GETs per object against one for a
    whole read, so it pays only when the bytes it skips exceed the cost of the
    three extra requests.
  - A one-column statement over 35 MB objects therefore reads only its column
    ranges.
  - The blocks of every object of 18,900,000 bytes or less are still read in
    one whole-object GET, with no tail probe. This applies on the
    whole-segment fast path, on the planned route (any statement that the
    whole-segment fast path refuses) and on the stream-directory read of the
    log-series route alike.
  - Above the break-even, a statement on the planned route probes the tail and
    directories of an object first. A wide projection whose surviving ranges
    cover at least 75% of the object then reads it whole anyway, because that
    route does not weigh the projected fraction.
- **A profile with free bytes and no timings.** The rate saturates. Every
  object is read whole, with no tail probe and no ranged read. The routing
  threshold saturates with it, which overrides `--logs-block-range-threshold`.
  When that flag was set explicitly, startup logs a WARN that names the value
  it overrode.
- **Egress prices.** The derived rate is small. For a profile priced at $0.40
  per million GET-class requests, $0.09/GiB transfer and $0.01/GiB retrieval,
  the derivation gives 4,294 bytes per saved request. That is the worked value
  that the request-cost decision record cited under Background uses. It is the
  same order of magnitude as the dollar break-even computed further down this
  page, and three orders of magnitude below the 1.8 MiB constant.

### An explicit flag

**An explicit flag overrides that derivation.** When set, it wins over the
rate of the policy under every policy, `cost-based` included. It is an expert
escape hatch, not a tie-breaker. A flag pinned to 1,887,437 is therefore not a
no-op:

- On an egress-billed deployment, it replaces a derived rate at the dollar
  break-even of that deployment with one three orders of magnitude above it.
  The deployment then buys requests with bytes that it is billed for.
- On a transfer-free deployment under `cost-based`, it replaces the 6,300,000
  bytes of the time term and drops the projection break-even that comes with a
  derived rate. The routing threshold is the break-even again. A narrow
  projection that skips more than 524,288 bytes reads ranged, including
  objects up to 18,900,000 bytes that the time term reads whole.

Leave the flag unset unless a measurement on your own deployment says that the
derived value is wrong for it.

Startup logs which way the value resolved. The `logs fetch policy resolved`
line carries the effective request cost and its source, either the explicit
flag or the derivation of the policy. The policy and profile flags are
documented in
[Operations: configuration](operations/configuration.md#logs-fetch-policy-and-store-cost-profile).

### What a change does

A higher value makes the engine value a request more: fewer segment fetches
take the ranged route, more read whole objects, request count falls and bytes
rise. A lower value does the reverse. On `ravel-server`, two of the three
decisions below also need `--logs-block-range-threshold` raised with it before
the routing moves (see
[Three decisions from one value](#three-decisions-from-one-value)).

The decision is per segment, so one statement that spans many segments can
take both routes within a single query. For that reason the counters report
opens by shape, not statements by shape.

The setting never changes the answer of a query. It selects which read path
fetches the bytes. The rows returned are identical at every value, and only
request counts, byte counts, and timing differ.

### Three decisions from one value

The value drives three decisions in the logs fetch layer:

1. **The coalescing gap**: two wanted extents separated by less than one
   request cost fuse into a single GET.
2. **The pre-probe whole-object crossover**: an object at or below five
   request costs is read whole, because the ranged protocol adds roughly that
   many round trips and cannot save enough bytes below that size to pay for
   them.
3. **Projection routing on the whole-segment fast path**: a predicate-free
   scan opens by column chunk only when the bytes that its projection skips
   clear that same crossover.

One field drives all three, so a recalibration for the store moves the three
thresholds together. Two floors bound the low end: a 64 KiB coalescing gap and
a 512 KiB whole-object crossover. A very small value therefore clamps and does
not produce a one-block GET storm. The 4,294-byte egress derivation above is
below both, so that deployment gets the floors, not the raw rate.

One qualification on the second and third decisions changes what a higher
value does. `ravel-server` always hands the fetcher its resolved
`--logs-block-range-threshold`. The fetcher uses that threshold verbatim as
the pre-probe crossover and does not derive three request costs from this
value, except where the resolution also hands it a projection break-even:

| Case | The second and third decisions follow |
|---|---|
| `cost-based` derives a finite rate from the profile (the shipped flag set) | The projection break-even, which replaces the threshold in both decisions. It is the larger of the routing threshold and three request costs, so the decisions follow the derived rate (18,900,000 bytes at the reference profile). |
| `byte-minimal` or `latency-first`, or this flag set explicitly | The routing threshold. A higher value here without a higher `--logs-block-range-threshold` moves the coalescing gap and leaves the other two where the threshold puts them. |
| A saturated rate | Whole-object reads. A saturated rate saturates the routing threshold too. |

Two properties follow from what the number is:

- It is a property of the **store and the instance**, that is round-trip
  latency and single-stream bandwidth at the fetch concurrency in use, not of
  the RLOG format. A different store, a cross-region bucket, or a different
  `--fetch-concurrency` has a different right value. A change to fetch
  concurrency changes this break-even along with it.
- The `logs-` prefix is literal. Metric (RSEG) reads use fixed gap and
  crossover constants that are not request-cost-derived, and do not respond to
  this flag.

### Modeled cost of the trade

Every dollar figure in this section is list-price modeling, not a measured
bill. The amounts are computed from counted requests and counted bytes against
published us-east-1 list prices, never read off an invoice. The absolute
amounts for a single benchmark pass are small, so they matter as a rate at
production query volume, not as cents.

The measured example is one experiment on the ClickBench corpus (42
statements, cold, reference box, with the pass procedure in
[clickbench-aws-runbook.md](../internal/clickbench-aws-runbook.md)). It
compares reading every segment whole against routing narrow projections to
ranged reads:

```text
cold requests   203,243 whole    751,409 ranged    (+270%)
cold bytes       403.97 GB whole  194.19 GB ranged  (-52%)
```

The ranged pass moved half the bytes with 3.7x the requests. It finished
faster despite the extra requests, at 222.19 s cold. Modeled at us-east-1 list
prices, it costs about 2.2x more in request charges than the whole-object
pass.

Those two fenced lines and that 222.19 s time are one pass. Its fetch
concurrency is not recorded, and the whole-versus-ranged comparison is
sensitive to it. Read the three figures as a single experiment at an
unrecorded concurrency, and reproduce the procedure through the runbook linked
above.

The ranged pass can be both faster and more expensive because of an asymmetry
in the price sheet:

- Same-region S3-to-EC2 transfer is not billed. On that deployment shape,
  trading bytes for requests spends a billed resource to save a free one.
- Where transfer is billed (cross-region, internet-facing, or an object store
  that charges egress) the sign flips. At list egress near $0.09/GB against
  GETs near $0.0004/1000, the dollar break-even is around 4.4 KB per request,
  three orders of magnitude below the 1.8 MiB latency break-even.

Give those prices to `--store-cost-profile`, and the cost-based resolution
lands in that same range. An unset flag is therefore the cost-preferring
choice there. A flag pinned to the 1.8 MiB constant does the opposite. That
constant is three orders of magnitude above the dollar break-even, so it
spends billed egress to save requests that are nearly free. The right value
depends on the billing shape of the deployment, which is why this is a flag
and not a constant.

### Choosing a value

The options are in order, cheapest lever first:

1. **Leave it unset.** This costs nothing. At the shipped reference profile it
   lets the fetch-policy resolver take the time term, 6,300,000 bytes, with
   its 18,900,000-byte projection break-even. Narrow projections of objects
   large enough to skip more than that read ranged, and everything else reads
   whole.
   - Setting the flag at all replaces that rate, and the break-even with it.
   - An operator who wants ranged reads wherever they save any bytes has to
     ask for them explicitly through the fetch-policy flag (see
     [caching.md](caching.md)). The fetch-policy record listed under
     Background carries the current whole-versus-ranged ratio.
   - A different store, region, or fetch concurrency shifts the break-even for
     this knob independently of that choice. The two timings of the profile
     are for that case.
2. **Leave it unset on an egress-billed backend** too, and give the prices of
   that backend to `--store-cost-profile` instead. Unset with those prices
   loaded, the cost-based resolution derives a rate at the dollar break-even
   of that deployment. The two floors clamp it from there, so there is no
   lower value worth inventing. Do not pin the flag to the 1.8 MiB fallback
   constant to match what you believe the default is. On this backend that
   raises the rate three orders of magnitude above the break-even. The
   coalescing gap follows it all the way up, fuses extents up to 1.8 MiB
   apart, and moves bytes that this deployment is billed for.
3. **Raise it on a request-billed, transfer-free backend** (same-region S3)
   when you want every object read whole.
   - On the shipped `cost-based` policy at the reference profile, option 1
     already reads whole every object that the 18,900,000-byte break-even
     covers. Setting the flag replaces the derived rate and drops that
     break-even, so the routing threshold decides again.
   - Set it at or above the largest segment object that *any* tenant this
     process serves writes. The flag is process-wide, so the largest object of
     a single tenant is the wrong unit, and any tenant that holds bigger
     objects keeps routing ranged.
   - No format-level object-size cap exists to read this from. Object size
     comes from `--batch-rows` and `--target-bytes` at write time and is
     observable per tenant, so measure it and round up.
   - Raise `--logs-block-range-threshold` with it. With the flag set, that
     threshold, not the three-request-costs derivation, is the pre-probe
     crossover on the server. A raise here alone moves the coalescing gap and
     leaves the routing where the threshold puts it.
   - With both raised, every candidate segment at or under
     `--logs-max-fetch-run-bytes` becomes one GET, and a larger one becomes a
     few sequential covering GETs.
   - The cost is the other column of the comparison above: roughly twice the
     bytes, and the cold-latency win given back.

The flag is read at startup only, like the other read-path knobs in
[query.md](query.md#operator-configurable-budgets-server-flags). It sits
inside `--logs-block-range-threshold`, which selects which fetcher entry point
an object takes before this value governs how that fetch behaves. That
threshold is itself a resolved value, not whatever was passed: a saturated
request cost overrides it. On the shipped flag set the rate is finite, so the
threshold keeps its 512 KiB default. Above it, the projection break-even
decides whether the read is ranged or whole.

To see which way your queries route, read the `fast_path_whole_object_segments`
and `fast_path_ranged_segments` plan metrics of the logs scan from an
`EXPLAIN ANALYZE`, beside the per-operation request and byte counts. A large
ranged-open share on statements that move few bytes, on a backend that bills
requests, is the signal that this value is set for the wrong objective.

No ClickBench pass under the cost-preferring setting is published. The figures
above compare the two read shapes, not this flag. A high value also collapses
the ranged reads on the predicate path that were active on both sides of that
comparison. Such a pass lands at or below 203,243 requests and at or above
403.97 GB. Only a measurement says where.

## Per-query cost accounting

Every accounted read reports what it spent on object storage, and Ravel
exports the running totals in the `ravel_query_*` metric family. The
[observability guide](observability.md#per-query-cost) catalogs that family
and shows the PromQL that reads it. This section is the accounting behind the
numbers.

Each accounted query folds one snapshot at completion:

- the object-store requests that it issued
- the bytes that it transferred
- the in-process cache hits and misses attributed to it
- the decompressed sample bytes that it decoded

Beside each actual is a pre-execution estimate of the same quantity. The
estimate is an upper envelope, never a prediction. The planner takes the worst
case wherever it cannot bound a quantity, so a correct estimate lands at or
above the actual, never below it.

An actual divided by its matching estimate gives a ratio at or below 1 in the
healthy case. A ratio above 1 means that the actual exceeded the envelope
meant to bound it. That rules in one of two causes:

- a cost-model gap: the estimate omits a real source of spend
- a runaway query pattern that the model did not anticipate

Nothing rejects a query on that envelope. It is measurement only.

Three gaps limit what the per-query cost family can show:

- A query that fails still folds the cost that it incurred before it failed,
  including a deadline breach. An accounting snapshot cannot yet show which of
  success, error, timeout, or cancellation the query ended in. The exported
  family therefore does not distinguish the spend of a failed query from the
  spend of a successful one today. Until that split is exported, read a sudden
  drop in the completed-query count against steady request logs as failures,
  not as idle capacity.
- A Flight SQL statement records two folds, one per RPC. The plan request
  records the first fold and the fetch request records the second. The
  completed-query counter therefore counts 2 for one logical query, and the
  two folds sum to one whole-query estimate beside the summed whole-query
  actual.
- A Flight fetch that a client abandons after one batch still records its
  partial cost. The stream ends when the client disconnects, so the bytes
  already spent are recorded and count as one query. An unusually low
  cost-per-query ratio on the Flight path can therefore mean early client
  disconnects, not cheap queries.

## Limits of a modeled cost

Every dollar figure in this guide is computed from counted requests and counted
bytes multiplied by a published list price. None of it is read off an invoice.
Two assumptions sit under any such figure, and either can move it:

- The **price sheet**. The figures here use us-east-1 list prices for PUT,
  GET, and transfer. A different region, a negotiated rate, the per-request
  price of a different provider, or an egress charge that the list model omits
  all change the total. The change is sometimes enough to flip which lever is
  cheapest, as the same-region-versus-egress reversal above shows.
- The **workload**. Tenant count, signal mix, and query shape are inputs to
  the formula and the per-query accounting, not constants. A projection tuned
  for one corpus predicts a different bill on another.

The counted requests and bytes are real. They come from the object-store call
counters and the per-query fold, not from a guess. The dollars are a model
laid over those counts.

Use a projection to compare levers and to size a deployment before the first
invoice. Then reconcile it against a real bill once traffic is flowing. Treat
a persistent gap between the two as a signal that one of the two assumptions
no longer holds.

## Background

The two-object commit protocol and request-cost reduction through flush
cadence and ingest affinity: ADR-0076. The idle flush byte floor, the
sub-floor hold, and the buffered-mode durability window it widens: ADR-1737.
Per-query cost accounting and the `/metrics` cost family: ADR-0044. The read-side request-cost knob and its
whole-versus-ranged routing: ADR-0904. The read-side request budget derived
from shard count and cadence: ADR-0075. Fetch concurrency: ADR-0088. The
cost-based-versus-latency-first fetch policy and its measured trade: ADR-1196.
The store cost profile, and the resolution that turns a fetch policy into the
byte quantities the fetch layer runs on, including the precedence an explicit
`--logs-request-cost-bytes` takes over a derived rate: ADR-0996.
