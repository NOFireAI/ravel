# Predicting the S3 request bill

Storage is a rounding error in Ravel's cost. Request charges dominate: at a
modeled 100-tenant, 1 TB/day workload, ADR-0076 estimated roughly $7-8k/month
of request fees against roughly $100-150/month of storage, and at a
single-tenant deployment request fees run over 98% of the total bill. This
guide gives an operator the formula and the levers to predict and control
that bill before deploying, not after the first invoice.

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
