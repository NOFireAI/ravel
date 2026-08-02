# ADR-0044: Per-query cost accounting, a bounded metrics endpoint, and a pre-execution cost estimate

Status: Accepted (2026-08-02)

## Context

Ravel's query budgets are counts, not costs. `EngineConfig`
(crates/ravel-query/src/config.rs) caps `max_segments` at 1024,
`max_series` at 10,000, `max_samples` at 10,000,000, and the wall
deadline at 30 s. None of these bound the resource that Ravel actually
spends. S3 is the only durable backend, so a query costs request calls
and transferred bytes. A query over 1000 small L0 objects and a query
over 1000 compacted 64 MiB L1 parts have the same `max_segments` cost and
differ by four orders of magnitude in bytes read and in money.

The measurement to prove this is already in the repository. The metric
index phase 4 benchmark found that for a folded, selective query,
per-segment fetch cost is 97.6% of requests. The
`selective_read_accounting` bench models the within-segment GET and byte
counts. Neither number is available at runtime for a real query.

Three concrete gaps block any cost-aware work:

1. **Store counters are process-global.** `StoreMetrics`
   (crates/ravel-object-store/src/instrument.rs:357) holds monotonic
   totals with no per-tenant and no per-request dimension, by design. One
   `InstrumentedStore` is shared by ingest, fold, compaction, PromQL, and
   SQL (services/ravel-server/src/lib.rs:245), so a GET issued by the
   compactor is indistinguishable from one issued by a query.
2. **Nothing surfaces any counter.** There is no `/metrics` route and no
   metrics or exporter crate in the workspace. `build_store` returns the
   `Arc<StoreMetrics>` handle and `main.rs:107` binds it to
   `_store_metrics` and drops it.
3. **Query stats are two fields.** `QueryStats`
   (crates/ravel-query/src/engine.rs:52) carries `segments_fetched` and
   `segments_pruned`. The richer per-segment `FetchStats` is computed and
   then discarded at engine.rs:585.

Enforcement is out of scope here on purpose. A limiter calibrated on
guessed weights is an availability incident. This ADR delivers the
measurement that a later enforcement ADR calibrates against.

## Decision

### 1. A per-request accounting handle in `ravel-types`

`QueryAccounting` is a cheap-to-clone handle over atomic counters,
created once per query and threaded through every component that touches
the store on that query's behalf. It lives in `ravel-types` because both
`ravel-catalog` and `ravel-query` must write to it and the crate
dependency order runs types before catalog before query.

Counters, all `u64`:

- `s3_requests` and `s3_bytes`, split by operation kind (get, list, head)
- `cache_hits`, `cache_misses`, `cache_bytes` for the in-process caches
  that already exist in `ravel-catalog`
- `decompressed_bytes`
- `segments_opened`, `segments_pruned`, `series_matched`
- `bytes_reused`, counting bytes served from the fetcher's own
  `FetchedRegions` without a second GET
- `peak_intermediate_bytes`, recorded as a maximum, not a sum

The handle is passed explicitly as a parameter. It is not a thread-local
and not a task-local, because the query path fans out across `join_all`
and a task-local would silently attribute a sibling query's work.

### 2. Accounting is recorded at existing funnels only

Three funnels already exist and every store call on a query's behalf
passes through one of them:

- `SegmentFetcher::guarded_get` (crates/ravel-query/src/fetcher.rs:307)
- `Catalog::guarded_get` and `Catalog::guarded_list_all`
  (crates/ravel-catalog/src/catalog.rs:96 and :111)

`LogSegmentFetcher` (crates/ravel-query/src/log_fetcher.rs:288) calls the
store directly with no funnel. This ADR adds one, so the log path is
accounted like the metric path rather than being silently free.

Recording at funnels rather than at call sites means a new call site
cannot forget to account for itself, and it keeps the change small.

### 3. A pre-execution cost estimate

After `Catalog::resolve` returns a pinned snapshot and before any page
fetch, the planner computes a `CostEstimate`. Every input is already
present: `SegmentRef` carries `object_size`, `sample_count`, and
`series_count` (crates/ravel-catalog/src/snapshot.rs:39), and the fetch
plan is deterministic given the matchers.

The estimate is an upper envelope, never a prediction. Where the planner
cannot bound a quantity it takes the worst case. Under-estimating is the
failure that matters, because a later ADR will reject queries on this
number, and a query admitted on a low estimate is exactly the runaway the
limiter exists to stop.

Each query records the estimate and the actual side by side, so the
estimate's accuracy is itself a measurable quantity before anything
depends on it.

### 4. A `/metrics` endpoint with hand-written Prometheus exposition

`ravel-server` serves `GET /metrics` in every mode, including maintain,
next to `/healthz` and `/readyz`. It renders the existing snapshot
structs (`StoreMetrics`, `IngestMetrics`, log and span metrics, catalog
anomaly counters) plus the new query accounting aggregates into
Prometheus text exposition format.

The renderer is written in this repository rather than pulled from a
metrics crate. The exposition format is a few hundred lines of text
generation, every counter Ravel has is already a snapshot struct with a
fixed shape, and a registry abstraction would add a second place where
label sets are decided. Keeping the renderer in-tree means the label
allowlist below is enforced by the type system, not by convention.

Permitted labels, exhaustively: `tenant_hash`, `signal`, `mode`, `op`,
`error_kind`, `workload_class`, `level`. Every one is a closed set or a
fixed-width hash. Per-tenant series are emitted only for configured
tenants; anything else folds into `tenant_hash="other"`. `shard` is not a
label. Query text, metric names, label values, stream ids, trace ids, and
object keys are never labels.

### 5. Tracing spans on the query path

Each crate instruments its own phases with `tracing` spans: catalog
resolve, segment open, catalog decode, page fetch, decode, evaluate. Span
fields carry the same bounded values the metrics labels allow, plus
per-span byte and request counts. There are no spans on the query path
today, so a slow query cannot be attributed to a phase.

## Rejected alternatives

1. **Derive per-query cost from `StoreMetrics` deltas.** Read the global
   counters before and after a query and subtract. Rejected: ingest,
   fold, compaction, and every concurrent query write to the same
   counters, so the delta is meaningless under any real load. It would
   also be exactly wrong in the case that matters most, a busy tenant.

2. **A task-local or thread-local accounting context.** Rejected: the
   fetcher fans out with `join_all` and the catalog with `buffered`, so
   work for one query runs on tasks the query did not spawn. Explicit
   parameter passing is more code and is correct.

3. **Add the `prometheus` or `metrics` crate.** Rejected for now: it
   introduces a registry whose label sets are decided at call sites
   scattered across the workspace, which is the mechanism by which
   observability systems acquire unbounded self-telemetry. Ravel's own
   cardinality rule is the reason this ADR exists at all. If a future
   requirement needs OpenMetrics exemplars or histograms Ravel cannot
   render, that is its own ADR.

4. **Enforce budgets in this change.** Rejected: weights calibrated
   against no measurement produce false rejections, and a rejection is
   customer-visible. Measurement first, one release of observed data,
   then enforcement in its own ADR.

5. **Instrument at every store call site instead of at the funnels.**
   Rejected: it spreads accounting across dozens of sites and makes
   omission the default failure mode. The funnels exist; use them.

6. **Emit per-shard metrics.** Rejected: shard count times tenant count
   times operation count is unbounded in the dimension Ravel controls
   least. Shards are aggregated; per-shard detail belongs in a debug
   endpoint, not in the scrape.

## Consequences

- Every component that reads the store on a query's behalf takes a
  `QueryAccounting` parameter. This is a wide but mechanical signature
  change across `ravel-catalog`, `ravel-query`, and `ravel-sql`.
- `/metrics` is a new public surface, and it is **unauthenticated**, like
  `/healthz` and `/readyz`. Ravel has no global auth layer: every other
  route authenticates inside its own handler through the tenant resolver,
  and a scrape has no tenant to resolve. Operators must not expose the
  listener to untrusted networks.

  This is safe only while no sample carries a `tenant_hash`. The moment
  per-tenant series exist, an unauthenticated scrape discloses one
  tenant's volumes and error rates to anyone who can reach the port.
  Per-tenant series are therefore blocked on an authentication decision
  for this route, recorded on the issue that adds them.
- The estimate is recorded but never enforced. Nothing in this change can
  reject a query that runs today.
- The cost model's weights are not fixed here. Recording requests, bytes,
  cache hits, and decompressed bytes separately means a later ADR can
  choose weights against real data rather than committing to a formula
  now.
- `put_multipart` remains uncounted (instrument.rs:495). Compaction is
  not on the query path, so it is out of scope; the gap is recorded here
  so it is not mistaken for an oversight.
- No frozen format changes. No persistent object changes. Nothing in the
  commit protocol, the key layout, or any segment format is touched.
