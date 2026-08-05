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

### 3. A pre-execution cost estimate, in two parts

The estimate is an upper envelope, never a prediction. Where the planner
cannot bound a quantity it takes the worst case. Under-estimating is the
failure that matters, because a later ADR will reject queries on this
number, and a query admitted on a low estimate is exactly the runaway the
limiter exists to stop.

It has two parts, computed at two different moments, because one moment
cannot cover both.

**The catalog term, computed before `Catalog::resolve` runs.** Resolve is
itself expensive: it LISTs one prefix per (shard, ingest-hour bucket) in
the window, and GETs the records it finds. With no snapshot HEAD, because
folding is disabled or has not run yet, the listing start is the window
start, so a 24 h window at `shard_count = 16` is several hundred LISTs.
Those requests are bounded before resolve by inputs the planner already
has: `shard_count` and the number of hour buckets the padded window spans.

**The segment term, computed after resolve returns a pinned snapshot.**
`SegmentRef` carries `object_size`, `sample_count`, and `series_count`,
and the fetch plan is deterministic given the matchers.

The original form of this decision specified only the second part, which
was wrong and unbuildable: an estimate computed after resolve structurally
cannot bound resolve's own spend, so wiring the catalog's accounting in
would make actual exceed estimate on exactly the queries a limiter most
needs to catch. Amended 2026-08-02 after a checkpoint review found the
contradiction.

**Per-sample cost is per value kind, not one constant.** A single
bytes-per-sample bound cannot hold: a scalar sample decodes to 16 bytes
while one native-histogram sample decodes to roughly
`45 + 8 * (buckets + spans + custom_values)`, and no reader limit caps the
bucket count. A constant loose enough for a wide histogram is orders of
magnitude too loose for scalars, and one tight enough for scalars
under-shoots histograms without bound. The estimate therefore derives its
per-sample term from the segment's own value kinds.

Each query records the estimate and the actual side by side, so the
estimate's accuracy is itself a measurable quantity before anything
depends on it.

**Amendment, 2026-08-05 (issue #635).** Decision 3's catalog term becomes
load-bearing: it now gates admission. Rejected alternative 4 ("no query is
rejected on a cost ceiling") is narrowed, not reversed wholesale. The
*segment* term and every byte- and sample-per-value weight remain
measurement-only, exactly as this ADR shipped them. Only the *catalog* term
acquires an enforced ceiling, and only because it is the one part of the
estimate that is both computed before any work is done and, left unchecked,
an availability incident on its own: a client-supplied `start` of `0.0` makes
`Catalog::resolve` issue one LIST per (shard, ingest-hour) from the epoch to
now -- 496,089 LISTs for a single shard as of this writing (issue #634's
measurement), a count that grows by one every wall-clock hour, forever, and
multiplies by `shard_count`.

The catalog term stays an upper envelope and never a prediction. The ceiling
gates the *worst case* resolve would face with no snapshot watermark to
shorten the listed suffix; a folded tenant's real LIST count is far lower. The
ceiling therefore refuses only windows whose worst case is itself
unsustainable, and never narrows a window that folding would have made cheap.

*Fail-closed, refuse rather than clamp.* A window whose estimate exceeds the
ceiling is refused with a typed error carrying the estimate and the limit; it
is never silently narrowed to fit. A clamped window would return a partial
result that looks complete, which contradicts this system's posture of exact
semantics by default and approximation only when it is opt-in and visible. The
caller learns by how much it was over and can narrow its own window and retry.

*Default ceiling.* `CatalogConfig::max_catalog_list_requests` defaults to
100,000 catalog requests (LISTs plus the `SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`
constant). The estimate is
`shard_count * hour_buckets + SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`, so the
permitted window width falls as `shard_count` rises:

- at `shard_count = 1`: about 100,000 hour-buckets, roughly an 11-year window;
- at `shard_count = 16`, a realistic mid-size tenant: about 6,250
  hour-buckets, roughly 8.5 months;
- at `shard_count = 64`: about 1,560 hour-buckets, roughly 65 days.

The epoch-width query that motivated this (about 496,089 LISTs at
`shard_count = 1`, and about 7.9 million at `shard_count = 16`) is refused
with several times the ceiling to spare, while every ordinary query passes
with room left over: the one-hour default window of the SQL and PromQL
endpoints costs on the order of ten catalog requests. A deployment whose
shard count and retention make a wider worst case routine raises the field;
the default is sized to catch runaways, not to cap a tuned deployment.

*Configurability.* The ceiling is a field on `CatalogConfig`
(`max_catalog_list_requests`), so any `Catalog` constructor can set it and
`services/ravel-server` inherits the default through
`CatalogConfig::default()`. It is not yet a server CLI flag; wiring one is a
mechanical follow-up if an operator needs to tune it without a rebuild.

*One choke point.* The guard is enforced inside `Catalog::resolve` (the single
funnel all four public resolve entry points delegate to), before any LIST is
issued, so it covers every caller -- SQL, PromQL, exemplars, analytics -- by
construction rather than one endpoint at a time. The estimate and the limit
ride on the typed error only; per section 4's closed label allowlist they are
never emitted as a metrics label.

**Amendment, 2026-08-05 (issue #636, ADR-0056).** ADR-0056 replaced the
per-`(shard, ingest-hour)` LIST loop with a single per-shard recursive prefix
LIST for wide windows. This changes what the catalog term above must mean, in
two coordinated ways, and the amended contract is:

- The formula is unchanged: `estimated_catalog_requests` still returns
  `shard_count * hour_buckets + SNAPSHOT_WINDOW_REQUESTS_UPPER_BOUND`, the
  per-bucket worst case. It remains a **true upper envelope** of whatever
  traversal runs -- the prefix scan issues `O(objects / page_size)` LISTs,
  strictly fewer than the per-bucket loop's `shard_count * hours` base for any
  window the prefix scan is chosen for, so the bound holds over both paths
  under the same sparse-bucket assumption this decision already rested on (at
  most `page_size` objects per bucket; the formula already counts one LIST per
  bucket and ignores intra-bucket pagination). It stays the number threaded
  into cost accounting.
- It is **no longer the admission gate** for wide windows. The pre-execution
  refusal of `estimate > max_catalog_list_requests` counted hours, and after
  ADR-0056 a wide window's real cost is object-bounded, not hour-bounded, so
  refusing on the hour count would reject a prefix scan issuing three orders
  of magnitude fewer requests -- a cheap query wrongly refused. Instead, a
  window whose per-bucket cost would exceed the ceiling is **routed to the
  prefix scan** (which does not amplify), and that scan carries a **runtime
  LIST cap** at the same `max_catalog_list_requests`: it aborts with
  `WindowTooWide` before issuing a page that would exceed the ceiling. The
  hard guarantee "a single resolve never issues more than
  `max_catalog_list_requests` catalog LISTs" is preserved -- now enforced at
  runtime on the one path whose cost is not knowable before listing. A
  wide-but-sparse window that this decision (as amended for #635) would have
  refused is now served; only a scan whose actual object volume is
  unsustainable is refused. The typed error, its fields, and its HTTP-422
  mapping are unchanged.

The envelope property `estimated_catalog_requests >= actual requests issued`,
the property this ceiling rests on, is verified for both paths and several
window shapes in `crates/ravel-catalog/tests/resolve_prefix_traversal.rs`.

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

**Amendment, 2026-08-04 (issue #603).** ADR-0051 section 6 added an
eighth permitted label, `reason`, for the admission family's
`ravel_admission_rejected_total`. That decision belongs to ADR-0051; this
paragraph records it so that the list above stays exhaustive rather than
becoming the stale half of a contradiction. A reader who treated this
section as normative and the code as wrong, or the reverse, would have
been correct either way, which is why the list is amended in place
instead of being left to the later ADR alone.

`reason` meets the same two tests every other key meets. Its values are a
closed enum, so cardinality is bounded by construction, and none of them
carries tenant identity. Extending the list is therefore a widening of
the allowlist, not an exception to it.

The `reason` value set is smaller in code than in ADR-0051 section 6.
That section names six values; `RejectReason` in
`services/ravel-server/src/metrics.rs` has three (`byte_rate`,
`series_rate`, `series_cap`). The other three have no counter to render,
and a rendered series that no data source can fill is worse than an
absent one. They join the enum when their counters do.

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
