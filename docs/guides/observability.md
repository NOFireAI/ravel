# Observability

Ravel measures its own work and exports the counts at `GET /metrics`. The
route renders one Prometheus text exposition document. It carries what Ravel
spent and what Ravel refused, per object-store operation, per ingest signal,
per tenant bucket, and per query workload. It never carries a sample value, a
query text, an object key, or a trace id.

One rule decides what belongs at `/metrics`: a number that summarizes the
process itself is a metric, and a number that answers a question about stored
telemetry is a query. Request counts, byte counts, cache outcomes, and error
kinds are metrics. A tenant's actual time series are a query against
`/api/v1/query` or `/api/v1/sql`. The two never mix on this route.

## This guide and the operations guide

This guide is the catalog of `/metrics`. It names every metric family, states
what each number means, and shows how to read the query-cost numbers. Read it
to understand a sample you see on the route.

The [operations guide](operations.md) is the runbook. It holds the default
alert rules, the admission-usage family in operational depth, the mass-orphan
breaker runbook, and the durable GC config. Read it to decide what to page on
and how to respond. This guide does not repeat those procedures. It links to
them.

## The route

`ravel-server` serves `GET /metrics` in every mode, next to `/healthz` and
`/readyz`. The route reads in-memory atomic counters only. It makes no
object-store call, so a scrape costs nothing on the store.

The route is unauthenticated, like the two health routes. A scrape has no
tenant to resolve, so no handler can authenticate it. An operator must keep
the listener off untrusted networks (ADR-0044 consequences).

Every sample carries a `mode` label. One Prometheus job can then scrape a
fleet of `all`, `gateway`, `query`, and `maintain` processes without their
series colliding.

## The label allowlist

The renderer can attach only these label keys: `tenant_hash`, `signal`,
`mode`, `op`, `error_kind`, `workload_class`, `level`, and `reason`. Section 4
of ADR-0044 fixed the first seven. Section 6 of ADR-0051 added `reason` for
the admission family. Each key carries a closed enum value or a fixed-width
hash. The `level` key is reserved and no family renders it today.

The allowlist is closed for two reasons. The first reason is cardinality. An
unbounded label value multiplies the series count without a ceiling, and the
scrape then grows with traffic Ravel does not control. The second reason is
tenant-identity disclosure. A raw tenant name or a raw query text on an
unauthenticated route would leak one tenant's activity to anyone who can reach
the port. A closed enum cannot carry either failure.

The renderer enforces the allowlist at compile time. A ninth label key is a
compile error across the module. A `shard` label is deliberately absent,
because shard count times tenant count times operation count is unbounded in
the dimension Ravel controls least.

### The `tenant_hash="other"` fold

Two families can carry a `tenant_hash` label: the admission family and the
per-query cost family. By default every tenant folds into the single bucket
`tenant_hash="other"`, and that bucket sums every folded tenant's counters.
The scrape then holds one series per (signal or workload class), never one per
tenant, regardless of how many tenants send traffic.

The `--metrics-tenant-labels` flag opts out of the fold (ADR-0051 section 6).
With the flag on, each configured tenant keeps its own real `tenant_hash`, and
the operator accepts both the higher cardinality and the disclosure on this
unauthenticated route. Turn the flag on only where the scrape network is
trusted. Every tenant without an explicit admission limit still folds into
`other` even with the flag on.

## Metric families by subsystem

Every name below comes from `services/ravel-server/src/metrics.rs`. A family
that has no data source in the current mode is omitted, not rendered as zero.
The ingest families are absent in `query` and `maintain` mode. The two
maintenance families and the tenant-discovery family render only in `maintain`
mode. The cache family is absent under `--disable-cache`.

### Object store (`ravel_store_*`)

Labels: `mode`, `op`, and `error_kind` on the error counter only.

| Metric | Meaning |
|---|---|
| `ravel_store_calls_total` | Completed object-store calls, by operation. |
| `ravel_store_ok_total` | Object-store calls that returned Ok, by operation. |
| `ravel_store_errors_total` | Object-store call failures, by operation and error kind. |
| `ravel_store_bytes_total` | Bytes returned by a successful get or offered by a put, by operation. |
| `ravel_store_latency_seconds` | Object-store call latency histogram, by operation. |

The `op` label carries one value per object-store operation. The latency
histogram renders one `_bucket` series per bound with an `le` label, plus a
`_sum` and a `_count` series. The `+Inf` bucket equals the `_count`.

These counters are process-global. They sum every caller, so an ingest get and
a query get land in the same series. Per-query attribution lives in the
per-query cost family below.

### Ingest pipelines (`ravel_ingest_*`)

Labels: `mode` and `signal`. The `signal` label carries `metrics`, `logs`, or
`spans`.

| Metric | Meaning |
|---|---|
| `ravel_ingest_flushes_by_size_total` | Flushes opened because the tenant buffer reached target_bytes. |
| `ravel_ingest_flushes_by_age_total` | Flushes opened because the tenant buffer aged past max_flush_delay. |
| `ravel_ingest_flushes_manual_total` | Flushes opened by an explicit, shutdown, or drop-path drain. |
| `ravel_ingest_put_retries_total` | Retried PUT attempts on the data-object or commit-record path. |
| `ravel_ingest_abandoned_retry_exhausted_total` | Flushes abandoned by retry-budget or lifetime exhaustion. |
| `ravel_ingest_abandoned_input_rejected_total` | Flushes abandoned because the input could not build a durable object. |
| `ravel_ingest_buffered_bytes_total` | Bytes admitted into shard buffers at enqueue time. |
| `ravel_ingest_buffered_items_total` | Samples, records, or spans admitted into shard buffers. |
| `ravel_ingest_acks_ok_total` | Strict-mode waiters acked with a commit token. |
| `ravel_ingest_acks_err_total` | Strict-mode waiters acked with a write error. |
| `ravel_ingest_collisions_total` | Batches rejected for a series or stream identity collision. |
| `ravel_ingest_shard_deaths_total` | Distinct shard actors observed dead by the router. |

The collisions family carries no `signal="spans"` series. Spans derive no
identity that can collide, so that sample is structurally absent, not zero.

### Catalog integrity (`ravel_catalog_*`)

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_catalog_interlock_violations_total` | Unlisted L0 commit records observed postdating a compaction record in their bucket. |
| `ravel_catalog_compaction_input_set_conflicts_total` | Buckets observed holding two compaction records with different input_set_hash. |
| `ravel_catalog_isolation_breach_total` | Hard-failed queries from a HEAD or postings tenant_hash mismatch or an out-of-prefix listing result. |

The first two counters tally an anomaly the query resolves past. Each
`ravel_catalog_isolation_breach_total` increment is a query that failed with
an explicit isolation-fault error. The operations guide gives its alert rule.

### Tenancy adoption (`ravel_tenancy_v1_unkeyed_adoptions_total`)

Labels: `mode`. Counts buckets this process pinned to the unkeyed tenant hash
when it adopted a pre-ADR-0050 bucket that held `t/` data but no `sys/tenancy`
marker. A nonzero value is the visible signal that the one-time migration
happened.

### Provisioning (`ravel_provisioning_shard_count_mismatch_total`)

Labels: `mode`. Counts dynamic-tenant provisioning checks that failed: a
`shard_count` disagreement, an unreadable record, or a maintain-loop check
catching either (ADR-0050 section 5). The operations guide gives its alert
rule.

### Store reachability (`ravel_store_*`)

Labels: `mode`. Both samples come from the background store-reachability probe
(ADR-0050 section 7).

| Metric | Meaning |
|---|---|
| `ravel_store_reachable` | Gauge. 1 when the probe reports the store reachable, 0 after K consecutive failed probes. |
| `ravel_store_probe_failures_total` | Every failed probe cycle, monotonic, incremented even below the readiness threshold. |

### Maintenance discovery (`ravel_maintain_*`)

Labels: `mode`. Renders only in `maintain` mode (ADR-0048 decision 3).

| Metric | Meaning |
|---|---|
| `ravel_maintain_tenants_discovered` | Gauge. Tenant prefixes storage reported under `t/` on the last successful discovery cycle. |
| `ravel_maintain_tenants_maintained` | Gauge. Discovered tenants actually maintained this cycle, after any flag restriction. |
| `ravel_maintain_tenant_discovery_failures_total` | Maintenance cycles skipped because tenant discovery itself failed. |

### Maintenance safety (`ravel_maintain_*`)

Labels: `mode`, plus `signal` on three of the four. Renders only in `maintain`
mode (ADR-0048 decisions 1, 4, and 6). These carry no `tenant_hash` label.

| Metric | Meaning |
|---|---|
| `ravel_maintain_legal_hold_refresh_failures_total` | Legal-hold refresh failures. Each one skips that tenant's whole maintenance tick. |
| `ravel_maintain_conservation_aborts_total` | Compaction publishes aborted by the record-count conservation gate, by signal. |
| `ravel_maintain_orphan_breaker_tripped_total` | Orphan-GC mass-orphan circuit breaker trips, by signal. |
| `ravel_maintain_orphans_withheld` | Gauge. Orphan candidates withheld by the most recent sweep pass, by signal. |

The operations guide gives the alert rules and the breaker runbook. A zero
value on the `orphans_withheld` gauge does not mean a prior trip was resolved.

### Read cache (`ravel_cache_*`)

Labels: `mode`. Absent under `--disable-cache` (ADR-0046).

| Metric | Meaning |
|---|---|
| `ravel_cache_hits_total` | Read-cache lookups served from the cache. |
| `ravel_cache_misses_total` | Read-cache lookups not found in the cache. |
| `ravel_cache_bytes_served_total` | Bytes served from the cache on a hit. |
| `ravel_cache_bytes_admitted_total` | Bytes admitted into the cache after a miss. |
| `ravel_cache_evictions_total` | Entries evicted from the read cache by its S3-FIFO policy. |
| `ravel_cache_disk_errors_degraded_to_misses_total` | Disk-tier reads that found an entry but discarded it as unhealthy rather than a clean miss. |

The request hit rate is `hits / (hits + misses)`. The byte hit rate is
`bytes_served / (bytes_served + bytes_admitted)`. The renderer leaves both
ratios for PromQL to compute.

### Admission (`ravel_admission_*`)

Labels: `mode`, `tenant_hash`, `signal`, plus `reason` on the rejection
counter. This family folds tenants per the rule above. The
[operations guide](operations.md) covers this family in operational depth and
gives its alert rules.

| Metric | Meaning |
|---|---|
| `ravel_admission_active_series` | Gauge. Active series (metrics) or streams (logs) tracked for the active cap, by tenant and signal. |
| `ravel_admission_admitted_total` | Requests admitted past the ingest byte-rate layer, by tenant and signal. |
| `ravel_admission_admitted_bytes_total` | Wire body bytes admitted past the ingest byte-rate layer, by tenant and signal. |
| `ravel_admission_rejected_total` | Admission rejections, by tenant, signal, and reason. |

The `reason` label carries `byte_rate`, `series_rate`, or `series_cap`. The
active-streams count for logs renders under `ravel_admission_active_series`
with `signal="logs"`, not under a separate metric name.

### Per-query cost (`ravel_query_*`)

Labels: `mode`, `tenant_hash`, `workload_class`. Every read surface folds its
per-query cost into this family (ADR-0044, issue #425). Coverage spans
`POST /api/v1/sql`, `POST /api/v1/analytics`, the Prometheus-shaped
`GET /api/v1/query`, `GET /api/v1/query_range`, `GET /api/v1/labels`, and
`GET /api/v1/series`, and every Flight SQL request. The `workload_class` label
carries `interactive` or `background`. Only `interactive` occurs today.

| Metric | Meaning |
|---|---|
| `ravel_query_queries_total` | Completed queries that reported cost accounting. This is the denominator for a per-query average. |
| `ravel_query_s3_requests_total` | Actual object-store requests issued by accounted queries. |
| `ravel_query_s3_bytes_total` | Actual object-store bytes transferred by accounted queries. |
| `ravel_query_cache_hits_total` | In-process read-cache hits attributed to accounted queries. |
| `ravel_query_cache_misses_total` | In-process read-cache misses attributed to accounted queries. |
| `ravel_query_decompressed_bytes_total` | Actual decompressed sample bytes decoded by accounted queries. |
| `ravel_query_estimated_requests_total` | Pre-execution upper-envelope estimate of object-store requests. |
| `ravel_query_estimated_store_bytes_total` | Pre-execution upper-envelope estimate of object-store bytes. |
| `ravel_query_estimated_decompressed_bytes_total` | Pre-execution upper-envelope estimate of decompressed sample bytes. |

## Reading estimate against actual

The estimate is an upper envelope, never a prediction (ADR-0044 section 3).
The planner takes the worst case wherever it cannot bound a quantity. A
correct estimate therefore lands at or above the actual, never below it. The
estimate and the actual render under separate names, so their ratio is
directly computable in PromQL.

Divide an actual by its matching estimate. The requests ratio is
`ravel_query_s3_requests_total / ravel_query_estimated_requests_total`. A ratio
at or below 1 means the actual stayed inside its own upper envelope, which is
the healthy state. A ratio above 1 means the actual exceeded the envelope that
was meant to bound it.

A ratio above 1 rules in one of two causes. The first cause is a cost-model
gap, where the estimate omits a real source of spend. The second cause is a
runaway query pattern the model did not anticipate. Either cause is worth an
operator's attention, because a later admission ADR could reject queries on
this envelope. Nothing in this release rejects a query on it. This is
measurement only.

## Worked examples

Each example is a short procedure. Run the PromQL against Ravel's own
`/metrics`, read the named number, and act on what it rules in or out.

### A slow query

1. Compute average requests per query. Run `rate(ravel_query_s3_requests_total[5m]) / rate(ravel_query_queries_total[5m])`.
2. Read the result. It is object-store requests per query over the window.
3. If the value is high, the query fans out over many objects.
4. Compute store latency. Run `histogram_quantile(0.99, rate(ravel_store_latency_seconds_bucket[5m]))`.
5. If requests per query is high, a wide fan-out rules in as the cause.
6. If requests per query is low and latency is high, a slow store rules in and fan-out rules out.

### One tenant reading far more bytes than the others

1. Confirm the flag. Per-tenant bytes need `--metrics-tenant-labels` on.
2. Rank tenants by byte rate. Run `sum by (tenant_hash) (rate(ravel_query_s3_bytes_total[1h]))`.
3. Read the top `tenant_hash`. It is the tenant whose queries cost the most object-store bytes.
4. If one `tenant_hash` dominates the rest, a single heavy tenant rules in.
5. If every series folds into `tenant_hash="other"`, the flag is off.
6. If the flag is off, turn on `--metrics-tenant-labels` on a trusted scrape network, then repeat step 2.

### A cache that is not helping

1. Compute the request hit rate. Run `rate(ravel_cache_hits_total[5m]) / (rate(ravel_cache_hits_total[5m]) + rate(ravel_cache_misses_total[5m]))`.
2. Read the result. A value near 1 means the cache serves most reads.
3. If the hit rate is low, the cache is not helping.
4. Compute the eviction rate. Run `rate(ravel_cache_evictions_total[5m])`.
5. If evictions are high alongside the misses, an undersized cache rules in. Raise `--cache-max-bytes`.
6. If evictions are near zero alongside the misses, cold or unique reads rule in and undersizing rules out.

## Known gaps

Three gaps limit what the per-query cost family can show. All three are
documented in the [operations guide](operations.md#per-query-cost-accounting-adr-0044-issue-425).

- A query that fails records no cost. A deadline breach, an admission
  rejection, and an execution error all return before the fold, and the error
  type carries no accounting snapshot. Read a sudden drop in
  `ravel_query_queries_total` against steady request logs as failures, not as
  idle capacity.
- A Flight SQL statement records two folds, one per RPC. The plan request
  records the first fold and the fetch request records the second. The
  `ravel_query_queries_total` counter therefore counts 2 for one logical query,
  and the two folds sum to one whole-query estimate beside the summed
  whole-query actual.
- A Flight fetch that a client abandons after one batch still records its
  partial cost. The stream ends when the client disconnects, so the bytes
  already spent are recorded and count as one query. An unusually low
  cost-per-query ratio on the Flight path can therefore mean early client
  disconnects, not cheap queries.
