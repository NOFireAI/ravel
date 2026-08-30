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
breaker runbook, and the durable GC configuration. Read it to decide what to
page on and how to respond. This guide does not repeat those procedures. It
links to them.

The [tracing guide](tracing.md) covers the query-path `tracing` spans. Metrics
here answer "how much" in aggregate across the process. The per-request spans
answer "where the time went" for one query. Read the tracing guide to
attribute a slow query to a phase.

## The route

`ravel-server` serves `GET /metrics` in every mode, next to `/healthz` and
`/readyz`. The route reads in-memory atomic counters only. It makes no
object-store call, so a scrape costs nothing on the store.

The route is unauthenticated, like the two health routes. A scrape has no
tenant to resolve, so no handler can authenticate it. You must keep the
listener off untrusted networks.

Every sample carries a `mode` label. One Prometheus job can then scrape a
fleet of `all`, `gateway`, `query`, and `maintain` processes without their
series colliding.

## The label allowlist

The renderer can attach only these label keys: `tenant_hash`, `signal`,
`mode`, `op`, `error_kind`, `workload_class`, `level`, `reason`, `cache`,
`tier`, and `kind`. Each key carries a closed enum value or a fixed-width
hash. Histogram buckets also carry the format-reserved `le` key. The `level`
key is reserved and no family renders it today.

The allowlist is closed for two reasons. The first reason is cardinality. An
unbounded label value multiplies the series count without a ceiling, and the
scrape then grows with traffic Ravel does not control. The second reason is
tenant-identity disclosure. A raw tenant name or a raw query text on an
unauthenticated route would leak one tenant's activity to anyone who can reach
the port. A closed enum cannot carry either failure.

The renderer enforces the allowlist at compile time. A new label key is a
compile error across the module. A `shard` label is deliberately absent,
because shard count times tenant count times operation count is unbounded in
the dimension Ravel controls least.

### The `tenant_hash="other"` fold

Three families can carry a `tenant_hash` label: the admission family, the
per-query cost family, and the ingest PUT attribution family. By default every
tenant folds into the single bucket `tenant_hash="other"`, and that bucket
sums every folded tenant's counters. The scrape then holds one series per
(signal or workload class), never one per tenant, regardless of how many
tenants send traffic.

The `--metrics-tenant-labels` flag opts out of the fold. With the flag on,
each configured tenant keeps its own real `tenant_hash`, and you accept both
the higher cardinality and the disclosure on this unauthenticated route. Turn
the flag on only where the scrape network is trusted. Every tenant without an
explicit admission limit still folds into `other` even with the flag on.

## Metric families by subsystem

Every name below comes from `services/ravel-server/src/metrics.rs`. Ravel
omits a family that has no data source in the current mode, rather than
rendering it as zero. The ingest families are absent in `query` and `maintain`
mode. The two maintenance families and the tenant-discovery family render only
in `maintain` mode. The cache family is absent under `--disable-cache`.

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

#### Per-tenant PUT attribution

| Metric | Meaning |
|---|---|
| `ravel_ingest_attribution_puts_total` | Object-store PUT requests attributed to completed flushes, by tenant and signal. |

Labels: `mode`, `tenant_hash`, `signal`. This is the answer to "which tenant is
generating the PUT bill". Each completed flush charges 2 PUTs (a data object
and a commit record) to the flushing tenant, tracked per signal by a bounded
top-K structure in the ingest router (`MAX_TRACKED_TENANTS = 1024`, see
`crates/ravel-ingest/src/attribution.rs`). That top-K bound protects the
router's internal accounting. The `tenant_hash` label on this family is
bounded separately, by the same `--metrics-tenant-labels` allowlist and the
same `tenant_hash="other"` fold described above. A tenant outside the
allowlist never gets a series of its own here, regardless of how much it
contributes to the top-K table.

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
when it adopted a legacy bucket that held `t/` data but no `sys/tenancy`
marker. A nonzero value is the visible signal that the one-time migration
happened.

### Provisioning (`ravel_provisioning_shard_count_mismatch_total`)

Labels: `mode`. Counts dynamic-tenant provisioning checks that failed: a
`shard_count` disagreement, an unreadable record, or a maintain-loop check
catching either. The operations guide gives its alert rule.

### Store reachability (`ravel_store_reachable`, `ravel_store_probe_failures_total`)

Labels: `mode`. Both samples come from the background store-reachability probe.

| Metric | Meaning |
|---|---|
| `ravel_store_reachable` | Gauge. 1 when the probe reports the store reachable, 0 after K consecutive failed probes. |
| `ravel_store_probe_failures_total` | Every failed probe cycle, monotonic, incremented even below the readiness threshold. |

### Durable auth refresh (`ravel_durable_auth_*`)

Labels: `mode`. Renders only when the process built a durable `sys/auth`
resolver, that is `--tenant-hash-key-file` set in a request-serving mode
(`all`, `gateway`, `query`). Ravel omits it otherwise. All three come from the
background refresh loop that keeps the cached token map current.

| Metric | Meaning |
|---|---|
| `ravel_durable_auth_refresh_failures_total` | Background refreshes that failed to read or decode `sys/auth`. Ravel does not advance the staleness gate on a failure, so a sustained failure eventually fails auth closed. |
| `ravel_durable_auth_on_miss_rereads_total` | Off-horizon on-miss re-reads of `sys/auth` begun after the rate limiter, when the request path saw an unknown token. |
| `ravel_durable_auth_stale_fail_closed_total` | Bearer-token resolutions refused because the cached map was hard-stale (fail-closed). |

`ravel_durable_auth_refresh_failures_total` is the credential-break
early-warning signal. It climbs as soon as the loop cannot read `sys/auth`,
long before the hard-stale horizon starts refusing tokens. The operations
guide gives its alert rule.

### Maintenance discovery (`ravel_maintain_*`)

Labels: `mode`. Renders only in `maintain` mode.

| Metric | Meaning |
|---|---|
| `ravel_maintain_tenants_discovered` | Gauge. Tenant prefixes storage reported under `t/` on the last successful discovery cycle. |
| `ravel_maintain_tenants_maintained` | Gauge. Discovered tenants actually maintained this cycle, after any flag restriction. |
| `ravel_maintain_tenant_discovery_failures_total` | Maintenance cycles skipped because tenant discovery itself failed. |

### Maintenance safety (`ravel_maintain_*`)

Labels: `mode`, plus `signal` on three of the four. Renders only in `maintain`
mode. These carry no `tenant_hash` label.

| Metric | Meaning |
|---|---|
| `ravel_maintain_legal_hold_refresh_failures_total` | Legal-hold refresh failures. Each one skips that tenant's whole maintenance tick. |
| `ravel_maintain_conservation_aborts_total` | Compaction publishes aborted by the record-count conservation gate, by signal. |
| `ravel_maintain_orphan_breaker_tripped_total` | Orphan-GC mass-orphan circuit breaker trips, by signal. |
| `ravel_maintain_orphans_withheld` | Gauge. Orphan candidates withheld by the most recent sweep pass, by signal. |

The operations guide gives the alert rules and the breaker runbook. A zero
value on the `orphans_withheld` gauge does not mean a prior trip was resolved.

### Read cache (`ravel_cache_*`)

Labels: `mode` and `cache`, plus `tier` when a disk tier is configured
(`--cache-dir`). Absent under `--disable-cache`. The `cache` label carries
`fetch` (the query fetcher cache) or `catalog` (the catalog's byte cache). See
the [caching guide](caching.md) for the full model.

| Metric | Meaning |
|---|---|
| `ravel_cache_hits_total` | Read-cache lookups served from the cache. |
| `ravel_cache_misses_total` | Read-cache lookups not found in the cache. |
| `ravel_cache_bytes_served_total` | Bytes served from the cache on a hit. |
| `ravel_cache_bytes_admitted_total` | Bytes admitted into the cache after a miss. |
| `ravel_cache_evictions_total` | Entries evicted from the read cache by its S3-FIFO policy. |
| `ravel_cache_disk_errors_degraded_to_misses_total` | Disk-tier reads that found an entry but discarded it as unhealthy rather than a clean miss. |
| `ravel_cache_disk_entries_expired_max_age_total` | Disk-tier entries dropped for aging past the per-entry max-age, across the hit check, the startup scan, and the periodic background sweep. A time bound, not the capacity-driven eviction counter above. |

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
| `ravel_admission_admitted_bytes_total` | Charged (decompressed) bytes admitted past the ingest byte-rate layer, by tenant and signal. For a gzip OTLP request this is the decompressed size. For an uncompressed request it equals the wire size. |
| `ravel_ingest_wire_bytes_total` | Wire (on-the-wire, compressed when the client compressed) OTLP request-body bytes admitted, by tenant and signal. |
| `ravel_admission_rejected_total` | Admission rejections, by tenant, signal, and reason. |

The `reason` label carries `byte_rate`, `series_rate`, `series_cap`, or
`clock`. The active-streams count for logs renders under
`ravel_admission_active_series` with `signal="logs"`, not under a separate
metric name.

`ravel_ingest_wire_bytes_total` is emitted from the ingest byte-metrics tracker
rather than the admission snapshot, so its name carries the `ravel_ingest_`
prefix, but it folds tenants by the same allowlist and you read it alongside
the admission counters. The ratio
`ravel_admission_admitted_bytes_total / ravel_ingest_wire_bytes_total` is the
tenant's effective compression factor. It distinguishes two situations a raw
admitted-bytes rise cannot: a tenant that genuinely grew its telemetry (the
ratio holds roughly steady) from one that turned client-side compression off
(admitted bytes flat, wire bytes jump, ratio falls toward 1). The two need
different responses, so read the ratio, not either counter alone.

### Per-query cost (`ravel_query_*`)

Labels: `mode`, `tenant_hash`, `workload_class`. Every read surface folds its
per-query cost into this family. Coverage spans `POST /api/v1/sql`,
`POST /api/v1/analytics`, the Prometheus-shaped `GET /api/v1/query`,
`GET /api/v1/query_range`, `GET /api/v1/labels`, and `GET /api/v1/series`, and
every Flight SQL request. The `workload_class` label carries `interactive` or
`background`. Only `interactive` occurs today.

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

### Metric metadata cache (`query_metadata_cache_*`)

Labels: `mode` only. This is the per-process cache over each tenant's metric
metadata record that serves `/api/v1/metadata` at one GET per (tenant, refresh
horizon, process). It renders only in a request-serving mode that built the
cache (`Mode::All`/`Mode::Query`). A gateway- or maintain-only process omits
the family. All four are cumulative counters.

| Metric | Meaning |
|---|---|
| `query_metadata_cache_hits_total` | Metadata requests served from an already-cached tenant record, fresh or stale. |
| `query_metadata_cache_misses_total` | Metadata requests that found no cached record and did an inline fill GET. |
| `query_metadata_cache_refreshes_total` | Background refreshes started by a past-horizon request that won the single-flight (includes refreshes that later errored). |
| `query_metadata_cache_refresh_errors_total` | Background refreshes that failed their GET or decode. Ravel keeps serving the stale record and the client never sees the error. A climbing value means the record is becoming unreadable. |

The request hit rate is `hits / (hits + misses)`. A refresh-error rate rising
toward the refresh rate means the metadata record is unreadable while stale
data is still being served, which the operations guide pages on.

### Distributed read fan-out (`ravel_distrib_*`)

Labels: `mode` only, plus `le` on the histogram buckets. This family carries no
per-shard, per-worker, or per-tenant label: a fan-out spanning many workers
and tenants must not turn one query into a cardinality explosion. It renders
only when the process runs with `--distributed-query`. A local-only process
omits the family entirely.

| Metric | Meaning |
|---|---|
| `ravel_distrib_fragment_requests_total` | Inbound fragment (`SeriesFetch`) requests served after passing capability auth and fragment admission. Worker side. |
| `ravel_distrib_fragment_auth_failures_total` | Inbound fragment requests refused for a missing or invalid fragment capability. |
| `ravel_distrib_fragment_inflight` | Gauge. Fragment requests currently holding a fragment-admission permit. |
| `ravel_distrib_slices_local_total` | Slices this coordinator executed locally because it owns them (self-mapped, no network hop). |
| `ravel_distrib_slices_remote_total` | Slices this coordinator dispatched to a remote worker and read back over the wire (counts the attempt that produced the usable result, whether the primary or the re-dispatch). |
| `ravel_distrib_slices_redispatched_total` | Slices whose rendezvous-primary worker was lost at transport or returned `Unavailable`, so the coordinator re-dispatched the slice once to the next rendezvous worker. |
| `ravel_distrib_slices_fallback_total` | Slices that fell back to coordinator-local execution after the primary and its one re-dispatch both failed re-dispatchably (transport loss or `Unavailable`), rather than failing the query. |
| `ravel_distrib_slice_fetch_seconds` | Per-slice fetch latency histogram, covering both locally-run and remote slices. |
| `ravel_distrib_quarantine_marks_total` | Dead fragment endpoints marked into the coordinator's quarantine map after a re-dispatchable dispatch failure (transport loss or an `Unavailable` summary), cumulative. |
| `ravel_distrib_quarantine_readmits_total` | Quarantined endpoints readmitted by a strictly newer worker heartbeat stamp (the half-open probe), cumulative. |
| `ravel_distrib_quarantine_current` | Gauge. Fragment endpoints currently held in the coordinator's quarantine map. |

Fragment admission is a distinct workload class from client-query admission
(`--max-inflight-fragments`, separate from the query concurrency limit), so a
burst of inbound fragments cannot starve the coordinator's own client queries
and vice versa. A rising `ravel_distrib_slices_redispatched_total` means a
rendezvous-primary worker is being lost or returning `Unavailable` and slices
are retrying on their next owner. If `ravel_distrib_slices_fallback_total` also
rises, both the primary and its failover are unreachable and the fan-out is
degrading to local execution. In every case the query still returns correct
results, because the coordinator can read any slice itself, but latency will
climb. Ravel never re-dispatches or masks a worker-reported `CORRUPT` status
by fallback. It fails the query typed, so the corruption is not silently
papered over. See the [distributed-query guide](distributed-query.md) for the
operator view of fan-out.

## Reading estimate against actual

The estimate is an upper envelope, never a prediction. The planner takes the
worst case wherever it cannot bound a quantity. A correct estimate therefore
lands at or above the actual, never below it. The estimate and the actual
render under separate names, so their ratio is directly computable in PromQL.

Divide an actual by its matching estimate. The requests ratio is
`ravel_query_s3_requests_total / ravel_query_estimated_requests_total`. A ratio
at or below 1 means the actual stayed inside its own upper envelope, which is
the healthy state. A ratio above 1 means the actual exceeded the envelope that
was meant to bound it.

A ratio above 1 rules in one of two causes. The first cause is a cost-model
gap, where the estimate omits a real source of spend. The second cause is a
runaway query pattern the model did not anticipate. Either cause is worth your
attention. Nothing in this release rejects a query on this envelope. This is
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

1. Check the flag. Per-tenant bytes need `--metrics-tenant-labels` on.
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
documented in the [operations guide](operations.md).

- A query that fails records no cost. A deadline breach, an admission
  rejection, and an execution error all return before the fold, and the error
  type carries no accounting snapshot. Read a sudden drop in
  `ravel_query_queries_total` against steady request logs as failures, not as
  idle capacity.
- A Flight SQL statement records two folds, one per RPC. The plan request
  records the first fold and the fetch request records the second. The
  `ravel_query_queries_total` counter therefore counts 2 for one logical
  query, and the two folds sum to one whole-query estimate beside the summed
  whole-query actual.
- A Flight fetch that a client abandons after one batch still records its
  partial cost. The stream ends when the client disconnects, so Ravel records
  the bytes already spent and counts one query. An unusually low
  cost-per-query ratio on the Flight path can therefore mean early client
  disconnects, not cheap queries.
</content>
