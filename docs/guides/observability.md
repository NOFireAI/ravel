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

This guide is the catalog of `/metrics`. It names the metric families, states
what each number means, and shows how to read the query-cost numbers. Read it
to understand a sample you see on the route.

The [operations guide](operations.md) is the runbook:
[troubleshooting](operations/troubleshooting.md) holds the alert rules and the
mass-orphan breaker runbook, and [configuration](operations/configuration.md)
the admission limits and the durable GC config. Read those to decide what to
page on and how to respond. This guide does not repeat those procedures. It
links to them.

The [tracing guide](tracing.md) covers the query-path `tracing` spans. Metrics
here answer "how much" in aggregate across the process; the per-request spans
answer "where the time went" for one query. Read the tracing guide to
attribute a slow query to a phase. The [cost model guide](cost-model.md)
turns the request and byte counts here into a predicted bill.

## The route

`ravel-server` serves `GET /metrics` in every mode, next to `/healthz` and
`/readyz`. The route reads in-memory atomic counters only. It makes no
object-store call, so a scrape costs nothing on the store.

The route is unauthenticated, like the two health routes. A scrape has no
tenant to resolve, so no handler can authenticate it. An operator must keep
the listener off untrusted networks.

Every sample carries a `mode` label. One Prometheus job can then scrape a
fleet of `all`, `gateway`, `query`, and `maintain` processes without their
series colliding.

## The label allowlist

The renderer can attach only these label keys: `tenant_hash`, `signal`,
`mode`, `op`, `error_kind`, `workload_class`, `level`, `reason`, `cache`,
`tier`, and `kind`, eleven in all. `reason` is shared by two families, the
admission-rejection counter and the scrub seal-divergence counter. `cache` and
`tier` split the read-cache family across its two caches and, when a disk tier
is configured, its two tiers; the [caching guide](caching.md) documents both.
`kind` splits the maintenance merge-memory gauge into its transient and total
high-water marks. The `level` key is reserved and no family renders it.

The allowlist is closed for two reasons. The first reason is cardinality. An
unbounded label value multiplies the series count without a ceiling, and the
scrape then grows with traffic Ravel does not control. The second reason is
tenant-identity disclosure. A raw tenant name or a raw query text on an
unauthenticated route would leak one tenant's activity to anyone who can reach
the port. A closed enum cannot carry either failure.

The allowlist is closed at compile time. Attaching a label is possible only
through a closed enum, so a new label key means a new enum variant and a
compilation failure at every place the renderer matches on a label; a raw
string can never reach the label position. A `shard` label is deliberately
absent, because shard count times tenant count times operation count is
unbounded in the dimension Ravel controls least.

Histogram families carry one further reserved key, `le`, on their `_bucket`
series. It is the Prometheus-standard bucket bound, not a member of the
allowlist above, and no non-histogram sample renders it.

### The `tenant_hash="other"` fold

Four families can carry a `tenant_hash` label: the admission family, the
per-query cost family, the ingest PUT attribution family, and the fleet
admission-reconciliation counter. By default every tenant folds into the
single bucket `tenant_hash="other"`, and that bucket sums every folded
tenant's counters. The scrape then holds one series per (signal or workload
class), never one per tenant, regardless of how many tenants send traffic.

The `--metrics-tenant-labels` flag opts out of the fold. With the flag on,
each configured tenant keeps its own real `tenant_hash`, and the operator
accepts both the higher cardinality and the disclosure on this unauthenticated
route. Turn the flag on only where the scrape network is trusted. Every tenant
without an explicit admission limit still folds into `other` even with the
flag on.

## Metric families by subsystem

Every name below is emitted by `ravel-server`'s `/metrics` renderer. A family
that has no data source in the current mode is omitted, not rendered as zero.
The ingest and log-postings families are absent in `query` and `maintain`
mode. The cache family is absent under `--disable-cache`.

The maintenance families, that is discovery, safety, ownership and
concurrency, merge memory, and the at-rest scrubber, render only in a
`maintain` mode process, because only that mode runs compaction, retention,
the sweep, and the scrubber. An operator scraping an `all` mode process never
sees those families move, and that is expected.

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

#### Per-tenant PUT attribution (`ravel_ingest_attribution_puts_total`)

Labels: `mode`, `tenant_hash`, `signal`.

| Metric | Meaning |
|---|---|
| `ravel_ingest_attribution_puts_total` | Object-store PUT requests attributed to completed flushes, by tenant and signal. |

This answers which tenant is generating the PUT bill: each completed flush
charges 2 PUTs (a data object and a commit record) to the flushing tenant,
tracked per signal by a bounded top-K structure in the ingest router that
follows at most 1024 tenants. That bound protects the router's internal
accounting; the `tenant_hash` label on this family is bounded separately, by
the same `--metrics-tenant-labels` allowlist and the same `tenant_hash="other"`
fold described above. A tenant outside the allowlist never gets a series of its
own here, regardless of how much it contributes to the top-K table.

### Log postings and dynamic columns (`ravel_logs_postings_*`, `ravel_logs_dynamic_columns_*`)

Labels: `mode` and `signal`. One series per pipeline that builds a POSTINGS
index, so an idle-but-configured pipeline still renders its zero. These
families are ingest-side, present where ingest runs.

| Metric | Meaning |
|---|---|
| `ravel_logs_postings_objects_total` | Flushed log objects that carried a POSTINGS section, by signal (the denominator for average section bytes per indexed object). |
| `ravel_logs_postings_bytes_total` | Cumulative encoded POSTINGS section bytes across flushed log objects, by signal. |
| `ravel_logs_postings_indexed_fields_total` | Cumulative count of indexed fields that emitted a posting list, summed over objects, by signal. |
| `ravel_logs_postings_distinct_values_total` | Cumulative distinct-value count across non-capped indexed fields, summed over objects, by signal. |
| `ravel_logs_postings_capped_fields_total` | Indexed fields dropped from POSTINGS for exceeding the per-field distinct-value cap, summed over objects, by signal. |
| `ravel_logs_dynamic_columns_used_total` | Distinct (name, type) attribute pairs that received a real dynamic column, summed over flushed log objects, by signal. |
| `ravel_logs_dynamic_columns_overflowed_total` | Distinct (name, type) attribute pairs that overflowed the max_dynamic_columns budget and folded into attrs_raw, summed over flushed log objects, by signal. |
| `ravel_logs_dynamic_columns_used_max` | Gauge. Largest per-object dynamic-column count seen so far, by signal: the budget-pressure signal that rises before any object overflows max_dynamic_columns. |

A climbing `ravel_logs_dynamic_columns_overflowed_total`, or a
`ravel_logs_dynamic_columns_used_max` gauge sitting near the configured
budget, is the sign that a tenant's attribute set has outgrown its
`max_dynamic_columns` and is folding attributes into the raw column. A
climbing `ravel_logs_postings_capped_fields_total` is the same story for a
single high-cardinality field hitting the per-field distinct-value cap.

### Query-side pruning (`ravel_logs_prune_*`)

Labels: `mode` and `signal="logs"`. These are query-side counters over the
logs scans, present where queries run.

| Metric | Meaning |
|---|---|
| `ravel_logs_prune_blocks_total` | Blocks the logs scans considered before postings pruning, cumulative (the denominator of prune selectivity). |
| `ravel_logs_prune_blocks_survived_total` | Blocks that survived postings pruning and were scanned, cumulative (the numerator of prune selectivity). |
| `ravel_logs_prune_blocks_pruned_by_postings_total` | Blocks dropped by the POSTINGS index before scanning, cumulative. |

Prune selectivity is `survived / total`. A ratio near 1 means the POSTINGS
index is dropping almost nothing and the scans are reading most blocks anyway,
which points either at queries whose predicates the index cannot serve or at
an index that is not discriminating for this workload.

### Catalog integrity (`ravel_catalog_*`)

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_catalog_interlock_violations_total` | Unlisted L0 commit records observed postdating a compaction record in their bucket. |
| `ravel_catalog_compaction_input_set_conflicts_total` | Buckets observed holding two compaction records with different input_set_hash. |
| `ravel_catalog_isolation_breach_total` | Hard-failed queries from a HEAD or postings tenant_hash mismatch or an out-of-prefix listing result. |

The first two counters tally an anomaly the query resolves past. Each
`ravel_catalog_isolation_breach_total` increment is a query that failed with
an explicit isolation-fault error. [Troubleshooting](operations/troubleshooting.md) gives its alert rule.

### Tenancy adoption (`ravel_tenancy_v1_unkeyed_adoptions_total`)

Labels: `mode`. Counts buckets this process pinned to the unkeyed tenant hash
when it adopted a bucket that held `t/` data but no `sys/tenancy` marker. A
nonzero value is the visible signal that the one-time migration happened.

### Provisioning (`ravel_provisioning_shard_count_mismatch_total`)

Labels: `mode`. Counts dynamic-tenant provisioning checks that failed: a
`shard_count` disagreement, an unreadable record, or a maintain-loop check
catching either. [Troubleshooting](operations/troubleshooting.md) gives its alert rule.

### Store reachability (`ravel_store_reachable`, `ravel_store_probe_failures_total`)

Labels: `mode`. Both samples come from the background store-reachability probe.

| Metric | Meaning |
|---|---|
| `ravel_store_reachable` | Gauge. 1 when the probe reports the store reachable, 0 after K consecutive failed probes. |
| `ravel_store_probe_failures_total` | Every failed probe cycle, monotonic, incremented even below the readiness threshold. |

### Durable auth refresh (`ravel_durable_auth_*`)

Labels: `mode`. Renders only when the process built a durable `sys/auth`
resolver: `--tenant-hash-key-file` set on `ravel-server` in a request-serving
mode (`all`, `gateway`, `query`). A `maintain` mode process, or one started
without a deployment key, omits the whole family. The file that flag names is
the deployment key; the `ravel-cli tenant-token` subcommands take the same key
through their own `--deployment-key-file` flag, a separate flag on a separate
binary despite the similar name. All three counters come from the background
refresh loop that keeps the cached token map current.

| Metric | Meaning |
|---|---|
| `ravel_durable_auth_refresh_failures_total` | Background refreshes that failed to read or decode `sys/auth`. The staleness gate is not advanced on a failure, so a sustained failure eventually fails auth closed. |
| `ravel_durable_auth_on_miss_rereads_total` | Off-horizon on-miss re-reads of `sys/auth` begun after the rate limiter, when the request path saw an unknown token. |
| `ravel_durable_auth_stale_fail_closed_total` | Bearer-token resolutions refused because the cached map was hard-stale (fail-closed). |

`ravel_durable_auth_refresh_failures_total` is the credential-break
early-warning signal: it climbs as soon as the loop cannot read `sys/auth`,
long before the hard-stale horizon starts refusing tokens. The operations
guide gives its alert rule.

### Maintenance discovery (`ravel_maintain_tenants_*`, `ravel_maintain_tenant_discovery_failures_total`)

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_maintain_tenants_discovered` | Gauge. Tenant prefixes storage reported under `t/` on the last successful discovery cycle. |
| `ravel_maintain_tenants_maintained` | Gauge. Discovered tenants actually maintained this cycle, after any flag restriction. |
| `ravel_maintain_tenant_discovery_failures_total` | Maintenance cycles skipped because tenant discovery itself failed. |

### Maintenance safety (`ravel_maintain_legal_hold_*`, `ravel_maintain_conservation_*`, `ravel_maintain_orphan*`)

Labels: `mode`, plus `signal` on all but the legal-hold counter. These carry no
`tenant_hash` label.

| Metric | Meaning |
|---|---|
| `ravel_maintain_legal_hold_refresh_failures_total` | Legal-hold refresh failures. Each one skips that tenant's whole maintenance tick. |
| `ravel_maintain_conservation_aborts_total` | Compaction publishes aborted by the record-count conservation gate, by signal. |
| `ravel_maintain_orphan_breaker_tripped_total` | Orphan-GC mass-orphan circuit breaker trips, by signal. |
| `ravel_maintain_orphans_withheld` | Gauge. Orphan candidates withheld by the most recent sweep pass, by signal. |
| `ravel_maintain_orphans_present` | Gauge. Orphan candidates the most recent sweep pass found, by signal, whether or not the breaker tripped. |

[Troubleshooting](operations/troubleshooting.md) gives the alert rules and the
breaker runbook. A zero
value on the `orphans_withheld` or `orphans_present` gauge does not mean a
prior trip was resolved: it is this pass's count, not a resolution signal.

### Maintenance ownership and concurrency (`ravel_maintain_workers_live`, `ravel_maintain_units_*`, `ravel_maintain_memo_warm_start_units_total`, `ravel_maintain_full_sweep_passes_total`)

Labels: `mode`. Every series here is process-wide, with no `tenant_hash`
dimension.

| Metric | Meaning |
|---|---|
| `ravel_maintain_workers_live` | Gauge. In-process maintenance workers this supervisor currently sees as live. |
| `ravel_maintain_units_owned` | Gauge. Owned (tenant, signal, shard) units this process is currently maintaining. |
| `ravel_maintain_units_stalled` | Gauge. Owned units with consecutive failing ticks past the configured threshold. Alert on a sustained nonzero value, not on any single scrape. |
| `ravel_maintain_memo_warm_start_units_total` | Units seeded from a durable memo snapshot on handoff or startup, instead of rescanning cold. |
| `ravel_maintain_full_sweep_passes_total` | Full (unscoped) sweep passes run, as opposed to a zone-scoped sweep. |

### Merge memory (`ravel_maintain_rlog_merge_peak_bytes`)

Labels: `mode` and `kind`. No `tenant_hash`: the tracker is one process-wide
handle shared across every tenant's merges.

| Metric | Meaning |
|---|---|
| `ravel_maintain_rlog_merge_peak_bytes` | Gauge. High-water mark of RLOG k-way merge memory, by kind. |

The `kind` label carries `transient` (in-flight fetched-minus-released block
bytes at any instant during a merge) or `total` (transient plus the writer's
buffered output bytes). This is the gauge to watch when a maintain process is
under memory pressure during compaction merges.

### At-rest scrubber (`ravel_scrub_*`)

Labels: `mode` and `signal`, plus `reason` on the seal-divergence counter.
These carry no `tenant_hash` label.

| Metric | Meaning |
|---|---|
| `ravel_scrub_checksum_mismatch_total` | Data objects that failed at-rest integrity re-verification (a whole-object blake3 mismatch or a footer or section crc failure), by signal. |
| `ravel_scrub_postings_disagreement_total` | Objects whose covering name-postings object omitted a `__name__` the object really carries (a false negative), by signal. |
| `ravel_scrub_seal_divergence_total` | Divergences between the folded snapshot and the re-listed sealed commit history, by signal and reason. |
| `ravel_scrub_cursor_position` | Gauge. Fraction of the current scrub rotation the content-tier cursor has covered so far, by signal, in [0,1]. |

`ravel_scrub_checksum_mismatch_total` is the one to alert on for any increase:
there is no redundant copy to repair from, so a nonzero increase is corruption
an operator must investigate. The `reason` label on
`ravel_scrub_seal_divergence_total` carries `missing` (a sealed commit record
absent from the snapshot, an under-count) or `mismatched` (a snapshot entry
whose content hash disagrees with the sealed record); an orphaned entry, a
snapshot entry with no surviving commit record, is the expected
retention-after-fold shape and is never counted. A `ravel_scrub_cursor_position`
stuck near 0 means scrubbing is not keeping pace with the configured
`--scrub-period`.

### Read cache (`ravel_cache_*`)

Labels: `mode`, `cache`, and `tier`. The `cache` label carries `fetch` (the
query fetchers' RAM cache) or `catalog` (the catalog's content-addressed byte
cache), so both caches share one family and are told apart by that label. The
`tier` label is present only when a family has a local-disk tier configured
with `--cache-dir`: then its RAM sample carries `tier="ram"` and its disk
sample `tier="disk"`. With no disk tier, a family renders one sample with no
`tier` label at all. Absent entirely under `--disable-cache`.

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
ratios for PromQL to compute, per `cache` and per `tier`.

### Admission (`ravel_admission_*`, `ravel_ingest_wire_bytes_total`)

Labels: `mode`, `tenant_hash`, `signal`, plus `reason` on the rejection
counter. This family folds tenants per the rule above. The
[admission limits guide](admission-limits.md) covers this family in
operational depth, and [troubleshooting](operations/troubleshooting.md) gives
its alert rules.

| Metric | Meaning |
|---|---|
| `ravel_admission_active_series` | Gauge. Active series (metrics) or streams (logs) tracked for the active cap, by tenant and signal. |
| `ravel_admission_admitted_total` | Requests admitted past the ingest byte-rate layer, by tenant and signal. |
| `ravel_admission_admitted_bytes_total` | Charged (decompressed) bytes admitted past the ingest byte-rate layer, by tenant and signal. For a gzip OTLP request this is the decompressed size; for an uncompressed request it equals the wire size. |
| `ravel_ingest_wire_bytes_total` | Wire (on-the-wire, compressed when the client compressed) OTLP request-body bytes admitted, by tenant and signal. |
| `ravel_admission_rejected_total` | Admission rejections, by tenant, signal, and reason. |
| `ravel_admission_reconciliation_failures_total` | Fleet-admission reconciliation cycles whose sibling-snapshot read (LIST or GET) failed, by tenant and signal; the last-known soft threshold stays in force. |

The `reason` label carries `byte_rate`, `series_rate`, `series_cap`, or
`clock`. The active-streams count for logs renders under
`ravel_admission_active_series` with `signal="logs"`, not under a separate
metric name. A sustained nonzero
`ravel_admission_reconciliation_failures_total` rate means a process cannot
read its siblings' snapshots and is falling back to its last-computed soft
threshold; admission never fails closed on it, so it signals degrading
fleet-wide accuracy, not that ingest is down.

`ravel_ingest_wire_bytes_total` is emitted from the ingest byte-metrics tracker
rather than the admission snapshot, so its name carries the `ravel_ingest_`
prefix, but it folds tenants by the same allowlist and is read alongside the
admission counters. The ratio
`ravel_admission_admitted_bytes_total / ravel_ingest_wire_bytes_total` is the
tenant's effective compression factor. It distinguishes two situations a raw
admitted-bytes rise cannot: a tenant that genuinely grew its telemetry (the
ratio holds roughly steady) from one that turned client-side compression off
(admitted bytes flat, wire bytes jump, ratio falls toward 1). The two need
different responses, so read the ratio, not either counter alone.

### Per-query cost

Labels: `mode`, `tenant_hash`, `workload_class`. Every read surface folds its
per-query cost into the `ravel_query_*` family. Coverage spans
`POST /api/v1/sql`, `POST /api/v1/analytics`, the Prometheus-shaped
`GET /api/v1/query`, `GET /api/v1/query_range`, `GET /api/v1/labels`, and
`GET /api/v1/series`, and every Flight SQL request. The `workload_class` label
carries `interactive` or `background`. Only `interactive` occurs. The
[cost model guide](cost-model.md#per-query-cost-accounting) explains the
accounting behind these numbers.

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
cache (`all` or `query`); a gateway- or maintain-only process omits the
family. All four are cumulative counters.

| Metric | Meaning |
|---|---|
| `query_metadata_cache_hits_total` | Metadata requests served from an already-cached tenant record, fresh or stale. |
| `query_metadata_cache_misses_total` | Metadata requests that found no cached record and did an inline fill GET. |
| `query_metadata_cache_refreshes_total` | Background refreshes started by a past-horizon request that won the single-flight (includes refreshes that later errored). |
| `query_metadata_cache_refresh_errors_total` | Background refreshes that failed their GET or decode. The stale record keeps being served and the client never sees the error; a climbing value means the record is becoming unreadable. |

The request hit rate is `hits / (hits + misses)`. A refresh-error rate rising
toward the refresh rate means the metadata record is unreadable while stale data
is still being served, which the operations guide pages on.

### Distributed read fan-out (`ravel_distrib_*`)

Labels: `mode` only, plus `le` on the histogram buckets. This family carries no
per-shard, per-worker, or per-tenant label: a fan-out spanning many workers and
tenants must not turn one query into a cardinality explosion. It renders only
when the process runs with `--distributed-query`; a local-only process omits
the family entirely.

| Metric | Meaning |
|---|---|
| `ravel_distrib_fragment_requests_total` | Inbound fragment (`SeriesFetch`) requests served after passing token auth and fragment admission. Worker side. |
| `ravel_distrib_fragment_auth_failures_total` | Inbound fragment requests refused for a missing or invalid cluster bearer token. |
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
are retrying on their next owner; if `ravel_distrib_slices_fallback_total` also
rises, both the primary and its failover are unreachable and the fan-out is
degrading to local execution. In every case the query still returns correct
results (the coordinator can read any slice itself), but latency will climb.
A worker-reported `CORRUPT` status is never re-dispatched or masked by
fallback: it fails the query typed so the corruption is not silently papered
over.

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
runaway query pattern the model did not anticipate. Either cause is worth an
operator's attention, because a later admission decision could reject queries
on this envelope. Nothing rejects a query on it; this is measurement only.

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

Three gaps limit what the per-query cost family can show: a failed query
records no cost, a Flight SQL statement records two folds for one logical
query, and an abandoned Flight fetch still records its partial cost. The
[cost model guide](cost-model.md#per-query-cost-accounting) sets each one out
in full, because each is a property of the accounting rather than of the
metric route.

## Background

The `/metrics` route, the label allowlist, and per-query cost accounting:
ADR-0044. The `reason` label and the admission usage family: ADR-0051. The
read caches and their disk tier: ADR-0046, ADR-0064. Maintenance safety,
ownership, merge memory, and the at-rest scrubber: ADR-0048, ADR-0058,
ADR-0059, ADR-0065. Log POSTINGS and dynamic columns: ADR-0049, ADR-0100.
Distributed read fan-out: ADR-0071. Wire-byte accounting: ADR-0084. The metric
metadata cache: ADR-0085.
