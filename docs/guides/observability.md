# Observability

Ravel measures its own work and exports the counts at `GET /metrics`. The
route renders one Prometheus text exposition document. It carries what Ravel
spent and what Ravel refused, per object-store operation, per ingest signal,
per tenant bucket, and per query workload. It never carries a sample value, a
query text, an object key, or a trace id.

A number that summarizes the process is a metric: request counts, byte
counts, cache outcomes, and error kinds. A number that answers a question
about stored telemetry is a query against `/api/v1/query` or `/api/v1/sql`.
`/metrics` carries only the first kind.

## Related guides

| Guide | Use it to |
|---|---|
| [Operations](operations.md) | Decide what to page on and how to respond. |
| [Troubleshooting](operations/troubleshooting.md) | Find the alert rules and the mass-orphan breaker runbook. |
| [Configuration](operations/configuration.md) | Set the admission limits and the durable GC config. |
| [Tracing](tracing.md) | Attribute a slow query to a phase. Metrics answer "how much" in aggregate across the process. The per-request spans answer "where the time went" for one query. |
| [Cost model](cost-model.md) | Turn the request and byte counts into a predicted bill. |

## The route

`ravel-server` serves `GET /metrics` in every mode, next to `/healthz` and
`/readyz`. The route reads in-memory atomic counters only. It makes no
object-store call, so a scrape costs nothing on the store.

The route is unauthenticated, like the two health routes. A scrape has no
tenant to resolve, so no handler can authenticate it. An operator must keep
the listener off untrusted networks.

Every sample carries a `mode` label. One Prometheus job can then scrape a
fleet of `all`, `gateway`, `query`, and `maintain` processes without series
collisions.

## The label allowlist

The renderer can attach only these label keys: `tenant_hash`, `signal`,
`mode`, `op`, `error_kind`, `workload_class`, `level`, `reason`, `cache`,
`tier`, `kind`, `outcome`, `allocator`, `stat`, `component`, `class`,
`carrier`, `gate`, `site`, `worker`, `shard`, and `phase`, twenty-two in
all. Some keys split more than one family:

| Label | What it splits |
|---|---|
| `phase` | The [SQL DDL families](#sql-ddl-statements-and-their-store-cost-ravel_sql_ddl_) only. It exists only in a build with the `sql` feature. |
| `kind` | DDL statements by leading keyword. The maintenance merge-memory gauge into its transient and total high-water marks. |
| `outcome` | DDL statements by how they ended. The alert-tick counter by how one evaluation tick ended. |
| `reason` | The admission-rejection counter, the scrub counters, the alert retention-skip counter, the superseded-inputs-held counter, the compaction inputs-skipped counter, the ingest rerouted-flushes counter, the fragment capability reject counter, and the SQL slice capability reject counter. |
| `cache`, `tier` | The read-cache family across its two caches and, when a disk tier is configured, its two tiers. The [caching guide](caching.md) documents both. |
| `class` | The fragment in-flight gauge and admission-wait counter into their `pinned` and `resolve` fragment admission classes. |
| `carrier` | The declared-statistics drop tally across its four carrier labels. |
| `allocator`, `stat` | The process allocator gauges. |
| `component` | The memory budget's reserved-bytes gauge by which side reserved it. |
| `gate`, `site` | The CPU gate families by gate and by call site, each from a closed set. `site` also splits the memory gate's two refusal counters (`ravel_memory_gate_refusals_total`, `ravel_memory_budget_refusals_total`) by check site. |
| `worker` | The tokio runtime's per-worker busy counter, bounded by the runtime's fixed worker count. |
| `level` | The scrub counters by which part of the commit lineage a target came from (`l0`, `l1` or `rewrite`). |
| `shard` | The per-shard ingest skew family only, beside `mode` and `signal` and never beside `tenant_hash`. |

The allowlist is closed at compile time. A label key is a variant of a closed
enum, so a raw string can never reach the label position. The closed set
bounds two risks:

- **Cardinality.** An unbounded label value multiplies the series count
  without a ceiling. The scrape then grows with traffic that Ravel does not
  control.
- **Tenant-identity disclosure.** A raw tenant name or a raw query text on an
  unauthenticated route leaks one tenant's activity to anyone who can reach
  the port.

Histogram families carry one more reserved key, `le`, on their `_bucket`
series. `le` is the Prometheus-standard bucket bound and is outside the
allowlist. No non-histogram sample renders it.

### The `tenant_hash="other"` fold

By default every tenant folds into the single bucket `tenant_hash="other"`,
which sums the counters of every folded tenant. The scrape then holds one
series per signal or workload class, however many tenants send traffic.

These families can carry a `tenant_hash` label: the admission family, the
per-query cost family, the three SQL DDL families, the ingest PUT
attribution family, and the fleet admission-reconciliation counter.

The `--metrics-tenant-labels` flag turns the fold off. Each configured tenant
then keeps its real `tenant_hash`. That raises cardinality and discloses
tenants on this unauthenticated route, so turn the flag on only where the
scrape network is trusted. A tenant without an explicit admission limit still
folds into `other` with the flag on.

## Metric families by subsystem

The `/metrics` renderer of `ravel-server` emits every name below. A family
that has no data source in the current mode is omitted, not rendered as zero:

| Families | Where they render |
|---|---|
| Ingest and log postings | Absent in `query` and `maintain` mode. |
| Cache | Absent under `--disable-cache`. |
| Maintenance: discovery, safety, ownership and concurrency, merge memory, and the at-rest scrubber | Only in a `maintain` mode process, because only that mode runs compaction, retention, the sweep, and the scrubber. A scrape of an `all` mode process never shows those families move. |
| Alerting | Only when the process built an alert evaluator, which needs `--alert-rules-file` naming at least one rule. That is every `all` or `query` mode process configured with rules, whether or not the process holds a tenant's alert lease. |

The alert rules printed in `yaml` blocks below ship in
[`deploy/prometheus/ravel.rules.yaml`](../../deploy/prometheus/ravel.rules.yaml),
together with the conditions that the
[troubleshooting guide](operations/troubleshooting.md) states. Load that file
from the `rule_files:` key of Prometheus. Do not copy a block from this page,
because nothing updates a copy when the deployment changes.
[`deploy/grafana/dashboards-standalone/ravel.json`](../../deploy/grafana/dashboards-standalone/ravel.json)
ships a dashboard that graphs the same `ravel_` families the rules alert on.
See `deploy/README.md` for where to import it.

### Object store (`ravel_store_*`)

Labels on the per-operation families: `mode`, `op`, and `error_kind` on the
error counter only.

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
a query get land in the same series. For per-query attribution, see
[Per-query cost](#per-query-cost).

Four more store counters carry `mode` and no `op` label:

| Metric | Meaning |
|---|---|
| `ravel_store_get_unverified_total` | Full-object reads served without verifying the body against a stored checksum. |
| `ravel_store_control_plane_requests_total` | Bucket-protection control-plane GETs sent, counted before dispatch. |
| `ravel_store_control_plane_calls_total` | Bucket-protection control-plane GETs that got an HTTP response back, whatever its status. |
| `ravel_store_control_plane_response_bytes_total` | Wire bytes of the control-plane response bodies, as received. |

`ravel_store_get_unverified_total` stays at zero on a backend other than S3.
On S3 it moves when a full-object read comes back without a stored checksum
that the store can check the body against:

- An object written with `--s3-upload-integrity off` (or before upload
  checksums were on) or with `sha256`.
- An object larger than one request body (8 MiB by default), which is read in
  several responses.
- An endpoint that does not return stored checksums.
- Every full-object read of a process started with
  `--s3-request-stored-checksum=false`.

See
[Upload and read checksums](operations/configuration.md#upload-and-read-checksums).

The control-plane counters cover only the read-only bucket-configuration GETs
behind the bucket-protection check. Those GETs are not object-store
operations, so they never appear in the per-operation families, which count
data-plane traffic only. The server sends these GETs only from its
`--require-bucket-protection` startup check on `--store s3`: three per start
(`?versioning`, `?lifecycle`, `?object-lock`). With the flag off, or on any
other backend, the three control-plane counters read 0.

### Ingest pipelines (`ravel_ingest_*`)

Labels: `mode` and `signal`. The `signal` label carries `metrics`, `logs`, or
`spans`. `ravel_ingest_rerouted_flushes_total` also carries `reason`.

| Metric | Meaning |
|---|---|
| `ravel_ingest_flushes_by_size_total` | Flushes opened because the tenant buffer reached target_bytes. |
| `ravel_ingest_flushes_by_age_total` | Flushes opened because the tenant buffer aged past max_flush_delay. |
| `ravel_ingest_flushes_by_age_floor_total` | Flushes opened because the tenant buffer aged past the sub-floor hold, which a buffer holding fewer object bytes than `--idle-flush-byte-floor` waits for instead of max_flush_delay_idle. Zero unless that flag is set. A rising figure means the floor is holding buffers, which is the point of setting it, and it is also the count of flushes whose rows sat in memory for up to an hour before the flush opened: it is how an operator sizes the buffered-mode loss window they accepted. |
| `ravel_ingest_flushes_manual_total` | Flushes opened by an explicit, shutdown, or drop-path drain. |
| `ravel_ingest_put_retries_total` | Retried PUT attempts on the data-object or commit-record path. |
| `ravel_ingest_abandoned_retry_exhausted_total` | Flushes abandoned by retry-budget or lifetime exhaustion. |
| `ravel_ingest_abandoned_input_rejected_total` | Flushes abandoned because the input could not build a durable object. |
| `ravel_ingest_buffered_bytes_total` | Bytes admitted into shard buffers at enqueue time. |
| `ravel_ingest_buffered_items_total` | Samples, records, or spans admitted into shard buffers. |
| `ravel_ingest_acks_ok_total` | Strict-mode waiters acked with a commit token. |
| `ravel_ingest_acks_err_total` | Strict-mode waiters acked with a write error. |
| `ravel_ingest_collisions_total` | Batches rejected for a series or stream identity collision. |
| `ravel_ingest_shard_deaths_total` | Shard-actor deaths observed by the router. What a death means differs by signal (the `signal` label). On `signal="metrics"` the router respawns, so the counter counts every death including each respawned incarnation and can exceed the shard count. A low steady rate there is transient respawn recovery, because the respawn budget decays to zero after a shard runs a whole `max_flush_lifetime` without dying, so deaths spread that far apart never accumulate into a condemnation, and a sustained climb on one shard is a poison-pill input. On `signal="logs"` and `signal="spans"` the routers never respawn, so a death is already a condemnation: it is counted once per shard per live generation, never more, and the matching `ravel_ingest_shards_condemned_total` sample moves on the same death. Any increase on those two signals is permanent shard loss, not recovery. |
| `ravel_ingest_shards_condemned_total` | Shards condemned and no longer accepting writes, counted at most once per shard per live generation and bounded by the live generation count times the shard count, not the shard count: under resharding each generation condemns a shard index independently. The threshold differs by signal (the `signal` label): the metrics pipeline condemns a shard only after it exhausts its respawn budget within one decay window, while the logs and spans pipelines never respawn and so condemn on the first shard-actor death. Any nonzero value means the process reports `/readyz` 503, which sheds traffic but does not replace the process. It stays condemned until someone rolls it. Alert on `> 0`. |
| `ravel_ingest_partial_writes_total` | Multi-shard strict writes the router observed committing on some shards and then failing on a sibling. The client received an error that is retryable when and only when the sibling's failure was, and the durable tokens reach it only where the transport can carry them (the OTLP, remote-write, and OTAP gateways log the durable count instead). A rising figure means retries can be re-ingesting data that already committed (see the consistency model's partial multi-shard commit section). |
| `ravel_ingest_idempotency_lookup_failures_total` | Keyed log and span writes refused with a retryable 503 (gRPC `UNAVAILABLE`) because their idempotency marker lookup failed, before the request's own data was written, so the client may retry. Two refusals count here: a GET of a marker key that failed with a store error other than not-found, and a lookup still running at half of the request's `ack_deadline`, the share the lookup may use so the write keeps the rest of that budget. Each refusal also logs a WARN line naming the GET and either the marker key and the store error, or the lookup's deadline; the client response names none of them. When those WARN lines name the deadline rather than a key, the store is answering marker GETs too slowly for the lookup to finish inside its half of the request's budget. A sustained figure usually means the gateway credential cannot read `t/*/*/idem/*`, or on AWS S3 lacks the list grant on the marker key that turns an absent marker into a 404 (`deploy/iam/README.md`), and every keyed write is failing. |
| `ravel_ingest_idempotency_probe_gets_total` | Idempotency marker probe GETs issued by keyed log and span writes' marker lookups, by signal, hits and misses alike. A lookup that hits in its first batch (the hour ahead, the current hour or the hour before) costs 3; any other hit, or a miss, costs one per hour of the dedup window plus 2, 26 at the default 24-hour window. Divided by the keyed write rate it gives the GETs each keyed write costs the store, which the single store request of an unkeyed write does not include. |
| `ravel_ingest_exemplars_written_total` | Exemplars stored on flushed objects. |
| `ravel_ingest_exemplars_dropped_total` | Exemplars discarded by the per-series admission cap. |
| `ravel_ingest_stale_provisioning_flushes_total` | Flushes failed closed because the router's cached shard-generation view was older than the refresh interval, and buffers whose flush stayed closed because that view was too old to trust when the flush opened (counted once per buffer until it clears). A rising figure means the provisioning re-read is failing for longer than the grace window allows. |
| `ravel_ingest_grace_extended_stale_flushes_total` | Flushes routed on a last-known-good provisioning view inside the bounded grace window. A rising figure means the store is slow to serve the provisioning re-read and this router is running degraded-but-available. |
| `ravel_ingest_rerouted_flushes_total` | Flushes that handed their rows to another shard set instead of writing them, by `reason`. `reason="retired_index"`: after a shard-count decrease, a flush that would have written under a shard index readers no longer scan for the ingest hour it pinned, handed to the tenant's current shard count. `reason="generation_mismatch"`: after a decrease or an increase, a flush that would have written into an ingest hour owned by a shard generation that routes at another count, handed to that generation's shards. Both series render from zero for every signal, and each is its own counter. Every hand-back is counted under exactly one reason. Counted once per flush attempt that delivered rows to at least one target, so a hand-back that delivers part of a buffer and the rest on a later attempt counts twice. The rows are written once, where queries find them; a strict write still waiting on them was answered 503 (outcome unknown). Expected after a reshard when flushes ran late; a sustained figure with no recent reshard is worth a look. |
| `ravel_ingest_hand_back_failures_total` | Hand-backs that kept their rows: one after a shard-count decrease that found a target shard dead, condemned, or closed, and one into an hour another shard generation owns that found a target's mailbox full on a hand-back to a larger shard set. Counted once per buffer until a hand-back from it delivers. Nothing is lost: the rows stay with the shard that held them and a later flush retries. A hand-back into an hour another shard generation owns is retried only until the flush deferral cap, and a drain (`flush_all`, shutdown) does not wait that long; past either, the shard that held the rows writes them in place, where readers find them but a distributed query over that hour can split their series across two slices, and counts the write on `ravel_ingest_generation_mismatch_in_place_writes_total`. It logs a WARN only for the first such write on that shard per tenant, cause (a target not live or, on a drain, still full; or the retry run out) and ingest hour, and may repeat once a shard tracks more than 4096 tenant and cause pairs, so count these writes on the counter, not the WARN. Such a hand-back whose target is dead, condemned, or closed is not counted here and not retried: the flush that finds it writes the rows in place at once, the same way. A dead metrics shard is respawned by the next write routed to it; a condemned logs or spans shard stays down until the process is replaced, so on those signals a rise after a decrease means rows that wait for shutdown, alongside `ravel_ingest_shards_condemned_total`. |
| `ravel_ingest_teardown_unscanned_writes_total` | Flushes a shutdown or channel-close drain wrote in place under a shard index readers do not scan for their ingest hour, because no live shard of the current shard count could take the rows. Those rows are stored but no query returns them; each write also logs an ERROR with the tenant, shard and hour. Alert on any increase. |
| `ravel_ingest_generation_mismatch_in_place_writes_total` | Buffers written under the shard index that held them in an ingest hour another shard generation owns alone, because a hand-back into that hour could not deliver: at once when a target shard was dead, condemned, or closed, and when a target's mailbox stayed full, at the flush deferral cap or during a drain. Counted once per buffer; each write also logs a WARN with the tenant, shard and hour. Readers find the rows, but that hour stays eligible for distributed query pushdown with a series at two shard indices, so a distributed aggregate over it can differ from the single-node answer, and since stored objects are never rewritten the hour stays that way. Renders from zero for every signal. Alert on any increase. |
| `ravel_ingest_flushes_by_age_adaptive_total` | Flushes opened on the adaptive-delay corridor age trigger rather than the fixed max_flush_delay. A rising figure means the adaptive corridor, not the fixed delay, is driving age flushes. |
| `ravel_ingest_in_flight_flushes` | Flush tasks spawned but not yet acked, summed across shards (a gauge). A sustained high value means flushes are not keeping up with the load. |
| `ravel_ingest_flush_permit_wait_seconds_total` | Total seconds every flush has spent waiting for a `max_inflight_flushes` permit, summed across shards. Zero unless a shard is asked for a second concurrent flush. A rising figure means `max_inflight_flushes` is the binding window. |
| `ravel_ingest_queued_flushes` | Flush tasks spawned and not yet reaped, summed across shards (a gauge): the per-shard queue `--max-queued-flushes` caps. It can exceed the shard count times that cap, because a tenant buffer over its memory backstop spawns whatever the queue depth. This gauge rising while `ravel_ingest_flush_trigger_deferred_total` stays flat is that exemption, not a queue that lost its bound. |
| `ravel_ingest_flush_trigger_deferred_total` | Size and age flush triggers refused because their shard was already holding `--max-queued-flushes` spawned flush tasks, summed across shards. A refusal is a deferral, not a shed: the buffer rides back untouched and the next tick re-fires once a flush has been reaped, so a rising figure means flush latency slipped past `--max-flush-delay` while nothing was dropped. Once a shard's oldest deferred flush has waited the flush deferral cap (3,559.8 s at the defaults), the shard also refuses new writes, counted on `ravel_ingest_deferral_cap_refused_total`, while the deferred flush keeps waiting for a slot. |
| `ravel_ingest_deferral_cap_refused_total` | Writes refused with a retryable 429 (gRPC `RESOURCE_EXHAUSTED`) because the shard had a flush deferred for the whole flush deferral cap: the two-hour flush slack (the flush-timing part of the three-hour read-side scan slack) less the flush lifetime, the slowest flush trigger (the largest of `--max-flush-delay`, `--max-flush-delay-idle` and, with `--adaptive-flush-delay`, the adaptive corridor's widest ceiling) and one flush tick. The router counts one for each write it refuses before enqueue. A write the router let through just before the cap can instead be refused by each shard it reaches, each counting one, so one multi-shard write can count more than once. Both write modes are refused. A strict write already waiting on the capped flush is answered 503 instead, and its rows are still written by the flush that opens past the cap, so a client retry of it stores logs and spans twice. A rising figure means a shard's flushes have been stalled for about an hour. The shard accepts again once they open. The first refusal of each episode is also logged at WARN with the shard and signal. |
| `ravel_ingest_flush_all_residue_tenants_total` | Tenants a teardown drain left with buffered rows still unflushed, summed across shards. In buffered mode those rows were already acknowledged ([Consistency model](../consistency-model.md)), so a nonzero increase is data loss, not backpressure. It counts TENANTS, and a residual buffer can also hold strict-mode rows whose waiters were answered 503 and will retry, so the count is an upper bound on tenants that lost acknowledged data. The same drain also logs an ERROR per residual shard, carrying that shard's tenant count. See [Reachability during shutdown](#reachability-during-shutdown) below for when a scrape can see this change. |
| `ravel_ingest_clock_lag_refused_total` | Flushes refused because the writer's flush-open clock reading lagged the object store's observed clock (the latest response `Date` the store adapter saw) by more than the five-minute clock-skew allowance. Strict waiters get the retryable 503 and the buffer goes back for the next trigger. Nothing is written. A rising figure means this host's clock runs behind the store's and, without the refusal, publishes into an ingest hour the fold can already have sealed: fix the host clock. See [the writer clock-lag alert](#the-writer-clock-lag-alert). |
| `ravel_ingest_clock_lag_unchecked_total` | Flush-open attempts that found no store-clock observation yet, so the lag check did not run and the flush proceeded unchecked. Cumulative and never reset: the attempts a process makes before its first store response stay in the total, so the signal is a figure still growing past the process's first minute, which is a wiring defect against a store that reports a clock. A `MemoryStore`-backed process reports none, so there it grows by design. |
| `ravel_ingest_clock_lag_bypassed_at_shutdown_total` | Flush-open attempts on a `Shutdown` or channel-close drain that found a lagging reading and went on with the lag check bypassed, rather than strand rows buffered mode had already acknowledged. It counts the bypass, not the publication: the monotonic floor still applies on those passes. Those rows can land in an ingest hour the fold has sealed, invisible to token-less reads until a catalog HEAD rebuild. Each bypass also logs at WARN naming the measured lag. A nonzero figure means a writer was shut down with a lagging clock: fix the host clock, and rebuild the HEAD if a token-less read is missing the rows. |

Three families carry fewer signals. The missing samples are structurally
absent, not zero:

| Family | Series it carries | Reason |
|---|---|---|
| `ravel_ingest_collisions_total` | No `signal="spans"` series. | Spans derive no identity that can collide. |
| `ravel_ingest_idempotency_lookup_failures_total` | Only `signal="logs"` and `signal="spans"`. | Metrics requests take no idempotency key. |
| The two exemplar families | Only `signal="metrics"`. | Exemplars ride on metric points. |
| `ravel_ingest_flushes_by_age_adaptive_total` | Only `signal="metrics"`. | The adaptive-delay corridor is a metrics-pipeline feature. |

These families render for all three signals, so a logs-only or spans-only
process still renders a real (possibly zero) sample for each:

- `ravel_ingest_flushes_by_age_floor_total`
- `ravel_ingest_in_flight_flushes`
- `ravel_ingest_flush_permit_wait_seconds_total`
- `ravel_ingest_queued_flushes`
- `ravel_ingest_flush_trigger_deferred_total`
- `ravel_ingest_grace_extended_stale_flushes_total`
- `ravel_ingest_flush_all_residue_tenants_total`
- The three `ravel_ingest_clock_lag_*` families

#### The writer clock-lag alert

A refused flush means that a writer's clock runs behind the object store's
clock by more than the clock-skew allowance. The refusal does not clear until
the host clock converges. Nothing that the flush does moves either clock, so
every retry refuses again.

The refused rows are never dropped. The flush puts them back in the buffer,
in strict and buffered acknowledgement mode alike. The buffer grows until the
process-wide byte budget is full. After that, new writes to that process are
shed with HTTP 429 (gRPC `RESOURCE_EXHAUSTED`).

The rule ships as the `ravel-ingest-clock` group.

```yaml
groups:
  - name: ravel-ingest-clock
    rules:
      - alert: RavelWriterClockLagRefused
        expr: |
          increase(ravel_ingest_clock_lag_refused_total[10m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: >-
            A Ravel writer's clock runs behind the object store's clock, so its
            {{ $labels.signal }} flushes are refused
```

The other two clock-lag counters do not page:

- `ravel_ingest_clock_lag_unchecked_total` counts flushes made before any
  store time was observed. The observation is kept per store instance. The
  server's startup requests and its ingest writes share the default store,
  whose S3 connector records the `Date` of every response. On S3 the expected
  value is therefore 0. A nonzero value there is a wiring defect to file, not
  an incident.
- `ravel_ingest_clock_lag_bypassed_at_shutdown_total` is teardown residue. It
  moves on a process that is already exiting, so an alert on it fires after
  the process that could act on it is gone. Read it after a rollout, beside
  the history of the refusal counter.

#### Per-tenant PUT attribution (`ravel_ingest_attribution_puts_total`)

Labels: `mode`, `tenant_hash`, `signal`.

| Metric | Meaning |
|---|---|
| `ravel_ingest_attribution_puts_total` | Object-store PUT requests attributed to completed flushes, by tenant and signal. |

Use this family to find which tenant generates the PUT bill. Each completed
flush charges 2 PUTs (a data object and a commit record) to the flushing
tenant. The ingest router tracks the charge per signal in a bounded top-K
structure that follows at most 1024 tenants.

The `tenant_hash` label is bounded separately, by the
`--metrics-tenant-labels` allowlist and the
[`tenant_hash="other"` fold](#the-tenant_hashother-fold). A tenant outside
the allowlist never gets its own series here, however much it contributes to
the top-K table.

### Per-shard ingest skew (`ravel_ingest_shard_*`)

Labels: `mode`, `signal`, `shard`. No sample combines `shard` with
`tenant_hash`.

| Metric | Meaning |
|---|---|
| `ravel_ingest_shard_messages_enqueued_total` | Write messages the router sent into this shard's channel (`Write` on every signal, plus `WriteColumnar` on logs). |
| `ravel_ingest_shard_messages_processed_total` | Write messages this shard's actor pulled and handled. |
| `ravel_ingest_shard_queue_depth` | Gauge. `messages_enqueued - messages_processed` at read time, saturating at zero: write messages still in the channel. Flush and shutdown requests are not counted. |
| `ravel_ingest_shard_on_actor_seconds_total` | Merge-and-pin work the single-threaded shard actor genuinely serialises, nanoseconds rendered as seconds. |
| `ravel_ingest_shard_flush_permit_wait_seconds_total` | This shard's flush backpressure: a sum over concurrently waiting tasks, so it can exceed wall time. |
| `ravel_ingest_shard_off_actor_seconds_total` | The whole of this shard's flushes once their permit is granted: encode, the data-object PUT, and the commit-record publish on every signal, plus exemplar admission on the metrics pipeline only. |

How the family renders:

- An idle shard renders at zero, up to the configured `--shards` count. The
  renderer emits a sample for every index from 0 to `--shards - 1`, plus any
  index at or above it that has recorded activity. An idle shard that reads
  zero is a finding, not an absent series.
- The bound is the configured default (each router's `shard_count()`), not
  the live shard count of a tenant. A tenant resharded above the default
  renders its extra shards only after they record something. An idle shard
  above the default does not render.
- The accumulator is keyed by shard index alone. During a reshard, or when
  tenants run different shard counts, one `shard` value sums the actor of
  every generation at that index.
- Each gateway or `all` process adds `6 * signals * shards` series to its
  scrape when no tenant's shard count differs from the configured one. Three
  signals and the `--shards` default of 4 make 72. The count grows when a
  tenant resharded above the default records activity on a higher index.

See [the per-shard skew section of the ingest
reference](../ingest.md#per-shard-skew) for the meaning of the six underlying
`ShardSkewStats` fields and the ordering guarantees that a single read does
and does not carry.

A pinned shard shows as one `shard` value that carries the load the others do
not. Compare `rate(ravel_ingest_shard_messages_enqueued_total[5m])` across
`shard` for a fixed `signal`. A collector that sends every stream from one
resource attribute set lands entirely on one series. Raising `--shards` does
not spread that load. See [the ingest
guide](ingest.md#per-pod-resource-attributes-and-shard-spread) for the
collector-side fix.

### Log postings and dynamic columns

Labels: `mode` and `signal`. One series renders per pipeline that builds a
POSTINGS index, so an idle pipeline that is configured still renders its
zero. These families are ingest-side and render where ingest runs.

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

- A climbing `ravel_logs_dynamic_columns_overflowed_total`, or a
  `ravel_logs_dynamic_columns_used_max` gauge near the configured budget,
  means that a tenant's attribute set has outgrown its `max_dynamic_columns`.
  Ravel then folds attributes into the raw column.
- A climbing `ravel_logs_postings_capped_fields_total` means that a single
  high-cardinality field hits the per-field distinct-value cap.

### Query-side pruning (`ravel_logs_prune_*`)

Labels: `mode` and `signal="logs"`. These query-side counters cover the logs
scans and render where queries run.

| Metric | Meaning |
|---|---|
| `ravel_logs_prune_blocks_total` | Blocks the logs scans considered before postings pruning, cumulative (the denominator of prune selectivity). |
| `ravel_logs_prune_blocks_survived_total` | Blocks that survived postings pruning and were scanned, cumulative (the numerator of prune selectivity). |
| `ravel_logs_prune_blocks_pruned_by_postings_total` | Blocks dropped by the POSTINGS index before scanning, cumulative. |

Prune selectivity is `survived / total`. A ratio near 1 means that the
POSTINGS index drops almost nothing and the scans read most blocks. Either
the queries use predicates that the index cannot serve, or the index does not
discriminate for this workload.

### Catalog integrity (`ravel_catalog_*`)

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_catalog_interlock_violations_total` | Unlisted L0 commit records observed postdating a compaction record in their bucket. |
| `ravel_catalog_compaction_input_set_conflicts_total` | Buckets observed holding two compaction records with different input_set_hash. |
| `ravel_catalog_isolation_breach_total` | Hard-failed queries from a HEAD or postings tenant_hash mismatch or an out-of-prefix listing result. |
| `ravel_catalog_column_stats_decode_refusals_total` | Column-statistics objects the HEAD referenced that the reader refused to decode: an oversized declared body, corruption, or a checksum or header failure. Counted on every load, not once per object. |
| `ravel_catalog_column_stats_decode_panics_total` | Column-statistics decode jobs that panicked on the read CPU gate. Counted on every load. A job the gate cancelled at shutdown is not counted. |

The first two counters count an anomaly that the query resolves past. Each
`ravel_catalog_isolation_breach_total` increment is a query that failed with
an explicit isolation-fault error.
[Troubleshooting](operations/troubleshooting.md) gives its alert rule.

The two column-statistics counters fail no query: a query whose statistics
object cannot be decoded reads the data instead, at the cost of scanning what
the statistics would have pruned. A figure that keeps climbing means a folded
HEAD keeps pointing at an object that cannot be decoded, so that tenant runs
with no column statistics for it. A WARN naming the tenant, signal and object
key is logged once per object.

### Catalog fold liveness (`ravel_catalog_fold_*`)

Labels: `mode`, `signal`. Each folded signal (`metrics`, `logs`, `spans`) has
one series. The mode decides which of the four families a process renders:

| Mode | Renders | Reason |
|---|---|---|
| `maintain`, `all` | All four. | These modes run the scheduled fold. |
| `query` | The two counters. No gauge and no restart counter. | It folds only on a call to the on-demand `POST /api/v1/admin/fold` route, so the failures of an on-demand fold are visible on `/metrics`. It has no scheduled loop to restart. Nothing makes an on-demand fold recur, so the mode has no liveness gauge. |
| `gateway` | None. | It mounts no fold route. |

`--disable-fold` stops the loop inside a folding mode. Such a process still
renders all four families and reports zeros permanently.

The `ravel_catalog_fold_stamped_*` pair shares this prefix and belongs to
[Declared-column statistics](#declared-column-statistics), which states where
the pair renders.

| Metric | Meaning |
|---|---|
| `ravel_catalog_fold_cycles_total` | Catalog folds of this signal that completed successfully, no-op folds included. |
| `ravel_catalog_fold_failures_total` | Catalog folds of this signal that failed. The fold retries on the next tick and never fails a query directly. |
| `ravel_catalog_fold_last_success_timestamp_seconds` | Gauge. Unix time of the last successful fold of this signal in this process, `0` if none has succeeded since it started. |
| `ravel_catalog_fold_loop_restarts_total` | This signal's fold loop caught panicking and restarted by its supervisor in this process. Rendered under the same rule as the gauge above: only where the fold runs on a schedule. |
| `ravel_catalog_fold_refold_requests_dropped_total` | Pending re-fold requests evicted from this process's in-memory queue because it already held 256 `(tenant, signal)` pairs, one per evicted pair. Labelled by `mode` only: one queue serves every signal. A maintain sweep that finds an hour held by a named snapshot queues it for the scheduled fold of that pair. An evicted request is sent again by a later sweep pass that finds the hold. A pair removed because the process no longer folds it, or because the fold no longer maintains the tenant, is not counted here. Rendered under the same rule as the gauge above, and moves only in a `maintain` process whose scheduled fold is enabled: `--disable-fold` there leaves the queue unfed, and in mode `all` no sweep runs. |

Read the age of the gauge. The two counters move while the fold runs. When
the fold stops, only the age of the gauge moves. A stopped fold gets worse
with time: the unsealed span grows for as long as nothing seals it. A cold
recent-window query over a wide enough unsealed span then exceeds its
per-query object-store request budget and is refused.

A no-op fold counts as a cycle and advances the gauge. A fold seals an ingest
hour only after `max_flush_lifetime + clock_skew_allowance +
fold_safety_margin` has elapsed past the end of that hour, so on a quiet
tenant almost every cycle publishes nothing.

The fold runs as one independent task per signal, each with its own loop. One
signal's fold can stop while the other two keep running, so each signal has
its own series.

Each loop runs under a supervisor. The supervisor catches a panic in a tick
body, counts it on `ravel_catalog_fold_loop_restarts_total` for that signal,
and logs it at error level. It respawns the loop after a bounded backoff that
doubles from 1 s to 60 s and resets when an attempt completes a tick. A
respawned attempt ticks as soon as its backoff ends, not after a further fold
interval. At the defaults:

| Loop behaviour | Restarts counted |
|---|---|
| A single transient panic | 1. The panic costs one skipped tick. |
| A panic on every other tick | About 3 per 15 minutes, one per fold interval. |
| A panic on every tick | 20 in the first 15 minutes and 15 in every 15 minutes after. The loop restarts at 0, 1, 3, 7, 15, 31, 63 and 123 s after its first panic and every 60 s after that. It folds nothing. |

`RavelCatalogFoldLoopCrashLooping` fires on more than 5 restarts in 15m. A
loop that panics on every tick crosses that threshold 31 s after the first
panic. The other two rows stay under it. The liveness gauge cannot report a
crash loop on one replica: the replica keeps heartbeating and keeps its
pairs, while its peers hold the fleet-wide maximum fresh.

#### The fold-stalled alert

The rules ship as the `ravel-catalog-fold` group.

```yaml
groups:
  - name: ravel-catalog-fold
    # RavelCatalogFoldStalled fires on any deployment where some signal has
    # no fresh fold, which includes a fleet that never folds at all: a fleet
    # running neither --mode maintain nor --mode all (the two modes that fold
    # on a timer, ADR-1693), or one running --disable-fold everywhere. Such a
    # fleet must drop this rule or inhibit it; the state walkthrough below
    # explains why that opt-out is deliberate. A gateway-only or query-only
    # fleet is in that class. Neither mode renders
    # ravel_catalog_fold_last_success_timestamp_seconds, so the `absent()` arm
    # is what fires there. A query process does render
    # ravel_catalog_fold_cycles_total and _failures_total, for the on-demand
    # `POST /api/v1/admin/fold` route it keeps; a gateway process folds by no
    # route and emits no fold series at all.
    rules:
      - alert: RavelCatalogFoldStalled
        expr: |
          (
            time() - max by (signal) (
              ravel_catalog_fold_last_success_timestamp_seconds
            ) > 4800
          )
          or
          absent(ravel_catalog_fold_last_success_timestamp_seconds)
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: >-
            No Ravel process has completed a catalog fold of signal
            {{ $labels.signal }} for longer than the unsealed ingest span the
            configuration allows
          description: >-
            The unsealed span for this signal grows for as long as this holds,
            and a cold recent-window query over a wide enough span is refused
            for exceeding its object-store request budget. Check
            ravel_catalog_fold_failures_total for the same signal for a fold
            that is running and failing, and the fold task's logs for the
            underlying store error.
      - alert: RavelCatalogFoldLoopCrashLooping
        # The shape RavelCatalogFoldStalled cannot catch since the fold became
        # partitioned. A replica whose loop for one signal keeps panicking goes
        # on heartbeating, so it stays in the live set and keeps its pairs,
        # while its peers' fresh gauges hold max by (signal) under the stalled
        # threshold. This counter is the only figure that moves.
        # At the defaults (300 s fold interval, restart backoff 1 s doubling
        # to 60 s) a restarted attempt ticks as soon as its backoff ends, so
        # a loop panicking on every tick restarts at 0, 1, 3, 7, 15, 31, 63
        # and 123 s after its first panic and every 60 s after that: 20
        # restarts in its first 15m and 15 in every 15m after. A single
        # transient panic is 1, and a loop panicking on every other tick is
        # about 3, one per fold interval. More than 5 is crossed 31 s into a
        # crash loop and stays crossed, so the alert fires about 15.5 minutes
        # after the loop starts panicking, plus scrape and evaluation delay.
        # Panics only: a tick hung on a store call that never returns moves
        # no counter here.
        expr: |
          increase(ravel_catalog_fold_loop_restarts_total[15m]) > 5
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: >-
            A Ravel catalog fold loop for {{ $labels.signal }} is panicking and
            restarting repeatedly
          description: >-
            The supervisor is catching a panic and restarting this signal's
            fold loop faster than the loop is making progress, so the pairs
            this replica owns are going unfolded while the rest of the fleet
            looks healthy. Read this process's logs for the panic itself. No
            aggregation, unlike RavelCatalogFoldStalled: the point is the one
            replica, and a peer's health must not average it away.
      - alert: RavelCatalogFoldFailing
        expr: |
          sum by (signal) (rate(ravel_catalog_fold_failures_total[15m])) > 0
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: Ravel catalog folds of {{ $labels.signal }} are failing
          description: >-
            The fold retries each tick, so a transient store fault clears on
            its own. A sustained failure rate does not, and it precedes
            RavelCatalogFoldStalled by however long the threshold there
            allows.
```

A fleet that never folds must opt out of `RavelCatalogFoldStalled`. The rule
fires on a fleet that runs neither `maintain` nor `all`, and on one that runs
`--disable-fold` everywhere. Drop the rule or inhibit it on such a fleet.
With `--disable-fold` set, the rule pages about ten minutes after start, so
silence it when you set the flag. The page is a true report: the unsealed
span grows without a fold to seal it.

How to read the `RavelCatalogFoldStalled` expression:

- **`max by (signal)` aggregates across the fleet.** A replica can correctly
  have a stale gauge. Each `maintain` process owns the tenant/signal pairs
  that the rendezvous hash assigns it. A peer folds a pair that the process
  does not own, and that fold never stamps the gauge of the process. The loop
  also skips its tick when `HEAD` is already fresher than `fold_interval`.
- **The grouping is by `signal`.** Each process has three independent fold
  loops, one per signal. One replica's idle loop is healthy while a peer
  still folds and stamps that signal. An ungrouped `max()` collapses the
  three signals into one number that two healthy loops keep fresh while the
  third is dead fleet-wide. With the grouping, the alert carries the `signal`
  whose history stopped sealing.
- **One process's loop is a single point of failure in two cases.** The
  first is a deployment with one folding replica. The second is a
  signal-specific fault that stops that loop on every replica at once.
- **`or absent(...)` covers a family that is not scraped at all.** Examples
  are a fleet scaled to zero, a fleet that crash-loops fast enough that its
  targets go stale, and a scrape-config edit that drops the job. The
  staleness operand is empty in those cases and stays silent. An alert from
  the `absent()` branch carries no `signal` label. It renders an empty
  `{{ $labels.signal }}` and means that the whole family stopped arriving.
- **The expression has no `mode` filter.** `maintain` runs the scheduled
  fold, so a `mode!="maintain"` filter discards the only fresh sample on a
  maintenance-only fleet. `gateway` and `query` do not fold on a timer and
  render no fold series at all, so they contribute no `0` for a filter to
  exclude.

What the rule does in each state of the fleet:

| What the fleet is doing | Series at the scrape | Staleness operand | `absent()` operand | Alert |
|---|---|---|---|---|
| Nothing scraped at all | none | empty | fires | **fires** |
| Only `gateway` and `query` nodes scraped, intentionally | none | empty | fires | **fires** (opt out) |
| Healthy folding fleet | 3 per `maintain`/`all` process, fresh on every replica that owns pairs | under threshold for all 3 signals | silent | silent |
| Healthy fleet, one `maintain` replica the hash gives no pairs | 3 on that replica, all at `0`, fresh on its peers | under threshold: the fleet-wide max is a peer's fresh sample | silent | silent |
| Folding fleet scraped, every fold loop stalled | 3 per folding process, all stale | over threshold for all 3 signals | silent | **fires** |
| Every `maintain` node dead, `gateway`/`query` nodes alive | none | empty | fires | **fires** |
| One signal's loop crash-looping on every folding replica | 3 per folding process: 2 fresh, 1 stale | over threshold for that one signal | silent | **fires** for that signal |
| One signal's loop crash-looping on one replica of several | 3 per folding process, and the peers' stay fresh | under threshold: the fleet-wide max is a peer's | silent | silent. `RavelCatalogFoldLoopCrashLooping` is what fires |
| `--disable-fold` on every folding process | 3 per folding process, all at `0` | `time() - 0` over threshold for all 3 signals | silent | **fires** (opt out) |
| Fleet whose tenants write only one signal | 3 per folding process, all fresh | under threshold for all 3 signals | silent | silent |

Notes on the rows:

- **One `maintain` replica that the hash gives no pairs.** The replica folds
  nothing for that signal, never stamps that gauge, and reports the `0`
  sentinel for its whole process life. That reading is correct, so
  per-instance staleness is not a usable condition.
- **One signal's loop crash-looping on one replica of several.** The pairs of
  that replica go unsealed while `max by (signal)` reports a peer's fresh
  sample. `RavelCatalogFoldLoopCrashLooping` covers this case and has no
  aggregation for that reason.
- **Tenants that write only one signal.** Every loop folds every discovered
  tenant that it owns for its signal on every tick. A fold over a signal that
  a tenant never writes is a healthy no-op cycle and stamps the gauge. A
  fleet that ingests only logs still has all three gauges fresh on the
  replicas that own pairs.

Two pairs of states produce identical telemetry, and the rule fires on all
four:

- "Only `gateway` and `query` nodes scraped, intentionally" and "every
  `maintain` node dead" both show no fold series at all. The expression
  cannot tell an intended topology from a fleet-wide death.
- "`--disable-fold` everywhere" and "every fold loop crashed before its first
  success" both show every gauge at its `0` sentinel under a full set of
  folding-mode series.

No shipped alert catches a hung loop on one replica of several:

- The supervisor covers panics only. A tick that hangs, for example on a
  store call that never returns, neither panics nor completes. The restart
  counter does not move and `RavelCatalogFoldLoopCrashLooping` stays silent.
- That replica's `ravel_catalog_fold_last_success_timestamp_seconds` and
  `ravel_catalog_fold_cycles_total` for the signal stop, while the same
  series of its peers keep moving.
- `RavelCatalogFoldStalled` reads `max by (signal)`, so a peer's fresh sample
  holds it under its threshold. It fires only when the loop of every folding
  replica for that signal is stalled, which includes a deployment with one
  folding process.
- To detect a hung loop, read that gauge per instance against the replicas
  that you expect to own pairs. A gauge that stands still is also the healthy
  reading of a replica that the hash gives no pairs, and of one that owned
  pairs and lost them to a scale-up. This family exports nothing that
  separates those readings from a hung loop.

The threshold is the unsealed span that the catalog configuration implies, in
seconds:

| Term | Default | Seconds |
|---|---|---|
| `max_flush_lifetime` | 1 hour | 3600 |
| `clock_skew_allowance` | 5 minutes | 300 |
| `fold_safety_margin` | 15 minutes | 900 |
| **Sum** | **1 h 20 min** | **4800** |

A healthy fold never seals the last 3600 + 300 + 900 = 4800 seconds. A fold
that has not succeeded for longer than that leaves history unsealed that the
configuration intends to be sealed. Change the threshold together with those
three settings.

The headroom at the defaults:

- `fold_interval` defaults to 5 minutes and the loop adds up to 10% jitter,
  so the sleep between two cycles is at most 330 seconds.
- 4800 is about 14 missed ticks of margin over that sleep ceiling. No single
  slow cycle, restart, or rolling deploy reaches it.
- The widest healthy gauge age is more than 330 s. The loop stamps the gauge
  before each tenant's fold and re-stamps per tenant within a cycle. The
  widest healthy gap runs from the last tenant's stamp in one cycle to the
  first tenant's stamp in the next. That gap is 330 s, plus the
  tenant-discovery LIST that opens the cycle, plus the fold duration of that
  first tenant, which is tens of seconds on a large tenant.
- If you tighten the threshold on a fleet with a shorter `fold_interval`, use
  that fuller form and not the 330.

`for: 10m` covers process start. The gauge reads `0` until the first fold
succeeds, so `time() - 0` exceeds any threshold at once. The first scheduled
fold lands one `fold_interval` plus jitter after start. Ten minutes is a
little under two intervals of grace. Because the series reads `0` and is not
omitted, the one expression covers a fold that stopped and a fold that never
worked.

Two limits apply to this rule:

- The gauge is per signal and not per tenant. A signal's series stays fresh
  when one tenant's fold is stuck behind a permanent fault while every other
  tenant of that signal folds normally. Find a stuck single tenant through
  `ravel_catalog_fold_failures_total` and the per-tenant logs of the fold
  task.
- A deployment that has discovered no tenants folds nothing and trips this
  rule. Scope the group to deployments that serve traffic.

### Declared-column statistics

Three families cover the per-declared-column min/max stamps: one defect
tally on the read side, and one coverage pair at the fold.

| Metric | Labels | Meaning |
|---|---|---|
| `ravel_declared_stats_drops_observed_total` | `mode`, `carrier` | Declared-column statistics entries a reader dropped as defective, by the carrier it was reading. |
| `ravel_catalog_fold_stamped_records_total` | `mode` | Carriers of declared-column statistics that the fold read: L0 commit records and L1 compaction parts. |
| `ravel_catalog_fold_stamped_entries_total` | `mode` | Snapshot entries the fold built carrying declared-column statistics, from either carrier. |

`carrier` is a closed set of four:

| `carrier` | Source |
|---|---|
| `commit-record`, `compaction-part` | The two stamp carriers. |
| `snapshot-entry` | The fold's copy of them. |
| `cstat` | The `ColumnStat` entries of the `.cstat` object, whose reader lives in `ravel-sql`. |

How to read the drop tally:

- **It counts observations, not distinct defects.** One defective record
  read by a thousand queries counts a thousand times. Compare its rate across
  equal windows. Never compare its magnitude across windows of different
  query volume. The [query engine guide](../query-engine.md) states the same
  semantics next to the predicate that the drops come from.
- **A flat `snapshot-entry` series means "not reported separately".** It
  never means "the fold's copies are clean". The series renders in every
  exposition and no shipped code path increments it. The catalog re-validates
  a snapshot entry by converting it to its commit-record twin and reading it
  through the commit-record reader. A drop on the fold's copy is therefore
  observed under `commit-record`, along with the fold's own reads of commit
  records. Do not build a remediation step on `snapshot-entry`.
- **It renders in every mode, including `maintain`.** The compaction that
  only `maintain` runs observes `compaction-part`.

The two fold families are the coverage pair. They show whether a stamp that
was written reaches the snapshot:

- `ravel_catalog_fold_stamped_records_total` counts the carriers with
  statistics that the fold read. It covers both paths that a fold builds a
  stamped entry from: L0 commit records (field 20) and L1 compaction parts
  (field 12).
- `ravel_catalog_fold_stamped_entries_total` counts the snapshot entries
  with statistics that the fold then built, from either path, as it builds
  each entry.
- Neither side counts rewrite output parts. A rewrite drops rows, so the fold
  never carries a stamp computed before the drop.
- Both are process-wide totals. The fold adds to them when it commits its
  `HEAD`, so a fold attempt that lost its compare-and-swap and retried
  contributes nothing.

In the healthy state the two rise together, one entry per stamped carrier.

The coverage pair renders only where a fold can run in the process, by either
route. Both routes accumulate the totals:

| Route | Where it exists |
|---|---|
| The scheduled fold task | `maintain` and `all`. `--disable-fold` disables it there. |
| The on-demand `POST /api/v1/admin/fold` route | `all` and `query`, whatever `--disable-fold` says. |

A `--mode all --disable-fold` process therefore renders the pair, and a fold
by hand moves the counters. Both series are absent, not zero, where neither
route exists: every `gateway` process, and a `maintain` process run with
`--disable-fold`. A scrape of one of those shows
`ravel_declared_stats_drops_observed_total` and no
`ravel_catalog_fold_stamped_*` series.

#### The stamp-coverage shortfall alert

The rules ship as the `ravel-declared-stats` group.

```yaml
groups:
  - name: ravel-declared-stats
    rules:
      - alert: RavelFoldStampCoverageShortfall
        expr: |
          sum(increase(ravel_catalog_fold_stamped_records_total[1h]))
            >
          sum(increase(ravel_catalog_fold_stamped_entries_total[1h]))
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: >-
            The Ravel fold is reading more stamped carriers than it is writing
            stamped snapshot entries
          description: >-
            Declared-column statistics are being read off commit records or
            compaction parts and not carried onto the snapshot entries the
            query side prunes with, so pruning silently degrades to a full scan
            on the affected segments. A shortfall means every stamp of some
            carrier was dropped as defective, so
            ravel_declared_stats_drops_observed_total names the writer:
            compare its rate on carrier="commit-record" against
            carrier="compaction-part" over the same window as the shortfall.
            Read carrier="commit-record" as the union of two readers, the fold
            reading commit records and the query side re-validating snapshot
            entries, which reports under that same label.
      - alert: RavelFoldStampCoverageMissing
        expr: |
          absent(ravel_catalog_fold_stamped_records_total)
          and
          (
            sum(increase(ravel_ingest_flushes_by_size_total{signal="logs"}[1h]))
            +
            sum(increase(ravel_ingest_flushes_by_age_total{signal="logs"}[1h]))
            +
            sum(increase(ravel_ingest_flushes_manual_total{signal="logs"}[1h]))
          ) > 0
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: >-
            Ravel is flushing log segments but no process reports fold stamp
            coverage
          description: >-
            No process in this deployment renders the stamp-coverage families
            at all, so nothing can say whether the stamps the ingest side
            writes are reaching the snapshot. What it detects is the absence
            of the families, not that folds are old: one `all` or `query`
            process of a new enough build renders both, whether or not it ever
            folds, and this rule goes quiet from then on even if every process
            that actually folds is still on the old shape. Roll the fold
            processes, and read the rest of the rollout off the per-pass
            `ravel-cli catalog fold` figures per folding process. For a
            deployment that deliberately folds nowhere, RavelCatalogFoldStalled
            is the rule that covers it; scope this one out.
```

An old fold cannot emit a counter that it does not have, so the group has two
rules:

- `RavelFoldStampCoverageShortfall` covers a divergence between the two
  counters.
- `RavelFoldStampCoverageMissing` covers an absent fold-side family while the
  ingest side rises. It needs the ingest term. An absent fold family alone is
  also the reading of a `maintain`-only deployment, a deployment that can
  fold by neither route, and an idle deployment.

How to read the shortfall rule:

- **`sum()` on both sides.** The fold loop skips its tick when `HEAD` is
  already fresher than `fold_interval`. A replica whose peers folded
  correctly reads zero on both counters, so a per-instance comparison
  compares two zeros.
- **`increase(...[1h])`, not the raw totals.** Both are monotonic
  process-lifetime counters. A process that had a shortfall once and is
  healthy since keeps the raw gap forever.
- **`for: 5m` holds against one anomalous sample and nothing else.** A fold
  pass adds to both totals as two adjacent increments when it commits its
  `HEAD`, so the totals move together. A scrape can land between the two
  increments and show either total ahead of the other. Such a sample skews
  the windowed increase for one scrape interval, and a few intervals of
  agreement rule it out.
- **A shortfall that survives the hold is live.** Every stamp of some carrier
  was dropped. Work it through the drop tally. It does not clear by waiting.

The rules and the drop tally cannot see these cases:

- **A mixed-version fleet past its first upgraded process.** The families
  render on any `all` or `query` process of a new enough build, whether it
  folds or not. One such process makes `absent()` false. The shortfall rule
  then compares the healthy, equal contribution of that process, while the
  old folders emit nothing. For the rest of a rollout, read the per-pass
  figures in the fold report against each folding process. A fleet-wide sum
  cannot attribute a zero contribution to an old folder.
- **A stamp that was never written.** Both counters live on the fold. An
  ingest pipeline that stops stamping drives both to zero together, which
  reads the same as an idle tenant. The drop tally counts stamps that a
  reader rejected, so a stamp never written moves nothing. A flat
  `carrier="commit-record"` line is not evidence that stamping is healthy.
  For a rollout, alert on an ingest-side rate that moves while both fold
  counters stay at zero. That reading means that the fold reads records that
  carry no stamps.
- **A compactor of an older shape.** The ingest-side comparison covers the
  ingest half of a rollout only. A compactor that seals compaction parts with
  an empty statistics list is invisible to every series in this section. The
  fold counts a carrier only when its list is non-empty. The drop tally
  counts rejected entries, and an empty list has none. The stamped L0 records
  that the compactor consumed also leave the pair: when a compaction record
  wins, the fold excludes its inputs before the tally.

A fleet with upgraded ingest and fold and one old compactor keeps both fold
counters rising in step at the L0 rate, with their ratio at one. Every L1
segment that the compactor seals is uncovered for every declared-column
statistic, and every query over it gives up the shortcut. Two things show
that state, and neither is in the exposition:

- **The fold report.** `ravel-cli catalog fold` prints `stamped_records` and
  `stamped_entries` for the pass it just ran. A fold of one tenant whose
  recent hours were just compacted reports what those parts contributed as
  carriers. A pass that folds compaction output and counts no carriers for it
  is the signal that a fleet-wide sum hides.
- **Rollout order.** Compaction runs only in `maintain`, a `maintain` process
  renders neither fold counter, and no counter on any process reports which
  shape a compactor writes. Upgrade every `maintain` process before you read
  the pair as an answer about the whole fleet.

### Tenancy adoption (`ravel_tenancy_v1_unkeyed_adoptions_total`)

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_tenancy_v1_unkeyed_adoptions_total` | Buckets this process pinned to the unkeyed tenant hash when it adopted a bucket that held `t/` data but no `sys/tenancy` marker. |

A nonzero value shows that the one-time migration happened.

### Provisioning

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_provisioning_shard_count_mismatch_total` | Provisioning checks that failed hard: an unreadable record, a decodable record whose generation history fails structural validation, or pre-ADR data a lower `shard_count` would hide. Alert on any increase. [Troubleshooting](operations/troubleshooting.md) gives its rule. |
| `ravel_provisioning_shard_count_drift_total` | Validations where a decodable record with a structurally valid generation history had a recorded `shard_count` that differed from the live `--shards` default (a record that fails structural validation is counted by the mismatch counter above, never here). The drift is tolerated and routing uses the record's own generation history, so this is informational: a nonzero value is expected after lowering the global default, not a fault. |

### Store reachability

Labels: `mode`. All three samples come from the background store-reachability
probe.

| Metric | Meaning |
|---|---|
| `ravel_store_reachable` | Gauge. 1 when the probe reports the store reachable, 0 after K consecutive failed probes. |
| `ravel_store_probe_failures_total` | Every failed probe cycle, monotonic, incremented even below the readiness threshold. |
| `ravel_store_probe_last_run_timestamp_seconds` | Gauge. Unix time of the probe task's last completed cycle or of its spawn, whichever is later. A cycle stamps it whether it succeeded or failed. A reading of `0` has three causes: see [What `0` means](#what-0-means) below. |

`ravel_store_reachable` and `ravel_store_probe_failures_total` are written
only while the probe task runs. The task has no restart path, and nothing
observes its exit. If the task dies, both freeze at their last values.
`ravel_store_reachable` most often freezes at 1, and `/readyz` then reads
that stale value as healthy.

`ravel_store_probe_last_run_timestamp_seconds` catches a dead probe task.
Read its age, not its value. A failing probe still completes a cycle and
advances this gauge every `--store-probe-interval`.

Alert on `time() - ravel_store_probe_last_run_timestamp_seconds > 132`,
`for: 5m`. One rule covers every case, with no second term and no companion
rule, for the reasons that [What `0` means](#what-0-means) gives.

The threshold:

- `132` is `K * interval * 1.1`. The default probe interval is 30s, the probe
  sleeps a jittered interval that adds up to 10%, and `K` is 4, so
  `4 * 30 * 1.1 = 132`.
- `132` scales with `--store-probe-interval`. For a non-default interval,
  recompute `132`. Also recompute the margin that `for: 5m` relies on,
  `200 - (K - 1) * interval * 1.1`. A larger interval grows the sleep term
  inside both the threshold and the gap. Recompute both before you tighten
  the threshold or `for:`.

The `for: 5m` window:

- `132` is the sleep-only ceiling between two scheduled probes. It is not the
  ceiling on the gap between two completed cycles. A probe cycle has no
  deadline, so one cycle can run as long as the retry budget of the backend
  allows. For the S3 backend that budget is `retry_timeout + request_timeout`
  ≈ 180s + 20s = 200s.
- The worst-case gap between two live cycle completions is therefore
  `interval * 1.1 + 200s = 33 + 200 = 233s` for the default interval.
- During a store outage the comparison is true for `233 - 132 = 101s` per
  cycle, until the next completed cycle re-stamps the gauge. That is well
  under the 300s `for:` window. A probe that fails and still runs does not
  fire this rule. Only a probe that has stopped completing cycles keeps the
  comparison true past `for: 5m`.
- `RavelStoreUnreachable` pages for the outage. It carries no `for:`, so it
  fires as soon as `ravel_store_reachable` flips to 0, on the `K`th
  consecutive failed cycle (`K` is 4). At the default interval that is about
  `4 * 33 = 132s` after the outage starts. It is at most `4 * 233 = 932s`,
  when every one of those cycles runs its full retry budget before it fails.
- Do not shrink `for:` on the strength of the 33s sleep-only ceiling. A
  `for: 1m` pages on every ordinary store outage, with the wrong cause.

That derivation is complete only without `--store-scheduling`. With that
flag:

- The probe's GET goes through the foreground request class. It waits on the
  global semaphore with no timeout.
- During a store outage every in-flight foreground operation can run for the
  full 200s. The permits stay saturated, and the probe's GET can wait in the
  queue for minutes before its cycle starts.
- The gap between completions then exceeds `132 + 300 = 432s` on the shipped
  thresholds. `RavelStoreProbeStalled` fires, and its description says that
  the task has likely died while the task is alive and queued.
- The queue wait has no bound. A fleet that runs `--store-scheduling` must
  size `for:` against its own measured foreground saturation window.
  Otherwise a long enough outage pages under this alert as well as under
  `RavelStoreUnreachable`.

#### What `0` means

<!-- claim:store-probe-zero:canonical-begin -->
This section is the single source for what a `0` on
`ravel_store_probe_last_run_timestamp_seconds` means. Every other place in this
repository that touches the question points here rather than restating it, and
`scripts/guards/check-claim-single-source.sh` fails the build on a restatement
that is not a pointer. The one deliberate exception is the gauge's own `HELP`
line in `/metrics` output, where the reader has no link to follow, so it
carries a one-line summary of the three causes below.

A reading of `0` has three causes. They do not behave alike, and only the first
two are about the probe task at all.

**Cause 1: no probe task in this process.** `store_probe::spawn` was not called
here, so nothing stamped the gauge. `time() - 0` is roughly the current Unix
time, far over the `132` threshold, so `RavelStoreProbeStalled` goes true
immediately and holds past `for: 5m`. That is the alert firing correctly: a
process exporting this family with no probe behind it has no reachability
signal at all, and the single staleness comparison covers it with no second
term.

**Cause 2: the startup window.** `/metrics` begins serving as soon as the HTTP
task is spawned, and `store_probe::spawn` runs later in the same startup path,
after the listener binds this mode makes (gRPC, and Flight and mTLS when
configured) and after the blocking initial JWKS fetch. A scrape that lands in between reads `0` from a process that is starting
normally. The window ends the moment `store_probe::spawn` stamps the gauge.
Its length is the startup path between the two: the listener binds, which are
local, and, when OIDC refresh is configured, the initial JWKS fetch, the only
step in that path that waits on the network, capped at 10 s by its client
timeout. A fetch that fails refuses the start instead of leaving the gauge at
`0`. That is well inside `for: 5m`, so it does not page.

**Cause 3: a pre-1970 host clock.** `stamp_last_run` stores `clock.now_ns()`
unconditionally, and `SystemClock::now_ns` returns `0` through its
`unwrap_or(0)` when `SystemTime::now().duration_since(UNIX_EPOCH)` fails, which
is what a host clock set earlier than the Unix epoch produces. A probe task
that is alive and completing cycles then re-stamps `0` on every interval. This
one does not clear on its own and it does page, until the host clock is
corrected. It is a true positive about the host and a false one about the
probe: the alert's description points at a dead task, and under this cause the
task is running normally. Check the host clock before going to look for a probe
that is not missing.
<!-- claim:store-probe-zero:canonical-end -->

Causes 1 and 2 are why the rule is a single bare staleness comparison:

- A probe that stops completing cycles (the task died, panicked, or its
  channel was dropped) leaves an ageing timestamp. The timestamp crosses
  `132` and holds past `for: 5m`.
- A task that stopped before its first cycle completed ages out the same way.
  The probe stamps the gauge at spawn, before the first (jittered) sleep of
  the task.
- The spawn stamp ends cause 2 and starts a separate window. The stamp and
  the first completed cycle are at most `interval * 1.1 + 200s = 233s` apart,
  the same worst-case gap as for any two live completions. On a healthy
  start the comparison can therefore be true for at most `233 - 132 = 101s`
  after the stamp, until that first cycle re-stamps the gauge. In that window
  the gauge ages and does not read `0`.

The store-probe family renders unconditionally. A metrics-only monitoring
setup with nothing reading `/readyz` therefore still sees a store outage or a
dead probe task.

#### The store-probe liveness alert

The rule ships in the `ravel-storage-and-auth` group.

```yaml
groups:
  - name: ravel-storage-and-auth
    rules:
      - alert: RavelStoreProbeStalled
        # ravel_store_reachable and ravel_store_probe_failures_total are both
        # written only while the probe task is alive (issue #1728): if
        # tokio::spawn's task dies, they freeze and RavelStoreUnreachable
        # never fires. This gauge's age is the only signal that catches a
        # dead task; a merely failing-but-alive probe already trips
        # RavelStoreUnreachable well inside this window, so this rule is the
        # complement, not a duplicate.
        #
        # One rule covers every dead-probe state: store_probe::spawn stamps
        # the gauge synchronously before the loop's first sleep, so a task
        # that dies, panics, or is never reached by its first cycle leaves an
        # AGEING timestamp that crosses this threshold like any other
        # stoppage, and time() - 0 is roughly the current Unix time, so a
        # zero reading fires here too. What a 0 reading means is documented
        # in one place: the "What 0 means" section of
        # docs/guides/observability.md.
        #
        # 132 = K * interval * 1.1 at the defaults (K = 4, interval = 30s,
        # fold::jittered adds up to 10%). Both terms scale with
        # --store-probe-interval, which is an unbounded flag: recompute this
        # threshold before running a fleet on a non-default interval.
        expr: |
          time() - ravel_store_probe_last_run_timestamp_seconds > 132
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: >-
            A Ravel process's background store probe has not completed a
            cycle in over seven minutes
          description: >-
            The probe task itself has likely died: tokio::spawn has no
            restart path and nothing observes its JoinHandle. Check the
            process logs for a panic in the probe task; a store outage alone
            does not hold this condition true long enough to fire, since
            RavelStoreUnreachable already covers a probe that is failing but
            still running.
```

### Graceful shutdown (`ravel_shutdown_drain_overrun_total`)

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_shutdown_drain_overrun_total` | Graceful shutdowns this process ran past `--shutdown-timeout`, monotonic. The same overrun also logs an ERROR, `graceful shutdown did not complete cleanly`. See [Reachability during shutdown](#reachability-during-shutdown) below for when a scrape can see this change. |

#### Reachability during shutdown

A scrape that lands before shutdown begins, or one already in flight when the
drain starts, reads both shutdown counters correctly. A scrape that opens its
connection later cannot see either counter change during that shutdown:

- The drain stops the client-facing HTTP listener from accepting new
  connections at its very start, so that Kubernetes drains traffic before
  anything else closes.
- `ravel_ingest_flush_all_residue_tenants_total` can change only later in the
  same drain, at the ingest flush.
- `ravel_shutdown_drain_overrun_total` is known only when the whole drain, or
  its `--shutdown-timeout` bound, finishes.

The ERROR log lines are the only channel that is guaranteed to carry a single
event to an operator. `ravel-ingest` logs one line per residual shard, each
naming the tenant count of that shard. The server logs one line on an
overrun.

### Bucket protection (`ravel_bucket_protection_*`)

Labels: `mode`. Every process renders the family. The
`--require-bucket-protection` startup check sets it once. All three read `0`
when the flag is off.

| Metric | Meaning |
|---|---|
| `ravel_bucket_protection_conditions_failed` | Gauge. Bucket-protection conditions the startup check observed failed. A failure outside the refusing set (see [Deployment](operations/deployment.md#bucket-protection-at-startup)) starts the process with a warning and counts here. |
| `ravel_bucket_protection_conditions_unknown` | Gauge. Bucket-protection conditions the startup check could not determine: no API for the call, an access denial, a response it could not parse, or a bucket-configuration read that did not finish within 10 seconds. |
| `ravel_bucket_protection_unknown` | Gauge. `1` whenever `ravel_bucket_protection_conditions_unknown` is nonzero, else `0`. |

The startup check counts the seven conditions it evaluates: `versioning`,
`noncurrent-expiration`, `expired-delete-marker`, `abort-multipart`,
`rule-scope`, `no-foreign-rule` and `object-lock`. It never asks for
`delete-marker-replication` or `object-retention`, which `ravel-cli store
verify-protection` checks, so neither ever counts here.

On `--store s3` the check reads the bucket's configuration with three
read-only GETs, counted in the control-plane counters of the
[object store family](#object-store-ravel_store_). All seven conditions count
as unknown, and `ravel_bucket_protection_unknown` reads `1`, in three cases:

- On every other backend, where the check cannot read the configuration.
- On S3, when the bucket-configuration read has not finished within 10
  seconds.
- On S3, when the process's identity lacks the three read permissions, which
  no shipped IAM template grants (see
  [Deployment](operations/deployment.md#bucket-protection-at-startup)).

Two shipped rules cover the family:

- `RavelBucketProtectionUnknown` alerts on
  `ravel_bucket_protection_unknown == 1`.
- `RavelBucketProtectionConditionsFailed` alerts on
  `ravel_bucket_protection_conditions_failed > 0`, a process that started
  with a failed condition outside the refusing set.

How to read a zero on `ravel_bucket_protection_conditions_failed`:

- It is evidence that the bucket passes the seven conditions only when
  `ravel_bucket_protection_conditions_unknown` is also zero. A rule or
  dashboard that reads `conditions_failed == 0` as healthy must also require
  `conditions_unknown == 0`.
- It says nothing about `delete-marker-replication` or the `NoncurrentDays`
  value, which `ravel-cli store verify-protection` checks, or about
  `object-retention`, which no Ravel command checks yet.
- The values are those of the last startup. They do not move when the
  bucket's configuration changes under a running process.

### Durable auth refresh (`ravel_durable_auth_*`)

Labels: `mode`. The family renders only when the process built a durable
`sys/auth` resolver: `--tenant-hash-key-file` set on `ravel-server` in a
request-serving mode (`all`, `gateway`, `query`). A `maintain` mode process,
or one started without a deployment key, omits the whole family. All three
counters come from the background refresh loop that keeps the cached token
map current.

The file that `--tenant-hash-key-file` names is the deployment key. The
`ravel-cli tenant-token` subcommands take the same key through
`--deployment-key-file`, a separate flag on a separate binary.

| Metric | Meaning |
|---|---|
| `ravel_durable_auth_refresh_failures_total` | Background refreshes that failed to read or decode `sys/auth`. The staleness gate is not advanced on a failure, so a sustained failure eventually fails auth closed. |
| `ravel_durable_auth_on_miss_rereads_total` | Off-horizon on-miss re-reads of `sys/auth` begun after the rate limiter, when the request path saw an unknown token. |
| `ravel_durable_auth_stale_fail_closed_total` | Bearer-token resolutions refused because the cached map was hard-stale (fail-closed). |

`ravel_durable_auth_refresh_failures_total` gives early warning of a
credential break. It climbs as soon as the loop cannot read `sys/auth`, long
before the hard-stale horizon starts to refuse tokens. The operations guide
gives its alert rule.

### Maintenance discovery

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_maintain_tenants_discovered` | Gauge. Tenant prefixes storage reported under `t/` on the last successful discovery cycle. |
| `ravel_maintain_tenants_maintained` | Gauge. Discovered tenants maintained this cycle, after any flag restriction. |
| `ravel_maintain_tenant_discovery_failures_total` | Maintenance cycles skipped because tenant discovery itself failed. |

### Maintenance safety

Labels: `mode` and `signal`, with seven exceptions. No series carries a
`tenant_hash` label.

| Series | Labels |
|---|---|
| `ravel_maintain_legal_hold_refresh_failures_total` | `mode` only. |
| `ravel_maintain_retention_held_out_of_window_objects_total` | `mode` only. |
| `ravel_maintain_retention_held_by_lease_buckets_total` | `mode` only. |
| `ravel_maintain_objects_deleted_total` | `mode` and `kind`, no `signal`. |
| `ravel_maintain_superseded_inputs_held_total` | `mode`, `signal`, and `reason`. |
| `ravel_maintain_compaction_inputs_skipped_total` | `mode`, `signal`, and `reason`. |
| `ravel_maintain_erasure_unwritable_objects_total` | `mode`, `signal`, and `reason`. |

| Metric | Meaning |
|---|---|
| `ravel_maintain_legal_hold_refresh_failures_total` | Legal-hold refresh failures. Each one skips that tenant's whole maintenance tick. |
| `ravel_maintain_retention_held_out_of_window_objects_total` | Data objects the retention sweep declined to delete because their format version is outside this build's reader window, counted once per object per pass and summed over every signal. The whole bucket keeps its tombstone and nothing in it is deleted that pass. A rising total means retention is holding data past its window: finish the upgrade, complete `maintain migrate`, or roll back. See [maintenance](operations/maintenance.md#the-format-version-hold). |
| `ravel_maintain_retention_held_by_lease_buckets_total` | Tombstoned buckets the retention sweep declined to touch because a lease or legal hold protects a key it would delete, counted once per bucket per pass and summed over every signal. It rises for as long as a hold stands, so a rise is not by itself a fault. See [maintenance](operations/maintenance.md#retention-under-a-hold). |
| `ravel_maintain_l0_records_pending` | Gauge. L0 commit records sitting below `min_compaction_inputs` in a sealed bucket, by signal, summed over every tenant and shard this process maintains. |
| `ravel_maintain_objects_deleted_total` | Objects the sweep physically deleted, by `kind`: `superseded_records_deleted`, `superseded_data_deleted`, `unreferenced_parts_deleted`, `quarantine_reaped`. |
| `ravel_maintain_bytes_reclaimed_total` | Bytes of deleted objects reclaimed by the sweep, by signal. Object sizes, not wire bytes: the quarantine reaper and the unreferenced-part delete at their listed size, and the superseded-input sweep at the `object_size` the commit, compaction or rewrite record naming each object carries, a superseded L1 segment on the pass that deletes that record, so it is charged once. Retention deletions are excluded because they delete by key without a known size, so it undercounts the bytes reclaimed, except that two replicas sweeping one unit during an ownership handoff can each count the same object. Per process. |
| `ravel_maintain_retention_lag_seconds` | Gauge. How far past its retention expiry the oldest still-present expired bucket is, by signal, from this process's most recent completed cycle. 0 when none. A per-cycle maximum over the process's units, so it names the single worst bucket. Three cases. Exact when this process tombstoned the bucket. Otherwise measured from the earlier of the ingest hour's end plus the window and the tombstone time, which can under-read by up to one hour plus `max_ingest_lag` (three hours at the defaults) and over-read by at most the allowed future clock skew, except transiently after a physical sweep that stopped partway (see below). And, for a bucket holding a rewrite record with no parts, from the tombstone time alone, which never over-reads and under-reads by however long after the bucket expired its tombstone was written, with no fixed bound. A unit whose scan failed, or that a failed provisioning read skipped, contributes nothing: read it beside `ravel_maintain_units_scan_failed`. |
| `ravel_maintain_units_scan_failed` | Gauge. Units whose retention and compaction scan returned an error in this process's most recent completed cycle, or that a failed provisioning check or shard-generation read skipped unscanned, by signal. While it is nonzero the retention lag does not cover every unit. |
| `ravel_maintain_conservation_aborts_total` | Compaction publishes aborted by the record-count conservation gate, by signal. |
| `ravel_maintain_compaction_inputs_skipped_total` | Input objects compaction left out of its merge instead of failing on them, by signal and `reason`, each object counted once per process. Carries `signal="logs"` and `signal="audit"` (the query-audit shard), the two signals that compact through RLOG, each rendered from zero. `reason="unwritable_stream_attrs"`: a log object carrying a `stream_attrs` blob the RLOG writer refuses, written before the writer checked it. The rest of the bucket is merged when at least `min_compaction_inputs` inputs remain; the skipped object stays in storage, named by no compaction record, and a `WARN` line names its key. The counter resets on restart, so alert on an increase, not on a level. See [maintenance](operations/maintenance.md#a-log-object-compaction-cannot-rewrite) for what to do. |
| `ravel_maintain_erasure_unwritable_objects_total` | Input objects that blocked the selective-erasure rewrite of their bucket, by signal and `reason`, each object counted once per process however many passes it blocks. Carries `signal="logs"` only, rendered from zero. `reason="unwritable_stream_attrs"`: a log object carrying a `stream_attrs` blob the RLOG writer refuses, in a bucket with no compaction record. Nothing in the bucket is rewritten, so every live object in it, healthy ones included, stays live, and every erasure request whose window reaches any of them stays pending; a request whose window reaches no live object of a blocked bucket completes. A `WARN` line names the object's key. The counter resets on restart, so alert on an increase, not on a level. See [maintenance](operations/maintenance.md#a-log-object-an-erasure-rewrite-cannot-rewrite) for what to do. |
| `ravel_maintain_orphan_breaker_tripped_total` | Orphan-GC mass-orphan circuit breaker trips, by signal. Also carries `signal="alerts"` and `signal="audit"` for the alerts shard's orphan sweep and the query-audit shard's input-cleanup sweep, which run outside the maintained signals. The superseded refusal counter and the two superseded hold families below carry those two signals as well, `ravel_maintain_compaction_inputs_skipped_total` carries only logs and audit, and `ravel_maintain_erasure_unwritable_objects_total` carries only logs; every other per-signal series here covers only metrics, logs and spans. |
| `ravel_maintain_orphans_withheld` | Gauge. Orphan candidates withheld by the last completed orphan pass, by signal. |
| `ravel_maintain_orphans_present` | Gauge. Orphan candidates the last completed orphan pass found, by signal, whether or not the breaker tripped. |
| `ravel_maintain_orphans_quarantined_total` | Orphan candidates moved from the live L0 set to the quarantine prefix, by signal. |
| `ravel_maintain_orphans_quarantine_refused_total` | Orphan candidates whose copy to the quarantine prefix failed, by signal. The live object was left in place rather than deleted without a copy. |
| `ravel_maintain_superseded_deletes_refused_total` | Superseded-input deletes the store refused (access denied, a failed precondition, or a permanent error), by signal. The refusing supersession chain keeps its remaining keys for a later pass and the pass still succeeds. Also carries `signal="alerts"` and `signal="audit"`. The alerts sample reads zero today, because nothing compacts or rewrites the alerts shard, so it has no supersession chain. |
| `ravel_maintain_superseded_inputs_held_total` | Superseded objects the superseded-input sweep held instead of deleting, by signal and `reason`, counted once per pass that holds them. Objects a legal hold protects are not counted here. `ravel_maintain_superseded_groups_held_by_legal_hold_total` counts them, in chain groups. `reason="named"`: the live catalog HEAD snapshot still names the object. `reason="unreadable_head"`: HEAD or a covering snapshot part is present and cannot be read, or the object's unnamed-since marker cannot be read, written or deleted. `reason="pinned_window"`: HEAD no longer names the object, but its unnamed-since marker is younger than `max_query_duration + head_cache_ttl + 4 * clock_skew_allowance`. Also carries `signal="alerts"`, which reads zero today for the same reason, and `signal="audit"`. |
| `ravel_maintain_superseded_groups_held_by_legal_hold_total` | Supersession chain groups the superseded-input sweep skipped whole because a legal hold protects a key in them, by signal, counted once per pass that skips them. Also carries `signal="alerts"`, which reads zero today for the same reason, and `signal="audit"`. |
| `ravel_maintain_dreq_held_by_superseded_inputs_total` | Erasure requests (`.dreq`) the erasure-request sweep kept past their protection horizon, by signal, counted once per tick that keeps them. The sweep decides from its own observing pass of the superseded-input sweep, which deletes nothing and covers every hour and every chain whatever its age. It keeps a `.dreq` when that pass held a chain group naming the request, or when it held a chain it could not walk to the end anywhere in the signal, which keeps every `.dreq` past its horizon. |
| `ravel_maintain_quarantine_reaped_total` | Objects physically deleted from the quarantine prefix past the quarantine horizon, by signal. |

[Troubleshooting](operations/troubleshooting.md) gives the alert rules and the
breaker runbook.

#### Orphans and quarantine

A zero on `ravel_maintain_orphans_withheld` or
`ravel_maintain_orphans_present` does not mean that a prior trip was
resolved. Each gauge is the count of the last completed orphan pass. Only a
sweep that ran the orphan rule refreshes them. That sweep runs on the
full-sweep cadence (`interior_reverify_ns`, default 6 h), not on the maintain
tick (default 300 s). The ticks in between leave both gauges at the values of
the last completed pass.

The three quarantine series are counters. Each counts what a sweep pass did,
and a later quiet pass does not undo it. Read them together:

- `ravel_maintain_orphans_quarantined_total` climbs while
  `ravel_maintain_quarantine_reaped_total` stays flat: the quarantine prefix
  fills and nothing reclaims it.
- `ravel_maintain_orphans_quarantine_refused_total` increases: quarantine
  cannot make progress, from a store fault or a permissions or capacity
  problem on that prefix. The candidates that it counts are still live. The
  copy is taken before the delete, so a refused copy leaves the object in
  place. Alert on `increase(...) > 0`, the same shape as the breaker-trip
  counter, because the next pass retries the same candidate and is refused
  again.

#### Superseded-input holds

`ravel_maintain_superseded_deletes_refused_total` shows a deny policy on part
of the keyspace. A refused delete stops only the supersession chain that it
belongs to. A pass with at least one successful delete still succeeds, so the
unit's tick is not recorded as failed. Alert on `increase(...[6h]) > 0`, not
over a shorter window. The next pass over that hour retries the chain and is
refused again, but for an interior hour that next pass is the next full
sweep.

The three hold counters count what the sweep kept. Each pass that holds an
object counts it again, so read their growth over a window, not their total.
Use `increase(...[6h])`, or a window at least as long as
`interior_reverify_ns` where that is set longer. Do not read them as a rate.
The standalone dashboard plots them that way.

Which passes count a hold depends on the hour:

| Hour | Swept by | When a hold counts |
|---|---|---|
| Head hour | The zoned sweep, which most maintain ticks run. It lists only head and tail hours. | Never on the default horizons: the chains are younger than the protection horizon, and the sweep skips those before the hold gate. On every tick in a deployment whose head zone (`max_flush_lifetime` plus clock skew plus one hour) outlasts its protection horizon. |
| Tail hour | The zoned sweep. A tail hour exists only under a retention policy, from the hour's retention expiry through the protection horizon past it. | On every tick. |
| Interior hour, where the holds of a lagging fold sit | Only the full sweep (`interior_reverify_ns`, 6 hours by default). | Once per full sweep, so a rate spikes every 6 hours and reads zero between. |
| The query-audit shard | Every maintain tick of the process that owns it, whole. | On every tick. |

What growth on each hold series means:

| Series | Reading |
|---|---|
| `ravel_maintain_superseded_inputs_held_total{reason="named"}` | The ordinary lagging-fold case. A snapshot part that the fold has not reconciled still names superseded inputs. The sweep collects them after the fold reconciles that hour or HEAD is rebuilt. Act only when the series keeps growing across many folds: the hour then lies outside the fold's reconcile window and HEAD needs a rebuild. |
| `ravel_maintain_superseded_inputs_held_total{reason="unreadable_head"}` | Any sustained growth needs an operator. HEAD or a snapshot part is present and cannot be read. The sweep holds every superseded input that it gates, fail-closed, until the catalog object is repaired or HEAD is rebuilt. |
| `ravel_maintain_superseded_inputs_held_total{reason="pinned_window"}` | Expected on every superseded object for at least one pass. The first pass that finds an object past its horizon and unnamed writes its unnamed-since marker and holds it. The object goes on the first pass at least `max_query_duration + head_cache_ttl + 4 * clock_skew_allowance` (1 h 20 min 30 s with defaults) later. Growth that never drains means that the marker cannot age. Check the sweeper's clock and the window terms: `max_query_duration` and `head_cache_ttl` in `sys/gc`, and the sweeper's own `clock_skew_allowance`. |
| `ravel_maintain_superseded_groups_held_by_legal_hold_total` | Expected while a legal hold covers the shard. It stops growing when the hold is lifted. Growth with no hold in force points at a hold that nobody meant to keep. |

#### Held erasure requests

`ravel_maintain_dreq_held_by_superseded_inputs_total` counts erasure requests
whose query-time exclusion filter stays in force past their horizon. The
subject stays hidden from queries meanwhile. The `.dreq`, which carries the
subject identifier, outlives its horizon.

This counter does not trace back to the three hold counters or the refusal
counter. The erasure-request sweep decides from its own observing pass of the
superseded-input sweep. That pass runs on every tick while a `.dreq` is past
its horizon. The three hold counters come from deleting passes. A `.dreq` can
therefore be held while all three stay flat, and a refused delete never holds
one.

Start from the WARN line of the sweep,
`erasure-request sweep: holding a .dreq past its horizon`. The line names the
request and carries how many requests and truncated buckets the observing
pass held.

#### Pending L0 records

`ravel_maintain_l0_records_pending` is a per-process total. One maintenance
cycle (default 300 s) sums every sealed bucket of every `(tenant, shard)`
that this process owns, and publishes the result when the cycle has covered
all of them. A scrape that lands mid-cycle reads the complete total of the
previous cycle, never a partial sum.

For a deployment-wide figure, sum the gauge across processes. With several
maintain replicas, each owns a disjoint share of the units.

A dip can mean that pending work fell, or that a unit was not reached. A unit
whose pass failed contributes nothing for that cycle.
`ravel_maintain_units_stalled` alone does not separate the two:

- It moves only for a per-unit failure that has repeated past the stall
  threshold (three consecutive ticks on the defaults). A per-unit failure of
  one or two cycles dips this gauge with `ravel_maintain_units_stalled` still
  at zero.
- A tenant whose legal-hold refresh fails is skipped for the entire tick.
  `ravel_maintain_legal_hold_refresh_failures_total` moves and
  `ravel_maintain_units_stalled` does not.
- A tenant skipped by the provisioning or shard-generation check is also
  dropped before any per-unit accounting
  (`ravel_provisioning_shard_count_mismatch_total`).

Before you conclude that compaction caught up, read a dip against those
counters and against the age of
`ravel_maintain_last_cycle_completed_timestamp_seconds`.

This gauge does not follow the full-sweep cadence of the orphan gauges. A
below-threshold bucket in the interior zone is skipped on the ticks between
re-verifies (`interior_reverify_ns`, default 6 h). The memo carries the
last-known L0 record count of the bucket, and the skipped bucket still
contributes that count. Every cycle therefore publishes the whole pending
population. The count from a skipped bucket is as old as its last re-verify.
A bucket that crossed the threshold since then shows only after its re-verify
or its compaction runs.

#### Bytes reclaimed

`ravel_maintain_bytes_reclaimed_total` counts the bytes that the sweep
physically freed. It counts object sizes, not wire bytes, from the three
deletions whose size the sweep knows without an extra request:

| Deletion | Size charged |
|---|---|
| Quarantine reaper, unreferenced-part delete | The size that their own listing returned. |
| Superseded L0 data object | The `object_size` that its commit record carries. |
| Superseded L1 segment | The `object_size` that its compaction or rewrite record carries, charged on the pass that deletes the record naming the segment. |

The superseded-input sweep frees most of the bytes in steady state. A pass
whose delete of a record is refused leaves the next pass to delete the same
L1 segments again, and the delete of a missing key succeeds. Only the delete
of the record charges them, so each is charged once.

The counter has three limits:

- It undercounts all bytes reclaimed, because it excludes retention
  deletions. Those delete by object key without a known size.
  `ravel_maintain_objects_deleted_total{kind=...}` counts objects on the same
  sweep paths and leaves retention deletions out too. No metric counts
  retention deletions. `ravel_maintain_retention_lag_seconds` shows the
  progress of retention.
- It can count one object twice when two replicas sweep a unit during an
  ownership handoff, because the delete of a missing key succeeds.
- It is a per-process counter. Sum it across maintain replicas for a
  deployment-wide figure.

#### Retention lag

`ravel_maintain_retention_lag_seconds` reports how far the physical sweep of
retention has fallen behind. For the oldest bucket that is expired and still
physically present, it is how far the clock is past the expiry of that
bucket. It is `0` when no expired bucket is still present.

- It is a gauge and a per-cycle maximum over the units that this process
  owns, so it names the single worst bucket. A scrape mid-cycle reads the
  value of the previous completed cycle.
- A healthy sweep holds it near one protection horizon. A value that climbs
  steadily means that expired data is not deleted fast enough. The causes are
  a HEAD-reachability block from a lagging fold, an out-of-window format
  version, or a legal hold on the bucket.
- A bucket kept on purpose by a legal hold or a format version hold counts as
  lag for as long as the hold stands.
- With several maintain replicas, take the maximum across them, not the sum.
  Each owns a disjoint share of the units, and the lag is a worst-case figure.

How exact the lag is depends on where the expiry comes from:

| Case | Expiry used | Error |
|---|---|---|
| This process tombstoned the bucket. | The bucket's newest event plus the retention window, read from the records that decided the expiry and kept in memory for the bucket's later passes. | Exact. |
| After a restart, or a bucket that another replica tombstoned. The tombstone records no event time. | The earlier of the ingest hour's end plus the window and the tombstone's write time. | Under-reads by up to one hour plus `max_ingest_lag`, three hours at the defaults, because admission accepts an event up to `max_ingest_lag` before its ingest hour. Over-reads by up to the allowed future clock skew, for an event that runs past its ingest hour by that skew. |
| A tombstoned bucket that holds a rewrite record with no parts. | The tombstone's write time alone. | Never over-reads. Under-reads by however long after its expiry the tombstone was written, which has no fixed bound. |

Notes on those cases:

- An erasure that dropped every record in a bucket leaves a rewrite whose
  publish time stands in for the bucket's newest event. The expiry of that
  bucket can sit anywhere up to the tombstone, and the ingest hour's end
  bounds nothing. That is why the third case uses the tombstone alone.
- A rewrite that keeps parts carries their event times. Its bucket keeps the
  second case and its bound.
- To tell a parts-less rewrite apart costs one GET per listed rewrite record,
  up to the first one with no parts. A bucket whose rewrites all keep parts
  costs one GET for each. The process issues the GETs only when it does not
  hold the exact expiry, and only until one pass reads them without error. It
  keeps that answer in memory for the bucket's later passes. A read that
  fails falls back to the tombstone's write time alone and never fails the
  retention pass.
- One state over-reads by more than the second case allows, transiently. A
  physical sweep deletes a bucket's rewrite records before its data objects,
  L1 segments and tombstone. After a sweep that stopped partway, a process
  that holds neither the exact expiry nor an earlier read of those rewrites
  lists none of them, or only some that keep parts. It then uses the second
  case and can over-read by the time between the hour's end and the publish
  time of a deleted parts-less rewrite. The next pass finishes the sweep and
  the bucket stops contributing.

A unit whose retention and compaction scan fails contributes no lag, so a
signal whose sweep is stuck on a failing listing can read `0`.
`ravel_maintain_units_scan_failed` covers that gap. A lag of `0` means that
nothing is behind only while `ravel_maintain_units_scan_failed` is also `0`.

- It counts a failed unit once per cycle, however many ticks the unit has
  failed. `ravel_maintain_units_stalled` shows that the same unit has failed
  several ticks in a row.
- The last measured lag of a failed unit is not carried forward. A unit whose
  scan has failed on every tick since the process started, or since the
  process took the unit over, has no last lag to carry.
- Units that a tick skips before the scan, because their provisioning check
  or shard-generation read failed, count here too. They count once per cycle
  for each unit of that tenant and signal that this process owns below the
  configured `shard_count`. Only the read that failed can name a wider
  shard.
- A store error on either of those reads increments no other counter. Only a
  hard provisioning failure also moves
  `ravel_provisioning_shard_count_mismatch_total`.
- A failed legal-hold refresh, which skips the tenant's whole tick, is not
  counted here. `ravel_maintain_legal_hold_refresh_failures_total` covers it.

### Alert retention skips (`ravel_alert_retention_skipped_total`)

Labels: `mode` and `reason`. The counter is summed over every tenant that this
process maintains, with no `tenant_hash` dimension. It shows that some tenant
is in one of these states, never which one. The maintenance tick records it,
so it renders with the maintenance-safety families.

The maintenance tick sweeps alert transitions older than `--alert-retention`.
It first reads the tenant's alert state memo to learn which record is the
current state of each alert identity. Without a memo that it can trust, it
does not sweep that tenant on that tick, and counts the reason here. Under
`--alert-retention 0` it reads no memo and counts nothing here. The orphan
sweep of the alerts shard still runs.

| `reason` | Meaning |
|---|---|
| `absent` | The tenant has alert records but no memo. A tenant that has never written an alert transition has no memo either and is not counted: it has nothing to sweep. |
| `undecodable` | The memo object does not decode. |
| `unsupported_version` | The memo decodes but carries a format version this build does not read. |
| `watermark_below_floor` | The memo's watermark hour, clamped to the maintaining process's own clock hour, is below the hour of the window's expiry floor: the memo is complete only up to an hour older than that floor, so it does not name every identity's current state across the range the sweep would delete from. |
| `store_error` | Reading the memo, or the one listing that tells an unused alert keyspace from a lost one, failed against object storage. |

Each count is a skip for one tenant on one tick, and the next tick retries.
On a deployment that runs alert rules, a rate that stays at zero is the
healthy state. The evaluator rewrites each tenant's memo on every tick that
it runs, so a memo is at most one tick old.

| Sustained rate on | Points at | Read next |
|---|---|---|
| `absent`, `undecodable`, `unsupported_version` or `watermark_below_floor` | An evaluator that does not run or cannot write its memo. The remedy is at the evaluator, not at the sweep. | `ravel_alert_ticks_total{outcome=...}` and the age of `ravel_alert_last_tick_completed_timestamp_seconds`. |
| `store_error` | Object storage. | `ravel_store_probe_failures_total`. |

In both cases the alert history is not trimmed while the rate lasts. That
shows later as a growing prefix and a slower cold-start fold, not as a failed
query.

A steady `watermark_below_floor` rate immediately after a configuration
change can mean a window that is too short for the evaluation interval of the
deployment, and not a broken evaluator. The server refuses the worst case at
startup. A window only a little above the floor leaves little room for an
evaluator that misses ticks.

### Maintenance ownership and concurrency

Labels: `mode`. Every series is process-wide, with no `tenant_hash`
dimension.

| Metric | Meaning |
|---|---|
| `ravel_maintain_workers_live` | Gauge. In-process maintenance workers this supervisor currently sees as live. |
| `ravel_maintain_units_owned` | Gauge. Owned (tenant, signal, shard) units this process is currently maintaining. |
| `ravel_maintain_units_stalled` | Gauge. Owned units with consecutive failing ticks past the configured threshold. A pair's shard 0 unit also fails a tick when this process's sweep of another shard of the pair fails, since the owner of shard 0 sweeps every shard of the pair. Alert on a sustained nonzero value, not on any single scrape. |
| `ravel_maintain_memo_warm_start_units_total` | Units seeded from a durable memo snapshot on handoff or startup, instead of rescanning cold. |
| `ravel_maintain_full_sweep_passes_total` | Full (unscoped) sweep passes run, as opposed to a zone-scoped sweep. |

### Maintenance loop liveness

Labels: `mode`. Both series are process-wide, with no `tenant_hash` dimension.

| Metric | Meaning |
|---|---|
| `ravel_maintain_last_cycle_completed_timestamp_seconds` | Gauge. Unix time the maintenance loop last completed a cycle in this process, `0` if none has completed since it started. Its age is the maintain-liveness signal. |
| `ravel_maintain_loop_panics_total` | Panics caught in the loop body and restarted by the supervisor. An `increase()` here is the loop crash-looping. |

Read the age of the gauge. Only its age moves when the loop stops, the same
as the [catalog fold liveness
gauge](#catalog-fold-liveness-ravel_catalog_fold_).

Every other maintenance figure is written at the end of a cycle that
completed. Examples are `ravel_maintain_tenants_maintained`,
`ravel_maintain_units_stalled`, and the safety gauges. If the single spawned
supervisor task dies (a panic anywhere in the discovery or sweep call graph),
they all freeze at their last healthy values:

- `ravel_maintain_tenants_maintained` still equals
  `ravel_maintain_tenants_discovered`.
- `ravel_maintain_units_stalled` still reads `0`.
- The pod stays Running and Ready.

Retention deletes nothing, compaction stops, and the sweeper reclaims
nothing. No other series on `/metrics` moves until a recent-window query is
refused days later.

The loop runs under a supervisor. The supervisor catches a panicking cycle,
counts it on `ravel_maintain_loop_panics_total`, and restarts the loop after
a bounded backoff. After a single transient panic, the next completed cycle
re-stamps the gauge. A rising panic counter with a stalling gauge is a loop
that cannot make progress between crashes.

The panic counter has its own rule, because a crash loop does not always
stall the gauge. An attempt that completes a cycle and then panics re-stamps
the gauge on that cycle. The supervisor resets the backoff to its initial
value whenever the dead attempt completed at least one cycle. A loop that
crashes on every attempt after one cycle therefore keeps the gauge fresh. It
also keeps the full-sweep counter moving when the panic lands after the
sweep. `RavelMaintenanceLoopStalled` and `RavelMaintenanceNoFullSweeps` then
stay silent, and the panic counter is the only signal that moves.

#### The maintenance-stalled alert

The rules ship as the `ravel-maintain-liveness` group.

```yaml
groups:
  - name: ravel-maintain-liveness
    # These rules assume at least one maintain-mode process is scraped. A
    # deployment that runs no maintain mode at all (retention, compaction, and
    # GC disabled by design) has no maintenance loop to be alive, and must drop
    # or inhibit this group; the family is absent there and the absent() branch
    # would otherwise fire permanently.
    rules:
      - alert: RavelMaintenanceLoopStalled
        # No max()/min() aggregation: unlike the fold, the maintenance loop is
        # NOT covered by peers. Ownership of units is partitioned across
        # replicas (ADR-0065), so a single dead loop strands its own units
        # while healthy peers keep their gauges fresh. The rule must fire on
        # ANY instance going stale, so it is left per-series. The operator
        # default is one maintain replica anyway.
        #
        # The gauge reads 0 from process start until the first cycle
        # completes, so this expression is true on a fresh process and the
        # `for:` below is what suppresses it until the first cycle lands.
        # Keep `for:` comfortably above the configured maintenance interval:
        # a deployment that raises the interval past 10m, or that has a slow
        # cold first scan, pages on every restart otherwise.
        expr: |
          (
            time() - ravel_maintain_last_cycle_completed_timestamp_seconds > 1800
          )
          or
          absent(ravel_maintain_last_cycle_completed_timestamp_seconds)
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: >-
            A Ravel maintenance loop has not completed a cycle for longer than
            several maintain intervals
          description: >-
            Retention, compaction, and GC have stopped for the units this
            process owns, and the pod still reads Running and Ready. Check
            ravel_maintain_loop_panics_total for a crash-looping loop and the
            maintain process logs for the panic. The threshold is set well
            above the default 5m maintain interval so a slow cycle does not
            page; a stall this long is the loop being down, not busy.
      - alert: RavelMaintenanceNoFullSweeps
        # A healthy loop runs an unscoped full sweep on each owned unit's
        # interior re-verify cadence (default 1h) and on the cold first tick,
        # so this counter advances at least hourly on a process that owns at
        # least one unit. A window of several hours with zero increase
        # corroborates the stall gauge for the class of hang where the loop is
        # alive enough to scrape but is no longer sweeping.
        #
        # The counter advances per swept unit, so a process that owns none
        # never moves it while completing cycles normally: an empty cluster,
        # or a replica whose peers hold every unit under the ADR-0065 split.
        # Since ADR-1693's sweep-ownership amendment, only the owner of a
        # pair's shard 0 sweeps it, so a replica can own units for retention
        # and compaction yet run no sweep pass; a per-process check would then
        # false-fire on that replica forever. This rule therefore aggregates
        # over the cluster: it fires only when no process advanced the counter
        # while the cluster still owns units, the true "nobody is sweeping"
        # condition.
        expr: |
          sum(increase(ravel_maintain_full_sweep_passes_total[3h])) == 0
          and
          sum(ravel_maintain_units_owned) > 0
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: >-
            No Ravel maintenance full sweep has run in the last several hours
          description: >-
            The GC sweeper reclaims nothing while this holds. It precedes the
            operator-visible symptom (a refused recent-window query) and
            corroborates RavelMaintenanceLoopStalled.
      - alert: RavelMaintenanceLoopCrashLooping
        # The shape neither rule above catches. A supervised attempt that
        # completes a cycle and then panics re-stamps the liveness gauge on
        # that cycle, and the supervisor resets its backoff to the initial
        # value because the dead attempt completed at least one cycle. The
        # gauge stays fresh, the full-sweep counter keeps moving when the
        # panic lands after the sweep, and the panic counter is the only
        # signal that moves. A single transient panic self-heals by design,
        # so the threshold is a repeat rate rather than any panic at all.
        expr: |
          increase(ravel_maintain_loop_panics_total[1h]) > 3
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: >-
            The Ravel maintenance loop is panicking and restarting repeatedly
          description: >-
            The supervisor is catching a panic and restarting the loop faster
            than the loop is making progress. Read the maintain process logs
            for the panic itself. This fires while the liveness gauge is still
            fresh, so it is the only signal for a crash loop that completes a
            cycle between panics.
```

The staleness threshold is `1800s`: 30 minutes, or six default 5m maintain
intervals. It is lower than the fold rule's `4800s`, which is sized to the
unsealed-span budget. Here any lapse of a few cycles means that retention and
GC have stopped.

The `or absent(...)` branch covers the total-outage cases that the staleness
comparison cannot see, because `time() - <empty>` is empty. Those cases are a
maintain mode scaled to zero, one that crash-loops fast enough to go stale,
and one dropped from the scrape config.

### Merge memory (`ravel_maintain_rlog_merge_peak_bytes`)

Labels: `mode` and `kind`. The family carries no `tenant_hash`, because one
process-wide tracker covers the merges of every tenant.

| Metric | Meaning |
|---|---|
| `ravel_maintain_rlog_merge_peak_bytes` | Gauge. High-water mark of RLOG k-way merge memory, by kind. |

| `kind` | Meaning |
|---|---|
| `transient` | In-flight fetched-minus-released block bytes at any instant during a merge. |
| `total` | Transient plus the writer's buffered output bytes. |

Watch this gauge when a maintain process is under memory pressure during
compaction merges.

### Alert evaluation (`ravel_alert_*`)

Labels: `mode`, plus `outcome` on the tick counter. Every series is
process-wide, with no `tenant_hash` dimension. One process runs one evaluator
per tenant that has rules, and each figure is the sum across them.

The whole family is absent unless this process built at least one alert
evaluator: `--alert-rules-file` was given and the file named at least one
rule. A deployment with no alerting configured exports none of these series,
which keeps the alert rules below quiet there.

| Metric | Meaning |
|---|---|
| `ravel_alert_rules_evaluated_total` | Alert rules whose query ran and whose condition was decided. |
| `ravel_alert_rules_failed_total` | Alert rules skipped because the query, the condition, or the write failed. Every one is logged. The rule is retried next tick. |
| `ravel_alert_records_written_total` | Alert transition records durably written. |
| `ravel_alert_repeats_queued_total` | Repeat notifications queued for a still-firing alert. A repeat writes no new record, so it advances this and then the delivery counter, never `ravel_alert_records_written_total`. |
| `ravel_alert_notifications_delivered_total` | Notifications delivered to every configured sink, including ones carried over from an earlier tick's failure. |
| `ravel_alert_notifications_failed_total` | Notifications attempted but not accepted by every configured sink, counted once per tick per notification, so one stuck notification keeps advancing it while it is retried. |
| `ravel_alert_notifications_deferred_total` | Notifications not attempted in a tick because the per-tick delivery deadline (half the evaluation interval) elapsed first, counted once per notification per tick: a notification deferred again on the next tick is counted again. They keep their place at the front of the queue. A rising value has two causes: a sink too slow to drain the queue within a tick, or a tick whose work before delivery (history fold, rule evaluation, memo write) already ran past the deadline, in which case every notification after the first is deferred even when every sink answers at once. |
| `ravel_alert_undelivered_notifications` | Gauge. Notifications not yet accepted by every configured sink, summed over this process's evaluators, at most one per alert identity. While a sink keeps failing it grows by one for every identity that transitions, without bound. |
| `ravel_alert_ticks_total` | Evaluation ticks by `outcome`. |
| `ravel_alert_last_tick_completed_timestamp_seconds` | Gauge. Unix time the alert loop last completed a tick in this process, `0` if none has completed since it started. Its age is the alert-loop liveness signal. |

The `outcome` label carries one of four values, one per tick:

| `outcome` | Meaning |
|---|---|
| `evaluated` | This replica held the tenant's alert lease and evaluated every rule. |
| `lease_not_held` | A peer replica held the lease, so this one skipped evaluation. Healthy, and the steady state of every replica that is not the holder. |
| `lease_unavailable` | The lease read or write failed against object storage. Evaluation was skipped and is retried next tick. |
| `history_unavailable` | The tenant's alert history could not be read, so nothing was evaluated. The evaluator never acts on a partial history, because a partial history re-fires an alert that is already firing. |

Do not sum `lease_not_held` with the two failure outcomes. In a multi-replica
deployment every replica but one reports it on every tick, so a rule that
includes it pages on the expected steady state.

Read the age of the liveness gauge. Every counter is cumulative, so an
evaluator that dies leaves them all frozen. A frozen
`ravel_alert_rules_failed_total` looks the same as a healthy pipeline whose
rules never fail. Only the age of the gauge moves when the loop stops. A tick
that ended in `lease_not_held` stamps the gauge, because a standby replica is
alive.

#### The alerting-pipeline alerts

The rules ship as the `ravel-alerting-pipeline` group.

```yaml
groups:
  - name: ravel-alerting-pipeline
    # None of these rules carries an `absent()` branch, unlike the maintenance
    # group above, and that is deliberate. A deployment with no alert rules
    # configured builds no evaluator and therefore exports none of this family,
    # which is a legitimate steady state, not an outage. With no series to
    # match, every expression below is the empty vector and no rule fires. The
    # cost is that this group cannot tell "alerting was never configured" from
    # "the whole process is gone"; the latter belongs to a scrape-level `up`
    # rule, which covers every subsystem at once rather than this one.
    rules:
      - alert: RavelAlertLoopStalled
        # Per-series, no aggregation: a replica that is not the lease holder
        # still ticks and still stamps this gauge, so a healthy peer does not
        # cover a dead one and the rule must fire on any instance going stale.
        #
        # The gauge reads 0 from process start until the first tick completes,
        # so this expression is true on a fresh process and the `for:` below is
        # what suppresses it until that first tick lands. Keep `for:` well above
        # `--alert-eval-interval-secs` (default 60s).
        expr: |
          time() - ravel_alert_last_tick_completed_timestamp_seconds > 600
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: >-
            A Ravel alert evaluation loop has not completed a tick for ten
            evaluation intervals
          description: >-
            No rule is being evaluated and no transition is being written or
            notified on this process, while the pod still reads Running and
            Ready. The first operator-visible symptom would otherwise be an
            alert that never arrived. Check the process logs for a panic in an
            evaluator task. This gauge is process-wide liveness and not
            per-tenant: the evaluator runs one task per tenant and every task
            stamps the same gauge, so one tenant's dead evaluator stays hidden
            while any other tenant on the process keeps ticking. Per-tenant
            liveness cannot be a label on this route under ADR-0044, so a
            deployment that needs it runs one tenant per process or watches
            the alert output itself.
      - alert: RavelAlertNotificationsAllFailing
        # Delivery failure is retried every tick, so a genuinely broken sink
        # advances the failure counter continuously while the delivered counter
        # stays flat. The last term is what keeps a partial failure (one
        # notification stuck behind a bad URL while the rest get through) out of
        # this critical rule; it belongs to RavelAlertRuleEvaluationFailing's
        # quieter class.
        #
        # No term on ravel_alert_notifications_deferred_total: every pass
        # attempts its first notification before the deadline check applies,
        # and each attempt counts as delivered or failed in the same tick, so
        # a window with deferrals and no deliveries already has failures.
        #
        # Quiet on a healthy deployment with no rules configured: the family is
        # absent, so both terms are empty. Quiet on one whose rules simply never
        # fire: nothing is ever queued, so the failure counter never increases.
        expr: |
          increase(ravel_alert_notifications_failed_total[15m]) > 0
          and
          increase(ravel_alert_notifications_delivered_total[15m]) == 0
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: >-
            Every Ravel alert notification is failing to reach its sinks
          description: >-
            Transitions are still being written durably, so no alert history is
            lost, but nothing is reaching Alertmanager or the configured
            webhooks: the sinks are refusing every attempt. Check the sink
            URLs and credentials, and the evaluator logs for the per-sink
            delivery error.
      - alert: RavelAlertRuleEvaluationFailing
        # A rule whose query, condition, or write fails is retried next tick, so
        # a persistently broken rule (a PromQL expression that no longer parses
        # against the data, a SQL statement naming a dropped column) advances
        # this every tick and never self-heals.
        #
        # Quiet with no rules configured, for the same reason as above: no
        # series to match. Quiet on a deployment whose rules all evaluate
        # cleanly: the counter never moves.
        expr: |
          increase(ravel_alert_rules_failed_total[30m]) > 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: >-
            A Ravel alert rule has been failing to evaluate for half an hour
          description: >-
            The rule is skipped and retried every tick, so the condition it
            watches is unguarded for as long as this holds. The evaluator logs
            name the rule_id and the error.
      - alert: RavelAlertPipelineBlocked
        # The two store-failure outcomes, and ONLY those two. `lease_not_held`
        # is excluded on purpose: it is the steady state of every replica that
        # is not the lease holder, so including it would page on a normal
        # two-replica deployment forever.
        expr: |
          increase(ravel_alert_ticks_total{outcome=~"history_unavailable|lease_unavailable"}[30m]) > 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: >-
            Ravel alert evaluation is blocked on object storage
          description: >-
            The evaluator can neither read the tenant's alert history nor hold
            its lease, so no rule is evaluated on these ticks. The liveness
            gauge does not advance on them either, so a sustained case also
            trips RavelAlertLoopStalled; this rule names the cause. Check the
            store reachability family.
```

The `600s` staleness threshold is ten default 60s evaluation intervals. It is
tighter than the maintenance group's `1800s` because the alert loop's interval
is five times shorter (60s against the 300s maintain default).

### At-rest scrubber (`ravel_scrub_*`)

Labels: `mode` and `signal`, plus `level` on the checksum-mismatch and
unreadable counters and `reason` on the seal-divergence and unreadable
counters. These carry no `tenant_hash` label.

| Metric | Meaning |
|---|---|
| `ravel_scrub_checksum_mismatch_total` | Data objects that failed at-rest integrity re-verification (a whole-object blake3 mismatch or a footer or section crc failure), by signal and level. Only bytes that were read and did not match count here. |
| `ravel_scrub_unreadable_total` | Objects and records the scrub could not read, and so could not verify, by signal, level, and reason. `reason="access_denied"` is a GET the store refused as access denied (a bucket or key policy, or a credential fault). `reason="permanent"` is any other GET error retrying cannot clear, or a record whose bytes do not decode. `reason="retry_exhausted"` is a GET that still failed with a retryable error after its unit had held the marker for the maximum number of ticks, so the scrub moved past it. An object counts once at its own level. A record counts once at its own level (`l0` a commit record, `l1` a compaction record, `rewrite` a rewrite record) however many objects it names. An object or record deleted after it was listed is not counted. |
| `ravel_scrub_postings_disagreement_total` | Objects whose covering name-postings object omitted a `__name__` the object really carries (a false negative), by signal. |
| `ravel_scrub_seal_divergence_total` | Divergences between the folded snapshot and the re-listed sealed commit history, by signal and reason. |
| `ravel_scrub_cursor_position` | Gauge. Fraction of the current scrub rotation's commit shard listing entries the content-tier cursor has consumed so far, by signal, in [0,1]. The unit is listing entries (commit, compaction, and rewrite records and tombstones), not data objects: one compaction record can name several parts. |
| `ravel_scrub_behind_total` | Shard ticks whose content-tier rotation cannot finish inside its window (`--scrub-period`, capped at half the tenant's retention window), by signal: the entries the tick needed to reach the end of the listing by the deadline exceeded four times the rotation's sustained rate. Causes are a sustained commit rate above four times the measured sustained rate, or scrub cycles slower than a tick. A marker held on a unit whose GETs keep failing retryably increments it only once the lost ticks push the needed rate past the ceiling, so it is not the signal for a held marker. `ravel_scrub_marker_held_ticks` is. A nonzero increase means some objects can expire before they are verified. The log line beside it names the entries per tick needed and allowed, the window, and the period. |
| `ravel_scrub_marker_held_ticks` | Gauge. Consecutive ticks the worst shard of the signal has held its content-tier marker on one unit because a GET of it failed with a retryable error (throttled, timeout, transient), as of the last scrub cycle. 0 when no shard is held. After 6 held ticks, six hours of tick cadence at the default one-hour tick, the next tick moves past the unit and counts each of its records and objects that still fail on `ravel_scrub_unreadable_total{reason="retry_exhausted"}`. |

#### Checksum mismatches

Alert on any increase of `ravel_scrub_checksum_mismatch_total`. Ravel keeps
no redundant copy to repair a corrupt object from, so a nonzero increase is
corruption that an operator must investigate, with one `level="l0"` caveat
(see [L0 mismatches](#l0-mismatches)). The `level` label names the part of
the commit lineage that the corrupt object came from:

| `level` | Object |
|---|---|
| `l0` | An original ingested segment. |
| `l1` | A compaction output part. |
| `rewrite` | A selective-erasure rewrite output part. |

The scrub corpus covers all three. A compaction or rewrite output part that a
record still lists as live joins the same rotation as an L0 segment.

#### Unreadable objects

Alert on any increase of `ravel_scrub_unreadable_total`. It is not
corruption, because the bytes were never read. What it counts stays
unverified until a later rotation reads it. An access denial recurs every
rotation until it is fixed. Its source is a key policy, a bucket policy, or
credentials that the maintain process lost.

A `reason="retry_exhausted"` count follows a `ravel_scrub_marker_held_ticks`
that climbed to 6. The store failed the reads of that unit on six ticks in a
row and again on the seventh. The scrub then moved past the unit, so that the
rest of the shard does not stay unverified.

#### Excluded output parts

The lineage filter applies to compaction and rewrite output parts only. It
leaves out three shapes:

- A compaction or rewrite record that a later rewrite record names in
  `superseded_record_key`. Its parts survive until a sweep retires them, and
  no query reads them.
- A compaction or rewrite record in a bucket that a retention tombstone has
  retired. Its parts also survive until a sweep retires them, and no query
  reads them.
- A compaction record that loses its bucket's overlap component to another
  compaction record. Two compactors that race leave two records in one bucket
  whose input sets share an L0 input. The catalog keeps one authoritative
  record per overlap component and serves none of the parts of the other.

The third exclusion has a caveat. No node that has adopted the overlap rule
reads the loser's parts, but a node that has not adopted it can still serve
them. The loser is also not horizon-bounded the way a superseded generation
is. The losing record keeps its parts referenced for as long as it exists, so
the sweep reclaims none of them.

#### L0 mismatches

L0 commit records are scrubbed whatever their lineage. That path has no
supersession or tombstone check, so an L0 segment that a live compaction has
already folded into a compaction output part stays in the rotation.

A `level="l0"` mismatch on an already-compacted hour can therefore name a
redundant copy, and not data that a query can still reach. The catalog puts
the input identities of a live compaction record into its query-time excluded
set. Before you treat an `l0` mismatch as unrecoverable, check whether the
hour is compacted.

#### Seal divergence and the cursor

The `reason` label on `ravel_scrub_seal_divergence_total` carries one of two
values:

| `reason` | Meaning |
|---|---|
| `missing` | A sealed commit record absent from the snapshot, an under-count. |
| `mismatched` | A snapshot entry whose content hash disagrees with the sealed record. |

An orphaned entry, a snapshot entry with no surviving commit record, is the
expected retention-after-fold shape and is never counted.

A `ravel_scrub_cursor_position` stuck near 0 means that scrubbing does not
keep pace with the configured `--scrub-period`. The gauge drops to 0 once
when a rotation rolls over. It also drops once after an upgrade from a build
that kept an object-based cursor: that cursor's position is discarded and the
first tick starts a fresh rotation.

### Read cache (`ravel_cache_*`)

Labels: `mode`, `cache`, and `tier`. The whole family is absent under
`--disable-cache`.

| Label | Values |
|---|---|
| `cache` | `fetch` (the query fetchers' RAM cache) or `catalog` (the catalog's content-addressed byte cache). Both caches share one family. |
| `tier` | Present only when a family has a local-disk tier configured with `--cache-dir`. Its RAM sample then carries `tier="ram"` and its disk sample `tier="disk"`. With no disk tier, a family renders one sample with no `tier` label. |

| Metric | Meaning |
|---|---|
| `ravel_cache_hits_total` | Read-cache lookups served from the cache. |
| `ravel_cache_misses_total` | Read-cache lookups not found in the cache. |
| `ravel_cache_bytes_served_total` | Bytes served from the cache on a hit. |
| `ravel_cache_bytes_admitted_total` | Bytes admitted into the cache after a miss. |
| `ravel_cache_evictions_total` | Entries evicted from the read cache by its S3-FIFO policy. |
| `ravel_cache_disk_errors_degraded_to_misses_total` | Disk-tier reads that found an entry but discarded it as unhealthy rather than a clean miss. |
| `ravel_cache_disk_entries_expired_max_age_total` | Disk-tier entries dropped for aging past the per-entry max-age, across the hit check, the startup scan, and the periodic background sweep. An age-based expiry, separate from the capacity-driven eviction counter above. |
| `ravel_cache_resident_entries` | Gauge. Entries currently held in this cache tier, live rather than cumulative. Rendered for both `cache="fetch"` and `cache="catalog"`. |
| `ravel_cache_resident_bytes` | Gauge. Payload bytes currently held in this cache tier, live rather than cumulative. Rendered for both `cache="fetch"` and `cache="catalog"`. |
| `ravel_cache_max_bytes` | Gauge. The resolved startup byte ceiling for this cache, for comparing held-vs-budgeted. Rendered for both `cache="fetch"` and `cache="catalog"`, with no `tier` label even when a disk tier exists. |

The renderer leaves the ratios for PromQL to compute, per `cache` and per
`tier`:

| Ratio | Expression |
|---|---|
| Request hit rate | `hits / (hits + misses)` |
| Byte hit rate | `bytes_served / (bytes_served + bytes_admitted)` |
| Held-vs-budgeted | `ravel_cache_resident_bytes / on(mode, cache) group_left ravel_cache_max_bytes` |

Use the `on(mode, cache) group_left` form for the held-vs-budgeted ratio.
With a disk tier, the residency series carry a `tier` label and the ceiling
does not, so a plain division matches nothing.

Both caches bound each tier to the configured byte figure independently. The
RAM and disk tiers of the fetch cache are built from the same
`--cache-max-bytes` limits, and those of the catalog cache from the same
catalog limits. A tiered cache of either kind can therefore hold up to twice
its configured bytes in total. Read its held-vs-budgeted ratio per tier, not
summed across tiers.

### Admission

Labels: `mode`, `tenant_hash`, `signal`, plus `reason` on the rejection
counter. This family folds tenants as
[The `tenant_hash="other"` fold](#the-tenant_hashother-fold) describes. The
[admission limits guide](admission-limits.md) covers this family in
operational depth.

| Metric | Meaning |
|---|---|
| `ravel_admission_active_series` | Gauge. Active series (metrics) or streams (logs) tracked for the active cap, by tenant and signal. |
| `ravel_admission_admitted_total` | Requests admitted past the ingest byte-rate layer, by tenant and signal. |
| `ravel_admission_admitted_bytes_total` | Charged (decompressed) bytes admitted past the ingest byte-rate layer, by tenant and signal. For a gzip OTLP request this is the decompressed size. For an uncompressed request it equals the wire size. |
| `ravel_ingest_wire_bytes_total` | Wire (on-the-wire, compressed when the client compressed) OTLP request-body bytes admitted, by tenant and signal. |
| `ravel_admission_rejected_total` | Admission rejections, by tenant, signal, and reason. |
| `ravel_ingest_body_conversions_total` | Log records whose structured (array or map) body was converted to canonical JSON text at normalization, by tenant and signal. Not a rejection, and not a count of stored records: see "Neither rule alerts on" below. |
| `ravel_ingest_resource_attrs_dropped_total` | Metric resource attributes outside the configured allowlist, dropped rather than turned into labels, by tenant, for the metrics signal only. Not a rejection: counted before the series cap and the write, so not a count of stored points. Covers OTLP HTTP and OTLP gRPC ingest only. OTAP is not covered, since it builds no resource labels at all. See "Resource attributes outside the allowlist" below. |
| `ravel_admission_reconciliation_failures_total` | Fleet-admission reconciliation cycles whose sibling-snapshot read (LIST or GET) failed, by tenant and signal. The last-known soft threshold stays in force. |

Four more series report the reconciliation cycle. They carry `mode` alone,
with no `tenant_hash` or `signal`, because one cycle reconciles every tenant
that the process tracks.

| Metric | Meaning |
|---|---|
| `ravel_admission_reconciliation_cycle_duration_seconds` | Gauge. Duration of the last completed reconciliation cycle. |
| `ravel_admission_reconciliation_siblings_observed` | Gauge. Distinct non-stale sibling processes the last cycle saw, the live fleet size this process reconciled against. |
| `ravel_admission_reconciliation_stale_keys_skipped` | Gauge. Snapshot keys the last cycle skipped reading because the listing already showed them past the staleness window. |
| `ravel_admission_reconciliation_keys_reaped_total` | Snapshot keys past the reap horizon deleted by reconciliation cycles since process start. |

These four series move first when reconciliation degrades. None of them
shows as a failure, because the listings and reads all succeed.

- **Cycle duration.** A cycle whose duration approaches twice the
  reconciliation interval ages every sibling snapshot past the staleness
  window before it is read. Each process then reads the fleet as empty and
  starts to enforce the whole tenant cap alone. Alert on the duration against
  your configured interval.
- **Siblings observed.** Alert on
  `ravel_admission_reconciliation_siblings_observed` falling to zero while
  replicas are up.
- **Alert scope.** Scope both alerts to `mode="all"` and `mode="gateway"`.
  The four series render on every replica, but only those two modes run the
  reconciliation loop. A `query` or `maintain` replica reports
  `ravel_admission_reconciliation_siblings_observed` as zero for its whole
  life.
- **Stale keys.** Growth in
  `ravel_admission_reconciliation_stale_keys_skipped` while the siblings
  gauge is flat means that the control-plane prefix fills with the keys of
  dead processes. If
  `rate(ravel_admission_reconciliation_keys_reaped_total[1h])` is at zero at
  the same time, the prefix fills faster than it is cleared.

A sustained nonzero `ravel_admission_reconciliation_failures_total` rate
means that a process cannot read its siblings' snapshots and falls back to
its last-computed soft threshold. Admission never fails closed on it. It
signals that fleet-wide accuracy degrades, not that ingest is down.

The active-streams count for logs renders under
`ravel_admission_active_series` with `signal="logs"`, not under a separate
metric name.

#### Reading the `reason` label

The `reason` label carries one of six values. Each reason counts a different
unit, so a rate summed across reasons means nothing. Read them separately.

| `reason` | Counts | What it means |
|---|---|---|
| `byte_rate` | Requests | The tenant sent more charged bytes per second than its ingest byte-rate limit allows. |
| `clock` | Requests | The receiving replica's own clock was implausible, so the request was refused before any data was read. The fault is the replica's. |
| `series_rate` | Series | New series or log streams appeared faster than the creation-rate limit allows. |
| `series_cap` | Series | The tenant is at its active series or stream cap, so points for series past the cap were dropped. |
| `skew` | Points, records, or spans | The event timestamp sat too far ahead of, or behind, ingest time. A sender clock problem, or a backfill wider than the accepted lag. |
| `structural` | Points, records, or spans | The data itself cannot be represented: a delta-temporality metric, an over-long label, a body kind with no stored form. Retrying the same payload always fails the same way. |

`skew` and `structural` count individual points, log records, or spans. They
match the count that the sender receives in the OTLP partial-success
response, so a client that reads `rejected_data_points` and an operator who
reads this counter see the same number. The OTLP Arrow (OTAP) surface has no
partial-success field, so on that surface this counter is the only place
where the drop appears.

The two reasons need different alerts:

- `skew` is usually a fleet-wide clock or backfill problem. It clears when
  the sender is fixed.
- `structural` never clears without a change to what the sender emits. Page a
  human on any sustained rate. See
  [the ingest guide's temporality recipe](ingest.md#delta-temporality-metrics).

Both alerts ship as the `ravel-ingest-rejections` group.

```yaml
groups:
  - name: ravel-ingest-rejections
    rules:
      - alert: RavelStructuralRejections
        expr: |
          sum by (tenant_hash, signal) (
            rate(ravel_admission_rejected_total{reason="structural"}[5m])
          ) > 0
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: >-
            Tenant {{ $labels.tenant_hash }} is sending {{ $labels.signal }}
            data Ravel cannot represent
          description: >-
            Structural rejections do not clear on retry. Check the OTLP
            partial-success message the sender receives for the reason, then
            fix the exporter or add a collector processor for it.
      - alert: RavelEventTimeSkew
        expr: |
          sum by (tenant_hash, signal) (
            rate(ravel_admission_rejected_total{reason="skew"}[5m])
          ) > 1
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: >-
            Tenant {{ $labels.tenant_hash }} is dropping {{ $labels.signal }}
            data outside the accepted event-time window
          description: >-
            This is an absolute rate of rejected points, records, or spans per
            second, not a fraction of the tenant's traffic. Check sender clock
            sync first, then whether a backfill is running outside the tenant's
            accepted ingest lag. Raise the threshold for a tenant that runs a
            steady expected backfill.
```

How to read the two rules:

- Both are absolute rates of rejected units per second, by tenant and signal.
  No per-tenant admitted-points series exists to divide by.
  `ravel_admission_rejected_total{reason="skew"}` counts individual points,
  records, or spans, and `ravel_admission_admitted_total` counts requests. A
  ratio of the two is inflated by the mean points per request, which is three
  orders of magnitude at typical batching.
- An absolute rate shows how much data a tenant loses to the event-time
  window. It does not show what fraction of the tenant's traffic that is. A
  large tenant with a steady backfill and a small tenant with a broken clock
  can trip the same threshold. Tune the threshold per tenant, and use the
  alert as a prompt to check clock sync and backfill status.
- The skew rule uses a nonzero threshold because a few late points are
  normal.
- The structural rule keeps `> 0`. One sender that emits a metric type that
  Ravel cannot store drops every point of that metric forever.

Neither rule alerts on `ravel_ingest_body_conversions_total`. A sustained
rate means that a sender emits structured log bodies, which is supported and
is not a fault. The counter explains a query that returns JSON text where a
reader expected a plain message.

The ingest guide, the admission-limits reference, and the `HELP` text of
`ravel_ingest_body_conversions_total` point here for its normative
description:

- It counts conversions at normalization, not stored records. The logs ingest
  handler increments it as soon as normalization returns. That is before the
  layer-4 active-stream cap drops the records whose stream is over the cap,
  and before the shard write runs.
- A converted record can therefore be counted and then not stored: dropped by
  the stream cap, or lost with every other record in a request whose write
  fails.
- Read it as a conversion rate: how much of this tenant's log traffic arrives
  with a structured body. Never read it as a count of rows in storage.
- It is not a rejection counter, so it is its own family and not a `reason`
  on `ravel_admission_rejected_total`. Normalization admitted each record
  that it counts, and an alert on rejection reasons must see nothing from it.

#### Resource attributes outside the allowlist

`job` and `instance` come from `service.name`/`service.namespace` and
`service.instance.id`. Every other resource attribute becomes a label only if
it is in the `resource_attribute_allowlist`, which is fixed at build time.
An attribute outside both sets is dropped. Two resources that differ only in
such an attribute flatten to the same label set and merge into one series.

`ravel_ingest_resource_attrs_dropped_total` makes that drop visible:

- It counts attributes, not points. One resource with five out-of-allowlist
  attributes adds 5, however many points that resource carried.
- It is informational, like `ravel_ingest_body_conversions_total`, and is not
  a `reason` on `ravel_admission_rejected_total`. It is counted before the
  series cap and the write, so it is not a count of stored points. An alert
  on rejection reasons must see nothing from it.
- It covers OTLP HTTP and OTLP gRPC ingest, for the metrics signal only. OTAP
  is not covered, because its normalizer builds no resource labels. Logs and
  traces never build them either.
- The key names of the dropped attributes are not reported anywhere, on this
  counter or in the OTLP partial-success `error_message`. A resource
  attribute's key is caller-supplied and unbounded. A label or a per-key
  series built from it gives a sender control over the cardinality of this
  process.
- A sustained nonzero rate means that resources carry attributes that the
  allowlist does not cover. To find which ones, inspect the sender's resource
  attributes directly.
- The count is a lower bound when a resource repeats a job/instance-source
  or allowlisted key, or gives one an empty value. Only the first occurrence
  of a repeated key is consulted. An empty-value label is dropped after this
  count already excluded that attribute by key. Neither case is added back as
  dropped.

The counter does not change what gets dropped.

#### Wire bytes and compression

`ravel_ingest_wire_bytes_total` carries the `ravel_ingest_` prefix because
the ingest byte-metrics tracker emits it, not the admission snapshot. It
folds tenants by the same allowlist. Read it with the admission counters.

The ratio
`ravel_admission_admitted_bytes_total / ravel_ingest_wire_bytes_total` is the
tenant's effective compression factor. Read the ratio, not either counter
alone:

| Reading | Meaning |
|---|---|
| The ratio holds roughly steady while admitted bytes rise. | The tenant grew its telemetry. |
| The ratio falls toward 1: admitted bytes stay flat and wire bytes jump. | The tenant turned client-side compression off. |

### Per-query cost

Labels: `mode`, `tenant_hash`, `workload_class`. The `workload_class` label
carries `interactive` or `background`. Only `interactive` occurs.

Every read surface folds its per-query cost into the `ravel_query_*` family:

- `POST /api/v1/sql` and `POST /api/v1/analytics`
- The Prometheus-shaped `GET /api/v1/query`, `GET /api/v1/query_range`,
  `GET /api/v1/labels`, and `GET /api/v1/series`
- Every Flight SQL request

The [cost model guide](cost-model.md#per-query-cost-accounting) explains the
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

The SQL query endpoint also records the final outcome of each query: success,
error, timeout, or cancelled. A timeout or a cancelled outcome (the client
disconnected while the query was still running) carries the cost that the
query incurred up to that point, not zeros:

- An object-store request is counted when it is issued. The query reports
  every request that it issued before it stopped, including a fetch still
  outstanding when the deadline trips or the caller goes away.
- The transferred bytes of a fetch are counted when the fetch returns,
  because their length is not known before then. The query reports the bytes
  of the fetches that had completed by then.
- Fetches that the query did not issue before it stopped are not counted.

### SQL DDL statements and their store cost (`ravel_sql_ddl_*`)

Labels: `mode` and `tenant_hash` on every sample, folded the same way as in
the per-query cost family. The statement counter adds `kind` and `outcome`.
The request counter adds `phase` and `op`. The byte counter adds `phase`.

A process built with the `sql` feature renders the three headers on every
scrape. A build without it omits the families. The samples of a tenant bucket
appear after the tenant sends its first DDL statement. From then on every
(`phase`, `op`) pair and every `phase` renders, zeros included.

A `CREATE EXTERNAL TABLE` or `DROP TABLE` over `POST /api/v1/sql` is not a
query. Its cost never enters the `ravel_query_*` family, the usage record, or
any query budget. These three counters report it.

| Metric | Meaning |
|---|---|
| `ravel_sql_ddl_statements_total{kind, outcome}` | DDL statements handled, by leading keyword (`kind="create"` or `"drop"`) and by how each ended (`outcome="created"`, `"dropped"`, `"noop"` or `"error"`). Every statement past the `timeout` check counts once. A statement refused for the `ddl` capability, by a failed `attempted` audit submission, or by admission counts as `error` with zero cost. An executed statement counts its own outcome, or `error` when it failed. A statement whose execution task panics is not counted. A request with a malformed `timeout` is not counted, the same as one whose body is not valid JSON. |
| `ravel_sql_ddl_store_requests_total{phase, op}` | Object-store requests the statement issued, one per call at the object-store trait, counted when issued whether it succeeds, fails, or is still outstanding when the statement deadline trips. Retries below the trait are not counted. |
| `ravel_sql_ddl_store_bytes_total{phase}` | Response-body bytes of completed GET requests as returned across the object-store trait, undecoded (a ranged read counts the range returned). Request bodies, failed GETs, HEAD, LIST, PUT and DELETE count zero, and retries below the trait are not counted. |

Each request that a statement issues is counted in one phase:

- `grant`: the read of the tenant's location grants record.
- `probe`: the listing or HEAD that finds an object under the `LOCATION`, and
  the qualification probes on the external store and on Ravel's own store,
  including the scratch object the bucket probe writes and deletes.
- `snapshot`: the listing of the `LOCATION` and the Parquet footer reads.
- `write`: the manifest resolve and write, including the existence check a
  plain `CREATE` makes first. A `DROP` touches only this phase.

A statement that fails reports the requests and bytes that it accrued before
it failed. That includes a statement cut off by its deadline. A statement
refused by validation issued no request.

### Metric metadata cache (`query_metadata_cache_*`)

Labels: `mode` only. This per-process cache holds each tenant's metric
metadata record. It serves `/api/v1/metadata` at one GET per (tenant, refresh
horizon, process). The family renders only in a request-serving mode that
built the cache (`all` or `query`). A gateway-only or maintain-only process
omits the family. All four are cumulative counters.

| Metric | Meaning |
|---|---|
| `query_metadata_cache_hits_total` | Metadata requests served from an already-cached tenant record, fresh or stale. |
| `query_metadata_cache_misses_total` | Metadata requests that found no cached record and did an inline fill GET. |
| `query_metadata_cache_refreshes_total` | Background refreshes started by a past-horizon request that won the single-flight (includes refreshes that later errored). |
| `query_metadata_cache_refresh_errors_total` | Background refreshes that failed their GET or decode. The stale record keeps being served and the client never sees the error. A climbing value means the record is becoming unreadable. |

The request hit rate is `hits / (hits + misses)`. A refresh-error rate that
rises toward the refresh rate means that the metadata record is unreadable
while stale data is still served. The operations guide pages on that state.

### Query audit (`ravel_audit_write_failures_total`)

Labels: `mode` only. The family carries no `tenant_hash` label, because that
label discloses which tenant's writes failed on this unauthenticated route. The family renders only in a mode that installed the query-audit
pipeline (`all` or `query`). A gateway-only or maintain-only process omits
it.

A flush writes one object per tenant. Within one flush, the failure counter
increments once per tenant group whose write fails.

| Metric | Meaning |
|---|---|
| `ravel_audit_write_failures_total` | Query-audit writes that failed and were released anyway under `--audit-mode best-effort`. Each one is a query that was served with no durable audit record. |
| `ravel_audit_put_retries_total` | audit PUT attempts retried after a transient object-store error. |

How the two counters read:

- A transient object-store error (a timeout or a throttle response) is
  retried up to two additional times with a short jittered backoff. All
  attempts fit in a 30-second budget per tenant group, which covers both of
  its PUTs. A flush writes its tenant groups one after another, each under
  its own budget.
- `ravel_audit_put_retries_total` counts each retried attempt.
  `ravel_audit_write_failures_total` counts the writes that kept failing
  until the budget ran out, not a single slow request. A climbing retry
  counter with a flat failure counter means that the store is degraded and
  the retries still absorb it.
- Under `--audit-mode required` (the default) a failed audit write fails the
  query with a 503 and is not counted here. The failure counter is therefore
  always zero on a fail-closed deployment.
- Under `--audit-mode best-effort`, any increase means that the audit trail
  is incomplete while queries keep succeeding. Alert on the increase, not on
  a threshold.

### Distributed read fan-out (`ravel_distrib_*`)

Labels: `mode` only, with these additions:

- `le` on the histogram buckets.
- `class` (`pinned`|`resolve`) on the fragment in-flight gauge, the
  admission-wait counter and the deadline-stop counter.
- `reason` on the fragment capability reject counter.

The family carries no per-shard, per-worker, or per-tenant label, so a
fan-out that spans many workers and tenants adds no series. It renders only
when the process runs with `--distributed-query`. A local-only process omits
the family.

| Metric | Meaning |
|---|---|
| `ravel_distrib_fragment_requests_total` | Inbound fragment (`SeriesFetch`) requests served after passing capability auth and fragment admission. Worker side. |
| `ravel_distrib_fragment_auth_failures_total` | Inbound `Resolve`-scope (cross-cluster federation) fragment requests whose presented credential did not resolve to a tenant. Worker side. It does not count `Pinned` capability rejections: those are `ravel_distrib_fragment_capability_rejects_total`. |
| `ravel_distrib_fragment_capability_rejects_total{reason}` | Counter. Inbound `Pinned` (intra-cluster) fragment requests refused at fragment capability verification: `Unauthenticated` for every reason except `expired`, which is answered in-band with a zero-spend `TIMEOUT` (an expiry found on arrival or after admission counts here alike). Worker side. One series per reason, each rendered from zero. The checks run in a fixed order and a refusal counts under the first one it fails, so a capability that is both expired and minted for another tenant counts as `expired`. `reason="missing"`: the request carried no capability at all. `reason="bad_mac"`: the capability is malformed (it does not decode as a capability) or its MAC verifies under none of this node's fragment keys, which is the reason a coordinator minting under a key this worker does not hold produces. `reason="expired"`: the capability's expiry is at or before this worker's clock. `reason="tenant_mismatch"`: the request names a tenant other than the one the capability was minted for. `reason="query_mismatch"`: the request's query id or signal differs from the capability's. A `Pinned` fetch on the public listener while `--fragment-listener` is set is refused `PermissionDenied` before verification and is counted under no reason. The coordinator whose capability was refused counts no reason: for an `Unauthenticated` refusal its `warn` log line carries the worker's refusal message and its re-dispatch and fallback counters move. An `expired` refusal ends the query with `DeadlineExceeded` and moves neither once the coordinator's own monotonic deadline has passed, and before it is re-dispatched like an `Unauthenticated` one, with a `warn` line saying the worker's clock runs ahead and no quarantine mark. An `Unauthenticated` refusal from a worker that predates the in-band `TIMEOUT`, for a slice whose deadline has passed on the coordinator's clock, is an expired capability too and marks no quarantine. So this counter, on the worker, is the per-reason count. |
| `ravel_distrib_fragment_inflight{class}` | Gauge. Fragment requests currently holding a fragment-admission permit, by admission class: `class="pinned"` for intra-cluster requests (admits against `--max-inflight-fragments`), `class="resolve"` for cross-cluster federation requests (admits against `--max-inflight-federated-resolves`). The two classes never share a permit pool, so a peer cluster saturating `resolve` cannot starve this cluster's own `pinned` slices. |
| `ravel_distrib_fragment_admission_waits_total{class}` | Counter. Inbound fragment requests, by admission class, that found their class's semaphore saturated at acquire time and had to queue. |
| `ravel_distrib_fragment_deadline_stops_total{class}` | Counter. Admitted fragment slices this worker stopped mid-run because their query's deadline passed while they read, by admission class: `class="pinned"` for a slice whose capability expired, `class="resolve"` for a federated request whose carried deadline passed. Worker side. Both classes render from zero. Each stop ends in-band with `TIMEOUT`, which fails its query with `DeadlineExceeded` on the coordinator once the coordinator's own monotonic deadline has passed. One that reaches the coordinator before it is re-dispatched without a quarantine mark. A request refused at its deadline before it ran is not counted here: a `Pinned` one counts under `ravel_distrib_fragment_capability_rejects_total{reason="expired"}` (on arrival or after admission), and a `Resolve` one counts nowhere. Read the two `pinned` counters together: rejects mean the deadline had already passed when the slice arrived or was admitted, stops mean it passed while the slice read. Either one rising while client queries still fit their timeouts points at this worker's clock running ahead of the coordinator's, since the deadline is a coordinator wall-clock instant compared on the worker's clock. |
| `ravel_distrib_fragment_record_get_requests_total` | Counter. Object-store GETs this worker's pinned resolves issued to read each pinned segment's own durable record: one per pinned L0 segment, one per pinned L1 segment, and two for an L1 segment that only an erasure rewrite record describes. Worker side. These GETs are charged to no query's accounting and the slice summary cannot carry them, so this counter is the only report of the resolve phase's request cost: expect it to track `ravel_distrib_fragment_requests_total` times the mean pinned segments per slice, and read a rise against that ratio as rewrite-record fallbacks. |
| `ravel_distrib_fragment_record_get_bytes_total` | Counter. Bytes those record GETs transferred, as the store served them (wire bytes, not decompressed, and a GET that missed transferred none, so a rewrite-only L1 segment adds two requests and one record's bytes). Divide by the request counter for the mean record size. |
| `ravel_distrib_slices_local_total` | Slices this coordinator executed locally because it owns them (self-mapped, no network hop). |
| `ravel_distrib_slices_remote_total` | Slices this coordinator dispatched to a remote worker and read back over the wire (counts the attempt that produced the usable result, whether the primary or the re-dispatch). |
| `ravel_distrib_slices_redispatched_total` | Slices whose rendezvous-primary worker was lost at transport, returned `Unavailable`, or refused the slice for its deadline before the coordinator's own deadline passed, so the coordinator re-dispatched the slice once to the next rendezvous worker. |
| `ravel_distrib_slices_fallback_total` | Slices that fell back to coordinator-local execution after the primary and its one re-dispatch both failed re-dispatchably (transport loss, `Unavailable`, or a deadline refusal before the coordinator's own deadline), rather than failing the query. |
| `ravel_distrib_slice_fetch_seconds` | Per-slice fetch latency histogram, covering both locally-run and remote slices. |
| `ravel_distrib_quarantine_marks_total` | Dead fragment endpoints marked into the coordinator's quarantine map after a re-dispatchable dispatch failure (transport loss or an `Unavailable` summary), cumulative. A deadline refusal (`TIMEOUT`, or an older worker's `Unauthenticated` for a capability already expired on the coordinator's clock) marks nothing. |
| `ravel_distrib_quarantine_readmits_total` | Quarantined endpoints readmitted by a strictly newer worker heartbeat stamp (the half-open probe), cumulative. |
| `ravel_distrib_quarantine_current` | Gauge. Fragment endpoints currently held in the coordinator's quarantine map. |

Fragment admission is a workload class distinct from client-query admission
(`--max-inflight-fragments`, separate from the query concurrency limit). A
burst of inbound fragments cannot starve the coordinator's own client
queries, and client queries cannot starve fragments.

| Reading | Meaning |
|---|---|
| `ravel_distrib_slices_redispatched_total` rises. | A rendezvous-primary worker is lost or returns `Unavailable`, and slices retry on their next owner. |
| `ravel_distrib_slices_fallback_total` also rises. | The primary and its failover are both unreachable, and the fan-out degrades to local execution. |

In both cases the query still returns correct results, because the
coordinator can read any slice itself. Latency climbs.

A worker-reported `CORRUPT` status is never re-dispatched or masked by
fallback. It fails the query with a typed error, so the corruption stays
visible.

### SQL slice capability rejects (`ravel_sql_slice_rejects_total`)

Labels: `mode` and `reason`. The family renders on every process that built
the Flight SQL service, with or without `--distributed-query`, and every
reason renders from zero. A process that serves no Flight SQL omits the
family: a `gateway` or `maintain` mode process, or a build without the
`flight-sql` feature.

| Metric | Meaning |
|---|---|
| `ravel_sql_slice_rejects_total{reason}` | Inbound SQL slice `DoGet` requests refused at slice capability verification. Worker side. `reason="missing"`: a `DoGet` on the dedicated fragment listener that carries no slice capability, which is either a `TicketStatementQuery` whose handle is empty or ends before a complete ticket, or a ticket that decodes as a protobuf `Any` but names no Flight SQL command. A handle long enough to hold a ticket that then fails verification is `bad_mac`, not `missing`. Two refusals on that listener are not counted under any reason: a `DoGet` carrying another Flight SQL command, refused `permission_denied` before slice verification, and a ticket that is not a valid protobuf `Any` (or names a Flight SQL command whose body does not decode), refused by the Flight SQL dispatcher before the service sees it. `reason="bad_mac"`: the ticket verifies under neither this node's slice keys nor its client keys. `reason="expired"`: the ticket's deadline has passed. `reason="wrong_surface"`: a client ticket presented as a slice, a slice-key ticket that is not a servable slice, or a slice ticket on the public listener. |

Only the dedicated fragment listener counts `missing` and `bad_mac`. The public
listener recognises a slice ticket only when its MAC verifies under this
node's slice keys. A forged ticket, or one minted under a key that this node
does not hold, is not recognised as a slice ticket and is not counted. It
takes the client path and is refused there.

### SQL slice TLS dials (`ravel_sql_slice_tls_dials_total`)

Label: `mode`. The counter renders on every process that built the Flight SQL
service, from zero. It is coordinator side: one increment per SQL slice
`DoGet` this process dials to a worker with its client TLS configuration
attached. The server builds every slice location as `https://`, so each
count is a TLS dial to the worker's dedicated fragment listener; the counter
records that the configuration was attached, not the handshake itself. A
PromQL fan-out does not move it.

| Metric | Meaning |
|---|---|
| `ravel_sql_slice_tls_dials_total` | Outbound SQL slice `DoGet` fetches this coordinator dialed with its client TLS configuration. A distributed SQL statement adds one per slice fetch it sends to a worker, a re-dispatch included; a statement that runs whole-set on the coordinator, and a slice read coordinator-local, add none. |

### CPU gates and runtime

Labels: `mode` and `gate` (`read`|`write`) on every CPU gate sample, plus
`site` on the two per-site counters. The runtime gauges and the heartbeat age
carry `mode` only, and the per-worker busy counter adds `worker`. The
families render in every mode.

A CPU gate runs codec work on the tokio blocking pool behind a fixed number
of permits. A job waits for a permit, runs, and releases the permit when it
returns.

| Gate | Serves | Sized by |
|---|---|---|
| Read | Query, catalog and maintenance decode. | `--cpu-gate-read-permits` (default `max(1, cores - 1)`) |
| Write | Flush encode and ingest payload decode. | `--cpu-gate-write-permits` (default `max(1, cores / 2)`) |

A job below the gate's inline floor runs on the calling thread and takes no
permit. The floor is 256 KiB of uncompressed bytes, or 100,000 samples for a
PromQL evaluation.

The catalog's snapshot part, postings and column-statistics decodes are the
only work submitted to the gates so far. They use the read gate's
`catalog_part`, `catalog_postings` and `catalog_column_stats` sites. The job
and inline figures of every other site, and all of the write gate's figures,
stay at 0 until that decode or encode moves onto a gate.

| Metric | Meaning |
|---|---|
| `ravel_cpu_gate_permits` | Gauge. Jobs the gate can run at once. |
| `ravel_cpu_gate_running` | Gauge. Jobs holding a permit right now, including a job whose caller stopped waiting. |
| `ravel_cpu_gate_queued` | Gauge. Callers waiting for a permit right now. |
| `ravel_cpu_gate_wait_seconds_sum`, `ravel_cpu_gate_wait_seconds_count` | Summary with no quantiles. Time callers waited for a permit, and the number of waits, counting only waits that got a permit. |
| `ravel_cpu_gate_run_seconds_sum`, `ravel_cpu_gate_run_seconds_count` | Summary with no quantiles. Time jobs ran on the blocking pool, and the number of jobs. |
| `ravel_cpu_gate_abandoned_total` | Counter. Jobs that ran to completion after their caller stopped waiting, so their result was discarded. A started decode cannot be interrupted, so a cancelled query can leave one such job per permit it held. |
| `ravel_cpu_gate_jobs_total{site}` | Counter. Jobs a call site ran through a permit. |
| `ravel_cpu_gate_inline_total{site}` | Counter. Jobs a call site ran inline, below the inline floor. |
| `ravel_runtime_workers` | Gauge. Worker threads of the tokio runtime serving the process. |
| `ravel_runtime_alive_tasks` | Gauge. Tasks alive on the runtime. |
| `ravel_runtime_global_queue_depth` | Gauge. Tasks waiting in the runtime's global queue. |
| `ravel_runtime_worker_busy_seconds_total{worker}` | Counter. Time each worker spent busy. Rendered only on targets with 64-bit atomics, which covers every target Ravel builds for. |
| `ravel_health_heartbeat_age_seconds` | Gauge. Seconds since the heartbeat task on the main runtime last ran. It beats every second, so an idle node reads under 2. A value that keeps growing means the main runtime is not scheduling the task. Rendered in every mode, with or without `--listen-health`, whose `/readyz` fails above 30 and `/healthz` above 60 on this same heartbeat. |

The `site` label takes one value per wrapped call site, from a closed set per
gate. Every site renders a sample even at 0.

| Gate | Sites |
|---|---|
| Read | `catalog_part`, `catalog_postings`, `catalog_column_stats`, `metrics_meta`, `segment_section`, `segment_sparse_catalog`, `log_block`, `log_postings`, `log_section`, `span_block`, `span_section`, `promql_eval`, `compaction`, `fold`, `scrub`, and `reachability` |
| Write | `metrics_flush`, `log_flush`, `span_flush`, `otap_decode`, `otlp_http_gzip`, and `remote_write_snappy` |

A node that is CPU-bound on decode shows a rising mean wait while the gate is
full. The mean wait is:

```promql
rate(ravel_cpu_gate_wait_seconds_sum[5m])
  / rate(ravel_cpu_gate_wait_seconds_count[5m])
```

The gate is full when `ravel_cpu_gate_running` equals
`ravel_cpu_gate_permits` for the same `gate`.

| Reading | Meaning |
|---|---|
| A rising mean wait and a full gate. | The node needs more replicas or more permits. On a node shared with other CPU-heavy work, lower the read gate's permits instead. |
| A per-worker busy rate near 1 on every worker, from `rate(ravel_runtime_worker_busy_seconds_total[5m])`. | The runtime workers are saturated, which the gates do not cover. |

## Reading estimate against actual

The estimate is an upper envelope, not a prediction. The planner takes the
worst case wherever it cannot bound a quantity, so a correct estimate lands
at or above the actual. The estimate and the actual render under separate
names, so PromQL can compute their ratio.

Divide an actual by its matching estimate. The requests ratio is
`ravel_query_s3_requests_total / ravel_query_estimated_requests_total`.

| Ratio | Meaning |
|---|---|
| At or below 1 | The actual stayed inside its upper envelope. This is the healthy state. |
| Above 1 | The actual exceeded the envelope. Either the cost model has a gap, where the estimate omits a real source of spend, or a runaway query pattern occurred that the model did not anticipate. |

Either cause of a ratio above 1 needs an operator's attention, because a
later admission decision can reject queries on this envelope. Nothing rejects
a query on it at present. It is measurement only.

## Worked examples

Run the PromQL in each example against Ravel's own `/metrics`. Read the named
number, and act on what it rules in or out.

### A slow query

1. Compute average requests per query. Run `rate(ravel_query_s3_requests_total[5m]) / rate(ravel_query_queries_total[5m])`.
2. Read the result. It is object-store requests per query over the window.
3. If the value is high, the query fans out over many objects.
4. Compute store latency. Run `histogram_quantile(0.99, rate(ravel_store_latency_seconds_bucket[5m]))`.
5. If requests per query is high, a wide fan-out rules in as the cause.
6. If requests per query is low and latency is high, a slow store rules in and fan-out rules out.

### One heavy tenant

1. Confirm the flag. Per-tenant bytes need `--metrics-tenant-labels` on.
2. Rank tenants by byte rate. Run `sum by (tenant_hash) (rate(ravel_query_s3_bytes_total[1h]))`.
3. Read the top `tenant_hash`. It is the tenant whose queries cost the most object-store bytes.
4. If one `tenant_hash` dominates the rest, a single heavy tenant rules in.
5. If every series folds into `tenant_hash="other"`, the flag is off.
6. If the flag is off, turn on `--metrics-tenant-labels` on a trusted scrape network, then repeat step 2.

### A low cache hit rate

1. Compute the request hit rate. Run `rate(ravel_cache_hits_total[5m]) / (rate(ravel_cache_hits_total[5m]) + rate(ravel_cache_misses_total[5m]))`.
2. Read the result. A value near 1 means the cache serves most reads.
3. If the hit rate is low, the cache is not helping.
4. Compute the eviction rate. Run `rate(ravel_cache_evictions_total[5m])`.
5. If evictions are high alongside the misses, an undersized cache rules in. Raise the ceiling for the `cache` label that is evicting: `--cache-max-bytes` for `cache="fetch"`, `--catalog-cache-max-bytes` for `cache="catalog"`.
6. If evictions are near zero alongside the misses, cold or unique reads rule in and undersizing rules out.

## Known gaps

Three gaps limit what the per-query cost family can show:

- A failed, timed-out, or cancelled query folds the cost that it incurred,
  but the exported counters do not yet split by outcome. That spend is
  indistinguishable from the spend of a successful query.
- A Flight SQL statement records two folds for one logical query.
- An abandoned Flight fetch still records its partial cost.

The [cost model guide](cost-model.md#per-query-cost-accounting) sets out each
gap in full.

## Background

The `/metrics` route, the label allowlist, and per-query cost accounting:
ADR-0044. The `reason` label and the admission usage family: ADR-0051. The
read caches and their disk tier: ADR-0046, ADR-0064. Maintenance safety,
ownership, merge memory, and the at-rest scrubber: ADR-0048, ADR-0058,
ADR-0059, ADR-0065. Log POSTINGS and dynamic columns: ADR-0049, ADR-0100.
Distributed read fan-out: ADR-0071. Wire-byte accounting: ADR-0084. The metric
metadata cache: ADR-0085. Alert evaluation and its at-least-once notification
contract: ADR-0043. Per-shard ingest skew metrics and the `shard` label:
ADR-1692. The retiring-generation shard set that family also renders:
ADR-0052. The sub-floor hold counter and the flag that turns it on:
ADR-1737. The CPU gates and the tokio runtime families: ADR-1702.
