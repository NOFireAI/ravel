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
`tier`, `kind`, `outcome`, `allocator`, `stat`, `component`, `class`, and
`carrier`, seventeen in all. `reason` is shared by two
families, the admission-rejection counter and the scrub seal-divergence
counter. `cache` and `tier` split the read-cache family across its two caches
and, when a disk tier is configured, its two tiers; the [caching
guide](caching.md) documents both. `kind` splits the maintenance
merge-memory gauge into its transient and total high-water marks. `class`
splits the fragment in-flight gauge and admission-wait counter into their
`pinned` and `resolve` fragment admission classes. `carrier` splits the
declared-statistics drop tally across its four carrier labels.
`outcome` splits the alert-tick counter by how one evaluation tick ended, `allocator`
and `stat` carry the process allocator gauges, and `component` splits the
memory budget's reserved-bytes gauge by which side reserved it. The `level`
key is reserved and no family renders it.

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

The alerting family is absent unless the process built an alert evaluator,
which needs `--alert-rules-file` naming at least one rule. It renders wherever
rule evaluation runs, which is every `all` or `query` mode process configured
with rules, whether or not that process currently holds a tenant's alert lease.

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
| `ravel_ingest_shard_deaths_total` | Shard-actor deaths observed by the router. What a death means differs by signal (the `signal` label). On `signal="metrics"` the router respawns, so the counter counts every death including each respawned incarnation and can exceed the shard count; a low steady rate there is transient respawn recovery, because the respawn budget decays to zero after a shard runs a whole `max_flush_lifetime` without dying, so deaths spread that far apart never accumulate into a condemnation, and a sustained climb on one shard is a poison-pill input. On `signal="logs"` and `signal="spans"` the routers never respawn, so a death is already a condemnation: it is counted once per shard per live generation, never more, and the matching `ravel_ingest_shards_condemned_total` sample moves on the same death. Any increase on those two signals is permanent shard loss, not recovery. |
| `ravel_ingest_shards_condemned_total` | Shards condemned and no longer accepting writes, counted at most once per shard per live generation and bounded by the live generation count times the shard count, not the shard count: under resharding each generation condemns a shard index independently. The threshold differs by signal (the `signal` label): the metrics pipeline condemns a shard only after it exhausts its respawn budget within one decay window, while the logs and spans pipelines never respawn and so condemn on the first shard-actor death. Any nonzero value means the process reports `/readyz` 503, which sheds traffic but does not replace the process; it stays condemned until someone rolls it. Alert on `> 0`. |
| `ravel_ingest_partial_writes_total` | Multi-shard strict writes the router observed committing on some shards and then failing on a sibling. The client received an error that is retryable exactly when the sibling's failure was, and the durable tokens reach it only where the transport can carry them (the OTLP, remote-write, and OTAP gateways log the durable count instead); a rising figure means retries may be re-ingesting data that already committed (see the consistency model's partial multi-shard commit section). |
| `ravel_ingest_exemplars_written_total` | Exemplars stored on flushed objects. |
| `ravel_ingest_exemplars_dropped_total` | Exemplars discarded by the per-series admission cap. |
| `ravel_ingest_stale_provisioning_flushes_total` | Flushes failed closed because the router's cached shard-generation view was older than the refresh interval. A rising figure means the provisioning re-read is failing for longer than the grace window allows. |
| `ravel_ingest_grace_extended_stale_flushes_total` | Flushes routed on a last-known-good provisioning view inside the bounded grace window. A rising figure means the store is slow to serve the provisioning re-read and this router is running degraded-but-available. |
| `ravel_ingest_flushes_by_age_adaptive_total` | Flushes opened on the adaptive-delay corridor age trigger rather than the fixed max_flush_delay. A rising figure means the adaptive corridor, not the fixed delay, is driving age flushes. |
| `ravel_ingest_in_flight_flushes` | Flush tasks spawned but not yet acked, summed across shards (a gauge). A sustained high value means flushes are not keeping up with the load. |
| `ravel_ingest_flush_permit_wait_seconds_total` | Total seconds every flush has spent waiting for a `max_inflight_flushes` permit, summed across shards. Zero unless a shard is actually asked for a second concurrent flush; a rising figure means `max_inflight_flushes` is the binding window. |

The collisions family carries no `signal="spans"` series. Spans derive no
identity that can collide, so that sample is structurally absent, not zero.

The two exemplar families carry only the `signal="metrics"` series. Exemplars
ride on metric points, so those samples are structurally absent for logs and
spans, not zero.

The `ravel_ingest_flushes_by_age_adaptive_total` family likewise carries only
the `signal="metrics"` series: the adaptive-delay corridor is a
metrics-pipeline feature, so that sample is structurally absent for logs and
spans, not zero. `ravel_ingest_in_flight_flushes`,
`ravel_ingest_flush_permit_wait_seconds_total`, and
`ravel_ingest_grace_extended_stale_flushes_total` are carried for every
signal, each for its own reason: the in-flight gauge because all three shard
actors arm an `InFlightFlushGuard`; the permit-wait counter because the
`max_inflight_flushes` acquire runs off-actor for all three ingest pipelines;
the grace-extended counter because all three snapshots already expose the
stale-provisioning counter it pairs with. A logs- or spans-only process
therefore still renders a real (possibly zero) sample for all three.

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

### Catalog fold liveness (`ravel_catalog_fold_*`)

Labels: `mode`, `signal`. All three families render in every mode, with one
series per folded signal (`metrics`, `logs`, `spans`), but only the processes
that actually fold ever move them. Two things stop the background fold loop:
the `maintain` mode, which never spawns it, and `--disable-fold`, which
returns no fold tasks in any mode. The on-demand fold route is mounted only
in `all` and `query`. A `maintain` process, and any process run with
`--disable-fold`, therefore reports zeros permanently.

The `ravel_catalog_fold_stamped_*` pair shares this prefix and is not part of
this family. It is stamp coverage, documented under declared-column
statistics below, and unlike the three liveness families it is omitted rather
than rendered as zeros on any process that can fold by neither route -- a
`maintain` process, and a `gateway` process run with `--disable-fold`.

The `signal` label is the family's per-signal keying, not a convenience. The
fold runs as one independent task per signal, each with its own loop and no
supervisor, so one signal's fold can stop while the other two keep running.
Process-global families read as healthy throughout that, because the two
surviving loops keep the shared figures fresh: the span history stops sealing,
the unsealed span grows, and nothing moves. One series per signal removes that
blind spot, and the cardinality is three values per process, the same closed
set `signal` already carries on the ingest and postings families.

| Metric | Meaning |
|---|---|
| `ravel_catalog_fold_cycles_total` | Catalog folds of this signal that completed successfully, no-op folds included. |
| `ravel_catalog_fold_failures_total` | Catalog folds of this signal that failed. The fold retries on the next tick and never fails a query directly. |
| `ravel_catalog_fold_last_success_timestamp_seconds` | Gauge. Unix time of the last successful fold of this signal in this process, `0` if none has succeeded since it started. |

A no-op fold counts as a cycle and advances the gauge. That is deliberate: a
fold seals an ingest hour only once `max_flush_lifetime +
clock_skew_allowance + fold_safety_margin` has elapsed past the end of that
hour, so on a quiet tenant almost every cycle legitimately publishes nothing.
A counter that moved only on a published snapshot would read as a stopped
fold on exactly the tenants where nothing is wrong.

The gauge is the family's point. The two counters move when the fold runs;
only the gauge's age moves when the fold stops, and a stopped fold is the
self-worsening failure here. The unsealed span grows for as long as nothing
seals it, and a cold recent-window query over a wide enough unsealed span
eventually exceeds its per-query object-store request budget and is refused
outright. Until this gauge existed the first operator-visible symptom of a
stopped fold was that refusal, because the fold reported its failures through
logs alone.

#### The fold-stalled alert

This group, and every other rule block in this guide, ships as
[`deploy/prometheus/ravel.rules.yaml`](../../deploy/prometheus/ravel.rules.yaml)
together with the conditions the
[troubleshooting guide](operations/troubleshooting.md) states. Load that file
from Prometheus's `rule_files:` key rather than transcribing the block below:
a `ravel-server` integration test asserts that every metric the shipped file
names is one a running server renders, and a copy pasted out of this page is
a copy nothing updates when the deployment does. The block is kept here
because its comments explain the rule.

That test reads this page too. Every rule printed in a `yaml` block below is
compared against the rule of the same name in the shipped file, expression,
`for:` duration and severity alike, so a block here cannot drift into a
condition the shipped file no longer carries. It does not compare the
`annotations:` of a block, so a metric named only in a `description:` on this
page is checked against nothing; the shipped file's own annotations are.

```yaml
groups:
  - name: ravel-catalog-fold
    # RavelCatalogFoldStalled fires on any deployment where some signal has
    # no fresh fold, which includes a fleet that never folds at all: an
    # intentionally maintain-only fleet, or one running --disable-fold
    # everywhere. Such a fleet must drop this rule or inhibit it; the state
    # walkthrough below explains why that opt-out is deliberate.
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

`max by (signal)`, not a bare `max()` and not a per-instance comparison. The
grouping and the aggregation answer two different questions.

The aggregation is fleet-wide because the fold loop skips its tick entirely
when `HEAD` is already fresher than `fold_interval`. A replica whose peers are
folding on schedule correctly does no folding of its own, and its own gauge is
correctly stale; the fleet-wide maximum is the figure that answers "is this
catalog being folded."

The grouping is by `signal` because there is no such thing as "the fold" to be
alive or dead. There are three independent fold loops per process, one per
signal, and one replica's loop going idle is the healthy case described
above, not a failure: the fleet stays covered for that signal as long as one
peer still folds and stamps it. A single process's loop is a point of
failure for a signal's sealed history only in a single-folding-replica
deployment, or fleet-wide when a signal-specific fault kills that loop on
every replica at once -- and that fleet-wide case is what the grouping
exists to catch. An ungrouped `max()` collapses all three signals into one
number that two healthy loops keep fresh while the third is dead fleet-wide,
which is the same hides-a-dead-component shape as reading a process-global
gauge across a split-role fleet. Grouping by `signal` produces one sample per
signal, and any one of them crossing the threshold fires with `signal` on the
alert, so the page names which history has stopped sealing.

The `or absent(...)` branch covers the outage the staleness comparison alone
cannot see. `max by (signal)` of an empty instant vector is empty, and
`time() - <empty>` is empty, so when the family is not being scraped at all the
first operand produces no sample and a rule of only that operand stays silent
through the exact outage it exists to catch: a fleet scaled to zero, a fleet
crash-looping fast enough that its targets go stale, or a scrape-config edit
that drops the job. `absent()` returns `1` precisely when its argument matches
no series, so it fires on that absence. That branch carries no `signal` label,
because there is no series to take one from; an alert from it renders an empty
`{{ $labels.signal }}` and means the whole family stopped arriving, not that
one signal stalled.

Neither operand filters `mode`. Earlier revisions of this rule carried
`mode!="maintain"` on both, and it is behaviour-neutral here: `render_catalog_family`
runs in every mode, so a co-scraped `maintain` process contributes a permanent
`0` to each signal's group, and `0` can never win a `max()` against any live
gauge. Where the filter used to matter was a folding fleet that died beside a
surviving `maintain` node, and the unfiltered form covers that state too, just
through the other operand: the `0` is the only sample left in each group, so
`time() - 0` clears any threshold and the staleness operand fires where the
filtered form needed `absent()` to. Every state below is identical under both
forms, so the rule carries the simpler expression. The filter also cannot be
what makes an intentionally non-folding fleet quiet: such a fleet pages under
both forms, for the reason in the opt-out paragraph below.

The states, of the observed system rather than of the expression:

| What the fleet is doing | Series at the scrape | Staleness operand | `absent()` operand | Alert |
|---|---|---|---|---|
| Nothing scraped at all | none | empty | fires | **fires** |
| Only `maintain` nodes scraped, intentionally | 3, all `mode="maintain"` at `0` | `time() - 0` over threshold for all 3 signals | silent | **fires** (opt out) |
| Healthy folding fleet | 3 per process, all fresh | under threshold for all 3 signals | silent | silent |
| Folding fleet scraped, every fold loop stalled | 3 per process, all stale | over threshold for all 3 signals | silent | **fires** |
| Folding fleet dead, co-scraped `maintain` alive | 3, all `mode="maintain"` at `0` | `time() - 0` over threshold for all 3 signals | silent | **fires** |
| One signal's loop dead, other two healthy | 3 per process; 2 fresh, 1 stale | over threshold for that one signal | silent | **fires** for that signal |
| `--disable-fold` on every non-`maintain` process | 3 per process, all at `0` | `time() - 0` over threshold for all 3 signals | silent | **fires** (opt out) |
| Fleet whose tenants write only one signal | 3 per process, all fresh | under threshold for all 3 signals | silent | silent |

The last row is the one that would be a false page if the gauge tracked
published snapshots rather than fold cycles. Every loop folds every discovered
tenant for its own signal every tick; a fold over a signal a tenant never
writes is a healthy no-op cycle and stamps the gauge like any other. A fleet
ingesting only logs still has all three gauges fresh.

Do any two rows produce identical telemetry while meaning different things?
Yes, two pairs, and both are deliberate:

- "Only `maintain` nodes scraped, intentionally" and "folding fleet dead,
  co-scraped `maintain` alive" are byte-for-byte identical at the scrape: three
  `mode="maintain"` series at `0` and nothing else. No arrangement of these
  operands can tell an intended topology from a fleet-wide death, because the
  dead processes' series are gone and absence carries no intent.
- "`--disable-fold` everywhere" and "every fold loop crashed before its first
  success" are likewise identical: every gauge at its `0` sentinel under a full
  set of non-`maintain` series.

Both pairs resolve the same way, and the rule fires loud on all four. A fleet
that never folds -- maintain-only, or `--disable-fold` everywhere -- must opt
out by dropping `RavelCatalogFoldStalled` or inhibiting it (the group comment
on the rule marks this). `--disable-fold` is documented elsewhere as a pure
query-cost optimization, so an operator who sets it deliberately should expect
this rule to page about ten minutes after start and should silence it as part
of setting the flag, not treat the page as a false positive: the unsealed span
really does grow without a fold to seal it, and that is what the rule reports.
Firing on a real outage and forcing one deliberate silencing on a fleet that
never folds is safer than staying silent on the outage to spare that fleet the
page: an alert that is silent in the case it exists for manufactures
confidence.

The threshold is the unsealed span the catalog configuration implies, in
seconds:

| Term | Default | Seconds |
|---|---|---|
| `max_flush_lifetime` | 1 hour | 3600 |
| `clock_skew_allowance` | 5 minutes | 300 |
| `fold_safety_margin` | 15 minutes | 900 |
| **Sum** | **1 h 20 min** | **4800** |

3600 + 300 + 900 = 4800 seconds is the span behind the present that a fold is
never allowed to seal, however healthy it is. A fold that has not succeeded
for longer than that has left more history unsealed than the configuration
ever intends to be unsealed: below the threshold the growth is inside a
window the deployment has already accepted as un-indexed, above it every
further second is history that should have been sealed and was not. Raise or
lower the threshold with those three settings, not independently of them.

The headroom that keeps it quiet: `fold_interval` defaults to 5 minutes and the
loop adds up to 10% jitter, so the sleep between two cycles is at most 330
seconds. That is the sleep ceiling, not the gauge-age ceiling. The stamp is the
reading taken before each tenant's fold and the loop re-stamps per tenant
within a cycle, so the widest healthy gap runs from the last tenant's stamp in
one cycle to the first tenant's stamp in the next: 330 s, plus the
tenant-discovery LIST that opens the cycle, plus that first tenant's fold
duration, which on a large tenant is tens of seconds. 4800 is about 14 missed
ticks of margin over the sleep ceiling, which no single slow cycle, restart, or
rolling deploy reaches. Use the fuller form, not the 330, if you tighten the
threshold on a fleet with a shorter `fold_interval`: the discovery-plus-one-fold
term stops being noise once the sleep shrinks toward it.

`for: 10m` covers process start rather than the stall itself. The gauge reads
`0` until the first fold succeeds, which makes `time() - 0` exceed any
threshold instantly, and the first scheduled fold lands one `fold_interval`
plus jitter after start. Ten minutes is a little under two intervals of
grace. Keeping the `0` rather than omitting the series is what lets the one
expression cover both a fold that stopped and a fold that never worked at
all.

Two limits to know before relying on it. The gauge is per signal but not per
tenant: one signal's series goes stale when that signal's fold stops, and stays
fresh when one tenant's fold is stuck behind a permanent fault while every
other tenant of that signal folds normally. `signal` is a closed set of three
values and is labelled here for exactly that reason; `tenant` is not, and
tenant-labelled fold series would carry cardinality that grows with the tenant
count, so a stuck single tenant is found through
`ravel_catalog_fold_failures_total` and the fold task's per-tenant logs
instead. And a deployment that has discovered no tenants at all folds nothing
and so trips this rule; scope the group to deployments that serve traffic.

### Declared-column statistics (`ravel_declared_stats_drops_observed_total`, `ravel_catalog_fold_stamped_*`)

Three families cover the per-declared-column min/max stamps: one defect
tally on the read side, and one coverage pair at the fold.

| Metric | Labels | Meaning |
|---|---|---|
| `ravel_declared_stats_drops_observed_total` | `mode`, `carrier` | Declared-column statistics entries a reader dropped as defective, by the carrier it was reading. |
| `ravel_catalog_fold_stamped_records_total` | `mode` | Carriers of declared-column statistics that the fold read: L0 commit records and L1 compaction parts. |
| `ravel_catalog_fold_stamped_entries_total` | `mode` | Snapshot entries the fold built carrying declared-column statistics, from either carrier. |

`carrier` is a closed set of four: `commit-record` and `compaction-part` are
the two stamp carriers, `snapshot-entry` is the fold's copy of them, and
`cstat` is the `.cstat` object's own `ColumnStat` entries, whose reader lives
in `ravel-sql`.

`snapshot-entry` renders in every exposition and no shipped code path
increments it. The catalog re-validates a snapshot entry by converting it to
its commit-record twin and reading it through the commit-record reader, so a
drop on the fold's copy is observed under `commit-record` along with the
fold's own reads of commit records. Read a flat `snapshot-entry` series as
"not reported separately", never as "the fold's copies are clean", and do not
build a remediation step on it.

The drop tally counts OBSERVATIONS, not distinct defects: one
defective record read by a thousand queries counts a thousand times, because
deduplicating per entry would need unbounded state keyed by (object, column)
on a path walked once per segment per query. So compare its rate across equal
windows and never its magnitude across windows of different query volume. The
[query engine guide](../query-engine.md) states the same semantics next to the
predicate the drops come from.

This family renders in every mode, including `maintain`. Its four carriers are
read by four different subsystems, and `compaction-part` is observed by the
compaction that only `maintain` runs, so gating the family on folding would
hide a defect signal in exactly the mode that produces it.

The two fold families are the coverage pair, and they answer a different
question from the drop tally: not "is a stamp defective" but "is a stamp that
was written reaching the snapshot at all". `..._stamped_records_total` counts
the carriers the fold read that carried statistics, which is both of the paths
a fold builds a stamped entry from: L0 commit records (field 20) and L1
compaction parts (field 12). `..._stamped_entries_total` counts snapshot
entries the fold then built carrying statistics, from either path, counted as
each entry is built. Rewrite
output parts are counted on neither side: a rewrite drops rows, so the fold
never carries a stamp computed before the drop, and counting a carrier that can
never carry would report a permanent shortfall. Both are process-wide totals
accumulated by the fold as each fold commits its `HEAD`, so a fold attempt
that lost its compare-and-swap and retried contributes nothing. In the healthy
state the two rise together, one entry per stamped carrier.

Unlike the drop tally, the coverage pair renders only when a fold can run in
this process at all, by either route. Two routes fold, and both accumulate
these totals: the background fold task, which `maintain` never spawns and
which `--disable-fold` disables in every other mode, and the on-demand
`POST /api/v1/admin/fold` route, mounted in `all` and `query` whatever
`--disable-fold` says. So a `--mode all --disable-fold` process DOES render
the pair: an operator can still fold it by hand, and that fold moves these
counters. Both series are absent, not zero, exactly where neither route
exists: every `maintain` process, and a `gateway` process run with
`--disable-fold`. An operator scraping one of those sees
`ravel_declared_stats_drops_observed_total` present as usual and both
`ravel_catalog_fold_stamped_*` series missing from the exposition.

#### The stamp-coverage shortfall alert

Shipped as the `ravel-declared-stats` group in
[`deploy/prometheus/ravel.rules.yaml`](../../deploy/prometheus/ravel.rules.yaml);
load that file rather than copying the block.

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

Why two rules rather than one. An old fold cannot emit a counter it does not
have, so the detectable signal is a divergence between the two counters, or
the fold-side family being absent while the ingest side rises. The first rule
covers the divergence; the second covers the absence, and it needs the ingest
side as its second term, because an absent fold family on its own is also what
a `maintain`-only deployment, a deployment that can fold by neither route, or
an idle deployment looks like.

Neither rule watches a mixed-version fleet past its first upgraded process.
The families render on any `all` or `query` process of a new enough build,
folding or not, so one such process makes `absent()` false, and the shortfall
rule then compares that process's own healthy, equal contribution while the
old folders it is rolling alongside emit nothing. For the remainder of a
rollout the per-pass figures in the fold report, read against each folding
process, are what says whether that process carries stamps; a fleet-wide sum
cannot attribute a zero contribution to an old folder.

`sum()` over both sides of the shortfall rule, not a per-instance comparison.
The fold loop skips its tick when `HEAD` is already fresher than
`fold_interval`, so a replica whose peers folded correctly reads zero on both
counters and a per-instance comparison would compare two zeros forever; the
fleet-wide sums are what "is this catalog carrying its stamps" is a question
about. `increase(...[1h])` rather than the raw totals because both are
monotonic process-lifetime counters: a process that carried a shortfall once
and has been healthy since keeps the raw gap forever, and only a windowed rate
distinguishes a live shortfall from a scar.

`for: 5m` is a hold against a single anomalous sample, and against nothing
else. A fold pass publishes both halves at the moment it commits its `HEAD`,
as two adjacent increments inside one call, so the totals move together:
there is no state where a pass's carriers are counted for minutes before its
entries are, and no benign reading in which a divergence is waiting to close
on its own. What the hold covers is a scrape that interleaves with those two
increments. The fold adds records then entries, and the renderer loads
records then entries, and both orders can be caught mid-flight: if both loads
land between the two adds the exposition carries the pass's records without
its entries, and if the pass completes between the two loads it carries the
entries without the records. So a single scrape can show either total ahead
of the other, and neither direction is by itself evidence of anything. Such a
sample skews the windowed increase for as long as it is the newest point in
the window, which is one scrape interval, and a few intervals of agreement
rule it out. Past the hold, the gap is a carrier every one of whose
stamps was dropped, so treat a shortfall that survives the hold as live and
work it through the drop tally; it will not clear by waiting.

The pair cannot detect a stamp that was never written. Both counters live on
the fold, so an ingest pipeline that stops stamping entirely drives both to
zero together, which reads identically to an idle tenant. The drop tally does
not cover that case either: it counts stamps a reader rejected, so a stamp
never written moves nothing at all, and a flat `carrier="commit-record"` line
is not evidence that stamping is healthy.

What distinguishes the two is a signal that rises when data arrives: an
ingest-side rate that is moving while both fold counters stay at zero means
the fold is reading records that carry no stamps. That comparison is what to
alert on for a rollout, and neither of these two counters can make it alone.

That comparison covers the ingest half of a rollout only. Compaction is the
other writer of a stamp, and a compactor of an older shape, sealing
compaction parts with an empty statistics list, is invisible to every series
in this section. The fold counts a carrier only when its list is non-empty,
so an empty one increments neither half, and the drop tally counts entries a
reader rejected, of which an empty list has none. The stamped L0 records that
compactor consumed disappear from the pair too: once a compaction record wins,
the fold excludes its inputs and skips them before the tally, so the coverage
those hours had is not preserved anywhere. The pair sees only the carriage of
stamps that reached the fold already stamped; a carrier that arrived unstamped,
whether ingest never stamped it or a compactor dropped it, is invisible on both
halves. A fleet whose ingest and
fold are upgraded while one compactor is not keeps both fold counters rising
at the L0 rate, in step, ratio at one, while every L1 segment that compactor
seals is uncovered for every declared-column statistic and every query over
it gives up the shortcut.

Two things see that, and neither is in the exposition. The first is the fold
report, which carries the same two numbers per pass instead of summed over
the process: `ravel-cli catalog fold` prints `stamped_records` and
`stamped_entries` for the pass it just ran, so a fold of one tenant whose
recent hours have just been compacted reports what those parts contributed as
carriers, and a pass that folds compaction output and counts no carriers for
it is the signal a fleet-wide sum hides. The second is rollout order, which
is what actually settles it: compaction runs only in `maintain`, a `maintain`
process renders neither fold counter, and no counter on any process reports
which shape a compactor writes. Upgrade every `maintain` process before
reading the pair as an answer about the whole fleet.

### Tenancy adoption (`ravel_tenancy_v1_unkeyed_adoptions_total`)

Labels: `mode`. Counts buckets this process pinned to the unkeyed tenant hash
when it adopted a bucket that held `t/` data but no `sys/tenancy` marker. A
nonzero value is the visible signal that the one-time migration happened.

### Provisioning (`ravel_provisioning_shard_count_mismatch_total`, `ravel_provisioning_shard_count_drift_total`)

Labels: `mode`.

| Metric | Meaning |
|---|---|
| `ravel_provisioning_shard_count_mismatch_total` | Provisioning checks that failed hard: an unreadable record, a decodable record whose generation history fails structural validation, or pre-ADR data a lower `shard_count` would hide. Alert on any increase; [Troubleshooting](operations/troubleshooting.md) gives its rule. |
| `ravel_provisioning_shard_count_drift_total` | Validations where a decodable record with a structurally valid generation history had a recorded `shard_count` that differed from the live `--shards` default (a record that fails structural validation is counted by the mismatch counter above, never here). The drift is tolerated and routing uses the record's own generation history, so this is informational: a nonzero value is expected after lowering the global default, not a fault. |

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

### Maintenance safety (`ravel_maintain_legal_hold_*`, `ravel_maintain_conservation_*`, `ravel_maintain_orphan*`, `ravel_maintain_l0_records_pending`, `ravel_maintain_objects_deleted_total`)

Labels: `mode`, plus `signal` on every series except the legal-hold counter
(`mode` only) and `ravel_maintain_objects_deleted_total`, which carries `mode`
and `kind` and no `signal`. These carry no `tenant_hash` label.

| Metric | Meaning |
|---|---|
| `ravel_maintain_legal_hold_refresh_failures_total` | Legal-hold refresh failures. Each one skips that tenant's whole maintenance tick. |
| `ravel_maintain_l0_records_pending` | Gauge. L0 commit records sitting below `min_compaction_inputs` in a sealed bucket, by signal, summed over every tenant and shard this process maintains. |
| `ravel_maintain_objects_deleted_total` | Objects the sweep physically deleted, by `kind`: `superseded_records_deleted`, `superseded_data_deleted`, `unreferenced_parts_deleted`, `quarantine_reaped`. |
| `ravel_maintain_conservation_aborts_total` | Compaction publishes aborted by the record-count conservation gate, by signal. |
| `ravel_maintain_orphan_breaker_tripped_total` | Orphan-GC mass-orphan circuit breaker trips, by signal. |
| `ravel_maintain_orphans_withheld` | Gauge. Orphan candidates withheld by the last completed orphan pass, by signal. |
| `ravel_maintain_orphans_present` | Gauge. Orphan candidates the last completed orphan pass found, by signal, whether or not the breaker tripped. |
| `ravel_maintain_orphans_quarantined_total` | Orphan candidates moved from the live L0 set to the quarantine prefix, by signal. |
| `ravel_maintain_orphans_quarantine_refused_total` | Orphan candidates whose copy to the quarantine prefix failed, by signal; the live object was left in place rather than deleted without a copy. |
| `ravel_maintain_quarantine_reaped_total` | Objects physically deleted from the quarantine prefix past the quarantine horizon, by signal. |

[Troubleshooting](operations/troubleshooting.md) gives the alert rules and the
breaker runbook. A zero
value on the `orphans_withheld` or `orphans_present` gauge does not mean a
prior trip was resolved: it is this pass's count, not a resolution signal.
Both gauges are refreshed only by a sweep that ran the orphan rule, which is
the full-sweep cadence (`interior_reverify_ns`, default 6 h) rather than the
maintain tick (default 300 s); the ticks in between skip that rule and leave
both gauges at the last completed pass's values.

The three quarantine series are counters, not gauges: each counts what a sweep
pass did, and a later quiet pass does not undo it. Read them together. A
`ravel_maintain_orphans_quarantined_total` that climbs while
`ravel_maintain_quarantine_reaped_total` stays flat is a quarantine prefix
filling and never being reclaimed. Any increase in
`ravel_maintain_orphans_quarantine_refused_total` means quarantine cannot make
progress at all, from a store fault or a permissions or capacity problem on
that prefix, and the candidates it counts are still live: the copy is taken
before the delete, so a refused copy leaves the object in place rather than
deleting it uncopied. Alert on `increase(...) > 0` there, the same shape as the
breaker-trip counter, because the next pass retries the same candidate and
refuses again.

`ravel_maintain_l0_records_pending` is a per-process total, not a per-bucket or
per-tenant one: one maintenance cycle (default 300 s) sums every sealed bucket
of every `(tenant, shard)` this process currently owns, and publishes the
result once the cycle has covered all of them. A scrape that lands mid-cycle
reads the previous cycle's complete total, never a partial sum.

A unit whose pass failed contributes nothing for that cycle, so a dip can mean
either that pending work really fell or that a unit was not reached, and
`ravel_maintain_units_stalled` does not separate the two on its own. It only
moves for a per-unit failure that has repeated past the stall threshold
(three consecutive ticks on the defaults), and several paths drop a unit's
whole contribution before any per-unit accounting happens at all: a tenant
whose legal-hold refresh fails is skipped for the entire tick
(`ravel_maintain_legal_hold_refresh_failures_total` moves, `units_stalled`
does not), and so is one skipped by the provisioning or shard-generation
check (`ravel_provisioning_shard_count_mismatch_total`). A one- or two-cycle
per-unit failure dips this gauge with `units_stalled` still at zero. Read a
dip against those counters and against the age of
`ravel_maintain_last_cycle_completed_timestamp_seconds` before concluding
compaction caught up. To get a deployment-wide
figure, sum the gauge across processes: with several maintain replicas each
owns a disjoint share of the units, so no replica's value is the whole
population and the shares do not overlap.

Unlike the orphan gauges above, this one does not follow the full-sweep
cadence. A below-threshold bucket in the interior zone is skipped on the ticks
between re-verifies (`interior_reverify_ns`, default 6 h), but the memo carries
its last-known L0 record count and the skipped bucket still contributes it, so
every cycle publishes the whole pending population rather than only what that
cycle re-read. The count a skipped bucket contributes is as old as its last
re-verify, so a bucket that crossed the threshold since then is reflected only
once its re-verify or its compaction runs.

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

### Maintenance loop liveness (`ravel_maintain_last_cycle_completed_timestamp_seconds`, `ravel_maintain_loop_panics_total`)

Labels: `mode`. Both series are process-wide, with no `tenant_hash` dimension.

| Metric | Meaning |
|---|---|
| `ravel_maintain_last_cycle_completed_timestamp_seconds` | Gauge. Unix time the maintenance loop last completed a cycle in this process, `0` if none has completed since it started. Its age is the maintain-liveness signal. |
| `ravel_maintain_loop_panics_total` | Panics caught in the loop body and restarted by the supervisor. An `increase()` here is the loop crash-looping. |

This is the maintenance analogue of the [catalog fold liveness
gauge](#catalog-fold-liveness-ravel_catalog_fold_) above, and it exists for the
same reason. Every other maintenance figure -- `ravel_maintain_tenants_maintained`,
`ravel_maintain_units_stalled`, the safety gauges -- is written at the end of a
cycle that completed, so if the single spawned supervisor task dies (a panic
anywhere in the discovery or sweep call graph), they all freeze at their last
healthy values: `tenants_maintained` still equals `tenants_discovered`,
`units_stalled` still reads `0`, and the pod stays Running and Ready. Retention
deletes nothing, compaction stops, and the sweeper reclaims nothing, but nothing
on `/metrics` moves to say so until a recent-window query is refused days later.

The gauge is the family's point: only its age moves when the loop stops. The
loop now runs under a supervisor that catches a panicking cycle, counts it on
`ravel_maintain_loop_panics_total`, and restarts the loop after a bounded
backoff, so a single transient panic self-heals and the next completed cycle
re-stamps the gauge. A rising panic counter with a stalling gauge is a loop
that cannot make progress between crashes.

The panic counter needs its own rule, because a crash loop does not always
stall the gauge. An attempt that completes a cycle and then panics re-stamps
the gauge on that cycle, and the supervisor resets the backoff to its initial
value whenever the dead attempt completed at least one cycle. A loop that
crashes on every attempt after one cycle therefore keeps the gauge fresh, and
keeps the full-sweep counter moving when the panic lands after the sweep.
Neither of the two rules above fires, and the panic counter is the only signal
that moves.

#### The maintenance-stalled alert

Shipped as the `ravel-maintain-liveness` group in
[`deploy/prometheus/ravel.rules.yaml`](../../deploy/prometheus/ravel.rules.yaml);
load that file rather than copying the block.

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
        # The `ravel_maintain_units_owned` term is what keeps that healthy
        # case quiet, and it is why this rule is not a bare counter check.
        expr: |
          increase(ravel_maintain_full_sweep_passes_total[3h]) == 0
          and
          ravel_maintain_units_owned > 0
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

The staleness threshold is `1800s` (30 minutes, six default 5m maintain
intervals) rather than the fold rule's `4800s`: the fold's is sized to the
unsealed-span budget it protects, whereas here any lapse of a few cycles means
retention and GC have stopped, so the bar is lower. The `or absent(...)` branch
covers the same total-outage case the fold rule's does -- a maintain mode
scaled to zero, crash-looping fast enough to go stale, or dropped from the
scrape config -- which the staleness comparison alone cannot see because
`time() - <empty>` is itself empty.

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

### Alert evaluation (`ravel_alert_*`)

Labels: `mode`, plus `outcome` on the tick counter. Every series here is
process-wide, with no `tenant_hash` dimension: one process runs one evaluator
per tenant that has rules, and each figure is the sum across them.

The whole family is absent unless this process built at least one alert
evaluator, which means `--alert-rules-file` was given and the file named at
least one rule. A deployment that configured no alerting exports none of these
series rather than a row of permanent zeros, and that absence is what keeps the
alert rules below quiet there.

| Metric | Meaning |
|---|---|
| `ravel_alert_rules_evaluated_total` | Alert rules whose query ran and whose condition was decided. |
| `ravel_alert_rules_failed_total` | Alert rules skipped because the query, the condition, or the write failed. Every one is logged; the rule is retried next tick. |
| `ravel_alert_records_written_total` | Alert transition records durably written. |
| `ravel_alert_repeats_queued_total` | Repeat notifications queued for a still-firing alert. A repeat writes no new record, so it advances this and then the delivery counter, never `ravel_alert_records_written_total`. |
| `ravel_alert_notifications_delivered_total` | Notifications delivered to every configured sink, including ones carried over from an earlier tick's failure. |
| `ravel_alert_notifications_failed_total` | Notifications still undelivered after a tick's attempt, counted once per tick per notification, so one stuck notification keeps advancing it while it is retried. |
| `ravel_alert_ticks_total` | Evaluation ticks by `outcome`. |
| `ravel_alert_last_tick_completed_timestamp_seconds` | Gauge. Unix time the alert loop last completed a tick in this process, `0` if none has completed since it started. Its age is the alert-loop liveness signal. |

The `outcome` label carries one of four values, exactly one per tick:

| `outcome` | Meaning |
|---|---|
| `evaluated` | This replica held the tenant's alert lease and evaluated every rule. |
| `lease_not_held` | A peer replica held the lease, so this one skipped evaluation. Healthy, and the steady state of every replica that is not the holder. |
| `lease_unavailable` | The lease read or write failed against object storage. Evaluation was skipped and is retried next tick. |
| `history_unavailable` | The tenant's alert history could not be read, so nothing was evaluated. The evaluator never acts on a partial history, because that would re-fire an alert that is already firing. |

`lease_not_held` is deliberately its own outcome and not part of any failure
count. In a multi-replica deployment every replica but one reports it on every
tick, forever; a rule that sums it with the two failure outcomes pages on the
expected steady state.

The liveness gauge is this family's point, for the same reason the maintenance
one is. Every counter above is cumulative, so an evaluator that dies leaves
them all frozen, and a frozen `ravel_alert_rules_failed_total` looks exactly
like a healthy pipeline whose rules never fail. Only the gauge's age moves when
the loop stops. A tick that ended in `lease_not_held` stamps the gauge: a
standby replica is alive and evaluating nothing by design.

#### The alerting-pipeline alerts

Shipped as the `ravel-alerting-pipeline` group in
[`deploy/prometheus/ravel.rules.yaml`](../../deploy/prometheus/ravel.rules.yaml);
load that file rather than copying the block.

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
        # stays flat. The second term is what keeps a partial failure (one
        # notification stuck behind a bad URL while the rest get through) out of
        # this critical rule; it belongs to RavelAlertRuleEvaluationFailing's
        # quieter class.
        #
        # Quiet on a healthy deployment with no rules configured: the family is
        # absent, so both terms are empty. Quiet on one whose rules simply never
        # fire: nothing is ever queued, so the failure counter never increases
        # and the first term is false.
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
            webhooks. Check the sink URLs and credentials, and the evaluator
            logs for the per-sink delivery error.
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
is five times shorter (60s against the 300s maintain default): ten missed
ticks here is the same evidence six missed cycles is there.

### At-rest scrubber (`ravel_scrub_*`)

Labels: `mode` and `signal`, plus `level` on the checksum-mismatch counter and
`reason` on the seal-divergence counter. These carry no `tenant_hash` label.

| Metric | Meaning |
|---|---|
| `ravel_scrub_checksum_mismatch_total` | Data objects that failed at-rest integrity re-verification (a whole-object blake3 mismatch or a footer or section crc failure), by signal and level. |
| `ravel_scrub_postings_disagreement_total` | Objects whose covering name-postings object omitted a `__name__` the object really carries (a false negative), by signal. |
| `ravel_scrub_seal_divergence_total` | Divergences between the folded snapshot and the re-listed sealed commit history, by signal and reason. |
| `ravel_scrub_cursor_position` | Gauge. Fraction of the current scrub rotation the content-tier cursor has covered so far, by signal, in [0,1]. |

`ravel_scrub_checksum_mismatch_total` is the one to alert on for any increase:
Ravel keeps no redundant copy to repair a corrupt object from, so a nonzero
increase is corruption an operator must investigate (with the one `level="l0"`
caveat below). Its `level` label says which part of the commit lineage the
corrupt object came from: `l0` an original ingested segment, `l1` a compaction
output part, or `rewrite` a selective-erasure rewrite output part. The scrub
corpus now covers all three: a compaction or rewrite output part that a record
still lists as live joins the same rotation an L0 segment does.

The lineage filter applies to those output parts only. A compaction or rewrite
record that a later rewrite record names in `superseded_record_key`, and one in
a bucket a retention tombstone has retired, are both left out, because their
parts survive until a sweep retires them and no query reads them. L0 commit
records are scrubbed whatever their lineage: there is no supersession or
tombstone check on that path, so an L0 segment a live compaction has already
folded into a compaction output part stays in the rotation. A `level="l0"`
mismatch on an already-compacted hour may therefore name a redundant copy
rather than data a query can still reach, because the catalog puts a live
compaction record's input identities into its query-time excluded set. Check
whether the hour is compacted before treating an `l0` mismatch as
unrecoverable. The `reason` label on
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
| `ravel_cache_disk_entries_expired_max_age_total` | Disk-tier entries dropped for aging past the per-entry max-age, across the hit check, the startup scan, and the periodic background sweep. An age-based expiry, separate from the capacity-driven eviction counter above. |

The request hit rate is `hits / (hits + misses)`. The byte hit rate is
`bytes_served / (bytes_served + bytes_admitted)`. The renderer leaves both
ratios for PromQL to compute, per `cache` and per `tier`.

### Admission (`ravel_admission_*`, `ravel_ingest_wire_bytes_total`)

Labels: `mode`, `tenant_hash`, `signal`, plus `reason` on the rejection
counter. This family folds tenants per the rule above. The
[admission limits guide](admission-limits.md) covers this family in
operational depth.

| Metric | Meaning |
|---|---|
| `ravel_admission_active_series` | Gauge. Active series (metrics) or streams (logs) tracked for the active cap, by tenant and signal. |
| `ravel_admission_admitted_total` | Requests admitted past the ingest byte-rate layer, by tenant and signal. |
| `ravel_admission_admitted_bytes_total` | Charged (decompressed) bytes admitted past the ingest byte-rate layer, by tenant and signal. For a gzip OTLP request this is the decompressed size; for an uncompressed request it equals the wire size. |
| `ravel_ingest_wire_bytes_total` | Wire (on-the-wire, compressed when the client compressed) OTLP request-body bytes admitted, by tenant and signal. |
| `ravel_admission_rejected_total` | Admission rejections, by tenant, signal, and reason. |
| `ravel_ingest_body_conversions_total` | Log records whose structured (array or map) body was converted to canonical JSON text at normalization, by tenant and signal. Not a rejection, and not a count of stored records: see "Neither rule alerts on" below. |
| `ravel_admission_reconciliation_failures_total` | Fleet-admission reconciliation cycles whose sibling-snapshot read (LIST or GET) failed, by tenant and signal; the last-known soft threshold stays in force. |

Four more series report the reconciliation cycle itself. They carry `mode`
alone, with no `tenant_hash` or `signal`: one cycle reconciles every tenant the
process tracks, so there is no per-tenant figure to label.

| Metric | Meaning |
|---|---|
| `ravel_admission_reconciliation_cycle_duration_seconds` | Gauge. Duration of the last completed reconciliation cycle. |
| `ravel_admission_reconciliation_siblings_observed` | Gauge. Distinct non-stale sibling processes the last cycle saw, the live fleet size this process reconciled against. |
| `ravel_admission_reconciliation_stale_keys_skipped` | Gauge. Snapshot keys the last cycle skipped reading because the listing already showed them past the staleness window. |
| `ravel_admission_reconciliation_keys_reaped_total` | Snapshot keys past the reap horizon deleted by reconciliation cycles since process start. |

These four move before anything else does when reconciliation degrades, and
none of them shows up as a failure: the listings and reads all succeed. A cycle
whose duration approaches twice the reconciliation interval ages every sibling
snapshot past the staleness window before it is read, at which point each
process reads the fleet as empty and starts enforcing the whole tenant cap
alone. Alert on the duration against your configured interval, and on
`siblings_observed` falling to zero while replicas are up. Scope both alerts
to `mode="all"` and `mode="gateway"`: the four series render on every
replica, but only those two modes run the reconciliation loop, so a `query`
or `maintain` replica reports `siblings_observed` as zero for its whole
life. Growth in
`stale_keys_skipped` while `siblings_observed` is flat means the control-plane
prefix is filling with dead processes' keys; if
`rate(ravel_admission_reconciliation_keys_reaped_total[1h])` is at zero
alongside it, the prefix is filling faster than it is being cleared.

The `reason` label carries `byte_rate`, `series_rate`, `series_cap`, `clock`,
`skew`, or `structural`. The active-streams count for logs renders under
`ravel_admission_active_series` with `signal="logs"`, not under a separate
metric name. A sustained nonzero
`ravel_admission_reconciliation_failures_total` rate means a process cannot
read its siblings' snapshots and is falling back to its last-computed soft
threshold; admission never fails closed on it, so it signals degrading
fleet-wide accuracy, not that ingest is down.

#### Reading the `reason` label

Each reason answers a different operator question, and the unit each one
counts differs, so a rate summed across reasons means nothing. Read them
separately.

| `reason` | Counts | What it means |
|---|---|---|
| `byte_rate` | Requests | The tenant sent more charged bytes per second than its ingest byte-rate limit allows. |
| `clock` | Requests | The receiving replica's own clock was implausible, so the request was refused before any data was read. The fault is the replica's. |
| `series_rate` | Series | New series or log streams appeared faster than the creation-rate limit allows. |
| `series_cap` | Series | The tenant is at its active series or stream cap, so points for series past the cap were dropped. |
| `skew` | Points, records, or spans | The event timestamp sat too far ahead of, or behind, ingest time. A sender clock problem, or a backfill wider than the accepted lag. |
| `structural` | Points, records, or spans | The data itself cannot be represented: a delta-temporality metric, an over-long label, a body kind with no stored form. Retrying the same payload always fails the same way. |

`skew` and `structural` count individual points, log records, or spans, and
they match what the sender is told in the OTLP partial-success response, so a
client that reads `rejected_data_points` and an operator reading this counter
see the same number. The OTLP Arrow (OTAP) surface has no partial-success
field, so on that surface this counter is the only place the drop appears.

The two reasons want different alerts. `skew` is usually a fleet-wide clock or
backfill problem and clears on its own once the sender is fixed; `structural`
never clears without a change to what the sender emits, so any sustained rate
is worth paging a human who can go and read
[the ingest guide's temporality recipe](ingest.md#delta-temporality-metrics).

Both alerts ship as the `ravel-ingest-rejections` group in
[`deploy/prometheus/ravel.rules.yaml`](../../deploy/prometheus/ravel.rules.yaml);
load that file rather than copying the block.

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

Both rules are absolute rates of rejected units per second, broken out by
tenant and signal, because there is no per-tenant admitted-points series to
divide by. `ravel_admission_rejected_total{reason="skew"}` counts individual
points, records, or spans, while `ravel_admission_admitted_total` counts
requests; dividing one by the other inflates the result by the mean points per
request, which is three orders of magnitude at typical batching, so that ratio
means nothing. An absolute rate tells you how much data a tenant is losing to
the event-time window. It does not tell you what fraction of that tenant's
traffic that is, so a large tenant with a steady backfill and a small tenant
with a broken clock can trip the same threshold; tune the threshold per tenant
and treat the alert as a prompt to check clock sync and backfill status, not as
a percentage. The skew rule uses a nonzero threshold because a few late points
are normal. The structural rule keeps `> 0` because one sender emitting a
metric type Ravel cannot store drops every point of that metric forever, and
that is worth seeing even at a low rate.

Neither rule alerts on `ravel_ingest_body_conversions_total`. It exists so a
query that returns JSON text where a reader expected a plain message has an
explanation. A sustained rate means a sender is emitting structured log bodies,
which is supported, not a fault.

This paragraph is the normative description of that counter; the ingest guide,
the admission-limits reference, and the counter's own `HELP` text point here.
It counts conversions at normalization, not stored records. The logs ingest
handler increments it as soon as `normalize_logs` returns, which is before the
layer-4 active-stream cap drops the records whose stream is over the cap, and
before the shard write runs at all. So a converted record can be counted and
then not stored: dropped by the stream cap, or lost with every other record in
a request whose write fails. Read it as a conversion rate, in the sense of "how
much of this tenant's log traffic arrives with a structured body", and never as
a count of rows in storage. It is still not a rejection counter, which is why
it is its own family rather than a `reason` on
`ravel_admission_rejected_total`: a record it counts was admitted by
normalization, and an operator alerting on rejection reasons must see nothing
from it.

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

Alongside the cost, the SQL query endpoint records each query's final outcome:
success, error, timeout, or cancelled. A timeout or a cancelled outcome (the
client disconnected while the query was still running) carries the cost the
query incurred up to that point, not zeros. This works because an object-store
request is counted the moment it is issued, so a fetch still outstanding when
the deadline trips or the caller goes away is already counted; only its
transferred bytes wait until the fetch returns, since their length is not known
before then. A query that times out or is cancelled therefore reports every
request it issued before it stopped, including any still outstanding at that
moment, and the bytes of the fetches that had completed by then. Fetches the
query would have issued later, had it run on, are not counted.

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

### Query audit (`ravel_audit_write_failures_total`)

Labels: `mode` only. Within one flush, the counter increments once per tenant
group whose write fails, since a flush now writes one object per tenant. A
`tenant_hash` label would disclose which tenant's writes failed on this
unauthenticated route, so the family carries none. It renders only in a mode
that installed the query-audit pipeline (`all` or `query`); a gateway- or
maintain-only process omits the family rather than reporting a zero for a
pipeline it never ran.

| Metric | Meaning |
|---|---|
| `ravel_audit_write_failures_total` | Query-audit writes that failed and were released anyway under `--audit-mode best-effort`. Each one is a query that was served with no durable audit record. |

Under `--audit-mode required` (the default) a failed audit write fails the
query with a 503 instead, and is not counted here, so this counter is always
zero on a fail-closed deployment. On a best-effort one, any increase is the
audit trail going incomplete while queries keep succeeding, which is why an
operator alerts on the increase rather than on a threshold.

### Distributed read fan-out (`ravel_distrib_*`)

Labels: `mode` only, plus `le` on the histogram buckets and `class`
(`pinned`|`resolve`) on the fragment in-flight gauge and admission-wait
counter. This
family carries no per-shard, per-worker, or per-tenant label: a fan-out
spanning many workers and tenants must not turn one query into a cardinality
explosion. It renders only when the process runs with `--distributed-query`;
a local-only process omits the family entirely.

| Metric | Meaning |
|---|---|
| `ravel_distrib_fragment_requests_total` | Inbound fragment (`SeriesFetch`) requests served after passing token auth and fragment admission. Worker side. |
| `ravel_distrib_fragment_auth_failures_total` | Inbound fragment requests refused at capability auth: missing, bad MAC, expired, tenant mismatch, or query mismatch. |
| `ravel_distrib_fragment_inflight{class}` | Gauge. Fragment requests currently holding a fragment-admission permit, by admission class: `class="pinned"` for intra-cluster requests (admits against `--max-inflight-fragments`), `class="resolve"` for cross-cluster federation requests (admits against `--max-inflight-federated-resolves`). The two classes never share a permit pool, so a peer cluster saturating `resolve` cannot starve this cluster's own `pinned` slices. |
| `ravel_distrib_fragment_admission_waits_total{class}` | Counter. Inbound fragment requests, by admission class, that found their class's semaphore saturated at acquire time and had to queue. |
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

Three gaps limit what the per-query cost family can show: a failed, timed-out,
or cancelled query folds the cost it actually incurred, but the exported
counters do not yet split by outcome, so that spend is indistinguishable from
a successful query's; a Flight SQL statement records two folds for one
logical query; and an abandoned Flight fetch still records its partial cost.
The [cost model guide](cost-model.md#per-query-cost-accounting) sets each one
out in full, because each is a property of the accounting rather than of the
metric route.

## Background

The `/metrics` route, the label allowlist, and per-query cost accounting:
ADR-0044. The `reason` label and the admission usage family: ADR-0051. The
read caches and their disk tier: ADR-0046, ADR-0064. Maintenance safety,
ownership, merge memory, and the at-rest scrubber: ADR-0048, ADR-0058,
ADR-0059, ADR-0065. Log POSTINGS and dynamic columns: ADR-0049, ADR-0100.
Distributed read fan-out: ADR-0071. Wire-byte accounting: ADR-0084. The metric
metadata cache: ADR-0085. Alert evaluation and its at-least-once notification
contract: ADR-0043.
