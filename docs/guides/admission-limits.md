# Admission limits

Ravel enforces per-tenant admission limits on every ingest path (OTLP HTTP,
OTLP gRPC, OTAP, Remote Write, logs, spans). The limits give each tenant a
bound on fleet-wide object count, PUT spend, catalog size and query fan-out,
so that the cost can be attributed and capped. Each layer runs before the
allocation that it bounds. Nothing reaches a shard buffer before it passes
all of them.

The read-side query budgets (`max_series`/`max_samples`/`max_segments`) are a
different mechanism. See [query.md](query.md#query-budgets). Several of them
resolve at startup, and others are fixed defaults:

- Unset, `--fetch-concurrency` follows the core count.
- The two SQL memory ceilings and `--cache-max-bytes` follow the memory of
  the host (`MemTotal`, capped by the cgroup limit in a container).
- `--max-segments` (1,000,000) and the engine deadline (11 minutes) are fixed
  defaults that do not vary with the host.

Read the resolved values from the startup log.

## Where limits are configured

Point `ravel-server` at a TOML file with `--limits-file <path>`. The file has
a `[defaults]` table and one `[tenants.<tenant-id>]` table per override:

```toml
[defaults]
max_active_series = 200000
max_active_streams = 200000
ingest_bytes_per_sec = 33554432
ingest_byte_burst = 67108864
series_creation_rate_per_sec = 10000
series_creation_burst = 100000

[tenants.acme]
max_active_series = 5000000
ingest_bytes_per_sec = "unlimited"
```

`ravel-server` loads and validates the file once at startup. To change a
limit, restart the server, as for every other per-tenant flag
(`--retention-tenant`, `--tenant-token`). Startup fails on an unparseable
file, an unknown key or an invalid limit. An invalid limit is zero, a burst
without its rate, or a rate without its burst.

Without `--limits-file`, every tenant gets the shipped defaults below.
Enforcement is on by default, with finite defaults. To remove the cap on one
knob for a tenant, set the key of that knob to the string `"unlimited"` (for
example `max_active_series = "unlimited"`,
`ingest_bytes_per_sec = "unlimited"`, as shown above). The choice is then
visible in a configuration review.

## The limits

All limits are per tenant except `max_bytes_scanned` and the
concurrent-query ceiling (see the notes after the table). A rate is a token
bucket: a sustained `per_sec` plus an instantaneous `burst`.

| Knob | Shipped default | Config key |
|---|---|---|
| Request body size | 16 MiB | not configurable (fixed transport cap) |
| Ingest byte rate / burst | 32 MiB/s / 64 MiB | `ingest_bytes_per_sec` / `ingest_byte_burst` |
| Active-series cap (metrics) | 200,000 | `max_active_series` |
| Active-stream cap (logs) | 200,000 | `max_active_streams` |
| Series-creation rate / burst | 10,000/s / 100,000 | `series_creation_rate_per_sec` / `series_creation_burst` |
| Query bytes-scanned budget | unlimited | `max_bytes_scanned` |
| Fleet-wide concurrent-query ceiling | unlimited | `--max-concurrent-queries` (CLI flag, not a `--limits-file` key) |
| Event-time future skew | 10 m | not configurable (compiled-in default) |
| Event-time ingest lag | 2 h | not configurable (compiled-in default) |
| Fast-tier flush delay | 2 s | `--max-flush-delay` (CLI flag) |
| Idle-tier flush delay | 40 s | `--max-flush-delay-idle` (CLI flag) |
| Min flush bytes | 256 KiB | `--min-flush-bytes` (CLI flag) |

The three flush-cadence flags move as a set. See
[Flush cadence](#flush-cadence), and [cost-model.md](cost-model.md) for what
a change to them costs and buys.

### Query bytes-scanned budget

`max_bytes_scanned` lives in the same `[defaults]`/`[tenants.<id>]` tables as
the ingest limits. Only the `[defaults]` value is enforced, because the query
engine holds one process-wide budget. A `[tenants.<id>]` override for this
key is parsed and validated. Its only effect is a startup warning that names
the ineffective tenant.

### Concurrent-query ceiling

`--max-concurrent-queries` is a fleet-wide ceiling on how many queries can
execute concurrently across the whole process. It is not a per-tenant
ceiling. It guards the fleet against total query fan-out, not against the
concurrency of one tenant.

- It is a CLI flag, not a `--limits-file` key.
- It runs only in the query-serving modes (`all`, `query`).
- It covers every query surface: the PromQL HTTP handlers, the SQL HTTP
  endpoint, and both Flight SQL phases. `GetFlightInfo` (planning) and
  `DoGet` (execution) each hold a slot for the duration of their own phase.
- Omitted means unlimited, the same convention as `max_bytes_scanned`.
- A `0` value is rejected at startup.
- A rejected query gets HTTP 503 (PromQL/SQL HTTP) or the
  `RESOURCE_EXHAUSTED` status of Flight (Flight SQL), the same shape as any
  other admission rejection.

### Active-series and active-stream default

The active-series and active-stream default is 200,000. The exact two-epoch
tracker costs 35-56 bytes per live entry. That measurement counts
hashbrown's power-of-two table sizing at 7/8 load and allocator headroom.

| Cap | Worst-case tracker footprint per fully active tenant |
|---|---|
| 200,000 | near 27-43 MiB |
| 1,000,000 | 134-214 MiB |

The arithmetic is cap times bytes-per-entry times two rotating epochs times
two tracked signals. Multiply the result across tenants and replicas. Raise
the cap per tenant in the `--limits-file` where the memory is available.

200,000 is also the `AdmissionLimits::default()` of `ravel-ingest`.
`ravel-server` serves that value as its shipped default and holds no second
copy of the number. A process that embeds the ingest library therefore
enforces the same caps.

## What a client sees

The rejection depends on the scope of the limit. A **request-scoped** limit
rejects the whole request. A **per-item** limit rejects only the offending
points, records or spans, and admits the rest through OTLP partial success.

| Limit | Scope | OTLP HTTP | OTLP / OTAP gRPC | Remote Write |
|---|---|---|---|---|
| Body size | request | 413 | `RESOURCE_EXHAUSTED` | 413 |
| Byte rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Series-creation rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Active-series/stream cap | per series | 200 + partial success | OK + partial success | 204, written-count header excludes rejected samples |
| Data points per request | request | 200 + partial success, whole-request count | OK + partial success | not applicable |
| Histogram bucket cap | per point | 200 + partial success | OK + partial success | not applicable |
| Event-time skew | per point | 200 + partial success | OK + partial success | 204, written-count header excludes rejected samples |
| Informational field drop | per item, costs no item | 200 + partial success, zero count | OTLP: OK + partial success, zero count. OTAP: not reported | not reported (no partial-success message) |

The last row is not an admission limit. It is a field of an admitted item
that Ravel did not store: a histogram `min`/`max`, an exemplar, an integer
past 2^53, or one bad attribute of a log record or span. The transports
answer it differently. A zero rejected count with a populated `error_message`
can look like a clean write.
[ingest.md](ingest.md#zero-count-partial-success) is normative for it.

### Body size

Ravel rejects an oversized body at the transport, before it buffers the body:

- `/v1/metrics`, `/v1/logs` and `/v1/traces` carry an explicit 16 MiB
  `DefaultBodyLimit`.
- Every tonic service caps `max_decoding_message_size` at 16 MiB.
- `/api/v1/write` caps the compressed body at 16 MiB ahead of its 64 MiB
  decompressed cap.

### Ingest byte rate

Ravel charges the byte rate after tenant resolution and before decode. The
charge is on the decompressed body for OTLP and on the compressed body for
Remote Write. Over-rate bytes therefore cost one buffered body and nothing
else.

A request larger than the available tokens is rejected whole and consumes no
tokens. A retry after the bucket refills succeeds. For that reason the
rejection is 429 with `Retry-After` and not a partial success.

### Series-creation rate

Ravel first computes the distinct new-series demand of the batch. If the
demand exceeds the available tokens, the whole request is rejected with 429.
The rejection consumes no tokens and admits none of the batch.

A retryable rejection is always all-or-nothing. When a client retries a
partially admitted batch, it re-ingests the admitted part. For logs and
spans that re-ingest is duplication that a user can see, because they have
no query-time dedup (see
[consistency-model.md](../consistency-model.md#duplicates-and-idempotency)).

### Active-series and active-stream caps

A point that creates a series (or a log stream) beyond the cap is rejected
per series through partial success. Points for series already active in the
current or previous one-hour epoch are admitted. This rejection is not
whole-request: a cap breach is not retryable soon, and OTLP partial-success
semantics tell the client not to resend the rejected items. No retry
duplication occurs.

Remote Write carries no partial-success message. A per-series (or per-point)
rejection admits the rest of the batch and returns 2xx. RW 2.0 also reports
the true count in `X-Prometheus-Remote-Write-Samples-Written`. A non-2xx
response makes Prometheus retry or drop the whole batch, including its
admitted samples. `429` is therefore reserved for the rate limits, where a
later retry succeeds. The per-tenant rejection counters show the dropped
over-cap series.

### Data-point and histogram-bucket caps

Both are structural bounds in normalization. They live in `IngestLimits`,
and no flag or `--limits-file` key configures them.
[ingest.md](ingest.md#admission-limits) lists them with their defaults.

`max_data_points_per_request` (100,000) is checked twice: against the wire
data-point count, and against the normalized points that those data points
expand into. One classic `Histogram` data point expands into one point per
explicit bound plus `+Inf`/`_sum`/`_count`. Each check rejects the whole
request and reports the rejected total in wire data points.

`max_histogram_buckets` (160) bounds the `explicit_bounds` of one classic
`Histogram` data point and rejects only that data point. It is a
memory-safety bound. The sender chooses the bound list, and every bound
becomes a series with its own copy of the labels of the point. Without the
cap, the transport body limit lets one request expand into millions of them.
A real
exporter never meets the cap. A deployment that sees this rejection has a
misconfigured or hostile sender, and the cap is not a limit to raise.

### Event-time skew

Metrics, logs and spans all enforce event-time skew at admission, through
the same typed partial-success machinery. A record whose event time falls
outside `[ingest_ts - max_ingest_lag, ingest_ts + max_future_skew]` is
rejected, never clamped. A rejection is visible and countable. A rewritten
event time is silent data corruption. These bounds keep the catalog listing
window sound. See [ingest.md](ingest.md#event-time-skew-bounds).

For a span, both bounds apply to `end_ts`:

- The end can lead ingest time by at most `max_future_skew`.
- The end can lag ingest time by at most `max_ingest_lag`.
- A span with `end_ts < start_ts` is rejected.
- A long-running span that started more than `max_ingest_lag` ago but ended
  within the window is admitted.
- A span reported more than `max_ingest_lag` after it *ended* is rejected as
  late.

### Receiver-clock floor

Ravel also checks its own admission clock, independently of the timestamps
of the sender. Ravel rejects the *whole* request with HTTP 503 / gRPC
`UNAVAILABLE` in two cases:

- The clock reads below a compiled floor (2020-01-01T00:00:00Z).
- The reading yields no representable ingest-hour bucket.

The rejection counts under `ravel_admission_rejected_total{reason="clock"}`.
The fault is in the replica and not in the request, so a retry against a
healthy replica succeeds.

The same floor extends the flush-open check. If a clock goes bad between a
buffered-mode ack and the flush, the flush fails loudly. It does not write
acked data into a far-past hour bucket.

Ravel has no reference to detect a wrong clock that reads after 2020. In
that case the current timestamps of honest clients fall outside the shifted
window of the bad clock and are rejected with `reason="skew"`. The result is
a rejection spike that can be attributed, and the hour-partitioned layout is
not silently polluted.

## Replaying old telemetry

To replay telemetry older than 2h after an outage, or to bulk-import an
archive, set `--max-ingest-lag`:

```
ravel-server --max-ingest-lag 720h ...
```

- The default is `2h`.
- The value is a humantime duration (for example `2h`, `720h`, `30d`). A
  zero duration is rejected.
- The change takes effect at startup. It reaches every network ingest
  surface at once (OTLP HTTP, OTLP gRPC, OTAP, Remote Write, and the span
  surface).
- Startup refuses a value that the catalog listing window cannot serve, and
  names both values.
- The raise still respects the `max_flush_lifetime` retention-floor
  discipline ([catalog-and-mvcc.md](../catalog-and-mvcc.md), "Config
  discipline"). A `--max-ingest-lag` above the configured retention window
  fails startup at the retention-floor check.

`max_ingest_lag` is one shared bound for all signals. The three admission
checks (metrics, logs, spans) and the catalog listing window each hold their
own `max_ingest_lag_ns` constant. The admission bound decides what old data
is *admitted*. The listing window decides what old data is *discoverable*.
A record that is admitted but that the listing window cannot find is lost
silently on any non-token query. The two must therefore move together: the
catalog window first, then the admission bound. Lowering the bound is always
safe.

The flag makes that move with one value. The value drives the catalog
listing window (`ravel_catalog::CatalogConfig::max_ingest_lag_ns`) and all
three OTLP admission bounds
(`IngestLimits`/`LogIngestLimits`/`SpanIngestLimits::max_ingest_lag_ns`).
The window is set first and the admission bound is derived from it, so the
two can never be set inconsistently.

The normative statement of the late-data rule is in
[consistency-model.md](../consistency-model.md#late-and-skewed-data).

## Fleet-wide enforcement via reconciliation

Every configured limit is a **fleet-wide** total. The whole fleet enforces
the value that you set, for any number of ingest replicas behind the load
balancer. Do not divide a target by the replica count.

The hot-path check stays per-process and sub-microsecond, with no S3
round-trip on any admission decision. Each process reconciles its effective
caps off the hot path on a fixed interval. It reads the usage of every
sibling from object storage and adjusts the number that its local check
compares against.

- `--admission-reconcile-interval` sets the interval (default 10s).
- A zero or unparseable duration fails startup.
- Reconciliation runs only in the ingest-serving modes (`all`, `gateway`).

The two kinds of cap converge differently:

- **Count caps** (`max_active_series`, `max_active_streams`) are a safe
  overestimate. Reconciliation sums the active set of each replica and does
  not deduplicate a series that two replicas both hold. It can only make a
  replica reject *sooner* than the configured cap, and never admits more
  than the cap. The fleet total is bounded within one reconciliation
  interval's worth of admission per process.
- **Rate caps** (`ingest_bytes_per_sec`, `series_creation_rate_per_sec`)
  converge to the configured cap as a fleet-wide total by equal-share
  division. Each of the `N` live processes enforces `cap / N`, so their sum
  is at most the configured cap. When a replica joins or leaves, the fleet
  total settles back to the cap within one interval.

Until the first reconciliation cycle of a process completes, enforcement is
per-process. The same applies briefly after a replica count change. A newly
started fleet can therefore admit more than the cap for a short time, by a
bounded margin that corrects itself. A shorter interval tightens that window
and costs more reconciliation requests. A longer interval does the reverse.

## Flush cadence

The flush cadence tunes PUT cost and rejects nothing. The shipped defaults
are `--max-flush-delay` 2 s, `--max-flush-delay-idle` 40 s, and
`--min-flush-bytes` 256 KiB. All three apply to all three ingest pipelines
(metrics, logs, spans).

The fast 2 s age trigger fires only in two cases: the flush window holds a
strict-mode waiter, or the buffer holds at least `--min-flush-bytes`. An
otherwise idle buffer waits the slower `--max-flush-delay-idle`. That drops
the volume-independent PUT floor for a buffered-mode trickle tenant by
roughly 20x. Strict-mode acknowledgement latency is unchanged, because a
strict-mode waiter is always a priority flush.

All three are `ravel-server` flags. Set them as a set:

- Startup refuses one or two of the three. A higher age threshold with a
  small byte threshold does not slow the fast tier down.
- Startup rejects a zero or unparseable duration, and a `--min-flush-bytes`
  of `0`.
- A higher `--max-flush-delay` also costs strict-mode acknowledgement
  latency directly. Startup validates it against a derived ceiling. Use it
  as the last lever.

[cost-model.md](cost-model.md) has the measured effect and the order in
which to use the other levers.

## Spans have no series-count cap

Metrics series and log streams have stable identity (`SeriesId`,
`LogStreamId`). They get the active-count cap and the series-creation-rate
cap. Spans have no stable series identity, because the sender chooses
`trace_id` and it is naturally unbounded. Only the body-size, byte-rate and
event-time-skew layers bound spans. There is no per-tenant span-count cap to
configure.

## Per-tenant usage counters

`GET /metrics` renders the per-(tenant, signal) counters of the admission
controller. Each carries `mode`, `tenant_hash`, and `signal` labels:

| Metric | Type | What it counts |
|---|---|---|
| `ravel_admission_active_series` | gauge | Series (metrics) or streams (logs) tracked for the active cap. |
| `ravel_admission_admitted_total` | counter | Requests admitted past the byte-rate layer. |
| `ravel_admission_admitted_bytes_total` | counter | Bytes charged against the byte-rate layer, which for a compressed request is the decompressed size. |
| `ravel_ingest_wire_bytes_total` | counter | Request-body bytes as they arrived on the wire. Its ratio to the row above is a tenant's effective compression factor. |
| `ravel_admission_rejected_total` | counter | Rejections, with a fourth `reason` label: `byte_rate`, `series_rate`, `series_cap`, `clock`, `skew`, `structural`. The first four count whole requests or series; `skew` and `structural` count individual points, log records, or spans, matching what the OTLP partial-success response tells the sender. |
| `ravel_ingest_body_conversions_total` | counter | Log records whose structured body was converted to canonical JSON text at normalization. Not a rejection, and counted before the stream cap and the write, so not a count of stored records. Normative description: [the observability guide](observability.md#reading-the-reason-label). |
| `ravel_ingest_resource_attrs_dropped_total` | counter | Metric resource attributes outside the allowlist, dropped rather than turned into labels. Not a rejection, and counted before the series cap and the write, so not a count of stored points. Covers OTLP HTTP and OTLP gRPC ingest only, for the metrics signal only. Normative description: [the observability guide](observability.md#resource-attributes-outside-the-allowlist). |
| `ravel_admission_reconciliation_failures_total` | counter | Reconciliation cycles whose sibling-snapshot read failed. The last-known threshold stays in force, so this says fleet-wide accuracy is degrading, not that ingest is down. |

By default the rows of every tenant fold into `tenant_hash="other"`. Signal
and reason then bound the cardinality of the family, and tenant count does
not. `--metrics-tenant-labels` renders the real per-tenant `tenant_hash`.
Turn it on only where the scrape network is trusted. The `/metrics` route is
unauthenticated, and per-tenant labels let a scraper enumerate tenant hashes
and their traffic.

## Background

- The per-tenant admission mechanism is
  [ADR-0051](../adrs/0051-tenant-admission-control.md).
- The fleet-global reconciliation above it is
  [ADR-0057](../adrs/0057-fleet-global-admission-reconciliation.md).
- The query bytes-scanned budget and the concurrent-query ceiling are
  ADR-0061.
- The flush-cadence defaults are ADR-0076 decision 4.
