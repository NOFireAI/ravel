# Admission limits

Ravel enforces per-tenant admission limits on every ingest path (OTLP HTTP,
OTLP gRPC, OTAP, Remote Write, logs, spans). The limits exist so that one tenant
cannot drive fleet-wide object count, PUT spend, catalog size, or query fan-out
without a per-tenant bound to attribute and cap it. Each layer runs before the
allocation it exists to bound. Nothing reaches a shard buffer without passing
all of them.

This guide covers what each knob does, its shipped default, and exactly what a
client sees when it trips one. For the read-side query budgets
(`max_series`/`max_samples`/`max_segments`), see
[query.md](query.md#query-budgets). Those are a different mechanism.

## Where limits are configured

`--limits-file <path>` points `ravel-server` at a TOML file with a `[defaults]`
table and one `[tenants.<tenant-id>]` table per override:

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

Ravel loads and checks the file once at startup. Changing limits is a restart,
the same as every other per-tenant flag (`--retention-tenant`,
`--tenant-token`). An unparseable file, an unknown key, or a nonsensical limit
(zero, or a burst set without its rate or vice versa) fails startup rather than
silently falling back to defaults.

Absent `--limits-file`, every tenant gets the shipped defaults below.
Enforcement is on by default with finite defaults. A tenant that genuinely needs
no cap on a knob sets it explicitly to the string `"unlimited"` on that knob's
own key (for example `max_active_series = "unlimited"`, `ingest_bytes_per_sec =
"unlimited"`, as shown above). This is visible in configuration review rather
than being the silent default.

## The limits

All limits are per tenant except `max_bytes_scanned` and the concurrent-query
ceiling (see the notes below the table). Rates are token buckets (a sustained
`per_sec` plus an instantaneous `burst`).

| Knob | Shipped default | Config key |
|---|---|---|
| Request body size | 16 MiB | not configurable (fixed transport cap) |
| Ingest byte rate / burst | 32 MiB/s / 64 MiB | `ingest_bytes_per_sec` / `ingest_byte_burst` |
| Active-series cap (metrics) | 200,000 | `max_active_series` |
| Active-stream cap (logs) | 200,000 | `max_active_streams` |
| Series-creation rate / burst | 10,000/s / 100,000 | `series_creation_rate_per_sec` / `series_creation_burst` |
| Query bytes-scanned budget | unlimited | `max_bytes_scanned` |
| Fleet-wide concurrent-query ceiling | unlimited | `--max-concurrent-queries` (CLI flag, not a `--limits-file` key) |
| Event-time future skew | 10 m | not configurable (`ravel-otlp` default) |
| Event-time ingest lag | 2 h | not configurable (`ravel-otlp` default) |
| Idle flush delay | 40 s | not configurable (shard default) |
| Min flush bytes | 256 KiB | not configurable (shard default) |

`max_bytes_scanned` lives in the same `[defaults]`/`[tenants.<id>]` tables as the
ingest limits above, but is not yet tenant-parameterized at enforcement time.
The query engine holds one process-wide budget, so only the `[defaults]` value
is enforced. A `[tenants.<id>]` override for this key is parsed and checked, but
has no effect beyond a startup warning naming the ineffective tenant. True
per-tenant query enforcement needs a tenant-aware engine configuration lookup,
tracked separately from this ingest-side admission mechanism.

`--max-concurrent-queries` is a fleet-wide, not per-tenant, ceiling on how many
queries can execute concurrently across the whole process. It guards against
total query fan-out overwhelming the fleet rather than any one tenant's own
concurrency. It is a CLI flag, not a `--limits-file` key, and it runs only in
the query-serving modes (`all`, `query`). It covers every query surface: the
PromQL HTTP handlers, the SQL HTTP endpoint, and both Flight SQL phases
(`GetFlightInfo` for planning, `DoGet` for execution, each holding its own slot
for its own phase's duration). Omitted means unlimited, the same safe-default
convention as `max_bytes_scanned`. A `0` value is rejected at startup rather than
rejecting every query. A rejected query gets HTTP 503 (PromQL/SQL HTTP) or
Flight's `RESOURCE_EXHAUSTED` status (Flight SQL), the same shape as any other
admission rejection.

The active-series/stream default is 200,000. The real per-entry cost of the
exact two-epoch tracker was measured at 35-56 bytes, so `ravel-server` ships
this figure to keep the worst-case tracker footprint near 27-43 MiB per fully
active tenant instead of 140-224 MiB at 1,000,000. The `--limits-file` raises it
per tenant where the memory is available.

## What a breach looks like to a client

The rejection shape depends on the limit's scope. A **request-scoped** limit
rejects the whole request. A **per-item** limit rejects only the offending
points/records/spans and admits the rest through OTLP partial success.

| Limit | Scope | OTLP HTTP | OTLP / OTAP gRPC | Remote Write |
|---|---|---|---|---|
| Body size | request | 413 | `RESOURCE_EXHAUSTED` | 413 |
| Byte rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Series-creation rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Active-series/stream cap | per series | 200 + partial success | OK + partial success | 204, written-count header excludes rejected samples |
| Event-time skew | per point | 200 + partial success | OK + partial success | 204, written-count header excludes rejected samples |

### Body size

Rejected at the transport, before Ravel buffers the body. `/v1/metrics`,
`/v1/logs`, `/v1/traces` carry an explicit 16 MiB `DefaultBodyLimit`. Every
tonic service caps `max_decoding_message_size` at 16 MiB. `/api/v1/write` caps
the compressed body at 16 MiB ahead of its 64 MiB decompressed cap.

### Ingest byte rate

Charged on wire body bytes after tenant resolution and before decode, so
over-rate bytes cost one buffered body and nothing else. A request whose size
exceeds the available tokens is rejected whole without consuming any tokens. A
retry after the bucket refills succeeds. This is why the rejection is 429 with
`Retry-After` and not a partial success: the same request will work later.

### Series-creation rate

Ravel computes the batch's distinct new-series demand first. If it exceeds
available tokens, Ravel rejects the whole request 429 without consuming tokens
and without admitting any of the batch. The rejection is whole-request on
purpose. A partially admitted batch that the client retries would re-ingest the
admitted remainder, and for logs and spans that re-ingest is user-visible
duplication (there is no query-time dedup for them, see
[consistency-model.md](../consistency-model.md#duplicates-and-idempotency)).
Retryable rejections are therefore always all-or-nothing.

### Active-series and active-stream caps

A point that would create a series (or a log stream) beyond the cap is rejected
per-series through partial success. Points for series already active in the
current or previous one-hour epoch are admitted. This rejection is not
whole-request, because a cap breach is not retryable-soon and OTLP
partial-success semantics instruct the client not to resend the rejected items,
so no retry-duplication arises.

Remote Write carries no partial-success message, so a per-series (or per-point)
rejection admits the rest of the batch and returns 2xx. RW 2.0 additionally
reports the true count in `X-Prometheus-Remote-Write-Samples-Written`. A non-2xx
here would make Prometheus retry or drop the whole batch, including its admitted
samples, so `429` is reserved for the rate limits, where a retry genuinely
succeeds later. The dropped over-cap series is observable through the per-tenant
rejection counters.

### Event-time skew

Metrics already enforced event-time skew at admission. Logs and spans now do
too, through the same typed partial-success machinery. A record whose event time
falls outside `[ingest_ts - max_ingest_lag, ingest_ts + max_future_skew]` is
rejected, never clamped. Rewriting a sender's event time would be silent data
corruption, where a rejection is visible and countable. For the full explanation
of why these bounds exist (they are what makes the catalog listing window
sound), see [ingest.md](ingest.md#event-time-skew-bounds).

For spans specifically: the span's `end_ts` is bounded on both edges. It can
lead ingest time by at most `max_future_skew` and lag it by at most
`max_ingest_lag`, and `end_ts < start_ts` is rejected outright. Both bounds
anchor on the end, not the start. A long-running span that started more than
`max_ingest_lag` ago but ended within the window is admitted, and only a span
reported more than `max_ingest_lag` after it *ended* is rejected as late. This
lets a genuine long-running span through while keeping the catalog listing window
sound. Any span overlapping a query range has its end at or after the range
start, so the end's window placement alone keeps its ingest hour listed.

### Receiver-clock floor

Independently of the sender's timestamps, Ravel checks its own admission clock. A
reading below a compiled floor (2020-01-01T00:00:00Z, since no host legitimately
runs Ravel with a clock older than the system), or one that yields no
representable ingest-hour bucket, rejects the *whole* request with HTTP 503 /
gRPC `UNAVAILABLE`, counted under
`ravel_admission_rejected_total{reason="clock"}`. The fault is the replica's,
not the request's, so a retry against a healthy replica succeeds. No per-record
decision is meaningful when the reference clock itself is nonsense. The same
floor also extends the fail-loud flush-open check, so a clock that goes bad
between a buffered-mode ack and the flush fails the flush loudly rather than
writing acked data into a far-past hour bucket. A wrong-but-post-2020 clock still
cannot be detected against any reference. What the floor buys there is a loud,
attributable rejection spike (honest clients' current timestamps fall outside
the bad clock's shifted window and are rejected `reason="skew"`) instead of
silent pollution of the hour-partitioned layout.

## Raising max_ingest_lag: a coordinated change

`max_ingest_lag` is one shared bound, not a per-signal one, in the sense that
matters operationally. The admission checks (metrics, logs, spans, each in its
own crate, `ravel-otlp`'s `limits.rs`/`logs_limits.rs`/`traces_limits.rs`) and
the catalog listing window (`crates/ravel-catalog/src/config.rs`) each hold their
own `max_ingest_lag_ns` constant, duplicated rather than shared by reference.
`ravel-maintain`'s own copy carries an explicit "MUST be kept in sync" comment
and a startup equality assertion against it. This is exactly why the
coordinated-raise rule below exists: nothing in code enforces the values staying
equal, only convention and this assertion. The admission bound decides what old
data is *admitted*. The listing window decides what old data is *discoverable*.
If you raise the admission lag alone, you admit and acknowledge records that the
listing window can then fail to find on any non-token query.

Raising the admission lag for a signal or tenant is therefore legal only
together with the catalog-side listing-window configuration. Widen the catalog
window first, then the admission bound. Lowering the admission lag is always
safe. This is the same coordinated-configuration discipline `max_flush_lifetime`
follows between writers and folders
([catalog-and-mvcc.md](../catalog-and-mvcc.md), "Config discipline"). The
normative statement of this rule lives in
[consistency-model.md](../consistency-model.md#late-and-skewed-data).

## Fleet-wide enforcement via reconciliation

Every configured limit is a **fleet-wide** total, not a per-process one. The
value you set is what the whole fleet enforces, regardless of how many ingest
replicas run behind the load balancer. You do not divide a target by replica
count.

The hot-path check stays per-process and sub-microsecond. There is no S3
round-trip on any admission decision. Instead each process reconciles its
effective caps off the hot path on a fixed interval, reading every sibling's
usage from object storage and adjusting the number its local check compares
against. `--admission-reconcile-interval` sets the interval (default 10s, the
value of `ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL`).

The two kinds of cap converge differently:

- **Count caps** (`max_active_series`, `max_active_streams`) are a safe
  overestimate. Reconciliation sums each replica's own active set without
  deduplicating a series that two replicas both hold, so it can only drive a
  replica to reject *sooner* than the configured cap, never admit more than it.
  The fleet total is bounded within one reconciliation interval's worth of
  admission per process.
- **Rate caps** (`ingest_bytes_per_sec`, `series_creation_rate_per_sec`)
  converge to the configured cap as a fleet-wide total by equal-share division.
  Each of the `N` live processes enforces `cap / N`, so their sum is at most the
  configured cap. When a replica joins or leaves, the fleet total settles back
  to the cap within one interval.

Until a process's first reconciliation cycle completes (and briefly after a
replica count change), enforcement falls back to per-process behavior, so a
newly started fleet can transiently admit above the cap by a bounded,
self-correcting margin.

## Flush cadence: cost, not rejection

`max_flush_delay_idle` (40 s) and `min_flush_bytes` (256 KiB) are not admission
rejections. They tune PUT cost. The 500 ms flush-age trigger fires promptly only
when the flush window holds a strict-mode waiter or the buffer has reached
`min_flush_bytes`. An otherwise-idle buffer waits the slower `max_flush_delay_idle`
before flushing. This drops the volume-independent PUT floor for buffered-mode
trickle tenants by roughly 20x. Strict-mode ack latency is unchanged: a
strict-mode waiter is always a priority flush. These two values are shard
defaults today and are not server-flag configurable.

## Spans have no series-count cap

Metrics series and log streams both have stable identity (`SeriesId`,
`LogStreamId`) and get both the active-count cap and the series-creation-rate
cap. Spans have no stable series identity (`trace_id` is sender-chosen and
naturally unbounded), so the body-size, byte-rate, and event-time-skew layers
bound spans alone. This is stated, not silent: there is no per-tenant span-count
cap to configure.

## Per-tenant usage counters

The admission controller keeps per-(tenant, signal) counters (active series,
admitted/rejected totals, rejected bytes, rejections by reason) in-process. As
of this writing they are not exposed at `GET /metrics`: no admission snapshot is
wired into the metrics renderer. Per-tenant attribution behind an opt-in
`--metrics-tenant-labels` flag, and folding untracked tenants into
`tenant_hash="other"`, is designed but not yet implemented. There is currently
no admission-usage surface at `/metrics` at all, fleet-total or per-tenant.
</content>
