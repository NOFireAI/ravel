# Admission limits

Ravel enforces per-tenant admission limits on every ingest path (OTLP HTTP,
OTLP gRPC, OTAP, Remote Write, logs, spans). The limits exist so that one
tenant cannot drive fleet-wide object count, PUT spend, catalog size, or
query fan-out without a per-tenant bound to attribute and cap it. Each layer
runs before the allocation it exists to bound: nothing reaches a shard buffer
without passing all of them.

This guide covers what each knob does, its shipped default, and exactly what
a client sees when it trips one. For the read-side query budgets
(`max_series`/`max_samples`/`max_segments`), see
[query.md](query.md#query-budgets); those are a different mechanism.

## Where limits are configured

`--limits-file <path>` points `ravel-server` at a TOML file with a
`[defaults]` table and one `[tenants.<tenant-id>]` table per override:

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

The file is loaded and validated once at startup; changing limits is a
restart, the same as every other per-tenant flag (`--retention-tenant`,
`--tenant-token`). An unparseable file, an unknown key, or a nonsensical
limit (zero, or a burst set without its rate or vice versa) fails startup
rather than silently falling back to defaults.

Absent `--limits-file`, every tenant gets the shipped defaults below.
Enforcement is on by default with finite defaults: an unbounded tenant is the
defect, not a capped one. A tenant
that genuinely needs no cap on a knob sets it explicitly to the string
`"unlimited"` on that knob's own key (e.g. `max_active_series =
"unlimited"`, `ingest_bytes_per_sec = "unlimited"`, as shown above), which
is visible in config review rather than being the silent default.

## The limits

All limits are per tenant except `max_bytes_scanned` and the
concurrent-query ceiling (see the notes below the table). Rates are token
buckets (a sustained `per_sec` plus an instantaneous `burst`).

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

The three flush-cadence flags move as a set: setting only one or two of the
three is refused at startup, because raising the age threshold alone while the
byte threshold stays small does not actually slow the fast tier down. See
[Flush cadence](#flush-cadence-cost-not-rejection) below, and
[cost-model.md](cost-model.md) for what moving them costs and buys.

`max_bytes_scanned` lives in the same `[defaults]`/
`[tenants.<id>]` tables as the ingest limits above, but only the
`[defaults]` value is enforced: the query engine holds one process-wide
budget. A `[tenants.<id>]` override for this key is parsed and validated, but
has no effect beyond a startup warning naming the ineffective tenant.

`--max-concurrent-queries` is a fleet-wide, not
per-tenant, ceiling on how many queries may execute concurrently across the
whole process, guarding against total query fan-out overwhelming the fleet
rather than any one tenant's own concurrency. It is a CLI flag, not a
`--limits-file` key, and only runs in the query-serving modes (`all`,
`query`). It covers every query surface: the PromQL HTTP handlers, the SQL
HTTP endpoint, and both Flight SQL phases (`GetFlightInfo` for planning,
`DoGet` for execution, each holding its own slot for its own phase's
duration). Omitted means unlimited, the same safe-default convention as
`max_bytes_scanned`; a `0` value is rejected at startup rather than
rejecting every query. A rejected query gets HTTP 503 (PromQL/SQL HTTP) or
Flight's `RESOURCE_EXHAUSTED` status (Flight SQL), the same shape as any
other admission rejection.

The active-series/stream default is 200,000. The per-entry cost of the exact
two-epoch tracker was measured at 35-56 bytes per live entry, once
hashbrown's power-of-two table sizing at 7/8 load and allocator headroom are
counted, so `ravel-server` ships that figure to keep the worst-case tracker
footprint near 27-43 MiB per fully active tenant. At 1,000,000 the same
arithmetic (cap times bytes-per-entry times two rotating epochs times two
tracked signals) gives 134-214 MiB per tenant, before multiplying across
tenants and replicas. The `--limits-file` raises it per tenant where the
memory is available.

## What a breach looks like to a client

The rejection shape depends on the limit's scope. A **request-scoped** limit
rejects the whole request; a **per-item** limit rejects only the offending
points/records/spans and admits the rest through OTLP partial success.

| Limit | Scope | OTLP HTTP | OTLP / OTAP gRPC | Remote Write |
|---|---|---|---|---|
| Body size | request | 413 | `RESOURCE_EXHAUSTED` | 413 |
| Byte rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Series-creation rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Active-series/stream cap | per series | 200 + partial success | OK + partial success | 204, written-count header excludes rejected samples |
| Event-time skew | per point | 200 + partial success | OK + partial success | 204, written-count header excludes rejected samples |

### Body size

Rejected at the transport, before the body is buffered. `/v1/metrics`,
`/v1/logs`, `/v1/traces` carry an explicit 16 MiB `DefaultBodyLimit`; every
tonic service caps `max_decoding_message_size` at 16 MiB; `/api/v1/write`
caps the compressed body at 16 MiB ahead of its 64 MiB decompressed cap.

### Ingest byte rate

Charged on wire body bytes after tenant resolution and before decode, so
over-rate bytes cost one buffered body and nothing else. A request whose
size exceeds the available tokens is rejected whole without consuming any
tokens; a retry after the bucket refills succeeds. This is why the rejection
is 429 with `Retry-After` and not a partial success: the same request will
work later.

### Series-creation rate

The batch's distinct new-series demand is computed first; if it exceeds
available tokens the whole request is rejected 429 without consuming tokens
and without admitting any of the batch. The rejection is whole-request on
purpose: a partially admitted batch that the client retries would re-ingest
the admitted remainder, and for logs and spans that re-ingest is
user-visible duplication (there is no query-time dedup for them, see
[consistency-model.md](../consistency-model.md#duplicates-and-idempotency)).
Retryable rejections are therefore always all-or-nothing.

### Active-series and active-stream caps

A point that would create a series (or a log stream) beyond the cap is
rejected per-series through partial success; points for series already
active in the current or previous one-hour epoch are admitted. This
rejection is not whole-request, because a cap breach is not retryable-soon
and OTLP partial-success semantics instruct the client not to resend the
rejected items, so no retry-duplication arises.

Remote Write carries no partial-success message, so a per-series (or
per-point) rejection admits the rest of the batch and returns 2xx; RW 2.0
additionally reports the true count in
`X-Prometheus-Remote-Write-Samples-Written`. A non-2xx here would make
Prometheus retry or drop the whole batch, including its admitted samples, so
`429` is reserved for the rate limits, where a retry genuinely succeeds
later. The dropped over-cap series is observable through the per-tenant
rejection counters.

### Event-time skew

Metrics, logs, and spans all enforce event-time skew at admission, through
the same typed partial-success machinery. A record whose
event time falls outside `[ingest_ts - max_ingest_lag, ingest_ts +
max_future_skew]` is rejected, never clamped: rewriting a sender's event
time would be silent data corruption, where a rejection is visible and
countable. For the full explanation of why these bounds exist (they are what
makes the catalog listing window sound), see
[ingest.md](ingest.md#event-time-skew-bounds).

For spans specifically: the span's `end_ts` is bounded on both edges -- it
may lead ingest time by at most `max_future_skew` and lag it by at most
`max_ingest_lag` -- and `end_ts < start_ts` is rejected outright. Both bounds
anchor on the end, not the start: a
long-running span that started more than `max_ingest_lag` ago but ended
within the window is admitted, and only a span reported more than
`max_ingest_lag` after it *ended* is rejected as late. This lets a genuine
long-running span through while keeping the catalog listing window sound (any
span overlapping a query range has its end at or after the range start, so
the end's window placement alone keeps its ingest hour listed).

### Receiver-clock floor

Independently of the sender's timestamps, Ravel checks its own admission
clock. A reading below a compiled floor
(2020-01-01T00:00:00Z -- no host legitimately runs Ravel with a clock older
than the system) or one that yields no representable ingest-hour bucket
rejects the *whole* request with HTTP 503 / gRPC `UNAVAILABLE`, counted under
`ravel_admission_rejected_total{reason="clock"}`. The fault is the replica's,
not the request's, so a retry against a healthy replica succeeds; no per-record
decision is meaningful when the reference clock itself is nonsense. The same
floor also extends the fail-loud flush-open check, so a clock that goes bad
between a buffered-mode ack and the flush fails the flush loudly rather than
writing acked data into a far-past hour bucket. A wrong-but-post-2020 clock
still cannot be detected against any reference; what the floor buys there is a
loud, attributable rejection spike (honest clients' current timestamps fall
outside the bad clock's shifted window and are rejected `reason="skew"`)
instead of silent pollution of the hour-partitioned layout.

## Raising max_ingest_lag: a coordinated change

`max_ingest_lag` is one shared bound, not a per-signal one, in the sense
that matters operationally: the three admission checks (metrics, logs, spans)
and the catalog listing window each hold their own `max_ingest_lag_ns`
constant, duplicated rather than shared by reference. Maintenance carries a
startup equality assertion against its own copy, and that assertion plus
convention is the whole of the enforcement: nothing else keeps the values
equal, which is exactly why the coordinated-raise rule below exists. The
admission bound decides what old data is *admitted*;
the listing window decides what old data is *discoverable*. If you raise the
admission lag alone, you admit and acknowledge records that the listing
window can then fail to find on any non-token query.

Raising the admission lag for a signal or tenant is therefore legal only
together with the catalog-side listing-window config: widen the catalog
window first, then the admission bound. Lowering the admission lag is always
safe. This is the same coordinated-config discipline `max_flush_lifetime`
follows between writers and folders
([catalog-and-mvcc.md](../catalog-and-mvcc.md), "Config discipline"); the
normative statement of this rule lives in
[consistency-model.md](../consistency-model.md#late-and-skewed-data).

## Fleet-wide enforcement via reconciliation

Every configured limit is a **fleet-wide** total, not a per-process one.
The value you set is what the whole fleet enforces, regardless of how many
ingest replicas run behind the load balancer: you do not divide a target by
replica count.

The hot-path check stays per-process and sub-microsecond; there is no S3
round-trip on any admission decision. Instead each process reconciles its
effective caps off the hot path on a fixed interval, reading every sibling's
usage from object storage and adjusting the number its local check compares
against. The interval is set by `--admission-reconcile-interval` (default
10s). A zero or unparseable duration fails startup rather than reconciling in
a tight loop, and reconciliation runs only in the ingest-serving modes (`all`,
`gateway`).

The two kinds of cap converge differently:

- **Count caps** (`max_active_series`, `max_active_streams`) are a safe
  overestimate. Reconciliation sums each replica's own active set without
  deduplicating a series that two replicas both hold, so it can only drive a
  replica to reject *sooner* than the configured cap, never admit more than
  it. The fleet total is bounded within one reconciliation interval's worth
  of admission per process.
- **Rate caps** (`ingest_bytes_per_sec`, `series_creation_rate_per_sec`)
  converge to the configured cap as a fleet-wide total by equal-share
  division: each of the `N` live processes enforces `cap / N`, so their sum
  is at most the configured cap. When a replica joins
  or leaves, the fleet total settles back to the cap within one interval.

Until a process's first reconciliation cycle completes (and briefly after a
replica count change), enforcement falls back to per-process behavior, so a
newly started fleet can transiently admit above the cap by a bounded,
self-correcting margin. Shortening the interval tightens that window at the
cost of more reconciliation requests; lengthening it does the reverse.

## Flush cadence: cost, not rejection

The flush cadence is not an admission rejection; it tunes PUT cost. The
shipped defaults are `--max-flush-delay` 2 s, `--max-flush-delay-idle` 40 s,
and `--min-flush-bytes` 256 KiB, and all three apply to all three ingest
pipelines (metrics, logs, spans).

The fast 2 s age trigger fires only when the flush window holds a strict-mode
waiter or the buffer already holds at least `--min-flush-bytes`; an
otherwise-idle buffer waits the slower `--max-flush-delay-idle` instead. That
is what drops the volume-independent PUT floor for a buffered-mode trickle
tenant by roughly 20x. Strict-mode acknowledgement latency is unchanged: a
strict-mode waiter is always a priority flush.

All three are `ravel-server` flags rather than fixed shard defaults, and they
must be set as a set: startup refuses one or two of the three, and rejects a
zero or unparseable duration, or a `--min-flush-bytes` of `0`. Raising
`--max-flush-delay` also costs strict-mode acknowledgement latency directly,
and it is validated at startup against a derived ceiling, so it is the lever
of last resort; [cost-model.md](cost-model.md) has the measured effect and the
order to reach for the other levers in.

## Spans have no series-count cap

Metrics series and log streams both have stable identity (`SeriesId`,
`LogStreamId`) and get both the active-count cap and the series-creation-rate
cap. Spans have no stable series identity (`trace_id` is sender-chosen and
naturally unbounded), so spans are bounded by the body-size, byte-rate, and
event-time-skew layers only. This is stated, not silent: there is no
per-tenant span-count cap to configure.

## Per-tenant usage counters

The admission controller's per-(tenant, signal) counters are rendered on
`GET /metrics`, each carrying `mode`, `tenant_hash`, and `signal` labels:

| Metric | Type | What it counts |
|---|---|---|
| `ravel_admission_active_series` | gauge | Series (metrics) or streams (logs) tracked for the active cap. |
| `ravel_admission_admitted_total` | counter | Requests admitted past the byte-rate layer. |
| `ravel_admission_admitted_bytes_total` | counter | Bytes charged against the byte-rate layer, which for a compressed request is the decompressed size. |
| `ravel_ingest_wire_bytes_total` | counter | Request-body bytes as they arrived on the wire. Its ratio to the row above is a tenant's effective compression factor. |
| `ravel_admission_rejected_total` | counter | Rejections, with a fourth `reason` label: `byte_rate`, `series_rate`, `series_cap`, `clock`. |
| `ravel_admission_reconciliation_failures_total` | counter | Reconciliation cycles whose sibling-snapshot read failed. The last-known threshold stays in force, so this says fleet-wide accuracy is degrading, not that ingest is down. |

By default every tenant's rows fold into `tenant_hash="other"`, so the family's
cardinality is bounded by signal and reason rather than by tenant count.
`--metrics-tenant-labels` renders the real per-tenant `tenant_hash` instead.
Turn it on only where the scrape network is trusted: the `/metrics` route is
unauthenticated, and per-tenant labels let a scraper enumerate tenant hashes
and their traffic.

## Background

The per-tenant admission mechanism is
[ADR-0051](../adrs/0051-tenant-admission-control.md); the fleet-global
reconciliation above it is
[ADR-0057](../adrs/0057-fleet-global-admission-reconciliation.md); the query
bytes-scanned budget and the concurrent-query ceiling are ADR-0061; the
flush-cadence defaults are ADR-0076 decision 4.
