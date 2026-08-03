# Admission limits

Ravel enforces per-tenant admission limits on every ingest path (OTLP HTTP,
OTLP gRPC, OTAP, Remote Write, logs, spans). The limits exist so that one
tenant cannot drive fleet-wide object count, PUT spend, catalog size, or
query fan-out without a per-tenant bound to attribute and cap it
([ADR-0051](../adrs/0051-tenant-admission-control.md)). Each layer runs
before the allocation it exists to bound: nothing reaches a shard buffer
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
Enforcement is on by default with finite defaults: ADR-0009 promised
admission control, so its absence is the bug, not its presence. A tenant
that genuinely needs no cap on a knob sets it explicitly to the string
`"unlimited"` on that knob's own key (e.g. `max_active_series =
"unlimited"`, `ingest_bytes_per_sec = "unlimited"`, as shown above), which
is visible in config review rather than being the silent default.

## The limits

All limits are per tenant. Rates are token buckets (a sustained `per_sec`
plus an instantaneous `burst`).

| Knob | Shipped default | Config key |
|---|---|---|
| Request body size | 16 MiB | not configurable (fixed transport cap) |
| Ingest byte rate / burst | 32 MiB/s / 64 MiB | `ingest_bytes_per_sec` / `ingest_byte_burst` |
| Active-series cap (metrics) | 200,000 | `max_active_series` |
| Active-stream cap (logs) | 200,000 | `max_active_streams` |
| Series-creation rate / burst | 10,000/s / 100,000 | `series_creation_rate_per_sec` / `series_creation_burst` |
| Event-time future skew | 10 m | not configurable (`ravel-otlp` default) |
| Event-time ingest lag | 2 h | not configurable (`ravel-otlp` default) |
| Idle flush delay | 10 s | not configurable (shard default) |
| Min flush bytes | 64 KiB | not configurable (shard default) |

The active-series/stream default is 200,000, not the 1,000,000 ADR-0051
section 3 names: issue #491 measured the real per-entry cost of the exact
two-epoch tracker at 35-56 bytes (not the ~16 the ADR assumed), so
`ravel-server` ships the lower figure to keep the worst-case tracker
footprint near 27-43 MiB per fully active tenant instead of 140-224 MiB.
The `--limits-file` raises it per tenant where the memory is available.

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

Metrics already enforced event-time skew at admission; logs and spans now
do too, through the same typed partial-success machinery. A record whose
event time falls outside `[ingest_ts - max_ingest_lag, ingest_ts +
max_future_skew]` is rejected, never clamped: rewriting a sender's event
time would be silent data corruption, where a rejection is visible and
countable. For the full explanation of why these bounds exist (they are what
makes the catalog listing window sound), see
[ingest.md](ingest.md#event-time-skew-bounds).

For spans specifically: `end_ts` is bounded by `max_future_skew` and
`start_ts` by `max_ingest_lag`, and `end_ts < start_ts` is rejected outright.
A span longer than `max_ingest_lag`, or reported more than that after it
started, is rejected at admission under the default 2 h window.

## Raising max_ingest_lag: a coordinated change

`max_ingest_lag` is one shared bound, not a per-signal one, in the sense
that matters operationally: the admission checks (metrics, logs, spans --
each its own crate, `ravel-otlp`'s `limits.rs`/`logs_limits.rs`/
`traces_limits.rs`) and the catalog listing window
(`crates/ravel-catalog/src/config.rs`) each hold their own
`max_ingest_lag_ns` constant, duplicated rather than shared by reference
(`ravel-maintain`'s own copy carries an explicit "MUST be kept in sync"
comment and a startup equality assertion against it). This is exactly why
the coordinated-raise rule below exists: nothing in code enforces the
values staying equal, only convention and this assertion. The admission
bound decides what old data is *admitted*;
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

## Per-process enforcement and replica count

Admission state (the active-series/stream sets and the token buckets) is
**per process**, not fleet-global. With N ingest replicas the effective
fleet-wide bound on a cap is N times its configured value. This is a
deliberate trade-off, not an oversight: Ravel's compute processes are
disposable and share no state except object storage, and putting an S3
round-trip into every admission decision to make a cap globally exact would
add latency and request cost to the hottest path in the system to defend
against a factor (replica count) the operator already controls. An operator
sizing a hard business cap divides the target by the ingest replica count.

## Flush cadence: cost, not rejection

`max_flush_delay_idle` (10 s) and `min_flush_bytes` (64 KiB) are not
admission rejections; they tune PUT cost. The 500 ms flush-age trigger fires
promptly only when the flush window holds a strict-mode waiter or the buffer
has reached `min_flush_bytes`; an otherwise-idle buffer waits the slower
`max_flush_delay_idle` before flushing. This drops the volume-independent PUT
floor for buffered-mode trickle tenants by roughly 20x. Strict-mode ack
latency is unchanged: a strict-mode waiter is always a priority flush. These
two values are shard defaults today and are not server-flag configurable.

## Spans have no series-count cap

Metrics series and log streams both have stable identity (`SeriesId`,
`LogStreamId`) and get both the active-count cap and the series-creation-rate
cap. Spans have no stable series identity (`trace_id` is sender-chosen and
naturally unbounded), so spans are bounded by the body-size, byte-rate, and
event-time-skew layers only. This is stated, not silent: there is no
per-tenant span-count cap to configure.

## Per-tenant usage counters

The admission controller keeps per-(tenant, signal) counters (active series,
admitted/rejected totals, rejected bytes, rejections by reason) in-process.
As of this writing they are not exposed at `GET /metrics`: no admission
snapshot is wired into the metrics renderer. Per-tenant attribution behind
an opt-in `--metrics-tenant-labels` flag, and folding untracked tenants into
`tenant_hash="other"`, is designed in ADR-0051 section 6 (task EB-6) but not
yet implemented -- there is currently no admission-usage surface at
`/metrics` at all, fleet-total or per-tenant.
