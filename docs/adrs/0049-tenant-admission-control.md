# ADR-0049: Tenant admission control and ingest-time correctness

Status: Accepted (2026-08-02)

## Context

ADR-0009 decided that per-tenant quotas (series, bytes per second) are
"enforced at gateway/frontend from config; hard limits precede
allocation". Nothing implements that sentence. The adversarial
architecture review (docs/reviews/adversarial/RAVEL-ADVERSARIAL-REVIEW.md,
findings S3-01/S4-05, risk B5, experiment L10) verified against the code
that the only ingest-side limits are per-request structural caps in
`ravel-otlp` (`IngestLimits`, `LogIngestLimits`, `TraceIngestLimits`) and
the decompression caps in OTAP (16 MiB) and Remote Write (64 MiB). There
is no active-series cap, no series-creation-rate limit, and no ingest
byte-rate quota anywhere; one tenant emitting a fresh label set per point
drives fleet-wide object count, PUT spend, catalog size, and query
fan-out, with no per-tenant counter to attribute it (S3-08).

The same epic covers a cluster of ingest-time correctness gaps the review
rated alongside the quota gap:

- **S2-01 (CRITICAL).** Metrics enforce event-time skew bounds at
  admission (`checked_event_ts`, crates/ravel-otlp/src/normalize.rs:729;
  ADR-0010 §8): event time outside
  `[ingest_ts - max_ingest_lag, ingest_ts + max_future_skew]` is rejected
  with a typed partial-success reason. Logs and spans have no such check:
  `logs_normalize` uses `ingest_ts_ns` only as a fallback timestamp, and
  `traces_normalize` passes sender timestamps through unchecked. The
  catalog listing window (crates/ravel-catalog/src/catalog.rs:344,
  docs/consistency-model.md "Late and skewed data") is provably complete
  only under those admission bounds, and logs and spans resolve through
  the identical window formula. A future-skewed record is stored and
  acked but invisible to every non-token query, and because retention
  anchors expiry on `max_event_ts`, one `now + 100y` record makes its
  bucket unexpirable forever.
- **S2-02 / S1-07 (CRITICAL).** Query-time dedup exists for metrics only
  (`(series_id, ts)`). Logs and spans have no duplicate identity on any
  read path, so a client retry after a lost ack (crash-matrix row 3, a
  normal event) doubles visible log rows and spans with no signal.
  docs/consistency-model.md documents at-least-once delivery honestly,
  but its "identical duplicates are harmless" framing is true for
  metrics only and is widely over-read.
- **S1-12.** All three shard actors map a non-representable flush-open
  clock reading to ingest hour bucket 0 (`u32::try_from(...).unwrap_or(0)`
  at crates/ravel-ingest/src/shard.rs:423, log_shard.rs:349,
  span_shard.rs:317 — the review cited only the metrics shard; the same
  line exists in all three). A clock that goes bad between admission and
  flush open produces a strict-acked commit in the 1970 bucket that no
  realistic query window ever lists.
- **S4-11.** The OTLP-HTTP handlers decode from an axum `Bytes` body with
  no explicit `DefaultBodyLimit` on the routes; the bound is the
  framework default, undocumented, and can regress silently.
- **S2-06 / S3-05.** The unconditional 500 ms flush age trigger gives
  every tenant a PUT floor set by cadence, not volume: up to ~16 PUTs/s
  across 4 shards per signal regardless of data (~$200/month/tenant in
  PUT requests alone).
- **S4-07.** `max_series` on the query path is enforced after the full
  `by_id` map is materialized (crates/ravel-query/src/engine.rs:352-364),
  so the memory the cap exists to bound is allocated before the cap
  rejects.

Where the review is stale, this ADR designs for what is on `main` now,
not for what the review saw:

- A `/metrics` endpoint exists (services/ravel-server/src/metrics.rs,
  ADR-0044 §4): a hand-written Prometheus exposition renderer with a
  compile-time-closed `Label` allowlist (`tenant_hash`, `signal`, `mode`,
  `op`, `error_kind`, `workload_class`, `level`) and a `TenantHashLabel`
  that folds every unconfigured tenant into `other` so per-tenant
  cardinality is bounded by configured-tenant count, not traffic. The
  review's "Ravel exports no runtime telemetry at all" is no longer true;
  this ADR reuses that renderer and adds no second registry.
- Per-query cost accounting types exist (`QueryAccounting` in
  crates/ravel-types/src/accounting.rs, ADR-0044). Measurement is in;
  enforcement was explicitly deferred by ADR-0044 to a later ADR. The
  read-side "query bytes scanned" quota named by ADR-0009 therefore stays
  out of scope here and lands in that enforcement ADR.
- ADR-0044 blocked per-tenant `/metrics` series on an authentication
  decision for the route. This ADR makes that decision (below).

## Decision

### 1. Admission layers, and where each sits relative to allocation

Admission is enforced in `ravel-server` gateway handlers and in a new
`ravel-ingest` admission component, in this order on every ingest path
(OTLP HTTP, OTLP gRPC, OTAP, Remote Write, logs, spans). Each layer runs
before the allocation it exists to bound; nothing reaches a shard buffer
without passing all of them.

1. **Request body size** — at the transport, before the body is buffered.
   Explicit `DefaultBodyLimit` on `/v1/metrics`, `/v1/logs`, `/v1/traces`
   (default 16 MiB), `max_decoding_message_size` (16 MiB) on every tonic
   service, an explicit compressed-body cap on `/api/v1/write` (16 MiB)
   ahead of its existing 64 MiB decompressed cap. OTAP's existing
   decompression caps stay. Rejection: HTTP 413, gRPC
   `RESOURCE_EXHAUSTED`. Per-request.
2. **Ingest byte rate** — per tenant, charged on wire body bytes after
   tenant resolution and before decode, so over-rate bytes cost one
   buffered body and nothing else. Token bucket
   (`ingest_bytes_per_sec` + burst). A request whose size exceeds the
   available tokens is rejected whole without consuming tokens.
   Rejection: HTTP 429 with `Retry-After`, gRPC `RESOURCE_EXHAUSTED`.
   Per-request.
3. **Structural and event-time bounds** — in normalization, per point /
   record / span, via the existing typed-rejection partial-success
   machinery. This is where the new log/span skew bounds land (§4).
4. **Series admission** — per tenant, between normalization and
   `IngestRouter::write`, before any shard-buffer allocation (this is
   ADR-0009's "hard limits precede allocation" point):
   - **Series-creation rate** (`series_creation_rate_per_sec` + burst):
     the batch's distinct new-series demand is computed first; if it
     exceeds available tokens the whole request is rejected 429 without
     consuming tokens and without admitting any of the batch. Whole-
     request, because a partially admitted batch that the client retries
     re-ingests the admitted remainder, and for logs that duplication is
     user-visible (S2-02). Retryable rejections are therefore always
     all-or-nothing.
   - **Active-series cap** (`max_active_series`): points that would
     create a series beyond the cap are rejected per-series through
     partial success; points for already-active series are admitted.
     Per-series, because a cap breach is not retryable-soon and OTLP
     partial-success semantics instruct the client not to resend the
     rejected items, so no retry-duplication arises.

   Metrics series and log streams both have stable identity (`SeriesId`,
   `LogStreamId`) and get both controls. Spans have no stable series
   identity (`trace_id` is sender-chosen and naturally unbounded), so
   spans are bounded by layers 1–3 only; this is stated, not silent.

**Rejection status codes, per limit:**

| Limit | Scope | OTLP HTTP | OTLP/OTAP gRPC | Remote Write |
|---|---|---|---|---|
| Body size | request | 413 | `RESOURCE_EXHAUSTED` | 413 |
| Byte rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Series-creation rate | request | 429 + `Retry-After` | `RESOURCE_EXHAUSTED` | 429 + `Retry-After` |
| Active-series cap | per series | 200 + partial success | OK + partial success | 200 (see below) |
| Event-time skew | per point | 200 + partial success | OK + partial success | 400 only if all samples rejected, else 200 + written-count |

Remote Write has no partial-success message: per-series and per-point
rejections admit the rest of the batch and return 2xx (RW 2.0 additionally
carries the true count in `X-Prometheus-Remote-Write-Samples-Written`),
because a non-2xx makes Prometheus retry or drop the whole batch
including its admitted samples. The drop is observable through the
per-tenant rejection counters (§6). 429 is reserved for rate limits,
where a retry genuinely will succeed later, matching Prometheus
backoff semantics.

### 2. Active-series accounting: exact, per process, bounded by the cap

An `AdmissionController` in `ravel-ingest` holds, per (tenant, signal),
an exact `HashSet` of series/stream ids over two rotating one-hour
epochs: a series is active if seen in the current or previous epoch.
Exact, not sketched, because the workspace invariant is exact semantics
by default and approximation must be opt-in and visible; the memory
argument for a sketch does not apply here, since the set stops growing at
`max_active_series` — the cap itself bounds the tracker at roughly
16 bytes × cap × 2 epochs per tenant.

Enforcement state is **per process**. With N ingest replicas the
fleet-wide effective bound is N × the configured limit. This is stated
in the limits documentation rather than hidden: Ravel's compute processes
are disposable and share no state except object storage, and putting an
S3 round-trip into every admission decision to make the cap global would
add latency and request cost to the hottest path in the system to defend
against a factor the operator already controls (replica count).

### 3. Configuration and per-tenant overrides

A TOML limits file, `--limits-file <path>`, with a `[defaults]` table and
`[tenants.<tenant-id>]` override tables; absent file means shipped
defaults. Loaded at startup; changing limits is a restart, consistent
with every other per-tenant flag today (`--retention-tenant`,
`--tenant-token`). Enforcement is on by default with finite shipped
defaults — ADR-0009 promised admission control, so its absence is the
bug, not its presence; a tenant needing no limit sets an explicit
`unlimited = true`, which is visible in config review rather than being
the silent default.

Shipped defaults (all per tenant, all overridable):

| knob | default |
|---|---|
| max_request_body_bytes | 16 MiB |
| ingest_bytes_per_sec / burst | 32 MiB/s / 64 MiB |
| max_active_series (metrics) | 1,000,000 |
| max_active_streams (logs) | 1,000,000 |
| series_creation_rate_per_sec / burst | 10,000/s / 100,000 |
| max_future_skew (all signals) | 10 m |
| max_ingest_lag (all signals) | 2 h |
| max_flush_delay_idle | 10 s |
| min_flush_bytes | 64 KiB |

### 4. Event-time skew bounds for logs and spans

Logs and spans mirror the metrics `checked_event_ts` path exactly: the
bound is checked in `logs_normalize` and `traces_normalize` against the
receiver's admission-time clock, rejecting with typed reasons through the
existing partial-success machinery. They do not anchor stored timestamps
on ingest time: rewriting or clamping a sender's event time would be
silent data corruption (the exactness invariant), where rejection is
visible and reportable.

- **Logs:** the resolved `ts_ns` (after the existing observed-time and
  ingest-time fallbacks, which are trivially in bounds) must lie in
  `[ingest_ts - max_ingest_lag, ingest_ts + max_future_skew]`.
- **Spans:** `end_ts_ns` is bounded by `max_future_skew` and
  `start_ts_ns` by `max_ingest_lag`, because the commit record advertises
  `[min start_ts, max end_ts]` and the listing window is sound only if
  both advertised bounds are admission-bounded. `end_ts < start_ts` is
  rejected outright. Consequence: a span longer than `max_ingest_lag`, or
  reported later than that after it started, is rejected at admission.

The defaults are the same 10 m / 2 h the metrics path uses, because the
catalog listing window (crates/ravel-catalog/src/config.rs) is one shared
`max_ingest_lag_ns`, not per-signal. Raising the admission lag for a
signal or tenant is legal only together with the catalog-side window
config, and the limits documentation states this coordinated-raise rule
the same way docs/consistency-model.md states it for
`max_flush_lifetime`.

### 5. Idempotency for logs and spans

Two-part decision:

1. **The documented contract is at-least-once.** docs/consistency-model.md
   and the log/span query documentation are amended to say, in so many
   words, that log and span counts are at-least-once and a client retry
   after a lost ack is user-visible duplication — removing the
   "duplicates are harmless" over-read, which is true for metrics only.
2. **An opt-in client idempotency key closes the window where it
   matters.** Log and span ingest surfaces accept an optional
   `x-ravel-idempotency-key` HTTP header / gRPC metadata entry (opaque,
   ≤128 bytes). For a keyed request the gateway, after a successful
   flush, writes a marker object before releasing the ack:

   ```
   data PUT → commit PUT → marker PUT (CreateIfAbsent) → ack
   ```

   A retry of a keyed request first consults the marker (one prefix LIST)
   and, on a hit inside the dedup window, replays the stored receipt
   without re-ingesting. The OTLP protobuf schemas are untouched: the key
   travels as transport metadata, never in a proto field.

   Marker keyspace, additive to the frozen key layout per the
   format-change procedure (new prefix, no reshaping of any existing
   key):

   ```
   t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm
   keyhash = hex(blake3("ravel-idem-v1" || tenant_id || client_key)[0..16])
   ```

   docs/catalog-and-mvcc.md is amended in the same change that adds the
   key builder. The marker body is versioned and checksummed (`RIDM`
   magic, u16 version = 1, crc32c over the payload, payload = the
   serialized write receipt); a corrupt or truncated marker is a typed
   error treated as a miss (fail-open to at-least-once, never a lost
   ack), counted and exported. There is no dual-reader question: the
   prefix is new, no old data exists under it, and no existing read,
   resolve, or sweep path lists it (commit resolution lists `c/…`, the
   orphan sweep lists `l0/…`; the fail-loud unknown-key rule applies to
   the `c/` prefix only). Markers older than the dedup window (default
   24 h, from the `ingest_hour` in the file name) are deleted by a new
   stateless sweep rule in `ravel-maintain`; `ravel-cli` gets an
   inspector for the new object class.

   Honest residuals, documented with the feature: a crash after the
   commit PUT but before the marker PUT still yields a duplicate on
   retry; two concurrent requests with the same key can both ingest
   (the window targets sequential retry, the actual failure mode); and
   unkeyed requests get plain at-least-once. Keyed requests pay one LIST
   plus one PUT.

### 6. Per-tenant usage export

The admission controller's per-(tenant, signal) counters are rendered by
a new family function beside `render_ingest_family` in the existing
`/metrics` renderer — no new registry, no metrics crate, exactly the
extension seam the renderer's module docs define:

- `ravel_admission_active_series{tenant_hash, signal}` (gauge)
- `ravel_admission_admitted_total{tenant_hash, signal}` and
  `ravel_admission_admitted_bytes_total{tenant_hash, signal}`
- `ravel_admission_rejected_total{tenant_hash, signal, reason}`, with
  `reason` a new closed `Label` variant over
  `{body_size, byte_rate, series_rate, series_cap, skew, structural}`

Cardinality is bounded by construction: (configured tenants + `other`) ×
3 signals × 6 reasons, all through the compile-time-closed `Label` enum.
ADR-0044 blocked per-tenant series on an auth decision for the
unauthenticated `/metrics` route; the decision here is an explicit
opt-in flag, `--metrics-tenant-labels` (default off, everything folds to
`tenant_hash="other"`), turned on only where the operator attests the
scrape network is trusted. Per-tenant attribution therefore costs one
deliberate flag, not a new auth subsystem.

### 7. Ingest-time correctness fixes carried with the epic

- **Hour-bucket fail-loud (S1-12):** in all three shard actors a
  non-positive or non-representable `flush_open_ns` fails the flush with
  `WriteError::SegmentBuild` (typed, waiters errored, client retries)
  instead of `unwrap_or(0)`.
- **Idle-aware age trigger (S2-06/S3-05):** the 500 ms age trigger fires
  only when the flush window contains a strict-mode waiter or the buffer
  holds at least `min_flush_bytes`; otherwise `max_flush_delay_idle`
  (default 10 s) applies. Strict-mode ack latency is unchanged;
  buffered-mode trickle tenants' PUT floor drops ~20x. A strict-mode
  trickle tenant still pays the floor — that is the price of its ack
  latency, and the per-tenant `max_flush_delay` override is the operator
  lever for tenants that prefer cost over latency.
- **Incremental `max_series` (S4-07):** the query engine enforces
  `max_series` during `by_id` construction, aborting the loop at the
  bound, so peak memory is bounded by the cap rather than by the match.

### Relation to ADR-0009

This ADR **implements** ADR-0009's admission-control decision (the
"per-tenant quotas … hard limits precede allocation" bullet) as written —
enforcement at the gateway, rejection before allocation — and makes it
concrete where ADR-0009 was silent: the specific limits, their rejection
semantics and status codes, per-signal applicability, and the per-process
enforcement scope. It does not supersede or amend ADR-0009, which remains
Accepted. The one part of that bullet not delivered here is the read-side
query-bytes-scanned quota, which ADR-0044 deliberately staged behind its
measurement layer and which lands in ADR-0044's follow-up enforcement
ADR, not this one.

## Rejected alternatives

1. **Globally consistent (S3-coordinated) quota enforcement.** Shared
   counters CAS'd in object storage, so the cap holds fleet-wide
   regardless of replica count. Rejected: it puts an S3 round-trip into
   every admission decision on the hottest path in the system, adds a new
   mutable-object churn class, and defends against a multiplier (replica
   count) the operator already controls. Per-process enforcement with the
   N× bound documented is honest and has zero hot-path cost.
2. **Approximate cardinality tracking (HyperLogLog or similar) for the
   active-series set.** Rejected: exactness is the workspace default and
   approximation must be opt-in and visible; and the memory argument for
   a sketch is void because the exact set is bounded by the very cap it
   enforces.
3. **Anchoring log/span timestamps on ingest time (clamping event time
   into bounds) instead of rejecting.** Rejected: silently rewriting a
   sender's event time is data corruption of the plausible-wrong-result
   class the review rates most dangerous; a typed rejection is visible to
   the sender and countable by the operator.
4. **Content-hash query-time dedup for logs/spans (dedup by
   (stream_id, ts, body-hash) as metrics dedup by (series_id, ts)).**
   Rejected: two identical log lines in the same nanosecond are
   legitimate distinct events for logs (metrics dedup is safe only
   because `(series_id, ts)` is a sample's entire identity), so this
   trades visible duplicates for silently dropped real records — a worse
   wrong-result class; and it charges every read a per-record hash
   forever to fix a write-side fault.
5. **Mandatory (rather than opt-in) idempotency keys, or a
   pre-ingest pending-marker protocol for exactly-once.** Rejected:
   mandatory keys break every stock OTLP exporter; a pending/complete
   two-phase marker gives exactly-once against concurrent replay but
   needs TTL'd lock recovery (a pending marker whose writer died blocks
   the key until a timeout), which is a distributed-lock protocol grown
   from a dedup cache. The post-commit marker suppresses the actual
   failure mode (sequential retry after a lost ack) at one PUT of cost
   and degrades to documented at-least-once in the corners.
6. **429 (or 400) whole-request rejection for active-series cap
   breaches on Remote Write.** Rejected: 429 makes Prometheus retry a
   batch that can never succeed while the tenant is at cap (retry storm,
   and the batch's under-cap samples are delayed indefinitely); 400 makes
   it drop the whole batch including admitted samples. Partial admission
   with 2xx plus the RW 2.0 written-count header and rejection counters
   loses only the over-cap series and keeps the sender healthy.
7. **A second metrics registry (adopt the `prometheus`/`metrics` crate)
   for per-tenant usage.** Rejected: ADR-0044 already rejected
   registry-style label allocation as the mechanism by which
   observability systems acquire unbounded self-telemetry, and the
   existing renderer's closed `Label` enum is the cardinality bound this
   epic itself requires (S3-08). One registry, extended at its documented
   seam.
8. **Enforcing quotas inside the shard actors instead of at the
   gateway.** Rejected: by the time a point is in a `ShardMsg` the
   allocation ADR-0009 orders the rejection to precede has already
   happened (normalized point vectors, channel slots, buffer merge), and
   per-shard enforcement fragments one tenant's budget across shards,
   making rejection order dependent on shard routing.

## Consequences

- Every ingest path gains two cheap checks (a token-bucket charge and a
  set lookup per distinct series in the batch) between tenant resolution
  and buffer admission. No object format, no proto schema, no series
  identity, and no commit token changes. The only frozen-contract change
  is the additive `idem/` keyspace, carried out under the format-change
  procedure (versioned checksummed body, docs/catalog-and-mvcc.md amended
  with the key-builder change, property tests over corrupt/truncated
  markers, CLI inspector).
- Experiment L10 (one tenant drives fleet cost with no cap) passes:
  series count is capped by `max_active_series`, PUT-driving volume by
  the byte-rate bucket, and the idle age trigger removes the
  volume-independent PUT floor for buffered-mode tenants. Experiment L6
  (retry doubles log rows with no signal) passes for keyed requests
  (count stays flat) and is a documented, counted contract for unkeyed
  ones.
- Fleet-wide limits are per-process × replicas; operators sizing a hard
  business cap divide by ingest replica count. Documented, not silent.
- Long-running spans (duration > `max_ingest_lag`) are rejected at
  admission under the default 2 h window; deployments that need them
  raise the lag together with the catalog window config.
- Senders with skewed clocks now see log/span rejections where they saw
  silent acceptance; that is the point, but it is a behavior change for
  any pipeline that was (unknowingly) storing unqueryable records.
- Per-tenant `/metrics` series exist only behind `--metrics-tenant-labels`;
  fleets that leave it off keep today's `other`-folded exposition and
  still get fleet-total admission counters.
- The dual-compaction-record overlap for signals without dedup (the
  read-side half of S2-02) is not addressed here: it is Ravel-caused
  duplication, not client-caused, and belongs to the compaction/read
  path, tracked in the review ledger, not to admission control.
- Read-side cost enforcement (bytes-scanned budgets) remains staged
  behind ADR-0044's measurement, unchanged by this ADR.
