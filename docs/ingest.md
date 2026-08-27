# Ingest Pipeline Design (Phase 1)

Implementer contract for `ravel-ingest` and the ingest side of
`ravel-server`. See ADR-0001/0002 and docs/consistency-model.md for the
semantics this design must deliver.

## Structure

```
gateway handler (axum / tonic)
  -> auth + tenant resolve + admission limits (ravel-otlp::limits)
  -> normalize (ravel-otlp) -> Vec<NormalizedPoint> + rejects
  -> IngestRouter::write(tenant, points, mode) -> WriteReceipt
```

Remote Write (`POST /api/v1/write`, ADR-0015) is a second gateway handler
in front of the same router, decode/normalize swapped for the RW-specific
crate:

```
POST /api/v1/write (axum)
  -> auth + tenant resolve (shared TenantResolver)
  -> negotiate RW1 vs RW2 (Content-Type, then X-Prometheus-Remote-Write-Version; else 415)
  -> snappy-decompress (capped) + protobuf decode (ravel-remote-write) -> ResolvedRequest
  -> normalize_resolved (ravel-remote-write) -> Vec<NormalizedPoint> + rejects
  -> IngestRouter::write(tenant, points, mode=Strict) -> WriteReceipt
```

This surface always passes `mode=Strict`: it never reads
`x-ravel-ingest-mode`, since a Remote Write sender treats any 2xx as
durable and drops its WAL entry on that basis. A commit token still comes
back in `x-ravel-commit-token`; RW2 responses additionally carry
`X-Prometheus-Remote-Write-Samples-Written` (and the histogram- and
exemplar-written counterparts, currently always 0). Malformed input is
400; shard backpressure and retryable store failures are mapped to a
retryable 5xx with `Retry-After`.

IngestRouter owns `shard_count` shard handles. It groups points by
`shard_for(series_id, shard_count)` and sends one message per shard:

```
ShardMsg::Write { tenant, points: Vec<NormalizedPoint>, ack: Option<oneshot::Sender<Result<CommitToken, WriteError>>> }
```

Channel: `tokio::sync::mpsc` bounded (default 256 messages per shard).
`send` awaiting on a full channel IS the backpressure mechanism; the gateway
holds the request open while it awaits.

What actually bounds gateway memory is a process-wide in-flight
ingest-request ceiling, `--max-inflight-ingest-requests`
(default 1024, 0 disables it). One `tokio::sync::Semaphore`, shared across
every OTLP metrics/logs/traces and Remote Write handler, on both the public
and mTLS listeners and both the HTTP and gRPC transports. A request that does
not get a permit is shed immediately, never queued: HTTP 429 with
`Retry-After`, gRPC `RESOURCE_EXHAUSTED`. Query, health, and `/metrics` are
not covered.

Worst-case memory bound at the default: each in-flight request holds at most
one decoded request body. The largest single-request cap on any covered
route is Remote Write's post-decompression limit
(`MAX_DECOMPRESSED_PAYLOAD_BYTES`, 64 MiB); OTLP's per-request cap is smaller
(`MAX_DECODED_MESSAGE_BYTES`/`MAX_REQUEST_BODY_BYTES`, 16 MiB). So the
process-wide worst case is 1024 * 64 MiB = 64 GiB if every in-flight slot
happens to be a max-size Remote Write request, or 1024 * 16 MiB = 16 GiB if
all are OTLP. This is a coarse ceiling, not a target: it bounds the worst
case the operator is exposed to, not typical usage, which is why the default
is sized for concurrency headroom rather than to fit a specific memory
budget. Operators tune `--max-inflight-ingest-requests` down to bring the
worst case in line with available memory.

### Generation live switch (ADR-0052)

`shard_count` is no longer fixed for a router's whole life: it is
generation-versioned per (tenant, signal) in the provisioning record
(`crates/ravel-catalog/src/provisioning.rs`). Each router holds a
`GenerationSwitch` that keeps one shard-actor set per distinct active
`shard_count` and routes a write with the count of the latest generation whose
`activation_hour` is at or before the write's wall-clock hour
(`ravel_catalog::active_shard_count`). A reshard's activation spawns the new
generation's set and routes subsequent writes to it while the old set keeps
draining and flushing under its original shard indices; no data is moved or
re-keyed. A router routes on its cached generation view while it is younger than
the refresh interval `C` (default 60s); once older, it re-reads the provisioning
record before routing and fails the flush closed (typed error, the
`ravel_ingest_stale_provisioning_flushes_total` counter) if that re-read cannot
complete, so it never routes on a stale view. Operators append a generation with
`ravel-cli provision reshard`; the record enforces the append-only,
future-activation mutation model. See ADR-0052 for the full design.

#### Bounded grace window on a stuck re-read

Fail-closed staleness has an availability failure mode: if the store is slow
or unreachable for longer than `C`, every re-read attempt fails and the router
fails every subsequent flush closed for as long as that lasts -- under
sustained store latency, a total ingest outage rather than a degraded one.
`GenerationSwitch::try_grace_extend` bounds that cost. When a re-read cannot
complete, the router does not immediately fail the flush; it asks the switch
whether continuing to route on the last-known-good cached view is still
provably correct. Continue-on-stale is safe only when the cached generation's
validity horizon has not been crossed AND no pending generation change is
knowable: concretely, `hour_of(now_ns) < hour_of(view.refreshed_at_ns) +
min_lead_hours(C)`, where `min_lead_hours(C) = ceil(C) + 1` reuses the ADR's
own reshard lead-time floor in reverse -- a generation appended after the
router's last successful refresh cannot activate before that horizon, so a
cached view is provably still exactly what a fresh read would return for any
hour strictly before it. Once the horizon is crossed, an unseen append becomes
possible and the switch returns no set, so the flush fails closed exactly as
it did before this fallback existed: the grace window degrades cost under
store latency, it never converts a genuine shard-count change into silent
wrong-routing. A router routing through this window increments
`ravel_ingest_grace_extended_stale_flushes_total`, distinct from
`ravel_ingest_stale_provisioning_flushes_total` (which counts a flush that
still failed closed), so sustained store degradation is visible as "degraded
but available" rather than indistinguishable from an outage.

#### Decrease and the straggler slack window `S`

On a decrease (say 8 shards down to 4), a write routed under the retiring,
larger generation lands in a shard index the successor's range does not cover
(e.g. shard 6). Such a straggler stays findable: a flush pins its ingest-hour
bucket at flush-open, but its records were routed up to `max_flush_delay`
earlier and the flush lives at most `max_flush_lifetime`, plus inter-writer
clock skew, so a write routed just before the activation can land in an
ingest-hour bucket just after it. The read side keeps the retiring generation's
count in the scan set for `S = ceil(max_flush_delay + max_flush_lifetime + max
clock skew)` hours past the successor's activation
(`ravel_catalog::DEFAULT_SCAN_SLACK_HOURS`, `S = 3` with today's defaults:
`ravel_catalog::FLUSH_BOUND_SLACK_HOURS` = 2 from flush timing plus
`ravel_catalog::TOLERATED_CLOCK_SKEW_HOURS` = 1 of tolerated inter-writer clock
skew), so a straggler that lands within `S` hours of the activation is
still scanned and returned, for any writer whose clock skew stays within
`TOLERATED_CLOCK_SKEW_HOURS`. A writer skewed beyond that bound has no
read-side fix -- no finite slack covers unbounded skew -- and is a distinct,
open hazard: routing itself has no local signal to detect its own clock's
disagreement with the fleet's shared notion of "now", so it cannot clamp or
reject on skew it cannot observe. On an increase no slack is needed: the old,
smaller range is a subset of the new one.

Operationally: do **not** decrease `shard_count` and immediately assume every
prior write is now under the new, narrower range. For `S` hours past the
activation, queries still fan out over the wider retiring range for the affected
hours (a bounded number of extra, mostly-empty LISTs). A commit token minted
before the decrease resolves regardless of `S` -- it names its exact object -- so
read-your-write never depends on the window. Reshard with:

```
ravel-cli provision reshard --tenant <t> --signal <s> --shard-count <n> [--lead-hours <L>]
```

`--lead-hours L` places the activation `L` hours out and must satisfy `L >=
ceil(C) + 1` (the CLI refuses less), so every live writer refreshes its record
view within `C` and observes the new generation before it activates -- or
fail-stops. `C` bounds when writers pick up the change; `S` bounds how long
readers keep scanning the old range after it. See docs/consistency-model.md,
"Online resharding", for the reader/writer transition contract.

## Shard actor

Single task per shard. No locks on the hot path; all state actor-local:

- `buf: HashMap<SeriesId, SeriesBuf { labels: LabelSet, samples: Vec<Sample> }>`
- `exemplars: Vec<IngestExemplar>` in arrival order, one per exemplar the wire
  admitted for a series routed to this shard (ADR-0047)
- `est_bytes`: running estimate (samples * 16 + label bytes on first sight,
  plus the `IngestExemplar` struct width and its attribute bytes per buffered
  exemplar). "Label bytes" means what the buffer holds, not what the object
  will hold: each label costs `size_of::<Label>()` (two `String` headers, 48
  bytes) plus its name and value bytes. Leaving the header term out
  understates a ten-label series by roughly 480 bytes against the 200 it
  counts, so both flush triggers and the process-wide budget below fire late
  on exactly the label-heavy workloads they exist to bound.
- `oldest_ns`: ingest-arrival time of the oldest buffered point
- `waiters: Vec<oneshot::Sender<...>>` for strict-mode acks in this flush window
- writer identity: (writer_id uuid, epoch, next_seq) owned by the process

Loop over `select!`:
- message received: merge points, push `ack` to waiters (strict) or reply
  immediately (buffered), flush if `est_bytes >= target_bytes` (default
  8 MiB). Before merging, each point's series_id is checked against the
  canonical label set that id already claims in the buffer; a mismatch
  (hash collision) rejects the point with a typed error and increments
  the series_id_collisions counter instead of silently merging
  (ADR-0005 fail-loud rule).
- flush tick (interval default 200 ms): flush if `oldest_ns` older than an
  age threshold, and buffer non-empty. The threshold is `max_flush_delay`
  (default 2 s) when the buffer has a strict-mode waiter or already holds
  at least `min_flush_bytes` (default 256 KiB); otherwise the buffer is idle
  and the threshold is `max_flush_delay_idle` (default 40 s) instead
  (ADR-0051 section 7). Strict-mode ack latency is unaffected, since a
  strict write always leaves a waiter in the buffer for its whole flush
  window; only a low-volume buffered-mode tenant's PUT cadence changes.
- channel closed (router dropped): flush the remaining buffer before
  exiting rather than discarding it; points that still fail to flush are
  counted, never silently lost.

Shard-actor death is observable: the router marks a shard dead when its
channel closes or an ack receiver fails, routes subsequent points for
that shard to a typed shard-unavailable error, and increments a
shard_deaths counter. Surviving shards keep working.

Flush (still inside the actor; ingest-ordering per shard is the point):
1. Build RSEG via `ravel-segment::SegmentWriter` (one segment per tenant in
   the buffer; Phase 1 actors buffer per tenant already, key the buf map by
   (tenant, series_id)).
2. blake3 -> data key -> data PUT (Overwrite) with bounded retries on
   retryable errors.
3. Build CommitRecord, `ravel-commit::publish` (CreateIfAbsent + idempotency
   check) -> CommitToken.
4. Send token to all waiters; clear buffer; seq += 1.
5. On permanent failure: error to all waiters (client retries; nothing was
   acknowledged), drop the buffer (documented at-least-once), count it.

Step 1 also decides which buffered exemplars the object carries (ADR-0047
decisions 1 and 2). An exemplar whose parent sample is not in this flush is
dropped first, before the cap, since the object carries no measurement for it
and the writer treats such a record as an error rather than a silent drop.
What survives is offered to an `ExemplarCap` built for this flush and dropped
with it (a shard-lived cap would hold an unbounded per-series map), newest
-first with a stable sort, because `ExemplarCap::admit` is first-wins and
never retracts. A flush with nothing admitted emits no EXEMPLARS section at
all. Both outcomes are counted (`exemplars_written_total`,
`exemplars_dropped_total`) so the drop stays visible.

The commit record's `ingest_hour_bucket` is derived from the flush-open
clock reading at step 1, before the segment is built. A non-positive or
non-representable reading fails the flush the same way a segment-build
error does (typed `SegmentBuild` error, every waiter acked with it, no
object written) rather than defaulting to bucket 0 (ADR-0051 section 7):
a fallback bucket would make the data undiscoverable by hour
with no trace of the failure.

### Pipelined flushes (ADR-0067)

The PUTs no longer run inline in the actor. At flush-open the actor pins
the flush's identity synchronously (seq, waiters, ingest-hour bucket) in
its own message-processing order, then moves the buffer out and hands
steps 1-5 above to a spawned task by ownership transfer -- no shared
mutable state, per ADR-0067 decision 1. The actor returns immediately to
`select!` and keeps draining its mailbox, merging new points and opening
further flushes, while any number of earlier flushes are still stuck in
their PUTs.

Because identity is pinned before the spawned task ever issues a PUT,
submission order still determines `seq` order even when two flushes'
PUTs resolve out of order (the slower one first): ack isolation keys off
pinned identity, not completion order, so a caller's `write()` always
resolves to its own token regardless of which flush's PUT the store lets
through first.

`max_inflight_flushes` (`IngestConfig::max_inflight_flushes`, CLI
`--max-inflight-flushes`, default **1**) bounds how many such spawned
tasks one shard may have outstanding at once, via a per-shard
`tokio::sync::Semaphore`. It is the only thing a flush trigger can now
block on. Default 1 reproduces today's one-flush-at-a-time behavior bit
for bit; raising it trades bounded extra per-shard memory (buffers held
open by the extra in-flight flushes, up to `max_inflight_flushes - 1`
flush windows' worth) for overlapped PUT latency, and should be raised
only as a measured decision. `0` is rejected at
the CLI edge (`Cli::validate`): it would deadlock every flush, since a
shard could never acquire a permit to run one.

Pipelining does not change what the catalog already tolerates: a
flush's seq is allocated at pin time, not at commit time, so two
overlapped flushes for the same shard can publish their commit records
out of seq order when the store resolves their PUTs out of order. This
is the same seq-gap tolerance the per-(writer,shard) commit protocol
already provides (docs/catalog-and-mvcc.md) for a writer restart or a
retried, abandoned flush; pipelining just makes it a routine occurrence
under concurrency greater than 1 instead of an edge case. Nothing about
resolution or read-your-write changes: a commit token still names its
exact object directly.

This applies to all three ingest pipelines. The log and span shard
actors (below) pipeline their flushes on the same terms (ADR-0076
decision 3): the same `max_inflight_flushes` bound, the same spawned
flush task, and the same shutdown join. Only the adaptive flush delay
(decision 3 of ADR-0067, next section) stayed metrics-only.

### Adaptive flush delay (ADR-0067 decision 3)

`adaptive_flush_delay` (`IngestConfig::adaptive_flush_delay`, CLI
`--adaptive-flush-delay`, default **false**) replaces the fixed
`max_flush_delay` age threshold, for a buffer with a strict-mode waiter
or already past `min_flush_bytes`, with a per-(shard, tenant) threshold
clamped into a corridor `[max_flush_delay, ceiling]`:

- **Floor**: `max_flush_delay` (2 s default), unchanged from today.
- **Ceiling**: the strict-mode visibility budget (`IngestConfig::strict_visibility_budget_ns`,
  2.5 s default -- `max_flush_delay` plus `STRICT_VISIBILITY_RESERVE_NS` (500 ms),
  following `max_flush_delay` -- ADR-0076 decision 4; the same budget
  docs/consistency-model.md's strict-mode ack contract names) minus two
  PUT round trips at their observed p99 (data object, then commit
  record) minus one retry's base backoff as headroom, floored at
  `max_flush_delay` so the corridor never inverts. The reserve keeps the
  corridor's width non-zero: a budget set exactly equal to `max_flush_delay`
  would leave the subtraction nothing to work with, collapsing the ceiling to
  the floor unconditionally. With no PUT RTT observed yet, the ceiling still
  collapses to the floor: adapting upward would be a guess the budget cannot
  back, so a tenant sees the fixed `max_flush_delay` from its very first
  flush, not just after warm-up. RTT is sampled from both PUTs of every flush
  (from spawned tasks, concurrently with the actor and with each other), kept
  as a bounded p99 estimate (last 64 samples) per shard.
- **Threshold**: the tenant's own observed inter-arrival gap, clamped
  into `[floor, ceiling]`. A bursty tenant (small gap) clamps up to the
  floor -- today's behavior, unchanged. A trickle tenant (large gap)
  clamps down to the ceiling instead of a fixed-delay actor waiting on it
  indefinitely for a full `target_bytes` batch.

Off by default, which keeps today's fixed-delay behavior so an operator
opts in deliberately; A/B'd against the fixed corridor in the ingest
bench. A flush the corridor actually stretched past the floor is counted
separately from one that used the fixed value or the idle threshold
(`flushes_by_age_adaptive` vs `flushes_by_age`, "Metrics" below). Strict
write ack latency for a buffer that already has a waiter is bounded by
whichever threshold applies the same way it always was; adaptive delay
changes only where in `[floor, ceiling]` that threshold sits, never
whether a strict waiter's flush eventually fires. Applies to the metrics
ingest pipeline only. Unlike `max_inflight_flushes` above, which bounds
in-flight flushes on all three pipelines, the adaptive delay was
deliberately not extended to logs and spans: those actors keep the fixed
`max_flush_delay` / `max_flush_delay_idle` age trigger.

## Log pipeline

Logs run a parallel pipeline, not a mode of the metrics one (ADR-0029):

```
POST /v1/logs (axum) | logs.v1.LogsService/Export (tonic)
  -> auth + tenant resolve (the same TenantResolver both metrics surfaces use)
  -> normalize_logs (ravel-otlp::logs_normalize) -> Vec<NormalizedLogRecord> + rejects
  -> LogIngestRouter::write(tenant, records, mode) -> LogWriteReceipt
```

`LogIngestRouter` and `LogShardActor` mirror `IngestRouter`/`ShardActor`
structurally (one bounded mpsc channel and one actor task per shard, the
same `IngestConfig` knobs, the same flush triggers, the same pinned
writer identity, the same commit sequence, and the same pipelined flush:
ADR-0067 decisions 1 and 2 apply here too, so a flush runs in a spawned
task bounded by `max_inflight_flushes` and shutdown joins every in-flight
flush before the actor completes) and diverge in exactly four places:

- Objects are RLOG, built with `ravel_logseg::RlogWriter`, not RSEG built
  with `SegmentWriter`. They land under the `l` keyspace
  (`t/<tenant>/l/l0/...`); commit records under `t/<tenant>/l/c/...`.
  `keys::data_key` and the commit protocol are already
  `Signal`-parameterized, so nothing in `ravel-commit` changed for logs.
- Routing is by log stream, `shard_for_log(stream_id, shard_count)`, with
  the identical leading-8-bytes-mod-shard_count math `shard_for` uses on a
  `SeriesId`. Unlike a `SeriesId`, a `LogStreamId` does not itself carry
  the tenant: routing is tenant-scoped because each shard's buffer is keyed
  by tenant, not because the id is.
- There is no series-value-kind concept, so no per-series kind check on
  merge. A log record's unit of identity is its `stream_id` plus the
  canonical `stream_attrs` bytes that id was hashed from.
- The fail-loud identity check does not live in the shard buffer. Where
  `TenantBuf::merge` checks an incoming point's `series_id` against the
  label set that id already claims (ADR-0005), `LogTenantBuf::merge` checks
  nothing: `RlogWriter::finish()` already compares every buffered record's
  `stream_attrs` for a shared `stream_id` and rejects the whole object with
  `LogSegError::InconsistentStreamAttrs`. The flush step maps
  that one variant to `LogWriteError::StreamIdCollision` and counts it in
  `stream_id_collisions`; every other `LogSegError` becomes
  `LogWriteError::SegmentBuild`. Duplicating the check in the buffer would
  be dead code with a second chance to drift.

Commit-record fields the log flush fills differently: `sample_count` is the
log record count (a record is the RLOG analogue of a sample), `series_count`
is the number of distinct `stream_id`s in the batch (tracked in the actor,
since `finish()` does not report it back), and `segment_format_version` is
`LOG_SEGMENT_FORMAT_VERSION`, RLOG's own trailer version, not
`SEGMENT_FORMAT_VERSION`.

`LogIngestMetrics` mirrors `IngestMetrics` counter for counter under two
renames that follow the unit change: `buffered_records_total` for
`buffered_points_total`, `stream_id_collisions` for `series_id_collisions`.

Snapshot resolution for logs is wired: `services/ravel-server/src/fold.rs`
folds `Signal::Logs` alongside metrics, so `catalog/l/HEAD` and its snapshot
parts are produced the same way they are for metrics, and a catalog-based
read over log objects works from them. Ingest durability never depended on
it either way (a commit token resolves to its commit record directly).

## Span pipeline

Spans run a third parallel pipeline on the same terms (ADR-0041):

```
POST /v1/traces (axum) | trace.v1.TraceService/Export (tonic)
  -> auth + tenant resolve (the same TenantResolver every ingest surface uses)
  -> normalize_traces (ravel-otlp::traces_normalize) -> Vec<NormalizedSpan> + rejects
  -> SpanIngestRouter::write(tenant, spans, mode) -> SpanWriteReceipt
```

`SpanIngestRouter` and `SpanShardActor` mirror `LogIngestRouter`/
`LogShardActor` structurally (one bounded mpsc channel and one actor task
per shard, the same `IngestConfig` knobs, the same flush triggers, the same
pinned writer identity, the same commit sequence, and the same pipelined
flush under `max_inflight_flushes` with a shutdown join) and diverge in
these places:

- Objects are RSPAN, built with `ravel_rspan::RspanWriter`. They land under
  the `s` keyspace (`t/<tenant>/s/l0/...`); commit records under
  `t/<tenant>/s/c/...`.
- Routing is by trace, `shard_for_span(trace_id, shard_count)`, with the
  identical leading-8-bytes-mod-shard_count math `shard_for` uses on a
  `SeriesId`. Keying on `trace_id` rather than on a resource-derived
  identity is ADR-0041 decision 2: it keeps one trace's spans confined to
  one shard, so trace-by-id assembly is a bounded scan instead of a
  fan-out across every shard. The tradeoff ADR-0041 accepts is that
  service-scoped span search is cross-shard.
  `shard_for_span` currently lives in `crates/ravel-ingest/src/span_router.rs`
  rather than beside `shard_for`/`shard_for_log` in `ravel-types`, because
  `ravel-types` was outside the scope of the change that added it. The
  placement is provisional; the routing rule itself is frozen, since it
  determines which shard's object keys a trace's spans land under.
- There is no derived identity, so no identity-collision check anywhere in
  the path: `trace_id` and `span_id` come from the sender verbatim, unlike
  a metric's `series_id` or a log's `stream_id`. `SpanWriteError`
  accordingly has no collision variant and `SpanIngestMetrics` no
  collision counter.
- Resource and scope attributes are merged into each span's single `attrs`
  map at normalization time (`ravel_rspan::merge_attrs`, resource beats
  scope beats span), not carried as a separate identity blob. Because they
  feed no identity, a resource attribute that cannot be converted is
  dropped and reported on its own rather than rejecting every span under
  the resource the way the log path must. Span kind, trace state, flags,
  events, and links have no RSPAN column and are stored under reserved
  underscore-prefixed `attrs` keys (`_kind`, `_trace_state`, `_flags`,
  `_events_raw`, `_links_raw`); events and links are opaque hex blobs in
  v1 (ADR-0041 decision 4).

Commit-record fields the span flush fills differently: `sample_count` is the
span count, `series_count` is the number of distinct `trace_id`s in the batch
(tracked in the actor), `segment_format_version` is
`SPAN_SEGMENT_FORMAT_VERSION`, and the event-time bounds are the batch's
interval — the minimum `start_ts_ns` and the maximum `end_ts_ns` — so a
commit record advertises the same interval RSPAN's skip index prunes with.

`SpanIngestMetrics` mirrors `LogIngestMetrics` counter for counter, minus
`stream_id_collisions` and with `buffered_spans_total` in place of
`buffered_records_total`.

Spans fold and are queryable the same way logs are:
`services/ravel-server/src/fold.rs` folds metrics, logs, and spans, and the
`spans` SQL table on `POST /api/v1/sql` reads them back. Ingest durability
never depended on this fold either way.

## Admission control (ADR-0051)

`ravel-server` builds one `AdmissionController` (`crates/ravel-ingest/src/
admission.rs`) at startup from `--limits-file` (validated at parse time
regardless of mode; an unparseable file or unknown key fails startup) and
threads the same `Arc` into every ingest path: OTLP HTTP and gRPC
(metrics/logs/traces), OTAP, and Remote Write. `--limits-file` now has a
runtime effect, not just a startup validation pass.

Four layers, each enforced before the allocation it bounds, in this order:

1. **Body size.** `DefaultBodyLimit` 16 MiB on `/v1/metrics`, `/v1/logs`,
   `/v1/traces`, and `/api/v1/write`; `max_decoding_message_size` 16 MiB on
   every tonic *ingest* service, OTAP's `ArrowMetricsServiceServer` included.
   The Flight SQL query service (`services/ravel-server/src/flight.rs`) is
   not an ingest path and is out of scope here; it still runs on tonic's
   4 MiB default. Remote
   Write additionally caps the *compressed* body at 16 MiB ahead of its
   existing 64 MiB decompressed cap (`MAX_DECOMPRESSED_PAYLOAD_BYTES`).
   OTAP's Arrow-stream decompression cap
   (`StreamConfig::default().max_decompressed_payload_bytes == 16 MiB`) does
   *not* bound the whole wire message: it is checked per
   `ArrowPayload.record`, and a `BatchArrowRecords` message carries a vector
   of payloads, so the decompressed bound it provides is 16 MiB times the
   payload count, not a flat 16 MiB. The `max_decoding_message_size` cap on
   the tonic service is therefore the explicit per-message bound for OTAP
   too; tonic's own 4 MiB default is stricter than our 16 MiB cap today, but
   the cap is set explicitly rather than left to that upstream default.
2. **Byte rate.** `check_byte_rate`, on wire body bytes, after tenant
   resolution and before decode. HTTP and gRPC each decode independently
   before calling into the shared normalize/write path, so this check has
   no single shared insertion point: it runs once per transport handler
   (all three OTLP HTTP handlers, all three OTLP gRPC services, OTAP's
   per-batch handler, and Remote Write), eight call sites in total. A gRPC
   handler reads wire bytes from a `WireByteCountLayer`
   (`services/ravel-server/src/wire_byte_count.rs`), a `tower::Layer`
   installed on the gRPC listener that parses gRPC's length-delimited
   message framing off the transport bytes as tonic's decoder reads them,
   rather than measuring `request.get_ref().encoded_len()` (a walk of the
   already-decoded protobuf tree) after the fact.
3. **Event-time skew.** Out of scope for this change; unchanged.
4. **Series/stream admission**, metrics and logs only (spans excluded).
   `check_series_creation_rate`/`check_stream_creation_rate` first (a
   breach rejects the whole request); then `admit_series`/`admit_streams`,
   which partially admits: rejected series/streams are dropped from the
   write and folded into the signal's existing partial-success reporting
   (`ExportMetricsPartialSuccess`/`ExportLogsPartialSuccess`).

Rejection is per signal and per layer:

| layer | HTTP | gRPC | Remote Write |
|---|---|---|---|
| body size | 413 | RESOURCE_EXHAUSTED | 413 |
| byte rate | 429 + `Retry-After` | RESOURCE_EXHAUSTED | 429 + `Retry-After` |
| series/stream creation rate | 429 + `Retry-After` | RESOURCE_EXHAUSTED | 429 + `Retry-After` |
| active series/stream cap | 200 + partial success | OK + partial success | 200, no partial-success message, reduced `X-Prometheus-Remote-Write-Samples-Written` |

Remote Write's partial-admission semantics are pinned and do not follow
the other two signals' shape: it never emits a partial-success message,
always answers 2xx with the true written count once body-size and rate
checks pass, and reserves 429 for the rate-limit rows only, never for an
active-series-cap breach.

## Process-wide ingest buffer byte budget (ADR-0069)

The per-(tenant, shard, signal) buffer cap (~`target_bytes`) bounds each
tenant, but not their *sum*: a burst of active tenants can grow resident
memory without any per-tenant limit tripping (ADR-0069). One process-wide
atomic gauge (`ravel_ingest::IngestByteBudget`) bounds that sum. It is shared
by `Arc` across the metrics, log, and span routers, so a single ceiling
covers every signal.

Each ingest write charges its estimated buffered bytes into the gauge in the
router's write path, after decode/normalize/admission and before any shard
buffer is touched (`IngestPoint::est_charge_bytes`: 16 bytes per sample plus,
per label, the `Label` struct header and the name/value bytes, plus each
exemplar's buffered width). The log and span routers charge the same gauge with
`est_record_bytes`/`est_span_bytes`, and those apply the identical per-attribute
rule: each attribute costs its pair struct header (`(String, AttrValue)` for a
log attribute, `(String, String)` for a span attribute, the latter byte-for-byte
the same as a `Label`) plus its key/value bytes. Counting only the string bytes
on any one signal would undercharge this shared ceiling on that signal while the
others charge honestly. If the charge would push the gauge past the ceiling
(`--max-ingest-buffer-bytes`, default 512 MiB, `0` = unlimited) the request
is shed *before* buffering: no shard is touched, no commit token is minted,
the shed counter increments, and the caller gets HTTP 429 with `Retry-After`
(gRPC `RESOURCE_EXHAUSTED`), exactly like the layer-2 byte-rate rejection and
the in-flight shed. The charge is an RAII guard cloned into every shard
message the request fans out to; each shard buffer holds its clones and moves
them into the flush, and the guard refunds the exact charged amount when the
last buffer holding any of the request's bytes flushes (or its flush fails or
is abandoned). In-flight pipelined flushes (ADR-0067) therefore stay charged
until their PUTs complete, so pipelining depth is automatically accounted
for. The gauge, its configured ceiling, and the shed counter render on
`/metrics` as `ravel_ingest_buffer_bytes`, `ravel_ingest_buffer_bytes_limit`,
and `ravel_ingest_buffer_shed_total`.

### Worst-case resident memory

Worst-case ingest resident memory is the sum of three named, config-bounded
terms:

1. **Buffered ingest state**: `--max-ingest-buffer-bytes` (default 512 MiB).
   This ceiling covers the estimated bytes of every shard buffer *and* every
   in-flight pipelined flush across all tenants and signals at once, since a
   flush's buffer stays charged until its PUTs complete. The charge is an
   estimate. For metrics (`est_charge_bytes`) it counts a series' label bytes
   on every point rather than only on first sight, which over-counts, and it
   counts neither `HashMap` overhead nor allocator slack, which under-counts
   by more. The second effect dominates: measured on a 50k-series, 11-label
   buffered workload, the charged figure was 38.4 MB against a 69.3 MB
   resident delta. Size a metrics host for roughly twice this ceiling, not
   for the ceiling itself.

   That two-times figure is a metrics measurement and does not transfer to
   logs. The nesting-accounting gap it once warned about is closed:
   `attr_value_len` now charges the per-element struct header at every nesting
   level (a `(String, AttrValue)` per `Map` entry and a `size_of::<AttrValue>()`
   per `List` item), not only for a record's own top-level attributes, so a
   record whose attributes nest is charged for the structs the buffer actually
   holds rather than for leaf bytes alone. What is still unestablished is the
   ratio itself: no one has measured the charged-to-resident ratio on a log
   workload the way the 38.4 MB / 69.3 MB metrics figure above was measured, and
   closing the accounting gap does not by itself make the metrics "roughly twice"
   ratio hold for logs. Until a log workload is measured, size a log-heavy host
   from measurement rather than from this multiplier.
2. **In-flight decode overhead**: each admitted in-flight request transiently
   holds one decoded/normalized request body during normalization, before its
   points reach a buffer. This is bounded by
   `--max-inflight-ingest-requests` (default 1024) times the largest
   per-request decoded size (Remote Write's 64 MiB post-decompression cap, or
   OTLP's 16 MiB) — the same worst case the concurrency limit already
   documents above. It is *not* covered by the buffer budget, which is
   charged post-decode.
3. **Fixed overhead**: shard-actor and router state, the admission
   controller's per-tenant maps, and the read caches (`--cache-max-bytes`),
   all bounded independently of ingest volume.

So an operator sizes ingest RSS as
`max_ingest_buffer_bytes + (max_inflight_ingest_requests x largest_decoded_body)
+ fixed_overhead`, every term a knob. Lowering `--max-ingest-buffer-bytes`
tightens term 1 directly, trading a lower memory ceiling for earlier shedding
under a many-tenant burst.

### Idle-tenant state eviction (ADR-0069 decision 2)

The buffer budget above bounds *buffered* bytes, but several per-tenant maps
grew monotonically over process lifetime regardless of buffering: the
generation-switch views (one per tenant that ever wrote), the catalog's
per-tenant decoded caches, and the SQL per-tenant memory accountants. A single
background sweep bounds all three. Every `--idle-tenant-state-ttl` (default
`1h`; `0` disables the sweep) it evicts per-tenant state last touched more than
that long ago, on a jittered cadence, from the same worker-loop shape every
other background task uses:

- **Generation views** — re-read from the provisioning record on the tenant's
  next write (the evicted view reports stale exactly as a first-touch tenant
  does), so the cost is one provisioning-record GET on the next write.
- **Catalog per-tenant caches** — the decoded commit-record, compaction-record,
  HEAD, part, and postings caches; all immutable, content-addressed, or
  TTL-revalidated, so an evicted entry is re-read on the next resolve.
- **SQL memory accountants** — only those with zero outstanding reservations
  (an accountant backing a live query is never evicted); a re-created one is a
  byte-for-byte-equivalent counter.

"Last touched" is stamped from an injected clock at each tenant's write
(generation views), resolve (catalog caches), or query resolve (SQL
accountants), so eviction is deterministic and reads no clock in the library
layers — only the sweep loop reads the wall clock.

**Admission-controller state is explicitly excluded.** Its active-series and
active-stream counts are correctness-bearing caps; silently resetting a
tenant's cap consumption on a memory-pressure sweep is never a valid trade-off
(ADR-0069 decision 2). That map therefore still grows with tenant count, a
documented gap with a named follow-up, not an unsafe eviction.

## Modes

`mode=strict` (default): ack after step 3 for every flush the request's
points landed in (a request spanning shards awaits all of them; the receipt
carries max token per shard).
`mode=buffered`: ack at enqueue. Config per tenant; header override
`x-ravel-ingest-mode: buffered` allowed only when tenant config permits.

## Sizing defaults (config, all overridable)

| knob | default |
|---|---|
| shard_count | 4 (dev), scale with cores |
| channel depth | 256 msgs |
| target_bytes | 8 MiB |
| max_flush_delay | 2 s (`--max-flush-delay`) |
| max_flush_delay_idle | 40 s (`--max-flush-delay-idle`) |
| min_flush_bytes | 256 KiB (`--min-flush-bytes`) |
| put retry budget | 4 attempts, 100ms..2s jittered backoff |
| max in-flight ingest requests (process-wide) | 1024 (`--max-inflight-ingest-requests`, 0 = unlimited) |
| max ingest buffer bytes (process-wide, all signals) | 512 MiB (`--max-ingest-buffer-bytes`, 0 = unlimited) |
| max_inflight_flushes (per shard, all three pipelines) | 1 (`--max-inflight-flushes`, rejects 0) |
| adaptive_flush_delay (metrics pipeline only) | off (`--adaptive-flush-delay`) |
| idle-tenant state TTL (process-wide) | 1 h (`--idle-tenant-state-ttl`, 0 = disabled) |

## CPU cost of the write path (measured)

A CPU flamegraph of the write path, from `ravel-bench`'s `ingest_bench` bin
built `--release --features profiling`. A `pprof` sampler at 997 Hz brackets
only the measured region (fixture generation and report assembly excluded).
While a profile is active the harness disables its own visibility poller,
per-batch catalog resolves, and depth sampler, so samples attribute to the
ingest path and not the benchmark measuring itself; such a run reports no
visibility-lag figures and in-flight depth `n=0`.

- Host: aarch64, 4 cores, ~8 GiB RAM, single-board class. Build profile:
  `release`. The ranking below is bounded to this host: its memory bandwidth
  is far below a server-class machine's, and the dominant cost here is memory
  movement, so the shares are the ones most likely to shift on other
  hardware. Re-measure before sizing or optimising against a different host.
- Workload: `--store memory --shards 4 --target-series 2000
  --points-per-sec 4000000 --duration-secs 1 --batch-size 5000
  --ack-timeout-secs 120`; 4,000,000 points, all accepted, no errors.
- Sampler collected 648 on-CPU samples.

What this profile supports and what it does not:

- It is a composition of on-CPU time, not of wall time. `pprof` uses
  `ITIMER_PROF`, which samples only running threads, so parked tokio workers
  never appear. 0% of the profile is idle/park frames by construction, and
  the write path spends most of its wall time off-CPU awaiting flushes,
  where this profile is blind.
- At 648 samples the 95% binomial interval on a 50%-of-samples group is
  about +/-4%, so only the broad grouping below is supportable, not
  per-function figures to better than a few percent. Across three
  independent runs (251, 648, 932 on-CPU samples) the two dominant costs,
  memory movement and the series-id hash map, are stable in rank; the finer
  splits below them shift with sample count and label cardinality.

Essentially all on-CPU time is the ingest write path: 99.4% of samples fall
in `ravel_ingest`/`ravel_segment`/`ravel_codec`/`ravel_commit` frames, the
remaining 0.6% in tokio worker scheduling. Grouped by self time:

| group | share of on-CPU samples |
|---|---|
| point/buffer memory traffic (moves, copies, `Vec<u8>`/`Vec<i64>` growth including segment byte-buffer construction, `String` drops) | ~56% |
| router per-point conversion loop (`IngestRouter::write_points`) | ~16% |
| series-id hash map (`hashbrown` probe + SipHash) | ~15% |
| CRC32C checksum on the object-store PUT | ~6% |
| segment codec encode (`encode_i64`, varint) | ~5% |
| sample sort + tokio scheduling | ~2% |

The dominant cost is memory movement, not compression, hashing, or I/O:
building `IngestPoint`s, growing the per-shard byte buffers, and dropping
label `String`s. Byte-slice label comparison during series accumulation is
also visible and grows with shared-label cardinality. The only object-store
work is the CRC32C over the in-memory PUT; a real S3 backend adds network
I/O this in-memory profile omits.

## CPU cost of the columnar load path vs the row path (measured)

ADR-0109 replaced the bulk loader's per-row write path
(`LogIngestRouter::write`, which pivots each row into columns inside the RLOG
writer) with a columnar fast path (`LogIngestRouter::write_columnar`, which
stages contiguous per-column arrays and skips the `write_block` gather and the
per-attribute `column_of` probe). Decision 8 of that ADR *argued from a
microbench ratio* that removing the pivot addresses roughly 78 points of load
CPU; no end-to-end number had ever been measured. This is the first one
(`ravel-bench`'s `columnar_load_compare` bin, issue #606).

- Host: x86_64, AMD EPYC-Rome, 8 logical cores, ~15 GiB RAM. Build profile:
  `release`.
- Workload: a synthetic ClickBench-shaped Parquet sample (integer-heavy with a
  string minority, values derived from row/column indices) decoded once and
  loaded through both paths on a fresh in-process router and `MemoryStore`
  each. `--shards 1` (whole corpus on one shard, so the differential is the
  write-path pivot alone and not shard fan-out), `--batch-rows 10000`
  (one RLOG object per batch, Strict acks).
- CPU is process user+system time from `/proc/self/stat` across the write loop
  only; the Parquet decode and the columnar-batch build both happen before the
  timed region, exactly as the shipping loader builds its columnar batch in the
  decode task off the flush critical path.

Measured, at two column widths (three runs each at 100 columns, two at 300):

| corpus | rows | row-path write CPU | columnar-path write CPU | pivot share of row write CPU |
|---|---|---|---|---|
| 100 attribute columns (ClickBench width) | 50,000 | ~7.0-7.5 s | ~7.3-7.5 s | **-4% to +1% (no measurable reduction)** |
| 300 attribute columns (exaggerated width) | 20,000 | ~13.7-14.0 s | ~11.8-12.1 s | ~14-16% |

At the ClickBench-representative ~100-column shape the columnar path is within
run-to-run noise of the row path (if anything marginally slower); a reduction
only becomes visible (~15%) at 300 columns, three times wider than ClickBench's
~105. The saving scales super-linearly with column count, consistent with the
removed cost being the quadratic gather, but that quadratic term is not yet a
large share of the full object build at ClickBench width: the columnar path
still pays the per-row merged-view/`attrs_raw` derivation `build_object_columnar`
retains, plus block framing, compression, indexing, and the object PUT, none of
which the pivot removal touches.

What this measurement supports and what it does not:

- **It is a local differential on a bounded synthetic sample, not the
  ClickBench reference figure.** The reference-box run (c6a.4xlarge, full
  `hits.parquet`, S3) is deliberately out of scope. Nothing here should be read
  as the reference result.
- **It measures the epic WITHOUT ADR-0109 decision 3 contributing.** The
  columnar batch is built through `ColumnarLogBatch::from_records`, which
  attaches no dictionaries, so the dictionary-preserving column and
  dictionary-aware bloom path never engages -- exactly as it fails to engage on
  ClickBench-shaped plain-`BYTE_ARRAY` Parquet (issue #660, arrow-rs fuses the
  column's dictionary away on decode). Decision 8's arithmetic counted those
  savings; this number does not include them.
- **The store is in-memory.** S3 latency, multi-shard fan-out scaling, and real
  PUT round trips are invisible to this harness; the CRC32C over the in-memory
  PUT is the only object-store work timed. The differential also excludes the
  Parquet decode (shared by both paths), so it reports the pivot's share of the
  *write* CPU, not of end-to-end load CPU, which is lower again.
- The isolated microbench decision 8 rests on is
  `crates/ravel-logseg/benches/wide_gather.rs`: on this host (`--quick`) the
  gather is ~23% of `write_block` at 10 columns and ~99% of it at 105
  (gather 413 ms vs `write_block` 419 ms). The gather dominating the isolated
  block encode does **not** translate into a proportional end-to-end write-path
  saving at that width, which is the gap this measurement exposes.

### Bulk-load write concurrency (ADR-0807)

The CPU numbers above measure one write path against another; they do not
measure how much of the object-storage round trip is hidden behind other work.
On the reference box a 100M-row ClickBench load ran at 2.33 of 16 cores busy
with 0.06% iowait, because the bulk loader serializes its writes at the default
settings. Two nested concurrency windows bound the bulk write path, and both
default to 1: `--pipeline-depth` (batches the loader keeps outstanding) and
`max_inflight_flushes` (flushes per shard, the per-shard semaphore above). The
loader builds its own `IngestConfig` from `..IngestConfig::default()`, so
`max_inflight_flushes` is fixed at 1 on the bulk path and, unlike on
`ravel-server`, not reachable by a flag. The concurrent-write ceiling is
`shards * min(pipeline_depth, max_inflight_flushes)`. ADR-0807 audits every
write-path bound, decides to expose `--max-inflight-flushes` on the loader while
keeping both defaults at 1 (raising `--pipeline-depth` above 1 weakens the
loader's durable-token report, so the speed-up is opt-in), and records that every
published ClickBench load figure was measured at depth 1.

## Metrics (self-observability)

`IngestMetrics` (crates/ravel-ingest/src/metrics.rs) exposes process-global
`u64` counters through `IngestMetricsSnapshot`. They carry no per-shard and no
per-tenant dimension: one `IngestMetrics` is built by the router and shared by
every shard actor via `Arc`, so each value is the sum across all shards and all
tenants of the process.

Counters recorded today:

- `flushes_by_size`, `flushes_by_age`, `flushes_by_age_adaptive`,
  `flushes_manual`: flush count by trigger. `flushes_by_age_adaptive` is the
  subset of age-triggered flushes where `adaptive_flush_delay` actually
  stretched the threshold past `max_flush_delay`; it is zero unless that knob
  is enabled, and is disjoint from `flushes_by_age` (a flush counts in exactly
  one of the two). `flushes_manual` covers explicit `FlushNow`, the `Shutdown`
  drain, and the channel-close drop-path drain. These are **attempt-time**:
  incremented when a flush is opened, before the segment build or any PUT, so
  a later-abandoned flush is counted here as well as in an `abandoned_*`
  counter. Successful flushes = the four trigger counters minus the two
  `abandoned_*` counters.
- `abandoned_retry_exhausted`: flush abandoned because a PUT exhausted its retry
  budget or `max_flush_lifetime` elapsed (`WriteError::Abandoned`). Durability
  signal; retryable.
- `abandoned_input_rejected`: flush abandoned because the input could not be
  built into a durable object (`WriteError::SegmentBuild`). Client signal; not
  retryable. Split from `abandoned_retry_exhausted` so a store problem is
  distinguishable from a bad-input problem by counter alone.
- `put_retries`: retried PUT attempts across the data-object and commit-record
  paths (first attempt of each excluded).
- `buffered_bytes_total`, `buffered_points_total`: cumulative volume admitted
  into shard buffers at enqueue time.
- `acks_ok`, `acks_err`: strict-mode waiters acked (**success-time**, at the
  flush's terminal outcome). Zero for buffered-mode and for flushes with no
  strict waiter, so this is an ack-outcome counter, not a flush-outcome one.
- `series_id_collisions`: batches rejected fail-loud on an ADR-0005 series-id
  collision.
- `shard_deaths`: distinct shard actors observed dead by the router, counted
  once per shard.
- `in_flight_flushes_total`: gauge, sum across shards of flush tasks spawned
  but not yet acked (ADR-0067 decision 2 consequence of pipelining). Unlike
  every other counter here it is per-shard underneath
  (`IngestMetrics::in_flight_flushes_by_shard`) before being summed into this
  flat total; a shard with no flush in flight contributes 0. With
  `max_inflight_flushes` at its default of 1, this never exceeds
  `shard_count`.

Tracked future work (not yet implemented): a per-shard
and per-tenant dimensioned model — per-shard buffered bytes/points, flush
build/put/commit latency histograms, ack latency, and queue depth; per-tenant
accepted/rejected points and bytes. It requires a metrics backend that these
flat atomics do not provide and is out of scope for the counters above.
