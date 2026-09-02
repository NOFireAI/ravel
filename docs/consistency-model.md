# Consistency, Durability, and Failure Semantics

Normative. Tests in `crates/ravel-failure-tests` assert every claim here. If
code and this document disagree, one of them is a bug and the fix updates both.

Guarantees come first in this file, mechanism second. Four distinctions run
through it, and keeping them apart is how to read it:

- **Durability against visibility.** Whether a write survives a crash and
  whether a query can see it are separate properties with separate rules.
  Acknowledgement semantics governs the first, Visibility semantics the
  second, and conflating them is the most common misreading.
- **Safety against liveness.** No deletion ever removes an object a
  referenced snapshot still names (safety), and retention still eventually
  completes and reclaims storage (liveness). The Deletion and GC guarantee
  below holds both at once.
- **Atomicity against convergence.** A commit is atomic (a query sees all of
  a segment's rows or none). Compaction is not atomic; it converges, because
  query-time dedup collapses an input seen together with its replacement to
  one answer.
- **Logical correctness against duplicated work.** At-least-once delivery can
  store a batch twice. For metrics that is duplicated work a query dedups
  away; for logs and spans it is user-visible duplication unless an
  idempotency key is supplied.

## Acknowledgement semantics

Strict mode (default):
- An OTLP export is acknowledged only after every batch it contributed to has
  (a) its L0 data object durably stored and (b) its commit record created.
- The response carries a commit token set (header `x-ravel-commit-token`,
  comma-separated): one token per shard the request's points flushed
  through. Tokens are v2 and self-locating (they embed the ingest-hour
  bucket; ADR-0010 §2).
- After a strict ack, no crash of any Ravel process may lose that data.
  Object-store durability is the floor: data survives anything the object
  store survives.

Buffered mode (opt-in per request, named "buffered"):
- Acknowledged after admission and enqueue to a shard actor. A crash between
  ack and flush loses the buffered window (bounded by max flush delay).
- Never described as durable. No commit token is returned.

Rejection: admission failures (limits, auth, quota) reject before buffering
in both modes. Partial success uses the OTLP partial-success message with
rejected point counts and reasons.

## Visibility semantics

- A batch becomes visible to queries when its commit record exists; commit
  record creation is atomic (create-if-absent), so visibility is atomic per
  L0 object.
- Visibility latency = flush delay + data PUT + commit PUT. The flush delay
  is a configurable operator budget (`--max-flush-delay`), default 2 s in
  strict mode (ADR-0076 decision 4); the p99 visibility target under target
  load tracks that budget, not a fixed sub-second constant.
- There is no cross-shard ordering guarantee. A query snapshot may include
  commit N+1 of shard A and not commit M of shard B, regardless of wall-clock
  order. Per (writer, shard), commits are sequenced by `seq`.

## Read-your-write

- A caller holding commit tokens sees the referenced commits by passing
  `min_commit_token` (repeatable) to query APIs. Each token fully
  determines its commit-record key; the catalog GETs those keys directly
  and includes the segments, or fails with `unsatisfiable token` rather
  than silently serving stale data.
- Without a token, queries see some recent consistent snapshot; freshness is
  bounded by listing behavior, not guaranteed.

## Snapshot isolation

- A query resolves one snapshot (a logical set of immutable segments) and
  uses it for its entire execution. Commits, compactions, and deletions that
  land mid-query do not affect it.
- Compaction transactions atomically swap inputs for outputs in new
  snapshots; both sets remain physically present until GC clears the inputs
  after the protection horizon.

## Crash matrix (strict mode)

| Crash point | Data object | Commit record | Ack | Outcome |
|---|---|---|---|---|
| Before data PUT | absent | absent | no | client retries; nothing stored |
| After data PUT, before commit PUT | present (orphan) | absent | no | invisible; GC after grace; client retries |
| After commit PUT, before ack | present | present | no | visible; unkeyed client retry stores a duplicate (see above); a keyed retry replays the marker and stores nothing new, since the marker PUT precedes the ack |
| After ack | present | present | yes | durable and visible |

## Duplicates and idempotency

- Delivery model is at-least-once. A client retry after a lost ack re-ingests
  the batch; both copies are stored.
- For **metrics**, this is harmless: queries dedup by `(series_id, ts)`, so a
  retried sample collapses exactly as Prometheus would if scraped twice
  (PromQL takes the last value at a timestamp; identical duplicates are
  harmless, differing values at the same timestamp are last-write-wins per
  evaluation order and documented as such).
- For **logs and spans**, there is no query-time dedup: a retry after a lost
  ack is *user-visible* duplication (extra rows / spans). The
  "identical duplicates are harmless" framing above is true for metrics only
  and must not be over-read to cover logs and spans (ADR-0051 §5).
- Writer-side retries of the same flush are idempotent by construction:
  same commit key, content-hash-verified (ADR-0002).
- The `ravel-cli load` bulk path is an instance of this at-least-once logs
  duplication, not an exception to it. At `--pipeline-depth` > 1 a batch after a
  failing one may commit in the background after the loader has already returned
  an error, becoming query-visible without appearing in the loader's reported
  durable-token list; a resume from that list re-ingests those rows. The Strict
  ack contract above is intact (the router still returns a token only for a shard
  that durably committed); the gap is between that ack and the loader's resume
  aid. See ADR-0807, which keeps the loader default at `--pipeline-depth 1`, where
  the gap cannot arise.

### Opt-in client idempotency key (logs and spans)

Log and span ingest accept an optional, opaque `x-ravel-idempotency-key`
(HTTP header or gRPC metadata, `≤128` bytes; a longer key is rejected with
HTTP 400 / gRPC `InvalidArgument`, never truncated). The OTLP protobuf
schemas are untouched: the key travels only as transport metadata.

**Ordering guarantee.** For a keyed request the gateway writes the
idempotency marker *after* the data is durably committed and *before* the
response returns, so the client can never observe an ack for data whose
marker was not yet written:

```
data PUT -> commit PUT -> marker PUT (CreateIfAbsent) -> ack
```

The marker records the original request's written row/span count and its
`x-ravel-commit-token` header value verbatim. A marker is written only for a
request whose *every* shard committed durably; buffered-mode and
fully-rejected requests commit no durable data and write no marker. A
multi-shard request where some but not all shards committed
(`LogWriteError::PartialWrite`) also writes no marker, even
though it did produce a partial durable commit: a hit on this marker's
replay path reports the original commit token with zero rejections, so
marking a partial commit as the request's receipt would make the next retry
skip resending the shard that never committed, permanently losing its
records instead of the honest at-least-once duplication an unkeyed retry
gets. The recovered tokens for the durable siblings are not returned
to the OTLP client (the protocol has no error-response channel for them,
unlike the CLI's `load` path, which does report them); the gateway logs
them at `warn` for operators instead.

**Replay contract.** A retry that supplies the same key first consults the
marker (one prefix LIST over the dedup window, default 24 h, shared with
`ravel-maintain`'s sweep so the read path and the sweep agree on the window).
On a hit inside the window the retry skips layer-3/4 admission (structural
bounds and active-series/stream caps), normalization, and the router write
entirely, and replays the stored receipt: the original commit-token header
value byte-for-byte, and `rejected_log_records` / `rejected_spans` of 0 with
no partial-success (the original request already accounted for its own
rejections at write time). No new rows or spans are written, and no
layer-3/4 admission usage is charged. The layer-1 body-size cap and layer-2
byte-rate token bucket still apply to a replayed request exactly as they do
to any other: a replay still costs wire bytes, and a tenant well over its
byte-rate budget can still see a replayed retry rejected at layer 2 before
the marker lookup ever runs.

**Fail-open, never a lost ack.** A corrupt or unparseable marker, or a store
error on the lookup, is treated as a miss: the request proceeds down the
normal path (at-least-once), it is never surfaced as an error to the caller.
A `write_marker` failure after a durable commit is logged and the request
still acks success, because the data is already committed; the retry then
reingests (at-least-once) since no marker exists.

**Honest residuals** (unchanged from ADR-0051 §5): a crash after the commit
PUT but before the marker PUT still yields a duplicate on retry; two
concurrent requests with the same key can both ingest (the window targets
sequential retry after a lost ack, the actual failure mode); and unkeyed
requests get plain at-least-once. Ravel's ingestion contract is at-least-once
and no stronger; it does not promise a single stored copy per record.

## Late and skewed data

- Event time is never trusted for discovery. Commit records are bucketed by
  ingest hour; event-time bounds ride along for pruning. Late data is always
  discoverable; queries bound their listing by `max_ingest_lag` plus catalog
  snapshots for anything older.
- Admission bounds event-time skew (ADR-0010 §8): points with
  `event_ts > ingest_ts + max_future_skew` (default 10 m) or
  `event_ts < ingest_ts - max_ingest_lag` (default 2 h) are rejected with a
  partial-success reason. These bounds are what make the catalog listing
  window sound. Logs and spans enforce the same bounds at admission
  (ADR-0051 §4); spans bound `end_ts` by both edges (`max_future_skew` on the
  future side, `max_ingest_lag` on the late side), and reject `end_ts <
  start_ts` outright. The lag bound anchors on the span's end, not its start
  (ADR-0051 amendment 2026-08-13): a long-running span that started more than
  `max_ingest_lag` ago but ended within the window is admitted; only a span
  reported more than `max_ingest_lag` after it ended is rejected.
- The receiver's own admission clock is checked too (ADR-0051 amendment):
  a reading below a compiled floor (2020-01-01T00:00:00Z) or one that
  yields no representable ingest-hour bucket rejects the whole request with
  HTTP 503 / gRPC `UNAVAILABLE`, rather than bucketing acked data into a
  far-past or far-future hour. The same floor extends the fail-loud flush-open
  check, so a clock that goes bad between a buffered ack and flush open fails
  the flush instead of writing a nonsense bucket.
- Config discipline: `max_ingest_lag` is one shared bound, not a per-signal
  one, in the sense that matters operationally, though it is not shared by
  reference: the admission checks (one `max_ingest_lag_ns` constant per
  signal crate) and the catalog listing window
  (crates/ravel-catalog/src/config.rs) each hold their own copy, which must
  be kept numerically equal by convention (ravel-maintain's own copy
  documents this with a startup equality assertion). The bound on what is
  *admitted* and the bound on what is *discoverable* must be the same
  value. Raising the admission lag for a signal or tenant is
  legal only together with the catalog-side listing-window config: widen the
  catalog window first, then the admission bound, the same ordering
  `max_flush_lifetime` follows between folders and writers
  (docs/catalog-and-mvcc.md, "Config discipline"). Lowering the admission lag
  is always safe. Raising the admission lag alone admits records the listing
  window then fails to discover on any non-token query.
- Bulk-import exception (ADR-0089): `ravel-cli load --parquet` relaxes the
  *past-event-time lag* admission bound for offline bulk loads without
  violating the paired-bound rule above, because it does not touch the listing
  side. A bulk-loaded record buckets by the flush-open wall clock, not its
  event time, so it lands in today's ingest-hour bucket regardless of how old
  the event is; the listing window's future bound (`now + max_future_skew`)
  still reaches that bucket, so a query with a normal `start`/`end` window
  (compared against event-range overlap) discovers it. Future skew stays
  enforced on this path for exactly the reason above: a far-future event
  would bucket by today's wall clock but never be reached by any later query's
  listing window. See docs/guides/ingest.md "Bulk import".

## Catalog snapshot staleness

The catalog fold precomputes an immutable snapshot part per sealed ingest
hour. This is a cost optimization only: it never changes which commits a
query sees. Guarantees:

- Without a `min_commit_token`, freshness of recent commits above the fold
  watermark is listing-immediate, and sealed history below it is complete by
  the seal lemma, so staleness there is zero in healthy operation.
- Every index failure mode (HEAD or snapshot part missing, corrupt, stale, or
  a folder down for hours) degrades to wider listing, never to missing or
  wrong data. A snapshot entry whose object was since retired resolves to
  NotFound, then SnapshotInvalidated, then re-resolve.
- One narrow, documented exception: a folder whose clock runs fast beyond
  `fold_safety_margin` can seal an hour before every writer's flush for it has
  landed. A commit published into that already-sealed bucket is invisible to
  non-token queries until an operator forces a HEAD rebuild (see
  docs/guides/operations/maintenance.md "Catalog fold and verify"). The
  `min_commit_token` (read-your-write) path is unaffected: it always GETs its
  exact commit key directly, never through the snapshot.

The fold protocol (the CAS'd HEAD pointer, watermark computation, and how
each degraded path resolves) is in docs/catalog-and-mvcc.md.

## Recent-hours read path

`max_segments` (default 1024) caps only the sealed, below-watermark set a
resolve extracts from snapshot parts. Recent segments listed live above the
fold watermark and token-resolved segments from an explicit
`min_commit_token` are exempt from that cap, so a hot tenant's open hour and a
read-your-write query both stay queryable through the compaction-lag window
instead of failing on segment count. Their cost is bounded separately by a
per-query request budget that returns a typed `RequestBudgetExceeded` (HTTP
422), distinct from `TooManySegments`.

This changes only which segments are admitted into a resolved snapshot.
Visibility, ordering, erasure, and the listing-immediate freshness of the open
hour are unchanged. The admission seam that enforces it across both query
surfaces, and the end-to-end reachability test, are in docs/query-engine.md.

## Compaction protocol

Compaction is publish-then-supersede (ADR-0018): a run writes its L1
segments, then publishes one `CompactionRecord` with `CreateIfAbsent` naming
its exact input set. Nothing about the inputs is mutated or removed at publish
time; physical removal is a separate, later, horizon-gated step (see Deletion
and GC below). Guarantees:

- **Overlap harmlessness** makes every intermediate state query-correct
  without locking: as long as a compaction record's parts contain every
  sample of every input it names, a resolved snapshot that includes both the
  record's parts and some or all of its listed inputs produces the same result
  as one that includes only the parts, because query-time dedup collapses the
  duplicate candidates. No dedup runs at compaction time, and a reader may
  transiently see an L0 input and its L1 replacement together and still get
  exact answers. This is the atomicity-against-convergence distinction: the
  swap is not atomic, but it converges.
- **Record-count conservation** is enforced pre-publish (ADR-0048): the sum of
  `sample_count` over the input set must exactly equal the sum over the built
  parts, or the run aborts and publishes nothing. Compaction never dedups, so
  any inequality means the merge dropped or invented records.

The mechanism (racing-compactor resolution, `input_set_hash`, the dry-run
gate, and reconciliation of two legitimately-different input sets) is in
docs/catalog-and-mvcc.md.

## Online resharding

`shard_count` is changeable online, per generation, with ingest running
(ADR-0052). A reshard never moves, rewrites, or re-keys existing data: it
appends a new `(generation, shard_count, activation_hour)` entry to the
tenant's provisioning record, and only data ingested from `activation_hour`
onward routes with the new count. Guarantees:

- A query spanning the activation hour returns complete results from both the
  old and new shard ranges: the engine merges a series' samples from any set of
  segments by series identity, and the scan rule lists every written shard
  index for each hour.
- A writer during the transition either observes the new generation before it
  activates or fail-stops on record staleness; it never silently writes to the
  wrong shard set.
- Commit tokens are unaffected: a token minted under any generation resolves
  forever, because token resolution reconstructs the exact key from the
  token's own fields and never consults `shard_count`. Read-your-write holds
  across a reshard in either direction.

The operator command, the activation lead-time floor, what a writer and a
reader each observe, and pre-reshard HEAD acceptance and rejection are in
docs/catalog-and-mvcc.md; see docs/ingest.md for the decrease-specific
straggler slack window.

## Deletion and GC

Every deletion in Ravel is a durable transaction first (a tombstone,
compaction record, or rewrite record), then logical exclusion from newly
resolved snapshots, then physical removal by a sweep. Only a `maintain` mode
process runs compaction, retention, the sweep, and the scrubber; a deployment
with no `maintain` process never deletes an object.

The guarantee this document is normative for: **no object a referenced
snapshot still names is ever deleted by retention.** The protection-horizon
interlock (a validated, deployment-wide config fence) and the
HEAD-referenced-snapshot delete blocker together keep both a pinned in-flight
reader's snapshot and the current HEAD snapshot safe from the sweeper, while
retention still completes and is not permanently blocked. That is the
safety-against-liveness pair: safety is that nothing a live snapshot names is
removed; liveness is that retention nonetheless finishes and reclaims storage.

The sweep rules with their preconditions and anchors, the delete-blocker
behaviour, the timing, and the selective subject erasure staging are in
[deletion and garbage collection](deletion-and-gc.md).

## Selective subject erasure

Selective subject erasure (a GDPR/CCPA/DSAR request naming a *subject*, a
label or attribute value scattered across many hour buckets, shards, and
tiers) is predicate-granular deletion built on the same durable-transaction,
logical-exclusion, physical-removal shape as every other deletion. Query
exclusion is immediate and cache-tight: no query whose snapshot resolves after
the request ack returns matching records, from store or any cache tier, and
in-flight queries drain within `max_query_duration`. The erasure stage bounds
are a guarantee, not a target:

| Stage | Guarantee | Worst-case bound (defaults) |
|---|---|---|
| Query exclusion | No query whose snapshot resolves after the request ack returns matching records, from store or any cache tier | immediate; all in-flight queries drain within `max_query_duration` (30 s) |
| Rewrite complete (`.done`) | Every live commit-record segment a snapshot resolves is free of matching records, verified through the catalog resolver. Index entries and derived datasets carry no subject values, so they are free of matching records by construction | `erasure_rewrite_deadline`, default 72 h; a pending request older than this raises an alarm metric |
| Physical bytes gone from the bucket | Superseded inputs swept | `.done` + `protection_horizon` (default `max_query_duration` + `grace` = 30 s + 24 h) + one sweep interval; with defaults, under 4 days end to end |
| Physical bytes gone from query-node disk caches | Non-durable local copies aged out | sweep + disk-tier entry max-age (24 h); or immediately, by deleting cache directories (ADR-0046: a node with its cache directory deleted mid-flight answers every query correctly) |

The mechanism behind these bounds (the durable predicate record, the
rewrite-and-supersede pass, the `.done` completion gate, the modifiers that
extend the bounds, and the scope and interactions with index, analytics, and
replica state) is in [deletion and garbage collection](deletion-and-gc.md).
