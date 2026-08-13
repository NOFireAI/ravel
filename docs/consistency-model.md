# Consistency, Durability, and Failure Semantics

Normative. Tests in tests/failure/ assert every claim here. If code and this
document disagree, one of them is a bug and the fix updates both.

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

Buffered mode (opt-in per tenant or per request, named "buffered"):
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
- Visibility latency = flush delay + data PUT + commit PUT. The p99 target in
  strict mode under target load is < 1 s.
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

## Catalog snapshot staleness (docs/metric-index-plan.md 5.4)

- The catalog fold (background task, one-shot via `ravel-cli catalog fold`)
  precomputes an immutable snapshot part per sealed ingest hour behind a
  CAS'd HEAD pointer, so `resolve` can skip listing everything at or below
  the snapshot's watermark. This is a cost optimization only: it never
  changes which commits a query sees.
- Without a `min_commit_token`, freshness is bounded by listing behavior for
  the open window above the watermark (unchanged from Phase 1: freshness of
  recent commits is listing-immediate) and by the snapshot for sealed
  history, which is complete by the seal lemma (docs/metric-index-plan.md
  section 2), so staleness there is zero in healthy operation.
- Every index failure mode (HEAD missing/corrupt, snapshot part
  missing/corrupt, a stale cached HEAD, a folder down for hours, folders
  racing the HEAD CAS) degrades to wider Phase 1 listing, never to missing
  or wrong data (docs/metric-index-plan.md 5.3). The index never introduces
  false positives beyond what MVCC already handles: a snapshot entry whose
  object was since retired resolves to NotFound -> SnapshotInvalidated ->
  re-resolve, the existing path above.
- One narrow, documented exception: a folder whose clock runs fast beyond
  `fold_safety_margin` can seal an hour before every writer's flush for it
  has landed. A commit published into that already-sealed bucket is
  invisible to non-token queries until an operator forces a HEAD rebuild
  (see docs/guides/operations.md "Catalog fold and verify"). The
  `min_commit_token` (read-your-write) path is unaffected: it always GETs
  its exact commit key directly, never through the snapshot.

## Recent-hours read path (ADR-0073)

- `max_segments` (default 1024) applies only to the sealed, below-watermark
  set a resolve extracts from snapshot parts. Recent segments (listed live
  above the fold watermark) and token-resolved segments (from an explicit
  `min_commit_token`) are exempt from that cap, so a hot tenant's open hour
  and a read-your-write query holding its own commit token both stay
  queryable through the compaction lag window instead of 422ing on count.
- Their cost is bounded separately, by a per-query S3 request budget
  (`EngineConfig::max_s3_requests`, default 25,000), enforced incrementally
  at the same checkpoints the existing bytes-scanned budget already checks.
  Exceeding it returns a typed `RequestBudgetExceeded` (HTTP 422), distinct
  from `TooManySegments`.
- This changes only which segments are admitted into a resolved snapshot.
  Visibility, ordering, erasure, and the listing-immediate freshness of the
  open hour above are unchanged.
- Both query surfaces enforce this through one admission seam
  (`ravel-query`'s `segment_admission` module): the PromQL engine as of
  RH-T1 (#901), and the SQL executor, the five SQL table providers, and the
  exemplars state as of RH-T2 (#902) — no per-surface check remains outside
  it.
- RH-T3 (#903) is the end-to-end reachability proof: a real `IngestRouter`
  sustains flushes past `max_segments`-worth of L0 objects in the open
  hour, and both real HTTP query surfaces (PromQL and SQL) keep serving
  results bit-identical to a post-compaction read of the same data, while a
  deliberately low request budget still trips the typed
  `RequestBudgetExceeded` rather than hanging or truncating
  (`services/ravel-server/tests/recent_hours_reachability_e2e.rs`). This
  closes the S1-13 finding from the adversarial review.

## Snapshot isolation

- A query resolves one snapshot (a logical set of immutable segments) and
  uses it for its entire execution. Commits, compactions, and deletions that
  land mid-query do not affect it.
- Compaction transactions (Phase 2) atomically swap inputs for outputs in
  new snapshots; both sets remain physically present until GC clears the
  inputs after the protection horizon.

## Compaction protocol (ADR-0018)

- Publish-then-supersede: a compaction run writes its L1 parts, then
  publishes one `CompactionRecord` with `PutMode::CreateIfAbsent`. The
  record names its exact input set (sorted `(writer_id, writer_epoch,
  writer_seq)` list, hashed as `input_set_hash`); nothing about the inputs
  is mutated or removed at publish time. The only ordering the protocol
  depends on is that the record becomes visible before any input is
  physically removed, and that removal is a separate, later, horizon-gated
  step (see "Deletion and GC" below).
- Overlap harmlessness is what makes every intermediate state
  query-correct without locking: as long as a compaction record's parts
  contain every sample of every input it names, a resolved snapshot that
  includes both the record's parts and (some or all of) its listed inputs
  produces the same query result as one that includes only the parts,
  because query-time dedup (see "Cross-segment duplicate samples" above)
  collapses the duplicate candidates. Concretely this means: no dedup runs
  at compaction time, and a reader may transiently see an L0 input and its
  L1 replacement together and still get exact answers.
- Record-count conservation is enforced, not assumed (ADR-0048): before the
  record PUT, the run checks that the sum of `sample_count` over its input
  set exactly equals the sum of `sample_count` over its built parts.
  Compaction is a verbatim page copy for every signal and never dedups, so
  any inequality means the merge dropped or invented records; the run then
  aborts with a typed error and publishes nothing. The L0 inputs remain
  live and queryable, and the abandoned parts age out under the
  unreferenced-part rule. The check runs pre-publish because publish is the
  point of no return: once the record is visible the resolver excludes the
  inputs, and after the protection horizon the sweep removes them
  physically. The gate also runs under dry-run, so a dry run of a bucket
  that would trip it reports the violation.
- Racing compactors over the same sealed bucket are resolved by
  `CreateIfAbsent` picking one record as the winner; a losing compactor's
  parts are simply unreferenced objects that age out under the
  unreferenced-part rule. Two records that legitimately name different
  input sets (rare: concurrent partial seals) are not reconciled
  automatically; the resolver includes both parts sets plus any L0 input
  uncovered by either (harmless per the property above) and raises an
  alarm metric for a human to investigate.

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
request that produced a durable commit (strict mode with at least one flushed
shard); buffered-mode and fully-rejected requests commit no durable data and
write no marker.

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
simply reingests (at-least-once) since no marker exists.

**Honest residuals** (unchanged from ADR-0051 §5): a crash after the commit
PUT but before the marker PUT still yields a duplicate on retry; two
concurrent requests with the same key can both ingest (the window targets
sequential retry after a lost ack, the actual failure mode); and unkeyed
requests get plain at-least-once. Ravel does not claim exactly-once
ingestion.

## Late and skewed data

- Event time is never trusted for discovery. Commit records are bucketed by
  ingest hour; event-time bounds ride along for pruning. Late data is always
  discoverable; queries bound their listing by `max_ingest_lag` plus catalog
  snapshots (Phase 2) for anything older.
- Admission bounds event-time skew (ADR-0010 §8): points with
  `event_ts > ingest_ts + max_future_skew` (default 10 m) or
  `event_ts < ingest_ts - max_ingest_lag` (default 2 h) are rejected with a
  partial-success reason. These bounds are what make the catalog listing
  window sound. Logs and spans enforce the same bounds at admission
  (ADR-0051 §4); spans bound `end_ts` by `max_future_skew` and `start_ts` by
  `max_ingest_lag`.
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

## Online resharding (ADR-0052)

`shard_count` is changeable online, per generation, with ingest running. A
reshard never moves, rewrites, or re-keys existing data: it appends a new
`(generation, shard_count, activation_hour)` entry to the tenant's provisioning
record, and only data ingested from `activation_hour` onward routes with the new
count. Old generations stay readable under their original shard indices until
retention ages their hours out. The operator command is:

```
ravel-cli provision reshard --tenant <t> --signal <s> --shard-count <n> [--lead-hours <L>]
```

- **Activation lead time.** Activation is denominated in ingest-hour buckets and
  is placed `L` hours in the future: `activation_hour = now_hour + L`. `L` must
  satisfy `L >= ceil(C) + 1` hours, where `C` is the router's provisioning-record
  refresh interval (default 60 s, so the floor is 2 hours). The CLI refuses a
  shorter lead. The reason: every live writer re-reads the record at least once
  per `C`, so a lead of at least `ceil(C) + 1` guarantees each writer either
  observes the new generation before it activates or has already fail-stopped on
  record staleness. Never route past an activation a writer has not seen.

- **What a writer observes.** A writer routes new data with the count of the
  latest generation whose `activation_hour <= hour(now)`. It refreshes its view
  of the record on the bounded interval `C`; a writer whose cached view is older
  than `C` and cannot re-read the record fails the flush closed (typed error,
  metrics counter), rather than route on a stale view. So a writer during the
  transition either sees the new generation before it activates or fail-stops --
  it never silently writes to the wrong shard set.

- **What a reader observes.** A reader derives its per-hour scan set from the
  generation history read fresh on every resolve, so its fan-out widens (increase)
  or narrows (decrease) automatically once its own record view is current, with
  no coordination with writers. A query spanning the activation hour returns
  complete results from both the old and new shard ranges: the query engine
  already merges a series' samples from any set of segments by series identity,
  and the scan rule guarantees every written shard index for each hour is listed.
  A snapshot HEAD folded before the reshard (a lower generation count than the
  reader's) is accepted, not rejected, when its watermark predates the activation
  of the first generation it did not know about: the reader lists the newer hours
  the HEAD predates. A pre-reshard HEAD whose watermark instead reaches into hours
  an unknown generation was already active for is rejected loudly (fail-closed
  `FieldMismatch`), because its parts cannot carry the wider shard range those
  hours were written under; serving it would silently omit data below its own
  watermark.

Commit tokens are unaffected: a token minted under any generation resolves
forever, because token resolution reconstructs the exact key from the token's own
fields and never consults `shard_count` (ADR-0052 section 6). Read-your-write
holds across a reshard in either direction. See docs/ingest.md for the
decrease-specific straggler slack window.

## Crash matrix (strict mode)

| Crash point | Data object | Commit record | Ack | Outcome |
|---|---|---|---|---|
| Before data PUT | absent | absent | no | client retries; nothing stored |
| After data PUT, before commit PUT | present (orphan) | absent | no | invisible; GC after grace; client retries |
| After commit PUT, before ack | present | present | no | visible; unkeyed client retry stores a duplicate (see above); a keyed retry replays the marker and stores nothing new, since the marker PUT precedes the ack |
| After ack | present | present | yes | durable and visible |

## Deletion and GC

Deletion is always a durable transaction first (tombstone or compaction
record), then logical exclusion from new snapshots, then physical removal
via a sweeper. One sweeper component implements all rules below; all are
stateless per pass and restartable from zero, and every delete is
idempotent. Reader leases are not implemented: the "not lease-protected"
precondition is vacuously satisfied everywhere below, via a `LeaseCheck`
hook that is a constant "unprotected" (a seam for future slow-consumer
work, not a correctness dependency today). The first four rules anchor on
durable timestamps (never wall-clock at sweep time); `protection_horizon
>= max_query_duration + grace` and `grace` (default 24 h) are shared
across those four. The fifth rule (idempotency marker) anchors on the
marker's own `<ingest_hour>` instead, and its own age gate carries a
forward-skew tolerance the other four don't need (see below the table).

`protection_horizon`, `grace`, `max_query_duration`, and `max_flush_lifetime`
are not per-process knobs each component sets independently. They are recorded
once, deployment-wide, in the durable object `sys/gc` at the bucket root
(ADR-0050 section 4, EC4). The first process to touch a fresh bucket bootstraps
`sys/gc` from the maintain defaults via `CreateIfAbsent` (the defaults satisfy
`protection_horizon >= max_query_duration + grace` by construction; a racing
loser re-reads the winner's object, so a fresh bucket never fails startup), and
only `ravel-cli gc-config set` mutates it, enforcing the constraint at write
time and swapping with `CasVersion`. Every mode then validates itself against
`sys/gc` at startup and refuses to start on a real violation: maintain's
configured horizon and grace must equal the stored values; a query engine's
deadline must be `<= max_query_duration`; a Flight SQL ticket-TTL ceiling must
be `<= protection_horizon - grace`. A process that can read a bootstrapped
`sys/gc` and finds a real violation does not start; there is no "assume
defaults" path, because assumed defaults are precisely the cross-process drift
this object exists to prevent.

| rule | targets | preconditions (ALL must hold) | anchor |
|---|---|---|---|
| orphan (first implementation, ADR-0010 §11; batched re-verify and breaker, ADR-0048 decisions 4-5) | data object with no commit record | age > grace + max_flush_lifetime (default 1 h); record absence re-verified by one fresh LIST shared by every candidate in the pass; the mass-orphan circuit breaker not tripped (or deliberately overridden) | object last_modified |
| superseded input (ADR-0018) | L0 commit records + data objects named in a compaction record's input list | now >= record.created_unix_ns + protection_horizon | compaction record created_unix_ns |
| unreferenced part | `l1/` object referenced by no compaction record in its bucket | a compaction record OR a retention tombstone exists for the bucket (a tombstone makes future compaction impossible, so a record-less part can never be re-referenced; issue #273); age > grace + max_compaction_lifetime; the branch condition (non-reference, or record-absent-and-tombstoned) re-verified immediately before delete | part last_modified |
| retention (ADR-0019) | everything in a tombstoned bucket, tombstone deleted last | now >= tombstone.retired_at_ns + protection_horizon; bucket LIST-verified empty before the tombstone itself is deleted | tombstone retired_at_ns |
| idempotency marker (ADR-0051 §5, EB-9; logs and spans only, run once per signal rather than per shard) | `t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm` marker object | marker's `<ingest_hour>` older than `now_hour - idem_dedup_window_hours - IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS`; a key that fails to parse as `<keyhash32>.<ingest_hour>.idm` is skipped, never deleted | marker key's own `<ingest_hour>` |

The idempotency-marker rule's age gate subtracts
`IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS` (1 h) from its lower bound, the
same tolerance `ravel_ingest::idempotency::read_marker` grants on its own
upper bound: this protects a reader whose clock lags the sweeper's by up
to that much from ever finding a marker gone that `read_marker`'s own
window would still call a Hit.

### Zone-scheduled maintenance (ADR-0065 decision 3)

The unit scan and the sweeper both split a unit's ingest hours into three
zones, recomputed every tick from the seal margin and the tenant's
retention config, never stored: **head** (newer than the seal margin plus
one hour's slack) and **tail** (from a bucket's computed retention expiry
through the protection horizon past it) are evaluated every tick, exactly
as before this split; **interior** (everything else) is memo-gated,
re-verified only every `maintain_interior_reverify` (default 6 h) or
immediately on the natural zone transition into tail at the bucket's
computed expiry. The sweeper mirrors this: its per-tick pass lists only
the head and tail hour prefixes for the superseded-input and
unreferenced-part rules, and a full-keyspace pass on the same
`maintain_interior_reverify` cadence is the safety net that eventually
covers every hour, including one an invalidation gap or a scheduling bug
left permanently out of the per-tick set.

This bounds, not eliminates, promptness for the affected rules: a
tombstoned interior bucket's physical sweep, and an operator hold's
effect on interior buckets, land no later than `maintain_interior_reverify`
after they become eligible, rather than on the next tick. This is a
documented latency, never a correctness gap -- retention and legal-hold
checks in `maintain_bucket` still run whenever a bucket is actually
evaluated, and the safety-net pass guarantees every hour is eventually
re-evaluated. Orphan GC is never zone-scoped: L0 data keys carry no
ingest-hour component, so there is no hour-scoped prefix to restrict its
listing to, and it always lists the whole shard on every pass, per-tick or
safety-net alike.

`maintain_interior_reverify` is one operator knob shared by both halves of
the split (the scan's interior re-verify interval and the sweeper's
full-pass cadence): setting it to a non-positive value disables the
skip in both, reproducing the pre-split full-scan-every-tick behavior.

### Bounded intra-process unit concurrency (ADR-0065 decision 2)

`--maintain-unit-concurrency` (default 4) runs a tick's owned units through
`run_bounded` (`crates/ravel-fleet`'s order-preserving buffered fan-out)
instead of the strictly sequential per-shard walk. This changes intra-tick
resource shape, not any promptness bound in this document: `run_bounded`
still awaits every owned unit's retention-and-compaction pass and sweep to
complete before the tick's results (including the deletion rules in the
table above) are used, exactly as the sequential walk did. A tick still
either finishes all of its owned units or it hasn't finished, regardless of
`--maintain-unit-concurrency`'s value; concurrency only changes how many of
those units are in flight against the store at once, and the sweeper's
head/tail-vs-interior zone split above remains the only source of a
documented promptness bound.

This does depend on every owned unit's operation eventually returning
(`Ok` or `Err`), not merely running slowly: `run_bounded` preserves input
order in its output, so a unit that never returns blocks every
later-ordered unit's result from being collected even after their own work
has actually completed against the store, and therefore blocks this tick,
this tenant's discovery-cycle entry, and (since `run_discovery_cycle` walks
tenants sequentially within one `run_loop` iteration) every tenant ordered
after it this cycle. The stalled-units gauge below cannot observe this case
either -- `observe_unit_tick` records only once a unit's future resolves.
The design's actual backstop for a non-terminating unit is decision 1: a
`run_loop` stuck inside the discovery branch of its `select!` also stops
polling its heartbeat branch, so the process stops heartbeating and a live
sibling treats it as stale and takes over its units within `3 * H`, the
same recovery path a hard process hang would take, not a per-unit lease.
`ravel_maintain_units_stalled` (docs/guides/operations.md) covers the
distinct, and more common, case of a unit whose operation keeps returning
`Err` on schedule rather than never returning at all.

- Orphan GC (data objects with no commit record) considers only objects
  with last_modified age > grace + max_flush_lifetime. Writers abandon any
  flush older than max_flush_lifetime and never publish it afterward; the
  interlock is what makes orphan deletion safe (ADR-0010 §11). A pass runs
  in three phases: candidate selection over one listing of the shard's
  data objects, checked against the shard's commit-record identities from
  one initial commit-prefix LIST; one fresh, strongly consistent LIST of
  that same commit prefix, shared by every surviving candidate, dropping
  any whose identity now appears (the batched re-verify, ADR-0048 decision
  5 -- one extra LIST per pass, not one per candidate); then the
  mass-orphan circuit breaker gate below. Deletes are all-or-nothing: a
  tripped, non-overridden breaker deletes zero candidates that pass.
- The mass-orphan circuit breaker (ADR-0048 decision 4) trips when a
  pass's surviving candidate count is at least `orphan_breaker_min_count`
  (default 50) AND exceeds `orphan_breaker_max_ratio` (default 0.10) of
  the shard's listed data objects -- both conditions must hold, so a tiny
  shard's small orphan count never trips on ratio alone, and a genuinely
  mass orphan population trips regardless of shard size. This shape (many
  record-less objects appearing at once) is the signature of commit
  records lost out-of-band, not routine cleanup: the same physical delete
  that is safe for a handful of true orphans would be permanent data loss
  if applied to a shard whose commit records vanished by accident. A
  tripped pass deletes nothing and halts, but the halt is not a latch on
  the underlying loss: the predicate is recomputed from live counts on
  every pass, with no memory of a prior trip, so a shard can stop tripping
  while the missing commit records are still missing. Two distinct
  mechanisms produce this: dilution, where new well-recorded writes to the
  same shard lower the orphan ratio below `orphan_breaker_max_ratio` even
  though the orphan count itself hasn't changed (55 orphans among 500
  objects trips at 11%; 200 further writes with no data loss give 55/700 =
  7.9%, which does not trip, and the 55 still-orphaned objects are
  deleted); and partial restoration, where an operator restores some but
  not all of the missing records and the remaining candidate count crosses
  below `orphan_breaker_min_count` (55 orphans trips; restoring 6 leaves
  49 candidates, under the default floor of 50, so the pass stops tripping
  and deletes the other 49 before they were restored). An operator relying
  on the breaker to hold a shard open until every missing record is back
  is relying on a guarantee the code does not provide: the only durable
  way to stop deletion is to restore records (or use
  `CompactorConfig::force_orphan_gc` deliberately in the other direction,
  see below) before the next pass runs, not to assume the trip persists.
  The breaker also has three scope limits, tracked as open gaps in issue
  #500 rather than fixed by design: it evaluates one (tenant, signal,
  shard) in isolation, with no cross-shard or cross-tenant aggregation, so
  loss spread thin across many shards can stay under every shard's
  threshold; it never trips below `orphan_breaker_min_count` regardless of
  ratio, so small-shard total loss is always deletable; and because it
  only trips once the ratio exceeds `orphan_breaker_max_ratio` (10%), up
  to that fraction of a large shard's objects is deletable in a single
  pass without ever tripping. An operator can still deliberately force a
  pass through a trip via
  `CompactorConfig::force_orphan_gc`, a one-shot flag the server itself
  never sets. The other two sweep rules are unaffected by a tripped
  orphan breaker and still run, since they are anchored on durable records
  an operator or compactor deliberately wrote, never on record absence.
- Superseded-input and unreferenced-part deletion never depend on reader
  leases or on removing an input before its compaction record is durable;
  the horizon alone bounds how long a pinned query can still need an
  input, and orphan-GC-style convergence handles crash remnants (a
  compactor that died mid-publish leaves record-less parts, which the
  unreferenced-part rule collects once old enough).
- Retention sweep deletes in a fixed order: L0 commit records, compaction
  records, L0 data objects, L1 parts, then the tombstone last, after a
  verifying LIST of both `c/<shard>/<hour>/` (must contain only the
  tombstone by then) and `l1/<shard>/<hour>/` (must be empty). Any
  residue found by that LIST: leave the tombstone in place and retry on
  the next pass. Expiry evaluation reuses the bucket's already-decoded
  commit and compaction records (no footer reads needed), taking
  max(max_event_ts) across both.
- Observing a tombstone invalidates that bucket's cached commit and
  compaction records (the trigger ADR-0010 §10 promises).
- Retention is durable and irreversible: raising a tenant's retention
  window after a bucket is tombstoned never resurrects it. A token whose
  bucket is tombstoned resolves as satisfied with zero segments, not as
  `unsatisfiable token`; the data was retired on purpose, not lost to a
  race.
- A store NotFound on a segment pinned by a running query surfaces as
  SnapshotInvalidated; the frontend re-resolves and retries once before
  failing the query.

## Deletion guarantees (ADR-0064 selective subject erasure)

Everything under "Deletion and GC" above destroys whole objects at bucket
granularity (age-based retention, supersession, orphan GC). Selective subject
erasure (a GDPR/CCPA/DSAR request naming a *subject* -- a label or attribute
value scattered across many hour buckets, shards, and tiers) is
predicate-granular deletion built on the same three-step shape every other
deletion in Ravel has: a durable transaction first, logical exclusion from new
snapshots second, physical removal third. The mechanism is a durable erasure
request, immediate query-time exclusion, then an asynchronous
rewrite-and-supersede pass in Maintain, then the existing horizon-gated
physical sweep. See the lifecycle diagram:
[diagrams/erasure-lifecycle.svg](diagrams/erasure-lifecycle.svg).

An erasure request is submitted under the Admin credential
(`ravel-cli erase submit`) and lands as a durable, immutable predicate record
at `t/<tenant_hash>/<signal>/del/<request_id>.dreq` (`CreateIfAbsent`). The
predicate is a conjunction of exact-match label/attribute matchers plus an
optional event-time range; v1 predicates are equality-only (exact semantics by
default). The `CreateIfAbsent` ack timestamp is `t = 0` -- the point from which
every bound below is measured.

### The guarantee, stage by stage

| Stage | Guarantee | Worst-case bound (defaults) |
|---|---|---|
| Query exclusion | No query whose snapshot resolves after the request ack returns matching records, from store or any cache tier | immediate; all in-flight queries drain within `max_query_duration` (30 s) |
| Rewrite complete (`.done`) | Every live segment, index entry, and derived dataset is free of matching records | `erasure_rewrite_deadline`, default 72 h; a pending request older than this raises an alarm metric |
| Physical bytes gone from the bucket | Superseded inputs swept | `.done` + `protection_horizon` (default `max_query_duration` + `grace` = 30 s + 24 h) + one sweep interval -- with defaults, under 4 days end to end |
| Physical bytes gone from query-node disk caches | Non-durable local copies aged out | sweep + disk-tier entry max-age (24 h); or immediately, by deleting cache directories (ADR-0046: a node with its cache directory deleted mid-flight answers every query correctly) |

Each stage in detail:

- **Query exclusion is immediate and cache-tight.** Snapshot resolution lists
  `t/<th>/<sig>/del/` per resolve and attaches every pending `.dreq`
  predicate to the resolved snapshot; the scan/materialization layer filters
  matching series, rows, and spans out of results *after* fetch, *after*
  cache, before any result reaches the caller. So the filter applies to
  cached bytes exactly as to freshly-fetched bytes, and no cache tier can
  surface an excluded record. A query already running keeps its pinned
  snapshot (snapshot isolation) and is bounded by `max_query_duration`. This
  is the bounded *bridge* between request and physical rewrite; alone it
  would be "query exclusion, not erasure," which is why the rewrite pass
  below is not optional.

- **The rewrite pass physically erases by rewrite-and-supersede.** A Maintain
  rule reads each in-scope sealed bucket's live segments, decodes, drops the
  matching records, re-encodes into new segments of the *same frozen format
  version* (RSEG/RLOG/RSPAN -- producing new valid instances of a frozen
  format is not a format change), and publishes one `RewriteRecord` that
  atomically supersedes its named inputs, exactly as a `CompactionRecord`
  does. A conservation gate asserts `sum(output sample_count) + sum(dropped
  counts) == sum(input sample_count)` pre-publish; any inequality aborts and
  publishes nothing, leaving the inputs live (the ADR-0048 gate rearranged
  for deliberate drops). Unlike compaction, overlap harmlessness does *not*
  hold for a rewrite -- its outputs deliberately lack records the inputs
  contain -- so the query-time filter (Query exclusion, above) stays active
  for a request's
  predicate until its `.dreq` is removed, which by construction happens only
  after no resolvable snapshot can still reference a pre-rewrite input.
  Correctness never depends on the per-bucket compaction/rewrite
  serialization; that serialization is an efficiency measure.

- **The 72 h rewrite bound derives from the maintenance ownership cadence
  (ADR-0065).** The rewrite pass runs on the Maintain worker that owns each
  `(tenant_hash, signal, shard)` unit under ADR-0065's rendezvous-hash
  ownership. A dead or wedged owner's units are taken over by a live sibling
  within `3 * H` (heartbeat interval `H`, default 60 s, so ~3 min), and even
  a terminal *interior*-zone bucket -- where a subject's historical data
  most often sits -- is re-verified at least every `maintain_interior_reverify`
  (default 6 h), or immediately when EJ's rewrite orders drive it out of
  terminal state through ADR-0065's `invalidate` hook. So every in-scope
  bucket is revisited on a cadence far tighter than the 72 h
  `erasure_rewrite_deadline`; the deadline is the outer alarm, not the
  expected latency. Completion is *verified, not assumed*: the pass writes
  the `.done` record only when every bucket in the request's scope has a live
  record set whose every non-superseded rewrite record names this request in
  its drops.

- **Physical removal reuses the existing sweep.** A rewrite's superseded
  inputs become inputs to `sweep_superseded`, deleted after
  `protection_horizon`, under the same `LegalHoldCheck` gate as every other
  delete (see "Deletion and GC"). The `.dreq` itself contains the subject
  identifier and is therefore not kept forever: a sweep rule deletes it once
  its `.done` exists, `now >= done.created_unix_ns + protection_horizon`, and
  the legal-hold check passes -- the horizon wait guarantees the query-time
  filter only disappears after no resolvable snapshot can still include a
  pre-rewrite input. The `.done` record carries only a hash of the canonical
  predicate, per-bucket dropped counts, and timestamps -- no subject
  identifier -- and is permanent, deny-delete audit evidence for every role
  (ADR-0055 amendment).

### Modifiers to the bound

Each of these extends the bounds above rather than being silently absorbed
into them. An operator with erasure obligations must budget them deliberately.

- **`+D` -- bucket-default Object Lock retention.** If the operator enabled
  compliance-mode default retention `D` (the out-of-band step ADR-0042
  documents; Ravel cannot set or enforce per-object retention through
  `object_store`), S3 itself refuses the sweep's deletes until each object's
  retain-until passes, so the physical-removal bound becomes `max(bound, D)`.
  docs/object-store-contract.md "Required bucket configuration" advises
  operators with erasure obligations to prefer scoped legal holds over
  blanket default retention, or to keep `D` inside their erasure SLA.

- **`+E_v` -- bucket versioning.** On a versioned bucket every physical delete
  becomes a soft delete, and the noncurrent version survives until the
  operator's required `NoncurrentDays = E_v` expiration rule reaps it. Every
  physical-erasure and retention bound then gains `+E_v`. Versioning without
  that expiration rule is an unsupported configuration that silently inverts
  every deletion guarantee here (S4-12); see the object-store contract.

- **paused -- overlapping legal hold.** The rewrite pass and the
  superseded-input sweep both consult `LegalHoldCheck`; a bucket under an
  overlapping hold is skipped, the request stays pending, and its status
  records `deferred: legal hold <scope>`. The erasure-latency clock is
  explicitly *paused* for held ranges: a hold preserves evidence against
  destruction and wins over erasure until an authorized human clears it via
  the separate Admin-only legal-hold operation (ADR-0042/ADR-0055). Erasure
  never clears a hold, and no re-submission is needed -- the next pass
  completes once the hold clears. Query-time exclusion (above) stays active
  throughout: a hold does not oblige Ravel to keep *serving* the data.

### Scope and interactions

- **Catalog and derived state carry no subject values.** `SnapshotEntry`,
  `SnapshotPartHeader`, and name postings hold identities, hashes, counts,
  and metric names -- never label/attribute *values* -- so the deny-deleted
  `catalog/`, `prov`, and `sys/*` prefixes are disjoint from subject erasure
  by construction. This holds *only if* subject identifiers appear as
  label/attribute values and never inside metric names (a documented
  requirement; see docs/object-store-contract.md "Required bucket
  configuration" point 5). Superseded catalog entries resolve to NotFound ->
  SnapshotInvalidated -> re-resolve and the next fold rebuilds over the
  rewrite outputs.

- **The query-audit keyspace is the one excluded derived store.** It may
  retain matcher values from audited query text (S4-13); it is deny-deleted
  under ADR-0055 and owned by epic EL (#462), which will hash/tokenize
  matcher values. Until EL lands, the erasure guarantee explicitly does not
  reach the audit keyspace.

- **Erasure applies to the primary bucket only.** Replicas or external
  backups are outside Ravel's deletion reach by definition (ADR-0058/0059 DR
  posture); an operator with replicated buckets must apply the same lifecycle
  discipline (docs/object-store-contract.md) to replicas. Per-tenant KMS
  crypto-erasure (epic EL) is the complementary, backup-reaching,
  tenant-granularity layer to this ADR's subject-granularity physical
  erasure.
