# Deletion and Garbage Collection

Mechanism, not guarantee. The guarantee these mechanics implement, that no
object a referenced snapshot still names is ever deleted by retention, is
stated in [the consistency model](consistency-model.md#deletion-and-gc), which
is normative for it. This page is how that guarantee is achieved, plus the
timing and preconditions of every sweep rule.

Only a `maintain` mode process runs compaction, retention, the sweep, and the
scrubber; `all` mode does not. A deployment with no `maintain` process never
deletes an object: its L0 segments accumulate unmerged, retention windows have
no effect, and nothing reclaims storage. See
docs/guides/operations/maintenance.md.

## The sweep and its anchors

Deletion is always a durable transaction first (tombstone or compaction
record), then logical exclusion from new snapshots, then physical removal
via a sweeper. One sweeper component implements all rules below; all are
stateless per pass and restartable from zero, and every delete is
idempotent. Reader leases are not implemented: the "not lease-protected"
precondition holds trivially everywhere below, because the `LeaseCheck`
hook always answers "unprotected" and nothing depends on it for
correctness. The first four rules anchor on
durable timestamps (never wall-clock at sweep time); `protection_horizon
>= max_query_duration + grace + clock_skew_allowance` and `grace`
(default 24 h) are shared across those four. The fifth rule (idempotency
marker) anchors on the marker's own `<ingest_hour>` instead, and its own age
gate carries a forward-skew tolerance the other four don't need (see below the
table).

`protection_horizon`, `grace`, `max_query_duration`, and `max_flush_lifetime`
are not per-process knobs each component sets independently. They are recorded
once, deployment-wide, in the durable object `sys/gc` at the bucket root
(ADR-0050 section 4). The first process to touch a fresh bucket bootstraps
`sys/gc` from the maintain defaults via `CreateIfAbsent` (the defaults satisfy
`protection_horizon >= max_query_duration + grace + clock_skew_allowance` by
construction; a racing loser re-reads the winner's object, so a fresh bucket
never fails startup), and only `ravel-cli gc-config set` mutates it, enforcing
the constraint at write time and swapping with `CasVersion`. Every mode then
validates itself against `sys/gc` at startup and refuses to start on a real
violation: maintain's configured horizon and grace must equal the stored
values; a query engine's deadline must be `<= max_query_duration`; a Flight SQL
ticket-TTL ceiling must be `<= protection_horizon - grace`. A process that can
read a bootstrapped `sys/gc` and finds a real violation does not start; there is
no "assume defaults" path, because assumed defaults are precisely the
cross-process drift this object exists to prevent.

## The GC/reader interlock

**The GC/reader interlock is this validated config, and it is a real fence.**
The four horizon-gated rules below enforce a
reader's pinned snapshot purely by `protection_horizon` arithmetic against a
durable anchor; there is no store-side lock on the objects a live reader holds.
What makes that arithmetic sound against a sweeper whose clock disagrees is the
`clock_skew_allowance` term in the constraint. A reader holds a resolved
snapshot for at most `max_query_duration`. A sweeper deletes an anchored object
once its own clock reads `now >= anchor + protection_horizon`; if that clock
leads true time (and the reader's) by up to `clock_skew_allowance`, the sweeper
reaches its threshold that much early in true time. So the horizon must budget
for both: the reader's full hold **and** the sweeper's lead, with `grace`
absorbing any residual. The bound

```
protection_horizon >= max_query_duration + grace + clock_skew_allowance
```

is exactly that budget. The `clock_skew_allowance` is not stored in `sys/gc`
(the object's format is a frozen contract); it is a config input to the
constraint, supplied from the sweeper's
`CompactorConfig::clock_skew_allowance_ns` (default 5 min). The fence is
enforced at two choke points, and needs both to be sound against the sweeper
that actually deletes:

1. **Write time.** `ravel-cli gc-config set` (the single `sys/gc` mutation path,
   `ravel_maintain::set_gc_config`) refuses fail-closed any config that does not
   meet the bound, taking the skew from the CLI's `--clock-skew-allowance`, and
   the bootstrap defaults meet it by construction. A skew-uncovered horizon
   cannot be written in the first place.
2. **Maintain startup.** The write-time skew and the running sweeper's
   `CompactorConfig::clock_skew_allowance_ns` are independent knobs: a
   deployment could write `sys/gc` with a 5 min skew while running sweepers
   configured with a *larger* one, leaving the durable horizon skew-uncovered
   for the process that actually deletes (the write fence alone does not
   close this). So at maintain startup the
   server RE-ASSERTS the same bound with the skew taken from THIS running
   sweeper's config (`ravel_maintain::validate_maintain_skew`, called from
   `maintain::spawn` on the shipping `start` -> `spawn` -> `run_loop` path,
   before any delete). A violation fails closed: `spawn` returns
   `GcConfigError::MaintainSkewUncovered` and the sweep loop is never entered, so
   startup fails before any listener binds rather than let the sweeper delete a
   pinned snapshot. (The must-match check `validate_maintain` cannot catch this
   on its own; it only requires the configured horizon and grace to EQUAL the
   stored values, and the skew term is in neither field.)

Because both fences hold, **no reachable sweeper config can delete an object a
pinned reader still holds**: a skew-uncovered horizon can neither be written nor
run against. Residual not covered by the config fence: a sweeper whose *real*
clock skew exceeds its OWN declared `clock_skew_allowance` (a mis-measurement of
the hardware, not a config mismatch the startup re-assert now catches), or a
query that runs longer than the declared `max_query_duration` (the query
engine's own deadline enforcement, validated `<= max_query_duration` at startup,
is what keeps the latter honest). Both are mis-declarations of the deployment's
own parameters, not gaps a correctly declared config leaves open.

## Sweep rules

| rule | targets | preconditions (ALL must hold) | anchor |
|---|---|---|---|
| orphan (first implementation, ADR-0010 §11; batched re-verify and breaker, ADR-0048 decisions 4-5) | data object with no commit record | age > grace + max_flush_lifetime (default 1 h); record absence re-verified by one fresh LIST shared by every candidate in the pass; the mass-orphan circuit breaker not tripped (or deliberately overridden) | object last_modified |
| superseded input (ADR-0018) | L0 commit records + data objects named in a compaction record's input list | now >= record.created_unix_ns + protection_horizon | compaction record created_unix_ns |
| unreferenced part | `l1/` object referenced by no compaction record in its bucket | a compaction record OR a retention tombstone exists for the bucket (a tombstone makes future compaction impossible, so a record-less part can never be re-referenced); age > grace + max_compaction_lifetime; the branch condition (non-reference, or record-absent-and-tombstoned) re-verified immediately before delete | part last_modified |
| retention (ADR-0019, HEAD-reachability gate ADR-0020) | everything in a tombstoned bucket, tombstone deleted last | now >= tombstone.retired_at_ns + protection_horizon; the live catalog HEAD snapshot names no object inside the bucket (delete blocker, see below); bucket LIST-verified empty before the tombstone itself is deleted | tombstone retired_at_ns |
| idempotency marker (ADR-0051 §5; logs and spans only, run once per signal rather than per shard) | `t/<tenant_hash>/<signal>/idem/<keyhash32>.<ingest_hour>.idm` marker object | marker's `<ingest_hour>` older than `now_hour - idem_dedup_window_hours - IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS`; a key that fails to parse as `<keyhash32>.<ingest_hour>.idm` is skipped, never deleted | marker key's own `<ingest_hour>` |

## HEAD-referenced snapshot delete blocker

**A HEAD-referenced snapshot blocks retention deletion (ADR-0020).** The
protection-horizon arithmetic above bounds a *pinned in-flight reader* against
the current snapshot's history; it does not on its own prove that the *current*
HEAD snapshot has stopped naming the bucket. A retention tombstone is written at
its own bucket's ingest-hour key, which is `R` (the tenant's retention window)
behind the fold watermark, so it lands far outside the fold's fixed
near-watermark reconcile window. To close the gap, the physical retention sweep,
before deleting anything in a bucket, loads the `(tenant, signal)` HEAD and the
snapshot part(s) whose hour range covers the bucket's ingest hour, and refuses
the delete if any snapshot entry names an object inside the bucket (same shard
and ingest hour). Nothing is deleted and the tombstone is left in place, so
bucket-wide exclusion still holds and a later sweep finishes the job. The block
is cleared by the fold: its retention-frontier reconcile pass re-lists the
snapshot-named hours at or approaching the tenant's retirement frontier (derived
from the tenant's durable retention window and the protection horizon, bounded
per fold and carried across folds), observes the out-of-window tombstone, and
drops the bucket from the next published snapshot. Once HEAD no longer names the
bucket, the sweep proceeds. Together these guarantee that **no object a
HEAD-referenced snapshot still names is ever deleted by retention**, while
retention still completes (it is not permanently blocked). Without the
blocker, a query that resolved a stale snapshot naming an already-deleted
object would fail permanently with `SnapshotInvalidated` (503).

HEAD read failures are explicit. An **absent** HEAD is NOT a block: with no
snapshot naming anything, the sweep proceeds (ADR-0020: the catalog index is a
pure optimization; a missing HEAD degrades to listing). A HEAD, or a covering
part, that is **present but unreadable** (undecodable, checksum/blake3 mismatch,
an unsupported newer format, or a HEAD-named part that is missing) blocks the
sweep **fail-closed**: non-reachability cannot be proven from data that cannot
be read, and a wrongly-permitted delete is unrecoverable while a delayed one is
not. HEAD and each covering part are read at most once per sweep pass (cached
across the pass's buckets), not once per bucket.

The idempotency-marker rule's age gate subtracts
`IDEM_MARKER_FORWARD_SKEW_TOLERANCE_HOURS` (1 h) from its lower bound, the
same tolerance `ravel_ingest::idempotency::read_marker` grants on its own
upper bound: this protects a reader whose clock lags the sweeper's by up
to that much from ever finding a marker gone that `read_marker`'s own
window would still call a Hit.

## Orphan GC

- Orphan GC (data objects with no commit record) considers only objects
  with last_modified age > grace + max_flush_lifetime. Writers abandon any
  flush older than max_flush_lifetime and never publish it afterward; the
  interlock is what makes orphan deletion safe (ADR-0010 §11). A pass runs
  in three phases: candidate selection over one listing of the shard's
  data objects, checked against the shard's commit-record identities from
  one initial commit-prefix LIST; one fresh, read-after-write consistent LIST of
  that same commit prefix, shared by every surviving candidate, dropping
  any whose identity now appears (the batched re-verify, ADR-0048 decision
  5, one extra LIST per pass, not one per candidate); then the
  mass-orphan circuit breaker gate below. Deletes are all-or-nothing: a
  tripped, non-overridden breaker deletes zero candidates that pass.
- The mass-orphan circuit breaker (ADR-0048 decision 4) trips when a
  pass's surviving candidate count is at least `orphan_breaker_min_count`
  (default 50) AND exceeds `orphan_breaker_max_ratio` (default 0.10) of
  the shard's listed data objects; both conditions must hold, so a tiny
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
  The breaker also has three scope limits, known and deliberate rather than
  fixed by design: it evaluates one (tenant, signal,
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

## Superseded input and unreferenced part

- Superseded-input and unreferenced-part deletion never depend on reader
  leases or on removing an input before its compaction record is durable;
  the horizon alone bounds how long a pinned query can still need an
  input, and orphan-GC-style convergence handles crash remnants (a
  compactor that died mid-publish leaves record-less parts, which the
  unreferenced-part rule collects once old enough).

## Retention sweep order

- Retention sweep deletes in a fixed order: L0 commit records, compaction
  records, L0 data objects, L1 segments, then the tombstone last, after a
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

## Selective subject erasure (ADR-0064)

Everything above destroys whole objects at bucket
granularity (age-based retention, supersession, orphan GC). Selective subject
erasure (a GDPR/CCPA/DSAR request naming a *subject*, a label or attribute
value scattered across many hour buckets, shards, and tiers) is
predicate-granular deletion built on the same three-step shape every other
deletion in Ravel has: a durable transaction first, logical exclusion from new
snapshots second, physical removal third. The mechanism is a durable erasure
request, immediate query-time exclusion, then an asynchronous
rewrite-and-supersede pass in Maintain, then the existing horizon-gated
physical sweep. The stage bounds this mechanism achieves are a guarantee and
live in [the consistency model](consistency-model.md#selective-subject-erasure).
See the lifecycle diagram:
[diagrams/erasure-lifecycle.svg](diagrams/erasure-lifecycle.svg).

An erasure request is submitted under the Admin credential
(`ravel-cli erase submit`) and lands as a durable, immutable predicate record
at `t/<tenant_hash>/<signal>/del/<request_id>.dreq` (`CreateIfAbsent`). The
predicate is a conjunction of exact-match label/attribute matchers plus an
optional event-time range; v1 predicates are equality-only (exact semantics by
default). The `CreateIfAbsent` ack timestamp is `t = 0`, the point from which
every bound is measured.

### The stages in detail

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
  version* (RSEG/RLOG/RSPAN, producing new valid instances of a frozen
  format is not a format change), and publishes one `RewriteRecord` that
  atomically supersedes its named inputs, exactly as a `CompactionRecord`
  does. A conservation gate asserts `sum(output sample_count) + sum(dropped
  counts) == sum(input sample_count)` pre-publish; any inequality aborts and
  publishes nothing, leaving the inputs live (the ADR-0048 gate rearranged
  for deliberate drops). Unlike compaction, overlap harmlessness does *not*
  hold for a rewrite (its outputs deliberately lack records the inputs
  contain), so the query-time filter (Query exclusion, above) stays active
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
  a terminal *interior*-zone bucket, where a subject's historical data
  most often sits, is re-verified at least every `maintain_interior_reverify`
  (default 6 h), or immediately when erasure's rewrite orders drive it out of
  terminal state through ADR-0065's `invalidate` hook. So every in-scope
  bucket is revisited on a cadence far tighter than the 72 h
  `erasure_rewrite_deadline`; the deadline is the outer alarm, not the
  expected latency. Completion is *verified, not assumed*, and verified
  through the SAME resolver a query runs: the pass writes the `.done` record
  only when, for every bucket in the request's scope, the catalog resolver
  (`resolve_rewrite_supersession`, the exact supersession chase a snapshot
  resolve and the index fold use) serves nothing that could carry the subject:
  no live raw L0 input, no un-rewritten compaction part, and no live
  sibling rewrite whose drops omit this request. Deriving completion from the
  rewrite pass's own one-hop live-record view instead would let a `.done` land
  while a snapshot still resolves an L0 input the one hop never excluded
  (ADR-0064 §4); the completion gate blocks exactly
  that.

- **Physical removal reuses the existing sweep.** A rewrite's superseded
  inputs become inputs to `sweep_superseded`, deleted after
  `protection_horizon`, under the same `LegalHoldCheck` gate as every other
  delete. The `.dreq` itself contains the subject
  identifier and is therefore not kept forever: a sweep rule deletes it once
  its `.done` exists, `now >= done.created_unix_ns + protection_horizon`, and
  the legal-hold check passes; the horizon wait guarantees the query-time
  filter only disappears after no resolvable snapshot can still include a
  pre-rewrite input. The `.done` record carries only a hash of the canonical
  predicate, per-bucket dropped counts, and timestamps, no subject
  identifier, and is permanent, deny-delete audit evidence for every role
  (ADR-0055 amendment).

### Modifiers to the bound

Each of these extends the bounds rather than being silently absorbed
into them. An operator with erasure obligations must budget them deliberately.

- **`+D`, bucket-default Object Lock retention.** If the operator enabled
  compliance-mode default retention `D` (the out-of-band step ADR-0042
  documents; Ravel cannot set or enforce per-object retention through
  `object_store`), S3 itself refuses the sweep's deletes until each object's
  retain-until passes, so the physical-removal bound becomes `max(bound, D)`.
  docs/object-store-contract.md "Required bucket configuration" advises
  operators with erasure obligations to prefer scoped legal holds over
  blanket default retention, or to keep `D` inside their erasure SLA.

- **`+E_v`, bucket versioning.** On a versioned bucket every physical delete
  becomes a soft delete, and the noncurrent version survives until the
  operator's required `NoncurrentDays = E_v` expiration rule reaps it. Every
  physical-erasure and retention bound then gains `+E_v`. Versioning without
  that expiration rule is an unsupported configuration that silently inverts
  every deletion guarantee here; see the object-store contract.

- **paused, overlapping legal hold.** The rewrite pass and the
  superseded-input sweep both consult `LegalHoldCheck`; a bucket under an
  overlapping hold is skipped, the request stays pending, and its status
  records `deferred: legal hold <scope>`. The erasure-latency clock is
  explicitly *paused* for held ranges: a hold preserves evidence against
  destruction and wins over erasure until an authorized human clears it via
  the separate Admin-only legal-hold operation (ADR-0042/ADR-0055). Erasure
  never clears a hold, and no re-submission is needed; the next pass
  completes once the hold clears. Query-time exclusion (above) stays active
  throughout: a hold does not oblige Ravel to keep *serving* the data.

- **+ replica residue, a level 1 or level 2 DR replica exists.** When the operator
  runs a cross-region cross-account replica (the DR posture of ADR-0077
  decision 1; see [guides/disaster-recovery.md](guides/disaster-recovery.md)),
  a subject erased on the primary survives on the replica until the replica's
  own noncurrent-version expiration reaps it. With `DeleteMarkerReplication`
  enabled, the primary's simple DELETE replicates as a delete marker and the
  replica's copy is physically gone within **replication lag + `E_v_r`** after
  the primary sweep (`E_v_r` is the replica's `NoncurrentDays` rule). This is
  additive to the primary's own `+E_v`: the primary carries erased-subject
  residue for up to `E_v`, the replica for up to replication lag + `E_v_r`. A
  level 2 replica under bucket-default Object Lock retention `D_r` further
  extends the replica bound to `max(replication lag + E_v_r, D_r)`, exactly as
  `+D` does on the primary. Erasure applies only to the primary bucket; the
  replica is written by the platform's replication channel, and the operator
  must apply the same lifecycle discipline to it (ADR-0077 decision 1,
  Consequences).

  > **Unsupported configuration: a replica without `DeleteMarkerReplication`.**
  > Every Ravel delete is a simple DELETE, which becomes a delete marker on a
  > versioned bucket and replicates **only** when `DeleteMarkerReplication` is
  > enabled. A replica configured without it never receives the delete markers
  > that reap erased (or retention-, orphan-, supersession-deleted) bytes, so
  > **erased bytes persist on the replica indefinitely.** For any deployment
  > with erasure obligations this is an **unsupported configuration**:
  > `DeleteMarkerReplication` is mandatory (ADR-0077 decision 1;
  > [guides/disaster-recovery.md](guides/disaster-recovery.md)).

### Scope and interactions

- **Why the `.done` guarantee needs only the commit-record pass.** The
  completion pass walks `c/<shard>/<hour>/` commit records and verifies the
  segment data a snapshot resolves is subject-free. It does NOT separately
  walk index objects or analytics, and it does not need to, because neither
  can hold a record matching an erasure subject:
  - **Index objects carry no subject values.** `SnapshotEntry`,
    `SnapshotPartHeader`, and name postings hold identities, hashes, counts,
    and metric names, never label/attribute *values*. So the deny-deleted
    `catalog/`, `prov`, and `sys/*` prefixes are disjoint from subject
    erasure by construction. This holds *only if* subject identifiers appear
    as label/attribute values and never inside metric names (a documented
    requirement; see docs/object-store-contract.md "Required bucket
    configuration" point 5). Superseded catalog entries resolve to NotFound,
    then SnapshotInvalidated, then re-resolve, and the next fold rebuilds over the
    rewrite outputs.
  - **ADR-0028 analytics/derived datasets are a pure query-time stage, not a
    persisted store.** `ravel-analytics` carries no clock, IO, object-store,
    or catalog (docs/analytics.md): every analytic runs in memory over query
    output, *after* the query-time exclusion filter (Query exclusion, above),
    and persists nothing durable. A derived result therefore can never
    surface an erased subject once the `.dreq` is live, and there is no
    durable derived object for the pass to clear. The only persisted
    analytics-adjacent store that can retain subject values is the
    query-audit keyspace, covered next.

  Because index and derived state hold no subject values, the pass verifying
  only commit-record segments is not under-asserting the `.done` guarantee;
  it verifies the only place a subject physically lives.

- **The query-audit keyspace is the one excluded derived store.** It may
  retain matcher values from audited query text, and it is deny-deleted
  under ADR-0055, so the erasure guarantee explicitly does not reach the
  audit keyspace.

- **Erasure applies to the primary bucket only.** Replicas or external
  backups are outside Ravel's deletion reach by definition (ADR-0058/0059/0077
  DR posture); an operator with replicated buckets must apply the same
  lifecycle discipline (docs/object-store-contract.md) to replicas, and the
  sanctioned replica configuration and its residue bound are the "+ replica
  residue" modifier above and the level 1 and level 2 postures in
  [guides/disaster-recovery.md](guides/disaster-recovery.md). Per-tenant KMS
  crypto-erasure is the complementary, backup-reaching,
  tenant-granularity layer to this ADR's subject-granularity physical
  erasure.
