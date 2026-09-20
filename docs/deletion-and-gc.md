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

Deletion of published data is always a durable transaction first (a
retention tombstone, a compaction record, or a rewrite record), then logical
exclusion from new snapshots, then physical removal via a sweeper. Orphan
collection, which removes objects no commit record ever published, has no
record to anchor on and is gated by age and a fresh re-listing instead (the
first rule below). Orphan collection does not delete the object outright: it
moves it to a `quarantine/` prefix for a second horizon and a reaper deletes it
only after that, so a small out-of-band commit-record loss below the
mass-orphan breaker's thresholds is recoverable rather than permanent
(ADR-0058 amendment; see the orphan-GC section below). One sweeper component implements all rules below; all are
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

```text
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
| orphan (first implementation, ADR-0010 §11; batched re-verify and breaker, ADR-0048 decisions 4-5; quarantine, ADR-0058 amendment) | data object with no commit record | age > grace + max_flush_lifetime (default 1 h); record absence re-verified by one fresh LIST shared by every candidate in the pass; the mass-orphan circuit breaker not tripped (or deliberately overridden). A candidate that clears these is moved to `quarantine/`, not deleted | object last_modified |
| quarantine reaper (ADR-0058 amendment) | a `quarantine/<original key>/q<ns>` object an orphan pass moved out of the live keyspace | age since the quarantine timestamp embedded in the key > quarantine_horizon (default 7 days); a key whose `/q<ns>` segment does not parse is skipped, never deleted | quarantine key's own `/q<ns>` timestamp |
| superseded input (ADR-0018, HEAD-reachability gate ADR-0020) | L0 commit records + data objects named in a compaction or rewrite record's input list, or a whole superseded predecessor record together with the parts it names | now >= record.created_unix_ns + protection_horizon; the live catalog HEAD snapshot names none of the objects the delete would remove (delete blocker, see below) | compaction or rewrite record created_unix_ns |
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

**The superseded-input sweep is gated the same way.** The same blocker, the
same three answers, and the same per-pass cache apply to a compaction or
rewrite record's superseded inputs, with the question asked per object rather
than per bucket: after a compaction or a rewrite the snapshot legitimately
names the output parts sitting in the same bucket, so only the specific objects
a delete would remove may be tested. The horizon alone is not enough there for
the same reason it is not enough for retention. A selective-erasure rewrite
record can land in any sealed hour, including one both the fold's fixed
reconcile window and its retention-frontier band miss, and the snapshot part
covering that hour then keeps naming the pre-rewrite inputs. An input the live
HEAD still names is held for a later pass instead of deleted, and the pass
reports how many objects it held under each of the two reasons below.

A rewrite record can itself be superseded by a later rewrite applying a
different erasure request, so the objects one delete unit covers are a whole
supersession chain, not a single record's own outputs. The gate is asked over
the entire chain at once, down to the raw inputs the oldest generation
superseded: a HEAD naming anything in the chain holds all of it. Asking per
generation would clear on a stale HEAD, which names the raw inputs rather than
any generation's outputs, and would delete a record while the inputs it erased
a subject out of were still resolvable.

A rewrite record landing outside both the fixed reconcile window and the
frontier band is not left to wait indefinitely for one of those two passes to
eventually cover its hour: `Catalog::fold_with_refold_request` (issue #526,
ADR-0063 amendment) takes a caller-supplied set of ingest hours and re-lists
them in the same fold call, closing the gap on demand instead of on a
schedule. A request submitted to a fold call that turns out to be a
**no-op** (nothing newly sealed beyond the previous watermark) reconciles
**zero hours**, whatever hours it named: the targeted pass sits inside the
same reconcile branch as the fixed window and the frontier band, and a
no-op fold returns before that branch ever runs. `FoldReport`'s
`refold_hours_reconciled` field reports the count, and is `0` on that path,
on a plain `Catalog::fold` call, and on any request naming hours the pass
did not reach (docs/adrs/0064-selective-subject-erasure.md, the no-op
carve-out).

HEAD read failures are explicit. An **absent** HEAD is NOT a block: with no
snapshot naming anything, the sweep proceeds (ADR-0020: the catalog index is a
pure optimization; a missing HEAD degrades to listing). A HEAD, or a covering
part, that is **present but unreadable** (undecodable, checksum/blake3 mismatch,
an unsupported newer format, a HEAD-named part that is missing, or a snapshot
entry whose identity fields do not fit the shape a fold writes) blocks the
sweep **fail-closed**: non-reachability cannot be proven from data that cannot
be read, and a wrongly-permitted delete is unrecoverable while a delayed one is
not. HEAD and each covering part are read at most once per sweep pass (cached
across the pass's buckets and, for superseded inputs, across the pass's
records), never once per bucket or once per input. A pass that finds nothing
past its horizon reads neither.

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
  mass-orphan circuit breaker gate below; then, for every surviving
  candidate, a move to quarantine (see "Quarantine and the second horizon"
  below), never a direct delete. The breaker is all-or-nothing: a
  tripped, non-overridden breaker quarantines zero candidates that pass.
- **Candidate selection's initial listing runs on the full-sweep cadence, not
  every maintain tick.** That listing is the one phase of a
  pass that cannot be hour-scoped (L0 data keys carry no ingest-hour
  component), so it would otherwise re-list the whole shard's `l0/`
  data prefix on every maintain tick (default 300 s) even though rules 2 and
  3 already list only the tick's zone-scoped hours. `ravel-maintain`'s
  per-tick sweep now runs candidate selection only on the tick a full sweep
  is due -- the same cadence memo (`MaintainMemo::full_sweep_due`,
  `interior_reverify_ns`, default 6 h) that already governs when rules 2 and
  3 fall back to their own unscoped pass. Every other tick skips candidate
  selection entirely: no LIST of the `l0/` data prefix, and that pass's
  orphan and breaker figures (deleted, quarantined, quarantine-refused, and
  the breaker fields) are reported as zero rather than a value carried over
  from the last tick that did run it. Those zeros mean "the rule did not
  run", not "the rule looked and found nothing", and the two are
  indistinguishable from the counts alone, so the report records which pass
  it was and a consumer that keeps a last-observed-value gauge must not
  publish the skipped kind. The `ravel_maintain_orphans_present` and
  `ravel_maintain_orphans_withheld` gauges are therefore written only by a
  pass that ran the rule: they report the last completed orphan pass and are
  refreshed once per full-sweep interval rather than reset to zero by every
  tick in between. Publishing the zeros would silently disable the
  `orphans_present > 0 for 12h` alert
  (`docs/guides/operations/troubleshooting.md`), since on the defaults 71 of
  every 72 ticks skip the rule. The per-pass counters
  (`orphans_quarantined`, `orphans_quarantine_refused`, `quarantine_reaped`,
  and the breaker-trip counter) are unaffected: a skipped pass adds zero,
  which is the truth about the events it performed. The L0 listing cost that
  used to be paid every 300 s is now paid once per full-sweep interval
  instead. The quarantine reaper runs on that same cadence: it is a different
  rule over the separate `quarantine/` prefix, but it runs only on a pass that
  ran candidate selection, so the breaker's hold on it cannot be stepped
  around by the next tick (see "A tripped breaker holds the reaper" below).
  Objects already quarantined keep aging out, one full-sweep interval at a
  time rather than one tick at a time, which moves the effective second
  horizon later by at most one full-sweep interval and never earlier. The key
  layout of `l0/` and `quarantine/` is unchanged.
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

### Quarantine and the second horizon (ADR-0058 amendment)

The breaker catches mass loss but, by design, lets small or thinly-spread
loss through: fewer than `orphan_breaker_min_count` candidates, or a count
under `orphan_breaker_max_ratio` of a large shard, does not trip it (the
three scope limits above). Before this amendment those candidates were
deleted permanently at the first horizon, so an out-of-band commit-record
loss below the thresholds became permanent data loss with no recovery
window and nothing paging. Orphan GC therefore no longer deletes a
candidate at all. It moves each surviving candidate to a `quarantine/`
prefix and a separate reaper deletes it only after a second horizon:

- **The move is copy-first, delete-second.** An object store has no atomic
  rename, so the move is a copy (`get` the bytes, `put` them under
  `quarantine/<original key>/q<quarantined_at_ns>`) followed by a delete of
  the live key. The copy always precedes the delete, so a crash or a store
  fault between the two leaves the bytes in at least one location, never
  none. A candidate whose copy fails is left live and counted as refused
  (`ravel_maintain_orphans_quarantine_refused`), and the live delete never
  runs for it; the candidate is retried on the next pass. The `put` is an
  overwrite, which makes a retry idempotent within one pass. It is not
  idempotent across passes: the destination key embeds that pass's timestamp,
  so a crash between the copy and the live delete leaves the object live and
  the next pass writes a second copy under a different `/q<ns>`. That
  duplicate is self-cleaning, because the reaper collects each copy on its own
  horizon, and the live object is never deleted while no copy of it exists.
- **The quarantine key carries its own timestamp.** The trailing `/q<ns>`
  segment records when the object was quarantined, taken from the injected
  clock. The reaper reads the second horizon from that segment, not from the
  copy's store `last_modified`, so the horizon is deterministic and does not
  depend on the store preserving a copy's modification time. The whole
  original key is preserved verbatim (strip the `quarantine/` prefix and the
  `/q<ns>` segment) so an operator can copy the bytes back to their original
  key to recover.
- **The reaper is the only place orphan-GC'd data is physically deleted.**
  It lists `quarantine/t/<tenant_hash>/<signal>/l0/<shard>/`, and for each
  object whose embedded timestamp is more than `quarantine_horizon_ns`
  (default 7 days) behind the clock, deletes it. A key whose `/q<ns>`
  segment cannot be parsed is skipped, never deleted (fail-closed: an
  unreadable age is treated as not-yet-expired). It runs on the same pass as
  orphan GC's candidate selection, so on the full-sweep cadence
  (`interior_reverify_ns`, default 6 h) rather than on every maintain tick,
  whole-shard like orphan GC itself (quarantine keys are not hour-bucketed),
  and is stateless and idempotent.
- **A tripped breaker holds the reaper.** A pass whose mass-orphan breaker
  tripped reaps nothing, whatever the quarantine ages say. A loss that grows
  over time reaches the breaker's thresholds days after it started, so
  reaping on such a pass deletes the copies taken while it was still small.
  The two horizons are therefore chained, not independent, and they are
  chained because both run on the same pass: a pass that skipped candidate
  selection never evaluated the breaker, so it reports not-tripped
  structurally, and reaping there would delete on the next tick exactly what
  the trip just held. Tying the reaper to candidate selection's cadence makes
  the hold hold by construction, with no breaker state persisted between
  passes. A `force_orphan_gc` override is not a trip and still reclaims.
- **The event is visible in the logs, not yet on `/metrics`.** A pass counts
  objects quarantined (equal to the retained `orphans_deleted` count of
  candidates removed from the live set), refused, and reaped, and emits a
  `warn`-level tracing event for each nonzero count. Today that tracing event
  is the only operator-facing signal. The counter names
  `ravel_maintain_orphans_quarantined` and
  `ravel_maintain_orphans_quarantine_refused` are **not rendered on
  `/metrics`** yet, so an alert rule written on either name can never fire.
  Alert on `ravel_maintain_orphans_present` and on the tracing events
  instead. `docs/observability.md` lists what `/metrics` actually exposes.

The cost is storage plus transfer. A quarantined object occupies the bucket
for the second horizon before it is reclaimed, and the `quarantine/` prefix
would leak without the reaper, which is why the reaper is part of the
mechanism, not a follow-up. The request cost changed shape too: orphan GC
went from one DELETE per candidate to a full-object GET plus a full PUT per
candidate, run serially with no cap on candidates per pass, and the reaper
adds one LIST per swept unit per full-sweep interval: it runs on the pass
that ran candidate selection, not on every tick. The thin-spread record
loss this feature exists for is also the expensive case, because it moves
those bytes twice through a single maintain tick. The per-pass
unboundedness is an acknowledged open item, not a property anything
enforces. `force_orphan_gc` (the breaker override) still quarantines rather
than deletes, so even a forced pass keeps the recovery window.

## Superseded input and unreferenced part

- Superseded-input and unreferenced-part deletion never depend on reader
  leases or on removing an input before its compaction record is durable;
  the horizon alone bounds how long a pinned query can still need an
  input, and orphan-GC-style convergence handles crash remnants (a
  compactor that died mid-publish leaves record-less parts, which the
  unreferenced-part rule collects once old enough).
- Superseded-input deletion is gated on HEAD reachability exactly as
  retention is (ADR-0020, "HEAD-referenced snapshot delete blocker" above),
  asked per object rather than per bucket. The horizon bounds a pinned
  in-flight reader; it does not prove the *current* snapshot has stopped
  naming the input. An input a part named by the live HEAD still references
  is skipped, and the record that superseded it stays in place so the next
  pass retries. A superseded predecessor record and the parts it names are
  held or deleted together: dropping the record while a still-named part is
  held would leave that part unreferenced, and the unreferenced-part rule
  would then collect it and undo the hold.
- **An input is superseded only where the record a query reads names it.**
  When two compaction records in one bucket name overlapping input sets, the
  resolver picks one authoritative record per overlap group and serves every
  input the winner does not name as a raw segment. The sweep applies that same
  choice: a losing record's inputs count as superseded only where the winner
  also names them, so an input only a loser names is left in place, because
  that raw object is where a query still reads those rows from. Deleting it on
  the strength of the losing record alone would turn duplicated rows into
  missing rows. The losing record's own parts are served from nowhere, but
  they stay referenced for as long as the losing record exists, and no rule
  removes a losing record today: the sweep reclaims neither, so an overlap
  leaves the loser's parts in place as a bounded storage cost.
- **A rewrite record outlives every input it superseded.** A whole
  supersession chain, from the live record back to the raw inputs its oldest
  generation superseded, is one indivisible delete unit: the HEAD gate decides
  it all at once, and within a pass the superseded inputs and each
  generation's parts are deleted before the records that superseded them,
  oldest generation first. A record is the only durable statement that a
  subject was erased out of a particular set of inputs, so a record that
  disappeared while one of those inputs remained would leave that input
  present with nothing naming it as erased.
- **The legal-hold gate decides a chain group as a whole, never part of
  one.** Every key in the group, the raw inputs, each generation's parts, and
  each generation's records, is checked against `LegalHoldCheck` before
  anything is deleted; if the hold protects any one of them the whole group
  is skipped for this pass and nothing in it is deleted. Hold scopes are per
  prefix, so a hold covering a shard's data prefixes but not its commit
  prefix would otherwise let one pass delete a chain's records while the
  input bytes those records account for survive.
- A held input costs storage until the fold reconciles its hour or an
  operator rebuilds HEAD; nothing else about the hour changes, and every
  query over it keeps resolving normally. That is the deliberate trade
  against the alternative, a permanently failing query.

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
- The retention window a sweep applies to a bucket is resolved by a single
  precedence, the same one the fold applies (ADR-0078): the durable per-tenant
  retention window wins when set, otherwise the deployment default, where the
  deployment default is the per-tenant deployment override if one is configured
  and the deployment-wide default otherwise. So a durable per-tenant record
  overrides both deployment settings. The sweep reads that durable record from
  object storage under the same key and through the same decoder the fold uses,
  so the sweep and the fold always resolve the same window for a tenant. This
  agreement is required, not incidental: the two passes run independently, and a
  sweep using a shorter window than the fold would tombstone an hour the fold's
  snapshot still names and whose retention-frontier reconcile never drops,
  leaving the physical sweep blocked on the HEAD-referenced-snapshot delete
  blocker on every pass.
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

- **Compaction and format migration refuse a bucket that already holds a live
  rewrite record.** This is the producer-side half of the same rule, and it is
  a correctness requirement rather than an optimization: because overlap
  harmlessness does not hold for a rewrite, a compaction (or migration) record
  published over the same inputs leaves one bucket serving two record sets,
  and a snapshot naming both resurrects the erased records through query-time
  dedup. `compact_bucket` and `migrate_bucket_format` therefore gate on the
  bucket's rewrite records exactly as they gate on its compaction records, and
  report `RewritePresent`: the bucket is left untouched, its L0 inputs stay
  live behind the rewrite record, and the maintenance pass memoizes the
  refusal as terminal. The query-time filter (Query exclusion, above) remains
  the guarantee that covers the window before a rewrite lands. This refusal
  closes the case where the rewrite record is already durable when the
  compactor lists the bucket; it does not close the concurrent case, where
  compaction and the rewrite pass each list before the other publishes and
  neither can see the other's record yet. The maintenance driver's per-bucket
  serialization of compaction and rewrite is what covers that case today, and
  it remains load-bearing -- not an optimization this refusal makes
  unnecessary. Closing the residual window needs a compare-and-swap or an
  explicit claim on the bucket; this change does not add one.

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
  that. Overlapping compaction records are read here under the same
  authoritative-record rule the resolver and the sweep use: only a winning
  record's inputs count as compacted away, so a raw L0 that only a losing
  record names stays in the gate's live set, and a losing record's parts,
  which no query reads, do not by themselves keep a request blocked.

- **Physical removal reuses the existing sweep.** A rewrite's superseded
  inputs become inputs to the superseded-input sweep, deleted after
  `protection_horizon`, under the same `LegalHoldCheck` gate as every other
  delete. That sweep is gated on HEAD reachability the same way retention is
  (ADR-0020, "HEAD-referenced snapshot delete blocker" above): an input a
  snapshot part the fold has not reconciled still names is held rather than
  deleted, and collected once the fold reconciles the hour or an operator
  rebuilds HEAD (see "Scope and interactions" below for what a query sees
  meanwhile). The `.dreq` itself contains the subject
  identifier and is therefore not kept forever: a sweep rule deletes it once
  its `.done` exists, `now >= done.completed_unix_ns + protection_horizon`,
  the superseded-input sweep held nothing belonging to that request on this
  pass, and the legal-hold check passes. The horizon and that hold
  together are what guarantee the query-time filter only disappears after no
  resolvable snapshot can still include a pre-rewrite input: the horizon
  covers the ordinary case, and the hold covers the two cases that outlive
  it, an input the HEAD gate is holding and an input a legal hold over the
  data prefixes preserves (such a hold does not cover the request keyspace,
  so it would otherwise retire the filter over data it is preserving). The
  `.done` record carries only a hash of the canonical
  predicate, timestamps, and optionally per-bucket dropped counts, no subject
  identifier, and is permanent, deny-delete audit evidence for every role
  (ADR-0055 amendment).

- **The hold is read off the superseded-input sweep, not rediscovered.** That
  sweep runs first in the same pass and already knows, per chain group, which
  requests the chain applied and whether the group was deleted or held. It
  reports two things: the request ids whose superseded inputs it held this
  pass, for either reason the gate holds them, and the buckets in which it
  held the inputs of a chain it could not walk back to a raw input. The
  erasure rule holds a `.dreq` past its horizon when its request id is in the
  first set, or when the second set is non-empty at all.

  **The hold is computed over every supersession chain a HEAD-named part
  still resolves, whether or not the chain is old enough to delete.** Two
  filters decide whether a chain is collectable *yet*: the protection horizon
  on the live record's own `created_unix_ns`, and the rule that a record
  another present rewrite supersedes is processed only as part of that
  rewrite's chain group. Both bound deletion only. Neither says anything about
  whether a snapshot still resolves the chain's inputs, which is the only
  question this hold turns on, and a chain still inside its own horizon is the
  likeliest one a stale HEAD names. So the observation gathers and gates every
  chain in the signal and applies neither filter. A chain that is merely young
  contributes exactly the request ids and buckets it will contribute after its
  horizon; that a pass could not have deleted it is not recorded and not
  consulted.

  **A completion record's bucket list is informational only.** No part of this
  rule reads `bucket_drops`: not the hold decision, and not the scope of the
  observation. The field is optional on the wire, is written empty by the
  production completion writer, and a writer that does populate it is not
  obliged to enumerate every bucket it touched, so a present list can be
  partial. Narrowing either the decision or the observation by such a list
  would release the filter over a bucket the request did touch. The
  observation is therefore always whole-signal: one listing of the commit
  keyspace enumerates the signal's shards, and every shard is observed across
  every hour. A shard with no commit key holds nothing, so that scope costs
  listing, never correctness. The rule issues no GET of its own beyond the
  sweep's and the completions it must read anyway; it reads a verdict that
  sweep produced while doing its own work.

- **An erasure request's `.dreq` outlives every input any rewrite in its
  supersession chain superseded.** The hold does not look only for the record
  that applied this request. The superseded-input sweep gathers each live
  record's whole chain, from the record back to the raw inputs at its end,
  collecting the requests every generation applied, and reports every request
  on a chain whose objects it held. A superseded generation's own parts count:
  they still carry whatever the generation above them erased. When the walk
  cannot reach a raw input because a generation's record is already gone, the
  requests that generation applied are no longer named anywhere; the sweep
  reports that bucket as one where a chain was cut, and any candidate `.dreq`
  is held while such a bucket exists, since no surviving record can say which
  requests the missing generation applied. A chain group
  the legal-hold gate skipped holds its requests' `.dreq`s the same way a
  HEAD-held one does, which is what keeps a data-prefix-only hold from
  retiring a filter over data it is preserving.

- **Why the hold terminates.** The cut-chain hold counts only objects the
  superseded-input sweep actually held on this pass, never the bare presence
  of a commit record in the bucket: a bucket with a cut chain and no held
  object releases the `.dreq`. Two properties make that reachable. First, the
  delete order within a chain (inputs and each generation's parts before the
  records that superseded them, oldest generation first) means a missing
  generation record implies everything that generation superseded is already
  gone, so a cut never outlives the data it stands in for; and a chain whose
  predecessor record is absent forms no delete group at all, so it has
  nothing to hold. Second, an object that lands in an already-swept hour
  after the fact, a late L0 flush into a sealed bucket, is an input of no
  record that has passed its horizon: it forms no group, is held by nothing,
  and cannot pin any request's filter, however long it sits there. So each
  hold is discharged by the event that released the objects behind it, the
  fold reconciling the hour, an operator rebuilding HEAD, or a human clearing
  the legal hold, and no state pins a request forever.

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

- **`+R`, scoped per-object compliance retention on commit records
  (`t/*/*/c/*`).** Under the scoped posture (bucket default retention OFF,
  an operator-run mechanism applying per-object retention instead;
  docs/object-store-contract.md "Required bucket configuration", ADR-0072
  decision 3), a superseded commit record still under its retention period
  `R` refuses the same sweep delete `+D` describes. `sweep_superseded` runs
  three delete loops in order over every cleared chain: every chain's input
  commit records first, then every chain's input data objects, then every
  chain's own compaction or rewrite records last. A lock on a chain's input
  commit record aborts the pass at the first loop, so the data-delete loop
  never runs for any chain in that pass and the physical-removal bound stays
  at `max(bound, R)` until `R` elapses. A lock on a chain's own compaction or
  rewrite record instead aborts the pass at the third loop, after that
  chain's input records and their data are already gone: it holds only that
  record at `max(bound, R)`, and the chain survives the retention period for
  the next pass to retry. `sys/*`, `t/*/*/prov`, and
  `t/*/catalog/*/*` carry the same scoped retention but are never targets
  of supersession GC, ADR-0019 retention deletion, or ADR-0064 erasure.
  That is a statement about those three mechanisms and nothing wider: the
  catalog family is swept by a fourth one, the unreferenced-catalog sweep,
  which carries its own `+R` erasure bound for some tenants (below). Keep
  `R` inside `protection_horizon` (about 25 h with defaults) so the sweep
  keeps making progress on superseded chains.

- **`+R` again, scoped per-object compliance retention on the catalog
  keyspace (`t/*/catalog/*/*`).** A compliance lock on this keyspace
  costs an erasure obligation, not only reclamation. The unreferenced-catalog
  sweep deletes the snapshot and index objects the current HEAD no longer
  names, and for a tenant with a `STR` or `BYTES` typed attribute column a
  per-part `.cstat` index object among them holds that subject's own column
  value; a lock over the keyspace delays that delete, and the erased value
  persists until the fold reconciles the hour and then a further `R`. Under
  the shipped Maintain IAM policy the delete is denied outright, so the
  bound is open-ended until that policy changes. The four-step mechanism,
  the exact bound, the IAM ceiling, and the HEAD-scoping advice are in
  docs/object-store-contract.md's "Required bucket configuration" section,
  "A lock on the catalog family".

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

- **What the `.done` pass verifies, and the one object it does not.** The
  completion pass walks `c/<shard>/<hour>/` commit records and verifies the
  segment data a snapshot resolves is subject-free. It does NOT separately
  walk index objects or analytics. For analytics that is sound (below); for
  the catalog family it is sound for three of its four object kinds and not
  for the fourth:
  - **Three index object kinds hold no value, and `.cstat` is the
    exception.** `SnapshotEntry`, `SnapshotPartHeader`, and name postings
    hold identities, hashes, counts, and metric names, never label/attribute
    *values*, and a snapshot entry's own typed-column stamp is restricted to
    `I64` and `BOOL` extrema (proto/ravel/commit.proto,
    `DeclaredColumnStatValue`), which cannot represent a `STR` or `BYTES`
    value either. The per-part `.cstat` column-statistics objects are the
    exception the `+R` modifier above sets out: for a tenant with a `STR` or
    `BYTES` typed attribute column they hold that column's exact min, max,
    and distinct-value dictionary (the dictionary only up to a fixed entry
    cap; the min and max always), so an erased subject's own value can sit
    in one verbatim. So the deny-deleted `prov` and `sys/*` prefixes hold
    nothing an erasure subject can match, while the `catalog/` prefix does:
    the deny list's "disjoint by construction" claim no longer holds for the
    catalog family. ADR-0064's Decision still states the wider form;
    amending it is tracked separately. The three value-free kinds are
    value-free *only if* subject identifiers appear as label/attribute
    values and never inside metric names (a documented requirement; see
    docs/object-store-contract.md "Required bucket
    configuration" point 5). A snapshot entry whose object a rewrite
    superseded is refreshed only when the fold reconciles that hour, through
    the fixed window or the retention-frontier band (docs/catalog-and-mvcc.md,
    "Fold reconcile pass"), or when a HEAD rebuild re-derives every hour.
    Until then the superseded-input sweep holds those inputs rather than
    deleting them, because the live HEAD still names them. A query over that
    hour keeps resolving the pre-rewrite inputs from the stale part and the
    query-time predicate removes the erased records after fetch, exactly as it
    did before the horizon elapsed; the request stays live for as long as any
    held input does, so the filter never retires under a snapshot that can
    still resolve one. That holds however many rewrite generations the hour has
    accumulated: each rewrite record stays in place behind the inputs it
    superseded, and each request's filter stays live behind the whole chain,
    not just behind the one generation that applied it. The cost of the held
    inputs is storage, not a failed query, and it ends when the fold
    reconciles the hour or an operator rebuilds HEAD: the next sweep deletes
    the inputs, then the records that superseded them, and the requests
    become removable behind both.
  - **ADR-0028 analytics/derived datasets are a pure query-time stage, not a
    persisted store.** `ravel-analytics` carries no clock, IO, object-store,
    or catalog (docs/analytics.md): every analytic runs in memory over query
    output, *after* the query-time exclusion filter (Query exclusion, above),
    and persists nothing durable. A derived result therefore can never
    surface an erased subject once the `.dreq` is live, and there is no
    durable derived object for the pass to clear. The only persisted
    analytics-adjacent store that can retain subject values is the
    query-audit keyspace, covered next.

  So the pass's commit-record scope covers the data objects a snapshot
  resolves and the three value-free index kinds, and it covers no `.cstat`:
  a stale column-statistics object the live HEAD still names sits outside
  everything the pass verifies. What covers that gap is not the pass and
  not the row-level exclusion filter (no row is ever sourced from a
  `.cstat`) but the statistics gate named below, which declines every
  metadata-only answer while an erasure predicate is pending; the filter
  covers the held inputs. The filter cannot retire while the
  superseded-input sweep still holds an input this request's rewrites
  superseded (`crates/ravel-maintain/src/sweep.rs`), which is the same
  condition under which the stale `.cstat` is still HEAD-referenced, and a
  metadata-only answer read from typed-column statistics is refused
  outright while any erasure predicate is pending
  (`crates/ravel-sql/src/logs_scan.rs`).

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
