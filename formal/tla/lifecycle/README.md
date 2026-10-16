# Lifecycle GC TLA+ model

A TLA+ model of retention, selective erasure, legal holds, and physical garbage
collection over the shared object store (task T4 of epic #1113, ADR-1113). It
pins the safety of the maintainer's delete decisions: nothing is deleted inside
a protection window, under a legal hold, out from under a pinned reader, or ahead
of the fold that makes it unreferenced.

`LifecycleGC.tla` is the spec; `MCLifecycleGC.tla` is the model-checking entry
point that extends it. Both instantiate the shared `RavelObjectStore.tla` (under
`../common`) as the durable backend, so every write, overwrite, delete, and
conditional create obeys the object-store contract rather than a hand-rolled map.

## What this model claims (ADR-1113 D12)

TLC checked this finite model under the bounds and assumptions in each `.cfg`.
This model verifies the protocol design; implementation conformance is argued in
the traceability table and asserted by the named Rust tests, not proved. Safety
and liveness are stated separately below, and every liveness result carries its
fairness assumptions. The object store's own conformance to its contract, and any
hash or identity function the rewrite identity relies on, are assumptions, stated
as such and not checked here.

## What the model contains

- A store of a few named objects: one raw input, two rewrite outputs, one
  compaction output, a retention tombstone, an erasure request marker, an
  erasure completion marker, and a system GC config object.
- A HEAD with three read states (present, absent, unreadable) so the sweeps see
  a real fold that can lag, clear, or fail to read.
- Actors: the environment (clock, HEAD state, hold refresh, query pin, lease
  expiry), the erasure and rewrite maintainer under two independent identities,
  the compaction maintainer, the retention maintainer (data-object sweep and the
  final tombstone sweep), and the physical GC maintainer (superseded and
  request-marker sweeps).
- One open ingest bucket (issue #1411): the ingest hour that can be open at the
  moment an erasure request is acknowledged. It opens at clock 0, seals once its
  seal wait bound elapses, and while open it can accept one record of the erased
  subject that becomes visible only after the erasure pass listed the bucket.
  `RequestErasure` captures whether it was open at the acknowledgement, and only
  sealing it and rewriting the sealed bucket discharge the obligation that
  capture creates.
- Every maintenance pass in two steps, a listing and a publish, because the
  shipped passes are two object-store round trips with no compare-and-swap
  between them. That split is what makes a decision taken against a listing
  observable as stale by the time it is acted on; a single atomic action cannot
  express it, and an invariant checked only over such an action holds vacuously.
  Each publish is held to the input set its own listing resolved, not to a
  static input set: `StartRewrite`/`StartCompaction` snapshot the live inputs
  (`rwInputs`, `cmpInputs`), and `PublishRewrite`/`PublishCompaction` supersede
  and stamp exactly that snapshot. `PublishCompaction` additionally requires
  every recorded input still present, and `CancelCompaction` returns a pass
  whose input vanished between listing and publish to idle, so a compaction can
  never stamp or supersede an object retention swept after it listed.
- A witness, `lastGc`, that records what a delete (or, for `CompleteErasure`,
  a completion write) OBSERVED at its own step: the hold state, the refresh
  state, the permitted-query needs, the HEAD-named subset, whether a held
  raw input served the erased subject, and, for a completion, whether a bucket
  open at the request's acknowledgement was still unsealed or still served a
  record accepted before that acknowledgement. Every safety invariant reads the
  witness or the store, never a ghost field the action writes about itself,
  so a hold or refresh flipped after a legitimate delete or completion
  cannot retroactively make it look unsafe.

## Invariants

Eighteen safety invariants including `TypeOK`; see `traceability.md` for the
one-line meaning of each and its Rust source. The load-bearing protocol
properties: `NoDeleteInsideProtectionWindow`, `HeldObjectNeverDeleted`,
`RefreshFailureNeverSweeps`, `TombstoneExcludesBeforeDelete`,
`TombstoneNotDeletedBeforeBucketEmpty`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`,
`RewriteTargetMatchesResolvedInputs`,
`CompletionImpliesNoPreRewriteExposure`, `CompletionRespectsLegalHold`,
`DreqRemovalCannotResurrect`, `DreqSweepRespectsLegalHold`,
`IdenticalInputSetsDoNotCollide`, `HeadNamedObjectNeverDeletedBySupersededSweep`,
`AtMostOneLiveRecordSetServed`, `CompletionCoversEveryBucketOpenAtRequest`.
`RawInputContentAssumedImmutable` is not a protocol property; it pins an
environmental assumption the model is built on (see "Assumptions" below).

`RewriteTargetMatchesResolvedInputs` (issues #1289, #1221) pins the join
between what a rewrite pass resolves and what its published object claims:
`PublishRewrite` names its target by `TargetOf(rwInputs[id])` and fills it from
`Predecessors(target)`, so for every completed pass the predecessors of the
chosen target must equal the input set the pass recorded. It holds in the
shipped model because `RawInputs` and `CompactOut` are singletons, so
`TargetOf` and `Predecessors` cannot disagree; its non-vacuity is shown by a
two-raw-input probe where a pass resolves a proper subset (see "Non-vacuity"
below and `counterexamples/rewrite-target-matches-resolved-inputs-probe.md`).

`AtMostOneLiveRecordSetServed` (issue #1289) counts the record sets a reader
can be served from, meaning present in the store and not themselves superseded,
whose predecessors intersect the superseded set, and requires at most one. That
is the ADR-0064 decision 3 point 5 rule stated over the store: a rewrite's
outputs deliberately lack records its inputs contain, so a compaction record
over the same inputs is not overlap-harmless against it, and a snapshot naming
both resurrects the erased records. Nothing in the object store prevents the
pair, because the two land under different key classes and each publish's
CreateIfAbsent succeeds; the only thing that prevents it is the
`CompactionOutcome::RewritePresent` refusal in `compact_bucket_scoped`.
`negative/compaction-ignores-rewrite.cfg` removes that refusal and TLC breaks
the invariant, which is what makes it a measurement of the guard rather than of
the model's shape.

The scope of that guard is what commit `cdce1722` records, and the model keeps
the two cases apart rather than overstating the first as the second. The
refusal is a list-time observation with no compare-and-swap, so it closes the
case where the rewrite record is already durable when the compactor lists the
bucket. The concurrent case, a rewrite that has listed but not yet published
when the compactor lists, is covered today only by the maintenance driver
serialising the two passes per bucket. That serialisation is the
`SerializeCompactionAndRewrite` constant, `TRUE` in every shipped cfg; setting
it `FALSE` opens the residual window `cdce1722` names as an open gap closable
only by a compare-and-swap or a claim on the bucket. See
`counterexamples/compaction-ignores-rewrite.md` for both traces.

`StartCompaction` records the live L0 inputs it resolved in `cmpInputs`, and
`PublishCompaction` supersedes and stamps exactly that snapshot only after
checking every recorded input is still present. This mirrors the shipped
compactor, which reads each input to build its output and so fails a pass whose
input was swept between listing and publish rather than publishing over the gap.
An earlier draft superseded a static `RawInputs` unconditionally, so a
`RetentionSweep` that deleted the raw input while the pass sat in `"listed"` let
the publish stamp and supersede an object no longer present.
`CancelCompaction` returns such a stale pass to idle so the presence guard does
not strand it. `counterexamples/compaction-publish-presence-probe.md` records
the probe: an action property that a compaction publish stamps only present
inputs, violated before the guard and holding after.

`TombstoneNotDeletedBeforeBucketEmpty` pins the last step of
`physical_sweep`: the tombstone itself is only deleted once a fresh listing
(`bucket_is_empty_but_tombstone`) shows every other data object in the
bucket already gone. Kept as its own invariant rather than folded into
`TombstoneExcludesBeforeDelete`, which pins the opposite ordering (the
tombstone exists before any data delete): each rule's claim stays
independently falsifiable.

`CompletionRespectsLegalHold` and `DreqSweepRespectsLegalHold` pin the same
legal-hold-wins-over-erasure rule (ADR-0064 section 6) that
`bucket_is_held` enforces in `bucket_erasure_completion` and that
`chain_groups_held_by_legal_hold` enforces in the request-marker sweep: a
held, still-present data object (raw input or rewrite output) in the erased
bucket blocks completion and blocks the `.dreq` sweep, independently of
whether that object's current content still serves the erased subject and of
whether the subject is reachable through HEAD or a pinned read. The gate is
scoped to `DataObjects`, not only `RawInputs`, and content-blind, matching
`bucket_is_held`'s and `chain_groups_held_by_legal_hold`'s own per-live-key
gating rather than a per-subject one; a narrower, raw-input-only, content-
matching version of this gate let a hold on the rewrite output through once
its raw input was swept (finding 2, `counterexamples/rewrite-output-hold-
probe.md`). Both invariants read a per-step witness on `lastGc`
(`heldInputServed`, tagged by `rule`), not the live `heldBuckets`, so a hold
placed or released strictly after a legitimate completion or sweep does not
retroactively fail them; this is the same reason `NoDeleteInsideProtectionWindow`
reads `lastGc` instead of the live `supersededAt`/query state.

`NoDeleteInsideProtectionWindow` has a dedicated horizon clause per
horizon-gated rule (retention, superseded, `.dreq`), each comparing
`lastGc.atClock` against the recorded time the horizon is measured from. The
`.dreq` clause (`lastGc.rule = "dreq" => lastGc.atClock >= dreqHorizon`) is
separate from the invariant's `permittedNeeds`-intersection clause, which
cannot say anything about `.dreq`: `.dreq` is a control object,
`permittedNeeds` is always a subset of `DataObjects`, so that clause is
structurally empty whenever `lastGc.rule = "dreq"` (issue #1122). Proved
non-vacuous by removing `DreqSweep`'s horizon guard in a scratch copy and
recording the resulting TLC violation (`results.md`).

The config horizon inequality (protection_horizon at or above
max_query_duration plus grace plus clock_skew) is an `ASSUME` on the constants,
not a checked invariant: it is a precondition on config rather than a
store-observable property, so a runtime invariant reading it would only restate
the constant. The rewrite lineage is likewise not a separate invariant; the
store-derived `RewriteOutputsAreInputsMinusErased` and `IdenticalInputSetsDoNotCollide`
already pin what the output serves and how its identity is bound.

## Assumptions

Raw inputs are immutable: no `Next` action ever changes a raw input's stored
content, and `RawInputContentAssumedImmutable` pins that assumption directly
(`\A o \in RawInputs : objContent[o] = InitContent(o)`), checked in
`smoke.cfg` and `exhaustive.cfg`. The name marks it as an environmental
assumption being asserted, not a protocol property being proven: nothing in
this model derives raw-input immutability from anything else, it is taken as
given because data objects are immutable by system invariant (see
`traceability.md` for the Rust path).

A prior review (issue #1122, finding 1) asked whether the rewrite should
instead gain a bounded raw-input replacement transition, since no
reachable `Next` action changes `objContent["raw1"]` and the previous
review round's fix to `RecordSetContent` (reading current `objContent`
rather than frozen `InitContent`) has no raw-input predecessor left to
exercise it. The decision is to restrict scope instead: a transition that
replaces a raw input's content would model behaviour the object store
forbids, and every property proved over it would be a property of a system
that does not exist. `RawInputContentAssumedImmutable` makes that scope
decision mechanical: a future edit that added such a transition would break
this invariant immediately, in `smoke.cfg`, rather than silently widening
the model to a rewrite the storage layer refuses.

`RecordSetContent`'s current-state read is not vacuous for every
predecessor, only for a raw-input one: it matters for a predecessor that is
itself a published record set, whose content an earlier publish wrote. That
case is now reachable (issue #1221). `Predecessors("rwB")` is the compaction
output, so a compaction followed by a rewrite over the resulting set reads a
body no `Init` wrote. The read is written the way
`resolve_live_inputs` actually behaves (re-list and read current content)
rather than the narrower thing this finite model happens to be able to
observe.

## The two rewrite identities (issue #1221)

`RewriteIds = {"A", "B"}` are two independent worker attempts at the same
bucket's erasure rewrite, and every existing invariant is quantified over
both outputs rather than over `rwA` alone. The pair exists because ADR-0065
grants unit ownership by rendezvous hash with no per-unit compare-and-swap
lease and no fencing token (decision 2, rejected alternative 1), and its
membership-transition overlap window (bounded by `3*H` plus one heartbeat)
lets two workers both believe they own a bucket. The erasure rewrite is not
among the concurrency-safe operations that decision enumerates; ADR-0064
landed afterwards.

`leaseOwner`, `ExpireLease`, and the `StartRewrite`/`PublishRewrite` split
are what make the race expressible: `StartRewrite` takes the lease and
resolves its live input set, `ExpireLease` can drop the lease with a pass
still in flight, a second identity can then start, and `PublishRewrite` is
deliberately NOT re-checked against the lease, because nothing in the
shipped pass re-reads ownership between the listing and the write. A single
atomic action could not express that interleaving at all, which is why the
existing invariants held over it without ever being tested by it.

The result: they still hold, and the interleaving is reached rather than
pruned. Three probes in
`counterexamples/two-rewrite-identities-probe.md` assert each new behaviour is
impossible and each is violated at TLC exit 12: two identities holding a
listing at the same time (`RequestErasure`, `StartRewrite("A")`,
`ExpireLease`, `StartRewrite("B")`), `rwB` materialised, and a rewrite
superseding a compaction output.

What makes the invariants hold is not the lease but the store. The rewrite record key is content-addressed over the resolved input
set and the sorted applied request ids (`compute_rewrite_input_set_hash`)
and is published `CreateIfAbsent`, so two identities that resolved the same
live inputs aim at the same key and converge on one object; the loser's
publish is a no-op over a record that already covers its work. That is a
property of the key and of `CreateIfAbsent`, not of the lease, which is
exactly why it survives the lease moving mid-pass. The compaction record is
the contrasting case: it lands under a different key class, so `CreateIfAbsent`
cannot make the two exclude each other and only the producer-side guard can
(`AtMostOneLiveRecordSetServed`, above).

## Switches and negative controls

Fifteen boolean CONSTANTS gate the model's guards; all are at their shipped value
in `smoke.cfg` and `exhaustive.cfg`, where the four added for ADR-1133
(`WindowGate` and its three switches) are `FALSE`. Each of the nine controls
that predate ADR-1133 flips exactly one, runs with `FullEnv = TRUE` and all
eighteen INVARIANT lines (TypeOK plus seventeen named) from `smoke.cfg`
(finding 5), and names the single invariant it must break, so a
guard silently deleted from the spec fails a control rather than passing
unnoticed under a reduction that happened to dodge the other invariants.
`pinned-query-ungated` runs at those same bounds and is described below. The
other three ADR-1133 controls (`window-three-skew`, `window-no-cache-delay`,
`marker-ignores-anchor`) run at the window gate's bounds, described under "The
ADR-1133 window gate" below. There are thirteen controls, one per `negative/*.cfg`; each has a
note under `counterexamples/`.

The controls run at `MaxClock = 2`, one step above the smoke lane's bound. Each
still fires its own target at `MaxClock = 1`, so the wider bound is a deliberate
widening of the control lane rather than a bound the controls need.

Notes under `counterexamples/` are dated records of the run that produced
them, and `results.md` is append-only by round. Both keep the action and
operator names the model carried at the time, so a note from an earlier round
may name an action this spec has since split or renamed (`PerformRewrite`,
split into `StartRewrite` and `PublishRewrite` in round eight;
`RewriteOutputContent`, now `RecordSetContent`). Renaming them in place would
falsify the record. The notes for the thirteen live controls, which describe
traces the current lane still produces, do use the current names.

The two constants added for issues #1289 and #1221 are
`CompactionIgnoresRewrite` (a negative-control switch like the other seven,
`FALSE` in every shipped cfg) and `SerializeCompactionAndRewrite`, which is
not a control switch: it is `TRUE` in every shipped cfg and models the
maintenance driver's per-bucket serialisation of compaction and erasure
rewrite, a real property of the shipped system that the shipped guard leans
on. Flipping it does not model a deleted guard, it models a second worker
under an ADR-0065 membership transition, so it does not get a
`negative/*.cfg` of its own; `counterexamples/compaction-ignores-rewrite.md`
records what TLC finds with it `FALSE`.

`HorizonGuardsPinnedQueries` is candidate #1133: with it FALSE a sweep delete
gates on the horizon and an unnamed HEAD but not on an in-window pinned query.
It is an idealised oracle, and the Rust gates do not implement it (ADR-1133,
Context). With it FALSE and
nothing in its place the model is unsafe; `negative/pinned-query-ungated.cfg`
(formerly `candidate-1133.cfg`) is that control. `smoke.cfg` and
`exhaustive.cfg` keep the switch TRUE.

## The ADR-1133 window gate (issue #2339)

ADR-1133 replaces the oracle with something a sweeper can check: an
unnamed-since marker per delete candidate, and a delete that waits until the
marker is older than the pinned-query window. `positive/window-gate.cfg` checks
that the gate alone is enough: `HorizonGuardsPinnedQueries = FALSE`,
`WindowGate = TRUE`, and `NoDeleteInsideProtectionWindow` holds.

What the model adds, all inert at `WindowGate = FALSE` and `HeadCacheTtl = 0`:

- `marker`, one per candidate: the retention marker per bucket, a superseded
  marker per superseded object (`MarkerKeys`). Each holds a reading and an
  anchor identity, the anchor's timestamp (`tombRetiredAt` or `supersededAt`).
- `WriteMarker` (decision 1): a pass that finds the candidate past its horizon
  and unnamed by HEAD writes the marker if none exists. `RenewMarker`
  (decision 2): a marker whose anchor does not match is rewritten with a fresh
  reading. `ClearRenamedMarker` (decision 5): a marker on a candidate HEAD names
  again is deleted. The sweeps delete the marker with the candidate
  (decision 6). `StaleMarker` is the environment: a marker written for another
  anchor can appear under any key with a live candidate, already as old as the
  model allows. It stands in for both of the ADR's sources of such markers (an
  older sweeper, a re-rooted chain group) without modelling either.
- `cacheUntil` and `CachedNames` (the cache delay): after a HEAD drops an
  object, a pin can still name it through drop + `HeadCacheTtl`, inclusive, as
  the head cache serves an entry while its age is at most the TTL.
- The four `ClockSkew` terms of decision 3, each a nondeterministic choice in
  `0..ClockSkew` where the ADR names it: the Flight ticket's two terms as one
  choice of up to `2 * ClockSkew` added to the deadline at `PinQuery`, the
  marker writer's lag in `FreshMarker`, and the deleting sweeper's lead in
  `WindowPermits`. The resolving process's own offset cancels (decision 3) and
  has no choice of its own.
- `WindowPermits`, the gate: a matching marker whose reading plus
  `MaxQueryDuration + HeadCacheTtl + 4 * ClockSkew` is at most the deleting
  sweeper's clock, decision 3's `<=`. `RetentionSweep` and `SupersededSweep`
  require it.
- Three switches, each a negative control: `WindowThreeSkew`
  (`3 * ClockSkew`, one sigma short), `WindowNoCacheDelay` (no `HeadCacheTtl`
  term while the pin keeps its delay) and `MarkerIgnoresAnchor` (any marker
  counts, and `RenewMarker` is off). Each violates
  `NoDeleteInsideProtectionWindow`; the notes under `counterexamples/` give the
  traces.

Reader deadlines are exclusive and the cache bound is inclusive, as in the
code: a pinned query reads only while `clock < deadline` and `ExpireQuery`
fires at `clock >= deadline` (`deadline_exceeded` in
`crates/ravel-query/src/log_series.rs` treats `now >= deadline` as exceeded,
and the Flight SQL redemption refuses at `now_ns >= deadline_ns`), while the
head cache serves through drop + `HeadCacheTtl` (`HeadCache::get` serves while
`age <= ttl`). Under those boundaries decision 3's `<=` is exact: the first
tick the gate opens is the first tick no covered reader can read.
`results.md`, "Round fifteen", has the runs showing that the gate with `<=`
passes and that dropping one skew term or the `HeadCacheTtl` term fails.

`window-gate.cfg` runs with every window term at 1, `ProtectionHorizon = 2`
(the smallest the startup inequality allows) and `MaxClock = 8`, so a marker
written at the horizon ages fully whether the writer's clock ran behind or not.
To reach that clock bound inside the lane's budget it adds the state
constraint `WindowGateScope` and sets `FullEnv = FALSE`. A scratch run of the
same cfg with `FullEnv = TRUE` (absent and unreadable HEAD reads, failed hold
refreshes) also passes, at 3488170 distinct states in 4 min 22 s, over the
lane's 300 s budget (results.md, "Round fifteen"). The constraint leaves out:

- the erasure request and everything behind it (the rewrites, completion, the
  `.dreq` sweep), the open ingest bucket, and legal holds;
- a compaction lease that expires mid-pass;
- a tombstone or supersession stamped after clock 0: both anchors and both
  horizons are fixed, while the HEAD drop, the pin, the marker write and the
  delete are free at every clock.

Two consequences of that scope and of `PinQuery`'s shape:

- Decision 2's anchor mismatch is exercised only through `StaleMarker`, the
  environment action that leaves a marker for another anchor under a key.
  `WindowGateScope` prunes the erasure rewrite that would supersede `cmpA` again
  and change its anchor, and fixes both anchors at clock 0, so no live anchor
  moves under a marker already written.
- `PinQuery` pins only objects still present, so the model never pins an
  object a sweep already deleted, which a cached HEAD could still name in the
  code. That omission hides nothing only because the gate outlasts the
  cache: a delete lands at least `MaxQueryDuration + HeadCacheTtl +
  2 * ClockSkew` of true time after the drop (the window less the writer's
  and the deleter's clock offsets), which is past drop + `HeadCacheTtl`, the
  last tick the cache names the object, whenever `MaxQueryDuration` or
  `ClockSkew` is nonzero.

What the window gate as modelled does not cover at all:

- Readers outside the consistency model. The only reader here is the pinned
  query, with a deadline. The fold, scrub, compaction, the erasure rewrite,
  `ravel-cli export` and the bench harness carry no validated deadline, and
  ADR-1133 does not bound them either.
- The listing-fallback resolve. A resolve whose HEAD GET fails pins no HEAD and
  stays bounded by the protection horizon; `PinQuery` always pins a HEAD.
- Clocks outside their bound. Every offset is drawn from `0..ClockSkew`; a real
  clock outside `clock_skew_allowance` can open the gate early, and the model
  says nothing about that case.
- Marker store errors and undecodable bodies (decision 6 blocks the delete on
  any), the observing pass's read-only use of markers, the orphan-marker reaper
  (a superseded object's marker goes in the same step as the object), the
  per-chain-group marker key (each group here holds one object), the store
  version in the anchor identity, and the `sys/gc` version ratchet.
- Liveness: ADR-1133's "cannot stall" argument is not checked; `FairSpec` gives
  the marker actions no fairness.
- The widened pin against the erasure invariants. `CachedNames` and the Flight
  ticket term make `ServesAny` strictly larger, so they can only make
  `ErasedSubjectNeverServedAfterRequest` and `DreqRemovalCannotResurrect`
  easier to violate. No cfg checks that combination: the window cfgs, the only
  ones with `HeadCacheTtl` and `ClockSkew` nonzero, do not list those two
  invariants and prune the erasure request with `WindowGateScope`, and every
  cfg that lists them sets both constants to 0. Issue #2358 tracks it.
- Decision 5's reset of a re-named candidate's marker. In this instance a
  candidate never returns to HEAD once a marker exists: every data object is
  in the one bucket `b1`, so `DropRetiredBucketFromHead` empties `head` for
  good, and `HeadAdvanceRewrite` draws from `LiveRecordSets`, which excludes
  the superseded set, which only grows. So `ClearRenamedMarker` fires only on a
  marker planted by `StaleMarker`, and the hazard it exists for (a candidate
  marked, re-named, dropped again, then deleted on the old marker inside a new
  pin's window) is unreachable. The positive cfg gives no evidence that the
  reset is load-bearing, and there is no negative control for omitting it.

## Non-vacuity

An invariant that no reachable behaviour can break is decoration. Nine of
them are shown breakable by mutating the BEHAVIOUR (not a switch) in a
scratch copy and running TLC: `HeldObjectNeverDeleted`,
`TombstoneExcludesBeforeDelete`, `TombstoneNotDeletedBeforeBucketEmpty`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`
(the "kept" direction), `CompletionRespectsLegalHold`,
`DreqRemovalCannotResurrect`, `CompletionCoversEveryBucketOpenAtRequest` (both
clauses, each deleted on its own), and `RawInputContentAssumedImmutable` (a
scratch action that mutates a raw input's content, disjuncted into `Next`,
which is not part of the shipped model). The mutations and the exact TLC
violation lines are recorded under `counterexamples/*-mutant.md`, except the
`CompletionCoversEveryBucketOpenAtRequest` pair, which sits with its control in
`counterexamples/completion-ignores-open-bucket.md`. The nine
negative controls provide the same evidence for their target invariants by
switch (two targets, `RewriteOutputsAreInputsMinusErased` and
`CompletionCoversEveryBucketOpenAtRequest`, are also covered by
a behaviour mutant above). The seventeenth,
`RewriteTargetMatchesResolvedInputs`, is neither a mutant nor a switch target:
it holds vacuously on the shipped singleton model, so its non-vacuity is shown
by widening `RawInputs` to two elements in a scratch copy where a pass resolves
a proper subset, which breaks it (TLC exit 12) until `TargetOf` is widened to
match; `counterexamples/rewrite-target-matches-resolved-inputs-probe.md` has
the before/after runs. With that probe, all seventeen named safety invariants
have a recorded TLC violation.

`positive/window-gate.cfg` is a pass, so it needs the opposite evidence: that
the deletes it judges safe actually happen. Scratch cover invariants (a delete
never happens, a pinned object is never deleted, a pin through the cache is
never enabled, `RenewMarker` is never enabled) each fail under its bounds;
results.md, "Round fourteen", has the runs, and "Round fifteen" re-runs them
on the current model. Its three switch controls show that
the full `4 * ClockSkew` term (`WindowThreeSkew` is one sigma short, and its
trace spends all four sigma), the `HeadCacheTtl` term and the anchor check are
each load-bearing: each removal alone violates
`NoDeleteInsideProtectionWindow`.

## State-space control

The store carries a monotonic version counter that makes every write ordering a
distinct global state, so the raw state space explodes. `VIEW View` in every cfg
projects the store down to object presence plus the lifecycle variables, which is
what the invariants read, collapsing the space to a size TLC finishes quickly.

## Liveness (exhaustive only)

`FairSpec` adds weak fairness to the maintainer sweeps, the fold's HEAD advance,
erasure completion, the clock (`Tick`), pinned-query expiry (`ExpireQuery`), the
first superseding rewrite's listing step (`StartRewrite`, restricted to firing
while `superseded = {}`), both publish steps (`PublishRewrite`,
`PublishCompaction`), and the compaction abort (`CancelCompaction`).
`PlaceHold`, `ReleaseHold`, `SetHeadState`, `SetRefresh`,
`ExpireLease`, and `StartCompaction` stay unfair: a legal hold is a business
decision with no code-side release guarantee, nothing in the implementation
guarantees a HEAD read recovers, a refresh eventually succeeds, or a lease
eventually lapses, and nothing compels a compaction to run on any given bucket.
`StartRewrite`'s fairness is deliberately
scoped to its first firing rather than granted unconditionally: `RetentionSweep`
ranges over `DataObjects`, which includes the rewrite output, so it can delete
an already-produced rewrite output; an unconditionally fair `StartRewrite`
would then be compelled to recreate it every time, an endless
publish-and-delete loop. That loop no longer resets the horizon countdown for
the raw input `EventuallySwept` is waiting on, because `supersededAt` now keeps
the clock of the FIRST supersession and a republish over an already superseded
input leaves it alone (`counterexamples/superseded-restamp-probe.md`); the loop
itself is still not what the pass does. The implementation runs one rewrite per
erasure request, not a loop that re-derives an already-produced output whenever
ordinary retention ages it out, so unconditional fairness there would assert a
guarantee the implementation doesn't make.

The two publish steps are fair where their listing steps are not, and the
asymmetry is deliberate. A pass that has already listed does finish: that is
the whole reason its acknowledgement can land after its lease has moved, which
is the behaviour issue #1221 asks about. Leaving `PublishCompaction` unfair
would additionally let a compaction sit in `"listed"` forever and, under
`SerializeCompactionAndRewrite`, block every rewrite behind a stall the
implementation does not have. `CancelCompaction` is fair for the same reason:
once a recorded input is gone `PublishCompaction` is disabled, so the abort is
the step that finishes a stale pass, and leaving it unfair would reopen the same
`"listed"` stall the publish fairness closes.

An earlier draft of `EventuallySwept` and `EventuallyCompleted` stated their
hypothesis as "the environment eventually goes quiet" on the four unfair
actions above. Checkpoint review (#1122 finding 1) showed that hypothesis false
as written: TLC found counterexamples where those four actions never fire, yet
the properties still failed, because `Tick`, `ExpireQuery`, and the rewrite
were themselves unfair. Adding fairness to those three actions (as above) is
necessary but not sufficient: reduced runs during this fix also surfaced a
finite-clock-ceiling artifact (a horizon or query-deadline check that lands
exactly at `MaxClock` can never clear, because `Tick`'s own guard requires
`clock < MaxClock`) that no amount of fairness closes, for any finite bound.

Both properties are now stated as explicit antecedents grounded in the real
enabling condition of the action each is waiting on, instead of a quiescence
hypothesis. Neither antecedent includes the condition its own awaited action
negates as a side effect:

- `EventuallySwept`: for each object a publish can supersede (the raw inputs
  and the compaction output), if `SupersededSweep`'s own guard,
  minus the object's own presence (superseded, not legal-held, past the
  horizon or query-permitted, not gated off by the sweep gate, HEAD present,
  no unrecovered failed refresh), holds *permanently* from some point on
  (`<>[]`), the input is eventually gone. Presence is excluded because
  `SupersededSweep` is the action being awaited and removing the object is
  its own effect; an antecedent that also demanded the object stay present
  could never hold permanently once the action's other guards did, making
  the property trivially true no matter what the protocol did.
- `EventuallyCompleted`: if `CompleteErasure`'s own guard, minus `.done`'s
  own absence (`.dreq` present, HEAD present, served-set clear, no held
  input serving the subject, clock past zero), holds permanently from some
  point on, `.done` eventually exists. `.done`'s absence is excluded for the
  same reason: writing `.done` is `CompleteErasure`'s own effect.

`results.md` records per-property runs that took each property alone
against the real, non-quiescent `Next` (all four environment actions present
and still unfair), in a cfg scoped to `TypeOK` plus that one property: both
`EventuallySwept` and `EventuallyCompleted` pass at `MaxClock = 2` under this
reduced configuration. `EventuallySwept`'s restated form was additionally
confirmed at `MaxClock = 4` against a quiescent diagnostic variant; the full
non-quiescent model has not been run to completion at `MaxClock = 4`.

## Running

Use the repository harness from the repo root:

```sh
scripts/check-tla.sh smoke -a lifecycle          # all invariants hold, seconds
scripts/check-tla.sh positive -a lifecycle       # positive/window-gate.cfg holds
scripts/check-tla.sh negative -a lifecycle       # each control breaks its target
scripts/check-tla.sh traceability -a lifecycle   # every source ref resolves
```

`exhaustive.cfg` checks every invariant and both liveness properties against
`FairSpec`. It is run by the `all` lane, not by `ci`, and its measured figures
are in `results.md` and `bands.tsv`.

It uses the same clock bound as `smoke.cfg`, so the two lanes explore the same
state graph and what `exhaustive.cfg` adds is the liveness lane alone.

Round thirteen moved both lanes from `MaxClock = 2` to `MaxClock = 1`. The open
ingest bucket (issue #1411) added three variables, which multiplied the reachable
graph by about 8.5 at a fixed bound: `MaxClock = 2` does not finish inside the 300
second smoke budget on a four-core runner with a 2 GB heap, where it was killed at
300 seconds having reached about 15.1M distinct states; its complete graph is 30.5M
distinct states at depth 34, measured separately on a 16-core host with the JVM
default heap in 3 min 54 s. `MaxClock = 1` finishes in 54 seconds at 2.8M distinct
states, depth 31. Each of the nine negative controls still fires its own target
invariant at `MaxClock = 1`, which is the evidence that the smaller bound still
reaches every guarded behaviour. `results.md`, "Round thirteen", records both
runs and the bound decision.
