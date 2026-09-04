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

- A store of a few named objects: one raw input, one rewrite output, a retention
  tombstone, an erasure request marker, an erasure completion marker, and a
  system GC config object.
- A HEAD with three read states (present, absent, unreadable) so the sweeps see
  a real fold that can lag, clear, or fail to read.
- Actors: the environment (clock, HEAD state, hold refresh, query pin), the
  erasure and rewrite maintainer, the retention maintainer (data-object sweep
  and the final tombstone sweep), and the physical GC maintainer (superseded
  and request-marker sweeps).
- A witness, `lastGc`, that records what a delete (or, for `CompleteErasure`,
  a completion write) OBSERVED at its own step: the hold state, the refresh
  state, the permitted-query needs, the HEAD-named subset, and whether a held
  raw input served the erased subject. Every safety invariant reads the
  witness or the store, never a ghost field the action writes about itself,
  so a hold or refresh flipped after a legitimate delete or completion
  cannot retroactively make it look unsafe.

## Invariants

Fourteen safety invariants including `TypeOK`; see `traceability.md` for the
one-line meaning of each and its Rust source. The load-bearing ones:
`NoDeleteInsideProtectionWindow`, `HeldObjectNeverDeleted`,
`RefreshFailureNeverSweeps`, `TombstoneExcludesBeforeDelete`,
`TombstoneNotDeletedBeforeBucketEmpty`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`,
`CompletionImpliesNoPreRewriteExposure`, `CompletionRespectsLegalHold`,
`DreqRemovalCannotResurrect`, `DreqSweepRespectsLegalHold`,
`IdenticalInputSetsDoNotCollide`, `HeadNamedObjectNeverDeletedBySupersededSweep`.

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

## Switches and negative controls

Eight boolean CONSTANTS gate the model's guards; all are at their shipped value
in `smoke.cfg` and `exhaustive.cfg`. Each `negative/*.cfg` flips exactly one,
runs with `FullEnv = TRUE` and the full fourteen-invariant list from
`smoke.cfg` (finding 5), and names the single invariant it must break, so a
guard silently deleted from the spec fails a control rather than passing
unnoticed under a reduction that happened to dodge the other invariants. There
are seven controls, one per `negative/*.cfg`; each has a note under
`counterexamples/`.

`HorizonGuardsPinnedQueries` is candidate #1133: with it FALSE a sweep delete
gates on the horizon and an unnamed HEAD but not on an in-window pinned query.
`candidate-1133.cfg` runs that configuration and it is unsafe;
`counterexamples/candidate-1133.md` has the trace. The shipped model keeps the
switch TRUE.

## Non-vacuity

An invariant that no reachable behaviour can break is decoration. Seven of them
are shown breakable by mutating the BEHAVIOUR (not a switch) in a scratch copy
and running TLC: `HeldObjectNeverDeleted`, `TombstoneExcludesBeforeDelete`,
`TombstoneNotDeletedBeforeBucketEmpty`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`
(the "kept" direction), `CompletionRespectsLegalHold`, and
`DreqRemovalCannotResurrect`. The mutations and the exact TLC violation lines
are recorded under `counterexamples/*-mutant.md`. The seven negative controls
provide the same evidence for their target invariants by switch (one target,
`RewriteOutputsAreInputsMinusErased`, is also covered by a behaviour mutant
above), so all thirteen named safety invariants have a recorded TLC violation.

## State-space control

The store carries a monotonic version counter that makes every write ordering a
distinct global state, so the raw state space explodes. `VIEW View` in every cfg
projects the store down to object presence plus the lifecycle variables, which is
what the invariants read, collapsing the space to a size TLC finishes quickly.

## Liveness (exhaustive only)

`FairSpec` adds weak fairness to the maintainer sweeps, the fold's HEAD advance,
erasure completion, the clock (`Tick`), pinned-query expiry (`ExpireQuery`), and
the first superseding rewrite (`PerformRewrite`, restricted to firing while
`superseded = {}`). `PlaceHold`, `ReleaseHold`, `SetHeadState`, and `SetRefresh`
stay unfair: a legal hold is a business decision with no code-side release
guarantee, and nothing in the implementation guarantees a HEAD read recovers or
a refresh eventually succeeds. `PerformRewrite`'s fairness is deliberately
scoped to its first firing rather than granted unconditionally: `RetentionSweep`
ranges over `DataObjects`, which includes the rewrite output, so it can delete
an already-produced rewrite output; an unconditionally fair `PerformRewrite`
would then be compelled to recreate it and re-stamp the shared `supersededAt`
every time, perpetually resetting the horizon countdown for the very raw input
`EventuallySwept` is waiting on. The implementation runs one rewrite per erasure
request, not a loop that re-derives an already-produced output whenever ordinary
retention ages it out, so unconditional fairness there would assert a guarantee
the implementation doesn't make.

An earlier draft of `EventuallySwept` and `EventuallyCompleted` stated their
hypothesis as "the environment eventually goes quiet" on the four unfair
actions above. Checkpoint review (#1122 finding 1) showed that hypothesis false
as written: TLC found counterexamples where those four actions never fire, yet
the properties still failed, because `Tick`, `ExpireQuery`, and `PerformRewrite`
were themselves unfair. Adding fairness to those three actions (as above) is
necessary but not sufficient: reduced runs during this fix also surfaced a
finite-clock-ceiling artifact (a horizon or query-deadline check that lands
exactly at `MaxClock` can never clear, because `Tick`'s own guard requires
`clock < MaxClock`) that no amount of fairness closes, for any finite bound.

Both properties are now stated as explicit antecedents grounded in the real
enabling condition of the action each is waiting on, instead of a quiescence
hypothesis. Neither antecedent includes the condition its own awaited action
negates as a side effect (a later review, issue #1122, found the first
attempt at this restatement still included that condition, making the
leads-to trivially true regardless of whether the action ever fired):

- `EventuallySwept`: for each raw input, if `SupersededSweep`'s own guard,
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

An earlier round did not run the exhaustive configuration, which is why the
per-property runs below exist; that configuration is now run directly and its
figures are in `results.md`. Those runs took each property alone against the real, non-quiescent `Next` (all
four environment actions present and still unfair), in a cfg scoped to
`TypeOK` plus that one property: both `EventuallySwept` and
`EventuallyCompleted` pass at `MaxClock = 2` under this reduced configuration
(exact TLC output recorded in `results.md`). `EventuallySwept`'s restated form
was additionally confirmed at `MaxClock = 4` against a quiescent diagnostic
variant (see `results.md`); the full non-quiescent model was not re-run at
`MaxClock = 4` in this task because its state space grew past what this task's
reduced-configuration budget and host memory should spend, and doing so would
edge into the exhaustive-scale run this task is forbidden from running. The
README claims only what the smoke, negative, traceability, and these reduced
liveness runs actually showed.

## Running

Use the repository harness from the repo root:

```sh
scripts/check-tla.sh smoke -a lifecycle          # all invariants hold, seconds
scripts/check-tla.sh negative -a lifecycle       # each control breaks its target
scripts/check-tla.sh traceability -a lifecycle   # every source ref resolves
```

`exhaustive.cfg` checks every invariant and both liveness properties against
`FairSpec` over a larger clock horizon. It is run as part of the gate lane and
its measured figures are in `results.md` and `bands.tsv`.
