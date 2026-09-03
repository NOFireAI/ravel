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
  erasure and rewrite maintainer, the retention maintainer, and the physical GC
  maintainer (superseded and request-marker sweeps).
- A witness, `lastGc`, that records what a delete OBSERVED at its own step (the
  hold state, the refresh state, the permitted-query needs, the HEAD-named
  subset). Every safety invariant reads the witness or the store, never a ghost
  field the action writes about itself, so a hold or refresh flipped after a
  legitimate delete cannot retroactively make it look unsafe.

## Invariants

Eleven safety invariants including `TypeOK`; see `traceability.md` for the
one-line meaning of each and its Rust source. The load-bearing ones:
`NoDeleteInsideProtectionWindow`, `HeldObjectNeverDeleted`,
`RefreshFailureNeverSweeps`, `TombstoneExcludesBeforeDelete`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`,
`CompletionImpliesNoPreRewriteExposure`, `DreqRemovalCannotResurrect`,
`IdenticalInputSetsDoNotCollide`, `HeadNamedObjectNeverDeletedBySupersededSweep`.

The config horizon inequality (protection_horizon at or above
max_query_duration plus grace plus clock_skew) is an `ASSUME` on the constants,
not a checked invariant: it is a precondition on config rather than a
store-observable property, so a runtime invariant reading it would only restate
the constant. The rewrite lineage is likewise not a separate invariant; the
store-derived `RewriteOutputsAreInputsMinusErased` and `IdenticalInputSetsDoNotCollide`
already pin what the output serves and how its identity is bound.

## Switches and negative controls

Eight boolean CONSTANTS gate the model's guards; all are at their shipped value
in `smoke.cfg` and `exhaustive.cfg`. Each `negative/*.cfg` flips exactly one and
names the single invariant it must break (scoped to `TypeOK` plus that one
target, so a control cannot pass on the wrong invariant), so a guard silently
deleted from the spec fails a control rather than passing unnoticed. There are
seven controls, one per `negative/*.cfg`; each has a note under
`counterexamples/`.

`HorizonGuardsPinnedQueries` is candidate #1133: with it FALSE a sweep delete
gates on the horizon and an unnamed HEAD but not on an in-window pinned query.
`candidate-1133.cfg` runs that configuration and it is unsafe;
`counterexamples/candidate-1133.md` has the trace. The shipped model keeps the
switch TRUE.

## Non-vacuity

An invariant that no reachable behaviour can break is decoration. Three of them
are shown breakable by mutating the BEHAVIOUR (not a switch) in a scratch copy
and running TLC: `HeldObjectNeverDeleted`, `TombstoneExcludesBeforeDelete`, and
`ErasedSubjectNeverServedAfterRequest`. The mutations and the exact TLC violation
lines are recorded under `counterexamples/*-mutant.md`. The seven negative controls
provide the same evidence for seven more invariants by switch, so all ten named
safety invariants have a recorded TLC violation.

## State-space control

The store carries a monotonic version counter that makes every write ordering a
distinct global state, so the raw state space explodes. `VIEW View` in every cfg
projects the store down to object presence plus the lifecycle variables, which is
what the invariants read, collapsing the space to a size TLC finishes quickly.

## Liveness (exhaustive only)

`FairSpec` adds weak fairness on the maintainer sweeps, the fold's HEAD advance,
and erasure completion. Two conditional liveness properties are defined,
`EventuallySwept` and `EventuallyCompleted`. They are only meaningful under the
fairness and window agreement the exhaustive cfg sets up: a legal hold, a stopped
maintainer, or a fold and sweep whose retention windows disagree make them false
(#1131). This task did not run the exhaustive configuration, so it makes no claim
here about whether the two properties hold; the orchestrator's exhaustive run and
its outcome are recorded in `results.md`. The README claims only what the smoke,
negative, and traceability lanes this task ran actually showed.

## Running

Use the repository harness from the repo root:

```sh
scripts/check-tla.sh smoke -a lifecycle          # all invariants hold, seconds
scripts/check-tla.sh negative -a lifecycle       # each control breaks its target
scripts/check-tla.sh traceability -a lifecycle   # every source ref resolves
```

`exhaustive.cfg` checks every invariant and both liveness properties against
`FairSpec` over a larger clock horizon. It is not run by the executor; the
orchestrator runs it. See `results.md`.
