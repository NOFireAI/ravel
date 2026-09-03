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

Thirteen safety invariants including `TypeOK`; see `traceability.md` for the
one-line meaning of each and its Rust source. The load-bearing ones:
`NoDeleteInsideProtectionWindow`, `HeldObjectNeverDeleted`,
`RefreshFailureNeverSweeps`, `TombstoneExcludesBeforeDelete`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`,
`CompletionImpliesNoPreRewriteExposure`, `DreqRemovalCannotResurrect`,
`IdenticalInputSetsDoNotCollide`, `PredecessorChainRepresentable`,
`HeadNamedObjectNeverDeletedBySupersededSweep`, `GcConfigSatisfiesHorizon`.

## Switches and negative controls

Seven boolean CONSTANTS gate the model's guards; all are at their shipped value
in `smoke.cfg` and `exhaustive.cfg`. Each `negative/*.cfg` flips exactly one and
names the single invariant it must break, so a guard silently deleted from the
spec fails a control rather than passing unnoticed. The six controls and their
targets are listed in `bands.tsv`; each has a note under `counterexamples/`.

`HorizonGuardsPinnedQueries` is candidate #1133: with it FALSE the retention
delete gates on the horizon and an empty HEAD but not on an in-window pinned
query. `candidate-1133.cfg` runs that configuration and it is unsafe;
`counterexamples/candidate-1133.md` has the trace. The shipped model keeps the
switch TRUE.

## Non-vacuity

An invariant that no reachable behaviour can break is decoration. Three of them
are shown breakable by mutating the BEHAVIOUR (not a switch) in a scratch copy
and running TLC: `HeldObjectNeverDeleted`, `TombstoneExcludesBeforeDelete`, and
`ErasedSubjectNeverServedAfterRequest`. The mutations and the exact TLC violation
lines are recorded under `counterexamples/*-mutant.md`. The six negative controls
provide the same evidence for six more invariants by switch.

## State-space control

The store carries a monotonic version counter that makes every write ordering a
distinct global state, so the raw state space explodes. `VIEW View` in every cfg
projects the store down to object presence plus the lifecycle variables, which is
what the invariants read, collapsing the space to a size TLC finishes quickly.

## Liveness (exhaustive only)

`FairSpec` adds weak fairness on the maintainer sweeps, the fold's HEAD advance,
and erasure completion. Two properties, `EventuallySwept` and
`EventuallyCompleted`, hold under it. They are intentionally conditional: a legal
hold, a stopped maintainer, or a fold and sweep whose retention windows disagree
make them false (#1131). The exhaustive cfg sets both windows to agree.

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
