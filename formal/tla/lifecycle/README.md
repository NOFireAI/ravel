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
- A witness, `lastGc`, that records what a delete (or, for `CompleteErasure`,
  a completion write) OBSERVED at its own step: the hold state, the refresh
  state, the permitted-query needs, the HEAD-named subset, and whether a held
  raw input served the erased subject. Every safety invariant reads the
  witness or the store, never a ghost field the action writes about itself,
  so a hold or refresh flipped after a legitimate delete or completion
  cannot retroactively make it look unsafe.

## Invariants

Thirteen safety invariants including `TypeOK`; see `traceability.md` for the
one-line meaning of each and its Rust source. The load-bearing ones:
`NoDeleteInsideProtectionWindow`, `HeldObjectNeverDeleted`,
`RefreshFailureNeverSweeps`, `TombstoneExcludesBeforeDelete`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`,
`CompletionImpliesNoPreRewriteExposure`, `CompletionRespectsLegalHold`,
`DreqRemovalCannotResurrect`, `DreqSweepRespectsLegalHold`,
`IdenticalInputSetsDoNotCollide`, `HeadNamedObjectNeverDeletedBySupersededSweep`.

`CompletionRespectsLegalHold` and `DreqSweepRespectsLegalHold` pin the same
legal-hold-wins-over-erasure rule (ADR-0064 section 6) that
`bucket_is_held` enforces in `bucket_erasure_completion` and that
`chain_groups_held_by_legal_hold` enforces in the request-marker sweep: a
held, still-present superseded input serving the erased subject blocks
completion and blocks the `.dreq` sweep, independently of whether the
subject is reachable through HEAD or a pinned read. Both invariants read a
per-step witness on `lastGc` (`heldInputServed`, tagged by `rule`), not the
live `heldBuckets`, so a hold placed or released strictly after a
legitimate completion or sweep does not retroactively fail them; this is
the same reason `NoDeleteInsideProtectionWindow` reads `lastGc` instead of
the live `supersededAt`/query state.

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
runs with `FullEnv = TRUE` and the full thirteen-invariant list from
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

An invariant that no reachable behaviour can break is decoration. Six of them
are shown breakable by mutating the BEHAVIOUR (not a switch) in a scratch copy
and running TLC: `HeldObjectNeverDeleted`, `TombstoneExcludesBeforeDelete`,
`ErasedSubjectNeverServedAfterRequest`, `RewriteOutputsAreInputsMinusErased`
(the "kept" direction), `CompletionRespectsLegalHold`, and
`DreqRemovalCannotResurrect`. The mutations and the exact TLC violation lines
are recorded under `counterexamples/*-mutant.md`. The seven negative controls
provide the same evidence for their target invariants by switch (one target,
`RewriteOutputsAreInputsMinusErased`, is also covered by a behaviour mutant
above), so all twelve named safety invariants have a recorded TLC violation.

## State-space control

The store carries a monotonic version counter that makes every write ordering a
distinct global state, so the raw state space explodes. `VIEW View` in every cfg
projects the store down to object presence plus the lifecycle variables, which is
what the invariants read, collapsing the space to a size TLC finishes quickly.

## Liveness (exhaustive only)

`FairSpec` adds weak fairness on the maintainer sweeps, the fold's HEAD advance,
and erasure completion. Two conditional liveness properties are defined,
`EventuallySwept` and `EventuallyCompleted`. Both hold only under an environment
that eventually goes quiet: after some point, no further `PlaceHold`/`ReleaseHold`,
`SetHeadState`, or `SetRefresh` transitions occur, so the corresponding sweep or
completion action's guard, once enabled, stays enabled instead of being
recurrently knocked down and re-armed. A legal hold left in place, a stopped
maintainer, or a fold and sweep whose retention windows disagree (#1131) are each
a special case of this: a hold that is placed and never released, for instance,
is an environment that went quiet on the hold variable specifically. `FairSpec`
grants weak fairness only to the maintainer and store actions
(`SupersededSweep`, `HeadAdvanceRewrite`, `RetentionSweep`, `CompleteErasure`),
never to a `FullEnv`-gated environment action; none of `PlaceHold`/`ReleaseHold`
(a legal hold is a business decision with no code-side progress guarantee),
`SetHeadState`, or `SetRefresh` has fairness added, so an environment that keeps
perturbing hold state, HEAD readability, or refresh outcome forever can
recurrently disable a maintainer action's guard just before it would fire,
defeating weak fairness without technically violating it.

This task did not run the exhaustive configuration (forbidden for this task; the
orchestrator runs it and its outcome is recorded in `results.md`). It instead ran
each property alone, at `MaxClock = 2`, in a cfg scoped to `TypeOK` plus that one
property, to identify a violation cheaply: both `EventuallySwept` and
`EventuallyCompleted` fail under this reduced configuration, with exact TLC
counter-example traces recorded in `results.md`. `EventuallySwept` fails via a
stutter where `SetRefresh` never clears a failed refresh, so
`RefreshFailureNeverSweeps`'s fail-closed gate leaves `SupersededSweep` forever
disabled. `EventuallyCompleted` fails via a genuine lasso where `SetHeadState`
repeatedly toggles `headState` away from `"present"`, so `CompleteErasure`'s
`HeadDeletable` gate is never continuously enabled. Neither trace is a lasso
through `PlaceHold`/`ReleaseHold` specifically, but both are instances of the
same class the orchestrator's exhaustive run reported (#1122 finding 2): an
unfair environment action recurrently disabling a fairly-scheduled one. The
README claims only what the smoke, negative, traceability, and these two
reduced liveness lanes actually showed.

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
