# Lifecycle GC model-checking results

Toolchain: tla2tools 1.7.4, TLC2 version 2.19 (rev 5a47802), OpenJDK 21. All
runs below are the executor's; the exhaustive configuration was not run by the
executor (see the last section).

## Smoke

`smoke.cfg` against `Spec`, all thirteen invariants, `VIEW View`, `MaxClock = 2`,
`FullEnv = TRUE`, every switch at its shipped value.

- Result: PASS.
- States generated: 463955. Distinct: 72434. Diameter (depth): 19. Wall: 3 s.
- Band (`bands.tsv`): distinct within 61568 to 83299. Observed 72434 is inside.

## Negative controls

Each flips one switch and must break one named invariant (exit 12). All six fire
their target:

| control | switch flipped | invariant violated |
|---|---|---|
| delete-before-horizon | DeleteBeforeHorizon | NoDeleteInsideProtectionWindow |
| refresh-failure-is-no-hold | RefreshFailureSweepsAnyway | RefreshFailureNeverSweeps |
| superseded-sweep-ungated | SupersededSweepUngated | HeadNamedObjectNeverDeletedBySupersededSweep |
| dreq-ignores-held-inputs | DreqIgnoresHeldInputs | DreqRemovalCannotResurrect |
| rewrite-identity-omits-requests | RewriteIdentityOmitsRequests | IdenticalInputSetsDoNotCollide |
| gc-config-violates-inequality | GcConfigViolatesInequality | GcConfigSatisfiesHorizon |

Traces are under `counterexamples/`. `gc-config-violates-inequality` violates in
the initial state; the others reach the violation within a few actions.

## Non-vacuity mutants

Three invariants are shown breakable by mutating the behaviour (not a switch) in
a scratch copy under `/tmp` and running TLC against `smoke.cfg`. Exact lines:

- `HeldObjectNeverDeleted` (removed the `~HeldObject` gate from the sweeps):
  `Error: Invariant HeldObjectNeverDeleted is violated.`
- `TombstoneExcludesBeforeDelete` (removed the `PresentObj("tombB1")` gate from
  the retention sweep):
  `Error: Invariant TombstoneExcludesBeforeDelete is violated.`
- `ErasedSubjectNeverServedAfterRequest` (made `RequestErasure` skip the `.dreq`
  write):
  `Error: Invariant ErasedSubjectNeverServedAfterRequest is violated.`

Details under `counterexamples/held-object-mutant.md`,
`counterexamples/tombstone-mutant.md`, `counterexamples/erased-subject-mutant.md`.
The six negative controls give the same non-vacuity evidence for six more
invariants.

## Candidate #1133: CONFIRMED unsafe

`candidate-1133.cfg` sets `HorizonGuardsPinnedQueries = FALSE`: the retention
delete gates on the horizon and an empty HEAD but not on an in-window pinned
query. TLC violates `NoDeleteInsideProtectionWindow` (exit 12).

Trace (six states): a reader pins on the current HEAD naming `raw1`; a late fold
publishes an absent HEAD so the head-empty gate passes; the bucket is retired and
the clock reaches the horizon; the retention sweep then deletes `raw1` while the
pinned query, still in window, needs it. The witness records `rule = retention`,
`deleted = {raw1}`, `permittedNeeds = {raw1}`, so the intersection is non-empty
and clause two fails. Full trace in `counterexamples/candidate-1133.md`.

Verdict: the horizon plus head-empty gate is not sufficient; the pinned-query
clause is load-bearing. The shipped model keeps `HorizonGuardsPinnedQueries`
TRUE, under which this trace has no successor.

## Findings from the epic

- #1132 and #1134 are modeled as fixed: the shipped model is safe and their
  ungated variants are carried as the `superseded-sweep-ungated` and
  `dreq-ignores-held-inputs` negative controls.
- #1131 is a liveness limitation, not a safety defect. `EventuallySwept` and
  `EventuallyCompleted` hold only when the fold's and the sweep's retention
  windows agree; when they disagree the sweep waits on the fold forever and the
  properties are intentionally false. `exhaustive.cfg` sets both windows to 0 so
  liveness is checkable.

## Exhaustive

`exhaustive.cfg` checks every invariant and both liveness properties against
`FairSpec` over `MaxClock = 4`. Exhaustive: not run by the executor, see the
orchestrator's run. The `bands.tsv` band for it is a wide estimate (100000 to
5000000 distinct) because it was not measured; the orchestrator should tighten it
after the run. The FairSpec liveness pass makes it heavier than smoke, so it is
left to the orchestrator's longer run.
