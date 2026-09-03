# Lifecycle GC model-checking results

Toolchain: tla2tools 1.7.4, TLC2 version 2.19 (rev 5a47802), OpenJDK 21. All
runs below are the executor's; the exhaustive configuration was not run by the
executor (see the last section). TLC checked this finite model under the bounds
and assumptions in each `.cfg`. This model verifies the protocol design;
implementation conformance is argued in `traceability.md` and asserted by the
named Rust tests, not proved.

## Smoke

`smoke.cfg` against `Spec`, all eleven invariants, `VIEW View`, `MaxClock = 2`,
`FullEnv = TRUE`, every switch at its shipped value.

- Result: PASS.
- States generated: 465355. Distinct: 78584. Diameter (depth): 19. Wall: 4 s.
- Band (`bands.tsv`): distinct within 78000 to 79000. Observed 78584 is inside.

## Negative controls

Each flips one switch and must break one named invariant (exit 12). Each control
is scoped to `TypeOK` plus its single target invariant, so it cannot pass on the
wrong invariant. All seven fire their target:

| control | switch flipped | invariant violated |
|---|---|---|
| delete-before-horizon | DeleteBeforeHorizon | NoDeleteInsideProtectionWindow |
| refresh-failure-is-no-hold | RefreshFailureSweepsAnyway | RefreshFailureNeverSweeps |
| superseded-sweep-ungated | SupersededSweepUngated | HeadNamedObjectNeverDeletedBySupersededSweep |
| dreq-ignores-held-inputs | DreqIgnoresHeldInputs | DreqRemovalCannotResurrect |
| rewrite-identity-omits-requests | RewriteIdentityOmitsRequests | IdenticalInputSetsDoNotCollide |
| rewrite-keeps-erased-records | RewriteKeepsErasedRecords | RewriteOutputsAreInputsMinusErased |
| complete-ignores-served-set | CompleteIgnoresServedSet | CompletionImpliesNoPreRewriteExposure |

Traces are under `counterexamples/`; each control reaches its violation within a
few actions.

## Non-vacuity mutants

Three invariants are shown breakable by mutating the behaviour (not a switch) in
a scratch copy under `/tmp` and running TLC against `smoke.cfg`. Exact lines:

- `HeldObjectNeverDeleted` (removed the `~HeldObject` gate from both the
  retention and superseded sweeps; TLC reaches the violation through the
  superseded sweep):
  `Error: Invariant HeldObjectNeverDeleted is violated.`
- `TombstoneExcludesBeforeDelete` (removed the `PresentObj("tombB1")` gate from
  both `RetentionSweep` and `DropRetiredBucketFromHead`; removing it from the
  sweep alone is unreachable because HEAD can only be emptied of the bucket after
  the tombstone exists):
  `Error: Invariant TombstoneExcludesBeforeDelete is violated.`
- `ErasedSubjectNeverServedAfterRequest` (made `RequestErasure` skip the `.dreq`
  write):
  `Error: Invariant ErasedSubjectNeverServedAfterRequest is violated.`

Details under `counterexamples/held-object-mutant.md`,
`counterexamples/tombstone-mutant.md`, `counterexamples/erased-subject-mutant.md`.
Together with the seven negative controls, all ten named safety invariants have a
recorded TLC violation, so none is vacuously true.

## Per-invariant store/witness audit

Every safety invariant reads the modeled store, the current HEAD, or the `lastGc`
witness recording what a delete OBSERVED at its own step. None reads a bookkeeping
flag the action sets to certify itself, and none reduces to a constant.

- `NoDeleteInsideProtectionWindow`: reads `lastGc` (rule, deleted, permittedNeeds,
  atClock) and `supersededAt`; the witness records the permitted in-window query
  needs at the delete step.
- `HeldObjectNeverDeleted`: reads `lastGc.held`, recorded from `heldBuckets` at
  the delete step, and `lastGc.deleted`.
- `RefreshFailureNeverSweeps`: reads `lastGc.refreshWasFailed` and `lastGc.deleted`.
- `TombstoneExcludesBeforeDelete`: reads the store (`PresentObj("tombB1")`),
  `tombRetiredAt`, and the witness rule/deleted.
- `ErasedSubjectNeverServedAfterRequest`: reads the store and HEAD via
  `ServedRead`, and `erasureRequested`.
- `RewriteOutputsAreInputsMinusErased`: reads the materialised output content
  `objContent["rwA"]` via `ServesSubject`.
- `CompletionImpliesNoPreRewriteExposure`: reads the store (`PresentObj("doneR1")`)
  and the current HEAD content via `ServesNow`.
- `DreqRemovalCannotResurrect`: reads the store and HEAD serving state and the
  witness of the request-marker sweep.
- `IdenticalInputSetsDoNotCollide`: reads `variantKey`, which records the actual
  names `PerformRewrite` stored for the two variants.
- `HeadNamedObjectNeverDeletedBySupersededSweep`: reads the current HEAD and the
  store (`PresentObj`) over the raw inputs.

## Candidate #1133: CONFIRMED unsafe

`candidate-1133.cfg` sets `HorizonGuardsPinnedQueries = FALSE`: a sweep delete
gates on the horizon and an unnamed HEAD but not on an in-window pinned query.
TLC violates `NoDeleteInsideProtectionWindow` (exit 12).

Trace (six states): a reader pins on the current HEAD naming `raw1`; a rewrite
supersedes `raw1` (`supersededAt = 0`) and materialises `rwA`; the clock reaches
the horizon; `HeadAdvanceRewrite` advances HEAD to `{rwA}` so the input is no
longer HEAD-named; the superseded sweep then deletes `raw1` while the pinned
query, still in window at `clock = 1 <= deadline = 1`, needs it. The witness
records `rule = superseded`, `deleted = {raw1}`, `permittedNeeds = {raw1}`, so
the intersection is non-empty and the invariant fails. Full trace in
`counterexamples/candidate-1133.md`.

Verdict: the horizon plus unnamed-HEAD gate is not sufficient; the pinned-query
clause is load-bearing. The shipped model keeps `HorizonGuardsPinnedQueries`
TRUE, under which this trace has no successor.

## Findings from the epic

- #1132 and #1134 are modeled as fixed: the shipped model is safe and their
  ungated variants are carried as the `superseded-sweep-ungated` and
  `dreq-ignores-held-inputs` negative controls.
- #1131 is a liveness limitation, not a safety defect. `EventuallySwept` and
  `EventuallyCompleted` are conditional on fairness and on the fold's and the
  sweep's retention windows agreeing; when they disagree the sweep waits on the
  fold forever and the properties are intentionally false. Whether they hold
  under `FairSpec` is checked only in `exhaustive.cfg`, which this task did not
  run, so this document makes no claim here that they hold.

## Review findings (issue #1122)

The fifteen adversarial review findings and their disposition:

1. Guard-mirror invariants replaced by store/witness reads: every safety
   invariant now reads the store, HEAD, or the `lastGc` witness (audit above).
2. Serving is store-derived via `objContent`; `ServesSubject`/`ServesNow` read
   the modeled object content, not a bookkeeping set.
3. `supersededAt` records when a rewrite superseded its inputs, so the
   superseded sweep's horizon gate is observed, not assumed.
4. Rewrite identity is store-derived: `variantKey` records the actual output
   names, and `IdenticalInputSetsDoNotCollide` reads them.
5. Sweeps fail closed on a non-present HEAD (`HeadDeletable`): an absent or
   unreadable read blocks the pass rather than being treated as empty.
6. The superseded sweep gates on `clock >= supersededAt + horizon` and on the
   pinned query, closing the delete-before-horizon path for superseded inputs.
7. Rewrite output content excludes erased subjects (`RewriteOutputContent`), and
   `RewriteOutputsAreInputsMinusErased` reads the materialised content.
8. Completion checks the served set (`~ServesNow`) at HEAD level; the pinned
   reader path is handled by the `.dreq` sweep and `DreqRemovalCannotResurrect`.
9. The config horizon inequality is an `ASSUME` on the constants, not a runtime
   invariant that would only restate a constant.
10. Non-vacuity is proven by behaviour mutation with recorded TLC lines, not by a
    switch the invariant reads.
11. Negative controls are target-scoped (`TypeOK` plus one invariant) so none can
    pass on the wrong first invariant.
12. The tombstone non-vacuity mutant removes the gate from both the sweep and the
    HEAD-drop, because the single-gate mutant is unreachable under finding 5.
13. `bands.tsv` carries only the measured smoke row; the unmeasured exhaustive
    row was removed rather than shipped as a fabricated band.
14. The candidate trace is the honest superseded-sweep path, not the removed
    absent-HEAD shortcut that finding 5 closed.
15. README and traceability claim only what the lanes this task ran showed, use
    ADR-1113 D12 language, and separate safety from liveness.

## Exhaustive

`exhaustive.cfg` checks every invariant and both liveness properties against
`FairSpec` over `MaxClock = 4`. Not run by the executor; see the orchestrator's
run. `bands.tsv` carries no exhaustive row because the executor did not measure
one; the orchestrator adds it after the run. The FairSpec liveness pass makes it
heavier than smoke, so it is left to the orchestrator's longer run.
