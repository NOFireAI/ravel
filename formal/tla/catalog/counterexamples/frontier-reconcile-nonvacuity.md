# frontier-reconcile-nonvacuity

Non-vacuity probe, not a mutation. `negative/frontier-reconcile-nonvacuity.cfg`
checks `NoFrontierReconcile` (no fold ever picks an already-tombstoned,
below-floor hour into its ADR-0020 frontier set) over three-hour bounds where a
second, watermark-advancing fold is reachable. The probe MUST be violated: it
proves the nondeterministic frontier choice added to `DoFoldStart` /
`DoRivalFoldWin` does real work at these bounds, so
`TombstonedBucketContributesNothing`'s `frontierReconciled` disjunct is not
vacuous.

Violated invariant: `NoFrontierReconcile` (probe, TLC exit 12).

## Trace shape (from the recorded run)

1. An L0 input is committed into hour 0. Hour 0 is later tombstoned.
2. A fold against an absent HEAD wins its CAS at watermark 1, carrying hour 0
   forward below its reconcile floor (the ordinary carry-forward path, not the
   frontier pass).
3. A second, watermark-advancing fold reaches `DoRivalFoldWin` and
   nondeterministically picks hour 0 into its frontier set (`FH = {0}`) even
   though hour 0 sits below that fold's reconcile floor (`reconcileLo = 1`).
   Its `FrontierAdmits` check sees hour 0 tombstoned
   (`tombAtWrite = (0 :> TRUE @@ 1 :> FALSE @@ 2 :> FALSE)`) and filters the
   entry out, so `lastHead.frontierReconciled = {0}` while the tombstoned hour
   contributes nothing. `FrontierReconcileExercised` becomes true and
   `NoFrontierReconcile` is false.

Recorded run: state 11 / depth 11, 96113 states generated, 44168 distinct
states found, exit 12.

## Why it is here

At the two-hour smoke bounds there is no below-floor hour left to reconcile
once carry-forward has already absorbed it, so the frontier pass is dormant
and `FrontierAdmits` never gets a chance to filter a live tombstone out of a
carried-forward hour. Without this probe, a below-floor tombstoned hour could
sit unreconciled forever with `frontierReconciled` never populated, and
`TombstonedBucketContributesNothing`'s frontier disjunct would hold only
because the mechanism never ran. See finding 5 in `results.md`.
