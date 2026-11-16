# missing-part-reports-converged

Switch: `MissingPartReportsConverged = TRUE` (module `MCCompactionClaims`).
Expected: `MergeAttemptsConverge` violated (exit 12).

Trace: a part is PUT, a record is published, the winning part is tombstoned
(`TombstonePart`, non-re-PUTtable), then a loser takes the broken
`BrokenMissingPartConverge` step, which self-reports
`lastPub.outcome = "Converged"` while `lastPub.winnerPartPresent`, read from the
store, is FALSE. The invariant `lastPub.outcome = "Converged" =>
lastPub.winnerPartPresent` fails on that step.

Why it matters: F7. `MergeAttemptsConverge` held only because no part ever
vanished; a bare `Converged` on a missing winning part hides a torn merge. The
correct terminal outcome is `ConvergedWinnerPartMissing`. The invariant reads
the store-derived `winnerPartPresent` witness, not the action's outcome label,
so the vanished part is observable.
