# mo-missing-part-reports-converged

Switch: `MissingPartReportsConverged = TRUE` (module `MCMaintenanceOwnership`).
Expected: `MergeAttemptsConverge` violated (exit 12).

Trace: a part is PUT, a record is published, the winning part is tombstoned
(`TombstonePart`, non-re-PUTtable), then a loser takes the broken
`BrokenMissingPartConverge` step, which self-reports
`lastPub.outcome = "Converged"` while `lastPub.winnerPartPresent`, read from the
store, is FALSE. The invariant `lastPub.outcome = "Converged" =>
lastPub.winnerPartPresent` fails on that step.

Why it matters: F7 in the shipped ownership model. A bare `Converged` on a
vanished, non-re-PUTtable winning part hides a torn merge; the correct terminal
outcome is `ConvergedWinnerPartMissing`. The invariant reads the store-derived
`winnerPartPresent` witness, not the action's outcome label.
