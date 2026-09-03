# Mutant: loser reports Converged with the winning part vanished (F7)

Reviewer mutant applied to a scratch copy of the ownership model (`/tmp`,
never the repo): after the winning part is tombstoned (`TombstonePart`, the
content-addressed key marked non-re-PUTtable, drawn from
`crates/ravel-maintain/tests/tombstone_race.rs::rerun_with_revanished_part_fails_typed_not_converged`),
a loser self-reports `outcome |-> "Converged"` even though the part is gone
and cannot be re-PUT. This is the F7 defect: `MergeAttemptsConverge` held only
because no part ever vanished, and a bare `Converged` on a missing winning
part hides a torn merge. The restated invariant requires the terminal
outcome to be `ConvergedWinnerPartMissing`, never `Converged`, when the
winning part is absent.

The mutant is the committed negative control `BrokenMissingPartConverge`,
gated off by `MissingPartReportsConverged = FALSE` in every shipping cfg. It
self-reports `outcome |-> "Converged"` but sets `winnerPartPresent` from the
store (`Present(...)`, which is FALSE because the part was tombstoned).
`MergeAttemptsConverge` reads that store-derived witness
(`outcome = "Converged" => winnerPartPresent`), so the lie is caught.

Run: `negative/mo-missing-part-reports-converged.cfg` (module
`MCMaintenanceOwnership`, `MissingPartReportsConverged = TRUE`).

```
Error: Invariant MergeAttemptsConverge is violated.
State 4: <TombstonePart ... of module MaintenanceOwnership>
State 5: <BrokenMissingPartConverge ... of module MCMaintenanceOwnership>
/\ lastPub = [outcome |-> "Converged", winnerPartPresent |-> FALSE, recOverwritten |-> FALSE]
```

The violating state claims `Converged` while `winnerPartPresent |-> FALSE`:
the store shows the winning part gone. An invariant reading only the outcome
label would pass; the store-derived `winnerPartPresent` witness makes the
missing part observable, which is the F7 fix. TLC exits 12, matching
`negative/mo-missing-part-reports-converged.expect`.
