# Mutant: loser reports Converged with the winning part vanished (F7, CompactionClaims)

Reviewer mutant applied to a scratch copy of the claims model (`/tmp`, never
the repo): after the winning part is tombstoned (`TombstonePart`, the
content-addressed key marked non-re-PUTtable, drawn from
`crates/ravel-maintain/tests/tombstone_race.rs::rerun_with_revanished_part_fails_typed_not_converged`),
a loser self-reports `outcome |-> "Converged"` even though the part is gone and
cannot be re-PUT. This is the F7 defect: `MergeAttemptsConverge` held only
because no part ever vanished, and a bare `Converged` on a missing winning part
hides a torn merge. The restated invariant requires the terminal outcome to be
`ConvergedWinnerPartMissing`, never `Converged`, when the winning part is
absent.

The mutant is the committed negative control `BrokenMissingPartConverge`, gated
off by `MissingPartReportsConverged = FALSE` in every shipping cfg. It
self-reports `outcome |-> "Converged"` but sets `winnerPartPresent` from the
store (`Present(...)`, which is FALSE because the part was tombstoned).
`MergeAttemptsConverge` reads that store-derived witness
(`outcome = "Converged" => winnerPartPresent`), so the lie is caught.

Run: `negative/missing-part-reports-converged.cfg` (module
`MCCompactionClaims`, `MissingPartReportsConverged = TRUE`).

```
Error: Invariant MergeAttemptsConverge is violated.
State 5: <TombstonePart ... of module CompactionClaims>
State 6: <BrokenMissingPartConverge ... of module MCCompactionClaims>
/\ lastPub = [outcome |-> "Converged", winnerPartPresent |-> FALSE]
/\ partTomb = <<(iA :> FALSE @@ iB :> TRUE)>>
```

The violating state claims `Converged` while `winnerPartPresent |-> FALSE` and
`partTomb` shows the winning variant tombstoned: the store confirms the part is
gone and non-re-PUTtable. An invariant reading only the outcome label would
pass; the store-derived `winnerPartPresent` witness makes the missing part
observable, which is the F7 fix. TLC exits 12, matching
`negative/missing-part-reports-converged.expect`.
