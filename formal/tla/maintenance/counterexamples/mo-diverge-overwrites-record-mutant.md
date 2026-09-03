# Mutant: divergent-input-set loser overwrites the record (F6)

Reviewer mutant applied to a scratch copy of the ownership model (`/tmp`,
never the repo): a loser that observes a record whose input-set hash differs
from its own (`Variants`, a divergent listing yielding a different
`input_set_hash`) `PutOverwrite`s the terminal record instead of failing
closed. The overwrite keeps the same content, so record immutability is not
what fires; the store version delta moves. This is the F6 fail-closed
convergence property (ADR-1113 D3): a divergent input set must alarm and
mutate nothing.

The mutant is carried as the committed negative control
`BrokenDivergePublish`, gated off by `DivergeOverwritesRecord = FALSE` in
every shipping cfg and flipped on only by the negative cfg. It self-reports
`outcome |-> "InputSetHashDivergence"` yet performs the `PutOverwrite`, and
it sets `recOverwritten` from the store post-state
(`store'[rk].version # VersionOf(rk)`), not from intent.
`DivergentInputSetNeverMutates` reads that store-derived witness, so it
catches the mutation regardless of what the action claims its outcome was.

Run: `negative/mo-diverge-overwrites-record.cfg` (module
`MCMaintenanceOwnership`, `DivergeOverwritesRecord = TRUE`).

```
Error: Invariant DivergentInputSetNeverMutates is violated.
State 4: <BrokenDivergePublish ... of module MCMaintenanceOwnership>
/\ lastPub = [ outcome |-> "InputSetHashDivergence",
  winnerPartPresent |-> TRUE,
  recOverwritten |-> TRUE ]
```

The violating state has `outcome |-> "InputSetHashDivergence"` and
`recOverwritten |-> TRUE`: the store's version advanced across the divergent
publish. An invariant that trusted the outcome label alone, or that read a
self-reported "I did not overwrite" flag, would pass. The store-derived
`recOverwritten` witness makes the overwrite observable, which is exactly the
F6 fix. TLC exits 12, matching `negative/mo-diverge-overwrites-record.expect`.
