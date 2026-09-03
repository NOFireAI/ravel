# mo-diverge-overwrites-record

Switch: `DivergeOverwritesRecord = TRUE` (module `MCMaintenanceOwnership`).
Expected: `DivergentInputSetNeverMutates` violated (exit 12).

Trace: a part is PUT, a record is published, then a loser whose input-set hash
diverges from the winner takes the broken `BrokenDivergePublish` step, which
`PutOverwrite`s the terminal record. The store version delta is recorded in the
store-derived witness `lastPub.recOverwritten`, which becomes TRUE. The
invariant `lastPub.outcome = "InputSetHashDivergence" => ~lastPub.recOverwritten`
fails on that step.

Why it matters: fail-closed convergence (ADR-1113 D3) applies to the shipped
ownership model too. A loser observing a divergent `input_set_hash` must alarm
and mutate nothing. The invariant reads the store version delta, not the action's
self-reported outcome.
