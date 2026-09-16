# Mutant: divergent-input-set loser overwrites the record (F6, CompactionClaims)

Reviewer mutant applied to a scratch copy of the claims model (`/tmp`, never
the repo): a loser that observes a record whose input-set hash differs from
its own (`Variants`, a divergent listing yielding a different
`input_set_hash`) `PutOverwrite`s the terminal record instead of failing
closed. The overwrite keeps the same content, so record immutability is not
what fires; the store version delta moves. This is the F6 fail-closed
convergence property (ADR-1113 D3): a divergent input set must alarm and
mutate nothing.

The mutant is the committed negative control `BrokenDivergePublish`, gated off
by `DivergeOverwritesRecord = FALSE` in every shipping cfg and flipped on only
by the negative cfg. It self-reports `outcome |-> "InputSetHashDivergence"`
yet performs the `PutOverwrite` with the same content bytes, which mints a fresh
store version, and it leaves the latch `recVer` unchanged.
`DivergentInputSetNeverMutates` reads the store directly: the record's minted
version is latched into `recVer[u]` at publish, and the invariant holds
`VersionOf(RecordKey(u))` equal to that latch, so the fresh version the
overwrite mints is caught regardless of the action's own outcome label.

Run: `negative/diverge-overwrites-record.cfg` (module `MCCompactionClaims`,
`DivergeOverwritesRecord = TRUE`).

```
Error: Invariant DivergentInputSetNeverMutates is violated.
State 4: <BrokenDivergePublish ... of module MCCompactionClaims>
/\ lastPub = [ outcome |-> "InputSetHashDivergence",
  winnerPartPresent |-> TRUE ]
```

The violating state self-reports `outcome |-> "InputSetHashDivergence"` while
the store's version for the record key has advanced past the latched `recVer`:
the identical-content `PutOverwrite` minted a new version. An invariant that
trusted the outcome label alone, or that compared content bytes, would pass. The
store-version latch makes the overwrite observable, which is the F6 fix. TLC
exits 12, matching `negative/diverge-overwrites-record.expect`.
