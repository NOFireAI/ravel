# diverge-overwrites-record

Switch: `DivergeOverwritesRecord = TRUE` (module `MCCompactionClaims`).
Expected: `DivergentInputSetNeverMutates` violated (exit 12).

Trace: a part is PUT, a record is published (its minted version latched into
`recVer`), then a loser whose input-set hash diverges from the winner takes the
broken `BrokenDivergePublish` step, which `PutOverwrite`s the terminal record
with identical content and mints a fresh store version while leaving `recVer`
unchanged. The invariant `recVer[u] # 0 => VersionOf(RecordKey(u)) = recVer[u]`
fails on that step: the store version has moved off the latch.

Why it matters: fail-closed convergence (ADR-1113 D3). When a loser observes a
divergent `input_set_hash` it must alarm and mutate nothing, never overwrite the
terminal record. The invariant reads the store version delta, not the action's
self-reported outcome, so an overwrite that keeps identical content is still
caught.
