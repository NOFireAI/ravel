# steal-ignores-cas

Switch: `StealIgnoresCas = TRUE`. Expected: `AtMostOneThiefWinsAVersion`
violated (exit 12).

Trace: two workers both observe version 1 of unit 1's claim. The claim expires
and a thief steals it, recording version 1 in `stealWonVers`. Because the smoke
bounds use `DeclaredLease = 0`, the claim re-expires while the other worker
still observes version 1. The broken `BrokenSteal` step then counts a second win
for version 1: it is a correct CAS in every respect (the store write, the honest
`ok`, `stolen`, and the store-read witness are all the correct `Steal`'s) except
that its duplicate-win guard drops the `ok` conjunct, so it records
`dupThiefWin = TRUE` even though a version can be won at most once.
`AtMostOneThiefWinsAVersion` (`~dupThiefWin`) fails.

Why it matters: version-gated steal is the mechanism that serialises claim
ownership across a re-expiry. If the duplicate-win detection stops depending on
the CAS outcome, two workers can both believe they stole the same version and
run the unit concurrently. The invariant is only non-vacuous once the race is
reachable; see `../counterexamples/two-thieves-reachable.md` for the reachability
proof behind the `DeclaredLease = 0` smoke bound this control shares.
