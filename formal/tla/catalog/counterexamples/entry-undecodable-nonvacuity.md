# entry-undecodable-nonvacuity

Non-vacuity probe, not a mutation. `negative/entry-undecodable-nonvacuity.cfg`
checks `NoEntryUndecodable` with `EnableDeletePathCorruption = TRUE` and no
other corruption switch set. The probe MUST be violated: it proves
`DoPoisonEntry`, the entry-identity trigger behind
`CorruptHeadFailsClosedOnDeletePaths`, is reachable on its own wherever the
constant is TRUE, independent of the covering-part trigger that
`exhaustive.cfg` now gates out of `Next` to fit its budget. The HEAD-status
trigger is not gated by this constant and stays in `Next` regardless.

Violated invariant: `NoEntryUndecodable` (probe, TLC exit 12).

Trace, in prose: `DoPoisonEntry` has no precondition beyond
`EnableDeletePathCorruption` and its own one-shot flag, so it fires from
`Init` in a single step, poisoning a catalog entry's decode. TLC reaches the
violation at depth 4 (51 states generated, 39 distinct).

## Why it is here

`exhaustive.cfg` sets `EnableDeletePathCorruption = FALSE` to fit its state
budget, which drops the entry-identity and covering-part triggers from that
run's reachable behaviour (the HEAD-status trigger is not gated by this
constant). Without this probe, a regression that made `DoPoisonEntry`
unreachable wherever the constant is TRUE could not be told apart from a
config that simply never enables it.
