# part-unreadable-nonvacuity

Non-vacuity probe, not a mutation. `negative/part-unreadable-nonvacuity.cfg`
checks `NoPartUnreadable` with `EnableDeletePathCorruption = TRUE` and no
other corruption switch set. The probe MUST be violated: it proves
`DoCorruptPart`, the covering-part trigger behind
`CorruptHeadFailsClosedOnDeletePaths`, is reachable on its own wherever the
constant is TRUE, independent of the entry-identity trigger that
`exhaustive.cfg` now gates out of `Next` to fit its budget. The HEAD-status
trigger is not gated by this constant and stays in `Next` regardless.

Violated invariant: `NoPartUnreadable` (probe, TLC exit 12).

Trace, in prose: `DoCorruptPart` has no precondition beyond
`EnableDeletePathCorruption` and its own one-shot flag, so it fires from
`Init` in a single step, making a covering part unreadable. TLC reaches the
violation at depth 4 (43 states generated, 38 distinct).

## Why it is here

`exhaustive.cfg` sets `EnableDeletePathCorruption = FALSE` to fit its state
budget, which drops the entry-identity and covering-part triggers from that
run's reachable behaviour (the HEAD-status trigger is not gated by this
constant). Without this probe, a regression that made `DoCorruptPart`
unreachable wherever the constant is TRUE could not be told apart from a
config that simply never enables it.
