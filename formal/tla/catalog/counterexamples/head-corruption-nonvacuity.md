# head-corruption-nonvacuity

Non-vacuity probe, not a mutation. `negative/head-corruption-nonvacuity.cfg`
checks `NoHeadCorrupted` with `EnableDeletePathCorruption = FALSE`, the same
bounds as `negative/corrupt-head-ignores-delete-gate.cfg`. The probe MUST be
violated: it proves `DoCorruptHead`, the HEAD-status trigger behind
`CorruptHeadFailsClosedOnDeletePaths`, is reachable on its own, independent
of the covering-part and entry-identity triggers `EnableDeletePathCorruption`
also gates.

Violated invariant: `NoHeadCorrupted` (probe, TLC exit 12).

Trace, in prose: unlike the other two delete-path corruption triggers,
`DoCorruptHead` needs a completed fold (`head.status = "valid"`) before it is
enabled, so the run first drives a fold to completion and only then fires
`DoCorruptHead`. TLC reaches the violation at depth 9 (9565 states
generated, 5121 distinct).

## Why it is here

`exhaustive.cfg` now gates the covering-part and entry-identity triggers out
of its `Next` to fit its budget, leaving `DoCorruptHead` as the one trigger
still reachable there. This probe pins that `DoCorruptHead` fires on its own
wherever the constant is TRUE, so a regression that made it unreachable
could not hide behind the other two triggers still being exercised
elsewhere.
