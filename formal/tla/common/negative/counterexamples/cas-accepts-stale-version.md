# Counterexample: CAS accepts a stale or absent version

Config: `negative/cas-accepts-stale-version.cfg` (`CasAcceptsStale = TRUE`).
Expected: `CasOutcomeMatchesEffect` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant CasOutcomeMatchesEffect is violated.

## What the switch breaks

A CasVersion put must apply only when the caller's expected version equals the
key's current version; otherwise the store returns PreconditionFailed and
applies nothing. The switch makes `DoCas` fall through to a plain overwrite
when `CasResult(k, v) # "Ok"` (a stale version or an absent key), while
`lastOp.outcome` still records the PreconditionFailed the result function
computed. The witness now holds a step whose outcome is PreconditionFailed but
whose store record changed.

## Why it is a real bug if it ever held

`CasOutcomeMatchesEffect` reads the store, not a ghost: a non-Ok CAS must leave
`lastOp.after = lastOp.before`. The switch changes the store on a non-Ok
outcome, so `after # before` and the invariant fails. A store with this bug
would let two racing compare-and-swap callers both win against the same base
version, losing one update: the lost-update race CAS exists to prevent. TLC
reaches the violating state after one write followed by a CAS quoting the wrong
version.

The correct model (switch FALSE) passes: a stale or absent CAS is a no-op that
returns PreconditionFailed, so outcome and store delta agree.
